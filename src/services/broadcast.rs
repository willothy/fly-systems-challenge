use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use snafu::Snafu;

use crate::async_dashmap::AsyncDashMap;
pub use crate::error::*;
use crate::message::{DataOrInit, Message};
use crate::node::{Node, NodeState};

/// A Maelstrom error code.
#[derive(Debug, Serialize_repr, Deserialize_repr)]
#[serde(rename_all = "snake_case")]
#[repr(u64)]
pub enum ErrorCode {
    Timeout = 0,
    NodeNotFound = 1,
    NotSupported = 10,
    TemporarilyUnavailable = 11,
    MalformedRequest = 12,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    KeyAlreadyExists = 21,
    PreconditionFailed = 22,
    TxnConflict = 30,
}

type BroadcastValue = u64;

/// The message body of a Maelstrom message.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BroadcastMessage {
    Error {
        code: ErrorCode,
        text: String,
    },
    Topology {
        topology: HashMap<String, HashSet<String>>,
    },
    TopologyOk,
    Read,
    ReadOk {
        messages: HashSet<BroadcastValue>,
    },
    Broadcast {
        message: BroadcastValue,
    },
    BroadcastOk,
    Gossip {
        seen: HashSet<BroadcastValue>,
    },
}

pub struct BroadcastServiceInner {
    neighbors: arc_swap::ArcSwap<HashSet<String>>,
    received: AsyncDashMap<u64, ()>,
    known: AsyncDashMap<String, HashSet<u64>>,
}

#[derive(Clone)]
pub struct BroadcastService {
    inner: Arc<BroadcastServiceInner>,
}

impl Default for BroadcastService {
    fn default() -> Self {
        Self {
            inner: Arc::new(BroadcastServiceInner {
                neighbors: arc_swap::ArcSwap::new(Arc::new(HashSet::new())),
                received: AsyncDashMap::new(),
                known: AsyncDashMap::new(),
            }),
        }
    }
}

#[derive(Debug, Snafu)]
pub enum BroadcastError {
    #[snafu(whatever, display("{message}"))]
    Whatever {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error+Send+Sync+'static>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },
}

impl Into<Error<Self>> for BroadcastError {
    fn into(self) -> Error<Self> {
        Error::Node { source: self }
    }
}

impl BroadcastService {
    pub async fn gossip(&self, node: NodeState<Self>) -> crate::Result<(), BroadcastError> {
        for neighbor in self.inner.neighbors.load().iter() {
            let known_to_neighbor =
                self.inner
                    .known
                    .get(neighbor)
                    .await
                    .ok_or_else(|| Error::Node {
                        source: BroadcastError::Whatever {
                            message: "No known messages for neighbor".into(),
                            source: None,
                        },
                    })?;

            let (_already_known, notify_of) = self
                .inner
                .received
                .clone()
                .into_iter()
                .map(|(x, _)| x)
                .partition(|m| known_to_neighbor.contains(m));

            node.send(
                neighbor.as_str(),
                BroadcastMessage::Gossip { seen: notify_of },
            )
            .await?;
        }

        Ok(())
    }
}

impl Node for BroadcastService {
    type Message = BroadcastMessage;
    type Error = BroadcastError;

    async fn init(
        &self,
        node: &NodeState<Self>,
        node_ids: Vec<String>,
    ) -> crate::Result<(), Self::Error> {
        for node_id in node_ids {
            self.inner.known.insert(node_id, HashSet::new()).await;
        }

        let service = self.clone();
        let node = node.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                service.gossip(node.clone()).await.ok();
            }
        });

        Ok(())
    }

    async fn handle_message(
        &self,
        Message { src, body, .. }: Message<Self::Message>,
        node: &NodeState<Self>,
    ) -> Result<(), Self::Error> {
        match body.data {
            BroadcastMessage::Gossip { seen } => {
                self.inner
                    .known
                    .get_mut(&src.to_string())
                    .await
                    .ok_or_else(|| Error::Node {
                        source: BroadcastError::Whatever {
                            message: "No known messages for neighbor".into(),
                            source: None,
                        },
                    })?
                    .extend(&seen);
                for message in seen {
                    self.inner.received.insert(message, ()).await;
                }
            }
            BroadcastMessage::Topology { topology } => {
                tracing::info!("{:?}", topology);

                let reply = body.id.ok_or_else(|| Error::Node {
                    source: BroadcastError::Whatever {
                        message: "No ID in message".into(),
                        source: None,
                    },
                })?;

                node.reply(src, reply, BroadcastMessage::TopologyOk).await?;

                self.inner.neighbors.store(Arc::new(
                    topology.get(&*node.id()).cloned().expect("topology"),
                ));
            }
            BroadcastMessage::Broadcast { message } => {
                self.inner.received.insert(message, ()).await;

                node.send_message(
                    src.clone(),
                    body.id,
                    crate::message::DataOrInit::Data(BroadcastMessage::BroadcastOk),
                )
                .await?;
            }
            BroadcastMessage::BroadcastOk => {}
            BroadcastMessage::ReadOk { .. } => {}
            BroadcastMessage::Read => {
                let messages = self
                    .inner
                    .received
                    .clone()
                    .iter()
                    .map(|x| *x.key())
                    .collect::<HashSet<_>>();

                node.send_message(
                    src,
                    body.id,
                    DataOrInit::Data(BroadcastMessage::ReadOk { messages }),
                )
                .await
                .ok();
            }
            unexpected => {
                tracing::warn!("Unexpected message: {:?}", unexpected);
            }
        }
        Ok(())
    }
}
