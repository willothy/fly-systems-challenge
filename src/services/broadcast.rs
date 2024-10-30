use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use snafu::Snafu;
use tokio::sync::{Mutex, RwLock};

use crate::async_dashmap::AsyncDashMap;
pub use crate::error::*;
use crate::message::Message;
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

type BroadcsatValue = u64;

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
        messages: Vec<BroadcsatValue>,
    },
    Broadcast {
        message: BroadcsatValue,
    },
    BroadcastOk,
}

pub struct BroadcastServiceInner {
    topology: arc_swap::ArcSwap<HashMap<String, HashSet<String>>>,
    // topology: atomic::Atomic<rpds::RedBlackTreeMapSync<String, HashSet<String>>>,
    // received: AsyncDashMap<BroadcsatValue, ()>,
    received_w: Mutex<evmap::WriteHandle<u64, ()>>,
    read_factory: evmap::ReadHandleFactory<u64, ()>,
}

#[derive(Clone)]
pub struct BroadcastService {
    inner: Arc<BroadcastServiceInner>,
}

impl Default for BroadcastService {
    fn default() -> Self {
        let (_received_r, received_w) = evmap::new();
        Self {
            inner: Arc::new(BroadcastServiceInner {
                topology: arc_swap::ArcSwap::new(Arc::new(HashMap::new())),
                // topology: atomic::Atomic::new(rpds::RedBlackTreeMap::new_sync()),
                // received: AsyncDashMap::new(),
                read_factory: received_w.factory(),
                received_w: Mutex::new(received_w),
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

impl Node for BroadcastService {
    type Message = BroadcastMessage;
    type Error = BroadcastError;

    async fn init(&self, node: &NodeState<Self>) -> crate::Result<(), Self::Error> {
        tokio::spawn({
            let node = node.clone();
            let inner = self.inner.clone();
            async move {
                let mut interval = tokio::time::interval(Duration::from_millis(500));
                let received_r = inner.read_factory.handle();

                loop {
                    interval.tick().await;

                    if let Some(neighbors) = inner.topology.load().get(node.id()).cloned() {
                        let Some(updates) = received_r.read().map(|updates| {
                            updates.iter().map(|entry| *entry.0).collect::<Vec<_>>()
                        }) else {
                            tracing::warn!("No updates");
                            continue;
                        };
                        // let received: Vec<_> = self.inner.received.iter().map(|e| *e.key()).collect();
                        tracing::info!("Sending messages to {:?}", neighbors);
                        for neighbor in neighbors.iter() {
                            for message in updates.iter().copied() {
                                node.send(
                                    neighbor.clone(),
                                    BroadcastMessage::Broadcast { message },
                                )
                                .await?;
                            }
                        }
                    } else {
                        tracing::warn!("No topology entry for {}", node.id());
                        break;
                    }
                }

                crate::Result::Ok(())
            }
        });

        Ok(())
    }

    async fn handle_message(
        &self,
        Message {
            src, dest, body, ..
        }: Message<Self::Message>,
        node: &NodeState<Self>,
    ) -> Result<(), Self::Error> {
        match body.data {
            BroadcastMessage::Topology { topology } => {
                tracing::info!("{:?}", topology);

                let reply = body.id.ok_or_else(|| Error::Node {
                    source: BroadcastError::Whatever {
                        message: "No ID in message".into(),
                        source: None,
                    },
                })?;

                node.reply(src, reply, BroadcastMessage::TopologyOk).await?;

                self.inner.topology.store(Arc::new(topology));
            }
            BroadcastMessage::Broadcast { message } => {
                node.send_message(
                    src.clone(),
                    body.id,
                    crate::message::DataOrInit::Data(BroadcastMessage::BroadcastOk),
                )
                .await?;

                self.inner
                    .received_w
                    .lock()
                    .await
                    .insert(message, ())
                    .refresh();
            }
            BroadcastMessage::Read => {
                let messages = self
                    .inner
                    .read_factory
                    .handle()
                    .read()
                    .into_iter()
                    .map(|mapref| {
                        mapref
                            .into_iter()
                            .map(|(k, _)| *k)
                            .collect::<Vec<_>>()
                            .into_iter()
                    })
                    .flatten()
                    .collect();

                node.send_message(
                    src,
                    body.id,
                    crate::message::DataOrInit::Data(BroadcastMessage::ReadOk { messages }),
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
