use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use snafu::Snafu;
use tokio::sync::RwLock;

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
    topology: RwLock<HashMap<String, HashSet<String>>>,
    received: RwLock<HashSet<BroadcsatValue>>,
}

#[derive(Clone)]
pub struct BroadcastService {
    inner: Arc<BroadcastServiceInner>,
}

impl Default for BroadcastService {
    fn default() -> Self {
        Self {
            inner: Arc::new(BroadcastServiceInner {
                topology: RwLock::new(HashMap::new()),
                received: RwLock::new(HashSet::new()),
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

    async fn handle_message(
        &self,
        Message { src, body, .. }: Message<Self::Message>,
        node: &NodeState<Self>,
    ) -> Result<(), Self::Error> {
        match body.data {
            BroadcastMessage::Topology { topology } => {
                *self.inner.topology.write().await = topology;

                let reply = body.id.ok_or_else(|| Error::Node {
                    source: BroadcastError::Whatever {
                        message: "No ID in message".into(),
                        source: None,
                    },
                })?;

                node.reply(src, reply, BroadcastMessage::TopologyOk).await?;
            }
            BroadcastMessage::Broadcast { message } => {
                self.inner.received.write().await.insert(message);

                let reply = body.id.ok_or_else(|| Error::Node {
                    source: BroadcastError::Whatever {
                        message: "No ID in message".into(),
                        source: None,
                    },
                })?;

                node.reply(src, reply, BroadcastMessage::BroadcastOk)
                    .await?;
            }
            BroadcastMessage::Read => {
                let messages = self.inner.received.read().await.iter().cloned().collect();
                let reply = body.id.ok_or_else(|| Error::Node {
                    source: BroadcastError::Whatever {
                        message: "No ID in message".into(),
                        source: None,
                    },
                })?;
                node.reply(src, reply, BroadcastMessage::ReadOk { messages })
                    .await?;
            }
            unexpected => {
                tracing::warn!("Unexpected message: {:?}", unexpected);
            }
        }
        Ok(())
    }
}
