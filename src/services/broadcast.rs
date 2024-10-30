use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use snafu::Snafu;

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

#[derive(Default)]
pub struct BroadcastService {
    topology: HashMap<String, HashSet<String>>,
    received: HashSet<BroadcsatValue>,
}

#[derive(Debug, Snafu)]
pub enum BroadcastError {
    #[snafu(whatever, display("{message}"))]
    Whatever {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error>, Some)))]
        source: Option<Box<dyn std::error::Error>>,
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
        &mut self,
        Message { src, body, .. }: Message<Self::Message>,
        node: &mut NodeState<Self>,
    ) -> Result<(), Self::Error> {
        match body.data {
            BroadcastMessage::Topology { topology } => {
                self.topology = topology;

                tracing::info!("Updated topology: {:?}", self.topology);

                let reply = body.id.ok_or_else(|| Error::Node {
                    source: BroadcastError::Whatever {
                        message: "No ID in message".into(),
                        source: None,
                    },
                })?;

                node.reply(src, reply, BroadcastMessage::TopologyOk).await?;
            }
            BroadcastMessage::Broadcast { message } => {
                self.received.insert(message);

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
                let messages = self.received.iter().cloned().collect();
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
