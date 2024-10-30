use std::ops::RangeFrom;

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

/// The message body of a Maelstrom message.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum UniqueIdServiceMessage {
    /// Sent from Maelstrom to each node at the start of the simulation.
    Init {
        /// The node ID assigned to the receiver. The receiver should use this ID in all subsequent
        node_id: String,
        /// A list of every node ID in the network, including the receiver. An identical list,
        /// including order, is sent to each node.
        node_ids: Vec<String>,
    },
    /// Sent from Maelstrom to each node to indicate that the simulation has started.
    InitOk,

    Error {
        code: ErrorCode,
        text: String,
    },

    // Application messages
    Generate,
    GenerateOk {
        id: String,
    },
}

pub struct UniqueIdService {
    next_id: RangeFrom<u64>,
}

impl UniqueIdService {
    pub fn new() -> Self {
        Self { next_id: 0.. }
    }
}

impl Default for UniqueIdService {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Snafu)]
pub enum UniqueIdServiceError {
    #[snafu(display("Missing message ID"))]
    MissingMessageId,
    #[snafu(whatever, display("{message}"))]
    Whatever {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error>, Some)))]
        source: Option<Box<dyn std::error::Error>>,
    },
}

impl Into<Error<Self>> for UniqueIdServiceError {
    fn into(self) -> Error<Self> {
        Error::Node { source: self }
    }
}

impl Node for UniqueIdService {
    type Message = UniqueIdServiceMessage;
    type Error = UniqueIdServiceError;

    async fn handle_message(
        &mut self,
        Message { src, body, .. }: Message<Self::Message>,
        node: &mut NodeState<Self>,
    ) -> Result<(), Self::Error> {
        match body.data {
            UniqueIdServiceMessage::Init {
                node_id,
                node_ids: _,
            } => {
                tracing::info!("Received Init message from {}", src);
                node.id.set(node_id).ok();

                let Some(id) = body.id else {
                    return Err(UniqueIdServiceError::MissingMessageId.into());
                };

                node.reply(src, id, UniqueIdServiceMessage::InitOk).await?;
            }
            UniqueIdServiceMessage::Generate => {
                tracing::info!("Received Generate message from {}", src);

                let Some(msg_id) = body.id else {
                    tracing::error!("Missing message ID");
                    return Err(UniqueIdServiceError::MissingMessageId.into());
                };
                let node_id = node.id();

                node.reply(
                    src,
                    msg_id,
                    UniqueIdServiceMessage::GenerateOk {
                        id: format!("{}-{}", node_id, self.next_id.next().expect("unique ID")),
                    },
                )
                .await?;
            }
            unexpected => {
                panic!("Unexpected message: {:?}", unexpected);
                // tracing::warn!("Unexpected message: {:?}", unexpected);
            }
        }
        Ok(())
    }
}
