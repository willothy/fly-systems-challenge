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

// Valid message for testing: { "src": "a", "dest": "b", "body": { "type": "error", "code": 1, "text": "test", "msg_id": 1, "in_reply_to": 1 }}
// { "src": "a", "dest": "b", "body": { "type": "init", "node_id": "a", "node_ids": ["a", "b"] }}

/// The message body of a Maelstrom message.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum EchoServiceMessage {
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
    Echo {
        echo: serde_json::Value,
    },
    EchoOk {
        echo: serde_json::Value,
    },
}

#[derive(Default)]
pub struct EchoService;

#[derive(Debug, Snafu)]
pub enum EchoServiceError {
    #[snafu(display("Missing message ID"))]
    MissingMessageId,
    #[snafu(whatever, display("{message}"))]
    Whatever {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error>, Some)))]
        source: Option<Box<dyn std::error::Error>>,
    },
}

impl Into<Error<Self>> for EchoServiceError {
    fn into(self) -> Error<Self> {
        Error::Node { source: self }
    }
}

impl Node for EchoService {
    type Message = EchoServiceMessage;
    type Error = EchoServiceError;

    async fn handle_message(
        &mut self,
        Message { src, body, .. }: Message<Self::Message>,
        node: &mut NodeState<Self>,
    ) -> Result<(), Self::Error> {
        match body.data {
            EchoServiceMessage::Init {
                node_id,
                node_ids: _,
            } => {
                tracing::info!("Received Init message from {}", src);
                node.id.set(node_id).ok();

                let Some(id) = body.id else {
                    return Err(EchoServiceError::MissingMessageId.into());
                };

                node.reply(src, id, EchoServiceMessage::InitOk).await?;
            }
            EchoServiceMessage::Echo { echo } => {
                tracing::info!("Received Echo message from {}", src);

                let Some(id) = body.id else {
                    return Err(EchoServiceError::MissingMessageId.into());
                };

                node.reply(src, id, EchoServiceMessage::EchoOk { echo })
                    .await?;
            }
            unexpected => {
                tracing::warn!("Unexpected message: {:?}", unexpected);
            }
        }
        Ok(())
    }
}
