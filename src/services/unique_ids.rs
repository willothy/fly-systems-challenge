use std::sync::atomic::AtomicU64;
use std::sync::Arc;

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
    Error { code: ErrorCode, text: String },

    // Application messages
    Generate,
    GenerateOk { id: String },
}

#[derive(Clone, Default)]
pub struct UniqueIdService {
    next_id: Arc<AtomicU64>,
}

#[derive(Debug, Snafu)]
pub enum UniqueIdServiceError {
    #[snafu(display("Missing message ID"))]
    MissingMessageId,
    #[snafu(whatever, display("{message}"))]
    Whatever {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error+Send+Sync+'static>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
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
        &self,
        Message { src, body, .. }: Message<Self::Message>,
        node: &NodeState<Self>,
    ) -> Result<(), Self::Error> {
        match body.data {
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
                        id: format!(
                            "{}-{}",
                            node_id,
                            self.next_id
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                        ),
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
