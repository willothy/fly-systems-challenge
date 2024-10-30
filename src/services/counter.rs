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

/// The message body of a Maelstrom message.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CounterMessage {
    Error { code: ErrorCode, text: String },

    Add { delta: u64 },
    AddOk,
    Read,
    ReadOk { value: u64 },
}

#[derive(Default, Clone)]
pub struct CounterService {}

#[derive(Debug, Snafu)]
pub enum CounterError {
    #[snafu(whatever, display("{message}"))]
    Whatever {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error+Send+Sync+ 'static>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },
}

impl Into<Error<Self>> for CounterError {
    fn into(self) -> Error<Self> {
        Error::Node { source: self }
    }
}

impl Node for CounterService {
    type Message = CounterMessage;
    type Error = CounterError;

    async fn handle_message(
        &self,
        Message { src, body, .. }: Message<Self::Message>,
        node: &NodeState<Self>,
    ) -> Result<(), Self::Error> {
        match body.data {
            unexpected => {
                tracing::warn!("Unexpected message: {:?}", unexpected);
            }
        }
        Ok(())
    }
}
