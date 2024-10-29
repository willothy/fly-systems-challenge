use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::AtomicU64,
};

use message::{GenerateId, Message, MessageProtocol};
use node::Node;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

mod tokio_serde;

mod error;
mod message;
mod node;

pub use error::*;

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

/// The message body of a Maelstrom message.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum RealMessageData {
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
    Topology {
        topology: BTreeMap<String, BTreeSet<String>>,
    },
    TopologyOk,

    // Application messages
    Echo {
        echo: serde_json::Value,
    },
    EchoOk {
        echo: serde_json::Value,
    },
    Generate,
    GenerateOk {
        id: u64,
    },
}

#[derive(Default)]
struct FlyChallengeService;

impl MessageProtocol for FlyChallengeService {
    type IdGenerator = U64Generator;
    type Data = RealMessageData;
    type Error = Error;

    async fn handle(
        &self,
        Message { src, body, .. }: Message<Self::Data, <Self::IdGenerator as GenerateId>::Id>,
        node: &mut Node<Self, Self::IdGenerator>,
    ) -> Result<()> {
        match body.data {
            RealMessageData::Init {
                node_id,
                node_ids: _,
            } => {
                tracing::info!("Received Init message from {}", src);
                node.id.set(node_id).ok();

                node.send(src, RealMessageData::InitOk).await?;
            }
            RealMessageData::Echo { echo } => {
                tracing::info!("Received Echo message from {}", src);
                node.send(src, RealMessageData::EchoOk { echo }).await?;
            }
            unexpected => {
                tracing::warn!("Unexpected message: {:?}", unexpected);
            }
        }
        Ok(())
    }
}

pub struct U64Generator(AtomicU64);

impl Default for U64Generator {
    fn default() -> Self {
        Self(AtomicU64::new(0))
    }
}

impl GenerateId for U64Generator {
    type Id = u64;

    fn generate_id(&self) -> Self::Id {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(true)
        // Output logs to stderr to conform with Maelstrom spec.
        .with_writer(std::io::stderr)
        .with_thread_names(false)
        .with_file(true)
        .init();

    Node::<FlyChallengeService, U64Generator>::new().run().await
}
