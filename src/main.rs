use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    ptr::NonNull,
    sync::atomic::AtomicU64,
};

use futures::SinkExt;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use snafu::Snafu;
use tokio::{
    io::{Stdin, Stdout},
    sync::{OnceCell, RwLock},
};

mod tokio_serde;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("IO error: {}", source))]
    Io {
        #[snafu(source)]
        source: std::io::Error,
    },

    #[snafu(whatever, display("{message}"))]
    Whatever {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error>, Some)))]
        source: Option<Box<dyn std::error::Error>>,
    },
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Self::Io { source }
    }
}

pub type Result<T = ()> = std::result::Result<T, Error>;

/// A peer node in the Maelstrom network.
pub struct Peer {
    /// The node ID of the peer.
    id: String,
}

/// The top-level service state for a Maelstrom node.
pub struct Node {
    /// The node ID.
    id: OnceCell<(NonNull<str>, usize)>,
    /// Map of node ID to neighnor node IDs.
    topology: RwLock<HashMap<String, HashSet<Peer>>>,
    /// The next message ID to assign.
    next_message_id: AtomicU64,
    /// The channel used to send and receive messages
    channel: tokio_util::codec::Framed<
        tokio::io::Join<Stdin, Stdout>,
        tokio_serde::formats::Json<Message<String>, Message<&'static str>>,
    >,
}

impl Drop for Node {
    fn drop(&mut self) {
        let Some((mut id, cap)) = self.id.take() else {
            return;
        };

        // Safety: there should be no other references to this string.
        let id = unsafe { id.as_mut() };

        drop(unsafe { String::from_raw_parts(id.as_mut_ptr(), id.len(), cap) });
    }
}

impl Node {
    pub fn new() -> Self {
        Self {
            // Default ID should be empty string - this will be set by the Maelstrom service.
            id: OnceCell::new(),
            topology: RwLock::new(HashMap::new()),
            next_message_id: AtomicU64::new(0),
            channel: tokio_util::codec::Framed::new(
                tokio::io::join(tokio::io::stdin(), tokio::io::stdout()),
                tokio_serde::formats::Json::default(),
            ),
        }
    }

    pub fn next_message_id(&self) -> u64 {
        self.next_message_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn send(&mut self, dest: String, body: MessageBody) -> Result<()> {
        let src = self.node_id();
        Ok(self.channel.send(Message { src, dest, body }).await?)
    }

    pub async fn recv(&mut self) -> Result<Option<Message<String>>> {
        Ok(tokio_stream::StreamExt::next(&mut self.channel)
            .await
            .transpose()?)
    }

    fn node_id(&mut self) -> &'static str {
        unsafe {
            self.id
                .get()
                .expect("node ID should be set on init")
                .0
                .as_ref()
        }
    }

    async fn set_node_id(&mut self, id: String) {
        self.id
            .get_or_init(|| async move {
                let len = id.len();
                (
                    NonNull::new(id.leak()).expect("id ptr should never be null"),
                    len,
                )
            })
            .await;
    }

    pub async fn run(&mut self) -> Result<()> {
        tracing::info!("Starting Maelstrom node");

        while let Some(msg) = self.recv().await? {
            match msg {
                Message { src, body, .. } => match body {
                    MessageBody::Init {
                        msg_id,
                        node_id,
                        node_ids: _,
                    } => {
                        tracing::info!("Received Init message from {}", src);
                        self.set_node_id(node_id).await;

                        self.send(
                            src,
                            MessageBody::InitOk {
                                msg_id: self.next_message_id(),
                                in_reply_to: msg_id,
                            },
                        )
                        .await?;
                    }
                    MessageBody::Echo { msg_id, echo } => {
                        tracing::info!("Received Echo message from {}", src);
                        self.send(
                            src,
                            MessageBody::EchoOk {
                                msg_id: self.next_message_id(),
                                in_reply_to: msg_id,
                                echo,
                            },
                        )
                        .await?;
                    }
                    unexpected => {
                        tracing::warn!("Unexpected message: {:?}", unexpected);
                    }
                },
            }
        }

        Ok(())
    }
}

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
pub enum MessageBody {
    /// Sent from Maelstrom to each node at the start of the simulation.
    Init {
        msg_id: u64,
        /// The node ID assigned to the receiver. The receiver should use this ID in all subsequent
        node_id: String,
        /// A list of every node ID in the network, including the receiver. An identical list,
        /// including order, is sent to each node.
        node_ids: Vec<String>,
    },
    /// Sent from Maelstrom to each node to indicate that the simulation has started.
    InitOk {
        msg_id: u64,
        in_reply_to: u64,
    },

    Error {
        in_reply_to: u64,
        code: ErrorCode,
        text: String,
    },
    Topology {
        msg_id: u64,
        topology: BTreeMap<String, BTreeSet<String>>,
    },
    TopologyOk {
        msg_id: Option<u64>,
        in_reply_to: u64,
    },

    // Application messages
    Echo {
        msg_id: u64,
        echo: serde_json::Value,
    },
    EchoOk {
        msg_id: u64,
        in_reply_to: u64,
        echo: serde_json::Value,
    },
    Generate,
    GenerateOk {
        id: u64,
    },
}

/// A Maelstrom message.
#[derive(Debug, Serialize, Deserialize)]
pub struct Message<Src> {
    /// The node ID of the sender.
    pub src: Src,

    /// The node ID of the receiver.
    pub dest: String,

    /// The message content, with type defined by enum variant.
    pub body: MessageBody,
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

    Node::new().run().await
}