use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
};

use futures::SinkExt as _;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{Stdin, Stdout},
    sync::{OnceCell, RwLock},
};
use tokio_stream::StreamExt as _;

use crate::{
    message::{Message, MessageBody, MessageId},
    tokio_serde,
};

/// A peer node in the Maelstrom network.
pub struct Peer {
    /// The node ID of the peer.
    #[allow(unused)]
    id: String,
}

/// The top-level service state for a Maelstrom node.
pub struct NodeState<NodeImpl: Node> {
    /// The node ID.
    pub id: OnceCell<String>,
    /// Map of node ID to neighnor node IDs.
    #[allow(unused)]
    topology: RwLock<HashMap<String, HashSet<Peer>>>,
    /// The channel used to send and receive messages
    channel: tokio_util::codec::Framed<
        tokio::io::Join<Stdin, Stdout>,
        tokio_serde::formats::SymmetricalJson<Message<NodeImpl::Message>>,
    >,
    next_id: AtomicU64,
}

pub trait Node
where
    Self: Sized,
{
    type Message: Serialize + for<'de> Deserialize<'de>;
    type Error: std::error::Error + 'static;

    fn handle_message(
        &mut self,
        message: Message<Self::Message>,
        state: &mut NodeState<Self>,
    ) -> impl Future<Output = crate::Result<(), Self::Error>>;
}

impl<NodeImpl: Node> NodeState<NodeImpl> {
    fn new() -> Self {
        Self {
            // Default ID should be empty string - this will be set by the Maelstrom service.
            id: OnceCell::new(),
            topology: RwLock::new(HashMap::new()),
            channel: tokio_util::codec::Framed::new(
                tokio::io::join(tokio::io::stdin(), tokio::io::stdout()),
                tokio_serde::formats::SymmetricalJson::default(),
            ),
            next_id: AtomicU64::new(0),
        }
    }

    fn next_message_id(&self) -> crate::message::MessageId {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    async fn recv(&mut self) -> crate::Result<Option<Message<NodeImpl::Message>>, NodeImpl::Error> {
        Ok(self.channel.next().await.transpose()?)
    }

    /// Get the node ID. Panics if called before init.
    pub fn id(&self) -> &str {
        self.id.get().expect("node ID should be set on init")
    }

    pub async fn reply(
        &mut self,
        dest: String,
        re: MessageId,
        data: NodeImpl::Message,
    ) -> crate::Result<(), NodeImpl::Error> {
        self.send_message(dest, Some(re), data).await
    }

    #[allow(unused)]
    pub async fn send(
        &mut self,
        dest: String,
        data: NodeImpl::Message,
    ) -> crate::Result<(), NodeImpl::Error> {
        self.send_message(dest, None, data).await
    }

    async fn send_message(
        &mut self,
        dest: String,
        re: Option<MessageId>,
        data: NodeImpl::Message,
    ) -> crate::Result<(), NodeImpl::Error> {
        let src = self
            .id
            .get()
            .expect("node ID should be set on init")
            .clone();
        Ok(self
            .channel
            .send(Message {
                src,
                dest,
                body: MessageBody {
                    id: Some(self.next_message_id()),
                    re,
                    data,
                },
            })
            .await?)
    }

    pub async fn run(mut self, mut node: NodeImpl) -> Result<(), crate::Error<NodeImpl::Error>> {
        tracing::info!("Starting Maelstrom node");

        loop {
            match self.recv().await {
                Ok(Some(msg)) => node.handle_message(msg, &mut self).await?,
                Ok(None) => {
                    tracing::warn!("EOF on stdin");
                    return Ok(());
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
    }
}

pub async fn run<NodeImpl: Node + 'static>(
    node: NodeImpl,
) -> Result<(), crate::Error<NodeImpl::Error>> {
    NodeState::new().run(node).await
}
