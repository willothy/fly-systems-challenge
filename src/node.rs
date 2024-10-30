use std::{
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
};

use futures::SinkExt as _;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use tokio::{
    io::{Stdin, Stdout},
    sync::OnceCell,
};
use tokio_stream::StreamExt as _;

use crate::{
    message::{DataOrInit, Message, MessageBody, MessageId},
    tokio_serde,
};

#[derive(Debug, Snafu)]
pub enum InternalError {
    #[snafu(display("EOF on stdin"))]
    Eof,
    #[snafu(display("Unexpected Init message"))]
    UnexpectedInit,
    #[snafu(display("Node was queried before init"))]
    NeedsInit,
    #[snafu(whatever, display("{message}"))]
    Whatever {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error>, Some)))]
        source: Option<Box<dyn std::error::Error>>,
    },
}

impl Into<crate::Error<InternalError>> for InternalError {
    fn into(self) -> crate::Error<InternalError> {
        crate::Error::Internal { source: self }
    }
}

/// The top-level service state for a Maelstrom node.
pub struct NodeState<NodeImpl: Node> {
    /// The node ID.
    pub id: OnceCell<String>,
    /// The channel used to send and receive messages
    channel: tokio_util::codec::Framed<
        tokio::io::Join<Stdin, Stdout>,
        tokio_serde::formats::SymmetricalJson<Message<DataOrInit<NodeImpl::Message>>>,
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

    async fn recv(
        &mut self,
    ) -> crate::Result<Option<Message<DataOrInit<NodeImpl::Message>>>, NodeImpl::Error> {
        Ok(self.channel.next().await.transpose()?)
    }

    /// Get the node ID. Panics if called before init.
    pub fn id(&self) -> &str {
        self.id.get().expect("node ID should be set on init")
    }

    pub async fn send_init_ok(
        &mut self,
        re: MessageId,
        dest: String,
    ) -> crate::Result<(), NodeImpl::Error> {
        self.send_message(dest, Some(re), DataOrInit::InitOk).await
    }

    pub async fn reply(
        &mut self,
        dest: String,
        re: MessageId,
        data: NodeImpl::Message,
    ) -> crate::Result<(), NodeImpl::Error> {
        self.send_message(dest, Some(re), DataOrInit::Data(data))
            .await
    }

    #[allow(unused)]
    pub async fn send(
        &mut self,
        dest: String,
        data: NodeImpl::Message,
    ) -> crate::Result<(), NodeImpl::Error> {
        self.send_message(dest, None, DataOrInit::Data(data)).await
    }

    async fn send_message(
        &mut self,
        dest: String,
        re: Option<MessageId>,
        data: DataOrInit<NodeImpl::Message>,
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

    pub async fn run(mut self, mut node: NodeImpl) -> crate::Result<(), NodeImpl::Error> {
        tracing::info!("Starting Maelstrom node");

        let Message { src, body, .. } =
            self.recv().await?.ok_or_else(|| crate::Error::Internal {
                source: InternalError::Eof,
            })?;

        match body.data {
            DataOrInit::Init {
                node_id,
                node_ids: _,
            } => {
                tracing::info!("Received Init message from {}", node_id);
                self.id.set(node_id).expect("ID should not be set yet");

                self.send_init_ok(body.id.expect("init message ID"), src)
                    .await?;
            }
            _ => {
                return Err(crate::Error::Internal {
                    source: crate::node::InternalError::NeedsInit,
                });
            }
        }

        loop {
            match self.recv().await {
                Ok(Some(msg)) => {
                    let data = match msg.into_data::<crate::Error<NodeImpl::Error>>() {
                        Ok(data) => data,
                        Err(e) => {
                            tracing::warn!("Error decoding message: {}", e);
                            continue;
                        }
                    };
                    node.handle_message(data, &mut self).await?
                }
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
