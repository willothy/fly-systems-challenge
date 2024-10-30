use std::{
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use futures::SinkExt as _;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use tokio::sync::OnceCell;
use tokio_stream::StreamExt;

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
        #[snafu(source(from(Box<dyn std::error::Error + Send + Sync + 'static>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },
}

impl Into<crate::Error<InternalError>> for InternalError {
    fn into(self) -> crate::Error<InternalError> {
        crate::Error::Internal { source: self }
    }
}

pub struct NodeStateInner<NodeImpl: Node + Send + Sync + 'static> {
    /// The node ID.
    pub id: OnceCell<String>,

    // stdin: tokio_util::codec::FramedRead<
    //     Stdin,
    //     tokio_serde::formats::SymmetricalJson<Message<DataOrInit<NodeImpl::Message>>>,
    // >,
    /// The channel used to send and receive messages
    // channel: tokio_util::codec::Framed<
    //     tokio::io::Join<Stdin, Stdout>,
    //     tokio_serde::formats::SymmetricalJson<Message<DataOrInit<NodeImpl::Message>>>,
    // >,
    next_id: AtomicU64,
    rpc: tokio::sync::mpsc::UnboundedSender<Message<DataOrInit<NodeImpl::Message>>>,
    node: NodeImpl,
}

/// The top-level service state for a Maelstrom node.
pub struct NodeState<NodeImpl: Node + Send + Sync + 'static> {
    inner: Arc<NodeStateInner<NodeImpl>>,
}

impl<NodeImpl: Node + Send + Sync + 'static> Clone for NodeState<NodeImpl> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

pub trait Node
where
    Self: Clone + Sync + Send + Sized + 'static,
{
    type Message: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn handle_message(
        &self,
        message: Message<Self::Message>,
        state: &NodeState<Self>,
    ) -> impl Future<Output = crate::Result<(), Self::Error>> + Send + Sync;

    fn init(
        &self,
        state: &NodeState<Self>,
    ) -> impl Future<Output = crate::Result<(), Self::Error>> + Send + Sync {
        async { Ok(()) }
    }
}

impl<NodeImpl: Node + Send + Sync + 'static> NodeState<NodeImpl> {
    fn next_message_id(&self) -> crate::message::MessageId {
        self.inner.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the node ID. Panics if called before init.
    pub fn id(&self) -> &str {
        self.inner.id.get().expect("node ID should be set on init")
    }

    pub async fn send_init_ok(
        &mut self,
        re: MessageId,
        dest: String,
    ) -> crate::Result<(), NodeImpl::Error> {
        self.send_message(dest, Some(re), DataOrInit::InitOk).await
    }

    pub async fn reply(
        &self,
        dest: String,
        re: MessageId,
        data: NodeImpl::Message,
    ) -> crate::Result<(), NodeImpl::Error> {
        self.send_message(dest, Some(re), DataOrInit::Data(data))
            .await
    }

    #[allow(unused)]
    pub async fn send(
        &self,
        dest: String,
        data: NodeImpl::Message,
    ) -> crate::Result<(), NodeImpl::Error> {
        self.send_message(dest, None, DataOrInit::Data(data)).await
    }

    pub async fn send_message(
        &self,
        dest: String,
        re: Option<MessageId>,
        data: DataOrInit<NodeImpl::Message>,
    ) -> crate::Result<(), NodeImpl::Error> {
        let src = self.id().to_owned();
        Ok(self
            .inner
            .rpc
            .send(Message {
                src,
                dest,
                body: MessageBody {
                    id: Some(self.next_message_id()),
                    re,
                    data,
                },
            })
            .map_err(|e| crate::Error::Internal {
                source: InternalError::Whatever {
                    message: format!("Error sending message: {}", e),
                    source: None,
                },
            })?)
    }

    pub async fn run(node: NodeImpl) -> crate::Result<(), NodeImpl::Error> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let mut state = Self {
            inner: Arc::new(NodeStateInner {
                // Default ID should be empty string - this will be set by the Maelstrom service.
                id: OnceCell::new(),
                node,
                // channel: tokio_util::codec::Framed::new(
                //     tokio::io::join(tokio::io::stdin(), tokio::io::stdout()),
                //     tokio_serde::formats::SymmetricalJson::default(),
                // ),
                next_id: AtomicU64::new(0),
                rpc: tx,
            }),
        };

        let json = tokio_serde::formats::SymmetricalJson::default();
        let mut input = tokio_util::codec::FramedRead::new(tokio::io::stdin(), json);
        let mut output = tokio_util::codec::FramedWrite::new(tokio::io::stdout(), json);

        tracing::info!("Starting Maelstrom node");

        let Message { src, body, .. } =
            input.next().await.ok_or_else(|| crate::Error::Internal {
                source: InternalError::Eof,
            })??;

        match body.data {
            DataOrInit::Init {
                node_id,
                node_ids: _,
            } => {
                tracing::info!("Received Init message from {}", node_id);
                state
                    .inner
                    .id
                    .set(node_id)
                    .expect("ID should not be set yet");

                state
                    .send_init_ok(body.id.expect("init message ID"), src)
                    .await?;
            }
            _ => {
                return Err(crate::Error::Internal {
                    source: crate::node::InternalError::NeedsInit,
                });
            }
        }

        state.inner.node.init(&state).await?;

        tokio::spawn(async move {
            let mut buf = Vec::new();
            loop {
                rx.recv_many(&mut buf, 8).await;
                for msg in buf.drain(..) {
                    output.feed(msg).await.ok();
                }
                output.flush().await.ok();
            }
        });

        loop {
            match input.next().await.transpose() {
                Ok(Some(msg)) => {
                    tokio::spawn({
                        let state = state.clone();
                        async move {
                            match msg.into_data::<NodeImpl::Error>() {
                                Ok(data) => {
                                    state.inner.node.handle_message(data, &state).await.ok();
                                }
                                Err(e) => {
                                    tracing::warn!("Error decoding message: {}", e);
                                }
                            };
                        }
                    });
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

pub async fn run<NodeImpl: Node + Send + Sync + 'static>(
    node: NodeImpl,
) -> Result<(), crate::Error<NodeImpl::Error>> {
    NodeState::run(node).await
}
