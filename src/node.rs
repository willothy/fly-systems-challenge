use std::collections::{HashMap, HashSet};

use futures::SinkExt as _;
use tokio::{
    io::{Stdin, Stdout},
    sync::{OnceCell, RwLock},
};
use tokio_stream::StreamExt as _;

use crate::{
    message::{GenerateId, Message, MessageBody, MessageId, MessageProtocol},
    tokio_serde,
};

/// A peer node in the Maelstrom network.
pub struct Peer {
    /// The node ID of the peer.
    #[allow(unused)]
    id: String,
}

/// The top-level service state for a Maelstrom node.
pub struct Node<Protocol: MessageProtocol, IdGenerator: GenerateId> {
    /// The node ID.
    pub id: OnceCell<String>,
    /// Map of node ID to neighnor node IDs.
    #[allow(unused)]
    topology: RwLock<HashMap<String, HashSet<Peer>>>,
    /// The channel used to send and receive messages
    channel: tokio_util::codec::Framed<
        tokio::io::Join<Stdin, Stdout>,
        tokio_serde::formats::SymmetricalJson<Message<Protocol::Data, IdGenerator::Id>>,
    >,

    /// Producer for message IDs.
    id_generator: IdGenerator,
}

impl<Protocol, IdGenerator> Node<Protocol, IdGenerator>
where
    IdGenerator: GenerateId,
    IdGenerator::Id: MessageId,
    Protocol: MessageProtocol<IdGenerator = IdGenerator>,
    Protocol::Error: From<futures::io::Error>,
{
    pub fn new() -> Self {
        Self {
            // Default ID should be empty string - this will be set by the Maelstrom service.
            id: OnceCell::new(),
            topology: RwLock::new(HashMap::new()),
            channel: tokio_util::codec::Framed::new(
                tokio::io::join(tokio::io::stdin(), tokio::io::stdout()),
                tokio_serde::formats::SymmetricalJson::default(),
            ),
            id_generator: IdGenerator::default(),
        }
    }

    pub fn next_message_id(&self) -> IdGenerator::Id {
        self.id_generator.generate_id()
    }

    pub async fn send(
        &mut self,
        dest: String,
        re: Option<IdGenerator::Id>,
        data: Protocol::Data,
    ) -> std::result::Result<(), Protocol::Error> {
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

    pub async fn recv(
        &mut self,
    ) -> std::result::Result<Option<Message<Protocol::Data, IdGenerator::Id>>, Protocol::Error>
    {
        Ok(self.channel.next().await.transpose()?)
    }

    pub async fn run(&mut self) -> std::result::Result<(), Protocol::Error> {
        tracing::info!("Starting Maelstrom node");

        let service = Protocol::default();

        while let Some(msg) = self.recv().await? {
            service.handle(msg, self).await?;
        }

        Ok(())
    }
}
