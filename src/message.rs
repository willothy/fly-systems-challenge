use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::node::Node;

pub trait MessageId
where
    Self: std::fmt::Debug
        + Clone
        + PartialEq
        + Serialize
        + DeserializeOwned
        + for<'a> Deserialize<'a>,
{
}

impl<T> MessageId for T where
    Self: std::fmt::Debug
        + Clone
        + PartialEq
        + Serialize
        + DeserializeOwned
        + for<'a> Deserialize<'a>
{
}

pub trait GenerateId
where
    Self: Default,
{
    type Id: MessageId;

    fn generate_id(&self) -> Self::Id;
}

pub trait MessageProtocol: Default + Sized {
    type IdGenerator: GenerateId;
    type Data: Serialize + DeserializeOwned;
    type Error: std::error::Error + 'static;

    fn handle(
        &self,
        message: Message<Self::Data, <Self::IdGenerator as GenerateId>::Id>,
        node: &mut Node<Self, Self::IdGenerator>,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageBody<Data, Id> {
    #[serde(rename = "msg_id")]
    pub id: Option<Id>,
    /// The ID of the message this message is in reply to.
    pub re: Option<Id>,
    #[serde(flatten)]
    pub data: Data,
}

/// A Maelstrom message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Data, Id> {
    /// The node ID of the sender.
    pub src: String,

    /// The node ID of the receiver.
    pub dest: String,

    /// The message content, with type defined by enum variant.
    pub body: MessageBody<Data, Id>,
}
