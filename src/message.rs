use serde::{Deserialize, Serialize};

pub type MessageId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageBody<Data> {
    #[serde(rename = "msg_id")]
    pub id: Option<MessageId>,
    /// The ID of the message this message is in reply to.
    #[serde(rename = "in_reply_to")]
    pub re: Option<MessageId>,
    #[serde(flatten)]
    pub data: Data,
}

/// A Maelstrom message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Data> {
    /// The node ID of the sender.
    pub src: String,

    /// The node ID of the receiver.
    pub dest: String,

    /// The message content, with type defined by enum variant.
    pub body: MessageBody<Data>,
}
