use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub type MessageId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DataOrInit<Data> {
    InitOk,
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    #[serde(untagged)]
    Data(Data),
}

impl<Data: PartialEq> std::cmp::PartialEq for DataOrInit<Data> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DataOrInit::Data(a), DataOrInit::Data(b)) => a == b,
            (DataOrInit::InitOk, DataOrInit::InitOk) => true,
            (
                DataOrInit::Init {
                    node_id: l_id,
                    node_ids: l_ids,
                },
                DataOrInit::Init {
                    node_ids: r_ids,
                    node_id: r_id,
                },
            ) => l_id == r_id && l_ids == r_ids,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Message<Data> {
    /// The node ID of the sender.
    pub src: Arc<str>,

    /// The node ID of the receiver.
    pub dest: Arc<str>,

    /// The message content, with type defined by enum variant.
    pub body: MessageBody<Data>,
}

impl<Data> Message<DataOrInit<Data>> {
    pub fn into_data<E: std::error::Error + Send + Sync + 'static>(
        self,
    ) -> crate::Result<Message<Data>, E> {
        match self.body.data {
            DataOrInit::Data(data) => Ok(Message {
                src: self.src,
                dest: self.dest,
                body: MessageBody {
                    id: self.body.id,
                    re: self.body.re,
                    data,
                },
            }),
            _ => Err(crate::Error::Internal {
                source: crate::node::InternalError::UnexpectedInit,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_or_init_ser() {
        let data = DataOrInit::Data(42);
        let init = DataOrInit::<u32>::InitOk;
        let data_json = serde_json::to_string(&data).unwrap();
        let init_json = serde_json::to_string(&init).unwrap();
        assert_eq!(data_json, r#"42"#);
        assert_eq!(init_json, r#"{"type":"init_ok"}"#);

        let init = DataOrInit::<u32>::Init {
            node_id: "a".to_string(),
            node_ids: vec!["a".to_string(), "b".to_string()],
        };
        let init_json = serde_json::to_string(&init).unwrap();
        assert_eq!(
            init_json,
            r#"{"type":"init","node_id":"a","node_ids":["a","b"]}"#
        );
    }

    #[test]
    fn test_data_or_init_de() {
        let data_json = r#"42"#;
        let init_json = r#"{"type":"init_ok"}"#;
        let data: DataOrInit<u32> = serde_json::from_str(data_json).unwrap();
        let init: DataOrInit<u32> = serde_json::from_str(init_json).unwrap();
        assert_eq!(data, DataOrInit::Data(42));
        assert_eq!(init, DataOrInit::InitOk);
    }

    #[test]
    fn test_data_or_init_message() {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        #[serde(rename_all = "snake_case", tag = "type")]
        enum MessageData {
            Test { value: u32 },
        }

        let message = Message {
            src: "a".into(),
            dest: "b".into(),
            body: MessageBody {
                id: Some(1),
                re: None,
                data: MessageData::Test { value: 5 },
            },
        };

        let data = DataOrInit::Data(message);
        let init = DataOrInit::<u32>::InitOk;

        let data_json = serde_json::to_string(&data).unwrap();
        let init_json = serde_json::to_string(&init).unwrap();

        let data_de: DataOrInit<Message<MessageData>> = serde_json::from_str(&data_json).unwrap();
        let init_de: DataOrInit<u32> = serde_json::from_str(&init_json).unwrap();

        assert_eq!(data_de, data);
        assert_eq!(init_de, init);
    }

    #[test]
    fn test_data_or_init_deser_complex() {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        #[serde(tag = "type", rename_all = "snake_case")]
        enum MessageData {
            Test { value: u32 },
        }

        let data = r#"{
        "type": "init_ok"
    }"#;

        let init: DataOrInit<MessageData> = serde_json::from_str(data).unwrap();
        assert_eq!(init, DataOrInit::InitOk);

        let data = r#"{
        "type": "init",
        "node_id": "a",
        "node_ids": ["a", "b"]
    }"#;
        let init = serde_json::from_str::<DataOrInit<MessageData>>(data).unwrap();
        assert_eq!(
            init,
            DataOrInit::Init {
                node_id: "a".to_string(),
                node_ids: vec!["a".to_string(), "b".to_string()],
            }
        );

        let data = r#"{
        "type": "test",
        "value": 5
    }"#;
        let init = serde_json::from_str::<DataOrInit<MessageData>>(data).unwrap();
        assert_eq!(init, DataOrInit::Data(MessageData::Test { value: 5 }));

        let data = r#"{
        "src": "a",
        "dest": "b",
        "body": {
            "msg_id": 1,
            "in_reply_to": 2,
            "type": "test",
            "value": 5
        }
    }"#;
        let init = serde_json::from_str::<Message<DataOrInit<MessageData>>>(data).unwrap();
        assert_eq!(
            init,
            Message {
                src: "a".into(),
                dest: "b".into(),
                body: MessageBody {
                    id: Some(1),
                    re: Some(2),
                    data: DataOrInit::Data(MessageData::Test { value: 5 }),
                },
            }
        );

        let serialized = serde_json::to_string(&init).unwrap();
        assert_eq!(
            serialized,
            data.replace(" ", "").replace("\n", "").replace("\t", "")
        );

        let data = r#"{
        "src": "a",
        "dest": "b",
        "body": {
            "msg_id": 1,
            "in_reply_to": 2,
            "type": "init",
            "node_id": "a",
            "node_ids": ["a", "b"]
        }
    }"#;
        let init = serde_json::from_str::<Message<DataOrInit<MessageData>>>(data).unwrap();
        assert_eq!(
            init,
            Message {
                src: "a".into(),
                dest: "b".into(),
                body: MessageBody {
                    id: Some(1),
                    re: Some(2),
                    data: DataOrInit::Init {
                        node_id: "a".to_string(),
                        node_ids: vec!["a".to_string(), "b".to_string()],
                    },
                },
            }
        );

        let serialized = serde_json::to_string(&init).unwrap();
        assert_eq!(
            serialized,
            data.replace(" ", "").replace("\n", "").replace("\t", "")
        );
    }
}
