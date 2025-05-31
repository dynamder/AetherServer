use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize,Deserialize};
use uuid::Uuid;
use chrono::Utc;

#[derive(Clone, Debug,Serialize,Deserialize,PartialEq)]
pub enum MsgContent {
    Text(String),
    Image(String),
    Emoji(String)
}

#[derive(Debug, Clone, Serialize, Deserialize,PartialEq)]
pub struct UserInfo {
    pub username: String,
    pub id: String,
}

impl UserInfo {
    pub fn from_string(id: String) -> Self {
        UserInfo {
            username: "None".to_string(),//TODO: get username from id
            id
        }
    }
}

// 内部数据结构，用于序列化
#[derive(Debug, Clone, Serialize, Deserialize,PartialEq)]
pub struct MessageData {
    pub id: String,
    pub content: MsgContent,
    pub timestamp: u64,
    pub from: UserInfo,
    pub session_id: String,
    pub references: Option<String>,
}

// 外部数据结构，用于运行时
#[derive(Debug, Clone,PartialEq)]
pub struct Message {
    inner: Arc<MessageData>
}

impl Message {
    pub fn new(data: MessageData) -> Self {
        Self {
            inner: Arc::new(data)
        }
    }

    // 获取内部数据的引用
    pub fn inner(&self) -> &MessageData {
        &self.inner
    }
}
impl PartialOrd for Message {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.inner.timestamp.partial_cmp(&other.inner.timestamp) {
            Some(std::cmp::Ordering::Equal) => self.inner.id.partial_cmp(&other.inner.id),
            ord => ord,
        }
    }
}

// 实现序列化
impl Serialize for Message {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.serialize(serializer)
    }
}

// 实现反序列化
impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = MessageData::deserialize(deserializer)?;
        Ok(Message::new(data))
    }
}

// 消息构建器
pub struct MessageBuilder {
    content: MsgContent,
    from: String,
    session_id: String,
    references: Option<String>,
    msg_id: Option<String>,
    timestamp: Option<u64>
}

impl MessageBuilder {
    pub fn new(content: MsgContent, from: String, session_id: String, references: Option<String>) -> Self {
        Self {
            content,
            from,
            session_id,
            references,
            msg_id: None,
            timestamp: None
        }
    }

    pub fn ref_to(mut self, msg_id: String) -> Self {
        self.references = Some(msg_id);
        self
    }
    pub fn with_id(mut self, msg_id: String) -> Self {
        self.msg_id = Some(msg_id);
        self
    }
    pub fn with_timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn build(self) -> Message {
        let data = MessageData {
            id: self.msg_id.unwrap_or(Uuid::new_v4().to_string()),
            content: self.content,
            timestamp: self.timestamp.unwrap_or(Utc::now().timestamp() as u64),
            from: UserInfo::from_string(self.from),
            session_id: self.session_id,
            references: self.references,
        };
        Message::new(data)
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use std::time::Duration;
    use super::*;
    use serde_json;

    #[test]
    fn test_serialization() {
        let msg = MessageBuilder::new(
            MsgContent::Text("Hello".to_string()),
            "user1".to_string(),
            "session1".to_string(),
            None
        ).build();

        // 测试序列化
        let serialized = serde_json::to_string(&msg).unwrap();

        // 测试反序列化
        let deserialized: Message = serde_json::from_str(&serialized).unwrap();

        assert_eq!(msg.inner().clone(), deserialized.inner().clone());
    }

    #[test]
    fn test_cloning() {
        let msg = MessageBuilder::new(
            MsgContent::Text("Hello".to_string()),
            "user1".to_string(),
            "session1".to_string(),
            None
        ).build();

        let msg_clone = msg.clone();

        assert_eq!(msg, msg_clone);
    }
    #[test]
    fn test_partial_ord() {
        let msg1 = MessageBuilder::new(
            MsgContent::Text("Hello".to_string()),
            "user1".to_string(),
            "session1".to_string(),
            None
        ).build();
        sleep(Duration::from_millis(1000));
        let msg2 = MessageBuilder::new(
            MsgContent::Text("World".to_string()),
            "user2".to_string(),
            "session2".to_string(),
            None
        ).build();
        assert!(msg1.clone() < msg2.clone(),
                "{}", format!("{:?} < {:?}",msg1.inner().timestamp.to_string().as_str(),msg2.inner.timestamp.to_string().as_str())
        );
    }
}