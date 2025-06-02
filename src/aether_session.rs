use crate::aether_msg::{msg,msg_manage};
use crate::aether_msg::msg::UserInfo;

use std::sync::Arc;
use chrono::Utc;
use sqlx::SqlitePool;
use crate::aether_msg::msg::{Message, MessageBuilder,MsgContent};
use crate::aether_msg::msg_manage::{MessageSequence};
use anyhow::Error;
use uuid::Uuid;

#[derive(Debug)]
pub struct Session {
    id: String,
    created_at: u64,
    last_message_at: u64,
    participants: Vec<String>, // List of participant IDs
    message_sequence: MessageSequence,
}

impl Session {
    pub fn new(
        participants: Vec<String>,
        pool: Arc<SqlitePool>,
        append_chunk_size: usize,
        init_capacity: usize,
    ) -> Self {
        let id = Uuid::new_v4().to_string();
        let created_at = Utc::now().timestamp() as u64;
        let last_message_at = created_at;
        let message_sequence = MessageSequence::new(id.clone(),pool, append_chunk_size, init_capacity);
        Self {
            id,
            created_at,
            last_message_at,
            participants,
            message_sequence,
        }
    }

    pub fn add_participant(&mut self, participant_id: String) {
        self.participants.push(participant_id);
    }

    pub fn remove_participant(&mut self, participant_id: String) -> bool {
        if let Some(pos) = self.participants.iter().position(|id| id == &participant_id) {
            self.participants.remove(pos);
            true
        } else {
            false
        }
    }

    pub async fn add_message(&mut self, message: Arc<Message>) -> Result<(), Error> {
        self.message_sequence.write_message(Arc::clone(&message)).await?;
        self.last_message_at = message.inner().timestamp;
        Ok(())
    }

    pub async fn get_message(&self, msg_id: &str) -> Option<Arc<Message>> {
        self.message_sequence.get_msg(msg_id).await
    }

    pub async fn get_messages_seq(&self) -> Vec<Arc<Message>> { // get the messages in the sequence, order by timestamp
        self.message_sequence.get_msg_sequence().await
    }

    pub async fn load_previous_messages(&mut self, timestamp_offset: u64) -> Result<(), Error> {
        self.message_sequence.load_prev(timestamp_offset).await?;
        Ok(())
    }

    pub async fn unload_previous_chunk(&mut self) {
        self.message_sequence.unload_prev_chunk().await;
    }

    pub fn update_last_message_at(&mut self, timestamp: u64) {
        self.last_message_at = timestamp;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aether_msg::msg::{Message, MessageBuilder, MsgContent, UserInfo};
    use crate::aether_msg::msg_manage::{MessageSequence};
    use chrono::Utc;
    use sqlx::SqlitePool;
    use std::sync::Arc;
    use tokio::runtime::Runtime;

    async fn setup_db() -> SqlitePool {
        let pool = msg_manage::init_db("test.db").await.unwrap();

        let insert_query = "INSERT OR IGNORE INTO users (id, username) VALUES (?, ?)";

        sqlx::query(insert_query.clone())
            .bind("user1")
            .bind("Alice")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(insert_query.clone())
            .bind("user2")
            .bind("Bob")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(insert_query.clone())
            .bind("user3")
            .bind("Kein")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(insert_query.clone())
            .bind("user4")
            .bind("Larcie")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(insert_query.clone())
            .bind("user5")
            .bind("NaiLong")
            .execute(&pool)
            .await
            .unwrap();

        let session_query = "INSERT OR IGNORE INTO sessions (id, created_at, last_message_at) VALUES (?, ?, ?)";
        sqlx::query(session_query.clone())
            .bind("session_1")
            .bind(Utc::now().timestamp())
            .bind(Utc::now().timestamp())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(session_query.clone())
            .bind("session_2")
            .bind(Utc::now().timestamp())
            .bind(Utc::now().timestamp())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(session_query.clone())
            .bind("session_3")
            .bind(Utc::now().timestamp())
            .bind(Utc::now().timestamp())
            .execute(&pool)
            .await
            .unwrap();

        pool
    }

    async fn cleanup_db(pool: &SqlitePool) {
        sqlx::query("DELETE FROM messages")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM sessions")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM users")
            .execute(pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_session_new() {
        let pool = setup_db().await;
        let participants = vec!["user1".to_string(), "user2".to_string()];

        let session = Session::new(participants.clone(), Arc::new(pool.clone()), 2, 10);

        assert_eq!(session.participants, participants);
        assert_eq!(session.id.len(), 36); // UUID length
        assert_eq!(session.created_at, session.last_message_at);
        cleanup_db(&pool).await;
    }

    #[tokio::test]
    async fn test_session_add_participant() {
        let pool = setup_db().await;
        let participants = vec!["user1".to_string(), "user2".to_string()];

        let mut session = Session::new(participants.clone(), Arc::new(pool.clone()), 2, 10);

        session.add_participant("user3".to_string());

        assert_eq!(session.participants, vec!["user1".to_string(), "user2".to_string(), "user3".to_string()]);
        cleanup_db(&pool).await;
    }

    #[tokio::test]
    async fn test_session_remove_participant() {
        let pool = setup_db().await;
        let participants = vec!["user1".to_string(), "user2".to_string()];

        let mut session = Session::new(participants.clone(), Arc::new(pool.clone()), 2, 10);

        assert!(session.remove_participant("user1".to_string()));
        assert!(!session.remove_participant("user1".to_string()));
        assert_eq!(session.participants, vec!["user2".to_string()]);
        cleanup_db(&pool).await;
    }

    #[tokio::test]
    async fn test_session_add_message() {
        let pool = setup_db().await;
        let participants = vec!["user1".to_string(), "user2".to_string()];

        let mut session = Session::new(participants, Arc::new(pool.clone()), 2, 10);

        let message = MessageBuilder::new(
            MsgContent::Text("Hello".to_string()),
            "user1".to_string(),
            "session_1".to_string(),
            None,
        )
            .with_id("msg1".to_string())
            .with_timestamp(1710000000)
            .build();

        session.add_message(Arc::new(message)).await.unwrap();

        let retrieved_message = session.get_message("msg1").await.unwrap();
        assert_eq!(retrieved_message.inner().content, MsgContent::Text("Hello".to_string()));
        cleanup_db(&pool).await;
    }

    #[tokio::test]
    async fn test_session_get_messages_seq() {
        let pool = setup_db().await;
        let participants = vec!["user1".to_string(), "user2".to_string()];

        let mut session = Session::new(participants, Arc::new(pool.clone()), 2, 10);

        let messages = vec![
            MessageBuilder::new(
                MsgContent::Text("Hello".to_string()),
                "user1".to_string(),
                "session_1".to_string(),
                None,
            )
                .with_id("msg1".to_string())
                .with_timestamp(1710000000)
                .build(),
            MessageBuilder::new(
                MsgContent::Text("World".to_string()),
                "user2".to_string(),
                "session_1".to_string(),
                None,
            )
                .with_id("msg2".to_string())
                .with_timestamp(1710000010)
                .build(),
        ];

        for msg in &messages {
            session.add_message(Arc::new(msg.clone())).await.unwrap();
        }

        let retrieved_messages = session.get_messages_seq().await;
        assert_eq!(retrieved_messages.len(), 2);
        assert_eq!(retrieved_messages[0].inner().content, MsgContent::Text("Hello".to_string()));
        assert_eq!(retrieved_messages[1].inner().content, MsgContent::Text("World".to_string()));
        cleanup_db(&pool).await;
    }

    #[tokio::test]
    async fn test_session_load_previous_messages() {
        let pool = setup_db().await;
        let participants = vec!["user1".to_string(), "user2".to_string()];

        let mut session = Session::new(participants, Arc::new(pool.clone()), 2, 10);

        let messages = vec![
            MessageBuilder::new(
                MsgContent::Text("Hello".to_string()),
                "user1".to_string(),
                "session_1".to_string(),
                None,
            )
                .with_id("msg1".to_string())
                .with_timestamp(1710000000)
                .build(),
            MessageBuilder::new(
                MsgContent::Text("World".to_string()),
                "user2".to_string(),
                "session_1".to_string(),
                None,
            )
                .with_id("msg2".to_string())
                .with_timestamp(1710000010)
                .build(),
        ];

        for msg in &messages {
            session.add_message(Arc::new(msg.clone())).await.unwrap();
        }

        session.load_previous_messages(1000).await.unwrap();

        let retrieved_messages = session.get_messages_seq().await;
        assert_eq!(retrieved_messages.len(), 2);
        assert_eq!(retrieved_messages[0].inner().content, MsgContent::Text("Hello".to_string()));
        assert_eq!(retrieved_messages[1].inner().content, MsgContent::Text("World".to_string()));
        cleanup_db(&pool).await;
    }

    #[tokio::test]
    async fn test_session_unload_previous_chunk() {
        let pool = setup_db().await;
        let participants = vec!["user1".to_string(), "user2".to_string()];

        let mut session = Session::new(participants, Arc::new(pool.clone()), 1, 10);

        let messages = vec![
            MessageBuilder::new(
                MsgContent::Text("Hello".to_string()),
                "user1".to_string(),
                "session_1".to_string(),
                None,
            )
                .with_id("msg1".to_string())
                .with_timestamp(1710000000)
                .build(),
            MessageBuilder::new(
                MsgContent::Text("World".to_string()),
                "user2".to_string(),
                "session_1".to_string(),
                None,
            )
                .with_id("msg2".to_string())
                .with_timestamp(1710000010)
                .build(),
        ];

        for msg in &messages {
            session.add_message(Arc::new(msg.clone())).await.unwrap();
        }

        session.unload_previous_chunk().await;

        let retrieved_messages = session.get_messages_seq().await;
        assert_eq!(retrieved_messages.len(), 1);
        assert_eq!(retrieved_messages[0].inner().content, MsgContent::Text("World".to_string()));
        cleanup_db(&pool).await;
    }
}
