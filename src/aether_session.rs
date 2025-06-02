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

