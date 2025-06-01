use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use serde::{Serialize,Deserialize};
use sqlite::Sqlite;
use sqlx::migrate::{Migrate, MigrateDatabase};
use super::msg::{Message, MessageData, MsgContent, UserInfo};
use sqlx::{sqlite, SqlitePool};
use anyhow::{Result, Error, anyhow};
use chrono::Utc;
use dashmap::DashMap;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageChunk {
    msgs: HashMap<String, Arc<Message>>,
    sorted_msgs: Vec<Arc<Message>>,
    size: usize,
    id: String,
}

impl MessageChunk {
    pub fn new(chunk_size: usize) -> Self {
        Self {
            msgs: HashMap::with_capacity(chunk_size),
            sorted_msgs: Vec::with_capacity(chunk_size),
            size: chunk_size,
            id: Uuid::new_v4().to_string(),
        }
    }
    pub fn from_msgs(msgs: Vec<Arc<Message>>) -> Self {
        let mut chunk = Self {
            msgs: HashMap::with_capacity(msgs.len()),
            sorted_msgs: Vec::with_capacity(msgs.len()),
            size: msgs.len(),
            id: Uuid::new_v4().to_string(),
        };

        for msg in msgs {
            chunk.msgs.insert(msg.inner().id.clone(), msg.clone());
            chunk.sorted_msgs.push(msg);
        }

        chunk.sorted_msgs.sort_by(|a, b|
            a.partial_cmp(&b).unwrap_or(Ordering::Equal));//WARNING: if the value is not valid we just make it equal

        chunk
    }
    pub fn get_sorted_msgs(&self) -> &Vec<Arc<Message>> {
        &self.sorted_msgs
    }
    pub fn get_msg(&self, msg_id: &str) -> Option<Arc<Message>> {
        self.msgs.get(msg_id).and_then(|msg| Some(msg.clone()))
    }
    pub fn push_msg(&mut self, msg: Arc<Message>) -> Result<(),Error>{
        if self.sorted_msgs.len() >= self.size {
            return Err(anyhow!("Chunk has full!"));
        }
        self.msgs.insert(msg.inner().id.clone(), msg.clone());
        self.sorted_msgs.push(msg);
        Ok(())
    }
    pub fn is_full(&self) -> bool{
        self.sorted_msgs.len() >= self.size
    }
   //slow operation, but you will rarely do single deletion
    pub fn delete_msg(&mut self, msg_id: &str) -> Result<(), Error> {
       if self.msgs.remove(msg_id).is_none() {
           return Err(anyhow!("No such message id: {}", msg_id));
       }

       if let Some(pos) = self.sorted_msgs.iter().position(|x| x.inner().id == msg_id) {
           self.sorted_msgs.remove(pos);
       }
       Ok(())
    }
}
pub struct MessageChunkLoader {
    pool: Arc<SqlitePool>,
}
pub struct MessageWriter {
    pool: Arc<SqlitePool>,
    cache: Vec<Arc<Message>>, // 缓存消息，避免频繁访问数据库
}

const SQLITE_MIGRATIONS: &str = r#"
-- 启用外键约束（每条连接都需要执行）
PRAGMA foreign_keys = ON;

-- 用户表
CREATE TABLE IF NOT EXISTS users (
                                     id TEXT PRIMARY KEY,
                                     username TEXT NOT NULL DEFAULT 'Anonymous'
);

-- 会话表
CREATE TABLE IF NOT EXISTS sessions (
                                        id TEXT PRIMARY KEY,
                                        created_at INTEGER NOT NULL,
                                        last_message_at INTEGER NOT NULL
);

-- 消息表
CREATE TABLE IF NOT EXISTS messages (
                                        id TEXT PRIMARY KEY,
                                        content_type TEXT NOT NULL CHECK (content_type IN ('Text', 'Image', 'Emoji')),
                                        content TEXT NOT NULL,
                                        timestamp INTEGER NOT NULL,
                                        sender_id TEXT NOT NULL,
                                        session_id TEXT NOT NULL,
                                        referenced_msg_id TEXT,
                                        is_deleted INTEGER NOT NULL DEFAULT 0 CHECK (is_deleted IN (0, 1)),
                                        created_at TEXT NOT NULL DEFAULT (datetime('now')),

                                        FOREIGN KEY (sender_id) REFERENCES users(id),
                                        FOREIGN KEY (session_id) REFERENCES sessions(id),
                                        FOREIGN KEY (referenced_msg_id) REFERENCES messages(id)
);

-- 优化查询的索引
CREATE INDEX IF NOT EXISTS idx_msg_session_ts_id ON messages(session_id, is_deleted, timestamp DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender_id);
CREATE INDEX IF NOT EXISTS idx_messages_references ON messages(referenced_msg_id)
    WHERE referenced_msg_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_users_id ON users(id);
"#;
pub async fn init_db(db_path: &str) -> Result<SqlitePool, sqlx::Error> {
    let mut need_migration: bool = false;
    if !Sqlite::database_exists(db_path).await.unwrap_or(false) {
        Sqlite::create_database(db_path).await?;
        need_migration = true;
    }

    let pools =
        sqlite::SqlitePoolOptions::new()
        .max_connections(5)
        .test_before_acquire(true)
        .connect(db_path)
        .await?;

    if need_migration {
        sqlx::query(SQLITE_MIGRATIONS).execute(&pools).await?;
    }
    Ok(pools)
}


impl MessageChunkLoader {
    pub fn new(db: Arc<SqlitePool>) -> Self {
        Self {
            pool: db,
        }
    }
    pub async fn load_chunk(&self, session_id: &str, start_timestamp: u64, end_timestamp: u64) -> Result<MessageChunk, sqlx::Error> {
        // 使用query_as宏和自定义Row类型更安全
        #[derive(sqlx::FromRow)]
        struct MessageRow {
            id: String,
            content_type: String,
            content: String,
            timestamp: i64,
            sender_id: String,
            sender_name: String,
            session_id: String,
            referenced_msg_id: Option<String>,
        }

        let rows = sqlx::query_as::<_, MessageRow>(r#"
        SELECT
            m.id,
            m.content_type,
            m.content,
            m.timestamp,
            m.sender_id,
            COALESCE(u.username, 'Unknown') AS sender_name,
            m.session_id,
            m.referenced_msg_id
        FROM messages m
        LEFT JOIN users u ON m.sender_id = u.id
        WHERE m.session_id = ?
            AND m.is_deleted = 0
            AND m.timestamp >= ?
            AND m.timestamp < ?
        ORDER BY m.timestamp DESC, m.id DESC
        "#,)
            .bind(session_id)
            .bind(start_timestamp as i64)
            .bind(end_timestamp as i64)
            .fetch_all(&*self.pool)
            .await?;

        // 转换为Message结构
        let messages = rows.into_iter()
            .map(|row| {
            let content = match row.content_type.as_str() {
                "Text" => MsgContent::Text(row.content),
                "Image" => MsgContent::Image(row.content),
                "Emoji" => MsgContent::Emoji(row.content),
                _ => unreachable!("Invalid content type in database"),
            };
            Arc::new(Message::new(MessageData {
                id: row.id,
                content,
                timestamp: row.timestamp as u64,
                from: UserInfo {
                    id: row.sender_id,
                    username: row.sender_name, // 简化处理，实际应从用户表获取
                },
                session_id: row.session_id,
                references: row.referenced_msg_id,
            }))
        }).collect();

        Ok(MessageChunk::from_msgs(messages))
    }
}
impl MessageWriter {
    pub fn new(db: Arc<SqlitePool>, init_capacity: usize) -> Self {
        Self {
            pool: db,
            cache: Vec::with_capacity(init_capacity),
        }
    }
    pub async fn write(&self, msg: Arc<Message>) -> Result<(), sqlx::Error> {
        let inner = msg.clone().inner().clone();
        sqlx::query(
            r#"INSERT INTO messages (id, content_type, content, timestamp, sender_id, session_id, referenced_msg_id, is_deleted)
                    VALUES (?,?,?,?,?,?,?,0)"#
        )
            .bind(inner.id)
            .bind(match inner.content {
                MsgContent::Text(_) => "Text",
                MsgContent::Image(_) => "Image",
                MsgContent::Emoji(_) => "Emoji",
            })
            .bind(match inner.content {
                MsgContent::Text(text) => text,
                MsgContent::Image(image) => image,
                MsgContent::Emoji(emoji) => emoji,
            })
            .bind(inner.timestamp as i64)
            .bind(inner.from.id)
            .bind(inner.session_id)
            .bind(inner.references)
            .execute(&*self.pool)
            .await?;
        Ok(())
    }
   //for future use
    pub async fn start_flusher(&mut self) {
        let(tx,mut rx) = tokio::sync::mpsc::channel::<Message>(100);
        let pool = self.pool.clone();
        tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(100);
            loop {
                tokio::select! {
                    Some(msg) = rx.recv() => {
                        buffer.push(msg);
                        if buffer.len() >= 100 {
                            match Self::flush(&mut buffer, pool.clone()).await {
                                Ok(_) => {}
                                Err(e) => {}
                            }
                        }
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                        if !buffer.is_empty() {
                            Self::flush(&mut buffer, pool.clone()).await.unwrap_or(());
                        }
                    }
                }
            }
        });
    }
    pub async fn flush(buffer: &mut Vec<Message>, pool: Arc<SqlitePool>) -> Result<(), sqlx::Error> {
        if buffer.is_empty() {
            return Ok(());
        }
        //TODO: prevent the possible SQL Injection by using .bind
        let mut query = "INSERT INTO messages (id, content_type, content, timestamp, sender_id, session_id, referenced_msg_id, is_deleted) VALUES ".to_string();
        let values: Vec<String> = buffer
            .into_iter()
            .map(|msg| {
                let inner = msg.inner().clone();
                format!(
                    "('{}', '{}', '{}', {}, '{}', '{}', {}, 0)",
                    inner.id,
                    match inner.content {
                        MsgContent::Text(_) => "Text",
                        MsgContent::Image(_) => "Image",
                        MsgContent::Emoji(_) => "Emoji",
                    },
                    match inner.content {
                        MsgContent::Text(text) => text,
                        MsgContent::Image(image) => image,
                        MsgContent::Emoji(emoji) => emoji,
                    },
                    inner.timestamp,
                    inner.from.id,
                    inner.session_id,
                    inner.references.unwrap_or("NULL".to_owned()),//TODO: this line is not correct, violate the foreign key
                )
            })
            .collect();
        query.push_str(&values.join(","));

        let mut tx = pool.begin().await?;
        sqlx::query(&query).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}
pub struct MessageSequence {
    session_id: String,
    sequence: VecDeque<Arc<MessageChunk>>,
    last_prev_chunk_id: String,
    id_index: DashMap<String, Arc<MessageChunk>>,

    loader: Arc<MessageChunkLoader>,
    writer: Arc<MessageWriter>,
    append_chunk_size: usize,

}
impl MessageSequence {
    pub fn new(session_id: String, pool: Arc<SqlitePool>, append_chunk_size: usize,init_capacity: usize) ->  Self {
        Self {
            session_id,
            sequence: VecDeque::with_capacity(init_capacity),
            last_prev_chunk_id: String::new(),
            id_index: DashMap::new(),
            loader: Arc::new(MessageChunkLoader::new(pool.clone())),
            writer: Arc::new(MessageWriter::new(pool, init_capacity)),
            append_chunk_size
        }
    }
    pub fn create_index(&mut self, chunk: &Arc<MessageChunk>) {
        for msg in chunk.get_sorted_msgs() {
            self.id_index.insert(msg.inner().id.clone(), Arc::clone(chunk));
        }
    }
    pub fn push_prev(&mut self, chunk: Arc<MessageChunk>) {
        self.create_index(&chunk);
        self.last_prev_chunk_id = chunk.id.clone();
        self.sequence.push_front(chunk);

    }
    pub fn new_chunk(&mut self) {
        self.sequence.push_back(Arc::new(MessageChunk::new(self.append_chunk_size)));
    }
    pub fn get_msg(&self, msg_id: &str) -> Option<Arc<Message>> {
        if let Some(chunk) = self.id_index.get(msg_id) {
            return chunk.value().get_msg(msg_id);
        }
        None
    }
    pub fn clean_index(&mut self, retained_ids: &HashSet<String>) { //CAUTION: potential memory leak
        self.id_index.retain(|id, _| retained_ids.contains(id));
    }
    pub fn unload_prev_chunk(&mut self) {
        if !self.sequence.is_empty() {
            self.sequence.pop_front();
            if let Some(chunk) = self.sequence.front() {
                self.last_prev_chunk_id = chunk.id.clone();
            }else {
                self.last_prev_chunk_id = String::new();
            }
        }
        //we don't clean the index here, batch it later
    }
    pub async fn load_prev(&mut self, timestamp_offset: u64) -> Result<(), Error> {
        if !self.sequence.is_empty() {
            let chunk = self.sequence.front().unwrap();
            match (*chunk.get_sorted_msgs()).get(0) {
                Some(msg) => {
                    let last_timestamp = msg.inner().timestamp;
                    let chunk =  Arc::new(self.loader.load_chunk(
                        self.session_id.as_str(),
                        last_timestamp-timestamp_offset,
                        last_timestamp,
                    ).await?);
                    self.create_index(&chunk);
                    self.push_prev(
                       chunk
                    );
                    Ok(())
                },
                None => {Err(anyhow!("empty chunk, continuous timestamp read failed, can't load prev messages."))}
            }
        }else {
            let now = Utc::now().timestamp() as u64;
            let chunk = Arc::new(
                self.loader.load_chunk(
                    self.session_id.as_str(),
                    now - timestamp_offset,
                    now
                ).await?
            );
            self.create_index(&chunk);
            self.push_prev(
                chunk
            );
            Ok(())
        }
    }
    pub async fn write_message(&mut self, msg: Arc<Message>) -> Result<(),Error>{
        if let Some(chunk) = self.sequence.back() {
            if chunk.is_full() {
                self.new_chunk();
            }
        }else {
            self.new_chunk();
        }

        if let Some(chunk) = Arc::get_mut(self.sequence.back_mut().expect("chunk should exist")) {
            chunk.push_msg(Arc::clone(&msg))?;
        }
        self.writer.write(Arc::clone(&msg)).await?;
        self.id_index.insert(msg.inner().id.clone(), Arc::clone(self.sequence.back().expect("chunk should exist")));
        Ok(())
    }
    pub fn get_msg_sequence(&self) -> Vec<Arc<Message>> {
        let mut msgs: Vec<Arc<Message>> = Vec::new();
        for chunk in self.sequence.iter() {
           msgs.extend_from_slice(chunk.get_sorted_msgs())
        }
        msgs
    }
}



#[cfg(test)]
mod tests {
    use chrono::Utc;
    use crate::aether_msg::msg::MessageBuilder;
    use super::*;
    
    fn prepare_messages() -> Vec<Message> {
        vec![
            // session_1 最新消息
            MessageBuilder::new(
                MsgContent::Text("大家觉得这个方案怎么样？".to_string()),
                "user1".to_string(),
                "session_1".to_string(),
                None,
            )
                .with_id("msg36".to_string())
                .with_timestamp(1710003600)
                .build(),

            MessageBuilder::new(
                MsgContent::Emoji("[good]".to_string()),
                "user3".to_string(),
                "session_1".to_string(),
                Some("msg36".to_string()),
            )
                .with_id("msg35".to_string())
                .with_timestamp(1710003580)
                .build(),

            MessageBuilder::new(
                MsgContent::Image("design.png".to_string()),
                "user2".to_string(),
                "session_1".to_string(),
                None,
            )
                .with_id("msg34".to_string())
                .with_timestamp(1710003560)
                .build(),

            MessageBuilder::new(
                MsgContent::Text("我完成了设计稿".to_string()),
                "user2".to_string(),
                "session_1".to_string(),
                None,
            )
                .with_id("msg33".to_string())
                .with_timestamp(1710003540)
                .build(),

            // session_1 中间段消息
            MessageBuilder::new(
                MsgContent::Text("明天10点开会".to_string()),
                "user1".to_string(),
                "session_1".to_string(),
                None,
            )
                .with_id("msg30".to_string())
                .with_timestamp(1710003000)
                .build(),

            MessageBuilder::new(
                MsgContent::Emoji("[smile]".to_string()),
                "user4".to_string(),
                "session_1".to_string(),
                Some("msg30".to_string()),
            )
                .with_id("msg29".to_string())
                .with_timestamp(1710002980)
                .build(),

            MessageBuilder::new(
                MsgContent::Text("收到".to_string()),
                "user3".to_string(),
                "session_1".to_string(),
                Some("msg30".to_string()),
            )
                .with_id("msg28".to_string())
                .with_timestamp(1710002960)
                .build(),

            MessageBuilder::new(
                MsgContent::Text("我可能迟到".to_string()),
                "user5".to_string(),
                "session_1".to_string(),
                Some("msg30".to_string()),
            )
                .with_id("msg27".to_string())
                .with_timestamp(1710002940)
                .build(),

            MessageBuilder::new(
                MsgContent::Image("schedule.jpg".to_string()),
                "user1".to_string(),
                "session_1".to_string(),
                None,
            )
                .with_id("msg26".to_string())
                .with_timestamp(1710002900)
                .build(),

            // session_1 早期消息
            MessageBuilder::new(
                MsgContent::Text("项目启动".to_string()),
                "user1".to_string(),
                "session_1".to_string(),
                None,
            )
                .with_id("msg18".to_string())
                .with_timestamp(1710001800)
                .build(),

            MessageBuilder::new(
                MsgContent::Text("需要帮助吗？".to_string()),
                "user2".to_string(),
                "session_1".to_string(),
                Some("msg18".to_string()),
            )
                .with_id("msg17".to_string())
                .with_timestamp(1710001700)
                .build(),

            MessageBuilder::new(
                MsgContent::Text("我可以参与".to_string()),
                "user3".to_string(),
                "session_1".to_string(),
                Some("msg17".to_string()),
            )
                .with_id("msg16".to_string())
                .with_timestamp(1710001600)
                .build(),

            MessageBuilder::new(
                MsgContent::Emoji("[share_hands]".to_string()),
                "user4".to_string(),
                "session_1".to_string(),
                Some("msg16".to_string()),
            )
                .with_id("msg15".to_string())
                .with_timestamp(1710001500)
                .build(),

            // 其他会话消息
            MessageBuilder::new(
                MsgContent::Text("其他会话消息".to_string()),
                "user1".to_string(),
                "session_2".to_string(),
                None,
            )
                .with_id("msg50".to_string())
                .with_timestamp(1710002500)
                .build(),

            MessageBuilder::new(
                MsgContent::Text("另一个会话".to_string()),
                "user3".to_string(),
                "session_3".to_string(),
                None,
            )
                .with_id("msg51".to_string())
                .with_timestamp(1710003500)
                .build(),

            // 更多 session_1 消息
            MessageBuilder::new(
                MsgContent::Text("文档在这里".to_string()),
                "user1".to_string(),
                "session_1".to_string(),
                None,
            )
                .with_id("msg14".to_string())
                .with_timestamp(1710001400)
                .build(),

            MessageBuilder::new(
                MsgContent::Image("doc.pdf".to_string()),
                "user1".to_string(),
                "session_1".to_string(),
                Some("msg14".to_string()),
            )
                .with_id("msg13".to_string())
                .with_timestamp(1710001300)
                .build(),

            MessageBuilder::new(
                MsgContent::Text("谢谢分享".to_string()),
                "user2".to_string(),
                "session_1".to_string(),
                Some("msg13".to_string()),
            )
                .with_id("msg12".to_string())
                .with_timestamp(1710001200)
                .build(),

            MessageBuilder::new(
                MsgContent::Emoji("[heart]".to_string()),
                "user3".to_string(),
                "session_1".to_string(),
                Some("msg12".to_string()),
            )
                .with_id("msg11".to_string())
                .with_timestamp(1710001100)
                .build(),

            MessageBuilder::new(
                MsgContent::Text("不客气".to_string()),
                "user1".to_string(),
                "session_1".to_string(),
                Some("msg11".to_string()),
            )
                .with_id("msg10".to_string())
                .with_timestamp(1710001000)
                .build(),
        ]
    }
    async fn prepare_db() -> SqlitePool {
        let pool = init_db("test.db").await.unwrap();

        let insert_query = "INSERT OR IGNORE INTO users (id, username) VALUES (?, ?)";

        sqlx::query(insert_query.clone())
            .bind("user1")
            .bind("Alice")
            .execute(&pool)
            .await.unwrap();
        sqlx::query(insert_query.clone())
            .bind("user2")
            .bind("Bob")
            .execute(&pool)
            .await.unwrap();
        sqlx::query(insert_query.clone())
            .bind("user3")
            .bind("Kein")
            .execute(&pool)
            .await.unwrap();
        sqlx::query(insert_query.clone())
            .bind("user4")
            .bind("Larcie")
            .execute(&pool)
            .await.unwrap();
        sqlx::query(insert_query.clone())
            .bind("user5")
            .bind("NaiLong")
            .execute(&pool)
            .await.unwrap();

        let session_query = "INSERT OR IGNORE INTO sessions (id, created_at, last_message_at) VALUES (?, ?, ?)";
        sqlx::query(session_query.clone())
            .bind("session_1")
            .bind(Utc::now().timestamp())
            .bind(Utc::now().timestamp())
            .execute(&pool)
            .await.unwrap();
        sqlx::query(session_query.clone())
            .bind("session_2")
            .bind(Utc::now().timestamp())
            .bind(Utc::now().timestamp())
            .execute(&pool)
            .await.unwrap();
        sqlx::query(session_query.clone())
            .bind("session_3")
            .bind(Utc::now().timestamp())
            .bind(Utc::now().timestamp())
            .execute(&pool)
            .await.unwrap();


        pool
    }
    #[sqlx::test]
    #[ignore]
    async fn test_init_db() {
        let _ = init_db("test.db").await.unwrap();
    }
    #[sqlx::test]
    #[ignore]
    async fn test_migration() {
        let db = prepare_db().await;
        let _ = MessageChunkLoader::new(Arc::new(db));
    }
    #[sqlx::test]
    async fn test_load_chunk() {
        let db = prepare_db().await;
        let loader = MessageChunkLoader::new(Arc::new(db));
        let chunk = loader.load_chunk("session_1", 1710000000, 1710003600).await.unwrap();
        println!("{:?}", chunk);
        if !chunk.sorted_msgs.is_empty(){
            println!("len:{:?}", chunk.sorted_msgs.len());
            let mut i = chunk.sorted_msgs.get(0).unwrap().clone().inner().timestamp;
            for c in chunk.sorted_msgs {
                assert!(c.clone().inner().timestamp >= i
                ,"last:{}, this:{}",i,c.clone().inner().timestamp);
                i = c.clone().inner().timestamp;
            }
        }
    }
    #[sqlx::test]
    async fn test_write() {
        let db = prepare_db().await;
        let msgs = prepare_messages();
        let mut writer = MessageWriter::new(Arc::new(db),5);
        for msg in msgs {
            writer.write(Arc::new(msg)).await.unwrap();
        }
    }
    #[sqlx::test]
    async fn test_chunk_get_msg() {
        let db = prepare_db().await;
        let loader = MessageChunkLoader::new(Arc::new(db));
        let chunk = loader.load_chunk("session_1", 1710000000, 1710003700).await.unwrap();
        let msg = chunk.get_msg("msg35").unwrap();
        assert_eq!(msg.inner().content, MsgContent::Emoji("[good]".to_string()))
    }
    #[sqlx::test]
    #[should_panic]
    async fn test_chunk_push_msg() {
        let db = prepare_db().await;
        let loader = MessageChunkLoader::new(Arc::new(db));
        let mut chunk = loader.load_chunk("session_1", 1710000000, 1710003700).await.unwrap();
        chunk.push_msg(
            Arc::new(
                MessageBuilder::new(
                    MsgContent::Text("pushed".to_string()),
                    "user1".to_string(),
                    "session_1".to_string(),
                    Some("msg35".to_string()),
                ).build()
            )
        ).expect(format!("Msg size reached {}, which max is {}",chunk.sorted_msgs.len(),chunk.size).as_str());
    }

}


/////
#[cfg(test)]
mod tests_sequence {
    use super::*;
    use crate::aether_msg::msg::{Message, MessageBuilder, MsgContent, UserInfo};
    use chrono::Utc;
    use std::sync::Arc;
    use tokio::runtime::Runtime;

    async fn setup_db() -> SqlitePool {
        let pool = init_db("test.db").await.unwrap();

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
    async fn test_message_sequence_new() {
        let pool = setup_db().await;


        // Create a new MessageSequence
        let sequence = MessageSequence::new("session_1".to_string(), Arc::new(pool.clone()), 2, 10);

        // Check initial state
        assert!(sequence.sequence.is_empty());
        assert_eq!(sequence.last_prev_chunk_id, String::new());
        assert!(sequence.id_index.is_empty());
        cleanup_db(&pool).await;
    }

    #[tokio::test]
    async fn test_message_sequence_write_message() {
        let pool = setup_db().await;


        // Prepare messages
        let messages = vec![
            MessageBuilder::new(
                MsgContent::Text("Hello".to_string()),
                "user1".to_string(),
                "session_1".to_string(),
                None,
            )
                .with_id("msg88888".to_string())
                .with_timestamp(1710000000)
                .build(),
            MessageBuilder::new(
                MsgContent::Text("World".to_string()),
                "user2".to_string(),
                "session_1".to_string(),
                None,
            )
                .with_id("msg99999999".to_string())
                .with_timestamp(1710000010)
                .build(),
        ];

        // Create a new MessageSequence
        let mut sequence = MessageSequence::new("session_1".to_string(), Arc::new(pool.clone()), 2, 10);

        // Write messages to the sequence
        for msg in &messages {
            sequence.write_message(Arc::new(msg.clone())).await.unwrap();
        }

        // Check the content of the retrieved messages
        let retrieved_messages = sequence.get_msg_sequence();
        assert_eq!(retrieved_messages.len(), 2);
        assert_eq!(retrieved_messages[0].inner().content, MsgContent::Text("Hello".to_string()));
        assert_eq!(retrieved_messages[1].inner().content, MsgContent::Text("World".to_string()));
        cleanup_db(&pool).await;
    }

    #[tokio::test]
    async fn test_message_sequence_get_msg() {
        let pool = setup_db().await;


        // Prepare messages
        let messages = vec![
            MessageBuilder::new(
                MsgContent::Text("Hello".to_string()),
                "user1".to_string(),
                "session_1".to_string(),
                None,
            )
                .with_id("msg999999966666".to_string())
                .with_timestamp(1710000000)
                .build(),
        ];

        // Create a new MessageSequence
        let mut sequence = MessageSequence::new("session_1".to_string(), Arc::new(pool.clone()), 2, 10);

        // Write messages to the sequence
        sequence.write_message(Arc::new(messages[0].clone())).await.unwrap();

        // Retrieve a specific message by ID
        let msg = sequence.get_msg("msg999999966666").unwrap();
        assert_eq!(msg.inner().content, MsgContent::Text("Hello".to_string()));
        cleanup_db(&pool).await;
    }

    #[tokio::test]
    async fn test_message_sequence_load_prev() {
        let pool = setup_db().await;


        // Prepare messages
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

        // Create a new MessageSequence
        let mut sequence = MessageSequence::new("session_1".to_string(), Arc::new(pool.clone()), 2, 10);

        // Write messages to the sequence
        for msg in &messages {
            sequence.write_message(Arc::new(msg.clone())).await.unwrap();
        }

        // Load previous chunks
        sequence.load_prev(1000).await.unwrap();

        // Check the content of the retrieved messages
        let retrieved_messages = sequence.get_msg_sequence();
        assert_eq!(retrieved_messages.len(), 2);
        assert_eq!(retrieved_messages[0].inner().content, MsgContent::Text("Hello".to_string()));
        assert_eq!(retrieved_messages[1].inner().content, MsgContent::Text("World".to_string()));
        cleanup_db(&pool).await;
    }

    #[tokio::test]
    async fn test_message_sequence_unload_prev_chunk() {
        let pool = setup_db().await;
        

        // Prepare messages
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

        // Create a new MessageSequence
        let mut sequence = MessageSequence::new("session_1".to_string(), Arc::new(pool.clone()), 1, 10);

        // Write messages to the sequence
        for msg in &messages {
            sequence.write_message(Arc::new(msg.clone())).await.unwrap();
        }

        // Unload previous chunks
        sequence.unload_prev_chunk();

        // Check the content of the retrieved messages
        let retrieved_messages = sequence.get_msg_sequence();
        assert_eq!(retrieved_messages.len(), 1);
        assert_eq!(retrieved_messages[0].inner().content, MsgContent::Text("World".to_string()));
       
        cleanup_db(&pool).await;
    }
}

