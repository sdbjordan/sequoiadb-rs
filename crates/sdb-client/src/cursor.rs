use std::sync::Arc;

use sdb_bson::Document;
use sdb_common::{Result, SdbError};
use sdb_msg::header::MsgHeader;
use sdb_msg::opcode::OpCode;
use sdb_msg::request::MsgOpGetMore;
use tokio::sync::Mutex;

use crate::client::{check_reply, InnerConn};

/// Client-side cursor for iterating query results.
///
/// Supports batched results via GetMore protocol:
/// - `next()` reads from the local buffer (sync)
/// - `fetch_more()` sends GetMore to the server when buffer is exhausted
/// - `collect_all_async()` fetches everything automatically
pub struct ClientCursor {
    buffer: Vec<Document>,
    pos: usize,
    closed: bool,
    context_id: i64,
    conn: Option<Arc<Mutex<InnerConn>>>,
}

impl ClientCursor {
    /// Create a cursor from a server reply (first batch + context_id).
    pub(crate) fn new_with_context(
        docs: Vec<Document>,
        context_id: i64,
        conn: Arc<Mutex<InnerConn>>,
    ) -> Self {
        Self {
            buffer: docs,
            pos: 0,
            closed: false,
            context_id,
            conn: Some(conn),
        }
    }

    /// Create a cursor from a vec of documents (no GetMore support).
    pub fn new(docs: Vec<Document>) -> Self {
        Self {
            buffer: docs,
            pos: 0,
            closed: false,
            context_id: -1,
            conn: None,
        }
    }

    /// Create an empty cursor.
    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    /// Advance to the next document from the local buffer.
    /// Returns `None` when the buffer is exhausted.
    /// Use `has_more()` to check if more batches are available on the server.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Document> {
        if self.closed || self.pos >= self.buffer.len() {
            return None;
        }
        let doc = self.buffer[self.pos].clone();
        self.pos += 1;
        Some(doc)
    }

    /// Check if the server has more batches available.
    pub fn has_more(&self) -> bool {
        !self.closed && self.context_id != -1
    }

    /// Fetch the next batch from the server via GetMore.
    /// Returns `true` if new documents were loaded, `false` if exhausted.
    pub async fn fetch_more(&mut self) -> Result<bool> {
        if self.closed || self.context_id == -1 {
            return Ok(false);
        }

        let conn = self.conn.as_ref().ok_or(SdbError::NetworkError)?;
        let mut conn = conn.lock().await;
        let rid = conn.next_id();

        let msg = MsgOpGetMore {
            header: MsgHeader::new_request(OpCode::GetMoreReq as i32, rid),
            context_id: self.context_id,
            num_to_return: -1, // use server default batch size
        };
        let bytes = msg.encode();
        let reply = conn.send_and_recv(&bytes).await?;
        check_reply(&reply)?;

        self.context_id = reply.context_id;
        self.buffer = reply.docs;
        self.pos = 0;

        if self.buffer.is_empty() {
            self.closed = true;
            Ok(false)
        } else {
            Ok(true)
        }
    }

    /// Collect all remaining documents, fetching additional batches as needed.
    pub async fn collect_all_async(&mut self) -> Result<Vec<Document>> {
        let mut all = Vec::new();
        loop {
            while let Some(doc) = self.next() {
                all.push(doc);
            }
            if !self.fetch_more().await? {
                break;
            }
        }
        Ok(all)
    }

    /// Mark the cursor as closed. If it has a server-side cursor, sends KillContext.
    pub async fn close_async(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;

        if self.context_id != -1 {
            if let Some(ref conn) = self.conn {
                let mut conn = conn.lock().await;
                let rid = conn.next_id();
                let msg = sdb_msg::request::MsgOpKillContexts {
                    header: MsgHeader::new_request(OpCode::KillContextReq as i32, rid),
                    context_ids: vec![self.context_id],
                };
                let bytes = msg.encode();
                // Best-effort: ignore errors
                let _ = conn.send_and_recv(&bytes).await;
            }
            self.context_id = -1;
        }
        Ok(())
    }

    /// Mark the cursor as closed (sync, no server notification).
    pub fn close(&mut self) {
        self.closed = true;
    }

    /// Consume the cursor and collect all remaining documents from the buffer.
    /// Does NOT fetch more from the server — use `collect_all_async()` for that.
    pub fn collect_all(self) -> Vec<Document> {
        if self.closed {
            return Vec::new();
        }
        self.buffer.into_iter().skip(self.pos).collect()
    }
}
