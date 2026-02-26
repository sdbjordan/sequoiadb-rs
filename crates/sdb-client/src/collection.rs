use std::sync::Arc;

use sdb_bson::Document;
use sdb_common::Result;
use sdb_msg::request::{MsgOpDelete, MsgOpInsert, MsgOpQuery, MsgOpUpdate};
use tokio::sync::Mutex;

use crate::client::{check_reply, InnerConn};
use crate::cursor::ClientCursor;

/// Handle for a collection on the remote server.
pub struct Collection {
    conn: Arc<Mutex<InnerConn>>,
    pub full_name: String,
}

impl Collection {
    pub(crate) fn new(conn: Arc<Mutex<InnerConn>>, full_name: String) -> Self {
        Self { conn, full_name }
    }

    /// Insert a single document.
    pub async fn insert(&self, doc: Document) -> Result<()> {
        self.insert_many(vec![doc]).await
    }

    /// Insert multiple documents.
    pub async fn insert_many(&self, docs: Vec<Document>) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let rid = conn.next_id();
        let bytes = MsgOpInsert::new(rid, &self.full_name, docs, 0).encode();
        let reply = conn.send_and_recv(&bytes).await?;
        check_reply(&reply)
    }

    /// Query the collection. Returns a cursor over matching documents.
    pub async fn query(&self, condition: Option<Document>) -> Result<ClientCursor> {
        let mut conn = self.conn.lock().await;
        let rid = conn.next_id();
        let bytes = MsgOpQuery::new(
            rid,
            &self.full_name,
            condition,
            None, None, None, 0, -1, 0,
        ).encode();
        let reply = conn.send_and_recv(&bytes).await?;
        check_reply(&reply)?;
        Ok(ClientCursor::new(reply.docs))
    }

    /// Update documents matching `condition` with `modifier`.
    pub async fn update(&self, condition: Document, modifier: Document) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let rid = conn.next_id();
        let bytes = MsgOpUpdate::new(rid, &self.full_name, condition, modifier, None, 0).encode();
        let reply = conn.send_and_recv(&bytes).await?;
        check_reply(&reply)
    }

    /// Delete documents matching `condition`.
    pub async fn delete(&self, condition: Document) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let rid = conn.next_id();
        let bytes = MsgOpDelete::new(rid, &self.full_name, condition, None, 0).encode();
        let reply = conn.send_and_recv(&bytes).await?;
        check_reply(&reply)
    }

    /// Count documents matching `condition` (v1: query + count in buffer).
    pub async fn count(&self, condition: Option<Document>) -> Result<u64> {
        let cursor = self.query(condition).await?;
        Ok(cursor.collect_all().len() as u64)
    }
}
