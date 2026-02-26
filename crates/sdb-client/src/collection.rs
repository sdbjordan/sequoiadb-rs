use std::sync::Arc;

use sdb_bson::{Document, Value};
use sdb_common::Result;
use sdb_msg::request::{MsgOpDelete, MsgOpInsert, MsgOpQuery, MsgOpUpdate};

use crate::client::{check_reply, send_recv_with_txn, ConnectionPool, SharedTxn};
use crate::cursor::ClientCursor;

/// Handle for a collection on the remote server.
pub struct Collection {
    pool: Arc<ConnectionPool>,
    txn_conn: SharedTxn,
    pub full_name: String,
}

impl Collection {
    pub(crate) fn new(pool: Arc<ConnectionPool>, txn_conn: SharedTxn, full_name: String) -> Self {
        Self { pool, txn_conn, full_name }
    }

    /// Insert a single document.
    pub async fn insert(&self, doc: Document) -> Result<()> {
        self.insert_many(vec![doc]).await
    }

    /// Insert multiple documents.
    pub async fn insert_many(&self, docs: Vec<Document>) -> Result<()> {
        let full_name = self.full_name.clone();
        let reply = send_recv_with_txn(&self.pool, &self.txn_conn, |rid| {
            MsgOpInsert::new(rid, &full_name, docs, 0).encode()
        }).await?;
        check_reply(&reply)
    }

    /// Batch insert — sends all docs in a single MsgOpInsert message.
    /// Alias for insert_many, optimized for bulk operations.
    pub async fn insert_batch(&self, docs: Vec<Document>) -> Result<()> {
        self.insert_many(docs).await
    }

    /// Query the collection. Returns a cursor with GetMore support.
    pub async fn query(&self, condition: Option<Document>) -> Result<ClientCursor> {
        let full_name = self.full_name.clone();
        let reply = send_recv_with_txn(&self.pool, &self.txn_conn, |rid| {
            MsgOpQuery::new(
                rid,
                &full_name,
                condition,
                None,
                None,
                None,
                0,
                -1,
                0,
            )
            .encode()
        }).await?;
        check_reply(&reply)?;
        Ok(ClientCursor::new_with_context(
            reply.docs,
            reply.context_id,
            self.pool.clone(),
            self.txn_conn.clone(),
        ))
    }

    /// Update documents matching `condition` with `modifier`.
    pub async fn update(&self, condition: Document, modifier: Document) -> Result<()> {
        let full_name = self.full_name.clone();
        let reply = send_recv_with_txn(&self.pool, &self.txn_conn, |rid| {
            MsgOpUpdate::new(rid, &full_name, condition, modifier, None, 0).encode()
        }).await?;
        check_reply(&reply)
    }

    /// Delete documents matching `condition`.
    pub async fn delete(&self, condition: Document) -> Result<()> {
        let full_name = self.full_name.clone();
        let reply = send_recv_with_txn(&self.pool, &self.txn_conn, |rid| {
            MsgOpDelete::new(rid, &full_name, condition, None, 0).encode()
        }).await?;
        check_reply(&reply)
    }

    /// Count documents matching `condition` (server-side count).
    pub async fn count(&self, condition: Option<Document>) -> Result<u64> {
        let full_name = self.full_name.clone();
        let reply = send_recv_with_txn(&self.pool, &self.txn_conn, |rid| {
            let mut cond = Document::new();
            cond.insert("Collection", Value::String(full_name));
            if let Some(fc) = condition {
                cond.insert("Condition", Value::Document(fc));
            }
            MsgOpQuery::new(rid, "$count", Some(cond), None, None, None, 0, -1, 0).encode()
        }).await?;
        check_reply(&reply)?;

        // Server returns { "count": N }
        if let Some(doc) = reply.docs.first() {
            match doc.get("count") {
                Some(Value::Int64(n)) => Ok(*n as u64),
                Some(Value::Int32(n)) => Ok(*n as u64),
                _ => Ok(0),
            }
        } else {
            Ok(0)
        }
    }

    /// Run an aggregation pipeline on this collection.
    pub async fn aggregate(&self, pipeline: Vec<Value>) -> Result<Vec<Document>> {
        let full_name = self.full_name.clone();
        let reply = send_recv_with_txn(&self.pool, &self.txn_conn, |rid| {
            let mut cond = Document::new();
            cond.insert("Collection", Value::String(full_name));
            cond.insert("Pipeline", Value::Array(pipeline));
            MsgOpQuery::new(rid, "$aggregate", Some(cond), None, None, None, 0, -1, 0).encode()
        }).await?;
        check_reply(&reply)?;
        Ok(reply.docs)
    }
}
