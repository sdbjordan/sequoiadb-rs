use std::sync::atomic::{AtomicU64, Ordering};

use sdb_bson::{Document, Value};
use sdb_common::{Result, SdbError};
use sdb_msg::header::MsgHeader;
use sdb_msg::opcode::OpCode;
use sdb_msg::reply::MsgOpReply;
use sdb_msg::request::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex as TokioMutex;

/// TCP client for communicating with a DataNode server.
pub struct DataNodeClient {
    addr: String,
    stream: TokioMutex<Option<TcpStream>>,
    next_rid: AtomicU64,
}

impl DataNodeClient {
    pub fn new(addr: String) -> Self {
        Self {
            addr,
            stream: TokioMutex::new(None),
            next_rid: AtomicU64::new(1),
        }
    }

    fn next_id(&self) -> u64 {
        self.next_rid.fetch_add(1, Ordering::Relaxed)
    }

    async fn ensure_connected(&self) -> Result<()> {
        let mut guard = self.stream.lock().await;
        if guard.is_none() {
            let stream = TcpStream::connect(&self.addr)
                .await
                .map_err(|_| SdbError::NetworkError)?;
            *guard = Some(stream);
        }
        Ok(())
    }

    /// Send raw bytes and receive a reply. Ensures connected first.
    async fn send_recv_raw(&self, bytes: &[u8]) -> Result<MsgOpReply> {
        self.ensure_connected().await?;
        let mut guard = self.stream.lock().await;
        let stream = guard.as_mut().ok_or(SdbError::NetworkClose)?;

        stream
            .write_all(bytes)
            .await
            .map_err(|_| SdbError::NetworkError)?;
        stream
            .flush()
            .await
            .map_err(|_| SdbError::NetworkError)?;

        // Read reply
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|_| SdbError::NetworkClose)?;
        let msg_len = i32::from_le_bytes(len_buf) as usize;
        if msg_len < MsgHeader::SIZE {
            return Err(SdbError::InvalidArg);
        }
        let mut full_buf = Vec::with_capacity(msg_len);
        full_buf.extend_from_slice(&len_buf);
        full_buf.resize(msg_len, 0);
        stream
            .read_exact(&mut full_buf[4..])
            .await
            .map_err(|_| SdbError::NetworkClose)?;

        let header = MsgHeader::decode(&full_buf)?;
        let payload = &full_buf[MsgHeader::SIZE..];
        MsgOpReply::decode(&header, payload)
    }

    fn check_reply(reply: &MsgOpReply) -> Result<()> {
        if reply.flags == 0 {
            Ok(())
        } else {
            Err(i32_to_error(reply.flags))
        }
    }

    // ── DDL methods ─────────────────────────────────────────────────

    pub async fn create_collection_space(&self, name: &str) -> Result<()> {
        let rid = self.next_id();
        let mut cond = Document::new();
        cond.insert("Name", Value::String(name.into()));
        let bytes = MsgOpQuery::new(
            rid,
            "$create collectionspace",
            Some(cond),
            None,
            None,
            None,
            0,
            -1,
            0,
        )
        .encode();
        let reply = self.send_recv_raw(&bytes).await?;
        Self::check_reply(&reply)
    }

    pub async fn drop_collection_space(&self, name: &str) -> Result<()> {
        let rid = self.next_id();
        let mut cond = Document::new();
        cond.insert("Name", Value::String(name.into()));
        let bytes = MsgOpQuery::new(
            rid,
            "$drop collectionspace",
            Some(cond),
            None,
            None,
            None,
            0,
            -1,
            0,
        )
        .encode();
        let reply = self.send_recv_raw(&bytes).await?;
        Self::check_reply(&reply)
    }

    pub async fn create_collection(&self, full_name: &str) -> Result<()> {
        let rid = self.next_id();
        let mut cond = Document::new();
        cond.insert("Name", Value::String(full_name.into()));
        let bytes = MsgOpQuery::new(
            rid,
            "$create collection",
            Some(cond),
            None,
            None,
            None,
            0,
            -1,
            0,
        )
        .encode();
        let reply = self.send_recv_raw(&bytes).await?;
        Self::check_reply(&reply)
    }

    pub async fn drop_collection(&self, full_name: &str) -> Result<()> {
        let rid = self.next_id();
        let mut cond = Document::new();
        cond.insert("Name", Value::String(full_name.into()));
        let bytes = MsgOpQuery::new(
            rid,
            "$drop collection",
            Some(cond),
            None,
            None,
            None,
            0,
            -1,
            0,
        )
        .encode();
        let reply = self.send_recv_raw(&bytes).await?;
        Self::check_reply(&reply)
    }

    pub async fn create_index(
        &self,
        collection: &str,
        idx_name: &str,
        key: Document,
        unique: bool,
    ) -> Result<()> {
        let rid = self.next_id();
        let mut index_doc = Document::new();
        index_doc.insert("name", Value::String(idx_name.into()));
        index_doc.insert("key", Value::Document(key));
        index_doc.insert("unique", Value::Boolean(unique));

        let mut cond = Document::new();
        cond.insert("Collection", Value::String(collection.into()));
        cond.insert("Index", Value::Document(index_doc));

        let bytes = MsgOpQuery::new(
            rid,
            "$create index",
            Some(cond),
            None,
            None,
            None,
            0,
            -1,
            0,
        )
        .encode();
        let reply = self.send_recv_raw(&bytes).await?;
        Self::check_reply(&reply)
    }

    pub async fn drop_index(&self, collection: &str, idx_name: &str) -> Result<()> {
        let rid = self.next_id();
        let mut cond = Document::new();
        cond.insert("Collection", Value::String(collection.into()));
        cond.insert("Index", Value::String(idx_name.into()));

        let bytes = MsgOpQuery::new(
            rid,
            "$drop index",
            Some(cond),
            None,
            None,
            None,
            0,
            -1,
            0,
        )
        .encode();
        let reply = self.send_recv_raw(&bytes).await?;
        Self::check_reply(&reply)
    }

    // ── DML methods ─────────────────────────────────────────────────

    pub async fn insert(&self, collection: &str, docs: Vec<Document>) -> Result<()> {
        let rid = self.next_id();
        let msg = MsgOpInsert::new(rid, collection, docs, 0);
        let bytes = msg.encode();
        let reply = self.send_recv_raw(&bytes).await?;
        Self::check_reply(&reply)
    }

    pub async fn update(
        &self,
        collection: &str,
        condition: Document,
        modifier: Document,
    ) -> Result<()> {
        let rid = self.next_id();
        let msg = MsgOpUpdate::new(rid, collection, condition, modifier, None, 0);
        let bytes = msg.encode();
        let reply = self.send_recv_raw(&bytes).await?;
        Self::check_reply(&reply)
    }

    pub async fn delete(&self, collection: &str, condition: Document) -> Result<()> {
        let rid = self.next_id();
        let msg = MsgOpDelete::new(rid, collection, condition, None, 0);
        let bytes = msg.encode();
        let reply = self.send_recv_raw(&bytes).await?;
        Self::check_reply(&reply)
    }

    // ── Query ───────────────────────────────────────────────────────

    /// Execute a query and collect all results, including GetMore batches.
    pub async fn query(
        &self,
        collection: &str,
        condition: Option<Document>,
        selector: Option<Document>,
        order_by: Option<Document>,
        num_to_skip: i64,
        num_to_return: i64,
    ) -> Result<Vec<Document>> {
        let rid = self.next_id();
        let msg = MsgOpQuery::new(
            rid,
            collection,
            condition,
            selector,
            order_by,
            None,
            num_to_skip,
            num_to_return,
            0,
        );
        let bytes = msg.encode();
        let reply = self.send_recv_raw(&bytes).await?;
        Self::check_reply(&reply)?;

        let mut all_docs = reply.docs;
        let mut context_id = reply.context_id;

        // Handle GetMore batching
        while context_id != -1 {
            let gm_rid = self.next_id();
            let gm = MsgOpGetMore {
                header: MsgHeader::new_request(OpCode::GetMoreReq as i32, gm_rid),
                context_id,
                num_to_return: -1,
            };
            let gm_bytes = gm.encode();
            let gm_reply = self.send_recv_raw(&gm_bytes).await?;
            Self::check_reply(&gm_reply)?;
            all_docs.extend(gm_reply.docs);
            context_id = gm_reply.context_id;
        }

        Ok(all_docs)
    }

    // ── Transaction methods ──────────────────────────────────────────

    pub async fn transaction_begin(&self) -> Result<u64> {
        let rid = self.next_id();
        let header = MsgHeader::new_request(OpCode::TransBeginReq as i32, rid);
        let mut buf = Vec::new();
        header.encode(&mut buf);
        let len = buf.len() as i32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        let reply = self.send_recv_raw(&buf).await?;
        Self::check_reply(&reply)?;
        if let Some(doc) = reply.docs.first() {
            match doc.get("txn_id") {
                Some(Value::Int64(n)) => Ok(*n as u64),
                _ => Err(SdbError::TransactionError),
            }
        } else {
            Err(SdbError::TransactionError)
        }
    }

    pub async fn transaction_commit(&self) -> Result<()> {
        let rid = self.next_id();
        let header = MsgHeader::new_request(OpCode::TransCommitReq as i32, rid);
        let mut buf = Vec::new();
        header.encode(&mut buf);
        let len = buf.len() as i32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        let reply = self.send_recv_raw(&buf).await?;
        Self::check_reply(&reply)
    }

    pub async fn transaction_rollback(&self) -> Result<()> {
        let rid = self.next_id();
        let header = MsgHeader::new_request(OpCode::TransRollbackReq as i32, rid);
        let mut buf = Vec::new();
        header.encode(&mut buf);
        let len = buf.len() as i32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        let reply = self.send_recv_raw(&buf).await?;
        Self::check_reply(&reply)
    }

    // ── Count ───────────────────────────────────────────────────────

    pub async fn count(
        &self,
        collection: &str,
        condition: Option<Document>,
    ) -> Result<u64> {
        let rid = self.next_id();
        let mut cond = Document::new();
        cond.insert("Collection", Value::String(collection.into()));
        if let Some(fc) = condition {
            cond.insert("Condition", Value::Document(fc));
        }
        let bytes =
            MsgOpQuery::new(rid, "$count", Some(cond), None, None, None, 0, -1, 0).encode();
        let reply = self.send_recv_raw(&bytes).await?;
        Self::check_reply(&reply)?;

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
}

/// Map a negative i32 error code back to SdbError.
fn i32_to_error(code: i32) -> SdbError {
    match code {
        0 => SdbError::Ok,
        -1 => SdbError::Sys,
        -2 => SdbError::Oom,
        -6 => SdbError::InvalidArg,
        -7 => SdbError::PermissionDenied,
        -9 => SdbError::Eof,
        -15 => SdbError::NetworkError,
        -16 => SdbError::NetworkClose,
        -17 => SdbError::Timeout,
        -22 => SdbError::CollectionAlreadyExists,
        -23 => SdbError::CollectionNotFound,
        -29 => SdbError::NotFound,
        -31 => SdbError::QueryNotFound,
        -33 => SdbError::CollectionSpaceAlreadyExists,
        -34 => SdbError::CollectionSpaceNotFound,
        -38 => SdbError::DuplicateKey,
        -46 => SdbError::IndexAlreadyExists,
        -47 => SdbError::IndexNotFound,
        -179 => SdbError::AuthFailed,
        _ => SdbError::Sys,
    }
}
