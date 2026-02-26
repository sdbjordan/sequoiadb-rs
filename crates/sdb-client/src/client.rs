use std::sync::Arc;

use sdb_bson::{Document, Value};
use sdb_common::{Result, SdbError};
use sdb_msg::header::MsgHeader;
use sdb_msg::opcode::OpCode;
use sdb_msg::reply::MsgOpReply;
use sdb_msg::request::MsgOpQuery;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::collection::Collection;
use crate::options::ConnectOptions;

/// Internal TCP connection state, shared between Client and Collections.
pub(crate) struct InnerConn {
    stream: TcpStream,
    next_request_id: u64,
}

impl InnerConn {
    /// Send raw message bytes over the wire.
    pub(crate) async fn send_msg(&mut self, bytes: &[u8]) -> Result<()> {
        self.stream
            .write_all(bytes)
            .await
            .map_err(|_| SdbError::NetworkError)?;
        self.stream
            .flush()
            .await
            .map_err(|_| SdbError::NetworkError)?;
        Ok(())
    }

    /// Read one reply from the wire: 4-byte msg_len → remaining bytes → decode.
    pub(crate) async fn recv_reply(&mut self) -> Result<MsgOpReply> {
        let mut len_buf = [0u8; 4];
        self.stream
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
        self.stream
            .read_exact(&mut full_buf[4..])
            .await
            .map_err(|_| SdbError::NetworkClose)?;

        let header = MsgHeader::decode(&full_buf)?;
        let payload = &full_buf[MsgHeader::SIZE..];
        MsgOpReply::decode(&header, payload)
    }

    /// Send a message and wait for the reply.
    pub(crate) async fn send_and_recv(&mut self, bytes: &[u8]) -> Result<MsgOpReply> {
        self.send_msg(bytes).await?;
        self.recv_reply().await
    }

    /// Get the next monotonic request ID.
    pub(crate) fn next_id(&mut self) -> u64 {
        let id = self.next_request_id;
        self.next_request_id += 1;
        id
    }
}

/// SequoiaDB client — main entry point for the driver.
pub struct Client {
    conn: Arc<Mutex<InnerConn>>,
    options: ConnectOptions,
}

impl Client {
    /// Connect to a SequoiaDB data node.
    pub async fn connect(options: ConnectOptions) -> Result<Self> {
        let addr = format!("{}:{}", options.host, options.port);

        let stream = tokio::time::timeout(
            std::time::Duration::from_millis(options.connect_timeout_ms),
            TcpStream::connect(&addr),
        )
        .await
        .map_err(|_| SdbError::Timeout)?
        .map_err(|_| SdbError::NetworkError)?;

        let inner = InnerConn {
            stream,
            next_request_id: 1,
        };

        Ok(Self {
            conn: Arc::new(Mutex::new(inner)),
            options,
        })
    }

    /// Send a Disconnect message and close the stream.
    pub async fn disconnect(&mut self) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let rid = conn.next_id();
        // Disconnect is a header-only message (no payload, no reply expected).
        let header = MsgHeader::new_request(OpCode::Disconnect as i32, rid);
        let mut buf = Vec::new();
        header.encode(&mut buf);
        // Patch msg_len to header size.
        let len = buf.len() as i32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        // Best-effort send; ignore error if server already closed.
        let _ = conn.send_msg(&buf).await;
        Ok(())
    }

    /// Get a handle to a collection.
    pub fn get_collection(&self, cs_name: &str, cl_name: &str) -> Collection {
        let full_name = format!("{}.{}", cs_name, cl_name);
        Collection::new(self.conn.clone(), full_name)
    }

    /// Check if the client was constructed (v1: always true after connect).
    pub fn is_connected(&self) -> bool {
        true
    }

    /// Return a reference to the connection options.
    pub fn options(&self) -> &ConnectOptions {
        &self.options
    }

    // ── DDL operations ───────────────────────────────────────────────

    pub async fn create_collection_space(&self, name: &str) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let rid = conn.next_id();
        let mut cond = Document::new();
        cond.insert("Name", Value::String(name.into()));
        let bytes = MsgOpQuery::new(
            rid,
            "$create collectionspace",
            Some(cond),
            None, None, None, 0, -1, 0,
        ).encode();
        let reply = conn.send_and_recv(&bytes).await?;
        check_reply(&reply)
    }

    pub async fn drop_collection_space(&self, name: &str) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let rid = conn.next_id();
        let mut cond = Document::new();
        cond.insert("Name", Value::String(name.into()));
        let bytes = MsgOpQuery::new(
            rid,
            "$drop collectionspace",
            Some(cond),
            None, None, None, 0, -1, 0,
        ).encode();
        let reply = conn.send_and_recv(&bytes).await?;
        check_reply(&reply)
    }

    pub async fn create_collection(&self, cs: &str, cl: &str) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let rid = conn.next_id();
        let full_name = format!("{}.{}", cs, cl);
        let mut cond = Document::new();
        cond.insert("Name", Value::String(full_name));
        let bytes = MsgOpQuery::new(
            rid,
            "$create collection",
            Some(cond),
            None, None, None, 0, -1, 0,
        ).encode();
        let reply = conn.send_and_recv(&bytes).await?;
        check_reply(&reply)
    }

    pub async fn drop_collection(&self, cs: &str, cl: &str) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let rid = conn.next_id();
        let full_name = format!("{}.{}", cs, cl);
        let mut cond = Document::new();
        cond.insert("Name", Value::String(full_name));
        let bytes = MsgOpQuery::new(
            rid,
            "$drop collection",
            Some(cond),
            None, None, None, 0, -1, 0,
        ).encode();
        let reply = conn.send_and_recv(&bytes).await?;
        check_reply(&reply)
    }

    pub async fn create_index(
        &self,
        cs: &str,
        cl: &str,
        idx_name: &str,
        key: Document,
        unique: bool,
    ) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let rid = conn.next_id();
        let full_name = format!("{}.{}", cs, cl);

        let mut index_doc = Document::new();
        index_doc.insert("name", Value::String(idx_name.into()));
        index_doc.insert("key", Value::Document(key));
        index_doc.insert("unique", Value::Boolean(unique));

        let mut cond = Document::new();
        cond.insert("Collection", Value::String(full_name));
        cond.insert("Index", Value::Document(index_doc));

        let bytes = MsgOpQuery::new(
            rid,
            "$create index",
            Some(cond),
            None, None, None, 0, -1, 0,
        ).encode();
        let reply = conn.send_and_recv(&bytes).await?;
        check_reply(&reply)
    }

    pub async fn drop_index(&self, cs: &str, cl: &str, idx_name: &str) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let rid = conn.next_id();
        let full_name = format!("{}.{}", cs, cl);

        let mut cond = Document::new();
        cond.insert("Collection", Value::String(full_name));
        cond.insert("Index", Value::String(idx_name.into()));

        let bytes = MsgOpQuery::new(
            rid,
            "$drop index",
            Some(cond),
            None, None, None, 0, -1, 0,
        ).encode();
        let reply = conn.send_and_recv(&bytes).await?;
        check_reply(&reply)
    }

    // ── SQL ─────────────────────────────────────────────────────────

    /// Execute a SQL statement and return result documents.
    pub async fn exec_sql(&self, sql: &str) -> Result<Vec<Document>> {
        let mut conn = self.conn.lock().await;
        let rid = conn.next_id();

        let mut cond = Document::new();
        cond.insert("SQL", Value::String(sql.into()));

        let bytes = MsgOpQuery::new(
            rid, "$sql", Some(cond), None, None, None, 0, -1, 0,
        ).encode();
        let reply = conn.send_and_recv(&bytes).await?;
        check_reply(&reply)?;
        Ok(reply.docs)
    }

    // ── Auth ────────────────────────────────────────────────────────

    /// Authenticate with the server.
    pub async fn authenticate(&self, username: &str, password: &str) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let rid = conn.next_id();

        let mut cond = Document::new();
        cond.insert("User", Value::String(username.into()));
        cond.insert("Passwd", Value::String(password.into()));

        let bytes = MsgOpQuery::new(
            rid, "$authenticate", Some(cond), None, None, None, 0, -1, 0,
        ).encode();
        let reply = conn.send_and_recv(&bytes).await?;
        check_reply(&reply)
    }

    /// Create a user on the server.
    pub async fn create_user(
        &self,
        username: &str,
        password: &str,
        roles: Vec<String>,
    ) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let rid = conn.next_id();

        let role_vals: Vec<Value> = roles.into_iter().map(Value::String).collect();
        let mut cond = Document::new();
        cond.insert("User", Value::String(username.into()));
        cond.insert("Passwd", Value::String(password.into()));
        cond.insert("Roles", Value::Array(role_vals));

        let bytes = MsgOpQuery::new(
            rid, "$create user", Some(cond), None, None, None, 0, -1, 0,
        ).encode();
        let reply = conn.send_and_recv(&bytes).await?;
        check_reply(&reply)
    }

    /// Drop a user from the server.
    pub async fn drop_user(&self, username: &str) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let rid = conn.next_id();

        let mut cond = Document::new();
        cond.insert("User", Value::String(username.into()));

        let bytes = MsgOpQuery::new(
            rid, "$drop user", Some(cond), None, None, None, 0, -1, 0,
        ).encode();
        let reply = conn.send_and_recv(&bytes).await?;
        check_reply(&reply)
    }
}

/// Check a reply's flags; return Ok(()) on success, Err on server error.
pub(crate) fn check_reply(reply: &MsgOpReply) -> Result<()> {
    if reply.flags == 0 {
        Ok(())
    } else {
        Err(i32_to_sdb_error(reply.flags))
    }
}

/// Map a negative i32 error code back to SdbError (inverse of sdb_error_to_i32 in sdb-msg).
fn i32_to_sdb_error(code: i32) -> SdbError {
    match code {
        0 => SdbError::Ok,
        -1 => SdbError::Sys,
        -2 => SdbError::Oom,
        -6 => SdbError::InvalidArg,
        -7 => SdbError::PermissionDenied,
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
        _ => SdbError::Sys,
    }
}
