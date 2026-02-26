use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use sdb_bson::{Document, Value};
use sdb_common::{Result, SdbError};
use sdb_msg::header::MsgHeader;
use sdb_msg::opcode::OpCode;
use sdb_msg::reply::MsgOpReply;
use sdb_msg::request::MsgOpQuery;
use sdb_net::MaybeTlsStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::collection::Collection;
use crate::options::ConnectOptions;

pub(crate) type SharedTxn = Arc<tokio::sync::Mutex<Option<InnerConn>>>;

/// Internal connection state (plain TCP or TLS).
pub(crate) struct InnerConn {
    stream: MaybeTlsStream,
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

    /// Read one reply from the wire: 4-byte msg_len -> remaining bytes -> decode.
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

    /// Check if the underlying TCP connection is still alive.
    /// Uses a non-blocking peek on the underlying stream. Returns false if the peer
    /// has closed the connection or an error occurred.
    pub(crate) fn is_alive(&self) -> bool {
        match &self.stream {
            MaybeTlsStream::Plain(tcp) => {
                // Try to peek — if the peer has closed, this returns Ok(0) or Err.
                // We only check WouldBlock (healthy idle socket).
                match tcp.try_read(&mut [0u8; 1]) {
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => true,
                    Ok(0) => false, // peer closed
                    Ok(_) => true,  // unexpected data, but socket is alive
                    Err(_) => false,
                }
            }
            // For TLS streams, we cannot easily peek without consuming data,
            // so assume they are alive unless we get an error on use.
            #[cfg(feature = "tls")]
            _ => true,
        }
    }
}

// ── Connection Pool ─────────────────────────────────────────────────

pub(crate) struct ConnectionPool {
    options: ConnectOptions,
    idle: std::sync::Mutex<Vec<InnerConn>>,
    size: AtomicUsize,
    max_size: usize,
}

impl ConnectionPool {
    pub(crate) fn new(options: ConnectOptions) -> Self {
        let max_size = options.max_pool_size;
        Self {
            options,
            idle: std::sync::Mutex::new(Vec::new()),
            size: AtomicUsize::new(0),
            max_size,
        }
    }

    pub(crate) async fn acquire(self: &Arc<Self>) -> Result<PoolGuard> {
        // Try to pop an idle connection with health check
        {
            let mut idle = self.idle.lock().unwrap_or_else(|e| e.into_inner());
            while let Some(conn) = idle.pop() {
                if conn.is_alive() {
                    return Ok(PoolGuard {
                        conn: Some(conn),
                        pool: self.clone(),
                    });
                }
                // Connection is dead, decrement pool size
                self.size.fetch_sub(1, Ordering::SeqCst);
            }
        }

        // Try to create a new connection
        let current = self.size.load(Ordering::SeqCst);
        if current >= self.max_size {
            return Err(SdbError::Timeout);
        }

        // Optimistically increment size
        let prev = self.size.fetch_add(1, Ordering::SeqCst);
        if prev >= self.max_size {
            // Another thread beat us; roll back
            self.size.fetch_sub(1, Ordering::SeqCst);
            return Err(SdbError::Timeout);
        }

        match self.create_connection().await {
            Ok(conn) => Ok(PoolGuard {
                conn: Some(conn),
                pool: self.clone(),
            }),
            Err(e) => {
                self.size.fetch_sub(1, Ordering::SeqCst);
                Err(e)
            }
        }
    }

    pub(crate) fn release(&self, conn: InnerConn) {
        let mut idle = self.idle.lock().unwrap_or_else(|e| e.into_inner());
        idle.push(conn);
    }

    async fn create_connection(&self) -> Result<InnerConn> {
        let addr = format!("{}:{}", self.options.host, self.options.port);
        let tcp_stream = tokio::time::timeout(
            std::time::Duration::from_millis(self.options.connect_timeout_ms),
            TcpStream::connect(&addr),
        )
        .await
        .map_err(|_| SdbError::Timeout)?
        .map_err(|_| SdbError::NetworkError)?;

        // Wrap with TLS if enabled
        let stream = if self.options.tls_enabled {
            #[cfg(feature = "tls")]
            {
                use std::sync::Arc as StdArc;
                use tokio_rustls::TlsConnector;

                let mut root_store = tokio_rustls::rustls::RootCertStore::empty();

                // Load CA certificate if provided
                if let Some(ref ca_path) = self.options.tls_ca_path {
                    let ca_data = std::fs::read(ca_path).map_err(|_| SdbError::InvalidConfig)?;
                    let mut reader = std::io::BufReader::new(ca_data.as_slice());
                    let certs = rustls_pemfile::certs(&mut reader)
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(|_| SdbError::InvalidConfig)?;
                    for cert in certs {
                        root_store.add(cert).map_err(|_| SdbError::InvalidConfig)?;
                    }
                }

                let config = tokio_rustls::rustls::ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_no_client_auth();

                let connector = TlsConnector::from(StdArc::new(config));
                let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from(
                    self.options.host.clone(),
                )
                .map_err(|_| SdbError::InvalidConfig)?;

                let tls_stream = connector
                    .connect(server_name, tcp_stream)
                    .await
                    .map_err(|_| SdbError::NetworkError)?;
                MaybeTlsStream::Tls(tls_stream)
            }
            #[cfg(not(feature = "tls"))]
            {
                tracing::warn!("TLS requested but feature not enabled, using plain TCP");
                MaybeTlsStream::Plain(tcp_stream)
            }
        } else {
            MaybeTlsStream::Plain(tcp_stream)
        };

        let mut conn = InnerConn {
            stream,
            next_request_id: 1,
        };

        // Auto-auth if credentials provided
        if let (Some(ref user), Some(ref pass)) = (&self.options.username, &self.options.password) {
            let rid = conn.next_id();
            let mut cond = Document::new();
            cond.insert("User", Value::String(user.clone()));
            cond.insert("Passwd", Value::String(pass.clone()));
            let bytes = MsgOpQuery::new(
                rid,
                "$authenticate",
                Some(cond),
                None,
                None,
                None,
                0,
                -1,
                0,
            )
            .encode();
            let reply = conn.send_and_recv(&bytes).await?;
            check_reply(&reply)?;
        }

        Ok(conn)
    }
}

// ── Pool Guard ──────────────────────────────────────────────────────

pub(crate) struct PoolGuard {
    conn: Option<InnerConn>,
    pool: Arc<ConnectionPool>,
}

impl std::ops::Deref for PoolGuard {
    type Target = InnerConn;
    fn deref(&self) -> &InnerConn {
        self.conn.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PoolGuard {
    fn deref_mut(&mut self) -> &mut InnerConn {
        self.conn.as_mut().unwrap()
    }
}

impl PoolGuard {
    /// Take the inner connection out, preventing it from being returned to the pool on drop.
    pub(crate) fn take(mut self) -> InnerConn {
        self.conn.take().unwrap()
    }
}

impl Drop for PoolGuard {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.release(conn);
        }
    }
}

// ── Client ──────────────────────────────────────────────────────────

/// Helper: send a message either through a transaction connection or a pool connection.
pub(crate) async fn send_recv_with_txn(
    pool: &Arc<ConnectionPool>,
    txn_conn: &SharedTxn,
    build_msg: impl FnOnce(u64) -> Vec<u8>,
) -> Result<MsgOpReply> {
    let mut txn = txn_conn.lock().await;
    if let Some(ref mut conn) = *txn {
        let rid = conn.next_id();
        let bytes = build_msg(rid);
        conn.send_and_recv(&bytes).await
    } else {
        drop(txn);
        let mut guard = pool.acquire().await?;
        let rid = guard.next_id();
        let bytes = build_msg(rid);
        guard.send_and_recv(&bytes).await
    }
}

/// SequoiaDB client — main entry point for the driver.
pub struct Client {
    pool: Arc<ConnectionPool>,
    options: ConnectOptions,
    txn_conn: SharedTxn,
}

impl Client {
    /// Connect to a SequoiaDB data node.
    pub async fn connect(options: ConnectOptions) -> Result<Self> {
        let pool = Arc::new(ConnectionPool::new(options.clone()));

        // Acquire one connection to verify connectivity, then release
        {
            let _guard = pool.acquire().await?;
        }

        Ok(Self {
            pool,
            options,
            txn_conn: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    /// Send a Disconnect message (best-effort) and drop the pool.
    pub async fn disconnect(&mut self) -> Result<()> {
        // Acquire a connection and send Disconnect header
        if let Ok(mut guard) = self.pool.acquire().await {
            let rid = guard.next_id();
            let header = MsgHeader::new_request(OpCode::Disconnect as i32, rid);
            let mut buf = Vec::new();
            header.encode(&mut buf);
            let len = buf.len() as i32;
            buf[0..4].copy_from_slice(&len.to_le_bytes());
            let _ = guard.send_msg(&buf).await;
        }
        Ok(())
    }

    /// Get a handle to a collection.
    pub fn get_collection(&self, cs_name: &str, cl_name: &str) -> Collection {
        let full_name = format!("{}.{}", cs_name, cl_name);
        Collection::new(self.pool.clone(), self.txn_conn.clone(), full_name)
    }

    /// Check if the client was constructed (v1: always true after connect).
    pub fn is_connected(&self) -> bool {
        self.pool.size.load(Ordering::SeqCst) > 0
    }

    /// Return a reference to the connection options.
    pub fn options(&self) -> &ConnectOptions {
        &self.options
    }

    // ── DDL operations ───────────────────────────────────────────────

    pub async fn create_collection_space(&self, name: &str) -> Result<()> {
        let name = name.to_string();
        let reply = send_recv_with_txn(&self.pool, &self.txn_conn, |rid| {
            let mut cond = Document::new();
            cond.insert("Name", Value::String(name));
            MsgOpQuery::new(rid, "$create collectionspace", Some(cond), None, None, None, 0, -1, 0).encode()
        }).await?;
        check_reply(&reply)
    }

    pub async fn drop_collection_space(&self, name: &str) -> Result<()> {
        let name = name.to_string();
        let reply = send_recv_with_txn(&self.pool, &self.txn_conn, |rid| {
            let mut cond = Document::new();
            cond.insert("Name", Value::String(name));
            MsgOpQuery::new(rid, "$drop collectionspace", Some(cond), None, None, None, 0, -1, 0).encode()
        }).await?;
        check_reply(&reply)
    }

    pub async fn create_collection(&self, cs: &str, cl: &str) -> Result<()> {
        let full_name = format!("{}.{}", cs, cl);
        let reply = send_recv_with_txn(&self.pool, &self.txn_conn, |rid| {
            let mut cond = Document::new();
            cond.insert("Name", Value::String(full_name));
            MsgOpQuery::new(rid, "$create collection", Some(cond), None, None, None, 0, -1, 0).encode()
        }).await?;
        check_reply(&reply)
    }

    pub async fn drop_collection(&self, cs: &str, cl: &str) -> Result<()> {
        let full_name = format!("{}.{}", cs, cl);
        let reply = send_recv_with_txn(&self.pool, &self.txn_conn, |rid| {
            let mut cond = Document::new();
            cond.insert("Name", Value::String(full_name));
            MsgOpQuery::new(rid, "$drop collection", Some(cond), None, None, None, 0, -1, 0).encode()
        }).await?;
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
        let full_name = format!("{}.{}", cs, cl);
        let idx_name = idx_name.to_string();
        let reply = send_recv_with_txn(&self.pool, &self.txn_conn, |rid| {
            let mut index_doc = Document::new();
            index_doc.insert("name", Value::String(idx_name));
            index_doc.insert("key", Value::Document(key));
            index_doc.insert("unique", Value::Boolean(unique));

            let mut cond = Document::new();
            cond.insert("Collection", Value::String(full_name));
            cond.insert("Index", Value::Document(index_doc));

            MsgOpQuery::new(rid, "$create index", Some(cond), None, None, None, 0, -1, 0).encode()
        }).await?;
        check_reply(&reply)
    }

    pub async fn drop_index(&self, cs: &str, cl: &str, idx_name: &str) -> Result<()> {
        let full_name = format!("{}.{}", cs, cl);
        let idx_name = idx_name.to_string();
        let reply = send_recv_with_txn(&self.pool, &self.txn_conn, |rid| {
            let mut cond = Document::new();
            cond.insert("Collection", Value::String(full_name));
            cond.insert("Index", Value::String(idx_name));

            MsgOpQuery::new(rid, "$drop index", Some(cond), None, None, None, 0, -1, 0).encode()
        }).await?;
        check_reply(&reply)
    }

    // ── SQL ─────────────────────────────────────────────────────────

    /// Execute a SQL statement and return result documents.
    pub async fn exec_sql(&self, sql: &str) -> Result<Vec<Document>> {
        let sql = sql.to_string();
        let reply = send_recv_with_txn(&self.pool, &self.txn_conn, |rid| {
            let mut cond = Document::new();
            cond.insert("SQL", Value::String(sql));
            MsgOpQuery::new(rid, "$sql", Some(cond), None, None, None, 0, -1, 0).encode()
        }).await?;
        check_reply(&reply)?;
        Ok(reply.docs)
    }

    // ── Auth ────────────────────────────────────────────────────────

    /// Authenticate with the server.
    pub async fn authenticate(&self, username: &str, password: &str) -> Result<()> {
        let username = username.to_string();
        let password = password.to_string();
        let reply = send_recv_with_txn(&self.pool, &self.txn_conn, |rid| {
            let mut cond = Document::new();
            cond.insert("User", Value::String(username));
            cond.insert("Passwd", Value::String(password));
            MsgOpQuery::new(rid, "$authenticate", Some(cond), None, None, None, 0, -1, 0).encode()
        }).await?;
        check_reply(&reply)
    }

    /// Create a user on the server.
    pub async fn create_user(
        &self,
        username: &str,
        password: &str,
        roles: Vec<String>,
    ) -> Result<()> {
        let username = username.to_string();
        let password = password.to_string();
        let reply = send_recv_with_txn(&self.pool, &self.txn_conn, |rid| {
            let role_vals: Vec<Value> = roles.into_iter().map(Value::String).collect();
            let mut cond = Document::new();
            cond.insert("User", Value::String(username));
            cond.insert("Passwd", Value::String(password));
            cond.insert("Roles", Value::Array(role_vals));
            MsgOpQuery::new(rid, "$create user", Some(cond), None, None, None, 0, -1, 0).encode()
        }).await?;
        check_reply(&reply)
    }

    /// Drop a user from the server.
    pub async fn drop_user(&self, username: &str) -> Result<()> {
        let username = username.to_string();
        let reply = send_recv_with_txn(&self.pool, &self.txn_conn, |rid| {
            let mut cond = Document::new();
            cond.insert("User", Value::String(username));
            MsgOpQuery::new(rid, "$drop user", Some(cond), None, None, None, 0, -1, 0).encode()
        }).await?;
        check_reply(&reply)
    }

    // ── Transactions ────────────────────────────────────────────────

    pub async fn transaction_begin(&self) -> Result<()> {
        // Acquire a connection from pool and hold it for the txn
        let guard = self.pool.acquire().await?;
        let mut conn = guard.take();

        // Send TransBeginReq (header-only message)
        let rid = conn.next_id();
        let header = MsgHeader::new_request(OpCode::TransBeginReq as i32, rid);
        let mut buf = Vec::new();
        header.encode(&mut buf);
        let len = buf.len() as i32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        let reply = conn.send_and_recv(&buf).await?;
        check_reply(&reply)?;

        // Store the connection for txn use
        *self.txn_conn.lock().await = Some(conn);
        Ok(())
    }

    pub async fn transaction_commit(&self) -> Result<()> {
        let mut txn = self.txn_conn.lock().await;
        let mut conn = txn.take().ok_or(SdbError::TransactionError)?;

        let rid = conn.next_id();
        let header = MsgHeader::new_request(OpCode::TransCommitReq as i32, rid);
        let mut buf = Vec::new();
        header.encode(&mut buf);
        let len = buf.len() as i32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        let reply = conn.send_and_recv(&buf).await?;

        // Return connection to pool regardless of result
        self.pool.release(conn);
        check_reply(&reply)
    }

    pub async fn transaction_rollback(&self) -> Result<()> {
        let mut txn = self.txn_conn.lock().await;
        let mut conn = txn.take().ok_or(SdbError::TransactionError)?;

        let rid = conn.next_id();
        let header = MsgHeader::new_request(OpCode::TransRollbackReq as i32, rid);
        let mut buf = Vec::new();
        header.encode(&mut buf);
        let len = buf.len() as i32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        let reply = conn.send_and_recv(&buf).await?;

        self.pool.release(conn);
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
        -179 => SdbError::AuthFailed,
        _ => SdbError::Sys,
    }
}
