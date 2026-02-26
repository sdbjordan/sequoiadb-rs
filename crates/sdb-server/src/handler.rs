use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, Mutex as StdMutex, RwLock};

use async_trait::async_trait;
use sdb_aggr::{Pipeline, Stage, StageType};
use sdb_auth::AuthManager;
use sdb_bson::{Document, Value};
use sdb_cat::CatalogManager;
use sdb_cls::election::{ElectionManager, ElectionState};
use sdb_cls::ReplicationAgent;
use sdb_common::{NodeAddress, ReadPreference, RecordId, Result, SdbError};
use sdb_dps::{LogOp, LogRecord, WriteAheadLog};
use sdb_mon::{Metrics, Snapshot, SnapshotType};
use sdb_mth::{Matcher, Modifier};
use sdb_msg::header::MsgHeader;
use sdb_msg::opcode::OpCode;
use sdb_msg::reply::MsgOpReply;
use sdb_msg::request::*;
use sdb_net::Connection;
use sdb_net::MessageHandler;
use sdb_opt::Optimizer;
use sdb_rtn::{CollectionHandle, DefaultExecutor, Executor};
use sdb_sql::{QueryGraph, SqlParser};

use sdb_dps::TransactionManager;

use crate::cursor_manager::CursorManager;

/// A buffered DML operation within a transaction.
enum BufferedOp {
    Insert { collection: String, docs: Vec<Document> },
    Update { collection: String, condition: Document, modifier: Document },
    Delete { collection: String, condition: Document },
}

/// Per-connection transaction buffer.
struct TxnBuffer {
    txn_id: u64,
    ops: Vec<BufferedOp>,
}

/// Data node message handler — dispatches CRUD, DDL, auth, aggregate, and SQL commands.
pub struct DataNodeHandler {
    catalog: Arc<RwLock<CatalogManager>>,
    auth: Arc<RwLock<AuthManager>>,
    cursors: Arc<StdMutex<CursorManager>>,
    wal: Option<Arc<Mutex<WriteAheadLog>>>,
    sessions: Arc<StdMutex<HashMap<SocketAddr, bool>>>,
    txn_mgr: Arc<StdMutex<TransactionManager>>,
    txn_buffers: Arc<StdMutex<HashMap<SocketAddr, TxnBuffer>>>,
    metrics: Arc<Metrics>,
    election: Option<Arc<StdMutex<ElectionManager>>>,
    repl_agent: Option<Arc<tokio::sync::Mutex<ReplicationAgent>>>,
    wal_path: Option<String>,
    read_prefs: Arc<StdMutex<HashMap<SocketAddr, ReadPreference>>>,
}

impl DataNodeHandler {
    pub fn new(catalog: Arc<RwLock<CatalogManager>>) -> Self {
        Self {
            catalog,
            auth: Arc::new(RwLock::new(AuthManager::new())),
            cursors: Arc::new(StdMutex::new(CursorManager::new())),
            wal: None,
            sessions: Arc::new(StdMutex::new(HashMap::new())),
            txn_mgr: Arc::new(StdMutex::new(TransactionManager::new())),
            txn_buffers: Arc::new(StdMutex::new(HashMap::new())),
            metrics: Arc::new(Metrics::new()),
            election: None,
            repl_agent: None,
            wal_path: None,
            read_prefs: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    pub fn new_with_wal(
        catalog: Arc<RwLock<CatalogManager>>,
        wal: Arc<Mutex<WriteAheadLog>>,
    ) -> Self {
        Self {
            catalog,
            auth: Arc::new(RwLock::new(AuthManager::new())),
            cursors: Arc::new(StdMutex::new(CursorManager::new())),
            wal: Some(wal),
            sessions: Arc::new(StdMutex::new(HashMap::new())),
            txn_mgr: Arc::new(StdMutex::new(TransactionManager::new())),
            txn_buffers: Arc::new(StdMutex::new(HashMap::new())),
            metrics: Arc::new(Metrics::new()),
            election: None,
            repl_agent: None,
            wal_path: None,
            read_prefs: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    pub fn new_with_auth(
        catalog: Arc<RwLock<CatalogManager>>,
        auth: Arc<RwLock<AuthManager>>,
    ) -> Self {
        Self {
            catalog,
            auth,
            cursors: Arc::new(StdMutex::new(CursorManager::new())),
            wal: None,
            sessions: Arc::new(StdMutex::new(HashMap::new())),
            txn_mgr: Arc::new(StdMutex::new(TransactionManager::new())),
            txn_buffers: Arc::new(StdMutex::new(HashMap::new())),
            metrics: Arc::new(Metrics::new()),
            election: None,
            repl_agent: None,
            wal_path: None,
            read_prefs: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    /// Create a handler with WAL + replication support.
    pub fn new_with_replication(
        catalog: Arc<RwLock<CatalogManager>>,
        wal: Arc<Mutex<WriteAheadLog>>,
        local_node: NodeAddress,
        peers: Vec<(NodeAddress, String)>,
        wal_path: String,
    ) -> Self {
        let mut em = ElectionManager::new(local_node);
        let mut agent = ReplicationAgent::new(local_node);

        for (peer_addr, tcp_addr) in &peers {
            em.add_peer(*peer_addr);
            agent.add_replica(*peer_addr);
            agent.peer_addrs.insert(peer_addr.node_id, tcp_addr.clone());
        }

        // Add jitter based on node_id to reduce split votes
        let jitter_ms = local_node.node_id as u64 * 200;
        em.heartbeat_timeout =
            std::time::Duration::from_millis(3000 + jitter_ms);

        Self {
            catalog,
            auth: Arc::new(RwLock::new(AuthManager::new())),
            cursors: Arc::new(StdMutex::new(CursorManager::new())),
            wal: Some(wal),
            sessions: Arc::new(StdMutex::new(HashMap::new())),
            txn_mgr: Arc::new(StdMutex::new(TransactionManager::new())),
            txn_buffers: Arc::new(StdMutex::new(HashMap::new())),
            metrics: Arc::new(Metrics::new()),
            election: Some(Arc::new(StdMutex::new(em))),
            repl_agent: Some(Arc::new(tokio::sync::Mutex::new(agent))),
            wal_path: Some(wal_path),
            read_prefs: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    /// Check if this node is primary (or if replication is not enabled, always Ok).
    pub fn check_primary(&self) -> Result<()> {
        match &self.election {
            None => Ok(()),
            Some(em) => {
                let em = em.lock().unwrap_or_else(|e| e.into_inner());
                if em.is_primary() {
                    Ok(())
                } else {
                    Err(SdbError::NotPrimary)
                }
            }
        }
    }

    /// Check if this connection is allowed to read (considering read preference).
    /// Primary always allows reads. Secondaries allow reads if read preference permits.
    pub fn check_read_allowed(&self, addr: &SocketAddr) -> Result<()> {
        match &self.election {
            None => Ok(()), // no replication = always allow
            Some(em) => {
                let em = em.lock().unwrap_or_else(|e| e.into_inner());
                if em.is_primary() {
                    return Ok(()); // primary always allows reads
                }
                // We're a secondary — check read preference
                let pref = self.get_read_preference(addr);
                if pref.allows_secondary() {
                    Ok(())
                } else {
                    Err(SdbError::NotPrimary)
                }
            }
        }
    }

    /// Get the read preference for a connection.
    fn get_read_preference(&self, addr: &SocketAddr) -> ReadPreference {
        self.read_prefs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(addr)
            .copied()
            .unwrap_or(ReadPreference::Primary)
    }

    /// Set the read preference for a connection.
    fn set_read_preference(&self, addr: &SocketAddr, pref: ReadPreference) {
        self.read_prefs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(*addr, pref);
    }

    /// Get the election manager (for tests).
    pub fn election(&self) -> &Option<Arc<StdMutex<ElectionManager>>> {
        &self.election
    }

    /// Get the replication agent (for tests/dynamic membership).
    pub fn repl_agent(&self) -> &Option<Arc<tokio::sync::Mutex<ReplicationAgent>>> {
        &self.repl_agent
    }

    /// Get a reference to the metrics for snapshot queries.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    // ── Auth enforcement helpers ────────────────────────────────────

    fn auth_required(&self) -> bool {
        let auth = self.auth.read().unwrap_or_else(|e| e.into_inner());
        !auth.list_users().is_empty()
    }

    fn is_authenticated(&self, addr: &SocketAddr) -> bool {
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(addr)
            .copied()
            .unwrap_or(false)
    }

    fn set_authenticated(&self, addr: &SocketAddr, value: bool) {
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(*addr, value);
    }

    fn check_auth(&self, addr: &SocketAddr) -> Result<()> {
        if !self.auth_required() {
            return Ok(());
        }
        if self.is_authenticated(addr) {
            Ok(())
        } else {
            Err(SdbError::AuthFailed)
        }
    }

    /// Write a WAL record and flush. No-op if WAL is not configured.
    fn wal_log(&self, op: LogOp, data: Vec<u8>) -> Result<()> {
        if let Some(ref wal) = self.wal {
            let mut wal = wal.lock().map_err(|_| SdbError::Sys)?;
            let mut record = LogRecord {
                lsn: 0,
                prev_lsn: 0,
                txn_id: 0,
                op,
                data,
            };
            wal.append(&mut record)?;
            wal.flush()?;
        }
        Ok(())
    }

    /// Write multiple WAL records and flush once. No-op if WAL is not configured.
    fn wal_log_batch(&self, records: Vec<(LogOp, Vec<u8>)>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        if let Some(ref wal) = self.wal {
            let mut wal = wal.lock().map_err(|_| SdbError::Sys)?;
            for (op, data) in records {
                let mut record = LogRecord {
                    lsn: 0,
                    prev_lsn: 0,
                    txn_id: 0,
                    op,
                    data,
                };
                wal.append(&mut record)?;
            }
            wal.flush()?;
        }
        Ok(())
    }

    /// Direct access to catalog — used by CoordNodeHandler for in-process routing.
    pub fn catalog(&self) -> &Arc<RwLock<CatalogManager>> {
        &self.catalog
    }

    // ── Command dispatch ────────────────────────────────────────────

    async fn handle_query(
        &self,
        header: &MsgHeader,
        payload: &[u8],
        addr: &SocketAddr,
    ) -> MsgOpReply {
        let query = match MsgOpQuery::decode(header, payload) {
            Ok(q) => q,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        if query.name.starts_with('$') {
            self.handle_command(header, &query, addr).await
        } else {
            // Data query requires auth
            if let Err(e) = self.check_auth(addr) {
                return MsgOpReply::error(header.opcode, header.request_id, &e);
            }
            // Read preference: allow reads on secondaries if preference permits
            if let Err(e) = self.check_read_allowed(addr) {
                return MsgOpReply::error(header.opcode, header.request_id, &e);
            }
            self.handle_data_query(header, &query).await
        }
    }

    async fn handle_command(
        &self,
        header: &MsgHeader,
        query: &MsgOpQuery,
        addr: &SocketAddr,
    ) -> MsgOpReply {
        let result = match query.name.as_str() {
            // Auth commands — always allowed (no auth check)
            "$authenticate" => self.cmd_authenticate(query, addr),
            "$create user" => {
                // Allow without auth if no users exist (bootstrap)
                if self.auth_required() {
                    if let Err(e) = self.check_auth(addr) {
                        return MsgOpReply::error(header.opcode, header.request_id, &e);
                    }
                }
                self.cmd_create_user(query)
            }
            "$drop user" => {
                if let Err(e) = self.check_auth(addr) {
                    return MsgOpReply::error(header.opcode, header.request_id, &e);
                }
                self.cmd_drop_user(query)
            }
            // All other commands require auth
            _ => {
                if let Err(e) = self.check_auth(addr) {
                    return MsgOpReply::error(header.opcode, header.request_id, &e);
                }
                match query.name.as_str() {
                    "$create collectionspace" => self.cmd_create_cs(query),
                    "$drop collectionspace" => self.cmd_drop_cs(query),
                    "$create collection" => self.cmd_create_cl(query),
                    "$drop collection" => self.cmd_drop_cl(query),
                    "$create index" => self.cmd_create_index(query),
                    "$drop index" => self.cmd_drop_index(query),
                    "$aggregate" => self.cmd_aggregate(query),
                    "$sql" => return self.handle_sql_command(header, query).await,
                    "$count" => self.cmd_count(query),
                    "$snapshot database" => self.cmd_snapshot(SnapshotType::Database),
                    "$snapshot sessions" => self.cmd_snapshot(SnapshotType::Sessions),
                    "$snapshot collections" => self.cmd_snapshot(SnapshotType::Collections),
                    "$snapshot health" => self.cmd_snapshot(SnapshotType::Health),
                    "$set read preference" => self.cmd_set_read_preference(query, addr),
                    "$get read preference" => self.cmd_get_read_preference(addr),
                    "$add member" => self.cmd_add_member(query),
                    "$remove member" => self.cmd_remove_member(query),
                    "$get members" => self.cmd_get_members(),
                    _ => Err(SdbError::InvalidArg),
                }
            }
        };
        match result {
            Ok(docs) => {
                let mut reply = MsgOpReply::ok(header.opcode, header.request_id, docs);
                reply.context_id = -1;
                reply
            }
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    async fn handle_sql_command(&self, header: &MsgHeader, query: &MsgOpQuery) -> MsgOpReply {
        let result = self.cmd_sql(query).await;
        match result {
            Ok(docs) => {
                let mut reply = MsgOpReply::ok(header.opcode, header.request_id, docs);
                reply.context_id = -1;
                reply
            }
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    async fn handle_data_query(&self, header: &MsgHeader, query: &MsgOpQuery) -> MsgOpReply {
        let result = self.execute_query(query).await;
        match result {
            Ok(docs) => {
                self.metrics.inc_query();
                // Use cursor manager for batching
                let mut cursors = self.cursors.lock().unwrap();
                let (batch, context_id) = cursors.create_cursor(docs);
                let mut reply = MsgOpReply::ok(header.opcode, header.request_id, batch);
                reply.context_id = context_id;
                reply
            }
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    async fn execute_query(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let (cs, cl) = parse_collection_name(&query.name)?;

        let (plan, executor) = {
            let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;
            let stats = catalog.collection_stats(cs, cl)?;
            let (storage, indexes) = catalog.collection_handle(cs, cl)?;

            let optimizer = Optimizer::new();
            let plan = optimizer.optimize(
                &query.name,
                query.condition.as_ref(),
                query.selector.as_ref(),
                query.order_by.as_ref(),
                query.num_to_skip,
                query.num_to_return,
                &stats,
            )?;

            let mut executor = DefaultExecutor::new();
            executor.register(
                query.name.clone(),
                CollectionHandle { storage, indexes },
            );
            (plan, executor)
        };

        let mut cursor = executor.execute(&plan).await?;

        let mut docs = Vec::new();
        while let Ok(Some(doc)) = cursor.next() {
            docs.push(doc);
        }
        Ok(docs)
    }

    // ── Transaction handlers ─────────────────────────────────────────

    fn handle_trans_begin(&self, header: &MsgHeader, addr: &SocketAddr) -> MsgOpReply {
        let mut mgr = self.txn_mgr.lock().unwrap_or_else(|e| e.into_inner());
        match mgr.begin() {
            Ok(txn_id) => {
                let mut buffers = self.txn_buffers.lock().unwrap_or_else(|e| e.into_inner());
                buffers.insert(*addr, TxnBuffer { txn_id, ops: Vec::new() });
                let mut doc = Document::new();
                doc.insert("txn_id", Value::Int64(txn_id as i64));
                MsgOpReply::ok(header.opcode, header.request_id, vec![doc])
            }
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    fn handle_trans_commit(&self, header: &MsgHeader, addr: &SocketAddr) -> MsgOpReply {
        let buffer = {
            let mut buffers = self.txn_buffers.lock().unwrap_or_else(|e| e.into_inner());
            buffers.remove(addr)
        };
        let buffer = match buffer {
            Some(b) => b,
            None => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::TransactionError),
        };

        match self.apply_txn_buffer(&buffer) {
            Ok(()) => {
                let mut mgr = self.txn_mgr.lock().unwrap_or_else(|e| e.into_inner());
                let _ = mgr.commit(buffer.txn_id);
                MsgOpReply::ok(header.opcode, header.request_id, vec![])
            }
            Err(e) => {
                let mut mgr = self.txn_mgr.lock().unwrap_or_else(|e| e.into_inner());
                let _ = mgr.abort(buffer.txn_id);
                MsgOpReply::error(header.opcode, header.request_id, &e)
            }
        }
    }

    fn handle_trans_rollback(&self, header: &MsgHeader, addr: &SocketAddr) -> MsgOpReply {
        let buffer = {
            let mut buffers = self.txn_buffers.lock().unwrap_or_else(|e| e.into_inner());
            buffers.remove(addr)
        };
        if let Some(buffer) = buffer {
            let mut mgr = self.txn_mgr.lock().unwrap_or_else(|e| e.into_inner());
            let _ = mgr.abort(buffer.txn_id);
        }
        MsgOpReply::ok(header.opcode, header.request_id, vec![])
    }

    fn apply_txn_buffer(&self, buffer: &TxnBuffer) -> Result<()> {
        // WAL: TxnBegin
        self.wal_log(LogOp::TxnBegin, Vec::new())?;

        for op in &buffer.ops {
            match op {
                BufferedOp::Insert { collection, docs } => {
                    let (cs, cl) = parse_collection_name(collection)?;
                    // WAL log inserts
                    let wal_records: Vec<(LogOp, Vec<u8>)> = docs.iter().filter_map(|doc| {
                        let mut wal_doc = Document::new();
                        wal_doc.insert("c", Value::String(collection.clone()));
                        wal_doc.insert("op", Value::String("I".into()));
                        wal_doc.insert("doc", Value::Document(doc.clone()));
                        wal_doc.to_bytes().ok().map(|data| (LogOp::Insert, data))
                    }).collect();
                    self.wal_log_batch(wal_records)?;

                    let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;
                    for doc in docs {
                        catalog.insert_document(cs, cl, doc)?;
                    }
                }
                BufferedOp::Update { collection, condition, modifier } => {
                    let (cs, cl) = parse_collection_name(collection)?;
                    let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;
                    let (storage, _) = catalog.collection_handle(cs, cl)?;
                    let rows = storage.scan();
                    let matcher = Matcher::new(condition.clone())?;
                    let modifier_obj = Modifier::new(modifier.clone())?;

                    let matching: Vec<RecordId> = rows.iter()
                        .filter(|(_, doc)| matcher.matches(doc).unwrap_or(false))
                        .map(|(rid, _)| *rid)
                        .collect();

                    let mut updates: Vec<(RecordId, Document, Document)> = Vec::new();
                    for rid in &matching {
                        let old_doc = storage.find(*rid)?;
                        let new_doc = modifier_obj.modify(&old_doc)?;
                        updates.push((*rid, old_doc, new_doc));
                    }

                    let wal_records: Vec<(LogOp, Vec<u8>)> = updates.iter().filter_map(|(_, old_doc, new_doc)| {
                        let mut wal_doc = Document::new();
                        wal_doc.insert("c", Value::String(collection.clone()));
                        wal_doc.insert("op", Value::String("U".into()));
                        wal_doc.insert("old", Value::Document(old_doc.clone()));
                        wal_doc.insert("new", Value::Document(new_doc.clone()));
                        wal_doc.to_bytes().ok().map(|data| (LogOp::Update, data))
                    }).collect();
                    self.wal_log_batch(wal_records)?;

                    for (rid, _, new_doc) in &updates {
                        catalog.update_document(cs, cl, *rid, new_doc)?;
                    }
                }
                BufferedOp::Delete { collection, condition } => {
                    let (cs, cl) = parse_collection_name(collection)?;
                    let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;
                    let (storage, _) = catalog.collection_handle(cs, cl)?;
                    let rows = storage.scan();
                    let matcher = Matcher::new(condition.clone())?;

                    let matching: Vec<(RecordId, Document)> = rows.iter()
                        .filter(|(_, doc)| matcher.matches(doc).unwrap_or(false))
                        .map(|(rid, doc)| (*rid, doc.clone()))
                        .collect();

                    let wal_records: Vec<(LogOp, Vec<u8>)> = matching.iter().filter_map(|(_, doc)| {
                        let mut wal_doc = Document::new();
                        wal_doc.insert("c", Value::String(collection.clone()));
                        wal_doc.insert("op", Value::String("D".into()));
                        wal_doc.insert("doc", Value::Document(doc.clone()));
                        wal_doc.to_bytes().ok().map(|data| (LogOp::Delete, data))
                    }).collect();
                    self.wal_log_batch(wal_records)?;

                    for (rid, _) in &matching {
                        catalog.delete_document(cs, cl, *rid)?;
                    }
                }
            }
        }

        // WAL: TxnCommit
        self.wal_log(LogOp::TxnCommit, Vec::new())?;
        Ok(())
    }

    // ── DML handlers ────────────────────────────────────────────────

    fn handle_insert(&self, header: &MsgHeader, payload: &[u8], addr: &SocketAddr) -> MsgOpReply {
        // Reject writes on secondary
        if let Err(e) = self.check_primary() {
            return MsgOpReply::error(header.opcode, header.request_id, &e);
        }

        let msg = match MsgOpInsert::decode(header, payload) {
            Ok(m) => m,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        // Check if in transaction — buffer instead of executing
        {
            let mut buffers = self.txn_buffers.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(buffer) = buffers.get_mut(addr) {
                buffer.ops.push(BufferedOp::Insert {
                    collection: msg.name.clone(),
                    docs: msg.docs.clone(),
                });
                self.metrics.inc_insert();
                return MsgOpReply::ok(header.opcode, header.request_id, vec![]);
            }
        }

        // Normal (non-txn) execution
        let result = (|| -> Result<()> {
            let (cs, cl) = parse_collection_name(&msg.name)?;

            // Write WAL records before modifying catalog
            let wal_records: Vec<(LogOp, Vec<u8>)> = msg
                .docs
                .iter()
                .filter_map(|doc| {
                    let mut wal_doc = Document::new();
                    wal_doc.insert("c", Value::String(msg.name.clone()));
                    wal_doc.insert("op", Value::String("I".into()));
                    wal_doc.insert("doc", Value::Document(doc.clone()));
                    wal_doc.to_bytes().ok().map(|data| (LogOp::Insert, data))
                })
                .collect();
            self.wal_log_batch(wal_records)?;

            let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;
            for doc in &msg.docs {
                catalog.insert_document(cs, cl, doc)?;
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                self.metrics.inc_insert();
                MsgOpReply::ok(header.opcode, header.request_id, vec![])
            }
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    fn handle_update(&self, header: &MsgHeader, payload: &[u8], addr: &SocketAddr) -> MsgOpReply {
        if let Err(e) = self.check_primary() {
            return MsgOpReply::error(header.opcode, header.request_id, &e);
        }
        let msg = match MsgOpUpdate::decode(header, payload) {
            Ok(m) => m,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        // Check if in transaction — buffer instead of executing
        {
            let mut buffers = self.txn_buffers.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(buffer) = buffers.get_mut(addr) {
                buffer.ops.push(BufferedOp::Update {
                    collection: msg.name.clone(),
                    condition: msg.condition.clone(),
                    modifier: msg.modifier.clone(),
                });
                self.metrics.inc_update();
                return MsgOpReply::ok(header.opcode, header.request_id, vec![]);
            }
        }

        // Normal (non-txn) execution
        let result = (|| -> Result<i32> {
            let (cs, cl) = parse_collection_name(&msg.name)?;
            let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;

            let (storage, _indexes) = catalog.collection_handle(cs, cl)?;
            let rows = storage.scan();
            let matcher = Matcher::new(msg.condition.clone())?;
            let modifier = Modifier::new(msg.modifier.clone())?;

            let matching: Vec<RecordId> = rows
                .iter()
                .filter(|(_, doc)| matcher.matches(doc).unwrap_or(false))
                .map(|(rid, _)| *rid)
                .collect();

            // Compute old/new docs and write WAL records before modifying
            let mut updates: Vec<(RecordId, Document, Document)> = Vec::new();
            for rid in &matching {
                let old_doc = storage.find(*rid)?;
                let new_doc = modifier.modify(&old_doc)?;
                updates.push((*rid, old_doc, new_doc));
            }

            let wal_records: Vec<(LogOp, Vec<u8>)> = updates
                .iter()
                .filter_map(|(_rid, old_doc, new_doc)| {
                    let mut wal_doc = Document::new();
                    wal_doc.insert("c", Value::String(msg.name.clone()));
                    wal_doc.insert("op", Value::String("U".into()));
                    wal_doc.insert("old", Value::Document(old_doc.clone()));
                    wal_doc.insert("new", Value::Document(new_doc.clone()));
                    wal_doc.to_bytes().ok().map(|data| (LogOp::Update, data))
                })
                .collect();
            self.wal_log_batch(wal_records)?;

            let mut count = 0i32;
            for (rid, _old_doc, new_doc) in &updates {
                catalog.update_document(cs, cl, *rid, new_doc)?;
                count += 1;
            }
            Ok(count)
        })();

        match result {
            Ok(_count) => {
                self.metrics.inc_update();
                MsgOpReply::ok(header.opcode, header.request_id, vec![])
            }
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    fn handle_delete(&self, header: &MsgHeader, payload: &[u8], addr: &SocketAddr) -> MsgOpReply {
        if let Err(e) = self.check_primary() {
            return MsgOpReply::error(header.opcode, header.request_id, &e);
        }
        let msg = match MsgOpDelete::decode(header, payload) {
            Ok(m) => m,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        // Check if in transaction — buffer instead of executing
        {
            let mut buffers = self.txn_buffers.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(buffer) = buffers.get_mut(addr) {
                buffer.ops.push(BufferedOp::Delete {
                    collection: msg.name.clone(),
                    condition: msg.condition.clone(),
                });
                self.metrics.inc_delete();
                return MsgOpReply::ok(header.opcode, header.request_id, vec![]);
            }
        }

        // Normal (non-txn) execution
        let result = (|| -> Result<i32> {
            let (cs, cl) = parse_collection_name(&msg.name)?;
            let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;

            let (storage, _indexes) = catalog.collection_handle(cs, cl)?;
            let rows = storage.scan();
            let matcher = Matcher::new(msg.condition.clone())?;

            let matching: Vec<(RecordId, Document)> = rows
                .iter()
                .filter(|(_, doc)| matcher.matches(doc).unwrap_or(false))
                .map(|(rid, doc)| (*rid, doc.clone()))
                .collect();

            // Write WAL records before modifying catalog
            let wal_records: Vec<(LogOp, Vec<u8>)> = matching
                .iter()
                .filter_map(|(_rid, doc)| {
                    let mut wal_doc = Document::new();
                    wal_doc.insert("c", Value::String(msg.name.clone()));
                    wal_doc.insert("op", Value::String("D".into()));
                    wal_doc.insert("doc", Value::Document(doc.clone()));
                    wal_doc.to_bytes().ok().map(|data| (LogOp::Delete, data))
                })
                .collect();
            self.wal_log_batch(wal_records)?;

            let mut count = 0i32;
            for (rid, _doc) in &matching {
                catalog.delete_document(cs, cl, *rid)?;
                count += 1;
            }
            Ok(count)
        })();

        match result {
            Ok(_count) => {
                self.metrics.inc_delete();
                MsgOpReply::ok(header.opcode, header.request_id, vec![])
            }
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    fn handle_get_more(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let msg = match MsgOpGetMore::decode(header, payload) {
            Ok(m) => m,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        let mut cursors = self.cursors.lock().unwrap();
        match cursors.get_more(msg.context_id, msg.num_to_return) {
            Some((batch, exhausted)) => {
                let mut reply = MsgOpReply::ok(header.opcode, header.request_id, batch);
                reply.context_id = if exhausted { -1 } else { msg.context_id };
                reply
            }
            None => {
                MsgOpReply::error(header.opcode, header.request_id, &SdbError::QueryNotFound)
            }
        }
    }

    fn handle_kill_context(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let msg = match MsgOpKillContexts::decode(header, payload) {
            Ok(m) => m,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        let mut cursors = self.cursors.lock().unwrap();
        for ctx_id in &msg.context_ids {
            cursors.kill(*ctx_id);
        }
        MsgOpReply::ok(header.opcode, header.request_id, vec![])
    }

    // ── DDL commands ────────────────────────────────────────────────

    fn cmd_create_cs(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let name = get_string_field(query.condition.as_ref(), "Name")?;

        // WAL: log CreateCS before modifying catalog
        let mut wal_doc = Document::new();
        wal_doc.insert("op", Value::String("CCS".into()));
        wal_doc.insert("name", Value::String(name.clone()));
        let data = wal_doc.to_bytes().map_err(|_| SdbError::InvalidBson)?;
        self.wal_log(LogOp::CollectionCreate, data)?;

        let mut catalog = self.catalog.write().map_err(|_| SdbError::Sys)?;
        catalog.create_collection_space(&name)?;
        Ok(vec![])
    }

    fn cmd_drop_cs(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let name = get_string_field(query.condition.as_ref(), "Name")?;

        // WAL: log DropCS before modifying catalog
        let mut wal_doc = Document::new();
        wal_doc.insert("op", Value::String("DCS".into()));
        wal_doc.insert("name", Value::String(name.clone()));
        let data = wal_doc.to_bytes().map_err(|_| SdbError::InvalidBson)?;
        self.wal_log(LogOp::CollectionDrop, data)?;

        let mut catalog = self.catalog.write().map_err(|_| SdbError::Sys)?;
        catalog.drop_collection_space(&name)?;
        Ok(vec![])
    }

    fn cmd_create_cl(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let full_name = get_string_field(query.condition.as_ref(), "Name")?;
        let (cs, cl) = parse_collection_name(&full_name)?;

        // WAL: log CreateCL before modifying catalog
        let mut wal_doc = Document::new();
        wal_doc.insert("op", Value::String("CCL".into()));
        wal_doc.insert("name", Value::String(full_name.clone()));
        let data = wal_doc.to_bytes().map_err(|_| SdbError::InvalidBson)?;
        self.wal_log(LogOp::CollectionCreate, data)?;

        let mut catalog = self.catalog.write().map_err(|_| SdbError::Sys)?;
        catalog.create_collection(cs, cl)?;
        Ok(vec![])
    }

    fn cmd_drop_cl(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let full_name = get_string_field(query.condition.as_ref(), "Name")?;
        let (cs, cl) = parse_collection_name(&full_name)?;

        // WAL: log DropCL before modifying catalog
        let mut wal_doc = Document::new();
        wal_doc.insert("op", Value::String("DCL".into()));
        wal_doc.insert("name", Value::String(full_name.clone()));
        let data = wal_doc.to_bytes().map_err(|_| SdbError::InvalidBson)?;
        self.wal_log(LogOp::CollectionDrop, data)?;

        let mut catalog = self.catalog.write().map_err(|_| SdbError::Sys)?;
        catalog.drop_collection(cs, cl)?;
        Ok(vec![])
    }

    fn cmd_create_index(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;

        let full_name = match cond.get("Collection") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let (cs, cl) = parse_collection_name(&full_name)?;

        let index_doc = match cond.get("Index") {
            Some(Value::Document(d)) => d,
            _ => return Err(SdbError::InvalidArg),
        };

        let idx_name = match index_doc.get("name") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };

        let key_pattern = match index_doc.get("key") {
            Some(Value::Document(d)) => d.clone(),
            _ => return Err(SdbError::InvalidArg),
        };

        let unique = match index_doc.get("unique") {
            Some(Value::Boolean(b)) => *b,
            _ => false,
        };

        // WAL: log CreateIndex before modifying catalog
        let mut wal_doc = Document::new();
        wal_doc.insert("op", Value::String("CIX".into()));
        wal_doc.insert("c", Value::String(full_name.clone()));
        wal_doc.insert("name", Value::String(idx_name.clone()));
        wal_doc.insert("key", Value::Document(key_pattern.clone()));
        wal_doc.insert("unique", Value::Boolean(unique));
        let data = wal_doc.to_bytes().map_err(|_| SdbError::InvalidBson)?;
        self.wal_log(LogOp::IndexCreate, data)?;

        let mut catalog = self.catalog.write().map_err(|_| SdbError::Sys)?;
        catalog.create_index(cs, cl, &idx_name, key_pattern, unique)?;
        Ok(vec![])
    }

    fn cmd_drop_index(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;

        let full_name = match cond.get("Collection") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let (cs, cl) = parse_collection_name(&full_name)?;

        let idx_name = match cond.get("Index") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };

        // WAL: log DropIndex before modifying catalog
        let mut wal_doc = Document::new();
        wal_doc.insert("op", Value::String("DIX".into()));
        wal_doc.insert("c", Value::String(full_name.clone()));
        wal_doc.insert("name", Value::String(idx_name.clone()));
        let data = wal_doc.to_bytes().map_err(|_| SdbError::InvalidBson)?;
        self.wal_log(LogOp::IndexDrop, data)?;

        let mut catalog = self.catalog.write().map_err(|_| SdbError::Sys)?;
        catalog.drop_index(cs, cl, &idx_name)?;
        Ok(vec![])
    }

    // ── Auth commands ───────────────────────────────────────────────

    fn cmd_authenticate(&self, query: &MsgOpQuery, addr: &SocketAddr) -> Result<Vec<Document>> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;
        let username = match cond.get("User") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let password = match cond.get("Passwd") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let auth = self.auth.read().map_err(|_| SdbError::Sys)?;
        auth.authenticate(&username, password.as_bytes())?;
        // Mark this connection as authenticated
        self.set_authenticated(addr, true);
        Ok(vec![])
    }

    fn cmd_create_user(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;
        let username = match cond.get("User") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let password = match cond.get("Passwd") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let roles: Vec<String> = match cond.get("Roles") {
            Some(Value::Array(arr)) => arr
                .iter()
                .filter_map(|v| {
                    if let Value::String(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
                .collect(),
            _ => vec![],
        };

        let hash = sdb_auth::User::hash_password(&username, password.as_bytes());
        let user = sdb_auth::User::new(&username, hash, roles);
        let mut auth = self.auth.write().map_err(|_| SdbError::Sys)?;
        auth.create_user(user)?;
        Ok(vec![])
    }

    fn cmd_drop_user(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let username = get_string_field(query.condition.as_ref(), "User")?;
        let mut auth = self.auth.write().map_err(|_| SdbError::Sys)?;
        auth.drop_user(&username)?;
        Ok(vec![])
    }

    // ── Aggregate command ───────────────────────────────────────────

    fn cmd_aggregate(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;

        let collection = match cond.get("Collection") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };

        let stages = match cond.get("Pipeline") {
            Some(Value::Array(arr)) => parse_pipeline_stages(arr)?,
            _ => return Err(SdbError::InvalidArg),
        };

        let (cs, cl) = parse_collection_name(&collection)?;

        // Get input documents from the collection
        let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;
        let (storage, _) = catalog.collection_handle(cs, cl)?;
        let rows = storage.scan();
        let input: Vec<Document> = rows.into_iter().map(|(_, doc)| doc).collect();

        let pipeline = Pipeline::new(collection, stages);
        pipeline.execute_with_input(input)
    }

    // ── SQL command ─────────────────────────────────────────────────

    async fn cmd_sql(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let sql_text = get_string_field(query.condition.as_ref(), "SQL")?;

        let parser = SqlParser::new();
        let stmt = parser.parse(&sql_text)?;

        let qgm = QueryGraph::new();

        match &stmt {
            sdb_sql::SqlStatement::Select(_) => {
                // Convert to query plan and execute
                let plan = qgm.to_plan(&stmt)?;
                self.execute_sql_plan(&plan).await
            }
            sdb_sql::SqlStatement::Insert(ins) => {
                let (cs, cl) = parse_collection_name(&ins.table)?;
                let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;
                for row in &ins.values {
                    let mut doc = Document::new();
                    for (i, col) in ins.columns.iter().enumerate() {
                        if let Some(val_str) = row.get(i) {
                            doc.insert(col.as_str(), parse_sql_value(val_str));
                        }
                    }
                    catalog.insert_document(cs, cl, &doc)?;
                }
                Ok(vec![])
            }
            sdb_sql::SqlStatement::Update(upd) => {
                let (cs, cl) = parse_collection_name(&upd.table)?;
                let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;
                let (storage, _) = catalog.collection_handle(cs, cl)?;
                let rows = storage.scan();

                // Build condition matcher
                let condition = if let Some(ref where_str) = upd.condition {
                    sdb_sql::QueryGraph::parse_where_to_bson(where_str).ok()
                } else {
                    None
                };
                let matcher = Matcher::new(condition.unwrap_or_default())?;

                // Build modifier
                let mut set_doc = Document::new();
                for (col, val) in &upd.assignments {
                    set_doc.insert(col.as_str(), parse_sql_value(val));
                }
                let mut mod_doc = Document::new();
                mod_doc.insert("$set", Value::Document(set_doc));
                let modifier = Modifier::new(mod_doc)?;

                let matching: Vec<RecordId> = rows
                    .iter()
                    .filter(|(_, doc)| matcher.matches(doc).unwrap_or(false))
                    .map(|(rid, _)| *rid)
                    .collect();

                for rid in matching {
                    let old = storage.find(rid)?;
                    let new = modifier.modify(&old)?;
                    catalog.update_document(cs, cl, rid, &new)?;
                }
                Ok(vec![])
            }
            sdb_sql::SqlStatement::Delete(del) => {
                let (cs, cl) = parse_collection_name(&del.table)?;
                let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;
                let (storage, _) = catalog.collection_handle(cs, cl)?;
                let rows = storage.scan();

                let condition = if let Some(ref where_str) = del.condition {
                    sdb_sql::QueryGraph::parse_where_to_bson(where_str).ok()
                } else {
                    None
                };
                let matcher = Matcher::new(condition.unwrap_or_default())?;

                let matching: Vec<RecordId> = rows
                    .iter()
                    .filter(|(_, doc)| matcher.matches(doc).unwrap_or(false))
                    .map(|(rid, _)| *rid)
                    .collect();

                for rid in matching {
                    catalog.delete_document(cs, cl, rid)?;
                }
                Ok(vec![])
            }
            sdb_sql::SqlStatement::CreateTable(ct) => {
                // Treat table as cs.cl
                let (cs, cl) = parse_collection_name(&ct.table)?;
                let mut catalog = self.catalog.write().map_err(|_| SdbError::Sys)?;
                let _ = catalog.create_collection_space(cs); // ignore if exists
                catalog.create_collection(cs, cl)?;
                Ok(vec![])
            }
            sdb_sql::SqlStatement::DropTable(dt) => {
                let (cs, cl) = parse_collection_name(&dt.table)?;
                let mut catalog = self.catalog.write().map_err(|_| SdbError::Sys)?;
                catalog.drop_collection(cs, cl)?;
                Ok(vec![])
            }
        }
    }

    async fn execute_sql_plan(
        &self,
        plan: &sdb_opt::plan::QueryPlan,
    ) -> Result<Vec<Document>> {
        let collection = &plan.collection;
        let (cs, cl) = parse_collection_name(collection)?;

        let executor = {
            let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;
            let (storage, indexes) = catalog.collection_handle(cs, cl)?;

            let mut executor = DefaultExecutor::new();
            executor.register(
                collection.clone(),
                CollectionHandle { storage, indexes },
            );
            executor
        };

        let mut cursor = executor.execute(plan).await?;

        let mut docs = Vec::new();
        while let Ok(Some(doc)) = cursor.next() {
            docs.push(doc);
        }
        Ok(docs)
    }

    // ── Count command ───────────────────────────────────────────────

    fn cmd_count(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;

        let collection = match cond.get("Collection") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let (cs, cl) = parse_collection_name(&collection)?;

        let filter_cond = match cond.get("Condition") {
            Some(Value::Document(d)) => Some(d.clone()),
            _ => None,
        };

        let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;
        let (storage, _) = catalog.collection_handle(cs, cl)?;

        let count = if let Some(fc) = filter_cond {
            let matcher = Matcher::new(fc)?;
            let rows = storage.scan();
            rows.iter()
                .filter(|(_, doc)| matcher.matches(doc).unwrap_or(false))
                .count() as u64
        } else {
            storage.total_records()
        };

        let mut result = Document::new();
        result.insert("count", Value::Int64(count as i64));
        Ok(vec![result])
    }

    // ── Snapshot command ───────────────────────────────────────────

    fn cmd_snapshot(&self, snap_type: SnapshotType) -> Result<Vec<Document>> {
        match snap_type {
            SnapshotType::Database => {
                let metrics_doc = self.metrics.to_document();
                let snap = Snapshot::with_details(SnapshotType::Database, vec![metrics_doc.clone()]);
                let mut result = snap.to_document();
                // Merge metrics fields into the result document
                for entry in metrics_doc.iter() {
                    result.insert(entry.key.as_str(), entry.value.clone());
                }
                Ok(vec![result])
            }
            SnapshotType::Sessions => {
                let sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
                let count = sessions.len() as i64;
                let authenticated = sessions.values().filter(|v| **v).count() as i64;
                let mut doc = Document::new();
                doc.insert("Type", Value::String("Sessions".into()));
                doc.insert("TotalSessions", Value::Int64(count));
                doc.insert("AuthenticatedSessions", Value::Int64(authenticated));
                Ok(vec![doc])
            }
            SnapshotType::Collections => {
                let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;
                let mut docs = Vec::new();
                for cs_name in catalog.list_collection_spaces() {
                    if let Ok(cls) = catalog.list_collections(&cs_name) {
                        for cl_name in cls {
                            let full_name = format!("{}.{}", cs_name, cl_name);
                            let mut doc = Document::new();
                            doc.insert("Name", Value::String(full_name));
                            docs.push(doc);
                        }
                    }
                }
                Ok(docs)
            }
            SnapshotType::Health => {
                let mut doc = Document::new();
                doc.insert("Status", Value::String("OK".into()));
                doc.insert("ActiveSessions", Value::Int64(
                    self.metrics.active_sessions.load(std::sync::atomic::Ordering::Relaxed) as i64
                ));
                Ok(vec![doc])
            }
            _ => Err(SdbError::InvalidArg),
        }
    }
    // ── Read Preference commands ─────────────────────────────────────

    fn cmd_set_read_preference(&self, query: &MsgOpQuery, addr: &SocketAddr) -> Result<Vec<Document>> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;
        let pref_str = match cond.get("Preference") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let pref = ReadPreference::parse(&pref_str).ok_or(SdbError::InvalidArg)?;
        self.set_read_preference(addr, pref);
        let mut doc = Document::new();
        doc.insert("ReadPreference", Value::String(pref.as_str().to_string()));
        Ok(vec![doc])
    }

    fn cmd_get_read_preference(&self, addr: &SocketAddr) -> Result<Vec<Document>> {
        let pref = self.get_read_preference(addr);
        let mut doc = Document::new();
        doc.insert("ReadPreference", Value::String(pref.as_str().to_string()));
        let is_primary = match &self.election {
            None => true,
            Some(em) => em.lock().unwrap_or_else(|e| e.into_inner()).is_primary(),
        };
        doc.insert("IsPrimary", Value::Boolean(is_primary));
        Ok(vec![doc])
    }

    // ── Dynamic Membership commands ───────────────────────────────────

    fn cmd_add_member(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;
        let node_id = match cond.get("NodeId") {
            Some(Value::Int32(n)) => *n as u16,
            _ => return Err(SdbError::InvalidArg),
        };
        let addr = match cond.get("Address") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let group_id = match cond.get("GroupId") {
            Some(Value::Int32(n)) => *n as u32,
            _ => {
                // Default: same group as local node
                match &self.election {
                    Some(em) => em.lock().unwrap_or_else(|e| e.into_inner()).local_node.group_id,
                    None => 1,
                }
            }
        };

        let peer_addr = NodeAddress { group_id, node_id };

        // Add to election manager
        if let Some(em) = &self.election {
            let mut em = em.lock().unwrap_or_else(|e| e.into_inner());
            em.add_peer(peer_addr);
        }

        // Add to replication agent
        if let Some(agent) = &self.repl_agent {
            if let Ok(mut ag) = agent.try_lock() {
                ag.add_replica(peer_addr);
                ag.peer_addrs.insert(node_id, addr.clone());
            }
        }

        let mut doc = Document::new();
        doc.insert("Added", Value::Boolean(true));
        doc.insert("NodeId", Value::Int32(node_id as i32));
        doc.insert("Address", Value::String(addr));
        Ok(vec![doc])
    }

    fn cmd_remove_member(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;
        let node_id = match cond.get("NodeId") {
            Some(Value::Int32(n)) => *n as u16,
            _ => return Err(SdbError::InvalidArg),
        };

        // Remove from election manager
        let removed_em = if let Some(em) = &self.election {
            let mut em = em.lock().unwrap_or_else(|e| e.into_inner());
            em.remove_peer(node_id)
        } else {
            false
        };

        // Remove from replication agent
        if let Some(agent) = &self.repl_agent {
            if let Ok(mut ag) = agent.try_lock() {
                let _ = ag.remove_replica(node_id);
                ag.peer_addrs.remove(&node_id);
            }
        }

        let mut doc = Document::new();
        doc.insert("Removed", Value::Boolean(removed_em));
        doc.insert("NodeId", Value::Int32(node_id as i32));
        Ok(vec![doc])
    }

    fn cmd_get_members(&self) -> Result<Vec<Document>> {
        let mut docs = Vec::new();
        if let Some(em) = &self.election {
            let em = em.lock().unwrap_or_else(|e| e.into_inner());
            // Add local node
            let mut local = Document::new();
            local.insert("NodeId", Value::Int32(em.local_node.node_id as i32));
            local.insert("GroupId", Value::Int32(em.local_node.group_id as i32));
            local.insert("State", Value::String(format!("{:?}", em.state)));
            local.insert("IsSelf", Value::Boolean(true));
            docs.push(local);
            // Add peers
            for peer in em.peer_list() {
                let mut pdoc = Document::new();
                pdoc.insert("NodeId", Value::Int32(peer.node_id as i32));
                pdoc.insert("GroupId", Value::Int32(peer.group_id as i32));
                pdoc.insert("IsSelf", Value::Boolean(false));
                if let Some(agent) = &self.repl_agent {
                    if let Ok(ag) = agent.try_lock() {
                        if let Some(addr) = ag.peer_addrs.get(&peer.node_id) {
                            pdoc.insert("Address", Value::String(addr.clone()));
                        }
                        if let Some(status) = ag.replicas.get(&peer.node_id) {
                            pdoc.insert("SyncedLsn", Value::Int64(status.synced_lsn as i64));
                            pdoc.insert("IsAlive", Value::Boolean(status.is_alive));
                        }
                    }
                }
                docs.push(pdoc);
            }
        }
        Ok(docs)
    }

    // ── Replication message handlers ────────────────────────────────

    fn handle_repl_sync(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        if payload.len() < 4 {
            return MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg);
        }

        let num_records = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
        let mut offset = 4;
        let mut last_lsn: u64 = 0;

        for _ in 0..num_records {
            if offset + 4 > payload.len() {
                break;
            }
            let frame_len =
                u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + frame_len > payload.len() {
                break;
            }
            let frame_bytes = &payload[offset..offset + frame_len];
            offset += frame_len;

            // Decode frame as BSON doc
            if let Ok(doc) = Document::from_bytes(frame_bytes) {
                let lsn = match doc.get("lsn") {
                    Some(Value::Int64(n)) => *n as u64,
                    _ => continue,
                };
                last_lsn = lsn;

                // Extract the original WAL data and replay it
                let data = match doc.get("data") {
                    Some(Value::Binary(b)) => b.clone(),
                    _ => continue,
                };

                // Parse the WAL data as a BSON doc for replay
                if let Ok(wal_doc) = Document::from_bytes(&data) {
                    let op_str = match wal_doc.get("op") {
                        Some(Value::String(s)) => s.clone(),
                        _ => continue,
                    };

                    let mut catalog = match self.catalog.write() {
                        Ok(c) => c,
                        Err(_) => continue,
                    };

                    let _ = match op_str.as_str() {
                        "CCS" => replay_create_cs(&mut catalog, &wal_doc),
                        "DCS" => replay_drop_cs(&mut catalog, &wal_doc),
                        "CCL" => replay_create_cl(&mut catalog, &wal_doc),
                        "DCL" => replay_drop_cl(&mut catalog, &wal_doc),
                        "CIX" => replay_create_index(&mut catalog, &wal_doc),
                        "DIX" => replay_drop_index(&mut catalog, &wal_doc),
                        "I" => replay_insert(&mut catalog, &wal_doc),
                        "D" => replay_delete(&catalog, &wal_doc),
                        "U" => replay_update(&catalog, &wal_doc),
                        _ => Ok(()),
                    };
                }

                // Also append to local WAL
                if let Some(ref wal) = self.wal {
                    if let Ok(mut wal) = wal.lock() {
                        let op_code = match doc.get("op") {
                            Some(Value::Int32(n)) => *n,
                            _ => 0,
                        };
                        let mut record = LogRecord {
                            lsn: 0,
                            prev_lsn: 0,
                            txn_id: 0,
                            op: log_op_from_i32(op_code),
                            data,
                        };
                        let _ = wal.append(&mut record);
                        let _ = wal.flush();
                    }
                }
            }
        }

        // Return ack with last_lsn
        let mut ack_doc = Document::new();
        ack_doc.insert("acked_lsn", Value::Int64(last_lsn as i64));
        MsgOpReply::ok(header.opcode, header.request_id, vec![ack_doc])
    }

    fn handle_repl_vote_req(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let em = match &self.election {
            Some(em) => em,
            None => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg),
        };

        // Decode BSON payload
        let doc = match Document::from_bytes(payload) {
            Ok(d) => d,
            Err(_) => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidBson),
        };

        let term = match doc.get("term") {
            Some(Value::Int64(n)) => *n as u64,
            _ => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg),
        };
        let candidate_id = match doc.get("candidate_id") {
            Some(Value::Int32(n)) => *n as u16,
            _ => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg),
        };
        let last_lsn = match doc.get("last_lsn") {
            Some(Value::Int64(n)) => *n as u64,
            _ => 0,
        };

        let mut em = em.lock().unwrap_or_else(|e| e.into_inner());
        let granted = em.should_grant_vote(candidate_id, term, last_lsn);

        let mut reply_doc = Document::new();
        reply_doc.insert("term", Value::Int64(em.term as i64));
        reply_doc.insert("voter_id", Value::Int32(em.local_node.node_id as i32));
        reply_doc.insert("granted", Value::Boolean(granted));
        MsgOpReply::ok(header.opcode, header.request_id, vec![reply_doc])
    }

    fn handle_repl_heartbeat(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let em = match &self.election {
            Some(em) => em,
            None => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg),
        };

        let doc = match Document::from_bytes(payload) {
            Ok(d) => d,
            Err(_) => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidBson),
        };

        let term = match doc.get("term") {
            Some(Value::Int64(n)) => *n as u64,
            _ => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg),
        };
        let primary_id = match doc.get("primary_id") {
            Some(Value::Int32(n)) => *n as u16,
            _ => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg),
        };

        let mut em = em.lock().unwrap_or_else(|e| e.into_inner());
        em.heartbeat_received(primary_id, term);

        MsgOpReply::ok(header.opcode, header.request_id, vec![])
    }

    // ── Dynamic Membership RPC handler ───────────────────────────────

    fn handle_repl_member_change(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let doc = match Document::from_bytes(payload) {
            Ok(d) => d,
            Err(_) => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidBson),
        };

        let action = match doc.get("action") {
            Some(Value::String(s)) => s.clone(),
            _ => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg),
        };
        let node_id = match doc.get("node_id") {
            Some(Value::Int32(n)) => *n as u16,
            _ => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg),
        };
        let group_id = match doc.get("group_id") {
            Some(Value::Int32(n)) => *n as u32,
            _ => 1,
        };
        let addr_str = match doc.get("address") {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        };

        match action.as_str() {
            "add" => {
                let peer = NodeAddress { group_id, node_id };
                if let Some(em) = &self.election {
                    let mut em = em.lock().unwrap_or_else(|e| e.into_inner());
                    em.add_peer(peer);
                }
                if let (Some(agent), Some(addr)) = (&self.repl_agent, addr_str) {
                    if let Ok(mut ag) = agent.try_lock() {
                        ag.add_replica(peer);
                        ag.peer_addrs.insert(node_id, addr);
                    }
                }
            }
            "remove" => {
                if let Some(em) = &self.election {
                    let mut em = em.lock().unwrap_or_else(|e| e.into_inner());
                    em.remove_peer(node_id);
                }
                if let Some(agent) = &self.repl_agent {
                    if let Ok(mut ag) = agent.try_lock() {
                        let _ = ag.remove_replica(node_id);
                        ag.peer_addrs.remove(&node_id);
                    }
                }
            }
            _ => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg),
        }

        MsgOpReply::ok(header.opcode, header.request_id, vec![])
    }

    // ── Chunk Migration Data handler ────────────────────────────────

    fn handle_chunk_migrate_data(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        // Receive migrated documents and insert them into the local catalog.
        // Payload: BSON doc { collection: String, docs: [Document] }
        let doc = match Document::from_bytes(payload) {
            Ok(d) => d,
            Err(_) => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidBson),
        };

        let collection = match doc.get("collection") {
            Some(Value::String(s)) => s.clone(),
            _ => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg),
        };
        let docs = match doc.get("docs") {
            Some(Value::Array(arr)) => arr.clone(),
            _ => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg),
        };

        let result = (|| -> Result<u64> {
            let (cs, cl) = parse_collection_name(&collection)?;
            let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;
            let mut count = 0u64;
            for val in &docs {
                if let Value::Document(d) = val {
                    catalog.insert_document(cs, cl, d)?;
                    count += 1;
                }
            }
            Ok(count)
        })();

        match result {
            Ok(count) => {
                let mut ack = Document::new();
                ack.insert("inserted", Value::Int64(count as i64));
                MsgOpReply::ok(header.opcode, header.request_id, vec![ack])
            }
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    // ── Election background loop ────────────────────────────────────

    /// Run the election loop as a background task.
    /// Checks heartbeat timeout, triggers elections, and sends heartbeats if primary.
    pub async fn run_election_loop(self: Arc<Self>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            let em = match &self.election {
                Some(em) => em.clone(),
                None => return,
            };
            let agent = match &self.repl_agent {
                Some(a) => a.clone(),
                None => return,
            };

            let (should_elect, is_primary, _term, node_id, peers, local_lsn) = {
                let em = em.lock().unwrap_or_else(|e| e.into_inner());
                let should = em.state != ElectionState::Primary
                    && em.is_heartbeat_timed_out();
                let peers: Vec<(u16, String)> = {
                    let ag = agent.try_lock();
                    match ag {
                        Ok(ag) => ag.peer_addrs.iter().map(|(id, addr)| (*id, addr.clone())).collect(),
                        Err(_) => vec![],
                    }
                };
                (should, em.is_primary(), em.term, em.local_node.node_id, peers, em.local_lsn)
            };

            if should_elect && !peers.is_empty() {
                // Start election
                {
                    let mut em = em.lock().unwrap_or_else(|e| e.into_inner());
                    let _ = em.start_election();
                }
                let current_term = {
                    let em = em.lock().unwrap_or_else(|e| e.into_inner());
                    em.term
                };

                // Send VoteReq to all peers
                for (peer_id, addr) in &peers {
                    let mut vote_doc = Document::new();
                    vote_doc.insert("term", Value::Int64(current_term as i64));
                    vote_doc.insert("candidate_id", Value::Int32(node_id as i32));
                    vote_doc.insert("last_lsn", Value::Int64(local_lsn as i64));

                    if let Ok(payload) = vote_doc.to_bytes() {
                        let mut header = MsgHeader::new_request(OpCode::ReplVoteReq as i32, 0);
                        header.msg_len = (MsgHeader::SIZE + payload.len()) as i32;
                        let mut msg = Vec::new();
                        header.encode(&mut msg);
                        msg.extend_from_slice(&payload);

                        if let Ok(mut stream) = TcpStream::connect(addr).await {
                            if stream.write_all(&msg).await.is_ok() {
                                let _ = stream.flush().await;
                                // Read reply
                                let mut len_buf = [0u8; 4];
                                if stream.read_exact(&mut len_buf).await.is_ok() {
                                    let reply_len = i32::from_le_bytes(len_buf) as usize;
                                    if (MsgHeader::SIZE..65536).contains(&reply_len) {
                                        let mut reply_buf = vec![0u8; reply_len];
                                        reply_buf[..4].copy_from_slice(&len_buf);
                                        if stream.read_exact(&mut reply_buf[4..]).await.is_ok() {
                                            if let Ok(reply_header) = MsgHeader::decode(&reply_buf) {
                                                let reply_payload = &reply_buf[MsgHeader::SIZE..];
                                                if let Ok(reply) = sdb_msg::reply::MsgOpReply::decode(&reply_header, reply_payload) {
                                                    if let Some(doc) = reply.docs.first() {
                                                        let granted = matches!(doc.get("granted"), Some(Value::Boolean(true)));
                                                        if granted {
                                                            let mut em = em.lock().unwrap_or_else(|e| e.into_inner());
                                                            let _ = em.receive_vote(*peer_id);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if is_primary {
                // Try to catch up failed peers (reconnect with backoff)
                {
                    if let Ok(mut ag) = agent.try_lock() {
                        let catchup_peers = ag.peers_needing_catchup();
                        for (peer_id, from_lsn) in catchup_peers {
                            if let Some(addr) = ag.peer_addrs.get(&peer_id).cloned() {
                                // Quick connectivity check
                                match TcpStream::connect(&addr).await {
                                    Ok(_) => {
                                        ag.record_success(peer_id);
                                        tracing::info!("Peer {} reconnected (from LSN {})", peer_id, from_lsn);
                                    }
                                    Err(_) => {
                                        ag.record_failure(peer_id);
                                    }
                                }
                            }
                        }
                    }
                }

                // Send heartbeat to all peers
                let current_term = {
                    let em = em.lock().unwrap_or_else(|e| e.into_inner());
                    em.term
                };

                for (_peer_id, addr) in &peers {
                    let mut hb_doc = Document::new();
                    hb_doc.insert("term", Value::Int64(current_term as i64));
                    hb_doc.insert("primary_id", Value::Int32(node_id as i32));
                    hb_doc.insert("commit_lsn", Value::Int64(0));

                    if let Ok(payload) = hb_doc.to_bytes() {
                        let mut header = MsgHeader::new_request(OpCode::ReplHeartbeat as i32, 0);
                        header.msg_len = (MsgHeader::SIZE + payload.len()) as i32;
                        let mut msg = Vec::new();
                        header.encode(&mut msg);
                        msg.extend_from_slice(&payload);

                        if let Ok(mut stream) = TcpStream::connect(addr).await {
                            let _ = stream.write_all(&msg).await;
                            let _ = stream.flush().await;
                            // Read and discard ack
                            let mut len_buf = [0u8; 4];
                            if stream.read_exact(&mut len_buf).await.is_ok() {
                                let rl = i32::from_le_bytes(len_buf) as usize;
                                if (MsgHeader::SIZE..65536).contains(&rl) {
                                    let mut buf = vec![0u8; rl - 4];
                                    let _ = stream.read_exact(&mut buf).await;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn log_op_from_i32(v: i32) -> LogOp {
    match v {
        0 => LogOp::Insert,
        1 => LogOp::Update,
        2 => LogOp::Delete,
        3 => LogOp::TxnBegin,
        4 => LogOp::TxnCommit,
        5 => LogOp::TxnAbort,
        6 => LogOp::CollectionCreate,
        7 => LogOp::CollectionDrop,
        8 => LogOp::IndexCreate,
        9 => LogOp::IndexDrop,
        _ => LogOp::Insert,
    }
}

#[async_trait]
impl MessageHandler for DataNodeHandler {
    async fn on_message(
        &self,
        conn: &mut Connection,
        header: MsgHeader,
        payload: &[u8],
    ) -> Result<()> {
        let opcode = OpCode::from_i32(header.opcode);
        let addr = conn.addr;
        tracing::debug!("Received opcode {:?} from {}", opcode, addr);

        let reply = match opcode {
            Some(OpCode::QueryReq) => self.handle_query(&header, payload, &addr).await,
            Some(OpCode::InsertReq) => {
                if let Err(e) = self.check_auth(&addr) {
                    MsgOpReply::error(header.opcode, header.request_id, &e)
                } else {
                    self.handle_insert(&header, payload, &addr)
                }
            }
            Some(OpCode::UpdateReq) => {
                if let Err(e) = self.check_auth(&addr) {
                    MsgOpReply::error(header.opcode, header.request_id, &e)
                } else {
                    self.handle_update(&header, payload, &addr)
                }
            }
            Some(OpCode::DeleteReq) => {
                if let Err(e) = self.check_auth(&addr) {
                    MsgOpReply::error(header.opcode, header.request_id, &e)
                } else {
                    self.handle_delete(&header, payload, &addr)
                }
            }
            Some(OpCode::GetMoreReq) => self.handle_get_more(&header, payload),
            Some(OpCode::KillContextReq) => self.handle_kill_context(&header, payload),
            Some(OpCode::AggregateReq) | Some(OpCode::SqlReq) => {
                self.handle_query(&header, payload, &addr).await
            }
            Some(OpCode::TransBeginReq) => self.handle_trans_begin(&header, &addr),
            Some(OpCode::TransCommitReq) => self.handle_trans_commit(&header, &addr),
            Some(OpCode::TransRollbackReq) => self.handle_trans_rollback(&header, &addr),
            Some(OpCode::Disconnect) => return Err(SdbError::NetworkClose),
            Some(OpCode::ReplSync) => self.handle_repl_sync(&header, payload),
            Some(OpCode::ReplVoteReq) => self.handle_repl_vote_req(&header, payload),
            Some(OpCode::ReplVoteReply) => {
                // VoteReply is handled inline in election loop, not here
                MsgOpReply::ok(header.opcode, header.request_id, vec![])
            }
            Some(OpCode::ReplHeartbeat) => self.handle_repl_heartbeat(&header, payload),
            Some(OpCode::ReplMemberChange) => self.handle_repl_member_change(&header, payload),
            Some(OpCode::ChunkMigrateData) => self.handle_chunk_migrate_data(&header, payload),
            _ => MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg),
        };

        conn.send_reply(&reply).await
    }

    async fn on_connect(&self, conn: &Connection) -> Result<()> {
        tracing::info!("Client connected: {}", conn.addr);
        self.metrics.inc_sessions();
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(conn.addr, false);
        Ok(())
    }

    async fn on_disconnect(&self, conn: &Connection) -> Result<()> {
        tracing::info!("Client disconnected: {}", conn.addr);
        self.metrics.dec_sessions();
        // Auto-rollback any active transaction
        let buffer = {
            let mut buffers = self.txn_buffers.lock().unwrap_or_else(|e| e.into_inner());
            buffers.remove(&conn.addr)
        };
        if let Some(buffer) = buffer {
            let mut mgr = self.txn_mgr.lock().unwrap_or_else(|e| e.into_inner());
            let _ = mgr.abort(buffer.txn_id);
            tracing::info!("Auto-rolled back txn {} for disconnected client {}", buffer.txn_id, conn.addr);
        }
        // Also remove from sessions and read preferences
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&conn.addr);
        self.read_prefs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&conn.addr);
        Ok(())
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Parse "cs.cl" into ("cs", "cl").
fn parse_collection_name(name: &str) -> Result<(&str, &str)> {
    name.split_once('.').ok_or(SdbError::InvalidArg)
}

/// Extract a string field from an optional BSON document.
fn get_string_field(doc: Option<&Document>, key: &str) -> Result<String> {
    let doc = doc.ok_or(SdbError::InvalidArg)?;
    match doc.get(key) {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(SdbError::InvalidArg),
    }
}

/// Parse pipeline stages from a BSON array.
fn parse_pipeline_stages(arr: &[Value]) -> Result<Vec<Stage>> {
    let mut stages = Vec::new();
    for val in arr {
        if let Value::Document(doc) = val {
            // Each stage doc has one key: "$match", "$project", etc.
            let elem = doc.iter().next().ok_or(SdbError::InvalidArg)?;
            let stage_type = match elem.key.as_str() {
                "$match" => StageType::Match,
                "$project" => StageType::Project,
                "$sort" => StageType::Sort,
                "$limit" => StageType::Limit,
                "$skip" => StageType::Skip,
                "$count" => StageType::Count,
                "$group" => StageType::Group,
                "$unwind" => StageType::Unwind,
                _ => return Err(SdbError::InvalidArg),
            };
            let spec = match &elem.value {
                Value::Document(d) => d.clone(),
                Value::Int32(n) => {
                    let mut d = Document::new();
                    d.insert("n", Value::Int32(*n));
                    d
                }
                Value::Int64(n) => {
                    let mut d = Document::new();
                    d.insert("n", Value::Int64(*n));
                    d
                }
                Value::String(s) => {
                    let mut d = Document::new();
                    d.insert("field", Value::String(s.clone()));
                    d
                }
                _ => return Err(SdbError::InvalidArg),
            };
            stages.push(Stage { stage_type, spec });
        } else {
            return Err(SdbError::InvalidArg);
        }
    }
    Ok(stages)
}

/// Parse a SQL value string to a BSON Value.
fn parse_sql_value(s: &str) -> Value {
    // Try integer
    if let Ok(n) = s.parse::<i64>() {
        if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
            return Value::Int32(n as i32);
        }
        return Value::Int64(n);
    }
    // Try float
    if let Ok(f) = s.parse::<f64>() {
        return Value::Double(f);
    }
    // String (strip quotes if present)
    let trimmed = s.trim_matches('\'').trim_matches('"');
    Value::String(trimmed.to_string())
}

// ── WAL Recovery ────────────────────────────────────────────────────────

/// Recover a CatalogManager by replaying all WAL records.
///
/// Replays records in WAL order:
/// - DDL: CreateCS, DropCS, CreateCL, DropCL, CreateIndex, DropIndex
/// - DML: Insert documents, Delete documents (by content match), Update (old->new)
///
/// Returns a fully populated CatalogManager reflecting the state at the
/// time of the last flushed WAL record.
pub fn recover_from_wal(wal: &mut WriteAheadLog) -> Result<CatalogManager> {
    let records = wal.recover()?;
    let mut catalog = CatalogManager::new();

    for record in &records {
        let wal_doc = match Document::from_bytes(&record.data) {
            Ok(d) => d,
            Err(_) => {
                // Skip corrupted/unparseable records during recovery
                continue;
            }
        };

        let op_str = match wal_doc.get("op") {
            Some(Value::String(s)) => s.clone(),
            _ => continue,
        };

        let result = match op_str.as_str() {
            "CCS" => replay_create_cs(&mut catalog, &wal_doc),
            "DCS" => replay_drop_cs(&mut catalog, &wal_doc),
            "CCL" => replay_create_cl(&mut catalog, &wal_doc),
            "DCL" => replay_drop_cl(&mut catalog, &wal_doc),
            "CIX" => replay_create_index(&mut catalog, &wal_doc),
            "DIX" => replay_drop_index(&mut catalog, &wal_doc),
            "I" => replay_insert(&mut catalog, &wal_doc),
            "D" => replay_delete(&catalog, &wal_doc),
            "U" => replay_update(&catalog, &wal_doc),
            _ => Ok(()), // Unknown op, skip
        };

        if let Err(e) = result {
            // Log but continue — partial recovery is better than none.
            // In production, certain errors (like "already exists" after
            // replaying a create that was already done) are expected if
            // the WAL contains duplicate records.
            tracing::warn!("WAL replay warning for op '{}': {}", op_str, e);
        }
    }

    Ok(catalog)
}

fn replay_create_cs(catalog: &mut CatalogManager, doc: &Document) -> Result<()> {
    let name = match doc.get("name") {
        Some(Value::String(s)) => s.clone(),
        _ => return Err(SdbError::InvalidArg),
    };
    let _ = catalog.create_collection_space(&name);
    Ok(())
}

fn replay_drop_cs(catalog: &mut CatalogManager, doc: &Document) -> Result<()> {
    let name = match doc.get("name") {
        Some(Value::String(s)) => s.clone(),
        _ => return Err(SdbError::InvalidArg),
    };
    let _ = catalog.drop_collection_space(&name);
    Ok(())
}

fn replay_create_cl(catalog: &mut CatalogManager, doc: &Document) -> Result<()> {
    let full_name = match doc.get("name") {
        Some(Value::String(s)) => s.clone(),
        _ => return Err(SdbError::InvalidArg),
    };
    let (cs, cl) = parse_collection_name(&full_name)?;
    let _ = catalog.create_collection(cs, cl);
    Ok(())
}

fn replay_drop_cl(catalog: &mut CatalogManager, doc: &Document) -> Result<()> {
    let full_name = match doc.get("name") {
        Some(Value::String(s)) => s.clone(),
        _ => return Err(SdbError::InvalidArg),
    };
    let (cs, cl) = parse_collection_name(&full_name)?;
    let _ = catalog.drop_collection(cs, cl);
    Ok(())
}

fn replay_create_index(catalog: &mut CatalogManager, doc: &Document) -> Result<()> {
    let full_name = match doc.get("c") {
        Some(Value::String(s)) => s.clone(),
        _ => return Err(SdbError::InvalidArg),
    };
    let (cs, cl) = parse_collection_name(&full_name)?;

    let idx_name = match doc.get("name") {
        Some(Value::String(s)) => s.clone(),
        _ => return Err(SdbError::InvalidArg),
    };
    let key_pattern = match doc.get("key") {
        Some(Value::Document(d)) => d.clone(),
        _ => return Err(SdbError::InvalidArg),
    };
    let unique = match doc.get("unique") {
        Some(Value::Boolean(b)) => *b,
        _ => false,
    };

    let _ = catalog.create_index(cs, cl, &idx_name, key_pattern, unique);
    Ok(())
}

fn replay_drop_index(catalog: &mut CatalogManager, doc: &Document) -> Result<()> {
    let full_name = match doc.get("c") {
        Some(Value::String(s)) => s.clone(),
        _ => return Err(SdbError::InvalidArg),
    };
    let (cs, cl) = parse_collection_name(&full_name)?;

    let idx_name = match doc.get("name") {
        Some(Value::String(s)) => s.clone(),
        _ => return Err(SdbError::InvalidArg),
    };

    let _ = catalog.drop_index(cs, cl, &idx_name);
    Ok(())
}

fn replay_insert(catalog: &mut CatalogManager, wal_doc: &Document) -> Result<()> {
    let collection = match wal_doc.get("c") {
        Some(Value::String(s)) => s.clone(),
        _ => return Err(SdbError::InvalidArg),
    };
    let (cs, cl) = parse_collection_name(&collection)?;

    let doc = match wal_doc.get("doc") {
        Some(Value::Document(d)) => d.clone(),
        _ => return Err(SdbError::InvalidArg),
    };

    catalog.insert_document(cs, cl, &doc)?;
    Ok(())
}

fn replay_delete(catalog: &CatalogManager, wal_doc: &Document) -> Result<()> {
    let collection = match wal_doc.get("c") {
        Some(Value::String(s)) => s.clone(),
        _ => return Err(SdbError::InvalidArg),
    };
    let (cs, cl) = parse_collection_name(&collection)?;

    let target_doc = match wal_doc.get("doc") {
        Some(Value::Document(d)) => d.clone(),
        _ => return Err(SdbError::InvalidArg),
    };

    // Find the document by content match and delete it
    let (storage, _) = catalog.collection_handle(cs, cl)?;
    let rows = storage.scan();

    // Find first matching document by content equality
    for (rid, doc) in &rows {
        if *doc == target_doc {
            catalog.delete_document(cs, cl, *rid)?;
            return Ok(());
        }
    }
    // Document not found — might have already been deleted. Not an error during recovery.
    Ok(())
}

fn replay_update(catalog: &CatalogManager, wal_doc: &Document) -> Result<()> {
    let collection = match wal_doc.get("c") {
        Some(Value::String(s)) => s.clone(),
        _ => return Err(SdbError::InvalidArg),
    };
    let (cs, cl) = parse_collection_name(&collection)?;

    let old_doc = match wal_doc.get("old") {
        Some(Value::Document(d)) => d.clone(),
        _ => return Err(SdbError::InvalidArg),
    };
    let new_doc = match wal_doc.get("new") {
        Some(Value::Document(d)) => d.clone(),
        _ => return Err(SdbError::InvalidArg),
    };

    // Find the old document by content match
    let (storage, _) = catalog.collection_handle(cs, cl)?;
    let rows = storage.scan();

    for (rid, doc) in &rows {
        if *doc == old_doc {
            catalog.update_document(cs, cl, *rid, &new_doc)?;
            return Ok(());
        }
    }
    // Old doc not found — might be a replayed update on already-updated data.
    // Not an error during recovery.
    Ok(())
}
