use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex, RwLock};

use async_trait::async_trait;
use sdb_bson::{Document, Value};
use sdb_cls::ShardManager;
use sdb_common::{GroupId, Result, SdbError};
use sdb_coord::CoordRouter;
use sdb_mon::Metrics;
use sdb_msg::header::MsgHeader;
use sdb_msg::opcode::OpCode;
use sdb_msg::reply::MsgOpReply;
use sdb_msg::request::*;
use sdb_net::Connection;
use sdb_net::MessageHandler;

use crate::cursor_manager::CursorManager;
use crate::data_node_client::DataNodeClient;

struct CoordTxnState {
    group_txns: HashMap<GroupId, u64>, // group_id -> txn_id on that data node
}

/// Coordinator node handler — routes requests across multiple data groups
/// via real TCP connections to DataNode servers.
pub struct CoordNodeHandler {
    router: RwLock<CoordRouter>,
    pub(crate) clients: HashMap<GroupId, Arc<DataNodeClient>>,
    cursors: StdMutex<CursorManager>,
    coord_txns: StdMutex<HashMap<SocketAddr, CoordTxnState>>,
    metrics: Arc<Metrics>,
}

impl CoordNodeHandler {
    /// Create a coordinator with TCP connections to data nodes.
    /// data_nodes: vec of (group_id, "host:port") pairs.
    pub fn new(data_nodes: Vec<(GroupId, String)>) -> Self {
        let mut clients = HashMap::new();
        for (gid, addr) in data_nodes {
            clients.insert(gid, Arc::new(DataNodeClient::new(addr)));
        }
        Self {
            router: RwLock::new(CoordRouter::new()),
            clients,
            cursors: StdMutex::new(CursorManager::new()),
            coord_txns: StdMutex::new(HashMap::new()),
            metrics: Arc::new(Metrics::new()),
        }
    }

    pub fn all_group_ids(&self) -> Vec<GroupId> {
        self.clients.keys().copied().collect()
    }

    /// Register sharding for a collection.
    pub fn set_shard(
        &self,
        collection: &str,
        shard_key: &str,
        num_groups: u32,
    ) -> Result<()> {
        let mut router = self.router.write().map_err(|_| SdbError::Sys)?;
        let mut sm = ShardManager::new();
        sm.set_hash_sharding(shard_key, num_groups);
        router.register_shard(collection, sm);
        Ok(())
    }

    // ── Query dispatch ──────────────────────────────────────────────

    async fn handle_query(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let query = match MsgOpQuery::decode(header, payload) {
            Ok(q) => q,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        if query.name.starts_with('$') {
            self.handle_command(header, &query).await
        } else {
            self.handle_data_query(header, &query).await
        }
    }

    async fn handle_command(&self, header: &MsgHeader, query: &MsgOpQuery) -> MsgOpReply {
        let result = match query.name.as_str() {
            "$create collectionspace" => {
                let name = get_string_field(query.condition.as_ref(), "Name");
                match name {
                    Ok(name) => self.broadcast_create_cs(&name).await,
                    Err(e) => Err(e),
                }
            }
            "$drop collectionspace" => {
                let name = get_string_field(query.condition.as_ref(), "Name");
                match name {
                    Ok(name) => self.broadcast_drop_cs(&name).await,
                    Err(e) => Err(e),
                }
            }
            "$create collection" => {
                let name = get_string_field(query.condition.as_ref(), "Name");
                match name {
                    Ok(full_name) => self.broadcast_create_cl(&full_name).await,
                    Err(e) => Err(e),
                }
            }
            "$drop collection" => {
                let name = get_string_field(query.condition.as_ref(), "Name");
                match name {
                    Ok(full_name) => self.broadcast_drop_cl(&full_name).await,
                    Err(e) => Err(e),
                }
            }
            "$create index" => self.broadcast_create_index(query).await,
            "$drop index" => self.broadcast_drop_index(query).await,
            "$count" => match self.cmd_count(query).await {
                Ok(docs) => return MsgOpReply::ok(header.opcode, header.request_id, docs),
                Err(e) => Err(e),
            },
            "$enable sharding" => self.cmd_enable_sharding(query),
            "$get shard info" => match self.cmd_get_shard_info(query) {
                Ok(docs) => return MsgOpReply::ok(header.opcode, header.request_id, docs),
                Err(e) => Err(e),
            },
            _ => Err(SdbError::InvalidArg),
        };
        match result {
            Ok(()) => MsgOpReply::ok(header.opcode, header.request_id, vec![]),
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    // ── DDL broadcast methods ───────────────────────────────────────

    async fn broadcast_create_cs(&self, name: &str) -> Result<()> {
        for client in self.clients.values() {
            client.create_collection_space(name).await?;
        }
        Ok(())
    }

    async fn broadcast_drop_cs(&self, name: &str) -> Result<()> {
        for client in self.clients.values() {
            client.drop_collection_space(name).await?;
        }
        Ok(())
    }

    async fn broadcast_create_cl(&self, full_name: &str) -> Result<()> {
        for client in self.clients.values() {
            client.create_collection(full_name).await?;
        }
        Ok(())
    }

    async fn broadcast_drop_cl(&self, full_name: &str) -> Result<()> {
        for client in self.clients.values() {
            client.drop_collection(full_name).await?;
        }
        Ok(())
    }

    async fn broadcast_create_index(&self, query: &MsgOpQuery) -> Result<()> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;
        let full_name = match cond.get("Collection") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
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
        let unique = matches!(index_doc.get("unique"), Some(Value::Boolean(true)));

        for client in self.clients.values() {
            client
                .create_index(&full_name, &idx_name, key_pattern.clone(), unique)
                .await?;
        }
        Ok(())
    }

    async fn broadcast_drop_index(&self, query: &MsgOpQuery) -> Result<()> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;
        let full_name = match cond.get("Collection") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let idx_name = match cond.get("Index") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        for client in self.clients.values() {
            client.drop_index(&full_name, &idx_name).await?;
        }
        Ok(())
    }

    // ── Data query (scatter-gather) ─────────────────────────────────

    async fn handle_data_query(&self, header: &MsgHeader, query: &MsgOpQuery) -> MsgOpReply {
        let result = self.execute_scatter_query(query).await;
        match result {
            Ok(docs) => {
                self.metrics.inc_query();
                let mut cursors = self.cursors.lock().unwrap();
                let (batch, context_id) = cursors.create_cursor(docs);
                let mut reply =
                    MsgOpReply::ok(header.opcode, header.request_id, batch);
                reply.context_id = context_id;
                reply
            }
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    /// Execute a query across relevant groups and merge results.
    pub async fn execute_scatter_query(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let target_groups = {
            let router = self.router.read().map_err(|_| SdbError::Sys)?;
            let routed = router.route_query(&query.name, query.condition.as_ref())?;
            // If only default group returned and we have multiple groups,
            // broadcast to all (unsharded collection).
            if routed.len() == 1 && self.clients.len() > 1 && !self.clients.contains_key(&routed[0])
            {
                self.clients.keys().copied().collect()
            } else if routed.len() == 1 && self.clients.len() > 1 {
                // Check if there's actually a shard registered
                let has_shard = router
                    .route_query(&query.name, None)
                    .map(|v| v.len() > 1)
                    .unwrap_or(false);
                if has_shard {
                    routed
                } else {
                    // No sharding -- broadcast to all groups
                    self.clients.keys().copied().collect()
                }
            } else {
                routed
            }
        }; // router lock dropped here before any .await

        let mut all_docs = Vec::new();

        for &gid in &target_groups {
            let client = self.clients.get(&gid).ok_or(SdbError::NodeNotFound)?;
            let docs = client
                .query(
                    &query.name,
                    query.condition.clone(),
                    query.selector.clone(),
                    query.order_by.clone(),
                    query.num_to_skip,
                    query.num_to_return,
                )
                .await?;
            all_docs.extend(docs);
        }

        Ok(all_docs)
    }

    // ── Insert routing ──────────────────────────────────────────────

    async fn handle_insert(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let msg = match MsgOpInsert::decode(header, payload) {
            Ok(m) => m,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        // Compute grouping synchronously (no .await while holding lock)
        let grouped = {
            let router = match self.router.read() {
                Ok(r) => r,
                Err(_) => return MsgOpReply::error(header.opcode, header.request_id, &SdbError::Sys),
            };
            let mut grouped: HashMap<GroupId, Vec<Document>> = HashMap::new();
            for doc in &msg.docs {
                match router.route_insert(&msg.name, doc) {
                    Ok(gid) => grouped.entry(gid).or_default().push(doc.clone()),
                    Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
                }
            }
            grouped
        }; // router lock dropped here

        let result = async {
            for (gid, docs) in grouped {
                let client = self.clients.get(&gid).ok_or(SdbError::NodeNotFound)?;
                client.insert(&msg.name, docs).await?;
            }
            Ok::<(), SdbError>(())
        }
        .await;

        match result {
            Ok(()) => {
                self.metrics.inc_insert();
                MsgOpReply::ok(header.opcode, header.request_id, vec![])
            }
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    // ── Update broadcast ────────────────────────────────────────────

    async fn handle_update(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let msg = match MsgOpUpdate::decode(header, payload) {
            Ok(m) => m,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        let result = async {
            let target_groups = {
                let router = self.router.read().map_err(|_| SdbError::Sys)?;
                router.route_update(&msg.name, Some(&msg.condition))?
            };

            for &gid in &target_groups {
                let client = self.clients.get(&gid).ok_or(SdbError::NodeNotFound)?;
                client
                    .update(&msg.name, msg.condition.clone(), msg.modifier.clone())
                    .await?;
            }
            Ok::<(), SdbError>(())
        }
        .await;

        match result {
            Ok(()) => {
                self.metrics.inc_update();
                MsgOpReply::ok(header.opcode, header.request_id, vec![])
            }
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    // ── Delete broadcast ────────────────────────────────────────────

    async fn handle_delete(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let msg = match MsgOpDelete::decode(header, payload) {
            Ok(m) => m,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        let result = async {
            let target_groups = {
                let router = self.router.read().map_err(|_| SdbError::Sys)?;
                router.route_delete(&msg.name, Some(&msg.condition))?
            };

            for &gid in &target_groups {
                let client = self.clients.get(&gid).ok_or(SdbError::NodeNotFound)?;
                client.delete(&msg.name, msg.condition.clone()).await?;
            }
            Ok::<(), SdbError>(())
        }
        .await;

        match result {
            Ok(()) => {
                self.metrics.inc_delete();
                MsgOpReply::ok(header.opcode, header.request_id, vec![])
            }
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    // ── GetMore / KillContext ────────────────────────────────────────

    fn handle_get_more(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let msg = match MsgOpGetMore::decode(header, payload) {
            Ok(m) => m,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        let mut cursors = self.cursors.lock().unwrap();
        match cursors.get_more(msg.context_id, msg.num_to_return) {
            Some((batch, exhausted)) => {
                let mut reply =
                    MsgOpReply::ok(header.opcode, header.request_id, batch);
                reply.context_id = if exhausted { -1 } else { msg.context_id };
                reply
            }
            None => MsgOpReply::error(header.opcode, header.request_id, &SdbError::QueryNotFound),
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

    // ── Transaction handlers ─────────────────────────────────────────

    async fn handle_trans_begin(&self, header: &MsgHeader, addr: &SocketAddr) -> MsgOpReply {
        let mut group_txns = HashMap::new();
        for (&gid, client) in &self.clients {
            match client.transaction_begin().await {
                Ok(txn_id) => { group_txns.insert(gid, txn_id); }
                Err(e) => {
                    // Rollback already started txns
                    for (&rgid, _) in &group_txns {
                        let _ = self.clients[&rgid].transaction_rollback().await;
                    }
                    return MsgOpReply::error(header.opcode, header.request_id, &e);
                }
            }
        }
        self.coord_txns.lock().unwrap_or_else(|e| e.into_inner())
            .insert(*addr, CoordTxnState { group_txns });

        let mut doc = Document::new();
        doc.insert("txn_id", Value::Int64(1)); // coord-level placeholder
        MsgOpReply::ok(header.opcode, header.request_id, vec![doc])
    }

    async fn handle_trans_commit(&self, header: &MsgHeader, addr: &SocketAddr) -> MsgOpReply {
        let state = self.coord_txns.lock().unwrap_or_else(|e| e.into_inner()).remove(addr);
        match state {
            Some(state) => {
                let mut committed = Vec::new();
                for (&gid, _) in &state.group_txns {
                    match self.clients[&gid].transaction_commit().await {
                        Ok(()) => committed.push(gid),
                        Err(e) => {
                            // Rollback uncommitted groups
                            for (&rgid, _) in &state.group_txns {
                                if !committed.contains(&rgid) {
                                    let _ = self.clients[&rgid].transaction_rollback().await;
                                }
                            }
                            return MsgOpReply::error(header.opcode, header.request_id, &e);
                        }
                    }
                }
                MsgOpReply::ok(header.opcode, header.request_id, vec![])
            }
            None => MsgOpReply::error(header.opcode, header.request_id, &SdbError::TransactionError),
        }
    }

    async fn handle_trans_rollback(&self, header: &MsgHeader, addr: &SocketAddr) -> MsgOpReply {
        let state = self.coord_txns.lock().unwrap_or_else(|e| e.into_inner()).remove(addr);
        if let Some(_state) = state {
            for (_, client) in &self.clients {
                let _ = client.transaction_rollback().await;
            }
        }
        MsgOpReply::ok(header.opcode, header.request_id, vec![])
    }

    // ── Shard management ──────────────────────────────────────────────

    fn cmd_enable_sharding(&self, query: &MsgOpQuery) -> Result<()> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;
        let collection = match cond.get("Collection") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let shard_key = match cond.get("ShardKey") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let num_groups = match cond.get("NumGroups") {
            Some(Value::Int32(n)) => *n as u32,
            Some(Value::Int64(n)) => *n as u32,
            _ => self.clients.len() as u32,
        };
        self.set_shard(&collection, &shard_key, num_groups)
    }

    fn cmd_get_shard_info(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;
        let collection = match cond.get("Collection") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let router = self.router.read().map_err(|_| SdbError::Sys)?;
        let mut result = Document::new();
        if let Some((shard_key, num_groups)) = router.shard_info(&collection) {
            result.insert("Sharded", Value::Boolean(true));
            result.insert("ShardKey", Value::String(shard_key));
            result.insert("NumGroups", Value::Int32(num_groups as i32));
        } else {
            result.insert("Sharded", Value::Boolean(false));
        }
        Ok(vec![result])
    }

    // ── Count ───────────────────────────────────────────────────────

    pub async fn cmd_count(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;
        let collection = match cond.get("Collection") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };

        let filter_cond = match cond.get("Condition") {
            Some(Value::Document(d)) => Some(d.clone()),
            _ => None,
        };

        let mut total: u64 = 0;
        for client in self.clients.values() {
            total += client.count(&collection, filter_cond.clone()).await?;
        }

        let mut result = Document::new();
        result.insert("count", Value::Int64(total as i64));
        Ok(vec![result])
    }
}

#[async_trait]
impl MessageHandler for CoordNodeHandler {
    async fn on_message(
        &self,
        conn: &mut Connection,
        header: MsgHeader,
        payload: &[u8],
    ) -> Result<()> {
        let opcode = OpCode::from_i32(header.opcode);
        let addr = conn.addr;
        tracing::debug!("Coord received opcode {:?} from {}", opcode, addr);

        let reply = match opcode {
            Some(OpCode::QueryReq) => self.handle_query(&header, payload).await,
            Some(OpCode::InsertReq) => self.handle_insert(&header, payload).await,
            Some(OpCode::UpdateReq) => self.handle_update(&header, payload).await,
            Some(OpCode::DeleteReq) => self.handle_delete(&header, payload).await,
            Some(OpCode::GetMoreReq) => self.handle_get_more(&header, payload),
            Some(OpCode::KillContextReq) => self.handle_kill_context(&header, payload),
            Some(OpCode::TransBeginReq) => self.handle_trans_begin(&header, &addr).await,
            Some(OpCode::TransCommitReq) => self.handle_trans_commit(&header, &addr).await,
            Some(OpCode::TransRollbackReq) => self.handle_trans_rollback(&header, &addr).await,
            Some(OpCode::Disconnect) => return Err(SdbError::NetworkClose),
            _ => MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg),
        };

        conn.send_reply(&reply).await
    }

    async fn on_connect(&self, conn: &Connection) -> Result<()> {
        tracing::info!("Coord client connected: {}", conn.addr);
        self.metrics.inc_sessions();
        Ok(())
    }

    async fn on_disconnect(&self, conn: &Connection) -> Result<()> {
        tracing::info!("Coord client disconnected: {}", conn.addr);
        self.metrics.dec_sessions();
        Ok(())
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn get_string_field(doc: Option<&Document>, key: &str) -> Result<String> {
    let doc = doc.ok_or(SdbError::InvalidArg)?;
    match doc.get(key) {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(SdbError::InvalidArg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::DataNodeHandler;
    use sdb_cat::CatalogManager;
    use tokio::net::TcpListener;

    fn doc(pairs: &[(&str, Value)]) -> Document {
        let mut d = Document::new();
        for (k, v) in pairs {
            d.insert(*k, v.clone());
        }
        d
    }

    /// Start a DataNode server on ephemeral port, return port.
    async fn start_data_node() -> u16 {
        let catalog = Arc::new(std::sync::RwLock::new(CatalogManager::new()));
        let handler: Arc<dyn MessageHandler> = Arc::new(DataNodeHandler::new(catalog));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let (stream, addr) = listener.accept().await.unwrap();
                let handler = handler.clone();
                tokio::spawn(async move {
                    let mut conn = sdb_net::Connection::new(stream, addr);
                    let _ = handler.on_connect(&conn).await;
                    while let Ok((header, payload)) = conn.recv_msg().await {
                        if handler.on_message(&mut conn, header, &payload).await.is_err() {
                            break;
                        }
                    }
                    let _ = handler.on_disconnect(&conn).await;
                });
            }
        });
        port
    }

    #[tokio::test]
    async fn create_coord_with_groups() {
        let p1 = start_data_node().await;
        let p2 = start_data_node().await;
        let p3 = start_data_node().await;
        let coord = CoordNodeHandler::new(vec![
            (1, format!("127.0.0.1:{}", p1)),
            (2, format!("127.0.0.1:{}", p2)),
            (3, format!("127.0.0.1:{}", p3)),
        ]);
        assert_eq!(coord.all_group_ids().len(), 3);
    }

    #[tokio::test]
    async fn broadcast_ddl_to_all_groups() {
        let p1 = start_data_node().await;
        let p2 = start_data_node().await;
        let coord = CoordNodeHandler::new(vec![
            (1, format!("127.0.0.1:{}", p1)),
            (2, format!("127.0.0.1:{}", p2)),
        ]);

        // Broadcast create CS
        for client in coord.clients.values() {
            client.create_collection_space("mycs").await.unwrap();
        }

        // Verify by creating a collection in the CS on each
        for client in coord.clients.values() {
            client.create_collection("mycs.cl").await.unwrap();
        }
    }

    #[tokio::test]
    async fn scatter_query_merges_results() {
        let p1 = start_data_node().await;
        let p2 = start_data_node().await;
        let coord = CoordNodeHandler::new(vec![
            (1, format!("127.0.0.1:{}", p1)),
            (2, format!("127.0.0.1:{}", p2)),
        ]);

        // Create CS/CL on all groups
        for client in coord.clients.values() {
            client.create_collection_space("cs").await.unwrap();
            client.create_collection("cs.cl").await.unwrap();
        }

        // Insert one doc directly into each data node
        coord.clients[&1]
            .insert("cs.cl", vec![doc(&[("x", Value::Int32(1))])])
            .await
            .unwrap();
        coord.clients[&2]
            .insert("cs.cl", vec![doc(&[("x", Value::Int32(2))])])
            .await
            .unwrap();

        // Scatter query via coord
        let query = MsgOpQuery::new(1, "cs.cl", None, None, None, None, 0, -1, 0);
        let docs = coord.execute_scatter_query(&query).await.unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[tokio::test]
    async fn count_across_groups() {
        let p1 = start_data_node().await;
        let p2 = start_data_node().await;
        let coord = CoordNodeHandler::new(vec![
            (1, format!("127.0.0.1:{}", p1)),
            (2, format!("127.0.0.1:{}", p2)),
        ]);

        for client in coord.clients.values() {
            client.create_collection_space("cs").await.unwrap();
            client.create_collection("cs.cl").await.unwrap();
        }

        coord.clients[&1]
            .insert("cs.cl", vec![doc(&[("x", Value::Int32(1))])])
            .await
            .unwrap();
        coord.clients[&2]
            .insert("cs.cl", vec![doc(&[("x", Value::Int32(2))])])
            .await
            .unwrap();

        let mut cond = Document::new();
        cond.insert("Collection", Value::String("cs.cl".into()));
        let query = MsgOpQuery::new(1, "$count", Some(cond), None, None, None, 0, -1, 0);
        let result = coord.cmd_count(&query).await.unwrap();
        assert_eq!(result[0].get("count"), Some(&Value::Int64(2)));
    }
}
