use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, RwLock};

use async_trait::async_trait;
use sdb_bson::{Document, Value};
use sdb_cat::CatalogManager;
use sdb_cls::ShardManager;
use sdb_common::{GroupId, RecordId, Result, SdbError};
use sdb_coord::CoordRouter;
use sdb_mth::{Matcher, Modifier};
use sdb_msg::header::MsgHeader;
use sdb_msg::opcode::OpCode;
use sdb_msg::reply::MsgOpReply;
use sdb_msg::request::*;
use sdb_net::Connection;
use sdb_net::MessageHandler;
use sdb_opt::Optimizer;
use sdb_rtn::{CollectionHandle, DefaultExecutor, Executor};

use crate::cursor_manager::CursorManager;

/// Coordinator node handler — routes requests across multiple data groups.
///
/// V1: in-process routing. Each group has its own CatalogManager.
/// No real network between coord and data nodes.
pub struct CoordNodeHandler {
    router: RwLock<CoordRouter>,
    groups: HashMap<GroupId, Arc<RwLock<CatalogManager>>>,
    cursors: StdMutex<CursorManager>,
}

impl CoordNodeHandler {
    /// Create a coordinator with a set of data groups.
    pub fn new(group_ids: &[GroupId]) -> Self {
        let mut groups = HashMap::new();
        for &gid in group_ids {
            groups.insert(gid, Arc::new(RwLock::new(CatalogManager::new())));
        }
        Self {
            router: RwLock::new(CoordRouter::new()),
            groups,
            cursors: StdMutex::new(CursorManager::new()),
        }
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

    /// Get a reference to a group's catalog.
    pub fn group_catalog(&self, group_id: GroupId) -> Result<&Arc<RwLock<CatalogManager>>> {
        self.groups.get(&group_id).ok_or(SdbError::NodeNotFound)
    }

    fn all_group_ids(&self) -> Vec<GroupId> {
        self.groups.keys().copied().collect()
    }

    // ── Query dispatch ──────────────────────────────────────────────

    async fn handle_query(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let query = match MsgOpQuery::decode(header, payload) {
            Ok(q) => q,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        if query.name.starts_with('$') {
            self.handle_command(header, &query)
        } else {
            self.handle_data_query(header, &query).await
        }
    }

    fn handle_command(&self, header: &MsgHeader, query: &MsgOpQuery) -> MsgOpReply {
        let result = match query.name.as_str() {
            "$create collectionspace" => self.cmd_broadcast_ddl(|cat| {
                let name = get_string_field(query.condition.as_ref(), "Name")?;
                cat.create_collection_space(&name)?;
                Ok(())
            }),
            "$drop collectionspace" => self.cmd_broadcast_ddl(|cat| {
                let name = get_string_field(query.condition.as_ref(), "Name")?;
                cat.drop_collection_space(&name)?;
                Ok(())
            }),
            "$create collection" => self.cmd_broadcast_ddl(|cat| {
                let full_name = get_string_field(query.condition.as_ref(), "Name")?;
                let (cs, cl) =
                    full_name.split_once('.').ok_or(SdbError::InvalidArg)?;
                cat.create_collection(cs, cl)?;
                Ok(())
            }),
            "$drop collection" => self.cmd_broadcast_ddl(|cat| {
                let full_name = get_string_field(query.condition.as_ref(), "Name")?;
                let (cs, cl) =
                    full_name.split_once('.').ok_or(SdbError::InvalidArg)?;
                cat.drop_collection(cs, cl)?;
                Ok(())
            }),
            "$create index" => self.cmd_broadcast_index_ddl(query, true),
            "$drop index" => self.cmd_broadcast_index_ddl(query, false),
            "$count" => self.cmd_count(query),
            _ => Err(SdbError::InvalidArg),
        };
        match result {
            Ok(docs) => MsgOpReply::ok(header.opcode, header.request_id, docs),
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    /// Broadcast a DDL operation to all groups.
    fn cmd_broadcast_ddl<F>(&self, op: F) -> Result<Vec<Document>>
    where
        F: Fn(&mut CatalogManager) -> Result<()>,
    {
        for cat_lock in self.groups.values() {
            let mut cat = cat_lock.write().map_err(|_| SdbError::Sys)?;
            op(&mut cat)?;
        }
        Ok(vec![])
    }

    fn cmd_broadcast_index_ddl(
        &self,
        query: &MsgOpQuery,
        create: bool,
    ) -> Result<Vec<Document>> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;
        let full_name = match cond.get("Collection") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let (cs, cl) = full_name.split_once('.').ok_or(SdbError::InvalidArg)?;

        if create {
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

            for cat_lock in self.groups.values() {
                let mut cat = cat_lock.write().map_err(|_| SdbError::Sys)?;
                cat.create_index(cs, cl, &idx_name, key_pattern.clone(), unique)?;
            }
        } else {
            let idx_name = match cond.get("Index") {
                Some(Value::String(s)) => s.clone(),
                _ => return Err(SdbError::InvalidArg),
            };
            for cat_lock in self.groups.values() {
                let mut cat = cat_lock.write().map_err(|_| SdbError::Sys)?;
                cat.drop_index(cs, cl, &idx_name)?;
            }
        }
        Ok(vec![])
    }

    async fn handle_data_query(&self, header: &MsgHeader, query: &MsgOpQuery) -> MsgOpReply {
        let result = self.execute_scatter_query(query).await;
        match result {
            Ok(docs) => {
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
    /// Broadcasts to all groups when no sharding is configured for the collection.
    async fn execute_scatter_query(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let target_groups = {
            let router = self.router.read().map_err(|_| SdbError::Sys)?;
            let routed = router.route_query(&query.name, query.condition.as_ref())?;
            // If only default group returned and we have multiple groups,
            // broadcast to all (unsharded collection).
            if routed.len() == 1 && self.groups.len() > 1 && !self.groups.contains_key(&routed[0]) {
                self.groups.keys().copied().collect()
            } else if routed.len() == 1 && self.groups.len() > 1 {
                // Check if there's actually a shard registered
                let has_shard = router.route_query(&query.name, None)
                    .map(|v| v.len() > 1)
                    .unwrap_or(false);
                if has_shard {
                    routed
                } else {
                    // No sharding — broadcast to all groups
                    self.groups.keys().copied().collect()
                }
            } else {
                routed
            }
        }; // router lock dropped here before any .await

        let mut all_docs = Vec::new();

        for &gid in &target_groups {
            let cat_lock = self.groups.get(&gid).ok_or(SdbError::NodeNotFound)?;
            let docs = execute_query_on_group(cat_lock, query).await?;
            all_docs.extend(docs);
        }

        Ok(all_docs)
    }

    fn handle_insert(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let msg = match MsgOpInsert::decode(header, payload) {
            Ok(m) => m,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        let result = (|| -> Result<()> {
            let (cs, cl) = msg.name.split_once('.').ok_or(SdbError::InvalidArg)?;
            let router = self.router.read().map_err(|_| SdbError::Sys)?;

            for doc in &msg.docs {
                let gid = router.route_insert(&msg.name, doc)?;
                let cat_lock = self.groups.get(&gid).ok_or(SdbError::NodeNotFound)?;
                let catalog = cat_lock.read().map_err(|_| SdbError::Sys)?;
                catalog.insert_document(cs, cl, doc)?;
            }
            Ok(())
        })();

        match result {
            Ok(()) => MsgOpReply::ok(header.opcode, header.request_id, vec![]),
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    fn handle_update(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let msg = match MsgOpUpdate::decode(header, payload) {
            Ok(m) => m,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        let result = (|| -> Result<i32> {
            let (cs, cl) = msg.name.split_once('.').ok_or(SdbError::InvalidArg)?;
            let router = self.router.read().map_err(|_| SdbError::Sys)?;
            let target_groups =
                router.route_update(&msg.name, Some(&msg.condition))?;
            drop(router);

            let matcher = Matcher::new(msg.condition.clone())?;
            let modifier = Modifier::new(msg.modifier.clone())?;
            let mut total = 0i32;

            for &gid in &target_groups {
                let cat_lock = self.groups.get(&gid).ok_or(SdbError::NodeNotFound)?;
                let catalog = cat_lock.read().map_err(|_| SdbError::Sys)?;
                let (storage, _) = catalog.collection_handle(cs, cl)?;
                let rows = storage.scan();

                let matching: Vec<RecordId> = rows
                    .iter()
                    .filter(|(_, doc)| matcher.matches(doc).unwrap_or(false))
                    .map(|(rid, _)| *rid)
                    .collect();

                for rid in matching {
                    let old = storage.find(rid)?;
                    let new = modifier.modify(&old)?;
                    catalog.update_document(cs, cl, rid, &new)?;
                    total += 1;
                }
            }
            Ok(total)
        })();

        match result {
            Ok(_) => MsgOpReply::ok(header.opcode, header.request_id, vec![]),
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    fn handle_delete(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let msg = match MsgOpDelete::decode(header, payload) {
            Ok(m) => m,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        let result = (|| -> Result<i32> {
            let (cs, cl) = msg.name.split_once('.').ok_or(SdbError::InvalidArg)?;
            let router = self.router.read().map_err(|_| SdbError::Sys)?;
            let target_groups =
                router.route_delete(&msg.name, Some(&msg.condition))?;
            drop(router);

            let matcher = Matcher::new(msg.condition.clone())?;
            let mut total = 0i32;

            for &gid in &target_groups {
                let cat_lock = self.groups.get(&gid).ok_or(SdbError::NodeNotFound)?;
                let catalog = cat_lock.read().map_err(|_| SdbError::Sys)?;
                let (storage, _) = catalog.collection_handle(cs, cl)?;
                let rows = storage.scan();

                let matching: Vec<RecordId> = rows
                    .iter()
                    .filter(|(_, doc)| matcher.matches(doc).unwrap_or(false))
                    .map(|(rid, _)| *rid)
                    .collect();

                for rid in matching {
                    catalog.delete_document(cs, cl, rid)?;
                    total += 1;
                }
            }
            Ok(total)
        })();

        match result {
            Ok(_) => MsgOpReply::ok(header.opcode, header.request_id, vec![]),
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

    fn cmd_count(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;
        let collection = match cond.get("Collection") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let (cs, cl) = collection.split_once('.').ok_or(SdbError::InvalidArg)?;

        let filter_cond = match cond.get("Condition") {
            Some(Value::Document(d)) => Some(d.clone()),
            _ => None,
        };

        let mut total: u64 = 0;
        for cat_lock in self.groups.values() {
            let catalog = cat_lock.read().map_err(|_| SdbError::Sys)?;
            if let Ok((storage, _)) = catalog.collection_handle(cs, cl) {
                if let Some(ref fc) = filter_cond {
                    let matcher = Matcher::new(fc.clone())?;
                    let rows = storage.scan();
                    total += rows
                        .iter()
                        .filter(|(_, doc)| matcher.matches(doc).unwrap_or(false))
                        .count() as u64;
                } else {
                    total += storage.total_records();
                }
            }
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
        tracing::debug!("Coord received opcode {:?} from {}", opcode, conn.addr);

        let reply = match opcode {
            Some(OpCode::QueryReq) => self.handle_query(&header, payload).await,
            Some(OpCode::InsertReq) => self.handle_insert(&header, payload),
            Some(OpCode::UpdateReq) => self.handle_update(&header, payload),
            Some(OpCode::DeleteReq) => self.handle_delete(&header, payload),
            Some(OpCode::GetMoreReq) => self.handle_get_more(&header, payload),
            Some(OpCode::KillContextReq) => self.handle_kill_context(&header, payload),
            Some(OpCode::Disconnect) => return Err(SdbError::NetworkClose),
            _ => MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg),
        };

        conn.send_reply(&reply).await
    }

    async fn on_connect(&self, conn: &Connection) -> Result<()> {
        tracing::info!("Coord client connected: {}", conn.addr);
        Ok(())
    }

    async fn on_disconnect(&self, conn: &Connection) -> Result<()> {
        tracing::info!("Coord client disconnected: {}", conn.addr);
        Ok(())
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Execute a query on a single group's catalog.
async fn execute_query_on_group(
    cat_lock: &Arc<RwLock<CatalogManager>>,
    query: &MsgOpQuery,
) -> Result<Vec<Document>> {
    let (cs, cl) = query.name.split_once('.').ok_or(SdbError::InvalidArg)?;

    let (plan, executor) = {
        let catalog = cat_lock.read().map_err(|_| SdbError::Sys)?;
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

    fn doc(pairs: &[(&str, Value)]) -> Document {
        let mut d = Document::new();
        for (k, v) in pairs {
            d.insert(*k, v.clone());
        }
        d
    }

    #[test]
    fn create_coord_with_groups() {
        let coord = CoordNodeHandler::new(&[1, 2, 3]);
        assert_eq!(coord.all_group_ids().len(), 3);
    }

    #[test]
    fn broadcast_ddl_to_all_groups() {
        let coord = CoordNodeHandler::new(&[1, 2]);

        // Create CS on all groups
        coord
            .cmd_broadcast_ddl(|cat| {
                cat.create_collection_space("mycs")?;
                Ok(())
            })
            .unwrap();

        // Verify both groups have the CS
        for gid in &[1, 2] {
            let cat = coord.group_catalog(*gid).unwrap();
            let c = cat.read().unwrap();
            assert!(c.list_collection_spaces().contains(&"mycs".to_string()));
        }
    }

    #[test]
    fn insert_routes_to_group() {
        let coord = CoordNodeHandler::new(&[1, 2, 3]);

        // Create CS/CL on all groups
        coord
            .cmd_broadcast_ddl(|cat| {
                cat.create_collection_space("cs")?;
                cat.create_collection("cs", "cl")?;
                Ok(())
            })
            .unwrap();

        // Set up hash sharding
        coord.set_shard("cs.cl", "x", 3).unwrap();

        // Insert a doc — goes to one group
        let header = MsgHeader::new_request(OpCode::InsertReq as i32, 1);
        let msg = MsgOpInsert::new(1, "cs.cl", vec![doc(&[("x", Value::Int32(42))])], 0);
        let bytes = msg.encode();
        let payload = &bytes[MsgHeader::SIZE..];
        let reply = coord.handle_insert(&header, payload);
        assert_eq!(reply.flags, 0);

        // Count total docs across all groups
        let mut total = 0u64;
        for gid in &[1, 2, 3] {
            let cat = coord.group_catalog(*gid).unwrap();
            let c = cat.read().unwrap();
            if let Ok((s, _)) = c.collection_handle("cs", "cl") {
                total += s.total_records();
            }
        }
        assert_eq!(total, 1);
    }

    #[tokio::test]
    async fn scatter_query_merges_results() {
        let coord = CoordNodeHandler::new(&[1, 2]);

        coord
            .cmd_broadcast_ddl(|cat| {
                cat.create_collection_space("cs")?;
                cat.create_collection("cs", "cl")?;
                Ok(())
            })
            .unwrap();

        // Insert one doc directly into each group
        {
            let cat1 = coord.group_catalog(1).unwrap().read().unwrap();
            cat1.insert_document("cs", "cl", &doc(&[("x", Value::Int32(1))]))
                .unwrap();
        }
        {
            let cat2 = coord.group_catalog(2).unwrap().read().unwrap();
            cat2.insert_document("cs", "cl", &doc(&[("x", Value::Int32(2))]))
                .unwrap();
        }

        // Query all — should merge results from both groups
        let query = MsgOpQuery::new(1, "cs.cl", None, None, None, None, 0, -1, 0);
        let docs = coord.execute_scatter_query(&query).await.unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn count_across_groups() {
        let coord = CoordNodeHandler::new(&[1, 2]);

        coord
            .cmd_broadcast_ddl(|cat| {
                cat.create_collection_space("cs")?;
                cat.create_collection("cs", "cl")?;
                Ok(())
            })
            .unwrap();

        // Insert docs directly
        for gid in &[1, 2] {
            let cat = coord.group_catalog(*gid).unwrap().read().unwrap();
            cat.insert_document("cs", "cl", &doc(&[("x", Value::Int32(*gid as i32))]))
                .unwrap();
        }

        let mut cond = Document::new();
        cond.insert("Collection", Value::String("cs.cl".into()));
        let query = MsgOpQuery::new(1, "$count", Some(cond), None, None, None, 0, -1, 0);
        let result = coord.cmd_count(&query).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("count"), Some(&Value::Int64(2)));
    }
}
