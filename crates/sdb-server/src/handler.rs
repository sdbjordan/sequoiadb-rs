use std::sync::{Arc, Mutex as StdMutex, RwLock};

use async_trait::async_trait;
use sdb_aggr::{Pipeline, Stage, StageType};
use sdb_auth::AuthManager;
use sdb_bson::{Document, Value};
use sdb_cat::CatalogManager;
use sdb_common::{RecordId, Result, SdbError};
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

use crate::cursor_manager::CursorManager;

/// Data node message handler — dispatches CRUD, DDL, auth, aggregate, and SQL commands.
pub struct DataNodeHandler {
    catalog: Arc<RwLock<CatalogManager>>,
    auth: Arc<RwLock<AuthManager>>,
    cursors: Arc<StdMutex<CursorManager>>,
}

impl DataNodeHandler {
    pub fn new(catalog: Arc<RwLock<CatalogManager>>) -> Self {
        Self {
            catalog,
            auth: Arc::new(RwLock::new(AuthManager::new())),
            cursors: Arc::new(StdMutex::new(CursorManager::new())),
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
        }
    }

    /// Direct access to catalog — used by CoordNodeHandler for in-process routing.
    pub fn catalog(&self) -> &Arc<RwLock<CatalogManager>> {
        &self.catalog
    }

    // ── Command dispatch ────────────────────────────────────────────

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
            // DDL
            "$create collectionspace" => self.cmd_create_cs(query),
            "$drop collectionspace" => self.cmd_drop_cs(query),
            "$create collection" => self.cmd_create_cl(query),
            "$drop collection" => self.cmd_drop_cl(query),
            "$create index" => self.cmd_create_index(query),
            "$drop index" => self.cmd_drop_index(query),
            // Auth
            "$authenticate" => self.cmd_authenticate(query),
            "$create user" => self.cmd_create_user(query),
            "$drop user" => self.cmd_drop_user(query),
            // Aggregate
            "$aggregate" => self.cmd_aggregate(query),
            // SQL — async path
            "$sql" => return self.handle_sql_command(header, query).await,
            // Count
            "$count" => self.cmd_count(query),
            _ => Err(SdbError::InvalidArg),
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
                // Use cursor manager for batching
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

    fn handle_insert(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let msg = match MsgOpInsert::decode(header, payload) {
            Ok(m) => m,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        let result = (|| -> Result<()> {
            let (cs, cl) = parse_collection_name(&msg.name)?;
            let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;
            for doc in &msg.docs {
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

            let mut count = 0i32;
            for rid in matching {
                let old_doc = storage.find(rid)?;
                let new_doc = modifier.modify(&old_doc)?;
                catalog.update_document(cs, cl, rid, &new_doc)?;
                count += 1;
            }
            Ok(count)
        })();

        match result {
            Ok(_count) => MsgOpReply::ok(header.opcode, header.request_id, vec![]),
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    fn handle_delete(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let msg = match MsgOpDelete::decode(header, payload) {
            Ok(m) => m,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        let result = (|| -> Result<i32> {
            let (cs, cl) = parse_collection_name(&msg.name)?;
            let catalog = self.catalog.read().map_err(|_| SdbError::Sys)?;

            let (storage, _indexes) = catalog.collection_handle(cs, cl)?;
            let rows = storage.scan();
            let matcher = Matcher::new(msg.condition.clone())?;

            let matching: Vec<RecordId> = rows
                .iter()
                .filter(|(_, doc)| matcher.matches(doc).unwrap_or(false))
                .map(|(rid, _)| *rid)
                .collect();

            let mut count = 0i32;
            for rid in matching {
                catalog.delete_document(cs, cl, rid)?;
                count += 1;
            }
            Ok(count)
        })();

        match result {
            Ok(_count) => MsgOpReply::ok(header.opcode, header.request_id, vec![]),
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

    // ── DDL commands ────────────────────────────────────────────────

    fn cmd_create_cs(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let name = get_string_field(query.condition.as_ref(), "Name")?;
        let mut catalog = self.catalog.write().map_err(|_| SdbError::Sys)?;
        catalog.create_collection_space(&name)?;
        Ok(vec![])
    }

    fn cmd_drop_cs(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let name = get_string_field(query.condition.as_ref(), "Name")?;
        let mut catalog = self.catalog.write().map_err(|_| SdbError::Sys)?;
        catalog.drop_collection_space(&name)?;
        Ok(vec![])
    }

    fn cmd_create_cl(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let full_name = get_string_field(query.condition.as_ref(), "Name")?;
        let (cs, cl) = parse_collection_name(&full_name)?;
        let mut catalog = self.catalog.write().map_err(|_| SdbError::Sys)?;
        catalog.create_collection(cs, cl)?;
        Ok(vec![])
    }

    fn cmd_drop_cl(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let full_name = get_string_field(query.condition.as_ref(), "Name")?;
        let (cs, cl) = parse_collection_name(&full_name)?;
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

        let mut catalog = self.catalog.write().map_err(|_| SdbError::Sys)?;
        catalog.drop_index(cs, cl, &idx_name)?;
        Ok(vec![])
    }

    // ── Auth commands ───────────────────────────────────────────────

    fn cmd_authenticate(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
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
        tracing::debug!("Received opcode {:?} from {}", opcode, conn.addr);

        let reply = match opcode {
            Some(OpCode::QueryReq) => self.handle_query(&header, payload).await,
            Some(OpCode::InsertReq) => self.handle_insert(&header, payload),
            Some(OpCode::UpdateReq) => self.handle_update(&header, payload),
            Some(OpCode::DeleteReq) => self.handle_delete(&header, payload),
            Some(OpCode::GetMoreReq) => self.handle_get_more(&header, payload),
            Some(OpCode::KillContextReq) => self.handle_kill_context(&header, payload),
            Some(OpCode::AggregateReq) => {
                // AggregateReq uses same format as QueryReq with $aggregate command
                self.handle_query(&header, payload).await
            }
            Some(OpCode::SqlReq) => {
                // SqlReq uses same format as QueryReq with $sql command
                self.handle_query(&header, payload).await
            }
            Some(OpCode::Disconnect) => return Err(SdbError::NetworkClose),
            _ => MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg),
        };

        conn.send_reply(&reply).await
    }

    async fn on_connect(&self, conn: &Connection) -> Result<()> {
        tracing::info!("Client connected: {}", conn.addr);
        Ok(())
    }

    async fn on_disconnect(&self, conn: &Connection) -> Result<()> {
        tracing::info!("Client disconnected: {}", conn.addr);
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
