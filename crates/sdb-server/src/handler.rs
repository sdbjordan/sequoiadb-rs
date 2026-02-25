use std::sync::{Arc, RwLock};

use async_trait::async_trait;
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

/// Data node message handler — dispatches CRUD and DDL commands.
pub struct DataNodeHandler {
    catalog: Arc<RwLock<CatalogManager>>,
}

impl DataNodeHandler {
    pub fn new(catalog: Arc<RwLock<CatalogManager>>) -> Self {
        Self { catalog }
    }

    // ── Command dispatch ────────────────────────────────────────────

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
            "$create collectionspace" => self.cmd_create_cs(query),
            "$drop collectionspace" => self.cmd_drop_cs(query),
            "$create collection" => self.cmd_create_cl(query),
            "$drop collection" => self.cmd_drop_cl(query),
            "$create index" => self.cmd_create_index(query),
            "$drop index" => self.cmd_drop_index(query),
            _ => Err(SdbError::InvalidArg),
        };
        match result {
            Ok(docs) => MsgOpReply::ok(header.opcode, header.request_id, docs),
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    async fn handle_data_query(&self, header: &MsgHeader, query: &MsgOpQuery) -> MsgOpReply {
        let result = self.execute_query(query).await;
        match result {
            Ok(docs) => MsgOpReply::ok(header.opcode, header.request_id, docs),
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    async fn execute_query(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let (cs, cl) = parse_collection_name(&query.name)?;

        // Gather everything we need under the lock, then drop it before await
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
        }; // catalog lock dropped here

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

            // Find matching documents
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

    fn handle_get_more(&self, header: &MsgHeader, _payload: &[u8]) -> MsgOpReply {
        // V1: no cursor support, always returns context-not-found
        MsgOpReply::error(header.opcode, header.request_id, &SdbError::QueryNotFound)
    }

    fn handle_kill_context(&self, header: &MsgHeader, _payload: &[u8]) -> MsgOpReply {
        // V1: no cursor support, just acknowledge
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
