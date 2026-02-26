use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};

use async_trait::async_trait;
use sdb_bson::{Document, Value};
use sdb_common::{Result, SdbError};
use sdb_msg::header::MsgHeader;
use sdb_msg::opcode::OpCode;
use sdb_msg::reply::MsgOpReply;
use sdb_msg::request::*;
use sdb_net::Connection;
use sdb_net::MessageHandler;

/// Persisted catalog metadata: collection spaces, collections, groups, shard configs.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct CatalogMeta {
    pub collection_spaces: Vec<CsEntry>,
    pub groups: Vec<GroupEntry>,
    pub shard_configs: Vec<ShardEntry>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CsEntry {
    pub name: String,
    pub collections: Vec<ClEntry>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClEntry {
    pub name: String,
    pub full_name: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GroupEntry {
    pub group_id: u32,
    pub name: String,
    pub nodes: Vec<NodeEntry>,
    pub primary_node: Option<u16>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeEntry {
    pub node_id: u16,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ShardEntry {
    pub collection: String,
    pub shard_key: String,
    pub shard_type: String, // "hash" or "range"
    pub num_groups: u32,
    pub ranges: Vec<RangeEntry>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RangeEntry {
    pub group_id: u32,
    pub low_bound: Option<i64>,
    pub up_bound: Option<i64>,
}

/// Catalog node handler — manages cluster metadata and persists to disk.
pub struct CatalogNodeHandler {
    meta: RwLock<CatalogMeta>,
    persist_path: String,
    dirty: Mutex<bool>,
}

impl CatalogNodeHandler {
    pub fn new(db_path: &str) -> Self {
        let persist_path = Self::meta_file_path(db_path);
        let meta = Self::load_or_default(&persist_path);
        Self {
            meta: RwLock::new(meta),
            persist_path,
            dirty: Mutex::new(false),
        }
    }

    fn meta_file_path(db_path: &str) -> String {
        let mut p = PathBuf::from(db_path);
        p.push("catalog_meta.json");
        p.to_string_lossy().to_string()
    }

    fn load_or_default(path: &str) -> CatalogMeta {
        match std::fs::read_to_string(path) {
            Ok(data) => serde_json::from_str(&data).unwrap_or_default(),
            Err(_) => CatalogMeta::default(),
        }
    }

    fn persist(&self) {
        let meta = match self.meta.read() {
            Ok(m) => m.clone(),
            Err(_) => return,
        };
        if let Ok(json) = serde_json::to_string_pretty(&meta) {
            if let Some(parent) = Path::new(&self.persist_path).parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::write(&self.persist_path, json);
            if let Ok(mut d) = self.dirty.lock() {
                *d = false;
            }
        }
    }

    fn mark_dirty(&self) {
        if let Ok(mut d) = self.dirty.lock() {
            *d = true;
        }
    }

    /// Get a read-only snapshot of the metadata.
    pub fn snapshot(&self) -> CatalogMeta {
        self.meta.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    // ── Command dispatch ────────────────────────────────────────────

    fn handle_query(&self, header: &MsgHeader, payload: &[u8]) -> MsgOpReply {
        let query = match MsgOpQuery::decode(header, payload) {
            Ok(q) => q,
            Err(e) => return MsgOpReply::error(header.opcode, header.request_id, &e),
        };

        let result = match query.name.as_str() {
            "$register group" => self.cmd_register_group(&query),
            "$remove group" => self.cmd_remove_group(&query),
            "$list groups" => self.cmd_list_groups(),
            "$register shard" => self.cmd_register_shard(&query),
            "$list shards" => self.cmd_list_shards(),
            "$list collections" => self.cmd_list_collections(),
            "$catalog snapshot" => self.cmd_snapshot(),
            "$create collectionspace" => self.cmd_create_cs(&query),
            "$drop collectionspace" => self.cmd_drop_cs(&query),
            "$create collection" => self.cmd_create_cl(&query),
            "$drop collection" => self.cmd_drop_cl(&query),
            _ => Err(SdbError::InvalidArg),
        };

        match result {
            Ok(docs) => MsgOpReply::ok(header.opcode, header.request_id, docs),
            Err(e) => MsgOpReply::error(header.opcode, header.request_id, &e),
        }
    }

    fn handle_catalog_req(&self, header: &MsgHeader, _payload: &[u8]) -> MsgOpReply {
        // CatalogReq: return full catalog metadata as a BSON document
        let meta = self.snapshot();
        let mut doc = Document::new();
        // Collection spaces
        let mut cs_list = Vec::new();
        for cs in &meta.collection_spaces {
            let mut cs_doc = Document::new();
            cs_doc.insert("Name", Value::String(cs.name.clone()));
            let cl_names: Vec<Value> = cs
                .collections
                .iter()
                .map(|cl| Value::String(cl.full_name.clone()))
                .collect();
            cs_doc.insert("Collections", Value::Array(cl_names));
            cs_list.push(Value::Document(cs_doc));
        }
        doc.insert("CollectionSpaces", Value::Array(cs_list));

        // Groups
        let mut grp_list = Vec::new();
        for g in &meta.groups {
            let mut gdoc = Document::new();
            gdoc.insert("GroupId", Value::Int32(g.group_id as i32));
            gdoc.insert("Name", Value::String(g.name.clone()));
            if let Some(pid) = g.primary_node {
                gdoc.insert("PrimaryNode", Value::Int32(pid as i32));
            }
            let nodes: Vec<Value> = g
                .nodes
                .iter()
                .map(|n| {
                    let mut ndoc = Document::new();
                    ndoc.insert("NodeId", Value::Int32(n.node_id as i32));
                    ndoc.insert("Host", Value::String(n.host.clone()));
                    ndoc.insert("Port", Value::Int32(n.port as i32));
                    Value::Document(ndoc)
                })
                .collect();
            gdoc.insert("Nodes", Value::Array(nodes));
            grp_list.push(Value::Document(gdoc));
        }
        doc.insert("Groups", Value::Array(grp_list));

        // Shard configs
        let mut shard_list = Vec::new();
        for s in &meta.shard_configs {
            let mut sdoc = Document::new();
            sdoc.insert("Collection", Value::String(s.collection.clone()));
            sdoc.insert("ShardKey", Value::String(s.shard_key.clone()));
            sdoc.insert("ShardType", Value::String(s.shard_type.clone()));
            sdoc.insert("NumGroups", Value::Int32(s.num_groups as i32));
            shard_list.push(Value::Document(sdoc));
        }
        doc.insert("ShardConfigs", Value::Array(shard_list));

        MsgOpReply::ok(header.opcode, header.request_id, vec![doc])
    }

    // ── DDL ─────────────────────────────────────────────────────────

    fn cmd_create_cs(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let name = get_string_field(query.condition.as_ref(), "Name")?;
        let mut meta = self.meta.write().map_err(|_| SdbError::Sys)?;
        if meta.collection_spaces.iter().any(|cs| cs.name == name) {
            return Err(SdbError::CollectionSpaceAlreadyExists);
        }
        meta.collection_spaces.push(CsEntry {
            name,
            collections: Vec::new(),
        });
        drop(meta);
        self.mark_dirty();
        self.persist();
        Ok(vec![])
    }

    fn cmd_drop_cs(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let name = get_string_field(query.condition.as_ref(), "Name")?;
        let mut meta = self.meta.write().map_err(|_| SdbError::Sys)?;
        let before = meta.collection_spaces.len();
        meta.collection_spaces.retain(|cs| cs.name != name);
        if meta.collection_spaces.len() == before {
            return Err(SdbError::CollectionSpaceNotFound);
        }
        drop(meta);
        self.mark_dirty();
        self.persist();
        Ok(vec![])
    }

    fn cmd_create_cl(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let full_name = get_string_field(query.condition.as_ref(), "Name")?;
        let parts: Vec<&str> = full_name.splitn(2, '.').collect();
        if parts.len() != 2 {
            return Err(SdbError::InvalidArg);
        }
        let (cs_name, cl_name) = (parts[0], parts[1]);
        let mut meta = self.meta.write().map_err(|_| SdbError::Sys)?;
        let cs = meta
            .collection_spaces
            .iter_mut()
            .find(|cs| cs.name == cs_name)
            .ok_or(SdbError::CollectionSpaceNotFound)?;
        if cs.collections.iter().any(|cl| cl.name == cl_name) {
            return Err(SdbError::CollectionAlreadyExists);
        }
        cs.collections.push(ClEntry {
            name: cl_name.to_string(),
            full_name: full_name.clone(),
        });
        drop(meta);
        self.mark_dirty();
        self.persist();
        Ok(vec![])
    }

    fn cmd_drop_cl(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let full_name = get_string_field(query.condition.as_ref(), "Name")?;
        let parts: Vec<&str> = full_name.splitn(2, '.').collect();
        if parts.len() != 2 {
            return Err(SdbError::InvalidArg);
        }
        let (cs_name, cl_name) = (parts[0], parts[1]);
        let mut meta = self.meta.write().map_err(|_| SdbError::Sys)?;
        let cs = meta
            .collection_spaces
            .iter_mut()
            .find(|cs| cs.name == cs_name)
            .ok_or(SdbError::CollectionSpaceNotFound)?;
        let before = cs.collections.len();
        cs.collections.retain(|cl| cl.name != cl_name);
        if cs.collections.len() == before {
            return Err(SdbError::CollectionNotFound);
        }
        drop(meta);
        self.mark_dirty();
        self.persist();
        Ok(vec![])
    }

    // ── Group management ────────────────────────────────────────────

    fn cmd_register_group(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;
        let group_id = match cond.get("GroupId") {
            Some(Value::Int32(n)) => *n as u32,
            _ => return Err(SdbError::InvalidArg),
        };
        let name = match cond.get("Name") {
            Some(Value::String(s)) => s.clone(),
            _ => format!("group{}", group_id),
        };
        let host = match cond.get("Host") {
            Some(Value::String(s)) => s.clone(),
            _ => "127.0.0.1".to_string(),
        };
        let port = match cond.get("Port") {
            Some(Value::Int32(n)) => *n as u16,
            _ => 11810,
        };
        let node_id = match cond.get("NodeId") {
            Some(Value::Int32(n)) => *n as u16,
            _ => 1,
        };

        let mut meta = self.meta.write().map_err(|_| SdbError::Sys)?;
        if let Some(g) = meta.groups.iter_mut().find(|g| g.group_id == group_id) {
            // Add node to existing group if not already present
            if !g.nodes.iter().any(|n| n.node_id == node_id) {
                g.nodes.push(NodeEntry {
                    node_id,
                    host,
                    port,
                });
            }
        } else {
            meta.groups.push(GroupEntry {
                group_id,
                name,
                nodes: vec![NodeEntry {
                    node_id,
                    host,
                    port,
                }],
                primary_node: None,
            });
        }
        drop(meta);
        self.mark_dirty();
        self.persist();
        Ok(vec![])
    }

    fn cmd_remove_group(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;
        let group_id = match cond.get("GroupId") {
            Some(Value::Int32(n)) => *n as u32,
            _ => return Err(SdbError::InvalidArg),
        };
        let mut meta = self.meta.write().map_err(|_| SdbError::Sys)?;
        meta.groups.retain(|g| g.group_id != group_id);
        drop(meta);
        self.mark_dirty();
        self.persist();
        Ok(vec![])
    }

    fn cmd_list_groups(&self) -> Result<Vec<Document>> {
        let meta = self.meta.read().map_err(|_| SdbError::Sys)?;
        let mut docs = Vec::new();
        for g in &meta.groups {
            let mut doc = Document::new();
            doc.insert("GroupId", Value::Int32(g.group_id as i32));
            doc.insert("Name", Value::String(g.name.clone()));
            doc.insert("NodeCount", Value::Int32(g.nodes.len() as i32));
            if let Some(pid) = g.primary_node {
                doc.insert("PrimaryNode", Value::Int32(pid as i32));
            }
            docs.push(doc);
        }
        Ok(docs)
    }

    // ── Shard config management ─────────────────────────────────────

    fn cmd_register_shard(&self, query: &MsgOpQuery) -> Result<Vec<Document>> {
        let cond = query.condition.as_ref().ok_or(SdbError::InvalidArg)?;
        let collection = match cond.get("Collection") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let shard_key = match cond.get("ShardKey") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(SdbError::InvalidArg),
        };
        let shard_type = match cond.get("ShardType") {
            Some(Value::String(s)) => s.clone(),
            _ => "hash".to_string(),
        };
        let num_groups = match cond.get("NumGroups") {
            Some(Value::Int32(n)) => *n as u32,
            _ => 1,
        };

        let mut meta = self.meta.write().map_err(|_| SdbError::Sys)?;
        // Replace existing shard config for this collection
        meta.shard_configs.retain(|s| s.collection != collection);
        meta.shard_configs.push(ShardEntry {
            collection,
            shard_key,
            shard_type,
            num_groups,
            ranges: Vec::new(),
        });
        drop(meta);
        self.mark_dirty();
        self.persist();
        Ok(vec![])
    }

    fn cmd_list_shards(&self) -> Result<Vec<Document>> {
        let meta = self.meta.read().map_err(|_| SdbError::Sys)?;
        let mut docs = Vec::new();
        for s in &meta.shard_configs {
            let mut doc = Document::new();
            doc.insert("Collection", Value::String(s.collection.clone()));
            doc.insert("ShardKey", Value::String(s.shard_key.clone()));
            doc.insert("ShardType", Value::String(s.shard_type.clone()));
            doc.insert("NumGroups", Value::Int32(s.num_groups as i32));
            docs.push(doc);
        }
        Ok(docs)
    }

    fn cmd_list_collections(&self) -> Result<Vec<Document>> {
        let meta = self.meta.read().map_err(|_| SdbError::Sys)?;
        let mut docs = Vec::new();
        for cs in &meta.collection_spaces {
            for cl in &cs.collections {
                let mut doc = Document::new();
                doc.insert("Name", Value::String(cl.full_name.clone()));
                doc.insert("CollectionSpace", Value::String(cs.name.clone()));
                docs.push(doc);
            }
        }
        Ok(docs)
    }

    fn cmd_snapshot(&self) -> Result<Vec<Document>> {
        let meta = self.meta.read().map_err(|_| SdbError::Sys)?;
        let mut doc = Document::new();
        doc.insert(
            "CollectionSpaces",
            Value::Int32(meta.collection_spaces.len() as i32),
        );
        let total_cls: usize = meta.collection_spaces.iter().map(|cs| cs.collections.len()).sum();
        doc.insert("Collections", Value::Int32(total_cls as i32));
        doc.insert("Groups", Value::Int32(meta.groups.len() as i32));
        doc.insert(
            "ShardConfigs",
            Value::Int32(meta.shard_configs.len() as i32),
        );
        Ok(vec![doc])
    }
}

#[async_trait]
impl MessageHandler for CatalogNodeHandler {
    async fn on_message(
        &self,
        conn: &mut Connection,
        header: MsgHeader,
        payload: &[u8],
    ) -> Result<()> {
        let opcode = OpCode::from_i32(header.opcode);
        tracing::debug!("Catalog received opcode {:?} from {}", opcode, conn.addr);

        let reply = match opcode {
            Some(OpCode::QueryReq) => self.handle_query(&header, payload),
            Some(OpCode::CatalogReq) => self.handle_catalog_req(&header, payload),
            Some(OpCode::Disconnect) => return Err(SdbError::NetworkClose),
            _ => MsgOpReply::error(header.opcode, header.request_id, &SdbError::InvalidArg),
        };

        conn.send_reply(&reply).await
    }

    async fn on_connect(&self, conn: &Connection) -> Result<()> {
        tracing::info!("Catalog client connected: {}", conn.addr);
        Ok(())
    }

    async fn on_disconnect(&self, conn: &Connection) -> Result<()> {
        tracing::info!("Catalog client disconnected: {}", conn.addr);
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

    fn tmp_path(name: &str) -> String {
        let p = std::env::temp_dir().join(format!("sdb_cat_handler_{}", name));
        let _ = std::fs::remove_dir_all(&p);
        let _ = std::fs::create_dir_all(&p);
        p.to_string_lossy().to_string()
    }

    #[test]
    fn create_and_persist_cs() {
        let path = tmp_path("persist_cs");
        {
            let handler = CatalogNodeHandler::new(&path);
            let mut meta = handler.meta.write().unwrap();
            meta.collection_spaces.push(CsEntry {
                name: "testcs".into(),
                collections: vec![ClEntry {
                    name: "cl1".into(),
                    full_name: "testcs.cl1".into(),
                }],
            });
            drop(meta);
            handler.persist();
        }
        // Reload
        let handler = CatalogNodeHandler::new(&path);
        let snap = handler.snapshot();
        assert_eq!(snap.collection_spaces.len(), 1);
        assert_eq!(snap.collection_spaces[0].name, "testcs");
        assert_eq!(snap.collection_spaces[0].collections.len(), 1);
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn register_group_persists() {
        let path = tmp_path("register_group");
        let handler = CatalogNodeHandler::new(&path);
        let mut meta = handler.meta.write().unwrap();
        meta.groups.push(GroupEntry {
            group_id: 1,
            name: "group1".into(),
            nodes: vec![NodeEntry {
                node_id: 1,
                host: "localhost".into(),
                port: 11810,
            }],
            primary_node: Some(1),
        });
        drop(meta);
        handler.persist();

        let snap = handler.snapshot();
        assert_eq!(snap.groups.len(), 1);
        assert_eq!(snap.groups[0].nodes.len(), 1);
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn shard_config_persists() {
        let path = tmp_path("shard_config");
        let handler = CatalogNodeHandler::new(&path);
        let mut meta = handler.meta.write().unwrap();
        meta.shard_configs.push(ShardEntry {
            collection: "cs.cl".into(),
            shard_key: "user_id".into(),
            shard_type: "hash".into(),
            num_groups: 3,
            ranges: Vec::new(),
        });
        drop(meta);
        handler.persist();

        // Reload and verify
        let handler2 = CatalogNodeHandler::new(&path);
        let snap = handler2.snapshot();
        assert_eq!(snap.shard_configs.len(), 1);
        assert_eq!(snap.shard_configs[0].shard_key, "user_id");
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn empty_db_path_creates_default() {
        let path = tmp_path("empty_default");
        let handler = CatalogNodeHandler::new(&path);
        let snap = handler.snapshot();
        assert!(snap.collection_spaces.is_empty());
        assert!(snap.groups.is_empty());
        assert!(snap.shard_configs.is_empty());
        let _ = std::fs::remove_dir_all(&path);
    }
}
