use serde::{Deserialize, Serialize};

/// Node role in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    Coord,
    Catalog,
    Data,
}

/// Configuration for a SequoiaDB node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub role: NodeRole,
    pub host: String,
    pub port: u16,
    pub db_path: String,
    pub log_path: String,
    pub catalog_addr: Option<String>,
    pub repl_port: Option<u16>,
    pub shard_port: Option<u16>,
    pub max_pool_size: u32,
    pub page_size: u32,
    pub log_file_size: u64,
    pub log_file_num: u32,
    pub transaction_on: bool,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            role: NodeRole::Data,
            host: "0.0.0.0".to_string(),
            port: 11810,
            db_path: "/opt/sequoiadb/database".to_string(),
            log_path: "/opt/sequoiadb/log".to_string(),
            catalog_addr: None,
            repl_port: Some(11800),
            shard_port: Some(11820),
            max_pool_size: 100,
            page_size: 65536,
            log_file_size: 64 * 1024 * 1024,
            log_file_num: 20,
            transaction_on: true,
        }
    }
}
