use serde::{Deserialize, Serialize};

use crate::{Result, SdbError};

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
    pub data_addrs: Vec<String>,
    pub tls_enabled: bool,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
    pub tls_ca_path: Option<String>,
    pub monitoring_enabled: bool,
    #[serde(default = "default_group_id")]
    pub group_id: u32,
    #[serde(default = "default_node_id")]
    pub node_id: u16,
    #[serde(default)]
    pub repl_peers: Vec<String>,
    #[serde(default)]
    pub repl_enabled: bool,
}

fn default_group_id() -> u32 { 1 }
fn default_node_id() -> u16 { 1 }

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
            data_addrs: vec![],
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
            tls_ca_path: None,
            monitoring_enabled: true,
            group_id: 1,
            node_id: 1,
            repl_peers: vec![],
            repl_enabled: false,
        }
    }
}

impl NodeConfig {
    /// Load a NodeConfig from a TOML file at the given path.
    pub fn load_from_file(path: &str) -> Result<NodeConfig> {
        let content = std::fs::read_to_string(path).map_err(|_| SdbError::IoError)?;
        toml::from_str(&content).map_err(|_| SdbError::InvalidConfig)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_correct_tls_defaults() {
        let config = NodeConfig::default();
        assert!(!config.tls_enabled);
        assert!(config.tls_cert_path.is_none());
        assert!(config.tls_key_path.is_none());
        assert!(config.tls_ca_path.is_none());
        assert!(config.monitoring_enabled);
    }

    #[test]
    fn load_from_file_nonexistent_returns_error() {
        let result = NodeConfig::load_from_file("/tmp/nonexistent_sdb_config.toml");
        assert!(result.is_err());
    }

    #[test]
    fn load_from_toml_string() {
        let toml_str = r#"
role = "Data"
host = "127.0.0.1"
port = 12345
db_path = "/data/sdb"
log_path = "/data/sdb/log"
max_pool_size = 50
page_size = 65536
log_file_size = 67108864
log_file_num = 10
transaction_on = true
data_addrs = []
tls_enabled = true
tls_cert_path = "/path/to/cert.pem"
tls_key_path = "/path/to/key.pem"
monitoring_enabled = false
"#;
        let config: NodeConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.port, 12345);
        assert!(config.tls_enabled);
        assert_eq!(config.tls_cert_path, Some("/path/to/cert.pem".to_string()));
        assert!(!config.monitoring_enabled);
    }
}
