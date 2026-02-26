use std::sync::{Arc, Mutex, RwLock};

use clap::Parser;
use sdb_common::config::{NodeConfig, NodeRole};
use sdb_common::NodeAddress;
use sdb_dps::WriteAheadLog;
use tracing_subscriber::EnvFilter;

mod catalog_handler;
mod coord_handler;
mod cursor_manager;
mod data_node_client;
mod data_store;
mod handler;

use catalog_handler::CatalogNodeHandler;
use coord_handler::CoordNodeHandler;
use data_store::DataStore;
use handler::DataNodeHandler;

#[derive(Parser, Debug)]
#[command(
    name = "sequoiadb",
    about = "SequoiaDB-RS distributed document database"
)]
struct Cli {
    /// Node role: coord, catalog, or data
    #[arg(short, long, default_value = "data")]
    role: String,

    /// Bind port
    #[arg(short, long, default_value_t = 11810)]
    port: u16,

    /// Database path
    #[arg(short, long, default_value = "/opt/sequoiadb/database")]
    db_path: String,

    /// Configuration file path
    #[arg(short, long)]
    config: Option<String>,

    /// Catalog address (host:port) for data and coord nodes
    #[arg(long)]
    catalog_addr: Option<String>,

    /// Data node addresses (host:port) for coordinator mode (repeatable)
    #[arg(long)]
    data_addr: Vec<String>,

    /// Replica group ID
    #[arg(long, default_value_t = 1)]
    group_id: u32,

    /// Node ID within the replica group
    #[arg(long, default_value_t = 1)]
    node_id: u16,

    /// Replica peer address (host:port), repeatable
    #[arg(long)]
    repl_peer: Vec<String>,
}

fn parse_role(s: &str) -> NodeRole {
    match s.to_lowercase().as_str() {
        "coord" => NodeRole::Coord,
        "catalog" | "cat" => NodeRole::Catalog,
        _ => NodeRole::Data,
    }
}

fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    // Load base config from file if --config is provided, otherwise use defaults
    let mut config = if let Some(ref config_path) = cli.config {
        match NodeConfig::load_from_file(config_path) {
            Ok(c) => {
                tracing::info!("Loaded config from {}", config_path);
                c
            }
            Err(e) => {
                tracing::error!("Failed to load config from {}: {}", config_path, e);
                std::process::exit(1);
            }
        }
    } else {
        NodeConfig::default()
    };

    // CLI args override file values
    config.role = parse_role(&cli.role);
    config.port = cli.port;
    config.db_path = cli.db_path;
    if cli.catalog_addr.is_some() {
        config.catalog_addr = cli.catalog_addr;
    }
    if !cli.data_addr.is_empty() {
        config.data_addrs = cli.data_addr;
    }
    config.group_id = cli.group_id;
    config.node_id = cli.node_id;
    if !cli.repl_peer.is_empty() {
        config.repl_peers = cli.repl_peer;
        config.repl_enabled = true;
    }

    tracing::info!(
        role = ?config.role,
        port = config.port,
        "Starting SequoiaDB-RS"
    );

    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    rt.block_on(async {
        match config.role {
            NodeRole::Coord => start_coord(&config).await,
            NodeRole::Catalog => start_catalog(&config).await,
            NodeRole::Data => start_data(&config).await,
        }
    });
}

async fn start_coord(config: &NodeConfig) {
    tracing::info!(port = config.port, "Starting coordinator node");

    let data_nodes: Vec<(u32, String)> = if config.data_addrs.is_empty() {
        tracing::warn!("No data node addresses configured, using default localhost:11810");
        vec![(1, "127.0.0.1:11810".to_string())]
    } else {
        config
            .data_addrs
            .iter()
            .enumerate()
            .map(|(i, addr)| ((i + 1) as u32, addr.clone()))
            .collect()
    };

    let config_path = format!("{}/coord_state.json", config.db_path);
    let coord = Arc::new(CoordNodeHandler::new_with_persistence(data_nodes, config_path));

    let mut frame = sdb_net::NetFrame::new(format!("{}:{}", config.host, config.port));
    let shutdown_tx = frame.shutdown_sender();
    frame.set_handler(coord);

    // Install SIGTERM/SIGINT handler
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Received shutdown signal (coord)");
        let _ = shutdown_tx.send(true);
    });

    tracing::info!("Coordinator node ready");
    if let Err(e) = frame.run().await {
        tracing::error!("Coordinator node error: {}", e);
    }
}

async fn start_catalog(config: &NodeConfig) {
    tracing::info!(port = config.port, "Starting catalog node");

    let handler = Arc::new(CatalogNodeHandler::new(&config.db_path));

    let mut frame = sdb_net::NetFrame::new(format!("{}:{}", config.host, config.port));
    let shutdown_tx = frame.shutdown_sender();
    frame.set_handler(handler);

    // Install SIGTERM/SIGINT handler
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Received shutdown signal (catalog)");
        let _ = shutdown_tx.send(true);
    });

    tracing::info!("Catalog node ready");
    if let Err(e) = frame.run().await {
        tracing::error!("Catalog node error: {}", e);
    }
}

async fn start_data(config: &NodeConfig) {
    tracing::info!(port = config.port, "Starting data node");

    // Open WAL
    let wal_path = format!("{}/wal", config.db_path);
    let mut wal = match WriteAheadLog::open(&wal_path) {
        Ok(w) => w,
        Err(e) => {
            tracing::error!("Failed to open WAL at {}: {}", wal_path, e);
            return;
        }
    };

    // Recover catalog from WAL
    let catalog = match handler::recover_from_wal(&mut wal) {
        Ok(cat) => {
            tracing::info!("Recovered catalog from WAL at {}", wal_path);
            Arc::new(RwLock::new(cat))
        }
        Err(e) => {
            tracing::warn!("WAL recovery failed ({}), trying DataStore fallback", e);
            // Fallback to DataStore if WAL recovery fails
            let data_store = DataStore::new(&config.db_path);
            match data_store.load() {
                Ok(cat) => {
                    tracing::info!("Loaded persisted catalog from {}", config.db_path);
                    Arc::new(RwLock::new(cat))
                }
                Err(e2) => {
                    tracing::warn!("Could not load catalog ({}), starting fresh", e2);
                    Arc::new(RwLock::new(sdb_cat::CatalogManager::new()))
                }
            }
        }
    };

    let wal = Arc::new(Mutex::new(wal));
    let wal_for_shutdown = wal.clone();

    let handler: Arc<DataNodeHandler> = if config.repl_enabled && !config.repl_peers.is_empty() {
        let local_node = NodeAddress {
            group_id: config.group_id,
            node_id: config.node_id,
        };

        // Parse peer addresses: each is "node_id@host:port" or just "host:port"
        let mut peers = Vec::new();
        for (i, peer_str) in config.repl_peers.iter().enumerate() {
            let (nid, addr) = if let Some(at_pos) = peer_str.find('@') {
                let nid: u16 = peer_str[..at_pos].parse().unwrap_or((i + 2) as u16);
                (nid, peer_str[at_pos + 1..].to_string())
            } else {
                ((i + 2) as u16, peer_str.clone())
            };
            let peer_node = NodeAddress {
                group_id: config.group_id,
                node_id: nid,
            };
            peers.push((peer_node, addr));
        }

        tracing::info!(
            group_id = config.group_id,
            node_id = config.node_id,
            peers = config.repl_peers.len(),
            "Starting data node with replication"
        );

        let handler = Arc::new(DataNodeHandler::new_with_replication(
            catalog.clone(),
            wal,
            local_node,
            peers,
            wal_path,
        ));

        // Spawn election loop
        let handler_clone = handler.clone();
        tokio::spawn(async move {
            handler_clone.run_election_loop().await;
        });

        handler
    } else {
        Arc::new(DataNodeHandler::new_with_wal(catalog.clone(), wal))
    };

    let mut frame = sdb_net::NetFrame::new(format!("{}:{}", config.host, config.port));
    let shutdown_tx = frame.shutdown_sender();
    frame.set_handler(handler);

    // Install SIGTERM/SIGINT handler — flush WAL before exit
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Received shutdown signal (data node)");
        // Flush WAL before shutdown
        if let Ok(mut w) = wal_for_shutdown.lock() {
            if let Err(e) = w.flush() {
                tracing::error!("WAL flush on shutdown failed: {}", e);
            } else {
                tracing::info!("WAL flushed successfully on shutdown");
            }
        }
        let _ = shutdown_tx.send(true);
    });

    if let Err(e) = frame.run().await {
        tracing::error!("Data node error: {}", e);
    }
}
