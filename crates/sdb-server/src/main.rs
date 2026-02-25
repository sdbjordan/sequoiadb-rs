use std::sync::{Arc, RwLock};

use clap::Parser;
use sdb_common::config::{NodeConfig, NodeRole};
use tracing_subscriber::EnvFilter;

mod handler;

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

    let config = NodeConfig {
        role: parse_role(&cli.role),
        port: cli.port,
        db_path: cli.db_path,
        catalog_addr: cli.catalog_addr,
        ..NodeConfig::default()
    };

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
    // Stub: initialize coordinator components
    let _router = sdb_coord::CoordRouter::new();
    tracing::info!("Coordinator node ready");
}

async fn start_catalog(config: &NodeConfig) {
    tracing::info!(port = config.port, "Starting catalog node");
    // Stub: initialize catalog components
    let _catalog = sdb_cat::CatalogManager::new();
    tracing::info!("Catalog node ready");
}

async fn start_data(config: &NodeConfig) {
    tracing::info!(port = config.port, "Starting data node");

    let catalog = Arc::new(RwLock::new(sdb_cat::CatalogManager::new()));
    let handler = Arc::new(DataNodeHandler::new(catalog));

    let mut frame = sdb_net::NetFrame::new(format!("{}:{}", config.host, config.port));
    frame.set_handler(handler);

    if let Err(e) = frame.run().await {
        tracing::error!("Data node error: {}", e);
    }
}
