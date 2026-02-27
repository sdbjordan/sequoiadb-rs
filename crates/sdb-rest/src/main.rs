mod convert;
mod routes;
mod ui;

use std::sync::Arc;

use axum::routing::{delete, get, post};
use axum::Router;
use clap::Parser;
use sdb_client::{Client, ConnectOptions};
use tower_http::cors::CorsLayer;
use tracing::info;

#[derive(Clone)]
pub struct AppState {
    pub client: Arc<Client>,
}

#[derive(Parser)]
#[command(name = "sdb-rest", about = "REST API gateway for SequoiaDB-RS")]
struct Cli {
    /// SequoiaDB server host
    #[arg(long, default_value = "localhost")]
    sdb_host: String,

    /// SequoiaDB server port
    #[arg(long, default_value_t = 11810)]
    sdb_port: u16,

    /// REST API listen port
    #[arg(long, default_value_t = 8080)]
    port: u16,

    /// Optional auth username
    #[arg(long)]
    username: Option<String>,

    /// Optional auth password
    #[arg(long)]
    password: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    // Connect to SequoiaDB
    let opts = ConnectOptions {
        host: cli.sdb_host.clone(),
        port: cli.sdb_port,
        username: cli.username.clone(),
        password: cli.password.clone(),
        ..Default::default()
    };

    info!(
        "Connecting to SequoiaDB at {}:{}...",
        cli.sdb_host, cli.sdb_port
    );

    let client = Client::connect(opts)
        .await
        .expect("failed to connect to SequoiaDB");

    if let (Some(user), Some(pass)) = (&cli.username, &cli.password) {
        client
            .authenticate(user, pass)
            .await
            .expect("authentication failed");
    }

    let state = AppState {
        client: Arc::new(client),
    };

    let app = Router::new()
        // Web UI
        .route("/", get(ui::index))
        // Status
        .route("/api/status", get(routes::status::health))
        // Databases (collection spaces)
        .route("/api/databases", get(routes::databases::list))
        .route(
            "/api/databases/{cs}",
            post(routes::databases::create).delete(routes::databases::drop),
        )
        // Collections
        .route(
            "/api/databases/{cs}/collections",
            get(routes::collections::list),
        )
        .route(
            "/api/databases/{cs}/collections/{cl}",
            post(routes::collections::create).delete(routes::collections::drop),
        )
        // Documents
        .route(
            "/api/databases/{cs}/collections/{cl}/documents",
            get(routes::documents::query)
                .post(routes::documents::insert)
                .put(routes::documents::update)
                .delete(routes::documents::delete),
        )
        // Count
        .route(
            "/api/databases/{cs}/collections/{cl}/count",
            get(routes::documents::count),
        )
        // Indexes
        .route(
            "/api/databases/{cs}/collections/{cl}/indexes",
            post(routes::indexes::create),
        )
        .route(
            "/api/databases/{cs}/collections/{cl}/indexes/{idx}",
            delete(routes::indexes::drop),
        )
        // SQL
        .route("/api/sql", post(routes::sql::exec))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cli.port);
    info!("sdb-rest listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("failed to bind");

    axum::serve(listener, app).await.expect("server error");
}
