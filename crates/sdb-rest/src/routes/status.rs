use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use crate::AppState;

pub async fn health(State(state): State<AppState>) -> Json<Value> {
    let connected = state.client.is_connected();
    let host = &state.client.options().host;
    let port = state.client.options().port;
    Json(json!({
        "ok": true,
        "data": {
            "connected": connected,
            "sdb_host": host,
            "sdb_port": port,
        }
    }))
}
