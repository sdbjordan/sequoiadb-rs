use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};

use crate::convert::doc_to_json;
use crate::AppState;

/// GET /api/databases — list collection spaces
pub async fn list(State(state): State<AppState>) -> Json<Value> {
    match state.client.exec_sql("list collectionspaces").await {
        Ok(docs) => {
            let data: Vec<Value> = docs.iter().map(doc_to_json).collect();
            Json(json!({ "ok": true, "data": data }))
        }
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

/// POST /api/databases/:cs — create collection space
pub async fn create(State(state): State<AppState>, Path(cs): Path<String>) -> Json<Value> {
    match state.client.create_collection_space(&cs).await {
        Ok(()) => Json(json!({ "ok": true, "data": format!("collection space '{}' created", cs) })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

/// DELETE /api/databases/:cs — drop collection space
pub async fn drop(State(state): State<AppState>, Path(cs): Path<String>) -> Json<Value> {
    match state.client.drop_collection_space(&cs).await {
        Ok(()) => Json(json!({ "ok": true, "data": format!("collection space '{}' dropped", cs) })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}
