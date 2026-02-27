use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};

use crate::convert::doc_to_json;
use crate::AppState;

/// GET /api/databases/:cs/collections — list collections in a collection space
pub async fn list(State(state): State<AppState>, Path(cs): Path<String>) -> Json<Value> {
    let sql = format!("list collections in {cs}");
    match state.client.exec_sql(&sql).await {
        Ok(docs) => {
            let data: Vec<Value> = docs.iter().map(doc_to_json).collect();
            Json(json!({ "ok": true, "data": data }))
        }
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

/// POST /api/databases/:cs/collections/:cl — create collection
pub async fn create(
    State(state): State<AppState>,
    Path((cs, cl)): Path<(String, String)>,
) -> Json<Value> {
    match state.client.create_collection(&cs, &cl).await {
        Ok(()) => Json(json!({ "ok": true, "data": format!("collection '{cs}.{cl}' created") })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

/// DELETE /api/databases/:cs/collections/:cl — drop collection
pub async fn drop(
    State(state): State<AppState>,
    Path((cs, cl)): Path<(String, String)>,
) -> Json<Value> {
    match state.client.drop_collection(&cs, &cl).await {
        Ok(()) => Json(json!({ "ok": true, "data": format!("collection '{cs}.{cl}' dropped") })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}
