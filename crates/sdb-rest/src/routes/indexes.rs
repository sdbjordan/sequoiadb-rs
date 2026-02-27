use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};

use crate::convert::json_to_doc;
use crate::AppState;

/// POST /api/databases/:cs/collections/:cl/indexes — create index
///
/// Body: { "name": "idx_name", "key": {"field": 1}, "unique": false }
pub async fn create(
    State(state): State<AppState>,
    Path((cs, cl)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let idx_name = match body.get("name").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => return Json(json!({ "ok": false, "error": "missing 'name' field" })),
    };

    let key_json = match body.get("key") {
        Some(v) => v,
        None => return Json(json!({ "ok": false, "error": "missing 'key' field" })),
    };

    let key_doc = match json_to_doc(key_json) {
        Ok(d) => d,
        Err(e) => return Json(json!({ "ok": false, "error": format!("invalid key: {e}") })),
    };

    let unique = body.get("unique").and_then(|v| v.as_bool()).unwrap_or(false);

    match state.client.create_index(&cs, &cl, &idx_name, key_doc, unique).await {
        Ok(()) => Json(json!({ "ok": true, "data": format!("index '{idx_name}' created on {cs}.{cl}") })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

/// DELETE /api/databases/:cs/collections/:cl/indexes/:idx — drop index
pub async fn drop(
    State(state): State<AppState>,
    Path((cs, cl, idx)): Path<(String, String, String)>,
) -> Json<Value> {
    match state.client.drop_index(&cs, &cl, &idx).await {
        Ok(()) => Json(json!({ "ok": true, "data": format!("index '{idx}' dropped from {cs}.{cl}") })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}
