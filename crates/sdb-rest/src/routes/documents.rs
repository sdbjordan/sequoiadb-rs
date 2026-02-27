use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::convert::{doc_to_json, json_to_doc};
use crate::AppState;

#[derive(Deserialize)]
pub struct QueryParams {
    pub filter: Option<String>,
    pub skip: Option<u64>,
    pub limit: Option<u64>,
}

/// GET /api/databases/:cs/collections/:cl/documents — query documents
pub async fn query(
    State(state): State<AppState>,
    Path((cs, cl)): Path<(String, String)>,
    Query(params): Query<QueryParams>,
) -> Json<Value> {
    let condition = if let Some(ref filter_str) = params.filter {
        match serde_json::from_str::<Value>(filter_str) {
            Ok(v) => match json_to_doc(&v) {
                Ok(doc) => Some(doc),
                Err(e) => return Json(json!({ "ok": false, "error": e })),
            },
            Err(e) => return Json(json!({ "ok": false, "error": format!("invalid filter JSON: {e}") })),
        }
    } else {
        None
    };

    let skip = params.skip.unwrap_or(0) as usize;
    let limit = params.limit.unwrap_or(20).min(1000) as usize;

    let collection = state.client.get_collection(&cs, &cl);
    match collection.query(condition).await {
        Ok(mut cursor) => {
            match cursor.collect_all_async().await {
                Ok(all_docs) => {
                    let data: Vec<Value> = all_docs
                        .iter()
                        .skip(skip)
                        .take(limit)
                        .map(doc_to_json)
                        .collect();
                    Json(json!({ "ok": true, "data": data, "count": data.len() }))
                }
                Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
            }
        }
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

/// POST /api/databases/:cs/collections/:cl/documents — insert document
pub async fn insert(
    State(state): State<AppState>,
    Path((cs, cl)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let doc = match json_to_doc(&body) {
        Ok(d) => d,
        Err(e) => return Json(json!({ "ok": false, "error": e })),
    };

    let collection = state.client.get_collection(&cs, &cl);
    match collection.insert(doc).await {
        Ok(()) => Json(json!({ "ok": true, "data": "document inserted" })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

/// PUT /api/databases/:cs/collections/:cl/documents — update documents
pub async fn update(
    State(state): State<AppState>,
    Path((cs, cl)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let condition = match body.get("condition") {
        Some(v) => match json_to_doc(v) {
            Ok(d) => d,
            Err(e) => return Json(json!({ "ok": false, "error": format!("invalid condition: {e}") })),
        },
        None => return Json(json!({ "ok": false, "error": "missing 'condition' field" })),
    };

    let modifier = match body.get("modifier") {
        Some(v) => match json_to_doc(v) {
            Ok(d) => d,
            Err(e) => return Json(json!({ "ok": false, "error": format!("invalid modifier: {e}") })),
        },
        None => return Json(json!({ "ok": false, "error": "missing 'modifier' field" })),
    };

    let collection = state.client.get_collection(&cs, &cl);
    match collection.update(condition, modifier).await {
        Ok(()) => Json(json!({ "ok": true, "data": "documents updated" })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

/// DELETE /api/databases/:cs/collections/:cl/documents — delete documents
pub async fn delete(
    State(state): State<AppState>,
    Path((cs, cl)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let condition = match json_to_doc(&body) {
        Ok(d) => d,
        Err(e) => return Json(json!({ "ok": false, "error": e })),
    };

    let collection = state.client.get_collection(&cs, &cl);
    match collection.delete(condition).await {
        Ok(()) => Json(json!({ "ok": true, "data": "documents deleted" })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

/// GET /api/databases/:cs/collections/:cl/count — count documents
pub async fn count(
    State(state): State<AppState>,
    Path((cs, cl)): Path<(String, String)>,
    Query(params): Query<QueryParams>,
) -> Json<Value> {
    let condition = if let Some(ref filter_str) = params.filter {
        match serde_json::from_str::<Value>(filter_str) {
            Ok(v) => match json_to_doc(&v) {
                Ok(doc) => Some(doc),
                Err(e) => return Json(json!({ "ok": false, "error": e })),
            },
            Err(e) => return Json(json!({ "ok": false, "error": format!("invalid filter JSON: {e}") })),
        }
    } else {
        None
    };

    let collection = state.client.get_collection(&cs, &cl);
    match collection.count(condition).await {
        Ok(n) => Json(json!({ "ok": true, "data": { "count": n } })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}
