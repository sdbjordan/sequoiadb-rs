use axum::extract::State;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::convert::doc_to_json;
use crate::AppState;

#[derive(Deserialize)]
pub struct SqlRequest {
    pub sql: String,
}

/// POST /api/sql — execute SQL statement
pub async fn exec(State(state): State<AppState>, Json(body): Json<SqlRequest>) -> Json<Value> {
    match state.client.exec_sql(&body.sql).await {
        Ok(docs) => {
            let data: Vec<Value> = docs.iter().map(doc_to_json).collect();
            Json(json!({ "ok": true, "data": data }))
        }
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}
