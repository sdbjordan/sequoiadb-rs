use axum::http::header;
use axum::response::IntoResponse;

const INDEX_HTML: &str = include_str!("../../static/index.html");

pub async fn index() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], INDEX_HTML)
}
