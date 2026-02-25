use std::sync::{Arc, RwLock};

use sdb_bson::{Document, Value};
use sdb_cat::CatalogManager;
use sdb_msg::header::MsgHeader;
use sdb_msg::reply::MsgOpReply;
use sdb_msg::request::*;
use sdb_net::MessageHandler;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// We import the handler from the server crate's lib
use sdb_server::handler::DataNodeHandler;

fn doc(pairs: &[(&str, Value)]) -> Document {
    let mut d = Document::new();
    for (k, v) in pairs {
        d.insert(*k, v.clone());
    }
    d
}

/// Start a test server on an ephemeral port, returning the port.
async fn start_test_server() -> u16 {
    let catalog = Arc::new(RwLock::new(CatalogManager::new()));
    let handler: Arc<dyn MessageHandler> = Arc::new(DataNodeHandler::new(catalog));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let handler = handler.clone();
            tokio::spawn(async move {
                let mut conn = sdb_net::Connection::new(stream, addr);
                let _ = handler.on_connect(&conn).await;
                while let Ok((header, payload)) = conn.recv_msg().await {
                    if handler.on_message(&mut conn, header, &payload).await.is_err() {
                        break;
                    }
                }
                let _ = handler.on_disconnect(&conn).await;
            });
        }
    });

    port
}

/// Send a raw message and receive a reply.
async fn send_recv(stream: &mut TcpStream, msg_bytes: &[u8]) -> MsgOpReply {
    stream.write_all(msg_bytes).await.unwrap();
    stream.flush().await.unwrap();

    // Read reply: first 4 bytes = msg_len
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.unwrap();
    let msg_len = i32::from_le_bytes(len_buf) as usize;

    let mut full_buf = Vec::with_capacity(msg_len);
    full_buf.extend_from_slice(&len_buf);
    full_buf.resize(msg_len, 0);
    stream.read_exact(&mut full_buf[4..]).await.unwrap();

    let header = MsgHeader::decode(&full_buf).unwrap();
    let payload = &full_buf[MsgHeader::SIZE..];
    MsgOpReply::decode(&header, payload).unwrap()
}

fn create_cs_msg(request_id: u64, name: &str) -> Vec<u8> {
    MsgOpQuery::new(
        request_id,
        "$create collectionspace",
        Some(doc(&[("Name", Value::String(name.into()))])),
        None,
        None,
        None,
        0,
        -1,
        0,
    )
    .encode()
}

fn drop_cs_msg(request_id: u64, name: &str) -> Vec<u8> {
    MsgOpQuery::new(
        request_id,
        "$drop collectionspace",
        Some(doc(&[("Name", Value::String(name.into()))])),
        None,
        None,
        None,
        0,
        -1,
        0,
    )
    .encode()
}

fn create_cl_msg(request_id: u64, full_name: &str) -> Vec<u8> {
    MsgOpQuery::new(
        request_id,
        "$create collection",
        Some(doc(&[("Name", Value::String(full_name.into()))])),
        None,
        None,
        None,
        0,
        -1,
        0,
    )
    .encode()
}

fn drop_cl_msg(request_id: u64, full_name: &str) -> Vec<u8> {
    MsgOpQuery::new(
        request_id,
        "$drop collection",
        Some(doc(&[("Name", Value::String(full_name.into()))])),
        None,
        None,
        None,
        0,
        -1,
        0,
    )
    .encode()
}

fn insert_msg(request_id: u64, collection: &str, docs: Vec<Document>) -> Vec<u8> {
    MsgOpInsert::new(request_id, collection, docs, 0).encode()
}

fn query_msg(
    request_id: u64,
    collection: &str,
    condition: Option<Document>,
) -> Vec<u8> {
    MsgOpQuery::new(request_id, collection, condition, None, None, None, 0, -1, 0).encode()
}

fn update_msg(
    request_id: u64,
    collection: &str,
    condition: Document,
    modifier: Document,
) -> Vec<u8> {
    MsgOpUpdate::new(request_id, collection, condition, modifier, None, 0).encode()
}

fn delete_msg(request_id: u64, collection: &str, condition: Document) -> Vec<u8> {
    MsgOpDelete::new(request_id, collection, condition, None, 0).encode()
}

fn create_index_msg(
    request_id: u64,
    collection: &str,
    index_name: &str,
    key_doc: Document,
    unique: bool,
) -> Vec<u8> {
    let mut index_doc = Document::new();
    index_doc.insert("name", Value::String(index_name.into()));
    index_doc.insert("key", Value::Document(key_doc));
    index_doc.insert("unique", Value::Boolean(unique));

    let mut cond = Document::new();
    cond.insert("Collection", Value::String(collection.into()));
    cond.insert("Index", Value::Document(index_doc));

    MsgOpQuery::new(request_id, "$create index", Some(cond), None, None, None, 0, -1, 0).encode()
}

fn drop_index_msg(request_id: u64, collection: &str, index_name: &str) -> Vec<u8> {
    let mut cond = Document::new();
    cond.insert("Collection", Value::String(collection.into()));
    cond.insert("Index", Value::String(index_name.into()));

    MsgOpQuery::new(request_id, "$drop index", Some(cond), None, None, None, 0, -1, 0).encode()
}

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn tcp_connect_disconnect() {
    let port = start_test_server().await;
    let stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await;
    assert!(stream.is_ok());
    drop(stream);
}

#[tokio::test]
async fn create_cs_cl_insert_query() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Create CS
    let reply = send_recv(&mut stream, &create_cs_msg(1, "testcs")).await;
    assert_eq!(reply.flags, 0, "create cs should succeed");

    // Create CL
    let reply = send_recv(&mut stream, &create_cl_msg(2, "testcs.testcl")).await;
    assert_eq!(reply.flags, 0, "create cl should succeed");

    // Insert
    let docs = vec![
        doc(&[("x", Value::Int32(1)), ("name", Value::String("alice".into()))]),
        doc(&[("x", Value::Int32(2)), ("name", Value::String("bob".into()))]),
    ];
    let reply = send_recv(&mut stream, &insert_msg(3, "testcs.testcl", docs)).await;
    assert_eq!(reply.flags, 0, "insert should succeed");

    // Query all
    let reply = send_recv(&mut stream, &query_msg(4, "testcs.testcl", None)).await;
    assert_eq!(reply.flags, 0, "query should succeed");
    assert_eq!(reply.docs.len(), 2);
}

#[tokio::test]
async fn insert_multiple_then_query_with_condition() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "cs2")).await;
    send_recv(&mut stream, &create_cl_msg(2, "cs2.cl")).await;

    // Insert 5 docs
    let docs: Vec<Document> = (0..5)
        .map(|i| doc(&[("val", Value::Int32(i))]))
        .collect();
    send_recv(&mut stream, &insert_msg(3, "cs2.cl", docs)).await;

    // Query with condition: val >= 3
    let cond = doc(&[("val", Value::Document(doc(&[("$gte", Value::Int32(3))])))]);
    let reply = send_recv(&mut stream, &query_msg(4, "cs2.cl", Some(cond))).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs.len(), 2); // val=3, val=4
}

#[tokio::test]
async fn update_then_query() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "cs3")).await;
    send_recv(&mut stream, &create_cl_msg(2, "cs3.cl")).await;
    send_recv(
        &mut stream,
        &insert_msg(
            3,
            "cs3.cl",
            vec![doc(&[("x", Value::Int32(1)), ("y", Value::Int32(10))])],
        ),
    )
    .await;

    // Update: { x: 1 } → { $set: { y: 99 } }
    let modifier = doc(&[("$set", Value::Document(doc(&[("y", Value::Int32(99))])))]);
    let reply = send_recv(
        &mut stream,
        &update_msg(4, "cs3.cl", doc(&[("x", Value::Int32(1))]), modifier),
    )
    .await;
    assert_eq!(reply.flags, 0, "update should succeed");

    // Query to verify
    let reply = send_recv(&mut stream, &query_msg(5, "cs3.cl", None)).await;
    assert_eq!(reply.docs.len(), 1);
    assert_eq!(reply.docs[0].get("y"), Some(&Value::Int32(99)));
}

#[tokio::test]
async fn delete_then_query() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "cs4")).await;
    send_recv(&mut stream, &create_cl_msg(2, "cs4.cl")).await;

    let docs = vec![
        doc(&[("x", Value::Int32(1))]),
        doc(&[("x", Value::Int32(2))]),
        doc(&[("x", Value::Int32(3))]),
    ];
    send_recv(&mut stream, &insert_msg(3, "cs4.cl", docs)).await;

    // Delete where x == 2
    let reply = send_recv(
        &mut stream,
        &delete_msg(4, "cs4.cl", doc(&[("x", Value::Int32(2))])),
    )
    .await;
    assert_eq!(reply.flags, 0, "delete should succeed");

    // Query to verify
    let reply = send_recv(&mut stream, &query_msg(5, "cs4.cl", None)).await;
    assert_eq!(reply.docs.len(), 2);
    let vals: Vec<i32> = reply
        .docs
        .iter()
        .filter_map(|d| match d.get("x") {
            Some(Value::Int32(v)) => Some(*v),
            _ => None,
        })
        .collect();
    assert!(vals.contains(&1));
    assert!(vals.contains(&3));
    assert!(!vals.contains(&2));
}

#[tokio::test]
async fn create_index_then_query() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "cs5")).await;
    send_recv(&mut stream, &create_cl_msg(2, "cs5.cl")).await;

    let docs: Vec<Document> = (0..10)
        .map(|i| doc(&[("val", Value::Int32(i))]))
        .collect();
    send_recv(&mut stream, &insert_msg(3, "cs5.cl", docs)).await;

    // Create index
    let key = doc(&[("val", Value::Int32(1))]);
    let reply = send_recv(
        &mut stream,
        &create_index_msg(4, "cs5.cl", "idx_val", key, false),
    )
    .await;
    assert_eq!(reply.flags, 0, "create index should succeed");

    // Query — optimizer should pick index scan
    let cond = doc(&[("val", Value::Int32(5))]);
    let reply = send_recv(&mut stream, &query_msg(5, "cs5.cl", Some(cond))).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs.len(), 1);
    assert_eq!(reply.docs[0].get("val"), Some(&Value::Int32(5)));
}

#[tokio::test]
async fn drop_cl_then_drop_cs() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "cs6")).await;
    send_recv(&mut stream, &create_cl_msg(2, "cs6.cl")).await;
    send_recv(
        &mut stream,
        &insert_msg(3, "cs6.cl", vec![doc(&[("a", Value::Int32(1))])]),
    )
    .await;

    // Drop CL
    let reply = send_recv(&mut stream, &drop_cl_msg(4, "cs6.cl")).await;
    assert_eq!(reply.flags, 0, "drop cl should succeed");

    // Query should fail (collection not found)
    let reply = send_recv(&mut stream, &query_msg(5, "cs6.cl", None)).await;
    assert!(reply.flags < 0, "query on dropped cl should fail");

    // Drop CS
    let reply = send_recv(&mut stream, &drop_cs_msg(6, "cs6")).await;
    assert_eq!(reply.flags, 0, "drop cs should succeed");
}

#[tokio::test]
async fn error_nonexistent_cs_cl() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Query on nonexistent collection
    let reply = send_recv(&mut stream, &query_msg(1, "nope.nope", None)).await;
    assert!(reply.flags < 0, "should error on nonexistent cs.cl");

    // Insert on nonexistent collection
    let reply = send_recv(
        &mut stream,
        &insert_msg(2, "nope.nope", vec![doc(&[("x", Value::Int32(1))])]),
    )
    .await;
    assert!(reply.flags < 0, "should error on nonexistent cs.cl");

    // Create CL without CS
    let reply = send_recv(&mut stream, &create_cl_msg(3, "nope.cl")).await;
    assert!(reply.flags < 0, "should error when cs doesn't exist");
}

#[tokio::test]
async fn drop_index_command() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "cs7")).await;
    send_recv(&mut stream, &create_cl_msg(2, "cs7.cl")).await;

    let key = doc(&[("x", Value::Int32(1))]);
    send_recv(
        &mut stream,
        &create_index_msg(3, "cs7.cl", "idx_x", key, false),
    )
    .await;

    // Drop index
    let reply = send_recv(
        &mut stream,
        &drop_index_msg(4, "cs7.cl", "idx_x"),
    )
    .await;
    assert_eq!(reply.flags, 0, "drop index should succeed");

    // Drop nonexistent index should fail
    let reply = send_recv(
        &mut stream,
        &drop_index_msg(5, "cs7.cl", "idx_x"),
    )
    .await;
    assert!(reply.flags < 0, "drop nonexistent index should fail");
}
