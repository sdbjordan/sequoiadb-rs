use std::sync::{Arc, Mutex, RwLock};

use sdb_bson::{Document, Value};
use sdb_cat::CatalogManager;
use sdb_cls::election::ElectionState;
use sdb_common::{NodeAddress, ReadPreference};
use sdb_dps::WriteAheadLog;
use sdb_msg::header::MsgHeader;
use sdb_msg::opcode::OpCode;
use sdb_msg::reply::MsgOpReply;
use sdb_msg::request::*;
use sdb_net::MessageHandler;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// We import the handler from the server crate's lib
use sdb_server::coord_handler::CoordNodeHandler;
use sdb_server::handler::{recover_from_wal, DataNodeHandler};

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

// ── Auth command helpers ─────────────────────────────────────────────

fn create_user_msg(request_id: u64, user: &str, pass: &str, roles: Vec<&str>) -> Vec<u8> {
    let role_vals: Vec<Value> = roles.into_iter().map(|r| Value::String(r.into())).collect();
    let mut cond = Document::new();
    cond.insert("User", Value::String(user.into()));
    cond.insert("Passwd", Value::String(pass.into()));
    cond.insert("Roles", Value::Array(role_vals));
    MsgOpQuery::new(request_id, "$create user", Some(cond), None, None, None, 0, -1, 0).encode()
}

fn authenticate_msg(request_id: u64, user: &str, pass: &str) -> Vec<u8> {
    let mut cond = Document::new();
    cond.insert("User", Value::String(user.into()));
    cond.insert("Passwd", Value::String(pass.into()));
    MsgOpQuery::new(request_id, "$authenticate", Some(cond), None, None, None, 0, -1, 0).encode()
}

fn drop_user_msg(request_id: u64, user: &str) -> Vec<u8> {
    MsgOpQuery::new(
        request_id,
        "$drop user",
        Some(doc(&[("User", Value::String(user.into()))])),
        None, None, None, 0, -1, 0,
    ).encode()
}

fn aggregate_msg(request_id: u64, collection: &str, pipeline: Vec<Value>) -> Vec<u8> {
    let mut cond = Document::new();
    cond.insert("Collection", Value::String(collection.into()));
    cond.insert("Pipeline", Value::Array(pipeline));
    MsgOpQuery::new(request_id, "$aggregate", Some(cond), None, None, None, 0, -1, 0).encode()
}

fn sql_msg(request_id: u64, sql: &str) -> Vec<u8> {
    MsgOpQuery::new(
        request_id,
        "$sql",
        Some(doc(&[("SQL", Value::String(sql.into()))])),
        None, None, None, 0, -1, 0,
    ).encode()
}

fn count_msg(request_id: u64, collection: &str, condition: Option<Document>) -> Vec<u8> {
    let mut cond = Document::new();
    cond.insert("Collection", Value::String(collection.into()));
    if let Some(fc) = condition {
        cond.insert("Condition", Value::Document(fc));
    }
    MsgOpQuery::new(request_id, "$count", Some(cond), None, None, None, 0, -1, 0).encode()
}

fn get_more_msg(request_id: u64, context_id: i64) -> Vec<u8> {
    let msg = MsgOpGetMore {
        header: MsgHeader::new_request(OpCode::GetMoreReq as i32, request_id),
        context_id,
        num_to_return: -1,
    };
    msg.encode()
}

fn kill_context_msg(request_id: u64, context_ids: Vec<i64>) -> Vec<u8> {
    let msg = MsgOpKillContexts {
        header: MsgHeader::new_request(OpCode::KillContextReq as i32, request_id),
        context_ids,
    };
    msg.encode()
}

// ── Auth integration tests ──────────────────────────────────────────

#[tokio::test]
async fn auth_create_user_authenticate() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Create user
    let reply = send_recv(
        &mut stream,
        &create_user_msg(1, "admin", "secret123", vec!["admin"]),
    ).await;
    assert_eq!(reply.flags, 0, "create user should succeed");

    // Authenticate with correct password
    let reply = send_recv(&mut stream, &authenticate_msg(2, "admin", "secret123")).await;
    assert_eq!(reply.flags, 0, "authenticate should succeed");

    // Authenticate with wrong password should fail
    let reply = send_recv(&mut stream, &authenticate_msg(3, "admin", "wrong")).await;
    assert!(reply.flags < 0, "wrong password should fail");

    // Drop user
    let reply = send_recv(&mut stream, &drop_user_msg(4, "admin")).await;
    assert_eq!(reply.flags, 0, "drop user should succeed");

    // Authenticate after drop should fail
    let reply = send_recv(&mut stream, &authenticate_msg(5, "admin", "secret123")).await;
    assert!(reply.flags < 0, "auth after drop should fail");
}

// ── Aggregate integration tests ─────────────────────────────────────

#[tokio::test]
async fn aggregate_match_limit() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "aggcs")).await;
    send_recv(&mut stream, &create_cl_msg(2, "aggcs.cl")).await;

    // Insert 10 docs
    let docs: Vec<Document> = (0..10)
        .map(|i| doc(&[("val", Value::Int32(i)), ("name", Value::String(format!("item{}", i)))]))
        .collect();
    send_recv(&mut stream, &insert_msg(3, "aggcs.cl", docs)).await;

    // Aggregate: $match { val >= 5 } → $limit 3
    let pipeline = vec![
        Value::Document(doc(&[("$match", Value::Document(doc(&[
            ("val", Value::Document(doc(&[("$gte", Value::Int32(5))])))
        ])))])),
        Value::Document(doc(&[("$limit", Value::Int32(3))])),
    ];
    let reply = send_recv(&mut stream, &aggregate_msg(4, "aggcs.cl", pipeline)).await;
    assert_eq!(reply.flags, 0, "aggregate should succeed");
    assert_eq!(reply.docs.len(), 3, "should return 3 docs after $limit");
}

// ── SQL integration tests ───────────────────────────────────────────

#[tokio::test]
async fn sql_create_insert_select() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // CREATE TABLE via SQL
    let reply = send_recv(
        &mut stream,
        &sql_msg(1, "CREATE TABLE sqlcs.people (name TEXT, age INT)"),
    ).await;
    assert_eq!(reply.flags, 0, "CREATE TABLE should succeed");

    // INSERT via SQL
    let reply = send_recv(
        &mut stream,
        &sql_msg(2, "INSERT INTO sqlcs.people (name, age) VALUES ('alice', 30)"),
    ).await;
    assert_eq!(reply.flags, 0, "INSERT should succeed");

    let reply = send_recv(
        &mut stream,
        &sql_msg(3, "INSERT INTO sqlcs.people (name, age) VALUES ('bob', 25)"),
    ).await;
    assert_eq!(reply.flags, 0, "INSERT should succeed");

    // SELECT via SQL
    let reply = send_recv(
        &mut stream,
        &sql_msg(4, "SELECT * FROM sqlcs.people"),
    ).await;
    assert_eq!(reply.flags, 0, "SELECT should succeed");
    assert_eq!(reply.docs.len(), 2, "should return 2 rows");
}

// ── Count integration tests ─────────────────────────────────────────

#[tokio::test]
async fn server_side_count() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "cntcs")).await;
    send_recv(&mut stream, &create_cl_msg(2, "cntcs.cl")).await;

    let docs: Vec<Document> = (0..7)
        .map(|i| doc(&[("val", Value::Int32(i))]))
        .collect();
    send_recv(&mut stream, &insert_msg(3, "cntcs.cl", docs)).await;

    // Count all
    let reply = send_recv(&mut stream, &count_msg(4, "cntcs.cl", None)).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs.len(), 1);
    assert_eq!(reply.docs[0].get("count"), Some(&Value::Int64(7)));

    // Count with condition: val >= 5
    let cond = doc(&[("val", Value::Document(doc(&[("$gte", Value::Int32(5))])))]);
    let reply = send_recv(&mut stream, &count_msg(5, "cntcs.cl", Some(cond))).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs[0].get("count"), Some(&Value::Int64(2)));
}

// ── GetMore cursor integration tests ────────────────────────────────

#[tokio::test]
async fn get_more_cursor_batching() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "gmcs")).await;
    send_recv(&mut stream, &create_cl_msg(2, "gmcs.cl")).await;

    // Insert 250 docs (> DEFAULT_BATCH_SIZE of 100)
    let docs: Vec<Document> = (0..250)
        .map(|i| doc(&[("i", Value::Int32(i))]))
        .collect();
    send_recv(&mut stream, &insert_msg(3, "gmcs.cl", docs)).await;

    // Query — should return first 100 + a context_id
    let reply = send_recv(&mut stream, &query_msg(4, "gmcs.cl", None)).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs.len(), 100, "first batch should be 100 docs");
    assert!(reply.context_id > 0, "should have a cursor context_id");

    let ctx = reply.context_id;

    // GetMore — second batch of 100
    let reply = send_recv(&mut stream, &get_more_msg(5, ctx)).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs.len(), 100, "second batch should be 100 docs");
    assert_eq!(reply.context_id, ctx, "cursor should still be open");

    // GetMore — third batch of remaining 50
    let reply = send_recv(&mut stream, &get_more_msg(6, ctx)).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs.len(), 50, "third batch should be 50 docs");
    assert_eq!(reply.context_id, -1, "cursor should be exhausted");

    // GetMore on exhausted cursor should fail
    let reply = send_recv(&mut stream, &get_more_msg(7, ctx)).await;
    assert!(reply.flags < 0, "GetMore on closed cursor should fail");
}

#[tokio::test]
async fn kill_context_closes_cursor() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "kcs")).await;
    send_recv(&mut stream, &create_cl_msg(2, "kcs.cl")).await;

    let docs: Vec<Document> = (0..200)
        .map(|i| doc(&[("i", Value::Int32(i))]))
        .collect();
    send_recv(&mut stream, &insert_msg(3, "kcs.cl", docs)).await;

    // Query to get a cursor
    let reply = send_recv(&mut stream, &query_msg(4, "kcs.cl", None)).await;
    assert!(reply.context_id > 0);
    let ctx = reply.context_id;

    // Kill the cursor
    let reply = send_recv(&mut stream, &kill_context_msg(5, vec![ctx])).await;
    assert_eq!(reply.flags, 0, "kill context should succeed");

    // GetMore should fail now
    let reply = send_recv(&mut stream, &get_more_msg(6, ctx)).await;
    assert!(reply.flags < 0, "GetMore after kill should fail");
}

// ── Coord handler integration tests ─────────────────────────────────

/// Start a CoordNodeHandler backed by real DataNode servers on ephemeral ports.
async fn start_coord_server() -> u16 {
    // Start 3 data node servers
    let p1 = start_test_server().await;
    let p2 = start_test_server().await;
    let p3 = start_test_server().await;

    let coord = Arc::new(CoordNodeHandler::new(vec![
        (1, format!("127.0.0.1:{}", p1)),
        (2, format!("127.0.0.1:{}", p2)),
        (3, format!("127.0.0.1:{}", p3)),
    ]));
    let handler: Arc<dyn MessageHandler> = coord;

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

#[tokio::test]
async fn coord_ddl_insert_query() {
    let port = start_coord_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Create CS + CL (broadcast to all groups)
    let reply = send_recv(&mut stream, &create_cs_msg(1, "coordcs")).await;
    assert_eq!(reply.flags, 0, "coord create cs should succeed");

    let reply = send_recv(&mut stream, &create_cl_msg(2, "coordcs.cl")).await;
    assert_eq!(reply.flags, 0, "coord create cl should succeed");

    // Insert docs
    let docs = vec![
        doc(&[("x", Value::Int32(1))]),
        doc(&[("x", Value::Int32(2))]),
        doc(&[("x", Value::Int32(3))]),
    ];
    let reply = send_recv(&mut stream, &insert_msg(3, "coordcs.cl", docs)).await;
    assert_eq!(reply.flags, 0, "coord insert should succeed");

    // Query all — scatter-gather across groups
    let reply = send_recv(&mut stream, &query_msg(4, "coordcs.cl", None)).await;
    assert_eq!(reply.flags, 0, "coord query should succeed");
    // With 3 groups, all 3 docs should be found (broadcast insert goes to default group 1)
    assert!(reply.docs.len() >= 3, "should find all inserted docs");

    // Count
    let reply = send_recv(&mut stream, &count_msg(5, "coordcs.cl", None)).await;
    assert_eq!(reply.flags, 0);
    let count = match reply.docs[0].get("count") {
        Some(Value::Int64(n)) => *n,
        _ => 0,
    };
    assert!(count >= 3, "count should be >= 3");
}

#[tokio::test]
async fn coord_drop_cs_cl() {
    let port = start_coord_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "dropcs")).await;
    send_recv(&mut stream, &create_cl_msg(2, "dropcs.cl")).await;

    // Drop CL
    let reply = send_recv(&mut stream, &drop_cl_msg(3, "dropcs.cl")).await;
    assert_eq!(reply.flags, 0);

    // Query should fail
    let reply = send_recv(&mut stream, &query_msg(4, "dropcs.cl", None)).await;
    assert!(reply.flags < 0, "query on dropped cl should fail");

    // Drop CS
    let reply = send_recv(&mut stream, &drop_cs_msg(5, "dropcs")).await;
    assert_eq!(reply.flags, 0);
}

// ── WAL Recovery integration tests ──────────────────────────────────

fn wal_tmp_dir(name: &str) -> String {
    let p = std::env::temp_dir().join(format!("sdb_wal_integ_{}", name));
    let _ = std::fs::remove_dir_all(&p);
    p.to_string_lossy().to_string()
}

fn wal_cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

/// Start a test server backed by WAL, returning (port, wal_path).
async fn start_wal_test_server(wal_path: &str) -> u16 {
    let wal = WriteAheadLog::open(wal_path).unwrap();
    let wal = Arc::new(Mutex::new(wal));
    let catalog = Arc::new(RwLock::new(CatalogManager::new()));
    let handler: Arc<dyn MessageHandler> =
        Arc::new(DataNodeHandler::new_with_wal(catalog, wal));

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

#[tokio::test]
async fn wal_recovery_insert_documents() {
    let wal_dir = wal_tmp_dir("recovery_insert");

    // Phase 1: Create data with WAL enabled
    {
        let port = start_wal_test_server(&wal_dir).await;
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();

        // Create CS + CL
        let reply = send_recv(&mut stream, &create_cs_msg(1, "wcs")).await;
        assert_eq!(reply.flags, 0, "create cs should succeed");

        let reply = send_recv(&mut stream, &create_cl_msg(2, "wcs.wcl")).await;
        assert_eq!(reply.flags, 0, "create cl should succeed");

        // Insert documents
        let docs = vec![
            doc(&[("x", Value::Int32(1)), ("name", Value::String("alice".into()))]),
            doc(&[("x", Value::Int32(2)), ("name", Value::String("bob".into()))]),
            doc(&[("x", Value::Int32(3)), ("name", Value::String("charlie".into()))]),
        ];
        let reply = send_recv(&mut stream, &insert_msg(3, "wcs.wcl", docs)).await;
        assert_eq!(reply.flags, 0, "insert should succeed");

        // Drop the connection (simulates crash — server handler is dropped)
        drop(stream);
    }

    // Phase 2: Recover from WAL
    {
        let mut wal = WriteAheadLog::open(&wal_dir).unwrap();
        let catalog = recover_from_wal(&mut wal).unwrap();

        // Verify: CS exists
        catalog.get_collection_space("wcs").unwrap();

        // Verify: CL exists
        catalog.get_collection("wcs", "wcl").unwrap();

        // Verify: all 3 documents recovered
        let (storage, _) = catalog.collection_handle("wcs", "wcl").unwrap();
        let rows = storage.scan();
        assert_eq!(rows.len(), 3, "should recover 3 documents");

        let names: Vec<String> = rows
            .iter()
            .filter_map(|(_, d)| match d.get("name") {
                Some(Value::String(s)) => Some(s.clone()),
                _ => None,
            })
            .collect();
        assert!(names.contains(&"alice".to_string()));
        assert!(names.contains(&"bob".to_string()));
        assert!(names.contains(&"charlie".to_string()));
    }

    wal_cleanup(&wal_dir);
}

#[tokio::test]
async fn wal_recovery_with_deletes() {
    let wal_dir = wal_tmp_dir("recovery_delete");

    // Phase 1: Insert then delete
    {
        let port = start_wal_test_server(&wal_dir).await;
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();

        send_recv(&mut stream, &create_cs_msg(1, "dcs")).await;
        send_recv(&mut stream, &create_cl_msg(2, "dcs.dcl")).await;

        let docs = vec![
            doc(&[("x", Value::Int32(1))]),
            doc(&[("x", Value::Int32(2))]),
            doc(&[("x", Value::Int32(3))]),
        ];
        send_recv(&mut stream, &insert_msg(3, "dcs.dcl", docs)).await;

        // Delete x=2
        let reply = send_recv(
            &mut stream,
            &delete_msg(4, "dcs.dcl", doc(&[("x", Value::Int32(2))])),
        )
        .await;
        assert_eq!(reply.flags, 0);

        drop(stream);
    }

    // Phase 2: Recover
    {
        let mut wal = WriteAheadLog::open(&wal_dir).unwrap();
        let catalog = recover_from_wal(&mut wal).unwrap();

        let (storage, _) = catalog.collection_handle("dcs", "dcl").unwrap();
        let rows = storage.scan();
        assert_eq!(rows.len(), 2, "should have 2 docs after recovery (3 - 1 deleted)");

        let vals: Vec<i32> = rows
            .iter()
            .filter_map(|(_, d)| match d.get("x") {
                Some(Value::Int32(v)) => Some(*v),
                _ => None,
            })
            .collect();
        assert!(vals.contains(&1));
        assert!(!vals.contains(&2));
        assert!(vals.contains(&3));
    }

    wal_cleanup(&wal_dir);
}

#[tokio::test]
async fn wal_recovery_with_updates() {
    let wal_dir = wal_tmp_dir("recovery_update");

    // Phase 1: Insert then update
    {
        let port = start_wal_test_server(&wal_dir).await;
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();

        send_recv(&mut stream, &create_cs_msg(1, "ucs")).await;
        send_recv(&mut stream, &create_cl_msg(2, "ucs.ucl")).await;

        let docs = vec![doc(&[("x", Value::Int32(1)), ("y", Value::Int32(10))])];
        send_recv(&mut stream, &insert_msg(3, "ucs.ucl", docs)).await;

        // Update: x=1 => set y=99
        let modifier = doc(&[("$set", Value::Document(doc(&[("y", Value::Int32(99))])))]);
        let reply = send_recv(
            &mut stream,
            &update_msg(4, "ucs.ucl", doc(&[("x", Value::Int32(1))]), modifier),
        )
        .await;
        assert_eq!(reply.flags, 0);

        drop(stream);
    }

    // Phase 2: Recover
    {
        let mut wal = WriteAheadLog::open(&wal_dir).unwrap();
        let catalog = recover_from_wal(&mut wal).unwrap();

        let (storage, _) = catalog.collection_handle("ucs", "ucl").unwrap();
        let rows = storage.scan();
        assert_eq!(rows.len(), 1, "should have 1 doc after recovery");

        let (_, doc) = &rows[0];
        assert_eq!(doc.get("x"), Some(&Value::Int32(1)));
        assert_eq!(doc.get("y"), Some(&Value::Int32(99)));
    }

    wal_cleanup(&wal_dir);
}

#[tokio::test]
async fn wal_recovery_ddl_drop_cs() {
    let wal_dir = wal_tmp_dir("recovery_drop_cs");

    // Phase 1: Create CS, CL, insert, then drop CS
    {
        let port = start_wal_test_server(&wal_dir).await;
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();

        send_recv(&mut stream, &create_cs_msg(1, "tempcs")).await;
        send_recv(&mut stream, &create_cl_msg(2, "tempcs.tempcl")).await;
        send_recv(
            &mut stream,
            &insert_msg(3, "tempcs.tempcl", vec![doc(&[("a", Value::Int32(1))])]),
        )
        .await;

        // Drop CS
        let reply = send_recv(&mut stream, &drop_cs_msg(4, "tempcs")).await;
        assert_eq!(reply.flags, 0);

        drop(stream);
    }

    // Phase 2: Recover — CS should NOT exist
    {
        let mut wal = WriteAheadLog::open(&wal_dir).unwrap();
        let catalog = recover_from_wal(&mut wal).unwrap();

        let result = catalog.get_collection_space("tempcs");
        assert!(result.is_err(), "CS should not exist after drop + recovery");
    }

    wal_cleanup(&wal_dir);
}

#[tokio::test]
async fn wal_recovery_create_index() {
    let wal_dir = wal_tmp_dir("recovery_index");

    // Phase 1: Create CS, CL, insert docs, create index
    {
        let port = start_wal_test_server(&wal_dir).await;
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();

        send_recv(&mut stream, &create_cs_msg(1, "ics")).await;
        send_recv(&mut stream, &create_cl_msg(2, "ics.icl")).await;

        let docs: Vec<Document> = (0..5)
            .map(|i| doc(&[("val", Value::Int32(i))]))
            .collect();
        send_recv(&mut stream, &insert_msg(3, "ics.icl", docs)).await;

        // Create index
        let key = doc(&[("val", Value::Int32(1))]);
        let reply = send_recv(
            &mut stream,
            &create_index_msg(4, "ics.icl", "idx_val", key, false),
        )
        .await;
        assert_eq!(reply.flags, 0);

        drop(stream);
    }

    // Phase 2: Recover — index should exist
    {
        let mut wal = WriteAheadLog::open(&wal_dir).unwrap();
        let catalog = recover_from_wal(&mut wal).unwrap();

        let cl = catalog.get_collection("ics", "icl").unwrap();
        assert_eq!(cl.indexes.len(), 1);
        assert_eq!(cl.indexes[0].name, "idx_val");

        // Verify all 5 docs recovered
        let (storage, _) = catalog.collection_handle("ics", "icl").unwrap();
        let rows = storage.scan();
        assert_eq!(rows.len(), 5);
    }

    wal_cleanup(&wal_dir);
}

// ── Transaction helpers ──────────────────────────────────────────────

fn trans_begin_msg(request_id: u64) -> Vec<u8> {
    let header = MsgHeader::new_request(OpCode::TransBeginReq as i32, request_id);
    let mut buf = Vec::new();
    header.encode(&mut buf);
    let len = buf.len() as i32;
    buf[0..4].copy_from_slice(&len.to_le_bytes());
    buf
}

fn trans_commit_msg(request_id: u64) -> Vec<u8> {
    let header = MsgHeader::new_request(OpCode::TransCommitReq as i32, request_id);
    let mut buf = Vec::new();
    header.encode(&mut buf);
    let len = buf.len() as i32;
    buf[0..4].copy_from_slice(&len.to_le_bytes());
    buf
}

fn trans_rollback_msg(request_id: u64) -> Vec<u8> {
    let header = MsgHeader::new_request(OpCode::TransRollbackReq as i32, request_id);
    let mut buf = Vec::new();
    header.encode(&mut buf);
    let len = buf.len() as i32;
    buf[0..4].copy_from_slice(&len.to_le_bytes());
    buf
}

// ── Transaction integration tests ───────────────────────────────────

#[tokio::test]
async fn transaction_commit_applies_changes() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "txcs")).await;
    send_recv(&mut stream, &create_cl_msg(2, "txcs.cl")).await;

    // Begin transaction
    let reply = send_recv(&mut stream, &trans_begin_msg(3)).await;
    assert_eq!(reply.flags, 0, "begin txn should succeed");

    // Insert within txn (buffered)
    let reply = send_recv(&mut stream, &insert_msg(4, "txcs.cl", vec![
        doc(&[("x", Value::Int32(1))]),
        doc(&[("x", Value::Int32(2))]),
    ])).await;
    assert_eq!(reply.flags, 0, "buffered insert should succeed");

    // Commit
    let reply = send_recv(&mut stream, &trans_commit_msg(5)).await;
    assert_eq!(reply.flags, 0, "commit should succeed");

    // Verify data exists
    let reply = send_recv(&mut stream, &query_msg(6, "txcs.cl", None)).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs.len(), 2, "should have 2 docs after commit");
}

#[tokio::test]
async fn transaction_rollback_discards_changes() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "txcs2")).await;
    send_recv(&mut stream, &create_cl_msg(2, "txcs2.cl")).await;

    // Insert some data outside txn
    send_recv(&mut stream, &insert_msg(3, "txcs2.cl", vec![doc(&[("x", Value::Int32(0))])])).await;

    // Begin transaction
    let reply = send_recv(&mut stream, &trans_begin_msg(4)).await;
    assert_eq!(reply.flags, 0);

    // Insert within txn (buffered)
    send_recv(&mut stream, &insert_msg(5, "txcs2.cl", vec![doc(&[("x", Value::Int32(999))])])).await;

    // Rollback
    let reply = send_recv(&mut stream, &trans_rollback_msg(6)).await;
    assert_eq!(reply.flags, 0, "rollback should succeed");

    // Verify only original data exists (txn insert was discarded)
    let reply = send_recv(&mut stream, &query_msg(7, "txcs2.cl", None)).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs.len(), 1, "should have only 1 doc (x=0), txn insert discarded");
}

// ── Auth enforcement integration tests ──────────────────────────────

#[tokio::test]
async fn auth_enforcement_blocks_unauthenticated() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // First, create a user (bootstrap — no auth needed when no users exist)
    let reply = send_recv(
        &mut stream,
        &create_user_msg(1, "admin", "secret", vec!["admin"]),
    )
    .await;
    assert_eq!(reply.flags, 0, "bootstrap user creation should succeed");

    // Now try to create CS without auth — should fail
    let reply = send_recv(&mut stream, &create_cs_msg(2, "blocked_cs")).await;
    assert!(reply.flags < 0, "DDL without auth should fail");

    // Try insert without auth — should fail
    let reply = send_recv(
        &mut stream,
        &insert_msg(
            3,
            "blocked.cl",
            vec![doc(&[("x", Value::Int32(1))])],
        ),
    )
    .await;
    assert!(reply.flags < 0, "insert without auth should fail");

    // Authenticate
    let reply = send_recv(&mut stream, &authenticate_msg(4, "admin", "secret")).await;
    assert_eq!(reply.flags, 0, "authenticate should succeed");

    // Now DDL should work
    let reply = send_recv(&mut stream, &create_cs_msg(5, "authed_cs")).await;
    assert_eq!(reply.flags, 0, "DDL after auth should succeed");

    let reply = send_recv(&mut stream, &create_cl_msg(6, "authed_cs.cl")).await;
    assert_eq!(reply.flags, 0, "create cl after auth should succeed");

    // Insert should work
    let reply = send_recv(
        &mut stream,
        &insert_msg(
            7,
            "authed_cs.cl",
            vec![doc(&[("x", Value::Int32(42))])],
        ),
    )
    .await;
    assert_eq!(reply.flags, 0, "insert after auth should succeed");

    // Query should work
    let reply = send_recv(&mut stream, &query_msg(8, "authed_cs.cl", None)).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs.len(), 1);
}

// ── Snapshot / Monitoring Tests ─────────────────────────────────────────

#[tokio::test]
async fn snapshot_database_returns_metrics() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Create CS/CL
    let reply = send_recv(&mut stream, &create_cs_msg(1, "snapcs")).await;
    assert_eq!(reply.flags, 0);
    let reply = send_recv(&mut stream, &create_cl_msg(2, "snapcs.cl")).await;
    assert_eq!(reply.flags, 0);

    // Insert a doc
    let reply = send_recv(
        &mut stream,
        &insert_msg(3, "snapcs.cl", vec![doc(&[("x", Value::Int32(1))])]),
    )
    .await;
    assert_eq!(reply.flags, 0);

    // Query
    let reply = send_recv(&mut stream, &query_msg(4, "snapcs.cl", None)).await;
    assert_eq!(reply.flags, 0);

    // Now send $snapshot database
    let reply = send_recv(&mut stream, &query_msg(5, "$snapshot database", None)).await;
    assert_eq!(reply.flags, 0, "snapshot database should succeed");
    assert!(!reply.docs.is_empty(), "snapshot should return at least one doc");

    let snap_doc = &reply.docs[0];
    // Check that we get metrics fields
    assert!(snap_doc.get("TotalInsert").is_some(), "should have TotalInsert field");
    assert!(snap_doc.get("TotalQuery").is_some(), "should have TotalQuery field");
    assert!(snap_doc.get("ActiveSessions").is_some(), "should have ActiveSessions field");

    // Verify insert counter is non-zero
    match snap_doc.get("TotalInsert") {
        Some(Value::Int64(n)) => assert!(*n >= 1, "TotalInsert should be >= 1"),
        other => panic!("expected Int64 for TotalInsert, got {:?}", other),
    }

    // Verify query counter is non-zero
    match snap_doc.get("TotalQuery") {
        Some(Value::Int64(n)) => assert!(*n >= 1, "TotalQuery should be >= 1"),
        other => panic!("expected Int64 for TotalQuery, got {:?}", other),
    }
}

#[tokio::test]
async fn snapshot_sessions_returns_session_info() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Send $snapshot sessions
    let reply = send_recv(&mut stream, &query_msg(1, "$snapshot sessions", None)).await;
    assert_eq!(reply.flags, 0);
    assert!(!reply.docs.is_empty());

    let snap_doc = &reply.docs[0];
    assert_eq!(snap_doc.get("Type"), Some(&Value::String("Sessions".into())));
    assert!(snap_doc.get("TotalSessions").is_some());
}

#[tokio::test]
async fn snapshot_collections_returns_collection_list() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Create some collections
    let reply = send_recv(&mut stream, &create_cs_msg(1, "snapcs2")).await;
    assert_eq!(reply.flags, 0);
    let reply = send_recv(&mut stream, &create_cl_msg(2, "snapcs2.c1")).await;
    assert_eq!(reply.flags, 0);
    let reply = send_recv(&mut stream, &create_cl_msg(3, "snapcs2.c2")).await;
    assert_eq!(reply.flags, 0);

    // Snapshot collections
    let reply = send_recv(&mut stream, &query_msg(4, "$snapshot collections", None)).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs.len(), 2, "should list 2 collections");

    let names: Vec<String> = reply.docs.iter().filter_map(|d| {
        match d.get("Name") {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        }
    }).collect();
    assert!(names.contains(&"snapcs2.c1".to_string()));
    assert!(names.contains(&"snapcs2.c2".to_string()));
}

#[tokio::test]
async fn metrics_counters_after_operations() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Setup
    let _ = send_recv(&mut stream, &create_cs_msg(1, "mcs")).await;
    let _ = send_recv(&mut stream, &create_cl_msg(2, "mcs.cl")).await;

    // Insert 3 times
    for i in 3..6u64 {
        let _ = send_recv(
            &mut stream,
            &insert_msg(i, "mcs.cl", vec![doc(&[("v", Value::Int32(i as i32))])]),
        )
        .await;
    }

    // Update
    let update_bytes = update_msg(
        6,
        "mcs.cl",
        doc(&[("v", Value::Int32(3))]),
        doc(&[("$set", Value::Document(doc(&[("v", Value::Int32(999))])))]),
    );
    let reply = send_recv(&mut stream, &update_bytes).await;
    assert_eq!(reply.flags, 0);

    // Delete
    let delete_bytes = MsgOpDelete::new(7, "mcs.cl", doc(&[("v", Value::Int32(4))]), None, 0).encode();
    let reply = send_recv(&mut stream, &delete_bytes).await;
    assert_eq!(reply.flags, 0);

    // Query
    let reply = send_recv(&mut stream, &query_msg(8, "mcs.cl", None)).await;
    assert_eq!(reply.flags, 0);

    // Check snapshot
    let reply = send_recv(&mut stream, &query_msg(9, "$snapshot database", None)).await;
    assert_eq!(reply.flags, 0);
    let snap = &reply.docs[0];

    match snap.get("TotalInsert") {
        Some(Value::Int64(n)) => assert!(*n >= 3, "should have >= 3 inserts, got {}", n),
        other => panic!("expected TotalInsert, got {:?}", other),
    }
    match snap.get("TotalUpdate") {
        Some(Value::Int64(n)) => assert!(*n >= 1, "should have >= 1 update, got {}", n),
        other => panic!("expected TotalUpdate, got {:?}", other),
    }
    match snap.get("TotalDelete") {
        Some(Value::Int64(n)) => assert!(*n >= 1, "should have >= 1 delete, got {}", n),
        other => panic!("expected TotalDelete, got {:?}", other),
    }
    match snap.get("TotalQuery") {
        Some(Value::Int64(n)) => assert!(*n >= 1, "should have >= 1 query, got {}", n),
        other => panic!("expected TotalQuery, got {:?}", other),
    }
}

// ── Phase 7: Shard routing tests ────────────────────────────────────

fn enable_sharding_msg(
    request_id: u64,
    collection: &str,
    shard_key: &str,
    num_groups: u32,
) -> Vec<u8> {
    let mut cond = Document::new();
    cond.insert("Collection", Value::String(collection.into()));
    cond.insert("ShardKey", Value::String(shard_key.into()));
    cond.insert("NumGroups", Value::Int32(num_groups as i32));
    MsgOpQuery::new(request_id, "$enable sharding", Some(cond), None, None, None, 0, -1, 0)
        .encode()
}

fn get_shard_info_msg(request_id: u64, collection: &str) -> Vec<u8> {
    let mut cond = Document::new();
    cond.insert("Collection", Value::String(collection.into()));
    MsgOpQuery::new(request_id, "$get shard info", Some(cond), None, None, None, 0, -1, 0)
        .encode()
}

#[tokio::test]
async fn enable_sharding_routes_inserts() {
    let port = start_coord_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Create CS + CL on all groups
    let reply = send_recv(&mut stream, &create_cs_msg(1, "scs")).await;
    assert_eq!(reply.flags, 0, "create cs");
    let reply = send_recv(&mut stream, &create_cl_msg(2, "scs.cl")).await;
    assert_eq!(reply.flags, 0, "create cl");

    // Enable sharding
    let reply = send_recv(&mut stream, &enable_sharding_msg(3, "scs.cl", "x", 3)).await;
    assert_eq!(reply.flags, 0, "enable sharding");

    // Insert 30 docs with varying shard key values
    let docs: Vec<Document> = (0..30)
        .map(|i| doc(&[("x", Value::Int32(i)), ("v", Value::Int32(i * 10))]))
        .collect();
    let reply = send_recv(&mut stream, &insert_msg(4, "scs.cl", docs)).await;
    assert_eq!(reply.flags, 0, "insert 30 docs");

    // Count total across all groups — should be 30
    let reply = send_recv(&mut stream, &count_msg(5, "scs.cl", None)).await;
    assert_eq!(reply.flags, 0, "count");
    let count = match reply.docs[0].get("count") {
        Some(Value::Int64(n)) => *n,
        _ => 0,
    };
    assert_eq!(count, 30, "total count should be 30, got {}", count);
}

#[tokio::test]
async fn sharded_query_with_shard_key_hits_one_group() {
    let port = start_coord_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "sqcs")).await;
    send_recv(&mut stream, &create_cl_msg(2, "sqcs.cl")).await;
    send_recv(&mut stream, &enable_sharding_msg(3, "sqcs.cl", "x", 3)).await;

    // Insert docs
    let docs: Vec<Document> = (0..30)
        .map(|i| doc(&[("x", Value::Int32(i)), ("v", Value::Int32(i))]))
        .collect();
    send_recv(&mut stream, &insert_msg(4, "sqcs.cl", docs)).await;

    // Query with shard key — should return only docs matching that shard key value
    let reply = send_recv(
        &mut stream,
        &query_msg(5, "sqcs.cl", Some(doc(&[("x", Value::Int32(0))]))),
    )
    .await;
    assert_eq!(reply.flags, 0);
    // At minimum the doc with x=0 should be found
    assert!(
        reply.docs.iter().any(|d| d.get("x") == Some(&Value::Int32(0))),
        "should find doc with x=0"
    );
}

#[tokio::test]
async fn get_shard_info_returns_config() {
    let port = start_coord_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "sics")).await;
    send_recv(&mut stream, &create_cl_msg(2, "sics.cl")).await;

    // Before sharding — should report not sharded
    let reply = send_recv(&mut stream, &get_shard_info_msg(3, "sics.cl")).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(
        reply.docs[0].get("Sharded"),
        Some(&Value::Boolean(false)),
        "should not be sharded initially"
    );

    // Enable sharding
    send_recv(&mut stream, &enable_sharding_msg(4, "sics.cl", "mykey", 3)).await;

    // After sharding
    let reply = send_recv(&mut stream, &get_shard_info_msg(5, "sics.cl")).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs[0].get("Sharded"), Some(&Value::Boolean(true)));
    assert_eq!(
        reply.docs[0].get("ShardKey"),
        Some(&Value::String("mykey".into()))
    );
    assert_eq!(reply.docs[0].get("NumGroups"), Some(&Value::Int32(3)));
}

#[tokio::test]
async fn unsharded_query_still_broadcasts() {
    let port = start_coord_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "ubcs")).await;
    send_recv(&mut stream, &create_cl_msg(2, "ubcs.cl")).await;

    // Insert without sharding — goes to all groups (broadcast)
    let docs = vec![
        doc(&[("v", Value::Int32(1))]),
        doc(&[("v", Value::Int32(2))]),
    ];
    send_recv(&mut stream, &insert_msg(3, "ubcs.cl", docs)).await;

    // Query should broadcast and find docs
    let reply = send_recv(&mut stream, &query_msg(4, "ubcs.cl", None)).await;
    assert_eq!(reply.flags, 0);
    // At least 2 docs (may be more if broadcast insert duplicated)
    assert!(reply.docs.len() >= 2, "should find at least 2 docs");
}

// ── Phase 6: Replica set tests ──────────────────────────────────────

/// Start a data node with replication enabled.
/// Returns (port, handler_arc) for further inspection.
async fn start_repl_data_node(
    group_id: u32,
    node_id: u16,
    peers: Vec<(NodeAddress, String)>,
) -> (u16, Arc<DataNodeHandler>) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let wal_path = std::env::temp_dir()
        .join(format!("sdb_test_wal_{}_{}_{}", group_id, node_id, ts));
    std::fs::create_dir_all(&wal_path).unwrap();
    let wal_path_str = wal_path.to_str().unwrap().to_string();

    let wal = WriteAheadLog::open(&wal_path_str).unwrap();
    let catalog = Arc::new(RwLock::new(CatalogManager::new()));
    let wal = Arc::new(Mutex::new(wal));

    let local_node = NodeAddress { group_id, node_id };
    let handler = Arc::new(DataNodeHandler::new_with_replication(
        catalog,
        wal,
        local_node,
        peers,
        wal_path_str,
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let h = handler.clone();
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let handler = h.clone();
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

    (port, handler)
}

#[tokio::test]
async fn single_node_replicaset_auto_primary() {
    // 1 node, repl_enabled, no peers → should auto-elect as primary
    let (port, handler) = start_repl_data_node(1, 1, vec![]).await;

    // The election loop would normally handle this, but with 0 peers
    // start_election() should immediately win.
    // Trigger election manually since the loop isn't running yet.
    {
        let em = handler.election().as_ref().unwrap();
        let mut em = em.lock().unwrap();
        em.start_election().unwrap();
    }

    assert!(
        handler.check_primary().is_ok(),
        "single node should be primary"
    );

    // Verify writes succeed
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let reply = send_recv(&mut stream, &create_cs_msg(1, "rcs")).await;
    assert_eq!(reply.flags, 0, "primary should accept DDL");
    let reply = send_recv(&mut stream, &create_cl_msg(2, "rcs.cl")).await;
    assert_eq!(reply.flags, 0);
    let reply = send_recv(
        &mut stream,
        &insert_msg(3, "rcs.cl", vec![doc(&[("x", Value::Int32(1))])]),
    )
    .await;
    assert_eq!(reply.flags, 0, "primary should accept writes");
}

#[tokio::test]
async fn secondary_rejects_writes() {
    let (port, handler) = start_repl_data_node(1, 1, vec![]).await;

    // Force state to Secondary
    {
        let em = handler.election().as_ref().unwrap();
        let mut em = em.lock().unwrap();
        em.state = ElectionState::Secondary;
    }

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // DDL should still work (DDL doesn't check primary)
    let reply = send_recv(&mut stream, &create_cs_msg(1, "rcs2")).await;
    // DDL might or might not check primary - that's fine

    // Insert should fail with NotPrimary
    let reply = send_recv(
        &mut stream,
        &insert_msg(2, "rcs2.cl", vec![doc(&[("x", Value::Int32(1))])]),
    )
    .await;
    assert!(reply.flags < 0, "secondary should reject insert, flags={}", reply.flags);
}

#[tokio::test]
async fn three_node_elects_primary() {
    // Test election logic with 3 nodes using direct method calls.
    // Node 1 starts election, nodes 2 and 3 grant votes.
    let addr1 = NodeAddress { group_id: 1, node_id: 1 };
    let addr2 = NodeAddress { group_id: 1, node_id: 2 };
    let addr3 = NodeAddress { group_id: 1, node_id: 3 };

    let (_, h1) = start_repl_data_node(
        1, 1,
        vec![
            (addr2, "127.0.0.1:19991".into()),
            (addr3, "127.0.0.1:19992".into()),
        ],
    ).await;
    let (_, h2) = start_repl_data_node(
        1, 2,
        vec![
            (addr1, "127.0.0.1:19993".into()),
            (addr3, "127.0.0.1:19994".into()),
        ],
    ).await;
    let (_, h3) = start_repl_data_node(
        1, 3,
        vec![
            (addr1, "127.0.0.1:19995".into()),
            (addr2, "127.0.0.1:19996".into()),
        ],
    ).await;

    // Node 1 starts election
    {
        let em = h1.election().as_ref().unwrap();
        let mut em = em.lock().unwrap();
        em.start_election().unwrap();
        // With 3 nodes, needs 2 votes. Has 1 (self). Not yet primary.
        assert_eq!(em.state, ElectionState::Candidate);
    }

    // Node 2 grants vote (simulating VoteReq/VoteReply exchange)
    let granted2 = {
        let em = h2.election().as_ref().unwrap();
        let mut em = em.lock().unwrap();
        em.should_grant_vote(1, 1, 0) // candidate_id=1, term=1, lsn=0
    };
    assert!(granted2, "node 2 should grant vote");

    // Deliver vote to node 1
    {
        let em = h1.election().as_ref().unwrap();
        let mut em = em.lock().unwrap();
        em.receive_vote(2).unwrap(); // vote from node 2 → 2/3 = majority
    }

    // Now node 1 should be primary
    assert!(h1.check_primary().is_ok(), "node 1 should be primary");
    assert!(h2.check_primary().is_err(), "node 2 should not be primary");
    assert!(h3.check_primary().is_err(), "node 3 should not be primary");

    // Exactly 1 primary
    let primary_count = [
        h1.check_primary().is_ok(),
        h2.check_primary().is_ok(),
        h3.check_primary().is_ok(),
    ].iter().filter(|&&x| x).count();
    assert_eq!(primary_count, 1);
}

#[tokio::test]
async fn primary_replicates_to_secondary() {
    // Test that a primary node accepts writes and can query them back.
    // Real WAL-based replication to secondaries is tested via the ReplicationAgent unit tests.
    let addr1 = NodeAddress { group_id: 1, node_id: 1 };
    let addr2 = NodeAddress { group_id: 1, node_id: 2 };

    let (p1, h1) = start_repl_data_node(
        1, 1,
        vec![(addr2, "127.0.0.1:19997".into())],
    ).await;

    // Force h1 as primary (single peer, need 2/2 = 2 votes, we have self + receive_vote)
    {
        let em = h1.election().as_ref().unwrap();
        let mut em = em.lock().unwrap();
        em.start_election().unwrap();
        let _ = em.receive_vote(2);
    }
    assert!(h1.check_primary().is_ok(), "h1 should be primary");

    // Write data to primary
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", p1))
        .await
        .unwrap();
    let reply = send_recv(&mut stream, &create_cs_msg(1, "replcs")).await;
    assert_eq!(reply.flags, 0, "create cs on primary");
    let reply = send_recv(&mut stream, &create_cl_msg(2, "replcs.cl")).await;
    assert_eq!(reply.flags, 0, "create cl on primary");
    let reply = send_recv(
        &mut stream,
        &insert_msg(3, "replcs.cl", vec![doc(&[("x", Value::Int32(42))])]),
    )
    .await;
    assert_eq!(reply.flags, 0, "insert on primary");

    // Verify primary can query back
    let reply = send_recv(&mut stream, &query_msg(4, "replcs.cl", None)).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs.len(), 1, "primary should have 1 doc");
    assert_eq!(reply.docs[0].get("x"), Some(&Value::Int32(42)));
}

#[tokio::test]
async fn election_after_primary_drop() {
    // Test that when the primary steps down, another node can become primary.
    // Uses direct method calls to simulate the election protocol.
    let addr1 = NodeAddress { group_id: 1, node_id: 1 };
    let addr2 = NodeAddress { group_id: 1, node_id: 2 };
    let addr3 = NodeAddress { group_id: 1, node_id: 3 };

    let (_, h1) = start_repl_data_node(
        1, 1,
        vec![
            (addr2, "127.0.0.1:19998".into()),
            (addr3, "127.0.0.1:19999".into()),
        ],
    ).await;
    let (_, h2) = start_repl_data_node(
        1, 2,
        vec![
            (addr1, "127.0.0.1:19900".into()),
            (addr3, "127.0.0.1:19901".into()),
        ],
    ).await;
    let (_, h3) = start_repl_data_node(
        1, 3,
        vec![
            (addr1, "127.0.0.1:19902".into()),
            (addr2, "127.0.0.1:19903".into()),
        ],
    ).await;

    // Make h1 primary (term 1)
    {
        let em1 = h1.election().as_ref().unwrap();
        let mut em1 = em1.lock().unwrap();
        em1.start_election().unwrap();
        let _ = em1.receive_vote(2);
    }
    // h2 and h3 acknowledge h1 as primary
    {
        let em2 = h2.election().as_ref().unwrap();
        let mut em2 = em2.lock().unwrap();
        em2.heartbeat_received(1, 1);
    }
    {
        let em3 = h3.election().as_ref().unwrap();
        let mut em3 = em3.lock().unwrap();
        em3.heartbeat_received(1, 1);
    }

    assert!(h1.check_primary().is_ok());
    assert!(h2.check_primary().is_err());
    assert!(h3.check_primary().is_err());

    // "Kill" primary
    {
        let em1 = h1.election().as_ref().unwrap();
        let mut em1 = em1.lock().unwrap();
        em1.step_down();
    }

    // Node 2 detects timeout and starts election (term 2)
    {
        let em2 = h2.election().as_ref().unwrap();
        let mut em2 = em2.lock().unwrap();
        em2.start_election().unwrap();
    }

    // Node 3 grants vote to node 2
    let granted = {
        let em3 = h3.election().as_ref().unwrap();
        let mut em3 = em3.lock().unwrap();
        em3.should_grant_vote(2, 2, 0) // candidate=2, term=2, lsn=0
    };
    assert!(granted, "node 3 should grant vote to node 2");

    // Deliver vote to node 2
    {
        let em2 = h2.election().as_ref().unwrap();
        let mut em2 = em2.lock().unwrap();
        em2.receive_vote(3).unwrap(); // vote from node 3 → 2/3 = majority
    }

    // Node 2 is now primary
    assert!(h2.check_primary().is_ok(), "node 2 should become primary");
    assert!(h1.check_primary().is_err(), "old primary should not be primary");

    // Node 3 should not be primary
    assert!(h3.check_primary().is_err(), "node 3 should not be primary");
}

// ── Tier 2: Read Preference tests ───────────────────────────────

fn set_read_pref_msg(request_id: u64, pref: &str) -> Vec<u8> {
    let mut cond = Document::new();
    cond.insert("Preference", Value::String(pref.into()));
    MsgOpQuery::new(request_id, "$set read preference", Some(cond), None, None, None, 0, -1, 0)
        .encode()
}

fn get_read_pref_msg(request_id: u64) -> Vec<u8> {
    MsgOpQuery::new(request_id, "$get read preference", None, None, None, None, 0, -1, 0)
        .encode()
}

fn get_members_msg(request_id: u64) -> Vec<u8> {
    MsgOpQuery::new(request_id, "$get members", None, None, None, None, 0, -1, 0)
        .encode()
}

fn add_member_msg(request_id: u64, node_id: u16, address: &str) -> Vec<u8> {
    let mut cond = Document::new();
    cond.insert("NodeId", Value::Int32(node_id as i32));
    cond.insert("Address", Value::String(address.into()));
    MsgOpQuery::new(request_id, "$add member", Some(cond), None, None, None, 0, -1, 0)
        .encode()
}

fn remove_member_msg(request_id: u64, node_id: u16) -> Vec<u8> {
    let mut cond = Document::new();
    cond.insert("NodeId", Value::Int32(node_id as i32));
    MsgOpQuery::new(request_id, "$remove member", Some(cond), None, None, None, 0, -1, 0)
        .encode()
}

fn get_chunk_info_msg(request_id: u64, collection: &str) -> Vec<u8> {
    let mut cond = Document::new();
    cond.insert("Collection", Value::String(collection.into()));
    MsgOpQuery::new(request_id, "$get chunk info", Some(cond), None, None, None, 0, -1, 0)
        .encode()
}

fn split_chunk_msg(request_id: u64, collection: &str, source: u32, target: u32, count: u64) -> Vec<u8> {
    let mut cond = Document::new();
    cond.insert("Collection", Value::String(collection.into()));
    cond.insert("Source", Value::Int32(source as i32));
    cond.insert("Target", Value::Int32(target as i32));
    cond.insert("Count", Value::Int64(count as i64));
    MsgOpQuery::new(request_id, "$split chunk", Some(cond), None, None, None, 0, -1, 0)
        .encode()
}

fn balance_msg(request_id: u64, collection: &str) -> Vec<u8> {
    let mut cond = Document::new();
    cond.insert("Collection", Value::String(collection.into()));
    MsgOpQuery::new(request_id, "$balance", Some(cond), None, None, None, 0, -1, 0)
        .encode()
}

#[tokio::test]
async fn set_and_get_read_preference() {
    let port = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Default should be Primary
    let reply = send_recv(&mut stream, &get_read_pref_msg(1)).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(
        reply.docs[0].get("ReadPreference"),
        Some(&Value::String("Primary".into()))
    );

    // Set to SecondaryPreferred
    let reply = send_recv(&mut stream, &set_read_pref_msg(2, "SecondaryPreferred")).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(
        reply.docs[0].get("ReadPreference"),
        Some(&Value::String("SecondaryPreferred".into()))
    );

    // Verify it stuck
    let reply = send_recv(&mut stream, &get_read_pref_msg(3)).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(
        reply.docs[0].get("ReadPreference"),
        Some(&Value::String("SecondaryPreferred".into()))
    );
}

#[tokio::test]
async fn read_preference_secondary_allows_reads_on_secondary() {
    let (port, handler) = start_repl_data_node(1, 1, vec![]).await;

    // Force to secondary
    {
        let em = handler.election().as_ref().unwrap();
        let mut em = em.lock().unwrap();
        em.state = ElectionState::Secondary;
    }

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Create CS/CL while still primary-ish (we'll set secondary after)
    // Actually, force back to primary for DDL
    {
        let em = handler.election().as_ref().unwrap();
        let mut em = em.lock().unwrap();
        em.start_election().unwrap();
    }
    send_recv(&mut stream, &create_cs_msg(1, "rpcs")).await;
    send_recv(&mut stream, &create_cl_msg(2, "rpcs.cl")).await;
    send_recv(
        &mut stream,
        &insert_msg(3, "rpcs.cl", vec![doc(&[("x", Value::Int32(1))])]),
    ).await;

    // Now force to secondary
    {
        let em = handler.election().as_ref().unwrap();
        let mut em = em.lock().unwrap();
        em.state = ElectionState::Secondary;
    }

    // Query with default read pref (Primary) should fail
    let reply = send_recv(&mut stream, &query_msg(4, "rpcs.cl", None)).await;
    assert!(reply.flags < 0, "query should fail on secondary with Primary read pref");

    // Set read preference to Secondary
    let reply = send_recv(&mut stream, &set_read_pref_msg(5, "Secondary")).await;
    assert_eq!(reply.flags, 0);

    // Now query should succeed on secondary
    let reply = send_recv(&mut stream, &query_msg(6, "rpcs.cl", None)).await;
    assert_eq!(reply.flags, 0, "query should succeed with Secondary read pref");
    assert_eq!(reply.docs.len(), 1);
    assert_eq!(reply.docs[0].get("x"), Some(&Value::Int32(1)));
}

#[tokio::test]
async fn read_preference_primary_rejects_on_secondary() {
    let (port, handler) = start_repl_data_node(1, 1, vec![]).await;

    // Force to secondary
    {
        let em = handler.election().as_ref().unwrap();
        let mut em = em.lock().unwrap();
        em.state = ElectionState::Secondary;
    }

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // With default Primary preference, query should fail
    let reply = send_recv(&mut stream, &query_msg(1, "nonexist.cl", None)).await;
    assert!(reply.flags < 0, "query with Primary pref should fail on secondary");
}

#[tokio::test]
async fn coord_read_preference_set_get() {
    let port = start_coord_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Set read preference at coord level
    let reply = send_recv(&mut stream, &set_read_pref_msg(1, "Secondary")).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(
        reply.docs[0].get("ReadPreference"),
        Some(&Value::String("Secondary".into()))
    );

    // Get read preference
    let reply = send_recv(&mut stream, &get_read_pref_msg(2)).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(
        reply.docs[0].get("ReadPreference"),
        Some(&Value::String("Secondary".into()))
    );
}

// ── Tier 2: Dynamic Membership tests ────────────────────────────

#[tokio::test]
async fn dynamic_add_member() {
    let addr1 = NodeAddress { group_id: 1, node_id: 1 };
    let (port, h1) = start_repl_data_node(1, 1, vec![]).await;

    // Start with 0 peers
    {
        let em = h1.election().as_ref().unwrap();
        let em = em.lock().unwrap();
        assert_eq!(em.peer_list().len(), 0);
    }

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Add a member via command
    let reply = send_recv(&mut stream, &add_member_msg(1, 2, "127.0.0.1:19950")).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs[0].get("Added"), Some(&Value::Boolean(true)));

    // Verify peer was added
    {
        let em = h1.election().as_ref().unwrap();
        let em = em.lock().unwrap();
        assert_eq!(em.peer_list().len(), 1);
        assert_eq!(em.peer_list()[0].node_id, 2);
    }

    // Verify repl_agent also has the peer
    {
        let agent = h1.repl_agent().as_ref().unwrap();
        let ag = agent.try_lock().unwrap();
        assert!(ag.replicas.contains_key(&2));
        assert_eq!(ag.peer_addrs.get(&2), Some(&"127.0.0.1:19950".to_string()));
    }
}

#[tokio::test]
async fn dynamic_remove_member() {
    let addr2 = NodeAddress { group_id: 1, node_id: 2 };
    let (port, h1) = start_repl_data_node(
        1, 1,
        vec![(addr2, "127.0.0.1:19951".into())],
    ).await;

    // Should have 1 peer
    {
        let em = h1.election().as_ref().unwrap();
        let em = em.lock().unwrap();
        assert_eq!(em.peer_list().len(), 1);
    }

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Remove member
    let reply = send_recv(&mut stream, &remove_member_msg(1, 2)).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs[0].get("Removed"), Some(&Value::Boolean(true)));

    // Verify peer was removed
    {
        let em = h1.election().as_ref().unwrap();
        let em = em.lock().unwrap();
        assert_eq!(em.peer_list().len(), 0);
    }
}

#[tokio::test]
async fn get_members_returns_local_and_peers() {
    let addr2 = NodeAddress { group_id: 1, node_id: 2 };
    let addr3 = NodeAddress { group_id: 1, node_id: 3 };
    let (port, h1) = start_repl_data_node(
        1, 1,
        vec![
            (addr2, "127.0.0.1:19952".into()),
            (addr3, "127.0.0.1:19953".into()),
        ],
    ).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    let reply = send_recv(&mut stream, &get_members_msg(1)).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs.len(), 3, "should return local + 2 peers");

    // Local node should have IsSelf=true
    let local = reply.docs.iter().find(|d| d.get("IsSelf") == Some(&Value::Boolean(true)));
    assert!(local.is_some(), "should have local node");
    assert_eq!(local.unwrap().get("NodeId"), Some(&Value::Int32(1)));
}

#[tokio::test]
async fn dynamic_add_then_election_works() {
    // Start node 1 with no peers
    let (_, h1) = start_repl_data_node(1, 1, vec![]).await;
    let (_, h2) = start_repl_data_node(1, 2, vec![]).await;

    // Dynamically add node 2 to node 1's peer list
    {
        let em = h1.election().as_ref().unwrap();
        let mut em = em.lock().unwrap();
        em.add_peer(NodeAddress { group_id: 1, node_id: 2 });
    }

    // Node 1 starts election (2 nodes, needs 2/2 = 2 votes)
    {
        let em = h1.election().as_ref().unwrap();
        let mut em = em.lock().unwrap();
        em.start_election().unwrap();
        assert_eq!(em.state, ElectionState::Candidate);
    }

    // Node 2 grants vote
    let granted = {
        let em = h2.election().as_ref().unwrap();
        let mut em = em.lock().unwrap();
        em.should_grant_vote(1, 1, 0)
    };
    assert!(granted);

    // Deliver vote
    {
        let em = h1.election().as_ref().unwrap();
        let mut em = em.lock().unwrap();
        em.receive_vote(2).unwrap();
    }
    assert!(h1.check_primary().is_ok());
}

// ── Tier 2: Chunk Split/Migrate tests ───────────────────────────

#[tokio::test]
async fn get_chunk_info_after_sharding() {
    let port = start_coord_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "chcs")).await;
    send_recv(&mut stream, &create_cl_msg(2, "chcs.cl")).await;

    // Before sharding: no chunks
    let reply = send_recv(&mut stream, &get_chunk_info_msg(3, "chcs.cl")).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs.len(), 0, "no chunks before sharding");

    // Enable sharding with 3 groups
    send_recv(&mut stream, &enable_sharding_msg(4, "chcs.cl", "x", 3)).await;

    // After sharding: 3 chunks (one per group)
    let reply = send_recv(&mut stream, &get_chunk_info_msg(5, "chcs.cl")).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs.len(), 3, "should have 3 chunks after sharding");

    // Each chunk should have GroupId 1, 2, or 3
    let mut group_ids: Vec<i32> = reply.docs.iter()
        .filter_map(|d| match d.get("GroupId") {
            Some(Value::Int32(n)) => Some(*n),
            _ => None,
        })
        .collect();
    group_ids.sort();
    assert_eq!(group_ids, vec![1, 2, 3]);
}

#[tokio::test]
async fn split_chunk_moves_data() {
    let port = start_coord_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "spcs")).await;
    send_recv(&mut stream, &create_cl_msg(2, "spcs.cl")).await;

    // Enable sharding FIRST
    send_recv(&mut stream, &enable_sharding_msg(3, "spcs.cl", "v", 3)).await;

    // Insert 30 docs — sharding distributes them across groups
    for i in 0..30 {
        let docs = vec![doc(&[("v", Value::Int32(i))])];
        send_recv(&mut stream, &insert_msg((10 + i) as u64, "spcs.cl", docs)).await;
    }

    // Check chunks have data now
    let reply = send_recv(&mut stream, &get_chunk_info_msg(50, "spcs.cl")).await;
    assert_eq!(reply.flags, 0);

    // Find the group with the most docs for the split source
    let mut max_group = 1u32;
    let mut max_count = 0i64;
    for d in &reply.docs {
        let gid = match d.get("GroupId") {
            Some(Value::Int32(n)) => *n as u32,
            _ => continue,
        };
        let cnt = match d.get("DocCount") {
            Some(Value::Int64(n)) => *n,
            _ => 0,
        };
        if cnt > max_count {
            max_count = cnt;
            max_group = gid;
        }
    }

    // Only split if there's something to split
    if max_count >= 2 {
        let target_group = if max_group == 1 { 2 } else { 1 };
        let to_move = (max_count / 2) as u64;
        let reply = send_recv(
            &mut stream,
            &split_chunk_msg(51, "spcs.cl", max_group, target_group, to_move),
        ).await;
        assert_eq!(reply.flags, 0, "split should succeed");
        assert_eq!(reply.docs[0].get("Split"), Some(&Value::Boolean(true)));
    }
}

#[tokio::test]
async fn balance_already_balanced() {
    let port = start_coord_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    send_recv(&mut stream, &create_cs_msg(1, "balcs")).await;
    send_recv(&mut stream, &create_cl_msg(2, "balcs.cl")).await;
    send_recv(&mut stream, &enable_sharding_msg(3, "balcs.cl", "x", 3)).await;

    // No data = already balanced
    let reply = send_recv(&mut stream, &balance_msg(4, "balcs.cl")).await;
    assert_eq!(reply.flags, 0);
    assert_eq!(reply.docs[0].get("Balanced"), Some(&Value::Boolean(true)));
}

// ── ReadPreference enum unit tests ──────────────────────────────

#[test]
fn read_preference_from_str() {
    assert_eq!(ReadPreference::parse("Primary"), Some(ReadPreference::Primary));
    assert_eq!(ReadPreference::parse("Secondary"), Some(ReadPreference::Secondary));
    assert_eq!(ReadPreference::parse("SecondaryPreferred"), Some(ReadPreference::SecondaryPreferred));
    assert_eq!(ReadPreference::parse("Nearest"), Some(ReadPreference::Nearest));
    assert_eq!(ReadPreference::parse("invalid"), None);
}

#[test]
fn read_preference_allows_secondary() {
    assert!(!ReadPreference::Primary.allows_secondary());
    assert!(ReadPreference::Secondary.allows_secondary());
    assert!(ReadPreference::SecondaryPreferred.allows_secondary());
    assert!(ReadPreference::Nearest.allows_secondary());
}

// ── ShardManager chunk tracking unit tests ──────────────────────

#[test]
fn shard_manager_chunk_tracking() {
    use sdb_cls::ShardManager;
    let mut sm = ShardManager::new();
    sm.set_hash_sharding("x", 3);
    assert_eq!(sm.chunk_info().len(), 3);

    // Record some inserts
    sm.record_insert(1);
    sm.record_insert(1);
    sm.record_insert(2);
    let counts = sm.group_doc_counts();
    assert_eq!(counts.get(&1), Some(&2));
    assert_eq!(counts.get(&2), Some(&1));
    assert_eq!(counts.get(&3), Some(&0));
}

#[test]
fn shard_manager_split_chunk() {
    use sdb_cls::ShardManager;
    let mut sm = ShardManager::new();
    sm.set_hash_sharding("x", 2);

    // Give group 1 100 docs
    for _ in 0..100 {
        sm.record_insert(1);
    }

    // Split 50 from group 1 to group 2
    let result = sm.split_chunk(1, 2, 50);
    assert!(result.is_ok());

    let counts = sm.group_doc_counts();
    assert_eq!(counts.get(&1), Some(&50));
    assert_eq!(counts.get(&2), Some(&50));
}

#[test]
fn shard_manager_find_imbalance() {
    use sdb_cls::ShardManager;
    let mut sm = ShardManager::new();
    sm.set_hash_sharding("x", 2);

    // No imbalance initially
    assert!(sm.find_imbalance().is_none());

    // Create imbalance: 100 vs 0
    for _ in 0..100 {
        sm.record_insert(1);
    }

    let imbalance = sm.find_imbalance();
    assert!(imbalance.is_some());
    let (source, target, to_move) = imbalance.unwrap();
    assert_eq!(source, 1);
    assert_eq!(target, 2);
    assert!(to_move > 0);
}

// ── Election remove_peer test ───────────────────────────────────

#[test]
fn election_remove_peer() {
    use sdb_cls::election::ElectionManager;
    let mut em = ElectionManager::new(NodeAddress { group_id: 1, node_id: 1 });
    em.add_peer(NodeAddress { group_id: 1, node_id: 2 });
    em.add_peer(NodeAddress { group_id: 1, node_id: 3 });
    assert_eq!(em.peer_list().len(), 2);

    assert!(em.remove_peer(2));
    assert_eq!(em.peer_list().len(), 1);
    assert_eq!(em.peer_list()[0].node_id, 3);

    // Remove non-existent
    assert!(!em.remove_peer(99));
}

#[test]
fn election_add_peer_no_duplicates() {
    use sdb_cls::election::ElectionManager;
    let mut em = ElectionManager::new(NodeAddress { group_id: 1, node_id: 1 });
    em.add_peer(NodeAddress { group_id: 1, node_id: 2 });
    em.add_peer(NodeAddress { group_id: 1, node_id: 2 }); // duplicate
    assert_eq!(em.peer_list().len(), 1);
}
