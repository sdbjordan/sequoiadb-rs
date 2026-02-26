use std::sync::{Arc, RwLock};

use sdb_bson::{Document, Value};
use sdb_cat::CatalogManager;
use sdb_client::{Client, ConnectOptions};
use sdb_net::{Connection, MessageHandler};
use sdb_server::handler::DataNodeHandler;
use tokio::net::TcpListener;

fn doc(pairs: &[(&str, Value)]) -> Document {
    let mut d = Document::new();
    for (k, v) in pairs {
        d.insert(*k, v.clone());
    }
    d
}

/// Start a DataNodeHandler on an ephemeral port, return the port.
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
                let mut conn = Connection::new(stream, addr);
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

/// Helper: connect a Client to the test server.
async fn test_client(port: u16) -> Client {
    Client::connect(ConnectOptions {
        host: "127.0.0.1".into(),
        port,
        ..Default::default()
    })
    .await
    .unwrap()
}

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn connect_disconnect() {
    let port = start_test_server().await;
    let mut client = test_client(port).await;
    assert!(client.is_connected());
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn create_cs_cl_insert_query() {
    let port = start_test_server().await;
    let client = test_client(port).await;

    client.create_collection_space("testcs").await.unwrap();
    client.create_collection("testcs", "testcl").await.unwrap();

    let cl = client.get_collection("testcs", "testcl");

    cl.insert(doc(&[
        ("x", Value::Int32(1)),
        ("name", Value::String("alice".into())),
    ]))
    .await
    .unwrap();

    cl.insert(doc(&[
        ("x", Value::Int32(2)),
        ("name", Value::String("bob".into())),
    ]))
    .await
    .unwrap();

    let docs = cl.query(None).await.unwrap().collect_all();
    assert_eq!(docs.len(), 2);
}

#[tokio::test]
async fn insert_many_then_query_condition() {
    let port = start_test_server().await;
    let client = test_client(port).await;

    client.create_collection_space("cs2").await.unwrap();
    client.create_collection("cs2", "cl").await.unwrap();

    let cl = client.get_collection("cs2", "cl");

    let docs: Vec<Document> = (0..5)
        .map(|i| doc(&[("val", Value::Int32(i))]))
        .collect();
    cl.insert_many(docs).await.unwrap();

    // Query with condition: val >= 3
    let cond = doc(&[("val", Value::Document(doc(&[("$gte", Value::Int32(3))])))]);
    let results = cl.query(Some(cond)).await.unwrap().collect_all();
    assert_eq!(results.len(), 2); // val=3, val=4
}

#[tokio::test]
async fn update_then_verify() {
    let port = start_test_server().await;
    let client = test_client(port).await;

    client.create_collection_space("cs3").await.unwrap();
    client.create_collection("cs3", "cl").await.unwrap();

    let cl = client.get_collection("cs3", "cl");
    cl.insert(doc(&[("x", Value::Int32(1)), ("y", Value::Int32(10))]))
        .await
        .unwrap();

    // Update: { x: 1 } → { $set: { y: 99 } }
    cl.update(
        doc(&[("x", Value::Int32(1))]),
        doc(&[("$set", Value::Document(doc(&[("y", Value::Int32(99))])))]),
    )
    .await
    .unwrap();

    let docs = cl.query(None).await.unwrap().collect_all();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].get("y"), Some(&Value::Int32(99)));
}

#[tokio::test]
async fn delete_then_verify() {
    let port = start_test_server().await;
    let client = test_client(port).await;

    client.create_collection_space("cs4").await.unwrap();
    client.create_collection("cs4", "cl").await.unwrap();

    let cl = client.get_collection("cs4", "cl");
    cl.insert_many(vec![
        doc(&[("x", Value::Int32(1))]),
        doc(&[("x", Value::Int32(2))]),
        doc(&[("x", Value::Int32(3))]),
    ])
    .await
    .unwrap();

    cl.delete(doc(&[("x", Value::Int32(2))])).await.unwrap();

    let docs = cl.query(None).await.unwrap().collect_all();
    assert_eq!(docs.len(), 2);
    let vals: Vec<i32> = docs
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
    let client = test_client(port).await;

    client.create_collection_space("cs5").await.unwrap();
    client.create_collection("cs5", "cl").await.unwrap();

    let cl = client.get_collection("cs5", "cl");
    let docs: Vec<Document> = (0..10)
        .map(|i| doc(&[("val", Value::Int32(i))]))
        .collect();
    cl.insert_many(docs).await.unwrap();

    // Create index on "val"
    client
        .create_index("cs5", "cl", "idx_val", doc(&[("val", Value::Int32(1))]), false)
        .await
        .unwrap();

    // Point query — optimizer should pick index scan
    let results = cl
        .query(Some(doc(&[("val", Value::Int32(5))])))
        .await
        .unwrap()
        .collect_all();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("val"), Some(&Value::Int32(5)));
}

#[tokio::test]
async fn drop_cl_drop_cs() {
    let port = start_test_server().await;
    let client = test_client(port).await;

    client.create_collection_space("cs6").await.unwrap();
    client.create_collection("cs6", "cl").await.unwrap();

    let cl = client.get_collection("cs6", "cl");
    cl.insert(doc(&[("a", Value::Int32(1))])).await.unwrap();

    // Drop CL
    client.drop_collection("cs6", "cl").await.unwrap();

    // Query should fail
    let err = cl.query(None).await;
    assert!(err.is_err());

    // Drop CS
    client.drop_collection_space("cs6").await.unwrap();
}

#[tokio::test]
async fn error_nonexistent() {
    let port = start_test_server().await;
    let client = test_client(port).await;

    // Query on nonexistent CS/CL
    let cl = client.get_collection("nope", "nope");
    assert!(cl.query(None).await.is_err());

    // Insert on nonexistent CS/CL
    assert!(cl.insert(doc(&[("x", Value::Int32(1))])).await.is_err());

    // Create CL without CS
    assert!(client.create_collection("nope", "cl").await.is_err());
}

// ── Auth tests ──────────────────────────────────────────────────────

#[tokio::test]
async fn auth_create_user_and_authenticate() {
    let port = start_test_server().await;
    let client = test_client(port).await;

    // Create user
    client
        .create_user("testuser", "pass123", vec!["admin".into()])
        .await
        .unwrap();

    // Authenticate with correct creds
    client.authenticate("testuser", "pass123").await.unwrap();

    // Wrong password should fail
    assert!(client.authenticate("testuser", "wrong").await.is_err());

    // Drop user
    client.drop_user("testuser").await.unwrap();

    // Auth after drop should fail
    assert!(client.authenticate("testuser", "pass123").await.is_err());
}

// ── Aggregate tests ─────────────────────────────────────────────────

#[tokio::test]
async fn aggregate_match_and_limit() {
    let port = start_test_server().await;
    let client = test_client(port).await;

    client.create_collection_space("aggcs").await.unwrap();
    client.create_collection("aggcs", "cl").await.unwrap();

    let cl = client.get_collection("aggcs", "cl");

    let docs: Vec<Document> = (0..10)
        .map(|i| doc(&[("val", Value::Int32(i))]))
        .collect();
    cl.insert_many(docs).await.unwrap();

    // Aggregate: $match val >= 5 → $limit 3
    let pipeline = vec![
        Value::Document(doc(&[(
            "$match",
            Value::Document(doc(&[("val", Value::Document(doc(&[("$gte", Value::Int32(5))])))])),
        )])),
        Value::Document(doc(&[("$limit", Value::Int32(3))])),
    ];
    let results = cl.aggregate(pipeline).await.unwrap();
    assert_eq!(results.len(), 3);
    // All should have val >= 5
    for r in &results {
        match r.get("val") {
            Some(Value::Int32(v)) => assert!(*v >= 5),
            _ => panic!("expected Int32 val"),
        }
    }
}

// ── SQL tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn sql_select() {
    let port = start_test_server().await;
    let client = test_client(port).await;

    client.create_collection_space("sqlcs").await.unwrap();
    client.create_collection("sqlcs", "cl").await.unwrap();

    let cl = client.get_collection("sqlcs", "cl");
    cl.insert_many(vec![
        doc(&[("name", Value::String("alice".into())), ("age", Value::Int32(30))]),
        doc(&[("name", Value::String("bob".into())), ("age", Value::Int32(25))]),
    ])
    .await
    .unwrap();

    let results = client.exec_sql("SELECT * FROM sqlcs.cl").await.unwrap();
    assert_eq!(results.len(), 2);
}

// ── Server-side count tests ─────────────────────────────────────────

#[tokio::test]
async fn server_side_count() {
    let port = start_test_server().await;
    let client = test_client(port).await;

    client.create_collection_space("cntcs").await.unwrap();
    client.create_collection("cntcs", "cl").await.unwrap();

    let cl = client.get_collection("cntcs", "cl");
    let docs: Vec<Document> = (0..8)
        .map(|i| doc(&[("val", Value::Int32(i))]))
        .collect();
    cl.insert_many(docs).await.unwrap();

    // Count all
    let count = cl.count(None).await.unwrap();
    assert_eq!(count, 8);

    // Count with condition
    let cond = doc(&[("val", Value::Document(doc(&[("$gte", Value::Int32(5))])))]);
    let count = cl.count(Some(cond)).await.unwrap();
    assert_eq!(count, 3); // val=5,6,7
}

// ── GetMore cursor tests ────────────────────────────────────────────

#[tokio::test]
async fn get_more_cursor_collect_all_async() {
    let port = start_test_server().await;
    let client = test_client(port).await;

    client.create_collection_space("gmcs").await.unwrap();
    client.create_collection("gmcs", "cl").await.unwrap();

    let cl = client.get_collection("gmcs", "cl");

    // Insert 250 docs (> batch size of 100)
    let docs: Vec<Document> = (0..250)
        .map(|i| doc(&[("i", Value::Int32(i))]))
        .collect();
    cl.insert_many(docs).await.unwrap();

    // collect_all_async should fetch all batches transparently
    let mut cursor = cl.query(None).await.unwrap();
    let all = cursor.collect_all_async().await.unwrap();
    assert_eq!(all.len(), 250, "should collect all 250 docs");
}

#[tokio::test]
async fn get_more_cursor_manual_fetch() {
    let port = start_test_server().await;
    let client = test_client(port).await;

    client.create_collection_space("gm2cs").await.unwrap();
    client.create_collection("gm2cs", "cl").await.unwrap();

    let cl = client.get_collection("gm2cs", "cl");

    // Insert 150 docs
    let docs: Vec<Document> = (0..150)
        .map(|i| doc(&[("i", Value::Int32(i))]))
        .collect();
    cl.insert_many(docs).await.unwrap();

    let mut cursor = cl.query(None).await.unwrap();

    // First batch: 100 docs in buffer
    let mut count = 0;
    while let Some(_) = cursor.next() {
        count += 1;
    }
    assert_eq!(count, 100, "first batch should be 100");
    assert!(cursor.has_more(), "should have more batches");

    // Fetch next batch
    let got_more = cursor.fetch_more().await.unwrap();
    assert!(got_more, "should have fetched more docs");

    let mut count2 = 0;
    while let Some(_) = cursor.next() {
        count2 += 1;
    }
    assert_eq!(count2, 50, "second batch should be 50");
    assert!(!cursor.has_more(), "should be exhausted (context_id == -1)");
}

#[tokio::test]
async fn cursor_close_async_kills_server_cursor() {
    let port = start_test_server().await;
    let client = test_client(port).await;

    client.create_collection_space("clcs").await.unwrap();
    client.create_collection("clcs", "cl").await.unwrap();

    let cl = client.get_collection("clcs", "cl");
    let docs: Vec<Document> = (0..200)
        .map(|i| doc(&[("i", Value::Int32(i))]))
        .collect();
    cl.insert_many(docs).await.unwrap();

    let mut cursor = cl.query(None).await.unwrap();
    assert!(cursor.has_more());

    // Close the cursor — sends KillContext to server
    cursor.close_async().await.unwrap();
    assert!(!cursor.has_more());
    assert!(cursor.next().is_none());
}
