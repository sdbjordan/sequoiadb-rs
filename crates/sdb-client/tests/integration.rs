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
