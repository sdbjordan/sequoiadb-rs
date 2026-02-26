# Quickstart

Build, run, and perform CRUD operations in 5 minutes.

## Prerequisites

- Rust 1.75+ (stable)
- cargo

## Build

```bash
git clone https://github.com/sdbjordan/sequoiadb-rs.git
cd sequoiadb-rs
cargo build --release
```

The binary is at `target/release/sdb-server`.

## Run a Single Data Node

```bash
# Create data directory
mkdir -p /tmp/sdb-data

# Start the server
cargo run --release -- -r data -p 11810 -d /tmp/sdb-data
```

The server is now listening on `127.0.0.1:11810`.

## Run Tests

```bash
# Run all tests (80 integration + unit tests)
cargo test --workspace

# Run benchmarks
cargo bench --package sdb-server
```

## CRUD via Rust Client

Add `sdb-client` and `sdb-bson` to your `Cargo.toml`:

```toml
[dependencies]
sdb-client = { path = "crates/sdb-client" }
sdb-bson = { path = "crates/sdb-bson" }
```

### Connect

```rust
use sdb_client::{Client, ConnectOptions};

let opts = ConnectOptions {
    host: "127.0.0.1".into(),
    port: 11810,
    ..Default::default()
};
let client = Client::connect(opts).await?;
```

### Create Collection Space and Collection

```rust
client.create_collection_space("mydb").await?;
client.create_collection("mydb", "users").await?;
```

### Insert Documents

```rust
use sdb_bson::DocumentBuilder;

let doc = DocumentBuilder::new()
    .append_string("name", "Alice")
    .append_i32("age", 30)
    .append_string("email", "alice@example.com")
    .build();

let cl = client.collection("mydb.users");
cl.insert(doc).await?;

// Batch insert
let docs = vec![
    DocumentBuilder::new()
        .append_string("name", "Bob")
        .append_i32("age", 25)
        .build(),
    DocumentBuilder::new()
        .append_string("name", "Carol")
        .append_i32("age", 35)
        .build(),
];
cl.insert_many(docs).await?;
```

### Query

```rust
use sdb_bson::{Document, Value};

// Query all
let mut cursor = cl.query(Document::new()).await?;
while let Some(doc) = cursor.next() {
    println!("{:?}", doc);
}

// Query with condition
let mut cond = Document::new();
cond.insert("age", Value::Document({
    let mut d = Document::new();
    d.insert("$gte", Value::Int32(30));
    d
}));
let mut cursor = cl.query(cond).await?;
```

### Update

```rust
let mut cond = Document::new();
cond.insert("name", Value::String("Alice".into()));

let mut rule = Document::new();
let mut set_doc = Document::new();
set_doc.insert("age", Value::Int32(31));
rule.insert("$set", Value::Document(set_doc));

cl.update(cond, rule).await?;
```

### Delete

```rust
let mut cond = Document::new();
cond.insert("name", Value::String("Bob".into()));
cl.delete(cond).await?;
```

### Create Index

```rust
client.create_index("mydb", "users", "idx_age",
    DocumentBuilder::new().append_i32("age", 1).build(),
    false  // unique
).await?;
```

### Count

```rust
let count = cl.count(Document::new()).await?;
println!("Total documents: {}", count);
```

### Aggregation

```rust
let pipeline = vec![
    DocumentBuilder::new()
        .append_document("$match",
            DocumentBuilder::new().append_i32("age", 30).build())
        .build(),
    DocumentBuilder::new()
        .append_document("$limit",
            DocumentBuilder::new().append_i64("n", 10).build())
        .build(),
];
let results = cl.aggregate(pipeline).await?;
```

### SQL

```rust
let results = client.exec_sql(
    "SELECT * FROM mydb.users WHERE age = 30"
).await?;
```

### Transactions

```rust
client.transaction_begin().await?;
cl.insert(DocumentBuilder::new()
    .append_string("name", "Dave")
    .append_i32("age", 28)
    .build()
).await?;
client.transaction_commit().await?;
// Or: client.transaction_rollback().await?;
```

### Authentication

```rust
// Bootstrap: create first user (no auth required)
client.create_user("admin", "secret", vec!["admin".into()]).await?;

// Subsequent connections must authenticate
let opts = ConnectOptions {
    host: "127.0.0.1".into(),
    port: 11810,
    username: Some("admin".into()),
    password: Some("secret".into()),
    ..Default::default()
};
let client = Client::connect(opts).await?;
```

## Multi-Node Deployment

### Coordinator + 3 Data Groups

```bash
# Start 3 data nodes (each is its own group)
sdb-server -r data -p 11810 -d /tmp/sdb-g1 &
sdb-server -r data -p 11811 -d /tmp/sdb-g2 &
sdb-server -r data -p 11812 -d /tmp/sdb-g3 &

# Start coordinator
sdb-server -r coord -p 11800 -d /tmp/sdb-coord \
  --data-addr "127.0.0.1:11810" \
  --data-addr "127.0.0.1:11811" \
  --data-addr "127.0.0.1:11812"
```

Connect to the coordinator on port 11800. DDL commands are broadcast to all groups; queries are scatter-gathered.

### Enable Sharding

Via the coordinator connection:

```rust
// Hash sharding
// Send: $enable sharding { Collection: "mydb.users", ShardKey: "user_id", NumGroups: 3 }

// Range sharding
// Send: $enable range sharding { Collection: "mydb.orders", ShardKey: "amount" }
// Send: $add range { Collection: "mydb.orders", GroupId: 1, LowBound: null, UpBound: 1000 }
// Send: $add range { Collection: "mydb.orders", GroupId: 2, LowBound: 1000, UpBound: 5000 }
// Send: $add range { Collection: "mydb.orders", GroupId: 3, LowBound: 5000, UpBound: null }
```

### 3-Node Replica Set

```bash
sdb-server -r data -p 11810 -d /tmp/n1 --node-id 1 \
  --repl-peer "2@127.0.0.1:11811" --repl-peer "3@127.0.0.1:11812" &
sdb-server -r data -p 11811 -d /tmp/n2 --node-id 2 \
  --repl-peer "1@127.0.0.1:11810" --repl-peer "3@127.0.0.1:11812" &
sdb-server -r data -p 11812 -d /tmp/n3 --node-id 3 \
  --repl-peer "1@127.0.0.1:11810" --repl-peer "2@127.0.0.1:11811" &
```

Nodes will elect a primary within ~4 seconds. Write to the primary; configure read preference for secondary reads.

## Monitoring

```rust
// Send: $snapshot database → returns TotalInsert, TotalQuery, etc.
// Send: $snapshot sessions → returns active session count
// Send: $snapshot collections → returns collection list
// Send: $snapshot health → returns node health status
```
