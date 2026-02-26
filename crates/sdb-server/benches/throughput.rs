use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::{Arc, RwLock};

use sdb_bson::{Document, Value};
use sdb_cat::CatalogManager;

/// Benchmark raw catalog insert throughput (no network, no WAL).
fn bench_catalog_insert(c: &mut Criterion) {
    let catalog = Arc::new(RwLock::new(CatalogManager::new()));
    {
        let mut cat = catalog.write().unwrap();
        cat.create_collection_space("bench").unwrap();
        cat.create_collection("bench", "cl").unwrap();
    }

    c.bench_function("catalog_insert_single", |b| {
        b.iter(|| {
            let mut doc = Document::new();
            doc.insert("x", Value::Int32(42));
            doc.insert("name", Value::String("benchmark".into()));
            let cat = catalog.read().unwrap();
            cat.insert_document("bench", "cl", &doc).unwrap();
        });
    });
}

/// Benchmark catalog batch insert (10 docs at a time).
fn bench_catalog_insert_batch(c: &mut Criterion) {
    let catalog = Arc::new(RwLock::new(CatalogManager::new()));
    {
        let mut cat = catalog.write().unwrap();
        cat.create_collection_space("bench2").unwrap();
        cat.create_collection("bench2", "cl").unwrap();
    }

    c.bench_function("catalog_insert_batch_10", |b| {
        b.iter(|| {
            let cat = catalog.read().unwrap();
            for i in 0..10 {
                let mut doc = Document::new();
                doc.insert("x", Value::Int32(i));
                cat.insert_document("bench2", "cl", &doc).unwrap();
            }
        });
    });
}

/// Benchmark catalog scan throughput.
fn bench_catalog_scan(c: &mut Criterion) {
    let catalog = Arc::new(RwLock::new(CatalogManager::new()));
    {
        let mut cat = catalog.write().unwrap();
        cat.create_collection_space("scan").unwrap();
        cat.create_collection("scan", "cl").unwrap();

        // Pre-populate with 1000 docs
        for i in 0..1000 {
            let mut doc = Document::new();
            doc.insert("x", Value::Int32(i));
            doc.insert("name", Value::String(format!("doc_{}", i)));
            cat.insert_document("scan", "cl", &doc).unwrap();
        }
    }

    c.bench_function("catalog_scan_1000", |b| {
        b.iter(|| {
            let cat = catalog.read().unwrap();
            let (storage, _) = cat.collection_handle("scan", "cl").unwrap();
            let rows = storage.scan();
            assert!(rows.len() >= 1000);
        });
    });
}

/// Benchmark BSON document encode/decode.
fn bench_bson_roundtrip(c: &mut Criterion) {
    let mut doc = Document::new();
    doc.insert("_id", Value::Int64(12345));
    doc.insert("name", Value::String("John Doe".into()));
    doc.insert("age", Value::Int32(30));
    doc.insert("email", Value::String("john@example.com".into()));
    let mut addr = Document::new();
    addr.insert("city", Value::String("San Francisco".into()));
    addr.insert("zip", Value::String("94102".into()));
    doc.insert("address", Value::Document(addr));

    c.bench_function("bson_encode", |b| {
        b.iter(|| {
            let _bytes = doc.to_bytes().unwrap();
        });
    });

    let bytes = doc.to_bytes().unwrap();
    c.bench_function("bson_decode", |b| {
        b.iter(|| {
            let _decoded = Document::from_bytes(&bytes).unwrap();
        });
    });
}

/// Benchmark shard routing hash throughput.
fn bench_shard_routing(c: &mut Criterion) {
    use sdb_cls::ShardManager;

    let mut sm = ShardManager::new();
    sm.set_hash_sharding("user_id", 8);

    let mut doc = Document::new();
    doc.insert("user_id", Value::Int32(42));

    c.bench_function("shard_route_hash", |b| {
        b.iter(|| {
            let _gid = sm.route(&doc);
        });
    });
}

/// Benchmark matcher throughput.
fn bench_matcher(c: &mut Criterion) {
    use sdb_mth::Matcher;

    let mut cond = Document::new();
    cond.insert("age", Value::Int32(30));

    let matcher = Matcher::new(cond).unwrap();

    let mut doc = Document::new();
    doc.insert("name", Value::String("Alice".into()));
    doc.insert("age", Value::Int32(30));
    doc.insert("city", Value::String("NYC".into()));

    c.bench_function("matcher_single_field", |b| {
        b.iter(|| {
            let _m = matcher.matches(&doc).unwrap();
        });
    });
}

criterion_group!(
    benches,
    bench_catalog_insert,
    bench_catalog_insert_batch,
    bench_catalog_scan,
    bench_bson_roundtrip,
    bench_shard_routing,
    bench_matcher,
);
criterion_main!(benches);
