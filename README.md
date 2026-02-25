# sequoiadb-rs

Rust rewrite of [SequoiaDB](https://github.com/SequoiaDB/SequoiaDB) distributed document database.

## Architecture

The project is organized as a Cargo workspace with 20 crates mirroring the original C++ engine modules:

| Crate | Description | Original Module |
|-------|-------------|-----------------|
| `sdb-bson` | BSON encode/decode | bson/ |
| `sdb-common` | Shared types, error codes, config | oss/ + pd/ + util/ |
| `sdb-msg` | Message formats & protocol | msg/ |
| `sdb-net` | Async network layer | net/ |
| `sdb-dms` | Storage engine: pages, extents | dms/ |
| `sdb-ixm` | Index management: B-tree | ixm/ |
| `sdb-dps` | WAL / transaction log | dps/ |
| `sdb-mth` | Query matcher & modifier | mth/ |
| `sdb-opt` | Query optimizer | opt/ |
| `sdb-rtn` | Query execution runtime | rtn/ |
| `sdb-cat` | Catalog service | cat/ |
| `sdb-cls` | Cluster replication & sharding | cls/ |
| `sdb-coord` | Coordinator node | coord/ |
| `sdb-auth` | Authentication & RBAC | auth/ |
| `sdb-aggr` | Aggregation pipeline | aggr/ |
| `sdb-sql` | SQL parser & QGM | sql/ + qgm/ |
| `sdb-mon` | Monitoring & diagnostics | mon/ + pd/ |
| `sdb-sched` | Task scheduling | sched/ |
| `sdb-client` | Rust client driver | — |
| `sdb-server` | Unified server binary | pmd/ |

## Build

```bash
cargo check   # verify compilation
cargo test    # run tests
cargo build   # build all crates
```

## License

AGPL-3.0
