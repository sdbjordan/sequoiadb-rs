# sequoiadb-rs

Rust rewrite of [SequoiaDB](https://github.com/SequoiaDB/SequoiaDB) distributed document database. ~25,000 lines of Rust across 20 crates, with 80 integration tests and 7 benchmark suites.

## Features

- **Document Store** — BSON documents with page-level storage engine (64KB pages, extent chains, bucketed delete list)
- **B+ Tree Indexes** — Compound indexes with directional sorting, unique constraints, cell-pointer page layout
- **Query Engine** — Cost-based optimizer with index selection, aggregation pipeline ($match/$group/$sort/$unwind), SQL parser
- **Write-Ahead Log** — CRC32-checksummed binary WAL with crash recovery and full state reconstruction
- **Transactions** — Per-connection buffered transactions with atomic commit, auto-rollback on disconnect, distributed 2PC via coordinator
- **Replication** — Raft-inspired election (term voting, log-completeness, majority quorum), push-based WAL replication with majority ack
- **Sharding** — Hash and range sharding via coordinator, scatter-gather queries, chunk management with auto-balance
- **Authentication** — RBAC with bootstrap mode (no auth until first user created)
- **Monitoring** — Atomic counters for insert/query/update/delete, snapshot commands for database/sessions/collections/health
- **Oplog Tailing** — `$watch` command with WAL-following cursors, collection filtering, resume from LSN
- **Wire Protocol** — SequoiaDB-compatible binary protocol (52-byte MsgHeader, 23 OpCodes)
- **Client Driver** — Connection pool with health checks, auto-auth, batch insert, cursor batching

## Quick Start

```bash
# Build
cargo build --release

# Run a single data node
mkdir -p /tmp/sdb-data
cargo run --release -- -r data -p 11810 -d /tmp/sdb-data

# Run tests
cargo test --workspace
```

See [docs/quickstart.md](docs/quickstart.md) for multi-node deployment and CRUD examples.

## Architecture

```
Clients ──▶ Coordinator (shard routing) ──▶ Data Groups (Primary + Secondaries)
                                                    │
                                               Catalog Node (cluster metadata)
```

Three node roles from a single binary: `--role data`, `--role coord`, `--role catalog`.

20 crates organized in 6 dependency layers:

| Layer | Crates | Purpose |
|-------|--------|---------|
| 0 | `sdb-bson`, `sdb-common` | BSON codec, shared types/errors |
| 1 | `sdb-msg` | Wire protocol (52B header, OpCodes) |
| 2 | `sdb-net`, `sdb-dms`, `sdb-dps`, `sdb-ixm`, `sdb-mth` | Network, storage, WAL, indexes, matcher |
| 3 | `sdb-opt`, `sdb-rtn`, `sdb-auth`, `sdb-aggr`, `sdb-sql`, `sdb-mon`, `sdb-sched` | Optimizer, runtime, auth, aggregation, SQL, monitoring |
| 4 | `sdb-cat`, `sdb-cls`, `sdb-coord` | Catalog, cluster (election/replication/sharding), coordinator |
| 5 | `sdb-client`, `sdb-server` | Client driver, server binary |

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture Overview](docs/architecture.md) | System design, node roles, data flow, crate layers |
| [Storage Engine](docs/storage-engine.md) | Page layout, extent chains, record format, delete list |
| [Index Engine](docs/index-engine.md) | B+ tree structure, cell-pointer layout, split/merge |
| [Query Engine](docs/query-engine.md) | Matcher operators, cost model, optimizer, executor, aggregation, SQL |
| [WAL & Transactions](docs/wal-and-transactions.md) | WAL file format, LSN scheme, recovery, transaction model |
| [Replication](docs/replication.md) | Raft-inspired election, WAL push, majority ack, backoff |
| [Sharding](docs/sharding.md) | Hash/range routing, chunk management, auto-balance |
| [Wire Protocol](docs/wire-protocol.md) | MsgHeader layout, all OpCodes, message formats |
| [Authentication](docs/authentication.md) | User model, RBAC, bootstrap mode |
| [Configuration](docs/configuration.md) | CLI flags, TOML config, all parameters with defaults |
| [Quickstart](docs/quickstart.md) | Build, deploy, CRUD examples |

## Build

```bash
cargo check          # verify compilation
cargo test           # run all tests (80 integration + unit)
cargo build          # build all crates
cargo bench          # run benchmarks
cargo clippy         # lint
```

## License

AGPL-3.0
