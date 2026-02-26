# Architecture Overview

SequoiaDB-RS is a Rust reimplementation of [SequoiaDB](https://github.com/SequoiaDB/SequoiaDB), a distributed document database. The system supports sharding, replication, ACID transactions, and a SequoiaDB-compatible wire protocol.

## System Architecture

```
                    ┌─────────────────────┐
                    │      Clients        │
                    │  (sdb-client / TCP)  │
                    └────────┬────────────┘
                             │ SequoiaDB Wire Protocol (port 11810)
                             ▼
                    ┌─────────────────────┐
                    │   Coordinator Node  │  ← Shard routing, scatter-gather
                    │  (CoordNodeHandler) │
                    └──┬──────┬──────┬────┘
                       │      │      │
            ┌──────────┘      │      └──────────┐
            ▼                 ▼                  ▼
   ┌────────────────┐ ┌────────────────┐ ┌────────────────┐
   │  Data Group 1  │ │  Data Group 2  │ │  Data Group 3  │
   │  ┌──────────┐  │ │  ┌──────────┐  │ │  ┌──────────┐  │
   │  │ Primary  │  │ │  │ Primary  │  │ │  │ Primary  │  │
   │  └──────────┘  │ │  └──────────┘  │ │  └──────────┘  │
   │  ┌──────────┐  │ │  ┌──────────┐  │ │  ┌──────────┐  │
   │  │Secondary │  │ │  │Secondary │  │ │  │Secondary │  │
   │  └──────────┘  │ │  └──────────┘  │ │  └──────────┘  │
   └────────────────┘ └────────────────┘ └────────────────┘
            │
            ▼
   ┌─────────────────────┐
   │    Catalog Node     │  ← Cluster metadata (CS/CL/Group/Shard)
   │ (CatalogNodeHandler)│
   └─────────────────────┘
```

## Node Roles

| Role | Binary Flag | Default Port | Purpose |
|------|-------------|--------------|---------|
| **Data** | `--role data` | 11810 | Stores documents, executes queries, manages WAL |
| **Coordinator** | `--role coord` | 11810 | Routes queries to data groups, manages sharding |
| **Catalog** | `--role catalog` | 11810 | Persists cluster metadata (JSON-backed) |

All three roles run from the same `sdb-server` binary, differentiated by the `--role` flag.

## Crate Dependency Layers

The project is organized as a Cargo workspace with 20 crates in 6 dependency layers:

```
Layer 5 (Application)
  sdb-server ─── Unified server binary (Data/Coord/Catalog handlers)
  sdb-client ─── Rust client driver (connection pool, auto-auth)

Layer 4 (Distributed)
  sdb-cat ────── Catalog manager (CS/CL/Index metadata + runtime)
  sdb-cls ────── Cluster services (election, replication, sharding)
  sdb-coord ──── Coordinator router (shard routing, scatter-gather)

Layer 3 (Query Processing)
  sdb-opt ────── Cost-based query optimizer
  sdb-rtn ────── Query execution runtime
  sdb-auth ───── Authentication and RBAC
  sdb-aggr ───── Aggregation pipeline ($match, $group, $sort, ...)
  sdb-sql ────── SQL parser (6 statements) + Query Graph Model
  sdb-mon ────── Monitoring metrics and snapshots
  sdb-sched ──── Task scheduler

Layer 2 (Storage & Matching)
  sdb-dms ────── Page-level storage engine (extents, records, delete list)
  sdb-dps ────── Write-ahead log + transaction manager
  sdb-ixm ────── B+ tree index engine (64KB pages, cell-pointer layout)
  sdb-mth ────── Query matcher ($gt, $in, $and, ...) + document modifier ($set, $inc)
  sdb-net ────── Async TCP server (NetFrame) with optional TLS

Layer 1 (Protocol)
  sdb-msg ────── Wire protocol: 52-byte MsgHeader, OpCodes, message codec

Layer 0 (Foundation)
  sdb-bson ───── BSON encode/decode (21 type tags, SequoiaDB variant)
  sdb-common ─── Shared types (RecordId, NodeAddress, SdbError, NodeConfig)
```

## Data Flow

### Write Path (Insert)

```
Client ──InsertReq──▶ Coordinator
                         │
                    Route by shard key
                         │
                         ▼
                    Data Node (Primary)
                         │
                    ┌────┴────┐
                    │  Auth   │  Check session authenticated
                    ├─────────┤
                    │  WAL    │  Append LogRecord (op=Insert)
                    ├─────────┤
                    │  Flush  │  fsync to disk
                    ├─────────┤
                    │ Catalog │  Insert into StorageUnit + update indexes
                    ├─────────┤
                    │  Repl   │  Push WAL frames to secondaries (majority ack)
                    └─────────┘
```

### Read Path (Query)

```
Client ──QueryReq──▶ Coordinator
                         │
                    Determine target groups
                    (shard key → 1 group, otherwise → all)
                         │
                    ┌────┴────┐
                    │ Scatter │  Send query to each group
                    ├─────────┤
                    │ Gather  │  Merge results
                    ├─────────┤
                    │ Cursor  │  Batch (100 docs) + GetMore
                    └─────────┘

Data Node (per group):
  QueryReq → Parse condition → Optimizer (choose index vs table scan)
           → Executor (scan → filter → sort → skip → limit → project)
           → Reply with documents
```

## Key Design Decisions

1. **Single binary, multi-role**: One `sdb-server` binary serves all node roles, selected at startup via `--role`. Simplifies deployment and testing.

2. **In-memory storage + WAL persistence**: Document data lives in-memory (`StorageUnit` pages). Durability is provided by the WAL — on startup, the full WAL is replayed to reconstruct the in-memory state.

3. **Push-based WAL replication**: The primary actively pushes WAL frames to secondaries via TCP (not pull-based). This reduces replication lag and simplifies secondary implementation.

4. **Same-port protocol multiplexing**: Replication, election, and client traffic share port 11810, differentiated by OpCode ranges (2000s for client, 4000s for internal).

5. **Raft-inspired election**: Term-based voting with log-completeness check and majority quorum. Heartbeat timeout with per-node jitter (3s + node_id × 200ms) to reduce split votes.

6. **Per-connection transaction buffering**: DML operations within a transaction are buffered in memory and applied atomically on commit. Auto-rollback on disconnect.

7. **Bootstrap authentication**: No auth required until the first user is created, allowing initial setup without credentials.

## Codebase Statistics

| Metric | Value |
|--------|-------|
| Total Rust LOC | ~25,200 |
| Crate count | 20 |
| Integration tests | 80 |
| Benchmark suites | 7 |

## Related Documentation

- [Storage Engine](storage-engine.md) — Page layout, extent chains, record format, delete list
- [Index Engine](index-engine.md) — B+ tree structure, cell-pointer layout, split/merge
- [Query Engine](query-engine.md) — Matcher, optimizer, executor pipeline
- [WAL & Transactions](wal-and-transactions.md) — WAL file format, recovery, transaction model
- [Replication](replication.md) — Election protocol, WAL push, majority ack
- [Sharding](sharding.md) — Hash and range routing, chunk management
- [Wire Protocol](wire-protocol.md) — MsgHeader, OpCodes, message formats
- [Authentication](authentication.md) — User model, RBAC, bootstrap mode
- [Configuration](configuration.md) — CLI flags, TOML config, all parameters
- [Quickstart](quickstart.md) — Build, run, and perform CRUD operations
