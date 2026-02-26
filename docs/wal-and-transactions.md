# Write-Ahead Log & Transactions (sdb-dps)

The WAL provides crash recovery by logging all mutations before applying them. The transaction manager provides per-connection buffered transactions with atomic commit.

## WAL File Format

The WAL is stored as a single file `wal.sdb` within the configured WAL directory.

### File Header (32 bytes)

| Offset | Size | Field | Type | Description |
|--------|------|-------|------|-------------|
| 0 | 2 | magic | `[u8; 2]` | `[0x57, 0x4C]` ("WL") |
| 2 | 1 | version | u8 | Always 1 |
| 3 | 1 | — | — | Padding |
| 4 | 4 | header_size | u32 LE | 32 |
| 8 | 8 | first_lsn | u64 LE | LSN of first record (= 32) |
| 16 | 8 | record_count | u64 LE | Total records in file |
| 24 | 8 | — | — | Reserved |

### Frame Layout (variable size)

Each log record is stored as a frame. Frames are concatenated sequentially after the file header.

```
┌──────────────────────────────────────────────────────┐
│ [0xD1 0x05]  frame_magic (2B)                         │
│ frame_len    (4B LE) — total frame size               │
│ lsn          (8B LE) — log sequence number            │
│ prev_lsn     (8B LE) — previous record's LSN          │
│ txn_id       (8B LE) — transaction ID                 │
│ op           (1B)    — operation type (0-9)            │
│ data_len     (4B LE) — payload length                 │
│ data         (var)   — payload bytes                  │
│ crc32        (4B LE) — CRC32 of all preceding bytes   │
└──────────────────────────────────────────────────────┘
```

**Frame header size**: 35 bytes (magic + frame_len + lsn + prev_lsn + txn_id + op + data_len).

**Minimum frame size**: 39 bytes (35 header + 0 data + 4 CRC).

### LSN Scheme

`Lsn = u64` — the **byte offset** of the frame within the WAL file.

- First record: LSN = 32 (FILE_HEADER_SIZE)
- Each subsequent: LSN = previous LSN + previous frame_len
- `prev_lsn` chains records backward (first record has `prev_lsn = 0`)

### CRC32 Checksum

Standard CRC32 IEEE 802.3 (polynomial `0xEDB88320`, reflected). Covers all frame bytes before the CRC field itself.

## Log Operations

```rust
pub enum LogOp {
    Insert,           // 0
    Update,           // 1
    Delete,           // 2
    TxnBegin,         // 3
    TxnCommit,        // 4
    TxnAbort,         // 5
    CollectionCreate, // 6
    CollectionDrop,   // 7
    IndexCreate,      // 8
    IndexDrop,        // 9
}
```

### WAL Payload Format (BSON)

Each operation's `data` field contains a BSON document:

| Operation | BSON Payload |
|-----------|-------------|
| Insert | `{ c: "cs.cl", op: "I", doc: <document> }` |
| Update | `{ c: "cs.cl", op: "U", old: <old_doc>, new: <new_doc> }` |
| Delete | `{ c: "cs.cl", op: "D", doc: <document> }` |
| CreateCS | `{ op: "CCS", name: "csname" }` |
| DropCS | `{ op: "DCS", name: "csname" }` |
| CreateCL | `{ op: "CCL", name: "cs.cl" }` |
| DropCL | `{ op: "DCL", name: "cs.cl" }` |
| CreateIndex | `{ op: "CIX", c: "cs.cl", name: "idx", key: <pattern>, unique: bool }` |
| DropIndex | `{ op: "DIX", c: "cs.cl", name: "idx" }` |

## WAL Operations

### Write Path

```rust
pub struct WriteAheadLog {
    log_path: String,
    current_lsn: Lsn,      // next available LSN
    flushed_lsn: Lsn,      // LSN up to which data is on disk
    file: File,
    write_buffer: Vec<u8>, // in-memory unflushed frames
    file_end_offset: u64,  // physical end of flushed data
    record_count: u64,
    prev_lsn: Lsn,         // LSN of most recent record
}
```

**Append**: Encodes the frame, appends to `write_buffer`. Does NOT write to disk. Returns assigned LSN.

**Flush**: Writes entire buffer to disk at `file_end_offset` via `pwrite`, clears buffer, updates file header, calls `fsync`.

The write path in the server handler is: **append to WAL → flush → apply to catalog → replicate to secondaries**.

### Read Path

- If `lsn >= file_end_offset`: read from `write_buffer` (unflushed data)
- Otherwise: `pread` from file at the given offset

### Recovery

`recover()` scans all flushed frames from byte 32 to `file_end_offset`:

1. Read 6 bytes: check frame magic, get `frame_len`
2. Read full frame, decode, validate CRC
3. Stop at first invalid/incomplete frame
4. Truncate any trailing garbage
5. Return all valid records in order

On server startup, `recover_from_wal()` replays all records to reconstruct the in-memory `CatalogManager`:
- DDL records (CCS, DCS, CCL, DCL, CIX, DIX) → create/drop collection spaces, collections, indexes
- DML records (I, U, D) → insert, update, delete documents
- Transaction records (TxnBegin, TxnCommit, TxnAbort) → tracked for transactional consistency

### Scan Iterator

`scan_from(from_lsn)` creates an independent `WalIterator`:

```rust
pub struct WalIterator {
    reader: BufReader<File>,  // own file handle, no WAL lock needed
    offset: u64,
    file_len: u64,
}
```

Implements `Iterator<Item = Result<LogRecord>>`. Used by replication (primary reads WAL frames without holding the WAL mutex) and oplog tailing cursors.

## Transaction Manager

```rust
pub struct TransactionManager {
    active_txns: HashMap<u64, TxnState>,
    next_txn_id: u64,  // starts at 1
}

pub enum TxnState { Active, Committed, Aborted }
```

### Per-Connection Transaction Model

The server implements per-connection buffered transactions:

1. **Begin**: Allocate `txn_id`, create `TxnBuffer` for the connection
2. **During transaction**: DML operations (Insert/Update/Delete) are buffered, not applied
3. **Commit**: Apply all buffered operations atomically:
   - WAL: TxnBegin → all DML records → TxnCommit
   - Apply to catalog in order
   - Mark committed in TransactionManager
4. **Rollback**: Discard buffer, mark aborted
5. **Auto-rollback on disconnect**: Any active transaction is automatically aborted

### Distributed 2PC (Coordinator)

The coordinator extends this with two-phase commit across data groups:

1. **Phase 1**: Send `TransBeginReq` to all data nodes, record per-group txn IDs
2. **During transaction**: Route DML to appropriate groups using the held connection
3. **Phase 2 (Commit)**: Send `TransCommitReq` to each group sequentially. On failure, rollback uncommitted groups.
4. **Rollback**: Send `TransRollbackReq` to all groups

## V1 Limitations

- Single WAL file (no rotation or compaction)
- No MVCC or snapshot isolation — transactions use simple buffering
- No undo/redo — recovery replays the full WAL
- Transaction manager tracks state only, no conflict detection
