# Wire Protocol (sdb-msg)

SequoiaDB-RS uses a binary TCP protocol compatible with the original SequoiaDB C++ engine. All messages share a common 52-byte header followed by operation-specific payloads. All multi-byte values are **little-endian**.

## Message Header (52 bytes)

Every message starts with `MsgHeader`:

| Offset | Size | Field | Type | Description |
|--------|------|-------|------|-------------|
| 0 | 4 | `msg_len` | i32 | Total message length (including this field) |
| 4 | 4 | `eye` | i32 | Eye catcher: `0x0000EEEE` |
| 8 | 4 | `tid` | u32 | Thread/task ID |
| 12 | 8 | `route_id` | u64 | Routing info (see MsgRouteID) |
| 20 | 8 | `request_id` | u64 | Caller-assigned request identifier |
| 28 | 4 | `opcode` | i32 | Operation code (see OpCodes) |
| 32 | 2 | `version` | i16 | Protocol version (default: 1) |
| 34 | 2 | `flags` | i16 | Message flags |
| 36 | 8 | `query_id` | `[u8; 8]` | Global query ID |
| 44 | 4 | `query_op_id` | u32 | Query operation ID |
| 48 | 4 | `reserve` | `[u8; 4]` | Reserved |

### MsgRouteID (8 bytes)

The 64-bit `route_id` encodes three identifiers:

```
┌───────────────┬───────────┬───────────┐
│ service_id    │ node_id   │ group_id  │
│ bits [63:48]  │ bits[47:32]│ bits[31:0]│
│ u16           │ u16       │ u32       │
└───────────────┴───────────┴───────────┘
```

## OpCodes

| OpCode | Value | Category | Description |
|--------|-------|----------|-------------|
| `MsgReq` | 1000 | General | Generic message |
| `UpdateReq` | 2001 | DML | Update documents |
| `InsertReq` | 2002 | DML | Insert documents |
| `SqlReq` | 2003 | SQL | Execute SQL statement |
| `QueryReq` | 2004 | Query/DDL | Query or `$command` |
| `GetMoreReq` | 2005 | Cursor | Get next batch |
| `DeleteReq` | 2006 | DML | Delete documents |
| `KillContextReq` | 2007 | Cursor | Close cursor(s) |
| `Disconnect` | 2008 | Session | Close connection |
| `TransBeginReq` | 2010 | Transaction | Begin transaction |
| `TransCommitReq` | 2011 | Transaction | Commit transaction |
| `TransRollbackReq` | 2012 | Transaction | Rollback transaction |
| `AggregateReq` | 2019 | Aggregation | Run pipeline |
| `CatalogReq` | 3001 | Internal | Request catalog metadata |
| `CatalogReply` | 3002 | Internal | Catalog metadata response |
| `ReplSync` | 4001 | Replication | Push WAL frames |
| `ReplConsistency` | 4002 | Replication | Reserved |
| `ReplVoteReq` | 4003 | Election | Vote request |
| `ReplVoteReply` | 4004 | Election | Vote response |
| `ReplHeartbeat` | 4005 | Election | Primary heartbeat |
| `ReplMemberChange` | 4006 | Membership | Dynamic membership |
| `ChunkMigrateData` | 4007 | Sharding | Chunk migration data |

### Reply Convention

Reply opcode = request opcode | `0x40000000` (REPLY_MASK, bit 30).

`OpCode::from_i32()` strips the reply bit before matching, so it handles both request and reply opcodes transparently.

## Message Types

### MsgOpQuery (opcode 2004)

Used for both data queries and `$command` operations (DDL, snapshots, etc.).

After header:

| Field | Size | Type | Notes |
|-------|------|------|-------|
| `version` | 4 | i32 | |
| `w` | 2 | i16 | Write concern |
| padding | 2 | — | Zeroed |
| `flags` | 4 | i32 | |
| `name_length` | 4 | i32 | Including null terminator |
| `num_to_skip` | 8 | i64 | |
| `num_to_return` | 8 | i64 | |
| `name` | var | C string | 4-byte aligned, null-terminated |
| `condition` | var | BSON doc | Optional (empty doc if absent) |
| `selector` | var | BSON doc | Optional |
| `order_by` | var | BSON doc | Optional |
| `hint` | var | BSON doc | Optional |

Commands use `$command` as the `name` and the command name in the `condition` document. For example, `$create collectionspace` with `{ Name: "mycs" }`.

### MsgOpInsert (opcode 2002)

| Field | Size | Type | Notes |
|-------|------|------|-------|
| `version` | 4 | i32 | |
| `w` | 2 | i16 | |
| padding | 2 | — | |
| `flags` | 4 | i32 | |
| `name_length` | 4 | i32 | |
| `name` | var | C string | Collection name, 4-byte aligned |
| `docs` | var | BSON[] | Concatenated BSON documents |

Supports batch insert — multiple documents are concatenated sequentially.

### MsgOpUpdate (opcode 2001)

| Field | Size | Type | Notes |
|-------|------|------|-------|
| `version` | 4 | i32 | |
| `w` | 2 | i16 | |
| padding | 2 | — | |
| `flags` | 4 | i32 | |
| `name_length` | 4 | i32 | |
| `name` | var | C string | 4-byte aligned |
| `condition` | var | BSON doc | Match filter |
| `modifier` | var | BSON doc | Update rule (`$set`, `$inc`, etc.) |
| `hint` | var | BSON doc | Optional |

### MsgOpDelete (opcode 2006)

| Field | Size | Type | Notes |
|-------|------|------|-------|
| `version` | 4 | i32 | |
| `w` | 2 | i16 | |
| padding | 2 | — | |
| `flags` | 4 | i32 | |
| `name_length` | 4 | i32 | |
| `name` | var | C string | 4-byte aligned |
| `condition` | var | BSON doc | Match filter |
| `hint` | var | BSON doc | Optional |

### MsgOpGetMore (opcode 2005)

The simplest message type:

| Field | Size | Type |
|-------|------|------|
| `context_id` | 8 | i64 |
| `num_to_return` | 4 | i32 |

### MsgOpKillContexts (opcode 2007)

| Field | Size | Type |
|-------|------|------|
| zero | 4 | i32 (always 0) |
| `num_contexts` | 4 | i32 |
| `context_ids` | 8×N | i64[] |

### MsgOpReply

Reply to any request. After header (28 fixed bytes):

| Field | Size | Type | Description |
|-------|------|------|-------------|
| `context_id` | 8 | i64 | Cursor ID (-1 = no cursor) |
| `flags` | 4 | i32 | 0 = success, negative = error code |
| `start_from` | 4 | i32 | Starting index |
| `num_returned` | 4 | i32 | Number of documents |
| `return_mask` | 4 | i32 | Bitmask |
| `data_len` | 4 | i32 | Total BSON data length |
| docs | var | BSON[] | Concatenated result documents |

### Error Codes in Reply

| Error | Flag Value |
|-------|------------|
| Success | 0 |
| System error | -1 |
| Out of memory | -2 |
| Invalid argument | -6 |
| Permission denied | -7 |
| End of file | -9 |
| Network error | -15 |
| Network close | -16 |
| Timeout | -17 |
| Collection already exists | -22 |
| Collection not found | -23 |
| Not found | -29 |
| Query not found | -31 |
| CS already exists | -33 |
| CS not found | -34 |
| Duplicate key | -38 |
| Index already exists | -46 |
| Index not found | -47 |
| Auth failed | -179 |

## Message Framing

TCP connection framing:

1. **Receive**: Read 4 bytes → `msg_len` (i32 LE). Read `msg_len - 4` more bytes. Decode header from first 52 bytes.
2. **Send**: Encode full message (header + payload). Write all bytes. Flush.

All strings in messages are null-terminated C strings. After each string, the buffer is padded to 4-byte alignment. Optional BSON documents encode as empty documents (5 bytes: `[5, 0, 0, 0, 0]`) when absent.

## Alignment Rules

- `pad_align4(buf)`: Pad buffer with zeros to next 4-byte boundary
- `skip_align4(buf, offset)`: Advance read offset to next 4-byte boundary
- Collection names and other C strings are always 4-byte aligned after the null terminator
