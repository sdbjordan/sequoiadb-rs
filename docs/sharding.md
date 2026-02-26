# Sharding

The coordinator routes documents to data groups based on a shard key. Two sharding strategies are supported: hash-based (default) and range-based.

## Architecture

```
Client
  │
  ▼
Coordinator (CoordNodeHandler)
  │
  ├── ShardManager per collection
  │     ├── shard_key: "user_id"
  │     ├── strategy: hash or range
  │     └── chunks: tracking doc distribution
  │
  ├──▶ Data Group 1 (hash % 3 == 0, or range [0, 100))
  ├──▶ Data Group 2 (hash % 3 == 1, or range [100, 200))
  └──▶ Data Group 3 (hash % 3 == 2, or range [200, ∞))
```

## Hash Sharding

### Enabling

```
$enable sharding { Collection: "cs.cl", ShardKey: "user_id", NumGroups: 3 }
```

### Hash Function

FNV-style hash, modulo number of groups:

```rust
fn simple_hash(val: &Value) -> u32 {
    match val {
        Value::Int32(n) => *n as u32,
        Value::Int64(n) => *n as u32,
        Value::String(s) => {
            let mut h: u32 = 0;
            for b in s.bytes() {
                h = h.wrapping_mul(31).wrapping_add(b as u32);
            }
            h
        }
        Value::Double(f) => (*f as i64) as u32,
        _ => 0,
    }
}

// Target group = (hash(shard_key_value) % num_groups) + 1
```

Group IDs are 1-indexed (1, 2, 3, ...).

## Range Sharding

### Enabling

```
$enable range sharding { Collection: "cs.cl", ShardKey: "age" }
```

Then add ranges:

```
$add range { Collection: "cs.cl", GroupId: 1, LowBound: null, UpBound: 30 }
$add range { Collection: "cs.cl", GroupId: 2, LowBound: 30, UpBound: 60 }
$add range { Collection: "cs.cl", GroupId: 3, LowBound: 60, UpBound: null }
```

### Range Structure

```rust
pub struct ShardRange {
    pub group_id: GroupId,
    pub low_bound: Option<Value>,  // inclusive (None = -∞)
    pub up_bound: Option<Value>,   // exclusive (None = +∞)
}
```

**Routing**: Iterate ranges, find the first where `low_bound <= value < up_bound`. Falls back to group 1 if no range matches.

## Query Routing

| Scenario | Routing |
|----------|---------|
| Insert with shard key | Hash/range → single group |
| Query with exact shard key match in condition | Single group |
| Query without shard key | Broadcast to all groups (scatter-gather) |
| Update/Delete with shard key | Single group |
| Update/Delete without shard key | Broadcast to all groups |

### Scatter-Gather Flow

1. Coordinator determines target groups from the router
2. Sends query to each group's data node (via connection pool)
3. Collects results from all groups
4. Merges into a single result set
5. Creates server-side cursor if results exceed batch size (100 docs)

## Chunk Management

Each sharded collection tracks document distribution via chunks:

```rust
pub struct ChunkInfo {
    pub chunk_id: u32,
    pub group_id: GroupId,
    pub low_bound: Option<Value>,
    pub up_bound: Option<Value>,
    pub doc_count: u64,
    pub migrating: bool,
}
```

### Operations

| Command | Description |
|---------|-------------|
| `$get chunk info` | List all chunks with doc counts |
| `$split chunk` | Move N documents from source group to target group |
| `$migrate chunk` | Transfer actual document data between groups |
| `$balance` | Auto-detect and fix imbalance |

### Auto-Balance

`find_imbalance()` triggers rebalance when:
- Difference between heaviest and lightest group > 20% of max
- AND the difference > 10 documents

Moves `diff / 2` documents from the heaviest to the lightest group.

### Migration Flow

1. `$split chunk`: Update chunk doc counts (source -= N, target += N)
2. `$migrate chunk`:
   - Query source group for N documents
   - Send via `ChunkMigrateData` OpCode (4007) to target group
   - Delete migrated documents from source
   - Rollback on failure (re-insert to source, delete from target)

## Shard Info Query

```
$get shard info { Collection: "cs.cl" }
```

Returns:
```json
{
  "Sharded": true,
  "ShardKey": "user_id",
  "NumGroups": 3,
  "ShardType": "hash"  // or "range"
}
```

## Config Persistence

Shard configurations are persisted to `{db_path}/coord_state.json`:

```json
[
  {
    "collection": "cs.cl",
    "shard_key": "user_id",
    "num_groups": 3,
    "shard_type": "hash",
    "ranges": []
  },
  {
    "collection": "cs.cl2",
    "shard_key": "age",
    "num_groups": 3,
    "shard_type": "range",
    "ranges": [
      { "group_id": 1, "low_bound": null, "up_bound": 30 },
      { "group_id": 2, "low_bound": 30, "up_bound": 60 },
      { "group_id": 3, "low_bound": 60, "up_bound": null }
    ]
  }
]
```

Configuration is saved after every `$enable sharding`, `$enable range sharding`, and `$add range` command. On coordinator startup, configs are loaded and shard managers are reconstructed.
