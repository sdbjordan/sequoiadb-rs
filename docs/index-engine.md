# Index Engine (sdb-ixm)

The index engine implements a B+ tree with 64KB pages, a cell-pointer layout inspired by SQLite, and support for compound indexes with directional sorting.

## B+ Tree Node Layout

Each node occupies a 64KB page with three regions:

```
┌──────────────────────────────────────────────┐
│ Node Header (32 bytes)                        │
├──────────────────────────────────────────────┤
│ Cell Pointer Array (grows →)                  │
│   [offset₀] [offset₁] [offset₂] ...          │
│                                               │
│              Free Space (gap)                 │
│                                               │
│                          ... [Cell₂] [Cell₁] [Cell₀] │
│ Cell Content Area (grows ←)                   │
└──────────────────────────────────────────────┘
byte 0                                    byte 65535
```

The cell pointer array grows forward from byte 32. Cell content grows backward from byte 65535. When the two regions meet, the node is full and must be split.

### Node Header (32 bytes)

| Offset | Size | Field | Type | Description |
|--------|------|-------|------|-------------|
| 0 | 2 | `eye` | `[u8; 2]` | Magic `['I', 'X']` (0x49, 0x58) |
| 2 | 1 | `node_type` | u8 | 0 = internal, 1 = leaf |
| 3 | 1 | `flag` | u8 | Reserved |
| 4 | 2 | `key_count` | u16 LE | Number of keys |
| 6 | 2 | — | — | Padding |
| 8 | 4 | `parent` | i32 LE | Parent page ID (-1 = root) |
| 12 | 4 | `right_sibling` | i32 LE | Right sibling page ID (-1 = none) |
| 16 | 4 | `right_child` | u32 LE | Rightmost child (internal only) |
| 20 | 4 | `free_end` | u32 LE | Start of cell content area |
| 24 | 8 | — | — | Reserved |

### Cell Pointer Array

Starting at byte 32, an array of `u16` offsets (2 bytes each), one per key. Each pointer stores the byte offset of its cell within the page. The array is maintained in sorted key order, enabling binary search.

**Available space** = `free_end - (32 + key_count × 2)`

### Cell Formats

**Leaf cell** — stores key + record pointer:
```
[key_len: u16] [key_bytes: var] [extent_id: u32] [offset: u32]
```
Total: `2 + key_bytes.len() + 8` bytes

**Internal cell** — stores left child + separator key:
```
[left_child: u32] [key_len: u16] [key_bytes: var]
```
Total: `4 + 2 + key_bytes.len()` bytes

## Index Key Format

Keys support compound fields with type-aware binary encoding:

```rust
pub struct IndexKey {
    pub fields: Vec<Value>,
}
```

**Binary layout**: `[field_count: u16 LE]` followed by per-field `[type_tag: u8][data]`:

| Tag | Type | Encoded Size |
|-----|------|-------------|
| 0 | Null | 1 byte (tag only) |
| 1 | MinKey | 1 byte |
| 2 | MaxKey | 1 byte |
| 3 | Boolean | 2 bytes |
| 4 | Int32 | 5 bytes |
| 5 | Int64 | 9 bytes |
| 6 | Double | 9 bytes |
| 7 | String | 5 + len bytes |

### Comparison Order

BSON comparison order (MongoDB-compatible):

```
MinKey < Null < Numbers < String < Boolean < MaxKey
```

Numbers (Int32, Int64, Double) cross-compare by promoting to f64. Strings compare lexicographically. Compound keys compare field-by-field with direction awareness (ascending or descending per field).

## Index Definition

```rust
pub struct IndexDefinition {
    pub name: String,
    pub key_pattern: Document,  // e.g., {"age": 1, "name": -1}
    pub unique: bool,
    pub enforced: bool,
    pub not_null: bool,
}
```

`key_pattern` values specify direction: positive = ascending, negative = descending.

## Operations

### Lookup

1. Walk from root to leaf via `find_leaf(key)`, binary searching at each internal node
2. At the leaf, binary search for the key
3. Scan forward (following `right_sibling` links) to collect all matching records (handles duplicates)

### Insert

1. If `unique`, check for existing key first
2. Navigate to target leaf via `find_leaf(key)`
3. If space available: write cell, insert pointer at sorted position
4. If full: trigger leaf split

### Leaf Split

1. Collect all entries + new entry, sort
2. Split at midpoint (`entries.len() / 2`)
3. Left half stays in original page, right half goes to new page
4. Update `right_sibling` linked list
5. Promote first key of right page as separator to parent
6. Recursively insert separator into parent (may trigger internal split)

### Internal Split

1. Collect all entries + new separator, sort
2. Split at midpoint — key at split point is **promoted** to parent (not kept in either child)
3. Left and right pages get their respective halves
4. If the split node was root, create a new root with the promoted key

### Delete

1. Find target leaf, binary search for matching key + RecordId
2. Remove cell pointer (shift subsequent pointers left)
3. Decrement `key_count`
4. No merge/rebalance in v1 (dead space is accepted)

### Range Scan

1. Find starting leaf and position (or leftmost leaf if unbounded)
2. Iterate forward through cells, respecting start/end bounds and inclusivity
3. Follow `right_sibling` links to cross leaf boundaries
4. Collect matching RecordIds into `IndexCursor`

## Index Cursor

```rust
pub struct IndexCursor {
    results: Vec<RecordId>,
    pos: usize,
}
```

Fully materialized — results are collected eagerly during scan, then iterated via `next()` / `has_more()`.

## Index Trait

```rust
pub trait Index: Send + Sync {
    fn insert(&mut self, key: &IndexKey, rid: RecordId) -> Result<()>;
    fn delete(&mut self, key: &IndexKey, rid: RecordId) -> Result<()>;
    fn find(&self, key: &IndexKey) -> Result<Option<RecordId>>;
    fn scan(&self, range: &KeyRange) -> Result<IndexCursor>;
}
```

## V1 Limitations

- Deleted cell space is not reclaimed (dead space accumulates)
- No merge/rebalance on underflow
- Index cursor is fully materialized (eager collection, not lazy)
- All index pages are in-memory
