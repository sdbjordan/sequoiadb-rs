# Storage Engine (sdb-dms)

The storage engine manages document data using a page-based architecture with 64KB pages, extent-linked record chains, and a bucketed delete list for free space reuse.

All data lives in-memory. Disk persistence is handled by the [WAL](wal-and-transactions.md).

## Page Layout

Each page is a flat 64KB buffer:

```rust
pub const PAGE_SIZE: usize = 65536; // 64 KB

pub struct Page {
    pub id: PageId,                    // u32
    pub data: Box<[u8; PAGE_SIZE]>,    // 65,536 bytes
    pub dirty: bool,
}
```

Pages are allocated sequentially and identified by `PageId` (u32). The `dirty` flag tracks modifications since last flush.

## Extent Structure

Each extent occupies exactly one page. The extent header (48 bytes) sits at the start, followed by records filling toward the end of the page.

```
┌─────────────────────────────────────────────┐
│ Extent Header (48 bytes)                     │
├─────────────────────────────────────────────┤
│ Record 1 (header 16B + payload)              │
│ Record 2 (header 16B + payload)              │
│ Record 3 ...                                 │
│                                              │
│              Free Space                      │
│                                              │
└─────────────────────────────────────────────┘
       byte 0                        byte 65535
```

### Extent Header (48 bytes)

| Offset | Size | Field | Type | Description |
|--------|------|-------|------|-------------|
| 0 | 2 | `eye_catcher` | `[u8; 2]` | Magic `[0x44, 0x45]` ("DE") |
| 2 | 2 | `block_size` | u16 LE | Always 1 (single-page extents) |
| 4 | 2 | `mb_id` | u16 LE | Collection ID |
| 6 | 1 | `flag` | u8 | 0 = normal |
| 7 | 1 | `version` | u8 | 0 initially |
| 8 | 4 | `logic_id` | i32 LE | Sequential extent ID |
| 12 | 4 | `prev_extent` | i32 LE | Previous extent page ID (-1 = none) |
| 16 | 4 | `next_extent` | i32 LE | Next extent page ID (-1 = none) |
| 20 | 4 | `rec_count` | u32 LE | Live record count |
| 24 | 4 | `first_record_offset` | i32 LE | Byte offset of first record (-1 = none) |
| 28 | 4 | `last_record_offset` | i32 LE | Byte offset of last record (-1 = none) |
| 32 | 4 | `free_space` | u32 LE | Remaining free bytes |
| 36 | 12 | `reserved` | — | Zeroed |

Initial free space = `PAGE_SIZE - EXTENT_HEADER_SIZE` = 65,488 bytes.

### Extent Chaining

Extents form a doubly-linked list within a `StorageUnit`:

```
StorageUnit
  first_extent ──▶ Extent 0 ──▶ Extent 1 ──▶ Extent 2 ──▶ ...
                   ◀──          ◀──          ◀──
                                              ◀── last_extent
```

When allocating a new extent:
1. Append a new `Page` to the pages vector
2. Initialize the extent header with `prev = last_extent`, `next = -1`
3. Update the previous last extent's `next_extent` pointer
4. Update `last_extent` to point to the new page

## Record Format

Records are stored inline after the extent header. Each has a 16-byte header:

| Offset | Size | Field | Type | Description |
|--------|------|-------|------|-------------|
| +0 | 1 | `flag` | u8 | `0x00` = normal, `0x01` = deleted |
| +1 | 3 | `pad` | — | Reserved, zeroed |
| +4 | 4 | `size` | u32 LE | Total record size (header + payload) |
| +8 | 4 | `prev_offset` | i32 LE | Previous record byte offset (-1 = first) |
| +12 | 4 | `next_offset` | i32 LE | Next record byte offset (-1 = last) |
| +16 | var | `payload` | `[u8]` | BSON document bytes |

Records within an extent are linked as a doubly-linked list via `prev_offset` / `next_offset`. The extent header's `first_record_offset` and `last_record_offset` serve as head/tail.

**Max record size**: 65,488 bytes (PAGE_SIZE - EXTENT_HEADER_SIZE).

### RecordId

A record is addressed by `(extent_id, offset)`:

```rust
pub struct RecordId {
    pub extent_id: u32,  // page ID of the extent
    pub offset: u32,     // byte offset within the page
}
```

## Insertion Strategy (3-tier)

1. **Delete list reuse**: Try `pop_fit(total_size)` to find a deleted slot of sufficient size. If found, write the record into the reused slot and re-link it into the extent's record chain.

2. **Append to last extent**: If the last extent has enough `free_space`, append at `offset = PAGE_SIZE - free_space`.

3. **Allocate new extent**: Create a fresh extent page and write the record there.

## Delete List (Free Space Reuse)

The delete list is a 10-bucket free-space manager. When a record is deleted, its slot is pushed onto the appropriate bucket based on total size:

| Bucket | Max Size |
|--------|----------|
| 0 | ≤ 64 B |
| 1 | ≤ 128 B |
| 2 | ≤ 256 B |
| 3 | ≤ 512 B |
| 4 | ≤ 1 KB |
| 5 | ≤ 2 KB |
| 6 | ≤ 4 KB |
| 7 | ≤ 8 KB |
| 8 | ≤ 16 KB |
| 9 | > 16 KB |

```rust
pub struct DeleteList {
    heads: [(i32, i32); 10],  // (extent_id, byte_offset) per bucket
}
```

**Push** (on delete): The deleted record's `prev_offset`/`next_offset` fields are repurposed as chain pointers. LIFO ordering — new deletions become the bucket head.

**Pop** (on insert): Searches from the target bucket upward (best-fit). Only checks the head of each bucket (no chain traversal in v1).

Deleted slot sizes are preserved — no splitting. Some internal fragmentation is accepted.

## Delete and Update

**Delete**: Unlink from record chain → decrement `rec_count` → push onto delete list (flag = `0x01`).

**Update**: Delete-then-insert. The old slot is freed and may be reused if the new document fits in the same size bucket.

## Scan

Full scan traverses the extent chain from `first_extent`, following `next_extent` until -1. Within each extent, follows the record chain from `first_record_offset` via `next_offset`. Skips deleted records.

## StorageEngine Trait

```rust
pub trait StorageEngine: Send + Sync {
    fn insert(&self, doc: &Document) -> Result<RecordId>;
    fn find(&self, rid: RecordId) -> Result<Document>;
    fn delete(&self, rid: RecordId) -> Result<()>;
    fn update(&self, rid: RecordId, doc: &Document) -> Result<RecordId>;
    fn scan(&self) -> Vec<(RecordId, Document)>;
}
```

`StorageUnit` implements this trait. All methods acquire a `Mutex<Inner>` lock for thread safety.

## V1 Limitations

- Delete list `pop_fit` only checks bucket heads, not the full chain
- Deleted slot reuse preserves original size (no splitting)
- All data is in-memory — the WAL provides durability
- Single-page extents only (no multi-page records)
