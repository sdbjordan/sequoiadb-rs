//! On-page B+ tree node layout functions.
//!
//! Page layout (64KB):
//! ```text
//! Header (32 bytes):
//!   [0..2]    eye: ['I', 'X']
//!   [2]       node_type: u8  (0=internal, 1=leaf)
//!   [3]       flag: u8
//!   [4..6]    key_count: u16
//!   [6..8]    reserved
//!   [8..12]   parent: i32  (-1 = root)
//!   [12..16]  right_sibling: i32  (-1 = none)
//!   [16..20]  right_child: u32  (internal: rightmost child pointer)
//!   [20..24]  free_end: u32  (cell region tail, cells grow downward from PAGE_SIZE)
//!   [24..32]  reserved
//!
//! Cell Pointer Array [32 .. 32 + key_count*2]:
//!   u16[] sorted by key, supports binary search
//!
//! Free Space [...gap...]
//!
//! Cells (grow from PAGE_SIZE toward header):
//!   Leaf:     [key_len: u16][key_bytes...][extent_id: u32][offset: u32]
//!   Internal: [left_child: u32][key_len: u16][key_bytes...]
//! ```

use sdb_common::{RecordId, Result, SdbError};

use crate::key::IndexKey;

pub const PAGE_SIZE: usize = 65536; // 64KB
pub const HEADER_SIZE: usize = 32;
pub const CELL_PTR_SIZE: usize = 2;

const EYE_0: u8 = b'I';
const EYE_1: u8 = b'X';

pub const NODE_INTERNAL: u8 = 0;
pub const NODE_LEAF: u8 = 1;

pub type Page = Vec<u8>;

/// Allocate a new zeroed page.
pub fn new_page() -> Page {
    vec![0u8; PAGE_SIZE]
}

// ============================================================
// Header read functions
// ============================================================

pub fn validate_node(data: &[u8]) -> Result<()> {
    if data.len() < HEADER_SIZE {
        return Err(SdbError::CorruptedRecord);
    }
    if data[0] != EYE_0 || data[1] != EYE_1 {
        return Err(SdbError::CorruptedRecord);
    }
    Ok(())
}

pub fn node_type(data: &[u8]) -> u8 {
    data[2]
}

pub fn key_count(data: &[u8]) -> u16 {
    u16::from_le_bytes([data[4], data[5]])
}

pub fn parent(data: &[u8]) -> i32 {
    i32::from_le_bytes([data[8], data[9], data[10], data[11]])
}

pub fn right_sibling(data: &[u8]) -> i32 {
    i32::from_le_bytes([data[12], data[13], data[14], data[15]])
}

pub fn right_child(data: &[u8]) -> u32 {
    u32::from_le_bytes([data[16], data[17], data[18], data[19]])
}

pub fn free_end(data: &[u8]) -> u32 {
    u32::from_le_bytes([data[20], data[21], data[22], data[23]])
}

// ============================================================
// Header write functions
// ============================================================

pub fn init_node(data: &mut [u8], ntype: u8, parent_id: i32) {
    // Zero out header
    data[..HEADER_SIZE].fill(0);
    // Eye catcher
    data[0] = EYE_0;
    data[1] = EYE_1;
    // Node type
    data[2] = ntype;
    // key_count = 0
    data[4..6].copy_from_slice(&0u16.to_le_bytes());
    // parent
    data[8..12].copy_from_slice(&parent_id.to_le_bytes());
    // right_sibling = -1
    data[12..16].copy_from_slice(&(-1i32).to_le_bytes());
    // right_child = 0
    data[16..20].copy_from_slice(&0u32.to_le_bytes());
    // free_end = PAGE_SIZE (cells grow downward)
    data[20..24].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
}

pub fn set_key_count(data: &mut [u8], count: u16) {
    data[4..6].copy_from_slice(&count.to_le_bytes());
}

pub fn set_parent(data: &mut [u8], p: i32) {
    data[8..12].copy_from_slice(&p.to_le_bytes());
}

pub fn set_right_sibling(data: &mut [u8], sib: i32) {
    data[12..16].copy_from_slice(&sib.to_le_bytes());
}

pub fn set_right_child(data: &mut [u8], child: u32) {
    data[16..20].copy_from_slice(&child.to_le_bytes());
}

pub fn set_free_end(data: &mut [u8], fe: u32) {
    data[20..24].copy_from_slice(&fe.to_le_bytes());
}

// ============================================================
// Cell pointer operations
// ============================================================

/// Get the cell offset stored at cell pointer index `idx`.
pub fn cell_pointer(data: &[u8], idx: u16) -> u16 {
    let off = HEADER_SIZE + (idx as usize) * CELL_PTR_SIZE;
    u16::from_le_bytes([data[off], data[off + 1]])
}

/// Set the cell pointer at index `idx` to `offset`.
pub fn set_cell_pointer(data: &mut [u8], idx: u16, offset: u16) {
    let off = HEADER_SIZE + (idx as usize) * CELL_PTR_SIZE;
    data[off..off + 2].copy_from_slice(&offset.to_le_bytes());
}

/// Insert a new cell pointer at `idx`, shifting subsequent pointers right.
/// Increments key_count.
pub fn insert_cell_pointer(data: &mut [u8], idx: u16, offset: u16) {
    let count = key_count(data);
    // Shift pointers [idx..count] to [idx+1..count+1]
    let src_start = HEADER_SIZE + (idx as usize) * CELL_PTR_SIZE;
    let move_bytes = ((count - idx) as usize) * CELL_PTR_SIZE;
    if move_bytes > 0 {
        let dst_start = src_start + CELL_PTR_SIZE;
        data.copy_within(src_start..src_start + move_bytes, dst_start);
    }
    // Write new pointer
    data[src_start..src_start + 2].copy_from_slice(&offset.to_le_bytes());
    set_key_count(data, count + 1);
}

/// Remove cell pointer at `idx`, shifting subsequent pointers left.
/// Decrements key_count. Does NOT reclaim cell space (dead space left in v1).
pub fn remove_cell_pointer(data: &mut [u8], idx: u16) {
    let count = key_count(data);
    let src_start = HEADER_SIZE + ((idx + 1) as usize) * CELL_PTR_SIZE;
    let dst_start = HEADER_SIZE + (idx as usize) * CELL_PTR_SIZE;
    let move_bytes = ((count - idx - 1) as usize) * CELL_PTR_SIZE;
    if move_bytes > 0 {
        data.copy_within(src_start..src_start + move_bytes, dst_start);
    }
    set_key_count(data, count - 1);
}

// ============================================================
// Cell read/write — Leaf
// ============================================================

/// Read a leaf cell at the given byte offset within the page.
/// Leaf cell format: [key_len: u16][key_bytes][extent_id: u32][offset: u32]
pub fn read_leaf_cell(data: &[u8], cell_off: u16) -> Result<(IndexKey, RecordId)> {
    let off = cell_off as usize;
    if off + 2 > data.len() {
        return Err(SdbError::CorruptedRecord);
    }
    let key_len = u16::from_le_bytes([data[off], data[off + 1]]) as usize;
    let key_start = off + 2;
    if key_start + key_len + 8 > data.len() {
        return Err(SdbError::CorruptedRecord);
    }
    let key = IndexKey::decode(&data[key_start..key_start + key_len])?;
    let rid_off = key_start + key_len;
    let extent_id = u32::from_le_bytes([
        data[rid_off],
        data[rid_off + 1],
        data[rid_off + 2],
        data[rid_off + 3],
    ]);
    let offset = u32::from_le_bytes([
        data[rid_off + 4],
        data[rid_off + 5],
        data[rid_off + 6],
        data[rid_off + 7],
    ]);
    Ok((key, RecordId { extent_id, offset }))
}

/// Write a leaf cell. Returns the byte offset where the cell was written.
/// Decrements free_end accordingly.
pub fn write_leaf_cell(data: &mut [u8], key_bytes: &[u8], rid: RecordId) -> u16 {
    let cell_size = 2 + key_bytes.len() + 8; // key_len + key + extent_id + offset
    let fe = free_end(data) as usize;
    let new_fe = fe - cell_size;
    // Write cell
    let off = new_fe;
    data[off..off + 2].copy_from_slice(&(key_bytes.len() as u16).to_le_bytes());
    data[off + 2..off + 2 + key_bytes.len()].copy_from_slice(key_bytes);
    let rid_off = off + 2 + key_bytes.len();
    data[rid_off..rid_off + 4].copy_from_slice(&rid.extent_id.to_le_bytes());
    data[rid_off + 4..rid_off + 8].copy_from_slice(&rid.offset.to_le_bytes());
    set_free_end(data, new_fe as u32);
    new_fe as u16
}

// ============================================================
// Cell read/write — Internal
// ============================================================

/// Read an internal cell at the given byte offset.
/// Internal cell format: [left_child: u32][key_len: u16][key_bytes]
pub fn read_internal_cell(data: &[u8], cell_off: u16) -> Result<(u32, IndexKey)> {
    let off = cell_off as usize;
    if off + 6 > data.len() {
        return Err(SdbError::CorruptedRecord);
    }
    let left_child = u32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]);
    let key_len = u16::from_le_bytes([data[off + 4], data[off + 5]]) as usize;
    let key_start = off + 6;
    if key_start + key_len > data.len() {
        return Err(SdbError::CorruptedRecord);
    }
    let key = IndexKey::decode(&data[key_start..key_start + key_len])?;
    Ok((left_child, key))
}

/// Write an internal cell. Returns the byte offset where the cell was written.
pub fn write_internal_cell(data: &mut [u8], key_bytes: &[u8], left_child: u32) -> u16 {
    let cell_size = 4 + 2 + key_bytes.len(); // left_child + key_len + key
    let fe = free_end(data) as usize;
    let new_fe = fe - cell_size;
    let off = new_fe;
    data[off..off + 4].copy_from_slice(&left_child.to_le_bytes());
    data[off + 4..off + 6].copy_from_slice(&(key_bytes.len() as u16).to_le_bytes());
    data[off + 6..off + 6 + key_bytes.len()].copy_from_slice(key_bytes);
    set_free_end(data, new_fe as u32);
    new_fe as u16
}

// ============================================================
// Space calculation
// ============================================================

/// Available space in the page for new cells + cell pointers.
pub fn available_space(data: &[u8]) -> usize {
    let count = key_count(data) as usize;
    let ptr_end = HEADER_SIZE + count * CELL_PTR_SIZE;
    let fe = free_end(data) as usize;
    if fe > ptr_end {
        fe - ptr_end
    } else {
        0
    }
}

/// Space needed to insert a leaf cell with the given key bytes.
pub fn leaf_cell_size(key_bytes: &[u8]) -> usize {
    2 + key_bytes.len() + 8 + CELL_PTR_SIZE // cell body + pointer
}

/// Space needed to insert an internal cell with the given key bytes.
pub fn internal_cell_size(key_bytes: &[u8]) -> usize {
    4 + 2 + key_bytes.len() + CELL_PTR_SIZE // cell body + pointer
}

#[cfg(test)]
mod tests {
    use super::*;
    use sdb_bson::element::Value;

    #[test]
    fn test_init_and_read_header() {
        let mut page = new_page();
        init_node(&mut page, NODE_LEAF, -1);
        validate_node(&page).unwrap();
        assert_eq!(node_type(&page), NODE_LEAF);
        assert_eq!(key_count(&page), 0);
        assert_eq!(parent(&page), -1);
        assert_eq!(right_sibling(&page), -1);
        assert_eq!(right_child(&page), 0);
        assert_eq!(free_end(&page), PAGE_SIZE as u32);
    }

    #[test]
    fn test_cell_pointer_insert_remove() {
        let mut page = new_page();
        init_node(&mut page, NODE_LEAF, -1);

        insert_cell_pointer(&mut page, 0, 100);
        insert_cell_pointer(&mut page, 1, 200);
        insert_cell_pointer(&mut page, 1, 150); // insert in middle

        assert_eq!(key_count(&page), 3);
        assert_eq!(cell_pointer(&page, 0), 100);
        assert_eq!(cell_pointer(&page, 1), 150);
        assert_eq!(cell_pointer(&page, 2), 200);

        remove_cell_pointer(&mut page, 1); // remove middle
        assert_eq!(key_count(&page), 2);
        assert_eq!(cell_pointer(&page, 0), 100);
        assert_eq!(cell_pointer(&page, 1), 200);
    }

    #[test]
    fn test_leaf_cell_roundtrip() {
        let mut page = new_page();
        init_node(&mut page, NODE_LEAF, -1);

        let key = IndexKey::new(vec![Value::Int32(42), Value::String("test".into())]);
        let key_bytes = key.encode();
        let rid = RecordId {
            extent_id: 5,
            offset: 1024,
        };

        let cell_off = write_leaf_cell(&mut page, &key_bytes, rid);
        let (decoded_key, decoded_rid) = read_leaf_cell(&page, cell_off).unwrap();

        assert_eq!(decoded_rid, rid);
        assert_eq!(decoded_key.fields.len(), 2);
    }

    #[test]
    fn test_internal_cell_roundtrip() {
        let mut page = new_page();
        init_node(&mut page, NODE_INTERNAL, -1);

        let key = IndexKey::new(vec![Value::Int64(999)]);
        let key_bytes = key.encode();
        let left_child = 7u32;

        let cell_off = write_internal_cell(&mut page, &key_bytes, left_child);
        let (decoded_child, decoded_key) = read_internal_cell(&page, cell_off).unwrap();

        assert_eq!(decoded_child, left_child);
        assert_eq!(decoded_key.fields.len(), 1);
    }

    #[test]
    fn test_available_space() {
        let mut page = new_page();
        init_node(&mut page, NODE_LEAF, -1);
        let initial = available_space(&page);
        assert_eq!(initial, PAGE_SIZE - HEADER_SIZE);

        // Write a cell to consume space
        let key = IndexKey::new(vec![Value::Int32(1)]);
        let kb = key.encode();
        let cell_off = write_leaf_cell(
            &mut page,
            &kb,
            RecordId {
                extent_id: 0,
                offset: 0,
            },
        );
        insert_cell_pointer(&mut page, 0, cell_off);

        let after = available_space(&page);
        assert!(after < initial);
    }
}
