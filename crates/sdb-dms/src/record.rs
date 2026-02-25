//! On-page record header read/write functions.
//!
//! Record layout (16-byte header + payload):
//! ```text
//! [+0]     flag: u8          (0x00=normal, 0x01=deleted)
//! [+1..+4] pad: [u8; 3]     (reserved, zero)
//! [+4..+8] size: u32         (total record size = header + payload)
//! [+8..+12] prev_offset: i32 (-1 = none)
//! [+12..+16] next_offset: i32 (-1 = none)
//! [+16..]  payload: [u8]     (BSON bytes for normal records)
//! ```
//!
//! For deleted records, the payload area is unused but prev/next are
//! repurposed as the delete-list chain pointers:
//! ```text
//! [+8..+12]  deleted_next_extent: i32  (-1 = end)
//! [+12..+16] deleted_next_offset: i32  (-1 = end)
//! ```

use crate::page::PAGE_SIZE;

pub const RECORD_HEADER_SIZE: usize = 16;
pub const FLAG_NORMAL: u8 = 0x00;
pub const FLAG_DELETED: u8 = 0x01;

// --- readers ---

#[inline]
pub fn record_flag(data: &[u8; PAGE_SIZE], offset: usize) -> u8 {
    data[offset]
}

#[inline]
pub fn record_size(data: &[u8; PAGE_SIZE], offset: usize) -> u32 {
    u32::from_le_bytes([
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ])
}

#[inline]
pub fn record_prev_offset(data: &[u8; PAGE_SIZE], offset: usize) -> i32 {
    i32::from_le_bytes([
        data[offset + 8],
        data[offset + 9],
        data[offset + 10],
        data[offset + 11],
    ])
}

#[inline]
pub fn record_next_offset(data: &[u8; PAGE_SIZE], offset: usize) -> i32 {
    i32::from_le_bytes([
        data[offset + 12],
        data[offset + 13],
        data[offset + 14],
        data[offset + 15],
    ])
}

/// Read the BSON payload slice from a record.
#[inline]
pub fn record_payload(data: &[u8; PAGE_SIZE], offset: usize) -> &[u8] {
    let total = record_size(data, offset) as usize;
    let payload_len = total - RECORD_HEADER_SIZE;
    &data[offset + RECORD_HEADER_SIZE..offset + RECORD_HEADER_SIZE + payload_len]
}

// --- deleted-record chain readers ---

/// For a deleted record, the prev_offset field stores the next extent id in the delete chain.
#[inline]
pub fn deleted_next_extent(data: &[u8; PAGE_SIZE], offset: usize) -> i32 {
    record_prev_offset(data, offset)
}

/// For a deleted record, the next_offset field stores the next record offset in the delete chain.
#[inline]
pub fn deleted_next_offset(data: &[u8; PAGE_SIZE], offset: usize) -> i32 {
    record_next_offset(data, offset)
}

// --- writers ---

/// Write a normal record header at `offset` in the page.
pub fn write_record_header(
    data: &mut [u8; PAGE_SIZE],
    offset: usize,
    flag: u8,
    size: u32,
    prev: i32,
    next: i32,
) {
    data[offset] = flag;
    data[offset + 1] = 0;
    data[offset + 2] = 0;
    data[offset + 3] = 0;
    data[offset + 4..offset + 8].copy_from_slice(&size.to_le_bytes());
    data[offset + 8..offset + 12].copy_from_slice(&prev.to_le_bytes());
    data[offset + 12..offset + 16].copy_from_slice(&next.to_le_bytes());
}

/// Write the BSON payload bytes after the record header.
pub fn write_record_payload(data: &mut [u8; PAGE_SIZE], offset: usize, payload: &[u8]) {
    let start = offset + RECORD_HEADER_SIZE;
    data[start..start + payload.len()].copy_from_slice(payload);
}

/// Mark a record as deleted and set its delete-chain pointers.
pub fn write_deleted_record(
    data: &mut [u8; PAGE_SIZE],
    offset: usize,
    next_ext: i32,
    next_off: i32,
) {
    data[offset] = FLAG_DELETED;
    // size stays unchanged
    // repurpose prev/next as delete-chain pointers
    data[offset + 8..offset + 12].copy_from_slice(&next_ext.to_le_bytes());
    data[offset + 12..offset + 16].copy_from_slice(&next_off.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extent::EXTENT_HEADER_SIZE;

    #[test]
    fn write_and_read_header() {
        let mut page = [0u8; PAGE_SIZE];
        let off = EXTENT_HEADER_SIZE;
        write_record_header(&mut page, off, FLAG_NORMAL, 48, -1, -1);

        assert_eq!(record_flag(&page, off), FLAG_NORMAL);
        assert_eq!(record_size(&page, off), 48);
        assert_eq!(record_prev_offset(&page, off), -1);
        assert_eq!(record_next_offset(&page, off), -1);
    }

    #[test]
    fn write_and_read_payload() {
        let mut page = [0u8; PAGE_SIZE];
        let off = EXTENT_HEADER_SIZE;
        let payload = b"hello bson";
        let total = RECORD_HEADER_SIZE as u32 + payload.len() as u32;
        write_record_header(&mut page, off, FLAG_NORMAL, total, -1, -1);
        write_record_payload(&mut page, off, payload);

        assert_eq!(record_payload(&page, off), payload);
    }

    #[test]
    fn deleted_record_chain() {
        let mut page = [0u8; PAGE_SIZE];
        let off = EXTENT_HEADER_SIZE;
        write_record_header(&mut page, off, FLAG_NORMAL, 32, -1, -1);
        // Mark deleted, chain to extent=2, offset=100
        write_deleted_record(&mut page, off, 2, 100);

        assert_eq!(record_flag(&page, off), FLAG_DELETED);
        assert_eq!(deleted_next_extent(&page, off), 2);
        assert_eq!(deleted_next_offset(&page, off), 100);
        // size unchanged
        assert_eq!(record_size(&page, off), 32);
    }

    #[test]
    fn linked_records() {
        let mut page = [0u8; PAGE_SIZE];
        let off1 = EXTENT_HEADER_SIZE;
        let off2 = off1 + 64;
        write_record_header(&mut page, off1, FLAG_NORMAL, 64, -1, off2 as i32);
        write_record_header(&mut page, off2, FLAG_NORMAL, 64, off1 as i32, -1);

        assert_eq!(record_next_offset(&page, off1), off2 as i32);
        assert_eq!(record_prev_offset(&page, off2), off1 as i32);
    }
}
