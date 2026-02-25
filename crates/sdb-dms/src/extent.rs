//! On-page extent header read/write functions.
//!
//! Layout (48 bytes at offset 0 of each extent page):
//! ```text
//! [0..2]   eye_catcher: [u8; 2] = [b'D', b'E']
//! [2..4]   block_size: u16      (in pages, always 1 for now)
//! [4..6]   mb_id: u16           (metadata block / collection id)
//! [6]      flag: u8
//! [7]      version: u8
//! [8..12]  logic_id: i32
//! [12..16] prev_extent: i32     (-1 = none)
//! [16..20] next_extent: i32     (-1 = none)
//! [20..24] rec_count: u32
//! [24..28] first_record_offset: i32  (-1 = none)
//! [28..32] last_record_offset: i32   (-1 = none)
//! [32..36] free_space: u32
//! [36..48] reserved: [u8; 12]
//! ```

use crate::page::PAGE_SIZE;
use sdb_common::{Result, SdbError};

pub const EXTENT_HEADER_SIZE: usize = 48;
pub const EYE_CATCHER: [u8; 2] = [b'D', b'E'];
pub const INVALID_OFFSET: i32 = -1;

// --- readers ---

#[inline]
pub fn eye_catcher(data: &[u8; PAGE_SIZE]) -> [u8; 2] {
    [data[0], data[1]]
}

#[inline]
pub fn block_size(data: &[u8; PAGE_SIZE]) -> u16 {
    u16::from_le_bytes([data[2], data[3]])
}

#[inline]
pub fn mb_id(data: &[u8; PAGE_SIZE]) -> u16 {
    u16::from_le_bytes([data[4], data[5]])
}

#[inline]
pub fn flag(data: &[u8; PAGE_SIZE]) -> u8 {
    data[6]
}

#[inline]
pub fn version(data: &[u8; PAGE_SIZE]) -> u8 {
    data[7]
}

#[inline]
pub fn logic_id(data: &[u8; PAGE_SIZE]) -> i32 {
    i32::from_le_bytes([data[8], data[9], data[10], data[11]])
}

#[inline]
pub fn prev_extent(data: &[u8; PAGE_SIZE]) -> i32 {
    i32::from_le_bytes([data[12], data[13], data[14], data[15]])
}

#[inline]
pub fn next_extent(data: &[u8; PAGE_SIZE]) -> i32 {
    i32::from_le_bytes([data[16], data[17], data[18], data[19]])
}

#[inline]
pub fn rec_count(data: &[u8; PAGE_SIZE]) -> u32 {
    u32::from_le_bytes([data[20], data[21], data[22], data[23]])
}

#[inline]
pub fn first_record_offset(data: &[u8; PAGE_SIZE]) -> i32 {
    i32::from_le_bytes([data[24], data[25], data[26], data[27]])
}

#[inline]
pub fn last_record_offset(data: &[u8; PAGE_SIZE]) -> i32 {
    i32::from_le_bytes([data[28], data[29], data[30], data[31]])
}

#[inline]
pub fn free_space(data: &[u8; PAGE_SIZE]) -> u32 {
    u32::from_le_bytes([data[32], data[33], data[34], data[35]])
}

// --- writers ---

#[inline]
pub fn set_flag(data: &mut [u8; PAGE_SIZE], v: u8) {
    data[6] = v;
}

#[inline]
pub fn set_version(data: &mut [u8; PAGE_SIZE], v: u8) {
    data[7] = v;
}

#[inline]
pub fn set_prev_extent(data: &mut [u8; PAGE_SIZE], v: i32) {
    data[12..16].copy_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn set_next_extent(data: &mut [u8; PAGE_SIZE], v: i32) {
    data[16..20].copy_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn set_rec_count(data: &mut [u8; PAGE_SIZE], v: u32) {
    data[20..24].copy_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn set_first_record_offset(data: &mut [u8; PAGE_SIZE], v: i32) {
    data[24..28].copy_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn set_last_record_offset(data: &mut [u8; PAGE_SIZE], v: i32) {
    data[28..32].copy_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn set_free_space(data: &mut [u8; PAGE_SIZE], v: u32) {
    data[32..36].copy_from_slice(&v.to_le_bytes());
}

// --- init / validate ---

/// Initialize an extent page with default header values.
pub fn init_extent(
    data: &mut [u8; PAGE_SIZE],
    mb_id_val: u16,
    logic_id_val: i32,
    prev: i32,
    next: i32,
) {
    // zero the header region
    data[..EXTENT_HEADER_SIZE].fill(0);
    // eye catcher
    data[0] = EYE_CATCHER[0];
    data[1] = EYE_CATCHER[1];
    // block_size = 1
    data[2..4].copy_from_slice(&1u16.to_le_bytes());
    // mb_id
    data[4..6].copy_from_slice(&mb_id_val.to_le_bytes());
    // flag = 0, version = 0 (already zeroed)
    // logic_id
    data[8..12].copy_from_slice(&logic_id_val.to_le_bytes());
    // prev/next extent
    data[12..16].copy_from_slice(&prev.to_le_bytes());
    data[16..20].copy_from_slice(&next.to_le_bytes());
    // rec_count = 0 (already zeroed)
    // first/last record offset = -1
    data[24..28].copy_from_slice(&INVALID_OFFSET.to_le_bytes());
    data[28..32].copy_from_slice(&INVALID_OFFSET.to_le_bytes());
    // free_space = PAGE_SIZE - EXTENT_HEADER_SIZE
    let fs = (PAGE_SIZE - EXTENT_HEADER_SIZE) as u32;
    data[32..36].copy_from_slice(&fs.to_le_bytes());
}

/// Validate the eye catcher of an extent page.
pub fn validate_extent(data: &[u8; PAGE_SIZE]) -> Result<()> {
    if eye_catcher(data) != EYE_CATCHER {
        return Err(SdbError::CorruptedRecord);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_and_read_back() {
        let mut page = [0u8; PAGE_SIZE];
        init_extent(&mut page, 42, 7, INVALID_OFFSET, INVALID_OFFSET);

        assert_eq!(eye_catcher(&page), EYE_CATCHER);
        assert_eq!(block_size(&page), 1);
        assert_eq!(mb_id(&page), 42);
        assert_eq!(flag(&page), 0);
        assert_eq!(version(&page), 0);
        assert_eq!(logic_id(&page), 7);
        assert_eq!(prev_extent(&page), INVALID_OFFSET);
        assert_eq!(next_extent(&page), INVALID_OFFSET);
        assert_eq!(rec_count(&page), 0);
        assert_eq!(first_record_offset(&page), INVALID_OFFSET);
        assert_eq!(last_record_offset(&page), INVALID_OFFSET);
        assert_eq!(free_space(&page), (PAGE_SIZE - EXTENT_HEADER_SIZE) as u32);
    }

    #[test]
    fn setters_roundtrip() {
        let mut page = [0u8; PAGE_SIZE];
        init_extent(&mut page, 1, 0, INVALID_OFFSET, INVALID_OFFSET);

        set_prev_extent(&mut page, 3);
        assert_eq!(prev_extent(&page), 3);

        set_next_extent(&mut page, 5);
        assert_eq!(next_extent(&page), 5);

        set_rec_count(&mut page, 100);
        assert_eq!(rec_count(&page), 100);

        set_first_record_offset(&mut page, 48);
        assert_eq!(first_record_offset(&page), 48);

        set_last_record_offset(&mut page, 200);
        assert_eq!(last_record_offset(&page), 200);

        set_free_space(&mut page, 12345);
        assert_eq!(free_space(&page), 12345);

        set_flag(&mut page, 0x01);
        assert_eq!(flag(&page), 0x01);

        set_version(&mut page, 2);
        assert_eq!(version(&page), 2);
    }

    #[test]
    fn validate_good() {
        let mut page = [0u8; PAGE_SIZE];
        init_extent(&mut page, 0, 0, INVALID_OFFSET, INVALID_OFFSET);
        assert!(validate_extent(&page).is_ok());
    }

    #[test]
    fn validate_bad() {
        let page = [0u8; PAGE_SIZE];
        assert_eq!(
            validate_extent(&page).unwrap_err(),
            SdbError::CorruptedRecord
        );
    }

    #[test]
    fn linked_extents() {
        let mut p0 = [0u8; PAGE_SIZE];
        let mut p1 = [0u8; PAGE_SIZE];
        init_extent(&mut p0, 1, 0, INVALID_OFFSET, 1);
        init_extent(&mut p1, 1, 1, 0, INVALID_OFFSET);

        assert_eq!(next_extent(&p0), 1);
        assert_eq!(prev_extent(&p1), 0);
    }
}
