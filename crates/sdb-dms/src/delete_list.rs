//! Delete list: 10-bucket free-space manager for record slot reuse.
//!
//! Buckets are sized by total record size (header + payload):
//! ```text
//! 0: ≤64    1: ≤128   2: ≤256   3: ≤512   4: ≤1024
//! 5: ≤2048  6: ≤4096  7: ≤8192  8: ≤16384  9: >16384
//! ```
//!
//! Each bucket head stores `(extent_id: i32, offset: i32)` pointing to
//! the first deleted record in that bucket's chain. The chain continues
//! via the deleted record's repurposed prev/next fields.

use crate::page::Page;
use crate::record;

pub const BUCKET_COUNT: usize = 10;
const BUCKET_LIMITS: [u32; 9] = [64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384];

/// 10-bucket delete list for deleted-record space reuse.
pub struct DeleteList {
    /// Each bucket head: (extent_id, byte_offset). (-1, -1) = empty.
    heads: [(i32, i32); BUCKET_COUNT],
}

impl DeleteList {
    pub fn new() -> Self {
        Self {
            heads: [(-1, -1); BUCKET_COUNT],
        }
    }

    /// Determine which bucket a record of `total_size` bytes belongs to.
    pub fn bucket_for_size(total_size: u32) -> usize {
        for (i, &limit) in BUCKET_LIMITS.iter().enumerate() {
            if total_size <= limit {
                return i;
            }
        }
        BUCKET_COUNT - 1
    }

    /// Push a deleted record slot onto its bucket's chain head.
    ///
    /// The deleted record at `(ext_id, offset)` on the given page is updated
    /// so its delete-chain next pointers point to the old bucket head.
    pub fn push(&mut self, total_size: u32, ext_id: u32, offset: u32, pages: &mut [Page]) {
        let bucket = Self::bucket_for_size(total_size);
        let (old_ext, old_off) = self.heads[bucket];

        // Update the deleted record's chain pointers to the old head
        let page = &mut pages[ext_id as usize];
        record::write_deleted_record(
            &mut page.data,
            offset as usize,
            old_ext,
            old_off,
        );
        page.dirty = true;

        // This slot is now the new head
        self.heads[bucket] = (ext_id as i32, offset as i32);
    }

    /// Try to find a deleted slot that can hold `needed_size` bytes.
    ///
    /// Searches from the target bucket upward. V1: only checks the head
    /// of each bucket (no chain traversal).
    ///
    /// Returns `Some((extent_id, byte_offset, slot_total_size))` on success.
    pub fn pop_fit(&mut self, needed_size: u32, pages: &[Page]) -> Option<(u32, u32, u32)> {
        let start = Self::bucket_for_size(needed_size);
        for bucket in start..BUCKET_COUNT {
            let (ext, off) = self.heads[bucket];
            if ext < 0 {
                continue;
            }
            let ext_u = ext as u32;
            let off_u = off as u32;
            let page = &pages[ext_u as usize];
            let slot_size = record::record_size(&page.data, off_u as usize);
            if slot_size >= needed_size {
                // Pop: advance head to the next in chain
                let next_ext = record::deleted_next_extent(&page.data, off_u as usize);
                let next_off = record::deleted_next_offset(&page.data, off_u as usize);
                self.heads[bucket] = (next_ext, next_off);
                return Some((ext_u, off_u, slot_size));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extent::{self, EXTENT_HEADER_SIZE};
    use crate::record::{self as rec, FLAG_NORMAL};

    fn make_page(id: u32) -> Page {
        let mut p = Page::new(id);
        extent::init_extent(&mut p.data, 0, 0, -1, -1);
        p
    }

    #[test]
    fn bucket_boundaries() {
        assert_eq!(DeleteList::bucket_for_size(1), 0);
        assert_eq!(DeleteList::bucket_for_size(64), 0);
        assert_eq!(DeleteList::bucket_for_size(65), 1);
        assert_eq!(DeleteList::bucket_for_size(128), 1);
        assert_eq!(DeleteList::bucket_for_size(129), 2);
        assert_eq!(DeleteList::bucket_for_size(16384), 8);
        assert_eq!(DeleteList::bucket_for_size(16385), 9);
        assert_eq!(DeleteList::bucket_for_size(60000), 9);
    }

    #[test]
    fn push_and_pop() {
        let mut dl = DeleteList::new();
        let mut pages = vec![make_page(0)];

        let off = EXTENT_HEADER_SIZE as u32;
        let total_size = 48u32;
        // Write a dummy record first, then "delete" it
        rec::write_record_header(
            &mut pages[0].data,
            off as usize,
            FLAG_NORMAL,
            total_size,
            -1,
            -1,
        );

        dl.push(total_size, 0, off, &mut pages);

        // Should be in bucket 0 (≤64)
        assert_eq!(dl.heads[0], (0, off as i32));

        // Pop it back
        let result = dl.pop_fit(total_size, &pages);
        assert_eq!(result, Some((0, off, total_size)));
        // Bucket head should now be empty
        assert_eq!(dl.heads[0], (-1, -1));
    }

    #[test]
    fn pop_searches_upward() {
        let mut dl = DeleteList::new();
        let mut pages = vec![make_page(0)];

        // Put a 256-byte slot in bucket 2
        let off = EXTENT_HEADER_SIZE as u32;
        rec::write_record_header(
            &mut pages[0].data,
            off as usize,
            FLAG_NORMAL,
            256,
            -1,
            -1,
        );
        dl.push(256, 0, off, &mut pages);

        // Request 100 bytes — bucket 1 (≤128) is empty, should find in bucket 2
        let result = dl.pop_fit(100, &pages);
        assert_eq!(result, Some((0, off, 256)));
    }

    #[test]
    fn pop_empty_returns_none() {
        let mut dl = DeleteList::new();
        let pages: Vec<Page> = vec![];
        assert_eq!(dl.pop_fit(100, &pages), None);
    }

    #[test]
    fn chained_slots() {
        let mut dl = DeleteList::new();
        let mut pages = vec![make_page(0)];

        let off1 = EXTENT_HEADER_SIZE as u32;
        let off2 = off1 + 64;
        rec::write_record_header(&mut pages[0].data, off1 as usize, FLAG_NORMAL, 48, -1, -1);
        rec::write_record_header(&mut pages[0].data, off2 as usize, FLAG_NORMAL, 48, -1, -1);

        // Push two slots into same bucket
        dl.push(48, 0, off1, &mut pages);
        dl.push(48, 0, off2, &mut pages);

        // Head should be off2 (LIFO)
        assert_eq!(dl.heads[0], (0, off2 as i32));

        // Pop first — gets off2
        let r1 = dl.pop_fit(48, &pages).unwrap();
        assert_eq!(r1, (0, off2, 48));

        // Pop second — gets off1
        let r2 = dl.pop_fit(48, &pages).unwrap();
        assert_eq!(r2, (0, off1, 48));

        // Empty now
        assert_eq!(dl.pop_fit(48, &pages), None);
    }
}
