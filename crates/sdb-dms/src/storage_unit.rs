//! Storage unit — manages in-memory pages for a single collection.
//!
//! Implements extent-based storage with record offset chains and
//! delete-list space reuse, faithful to SequoiaDB's C++ dms layer.

use std::sync::Mutex;

use sdb_bson::Document;
use sdb_common::{CollectionId, CollectionSpaceId, RecordId, Result, SdbError};

use crate::delete_list::DeleteList;
use crate::extent::{self, EXTENT_HEADER_SIZE, INVALID_OFFSET};
use crate::page::{Page, PAGE_SIZE};
use crate::record::{self, FLAG_DELETED, FLAG_NORMAL, RECORD_HEADER_SIZE};

struct Inner {
    pages: Vec<Page>,
    first_extent: i32, // page id of first extent, -1 = none
    last_extent: i32,  // page id of last extent, -1 = none
    extent_count: u32,
    delete_list: DeleteList,
    next_logic_id: i32,
    total_records: u64,
}

pub struct StorageUnit {
    pub cs_id: CollectionSpaceId,
    pub cl_id: CollectionId,
    inner: Mutex<Inner>,
}

impl StorageUnit {
    pub fn new(cs_id: CollectionSpaceId, cl_id: CollectionId) -> Self {
        Self {
            cs_id,
            cl_id,
            inner: Mutex::new(Inner {
                pages: Vec::new(),
                first_extent: INVALID_OFFSET,
                last_extent: INVALID_OFFSET,
                extent_count: 0,
                delete_list: DeleteList::new(),
                next_logic_id: 0,
                total_records: 0,
            }),
        }
    }

    pub fn total_records(&self) -> u64 {
        self.inner.lock().unwrap().total_records
    }

    pub fn extent_count(&self) -> u32 {
        self.inner.lock().unwrap().extent_count
    }
}

// ── Internal helpers (called with lock held) ──

impl Inner {
    /// Allocate a new extent page, link it into the extent chain, return its page id.
    fn allocate_extent(&mut self, mb_id: u16) -> u32 {
        let page_id = self.pages.len() as u32;
        let mut page = Page::new(page_id);

        let prev = self.last_extent;
        let logic_id = self.next_logic_id;
        self.next_logic_id += 1;

        extent::init_extent(&mut page.data, mb_id, logic_id, prev, INVALID_OFFSET);
        page.dirty = true;
        self.pages.push(page);

        // Link previous extent's next to this one
        if prev >= 0 {
            let prev_page = &mut self.pages[prev as usize];
            extent::set_next_extent(&mut prev_page.data, page_id as i32);
            prev_page.dirty = true;
        }

        if self.first_extent < 0 {
            self.first_extent = page_id as i32;
        }
        self.last_extent = page_id as i32;
        self.extent_count += 1;

        page_id
    }

    /// Write a record (header + payload) at the given offset in a page.
    /// Updates extent header: rec_count, first/last_record_offset, free_space.
    /// Links into the record offset chain.
    fn write_record(&mut self, page_id: u32, offset: u32, bson_data: &[u8]) {
        let total_size = (RECORD_HEADER_SIZE + bson_data.len()) as u32;
        let page = &mut self.pages[page_id as usize];

        // Get current last_record_offset to link the chain
        let last_rec_off = extent::last_record_offset(&page.data);

        // Write record header: prev = last_record, next = -1
        record::write_record_header(
            &mut page.data,
            offset as usize,
            FLAG_NORMAL,
            total_size,
            last_rec_off,
            INVALID_OFFSET,
        );
        // Write payload
        record::write_record_payload(&mut page.data, offset as usize, bson_data);

        // If there was a previous last record, update its next to point here
        if last_rec_off >= 0 {
            let next_bytes = (offset as i32).to_le_bytes();
            let base = last_rec_off as usize + 12;
            page.data[base..base + 4].copy_from_slice(&next_bytes);
        }

        // Update extent header
        let count = extent::rec_count(&page.data) + 1;
        extent::set_rec_count(&mut page.data, count);

        if extent::first_record_offset(&page.data) == INVALID_OFFSET {
            extent::set_first_record_offset(&mut page.data, offset as i32);
        }
        extent::set_last_record_offset(&mut page.data, offset as i32);

        let fs = extent::free_space(&page.data) - total_size;
        extent::set_free_space(&mut page.data, fs);

        page.dirty = true;
    }

    /// Try to append a record to the tail of the given extent page.
    /// Returns the byte offset if there's enough free space, or None.
    fn try_append(&self, page_id: u32, needed: u32) -> Option<u32> {
        let page = &self.pages[page_id as usize];
        let fs = extent::free_space(&page.data);
        if fs < needed {
            return None;
        }
        // Append position = PAGE_SIZE - free_space
        let offset = (PAGE_SIZE as u32) - fs;
        Some(offset)
    }

    fn insert_impl(&mut self, doc: &Document, mb_id: u16) -> Result<RecordId> {
        let bson_data = doc.to_bytes().map_err(|_| SdbError::InvalidBson)?;
        let total_size = (RECORD_HEADER_SIZE + bson_data.len()) as u32;

        if total_size as usize > PAGE_SIZE - EXTENT_HEADER_SIZE {
            return Err(SdbError::InvalidArg);
        }

        // 1. Try delete list reuse
        if let Some((ext_id, off, slot_size)) = self.delete_list.pop_fit(total_size, &self.pages) {
            // Reuse the slot: write new record into it
            let page = &mut self.pages[ext_id as usize];

            // The slot may be larger; we write with the original slot_size to
            // preserve the delete-list bucket granularity (no splitting in v1).
            // Write header with slot_size so the full slot is accounted for.
            record::write_record_header(
                &mut page.data,
                off as usize,
                FLAG_NORMAL,
                slot_size, // keep original slot size
                INVALID_OFFSET,
                INVALID_OFFSET,
            );
            record::write_record_payload(&mut page.data, off as usize, &bson_data);

            // Re-link into the extent's record chain at the tail
            let last_rec_off = extent::last_record_offset(&page.data);

            // Update our header's prev to the current tail
            let prev_bytes = last_rec_off.to_le_bytes();
            page.data[off as usize + 8..off as usize + 12].copy_from_slice(&prev_bytes);

            // If there was a previous last record, update its next
            if last_rec_off >= 0 {
                let next_bytes = (off as i32).to_le_bytes();
                let base = last_rec_off as usize + 12;
                page.data[base..base + 4].copy_from_slice(&next_bytes);
            }

            // Update extent header
            let count = extent::rec_count(&page.data) + 1;
            extent::set_rec_count(&mut page.data, count);
            if extent::first_record_offset(&page.data) == INVALID_OFFSET {
                extent::set_first_record_offset(&mut page.data, off as i32);
            }
            extent::set_last_record_offset(&mut page.data, off as i32);
            // free_space doesn't change — the slot was already accounted for
            page.dirty = true;

            self.total_records += 1;
            return Ok(RecordId {
                extent_id: ext_id,
                offset: off,
            });
        }

        // 2. Try appending to the last extent
        if self.last_extent >= 0 {
            let last = self.last_extent as u32;
            if let Some(offset) = self.try_append(last, total_size) {
                self.write_record(last, offset, &bson_data);
                self.total_records += 1;
                return Ok(RecordId {
                    extent_id: last,
                    offset,
                });
            }
        }

        // 3. Allocate a new extent
        let new_ext = self.allocate_extent(mb_id);
        let offset = self
            .try_append(new_ext, total_size)
            .expect("fresh extent must have space");
        self.write_record(new_ext, offset, &bson_data);
        self.total_records += 1;
        Ok(RecordId {
            extent_id: new_ext,
            offset,
        })
    }

    fn find_impl(&self, rid: RecordId) -> Result<Document> {
        let page = self
            .pages
            .get(rid.extent_id as usize)
            .ok_or(SdbError::NotFound)?;
        extent::validate_extent(&page.data)?;

        if rid.offset as usize + RECORD_HEADER_SIZE > PAGE_SIZE {
            return Err(SdbError::CorruptedRecord);
        }

        let flag = record::record_flag(&page.data, rid.offset as usize);
        if flag == FLAG_DELETED {
            return Err(SdbError::NotFound);
        }

        let payload = record::record_payload(&page.data, rid.offset as usize);
        Document::from_bytes(payload).map_err(|_| SdbError::CorruptedRecord)
    }

    fn delete_impl(&mut self, rid: RecordId) -> Result<()> {
        let page_idx = rid.extent_id as usize;
        if page_idx >= self.pages.len() {
            return Err(SdbError::NotFound);
        }

        {
            let page = &self.pages[page_idx];
            extent::validate_extent(&page.data)?;
        }

        let offset = rid.offset as usize;
        {
            let page = &self.pages[page_idx];
            if record::record_flag(&page.data, offset) == FLAG_DELETED {
                return Err(SdbError::NotFound);
            }
        }

        let total_size;
        let prev_off;
        let next_off;
        {
            let page = &self.pages[page_idx];
            total_size = record::record_size(&page.data, offset);
            prev_off = record::record_prev_offset(&page.data, offset);
            next_off = record::record_next_offset(&page.data, offset);
        }

        // Unlink from the record chain
        {
            let page = &mut self.pages[page_idx];
            if prev_off >= 0 {
                // prev.next = our next
                let base = prev_off as usize + 12;
                page.data[base..base + 4].copy_from_slice(&next_off.to_le_bytes());
            } else {
                // We were first_record
                extent::set_first_record_offset(&mut page.data, next_off);
            }

            if next_off >= 0 {
                // next.prev = our prev
                let base = next_off as usize + 8;
                page.data[base..base + 4].copy_from_slice(&prev_off.to_le_bytes());
            } else {
                // We were last_record
                extent::set_last_record_offset(&mut page.data, prev_off);
            }

            let count = extent::rec_count(&page.data).saturating_sub(1);
            extent::set_rec_count(&mut page.data, count);
            page.dirty = true;
        }

        // Push onto delete list (this writes the deleted flag + chain pointers)
        self.delete_list
            .push(total_size, rid.extent_id, rid.offset, &mut self.pages);

        self.total_records = self.total_records.saturating_sub(1);
        Ok(())
    }

    fn update_impl(&mut self, rid: RecordId, doc: &Document, mb_id: u16) -> Result<RecordId> {
        self.delete_impl(rid)?;
        self.insert_impl(doc, mb_id)
    }
}

// ── Public API (acquires lock) ──

impl StorageUnit {
    pub fn insert(&self, doc: &Document) -> Result<RecordId> {
        let mut inner = self.inner.lock().unwrap();
        inner.insert_impl(doc, self.cl_id)
    }

    pub fn find(&self, rid: RecordId) -> Result<Document> {
        let inner = self.inner.lock().unwrap();
        inner.find_impl(rid)
    }

    pub fn delete(&self, rid: RecordId) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.delete_impl(rid)
    }

    pub fn update(&self, rid: RecordId, doc: &Document) -> Result<RecordId> {
        let mut inner = self.inner.lock().unwrap();
        inner.update_impl(rid, doc, self.cl_id)
    }
}

// ── StorageEngine trait impl ──

impl crate::engine::StorageEngine for StorageUnit {
    fn insert(&self, doc: &Document) -> Result<RecordId> {
        StorageUnit::insert(self, doc)
    }

    fn find(&self, rid: RecordId) -> Result<Document> {
        StorageUnit::find(self, rid)
    }

    fn delete(&self, rid: RecordId) -> Result<()> {
        StorageUnit::delete(self, rid)
    }

    fn update(&self, rid: RecordId, doc: &Document) -> Result<RecordId> {
        StorageUnit::update(self, rid, doc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sdb_bson::{DocumentBuilder, Value};

    fn make_doc(key: &str, val: &str) -> Document {
        DocumentBuilder::new().append_string(key, val).build()
    }

    fn make_large_doc(size: usize) -> Document {
        let payload = "x".repeat(size);
        DocumentBuilder::new()
            .append_string("data", payload)
            .build()
    }

    #[test]
    fn insert_and_find_roundtrip() {
        let su = StorageUnit::new(1, 1);
        let doc = make_doc("name", "alice");
        let rid = su.insert(&doc).unwrap();
        let found = su.find(rid).unwrap();
        assert_eq!(found.get("name").unwrap(), doc.get("name").unwrap());
    }

    #[test]
    fn multiple_inserts() {
        let su = StorageUnit::new(1, 1);
        let mut rids = Vec::new();
        for i in 0..100 {
            let doc = make_doc("i", &i.to_string());
            rids.push(su.insert(&doc).unwrap());
        }
        assert_eq!(su.total_records(), 100);

        for (i, rid) in rids.iter().enumerate() {
            let doc = su.find(*rid).unwrap();
            assert_eq!(doc.get("i").unwrap(), &Value::String(i.to_string()));
        }
    }

    #[test]
    fn delete_makes_not_found() {
        let su = StorageUnit::new(1, 1);
        let doc = make_doc("x", "y");
        let rid = su.insert(&doc).unwrap();
        su.delete(rid).unwrap();
        assert_eq!(su.find(rid).unwrap_err(), SdbError::NotFound);
        assert_eq!(su.total_records(), 0);
    }

    #[test]
    fn delete_nonexistent() {
        let su = StorageUnit::new(1, 1);
        let rid = RecordId {
            extent_id: 99,
            offset: 0,
        };
        assert_eq!(su.delete(rid).unwrap_err(), SdbError::NotFound);
    }

    #[test]
    fn update_returns_new_rid() {
        let su = StorageUnit::new(1, 1);
        let doc1 = make_doc("v", "1");
        let rid1 = su.insert(&doc1).unwrap();

        // Use a differently-sized doc to force a different slot
        let doc2 = make_large_doc(200);
        let rid2 = su.update(rid1, &doc2).unwrap();

        // Old rid should be gone (different slot due to size difference)
        assert_ne!(rid1, rid2);
        assert_eq!(su.find(rid1).unwrap_err(), SdbError::NotFound);
        // New rid should have updated value
        let found = su.find(rid2).unwrap();
        assert_eq!(found.get("data").unwrap(), doc2.get("data").unwrap());
        assert_eq!(su.total_records(), 1);
    }

    #[test]
    fn update_same_size_reuses_slot() {
        let su = StorageUnit::new(1, 1);
        let doc1 = make_doc("v", "1");
        let rid1 = su.insert(&doc1).unwrap();

        // Same-size doc reuses the slot (rid may be the same)
        let doc2 = make_doc("v", "2");
        let rid2 = su.update(rid1, &doc2).unwrap();

        let found = su.find(rid2).unwrap();
        assert_eq!(found.get("v").unwrap(), &Value::String("2".into()));
        assert_eq!(su.total_records(), 1);
    }

    #[test]
    fn extent_overflow_allocates_new() {
        let su = StorageUnit::new(1, 1);
        // Fill up pages with large records to force multiple extents
        let mut rids = Vec::new();
        for _ in 0..10 {
            // ~16KB record should fill a 64KB page in ~4 records
            let doc = make_large_doc(16000);
            rids.push(su.insert(&doc).unwrap());
        }
        assert!(su.extent_count() > 1);

        // All records should be readable
        for rid in &rids {
            su.find(*rid).unwrap();
        }
    }

    #[test]
    fn delete_and_reuse() {
        let su = StorageUnit::new(1, 1);
        let doc1 = make_doc("a", "1");
        let rid1 = su.insert(&doc1).unwrap();
        let ext_before = su.extent_count();

        // Delete, then insert a same-size doc — should reuse the slot
        su.delete(rid1).unwrap();
        let doc2 = make_doc("a", "2");
        let rid2 = su.insert(&doc2).unwrap();

        // Should not have allocated a new extent
        assert_eq!(su.extent_count(), ext_before);
        // Reused slot should be on the same extent
        assert_eq!(rid2.extent_id, rid1.extent_id);

        let found = su.find(rid2).unwrap();
        assert_eq!(found.get("a").unwrap(), &Value::String("2".into()));
    }

    #[test]
    fn concurrent_inserts() {
        use std::sync::Arc;
        use std::thread;

        let su = Arc::new(StorageUnit::new(1, 1));
        let mut handles = Vec::new();

        for t in 0..4 {
            let su = Arc::clone(&su);
            handles.push(thread::spawn(move || {
                let mut rids = Vec::new();
                for i in 0..50 {
                    let doc = make_doc("t", &format!("{}-{}", t, i));
                    rids.push(su.insert(&doc).unwrap());
                }
                rids
            }));
        }

        let all_rids: Vec<RecordId> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        assert_eq!(su.total_records(), 200);
        for rid in &all_rids {
            su.find(*rid).unwrap();
        }
    }
}
