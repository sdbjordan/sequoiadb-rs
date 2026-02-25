use std::cmp::Ordering;

use sdb_common::{RecordId, Result, SdbError};

use crate::cursor::IndexCursor;
use crate::definition::IndexDefinition;
use crate::key::{IndexKey, KeyRange};
use crate::node::{self, Page};

/// B+ tree index implementation backed by in-memory 64KB pages.
pub struct BTreeIndex {
    pub definition: IndexDefinition,
    pages: Vec<Page>,
    root_page: i32, // -1 = empty tree
    next_page_id: u32,
    directions: Vec<i8>,
}

impl BTreeIndex {
    pub fn new(definition: IndexDefinition) -> Self {
        let directions = definition.directions();
        Self {
            definition,
            pages: Vec::new(),
            root_page: -1,
            next_page_id: 0,
            directions,
        }
    }

    /// Allocate a new zeroed page, return its page id.
    fn alloc_page(&mut self) -> u32 {
        let id = self.next_page_id;
        self.next_page_id += 1;
        self.pages.push(node::new_page());
        id
    }

    fn page(&self, id: u32) -> &Page {
        &self.pages[id as usize]
    }

    fn page_mut(&mut self, id: u32) -> &mut Page {
        &mut self.pages[id as usize]
    }

    /// Compare key against the key at cell pointer index `idx` in the given page.
    fn cmp_key_at(&self, page_id: u32, idx: u16, key: &IndexKey) -> Result<Ordering> {
        let page = self.page(page_id);
        let cell_off = node::cell_pointer(page, idx);
        let cell_key = if node::node_type(page) == node::NODE_LEAF {
            let (k, _) = node::read_leaf_cell(page, cell_off)?;
            k
        } else {
            let (_, k) = node::read_internal_cell(page, cell_off)?;
            k
        };
        Ok(cell_key.cmp_with_directions(key, &self.directions))
    }

    /// Binary search within a node for the given key.
    /// For leaf: returns the index where key would be inserted (or found).
    /// For internal: returns the child index to follow.
    fn search_in_node(&self, page_id: u32, key: &IndexKey) -> Result<usize> {
        let count = node::key_count(self.page(page_id)) as usize;
        if count == 0 {
            return Ok(0);
        }

        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.cmp_key_at(page_id, mid as u16, key)? {
                Ordering::Less => lo = mid + 1,  // cell_key < search_key, go right
                _ => hi = mid,                   // cell_key >= search_key, go left (lower bound)
            }
        }
        Ok(lo)
    }

    /// Walk from root to the leaf that should contain `key`.
    fn find_leaf(&self, key: &IndexKey) -> Result<u32> {
        if self.root_page < 0 {
            return Err(SdbError::IndexNotFound);
        }
        let mut current = self.root_page as u32;
        loop {
            let page = self.page(current);
            if node::node_type(page) == node::NODE_LEAF {
                return Ok(current);
            }
            // Internal node: find child to follow
            let idx = self.search_in_node(current, key)?;
            let count = node::key_count(page) as usize;
            current = if idx >= count {
                // Go to right_child (rightmost pointer)
                node::right_child(page)
            } else {
                // Go to left_child of the cell at idx
                let cell_off = node::cell_pointer(page, idx as u16);
                let (left_child, _) = node::read_internal_cell(page, cell_off)?;
                left_child
            };
        }
    }

    /// Insert a key into a leaf page. May trigger split.
    fn insert_into_leaf(&mut self, page_id: u32, key: &IndexKey, rid: RecordId) -> Result<()> {
        let key_bytes = key.encode();

        // Check if there's enough space
        let needed = node::leaf_cell_size(&key_bytes);
        if node::available_space(self.page(page_id)) >= needed {
            // Find insertion position
            let idx = self.search_in_node(page_id, key)?;
            let page = self.page_mut(page_id);
            let cell_off = node::write_leaf_cell(page, &key_bytes, rid);
            node::insert_cell_pointer(page, idx as u16, cell_off);
            Ok(())
        } else {
            // Need to split
            self.split_leaf(page_id, key, &key_bytes, rid)
        }
    }

    /// Split a full leaf and insert the new key.
    fn split_leaf(
        &mut self,
        page_id: u32,
        new_key: &IndexKey,
        new_key_bytes: &[u8],
        new_rid: RecordId,
    ) -> Result<()> {
        // Collect all existing entries + the new one
        let old_count = node::key_count(self.page(page_id)) as usize;
        let old_right_sib = node::right_sibling(self.page(page_id));
        let parent_id = node::parent(self.page(page_id));

        let mut entries: Vec<(Vec<u8>, RecordId)> = Vec::with_capacity(old_count + 1);
        for i in 0..old_count {
            let cell_off = node::cell_pointer(self.page(page_id), i as u16);
            let (k, r) = node::read_leaf_cell(self.page(page_id), cell_off)?;
            entries.push((k.encode(), r));
        }

        // Find position for new entry
        let insert_pos = self.search_in_node(page_id, new_key)?;
        entries.insert(insert_pos, (new_key_bytes.to_vec(), new_rid));

        // Split point: left gets first half
        let split = entries.len() / 2;

        // Allocate new right leaf
        let new_page_id = self.alloc_page();
        node::init_node(self.page_mut(new_page_id), node::NODE_LEAF, parent_id);
        node::set_right_sibling(self.page_mut(new_page_id), old_right_sib);

        // Reinitialize left leaf
        node::init_node(self.page_mut(page_id), node::NODE_LEAF, parent_id);
        node::set_right_sibling(self.page_mut(page_id), new_page_id as i32);

        // Write left half
        for (i, (kb, rid)) in entries[..split].iter().enumerate() {
            let page = self.page_mut(page_id);
            let cell_off = node::write_leaf_cell(page, kb, *rid);
            node::insert_cell_pointer(page, i as u16, cell_off);
        }

        // Write right half
        for (i, (kb, rid)) in entries[split..].iter().enumerate() {
            let page = self.page_mut(new_page_id);
            let cell_off = node::write_leaf_cell(page, kb, *rid);
            node::insert_cell_pointer(page, i as u16, cell_off);
        }

        // Separator key = first key of right leaf
        let separator = IndexKey::decode(&entries[split].0)?;

        // Insert separator into parent
        self.insert_into_parent(page_id, &separator, new_page_id)
    }

    /// Insert a separator key into the parent after a child split.
    /// left_child and right_child are the two pages resulting from the split.
    fn insert_into_parent(
        &mut self,
        left_id: u32,
        separator: &IndexKey,
        right_id: u32,
    ) -> Result<()> {
        let parent_id = node::parent(self.page(left_id));

        if parent_id < 0 {
            // left_id was the root — create new root
            let new_root = self.alloc_page();
            node::init_node(self.page_mut(new_root), node::NODE_INTERNAL, -1);
            node::set_right_child(self.page_mut(new_root), right_id);

            let sep_bytes = separator.encode();
            let page = self.page_mut(new_root);
            let cell_off = node::write_internal_cell(page, &sep_bytes, left_id);
            node::insert_cell_pointer(page, 0, cell_off);

            // Update children's parent pointers
            node::set_parent(self.page_mut(left_id), new_root as i32);
            node::set_parent(self.page_mut(right_id), new_root as i32);

            self.root_page = new_root as i32;
            return Ok(());
        }

        let parent = parent_id as u32;
        let sep_bytes = separator.encode();
        let needed = node::internal_cell_size(&sep_bytes);

        if node::available_space(self.page(parent)) >= needed {
            // Find insertion position in parent
            let idx = self.search_in_node(parent, separator)?;
            let count = node::key_count(self.page(parent)) as usize;

            // The cell at `idx` currently has left_id as its left_child (or right_child if idx==count).
            // We need to insert the separator with left_child = left_id,
            // and the cell that was at idx should now point to right_id.
            // Actually: we insert a new cell with left_child pointing to left_id,
            // and the right side is handled by the existing structure.

            // The new cell at idx has left_child = left_id.
            // If idx < count, the cell at idx previously pointed to left_id as left_child;
            //   update it to point to right_id instead.
            // If idx == count, right_child previously was left_id; update to right_id.
            if idx < count {
                // Read the existing cell at idx, replace its left_child with right_id
                let cell_off = node::cell_pointer(self.page(parent), idx as u16);
                let (_, existing_key) = node::read_internal_cell(self.page(parent), cell_off)?;
                let existing_kb = existing_key.encode();

                // We'll rewrite by: insert new separator cell at idx, then fix up
                // Actually, simpler approach: the new separator goes at position idx,
                // with left_child = left_id. The old cell at idx keeps its left_child = right_id.
                // So we need to update old cell's left_child.
                self.update_internal_cell_left_child(parent, idx as u16, &existing_kb, right_id);
            } else {
                // idx == count, meaning separator > all keys in parent
                // right_child was the pointer that led to left_id
                node::set_right_child(self.page_mut(parent), right_id);
            }

            let page = self.page_mut(parent);
            let cell_off = node::write_internal_cell(page, &sep_bytes, left_id);
            node::insert_cell_pointer(page, idx as u16, cell_off);

            // Update right child's parent pointer
            node::set_parent(self.page_mut(right_id), parent as i32);

            Ok(())
        } else {
            // Split the internal node
            self.split_internal(parent, separator, &sep_bytes, left_id, right_id)
        }
    }

    /// Rewrite an internal cell's left_child pointer.
    /// Since cells are immutable blobs, we write a new cell and update the pointer.
    fn update_internal_cell_left_child(
        &mut self,
        page_id: u32,
        idx: u16,
        key_bytes: &[u8],
        new_left_child: u32,
    ) {
        let page = self.page_mut(page_id);
        let new_cell_off = node::write_internal_cell(page, key_bytes, new_left_child);
        node::set_cell_pointer(page, idx, new_cell_off);
    }

    /// Split a full internal node.
    fn split_internal(
        &mut self,
        page_id: u32,
        new_sep: &IndexKey,
        new_sep_bytes: &[u8],
        new_left_child: u32,
        new_right_child: u32,
    ) -> Result<()> {
        let old_count = node::key_count(self.page(page_id)) as usize;
        let parent_id = node::parent(self.page(page_id));
        let old_right_child = node::right_child(self.page(page_id));

        // Collect all entries: (left_child, key_bytes)
        // Plus the rightmost child pointer
        struct InternalEntry {
            left_child: u32,
            key_bytes: Vec<u8>,
        }

        let mut entries: Vec<InternalEntry> = Vec::with_capacity(old_count + 1);
        for i in 0..old_count {
            let cell_off = node::cell_pointer(self.page(page_id), i as u16);
            let (lc, k) = node::read_internal_cell(self.page(page_id), cell_off)?;
            entries.push(InternalEntry {
                left_child: lc,
                key_bytes: k.encode(),
            });
        }

        // Find position for new separator
        let insert_pos = self.search_in_node(page_id, new_sep)?;

        // The new entry: left_child = new_left_child
        // The entry that was at insert_pos (if any) gets its left_child replaced with new_right_child
        // Or if insert_pos == old_count, old_right_child becomes new_right_child
        let new_entry = InternalEntry {
            left_child: new_left_child,
            key_bytes: new_sep_bytes.to_vec(),
        };

        // If inserting before an existing entry, that entry's left_child should become new_right_child
        if insert_pos < entries.len() {
            entries[insert_pos].left_child = new_right_child;
        }
        entries.insert(insert_pos, new_entry);

        // rightmost child: if insert_pos was at end (== old_count), use new_right_child
        let final_right_child = if insert_pos == old_count {
            new_right_child
        } else {
            old_right_child
        };

        // Split: left gets [0..split), promote entries[split].key, right gets [split+1..)
        let split = entries.len() / 2;
        let promote_key = IndexKey::decode(&entries[split].key_bytes)?;

        // Allocate new right internal page
        let new_page_id = self.alloc_page();
        node::init_node(self.page_mut(new_page_id), node::NODE_INTERNAL, parent_id);

        // Reinitialize left page
        node::init_node(self.page_mut(page_id), node::NODE_INTERNAL, parent_id);

        // Left page: entries [0..split)
        // The right_child of left page = entries[split].left_child (the left pointer of promoted key)
        let left_right_child = entries[split].left_child;
        node::set_right_child(self.page_mut(page_id), left_right_child);
        for (i, entry) in entries[..split].iter().enumerate() {
            let page = self.page_mut(page_id);
            let cell_off = node::write_internal_cell(page, &entry.key_bytes, entry.left_child);
            node::insert_cell_pointer(page, i as u16, cell_off);
        }

        // Right page: entries [split+1..)
        // right_child of right page = final_right_child
        node::set_right_child(self.page_mut(new_page_id), final_right_child);
        for (i, entry) in entries[split + 1..].iter().enumerate() {
            let page = self.page_mut(new_page_id);
            let cell_off = node::write_internal_cell(page, &entry.key_bytes, entry.left_child);
            node::insert_cell_pointer(page, i as u16, cell_off);
        }

        // Update parent pointers of all children in both pages
        self.update_children_parent(page_id);
        self.update_children_parent(new_page_id);

        // Promote separator to parent
        self.insert_into_parent(page_id, &promote_key, new_page_id)
    }

    /// Update parent pointers for all children of an internal node.
    fn update_children_parent(&mut self, page_id: u32) {
        let count = node::key_count(self.page(page_id)) as usize;
        let rc = node::right_child(self.page(page_id));

        // Collect child ids first to avoid borrow issues
        let mut children = Vec::with_capacity(count + 1);
        for i in 0..count {
            let cell_off = node::cell_pointer(self.page(page_id), i as u16);
            let lc = u32::from_le_bytes([
                self.page(page_id)[cell_off as usize],
                self.page(page_id)[cell_off as usize + 1],
                self.page(page_id)[cell_off as usize + 2],
                self.page(page_id)[cell_off as usize + 3],
            ]);
            children.push(lc);
        }
        children.push(rc);

        for child_id in children {
            node::set_parent(self.page_mut(child_id), page_id as i32);
        }
    }

    /// Ensure tree has a root. Creates one if empty.
    fn ensure_root(&mut self) {
        if self.root_page < 0 {
            let root = self.alloc_page();
            node::init_node(self.page_mut(root), node::NODE_LEAF, -1);
            self.root_page = root as i32;
        }
    }

    /// Find all leaf cells matching the exact key, collecting their RecordIds.
    fn find_exact(&self, key: &IndexKey) -> Result<Vec<RecordId>> {
        if self.root_page < 0 {
            return Ok(Vec::new());
        }
        let leaf_id = self.find_leaf(key)?;
        let page = self.page(leaf_id);
        let count = node::key_count(page) as usize;
        let idx = self.search_in_node(leaf_id, key)?;

        let mut results = Vec::new();
        // Scan forward from idx while keys match (there could be duplicates)
        let mut cur_page_id = leaf_id;
        let mut cur_idx = idx;
        loop {
            let pg = self.page(cur_page_id);
            let cnt = node::key_count(pg) as usize;
            if cur_idx >= cnt {
                // Move to right sibling
                let sib = node::right_sibling(pg);
                if sib < 0 {
                    break;
                }
                cur_page_id = sib as u32;
                cur_idx = 0;
                continue;
            }
            let cell_off = node::cell_pointer(pg, cur_idx as u16);
            let (cell_key, rid) = node::read_leaf_cell(pg, cell_off)?;
            match cell_key.cmp_with_directions(key, &self.directions) {
                Ordering::Equal => {
                    results.push(rid);
                    cur_idx += 1;
                }
                _ => break,
            }
        }
        let _ = count; // suppress unused warning
        Ok(results)
    }

    /// Collect leaf entries in a range for scan.
    fn scan_range(&self, range: &KeyRange) -> Result<Vec<RecordId>> {
        if self.root_page < 0 {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();

        // Determine starting leaf and position
        let (start_page, start_idx) = if let Some(ref start_key) = range.start {
            let leaf = self.find_leaf(start_key)?;
            let idx = self.search_in_node(leaf, start_key)?;
            (leaf, idx)
        } else {
            // Start from leftmost leaf
            let leftmost = self.find_leftmost_leaf()?;
            (leftmost, 0)
        };

        let mut cur_page_id = start_page;
        let mut cur_idx = start_idx;

        loop {
            let pg = self.page(cur_page_id);
            let cnt = node::key_count(pg) as usize;
            if cur_idx >= cnt {
                let sib = node::right_sibling(pg);
                if sib < 0 {
                    break;
                }
                cur_page_id = sib as u32;
                cur_idx = 0;
                continue;
            }

            let cell_off = node::cell_pointer(pg, cur_idx as u16);
            let (cell_key, rid) = node::read_leaf_cell(pg, cell_off)?;

            // Check start bound
            if let Some(ref start_key) = range.start {
                let cmp = cell_key.cmp_with_directions(start_key, &self.directions);
                if cmp == Ordering::Less
                    || (cmp == Ordering::Equal && !range.start_inclusive)
                {
                    cur_idx += 1;
                    continue;
                }
            }

            // Check end bound
            if let Some(ref end_key) = range.end {
                let cmp = cell_key.cmp_with_directions(end_key, &self.directions);
                if cmp == Ordering::Greater
                    || (cmp == Ordering::Equal && !range.end_inclusive)
                {
                    break;
                }
            }

            results.push(rid);
            cur_idx += 1;
        }

        Ok(results)
    }

    /// Find the leftmost leaf by always following the first child from root.
    fn find_leftmost_leaf(&self) -> Result<u32> {
        let mut current = self.root_page as u32;
        loop {
            let page = self.page(current);
            if node::node_type(page) == node::NODE_LEAF {
                return Ok(current);
            }
            let count = node::key_count(page);
            if count == 0 {
                // Empty internal node, follow right_child
                current = node::right_child(page);
            } else {
                let cell_off = node::cell_pointer(page, 0);
                let (left_child, _) = node::read_internal_cell(page, cell_off)?;
                current = left_child;
            }
        }
    }
}

impl crate::Index for BTreeIndex {
    fn insert(&mut self, key: &IndexKey, rid: RecordId) -> Result<()> {
        self.ensure_root();

        // Unique constraint check
        if self.definition.unique {
            let existing = self.find_exact(key)?;
            if !existing.is_empty() {
                return Err(SdbError::DuplicateKey);
            }
        }

        let leaf_id = self.find_leaf(key)?;
        self.insert_into_leaf(leaf_id, key, rid)
    }

    fn delete(&mut self, key: &IndexKey, rid: RecordId) -> Result<()> {
        if self.root_page < 0 {
            return Ok(());
        }
        let leaf_id = self.find_leaf(key)?;
        let page = self.page(leaf_id);
        let count = node::key_count(page) as usize;

        // Find the matching cell
        let start = self.search_in_node(leaf_id, key)?;
        for i in start..count {
            let cell_off = node::cell_pointer(self.page(leaf_id), i as u16);
            let (cell_key, cell_rid) = node::read_leaf_cell(self.page(leaf_id), cell_off)?;
            if cell_key.cmp_with_directions(key, &self.directions) != Ordering::Equal {
                break;
            }
            if cell_rid == rid {
                node::remove_cell_pointer(self.page_mut(leaf_id), i as u16);
                return Ok(());
            }
        }
        Ok(()) // Not found — no-op (v1: no error on missing delete)
    }

    fn find(&self, key: &IndexKey) -> Result<Option<RecordId>> {
        let results = self.find_exact(key)?;
        Ok(results.into_iter().next())
    }

    fn scan(&self, range: &KeyRange) -> Result<IndexCursor> {
        let results = self.scan_range(range)?;
        Ok(IndexCursor::new(results))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sdb_bson::element::Value;
    use sdb_bson::Document;

    fn make_index(unique: bool) -> BTreeIndex {
        let mut kp = Document::new();
        kp.insert("a", Value::Int32(1));
        let def = IndexDefinition {
            name: "idx_a".to_string(),
            key_pattern: kp,
            unique,
            enforced: false,
            not_null: false,
        };
        BTreeIndex::new(def)
    }

    fn make_compound_index() -> BTreeIndex {
        let mut kp = Document::new();
        kp.insert("a", Value::Int32(1));
        kp.insert("b", Value::Int32(-1));
        let def = IndexDefinition {
            name: "idx_ab".to_string(),
            key_pattern: kp,
            unique: false,
            enforced: false,
            not_null: false,
        };
        BTreeIndex::new(def)
    }

    fn key1(v: i32) -> IndexKey {
        IndexKey::new(vec![Value::Int32(v)])
    }

    fn rid(e: u32, o: u32) -> RecordId {
        RecordId {
            extent_id: e,
            offset: o,
        }
    }

    #[test]
    fn test_insert_and_find_single() {
        use crate::Index;
        let mut idx = make_index(false);
        idx.insert(&key1(42), rid(1, 0)).unwrap();
        let found = idx.find(&key1(42)).unwrap();
        assert_eq!(found, Some(rid(1, 0)));
    }

    #[test]
    fn test_find_nonexistent() {
        use crate::Index;
        let mut idx = make_index(false);
        idx.insert(&key1(10), rid(1, 0)).unwrap();
        let found = idx.find(&key1(99)).unwrap();
        assert_eq!(found, None);
    }

    #[test]
    fn test_insert_multiple_and_find() {
        use crate::Index;
        let mut idx = make_index(false);
        for i in 0..100 {
            idx.insert(&key1(i), rid(0, i as u32)).unwrap();
        }
        for i in 0..100 {
            let found = idx.find(&key1(i)).unwrap();
            assert_eq!(found, Some(rid(0, i as u32)), "key={}", i);
        }
    }

    #[test]
    fn test_insert_reverse_order() {
        use crate::Index;
        let mut idx = make_index(false);
        for i in (0..100).rev() {
            idx.insert(&key1(i), rid(0, i as u32)).unwrap();
        }
        for i in 0..100 {
            assert_eq!(idx.find(&key1(i)).unwrap(), Some(rid(0, i as u32)));
        }
    }

    #[test]
    fn test_unique_constraint() {
        use crate::Index;
        let mut idx = make_index(true);
        idx.insert(&key1(1), rid(0, 0)).unwrap();
        let err = idx.insert(&key1(1), rid(0, 1));
        assert_eq!(err.unwrap_err(), SdbError::DuplicateKey);
    }

    #[test]
    fn test_delete() {
        use crate::Index;
        let mut idx = make_index(false);
        idx.insert(&key1(5), rid(0, 5)).unwrap();
        idx.insert(&key1(10), rid(0, 10)).unwrap();
        idx.delete(&key1(5), rid(0, 5)).unwrap();
        assert_eq!(idx.find(&key1(5)).unwrap(), None);
        assert_eq!(idx.find(&key1(10)).unwrap(), Some(rid(0, 10)));
    }

    #[test]
    fn test_scan_full_range() {
        use crate::Index;
        let mut idx = make_index(false);
        for i in 0..10 {
            idx.insert(&key1(i), rid(0, i as u32)).unwrap();
        }
        let range = KeyRange {
            start: None,
            end: None,
            start_inclusive: true,
            end_inclusive: true,
        };
        let mut cursor = idx.scan(&range).unwrap();
        let mut count = 0;
        while cursor.next().is_some() {
            count += 1;
        }
        assert_eq!(count, 10);
    }

    #[test]
    fn test_scan_bounded_range() {
        use crate::Index;
        let mut idx = make_index(false);
        for i in 0..20 {
            idx.insert(&key1(i), rid(0, i as u32)).unwrap();
        }
        let range = KeyRange {
            start: Some(key1(5)),
            end: Some(key1(10)),
            start_inclusive: true,
            end_inclusive: false,
        };
        let mut cursor = idx.scan(&range).unwrap();
        let mut results = Vec::new();
        while let Some(r) = cursor.next() {
            results.push(r);
        }
        // Should get keys 5,6,7,8,9 (10 exclusive)
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_large_insert_triggers_splits() {
        use crate::Index;
        let mut idx = make_index(false);
        // Insert enough keys to trigger multiple levels of splits
        let n = 5000;
        for i in 0..n {
            idx.insert(&key1(i), rid(0, i as u32)).unwrap();
        }
        // Verify all keys are findable
        for i in 0..n {
            assert_eq!(
                idx.find(&key1(i)).unwrap(),
                Some(rid(0, i as u32)),
                "missing key={}",
                i
            );
        }
        // Tree should have multiple pages
        assert!(idx.pages.len() > 1, "expected multiple pages after {} inserts", n);
    }

    #[test]
    fn test_delete_then_find() {
        use crate::Index;
        let mut idx = make_index(false);
        for i in 0..50 {
            idx.insert(&key1(i), rid(0, i as u32)).unwrap();
        }
        // Delete even keys
        for i in (0..50).step_by(2) {
            idx.delete(&key1(i), rid(0, i as u32)).unwrap();
        }
        // Odd keys should still exist
        for i in (1..50).step_by(2) {
            assert_eq!(idx.find(&key1(i)).unwrap(), Some(rid(0, i as u32)));
        }
        // Even keys should be gone
        for i in (0..50).step_by(2) {
            assert_eq!(idx.find(&key1(i)).unwrap(), None);
        }
    }

    #[test]
    fn test_compound_key() {
        use crate::Index;
        let mut idx = make_compound_index();
        // Compound key: (a ASC, b DESC)
        let k1 = IndexKey::new(vec![Value::Int32(1), Value::Int32(10)]);
        let k2 = IndexKey::new(vec![Value::Int32(1), Value::Int32(20)]);
        let k3 = IndexKey::new(vec![Value::Int32(2), Value::Int32(5)]);

        idx.insert(&k1, rid(0, 1)).unwrap();
        idx.insert(&k2, rid(0, 2)).unwrap();
        idx.insert(&k3, rid(0, 3)).unwrap();

        // Scan all — should be ordered by (a ASC, b DESC)
        // So: (1,20), (1,10), (2,5)
        let range = KeyRange {
            start: None,
            end: None,
            start_inclusive: true,
            end_inclusive: true,
        };
        let mut cursor = idx.scan(&range).unwrap();
        let mut results = Vec::new();
        while let Some(r) = cursor.next() {
            results.push(r);
        }
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], rid(0, 2)); // (1, 20) — b=20 first due to DESC
        assert_eq!(results[1], rid(0, 1)); // (1, 10)
        assert_eq!(results[2], rid(0, 3)); // (2, 5)
    }

    #[test]
    fn test_duplicate_keys_non_unique() {
        use crate::Index;
        let mut idx = make_index(false);
        idx.insert(&key1(5), rid(0, 1)).unwrap();
        idx.insert(&key1(5), rid(0, 2)).unwrap();
        idx.insert(&key1(5), rid(0, 3)).unwrap();

        // find returns first match
        let found = idx.find(&key1(5)).unwrap();
        assert!(found.is_some());

        // scan should return all 3
        let range = KeyRange {
            start: Some(key1(5)),
            end: Some(key1(5)),
            start_inclusive: true,
            end_inclusive: true,
        };
        let mut cursor = idx.scan(&range).unwrap();
        let mut count = 0;
        while cursor.next().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn test_scan_empty_index() {
        use crate::Index;
        let idx = make_index(false);
        let range = KeyRange {
            start: None,
            end: None,
            start_inclusive: true,
            end_inclusive: true,
        };
        let cursor = idx.scan(&range).unwrap();
        assert!(!cursor.has_more());
    }

    #[test]
    fn test_string_keys() {
        use crate::Index;
        let mut idx = make_index(false);
        let keys = ["alice", "bob", "charlie", "dave", "eve"];
        for (i, name) in keys.iter().enumerate() {
            let k = IndexKey::new(vec![Value::String(name.to_string())]);
            idx.insert(&k, rid(0, i as u32)).unwrap();
        }
        for (i, name) in keys.iter().enumerate() {
            let k = IndexKey::new(vec![Value::String(name.to_string())]);
            assert_eq!(idx.find(&k).unwrap(), Some(rid(0, i as u32)));
        }
    }

    #[test]
    fn test_find_on_empty_tree() {
        use crate::Index;
        let idx = make_index(false);
        assert_eq!(idx.find(&key1(1)).unwrap(), None);
    }

    #[test]
    fn test_delete_nonexistent() {
        use crate::Index;
        let mut idx = make_index(false);
        idx.insert(&key1(1), rid(0, 0)).unwrap();
        // Delete non-existent key — should be no-op
        idx.delete(&key1(99), rid(0, 0)).unwrap();
        assert_eq!(idx.find(&key1(1)).unwrap(), Some(rid(0, 0)));
    }
}
