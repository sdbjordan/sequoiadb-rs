use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use sdb_bson::Document;
use sdb_common::{CollectionId, CollectionSpaceId, GroupId, RecordId, Result, SdbError};
use sdb_dms::StorageUnit;
use sdb_ixm::definition::IndexDefinition;
use sdb_ixm::{BTreeIndex, Index};
use sdb_opt::CollectionStats;

use crate::metadata::{
    CollectionMeta, CollectionSpaceMeta, GroupMeta, IndexMeta,
};

/// Runtime state for a single collection: storage engine + live indexes.
struct CollectionRuntime {
    storage: Arc<StorageUnit>,
    indexes: Vec<Arc<RwLock<BTreeIndex>>>,
}

/// Manages cluster-wide catalog metadata and runtime objects.
pub struct CatalogManager {
    spaces: HashMap<String, CollectionSpaceMeta>,
    groups: HashMap<GroupId, GroupMeta>,
    runtimes: HashMap<String, CollectionRuntime>, // key = "cs.cl"
    next_cs_id: CollectionSpaceId,
    next_cl_id: HashMap<CollectionSpaceId, CollectionId>,
}

impl CatalogManager {
    pub fn new() -> Self {
        Self {
            spaces: HashMap::new(),
            groups: HashMap::new(),
            runtimes: HashMap::new(),
            next_cs_id: 1,
            next_cl_id: HashMap::new(),
        }
    }

    // ── helpers ──────────────────────────────────────────────────────

    fn rt_key(cs: &str, cl: &str) -> String {
        format!("{}.{}", cs, cl)
    }

    fn alloc_cs_id(&mut self) -> CollectionSpaceId {
        let id = self.next_cs_id;
        self.next_cs_id += 1;
        id
    }

    fn alloc_cl_id(&mut self, cs_id: CollectionSpaceId) -> CollectionId {
        let entry = self.next_cl_id.entry(cs_id).or_insert(1);
        let id = *entry;
        *entry += 1;
        id
    }

    // ── DDL: Collection Space ───────────────────────────────────────

    pub fn create_collection_space(&mut self, name: &str) -> Result<CollectionSpaceId> {
        if self.spaces.contains_key(name) {
            return Err(SdbError::CollectionSpaceAlreadyExists);
        }
        let id = self.alloc_cs_id();
        self.spaces.insert(
            name.to_string(),
            CollectionSpaceMeta {
                id,
                name: name.to_string(),
                page_size: 65536,
                collections: Vec::new(),
            },
        );
        Ok(id)
    }

    pub fn drop_collection_space(&mut self, name: &str) -> Result<()> {
        let cs = self
            .spaces
            .remove(name)
            .ok_or(SdbError::CollectionSpaceNotFound)?;
        // Remove runtimes for all collections in this space.
        for cl in &cs.collections {
            self.runtimes.remove(&Self::rt_key(name, &cl.name));
        }
        Ok(())
    }

    // ── DDL: Collection ─────────────────────────────────────────────

    pub fn create_collection(&mut self, cs: &str, cl: &str) -> Result<()> {
        let space = self
            .spaces
            .get(cs)
            .ok_or(SdbError::CollectionSpaceNotFound)?;
        if space.collections.iter().any(|c| c.name == cl) {
            return Err(SdbError::CollectionAlreadyExists);
        }
        let cs_id = space.id;
        let cl_id = self.alloc_cl_id(cs_id);
        let space = self.spaces.get_mut(cs).unwrap();
        space.collections.push(CollectionMeta {
            cs_id,
            cl_id,
            name: cl.to_string(),
            shard_key: None,
            replicated: false,
            groups: Vec::new(),
            indexes: Vec::new(),
        });
        self.runtimes.insert(
            Self::rt_key(cs, cl),
            CollectionRuntime {
                storage: Arc::new(StorageUnit::new(cs_id, cl_id)),
                indexes: Vec::new(),
            },
        );
        Ok(())
    }

    pub fn drop_collection(&mut self, cs: &str, cl: &str) -> Result<()> {
        let space = self
            .spaces
            .get_mut(cs)
            .ok_or(SdbError::CollectionSpaceNotFound)?;
        let pos = space
            .collections
            .iter()
            .position(|c| c.name == cl)
            .ok_or(SdbError::CollectionNotFound)?;
        space.collections.remove(pos);
        self.runtimes.remove(&Self::rt_key(cs, cl));
        Ok(())
    }

    // ── DDL: Index ──────────────────────────────────────────────────

    pub fn create_index(
        &mut self,
        cs: &str,
        cl: &str,
        name: &str,
        key_pattern: Document,
        unique: bool,
    ) -> Result<()> {
        // Validate collection exists and check duplicate index name.
        let cl_meta = self.get_collection_mut(cs, cl)?;
        if cl_meta.indexes.iter().any(|i| i.name == name) {
            return Err(SdbError::IndexAlreadyExists);
        }

        // Build the BTreeIndex and populate from existing data.
        let def = IndexDefinition {
            name: name.to_string(),
            key_pattern: key_pattern.clone(),
            unique,
            enforced: false,
            not_null: false,
        };
        let mut idx = BTreeIndex::new(def);

        let key = Self::rt_key(cs, cl);
        let rt = self.runtimes.get(&key).ok_or(SdbError::CollectionNotFound)?;
        let rows = rt.storage.scan();
        for (rid, doc) in &rows {
            let ik = sdb_ixm::IndexDefinition::extract_key(&idx.definition, doc);
            idx.insert(&ik, *rid)?;
        }

        // Register in metadata.
        let cl_meta = self.get_collection_mut(cs, cl)?;
        cl_meta.indexes.push(IndexMeta {
            name: name.to_string(),
            key_pattern,
            unique,
            enforced: false,
            not_null: false,
        });

        // Register in runtime.
        let rt = self
            .runtimes
            .get_mut(&key)
            .ok_or(SdbError::CollectionNotFound)?;
        rt.indexes.push(Arc::new(RwLock::new(idx)));
        Ok(())
    }

    pub fn drop_index(&mut self, cs: &str, cl: &str, index_name: &str) -> Result<()> {
        let cl_meta = self.get_collection_mut(cs, cl)?;
        let pos = cl_meta
            .indexes
            .iter()
            .position(|i| i.name == index_name)
            .ok_or(SdbError::IndexNotFound)?;
        cl_meta.indexes.remove(pos);

        let key = Self::rt_key(cs, cl);
        let rt = self
            .runtimes
            .get_mut(&key)
            .ok_or(SdbError::CollectionNotFound)?;
        let pos = rt
            .indexes
            .iter()
            .position(|i| i.read().unwrap().definition.name == index_name)
            .ok_or(SdbError::IndexNotFound)?;
        rt.indexes.remove(pos);
        Ok(())
    }

    // ── Query ───────────────────────────────────────────────────────

    pub fn get_collection_space(&self, name: &str) -> Result<&CollectionSpaceMeta> {
        self.spaces
            .get(name)
            .ok_or(SdbError::CollectionSpaceNotFound)
    }

    pub fn get_collection(&self, cs: &str, cl: &str) -> Result<&CollectionMeta> {
        let space = self.get_collection_space(cs)?;
        space
            .collections
            .iter()
            .find(|c| c.name == cl)
            .ok_or(SdbError::CollectionNotFound)
    }

    fn get_collection_mut(&mut self, cs: &str, cl: &str) -> Result<&mut CollectionMeta> {
        let space = self
            .spaces
            .get_mut(cs)
            .ok_or(SdbError::CollectionSpaceNotFound)?;
        space
            .collections
            .iter_mut()
            .find(|c| c.name == cl)
            .ok_or(SdbError::CollectionNotFound)
    }

    pub fn list_collection_spaces(&self) -> Vec<String> {
        self.spaces.keys().cloned().collect()
    }

    pub fn list_collections(&self, cs: &str) -> Result<Vec<String>> {
        let space = self.get_collection_space(cs)?;
        Ok(space.collections.iter().map(|c| c.name.clone()).collect())
    }

    // ── Bridge ──────────────────────────────────────────────────────

    pub fn collection_handle(
        &self,
        cs: &str,
        cl: &str,
    ) -> Result<(Arc<StorageUnit>, Vec<Arc<RwLock<BTreeIndex>>>)> {
        let key = Self::rt_key(cs, cl);
        let rt = self.runtimes.get(&key).ok_or(SdbError::CollectionNotFound)?;
        Ok((Arc::clone(&rt.storage), rt.indexes.clone()))
    }

    pub fn collection_stats(&self, cs: &str, cl: &str) -> Result<CollectionStats> {
        let key = Self::rt_key(cs, cl);
        let rt = self.runtimes.get(&key).ok_or(SdbError::CollectionNotFound)?;
        let definitions: Vec<IndexDefinition> = rt
            .indexes
            .iter()
            .map(|i| i.read().unwrap().definition.clone())
            .collect();
        Ok(CollectionStats {
            total_records: rt.storage.total_records(),
            extent_count: rt.storage.extent_count(),
            indexes: definitions,
        })
    }

    // ── DML ─────────────────────────────────────────────────────────

    pub fn insert_document(&self, cs: &str, cl: &str, doc: &Document) -> Result<RecordId> {
        let key = Self::rt_key(cs, cl);
        let rt = self.runtimes.get(&key).ok_or(SdbError::CollectionNotFound)?;

        let rid = rt.storage.insert(doc)?;

        // Maintain indexes; rollback storage on unique conflict.
        for (i, idx_lock) in rt.indexes.iter().enumerate() {
            let mut idx = idx_lock.write().unwrap();
            let ik = IndexDefinition::extract_key(&idx.definition, doc);
            if let Err(e) = idx.insert(&ik, rid) {
                // Undo already-inserted index entries.
                drop(idx);
                for prev_lock in &rt.indexes[..i] {
                    let mut prev = prev_lock.write().unwrap();
                    let pk = IndexDefinition::extract_key(&prev.definition, doc);
                    let _ = prev.delete(&pk, rid);
                }
                let _ = rt.storage.delete(rid);
                return Err(e);
            }
        }
        Ok(rid)
    }

    pub fn delete_document(&self, cs: &str, cl: &str, rid: RecordId) -> Result<()> {
        let key = Self::rt_key(cs, cl);
        let rt = self.runtimes.get(&key).ok_or(SdbError::CollectionNotFound)?;

        let old_doc = rt.storage.find(rid)?;

        // Remove from indexes first.
        for idx_lock in &rt.indexes {
            let mut idx = idx_lock.write().unwrap();
            let ik = IndexDefinition::extract_key(&idx.definition, &old_doc);
            let _ = idx.delete(&ik, rid);
        }
        rt.storage.delete(rid)?;
        Ok(())
    }

    pub fn update_document(
        &self,
        cs: &str,
        cl: &str,
        rid: RecordId,
        new_doc: &Document,
    ) -> Result<RecordId> {
        let key = Self::rt_key(cs, cl);
        let rt = self.runtimes.get(&key).ok_or(SdbError::CollectionNotFound)?;

        let old_doc = rt.storage.find(rid)?;

        // Remove old index entries.
        for idx_lock in &rt.indexes {
            let mut idx = idx_lock.write().unwrap();
            let old_key = IndexDefinition::extract_key(&idx.definition, &old_doc);
            let _ = idx.delete(&old_key, rid);
        }

        let new_rid = rt.storage.update(rid, new_doc)?;

        // Insert new index entries.
        for idx_lock in &rt.indexes {
            let mut idx = idx_lock.write().unwrap();
            let new_key = IndexDefinition::extract_key(&idx.definition, new_doc);
            idx.insert(&new_key, new_rid)?;
        }
        Ok(new_rid)
    }

    // ── Groups ──────────────────────────────────────────────────────

    pub fn get_group(&self, id: GroupId) -> Result<&GroupMeta> {
        self.groups
            .get(&id)
            .ok_or(SdbError::NodeNotFound)
    }

    pub fn register_group(&mut self, meta: GroupMeta) -> Result<()> {
        self.groups.insert(meta.id, meta);
        Ok(())
    }
}

impl Default for CatalogManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sdb_bson::Value;

    fn doc_with(pairs: &[(&str, Value)]) -> Document {
        let mut d = Document::new();
        for (k, v) in pairs {
            d.insert(*k, v.clone());
        }
        d
    }

    // ── CS tests ────────────────────────────────────────────────────

    #[test]
    fn create_and_get_cs() {
        let mut cat = CatalogManager::new();
        let id = cat.create_collection_space("mycs").unwrap();
        let cs = cat.get_collection_space("mycs").unwrap();
        assert_eq!(cs.id, id);
        assert_eq!(cs.name, "mycs");
    }

    #[test]
    fn duplicate_cs_errors() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("mycs").unwrap();
        assert_eq!(
            cat.create_collection_space("mycs").unwrap_err(),
            SdbError::CollectionSpaceAlreadyExists
        );
    }

    #[test]
    fn drop_cs() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("mycs").unwrap();
        cat.drop_collection_space("mycs").unwrap();
        assert_eq!(
            cat.get_collection_space("mycs").unwrap_err(),
            SdbError::CollectionSpaceNotFound
        );
    }

    #[test]
    fn drop_nonexistent_cs_errors() {
        let mut cat = CatalogManager::new();
        assert_eq!(
            cat.drop_collection_space("nope").unwrap_err(),
            SdbError::CollectionSpaceNotFound
        );
    }

    #[test]
    fn list_collection_spaces() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("a").unwrap();
        cat.create_collection_space("b").unwrap();
        let mut names = cat.list_collection_spaces();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
    }

    // ── CL tests ────────────────────────────────────────────────────

    #[test]
    fn create_and_get_cl() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "cl").unwrap();
        let cl = cat.get_collection("cs", "cl").unwrap();
        assert_eq!(cl.name, "cl");
    }

    #[test]
    fn cl_in_nonexistent_cs_errors() {
        let mut cat = CatalogManager::new();
        assert_eq!(
            cat.create_collection("nope", "cl").unwrap_err(),
            SdbError::CollectionSpaceNotFound
        );
    }

    #[test]
    fn duplicate_cl_errors() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "cl").unwrap();
        assert_eq!(
            cat.create_collection("cs", "cl").unwrap_err(),
            SdbError::CollectionAlreadyExists
        );
    }

    #[test]
    fn drop_cl() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "cl").unwrap();
        cat.drop_collection("cs", "cl").unwrap();
        assert_eq!(
            cat.get_collection("cs", "cl").unwrap_err(),
            SdbError::CollectionNotFound
        );
    }

    #[test]
    fn drop_cs_cascades_cl() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "cl1").unwrap();
        cat.create_collection("cs", "cl2").unwrap();
        cat.drop_collection_space("cs").unwrap();
        // Runtimes should be cleaned up.
        assert!(cat.runtimes.get("cs.cl1").is_none());
        assert!(cat.runtimes.get("cs.cl2").is_none());
    }

    #[test]
    fn list_collections() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "a").unwrap();
        cat.create_collection("cs", "b").unwrap();
        let mut names = cat.list_collections("cs").unwrap();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
    }

    // ── Index tests ─────────────────────────────────────────────────

    #[test]
    fn create_index_on_empty_collection() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "cl").unwrap();

        let kp = doc_with(&[("x", Value::Int32(1))]);
        cat.create_index("cs", "cl", "idx_x", kp, false).unwrap();

        let cl = cat.get_collection("cs", "cl").unwrap();
        assert_eq!(cl.indexes.len(), 1);
        assert_eq!(cl.indexes[0].name, "idx_x");
    }

    #[test]
    fn create_index_populates_existing_data() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "cl").unwrap();

        // Insert some documents first.
        for i in 0..5 {
            cat.insert_document("cs", "cl", &doc_with(&[("x", Value::Int32(i))]))
                .unwrap();
        }

        let kp = doc_with(&[("x", Value::Int32(1))]);
        cat.create_index("cs", "cl", "idx_x", kp, false).unwrap();

        // Verify index has entries by scanning.
        let rt = cat.runtimes.get("cs.cl").unwrap();
        let idx = rt.indexes[0].read().unwrap();
        let mut cursor = idx.scan(&sdb_ixm::KeyRange::all()).unwrap();
        let mut count = 0;
        while cursor.next().is_some() {
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[test]
    fn create_index_unique_conflict() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "cl").unwrap();

        // Insert duplicate values.
        cat.insert_document("cs", "cl", &doc_with(&[("x", Value::Int32(1))]))
            .unwrap();
        cat.insert_document("cs", "cl", &doc_with(&[("x", Value::Int32(1))]))
            .unwrap();

        let kp = doc_with(&[("x", Value::Int32(1))]);
        let err = cat.create_index("cs", "cl", "idx_x", kp, true).unwrap_err();
        assert_eq!(err, SdbError::DuplicateKey);
    }

    #[test]
    fn duplicate_index_name_errors() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "cl").unwrap();

        let kp = doc_with(&[("x", Value::Int32(1))]);
        cat.create_index("cs", "cl", "idx_x", kp.clone(), false)
            .unwrap();
        assert_eq!(
            cat.create_index("cs", "cl", "idx_x", kp, false).unwrap_err(),
            SdbError::IndexAlreadyExists
        );
    }

    #[test]
    fn drop_index() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "cl").unwrap();

        let kp = doc_with(&[("x", Value::Int32(1))]);
        cat.create_index("cs", "cl", "idx_x", kp, false).unwrap();
        cat.drop_index("cs", "cl", "idx_x").unwrap();

        let cl = cat.get_collection("cs", "cl").unwrap();
        assert!(cl.indexes.is_empty());
    }

    // ── DML tests ───────────────────────────────────────────────────

    #[test]
    fn insert_maintains_index() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "cl").unwrap();

        let kp = doc_with(&[("x", Value::Int32(1))]);
        cat.create_index("cs", "cl", "idx_x", kp, false).unwrap();

        let rid = cat
            .insert_document("cs", "cl", &doc_with(&[("x", Value::Int32(42))]))
            .unwrap();

        // Verify index can find it.
        let rt = cat.runtimes.get("cs.cl").unwrap();
        let idx = rt.indexes[0].read().unwrap();
        let key = IndexDefinition::extract_key(
            &idx.definition,
            &doc_with(&[("x", Value::Int32(42))]),
        );
        assert_eq!(idx.find(&key).unwrap(), Some(rid));
    }

    #[test]
    fn insert_unique_conflict_rollback() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "cl").unwrap();

        let kp = doc_with(&[("x", Value::Int32(1))]);
        cat.create_index("cs", "cl", "idx_x", kp, true).unwrap();

        cat.insert_document("cs", "cl", &doc_with(&[("x", Value::Int32(1))]))
            .unwrap();

        // Second insert with same value should fail.
        let err = cat
            .insert_document("cs", "cl", &doc_with(&[("x", Value::Int32(1))]))
            .unwrap_err();
        assert_eq!(err, SdbError::DuplicateKey);

        // Storage should have been rolled back — only 1 record total.
        let stats = cat.collection_stats("cs", "cl").unwrap();
        assert_eq!(stats.total_records, 1);
    }

    #[test]
    fn delete_maintains_index() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "cl").unwrap();

        let kp = doc_with(&[("x", Value::Int32(1))]);
        cat.create_index("cs", "cl", "idx_x", kp, false).unwrap();

        let rid = cat
            .insert_document("cs", "cl", &doc_with(&[("x", Value::Int32(7))]))
            .unwrap();
        cat.delete_document("cs", "cl", rid).unwrap();

        // Index should no longer find the key.
        let rt = cat.runtimes.get("cs.cl").unwrap();
        let idx = rt.indexes[0].read().unwrap();
        let key = IndexDefinition::extract_key(
            &idx.definition,
            &doc_with(&[("x", Value::Int32(7))]),
        );
        assert_eq!(idx.find(&key).unwrap(), None);
    }

    #[test]
    fn update_maintains_index() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "cl").unwrap();

        let kp = doc_with(&[("x", Value::Int32(1))]);
        cat.create_index("cs", "cl", "idx_x", kp, false).unwrap();

        let rid = cat
            .insert_document("cs", "cl", &doc_with(&[("x", Value::Int32(1))]))
            .unwrap();
        let new_rid = cat
            .update_document("cs", "cl", rid, &doc_with(&[("x", Value::Int32(2))]))
            .unwrap();

        let rt = cat.runtimes.get("cs.cl").unwrap();
        let idx = rt.indexes[0].read().unwrap();

        // Old key gone.
        let old_key = IndexDefinition::extract_key(
            &idx.definition,
            &doc_with(&[("x", Value::Int32(1))]),
        );
        assert_eq!(idx.find(&old_key).unwrap(), None);

        // New key points to new rid.
        let new_key = IndexDefinition::extract_key(
            &idx.definition,
            &doc_with(&[("x", Value::Int32(2))]),
        );
        assert_eq!(idx.find(&new_key).unwrap(), Some(new_rid));
    }

    // ── Bridge tests ────────────────────────────────────────────────

    #[test]
    fn collection_stats_correct() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "cl").unwrap();

        for i in 0..3 {
            cat.insert_document("cs", "cl", &doc_with(&[("x", Value::Int32(i))]))
                .unwrap();
        }

        let kp = doc_with(&[("x", Value::Int32(1))]);
        cat.create_index("cs", "cl", "idx_x", kp, false).unwrap();

        let stats = cat.collection_stats("cs", "cl").unwrap();
        assert_eq!(stats.total_records, 3);
        assert_eq!(stats.indexes.len(), 1);
        assert_eq!(stats.indexes[0].name, "idx_x");
    }

    #[test]
    fn collection_handle_usable() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "cl").unwrap();

        cat.insert_document("cs", "cl", &doc_with(&[("x", Value::Int32(1))]))
            .unwrap();

        let (storage, indexes) = cat.collection_handle("cs", "cl").unwrap();
        assert_eq!(storage.scan().len(), 1);
        assert!(indexes.is_empty());
    }

    // ── Auto-ID tests ───────────────────────────────────────────────

    #[test]
    fn cs_id_increments() {
        let mut cat = CatalogManager::new();
        let id1 = cat.create_collection_space("a").unwrap();
        let id2 = cat.create_collection_space("b").unwrap();
        assert_eq!(id2, id1 + 1);
    }

    #[test]
    fn cl_id_increments() {
        let mut cat = CatalogManager::new();
        cat.create_collection_space("cs").unwrap();
        cat.create_collection("cs", "a").unwrap();
        cat.create_collection("cs", "b").unwrap();
        let a = cat.get_collection("cs", "a").unwrap();
        let b = cat.get_collection("cs", "b").unwrap();
        assert_eq!(b.cl_id, a.cl_id + 1);
    }

    // ── Full lifecycle test ─────────────────────────────────────────

    #[test]
    fn full_lifecycle() {
        let mut cat = CatalogManager::new();

        // Create CS → CL
        cat.create_collection_space("mycs").unwrap();
        cat.create_collection("mycs", "mycl").unwrap();

        // Insert
        let rid1 = cat
            .insert_document("mycs", "mycl", &doc_with(&[("a", Value::Int32(10))]))
            .unwrap();
        let rid2 = cat
            .insert_document("mycs", "mycl", &doc_with(&[("a", Value::Int32(20))]))
            .unwrap();

        // Create index (populates from existing data)
        let kp = doc_with(&[("a", Value::Int32(1))]);
        cat.create_index("mycs", "mycl", "idx_a", kp, false)
            .unwrap();

        // Verify index populated
        let stats = cat.collection_stats("mycs", "mycl").unwrap();
        assert_eq!(stats.total_records, 2);
        assert_eq!(stats.indexes.len(), 1);

        // Insert after index creation
        let _rid3 = cat
            .insert_document("mycs", "mycl", &doc_with(&[("a", Value::Int32(30))]))
            .unwrap();

        // Verify index has all 3
        let rt = cat.runtimes.get("mycs.mycl").unwrap();
        let idx = rt.indexes[0].read().unwrap();
        let mut cursor = idx.scan(&sdb_ixm::KeyRange::all()).unwrap();
        let mut count = 0;
        while cursor.next().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
        drop(idx);

        // Delete one
        cat.delete_document("mycs", "mycl", rid1).unwrap();
        let stats = cat.collection_stats("mycs", "mycl").unwrap();
        assert_eq!(stats.total_records, 2); // 3 inserted - 1 deleted = 2

        // Update one
        cat.update_document(
            "mycs",
            "mycl",
            rid2,
            &doc_with(&[("a", Value::Int32(200))]),
        )
        .unwrap();

        // Verify via collection_handle
        let (storage, _) = cat.collection_handle("mycs", "mycl").unwrap();
        let rows = storage.scan();
        let values: Vec<i32> = rows
            .iter()
            .filter_map(|(_, doc)| match doc.get("a") {
                Some(Value::Int32(v)) => Some(*v),
                _ => None,
            })
            .collect();
        assert!(values.contains(&200));
        assert!(values.contains(&30));
        assert!(!values.contains(&10)); // deleted

        // Drop index
        cat.drop_index("mycs", "mycl", "idx_a").unwrap();
        let cl = cat.get_collection("mycs", "mycl").unwrap();
        assert!(cl.indexes.is_empty());

        // Drop CL → CS
        cat.drop_collection("mycs", "mycl").unwrap();
        cat.drop_collection_space("mycs").unwrap();
    }

    // ── Group tests (preserved) ─────────────────────────────────────

    #[test]
    fn register_and_get_group() {
        let mut cat = CatalogManager::new();
        let meta = GroupMeta {
            id: 1,
            name: "group1".to_string(),
            primary_node: None,
            nodes: vec![],
        };
        cat.register_group(meta).unwrap();
        let g = cat.get_group(1).unwrap();
        assert_eq!(g.name, "group1");
    }
}
