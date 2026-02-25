use sdb_common::{RecordId, Result};
use crate::cursor::IndexCursor;
use crate::definition::IndexDefinition;
use crate::key::{IndexKey, KeyRange};

/// B-tree index implementation.
pub struct BTreeIndex {
    pub definition: IndexDefinition,
    // Stub: actual B-tree pages managed by sdb-dms
}

impl BTreeIndex {
    pub fn new(definition: IndexDefinition) -> Self {
        Self { definition }
    }
}

impl crate::Index for BTreeIndex {
    fn insert(&mut self, _key: &IndexKey, _rid: RecordId) -> Result<()> {
        // Stub
        Ok(())
    }

    fn delete(&mut self, _key: &IndexKey, _rid: RecordId) -> Result<()> {
        // Stub
        Ok(())
    }

    fn find(&self, _key: &IndexKey) -> Result<Option<RecordId>> {
        // Stub
        Ok(None)
    }

    fn scan(&self, _range: &KeyRange) -> Result<IndexCursor> {
        // Stub
        Ok(IndexCursor::empty())
    }
}
