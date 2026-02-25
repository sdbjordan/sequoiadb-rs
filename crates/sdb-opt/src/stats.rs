use sdb_ixm::IndexDefinition;

/// Statistics about a collection, used by the optimizer for cost estimation.
/// This is a pure data structure — the caller (sdb-rtn) is responsible for
/// populating it from actual catalog/storage metadata.
pub struct CollectionStats {
    pub total_records: u64,
    pub extent_count: u32,
    pub indexes: Vec<IndexDefinition>,
}
