use sdb_common::{CollectionId, CollectionSpaceId};

/// A storage unit — manages the data file for a single collection.
pub struct StorageUnit {
    pub cs_id: CollectionSpaceId,
    pub cl_id: CollectionId,
    pub data_path: String,
    pub index_path: String,
    pub total_records: u64,
    pub total_data_pages: u32,
}
