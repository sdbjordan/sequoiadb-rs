pub mod btree;
pub mod key;
pub mod definition;
pub mod cursor;

pub use btree::BTreeIndex;
pub use key::{IndexKey, KeyRange};
pub use definition::IndexDefinition;
pub use cursor::IndexCursor;

use sdb_common::{RecordId, Result};

/// Core index trait for all index types.
pub trait Index: Send + Sync {
    fn insert(&mut self, key: &IndexKey, rid: RecordId) -> Result<()>;
    fn delete(&mut self, key: &IndexKey, rid: RecordId) -> Result<()>;
    fn find(&self, key: &IndexKey) -> Result<Option<RecordId>>;
    fn scan(&self, range: &KeyRange) -> Result<IndexCursor>;
}
