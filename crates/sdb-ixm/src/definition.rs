use sdb_bson::Document;

/// Definition of an index on a collection.
#[derive(Debug, Clone)]
pub struct IndexDefinition {
    pub name: String,
    pub key_pattern: Document,
    pub unique: bool,
    pub enforced: bool,
    pub not_null: bool,
}
