use sdb_bson::Document;
use sdb_common::Result;
use crate::stage::Stage;

/// Aggregation pipeline — a sequence of stages.
pub struct Pipeline {
    pub collection: String,
    pub stages: Vec<Stage>,
}

impl Pipeline {
    pub fn new(collection: impl Into<String>, stages: Vec<Stage>) -> Self {
        Self {
            collection: collection.into(),
            stages,
        }
    }

    /// Execute the pipeline and return result documents.
    pub fn execute(&self) -> Result<Vec<Document>> {
        // Stub: return empty results
        let _ = &self.stages;
        Ok(Vec::new())
    }
}
