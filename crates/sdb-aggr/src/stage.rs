use sdb_bson::Document;

/// Type of aggregation pipeline stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageType {
    Match,
    Project,
    Group,
    Sort,
    Limit,
    Skip,
    Unwind,
    Lookup,
    Count,
}

/// A single stage in the aggregation pipeline.
#[derive(Debug, Clone)]
pub struct Stage {
    pub stage_type: StageType,
    pub spec: Document,
}
