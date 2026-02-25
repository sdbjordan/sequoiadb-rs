use sdb_bson::Document;

/// Type of query plan node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanType {
    TableScan,
    IndexScan,
    Sort,
    Filter,
    Limit,
    Skip,
    Project,
    Merge,
}

/// A node in the query execution plan tree.
#[derive(Debug, Clone)]
pub struct PlanNode {
    pub plan_type: PlanType,
    pub children: Vec<PlanNode>,
    pub estimated_cost: f64,
    pub estimated_rows: u64,
}

/// Complete query plan for execution.
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub root: PlanNode,
    pub collection: String,
    pub condition: Option<Document>,
    pub selector: Option<Document>,
    pub order_by: Option<Document>,
    pub skip: i64,
    pub limit: i64,
}
