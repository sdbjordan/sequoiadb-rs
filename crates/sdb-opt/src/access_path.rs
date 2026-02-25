/// Represents a possible access path for a query (table scan, index scan, etc.).
#[derive(Debug, Clone)]
pub enum AccessPath {
    TableScan,
    IndexScan {
        index_name: String,
        direction: ScanDirection,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Backward,
}
