use sdb_common::ExtentId;

/// An extent — a contiguous group of pages forming a storage unit.
pub struct Extent {
    pub id: ExtentId,
    pub first_page: u32,
    pub page_count: u32,
    pub free_space: u32,
    pub next_extent: Option<ExtentId>,
    pub prev_extent: Option<ExtentId>,
}
