/// Cost per row for a full table scan.
pub const TABLE_SCAN_PER_ROW: f64 = 1.0;
/// Cost per row for an index scan (random I/O but smaller).
pub const INDEX_SCAN_PER_ROW: f64 = 0.4;
/// Fixed overhead for an index seek operation.
pub const INDEX_SEEK: f64 = 10.0;
/// Cost per row for an in-memory sort.
pub const SORT_PER_ROW: f64 = 3.0;
/// Cost per row for evaluating a filter predicate.
pub const FILTER_PER_ROW: f64 = 0.1;

/// Result of a cost estimation.
#[derive(Debug, Clone)]
pub struct CostEstimate {
    pub cost: f64,
    pub estimated_rows: u64,
}

/// Estimate cost of a full table scan.
pub fn estimate_table_scan(total_records: u64, needs_sort: bool) -> CostEstimate {
    let rows = total_records;
    let mut cost = rows as f64 * TABLE_SCAN_PER_ROW;
    if needs_sort && rows > 0 {
        cost += rows as f64 * SORT_PER_ROW;
    }
    CostEstimate {
        cost,
        estimated_rows: rows,
    }
}

/// Estimate cost of an index scan given selectivity (0.0–1.0) and whether
/// the index already provides the requested sort order.
pub fn estimate_index_scan(
    total_records: u64,
    selectivity: f64,
    avoids_sort: bool,
) -> CostEstimate {
    let raw_rows = (total_records as f64 * selectivity).max(0.0);
    let estimated_rows = raw_rows.ceil() as u64;
    // Use raw (non-rounded) row count for cost to preserve ordering between
    // indexes with very low selectivity that would otherwise round to the same integer.
    let mut cost = INDEX_SEEK + raw_rows * INDEX_SCAN_PER_ROW;
    if !avoids_sort && estimated_rows > 0 {
        cost += raw_rows * SORT_PER_ROW;
    }
    CostEstimate {
        cost,
        estimated_rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_scan_zero_rows() {
        let est = estimate_table_scan(0, false);
        assert_eq!(est.cost, 0.0);
        assert_eq!(est.estimated_rows, 0);
    }

    #[test]
    fn table_scan_no_sort() {
        let est = estimate_table_scan(1000, false);
        assert_eq!(est.cost, 1000.0 * TABLE_SCAN_PER_ROW);
        assert_eq!(est.estimated_rows, 1000);
    }

    #[test]
    fn table_scan_with_sort() {
        let est = estimate_table_scan(1000, true);
        let expected = 1000.0 * TABLE_SCAN_PER_ROW + 1000.0 * SORT_PER_ROW;
        assert_eq!(est.cost, expected);
    }

    #[test]
    fn table_scan_zero_rows_with_sort() {
        let est = estimate_table_scan(0, true);
        assert_eq!(est.cost, 0.0);
    }

    #[test]
    fn index_scan_full_selectivity() {
        let est = estimate_index_scan(1000, 1.0, true);
        let expected = INDEX_SEEK + 1000.0 * INDEX_SCAN_PER_ROW;
        assert_eq!(est.cost, expected);
        assert_eq!(est.estimated_rows, 1000);
    }

    #[test]
    fn index_scan_low_selectivity() {
        let est = estimate_index_scan(10000, 0.01, true);
        // ceil(10000 * 0.01) = 100
        let expected = INDEX_SEEK + 100.0 * INDEX_SCAN_PER_ROW;
        assert_eq!(est.cost, expected);
        assert_eq!(est.estimated_rows, 100);
    }

    #[test]
    fn index_scan_needs_sort() {
        let est = estimate_index_scan(1000, 0.1, false);
        // ceil(100) = 100
        let expected = INDEX_SEEK + 100.0 * INDEX_SCAN_PER_ROW + 100.0 * SORT_PER_ROW;
        assert_eq!(est.cost, expected);
    }

    #[test]
    fn index_scan_avoids_sort() {
        let est = estimate_index_scan(1000, 0.1, true);
        let expected = INDEX_SEEK + 100.0 * INDEX_SCAN_PER_ROW;
        assert_eq!(est.cost, expected);
    }

    #[test]
    fn index_scan_zero_rows() {
        let est = estimate_index_scan(0, 0.5, false);
        assert_eq!(est.estimated_rows, 0);
        assert_eq!(est.cost, INDEX_SEEK);
    }
}
