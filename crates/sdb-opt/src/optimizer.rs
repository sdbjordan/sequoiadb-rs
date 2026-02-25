use sdb_bson::Document;
use sdb_common::Result;
use sdb_mth::expression::{parse_condition, MatchExpression};

use crate::access_path::AccessPath;
use crate::cost::{estimate_index_scan, estimate_table_scan, FILTER_PER_ROW};
use crate::index_match::match_index;
use crate::plan::{PlanNode, PlanType, QueryPlan};
use crate::stats::CollectionStats;

/// Query optimizer — chooses the best execution plan.
pub struct Optimizer;

impl Optimizer {
    pub fn new() -> Self {
        Self
    }

    /// Generate an optimized query plan for the given query and collection stats.
    #[allow(clippy::too_many_arguments)]
    pub fn optimize(
        &self,
        collection: &str,
        condition: Option<&Document>,
        selector: Option<&Document>,
        order_by: Option<&Document>,
        skip: i64,
        limit: i64,
        stats: &CollectionStats,
    ) -> Result<QueryPlan> {
        let has_condition = condition.is_some_and(|c| !c.is_empty());
        let needs_sort = order_by.is_some_and(|o| !o.is_empty());

        // Parse condition into MatchExpression (if present)
        let expr = if let Some(cond) = condition.filter(|c| !c.is_empty()) {
            Some(parse_condition(cond)?)
        } else {
            None
        };

        // --- Enumerate candidate access paths ---
        let mut best_path = AccessPath::TableScan;

        // Candidate 1: TableScan
        let ts_est = estimate_table_scan(stats.total_records, needs_sort);
        let mut best_cost = ts_est.cost;
        let mut best_estimate = ts_est;

        // Candidate 2+: each index
        if let Some(ref expr) = expr {
            for idx_def in &stats.indexes {
                if let Some(im) = match_index(idx_def, expr, order_by, stats.total_records) {
                    let avoids_sort = im.covers_sort;
                    let is_est = estimate_index_scan(
                        stats.total_records,
                        im.selectivity,
                        avoids_sort && needs_sort,
                    );
                    if is_est.cost < best_cost {
                        best_cost = is_est.cost;
                        best_estimate = is_est;
                        best_path = AccessPath::IndexScan {
                            index_name: idx_def.name.clone(),
                            direction: im.direction,
                            selectivity: im.selectivity,
                            covers_sort: im.covers_sort,
                        };
                    }
                }
            }
        } else if needs_sort {
            // No condition but need sort — check if any index covers sort
            let empty_and = MatchExpression::And(vec![]);
            for idx_def in &stats.indexes {
                if let Some(im) = match_index(idx_def, &empty_and, order_by, stats.total_records) {
                    if im.covers_sort {
                        let is_est =
                            estimate_index_scan(stats.total_records, im.selectivity, true);
                        if is_est.cost < best_cost {
                            best_cost = is_est.cost;
                            best_estimate = is_est;
                            best_path = AccessPath::IndexScan {
                                index_name: idx_def.name.clone(),
                                direction: im.direction,
                                selectivity: im.selectivity,
                                covers_sort: true,
                            };
                        }
                    }
                }
            }
        }

        // --- Assemble plan tree (bottom-up): Scan → Filter → Sort → Skip → Limit → Project ---
        let covers_sort = matches!(
            &best_path,
            AccessPath::IndexScan { covers_sort: true, .. }
        );

        // 1. Scan node
        let (scan_type, scan_ap) = match &best_path {
            AccessPath::TableScan => (PlanType::TableScan, AccessPath::TableScan),
            AccessPath::IndexScan { .. } => (PlanType::IndexScan, best_path.clone()),
        };

        let mut current = PlanNode {
            plan_type: scan_type,
            children: Vec::new(),
            estimated_cost: best_estimate.cost,
            estimated_rows: best_estimate.estimated_rows,
            access_path: Some(scan_ap),
            sort_fields: None,
        };

        // 2. Filter node (always add if there's a condition — conservative v1)
        if has_condition {
            let filter_cost = current.estimated_rows as f64 * FILTER_PER_ROW;
            current = PlanNode {
                plan_type: PlanType::Filter,
                estimated_cost: current.estimated_cost + filter_cost,
                estimated_rows: current.estimated_rows,
                children: vec![current],
                access_path: None,
                sort_fields: None,
            };
        }

        // 3. Sort node (only if needed and index doesn't cover it)
        if needs_sort && !covers_sort {
            current = PlanNode {
                plan_type: PlanType::Sort,
                estimated_cost: current.estimated_cost,
                estimated_rows: current.estimated_rows,
                children: vec![current],
                access_path: None,
                sort_fields: order_by.cloned(),
            };
        }

        // 4. Skip node
        if skip > 0 {
            let rows_after_skip =
                current.estimated_rows.saturating_sub(skip.max(0) as u64);
            current = PlanNode {
                plan_type: PlanType::Skip,
                estimated_cost: current.estimated_cost,
                estimated_rows: rows_after_skip,
                children: vec![current],
                access_path: None,
                sort_fields: None,
            };
        }

        // 5. Limit node
        if limit > 0 {
            let rows_after_limit = current.estimated_rows.min(limit as u64);
            current = PlanNode {
                plan_type: PlanType::Limit,
                estimated_cost: current.estimated_cost,
                estimated_rows: rows_after_limit,
                children: vec![current],
                access_path: None,
                sort_fields: None,
            };
        }

        // 6. Project node
        if selector.is_some_and(|s| !s.is_empty()) {
            current = PlanNode {
                plan_type: PlanType::Project,
                estimated_cost: current.estimated_cost,
                estimated_rows: current.estimated_rows,
                children: vec![current],
                access_path: None,
                sort_fields: None,
            };
        }

        Ok(QueryPlan {
            root: current,
            collection: collection.to_string(),
            condition: condition.cloned(),
            selector: selector.cloned(),
            order_by: order_by.cloned(),
            skip,
            limit,
        })
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sdb_bson::Value;
    use sdb_ixm::IndexDefinition;

    fn make_index(name: &str, fields: &[(&str, i32)]) -> IndexDefinition {
        let mut kp = Document::new();
        for (f, d) in fields {
            kp.insert(*f, Value::Int32(*d));
        }
        IndexDefinition {
            name: name.to_string(),
            key_pattern: kp,
            unique: false,
            enforced: false,
            not_null: false,
        }
    }

    fn doc(pairs: &[(&str, Value)]) -> Document {
        let mut d = Document::new();
        for (k, v) in pairs {
            d.insert(*k, v.clone());
        }
        d
    }

    fn empty_stats() -> CollectionStats {
        CollectionStats {
            total_records: 0,
            extent_count: 0,
            indexes: vec![],
        }
    }

    fn stats_with(records: u64, indexes: Vec<IndexDefinition>) -> CollectionStats {
        CollectionStats {
            total_records: records,
            extent_count: 1,
            indexes,
        }
    }

    fn root_type(plan: &QueryPlan) -> PlanType {
        plan.root.plan_type
    }

    /// Walk down the plan tree and collect node types from root to leaf.
    fn collect_types(node: &PlanNode) -> Vec<PlanType> {
        let mut types = vec![node.plan_type];
        if let Some(child) = node.children.first() {
            types.extend(collect_types(child));
        }
        types
    }

    #[test]
    fn no_condition_no_sort_table_scan() {
        let opt = Optimizer::new();
        let plan = opt
            .optimize("test", None, None, None, 0, 0, &empty_stats())
            .unwrap();
        assert_eq!(root_type(&plan), PlanType::TableScan);
    }

    #[test]
    fn with_sort_adds_sort_node() {
        let opt = Optimizer::new();
        let sort = doc(&[("a", Value::Int32(1))]);
        let stats = stats_with(1000, vec![]);
        let plan = opt
            .optimize("test", None, None, Some(&sort), 0, 0, &stats)
            .unwrap();
        let types = collect_types(&plan.root);
        assert!(types.contains(&PlanType::Sort));
    }

    #[test]
    fn eq_match_uses_index_scan() {
        let opt = Optimizer::new();
        let cond = doc(&[("a", Value::Int32(5))]);
        let idx = make_index("idx_a", &[("a", 1)]);
        let stats = stats_with(10000, vec![idx]);
        let plan = opt
            .optimize("test", Some(&cond), None, None, 0, 0, &stats)
            .unwrap();
        let types = collect_types(&plan.root);
        // Should be Filter → IndexScan
        assert!(types.contains(&PlanType::IndexScan));
        assert!(types.contains(&PlanType::Filter));
    }

    #[test]
    fn picks_best_index() {
        let opt = Optimizer::new();
        // { a: 1, b: { $gt: 10 } } — idx_a matches 1 field, idx_ab matches 2
        let ops = doc(&[("$gt", Value::Int32(10))]);
        let cond = doc(&[("a", Value::Int32(1)), ("b", Value::Document(ops))]);
        let idx_a = make_index("idx_a", &[("a", 1)]);
        let idx_ab = make_index("idx_ab", &[("a", 1), ("b", 1)]);
        let stats = stats_with(10000, vec![idx_a, idx_ab]);
        let plan = opt
            .optimize("test", Some(&cond), None, None, 0, 0, &stats)
            .unwrap();
        // Find the scan node
        let scan = find_node(&plan.root, PlanType::IndexScan).unwrap();
        match &scan.access_path {
            Some(AccessPath::IndexScan { index_name, .. }) => {
                assert_eq!(index_name, "idx_ab");
            }
            _ => panic!("expected IndexScan access path"),
        }
    }

    #[test]
    fn index_covers_sort_no_sort_node() {
        let opt = Optimizer::new();
        let cond = doc(&[("a", Value::Int32(5))]);
        let sort = doc(&[("b", Value::Int32(1))]);
        let idx = make_index("idx_ab", &[("a", 1), ("b", 1)]);
        let stats = stats_with(10000, vec![idx]);
        let plan = opt
            .optimize("test", Some(&cond), None, Some(&sort), 0, 0, &stats)
            .unwrap();
        let types = collect_types(&plan.root);
        assert!(types.contains(&PlanType::IndexScan));
        assert!(!types.contains(&PlanType::Sort)); // index covers sort
    }

    #[test]
    fn skip_limit_selector_nodes() {
        let opt = Optimizer::new();
        let selector = doc(&[("name", Value::Int32(1))]);
        let stats = stats_with(1000, vec![]);
        let plan = opt
            .optimize("test", None, Some(&selector), None, 10, 20, &stats)
            .unwrap();
        let types = collect_types(&plan.root);
        assert!(types.contains(&PlanType::Project));
        assert!(types.contains(&PlanType::Limit));
        assert!(types.contains(&PlanType::Skip));
    }

    #[test]
    fn condition_adds_filter() {
        let opt = Optimizer::new();
        let cond = doc(&[("x", Value::Int32(1))]);
        let stats = stats_with(100, vec![]);
        let plan = opt
            .optimize("test", Some(&cond), None, None, 0, 0, &stats)
            .unwrap();
        let types = collect_types(&plan.root);
        assert!(types.contains(&PlanType::Filter));
    }

    #[test]
    fn empty_condition_no_filter() {
        let opt = Optimizer::new();
        let cond = Document::new();
        let stats = stats_with(100, vec![]);
        let plan = opt
            .optimize("test", Some(&cond), None, None, 0, 0, &stats)
            .unwrap();
        let types = collect_types(&plan.root);
        assert!(!types.contains(&PlanType::Filter));
    }

    #[test]
    fn no_sort_index_covers_sort_for_query_without_condition() {
        let opt = Optimizer::new();
        let sort = doc(&[("a", Value::Int32(1))]);
        let idx = make_index("idx_a", &[("a", 1)]);
        let stats = stats_with(10000, vec![idx]);
        let plan = opt
            .optimize("test", None, None, Some(&sort), 0, 0, &stats)
            .unwrap();
        let types = collect_types(&plan.root);
        // Index should cover sort, so no Sort node
        assert!(types.contains(&PlanType::IndexScan));
        assert!(!types.contains(&PlanType::Sort));
    }

    fn find_node(node: &PlanNode, target: PlanType) -> Option<&PlanNode> {
        if node.plan_type == target {
            return Some(node);
        }
        for child in &node.children {
            if let Some(found) = find_node(child, target) {
                return Some(found);
            }
        }
        None
    }
}
