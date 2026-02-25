use sdb_bson::Document;
use sdb_ixm::IndexDefinition;
use sdb_mth::expression::{CompareOp, MatchExpression};

use crate::access_path::ScanDirection;

/// The type of predicate on a field, relevant for index prefix matching.
#[derive(Debug, Clone, PartialEq)]
pub enum PredicateOp {
    Eq,
    Range,
    In(usize),
}

/// Result of matching an index against query predicates.
#[derive(Debug, Clone)]
pub struct IndexMatchResult {
    /// How many leading key_pattern fields were matched by predicates.
    pub matched_prefix_len: usize,
    /// Estimated selectivity (0.0–1.0).
    pub selectivity: f64,
    /// Whether this index provides the requested sort order.
    pub covers_sort: bool,
    /// Scan direction needed (Forward or Backward).
    pub direction: ScanDirection,
}

/// Extract field-level predicates from a MatchExpression.
/// Returns a list of (field_path, PredicateOp). Only extracts from
/// top-level AND conjunctions and simple comparisons — OR/NOT/nested
/// structures are ignored for index matching in v1.
fn extract_predicates(expr: &MatchExpression) -> Vec<(String, PredicateOp)> {
    let mut preds = Vec::new();
    collect_predicates(expr, &mut preds);
    preds
}

fn collect_predicates(expr: &MatchExpression, out: &mut Vec<(String, PredicateOp)>) {
    match expr {
        MatchExpression::And(subs) => {
            for sub in subs {
                collect_predicates(sub, out);
            }
        }
        MatchExpression::Compare { path, op, .. } => {
            let pred_op = match op {
                CompareOp::Eq => PredicateOp::Eq,
                CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
                    PredicateOp::Range
                }
                CompareOp::Ne => return, // $ne cannot use index prefix
            };
            out.push((path.clone(), pred_op));
        }
        MatchExpression::In { path, values } => {
            out.push((path.clone(), PredicateOp::In(values.len())));
        }
        // Or, Nor, Not, Exists, Nin — not usable for index prefix matching in v1
        _ => {}
    }
}

/// Match an index's key_pattern against query predicates and optional sort order.
///
/// Returns `None` if the index matches zero prefix fields AND doesn't cover sort.
pub fn match_index(
    index: &IndexDefinition,
    expr: &MatchExpression,
    order_by: Option<&Document>,
    total_records: u64,
) -> Option<IndexMatchResult> {
    let preds = extract_predicates(expr);
    let index_fields: Vec<(&str, i8)> = index
        .key_pattern
        .iter()
        .zip(index.directions())
        .map(|(elem, dir)| (elem.key.as_str(), dir))
        .collect();

    // --- Phase 1: prefix matching ---
    let mut matched_prefix_len = 0usize;
    let mut selectivity = 1.0f64;
    let mut last_was_eq = true; // track if we can keep extending

    for (field_name, _dir) in &index_fields {
        if !last_was_eq {
            break;
        }
        // Find the best predicate for this field
        let field_pred = best_predicate_for_field(&preds, field_name);
        match field_pred {
            Some(PredicateOp::Eq) => {
                matched_prefix_len += 1;
                let n = total_records.max(1) as f64;
                selectivity *= 1.0 / n;
                // continue extending
            }
            Some(PredicateOp::Range) => {
                matched_prefix_len += 1;
                selectivity *= 0.33;
                last_was_eq = false; // range stops further prefix extension
            }
            Some(PredicateOp::In(count)) => {
                matched_prefix_len += 1;
                let n = total_records.max(1) as f64;
                selectivity *= count as f64 / n;
                last_was_eq = false; // $in stops further prefix extension
            }
            None => {
                break;
            }
        }
    }

    // --- Phase 2: sort coverage ---
    let (covers_sort, direction) = check_sort_coverage(&index_fields, matched_prefix_len, order_by);

    if matched_prefix_len == 0 && !covers_sort {
        return None;
    }

    Some(IndexMatchResult {
        matched_prefix_len,
        selectivity,
        covers_sort,
        direction,
    })
}

/// Find the best predicate for a given field. Eq > In > Range.
fn best_predicate_for_field(preds: &[(String, PredicateOp)], field: &str) -> Option<PredicateOp> {
    let mut best: Option<PredicateOp> = None;
    for (path, op) in preds {
        if path == field {
            best = Some(match (&best, op) {
                (None, _) => op.clone(),
                (Some(PredicateOp::Eq), _) => PredicateOp::Eq, // Eq is already best
                (_, PredicateOp::Eq) => PredicateOp::Eq,
                (Some(PredicateOp::In(_)), PredicateOp::In(n)) => PredicateOp::In(*n),
                (Some(PredicateOp::In(n)), _) => PredicateOp::In(*n),
                (_, PredicateOp::In(n)) => PredicateOp::In(*n),
                (Some(PredicateOp::Range), _) => PredicateOp::Range,
            });
        }
    }
    best
}

/// Check whether the index provides the requested sort order, considering
/// that equality prefix fields are "free" (they're all the same value).
fn check_sort_coverage(
    index_fields: &[(&str, i8)],
    eq_prefix_len: usize,
    order_by: Option<&Document>,
) -> (bool, ScanDirection) {
    let order_by = match order_by {
        Some(ob) if !ob.is_empty() => ob,
        _ => return (false, ScanDirection::Forward),
    };

    let sort_fields: Vec<(&str, i8)> = order_by
        .iter()
        .map(|elem| {
            let dir = match &elem.value {
                sdb_bson::Value::Int32(v) if *v < 0 => -1i8,
                sdb_bson::Value::Int64(v) if *v < 0 => -1i8,
                sdb_bson::Value::Double(v) if *v < 0.0 => -1i8,
                _ => 1i8,
            };
            (elem.key.as_str(), dir)
        })
        .collect();

    // The remaining index fields after the equality prefix must match the sort fields
    let remaining = &index_fields[eq_prefix_len..];
    if sort_fields.len() > remaining.len() {
        return (false, ScanDirection::Forward);
    }

    // Check forward: all sort directions match index directions
    let forward = sort_fields
        .iter()
        .zip(remaining.iter())
        .all(|((sf, sd), (if_, id))| sf == if_ && sd == id);

    if forward {
        return (true, ScanDirection::Forward);
    }

    // Check backward: all sort directions are the reverse of index directions
    let backward = sort_fields
        .iter()
        .zip(remaining.iter())
        .all(|((sf, sd), (if_, id))| sf == if_ && *sd == -*id);

    if backward {
        return (true, ScanDirection::Backward);
    }

    (false, ScanDirection::Forward)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sdb_bson::Value;
    use sdb_mth::expression::parse_condition;

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

    fn parse(cond: &Document) -> MatchExpression {
        parse_condition(cond).unwrap()
    }

    // --- Single field eq ---
    #[test]
    fn single_field_eq() {
        let idx = make_index("idx_a", &[("a", 1)]);
        let cond = doc(&[("a", Value::Int32(5))]);
        let expr = parse(&cond);
        let result = match_index(&idx, &expr, None, 1000).unwrap();
        assert_eq!(result.matched_prefix_len, 1);
        assert!(result.selectivity <= 0.002); // 1/1000
        assert!(!result.covers_sort);
    }

    // --- Single field range ---
    #[test]
    fn single_field_range() {
        let ops = doc(&[("$gt", Value::Int32(10))]);
        let cond = doc(&[("a", Value::Document(ops))]);
        let idx = make_index("idx_a", &[("a", 1)]);
        let expr = parse(&cond);
        let result = match_index(&idx, &expr, None, 1000).unwrap();
        assert_eq!(result.matched_prefix_len, 1);
        assert!((result.selectivity - 0.33).abs() < 0.001);
    }

    // --- Compound eq + range ---
    #[test]
    fn compound_eq_then_range() {
        // { a: 1, b: { $gt: 10 } } with index (a, b)
        let ops = doc(&[("$gt", Value::Int32(10))]);
        let cond = doc(&[("a", Value::Int32(1)), ("b", Value::Document(ops))]);
        let idx = make_index("idx_ab", &[("a", 1), ("b", 1)]);
        let expr = parse(&cond);
        let result = match_index(&idx, &expr, None, 1000).unwrap();
        assert_eq!(result.matched_prefix_len, 2);
        // selectivity = (1/1000) * 0.33
        assert!(result.selectivity < 0.01);
    }

    // --- Range stops prefix ---
    #[test]
    fn range_stops_prefix() {
        // { a: { $gt: 1 }, b: 2 } with index (a, b)
        // Range on 'a' stops prefix, so b is not matched
        let ops_a = doc(&[("$gt", Value::Int32(1))]);
        let cond = doc(&[("a", Value::Document(ops_a)), ("b", Value::Int32(2))]);
        let idx = make_index("idx_ab", &[("a", 1), ("b", 1)]);
        let expr = parse(&cond);
        let result = match_index(&idx, &expr, None, 1000).unwrap();
        assert_eq!(result.matched_prefix_len, 1); // only 'a'
    }

    // --- No match ---
    #[test]
    fn no_match_returns_none() {
        // { c: 1 } with index (a, b) — field not in index
        let cond = doc(&[("c", Value::Int32(1))]);
        let idx = make_index("idx_ab", &[("a", 1), ("b", 1)]);
        let expr = parse(&cond);
        assert!(match_index(&idx, &expr, None, 1000).is_none());
    }

    // --- Sort coverage: exact match ---
    #[test]
    fn sort_coverage_exact() {
        let cond = Document::new(); // empty condition → match-all
        let expr = parse(&cond);
        let idx = make_index("idx_a", &[("a", 1)]);
        let sort = doc(&[("a", Value::Int32(1))]);
        let result = match_index(&idx, &expr, Some(&sort), 1000).unwrap();
        assert!(result.covers_sort);
        assert_eq!(result.direction, ScanDirection::Forward);
        assert_eq!(result.matched_prefix_len, 0);
    }

    // --- Sort coverage: reversed ---
    #[test]
    fn sort_coverage_reversed() {
        let cond = Document::new();
        let expr = parse(&cond);
        let idx = make_index("idx_a", &[("a", 1)]);
        let sort = doc(&[("a", Value::Int32(-1))]);
        let result = match_index(&idx, &expr, Some(&sort), 1000).unwrap();
        assert!(result.covers_sort);
        assert_eq!(result.direction, ScanDirection::Backward);
    }

    // --- Sort coverage after equality prefix ---
    #[test]
    fn sort_after_eq_prefix() {
        // { a: 1 } ORDER BY b with index (a, b)
        let cond = doc(&[("a", Value::Int32(1))]);
        let expr = parse(&cond);
        let idx = make_index("idx_ab", &[("a", 1), ("b", 1)]);
        let sort = doc(&[("b", Value::Int32(1))]);
        let result = match_index(&idx, &expr, Some(&sort), 1000).unwrap();
        assert_eq!(result.matched_prefix_len, 1);
        assert!(result.covers_sort);
        assert_eq!(result.direction, ScanDirection::Forward);
    }

    // --- Sort not covered ---
    #[test]
    fn sort_not_covered() {
        let cond = Document::new();
        let expr = parse(&cond);
        let idx = make_index("idx_a", &[("a", 1)]);
        let sort = doc(&[("b", Value::Int32(1))]);
        assert!(match_index(&idx, &expr, Some(&sort), 1000).is_none());
    }

    // --- $in predicate ---
    #[test]
    fn in_predicate() {
        let vals = vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)];
        let ops = doc(&[("$in", Value::Array(vals))]);
        let cond = doc(&[("a", Value::Document(ops))]);
        let idx = make_index("idx_a", &[("a", 1)]);
        let expr = parse(&cond);
        let result = match_index(&idx, &expr, None, 1000).unwrap();
        assert_eq!(result.matched_prefix_len, 1);
        // selectivity = 3/1000 = 0.003
        assert!((result.selectivity - 0.003).abs() < 0.001);
    }

    // --- Compound index, partial match ---
    #[test]
    fn compound_partial_match() {
        // { a: 1 } with index (a, b, c)
        let cond = doc(&[("a", Value::Int32(1))]);
        let idx = make_index("idx_abc", &[("a", 1), ("b", 1), ("c", 1)]);
        let expr = parse(&cond);
        let result = match_index(&idx, &expr, None, 1000).unwrap();
        assert_eq!(result.matched_prefix_len, 1);
    }

    // --- Sort coverage with compound backward ---
    #[test]
    fn sort_compound_backward() {
        let cond = Document::new();
        let expr = parse(&cond);
        // Index (a ASC, b DESC) → sort (a DESC, b ASC) = backward
        let idx = make_index("idx_ab", &[("a", 1), ("b", -1)]);
        let sort = doc(&[("a", Value::Int32(-1)), ("b", Value::Int32(1))]);
        let result = match_index(&idx, &expr, Some(&sort), 1000).unwrap();
        assert!(result.covers_sort);
        assert_eq!(result.direction, ScanDirection::Backward);
    }
}
