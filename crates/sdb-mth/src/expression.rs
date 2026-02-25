use sdb_bson::{Document, Value};
use sdb_common::{Result, SdbError};

use crate::compare::{compare_values, resolve_path, values_equal};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone)]
pub enum MatchExpression {
    And(Vec<MatchExpression>),
    Or(Vec<MatchExpression>),
    Nor(Vec<MatchExpression>),
    Not(Box<MatchExpression>),
    Compare {
        path: String,
        op: CompareOp,
        value: Value,
    },
    Exists {
        path: String,
        expected: bool,
    },
    In {
        path: String,
        values: Vec<Value>,
    },
    Nin {
        path: String,
        values: Vec<Value>,
    },
}

/// Parse a top-level condition document into a MatchExpression.
pub fn parse_condition(condition: &Document) -> Result<MatchExpression> {
    let mut exprs = Vec::new();

    for elem in condition.iter() {
        let key = &elem.key;
        if key.starts_with('$') {
            // Top-level logical operator
            let sub = parse_logical_op(key, &elem.value)?;
            exprs.push(sub);
        } else {
            // Field-level condition
            let sub = parse_field(key, &elem.value)?;
            exprs.push(sub);
        }
    }

    match exprs.len() {
        0 => Ok(MatchExpression::And(vec![])), // empty condition matches all
        1 => Ok(exprs.into_iter().next().unwrap()),
        _ => Ok(MatchExpression::And(exprs)), // implicit $and for multiple top-level fields
    }
}

/// Parse a top-level logical operator ($and, $or, $nor).
fn parse_logical_op(op: &str, value: &Value) -> Result<MatchExpression> {
    let arr = match value {
        Value::Array(arr) => arr,
        _ => return Err(SdbError::InvalidArg),
    };

    let mut subs = Vec::with_capacity(arr.len());
    for item in arr {
        match item {
            Value::Document(doc) => subs.push(parse_condition(doc)?),
            _ => return Err(SdbError::InvalidArg),
        }
    }

    match op {
        "$and" => Ok(MatchExpression::And(subs)),
        "$or" => Ok(MatchExpression::Or(subs)),
        "$nor" => Ok(MatchExpression::Nor(subs)),
        _ => Err(SdbError::InvalidArg),
    }
}

/// Parse a field-level condition.
/// - `{ field: value }` → implicit $eq
/// - `{ field: { $gt: 10, $lt: 20 } }` → implicit $and of compares
fn parse_field(path: &str, value: &Value) -> Result<MatchExpression> {
    match value {
        Value::Document(doc) => {
            // Check if this is an operator document (keys start with $)
            let is_op_doc = doc.iter().next().map_or(false, |e| e.key.starts_with('$'));
            if is_op_doc {
                parse_field_operators(path, doc)
            } else {
                // Literal document equality
                Ok(MatchExpression::Compare {
                    path: path.to_string(),
                    op: CompareOp::Eq,
                    value: value.clone(),
                })
            }
        }
        _ => {
            // Implicit $eq
            Ok(MatchExpression::Compare {
                path: path.to_string(),
                op: CompareOp::Eq,
                value: value.clone(),
            })
        }
    }
}

/// Parse operator document for a field: `{ $gt: 10, $lt: 20, $in: [...], $exists: true, $not: {...} }`
fn parse_field_operators(path: &str, ops: &Document) -> Result<MatchExpression> {
    let mut exprs = Vec::new();

    for elem in ops.iter() {
        let op_key = &elem.key;
        let op_val = &elem.value;

        match op_key.as_str() {
            "$eq" => exprs.push(MatchExpression::Compare {
                path: path.to_string(),
                op: CompareOp::Eq,
                value: op_val.clone(),
            }),
            "$ne" => exprs.push(MatchExpression::Compare {
                path: path.to_string(),
                op: CompareOp::Ne,
                value: op_val.clone(),
            }),
            "$gt" => exprs.push(MatchExpression::Compare {
                path: path.to_string(),
                op: CompareOp::Gt,
                value: op_val.clone(),
            }),
            "$gte" => exprs.push(MatchExpression::Compare {
                path: path.to_string(),
                op: CompareOp::Gte,
                value: op_val.clone(),
            }),
            "$lt" => exprs.push(MatchExpression::Compare {
                path: path.to_string(),
                op: CompareOp::Lt,
                value: op_val.clone(),
            }),
            "$lte" => exprs.push(MatchExpression::Compare {
                path: path.to_string(),
                op: CompareOp::Lte,
                value: op_val.clone(),
            }),
            "$in" => {
                let arr = match op_val {
                    Value::Array(a) => a.clone(),
                    _ => return Err(SdbError::InvalidArg),
                };
                exprs.push(MatchExpression::In {
                    path: path.to_string(),
                    values: arr,
                });
            }
            "$nin" => {
                let arr = match op_val {
                    Value::Array(a) => a.clone(),
                    _ => return Err(SdbError::InvalidArg),
                };
                exprs.push(MatchExpression::Nin {
                    path: path.to_string(),
                    values: arr,
                });
            }
            "$exists" => {
                let expected = match op_val {
                    Value::Boolean(b) => *b,
                    Value::Int32(n) => *n != 0,
                    Value::Int64(n) => *n != 0,
                    _ => return Err(SdbError::InvalidArg),
                };
                exprs.push(MatchExpression::Exists {
                    path: path.to_string(),
                    expected,
                });
            }
            "$not" => {
                let inner_doc = match op_val {
                    Value::Document(d) => d,
                    _ => return Err(SdbError::InvalidArg),
                };
                let inner = parse_field_operators(path, inner_doc)?;
                exprs.push(MatchExpression::Not(Box::new(inner)));
            }
            _ => return Err(SdbError::InvalidArg),
        }
    }

    match exprs.len() {
        0 => Err(SdbError::InvalidArg),
        1 => Ok(exprs.into_iter().next().unwrap()),
        _ => Ok(MatchExpression::And(exprs)),
    }
}

/// Evaluate a MatchExpression against a document.
pub fn evaluate(expr: &MatchExpression, doc: &Document) -> Result<bool> {
    match expr {
        MatchExpression::And(subs) => {
            for s in subs {
                if !evaluate(s, doc)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        MatchExpression::Or(subs) => {
            for s in subs {
                if evaluate(s, doc)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        MatchExpression::Nor(subs) => {
            for s in subs {
                if evaluate(s, doc)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        MatchExpression::Not(inner) => {
            Ok(!evaluate(inner, doc)?)
        }
        MatchExpression::Compare { path, op, value } => {
            let doc_val = resolve_path(doc, path);
            match doc_val {
                None => {
                    // Missing field: only $ne returns true (field != value when field doesn't exist)
                    Ok(*op == CompareOp::Ne)
                }
                Some(dv) => {
                    let ord = compare_values(dv, value);
                    let result = match op {
                        CompareOp::Eq => values_equal(dv, value),
                        CompareOp::Ne => !values_equal(dv, value),
                        CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                        CompareOp::Gte => ord != std::cmp::Ordering::Less,
                        CompareOp::Lt => ord == std::cmp::Ordering::Less,
                        CompareOp::Lte => ord != std::cmp::Ordering::Greater,
                    };
                    Ok(result)
                }
            }
        }
        MatchExpression::Exists { path, expected } => {
            let exists = resolve_path(doc, path).is_some();
            Ok(exists == *expected)
        }
        MatchExpression::In { path, values } => {
            let doc_val = match resolve_path(doc, path) {
                Some(v) => v,
                None => return Ok(false),
            };
            for candidate in values {
                if values_equal(doc_val, candidate) {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        MatchExpression::Nin { path, values } => {
            let doc_val = match resolve_path(doc, path) {
                Some(v) => v,
                None => return Ok(true), // missing field is not in the list
            };
            for candidate in values {
                if values_equal(doc_val, candidate) {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(pairs: &[(&str, Value)]) -> Document {
        let mut d = Document::new();
        for (k, v) in pairs {
            d.insert(*k, v.clone());
        }
        d
    }

    // --- parse tests ---

    #[test]
    fn parse_empty_condition() {
        let cond = Document::new();
        let expr = parse_condition(&cond).unwrap();
        // Empty $and matches everything
        let any_doc = doc(&[("x", Value::Int32(1))]);
        assert!(evaluate(&expr, &any_doc).unwrap());
    }

    #[test]
    fn parse_implicit_eq() {
        let cond = doc(&[("age", Value::Int32(25))]);
        let expr = parse_condition(&cond).unwrap();
        assert!(matches!(expr, MatchExpression::Compare { op: CompareOp::Eq, .. }));
    }

    #[test]
    fn parse_operator_doc() {
        // { age: { $gt: 10, $lt: 50 } }
        let ops = doc(&[("$gt", Value::Int32(10)), ("$lt", Value::Int32(50))]);
        let cond = doc(&[("age", Value::Document(ops))]);
        let expr = parse_condition(&cond).unwrap();
        assert!(matches!(expr, MatchExpression::And(_)));
    }

    #[test]
    fn parse_logical_and() {
        // { $and: [ {a: 1}, {b: 2} ] }
        let sub1 = Value::Document(doc(&[("a", Value::Int32(1))]));
        let sub2 = Value::Document(doc(&[("b", Value::Int32(2))]));
        let cond = doc(&[("$and", Value::Array(vec![sub1, sub2]))]);
        let expr = parse_condition(&cond).unwrap();
        assert!(matches!(expr, MatchExpression::And(_)));
    }

    #[test]
    fn parse_in() {
        // { status: { $in: [1, 2, 3] } }
        let vals = vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)];
        let ops = doc(&[("$in", Value::Array(vals))]);
        let cond = doc(&[("status", Value::Document(ops))]);
        let expr = parse_condition(&cond).unwrap();
        assert!(matches!(expr, MatchExpression::In { .. }));
    }

    #[test]
    fn parse_exists() {
        let ops = doc(&[("$exists", Value::Boolean(true))]);
        let cond = doc(&[("name", Value::Document(ops))]);
        let expr = parse_condition(&cond).unwrap();
        assert!(matches!(expr, MatchExpression::Exists { expected: true, .. }));
    }

    #[test]
    fn parse_not() {
        // { age: { $not: { $gt: 5 } } }
        let inner = doc(&[("$gt", Value::Int32(5))]);
        let ops = doc(&[("$not", Value::Document(inner))]);
        let cond = doc(&[("age", Value::Document(ops))]);
        let expr = parse_condition(&cond).unwrap();
        assert!(matches!(expr, MatchExpression::Not(_)));
    }

    #[test]
    fn parse_invalid_operator() {
        let ops = doc(&[("$bogus", Value::Int32(1))]);
        let cond = doc(&[("x", Value::Document(ops))]);
        assert!(parse_condition(&cond).is_err());
    }

    // --- evaluate tests ---

    #[test]
    fn eval_eq_match() {
        let cond = doc(&[("x", Value::Int32(10))]);
        let expr = parse_condition(&cond).unwrap();
        let d = doc(&[("x", Value::Int32(10))]);
        assert!(evaluate(&expr, &d).unwrap());
    }

    #[test]
    fn eval_eq_no_match() {
        let cond = doc(&[("x", Value::Int32(10))]);
        let expr = parse_condition(&cond).unwrap();
        let d = doc(&[("x", Value::Int32(20))]);
        assert!(!evaluate(&expr, &d).unwrap());
    }

    #[test]
    fn eval_eq_cross_numeric() {
        let cond = doc(&[("x", Value::Int32(10))]);
        let expr = parse_condition(&cond).unwrap();
        let d = doc(&[("x", Value::Int64(10))]);
        assert!(evaluate(&expr, &d).unwrap());
    }

    #[test]
    fn eval_gt_lt_range() {
        // { age: { $gt: 18, $lt: 65 } }
        let ops = doc(&[("$gt", Value::Int32(18)), ("$lt", Value::Int32(65))]);
        let cond = doc(&[("age", Value::Document(ops))]);
        let expr = parse_condition(&cond).unwrap();

        assert!(evaluate(&expr, &doc(&[("age", Value::Int32(30))])).unwrap());
        assert!(!evaluate(&expr, &doc(&[("age", Value::Int32(10))])).unwrap());
        assert!(!evaluate(&expr, &doc(&[("age", Value::Int32(70))])).unwrap());
        assert!(!evaluate(&expr, &doc(&[("age", Value::Int32(18))])).unwrap()); // not >18
    }

    #[test]
    fn eval_gte_lte() {
        let ops = doc(&[("$gte", Value::Int32(5)), ("$lte", Value::Int32(5))]);
        let cond = doc(&[("x", Value::Document(ops))]);
        let expr = parse_condition(&cond).unwrap();
        assert!(evaluate(&expr, &doc(&[("x", Value::Int32(5))])).unwrap());
        assert!(!evaluate(&expr, &doc(&[("x", Value::Int32(4))])).unwrap());
        assert!(!evaluate(&expr, &doc(&[("x", Value::Int32(6))])).unwrap());
    }

    #[test]
    fn eval_ne() {
        let ops = doc(&[("$ne", Value::Int32(5))]);
        let cond = doc(&[("x", Value::Document(ops))]);
        let expr = parse_condition(&cond).unwrap();
        assert!(evaluate(&expr, &doc(&[("x", Value::Int32(3))])).unwrap());
        assert!(!evaluate(&expr, &doc(&[("x", Value::Int32(5))])).unwrap());
        // Missing field: $ne returns true
        assert!(evaluate(&expr, &doc(&[("y", Value::Int32(5))])).unwrap());
    }

    #[test]
    fn eval_in() {
        let vals = vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)];
        let ops = doc(&[("$in", Value::Array(vals))]);
        let cond = doc(&[("x", Value::Document(ops))]);
        let expr = parse_condition(&cond).unwrap();

        assert!(evaluate(&expr, &doc(&[("x", Value::Int32(2))])).unwrap());
        assert!(!evaluate(&expr, &doc(&[("x", Value::Int32(5))])).unwrap());
        assert!(!evaluate(&expr, &doc(&[("y", Value::Int32(1))])).unwrap()); // missing field
    }

    #[test]
    fn eval_nin() {
        let vals = vec![Value::Int32(1), Value::Int32(2)];
        let ops = doc(&[("$nin", Value::Array(vals))]);
        let cond = doc(&[("x", Value::Document(ops))]);
        let expr = parse_condition(&cond).unwrap();

        assert!(!evaluate(&expr, &doc(&[("x", Value::Int32(1))])).unwrap());
        assert!(evaluate(&expr, &doc(&[("x", Value::Int32(5))])).unwrap());
        assert!(evaluate(&expr, &doc(&[("y", Value::Int32(1))])).unwrap()); // missing → true
    }

    #[test]
    fn eval_exists() {
        let ops_true = doc(&[("$exists", Value::Boolean(true))]);
        let cond_true = doc(&[("x", Value::Document(ops_true))]);
        let expr_true = parse_condition(&cond_true).unwrap();

        let ops_false = doc(&[("$exists", Value::Boolean(false))]);
        let cond_false = doc(&[("x", Value::Document(ops_false))]);
        let expr_false = parse_condition(&cond_false).unwrap();

        let with_x = doc(&[("x", Value::Int32(1))]);
        let without_x = doc(&[("y", Value::Int32(1))]);

        assert!(evaluate(&expr_true, &with_x).unwrap());
        assert!(!evaluate(&expr_true, &without_x).unwrap());
        assert!(!evaluate(&expr_false, &with_x).unwrap());
        assert!(evaluate(&expr_false, &without_x).unwrap());
    }

    #[test]
    fn eval_not() {
        // { age: { $not: { $gt: 50 } } } → matches if age <= 50 or missing
        let inner = doc(&[("$gt", Value::Int32(50))]);
        let ops = doc(&[("$not", Value::Document(inner))]);
        let cond = doc(&[("age", Value::Document(ops))]);
        let expr = parse_condition(&cond).unwrap();

        assert!(evaluate(&expr, &doc(&[("age", Value::Int32(30))])).unwrap());
        assert!(!evaluate(&expr, &doc(&[("age", Value::Int32(60))])).unwrap());
    }

    #[test]
    fn eval_or() {
        // { $or: [ {x: 1}, {y: 2} ] }
        let sub1 = Value::Document(doc(&[("x", Value::Int32(1))]));
        let sub2 = Value::Document(doc(&[("y", Value::Int32(2))]));
        let cond = doc(&[("$or", Value::Array(vec![sub1, sub2]))]);
        let expr = parse_condition(&cond).unwrap();

        assert!(evaluate(&expr, &doc(&[("x", Value::Int32(1))])).unwrap());
        assert!(evaluate(&expr, &doc(&[("y", Value::Int32(2))])).unwrap());
        assert!(!evaluate(&expr, &doc(&[("x", Value::Int32(2)), ("y", Value::Int32(1))])).unwrap());
    }

    #[test]
    fn eval_nor() {
        // { $nor: [ {x: 1}, {y: 2} ] }
        let sub1 = Value::Document(doc(&[("x", Value::Int32(1))]));
        let sub2 = Value::Document(doc(&[("y", Value::Int32(2))]));
        let cond = doc(&[("$nor", Value::Array(vec![sub1, sub2]))]);
        let expr = parse_condition(&cond).unwrap();

        assert!(!evaluate(&expr, &doc(&[("x", Value::Int32(1))])).unwrap());
        assert!(!evaluate(&expr, &doc(&[("y", Value::Int32(2))])).unwrap());
        assert!(evaluate(&expr, &doc(&[("x", Value::Int32(9))])).unwrap());
    }

    #[test]
    fn eval_nested_path() {
        // { "a.b": 10 }
        let cond = doc(&[("a.b", Value::Int32(10))]);
        let expr = parse_condition(&cond).unwrap();

        let inner = doc(&[("b", Value::Int32(10))]);
        let d = doc(&[("a", Value::Document(inner))]);
        assert!(evaluate(&expr, &d).unwrap());
    }

    #[test]
    fn eval_multi_field_implicit_and() {
        // { x: 1, y: 2 } → implicit $and
        let cond = doc(&[("x", Value::Int32(1)), ("y", Value::Int32(2))]);
        let expr = parse_condition(&cond).unwrap();

        assert!(evaluate(&expr, &doc(&[("x", Value::Int32(1)), ("y", Value::Int32(2))])).unwrap());
        assert!(!evaluate(&expr, &doc(&[("x", Value::Int32(1)), ("y", Value::Int32(3))])).unwrap());
    }
}
