use crate::stage::{Stage, StageType};
use sdb_bson::{Document, Value};
use sdb_common::{Result, SdbError};
use sdb_mth::Matcher;

/// Aggregation pipeline — a sequence of stages applied to input documents.
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

    /// Execute the pipeline against a set of input documents.
    pub fn execute_with_input(&self, input: Vec<Document>) -> Result<Vec<Document>> {
        let mut docs = input;
        for stage in &self.stages {
            docs = execute_stage(stage, docs)?;
        }
        Ok(docs)
    }

    /// Execute the pipeline (no input — for use when collection scan provides docs).
    pub fn execute(&self) -> Result<Vec<Document>> {
        self.execute_with_input(Vec::new())
    }
}

fn execute_stage(stage: &Stage, docs: Vec<Document>) -> Result<Vec<Document>> {
    match stage.stage_type {
        StageType::Match => stage_match(&stage.spec, docs),
        StageType::Project => stage_project(&stage.spec, docs),
        StageType::Sort => stage_sort(&stage.spec, docs),
        StageType::Limit => stage_limit(&stage.spec, docs),
        StageType::Skip => stage_skip(&stage.spec, docs),
        StageType::Count => stage_count(&stage.spec, docs),
        StageType::Group => stage_group(&stage.spec, docs),
        StageType::Unwind => stage_unwind(&stage.spec, docs),
        StageType::Lookup => Err(SdbError::InvalidArg), // not implemented in v1
    }
}

/// $match — filter documents using sdb-mth Matcher.
fn stage_match(spec: &Document, docs: Vec<Document>) -> Result<Vec<Document>> {
    let matcher = Matcher::new(spec.clone())?;
    let mut result = Vec::new();
    for doc in docs {
        if matcher.matches(&doc)? {
            result.push(doc);
        }
    }
    Ok(result)
}

/// $project — include/exclude fields. spec: { "field": 1 } to include, { "field": 0 } to exclude.
fn stage_project(spec: &Document, docs: Vec<Document>) -> Result<Vec<Document>> {
    // Determine mode: if any field has value 1, it's inclusion mode; if 0, exclusion mode.
    let mut include_fields = Vec::new();
    let mut exclude_fields = Vec::new();
    for elem in spec.iter() {
        match &elem.value {
            Value::Int32(1) | Value::Int64(1) | Value::Boolean(true) => {
                include_fields.push(elem.key.as_str());
            }
            Value::Int32(0) | Value::Int64(0) | Value::Boolean(false) => {
                exclude_fields.push(elem.key.as_str());
            }
            _ => {}
        }
    }

    let mut result = Vec::new();
    for doc in docs {
        let mut projected = Document::new();
        if !include_fields.is_empty() {
            for field in &include_fields {
                if let Some(val) = doc.get(field) {
                    projected.insert(*field, val.clone());
                }
            }
        } else {
            // Exclusion mode: copy all except excluded
            for elem in doc.iter() {
                if !exclude_fields.contains(&elem.key.as_str()) {
                    projected.insert(&elem.key, elem.value.clone());
                }
            }
        }
        result.push(projected);
    }
    Ok(result)
}

/// $sort — sort by fields. spec: { "field": 1 } for ascending, { "field": -1 } for descending.
fn stage_sort(spec: &Document, mut docs: Vec<Document>) -> Result<Vec<Document>> {
    let sort_keys: Vec<(&str, bool)> = spec.iter()
        .map(|e| {
            let asc = match &e.value {
                Value::Int32(n) => *n >= 0,
                Value::Int64(n) => *n >= 0,
                _ => true,
            };
            (e.key.as_str(), asc)
        })
        .collect();

    docs.sort_by(|a, b| {
        for &(key, asc) in &sort_keys {
            let va = a.get(key);
            let vb = b.get(key);
            let ord = match (va, vb) {
                (Some(va), Some(vb)) => sdb_mth::compare::compare_values(va, vb),
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (None, None) => std::cmp::Ordering::Equal,
            };
            let ord = if asc { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
    Ok(docs)
}

/// $limit — take first N documents. spec: { "n": <limit> }
fn stage_limit(spec: &Document, docs: Vec<Document>) -> Result<Vec<Document>> {
    let n = get_i64(spec, "n").unwrap_or(i64::MAX);
    Ok(docs.into_iter().take(n as usize).collect())
}

/// $skip — skip first N documents. spec: { "n": <skip> }
fn stage_skip(spec: &Document, docs: Vec<Document>) -> Result<Vec<Document>> {
    let n = get_i64(spec, "n").unwrap_or(0);
    Ok(docs.into_iter().skip(n as usize).collect())
}

/// $count — replace all docs with a single { "field": count }. spec: { "as": "fieldName" }
fn stage_count(spec: &Document, docs: Vec<Document>) -> Result<Vec<Document>> {
    let field = match spec.get("as") {
        Some(Value::String(s)) => s.clone(),
        _ => "count".into(),
    };
    let count = docs.len() as i64;
    let mut result = Document::new();
    result.insert(&field, Value::Int64(count));
    Ok(vec![result])
}

/// $group — group by _id field and apply accumulators.
/// spec: { "_id": "$field", "total": { "$sum": "$val" }, "cnt": { "$sum": 1 } }
fn stage_group(spec: &Document, docs: Vec<Document>) -> Result<Vec<Document>> {
    use std::collections::HashMap;

    // Get the group-by field
    let id_field = match spec.get("_id") {
        Some(Value::String(s)) if s.starts_with('$') => &s[1..],
        _ => return Err(SdbError::InvalidArg),
    };

    // Collect accumulators
    struct Accumulator {
        op: String,       // "$sum", "$count"
        source: String,   // field path (without $), or "" for $sum: 1
        is_literal: bool,
        literal_val: f64,
    }

    let mut accumulators: Vec<(String, Accumulator)> = Vec::new();
    for elem in spec.iter() {
        if elem.key == "_id" {
            continue;
        }
        match &elem.value {
            Value::Document(acc_doc) => {
                for acc_elem in acc_doc.iter() {
                    let op = acc_elem.key.clone();
                    let (source, is_literal, literal_val) = match &acc_elem.value {
                        Value::String(s) if s.starts_with('$') => (s[1..].to_string(), false, 0.0),
                        Value::Int32(n) => (String::new(), true, *n as f64),
                        Value::Int64(n) => (String::new(), true, *n as f64),
                        Value::Double(n) => (String::new(), true, *n),
                        _ => return Err(SdbError::InvalidArg),
                    };
                    accumulators.push((elem.key.clone(), Accumulator { op, source, is_literal, literal_val }));
                }
            }
            _ => return Err(SdbError::InvalidArg),
        }
    }

    // Group documents by the _id field value
    // key → (id_value, vec of running sums)
    let mut group_order: Vec<String> = Vec::new();
    let mut group_data: HashMap<String, (Value, Vec<f64>)> = HashMap::new();

    for doc in &docs {
        let key_val = doc.get(id_field).cloned().unwrap_or(Value::Null);
        let key_str = format!("{:?}", key_val);

        if !group_data.contains_key(&key_str) {
            group_order.push(key_str.clone());
            group_data.insert(key_str.clone(), (key_val.clone(), vec![0.0; accumulators.len()]));
        }

        let (_, ref mut sums) = group_data.get_mut(&key_str).unwrap();
        for (i, (_, acc)) in accumulators.iter().enumerate() {
            if acc.op == "$sum" {
                if acc.is_literal {
                    sums[i] += acc.literal_val;
                } else {
                    let val = doc.get(&acc.source);
                    let n = match val {
                        Some(Value::Int32(n)) => *n as f64,
                        Some(Value::Int64(n)) => *n as f64,
                        Some(Value::Double(n)) => *n,
                        _ => 0.0,
                    };
                    sums[i] += n;
                }
            }
        }
    }

    // Build result documents
    let mut result = Vec::new();
    for key_str in &group_order {
        let (id_val, sums) = group_data.get(key_str).unwrap();
        let mut doc = Document::new();
        doc.insert("_id", id_val.clone());
        for (i, (name, _)) in accumulators.iter().enumerate() {
            let v = sums[i];
            if v == (v as i64) as f64 {
                doc.insert(name.as_str(), Value::Int64(v as i64));
            } else {
                doc.insert(name.as_str(), Value::Double(v));
            }
        }
        result.push(doc);
    }

    Ok(result)
}

/// $unwind — deconstruct an array field. spec: { "path": "$arrayField" }
fn stage_unwind(spec: &Document, docs: Vec<Document>) -> Result<Vec<Document>> {
    let path = match spec.get("path") {
        Some(Value::String(s)) if s.starts_with('$') => &s[1..],
        _ => return Err(SdbError::InvalidArg),
    };

    let mut result = Vec::new();
    for doc in docs {
        match doc.get(path) {
            Some(Value::Array(arr)) => {
                for item in arr {
                    let mut new_doc = Document::new();
                    for elem in doc.iter() {
                        if elem.key == path {
                            new_doc.insert(path, item.clone());
                        } else {
                            new_doc.insert(&elem.key, elem.value.clone());
                        }
                    }
                    result.push(new_doc);
                }
            }
            _ => {
                result.push(doc);
            }
        }
    }
    Ok(result)
}

fn get_i64(doc: &Document, key: &str) -> Option<i64> {
    match doc.get(key) {
        Some(Value::Int32(n)) => Some(*n as i64),
        Some(Value::Int64(n)) => Some(*n),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stage::Stage;

    fn doc(pairs: &[(&str, Value)]) -> Document {
        let mut d = Document::new();
        for (k, v) in pairs {
            d.insert(*k, v.clone());
        }
        d
    }

    fn input_docs() -> Vec<Document> {
        vec![
            doc(&[("name", Value::String("alice".into())), ("age", Value::Int32(30)), ("dept", Value::String("eng".into()))]),
            doc(&[("name", Value::String("bob".into())), ("age", Value::Int32(25)), ("dept", Value::String("eng".into()))]),
            doc(&[("name", Value::String("carol".into())), ("age", Value::Int32(35)), ("dept", Value::String("sales".into()))]),
        ]
    }

    #[test]
    fn match_stage() {
        let pipeline = Pipeline::new("test", vec![
            Stage { stage_type: StageType::Match, spec: doc(&[("dept", Value::String("eng".into()))]) },
        ]);
        let result = pipeline.execute_with_input(input_docs()).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn project_include() {
        let pipeline = Pipeline::new("test", vec![
            Stage { stage_type: StageType::Project, spec: doc(&[("name", Value::Int32(1))]) },
        ]);
        let result = pipeline.execute_with_input(input_docs()).unwrap();
        assert_eq!(result.len(), 3);
        assert!(result[0].get("name").is_some());
        assert!(result[0].get("age").is_none());
    }

    #[test]
    fn project_exclude() {
        let pipeline = Pipeline::new("test", vec![
            Stage { stage_type: StageType::Project, spec: doc(&[("age", Value::Int32(0))]) },
        ]);
        let result = pipeline.execute_with_input(input_docs()).unwrap();
        assert!(result[0].get("age").is_none());
        assert!(result[0].get("name").is_some());
    }

    #[test]
    fn sort_ascending() {
        let pipeline = Pipeline::new("test", vec![
            Stage { stage_type: StageType::Sort, spec: doc(&[("age", Value::Int32(1))]) },
        ]);
        let result = pipeline.execute_with_input(input_docs()).unwrap();
        assert_eq!(result[0].get("name"), Some(&Value::String("bob".into())));
        assert_eq!(result[2].get("name"), Some(&Value::String("carol".into())));
    }

    #[test]
    fn sort_descending() {
        let pipeline = Pipeline::new("test", vec![
            Stage { stage_type: StageType::Sort, spec: doc(&[("age", Value::Int32(-1))]) },
        ]);
        let result = pipeline.execute_with_input(input_docs()).unwrap();
        assert_eq!(result[0].get("name"), Some(&Value::String("carol".into())));
    }

    #[test]
    fn limit_and_skip() {
        let pipeline = Pipeline::new("test", vec![
            Stage { stage_type: StageType::Skip, spec: doc(&[("n", Value::Int32(1))]) },
            Stage { stage_type: StageType::Limit, spec: doc(&[("n", Value::Int32(1))]) },
        ]);
        let result = pipeline.execute_with_input(input_docs()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("name"), Some(&Value::String("bob".into())));
    }

    #[test]
    fn count_stage() {
        let pipeline = Pipeline::new("test", vec![
            Stage { stage_type: StageType::Match, spec: doc(&[("dept", Value::String("eng".into()))]) },
            Stage { stage_type: StageType::Count, spec: doc(&[("as", Value::String("total".into()))]) },
        ]);
        let result = pipeline.execute_with_input(input_docs()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("total"), Some(&Value::Int64(2)));
    }

    #[test]
    fn group_sum() {
        let docs = vec![
            doc(&[("dept", Value::String("eng".into())), ("salary", Value::Int32(100))]),
            doc(&[("dept", Value::String("eng".into())), ("salary", Value::Int32(200))]),
            doc(&[("dept", Value::String("sales".into())), ("salary", Value::Int32(150))]),
        ];
        let mut group_spec = Document::new();
        group_spec.insert("_id", Value::String("$dept".into()));
        let mut sum_doc = Document::new();
        sum_doc.insert("$sum", Value::String("$salary".into()));
        group_spec.insert("total", Value::Document(sum_doc));

        let pipeline = Pipeline::new("test", vec![
            Stage { stage_type: StageType::Group, spec: group_spec },
        ]);
        let result = pipeline.execute_with_input(docs).unwrap();
        assert_eq!(result.len(), 2);
        // Find eng group
        let eng = result.iter().find(|d| d.get("_id") == Some(&Value::String("eng".into()))).unwrap();
        assert_eq!(eng.get("total"), Some(&Value::Int64(300)));
    }

    #[test]
    fn group_count() {
        let docs = vec![
            doc(&[("dept", Value::String("eng".into()))]),
            doc(&[("dept", Value::String("eng".into()))]),
            doc(&[("dept", Value::String("sales".into()))]),
        ];
        let mut group_spec = Document::new();
        group_spec.insert("_id", Value::String("$dept".into()));
        let mut cnt_doc = Document::new();
        cnt_doc.insert("$sum", Value::Int32(1));
        group_spec.insert("cnt", Value::Document(cnt_doc));

        let pipeline = Pipeline::new("test", vec![
            Stage { stage_type: StageType::Group, spec: group_spec },
        ]);
        let result = pipeline.execute_with_input(docs).unwrap();
        let eng = result.iter().find(|d| d.get("_id") == Some(&Value::String("eng".into()))).unwrap();
        assert_eq!(eng.get("cnt"), Some(&Value::Int64(2)));
    }

    #[test]
    fn unwind_array() {
        let mut d = Document::new();
        d.insert("name", Value::String("alice".into()));
        d.insert("tags", Value::Array(vec![
            Value::String("a".into()),
            Value::String("b".into()),
        ]));
        let pipeline = Pipeline::new("test", vec![
            Stage { stage_type: StageType::Unwind, spec: doc(&[("path", Value::String("$tags".into()))]) },
        ]);
        let result = pipeline.execute_with_input(vec![d]).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].get("tags"), Some(&Value::String("a".into())));
        assert_eq!(result[1].get("tags"), Some(&Value::String("b".into())));
    }

    #[test]
    fn multi_stage_pipeline() {
        let pipeline = Pipeline::new("test", vec![
            Stage { stage_type: StageType::Match, spec: doc(&[("dept", Value::String("eng".into()))]) },
            Stage { stage_type: StageType::Sort, spec: doc(&[("age", Value::Int32(1))]) },
            Stage { stage_type: StageType::Project, spec: doc(&[("name", Value::Int32(1))]) },
        ]);
        let result = pipeline.execute_with_input(input_docs()).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].get("name"), Some(&Value::String("bob".into())));
        assert!(result[0].get("age").is_none());
    }
}
