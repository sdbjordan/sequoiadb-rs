use sdb_bson::{Document, Value};
use sdb_common::{Result, SdbError};

/// A single modification operation parsed from the rule document.
#[derive(Debug, Clone)]
enum ModifyOp {
    Set { path: String, value: Value },
    Unset { path: String },
    Inc { path: String, value: Value },
}

/// Document modifier — applies $set, $unset, $inc modifications.
/// Corresponds to the original mthModifier class.
pub struct Modifier {
    ops: Vec<ModifyOp>,
}

impl Modifier {
    pub fn new(rule: Document) -> Result<Self> {
        let ops = parse_rule(&rule)?;
        Ok(Self { ops })
    }

    /// Apply the modification rule to a document, returning the modified document.
    pub fn modify(&self, doc: &Document) -> Result<Document> {
        let mut result = doc.clone();
        for op in &self.ops {
            result = apply_op(result, op)?;
        }
        Ok(result)
    }
}

/// Parse a rule document like `{ $set: { a: 1 }, $unset: { b: "" }, $inc: { c: 5 } }`
fn parse_rule(rule: &Document) -> Result<Vec<ModifyOp>> {
    let mut ops = Vec::new();

    for elem in rule.iter() {
        let op_name = &elem.key;
        let fields = match &elem.value {
            Value::Document(d) => d,
            _ => return Err(SdbError::InvalidArg),
        };

        match op_name.as_str() {
            "$set" => {
                for f in fields.iter() {
                    ops.push(ModifyOp::Set {
                        path: f.key.clone(),
                        value: f.value.clone(),
                    });
                }
            }
            "$unset" => {
                for f in fields.iter() {
                    ops.push(ModifyOp::Unset {
                        path: f.key.clone(),
                    });
                }
            }
            "$inc" => {
                for f in fields.iter() {
                    match &f.value {
                        Value::Int32(_) | Value::Int64(_) | Value::Double(_) => {}
                        _ => return Err(SdbError::InvalidArg),
                    }
                    ops.push(ModifyOp::Inc {
                        path: f.key.clone(),
                        value: f.value.clone(),
                    });
                }
            }
            _ => return Err(SdbError::InvalidArg),
        }
    }

    Ok(ops)
}

/// Apply a single ModifyOp to a document, returning the new document.
/// For v1, only top-level fields are supported (no nested dot-path modification).
fn apply_op(doc: Document, op: &ModifyOp) -> Result<Document> {
    match op {
        ModifyOp::Set { path, value } => {
            if path.contains('.') {
                return apply_nested_set(doc, path, value);
            }
            // Rebuild document: replace existing key or append
            let mut new_doc = Document::new();
            let mut found = false;
            for elem in doc.iter() {
                if elem.key == *path {
                    new_doc.insert(&elem.key, value.clone());
                    found = true;
                } else {
                    new_doc.insert(&elem.key, elem.value.clone());
                }
            }
            if !found {
                new_doc.insert(path.as_str(), value.clone());
            }
            Ok(new_doc)
        }
        ModifyOp::Unset { path } => {
            if path.contains('.') {
                return apply_nested_unset(doc, path);
            }
            let mut new_doc = Document::new();
            for elem in doc.iter() {
                if elem.key != *path {
                    new_doc.insert(&elem.key, elem.value.clone());
                }
            }
            Ok(new_doc)
        }
        ModifyOp::Inc { path, value } => {
            if path.contains('.') {
                // Nested $inc not supported in v1
                return Err(SdbError::InvalidArg);
            }
            let current = doc.get(path);
            let new_val = match (current, value) {
                (Some(Value::Int32(a)), Value::Int32(b)) => Value::Int32(a + b),
                (Some(Value::Int64(a)), Value::Int64(b)) => Value::Int64(a + b),
                (Some(Value::Int64(a)), Value::Int32(b)) => Value::Int64(a + *b as i64),
                (Some(Value::Int32(a)), Value::Int64(b)) => Value::Int64(*a as i64 + b),
                (Some(Value::Double(a)), Value::Double(b)) => Value::Double(a + b),
                (Some(Value::Double(a)), Value::Int32(b)) => Value::Double(a + *b as f64),
                (Some(Value::Double(a)), Value::Int64(b)) => Value::Double(a + *b as f64),
                (Some(Value::Int32(a)), Value::Double(b)) => Value::Double(*a as f64 + b),
                (Some(Value::Int64(a)), Value::Double(b)) => Value::Double(*a as f64 + b),
                (None, v) => v.clone(), // Missing field: treat as 0, result is the inc value
                _ => return Err(SdbError::InvalidArg),
            };

            let mut new_doc = Document::new();
            let mut found = false;
            for elem in doc.iter() {
                if elem.key == *path {
                    new_doc.insert(&elem.key, new_val.clone());
                    found = true;
                } else {
                    new_doc.insert(&elem.key, elem.value.clone());
                }
            }
            if !found {
                new_doc.insert(path.as_str(), new_val);
            }
            Ok(new_doc)
        }
    }
}

/// Apply $set on a nested dot-path (e.g. "a.b.c").
fn apply_nested_set(doc: Document, path: &str, value: &Value) -> Result<Document> {
    let (head, rest) = path.split_once('.').unwrap();
    let mut new_doc = Document::new();
    let mut found = false;

    for elem in doc.iter() {
        if elem.key == head {
            found = true;
            match &elem.value {
                Value::Document(inner) => {
                    let updated = apply_op(
                        inner.clone(),
                        &ModifyOp::Set {
                            path: rest.to_string(),
                            value: value.clone(),
                        },
                    )?;
                    new_doc.insert(&elem.key, Value::Document(updated));
                }
                _ => {
                    // Overwrite non-document with nested structure
                    let inner = apply_op(
                        Document::new(),
                        &ModifyOp::Set {
                            path: rest.to_string(),
                            value: value.clone(),
                        },
                    )?;
                    new_doc.insert(&elem.key, Value::Document(inner));
                }
            }
        } else {
            new_doc.insert(&elem.key, elem.value.clone());
        }
    }

    if !found {
        let inner = apply_op(
            Document::new(),
            &ModifyOp::Set {
                path: rest.to_string(),
                value: value.clone(),
            },
        )?;
        new_doc.insert(head, Value::Document(inner));
    }

    Ok(new_doc)
}

/// Apply $unset on a nested dot-path.
fn apply_nested_unset(doc: Document, path: &str) -> Result<Document> {
    let (head, rest) = path.split_once('.').unwrap();
    let mut new_doc = Document::new();

    for elem in doc.iter() {
        if elem.key == head {
            if let Value::Document(inner) = &elem.value {
                let updated = apply_op(
                    inner.clone(),
                    &ModifyOp::Unset {
                        path: rest.to_string(),
                    },
                )?;
                new_doc.insert(&elem.key, Value::Document(updated));
            } else {
                new_doc.insert(&elem.key, elem.value.clone());
            }
        } else {
            new_doc.insert(&elem.key, elem.value.clone());
        }
    }

    Ok(new_doc)
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

    #[test]
    fn set_existing_field() {
        let rule = doc(&[("$set", Value::Document(doc(&[("x", Value::Int32(99))])))]);
        let m = Modifier::new(rule).unwrap();
        let input = doc(&[("x", Value::Int32(1)), ("y", Value::Int32(2))]);
        let result = m.modify(&input).unwrap();
        assert_eq!(result.get("x"), Some(&Value::Int32(99)));
        assert_eq!(result.get("y"), Some(&Value::Int32(2)));
    }

    #[test]
    fn set_new_field() {
        let rule = doc(&[(
            "$set",
            Value::Document(doc(&[("z", Value::String("new".into()))])),
        )]);
        let m = Modifier::new(rule).unwrap();
        let input = doc(&[("x", Value::Int32(1))]);
        let result = m.modify(&input).unwrap();
        assert_eq!(result.get("x"), Some(&Value::Int32(1)));
        assert_eq!(result.get("z"), Some(&Value::String("new".into())));
    }

    #[test]
    fn unset_field() {
        let rule = doc(&[(
            "$unset",
            Value::Document(doc(&[("x", Value::String("".into()))])),
        )]);
        let m = Modifier::new(rule).unwrap();
        let input = doc(&[("x", Value::Int32(1)), ("y", Value::Int32(2))]);
        let result = m.modify(&input).unwrap();
        assert_eq!(result.get("x"), None);
        assert_eq!(result.get("y"), Some(&Value::Int32(2)));
    }

    #[test]
    fn unset_missing_field() {
        let rule = doc(&[(
            "$unset",
            Value::Document(doc(&[("z", Value::String("".into()))])),
        )]);
        let m = Modifier::new(rule).unwrap();
        let input = doc(&[("x", Value::Int32(1))]);
        let result = m.modify(&input).unwrap();
        assert_eq!(result.get("x"), Some(&Value::Int32(1)));
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn inc_existing() {
        let rule = doc(&[("$inc", Value::Document(doc(&[("count", Value::Int32(5))])))]);
        let m = Modifier::new(rule).unwrap();
        let input = doc(&[("count", Value::Int32(10))]);
        let result = m.modify(&input).unwrap();
        assert_eq!(result.get("count"), Some(&Value::Int32(15)));
    }

    #[test]
    fn inc_missing_field() {
        let rule = doc(&[("$inc", Value::Document(doc(&[("count", Value::Int32(3))])))]);
        let m = Modifier::new(rule).unwrap();
        let input = doc(&[("x", Value::Int32(1))]);
        let result = m.modify(&input).unwrap();
        assert_eq!(result.get("count"), Some(&Value::Int32(3)));
    }

    #[test]
    fn inc_cross_numeric() {
        let rule = doc(&[("$inc", Value::Document(doc(&[("val", Value::Double(1.5))])))]);
        let m = Modifier::new(rule).unwrap();
        let input = doc(&[("val", Value::Int32(10))]);
        let result = m.modify(&input).unwrap();
        assert_eq!(result.get("val"), Some(&Value::Double(11.5)));
    }

    #[test]
    fn set_nested_path() {
        let rule = doc(&[("$set", Value::Document(doc(&[("a.b", Value::Int32(42))])))]);
        let m = Modifier::new(rule).unwrap();
        let inner = doc(&[("b", Value::Int32(1))]);
        let input = doc(&[("a", Value::Document(inner))]);
        let result = m.modify(&input).unwrap();
        if let Some(Value::Document(a)) = result.get("a") {
            assert_eq!(a.get("b"), Some(&Value::Int32(42)));
        } else {
            panic!("expected nested document");
        }
    }

    #[test]
    fn unset_nested_path() {
        let rule = doc(&[(
            "$unset",
            Value::Document(doc(&[("a.b", Value::String("".into()))])),
        )]);
        let m = Modifier::new(rule).unwrap();
        let inner = doc(&[("b", Value::Int32(1)), ("c", Value::Int32(2))]);
        let input = doc(&[("a", Value::Document(inner))]);
        let result = m.modify(&input).unwrap();
        if let Some(Value::Document(a)) = result.get("a") {
            assert_eq!(a.get("b"), None);
            assert_eq!(a.get("c"), Some(&Value::Int32(2)));
        } else {
            panic!("expected nested document");
        }
    }

    #[test]
    fn combined_set_unset_inc() {
        let mut rule = Document::new();
        rule.insert(
            "$set",
            Value::Document(doc(&[("name", Value::String("updated".into()))])),
        );
        rule.insert(
            "$unset",
            Value::Document(doc(&[("temp", Value::String("".into()))])),
        );
        rule.insert(
            "$inc",
            Value::Document(doc(&[("version", Value::Int32(1))])),
        );
        let m = Modifier::new(rule).unwrap();

        let input = doc(&[
            ("name", Value::String("old".into())),
            ("temp", Value::Int32(999)),
            ("version", Value::Int32(3)),
        ]);
        let result = m.modify(&input).unwrap();
        assert_eq!(result.get("name"), Some(&Value::String("updated".into())));
        assert_eq!(result.get("temp"), None);
        assert_eq!(result.get("version"), Some(&Value::Int32(4)));
    }

    #[test]
    fn invalid_rule_operator() {
        let rule = doc(&[("$push", Value::Document(doc(&[("x", Value::Int32(1))])))]);
        assert!(Modifier::new(rule).is_err());
    }

    #[test]
    fn invalid_inc_non_numeric() {
        let rule = doc(&[(
            "$inc",
            Value::Document(doc(&[("x", Value::String("bad".into()))])),
        )]);
        assert!(Modifier::new(rule).is_err());
    }
}
