use sdb_bson::Document;
use sdb_common::Result;

use crate::expression::{evaluate, parse_condition, MatchExpression};

/// Query matcher — evaluates whether a document matches a query condition.
/// Corresponds to the original mthMatcher class.
pub struct Matcher {
    condition: Document,
    expression: MatchExpression,
}

impl Matcher {
    pub fn new(condition: Document) -> Result<Self> {
        let expression = parse_condition(&condition)?;
        Ok(Self {
            condition,
            expression,
        })
    }

    /// Returns true if the document matches the condition.
    pub fn matches(&self, doc: &Document) -> Result<bool> {
        evaluate(&self.expression, doc)
    }

    /// Returns true if this matcher matches everything (empty condition).
    pub fn is_match_all(&self) -> bool {
        self.condition.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sdb_bson::Value;

    fn doc(pairs: &[(&str, Value)]) -> Document {
        let mut d = Document::new();
        for (k, v) in pairs {
            d.insert(*k, v.clone());
        }
        d
    }

    #[test]
    fn match_all_empty_condition() {
        let m = Matcher::new(Document::new()).unwrap();
        assert!(m.is_match_all());
        assert!(m.matches(&doc(&[("x", Value::Int32(1))])).unwrap());
    }

    #[test]
    fn match_eq() {
        let m = Matcher::new(doc(&[("name", Value::String("alice".into()))])).unwrap();
        assert!(!m.is_match_all());
        assert!(m
            .matches(&doc(&[("name", Value::String("alice".into()))]))
            .unwrap());
        assert!(!m
            .matches(&doc(&[("name", Value::String("bob".into()))]))
            .unwrap());
    }

    #[test]
    fn match_range() {
        let ops = doc(&[("$gte", Value::Int32(18)), ("$lt", Value::Int32(65))]);
        let cond = doc(&[("age", Value::Document(ops))]);
        let m = Matcher::new(cond).unwrap();

        assert!(m.matches(&doc(&[("age", Value::Int32(18))])).unwrap());
        assert!(m.matches(&doc(&[("age", Value::Int32(64))])).unwrap());
        assert!(!m.matches(&doc(&[("age", Value::Int32(17))])).unwrap());
        assert!(!m.matches(&doc(&[("age", Value::Int32(65))])).unwrap());
    }
}
