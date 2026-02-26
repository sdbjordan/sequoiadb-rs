use crate::ast::{DeleteStatement, InsertStatement, SelectStatement, SqlStatement, UpdateStatement};
use sdb_bson::{Document, Value};
use sdb_common::{Result, SdbError};
use sdb_opt::access_path::AccessPath;
use sdb_opt::plan::{PlanNode, PlanType, QueryPlan};

/// Query Graph Model — converts SQL AST to query plans.
pub struct QueryGraph;

impl QueryGraph {
    pub fn new() -> Self {
        Self
    }

    /// Convert a SQL statement into a query plan.
    pub fn to_plan(&self, stmt: &SqlStatement) -> Result<QueryPlan> {
        match stmt {
            SqlStatement::Select(s) => self.select_to_plan(s),
            SqlStatement::Insert(_) | SqlStatement::Update(_) | SqlStatement::Delete(_)
            | SqlStatement::CreateTable(_) | SqlStatement::DropTable(_) => {
                // DML/DDL statements don't produce scan-based query plans.
                // For v1 we generate a minimal plan that the executor can interpret.
                match stmt {
                    SqlStatement::Insert(s) => self.insert_to_plan(s),
                    SqlStatement::Update(s) => self.update_to_plan(s),
                    SqlStatement::Delete(s) => self.delete_to_plan(s),
                    _ => Err(SdbError::InvalidArg),
                }
            }
        }
    }

    fn select_to_plan(&self, s: &SelectStatement) -> Result<QueryPlan> {
        let condition = s.condition.as_ref().map(|c| parse_where_to_bson(c)).transpose()?;
        let selector = if s.columns.len() == 1 && s.columns[0] == "*" {
            None
        } else {
            let mut doc = Document::new();
            for col in &s.columns {
                doc.insert(col.as_str(), Value::Int32(1));
            }
            Some(doc)
        };
        let order_by = if s.order_by.is_empty() {
            None
        } else {
            let mut doc = Document::new();
            for (col, asc) in &s.order_by {
                doc.insert(col.as_str(), Value::Int32(if *asc { 1 } else { -1 }));
            }
            Some(doc)
        };

        let mut root = PlanNode {
            plan_type: PlanType::TableScan,
            children: Vec::new(),
            estimated_cost: 0.0,
            estimated_rows: 0,
            access_path: Some(AccessPath::TableScan),
            sort_fields: None,
        };

        if condition.is_some() {
            root = PlanNode {
                plan_type: PlanType::Filter,
                children: vec![root],
                estimated_cost: 0.0,
                estimated_rows: 0,
                access_path: None,
                sort_fields: None,
            };
        }

        if let Some(ref ob) = order_by {
            root = PlanNode {
                plan_type: PlanType::Sort,
                children: vec![root],
                estimated_cost: 0.0,
                estimated_rows: 0,
                access_path: None,
                sort_fields: Some(ob.clone()),
            };
        }

        if s.offset.is_some() {
            root = PlanNode {
                plan_type: PlanType::Skip,
                children: vec![root],
                estimated_cost: 0.0,
                estimated_rows: 0,
                access_path: None,
                sort_fields: None,
            };
        }

        if s.limit.is_some() {
            root = PlanNode {
                plan_type: PlanType::Limit,
                children: vec![root],
                estimated_cost: 0.0,
                estimated_rows: 0,
                access_path: None,
                sort_fields: None,
            };
        }

        if selector.is_some() {
            root = PlanNode {
                plan_type: PlanType::Project,
                children: vec![root],
                estimated_cost: 0.0,
                estimated_rows: 0,
                access_path: None,
                sort_fields: None,
            };
        }

        Ok(QueryPlan {
            root,
            collection: s.table.clone(),
            condition,
            selector,
            order_by,
            skip: s.offset.unwrap_or(0),
            limit: s.limit.unwrap_or(-1),
        })
    }

    fn insert_to_plan(&self, s: &InsertStatement) -> Result<QueryPlan> {
        let root = PlanNode {
            plan_type: PlanType::TableScan,
            children: Vec::new(),
            estimated_cost: 0.0,
            estimated_rows: s.values.len() as u64,
            access_path: Some(AccessPath::TableScan),
            sort_fields: None,
        };
        Ok(QueryPlan {
            root,
            collection: s.table.clone(),
            condition: None,
            selector: None,
            order_by: None,
            skip: 0,
            limit: -1,
        })
    }

    fn update_to_plan(&self, s: &UpdateStatement) -> Result<QueryPlan> {
        let condition = s.condition.as_ref().map(|c| parse_where_to_bson(c)).transpose()?;
        let root = PlanNode {
            plan_type: PlanType::TableScan,
            children: Vec::new(),
            estimated_cost: 0.0,
            estimated_rows: 0,
            access_path: Some(AccessPath::TableScan),
            sort_fields: None,
        };
        Ok(QueryPlan {
            root,
            collection: s.table.clone(),
            condition,
            selector: None,
            order_by: None,
            skip: 0,
            limit: -1,
        })
    }

    fn delete_to_plan(&self, s: &DeleteStatement) -> Result<QueryPlan> {
        let condition = s.condition.as_ref().map(|c| parse_where_to_bson(c)).transpose()?;
        let root = PlanNode {
            plan_type: PlanType::TableScan,
            children: Vec::new(),
            estimated_cost: 0.0,
            estimated_rows: 0,
            access_path: Some(AccessPath::TableScan),
            sort_fields: None,
        };
        Ok(QueryPlan {
            root,
            collection: s.table.clone(),
            condition,
            selector: None,
            order_by: None,
            skip: 0,
            limit: -1,
        })
    }
}

impl Default for QueryGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a simple WHERE clause string into a BSON condition document.
/// Supports: field = value [AND field = value ...]
fn parse_where_to_bson(clause: &str) -> Result<Document> {
    let mut doc = Document::new();
    // Split on AND (case insensitive)
    let parts: Vec<&str> = clause.split_whitespace().collect();
    let mut i = 0;
    while i < parts.len() {
        if i + 2 >= parts.len() {
            // Need at least: field = value
            if parts[i].eq_ignore_ascii_case("AND") {
                i += 1;
                continue;
            }
            break;
        }

        if parts[i].eq_ignore_ascii_case("AND") {
            i += 1;
            continue;
        }

        let field = parts[i];
        let op = parts[i + 1];
        let value = parts[i + 2];

        if op != "=" {
            // v1: only support equality
            return Err(SdbError::InvalidArg);
        }

        let bson_val = if value.starts_with('\'') && value.ends_with('\'') {
            Value::String(value[1..value.len()-1].to_string())
        } else if let Ok(n) = value.parse::<i64>() {
            if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                Value::Int32(n as i32)
            } else {
                Value::Int64(n)
            }
        } else if let Ok(f) = value.parse::<f64>() {
            Value::Double(f)
        } else {
            Value::String(value.to_string())
        };

        doc.insert(field, bson_val);
        i += 3;
    }
    Ok(doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SqlParser;

    #[test]
    fn select_to_plan_basic() {
        let parser = SqlParser::new();
        let stmt = parser.parse("SELECT * FROM users").unwrap();
        let qg = QueryGraph::new();
        let plan = qg.to_plan(&stmt).unwrap();
        assert_eq!(plan.collection, "users");
        assert!(plan.condition.is_none());
        assert!(plan.selector.is_none());
        assert_eq!(plan.root.plan_type, PlanType::TableScan);
    }

    #[test]
    fn select_with_where_generates_filter() {
        let parser = SqlParser::new();
        let stmt = parser.parse("SELECT name FROM users WHERE age = 25").unwrap();
        let qg = QueryGraph::new();
        let plan = qg.to_plan(&stmt).unwrap();
        assert!(plan.condition.is_some());
        let cond = plan.condition.unwrap();
        assert_eq!(cond.get("age"), Some(&Value::Int32(25)));
        assert!(plan.selector.is_some());
    }

    #[test]
    fn select_with_order_and_limit() {
        let parser = SqlParser::new();
        let stmt = parser.parse("SELECT * FROM t ORDER BY name ASC LIMIT 10").unwrap();
        let qg = QueryGraph::new();
        let plan = qg.to_plan(&stmt).unwrap();
        assert!(plan.order_by.is_some());
        assert_eq!(plan.limit, 10);
    }

    #[test]
    fn delete_to_plan() {
        let parser = SqlParser::new();
        let stmt = parser.parse("DELETE FROM users WHERE id = 5").unwrap();
        let qg = QueryGraph::new();
        let plan = qg.to_plan(&stmt).unwrap();
        assert_eq!(plan.collection, "users");
        assert!(plan.condition.is_some());
    }

    #[test]
    fn where_clause_parsing() {
        let doc = parse_where_to_bson("name = 'alice' AND age = 30").unwrap();
        assert_eq!(doc.get("name"), Some(&Value::String("alice".into())));
        assert_eq!(doc.get("age"), Some(&Value::Int32(30)));
    }
}
