use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use sdb_bson::{Document, Value};
use sdb_common::{Result, SdbError};
use sdb_dms::StorageUnit;
use sdb_ixm::{BTreeIndex, Index, KeyRange};
use sdb_mth::Matcher;
use sdb_opt::access_path::AccessPath;
use sdb_opt::plan::{PlanType, QueryPlan};

use crate::cursor::Cursor;

/// Bundles storage + indexes for a collection.
pub struct CollectionHandle {
    pub storage: Arc<StorageUnit>,
    pub indexes: Vec<Arc<RwLock<BTreeIndex>>>,
}

/// Query executor trait.
#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(&self, plan: &QueryPlan) -> Result<Cursor>;
}

/// Default executor — recursively evaluates PlanNode tree.
pub struct DefaultExecutor {
    collections: HashMap<String, CollectionHandle>,
}

impl Default for DefaultExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultExecutor {
    pub fn new() -> Self {
        Self {
            collections: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: String, handle: CollectionHandle) {
        self.collections.insert(name, handle);
    }

    fn execute_node(
        &self,
        node: &sdb_opt::plan::PlanNode,
        plan: &QueryPlan,
    ) -> Result<Vec<Document>> {
        match node.plan_type {
            PlanType::TableScan => {
                let handle = self
                    .collections
                    .get(&plan.collection)
                    .ok_or(SdbError::CollectionNotFound)?;
                let rows = handle.storage.scan();
                Ok(rows.into_iter().map(|(_, doc)| doc).collect())
            }

            PlanType::IndexScan => {
                let handle = self
                    .collections
                    .get(&plan.collection)
                    .ok_or(SdbError::CollectionNotFound)?;
                let index_name = match &node.access_path {
                    Some(AccessPath::IndexScan { index_name, .. }) => index_name,
                    _ => return Err(SdbError::InvalidArg),
                };
                let idx_lock = handle
                    .indexes
                    .iter()
                    .find(|i| i.read().unwrap().definition.name == *index_name)
                    .ok_or(SdbError::IndexNotFound)?;
                let idx = idx_lock.read().unwrap();
                let mut cursor = idx.scan(&KeyRange::all())?;
                let mut docs = Vec::new();
                while let Some(rid) = cursor.next() {
                    match handle.storage.find(rid) {
                        Ok(doc) => docs.push(doc),
                        Err(SdbError::NotFound) => {} // deleted between index and fetch
                        Err(e) => return Err(e),
                    }
                }
                Ok(docs)
            }

            PlanType::Filter => {
                let child = node
                    .children
                    .first()
                    .ok_or(SdbError::InvalidArg)?;
                let mut docs = self.execute_node(child, plan)?;
                if let Some(ref cond) = plan.condition {
                    let matcher = Matcher::new(cond.clone())?;
                    if !matcher.is_match_all() {
                        docs.retain(|doc| matcher.matches(doc).unwrap_or(false));
                    }
                }
                Ok(docs)
            }

            PlanType::Sort => {
                let child = node
                    .children
                    .first()
                    .ok_or(SdbError::InvalidArg)?;
                let mut docs = self.execute_node(child, plan)?;
                if let Some(ref sort_fields) = node.sort_fields {
                    let sf = sort_fields.clone();
                    docs.sort_by(|a, b| compare_docs(a, b, &sf));
                }
                Ok(docs)
            }

            PlanType::Skip => {
                let child = node
                    .children
                    .first()
                    .ok_or(SdbError::InvalidArg)?;
                let mut docs = self.execute_node(child, plan)?;
                let skip = plan.skip.max(0) as usize;
                if skip < docs.len() {
                    docs.drain(..skip);
                } else {
                    docs.clear();
                }
                Ok(docs)
            }

            PlanType::Limit => {
                let child = node
                    .children
                    .first()
                    .ok_or(SdbError::InvalidArg)?;
                let mut docs = self.execute_node(child, plan)?;
                let limit = plan.limit;
                if limit >= 0 {
                    docs.truncate(limit as usize);
                }
                Ok(docs)
            }

            PlanType::Project => {
                let child = node
                    .children
                    .first()
                    .ok_or(SdbError::InvalidArg)?;
                let docs = self.execute_node(child, plan)?;
                if let Some(ref selector) = plan.selector {
                    Ok(docs.iter().map(|doc| project(doc, selector)).collect())
                } else {
                    Ok(docs)
                }
            }

            PlanType::Merge => Err(SdbError::InvalidArg),
        }
    }
}

#[async_trait]
impl Executor for DefaultExecutor {
    async fn execute(&self, plan: &QueryPlan) -> Result<Cursor> {
        let docs = self.execute_node(&plan.root, plan)?;
        Ok(Cursor::with_results(0, docs))
    }
}

/// Compare two documents for sorting.
fn compare_docs(a: &Document, b: &Document, sort_fields: &Document) -> Ordering {
    for elem in sort_fields.iter() {
        let key = &elem.key;
        let dir: i8 = match &elem.value {
            Value::Int32(v) if *v < 0 => -1,
            Value::Int64(v) if *v < 0 => -1,
            Value::Double(v) if *v < 0.0 => -1,
            _ => 1,
        };
        let va = a.get(key);
        let vb = b.get(key);
        let cmp = match (va, vb) {
            (Some(va), Some(vb)) => sdb_mth::compare::compare_values(va, vb),
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        };
        let cmp = if dir < 0 { cmp.reverse() } else { cmp };
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

/// Project a document to only include fields specified in the selector.
fn project(doc: &Document, selector: &Document) -> Document {
    let mut out = Document::new();
    for elem in selector.iter() {
        if let Some(val) = doc.get(&elem.key) {
            out.insert(elem.key.clone(), val.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use sdb_bson::Value;
    use sdb_ixm::definition::IndexDefinition;
    use sdb_opt::access_path::{AccessPath, ScanDirection};
    use sdb_opt::plan::{PlanNode, PlanType, QueryPlan};
    use std::sync::RwLock;

    fn make_storage_with_docs(docs: &[Document]) -> Arc<StorageUnit> {
        let su = Arc::new(StorageUnit::new(1, 1));
        for doc in docs {
            su.insert(doc).unwrap();
        }
        su
    }

    fn doc_with(pairs: &[(&str, Value)]) -> Document {
        let mut d = Document::new();
        for (k, v) in pairs {
            d.insert(*k, v.clone());
        }
        d
    }

    fn leaf_node(plan_type: PlanType) -> PlanNode {
        PlanNode {
            plan_type,
            children: Vec::new(),
            estimated_cost: 0.0,
            estimated_rows: 0,
            access_path: if plan_type == PlanType::TableScan {
                Some(AccessPath::TableScan)
            } else {
                None
            },
            sort_fields: None,
        }
    }

    fn wrap_node(plan_type: PlanType, child: PlanNode) -> PlanNode {
        PlanNode {
            plan_type,
            children: vec![child],
            estimated_cost: 0.0,
            estimated_rows: 0,
            access_path: None,
            sort_fields: None,
        }
    }

    fn plan_for(collection: &str, root: PlanNode) -> QueryPlan {
        QueryPlan {
            root,
            collection: collection.to_string(),
            condition: None,
            selector: None,
            order_by: None,
            skip: 0,
            limit: -1,
        }
    }

    fn collect_cursor(mut cursor: Cursor) -> Vec<Document> {
        let mut out = Vec::new();
        while let Ok(Some(doc)) = cursor.next() {
            out.push(doc);
        }
        out
    }

    #[tokio::test]
    async fn table_scan_empty() {
        let su = make_storage_with_docs(&[]);
        let mut exec = DefaultExecutor::new();
        exec.register(
            "c".into(),
            CollectionHandle {
                storage: su,
                indexes: vec![],
            },
        );
        let plan = plan_for("c", leaf_node(PlanType::TableScan));
        let cursor = exec.execute(&plan).await.unwrap();
        assert!(collect_cursor(cursor).is_empty());
    }

    #[tokio::test]
    async fn table_scan_returns_all() {
        let docs: Vec<Document> = (0..5)
            .map(|i| doc_with(&[("x", Value::Int32(i))]))
            .collect();
        let su = make_storage_with_docs(&docs);
        let mut exec = DefaultExecutor::new();
        exec.register(
            "c".into(),
            CollectionHandle {
                storage: su,
                indexes: vec![],
            },
        );
        let plan = plan_for("c", leaf_node(PlanType::TableScan));
        let result = collect_cursor(exec.execute(&plan).await.unwrap());
        assert_eq!(result.len(), 5);
    }

    #[tokio::test]
    async fn filter_matches() {
        let docs: Vec<Document> = (0..10)
            .map(|i| doc_with(&[("x", Value::Int32(i))]))
            .collect();
        let su = make_storage_with_docs(&docs);
        let mut exec = DefaultExecutor::new();
        exec.register(
            "c".into(),
            CollectionHandle {
                storage: su,
                indexes: vec![],
            },
        );

        let root = wrap_node(PlanType::Filter, leaf_node(PlanType::TableScan));
        // condition: { "x": { "$gte": 5 } }
        let cond_ops = doc_with(&[("$gte", Value::Int32(5))]);
        let cond = doc_with(&[("x", Value::Document(cond_ops))]);
        let mut plan = plan_for("c", root);
        plan.condition = Some(cond);

        let result = collect_cursor(exec.execute(&plan).await.unwrap());
        assert_eq!(result.len(), 5);
    }

    #[tokio::test]
    async fn filter_match_all() {
        let docs: Vec<Document> = (0..3)
            .map(|i| doc_with(&[("x", Value::Int32(i))]))
            .collect();
        let su = make_storage_with_docs(&docs);
        let mut exec = DefaultExecutor::new();
        exec.register(
            "c".into(),
            CollectionHandle {
                storage: su,
                indexes: vec![],
            },
        );

        let root = wrap_node(PlanType::Filter, leaf_node(PlanType::TableScan));
        let mut plan = plan_for("c", root);
        plan.condition = Some(Document::new()); // empty = match all

        let result = collect_cursor(exec.execute(&plan).await.unwrap());
        assert_eq!(result.len(), 3);
    }

    #[tokio::test]
    async fn index_scan_returns_docs() {
        let docs: Vec<Document> = (0..5)
            .map(|i| doc_with(&[("a", Value::Int32(i))]))
            .collect();
        let su = Arc::new(StorageUnit::new(1, 1));

        let mut kp = Document::new();
        kp.insert("a", Value::Int32(1));
        let def = IndexDefinition {
            name: "idx_a".to_string(),
            key_pattern: kp,
            unique: false,
            enforced: false,
            not_null: false,
        };
        let mut idx = BTreeIndex::new(def);

        // Insert docs into storage and index
        for doc in &docs {
            let rid = su.insert(doc).unwrap();
            let key = sdb_ixm::IndexDefinition::extract_key(
                &idx.definition,
                doc,
            );
            idx.insert(&key, rid).unwrap();
        }

        let mut exec = DefaultExecutor::new();
        exec.register(
            "c".into(),
            CollectionHandle {
                storage: su,
                indexes: vec![Arc::new(RwLock::new(idx))],
            },
        );

        let mut scan_node = leaf_node(PlanType::IndexScan);
        scan_node.access_path = Some(AccessPath::IndexScan {
            index_name: "idx_a".to_string(),
            direction: ScanDirection::Forward,
            selectivity: 1.0,
            covers_sort: false,
        });
        let plan = plan_for("c", scan_node);
        let result = collect_cursor(exec.execute(&plan).await.unwrap());
        assert_eq!(result.len(), 5);
        // Index scan returns in index order (ascending a)
        for (i, doc) in result.iter().enumerate() {
            assert_eq!(doc.get("a"), Some(&Value::Int32(i as i32)));
        }
    }

    #[tokio::test]
    async fn sort_ascending() {
        let docs = vec![
            doc_with(&[("v", Value::Int32(3))]),
            doc_with(&[("v", Value::Int32(1))]),
            doc_with(&[("v", Value::Int32(2))]),
        ];
        let su = make_storage_with_docs(&docs);
        let mut exec = DefaultExecutor::new();
        exec.register(
            "c".into(),
            CollectionHandle {
                storage: su,
                indexes: vec![],
            },
        );

        let mut sort = wrap_node(PlanType::Sort, leaf_node(PlanType::TableScan));
        sort.sort_fields = Some(doc_with(&[("v", Value::Int32(1))]));
        let plan = plan_for("c", sort);
        let result = collect_cursor(exec.execute(&plan).await.unwrap());
        assert_eq!(result[0].get("v"), Some(&Value::Int32(1)));
        assert_eq!(result[1].get("v"), Some(&Value::Int32(2)));
        assert_eq!(result[2].get("v"), Some(&Value::Int32(3)));
    }

    #[tokio::test]
    async fn sort_descending() {
        let docs = vec![
            doc_with(&[("v", Value::Int32(1))]),
            doc_with(&[("v", Value::Int32(3))]),
            doc_with(&[("v", Value::Int32(2))]),
        ];
        let su = make_storage_with_docs(&docs);
        let mut exec = DefaultExecutor::new();
        exec.register(
            "c".into(),
            CollectionHandle {
                storage: su,
                indexes: vec![],
            },
        );

        let mut sort = wrap_node(PlanType::Sort, leaf_node(PlanType::TableScan));
        sort.sort_fields = Some(doc_with(&[("v", Value::Int32(-1))]));
        let plan = plan_for("c", sort);
        let result = collect_cursor(exec.execute(&plan).await.unwrap());
        assert_eq!(result[0].get("v"), Some(&Value::Int32(3)));
        assert_eq!(result[1].get("v"), Some(&Value::Int32(2)));
        assert_eq!(result[2].get("v"), Some(&Value::Int32(1)));
    }

    #[tokio::test]
    async fn skip_records() {
        let docs: Vec<Document> = (0..5)
            .map(|i| doc_with(&[("x", Value::Int32(i))]))
            .collect();
        let su = make_storage_with_docs(&docs);
        let mut exec = DefaultExecutor::new();
        exec.register(
            "c".into(),
            CollectionHandle {
                storage: su,
                indexes: vec![],
            },
        );

        let root = wrap_node(PlanType::Skip, leaf_node(PlanType::TableScan));
        let mut plan = plan_for("c", root);
        plan.skip = 3;
        let result = collect_cursor(exec.execute(&plan).await.unwrap());
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn limit_records() {
        let docs: Vec<Document> = (0..5)
            .map(|i| doc_with(&[("x", Value::Int32(i))]))
            .collect();
        let su = make_storage_with_docs(&docs);
        let mut exec = DefaultExecutor::new();
        exec.register(
            "c".into(),
            CollectionHandle {
                storage: su,
                indexes: vec![],
            },
        );

        let root = wrap_node(PlanType::Limit, leaf_node(PlanType::TableScan));
        let mut plan = plan_for("c", root);
        plan.limit = 2;
        let result = collect_cursor(exec.execute(&plan).await.unwrap());
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn skip_and_limit() {
        let docs: Vec<Document> = (0..10)
            .map(|i| doc_with(&[("x", Value::Int32(i))]))
            .collect();
        let su = make_storage_with_docs(&docs);
        let mut exec = DefaultExecutor::new();
        exec.register(
            "c".into(),
            CollectionHandle {
                storage: su,
                indexes: vec![],
            },
        );

        let scan = leaf_node(PlanType::TableScan);
        let skip = wrap_node(PlanType::Skip, scan);
        let limit = wrap_node(PlanType::Limit, skip);
        let mut plan = plan_for("c", limit);
        plan.skip = 3;
        plan.limit = 4;
        let result = collect_cursor(exec.execute(&plan).await.unwrap());
        assert_eq!(result.len(), 4);
    }

    #[tokio::test]
    async fn project_fields() {
        let docs = vec![doc_with(&[
            ("name", Value::String("alice".into())),
            ("age", Value::Int32(30)),
            ("city", Value::String("sf".into())),
        ])];
        let su = make_storage_with_docs(&docs);
        let mut exec = DefaultExecutor::new();
        exec.register(
            "c".into(),
            CollectionHandle {
                storage: su,
                indexes: vec![],
            },
        );

        let root = wrap_node(PlanType::Project, leaf_node(PlanType::TableScan));
        let mut plan = plan_for("c", root);
        plan.selector = Some(doc_with(&[
            ("name", Value::Int32(1)),
            ("age", Value::Int32(1)),
        ]));
        let result = collect_cursor(exec.execute(&plan).await.unwrap());
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].get("name"),
            Some(&Value::String("alice".into()))
        );
        assert_eq!(result[0].get("age"), Some(&Value::Int32(30)));
        assert_eq!(result[0].get("city"), None);
    }

    #[tokio::test]
    async fn full_pipeline() {
        // Build: index_scan → filter → sort → skip → limit → project
        let su = Arc::new(StorageUnit::new(1, 1));

        let mut kp = Document::new();
        kp.insert("a", Value::Int32(1));
        let def = IndexDefinition {
            name: "idx_a".to_string(),
            key_pattern: kp,
            unique: false,
            enforced: false,
            not_null: false,
        };
        let mut idx = BTreeIndex::new(def);

        // Insert 20 docs: a=0..19, b=19..0
        for i in 0..20i32 {
            let doc = doc_with(&[
                ("a", Value::Int32(i)),
                ("b", Value::Int32(19 - i)),
                ("extra", Value::String("drop".into())),
            ]);
            let rid = su.insert(&doc).unwrap();
            let key = sdb_ixm::IndexDefinition::extract_key(&idx.definition, &doc);
            idx.insert(&key, rid).unwrap();
        }

        let mut exec = DefaultExecutor::new();
        exec.register(
            "c".into(),
            CollectionHandle {
                storage: su,
                indexes: vec![Arc::new(RwLock::new(idx))],
            },
        );

        // IndexScan
        let mut scan_node = leaf_node(PlanType::IndexScan);
        scan_node.access_path = Some(AccessPath::IndexScan {
            index_name: "idx_a".to_string(),
            direction: ScanDirection::Forward,
            selectivity: 1.0,
            covers_sort: false,
        });

        // Filter: a >= 5
        let filter = wrap_node(PlanType::Filter, scan_node);

        // Sort by b ascending
        let mut sort = wrap_node(PlanType::Sort, filter);
        sort.sort_fields = Some(doc_with(&[("b", Value::Int32(1))]));

        // Skip 2
        let skip = wrap_node(PlanType::Skip, sort);

        // Limit 5
        let limit = wrap_node(PlanType::Limit, skip);

        // Project: only a, b
        let project = wrap_node(PlanType::Project, limit);

        let cond_ops = doc_with(&[("$gte", Value::Int32(5))]);
        let cond = doc_with(&[("a", Value::Document(cond_ops))]);

        let mut plan = plan_for("c", project);
        plan.condition = Some(cond);
        plan.selector = Some(doc_with(&[
            ("a", Value::Int32(1)),
            ("b", Value::Int32(1)),
        ]));
        plan.skip = 2;
        plan.limit = 5;

        let result = collect_cursor(exec.execute(&plan).await.unwrap());
        // After filter: 15 docs (a=5..19)
        // After sort by b asc: b=0,1,2,...,14 → a=19,18,...,5
        // After skip 2: b=2,3,...,14
        // After limit 5: b=2,3,4,5,6
        // After project: only a,b fields
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].get("extra"), None); // projected out
        assert_eq!(result[0].get("b"), Some(&Value::Int32(2)));
        assert_eq!(result[4].get("b"), Some(&Value::Int32(6)));
    }
}
