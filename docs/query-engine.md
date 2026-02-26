# Query Engine

The query engine spans three crates forming a pipeline: **sdb-mth** (matching & modification) → **sdb-opt** (cost-based optimization) → **sdb-rtn** (execution runtime). Additionally, **sdb-aggr** provides aggregation pipelines and **sdb-sql** provides SQL parsing.

## Query Matcher (sdb-mth)

### Match Expressions

Query conditions are parsed into a `MatchExpression` AST:

```rust
pub enum MatchExpression {
    And(Vec<MatchExpression>),
    Or(Vec<MatchExpression>),
    Nor(Vec<MatchExpression>),
    Not(Box<MatchExpression>),
    Compare { path: String, op: CompareOp, value: Value },
    Exists { path: String, expected: bool },
    In { path: String, values: Vec<Value> },
    Nin { path: String, values: Vec<Value> },
}

pub enum CompareOp { Eq, Ne, Gt, Gte, Lt, Lte }
```

### Supported Operators

| Operator | Type | Description |
|----------|------|-------------|
| `$eq` | Comparison | Equality (also implicit: `{ field: value }`) |
| `$ne` | Comparison | Not equal |
| `$gt`, `$gte` | Comparison | Greater than (or equal) |
| `$lt`, `$lte` | Comparison | Less than (or equal) |
| `$in` | Set | Value in array |
| `$nin` | Set | Value not in array |
| `$exists` | Element | Field existence check |
| `$and` | Logical | All conditions must match |
| `$or` | Logical | Any condition must match |
| `$nor` | Logical | No condition must match |
| `$not` | Logical | Negate inner condition |

**Implicit AND**: Multiple top-level fields create an implicit `$and`. Multiple operators on the same field (`{ $gt: 10, $lt: 20 }`) also produce an implicit `$and`.

### Value Comparison

- **Dot-notation**: Paths like `"a.b.c"` resolve recursively into nested documents
- **Cross-type numerics**: Int32, Int64, and Double are promoted to f64 for comparison
- **BSON type ordering**: MinKey < Null < Numbers < String < Document < Array < Binary < ObjectId < Boolean < Date < Timestamp < Regex < Decimal < MaxKey

### Document Modifier

```rust
pub struct Modifier { ops: Vec<ModifyOp> }

pub enum ModifyOp {
    Set { path: String, value: Value },
    Unset { path: String },
    Inc { path: String, value: Value },
}
```

| Operator | Behavior |
|----------|----------|
| `$set` | Set field value (supports nested dot-paths, creates intermediates) |
| `$unset` | Remove field (no-op if missing) |
| `$inc` | Increment numeric field (cross-type promotion; missing field = 0) |

## Query Optimizer (sdb-opt)

### Access Paths

```rust
pub enum AccessPath {
    TableScan,
    IndexScan {
        index_name: String,
        direction: ScanDirection,  // Forward or Backward
        selectivity: f64,          // 0.0..1.0
        covers_sort: bool,
    },
}
```

### Cost Model

| Constant | Value | Description |
|----------|-------|-------------|
| `TABLE_SCAN_PER_ROW` | 1.0 | Full scan cost per row |
| `INDEX_SCAN_PER_ROW` | 0.4 | Index scan cost per row |
| `INDEX_SEEK` | 10.0 | Fixed index seek overhead |
| `SORT_PER_ROW` | 3.0 | Sort cost per row |
| `FILTER_PER_ROW` | 0.1 | Filter cost per row |

**Table scan cost** = `rows × 1.0` + sort cost if needed.

**Index scan cost** = `10.0 + (rows × selectivity × 0.4)` + sort cost if index doesn't cover.

### Index Matching

The optimizer extracts predicates from the top-level AND condition and matches them against index key prefixes:

1. **Equality** (`$eq`): selectivity = `1/total_records`, extends prefix
2. **Range** (`$gt`/`$lt`): selectivity = 0.33, stops prefix extension
3. **In** (`$in`): selectivity = `count/total_records`, stops prefix extension

After the equality prefix, checks if remaining index fields cover the ORDER BY (forward or backward match).

### Plan Tree

```rust
pub enum PlanType {
    TableScan, IndexScan, Sort, Filter, Limit, Skip, Project, Merge
}
```

Plans are built bottom-up: `Scan → Filter → Sort → Skip → Limit → Project`. The optimizer picks the lowest-cost access path from TableScan and all available IndexScans.

## Query Runtime (sdb-rtn)

### Executor

```rust
#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(&self, plan: &QueryPlan) -> Result<Cursor>;
}
```

`DefaultExecutor` recursively evaluates the plan tree:

| Node | Behavior |
|------|----------|
| TableScan | `storage.scan()` — returns all records |
| IndexScan | `index.scan(KeyRange::all())` → fetch each doc by RecordId |
| Filter | Apply `Matcher` to child results |
| Sort | Sort by `order_by` fields with direction (ASC/DESC), BSON-aware |
| Skip | Drop first N documents |
| Limit | Truncate to N documents |
| Project | Copy only fields present in selector |

### Cursor

```rust
pub struct Cursor {
    context_id: i64,
    buffer: Vec<Document>,
    pos: usize,
    exhausted: bool,
}
```

## Aggregation Pipeline (sdb-aggr)

Pipeline stages are executed sequentially, piping output to the next stage:

| Stage | Description |
|-------|-------------|
| `$match` | Filter using full `Matcher` support |
| `$project` | Include (1) or exclude (0) fields |
| `$sort` | Multi-key sort with direction |
| `$limit` | Take first N documents |
| `$skip` | Skip first N documents |
| `$count` | Replace input with `{ field: count }` |
| `$group` | Group by `_id` field, accumulator: `$sum` only (v1) |
| `$unwind` | Deconstruct array field into separate documents |
| `$lookup` | Not implemented in v1 |

### $group Example

```json
{ "$group": { "_id": "$department", "total": { "$sum": "$salary" }, "count": { "$sum": 1 } } }
```

## SQL Parser (sdb-sql)

Hand-written recursive descent parser supporting 6 statement types:

| Statement | Syntax |
|-----------|--------|
| SELECT | `SELECT [cols] FROM table [WHERE cond] [ORDER BY ...] [LIMIT n] [OFFSET n]` |
| INSERT | `INSERT INTO table (cols) VALUES (vals) [, ...]` |
| UPDATE | `UPDATE table SET col=val [, ...] [WHERE cond]` |
| DELETE | `DELETE FROM table [WHERE cond]` |
| CREATE TABLE | `CREATE TABLE name (col type [NOT NULL], ...)` |
| DROP TABLE | `DROP TABLE name` |

**WHERE clause to BSON**: Only equality (`=`) is supported. Values are type-inferred (quoted → String, numeric → Int32/Int64/Double).

SQL is converted to `QueryPlan` via the Query Graph Model (QGM), which builds a plan tree without optimizer cost estimation.
