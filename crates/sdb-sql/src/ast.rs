/// Top-level SQL statement.
#[derive(Debug, Clone)]
pub enum SqlStatement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub columns: Vec<String>,
    pub table: String,
    pub condition: Option<String>,
    pub order_by: Vec<(String, bool)>, // (column, ascending)
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<(String, String)>,
    pub condition: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DeleteStatement {
    pub table: String,
    pub condition: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CreateTableStatement {
    pub table: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct DropTableStatement {
    pub table: String,
}
