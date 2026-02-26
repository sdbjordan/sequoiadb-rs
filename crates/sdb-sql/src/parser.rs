use crate::ast::*;
use sdb_common::{Result, SdbError};

/// SQL parser — converts SQL text to AST via hand-written recursive descent.
pub struct SqlParser;

impl SqlParser {
    pub fn new() -> Self {
        Self
    }

    /// Parse a SQL string into a statement AST.
    pub fn parse(&self, sql: &str) -> Result<SqlStatement> {
        let tokens = tokenize(sql)?;
        let mut pos = 0;
        let stmt = parse_statement(&tokens, &mut pos)?;
        Ok(stmt)
    }
}

impl Default for SqlParser {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tokenizer ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Word(String),    // keyword or identifier (uppercased for keywords)
    Number(String),  // numeric literal
    StringLit(String), // 'string literal'
    Comma,
    LParen,
    RParen,
    Star,
    Eq,
    Dot,
    Semi,
}

fn tokenize(sql: &str) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b' ' | b'\t' | b'\n' | b'\r' => { i += 1; }
            b',' => { tokens.push(Token::Comma); i += 1; }
            b'(' => { tokens.push(Token::LParen); i += 1; }
            b')' => { tokens.push(Token::RParen); i += 1; }
            b'*' => { tokens.push(Token::Star); i += 1; }
            b'=' => { tokens.push(Token::Eq); i += 1; }
            b'.' => { tokens.push(Token::Dot); i += 1; }
            b';' => { tokens.push(Token::Semi); i += 1; }
            b'\'' => {
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i] != b'\'' { i += 1; }
                let s = std::str::from_utf8(&bytes[start..i]).map_err(|_| SdbError::InvalidArg)?;
                tokens.push(Token::StringLit(s.to_string()));
                if i < bytes.len() { i += 1; } // skip closing quote
            }
            b if b.is_ascii_digit() => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') { i += 1; }
                let s = std::str::from_utf8(&bytes[start..i]).map_err(|_| SdbError::InvalidArg)?;
                tokens.push(Token::Number(s.to_string()));
            }
            b if b.is_ascii_alphabetic() || b == b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') { i += 1; }
                let s = std::str::from_utf8(&bytes[start..i]).map_err(|_| SdbError::InvalidArg)?;
                tokens.push(Token::Word(s.to_string()));
            }
            _ => { i += 1; } // skip unknown chars
        }
    }
    Ok(tokens)
}

// ── Parser helpers ─────────────────────────────────────────────────────

fn peek(tokens: &[Token], pos: usize) -> Option<&Token> {
    tokens.get(pos)
}

fn expect_word(tokens: &[Token], pos: &mut usize, kw: &str) -> Result<()> {
    match tokens.get(*pos) {
        Some(Token::Word(w)) if w.eq_ignore_ascii_case(kw) => { *pos += 1; Ok(()) }
        _ => Err(SdbError::InvalidArg),
    }
}

fn is_word(tokens: &[Token], pos: usize, kw: &str) -> bool {
    matches!(tokens.get(pos), Some(Token::Word(w)) if w.eq_ignore_ascii_case(kw))
}

fn read_identifier(tokens: &[Token], pos: &mut usize) -> Result<String> {
    match tokens.get(*pos) {
        Some(Token::Word(w)) => { let s = w.clone(); *pos += 1; Ok(s) }
        _ => Err(SdbError::InvalidArg),
    }
}

fn read_value(tokens: &[Token], pos: &mut usize) -> Result<String> {
    match tokens.get(*pos) {
        Some(Token::Number(n)) => { let s = n.clone(); *pos += 1; Ok(s) }
        Some(Token::StringLit(s)) => { let v = s.clone(); *pos += 1; Ok(v) }
        Some(Token::Word(w)) => { let s = w.clone(); *pos += 1; Ok(s) }
        _ => Err(SdbError::InvalidArg),
    }
}

// ── Statement parsing ──────────────────────────────────────────────────

fn parse_statement(tokens: &[Token], pos: &mut usize) -> Result<SqlStatement> {
    match tokens.get(*pos) {
        Some(Token::Word(w)) => match w.to_ascii_uppercase().as_str() {
            "SELECT" => parse_select(tokens, pos).map(SqlStatement::Select),
            "INSERT" => parse_insert(tokens, pos).map(SqlStatement::Insert),
            "UPDATE" => parse_update(tokens, pos).map(SqlStatement::Update),
            "DELETE" => parse_delete(tokens, pos).map(SqlStatement::Delete),
            "CREATE" => parse_create_table(tokens, pos).map(SqlStatement::CreateTable),
            "DROP" => parse_drop_table(tokens, pos).map(SqlStatement::DropTable),
            _ => Err(SdbError::InvalidArg),
        },
        _ => Err(SdbError::InvalidArg),
    }
}

/// SELECT col1, col2 FROM table [WHERE cond] [ORDER BY col [ASC|DESC]] [LIMIT n] [OFFSET n]
fn parse_select(tokens: &[Token], pos: &mut usize) -> Result<SelectStatement> {
    expect_word(tokens, pos, "SELECT")?;

    // Columns
    let mut columns = Vec::new();
    if matches!(peek(tokens, *pos), Some(Token::Star)) {
        columns.push("*".to_string());
        *pos += 1;
    } else {
        columns.push(read_identifier(tokens, pos)?);
        while matches!(peek(tokens, *pos), Some(Token::Comma)) {
            *pos += 1;
            columns.push(read_identifier(tokens, pos)?);
        }
    }

    expect_word(tokens, pos, "FROM")?;
    let table = read_identifier(tokens, pos)?;

    // Optional WHERE
    let condition = if is_word(tokens, *pos, "WHERE") {
        *pos += 1;
        Some(read_where_clause(tokens, pos)?)
    } else {
        None
    };

    // Optional ORDER BY
    let mut order_by = Vec::new();
    if is_word(tokens, *pos, "ORDER") {
        *pos += 1;
        expect_word(tokens, pos, "BY")?;
        loop {
            let col = read_identifier(tokens, pos)?;
            let asc = if is_word(tokens, *pos, "DESC") {
                *pos += 1;
                false
            } else {
                if is_word(tokens, *pos, "ASC") { *pos += 1; }
                true
            };
            order_by.push((col, asc));
            if !matches!(peek(tokens, *pos), Some(Token::Comma)) { break; }
            *pos += 1;
        }
    }

    // Optional LIMIT
    let limit = if is_word(tokens, *pos, "LIMIT") {
        *pos += 1;
        let n = read_value(tokens, pos)?;
        Some(n.parse::<i64>().map_err(|_| SdbError::InvalidArg)?)
    } else {
        None
    };

    // Optional OFFSET
    let offset = if is_word(tokens, *pos, "OFFSET") {
        *pos += 1;
        let n = read_value(tokens, pos)?;
        Some(n.parse::<i64>().map_err(|_| SdbError::InvalidArg)?)
    } else {
        None
    };

    Ok(SelectStatement { columns, table, condition, order_by, limit, offset })
}

/// INSERT INTO table (col1, col2) VALUES (v1, v2), (v3, v4)
fn parse_insert(tokens: &[Token], pos: &mut usize) -> Result<InsertStatement> {
    expect_word(tokens, pos, "INSERT")?;
    expect_word(tokens, pos, "INTO")?;
    let table = read_identifier(tokens, pos)?;

    // Column list
    let mut columns = Vec::new();
    if matches!(peek(tokens, *pos), Some(Token::LParen)) {
        *pos += 1;
        columns.push(read_identifier(tokens, pos)?);
        while matches!(peek(tokens, *pos), Some(Token::Comma)) {
            *pos += 1;
            columns.push(read_identifier(tokens, pos)?);
        }
        if matches!(peek(tokens, *pos), Some(Token::RParen)) { *pos += 1; }
    }

    expect_word(tokens, pos, "VALUES")?;

    // Value rows
    let mut values = Vec::new();
    loop {
        if !matches!(peek(tokens, *pos), Some(Token::LParen)) { break; }
        *pos += 1;
        let mut row = Vec::new();
        row.push(read_value(tokens, pos)?);
        while matches!(peek(tokens, *pos), Some(Token::Comma)) {
            *pos += 1;
            row.push(read_value(tokens, pos)?);
        }
        if matches!(peek(tokens, *pos), Some(Token::RParen)) { *pos += 1; }
        values.push(row);
        if !matches!(peek(tokens, *pos), Some(Token::Comma)) { break; }
        *pos += 1;
    }

    Ok(InsertStatement { table, columns, values })
}

/// UPDATE table SET col = val [, col = val] [WHERE cond]
fn parse_update(tokens: &[Token], pos: &mut usize) -> Result<UpdateStatement> {
    expect_word(tokens, pos, "UPDATE")?;
    let table = read_identifier(tokens, pos)?;
    expect_word(tokens, pos, "SET")?;

    let mut assignments = Vec::new();
    loop {
        let col = read_identifier(tokens, pos)?;
        if !matches!(peek(tokens, *pos), Some(Token::Eq)) { return Err(SdbError::InvalidArg); }
        *pos += 1;
        let val = read_value(tokens, pos)?;
        assignments.push((col, val));
        if !matches!(peek(tokens, *pos), Some(Token::Comma)) { break; }
        *pos += 1;
    }

    let condition = if is_word(tokens, *pos, "WHERE") {
        *pos += 1;
        Some(read_where_clause(tokens, pos)?)
    } else {
        None
    };

    Ok(UpdateStatement { table, assignments, condition })
}

/// DELETE FROM table [WHERE cond]
fn parse_delete(tokens: &[Token], pos: &mut usize) -> Result<DeleteStatement> {
    expect_word(tokens, pos, "DELETE")?;
    expect_word(tokens, pos, "FROM")?;
    let table = read_identifier(tokens, pos)?;

    let condition = if is_word(tokens, *pos, "WHERE") {
        *pos += 1;
        Some(read_where_clause(tokens, pos)?)
    } else {
        None
    };

    Ok(DeleteStatement { table, condition })
}

/// CREATE TABLE table (col1 type, col2 type NOT NULL, ...)
fn parse_create_table(tokens: &[Token], pos: &mut usize) -> Result<CreateTableStatement> {
    expect_word(tokens, pos, "CREATE")?;
    expect_word(tokens, pos, "TABLE")?;
    let table = read_identifier(tokens, pos)?;

    let mut columns = Vec::new();
    if matches!(peek(tokens, *pos), Some(Token::LParen)) {
        *pos += 1;
        loop {
            let name = read_identifier(tokens, pos)?;
            let data_type = read_identifier(tokens, pos)?;
            let nullable = if is_word(tokens, *pos, "NOT") {
                *pos += 1;
                expect_word(tokens, pos, "NULL")?;
                false
            } else {
                true
            };
            columns.push(ColumnDef { name, data_type, nullable });
            if !matches!(peek(tokens, *pos), Some(Token::Comma)) { break; }
            *pos += 1;
        }
        if matches!(peek(tokens, *pos), Some(Token::RParen)) { *pos += 1; }
    }

    Ok(CreateTableStatement { table, columns })
}

/// DROP TABLE table
fn parse_drop_table(tokens: &[Token], pos: &mut usize) -> Result<DropTableStatement> {
    expect_word(tokens, pos, "DROP")?;
    expect_word(tokens, pos, "TABLE")?;
    let table = read_identifier(tokens, pos)?;
    Ok(DropTableStatement { table })
}

/// Read a WHERE clause as a raw string (col = val AND col = val ...)
/// For v1 we store it as a string; QGM converts to BSON conditions.
fn read_where_clause(tokens: &[Token], pos: &mut usize) -> Result<String> {
    let mut parts = Vec::new();
    while *pos < tokens.len() {
        match &tokens[*pos] {
            Token::Word(w) if w.eq_ignore_ascii_case("ORDER")
                || w.eq_ignore_ascii_case("LIMIT")
                || w.eq_ignore_ascii_case("OFFSET") => break,
            Token::Semi => break,
            Token::RParen => break,
            Token::Word(w) => { parts.push(w.clone()); *pos += 1; }
            Token::Number(n) => { parts.push(n.clone()); *pos += 1; }
            Token::StringLit(s) => { parts.push(format!("'{}'", s)); *pos += 1; }
            Token::Eq => { parts.push("=".into()); *pos += 1; }
            Token::Dot => { parts.push(".".into()); *pos += 1; }
            Token::Comma => break,
            _ => { *pos += 1; }
        }
    }
    Ok(parts.join(" "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_select_all() {
        let p = SqlParser::new();
        let stmt = p.parse("SELECT * FROM users").unwrap();
        match stmt {
            SqlStatement::Select(s) => {
                assert_eq!(s.columns, vec!["*"]);
                assert_eq!(s.table, "users");
                assert!(s.condition.is_none());
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn parse_select_columns_with_where() {
        let p = SqlParser::new();
        let stmt = p.parse("SELECT name, age FROM users WHERE age = 25").unwrap();
        match stmt {
            SqlStatement::Select(s) => {
                assert_eq!(s.columns, vec!["name", "age"]);
                assert_eq!(s.table, "users");
                assert_eq!(s.condition.as_deref(), Some("age = 25"));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn parse_select_order_limit_offset() {
        let p = SqlParser::new();
        let stmt = p.parse("SELECT * FROM t ORDER BY name DESC LIMIT 10 OFFSET 5").unwrap();
        match stmt {
            SqlStatement::Select(s) => {
                assert_eq!(s.order_by, vec![("name".into(), false)]);
                assert_eq!(s.limit, Some(10));
                assert_eq!(s.offset, Some(5));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn parse_insert() {
        let p = SqlParser::new();
        let stmt = p.parse("INSERT INTO users (name, age) VALUES ('alice', 30)").unwrap();
        match stmt {
            SqlStatement::Insert(s) => {
                assert_eq!(s.table, "users");
                assert_eq!(s.columns, vec!["name", "age"]);
                assert_eq!(s.values.len(), 1);
                assert_eq!(s.values[0], vec!["alice", "30"]);
            }
            _ => panic!("expected INSERT"),
        }
    }

    #[test]
    fn parse_insert_multi_rows() {
        let p = SqlParser::new();
        let stmt = p.parse("INSERT INTO t (x) VALUES (1), (2), (3)").unwrap();
        match stmt {
            SqlStatement::Insert(s) => {
                assert_eq!(s.values.len(), 3);
            }
            _ => panic!("expected INSERT"),
        }
    }

    #[test]
    fn parse_update() {
        let p = SqlParser::new();
        let stmt = p.parse("UPDATE users SET age = 31 WHERE name = 'alice'").unwrap();
        match stmt {
            SqlStatement::Update(s) => {
                assert_eq!(s.table, "users");
                assert_eq!(s.assignments, vec![("age".into(), "31".into())]);
                assert!(s.condition.is_some());
            }
            _ => panic!("expected UPDATE"),
        }
    }

    #[test]
    fn parse_delete() {
        let p = SqlParser::new();
        let stmt = p.parse("DELETE FROM users WHERE age = 25").unwrap();
        match stmt {
            SqlStatement::Delete(s) => {
                assert_eq!(s.table, "users");
                assert!(s.condition.is_some());
            }
            _ => panic!("expected DELETE"),
        }
    }

    #[test]
    fn parse_create_table() {
        let p = SqlParser::new();
        let stmt = p.parse("CREATE TABLE users (name TEXT NOT NULL, age INT)").unwrap();
        match stmt {
            SqlStatement::CreateTable(s) => {
                assert_eq!(s.table, "users");
                assert_eq!(s.columns.len(), 2);
                assert_eq!(s.columns[0].name, "name");
                assert!(!s.columns[0].nullable);
                assert_eq!(s.columns[1].name, "age");
                assert!(s.columns[1].nullable);
            }
            _ => panic!("expected CREATE TABLE"),
        }
    }

    #[test]
    fn parse_drop_table() {
        let p = SqlParser::new();
        let stmt = p.parse("DROP TABLE users").unwrap();
        match stmt {
            SqlStatement::DropTable(s) => {
                assert_eq!(s.table, "users");
            }
            _ => panic!("expected DROP TABLE"),
        }
    }

    #[test]
    fn parse_invalid_sql() {
        let p = SqlParser::new();
        assert!(p.parse("INVALID SQL BLAH").is_err());
    }
}
