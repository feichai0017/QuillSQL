use crate::error::QuillSQLResult;
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

pub fn parse_sql(sql: &str) -> QuillSQLResult<Vec<Statement>> {
    // Lightweight rewrite for unsupported SHOW syntax under Postgres dialect
    // Maps to information_schema queries to keep planner/executor simple.
    let normalized = sql.trim().trim_end_matches(';').trim();
    let lower = normalized.to_ascii_lowercase();

    let rewritten = if lower == "show databases" || lower == "show database" {
        // List schemas (databases) from information_schema.schemas
        Some("select schema from information_schema.schemas".to_string())
    } else if lower == "show tables" {
        // List all tables
        Some("select table_name from information_schema.tables".to_string())
    } else {
        None
    };

    let sql_to_parse = rewritten.as_deref().unwrap_or(normalized);
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql_to_parse)?;
    Ok(stmts)
}

#[cfg(test)]
mod tests {

    #[test]
    pub fn test_parser() {
        let sql = "select * from (select * from t1)";
        let stmts = super::parse_sql(sql).unwrap();
        println!("{:#?}", stmts[0]);
    }
}
