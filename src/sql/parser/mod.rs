use crate::error::QuillSQLResult;
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

pub fn parse_sql(sql: &str) -> QuillSQLResult<Vec<Statement>> {
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql)?;
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