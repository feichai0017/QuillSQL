use std::{collections::BTreeMap, iter::Peekable};

use ast::{Column, Expression, Operation, OrderDirection};
use lexer::{Keyword, Lexer, Token};

use crate::error::{Error, Result};

use super::types::DataType;

pub mod ast;
mod lexer;

// 解析器定义
pub struct Parser<'a> {
    lexer: Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        Parser {
            lexer: Lexer::new(input).peekable(),
        }
    }

    // 解析，获取到抽象语法树
    pub fn parse(&mut self) -> Result<ast::Statement> {
        let stmt = self.parse_statement()?;
        // 期望 sql 语句的最后有个分号
        self.next_expect(Token::Semicolon)?;
        // 分号之后不能有其他的符号
        if let Some(token) = self.peek()? {
            return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
        }
        Ok(stmt)
    }

    fn parse_statement(&mut self) -> Result<ast::Statement> {
        // 查看第一个 Token 类型
        match self.peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Drop)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            Some(Token::Keyword(Keyword::Update)) => self.parse_update(),
            Some(Token::Keyword(Keyword::Delete)) => self.parse_delete(),
            Some(Token::Keyword(Keyword::Begin)) => self.parse_transaction(),
            Some(Token::Keyword(Keyword::Commit)) => self.parse_transaction(),
            Some(Token::Keyword(Keyword::Rollback)) => self.parse_transaction(),
            Some(Token::Keyword(Keyword::Explain)) => self.parse_explain(),
            Some(t) => Err(Error::Parse(format!("[Parser] Unexpected token {}", t))),
            None => Err(Error::Parse(format!("[Parser] Unexpected end of input"))),
        }
    }

    // 解析 DDL 类型
    fn parse_ddl(&mut self) -> Result<ast::Statement> {
        match self.next()? {
            Token::Keyword(Keyword::Create) => self.parse_ddl_create_table(),
            Token::Keyword(Keyword::Drop) => self.parse_ddl_drop_table(),
            token => Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
        }
    }

    // 解析 Select 语句
    fn parse_select(&mut self) -> Result<ast::Statement> {
        Ok(ast::Statement::Select {
            select: self.parse_select_clause()?,
            from: self.parse_from_clause()?,
            where_clause: self.parse_where_clause()?,
            group_by: self.parse_group_clause()?,
            having: self.parse_having_clause()?,
            order_by: self.parse_order_clause()?,
            limit: {
                if self.next_if_token(Token::Keyword(Keyword::Limit)).is_some() {
                    Some(self.parse_expression()?)
                } else {
                    None
                }
            },
            offset: {
                if self
                    .next_if_token(Token::Keyword(Keyword::Offset))
                    .is_some()
                {
                    Some(self.parse_expression()?)
                } else {
                    None
                }
            },
        })
    }

    // 解析 Insert 语句
    fn parse_insert(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Insert))?;
        self.next_expect(Token::Keyword(Keyword::Into))?;

        // 表名
        let table_name = self.next_ident()?;

        // 查看是否给指定的列进行 insert
        let columns = if self.next_if_token(Token::OpenParen).is_some() {
            let mut cols = Vec::new();
            loop {
                cols.push(self.next_ident()?.to_string());
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {}
                    token => {
                        return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
                    }
                }
            }
            Some(cols)
        } else {
            None
        };

        // 解析 value 信息
        self.next_expect(Token::Keyword(Keyword::Values))?;
        // insert into tbl(a, b, c) values (1, 2, 3),(4, 5, 6);
        let mut values = Vec::new();
        loop {
            self.next_expect(Token::OpenParen)?;
            let mut exprs = Vec::new();
            loop {
                exprs.push(self.parse_expression()?);
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {}
                    token => {
                        return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
                    }
                }
            }
            values.push(exprs);
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        Ok(ast::Statement::Insert {
            table_name,
            columns,
            values,
        })
    }

    // 解析 Create Table 语句
    fn parse_ddl_create_table(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Table))?;
        // 期望是 Table 名
        let table_name = self.next_ident()?;
        // 表名之后应该是括号
        self.next_expect(Token::OpenParen)?;

        // 解析列信息
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_ddl_column()?);
            // 如果没有逗号，列解析完成，跳出
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        self.next_expect(Token::CloseParen)?;
        Ok(ast::Statement::CreateTable {
            name: table_name,
            columns,
        })
    }

    // 解析列信息
    fn parse_ddl_column(&mut self) -> Result<ast::Column> {
        let mut column = Column {
            name: self.next_ident()?,
            datatype: match self.next()? {
                Token::Keyword(Keyword::Int) | Token::Keyword(Keyword::Integer) => {
                    DataType::Integer
                }
                Token::Keyword(Keyword::Bool) | Token::Keyword(Keyword::Boolean) => {
                    DataType::Boolean
                }
                Token::Keyword(Keyword::Float) | Token::Keyword(Keyword::Double) => DataType::Float,
                Token::Keyword(Keyword::String)
                | Token::Keyword(Keyword::Text)
                | Token::Keyword(Keyword::Varchar) => DataType::String,
                token => return Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
            },
            nullable: None,
            default: None,
            primary_key: false,
            index: false,
        };

        // 解析列的默认值，以及是否可以为空
        while let Some(Token::Keyword(keyword)) = self.next_if_keyword() {
            match keyword {
                Keyword::Null => column.nullable = Some(true),
                Keyword::Not => {
                    self.next_expect(Token::Keyword(Keyword::Null))?;
                    column.nullable = Some(false);
                }
                Keyword::Default => column.default = Some(self.parse_expression()?),
                Keyword::Primary => {
                    self.next_expect(Token::Keyword(Keyword::Key))?;
                    column.primary_key = true;
                }
                Keyword::Index => column.index = true,
                k => return Err(Error::Parse(format!("[Parser] Unexpected keyword {}", k))),
            }
        }

        Ok(column)
    }

    // 解析 Drop Table 语句
    fn parse_ddl_drop_table(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Table))?;
        Ok(ast::Statement::DropTable {
            name: self.next_ident()?,
        })
    }

    // 解析 Update 语句
    fn parse_update(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Update))?;
        // 表名
        let table_name = self.next_ident()?;
        self.next_expect(Token::Keyword(Keyword::Set))?;

        let mut columns = BTreeMap::new();
        loop {
            let col = self.next_ident()?;
            self.next_expect(Token::Equal)?;
            let value = self.parse_expression()?;
            if columns.contains_key(&col) {
                return Err(Error::Parse(format!(
                    "[parser] Duplicate column {} for update",
                    col
                )));
            }
            columns.insert(col, value);
            // 如果没有逗号，列解析完成，跳出
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        Ok(ast::Statement::Update {
            table_name,
            columns,
            where_clause: self.parse_where_clause()?,
        })
    }

    // 解析 Delete 语句
    fn parse_delete(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Delete))?;
        self.next_expect(Token::Keyword(Keyword::From))?;
        // 表名
        let table_name = self.next_ident()?;

        Ok(ast::Statement::Delete {
            table_name,
            where_clause: self.parse_where_clause()?,
        })
    }

    // 解析事务语句
    fn parse_transaction(&mut self) -> Result<ast::Statement> {
        Ok(match self.next()? {
            Token::Keyword(Keyword::Begin) => ast::Statement::Begin,
            Token::Keyword(Keyword::Commit) => ast::Statement::Commit,
            Token::Keyword(Keyword::Rollback) => ast::Statement::Rollback,
            _ => return Err(Error::Parse("unknown transaction command".into())),
        })
    }

    // 解析 explain 语句
    fn parse_explain(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Explain))?;
        if let Some(Token::Keyword(Keyword::Explain)) = self.peek()? {
            return Err(Error::Parse("canno nest explain statement".into()));
        }
        let stmt = self.parse_statement()?;
        Ok(ast::Statement::Explain {
            stmt: Box::new(stmt),
        })
    }

    fn parse_where_clause(&mut self) -> Result<Option<Expression>> {
        if self.next_if_token(Token::Keyword(Keyword::Where)).is_none() {
            return Ok(None);
        }

        Ok(Some(self.parse_opreation_expr()?))
    }

    fn parse_having_clause(&mut self) -> Result<Option<Expression>> {
        if self
            .next_if_token(Token::Keyword(Keyword::Having))
            .is_none()
        {
            return Ok(None);
        }

        Ok(Some(self.parse_opreation_expr()?))
    }

    fn parse_order_clause(&mut self) -> Result<Vec<(String, OrderDirection)>> {
        let mut orders = Vec::new();
        if self.next_if_token(Token::Keyword(Keyword::Order)).is_none() {
            return Ok(orders);
        }
        self.next_expect(Token::Keyword(Keyword::By))?;

        loop {
            let col = self.next_ident()?;
            let ord = match self.next_if(|t| {
                matches!(
                    t,
                    Token::Keyword(Keyword::Asc) | Token::Keyword(Keyword::Desc)
                )
            }) {
                Some(Token::Keyword(Keyword::Asc)) => OrderDirection::Asc,
                Some(Token::Keyword(Keyword::Desc)) => OrderDirection::Desc,
                _ => OrderDirection::Asc,
            };
            orders.push((col, ord));

            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        Ok(orders)
    }

    fn parse_select_clause(&mut self) -> Result<Vec<(Expression, Option<String>)>> {
        self.next_expect(Token::Keyword(Keyword::Select))?;

        let mut select = Vec::new();
        // select *
        if self.next_if_token(Token::Asterisk).is_some() {
            return Ok(select);
        }

        loop {
            let expr = self.parse_expression()?;
            // 查看是否有别名
            let alias = match self.next_if_token(Token::Keyword(Keyword::As)) {
                Some(_) => Some(self.next_ident()?),
                None => None,
            };
            select.push((expr, alias));
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        Ok(select)
    }

    fn parse_from_clause(&mut self) -> Result<ast::FromItem> {
        // From 关键字
        self.next_expect(Token::Keyword(Keyword::From))?;

        // 第一个表名
        let mut item = self.parse_from_table_clause()?;
        // 是否有 Join
        while let Some(join_type) = self.parse_from_clause_join()? {
            let left = Box::new(item);
            let right = Box::new(self.parse_from_table_clause()?);

            // 解析 Join 条件
            let predicate = match join_type {
                ast::JoinType::Cross => None,
                _ => {
                    self.next_expect(Token::Keyword(Keyword::On))?;
                    let l = self.parse_expression()?;
                    self.next_expect(Token::Equal)?;
                    let r = self.parse_expression()?;

                    let (l, r) = match join_type {
                        ast::JoinType::Right => (r, l),
                        _ => (l, r),
                    };

                    let cond = Operation::Equal(Box::new(l), Box::new(r));
                    Some(ast::Expression::Operation(cond))
                }
            };

            item = ast::FromItem::Join {
                left,
                right,
                join_type,
                predicate,
            }
        }

        Ok(item)
    }

    fn parse_group_clause(&mut self) -> Result<Option<Expression>> {
        if self.next_if_token(Token::Keyword(Keyword::Group)).is_none() {
            return Ok(None);
        }

        self.next_expect(Token::Keyword(Keyword::By))?;
        Ok(Some(self.parse_expression()?))
    }

    fn parse_from_table_clause(&mut self) -> Result<ast::FromItem> {
        Ok(ast::FromItem::Table {
            name: self.next_ident()?,
        })
    }

    fn parse_from_clause_join(&mut self) -> Result<Option<ast::JoinType>> {
        // 是否是 Cross Join
        if self.next_if_token(Token::Keyword(Keyword::Cross)).is_some() {
            self.next_expect(Token::Keyword(Keyword::Join))?;
            Ok(Some(ast::JoinType::Cross)) // Cross Join
        } else if self.next_if_token(Token::Keyword(Keyword::Join)).is_some() {
            Ok(Some(ast::JoinType::Inner)) // Inner Join
        } else if self.next_if_token(Token::Keyword(Keyword::Left)).is_some() {
            self.next_expect(Token::Keyword(Keyword::Join))?;
            Ok(Some(ast::JoinType::Left)) // Left Join
        } else if self.next_if_token(Token::Keyword(Keyword::Right)).is_some() {
            self.next_expect(Token::Keyword(Keyword::Join))?;
            Ok(Some(ast::JoinType::Right)) // Right Join
        } else {
            Ok(None)
        }
    }

    fn parse_opreation_expr(&mut self) -> Result<ast::Expression> {
        let left = self.parse_expression()?;
        Ok(match self.next()? {
            Token::Equal => ast::Expression::Operation(Operation::Equal(
                Box::new(left),
                Box::new(self.compute_math_operator(1)?),
            )),
            Token::GreaterThan => ast::Expression::Operation(Operation::GreaterThan(
                Box::new(left),
                Box::new(self.compute_math_operator(1)?),
            )),
            Token::LessThan => ast::Expression::Operation(Operation::LessThan(
                Box::new(left),
                Box::new(self.compute_math_operator(1)?),
            )),
            _ => return Err(Error::Internal("Unexpected token".into())),
        })
    }

    // 解析表达式
    fn parse_expression(&mut self) -> Result<ast::Expression> {
        Ok(match self.next()? {
            Token::Ident(ident) => {
                // 函数
                // count(col_name)
                if self.next_if_token(Token::OpenParen).is_some() {
                    let col_name = self.next_ident()?;
                    self.next_expect(Token::CloseParen)?;
                    ast::Expression::Function(ident, col_name)
                } else {
                    // 列名
                    ast::Expression::Field(ident)
                }
            }
            Token::Number(n) => {
                if n.chars().all(|c| c.is_ascii_digit()) {
                    // 整数
                    ast::Consts::Integer(n.parse()?).into()
                } else {
                    // 浮点数
                    ast::Consts::Float(n.parse()?).into()
                }
            }
            Token::OpenParen => {
                let expr = self.compute_math_operator(1)?;
                self.next_expect(Token::CloseParen)?;
                expr
            }
            Token::String(s) => ast::Consts::String(s).into(),
            Token::Keyword(Keyword::True) => ast::Consts::Boolean(true).into(),
            Token::Keyword(Keyword::False) => ast::Consts::Boolean(false).into(),
            Token::Keyword(Keyword::Null) => ast::Consts::Null.into(),
            t => {
                return Err(Error::Parse(format!(
                    "[Parser] Unexpected expression token {}",
                    t
                )))
            }
        })
    }

    // 计算数学表达式
    // 5 + 2 + 1
    // 5 + 2 * 1
    fn compute_math_operator(&mut self, min_prec: i32) -> Result<Expression> {
        let mut left = self.parse_expression()?;
        loop {
            // 当前 Token
            let token = match self.peek()? {
                Some(t) => t,
                None => break,
            };

            if !token.is_operator() || token.precedence() < min_prec {
                break;
            }

            let next_prec = token.precedence() + 1;
            self.next()?;

            // 递归计算右边的表达式
            let right = self.compute_math_operator(next_prec)?;
            // 计算左右两边的值
            left = token.compute_expr(left, right)?;
        }
        Ok(left)
    }

    fn peek(&mut self) -> Result<Option<Token>> {
        self.lexer.peek().cloned().transpose()
    }

    fn next(&mut self) -> Result<Token> {
        self.lexer
            .next()
            .unwrap_or_else(|| Err(Error::Parse(format!("[Parser] Unexpected end of input"))))
    }

    fn next_ident(&mut self) -> Result<String> {
        match self.next()? {
            Token::Ident(ident) => Ok(ident),
            token => Err(Error::Parse(format!(
                "[Parser] Expected ident, got token {}",
                token
            ))),
        }
    }

    fn next_expect(&mut self, expect: Token) -> Result<()> {
        let token = self.next()?;
        if token != expect {
            return Err(Error::Parse(format!(
                "[Parser] Expected token {}, got {}",
                expect, token
            )));
        }
        Ok(())
    }

    // 如果满足条件，则跳转到下一个 Token
    fn next_if<F: Fn(&Token) -> bool>(&mut self, predicate: F) -> Option<Token> {
        self.peek().unwrap_or(None).filter(|t| predicate(t))?;
        self.next().ok()
    }

    // 如果下一个 Token 是关键字，则跳转
    fn next_if_keyword(&mut self) -> Option<Token> {
        self.next_if(|t| matches!(t, Token::Keyword(_)))
    }

    fn next_if_token(&mut self, token: Token) -> Option<Token> {
        self.next_if(|t| t == &token)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::Result,
        sql::parser::ast::{self, Consts, Expression, OrderDirection},
    };

    use super::Parser;

    #[test]
    fn test_parser_create_table() -> Result<()> {
        let sql1 = "
            create table tbl1 (
                a int default 100,
                b float not null,
                c varchar null,
                d bool default true
            );
        ";
        let stmt1 = Parser::new(sql1).parse()?;

        let sql2 = "
        create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        );
        ";
        let stmt2 = Parser::new(sql2).parse()?;
        assert_eq!(stmt1, stmt2);

        let sql3 = "
            create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        )
        ";

        let stmt3 = Parser::new(sql3).parse();
        assert!(stmt3.is_err());
        Ok(())
    }

    #[test]
    fn test_parser_insert() -> Result<()> {
        let sql1 = "insert into tbl1 values (1, 2, 3, 'a', true);";
        let stmt1 = Parser::new(sql1).parse()?;
        assert_eq!(
            stmt1,
            ast::Statement::Insert {
                table_name: "tbl1".to_string(),
                columns: None,
                values: vec![vec![
                    ast::Consts::Integer(1).into(),
                    ast::Consts::Integer(2).into(),
                    ast::Consts::Integer(3).into(),
                    ast::Consts::String("a".to_string()).into(),
                    ast::Consts::Boolean(true).into(),
                ]],
            }
        );

        let sql2 = "insert into tbl2 (c1, c2, c3) values (3, 'a', true),(4, 'b', false);";
        let stmt2 = Parser::new(sql2).parse()?;
        assert_eq!(
            stmt2,
            ast::Statement::Insert {
                table_name: "tbl2".to_string(),
                columns: Some(vec!["c1".to_string(), "c2".to_string(), "c3".to_string()]),
                values: vec![
                    vec![
                        ast::Consts::Integer(3).into(),
                        ast::Consts::String("a".to_string()).into(),
                        ast::Consts::Boolean(true).into(),
                    ],
                    vec![
                        ast::Consts::Integer(4).into(),
                        ast::Consts::String("b".to_string()).into(),
                        ast::Consts::Boolean(false).into(),
                    ],
                ],
            }
        );

        Ok(())
    }

    #[test]
    fn test_parser_select() -> Result<()> {
        let sql = "select * from tbl1 where a = 100 limit 10 offset 20;";
        let stmt = Parser::new(sql).parse()?;
        assert_eq!(
            stmt,
            ast::Statement::Select {
                select: vec![],
                from: ast::FromItem::Table {
                    name: "tbl1".into()
                },
                where_clause: Some(ast::Expression::Operation(ast::Operation::Equal(
                    Box::new(ast::Expression::Field("a".into())),
                    Box::new(ast::Expression::Consts(Consts::Integer(100)))
                ))),
                group_by: None,
                having: None,
                order_by: vec![],
                limit: Some(Expression::Consts(Consts::Integer(10))),
                offset: Some(Expression::Consts(Consts::Integer(20))),
            }
        );

        let sql = "select * from tbl1 order by a, b asc, c desc;";
        let stmt = Parser::new(sql).parse()?;
        assert_eq!(
            stmt,
            ast::Statement::Select {
                select: vec![],
                from: ast::FromItem::Table {
                    name: "tbl1".into()
                },
                where_clause: None,
                group_by: None,
                order_by: vec![
                    ("a".to_string(), OrderDirection::Asc),
                    ("b".to_string(), OrderDirection::Asc),
                    ("c".to_string(), OrderDirection::Desc),
                ],
                having: None,
                limit: None,
                offset: None,
            }
        );

        let sql = "select a as col1, b as col2, c from tbl1 order by a, b asc, c desc;";
        let stmt = Parser::new(sql).parse()?;
        assert_eq!(
            stmt,
            ast::Statement::Select {
                select: vec![
                    (Expression::Field("a".into()), Some("col1".into())),
                    (Expression::Field("b".into()), Some("col2".into())),
                    (Expression::Field("c".into()), None),
                ],
                from: ast::FromItem::Table {
                    name: "tbl1".into()
                },
                where_clause: None,
                group_by: None,
                having: None,
                order_by: vec![
                    ("a".to_string(), OrderDirection::Asc),
                    ("b".to_string(), OrderDirection::Asc),
                    ("c".to_string(), OrderDirection::Desc),
                ],
                limit: None,
                offset: None,
            }
        );

        let sql = "select * from tbl1 cross join tbl2 cross join tbl3;";
        let stmt = Parser::new(sql).parse()?;
        assert_eq!(
            stmt,
            ast::Statement::Select {
                select: vec![],
                from: ast::FromItem::Join {
                    left: Box::new(ast::FromItem::Join {
                        left: Box::new(ast::FromItem::Table {
                            name: "tbl1".into()
                        }),
                        right: Box::new(ast::FromItem::Table {
                            name: "tbl2".into()
                        }),
                        join_type: ast::JoinType::Cross,
                        predicate: None
                    }),
                    right: Box::new(ast::FromItem::Table {
                        name: "tbl3".into()
                    }),
                    join_type: ast::JoinType::Cross,
                    predicate: None
                },
                where_clause: None,
                group_by: None,
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );

        let sql = "select count(a), min(b), max(c) from tbl1 group by a having min = 10;";
        let stmt = Parser::new(sql).parse()?;
        assert_eq!(
            stmt,
            ast::Statement::Select {
                select: vec![
                    (ast::Expression::Function("count".into(), "a".into()), None),
                    (ast::Expression::Function("min".into(), "b".into()), None),
                    (ast::Expression::Function("max".into(), "c".into()), None),
                ],
                from: ast::FromItem::Table {
                    name: "tbl1".into()
                },
                where_clause: None,
                group_by: Some(ast::Expression::Field("a".into())),
                having: Some(ast::Expression::Operation(ast::Operation::Equal(
                    Box::new(ast::Expression::Field("min".into())),
                    Box::new(ast::Expression::Consts(Consts::Integer(10)))
                ))),
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );

        Ok(())
    }

    #[test]
    fn test_parser_update() -> Result<()> {
        let sql = "update tabl set a = 1, b = 2.0 where c = 'a';";
        let stmt = Parser::new(sql).parse()?;
        assert_eq!(
            stmt,
            ast::Statement::Update {
                table_name: "tabl".into(),
                columns: vec![
                    ("a".into(), ast::Consts::Integer(1).into()),
                    ("b".into(), ast::Consts::Float(2.0).into()),
                ]
                .into_iter()
                .collect(),
                where_clause: Some(ast::Expression::Operation(ast::Operation::Equal(
                    Box::new(ast::Expression::Field("c".into())),
                    Box::new(ast::Expression::Consts(Consts::String("a".into())))
                ))),
            }
        );

        Ok(())
    }
}
