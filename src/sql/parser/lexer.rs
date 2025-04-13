use std::{fmt::Display, iter::Peekable, str::Chars};

use crate::error::{Error, Result};

use super::ast::{Consts, Expression};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // defined keywords
    Keyword(Keyword),
    // other types of string tokens, such as table names and column names
    Ident(String),
    // string type data
    String(String),
    // numeric type, such as integers and floating-point numbers
    Number(String),
    // left parenthesis (
    OpenParen,
    // right parenthesis )
    CloseParen,
    // comma ,
    Comma,
    // semicolon ;
    Semicolon,
    // asterisk & multiplication sign *
    Asterisk,
    // plus sign +
    Plus,
    // minus sign -
    Minus,
    // slash & division sign /
    Slash,
    // equal sign =
    Equal,
    // greater than >
    GreaterThan,
    // less than <
    LessThan,
}

impl Token {
    // check if it is an operator
    pub fn is_operator(&self) -> bool {
        match self {
            Token::Plus | Token::Minus | Token::Asterisk | Token::Slash => true,
            _ => false,
        }
    }

    // get the precedence of the operator
    pub fn precedence(&self) -> i32 {
        match self {
            Token::Plus | Token::Minus => 1,
            Token::Asterisk | Token::Slash => 2,
            _ => 0,
        }
    }

    // compute the expression based on the operator
    pub fn compute_expr(&self, l: Expression, r: Expression) -> Result<Expression> {
        let val = match (l, r) {
            (Expression::Consts(c1), Expression::Consts(c2)) => match (c1, c2) {
                (super::ast::Consts::Integer(l), super::ast::Consts::Integer(r)) => {
                    self.compute(l as f64, r as f64)?
                }
                (super::ast::Consts::Integer(l), super::ast::Consts::Float(r)) => {
                    self.compute(l as f64, r)?
                }
                (super::ast::Consts::Float(l), super::ast::Consts::Integer(r)) => {
                    self.compute(l, r as f64)?
                }
                (super::ast::Consts::Float(l), super::ast::Consts::Float(r)) => {
                    self.compute(l, r)?
                }
                _ => return Err(Error::Parse("cannot compute the expresssion".into())),
            },
            _ => return Err(Error::Parse("cannot compute the expresssion".into())),
        };
        Ok(Expression::Consts(Consts::Float(val)))
    }

    fn compute(&self, l: f64, r: f64) -> Result<f64> {
        Ok(match self {
            Token::Asterisk => l * r,
            Token::Plus => l + r,
            Token::Minus => l - r,
            Token::Slash => l / r,
            _ => return Err(Error::Parse("cannot compute the expresssion".into())),
        })
    }
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Token::Keyword(keyword) => keyword.to_str(),
            Token::Ident(ident) => ident,
            Token::String(v) => v,
            Token::Number(n) => n,
            Token::OpenParen => "(",
            Token::CloseParen => ")",
            Token::Comma => ",",
            Token::Semicolon => ";",
            Token::Asterisk => "*",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Slash => "/",
            Token::Equal => "=",
            Token::GreaterThan => ">",
            Token::LessThan => "<",
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
    Create,
    Table,
    Int,
    Integer,
    Boolean,
    Bool,
    String,
    Text,
    Varchar,
    Float,
    Double,
    Select,
    From,
    Insert,
    Into,
    Values,
    True,
    False,
    Default,
    Not,
    Null,
    Primary,
    Key,
    Update,
    Set,
    Where,
    Delete,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Offset,
    As,
    Cross,
    Join,
    Left,
    Right,
    On,
    Group,
    Having,
    Begin,
    Commit,
    Rollback,
    Index,
    Explain,
    Drop,
}

impl Keyword {
    pub fn from_str(ident: &str) -> Option<Self> {
        Some(match ident.to_uppercase().as_ref() {
            "CREATE" => Keyword::Create,
            "TABLE" => Keyword::Table,
            "INT" => Keyword::Int,
            "INTEGER" => Keyword::Integer,
            "BOOLEAN" => Keyword::Boolean,
            "BOOL" => Keyword::Bool,
            "STRING" => Keyword::String,
            "TEXT" => Keyword::Text,
            "VARCHAR" => Keyword::Varchar,
            "FLOAT" => Keyword::Float,
            "DOUBLE" => Keyword::Double,
            "SELECT" => Keyword::Select,
            "FROM" => Keyword::From,
            "INSERT" => Keyword::Insert,
            "INTO" => Keyword::Into,
            "VALUES" => Keyword::Values,
            "TRUE" => Keyword::True,
            "FALSE" => Keyword::False,
            "DEFAULT" => Keyword::Default,
            "NOT" => Keyword::Not,
            "NULL" => Keyword::Null,
            "PRIMARY" => Keyword::Primary,
            "KEY" => Keyword::Key,
            "UPDATE" => Keyword::Update,
            "SET" => Keyword::Set,
            "WHERE" => Keyword::Where,
            "DELETE" => Keyword::Delete,
            "ORDER" => Keyword::Order,
            "BY" => Keyword::By,
            "ASC" => Keyword::Asc,
            "DESC" => Keyword::Desc,
            "LIMIT" => Keyword::Limit,
            "OFFSET" => Keyword::Offset,
            "AS" => Keyword::As,
            "CROSS" => Keyword::Cross,
            "JOIN" => Keyword::Join,
            "LEFT" => Keyword::Left,
            "RIGHT" => Keyword::Right,
            "ON" => Keyword::On,
            "GROUP" => Keyword::Group,
            "HAVING" => Keyword::Having,
            "BEGIN" => Keyword::Begin,
            "COMMIT" => Keyword::Commit,
            "ROLLBACK" => Keyword::Rollback,
            "INDEX" => Keyword::Index,
            "EXPLAIN" => Keyword::Explain,
            "DROP" => Keyword::Drop,
            _ => return None,
        })
    }

    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Create => "CREATE",
            Keyword::Table => "TABLE",
            Keyword::Int => "INT",
            Keyword::Integer => "INTEGER",
            Keyword::Boolean => "BOOLEAN",
            Keyword::Bool => "BOOL",
            Keyword::String => "STRING",
            Keyword::Text => "TEXT",
            Keyword::Varchar => "VARCHAR",
            Keyword::Float => "FLOAT",
            Keyword::Double => "DOUBLE",
            Keyword::Select => "SELECT",
            Keyword::From => "FROM",
            Keyword::Insert => "INSERT",
            Keyword::Into => "INTO",
            Keyword::Values => "VALUES",
            Keyword::True => "TRUE",
            Keyword::False => "FALSE",
            Keyword::Default => "DEFAULT",
            Keyword::Not => "NOT",
            Keyword::Null => "NULL",
            Keyword::Primary => "PRIMARY",
            Keyword::Key => "KEY",
            Keyword::Update => "UPDATE",
            Keyword::Set => "SET",
            Keyword::Where => "WHERE",
            Keyword::Delete => "DELETE",
            Keyword::Order => "ORDER",
            Keyword::By => "BY",
            Keyword::Asc => "ASC",
            Keyword::Desc => "DESC",
            Keyword::Limit => "LIMIT",
            Keyword::Offset => "OFFSET",
            Keyword::As => "AS",
            Keyword::Cross => "CROSS",
            Keyword::Join => "JOIN",
            Keyword::Left => "LEFT",
            Keyword::Right => "RIGHT",
            Keyword::On => "ON",
            Keyword::Group => "GROUP",
            Keyword::Having => "HAVING",
            Keyword::Begin => "BEGIN",
            Keyword::Commit => "COMMIT",
            Keyword::Rollback => "ROLLBACK",
            Keyword::Index => "INDEX",
            Keyword::Explain => "EXPLAIN",
            Keyword::Drop => "DROP",
        }
    }
}

impl Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

// define the Lexer struct
// currently supported SQL syntax
// see README.md
pub struct Lexer<'a> {
    iter: Peekable<Chars<'a>>,
}

// custom iterator, return Token
impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => self
                .iter
                .peek()
                .map(|c| Err(Error::Parse(format!("[Lexer] Unexpeted character {}", c)))),
            Err(err) => Some(Err(err)),
        }
    }
}

impl<'a> Lexer<'a> {
    pub fn new(sql_text: &'a str) -> Self {
        Self {
            iter: sql_text.chars().peekable(),
        }
    }

    // erase whitespace
    // eg. selct *       from        t;
    fn erase_whitespace(&mut self) {
        self.next_while(|c| c.is_whitespace());
    }

    // if the condition is met, skip to the next character and return the character
    fn next_if<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<char> {
        self.iter.peek().filter(|&c| predicate(*c))?;
        self.iter.next()
    }

    // check if the current character satisfies the condition, if so, skip to the next character
    fn next_while<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<String> {
        let mut value = String::new();
        while let Some(c) = self.next_if(&predicate) {
            value.push(c);
        }

        Some(value).filter(|v| !v.is_empty())
    }

    // only if it is a Token type, skip to the next character and return the Token
    fn next_if_token<F: Fn(char) -> Option<Token>>(&mut self, predicate: F) -> Option<Token> {
        let token = self.iter.peek().and_then(|c| predicate(*c))?;
        self.iter.next();
        Some(token)
    }

    // scan the next Token
    fn scan(&mut self) -> Result<Option<Token>> {
        // erase whitespace
        self.erase_whitespace();
        // based on the first character, determine the type of the Token
        match self.iter.peek() {
            Some('\'') => self.scan_string(), // scan string
            Some(c) if c.is_ascii_digit() => Ok(self.scan_number()), // scan number
            Some(c) if c.is_alphabetic() => Ok(self.scan_ident()), // scan Ident type
            Some(_) => Ok(self.scan_symbol()), // scan symbol
            None => Ok(None),
        }
    }

    // scan string
    fn scan_string(&mut self) -> Result<Option<Token>> {
        // check if it is a single quote
        if self.next_if(|c| c == '\'').is_none() {
            return Ok(None);
        }

        let mut val = String::new();
        loop {
            match self.iter.next() {
                Some('\'') => break,
                Some(c) => val.push(c),
                None => return Err(Error::Parse(format!("[Lexer] Unexpected end of string"))),
            }
        }

        Ok(Some(Token::String(val)))
    }

    // scan number
    fn scan_number(&mut self) -> Option<Token> {
        // scan part of the number
        let mut num = self.next_while(|c| c.is_ascii_digit())?;
        // if there is a decimal point, it is a floating-point number
        if let Some(sep) = self.next_if(|c| c == '.') {
            num.push(sep);
            // scan the part after the decimal point
            while let Some(c) = self.next_if(|c| c.is_ascii_digit()) {
                num.push(c);
            }
        }

        Some(Token::Number(num))
    }

    // scan Ident type, such as table names, column names, etc., also possible keywords, true / false
    fn scan_ident(&mut self) -> Option<Token> {
        let mut value = self.next_if(|c| c.is_alphabetic())?.to_string();
        while let Some(c) = self.next_if(|c| c.is_alphanumeric() || c == '_') {
            value.push(c);
        }

        Some(Keyword::from_str(&value).map_or(Token::Ident(value.to_lowercase()), Token::Keyword))
    }

    // scan symbol
    fn scan_symbol(&mut self) -> Option<Token> {
        self.next_if_token(|c| match c {
            '*' => Some(Token::Asterisk),
            '(' => Some(Token::OpenParen),
            ')' => Some(Token::CloseParen),
            ',' => Some(Token::Comma),
            ';' => Some(Token::Semicolon),
            '+' => Some(Token::Plus),
            '-' => Some(Token::Minus),
            '/' => Some(Token::Slash),
            '=' => Some(Token::Equal),
            '>' => Some(Token::GreaterThan),
            '<' => Some(Token::LessThan),
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::Lexer;
    use crate::{
        error::Result,
        sql::parser::lexer::{Keyword, Token},
    };

    #[test]
    fn test_lexer_create_table() -> Result<()> {
        let tokens1 = Lexer::new(
            "CREATE table tbl
                (
                    id1 int primary key,
                    id2 integer
                );
                ",
        )
        .peekable()
        .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Create),
                Token::Keyword(Keyword::Table),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id1".to_string()),
                Token::Keyword(Keyword::Int),
                Token::Keyword(Keyword::Primary),
                Token::Keyword(Keyword::Key),
                Token::Comma,
                Token::Ident("id2".to_string()),
                Token::Keyword(Keyword::Integer),
                Token::CloseParen,
                Token::Semicolon
            ]
        );

        let tokens2 = Lexer::new(
            "CREATE table tbl
                        (
                            id1 int primary key,
                            id2 integer,
                            c1 bool null,
                            c2 boolean not null,
                            c3 float null,
                            c4 double,
                            c5 string,
                            c6 text,
                            c7 varchar default 'foo',
                            c8 int default 100,
                            c9 integer
                        );
                        ",
        )
        .peekable()
        .collect::<Result<Vec<_>>>()?;

        assert!(tokens2.len() > 0);

        Ok(())
    }

    #[test]
    fn test_lexer_insert_into() -> Result<()> {
        let tokens1 = Lexer::new("insert into tbl values (1, 2, '3', true, false, 4.55);")
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Ident("tbl".to_string()),
                Token::Keyword(Keyword::Values),
                Token::OpenParen,
                Token::Number("1".to_string()),
                Token::Comma,
                Token::Number("2".to_string()),
                Token::Comma,
                Token::String("3".to_string()),
                Token::Comma,
                Token::Keyword(Keyword::True),
                Token::Comma,
                Token::Keyword(Keyword::False),
                Token::Comma,
                Token::Number("4.55".to_string()),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );

        let tokens2 = Lexer::new("INSERT INTO       tbl (id, name, age) values (100, 'db', 10);")
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens2,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id".to_string()),
                Token::Comma,
                Token::Ident("name".to_string()),
                Token::Comma,
                Token::Ident("age".to_string()),
                Token::CloseParen,
                Token::Keyword(Keyword::Values),
                Token::OpenParen,
                Token::Number("100".to_string()),
                Token::Comma,
                Token::String("db".to_string()),
                Token::Comma,
                Token::Number("10".to_string()),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );
        Ok(())
    }

    #[test]
    fn test_lexer_select() -> Result<()> {
        let tokens1 = Lexer::new("select * from tbl;")
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Select),
                Token::Asterisk,
                Token::Keyword(Keyword::From),
                Token::Ident("tbl".to_string()),
                Token::Semicolon,
            ]
        );
        Ok(())
    }
}
