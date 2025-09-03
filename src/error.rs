use thiserror::Error;

pub type QuillSQLResult<T, E = QuillSQLError> = Result<T, E>;

#[derive(Debug, Error)]
pub enum QuillSQLError {
    #[error("Not support: {0}")]
    NotSupport(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parser error: {0}")]
    Parser(#[from] sqlparser::parser::ParserError),

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("Plan error: {0}")]
    Plan(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Unwind")]
    Unwind,
}
