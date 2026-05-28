use thiserror::Error;

pub type JitResult<T> = std::result::Result<T, JitError>;

#[derive(Debug, Error)]
pub enum JitError {
    #[error("unsupported JIT expression: {0}")]
    UnsupportedExpr(String),
    #[error("unsupported JIT type: {0}")]
    UnsupportedType(String),
    #[error("JIT backend error: {0}")]
    Backend(String),
}
