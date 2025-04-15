// 自定义 Result 类型
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    Parse(String),
}

impl From<std::num::ParseIntError> for Error {
    fn from(value: std::num::ParseIntError) -> Self {
        Error::Parse(value.to_string())
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(value: std::num::ParseFloatError) -> Self {
        Error::Parse(value.to_string())
    }
}
