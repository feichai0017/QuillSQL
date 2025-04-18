use std::{array::TryFromSliceError, fmt::Display, string::FromUtf8Error, sync::PoisonError};

use bincode::ErrorKind;
use serde::{de, ser};

// 自定义 Result 类型
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    Parse(String),
    Internal(String),
    WriteConflict,
    KeyNotFound,
    KeyAlreadyExists,
    UnexpectedError,
    KeyOverflowError,
    ValueOverflowError,
    TryFromSliceError(&'static str),
    UTF8Error,
    NodeTooLarge,
    Io(String),
    Bincode(String),
    NotImplemented(String),
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

impl<T> From<PoisonError<T>> for Error {
    fn from(value: PoisonError<T>) -> Self {
        Error::Internal(value.to_string())
    }
}

impl From<Box<ErrorKind>> for Error {
    fn from(value: Box<ErrorKind>) -> Self {
        Error::Internal(value.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::Io(value.to_string())
    }
}

impl From<TryFromSliceError> for Error {
    fn from(value: TryFromSliceError) -> Self {
        Error::Internal(value.to_string())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_value: FromUtf8Error) -> Self {
        Error::UTF8Error
    }
}

impl std::error::Error for Error {}

impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Internal(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Internal(msg.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Parse(err) => write!(f, "parse error {}", err),
            Error::Internal(err) => write!(f, "internal error {}", err),
            Error::WriteConflict => write!(f, "write conflict, try transaction"),
            Error::KeyNotFound => write!(f, "key not found"),
            Error::KeyAlreadyExists => write!(f, "key already exists"),
            Error::UnexpectedError => write!(f, "unexpected error"),
            Error::KeyOverflowError => write!(f, "key overflow error"),
            Error::ValueOverflowError => write!(f, "value overflow error"),
            Error::TryFromSliceError(err) => write!(f, "try from slice error {}", err),
            Error::UTF8Error => write!(f, "utf8 error"),
            Error::NodeTooLarge => write!(f, "node data too large to fit in a single page"),
            Error::Io(err) => write!(f, "IO Error: {}", err),
            Error::Bincode(err) => write!(f, "Serialization/Deserialization error: {}", err),
            Error::NotImplemented(feature) => write!(f, "Feature not implemented: {}", feature),
        }
    }
}
