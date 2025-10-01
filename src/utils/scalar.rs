use crate::catalog::DataType;
use crate::error::{QuillSQLError, QuillSQLResult};
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub enum ScalarValue {
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Varchar(Option<String>),
}

impl ScalarValue {
    pub fn new_empty(data_type: DataType) -> Self {
        match data_type {
            DataType::Boolean => Self::Boolean(None),
            DataType::Int8 => Self::Int8(None),
            DataType::Int16 => Self::Int16(None),
            DataType::Int32 => Self::Int32(None),
            DataType::Int64 => Self::Int64(None),
            DataType::UInt8 => Self::UInt8(None),
            DataType::UInt16 => Self::UInt16(None),
            DataType::UInt32 => Self::UInt32(None),
            DataType::UInt64 => Self::UInt64(None),
            DataType::Float32 => Self::Float32(None),
            DataType::Float64 => Self::Float64(None),
            DataType::Varchar(_) => Self::Varchar(None),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Varchar(_) => DataType::Varchar(None),
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            ScalarValue::Boolean(v) => v.is_none(),
            ScalarValue::Int8(v) => v.is_none(),
            ScalarValue::Int16(v) => v.is_none(),
            ScalarValue::Int32(v) => v.is_none(),
            ScalarValue::Int64(v) => v.is_none(),
            ScalarValue::UInt8(v) => v.is_none(),
            ScalarValue::UInt16(v) => v.is_none(),
            ScalarValue::UInt32(v) => v.is_none(),
            ScalarValue::UInt64(v) => v.is_none(),
            ScalarValue::Float32(v) => v.is_none(),
            ScalarValue::Float64(v) => v.is_none(),
            ScalarValue::Varchar(v) => v.is_none(),
        }
    }

    /// Try to cast this value to a ScalarValue of type `data_type`
    pub fn cast_to(&self, data_type: &DataType) -> QuillSQLResult<Self> {
        let error =
            QuillSQLError::NotSupport(format!("Failed to cast {:?} to {} type", self, data_type));

        if &self.data_type() == data_type {
            return Ok(self.clone());
        }

        match data_type {
            DataType::Int8 => {
                let data = match self {
                    ScalarValue::Int64(v) => Ok(v.map(|v| v as i8)),
                    _ => Err(error),
                };
                data.map(ScalarValue::Int8)
            }
            DataType::Int16 => {
                let data = match self {
                    ScalarValue::Int8(v) => Ok(v.map(|v| v as i16)),
                    ScalarValue::Int64(v) => Ok(v.map(|v| v as i16)),
                    _ => Err(error),
                };
                data.map(ScalarValue::Int16)
            }
            DataType::Int32 => {
                let data = match self {
                    ScalarValue::Int8(v) => Ok(v.map(|v| v as i32)),
                    ScalarValue::Int64(v) => Ok(v.map(|v| v as i32)),
                    _ => Err(error),
                };
                data.map(ScalarValue::Int32)
            }
            DataType::Int64 => {
                let data = match self {
                    ScalarValue::Int8(v) => Ok(v.map(|v| v as i64)),
                    ScalarValue::Int32(v) => Ok(v.map(|v| v as i64)),
                    _ => Err(error),
                };
                data.map(ScalarValue::Int64)
            }
            DataType::UInt8 => {
                let data = match self {
                    ScalarValue::Int8(v) => Ok(v.map(|v| v as u8)),
                    ScalarValue::Int64(v) => Ok(v.map(|v| v as u8)),
                    _ => Err(error),
                };
                data.map(ScalarValue::UInt8)
            }
            DataType::UInt16 => {
                let data = match self {
                    ScalarValue::Int8(v) => Ok(v.map(|v| v as u16)),
                    ScalarValue::Int64(v) => Ok(v.map(|v| v as u16)),
                    _ => Err(error),
                };
                data.map(ScalarValue::UInt16)
            }
            DataType::UInt32 => {
                let data = match self {
                    ScalarValue::Int8(v) => Ok(v.map(|v| v as u32)),
                    ScalarValue::Int64(v) => Ok(v.map(|v| v as u32)),
                    _ => Err(error),
                };
                data.map(ScalarValue::UInt32)
            }
            DataType::UInt64 => {
                let data = match self {
                    ScalarValue::Int8(v) => Ok(v.map(|v| v as u64)),
                    ScalarValue::Int64(v) => Ok(v.map(|v| v as u64)),
                    _ => Err(error),
                };
                data.map(ScalarValue::UInt64)
            }
            DataType::Float32 => {
                let data = match self {
                    ScalarValue::Int8(v) => Ok(v.map(|v| v as f32)),
                    ScalarValue::Int64(v) => Ok(v.map(|v| v as f32)),
                    ScalarValue::Float64(v) => Ok(v.map(|v| v as f32)),
                    _ => Err(error),
                };
                data.map(ScalarValue::Float32)
            }
            DataType::Float64 => {
                let data = match self {
                    ScalarValue::Int8(v) => Ok(v.map(|v| v as f64)),
                    ScalarValue::Int32(v) => Ok(v.map(|v| v as f64)),
                    ScalarValue::Int64(v) => Ok(v.map(|v| v as f64)),
                    _ => Err(error),
                };
                data.map(ScalarValue::Float64)
            }
            DataType::Varchar(_) => {
                match self {
                    // If already a string, keep it and optionally truncate to the target length
                    ScalarValue::Varchar(v) => Ok(ScalarValue::Varchar(v.clone())),
                    // Simple numeric to string example kept for minimal compatibility
                    ScalarValue::Int8(v) => Ok(ScalarValue::Varchar(v.map(|v| v.to_string()))),
                    _ => Err(error),
                }
            }
            _ => Err(error),
        }
    }

    pub fn as_boolean(&self) -> QuillSQLResult<Option<bool>> {
        match self {
            ScalarValue::Boolean(v) => Ok(*v),
            _ => Err(QuillSQLError::Internal(format!(
                "Cannot treat {:?} as boolean",
                self
            ))),
        }
    }

    pub fn wrapping_add(&self, other: Self) -> QuillSQLResult<Self> {
        use ScalarValue::*;
        match (self, other.clone()) {
            (Int8(Some(a)), Int8(Some(b))) => Ok(Int8(Some(a.wrapping_add(b)))),
            (Int16(Some(a)), Int16(Some(b))) => Ok(Int16(Some(a.wrapping_add(b)))),
            (Int32(Some(a)), Int32(Some(b))) => Ok(Int32(Some(a.wrapping_add(b)))),
            (Int64(Some(a)), Int64(Some(b))) => Ok(Int64(Some(a.wrapping_add(b)))),
            (UInt8(Some(a)), UInt8(Some(b))) => Ok(UInt8(Some(a.wrapping_add(b)))),
            (UInt16(Some(a)), UInt16(Some(b))) => Ok(UInt16(Some(a.wrapping_add(b)))),
            (UInt32(Some(a)), UInt32(Some(b))) => Ok(UInt32(Some(a.wrapping_add(b)))),
            (UInt64(Some(a)), UInt64(Some(b))) => Ok(UInt64(Some(a.wrapping_add(b)))),
            (Float32(Some(a)), Float32(Some(b))) => Ok(Float32(Some(a + b))),
            (Float64(Some(a)), Float64(Some(b))) => Ok(Float64(Some(a + b))),
            (Int8(None), _)
            | (Int16(None), _)
            | (Int32(None), _)
            | (Int64(None), _)
            | (UInt8(None), _)
            | (UInt16(None), _)
            | (UInt32(None), _)
            | (UInt64(None), _)
            | (Float32(None), _)
            | (Float64(None), _) => Ok(self.clone()),
            _ => Err(QuillSQLError::Execution(format!(
                "Unsupported addition between {:?} and {:?}",
                self, other
            ))),
        }
    }

    pub fn wrapping_sub(&self, other: Self) -> QuillSQLResult<Self> {
        use ScalarValue::*;
        match (self, other.clone()) {
            (Int8(Some(a)), Int8(Some(b))) => Ok(Int8(Some(a.wrapping_sub(b)))),
            (Int16(Some(a)), Int16(Some(b))) => Ok(Int16(Some(a.wrapping_sub(b)))),
            (Int32(Some(a)), Int32(Some(b))) => Ok(Int32(Some(a.wrapping_sub(b)))),
            (Int64(Some(a)), Int64(Some(b))) => Ok(Int64(Some(a.wrapping_sub(b)))),
            (UInt8(Some(a)), UInt8(Some(b))) => Ok(UInt8(Some(a.wrapping_sub(b)))),
            (UInt16(Some(a)), UInt16(Some(b))) => Ok(UInt16(Some(a.wrapping_sub(b)))),
            (UInt32(Some(a)), UInt32(Some(b))) => Ok(UInt32(Some(a.wrapping_sub(b)))),
            (UInt64(Some(a)), UInt64(Some(b))) => Ok(UInt64(Some(a.wrapping_sub(b)))),
            (Float32(Some(a)), Float32(Some(b))) => Ok(Float32(Some(a - b))),
            (Float64(Some(a)), Float64(Some(b))) => Ok(Float64(Some(a - b))),
            (Int8(None), _)
            | (Int16(None), _)
            | (Int32(None), _)
            | (Int64(None), _)
            | (UInt8(None), _)
            | (UInt16(None), _)
            | (UInt32(None), _)
            | (UInt64(None), _)
            | (Float32(None), _)
            | (Float64(None), _) => Ok(self.clone()),
            _ => Err(QuillSQLError::Execution(format!(
                "Unsupported subtraction between {:?} and {:?}",
                self, other
            ))),
        }
    }

    pub fn wrapping_mul(&self, other: Self) -> QuillSQLResult<Self> {
        use ScalarValue::*;
        match (self, other.clone()) {
            (Int32(Some(a)), Int32(Some(b))) => Ok(Int32(Some(a.wrapping_mul(b)))),
            (Int64(Some(a)), Int64(Some(b))) => Ok(Int64(Some(a.wrapping_mul(b)))),
            (UInt32(Some(a)), UInt32(Some(b))) => Ok(UInt32(Some(a.wrapping_mul(b)))),
            (UInt64(Some(a)), UInt64(Some(b))) => Ok(UInt64(Some(a.wrapping_mul(b)))),
            (Float32(Some(a)), Float32(Some(b))) => Ok(Float32(Some(a * b))),
            (Float64(Some(a)), Float64(Some(b))) => Ok(Float64(Some(a * b))),
            (Int8(Some(a)), Int8(Some(b))) => Ok(Int8(Some(a.wrapping_mul(b)))),
            (Int16(Some(a)), Int16(Some(b))) => Ok(Int16(Some(a.wrapping_mul(b)))),
            (UInt8(Some(a)), UInt8(Some(b))) => Ok(UInt8(Some(a.wrapping_mul(b)))),
            (UInt16(Some(a)), UInt16(Some(b))) => Ok(UInt16(Some(a.wrapping_mul(b)))),
            (Int8(None), _)
            | (Int16(None), _)
            | (Int32(None), _)
            | (Int64(None), _)
            | (UInt8(None), _)
            | (UInt16(None), _)
            | (UInt32(None), _)
            | (UInt64(None), _)
            | (Float32(None), _)
            | (Float64(None), _) => Ok(self.clone()),
            _ => Err(QuillSQLError::Execution(format!(
                "Unsupported multiplication between {:?} and {:?}",
                self, other
            ))),
        }
    }

    pub fn wrapping_div(&self, other: Self) -> QuillSQLResult<Self> {
        use ScalarValue::*;
        match (self, other.clone()) {
            (Int8(Some(a)), Int8(Some(b))) => Ok(Int8(Some(a.wrapping_div(b)))),
            (Int16(Some(a)), Int16(Some(b))) => Ok(Int16(Some(a.wrapping_div(b)))),
            (Int32(Some(a)), Int32(Some(b))) => Ok(Int32(Some(a.wrapping_div(b)))),
            (Int64(Some(a)), Int64(Some(b))) => Ok(Int64(Some(a.wrapping_div(b)))),
            (UInt8(Some(a)), UInt8(Some(b))) => Ok(UInt8(Some(a.wrapping_div(b)))),
            (UInt16(Some(a)), UInt16(Some(b))) => Ok(UInt16(Some(a.wrapping_div(b)))),
            (UInt32(Some(a)), UInt32(Some(b))) => Ok(UInt32(Some(a.wrapping_div(b)))),
            (UInt64(Some(a)), UInt64(Some(b))) => Ok(UInt64(Some(a.wrapping_div(b)))),
            (Float32(Some(a)), Float32(Some(b))) => Ok(Float32(Some(a / b))),
            (Float64(Some(a)), Float64(Some(b))) => Ok(Float64(Some(a / b))),
            (Int8(None), _)
            | (Int16(None), _)
            | (Int32(None), _)
            | (Int64(None), _)
            | (UInt8(None), _)
            | (UInt16(None), _)
            | (UInt32(None), _)
            | (UInt64(None), _)
            | (Float32(None), _)
            | (Float64(None), _) => Ok(self.clone()),
            _ => Err(QuillSQLError::Execution(format!(
                "Unsupported division between {:?} and {:?}",
                self, other
            ))),
        }
    }

    pub fn from_string(string: &String, data_type: DataType) -> QuillSQLResult<Self> {
        let is_null = string.eq_ignore_ascii_case("null");
        match data_type {
            DataType::Boolean => {
                let v = if is_null {
                    None
                } else {
                    let v = string
                        .parse::<bool>()
                        .map_err(|_| QuillSQLError::Internal("Parse bool failed".to_string()))?;
                    Some(v)
                };
                Ok(ScalarValue::Boolean(v))
            }
            DataType::Int8 => {
                let v = if is_null {
                    None
                } else {
                    let v = string
                        .parse::<i8>()
                        .map_err(|_| QuillSQLError::Internal("Parse i8 failed".to_string()))?;
                    Some(v)
                };
                Ok(ScalarValue::Int8(v))
            }
            DataType::Int16 => {
                let v = if is_null {
                    None
                } else {
                    let v = string
                        .parse::<i16>()
                        .map_err(|_| QuillSQLError::Internal("Parse i16 failed".to_string()))?;
                    Some(v)
                };
                Ok(ScalarValue::Int16(v))
            }
            DataType::Int32 => {
                let v = if is_null {
                    None
                } else {
                    let v = string
                        .parse::<i32>()
                        .map_err(|_| QuillSQLError::Internal("Parse i32 failed".to_string()))?;
                    Some(v)
                };
                Ok(ScalarValue::Int32(v))
            }
            DataType::Int64 => {
                let v = if is_null {
                    None
                } else {
                    let v = string
                        .parse::<i64>()
                        .map_err(|_| QuillSQLError::Internal("Parse i64 failed".to_string()))?;
                    Some(v)
                };
                Ok(ScalarValue::Int64(v))
            }
            DataType::UInt8 => {
                let v = if is_null {
                    None
                } else {
                    let v = string
                        .parse::<u8>()
                        .map_err(|_| QuillSQLError::Internal("Parse u8 failed".to_string()))?;
                    Some(v)
                };
                Ok(ScalarValue::UInt8(v))
            }
            DataType::UInt16 => {
                let v = if is_null {
                    None
                } else {
                    let v = string
                        .parse::<u16>()
                        .map_err(|_| QuillSQLError::Internal("Parse u16 failed".to_string()))?;
                    Some(v)
                };
                Ok(ScalarValue::UInt16(v))
            }
            DataType::UInt32 => {
                let v = if is_null {
                    None
                } else {
                    let v = string
                        .parse::<u32>()
                        .map_err(|_| QuillSQLError::Internal("Parse u32 failed".to_string()))?;
                    Some(v)
                };
                Ok(ScalarValue::UInt32(v))
            }
            DataType::UInt64 => {
                let v = if is_null {
                    None
                } else {
                    let v = string
                        .parse::<u64>()
                        .map_err(|_| QuillSQLError::Internal("Parse u64 failed".to_string()))?;
                    Some(v)
                };
                Ok(ScalarValue::UInt64(v))
            }
            DataType::Float32 => {
                let v = if is_null {
                    None
                } else {
                    let v = string
                        .parse::<f32>()
                        .map_err(|_| QuillSQLError::Internal("Parse f32 failed".to_string()))?;
                    Some(v)
                };
                Ok(ScalarValue::Float32(v))
            }
            DataType::Float64 => {
                let v = if is_null {
                    None
                } else {
                    let v = string
                        .parse::<f64>()
                        .map_err(|_| QuillSQLError::Internal("Parse f64 failed".to_string()))?;
                    Some(v)
                };
                Ok(ScalarValue::Float64(v))
            }
            DataType::Varchar(_) => {
                let v = if is_null { None } else { Some(string.clone()) };
                Ok(ScalarValue::Varchar(v))
            }
        }
    }
}

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        use ScalarValue::*;
        match (self, other) {
            (Boolean(v1), Boolean(v2)) => v1.eq(v2),
            (Boolean(_), _) => false,
            (Int8(v1), Int8(v2)) => v1.eq(v2),
            (Int8(_), _) => false,
            (Int16(v1), Int16(v2)) => v1.eq(v2),
            (Int16(_), _) => false,
            (Int32(v1), Int32(v2)) => v1.eq(v2),
            (Int32(_), _) => false,
            (Int64(v1), Int64(v2)) => v1.eq(v2),
            (Int64(_), _) => false,
            (UInt8(v1), UInt8(v2)) => v1.eq(v2),
            (UInt8(_), _) => false,
            (UInt16(v1), UInt16(v2)) => v1.eq(v2),
            (UInt16(_), _) => false,
            (UInt32(v1), UInt32(v2)) => v1.eq(v2),
            (UInt32(_), _) => false,
            (UInt64(v1), UInt64(v2)) => v1.eq(v2),
            (UInt64(_), _) => false,
            (Float32(v1), Float32(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => v1.eq(v2),
            },
            (Float32(_), _) => false,
            (Float64(v1), Float64(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => v1.eq(v2),
            },
            (Float64(_), _) => false,
            (Varchar(v1), Varchar(v2)) => v1.eq(v2),
            (Varchar(_), _) => false,
        }
    }
}

impl Eq for ScalarValue {}

impl PartialOrd for ScalarValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use ScalarValue::*;
        match (self, other) {
            (Boolean(v1), Boolean(v2)) => v1.partial_cmp(v2),
            (Boolean(_), _) => None,
            (Int8(v1), Int8(v2)) => v1.partial_cmp(v2),
            (Int8(_), _) => None,
            (Int16(v1), Int16(v2)) => v1.partial_cmp(v2),
            (Int16(_), _) => None,
            (Int32(v1), Int32(v2)) => v1.partial_cmp(v2),
            (Int32(_), _) => None,
            (Int64(v1), Int64(v2)) => v1.partial_cmp(v2),
            (Int64(_), _) => None,
            (UInt8(v1), UInt8(v2)) => v1.partial_cmp(v2),
            (UInt8(_), _) => None,
            (UInt16(v1), UInt16(v2)) => v1.partial_cmp(v2),
            (UInt16(_), _) => None,
            (UInt32(v1), UInt32(v2)) => v1.partial_cmp(v2),
            (UInt32(_), _) => None,
            (UInt64(v1), UInt64(v2)) => v1.partial_cmp(v2),
            (UInt64(_), _) => None,
            (Float32(v1), Float32(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => Some(f1.total_cmp(f2)),
                _ => v1.partial_cmp(v2),
            },
            (Float32(_), _) => None,
            (Float64(v1), Float64(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => Some(f1.total_cmp(f2)),
                _ => v1.partial_cmp(v2),
            },
            (Float64(_), _) => None,
            (Varchar(v1), Varchar(v2)) => v1.partial_cmp(v2),
            (Varchar(_), _) => None,
        }
    }
}

impl std::hash::Hash for ScalarValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use ScalarValue::*;
        match self {
            Boolean(v) => v.hash(state),
            Float32(v) => v.map(Fl).hash(state),
            Float64(v) => v.map(Fl).hash(state),
            Int8(v) => v.hash(state),
            Int16(v) => v.hash(state),
            Int32(v) => v.hash(state),
            Int64(v) => v.hash(state),
            UInt8(v) => v.hash(state),
            UInt16(v) => v.hash(state),
            UInt32(v) => v.hash(state),
            UInt64(v) => v.hash(state),
            Varchar(v) => v.hash(state),
        }
    }
}

//Float wrapper over f32/f64. Just because we cannot build std::hash::Hash for floats directly we have to do it through type wrapper
struct Fl<T>(T);

macro_rules! hash_float_value {
    ($(($t:ty, $i:ty)),+) => {
        $(impl std::hash::Hash for Fl<$t> {
            #[inline]
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                state.write(&<$i>::from_ne_bytes(self.0.to_ne_bytes()).to_ne_bytes())
            }
        })+
    };
}

hash_float_value!((f64, u64), (f32, u32));

impl std::fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ScalarValue::Boolean(None) => write!(f, "NULL"),
            ScalarValue::Boolean(Some(v)) => write!(f, "{v}"),
            ScalarValue::Int8(None) => write!(f, "NULL"),
            ScalarValue::Int8(Some(v)) => write!(f, "{v}"),
            ScalarValue::Int16(None) => write!(f, "NULL"),
            ScalarValue::Int16(Some(v)) => write!(f, "{v}"),
            ScalarValue::Int32(None) => write!(f, "NULL"),
            ScalarValue::Int32(Some(v)) => write!(f, "{v}"),
            ScalarValue::Int64(None) => write!(f, "NULL"),
            ScalarValue::Int64(Some(v)) => write!(f, "{v}"),
            ScalarValue::UInt8(None) => write!(f, "NULL"),
            ScalarValue::UInt8(Some(v)) => write!(f, "{v}"),
            ScalarValue::UInt16(None) => write!(f, "NULL"),
            ScalarValue::UInt16(Some(v)) => write!(f, "{v}"),
            ScalarValue::UInt32(None) => write!(f, "NULL"),
            ScalarValue::UInt32(Some(v)) => write!(f, "{v}"),
            ScalarValue::UInt64(None) => write!(f, "NULL"),
            ScalarValue::UInt64(Some(v)) => write!(f, "{v}"),
            ScalarValue::Float32(None) => write!(f, "NULL"),
            ScalarValue::Float32(Some(v)) => write!(f, "{v}"),
            ScalarValue::Float64(None) => write!(f, "NULL"),
            ScalarValue::Float64(Some(v)) => write!(f, "{v}"),
            ScalarValue::Varchar(None) => write!(f, "NULL"),
            ScalarValue::Varchar(Some(v)) => write!(f, "{v}"),
        }
    }
}

macro_rules! impl_from_for_scalar {
    ($ty:ty, $scalar:tt) => {
        impl From<$ty> for ScalarValue {
            fn from(value: $ty) -> Self {
                ScalarValue::$scalar(Some(value))
            }
        }

        impl From<Option<$ty>> for ScalarValue {
            fn from(value: Option<$ty>) -> Self {
                ScalarValue::$scalar(value)
            }
        }
    };
}

impl_from_for_scalar!(bool, Boolean);
impl_from_for_scalar!(i8, Int8);
impl_from_for_scalar!(i16, Int16);
impl_from_for_scalar!(i32, Int32);
impl_from_for_scalar!(i64, Int64);
impl_from_for_scalar!(u8, UInt8);
impl_from_for_scalar!(u16, UInt16);
impl_from_for_scalar!(u32, UInt32);
impl_from_for_scalar!(u64, UInt64);
impl_from_for_scalar!(f32, Float32);
impl_from_for_scalar!(f64, Float64);
impl_from_for_scalar!(String, Varchar);
