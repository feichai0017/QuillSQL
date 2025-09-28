use crate::error::QuillSQLError;
use crate::error::QuillSQLResult;
use sqlparser::dialect::PostgreSqlDialect;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Varchar(Option<usize>),
}

impl DataType {
    /// Coerce `lhs_type` and `rhs_type` to a common type for the purposes of a comparison operation
    /// where one both are numeric
    pub fn comparison_numeric_coercion(l: &DataType, r: &DataType) -> QuillSQLResult<DataType> {
        use super::DataType::*;
        if l == r {
            return Ok(*l);
        }
        match (l, r) {
            (Float64, _) | (_, Float64) => Ok(Float64),
            (_, Float32) | (Float32, _) => Ok(Float32),
            // The following match arms encode the following logic: Given the two
            // integral types, we choose the narrowest possible integral type that
            // accommodates all values of both types. Note that some information
            // loss is inevitable when we have a signed type and a `UInt64`, in
            // which case we use `Int64`;i.e. the widest signed integral type.
            (Int64, _)
            | (_, Int64)
            | (UInt64, Int8)
            | (Int8, UInt64)
            | (UInt64, Int16)
            | (Int16, UInt64)
            | (UInt64, Int32)
            | (Int32, UInt64)
            | (UInt32, Int8)
            | (Int8, UInt32)
            | (UInt32, Int16)
            | (Int16, UInt32)
            | (UInt32, Int32)
            | (Int32, UInt32) => Ok(Int64),
            (UInt64, _) | (_, UInt64) => Ok(UInt64),
            (Int32, _)
            | (_, Int32)
            | (UInt16, Int16)
            | (Int16, UInt16)
            | (UInt16, Int8)
            | (Int8, UInt16) => Ok(Int32),
            (UInt32, _) | (_, UInt32) => Ok(UInt32),
            (Int16, _) | (_, Int16) | (Int8, UInt8) | (UInt8, Int8) => Ok(Int16),
            (UInt16, _) | (_, UInt16) => Ok(UInt16),
            (Int8, _) | (_, Int8) => Ok(Int8),
            (UInt8, _) | (_, UInt8) => Ok(UInt8),
            _ => Err(QuillSQLError::Internal(format!(
                "Cannot coerce {} and {} for comparison",
                l, r
            ))),
        }
    }
}

impl TryFrom<&sqlparser::ast::DataType> for DataType {
    type Error = QuillSQLError;

    fn try_from(value: &sqlparser::ast::DataType) -> Result<Self, Self::Error> {
        match value {
            sqlparser::ast::DataType::Boolean => Ok(DataType::Boolean),
            sqlparser::ast::DataType::TinyInt(_) => Ok(DataType::Int8),
            sqlparser::ast::DataType::SmallInt(_) => Ok(DataType::Int16),
            sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => {
                Ok(DataType::Int32)
            }
            sqlparser::ast::DataType::BigInt(_) => Ok(DataType::Int64),
            sqlparser::ast::DataType::UnsignedTinyInt(_) => Ok(DataType::UInt8),
            sqlparser::ast::DataType::UnsignedSmallInt(_) => Ok(DataType::UInt16),
            sqlparser::ast::DataType::UnsignedInt(_)
            | sqlparser::ast::DataType::UnsignedInteger(_) => Ok(DataType::UInt32),
            sqlparser::ast::DataType::UnsignedBigInt(_) => Ok(DataType::UInt64),
            sqlparser::ast::DataType::Float(_) => Ok(DataType::Float32),
            sqlparser::ast::DataType::Double => Ok(DataType::Float64),
            // Accept common custom synonyms like INT32/INT64/UINT32/FLOAT64, etc.
            sqlparser::ast::DataType::Custom(object_name, _) => {
                let name = object_name.to_string().to_ascii_lowercase();
                let dt = match name.as_str() {
                    "bool" | "boolean" => DataType::Boolean,
                    "int8" | "tinyint" => DataType::Int8,
                    "int16" | "smallint" => DataType::Int16,
                    "int32" | "int" | "integer" => DataType::Int32,
                    "int64" | "bigint" => DataType::Int64,
                    "uint8" => DataType::UInt8,
                    "uint16" => DataType::UInt16,
                    "uint32" => DataType::UInt32,
                    "uint64" => DataType::UInt64,
                    "float32" | "float" => DataType::Float32,
                    "float64" | "double" => DataType::Float64,
                    // allow bare varchar without length here
                    "varchar" => DataType::Varchar(None),
                    _ => {
                        return Err(QuillSQLError::NotSupport(format!(
                            "Not support datatype {}",
                            value
                        )))
                    }
                };
                Ok(dt)
            }
            sqlparser::ast::DataType::Varchar(len) => {
                Ok(DataType::Varchar(len.map(|l| l.length as usize)))
            }
            sqlparser::ast::DataType::CharVarying(len) => {
                Ok(DataType::Varchar(len.map(|l| l.length as usize)))
            }
            sqlparser::ast::DataType::CharacterVarying(len) => {
                Ok(DataType::Varchar(len.map(|l| l.length as usize)))
            }
            _ => Err(QuillSQLError::NotSupport(format!(
                "Not support datatype {}",
                value
            ))),
        }
    }
}

impl From<&DataType> for sqlparser::ast::DataType {
    fn from(value: &DataType) -> Self {
        match value {
            DataType::Boolean => sqlparser::ast::DataType::Boolean,
            DataType::Int8 => sqlparser::ast::DataType::TinyInt(None),
            DataType::Int16 => sqlparser::ast::DataType::SmallInt(None),
            DataType::Int32 => sqlparser::ast::DataType::Integer(None),
            DataType::Int64 => sqlparser::ast::DataType::BigInt(None),
            DataType::UInt8 => sqlparser::ast::DataType::UnsignedTinyInt(None),
            DataType::UInt16 => sqlparser::ast::DataType::UnsignedSmallInt(None),
            DataType::UInt32 => sqlparser::ast::DataType::UnsignedInteger(None),
            DataType::UInt64 => sqlparser::ast::DataType::UnsignedBigInt(None),
            DataType::Float32 => sqlparser::ast::DataType::Float(None),
            DataType::Float64 => sqlparser::ast::DataType::Double,
            DataType::Varchar(len) => {
                sqlparser::ast::DataType::Varchar(len.map(|l| sqlparser::ast::CharacterLength {
                    length: l as u64,
                    unit: None,
                }))
            }
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64 => write!(f, "{self:?}"),
            DataType::Varchar(len_opt) => {
                if let Some(len) = len_opt {
                    write!(f, "Varchar({len})")
                } else {
                    write!(f, "Varchar")
                }
            }
        }
    }
}

impl TryFrom<&str> for DataType {
    type Error = QuillSQLError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut parser =
            sqlparser::parser::Parser::new(&PostgreSqlDialect {}).try_with_sql(value)?;
        let sql_data_type = parser.parse_data_type()?;
        (&sql_data_type).try_into()
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::DataType;

    #[test]
    fn parse_data_type() {
        let sql_type: sqlparser::ast::DataType = (&DataType::Int32).into();
        assert_eq!(
            DataType::try_from(format!("{sql_type}").as_str()).unwrap(),
            DataType::Int32
        );

        let sql_type: sqlparser::ast::DataType = (&DataType::Varchar(Some(100))).into();
        assert_eq!(
            DataType::try_from(format!("{sql_type}").as_str()).unwrap(),
            DataType::Varchar(Some(100))
        );
    }
}
