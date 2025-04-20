use crate::error::{Error, Result};
use crate::sql::types::{DataType, Value};
use crate::storage::b_plus_tree::codec::{CommonCodec, DecodedData};

pub struct ScalarValueCodec;

impl ScalarValueCodec {
    pub fn encode(value: &Value) -> Vec<u8> {
        match value {
            Value::Null => vec![],
            Value::Boolean(v) => CommonCodec::encode_bool(*v),
            Value::Integer(v) => CommonCodec::encode_i64(*v),
            Value::Float(v) => CommonCodec::encode_f64(*v),
            Value::String(v) => {
                if v.len() > u16::MAX as usize {
                    panic!("String length ({}) exceeds u16::MAX", v.len());
                }
                let mut bytes = Vec::with_capacity(2 + v.len());
                bytes.extend_from_slice(&(v.len() as u16).to_be_bytes());
                bytes.extend_from_slice(v.as_bytes());
                bytes
            }
        }
    }

    pub fn decode(bytes: &[u8], data_type: DataType) -> Result<DecodedData<Value>> {
        if bytes.is_empty() {
            return Ok((Value::Null, 0));
        }

        match data_type {
            DataType::Boolean => {
                let (value, offset) = CommonCodec::decode_bool(bytes)?;
                Ok((Value::Boolean(value), offset))
            }
            DataType::Integer => {
                let (value, offset) = CommonCodec::decode_i64(bytes)?;
                Ok((Value::Integer(value), offset))
            }
            DataType::Float => {
                let (value, offset) = CommonCodec::decode_f64(bytes)?;
                Ok((Value::Float(value), offset))
            }
            DataType::String => {
                let mut left_bytes = bytes;

                let (length, offset) = CommonCodec::decode_u16(left_bytes)?;
                left_bytes = &left_bytes[offset..];

                if left_bytes.len() < length as usize {
                    return Err(Error::Internal(format!(
                        "Insufficient bytes for string: expected {}, got {}",
                        length,
                        left_bytes.len()
                    )));
                }

                let (value, offset) = CommonCodec::decode_string(&left_bytes[0..length as usize])?;
                let total_consumed = 2 + length as usize;

                Ok((Value::String(value), total_consumed))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;

    use super::*;

    mod common_codec_mock {
        use crate::error::{Error, Result};
        use std::convert::TryInto;

        pub struct CommonCodec;
        impl CommonCodec {
            pub fn encode_bool(v: bool) -> Vec<u8> {
                vec![if v { 1 } else { 0 }]
            }
            pub fn decode_bool(b: &[u8]) -> Result<(bool, usize)> {
                if b.is_empty() {
                    Err(Error::PageLayout("empty bool".into()))
                } else {
                    Ok((b[0] == 1, 1))
                }
            }
            pub fn encode_i64(v: i64) -> Vec<u8> {
                v.to_be_bytes().to_vec()
            }
            pub fn decode_i64(b: &[u8]) -> Result<(i64, usize)> {
                if b.len() < 8 {
                    Err(Error::PageLayout("empty i64".into()))
                } else {
                    Ok((i64::from_be_bytes(b[0..8].try_into().unwrap()), 8))
                }
            }
            pub fn encode_f64(v: f64) -> Vec<u8> {
                v.to_be_bytes().to_vec()
            }
            pub fn decode_f64(b: &[u8]) -> Result<(f64, usize)> {
                if b.len() < 8 {
                    Err(Error::PageLayout("empty f64".into()))
                } else {
                    Ok((f64::from_be_bytes(b[0..8].try_into().unwrap()), 8))
                }
            }
        }
    }

    #[test]
    fn test_scalar_codec_null() {
        let value = Value::Null;
        let bytes = ScalarValueCodec::encode(&value);
        assert!(bytes.is_empty());
        assert_eq!(
            ScalarValueCodec::decode(&bytes, DataType::Integer).unwrap(),
            (Value::Null, 0)
        );
        assert_eq!(
            ScalarValueCodec::decode(&bytes, DataType::String).unwrap(),
            (Value::Null, 0)
        );
    }

    #[test]
    fn test_scalar_codec_boolean() -> Result<()> {
        let val_true = Value::Boolean(true);
        let bytes_true = ScalarValueCodec::encode(&val_true);
        assert_eq!(bytes_true, vec![1]);
        assert_eq!(
            ScalarValueCodec::decode(&bytes_true, DataType::Boolean)?,
            (val_true, 1)
        );

        let val_false = Value::Boolean(false);
        let bytes_false = ScalarValueCodec::encode(&val_false);
        assert_eq!(bytes_false, vec![0]);
        assert_eq!(
            ScalarValueCodec::decode(&bytes_false, DataType::Boolean)?,
            (val_false, 1)
        );
        Ok(())
    }

    #[test]
    fn test_scalar_codec_integer() -> Result<()> {
        let value = Value::Integer(1234567890123456789_i64);
        let bytes = ScalarValueCodec::encode(&value);
        assert_eq!(bytes.len(), 8);
        assert_eq!(
            ScalarValueCodec::decode(&bytes, DataType::Integer)?,
            (value, 8)
        );
        Ok(())
    }

    #[test]
    fn test_scalar_codec_float() -> Result<()> {
        let value = Value::Float(std::f64::consts::PI);
        let bytes = ScalarValueCodec::encode(&value);
        assert_eq!(bytes.len(), 8);
        assert_eq!(
            ScalarValueCodec::decode(&bytes, DataType::Float)?,
            (value, 8)
        );
        Ok(())
    }

    #[test]
    fn test_scalar_codec_string() -> Result<()> {
        let value = Value::String("Hello, 世界".to_string());
        let bytes = ScalarValueCodec::encode(&value);
        assert_eq!(bytes.len(), 2 + "Hello, 世界".len());
        assert_eq!(
            ScalarValueCodec::decode(&bytes, DataType::String)?,
            (value, bytes.len())
        );

        let empty_str = Value::String("".to_string());
        let empty_bytes = ScalarValueCodec::encode(&empty_str);
        assert_eq!(empty_bytes.len(), 2);
        assert_eq!(
            ScalarValueCodec::decode(&empty_bytes, DataType::String)?,
            (empty_str, 2)
        );
        Ok(())
    }

    #[test]
    fn test_scalar_decode_errors() {
        assert!(matches!(
            ScalarValueCodec::decode(&[], DataType::Integer),
            Ok((Value::Null, 0))
        ));
        assert!(matches!(
            ScalarValueCodec::decode(&[0], DataType::Boolean),
            Ok((Value::Boolean(false), 1))
        ));
        assert!(matches!(
            ScalarValueCodec::decode(&[1, 2, 3], DataType::Integer),
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            ScalarValueCodec::decode(&[1, 2, 3], DataType::Float),
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            ScalarValueCodec::decode(&[0], DataType::String),
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            ScalarValueCodec::decode(&[0, 5, b'a', b'b'], DataType::String),
            Err(Error::Internal(_))
        ));
    }
}
