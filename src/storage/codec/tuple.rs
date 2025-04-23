use crate::catalog::SchemaRef;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::codec::{DecodedData, ScalarValueCodec};
use crate::storage::tuple::Tuple;
use crate::utils::bitmap::DynamicBitmap;
use crate::utils::scalar::ScalarValue;

pub struct TupleCodec;

impl TupleCodec {
    pub fn encode(tuple: &Tuple) -> Vec<u8> {
        // null map
        let mut null_map = DynamicBitmap::new();
        let mut attributes = Vec::new();
        for (idx, value) in tuple.data.iter().enumerate() {
            null_map.set(idx, value.is_null());
            if !value.is_null() {
                attributes.extend(ScalarValueCodec::encode(value));
            }
        }

        let mut bytes = null_map.to_bytes();
        bytes.extend(attributes);
        bytes
    }

    pub fn decode(bytes: &[u8], schema: SchemaRef) -> QuillSQLResult<DecodedData<Tuple>> {
        let mut total_offset = 0;

        let null_map_bytes = schema.column_count().div_ceil(8);
        let null_map = DynamicBitmap::from_bytes(&bytes[0..null_map_bytes]);
        total_offset += null_map_bytes;
        let mut bytes = &bytes[null_map_bytes..];

        let mut data = vec![];
        for (idx, col) in schema.columns.iter().enumerate() {
            let null = null_map.get(idx).ok_or(QuillSQLError::Internal(
                "null map size should be greater than or equal to col count".to_string(),
            ))?;
            if null {
                data.push(ScalarValue::new_empty(col.data_type));
            } else {
                let (value, offset) = ScalarValueCodec::decode(bytes, col.data_type)?;
                data.push(value);
                total_offset += offset;
                bytes = &bytes[offset..];
            }
        }

        Ok((Tuple::new(schema, data), total_offset))
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{Column, DataType, Schema};
    use crate::utils::scalar::ScalarValue;
    use crate::storage::codec::TupleCodec;
    use crate::storage::tuple::Tuple;
    use std::sync::Arc;

    #[test]
    fn tuple_codec() {
        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Boolean, true),
            Column::new("b", DataType::Int32, true),
            Column::new("c", DataType::UInt64, true),
            Column::new("d", DataType::Varchar(None), true),
        ]));
        let tuple = Tuple::new(
            schema.clone(),
            vec![
                true.into(),
                ScalarValue::Int32(None),
                1234u64.into(),
                "aabb".to_string().into(),
            ],
        );
        let new_tuple = TupleCodec::decode(&TupleCodec::encode(&tuple), schema)
            .unwrap()
            .0;
        assert_eq!(new_tuple, tuple);
    }
}
