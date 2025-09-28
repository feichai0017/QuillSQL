use crate::buffer::PAGE_SIZE;
use crate::catalog::SchemaRef;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::codec::{CommonCodec, DecodedData};
use crate::storage::page::{RecordId, TablePage, TablePageHeader, TupleInfo, TupleMeta};
use crate::utils::util::page_bytes_to_array;

pub struct TablePageCodec;

impl TablePageCodec {
    pub fn encode(page: &TablePage) -> Vec<u8> {
        let header_bytes = TablePageHeaderCodec::encode(&page.header);
        let mut all_bytes = page.data;
        all_bytes[0..header_bytes.len()].copy_from_slice(&header_bytes);
        all_bytes.to_vec()
    }

    pub fn decode(bytes: &[u8], schema: SchemaRef) -> QuillSQLResult<DecodedData<TablePage>> {
        if bytes.len() != PAGE_SIZE {
            return Err(QuillSQLError::Storage(format!(
                "Table page size is not {} instead of {}",
                PAGE_SIZE,
                bytes.len()
            )));
        }
        let (header, _) = TablePageHeaderCodec::decode(bytes)?;
        Ok((
            TablePage {
                schema,
                header,
                data: page_bytes_to_array(&bytes[0..PAGE_SIZE]),
            },
            PAGE_SIZE,
        ))
    }
}

pub struct TablePageHeaderCodec;

impl TablePageHeaderCodec {
    pub fn encode(header: &TablePageHeader) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(CommonCodec::encode_u64(header.lsn));
        bytes.extend(CommonCodec::encode_u32(header.next_page_id));
        bytes.extend(CommonCodec::encode_u16(header.num_tuples));
        bytes.extend(CommonCodec::encode_u16(header.num_deleted_tuples));
        for tuple_info in header.tuple_infos.iter() {
            bytes.extend(TablePageHeaderTupleInfoCodec::encode(tuple_info));
        }
        bytes
    }

    pub fn decode(bytes: &[u8]) -> QuillSQLResult<DecodedData<TablePageHeader>> {
        let mut left_bytes = bytes;

        let (lsn, offset) = CommonCodec::decode_u64(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let (next_page_id, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let (num_tuples, offset) = CommonCodec::decode_u16(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let (num_deleted_tuples, offset) = CommonCodec::decode_u16(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let mut tuple_infos = vec![];
        for _ in 0..num_tuples {
            let (tuple_info, offset) = TablePageHeaderTupleInfoCodec::decode(left_bytes)?;
            left_bytes = &left_bytes[offset..];
            tuple_infos.push(tuple_info);
        }
        Ok((
            TablePageHeader {
                lsn,
                next_page_id,
                num_tuples,
                num_deleted_tuples,
                tuple_infos,
            },
            bytes.len() - left_bytes.len(),
        ))
    }
}

pub struct TablePageHeaderTupleInfoCodec;

impl TablePageHeaderTupleInfoCodec {
    pub fn encode(tuple_info: &TupleInfo) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(CommonCodec::encode_u16(tuple_info.offset));
        bytes.extend(CommonCodec::encode_u16(tuple_info.size));
        bytes.extend(CommonCodec::encode_u64(tuple_info.meta.insert_txn_id));
        bytes.extend(CommonCodec::encode_u64(tuple_info.meta.delete_txn_id));
        bytes.extend(CommonCodec::encode_bool(tuple_info.meta.is_deleted));
        bytes
    }

    pub fn decode(bytes: &[u8]) -> QuillSQLResult<DecodedData<TupleInfo>> {
        let mut left_bytes = bytes;
        let (tuple_offset, offset) = CommonCodec::decode_u16(left_bytes)?;
        left_bytes = &left_bytes[offset..];
        let (size, offset) = CommonCodec::decode_u16(left_bytes)?;
        left_bytes = &left_bytes[offset..];
        let (insert_txn_id, offset) = CommonCodec::decode_u64(left_bytes)?;
        left_bytes = &left_bytes[offset..];
        let (delete_txn_id, offset) = CommonCodec::decode_u64(left_bytes)?;
        left_bytes = &left_bytes[offset..];
        let (is_deleted, offset) = CommonCodec::decode_bool(left_bytes)?;
        left_bytes = &left_bytes[offset..];
        Ok((
            TupleInfo {
                offset: tuple_offset,
                size,
                meta: TupleMeta {
                    insert_txn_id,
                    delete_txn_id,
                    is_deleted,
                },
            },
            bytes.len() - left_bytes.len(),
        ))
    }
}

pub struct RidCodec;

impl RidCodec {
    pub fn encode(rid: &RecordId) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend(CommonCodec::encode_u32(rid.page_id));
        bytes.extend(CommonCodec::encode_u32(rid.slot_num));
        bytes
    }

    pub fn decode(bytes: &[u8]) -> QuillSQLResult<DecodedData<RecordId>> {
        let mut left_bytes = bytes;

        let (page_id, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let (slot_num, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        Ok((
            RecordId::new(page_id, slot_num),
            bytes.len() - left_bytes.len(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::INVALID_PAGE_ID;
    use crate::catalog::{Column, DataType, Schema};
    use crate::storage::codec::table_page::TablePageHeaderCodec;
    use crate::storage::codec::TablePageCodec;
    use crate::storage::page::{TablePage, TupleMeta};
    use crate::storage::tuple::Tuple;
    use std::sync::Arc;

    #[test]
    fn table_page_codec() {
        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, true),
            Column::new("b", DataType::Int32, true),
        ]));
        let tuple1 = Tuple::new(schema.clone(), vec![1i8.into(), 1i32.into()]);
        let tuple1_meta = TupleMeta {
            insert_txn_id: 1,
            delete_txn_id: 2,
            is_deleted: false,
        };
        let tuple2 = Tuple::new(schema.clone(), vec![2i8.into(), 2i32.into()]);
        let tuple2_meta = TupleMeta {
            insert_txn_id: 3,
            delete_txn_id: 4,
            is_deleted: true,
        };

        let mut table_page = TablePage::new(schema.clone(), INVALID_PAGE_ID);
        table_page.set_lsn(42);
        table_page.insert_tuple(&tuple1_meta, &tuple1).unwrap();
        table_page.insert_tuple(&tuple2_meta, &tuple2).unwrap();

        let (new_page, _) =
            TablePageCodec::decode(&TablePageCodec::encode(&table_page), schema.clone()).unwrap();
        assert_eq!(new_page.schema, table_page.schema);
        assert_eq!(new_page.header, table_page.header);
        let header_size = TablePageHeaderCodec::encode(&table_page.header).len();
        assert_eq!(new_page.data[header_size..], table_page.data[header_size..]);
    }
}
