use crate::buffer::PAGE_SIZE;
use crate::catalog::SchemaRef;
use crate::storage::codec::{CommonCodec, DecodedData, RidCodec, TupleCodec};
use crate::storage::b_plus_tree::page::{
    BPlusTreeInternalPage, BPlusTreeInternalPageHeader, BPlusTreeLeafPage, BPlusTreeLeafPageHeader,
    BPlusTreePage, BPlusTreePageType,
};
use crate::error::{QuillSQLError, QuillSQLResult};

pub struct BPlusTreePageCodec;

impl BPlusTreePageCodec {
    pub fn encode(page: &BPlusTreePage) -> Vec<u8> {
        match page {
            BPlusTreePage::Leaf(page) => BPlusTreeLeafPageCodec::encode(page),
            BPlusTreePage::Internal(page) => BPlusTreeInternalPageCodec::encode(page),
        }
    }

    pub fn decode(bytes: &[u8], schema: SchemaRef) -> QuillSQLResult<DecodedData<BPlusTreePage>> {
        if bytes.len() != PAGE_SIZE {
            return Err(QuillSQLError::Storage(format!(
                "Index page size is not {} instead of {}",
                PAGE_SIZE,
                bytes.len()
            )));
        }

        // not consume left_bytes
        let (page_type, _) = BPlusTreePageTypeCodec::decode(bytes)?;

        match page_type {
            BPlusTreePageType::LeafPage => {
                let (page, offset) = BPlusTreeLeafPageCodec::decode(bytes, schema.clone())?;
                Ok((BPlusTreePage::Leaf(page), offset))
            }
            BPlusTreePageType::InternalPage => {
                let (page, offset) = BPlusTreeInternalPageCodec::decode(bytes, schema.clone())?;
                Ok((BPlusTreePage::Internal(page), offset))
            }
        }
    }
}

pub struct BPlusTreeLeafPageCodec;

impl BPlusTreeLeafPageCodec {
    pub fn encode(page: &BPlusTreeLeafPage) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend(BPlusTreeLeafPageHeaderCodec::encode(&page.header));
        for (tuple, rid) in page.array.iter() {
            bytes.extend(TupleCodec::encode(tuple));
            bytes.extend(RidCodec::encode(rid));
        }
        // make sure length of bytes is PAGE_SIZE
        assert!(bytes.len() <= PAGE_SIZE);
        bytes.extend(vec![0; PAGE_SIZE - bytes.len()]);
        bytes
    }

    pub fn decode(
        bytes: &[u8],
        schema: SchemaRef,
    ) -> QuillSQLResult<DecodedData<BPlusTreeLeafPage>> {
        if bytes.len() != PAGE_SIZE {
            return Err(QuillSQLError::Storage(format!(
                "Index page size is not {} instead of {}",
                PAGE_SIZE,
                bytes.len()
            )));
        }
        let mut left_bytes = bytes;

        // not consume left_bytes
        let (page_type, _) = BPlusTreePageTypeCodec::decode(left_bytes)?;

        if matches!(page_type, BPlusTreePageType::LeafPage) {
            let (header, offset) = BPlusTreeLeafPageHeaderCodec::decode(left_bytes)?;
            left_bytes = &left_bytes[offset..];

            let mut array = vec![];
            for _ in 0..header.current_size {
                let (tuple, offset) = TupleCodec::decode(left_bytes, schema.clone())?;
                left_bytes = &left_bytes[offset..];

                let (rid, offset) = RidCodec::decode(left_bytes)?;
                left_bytes = &left_bytes[offset..];

                array.push((tuple, rid));
            }

            Ok((
                BPlusTreeLeafPage {
                    schema,
                    header,
                    array,
                },
                PAGE_SIZE,
            ))
        } else {
            Err(QuillSQLError::Storage(
                "Index page type must be leaf page".to_string(),
            ))
        }
    }
}

pub struct BPlusTreeInternalPageCodec;

impl BPlusTreeInternalPageCodec {
    pub fn encode(page: &BPlusTreeInternalPage) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend(BPlusTreeInternalPageHeaderCodec::encode(&page.header));
        for (tuple, page_id) in page.array.iter() {
            bytes.extend(TupleCodec::encode(tuple));
            bytes.extend(CommonCodec::encode_u32(*page_id));
        }
        // make sure length of bytes is PAGE_SIZE
        assert!(bytes.len() <= PAGE_SIZE);
        bytes.extend(vec![0; PAGE_SIZE - bytes.len()]);
        bytes
    }

    pub fn decode(
        bytes: &[u8],
        schema: SchemaRef,
    ) -> QuillSQLResult<DecodedData<BPlusTreeInternalPage>> {
        if bytes.len() != PAGE_SIZE {
            return Err(QuillSQLError::Storage(format!(
                "Index page size is not {} instead of {}",
                PAGE_SIZE,
                bytes.len()
            )));
        }
        let mut left_bytes = bytes;

        // not consume left_bytes
        let (page_type, _) = BPlusTreePageTypeCodec::decode(left_bytes)?;

        if matches!(page_type, BPlusTreePageType::InternalPage) {
            let (header, offset) = BPlusTreeInternalPageHeaderCodec::decode(left_bytes)?;
            left_bytes = &left_bytes[offset..];

            let mut array = vec![];
            for _ in 0..header.current_size {
                let (tuple, offset) = TupleCodec::decode(left_bytes, schema.clone())?;
                left_bytes = &left_bytes[offset..];

                let (page_id, offset) = CommonCodec::decode_u32(left_bytes)?;
                left_bytes = &left_bytes[offset..];

                array.push((tuple, page_id));
            }

            Ok((
                BPlusTreeInternalPage {
                    schema,
                    header,
                    array,
                },
                PAGE_SIZE,
            ))
        } else {
            Err(QuillSQLError::Storage(
                "Index page type must be internal page".to_string(),
            ))
        }
    }
}

pub struct BPlusTreePageTypeCodec;

impl BPlusTreePageTypeCodec {
    pub fn encode(page_type: &BPlusTreePageType) -> Vec<u8> {
        match page_type {
            BPlusTreePageType::LeafPage => CommonCodec::encode_u8(1),
            BPlusTreePageType::InternalPage => CommonCodec::encode_u8(2),
        }
    }

    pub fn decode(bytes: &[u8]) -> QuillSQLResult<DecodedData<BPlusTreePageType>> {
        let (flag, offset) = CommonCodec::decode_u8(bytes)?;
        match flag {
            1 => Ok((BPlusTreePageType::LeafPage, offset)),
            2 => Ok((BPlusTreePageType::InternalPage, offset)),
            _ => Err(QuillSQLError::Storage(format!("Invalid page type {}", flag))),
        }
    }
}

pub struct BPlusTreeLeafPageHeaderCodec;

impl BPlusTreeLeafPageHeaderCodec {
    pub fn encode(header: &BPlusTreeLeafPageHeader) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(BPlusTreePageTypeCodec::encode(&header.page_type));
        bytes.extend(CommonCodec::encode_u32(header.current_size));
        bytes.extend(CommonCodec::encode_u32(header.max_size));
        bytes.extend(CommonCodec::encode_u32(header.next_page_id));
        bytes
    }

    pub fn decode(bytes: &[u8]) -> QuillSQLResult<DecodedData<BPlusTreeLeafPageHeader>> {
        let mut left_bytes = bytes;

        let (page_type, offset) = BPlusTreePageTypeCodec::decode(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let (current_size, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let (max_size, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let (next_page_id, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        Ok((
            BPlusTreeLeafPageHeader {
                page_type,
                current_size,
                max_size,
                next_page_id,
            },
            bytes.len() - left_bytes.len(),
        ))
    }
}

pub struct BPlusTreeInternalPageHeaderCodec;

impl BPlusTreeInternalPageHeaderCodec {
    pub fn encode(header: &BPlusTreeInternalPageHeader) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(BPlusTreePageTypeCodec::encode(&header.page_type));
        bytes.extend(CommonCodec::encode_u32(header.current_size));
        bytes.extend(CommonCodec::encode_u32(header.max_size));
        bytes
    }

    pub fn decode(bytes: &[u8]) -> QuillSQLResult<DecodedData<BPlusTreeInternalPageHeader>> {
        let mut left_bytes = bytes;

        let (page_type, offset) = BPlusTreePageTypeCodec::decode(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let (current_size, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let (max_size, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        Ok((
            BPlusTreeInternalPageHeader {
                page_type,
                current_size,
                max_size,
            },
            bytes.len() - left_bytes.len(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{Column, DataType, Schema};
    use crate::storage::codec::index_page::BPlusTreePageCodec;
    use crate::storage::b_plus_tree::page::{
        BPlusTreeInternalPage, BPlusTreeLeafPage, BPlusTreePage,
    };
    use crate::storage::tuple::Tuple;
    use crate::storage::b_plus_tree::page::RecordId;
    use std::sync::Arc;

    #[test]
    fn index_page_codec() {
        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, true),
            Column::new("b", DataType::Int32, true),
        ]));
        let tuple1 = Tuple::new(schema.clone(), vec![1i8.into(), 1i32.into()]);
        let rid1 = RecordId::new(1, 1);
        let tuple2 = Tuple::new(schema.clone(), vec![2i8.into(), 2i32.into()]);
        let rid2 = RecordId::new(2, 2);

        let mut leaf_page = BPlusTreeLeafPage::new(schema.clone(), 100);
        leaf_page.insert(tuple1.clone(), rid1);
        leaf_page.insert(tuple2.clone(), rid2);
        let page = BPlusTreePage::Leaf(leaf_page);
        let (new_page, _) =
            BPlusTreePageCodec::decode(&BPlusTreePageCodec::encode(&page), schema.clone()).unwrap();
        assert_eq!(new_page, page);

        let mut internal_page = BPlusTreeInternalPage::new(schema.clone(), 100);
        internal_page.insert(Tuple::empty(schema.clone()), 1);
        internal_page.insert(tuple1, 2);
        internal_page.insert(tuple2, 3);
        let page = BPlusTreePage::Internal(internal_page);
        let (new_page, _) =
            BPlusTreePageCodec::decode(&BPlusTreePageCodec::encode(&page), schema.clone()).unwrap();
        assert_eq!(new_page, page);
    }
}
