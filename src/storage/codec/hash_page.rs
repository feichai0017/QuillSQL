use crate::buffer::PAGE_SIZE;
use crate::catalog::SchemaRef;
use crate::storage::codec::{CommonCodec, DecodedData, TupleCodec};
use crate::storage::page::hash_page::{HashBucketPage, HashBucketPageHeader};
use crate::storage::page::RecordId;

pub struct HashBucketPageCodec;

impl HashBucketPageCodec {
    pub fn encode(page: &HashBucketPage) -> Vec<u8> {
        let mut bytes = Vec::new();
        // header
        bytes.extend(CommonCodec::encode_u32(page.header.current_size));
        bytes.extend(CommonCodec::encode_u32(page.header.max_size));
        bytes.extend(CommonCodec::encode_u32(page.header.local_depth));
        // array
        for (tuple, rid) in page.array.iter() {
            bytes.extend(TupleCodec::encode(tuple));
            bytes.extend(CommonCodec::encode_u32(rid.page_id));
            bytes.extend(CommonCodec::encode_u32(rid.slot_num));
        }
        assert!(bytes.len() <= PAGE_SIZE);
        bytes.resize(PAGE_SIZE, 0);
        bytes
    }

    pub fn decode(
        bytes: &[u8],
        schema: SchemaRef,
    ) -> crate::error::QuillSQLResult<DecodedData<HashBucketPage>> {
        assert_eq!(bytes.len(), PAGE_SIZE);
        let mut left = bytes;

        let (current_size, off) = CommonCodec::decode_u32(left)?;
        left = &left[off..];
        let (max_size, off) = CommonCodec::decode_u32(left)?;
        left = &left[off..];
        let (local_depth, off) = CommonCodec::decode_u32(left)?;
        left = &left[off..];

        let mut array = Vec::new();
        for _ in 0..current_size {
            let (tuple, off) = TupleCodec::decode(left, schema.clone())?;
            left = &left[off..];
            let (page_id, off) = CommonCodec::decode_u32(left)?;
            left = &left[off..];
            let (slot_num, off) = CommonCodec::decode_u32(left)?;
            left = &left[off..];
            array.push((tuple, RecordId::new(page_id, slot_num)));
        }

        Ok((
            HashBucketPage {
                schema,
                header: HashBucketPageHeader {
                    current_size,
                    max_size,
                    local_depth,
                },
                array,
            },
            PAGE_SIZE,
        ))
    }
}
