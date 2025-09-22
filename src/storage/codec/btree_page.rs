use crate::buffer::PAGE_SIZE;
use crate::catalog::SchemaRef;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::codec::{CommonCodec, DecodedData, RidCodec, TupleCodec};
use crate::storage::page::RecordId;
use crate::storage::page::{
    BPlusTreeHeaderPage, BPlusTreeInternalPage, BPlusTreeInternalPageHeader, BPlusTreeLeafPage,
    BPlusTreeLeafPageHeader, BPlusTreePage, BPlusTreePageType,
};
use crate::storage::tuple::Tuple;

pub struct BPlusTreeHeaderPageCodec;

impl BPlusTreeHeaderPageCodec {
    pub fn encode(page: &BPlusTreeHeaderPage) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend(CommonCodec::encode_u32(page.root_page_id));
        bytes.extend(vec![0; PAGE_SIZE - bytes.len()]);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> QuillSQLResult<DecodedData<BPlusTreeHeaderPage>> {
        if bytes.len() != PAGE_SIZE {
            return Err(QuillSQLError::Storage(format!(
                "Header page size is not {} instead of {}",
                PAGE_SIZE,
                bytes.len()
            )));
        }

        let (root_page_id, offset) = CommonCodec::decode_u32(bytes)?;
        let page = BPlusTreeHeaderPage { root_page_id };

        Ok((page, offset))
    }
}

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

    /// Decode only the page type header without materializing the entire page.
    pub fn decode_header(bytes: &[u8]) -> QuillSQLResult<DecodedData<BPlusTreePageType>> {
        BPlusTreePageTypeCodec::decode(bytes)
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

    /// Decode only the leaf page header and return (header, bytes_consumed)
    pub fn decode_header_only(
        bytes: &[u8],
    ) -> QuillSQLResult<DecodedData<BPlusTreeLeafPageHeader>> {
        if bytes.len() != PAGE_SIZE {
            return Err(QuillSQLError::Storage(format!(
                "Index page size is not {} instead of {}",
                PAGE_SIZE,
                bytes.len()
            )));
        }
        let (page_type, _) = BPlusTreePageTypeCodec::decode(bytes)?;
        if !matches!(page_type, BPlusTreePageType::LeafPage) {
            return Err(QuillSQLError::Storage(
                "Index page type must be leaf page".to_string(),
            ));
        }
        BPlusTreeLeafPageHeaderCodec::decode(bytes)
    }

    /// Decode one (Tuple, RecordId) starting at `offset` within the page bytes.
    /// Returns ((tuple, rid), new_offset).
    pub fn decode_kv_at_offset(
        bytes: &[u8],
        schema: SchemaRef,
        offset: usize,
    ) -> QuillSQLResult<DecodedData<(Tuple, RecordId)>> {
        if bytes.len() != PAGE_SIZE {
            return Err(QuillSQLError::Storage(format!(
                "Index page size is not {} instead of {}",
                PAGE_SIZE,
                bytes.len()
            )));
        }
        let mut left_bytes = &bytes[offset..];
        let (tuple, t_off) = TupleCodec::decode(left_bytes, schema.clone())?;
        left_bytes = &left_bytes[t_off..];
        let (rid, r_off) = RidCodec::decode(left_bytes)?;
        let consumed = t_off + r_off;
        Ok(((tuple, rid), offset + consumed))
    }
}

pub struct BPlusTreeInternalPageCodec;

impl BPlusTreeInternalPageCodec {
    pub fn encode(page: &BPlusTreeInternalPage) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend(BPlusTreeInternalPageHeaderCodec::encode(&page.header));
        // encode optional high_key presence
        if let Some(hk) = &page.high_key {
            bytes.extend(CommonCodec::encode_u8(1));
            bytes.extend(TupleCodec::encode(hk));
        } else {
            bytes.extend(CommonCodec::encode_u8(0));
        }
        // enable prefix-compress for internal keys
        bytes.extend(CommonCodec::encode_u8(1)); // compression flag = 1 (prefix-compressed)
        let mut prev_key_bytes: Vec<u8> = Vec::new();
        for (tuple, page_id) in page.array.iter() {
            let full = TupleCodec::encode(tuple);
            let mut lcp = 0usize;
            let max_lcp = prev_key_bytes.len().min(full.len());
            while lcp < max_lcp && prev_key_bytes[lcp] == full[lcp] {
                lcp += 1;
            }
            let suffix = &full[lcp..];
            bytes.extend(CommonCodec::encode_u16(lcp as u16));
            bytes.extend(CommonCodec::encode_u32(suffix.len() as u32));
            bytes.extend(suffix);
            bytes.extend(CommonCodec::encode_u32(*page_id));
            prev_key_bytes = full;
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

            // decode optional high_key
            let (has_high_key, offset) = CommonCodec::decode_u8(left_bytes)?;
            left_bytes = &left_bytes[offset..];
            let high_key = if has_high_key == 1 {
                let (hk, offset) = TupleCodec::decode(left_bytes, schema.clone())?;
                left_bytes = &left_bytes[offset..];
                Some(hk)
            } else {
                None
            };

            // compression flag
            let (compress_flag, offset) = CommonCodec::decode_u8(left_bytes)?;
            left_bytes = &left_bytes[offset..];

            let mut array = vec![];
            if compress_flag == 1 {
                // prefix-compressed decoding
                let mut prev_key_bytes: Vec<u8> = Vec::new();
                for _ in 0..header.current_size {
                    let (lcp_u16, o1) = CommonCodec::decode_u16(left_bytes)?;
                    left_bytes = &left_bytes[o1..];
                    let (suf_len_u32, o2) = CommonCodec::decode_u32(left_bytes)?;
                    left_bytes = &left_bytes[o2..];
                    let suf_len = suf_len_u32 as usize;
                    let suffix = &left_bytes[..suf_len];
                    left_bytes = &left_bytes[suf_len..];
                    // reconstruct full key bytes
                    let mut full = Vec::with_capacity(lcp_u16 as usize + suf_len);
                    full.extend_from_slice(
                        &prev_key_bytes[..(lcp_u16 as usize).min(prev_key_bytes.len())],
                    );
                    full.extend_from_slice(suffix);
                    let (tuple, _off) = TupleCodec::decode(&full, schema.clone())?;
                    prev_key_bytes = full;
                    let (page_id, o3) = CommonCodec::decode_u32(left_bytes)?;
                    left_bytes = &left_bytes[o3..];
                    array.push((tuple, page_id));
                }
            } else {
                // legacy raw format
                for _ in 0..header.current_size {
                    let (tuple, offset) = TupleCodec::decode(left_bytes, schema.clone())?;
                    left_bytes = &left_bytes[offset..];
                    let (page_id, offset) = CommonCodec::decode_u32(left_bytes)?;
                    left_bytes = &left_bytes[offset..];
                    array.push((tuple, page_id));
                }
            }

            Ok((
                BPlusTreeInternalPage {
                    schema,
                    header,
                    array,
                    high_key,
                },
                PAGE_SIZE,
            ))
        } else {
            Err(QuillSQLError::Storage(
                "Index page type must be internal page".to_string(),
            ))
        }
    }

    /// Decode only the internal page header without materializing payload.
    pub fn decode_header_only(
        bytes: &[u8],
    ) -> QuillSQLResult<DecodedData<BPlusTreeInternalPageHeader>> {
        if bytes.len() != PAGE_SIZE {
            return Err(QuillSQLError::Storage(format!(
                "Index page size is not {} instead of {}",
                PAGE_SIZE,
                bytes.len()
            )));
        }
        let (page_type, _) = BPlusTreePageTypeCodec::decode(bytes)?;
        if !matches!(page_type, BPlusTreePageType::InternalPage) {
            return Err(QuillSQLError::Storage(
                "Index page type must be internal page".to_string(),
            ));
        }
        BPlusTreeInternalPageHeaderCodec::decode(bytes)
    }
}

impl BPlusTreeInternalPageCodec {
    /// Lookup child page id from an internal page byte slice without fully decoding the page.
    pub fn lookup_child_from_bytes(
        bytes: &[u8],
        schema: SchemaRef,
        search_key: &Tuple,
    ) -> QuillSQLResult<u32> {
        if bytes.len() != PAGE_SIZE {
            return Err(QuillSQLError::Storage(format!(
                "Index page size is not {} instead of {}",
                PAGE_SIZE,
                bytes.len()
            )));
        }
        // Decode type and header
        let (page_type, _t_off) = BPlusTreePageTypeCodec::decode(bytes)?;
        if !matches!(page_type, BPlusTreePageType::InternalPage) {
            return Err(QuillSQLError::Storage(
                "Index page type must be internal page".to_string(),
            ));
        }
        let (header, h_off) = BPlusTreeInternalPageHeaderCodec::decode(bytes)?;
        let mut left_bytes = &bytes[h_off..];
        // Optional high_key
        let (has_high_key, o1) = CommonCodec::decode_u8(left_bytes)?;
        left_bytes = &left_bytes[o1..];
        if has_high_key == 1 {
            let (_hk, o2) = TupleCodec::decode(left_bytes, schema.clone())?;
            left_bytes = &left_bytes[o2..];
        }
        // Compression flag
        let (compress_flag, o3) = CommonCodec::decode_u8(left_bytes)?;
        left_bytes = &left_bytes[o3..];

        let mut result_pid: u32;
        if compress_flag == 1 {
            // sentinel
            let (_lcp, o4) = CommonCodec::decode_u16(left_bytes)?;
            left_bytes = &left_bytes[o4..];
            let (suf_len_u32, o5) = CommonCodec::decode_u32(left_bytes)?;
            left_bytes = &left_bytes[o5..];
            let suf_len = suf_len_u32 as usize;
            let suffix = &left_bytes[..suf_len];
            left_bytes = &left_bytes[suf_len..];
            let (pid0, o6) = CommonCodec::decode_u32(left_bytes)?;
            left_bytes = &left_bytes[o6..];
            result_pid = pid0;
            let mut prev_key_bytes: Vec<u8> = suffix.to_vec();
            for _ in 1..header.current_size {
                let (lcp_u16, o7) = CommonCodec::decode_u16(left_bytes)?;
                left_bytes = &left_bytes[o7..];
                let (suf_len_u32, o8) = CommonCodec::decode_u32(left_bytes)?;
                left_bytes = &left_bytes[o8..];
                let suf_len = suf_len_u32 as usize;
                let suffix = &left_bytes[..suf_len];
                left_bytes = &left_bytes[suf_len..];
                let keep = (lcp_u16 as usize).min(prev_key_bytes.len());
                let mut full = Vec::with_capacity(keep + suf_len);
                full.extend_from_slice(&prev_key_bytes[..keep]);
                full.extend_from_slice(suffix);
                let (tuple_i, _off_i) = TupleCodec::decode(&full, schema.clone())?;
                let (pid_i, o9) = CommonCodec::decode_u32(left_bytes)?;
                left_bytes = &left_bytes[o9..];
                if tuple_i <= *search_key {
                    result_pid = pid_i;
                }
                prev_key_bytes = full;
            }
        } else {
            // raw format
            let (_sentinel, o4) = TupleCodec::decode(left_bytes, schema.clone())?;
            left_bytes = &left_bytes[o4..];
            let (pid0, o5) = CommonCodec::decode_u32(left_bytes)?;
            left_bytes = &left_bytes[o5..];
            result_pid = pid0;
            for _ in 1..header.current_size {
                let (tuple_i, o6) = TupleCodec::decode(left_bytes, schema.clone())?;
                left_bytes = &left_bytes[o6..];
                let (pid_i, o7) = CommonCodec::decode_u32(left_bytes)?;
                left_bytes = &left_bytes[o7..];
                if tuple_i <= *search_key {
                    result_pid = pid_i;
                }
            }
        }
        Ok(result_pid)
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
            _ => Err(QuillSQLError::Storage(format!(
                "Invalid page type {}",
                flag
            ))),
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
        bytes.extend(CommonCodec::encode_u32(header.version));
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

        let (version, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        Ok((
            BPlusTreeLeafPageHeader {
                page_type,
                current_size,
                max_size,
                next_page_id,
                version,
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
        bytes.extend(CommonCodec::encode_u32(header.version));
        bytes.extend(CommonCodec::encode_u32(header.next_page_id));
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

        let (version, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        let (next_page_id, offset) = CommonCodec::decode_u32(left_bytes)?;
        left_bytes = &left_bytes[offset..];

        Ok((
            BPlusTreeInternalPageHeader {
                page_type,
                current_size,
                max_size,
                version,
                next_page_id,
            },
            bytes.len() - left_bytes.len(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{Column, DataType, Schema};
    use crate::storage::codec::btree_page::BPlusTreePageCodec;
    use crate::storage::page::RecordId;
    use crate::storage::page::{BPlusTreeInternalPage, BPlusTreeLeafPage, BPlusTreePage};
    use crate::storage::tuple::Tuple;
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
