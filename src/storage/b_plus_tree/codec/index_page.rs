use crate::error::{Error, Result};
use crate::storage::b_plus_tree::buffer_pool_manager::PAGE_SIZE;
use crate::storage::b_plus_tree::codec::{CommonCodec, DecodedData, RidCodec};
use crate::storage::b_plus_tree::comparator::default_comparator;
use crate::storage::b_plus_tree::page::index_page::{
    BPlusTreeInternalPage, BPlusTreeInternalPageHeader, BPlusTreeLeafPage, BPlusTreeLeafPageHeader,
    BPlusTreePage, BPlusTreePageType,
};

pub struct BPlusTreePageCodec;

impl BPlusTreePageCodec {
    pub fn encode(page: &BPlusTreePage) -> Vec<u8> {
        match page {
            BPlusTreePage::Leaf(page) => BPlusTreeLeafPageCodec::encode(page),
            BPlusTreePage::Internal(page) => BPlusTreeInternalPageCodec::encode(page),
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<DecodedData<BPlusTreePage>> {
        if bytes.len() != PAGE_SIZE {
            return Err(Error::Internal(format!(
                "Index page size is not {} instead of {}",
                PAGE_SIZE,
                bytes.len()
            )));
        }

        // not consume left_bytes
        let (page_type, _) = BPlusTreePageTypeCodec::decode(bytes)?;

        match page_type {
            BPlusTreePageType::LeafPage => {
                let (page, offset) = BPlusTreeLeafPageCodec::decode(bytes)?;
                Ok((BPlusTreePage::Leaf(page), offset))
            }
            BPlusTreePageType::InternalPage => {
                let (page, offset) = BPlusTreeInternalPageCodec::decode(bytes)?;
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
        for (key, rid) in page.array.iter() {
            // 对于Vec<u8>类型的key，先编码长度再添加内容
            bytes.extend(CommonCodec::encode_u32(key.len() as u32));
            bytes.extend(key);
            bytes.extend(RidCodec::encode(rid));
        }
        // make sure length of bytes is BUSTUBX_PAGE_SIZE
        assert!(bytes.len() <= PAGE_SIZE);
        bytes.extend(vec![0; PAGE_SIZE - bytes.len()]);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<DecodedData<BPlusTreeLeafPage>> {
        if bytes.len() != PAGE_SIZE {
            return Err(Error::Internal(format!(
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
                let (key, offset) = CommonCodec::decode_bytes(left_bytes)?;
                left_bytes = &left_bytes[offset..];

                let (rid, offset) = RidCodec::decode(left_bytes)?;
                left_bytes = &left_bytes[offset..];

                array.push((key, rid));
            }

            Ok((
                BPlusTreeLeafPage {
                    header,
                    array,
                    comparator: default_comparator,
                },
                PAGE_SIZE,
            ))
        } else {
            Err(Error::Internal(
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
        for (key, page_id) in page.array.iter() {
            // 对于Vec<u8>类型的key，先编码长度再添加内容
            bytes.extend(CommonCodec::encode_u32(key.len() as u32));
            bytes.extend(key);
            bytes.extend(CommonCodec::encode_u32(*page_id));
        }
        // make sure length of bytes is BUSTUBX_PAGE_SIZE
        assert!(bytes.len() <= PAGE_SIZE);
        bytes.extend(vec![0; PAGE_SIZE - bytes.len()]);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<DecodedData<BPlusTreeInternalPage>> {
        if bytes.len() != PAGE_SIZE {
            return Err(Error::Internal(format!(
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
                let (key, offset) = CommonCodec::decode_bytes(left_bytes)?;
                left_bytes = &left_bytes[offset..];

                let (page_id, offset) = CommonCodec::decode_u32(left_bytes)?;
                left_bytes = &left_bytes[offset..];

                array.push((key, page_id));
            }

            Ok((
                BPlusTreeInternalPage {
                    header,
                    array,
                    comparator: default_comparator,
                },
                PAGE_SIZE,
            ))
        } else {
            Err(Error::Internal(
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

    pub fn decode(bytes: &[u8]) -> Result<DecodedData<BPlusTreePageType>> {
        let (flag, offset) = CommonCodec::decode_u8(bytes)?;
        match flag {
            1 => Ok((BPlusTreePageType::LeafPage, offset)),
            2 => Ok((BPlusTreePageType::InternalPage, offset)),
            _ => Err(Error::Internal(format!("Invalid page type {}", flag))),
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

    pub fn decode(bytes: &[u8]) -> Result<DecodedData<BPlusTreeLeafPageHeader>> {
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

    pub fn decode(bytes: &[u8]) -> Result<DecodedData<BPlusTreeInternalPageHeader>> {
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
    use crate::storage::b_plus_tree::codec::index_page::BPlusTreePageCodec;
    use crate::storage::b_plus_tree::page::index_page::{
        BPlusTreeInternalPage, BPlusTreeLeafPage, BPlusTreePage,
    };
    use crate::storage::b_plus_tree::page::table_page::RecordId;

    // 辅助函数，创建测试用的字节数组
    fn create_test_key(value: u32) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    #[test]
    fn index_page_codec() {
        // 创建叶子页面并插入键值对
        let mut leaf_page = BPlusTreeLeafPage::new(100);
        let key1 = create_test_key(1);
        let key2 = create_test_key(2);
        let rid1 = RecordId::new(1, 1);
        let rid2 = RecordId::new(2, 2);

        leaf_page.insert(&key1, rid1);
        leaf_page.insert(&key2, rid2);

        // 测试叶子页面的编码和解码
        let page = BPlusTreePage::Leaf(leaf_page);
        let encoded = BPlusTreePageCodec::encode(&page);
        let (decoded_page, _) = BPlusTreePageCodec::decode(&encoded).unwrap();

        // 验证解码后的页面与原始页面相等
        match (decoded_page, page) {
            (BPlusTreePage::Leaf(decoded), BPlusTreePage::Leaf(original)) => {
                assert_eq!(decoded.header.current_size, original.header.current_size);
                assert_eq!(decoded.array.len(), original.array.len());
                // 可以进一步验证具体的key-value对是否匹配
            }
            _ => panic!("Decoded page type doesn't match original"),
        }

        // 创建内部页面并插入键值对
        let mut internal_page = BPlusTreeInternalPage::new(100);
        let empty_key = Vec::new(); // 第一个键为空

        internal_page.insert(empty_key, 0);
        internal_page.insert(key1.clone(), 1);
        internal_page.insert(key2.clone(), 2);

        // 测试内部页面的编码和解码
        let page = BPlusTreePage::Internal(internal_page);
        let encoded = BPlusTreePageCodec::encode(&page);
        let (decoded_page, _) = BPlusTreePageCodec::decode(&encoded).unwrap();

        // 验证解码后的页面与原始页面相等
        match (decoded_page, page) {
            (BPlusTreePage::Internal(decoded), BPlusTreePage::Internal(original)) => {
                assert_eq!(decoded.header.current_size, original.header.current_size);
                assert_eq!(decoded.array.len(), original.array.len());
                // 可以进一步验证具体的key-value对是否匹配
            }
            _ => panic!("Decoded page type doesn't match original"),
        }
    }
}
