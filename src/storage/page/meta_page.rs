use crate::buffer::{PageId, INVALID_PAGE_ID, PAGE_SIZE};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::codec::CommonCodec;
use std::sync::LazyLock;

pub static EMPTY_META_PAGE: MetaPage = MetaPage {
    major_version: 0,
    minor_version: 0,
    freelist_page_id: 0,
    information_schema_schemas_first_page_id: 0,
    information_schema_tables_first_page_id: 0,
    information_schema_columns_first_page_id: 0,
    information_schema_indexes_first_page_id: 0,
};

pub static META_PAGE_SIZE: LazyLock<usize> = LazyLock::new(|| PAGE_SIZE);

#[derive(Debug, Eq, PartialEq)]
pub struct MetaPage {
    pub major_version: u32,
    pub minor_version: u32,
    pub freelist_page_id: PageId,
    pub information_schema_schemas_first_page_id: PageId,
    pub information_schema_tables_first_page_id: PageId,
    pub information_schema_columns_first_page_id: PageId,
    pub information_schema_indexes_first_page_id: PageId,
}

impl MetaPage {
    pub fn try_new() -> QuillSQLResult<Self> {
        let version_str = env!("CARGO_PKG_VERSION");
        let version_arr = version_str.split('.').collect::<Vec<&str>>();
        if version_arr.len() < 2 {
            return Err(QuillSQLError::Storage(format!(
                "Package version is not xx.xx {}",
                version_str
            )));
        }
        let major_version = version_arr[0].parse::<u32>().map_err(|_| {
            QuillSQLError::Storage(format!("Failed to parse major version {}", version_arr[0]))
        })?;
        let minor_version = version_arr[1].parse::<u32>().map_err(|_| {
            QuillSQLError::Storage(format!("Failed to parse minor version {}", version_arr[1]))
        })?;

        Ok(Self {
            major_version,
            minor_version,
            freelist_page_id: INVALID_PAGE_ID,
            information_schema_schemas_first_page_id: INVALID_PAGE_ID,
            information_schema_tables_first_page_id: INVALID_PAGE_ID,
            information_schema_columns_first_page_id: INVALID_PAGE_ID,
            information_schema_indexes_first_page_id: INVALID_PAGE_ID,
        })
    }
}

pub fn encode_meta_page(page: &MetaPage) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(PAGE_SIZE);
    bytes.extend(CommonCodec::encode_u32(page.major_version));
    bytes.extend(CommonCodec::encode_u32(page.minor_version));
    bytes.extend(CommonCodec::encode_u32(page.freelist_page_id));
    bytes.extend(CommonCodec::encode_u32(
        page.information_schema_schemas_first_page_id,
    ));
    bytes.extend(CommonCodec::encode_u32(
        page.information_schema_tables_first_page_id,
    ));
    bytes.extend(CommonCodec::encode_u32(
        page.information_schema_columns_first_page_id,
    ));
    bytes.extend(CommonCodec::encode_u32(
        page.information_schema_indexes_first_page_id,
    ));
    bytes.resize(PAGE_SIZE, 0);
    bytes
}

pub fn decode_meta_page(bytes: &[u8]) -> QuillSQLResult<(MetaPage, usize)> {
    if bytes.len() != PAGE_SIZE {
        return Err(QuillSQLError::Storage(format!(
            "Meta page size is not {} instead of {}",
            PAGE_SIZE,
            bytes.len()
        )));
    }

    let mut left_bytes = bytes;

    let (major_version, offset) = CommonCodec::decode_u32(left_bytes)?;
    left_bytes = &left_bytes[offset..];
    let (minor_version, offset) = CommonCodec::decode_u32(left_bytes)?;
    left_bytes = &left_bytes[offset..];
    let (freelist_page_id, offset) = CommonCodec::decode_u32(left_bytes)?;
    left_bytes = &left_bytes[offset..];
    let (information_schema_schemas_first_page_id, offset) = CommonCodec::decode_u32(left_bytes)?;
    left_bytes = &left_bytes[offset..];
    let (information_schema_tables_first_page_id, offset) = CommonCodec::decode_u32(left_bytes)?;
    left_bytes = &left_bytes[offset..];
    let (information_schema_columns_first_page_id, offset) = CommonCodec::decode_u32(left_bytes)?;
    left_bytes = &left_bytes[offset..];
    let (information_schema_indexes_first_page_id, offset) = CommonCodec::decode_u32(left_bytes)?;
    left_bytes = &left_bytes[offset..];

    Ok((
        MetaPage {
            major_version,
            minor_version,
            freelist_page_id,
            information_schema_schemas_first_page_id,
            information_schema_tables_first_page_id,
            information_schema_columns_first_page_id,
            information_schema_indexes_first_page_id,
        },
        PAGE_SIZE - left_bytes.len(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_page_codec_roundtrip() {
        let page = MetaPage::try_new().unwrap();
        let (decoded, _) = decode_meta_page(&encode_meta_page(&page)).unwrap();
        assert_eq!(page, decoded);
    }
}
