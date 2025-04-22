use crate::error::{Error, Result};
use crate::storage::b_plus_tree::buffer_pool_manager::{PageId, INVALID_PAGE_ID};
use crate::storage::codec::meta_page::MetaPageCodec;
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

pub static META_PAGE_SIZE: LazyLock<usize> =
    LazyLock::new(|| MetaPageCodec::encode(&EMPTY_META_PAGE).len());

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
    pub fn try_new() -> Result<Self> {
        let version_str = env!("CARGO_PKG_VERSION");
        let version_arr = version_str.split('.').collect::<Vec<&str>>();
        if version_arr.len() < 2 {
            return Err(Error::Internal(format!(
                "Package version is not xx.xx {}",
                version_str
            )));
        }
        let major_version = version_arr[0].parse::<u32>().map_err(|_| {
            Error::Internal(format!("Failed to parse major version {}", version_arr[0]))
        })?;
        let minor_version = version_arr[1].parse::<u32>().map_err(|_| {
            Error::Internal(format!("Failed to parse minor version {}", version_arr[1]))
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
