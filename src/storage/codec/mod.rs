pub mod common;
pub mod freelist_page;
pub mod index_page;
pub mod meta_page;
pub mod scalar;
pub mod table_page;

// data + consumed offset
pub type DecodedData<T> = (T, usize);
