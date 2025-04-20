mod common;
mod freelist_page;
mod index_page;
mod meta_page;
mod scalar;
pub mod table_page;

pub use common::CommonCodec;
pub use freelist_page::{FreelistPageCodec, FreelistPageHeaderCodec};
pub use index_page::*;
pub use meta_page::MetaPageCodec;
pub use scalar::ScalarValueCodec;
pub use table_page::*;

// data + consumed offset
pub type DecodedData<T> = (T, usize);
