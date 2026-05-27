mod common;
mod scalar;
mod tuple;

pub use common::CommonCodec;
pub use scalar::ScalarValueCodec;
pub use tuple::TupleCodec;

// data + consumed offset
pub type DecodedData<T> = (T, usize);
