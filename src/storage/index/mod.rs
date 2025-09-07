use crate::{
    catalog::SchemaRef, error::QuillSQLResult, storage::page::RecordId, storage::tuple::Tuple,
};

pub mod btree_index;

/// A generic trait for database indexes.
/// This allows for different underlying implementations (e.g., B+Tree, BzTree).
pub trait Index: std::fmt::Debug + Send + Sync {
    /// Inserts a key-value pair into the index.
    fn insert(&self, key: &Tuple, value: RecordId) -> QuillSQLResult<()>;

    /// Retrieves the value associated with a key.
    fn get(&self, key: &Tuple) -> QuillSQLResult<Option<RecordId>>;

    /// Deletes a key-value pair from the index.
    fn delete(&self, key: &Tuple) -> QuillSQLResult<()>;

    /// Returns the schema of the keys in the index.
    fn key_schema(&self) -> &SchemaRef;
}
