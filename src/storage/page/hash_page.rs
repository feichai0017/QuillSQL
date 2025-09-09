use crate::catalog::SchemaRef;
use crate::storage::page::RecordId;
use crate::storage::tuple::Tuple;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HashBucketPageHeader {
    pub current_size: u32,
    pub max_size: u32,
    pub local_depth: u32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HashBucketPage {
    pub schema: SchemaRef,
    pub header: HashBucketPageHeader,
    pub array: Vec<(Tuple, RecordId)>,
}

impl HashBucketPage {
    pub fn new(schema: SchemaRef, max_size: u32, local_depth: u32) -> Self {
        Self {
            schema,
            header: HashBucketPageHeader {
                current_size: 0,
                max_size,
                local_depth,
            },
            array: Vec::with_capacity(max_size as usize),
        }
    }

    pub fn is_full(&self) -> bool {
        self.header.current_size >= self.header.max_size
    }
}
