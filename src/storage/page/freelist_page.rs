use crate::buffer::{PageId, PAGE_SIZE, INVALID_PAGE_ID};
use crate::storage::codec::{CommonCodec, FreelistPageHeaderCodec};
use std::sync::LazyLock;

static EMPTY_FREELIST_PAGE_HEADER: FreelistPageHeader = FreelistPageHeader {
    next_page_id: 0,
    current_size: 0,
    max_size: 0,
};

pub static FREELIST_PAGE_MAX_SIZE: LazyLock<usize> = LazyLock::new(|| {
    (PAGE_SIZE - FreelistPageHeaderCodec::encode(&EMPTY_FREELIST_PAGE_HEADER).len())
        / CommonCodec::encode_u32(INVALID_PAGE_ID).len()
});

#[derive(Debug, Eq, PartialEq)]
pub struct FreelistPage {
    pub header: FreelistPageHeader,
    pub array: Vec<PageId>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct FreelistPageHeader {
    pub next_page_id: PageId,
    pub current_size: u32,
    pub max_size: u32,
}

impl FreelistPage {
    pub fn new() -> Self {
        Self {
            header: FreelistPageHeader {
                next_page_id: INVALID_PAGE_ID,
                current_size: 0,
                max_size: *FREELIST_PAGE_MAX_SIZE as u32,
            },
            array: vec![],
        }
    }

    pub fn is_full(&self) -> bool {
        self.header.current_size >= self.header.max_size
    }

    pub fn push(&mut self, page_id: PageId) {
        self.array.push(page_id);
        self.header.current_size += 1;
    }

    pub fn pop(&mut self) -> Option<PageId> {
        let page_id = self.array.pop();
        if page_id.is_some() {
            self.header.current_size -= 1;
        }
        page_id
    }
}
