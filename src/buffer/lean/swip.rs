use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::buffer::PageId;

const SWIZZLED_TAG: usize = 1;

#[derive(Default)]
pub struct LeanSwip<T> {
    raw: AtomicUsize,
    _marker: PhantomData<T>,
}

impl<T> LeanSwip<T> {
    pub fn new(page_id: PageId) -> Self {
        Self::new_from_page(page_id)
    }

    pub fn new_from_page(page_id: PageId) -> Self {
        Self {
            raw: AtomicUsize::new(Self::encode_page(page_id)),
            _marker: PhantomData,
        }
    }

    pub fn is_swizzled(&self) -> bool {
        self.raw.load(Ordering::Acquire) & SWIZZLED_TAG == SWIZZLED_TAG
    }

    pub fn page_id(&self) -> Option<PageId> {
        let value = self.raw.load(Ordering::Acquire);
        if value & SWIZZLED_TAG == SWIZZLED_TAG {
            None
        } else {
            Some(Self::decode_page(value))
        }
    }

    pub fn ptr(&self) -> Option<NonNull<T>> {
        let value = self.raw.load(Ordering::Acquire);
        if value & SWIZZLED_TAG == SWIZZLED_TAG {
            let ptr = (value & !SWIZZLED_TAG) as *mut T;
            NonNull::new(ptr)
        } else {
            None
        }
    }

    pub fn swizzle(&self, ptr: NonNull<T>) {
        let tagged = (ptr.as_ptr() as usize) | SWIZZLED_TAG;
        debug_assert_eq!(tagged & SWIZZLED_TAG, SWIZZLED_TAG);
        self.raw.store(tagged, Ordering::Release);
    }

    pub fn unswizzle(&self, page_id: PageId) {
        self.raw
            .store(Self::encode_page(page_id), Ordering::Release);
    }

    pub fn replace_page_id(&self, page_id: PageId) {
        self.unswizzle(page_id);
    }

    fn encode_page(page_id: PageId) -> usize {
        (page_id as usize) << 1
    }

    fn decode_page(value: usize) -> PageId {
        (value >> 1) as PageId
    }
}

impl<T> From<PageId> for LeanSwip<T> {
    fn from(page_id: PageId) -> Self {
        Self::new(page_id)
    }
}

impl<T> Debug for LeanSwip<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(page_id) = self.page_id() {
            f.debug_struct("LeanSwip")
                .field("page_id", &page_id)
                .finish()
        } else if let Some(ptr) = self.ptr() {
            f.debug_struct("LeanSwip").field("ptr", &ptr).finish()
        } else {
            f.debug_struct("LeanSwip")
                .field("state", &"invalid")
                .finish()
        }
    }
}
