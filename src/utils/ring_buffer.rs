#[derive(Debug)]
pub struct RingBuffer<T> {
    buf: Vec<Option<T>>,
    head: usize,
    len: usize,
}

impl<T> RingBuffer<T> {
    pub fn with_capacity(cap: usize) -> Self {
        assert!(cap > 0);
        let mut buf = Vec::with_capacity(cap);
        buf.resize_with(cap, || None);
        Self {
            buf,
            head: 0,
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    pub fn push(&mut self, item: T) {
        let cap = self.buf.len();
        if self.len < cap {
            let tail = (self.head + self.len) % cap;
            self.buf[tail] = Some(item);
            self.len += 1;
        } else {
            self.buf[self.head] = Some(item);
            self.head = (self.head + 1) % cap;
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        let idx = self.head;
        let item = self.buf[idx].take();
        self.head = (self.head + 1) % self.buf.len();
        self.len -= 1;
        item
    }
}

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

const SLOT_EMPTY: u8 = 0;
const SLOT_READY: u8 = 1;

#[derive(Debug)]
struct Slot<T> {
    value: UnsafeCell<Option<T>>,
    state: AtomicU8,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Self {
            value: UnsafeCell::new(None),
            state: AtomicU8::new(SLOT_EMPTY),
        }
    }
}

unsafe impl<T: Send> Send for Slot<T> {}
unsafe impl<T: Send> Sync for Slot<T> {}

#[derive(Debug)]
pub struct ConcurrentRingBuffer<T> {
    slots: Vec<Slot<T>>,
    capacity: usize,
    head: AtomicU64,
    tail: AtomicU64,
}

unsafe impl<T: Send> Send for ConcurrentRingBuffer<T> {}
unsafe impl<T: Send> Sync for ConcurrentRingBuffer<T> {}

impl<T> ConcurrentRingBuffer<T> {
    pub fn with_capacity(cap: usize) -> Self {
        assert!(cap > 0, "concurrent ring buffer needs capacity > 0");
        let mut slots = Vec::with_capacity(cap);
        for _ in 0..cap {
            slots.push(Slot::new());
        }
        Self {
            slots,
            capacity: cap,
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
        }
    }

    #[inline]
    fn index(&self, cursor: u64) -> usize {
        (cursor as usize) % self.capacity
    }

    #[inline]
    pub fn len(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        tail.saturating_sub(head) as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn try_push(&self, value: T) -> Result<(), T> {
        loop {
            let head = self.head.load(Ordering::Acquire);
            let tail = self.tail.load(Ordering::Relaxed);
            if tail.wrapping_sub(head) as usize >= self.capacity {
                return Err(value);
            }
            if self
                .tail
                .compare_exchange(tail, tail + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                let slot = &self.slots[self.index(tail)];
                while slot.state.load(Ordering::Acquire) != SLOT_EMPTY {
                    std::hint::spin_loop();
                }
                unsafe {
                    *slot.value.get() = Some(value);
                }
                slot.state.store(SLOT_READY, Ordering::Release);
                return Ok(());
            }
            // CAS failed due to race – retry with same value
            std::hint::spin_loop();
        }
    }

    pub fn pop(&self) -> Option<T> {
        loop {
            let head = self.head.load(Ordering::Relaxed);
            let tail = self.tail.load(Ordering::Acquire);
            if head >= tail {
                return None;
            }
            if self
                .head
                .compare_exchange(head, head + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                let slot = &self.slots[self.index(head)];
                while slot.state.load(Ordering::Acquire) != SLOT_READY {
                    std::hint::spin_loop();
                }
                let item = unsafe { (*slot.value.get()).take() };
                slot.state.store(SLOT_EMPTY, Ordering::Release);
                return item;
            }
            std::hint::spin_loop();
        }
    }

    pub fn peek_clone(&self) -> Option<T>
    where
        T: Clone,
    {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        if head >= tail {
            return None;
        }
        let slot = &self.slots[self.index(head)];
        if slot.state.load(Ordering::Acquire) != SLOT_READY {
            return None;
        }
        unsafe { (*slot.value.get()).as_ref().cloned() }
    }

    pub fn snapshot(&self) -> Vec<T>
    where
        T: Clone,
    {
        let mut items = Vec::new();
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        for cursor in head..tail {
            let slot = &self.slots[self.index(cursor)];
            if slot.state.load(Ordering::Acquire) == SLOT_READY {
                if let Some(value) = unsafe { (*slot.value.get()).as_ref() } {
                    items.push(value.clone());
                }
            }
        }
        items
    }
}

impl<T> Drop for ConcurrentRingBuffer<T> {
    fn drop(&mut self) {
        for slot in &self.slots {
            unsafe {
                let _ = (*slot.value.get()).take();
            }
        }
    }
}
