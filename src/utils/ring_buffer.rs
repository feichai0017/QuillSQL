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
