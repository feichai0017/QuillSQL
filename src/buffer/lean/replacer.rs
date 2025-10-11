use dashmap::DashSet;
use parking_lot::Mutex;
use std::collections::VecDeque;

use crate::buffer::PageId;

use super::metadata::{LeanPageState, StateTransition};

#[derive(Debug)]
pub struct LeanReplacer {
    hot: Mutex<VecDeque<PageId>>,
    cooling: Mutex<VecDeque<PageId>>,
    cool: Mutex<VecDeque<PageId>>,
    evictable: DashSet<PageId>,
}

impl Default for LeanReplacer {
    fn default() -> Self {
        Self {
            hot: Mutex::new(VecDeque::new()),
            cooling: Mutex::new(VecDeque::new()),
            cool: Mutex::new(VecDeque::new()),
            evictable: DashSet::new(),
        }
    }
}

impl LeanReplacer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_page_created(&self, page_id: PageId, _state: LeanPageState) {
        self.remove_from_all(page_id);
        self.evictable.remove(&page_id);
    }

    pub fn on_page_removed(&self, page_id: PageId, _state: LeanPageState) {
        self.remove_from_all(page_id);
        self.evictable.remove(&page_id);
    }

    pub fn apply_transition(&self, page_id: PageId, transition: StateTransition) {
        if transition.old == transition.new {
            return;
        }
        self.remove_from_all(page_id);
        if self.evictable.contains(&page_id) && transition.new != LeanPageState::Hot {
            self.enqueue(page_id, transition.new);
        }
    }

    pub fn mark_evictable(&self, page_id: PageId, state: LeanPageState) {
        self.evictable.insert(page_id);
        self.remove_from_all(page_id);
        if state != LeanPageState::Hot {
            self.enqueue(page_id, state);
        }
    }

    pub fn mark_non_evictable(&self, page_id: PageId) {
        self.evictable.remove(&page_id);
        self.remove_from_all(page_id);
    }

    pub fn pop_victim(&self) -> Option<PageId> {
        loop {
            if let Some(candidate) = self.cool.lock().pop_front() {
                if self.evictable.contains(&candidate) {
                    return Some(candidate);
                }
                continue;
            }
            if let Some(candidate) = self.cooling.lock().pop_front() {
                if self.evictable.contains(&candidate) {
                    return Some(candidate);
                }
                continue;
            }
            return None;
        }
    }

    #[allow(dead_code)]
    pub fn next_victim_candidate(&self) -> Option<PageId> {
        self.pop_victim()
    }

    pub fn snapshot(&self) -> LeanReplacerSnapshot {
        LeanReplacerSnapshot {
            hot_len: self.hot.lock().len(),
            cooling_len: self.cooling.lock().len(),
            cool_len: self.cool.lock().len(),
        }
    }

    fn enqueue(&self, page_id: PageId, state: LeanPageState) {
        let queue = match state {
            LeanPageState::Hot => &self.hot,
            LeanPageState::Cooling => &self.cooling,
            LeanPageState::Cool => &self.cool,
        };
        queue.lock().push_back(page_id);
    }

    fn remove_from_all(&self, page_id: PageId) {
        Self::remove_from_queue(page_id, &self.hot);
        Self::remove_from_queue(page_id, &self.cooling);
        Self::remove_from_queue(page_id, &self.cool);
    }

    fn remove_from_queue(page_id: PageId, queue: &Mutex<VecDeque<PageId>>) {
        let mut guard = queue.lock();
        if let Some(pos) = guard.iter().position(|candidate| *candidate == page_id) {
            guard.remove(pos);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LeanReplacerSnapshot {
    pub hot_len: usize,
    pub cooling_len: usize,
    pub cool_len: usize,
}
