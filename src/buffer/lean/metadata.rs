use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;

use crate::buffer::PageId;

use super::LeanBufferOptions;

const HOT_THRESHOLD: f64 = 0.6;
const COOLING_THRESHOLD: f64 = 0.25;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeanPageState {
    Hot,
    Cooling,
    Cool,
}

#[derive(Debug, Clone, Copy)]
pub enum AccessKind {
    New,
    Read,
    Write,
    Prefetch,
}

#[derive(Debug, Clone, Copy)]
pub struct StateTransition {
    pub old: LeanPageState,
    pub new: LeanPageState,
}

#[derive(Debug, Clone)]
pub struct LeanPageSnapshot {
    pub page_id: PageId,
    pub state: LeanPageState,
    pub temperature: f64,
    pub access_count: u64,
    pub last_access: Instant,
}

#[derive(Debug)]
struct LeanPageStats {
    state: LeanPageState,
    temperature: f64,
    access_count: u64,
    last_access: Instant,
}

#[derive(Debug)]
pub struct LeanPageDescriptor {
    page_id: PageId,
    options: Arc<LeanBufferOptions>,
    stats: Mutex<LeanPageStats>,
}

impl LeanPageDescriptor {
    pub fn new(page_id: PageId, options: Arc<LeanBufferOptions>) -> Self {
        Self {
            page_id,
            options,
            stats: Mutex::new(LeanPageStats {
                state: LeanPageState::Cool,
                temperature: 0.0,
                access_count: 0,
                last_access: Instant::now(),
            }),
        }
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn state(&self) -> LeanPageState {
        self.stats.lock().state
    }

    pub fn snapshot(&self) -> LeanPageSnapshot {
        let stats = self.stats.lock();
        LeanPageSnapshot {
            page_id: self.page_id,
            state: stats.state,
            temperature: stats.temperature,
            access_count: stats.access_count,
            last_access: stats.last_access,
        }
    }

    pub fn record_access(&self, kind: AccessKind) -> StateTransition {
        let mut stats = self.stats.lock();
        let old_state = stats.state;
        stats.last_access = Instant::now();
        match kind {
            AccessKind::New | AccessKind::Read | AccessKind::Write => {
                stats.temperature = 1.0;
                stats.state = LeanPageState::Hot;
            }
            AccessKind::Prefetch => {
                stats.temperature = (stats.temperature * 0.5).max(0.4);
                stats.state = LeanPageState::Cooling;
            }
        }
        stats.access_count = stats.access_count.saturating_add(1);
        StateTransition {
            old: old_state,
            new: stats.state,
        }
    }

    pub fn record_release(&self, kind: AccessKind) -> StateTransition {
        let mut stats = self.stats.lock();
        let old_state = stats.state;
        stats.last_access = Instant::now();
        // Writes keep the page hot longer, so maintain higher temperature.
        let decay = if matches!(kind, AccessKind::Write) {
            (self.options.temperature_decay + 1.0) / 2.0
        } else {
            self.options.temperature_decay
        };
        stats.temperature *= decay.clamp(0.0, 1.0);
        stats.temperature = stats.temperature.clamp(0.0, 1.0);
        stats.state = categorize_state(stats.temperature);
        StateTransition {
            old: old_state,
            new: stats.state,
        }
    }

    pub fn cool_down(&self, decay: f64) -> Option<StateTransition> {
        if !(0.0..=1.0).contains(&decay) {
            return None;
        }
        let mut stats = self.stats.lock();
        let old_state = stats.state;
        let previous_temp = stats.temperature;
        stats.temperature = (stats.temperature * decay).clamp(0.0, 1.0);
        if (stats.temperature - previous_temp).abs() < f64::EPSILON {
            return None;
        }
        stats.state = categorize_state(stats.temperature);
        Some(StateTransition {
            old: old_state,
            new: stats.state,
        })
    }

    pub fn force_state(&self, target: LeanPageState) -> Option<StateTransition> {
        let mut stats = self.stats.lock();
        let old_state = stats.state;
        if old_state == target {
            return None;
        }
        stats.state = target;
        stats.temperature = match target {
            LeanPageState::Hot => 1.0,
            LeanPageState::Cooling => (HOT_THRESHOLD + COOLING_THRESHOLD) / 2.0,
            LeanPageState::Cool => COOLING_THRESHOLD / 2.0,
        };
        Some(StateTransition {
            old: old_state,
            new: target,
        })
    }
}

fn categorize_state(temperature: f64) -> LeanPageState {
    if temperature >= HOT_THRESHOLD {
        LeanPageState::Hot
    } else if temperature >= COOLING_THRESHOLD {
        LeanPageState::Cooling
    } else {
        LeanPageState::Cool
    }
}

#[derive(Debug, Default)]
pub struct LeanBufferStats {
    hot_pages: AtomicUsize,
    cooling_pages: AtomicUsize,
    cool_pages: AtomicUsize,
    total_accesses: AtomicU64,
    write_accesses: AtomicU64,
    prefetches: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub struct LeanBufferStatsSnapshot {
    pub hot_pages: usize,
    pub cooling_pages: usize,
    pub cool_pages: usize,
    pub total_accesses: u64,
    pub write_accesses: u64,
    pub prefetches: u64,
}

impl LeanBufferStats {
    pub fn on_page_created(&self, state: LeanPageState) {
        self.counter(state).fetch_add(1, Ordering::Relaxed);
    }

    pub fn on_page_removed(&self, state: LeanPageState) {
        self.counter(state).fetch_sub(1, Ordering::Relaxed);
    }

    pub fn apply_transition(&self, transition: StateTransition) {
        if transition.old == transition.new {
            return;
        }
        self.counter(transition.old).fetch_sub(1, Ordering::Relaxed);
        self.counter(transition.new).fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_access(&self, kind: AccessKind) {
        self.total_accesses.fetch_add(1, Ordering::Relaxed);
        match kind {
            AccessKind::Write => {
                self.write_accesses.fetch_add(1, Ordering::Relaxed);
            }
            AccessKind::Prefetch => {
                self.prefetches.fetch_add(1, Ordering::Relaxed);
            }
            AccessKind::New | AccessKind::Read => {}
        }
    }

    pub fn snapshot(&self) -> LeanBufferStatsSnapshot {
        LeanBufferStatsSnapshot {
            hot_pages: self.hot_pages.load(Ordering::Relaxed),
            cooling_pages: self.cooling_pages.load(Ordering::Relaxed),
            cool_pages: self.cool_pages.load(Ordering::Relaxed),
            total_accesses: self.total_accesses.load(Ordering::Relaxed),
            write_accesses: self.write_accesses.load(Ordering::Relaxed),
            prefetches: self.prefetches.load(Ordering::Relaxed),
        }
    }

    fn counter(&self, state: LeanPageState) -> &AtomicUsize {
        match state {
            LeanPageState::Hot => &self.hot_pages,
            LeanPageState::Cooling => &self.cooling_pages,
            LeanPageState::Cool => &self.cool_pages,
        }
    }
}
