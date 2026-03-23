//! Circuit breaker, exponential backoff with jitter, and software watchdog (heartbeat).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rand::Rng;

// --- Circuit breaker (closed → open → half-open) ---

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

struct CircuitInner {
    state: CircuitState,
    failures: u32,
    opened_at_ms: Option<u64>,
}

pub struct CircuitBreaker {
    failure_threshold: u32,
    open_cooldown_ms: u64,
    inner: Mutex<CircuitInner>,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, open_cooldown_ms: u64) -> Self {
        Self {
            failure_threshold,
            open_cooldown_ms,
            inner: Mutex::new(CircuitInner {
                state: CircuitState::Closed,
                failures: 0,
                opened_at_ms: None,
            }),
        }
    }

    pub fn state(&self) -> CircuitState {
        self.inner.lock().map(|g| g.state).unwrap_or(CircuitState::Open)
    }

    /// Returns true if a probe (connect / send) may be attempted now.
    pub fn allow_request(&self, now_ms: u64) -> bool {
        let Ok(mut g) = self.inner.lock() else {
            return false;
        };
        match g.state {
            CircuitState::Closed => true,
            CircuitState::HalfOpen => true,
            CircuitState::Open => {
                let Some(opened) = g.opened_at_ms else {
                    g.state = CircuitState::HalfOpen;
                    return true;
                };
                if now_ms.saturating_sub(opened) >= self.open_cooldown_ms {
                    g.state = CircuitState::HalfOpen;
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn on_success(&self) {
        let Ok(mut g) = self.inner.lock() else {
            return;
        };
        g.failures = 0;
        g.opened_at_ms = None;
        g.state = CircuitState::Closed;
    }

    pub fn on_failure(&self, now_ms: u64) {
        let Ok(mut g) = self.inner.lock() else {
            return;
        };
        match g.state {
            CircuitState::HalfOpen => {
                g.state = CircuitState::Open;
                g.opened_at_ms = Some(now_ms);
            }
            CircuitState::Closed => {
                g.failures = g.failures.saturating_add(1);
                if g.failures >= self.failure_threshold {
                    g.state = CircuitState::Open;
                    g.opened_at_ms = Some(now_ms);
                }
            }
            CircuitState::Open => {}
        }
    }
}

// --- Exponential backoff + jitter ---

pub struct ExponentialBackoff {
    base_ms: u64,
    max_ms: u64,
    attempt: u32,
}

impl ExponentialBackoff {
    pub fn new(base_ms: u64, max_ms: u64) -> Self {
        Self {
            base_ms,
            max_ms,
            attempt: 0,
        }
    }

    pub fn next_sleep_ms(&mut self) -> u64 {
        let pow = self.attempt.min(20);
        let exp = self
            .base_ms
            .saturating_mul(2u64.saturating_pow(pow));
        let capped = exp.min(self.max_ms);
        let jitter = rand::thread_rng().gen_range(0..=self.base_ms);
        let d = capped.saturating_add(jitter).min(self.max_ms);
        self.attempt = self.attempt.saturating_add(1);
        d
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

// --- Software watchdog (heartbeat) ---

#[derive(Clone)]
pub struct Watchdog {
    last_beat_ms: Arc<AtomicU64>,
    stale_after_ms: u64,
}

impl Watchdog {
    pub fn new(stale_after_ms: u64, initial_now_ms: u64) -> Self {
        Self {
            last_beat_ms: Arc::new(AtomicU64::new(initial_now_ms)),
            stale_after_ms,
        }
    }

    pub fn tick(&self, now_ms: u64) {
        self.last_beat_ms.store(now_ms, Ordering::Relaxed);
    }

    pub fn is_stale(&self, now_ms: u64) -> bool {
        let last = self.last_beat_ms.load(Ordering::Relaxed);
        now_ms.saturating_sub(last) > self.stale_after_ms
    }

    pub fn last_beat_ms(&self) -> u64 {
        self.last_beat_ms.load(Ordering::Relaxed)
    }
}
