//! GCS time: process start reference and elapsed milliseconds.

use std::sync::OnceLock;
use std::time::Instant;

use super::types::TimestampMs;

static START: OnceLock<Instant> = OnceLock::new();

/// Initialize start time once. Safe to call multiple times.
pub fn init_start_time() {
    START.get_or_init(Instant::now);
}

/// Milliseconds elapsed since process start.
/// If uninitialized, this call initializes the start time.
#[inline]
pub fn now_ms() -> TimestampMs {
    let start = START.get_or_init(Instant::now);
    TimestampMs(start.elapsed().as_millis() as u64)
}
