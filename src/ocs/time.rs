//! startup reference time and elapsed ms. for consistency with TimestampMs for measurements/logging.

use std::sync::OnceLock;
use std::time::Instant;

use super::types::TimestampMs;

static START: OnceLock<Instant> = OnceLock::new();

/// set the startup reference time. usually called only once at the beginning of runtime.
/// if [now_ms] is called without setting, it is set at that time.
pub fn init_start_time() {
    START.get_or_init(Instant::now);
}

/// elapsed milliseconds from startup. the reference time is determined at the first call.
#[inline]
pub fn now_ms() -> TimestampMs {
    let start = START.get_or_init(Instant::now);
    TimestampMs(start.elapsed().as_millis() as u64)
}
