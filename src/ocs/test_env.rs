//! Optional OCS integration-test hooks (env vars). Default: all disabled — normal behavior unchanged.

use std::sync::OnceLock;

/// `true` / `1` (case-insensitive) enables the flag; unset or anything else: disabled.
pub fn env_flag_true(name: &str) -> bool {
    std::env::var(name)
        .map(|v| {
            let v = v.trim();
            v.eq_ignore_ascii_case("true") || v == "1"
        })
        .unwrap_or(false)
}

pub fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(default)
}

static LOGGED: OnceLock<()> = OnceLock::new();

pub fn log_active_test_modes_once() {
    LOGGED.get_or_init(|| {
        let stall = force_downlink_stall();
        let init = force_init_timeout();
        let slow = force_slow_recovery();
        if stall || init || slow {
            crate::ocs_ts_eprintln!(
                "[test_env] active ocs_test_force_downlink_stall={} ocs_test_force_init_timeout={} ocs_test_force_slow_recovery={}",
                stall,
                init,
                slow
            );
        }
    });
}

pub fn force_downlink_stall() -> bool {
    env_flag_true("OCS_TEST_FORCE_DOWNLINK_STALL")
}

/// Sleep duration after first downlink dequeue (before processing) to starve the consumer and fill the queue.
pub fn downlink_stall_ms() -> u64 {
    env_u64("OCS_TEST_DOWNLINK_STALL_MS", 5_000).max(1)
}

pub fn force_init_timeout() -> bool {
    env_flag_true("OCS_TEST_FORCE_INIT_TIMEOUT")
}

/// Extra delay (ms) before UDP setup so the 5ms init timeout fires.
pub fn init_timeout_extra_ms() -> u64 {
    env_u64("OCS_TEST_INIT_TIMEOUT_EXTRA_MS", 6).max(1)
}

pub fn force_slow_recovery() -> bool {
    env_flag_true("OCS_TEST_FORCE_SLOW_RECOVERY")
}

/// Artificial delay before first thermal `Recovered` after benchmark fault clear (mission abort if > ~190ms).
pub fn slow_recovery_delay_ms() -> u64 {
    env_u64("OCS_TEST_SLOW_RECOVERY_DELAY_MS", 250).max(1)
}
