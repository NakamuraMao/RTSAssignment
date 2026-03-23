//! GCS: periodic system-wide CPU usage (%) for `monitoring::notify_system_load`.

use std::time::Duration;

use sysinfo::{CpuRefreshKind, RefreshKind, System};

use crate::gcs::monitoring;
use crate::gcs::time;

const SAMPLE_INTERVAL_SECS: u64 = 1;

/// Refresh CPU twice with a short gap so `global_cpu_usage` reflects a delta (sysinfo semantics).
fn sample_global_cpu_percent() -> f64 {
    let mut sys = System::new_with_specifics(
        RefreshKind::new().with_cpu(CpuRefreshKind::everything()),
    );
    sys.refresh_cpu_all();
    std::thread::sleep(Duration::from_millis(200));
    sys.refresh_cpu_all();
    f64::from(sys.global_cpu_usage())
}

/// Emit `monitoring_system_load` about once per second until the supervisor aborts this task.
pub async fn run_system_load_sampler() {
    let mut interval = tokio::time::interval(Duration::from_secs(SAMPLE_INTERVAL_SECS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let at_ms = time::now_ms().0;
        let load = tokio::task::spawn_blocking(|| sample_global_cpu_percent())
            .await
            .unwrap_or(0.0);
        monitoring::notify_system_load(load, at_ms);
    }
}
