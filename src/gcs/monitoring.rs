//! GCS monitoring: collect metrics (jitter, backlog, drift, deadlines, load, recovery, interlock latency), snapshot_metrics() for report.

use std::sync::{Mutex, OnceLock};

use crate::gcs::time;
use crate::gcs::types::{InterlockLatencyMetric, RecoveryMetric};

/// Internal aggregation state for report metrics (thread-safe).
#[derive(Default)]
struct MonitoringState {
    // Jitter (from consecutive notify_uplink_dispatch)
    last_execute_at_ms: Option<u64>,
    last_actual_dispatch_at_ms: Option<u64>,
    jitter_last_ms: Option<u64>,
    jitter_max_ms: Option<u64>,
    jitter_count: u64,
    jitter_sum_ms: u64,
    // Telemetry backlog
    backlog_last: Option<usize>,
    backlog_max: Option<usize>,
    backlog_at_ms: Option<u64>,
    // Drift (scheduler dispatch)
    drift_last_ms: Option<i64>,
    drift_count: u64,
    drift_sum_ms: i64,
    drift_max_ms: Option<i64>,
    drift_abs_max_ms: Option<u64>,
    // Missed deadlines
    deadline_miss_count: u64,
    deadline_miss_last_command_id: Option<u64>,
    deadline_miss_last_actual_ms: Option<u64>,
    // Fault recovery
    recovery_last: Option<RecoveryMetric>,
    recovery_count: u64,
    recovery_max_ms: Option<u64>,
    recovery_over_100ms_count: u64,
    // Interlock latency
    interlock_last_ms: Option<u64>,
    interlock_count: u64,
    interlock_max_ms: Option<u64>,
    interlock_sum_ms: u64,
    // System load
    load_last_percent: Option<f64>,
    load_max_percent: Option<f64>,
    load_count: u64,
    load_sum_percent: f64,
}

static STATE: OnceLock<Mutex<MonitoringState>> = OnceLock::new();

fn state() -> &'static Mutex<MonitoringState> {
    STATE.get_or_init(|| Mutex::new(MonitoringState::default()))
}

#[cfg(test)]
impl MonitoringState {
    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Resets aggregated metrics (test only). Call before tests that assert on snapshot values.
#[cfg(test)]
pub fn reset_metrics_for_test() {
    if let Ok(mut g) = state().lock() {
        g.reset();
    }
}

pub fn notify_rx_latency(sequence: u64, latency_ms: u64) {
    eprintln!(
        "ts_ms={} event=monitoring_rx_latency seq={} latency_ms={}",
        time::now_ms().0,
        sequence,
        latency_ms
    );
}

pub fn notify_rx_drift(
    sequence: u64,
    expected_interval_ms: u64,
    actual_interval_ms: u64,
    drift_ms: i64,
) {
    eprintln!(
        "ts_ms={} event=monitoring_rx_drift seq={} expected_interval_ms={} actual_interval_ms={} drift_ms={}",
        time::now_ms().0,
        sequence,
        expected_interval_ms,
        actual_interval_ms,
        drift_ms
    );
}

pub fn notify_decode_budget_violation(elapsed_ms: u64, budget_ms: u64) {
    eprintln!(
        "ts_ms={} event=monitoring_decode_budget_violation elapsed_ms={} budget_ms={}",
        time::now_ms().0,
        elapsed_ms,
        budget_ms
    );
}

pub fn notify_rerequest_gap(expected_seq: u64, received_seq: u64, missing_from: u64, missing_to: u64) {
    eprintln!(
        "ts_ms={} event=monitoring_rerequest_gap expected_seq={} received_seq={} missing_from={} missing_to={}",
        time::now_ms().0,
        expected_seq,
        received_seq,
        missing_from,
        missing_to
    );
}

pub fn notify_rerequest_decode_failure(reason: &'static str) {
    eprintln!(
        "ts_ms={} event=monitoring_rerequest_decode_failure reason={}",
        time::now_ms().0,
        reason
    );
}

pub fn notify_contact_lost(consecutive_failures: u32) {
    eprintln!(
        "ts_ms={} event=monitoring_contact_lost consecutive_failures={}",
        time::now_ms().0,
        consecutive_failures
    );
}

pub fn notify_contact_restored() {
    eprintln!(
        "ts_ms={} event=monitoring_contact_restored",
        time::now_ms().0
    );
}

/// Recovery duration after fault (for reporting).
pub fn notify_recovery(metric: RecoveryMetric) {
    let duration_ms = metric.duration_ms;
    let fault_at = metric.fault_at.0;
    let recovered_at = metric.recovered_at.0;
    if let Ok(mut s) = state().lock() {
        s.recovery_last = Some(metric);
        s.recovery_count = s.recovery_count.saturating_add(1);
        s.recovery_max_ms = Some(s.recovery_max_ms.map_or(duration_ms, |m| m.max(duration_ms)));
        if duration_ms > 100 {
            s.recovery_over_100ms_count = s.recovery_over_100ms_count.saturating_add(1);
        }
    }
    eprintln!(
        "ts_ms={} event=monitoring_recovery fault_at={} recovered_at={} duration_ms={}",
        time::now_ms().0,
        fault_at,
        recovered_at,
        duration_ms
    );
}

/// Interlock latency: fault message received on GCS → interlock state published (`fault_state_tx` send).
pub fn notify_interlock_latency(metric: InterlockLatencyMetric) {
    if let Ok(mut s) = state().lock() {
        s.interlock_last_ms = Some(metric.latency_ms);
        s.interlock_count = s.interlock_count.saturating_add(1);
        s.interlock_max_ms = Some(
            s.interlock_max_ms
                .map_or(metric.latency_ms, |m| m.max(metric.latency_ms)),
        );
        s.interlock_sum_ms = s.interlock_sum_ms.saturating_add(metric.latency_ms);
    }
    eprintln!(
        "ts_ms={} event=monitoring_interlock_latency at={} latency_ms={}",
        time::now_ms().0,
        metric.at.0,
        metric.latency_ms
    );
}

/// Critical alert: fault still non-Nominal after 100ms. Increments `recovery_over_100ms_count` only;
/// [`notify_recovery`] (and `recovery_count`) run when fault clears to Nominal—so the snapshot can show
/// `recovery_over_100ms_count > 0` with `recovery_count == 0` if the session ends before Nominal recovery.
pub fn notify_critical_alert_recovery_timeout(fault_detected_at_ms: u64, elapsed_ms: u64) {
    if let Ok(mut s) = state().lock() {
        s.recovery_over_100ms_count = s.recovery_over_100ms_count.saturating_add(1);
    }
    eprintln!(
        "ts_ms={} event=monitoring_critical_alert_recovery_timeout fault_detected_at_ms={} elapsed_ms={}",
        time::now_ms().0,
        fault_detected_at_ms,
        elapsed_ms
    );
}

/// Uplink dispatch (for jitter: actual_dispatch_at vs execute_at). Fixed args, no heap.
pub fn notify_uplink_dispatch(
    command_id: u64,
    execute_at_ms: u64,
    actual_dispatch_at_ms: u64,
) {
    if let Ok(mut s) = state().lock() {
        if let (Some(le), Some(la)) = (s.last_execute_at_ms, s.last_actual_dispatch_at_ms) {
            let expected_interval = execute_at_ms.saturating_sub(le);
            let actual_interval = actual_dispatch_at_ms.saturating_sub(la);
            let jitter_signed = actual_interval as i64 - expected_interval as i64;
            let jitter_ms = jitter_signed.unsigned_abs();
            s.jitter_last_ms = Some(jitter_ms);
            s.jitter_max_ms = Some(s.jitter_max_ms.map_or(jitter_ms, |m| m.max(jitter_ms)));
            s.jitter_count = s.jitter_count.saturating_add(1);
            s.jitter_sum_ms = s.jitter_sum_ms.saturating_add(jitter_ms);
        }
        s.last_execute_at_ms = Some(execute_at_ms);
        s.last_actual_dispatch_at_ms = Some(actual_dispatch_at_ms);
    }
    eprintln!(
        "ts_ms={} event=monitoring_uplink_dispatch command_id={} execute_at_ms={} actual_dispatch_at_ms={}",
        time::now_ms().0,
        command_id,
        execute_at_ms,
        actual_dispatch_at_ms
    );
}

/// Scheduling drift: actual_dispatch_at - execute_at (fixed args, no heap).
pub fn notify_scheduling_drift(execute_at_ms: u64, actual_dispatch_at_ms: u64, drift_ms: i64) {
    if let Ok(mut s) = state().lock() {
        s.drift_last_ms = Some(drift_ms);
        s.drift_count = s.drift_count.saturating_add(1);
        s.drift_sum_ms = s.drift_sum_ms.saturating_add(drift_ms);
        s.drift_max_ms = Some(s.drift_max_ms.map_or(drift_ms, |m| m.max(drift_ms)));
        let abs = drift_ms.unsigned_abs();
        s.drift_abs_max_ms = Some(s.drift_abs_max_ms.map_or(abs, |m| m.max(abs)));
    }
    eprintln!(
        "ts_ms={} event=monitoring_scheduling_drift execute_at_ms={} actual_dispatch_at_ms={} drift_ms={}",
        time::now_ms().0,
        execute_at_ms,
        actual_dispatch_at_ms,
        drift_ms
    );
}

/// Deadline miss: actual_dispatch_at > deadline (fixed args, no heap).
pub fn notify_deadline_miss(command_id: u64, deadline_ms: u64, actual_dispatch_at_ms: u64) {
    if let Ok(mut s) = state().lock() {
        s.deadline_miss_count = s.deadline_miss_count.saturating_add(1);
        s.deadline_miss_last_command_id = Some(command_id);
        s.deadline_miss_last_actual_ms = Some(actual_dispatch_at_ms);
    }
    eprintln!(
        "ts_ms={} event=monitoring_deadline_miss command_id={} deadline_ms={} actual_dispatch_at_ms={}",
        time::now_ms().0,
        command_id,
        deadline_ms,
        actual_dispatch_at_ms
    );
}

/// Telemetry backlog: unprocessed packets (decode/processing wait). Caller: telemetry.rs.
pub fn notify_telemetry_backlog(queue_len: usize, at_ms: u64) {
    if let Ok(mut s) = state().lock() {
        s.backlog_last = Some(queue_len);
        s.backlog_max = Some(s.backlog_max.map_or(queue_len, |m| m.max(queue_len)));
        s.backlog_at_ms = Some(at_ms);
    }
    eprintln!(
        "ts_ms={} event=monitoring_telemetry_backlog queue_len={} at_ms={}",
        time::now_ms().0,
        queue_len,
        at_ms
    );
}

/// System load (optional). Caller: runtime or dedicated task. at_ms for aggregation.
pub fn notify_system_load(load_percent: f64, at_ms: u64) {
    if let Ok(mut s) = state().lock() {
        let p = load_percent.clamp(0.0, 100.0);
        s.load_last_percent = Some(p);
        s.load_max_percent = Some(s.load_max_percent.map_or(p, |m| m.max(p)));
        s.load_count = s.load_count.saturating_add(1);
        s.load_sum_percent += p;
    }
    eprintln!(
        "ts_ms={} event=monitoring_system_load load_percent={:.2} at_ms={}",
        time::now_ms().0,
        load_percent,
        at_ms
    );
}

/// Returns a report-ready snapshot of all performance metrics (paste into final report).
pub fn snapshot_metrics() -> String {
    let s = match state().lock() {
        Ok(guard) => guard,
        Err(e) => e.into_inner(),
    };
    let now_ms = time::now_ms().0;

    let jitter_last = s
        .jitter_last_ms
        .map(|v| v.to_string())
        .unwrap_or_else(|| "n/a".to_string());
    let jitter_max = s
        .jitter_max_ms
        .map(|v| v.to_string())
        .unwrap_or_else(|| "n/a".to_string());
    let jitter_avg = if s.jitter_count > 0 {
        format!("{:.2}", s.jitter_sum_ms as f64 / s.jitter_count as f64)
    } else {
        "n/a".to_string()
    };

    let backlog_last = s
        .backlog_last
        .map(|v| v.to_string())
        .unwrap_or_else(|| "n/a".to_string());
    let backlog_max = s
        .backlog_max
        .map(|v| v.to_string())
        .unwrap_or_else(|| "n/a".to_string());

    let drift_last = s
        .drift_last_ms
        .map(|v| v.to_string())
        .unwrap_or_else(|| "n/a".to_string());
    let drift_max = s
        .drift_max_ms
        .map(|v| v.to_string())
        .unwrap_or_else(|| "n/a".to_string());
    let drift_avg = if s.drift_count > 0 {
        format!("{:.2}", s.drift_sum_ms as f64 / s.drift_count as f64)
    } else {
        "n/a".to_string()
    };
    let drift_abs_max = s
        .drift_abs_max_ms
        .map(|v| v.to_string())
        .unwrap_or_else(|| "n/a".to_string());

    let missed_deadlines = s.deadline_miss_count.to_string();
    let missed_last_cmd = s
        .deadline_miss_last_command_id
        .map(|v| v.to_string())
        .unwrap_or_else(|| "n/a".to_string());

    let recovery_last_ms = s
        .recovery_last
        .as_ref()
        .map(|r| r.duration_ms.to_string())
        .unwrap_or_else(|| "n/a".to_string());
    let recovery_max_ms = s
        .recovery_max_ms
        .map(|v| v.to_string())
        .unwrap_or_else(|| "n/a".to_string());
    let recovery_count = s.recovery_count.to_string();
    let recovery_over_100ms = s.recovery_over_100ms_count.to_string();

    let interlock_last = s
        .interlock_last_ms
        .map(|v| v.to_string())
        .unwrap_or_else(|| "n/a".to_string());
    let interlock_max = s
        .interlock_max_ms
        .map(|v| v.to_string())
        .unwrap_or_else(|| "n/a".to_string());
    let interlock_avg = if s.interlock_count > 0 {
        format!(
            "{:.2}",
            s.interlock_sum_ms as f64 / s.interlock_count as f64
        )
    } else {
        "n/a".to_string()
    };

    let load_last = s
        .load_last_percent
        .map(|v| format!("{:.2}", v))
        .unwrap_or_else(|| "n/a".to_string());
    let load_max = s
        .load_max_percent
        .map(|v| format!("{:.2}", v))
        .unwrap_or_else(|| "n/a".to_string());
    let load_avg = if s.load_count > 0 {
        format!("{:.2}", s.load_sum_percent / s.load_count as f64)
    } else {
        "n/a".to_string()
    };

    format!(
        r#"# GCS Performance Metrics Snapshot (at_ms={})

## Uplink jitter (ms)
- last: {}
- max: {}
- avg: {}
- count: {}

## Telemetry backlog (unprocessed packets)
- last: {}
- max: {}

## Scheduler drift (ms, actual_dispatch_at - execute_at)
- last: {}
- max: {}
- abs_max: {}
- avg: {}
- count: {}

## Missed deadlines
- total: {}
- last_command_id: {}

## Fault recovery
- last_duration_ms: {}
- max_duration_ms: {}
- recovery_count: {}
- recovery_over_100ms_count: {}

## Interlock latency (ms, fault recv → state published to scheduler)
- last: {}
- max: {}
- avg: {}
- count: {}

## System load (%)
- last: {}
- max: {}
- avg: {}
- count: {}
"#,
        now_ms,
        jitter_last,
        jitter_max,
        jitter_avg,
        s.jitter_count,
        backlog_last,
        backlog_max,
        drift_last,
        drift_max,
        drift_abs_max,
        drift_avg,
        s.drift_count,
        missed_deadlines,
        missed_last_cmd,
        recovery_last_ms,
        recovery_max_ms,
        recovery_count,
        recovery_over_100ms,
        interlock_last,
        interlock_max,
        interlock_avg,
        s.interlock_count,
        load_last,
        load_max,
        load_avg,
        s.load_count,
    )
}

#[cfg(test)]
mod tests {
    // These tests share global monitoring state. Run with: cargo test gcs::monitoring -- --test-threads=1
    use super::*;
    use crate::gcs::time;
    use crate::gcs::types::{InterlockLatencyMetric, RecoveryMetric, TimestampMs};

    #[test]
    fn snapshot_metrics_contains_all_sections() {
        time::init_start_time();
        let snap = snapshot_metrics();
        assert!(snap.contains("# GCS Performance Metrics Snapshot"));
        assert!(snap.contains("## Uplink jitter"));
        assert!(snap.contains("## Telemetry backlog"));
        assert!(snap.contains("## Scheduler drift"));
        assert!(snap.contains("## Missed deadlines"));
        assert!(snap.contains("## Fault recovery"));
        assert!(snap.contains("## Interlock latency"));
        assert!(snap.contains("## System load"));
    }

    #[test]
    fn snapshot_metrics_aggregates_telemetry_backlog() {
        time::init_start_time();
        reset_metrics_for_test();
        notify_telemetry_backlog(2, 1000);
        notify_telemetry_backlog(5, 2000);
        let snap = snapshot_metrics();
        assert!(snap.contains("last: 5"));
        assert!(snap.contains("max: 5"));
    }

    #[test]
    fn snapshot_metrics_aggregates_drift() {
        time::init_start_time();
        reset_metrics_for_test();
        notify_scheduling_drift(100, 105, 5);
        notify_scheduling_drift(200, 198, -2);
        let snap = snapshot_metrics();
        assert!(snap.contains("last: -2") || snap.contains("last:-2"));
        assert!(snap.contains("total: 0") || snap.contains("count: 2"));
    }

    #[test]
    fn snapshot_metrics_aggregates_deadline_miss() {
        time::init_start_time();
        reset_metrics_for_test();
        notify_deadline_miss(42, 100, 110);
        let snap = snapshot_metrics();
        assert!(snap.contains("total: 1"));
        assert!(snap.contains("last_command_id: 42"));
    }

    #[test]
    fn snapshot_metrics_aggregates_recovery_and_over_100ms() {
        time::init_start_time();
        reset_metrics_for_test();
        notify_recovery(RecoveryMetric {
            fault_at: TimestampMs(100),
            recovered_at: TimestampMs(250),
            duration_ms: 150,
        });
        let snap = snapshot_metrics();
        assert!(snap.contains("last_duration_ms: 150"));
        assert!(snap.contains("recovery_over_100ms_count: 1"));
    }

    #[test]
    fn snapshot_metrics_aggregates_interlock_latency() {
        time::init_start_time();
        reset_metrics_for_test();
        notify_interlock_latency(InterlockLatencyMetric {
            at: TimestampMs(500),
            latency_ms: 3,
        });
        notify_interlock_latency(InterlockLatencyMetric {
            at: TimestampMs(600),
            latency_ms: 7,
        });
        let snap = snapshot_metrics();
        assert!(snap.contains("last: 7"));
        assert!(snap.contains("max: 7"));
    }
}
