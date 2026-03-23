//! OCS: fault injection (Delayed) every 60s, wait for alert, measure recovery time, evaluate metrics and log.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

use super::safety;
use super::types::{BenchmarkMetrics, FaultInjectionState, FaultKind, RecoveryTimeMetric, SensorId};

const BENCH_INTERVAL_SECS: u64 = 60;
const RECOVERY_WAIT_TIMEOUT_SECS: u64 = 5;
const ALERT_WAIT_TIMEOUT_SECS: u64 = 5;
/// Benchmark OK determination (consistent with `safety::RECOVERY_ABORT_THRESHOLD_MS`).
const RECOVERY_DEADLINE_MS: u64 = safety::RECOVERY_ABORT_THRESHOLD_MS;

/// Every 60 seconds: inject Delayed fault → wait for alert → clear fault → receive RecoveryTimeMetric and evaluate/log only the target sensor.
/// At the start of each cycle, read BenchmarkMetrics, log a summary line of jitter/drift/deadline/cpu, and reset.
pub async fn run_benchmarking(
    fault_tx: watch::Sender<FaultInjectionState>,
    mut metrics_rx: mpsc::Receiver<RecoveryTimeMetric>,
    mut alert_rx: watch::Receiver<[bool; 3]>,
    bench_metrics: Arc<Mutex<BenchmarkMetrics>>,
) {
    let mut cycle: u64 = 0;
    let mut interval = tokio::time::interval(Duration::from_secs(BENCH_INTERVAL_SECS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        cycle += 1;

        // 60s start: read previous cycle metrics, log summary line, and reset
        {
            let mut m = bench_metrics.lock().expect("bench_metrics lock");
            let drift_avg_ms = if m.drift_count == 0 {
                0.0_f64
            } else {
                m.drift_sum_ms as f64 / m.drift_count as f64
            };
            let cpu_util_avg = if m.cpu_util_sum_total_ms == 0 {
                0.0_f64
            } else {
                (m.cpu_util_sum_active_ms as f64 / m.cpu_util_sum_total_ms as f64) * 100.0
            };
            let jitter_max = if m.thermal_max_jitter_ms == i64::MIN {
                "n/a".to_string()
            } else {
                m.thermal_max_jitter_ms.to_string()
            };
            let drift_max = if m.drift_max_ms == i64::MIN {
                "n/a".to_string()
            } else {
                m.drift_max_ms.to_string()
            };
            crate::ocs_ts_eprintln!(
                "[benchmarking] cycle={} eval jitter_max_ms={} drift_max_ms={} drift_avg_ms={:.2} drift_late_starts={} deadline_violations={} cpu_util_avg={:.1}%",
                cycle,
                jitter_max,
                drift_max,
                drift_avg_ms,
                m.drift_late_start_count,
                m.deadline_violations,
                cpu_util_avg
            );
            m.reset();
        }

        let target_sensor = SensorId(0); // Thermal
        let idx = target_sensor.0 as usize;

        // 1. inject Delayed fault
        let fault_on = FaultInjectionState {
            sensor_id: Some(target_sensor),
            kind: FaultKind::Delayed,
            active: true,
        };
        if fault_tx.send(fault_on).is_err() {
            crate::ocs_ts_eprintln!(
                "[benchmarking] cycle={} fault_tx send failed (no receivers)",
                cycle
            );
            continue;
        }
        crate::ocs_ts_eprintln!(
            "[benchmarking] cycle={} fault injected sensor_id={} kind=Delayed",
            cycle,
            target_sensor.0
        );

        // 2. wait for alert (timeout after 5s for "true to become")
        let alert_ok = timeout(
            Duration::from_secs(ALERT_WAIT_TIMEOUT_SECS),
            async {
                while !alert_rx.borrow()[idx] {
                    alert_rx.changed().await.map_err(|_| ())?;
                }
                Ok::<(), ()>(())
            },
        )
        .await;
        let alert_ok = match alert_ok {
            Ok(Ok(())) => true,
            Ok(Err(_)) => {
                crate::ocs_ts_eprintln!(
                    "[benchmarking] cycle={} alert_rx closed",
                    cycle
                );
                false
            }
            Err(_) => {
                crate::ocs_ts_eprintln!(
                    "[benchmarking] cycle={} alert wait timeout ({}s)",
                    cycle,
                    ALERT_WAIT_TIMEOUT_SECS
                );
                false
            }
        };

        if !alert_ok {
            // clear fault and next cycle
            let _ = fault_tx.send(FaultInjectionState::inactive());
            continue;
        }

        // 3. clear fault
        let _ = fault_tx.send(FaultInjectionState::inactive());
        crate::ocs_ts_eprintln!(
            "[benchmarking] cycle={} fault cleared, waiting for recovery metric",
            cycle
        );

        // 4. wait for RecoveryTimeMetric (only accept the target sensor's, ignore others and re-recv)
        let recv_result = timeout(
            Duration::from_secs(RECOVERY_WAIT_TIMEOUT_SECS),
            async {
                loop {
                    match metrics_rx.recv().await {
                        Some(m) if m.sensor_id == target_sensor => break Some(m),
                        Some(m) => {
                            crate::ocs_ts_eprintln!(
                                "[benchmarking] cycle={} ignored metric for sensor_id={} (expected {})",
                                cycle,
                                m.sensor_id.0,
                                target_sensor.0
                            );
                        }
                        None => break None,
                    }
                }
            },
        )
        .await;

        match recv_result {
            Ok(Some(metric)) => {
                let ok = metric.duration_ms <= RECOVERY_DEADLINE_MS;
                {
                    let mut m = bench_metrics.lock().expect("bench_metrics lock");
                    m.recovery_last_duration_ms = Some(metric.duration_ms);
                    m.recovery_count = m.recovery_count.saturating_add(1);
                    m.recovery_sum_duration_ms =
                        m.recovery_sum_duration_ms.saturating_add(metric.duration_ms);
                    m.recovery_max_duration_ms =
                        m.recovery_max_duration_ms.max(metric.duration_ms);
                    if metric.duration_ms > safety::RECOVERY_ABORT_THRESHOLD_MS {
                        m.recovery_over_abort_threshold_count =
                            m.recovery_over_abort_threshold_count.saturating_add(1);
                    }
                }
                crate::ocs_ts_eprintln!(
                    "[benchmarking] cycle={} recovery sensor_id={} duration_ms={} fault_at={} recovered_at={} result={}",
                    cycle,
                    metric.sensor_id.0,
                    metric.duration_ms,
                    metric.fault_at.0,
                    metric.recovered_at.0,
                    if ok { "OK" } else { "NG" }
                );
            }
            Ok(None) => {
                crate::ocs_ts_eprintln!(
                    "[benchmarking] cycle={} metrics_rx closed",
                    cycle
                );
            }
            Err(_) => {
                crate::ocs_ts_eprintln!(
                    "[benchmarking] cycle={} recovery metric timeout ({}s)",
                    cycle,
                    RECOVERY_WAIT_TIMEOUT_SECS
                );
            }
        }
    }
}
