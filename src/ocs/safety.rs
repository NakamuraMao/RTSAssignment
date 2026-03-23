//! OCS: safety rules, observation events from sensors/receiver,
//! 3 consecutive Miss → alert, recovery timeout → mission abort (shutdown).
//!
//! Specification: "recovery time is less than 200ms, otherwise mission abort". Millisecond rounding and 10ms timer delay to absorb.
//! **internal abort threshold is 190ms**. Check after each Safety event, not just timer-based.
//!
//! `duration_ms` for recovery time is only `TimestampMs` difference, so it can be 0 within the same millisecond.
//! Keep alert start with `Instant` and measure elapsed time with millisecond ceiling during recovery.

use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};

use super::time;
use super::types::{RecoveryTimeMetric, SafetyEvent, SafetyEventKind, SensorId, TimestampMs};

/// Absolute maximum for mission abort (ms) — for reporting / `logs/` snapshot.
pub const RECOVERY_SPEC_MAX_MS: u64 = 200;
/// Mission Abort if elapsed after alert start exceeds this threshold (&lt;200ms margin for specification).
pub const RECOVERY_ABORT_THRESHOLD_MS: u64 = 190;
const CHECK_INTERVAL_MS: u64 = 10;

/// number of sensors (SensorId 0, 1, 2)
const NUM_SENSORS: usize = 3;

fn sensor_index(sensor_id: SensorId) -> usize {
    (sensor_id.0 as usize).min(NUM_SENSORS.saturating_sub(1))
}

/// elapsed from monotonic clock to recovery after 3 consecutive Miss (ms, ceiling, minimum 1ms).
fn recovery_duration_ms(alert_started_at: Instant) -> u64 {
    let elapsed = alert_started_at.elapsed();
    let ceil_ms = (elapsed.as_nanos().saturating_add(999_999) / 1_000_000) as u64;
    ceil_ms.max(1)
}

/// Mission Abort if alerting and `now - alert_start >= RECOVERY_ABORT_THRESHOLD_MS`.
async fn mission_abort_if_recovery_timeout(
    alert_sensors: &[bool; NUM_SENSORS],
    alert_start_ms: &[Option<u64>; NUM_SENSORS],
    shutdown_tx: &mpsc::Sender<()>,
) -> bool {
    let now = time::now_ms().0;
    for idx in 0..NUM_SENSORS {
        if alert_sensors[idx] {
            if let Some(start) = alert_start_ms[idx] {
                if now.saturating_sub(start) >= RECOVERY_ABORT_THRESHOLD_MS {
                    crate::ocs_ts_eprintln!(
                        "[safety] mission_abort reason=RecoveryTimeout sensor_id={} alert_start={} now={} spec_max_ms={} abort_threshold_ms={}",
                        idx,
                        start,
                        now,
                        RECOVERY_SPEC_MAX_MS,
                        RECOVERY_ABORT_THRESHOLD_MS
                    );
                    if shutdown_tx.send(()).await.is_err() {
                        crate::ocs_ts_eprintln!(
                            "[safety] shutdown_tx send failed (receiver dropped)"
                        );
                    }
                    return true;
                }
            }
        }
    }
    false
}

/// receive observation event receiver, loop recv.
/// Rule: 3 consecutive Miss → alert, no Recovered within threshold → shutdown.
/// Send RecoveryTimeMetric to metrics_tx when Recovered (for evaluation by benchmarking).
pub async fn run_safety(
    mut safety_rx: mpsc::Receiver<SafetyEvent>,
    shutdown_tx: mpsc::Sender<()>,
    alert_tx: watch::Sender<[bool; NUM_SENSORS]>,
    metrics_tx: mpsc::Sender<RecoveryTimeMetric>,
) {
    let mut consecutive_miss: [u32; NUM_SENSORS] = [0; NUM_SENSORS];
    let mut alert_sensors: [bool; NUM_SENSORS] = [false; NUM_SENSORS];
    let mut alert_start_ms: [Option<u64>; NUM_SENSORS] = [None; NUM_SENSORS];
    // set only when the 3rd Miss alerts (duration_ms is based on Instant).
    let mut alert_started_at: [Option<Instant>; NUM_SENSORS] = [None; NUM_SENSORS];
    let mut last_sent_alert: [bool; NUM_SENSORS] = [false; NUM_SENSORS];

    let mut check_interval = tokio::time::interval(Duration::from_millis(CHECK_INTERVAL_MS));
    check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            ev = safety_rx.recv() => {
                match ev {
                    None => break,
                    Some(ev) => {
                        let sid = ev.sensor_id;
                        match ev.kind {
                            SafetyEventKind::Miss => {
                                if let Some(sid) = sid {
                                    let idx = sensor_index(sid);
                                    consecutive_miss[idx] = consecutive_miss[idx].saturating_add(1);
                                    if consecutive_miss[idx] >= 3 {
                                        alert_sensors[idx] = true;
                                        // record alert start only once at the 3rd Miss (don't overwrite after 4th).
                                        if consecutive_miss[idx] == 3 {
                                            alert_start_ms[idx] = Some(ev.at.0);
                                            alert_started_at[idx] = Some(Instant::now());
                                        }
                                        if last_sent_alert != alert_sensors {
                                            let _ = alert_tx.send(alert_sensors);
                                            last_sent_alert = alert_sensors;
                                        }
                                        crate::ocs_ts_eprintln!(
                                            "[safety] alert sensor_id={} consecutive_miss=3 event_at={}",
                                            sid.0,
                                            ev.at.0
                                        );
                                    }
                                }
                            }
                            SafetyEventKind::Recovered => {
                                if let Some(sid) = sid {
                                    let idx = sensor_index(sid);
                                    if alert_sensors[idx] {
                                        if let (Some(alert_start), Some(started_at)) =
                                            (alert_start_ms[idx], alert_started_at[idx])
                                        {
                                            let duration_ms = recovery_duration_ms(started_at);
                                            let metric = RecoveryTimeMetric {
                                                sensor_id: sid,
                                                fault_at: TimestampMs(alert_start),
                                                recovered_at: ev.at,
                                                duration_ms,
                                            };
                                            if metrics_tx.try_send(metric).is_err() {
                                                crate::ocs_ts_eprintln!(
                                                    "[safety] metrics_tx full, recovery metric dropped"
                                                );
                                            }
                                        }
                                        consecutive_miss[idx] = 0;
                                        alert_sensors[idx] = false;
                                        alert_start_ms[idx] = None;
                                        alert_started_at[idx] = None;
                                        if last_sent_alert != alert_sensors {
                                            let _ = alert_tx.send(alert_sensors);
                                            last_sent_alert = alert_sensors;
                                        }
                                        crate::ocs_ts_eprintln!(
                                            "[safety] recovered sensor_id={} event_at={}",
                                            sid.0,
                                            ev.at.0
                                        );
                                    }
                                }
                            }
                            SafetyEventKind::Drop
                            | SafetyEventKind::Delay
                            | SafetyEventKind::JitterExceeded
                            | SafetyEventKind::InvalidReading => {
                                if let Some(sid) = sid {
                                    let idx = sensor_index(sid);
                                    consecutive_miss[idx] = 0;
                                }
                            }
                        }
                        if mission_abort_if_recovery_timeout(
                            &alert_sensors,
                            &alert_start_ms,
                            &shutdown_tx,
                        )
                        .await
                        {
                            return;
                        }
                    }
                }
            }
            _ = check_interval.tick() => {
                if mission_abort_if_recovery_timeout(
                    &alert_sensors,
                    &alert_start_ms,
                    &shutdown_tx,
                )
                .await
                {
                    return;
                }
            }
        }
    }
}
