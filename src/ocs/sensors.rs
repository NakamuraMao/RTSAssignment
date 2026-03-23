//! OCS: simulation of 3 types of sensors, mpsc transmission, sensor-side latency / drift / jitter,
//! sending Safety events for observation facts. No Miss is sent (receiver detects, safety counts 3 consecutive).
//!
//! **task separation**: Thermal / IMU / Power are each run in a **separate `tokio` task** (individually `spawn`ed by `runtime`),
//! avoid a single `select!` super loop. send to a common `mpsc::Sender` (MPSC) with `try_send`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use super::time;
use crate::sensor_values::SensorQuantity;

use super::types::{
    BufferDropEvent, DataPriority, DropReason, FaultInjectionState, FaultKind, SafetyEvent,
    SafetyEventKind, SensorId, SensorSample,
};

// -----------------------------------------------------------------------------
// sensor definition (period / DataPriority)
// -----------------------------------------------------------------------------

/// Miss determination (`runtime::SENSOR_PERIOD_MS`) must be the same value.
pub const THERMAL_PERIOD_MS: u64 = 10;
pub const IMU_PERIOD_MS: u64 = 15;
pub const POWER_PERIOD_MS: u64 = 500;

const JITTER_THRESHOLD_MS: u64 = 1; // jitter threshold for Critical (Thermal)
const LATENCY_THRESHOLD_MS: u64 = 200; // SafetyEvent::Delay if exceeded
/// rate limit for priority drop logs (ms). log at most once in this interval.
const PRIORITY_DROP_LOG_INTERVAL_MS: u64 = 100;

fn thermal_value() -> f64 {
    20.0 + (time::now_ms().0 % 100) as f64 * 0.01
}
fn imu_value() -> f64 {
    (time::now_ms().0 % 1000) as f64 * 0.001
}
fn power_value() -> f64 {
    3.7 - (time::now_ms().0 % 10000) as f64 * 0.00001
}

// -----------------------------------------------------------------------------
// sensor state (jitter: last time, next deadline, sequence)
// -----------------------------------------------------------------------------

struct SensorState {
    last_sample_ms: u64,
    /// next sample deadline after the last sample (= previous actual + period). no phase accumulation error.
    next_deadline_ms: u64,
    sequence: u64,
}

impl SensorState {
    fn new() -> Self {
        Self {
            last_sample_ms: 0,
            next_deadline_ms: 0,
            sequence: 0,
        }
    }
}

// -----------------------------------------------------------------------------
// 3 tasks: each sensor has a loop (`tokio::time::interval`)
// -----------------------------------------------------------------------------

/// Thermal (Critical). single task for periodic sampling.
pub async fn run_thermal_sensor(
    sample_tx: mpsc::Sender<SensorSample>,
    safety_tx: mpsc::Sender<SafetyEvent>,
    drop_tx: mpsc::Sender<BufferDropEvent>,
    fault_rx: watch::Receiver<FaultInjectionState>,
    buffer_sample_count: Arc<AtomicUsize>,
    buffer_capacity: usize,
    last_priority_drop_log_ms: Arc<Mutex<u64>>,
) {
    sensor_sampling_loop(
        SensorId(0),
        DataPriority::Critical,
        THERMAL_PERIOD_MS,
        thermal_value,
        JITTER_THRESHOLD_MS,
        sample_tx,
        safety_tx,
        drop_tx,
        fault_rx,
        buffer_sample_count,
        buffer_capacity,
        last_priority_drop_log_ms,
    )
    .await;
}

/// IMU (High).
pub async fn run_imu_sensor(
    sample_tx: mpsc::Sender<SensorSample>,
    safety_tx: mpsc::Sender<SafetyEvent>,
    drop_tx: mpsc::Sender<BufferDropEvent>,
    fault_rx: watch::Receiver<FaultInjectionState>,
    buffer_sample_count: Arc<AtomicUsize>,
    buffer_capacity: usize,
    last_priority_drop_log_ms: Arc<Mutex<u64>>,
) {
    sensor_sampling_loop(
        SensorId(1),
        DataPriority::High,
        IMU_PERIOD_MS,
        imu_value,
        0,
        sample_tx,
        safety_tx,
        drop_tx,
        fault_rx,
        buffer_sample_count,
        buffer_capacity,
        last_priority_drop_log_ms,
    )
    .await;
}

/// Power (Normal).
pub async fn run_power_sensor(
    sample_tx: mpsc::Sender<SensorSample>,
    safety_tx: mpsc::Sender<SafetyEvent>,
    drop_tx: mpsc::Sender<BufferDropEvent>,
    fault_rx: watch::Receiver<FaultInjectionState>,
    buffer_sample_count: Arc<AtomicUsize>,
    buffer_capacity: usize,
    last_priority_drop_log_ms: Arc<Mutex<u64>>,
) {
    sensor_sampling_loop(
        SensorId(2),
        DataPriority::Normal,
        POWER_PERIOD_MS,
        power_value,
        0,
        sample_tx,
        safety_tx,
        drop_tx,
        fault_rx,
        buffer_sample_count,
        buffer_capacity,
        last_priority_drop_log_ms,
    )
    .await;
}

async fn sensor_sampling_loop(
    sensor_id: SensorId,
    data_priority: DataPriority,
    period_ms: u64,
    value_fn: fn() -> f64,
    jitter_threshold_ms: u64,
    sample_tx: mpsc::Sender<SensorSample>,
    safety_tx: mpsc::Sender<SafetyEvent>,
    drop_tx: mpsc::Sender<BufferDropEvent>,
    fault_rx: watch::Receiver<FaultInjectionState>,
    buffer_sample_count: Arc<AtomicUsize>,
    buffer_capacity: usize,
    last_priority_drop_log_ms: Arc<Mutex<u64>>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(period_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut state = SensorState::new();

    loop {
        interval.tick().await;
        let mut log_guard = last_priority_drop_log_ms.lock().expect("priority_drop_log lock");
        let tick_result = sample_tick(
            sensor_id,
            data_priority,
            period_ms,
            value_fn,
            &mut state,
            jitter_threshold_ms,
            &sample_tx,
            &safety_tx,
            &drop_tx,
            &fault_rx,
            &buffer_sample_count,
            buffer_capacity,
            &mut *log_guard,
        );
        drop(log_guard);
        if tick_result.is_err() {
            break;
        }
    }
}

/// 1 sample: measure from read start to try_send. drop & log & SafetyEvent::Drop if mpsc is full.
/// fault injection: return if Delayed.
/// return value: exit with Err(Closed) if the other side is closed.
/// (sync function: no `await` inside, no `MutexGuard` held across `await`).
fn sample_tick(
    sensor_id: SensorId,
    data_priority: DataPriority,
    period_ms: u64,
    value_fn: fn() -> f64,
    state: &mut SensorState,
    jitter_threshold_ms: u64,
    sample_tx: &mpsc::Sender<SensorSample>,
    safety_tx: &mpsc::Sender<SafetyEvent>,
    drop_tx: &mpsc::Sender<BufferDropEvent>,
    fault_rx: &watch::Receiver<FaultInjectionState>,
    buffer_sample_count: &Arc<AtomicUsize>,
    buffer_capacity: usize,
    last_priority_drop_log_ms: &mut u64,
) -> Result<(), ()> {
    let t_read_start = time::now_ms();
    let fault = fault_rx.borrow().clone();

    // Delayed: suppress sending for the target sensor. do not update the state related to sending.
    // (receiver can reliably detect "not coming" by last_seen, so the state on the sensors side does not affect the receiver).
    if fault.active
        && fault.sensor_id == Some(sensor_id)
        && fault.kind == FaultKind::Delayed
    {
        return Ok(());
    }

    let actual_ms = t_read_start.0;
    // difference between the next deadline (next_deadline_ms) synchronized with the previous actual = only the delay for this period (no chain accumulation).
    let drift_ms = if state.sequence > 0 {
        actual_ms as i64 - state.next_deadline_ms as i64
    } else {
        0
    };
    if state.sequence > 0 {
        crate::ocs_ts_eprintln!(
            "[sensors] drift sensor_id={} next_deadline_ms={} actual_ms={} drift_ms={}",
            sensor_id.0,
            state.next_deadline_ms,
            actual_ms,
            drift_ms
        );
    }

    // priority check before sending: Low/Normal and backlog >= 80% then drop without sending (update state).
    let threshold = buffer_capacity * 80 / 100;
    if (data_priority == DataPriority::Low || data_priority == DataPriority::Normal)
        && buffer_sample_count.load(Ordering::Relaxed) >= threshold
    {
        state.sequence += 1;
        state.last_sample_ms = actual_ms;
        state.next_deadline_ms = actual_ms.saturating_add(period_ms);
        let now_ms = time::now_ms().0;
        if *last_priority_drop_log_ms == 0
            || now_ms.saturating_sub(*last_priority_drop_log_ms) >= PRIORITY_DROP_LOG_INTERVAL_MS
        {
            crate::ocs_ts_eprintln!(
                "[sensors] drop priority buffer_high sensor_id={} event_at={}",
                sensor_id.0,
                now_ms
            );
            *last_priority_drop_log_ms = now_ms;
        }
        return Ok(());
    }

    let value = value_fn();

    let quantity = match SensorQuantity::try_from_raw(sensor_id.0, value) {
        Ok(q) => q,
        Err(e) => {
            let at = time::now_ms();
            crate::ocs_ts_eprintln!(
                "[sensors] invalid_reading sensor_id={} {} event_at={}",
                sensor_id.0,
                e,
                at.0
            );
            let _ = safety_tx.try_send(SafetyEvent {
                at,
                kind: SafetyEventKind::InvalidReading,
                sensor_id: Some(sensor_id),
                measured_value_ms: None,
            });
            return Ok(());
        }
    };

    state.sequence += 1;
    let sample = SensorSample {
        sensor_id,
        data_priority,
        value: quantity,
        read_at: t_read_start,
        sequence: state.sequence,
    };

    let send_result = sample_tx.try_send(sample);
    let t_after_send = time::now_ms();
    let sensor_side_latency_ms = t_after_send.0.saturating_sub(t_read_start.0);

    if sensor_side_latency_ms > LATENCY_THRESHOLD_MS {
        let _ = safety_tx.try_send(SafetyEvent {
            at: t_after_send,
            kind: SafetyEventKind::Delay,
            sensor_id: Some(sensor_id),
            measured_value_ms: Some(sensor_side_latency_ms),
        });
    }

    match send_result {
        Ok(()) => {
            buffer_sample_count.fetch_add(1, Ordering::Relaxed);
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            let at = time::now_ms();
            let drop_ev = BufferDropEvent {
                at,
                sensor_id: Some(sensor_id),
                reason: DropReason::MpscFull,
            };
            let _ = drop_tx.try_send(drop_ev);
            let _ = safety_tx.try_send(SafetyEvent {
                at,
                kind: SafetyEventKind::Drop,
                sensor_id: Some(sensor_id),
                measured_value_ms: None,
            });
            crate::ocs_ts_eprintln!(
                "[sensors] drop mpsc_full sensor_id={} event_at={}",
                sensor_id.0,
                at.0
            );
        }
        Err(mpsc::error::TrySendError::Closed(_)) => return Err(()),
    }

    if state.last_sample_ms > 0 && jitter_threshold_ms > 0 {
        let actual_interval_ms = actual_ms.saturating_sub(state.last_sample_ms);
        let jitter_ms = actual_interval_ms.saturating_sub(period_ms);
        if jitter_ms > jitter_threshold_ms {
            let at = time::now_ms();
            let _ = safety_tx.try_send(SafetyEvent {
                at,
                kind: SafetyEventKind::JitterExceeded,
                sensor_id: Some(sensor_id),
                measured_value_ms: Some(jitter_ms),
            });
            crate::ocs_ts_eprintln!(
                "[sensors] jitter_exceeded sensor_id={} period={} actual_interval={} jitter_ms={}",
                sensor_id.0,
                period_ms,
                actual_interval_ms,
                jitter_ms
            );
        }
    }

    state.last_sample_ms = actual_ms;
    state.next_deadline_ms = actual_ms.saturating_add(period_ms);

    Ok(())
}
