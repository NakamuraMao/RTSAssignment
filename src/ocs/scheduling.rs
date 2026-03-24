//! OCS: **EDF** (`BinaryHeap` + `Reverse` for shortest absolute deadline priority),
//! Compression is **Generation** (discard old tickets when Pop),
//! Antenna is **Pending flag** (at most 1 in heap, excess is summed externally).
//!
//! Avoid arbitrary updates in `BinaryHeap`, avoid jitter and O(N) with **invalidation at Pop** or **Push suppression**.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use super::health;
use super::time;
use super::types::{
    BenchmarkMetrics, CpuUtilizationMetric, DataPriority, DownlinkPacket, FaultInjectionState,
    SensorSample, TaskKind, TimestampMs,
};

use crate::gcs::types::MAX_PAYLOAD_LEN;

// -----------------------------------------------------------------------------
// period / relative deadline (absolute deadline = release time + rel)
// -----------------------------------------------------------------------------

const THERMAL_PERIOD_MS: u64 = 50;
const COMPRESSION_PERIOD_MS: u64 = 25;
const HEALTH_PERIOD_MS: u64 = 200;
const ANTENNA_PERIOD_MS: u64 = 500;

const THERMAL_DEADLINE_MS: u64 = 10;
const COMPRESSION_DEADLINE_MS: u64 = 20;
const HEALTH_DEADLINE_MS: u64 = 30;
const ANTENNA_DEADLINE_MS: u64 = 50;

const CPU_WINDOW_MS: u64 = 1000;

const PREPARE_DEADLINE_MS: u64 = 30;
const COMPRESSION_OVERLOAD_SKIP_LATE_MS: u64 = 10;
// Coarser cooperative preemption: reduce excessive context-switch overhead
// while still giving high-priority tasks regular chances to run.
const SCHED_LOOP_JOB_BUDGET: usize = 128;
const SCHED_LOOP_STALE_SKIP_BUDGET: usize = 64;

fn data_priority_rank(p: DataPriority) -> u8 {
    match p {
        DataPriority::Critical => 4,
        DataPriority::High => 3,
        DataPriority::Normal => 2,
        DataPriority::Low => 1,
    }
}

/// `try_recv` to empty the buffer while keeping only the latest per sensor, return the most important one (Lazy Cancel).
fn drain_buffer_latest_per_sensor_pick_one(
    buffer_rx: &mut mpsc::Receiver<SensorSample>,
) -> Option<SensorSample> {
    let mut latest: [Option<SensorSample>; 3] = [None, None, None];
    while let Ok(s) = buffer_rx.try_recv() {
        let idx = (s.sensor_id.0 as usize).min(2);
        latest[idx] = Some(s);
    }
    latest
        .into_iter()
        .flatten()
        .max_by_key(|s| data_priority_rank(s.data_priority))
}

/// next job release time (monotonic increase).
struct Periodic {
    period_ms: u64,
    rel_deadline_ms: u64,
    next_release_ms: u64,
}

impl Periodic {
    fn advance(&mut self) {
        self.next_release_ms = self.next_release_ms.saturating_add(self.period_ms);
    }
}

// -----------------------------------------------------------------------------
// EDF heap entry (pop the one with the shortest absolute deadline first with Reverse)
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EdfJob {
    /// absolute deadline of this job (ms from start).
    deadline_abs_ms: u64,
    kind: TaskKind,
    /// Compression only: generation at Push. Discard if not `latest_compression_gen` at Pop.
    compression_gen: u64,
    release_ms: u64,
}

impl Ord for EdfJob {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline_abs_ms
            .cmp(&other.deadline_abs_ms)
            .then_with(|| (self.kind as u8).cmp(&(other.kind as u8)))
    }
}

impl PartialOrd for EdfJob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

type EdfHeap = BinaryHeap<Reverse<EdfJob>>;

// -----------------------------------------------------------------------------
// TaskState (for drift / jitter logging)
// -----------------------------------------------------------------------------

struct TaskState {
    last_start_ms: Option<u64>,
    expected_start_ms: Option<u64>,
    run_count: u64,
}

impl TaskState {
    fn new() -> Self {
        Self {
            last_start_ms: None,
            expected_start_ms: None,
            run_count: 0,
        }
    }
}

fn record_task_start(
    kind: TaskKind,
    period_ms: u64,
    deadline_ms: u64,
    state: &mut TaskState,
    metrics: &Arc<Mutex<BenchmarkMetrics>>,
) -> (TimestampMs, u64) {
    let t_start = time::now_ms();
    let actual_ms = t_start.0;

    if state.expected_start_ms.is_none() {
        state.expected_start_ms = Some(actual_ms);
    }
    let expected_completion_ms = state
        .expected_start_ms
        .unwrap_or(actual_ms)
        .saturating_add(deadline_ms);

    let mut jitter_ms_opt: Option<i64> = None;
    if state.run_count > 0 {
        let expected = state.expected_start_ms.unwrap_or(0);
        let drift_ms = actual_ms as i64 - expected as i64;
        if let Some(last) = state.last_start_ms {
            let actual_interval_ms = actual_ms.saturating_sub(last);
            let jitter_ms = actual_interval_ms as i64 - period_ms as i64;
            jitter_ms_opt = Some(jitter_ms);
        }
        if let Ok(mut m) = metrics.try_lock() {
            m.drift_max_ms = m.drift_max_ms.max(drift_ms);
            m.drift_sum_ms = m.drift_sum_ms.saturating_add(drift_ms);
            m.drift_count = m.drift_count.saturating_add(1);
            if drift_ms > 0 {
                m.drift_late_start_count = m.drift_late_start_count.saturating_add(1);
            }
            if kind == TaskKind::Thermal {
                if let Some(jitter_ms) = jitter_ms_opt {
                    m.thermal_scheduling_drift_ms =
                        m.thermal_scheduling_drift_ms.max(jitter_ms);
                }
            }
        }
    }

    state.run_count += 1;
    state.last_start_ms = Some(actual_ms);
    (t_start, expected_completion_ms)
}

fn record_task_end(
    kind: TaskKind,
    period_ms: u64,
    expected_completion_ms: u64,
    state: &mut TaskState,
    metrics: &Arc<Mutex<BenchmarkMetrics>>,
) -> TimestampMs {
    let t_end = time::now_ms();
    if t_end.0 > expected_completion_ms {
        crate::ocs_ts_eprintln!(
            "[scheduling] deadline_violation kind=CompletionDelay task_kind={:?} at={} expected_completion={}",
            kind,
            t_end.0,
            expected_completion_ms
        );
        if let Ok(mut m) = metrics.try_lock() {
            m.deadline_violations = m.deadline_violations.saturating_add(1);
        }
    }
    let next_expected = state.expected_start_ms.unwrap_or(0).saturating_add(period_ms);
    state.expected_start_ms = Some(next_expected);
    t_end
}

fn sample_to_payload(sample: &SensorSample, fault: &FaultInjectionState) -> ([u8; MAX_PAYLOAD_LEN], usize) {
    let mut payload = [0u8; MAX_PAYLOAD_LEN];
    let fault_byte: u8 = if fault.active && fault.sensor_id == Some(sample.sensor_id) {
        1
    } else {
        0
    };
    payload[0] = fault_byte;
    payload[1..9].copy_from_slice(&sample.read_at.0.to_le_bytes());
    payload[9..17].copy_from_slice(&sample.value.as_f64().to_le_bytes());
    payload[17..25].copy_from_slice(&sample.sequence.to_le_bytes());
    payload[25] = sample.sensor_id.0;
    for i in 26..MAX_PAYLOAD_LEN {
        payload[i] = (i as u8).wrapping_mul(7);
    }
    (payload, MAX_PAYLOAD_LEN)
}

fn degraded_health_payload(fault: &FaultInjectionState) -> ([u8; MAX_PAYLOAD_LEN], usize) {
    // Keep this payload wire-compatible with GCS sensor parsing:
    // fault(1) + read_at(8) + value(8) + sequence(8) + sensor_id(1)
    let mut payload = [0u8; MAX_PAYLOAD_LEN];
    payload[0] = if fault.active { 1 } else { 0 };
    payload[1..9].copy_from_slice(&time::now_ms().0.to_le_bytes());
    payload[9..17].copy_from_slice(&20.0f64.to_le_bytes()); // valid thermal value
    payload[17..25].copy_from_slice(&0u64.to_le_bytes());
    payload[25] = 0; // thermal sensor id
    (payload, 26)
}

fn dummy_load() {
    let mut x = 0u64;
    for _ in 0..5_000 {
        x = x.wrapping_add(1);
    }
    let _ = x;
}

async fn run_thermal_control_lane(mut thermal_rx: mpsc::Receiver<()>) {
    while let Some(()) = thermal_rx.recv().await {
        if tokio::task::spawn_blocking(dummy_load).await.is_err() {
            crate::ocs_ts_eprintln!("[scheduling] thermal_lane_spawn_blocking_join_err");
        }
    }
}

/// push releases that have reached `now` to the heap.
fn release_due_jobs(
    now_ms: u64,
    thermal: &mut Periodic,
    compression: &mut Periodic,
    health: &mut Periodic,
    antenna: &mut Periodic,
    heap: &mut EdfHeap,
    compression_gen_counter: &mut u64,
    latest_compression_gen: &mut u64,
    antenna_ticket_in_heap: &mut bool,
    antenna_merged_skipped_releases: &mut u64,
) {
    while thermal.next_release_ms <= now_ms {
        let r = thermal.next_release_ms;
        let dl = r.saturating_add(thermal.rel_deadline_ms);
        heap.push(Reverse(EdfJob {
            deadline_abs_ms: dl,
            kind: TaskKind::Thermal,
            compression_gen: 0,
            release_ms: r,
        }));
        thermal.advance();
    }

    while compression.next_release_ms <= now_ms {
        *compression_gen_counter = compression_gen_counter.wrapping_add(1);
        *latest_compression_gen = *compression_gen_counter;
        let r = compression.next_release_ms;
        let dl = r.saturating_add(compression.rel_deadline_ms);
        heap.push(Reverse(EdfJob {
            deadline_abs_ms: dl,
            kind: TaskKind::Compression,
            compression_gen: *compression_gen_counter,
            release_ms: r,
        }));
        compression.advance();
    }

    while health.next_release_ms <= now_ms {
        let r = health.next_release_ms;
        let dl = r.saturating_add(health.rel_deadline_ms);
        heap.push(Reverse(EdfJob {
            deadline_abs_ms: dl,
            kind: TaskKind::Health,
            compression_gen: 0,
            release_ms: r,
        }));
        health.advance();
    }

    while antenna.next_release_ms <= now_ms {
        let r = antenna.next_release_ms;
        let dl = r.saturating_add(antenna.rel_deadline_ms);
        if !*antenna_ticket_in_heap {
            heap.push(Reverse(EdfJob {
                deadline_abs_ms: dl,
                kind: TaskKind::Antenna,
                compression_gen: 0,
                release_ms: r,
            }));
            *antenna_ticket_in_heap = true;
        } else {
            *antenna_merged_skipped_releases = antenna_merged_skipped_releases.saturating_add(1);
            crate::ocs_ts_eprintln!(
                "[scheduling] antenna_merge_skip_release release_ms={} merged_skipped_total={}",
                r,
                *antenna_merged_skipped_releases
            );
        }
        antenna.advance();
    }
}

fn min_next_release(p: [&Periodic; 4]) -> u64 {
    p[0].next_release_ms
        .min(p[1].next_release_ms)
        .min(p[2].next_release_ms)
        .min(p[3].next_release_ms)
}

// -----------------------------------------------------------------------------
// CPU window log
// -----------------------------------------------------------------------------

pub async fn run_cpu_logger(active_ms: Arc<AtomicU64>, metrics: Arc<Mutex<BenchmarkMetrics>>) {
    let mut interval = tokio::time::interval(Duration::from_millis(CPU_WINDOW_MS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut window_start_ms = time::now_ms().0;
    loop {
        interval.tick().await;
        let now_ms = time::now_ms().0;
        let total_ms = now_ms.saturating_sub(window_start_ms);
        let active = active_ms.swap(0, AtomicOrdering::Relaxed);
        if total_ms > 0 {
            let util = (active as f64 / total_ms as f64) * 100.0;
            let metric = CpuUtilizationMetric {
                active_ms: active,
                total_ms,
                utilization_percent: util,
            };
            crate::ocs_ts_eprintln!(
                "[scheduling] cpu_util active_ms={} total_ms={} util={:.1}%",
                metric.active_ms,
                metric.total_ms,
                metric.utilization_percent
            );
            if let Ok(mut m) = metrics.try_lock() {
                m.cpu_util_sum_active_ms = m.cpu_util_sum_active_ms.saturating_add(active);
                m.cpu_util_sum_total_ms = m.cpu_util_sum_total_ms.saturating_add(total_ms);
            }
        }
        window_start_ms = now_ms;
    }
}

// -----------------------------------------------------------------------------
// Main: EDF + Generation (Compression) + Pending (Antenna)
// -----------------------------------------------------------------------------

pub async fn run_scheduling(
    mut buffer_rx: mpsc::Receiver<SensorSample>,
    active_ms: Arc<AtomicU64>,
    alert_rx: watch::Receiver<[bool; 3]>,
    downlink_tx: mpsc::Sender<DownlinkPacket>,
    downlink_queued: Arc<AtomicUsize>,
    _downlink_capacity: usize,
    metrics: Arc<Mutex<BenchmarkMetrics>>,
    fault_rx: watch::Receiver<FaultInjectionState>,
) {
    let (thermal_tx, thermal_rx) = mpsc::channel::<()>(32);
    tokio::spawn(run_thermal_control_lane(thermal_rx));

    let t0 = time::now_ms().0;
    let mut thermal = Periodic {
        period_ms: THERMAL_PERIOD_MS,
        rel_deadline_ms: THERMAL_DEADLINE_MS,
        next_release_ms: t0.saturating_add(THERMAL_PERIOD_MS),
    };
    let mut compression = Periodic {
        period_ms: COMPRESSION_PERIOD_MS,
        rel_deadline_ms: COMPRESSION_DEADLINE_MS,
        next_release_ms: t0.saturating_add(COMPRESSION_PERIOD_MS),
    };
    let mut health = Periodic {
        period_ms: HEALTH_PERIOD_MS,
        rel_deadline_ms: HEALTH_DEADLINE_MS,
        next_release_ms: t0.saturating_add(HEALTH_PERIOD_MS),
    };
    let mut antenna = Periodic {
        period_ms: ANTENNA_PERIOD_MS,
        rel_deadline_ms: ANTENNA_DEADLINE_MS,
        next_release_ms: t0.saturating_add(ANTENNA_PERIOD_MS),
    };

    let mut thermal_state = TaskState::new();
    let mut compression_state = TaskState::new();
    let mut health_state = TaskState::new();
    let mut antenna_state = TaskState::new();
    let mut compression_seq: u64 = 0;

    let mut heap: EdfHeap = BinaryHeap::new();
    let mut compression_gen_counter: u64 = 0;
    let mut latest_compression_gen: u64 = 0;
    let mut antenna_ticket_in_heap = false;
    let mut antenna_merged_skipped_releases: u64 = 0;

    loop {
        let now_ms = time::now_ms().0;
        release_due_jobs(
            now_ms,
            &mut thermal,
            &mut compression,
            &mut health,
            &mut antenna,
            &mut heap,
            &mut compression_gen_counter,
            &mut latest_compression_gen,
            &mut antenna_ticket_in_heap,
            &mut antenna_merged_skipped_releases,
        );

        let mut executed = false;
        let mut budget_yielded = false;
        let mut processed_jobs = 0usize;
        let mut stale_skips = 0usize;
        while let Some(Reverse(job)) = heap.pop() {
            processed_jobs = processed_jobs.saturating_add(1);
            if processed_jobs >= SCHED_LOOP_JOB_BUDGET {
                budget_yielded = true;
                break;
            }
            match job.kind {
                TaskKind::Compression => {
                    if job.compression_gen != latest_compression_gen {
                        crate::ocs_ts_eprintln!(
                            "[scheduling] edf_compression_stale_skip gen={} latest={} deadline_abs={}",
                            job.compression_gen,
                            latest_compression_gen,
                            job.deadline_abs_ms
                        );
                        stale_skips = stale_skips.saturating_add(1);
                        if stale_skips >= SCHED_LOOP_STALE_SKIP_BUDGET {
                            budget_yielded = true;
                            break;
                        }
                        continue;
                    }
                    let (t_start, expected_completion) = record_task_start(
                        TaskKind::Compression,
                        COMPRESSION_PERIOD_MS,
                        COMPRESSION_DEADLINE_MS,
                        &mut compression_state,
                        &metrics,
                    );
                    let compression_release_ms = job.release_ms;
                    let completion_late_ms = t_start.0.saturating_sub(expected_completion);
                    if completion_late_ms >= COMPRESSION_OVERLOAD_SKIP_LATE_MS {
                        crate::ocs_ts_eprintln!(
                            "[scheduling] compression_overload_skip release_ms={} started_at={} completion_late_ms={}",
                            compression_release_ms,
                            t_start.0,
                            completion_late_ms
                        );
                        if let Ok(mut m) = metrics.try_lock() {
                            m.deadline_violations = m.deadline_violations.saturating_add(1);
                            m.compression_overload_skip_count =
                                m.compression_overload_skip_count.saturating_add(1);
                        }
                        compression_seq = compression_seq.wrapping_add(1);
                        let degraded_seq = compression_seq;
                        let degraded_fault = *fault_rx.borrow();
                        let (payload, payload_len) = degraded_health_payload(&degraded_fault);
                        let degraded_packet = DownlinkPacket {
                            sequence: degraded_seq,
                            payload,
                            payload_len,
                            prepared_at: time::now_ms(),
                        };
                        match downlink_tx.try_send(degraded_packet) {
                            Ok(()) => {
                                downlink_queued.fetch_add(1, AtomicOrdering::Relaxed);
                                crate::ocs_ts_eprintln!(
                                    "[scheduling] degraded_keepalive_enqueued sequence={} release_ms={}",
                                    degraded_seq,
                                    compression_release_ms
                                );
                            }
                            Err(mpsc::error::TrySendError::Full(p)) => {
                                crate::ocs_ts_eprintln!(
                                    "[scheduling] degraded_keepalive_drop_downlink_full sequence={} at={}",
                                    p.sequence,
                                    p.prepared_at.0
                                );
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => return,
                        }
                        let t_end = record_task_end(
                            TaskKind::Compression,
                            COMPRESSION_PERIOD_MS,
                            expected_completion,
                            &mut compression_state,
                            &metrics,
                        );
                        active_ms.fetch_add(
                            t_end.0.saturating_sub(t_start.0),
                            AtomicOrdering::Relaxed,
                        );
                        executed = true;
                        break;
                    }

                    let sample = match drain_buffer_latest_per_sensor_pick_one(&mut buffer_rx) {
                        Some(s) => s,
                        None => {
                            crate::ocs_ts_eprintln!(
                                "[scheduling] compression_skip no_sample_after_drain"
                            );
                            let t_end = record_task_end(
                                TaskKind::Compression,
                                COMPRESSION_PERIOD_MS,
                                expected_completion,
                                &mut compression_state,
                                &metrics,
                            );
                            active_ms.fetch_add(
                                t_end.0.saturating_sub(t_start.0),
                                AtomicOrdering::Relaxed,
                            );
                            executed = true;
                            break;
                        }
                    };

                    compression_seq = compression_seq.wrapping_add(1);
                    let seq = compression_seq;
                    let fault = *fault_rx.borrow();

                    let compress_result = tokio::task::spawn_blocking(move || {
                        sample_to_payload(&sample, &fault)
                    })
                    .await;

                    let (payload, payload_len) = match compress_result {
                        Ok(t) => t,
                        Err(e) => {
                            crate::ocs_ts_eprintln!(
                                "[scheduling] compression_spawn_blocking_join_err kind={:?} err={}",
                                TaskKind::Compression,
                                e
                            );
                            let t_end = record_task_end(
                                TaskKind::Compression,
                                COMPRESSION_PERIOD_MS,
                                expected_completion,
                                &mut compression_state,
                                &metrics,
                            );
                            active_ms.fetch_add(
                                t_end.0.saturating_sub(t_start.0),
                                AtomicOrdering::Relaxed,
                            );
                            executed = true;
                            break;
                        }
                    };

                    let prepared_at = time::now_ms();
                    let packet = DownlinkPacket {
                        sequence: seq,
                        payload,
                        payload_len,
                        prepared_at,
                    };
                    let prepare_deadline = compression_release_ms.saturating_add(PREPARE_DEADLINE_MS);
                    if prepared_at.0 > prepare_deadline {
                        let over_ms = prepared_at.0.saturating_sub(prepare_deadline);
                        crate::ocs_ts_eprintln!(
                            "[scheduling] deadline_violation kind=PrepareDeadline skip_send sequence={} release_ms={} deadline_ms={} prepared_at={} over_ms={}",
                            packet.sequence,
                            compression_release_ms,
                            prepare_deadline,
                            prepared_at.0,
                            over_ms
                        );
                        if let Ok(mut m) = metrics.try_lock() {
                            m.deadline_violations = m.deadline_violations.saturating_add(1);
                        }
                    } else {
                        match downlink_tx.try_send(packet) {
                            Ok(()) => {
                                downlink_queued.fetch_add(1, AtomicOrdering::Relaxed);
                            }
                            Err(mpsc::error::TrySendError::Full(p)) => {
                                crate::ocs_ts_eprintln!(
                                    "[scheduling] downlink_full drop sequence={} at={}",
                                    p.sequence,
                                    prepared_at.0
                                );
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => return,
                        }
                    }

                    let t_end = record_task_end(
                        TaskKind::Compression,
                        COMPRESSION_PERIOD_MS,
                        expected_completion,
                        &mut compression_state,
                        &metrics,
                    );
                    active_ms.fetch_add(
                        t_end.0.saturating_sub(t_start.0),
                        AtomicOrdering::Relaxed,
                    );
                    executed = true;
                    break;
                }
                TaskKind::Antenna => {
                    antenna_ticket_in_heap = false;
                    let merged = antenna_merged_skipped_releases;
                    antenna_merged_skipped_releases = 0;
                    if merged > 0 {
                        crate::ocs_ts_eprintln!(
                            "[scheduling] antenna_execute_merged_skipped_releases={}",
                            merged
                        );
                    }
                    let (t_start, expected_completion) = record_task_start(
                        TaskKind::Antenna,
                        ANTENNA_PERIOD_MS,
                        ANTENNA_DEADLINE_MS,
                        &mut antenna_state,
                        &metrics,
                    );
                    if tokio::task::spawn_blocking(|| dummy_load()).await.is_err() {
                        crate::ocs_ts_eprintln!(
                            "[scheduling] antenna_spawn_blocking_join_err"
                        );
                    }
                    let t_end = record_task_end(
                        TaskKind::Antenna,
                        ANTENNA_PERIOD_MS,
                        expected_completion,
                        &mut antenna_state,
                        &metrics,
                    );
                    active_ms.fetch_add(
                        t_end.0.saturating_sub(t_start.0),
                        AtomicOrdering::Relaxed,
                    );
                    executed = true;
                    break;
                }
                TaskKind::Thermal => {
                    let (t_start, expected_completion) = record_task_start(
                        TaskKind::Thermal,
                        THERMAL_PERIOD_MS,
                        THERMAL_DEADLINE_MS,
                        &mut thermal_state,
                        &metrics,
                    );
                    if thermal_tx.try_send(()).is_err() {
                        crate::ocs_ts_eprintln!("[scheduling] thermal_lane_full skip_tick");
                    }
                    tokio::task::yield_now().await;
                    let t_end = record_task_end(
                        TaskKind::Thermal,
                        THERMAL_PERIOD_MS,
                        expected_completion,
                        &mut thermal_state,
                        &metrics,
                    );
                    active_ms.fetch_add(
                        t_end.0.saturating_sub(t_start.0),
                        AtomicOrdering::Relaxed,
                    );
                    executed = true;
                    break;
                }
                TaskKind::Health => {
                    let (t_start, expected_completion) = record_task_start(
                        TaskKind::Health,
                        HEALTH_PERIOD_MS,
                        HEALTH_DEADLINE_MS,
                        &mut health_state,
                        &metrics,
                    );
                    let alert_snapshot = *alert_rx.borrow();
                    let _ = health::check(alert_snapshot);
                    if tokio::task::spawn_blocking(|| dummy_load()).await.is_err() {
                        crate::ocs_ts_eprintln!(
                            "[scheduling] health_spawn_blocking_join_err"
                        );
                    }
                    let t_end = record_task_end(
                        TaskKind::Health,
                        HEALTH_PERIOD_MS,
                        expected_completion,
                        &mut health_state,
                        &metrics,
                    );
                    active_ms.fetch_add(
                        t_end.0.saturating_sub(t_start.0),
                        AtomicOrdering::Relaxed,
                    );
                    executed = true;
                    break;
                }
            }
        }

        if budget_yielded {
            tokio::task::yield_now().await;
            continue;
        }

        if !executed {
            let next_wake = min_next_release([
                &thermal,
                &compression,
                &health,
                &antenna,
            ]);
            let sleep_ms = next_wake.saturating_sub(time::now_ms().0);
            if sleep_ms == 0 {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(Duration::from_millis(sleep_ms.min(50))).await;
            }
        }
    }
}
