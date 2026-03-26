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
const THERMAL_START_DELAY_BUDGET_MS: i64 = 3;
const NONCRITICAL_START_DELAY_BUDGET_MS: i64 = 5;
/// OCS-internal visibility window schedule (strict-from-start).
/// Prepare (produce `prepared_at`) must be within 30ms from `window_start`.
const VIS_WINDOW_PERIOD_MS: u64 = ANTENNA_PERIOD_MS;
const VIS_WINDOW_DURATION_MS: u64 = 100;
// Coarser cooperative preemption: reduce excessive context-switch overhead
// while still giving high-priority tasks regular chances to run.
const SCHED_LOOP_JOB_BUDGET: usize = 128;
const SCHED_LOOP_STALE_SKIP_BUDGET: usize = 64;
/// Worker -> metrics aggregator report channel capacity (bounded, drop-on-full via `try_send`).
const METRICS_REPORT_CAPACITY: usize = 100;
/// Per-lane trigger queue capacity (bounded, dispatcher uses `try_send`).
const LANE_TRIGGER_CAPACITY: usize = 32;

fn data_priority_rank(p: DataPriority) -> u8 {
    match p {
        DataPriority::Critical => 4,
        DataPriority::High => 3,
        DataPriority::Normal => 2,
        DataPriority::Low => 1,
    }
}

fn start_delay_budget_ms(kind: TaskKind) -> i64 {
    match kind {
        TaskKind::Thermal => THERMAL_START_DELAY_BUDGET_MS,
        TaskKind::Compression | TaskKind::Health | TaskKind::Antenna => {
            NONCRITICAL_START_DELAY_BUDGET_MS
        }
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
) -> (TimestampMs, u64, u64) {
    let t_start = time::now_ms();
    let actual_ms = t_start.0;

    if state.expected_start_ms.is_none() {
        state.expected_start_ms = Some(actual_ms);
    }
    let expected_start_ms = state.expected_start_ms.unwrap_or(actual_ms);
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
            if let Some(jitter_ms) = jitter_ms_opt {
                m.record_task_jitter(kind, jitter_ms);
            }
        }
    }

    state.run_count += 1;
    state.last_start_ms = Some(actual_ms);
    (t_start, expected_start_ms, expected_completion_ms)
}

fn advance_expected_start(period_ms: u64, state: &mut TaskState) {
    let next_expected = state
        .expected_start_ms
        .unwrap_or(0)
        .saturating_add(period_ms);
    state.expected_start_ms = Some(next_expected);
}

fn sample_to_payload(
    sample: &SensorSample,
    fault: &FaultInjectionState,
) -> ([u8; MAX_PAYLOAD_LEN], usize) {
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

#[derive(Clone, Debug)]
struct JobTrigger {
    kind: TaskKind,
    release_ms: u64,
    expected_start_ms: u64,
    expected_completion_ms: u64,
    deadline_abs_ms: u64,
    dispatched_at_ms: u64,
    vis_window_start_ms: u64,
}

#[derive(Clone, Debug)]
struct CompressionTrigger {
    base: JobTrigger,
    sample: Option<SensorSample>,
    fault: FaultInjectionState,
}

#[derive(Clone, Debug)]
struct SimpleTrigger {
    base: JobTrigger,
}

#[derive(Clone, Debug)]
struct CompletionReport {
    kind: TaskKind,
    actual_start_ms: u64,
    actual_end_ms: u64,
    expected_completion_ms: u64,
    /// (optional) release information for better logs
    release_ms: u64,
    /// For `StartDelay` log context
    expected_start_ms: u64,
    /// For visibility-window rule
    vis_window_start_ms: u64,
    /// Whether this completion is a violation
    completion_delay_violation: bool,
    /// Whether visibility-window prepare deadline was violated (Compression only)
    visibility_prepare_violation: bool,
    /// Whether the internal prepare deadline (release+30ms) was violated (Compression only)
    prepare_deadline_violation: bool,
    exec_time_ms: u64,
}

async fn run_simple_lane(
    kind: TaskKind,
    mut rx: mpsc::Receiver<SimpleTrigger>,
    report_tx: mpsc::Sender<CompletionReport>,
) {
    while let Some(trig) = rx.recv().await {
        let t0 = time::now_ms().0;
        // Simulated work; keep as blocking to model load.
        if tokio::task::spawn_blocking(dummy_load).await.is_err() {
            crate::ocs_ts_eprintln!(
                "[scheduling] lane_spawn_blocking_join_err task_kind={:?}",
                kind
            );
        }
        let t1 = time::now_ms().0;
        let completion_delay_violation = t1 > trig.base.expected_completion_ms;
        if completion_delay_violation {
            crate::ocs_ts_eprintln!(
                "[scheduling] deadline_violation kind=CompletionDelay task_kind={:?} at={} expected_completion={}",
                kind,
                t1,
                trig.base.expected_completion_ms
            );
        }
        let rep = CompletionReport {
            kind,
            actual_start_ms: t0,
            actual_end_ms: t1,
            expected_completion_ms: trig.base.expected_completion_ms,
            release_ms: trig.base.release_ms,
            expected_start_ms: trig.base.expected_start_ms,
            vis_window_start_ms: trig.base.vis_window_start_ms,
            completion_delay_violation,
            visibility_prepare_violation: false,
            prepare_deadline_violation: false,
            exec_time_ms: t1.saturating_sub(t0),
        };
        if report_tx.try_send(rep).is_err() {
            // Observability plane: drop-on-full (Backpressure).
            crate::ocs_ts_eprintln!(
                "[scheduling] metrics_report_drop kind={:?} at={}",
                kind,
                time::now_ms().0
            );
        }
    }
}

async fn run_compression_lane(
    mut rx: mpsc::Receiver<CompressionTrigger>,
    downlink_tx: mpsc::Sender<DownlinkPacket>,
    downlink_queued: Arc<AtomicUsize>,
    report_tx: mpsc::Sender<CompletionReport>,
) {
    let mut prev_in_visibility_window: Option<bool> = None;
    while let Some(trig) = rx.recv().await {
        let t_start_ms = time::now_ms().0;
        let mut visibility_prepare_violation = false;
        let prepare_deadline_violation = false;

        // Overload protection: if started too late, skip expensive compression and send degraded keepalive.
        let completion_late_ms = t_start_ms.saturating_sub(trig.base.expected_completion_ms);
        if completion_late_ms >= COMPRESSION_OVERLOAD_SKIP_LATE_MS {
            crate::ocs_ts_eprintln!(
                "[scheduling] compression_overload_skip release_ms={} started_at={} completion_late_ms={}",
                trig.base.release_ms,
                t_start_ms,
                completion_late_ms
            );
            let degraded_seq = 0_u64;
            let (payload, payload_len) = degraded_health_payload(&trig.fault);
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
                        trig.base.release_ms
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

            let t_end_ms = time::now_ms().0;
            let completion_delay_violation = t_end_ms > trig.base.expected_completion_ms;
            if completion_delay_violation {
                crate::ocs_ts_eprintln!(
                    "[scheduling] deadline_violation kind=CompletionDelay task_kind={:?} at={} expected_completion={}",
                    TaskKind::Compression,
                    t_end_ms,
                    trig.base.expected_completion_ms
                );
            }
            let rep = CompletionReport {
                kind: TaskKind::Compression,
                actual_start_ms: t_start_ms,
                actual_end_ms: t_end_ms,
                expected_completion_ms: trig.base.expected_completion_ms,
                release_ms: trig.base.release_ms,
                expected_start_ms: trig.base.expected_start_ms,
                vis_window_start_ms: trig.base.vis_window_start_ms,
                completion_delay_violation,
                visibility_prepare_violation,
                prepare_deadline_violation,
                exec_time_ms: t_end_ms.saturating_sub(t_start_ms),
            };
            if report_tx.try_send(rep).is_err() {
                crate::ocs_ts_eprintln!(
                    "[scheduling] metrics_report_drop kind=Compression at={}",
                    time::now_ms().0
                );
            }
            continue;
        }

        let Some(sample) = trig.sample else {
            crate::ocs_ts_eprintln!("[scheduling] compression_skip no_sample_after_drain");
            let t_end_ms = time::now_ms().0;
            let completion_delay_violation = t_end_ms > trig.base.expected_completion_ms;
            if completion_delay_violation {
                crate::ocs_ts_eprintln!(
                    "[scheduling] deadline_violation kind=CompletionDelay task_kind={:?} at={} expected_completion={}",
                    TaskKind::Compression,
                    t_end_ms,
                    trig.base.expected_completion_ms
                );
            }
            let rep = CompletionReport {
                kind: TaskKind::Compression,
                actual_start_ms: t_start_ms,
                actual_end_ms: t_end_ms,
                expected_completion_ms: trig.base.expected_completion_ms,
                release_ms: trig.base.release_ms,
                expected_start_ms: trig.base.expected_start_ms,
                vis_window_start_ms: trig.base.vis_window_start_ms,
                completion_delay_violation,
                visibility_prepare_violation,
                prepare_deadline_violation,
                exec_time_ms: t_end_ms.saturating_sub(t_start_ms),
            };
            let _ = report_tx.try_send(rep);
            continue;
        };

        // CPU-intensive payload generation.
        let fault = trig.fault;
        let compress_result =
            tokio::task::spawn_blocking(move || sample_to_payload(&sample, &fault)).await;
        let (payload, payload_len) = match compress_result {
            Ok(t) => t,
            Err(e) => {
                crate::ocs_ts_eprintln!(
                    "[scheduling] compression_spawn_blocking_join_err kind={:?} err={}",
                    TaskKind::Compression,
                    e
                );
                let t_end_ms = time::now_ms().0;
                let completion_delay_violation = t_end_ms > trig.base.expected_completion_ms;
                if completion_delay_violation {
                    crate::ocs_ts_eprintln!(
                        "[scheduling] deadline_violation kind=CompletionDelay task_kind={:?} at={} expected_completion={}",
                        TaskKind::Compression,
                        t_end_ms,
                        trig.base.expected_completion_ms
                    );
                }
                let rep = CompletionReport {
                    kind: TaskKind::Compression,
                    actual_start_ms: t_start_ms,
                    actual_end_ms: t_end_ms,
                    expected_completion_ms: trig.base.expected_completion_ms,
                    release_ms: trig.base.release_ms,
                    expected_start_ms: trig.base.expected_start_ms,
                    vis_window_start_ms: trig.base.vis_window_start_ms,
                    completion_delay_violation,
                    visibility_prepare_violation,
                    prepare_deadline_violation,
                    exec_time_ms: t_end_ms.saturating_sub(t_start_ms),
                };
                let _ = report_tx.try_send(rep);
                continue;
            }
        };

        let prepared_at = time::now_ms();
        let vis_window_start_ms = trig.base.vis_window_start_ms;
        let vis_window_end_ms = vis_window_start_ms.saturating_add(VIS_WINDOW_DURATION_MS);
        let in_visibility_window =
            prepared_at.0 >= vis_window_start_ms && prepared_at.0 < vis_window_end_ms;

        if prev_in_visibility_window != Some(in_visibility_window) {
            crate::ocs_ts_eprintln!(
                "[scheduling] visibility_window_transition state={} window_start_ms={} window_end_ms={} at_ms={}",
                if in_visibility_window { "open" } else { "closed" },
                vis_window_start_ms,
                vis_window_end_ms,
                prepared_at.0
            );
            prev_in_visibility_window = Some(in_visibility_window);
        }

        // Prepare deadline (RTS latency): once released, the packet must be prepared within 30ms.
        // This rule is only applied when we are within the visibility window.
        let prepare_deadline = trig.base.release_ms.saturating_add(PREPARE_DEADLINE_MS);
        if in_visibility_window && prepared_at.0 > prepare_deadline {
            visibility_prepare_violation = true;
            crate::ocs_ts_eprintln!(
                "[scheduling] deadline_violation kind=VisibilityPrepareDeadline skip_send release_ms={} window_start_ms={} deadline_ms={} prepared_at={} latency_ms={} over_ms={}",
                trig.base.release_ms,
                vis_window_start_ms,
                prepare_deadline,
                prepared_at.0,
                prepared_at.0.saturating_sub(trig.base.release_ms),
                prepared_at.0.saturating_sub(prepare_deadline)
            );
        }

        if !(visibility_prepare_violation || prepare_deadline_violation) {
            let packet = DownlinkPacket {
                sequence: 0_u64,
                payload,
                payload_len,
                prepared_at,
            };
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

        let t_end_ms = time::now_ms().0;
        let completion_delay_violation = t_end_ms > trig.base.expected_completion_ms;
        if completion_delay_violation {
            crate::ocs_ts_eprintln!(
                "[scheduling] deadline_violation kind=CompletionDelay task_kind={:?} at={} expected_completion={}",
                TaskKind::Compression,
                t_end_ms,
                trig.base.expected_completion_ms
            );
        }
        let rep = CompletionReport {
            kind: TaskKind::Compression,
            actual_start_ms: t_start_ms,
            actual_end_ms: t_end_ms,
            expected_completion_ms: trig.base.expected_completion_ms,
            release_ms: trig.base.release_ms,
            expected_start_ms: trig.base.expected_start_ms,
            vis_window_start_ms: trig.base.vis_window_start_ms,
            completion_delay_violation,
            visibility_prepare_violation,
            prepare_deadline_violation,
            exec_time_ms: t_end_ms.saturating_sub(t_start_ms),
        };
        if report_tx.try_send(rep).is_err() {
            crate::ocs_ts_eprintln!(
                "[scheduling] metrics_report_drop kind=Compression at={}",
                time::now_ms().0
            );
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
            let _metric = CpuUtilizationMetric {
                active_ms: active,
                total_ms,
                utilization_percent: util,
            };
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
    // Observability plane: bounded completion/metrics reports; drop-on-full.
    let (report_tx, mut report_rx) = mpsc::channel::<CompletionReport>(METRICS_REPORT_CAPACITY);

    // Worker lanes (dispatcher -> workers). Bounded; dispatcher uses `try_send`.
    let (compression_tx, compression_rx) =
        mpsc::channel::<CompressionTrigger>(LANE_TRIGGER_CAPACITY);
    let (health_tx, health_rx) = mpsc::channel::<SimpleTrigger>(LANE_TRIGGER_CAPACITY);
    let (antenna_tx, antenna_rx) = mpsc::channel::<SimpleTrigger>(LANE_TRIGGER_CAPACITY);
    let (thermal_tx, thermal_rx) = mpsc::channel::<SimpleTrigger>(LANE_TRIGGER_CAPACITY);

    // Spawn workers (they may await/compute; dispatcher must not).
    tokio::spawn(run_compression_lane(
        compression_rx,
        downlink_tx.clone(),
        downlink_queued.clone(),
        report_tx.clone(),
    ));
    tokio::spawn(run_simple_lane(
        TaskKind::Health,
        health_rx,
        report_tx.clone(),
    ));
    tokio::spawn(run_simple_lane(
        TaskKind::Antenna,
        antenna_rx,
        report_tx.clone(),
    ));
    tokio::spawn(run_simple_lane(
        TaskKind::Thermal,
        thermal_rx,
        report_tx.clone(),
    ));

    // Metrics aggregator: updates shared metrics and active time based on completion reports.
    {
        let metrics = Arc::clone(&metrics);
        let active_ms = Arc::clone(&active_ms);
        tokio::spawn(async move {
            while let Some(rep) = report_rx.recv().await {
                active_ms.fetch_add(rep.exec_time_ms, AtomicOrdering::Relaxed);
                if let Ok(mut m) = metrics.try_lock() {
                    if rep.completion_delay_violation {
                        m.deadline_violations = m.deadline_violations.saturating_add(1);
                        m.completion_delay_violations =
                            m.completion_delay_violations.saturating_add(1);
                    }
                    if rep.prepare_deadline_violation {
                        m.deadline_violations = m.deadline_violations.saturating_add(1);
                        m.prepare_deadline_violations =
                            m.prepare_deadline_violations.saturating_add(1);
                    }
                    if rep.visibility_prepare_violation {
                        m.deadline_violations = m.deadline_violations.saturating_add(1);
                        m.visibility_prepare_deadline_violations =
                            m.visibility_prepare_deadline_violations.saturating_add(1);
                    }
                }
            }
        });
    }

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
    let _compression_seq: u64 = 0;

    let mut heap: EdfHeap = BinaryHeap::new();
    let mut compression_gen_counter: u64 = 0;
    let mut latest_compression_gen: u64 = 0;
    let mut antenna_ticket_in_heap = false;
    let mut antenna_merged_skipped_releases: u64 = 0;

    // OCS-internal visibility window (strict-from-start).
    let mut vis_window_start_ms: u64 = t0;
    let _vis_window_end_ms: u64 = vis_window_start_ms.saturating_add(VIS_WINDOW_DURATION_MS);

    loop {
        let now_ms = time::now_ms().0;
        // Advance visibility window schedule.
        while now_ms >= vis_window_start_ms.saturating_add(VIS_WINDOW_PERIOD_MS) {
            vis_window_start_ms = vis_window_start_ms.saturating_add(VIS_WINDOW_PERIOD_MS);
        }
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
                    let (t_start, expected_start, expected_completion) = record_task_start(
                        TaskKind::Compression,
                        COMPRESSION_PERIOD_MS,
                        COMPRESSION_DEADLINE_MS,
                        &mut compression_state,
                        &metrics,
                    );
                    // StartDelay explicit violation log (any positive drift).
                    let drift_ms = t_start.0 as i64 - expected_start as i64;
                    let budget_ms = start_delay_budget_ms(TaskKind::Compression);
                    if drift_ms > budget_ms {
                        crate::ocs_ts_eprintln!(
                            "[scheduling] deadline_violation kind=StartDelay task_kind={:?} at={} expected_start={} drift_ms={} budget_ms={}",
                            TaskKind::Compression,
                            t_start.0,
                            expected_start,
                            drift_ms,
                            budget_ms
                        );
                        if let Ok(mut m) = metrics.try_lock() {
                            m.deadline_violations = m.deadline_violations.saturating_add(1);
                            m.start_delay_violations = m.start_delay_violations.saturating_add(1);
                        }
                    }
                    let sample = drain_buffer_latest_per_sensor_pick_one(&mut buffer_rx);
                    let fault = *fault_rx.borrow();
                    let trig = CompressionTrigger {
                        base: JobTrigger {
                            kind: TaskKind::Compression,
                            release_ms: job.release_ms,
                            expected_start_ms: expected_start,
                            expected_completion_ms: expected_completion,
                            deadline_abs_ms: job.deadline_abs_ms,
                            dispatched_at_ms: time::now_ms().0,
                            vis_window_start_ms,
                        },
                        sample,
                        fault,
                    };
                    if compression_tx.try_send(trig).is_err() {
                        crate::ocs_ts_eprintln!(
                            "[scheduling] lane_full skip_tick task_kind=Compression at={}",
                            time::now_ms().0
                        );
                    }
                    advance_expected_start(COMPRESSION_PERIOD_MS, &mut compression_state);
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
                    let (t_start, expected_start, expected_completion) = record_task_start(
                        TaskKind::Antenna,
                        ANTENNA_PERIOD_MS,
                        ANTENNA_DEADLINE_MS,
                        &mut antenna_state,
                        &metrics,
                    );
                    let drift_ms = t_start.0 as i64 - expected_start as i64;
                    let budget_ms = start_delay_budget_ms(TaskKind::Antenna);
                    if drift_ms > budget_ms {
                        crate::ocs_ts_eprintln!(
                            "[scheduling] deadline_violation kind=StartDelay task_kind={:?} at={} expected_start={} drift_ms={} budget_ms={}",
                            TaskKind::Antenna,
                            t_start.0,
                            expected_start,
                            drift_ms,
                            budget_ms
                        );
                        if let Ok(mut m) = metrics.try_lock() {
                            m.deadline_violations = m.deadline_violations.saturating_add(1);
                            m.start_delay_violations = m.start_delay_violations.saturating_add(1);
                        }
                    }
                    let trig = SimpleTrigger {
                        base: JobTrigger {
                            kind: TaskKind::Antenna,
                            release_ms: job.release_ms,
                            expected_start_ms: expected_start,
                            expected_completion_ms: expected_completion,
                            deadline_abs_ms: job.deadline_abs_ms,
                            dispatched_at_ms: time::now_ms().0,
                            vis_window_start_ms,
                        },
                    };
                    if antenna_tx.try_send(trig).is_err() {
                        crate::ocs_ts_eprintln!(
                            "[scheduling] lane_full skip_tick task_kind=Antenna at={}",
                            time::now_ms().0
                        );
                    }
                    advance_expected_start(ANTENNA_PERIOD_MS, &mut antenna_state);
                    executed = true;
                    break;
                }
                TaskKind::Thermal => {
                    let (t_start, expected_start, expected_completion) = record_task_start(
                        TaskKind::Thermal,
                        THERMAL_PERIOD_MS,
                        THERMAL_DEADLINE_MS,
                        &mut thermal_state,
                        &metrics,
                    );
                    let drift_ms = t_start.0 as i64 - expected_start as i64;
                    let budget_ms = start_delay_budget_ms(TaskKind::Thermal);
                    if drift_ms > budget_ms {
                        crate::ocs_ts_eprintln!(
                            "[scheduling] deadline_violation kind=StartDelay task_kind={:?} at={} expected_start={} drift_ms={} budget_ms={}",
                            TaskKind::Thermal,
                            t_start.0,
                            expected_start,
                            drift_ms,
                            budget_ms
                        );
                        if let Ok(mut m) = metrics.try_lock() {
                            m.deadline_violations = m.deadline_violations.saturating_add(1);
                            m.start_delay_violations = m.start_delay_violations.saturating_add(1);
                        }
                    }
                    let trig = SimpleTrigger {
                        base: JobTrigger {
                            kind: TaskKind::Thermal,
                            release_ms: job.release_ms,
                            expected_start_ms: expected_start,
                            expected_completion_ms: expected_completion,
                            deadline_abs_ms: job.deadline_abs_ms,
                            dispatched_at_ms: time::now_ms().0,
                            vis_window_start_ms,
                        },
                    };
                    if thermal_tx.try_send(trig).is_err() {
                        crate::ocs_ts_eprintln!(
                            "[scheduling] lane_full skip_tick task_kind=Thermal"
                        );
                    }
                    advance_expected_start(THERMAL_PERIOD_MS, &mut thermal_state);
                    executed = true;
                    break;
                }
                TaskKind::Health => {
                    let (t_start, expected_start, expected_completion) = record_task_start(
                        TaskKind::Health,
                        HEALTH_PERIOD_MS,
                        HEALTH_DEADLINE_MS,
                        &mut health_state,
                        &metrics,
                    );
                    let alert_snapshot = *alert_rx.borrow();
                    let _ = health::check(alert_snapshot);
                    let drift_ms = t_start.0 as i64 - expected_start as i64;
                    let budget_ms = start_delay_budget_ms(TaskKind::Health);
                    if drift_ms > budget_ms {
                        crate::ocs_ts_eprintln!(
                            "[scheduling] deadline_violation kind=StartDelay task_kind={:?} at={} expected_start={} drift_ms={} budget_ms={}",
                            TaskKind::Health,
                            t_start.0,
                            expected_start,
                            drift_ms,
                            budget_ms
                        );
                        if let Ok(mut m) = metrics.try_lock() {
                            m.deadline_violations = m.deadline_violations.saturating_add(1);
                            m.start_delay_violations = m.start_delay_violations.saturating_add(1);
                        }
                    }
                    let trig = SimpleTrigger {
                        base: JobTrigger {
                            kind: TaskKind::Health,
                            release_ms: job.release_ms,
                            expected_start_ms: expected_start,
                            expected_completion_ms: expected_completion,
                            deadline_abs_ms: job.deadline_abs_ms,
                            dispatched_at_ms: time::now_ms().0,
                            vis_window_start_ms,
                        },
                    };
                    if health_tx.try_send(trig).is_err() {
                        crate::ocs_ts_eprintln!(
                            "[scheduling] lane_full skip_tick task_kind=Health at={}",
                            time::now_ms().0
                        );
                    }
                    advance_expected_start(HEALTH_PERIOD_MS, &mut health_state);
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
            let next_wake = min_next_release([&thermal, &compression, &health, &antenna]);
            let sleep_ms = next_wake.saturating_sub(time::now_ms().0);
            if sleep_ms == 0 {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(Duration::from_millis(sleep_ms.min(50))).await;
            }
        }
    }
}
