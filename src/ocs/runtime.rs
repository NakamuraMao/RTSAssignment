//! OCS: tokio::spawn orchestration, receiver_loop (async fn), task startup.
//! OCS: spawn sensors + receiver_loop + scheduling + cpu_logger.
//! Typestate: [`RuntimeUninitialized`] / [`RuntimeRunning`] / [`RuntimeShuttingDown`] represents the startup phase.
//! Ctrl+C via `ShutdownHandle::try_shutdown` connects to Supervisor's graceful shutdown, waiting for `run` to complete.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

use super::benchmarking;
use super::downlink;
use super::safety;
use super::scheduling;
use super::sensors;
use super::time;
use super::types::{
    BenchmarkMetrics, BufferDropEvent, DownlinkPacket, DropReason, FaultInjectionState,
    RecoveryTimeMetric, SafetyEvent, SafetyEventKind, SensorId, SensorSample,
};
use super::uplink;
use crate::supervisor::{save_text_snapshot, ShutdownHandle, Supervisor};

/// sensor→receiver mpsc capacity (match the buffer counter)
const SAMPLE_BUFFER_CAPACITY: usize = 64;
/// sensor period (match THERMAL/IMU/POWER in `sensors.rs`)
const SENSOR_PERIOD_MS: [u64; 3] = [10, 15, 500]; // Thermal, IMU, Power
const MISS_MARGIN_MS: u64 = 10;
/// check interval for missed reception (Miss increases only once per period even every 10ms)
const CHECK_INTERVAL_MS: u64 = 10;
/// wait上限 for `sensor_rx.recv` wrapped with [`tokio::time::timeout`] (Statement Switch / deadman).
const RECV_TIMEOUT_MS: u64 = CHECK_INTERVAL_MS;

// -----------------------------------------------------------------------------
// Typestate: OCS runtime lifecycle (ZST + associated type Data)
// -----------------------------------------------------------------------------

/// Uninitialized (`time::init_start_time` before Supervisor is created)
pub struct RuntimeUninitialized;
/// Pipeline buildable, `Supervisor::run` is running
pub struct RuntimeRunning;
/// `Supervisor::run` has ended
pub struct RuntimeShuttingDown;

mod runtime_state_sealed {
    pub trait Sealed {}
    impl Sealed for super::RuntimeUninitialized {}
    impl Sealed for super::RuntimeRunning {}
    impl Sealed for super::RuntimeShuttingDown {}
}

/// Only allowed states at compile time. `Data` distinguishes the retained objects for each phase.
pub trait RuntimeState: runtime_state_sealed::Sealed {
    type Data;
}

impl RuntimeState for RuntimeUninitialized {
    type Data = ();
}
impl RuntimeState for RuntimeRunning {
    type Data = PreparedData;
}
impl RuntimeState for RuntimeShuttingDown {
    type Data = ();
}

/// State held by `run_until_shutdown` after `prepare` (fields are only in the crate).
pub struct PreparedData {
    bench_metrics: Arc<Mutex<BenchmarkMetrics>>,
    sup: Supervisor,
    shutdown_handle: ShutdownHandle,
    thermal_thread: Option<JoinHandle<()>>,
}

/// OCS runtime body. `S` is the current typestate.
pub struct OcsRuntime<S: RuntimeState> {
    _state: PhantomData<S>,
    inner: S::Data,
}

impl OcsRuntime<RuntimeUninitialized> {
    pub fn new() -> Self {
        Self {
            _state: PhantomData,
            inner: (),
        }
    }

    /// Time origin, Supervisor creation (only startup preparation).
    pub fn prepare(self) -> OcsRuntime<RuntimeRunning> {
        time::init_start_time();
        let bench_metrics = Arc::new(Mutex::new(BenchmarkMetrics::default()));
        let (sup, shutdown_handle) = Supervisor::new("ocs");
        OcsRuntime {
            _state: PhantomData,
            inner: PreparedData {
                bench_metrics,
                sup,
                shutdown_handle,
                thermal_thread: None,
            },
        }
    }
}

impl OcsRuntime<RuntimeRunning> {
    /// spawn + `sup.run(...)`. Transition to [`RuntimeShuttingDown`] after completion.
    pub async fn run_until_shutdown(self) -> OcsRuntime<RuntimeShuttingDown> {
        let PreparedData {
            bench_metrics,
            mut sup,
            shutdown_handle,
            mut thermal_thread,
        } = self.inner;
        let shutdown_ctrl_c = shutdown_handle.clone();
        let thermal_thread_slot = Arc::new(Mutex::new(None::<JoinHandle<()>>));

        let on_shutdown_save = {
            let bench_metrics = Arc::clone(&bench_metrics);
            move || {
                let m = bench_metrics.lock().expect("bench_metrics lock");
                let recovery_last = m
                    .recovery_last_duration_ms
                    .map(|x| x.to_string())
                    .unwrap_or_else(|| "n/a".to_string());
                let recovery_max = if m.recovery_count == 0 {
                    "n/a".to_string()
                } else {
                    m.recovery_max_duration_ms.to_string()
                };
                let recovery_avg = if m.recovery_count == 0 {
                    "n/a".to_string()
                } else {
                    format!(
                        "{:.2}",
                        m.recovery_sum_duration_ms as f64 / m.recovery_count as f64
                    )
                };
                let e2e_latency_avg = if m.e2e_latency_count == 0 {
                    "n/a".to_string()
                } else {
                    format!(
                        "{:.2}",
                        m.e2e_latency_sum_ms as f64 / m.e2e_latency_count as f64
                    )
                };
                let tx_queue_latency_avg = if m.tx_queue_latency_count == 0 {
                    "n/a".to_string()
                } else {
                    format!(
                        "{:.2}",
                        m.tx_queue_latency_sum_ms as f64 / m.tx_queue_latency_count as f64
                    )
                };
                let snapshot = format!(
                    "# OCS shutdown snapshot\n\
                     # Safety recovery: spec < {}ms; abort if alert persists >= {}ms (constants in safety.rs)\n\
                     recovery_measured_last_ms={}\n\
                     recovery_measured_max_ms={}\n\
                     recovery_measured_avg_ms={}\n\
                     recovery_measured_count={}\n\
                     recovery_measured_over_abort_threshold_count={}\n\
                     thermal_sensor_max_jitter_ms={}\n\
                     thermal_sensor_fault_max_jitter_ms={}\n\
                     thermal_task_jitter_max_ms={}\n\
                     compression_task_jitter_max_ms={}\n\
                     health_task_jitter_max_ms={}\n\
                     antenna_task_jitter_max_ms={}\n\
                     drift_max_ms={}\n\
                     drift_sum_ms={}\n\
                     drift_count={}\n\
                     drift_late_start_count={}\n\
                     deadline_violations={}\n\
                     start_delay_violations={}\n\
                     completion_delay_violations={}\n\
                     prepare_deadline_violations={}\n\
                     visibility_prepare_deadline_violations={}\n\
                     critical_jitter_violation_count={}\n\
                     cpu_util_sum_active_ms={}\n\
                     cpu_util_sum_total_ms={}\n\
                     e2e_latency_max_ms={}\n\
                     e2e_latency_avg_ms={}\n\
                     tx_queue_latency_max_ms={}\n\
                     tx_queue_latency_avg_ms={}\n\
                     peak_buffer_fill_rate_percent={}\n\
                     total_dropped_samples={}\n\
                     compression_overload_skip_count={}\n\
                     degraded_mode_enter_count={}\n\
                     missed_communication_count={}\n",
                    safety::RECOVERY_SPEC_MAX_MS,
                    safety::RECOVERY_ABORT_THRESHOLD_MS,
                    recovery_last,
                    recovery_max,
                    recovery_avg,
                    m.recovery_count,
                    m.recovery_over_abort_threshold_count,
                    m.thermal_sensor_max_jitter_ms,
                    m.thermal_sensor_fault_max_jitter_ms,
                    m.thermal_task_jitter_max_ms,
                    m.compression_task_jitter_max_ms,
                    m.health_task_jitter_max_ms,
                    m.antenna_task_jitter_max_ms,
                    m.drift_max_ms,
                    m.drift_sum_ms,
                    m.drift_count,
                    m.drift_late_start_count,
                    m.deadline_violations,
                    m.start_delay_violations,
                    m.completion_delay_violations,
                    m.prepare_deadline_violations,
                    m.visibility_prepare_deadline_violations,
                    m.critical_jitter_violation_count,
                    m.cpu_util_sum_active_ms,
                    m.cpu_util_sum_total_ms,
                    m.max_e2e_latency_ms,
                    e2e_latency_avg,
                    m.max_tx_queue_latency_ms,
                    tx_queue_latency_avg,
                    m.peak_buffer_fill_rate_percent,
                    m.total_dropped_samples,
                    m.compression_overload_skip_count,
                    m.degraded_mode_enter_count,
                    m.missed_communication_count,
                );
                save_text_snapshot("ocs_shutdown_benchmark_metrics", &snapshot);
            }
        };

        let build_pipeline = {
            let thermal_thread_slot = Arc::clone(&thermal_thread_slot);
            move |sup: &mut Supervisor| {
                {
                    let mut m = bench_metrics.lock().expect("bench_metrics lock");
                    m.reset_all();
                }

                let (sample_tx, sample_rx) = mpsc::channel(SAMPLE_BUFFER_CAPACITY);
                let (buffer_tx, buffer_rx) = mpsc::channel(64);
                let buffer_sample_count = Arc::new(AtomicUsize::new(0));
                let (safety_tx, safety_rx) = mpsc::channel(32);
                let (drop_tx, drop_rx) = mpsc::channel(32);
                let (alert_tx, alert_rx) = watch::channel([false; 3]);
                let (fault_tx, fault_rx) = watch::channel(FaultInjectionState::inactive());
                let (metrics_tx, metrics_rx) = mpsc::channel::<RecoveryTimeMetric>(8);

                const DOWNLINK_CAPACITY: usize = 64;
                let (downlink_tx, downlink_rx) = mpsc::channel::<DownlinkPacket>(DOWNLINK_CAPACITY);
                let downlink_queued = Arc::new(AtomicUsize::new(0));

                const GCS_ADDR: &str = "127.0.0.1:9000";
                const OCS_UPLINK_BIND: &str = "127.0.0.1:9001";

                let active_ms = Arc::new(AtomicU64::new(0));

                let alert_rx_bench = alert_rx.clone();

                let shutdown_tx = shutdown_handle.sender();

                let fault_rx_scheduling = fault_rx.clone();
                let last_priority_drop_log_ms = Arc::new(AtomicU64::new(0));
                let thermal_handle = spawn_thermal_isolated_thread(
                    sample_tx.clone(),
                    safety_tx.clone(),
                    drop_tx.clone(),
                    fault_rx.clone(),
                    buffer_sample_count.clone(),
                    SAMPLE_BUFFER_CAPACITY,
                    Arc::clone(&last_priority_drop_log_ms),
                    bench_metrics.clone(),
                );
                {
                    let mut slot = thermal_thread_slot
                        .lock()
                        .expect("thermal_thread_slot lock");
                    *slot = Some(thermal_handle);
                }
                sup.spawn(sensors::run_imu_sensor(
                    sample_tx.clone(),
                    safety_tx.clone(),
                    drop_tx.clone(),
                    fault_rx.clone(),
                    buffer_sample_count.clone(),
                    SAMPLE_BUFFER_CAPACITY,
                    Arc::clone(&last_priority_drop_log_ms),
                ));
                sup.spawn(sensors::run_power_sensor(
                    sample_tx,
                    safety_tx.clone(),
                    drop_tx.clone(),
                    fault_rx,
                    buffer_sample_count.clone(),
                    SAMPLE_BUFFER_CAPACITY,
                    last_priority_drop_log_ms,
                ));
                sup.spawn(receiver_loop(
                    sample_rx,
                    buffer_tx,
                    drop_tx,
                    safety_tx,
                    buffer_sample_count,
                    bench_metrics.clone(),
                ));
                sup.spawn(scheduling::run_scheduling(
                    buffer_rx,
                    active_ms.clone(),
                    alert_rx,
                    downlink_tx,
                    downlink_queued.clone(),
                    DOWNLINK_CAPACITY,
                    bench_metrics.clone(),
                    fault_rx_scheduling,
                ));
                sup.spawn(downlink::run_downlink(
                    downlink_rx,
                    GCS_ADDR,
                    DOWNLINK_CAPACITY,
                    downlink_queued,
                    bench_metrics.clone(),
                ));
                sup.spawn(uplink::run_uplink_listener(OCS_UPLINK_BIND));
                sup.spawn(scheduling::run_cpu_logger(active_ms, bench_metrics.clone()));
                sup.spawn(drain_drop_rx(drop_rx));
                sup.spawn(safety::run_safety(
                    safety_rx,
                    shutdown_tx,
                    alert_tx,
                    metrics_tx,
                ));
                sup.spawn(benchmarking::run_benchmarking(
                    fault_tx,
                    metrics_rx,
                    alert_rx_bench,
                    bench_metrics.clone(),
                ));
            }
        };

        crate::ocs_ts_eprintln!("[runtime] ocs runtime started (supervisor-managed)");
        let supervisor_run = async {
            sup.run(build_pipeline, on_shutdown_save).await;
        };
        tokio::pin!(supervisor_run);

        tokio::select! {
            res = tokio::signal::ctrl_c() => {
                let _ = res;
                shutdown_ctrl_c.try_shutdown();
            }
            _ = &mut supervisor_run => {}
        }
        supervisor_run.await;
        if thermal_thread.is_none() {
            thermal_thread = thermal_thread_slot
                .lock()
                .expect("thermal_thread_slot lock")
                .take();
        }
        if let Some(handle) = thermal_thread {
            if let Err(e) = handle.join() {
                crate::ocs_ts_eprintln!("[runtime] thermal isolated thread join failed: {:?}", e);
            }
        }

        OcsRuntime {
            _state: PhantomData,
            inner: (),
        }
    }
}

fn spawn_thermal_isolated_thread(
    sample_tx: mpsc::Sender<SensorSample>,
    safety_tx: mpsc::Sender<SafetyEvent>,
    drop_tx: mpsc::Sender<BufferDropEvent>,
    fault_rx: watch::Receiver<FaultInjectionState>,
    buffer_sample_count: Arc<AtomicUsize>,
    buffer_capacity: usize,
    last_priority_drop_log_ms: Arc<AtomicU64>,
    bench_metrics: Arc<Mutex<BenchmarkMetrics>>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("ocs-thermal-critical".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("thermal runtime");
            rt.block_on(sensors::run_thermal_sensor(
                sample_tx,
                safety_tx,
                drop_tx,
                fault_rx,
                buffer_sample_count,
                buffer_capacity,
                last_priority_drop_log_ms,
                bench_metrics,
            ));
        })
        .expect("spawn thermal isolated thread")
}

impl OcsRuntime<RuntimeShuttingDown> {
    pub fn finalize(self) {}
}

// -----------------------------------------------------------------------------
// receiver_loop: select! recv and timer. Miss 1 per period. Recovered only during alert.
// -----------------------------------------------------------------------------

/// Same as timer branch: 1 Miss per sensor if last reception is more than period+margin (advance last_seen by period).
fn emit_periodic_misses_for_stale_sensors(
    last_seen: &mut [Option<u64>; 3],
    safety_tx: &mpsc::Sender<SafetyEvent>,
) {
    let now = time::now_ms().0;
    for idx in 0..3 {
        let period = SENSOR_PERIOD_MS[idx];
        if let Some(prev) = last_seen[idx] {
            if now.saturating_sub(prev) >= period + MISS_MARGIN_MS {
                let sid = SensorId(idx as u8);
                let _ = safety_tx.try_send(SafetyEvent {
                    at: time::now_ms(),
                    kind: SafetyEventKind::Miss,
                    sensor_id: Some(sid),
                    measured_value_ms: None,
                });
                last_seen[idx] = Some(prev + period);
            }
        }
    }
}

/// select! `timeout(recv)` and timer from sensor_rx. Maintain sensor-specific Miss in the merge channel with `emit_periodic_misses`.
/// Miss is only once per period (prevent explosion by advancing last_seen by period). Send Recovered on successful reception (only reset by safety during alert).
async fn receiver_loop(
    mut sensor_rx: mpsc::Receiver<SensorSample>,
    buffer_tx: mpsc::Sender<SensorSample>,
    drop_tx: mpsc::Sender<BufferDropEvent>,
    safety_tx: mpsc::Sender<SafetyEvent>,
    buffer_sample_count: Arc<AtomicUsize>,
    bench_metrics: Arc<Mutex<BenchmarkMetrics>>,
) {
    let mut last_seen: [Option<u64>; 3] = [None, None, None];

    let mut check_interval = tokio::time::interval(Duration::from_millis(CHECK_INTERVAL_MS));
    check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // Branch 1: sample reception (Statement Switch: tokio::time::timeout)
            recv_result = timeout(Duration::from_millis(RECV_TIMEOUT_MS), sensor_rx.recv()) => {
                match recv_result {
                    Err(_elapsed) => {
                        emit_periodic_misses_for_stale_sensors(&mut last_seen, &safety_tx);
                    }
                    Ok(None) => break,
                    Ok(Some(sample)) => {
                        // underflow prevention: saturating subtraction (don't go below 0)
                        let prev = buffer_sample_count.fetch_sub(1, Ordering::Relaxed);
                        if prev == 0 {
                            buffer_sample_count.store(0, Ordering::Relaxed);
                        }
                        let sid = sample.sensor_id;
                        let idx = (sid.0 as usize).min(2);
                        let now = time::now_ms().0;
                        let e2e_latency_ms = now.saturating_sub(sample.read_at.0);
                        if let Ok(mut m) = bench_metrics.try_lock() {
                            m.max_e2e_latency_ms = m.max_e2e_latency_ms.max(e2e_latency_ms);
                            m.e2e_latency_sum_ms =
                                m.e2e_latency_sum_ms.saturating_add(e2e_latency_ms);
                            m.e2e_latency_count = m.e2e_latency_count.saturating_add(1);
                        }
                        let period = SENSOR_PERIOD_MS[idx];

                        // send Miss for the missed periods before reception (1 period = 1 Miss)
                        if let Some(prev) = last_seen[idx] {
                            let elapsed = now.saturating_sub(prev);
                            if elapsed > period + MISS_MARGIN_MS {
                                let missed_periods = (elapsed - MISS_MARGIN_MS) / period.max(1);
                                for _ in 0..missed_periods {
                                    let _ = safety_tx.try_send(SafetyEvent {
                                        at: time::now_ms(),
                                        kind: SafetyEventKind::Miss,
                                        sensor_id: Some(sid),
                                        measured_value_ms: None,
                                    });
                                }
                            }
                        }
                        last_seen[idx] = Some(now);

                        // send Recovered on successful reception (only reset/clear when safety is in alert)
                        let _ = safety_tx.try_send(SafetyEvent {
                            at: time::now_ms(),
                            kind: SafetyEventKind::Recovered,
                            sensor_id: Some(sid),
                            measured_value_ms: None,
                        });

                        match buffer_tx.try_send(sample) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(s)) => {
                                let at = time::now_ms();
                                let drop_ev = BufferDropEvent {
                                    at,
                                    sensor_id: Some(s.sensor_id),
                                    reason: DropReason::BufferFull,
                                };
                                let _ = drop_tx.try_send(drop_ev);
                                crate::ocs_ts_eprintln!(
                                    "[receiver] drop buffer_full sensor_id={} sample_at={}",
                                    s.sensor_id.0,
                                    at.0
                                );
                                if let Ok(mut m) = bench_metrics.try_lock() {
                                    m.total_dropped_samples =
                                        m.total_dropped_samples.saturating_add(1);
                                }
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => break,
                        }
                    }
                }
            }
            // Branch 2: timer (detect continuous Miss for specific sensors in the merge channel even if they are silent)
            _ = check_interval.tick() => {
                emit_periodic_misses_for_stale_sensors(&mut last_seen, &safety_tx);
            }
        }
    }
}

// -----------------------------------------------------------------------------
// minimum drain: log by reading drop_rx / safety_rx (prevent channel blocking)
// -----------------------------------------------------------------------------

async fn drain_drop_rx(mut drop_rx: mpsc::Receiver<BufferDropEvent>) {
    while let Some(ev) = drop_rx.recv().await {
        crate::ocs_ts_eprintln!(
            "[drain] drop event_at={} sensor_id={:?} reason={:?}",
            ev.at.0,
            ev.sensor_id,
            ev.reason
        );
    }
}

// -----------------------------------------------------------------------------
// run: typestate chain entry point
// -----------------------------------------------------------------------------

/// spawn sensors, receiver_loop, scheduling, safety. End on shutdown receive.
pub async fn run() {
    OcsRuntime::new()
        .prepare()
        .run_until_shutdown()
        .await
        .finalize();
}
