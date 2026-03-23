// OCS: common types, EDF task kinds, log/metrics types, buffer & downlink types

use crate::gcs::types::MAX_PAYLOAD_LEN;

pub use crate::sensor_values::{
    OutOfRange, SensorQuantity, SensorValidationError, Temperature, Velocity, Voltage,
};

// -----------------------------------------------------------------------------
// common / identifier
// -----------------------------------------------------------------------------

/// elapsed milliseconds from startup. for consistency with TimestampMs for measurements/logging.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimestampMs(pub u64);

/// sensor ID (example: 0=thermal, 1=..., 2=...)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SensorId(pub u8);

// -----------------------------------------------------------------------------
// EDF scheduler task kind (UDP downlink send is a `downlink` task)
// -----------------------------------------------------------------------------

/// task kind. identifies the EDF job in `scheduling.rs`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TaskKind {
    Thermal,
    Compression,
    Health,
    Antenna,
}

// -----------------------------------------------------------------------------
// sample importance (sensor MPSC side)
// -----------------------------------------------------------------------------

/// importance of sensor sample (critical / normal etc.). separate from EDF [`TaskKind`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum DataPriority {
    Critical, // example: thermal. strict requirements like jitter <1ms
    High,
    Normal,
    Low,
}

/// 1 sample sent from sensor (flows from receiver → buffer → scheduling)
#[derive(Clone, Debug)]
pub struct SensorSample {
    pub sensor_id: SensorId,
    pub data_priority: DataPriority,
    /// Validated quantity (°C, m/s, V, …) — never a bare `f64`.
    pub value: SensorQuantity,
    pub read_at: TimestampMs,
    pub sequence: u64,
}

// -----------------------------------------------------------------------------
// buffer drop metadata
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropReason {
    MpscFull,   // sensors side: mpsc (bounded channel) full
    BufferFull, // receiver side: bounded buffer full
}

// -----------------------------------------------------------------------------
// fault injection (benchmarking → sensors)
// -----------------------------------------------------------------------------

/// fault kind: Delayed = send suppression (Corrupted is not used due to the project requirements)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultKind {
    Delayed,
}

/// fault injection state. benchmarking sends to sensors via watch.
#[derive(Clone, Copy, Debug)]
pub struct FaultInjectionState {
    pub sensor_id: Option<SensorId>,
    pub kind: FaultKind,
    pub active: bool,
}

impl FaultInjectionState {
    /// inactive (no injection) initial value
    pub fn inactive() -> Self {
        Self {
            sensor_id: None,
            kind: FaultKind::Delayed,
            active: false,
        }
    }
}

/// buffer full drop metadata (for logging)
#[derive(Clone, Debug)]
pub struct BufferDropEvent {
    pub at: TimestampMs,
    pub sensor_id: Option<SensorId>,
    pub reason: DropReason,
}

// -----------------------------------------------------------------------------
// metrics / logging
// -----------------------------------------------------------------------------

/// scheduling drift (expected start time vs actual start time)
#[derive(Clone, Debug)]
pub struct DriftMetric {
    pub expected_start: TimestampMs,
    pub actual_start: TimestampMs,
    pub task_kind: TaskKind,
}

/// jitter (periodic task period deviation)
#[derive(Clone, Debug)]
pub struct JitterMetric {
    pub at: TimestampMs,
    pub task_kind: TaskKind,
    pub period_nominal_ms: u64,
    pub actual_interval_ms: u64,
}

/// deadline violation (start delay or completion delay)
#[derive(Clone, Debug)]
pub struct DeadlineViolation {
    pub at: TimestampMs,
    pub task_kind: TaskKind,
    pub kind: DeadlineViolationKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeadlineViolationKind {
    StartDelay,
    CompletionDelay,
}

/// recovery time measurement (fault injection → recovery). evaluate only the target sensor via `sensor_id`.
#[derive(Clone, Debug)]
pub struct RecoveryTimeMetric {
    pub sensor_id: SensorId,
    pub fault_at: TimestampMs,
    pub recovered_at: TimestampMs,
    pub duration_ms: u64,
}

/// CPU utilization (active time vs measurement interval)
#[derive(Clone, Debug)]
pub struct CpuUtilizationMetric {
    pub active_ms: u64,
    pub total_ms: u64,
    pub utilization_percent: f64,
}

/// for 60s cycle benchmark summary. scheduling / cpu_logger writes, benchmarking reads and resets the summary log.
///
/// recovery time (`recovery_*`) is the **cumulative of the entire execution**, not cleared by `reset` (only `reset_all` initializes).
#[derive(Clone, Debug)]
pub struct BenchmarkMetrics {
    pub thermal_max_jitter_ms: i64,
    pub drift_max_ms: i64,
    pub drift_sum_ms: i64,
    pub drift_count: u64,
    /// number of starts delayed from the scheduled start (drift_ms > 0). separate from deadline violation.
    pub drift_late_start_count: u64,
    /// relative deadline exceeded only: completion delay + prepare exceeded (start drift size is not included).
    pub deadline_violations: u64,
    pub cpu_util_sum_active_ms: u64,
    pub cpu_util_sum_total_ms: u64,
    /// last observed recovery time (ms) in the fault injection cycle.
    pub recovery_last_duration_ms: Option<u64>,
    /// maximum observed recovery time (ms). not used when `recovery_count == 0`.
    pub recovery_max_duration_ms: u64,
    pub recovery_sum_duration_ms: u64,
    pub recovery_count: u64,
    /// number of recoveries where `duration_ms` exceeds the abort threshold (`safety::RECOVERY_ABORT_THRESHOLD_MS`).
    pub recovery_over_abort_threshold_count: u64,
}

impl Default for BenchmarkMetrics {
    fn default() -> Self {
        Self {
            thermal_max_jitter_ms: i64::MIN,
            drift_max_ms: i64::MIN,
            drift_sum_ms: 0,
            drift_count: 0,
            drift_late_start_count: 0,
            deadline_violations: 0,
            cpu_util_sum_active_ms: 0,
            cpu_util_sum_total_ms: 0,
            recovery_last_duration_ms: None,
            recovery_max_duration_ms: 0,
            recovery_sum_duration_ms: 0,
            recovery_count: 0,
            recovery_over_abort_threshold_count: 0,
        }
    }
}

impl BenchmarkMetrics {
    /// reset only jitter / drift / deadline / CPU. recovery totals are retained.
    pub fn reset(&mut self) {
        self.thermal_max_jitter_ms = i64::MIN;
        self.drift_max_ms = i64::MIN;
        self.drift_sum_ms = 0;
        self.drift_count = 0;
        self.drift_late_start_count = 0;
        self.deadline_violations = 0;
        self.cpu_util_sum_active_ms = 0;
        self.cpu_util_sum_total_ms = 0;
    }

    /// when pipeline starts: initialize all fields including recovery totals.
    pub fn reset_all(&mut self) {
        *self = Self::default();
    }
}

// -----------------------------------------------------------------------------
// Safety (observation event)
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SafetyEventKind {
    Miss,
    Recovered,      // normal sample re-received (recovery)
    Drop,
    Delay,          // measured_value_ms = actual delay ms
    JitterExceeded, // measured_value_ms = actual jitter ms
    /// Construct-time or wire validation failed (range / non-finite).
    InvalidReading,
}

/// sensors send observation facts. rules (3 consecutive / 200ms) are applied in safety.rs.
#[derive(Clone, Debug)]
pub struct SafetyEvent {
    pub at: TimestampMs,
    pub kind: SafetyEventKind,
    pub sensor_id: Option<SensorId>,
    /// when the threshold is exceeded, the measured value (how much ms exceeded) is inserted.
    pub measured_value_ms: Option<u64>,
}

// -----------------------------------------------------------------------------
// downlink
// -----------------------------------------------------------------------------

/// 1 packet after compression / packetization (scheduling → downlink → GCS)
#[derive(Clone, Debug)]
pub struct DownlinkPacket {
    pub sequence: u64,
    pub payload: [u8; MAX_PAYLOAD_LEN],
    pub payload_len: usize,
    pub prepared_at: TimestampMs,
}

/// 可視窓関連（30ms で準備、5ms で init の制約用）
#[derive(Clone, Debug)]
pub struct VisibilityWindow {
    pub start: TimestampMs,
    pub duration_ms: u64,
}
