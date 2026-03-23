//! GCS common types used across runtime/tasks.
//! phase1: foundational type definitions.
//! RT path: all types used on scheduler/uplink boundary are heap-free (no String, Vec, .await).

use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// Max command payload length (RT path: no heap).
pub const MAX_PAYLOAD_LEN: usize = 256;

/// Milliseconds since process start.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimestampMs(pub u64);

/// Downlink telemetry sequence (wire `sequence` field).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TelemetrySequence(pub u64);

/// Fault bit from downlink payload byte 0 (0 = nominal, non-zero = fault indicated).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DownlinkFaultFlag(pub bool);

/// One decoded downlink packet body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedPacket {
    pub sequence: u64,
    pub prepared_at: TimestampMs,
    /// Payload length in bytes.
    ///
    /// For RT determinism, we avoid allocating/copying payload bytes on decode.
    pub payload_len: usize,
    /// First payload byte: satellite fault indication (false if `payload_len == 0`).
    pub fault_flag: bool,
}

/// Command priority for scheduler/interlocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CommandPriority {
    Urgent,
    High,
    Normal,
    Low,
}

/// Command from operator side (heap-free: RT path).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Command {
    pub id: u64,
    pub name: &'static str,
    pub payload: [u8; MAX_PAYLOAD_LEN],
    pub payload_len: usize,
    pub issued_at: TimestampMs,
    pub priority: CommandPriority,
}

/// Command with execution timing decided by scheduler (heap-free).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScheduledCommand {
    pub command: Command,
    pub enqueued_at: TimestampMs,
    pub execute_at: TimestampMs,
    pub deadline: TimestampMs,
}

/// Current fault level seen by GCS.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FaultState {
    Nominal,
    Degraded,
    Critical,
    Recovering,
}

/// Fault notification message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FaultMessage {
    pub at: TimestampMs,
    pub state: FaultState,
    pub source: String,
    pub detail: String,
}

/// Why a command was rejected by interlocks/scheduler/uplink (heap-free: &'static str only).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectionReason {
    InterlockViolation,
    FaultActive(FaultState),
    DeadlineRisk,
    QueueFullUrgent,
    QueueFullNormal,
    InvalidCommand(&'static str),
    LinkUnavailable,
}

/// Result of uplink dispatch (heap-free).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchResult {
    Sent,
    Rejected { reason: RejectionReason },
    Failed { detail: &'static str },
}

/// Outcome from uplink task for scheduler (drift/miss + log).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DispatchOutcome {
    pub command_id: u64,
    pub actual_dispatch_at: TimestampMs,
    pub result: DispatchResult,
    pub execute_at_ms: u64,
    pub deadline_ms: u64,
}

/// GCS → OCS: application-layer retransmit request (NACK) on the same uplink UDP frame layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetransmitRequest {
    pub missing_from: u64,
    pub missing_to: u64,
}

/// Reserved `command_id` for NACK (not a normal operator command).
pub const NACK_COMMAND_ID: u64 = 0x4E41_434B_0001_4E41;

/// Fixed-size scheduler log event (RT path → channel → logger task).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchedulerLogEvent {
    UplinkOk {
        command_id: u64,
        execute_at: u64,
        deadline: u64,
        actual_dispatch_at: u64,
        drift_ms: i64,
        missed: bool,
    },
    UplinkFailed { command_id: u64, detail: &'static str },
    InterlockRejected {
        command_id: u64,
        name: &'static str,
        state: FaultState,
        reason: RejectionReason,
    },
}

/// Monitoring: jitter measurement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JitterMetric {
    pub at: TimestampMs,
    pub observed_ms: u64,
    pub budget_ms: u64,
}

/// Monitoring: queue/backlog depth.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BacklogMetric {
    pub at: TimestampMs,
    pub queue_len: usize,
}

/// Monitoring: scheduling drift.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriftMetric {
    pub at: TimestampMs,
    pub expected_ms: u64,
    pub actual_ms: u64,
}

/// Monitoring: deadline violation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadlineMetric {
    pub at: TimestampMs,
    pub command_id: u64,
    pub deadline_ms: u64,
    pub completed_ms: u64,
}

/// Monitoring: CPU/load snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct LoadMetric {
    pub at: TimestampMs,
    pub load_percent: f64,
}

/// Monitoring: recovery duration after fault.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryMetric {
    pub fault_at: TimestampMs,
    pub recovered_at: TimestampMs,
    pub duration_ms: u64,
}

/// Monitoring: interlock check latency.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InterlockLatencyMetric {
    pub at: TimestampMs,
    pub latency_ms: u64,
}

/// Optional aggregate snapshot (monitoring module helper).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetricsSnapshot {
    pub last_jitter: Option<JitterMetric>,
    pub last_backlog: Option<BacklogMetric>,
    pub last_drift: Option<DriftMetric>,
    pub last_deadline: Option<DeadlineMetric>,
    pub last_load: Option<LoadMetric>,
    pub last_recovery: Option<RecoveryMetric>,
    pub last_interlock_latency: Option<InterlockLatencyMetric>,
}

/// Decode failure reasons.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    FrameTooShort { actual: usize, min: usize },
    InvalidLengthPrefix { length: u32 },
    LengthMismatch { expected: usize, actual: usize },
    PayloadTooLarge { actual: usize, max: usize },
}

impl Display for DecodeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooShort { actual, min } => {
                write!(f, "frame too short: actual={} min={}", actual, min)
            }
            Self::InvalidLengthPrefix { length } => {
                write!(f, "invalid length prefix: length={}", length)
            }
            Self::LengthMismatch { expected, actual } => {
                write!(
                    f,
                    "length mismatch: expected={} actual={}",
                    expected, actual
                )
            }
            Self::PayloadTooLarge { actual, max } => {
                write!(f, "payload too large: actual={} max={}", actual, max)
            }
        }
    }
}

impl Error for DecodeError {}
