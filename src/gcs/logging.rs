//! GCS logging: minimal key=value log API for phase1.

use crate::gcs::time;
use crate::gcs::types::{FaultState, RejectionReason, SchedulerLogEvent};

/// Shared log events for GCS tasks.
#[derive(Clone, Debug)]
pub enum LogEvent {
    RuntimeStarted { mode: &'static str },
    DownlinkListening { addr: String },
    DownlinkAccepted { peer: String },
    DownlinkConnectionClosed { peer: String },
    DownlinkDecoded {
        sequence: u64,
        prepared_at_ms: u64,
        payload_len: usize,
    },
    TelemetryDecodeTiming {
        elapsed_ms: u64,
    },
    DecodeBudgetViolation {
        elapsed_ms: u64,
        budget_ms: u64,
    },
    TelemetryRxLatency {
        sequence: u64,
        latency_ms: u64,
    },
    TelemetryRxDrift {
        sequence: u64,
        expected_interval_ms: u64,
        actual_interval_ms: u64,
        drift_ms: i64,
    },
    TelemetryRerequestGap {
        expected_seq: u64,
        received_seq: u64,
        missing_from: u64,
        missing_to: u64,
    },
    TelemetryNackEnqueued {
        missing_from: u64,
        missing_to: u64,
    },
    TelemetryNackQueueFull {
        missing_from: u64,
        missing_to: u64,
    },
    UplinkNackSent {
        missing_from: u64,
        missing_to: u64,
        enqueue_to_send_us: u64,
    },
    TelemetryRerequestDecodeFailure {
        reason: &'static str,
    },
    TelemetryContactLost {
        consecutive_failures: u32,
    },
    TelemetryContactRestored,
    /// GCS downlink bound; waiting for first valid telemetry (acquisition phase).
    TelemetryWaitingFirstPacket {
        acquisition_timeout_ms: u64,
    },
    /// First successfully decoded downlink packet (link established; strict deadman active).
    TelemetryLinkEstablished {
        sequence: u64,
    },
    /// No valid packet before acquisition timeout — graceful shutdown.
    TelemetryAcquisitionTimeout {
        acquisition_timeout_ms: u64,
    },
    Error {
        component: &'static str,
        action: &'static str,
        detail: String,
    },
    // Fault & interlocks
    FaultReceived {
        at_ms: u64,
        state: FaultState,
        source: String,
        detail: String,
    },
    FaultStateChanged {
        from: FaultState,
        to: FaultState,
    },
    FaultRecovered { duration_ms: u64 },
    CriticalAlertRecoveryTimeout {
        fault_detected_at_ms: u64,
        elapsed_ms: u64,
    },
    InterlockRejected {
        command_id: u64,
        name: &'static str,
        state: FaultState,
        reason: RejectionReason,
    },
    // Uplink (fixed-size for RT path → channel → logger)
    UplinkSendOk {
        command_id: u64,
        execute_at: u64,
        deadline: u64,
        actual_dispatch_at: u64,
        drift_ms: i64,
        missed: bool,
    },
    UplinkSendFailed {
        command_id: u64,
        detail: &'static str,
    },
}

fn escape_quoted(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Emit scheduler/uplink events (fixed-size, from channel). No heap in caller.
pub fn log_scheduler_event(ev: SchedulerLogEvent) {
    match ev {
        SchedulerLogEvent::UplinkOk {
            command_id,
            execute_at,
            deadline,
            actual_dispatch_at,
            drift_ms,
            missed,
        } => {
            log_event(LogEvent::UplinkSendOk {
                command_id,
                execute_at,
                deadline,
                actual_dispatch_at,
                drift_ms,
                missed,
            });
        }
        SchedulerLogEvent::UplinkFailed { command_id, detail } => {
            log_event(LogEvent::UplinkSendFailed { command_id, detail });
        }
        SchedulerLogEvent::InterlockRejected {
            command_id,
            name,
            state,
            reason,
        } => {
            log_event(LogEvent::InterlockRejected {
                command_id,
                name,
                state,
                reason,
            });
        }
    }
}

/// Emit one key=value log line with mandatory timestamp.
pub fn log_event(ev: LogEvent) {
    let ts_ms = time::now_ms().0;
    match ev {
        LogEvent::RuntimeStarted { mode } => {
            eprintln!("ts_ms={} event=runtime_started mode={}", ts_ms, mode);
        }
        LogEvent::DownlinkListening { addr } => {
            eprintln!("ts_ms={} event=downlink_listening addr={}", ts_ms, addr);
        }
        LogEvent::DownlinkAccepted { peer } => {
            eprintln!("ts_ms={} event=downlink_accepted peer={}", ts_ms, peer);
        }
        LogEvent::DownlinkConnectionClosed { peer } => {
            eprintln!(
                "ts_ms={} event=downlink_connection_closed peer={}",
                ts_ms, peer
            );
        }
        LogEvent::DownlinkDecoded {
            sequence,
            prepared_at_ms,
            payload_len,
        } => {
            eprintln!(
                "ts_ms={} event=downlink_decoded seq={} prepared_at_ms={} payload_len={}",
                ts_ms, sequence, prepared_at_ms, payload_len
            );
        }
        LogEvent::TelemetryDecodeTiming { elapsed_ms } => {
            eprintln!(
                "ts_ms={} event=telemetry_decode_timing elapsed_ms={}",
                ts_ms, elapsed_ms
            );
        }
        LogEvent::DecodeBudgetViolation {
            elapsed_ms,
            budget_ms,
        } => {
            eprintln!(
                "ts_ms={} event=decode_budget_violation elapsed_ms={} budget_ms={}",
                ts_ms, elapsed_ms, budget_ms
            );
        }
        LogEvent::TelemetryRxLatency {
            sequence,
            latency_ms,
        } => {
            eprintln!(
                "ts_ms={} event=telemetry_rx_latency seq={} latency_ms={}",
                ts_ms, sequence, latency_ms
            );
        }
        LogEvent::TelemetryRxDrift {
            sequence,
            expected_interval_ms,
            actual_interval_ms,
            drift_ms,
        } => {
            eprintln!(
                "ts_ms={} event=telemetry_rx_drift seq={} expected_interval_ms={} actual_interval_ms={} drift_ms={}",
                ts_ms, sequence, expected_interval_ms, actual_interval_ms, drift_ms
            );
        }
        LogEvent::TelemetryRerequestGap {
            expected_seq,
            received_seq,
            missing_from,
            missing_to,
        } => {
            eprintln!(
                "ts_ms={} event=telemetry_rerequest_gap expected_seq={} received_seq={} missing_from={} missing_to={}",
                ts_ms, expected_seq, received_seq, missing_from, missing_to
            );
        }
        LogEvent::TelemetryNackEnqueued {
            missing_from,
            missing_to,
        } => {
            eprintln!(
                "ts_ms={} event=telemetry_nack_enqueued missing_from={} missing_to={}",
                ts_ms, missing_from, missing_to
            );
        }
        LogEvent::TelemetryNackQueueFull {
            missing_from,
            missing_to,
        } => {
            eprintln!(
                "ts_ms={} event=telemetry_nack_queue_full missing_from={} missing_to={}",
                ts_ms, missing_from, missing_to
            );
        }
        LogEvent::UplinkNackSent {
            missing_from,
            missing_to,
            enqueue_to_send_us,
        } => {
            eprintln!(
                "ts_ms={} event=uplink_nack_sent missing_from={} missing_to={} enqueue_to_send_us={}",
                ts_ms, missing_from, missing_to, enqueue_to_send_us
            );
        }
        LogEvent::TelemetryRerequestDecodeFailure { reason } => {
            eprintln!(
                "ts_ms={} event=telemetry_rerequest_decode_failure reason={}",
                ts_ms, reason
            );
        }
        LogEvent::TelemetryContactLost {
            consecutive_failures,
        } => {
            eprintln!(
                "ts_ms={} event=telemetry_contact_lost consecutive_failures={}",
                ts_ms, consecutive_failures
            );
        }
        LogEvent::TelemetryContactRestored => {
            eprintln!("ts_ms={} event=telemetry_contact_restored", ts_ms);
        }
        LogEvent::TelemetryWaitingFirstPacket {
            acquisition_timeout_ms,
        } => {
            eprintln!(
                "ts_ms={} event=telemetry_waiting_first_packet acquisition_timeout_ms={}",
                ts_ms, acquisition_timeout_ms
            );
        }
        LogEvent::TelemetryLinkEstablished { sequence } => {
            eprintln!(
                "ts_ms={} event=telemetry_link_established seq={}",
                ts_ms, sequence
            );
        }
        LogEvent::TelemetryAcquisitionTimeout {
            acquisition_timeout_ms,
        } => {
            eprintln!(
                "ts_ms={} event=telemetry_acquisition_timeout acquisition_timeout_ms={}",
                ts_ms, acquisition_timeout_ms
            );
        }
        LogEvent::Error {
            component,
            action,
            detail,
        } => {
            let escaped = escape_quoted(&detail);
            eprintln!(
                "ts_ms={} event=error component={} action={} detail=\"{}\"",
                ts_ms, component, action, escaped
            );
        }
        LogEvent::FaultReceived {
            at_ms,
            state,
            source,
            detail,
        } => {
            let escaped = escape_quoted(&detail);
            eprintln!(
                "ts_ms={} event=fault_received at_ms={} state={:?} source={} detail=\"{}\"",
                ts_ms, at_ms, state, source, escaped
            );
        }
        LogEvent::FaultStateChanged { from, to } => {
            eprintln!(
                "ts_ms={} event=fault_state_changed from={:?} to={:?}",
                ts_ms, from, to
            );
        }
        LogEvent::FaultRecovered { duration_ms } => {
            eprintln!(
                "ts_ms={} event=fault_recovered duration_ms={}",
                ts_ms, duration_ms
            );
        }
        LogEvent::CriticalAlertRecoveryTimeout {
            fault_detected_at_ms,
            elapsed_ms,
        } => {
            eprintln!(
                "ts_ms={} event=critical_alert_recovery_timeout fault_detected_at_ms={} elapsed_ms={}",
                ts_ms, fault_detected_at_ms, elapsed_ms
            );
        }
        LogEvent::InterlockRejected {
            command_id,
            name,
            state,
            reason,
        } => {
            eprintln!(
                "ts_ms={} event=interlock_rejected command_id={} name={} state={:?} reason={:?}",
                ts_ms, command_id, name, state, reason
            );
        }
        LogEvent::UplinkSendOk {
            command_id,
            execute_at,
            deadline,
            actual_dispatch_at,
            drift_ms,
            missed,
        } => {
            eprintln!(
                "ts_ms={} event=uplink_send_ok command_id={} execute_at={} deadline={} actual_dispatch_at={} drift_ms={} missed={}",
                ts_ms, command_id, execute_at, deadline, actual_dispatch_at, drift_ms, missed
            );
        }
        LogEvent::UplinkSendFailed { command_id, detail } => {
            eprintln!(
                "ts_ms={} event=uplink_send_failed command_id={} detail={}",
                ts_ms, command_id, detail
            );
        }
    }
}
