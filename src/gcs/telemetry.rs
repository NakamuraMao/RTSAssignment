//! GCS telemetry: UDP receive, decode, reception latency/drift, re-request, loss of contact (3 consecutive failures).
//!
//! **Acquisition**: until the first successful decode, strict deadman / `note_failure` are disabled
//! (visibility window). If no packet arrives within `ACQUISITION_TIMEOUT_MS`, GCS shuts down.
//! After link establishment, `DEADMAN_MS` applies as before.

use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch};

use crate::gcs::decoder::decode;
use crate::gcs::logging::{log_event, LogEvent};
use crate::gcs::monitoring;
use crate::gcs::fault::FaultStateSnapshot;
use crate::gcs::telemetry_backlog::TelemetryBacklogSoa;
use crate::gcs::time;
use crate::gcs::types::{
    DecodeError, DownlinkFaultFlag, FaultMessage, FaultState, RetransmitRequest,
    TelemetrySequence,
};
use crate::reliability::{CircuitBreaker, Watchdog};
use crate::sensor_values::SensorQuantity;
use crate::wire::{LENGTH_PREFIX_BYTES, MIN_BODY_BYTES};
use crate::supervisor::debug_log_ndjson;

/// Maximum accepted telemetry body length (DoS guard).
const MAX_BODY_BYTES: usize = 64 * 1024;
/// Max bytes for one downlink datagram: length prefix + body.
const MAX_UDP_RECV_BYTES: usize = LENGTH_PREFIX_BYTES + MAX_BODY_BYTES;
/// Decode budget threshold in milliseconds (GCS-side decode CPU time).
const DECODE_BUDGET_MS: u64 = 3;
/// Deadman's switch: last successful decode tick from telemetry.
/// If no successful packet is decoded within this window, treat as loss of contact.
const DEADMAN_MS: u64 = 120;
/// Deadman's switch check period.
const DEADMAN_CHECK_INTERVAL_MS: u64 = 5;
/// Before the first successful decode: wait for OCS (visibility / acquisition). No strict deadman.
/// If still no packet after this, shutdown GCS.
const ACQUISITION_TIMEOUT_MS: u64 = 20_000;
/// Contact is considered lost after this many consecutive failures.
const CONTACT_LOSS_FAILURES: u32 = 3;
/// Circuit breaker open cooldown (ms) after failures (aligned with downlink/uplink).
const CIRCUIT_OPEN_COOLDOWN_MS: u64 = 500;
/// If the read/decode loop does not heartbeat for this long, treat as failure (hang detection).
const LOOP_STALE_MS: u64 = 5_000;
/// SoA backlog: recent decoded packets (fault column scanned without loading payloads).
const TELEMETRY_SOA_BACKLOG_CAP: usize = 64;

#[derive(Debug)]
enum TelemetryError {
    InvalidLengthPrefix { length: u32, min: usize },
    BodyTooLarge { length: u32, max: usize },
    DatagramSizeMismatch { n: usize, expected: usize },
}

impl Display for TelemetryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLengthPrefix { length, min } => {
                write!(f, "invalid length prefix: length={} min={}", length, min)
            }
            Self::BodyTooLarge { length, max } => {
                write!(f, "body too large: length={} max={}", length, max)
            }
            Self::DatagramSizeMismatch { n, expected } => {
                write!(
                    f,
                    "datagram size mismatch: got {} bytes, expected {}",
                    n, expected
                )
            }
        }
    }
}

#[derive(Debug, Default)]
struct ContactState {
    consecutive_failures: u32,
    contact_lost: bool,
}

/// Run telemetry UDP socket forever.
pub async fn run_telemetry(
    addr: &str,
    shutdown_tx: mpsc::Sender<()>,
    fault_tx: mpsc::Sender<FaultMessage>,
    fault_state_rx: watch::Receiver<FaultStateSnapshot>,
    nack_tx: mpsc::Sender<RetransmitRequest>,
) {
    let gcs_start_ms = time::now_ms().0;
    let contact_state = Arc::new(Mutex::new(ContactState::default()));
    let contact_circuit = Arc::new(CircuitBreaker::new(
        CONTACT_LOSS_FAILURES,
        CIRCUIT_OPEN_COOLDOWN_MS,
    ));
    let backlog = Arc::new(AtomicUsize::new(0));
    // After first successful decode; until then deadman and note_failure are no-ops.
    let comm_established = Arc::new(AtomicBool::new(false));
    let last_success_tick_ms = Arc::new(AtomicU64::new(0));

    let (_cancel_tx, cancel_rx) = watch::channel(false);

    {
        let contact_state_for_watchdog = Arc::clone(&contact_state);
        let contact_circuit_for_watchdog = Arc::clone(&contact_circuit);
        let last_success_tick_ms_for_watchdog = Arc::clone(&last_success_tick_ms);
        let comm_established_for_watchdog = Arc::clone(&comm_established);
        let shutdown_tx_for_watchdog = shutdown_tx.clone();
        let mut cancel_rx_for_watchdog = cancel_rx.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(DEADMAN_CHECK_INTERVAL_MS));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut last_deadman_miss_ms: Option<u64> = None;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let now = time::now_ms().0;
                        if !comm_established_for_watchdog.load(Ordering::Relaxed) {
                            if now.saturating_sub(gcs_start_ms) >= ACQUISITION_TIMEOUT_MS {
                                log_event(LogEvent::TelemetryAcquisitionTimeout {
                                    acquisition_timeout_ms: ACQUISITION_TIMEOUT_MS,
                                });
                                let _ = shutdown_tx_for_watchdog.try_send(());
                                break;
                            }
                            continue;
                        }
                        let last = last_success_tick_ms_for_watchdog.load(Ordering::Relaxed);
                        let elapsed = now.saturating_sub(last);
                        if elapsed > DEADMAN_MS {
                            let allow = last_deadman_miss_ms
                                .map(|prev| now.saturating_sub(prev) >= DEADMAN_MS)
                                .unwrap_or(true);
                            if allow {
                                note_failure(
                                    comm_established_for_watchdog.as_ref(),
                                    &contact_state_for_watchdog,
                                    &contact_circuit_for_watchdog,
                                    now,
                                    &shutdown_tx_for_watchdog,
                                );
                                last_deadman_miss_ms = Some(now);
                            }
                        }
                    }
                    _ = cancel_rx_for_watchdog.changed() => {
                        break;
                    }
                }
            }
        });
    }

    let socket = match UdpSocket::bind(addr).await {
        Ok(s) => {
            debug_log_ndjson(
                "H_BIND_LISTENER",
                "src/gcs/telemetry.rs:run_telemetry",
                "bind_ok",
                &format!("{{\"addr\":\"{}\"}}", addr),
            );
            s
        }
        Err(e) => {
            debug_log_ndjson(
                "H_BIND_LISTENER",
                "src/gcs/telemetry.rs:run_telemetry",
                "bind_failed",
                &format!("{{\"addr\":\"{}\"}}", addr),
            );
            log_event(LogEvent::Error {
                component: "telemetry",
                action: "bind",
                detail: format!("addr={} err={}", addr, e),
            });
            return;
        }
    };
    log_event(LogEvent::DownlinkListening {
        addr: addr.to_string(),
    });
    log_event(LogEvent::TelemetryWaitingFirstPacket {
        acquisition_timeout_ms: ACQUISITION_TIMEOUT_MS,
    });

    udp_recv_loop(
        socket,
        contact_state,
        backlog,
        last_success_tick_ms,
        comm_established,
        contact_circuit,
        shutdown_tx,
        fault_tx,
        fault_state_rx,
        nack_tx,
    )
    .await;
}

async fn udp_recv_loop(
    socket: UdpSocket,
    contact_state: Arc<Mutex<ContactState>>,
    backlog: Arc<AtomicUsize>,
    last_success_tick_ms: Arc<AtomicU64>,
    comm_established: Arc<AtomicBool>,
    contact_circuit: Arc<CircuitBreaker>,
    shutdown_tx: mpsc::Sender<()>,
    fault_tx: mpsc::Sender<FaultMessage>,
    fault_state_rx: watch::Receiver<FaultStateSnapshot>,
    nack_tx: mpsc::Sender<RetransmitRequest>,
) {
    let (_wd_cancel_tx, mut wd_cancel_rx) = watch::channel(false);
    let loop_watchdog = Watchdog::new(LOOP_STALE_MS, time::now_ms().0);
    let loop_watchdog_task = loop_watchdog.clone();
    let contact_state_loop_wd = Arc::clone(&contact_state);
    let contact_circuit_loop_wd = Arc::clone(&contact_circuit);
    let comm_established_loop_wd = Arc::clone(&comm_established);
    let shutdown_tx_loop_wd = shutdown_tx.clone();
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(Duration::from_millis(DEADMAN_CHECK_INTERVAL_MS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_stale_report_ms: Option<u64> = None;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = time::now_ms().0;
                    if loop_watchdog_task.is_stale(now) {
                        let allow = last_stale_report_ms
                            .map(|prev| now.saturating_sub(prev) >= LOOP_STALE_MS)
                            .unwrap_or(true);
                        if allow {
                            log_event(LogEvent::Error {
                                component: "telemetry",
                                action: "loop_watchdog_stale",
                                detail: format!(
                                    "elapsed_since_beat_ms={} last_beat_ms={}",
                                    now.saturating_sub(loop_watchdog_task.last_beat_ms()),
                                    loop_watchdog_task.last_beat_ms(),
                                ),
                            });
                            note_failure(
                                comm_established_loop_wd.as_ref(),
                                &contact_state_loop_wd,
                                &contact_circuit_loop_wd,
                                now,
                                &shutdown_tx_loop_wd,
                            );
                            last_stale_report_ms = Some(now);
                        }
                    }
                }
                res = wd_cancel_rx.changed() => {
                    if res.is_err() {
                        break;
                    }
                }
            }
        }
    });

    let mut first_recv_at_ms: Option<u64> = None;
    let mut first_prepared_at_ms: Option<u64> = None;
    let mut last_recv_at_ms: Option<u64> = None;
    let mut last_prepared_at_ms: Option<u64> = None;
    let mut last_seq: Option<u64> = None;
    let mut body_buf = vec![0_u8; MAX_BODY_BYTES];
    let mut recv_buf = vec![0u8; MAX_UDP_RECV_BYTES];
    let mut telemetry_soa_backlog = TelemetryBacklogSoa::new(TELEMETRY_SOA_BACKLOG_CAP);

    loop {
        loop_watchdog.tick(time::now_ms().0);

        let (n, peer) = match socket.recv_from(&mut recv_buf).await {
            Ok(v) => v,
            Err(e) => {
                log_event(LogEvent::Error {
                    component: "telemetry",
                    action: "recv_from",
                    detail: e.to_string(),
                });
                continue;
            }
        };

        log_event(LogEvent::DownlinkAccepted {
            peer: peer.to_string(),
        });

        if n < LENGTH_PREFIX_BYTES {
            note_failure(
                comm_established.as_ref(),
                &contact_state,
                &contact_circuit,
                time::now_ms().0,
                &shutdown_tx,
            );
            debug_log_ndjson(
                "H_TELEMETRY_UDP",
                "src/gcs/telemetry.rs:udp_recv_loop",
                "datagram_too_short",
                &format!("{{\"n\":{}}}", n),
            );
            continue;
        }

        let length = u32::from_le_bytes(recv_buf[0..LENGTH_PREFIX_BYTES].try_into().unwrap());

        let body_len = match validate_body_len(length) {
            Ok(v) => v,
            Err(e) => {
                note_failure(
                    comm_established.as_ref(),
                    &contact_state,
                    &contact_circuit,
                    time::now_ms().0,
                    &shutdown_tx,
                );
                debug_log_ndjson(
                    "H_TELEMETRY_UDP",
                    "src/gcs/telemetry.rs:udp_recv_loop",
                    "invalid_length",
                    "{\"branch\":\"invalid_length\"}",
                );
                log_event(LogEvent::Error {
                    component: "telemetry",
                    action: "invalid_length",
                    detail: e.to_string(),
                });
                continue;
            }
        };

        let expected_total = LENGTH_PREFIX_BYTES.saturating_add(body_len);
        if n != expected_total {
            note_failure(
                comm_established.as_ref(),
                &contact_state,
                &contact_circuit,
                time::now_ms().0,
                &shutdown_tx,
            );
            let e = TelemetryError::DatagramSizeMismatch {
                n,
                expected: expected_total,
            };
            log_event(LogEvent::Error {
                component: "telemetry",
                action: "datagram_size",
                detail: e.to_string(),
            });
            continue;
        }

        backlog.fetch_add(1, Ordering::Relaxed);
        monitoring::notify_telemetry_backlog(backlog.load(Ordering::Relaxed), time::now_ms().0);

        body_buf[..body_len].copy_from_slice(
            &recv_buf[LENGTH_PREFIX_BYTES..LENGTH_PREFIX_BYTES + body_len],
        );
        let body_len_local = body_len;

        let decode_started_ms = time::now_ms().0;
        let owned_body_buf = std::mem::take(&mut body_buf);
        let join_handle = tokio::task::spawn_blocking(move || {
            let slice = &owned_body_buf[..body_len_local];
            let res = decode(slice);
            (res, owned_body_buf)
        });

        let timed = tokio::time::timeout(Duration::from_millis(DECODE_BUDGET_MS), join_handle)
            .await;

        match timed {
            Ok(join_result) => match join_result {
                Ok((decoded, returned_buf)) => {
                    let decode_elapsed_ms = time::now_ms().0.saturating_sub(decode_started_ms);
                    let mut sensor_payload_rejected = false;

                    if let Ok(ref packet) = decoded {
                        let payload_off = 16usize;
                        let payload_end = payload_off.saturating_add(packet.payload_len);
                        if payload_end <= body_len_local {
                            let payload_slice = &returned_buf[payload_off..payload_end];
                            match SensorQuantity::try_from_wire_payload(payload_slice) {
                                Ok(_) => {
                                    telemetry_soa_backlog.push(
                                        TelemetrySequence(packet.sequence),
                                        packet.prepared_at.0,
                                        DownlinkFaultFlag(packet.fault_flag),
                                        payload_slice,
                                        packet.payload_len,
                                    );
                                    let _ = telemetry_soa_backlog.scan_faults_only();
                                }
                                Err(e) => {
                                    sensor_payload_rejected = true;
                                    log_event(LogEvent::Error {
                                        component: "telemetry",
                                        action: "sensor_quantity_validation",
                                        detail: e.to_string(),
                                    });
                                }
                            }
                        }
                    }

                    body_buf = returned_buf;

                    backlog.fetch_sub(1, Ordering::Relaxed);
                    monitoring::notify_telemetry_backlog(
                        backlog.load(Ordering::Relaxed),
                        time::now_ms().0,
                    );
                    log_event(LogEvent::TelemetryDecodeTiming {
                        elapsed_ms: decode_elapsed_ms,
                    });

                    if decode_elapsed_ms > DECODE_BUDGET_MS {
                        log_event(LogEvent::DecodeBudgetViolation {
                            elapsed_ms: decode_elapsed_ms,
                            budget_ms: DECODE_BUDGET_MS,
                        });
                        monitoring::notify_decode_budget_violation(
                            decode_elapsed_ms,
                            DECODE_BUDGET_MS,
                        );
                        note_failure(
                            comm_established.as_ref(),
                            &contact_state,
                            &contact_circuit,
                            time::now_ms().0,
                            &shutdown_tx,
                        );
                        continue;
                    }

                    if sensor_payload_rejected {
                        note_failure(
                            comm_established.as_ref(),
                            &contact_state,
                            &contact_circuit,
                            time::now_ms().0,
                            &shutdown_tx,
                        );
                        continue;
                    }

                    match decoded {
                        Ok(packet) => {
                            let now_ms = time::now_ms().0;
                            last_success_tick_ms.store(now_ms, Ordering::Relaxed);
                            if comm_established
                                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                                .is_ok()
                            {
                                log_event(LogEvent::TelemetryLinkEstablished {
                                    sequence: packet.sequence,
                                });
                            }
                            note_success(&contact_state, &contact_circuit);

                            if let Some(prev_seq) = last_seq {
                                let expected_seq = prev_seq.saturating_add(1);
                                if packet.sequence > expected_seq {
                                    log_event(LogEvent::TelemetryRerequestGap {
                                        expected_seq,
                                        received_seq: packet.sequence,
                                        missing_from: expected_seq,
                                        missing_to: packet.sequence.saturating_sub(1),
                                    });
                                    monitoring::notify_rerequest_gap(
                                        expected_seq,
                                        packet.sequence,
                                        expected_seq,
                                        packet.sequence.saturating_sub(1),
                                    );
                                    let missing_from = expected_seq;
                                    let missing_to = packet.sequence.saturating_sub(1);
                                    match nack_tx.try_send(RetransmitRequest {
                                        missing_from,
                                        missing_to,
                                    }) {
                                        Ok(()) => {
                                            log_event(LogEvent::TelemetryNackEnqueued {
                                                missing_from,
                                                missing_to,
                                            });
                                        }
                                        Err(_) => {
                                            log_event(LogEvent::TelemetryNackQueueFull {
                                                missing_from,
                                                missing_to,
                                            });
                                        }
                                    }
                                }
                            }

                            let recv_at_ms = time::now_ms().0;
                            if first_recv_at_ms.is_none() {
                                first_recv_at_ms = Some(recv_at_ms);
                            }
                            if first_prepared_at_ms.is_none() {
                                first_prepared_at_ms = Some(packet.prepared_at.0);
                            }

                            let recv_offset_ms = recv_at_ms
                                .saturating_sub(first_recv_at_ms.unwrap_or(recv_at_ms));
                            let prepared_offset_ms = packet
                                .prepared_at
                                .0
                                .saturating_sub(first_prepared_at_ms.unwrap_or(packet.prepared_at.0));
                            let rx_latency_ms = recv_offset_ms.saturating_sub(prepared_offset_ms);
                            log_event(LogEvent::TelemetryRxLatency {
                                sequence: packet.sequence,
                                latency_ms: rx_latency_ms,
                            });
                            monitoring::notify_rx_latency(packet.sequence, rx_latency_ms);

                            if let (Some(prev_recv), Some(prev_prepared)) =
                                (last_recv_at_ms, last_prepared_at_ms)
                            {
                                let expected_interval_ms =
                                    packet.prepared_at.0.saturating_sub(prev_prepared);
                                let actual_interval_ms = recv_at_ms.saturating_sub(prev_recv);
                                let drift_ms =
                                    actual_interval_ms as i64 - expected_interval_ms as i64;
                                log_event(LogEvent::TelemetryRxDrift {
                                    sequence: packet.sequence,
                                    expected_interval_ms,
                                    actual_interval_ms,
                                    drift_ms,
                                });
                                monitoring::notify_rx_drift(
                                    packet.sequence,
                                    expected_interval_ms,
                                    actual_interval_ms,
                                    drift_ms,
                                );
                            }

                            last_recv_at_ms = Some(recv_at_ms);
                            last_prepared_at_ms = Some(packet.prepared_at.0);
                            last_seq = Some(packet.sequence);

                            if packet.fault_flag {
                                let (state, _) = *fault_state_rx.borrow();
                                if state == FaultState::Nominal {
                                    let _ = fault_tx.try_send(FaultMessage {
                                        at: time::now_ms(),
                                        state: FaultState::Degraded,
                                        source: "downlink".to_string(),
                                        detail: "telemetry payload fault flag".to_string(),
                                    });
                                }
                            }

                            log_event(LogEvent::DownlinkDecoded {
                                sequence: packet.sequence,
                                prepared_at_ms: packet.prepared_at.0,
                                payload_len: packet.payload_len,
                            });
                        }
                        Err(e) => {
                            note_failure(
                                comm_established.as_ref(),
                                &contact_state,
                                &contact_circuit,
                                time::now_ms().0,
                                &shutdown_tx,
                            );
                            log_event(LogEvent::TelemetryRerequestDecodeFailure {
                                reason: decode_failure_reason(&e),
                            });
                            monitoring::notify_rerequest_decode_failure(
                                decode_failure_reason(&e),
                            );
                            log_event(LogEvent::Error {
                                component: "telemetry",
                                action: "decode",
                                detail: e.to_string(),
                            });
                        }
                    }
                }
                Err(_join_err) => {
                    let decode_elapsed_ms = time::now_ms().0.saturating_sub(decode_started_ms);
                    backlog.fetch_sub(1, Ordering::Relaxed);
                    monitoring::notify_telemetry_backlog(
                        backlog.load(Ordering::Relaxed),
                        time::now_ms().0,
                    );
                    log_event(LogEvent::TelemetryDecodeTiming {
                        elapsed_ms: decode_elapsed_ms,
                    });
                    note_failure(
                        comm_established.as_ref(),
                        &contact_state,
                        &contact_circuit,
                        time::now_ms().0,
                        &shutdown_tx,
                    );
                    debug_log_ndjson(
                        "H_TELEMETRY_UDP",
                        "src/gcs/telemetry.rs:udp_recv_loop",
                        "decode_join_err",
                        "{\"branch\":\"decode_join_err\"}",
                    );
                    continue;
                }
            },
            Err(_timeout) => {
                let decode_elapsed_ms = time::now_ms().0.saturating_sub(decode_started_ms);

                backlog.fetch_sub(1, Ordering::Relaxed);
                monitoring::notify_telemetry_backlog(
                    backlog.load(Ordering::Relaxed),
                    time::now_ms().0,
                );
                log_event(LogEvent::TelemetryDecodeTiming {
                    elapsed_ms: decode_elapsed_ms,
                });
                log_event(LogEvent::DecodeBudgetViolation {
                    elapsed_ms: decode_elapsed_ms,
                    budget_ms: DECODE_BUDGET_MS,
                });
                monitoring::notify_decode_budget_violation(decode_elapsed_ms, DECODE_BUDGET_MS);
                log_event(LogEvent::TelemetryRerequestDecodeFailure {
                    reason: "decode_timeout",
                });
                monitoring::notify_rerequest_decode_failure("decode_timeout");

                note_failure(
                    comm_established.as_ref(),
                    &contact_state,
                    &contact_circuit,
                    time::now_ms().0,
                    &shutdown_tx,
                );
                debug_log_ndjson(
                    "H_TELEMETRY_UDP",
                    "src/gcs/telemetry.rs:udp_recv_loop",
                    "decode_timeout",
                    "{\"branch\":\"decode_timeout\"}",
                );
                continue;
            }
        }
    }
}

fn validate_body_len(length: u32) -> Result<usize, TelemetryError> {
    let len = length as usize;
    if len < MIN_BODY_BYTES {
        return Err(TelemetryError::InvalidLengthPrefix {
            length,
            min: MIN_BODY_BYTES,
        });
    }
    if len > MAX_BODY_BYTES {
        return Err(TelemetryError::BodyTooLarge {
            length,
            max: MAX_BODY_BYTES,
        });
    }
    Ok(len)
}

fn decode_failure_reason(err: &DecodeError) -> &'static str {
    match err {
        DecodeError::FrameTooShort { .. } => "frame_too_short",
        DecodeError::InvalidLengthPrefix { .. } => "invalid_length_prefix",
        DecodeError::LengthMismatch { .. } => "length_mismatch",
        DecodeError::PayloadTooLarge { .. } => "payload_too_large",
    }
}

fn note_failure(
    comm_established: &AtomicBool,
    contact_state: &Arc<Mutex<ContactState>>,
    circuit: &CircuitBreaker,
    now_ms: u64,
    shutdown_tx: &mpsc::Sender<()>,
) {
    if !comm_established.load(Ordering::Relaxed) {
        return;
    }
    circuit.on_failure(now_ms);
    let Ok(mut state) = contact_state.lock() else {
        return;
    };
    state.consecutive_failures = state.consecutive_failures.saturating_add(1);
    if !state.contact_lost && state.consecutive_failures >= CONTACT_LOSS_FAILURES {
        state.contact_lost = true;
        log_event(LogEvent::TelemetryContactLost {
            consecutive_failures: state.consecutive_failures,
        });
        monitoring::notify_contact_lost(state.consecutive_failures);
        let sent_ok = shutdown_tx.try_send(()).is_ok();
        debug_log_ndjson(
            "H_DEADMAN_CONTACT_LOST",
            "src/gcs/telemetry.rs:note_failure",
            "contact_lost_triggered",
            &format!(
                "{{\"consecutive_failures\":{},\"shutdown_try_send_ok\":{}}}",
                state.consecutive_failures, sent_ok
            ),
        );
    }
}

fn note_success(contact_state: &Arc<Mutex<ContactState>>, circuit: &CircuitBreaker) {
    circuit.on_success();
    let Ok(mut state) = contact_state.lock() else {
        return;
    };
    let was_lost = state.contact_lost;
    state.consecutive_failures = 0;
    if was_lost {
        state.contact_lost = false;
        log_event(LogEvent::TelemetryContactRestored);
        monitoring::notify_contact_restored();
    }
}
