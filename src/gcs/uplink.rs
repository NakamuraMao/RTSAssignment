//! GCS uplink: send validated commands to OCS over UDP, record send time for jitter/deadline to monitoring.
//!
//! # Phase0: dispatch definition (fixed)
//!
//! - **actual_dispatch_at**: uplink socket `send` before start time captured by [`crate::gcs::time::now_ms()`].
//! - **NACK**: telemetry detected sequence gap as [`RetransmitRequest`] and sent to the same actor,
//!   sent to OCS in the same UDP frame format as normal commands.
//!
//! - Other measurements/queues/interlocks/wakeup strategy refer to the module documentation of [`crate::gcs::scheduler`].

use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use crate::gcs::logging::{log_event, LogEvent};
use crate::gcs::time;
use crate::gcs::types::{
    DispatchOutcome, DispatchResult, NACK_COMMAND_ID, RetransmitRequest, ScheduledCommand,
    TimestampMs, MAX_PAYLOAD_LEN,
};
use crate::reliability::{CircuitBreaker, ExponentialBackoff};

const CIRCUIT_FAILURE_THRESHOLD: u32 = 3;
const CIRCUIT_OPEN_COOLDOWN_MS: u64 = 500;
const BACKOFF_BASE_MS: u64 = 50;
const BACKOFF_MAX_MS: u64 = 10_000;

/// Uplink frame: [length: u32 LE][command_id: u64 LE][payload_len: u16 LE][payload].
const HEADER_LEN: usize = 4 + 8 + 2;
const SEND_BUF_LEN: usize = HEADER_LEN + MAX_PAYLOAD_LEN;

const NACK_PAYLOAD_LEN: usize = 16;

async fn ensure_udp_connected(
    ocs_addr: &str,
    socket: &mut Option<UdpSocket>,
    circuit: &CircuitBreaker,
    backoff: &mut ExponentialBackoff,
) {
    while socket.is_none() {
        let now_ms = time::now_ms().0;
        if !circuit.allow_request(now_ms) {
            tokio::time::sleep(Duration::from_millis(backoff.next_sleep_ms())).await;
            continue;
        }
        match UdpSocket::bind("0.0.0.0:0").await {
            Ok(s) => match s.connect(ocs_addr).await {
                Ok(()) => {
                    *socket = Some(s);
                    circuit.on_success();
                    backoff.reset();
                    break;
                }
                Err(_) => {
                    circuit.on_failure(now_ms);
                    tokio::time::sleep(Duration::from_millis(backoff.next_sleep_ms())).await;
                }
            },
            Err(_) => {
                circuit.on_failure(now_ms);
                tokio::time::sleep(Duration::from_millis(backoff.next_sleep_ms())).await;
            }
        }
    }
}

/// Send one validated command. Takes **actual_dispatch_at** immediately before first `send` (no heap).
pub async fn send_validated(
    socket: &UdpSocket,
    scheduled: &ScheduledCommand,
) -> (TimestampMs, DispatchResult) {
    let cmd = &scheduled.command;
    let payload_len = cmd.payload_len.min(MAX_PAYLOAD_LEN);
    let body_len = 8 + 2 + payload_len;
    let total_len = 4 + body_len;

    let mut buf = [0u8; SEND_BUF_LEN];
    buf[0..4].copy_from_slice(&(body_len as u32).to_le_bytes());
    buf[4..12].copy_from_slice(&cmd.id.to_le_bytes());
    buf[12..14].copy_from_slice(&(payload_len as u16).to_le_bytes());
    buf[14..14 + payload_len].copy_from_slice(&cmd.payload[..payload_len]);

    let actual_dispatch_at = time::now_ms();

    match socket.send(&buf[..total_len]).await {
        Ok(_) => (actual_dispatch_at, DispatchResult::Sent),
        Err(e) => {
            let detail = match e.kind() {
                std::io::ErrorKind::ConnectionRefused => "connection_refused",
                std::io::ErrorKind::TimedOut => "timed_out",
                _ => "send_failed",
            };
            (actual_dispatch_at, DispatchResult::Failed { detail })
        }
    }
}

/// Same frame layout as [`send_validated`]; `payload` = two `u64` LE (missing range).
pub async fn send_nack_frame(
    socket: &UdpSocket,
    req: RetransmitRequest,
) -> Result<(), &'static str> {
    let body_len = 8 + 2 + NACK_PAYLOAD_LEN;
    let total_len = 4 + body_len;

    let mut buf = [0u8; SEND_BUF_LEN];
    buf[0..4].copy_from_slice(&(body_len as u32).to_le_bytes());
    buf[4..12].copy_from_slice(&NACK_COMMAND_ID.to_le_bytes());
    buf[12..14].copy_from_slice(&(NACK_PAYLOAD_LEN as u16).to_le_bytes());
    buf[14..22].copy_from_slice(&req.missing_from.to_le_bytes());
    buf[22..30].copy_from_slice(&req.missing_to.to_le_bytes());

    match socket.send(&buf[..total_len]).await {
        Ok(_) => Ok(()),
        Err(_) => Err("send_failed"),
    }
}

/// Uplink task: `ScheduledCommand` と NACK（再送要求）を同じ UDP ソケットで送信。
pub async fn run_uplink_task(
    ocs_addr: &str,
    mut command_rx: mpsc::Receiver<ScheduledCommand>,
    mut nack_rx: mpsc::Receiver<RetransmitRequest>,
    result_tx: mpsc::Sender<DispatchOutcome>,
) {
    let circuit = CircuitBreaker::new(CIRCUIT_FAILURE_THRESHOLD, CIRCUIT_OPEN_COOLDOWN_MS);
    let mut backoff = ExponentialBackoff::new(BACKOFF_BASE_MS, BACKOFF_MAX_MS);
    let mut socket: Option<UdpSocket> = None;

    loop {
        tokio::select! {
            biased;
            nack = nack_rx.recv() => {
                let Some(req) = nack else { continue };
                let t0 = Instant::now();
                ensure_udp_connected(ocs_addr, &mut socket, &circuit, &mut backoff).await;
                let s = socket.as_mut().expect("socket connected");
                match send_nack_frame(s, req).await {
                    Ok(()) => {
                        circuit.on_success();
                        log_event(LogEvent::UplinkNackSent {
                            missing_from: req.missing_from,
                            missing_to: req.missing_to,
                            enqueue_to_send_us: t0.elapsed().as_micros() as u64,
                        });
                    }
                    Err(_) => {
                        socket = None;
                        circuit.on_failure(time::now_ms().0);
                    }
                }
            }
            cmd = command_rx.recv() => {
                let Some(scheduled) = cmd else { break };
                ensure_udp_connected(ocs_addr, &mut socket, &circuit, &mut backoff).await;
                let s = socket.as_mut().expect("socket connected");
                let (actual_dispatch_at, result) = send_validated(s, &scheduled).await;
                if matches!(result, DispatchResult::Failed { .. }) {
                    socket = None;
                    circuit.on_failure(time::now_ms().0);
                } else {
                    circuit.on_success();
                }
                let outcome = DispatchOutcome {
                    command_id: scheduled.command.id,
                    actual_dispatch_at,
                    result,
                    execute_at_ms: scheduled.execute_at.0,
                    deadline_ms: scheduled.deadline.0,
                };
                if result_tx.send(outcome).await.is_err() {
                    break;
                }
            }
        }
    }
}
