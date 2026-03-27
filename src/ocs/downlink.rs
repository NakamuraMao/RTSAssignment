//! OCS: GCS downlink task (UDP send only).
//! queued 80% degraded, queueing delay / buffer rate, socket setup failure / send failure logs.
//! [`UdpSocket`] is pseudo-connected with `bind(0.0.0.0:0)` → `connect(gcs_addr)` and sent with `send` thereafter.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::gcs::types::MAX_PAYLOAD_LEN;
use crate::reliability::{CircuitBreaker, ExponentialBackoff};
use crate::wire::{FIXED_HEADER_BYTES, LENGTH_PREFIX_BYTES};

use super::test_env;
use super::time;
use super::types::{BenchmarkMetrics, DownlinkPacket};
use crate::supervisor::debug_log_ndjson;

const DEGRADED_THRESHOLD_PERCENT: u64 = 80;
const BACKOFF_BASE_MS: u64 = 20;
const BACKOFF_MAX_MS: u64 = 5_000;
const CIRCUIT_FAILURE_THRESHOLD: u32 = 3;
const CIRCUIT_OPEN_COOLDOWN_MS: u64 = 500;
/// Task requirement: if UDP bind+connect doesn't complete within this time, communication is missed.
const DOWNLINK_INIT_TIMEOUT_MS: u64 = 5;

/// Maximum frame bytes when `payload_len == MAX_PAYLOAD_LEN`.
/// Layout: [length u32][sequence u64][prepared_at u64][payload...]
const MAX_FRAME_BYTES: usize = LENGTH_PREFIX_BYTES + FIXED_HEADER_BYTES + MAX_PAYLOAD_LEN;

fn build_frame(packet: &DownlinkPacket) -> Option<([u8; MAX_FRAME_BYTES], usize)> {
    if packet.payload_len > MAX_PAYLOAD_LEN {
        return None;
    }

    let body_len = FIXED_HEADER_BYTES + packet.payload_len;
    let frame_len = LENGTH_PREFIX_BYTES + body_len;

    let length = u32::try_from(body_len).ok()?;

    let mut frame = [0_u8; MAX_FRAME_BYTES];
    frame[0..LENGTH_PREFIX_BYTES].copy_from_slice(&length.to_le_bytes());
    frame[LENGTH_PREFIX_BYTES..(LENGTH_PREFIX_BYTES + 8)]
        .copy_from_slice(&packet.sequence.to_le_bytes());
    frame[(LENGTH_PREFIX_BYTES + 8)..(LENGTH_PREFIX_BYTES + 16)]
        .copy_from_slice(&packet.prepared_at.0.to_le_bytes());

    let payload_off = LENGTH_PREFIX_BYTES + FIXED_HEADER_BYTES;
    let payload_end = payload_off + packet.payload_len;
    frame[payload_off..payload_end].copy_from_slice(&packet.payload[..packet.payload_len]);

    Some((frame, frame_len))
}

async fn setup_udp_connected(gcs_addr: &str) -> std::io::Result<UdpSocket> {
    let s = UdpSocket::bind("0.0.0.0:0").await?;
    s.connect(gcs_addr).await?;
    Ok(s)
}

/// downlink send task. 1 loop: recv → queued safe decrement → delay log → 80% degraded → UDP send.
pub async fn run_downlink(
    mut downlink_rx: mpsc::Receiver<DownlinkPacket>,
    gcs_addr: &str,
    capacity: usize,
    queued: Arc<AtomicUsize>,
    bench_metrics: Arc<Mutex<BenchmarkMetrics>>,
) {
    let capacity = capacity.max(1);
    let mut socket: Option<UdpSocket> = None;
    let mut degraded = false;
    let circuit = CircuitBreaker::new(CIRCUIT_FAILURE_THRESHOLD, CIRCUIT_OPEN_COOLDOWN_MS);
    let mut backoff = ExponentialBackoff::new(BACKOFF_BASE_MS, BACKOFF_MAX_MS);
    let mut setup_fail_count: u32 = 0;

    test_env::log_active_test_modes_once();
    let stall_enabled = test_env::force_downlink_stall();
    let stall_ms = test_env::downlink_stall_ms();
    let mut stall_once_done = false;

    'packets: while let Some(packet) = downlink_rx.recv().await {
        if stall_enabled && !stall_once_done {
            stall_once_done = true;
            crate::ocs_ts_eprintln!(
                "[downlink] test_stall sleep_ms={} (consumer paused; queue may fill)",
                stall_ms
            );
            tokio::time::sleep(Duration::from_millis(stall_ms)).await;
        }
        let prev = queued.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            if v > 0 {
                Some(v - 1)
            } else {
                None
            }
        });
        if prev.is_err() {
            queued.store(0, Ordering::Relaxed);
        }

        let now = time::now_ms();
        let queueing_delay_ms = now.0.saturating_sub(packet.prepared_at.0);
        if let Ok(mut m) = bench_metrics.try_lock() {
            m.max_tx_queue_latency_ms = m.max_tx_queue_latency_ms.max(queueing_delay_ms);
            m.tx_queue_latency_sum_ms = m.tx_queue_latency_sum_ms.saturating_add(queueing_delay_ms);
            m.tx_queue_latency_count = m.tx_queue_latency_count.saturating_add(1);
        }

        let q = queued.load(Ordering::Relaxed);
        let usage_pct: u64 = (q.saturating_add(1)).saturating_mul(100) as u64 / capacity as u64;
        if let Ok(mut m) = bench_metrics.try_lock() {
            m.peak_buffer_fill_rate_percent = m.peak_buffer_fill_rate_percent.max(usage_pct);
        }

        if usage_pct >= DEGRADED_THRESHOLD_PERCENT {
            if !degraded {
                degraded = true;
                if let Ok(mut m) = bench_metrics.try_lock() {
                    m.degraded_mode_enter_count = m.degraded_mode_enter_count.saturating_add(1);
                }
                crate::ocs_ts_eprintln!("[downlink] degraded_enter usage_pct={}%", usage_pct);
            }
            continue;
        } else if degraded {
            degraded = false;
            crate::ocs_ts_eprintln!("[downlink] degraded_exit usage_pct={}%", usage_pct);
        }

        if socket.is_none() {
            let now_ms = time::now_ms().0;
            if !circuit.allow_request(now_ms) {
                crate::ocs_ts_eprintln!(
                    "[downlink] circuit_open_skip_socket sequence={} state={:?}",
                    packet.sequence,
                    circuit.state()
                );
                tokio::time::sleep(Duration::from_millis(backoff.next_sleep_ms())).await;
                continue;
            }

            let init_at = time::now_ms();
            let extra_init_delay_ms = if test_env::force_init_timeout() {
                let ms = test_env::init_timeout_extra_ms();
                crate::ocs_ts_eprintln!(
                    "[downlink] test_init_timeout extra_delay_ms={} (before udp setup)",
                    ms
                );
                ms
            } else {
                0
            };
            match timeout(
                Duration::from_millis(DOWNLINK_INIT_TIMEOUT_MS),
                async {
                    if extra_init_delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(extra_init_delay_ms)).await;
                    }
                    setup_udp_connected(gcs_addr).await
                },
            )
            .await
            {
                Ok(Ok(s)) => {
                    socket = Some(s);
                    setup_fail_count = 0;
                    circuit.on_success();
                    backoff.reset();
                }
                Ok(Err(e)) => {
                    crate::ocs_ts_eprintln!(
                        "[downlink] udp_setup_io_failed sequence={} init_at={} err={}",
                        packet.sequence,
                        init_at.0,
                        e
                    );
                    setup_fail_count = setup_fail_count.saturating_add(1);
                    circuit.on_failure(now_ms);
                    if setup_fail_count <= 3 {
                        debug_log_ndjson(
                            "H_OCS_UDP_SETUP",
                            "src/ocs/downlink.rs:run_downlink",
                            "setup_io_failed",
                            &format!(
                                "{{\"gcs_addr\":\"{}\",\"sequence\":{},\"setup_fail_count\":{}}}",
                                gcs_addr, packet.sequence, setup_fail_count
                            ),
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(backoff.next_sleep_ms())).await;
                    continue;
                }
                Err(_elapsed) => {
                    let at = time::now_ms();
                    crate::ocs_ts_eprintln!(
                        "[downlink] missed_communication reason=init_timeout sequence={} event_at={} deadline_ms={} gcs_addr={}",
                        packet.sequence,
                        at.0,
                        DOWNLINK_INIT_TIMEOUT_MS,
                        gcs_addr
                    );
                    if let Ok(mut m) = bench_metrics.try_lock() {
                        m.missed_communication_count =
                            m.missed_communication_count.saturating_add(1);
                    }
                    setup_fail_count = setup_fail_count.saturating_add(1);
                    circuit.on_failure(now_ms);
                    debug_log_ndjson(
                        "H_OCS_UDP_SETUP",
                        "src/ocs/downlink.rs:run_downlink",
                        "init_timeout",
                        &format!(
                            "{{\"gcs_addr\":\"{}\",\"sequence\":{},\"deadline_ms\":{}}}",
                            gcs_addr, packet.sequence, DOWNLINK_INIT_TIMEOUT_MS
                        ),
                    );
                    tokio::time::sleep(Duration::from_millis(backoff.next_sleep_ms())).await;
                    continue;
                }
            }
        }

        let Some(sock) = socket.as_mut() else {
            continue 'packets;
        };

        let Some((frame, frame_len)) = build_frame(&packet) else {
            crate::ocs_ts_eprintln!(
                "[downlink] frame_too_large sequence={} payload_len={}",
                packet.sequence,
                packet.payload_len
            );
            continue;
        };

        match sock.send(&frame[..frame_len]).await {
            Ok(_) => {
                circuit.on_success();
            }
            Err(e) => {
                crate::ocs_ts_eprintln!(
                    "[downlink] send_failed sequence={} err={}",
                    packet.sequence,
                    e
                );
                socket = None;
                circuit.on_failure(time::now_ms().0);
            }
        }
    }
}
