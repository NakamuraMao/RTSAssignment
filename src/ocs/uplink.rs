//! OCS: receive UDP commands from GCS uplink (only receive / validation. scheduler coordination is possible in an extension task).

use tokio::net::UdpSocket;

use crate::gcs::types::{MAX_PAYLOAD_LEN, NACK_COMMAND_ID};

const MAX_DATAGRAM: usize = 4 + 8 + 2 + MAX_PAYLOAD_LEN;

/// same frame length limit as GCS [`crate::gcs::uplink`].
pub async fn run_uplink_listener(addr: &str) {
    let socket = match UdpSocket::bind(addr).await {
        Ok(s) => s,
        Err(e) => {
            crate::ocs_ts_eprintln!("[uplink] bind_failed addr={} err={}", addr, e);
            return;
        }
    };
    crate::ocs_ts_eprintln!("[uplink] listening udp addr={}", addr);
    let mut buf = [0u8; MAX_DATAGRAM];
    loop {
        match socket.recv_from(&mut buf).await {
            Ok((n, peer)) => {
                if n < 4 {
                    continue;
                }
                let body_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
                let expected = 4usize.saturating_add(body_len);
                if n != expected {
                    crate::ocs_ts_eprintln!(
                        "[uplink] datagram_size_mismatch peer={} n={} expected={}",
                        peer,
                        n,
                        expected
                    );
                    continue;
                }
                if body_len < 8 + 2 {
                    continue;
                }
                let command_id = u64::from_le_bytes(buf[4..12].try_into().unwrap());
                let payload_len = u16::from_le_bytes(buf[12..14].try_into().unwrap()) as usize;
                if payload_len > MAX_PAYLOAD_LEN {
                    crate::ocs_ts_eprintln!(
                        "[uplink] payload_too_large peer={} payload_len={}",
                        peer,
                        payload_len
                    );
                    continue;
                }
                if command_id == NACK_COMMAND_ID {
                    if payload_len >= 16 {
                        let missing_from = u64::from_le_bytes(buf[14..22].try_into().unwrap());
                        let missing_to = u64::from_le_bytes(buf[22..30].try_into().unwrap());
                        crate::ocs_ts_eprintln!(
                            "[uplink] nack_recv peer={} missing_from={} missing_to={}",
                            peer,
                            missing_from,
                            missing_to
                        );
                    } else {
                        crate::ocs_ts_eprintln!(
                            "[uplink] nack_invalid_payload peer={} payload_len={}",
                            peer,
                            payload_len
                        );
                    }
                    continue;
                }
                crate::ocs_ts_eprintln!(
                    "[uplink] recv peer={} command_id={} payload_len={}",
                    peer,
                    command_id,
                    payload_len
                );
            }
            Err(e) => crate::ocs_ts_eprintln!("[uplink] recv_err={}", e),
        }
    }
}
