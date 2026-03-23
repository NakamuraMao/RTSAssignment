//! GCS decoder: decode body bytes to `DecodedPacket` (wire format A body:
//! [sequence u64][prepared_at u64][payload...]). 3ms measured by caller.

use std::convert::TryInto;

use crate::gcs::types::{DecodeError, DecodedPacket, TimestampMs, MAX_PAYLOAD_LEN};
use crate::wire::MIN_BODY_BYTES;

/// Decode one frame body (without the 4-byte wire length prefix).
pub fn decode(raw: &[u8]) -> Result<DecodedPacket, DecodeError> {
    if raw.len() < MIN_BODY_BYTES {
        return Err(DecodeError::FrameTooShort {
            actual: raw.len(),
            min: MIN_BODY_BYTES,
        });
    }

    let sequence = u64::from_le_bytes(
        raw[0..8]
            .try_into()
            .map_err(|_| DecodeError::FrameTooShort {
                actual: raw.len(),
                min: MIN_BODY_BYTES,
            })?,
    );
    let prepared_at = u64::from_le_bytes(
        raw[8..16]
            .try_into()
            .map_err(|_| DecodeError::FrameTooShort {
                actual: raw.len(),
                min: MIN_BODY_BYTES,
            })?,
    );
    let payload_len = raw.len().saturating_sub(16);
    if payload_len > MAX_PAYLOAD_LEN {
        return Err(DecodeError::PayloadTooLarge {
            actual: payload_len,
            max: MAX_PAYLOAD_LEN,
        });
    }

    let fault_flag = if payload_len > 0 {
        raw[16] != 0
    } else {
        false
    };

    Ok(DecodedPacket {
        sequence,
        prepared_at: TimestampMs(prepared_at),
        payload_len,
        fault_flag,
    })
}

/// Backward-compatible alias for callsites using the old name.
pub fn decode_frame(body: &[u8]) -> Result<DecodedPacket, DecodeError> {
    decode(body)
}

#[cfg(test)]
mod tests {
    use super::decode;
    use crate::gcs::types::{DecodeError, DecodedPacket, TimestampMs};

    #[test]
    fn decode_ok_non_empty_payload() {
        let sequence = 42_u64;
        let prepared_at = 1_234_u64;
        let payload = vec![0xAA, 0xBB, 0xCC];

        let mut body = Vec::new();
        body.extend_from_slice(&sequence.to_le_bytes());
        body.extend_from_slice(&prepared_at.to_le_bytes());
        body.extend_from_slice(&payload);

        let decoded = decode(&body).expect("decode should succeed");
        assert_eq!(
            decoded,
            DecodedPacket {
                sequence,
                prepared_at: TimestampMs(prepared_at),
                payload_len: payload.len(),
                fault_flag: true,
            }
        );
    }

    #[test]
    fn decode_fault_flag_clear_when_payload_first_byte_zero() {
        let sequence = 1_u64;
        let prepared_at = 2_u64;
        let payload = vec![0x00, 0x11];

        let mut body = Vec::new();
        body.extend_from_slice(&sequence.to_le_bytes());
        body.extend_from_slice(&prepared_at.to_le_bytes());
        body.extend_from_slice(&payload);

        let decoded = decode(&body).expect("decode should succeed");
        assert!(!decoded.fault_flag);
    }

    #[test]
    fn decode_ok_empty_payload() {
        let sequence = 7_u64;
        let prepared_at = 9_u64;

        let mut body = Vec::new();
        body.extend_from_slice(&sequence.to_le_bytes());
        body.extend_from_slice(&prepared_at.to_le_bytes());

        let decoded = decode(&body).expect("decode should succeed");
        assert_eq!(
            decoded,
            DecodedPacket {
                sequence,
                prepared_at: TimestampMs(prepared_at),
                payload_len: 0,
                fault_flag: false,
            }
        );
    }

    #[test]
    fn decode_err_too_short() {
        let too_short = [0_u8; 15];
        let err = decode(&too_short).expect_err("decode should fail");
        assert_eq!(
            err,
            DecodeError::FrameTooShort {
                actual: 15,
                min: 16
            }
        );
    }
}
