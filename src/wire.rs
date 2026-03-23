//! Wire-format contract (phase0, scheme A).
//!
//! One UDP datagram payload (same byte layout as the former TCP stream framing):
//! `[length: u32][sequence: u64][prepared_at: u64][payload...]`
//!
//! - Endian: little-endian for all integer fields.
//! - `length` means body length in bytes:
//!   `length = 8 (sequence) + 8 (prepared_at) + payload.len()`.
//! - `length == 16` is valid (empty payload).
//! - Downlink payload from OCS: byte 0 may be a fault indicator (0 = nominal, non-zero = fault);
//!   bytes 1+ hold sensor data and optional padding.

/// Length prefix size in bytes.
pub const LENGTH_PREFIX_BYTES: usize = 4;

/// Fixed header size in bytes (without length prefix).
pub const FIXED_HEADER_BYTES: usize = 16;

/// Minimum body length in bytes.
pub const MIN_BODY_BYTES: usize = FIXED_HEADER_BYTES;
