//! SoA telemetry backlog: fault flags and metadata in separate columns from full payloads.
//! Hot-path scan uses only the `fault` column without loading heavy payload arrays.

use crate::gcs::types::{DownlinkFaultFlag, TelemetrySequence, MAX_PAYLOAD_LEN};

/// Ring buffer: `fault` / `sequence` / `prepared_at_ms` are cache-friendly columns; `payloads`
/// holds full downlink payload for optional downstream use without blocking fault-only scans.
pub struct TelemetryBacklogSoa {
    capacity: usize,
    len: usize,
    head: usize,
    fault: Vec<bool>,
    sequence: Vec<u64>,
    prepared_at_ms: Vec<u64>,
    payloads: Vec<[u8; MAX_PAYLOAD_LEN]>,
}

impl TelemetryBacklogSoa {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            len: 0,
            head: 0,
            fault: vec![false; capacity],
            sequence: vec![0; capacity],
            prepared_at_ms: vec![0; capacity],
            payloads: vec![[0u8; MAX_PAYLOAD_LEN]; capacity],
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Push one decoded telemetry entry. Overwrites oldest when full.
    pub fn push(
        &mut self,
        seq: TelemetrySequence,
        prepared_at_ms: u64,
        fault: DownlinkFaultFlag,
        payload: &[u8],
        payload_len: usize,
    ) {
        let idx = if self.len < self.capacity {
            let i = (self.head + self.len) % self.capacity;
            self.len += 1;
            i
        } else {
            let i = self.head;
            self.head = (self.head + 1) % self.capacity;
            i
        };

        self.fault[idx] = fault.0;
        self.sequence[idx] = seq.0;
        self.prepared_at_ms[idx] = prepared_at_ms;
        let n = payload_len.min(MAX_PAYLOAD_LEN);
        self.payloads[idx] = [0u8; MAX_PAYLOAD_LEN];
        self.payloads[idx][..n].copy_from_slice(&payload[..n]);
    }

    /// Scan only the fault column (no payload access). Returns true if any entry is faulted.
    #[must_use]
    pub fn scan_faults_only(&self) -> bool {
        for i in 0..self.len {
            let idx = (self.head + i) % self.capacity;
            if self.fault[idx] {
                return true;
            }
        }
        false
    }
}

/// Standalone scan for benchmarks / tests: `flags` is the SoA fault column only.
#[must_use]
pub fn scan_fault_flags(flags: &[bool]) -> bool {
    flags.iter().any(|&f| f)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_overwrite_and_scan() {
        let mut b = TelemetryBacklogSoa::new(2);
        b.push(
            TelemetrySequence(1),
            10,
            DownlinkFaultFlag(false),
            &[],
            0,
        );
        b.push(
            TelemetrySequence(2),
            20,
            DownlinkFaultFlag(true),
            &[],
            0,
        );
        assert!(b.scan_faults_only());
        b.push(
            TelemetrySequence(3),
            30,
            DownlinkFaultFlag(false),
            &[],
            0,
        );
        // Oldest dropped: seq 1 gone; 2 and 3 remain — still has fault from seq 2
        assert!(b.scan_faults_only());
        b.push(
            TelemetrySequence(4),
            40,
            DownlinkFaultFlag(false),
            &[],
            0,
        );
        // seq 2 dropped; only 3,4 — no fault
        assert!(!b.scan_faults_only());
    }
}
