//! Criterion: SoA fault-flag column scan (telemetry backlog depth).

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use RTSassignment::gcs::telemetry_backlog::{scan_fault_flags, TelemetryBacklogSoa};
use RTSassignment::gcs::types::{DownlinkFaultFlag, MAX_PAYLOAD_LEN, TelemetrySequence};

fn build_backlog(n: usize, fault_every: usize) -> TelemetryBacklogSoa {
    let mut b = TelemetryBacklogSoa::new(n.max(1));
    let payload = [0u8; MAX_PAYLOAD_LEN];
    for i in 0..n {
        let fault = fault_every > 0 && i % fault_every == 0;
        b.push(
            TelemetrySequence(i as u64),
            i as u64,
            DownlinkFaultFlag(fault),
            &payload,
            MAX_PAYLOAD_LEN,
        );
    }
    b
}

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("telemetry_backlog_soa");
    for n in [64usize, 256, 1024] {
        let backlog = build_backlog(n, 17);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(backlog.scan_faults_only()));
        });
    }
    group.finish();

    c.bench_function("scan_fault_flags_slice", |b| {
        let flags: Vec<bool> = (0..1024).map(|i| i % 31 == 0).collect();
        b.iter(|| black_box(scan_fault_flags(black_box(&flags))));
    });
}

criterion_group!(benches, bench_scan);
criterion_main!(benches);
