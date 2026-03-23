# GCS (Ground Control System) Documentation

## How to run
GCS receives telemetry (downlink) over UDP, schedules commands to be sent at the intended execution time, and monitors the system while managing `fault` / `interlock`.

1. Start `OCS`: `cargo run --bin ocs`
2. Start `GCS` in another terminal: `cargo run --bin gcs`

UDP assumptions (hard-coded in the code):
- GCS downlink receive address: `127.0.0.1:9000`
- GCS uplink destination (OCS): `127.0.0.1:9001`

Stop with `Ctrl+C`.

## Design Overview

```mermaid
flowchart LR
  subgraph GCS[GCS]
    Telemetry[Telemetry UDP receive + decode + backlog + NACK/Fault cues]
    Decoder[decoder]
    Backlog[TelemetryBacklogSoa]
    Fault[Fault task (watch FaultState)]
    Interlocks[Interlocks (block unsafe commands)]
    Scheduler[Scheduler (Urgent FIFO + Normal EDF)]
    Uplink[Uplink UDP send (commands + NACK frames)]
    Monitoring[Monitoring snapshot metrics]
    SystemLoad[System load sampler]
    Logger[logging / scheduler log events]

    Telemetry --> Decoder
    Telemetry --> Backlog
    Telemetry -->|FaultMessage| Fault
    Fault -->|FaultState watch| Interlocks
    Scheduler -->|ScheduledCommand| Uplink
    Interlocks --> Scheduler
    Scheduler --> Logger
    Scheduler --> Monitoring
    Uplink --> Monitoring
    Telemetry --> Monitoring
    SystemLoad --> Monitoring
  end
```

## Module Files (one-line description for each `src/gcs/*.rs`)
- `src/gcs/mod.rs`: GCS module entry point that ties together `time/types/logging/decoder/telemetry/telemetry_backlog/fault/interlocks/uplink/scheduler/monitoring/system_load/runtime/test_commands`.
- `src/gcs/time.rs`: Defines the runtime time origin and provides `now_ms()` returning elapsed milliseconds as `TimestampMs`.
- `src/gcs/types.rs`: Common RT-boundary types (Command/ScheduledCommand/FaultState/metrics/log events, etc.).
- `src/gcs/logging.rs`: Minimal logging API for key=value-style `LogEvent`s.
- `src/gcs/decoder.rs`: Decodes telemetry downlink frames (sequence/prepared_at/payload_len/fault_flag).
- `src/gcs/telemetry_backlog.rs`: A SoA-style ring buffer that stores fault/sequence/prepared_at and payload.
- `src/gcs/telemetry.rs`: UDP receive -> decode -> deadman/contact-loss handling -> triggers fault updates and re-request (NACK) behavior.
- `src/gcs/fault.rs`: Receives `FaultMessage`, updates `FaultState`, and notifies monitoring about 100ms critical alerts and recovery timing.
- `src/gcs/interlocks.rs`: Blocks unsafe command names based on `FaultState` and logs rejection reasons.
- `src/gcs/scheduler.rs`: Uses a single heap to combine Urgent FIFO and Normal EDF, validates via interlocks, then dispatches to uplink.
- `src/gcs/uplink.rs`: Sends scheduled commands and NACK retransmit frames over UDP and measures jitter/deadline based on the actual dispatch time.
- `src/gcs/monitoring.rs`: Aggregates jitter/backlog/drift/deadline misses/recovery/interlock latency/system load and produces `snapshot_metrics()`.
- `src/gcs/system_load.rs`: Uses sysinfo to periodically sample CPU usage and forward it to monitoring.
- `src/gcs/command_state.rs`: Uses typestate (`UnsafeAllowed`/`UnsafeBlocked`) to model the unsafe command boundary in types.
- `src/gcs/runtime.rs`: Under `Supervisor`, spawns telemetry/fault/scheduler/uplink/monitoring and saves a monitoring snapshot on shutdown.
- `src/gcs/test_commands.rs`: Integration-test helper that injects faults and enqueues commands to verify interlock/uplink/recovery.

## Monitoring results (reference: `gcs/monitoring.rs`)
In `src/gcs/runtime.rs`, `monitoring::snapshot_metrics()` is printed to `stderr` every 10 seconds.
On shutdown, it also saves a snapshot to `./logs/gcs_shutdown_snapshot_monitoring_<timestamp>.txt`.

The snapshot includes the following sections:
- `Uplink jitter`
- `Telemetry backlog`
- `Scheduler drift`
- `Missed deadlines`
- `Fault recovery`
- `Interlock latency`
- `System load`

