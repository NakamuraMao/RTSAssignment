# OCS (Onboard Control System) Documentation

## How to run
OCS receives commands (UDP) from `GCS` and runs the pipeline in this order: sensors -> safety -> scheduling -> downlink.

1. Start `GCS` in another terminal: `cargo run --bin gcs`
2. Start `OCS` in a separate terminal: `cargo run --bin ocs`
test environment
1. Intentionally trigger Degraded mode / buffer drops
run:
OCS_TEST_FORCE_DOWNLINK_STALL=true OCS_TEST_DOWNLINK_STALL_MS=5000 cargo run --bin ocs > "logs/ocs_$(date +%Y%m%d_%H%M%S).log" 2>&1

2. Trigger a downlink initialization timeout (>5 ms)
run:
OCS_TEST_FORCE_INIT_TIMEOUT=true OCS_TEST_INIT_TIMEOUT_EXTRA_MS=6 cargo run --bin ocs > "logs/ocs_$(date +%Y%m%d_%H%M%S).log" 2>&1

3. Force recovery to exceed ~200 ms and trigger a Mission Abort
run:
OCS_TEST_FORCE_SLOW_RECOVERY=true OCS_TEST_SLOW_RECOVERY_DELAY_MS=250 cargo run --bin ocs > "logs/ocs_$(date +%Y%m%d_%H%M%S).log" 2>&1

UDP assumptions (hard-coded in the code):
- GCS downlink receive address: `127.0.0.1:9000`
- OCS uplink receive address: `127.0.0.1:9001`

Stop with `Ctrl+C`.

## Design Overview

```mermaid
flowchart LR
  subgraph OCS[OCS]
    Sensors[Sensor Tasks (Thermal/IMU/Power)]
    Receiver[receiver_loop (merge + Miss/Recovered emit)]
    Scheduling[EDF scheduling]
    Health[Health check]
    Safety[Safety rules]
    Benchmarking[Benchmarking (fault injection + recovery eval)]
    Downlink[Downlink UDP -> GCS]
    Uplink[Uplink UDP listener <- GCS]

    Sensors --> Receiver
    Receiver --> Scheduling
    Scheduling --> Downlink
    Receiver --> Safety
    Safety --> Health
    Safety --> Benchmarking
    Benchmarking --> Sensors
    Uplink -. recv/validate only .-> Scheduling
  end

  subgraph Link[Link]
    Downlink -->|telemetry payload| GCS[GCS]
    GCS -->|command frames| Uplink
  end
```

## Module Files (one-line description for each `src/ocs/*.rs`)
- `src/ocs/mod.rs`: OCS entry point that exposes the main modules (`time/types/sensors/scheduling/health/downlink/uplink/runtime/safety/benchmarking`).
- `src/ocs/time.rs`: Defines the runtime time origin and provides `now_ms()` returning elapsed milliseconds as `TimestampMs`.
- `src/ocs/types.rs`: Common types such as `SensorId` / `TimestampMs` / `SafetyEvent` / `RecoveryTimeMetric` / `BenchmarkMetrics` / `DownlinkPacket`.
- `src/ocs/sensors.rs`: Simulates Thermal/IMU/Power periodically and emits `SensorSample` and `SafetyEvent` (also supports Delayed fault injection via the fault watch).
- `src/ocs/runtime.rs`: Implements typestate-based startup/shutdown and spawns the full pipeline (sensors/receiver_loop/scheduling/downlink/uplink/safety/benchmarking).
- `src/ocs/scheduling.rs`: EDF task release/selection with worker lanes, start/completion/prepare deadline checks, visibility-window constraints, overload skip/degraded keepalive handling, and benchmark metric aggregation.
- `src/ocs/health.rs`: Health check that looks at the safety alert snapshot and logs `OK/NG`.
- `src/ocs/safety.rs`: Turns 3 consecutive `Miss` events into an alert, measures recovery time until `Recovered` as `RecoveryTimeMetric`, and triggers mission abort on threshold violation.
- `src/ocs/benchmarking.rs`: Every 60 seconds, logs a cumulative evaluation line, injects a Delayed fault, waits for alert -> clears the fault -> measures recovery, then logs `OK/NG`.
- `src/ocs/downlink.rs`: Frames `DownlinkPacket` and sends it over UDP to GCS (queue/degraded mode/circuit breaker handling).
- `src/ocs/uplink.rs`: Receives UDP command frames from GCS, validates sizes, and logs them (currently receive/validate-focused).

## How to read Benchmarking results (`src/ocs/benchmarking.rs`)
The following messages are mainly printed to `stderr`.

- `"[benchmarking] cycle=<n> eval thermal_sensor_jitter_max_ms=... thermal_sensor_fault_jitter_max_ms=... thermal_task_jitter_max_ms=... compression_task_jitter_max_ms=... health_task_jitter_max_ms=... antenna_task_jitter_max_ms=... drift_max_ms=... drift_avg_ms=... drift_late_starts=... deadline_violations=... start_delay_violations=... completion_delay_violations=... prepare_deadline_violations=... visibility_prepare_deadline_violations=... critical_jitter_violations=... cpu_util_avg=... e2e_latency_max_ms=... e2e_latency_avg_ms=... tx_queue_latency_max_ms=... tx_queue_latency_avg_ms=... peak_buffer_fill_rate_percent=... dropped_samples=... compression_overload_skips=... degraded_mode_enter_count=... missed_communication_count=..."`
  - Summary aggregated from current cumulative `BenchmarkMetrics` at cycle start (no per-cycle `reset()` is executed here).
- `"[benchmarking] cycle=<n> fault injected sensor_id=<id> kind=Delayed"`
  - Indicates that this cycle injected a Delayed fault (send suppression).
- `"[benchmarking] cycle=<n> fault cleared, waiting for recovery metric"`
  - After an alert is raised, the fault is cleared and the code waits for `Safety` to emit `RecoveryTimeMetric`.
- `"[benchmarking] cycle=<n> recovery sensor_id=<id> duration_ms=<ms> ... result=OK|NG"`
  - Determines the outcome based on recovery time (`duration_ms` from alert to recovery).
  - `OK` means `duration_ms <= RECOVERY_ABORT_THRESHOLD_MS=190ms` (Safety's internal abort threshold).
  - `NG` means `duration_ms > 190ms` (the spec is `RECOVERY_SPEC_MAX_MS=200ms` strict, so the internal threshold leaves margin for timing/rounding effects).

On shutdown, an execution-end snapshot is saved to `./logs/ocs_shutdown_benchmark_metrics_<timestamp>.txt`.
Main fields:
- `recovery_measured_last_ms / max_ms / avg_ms / count / recovery_measured_over_abort_threshold_count`
- `thermal_sensor_max_jitter_ms / thermal_sensor_fault_max_jitter_ms / thermal_task_jitter_max_ms / compression_task_jitter_max_ms / health_task_jitter_max_ms / antenna_task_jitter_max_ms`
- `drift_max_ms / drift_sum_ms / drift_count / drift_late_start_count / deadline_violations / start_delay_violations / completion_delay_violations / prepare_deadline_violations / visibility_prepare_deadline_violations / critical_jitter_violation_count`
- `cpu_util_sum_active_ms / cpu_util_sum_total_ms`
- `e2e_latency_max_ms / e2e_latency_avg_ms / tx_queue_latency_max_ms / tx_queue_latency_avg_ms`
- `peak_buffer_fill_rate_percent / total_dropped_samples / compression_overload_skip_count / degraded_mode_enter_count / missed_communication_count`

## How to read Safety results (`src/ocs/safety.rs`)
The following messages are mainly printed to `stderr`.

- `"[safety] alert sensor_id=<id> consecutive_miss=3 event_at=<t>"`
  - The moment the alert becomes active after the target sensor reaches 3 consecutive `Miss` events.
- `"[safety] recovered sensor_id=<id> event_at=<t>"`
  - The sensor recovers while the alert is active (normal sample re-received), so `RecoveryTimeMetric` is emitted.
- `"[safety] mission_abort reason=RecoveryTimeout ... abort_threshold_ms=190"`
  - Indicates mission abort (shutdown) because the time since alert start exceeded `190ms`.

Note:
- Safety measures recovery using `Instant` and rounds up elapsed milliseconds (so it won’t become `0ms` within the same millisecond), making `duration_ms` more representative of realistic timing delay.

