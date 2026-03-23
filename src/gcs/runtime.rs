//! GCS runtime: spawn telemetry, scheduler, uplink, logger, fault; wire channels.
//!
//! Ctrl+C (`tokio::signal::ctrl_c`) calls `ShutdownHandle::try_shutdown` so the same graceful path as
//! telemetry loss runs; `Supervisor::run` is awaited to completion afterward.
//!
//! ## T11 acceptance checklist
//! - Real-time command scheduling is maintained by a single `BinaryHeap` (custom `Ord`: Urgent-first + Normal EDF) and the scheduler loop.
//! - Urgent dispatch must be within <=2 ms: `deadline = enqueued_at + 2ms`, measured at `actual_dispatch_at`.
//! - Interlocks run synchronously with no `.await`: `check_command` is applied, rejected commands are not sent, reasons are logged, and processing continues.
//! - Deadline adherence / rejection reasons are recorded via `log_scheduler_event` as `UplinkSendOk` / `UplinkSendFailed` / `InterlockRejected`.
//! - The RT path uses `SchedulerQueues` (single `BinaryHeap` + `SchedHeapEntry`) and `SchedulerLogEvent`.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use crate::gcs::fault;
use crate::gcs::logging::{log_event, log_scheduler_event, LogEvent};
use crate::gcs::monitoring;
use crate::gcs::system_load;
use crate::gcs::scheduler;
use crate::gcs::telemetry;
use crate::gcs::test_commands;
use crate::gcs::time;
use crate::gcs::fault::FaultStateSnapshot;
use crate::gcs::types::{
    DispatchOutcome, FaultState, RetransmitRequest, SchedulerLogEvent, ScheduledCommand,
};
use crate::gcs::uplink;
use crate::supervisor::{save_text_snapshot, ShutdownHandle, Supervisor};

const DOWNLINK_BIND_ADDR: &str = "127.0.0.1:9000";
const OCS_UPLINK_ADDR: &str = "127.0.0.1:9001";
const FAULT_QUEUE_CAP: usize = 32;
const SCHEDULER_LOG_CAP: usize = 32;
/// Bounded queue: telemetry → uplink NACK actor (backpressure).
const NACK_QUEUE_CAP: usize = 16;
/// When true, spawns test command generator for integration (interlock / uplink / recovery).
const ENABLE_TEST_COMMAND_GENERATOR: bool = true;
/// Interval (ms) for periodic monitoring snapshot to stderr.
const MONITORING_SNAPSHOT_INTERVAL_MS: u64 = 10_000;

/// GCS entrypoint (Supervisor-managed).
pub async fn run() {
    time::init_start_time();
    log_event(LogEvent::RuntimeStarted {
        mode: "telemetry_and_fault",
    });

    let (mut sup, shutdown_handle) = Supervisor::new("gcs");
    let shutdown_ctrl_c: ShutdownHandle = shutdown_handle.clone();

    let on_shutdown_save = || {
        let snap = monitoring::snapshot_metrics();
        save_text_snapshot("gcs_shutdown_snapshot_monitoring", &snap);
    };

    let build_pipeline = move |sup: &mut Supervisor| {
        let (fault_tx, fault_rx) = mpsc::channel(FAULT_QUEUE_CAP);
        let (fault_state_tx, fault_state_rx) =
            watch::channel::<FaultStateSnapshot>((FaultState::Nominal, None));
        let fault_state_rx_telemetry = fault_state_rx.clone();

        sup.spawn(fault::run_fault(fault_rx, fault_state_tx));
        sup.spawn(system_load::run_system_load_sampler());

        let queues = Arc::new(std::sync::Mutex::new(scheduler::SchedulerQueues::new()));
        let wake = Arc::new(tokio::sync::Notify::new());
        let (command_tx, command_rx) = mpsc::channel::<ScheduledCommand>(1);
        let (result_tx, result_rx) = mpsc::channel::<DispatchOutcome>(1);
        let (log_tx, mut log_rx) = mpsc::channel::<SchedulerLogEvent>(SCHEDULER_LOG_CAP);
        let (nack_tx, nack_rx) = mpsc::channel::<RetransmitRequest>(NACK_QUEUE_CAP);

        sup.spawn(telemetry::run_telemetry(
            DOWNLINK_BIND_ADDR,
            shutdown_handle.sender(),
            fault_tx.clone(),
            fault_state_rx_telemetry,
            nack_tx,
        ));

        if ENABLE_TEST_COMMAND_GENERATOR {
            sup.spawn(test_commands::run_test_command_generator(
                fault_tx,
                Arc::clone(&queues),
                Arc::clone(&wake),
            ));
        }

        // Uplink + Scheduler
        sup.spawn(uplink::run_uplink_task(
            OCS_UPLINK_ADDR,
            command_rx,
            nack_rx,
            result_tx,
        ));
        sup.spawn(scheduler::run_scheduler_loop(
            Arc::clone(&queues),
            fault_state_rx,
            command_tx,
            result_rx,
            log_tx,
            wake,
        ));

        // Drain scheduler log channel.
        sup.spawn(async move {
            while let Some(ev) = log_rx.recv().await {
                log_scheduler_event(ev);
            }
        });

        // Periodic monitoring snapshot.
        sup.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(
                MONITORING_SNAPSHOT_INTERVAL_MS,
            ));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let snap = monitoring::snapshot_metrics();
                eprintln!("{}", snap);
            }
        });
    };

    let supervisor_run = async {
        sup.run(build_pipeline, on_shutdown_save).await;
    };
    tokio::pin!(supervisor_run);

    tokio::select! {
        res = tokio::signal::ctrl_c() => {
            let _ = res;
            shutdown_ctrl_c.try_shutdown();
        }
        _ = &mut supervisor_run => {}
    }
    supervisor_run.await;
}
