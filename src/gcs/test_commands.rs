//! GCS test command generator: for runtime integration tests.
//! Sends FaultMessages and enqueues ScheduledCommands (mode_change, execute, reset, status)
//! to verify interlock, uplink, and recovery time.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Notify};
use tokio::time::sleep;

use crate::gcs::logging::{log_event, LogEvent};
use crate::gcs::scheduler::SchedulerQueues;
use crate::gcs::time;
use crate::gcs::types::{
    Command, CommandPriority, FaultMessage, FaultState, ScheduledCommand, TimestampMs,
    MAX_PAYLOAD_LEN,
};

const TEST_DELAY_INIT_MS: u64 = 500;
const TEST_INTERVAL_MS: u64 = 2_000;
const TEST_URGENT_DEADLINE_MS: u64 = 2;

/// Runs the test command generator: after a short delay, periodically sends
/// FaultMessages (Degraded → Nominal) and enqueues commands (execute, reset, status)
/// so that interlock rejection, uplink send, and recovery can be verified.
pub async fn run_test_command_generator(
    fault_tx: mpsc::Sender<FaultMessage>,
    queues: Arc<std::sync::Mutex<SchedulerQueues>>,
    wake: Arc<Notify>,
) {
    sleep(Duration::from_millis(TEST_DELAY_INIT_MS)).await;

    let mut cmd_id: u64 = 1;

    // 1) Inject Degraded so that next unsafe command is rejected by interlock
    let _ = fault_tx
        .send(FaultMessage {
            at: time::now_ms(),
            state: FaultState::Degraded,
            source: String::from("test_commands"),
            detail: String::from("degraded"),
        })
        .await;
    sleep(Duration::from_millis(TEST_INTERVAL_MS)).await;

    // 2) Enqueue "execute" (unsafe) while Degraded → try_enqueue rejects at typestate boundary
    let now = time::now_ms();
    let exec_id = cmd_id;
    let scheduled = ScheduledCommand {
        command: Command {
            id: exec_id,
            name: "execute",
            payload: [0u8; MAX_PAYLOAD_LEN],
            payload_len: 0,
            issued_at: now,
            priority: CommandPriority::Normal,
        },
        enqueued_at: now,
        execute_at: now,
        deadline: TimestampMs(now.0.saturating_add(10)),
    };
    cmd_id = cmd_id.saturating_add(1);
    if let Ok(mut q) = queues.lock() {
        match q.try_enqueue(scheduled, FaultState::Degraded, Some(&*wake)) {
            Ok(()) => {}
            Err(reason) => {
                log_event(LogEvent::InterlockRejected {
                    command_id: exec_id,
                    name: "execute",
                    state: FaultState::Degraded,
                    reason,
                });
            }
        }
    }
    sleep(Duration::from_millis(TEST_INTERVAL_MS)).await;

    // 3) Restore Nominal so that next command can be sent via uplink
    let _ = fault_tx
        .send(FaultMessage {
            at: time::now_ms(),
            state: FaultState::Nominal,
            source: String::from("test_commands"),
            detail: String::from("nominal"),
        })
        .await;
    sleep(Duration::from_millis(TEST_INTERVAL_MS)).await;

    // 4) Enqueue "execute" again → should be sent (UplinkOk)
    let now = time::now_ms();
    let scheduled = ScheduledCommand {
        command: Command {
            id: cmd_id,
            name: "execute",
            payload: [0u8; MAX_PAYLOAD_LEN],
            payload_len: 0,
            issued_at: now,
            priority: CommandPriority::Urgent,
        },
        enqueued_at: now,
        execute_at: now,
        deadline: TimestampMs(now.0.saturating_add(TEST_URGENT_DEADLINE_MS)),
    };
    cmd_id = cmd_id.saturating_add(1);
    if let Ok(mut q) = queues.lock() {
        let _ = q.try_enqueue(scheduled, FaultState::Nominal, Some(&*wake));
    }
    sleep(Duration::from_millis(TEST_INTERVAL_MS)).await;

    // 5) Safe command "status" (always allowed)
    let now = time::now_ms();
    let scheduled = ScheduledCommand {
        command: Command {
            id: cmd_id,
            name: "status",
            payload: [0u8; MAX_PAYLOAD_LEN],
            payload_len: 0,
            issued_at: now,
            priority: CommandPriority::Normal,
        },
        enqueued_at: now,
        execute_at: now,
        deadline: TimestampMs(now.0.saturating_add(10)),
    };
    if let Ok(mut q) = queues.lock() {
        let _ = q.try_enqueue(scheduled, FaultState::Nominal, Some(&*wake));
    }
}
