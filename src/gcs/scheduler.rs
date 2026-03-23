//! GCS scheduler: real-time command schedule, urgent 2ms deadline, interlocks + uplink, scheduling drift to monitoring.
//!
//! # Phase0: fixed definitions for metrics, queues, and interlocks
//!
//! - **dispatch**: `actual_dispatch_at` = uplink send before UDP `send` start time.
//! - **drift**: `scheduling_drift_ms = actual_dispatch_at - execute_at`（execute_at = 送信予定時刻）。
//! - **deadline miss**: `missed = (actual_dispatch_at > deadline)`。
//! - **2 ms rule for Urgent (enqueue-based)**: `deadline = enqueued_at + 2ms`. Dispatch miss uses the same baseline: miss if `actual_dispatch_at > enqueued_at + 2ms`.
//!
//! ## Interlocks
//! - **Synchronous, no `.await`**: interlocks are pure checks without async I/O to keep decision time stable. This avoids lock-wait and I/O latency on the RT path.
//!
//! - **If any Urgent is ready, process immediately**. Otherwise, wait until the next Normal `execute_at` via `sleep_until(next_execute_at)`.
//! - **Wake the scheduler on new enqueue** (especially Urgent) via channel send or `Notify`. This reduces unnecessary polling and makes Normal drift behavior easier to explain.
//!
//! ## Queue policy
//! - **Single `BinaryHeap`** (custom `Ord`): Urgent is always ahead of Normal; Urgent is FIFO by `urgent_seq`; Normal uses EDF by `deadline` then `command_id`.
//! - **When full**: reject at `SCHEDULER_QUEUE_CAP`; rejections are tagged as `queue=urgent` / `queue=normal` in logs.
//! - **Memory**: capacity is reserved at startup to avoid reallocations during steady-state pushes.
//! - **Typestate-gated enqueue**: [`SchedulerQueues::try_enqueue`] uses [`crate::gcs::command_state::ScopedCommand`] with current [`FaultState`] to block unsafe commands. Dispatch-time interlock checks are still required to handle post-enqueue state changes.
//!
//! - `command_id`, `priority`, `execute_at`, `deadline`, `enqueued_at`, `actual_dispatch_at`, `drift_ms`, `missed`, `reject_reason?`, `queue_len`, `queue=urgent|normal`（reject 時）。

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch, Notify};
use tokio::time::sleep;

use crate::gcs::command_state::{ScopedCommand, UnsafeAllowed, UnsafeBlocked};
use crate::gcs::fault::FaultStateSnapshot;
use crate::gcs::interlocks;
use crate::gcs::monitoring;
use crate::gcs::time;
use crate::gcs::types::{
    CommandPriority, DispatchOutcome, DispatchResult, FaultState, RejectionReason,
    ScheduledCommand, SchedulerLogEvent, TimestampMs,
};

/// Urgent queue cap (roughly matches the old two-queue urgent side).
pub const URGENT_QUEUE_CAP: usize = 8;
/// Normal queue cap.
pub const NORMAL_QUEUE_CAP: usize = 32;
/// Total cap for the single heap.
pub const SCHEDULER_QUEUE_CAP: usize = URGENT_QUEUE_CAP + NORMAL_QUEUE_CAP;

/// One scheduler entry (`BinaryHeap` is a max-heap; `Ord` marks earlier-to-run as greater).
#[derive(Clone, Copy, Debug)]
pub struct SchedHeapEntry {
    /// FIFO order among Urgent entries (smaller value runs first).
    urgent_seq: u64,
    pub cmd: ScheduledCommand,
}

impl PartialEq for SchedHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmd.command.id == other.cmd.command.id && self.cmd.execute_at == other.cmd.execute_at
    }
}

impl Eq for SchedHeapEntry {}

impl Ord for SchedHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_u = self.cmd.command.priority == CommandPriority::Urgent;
        let other_u = other.cmd.command.priority == CommandPriority::Urgent;
        match (self_u, other_u) {
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (true, true) => self
                .urgent_seq
                .cmp(&other.urgent_seq)
                .reverse()
                .then_with(|| {
                    self.cmd
                        .command
                        .id
                        .cmp(&other.cmd.command.id)
                        .reverse()
                }),
            (false, false) => self
                .cmd
                .deadline
                .0
                .cmp(&other.cmd.deadline.0)
                .reverse()
                .then_with(|| {
                    self.cmd
                        .command
                        .id
                        .cmp(&other.cmd.command.id)
                        .reverse()
                }),
        }
    }
}

impl PartialOrd for SchedHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Single priority heap (Week 11: custom `Ord` + `BinaryHeap`).
pub struct SchedulerQueues {
    heap: BinaryHeap<SchedHeapEntry>,
    next_urgent_seq: u64,
}

impl Default for SchedulerQueues {
    fn default() -> Self {
        Self {
            heap: BinaryHeap::with_capacity(SCHEDULER_QUEUE_CAP),
            next_urgent_seq: 0,
        }
    }
}

impl SchedulerQueues {
    pub fn new() -> Self {
        Self::default()
    }

    /// Next `execute_at` (minimum across priorities), used for `sleep_until`. O(N).
    pub fn min_execute_at(&self) -> Option<TimestampMs> {
        self.heap.iter().map(|e| e.cmd.execute_at).min()
    }

    /// Among commands with `execute_at <= now`, returns the one that should run first by priority rules.
    pub fn pop_next(&mut self, now: TimestampMs) -> Option<ScheduledCommand> {
        if self.heap.is_empty() {
            return None;
        }
        let drained: Vec<SchedHeapEntry> = std::iter::from_fn(|| self.heap.pop()).collect();
        let mut best_idx: Option<usize> = None;
        for (i, e) in drained.iter().enumerate() {
            if e.cmd.execute_at <= now {
                let better = match best_idx {
                    None => true,
                    Some(j) => e.cmp(&drained[j]) == Ordering::Greater,
                };
                if better {
                    best_idx = Some(i);
                }
            }
        }
        let idx = best_idx?;
        let mut rest = drained;
        let chosen = rest.swap_remove(idx).cmd;
        for entry in rest {
            self.heap.push(entry);
        }
        Some(chosen)
    }

    /// Enqueue one scheduled command (test/simple Nominal path). Full queue -> `QueueFull*`.
    #[inline]
    pub fn enqueue(
        &mut self,
        scheduled: ScheduledCommand,
        wake: Option<&Notify>,
    ) -> Result<(), RejectionReason> {
        self.enqueue_inner(scheduled, wake)
    }

    /// Apply typestate boundary using current [`FaultState`] before enqueue.
    /// In non-Nominal states, only commands allowed by [`UnsafeBlocked`] can be inserted.
    #[inline]
    pub fn try_enqueue(
        &mut self,
        scheduled: ScheduledCommand,
        fault_state: FaultState,
        wake: Option<&Notify>,
    ) -> Result<(), RejectionReason> {
        match fault_state {
            FaultState::Nominal => {
                let _ = ScopedCommand::<UnsafeAllowed>::new_allowed(scheduled.command);
            }
            FaultState::Degraded | FaultState::Critical | FaultState::Recovering => {
                ScopedCommand::<UnsafeBlocked>::new_when_faulted(scheduled.command).map_err(
                    |_| RejectionReason::FaultActive(fault_state),
                )?;
            }
        }
        self.enqueue_inner(scheduled, wake)
    }

    fn enqueue_inner(
        &mut self,
        scheduled: ScheduledCommand,
        wake: Option<&Notify>,
    ) -> Result<(), RejectionReason> {
        let is_urgent = scheduled.command.priority == CommandPriority::Urgent;
        if self.heap.len() >= SCHEDULER_QUEUE_CAP {
            return Err(if is_urgent {
                RejectionReason::QueueFullUrgent
            } else {
                RejectionReason::QueueFullNormal
            });
        }
        let urgent_seq = if is_urgent {
            let s = self.next_urgent_seq;
            self.next_urgent_seq = self.next_urgent_seq.saturating_add(1);
            s
        } else {
            0
        };
        self.heap.push(SchedHeapEntry {
            urgent_seq,
            cmd: scheduled,
        });
        if let Some(w) = wake {
            w.notify_one();
        }
        Ok(())
    }

    pub fn total_len(&self) -> usize {
        self.heap.len()
    }
}

/// Runs the scheduler loop: pop_next (Urgent-first + EDF) -> interlocks -> uplink.
pub async fn run_scheduler_loop(
    queues: Arc<std::sync::Mutex<SchedulerQueues>>,
    fault_state_rx: watch::Receiver<FaultStateSnapshot>,
    command_tx: mpsc::Sender<ScheduledCommand>,
    mut result_rx: mpsc::Receiver<DispatchOutcome>,
    log_tx: mpsc::Sender<SchedulerLogEvent>,
    wake: Arc<Notify>,
) {
    loop {
        let scheduled = {
            let mut q = match queues.lock() {
                Ok(g) => g,
                Err(_) => break,
            };
            let now = time::now_ms();
            let min_at = q.min_execute_at();
            if min_at.map_or(false, |t| now.0 >= t.0) {
                q.pop_next(now)
            } else {
                None
            }
        };

        if let Some(scheduled) = scheduled {
            let (fault_state, _) = *fault_state_rx.borrow();
            match interlocks::check_command_with_log(&scheduled.command, fault_state) {
                Err(reason) => {
                    let _ = log_tx
                        .send(SchedulerLogEvent::InterlockRejected {
                            command_id: scheduled.command.id,
                            name: scheduled.command.name,
                            state: fault_state,
                            reason,
                        })
                        .await;
                    continue;
                }
                Ok(()) => {}
            }
            if command_tx.send(scheduled).await.is_err() {
                break;
            }
            let Some(outcome) = result_rx.recv().await else {
                break;
            };
            let actual_ms = outcome.actual_dispatch_at.0;
            let drift_ms = actual_ms as i64 - outcome.execute_at_ms as i64;
            let missed = actual_ms > outcome.deadline_ms;
            monitoring::notify_uplink_dispatch(outcome.command_id, outcome.execute_at_ms, actual_ms);
            monitoring::notify_scheduling_drift(outcome.execute_at_ms, actual_ms, drift_ms);
            if missed {
                monitoring::notify_deadline_miss(outcome.command_id, outcome.deadline_ms, actual_ms);
            }
            let ev = match outcome.result {
                DispatchResult::Sent => SchedulerLogEvent::UplinkOk {
                    command_id: outcome.command_id,
                    execute_at: outcome.execute_at_ms,
                    deadline: outcome.deadline_ms,
                    actual_dispatch_at: outcome.actual_dispatch_at.0,
                    drift_ms,
                    missed,
                },
                DispatchResult::Failed { detail } => SchedulerLogEvent::UplinkFailed {
                    command_id: outcome.command_id,
                    detail,
                },
                DispatchResult::Rejected { .. } => continue,
            };
            let _ = log_tx.send(ev).await;
            continue;
        }

        let (next_at, now) = {
            let q = match queues.lock() {
                Ok(g) => g,
                Err(_) => break,
            };
            let now = time::now_ms();
            (q.min_execute_at(), now)
        };

        match next_at {
            None => {
                wake.notified().await;
            }
            Some(t) => {
                let delay_ms = t.0.saturating_sub(now.0);
                let deadline = sleep(Duration::from_millis(delay_ms));
                tokio::select! {
                    _ = wake.notified() => {}
                    _ = deadline => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gcs::types::{Command, CommandPriority, MAX_PAYLOAD_LEN};

    fn scheduled(
        id: u64,
        priority: CommandPriority,
        execute_at: u64,
        deadline: u64,
    ) -> ScheduledCommand {
        ScheduledCommand {
            command: Command {
                id,
                name: "t",
                payload: [0u8; MAX_PAYLOAD_LEN],
                payload_len: 0,
                issued_at: TimestampMs(0),
                priority,
            },
            enqueued_at: TimestampMs(0),
            execute_at: TimestampMs(execute_at),
            deadline: TimestampMs(deadline),
        }
    }

    #[test]
    fn pop_next_prefers_earliest_deadline_among_normal_when_both_ready() {
        let mut q = SchedulerQueues::new();
        q.enqueue(scheduled(1, CommandPriority::Normal, 0, 100), None)
            .unwrap();
        q.enqueue(scheduled(2, CommandPriority::Normal, 0, 50), None)
            .unwrap();
        let now = TimestampMs(0);
        let first = q.pop_next(now).unwrap();
        assert_eq!(first.command.id, 2);
        let second = q.pop_next(now).unwrap();
        assert_eq!(second.command.id, 1);
    }

    #[test]
    fn pop_next_skips_not_yet_executable() {
        let mut q = SchedulerQueues::new();
        q.enqueue(scheduled(1, CommandPriority::Normal, 100, 10), None)
            .unwrap();
        q.enqueue(scheduled(2, CommandPriority::Normal, 0, 500), None)
            .unwrap();
        let now = TimestampMs(0);
        let only = q.pop_next(now).unwrap();
        assert_eq!(only.command.id, 2);
        assert!(q.pop_next(now).is_none());
    }

    #[test]
    fn urgent_fifo_ordering() {
        let mut q = SchedulerQueues::new();
        q.enqueue(scheduled(10, CommandPriority::Urgent, 0, 2), None)
            .unwrap();
        q.enqueue(scheduled(11, CommandPriority::Urgent, 0, 2), None)
            .unwrap();
        let now = TimestampMs(0);
        assert_eq!(q.pop_next(now).unwrap().command.id, 10);
        assert_eq!(q.pop_next(now).unwrap().command.id, 11);
    }

    #[test]
    fn urgent_before_normal_when_both_ready() {
        let mut q = SchedulerQueues::new();
        q.enqueue(scheduled(1, CommandPriority::Normal, 0, 10), None)
            .unwrap();
        q.enqueue(scheduled(2, CommandPriority::Urgent, 0, 100), None)
            .unwrap();
        let now = TimestampMs(0);
        assert_eq!(q.pop_next(now).unwrap().command.id, 2);
        assert_eq!(q.pop_next(now).unwrap().command.id, 1);
    }
}
