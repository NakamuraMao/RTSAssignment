//! GCS fault: receive simulated fault messages, FaultState, 100ms critical ground alert, fault recovery time to monitoring.

use std::time::Duration;

use tokio::sync::{mpsc, watch};

use crate::gcs::logging::{log_event, LogEvent};
use crate::gcs::monitoring;
use crate::gcs::time;
use crate::gcs::types::{
    FaultMessage, FaultState, InterlockLatencyMetric, RecoveryMetric, TimestampMs,
};

const RECOVERY_ALERT_THRESHOLD_MS: u64 = 100;
const CHECK_INTERVAL_MS: u64 = 10;

/// `(FaultState, fault_detected_at)` — second field is for recovery duration and 100 ms ground alert.
pub type FaultStateSnapshot = (FaultState, Option<TimestampMs>);

/// Run fault task: receive FaultMessages, update state, broadcast via watch.
/// Tracks fault_detected_at, fires 100ms critical alert, reports recovery duration.
pub async fn run_fault(
    mut fault_rx: mpsc::Receiver<FaultMessage>,
    fault_state_tx: watch::Sender<FaultStateSnapshot>,
) {
    let mut state = FaultState::Nominal;
    let mut fault_detected_at: Option<TimestampMs> = None;
    let mut critical_alert_fired = false;

    let mut check_interval = tokio::time::interval(Duration::from_millis(CHECK_INTERVAL_MS));
    check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            msg = fault_rx.recv() => {
                let msg = match msg {
                    Some(m) => m,
                    None => break,
                };
                // Interlock latency: stimulus = instant this task receives the fault message on GCS.
                let recv_at = time::now_ms();
                let at_ms = msg.at.0;
                log_event(LogEvent::FaultReceived {
                    at_ms,
                    state: msg.state,
                    source: msg.source.clone(),
                    detail: msg.detail.clone(),
                });

                let prev = state;
                state = msg.state;

                // Set fault_detected_at only when first leaving Nominal (to Degraded/Critical).
                if matches!(prev, FaultState::Nominal | FaultState::Recovering)
                    && matches!(state, FaultState::Degraded | FaultState::Critical)
                {
                    fault_detected_at = Some(msg.at);
                    critical_alert_fired = false;
                }

                // Recovery complete: back to Nominal.
                if state == FaultState::Nominal {
                    if let Some(detected) = fault_detected_at {
                        let recovered_at = time::now_ms();
                        let duration_ms = recovered_at.0.saturating_sub(detected.0);
                        log_event(LogEvent::FaultRecovered { duration_ms });
                        monitoring::notify_recovery(RecoveryMetric {
                            fault_at: detected,
                            recovered_at,
                            duration_ms,
                        });
                    }
                    fault_detected_at = None;
                }

                if prev != state {
                    log_event(LogEvent::FaultStateChanged {
                        from: prev,
                        to: state,
                    });
                }

                let snapshot: FaultStateSnapshot = (state, fault_detected_at);

                // Response = interlock armed (unsafe commands rejected): publish before watch send.
                // Does not include time until an unsafe command arrives.
                if matches!(prev, FaultState::Nominal | FaultState::Recovering)
                    && matches!(state, FaultState::Degraded | FaultState::Critical)
                {
                    let locked_at = time::now_ms();
                    let latency_ms = locked_at.0.saturating_sub(recv_at.0);
                    monitoring::notify_interlock_latency(InterlockLatencyMetric {
                        at: locked_at,
                        latency_ms,
                    });
                }

                if fault_state_tx.send(snapshot).is_err() {
                    break;
                }
            }
            _ = check_interval.tick() => {
                let now = time::now_ms().0;
                if let Some(detected) = fault_detected_at {
                    let elapsed = now.saturating_sub(detected.0);
                    if elapsed > RECOVERY_ALERT_THRESHOLD_MS && state != FaultState::Nominal && !critical_alert_fired {
                        critical_alert_fired = true;
                        log_event(LogEvent::CriticalAlertRecoveryTimeout {
                            fault_detected_at_ms: detected.0,
                            elapsed_ms: elapsed,
                        });
                        monitoring::notify_critical_alert_recovery_timeout(detected.0, elapsed);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{run_fault, FaultStateSnapshot};
    use crate::gcs::time;
    use crate::gcs::types::{FaultMessage, FaultState, TimestampMs};
    use tokio::sync::{mpsc, watch};

    fn snapshot_state(s: &FaultStateSnapshot) -> FaultState {
        s.0
    }

    #[tokio::test]
    async fn fault_state_transitions_nominal_to_degraded_to_recovering_to_nominal() {
        time::init_start_time();
        let (fault_tx, fault_rx) = mpsc::channel(4);
        let (state_tx, mut state_rx) = watch::channel((FaultState::Nominal, None));

        let task = tokio::spawn(run_fault(fault_rx, state_tx));

        assert_eq!(snapshot_state(&*state_rx.borrow()), FaultState::Nominal);

        fault_tx
            .send(FaultMessage {
                at: TimestampMs(100),
                state: FaultState::Degraded,
                source: "test".into(),
                detail: "degraded".into(),
            })
            .await
            .unwrap();
        state_rx.changed().await.unwrap();
        assert_eq!(snapshot_state(&*state_rx.borrow()), FaultState::Degraded);

        fault_tx
            .send(FaultMessage {
                at: TimestampMs(200),
                state: FaultState::Recovering,
                source: "test".into(),
                detail: "recovering".into(),
            })
            .await
            .unwrap();
        state_rx.changed().await.unwrap();
        assert_eq!(snapshot_state(&*state_rx.borrow()), FaultState::Recovering);

        fault_tx
            .send(FaultMessage {
                at: TimestampMs(300),
                state: FaultState::Nominal,
                source: "test".into(),
                detail: "ok".into(),
            })
            .await
            .unwrap();
        state_rx.changed().await.unwrap();
        assert_eq!(snapshot_state(&*state_rx.borrow()), FaultState::Nominal);

        drop(fault_tx);
        task.await.unwrap();
    }
}
