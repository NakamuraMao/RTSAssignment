//! GCS interlocks: validate command against FaultState, RejectionReason.

use crate::gcs::logging::{log_event, LogEvent};
use crate::gcs::types::{Command, FaultState, RejectionReason};

/// Unsafe command names (fault中に送ると危険): mode change, execute/trigger, reset/restart.
const UNSAFE_NAMES: &[&str] = &[
    "mode_change",
    "set_mode",
    "execute",
    "trigger",
    "fire",
    "reset",
    "restart",
    "reboot",
];

/// True if the command is unsafe (must be blocked when not Nominal).
pub fn is_unsafe_command(cmd: &Command) -> bool {
    UNSAFE_NAMES.contains(&cmd.name)
}

/// Check command against current fault state. Nominal: allow all. Otherwise: block unsafe only.
pub fn check_command(cmd: &Command, state: FaultState) -> Result<(), RejectionReason> {
    if state == FaultState::Nominal {
        return Ok(());
    }
    if is_unsafe_command(cmd) {
        return Err(RejectionReason::FaultActive(state));
    }
    Ok(())
}

/// Check command and on reject: log `InterlockRejected`. Interlock latency is recorded in [`crate::gcs::fault::run_fault`].
pub fn check_command_with_log(cmd: &Command, state: FaultState) -> Result<(), RejectionReason> {
    match check_command(cmd, state) {
        Ok(()) => Ok(()),
        Err(reason) => {
            log_event(LogEvent::InterlockRejected {
                command_id: cmd.id,
                name: cmd.name,
                state,
                reason,
            });
            Err(reason)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{check_command, is_unsafe_command};
    use crate::gcs::types::{Command, CommandPriority, FaultState, RejectionReason, TimestampMs, MAX_PAYLOAD_LEN};

    fn cmd(name: &'static str) -> Command {
        Command {
            id: 1,
            name,
            payload: [0; MAX_PAYLOAD_LEN],
            payload_len: 0,
            issued_at: TimestampMs(0),
            priority: CommandPriority::Normal,
        }
    }

    #[test]
    fn nominal_allows_unsafe() {
        assert!(check_command(&cmd("execute"), FaultState::Nominal).is_ok());
        assert!(check_command(&cmd("reset"), FaultState::Nominal).is_ok());
    }

    #[test]
    fn degraded_blocks_unsafe() {
        assert_eq!(
            check_command(&cmd("execute"), FaultState::Degraded),
            Err(RejectionReason::FaultActive(FaultState::Degraded))
        );
        assert_eq!(
            check_command(&cmd("mode_change"), FaultState::Degraded),
            Err(RejectionReason::FaultActive(FaultState::Degraded))
        );
    }

    #[test]
    fn critical_and_recovering_block_unsafe() {
        assert!(check_command(&cmd("fire"), FaultState::Critical).is_err());
        assert!(check_command(&cmd("restart"), FaultState::Recovering).is_err());
    }

    #[test]
    fn non_nominal_allows_safe() {
        assert!(check_command(&cmd("status"), FaultState::Degraded).is_ok());
        assert!(check_command(&cmd("ping"), FaultState::Critical).is_ok());
    }

    #[test]
    fn is_unsafe_names() {
        assert!(is_unsafe_command(&cmd("mode_change")));
        assert!(is_unsafe_command(&cmd("set_mode")));
        assert!(is_unsafe_command(&cmd("execute")));
        assert!(is_unsafe_command(&cmd("trigger")));
        assert!(is_unsafe_command(&cmd("fire")));
        assert!(is_unsafe_command(&cmd("reset")));
        assert!(is_unsafe_command(&cmd("restart")));
        assert!(is_unsafe_command(&cmd("reboot")));
        assert!(!is_unsafe_command(&cmd("status")));
        assert!(!is_unsafe_command(&cmd("query")));
    }
}
