//! Command boundary representation using typestate (ZST).
//! In addition to runtime interlocks, this distinguishes in tests/APIs
//! which command wrappers can be constructed for each system state.
//!

use std::marker::PhantomData;

use crate::gcs::interlocks;
use crate::gcs::types::Command;

/// Marker that allows unsafe commands to be queued only when the system is Nominal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UnsafeAllowed;

/// Fault-active marker: no unsafe command form is constructible
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UnsafeBlocked;

/// Phase marker trait (ZST).
pub trait CommandPhase: Send + Sync + 'static {}
impl CommandPhase for UnsafeAllowed {}
impl CommandPhase for UnsafeBlocked {}

/// Command wrapper tagged with phase `P` (constructed only at boundaries).
pub struct ScopedCommand<P: CommandPhase> {
    pub inner: Command,
    _marker: PhantomData<P>,
}

impl ScopedCommand<UnsafeAllowed> {
    /// Wrap any command under the Nominal assumption
    #[inline]
    pub fn new_allowed(cmd: Command) -> Self {
        Self {
            inner: cmd,
            _marker: PhantomData,
        }
    }
}

impl ScopedCommand<UnsafeBlocked> {
    /// During faults, only safe commands are constructible.
    /// Returns `Err` for unsafe commands.
    pub fn new_when_faulted(cmd: Command) -> Result<Self, ()> {
        if interlocks::is_unsafe_command(&cmd) {
            Err(())
        } else {
            Ok(Self {
                inner: cmd,
                _marker: PhantomData,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gcs::types::{CommandPriority, MAX_PAYLOAD_LEN};

    #[test]
    fn faulted_rejects_unsafe() {
        let cmd = Command {
            id: 1,
            name: "execute",
            payload: [0u8; MAX_PAYLOAD_LEN],
            payload_len: 0,
            issued_at: crate::gcs::types::TimestampMs(0),
            priority: CommandPriority::Normal,
        };
        assert!(ScopedCommand::<UnsafeBlocked>::new_when_faulted(cmd).is_err());
    }

    #[test]
    fn faulted_accepts_safe() {
        let cmd = Command {
            id: 1,
            name: "status",
            payload: [0u8; MAX_PAYLOAD_LEN],
            payload_len: 0,
            issued_at: crate::gcs::types::TimestampMs(0),
            priority: CommandPriority::Normal,
        };
        assert!(ScopedCommand::<UnsafeBlocked>::new_when_faulted(cmd).is_ok());
    }
}
