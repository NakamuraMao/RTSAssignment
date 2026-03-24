//! OCS: health monitoring task logic and reporting.
//! ① sensor alert monitoring (safety's [bool;3], NG if any true)
//! ③ normal log output (OK/NG both show which sensor is alerted via alerts={:?})

use super::time;

/// Health check. called from Health task in scheduling.
/// Monitor sensor alert state, NG if any true. Log result and alerts.
#[must_use]
pub fn check(alert_snapshot: [bool; 3]) -> bool {
    let at = time::now_ms();
    let any_alert = alert_snapshot.iter().any(|&b| b);
    if any_alert {
        crate::ocs_ts_eprintln!(
            "[health] ng event_at={} reason=sensor_alert alerts={:?}",
            at.0,
            alert_snapshot
        );
        false
    } else {
        crate::ocs_ts_eprintln!(
            "[health] ok event_at={} alerts={:?}",
            at.0,
            alert_snapshot
        );
        true
    }
}
