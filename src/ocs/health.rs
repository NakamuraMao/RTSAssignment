//! OCS: health monitoring task logic and reporting.
//! ① センサアラート監視（safety の [bool;3]、1つでも true なら NG）
//! ③ 正常ログ出力（OK/NG ともに alerts={:?} でどのセンサが alert か分かる）

use super::time;

/// ヘルスチェック。scheduling の Health タスクから呼ばれる。
/// センサアラート状態を監視し、1つでも true なら NG。結果と alerts をログ出力する。
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
