pub mod gcs;
pub mod ocs;

/// OCS stderr: uniform `ts_ms=` prefix (aligned with GCS `monitoring::*` lines).
#[macro_export]
macro_rules! ocs_ts_eprintln {
    ($fmt:literal $(, $arg:expr)*) => {
        eprintln!(
            concat!("ts_ms={} ", $fmt),
            $crate::ocs::time::now_ms().0
            $(, $arg)*
        )
    };
}
pub mod reliability;
pub mod sensor_values;
pub mod wire;
pub mod supervisor;
