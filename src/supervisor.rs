//! Supervisor: spawn orchestration, panic-only self-healing, graceful shutdown.
//!
//! Requirements (Phase 1c):
//! - Self-healing: only restart when a worker task panics.
//! - Graceful shutdown: on shutdown request, save a snapshot then stop all workers.

use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(debug_assertions)]
use std::io::Write;

use tokio::sync::mpsc;
use tokio::task::JoinError;
use tokio::task::JoinSet;

/// Handle given to other components to request graceful shutdown.
#[derive(Clone)]
pub struct ShutdownHandle {
    tx: mpsc::Sender<()>,
}

impl ShutdownHandle {
    /// Get a sender that the caller can use to `send`/`try_send`.
    pub fn sender(&self) -> mpsc::Sender<()> {
        self.tx.clone()
    }

    /// Non-blocking shutdown request (best-effort).
    pub fn try_shutdown(&self) {
        let _ = self.tx.try_send(());
    }

    /// Blocking shutdown request.
    pub async fn shutdown(&self) {
        let _ = self.tx.send(()).await;
    }
}

/// Panic-only task supervisor.
///
/// Design notes:
/// - Workers are managed via `JoinSet<()>` so that we can detect abnormal exits.
/// - On shutdown: save a snapshot then abort all workers and wait for them.
/// - On panic: abort all workers, drain join set, wait a tiny backoff, and rebuild.
pub struct Supervisor {
    name: &'static str,
    shutdown_rx: mpsc::Receiver<()>,
    join_set: JoinSet<()>,
}

impl Supervisor {
    /// Create a new supervisor.
    pub fn new(name: &'static str) -> (Self, ShutdownHandle) {
        let (tx, rx) = mpsc::channel(1);
        (
            Self {
                name,
                shutdown_rx: rx,
                join_set: JoinSet::new(),
            },
            ShutdownHandle { tx },
        )
    }

    /// Spawn a worker task under supervision.
    pub fn spawn(&mut self, fut: impl std::future::Future<Output = ()> + Send + 'static) {
        self.join_set.spawn(fut);
    }

    async fn drain_join_set(&mut self) {
        while let Some(_res) = self.join_set.join_next().await {}
    }

    async fn abort_all_and_drain(&mut self) {
        self.join_set.abort_all();
        self.drain_join_set().await;
    }

    /// Run a pipeline builder under supervision.
    ///
    /// - `build_pipeline`: creates all channels and tasks by calling `sup.spawn(...)`.
    /// - `on_shutdown_save`: called exactly once when shutdown is requested.
    pub async fn run<BuildPipeline, SaveOnShutdown>(
        &mut self,
        mut build_pipeline: BuildPipeline,
        on_shutdown_save: SaveOnShutdown,
    ) where
        BuildPipeline: FnMut(&mut Supervisor) + Send + 'static,
        SaveOnShutdown: FnOnce() + Send + 'static,
    {
        let mut on_shutdown_save = Some(on_shutdown_save);

        // Initial start.
        build_pipeline(self);

        loop {
            tokio::select! {
                // Graceful shutdown: save snapshot then stop everything.
                _ = self.shutdown_rx.recv() => {
                    // #region agent_debuglog shutdown_received
                    debug_log_ndjson(
                        "H_SHUTDOWN",
                        "src/supervisor.rs:run",
                        "shutdown_requested",
                        &format!(
                            "{{\"supervisor\":\"{}\"}}",
                            json_escape(self.name)
                        ),
                    );
                    // #endregion
                    if let Some(save) = on_shutdown_save.take() {
                        save();
                    }
                    self.abort_all_and_drain().await;
                    break;
                }

                // Panic-only self-healing.
                maybe = self.join_set.join_next() => {
                    match maybe {
                        None => {
                            // join_set empty: nothing to supervise (treat as stop).
                            break;
                        }
                        Some(Ok(())) => {
                            // Normal completion: per spec, restart only on panic.
                        }
                        Some(Err(e)) => {
                            if is_panic_join_error(&e) {
                                eprintln!("[supervisor:{}] worker panicked, restarting pipeline", self.name);

                                // #region agent_debuglog panic_detected
                                debug_log_ndjson(
                                    "H_PANIC_ONLY_RESTART",
                                    "src/supervisor.rs:run",
                                    "panic_detected_restart",
                                    &format!(
                                        "{{\"supervisor\":\"{}\"}}",
                                        json_escape(self.name)
                                    ),
                                );
                                // #endregion

                                self.abort_all_and_drain().await;
                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                                // Rebuild pipeline after full stop.
                                // #region agent_debuglog pipeline_rebuilt
                                debug_log_ndjson(
                                    "H_PANIC_ONLY_RESTART",
                                    "src/supervisor.rs:run",
                                    "pipeline_rebuilt",
                                    &format!(
                                        "{{\"supervisor\":\"{}\"}}",
                                        json_escape(self.name)
                                    ),
                                );
                                // #endregion
                                build_pipeline(self);
                            } else {
                                // Cancellation/abort: not a panic-only restart.
                            }
                        }
                    }
                }
            }
        }
    }
}

fn is_panic_join_error(e: &JoinError) -> bool {
    e.is_panic()
}

/// Save a text snapshot under `./logs/` using a timestamped filename.
pub fn save_text_snapshot(prefix: &str, contents: &str) {
    let _ = fs::create_dir_all("./logs");
    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    let path = format!("./logs/{}_{}.txt", prefix, ts_ms);
    let _ = fs::write(path, contents);
}

fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Write one NDJSON line to a debug log file (dev builds only; no-op in release).
pub fn debug_log_ndjson(
    hypothesis_id: &str,
    location: &str,
    message: &str,
    data_json: &str,
) {
    #[cfg(debug_assertions)]
    {
        const SESSION_ID: &str = "ddc943";
        const RUN_ID: &str = "debug_1";
        const LOG_PATH: &str = "/Users/nakamuramao/RTSassignment/.cursor/debug-ddc943.log";

        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();

        let line = format!(
            "{{\"sessionId\":\"{}\",\"runId\":\"{}\",\"hypothesisId\":\"{}\",\"location\":\"{}\",\"message\":\"{}\",\"data\":{},\"timestamp\":{}}}\n",
            SESSION_ID,
            RUN_ID,
            json_escape(hypothesis_id),
            json_escape(location),
            json_escape(message),
            data_json,
            ts_ms
        );

        if let Ok(mut f) = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(LOG_PATH)
        {
            let _ = f.write_all(line.as_bytes());
        }
    }
    let _ = (hypothesis_id, location, message, data_json);
}

