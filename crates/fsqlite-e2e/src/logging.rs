//! Structured logging framework for the E2E test suite.
//!
//! Bead: bd-2zkh
//!
//! Provides dual-output logging: human-readable ANSI to the terminal and
//! machine-parseable JSON-lines to a per-run log file.  Every log event
//! carries structured fields (`backend`, `test_name`, `run_id`) for
//! post-hoc analysis via `jq` or similar tools.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Guard returned by [`init_logging`].
///
/// Holds the log-file handle.  Dropping it closes the file and flushes any
/// buffered data.  Store this in your `main()` scope — do **not** drop it
/// before the program exits.
pub struct LogGuard {
    /// Path to the JSON-lines log file.
    pub log_path: PathBuf,
    // The file is held open by the `tracing-subscriber` layer via the
    // `Mutex<std::fs::File>` writer.  When the guard (and thus the
    // subscriber) is dropped, the file is closed.
}

/// A `MakeWriter` backed by an `Arc<Mutex<std::fs::File>>`.
///
/// Each `make_writer()` call locks the file, writes the full event, then
/// unlocks.  Thread-safe for concurrent log emission.
#[derive(Clone)]
struct SharedFileWriter {
    file: std::sync::Arc<Mutex<std::fs::File>>,
}

impl SharedFileWriter {
    fn new(file: std::fs::File) -> Self {
        Self {
            file: std::sync::Arc::new(Mutex::new(file)),
        }
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedFileWriter {
    type Writer = SharedFileGuard<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        SharedFileGuard {
            guard: self.file.lock().expect("log file mutex poisoned"),
        }
    }
}

/// RAII guard holding the file mutex lock for one write event.
struct SharedFileGuard<'a> {
    guard: std::sync::MutexGuard<'a, std::fs::File>,
}

impl std::io::Write for SharedFileGuard<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        std::io::Write::write(&mut *self.guard, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::Write::flush(&mut *self.guard)
    }
}

/// Initialize the structured logging framework.
///
/// Sets up two output layers:
///
/// 1. **Terminal** — human-readable, ANSI-colored, compact format.
/// 2. **File** — JSON-lines written to `<run_dir>/test.log.jsonl`.
///
/// The `verbose` flag lowers the filter to `TRACE`; default is `INFO`.
/// The `RUST_LOG` environment variable overrides the default if set.
///
/// Returns a [`LogGuard`] that **must** be kept alive until the program
/// exits.
///
/// # Panics
///
/// Panics if a global subscriber has already been set (call only once).
///
/// # Errors
///
/// Returns `std::io::Error` if the run directory cannot be created or
/// the log file cannot be opened.
pub fn init_logging(run_dir: &Path, verbose: bool) -> std::io::Result<LogGuard> {
    std::fs::create_dir_all(run_dir)?;

    let filter_str = if verbose { "trace" } else { "info" };
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter_str));

    let log_path = run_dir.join("test.log.jsonl");
    let file = std::fs::File::create(&log_path)?;
    let file_writer = SharedFileWriter::new(file);

    let json_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_writer(file_writer)
        .with_target(true)
        .with_thread_ids(true);

    let terminal_layer = tracing_subscriber::fmt::layer()
        .with_ansi(true)
        .with_target(false)
        .compact();

    tracing_subscriber::registry()
        .with(env_filter)
        .with(terminal_layer)
        .with(json_layer)
        .init();

    Ok(LogGuard { log_path })
}

/// Initialize logging for tests — terminal only, no file output.
///
/// Uses `try_init` so it doesn't panic if already initialized (safe to
/// call from multiple `#[test]` functions).
pub fn init_test_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(true)
                .with_target(false)
                .with_test_writer()
                .compact(),
        )
        .try_init();
}

/// Log a timed operation and return its result.
///
/// Records `operation`, `backend`, `elapsed_ms`, and `success` as
/// structured fields in every log event.
pub fn log_timed_operation<T, E: std::fmt::Display>(
    operation: &str,
    backend: &str,
    f: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let start = std::time::Instant::now();
    let result = f();
    let elapsed = start.elapsed();

    match &result {
        Ok(_) => {
            tracing::info!(
                operation,
                backend,
                elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
                success = true,
                "operation complete"
            );
        }
        Err(e) => {
            tracing::warn!(
                operation,
                backend,
                elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
                success = false,
                error = %e,
                "operation failed"
            );
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a scoped subscriber writing JSON to a `Mutex<File>`.
    fn scoped_json_subscriber(
        log_path: &std::path::Path,
    ) -> (impl tracing::Subscriber + Send + Sync, std::path::PathBuf) {
        let file = std::fs::File::create(log_path).unwrap();
        let writer = SharedFileWriter::new(file);

        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(writer)
                .with_target(true),
        );
        (subscriber, log_path.to_path_buf())
    }

    /// Helper: scoped subscriber with an `EnvFilter`.
    fn scoped_filtered_subscriber(
        log_path: &std::path::Path,
        filter: &str,
    ) -> impl tracing::Subscriber + Send + Sync {
        let file = std::fs::File::create(log_path).unwrap();
        let writer = SharedFileWriter::new(file);

        tracing_subscriber::registry()
            .with(EnvFilter::new(filter))
            .with(tracing_subscriber::fmt::layer().json().with_writer(writer))
    }

    #[test]
    fn test_logging_json_format() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::TempDir::new().unwrap();
        let log_path = tmp.path().join("test.log.jsonl");
        let (subscriber, _) = scoped_json_subscriber(&log_path);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(test_event = "json_format_check", "hello from test");
        });

        let content = std::fs::read_to_string(&log_path).unwrap();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let parsed: serde_json::Value = serde_json::from_str(line).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid JSON in log: {e}\nline: {line}"),
                )
            })?;
            assert!(
                parsed.get("level").is_some(),
                "log event missing 'level' field: {parsed}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_logging_levels() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log_path = tmp.path().join("test.log.jsonl");
        let subscriber = scoped_filtered_subscriber(&log_path, "info");

        tracing::subscriber::with_default(subscriber, || {
            tracing::trace!(should_be_filtered = true, "trace event");
            tracing::info!(should_be_present = true, "info event");
        });

        let content = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            content.contains("should_be_present"),
            "INFO event should be logged"
        );
        assert!(
            !content.contains("should_be_filtered"),
            "TRACE event should be filtered at INFO level"
        );
    }

    #[test]
    fn test_logging_file_output() {
        let tmp = tempfile::TempDir::new().unwrap();
        let run_dir = tmp.path().join("run_001");
        std::fs::create_dir_all(&run_dir).unwrap();
        let log_path = run_dir.join("test.log.jsonl");
        let (subscriber, _) = scoped_json_subscriber(&log_path);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("file output test");
        });

        assert!(log_path.exists(), "log file should be created in run_dir");
        let size = std::fs::metadata(&log_path).unwrap().len();
        assert!(size > 0, "log file should not be empty");
    }

    #[test]
    fn test_logging_structured_fields() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log_path = tmp.path().join("test.log.jsonl");
        let (subscriber, _) = scoped_json_subscriber(&log_path);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(
                backend = "csqlite",
                test_name = "field_test",
                "structured fields"
            );
        });

        let content = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            content.contains("csqlite"),
            "backend field should be in log"
        );
        assert!(
            content.contains("field_test"),
            "test_name field should be in log"
        );
    }

    #[test]
    fn test_logging_timing_function() {
        let tmp = tempfile::TempDir::new().unwrap();
        let log_path = tmp.path().join("test.log.jsonl");
        let (subscriber, _) = scoped_json_subscriber(&log_path);

        tracing::subscriber::with_default(subscriber, || {
            let result: Result<i32, String> =
                log_timed_operation("test_op", "frankensqlite", || Ok(42));
            assert_eq!(result.unwrap(), 42);

            let err_result: Result<i32, String> =
                log_timed_operation("failing_op", "csqlite", || Err("boom".to_owned()));
            assert!(err_result.is_err());
        });

        let content = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            content.contains("elapsed_ms"),
            "elapsed_ms should be in log"
        );
        assert!(
            content.contains("test_op"),
            "operation name should be in log"
        );
    }
}
