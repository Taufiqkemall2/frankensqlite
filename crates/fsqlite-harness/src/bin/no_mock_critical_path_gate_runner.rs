use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite_harness::no_mock_critical_path_gate::{
    DEFAULT_CRITICAL_CATEGORIES, NoMockVerdict, evaluate_no_mock_critical_path_gate,
};

const BEAD_ID: &str = "bd-mblr.3.4";

#[derive(Debug, Clone)]
struct Config {
    output_json: Option<PathBuf>,
    output_human: Option<PathBuf>,
    fail_on_warnings: bool,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut output_json: Option<PathBuf> = None;
        let mut output_human: Option<PathBuf> = None;
        let mut fail_on_warnings = false;

        let args: Vec<String> = env::args().skip(1).collect();
        let mut idx = 0_usize;
        while idx < args.len() {
            match args[idx].as_str() {
                "--output-json" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --output-json".to_owned())?;
                    output_json = Some(PathBuf::from(value));
                }
                "--output-human" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --output-human".to_owned())?;
                    output_human = Some(PathBuf::from(value));
                }
                "--fail-on-warnings" => {
                    fail_on_warnings = true;
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown_argument: {other}")),
            }
            idx += 1;
        }

        Ok(Self {
            output_json,
            output_human,
            fail_on_warnings,
        })
    }
}

fn print_help() {
    println!(
        "\
no_mock_critical_path_gate_runner — enforce non-mock evidence for critical invariants (bd-mblr.3.4)

USAGE:
  cargo run -p fsqlite-harness --bin no_mock_critical_path_gate_runner -- [OPTIONS]

OPTIONS:
  --output-json <PATH>      Write machine-readable gate report JSON
  --output-human <PATH>     Write human-readable gate summary markdown
  --fail-on-warnings        Treat PASS_WITH_WARNINGS as failure
  -h, --help                Show help
"
    );
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

fn write_text(path: &Path, content: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "output_parent_create_failed path={} error={error}",
                parent.display()
            )
        })?;
    }
    fs::write(path, content)
        .map_err(|error| format!("output_write_failed path={} error={error}", path.display()))
}

fn run() -> Result<bool, String> {
    let config = Config::parse()?;
    let generated_unix_ms = now_unix_ms();
    let report = evaluate_no_mock_critical_path_gate(&DEFAULT_CRITICAL_CATEGORIES);

    if let Some(path) = &config.output_json {
        let json = report
            .to_json()
            .map_err(|error| format!("report_serialize_failed: {error}"))?;
        write_text(path, &json)?;
        println!(
            "INFO no_mock_critical_path_report_json path={}",
            path.display()
        );
    }

    if let Some(path) = &config.output_human {
        write_text(path, &report.render_summary())?;
        println!(
            "INFO no_mock_critical_path_report_summary path={}",
            path.display()
        );
    }

    println!(
        "INFO no_mock_critical_path_gate bead_id={} generated_unix_ms={} verdict={} \
blocking_count={} warning_count={} total_critical_invariants={} real_evidence_count={} \
exception_count={} missing_evidence_count={}",
        BEAD_ID,
        generated_unix_ms,
        report.verdict,
        report.blocking_count,
        report.warning_count,
        report.total_critical_invariants,
        report.real_evidence_count,
        report.exception_count,
        report.missing_evidence_count,
    );

    let overall_pass = match report.verdict {
        NoMockVerdict::Fail => false,
        NoMockVerdict::PassWithWarnings => !config.fail_on_warnings,
        NoMockVerdict::Pass => true,
    };
    Ok(overall_pass)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => {
            eprintln!("ERROR no_mock_critical_path_gate_runner overall_pass=false");
            ExitCode::from(1)
        }
        Err(error) => {
            eprintln!("ERROR no_mock_critical_path_gate_runner failed: {error}");
            ExitCode::from(2)
        }
    }
}
