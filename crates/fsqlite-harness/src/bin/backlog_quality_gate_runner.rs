use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use fsqlite_harness::backlog_quality_gate::{
    BacklogQualityGateConfig, default_generated_unix_ms, run_backlog_quality_gate,
    write_report_json,
};

#[derive(Debug, Clone)]
struct Config {
    beads_path: PathBuf,
    baseline_path: Option<PathBuf>,
    output_json: PathBuf,
    output_human: PathBuf,
    critical_priority_max: i64,
    generated_unix_ms: u128,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let workspace_root = default_workspace_root()?;
        let mut beads_path = workspace_root.join(".beads/issues.jsonl");
        let mut baseline_path =
            Some(workspace_root.join("conformance/backlog_quality_gate_baseline.json"));
        let mut output_json = workspace_root.join("target/backlog_quality_gate_report.json");
        let mut output_human = workspace_root.join("target/backlog_quality_gate_report.md");
        let mut critical_priority_max = 1_i64;
        let mut generated_unix_ms = default_generated_unix_ms();

        let args: Vec<String> = env::args().skip(1).collect();
        let mut idx = 0_usize;
        while idx < args.len() {
            match args[idx].as_str() {
                "--beads" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --beads".to_owned())?;
                    beads_path = PathBuf::from(value);
                }
                "--baseline" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --baseline".to_owned())?;
                    baseline_path = Some(PathBuf::from(value));
                }
                "--no-baseline" => {
                    baseline_path = None;
                }
                "--output-json" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --output-json".to_owned())?;
                    output_json = PathBuf::from(value);
                }
                "--output-human" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --output-human".to_owned())?;
                    output_human = PathBuf::from(value);
                }
                "--critical-priority-max" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --critical-priority-max".to_owned())?;
                    critical_priority_max = value.parse::<i64>().map_err(|error| {
                        format!("invalid --critical-priority-max value={value}: {error}")
                    })?;
                }
                "--generated-unix-ms" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --generated-unix-ms".to_owned())?;
                    generated_unix_ms = value.parse::<u128>().map_err(|error| {
                        format!("invalid --generated-unix-ms value={value}: {error}")
                    })?;
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
            beads_path,
            baseline_path,
            output_json,
            output_human,
            critical_priority_max,
            generated_unix_ms,
        })
    }
}

fn default_workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn print_help() {
    println!(
        "\
backlog_quality_gate_runner — acceptance completeness regression gate (bd-1dp9.9.6)

USAGE:
  cargo run -p fsqlite-harness --bin backlog_quality_gate_runner -- [OPTIONS]

OPTIONS:
  --beads <PATH>                  Beads JSONL path (default: .beads/issues.jsonl)
  --baseline <PATH>               Baseline JSON path
  --no-baseline                   Disable regression baseline (strict mode)
  --output-json <PATH>            JSON report output path
  --output-human <PATH>           Markdown summary output path
  --critical-priority-max <I64>   Priority threshold for critical-path gating (default: 1)
  --generated-unix-ms <U128>      Deterministic timestamp for report output
  -h, --help                      Show help
"
    );
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
    let report = run_backlog_quality_gate(&BacklogQualityGateConfig {
        beads_path: config.beads_path.clone(),
        baseline_path: config.baseline_path.clone(),
        critical_priority_max: config.critical_priority_max,
        generated_unix_ms: Some(config.generated_unix_ms),
    })
    .map_err(|error| format!("backlog_quality_gate_failed: {error}"))?;

    write_report_json(&config.output_json, &report)
        .map_err(|error| format!("write_json_failed: {error}"))?;
    write_text(&config.output_human, &report.render_markdown())?;

    println!(
        "INFO backlog_quality_gate overall_pass={} scanned_active={} scanned_critical={} \
total_failures={} critical_failures={} regression_failures={} report={} summary={}",
        report.overall_pass,
        report.summary.scanned_active_beads,
        report.summary.scanned_critical_beads,
        report.summary.total_failures,
        report.summary.critical_failures,
        report.summary.regression_failures,
        config.output_json.display(),
        config.output_human.display(),
    );

    if !report.regression_failures.is_empty() {
        for failure in &report.regression_failures {
            println!(
                "ERROR backlog_quality_regression issue_id={} missing={}",
                failure.issue_id,
                failure
                    .missing_requirements
                    .iter()
                    .map(|item| item.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            );
        }
    }

    Ok(report.overall_pass)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => {
            eprintln!("ERROR backlog_quality_gate_runner overall_pass=false");
            ExitCode::from(1)
        }
        Err(error) => {
            eprintln!("ERROR backlog_quality_gate_runner failed: {error}");
            ExitCode::from(2)
        }
    }
}
