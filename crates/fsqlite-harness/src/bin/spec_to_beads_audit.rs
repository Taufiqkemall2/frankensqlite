use std::env;
use std::path::PathBuf;

use fsqlite_harness::spec_to_beads_audit::{
    AuditConfig, AuditMode, run_spec_to_beads_audit, write_report_json,
};

fn main() {
    if let Err(err) = real_main() {
        eprintln!("spec_to_beads_audit: {err}");
        std::process::exit(1);
    }
}

fn real_main() -> Result<(), String> {
    let workspace_root = workspace_root();

    let mut mode = AuditMode::Strict;
    let mut spec_path = workspace_root.join("COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md");
    let mut beads_path = workspace_root.join(".beads/issues.jsonl");
    let mut report_path = workspace_root.join("target/spec_to_beads_audit.json");

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--fast" => mode = AuditMode::Fast,
            "--strict" => mode = AuditMode::Strict,
            "--spec" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--spec requires a path argument".to_string())?;
                spec_path = PathBuf::from(value);
            }
            "--beads" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--beads requires a path argument".to_string())?;
                beads_path = PathBuf::from(value);
            }
            "--report" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--report requires a path argument".to_string())?;
                report_path = PathBuf::from(value);
            }
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    let config = AuditConfig {
        spec_path,
        beads_path,
        mode,
    };
    let report = run_spec_to_beads_audit(&config).map_err(|err| err.to_string())?;
    write_report_json(&report_path, &report).map_err(|err| err.to_string())?;

    println!(
        "spec_to_beads_audit mode={} pass={} checked={} missing={} open_task_failures={} dependency_failures={} report={}",
        report.mode,
        report.pass,
        report.coverage.total_checked_lines,
        report.coverage.missing_lines,
        report.open_task_structure_failures.len(),
        report.dependency_failures.len(),
        report_path.display(),
    );

    if report.pass {
        Ok(())
    } else {
        Err("audit found failures; see JSON report for details".to_string())
    }
}

fn workspace_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .unwrap_or(manifest_dir)
}

fn print_usage() {
    println!(
        "Usage: spec_to_beads_audit [--strict|--fast] [--spec PATH] [--beads PATH] [--report PATH]"
    );
}
