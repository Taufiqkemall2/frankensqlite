use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use fsqlite_harness::e2e_orchestrator::{
    ManifestExecutionMode, build_default_manifest, build_execution_manifest, execute_manifest,
};

#[derive(Debug)]
struct CliConfig {
    execute: bool,
    root_seed: Option<u64>,
    workspace_root: PathBuf,
    run_dir: PathBuf,
    summary_out: Option<PathBuf>,
    manifest_out: Option<PathBuf>,
}

fn default_workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn print_help() {
    let help = "\
e2e_full_suite_runner — canonical deterministic E2E script orchestrator (bd-mblr.4.5.2)

USAGE:
    cargo run -p fsqlite-harness --bin e2e_full_suite_runner -- [OPTIONS]

OPTIONS:
    --execute                   Execute scripts (default: dry-run summary only)
    --root-seed <u64>           Override manifest root seed
    --workspace-root <PATH>     Workspace root (default: repo root)
    --run-dir <PATH>            Run artifact directory (default: artifacts/e2e_full_suite)
    --summary-out <PATH>        Write execution summary JSON to file
    --manifest-out <PATH>       Write manifest JSON to file
    -h, --help                  Show this help
";
    println!("{help}");
}

fn resolve_path(workspace_root: &Path, raw: &str) -> PathBuf {
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        path
    } else {
        workspace_root.join(path)
    }
}

fn parse_args(args: &[String]) -> Result<CliConfig, String> {
    let workspace_root = default_workspace_root()?;
    let mut cfg = CliConfig {
        execute: false,
        root_seed: None,
        run_dir: workspace_root.join("artifacts/e2e_full_suite"),
        workspace_root,
        summary_out: None,
        manifest_out: None,
    };

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--execute" => cfg.execute = true,
            "--root-seed" => {
                i += 1;
                if i >= args.len() {
                    return Err("--root-seed requires a value".to_owned());
                }
                let seed = args[i]
                    .parse::<u64>()
                    .map_err(|_| format!("invalid --root-seed value: {}", args[i]))?;
                cfg.root_seed = Some(seed);
            }
            "--workspace-root" => {
                i += 1;
                if i >= args.len() {
                    return Err("--workspace-root requires a value".to_owned());
                }
                cfg.workspace_root = resolve_path(&cfg.workspace_root, &args[i]);
                if !cfg.workspace_root.exists() {
                    return Err(format!(
                        "workspace root does not exist: {}",
                        cfg.workspace_root.display()
                    ));
                }
                if cfg.run_dir == default_workspace_root()?.join("artifacts/e2e_full_suite") {
                    cfg.run_dir = cfg.workspace_root.join("artifacts/e2e_full_suite");
                }
            }
            "--run-dir" => {
                i += 1;
                if i >= args.len() {
                    return Err("--run-dir requires a value".to_owned());
                }
                cfg.run_dir = resolve_path(&cfg.workspace_root, &args[i]);
            }
            "--summary-out" => {
                i += 1;
                if i >= args.len() {
                    return Err("--summary-out requires a value".to_owned());
                }
                cfg.summary_out = Some(resolve_path(&cfg.workspace_root, &args[i]));
            }
            "--manifest-out" => {
                i += 1;
                if i >= args.len() {
                    return Err("--manifest-out requires a value".to_owned());
                }
                cfg.manifest_out = Some(resolve_path(&cfg.workspace_root, &args[i]));
            }
            "-h" | "--help" => {
                print_help();
                return Err(String::new());
            }
            unknown => return Err(format!("unknown option: {unknown}")),
        }
        i += 1;
    }

    Ok(cfg)
}

fn run(args: &[String]) -> Result<bool, String> {
    let cfg = parse_args(args)?;
    let manifest = if let Some(seed) = cfg.root_seed {
        build_execution_manifest(seed)
    } else {
        build_default_manifest()
    };

    let validation_errors = manifest.validate();
    if !validation_errors.is_empty() {
        return Err(format!(
            "manifest_validation_failed: {}",
            validation_errors.join("; ")
        ));
    }

    if let Some(path) = &cfg.manifest_out {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("manifest_out_parent_create_failed: {error}"))?;
        }
        let manifest_json = manifest
            .to_json()
            .map_err(|error| format!("manifest_json_serialize_failed: {error}"))?;
        fs::write(path, manifest_json).map_err(|error| {
            format!(
                "manifest_out_write_failed path={} error={error}",
                path.display()
            )
        })?;
    }

    let mode = if cfg.execute {
        ManifestExecutionMode::Execute
    } else {
        ManifestExecutionMode::DryRun
    };
    let summary = execute_manifest(&cfg.workspace_root, &cfg.run_dir, &manifest, mode)
        .map_err(|error| format!("manifest_execution_failed: {error}"))?;

    let summary_json = summary
        .to_json()
        .map_err(|error| format!("summary_json_serialize_failed: {error}"))?;

    if let Some(path) = &cfg.summary_out {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("summary_out_parent_create_failed: {error}"))?;
        }
        fs::write(path, summary_json).map_err(|error| {
            format!(
                "summary_out_write_failed path={} error={error}",
                path.display()
            )
        })?;
        println!(
            "INFO e2e_full_suite_summary_written path={} overall_pass={}",
            path.display(),
            summary.overall_pass
        );
    } else {
        println!("{summary_json}");
    }

    Ok(summary.overall_pass)
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match run(&args) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(error) if error.is_empty() => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ERROR e2e_full_suite_runner failed: {error}");
            ExitCode::from(2)
        }
    }
}
