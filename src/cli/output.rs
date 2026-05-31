//! main.rs split — output (Phase H).
use super::*;

pub(crate) fn output_json<T: Serialize>(value: &T, label: &str) {
    match serde_json::to_string_pretty(value) {
        Ok(json) => println!("{json}"),
        Err(err) => {
            eprintln!("failed to serialize {label} JSON: {err}");
            process::exit(1);
        }
    }
}

pub(crate) fn print_startup_lifecycle_human(report: &StartupLifecycleReport) {
    let command = if report.dry_run {
        "dry-run"
    } else if report.force_sync {
        "force-sync"
    } else {
        "startup"
    };
    let outcome = if report.completed && report.errors.is_empty() {
        "completed"
    } else if report.errors.is_empty() {
        "stopped"
    } else {
        "failed"
    };

    print_report_log_line(
        "INFO",
        &format!(
            "UDB {command} {outcome} run_id={} state={}",
            report.run_id, report.state
        ),
    );
    print_report_log_line(
        "INFO",
        &format!(
            "summary applied_sql_artifacts={} verified_tables={} verified_vector_collections={} verified_object_buckets={}",
            report.applied_sql_artifacts,
            report.verified_tables,
            report.verified_vector_collections,
            report.verified_object_buckets
        ),
    );

    for step in &report.steps {
        if let Some((phase, message)) = step.split_once(": ") {
            print_report_log_line("INFO", &format!("{phase:<19} {message}"));
        } else {
            print_report_log_line("INFO", step);
        }
    }

    if !report.dry_run_plan.is_empty() {
        print_report_log_line(
            "INFO",
            &format!("dry_run_plan {} SQL artifact(s)", report.dry_run_plan.len()),
        );
    }
    for warning in &report.warnings {
        print_report_log_line("WARN", warning);
    }
    for error in &report.errors {
        print_report_log_line("ERROR", error);
    }
}

pub(crate) fn print_report_log_line(level: &str, message: &str) {
    let time = chrono::Local::now().format("%H:%M");
    let level = match level {
        "WARN" => "WARN ",
        "ERROR" => "ERROR",
        _ => "INFO ",
    };
    let colors = env::var("NO_COLOR").is_err()
        && env::var("UDB_NO_COLOR")
            .map(|value| !matches!(value.as_str(), "1" | "true"))
            .unwrap_or(true);

    if colors {
        const RESET: &str = "\x1b[0m";
        const BOLD: &str = "\x1b[1m";
        const DIM: &str = "\x1b[2m";
        const GREEN: &str = "\x1b[32m";
        const YELLOW: &str = "\x1b[33m";
        const RED: &str = "\x1b[91m";

        let color = match level {
            "WARN " => YELLOW,
            "ERROR" => RED,
            _ => GREEN,
        };
        eprintln!("{DIM}{time}{RESET} {BOLD}{color}{level}{RESET} {message}");
    } else {
        eprintln!("{time} {level} {message}");
    }
}

pub(crate) fn env_truthy(name: &str) -> bool {
    env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

pub(crate) fn fatal_json<T>(label: &str, err: serde_json::Error) -> T {
    eprintln!("{label}: {err}");
    process::exit(1);
}

/// Load a prior `CatalogManifest` for drift comparison.
///
/// Checks (in order):
/// 1. `--prior <path>` CLI flag in `args`
/// 2. `UDB_PRIOR_MANIFEST_PATH` env-var
///
/// Returns `None` when neither is set.
pub(crate) fn load_prior_manifest_from_args(args: &[String]) -> Option<CatalogManifest> {
    let path = args
        .windows(2)
        .find(|w| w[0] == "--prior")
        .map(|w| w[1].clone())
        .or_else(|| {
            env::var("UDB_PRIOR_MANIFEST_PATH")
                .ok()
                .filter(|v| !v.is_empty())
        })?;
    match fs::read_to_string(&path) {
        Ok(content) => match serde_json::from_str::<CatalogManifest>(&content) {
            Ok(manifest) => {
                eprintln!("loaded prior manifest from {path}");
                Some(manifest)
            }
            Err(err) => {
                eprintln!(
                    "warning: could not parse prior manifest '{path}': {err} — running without diff"
                );
                None
            }
        },
        Err(err) => {
            eprintln!(
                "warning: could not read prior manifest '{path}': {err} — running without diff"
            );
            None
        }
    }
}

/// Load ABAC policies from `UDB_ABAC_POLICY_FILE`.
/// Returns an empty Vec if the env-var is unset or the file cannot be read.
pub(crate) fn load_abac_policies_for_lint() -> Vec<AbacPolicy> {
    let path = match env::var("UDB_ABAC_POLICY_FILE") {
        Ok(p) if !p.is_empty() => p,
        _ => {
            eprintln!("UDB_ABAC_POLICY_FILE is not set; linting against empty policy set");
            return Vec::new();
        }
    };
    match fs::read_to_string(&path) {
        Ok(content) => match serde_json::from_str::<Vec<AbacPolicy>>(&content) {
            Ok(policies) => {
                eprintln!("loaded {} policies from {path}", policies.len());
                policies
            }
            Err(err) => {
                eprintln!("failed to parse ABAC policy file '{path}': {err}");
                Vec::new()
            }
        },
        Err(err) => {
            eprintln!("failed to read ABAC policy file '{path}': {err}");
            Vec::new()
        }
    }
}

/// Escape and quote a string as a PostgreSQL string literal (single-quoted, escaping `'`).
pub(crate) fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) fn pg_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(crate) fn pg_relation(schema: &str, table: &str) -> String {
    format!("{}.{}", pg_identifier(schema), pg_identifier(table))
}

// ── Compatibility matrix ──────────────────────────────────────────────────────
