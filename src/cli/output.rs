//! main.rs split — output (Phase H).
use super::*;
use std::collections::HashSet;
use std::sync::OnceLock;

pub(crate) fn output_json<T: Serialize>(value: &T, label: &str) {
    let mut value = match serde_json::to_value(value) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("failed to serialize {label} JSON: {err}");
            process::exit(1);
        }
    };
    redact_sensitive_json(&mut value);
    // #216: stream the pretty JSON straight into a buffered stdout handle
    // instead of building a throwaway `String` per call.
    let stdout = std::io::stdout();
    if let Err(err) = write_json_value(stdout.lock(), &value) {
        // A downstream reader that closed the pipe early (`udb … | head`)
        // surfaces as BrokenPipe (EPIPE / os error 109). That is the normal
        // end of a consumer, not a failure: exit cleanly and silently instead
        // of the old exit(1)+stderr, which printed a spurious error line. The
        // matching cover for the many raw `println!` sites is the panic hook
        // in mod.rs (`install_broken_pipe_panic_hook`).
        if err.kind() == std::io::ErrorKind::BrokenPipe {
            process::exit(0);
        }
        eprintln!("failed to write {label} JSON to stdout: {err}");
        process::exit(1);
    }
}

/// Stream pretty JSON to `writer` through a buffered handle, recovering the
/// underlying `io::ErrorKind` (notably `BrokenPipe`) on failure.
///
/// `serde_json::to_writer_pretty` returns a `serde_json::Error`; converting it
/// with `io::Error::from` preserves the original `io::ErrorKind` when the
/// failure was an IO error (e.g. a closed pipe), so `output_json` can
/// special-case `BrokenPipe`. Kept separate from `output_json` so it is
/// unit-testable against an early-closing writer without terminating the test
/// process via `process::exit`.
fn write_json_value<W: std::io::Write>(
    writer: W,
    value: &serde_json::Value,
) -> std::io::Result<()> {
    use std::io::Write;
    let mut writer = std::io::BufWriter::new(writer);
    serde_json::to_writer_pretty(&mut writer, value).map_err(std::io::Error::from)?;
    writeln!(writer)?;
    writer.flush()
}

pub(crate) fn redact_sensitive_json(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            // A one-time key reveal (UDB_SHOW_SECRET=1) stamps `secret_revealed:
            // true` on the result object. In that case the one-time secret in
            // the SAME object must pass through — otherwise the "store this key
            // now" note prints but the key itself is redacted and unrecoverable.
            let revealed = map
                .get("secret_revealed")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            for (key, child) in map.iter_mut() {
                if is_sensitive_output_key(key) && !(revealed && is_revealable_secret_key(key)) {
                    *child = serde_json::Value::String("[REDACTED]".to_string());
                } else {
                    redact_sensitive_json(child);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                redact_sensitive_json(item);
            }
        }
        _ => {}
    }
}

fn is_sensitive_output_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    if descriptor_sensitive_output_keys().contains(normalized.as_str()) {
        return true;
    }
    matches!(
        normalized.as_str(),
        "token"
            | "access_token"
            | "refresh_token"
            | "bearer_token"
            | "session_id"
            | "session_token"
            | "csrf_token"
            | "plain_key"
            | "api_key"
            // NOTE: `key_id` / `key_prefix` are public identifiers used in REST
            // paths and the admin UI, and `key` is the generic identifier field
            // of an api-key create result — redacting them made the create
            // output unusable for later get/delete/patch. The actual secret is
            // `plain_key` / `key_hash`, which remain redacted.
            | "key_hash"
            | "secret"
            | "totp_secret"
            | "totp_qr_uri"
            | "otp_id"
            | "mfa_otp_id"
            | "operation_id"
    )
}

fn descriptor_sensitive_output_keys() -> &'static HashSet<String> {
    static KEYS: OnceLock<HashSet<String>> = OnceLock::new();
    KEYS.get_or_init(|| {
        let manifest = udb::runtime::descriptor_manifest::descriptor_contract_manifest_static();
        manifest
            .messages
            .iter()
            .flat_map(|message| &message.fields)
            .filter(|field| {
                let scalar = &field.scalar_security;
                let column = field.db_column_security.as_ref();
                scalar.sensitive
                    || scalar.encrypted_security
                    || scalar.log_redacted
                    || column.is_some_and(|security| {
                        matches!(security.output_view, 1 | 6)
                            || matches!(security.redaction_strategy, 3 | 4)
                    })
            })
            .map(|field| field.name.to_ascii_lowercase())
            .collect()
    })
}

/// Keys whose value is a one-time secret that the user explicitly asked to
/// reveal (via `secret_revealed: true`). Only these are exempt from redaction
/// on a reveal — long-lived secrets (`key_hash`, `token`, …) stay redacted.
fn is_revealable_secret_key(key: &str) -> bool {
    matches!(key.to_ascii_lowercase().as_str(), "plain_key" | "api_key")
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

/// Load authorization (Casbin `AuthzPolicy`) rules from `UDB_ABAC_POLICY_FILE`
/// (a JSON array of AuthzPolicy objects). Returns an empty Vec only when the
/// env-var is unset.
pub(crate) fn load_authz_policies_for_lint() -> Result<Vec<AuthzPolicy>, String> {
    let path = match env::var("UDB_ABAC_POLICY_FILE") {
        Ok(p) if !p.is_empty() => p,
        _ => {
            eprintln!("UDB_ABAC_POLICY_FILE is not set; linting against empty policy set");
            return Ok(Vec::new());
        }
    };
    load_authz_policies_from_file(&path)
}

pub(crate) fn load_authz_policies_from_file(path: &str) -> Result<Vec<AuthzPolicy>, String> {
    let content = fs::read_to_string(path)
        .map_err(|err| format!("failed to read authorization policy file '{path}': {err}"))?;
    let policies = serde_json::from_str::<Vec<AuthzPolicy>>(&content)
        .map_err(|err| format!("failed to parse authorization policy file '{path}': {err}"))?;
    eprintln!("loaded {} policies from {path}", policies.len());
    Ok(policies)
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

#[cfg(test)]
mod broken_pipe_tests {
    use super::write_json_value;
    use std::io::{Error, ErrorKind, Write};

    /// A writer that emulates a downstream reader which closed the pipe early
    /// (`udb … | head`): every write and flush fails with `BrokenPipe` — the
    /// same kind std maps EPIPE (os error 32, Unix) and ERROR_BROKEN_PIPE (os
    /// error 109, Windows) to. Deterministic and identical on both platforms.
    struct BrokenPipeWriter;

    impl Write for BrokenPipeWriter {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            Err(Error::from(ErrorKind::BrokenPipe))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Err(Error::from(ErrorKind::BrokenPipe))
        }
    }

    #[test]
    fn small_output_surfaces_broken_pipe_from_flush() {
        // Small payload stays in the buffer until flush; the broken pipe
        // surfaces there. `output_json` maps this kind to a clean exit(0)
        // rather than panicking or printing to stderr.
        let value = serde_json::json!({ "ok": true, "label": "small" });
        let err = write_json_value(BrokenPipeWriter, &value)
            .expect_err("write to a closed pipe must return an error, not panic");
        assert_eq!(err.kind(), ErrorKind::BrokenPipe);
    }

    #[test]
    fn large_output_surfaces_broken_pipe_mid_serialization() {
        // A large payload overflows the internal BufWriter, forcing a real
        // write mid-serialization — so the broken pipe surfaces from
        // `to_writer_pretty` (the `serde_json::Error` -> `io::Error::from`
        // branch), proving that path is recovered too, not just the final
        // flush. This is the "large-output against an early-closing reader"
        // case that used to blow up with a backtrace.
        let rows: Vec<String> = (0..4000).map(|i| format!("row-{i:010}")).collect();
        let value = serde_json::to_value(&rows).expect("serialize rows");
        let err = write_json_value(BrokenPipeWriter, &value)
            .expect_err("write to a closed pipe must return an error, not panic");
        assert_eq!(err.kind(), ErrorKind::BrokenPipe);
    }
}
