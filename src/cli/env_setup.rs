//! main.rs split — env_setup (Phase H).
use super::*;

/// Only called from the single-threaded CLI dispatch path; no other thread reads
/// the environment concurrently.
pub(crate) fn run_force_sync_for_instance(
    instance: &PgInstance,
    manifest: &udb::CatalogManifest,
    schemas: &[udb::ProtoSchema],
    runtime: &tokio::runtime::Runtime,
    all_ok: &mut bool,
) {
    let dsn = match instance.resolve_dsn() {
        Some(d) => d,
        None => {
            eprintln!(
                "force-sync: [{}] skipped — {} not set",
                instance.name, instance.dsn_env
            );
            return;
        }
    };
    eprintln!("force-sync: [{}] connecting…", instance.display_label());
    #[allow(unused_unsafe)]
    unsafe {
        env::set_var("UDB_PG_DSN", &dsn);
    }
    let broker_runtime = runtime.block_on(DataBrokerRuntime::from_env());
    eprintln!(
        "force-sync: [{}] connected — starting lifecycle",
        instance.display_label()
    );
    match runtime.block_on(run_startup_lifecycle(
        &broker_runtime,
        manifest,
        schemas,
        true,  // force_sync = true
        false, // dry_run = false
    )) {
        Ok(report) => {
            if env_truthy("UDB_REPORT_JSON") {
                output_json(
                    &serde_json::json!({ "instance": instance.name, "report": report }),
                    &format!("force-sync report [{}]", instance.name),
                );
            } else {
                eprintln!("=== {} ===", instance.display_label());
                print_startup_lifecycle_human(&report);
            }
        }
        Err(err) => {
            eprintln!("force-sync: [{}] failed: {err}", instance.display_label());
            *all_ok = false;
        }
    }
}

/// Run `admin dry-run` lifecycle against a single named PG instance.
pub(crate) fn run_dry_run_for_instance(
    instance: &PgInstance,
    manifest: &udb::CatalogManifest,
    schemas: &[udb::ProtoSchema],
    runtime: &tokio::runtime::Runtime,
    all_ok: &mut bool,
) {
    let dsn = match instance.resolve_dsn() {
        Some(d) => d,
        None => {
            eprintln!(
                "dry-run: [{}] skipped — {} not set",
                instance.name, instance.dsn_env
            );
            return;
        }
    };
    eprintln!("dry-run: [{}] connecting…", instance.display_label());
    #[allow(unused_unsafe)]
    unsafe {
        env::set_var("UDB_PG_DSN", &dsn);
    }
    let broker_runtime = runtime.block_on(DataBrokerRuntime::from_env());
    match runtime.block_on(run_startup_lifecycle(
        &broker_runtime,
        manifest,
        schemas,
        false, // force_sync = false
        true,  // dry_run = true — nothing applied
    )) {
        Ok(report) => {
            output_json(
                &serde_json::json!({ "instance": instance.name, "report": report }),
                &format!("dry-run plan [{}]", instance.name),
            );
        }
        Err(err) => {
            eprintln!("dry-run: [{}] failed: {err}", instance.display_label());
            *all_ok = false;
        }
    }
}

pub(crate) fn load_project_dotenv() {
    let app_env = env::var("APP_ENV").unwrap_or_default();
    // Build candidate list; tagged name must outlive the slice.
    let tagged = if app_env.is_empty() {
        None
    } else {
        Some(format!(".env.{}", app_env))
    };
    let mut candidates: Vec<&str> = Vec::new();
    if let Some(ref t) = tagged {
        candidates.push(t.as_str());
    }
    candidates.push(".env.local");
    candidates.push(".env.prod");
    candidates.push(".env");

    // Walk cwd → parent chain until we find one of the candidate files.
    let mut dir = env::current_dir().unwrap_or_default();
    loop {
        for name in &candidates {
            let path = dir.join(name);
            if path.exists() {
                // from_path does NOT override OS env vars — they always win.
                let _ = dotenvy::from_path(&path);
                return;
            }
        }
        match dir.parent() {
            Some(p) => dir = p.to_path_buf(),
            None => return, // reached filesystem root without finding any file
        }
    }
}

pub(crate) fn resolve_existing_project_path(raw: &str) -> PathBuf {
    let trimmed = raw.trim();
    let path = PathBuf::from(trimmed);
    if trimmed.is_empty() || path.is_absolute() || path.exists() {
        return path;
    }

    let mut dir = env::current_dir().unwrap_or_default();
    loop {
        let candidate = dir.join(&path);
        if candidate.exists() {
            return candidate;
        }
        match dir.parent() {
            Some(parent) => dir = parent.to_path_buf(),
            None => return path,
        }
    }
}

/// Load `UDB_CONFIG_PATH` (or `--config <path>`) and project its portable YAML,
/// JSON, or TOML
/// keys into the env vars consumed by the existing runtime.
///
/// This keeps the current env-based runtime backward compatible while giving
/// deployments a single config file to move between projects and environments.
/// OS env vars still win; this only fills missing values.
pub(crate) fn load_udb_config_overlay(args: &[String]) {
    let cli_path = cli_config_path(args);
    let Some(path) = cli_path
        .clone()
        .or_else(|| env::var("UDB_CONFIG_PATH").ok().map(PathBuf::from))
    else {
        return;
    };
    if !path.exists() {
        eprintln!(
            "UDB config overlay skipped: {} does not exist",
            path.display()
        );
        return;
    }
    let content = match fs::read_to_string(&path) {
        Ok(content) => content,
        Err(err) => {
            eprintln!(
                "UDB config overlay skipped: could not read {}: {err}",
                path.display()
            );
            return;
        }
    };
    let yaml: YamlValue = match parse_config_overlay_value(&path, &content) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("UDB config overlay skipped: {}: {err}", path.display());
            return;
        }
    };

    set_env_from_yaml(&yaml, &["database", "pg_dsn"], &["UDB_PG_DSN"]);
    set_env_from_yaml(&yaml, &["database", "url"], &["UDB_PG_DSN"]);
    set_env_from_yaml(&yaml, &["database", "cache_dsn"], &["UDB_REDIS_DSN"]);
    set_env_from_yaml(&yaml, &["database", "redis_dsn"], &["UDB_REDIS_DSN"]);
    set_env_from_yaml(&yaml, &["database", "vector_dsn"], &["UDB_QDRANT_URL"]);
    set_env_from_yaml(&yaml, &["database", "qdrant_url"], &["UDB_QDRANT_URL"]);
    set_env_from_yaml(
        &yaml,
        &["database", "qdrant_api_key"],
        &["UDB_QDRANT_API_KEY"],
    );
    set_env_from_yaml(&yaml, &["database", "object_dsn"], &["UDB_MINIO_ENDPOINT"]);
    set_env_from_yaml(&yaml, &["database", "s3_endpoint"], &["UDB_MINIO_ENDPOINT"]);
    set_env_from_yaml(
        &yaml,
        &["database", "aws_access_key_id"],
        &["UDB_MINIO_ACCESS_KEY", "AWS_ACCESS_KEY_ID"],
    );
    set_env_from_yaml(
        &yaml,
        &["database", "aws_secret_access_key"],
        &["UDB_MINIO_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"],
    );
    set_env_from_yaml(
        &yaml,
        &["database", "aws_region"],
        &["UDB_MINIO_REGION", "AWS_REGION"],
    );
    set_env_from_yaml(
        &yaml,
        &["database", "kafka_brokers"],
        &["UDB_KAFKA_BROKERS"],
    );

    set_env_from_yaml(&yaml, &["system", "schema"], &["UDB_CDC_SYSTEM_SCHEMA"]);
    set_env_from_yaml(&yaml, &["system", "abac_schema"], &["UDB_ABAC_SCHEMA"]);
    set_env_from_yaml(&yaml, &["system", "abac_table"], &["UDB_ABAC_TABLE"]);
    set_env_from_yaml(
        &yaml,
        &["system", "abac_default_allow"],
        &["UDB_ABAC_DEFAULT_ALLOW"],
    );
    set_env_from_yaml(
        &yaml,
        &["saga", "recovery_enabled"],
        &["UDB_SAGA_RECOVERY_ENABLED"],
    );
    set_env_from_yaml(
        &yaml,
        &["saga", "recovery_interval_seconds"],
        &["UDB_SAGA_RECOVERY_INTERVAL_SECONDS"],
    );
    set_env_from_yaml(
        &yaml,
        &["saga", "stale_threshold_seconds"],
        &["UDB_SAGA_STALE_THRESHOLD_SECONDS"],
    );

    if let Some(addr) = yaml_string(&yaml, &["server", "grpc_addr"]) {
        set_env_if_absent("UDB_GRPC_ADDR", &addr);
    } else if let Some(port) = yaml_string(&yaml, &["server", "port"]) {
        let host = yaml_string(&yaml, &["server", "host"]).unwrap_or_else(|| "0.0.0.0".to_string());
        set_env_if_absent("UDB_GRPC_ADDR", &format!("{host}:{port}"));
    }
    if let Some(addr) = yaml_string(&yaml, &["server", "metrics_addr"]) {
        set_env_if_absent("UDB_METRICS_ADDR", &addr);
    } else if let Some(port) = yaml_string(&yaml, &["server", "metrics_port"]) {
        let host = yaml_string(&yaml, &["server", "metrics_host"])
            .unwrap_or_else(|| "0.0.0.0".to_string());
        set_env_if_absent("UDB_METRICS_ADDR", &format!("{host}:{port}"));
    }

    if cli_path.is_some() {
        set_env("UDB_CONFIG_PATH", &path.display().to_string());
    } else {
        set_env_if_absent("UDB_CONFIG_PATH", &path.display().to_string());
    }
}

pub(crate) fn parse_config_overlay_value(
    path: &std::path::Path,
    content: &str,
) -> Result<YamlValue, String> {
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    if ext.eq_ignore_ascii_case("toml") {
        let value = toml::from_str::<toml::Value>(content)
            .map_err(|err| format!("is not valid TOML: {err}"))?;
        return serde_yaml::to_value(value)
            .map_err(|err| format!("failed to normalize TOML overlay: {err}"));
    }
    serde_yaml::from_str(content).map_err(|err| format!("is not valid YAML/JSON: {err}"))
}

pub(crate) fn cli_config_path(args: &[String]) -> Option<PathBuf> {
    args.windows(2)
        .find(|pair| pair.first().map(String::as_str) == Some("--config"))
        .and_then(|pair| pair.get(1))
        .map(PathBuf::from)
}

pub(crate) fn set_env_from_yaml(root: &YamlValue, path: &[&str], keys: &[&str]) {
    let Some(value) = yaml_string(root, path) else {
        return;
    };
    for key in keys {
        set_env_if_absent(key, &value);
    }
}

pub(crate) fn yaml_string(root: &YamlValue, path: &[&str]) -> Option<String> {
    let mut current = root;
    for segment in path {
        current = current.get(*segment)?;
    }
    let raw = match current {
        YamlValue::String(value) => value.clone(),
        YamlValue::Number(value) => value.to_string(),
        YamlValue::Bool(value) => value.to_string(),
        _ => return None,
    };
    let expanded = expand_env_template(&raw);
    if expanded.trim().is_empty() {
        None
    } else {
        Some(expanded)
    }
}

pub(crate) fn expand_env_template(input: &str) -> String {
    let mut out = String::new();
    let mut rest = input;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            out.push_str(&rest[start..]);
            return out;
        };
        let expr = &after[..end];
        let (name, fallback) = expr
            .split_once(":-")
            .map(|(name, fallback)| (name.trim(), Some(fallback)))
            .unwrap_or_else(|| (expr.trim(), None));
        let value = env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| fallback.map(ToString::to_string))
            .unwrap_or_default();
        out.push_str(&value);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    out
}

pub(crate) fn set_env_if_absent(key: &str, value: &str) {
    if env::var(key).is_ok() || value.trim().is_empty() {
        return;
    }
    set_env(key, value);
}

pub(crate) fn set_env(key: &str, value: &str) {
    if value.trim().is_empty() {
        return;
    }
    // Safety: called during single-threaded CLI startup before the Tokio runtime
    // or service background tasks are created.
    #[allow(unused_unsafe)]
    unsafe {
        env::set_var(key, value);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DoctorOutputMode {
    /// Emit pretty-printed JSON (default).
    Json,
    /// Emit human-readable ASCII table.
    Human,
}
