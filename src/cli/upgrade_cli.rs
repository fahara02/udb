//! `udb upgrade --check --from X --to Y [--repo <dir>] [--dsn <dsn>]`: list the
//! caller-visible breaking changes between two versions and find the ones that
//! touch this project. Each `### Breaking for callers` entry in CHANGELOG.md
//! (embedded in the binary) carries detectors and a fix:
//!
//! ```text
//! - **Upsert no longer fills missing columns.** …
//!   - detect: code: .Upsert(
//!   - detect: sql: SELECT COUNT(*) FROM app.orders WHERE status IS NULL
//!   - detect: file: .udb/native-services.json
//!   - detect: env: UDB_SOME_SETTING
//!   - fix: send whole rows to Upsert, or use Update for partial changes.
//! ```
//!
//! `code` is a literal substring searched in the repo's source files; `sql` is a
//! read-only probe whose first column counts affected rows; `file` is a relative
//! path found anywhere under the repo; `env` is a variable set in this process's
//! environment or assigned in a `.env*` file under the repo.

use super::*;

const CHANGELOG: &str = include_str!("../../CHANGELOG.md");

/// Directories never searched for code detectors.
const SKIP_DIRS: &[&str] = &[
    ".git",
    "node_modules",
    "target",
    "vendor",
    "dist",
    "build",
    ".venv",
    "__pycache__",
];

/// Source file extensions searched for code detectors.
const SOURCE_EXTENSIONS: &[&str] = &[
    "go", "rs", "ts", "tsx", "js", "mjs", "py", "php", "java", "cs", "kt", "proto", "sql", "yaml",
    "yml", "json", "toml", "sh", "bash", "ps1", "cmd", "bat",
];

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) enum Detector {
    Code(String),
    Sql(String),
    File(String),
    Env(String),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct BreakingEntry {
    pub(crate) version: String,
    pub(crate) summary: String,
    pub(crate) detectors: Vec<Detector>,
    pub(crate) fix: String,
}

fn parse_version(text: &str) -> Option<(u64, u64, u64)> {
    let mut parts = text.trim().trim_start_matches('v').split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts
        .next()?
        .split(|c: char| !c.is_ascii_digit())
        .next()?
        .parse()
        .ok()?;
    Some((major, minor, patch))
}

fn strip_ticks(text: &str) -> String {
    let text = text.trim();
    text.strip_prefix('`')
        .and_then(|inner| inner.strip_suffix('`'))
        .unwrap_or(text)
        .to_string()
}

/// Every breaking-for-callers entry of every release in `changelog`.
pub(crate) fn parse_breaking_entries(changelog: &str) -> Vec<BreakingEntry> {
    let mut entries = Vec::new();
    let mut version = String::new();
    let mut in_breaking = false;
    let mut current: Option<BreakingEntry> = None;
    for line in changelog.lines() {
        if let Some(heading) = line.strip_prefix("## ") {
            entries.extend(current.take());
            in_breaking = false;
            version = heading
                .trim_start_matches('[')
                .split(|c| c == ']' || c == ' ')
                .next()
                .unwrap_or_default()
                .to_string();
            continue;
        }
        if let Some(sub) = line.strip_prefix("### ") {
            entries.extend(current.take());
            in_breaking = sub.trim().eq_ignore_ascii_case("Breaking for callers");
            continue;
        }
        if !in_breaking {
            continue;
        }
        if let Some(item) = line.strip_prefix("- ") {
            entries.extend(current.take());
            current = Some(BreakingEntry {
                version: version.clone(),
                summary: item.trim().to_string(),
                detectors: Vec::new(),
                fix: String::new(),
            });
            continue;
        }
        let Some(entry) = current.as_mut() else {
            continue;
        };
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("- detect:") {
            let rest = rest.trim();
            if let Some(pattern) = rest.strip_prefix("code:") {
                entry.detectors.push(Detector::Code(strip_ticks(pattern)));
            } else if let Some(query) = rest.strip_prefix("sql:") {
                entry.detectors.push(Detector::Sql(strip_ticks(query)));
            } else if let Some(path) = rest.strip_prefix("file:") {
                entry.detectors.push(Detector::File(strip_ticks(path)));
            } else if let Some(name) = rest.strip_prefix("env:") {
                entry.detectors.push(Detector::Env(strip_ticks(name)));
            }
        } else if let Some(fix) = trimmed.strip_prefix("- fix:") {
            entry.fix = fix.trim().to_string();
        } else if !trimmed.is_empty() && entry.detectors.is_empty() && entry.fix.is_empty() {
            entry.summary.push(' ');
            entry.summary.push_str(trimmed);
        }
    }
    entries.extend(current.take());
    entries
        .into_iter()
        .filter(|entry| {
            !entry.summary.eq_ignore_ascii_case("none.")
                && !entry.summary.eq_ignore_ascii_case("none")
        })
        .collect()
}

/// Entries released after `from` up to and including `to`.
pub(crate) fn entries_between(
    entries: &[BreakingEntry],
    from: &str,
    to: &str,
) -> Result<Vec<BreakingEntry>, String> {
    let from = parse_version(from).ok_or_else(|| format!("--from '{from}' is not a version"))?;
    let to = parse_version(to).ok_or_else(|| format!("--to '{to}' is not a version"))?;
    Ok(entries
        .iter()
        .filter(|entry| {
            parse_version(&entry.version).is_some_and(|version| version > from && version <= to)
        })
        .cloned()
        .collect())
}

fn search_code(root: &std::path::Path, needle: &str, hits: &mut Vec<String>, limit: usize) {
    let Ok(read) = fs::read_dir(root) else {
        return;
    };
    for item in read.flatten() {
        if hits.len() >= limit {
            return;
        }
        let path = item.path();
        let name = item.file_name().to_string_lossy().to_string();
        if path.is_dir() {
            if !SKIP_DIRS.contains(&name.as_str()) {
                search_code(&path, needle, hits, limit);
            }
            continue;
        }
        let searchable = path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| SOURCE_EXTENSIONS.contains(&ext));
        if !searchable {
            continue;
        }
        let Ok(text) = fs::read_to_string(&path) else {
            continue;
        };
        for (number, line) in text.lines().enumerate() {
            if line.contains(needle) {
                hits.push(format!("{}:{}", path.display(), number + 1));
                if hits.len() >= limit {
                    return;
                }
            }
        }
    }
}

/// Every file under `root` whose path ends with the relative path `suffix`
/// (so `.udb/native-services.json` is found in nested app directories too).
fn search_files(root: &std::path::Path, suffix: &str, hits: &mut Vec<String>, limit: usize) {
    let Ok(read) = fs::read_dir(root) else {
        return;
    };
    let suffix: Vec<&str> = suffix
        .split(['/', '\\'])
        .filter(|part| !part.is_empty())
        .collect();
    for item in read.flatten() {
        if hits.len() >= limit {
            return;
        }
        let path = item.path();
        let name = item.file_name().to_string_lossy().to_string();
        if path.is_dir() {
            if !SKIP_DIRS.contains(&name.as_str()) {
                search_files(&path, &suffix.join("/"), hits, limit);
            }
            continue;
        }
        let parts: Vec<String> = path
            .components()
            .map(|part| part.as_os_str().to_string_lossy().to_string())
            .collect();
        if !suffix.is_empty()
            && parts.len() >= suffix.len()
            && parts[parts.len() - suffix.len()..]
                .iter()
                .zip(&suffix)
                .all(|(have, want)| have == want)
        {
            hits.push(path.display().to_string());
        }
    }
}

/// Where `name` is set: this process's environment, or an assignment in a
/// `.env*` file under `root` (`NAME=…` or `export NAME=…`).
fn search_env(root: &std::path::Path, name: &str, hits: &mut Vec<String>, limit: usize) {
    fn walk(dir: &std::path::Path, name: &str, hits: &mut Vec<String>, limit: usize) {
        let Ok(read) = fs::read_dir(dir) else {
            return;
        };
        for item in read.flatten() {
            if hits.len() >= limit {
                return;
            }
            let path = item.path();
            let file = item.file_name().to_string_lossy().to_string();
            if path.is_dir() {
                if !SKIP_DIRS.contains(&file.as_str()) {
                    walk(&path, name, hits, limit);
                }
                continue;
            }
            if !file.starts_with(".env") {
                continue;
            }
            let Ok(text) = fs::read_to_string(&path) else {
                continue;
            };
            for (number, line) in text.lines().enumerate() {
                let line = line.trim_start();
                let line = line.strip_prefix("export ").unwrap_or(line).trim_start();
                if line
                    .strip_prefix(name)
                    .is_some_and(|rest| rest.trim_start().starts_with('='))
                {
                    hits.push(format!("{}:{}", path.display(), number + 1));
                }
            }
        }
    }
    if env::var(name).is_ok_and(|value| !value.trim().is_empty()) {
        hits.push(format!("set in this process's environment ({name})"));
    }
    walk(root, name, hits, limit);
}

pub(crate) fn run_upgrade_command(
    check: bool,
    from: String,
    to: String,
    repo: String,
    dsn: String,
) -> i32 {
    if !check {
        eprintln!("udb upgrade: only --check is supported (it reports, it changes nothing)");
        return 2;
    }
    let to = if to.trim().is_empty() {
        env!("CARGO_PKG_VERSION").to_string()
    } else {
        to
    };
    if from.trim().is_empty() {
        eprintln!("udb upgrade --check: pass --from <the version you run now>");
        return 2;
    }
    let entries = match entries_between(&parse_breaking_entries(CHANGELOG), &from, &to) {
        Ok(entries) => entries,
        Err(err) => {
            eprintln!("udb upgrade --check: {err}");
            return 2;
        }
    };
    let repo = if repo.trim().is_empty() {
        ".".to_string()
    } else {
        repo
    };
    let dsn = if dsn.trim().is_empty() {
        env::var("UDB_PG_DSN")
            .or_else(|_| env::var("DATABASE_URL"))
            .unwrap_or_default()
    } else {
        dsn
    };
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("udb upgrade --check: failed to create tokio runtime: {err}");
            return 1;
        }
    };
    let pool = if dsn.trim().is_empty() {
        None
    } else {
        runtime.block_on(async {
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(std::time::Duration::from_secs(10))
                .connect(&dsn)
                .await
                .map_err(|err| {
                    eprintln!("udb upgrade --check: SQL probes skipped, cannot connect: {err}")
                })
                .ok()
        })
    };
    let mut affected = 0usize;
    let mut report = Vec::new();
    for entry in &entries {
        let mut findings = Vec::new();
        let mut unchecked = Vec::new();
        for detector in &entry.detectors {
            match detector {
                Detector::Code(needle) => {
                    let mut hits = Vec::new();
                    search_code(std::path::Path::new(&repo), needle, &mut hits, 50);
                    findings.extend(
                        hits.into_iter()
                            .map(|hit| format!("code `{needle}` at {hit}")),
                    );
                }
                Detector::File(suffix) => {
                    let mut hits = Vec::new();
                    search_files(std::path::Path::new(&repo), suffix, &mut hits, 50);
                    findings.extend(
                        hits.into_iter()
                            .map(|hit| format!("file `{suffix}` at {hit}")),
                    );
                }
                Detector::Env(name) => {
                    let mut hits = Vec::new();
                    search_env(std::path::Path::new(&repo), name, &mut hits, 50);
                    findings.extend(hits.into_iter().map(|hit| format!("env `{name}` {hit}")));
                }
                Detector::Sql(query) => match pool.as_ref() {
                    None => unchecked.push(format!("sql probe needs --dsn: {query}")),
                    Some(pool) => {
                        // Read-only: every probe runs in a transaction that is
                        // rolled back, under a read-only setting.
                        let count = runtime.block_on(async {
                            let mut tx = pool.begin().await?;
                            sqlx::query("SET TRANSACTION READ ONLY")
                                .execute(&mut *tx)
                                .await?;
                            let count: i64 = sqlx::query_scalar(query).fetch_one(&mut *tx).await?;
                            tx.rollback().await?;
                            Ok::<i64, sqlx::Error>(count)
                        });
                        match count {
                            Ok(0) => {}
                            Ok(count) => {
                                findings.push(format!("sql probe matched {count} row(s): {query}"))
                            }
                            Err(err) => {
                                unchecked.push(format!("sql probe failed ({err}): {query}"))
                            }
                        }
                    }
                },
            }
        }
        let hit = !findings.is_empty();
        if hit {
            affected += 1;
        }
        report.push(serde_json::json!({
            "version": entry.version,
            "change": entry.summary,
            "affects_you": hit,
            "findings": findings,
            "not_checked": unchecked,
            "fix": entry.fix,
        }));
    }
    eprintln!(
        "upgrade --check {from} -> {to}: {} breaking change(s), {affected} affect this project",
        entries.len()
    );
    output_json(
        &serde_json::json!({
            "from": from,
            "to": to,
            "breaking_changes": entries.len(),
            "affected": affected,
            "changes": report,
        }),
        "upgrade check",
    );
    if affected > 0 { 1 } else { 0 }
}

#[cfg(test)]
mod upgrade_cli_tests {
    use super::{
        CHANGELOG, Detector, entries_between, parse_breaking_entries, search_env, search_files,
    };

    const SAMPLE: &str = "# Changelog\n\n## [0.5.30] - 2026-10-09\n\n### Breaking for callers\n\n- None.\n\n## [0.5.29] - 2026-10-08\n\n### Added\n\n- something\n\n### Breaking for callers\n\n- **Upsert no longer fills missing columns.**\n  Partial rows fail.\n  - detect: code: `.Upsert(`\n  - detect: sql: SELECT 0\n  - fix: send whole rows, or use Update.\n\n## [0.5.28] - 2026-10-07\n\n### Breaking for callers\n\n- **old change**\n  - fix: nothing\n";

    #[test]
    fn parses_entries_with_detectors_and_skips_none() {
        let entries = parse_breaking_entries(SAMPLE);
        assert_eq!(entries.len(), 2);
        let first = &entries[0];
        assert_eq!(first.version, "0.5.29");
        assert!(first.summary.contains("Partial rows fail."));
        assert_eq!(
            first.detectors,
            vec![
                Detector::Code(".Upsert(".into()),
                Detector::Sql("SELECT 0".into())
            ]
        );
        assert_eq!(first.fix, "send whole rows, or use Update.");
    }

    #[test]
    fn selects_the_version_window() {
        let entries = parse_breaking_entries(SAMPLE);
        let window = entries_between(&entries, "0.5.28", "0.5.30").unwrap();
        assert_eq!(window.len(), 1);
        assert_eq!(window[0].version, "0.5.29");
        assert!(
            entries_between(&entries, "0.5.29", "0.5.30")
                .unwrap()
                .is_empty()
        );
        assert!(entries_between(&entries, "x", "0.5.30").is_err());
    }

    #[test]
    fn parses_file_and_env_detectors() {
        let sample = "## [0.6.0] - Unreleased\n\n### Breaking for callers\n\n- **moved**\n  - detect: file: `.udb/native-services.json`\n  - detect: env: `UDB_X`\n  - fix: move it.\n";
        let entries = parse_breaking_entries(sample);
        assert_eq!(
            entries[0].detectors,
            vec![
                Detector::File(".udb/native-services.json".into()),
                Detector::Env("UDB_X".into())
            ]
        );
    }

    #[test]
    fn file_and_env_detectors_find_nested_hits() {
        let root = std::env::temp_dir().join(format!("udb-upgrade-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("apps/web/.udb")).expect("mkdir");
        std::fs::write(root.join("apps/web/.udb/native-services.json"), "{}").expect("write");
        std::fs::write(
            root.join("apps/web/.env.production"),
            "A=1\nexport UDB_UPGRADE_TEST_FLAG=true\n",
        )
        .expect("write env");
        let mut files = Vec::new();
        search_files(&root, ".udb/native-services.json", &mut files, 10);
        assert_eq!(files.len(), 1, "{files:?}");
        let mut envs = Vec::new();
        search_env(&root, "UDB_UPGRADE_TEST_FLAG", &mut envs, 10);
        assert_eq!(envs.len(), 1, "{envs:?}");
        assert!(envs[0].ends_with(":2"), "{envs:?}");
        let mut none = Vec::new();
        search_env(&root, "UDB_UPGRADE_TEST", &mut none, 10);
        assert!(
            none.is_empty(),
            "a prefix of another name is not a hit: {none:?}"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    /// Every 0.6.0 removal ships a breaking entry with a fix; the detectable
    /// ones carry a detector.
    #[test]
    fn the_0_6_0_removals_carry_fixes_and_detectors() {
        let entries = entries_between(&parse_breaking_entries(CHANGELOG), "0.5.99", "0.6.0")
            .expect("version window");
        assert!(entries.len() >= 9, "{}", entries.len());
        assert!(entries.iter().all(|entry| !entry.fix.is_empty()));
        let detectors: Vec<&Detector> = entries.iter().flat_map(|e| &e.detectors).collect();
        for expected in [
            Detector::Code("RawAPIKey".into()),
            Detector::File(".udb/native-services.json".into()),
            Detector::Env("UDB_ALLOW_LEGACY_PUT_POLICY".into()),
            Detector::Code("authz seed".into()),
            Detector::Code("policy-seed".into()),
            Detector::Code("\"from_label\"".into()),
        ] {
            assert!(detectors.contains(&&expected), "missing {expected:?}");
        }
    }
}
