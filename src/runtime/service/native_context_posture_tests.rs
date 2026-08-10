//! Build-time guard for a SILENT breakage class: a native handler that builds its
//! entity context with `native_service_context(&metadata, &tenant, "")`.
//!
//! That helper falls back to the `x-udb-project-id` request header when the project
//! argument is empty, and the native entity layer then applies the resulting project
//! as an extra query predicate. The damage is invisible in unit tests because they
//! send no project header:
//!
//!   * against a UUID-typed `project_id` a human project code (`"default"`) fails the
//!     bind outright — `INVALID_ARGUMENT: uuid params must be UUID strings`;
//!   * against a textual `project_id` nothing errors at all — the read simply filters
//!     out rows the caller owns and returns `NOT_FOUND` for real data.
//!
//! Three shipped read paths (AssetService GetAsset/ListAssets, NotificationService
//! GetNotification) carried this defect into a release; only the post-release live SDK
//! benchmark caught it. The cure is `tenant_only_native_service_context`, which
//! deliberately does not consult the header.
//!
//! This is a RATCHET, not a ban: the remaining call sites are recorded per file below.
//! Adding one to a file fails this test; removing one (by moving to the tenant-only
//! context, or by passing a real project) requires lowering its number here. Either way
//! the change is deliberate and reviewed instead of silent. Run `cargo test` to check.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Per-file count of `native_service_context(&metadata, &<tenant>, "")` call sites,
/// relative to `src/runtime/service/`. A file absent from this map must have ZERO.
///
/// Lower a number when a handler moves to `tenant_only_native_service_context`; never
/// raise one without confirming the entity really is project-scoped AND that callers
/// always send a project id the column's type accepts.
fn allowed_empty_project_contexts() -> BTreeMap<&'static str, usize> {
    BTreeMap::from([
        ("analytics_service/handlers.rs", 4),
        ("asset_service/handlers.rs", 2),
        ("backup_service/export.rs", 1),
        ("backup_service/handlers.rs", 6),
        ("backup_service/import.rs", 2),
        ("cache_service/events.rs", 1),
        ("embedding_service/documents.rs", 2),
        ("embedding_service/handlers.rs", 4),
        ("embedding_service/jobs.rs", 2),
        ("embedding_service/registry.rs", 5),
        ("embedding_service/reports.rs", 2),
        ("embedding_service/retrieval.rs", 2),
        ("lock_service/handlers.rs", 5),
        ("metering_service/handlers.rs", 1),
        ("notification_service/handlers.rs", 5),
        ("search_service/handlers.rs", 5),
        ("tenant_service/handlers.rs", 3),
        ("vault_service/handlers.rs", 19),
        ("webrtc_service/mod.rs", 6),
    ])
}

fn service_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src/runtime/service")
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// Count `native_service_context(<anything>, "")` occurrences: the empty-string project
/// argument is the whole tell, whatever the tenant expression is named.
fn count_empty_project_contexts(source: &str) -> usize {
    source
        .match_indices("native_service_context(")
        .filter(|(start, _)| {
            let tail = &source[*start..];
            // The call ends at the first ');' — an empty project literal immediately
            // before it is the pattern under guard. Handles both one-line calls and
            // rustfmt-wrapped ones.
            let end = match tail.find(");") {
                Some(end) => end,
                None => return false,
            };
            let args = &tail[..end];
            let trimmed = args.trim_end();
            trimmed.ends_with("\"\"") || trimmed.ends_with("\"\",")
        })
        .count()
}

#[test]
fn native_reads_do_not_silently_inherit_the_header_project() {
    let root = service_dir();
    let mut files = Vec::new();
    rust_sources(&root, &mut files);
    assert!(
        !files.is_empty(),
        "found no service sources under {}",
        root.display()
    );

    let allowed = allowed_empty_project_contexts();
    let mut actual: BTreeMap<String, usize> = BTreeMap::new();
    for file in &files {
        let source = match std::fs::read_to_string(file) {
            Ok(source) => source,
            Err(_) => continue,
        };
        let rel = file
            .strip_prefix(&root)
            .unwrap_or(file)
            .to_string_lossy()
            .replace('\\', "/");
        // `native_helpers.rs` DEFINES the helper (and unit-tests it), and this file
        // quotes the pattern in its own detector self-tests. Neither is a handler.
        if rel == "native_helpers.rs" || rel == "native_context_posture_tests.rs" {
            continue;
        }
        let count = count_empty_project_contexts(&source);
        if count > 0 {
            actual.insert(rel, count);
        }
    }

    let mut problems = Vec::new();
    for (file, count) in &actual {
        let budget = allowed.get(file.as_str()).copied().unwrap_or(0);
        if *count > budget {
            problems.push(format!(
                "  {file}: {count} call site(s), budget {budget} — a NEW \
                 `native_service_context(.., \"\")` was added. If the entity is not \
                 project-scoped use `tenant_only_native_service_context`; otherwise pass \
                 a real project id and raise the budget deliberately."
            ));
        }
    }
    for (file, budget) in &allowed {
        let count = actual.get(*file).copied().unwrap_or(0);
        if count < *budget {
            problems.push(format!(
                "  {file}: {count} call site(s), budget {budget} — the ratchet improved. \
                 Lower the budget in allowed_empty_project_contexts() to lock the gain in."
            ));
        }
    }

    assert!(
        problems.is_empty(),
        "native-service context posture drifted:\n{}\n\nWhy this matters: \
         `native_service_context` falls back to the x-udb-project-id header when the \
         project argument is empty, and the native entity layer applies that project as \
         a query predicate. On a UUID-typed project_id column a human project code fails \
         the bind (INVALID_ARGUMENT); on a textual one it silently filters out rows the \
         caller owns (NOT_FOUND). Unit tests send no project header, so neither shows up \
         until live multi-project traffic hits it.",
        problems.join("\n")
    );
}

#[test]
fn empty_project_context_detector_matches_the_real_patterns() {
    // One-line call, the shape that shipped the AssetService/Notification defects.
    assert_eq!(
        count_empty_project_contexts(
            r#"let context = native_service_context(&metadata, &req.tenant_id, "");"#
        ),
        1
    );
    // rustfmt-wrapped across lines must still be caught.
    assert_eq!(
        count_empty_project_contexts(
            "let context = native_service_context(\n    &metadata,\n    &scoped_tenant,\n    \"\",\n);"
        ),
        1
    );
    // A real project argument is fine — that is the intended, explicit use.
    assert_eq!(
        count_empty_project_contexts(
            r#"let context = native_service_context(&metadata, &req.tenant_id, &req.project_id);"#
        ),
        0
    );
    // The tenant-only helper is the cure and must never be flagged.
    assert_eq!(
        count_empty_project_contexts(
            r#"let context = tenant_only_native_service_context(&metadata, &tenant);"#
        ),
        0
    );
}
