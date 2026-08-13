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
//! This is a TABOO, not a ratchet. The pattern is banned outright and the per-file
//! allowance list is GONE — all 77 recorded call sites were migrated. Every handler now
//! states its intent in the call it makes: `project_scoped_native_service_context` when
//! the entity really is project-scoped, `tenant_only_native_service_context` when it is
//! not. The project a query is scoped to is never again decided by an argument that
//! reads as "none". Run `cargo test` to check.
//!
//! Keeping the allowance list would have kept the defect: a ratchet only stops the 78th
//! site, it never fixes the 77 that are already there.

use std::path::{Path, PathBuf};

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
        // Both cures END WITH this identifier, so a bare substring match fires
        // inside `tenant_only_native_service_context(&metadata, "")` — where the
        // trailing empty literal is the TENANT argument, not a project. Require a
        // real word boundary so only the three-argument function is inspected.
        .filter(|(start, _)| {
            *start == 0
                || !source[..*start]
                    .chars()
                    .next_back()
                    .is_some_and(|c| c.is_alphanumeric() || c == '_')
        })
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

    let mut offenders = Vec::new();
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
            offenders.push(format!("  {rel}: {count} call site(s)"));
        }
    }

    assert!(
        offenders.is_empty(),
        "`native_service_context(.., \"\")` is BANNED; found it in:\n{}\n\nUse \
         `project_scoped_native_service_context(&metadata, &tenant)` when the entity is \
         project-scoped, or `tenant_only_native_service_context(&metadata, &tenant)` when \
         it is not. There is no allowance list to add to.\n\nWhy this matters: the empty \
         project argument falls back to the x-udb-project-id REQUEST HEADER, and the \
         native entity layer applies the result as a query predicate. On a UUID-typed \
         project_id column a human project code fails the bind (INVALID_ARGUMENT); on a \
         textual one it silently filters out rows the caller owns (NOT_FOUND); and for a \
         bearer token carrying no project claim the tower's header/claim equality check \
         does not fire, so the header alone chooses which project is read. Unit tests \
         send no project header, so none of it shows up until live multi-project traffic \
         hits it.",
        offenders.join("\n")
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
    // Both cures END WITH the banned identifier as a substring, which is how the
    // detector finds calls at all — so prove neither trips it. If this ever fails,
    // the ban would fire on the very call sites it tells people to migrate to.
    assert_eq!(
        count_empty_project_contexts(
            r#"let context = project_scoped_native_service_context(&metadata, &tenant_id);"#
        ),
        0
    );
    assert_eq!(
        count_empty_project_contexts(
            "let context = project_scoped_native_service_context(\n    &metadata,\n    &tenant_id,\n);"
        ),
        0
    );
    // REGRESSION: a two-argument cure whose LAST argument is an empty literal is
    // passing an empty TENANT, not a project. A bare substring match flagged it
    // as the banned three-argument call and sent people to "fix" a cure with the
    // cure. Only a real word boundary distinguishes them.
    assert_eq!(
        count_empty_project_contexts(
            r#"let context = tenant_only_native_service_context(&metadata, "");"#
        ),
        0
    );
    assert_eq!(
        count_empty_project_contexts(
            r#"let context = project_scoped_native_service_context(&metadata, "");"#
        ),
        0
    );
}

/// BLUNDER GUARD 1: a file that CONSUMES `context.project_id` must not build a
/// tenant-only context.
///
/// I flipped 51 call sites to `tenant_only_native_service_context` using the
/// wrong test — "does the ENTITY declare a project column?" — when the question
/// that decides it is "does anything here CONSUME the project?". Four services
/// did: `search_service` resolves its source table through
/// `resolve_source_tenant_column(&context.project_id, ..)`, and vault/backup/lock
/// stamp outbox events with it, where an EMPTY stamp is broader than a real one
/// (the CDC scope check lets an empty event project through to any
/// project-scoped subscriber). Emptying the project there was a regression
/// wearing the shape of a fix.
#[test]
fn a_file_consuming_the_project_does_not_build_a_tenant_only_context() {
    let root = service_dir();
    let mut files = Vec::new();
    rust_sources(&root, &mut files);
    let mut problems = Vec::new();
    for file in &files {
        let Ok(source) = std::fs::read_to_string(file) else {
            continue;
        };
        let rel = file
            .strip_prefix(&root)
            .unwrap_or(file)
            .to_string_lossy()
            .replace('\', "/");
        if rel == "native_helpers.rs" || rel == "native_context_posture_tests.rs" {
            continue;
        }
        // Only count a REAL consumer: `context.project_id` read somewhere in the
        // file, not merely the identifier appearing in a comment.
        let consumes = source.contains("context.project_id") || source.contains("ctx.project_id");
        let tenant_only = source.contains("tenant_only_native_service_context(");
        if consumes && tenant_only {
            problems.push(format!(
                "  {rel}: reads context.project_id AND builds a tenant-only context — \
                 the project it consumes will be empty"
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "tenant-only context in a file that consumes the project:\n{}\n\nUse \
         `project_scoped_native_service_context` when the service consumes the project for \
         routing or event stamping, even if the ENTITY carries no project column — the query \
         predicate is inert in that case, but the routing and the event stamp are not.",
        problems.join("\n")
    );
}

/// BLUNDER GUARD 2: a startup guard that REFUSES must name its own way out.
///
/// The unappliable-backend-delta guard shipped as a deadlock: the diff is
/// computed against the STORED manifest, which only advances after a successful
/// start, so reconciling the store by hand never cleared the condition and the
/// broker refused forever. Worse, its message advised exactly that impossible
/// reconciliation. A refusal an operator cannot clear is not fail-closed, it is
/// a brick.
#[test]
fn the_unappliable_backend_delta_refusal_names_its_recovery_path() {
    let lifecycle = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/control/lifecycle.rs");
    let source = std::fs::read_to_string(&lifecycle).expect("read lifecycle.rs");
    assert!(
        source.contains("backend_delta_unappliable"),
        "the unappliable-backend-delta guard is gone; delete this test with it"
    );
    assert!(
        source.contains("ACK_MANUAL_BACKEND_RECONCILIATION_ENV"),
        "the refusal must reference the acknowledgement variable that clears it, so the \
         operator is told how to recover instead of being left to revert the proto change"
    );
    assert!(
        source.contains("UDB_ACK_MANUAL_BACKEND_RECONCILIATION"),
        "the acknowledgement variable must be named in-source so it is greppable from an \
         error message seen in production"
    );
}
