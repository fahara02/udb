//! `udb check --code <dir>` uses the Go standard library's AST, including its
//! lexical object bindings. The analyzer is shipped inside this binary; the
//! project's Go installation compiles/runs only that analyzer, never app code.
//! It handles store constructors, aliases, typed parameters and struct fields,
//! and fails rather than accepting an unreadable or syntactically invalid file.

use std::path::{Path, PathBuf};

const GO_ANALYZER: &str = include_str!("go_code_scan.go");

/// One resolved store call site.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CallSite {
    pub(crate) file: String,
    pub(crate) line: usize,
    /// Go type short name; the caller resolves it against its proto manifest.
    pub(crate) entity: String,
    pub(crate) method: String,
    pub(crate) action: String,
    /// An explicit literal request override; None inherits the CLI's purpose.
    pub(crate) purpose: Option<String>,
    /// An explicit purpose is present but cannot be resolved statically.
    pub(crate) purpose_dynamic: bool,
}

/// Keep the embedded source in a content-addressed cache so `go run` can use
/// its build cache across CLI invocations. No project source or dependencies
/// are compiled, and toolchain/module downloads are explicitly disabled.
fn analyzer_source() -> Result<PathBuf, String> {
    use sha2::{Digest, Sha256};
    use std::io::Write;
    let digest = format!("{:x}", Sha256::digest(GO_ANALYZER.as_bytes()));
    let home = std::env::var_os("UDB_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .map(|path| PathBuf::from(path).join(".udb"))
        })
        .ok_or_else(|| "set UDB_HOME, HOME or USERPROFILE for the Go analyzer cache".to_string())?;
    let directory = home.join("code-scan").join(digest);
    std::fs::create_dir_all(&directory)
        .map_err(|err| format!("create Go analyzer cache: {err}"))?;
    let path = directory.join("analyzer.go");
    if std::fs::read_to_string(&path).ok().as_deref() != Some(GO_ANALYZER) {
        let temporary = directory.join(format!(".{}.go.tmp", uuid::Uuid::new_v4()));
        let install = (|| -> std::io::Result<()> {
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(GO_ANALYZER.as_bytes())?;
            file.sync_all()?;
            drop(file);
            std::fs::rename(&temporary, &path)
        })();
        let _ = std::fs::remove_file(&temporary);
        // Windows refuses a rename over an existing target. A concurrent CLI
        // invocation may have installed the same content while we wrote ours.
        if std::fs::read_to_string(&path).ok().as_deref() != Some(GO_ANALYZER) {
            return Err(format!(
                "install embedded Go analyzer: {}",
                install
                    .err()
                    .map(|err| err.to_string())
                    .unwrap_or_else(|| "cached source differs".into())
            ));
        }
    }
    // Absolute, not canonical: on Windows canonicalize yields the `\\?\` form,
    // which `go run` reads as an import path, and resolving through a file that
    // a concurrent installer just replaced lands in the volume's deleted area.
    std::path::absolute(&path).map_err(|err| format!("resolve Go analyzer cache: {err}"))
}

fn analyze(request: serde_json::Value) -> Result<Vec<CallSite>, String> {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let source = analyzer_source()?;
    let input =
        serde_json::to_vec(&request).map_err(|err| format!("Go analyzer request: {err}"))?;
    let mut child = Command::new("go")
        .arg("run")
        .arg(&source)
        .current_dir(source.parent().ok_or("Go analyzer cache has no parent")?)
        .env("GO111MODULE", "off")
        .env("GOENV", "off")
        .env("GOFLAGS", "")
        .env("GOWORK", "off")
        .env("GOTOOLCHAIN", "local")
        .env("GOPROXY", "off")
        .env("GOSUMDB", "off")
        .env("CGO_ENABLED", "0")
        .env_remove("GOOS")
        .env_remove("GOARCH")
        .env_remove("GOEXPERIMENT")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("Go AST analysis requires Go on PATH: {err}"))?;
    let sent = child
        .stdin
        .take()
        .ok_or_else(|| "Go analyzer stdin unavailable".to_string())
        .and_then(|mut stdin| stdin.write_all(&input).map_err(|err| err.to_string()));
    if let Err(err) = sent {
        let _ = child.kill();
        let _ = child.wait();
        return Err(format!("send source to Go analyzer: {err}"));
    }
    let output = child
        .wait_with_output()
        .map_err(|err| format!("Go AST analysis: {err}"))?;
    if !output.status.success() {
        return Err(format!(
            "Go AST analysis failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let mut sites: Vec<CallSite> = serde_json::from_slice(&output.stdout)
        .map_err(|err| format!("invalid Go analyzer response: {err}"))?;
    sites.sort_by(|a, b| {
        (&a.file, a.line, &a.entity, &a.method).cmp(&(&b.file, b.line, &b.entity, &b.method))
    });
    Ok(sites)
}

#[cfg(test)]
fn scan_go_source(file: &str, text: &str) -> Result<Vec<CallSite>, String> {
    analyze(serde_json::json!({"file": file, "source": text}))
}

/// Scan non-test Go files recursively; `./...` means the current directory tree.
pub(crate) fn scan_go_tree(root: &Path) -> Result<Vec<CallSite>, String> {
    let root = if root.file_name().is_some_and(|name| name == "...") {
        root.parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
    } else {
        root
    };
    // Absolute, not canonical: the Go analyzer cannot walk Windows' `\\?\` form.
    let root = std::path::absolute(root)
        .map_err(|err| format!("resolve code directory {}: {err}", root.display()))?;
    if !root.is_dir() {
        return Err(format!("{} is not a code directory", root.display()));
    }
    analyze(serde_json::json!({"root": root}))
}

#[cfg(test)]
mod code_scan_tests {
    use super::scan_go_source;

    #[test]
    fn missing_code_directory_is_an_error() {
        let path = std::env::temp_dir().join(format!("udb-code-scan-{}", uuid::Uuid::new_v4()));
        assert!(super::scan_go_tree(&path).is_err());
    }

    #[test]
    fn finds_store_calls_by_variable_and_inline() {
        let src = r#"
package svc
func run(u *udbclient.Udb) {
    notes := notesv1gen.NoteTable(u)
    tags := udbclient.TableOf[*tagsv1.Tag](u)
    n, _ := notes.Get(ctx, key)
    _ = notes.Patch(ctx, key, changes)
    tags.Delete(ctx, k)
    _, _ = NoteTable(u).Upsert(ctx, n)
    other.Get(ctx)
}
"#;
        let sites = scan_go_source("svc.go", src).expect("parse real Go source");
        let summary: Vec<(&str, &str)> = sites
            .iter()
            .map(|s| (s.entity.as_str(), s.action.as_str()))
            .collect();
        assert_eq!(
            summary,
            vec![
                ("Note", "Select"),
                ("Note", "Update"),
                ("Tag", "Delete"),
                ("Note", "Upsert")
            ]
        );
        assert_eq!(sites[0].line, 6);
    }

    #[test]
    fn ast_handles_multiline_aliases_and_shadowing_without_scanning_strings() {
        let src = r#"
package svc
func run(u *udbclient.Udb) {
    notes := NoteTable(
        u,
    )
    alias := notes
    _ = "notes.Delete(ctx, key)"
    /* notes.Upsert(ctx, row) */
    alias.
        Patch(ctx, key, changes)
    {
        notes := TagTable(u)
        notes.Delete(ctx, key)
    }
    notes.Get(ctx, key)
}
"#;
        let sites = scan_go_source("svc.go", src).expect("parse multiline source");
        let summary: Vec<(&str, &str)> = sites
            .iter()
            .map(|s| (s.entity.as_str(), s.action.as_str()))
            .collect();
        assert_eq!(
            summary,
            vec![("Note", "Update"), ("Tag", "Delete"), ("Note", "Select")]
        );
    }

    #[test]
    fn ast_resolves_typed_parameters_and_receiver_fields() {
        let src = r#"
package svc
type Service struct { notes *udbclient.Table[*notesv1.Note] }
func (s *Service) run(tags *udbclient.Table[*tagsv1.Tag]) {
    s.notes.Patch(ctx, key, changes)
    tags.Delete(ctx, key)
}
"#;
        let sites = scan_go_source("svc.go", src).expect("parse typed stores");
        let summary: Vec<(&str, &str)> = sites
            .iter()
            .map(|s| (s.entity.as_str(), s.action.as_str()))
            .collect();
        assert_eq!(summary, vec![("Note", "Update"), ("Tag", "Delete")]);
    }

    #[test]
    fn malformed_go_is_an_error_instead_of_an_empty_scan() {
        assert!(scan_go_source("broken.go", "package svc\nfunc run( {").is_err());
    }

    #[test]
    fn cyclic_aliases_do_not_overflow_the_analyzer_stack() {
        let source = "package svc\nvar a = b\nvar b = a\nfunc run() { a.Get(ctx, key) }";
        assert!(
            scan_go_source("cycle.go", source)
                .expect("bounded alias resolution")
                .is_empty()
        );
    }

    #[test]
    fn ast_reports_literal_and_dynamic_request_purposes() {
        let source = r#"package svc
func run(u *udbclient.Udb, ctx context.Context, purpose string) {
    notes := NoteTable(u)
    billing := udbclient.WithMetadata(ctx, udbclient.Metadata{Purpose: "billing"})
    notes.Get(billing, key)
    notes.Patch(udbclient.WithMetadata(ctx, udbclient.Metadata{Purpose: purpose}), key, changes)
}"#;
        let sites = scan_go_source("purpose.go", source).expect("resolve request metadata");
        assert_eq!(sites[0].purpose.as_deref(), Some("billing"));
        assert!(!sites[0].purpose_dynamic);
        assert!(sites[1].purpose_dynamic);
    }

    #[test]
    fn ast_resolves_store_type_aliases_and_bound_method_values() {
        let source = r#"package svc
type Notes = udbclient.Table[*notesv1.Note]
func run(notes *Notes, ctx context.Context) {
    get := notes.Get
    alias := get
    alias(ctx, key)
}"#;
        let sites = scan_go_source("method.go", source).expect("resolve bound method");
        assert_eq!(sites.len(), 1);
        assert_eq!(sites[0].entity, "Note");
        assert_eq!(sites[0].method, "Get");
        assert_eq!(sites[0].line, 6);
    }

    #[test]
    fn ast_checks_tx_mutations_using_the_enclosing_request_purpose() {
        let source = r#"package svc
func run(u *udbclient.Udb, ctx context.Context, row *notesv1.Note) {
    u.Tx(udbclient.WithMetadata(ctx, udbclient.Metadata{Purpose: "billing"}), func(tx *udbclient.TxScope) error {
        tx.Upsert(row)
        tx.UpdateIf(&notesv1.Note{}, expected)
        return tx.Emit(&notesv1.NoteChanged{})
    })
}"#;
        let sites = scan_go_source("tx.go", source).expect("resolve tx payloads");
        let summary: Vec<(&str, &str)> = sites
            .iter()
            .map(|s| (s.entity.as_str(), s.action.as_str()))
            .collect();
        assert_eq!(
            summary,
            vec![
                ("Note", "Upsert"),
                ("Note", "Update"),
                ("NoteChanged", "EnqueueOutboxEvent")
            ]
        );
        assert!(
            sites
                .iter()
                .all(|site| site.purpose.as_deref() == Some("billing"))
        );
    }

    #[test]
    fn ast_resolves_sibling_file_types_and_store_constructors() {
        let root = std::env::temp_dir().join(format!("udb-code-scan-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("types.go"), "package svc\ntype Service struct { notes *udbclient.Table[*notesv1.Note] }\nfunc tables() *udbclient.Table[*tagsv1.Tag] { return nil }").unwrap();
        std::fs::write(root.join("calls.go"), "package svc\nfunc (s *Service) run() { s.notes.Patch(ctx, key, changes); tables().Delete(ctx, key) }").unwrap();
        let result = super::scan_go_tree(&root);
        std::fs::remove_dir_all(root).unwrap();
        let sites = result.expect("resolve objects across the package");
        let summary: Vec<(&str, &str)> = sites
            .iter()
            .map(|s| (s.entity.as_str(), s.action.as_str()))
            .collect();
        assert_eq!(summary, vec![("Note", "Update"), ("Tag", "Delete")]);
    }
}
