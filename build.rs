/// Load key=value pairs from the project root env file into the build environment.
///
/// Resolution order (first file that exists wins):
///   1. `APP_ENV` already in OS env → `.env.<value>`  (e.g. `.env.local`, `.env.prod`)
///   2. `.env.local`
///   3. `.env.prod`
///   4. `.env`
///
/// Only sets variables that are not already present in the environment so that
/// OS env vars always win. Silently skips if no file is found.
fn load_dotenv(manifest_dir: &std::path::Path) {
    // Two layouts supported:
    //
    //   1. Standalone UDB repo (post-split, 2026-05-31):
    //      `.env*` lives at `<manifest_dir>/`
    //   2. Embedded inside a parent monorepo:
    //      `.env*` lives at `<manifest_dir>/../`
    //
    // We probe BOTH locations, manifest_dir first (so the standalone
    // repo's own .env wins when both exist). Each location tries the
    // standard filename priority (APP_ENV → .env.local → .env.prod
    // → .env) and the first existing file across all candidates is
    // loaded.

    let app_env = std::env::var("APP_ENV").unwrap_or_default();
    let mut search_roots: Vec<std::path::PathBuf> = vec![manifest_dir.to_path_buf()];
    if let Some(parent) = manifest_dir.parent() {
        search_roots.push(parent.to_path_buf());
    }

    let mut candidates: Vec<std::path::PathBuf> = Vec::new();
    for root in &search_roots {
        if !app_env.is_empty() {
            candidates.push(root.join(format!(".env.{}", app_env)));
        }
        candidates.push(root.join(".env.local"));
        candidates.push(root.join(".env.prod"));
        candidates.push(root.join(".env"));
    }

    let path = match candidates.into_iter().find(|p| p.exists()) {
        Some(p) => p,
        None => return,
    };

    let contents = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(_) => return,
    };

    // Tell Cargo to re-run build.rs whenever .env changes.
    println!("cargo:rerun-if-changed={}", path.display());

    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, val)) = line.split_once('=') {
            let key = key.trim();
            let val = val.trim().trim_matches('\'').trim_matches('"');
            // Only set if not already provided by the OS environment.
            if std::env::var(key).is_err() {
                // SAFETY: build scripts are single-threaded; no other thread
                // can be reading the environment concurrently.
                #[allow(unused_unsafe)]
                unsafe {
                    std::env::set_var(key, val);
                }
            }
        }
    }
}

/// Resolve the proto root directory.
///
/// Resolution order (first match wins):
///   1. `UDB_PROTO_ROOT` environment variable  – absolute path or project-relative proto root.
///   2. Walk up one level from `CARGO_MANIFEST_DIR` and look for a `proto/` sub-directory.
///      This matches a monorepo layout:
///      <repo>/udb/  ->  <repo>/proto/
///
/// Override examples:
///   UDB_PROTO_ROOT=proto cargo build
///   UDB_PROTO_ROOT=E:\Projects\backend\proto cargo build
fn resolve_proto_root(
    manifest_dir: &std::path::Path,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    if let Ok(explicit) = std::env::var("UDB_PROTO_ROOT") {
        let p = resolve_existing_project_path(&explicit, manifest_dir);
        if !p.exists() {
            return Err(format!(
                "UDB_PROTO_ROOT points to a path that does not exist: {}",
                p.display()
            )
            .into());
        }
        return Ok(p);
    }

    // Two auto-detect layouts, in priority order:
    //
    //   1. <manifest_dir>/proto/udb            (post-split layout, 2026-05-31)
    //      The dedicated `udb` repo OR the monorepo after the
    //      buf-module split — protos live alongside the Rust crate
    //      so the published crate is self-contained.
    //
    //   2. <manifest_dir>/../proto             (legacy monorepo layout)
    //      Pre-split, when `proto/udb/` was a sibling of `udb/`
    //      at the parent monorepo root.
    //
    // The first existing candidate wins. `UDB_PROTO_ROOT` overrides
    // both — set it when the operator has a non-standard layout.
    let local_candidate = manifest_dir.join("proto");
    if local_candidate.join("udb").exists() {
        return Ok(local_candidate);
    }
    let sibling_candidate = manifest_dir
        .parent()
        .map(|root| root.join("proto"))
        .ok_or("unable to locate proto directory: set UDB_PROTO_ROOT to override")?;

    if !sibling_candidate.exists() {
        return Err(format!(
            "auto-detected proto root does not exist. Tried:\n  - {}\n  - {}\nSet UDB_PROTO_ROOT to the correct path.",
            local_candidate.display(),
            sibling_candidate.display(),
        )
        .into());
    }

    Ok(sibling_candidate)
}

fn resolve_existing_project_path(raw: &str, manifest_dir: &std::path::Path) -> std::path::PathBuf {
    let trimmed = raw.trim();
    let path = std::path::PathBuf::from(trimmed);
    if trimmed.is_empty() || path.is_absolute() || path.exists() {
        return path;
    }

    if let Some(repo_root) = manifest_dir.parent() {
        let candidate = repo_root.join(&path);
        if candidate.exists() {
            return candidate;
        }
    }

    let mut dir = std::env::current_dir().unwrap_or_else(|_| manifest_dir.to_path_buf());
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);

    if std::env::var_os("PROTOC").is_none() {
        let protoc = protoc_bin_vendored::protoc_bin_path()?;
        // SAFETY: build scripts are single-threaded; tonic_build reads PROTOC
        // later in this same process.
        #[allow(unused_unsafe)]
        unsafe {
            std::env::set_var("PROTOC", protoc);
        }
    }

    // Load .env early so resolve_proto_root() and UDB_PROTO_PREFIX both see
    // any values defined there (OS env vars still override).
    load_dotenv(&manifest_dir);

    // UDB_PROTO_PREFIX controls the sub-directory inside the proto root that
    // contains the udb proto packages. If absent, use the neutral `udb/`
    // package.
    //
    // Override example:
    //   UDB_PROTO_PREFIX=udb cargo build
    let proto_root = resolve_proto_root(&manifest_dir)?;
    let requested_prefix = std::env::var("UDB_PROTO_PREFIX").unwrap_or_else(|_| "udb".to_string());
    let prefix = if requested_prefix != "udb"
        && proto_root.join("udb").exists()
        && std::env::var("UDB_PROTO_PREFIX_FORCE").is_err()
    {
        println!(
            "cargo:warning=Ignoring legacy UDB_PROTO_PREFIX={requested_prefix}; using neutral proto/udb. Set UDB_PROTO_PREFIX_FORCE=1 to override."
        );
        "udb".to_string()
    } else {
        requested_prefix
    };

    println!("cargo:rerun-if-changed={}", proto_root.display());
    println!("cargo:rerun-if-env-changed=UDB_PROTO_ROOT");
    println!("cargo:rerun-if-env-changed=UDB_PROTO_PREFIX");

    let udb_root = proto_root.join(&prefix);
    if !udb_root.exists() {
        return Err(format!(
            "udb proto directory not found: {}\n\
             Adjust UDB_PROTO_PREFIX (current value: \"{}\") or UDB_PROTO_ROOT.",
            udb_root.display(),
            prefix
        )
        .into());
    }

    let out_dir = std::env::var("OUT_DIR")?;
    let descriptor_path = std::path::Path::new(&out_dir).join("udb_descriptor.bin");

    // Recursively collect every `.proto` under the udb package root so new
    // domains (core/authn, core/authz, core/apikey, …) are compiled without
    // editing a hand-maintained file list. Sorted for deterministic codegen.
    let mut proto_files = Vec::new();
    collect_proto_files(&udb_root, &mut proto_files)?;
    proto_files.sort();
    if proto_files.is_empty() {
        return Err(format!("no .proto files found under {}", udb_root.display()).into());
    }
    for file in &proto_files {
        println!("cargo:rerun-if-changed={}", file.display());
    }

    // Include paths: the proto root (resolves `udb/...` imports) plus the
    // vendored third-party protos (google/api/*). Vendoring keeps `cargo build`
    // offline — no `buf` or network dependency at build time, matching the
    // protoc-bin-vendored approach. google/protobuf/* well-known types are
    // resolved by protoc itself.
    let mut includes: Vec<std::path::PathBuf> = vec![proto_root.clone()];
    for candidate in [
        manifest_dir.join("third_party/googleapis"),
        proto_root
            .parent()
            .map(|parent| parent.join("third_party/googleapis"))
            .unwrap_or_default(),
    ] {
        if candidate.join("google/api/annotations.proto").exists() {
            includes.push(candidate);
            break;
        }
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        // A.5: generate selected high-volume bytes fields as `bytes::Bytes` so
        // the Rust server can pass object chunks / row keys without copying.
        // Server-side (tonic_build) only — SDKs generate from Buf and are
        // unaffected; the wire format is identical (`bytes` is `bytes`).
        .bytes([
            ".udb.entity.v1.Chunk.data",
            ".udb.entity.v1.ProjectionDriftDivergentRow.row_key_json",
        ])
        .file_descriptor_set_path(&descriptor_path)
        .compile_protos(&proto_files, &includes)?;

    // Emit a `pub mod` tree mirroring the proto packages actually compiled,
    // e.g. `udb.core.authn.services.v1` -> `udb::core::authn::services::v1`,
    // with `tonic::include_proto!` at each package leaf. src/protocol/mod.rs
    // just `include!`s the result, so it never hard-codes package names.
    let mut packages = std::collections::BTreeSet::new();
    for file in &proto_files {
        if let Some(pkg) = read_proto_package(file) {
            packages.insert(pkg);
        }
    }
    if packages.is_empty() {
        return Err("no `package` declarations found in the compiled protos".into());
    }

    let mut protocol_rs = render_package_modules(&packages);
    // Preserve the historical flat re-exports so existing `crate::protocol::*`
    // references to the broker contract keep compiling. The newer core auth
    // packages are intentionally NOT glob-exported because type names such as
    // `Principal` recur across them; reference those by full module path.
    for legacy in ["udb.entity.v1", "udb.events.v1", "udb.services.v1"] {
        if packages.contains(legacy) {
            protocol_rs.push_str(&format!("pub use {}::*;\n", legacy.replace('.', "::")));
        }
    }

    std::fs::write(
        std::path::Path::new(&out_dir).join("protocol.rs"),
        protocol_rs,
    )?;

    // §7 DTO redaction codegen: generate a `RedactStorageOnly` trait + a blanking
    // impl per entity message that has any `output_view: OUTPUT_VIEW_STORAGE_ONLY`
    // field. Parsed straight from the proto source so a newly-annotated
    // storage-only column is blanked from outbound DTOs STRUCTURALLY, not by a
    // hand-edited mapper (final_task.md §7). Outbound mappers call
    // `.redact_storage_only()` on the built proto before returning it.
    let redaction_rs = render_storage_only_redaction(&proto_files);
    std::fs::write(
        std::path::Path::new(&out_dir).join("dto_redaction.rs"),
        redaction_rs,
    )?;

    Ok(())
}

/// Parse every compiled `.proto` for fields annotated
/// `output_view: OUTPUT_VIEW_STORAGE_ONLY` and emit a `RedactStorageOnly` trait
/// plus a blanking impl per affected message. The field set is descriptor-derived
/// (read from the proto source), so it can never drift from the annotations.
fn render_storage_only_redaction(proto_files: &[std::path::PathBuf]) -> String {
    // (package, MessageName) -> ordered, de-duped storage-only field idents.
    let mut entries: std::collections::BTreeMap<(String, String), Vec<String>> =
        std::collections::BTreeMap::new();
    for file in proto_files {
        let Ok(contents) = std::fs::read_to_string(file) else {
            continue;
        };
        let package = read_proto_package(file).unwrap_or_default();
        // Stack of (message_name, brace_depth_when_opened); innermost = last.
        let mut msg_stack: Vec<(String, i32)> = Vec::new();
        let mut depth: i32 = 0;
        for line in contents.lines() {
            let trimmed = line.trim();
            if let Some(rest) = trimmed.strip_prefix("message ") {
                if trimmed.contains('{') {
                    let name = rest
                        .split_whitespace()
                        .next()
                        .unwrap_or("")
                        .trim_end_matches('{')
                        .trim()
                        .to_string();
                    if !name.is_empty() {
                        msg_stack.push((name, depth));
                    }
                }
            }
            if trimmed.contains("OUTPUT_VIEW_STORAGE_ONLY") {
                if let (Some((msg, _)), Some(field)) =
                    (msg_stack.last(), field_name_before_eq(trimmed))
                {
                    // Field idents are lowercase snake_case; this excludes the
                    // `OUTPUT_VIEW_STORAGE_ONLY = 1` enum-value definition line.
                    let is_field = !field.is_empty()
                        && field
                            .chars()
                            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
                    if is_field {
                        let v = entries.entry((package.clone(), msg.clone())).or_default();
                        if !v.contains(&field) {
                            v.push(field);
                        }
                    }
                }
            }
            depth += line.matches('{').count() as i32 - line.matches('}').count() as i32;
            while let Some((_, opened_at)) = msg_stack.last() {
                if *opened_at >= depth {
                    msg_stack.pop();
                } else {
                    break;
                }
            }
        }
    }

    let mut out = String::new();
    out.push_str("// @generated by build.rs (render_storage_only_redaction) — DO NOT EDIT.\n");
    out.push_str(
        "// Blanks OUTPUT_VIEW_STORAGE_ONLY entity fields from outbound DTOs (final_task.md §7).\n",
    );
    out.push_str(
        "/// Blank every `output_view: OUTPUT_VIEW_STORAGE_ONLY` field on an outbound DTO.\n",
    );
    out.push_str("pub trait RedactStorageOnly {\n    fn redact_storage_only(&mut self);\n}\n\n");
    for ((package, message), fields) in &entries {
        // Match prost-build's identifier casing so the impl path resolves
        // (e.g. proto `OTP` -> Rust `Otp`); already-camel names are unchanged.
        let type_name = prost_rust_type_name(message);
        let path = format!(
            "crate::proto::{}::{}",
            package.replace('.', "::"),
            type_name
        );
        out.push_str(&format!("impl RedactStorageOnly for {path} {{\n"));
        out.push_str("    fn redact_storage_only(&mut self) {\n");
        for f in fields {
            out.push_str(&format!(
                "        self.{f} = ::core::default::Default::default();\n"
            ));
        }
        out.push_str("    }\n}\n\n");
    }
    out
}

fn prost_rust_type_name(proto_message: &str) -> String {
    if proto_message
        .chars()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
    {
        let mut chars = proto_message.chars();
        match chars.next() {
            Some(first) => {
                let mut out = first.to_string();
                out.push_str(&chars.as_str().to_ascii_lowercase());
                out
            }
            None => String::new(),
        }
    } else {
        heck::ToUpperCamelCase::to_upper_camel_case(proto_message)
    }
}

/// Extract the field identifier immediately before the first `=` on a proto field
/// line (e.g. `string password_hash = 4 [...]` -> `password_hash`).
fn field_name_before_eq(line: &str) -> Option<String> {
    let eq = line.find('=')?;
    line[..eq].split_whitespace().last().map(|s| s.to_string())
}

/// Recursively collect every `*.proto` file under `dir`.
fn collect_proto_files(
    dir: &std::path::Path,
    out: &mut Vec<std::path::PathBuf>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_proto_files(&path, out)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("proto") {
            out.push(path);
        }
    }
    Ok(())
}

/// Read the `package` declaration from a `.proto` file, if present.
fn read_proto_package(path: &std::path::Path) -> Option<String> {
    let contents = std::fs::read_to_string(path).ok()?;
    for line in contents.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("package ") {
            return Some(rest.trim().trim_end_matches(';').trim().to_string());
        }
    }
    None
}

/// Render a nested `pub mod` tree from a set of dotted proto package names,
/// emitting `tonic::include_proto!("<package>")` at each package leaf.
fn render_package_modules(packages: &std::collections::BTreeSet<String>) -> String {
    #[derive(Default)]
    struct Node {
        children: std::collections::BTreeMap<String, Node>,
        is_package: bool,
    }

    let mut root = Node::default();
    for pkg in packages {
        let mut node = &mut root;
        for segment in pkg.split('.') {
            node = node.children.entry(segment.to_string()).or_default();
        }
        node.is_package = true;
    }

    fn render(node: &Node, path: &str, indent: usize, out: &mut String) {
        for (segment, child) in &node.children {
            let pad = "    ".repeat(indent);
            let child_path = if path.is_empty() {
                segment.clone()
            } else {
                format!("{path}.{segment}")
            };
            out.push_str(&format!("{pad}pub mod {segment} {{\n"));
            if child.is_package {
                out.push_str(&format!(
                    "{pad}    tonic::include_proto!(\"{child_path}\");\n"
                ));
            }
            render(child, &child_path, indent + 1, out);
            out.push_str(&format!("{pad}}}\n"));
        }
    }

    let mut out =
        String::from("// @generated by build.rs from the compiled proto packages — do not edit.\n");
    render(&root, "", 0, &mut out);
    out
}
