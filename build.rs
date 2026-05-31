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
    //      at the lifeplusbd-backend root.
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

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(&descriptor_path)
        .compile_protos(
            &[
                udb_root.join("entity/v1/types.proto"),
                udb_root.join("events/v1/udb_events.proto"),
                udb_root.join("services/v1/data_broker.proto"),
            ],
            &[&proto_root],
        )?;

    // Derive the proto package prefix and root Rust module name from UDB_PROTO_PREFIX.
    // e.g. "udb" -> pkg_prefix="udb", root_mod="udb"
    let pkg_prefix = prefix.replace('/', ".");
    let segments = prefix.split('/').collect::<Vec<_>>();
    let root_mod = segments
        .first()
        .copied()
        .filter(|segment| !segment.is_empty())
        .ok_or("UDB_PROTO_PREFIX must not be empty")?;

    // Write a protocol.rs into OUT_DIR so src/protocol/mod.rs never needs
    // to know the package name — it just does: include!(concat!(env!("OUT_DIR"), "/protocol.rs"))
    let protocol_rs = if segments.len() == 1 && root_mod == "udb" {
        format!(
            r#"pub mod udb {{
    pub mod entity {{
        pub mod v1 {{
            tonic::include_proto!("{pkg}.entity.v1");
        }}
    }}
    pub mod events {{
        pub mod v1 {{
            tonic::include_proto!("{pkg}.events.v1");
        }}
    }}
    pub mod services {{
        pub mod v1 {{
            tonic::include_proto!("{pkg}.services.v1");
        }}
    }}
}}

pub use udb::entity::v1::*;
pub use udb::events::v1::*;
pub use udb::services::v1::*;
"#,
            pkg = pkg_prefix,
        )
    } else {
        format!(
            r#"pub mod {root_mod} {{
    pub mod udb {{
        pub mod entity {{
            pub mod v1 {{
                tonic::include_proto!("{pkg}.entity.v1");
            }}
        }}
        pub mod events {{
            pub mod v1 {{
                tonic::include_proto!("{pkg}.events.v1");
            }}
        }}
        pub mod services {{
            pub mod v1 {{
                tonic::include_proto!("{pkg}.services.v1");
            }}
        }}
    }}
}}

pub use {root_mod}::udb::entity::v1::*;
pub use {root_mod}::udb::events::v1::*;
pub use {root_mod}::udb::services::v1::*;
"#,
            root_mod = root_mod,
            pkg = pkg_prefix,
        )
    };
    std::fs::write(
        std::path::Path::new(&out_dir).join("protocol.rs"),
        protocol_rs,
    )?;

    Ok(())
}
