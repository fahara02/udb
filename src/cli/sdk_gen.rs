//! `udb sdk generate` — FSM-driven, template-based SDK code generation.
//!
//! The per-language **robustness/client layer** (typed wrapper per RPC, retry,
//! deadlines, TLS, error mapping, CLI-bundling glue) is rendered from editable
//! templates under `sdk-templates/<lang>/` into `sdk/<lang>/`. The list of RPCs
//! the templates iterate comes from the embedded `FileDescriptorSet`
//! ([`udb::runtime::sdk_manifest::rpc_manifest`]) — proto is the single source of
//! truth, so the generated surface cannot drift from the wire contract. (The raw
//! message/service *stubs* remain a separate concern, produced by `buf generate`
//! per `buf.gen.yaml`; this generator never touches `gen/`.)
//!
//! The flow is an explicit finite-state machine, mirroring [`super::proto_export`]
//! and [`crate::control::engine::FsmState`]:
//!
//! ```text
//!   Start ─▶ LoadManifest ─▶ ResolveTemplates ─▶ Render ─▶ Completed
//!     └────────────┴────────────────┴──────────────┴──────▶ Failed
//! ```
//!
//! ## Template contract (language-agnostic)
//!
//! Each file under `sdk-templates/<lang>/` is materialized at the mirror path
//! under `sdk/<lang>/`. A `.tmpl` suffix is rendered then stripped; any other
//! file is copied verbatim. Skipped (never emitted): `sdkgen.yaml`/`sdkgen.toml`
//! (per-lang config), `README.md`/`TEMPLATES.md` (these document the *template*
//! and would otherwise clobber the SDK's own README), and dotfiles.
//!
//! Rendering substitutes:
//!   * **Scalars** anywhere: `{{UDB_VERSION}}`, `{{PROTOCOL_VERSION}}`, `{{LANG}}`,
//!     `{{RPC_COUNT}}`, `{{SERVICE_COUNT}}`, `{{GENERATED_NOTE}}`.
//!   * **Per-RPC blocks** — the lines between a line containing `@@UDB_RPC_BEGIN`
//!     and one containing `@@UDB_RPC_END` are repeated once per RPC, with the
//!     marker lines removed. An optional filter follows the BEGIN token, e.g.
//!     `@@UDB_RPC_BEGIN service=DataBroker kind=unary`. Per-RPC placeholders:
//!     `{{RPC_NAME}}`, `{{RPC_SNAKE}}`, `{{RPC_INPUT}}`, `{{RPC_INPUT_PKG}}`,
//!     `{{RPC_OUTPUT}}`, `{{RPC_OUTPUT_PKG}}`, `{{RPC_CLIENT_STREAMING}}`,
//!     `{{RPC_SERVER_STREAMING}}`, `{{RPC_KIND}}`, `{{RPC_PATH}}`,
//!     `{{RPC_CSRF_REQUIRED}}`, `{{RPC_INTERNAL_GRPC_ONLY}}`,
//!     `{{RPC_PUBLIC_LISTENER}}`, `{{RPC_CONTROL_PLANE_LISTENER}}`,
//!     `{{RPC_PEER_LISTENER}}`,
//!     `{{SERVICE_NAME}}`, `{{SERVICE_PKG}}`, `{{SERVICE_FULL}}`. The optional
//!     BEGIN filter also accepts `surface=public|control_plane|peer`,
//!     `auth=<mode>`, and `native_service=<id>` in addition to `service=`/`kind=`.
//!   * **Per-service blocks** — `@@UDB_SERVICE_BEGIN`/`@@UDB_SERVICE_END`, repeated
//!     per service, with `{{SERVICE_NAME}}`, `{{SERVICE_PKG}}`, `{{SERVICE_FULL}}`,
//!     `{{SERVICE_RPC_COUNT}}`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

use udb::runtime::sdk_manifest::{
    EntityColumnDescriptor, EntityDescriptor, RpcDescriptor, entity_manifest,
    entity_manifest_from_proto_dir, rpc_manifest,
};

use super::{SdkAction, SdkSelector};

/// States of the SDK-generation FSM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SdkGenState {
    Start,
    LoadManifest,
    ResolveTemplates,
    Render,
    Completed,
    Failed,
}

impl SdkGenState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Start => "START",
            Self::LoadManifest => "LOAD_MANIFEST",
            Self::ResolveTemplates => "RESOLVE_TEMPLATES",
            Self::Render => "RENDER",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }

    pub(crate) fn valid_transitions(self) -> &'static [SdkGenState] {
        use SdkGenState::*;
        match self {
            Start => &[LoadManifest, Failed],
            LoadManifest => &[ResolveTemplates, Failed],
            ResolveTemplates => &[Render, Failed],
            Render => &[Completed, Failed],
            Completed | Failed => &[],
        }
    }

    pub(crate) fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }
}

/// Entry point for `udb sdk <action>`.
pub(crate) fn run(
    action: SdkAction,
    lang: &str,
    against: &str,
    templates_dir: &str,
    out_dir: &str,
    selector: &SdkSelector,
) -> i32 {
    match action {
        SdkAction::Manifest => emit_manifest_json(),
        SdkAction::ListLangs => list_languages(templates_dir),
        SdkAction::Init => init_sdk(lang),
        SdkAction::Generate => generate(lang, templates_dir, out_dir, selector, None),
        SdkAction::Diff => sdk_diff(selector, against),
    }
}

/// W3 (tip 9): `udb sdk diff --project-proto <dir> --against <generated-file>`.
/// Regenerates the typed-entities file IN MEMORY from the current protos and
/// byte-compares it with the committed file — the sqlc-style CI verb that
/// catches both stale and hand-edited generated output. Exit 0 = current,
/// 1 = drift (with an entity-level summary), 2 = usage/IO error.
fn sdk_diff(selector: &SdkSelector, against: &str) -> i32 {
    let Some(proto_root) = selector.project_proto.as_deref() else {
        eprintln!("sdk diff requires --project-proto <dir> (the protos to regenerate from)");
        return 2;
    };
    if against.trim().is_empty() {
        eprintln!("sdk diff requires --against <generated-file> (the committed output)");
        return 2;
    }
    let committed = match std::fs::read_to_string(against) {
        Ok(text) => text,
        Err(err) => {
            eprintln!("sdk diff: cannot read {against}: {err}");
            return 2;
        }
    };
    let entities = match udb::runtime::sdk_manifest::entity_manifest_from_proto_dir(
        std::path::Path::new(proto_root),
    ) {
        Ok(entities) => entities,
        Err(err) => {
            eprintln!("sdk diff: {err}");
            return 2;
        }
    };
    // The committed file's `package` line is authoritative for the comparison —
    // regenerating under a different package would drift on every line.
    let package = committed
        .lines()
        .find_map(|line| line.strip_prefix("package "))
        .map(str::trim)
        .unwrap_or("udbgen")
        .to_string();
    // Regenerate through the SAME render+gofmt pipeline the write path uses, so a
    // fresh file is byte-comparable with the committed (gofmt-canonical) one.
    let regenerated = match render_go_entities_file_canonical(&entities, &package) {
        Ok(regenerated) => regenerated,
        Err(err) => {
            eprintln!("sdk diff: {err}");
            return 2;
        }
    };
    let committed_normalized = committed.replace("\r\n", "\n");
    if regenerated == committed_normalized {
        println!(
            "sdk diff: {against} is current ({} entities)",
            entities.len()
        );
        return 0;
    }
    // Entity-level drift summary: compare each entity's marshalling section.
    let mut drifted = Vec::new();
    for entity in &entities {
        let marker = format!("func {}ToUDBRecord(", entity.short_name);
        let fresh = section_after(&regenerated, &marker);
        let old = section_after(&committed_normalized, &marker);
        if fresh != old {
            drifted.push(entity.short_name.clone());
        }
    }
    eprintln!(
        "sdk diff: {against} is STALE ({} of {} entities drifted{}{}) — regenerate with \
         `udb sdk generate --project-proto {proto_root} --lang go` and commit",
        drifted.len(),
        entities.len(),
        if drifted.is_empty() { "" } else { ": " },
        drifted.join(", "),
    );
    1
}

/// The text from `marker` to the next top-level `func ` (or EOF) — one
/// entity's marshalling section, for the drift summary.
fn section_after<'a>(text: &'a str, marker: &str) -> &'a str {
    let Some(start) = text.find(marker) else {
        return "";
    };
    let rest = &text[start..];
    match rest[marker.len()..].find("\nfunc ") {
        Some(end) => &rest[..marker.len() + end],
        None => rest,
    }
}

/// Entry point for `udb orm scaffold` (master-plan 10.5).
///
/// ORM model generation is deliberately **not** a separate code path: it drives
/// the exact same [`generate`] FSM/template pipeline as `udb sdk generate`,
/// optionally scoped to a single entity via [`select_entities`]. There is no
/// second/duplicate generator here — the typed entity/repository wrappers come
/// from the same `@@UDB_ENTITY_BEGIN` template blocks rendered from the embedded
/// descriptor set, so generated models can never drift from the SDK surface or
/// the wire contract. `udb plan` / `udb sync-migrations` evolve the schema; this
/// regenerates the models that read/write it.
pub(crate) fn run_orm_scaffold(
    lang: &str,
    templates_dir: &str,
    out_dir: &str,
    entity: Option<&str>,
) -> i32 {
    generate(
        lang,
        templates_dir,
        out_dir,
        &SdkSelector::default(),
        entity,
    )
}

/// Restrict the entity manifest to a single entity when `--entity <fqn>` is
/// passed. Accepts either the fully-qualified `pkg.Message` name or the bare
/// message name. Codegen STOPS (typed error) when the filter matches no known
/// entity — emitting an empty model set would be a silent lie. `None` (no
/// filter) returns the full manifest unchanged, so `udb sdk generate` behavior
/// is identical to before.
fn select_entities(
    entities: Vec<EntityDescriptor>,
    filter: Option<&str>,
) -> Result<Vec<EntityDescriptor>, String> {
    let Some(target) = filter.map(str::trim).filter(|t| !t.is_empty()) else {
        return Ok(entities);
    };
    let selected: Vec<EntityDescriptor> = entities
        .into_iter()
        .filter(|entity| entity_matches(entity, target))
        .collect();
    if selected.is_empty() {
        return Err(format!(
            "--entity `{target}` matched no annotated entity; pass the message name \
             (e.g. `User`) or its fully-qualified `pkg.Message` name (see `udb sdk manifest`)"
        ));
    }
    Ok(selected)
}

fn validate_repository_entities(entities: &[EntityDescriptor]) -> Result<(), String> {
    for entity in entities {
        if entity.primary_keys.is_empty() {
            let name = if entity.message_type.trim().is_empty() {
                entity.short_name.as_str()
            } else {
                entity.message_type.as_str()
            };
            return Err(format!(
                "entity `{name}` has no descriptor primary key; add a primary_key column \
                 annotation before generating repository wrappers"
            ));
        }
    }
    Ok(())
}

/// Whether `entity` is named by `target` — either its bare message name or its
/// fully-qualified `proto_package.Message` name.
fn entity_matches(entity: &EntityDescriptor, target: &str) -> bool {
    if entity.message_type == target {
        return true;
    }
    if entity.short_name == target {
        return true;
    }
    if !entity.proto_package.is_empty() {
        return format!("{}.{}", entity.proto_package, entity.short_name) == target;
    }
    false
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequirementKind {
    Required,
    Recommended,
}

#[derive(Debug, Clone)]
struct SdkRequirement {
    name: &'static str,
    ok: bool,
    kind: RequirementKind,
    install: &'static str,
}

#[derive(Debug, Clone)]
struct SdkPreflight {
    lang: &'static str,
    title: &'static str,
    bootstrap: String,
    requirements: Vec<SdkRequirement>,
}

const UDB_PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// T-1: the SDK templates embedded into the binary, so `udb sdk generate` works
/// from an INSTALLED binary in any directory — previously it read `sdk-templates/`
/// relative to CWD, so the flagship command only worked inside a UDB source
/// checkout. Mirrors the proto-catalog embedding in `native_catalog.rs`.
static EMBEDDED_SDK_TEMPLATES: include_dir::Dir<'_> =
    include_dir::include_dir!("$CARGO_MANIFEST_DIR/sdk-templates");

/// Resolve the templates directory to use. If `templates_dir` exists on disk
/// (source checkout, or an explicit `--templates <dir>` override), use it as-is.
/// Otherwise extract the embedded templates once to a version-keyed temp dir and
/// use that, so an installed binary is self-contained. The existing filesystem
/// render logic then runs unchanged against the returned path.
fn resolve_effective_templates_dir(templates_dir: &str) -> Result<PathBuf, String> {
    let fs_path = Path::new(templates_dir);
    if fs_path.is_dir() {
        return Ok(fs_path.to_path_buf());
    }
    let base = std::env::temp_dir().join(format!("udb-sdk-templates-{UDB_PACKAGE_VERSION}"));
    let sentinel = base.join(".udb-extracted");
    if !sentinel.exists() {
        std::fs::create_dir_all(&base)
            .map_err(|err| format!("create embedded-template cache `{}`: {err}", base.display()))?;
        // Manual recursive extraction rather than `Dir::extract`, so the output
        // layout is deterministic: `include_dir` file paths are relative to the
        // embedded root (`sdk-templates`), so each writes to `<base>/<lang>/…` and
        // the caller can use `<base>` directly (no crate-version-dependent nesting).
        extract_embedded_dir(&EMBEDDED_SDK_TEMPLATES, &base)
            .map_err(|err| format!("extract embedded SDK templates: {err}"))?;
        let _ = std::fs::write(&sentinel, b"1");
    }
    Ok(base)
}

/// Recursively write an embedded `include_dir::Dir` to `base`, preserving the
/// relative layout. Idempotent per file (overwrites), safe to re-run.
fn extract_embedded_dir(dir: &include_dir::Dir<'_>, base: &Path) -> std::io::Result<()> {
    for file in dir.files() {
        let path = base.join(file.path());
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, file.contents())?;
    }
    for sub in dir.dirs() {
        extract_embedded_dir(sub, base)?;
    }
    Ok(())
}

fn init_sdk(lang: &str) -> i32 {
    let languages = match resolve_init_languages(lang) {
        Ok(languages) => languages,
        Err(message) => {
            eprintln!("{message}");
            return 2;
        }
    };

    let mut missing_required = 0usize;
    println!(
        "sdk init preflight — checking {} language SDK(s) plus native feature tools",
        languages.len()
    );
    for lang in languages {
        let report = preflight_language(lang);
        missing_required += print_preflight_report(&report);
    }

    let native_report = native_feature_preflight();
    missing_required += print_preflight_report(&native_report);

    if missing_required == 0 {
        println!("\nsdk init preflight OK");
        0
    } else {
        eprintln!(
            "\nsdk init preflight found {missing_required} missing required prerequisite(s)."
        );
        eprintln!("Install the missing tools/extensions above, then rerun `udb sdk init`.");
        1
    }
}

fn print_preflight_report(report: &SdkPreflight) -> usize {
    let mut missing_required = 0usize;
    println!("\n{} ({})", report.title, report.lang);
    println!("  bootstrap: {}", report.bootstrap);
    for req in &report.requirements {
        let marker = if req.ok { "ok" } else { "missing" };
        let kind = match req.kind {
            RequirementKind::Required => "required",
            RequirementKind::Recommended => "recommended",
        };
        println!("  [{marker}] {kind}: {}", req.name);
        if !req.ok {
            println!("        install: {}", req.install);
            if req.kind == RequirementKind::Required {
                missing_required += 1;
            }
        }
    }
    missing_required
}

fn resolve_init_languages(lang: &str) -> Result<Vec<&'static str>, String> {
    let normalized = lang.trim().to_ascii_lowercase();
    if normalized.is_empty() || normalized == "all" {
        return Ok(vec!["typescript", "python", "go", "java", "csharp", "php"]);
    }
    let lang = match normalized.as_str() {
        "ts" | "typescript" | "node" | "javascript" => "typescript",
        "py" | "python" => "python",
        "go" | "golang" => "go",
        "java" | "jvm" => "java",
        "cs" | "c#" | "csharp" | "dotnet" => "csharp",
        "php" | "laravel" | "symfony" => "php",
        other => {
            return Err(format!(
                "sdk init: unknown language `{other}`; expected all, typescript, python, go, java, csharp, or php"
            ));
        }
    };
    Ok(vec![lang])
}

fn preflight_language(lang: &str) -> SdkPreflight {
    match lang {
        "typescript" => SdkPreflight {
            lang: "typescript",
            title: "TypeScript / Node SDK",
            bootstrap: format!("npm i @udb_plus/sdk@{UDB_PACKAGE_VERSION}"),
            requirements: vec![
                command_req(
                    "node",
                    "Node.js",
                    "Install Node.js 20+ from https://nodejs.org/",
                ),
                command_req("npm", "npm", "Install Node.js/npm, then run `npm install`."),
            ],
        },
        "python" => SdkPreflight {
            lang: "python",
            title: "Python SDK",
            bootstrap: format!("python -m pip install udb-client=={UDB_PACKAGE_VERSION}"),
            requirements: vec![
                any_command_req(
                    &["python", "python3", "py"],
                    "Python",
                    "Install Python 3.10+ and ensure `python` is on PATH.",
                ),
                any_command_req(
                    &["pip", "pip3"],
                    "pip",
                    "Install pip or run `python -m ensurepip --upgrade`.",
                ),
            ],
        },
        "go" => SdkPreflight {
            lang: "go",
            title: "Go SDK",
            bootstrap: format!("go get github.com/fahara02/udb/sdk/go@v{UDB_PACKAGE_VERSION}"),
            requirements: vec![command_req(
                "go",
                "Go toolchain",
                "Install Go 1.22+ from https://go.dev/dl/.",
            )],
        },
        "java" => SdkPreflight {
            lang: "java",
            title: "Java SDK",
            bootstrap: "mvn test  # or add the UDB Java client dependency in your pom.xml"
                .to_string(),
            requirements: vec![
                command_req(
                    "java",
                    "Java runtime/JDK",
                    "Install JDK 17+ and ensure `java` is on PATH.",
                ),
                command_req(
                    "mvn",
                    "Maven",
                    "Install Apache Maven and ensure `mvn` is on PATH.",
                ),
            ],
        },
        "csharp" => SdkPreflight {
            lang: "csharp",
            title: "C# / .NET SDK",
            bootstrap: format!("dotnet add package Udb.Client --version {UDB_PACKAGE_VERSION}"),
            requirements: vec![command_req(
                "dotnet",
                ".NET SDK",
                "Install .NET SDK 8+ from https://dotnet.microsoft.com/download.",
            )],
        },
        "php" => SdkPreflight {
            lang: "php",
            title: "PHP / Laravel SDK",
            bootstrap: format!("composer require fahara02/udb-laravel:^{UDB_PACKAGE_VERSION}"),
            requirements: vec![
                command_req(
                    "php",
                    "PHP CLI",
                    "Install PHP 8.1+ and ensure `php` is on PATH.",
                ),
                command_req(
                    "composer",
                    "Composer",
                    "Install Composer from https://getcomposer.org/.",
                ),
                php_extension_req(
                    "grpc",
                    RequirementKind::Required,
                    "Linux/macOS: `pecl install grpc` then add `extension=grpc.so`; Windows: download the matching PECL php_grpc.dll and add `extension=php_grpc.dll` to the active php.ini.",
                ),
                php_extension_req(
                    "protobuf",
                    RequirementKind::Recommended,
                    "Linux/macOS: `pecl install protobuf` then add `extension=protobuf.so`; Windows: download the matching PECL php_protobuf.dll and add `extension=php_protobuf.dll`. The Composer google/protobuf runtime works without it but is slower.",
                ),
            ],
        },
        _ => unreachable!("resolve_init_languages normalizes languages"),
    }
}

fn native_feature_preflight() -> SdkPreflight {
    SdkPreflight {
        lang: "native",
        title: "Native feature/toolchain preflight",
        bootstrap: "udb proto export --buf-yaml && buf generate && udb native doctor".to_string(),
        requirements: vec![
            command_req(
                "buf",
                "Buf CLI",
                "Install buf from https://buf.build/docs/installation, then rerun proto export/generation.",
            ),
            command_req_kind(
                "cmake",
                "CMake",
                RequirementKind::Recommended,
                "Install CMake and ensure it is on PATH; default Kafka/rdkafka builds use cmake-build.",
            ),
            command_req_kind(
                "perl",
                "Perl for vendored OpenSSL",
                RequirementKind::Recommended,
                "Install Strawberry Perl on Windows or system Perl on Linux/macOS; `--features webauthn` builds vendored OpenSSL through openssl-sys.",
            ),
            command_req_kind(
                "openssl",
                "OpenSSL CLI",
                RequirementKind::Recommended,
                "Install OpenSSL if you need certificate/key inspection; WebAuthn uses vendored OpenSSL but operators often need the CLI.",
            ),
            command_req_kind(
                "ffmpeg",
                "FFmpeg",
                RequirementKind::Recommended,
                "Install FFmpeg for native media/video transcode or caption pipelines before enabling those workloads.",
            ),
            command_req_kind(
                "ghz",
                "ghz gRPC load tester",
                RequirementKind::Recommended,
                "Install ghz from https://ghz.sh/ for scripts/native-load-test smoke and load checks.",
            ),
            command_req_kind(
                "docker",
                "Docker",
                RequirementKind::Recommended,
                "Install Docker Desktop or Docker Engine for local Postgres/Redis/Qdrant/MinIO/Kafka dependencies.",
            ),
            command_req_kind(
                "protoc",
                "protoc",
                RequirementKind::Recommended,
                "Install protoc if you use offline protobuf generation; buf-managed generation is preferred.",
            ),
        ],
    }
}

fn command_req(command: &'static str, name: &'static str, install: &'static str) -> SdkRequirement {
    command_req_kind(command, name, RequirementKind::Required, install)
}

fn command_req_kind(
    command: &'static str,
    name: &'static str,
    kind: RequirementKind,
    install: &'static str,
) -> SdkRequirement {
    SdkRequirement {
        name,
        ok: command_exists(command),
        kind,
        install,
    }
}

fn any_command_req(commands: &[&str], name: &'static str, install: &'static str) -> SdkRequirement {
    SdkRequirement {
        name,
        ok: commands.iter().any(|command| command_exists(command)),
        kind: RequirementKind::Required,
        install,
    }
}

fn php_extension_req(
    extension: &'static str,
    kind: RequirementKind,
    install: &'static str,
) -> SdkRequirement {
    SdkRequirement {
        name: extension,
        ok: php_extension_loaded(extension),
        kind,
        install,
    }
}

fn command_exists(command: &str) -> bool {
    let probe = if cfg!(windows) { "where" } else { "sh" };
    let mut cmd = ProcessCommand::new(probe);
    if cfg!(windows) {
        cmd.arg(command);
    } else {
        cmd.arg("-c")
            .arg(format!("command -v {command} >/dev/null 2>&1"));
    }
    cmd.output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}

fn php_extension_loaded(extension: &str) -> bool {
    let output = ProcessCommand::new("php")
        .arg("-r")
        .arg(format!("exit(extension_loaded('{extension}') ? 0 : 1);"))
        .output();
    output.map(|out| out.status.success()).unwrap_or(false)
}

/// Print the RPC manifest (proto-derived) as JSON, grouped by service.
fn emit_manifest_json() -> i32 {
    let manifest = rpc_manifest();
    if manifest.is_empty() {
        eprintln!("sdk manifest: no RPCs found (build mismatch)");
        return 1;
    }
    let mut services: Vec<String> = manifest.iter().map(|r| r.service_full()).collect();
    services.sort();
    services.dedup();

    let service_objs: Vec<serde_json::Value> = services
        .iter()
        .map(|full| {
            let rpcs: Vec<serde_json::Value> = manifest
                .iter()
                .filter(|r| &r.service_full() == full)
                .map(rpc_to_json)
                .collect();
            serde_json::json!({ "service": full, "rpc_count": rpcs.len(), "rpcs": rpcs })
        })
        .collect();

    let doc = serde_json::json!({
        "udb_version": env!("CARGO_PKG_VERSION"),
        "protocol_version": udb::runtime::native_catalog::protocol_version(),
        "service_count": services.len(),
        "rpc_count": manifest.len(),
        // master-plan 10.6: per-backend ORM capability tier, derived at build
        // time from BackendKind::orm_tier() (a projection of BackendTier — no
        // parallel enum). Surfaced here so generators/tooling can embed it.
        "backend_orm_tiers": backend_orm_tiers_json(),
        "services": service_objs,
    });
    match serde_json::to_string_pretty(&doc) {
        Ok(text) => {
            println!("{text}");
            0
        }
        Err(err) => {
            eprintln!("sdk manifest: serialize: {err}");
            1
        }
    }
}

fn rpc_to_json(rpc: &RpcDescriptor) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("method".to_string(), serde_json::json!(&rpc.method));
    obj.insert(
        "method_snake".to_string(),
        serde_json::json!(&rpc.method_snake),
    );
    obj.insert(
        "method_alias".to_string(),
        serde_json::json!(&rpc.method_alias),
    );
    obj.insert(
        "method_alias_snake".to_string(),
        serde_json::json!(&rpc.method_alias_snake),
    );
    obj.insert(
        "method_alias_camel".to_string(),
        serde_json::json!(&rpc.method_alias_camel),
    );
    obj.insert(
        "method_alias_pascal".to_string(),
        serde_json::json!(&rpc.method_alias_pascal),
    );
    obj.insert(
        "rest_operation_id".to_string(),
        serde_json::json!(&rpc.rest_operation_id),
    );
    obj.insert("input".to_string(), serde_json::json!(&rpc.input_short));
    obj.insert("input_pkg".to_string(), serde_json::json!(&rpc.input_pkg));
    obj.insert("output".to_string(), serde_json::json!(&rpc.output_short));
    obj.insert("output_pkg".to_string(), serde_json::json!(&rpc.output_pkg));
    obj.insert(
        "client_streaming".to_string(),
        serde_json::json!(rpc.client_streaming),
    );
    obj.insert(
        "server_streaming".to_string(),
        serde_json::json!(rpc.server_streaming),
    );
    obj.insert("kind".to_string(), serde_json::json!(rpc.kind()));
    obj.insert("path".to_string(), serde_json::json!(rpc.grpc_path()));
    obj.insert("http_verb".to_string(), serde_json::json!(&rpc.http_verb));
    obj.insert("http_path".to_string(), serde_json::json!(&rpc.http_path));
    obj.insert("http_body".to_string(), serde_json::json!(&rpc.http_body));
    obj.insert(
        "http_response_body".to_string(),
        serde_json::json!(&rpc.http_response_body),
    );
    obj.insert(
        "native_service_id".to_string(),
        serde_json::json!(&rpc.native_service_id),
    );
    obj.insert(
        "logical_service_id".to_string(),
        serde_json::json!(&rpc.logical_service_id),
    );
    obj.insert(
        "sdk_facade_name".to_string(),
        serde_json::json!(&rpc.sdk_facade_name),
    );
    obj.insert(
        "cli_scaffold_group".to_string(),
        serde_json::json!(&rpc.cli_scaffold_group),
    );
    obj.insert("auth_mode".to_string(), serde_json::json!(&rpc.auth_mode));
    obj.insert("roles".to_string(), serde_json::json!(&rpc.roles));
    obj.insert("scopes".to_string(), serde_json::json!(&rpc.scopes));
    obj.insert("policy_ref".to_string(), serde_json::json!(&rpc.policy_ref));
    obj.insert(
        "tenant_required".to_string(),
        serde_json::json!(rpc.tenant_required),
    );
    obj.insert(
        "tenant_field".to_string(),
        serde_json::json!(&rpc.tenant_field),
    );
    obj.insert(
        "project_field".to_string(),
        serde_json::json!(&rpc.project_field),
    );
    obj.insert(
        "credential_types".to_string(),
        serde_json::json!(&rpc.credential_types),
    );
    obj.insert(
        "requires_postgres".to_string(),
        serde_json::json!(rpc.requires_postgres),
    );
    obj.insert(
        "requires_redis".to_string(),
        serde_json::json!(rpc.requires_redis),
    );
    obj.insert(
        "requires_object_store".to_string(),
        serde_json::json!(rpc.requires_object_store),
    );
    obj.insert(
        "requires_kafka".to_string(),
        serde_json::json!(rpc.requires_kafka),
    );
    obj.insert(
        "requires_feature".to_string(),
        serde_json::json!(&rpc.requires_feature),
    );
    obj.insert(
        "default_enabled".to_string(),
        serde_json::json!(rpc.default_enabled),
    );
    obj.insert("surface".to_string(), serde_json::json!(&rpc.surface));
    obj.insert(
        "listener_kind".to_string(),
        serde_json::json!(&rpc.listener_kind),
    );
    obj.insert(
        "global_enablement_key".to_string(),
        serde_json::json!(&rpc.global_enablement_key),
    );
    obj.insert(
        "service_enablement_key".to_string(),
        serde_json::json!(&rpc.service_enablement_key),
    );
    obj.insert(
        "required_dependencies".to_string(),
        serde_json::json!(&rpc.required_dependencies),
    );
    obj.insert(
        "disabled_service_error_contract".to_string(),
        serde_json::json!(&rpc.disabled_service_error_contract),
    );
    obj.insert(
        "browser_safe".to_string(),
        serde_json::json!(rpc.browser_safe),
    );
    obj.insert(
        "server_only".to_string(),
        serde_json::json!(rpc.server_only),
    );
    obj.insert(
        "default_deadline_ms".to_string(),
        serde_json::json!(rpc.default_deadline_ms),
    );
    obj.insert(
        "default_max_attempts".to_string(),
        serde_json::json!(rpc.default_max_attempts),
    );
    obj.insert(
        "csrf_required".to_string(),
        serde_json::json!(rpc.csrf_required),
    );
    obj.insert(
        "internal_grpc_only".to_string(),
        serde_json::json!(rpc.internal_grpc_only),
    );
    obj.insert(
        "public_listener_allowed".to_string(),
        serde_json::json!(rpc.public_listener_allowed),
    );
    obj.insert(
        "control_plane_listener_allowed".to_string(),
        serde_json::json!(rpc.control_plane_listener_allowed),
    );
    obj.insert(
        "peer_listener_allowed".to_string(),
        serde_json::json!(rpc.peer_listener_allowed),
    );
    obj.insert(
        "operation_kind".to_string(),
        serde_json::json!(&rpc.operation_kind),
    );
    obj.insert("read_only".to_string(), serde_json::json!(rpc.read_only));
    obj.insert(
        "replay_safe".to_string(),
        serde_json::json!(rpc.replay_safe),
    );
    serde_json::Value::Object(obj)
}

/// List the language template directories available under `templates_dir`.
fn list_languages(templates_dir: &str) -> i32 {
    // T-1: fall back to the embedded templates when no on-disk dir exists.
    let root = match resolve_effective_templates_dir(templates_dir) {
        Ok(root) => root,
        Err(err) => {
            eprintln!("sdk list-langs: {err}");
            return 1;
        }
    };
    let root = root.as_path();
    if !root.is_dir() {
        eprintln!("sdk list-langs: template dir `{templates_dir}` not found");
        return 1;
    }
    let mut langs: Vec<String> = Vec::new();
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(err) => {
            eprintln!("sdk list-langs: read `{templates_dir}`: {err}");
            return 1;
        }
    };
    for entry in entries.flatten() {
        if entry.path().is_dir() {
            langs.push(entry.file_name().to_string_lossy().to_string());
        }
    }
    langs.sort();
    if langs.is_empty() {
        println!("(no language templates under {templates_dir})");
    } else {
        for lang in &langs {
            println!("{lang}");
        }
    }
    0
}

/// Drive the generation FSM for one language or `all`. `entity_filter`, when
/// `Some`, scopes the rendered entity/repository models to a single entity
/// (used by `udb orm scaffold --entity`); `None` renders every entity exactly
/// as `udb sdk generate` always has.
fn generate(
    lang: &str,
    templates_dir: &str,
    out_dir: &str,
    selector: &SdkSelector,
    entity_filter: Option<&str>,
) -> i32 {
    let mut fsm = Fsm::new();

    // ── Start ─▶ LoadManifest ───────────────────────────────────────────────
    if fsm.go(SdkGenState::LoadManifest).is_err() {
        return 1;
    }
    let full_manifest = rpc_manifest();
    if full_manifest.is_empty() {
        return fsm.fail("RPC manifest empty (descriptor-set build mismatch)".to_string());
    }
    // Apply CLI selectors (surface/service/native-only/deps/strict). With no
    // flags this is a clone of the full manifest — behavior is unchanged.
    let manifest = match apply_selectors(&full_manifest, selector) {
        Ok(filtered) => filtered,
        Err(err) => return fsm.fail(err),
    };
    // B1: source the ENTITY manifest from the consumer's own proto tree when
    // `--project-proto <dir>` is given (the RPC surface above stays UDB's). This is
    // what lets `udb sdk generate` emit typed repositories for a consumer's
    // entities instead of only UDB's embedded ones.
    let base_entities = if let Some(proto_root) = selector.project_proto.as_deref() {
        match entity_manifest_from_proto_dir(Path::new(proto_root)) {
            Ok(entities) => {
                fsm.note(format!(
                    "loaded {} consumer entity(ies) from --project-proto {proto_root}",
                    entities.len()
                ));
                entities
            }
            Err(err) => return fsm.fail(err),
        }
    } else {
        entity_manifest()
    };
    let entities = match select_entities(base_entities, entity_filter) {
        Ok(entities) => entities,
        Err(err) => return fsm.fail(err),
    };
    if let Err(err) = validate_repository_entities(&entities) {
        return fsm.fail(err);
    }
    if manifest.is_empty() {
        return fsm.fail(
            "selectors matched no RPCs — relax --surface/--service/--native-services".to_string(),
        );
    }
    if manifest.len() != full_manifest.len() {
        fsm.note(format!(
            "selectors retained {} of {} RPC(s)",
            manifest.len(),
            full_manifest.len()
        ));
    }
    if selector.include_deps {
        // Accepted but a documented no-op: see `apply_selectors` — proto asserts
        // no inter-service dependency edges to expand against.
        fsm.note(
            "--include-deps: no derivable inter-service edges; selection unchanged".to_string(),
        );
    }
    let service_count = manifest
        .iter()
        .map(|r| r.service_full())
        .collect::<BTreeSet<_>>()
        .len();
    fsm.note(format!(
        "{} RPC(s) across {service_count} service(s) from embedded descriptors",
        manifest.len()
    ));

    // ── LoadManifest ─▶ ResolveTemplates ────────────────────────────────────
    if fsm.go(SdkGenState::ResolveTemplates).is_err() {
        return 1;
    }
    // T-1: use the on-disk dir if present, else the embedded templates.
    let templates_root = match resolve_effective_templates_dir(templates_dir) {
        Ok(root) => root,
        Err(err) => return fsm.fail(err),
    };
    let templates_root = templates_root.as_path();
    if !templates_root.is_dir() {
        return fsm.fail(format!(
            "template dir `{templates_dir}` not found and no embedded templates available"
        ));
    }
    let langs = match resolve_langs(templates_root, lang) {
        Ok(langs) if !langs.is_empty() => langs,
        Ok(_) => {
            return fsm.fail(if lang == "all" {
                format!("no language templates under `{templates_dir}`")
            } else {
                format!("no template dir `{templates_dir}/{lang}`")
            });
        }
        Err(err) => return fsm.fail(err),
    };
    fsm.note(format!("languages: {}", langs.join(", ")));

    // ── ResolveTemplates ─▶ Render ──────────────────────────────────────────
    if fsm.go(SdkGenState::Render).is_err() {
        return 1;
    }
    let scalars = base_scalars(&manifest, service_count);
    let mut total_rendered = 0usize;
    let mut total_copied = 0usize;
    for lang_name in &langs {
        let lang_tmpl_dir = templates_root.join(lang_name);
        let lang_out_dir = Path::new(out_dir).join(lang_name);
        let mut lang_scalars = scalars.clone();
        lang_scalars.push(("LANG".to_string(), lang_name.clone()));
        match render_language(
            &lang_tmpl_dir,
            &lang_out_dir,
            &manifest,
            &entities,
            &lang_scalars,
        ) {
            Ok((rendered, copied)) => {
                total_rendered += rendered;
                total_copied += copied;
                fsm.note(format!(
                    "{lang_name}: {rendered} rendered, {copied} copied → {}",
                    lang_out_dir.to_string_lossy().replace('\\', "/")
                ));
            }
            Err(err) => return fsm.fail(format!("{lang_name}: {err}")),
        }

        // B3: for `--project-proto` + Go, also emit a typed proto<->record
        // marshalling file INTO the consumer's package. Rendered directly (not via
        // the string-template engine) because it must aggregate file-level Go
        // imports of the consumer's own proto packages — which the per-entity block
        // expander cannot do. This is what deletes the hand-written marshalling
        // (protorecord, udb_record/udb_helpers, and the `map[string]any{…}` in each
        // repository).
        if selector.project_proto.is_some() && lang_name == "go" {
            let go_pkg = selector.go_package.as_deref().unwrap_or("udbentities");
            // Render + gofmt-canonicalize BEFORE writing, so a fail-closed (#3) or a
            // formatter failure (#8) leaves any prior file intact.
            let content = match render_go_entities_file_canonical(&entities, go_pkg) {
                Ok(content) => content,
                Err(err) => return fsm.fail(err),
            };
            let file_path = lang_out_dir.join("udb_entities_gen.go");
            if let Err(err) = std::fs::create_dir_all(&lang_out_dir)
                .and_then(|_| std::fs::write(&file_path, content))
            {
                return fsm.fail(format!("failed to write {}: {err}", file_path.display()));
            }
            total_rendered += 1;
            fsm.note(format!(
                "go: typed entity marshalling → {}",
                file_path.to_string_lossy().replace('\\', "/")
            ));
        }
    }

    // ── Render ─▶ Completed ─────────────────────────────────────────────────
    if fsm.go(SdkGenState::Completed).is_err() {
        return 1;
    }
    println!(
        "\nsdk generate {} — {total_rendered} file(s) rendered, {total_copied} copied across \
         {} language(s).\nRaw proto stubs are produced separately by `buf generate`.",
        fsm.state.as_str(),
        langs.len()
    );
    0
}

/// B3: render a self-contained Go file of TYPED marshalling for the consumer's
/// entities. Emits, per entity, `<Entity>ToUDBRecord(*T) map[string]any` and
/// `<Entity>FromUDBRow(row) (*T, error)`, plus a small set of once-per-file
/// coercion helpers. Derived entirely from [`EntityColumnDescriptor`] (B2). This
/// replaces a consumer's hand-written proto<->record conversion. Correctness of
/// the GENERATED Go is verified by the litmus compile (`go build` on the output);
/// the Rust here is the emitter.
///
/// Returns `Err` (FAILS CLOSED) when an entity has a NOT NULL column the emitter
/// cannot marshal — currently a NOT NULL cross-package enum (#3) — rather than
/// silently omitting it and emitting a record the broker rejects.
fn render_go_entities_file(entities: &[EntityDescriptor], package: &str) -> Result<String, String> {
    // First pass: which entities are renderable + what imports/helpers are needed.
    let mut proto_imports: BTreeMap<String, String> = BTreeMap::new();
    let mut needs_time = false;
    let mut needs_timestamppb = false;
    let mut needs_strings = false;
    let mut renderable: Vec<(&EntityDescriptor, String, String)> = Vec::new();
    for entity in entities {
        let Some(go_type) = entity.language_classes.get("go") else {
            continue;
        };
        let Some((path, alias, type_name)) = parse_go_type(go_type) else {
            continue;
        };
        proto_imports.insert(path, alias.clone());
        for column in &entity.columns {
            // Mirror the emit rules exactly: a column that emits no marshalling
            // (injected audit column, repeated field) must not count toward the
            // imports, or the generated file carries an unused import.
            if !column.declared_in_proto || column.is_array {
                continue;
            }
            if is_go_timestamp(&column.proto_type) {
                needs_time = true;
                needs_timestamppb = true;
            }
            // `strings` is only needed by the SAME-package enum write path
            // (strings.TrimPrefix); a cross-package enum is skipped, so counting it
            // would import `strings` for a file that never uses it.
            if !column.enum_values.is_empty() && !column.enum_cross_package {
                needs_strings = true;
            }
        }
        renderable.push((entity, alias, type_name));
    }

    // #3: FAIL CLOSED before emitting anything — a NOT NULL column the emitter
    // cannot marshal (currently a NOT NULL cross-package enum) would be silently
    // omitted from the record, and the broker would reject the resulting INSERT.
    // A nullable such column is fine (it round-trips as NULL) and is skipped with a
    // TODO instead.
    for (entity, _, _) in &renderable {
        for column in &entity.columns {
            if column.enum_cross_package
                && column.not_null
                && column.declared_in_proto
                && !column.exclude_from_insert
            {
                return Err(format!(
                    "entity `{}`: column `{}` is a NOT NULL cross-package enum ({}); the Go entity \
                     generator cannot yet qualify an enum type across proto packages, so omitting \
                     it would emit a record the broker rejects. Move the enum into the entity's \
                     proto package, make the column nullable/optional, or annotate the column with \
                     a co-located enum.",
                    entity.short_name, column.field_name, column.proto_type,
                ));
            }
        }
    }

    // W3 (tip 9): the semantic input stamp — a sha256 over the canonical
    // serialization of the (sorted) entity descriptors. Regenerating from
    // identical protos is byte-identical; any proto change moves the hash. The
    // udb version line is informational (protoc-gen-go's lesson: don't make
    // toolchain bumps churn every file — the HASH is the contract).
    let manifest_hash = {
        use sha2::{Digest, Sha256};
        let mut sorted: Vec<&EntityDescriptor> = entities.iter().collect();
        sorted.sort_by(|a, b| a.message_type.cmp(&b.message_type));
        let canonical = serde_json::to_string(&sorted).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(canonical.as_bytes());
        format!("{:x}", hasher.finalize())
    };

    let mut out = String::new();
    out.push_str(&format!(
        "// Code generated by `udb sdk generate --project-proto`. DO NOT EDIT.\n\
         //\n\
         // udb {} · project-proto manifest sha256: {manifest_hash}\n\
         // Verify freshness in CI: `udb sdk diff --project-proto <dir> --against <this file>`\n\
         //\n",
        env!("CARGO_PKG_VERSION"),
    ));
    out.push_str(
        "// Typed UDB record marshalling for your entities. Replaces hand-written\n\
         // proto <-> map[string]any conversion (e.g. protorecord, udb_record.go,\n\
         // udb_helpers.go, and the `map[string]any{…}` in each repository).\n\
         //\n\
         // NULL handling — read this before using the full-record write path:\n\
         //   * proto3 `optional` fields have explicit presence (a pointer). Unset\n\
         //     means the key is OMITTED from the record, so the column keeps its\n\
         //     SQL NULL.\n\
         //   * A NULLABLE string WITHOUT `optional` cannot express \"unset\" in Go —\n\
         //     \"\" and unset are the same value. To avoid silently replacing SQL\n\
         //     NULL with \"\" (unique indexes treat two \"\"s as duplicates while two\n\
         //     NULLs coexist, and format CHECKs reject \"\"), an empty string is\n\
         //     OMITTED from the record too.\n\
         //   * So: to store a genuine empty string, declare the field `optional`\n\
         //     (or NOT NULL). Otherwise \"\" is indistinguishable from NULL here.\n\n",
    );
    out.push_str(&format!("package {package}\n\n"));

    // Imports (std first, then the consumer's proto packages, deterministic order).
    // The always-emitted coercion helpers reference encoding/base64 (bytes),
    // encoding/json (json.Number / JSON columns), fmt (fail-closed errors) and
    // strconv (unsigned parse), so those std imports are always present and used.
    out.push_str("import (\n");
    out.push_str("\t\"context\"\n");
    out.push_str("\t\"encoding/base64\"\n");
    out.push_str("\t\"encoding/json\"\n");
    out.push_str("\t\"fmt\"\n");
    out.push_str("\t\"strconv\"\n");
    if needs_strings {
        out.push_str("\t\"strings\"\n");
    }
    if needs_time {
        out.push_str("\t\"time\"\n");
    }
    out.push_str("\n\t\"github.com/fahara02/udb/sdk/go/udbclient\"\n");
    if !proto_imports.is_empty() || needs_timestamppb {
        out.push('\n');
    }
    if needs_timestamppb {
        out.push_str("\t\"google.golang.org/protobuf/types/known/timestamppb\"\n");
    }
    for (path, alias) in &proto_imports {
        out.push_str(&format!("\t{alias} \"{path}\"\n"));
    }
    out.push_str(")\n\n");

    for (entity, alias, type_name) in &renderable {
        let qualified = format!("{alias}.{type_name}");
        // ── ToUDBRecord (write path) ──────────────────────────────────────────
        out.push_str(&format!(
            "// {name}ToUDBRecord converts a *{qualified} into a UDB record.\n\
             func {name}ToUDBRecord(m *{qualified}) map[string]any {{\n\
             \tif m == nil {{\n\t\treturn nil\n\t}}\n\tr := map[string]any{{}}\n",
            name = entity.short_name,
        ));
        for column in &entity.columns {
            // Broker-injected audit columns (created_at/…) have no getter on the
            // consumer's message — the broker fills their defaults server-side.
            if !column.declared_in_proto {
                continue;
            }
            out.push_str(&go_to_record_stmt(column));
        }
        out.push_str("\treturn r\n}\n\n");

        // ── FromUDBRow (read path) ────────────────────────────────────────────
        out.push_str(&format!(
            "// {name}FromUDBRow converts a UDB row into a *{qualified}. A value that\n\
             // cannot be decoded into its message field fails the read (the row is\n\
             // rejected) instead of silently decoding to a zero value.\n\
             func {name}FromUDBRow(row map[string]any) (*{qualified}, error) {{\n\
             \tm := &{qualified}{{}}\n",
            name = entity.short_name,
        ));
        for column in &entity.columns {
            if !column.declared_in_proto {
                continue;
            }
            out.push_str(&go_from_row_stmt(column, alias));
        }
        out.push_str("\treturn m, nil\n}\n\n");
    }

    // W3: machine-checkable stamp — code can assert the generated layer
    // matches the protos it was built from.
    out.push_str(&format!(
        "// GeneratedManifestHash is the sha256 of the entity manifest this file\n\
         // was generated from (see the header). Assert it in tests to catch a\n\
         // stale generated layer at build time.\n\
         const GeneratedManifestHash = \"{manifest_hash}\"\n\n",
    ));

    // W6 (tip 8): the column-policy table AS DATA — the same facts the
    // marshalling above bakes into behavior, published for consumer policy
    // code and linters (Prisma-DMMF-style). One struct type per file.
    // Emitted WITHOUT hand-alignment padding (single space after each field name):
    // the write-site gofmt canonicalization (#8) column-aligns the fields, so
    // reproducing gofmt's alignment here by hand would only risk drifting from it.
    out.push_str(
        "// UDBColumn describes one column's write/read policy facts, generated from\n\
         // the proto + manifest truth. Consumers build policy-shaped writes on\n\
         // this instead of maintaining tribal knowledge per repository.\n\
         type UDBColumn struct {\n\
         \tSQLType string\n\
         \tNotNull bool\n\
         \tHasPresence bool\n\
         \tIsArray bool\n\
         \tJSON bool\n\
         \tJSONB bool\n\
         \tExcludeFromInsert bool\n\
         \tPII bool\n\
         \tEncrypted bool\n\
         \t// BlindIndex names the filterable HMAC sibling column (empty = none).\n\
         \tBlindIndex string\n\
         \tEnumTokens []string\n\
         \t// DeclaredInProto is false for broker-injected audit columns — they\n\
         \t// have no getter and are server-populated.\n\
         \tDeclaredInProto bool\n\
         }\n\n",
    );
    for (entity, _, _) in &renderable {
        out.push_str(&format!(
            "// {name}Columns is the column-policy table for {name}.\n\
             var {name}Columns = map[string]UDBColumn{{\n",
            name = entity.short_name,
        ));
        for column in &entity.columns {
            let blind_index = if column.is_blind_index {
                String::new()
            } else {
                let idx = format!("{}_idx", column.column_name);
                if entity
                    .columns
                    .iter()
                    .any(|c| c.column_name == idx && c.is_blind_index)
                {
                    idx
                } else {
                    String::new()
                }
            };
            let tokens = if column.enum_values.is_empty() {
                "nil".to_string()
            } else {
                format!(
                    "[]string{{{}}}",
                    column
                        .enum_values
                        .iter()
                        .map(|token| format!("{token:?}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
            out.push_str(&format!(
                "\t{key:?}: {{SQLType: {sql:?}, NotNull: {not_null}, HasPresence: {presence}, \
                 IsArray: {array}, JSON: {json}, JSONB: {jsonb}, ExcludeFromInsert: {excl}, \
                 PII: {pii}, Encrypted: {enc}, BlindIndex: {bidx:?}, EnumTokens: {tokens}, \
                 DeclaredInProto: {declared}}},\n",
                key = column.field_name,
                sql = column.sql_type,
                not_null = column.not_null,
                presence = column.has_presence,
                array = column.is_array,
                json = column.is_json,
                jsonb = column.is_jsonb,
                excl = column.exclude_from_insert,
                pii = column.is_pii,
                enc = column.is_encrypted,
                bidx = blind_index,
                declared = column.declared_in_proto,
            ));
        }
        out.push_str("}\n\n");
    }

    // W8 (tip 6): typed repositories on the new surface — List/Get compose
    // SelectPage/Select with the typed decode above; the guarded writes
    // delegate to the SDK's CAS verbs. Kills the hand-written per-entity
    // data-access layer (filters, paging, tenant threading, CAS plumbing).
    out.push_str("// ── Typed repositories (generated) ───────────────────────────────────\n\n");
    for (entity, alias, type_name) in &renderable {
        let name = &entity.short_name;
        let qualified = format!("{alias}.{type_name}");
        let keys = entity
            .primary_keys
            .iter()
            .map(|k| format!("{k:?}"))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!(
"// {name}Repo is a typed data-access layer for {name}: paged typed lists,\n\
// typed point reads, and CAS-guarded writes — zero hand-written glue.\n\
type {name}Repo struct {{ E *udbclient.Entity }}\n\n\
// New{name}Repo binds a UDB client to typed {name} access.\n\
func New{name}Repo(c *udbclient.Client) {name}Repo {{\n\
\treturn {name}Repo{{E: c.Entity({fqn:?}, udbclient.Key({keys}))}}\n\
}}\n\n\
// List pages through rows matching where, decoded to typed messages. A row that\n\
// fails to decode fails the whole page (no partially-zeroed results).\n\
func (r {name}Repo) List(ctx context.Context, where map[string]any, opts udbclient.PageOptions) ([]*{qualified}, string, int64, error) {{\n\
\tpage, err := r.E.SelectPage(ctx, where, opts)\n\
\tif err != nil {{\n\t\treturn nil, \"\", 0, err\n\t}}\n\
\tout := make([]*{qualified}, 0, len(page.Rows))\n\
\tfor _, row := range page.Rows {{\n\
\t\titem, err := {name}FromUDBRow(row)\n\
\t\tif err != nil {{\n\t\t\treturn nil, \"\", 0, err\n\t\t}}\n\
\t\tout = append(out, item)\n\t}}\n\
\treturn out, page.NextPageToken, int64(page.TotalCount), nil\n\
}}\n\n\
// Get returns the single row identified by its FULL primary key. where must pin\n\
// every primary-key column by equality; a partial key is rejected before the RPC\n\
// (use FindFirst for an arbitrary-filter lookup). Returns (nil, nil) when absent,\n\
// and a cardinality error if the key somehow matches more than one row.\n\
func (r {name}Repo) Get(ctx context.Context, where map[string]any) (*{qualified}, error) {{\n\
\tfor _, pk := range []string{{{keys}}} {{\n\
\t\tif _, ok := where[pk]; !ok {{\n\
\t\t\treturn nil, fmt.Errorf({get_pk_msg:?}, pk)\n\t\t}}\n\t}}\n\
\tpage, err := r.E.SelectPage(ctx, where, udbclient.PageOptions{{Limit: 2}})\n\
\tif err != nil {{\n\t\treturn nil, err\n\t}}\n\
\tif len(page.Rows) == 0 {{\n\t\treturn nil, nil\n\t}}\n\
\tif len(page.Rows) > 1 {{\n\
\t\treturn nil, fmt.Errorf({get_card_msg:?}, len(page.Rows))\n\t}}\n\
\treturn {name}FromUDBRow(page.Rows[0])\n\
}}\n\n\
// FindFirst returns the first row matching an ARBITRARY filter (not necessarily\n\
// the primary key), or (nil, nil) when none match. Use Get for a primary-key\n\
// point read with a cardinality guarantee.\n\
func (r {name}Repo) FindFirst(ctx context.Context, where map[string]any) (*{qualified}, error) {{\n\
\tpage, err := r.E.SelectPage(ctx, where, udbclient.PageOptions{{Limit: 1}})\n\
\tif err != nil {{\n\t\treturn nil, err\n\t}}\n\
\tif len(page.Rows) == 0 {{\n\t\treturn nil, nil\n\t}}\n\
\treturn {name}FromUDBRow(page.Rows[0])\n\
}}\n\n\
// UpdateGuarded is a CAS partial update: SET only changes on the rows matched\n\
// by where, only if expected still equals the current row. An empty expected map\n\
// is rejected before the RPC — a guarded update must never silently degrade into\n\
// an unconditional one (use Update for that).\n\
func (r {name}Repo) UpdateGuarded(ctx context.Context, where, changes, expected map[string]any) error {{\n\
\tif len(expected) == 0 {{\n\
\t\treturn fmt.Errorf({upd_cas_msg:?})\n\t}}\n\
\t_, err := r.E.Update(ctx, where, changes, udbclient.WithUpdateExpected(expected))\n\
\treturn err\n\
}}\n\n\
// DeleteGuarded deletes the matched row only if expected still equals it. An\n\
// empty expected map is rejected before the RPC (use Delete for an unconditional\n\
// delete).\n\
func (r {name}Repo) DeleteGuarded(ctx context.Context, where, expected map[string]any) error {{\n\
\tif len(expected) == 0 {{\n\
\t\treturn fmt.Errorf({del_cas_msg:?})\n\t}}\n\
\t_, err := r.E.Delete(ctx, where, udbclient.WithDeleteExpected(expected))\n\
\treturn err\n\
}}\n\n",
            fqn = entity.message_type,
            get_pk_msg = format!(
                "{name}.Get: primary key %q missing from filter; use FindFirst for a partial-key lookup"
            ),
            get_card_msg = format!(
                "{name}.Get: primary key matched %d rows, expected at most 1"
            ),
            upd_cas_msg = format!(
                "{name}.UpdateGuarded: expected precondition is empty; refusing to run an unconditional update"
            ),
            del_cas_msg = format!(
                "{name}.DeleteGuarded: expected precondition is empty; refusing to run an unconditional delete"
            ),
        ));
    }

    out.push_str(&go_coercion_helpers(needs_time));
    Ok(out)
}

/// Render the typed Go entity file AND canonicalize it with gofmt (#8). Used by
/// the `udb sdk generate --project-proto` write path and `udb sdk diff` so both
/// compare/emit byte-identical, gofmt-canonical output. A gofmt failure (a broken
/// toolchain, or — a generator bug — Go the formatter rejects) surfaces as a
/// generation error, leaving any previously-written file intact.
fn render_go_entities_file_canonical(
    entities: &[EntityDescriptor],
    package: &str,
) -> Result<String, String> {
    let raw = render_go_entities_file(entities, package)?;
    gofmt_canonicalize(&raw)
}

/// Canonicalize Go source through `gofmt` (reads a temp file, returns the
/// formatted bytes). Errors when `gofmt` is unavailable or rejects the input.
fn gofmt_canonicalize(src: &str) -> Result<String, String> {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let path = std::env::temp_dir().join(format!("udb-gofmt-{}-{stamp}.go", std::process::id()));
    std::fs::write(&path, src).map_err(|err| format!("gofmt: write temp file: {err}"))?;
    let result = ProcessCommand::new("gofmt").arg(&path).output();
    let _ = std::fs::remove_file(&path);
    let output = result.map_err(|err| {
        format!(
            "gofmt is required to emit the typed Go entity layer but could not be run ({err}); \
             install the Go toolchain (gofmt ships with it) and re-run"
        )
    })?;
    if !output.status.success() {
        return Err(format!(
            "gofmt rejected the generated Go (this is a udb generator bug — please report it): {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8(output.stdout).map_err(|err| format!("gofmt: non-UTF-8 output: {err}"))
}

/// Parse an `EntityDescriptor` Go type token `"import/path;alias.TypeName"` into
/// `(import_path, alias, TypeName)`.
fn parse_go_type(go_type: &str) -> Option<(String, String, String)> {
    let (path, rest) = go_type.split_once(';')?;
    let (alias, type_name) = rest.rsplit_once('.')?;
    if path.trim().is_empty() || alias.trim().is_empty() || type_name.trim().is_empty() {
        return None;
    }
    Some((path.to_string(), alias.to_string(), type_name.to_string()))
}

/// The Go enum type name from a proto type token (its last dotted segment):
/// `acme.authn.entity.v1.UserStatus` -> `UserStatus`, `UserStatus` -> `UserStatus`.
fn go_enum_type_name(proto_type: &str) -> &str {
    proto_type
        .trim_start_matches('.')
        .rsplit('.')
        .next()
        .unwrap_or(proto_type)
}

/// Whether a proto type token denotes a `google.protobuf.Timestamp`.
fn is_go_timestamp(proto_type: &str) -> bool {
    proto_type
        .trim_start_matches('.')
        .ends_with("google.protobuf.Timestamp")
        || proto_type == "Timestamp"
}

/// Camel-case a proto field name into its Go identifier EXACTLY as
/// `protoc-gen-go` does (`google.golang.org/protobuf/internal/strs.GoCamelCase`),
/// so the emitted accessors match the consumer's compiled protobuf Go. This is a
/// faithful byte-for-byte port of the upstream algorithm — proto field names are
/// ASCII (`[A-Za-z_][A-Za-z0-9_]*`), so byte and char processing coincide.
///
/// V23-1: the previous `split('_')` + first-char-uppercase approximation only
/// capitalized the first letter of each underscore segment, so it mishandled
/// letter/digit boundaries WITHIN a segment. A digit is its own word, so the
/// letter that FOLLOWS a digit is uppercased: `proj4text` -> `Proj4Text` (not
/// `Proj4text`), `checksum_sha256` -> `ChecksumSha256`, `s3_configured` ->
/// `S3Configured`. `protoc-gen-go` emits `GetProj4Text`; the old code emitted the
/// non-existent `GetProj4text`, so the generated Go did not compile.
fn go_pascal(field_name: &str) -> String {
    fn is_ascii_lower(c: u8) -> bool {
        c.is_ascii_lowercase()
    }
    let s = field_name.as_bytes();
    let mut b: Vec<u8> = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        let c = s[i];
        if c == b'.' && i + 1 < s.len() && is_ascii_lower(s[i + 1]) {
            // Skip '.' in ".{lowercase}".
        } else if c == b'.' {
            b.push(b'_'); // convert '.' to '_'
        } else if c == b'_' && (i == 0 || s[i - 1] == b'.') {
            // Leading '_' (or '_' after '.') -> 'X', so we start with a capital.
            b.push(b'X');
        } else if c == b'_' && i + 1 < s.len() && is_ascii_lower(s[i + 1]) {
            // Skip '_' in "_{lowercase}".
        } else if c.is_ascii_digit() {
            b.push(c); // digits are their own word
        } else {
            // A letter word: uppercase its first char, then take the trailing
            // lowercase run verbatim. (A '_' that is neither leading nor followed
            // by a lowercase letter falls here and is preserved as-is, matching
            // the upstream `default` case.)
            let up = if is_ascii_lower(c) {
                c - (b'a' - b'A')
            } else {
                c
            };
            b.push(up);
            while i + 1 < s.len() && is_ascii_lower(s[i + 1]) {
                b.push(s[i + 1]);
                i += 1;
            }
        }
        i += 1;
    }
    // Input is ASCII, so the byte buffer is always valid UTF-8.
    String::from_utf8(b).unwrap_or_else(|_| field_name.to_string())
}

/// Longest common `UPPER_SNAKE_` prefix of an enum's value names (e.g.
/// `USER_STATUS_` from `USER_STATUS_ACTIVE`/`USER_STATUS_SUSPENDED`), so the
/// generated code can trim it to the DB short token (`ACTIVE`).
pub(crate) fn enum_common_prefix(values: &[String]) -> String {
    if values.is_empty() {
        return String::new();
    }
    let first = &values[0];
    let mut end = first.len();
    for value in &values[1..] {
        let common = first
            .bytes()
            .zip(value.bytes())
            .take_while(|(a, b)| a == b)
            .count();
        end = end.min(common);
    }
    // Trim back to the last `_` so we cut whole segments, not mid-token.
    let prefix = &first[..end];
    match prefix.rfind('_') {
        Some(idx) => first[..=idx].to_string(),
        None => String::new(),
    }
}

/// The `r["field"] = …` statement for one column (proto -> record).
///
/// THE SYMMETRY RULE: this function must never write a column that
/// [`go_from_row_stmt`] cannot read back. An unsupported column (repeated
/// field, message type, unresolved enum) is omitted with a TODO on BOTH sides —
/// a raw `m.Get…()` write of an unsupported Go value silently corrupts the
/// column (enum → JSON number in a VARCHAR CHECK, message → struct into a
/// numeric column).
fn go_to_record_stmt(column: &EntityColumnDescriptor) -> String {
    let key = &column.field_name;
    let field = go_pascal(&column.field_name);
    let getter = format!("Get{field}");
    if column.is_array {
        // Repeated fields (TEXT[] etc.) have no scalar round-trip yet; skipped
        // on write AND read.
        return format!(
            "\t// TODO(udb-b3): unsupported write for \"{key}\" (repeated {}) — field skipped\n",
            column.proto_type
        );
    }
    if is_go_timestamp(&column.proto_type) {
        // timestamppb is already a pointer, so nil covers unset for both the
        // presence and non-presence cases. A DATE column takes the date-only
        // form — RFC3339Nano fails a strict DATE bind.
        let layout = if column.sql_type.trim().eq_ignore_ascii_case("DATE") {
            "\"2006-01-02\"".to_string()
        } else {
            "time.RFC3339Nano".to_string()
        };
        return format!(
            "\tif v := m.{getter}(); v != nil {{\n\
             \t\tr[\"{key}\"] = v.AsTime().UTC().Format({layout})\n\t}}\n",
        );
    }
    if !column.enum_values.is_empty() {
        if column.enum_cross_package {
            // A NOT NULL cross-package enum already failed closed in the render
            // pre-scan; a nullable one lands here and is skipped (NULL is legal).
            return format!(
                "\t// TODO(udb-b3): unsupported write for \"{key}\" (cross-package enum {}; \
                 Go import unavailable) — field skipped\n",
                column.proto_type
            );
        }
        let prefix = enum_common_prefix(&column.enum_values);
        let expr = format!("strings.TrimPrefix(m.{getter}().String(), \"{prefix}\")");
        if column.has_presence {
            return format!("\tif m.{field} != nil {{\n\t\tr[\"{key}\"] = {expr}\n\t}}\n");
        }
        return format!("\tr[\"{key}\"] = {expr}\n");
    }
    // #10: a JSON/JSONB textual column embeds the PARSED JSON (json.RawMessage) so
    // the record is not double-encoded — record_json is built with json.Marshal,
    // which embeds a RawMessage verbatim (and validates it, rejecting malformed
    // JSON) instead of string-escaping the whole document as `r[key] = string`
    // would. An unset/empty nullable value is omitted so the column stays NULL.
    if column.is_json || column.is_jsonb {
        if column.has_presence {
            return format!(
                "\tif m.{field} != nil {{\n\t\tr[\"{key}\"] = json.RawMessage(m.{getter}())\n\t}}\n"
            );
        }
        if !column.not_null {
            return format!(
                "\tif v := m.{getter}(); v != \"\" {{\n\t\tr[\"{key}\"] = json.RawMessage(v)\n\t}}\n"
            );
        }
        return format!("\tr[\"{key}\"] = json.RawMessage(m.{getter}())\n");
    }
    if matches!(go_scalar_kind(&column.proto_type), GoScalar::Unknown) {
        // Message-typed (or unresolved-enum) column: no scalar round-trip.
        // Skipped on write AND read — go_from_row_stmt emits the matching TODO.
        // Checked BEFORE the presence arm: an `optional` message field would
        // otherwise write a raw struct pointer.
        return format!(
            "\t// TODO(udb-b3): unsupported write for \"{key}\" ({}) — field skipped\n",
            column.proto_type
        );
    }
    // #11: optional `bytes` — []byte is already nilable, so nil means unset (omit →
    // SQL NULL) while a present empty []byte{} is a real value to write. The
    // general presence arm below cannot express this (it guards a scalar pointer),
    // and the old fall-through wrote `null` for an unset optional bytes.
    if column.has_presence && matches!(go_scalar_kind(&column.proto_type), GoScalar::Bytes) {
        return format!("\tif m.{field} != nil {{\n\t\tr[\"{key}\"] = m.{getter}()\n\t}}\n");
    }
    // proto3 `optional` (non-bytes) renders as a pointer: an unset field must be
    // OMITTED from the record, never flattened to a zero value.
    if column.has_presence {
        return format!("\tif m.{field} != nil {{\n\t\tr[\"{key}\"] = m.{getter}()\n\t}}\n");
    }
    // Nullable string WITHOUT presence: Go cannot distinguish "" from unset, and
    // writing "" where the row held SQL NULL changes meaning — a unique index
    // treats two ""s as duplicates while two NULLs coexist, and format CHECKs
    // reject "". Omit the key so NULL round-trips as NULL. A column that must be
    // written as an empty string should be declared `optional` (presence) or
    // NOT NULL.
    if !column.not_null && matches!(go_scalar_kind(&column.proto_type), GoScalar::String) {
        return format!("\tif v := m.{getter}(); v != \"\" {{\n\t\tr[\"{key}\"] = v\n\t}}\n");
    }
    // Scalars (int*/bool/bytes/float, and NOT NULL strings) map straight into `any`.
    format!("\tr[\"{key}\"] = m.{getter}()\n")
}

/// The `m.Field = …` statement for one column (record -> proto). `alias` is the
/// entity's Go package alias, used to qualify enum types (which share the entity's
/// package in the common co-located-proto case).
fn go_from_row_stmt(column: &EntityColumnDescriptor, alias: &str) -> String {
    let key = &column.field_name;
    let field = go_pascal(&column.field_name);
    // The `decode column %q: %w` wrapper, shared by every fail-closed arm.
    let fail = |indent: &str| {
        format!("{indent}return nil, fmt.Errorf(\"decode column %q: %w\", \"{key}\", err)\n")
    };
    if column.is_array {
        // Mirrors go_to_record_stmt's repeated-field skip (symmetry rule).
        return format!(
            "\t// TODO(udb-b3): unsupported read for \"{key}\" (repeated {}) — field skipped\n",
            column.proto_type
        );
    }
    if is_go_timestamp(&column.proto_type) {
        // A malformed timestamp string now fails the row (#5); nil/empty leaves the
        // field unset (ok=false, no error).
        return format!(
            "\tif t, ok, err := udbAsTime(row[\"{key}\"]); err != nil {{\n{fail}\
             \t}} else if ok {{\n\
             \t\tm.{field} = timestamppb.New(t)\n\t}}\n",
            fail = fail("\t\t"),
        );
    }
    if !column.enum_values.is_empty() {
        if column.enum_cross_package {
            // A NOT NULL cross-package enum already failed closed in the render
            // pre-scan; a nullable one lands here and is skipped (NULL is legal).
            return format!(
                "\t// TODO(udb-b3): unsupported read for \"{key}\" (cross-package enum {}; \
                 Go import unavailable) — field skipped\n",
                column.proto_type
            );
        }
        // The DB stores the enum SHORT token (the write path trims the prefix);
        // read it back through protoc-gen-go's `<Type>_value` map. Assumes the
        // enum's Go type is in the entity's package — the common case, since
        // co-located entity/enum protos share a `go_package`.
        let enum_type = go_enum_type_name(&column.proto_type);
        let prefix = enum_common_prefix(&column.enum_values);
        // W9 dual-read tolerance: probe the SHORT token first (the wire
        // convention), then the FULL enum name — so readers deployed before a
        // legacy-data `migrate-enum-tokens` rewrite decode both forms.
        if column.has_presence {
            return format!(
                "\tif s, err := udbAsString(row[\"{key}\"]); err != nil {{\n{fail}\
                 \t}} else if s != \"\" {{\n\
                 \t\tif v, ok := {alias}.{enum_type}_value[\"{prefix}\"+s]; ok {{\n\
                 \t\t\te := {alias}.{enum_type}(v)\n\t\t\tm.{field} = &e\n\
                 \t\t}} else if v, ok := {alias}.{enum_type}_value[s]; ok {{\n\
                 \t\t\te := {alias}.{enum_type}(v)\n\t\t\tm.{field} = &e\n\t\t}}\n\t}}\n",
                fail = fail("\t\t"),
            );
        }
        return format!(
            "\tif s, err := udbAsString(row[\"{key}\"]); err != nil {{\n{fail}\
             \t}} else if s != \"\" {{\n\
             \t\tif v, ok := {alias}.{enum_type}_value[\"{prefix}\"+s]; ok {{\n\
             \t\t\tm.{field} = {alias}.{enum_type}(v)\n\
             \t\t}} else if v, ok := {alias}.{enum_type}_value[s]; ok {{\n\
             \t\t\tm.{field} = {alias}.{enum_type}(v)\n\t\t}}\n\t}}\n",
            fail = fail("\t\t"),
        );
    }
    // #10: a JSON/JSONB textual column serializes the structured value back to
    // canonical JSON text rather than reading it as an opaque string (which for a
    // structured value would be its Go %v form, not JSON).
    if column.is_json || column.is_jsonb {
        let assign = if column.has_presence {
            format!("\t\tm.{field} = &encoded\n")
        } else {
            format!("\t\tm.{field} = encoded\n")
        };
        return format!(
            "\tif raw, ok := row[\"{key}\"]; ok && raw != nil {{\n\
             \t\tencoded, err := udbAsJSONText(raw)\n\
             \t\tif err != nil {{\n{fail}\t\t}}\n{assign}\t}}\n",
            fail = fail("\t\t\t"),
        );
    }
    let kind = go_scalar_kind(&column.proto_type);
    if matches!(kind, GoScalar::Unknown) {
        // Message-typed (or unresolved-enum) column: no scalar round-trip.
        return format!(
            "\t// TODO(udb-b3): unsupported read for \"{key}\" ({}) — field skipped\n",
            column.proto_type
        );
    }
    // (coercer, cast-open, cast-close) for the value the coercer yields.
    let (coercer, cast_open, cast_close) = match kind {
        GoScalar::String => ("udbAsString", "", ""),
        GoScalar::Bool => ("udbAsBool", "", ""),
        GoScalar::Int32 => ("udbAsInt64", "int32(", ")"),
        GoScalar::Int64 => ("udbAsInt64", "", ""),
        GoScalar::Uint32 => ("udbAsUint64", "uint32(", ")"),
        GoScalar::Uint64 => ("udbAsUint64", "", ""),
        GoScalar::Float32 => ("udbAsFloat64", "float32(", ")"),
        GoScalar::Float64 => ("udbAsFloat64", "", ""),
        GoScalar::Bytes => ("udbAsBytes", "", ""),
        GoScalar::Unknown => unreachable!("Unknown handled above"),
    };
    // proto3 `optional` scalars are pointers: only assign when the row actually
    // carried the key, so an absent column stays nil rather than becoming a zero.
    // (`bytes` is already nilable and keeps the plain-slice form.)
    if column.has_presence && !matches!(kind, GoScalar::Bytes) {
        return format!(
            "\tif raw, ok := row[\"{key}\"]; ok && raw != nil {{\n\
             \t\tval, err := {coercer}(raw)\n\
             \t\tif err != nil {{\n{fail}\t\t}}\n\
             \t\tv := {cast_open}val{cast_close}\n\t\tm.{field} = &v\n\t}}\n",
            fail = fail("\t\t\t"),
        );
    }
    // Non-presence (and bytes): block-scoped coerce + assign, so a bad value fails
    // the row instead of decoding to a zero (#5).
    format!(
        "\t{{\n\
         \t\tval, err := {coercer}(row[\"{key}\"])\n\
         \t\tif err != nil {{\n{fail}\t\t}}\n\
         \t\tm.{field} = {cast_open}val{cast_close}\n\t}}\n",
        fail = fail("\t\t\t"),
    )
}

#[derive(Clone, Copy)]
enum GoScalar {
    String,
    Bool,
    Int32,
    Int64,
    Uint32,
    Uint64,
    Float32,
    Float64,
    Bytes,
    Unknown,
}

fn go_scalar_kind(proto_type: &str) -> GoScalar {
    match proto_type.trim_start_matches('.') {
        "string" => GoScalar::String,
        "bool" => GoScalar::Bool,
        "int32" | "sint32" | "sfixed32" => GoScalar::Int32,
        "int64" | "sint64" | "sfixed64" => GoScalar::Int64,
        // #12: unsigned/fixed proto scalars map to Go uint32/uint64 — folding them
        // into the signed kinds emitted `int32(...)`/`int64(...)`, which does not
        // compile against a `uint32`/`uint64` protobuf field.
        "uint32" | "fixed32" => GoScalar::Uint32,
        "uint64" | "fixed64" => GoScalar::Uint64,
        "float" => GoScalar::Float32,
        "double" => GoScalar::Float64,
        "bytes" => GoScalar::Bytes,
        _ => GoScalar::Unknown,
    }
}

/// Once-per-file safe `any` -> typed coercion helpers used by `…FromUDBRow`.
///
/// Every coercer returns `(T, error)` and FAILS CLOSED on a type mismatch instead
/// of returning a zero value: a bad row must fail the read, not silently decode to
/// a zero (#5). Numbers arrive as `json.Number` (the SDK's `decodeRecordJSON`
/// decodes with `UseNumber` to preserve integer precision), so the integer/float
/// coercers parse the exact digits and reject overflow / fractional / sign errors
/// (#9/#12). A `nil` (absent/NULL column) is NOT an error — it yields the zero
/// value with a nil error so an absent column leaves the field unset.
fn go_coercion_helpers(needs_time: bool) -> String {
    let mut out = String::new();
    out.push_str(
        "func udbAsString(v any) (string, error) {\n\tswitch s := v.(type) {\n\tcase nil:\n\t\treturn \"\", nil\n\tcase string:\n\t\treturn s, nil\n\t}\n\treturn \"\", fmt.Errorf(\"expected string, got %T\", v)\n}\n\n",
    );
    out.push_str(
        "func udbAsBool(v any) (bool, error) {\n\tswitch b := v.(type) {\n\tcase nil:\n\t\treturn false, nil\n\tcase bool:\n\t\treturn b, nil\n\t}\n\treturn false, fmt.Errorf(\"expected bool, got %T\", v)\n}\n\n",
    );
    // #9: json.Number.Int64 rejects a fractional or out-of-range value; the float64
    // arm (legacy/non-UseNumber sources) rejects a non-integral value and any value
    // outside int64 (int64(n) round-trips only when the value is representable).
    out.push_str(
        "func udbAsInt64(v any) (int64, error) {\n\tswitch n := v.(type) {\n\tcase nil:\n\t\treturn 0, nil\n\tcase int64:\n\t\treturn n, nil\n\tcase int32:\n\t\treturn int64(n), nil\n\tcase int:\n\t\treturn int64(n), nil\n\tcase json.Number:\n\t\ti, err := n.Int64()\n\t\tif err != nil {\n\t\t\treturn 0, fmt.Errorf(\"expected 64-bit integer, got %q: %w\", n.String(), err)\n\t\t}\n\t\treturn i, nil\n\tcase float64:\n\t\tif i := int64(n); float64(i) == n {\n\t\t\treturn i, nil\n\t\t}\n\t\treturn 0, fmt.Errorf(\"expected integer, got non-integral or out-of-range %v\", n)\n\t}\n\treturn 0, fmt.Errorf(\"expected integer, got %T\", v)\n}\n\n",
    );
    // #12: unsigned coercer. json.Number is parsed with ParseUint so a negative or
    // overflowing value is rejected; the signed arms reject a negative source.
    out.push_str(
        "func udbAsUint64(v any) (uint64, error) {\n\tswitch n := v.(type) {\n\tcase nil:\n\t\treturn 0, nil\n\tcase uint64:\n\t\treturn n, nil\n\tcase uint32:\n\t\treturn uint64(n), nil\n\tcase uint:\n\t\treturn uint64(n), nil\n\tcase int64:\n\t\tif n < 0 {\n\t\t\treturn 0, fmt.Errorf(\"expected unsigned integer, got negative %d\", n)\n\t\t}\n\t\treturn uint64(n), nil\n\tcase int32:\n\t\tif n < 0 {\n\t\t\treturn 0, fmt.Errorf(\"expected unsigned integer, got negative %d\", n)\n\t\t}\n\t\treturn uint64(n), nil\n\tcase int:\n\t\tif n < 0 {\n\t\t\treturn 0, fmt.Errorf(\"expected unsigned integer, got negative %d\", n)\n\t\t}\n\t\treturn uint64(n), nil\n\tcase json.Number:\n\t\tu, err := strconv.ParseUint(n.String(), 10, 64)\n\t\tif err != nil {\n\t\t\treturn 0, fmt.Errorf(\"expected 64-bit unsigned integer, got %q: %w\", n.String(), err)\n\t\t}\n\t\treturn u, nil\n\tcase float64:\n\t\tif n < 0 {\n\t\t\treturn 0, fmt.Errorf(\"expected unsigned integer, got negative %v\", n)\n\t\t}\n\t\tif u := uint64(n); float64(u) == n {\n\t\t\treturn u, nil\n\t\t}\n\t\treturn 0, fmt.Errorf(\"expected unsigned integer, got non-integral or out-of-range %v\", n)\n\t}\n\treturn 0, fmt.Errorf(\"expected unsigned integer, got %T\", v)\n}\n\n",
    );
    out.push_str(
        "func udbAsFloat64(v any) (float64, error) {\n\tswitch n := v.(type) {\n\tcase nil:\n\t\treturn 0, nil\n\tcase float64:\n\t\treturn n, nil\n\tcase float32:\n\t\treturn float64(n), nil\n\tcase int64:\n\t\treturn float64(n), nil\n\tcase int:\n\t\treturn float64(n), nil\n\tcase json.Number:\n\t\tf, err := n.Float64()\n\t\tif err != nil {\n\t\t\treturn 0, fmt.Errorf(\"expected number, got %q: %w\", n.String(), err)\n\t\t}\n\t\treturn f, nil\n\t}\n\treturn 0, fmt.Errorf(\"expected number, got %T\", v)\n}\n\n",
    );
    // #2: a bytes column round-trips through record_json as a base64 STRING
    // (json.Marshal encodes []byte that way), so decode it — the old `[]byte(s)`
    // returned the base64 text verbatim as the field value.
    out.push_str(
        "func udbAsBytes(v any) ([]byte, error) {\n\tswitch b := v.(type) {\n\tcase nil:\n\t\treturn nil, nil\n\tcase []byte:\n\t\treturn b, nil\n\tcase string:\n\t\tdecoded, err := base64.StdEncoding.DecodeString(b)\n\t\tif err != nil {\n\t\t\treturn nil, fmt.Errorf(\"expected base64-encoded bytes: %w\", err)\n\t\t}\n\t\treturn decoded, nil\n\t}\n\treturn nil, fmt.Errorf(\"expected bytes, got %T\", v)\n}\n\n",
    );
    // #10: a JSON/JSONB column comes back as a structured value (map/slice/number)
    // OR as JSON text; canonicalize either into JSON text for the string field.
    out.push_str(
        "func udbAsJSONText(v any) (string, error) {\n\tswitch s := v.(type) {\n\tcase nil:\n\t\treturn \"\", nil\n\tcase string:\n\t\tif s == \"\" {\n\t\t\treturn \"\", nil\n\t\t}\n\t\tif !json.Valid([]byte(s)) {\n\t\t\treturn \"\", fmt.Errorf(\"expected JSON text, got %q\", s)\n\t\t}\n\t\treturn s, nil\n\t}\n\tb, err := json.Marshal(v)\n\tif err != nil {\n\t\treturn \"\", fmt.Errorf(\"encode JSON value: %w\", err)\n\t}\n\treturn string(b), nil\n}\n\n",
    );
    if needs_time {
        out.push_str(
            // The broker renders timestamps in more than one layout across column
            // types (offset form for TIMESTAMPTZ, space-separated for some
            // TIMESTAMP renderings, bare dates for DATE). Try the canonical form
            // first, then fall back through the observed set. A non-empty string
            // that matches NO layout is a decode error (fail closed), not a silent
            // zero; nil/empty means the column was absent/NULL (ok=false, no error).
            "func udbAsTime(v any) (time.Time, bool, error) {\n\tif v == nil {\n\t\treturn time.Time{}, false, nil\n\t}\n\ts, ok := v.(string)\n\tif !ok {\n\t\treturn time.Time{}, false, fmt.Errorf(\"expected timestamp string, got %T\", v)\n\t}\n\tif s == \"\" {\n\t\treturn time.Time{}, false, nil\n\t}\n\tfor _, layout := range []string{\n\t\ttime.RFC3339Nano,\n\t\ttime.RFC3339,\n\t\t\"2006-01-02T15:04:05\",\n\t\t\"2006-01-02 15:04:05.999999999Z07:00\",\n\t\t\"2006-01-02 15:04:05\",\n\t\t\"2006-01-02\",\n\t} {\n\t\tif t, err := time.Parse(layout, s); err == nil {\n\t\t\treturn t, true, nil\n\t\t}\n\t}\n\treturn time.Time{}, false, fmt.Errorf(\"unrecognized timestamp format %q\", s)\n}\n",
        );
    }
    out
}

/// Apply the `udb sdk generate` selectors to the full RPC manifest.
///
/// Order: validate `--service` names, then keep an RPC iff it satisfies every
/// active selector. With an all-default [`SdkSelector`] this returns a clone of
/// the input (identical to historical behavior).
///
/// `--include-deps`: the descriptor manifest exposes no explicit inter-service
/// dependency edges (services declare backend `requires_*`, not "service A
/// needs service B"), so a service's only derivable "dependency set" is itself.
/// We therefore treat `--include-deps` as an accepted, documented no-op rather
/// than fabricating an edge that proto does not assert. If proto later gains a
/// `depends_on_service` option, expand `selected_services` here.
///
/// `--strict-server-capabilities`: drop RPCs marked `internal_grpc_only` — a
/// generated client speaking the public/control-plane channel cannot reach the
/// loopback-only listener, so emitting a typed wrapper for it would be a
/// capability lie.
fn apply_selectors(
    manifest: &[RpcDescriptor],
    selector: &SdkSelector,
) -> Result<Vec<RpcDescriptor>, String> {
    // Validate --service against the known native/logical service IDs.
    if !selector.services.is_empty() {
        let known: BTreeSet<String> = manifest
            .iter()
            .flat_map(|rpc| {
                [
                    rpc.native_service_id.clone(),
                    rpc.logical_service_id.clone(),
                ]
            })
            .filter(|id| !id.is_empty())
            .collect();
        let unknown: Vec<&String> = selector
            .services
            .iter()
            .filter(|id| !known.contains(*id))
            .collect();
        if !unknown.is_empty() {
            let mut sorted: Vec<&String> = known.iter().collect();
            sorted.sort();
            return Err(format!(
                "unknown --service {:?}; known services: {}",
                unknown,
                sorted
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }

    let selected: Vec<RpcDescriptor> = manifest
        .iter()
        .filter(|rpc| selector_matches(rpc, selector))
        .cloned()
        .collect();
    Ok(selected)
}

fn selector_matches(rpc: &RpcDescriptor, selector: &SdkSelector) -> bool {
    if let Some(surface) = &selector.surface {
        if !rpc_matches_surface(rpc, surface) {
            return false;
        }
    }
    if !selector.services.is_empty()
        && !selector
            .services
            .iter()
            .any(|id| id == &rpc.native_service_id || id == &rpc.logical_service_id)
    {
        return false;
    }
    if selector.native_only && rpc.native_service_id.is_empty() {
        return false;
    }
    if selector.strict_server_capabilities && rpc.internal_grpc_only {
        return false;
    }
    true
}

/// Resolve the languages to generate. `all` → every subdir of `templates_root`;
/// otherwise just the requested one (validated to exist).
fn resolve_langs(templates_root: &Path, lang: &str) -> Result<Vec<String>, String> {
    if lang == "all" {
        let mut langs = Vec::new();
        let entries = std::fs::read_dir(templates_root).map_err(|e| e.to_string())?;
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                langs.push(entry.file_name().to_string_lossy().to_string());
            }
        }
        langs.sort();
        Ok(langs)
    } else if templates_root.join(lang).is_dir() {
        Ok(vec![lang.to_string()])
    } else {
        Ok(Vec::new())
    }
}

/// master-plan 10.6: the per-backend ORM capability tier, projected at SDK
/// generation time from [`udb::backend::BackendKind::orm_tier`] over the single
/// authoritative [`udb::backend::BackendKind::ALL`] variant enumeration. Emitted
/// as a `{ "<backend token>": "<orm tier>" }` object so it can be embedded as a
/// build-time constant the SDK ORM layer feature-gates on (GetCapabilities needs
/// admin_scope and is not reachable by an ORM client at runtime). DERIVED, never
/// a parallel `OrmTier` enum.
fn backend_orm_tiers_json() -> serde_json::Value {
    let map: serde_json::Map<String, serde_json::Value> = udb::backend::BackendKind::ALL
        .iter()
        .map(|kind| {
            (
                kind.as_str().to_string(),
                serde_json::Value::from(kind.orm_tier()),
            )
        })
        .collect();
    serde_json::Value::Object(map)
}

/// master-plan 10.4: the per-backend transaction honesty role, projected at SDK
/// generation time from [`udb::backend::BackendKind::role`]. This is the same
/// [`udb::backend::BackendRole`] source the runtime uses; SDK UnitOfWork helpers
/// embed it so they can fail closed before claiming a BeginTx transaction on a
/// projection-only backend.
fn backend_roles_json() -> serde_json::Value {
    let map: serde_json::Map<String, serde_json::Value> = udb::backend::BackendKind::ALL
        .iter()
        .map(|kind| {
            (
                kind.as_str().to_string(),
                serde_json::Value::from(kind.role().as_str()),
            )
        })
        .collect();
    serde_json::Value::Object(map)
}

fn backend_roles_java_literal() -> String {
    let entries = udb::backend::BackendKind::ALL
        .iter()
        .map(|kind| {
            format!(
                "Map.entry(\"{}\", \"{}\")",
                kind.as_str(),
                kind.role().as_str()
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("Map.ofEntries({entries})")
}

fn backend_orm_tiers_java_literal() -> String {
    let entries = udb::backend::BackendKind::ALL
        .iter()
        .map(|kind| format!("Map.entry(\"{}\", \"{}\")", kind.as_str(), kind.orm_tier()))
        .collect::<Vec<_>>()
        .join(", ");
    format!("Map.ofEntries({entries})")
}

fn json_string_literal(value: &serde_json::Value) -> String {
    serde_json::to_string(&value.to_string()).unwrap_or_else(|_| "\"{}\"".to_string())
}

/// Scalar substitutions common to every language.
fn base_scalars(manifest: &[RpcDescriptor], service_count: usize) -> Vec<(String, String)> {
    vec![
        (
            "UDB_VERSION".to_string(),
            env!("CARGO_PKG_VERSION").to_string(),
        ),
        (
            "PROTOCOL_VERSION".to_string(),
            udb::runtime::native_catalog::protocol_version().to_string(),
        ),
        ("RPC_COUNT".to_string(), manifest.len().to_string()),
        ("SERVICE_COUNT".to_string(), service_count.to_string()),
        // master-plan 10.6: embed the per-backend ORM capability tier as a
        // build-time constant. `BackendKind::orm_tier()` DERIVES it from the
        // storage tier (`BackendTier`, the single tier source of truth) — there
        // is no parallel enum. We embed at generation time because the runtime
        // `GetCapabilities` RPC requires admin_scope, so an ORM client cannot
        // read the tier itself; the SDK feature-gates ORM capabilities (e.g.
        // eager `.include()` is only valid for `"relational"`) on this map.
        (
            "ORM_TIERS".to_string(),
            backend_orm_tiers_json().to_string(),
        ),
        (
            "ORM_TIERS_STRING".to_string(),
            json_string_literal(&backend_orm_tiers_json()),
        ),
        (
            "ORM_TIERS_JAVA".to_string(),
            backend_orm_tiers_java_literal(),
        ),
        (
            "BACKEND_ROLES".to_string(),
            backend_roles_json().to_string(),
        ),
        (
            "BACKEND_ROLES_STRING".to_string(),
            json_string_literal(&backend_roles_json()),
        ),
        (
            "BACKEND_ROLES_JAVA".to_string(),
            backend_roles_java_literal(),
        ),
        (
            "GENERATED_NOTE".to_string(),
            "Generated by `udb sdk generate` from the embedded proto descriptor set. \
             Edit the template under sdk-templates/<lang>/, not this file."
                .to_string(),
        ),
    ]
}

/// Walk one language's template dir, rendering/copying each file. Returns
/// `(rendered_count, copied_count)`.
fn render_language(
    tmpl_dir: &Path,
    out_dir: &Path,
    manifest: &[RpcDescriptor],
    entities: &[EntityDescriptor],
    scalars: &[(String, String)],
) -> Result<(usize, usize), String> {
    let mut files: Vec<PathBuf> = Vec::new();
    collect_files(tmpl_dir, &mut files).map_err(|e| e.to_string())?;
    files.sort();

    let mut rendered = 0usize;
    let mut copied = 0usize;
    for src in &files {
        let rel = src.strip_prefix(tmpl_dir).map_err(|e| e.to_string())?;
        let rel_str = rel.to_string_lossy().replace('\\', "/");
        if should_skip(&rel_str) {
            continue;
        }

        if rel_str.ends_with(".tmpl") {
            let raw =
                std::fs::read_to_string(src).map_err(|e| format!("read {}: {e}", src.display()))?;
            let body = render_text(&raw, manifest, entities, scalars);
            if let Some(token) = first_unresolved_template_token(&body) {
                return Err(format!(
                    "{} rendered unresolved template token {token}",
                    rel_str
                ));
            }
            let dest_rel = rel_str.trim_end_matches(".tmpl");
            let dest = out_dir.join(dest_rel);
            write_file(&dest, body.as_bytes())?;
            rendered += 1;
        } else {
            let bytes = std::fs::read(src).map_err(|e| format!("read {}: {e}", src.display()))?;
            let dest = out_dir.join(&rel_str);
            write_file(&dest, &bytes)?;
            copied += 1;
        }
    }
    Ok((rendered, copied))
}

/// Files the generator never emits into the SDK tree: per-lang config, dotfiles,
/// and template-dir documentation (`README.md`/`TEMPLATES.md` under
/// `sdk-templates/<lang>/` explain the template itself — emitting them would
/// clobber the SDK's own README).
fn should_skip(rel: &str) -> bool {
    let name = rel.rsplit('/').next().unwrap_or(rel);
    matches!(
        name,
        "sdkgen.yaml" | "sdkgen.toml" | "README.md" | "TEMPLATES.md"
    ) || name.starts_with('.')
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, out)?;
        } else {
            out.push(path);
        }
    }
    Ok(())
}

fn write_file(dest: &Path, contents: &[u8]) -> Result<(), String> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    std::fs::write(dest, contents).map_err(|e| format!("write {}: {e}", dest.display()))
}

// ── Rendering engine ────────────────────────────────────────────────────────

/// Render a template: expand per-RPC/per-service/per-entity blocks, apply
/// global non-entity defaults, then substitute scalar placeholders across the
/// whole result.
fn render_text(
    template: &str,
    manifest: &[RpcDescriptor],
    entities: &[EntityDescriptor],
    scalars: &[(String, String)],
) -> String {
    let expanded = expand_blocks(template, manifest, entities);
    let with_defaults = apply_global_template_defaults(&expanded);
    apply_scalars(&with_defaults, scalars)
}

fn apply_global_template_defaults(text: &str) -> String {
    const EMPTY_GLOBALS: [&str; 6] = [
        "{{ENTITY_TS_RELATION_ACCESSORS}}",
        "{{ENTITY_PY_RELATION_ACCESSORS}}",
        "{{ENTITY_GO_RELATION_ACCESSORS}}",
        "{{ENTITY_CSHARP_RELATION_ACCESSORS}}",
        "{{ENTITY_JAVA_RELATION_ACCESSORS}}",
        "{{ENTITY_PHP_RELATION_ACCESSORS}}",
    ];
    let mut out = text.to_string();
    for token in EMPTY_GLOBALS {
        out = out.replace(token, "");
    }
    out
}

fn first_unresolved_template_token(text: &str) -> Option<String> {
    let mut rest = text;
    while let Some(start) = rest.find("{{") {
        let after_start = &rest[start..];
        let Some(end) = after_start.find("}}") else {
            return Some(after_start.chars().take(48).collect());
        };
        let token = &after_start[..end + 2];
        // Template docs intentionally mention the entity placeholder family.
        if token != "{{ENTITY_*}}" {
            return Some(token.to_string());
        }
        rest = &after_start[end + 2..];
    }
    None
}

const RPC_BEGIN: &str = "@@UDB_RPC_BEGIN";
const RPC_END: &str = "@@UDB_RPC_END";
const SVC_BEGIN: &str = "@@UDB_SERVICE_BEGIN";
const SVC_END: &str = "@@UDB_SERVICE_END";
const ENTITY_BEGIN: &str = "@@UDB_ENTITY_BEGIN";
const ENTITY_END: &str = "@@UDB_ENTITY_END";

/// Expand all template blocks: service blocks first (recursing into any RPC
/// blocks NESTED inside each service body, scoped to that service), then any
/// remaining top-level RPC blocks. Nesting matters because the idiomatic shape
/// is "one client class per service, one method per RPC" — i.e. an RPC block
/// inside a service block.
fn expand_blocks(text: &str, manifest: &[RpcDescriptor], entities: &[EntityDescriptor]) -> String {
    let after_services = expand_service_blocks(text, manifest);
    let after_entities = expand_entity_blocks(&after_services, entities);
    expand_rpc_blocks(&after_entities, manifest)
}

/// Expand `@@UDB_SERVICE_BEGIN…@@UDB_SERVICE_END` blocks once per service. Each
/// per-service body has its `{{SERVICE_*}}` placeholders substituted (so a nested
/// `service={{SERVICE_NAME}}` filter becomes concrete) and is then run through
/// [`expand_rpc_blocks`], so RPC blocks nested inside a service expand against
/// that one service. Lines outside service blocks (including top-level RPC
/// blocks) are copied verbatim for the later pass.
fn expand_service_blocks(text: &str, manifest: &[RpcDescriptor]) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0usize;
    while i < lines.len() {
        let line = lines[i];
        if let Some(filter) = marker_filter(line, SVC_BEGIN) {
            let (body, next) = collect_body(&lines, i + 1, SVC_END);
            for svc in services_of(manifest)
                .into_iter()
                .filter(|s| service_matches(s, &filter))
            {
                let with_service = substitute_service(&body, &svc, manifest);
                out.push_str(&expand_rpc_blocks(&with_service, manifest));
            }
            i = next;
        } else {
            out.push_str(line);
            out.push('\n');
            i += 1;
        }
    }
    out
}

/// Expand `@@UDB_RPC_BEGIN…@@UDB_RPC_END` blocks once per matching RPC.
fn expand_rpc_blocks(text: &str, manifest: &[RpcDescriptor]) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0usize;
    while i < lines.len() {
        let line = lines[i];
        if let Some(filter) = marker_filter(line, RPC_BEGIN) {
            let (body, next) = collect_body(&lines, i + 1, RPC_END);
            for rpc in manifest.iter().filter(|r| rpc_matches(r, &filter)) {
                out.push_str(&substitute_rpc(&body, rpc, manifest));
            }
            i = next;
        } else {
            out.push_str(line);
            out.push('\n');
            i += 1;
        }
    }
    out
}

/// Expand `@@UDB_ENTITY_BEGIN…@@UDB_ENTITY_END` blocks once per entity.
fn expand_entity_blocks(text: &str, entities: &[EntityDescriptor]) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0usize;
    while i < lines.len() {
        let line = lines[i];
        if line.contains(ENTITY_BEGIN) {
            let (body, next) = collect_body(&lines, i + 1, ENTITY_END);
            for entity in entities {
                out.push_str(&substitute_entity(&body, entity));
            }
            i = next;
        } else {
            out.push_str(line);
            out.push('\n');
            i += 1;
        }
    }
    out
}

/// If `line` contains `token`, return the remainder of the line after the token
/// (the optional filter), trimmed. Otherwise `None`.
fn marker_filter(line: &str, token: &str) -> Option<String> {
    line.find(token)
        .map(|idx| line[idx + token.len()..].trim().to_string())
}

/// Collect block-body lines starting at `start` until a line containing
/// `end_token`. Returns the body (each line newline-terminated) and the index
/// just past the end marker. If the end marker is absent, consumes to EOF.
fn collect_body(lines: &[&str], start: usize, end_token: &str) -> (String, usize) {
    let mut body = String::new();
    let mut i = start;
    while i < lines.len() {
        if lines[i].contains(end_token) {
            return (body, i + 1);
        }
        body.push_str(lines[i]);
        body.push('\n');
        i += 1;
    }
    (body, i)
}

/// Parse a `key=value` filter string into matchers. Supported keys: `service`,
/// `kind`, `surface` (`public`|`control_plane`|`peer`, matched against the
/// owning service's listener flags), `auth` (the RPC's `auth_mode`), and
/// `native_service` (the RPC's `native_service_id` or `logical_service_id`).
/// Unknown tokens are ignored.
struct BlockFilter {
    service: Option<String>,
    kind: Option<String>,
    surface: Option<String>,
    auth: Option<String>,
    native_service: Option<String>,
}

fn parse_filter(filter: &str) -> BlockFilter {
    let mut service = None;
    let mut kind = None;
    let mut surface = None;
    let mut auth = None;
    let mut native_service = None;
    for token in filter.split_whitespace() {
        if let Some(v) = token.strip_prefix("service=") {
            service = Some(v.to_string());
        } else if let Some(v) = token.strip_prefix("kind=") {
            kind = Some(v.to_string());
        } else if let Some(v) = token.strip_prefix("surface=") {
            surface = Some(v.to_string());
        } else if let Some(v) = token.strip_prefix("auth=") {
            auth = Some(v.to_string());
        } else if let Some(v) = token.strip_prefix("native_service=") {
            native_service = Some(v.to_string());
        }
    }
    BlockFilter {
        service,
        kind,
        surface,
        auth,
        native_service,
    }
}

/// Whether `rpc`'s owning service may bind the named listener surface
/// (`public`|`control_plane`|`peer`).
fn rpc_matches_surface(rpc: &RpcDescriptor, surface: &str) -> bool {
    match surface {
        "public" => rpc.public_listener_allowed,
        "control_plane" => rpc.control_plane_listener_allowed,
        "peer" => rpc.peer_listener_allowed,
        _ => false,
    }
}

fn rpc_matches(rpc: &RpcDescriptor, filter: &str) -> bool {
    let f = parse_filter(filter);
    if let Some(svc) = &f.service {
        if &rpc.service_name != svc && &rpc.service_full() != svc {
            return false;
        }
    }
    if let Some(kind) = &f.kind {
        if rpc.kind() != kind {
            return false;
        }
    }
    if let Some(surface) = &f.surface {
        if !rpc_matches_surface(rpc, surface) {
            return false;
        }
    }
    if let Some(auth) = &f.auth {
        if &rpc.auth_mode != auth {
            return false;
        }
    }
    if let Some(native) = &f.native_service {
        if &rpc.native_service_id != native && &rpc.logical_service_id != native {
            return false;
        }
    }
    true
}

#[derive(Clone)]
struct ServiceInfo {
    name: String,
    pkg: String,
}

impl ServiceInfo {
    fn full(&self) -> String {
        format!("{}.{}", self.pkg, self.name)
    }
}

fn services_of(manifest: &[RpcDescriptor]) -> Vec<ServiceInfo> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for rpc in manifest {
        let full = rpc.service_full();
        if seen.insert(full) {
            out.push(ServiceInfo {
                name: rpc.service_name.clone(),
                pkg: rpc.service_pkg.clone(),
            });
        }
    }
    out
}

fn service_matches(svc: &ServiceInfo, filter: &str) -> bool {
    let f = parse_filter(filter);
    match &f.service {
        Some(s) => &svc.name == s || &svc.full() == s,
        None => true,
    }
}

fn substitute_rpc(body: &str, rpc: &RpcDescriptor, manifest: &[RpcDescriptor]) -> String {
    let alias_snake = rpc_alias_snake(rpc);
    let alias_camel = rpc_alias_camel(rpc);
    let alias_pascal = rpc_alias_pascal(rpc);
    let php_method_camel = php_rpc_method_camel(rpc, manifest, &alias_camel);
    let php_method_alias_entries =
        php_method_alias_entries(rpc, manifest, &alias_snake, &php_method_camel);
    let rest_operation_id = if rpc.rest_operation_id.trim().is_empty() {
        alias_camel.clone()
    } else {
        rpc.rest_operation_id.clone()
    };
    let pairs: [(&str, String); 36] = [
        ("{{RPC_NAME}}", rpc.method.clone()),
        ("{{RPC_WIRE_NAME}}", rpc.method.clone()),
        ("{{RPC_SNAKE}}", rpc.method_snake.clone()),
        ("{{RPC_ALIAS_SNAKE}}", alias_snake.clone()),
        ("{{RPC_ALIAS_CAMEL}}", alias_camel.clone()),
        ("{{RPC_ALIAS_PASCAL}}", alias_pascal.clone()),
        ("{{REST_OPERATION_ID}}", rest_operation_id.clone()),
        ("{{RPC_HTTP_METHOD}}", rpc.http_verb.clone()),
        ("{{RPC_HTTP_PATH}}", rpc.http_path.clone()),
        // Compatibility placeholders used by older templates during migration.
        ("{{RPC_WIRE_PATH}}", rpc.grpc_path()),
        ("{{RPC_API_ALIAS}}", alias_snake.clone()),
        ("{{RPC_OPERATION_ID}}", rest_operation_id),
        ("{{RPC_METHOD_ALIAS_SNAKE}}", alias_snake),
        ("{{RPC_METHOD_ALIAS_CAMEL}}", alias_camel),
        ("{{RPC_METHOD_ALIAS_PASCAL}}", alias_pascal),
        ("{{PHP_RPC_METHOD_CAMEL}}", php_method_camel),
        ("{{RPC_INPUT}}", rpc.input_short.clone()),
        ("{{RPC_INPUT_PKG}}", rpc.input_pkg.clone()),
        ("{{RPC_OUTPUT}}", rpc.output_short.clone()),
        ("{{RPC_OUTPUT_PKG}}", rpc.output_pkg.clone()),
        ("{{RPC_CLIENT_STREAMING}}", rpc.client_streaming.to_string()),
        ("{{RPC_SERVER_STREAMING}}", rpc.server_streaming.to_string()),
        ("{{RPC_KIND}}", rpc.kind().to_string()),
        ("{{RPC_PATH}}", rpc.grpc_path()),
        ("{{SERVICE_NAME}}", rpc.service_name.clone()),
        ("{{SERVICE_PKG}}", rpc.service_pkg.clone()),
        ("{{SERVICE_FULL}}", rpc.service_full()),
        ("{{RPC_CSRF_REQUIRED}}", rpc.csrf_required.to_string()),
        (
            "{{RPC_INTERNAL_GRPC_ONLY}}",
            rpc.internal_grpc_only.to_string(),
        ),
        (
            "{{RPC_PUBLIC_LISTENER}}",
            rpc.public_listener_allowed.to_string(),
        ),
        (
            "{{RPC_CONTROL_PLANE_LISTENER}}",
            rpc.control_plane_listener_allowed.to_string(),
        ),
        (
            "{{RPC_PEER_LISTENER}}",
            rpc.peer_listener_allowed.to_string(),
        ),
        ("{{RPC_READ_ONLY}}", rpc.read_only.to_string()),
        ("{{RPC_OPERATION_KIND}}", rpc.operation_kind.clone()),
        ("{{RPC_REPLAY_SAFE}}", rpc.replay_safe.to_string()),
        ("{{PHP_METHOD_ALIAS_ENTRIES}}", php_method_alias_entries),
    ];
    let mut text = body.to_string();
    for (key, value) in &pairs {
        text = text.replace(key, value);
    }
    text
}

fn rpc_alias_value(rpc: &RpcDescriptor) -> &str {
    if rpc.method_alias.trim().is_empty() {
        rpc.method.as_str()
    } else {
        rpc.method_alias.as_str()
    }
}

fn rpc_alias_snake(rpc: &RpcDescriptor) -> String {
    if rpc.method_alias_snake.trim().is_empty() {
        alias_snake_case(rpc_alias_value(rpc))
    } else {
        rpc.method_alias_snake.clone()
    }
}

fn rpc_alias_camel(rpc: &RpcDescriptor) -> String {
    if rpc.method_alias_camel.trim().is_empty() {
        alias_camel_case(rpc_alias_value(rpc))
    } else {
        rpc.method_alias_camel.clone()
    }
}

fn rpc_alias_pascal(rpc: &RpcDescriptor) -> String {
    if rpc.method_alias_pascal.trim().is_empty() {
        alias_pascal_case(rpc_alias_value(rpc))
    } else {
        rpc.method_alias_pascal.clone()
    }
}

fn same_rpc(left: &RpcDescriptor, right: &RpcDescriptor) -> bool {
    left.service_name == right.service_name
        && left.service_pkg == right.service_pkg
        && left.method == right.method
}

fn php_rpc_method_camel(
    rpc: &RpcDescriptor,
    manifest: &[RpcDescriptor],
    alias_camel: &str,
) -> String {
    let mut duplicate_count = 0usize;
    let mut facade_owner = false;
    for other in manifest
        .iter()
        .filter(|other| rpc_alias_camel(other) == alias_camel)
    {
        duplicate_count += 1;
        if other.service_name == "DataBroker" {
            facade_owner = same_rpc(other, rpc);
        }
    }
    if duplicate_count <= 1 || facade_owner {
        return alias_camel.to_string();
    }
    format!(
        "{}{}",
        alias_camel_case(&rpc.service_name),
        alias_pascal_case(&rpc.method)
    )
}

fn php_alias_owner_is_rpc(manifest: &[RpcDescriptor], alias: &str, rpc: &RpcDescriptor) -> bool {
    let mut best_key: Option<(u8, u8, usize)> = None;
    let mut best_is_rpc = false;
    for (idx, candidate) in manifest.iter().enumerate() {
        let candidate_alias_snake = rpc_alias_snake(candidate);
        let aliases = [
            candidate_alias_snake.as_str(),
            candidate.method_snake.as_str(),
        ];
        if !aliases
            .into_iter()
            .any(|candidate_alias| candidate_alias == alias)
        {
            continue;
        }
        let key = (
            if candidate.service_name == "DataBroker" {
                0
            } else {
                1
            },
            if candidate_alias_snake == alias { 0 } else { 1 },
            idx,
        );
        if match best_key {
            Some(best) => key < best,
            None => true,
        } {
            best_key = Some(key);
            best_is_rpc = same_rpc(candidate, rpc);
        }
    }
    best_is_rpc
}

fn php_method_alias_entries(
    rpc: &RpcDescriptor,
    manifest: &[RpcDescriptor],
    alias_snake: &str,
    php_method_camel: &str,
) -> String {
    let mut seen = BTreeSet::new();
    [alias_snake, rpc.method_snake.as_str()]
        .into_iter()
        .filter(|alias| !alias.trim().is_empty())
        .filter(|alias| seen.insert((*alias).to_string()))
        .filter(|alias| php_alias_owner_is_rpc(manifest, alias, rpc))
        .map(|alias| format!("        \"{alias}\" => \"{php_method_camel}\","))
        .collect::<Vec<_>>()
        .join("\n")
}

fn alias_words(value: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let chars: Vec<char> = value.trim().chars().collect();
    for (idx, ch) in chars.iter().copied().enumerate() {
        if !ch.is_ascii_alphanumeric() {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            continue;
        }
        if !current.is_empty() {
            let prev = current.chars().last().unwrap_or_default();
            let next = chars.get(idx + 1).copied();
            let lower_to_upper = prev.is_ascii_lowercase() && ch.is_ascii_uppercase();
            let acronym_to_word = prev.is_ascii_uppercase()
                && ch.is_ascii_uppercase()
                && next.is_some_and(|n| n.is_ascii_lowercase());
            let alpha_digit = prev.is_ascii_alphabetic() && ch.is_ascii_digit();
            let digit_alpha = prev.is_ascii_digit() && ch.is_ascii_alphabetic();
            if lower_to_upper || acronym_to_word || alpha_digit || digit_alpha {
                words.push(std::mem::take(&mut current));
            }
        }
        current.push(ch);
    }
    if !current.is_empty() {
        words.push(current);
    }
    if words.is_empty() {
        vec![value.trim().to_string()]
    } else {
        words
    }
}

fn title_word(word: &str) -> String {
    let mut chars = word.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let mut out = String::new();
    out.push(first.to_ascii_uppercase());
    for ch in chars {
        out.push(ch.to_ascii_lowercase());
    }
    out
}

fn alias_snake_case(value: &str) -> String {
    alias_words(value)
        .into_iter()
        .map(|word| word.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join("_")
}

fn alias_pascal_case(value: &str) -> String {
    alias_words(value)
        .into_iter()
        .map(|word| title_word(&word))
        .collect::<String>()
}

fn alias_camel_case(value: &str) -> String {
    let words = alias_words(value);
    let mut out = String::new();
    for (idx, word) in words.iter().enumerate() {
        if idx == 0 {
            out.push_str(&word.to_ascii_lowercase());
        } else {
            out.push_str(&title_word(word));
        }
    }
    out
}

fn code_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
}

fn entity_relation_accessors_ts(entity: &EntityDescriptor) -> String {
    entity
        .relations
        .iter()
        .map(|rel| {
            format!(
                "  {}Relation(parent: Record<string, unknown>): Query {{\n    return this.relationQuery({}, parent);\n  }}",
                alias_camel_case(&rel.name),
                code_string(&rel.name)
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn entity_relation_accessors_python(entity: &EntityDescriptor) -> String {
    entity
        .relations
        .iter()
        .map(|rel| {
            format!(
                "    def {}_relation(self, parent: Mapping[str, Any]) -> Query:\n        return self.relation_query({}, parent)",
                alias_snake_case(&rel.name),
                code_string(&rel.name)
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn entity_relation_accessors_go(entity: &EntityDescriptor) -> String {
    entity
        .relations
        .iter()
        .map(|rel| {
            format!(
                "func (r *Repository) {}Relation(parent map[string]any) (*QueryBuilder, error) {{\n\treturn r.RelationQuery({}, parent)\n}}",
                alias_pascal_case(&rel.name),
                code_string(&rel.name)
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn entity_relation_accessors_csharp(entity: &EntityDescriptor) -> String {
    entity
        .relations
        .iter()
        .map(|rel| {
            format!(
                "    public IrQuery {}Relation(IReadOnlyDictionary<string, object?> parent) =>\n        RelationQuery({}, parent);",
                alias_pascal_case(&rel.name),
                code_string(&rel.name)
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn entity_relation_accessors_java(entity: &EntityDescriptor) -> String {
    entity
        .relations
        .iter()
        .map(|rel| {
            format!(
                "    public Query {}Relation(Map<String, Object> parent) {{\n      return relationQuery({}, parent);\n    }}",
                alias_camel_case(&rel.name),
                code_string(&rel.name)
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn entity_relation_accessors_php(entity: &EntityDescriptor) -> String {
    entity
        .relations
        .iter()
        .map(|rel| {
            format!(
                "    public function {}Relation(array $parent): IrQuery\n    {{\n        return $this->relationQuery({}, $parent);\n    }}",
                alias_camel_case(&rel.name),
                code_string(&rel.name)
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn substitute_entity(body: &str, entity: &EntityDescriptor) -> String {
    let primary_keys = entity
        .primary_keys
        .iter()
        .map(|key| format!("\"{key}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let json_fields = entity
        .json_field_names
        .iter()
        .map(|field| format!("\"{field}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let relations_json = serde_json::to_string(&entity.relations).unwrap_or_else(|_| "[]".into());
    let relations_json_string =
        serde_json::to_string(&relations_json).unwrap_or_else(|_| "\"[]\"".into());
    let java_string_list = |values: &[String]| {
        if values.is_empty() {
            "List.of()".to_string()
        } else {
            format!(
                "List.of({})",
                values
                    .iter()
                    .map(|value| code_string(value))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    };
    let java_relations = if entity.relations.is_empty() {
        "List.of()".to_string()
    } else {
        format!(
            "List.of({})",
            entity
                .relations
                .iter()
                .map(|rel| format!(
                    "new EntityRelationBinding({}, {}, {}, {}, {}, {}, {}, {})",
                    code_string(&rel.name),
                    code_string(&rel.kind),
                    java_string_list(&rel.local_fields),
                    code_string(&rel.target_message_type),
                    code_string(&rel.target_table),
                    java_string_list(&rel.target_fields),
                    code_string(&rel.on_delete),
                    code_string(&rel.on_update)
                ))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let short_name = if entity.short_name.trim().is_empty() {
        entity
            .message_type
            .rsplit('.')
            .next()
            .unwrap_or(&entity.message_type)
            .to_string()
    } else {
        entity.short_name.clone()
    };
    let lang = |keys: &[&str], fallback: &str| {
        keys.iter()
            .find_map(|key| entity.language_classes.get(*key))
            .map(|value| value.as_str())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(fallback)
            .to_string()
    };
    let py_import = entity
        .language_classes
        .get("py")
        .or_else(|| entity.language_classes.get("python"))
        .cloned()
        .unwrap_or_default();
    let pairs: [(&str, String); 27] = [
        ("{{ENTITY_MESSAGE_TYPE}}", entity.message_type.clone()),
        ("{{ENTITY_SHORT_NAME}}", short_name.clone()),
        ("{{ENTITY_ALIAS_SNAKE}}", alias_snake_case(&short_name)),
        ("{{ENTITY_ALIAS_CAMEL}}", alias_camel_case(&short_name)),
        ("{{ENTITY_ALIAS_PASCAL}}", alias_pascal_case(&short_name)),
        ("{{ENTITY_TABLE}}", entity.table.clone()),
        ("{{ENTITY_PRIMARY_KEYS}}", primary_keys),
        ("{{ENTITY_JSON_FIELDS}}", json_fields),
        ("{{ENTITY_RELATIONS_JSON}}", relations_json),
        ("{{ENTITY_RELATIONS_JSON_STRING}}", relations_json_string),
        ("{{ENTITY_JAVA_RELATIONS}}", java_relations),
        ("{{ENTITY_TENANT_FIELD}}", entity.tenant_field.clone()),
        ("{{ENTITY_PROJECT_FIELD}}", entity.project_field.clone()),
        (
            "{{ENTITY_SOFT_DELETE_FIELD}}",
            entity.soft_delete_field.clone(),
        ),
        ("{{ENTITY_VERSION_FIELD}}", entity.version_field.clone()),
        (
            "{{ENTITY_TS_TYPE}}",
            lang(&["ts", "typescript"], "Record<string, unknown>"),
        ),
        ("{{ENTITY_GO_TYPE}}", lang(&["go"], "map[string]any")),
        (
            "{{ENTITY_JAVA_TYPE}}",
            lang(&["java"], "Map<String, Object>"),
        ),
        (
            "{{ENTITY_CSHARP_TYPE}}",
            lang(&["csharp", "cs"], "IReadOnlyDictionary<string, object?>"),
        ),
        ("{{ENTITY_PHP_TYPE}}", lang(&["php"], "array")),
        ("{{ENTITY_PY_IMPORT}}", py_import),
        (
            "{{ENTITY_TS_RELATION_ACCESSORS}}",
            entity_relation_accessors_ts(entity),
        ),
        (
            "{{ENTITY_PY_RELATION_ACCESSORS}}",
            entity_relation_accessors_python(entity),
        ),
        (
            "{{ENTITY_GO_RELATION_ACCESSORS}}",
            entity_relation_accessors_go(entity),
        ),
        (
            "{{ENTITY_CSHARP_RELATION_ACCESSORS}}",
            entity_relation_accessors_csharp(entity),
        ),
        (
            "{{ENTITY_JAVA_RELATION_ACCESSORS}}",
            entity_relation_accessors_java(entity),
        ),
        (
            "{{ENTITY_PHP_RELATION_ACCESSORS}}",
            entity_relation_accessors_php(entity),
        ),
    ];
    let mut text = body.to_string();
    for (key, value) in &pairs {
        text = text.replace(key, value);
    }
    text
}

fn substitute_service(body: &str, svc: &ServiceInfo, manifest: &[RpcDescriptor]) -> String {
    let rpc_count = manifest
        .iter()
        .filter(|r| r.service_full() == svc.full())
        .count();
    let pairs: [(&str, String); 4] = [
        ("{{SERVICE_NAME}}", svc.name.clone()),
        ("{{SERVICE_PKG}}", svc.pkg.clone()),
        ("{{SERVICE_FULL}}", svc.full()),
        ("{{SERVICE_RPC_COUNT}}", rpc_count.to_string()),
    ];
    let mut text = body.to_string();
    for (key, value) in &pairs {
        text = text.replace(key, value);
    }
    text
}

fn apply_scalars(text: &str, scalars: &[(String, String)]) -> String {
    let mut out = text.to_string();
    for (key, value) in scalars {
        out = out.replace(&format!("{{{{{key}}}}}"), value);
    }
    out
}

/// Minimal guarded FSM with a step log, identical idiom to `proto_export::Fsm`.
struct Fsm {
    state: SdkGenState,
}

impl Fsm {
    fn new() -> Self {
        Self {
            state: SdkGenState::Start,
        }
    }

    fn go(&mut self, next: SdkGenState) -> Result<(), ()> {
        if self.state.is_terminal() {
            eprintln!(
                "sdk generate: cannot transition out of terminal state {}",
                self.state.as_str()
            );
            return Err(());
        }
        if !self.state.valid_transitions().contains(&next) {
            eprintln!(
                "sdk generate: illegal transition {} → {}",
                self.state.as_str(),
                next.as_str()
            );
            self.state = SdkGenState::Failed;
            return Err(());
        }
        self.state = next;
        println!("[{}]", next.as_str());
        Ok(())
    }

    fn note(&self, message: String) {
        println!("  {message}");
    }

    fn fail(&mut self, reason: String) -> i32 {
        self.state = SdkGenState::Failed;
        eprintln!("[{}] {reason}", SdkGenState::Failed.as_str());
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use udb::runtime::sdk_manifest::EntityRelationDescriptor;

    fn column(field_name: &str, proto_type: &str) -> EntityColumnDescriptor {
        EntityColumnDescriptor {
            field_name: field_name.to_string(),
            column_name: field_name.to_string(),
            proto_type: proto_type.to_string(),
            sql_type: String::new(),
            not_null: true,
            is_array: false,
            has_presence: false,
            enum_values: Vec::new(),
            enum_cross_package: false,
            is_json: false,
            is_jsonb: false,
            exclude_from_insert: false,
            is_blind_index: false,
            is_pii: false,
            is_encrypted: false,
            declared_in_proto: true,
        }
    }

    // proto3 `optional` renders as a Go pointer. Emitting a plain assignment
    // produced code that did not compile (`string` vs `*string`) for any consumer
    // proto using presence.
    #[test]
    fn presence_column_emits_pointer_aware_go() {
        let mut col = column("created_by", "string");
        col.has_presence = true;

        let write = go_to_record_stmt(&col);
        assert!(
            write.contains("if m.CreatedBy != nil {"),
            "presence write must be nil-guarded, got: {write}"
        );

        let read = go_from_row_stmt(&col, "acmev1");
        assert!(
            read.contains("m.CreatedBy = &v"),
            "presence read must assign a pointer, got: {read}"
        );
        assert!(
            read.contains("ok && raw != nil"),
            "presence read must only assign when the row carried the key, got: {read}"
        );
    }

    // A nullable string without presence cannot express "unset" in Go, so writing
    // "" would replace SQL NULL — which unique indexes and CHECK constraints treat
    // very differently. The key must be omitted instead.
    #[test]
    fn nullable_string_without_presence_omits_empty_value() {
        let mut col = column("mobile_number", "string");
        col.not_null = false;
        let write = go_to_record_stmt(&col);
        assert!(
            write.contains("if v := m.GetMobileNumber(); v != \"\" {"),
            "nullable string must omit empty, got: {write}"
        );
    }

    // A NOT NULL string has no NULL to preserve, so it keeps the direct assignment.
    #[test]
    fn not_null_string_writes_directly() {
        let write = go_to_record_stmt(&column("user_id", "string"));
        assert_eq!(write, "\tr[\"user_id\"] = m.GetUserId()\n");
    }

    // Numeric zero is a legitimate value; only strings carry the ""/NULL ambiguity.
    #[test]
    fn nullable_numeric_still_writes_zero() {
        let mut col = column("login_attempts", "int32");
        col.not_null = false;
        let write = go_to_record_stmt(&col);
        assert_eq!(write, "\tr[\"login_attempts\"] = m.GetLoginAttempts()\n");
    }

    #[test]
    fn presence_enum_emits_pointer_aware_go() {
        let mut col = column("status", "acme.authn.entity.v1.UserStatus");
        col.has_presence = true;
        col.enum_values = vec![
            "USER_STATUS_ACTIVE".to_string(),
            "USER_STATUS_SUSPENDED".to_string(),
        ];
        let read = go_from_row_stmt(&col, "authnv1");
        assert!(
            read.contains("e := authnv1.UserStatus(v)") && read.contains("m.Status = &e"),
            "presence enum read must assign a pointer, got: {read}"
        );
    }

    // THE SYMMETRY RULE (V19-1/V19-2): a column the read path cannot decode must
    // not be written either — a raw enum write puts a JSON number into a VARCHAR
    // CHECK column; a raw message write puts a struct into a scalar column.
    #[test]
    fn message_typed_column_is_skipped_on_both_sides() {
        let col = column("wallet_balance", "acme.common.v1.Money");
        let write = go_to_record_stmt(&col);
        assert!(
            write.contains("TODO(udb-b3): unsupported write for \"wallet_balance\""),
            "message-typed write must be a TODO, got: {write}"
        );
        assert!(
            !write.contains("m.GetWalletBalance()"),
            "message-typed column must never be written raw, got: {write}"
        );
        let read = go_from_row_stmt(&col, "acmev1");
        assert!(
            read.contains("TODO(udb-b3): unsupported read for \"wallet_balance\""),
            "message-typed read must be a TODO, got: {read}"
        );
    }

    // `optional` on an unsupported type must not fall into the pointer-guarded
    // raw write — the Unknown check has to run before the presence arm.
    #[test]
    fn optional_message_typed_column_still_skips_write() {
        let mut col = column("wallet_balance", "acme.common.v1.Money");
        col.has_presence = true;
        let write = go_to_record_stmt(&col);
        assert!(
            write.contains("TODO(udb-b3): unsupported write"),
            "optional message column must be a TODO, got: {write}"
        );
        assert!(!write.contains("m.GetWalletBalance()"), "got: {write}");
    }

    // V19-4: `repeated string` hit the scalar-string paths and produced Go that
    // does not compile (`v != ""` on []string). Repeated fields are skipped on
    // both sides until TEXT[] round-trip lands.
    #[test]
    fn repeated_column_is_skipped_on_both_sides() {
        let mut col = column("mfa_methods", "string");
        col.is_array = true;
        let write = go_to_record_stmt(&col);
        assert!(
            write.contains("unsupported write for \"mfa_methods\" (repeated string)"),
            "repeated write must be a TODO, got: {write}"
        );
        assert!(!write.contains("m.GetMfaMethods()"), "got: {write}");
        let read = go_from_row_stmt(&col, "acmev1");
        assert!(
            read.contains("unsupported read for \"mfa_methods\" (repeated string)"),
            "repeated read must be a TODO, got: {read}"
        );
        assert!(!read.contains("udbAsString"), "got: {read}");
    }

    // G-13: a DATE column takes the date-only layout — RFC3339Nano fails a
    // strict DATE bind. TIMESTAMPTZ keeps the full form.
    #[test]
    fn date_column_writes_date_only_layout() {
        let mut col = column("date_of_birth", "google.protobuf.Timestamp");
        col.sql_type = "DATE".to_string();
        let write = go_to_record_stmt(&col);
        assert!(
            write.contains("Format(\"2006-01-02\")"),
            "DATE column must use the date-only layout, got: {write}"
        );
        assert!(!write.contains("RFC3339Nano"), "got: {write}");

        let mut tz = column("created_at", "google.protobuf.Timestamp");
        tz.sql_type = "TIMESTAMPTZ".to_string();
        let write = go_to_record_stmt(&tz);
        assert!(
            write.contains("time.RFC3339Nano"),
            "TIMESTAMPTZ keeps the full layout, got: {write}"
        );
    }

    // V19-1 regression guard: a resolved enum keeps the short-token round-trip
    // (write trims the common prefix; read reconstructs via <Type>_value).
    #[test]
    fn resolved_enum_round_trips_short_tokens() {
        let mut col = column("status", "acme.order.v1.OrderStatus");
        col.enum_values = vec![
            "ORDER_STATUS_ACTIVE".to_string(),
            "ORDER_STATUS_CLOSED".to_string(),
        ];
        let write = go_to_record_stmt(&col);
        assert!(
            write.contains("strings.TrimPrefix(m.GetStatus().String(), \"ORDER_STATUS_\")"),
            "enum write must trim to the short token, got: {write}"
        );
        let read = go_from_row_stmt(&col, "orderv1");
        assert!(
            read.contains("orderv1.OrderStatus_value[\"ORDER_STATUS_\"+s]"),
            "enum read must rebuild the full name, got: {read}"
        );
    }

    // V19-3: broker-injected audit columns have no getter on the consumer's
    // message — marshalling must cover only proto-declared fields, and the
    // import scan must agree (an unused `time` import fails `go build`).
    #[test]
    fn undeclared_columns_are_omitted_from_marshalling_and_imports() {
        let mut language_classes = std::collections::BTreeMap::new();
        language_classes.insert(
            "go".to_string(),
            "example.com/gen/acmev1;acmev1.Profile".to_string(),
        );
        let mut phantom = column("updated_at", "google.protobuf.Timestamp");
        phantom.declared_in_proto = false;
        let entities = vec![EntityDescriptor {
            message_type: "acme.v1.Profile".to_string(),
            short_name: "Profile".to_string(),
            table: "profiles".to_string(),
            primary_keys: vec!["id".to_string()],
            tenant_field: String::new(),
            project_field: String::new(),
            soft_delete_field: String::new(),
            version_field: String::new(),
            proto_package: "acme.v1".to_string(),
            language_classes,
            json_field_names: vec!["id".to_string(), "updated_at".to_string()],
            relations: Vec::new(),
            columns: vec![column("id", "string"), phantom],
        }];
        let out = render_go_entities_file(&entities, "acmegen").expect("render");
        assert!(out.contains("m.GetId()"), "declared field must marshal");
        assert!(
            !out.contains("GetUpdatedAt"),
            "injected audit column must not invent a getter, got:\n{out}"
        );
        assert!(
            !out.contains("\"time\""),
            "no emitted timestamp code ⇒ no time import, got:\n{out}"
        );
    }

    // V23-1: `go_pascal` must reproduce protoc-gen-go's `GoCamelCase` EXACTLY, or
    // the emitted `m.<Field>` / `m.Get<Field>()` accessors don't exist on the
    // consumer's compiled message and the generated Go fails to compile. The old
    // split('_')+first-char approximation broke on letter/digit boundaries within
    // a segment. Each expected value below is verified against a REAL
    // protoc-gen-go getter present in this repo's committed `sdk/go/gen`.
    #[test]
    fn go_pascal_matches_protoc_gen_go_contract() {
        let cases = [
            // The reported regression: no underscore, a digit mid-word — the
            // letter AFTER the digit must capitalize. protoc-gen-go: GetProj4Text.
            ("proj4text", "Proj4Text"),
            // Ordinary snake_case (the cases the old code already handled).
            ("user_id", "UserId"),
            ("id", "Id"),
            ("created_by", "CreatedBy"),
            ("mobile_number", "MobileNumber"),
            // Digit boundaries cross-checked against sdk/go/gen getters:
            ("address_line1", "AddressLine1"), // GetAddressLine1
            ("checksum_sha256", "ChecksumSha256"), // GetChecksumSha256
            ("int64_values", "Int64Values"),   // GetInt64Values
            ("p99_execution_ms", "P99ExecutionMs"), // GetP99ExecutionMs
            ("p50_latency_ms", "P50LatencyMs"), // GetP50LatencyMs
            ("s3_configured", "S3Configured"), // GetS3Configured
            ("sha256", "Sha256"),              // GetSha256
            (
                "overall_p99_compliance_rate",
                "OverallP99ComplianceRate", // GetOverallP99ComplianceRate
            ),
        ];
        for (field, expected) in cases {
            let got = go_pascal(field);
            assert_eq!(
                got, expected,
                "go_pascal({field:?}) = {got:?}, want {expected:?} (protoc-gen-go contract)"
            );
            // The getter the emitter actually writes is `Get<Field>`.
            assert_eq!(format!("Get{got}"), format!("Get{expected}"));
        }
        // Guard the exact defect the bug report cited: never the old wrong form.
        assert_ne!(go_pascal("proj4text"), "Proj4text");
    }

    // V23-1: the typed repository `List` declares an `int64` count return but the
    // SDK's `udbclient.Page.TotalCount` is `int32` — the raw return did not
    // compile (`cannot use page.TotalCount (int32) as int64`). The emitter must
    // widen explicitly, and the digit-field accessors must use the correct names.
    #[test]
    fn typed_list_widens_total_count_and_names_digit_fields() {
        let mut language_classes = std::collections::BTreeMap::new();
        language_classes.insert(
            "go".to_string(),
            "example.com/gen/spatialv1;spatialv1.SpatialRefSys".to_string(),
        );
        let mut proj4 = column("proj4text", "string");
        proj4.not_null = true;
        let entities = vec![EntityDescriptor {
            message_type: "acme.spatial.v1.SpatialRefSys".to_string(),
            short_name: "SpatialRefSys".to_string(),
            table: "spatial_ref_sys".to_string(),
            primary_keys: vec!["srid".to_string()],
            tenant_field: String::new(),
            project_field: String::new(),
            soft_delete_field: String::new(),
            version_field: String::new(),
            proto_package: "acme.spatial.v1".to_string(),
            language_classes,
            json_field_names: vec!["srid".to_string(), "proj4text".to_string()],
            relations: Vec::new(),
            columns: vec![column("srid", "int32"), proj4],
        }];
        let out = render_go_entities_file(&entities, "spatialgen").expect("render");

        // Count type: the return must widen to match the int64 signature.
        assert!(
            out.contains("[]*spatialv1.SpatialRefSys, string, int64, error)"),
            "List must declare the int64 count return, got:\n{out}"
        );
        assert!(
            out.contains("return out, page.NextPageToken, int64(page.TotalCount), nil"),
            "List must widen page.TotalCount to int64, got:\n{out}"
        );
        assert!(
            !out.contains("page.NextPageToken, page.TotalCount, nil"),
            "the un-widened int32 return must not survive, got:\n{out}"
        );

        // Field naming: the digit-boundary accessors must match protoc-gen-go.
        assert!(
            out.contains("m.GetProj4Text()") && out.contains("m.Proj4Text = "),
            "digit-field accessors must be Proj4Text, got:\n{out}"
        );
        assert!(
            !out.contains("Proj4text"),
            "the old wrong Proj4text form must not survive, got:\n{out}"
        );
    }

    /// One renderable `Widget` entity (go language class present) with the given
    /// primary keys and columns — the boilerplate every render-level emitter test
    /// otherwise repeats.
    fn widget_entity(
        primary_keys: Vec<String>,
        columns: Vec<EntityColumnDescriptor>,
    ) -> Vec<EntityDescriptor> {
        let mut language_classes = std::collections::BTreeMap::new();
        language_classes.insert(
            "go".to_string(),
            "example.com/gen/acmev1;acmev1.Widget".to_string(),
        );
        vec![EntityDescriptor {
            message_type: "acme.v1.Widget".to_string(),
            short_name: "Widget".to_string(),
            table: "widgets".to_string(),
            primary_keys,
            tenant_field: String::new(),
            project_field: String::new(),
            soft_delete_field: String::new(),
            version_field: String::new(),
            proto_package: "acme.v1".to_string(),
            language_classes,
            json_field_names: Vec::new(),
            relations: Vec::new(),
            columns,
        }]
    }

    // #5: FromUDBRow returns (*T, error) and each scalar read propagates a decode
    // failure instead of zeroing the field. List surfaces it so a bad row fails the
    // whole page.
    #[test]
    fn read_path_fails_closed_on_decode_error() {
        let read = go_from_row_stmt(&column("user_id", "string"), "acmev1");
        assert!(
            read.contains("val, err := udbAsString(row[\"user_id\"])"),
            "read must coerce through the error channel, got: {read}"
        );
        assert!(
            read.contains("return nil, fmt.Errorf(\"decode column %q: %w\", \"user_id\", err)"),
            "read must propagate the decode error, got: {read}"
        );
        assert!(read.contains("m.UserId = val"), "got: {read}");

        let out = render_go_entities_file(
            &widget_entity(vec!["id".to_string()], vec![column("id", "string")]),
            "acmegen",
        )
        .expect("render");
        assert!(
            out.contains("func WidgetFromUDBRow(row map[string]any) (*acmev1.Widget, error) {"),
            "FromUDBRow must return (*T, error), got:\n{out}"
        );
        assert!(out.contains("\treturn m, nil\n"), "got:\n{out}");
        assert!(
            out.contains("item, err := WidgetFromUDBRow(row)")
                && out.contains("return nil, \"\", 0, err"),
            "List must propagate a per-row decode error, got:\n{out}"
        );
    }

    // #12: unsigned/fixed proto scalars must decode through udbAsUint64 with the
    // right Go cast, not int32/int64 (which does not compile against a uint field).
    #[test]
    fn unsigned_scalars_use_uint_coercers() {
        assert!(matches!(go_scalar_kind("uint32"), GoScalar::Uint32));
        assert!(matches!(go_scalar_kind("fixed32"), GoScalar::Uint32));
        assert!(matches!(go_scalar_kind("uint64"), GoScalar::Uint64));
        assert!(matches!(go_scalar_kind("fixed64"), GoScalar::Uint64));

        let read32 = go_from_row_stmt(&column("port", "uint32"), "acmev1");
        assert!(
            read32.contains("val, err := udbAsUint64(row[\"port\"])")
                && read32.contains("m.Port = uint32(val)"),
            "uint32 read must widen through udbAsUint64, got: {read32}"
        );
        let read64 = go_from_row_stmt(&column("counter", "uint64"), "acmev1");
        assert!(
            read64.contains("val, err := udbAsUint64(row[\"counter\"])")
                && read64.contains("m.Counter = val"),
            "uint64 read must use udbAsUint64, got: {read64}"
        );
    }

    // #2: a bytes column round-trips as base64 in record_json, so the read must
    // base64-decode (the old code returned the base64 TEXT as the field bytes).
    #[test]
    fn bytes_column_base64_decodes_on_read() {
        let read = go_from_row_stmt(&column("blob", "bytes"), "acmev1");
        assert!(
            read.contains("val, err := udbAsBytes(row[\"blob\"])"),
            "bytes read must go through udbAsBytes, got: {read}"
        );
        let helpers = go_coercion_helpers(false);
        assert!(
            helpers.contains("base64.StdEncoding.DecodeString(b)"),
            "udbAsBytes must base64-decode, got: {helpers}"
        );
    }

    // #11: an optional (presence) bytes field is nilable in Go; nil means unset
    // (omit → NULL) while a present empty []byte{} is written. The old presence arm
    // excluded bytes, so an unset optional bytes was serialized as null.
    #[test]
    fn optional_bytes_presence_omits_unset() {
        let mut col = column("avatar", "bytes");
        col.has_presence = true;
        let write = go_to_record_stmt(&col);
        assert!(
            write.contains("if m.Avatar != nil {")
                && write.contains("r[\"avatar\"] = m.GetAvatar()"),
            "optional bytes must be nil-guarded, got: {write}"
        );
    }

    // #10: a JSON/JSONB column must EMBED the parsed JSON (json.RawMessage) so the
    // record is not double-encoded, and read back as canonical JSON text.
    #[test]
    fn json_column_avoids_double_encoding() {
        let mut col = column("metadata", "string");
        col.is_jsonb = true;
        let write = go_to_record_stmt(&col);
        assert!(
            write.contains("r[\"metadata\"] = json.RawMessage(m.GetMetadata())"),
            "jsonb write must embed a RawMessage, got: {write}"
        );
        assert!(
            !write.contains("r[\"metadata\"] = m.GetMetadata()\n"),
            "jsonb write must NOT emit the double-encoding string form, got: {write}"
        );
        let read = go_from_row_stmt(&col, "acmev1");
        assert!(
            read.contains("encoded, err := udbAsJSONText(raw)")
                && read.contains("m.Metadata = encoded"),
            "jsonb read must canonicalize via udbAsJSONText, got: {read}"
        );

        // A nullable json column omits an empty value so the column stays NULL.
        let mut nullable = column("metadata", "string");
        nullable.is_json = true;
        nullable.not_null = false;
        let write = go_to_record_stmt(&nullable);
        assert!(
            write.contains("if v := m.GetMetadata(); v != \"\" {")
                && write.contains("r[\"metadata\"] = json.RawMessage(v)"),
            "nullable json write must omit empty, got: {write}"
        );
    }

    // #3: a NOT NULL cross-package enum can now resolve its value names but the Go
    // emitter cannot qualify its type across packages — so it FAILS CLOSED rather
    // than silently omitting a NOT NULL column. A NULLABLE one is skipped instead.
    #[test]
    fn cross_package_enum_not_null_fails_closed() {
        let mut region = column("region", "acme.geo.v1.Region");
        region.enum_values = vec!["REGION_UNSPECIFIED".to_string(), "REGION_EAST".to_string()];
        region.enum_cross_package = true;
        region.not_null = true;
        let err = render_go_entities_file(
            &widget_entity(
                vec!["id".to_string()],
                vec![column("id", "string"), region.clone()],
            ),
            "acmegen",
        )
        .expect_err("NOT NULL cross-package enum must fail closed");
        assert!(
            err.contains("cross-package enum") && err.contains("region"),
            "fail-closed message must name the offending column, got: {err}"
        );

        // Nullable → skipped with a TODO, generation succeeds.
        region.not_null = false;
        let out = render_go_entities_file(
            &widget_entity(vec!["id".to_string()], vec![column("id", "string"), region]),
            "acmegen",
        )
        .expect("nullable cross-package enum must not fail closed");
        assert!(
            out.contains("cross-package enum acme.geo.v1.Region"),
            "nullable cross-package enum must be skipped with a TODO, got:\n{out}"
        );
    }

    // #6: Get requires the FULL primary key, caps the read at 2 rows, and returns a
    // cardinality error on >1; FindFirst is the arbitrary-filter escape hatch.
    #[test]
    fn typed_get_requires_full_key_and_guards_cardinality() {
        let out = render_go_entities_file(
            &widget_entity(vec!["id".to_string()], vec![column("id", "string")]),
            "acmegen",
        )
        .expect("render");
        assert!(
            out.contains("for _, pk := range []string{\"id\"} {"),
            "Get must require every primary-key column, got:\n{out}"
        );
        assert!(
            out.contains("udbclient.PageOptions{Limit: 2}")
                && out.contains("primary key matched %d rows, expected at most 1"),
            "Get must LIMIT 2 and guard cardinality, got:\n{out}"
        );
        assert!(
            out.contains("func (r WidgetRepo) FindFirst(ctx context.Context, where map[string]any) (*acmev1.Widget, error) {")
                && out.contains("udbclient.PageOptions{Limit: 1}"),
            "FindFirst must exist for arbitrary-filter reads, got:\n{out}"
        );
    }

    // #7: guarded CAS writes must reject a nil/empty expected map BEFORE the RPC so
    // a guarded write can never silently degrade into an unconditional one.
    #[test]
    fn guarded_writes_reject_empty_expected() {
        let out = render_go_entities_file(
            &widget_entity(vec!["id".to_string()], vec![column("id", "string")]),
            "acmegen",
        )
        .expect("render");
        assert!(
            out.contains("func (r WidgetRepo) UpdateGuarded(")
                && out.contains("refusing to run an unconditional update"),
            "UpdateGuarded must reject empty expected, got:\n{out}"
        );
        assert!(
            out.contains("func (r WidgetRepo) DeleteGuarded(")
                && out.contains("refusing to run an unconditional delete"),
            "DeleteGuarded must reject empty expected, got:\n{out}"
        );
        assert_eq!(
            out.matches("if len(expected) == 0 {").count(),
            2,
            "both guarded writes must gate on an empty expected map, got:\n{out}"
        );
    }

    // #8: the emitted Go must be gofmt-canonical. The write path (and `sdk diff`)
    // run render output through gofmt before writing/comparing; assert that
    // pipeline succeeds and is a fixed point for a sample carrying the column-policy
    // table. Skipped when the Go toolchain (gofmt) is absent — an environment probe,
    // like the generated-client gate.
    #[test]
    fn emitted_column_policy_is_gofmt_clean() {
        let mut status = column("status", "acme.v1.WidgetStatus");
        status.enum_values = vec![
            "WIDGET_STATUS_UNSPECIFIED".to_string(),
            "WIDGET_STATUS_ACTIVE".to_string(),
        ];
        let raw = render_go_entities_file(
            &widget_entity(vec!["id".to_string()], vec![column("id", "string"), status]),
            "acmegen",
        )
        .expect("render");
        // The sample must actually carry the column-policy table we canonicalize.
        assert!(raw.contains("type UDBColumn struct {"), "got:\n{raw}");
        assert!(
            raw.contains("var WidgetColumns = map[string]UDBColumn{"),
            "got:\n{raw}"
        );
        // Skip only when the toolchain is absent; when gofmt IS present a failure to
        // format is a REAL generator bug (invalid Go) and must fail the test, not be
        // swallowed as "unavailable".
        if !command_exists("gofmt") {
            eprintln!("skipping gofmt-clean assertion: gofmt not installed");
            return;
        }
        let formatted = gofmt_canonicalize(&raw).expect(
            "gofmt must accept and format the emitted Go (a rejection here is a generator bug)",
        );
        let twice = gofmt_canonicalize(&formatted).expect("gofmt is idempotent");
        assert_eq!(
            formatted, twice,
            "gofmt output must be a fixed point (clean)"
        );
        assert!(
            formatted.contains("type UDBColumn struct {"),
            "got:\n{formatted}"
        );
    }

    fn sample_manifest() -> Vec<RpcDescriptor> {
        vec![
            RpcDescriptor {
                service_name: "DataBroker".into(),
                service_pkg: "udb.services.v1".into(),
                method: "Select".into(),
                method_snake: "select".into(),
                method_alias: "select".into(),
                method_alias_snake: "select".into(),
                method_alias_camel: "select".into(),
                method_alias_pascal: "Select".into(),
                rest_operation_id: "select".into(),
                input_short: "SelectRequest".into(),
                input_pkg: "udb.entity.v1".into(),
                output_short: "RecordSet".into(),
                output_pkg: "udb.entity.v1".into(),
                http_verb: "post".into(),
                http_path: "/v1/data/select".into(),
                http_body: "*".into(),
                http_response_body: String::new(),
                client_streaming: false,
                server_streaming: false,
                native_service_id: String::new(),
                logical_service_id: String::new(),
                sdk_facade_name: String::new(),
                cli_scaffold_group: String::new(),
                auth_mode: String::new(),
                roles: Vec::new(),
                scopes: Vec::new(),
                policy_ref: String::new(),
                tenant_required: false,
                tenant_field: String::new(),
                project_field: String::new(),
                credential_types: Vec::new(),
                requires_postgres: false,
                requires_redis: false,
                requires_object_store: false,
                requires_kafka: false,
                requires_feature: String::new(),
                default_enabled: true,
                surface: "data_plane".to_string(),
                listener_kind: "public".to_string(),
                global_enablement_key: String::new(),
                service_enablement_key: String::new(),
                required_dependencies: Vec::new(),
                disabled_service_error_contract: String::new(),
                browser_safe: false,
                server_only: false,
                default_deadline_ms: 0,
                default_max_attempts: 0,
                csrf_required: false,
                internal_grpc_only: false,
                public_listener_allowed: true,
                control_plane_listener_allowed: false,
                peer_listener_allowed: false,
                operation_kind: "read_only".to_string(),
                read_only: true,
                // Idempotency-annotated RPC: renders replay-safe `true`.
                replay_safe: true,
            },
            RpcDescriptor {
                service_name: "DataBroker".into(),
                service_pkg: "udb.services.v1".into(),
                method: "SelectV2".into(),
                method_snake: "select_v2".into(),
                method_alias: "select_v2".into(),
                method_alias_snake: "select_v2".into(),
                method_alias_camel: "selectV2".into(),
                method_alias_pascal: "SelectV2".into(),
                rest_operation_id: "selectV2".into(),
                input_short: "SelectRequest".into(),
                input_pkg: "udb.entity.v1".into(),
                output_short: "RecordBatchV2".into(),
                output_pkg: "udb.entity.v1".into(),
                http_verb: String::new(),
                http_path: String::new(),
                http_body: String::new(),
                http_response_body: String::new(),
                client_streaming: false,
                server_streaming: true,
                native_service_id: String::new(),
                logical_service_id: String::new(),
                sdk_facade_name: String::new(),
                cli_scaffold_group: String::new(),
                auth_mode: String::new(),
                roles: Vec::new(),
                scopes: Vec::new(),
                policy_ref: String::new(),
                tenant_required: false,
                tenant_field: String::new(),
                project_field: String::new(),
                credential_types: Vec::new(),
                requires_postgres: false,
                requires_redis: false,
                requires_object_store: false,
                requires_kafka: false,
                requires_feature: String::new(),
                default_enabled: true,
                surface: "data_plane".to_string(),
                listener_kind: "public".to_string(),
                global_enablement_key: String::new(),
                service_enablement_key: String::new(),
                required_dependencies: Vec::new(),
                disabled_service_error_contract: String::new(),
                browser_safe: false,
                server_only: false,
                default_deadline_ms: 0,
                default_max_attempts: 0,
                csrf_required: false,
                internal_grpc_only: false,
                public_listener_allowed: true,
                control_plane_listener_allowed: false,
                peer_listener_allowed: false,
                operation_kind: "read_only".to_string(),
                read_only: true,
                replay_safe: false,
            },
        ]
    }

    // T-1: `udb sdk generate` must work from an installed binary with no
    // `sdk-templates/` on disk — the templates are embedded and extracted with a
    // deterministic layout (`<base>/<lang>/…`). This exercises the extraction the
    // FS-path tests never reach.
    #[test]
    fn embedded_templates_extract_with_language_layout() {
        let base =
            std::env::temp_dir().join(format!("udb-sdk-templates-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).expect("create test extract dir");
        extract_embedded_dir(&EMBEDDED_SDK_TEMPLATES, &base).expect("extract embedded templates");

        // A known template lands at <base>/<lang>/…, not nested under sdk-templates/.
        let go_tmpl = base.join("go/udbclient/generated_client.go.tmpl");
        assert!(
            go_tmpl.is_file(),
            "embedded Go client template missing at {}",
            go_tmpl.display()
        );
        // resolve_langs must see the language dirs directly under the extract root.
        let langs = resolve_langs(&base, "all").expect("resolve langs from embedded");
        assert!(
            langs.contains(&"go".to_string()),
            "go lang not found: {langs:?}"
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn orm_tiers_are_embedded_at_build_time() {
        // master-plan 10.6: the ORM tier map is embedded as a build-time scalar
        // because GetCapabilities (runtime) requires admin_scope.
        let scalars = base_scalars(&sample_manifest(), 1);
        let orm = scalars
            .iter()
            .find(|(k, _)| k == "ORM_TIERS")
            .map(|(_, v)| v.clone())
            .expect("ORM_TIERS scalar must be embedded");
        let parsed: serde_json::Value =
            serde_json::from_str(&orm).expect("ORM_TIERS must be valid JSON");
        // Derived directly from BackendKind::orm_tier() — spot-check the tiers.
        assert_eq!(parsed["postgres"], "relational");
        assert_eq!(parsed["mongodb"], "document");
        assert_eq!(parsed["redis"], "kv");
        assert_eq!(parsed["qdrant"], "vector");
        assert_eq!(parsed["s3"], "blob");
        assert_eq!(parsed["neo4j"], "graph");
        // Every one of the 18 backend kinds is present.
        assert_eq!(
            parsed.as_object().map(|m| m.len()),
            Some(udb::backend::BackendKind::ALL.len())
        );
        let java = scalars
            .iter()
            .find(|(k, _)| k == "ORM_TIERS_JAVA")
            .map(|(_, v)| v.clone())
            .expect("ORM_TIERS_JAVA scalar must be embedded");
        assert!(java.contains("Map.entry(\"postgres\", \"relational\")"));
        assert!(java.contains("Map.entry(\"mongodb\", \"document\")"));
        let string_literal = scalars
            .iter()
            .find(|(k, _)| k == "ORM_TIERS_STRING")
            .map(|(_, v)| v.clone())
            .expect("ORM_TIERS_STRING scalar must be embedded");
        assert!(string_literal.contains("\\\"postgres\\\":\\\"relational\\\""));
    }

    #[test]
    fn backend_roles_are_embedded_at_build_time() {
        // master-plan 10.4: UnitOfWork transaction honesty comes from the same
        // BackendRole projection the runtime uses, embedded at generation time.
        let scalars = base_scalars(&sample_manifest(), 1);
        let roles = scalars
            .iter()
            .find(|(k, _)| k == "BACKEND_ROLES")
            .map(|(_, v)| v.clone())
            .expect("BACKEND_ROLES scalar must be embedded");
        let parsed: serde_json::Value =
            serde_json::from_str(&roles).expect("BACKEND_ROLES must be valid JSON");
        assert_eq!(parsed["postgres"], "canonical");
        assert_eq!(parsed["s3"], "projection");
        assert_eq!(
            parsed.as_object().map(|m| m.len()),
            Some(udb::backend::BackendKind::ALL.len())
        );
        let java = scalars
            .iter()
            .find(|(k, _)| k == "BACKEND_ROLES_JAVA")
            .map(|(_, v)| v.clone())
            .expect("BACKEND_ROLES_JAVA scalar must be embedded");
        assert!(java.contains("Map.entry(\"postgres\", \"canonical\")"));
        assert!(java.contains("Map.entry(\"s3\", \"projection\")"));
        let string_literal = scalars
            .iter()
            .find(|(k, _)| k == "BACKEND_ROLES_STRING")
            .map(|(_, v)| v.clone())
            .expect("BACKEND_ROLES_STRING scalar must be embedded");
        assert!(string_literal.contains("\\\"postgres\\\":\\\"canonical\\\""));
    }

    #[test]
    fn scalars_are_substituted() {
        let scalars = vec![
            ("UDB_VERSION".to_string(), "9.9.9".to_string()),
            ("LANG".to_string(), "python".to_string()),
        ];
        let out = render_text("v={{UDB_VERSION}} lang={{LANG}}", &[], &[], &scalars);
        assert_eq!(out.trim_end(), "v=9.9.9 lang=python");
    }

    #[test]
    fn global_relation_accessor_slots_render_empty_outside_entity_blocks() {
        let out = render_text(
            "before\n{{ENTITY_TS_RELATION_ACCESSORS}}\nafter\n",
            &[],
            &[],
            &[],
        );
        assert_eq!(out, "before\n\nafter\n");
        assert_eq!(first_unresolved_template_token(&out), None);
    }

    #[test]
    fn unresolved_template_tokens_are_reported_but_entity_family_docs_are_allowed() {
        assert_eq!(
            first_unresolved_template_token("ok {{ENTITY_*}} docs"),
            None
        );
        assert_eq!(
            first_unresolved_template_token("bad {{RPC_UNKNOWN}} token").as_deref(),
            Some("{{RPC_UNKNOWN}}")
        );
    }

    #[test]
    fn per_rpc_block_expands_once_per_rpc() {
        let tmpl =
            "# @@UDB_RPC_BEGIN\ndef {{RPC_SNAKE}}(): path = \"{{RPC_PATH}}\"\n# @@UDB_RPC_END\n";
        let out = render_text(tmpl, &sample_manifest(), &[], &[]);
        assert!(out.contains("def select(): path = \"/udb.services.v1.DataBroker/Select\""));
        assert!(out.contains("def select_v2(): path = \"/udb.services.v1.DataBroker/SelectV2\""));
        // Marker lines must be gone.
        assert!(!out.contains("@@UDB_RPC"));
    }

    #[test]
    fn replay_safe_placeholder_reflects_idempotency_contract() {
        let tmpl = "# @@UDB_RPC_BEGIN\n{{RPC_NAME}}={{RPC_REPLAY_SAFE}}\n# @@UDB_RPC_END\n";
        let out = render_text(tmpl, &sample_manifest(), &[], &[]);
        // The idempotency-annotated RPC renders replay-safe `true`...
        assert!(
            out.contains("Select=true\n"),
            "idempotency-annotated RPC should render replay-safe true: {out}"
        );
        // ...while a non-idempotent RPC renders `false`.
        assert!(
            out.contains("SelectV2=false"),
            "non-idempotent RPC should render replay-safe false: {out}"
        );
    }

    #[test]
    fn alias_placeholders_are_acronym_safe_and_wire_name_stays_compatible() {
        let mut manifest = sample_manifest();
        manifest[0].method = "GetJWKS".to_string();
        manifest[0].method_snake = "get_j_w_k_s".to_string();
        manifest[0].method_alias = "get_jwks".to_string();
        manifest[0].method_alias_snake = "get_jwks".to_string();
        manifest[0].method_alias_camel = "getJwks".to_string();
        manifest[0].method_alias_pascal = "GetJwks".to_string();
        manifest[0].rest_operation_id = "authn.getJwks".to_string();
        let tmpl = "# @@UDB_RPC_BEGIN kind=unary\n\
                    {{RPC_NAME}} {{RPC_WIRE_NAME}} {{RPC_SNAKE}} \
                    {{RPC_ALIAS_SNAKE}} {{RPC_ALIAS_CAMEL}} {{RPC_ALIAS_PASCAL}} \
                    {{REST_OPERATION_ID}}\n\
                    # @@UDB_RPC_END\n";
        let out = render_text(tmpl, &manifest, &[], &[]);
        assert!(out.contains("GetJWKS GetJWKS get_j_w_k_s get_jwks getJwks GetJwks authn.getJwks"));
    }

    #[test]
    fn php_method_alias_entries_do_not_duplicate_identical_wire_and_alias_names() {
        let manifest = sample_manifest();
        let tmpl = "# @@UDB_RPC_BEGIN kind=unary\n{{PHP_METHOD_ALIAS_ENTRIES}}\n# @@UDB_RPC_END\n";
        let out = render_text(tmpl, &manifest[..1], &[], &[]);
        assert_eq!(
            out.lines()
                .filter(|line| line.contains("\"select\" => \"select\","))
                .count(),
            1
        );
    }

    #[test]
    fn php_method_alias_entries_keep_distinct_wire_and_alias_names() {
        let mut manifest = sample_manifest();
        manifest[0].method = "GetJWKS".to_string();
        manifest[0].method_snake = "get_j_w_k_s".to_string();
        manifest[0].method_alias = "get_jwks".to_string();
        manifest[0].method_alias_snake = "get_jwks".to_string();
        manifest[0].method_alias_camel = "getJwks".to_string();
        let tmpl = "# @@UDB_RPC_BEGIN kind=unary\n{{PHP_METHOD_ALIAS_ENTRIES}}\n# @@UDB_RPC_END\n";
        let out = render_text(tmpl, &manifest[..1], &[], &[]);
        assert!(out.contains("\"get_jwks\" => \"getJwks\","));
        assert!(out.contains("\"get_j_w_k_s\" => \"getJwks\","));
    }

    #[test]
    fn php_rpc_wrappers_disambiguate_alias_collisions() {
        let mut manifest = sample_manifest();
        manifest.truncate(1);
        manifest[0].method = "CacheDelete".to_string();
        manifest[0].method_snake = "cache_delete".to_string();
        manifest[0].method_alias = "cache_delete".to_string();
        manifest[0].method_alias_snake = "cache_delete".to_string();
        manifest[0].method_alias_camel = "cacheDelete".to_string();
        manifest[0].method_alias_pascal = "CacheDelete".to_string();

        let mut cache_delete = manifest[0].clone();
        cache_delete.service_name = "CacheService".to_string();
        cache_delete.service_pkg = "udb.core.cache.services.v1".to_string();
        cache_delete.method = "Delete".to_string();
        cache_delete.method_snake = "delete".to_string();
        cache_delete.method_alias = "cache_delete".to_string();
        cache_delete.method_alias_snake = "cache_delete".to_string();
        cache_delete.method_alias_camel = "cacheDelete".to_string();
        cache_delete.method_alias_pascal = "CacheDelete".to_string();
        manifest.push(cache_delete);

        let tmpl = r#"
// @@UDB_RPC_BEGIN kind=unary
{{PHP_METHOD_ALIAS_ENTRIES}}
public function {{PHP_RPC_METHOD_CAMEL}}(): void {}
// @@UDB_RPC_END
"#;
        let out = render_text(tmpl, &manifest, &[], &[]);
        assert_eq!(
            out.lines()
                .filter(|line| line.contains("\"cache_delete\" =>"))
                .count(),
            1,
            "{out}"
        );
        assert!(
            out.contains("\"cache_delete\" => \"cacheDelete\","),
            "{out}"
        );
        assert_eq!(out.matches("public function cacheDelete()").count(), 1);
        assert!(out.contains("public function cacheServiceDelete()"));
    }

    #[test]
    fn sdk_manifest_json_exposes_template_token_fields() {
        let manifest = sample_manifest();
        let rpc = &manifest[0];
        let value = rpc_to_json(rpc);
        assert_eq!(
            value.get("operation_kind").and_then(|v| v.as_str()),
            Some("read_only")
        );
        assert_eq!(value.get("read_only").and_then(|v| v.as_bool()), Some(true));
        assert_eq!(
            value.get("method_alias_camel").and_then(|v| v.as_str()),
            Some("select")
        );
        assert_eq!(
            value.get("rest_operation_id").and_then(|v| v.as_str()),
            Some("select")
        );
        assert_eq!(
            value.get("http_path").and_then(|v| v.as_str()),
            Some("/v1/data/select")
        );
    }

    #[test]
    fn rpc_block_kind_filter_selects_streaming_only() {
        let tmpl = "// @@UDB_RPC_BEGIN kind=server_streaming\n{{RPC_NAME}}\n// @@UDB_RPC_END\n";
        let out = render_text(tmpl, &sample_manifest(), &[], &[]);
        assert!(out.contains("SelectV2"));
        assert!(!out.contains("Select\n") || out.contains("SelectV2"));
        // Only the streaming RPC should survive.
        assert_eq!(out.matches("SelectV2").count(), 1);
        assert!(!out.lines().any(|l| l.trim() == "Select"));
    }

    #[test]
    fn service_block_expands_per_service_with_count() {
        let tmpl = "// @@UDB_SERVICE_BEGIN\n{{SERVICE_FULL}} has {{SERVICE_RPC_COUNT}}\n// @@UDB_SERVICE_END\n";
        let out = render_text(tmpl, &sample_manifest(), &[], &[]);
        assert!(out.contains("udb.services.v1.DataBroker has 2"));
    }

    #[test]
    fn entity_block_substitutes_canonical_placeholders() {
        let mut language_classes = std::collections::BTreeMap::new();
        language_classes.insert("go".to_string(), "entityv1.Policy".to_string());
        language_classes.insert("ts".to_string(), "Policy".to_string());
        language_classes.insert("python".to_string(), "udb.entity.v1.Policy".to_string());
        let entities = vec![EntityDescriptor {
            message_type: "udb.entity.v1.Policy".to_string(),
            short_name: "Policy".to_string(),
            table: "policies".to_string(),
            primary_keys: vec!["id".to_string(), "tenant_id".to_string()],
            tenant_field: "tenant_id".to_string(),
            project_field: "project_id".to_string(),
            soft_delete_field: "deleted_at".to_string(),
            version_field: "version".to_string(),
            proto_package: "udb.entity.v1".to_string(),
            language_classes,
            json_field_names: vec!["id".to_string(), "tenant_id".to_string()],
            relations: vec![EntityRelationDescriptor {
                name: "tenant".to_string(),
                kind: "belongs_to".to_string(),
                local_fields: vec!["tenant_id".to_string()],
                target_message_type: "udb.entity.v1.Tenant".to_string(),
                target_table: "udb.tenants".to_string(),
                target_fields: vec!["id".to_string()],
                on_delete: "cascade".to_string(),
                on_update: String::new(),
            }],
            columns: Vec::new(),
        }];
        let tmpl = "// @@UDB_ENTITY_BEGIN\n\
                    {{ENTITY_MESSAGE_TYPE}} {{ENTITY_TABLE}} []string{ {{ENTITY_PRIMARY_KEYS}} } \
                    fields=[{{ENTITY_JSON_FIELDS}}] aliases={{ENTITY_ALIAS_SNAKE}}/{{ENTITY_ALIAS_CAMEL}}/{{ENTITY_ALIAS_PASCAL}} \
                    relations={{ENTITY_RELATIONS_JSON}} java_relations={{ENTITY_JAVA_RELATIONS}} \
                    {{ENTITY_TENANT_FIELD}} {{ENTITY_PROJECT_FIELD}} {{ENTITY_SOFT_DELETE_FIELD}} {{ENTITY_VERSION_FIELD}} \
                    {{ENTITY_GO_TYPE}} {{ENTITY_TS_TYPE}} {{ENTITY_PY_IMPORT}}\n\
                    ts={{ENTITY_TS_RELATION_ACCESSORS}}\n\
                    py={{ENTITY_PY_RELATION_ACCESSORS}}\n\
                    go={{ENTITY_GO_RELATION_ACCESSORS}}\n\
                    cs={{ENTITY_CSHARP_RELATION_ACCESSORS}}\n\
                    java={{ENTITY_JAVA_RELATION_ACCESSORS}}\n\
                    php={{ENTITY_PHP_RELATION_ACCESSORS}}\n\
                    // @@UDB_ENTITY_END\n";
        let out = render_text(tmpl, &[], &entities, &[]);
        assert!(out.contains("udb.entity.v1.Policy policies []string{ \"id\", \"tenant_id\" }"));
        assert!(out.contains("fields=[\"id\", \"tenant_id\"] aliases=policy/policy/Policy"));
        assert!(out.contains(
            "relations=[{\"name\":\"tenant\",\"kind\":\"belongs_to\",\"local_fields\":[\"tenant_id\"],\"target_message_type\":\"udb.entity.v1.Tenant\""
        ));
        assert!(out.contains(
            "java_relations=List.of(new EntityRelationBinding(\"tenant\", \"belongs_to\", List.of(\"tenant_id\"), \"udb.entity.v1.Tenant\""
        ));
        assert!(out.contains("tenant_id project_id deleted_at version"));
        assert!(out.contains("entityv1.Policy Policy udb.entity.v1.Policy"));
        assert!(out.contains("tenantRelation(parent: Record<string, unknown>): Query"));
        assert!(out.contains("def tenant_relation(self, parent: Mapping[str, Any]) -> Query"));
        assert!(out.contains("func (r *Repository) TenantRelation(parent map[string]any)"));
        assert!(out.contains(
            "public IrQuery TenantRelation(IReadOnlyDictionary<string, object?> parent)"
        ));
        assert!(out.contains("public Query tenantRelation(Map<String, Object> parent)"));
        assert!(out.contains("public function tenantRelation(array $parent): IrQuery"));
        assert!(!out.contains("@@UDB_ENTITY"));
    }

    fn sample_entities() -> Vec<EntityDescriptor> {
        let entity = |message: &str, pkg: &str, table: &str| EntityDescriptor {
            message_type: format!("{pkg}.{message}"),
            short_name: message.to_string(),
            table: table.to_string(),
            primary_keys: vec!["id".to_string()],
            tenant_field: "tenant_id".to_string(),
            project_field: String::new(),
            soft_delete_field: String::new(),
            version_field: String::new(),
            proto_package: pkg.to_string(),
            language_classes: std::collections::BTreeMap::new(),
            json_field_names: vec!["id".to_string()],
            relations: Vec::new(),
            columns: Vec::new(),
        };
        vec![
            entity("User", "myapp.v1", "users"),
            entity("Invoice", "myapp.v1", "invoices"),
        ]
    }

    // master-plan 10.5: the `udb orm scaffold` entry point reuses `generate`;
    // the only ORM-specific behavior is the optional per-entity scoping. These
    // assert the arg → generator-params mapping at that boundary.
    #[test]
    fn select_entities_none_returns_full_manifest_unchanged() {
        let entities = sample_entities();
        let out = select_entities(entities.clone(), None).expect("None is identity");
        assert_eq!(out, entities);
        // An empty/whitespace `--entity` is treated as "no filter", not "no match".
        let out_blank = select_entities(entities.clone(), Some("  ")).expect("blank is identity");
        assert_eq!(out_blank, entities);
    }

    #[test]
    fn select_entities_matches_short_name_and_fqn() {
        let by_short = select_entities(sample_entities(), Some("User")).expect("short-name match");
        assert_eq!(by_short.len(), 1);
        assert_eq!(by_short[0].message_type, "myapp.v1.User");

        let by_fqn =
            select_entities(sample_entities(), Some("myapp.v1.Invoice")).expect("fqn match");
        assert_eq!(by_fqn.len(), 1);
        assert_eq!(by_fqn[0].message_type, "myapp.v1.Invoice");
    }

    #[test]
    fn select_entities_unknown_entity_errors_and_stops() {
        let err = select_entities(sample_entities(), Some("Nope")).unwrap_err();
        assert!(err.contains("matched no annotated entity"), "got: {err}");
    }

    #[test]
    fn repository_entities_without_primary_key_error_and_stop() {
        let mut entities = sample_entities();
        entities[0].primary_keys.clear();
        let err = validate_repository_entities(&entities).unwrap_err();
        assert!(err.contains("has no descriptor primary key"), "got: {err}");
        assert!(err.contains("myapp.v1.User"), "got: {err}");
    }

    #[test]
    fn nested_rpc_block_inside_service_block_expands_per_service() {
        // The idiomatic shape: one class per service, one method per (filtered) RPC.
        let tmpl = "# @@UDB_SERVICE_BEGIN\n\
                    class {{SERVICE_NAME}}Client:  # {{SERVICE_RPC_COUNT}} rpcs\n\
                    # @@UDB_RPC_BEGIN service={{SERVICE_NAME}} kind=unary\n\
                    \x20\x20\x20\x20def {{RPC_SNAKE}}(self): pass  # {{SERVICE_NAME}}\n\
                    # @@UDB_RPC_END\n\
                    # @@UDB_SERVICE_END\n";
        let out = render_text(tmpl, &sample_manifest(), &[], &[]);
        // DataBroker class with both RPCs counted, but only the unary one emitted.
        assert!(
            out.contains("class DataBrokerClient:  # 2 rpcs"),
            "got:\n{out}"
        );
        assert!(out.contains("def select(self): pass  # DataBroker"));
        assert!(!out.contains("def select_v2"));
        // No markers survive after nested expansion.
        assert!(!out.contains("@@UDB_RPC"));
        assert!(!out.contains("@@UDB_SERVICE"));
    }

    #[test]
    fn skip_rules_hide_config_docs_and_dotfiles() {
        assert!(should_skip("sdkgen.yaml"));
        assert!(should_skip("README.md"));
        assert!(should_skip("TEMPLATES.md"));
        assert!(should_skip("sub/.gitkeep"));
        assert!(!should_skip("udb_client/client.py.tmpl"));
        assert!(!should_skip("src/GeneratedClient.cs"));
    }

    #[test]
    fn surface_filter_selects_public_listener_rpcs() {
        let tmpl = "// @@UDB_RPC_BEGIN surface=public\n{{RPC_NAME}}\n// @@UDB_RPC_END\n";
        let out = render_text(tmpl, &sample_manifest(), &[], &[]);
        // Both sample RPCs allow the public listener.
        assert!(out.contains("Select"));
        assert!(out.contains("SelectV2"));

        let tmpl_cp = "// @@UDB_RPC_BEGIN surface=control_plane\n{{RPC_NAME}}\n// @@UDB_RPC_END\n";
        let out_cp = render_text(tmpl_cp, &sample_manifest(), &[], &[]);
        // Neither sample RPC allows the control-plane listener.
        assert!(!out_cp.lines().any(|l| l.trim() == "Select"));
        assert!(!out_cp.contains("SelectV2"));
    }

    #[test]
    fn apply_selectors_default_is_identity() {
        let manifest = sample_manifest();
        let out = apply_selectors(&manifest, &SdkSelector::default()).expect("default ok");
        assert_eq!(out.len(), manifest.len());
    }

    #[test]
    fn apply_selectors_surface_public_keeps_all_and_control_plane_drops_all() {
        let manifest = sample_manifest();
        let public = apply_selectors(
            &manifest,
            &SdkSelector {
                surface: Some("public".to_string()),
                ..Default::default()
            },
        )
        .expect("public ok");
        assert_eq!(public.len(), manifest.len());

        let cp = apply_selectors(
            &manifest,
            &SdkSelector {
                surface: Some("control_plane".to_string()),
                ..Default::default()
            },
        )
        .expect("cp ok");
        assert!(cp.is_empty());
    }

    #[test]
    fn apply_selectors_unknown_service_errors_with_known_list() {
        let manifest = sample_manifest();
        let err = apply_selectors(
            &manifest,
            &SdkSelector {
                services: vec!["does_not_exist".to_string()],
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.contains("unknown --service"));
    }

    #[test]
    fn sdk_preflight_bootstrap_uses_package_version_source() {
        let version = env!("CARGO_PKG_VERSION");
        assert_eq!(
            preflight_language("typescript").bootstrap,
            format!("npm i @udb_plus/sdk@{version}")
        );
        assert_eq!(
            preflight_language("python").bootstrap,
            format!("python -m pip install udb-client=={version}")
        );
        assert_eq!(
            preflight_language("go").bootstrap,
            format!("go get github.com/fahara02/udb/sdk/go@v{version}")
        );
        assert_eq!(
            preflight_language("csharp").bootstrap,
            format!("dotnet add package Udb.Client --version {version}")
        );
        assert_eq!(
            preflight_language("php").bootstrap,
            format!("composer require fahara02/udb-laravel:^{version}")
        );
    }

    #[test]
    fn apply_selectors_native_only_drops_public_broker_rpcs() {
        // The sample manifest's RPCs have empty native_service_id.
        let manifest = sample_manifest();
        let out = apply_selectors(
            &manifest,
            &SdkSelector {
                native_only: true,
                ..Default::default()
            },
        )
        .expect("native-only ok");
        assert!(out.is_empty());
    }

    #[test]
    fn fsm_rejects_illegal_transition() {
        let mut fsm = Fsm::new();
        // Start may not jump straight to Render.
        assert!(fsm.go(SdkGenState::Render).is_err());
        assert_eq!(fsm.state, SdkGenState::Failed);
        assert!(SdkGenState::Failed.is_terminal());
    }
}
