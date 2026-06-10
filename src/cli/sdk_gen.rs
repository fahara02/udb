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

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

use udb::runtime::sdk_manifest::{RpcDescriptor, rpc_manifest};

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
    templates_dir: &str,
    out_dir: &str,
    selector: &SdkSelector,
) -> i32 {
    match action {
        SdkAction::Manifest => emit_manifest_json(),
        SdkAction::ListLangs => list_languages(templates_dir),
        SdkAction::Init => init_sdk(lang),
        SdkAction::Generate => generate(lang, templates_dir, out_dir, selector),
    }
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
    serde_json::Value::Object(obj)
}

/// List the language template directories available under `templates_dir`.
fn list_languages(templates_dir: &str) -> i32 {
    let root = Path::new(templates_dir);
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

/// Drive the generation FSM for one language or `all`.
fn generate(lang: &str, templates_dir: &str, out_dir: &str, selector: &SdkSelector) -> i32 {
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
    let templates_root = Path::new(templates_dir);
    if !templates_root.is_dir() {
        return fsm.fail(format!(
            "template dir `{templates_dir}` not found — author templates under \
             `{templates_dir}/<lang>/` (see `udb sdk list-langs`)"
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
        match render_language(&lang_tmpl_dir, &lang_out_dir, &manifest, &lang_scalars) {
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
            let body = render_text(&raw, manifest, scalars);
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

/// Render a template: expand per-RPC and per-service blocks, then substitute
/// scalar placeholders across the whole result.
fn render_text(template: &str, manifest: &[RpcDescriptor], scalars: &[(String, String)]) -> String {
    let expanded = expand_blocks(template, manifest);
    apply_scalars(&expanded, scalars)
}

const RPC_BEGIN: &str = "@@UDB_RPC_BEGIN";
const RPC_END: &str = "@@UDB_RPC_END";
const SVC_BEGIN: &str = "@@UDB_SERVICE_BEGIN";
const SVC_END: &str = "@@UDB_SERVICE_END";

/// Expand all template blocks: service blocks first (recursing into any RPC
/// blocks NESTED inside each service body, scoped to that service), then any
/// remaining top-level RPC blocks. Nesting matters because the idiomatic shape
/// is "one client class per service, one method per RPC" — i.e. an RPC block
/// inside a service block.
fn expand_blocks(text: &str, manifest: &[RpcDescriptor]) -> String {
    let after_services = expand_service_blocks(text, manifest);
    expand_rpc_blocks(&after_services, manifest)
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
                out.push_str(&substitute_rpc(&body, rpc));
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

fn substitute_rpc(body: &str, rpc: &RpcDescriptor) -> String {
    let pairs: [(&str, String); 18] = [
        ("{{RPC_NAME}}", rpc.method.clone()),
        ("{{RPC_SNAKE}}", rpc.method_snake.clone()),
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

    fn sample_manifest() -> Vec<RpcDescriptor> {
        vec![
            RpcDescriptor {
                service_name: "DataBroker".into(),
                service_pkg: "udb.services.v1".into(),
                method: "Select".into(),
                method_snake: "select".into(),
                input_short: "SelectRequest".into(),
                input_pkg: "udb.entity.v1".into(),
                output_short: "RecordSet".into(),
                output_pkg: "udb.entity.v1".into(),
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
            },
            RpcDescriptor {
                service_name: "DataBroker".into(),
                service_pkg: "udb.services.v1".into(),
                method: "SelectV2".into(),
                method_snake: "select_v2".into(),
                input_short: "SelectRequest".into(),
                input_pkg: "udb.entity.v1".into(),
                output_short: "RecordBatchV2".into(),
                output_pkg: "udb.entity.v1".into(),
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
            },
        ]
    }

    #[test]
    fn scalars_are_substituted() {
        let scalars = vec![
            ("UDB_VERSION".to_string(), "9.9.9".to_string()),
            ("LANG".to_string(), "python".to_string()),
        ];
        let out = render_text("v={{UDB_VERSION}} lang={{LANG}}", &[], &scalars);
        assert_eq!(out.trim_end(), "v=9.9.9 lang=python");
    }

    #[test]
    fn per_rpc_block_expands_once_per_rpc() {
        let tmpl =
            "# @@UDB_RPC_BEGIN\ndef {{RPC_SNAKE}}(): path = \"{{RPC_PATH}}\"\n# @@UDB_RPC_END\n";
        let out = render_text(tmpl, &sample_manifest(), &[]);
        assert!(out.contains("def select(): path = \"/udb.services.v1.DataBroker/Select\""));
        assert!(out.contains("def select_v2(): path = \"/udb.services.v1.DataBroker/SelectV2\""));
        // Marker lines must be gone.
        assert!(!out.contains("@@UDB_RPC"));
    }

    #[test]
    fn rpc_block_kind_filter_selects_streaming_only() {
        let tmpl = "// @@UDB_RPC_BEGIN kind=server_streaming\n{{RPC_NAME}}\n// @@UDB_RPC_END\n";
        let out = render_text(tmpl, &sample_manifest(), &[]);
        assert!(out.contains("SelectV2"));
        assert!(!out.contains("Select\n") || out.contains("SelectV2"));
        // Only the streaming RPC should survive.
        assert_eq!(out.matches("SelectV2").count(), 1);
        assert!(!out.lines().any(|l| l.trim() == "Select"));
    }

    #[test]
    fn service_block_expands_per_service_with_count() {
        let tmpl = "// @@UDB_SERVICE_BEGIN\n{{SERVICE_FULL}} has {{SERVICE_RPC_COUNT}}\n// @@UDB_SERVICE_END\n";
        let out = render_text(tmpl, &sample_manifest(), &[]);
        assert!(out.contains("udb.services.v1.DataBroker has 2"));
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
        let out = render_text(tmpl, &sample_manifest(), &[]);
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
        let out = render_text(tmpl, &sample_manifest(), &[]);
        // Both sample RPCs allow the public listener.
        assert!(out.contains("Select"));
        assert!(out.contains("SelectV2"));

        let tmpl_cp = "// @@UDB_RPC_BEGIN surface=control_plane\n{{RPC_NAME}}\n// @@UDB_RPC_END\n";
        let out_cp = render_text(tmpl_cp, &sample_manifest(), &[]);
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
