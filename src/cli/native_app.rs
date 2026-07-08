//! Native-service app lifecycle (`udb native add|remove|generate|doctor|smoke`)
//! and `udb app init`.
//!
//! Everything here is **descriptor-driven**: the set of selectable services, the
//! surface grouping, dependency tokens, facade names, and required env vars are
//! all read from the embedded proto descriptor set
//! ([`descriptor_contract_manifest`]) and the derived
//! [`native_service_registry`]. There is no hand-maintained service list to
//! drift from the wire contract.
//!
//! State lives in a `.udb/native-services.json` manifest under the target dir.
//! `add`/`remove` edit it idempotently; `generate` reads it back and emits
//! `udb.config.json`, `.env.example`, and a per-language client-factory snippet
//! that wires the `UdbProject` facade (`createUdb` / `Udb.project`). `doctor`
//! validates the manifest against the current descriptor version; `smoke`
//! reports how to run generated smoke artifacts.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

use serde::{Deserialize, Serialize};

use udb::runtime::descriptor_manifest::{
    DescriptorContractManifest, NativeServiceContract, descriptor_contract_manifest_static,
};
use udb::runtime::service::native_registry::native_service_registry;

use super::native_dependency_tokens;

/// On-disk schema for `.udb/native-services.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct NativeServicesManifest {
    /// Descriptor/protocol version the selection was made against.
    pub descriptor_version: String,
    /// `udb` binary version that last wrote this file.
    pub generator_version: String,
    /// Canonical, sorted, de-duplicated selected service IDs.
    pub services: Vec<String>,
    /// Target SDK language recorded for `generate`.
    #[serde(default)]
    pub lang: String,
    /// Target framework hint recorded for `generate`.
    #[serde(default)]
    pub framework: String,
    /// A reproducible command that recreates this selection.
    pub regenerate_command: String,
}

const MANIFEST_REL: &str = ".udb/native-services.json";

fn manifest_path(out_dir: &str) -> PathBuf {
    Path::new(out_dir).join(MANIFEST_REL)
}

fn descriptor_version() -> String {
    udb::runtime::native_catalog::protocol_version().to_string()
}

fn generator_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

// ── Alias expansion ──────────────────────────────────────────────────────────

/// Expand a single user-facing token (alias or canonical id) into one or more
/// canonical native-service IDs. Aliases are intentionally small and explicit;
/// canonical IDs (the registry `service_id`s) pass through unchanged.
fn expand_alias(token: &str) -> Vec<String> {
    let owned = |slice: &[&str]| slice.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    match token.to_ascii_lowercase().as_str() {
        "auth" => owned(&["authn", "authz", "apikey"]),
        "org" | "tenant" => owned(&["tenant"]),
        "notify" | "notification" => owned(&["notification"]),
        "storage" => owned(&["storage"]),
        "asset" => owned(&["asset"]),
        "all" | "native" | "native-services" => native_service_registry()
            .into_iter()
            .map(|entry| entry.service_id)
            .collect(),
        "webrtc" => owned(&[
            "webrtc_room",
            "webrtc_peer",
            "webrtc_track",
            "webrtc_turn",
            "webrtc_signaling",
        ]),
        "data" | "databroker" | "broker" => owned(&["data"]),
        other => vec![other.to_string()],
    }
}

/// Expand and canonicalize a list of user tokens; returns sorted+deduped IDs.
fn expand_aliases(tokens: &[String]) -> Vec<String> {
    let mut set: BTreeSet<String> = BTreeSet::new();
    for token in tokens {
        for id in expand_alias(token) {
            set.insert(id);
        }
    }
    set.into_iter().collect()
}

/// Set of selectable service IDs: every registry `service_id` plus the special
/// `data` token for the public DataBroker (which is not a native service).
fn known_service_ids() -> BTreeSet<String> {
    let mut ids: BTreeSet<String> = native_service_registry()
        .into_iter()
        .map(|entry| entry.service_id)
        .collect();
    ids.insert("data".to_string());
    ids
}

/// Validate already-expanded IDs against the descriptor-known set. Returns the
/// unknown IDs (empty == all valid).
fn unknown_ids(ids: &[String]) -> Vec<String> {
    let known = known_service_ids();
    ids.iter()
        .filter(|id| !known.contains(*id))
        .cloned()
        .collect()
}

// ── Surface grouping (descriptor-derived) ────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Surface {
    DataPlane,
    ControlPlane,
    Peer,
}

impl Surface {
    fn label(self) -> &'static str {
        match self {
            Surface::DataPlane => "data-plane",
            Surface::ControlPlane => "control-plane",
            Surface::Peer => "peer",
        }
    }
}

/// Classify a native service into a surface bucket from its listener flags.
/// Peer takes precedence (WebRTC), then control-plane, else data-plane.
fn surface_of(native: &NativeServiceContract) -> Surface {
    if native.peer_listener_allowed {
        Surface::Peer
    } else if native.control_plane_listener_allowed {
        Surface::ControlPlane
    } else {
        Surface::DataPlane
    }
}

/// Look up the native-service contract for a canonical service id.
fn native_for_id<'a>(
    manifest: &'a DescriptorContractManifest,
    service_id: &str,
) -> Option<&'a NativeServiceContract> {
    // Descriptor native ids are dotted (`webrtc.room`); CLI/registry ids are
    // underscore (`webrtc_room`). Canonicalize both sides so lookups match
    // regardless of source — otherwise `native generate` silently misses WebRTC.
    use udb::runtime::service::native_registry::canonical_service_id;
    let want = canonical_service_id(service_id);
    manifest
        .services
        .iter()
        .filter_map(|s| s.native_service.as_ref())
        .find(|n| canonical_service_id(&n.service_id) == want)
}

// ── `native list` ────────────────────────────────────────────────────────────

pub(crate) fn run_list(manifest: &DescriptorContractManifest, json: bool) -> i32 {
    // Aggregate per native service (a registry entry may span multiple proto
    // services, e.g. webrtc); use the descriptor manifest for RPC counts.
    #[derive(Serialize)]
    struct ServiceView {
        service_id: String,
        logical_service_id: String,
        sdk_facade_name: String,
        surface: String,
        dependencies: Vec<&'static str>,
        rpc_count: usize,
    }

    let mut by_id: BTreeMap<String, ServiceView> = BTreeMap::new();
    for service in &manifest.services {
        let Some(native) = service.native_service.as_ref() else {
            continue;
        };
        let entry = by_id
            .entry(native.service_id.clone())
            .or_insert_with(|| ServiceView {
                service_id: native.service_id.clone(),
                logical_service_id: native.logical_service_id.clone(),
                sdk_facade_name: native.sdk_facade_name.clone(),
                surface: surface_of(native).label().to_string(),
                dependencies: native_dependency_tokens(native),
                rpc_count: 0,
            });
        entry.rpc_count += service.methods.len();
    }
    let views: Vec<ServiceView> = by_id.into_values().collect();

    if json {
        super::output_json(&views, "native services");
        return 0;
    }

    // Grouped, human-readable listing by surface.
    for surface in [Surface::DataPlane, Surface::ControlPlane, Surface::Peer] {
        let group: Vec<&ServiceView> = views
            .iter()
            .filter(|v| v.surface == surface.label())
            .collect();
        if group.is_empty() {
            continue;
        }
        println!("[{}]", surface.label());
        for v in group {
            println!(
                "  {:<18} logical={:<14} facade={:<10} rpcs={:<3} deps={}",
                v.service_id,
                v.logical_service_id,
                v.sdk_facade_name,
                v.rpc_count,
                if v.dependencies.is_empty() {
                    "-".to_string()
                } else {
                    v.dependencies.join(",")
                }
            );
        }
    }
    0
}

// ── `native add` / `native remove` ───────────────────────────────────────────

fn load_manifest(out_dir: &str) -> Result<Option<NativeServicesManifest>, String> {
    let path = manifest_path(out_dir);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let parsed: NativeServicesManifest =
        serde_json::from_slice(&bytes).map_err(|e| format!("parse {}: {e}", path.display()))?;
    Ok(Some(parsed))
}

fn write_manifest(out_dir: &str, manifest: &NativeServicesManifest) -> Result<(), String> {
    let path = manifest_path(out_dir);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    let body = serde_json::to_string_pretty(manifest)
        .map_err(|e| format!("serialize native-services.json: {e}"))?;
    std::fs::write(&path, format!("{body}\n")).map_err(|e| format!("write {}: {e}", path.display()))
}

fn regenerate_command(services: &[String]) -> String {
    format!(
        "udb native add {} && udb native generate",
        services.join(" ")
    )
}

pub(crate) fn run_add(services: &[String], out_dir: &str) -> i32 {
    if services.is_empty() {
        eprintln!("native add: no services given (e.g. `udb native add auth storage`)");
        return 1;
    }
    let expanded = expand_aliases(services);
    let unknown = unknown_ids(&expanded);
    if !unknown.is_empty() {
        eprintln!(
            "native add: unknown service(s): {}\n  known: {}",
            unknown.join(", "),
            known_service_ids()
                .into_iter()
                .collect::<Vec<_>>()
                .join(", ")
        );
        return 1;
    }

    let existing = match load_manifest(out_dir) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("native add: {err}");
            return 1;
        }
    };
    let mut set: BTreeSet<String> = existing
        .as_ref()
        .map(|m| m.services.iter().cloned().collect())
        .unwrap_or_default();
    let before = set.len();
    for id in &expanded {
        set.insert(id.clone());
    }
    let merged: Vec<String> = set.into_iter().collect();
    let manifest = NativeServicesManifest {
        descriptor_version: descriptor_version(),
        generator_version: generator_version(),
        services: merged.clone(),
        lang: existing
            .as_ref()
            .map(|m| m.lang.clone())
            .unwrap_or_default(),
        framework: existing
            .as_ref()
            .map(|m| m.framework.clone())
            .unwrap_or_default(),
        regenerate_command: regenerate_command(&merged),
    };
    if let Err(err) = write_manifest(out_dir, &manifest) {
        eprintln!("native add: {err}");
        return 1;
    }
    println!(
        "native add: {} service(s) selected ({} new) → {}",
        merged.len(),
        merged.len().saturating_sub(before),
        manifest_path(out_dir).display()
    );
    println!("  services: {}", merged.join(", "));
    println!("  next: `udb native generate --out {out_dir}`");
    0
}

pub(crate) fn run_remove(services: &[String], out_dir: &str) -> i32 {
    if services.is_empty() {
        eprintln!("native remove: no services given");
        return 1;
    }
    let existing = match load_manifest(out_dir) {
        Ok(Some(value)) => value,
        Ok(None) => {
            eprintln!(
                "native remove: no {} — nothing to remove",
                manifest_path(out_dir).display()
            );
            return 1;
        }
        Err(err) => {
            eprintln!("native remove: {err}");
            return 1;
        }
    };
    let expanded = expand_aliases(services);
    let mut set: BTreeSet<String> = existing.services.iter().cloned().collect();
    let before = set.len();
    for id in &expanded {
        set.remove(id);
    }
    let merged: Vec<String> = set.into_iter().collect();
    let manifest = NativeServicesManifest {
        descriptor_version: descriptor_version(),
        generator_version: generator_version(),
        services: merged.clone(),
        lang: existing.lang.clone(),
        framework: existing.framework.clone(),
        regenerate_command: regenerate_command(&merged),
    };
    if let Err(err) = write_manifest(out_dir, &manifest) {
        eprintln!("native remove: {err}");
        return 1;
    }
    println!(
        "native remove: {} removed, {} remaining → {}",
        before.saturating_sub(merged.len()),
        merged.len(),
        manifest_path(out_dir).display()
    );
    0
}

// ── `native generate` ────────────────────────────────────────────────────────

/// Collect the env vars a selection implies: the global broker endpoint, the
/// native enablement keys, and per-service credential placeholders derived from
/// each service's required backends.
fn env_vars_for(
    manifest: &DescriptorContractManifest,
    services: &[String],
) -> Vec<(String, String)> {
    let mut vars: Vec<(String, String)> = vec![
        (
            "UDB_ENDPOINT".to_string(),
            "http://localhost:50051".to_string(),
        ),
        ("UDB_AUTH_TOKEN".to_string(), "".to_string()),
    ];
    let mut deps: BTreeSet<&'static str> = BTreeSet::new();
    let mut native_selected = false;
    for id in services {
        if id == "data" {
            continue;
        }
        if let Some(native) = native_for_id(manifest, id) {
            native_selected = true;
            for dep in native_dependency_tokens(native) {
                deps.insert(dep);
            }
            vars.push((
                format!(
                    "UDB_NATIVE_{}_ENABLED",
                    udb::runtime::service::native_registry::canonical_service_id(id)
                        .to_ascii_uppercase()
                ),
                "true".to_string(),
            ));
        }
    }
    if native_selected {
        vars.push((
            "UDB_NATIVE_SERVICES_ENABLED".to_string(),
            "true".to_string(),
        ));
    }
    for dep in deps {
        let var = match dep {
            "postgres" => "DATABASE_URL",
            "redis" => "REDIS_URL",
            "object_store" => "S3_ENDPOINT",
            "kafka" => "KAFKA_BROKERS",
            _ => continue,
        };
        vars.push((var.to_string(), "".to_string()));
    }
    vars
}

/// Facade names for the selected services, for the client-factory snippet.
fn facades_for(manifest: &DescriptorContractManifest, services: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for id in services {
        if id == "data" {
            out.push("data".to_string());
            continue;
        }
        if let Some(native) = native_for_id(manifest, id) {
            let facade = if native.sdk_facade_name.is_empty() {
                id.clone()
            } else {
                native.sdk_facade_name.clone()
            };
            out.push(facade);
        }
    }
    out.sort();
    out.dedup();
    out
}

struct GenFile {
    rel: &'static str,
    body: String,
}

/// Build (but do not yet write) the generated artifacts for a selection. Shared
/// by `native generate` and `app init`.
fn build_generated_files(
    manifest: &DescriptorContractManifest,
    services: &[String],
    lang: &str,
    framework: &str,
) -> Vec<GenFile> {
    let stamp = format!(
        "Generated by udb {} (descriptor {}). Do not edit by hand; run \
         `udb native generate` to refresh.",
        generator_version(),
        descriptor_version()
    );
    let env_vars = env_vars_for(manifest, services);
    let facades = facades_for(manifest, services);

    // udb.config.json
    let config = serde_json::json!({
        "_generated": stamp,
        "descriptor_version": descriptor_version(),
        "generator_version": generator_version(),
        "endpoint_env": "UDB_ENDPOINT",
        "auth_token_env": "UDB_AUTH_TOKEN",
        "native_services_global_switch_env": "UDB_NATIVE_SERVICES_ENABLED",
        "native_service_enablement_env_prefix": "UDB_NATIVE_",
        "services": services,
        "native_services": services
            .iter()
            .filter(|service| service.as_str() != "data")
            .collect::<Vec<_>>(),
        "facades": facades,
        "lang": lang,
        "framework": framework,
    });
    let config_body = serde_json::to_string_pretty(&config).unwrap_or_else(|_| "{}".to_string());

    // .env.example
    let mut env_body = format!("# {stamp}\n");
    for (key, value) in &env_vars {
        env_body.push_str(&format!("{key}={value}\n"));
    }

    // client factory (language-appropriate)
    let factory = client_factory_snippet(lang, &facades, &stamp);

    vec![
        GenFile {
            rel: "udb.config.json",
            body: format!("{config_body}\n"),
        },
        GenFile {
            rel: ".env.example",
            body: env_body,
        },
        GenFile {
            rel: factory.0,
            body: factory.1,
        },
        GenFile {
            rel: smoke_artifact(lang).0,
            body: smoke_artifact_body(lang, &facades, &stamp),
        },
    ]
}

/// Return `(relative_path, body)` for the client-factory file in `lang`.
fn client_factory_snippet(lang: &str, facades: &[String], stamp: &str) -> (&'static str, String) {
    match normalize_lang(lang) {
        Lang::Python => (
            "udb_client.py",
            format!(
                "# {stamp}\nimport os\nfrom udb import create_udb  # from the generated Python SDK\n\n\
                 def udb_project():\n    \"\"\"Construct the UdbProject facade for: {facades}.\"\"\"\n\
                 \x20\x20\x20\x20return create_udb(\n        endpoint=os.environ[\"UDB_ENDPOINT\"],\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20token=os.environ.get(\"UDB_AUTH_TOKEN\"),\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20services={services:?},\n    )\n",
                stamp = stamp,
                facades = facades.join(", "),
                services = facades,
            ),
        ),
        Lang::Csharp => (
            "UdbClient.cs",
            format!(
                "// {stamp}\nusing Udb.Sdk;\n\npublic static class UdbClientFactory\n{{\n\
                 \x20\x20\x20\x20// Facade for: {facades}\n\
                 \x20\x20\x20\x20public static UdbProject Project() =>\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20Udb.Project(\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20Environment.GetEnvironmentVariable(\"UDB_ENDPOINT\")!,\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20Environment.GetEnvironmentVariable(\"UDB_AUTH_TOKEN\"),\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20new[] {{ {svc_csv} }});\n}}\n",
                stamp = stamp,
                facades = facades.join(", "),
                svc_csv = facades
                    .iter()
                    .map(|f| format!("\"{f}\""))
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
        ),
        Lang::Go => (
            "udb_client.go",
            format!(
                "// {stamp}\npackage udbapp\n\nimport (\n\t\"context\"\n\t\"os\"\n\n\t\"github.com/fahara02/udb/sdk/go/udbclient\"\n)\n\n\
                 // UdbProject builds the facade for: {facades}\nfunc UdbProject() (*udbclient.Udb, error) {{\n\
                 \treturn udbclient.NewUdb(context.Background(), udbclient.Config{{\n\t\tTarget: os.Getenv(\"UDB_ENDPOINT\"),\n\
                 \t\tCredentials: udbclient.Credentials{{\n\t\t\tBearer: os.Getenv(\"UDB_AUTH_TOKEN\"),\n\t\t}},\n\
                 \t\tPurpose: \"native-app\",\n\t}})\n}}\n",
                stamp = stamp,
                facades = facades.join(", "),
            ),
        ),
        Lang::TypeScript => (
            "udb-client.ts",
            format!(
                "// {stamp}\nimport {{ createUdb }} from \"@udb/sdk\";\n\n\
                 // UdbProject facade for: {facades}\nexport const udb = createUdb({{\n\
                 \x20\x20endpoint: process.env.UDB_ENDPOINT!,\n\
                 \x20\x20token: process.env.UDB_AUTH_TOKEN,\n\
                 \x20\x20services: [{svc_csv}],\n}});\n\nexport type Udb = typeof udb;\n",
                stamp = stamp,
                facades = facades.join(", "),
                svc_csv = facades
                    .iter()
                    .map(|f| format!("\"{f}\""))
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
        ),
    }
}

fn smoke_artifact(lang: &str) -> (&'static str, ()) {
    let rel = match normalize_lang(lang) {
        Lang::Python => "udb_smoke_test.py",
        Lang::Csharp => "UdbSmokeTest.cs",
        Lang::Go => "udb_smoke_test.go",
        Lang::TypeScript => "udb.smoke.test.ts",
    };
    (rel, ())
}

fn smoke_artifact_body(lang: &str, facades: &[String], stamp: &str) -> String {
    let list = facades.join(", ");
    match normalize_lang(lang) {
        Lang::Python => format!(
            "# {stamp}\n# Smoke test: construct the facade and ping the broker.\n\
             from udb_client import udb_project\n\n\
             def test_udb_connects():\n    project = udb_project()\n\
             \x20\x20\x20\x20# Facades wired: {list}\n    assert project is not None\n"
        ),
        Lang::Csharp => format!(
            "// {stamp}\n// Smoke test: construct the facade ({list}).\n\
             public class UdbSmokeTest\n{{\n    public void Connects()\n    {{\n\
             \x20\x20\x20\x20\x20\x20\x20\x20var project = UdbClientFactory.Project();\n\
             \x20\x20\x20\x20\x20\x20\x20\x20System.Diagnostics.Debug.Assert(project != null);\n    }}\n}}\n"
        ),
        Lang::Go => format!(
            "// {stamp}\n// Smoke test: construct the facade ({list}).\npackage udbapp\n\n\
             import \"testing\"\n\nfunc TestUdbConnects(t *testing.T) {{\n\
             \tif _, err := UdbProject(); err != nil {{\n\t\tt.Fatalf(\"udb project: %v\", err)\n\t}}\n}}\n"
        ),
        Lang::TypeScript => format!(
            "// {stamp}\n// Smoke test: construct the facade ({list}).\nimport {{ udb }} from \"./udb-client\";\n\n\
             test(\"udb connects\", () => {{\n  expect(udb).toBeDefined();\n}});\n"
        ),
    }
}

#[derive(Clone, Copy)]
enum Lang {
    Python,
    Csharp,
    Go,
    TypeScript,
}

fn normalize_lang(lang: &str) -> Lang {
    match lang.to_ascii_lowercase().as_str() {
        "python" | "py" => Lang::Python,
        "csharp" | "cs" | "dotnet" | "c#" => Lang::Csharp,
        "go" | "golang" => Lang::Go,
        _ => Lang::TypeScript,
    }
}

/// Write a set of generated files under `out_dir`, overwriting generated ones
/// (they carry a regeneration stamp) but reporting each path.
fn write_generated(out_dir: &str, files: &[GenFile]) -> Result<(), String> {
    for file in files {
        let path = Path::new(out_dir).join(file.rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
        }
        std::fs::write(&path, file.body.as_bytes())
            .map_err(|e| format!("write {}: {e}", path.display()))?;
        println!("  wrote {}", path.display());
    }
    Ok(())
}

pub(crate) fn run_generate(out_dir: &str, lang: Option<&str>, framework: &str) -> i32 {
    let existing = match load_manifest(out_dir) {
        Ok(Some(value)) => value,
        Ok(None) => {
            eprintln!(
                "native generate: no {} — run `udb native add <services...>` first",
                manifest_path(out_dir).display()
            );
            return 1;
        }
        Err(err) => {
            eprintln!("native generate: {err}");
            return 1;
        }
    };
    if existing.services.is_empty() {
        eprintln!("native generate: selection is empty — add services first");
        return 1;
    }
    // CLI --lang/--framework override the recorded values when explicitly given;
    // otherwise fall back to what `add`/`app init` recorded, else the default.
    let effective_lang = match lang.map(str::trim).filter(|value| !value.is_empty()) {
        Some(explicit) => explicit.to_string(),
        None if !existing.lang.is_empty() => existing.lang.clone(),
        None => "typescript".to_string(),
    };
    let effective_framework = if framework.is_empty() {
        existing.framework.clone()
    } else {
        framework.to_string()
    };

    let manifest = descriptor_contract_manifest_static();
    let files = build_generated_files(
        manifest,
        &existing.services,
        &effective_lang,
        &effective_framework,
    );
    println!(
        "native generate: {} service(s), lang={} →",
        existing.services.len(),
        effective_lang
    );
    if let Err(err) = write_generated(out_dir, &files) {
        eprintln!("native generate: {err}");
        return 1;
    }
    // Re-stamp the manifest with the lang/framework actually used.
    let restamped = NativeServicesManifest {
        descriptor_version: descriptor_version(),
        generator_version: generator_version(),
        services: existing.services.clone(),
        lang: effective_lang,
        framework: effective_framework,
        regenerate_command: regenerate_command(&existing.services),
    };
    if let Err(err) = write_manifest(out_dir, &restamped) {
        eprintln!("native generate: {err}");
        return 1;
    }
    println!("  done. Run `udb native smoke --out {out_dir}` for next steps.");
    0
}

// ── `native doctor` ──────────────────────────────────────────────────────────

pub(crate) fn run_doctor(out_dir: &str) -> i32 {
    let mut hard_fail = false;
    let mut warned = false;
    println!("native doctor: checking {}", out_dir);

    for req in native_tool_requirements() {
        let status = if req.ok {
            "PASS"
        } else if req.required {
            "FAIL"
        } else {
            "WARN"
        };
        println!("  {status}  tool {} ({})", req.name, req.purpose);
        if !req.ok {
            println!("        install: {}", req.install);
            if req.required {
                hard_fail = true;
            } else {
                warned = true;
            }
        }
    }

    let existing = match load_manifest(out_dir) {
        Ok(Some(value)) => value,
        Ok(None) => {
            println!(
                "  FAIL  {} not found — run `udb native add <services...>`",
                manifest_path(out_dir).display()
            );
            return 1;
        }
        Err(err) => {
            println!("  FAIL  {err}");
            return 1;
        }
    };
    println!(
        "  PASS  manifest present ({} service(s))",
        existing.services.len()
    );

    // Descriptor version drift.
    let current = descriptor_version();
    if existing.descriptor_version == current {
        println!("  PASS  descriptor version {current} matches binary");
    } else {
        warned = true;
        println!(
            "  WARN  descriptor version {} != binary {} — regenerate to refresh",
            existing.descriptor_version, current
        );
    }

    // Selected services known to the descriptor.
    let unknown = unknown_ids(&existing.services);
    if unknown.is_empty() {
        println!("  PASS  all selected services are descriptor-known");
    } else {
        hard_fail = true;
        println!("  FAIL  unknown service(s): {}", unknown.join(", "));
    }

    // Expected env vars present.
    let manifest = descriptor_contract_manifest_static();
    let env_vars = env_vars_for(manifest, &existing.services);
    let mut missing = Vec::new();
    for (key, _) in &env_vars {
        if std::env::var(key).map(|v| v.is_empty()).unwrap_or(true) {
            missing.push(key.clone());
        }
    }
    if missing.is_empty() {
        println!("  PASS  all expected env vars are set");
    } else {
        warned = true;
        println!(
            "  WARN  unset env var(s): {} (see {}/.env.example)",
            missing.join(", "),
            out_dir
        );
    }

    if hard_fail {
        println!("native doctor: FAIL");
        1
    } else if warned {
        println!("native doctor: PASS with warnings");
        0
    } else {
        println!("native doctor: PASS");
        0
    }
}

struct NativeToolRequirement {
    name: &'static str,
    purpose: &'static str,
    install: &'static str,
    required: bool,
    ok: bool,
}

fn native_tool_requirements() -> Vec<NativeToolRequirement> {
    vec![
        native_tool_req(
            "buf",
            "proto export/generation",
            "Install buf from https://buf.build/docs/installation.",
            true,
        ),
        native_tool_req(
            "cmake",
            "Kafka/rdkafka native build support",
            "Install CMake and ensure it is on PATH.",
            false,
        ),
        native_tool_req(
            "perl",
            "vendored OpenSSL build support for WebAuthn",
            "Install Strawberry Perl on Windows or system Perl on Linux/macOS.",
            false,
        ),
        native_tool_req(
            "openssl",
            "certificate/key inspection",
            "Install OpenSSL if you need local certificate and key inspection.",
            false,
        ),
        native_tool_req(
            "ffmpeg",
            "native media/video workloads",
            "Install FFmpeg before enabling video transcode or caption workloads.",
            false,
        ),
        native_tool_req(
            "ghz",
            "gRPC load smoke checks",
            "Install ghz from https://ghz.sh/ for scripts/native-load-test.",
            false,
        ),
        native_tool_req(
            "docker",
            "local native service dependencies",
            "Install Docker Desktop or Docker Engine for local backend dependencies.",
            false,
        ),
        native_tool_req(
            "protoc",
            "offline protobuf generation",
            "Install protoc if you use offline generation outside buf.",
            false,
        ),
    ]
}

fn native_tool_req(
    name: &'static str,
    purpose: &'static str,
    install: &'static str,
    required: bool,
) -> NativeToolRequirement {
    NativeToolRequirement {
        name,
        purpose,
        install,
        required,
        ok: command_exists(name),
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

// ── `native smoke` ───────────────────────────────────────────────────────────

pub(crate) fn run_smoke(out_dir: &str) -> i32 {
    let existing = match load_manifest(out_dir) {
        Ok(Some(value)) => value,
        Ok(None) => {
            eprintln!(
                "native smoke: not configured — run `udb native generate` first ({} missing)",
                manifest_path(out_dir).display()
            );
            return 1;
        }
        Err(err) => {
            eprintln!("native smoke: {err}");
            return 1;
        }
    };
    let lang = if existing.lang.is_empty() {
        "typescript".to_string()
    } else {
        existing.lang.clone()
    };
    let (smoke_rel, ()) = smoke_artifact(&lang);
    let smoke_path = Path::new(out_dir).join(smoke_rel);
    if !smoke_path.exists() {
        eprintln!(
            "native smoke: no smoke artifact at {} — run `udb native generate --out {out_dir}` first",
            smoke_path.display()
        );
        return 1;
    }
    println!("native smoke: artifact present at {}", smoke_path.display());
    let how = match normalize_lang(&lang) {
        Lang::Python => format!("pytest {}", smoke_path.display()),
        Lang::Csharp => "dotnet test".to_string(),
        Lang::Go => "go test ./...".to_string(),
        Lang::TypeScript => format!("npx vitest run {}", smoke_path.display()),
    };
    println!("  run it with: {how}");
    println!("  (ensure {out_dir}/.env.example values are exported first)");
    0
}

// ── `udb app init` ───────────────────────────────────────────────────────────

pub(crate) struct AppInitArgs {
    pub lang: String,
    pub framework: String,
    pub services: Vec<String>,
    pub tenant: String,
    pub project: String,
    pub auth: String,
    pub out_dir: String,
    pub package_manager: String,
    pub confirmed: bool,
}

/// Append the app-context flags supplied to `app init` to the generated
/// `.env.example` (only the ones the caller actually provided). Keeps the flags
/// meaningful rather than accepting-and-dropping them.
fn append_app_context_env(args: &AppInitArgs) -> Result<(), String> {
    let mut extra = String::new();
    for (key, value) in [
        ("UDB_TENANT_ID", &args.tenant),
        ("UDB_PROJECT_ID", &args.project),
        ("UDB_DEFAULT_AUTH_MODE", &args.auth),
        ("UDB_PACKAGE_MANAGER", &args.package_manager),
    ] {
        if !value.is_empty() {
            extra.push_str(&format!("{key}={value}\n"));
        }
    }
    if extra.is_empty() {
        return Ok(());
    }
    let path = Path::new(&args.out_dir).join(".env.example");
    let mut body = std::fs::read_to_string(&path).unwrap_or_default();
    body.push_str("# app-init context\n");
    body.push_str(&extra);
    std::fs::write(&path, body).map_err(|e| format!("write {}: {e}", path.display()))
}

pub(crate) fn run_app_init(args: AppInitArgs) -> i32 {
    let _ = args.confirmed;

    // Default selection: authn + authz (expanded from `auth` minus apikey would
    // surprise; use the explicit pair the spec names).
    let requested = if args.services.is_empty() {
        vec!["authn".to_string(), "authz".to_string()]
    } else {
        args.services.clone()
    };
    let expanded = expand_aliases(&requested);
    let unknown = unknown_ids(&expanded);
    if !unknown.is_empty() {
        eprintln!(
            "app init: unknown service(s): {}\n  known: {}",
            unknown.join(", "),
            known_service_ids()
                .into_iter()
                .collect::<Vec<_>>()
                .join(", ")
        );
        return 1;
    }

    // 1. Write the .udb/native-services.json manifest.
    let manifest_record = NativeServicesManifest {
        descriptor_version: descriptor_version(),
        generator_version: generator_version(),
        services: expanded.clone(),
        lang: args.lang.clone(),
        framework: args.framework.clone(),
        regenerate_command: regenerate_command(&expanded),
    };
    if let Err(err) = write_manifest(&args.out_dir, &manifest_record) {
        eprintln!("app init: {err}");
        return 1;
    }
    println!(
        "app init: wrote {} ({} service(s))",
        manifest_path(&args.out_dir).display(),
        expanded.len()
    );

    // 2. Emit config/env/client-factory/smoke via the shared generator.
    let manifest = descriptor_contract_manifest_static();
    let files = build_generated_files(manifest, &expanded, &args.lang, &args.framework);
    if let Err(err) = write_generated(&args.out_dir, &files) {
        eprintln!("app init: {err}");
        return 1;
    }

    // Thread the app-context flags (tenant/project/auth/package_manager) into the
    // generated `.env.example` as concrete defaults so they are not inert inputs.
    if let Err(err) = append_app_context_env(&args) {
        eprintln!("app init: {err}");
        return 1;
    }

    // 3. Print a short usage snippet.
    let facades = facades_for(&manifest, &expanded);
    println!("\napp init: ready. Services wired: {}", facades.join(", "));
    println!(
        "  1. cp {0}/.env.example {0}/.env  &&  fill in UDB_ENDPOINT / UDB_AUTH_TOKEN",
        args.out_dir
    );
    println!("  2. import the facade from the generated client file");
    println!(
        "  3. `udb native doctor --out {}` to validate",
        args.out_dir
    );
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alias_auth_expands_to_authn_authz_apikey() {
        let out = expand_aliases(&["auth".to_string()]);
        assert_eq!(out, vec!["apikey", "authn", "authz"]);
    }

    #[test]
    fn alias_all_expands_to_every_native_service() {
        let out = expand_aliases(&["all".to_string()]);
        let known_native = native_service_registry()
            .into_iter()
            .map(|entry| entry.service_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(out.iter().cloned().collect::<BTreeSet<_>>(), known_native);
        assert!(!out.contains(&"data".to_string()));
    }

    #[test]
    fn alias_webrtc_expands_to_all_webrtc_services() {
        let out = expand_aliases(&["webrtc".to_string()]);
        assert!(out.contains(&"webrtc_signaling".to_string()));
        assert!(out.contains(&"webrtc_turn".to_string()));
        assert_eq!(out.len(), 5);
    }

    #[test]
    fn alias_canonical_ids_pass_through_and_dedup() {
        let out = expand_aliases(&[
            "storage".to_string(),
            "storage".to_string(),
            "notify".to_string(),
        ]);
        assert_eq!(out, vec!["notification", "storage"]);
    }

    #[test]
    fn known_ids_include_registry_services_and_data() {
        let known = known_service_ids();
        assert!(known.contains("authn"));
        assert!(known.contains("storage"));
        assert!(known.contains("data"));
    }

    #[test]
    fn unknown_ids_flags_garbage() {
        let bad = unknown_ids(&["authn".to_string(), "nope".to_string()]);
        assert_eq!(bad, vec!["nope".to_string()]);
    }

    #[test]
    fn manifest_round_trips_through_json() {
        let m = NativeServicesManifest {
            descriptor_version: "9.9".to_string(),
            generator_version: "1.2.3".to_string(),
            services: vec!["authn".to_string(), "authz".to_string()],
            lang: "typescript".to_string(),
            framework: "next".to_string(),
            regenerate_command: "udb native add authn authz && udb native generate".to_string(),
        };
        let json = serde_json::to_string(&m).expect("serialize");
        let back: NativeServicesManifest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(m, back);
    }

    #[test]
    fn native_generate_explicit_lang_overrides_recorded_manifest_lang() {
        let root =
            std::env::temp_dir().join(format!("udb-native-generate-lang-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create temp native app dir");
        let out_dir = root.to_string_lossy().to_string();
        let manifest = NativeServicesManifest {
            descriptor_version: descriptor_version(),
            generator_version: generator_version(),
            services: vec!["authn".to_string()],
            lang: "python".to_string(),
            framework: String::new(),
            regenerate_command: "udb native add authn && udb native generate".to_string(),
        };
        write_manifest(&out_dir, &manifest).expect("write native manifest");

        let code = run_generate(&out_dir, Some("typescript"), "");

        assert_eq!(code, 0);
        assert!(
            root.join("udb-client.ts").is_file(),
            "explicit --lang typescript should emit the TypeScript factory"
        );
        let restamped = load_manifest(&out_dir)
            .expect("load restamped manifest")
            .expect("manifest exists");
        assert_eq!(restamped.lang, "typescript");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn surface_classification_prefers_peer_then_control_plane() {
        let manifest = descriptor_contract_manifest_static();
        // storage is a control-plane native service in the shipped descriptors.
        if let Some(storage) = native_for_id(manifest, "storage") {
            assert_eq!(surface_of(storage), Surface::ControlPlane);
        }
    }

    #[test]
    fn env_vars_include_endpoint_and_native_enablement() {
        let manifest = descriptor_contract_manifest_static();
        let vars = env_vars_for(manifest, &["authn".to_string()]);
        assert!(vars.iter().any(|(k, _)| k == "UDB_ENDPOINT"));
        assert!(vars.iter().any(|(k, _)| k == "UDB_NATIVE_SERVICES_ENABLED"));
        assert!(vars.iter().any(|(k, _)| k == "UDB_NATIVE_AUTHN_ENABLED"));
    }

    #[test]
    fn generated_files_carry_config_env_factory_and_smoke() {
        let manifest = descriptor_contract_manifest_static();
        let files = build_generated_files(manifest, &["authn".to_string()], "typescript", "");
        let rels: Vec<&str> = files.iter().map(|f| f.rel).collect();
        assert!(rels.contains(&"udb.config.json"));
        assert!(rels.contains(&".env.example"));
        assert!(rels.contains(&"udb-client.ts"));
        assert!(rels.contains(&"udb.smoke.test.ts"));
        let config = files
            .iter()
            .find(|file| file.rel == "udb.config.json")
            .expect("config file");
        assert!(config.body.contains("UDB_NATIVE_SERVICES_ENABLED"));
    }

    #[test]
    fn go_client_factory_uses_real_udbclient_module_and_api() {
        let facades = vec!["auth".to_string(), "data".to_string()];
        let (rel, go) = client_factory_snippet("go", &facades, "test stamp");

        assert_eq!(rel, "udb_client.go");
        assert!(
            go.contains("\"github.com/fahara02/udb/sdk/go/udbclient\""),
            "Go native app snippet must import the real Go SDK facade package"
        );
        assert!(
            go.contains("udbclient.NewUdb(context.Background(), udbclient.Config{"),
            "Go native app snippet must call the real NewUdb constructor"
        );
        assert!(
            go.contains("Target: os.Getenv(\"UDB_ENDPOINT\")"),
            "Go native app snippet must populate the real Config.Target field"
        );
        assert!(
            go.contains("Credentials: udbclient.Credentials{"),
            "Go native app snippet must use the real Credentials API"
        );
        assert!(
            !go.contains("github.com/udb-project/"),
            "Go native app snippet still references the fictional udb-project org"
        );
        assert!(
            !go.contains("CreateUdb") && !go.contains("udb.Options"),
            "Go native app snippet still references nonexistent SDK symbols"
        );
    }
}
