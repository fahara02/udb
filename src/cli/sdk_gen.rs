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
//!     `{{SERVICE_NAME}}`, `{{SERVICE_PKG}}`, `{{SERVICE_FULL}}`.
//!   * **Per-service blocks** — `@@UDB_SERVICE_BEGIN`/`@@UDB_SERVICE_END`, repeated
//!     per service, with `{{SERVICE_NAME}}`, `{{SERVICE_PKG}}`, `{{SERVICE_FULL}}`,
//!     `{{SERVICE_RPC_COUNT}}`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use udb::runtime::sdk_manifest::{RpcDescriptor, rpc_manifest};

use super::SdkAction;

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
pub(crate) fn run(action: SdkAction, lang: &str, templates_dir: &str, out_dir: &str) -> i32 {
    match action {
        SdkAction::Manifest => emit_manifest_json(),
        SdkAction::ListLangs => list_languages(templates_dir),
        SdkAction::Generate => generate(lang, templates_dir, out_dir),
    }
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
    serde_json::json!({
        "method": rpc.method,
        "method_snake": rpc.method_snake,
        "input": rpc.input_short,
        "input_pkg": rpc.input_pkg,
        "output": rpc.output_short,
        "output_pkg": rpc.output_pkg,
        "client_streaming": rpc.client_streaming,
        "server_streaming": rpc.server_streaming,
        "kind": rpc.kind(),
        "path": rpc.grpc_path(),
    })
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
fn generate(lang: &str, templates_dir: &str, out_dir: &str) -> i32 {
    let mut fsm = Fsm::new();

    // ── Start ─▶ LoadManifest ───────────────────────────────────────────────
    if fsm.go(SdkGenState::LoadManifest).is_err() {
        return 1;
    }
    let manifest = rpc_manifest();
    if manifest.is_empty() {
        return fsm.fail("RPC manifest empty (descriptor-set build mismatch)".to_string());
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
/// `kind`. Unknown tokens are ignored.
struct BlockFilter {
    service: Option<String>,
    kind: Option<String>,
}

fn parse_filter(filter: &str) -> BlockFilter {
    let mut service = None;
    let mut kind = None;
    for token in filter.split_whitespace() {
        if let Some(v) = token.strip_prefix("service=") {
            service = Some(v.to_string());
        } else if let Some(v) = token.strip_prefix("kind=") {
            kind = Some(v.to_string());
        }
    }
    BlockFilter { service, kind }
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
    let pairs: [(&str, String); 13] = [
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
    fn fsm_rejects_illegal_transition() {
        let mut fsm = Fsm::new();
        // Start may not jump straight to Render.
        assert!(fsm.go(SdkGenState::Render).is_err());
        assert_eq!(fsm.state, SdkGenState::Failed);
        assert!(SdkGenState::Failed.is_terminal());
    }
}
