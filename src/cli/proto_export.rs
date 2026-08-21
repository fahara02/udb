//! `udb proto export` — FSM-driven, idempotent export/sync of UDB's embedded
//! proto contract into a user project.
//!
//! The flow is modeled as an explicit finite-state machine (mirroring
//! [`crate::control::engine::FsmState`]) so every step is auditable and the
//! ordering/guards are enforced rather than implied by call order:
//!
//! ```text
//!   Start ─▶ BufYaml ─▶ ProtoDir ─▶ SyncProtos ─▶ VendorThirdParty ─▶ Completed
//!     └──────────┴──────────┴───────────┴───────────────┴───────────────▶ Failed
//! ```
//!
//! Behaviour at each state (idempotent — safe to re-run):
//!   * `BufYaml`          — at the project root, create `buf.yaml` if absent; if
//!                          it exists, **merge** (add the proto module path + UDB
//!                          deps) without clobbering the user's other config.
//!   * `ProtoDir`         — ensure the proto root exists (create `proto/` if
//!                          absent).
//!   * `SyncProtos`       — write the entire embedded `udb/**` contract (the
//!                          annotation contract *and* the broker wire surface:
//!                          `udb/entity`, `udb/services`, `udb/events`); if a file
//!                          already exists, **consistency-check** it (unchanged vs
//!                          stale) and refresh stale copies (proto is the source
//!                          of truth).
//!   * `VendorThirdParty` — write the vendored `google/api/**` protos the UDB
//!                          contract imports into a sibling `third_party/googleapis`
//!                          dir, so the tree compiles offline with bare
//!                          `protoc -I <proto> -I third_party/googleapis`. (buf
//!                          users resolve these via the `deps` added to buf.yaml,
//!                          so the vendored copy stays *outside* the buf module to
//!                          avoid a duplicate-import conflict.)

use std::path::Path;

/// States of the proto-export FSM. Canonical uppercase strings are stable for
/// JSON/log output, matching the `FsmState` convention elsewhere in UDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProtoExportState {
    Start,
    BufYaml,
    ProtoDir,
    SyncProtos,
    VendorThirdParty,
    Completed,
    Failed,
}

impl ProtoExportState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Start => "START",
            Self::BufYaml => "BUF_YAML",
            Self::ProtoDir => "PROTO_DIR",
            Self::SyncProtos => "SYNC_PROTOS",
            Self::VendorThirdParty => "VENDOR_THIRD_PARTY",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }

    pub(crate) fn valid_transitions(self) -> &'static [ProtoExportState] {
        use ProtoExportState::*;
        match self {
            Start => &[BufYaml, Failed],
            BufYaml => &[ProtoDir, Failed],
            ProtoDir => &[SyncProtos, Failed],
            SyncProtos => &[VendorThirdParty, Failed],
            VendorThirdParty => &[Completed, Failed],
            Completed | Failed => &[],
        }
    }

    pub(crate) fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }
}

/// Per-file outcome of the `SyncProtos` state.
#[derive(Debug)]
struct FileOutcome {
    path: String,
    status: &'static str, // "created" | "updated" | "unchanged"
}

/// Drives the FSM. Returns a process exit code (0 success, 1 failure).
pub(crate) fn run(
    out_dir: &str,
    manage_buf_yaml: bool,
    format_proto: bool,
    confirmed: bool,
) -> i32 {
    let proto_dir = if out_dir.trim().is_empty() {
        "proto".to_string()
    } else {
        out_dir.trim().trim_end_matches(['/', '\\']).to_string()
    };

    let mut fsm = Fsm::new();

    // ── Start ──▶ BufYaml ───────────────────────────────────────────────────
    if fsm.go(ProtoExportState::BufYaml).is_err() {
        return 1;
    }
    if manage_buf_yaml {
        match merge_buf_yaml(Path::new("buf.yaml"), &proto_dir, confirmed) {
            Ok(status) => fsm.note(format!("buf.yaml {status} (module path `{proto_dir}`)")),
            Err(err) => return fsm.fail(format!("buf.yaml: {err}")),
        }
    } else {
        fsm.note("buf.yaml management skipped (--no-buf-yaml)".to_string());
    }

    // ── BufYaml ──▶ ProtoDir ────────────────────────────────────────────────
    if fsm.go(ProtoExportState::ProtoDir).is_err() {
        return 1;
    }
    let root = Path::new(&proto_dir);
    if root.exists() {
        fsm.note(format!("proto dir `{proto_dir}` exists — syncing into it"));
    } else if let Err(err) = std::fs::create_dir_all(root) {
        return fsm.fail(format!("create proto dir `{proto_dir}`: {err}"));
    } else {
        fsm.note(format!("created proto dir `{proto_dir}`"));
    }

    // ── ProtoDir ──▶ SyncProtos ─────────────────────────────────────────────
    if fsm.go(ProtoExportState::SyncProtos).is_err() {
        return 1;
    }
    let files = udb::runtime::native_catalog::embedded_broker_protos();
    if files.is_empty() {
        return fsm.fail("no embedded protos found (build mismatch)".to_string());
    }
    let mut outcomes: Vec<FileOutcome> = Vec::with_capacity(files.len());
    for (import_path, contents) in &files {
        let dest = root.join(import_path);
        let bytes: &[u8] = contents;
        match sync_file(&dest, bytes) {
            Ok(status) => outcomes.push(FileOutcome {
                path: dest.to_string_lossy().replace('\\', "/"),
                status,
            }),
            Err(err) => return fsm.fail(format!("write {}: {err}", dest.display())),
        }
    }
    let created = outcomes.iter().filter(|o| o.status == "created").count();
    let updated = outcomes.iter().filter(|o| o.status == "updated").count();
    let unchanged = outcomes.iter().filter(|o| o.status == "unchanged").count();
    for o in &outcomes {
        println!("  [{}] {}", o.status, o.path);
    }
    fsm.note(format!(
        "{} proto file(s): {created} created, {updated} updated (stale), {unchanged} consistent",
        outcomes.len()
    ));

    // ── SyncProtos ──▶ VendorThirdParty ─────────────────────────────────────
    if fsm.go(ProtoExportState::VendorThirdParty).is_err() {
        return 1;
    }
    // Vendored third-party protos live OUTSIDE the buf module (a sibling
    // `third_party/googleapis` dir) so buf users — who resolve googleapis via the
    // `deps` in buf.yaml — don't hit a duplicate-import conflict, while `protoc`
    // users can add `-I third_party/googleapis` to compile offline.
    let third_party = udb::runtime::native_catalog::embedded_third_party_protos();
    let tp_root = root
        .parent()
        .map(|p| p.join("third_party").join("googleapis"))
        .unwrap_or_else(|| Path::new("third_party").join("googleapis"));
    let mut tp_created = 0usize;
    let mut tp_updated = 0usize;
    let mut tp_unchanged = 0usize;
    for (import_path, contents) in &third_party {
        let dest = tp_root.join(import_path);
        let bytes: &[u8] = contents;
        match sync_file(&dest, bytes) {
            Ok("created") => tp_created += 1,
            Ok("updated") => tp_updated += 1,
            Ok(_) => tp_unchanged += 1,
            Err(err) => return fsm.fail(format!("vendor {}: {err}", dest.display())),
        }
    }
    let tp_root_display = tp_root.to_string_lossy().replace('\\', "/");
    fsm.note(format!(
        "{} third-party proto(s) → {tp_root_display}/: {tp_created} created, {tp_updated} updated (stale), {tp_unchanged} consistent",
        third_party.len()
    ));

    if format_proto {
        match super::proto_fmt::format_tree(root, false) {
            Ok(report) => fsm.note(format!(
                "formatted exported protos: {} scanned, {} changed",
                report.scanned, report.changed
            )),
            Err(err) => return fsm.fail(format!("proto fmt: {err}")),
        }
    } else {
        fsm.note(
            "proto formatting skipped (pass `--fmt` to normalize long field annotations)"
                .to_string(),
        );
    }

    // ── VendorThirdParty ──▶ Completed ──────────────────────────────────────
    if fsm.go(ProtoExportState::Completed).is_err() {
        return 1;
    }
    println!(
        "\nproto export {state} → {proto_dir}/udb/ (annotation contract + broker wire surface) \
         and {tp_root_display}/ (vendored google/api).\n\n\
         Annotate messages with `option (udb.core.common.v1.pg_table) = {{ … }}` and fields with \
         `(udb.core.common.v1.pg_column)`; import via `import \"udb/core/common/v1/db.proto\";`.\n\
         Compile with buf (`buf generate`; googleapis comes from buf.yaml deps) or \
         offline protoc: `protoc -I {proto_dir} -I {tp_root_display} <file.proto>`.",
        state = fsm.state.as_str()
    );
    0
}

/// Minimal guarded FSM with a step log printed as it advances.
struct Fsm {
    state: ProtoExportState,
}

impl Fsm {
    fn new() -> Self {
        Self {
            state: ProtoExportState::Start,
        }
    }

    /// Transition to `next`, enforcing `valid_transitions`. Prints the step.
    fn go(&mut self, next: ProtoExportState) -> Result<(), ()> {
        if self.state.is_terminal() {
            eprintln!(
                "proto export: cannot transition out of terminal state {}",
                self.state.as_str()
            );
            return Err(());
        }
        if !self.state.valid_transitions().contains(&next) {
            eprintln!(
                "proto export: illegal transition {} → {}",
                self.state.as_str(),
                next.as_str()
            );
            self.state = ProtoExportState::Failed;
            return Err(());
        }
        self.state = next;
        println!("[{}]", next.as_str());
        Ok(())
    }

    fn note(&self, message: String) {
        println!("  {message}");
    }

    /// Drive to the terminal `Failed` state, print the reason, return exit 1.
    fn fail(&mut self, reason: String) -> i32 {
        // `Failed` is reachable from every non-terminal state.
        self.state = ProtoExportState::Failed;
        eprintln!("[{}] {reason}", ProtoExportState::Failed.as_str());
        1
    }
}

/// Write `contents` to `dest`, reporting whether the file was created, updated
/// (an existing copy drifted from the embedded source of truth), or already
/// consistent.
fn sync_file(dest: &Path, contents: &[u8]) -> Result<&'static str, String> {
    if dest.exists() {
        match std::fs::read(dest) {
            Ok(existing) if existing == contents => return Ok("unchanged"),
            Ok(_) => {}
            Err(err) => return Err(err.to_string()),
        }
        std::fs::write(dest, contents).map_err(|e| e.to_string())?;
        return Ok("updated");
    }
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    std::fs::write(dest, contents).map_err(|e| e.to_string())?;
    Ok("created")
}

/// Create-or-merge a v2 `buf.yaml` so it includes the proto module path and the
/// UDB broker-contract deps, without disturbing the user's existing config.
/// Returns `"created"`, `"merged"`, or `"unchanged"`.
fn merge_buf_yaml(path: &Path, proto_dir: &str, confirmed: bool) -> Result<&'static str, String> {
    use serde_yaml::Value;

    const DEPS: [&str; 2] = [
        "buf.build/googleapis/googleapis",
        "buf.build/grpc-ecosystem/grpc-gateway",
    ];

    if !path.exists() {
        let content = format!(
            "version: v2\nmodules:\n  - path: {proto_dir}\ndeps:\n  - {}\n  - {}\n",
            DEPS[0], DEPS[1]
        );
        std::fs::write(path, content).map_err(|e| e.to_string())?;
        return Ok("created");
    }

    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let mut doc: Value =
        serde_yaml::from_str(&text).map_err(|e| format!("parse existing buf.yaml: {e}"))?;
    let map = doc
        .as_mapping_mut()
        .ok_or_else(|| "existing buf.yaml is not a YAML mapping".to_string())?;
    let mut changed = false;

    if !map.contains_key("version") {
        map.insert(Value::from("version"), Value::from("v2"));
        changed = true;
    }

    // modules: ensure a sequence entry whose `path` is the proto dir.
    if !map.contains_key("modules") {
        map.insert(Value::from("modules"), Value::Sequence(Vec::new()));
        changed = true;
    }
    if let Some(Value::Sequence(seq)) = map.get_mut("modules") {
        let has_path = seq.iter().any(|m| {
            m.as_mapping()
                .and_then(|mm| mm.get("path"))
                .and_then(|p| p.as_str())
                == Some(proto_dir)
        });
        if !has_path {
            let mut entry = serde_yaml::Mapping::new();
            entry.insert(Value::from("path"), Value::from(proto_dir));
            seq.push(Value::Mapping(entry));
            changed = true;
        }
    } else {
        return Err("buf.yaml `modules` is not a sequence".to_string());
    }

    // deps: ensure both UDB-contract deps are present (additive).
    if !map.contains_key("deps") {
        map.insert(Value::from("deps"), Value::Sequence(Vec::new()));
        changed = true;
    }
    if let Some(Value::Sequence(seq)) = map.get_mut("deps") {
        for dep in DEPS {
            if !seq.iter().any(|d| d.as_str() == Some(dep)) {
                seq.push(Value::from(dep));
                changed = true;
            }
        }
    } else {
        return Err("buf.yaml `deps` is not a sequence".to_string());
    }

    if !changed {
        return Ok("unchanged");
    }
    // Merging is done by parsing and re-serialising, which cannot preserve
    // comments - a project that documented its own buf.yaml would silently lose
    // that documentation. Creating an ABSENT buf.yaml destroys nothing and needs
    // no consent; rewriting one the project already owns does. Say exactly what
    // would be added and how to proceed, rather than doing it unannounced.
    if !confirmed && text.contains('#') {
        return Err(format!(
            "buf.yaml already exists and carries comments that a merge cannot preserve. \
             It needs `modules: - path: {proto_dir}` plus the googleapis and \
             grpc-gateway deps. Re-run with `--yes` to let UDB rewrite it (comments \
             will be dropped), add those entries by hand, or pass `--no-buf-yaml` to \
             leave it untouched."
        ));
    }
    let out = serde_yaml::to_string(&doc).map_err(|e| e.to_string())?;
    std::fs::write(path, out).map_err(|e| e.to_string())?;
    Ok("merged")
}
