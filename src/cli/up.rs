//! `udb up`: one command from a project's protos to a served catalog.
//!
//! It builds the manifest, checks the database schema against it, gives the
//! project its catalog (the first bootstrap, nothing when the ACTIVE catalog
//! already matches, or the reviewed transition when it does not), reconciles
//! the declared policies, identities, embedding sources and seed data, and
//! reports the project check and the `.env` posture. Each step prints one line;
//! a step that needs a person names the exact next command and only the
//! credentials that command needs. Running it again changes nothing that is
//! already in place.
//!
//! The decisions live in `drive` over the `UpBackend` trait, so they are tested
//! without a database or a broker; `LiveBackend` wraps the functions the
//! standalone commands (`udb verify --live`, `udb catalog bootstrap`, `udb
//! policy apply`, `udb catalog transition`) already run.

use std::io::{BufRead, IsTerminal};
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use super::catalog_transition::{TransitionAction, TransitionCommand};
use super::*;

/// `udb up` flags. Empty strings mean "not given".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct UpOptions {
    /// `-f`: the declaration file; empty means `udb.yaml` when it exists.
    pub file: String,
    pub project: String,
    pub tenant: String,
    pub dsn: String,
    /// `--proto`: the proto directory the catalog is built from.
    pub proto: String,
    /// `--approve <run-id>`: approve exactly this reviewed transition run.
    pub approve: String,
    pub dry_run: bool,
    /// Confirms changes that need no review (the first activation; any change
    /// under UDB_ENV=production). Never approves a reviewed transition.
    pub yes: bool,
    pub allow_transfer: bool,
    /// Machine mode: the JSON report on stdout only, no step lines on stderr.
    pub json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StepStatus {
    /// A read-only step found nothing wrong.
    Ok,
    /// The step changed something.
    Done,
    /// Nothing to change.
    Skipped,
    /// A person must act; the step names the command.
    Next,
    Failed,
}

impl StepStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Done => "done",
            Self::Skipped => "skipped",
            Self::Next => "next",
            Self::Failed => "failed",
        }
    }
}

/// Steps that only report: a `[next]` from them never stops the run or
/// changes the exit code.
const ADVISORY_STEPS: &[&str] = &["check", "runtime-flags"];

const NEEDS_PG: &str = "UDB_PG_DSN (or DATABASE_URL, or --dsn)";
const NEEDS_BROKER: &str = "UDB_AUTH_TOKEN (or UDB_BEARER_TOKEN) set to an authorized operator \
                            bearer, and UDB_GRPC_TARGET (or catalog.target in udb.yaml)";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StepOutcome {
    pub step: &'static str,
    pub status: StepStatus,
    pub detail: String,
    pub next_command: Option<String>,
    /// The credentials or settings `next_command` needs, and nothing else.
    pub next_command_needs: Option<String>,
    /// A command that shows what a person is asked to review.
    pub review_command: Option<String>,
}

impl StepOutcome {
    fn new(step: &'static str, status: StepStatus, detail: impl Into<String>) -> Self {
        Self {
            step,
            status,
            detail: detail.into(),
            next_command: None,
            next_command_needs: None,
            review_command: None,
        }
    }

    fn ok(step: &'static str, detail: impl Into<String>) -> Self {
        Self::new(step, StepStatus::Ok, detail)
    }

    fn done(step: &'static str, detail: impl Into<String>) -> Self {
        Self::new(step, StepStatus::Done, detail)
    }

    fn skipped(step: &'static str, detail: impl Into<String>) -> Self {
        Self::new(step, StepStatus::Skipped, detail)
    }

    fn failed(step: &'static str, detail: impl Into<String>) -> Self {
        Self::new(step, StepStatus::Failed, detail)
    }

    fn next(
        step: &'static str,
        detail: impl Into<String>,
        command: impl Into<String>,
        needs: Option<&str>,
    ) -> Self {
        let mut outcome = Self::new(step, StepStatus::Next, detail);
        outcome.next_command = Some(command.into());
        outcome.next_command_needs = needs.map(str::to_string);
        outcome
    }

    fn with_next(mut self, command: impl Into<String>, needs: Option<&str>) -> Self {
        self.next_command = Some(command.into());
        self.next_command_needs = needs.map(str::to_string);
        self
    }

    fn with_review(mut self, command: impl Into<String>) -> Self {
        self.review_command = Some(command.into());
        self
    }

    fn to_json(&self) -> Value {
        json!({
            "step": self.step,
            "status": self.status.label(),
            "detail": self.detail,
            "next_command": self.next_command,
            "next_command_needs": self.next_command_needs,
            "review_command": self.review_command,
        })
    }

    fn render(&self) -> String {
        let mut out = format!("[{}] {} {}", self.status.label(), self.step, self.detail);
        if let Some(command) = &self.review_command {
            out.push_str(&format!("\n  Review: {command}"));
        }
        if let Some(command) = &self.next_command {
            out.push_str(&format!("\n  Next: {command}"));
        }
        if let Some(needs) = &self.next_command_needs {
            out.push_str(&format!("\n  Needs: {needs}"));
        }
        out
    }
}

/// The step lines of one run, printed as they happen.
pub(crate) struct Steps {
    pub(crate) outcomes: Vec<StepOutcome>,
    quiet: bool,
    dry_run: bool,
}

impl Steps {
    fn new(quiet: bool, dry_run: bool) -> Self {
        Self {
            outcomes: Vec::new(),
            quiet,
            dry_run,
        }
    }

    /// Record a step; true when the run must stop here. A dry run goes on past
    /// `[next]` so it shows every pending change; nothing goes on past a
    /// failure.
    fn record(&mut self, outcome: StepOutcome) -> bool {
        if !self.quiet {
            eprintln!("{}", outcome.render());
        }
        let stop = match outcome.status {
            StepStatus::Failed => true,
            StepStatus::Next => !self.dry_run && !ADVISORY_STEPS.contains(&outcome.step),
            _ => false,
        };
        self.outcomes.push(outcome);
        stop
    }

    /// 0 all done or skipped; 3 stopped at a step that needs a person; 1 a step
    /// failed; 2 the inputs were incomplete.
    pub(crate) fn exit_code(&self) -> i32 {
        if let Some(failed) = self
            .outcomes
            .iter()
            .find(|outcome| outcome.status == StepStatus::Failed)
        {
            return if failed.step == "inputs" { 2 } else { 1 };
        }
        let waiting = self.outcomes.iter().any(|outcome| {
            outcome.status == StepStatus::Next && !ADVISORY_STEPS.contains(&outcome.step)
        });
        if waiting && !self.dry_run { 3 } else { 0 }
    }
}

/// Everything `drive` decides with, resolved before any step runs.
pub(crate) struct UpContext {
    pub(crate) options: UpOptions,
    pub(crate) project: String,
    pub(crate) tenant: String,
    /// `catalog.target` from udb.yaml; `None` leaves it to UDB_GRPC_TARGET.
    pub(crate) target: Option<String>,
    pub(crate) proto_root: String,
    pub(crate) production: bool,
    /// An operator bearer and a broker target are set (the reviewed
    /// transition runs through the broker).
    pub(crate) broker_ready: bool,
    /// A person can answer a prompt on this terminal.
    pub(crate) interactive: bool,
    /// Where reviewed-transition response files are kept.
    pub(crate) transitions_dir: PathBuf,
}

impl UpContext {
    /// `udb up` again with this run's addressing flags plus `extra`. The DSN
    /// is never repeated: it carries a password.
    fn up_command(&self, extra: &str) -> String {
        let mut command = "udb up".to_string();
        if !self.options.file.trim().is_empty() {
            command.push_str(&format!(" -f {}", self.options.file.trim()));
        }
        command.push_str(&format!(
            " --project {} --tenant {}",
            self.project, self.tenant
        ));
        if !self.options.proto.trim().is_empty() {
            command.push_str(&format!(" --proto {}", self.options.proto.trim()));
        }
        if self.options.allow_transfer {
            command.push_str(" --allow-transfer");
        }
        if !extra.is_empty() {
            command.push(' ');
            command.push_str(extra);
        }
        command
    }

    /// `--yes` when this run needs it to change anything.
    fn yes_flag(&self) -> &'static str {
        if self.production { "--yes" } else { "" }
    }

    /// True when a change must wait: UDB_ENV=production without --yes.
    fn needs_yes(&self) -> bool {
        self.production && !self.options.yes
    }

    fn transition(&self, action: TransitionAction) -> TransitionCommand {
        TransitionCommand::new(
            action,
            &self.project,
            Some(self.tenant.clone()),
            self.target.clone(),
        )
    }

    /// The manual `udb catalog transition status` command for this project
    /// (and run, when known).
    fn status_command(&self, run: Option<&str>) -> String {
        let flags = super::catalog_transition::connection_flags(
            &self.transition(TransitionAction::Status),
            &self.tenant,
        );
        match run {
            Some(run) => format!("udb catalog transition status {flags} --run-id {run}"),
            None => format!("udb catalog transition status {flags}"),
        }
    }
}

/// What `udb up` reads and changes. `LiveBackend` is the real one.
pub(crate) trait UpBackend {
    /// Live-schema differences from `manifest`, one message each.
    async fn schema_drift(&mut self, manifest: &CatalogManifest) -> Result<Vec<String>, String>;
    /// The ACTIVE catalog as `(catalog_id, schema checksum)`.
    async fn active_catalog(&mut self, project: &str) -> Result<Option<(String, String)>, String>;
    /// Stage and activate the project's first catalog; returns its id.
    async fn bootstrap(&mut self, project: &str, manifest_json: &[u8]) -> Result<String, String>;
    async fn reconcile_policies(
        &mut self,
        tenant: &str,
        declared: Vec<AuthzPolicy>,
        apply: bool,
    ) -> Result<udb::runtime::service::PolicyReconcileReport, String>;
    /// One `udb catalog transition` command.
    async fn transition(&mut self, command: TransitionCommand) -> Result<Value, String>;
}

/// The schema, catalog and policy steps. Stops at the first step that must
/// stop; the caller runs the remaining steps only when this returns true.
pub(crate) async fn drive<B: UpBackend>(
    ctx: &UpContext,
    manifest: &CatalogManifest,
    policies: &[AuthzPolicy],
    backend: &mut B,
    steps: &mut Steps,
    report: &mut serde_json::Map<String, Value>,
    approve_prompt: &mut dyn FnMut(&str, &Value) -> bool,
) -> bool {
    // Observe the catalog before judging the schema: differences the reviewed
    // transition applies are expected, the same differences under an
    // unchanged catalog are not.
    let drift = match backend.schema_drift(manifest).await {
        Ok(drift) => drift,
        Err(err) => {
            steps.record(StepOutcome::failed(
                "schema",
                format!("could not compare the database with the protos because {err}"),
            ));
            return false;
        }
    };
    let active = match backend.active_catalog(&ctx.project).await {
        Ok(active) => active,
        Err(err) => {
            steps.record(StepOutcome::failed(
                "catalog",
                format!("could not read the ACTIVE catalog because {err}"),
            ));
            return false;
        }
    };
    let differs = active
        .as_ref()
        .is_some_and(|(_, checksum)| checksum != &manifest.checksum_sha256);
    if steps.record(schema_step(ctx, manifest, &drift, differs)) {
        return false;
    }
    let catalog = catalog_step(ctx, manifest, active, backend, approve_prompt).await;
    report.insert("catalog".into(), json!({ "detail": catalog.detail }));
    if steps.record(catalog) {
        return false;
    }
    let (policy, policy_report) = policy_step(ctx, policies, backend).await;
    if let Some(policy_report) = policy_report {
        report.insert("policies".into(), policy_report);
    }
    !steps.record(policy)
}

fn schema_step(
    ctx: &UpContext,
    manifest: &CatalogManifest,
    drift: &[String],
    catalog_differs: bool,
) -> StepOutcome {
    if drift.is_empty() {
        return StepOutcome::ok(
            "schema",
            format!(
                "the database matches the protos ({} tables)",
                manifest.tables.len()
            ),
        );
    }
    if catalog_differs {
        return StepOutcome::ok(
            "schema",
            format!(
                "{} difference(s) from the database; the reviewed catalog transition applies them",
                drift.len()
            ),
        );
    }
    let detail = format!(
        "udb up stopped because the database differs from the protos in {} place(s) (first: {})",
        drift.len(),
        drift[0]
    );
    if ctx.production {
        // Production never migrates on its own: the operator plans the change
        // and points the broker's approval gate at the signed plan.
        StepOutcome::next(
            "schema",
            format!("{detail}; UDB_ENV=production never migrates automatically"),
            format!(
                "udb plan {} --prior <manifest exported from the release now running> \
                 --emit-approval-plan plan.json",
                ctx.proto_root
            ),
            Some("migration.require_approval_plan on the broker pointing at plan.json"),
        )
    } else {
        StepOutcome::next(
            "schema",
            format!("{detail}; let the broker migrate it, then run udb up again"),
            format!("UDB_MIGRATE_ENABLED=true udb serve {}", ctx.proto_root),
            Some(NEEDS_PG),
        )
    }
}

async fn catalog_step<B: UpBackend>(
    ctx: &UpContext,
    manifest: &CatalogManifest,
    active: Option<(String, String)>,
    backend: &mut B,
    approve_prompt: &mut dyn FnMut(&str, &Value) -> bool,
) -> StepOutcome {
    let Some((active_id, active_checksum)) = active else {
        if ctx.options.dry_run {
            return StepOutcome::next(
                "catalog",
                format!(
                    "project '{}' has no ACTIVE catalog; would bootstrap one from the protos (dry run)",
                    ctx.project
                ),
                ctx.up_command(ctx.yes_flag()),
                Some(NEEDS_PG),
            );
        }
        if ctx.needs_yes() {
            return StepOutcome::next(
                "catalog",
                format!(
                    "the first catalog for project '{}' waits because UDB_ENV=production needs --yes for every change",
                    ctx.project
                ),
                ctx.up_command("--yes"),
                Some(NEEDS_PG),
            );
        }
        let manifest_json = match serde_json::to_vec(manifest) {
            Ok(bytes) => bytes,
            Err(err) => {
                return StepOutcome::failed(
                    "catalog",
                    format!("could not encode the manifest: {err}"),
                );
            }
        };
        return match backend.bootstrap(&ctx.project, &manifest_json).await {
            Ok(catalog_id) => StepOutcome::done(
                "catalog",
                format!(
                    "project '{}' now has an ACTIVE catalog ({catalog_id}, {} tables)",
                    ctx.project,
                    manifest.tables.len()
                ),
            ),
            Err(err) => StepOutcome::failed(
                "catalog",
                format!(
                    "bootstrapping project '{}' failed because {err}",
                    ctx.project
                ),
            ),
        };
    };
    if active_checksum == manifest.checksum_sha256 {
        return StepOutcome::skipped(
            "catalog",
            format!("unchanged; the ACTIVE catalog ({active_id}) matches the protos"),
        );
    }
    transition_step(ctx, manifest, &active_id, backend, approve_prompt).await
}

/// Advance a differing ACTIVE catalog through the reviewed transition: plan,
/// approve (only by `--approve <run-id>` or a person at the terminal), apply,
/// stage and activate. The plan's idempotency key is derived from the ACTIVE
/// catalog and the target, so a re-run replays the same broker run; each
/// finished step leaves a response file, and an existing file means the step
/// is done.
async fn transition_step<B: UpBackend>(
    ctx: &UpContext,
    manifest: &CatalogManifest,
    active_id: &str,
    backend: &mut B,
    approve_prompt: &mut dyn FnMut(&str, &Value) -> bool,
) -> StepOutcome {
    let differs = format!("the ACTIVE catalog ({active_id}) differs from the protos");
    if ctx.options.dry_run {
        return StepOutcome::next(
            "catalog",
            format!("{differs}; would plan a reviewed transition (dry run)"),
            ctx.up_command(ctx.yes_flag()),
            Some(NEEDS_BROKER),
        );
    }
    if !ctx.broker_ready {
        return StepOutcome::next(
            "catalog",
            format!(
                "{differs}; the reviewed transition waits because it runs through the broker and \
                 no operator bearer or broker target is set"
            ),
            ctx.up_command(ctx.yes_flag()),
            Some(NEEDS_BROKER),
        )
        .with_review(ctx.status_command(None));
    }
    if ctx.needs_yes() {
        return StepOutcome::next(
            "catalog",
            format!(
                "{differs}; planning the transition waits because UDB_ENV=production needs --yes"
            ),
            ctx.up_command("--yes"),
            Some(NEEDS_BROKER),
        );
    }

    let target: String = manifest.checksum_sha256.chars().take(16).collect();
    let base: String = active_id.chars().take(8).collect();
    let project: String = ctx
        .project
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let dir = ctx
        .transitions_dir
        .join(format!("{project}-{base}-{target}"));
    if let Err(err) = fs::create_dir_all(&dir) {
        return StepOutcome::failed(
            "catalog",
            format!("could not create {}: {err}", dir.display()),
        );
    }
    let path = |name: &str| dir.join(name).display().to_string();
    let failed = |action: &str, err: String, run: Option<&str>| {
        StepOutcome::failed(
            "catalog",
            format!("the reviewed transition {action} failed because {err}"),
        )
        .with_next(ctx.status_command(run), Some(NEEDS_BROKER))
    };

    // The exact candidate the broker plans, stages and activates.
    let manifest_path = path("manifest.json");
    if !Path::new(&manifest_path).exists() {
        let value = match serde_json::to_value(manifest) {
            Ok(value) => value,
            Err(err) => return failed("plan", err.to_string(), None),
        };
        if let Err(err) = super::catalog_transition::write_new_json(&manifest_path, &value) {
            return failed("plan", err, None);
        }
    }

    let plan_path = path("plan.json");
    let plan = if Path::new(&plan_path).exists() {
        match super::catalog_transition::read_json(&plan_path) {
            Ok(plan) => plan,
            Err(err) => return failed("plan", err, None),
        }
    } else {
        // The broker's own record of the ACTIVE catalog supplies the outer
        // integrity the plan is bound to.
        let status = match backend
            .transition(ctx.transition(TransitionAction::Status))
            .await
        {
            Ok(status) => status,
            Err(err) => return failed("status", err, None),
        };
        if status["catalog_id"].as_str() != Some(active_id) {
            return StepOutcome::failed(
                "catalog",
                format!(
                    "the broker reports ACTIVE catalog {} but the database has {active_id}; \
                     the broker target and the DSN address different deployments",
                    status["catalog_id"].as_str().unwrap_or("(none)")
                ),
            )
            .with_next(ctx.status_command(None), Some(NEEDS_BROKER));
        }
        let mut command = ctx.transition(TransitionAction::Plan);
        command.manifest = Some(manifest_path.clone());
        command.expected_active_catalog_id = Some(active_id.to_string());
        let Some(integrity) = status["manifest_integrity_sha256"].as_str() else {
            return failed(
                "status",
                "the broker returned no ACTIVE manifest integrity".to_string(),
                None,
            );
        };
        command.expected_active_manifest_integrity_sha256 = Some(integrity.to_string());
        command.out = Some(plan_path.clone());
        command.idempotency_key = Some(format!(
            "up-plan:{}:{active_id}:{}",
            ctx.project, manifest.checksum_sha256
        ));
        match backend.transition(command).await {
            Ok(plan) => plan,
            Err(err) => return failed("plan", err, None),
        }
    };
    let Some(run) = plan["run_id"].as_str().map(str::to_string) else {
        return failed("plan", format!("{plan_path} has no run_id"), None);
    };
    let list = |field: &str| -> Vec<String> {
        plan[field]
            .as_array()
            .map(|values| {
                values
                    .iter()
                    .filter_map(|value| value.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };
    let operations = list("operations");
    let blocked = list("blocked");
    if !blocked.is_empty() {
        return StepOutcome::failed(
            "catalog",
            format!(
                "the reviewed transition {run} is refused because {} operation(s) are blocked (first: {})",
                blocked.len(),
                blocked[0]
            ),
        )
        .with_next(ctx.status_command(Some(run.as_str())), Some(NEEDS_BROKER));
    }

    let approval_path = path("approval.json");
    if !Path::new(&approval_path).exists() {
        let approve = ctx.options.approve.trim();
        let approved_here = approve == run
            || (ctx.interactive && approve.is_empty() && approve_prompt(&run, &plan));
        if !approved_here {
            let mismatch = if approve.is_empty() {
                String::new()
            } else {
                format!(" (--approve {approve} does not name it)")
            };
            return StepOutcome::next(
                "catalog",
                format!(
                    "{differs}; reviewed transition {run} waits for approval{mismatch}: {} operation(s), \
                     {} marked for review. --yes never approves it",
                    operations.len(),
                    list("requires_review").len()
                ),
                ctx.up_command(format!("--approve {run} {}", ctx.yes_flag()).trim()),
                Some(NEEDS_BROKER),
            )
            .with_review(ctx.status_command(Some(run.as_str())));
        }
        let mut command = ctx.transition(TransitionAction::Approve);
        command.plan = Some(plan_path.clone());
        command.out = Some(approval_path.clone());
        command.idempotency_key = Some(format!("up-approve:{run}"));
        if let Err(err) = backend.transition(command).await {
            return failed("approve", err, Some(run.as_str()));
        }
    }

    let applied_path = path("applied.json");
    if !Path::new(&applied_path).exists() {
        let mut command = ctx.transition(TransitionAction::Apply);
        command.approval = Some(approval_path.clone());
        command.idempotency_key = Some(format!("up-apply:{run}"));
        match backend.transition(command).await {
            Ok(value) => {
                if let Err(err) = super::catalog_transition::write_new_json(&applied_path, &value) {
                    return failed("apply", err, Some(run.as_str()));
                }
            }
            Err(err) => return failed("apply", err, Some(run.as_str())),
        }
    }

    let staged_path = path("staged.json");
    let staged = if Path::new(&staged_path).exists() {
        super::catalog_transition::read_json(&staged_path)
    } else {
        let mut command = ctx.transition(TransitionAction::Stage);
        command.manifest = Some(manifest_path.clone());
        command.run_id = Some(run.clone());
        command.idempotency_key = Some(format!("up-stage:{run}"));
        command.reason = format!("udb up: reviewed transition {run}");
        backend.transition(command).await.and_then(|value| {
            super::catalog_transition::write_new_json(&staged_path, &value).map(|()| value)
        })
    };
    let staged_id = match staged {
        Ok(staged) => match staged["catalog_id"].as_str() {
            Some(id) => id.to_string(),
            None => {
                return failed(
                    "stage",
                    format!("{staged_path} has no catalog_id"),
                    Some(run.as_str()),
                );
            }
        },
        Err(err) => return failed("stage", err, Some(run.as_str())),
    };

    let activated_path = path("activated.json");
    if !Path::new(&activated_path).exists() {
        let mut command = ctx.transition(TransitionAction::Activate);
        command.catalog_id = Some(staged_id.clone());
        command.run_id = Some(run.clone());
        command.idempotency_key = Some(format!("up-activate:{run}"));
        command.reason = format!("udb up: reviewed transition {run}");
        match backend.transition(command).await {
            Ok(value) => {
                if let Err(err) = super::catalog_transition::write_new_json(&activated_path, &value)
                {
                    return failed("activate", err, Some(run.as_str()));
                }
            }
            Err(err) => return failed("activate", err, Some(run.as_str())),
        }
    }
    StepOutcome::done(
        "catalog",
        format!(
            "the ACTIVE catalog is now {staged_id} (reviewed transition {run}, {} operation(s))",
            operations.len()
        ),
    )
}

/// Compare the declared policies with the live rows, then apply when there is
/// a difference and this run may change things.
async fn policy_step<B: UpBackend>(
    ctx: &UpContext,
    policies: &[AuthzPolicy],
    backend: &mut B,
) -> (StepOutcome, Option<Value>) {
    if policies.is_empty() {
        return (
            StepOutcome::skipped("policies", "none declared in udb.yaml"),
            None,
        );
    }
    let diff = match backend
        .reconcile_policies(&ctx.tenant, policies.to_vec(), false)
        .await
    {
        Ok(diff) => diff,
        Err(err) => {
            return (
                StepOutcome::failed(
                    "policies",
                    format!("comparing the declared policies failed because {err}"),
                ),
                None,
            );
        }
    };
    let to_json = |report: &udb::runtime::service::PolicyReconcileReport| {
        serde_json::to_value(report).unwrap_or_default()
    };
    let changes = diff.added.len() + diff.removed.len() + diff.duplicates.len();
    if changes == 0 {
        return (
            StepOutcome::skipped(
                "policies",
                format!(
                    "unchanged ({} declared rule(s) already active)",
                    diff.unchanged
                ),
            ),
            Some(to_json(&diff)),
        );
    }
    let summary = format!(
        "add {}, soft-delete {} active rule(s) the file no longer declares, retire {} duplicate(s)",
        diff.added.len(),
        diff.removed.len(),
        diff.duplicates.len()
    );
    if ctx.options.dry_run {
        return (
            StepOutcome::next(
                "policies",
                format!("would {summary} (dry run)"),
                ctx.up_command(ctx.yes_flag()),
                Some(NEEDS_PG),
            ),
            Some(to_json(&diff)),
        );
    }
    if ctx.needs_yes() {
        return (
            StepOutcome::next(
                "policies",
                format!(
                    "policy apply waits because UDB_ENV=production needs --yes; it would {summary}"
                ),
                ctx.up_command("--yes"),
                Some(NEEDS_PG),
            ),
            Some(to_json(&diff)),
        );
    }
    match backend
        .reconcile_policies(&ctx.tenant, policies.to_vec(), true)
        .await
    {
        Ok(applied) => (
            StepOutcome::done(
                "policies",
                format!(
                    "added {}, soft-deleted {}, retired {} duplicate(s)",
                    applied.added.len(),
                    applied.removed.len(),
                    applied.duplicates.len()
                ),
            ),
            Some(to_json(&applied)),
        ),
        Err(err) => (
            StepOutcome::failed(
                "policies",
                format!("applying the declared policies failed because {err}"),
            ),
            Some(to_json(&diff)),
        ),
    }
}

/// The project check `udb check` runs: catalog lint plus, when policies are
/// declared, entity coverage. Report-only.
fn check_step(
    ctx: &UpContext,
    manifest: &CatalogManifest,
    policies: &[AuthzPolicy],
    policies_path: Option<&str>,
) -> StepOutcome {
    let lint = lint_catalog(manifest);
    let uncovered = if policies.is_empty() {
        0
    } else {
        udb::lint_policy_entity_coverage(policies, &manifest_entities(manifest))
            .iter()
            .filter(|finding| finding.severity == "error")
            .count()
    };
    if lint.passed && uncovered == 0 {
        return StepOutcome::ok(
            "check",
            format!(
                "catalog lint passed ({} warning(s)){}",
                lint.warning_count,
                if policies.is_empty() {
                    "; no policies declared, coverage not checked"
                } else {
                    "; every entity has a policy"
                }
            ),
        );
    }
    let command = match policies_path {
        Some(path) => format!("udb check {} --policies {path}", ctx.proto_root),
        None => format!("udb check {}", ctx.proto_root),
    };
    StepOutcome::next(
        "check",
        format!(
            "the project check found {} catalog error(s) and {} policy coverage error(s)",
            lint.error_count, uncovered
        ),
        command,
        None,
    )
}

/// Whether `.env` here has what its profile needs. Report-only.
fn runtime_flags_step() -> StepOutcome {
    match super::env_fill::check_missing(Path::new(".env"), Path::new(".udb/keys")) {
        Ok(None) => StepOutcome::skipped(
            "runtime-flags",
            "no .env here; check the broker's own env file with `udb env fill --check --file <path>`",
        ),
        Ok(Some((profile, missing))) if missing.is_empty() => StepOutcome::ok(
            "runtime-flags",
            format!(".env is complete for the {profile} profile"),
        ),
        Ok(Some((profile, missing))) => StepOutcome::next(
            "runtime-flags",
            format!(
                ".env lacks {} value(s) the {profile} profile needs: {}",
                missing.len(),
                missing.join(", ")
            ),
            "udb env fill --file .env",
            None,
        ),
        Err(err) => StepOutcome::next(
            "runtime-flags",
            format!("could not check .env because {err}"),
            "udb env fill --check --file .env",
            None,
        ),
    }
}

/// The real backend: the catalog store and the policy rows over `dsn`, the
/// reviewed transition over the broker.
struct LiveBackend {
    runtime: DataBrokerRuntime,
    dsn: String,
}

impl UpBackend for LiveBackend {
    async fn schema_drift(&mut self, manifest: &CatalogManifest) -> Result<Vec<String>, String> {
        self.runtime
            .verify_postgres_manifest_drift(manifest)
            .await
            .map(|drift| drift.into_iter().map(|finding| finding.message).collect())
            .map_err(|err| err.message().to_string())
    }

    async fn active_catalog(&mut self, project: &str) -> Result<Option<(String, String)>, String> {
        self.runtime
            .active_project_catalog_schema_checksum(project)
            .await
            .map_err(|err| err.message().to_string())
    }

    async fn bootstrap(&mut self, project: &str, manifest_json: &[u8]) -> Result<String, String> {
        self.runtime
            .bootstrap_project_catalog(project, manifest_json, "udb up: first catalog", "udb-up")
            .await
            .map_err(|err| err.message().to_string())
    }

    async fn reconcile_policies(
        &mut self,
        tenant: &str,
        declared: Vec<AuthzPolicy>,
        apply: bool,
    ) -> Result<udb::runtime::service::PolicyReconcileReport, String> {
        udb::runtime::service::reconcile_authz_policies_offline(
            &self.dsn, tenant, declared, apply, "udb-up",
        )
        .await
    }

    async fn transition(&mut self, command: TransitionCommand) -> Result<Value, String> {
        super::catalog_transition::run_async(command).await
    }
}

fn env_set(name: &str) -> bool {
    env::var(name).is_ok_and(|value| !value.trim().is_empty())
}

/// Print the report and return the exit code.
fn finish(steps: &Steps, mut report: serde_json::Map<String, Value>) -> i32 {
    report.insert(
        "steps".into(),
        Value::Array(steps.outcomes.iter().map(StepOutcome::to_json).collect()),
    );
    output_json(&Value::Object(report), "udb up report");
    steps.exit_code()
}

/// `udb up`. Exit codes: 0 every step done or skipped, 3 stopped at a step that
/// needs a person, 1 a step failed, 2 the inputs were incomplete.
pub(crate) async fn run(options: UpOptions) -> i32 {
    let mut steps = Steps::new(options.json, options.dry_run);
    let mut report = serde_json::Map::new();
    report.insert("dry_run".into(), Value::Bool(options.dry_run));

    // 0 inputs: flags, then udb.yaml, then the environment.
    let (file, declared) = match super::ops_cli::load_project_file(&options.file) {
        Ok(loaded) => loaded,
        Err(err) => {
            steps.record(StepOutcome::failed(
                "inputs",
                format!("udb up refused because the declaration file could not be read: {err}"),
            ));
            return finish(&steps, report);
        }
    };
    let project = super::ops_cli::pick(&options.project, &declared.project, "UDB_PROJECT_ID");
    let tenant = super::ops_cli::pick(&options.tenant, &declared.tenant, "UDB_TENANT_ID");
    let first_set = |values: [String; 4]| {
        values
            .into_iter()
            .map(|value| value.trim().to_string())
            .find(|value| !value.is_empty())
    };
    let dsn = first_set([
        options.dsn.clone(),
        env::var("UDB_PG_DSN").unwrap_or_default(),
        env::var("DATABASE_URL").unwrap_or_default(),
        String::new(),
    ])
    .unwrap_or_default();
    // A proto_root in udb.yaml is relative to the file, like its other paths.
    let proto_root = first_set([
        options.proto.clone(),
        (!declared.proto_root.trim().is_empty())
            .then(|| super::ops_cli::relative_to(&file, declared.proto_root.trim()))
            .unwrap_or_default(),
        env::var("UDB_PROTO_ROOT").unwrap_or_default(),
        env::var("UDB_PROTO_DIR").unwrap_or_default(),
    ])
    .unwrap_or_else(|| "proto".to_string());
    let namespace = super::ops_cli::pick("", &declared.proto_namespace, "UDB_PROTO_NAMESPACE");
    report.insert("project_id".into(), Value::String(project.clone()));
    report.insert("tenant_id".into(), Value::String(tenant.clone()));

    let mut missing = Vec::new();
    let mut needs = Vec::new();
    if project.is_empty() {
        missing.push("project");
        needs.push("`project:` in udb.yaml, --project, or UDB_PROJECT_ID");
    }
    if tenant.is_empty() {
        missing.push("tenant");
        needs.push("`tenant:` in udb.yaml, --tenant, or UDB_TENANT_ID (the canonical tenant UUID)");
    }
    if dsn.is_empty() {
        missing.push("PostgreSQL DSN");
        needs.push("UDB_PG_DSN (or DATABASE_URL), or --dsn <postgres-dsn>");
    }
    let production = env::var("UDB_ENV").is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "production" | "prod"
        )
    });
    let ctx = UpContext {
        project: if project.is_empty() {
            "<project-id>".to_string()
        } else {
            project.clone()
        },
        tenant: if tenant.is_empty() {
            "<tenant-uuid>".to_string()
        } else {
            tenant.clone()
        },
        target: Some(declared.catalog.target.trim().to_string()).filter(|value| !value.is_empty()),
        proto_root: proto_root.clone(),
        production,
        broker_ready: (env_set("UDB_AUTH_TOKEN") || env_set("UDB_BEARER_TOKEN"))
            && (!declared.catalog.target.trim().is_empty()
                || env_set("UDB_GRPC_TARGET")
                || env_set("UDB_GRPC_ADDR")),
        interactive: !production
            && !options.json
            && std::io::stdin().is_terminal()
            && std::io::stderr().is_terminal(),
        transitions_dir: PathBuf::from(".udb").join("transitions"),
        options,
    };
    if !missing.is_empty() {
        steps.record(
            StepOutcome::failed(
                "inputs",
                format!(
                    "udb up refused because the {} {} not set",
                    missing.join(" and the "),
                    if missing.len() == 1 { "is" } else { "are" }
                ),
            )
            .with_next(ctx.up_command(""), Some(&needs.join("; "))),
        );
        return finish(&steps, report);
    }
    steps.record(StepOutcome::ok(
        "inputs",
        format!("project {project}, tenant {tenant}, protos {proto_root}"),
    ));

    // 1 manifest: the same effective manifest `serve` stores.
    let manifest =
        match build_effective_manifest(&resolve_existing_project_path(&proto_root), &namespace) {
            Ok(manifest) => manifest,
            Err(err) => {
                steps.record(StepOutcome::failed(
                    "manifest",
                    format!("could not build the catalog manifest from {proto_root} because {err}"),
                ));
                return finish(&steps, report);
            }
        };
    steps.record(StepOutcome::ok(
        "manifest",
        format!(
            "{} tables, {} stores, checksum {}",
            manifest.tables.len(),
            manifest.stores.len(),
            manifest
                .checksum_sha256
                .chars()
                .take(12)
                .collect::<String>()
        ),
    ));

    let policies = match declared.declared_policies(&file).and_then(|policies| {
        if policies.is_empty() {
            Ok(policies)
        } else {
            udb::runtime::service::normalize_declared_policies(&tenant, policies)
        }
    }) {
        Ok(policies) => policies,
        Err(err) => {
            steps.record(StepOutcome::failed(
                "policies",
                format!("the declared policies are invalid: {err}"),
            ));
            return finish(&steps, report);
        }
    };

    // 2-4 schema, catalog, policies.
    #[allow(unused_unsafe)]
    unsafe {
        env::set_var("UDB_PG_DSN", &dsn);
    }
    let runtime = DataBrokerRuntime::from_env().await;
    if runtime.pg_pool_clone().is_none() {
        steps.record(
            StepOutcome::failed(
                "schema",
                "could not compare the database with the protos because PostgreSQL is not reachable",
            )
            .with_next(ctx.up_command(""), Some(NEEDS_PG)),
        );
        return finish(&steps, report);
    }
    let mut backend = LiveBackend {
        runtime,
        dsn: dsn.clone(),
    };
    let mut approve_prompt = |run: &str, plan: &Value| -> bool {
        eprintln!("reviewed catalog transition {run} applies:");
        for operation in plan["operations"].as_array().into_iter().flatten() {
            eprintln!("  {}", operation.as_str().unwrap_or_default());
        }
        for operation in plan["requires_review"].as_array().into_iter().flatten() {
            eprintln!("  review: {}", operation.as_str().unwrap_or_default());
        }
        eprint!("Approve and apply transition {run}? [y/N]: ");
        let mut line = String::new();
        std::io::stdin().lock().read_line(&mut line).is_ok()
            && matches!(line.trim().to_ascii_lowercase().as_str(), "y" | "yes")
    };
    if !drive(
        &ctx,
        &manifest,
        &policies,
        &mut backend,
        &mut steps,
        &mut report,
        &mut approve_prompt,
    )
    .await
    {
        return finish(&steps, report);
    }

    // 5-7 identities, embedding, seed: the standalone commands' code.
    let mutate = !ctx.options.dry_run && !ctx.needs_yes();
    let waiting = |step: &'static str, what: &str| {
        if ctx.options.dry_run {
            StepOutcome::next(
                step,
                format!("would {what} (dry run)"),
                ctx.up_command(ctx.yes_flag()),
                None,
            )
        } else {
            StepOutcome::next(
                step,
                format!("{what} waits because UDB_ENV=production needs --yes"),
                ctx.up_command("--yes"),
                None,
            )
        }
    };
    let accounts = match declared.declared_accounts(&file, &tenant) {
        Ok(accounts) => accounts,
        Err(err) => {
            steps.record(StepOutcome::failed(
                "identities",
                format!("the declared service accounts are invalid: {err}"),
            ));
            return finish(&steps, report);
        }
    };
    let outcome = if accounts.is_empty() {
        StepOutcome::skipped("identities", "none declared in udb.yaml")
    } else {
        match super::ops_cli::reconcile_identities(
            &accounts,
            &tenant,
            mutate,
            ctx.options.allow_transfer,
        )
        .await
        {
            Ok(result) => {
                report.insert("identities".into(), result);
                if mutate {
                    StepOutcome::done(
                        "identities",
                        format!("reconciled {} service account(s)", accounts.len()),
                    )
                } else {
                    waiting(
                        "identities",
                        &format!("reconcile {} service account(s)", accounts.len()),
                    )
                }
            }
            Err(err) => StepOutcome::failed(
                "identities",
                format!("reconciling service accounts failed because {err}"),
            ),
        }
    };
    if steps.record(outcome) {
        return finish(&steps, report);
    }

    match declared.declared_pipelines(&file) {
        Ok(names) if names.is_empty() => {}
        Ok(names) => {
            report.insert(
                "pipelines".into(),
                json!({
                    "declared": names,
                    "run_by": "the broker, with UDB_PIPELINES_FILE pointing at this file",
                }),
            );
        }
        Err(err) => {
            steps.record(StepOutcome::failed(
                "pipelines",
                format!("the declared pipelines are invalid: {err}"),
            ));
            return finish(&steps, report);
        }
    }

    let outcome = match &declared.embedding {
        None => StepOutcome::skipped("embedding", "none declared in udb.yaml"),
        Some(section) => match super::ops_cli::embedding_up(&tenant, section, !mutate).await {
            Ok(result) => {
                let pending = ["models", "sources"]
                    .iter()
                    .flat_map(|key| result[*key].as_array().cloned().unwrap_or_default())
                    .filter(|entry| entry["action"] != "unchanged")
                    .count();
                report.insert("embedding".into(), result);
                if pending == 0 {
                    StepOutcome::skipped(
                        "embedding",
                        "every declared model and source is registered",
                    )
                } else if mutate {
                    StepOutcome::done(
                        "embedding",
                        format!("{pending} model(s) or source(s) registered or reported"),
                    )
                } else {
                    waiting(
                        "embedding",
                        &format!("register {pending} model(s) or source(s)"),
                    )
                }
            }
            Err(err) => StepOutcome::failed(
                "embedding",
                format!("registering embedding models and sources failed because {err}"),
            ),
        },
    };
    if steps.record(outcome) {
        return finish(&steps, report);
    }

    let seed = match declared.declared_seed(&file) {
        Ok(seed) => seed,
        Err(err) => {
            steps.record(StepOutcome::failed(
                "seed",
                format!("the declared seed data is invalid: {err}"),
            ));
            return finish(&steps, report);
        }
    };
    let outcome = match seed {
        None => StepOutcome::skipped("seed", "none declared in udb.yaml"),
        Some(seed) => match super::ops_cli::seed_records(&seed, &tenant, &project, !mutate).await {
            Ok(result) => {
                let records = result["written"]
                    .as_u64()
                    .or_else(|| result["records"].as_u64())
                    .unwrap_or(0);
                report.insert("seed".into(), result);
                if mutate {
                    StepOutcome::done("seed", format!("upserted {records} record(s)"))
                } else {
                    waiting("seed", &format!("upsert {records} record(s)"))
                }
            }
            Err(err) => StepOutcome::failed(
                "seed",
                format!("writing the seed data failed because {err}"),
            ),
        },
    };
    if steps.record(outcome) {
        return finish(&steps, report);
    }

    // 8-9 report-only.
    let policies_path = declared.policies_path(&file);
    steps.record(check_step(
        &ctx,
        &manifest,
        &policies,
        policies_path.as_deref(),
    ));
    steps.record(runtime_flags_step());
    finish(&steps, report)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Records every call; answers from fixed state.
    #[derive(Default)]
    struct FakeBackend {
        drift: Vec<String>,
        active: Option<(String, String)>,
        policy_diff: udb::runtime::service::PolicyReconcileReport,
        calls: Vec<String>,
    }

    impl UpBackend for FakeBackend {
        async fn schema_drift(
            &mut self,
            _manifest: &CatalogManifest,
        ) -> Result<Vec<String>, String> {
            self.calls.push("schema_drift".into());
            Ok(self.drift.clone())
        }

        async fn active_catalog(
            &mut self,
            _project: &str,
        ) -> Result<Option<(String, String)>, String> {
            self.calls.push("active_catalog".into());
            Ok(self.active.clone())
        }

        async fn bootstrap(
            &mut self,
            _project: &str,
            _manifest_json: &[u8],
        ) -> Result<String, String> {
            self.calls.push("bootstrap".into());
            Ok("cat-1".into())
        }

        async fn reconcile_policies(
            &mut self,
            _tenant: &str,
            _declared: Vec<AuthzPolicy>,
            apply: bool,
        ) -> Result<udb::runtime::service::PolicyReconcileReport, String> {
            self.calls.push(format!("reconcile_policies apply={apply}"));
            let mut report = self.policy_diff.clone();
            report.applied = apply;
            Ok(report)
        }

        async fn transition(&mut self, command: TransitionCommand) -> Result<Value, String> {
            self.calls.push(format!("transition {:?}", command.action));
            Ok(match command.action {
                TransitionAction::Status => json!({
                    "catalog_id": "active-1",
                    "manifest_integrity_sha256": "a".repeat(64),
                }),
                TransitionAction::Plan => json!({
                    "schema_version": 1, "run_id": "run-1", "project_id": command.project,
                    "state": "PREFLIGHT", "operations": ["add column notes.body"],
                    "requires_review": ["add column notes.body"], "blocked": [],
                }),
                TransitionAction::Approve => json!({"run_id": "run-1", "state": "APPROVED"}),
                TransitionAction::Apply => json!({"run_id": "run-1", "state": "COMPLETED"}),
                TransitionAction::Stage => json!({"catalog_id": "staged-1", "status": "STAGED"}),
                TransitionAction::Activate => json!({"catalog_id": "staged-1", "status": "ACTIVE"}),
            })
        }
    }

    fn manifest() -> CatalogManifest {
        CatalogManifest {
            checksum_sha256: "b".repeat(64),
            ..Default::default()
        }
    }

    fn context(options: UpOptions) -> UpContext {
        UpContext {
            options,
            project: "notes".into(),
            tenant: "7d0f5bb4-0000-4000-8000-000000000001".into(),
            target: None,
            proto_root: "proto".into(),
            production: false,
            broker_ready: true,
            interactive: false,
            transitions_dir: std::env::temp_dir()
                .join(format!("udb-up-test-{}", uuid::Uuid::new_v4())),
        }
    }

    fn policy() -> AuthzPolicy {
        AuthzPolicy {
            id: "p1".into(),
            action: "data.select".into(),
            ..Default::default()
        }
    }

    fn with_pending_policy() -> udb::runtime::service::PolicyReconcileReport {
        udb::runtime::service::PolicyReconcileReport {
            added: vec![policy()],
            ..Default::default()
        }
    }

    async fn drive_once(ctx: &UpContext, backend: &mut FakeBackend) -> (Steps, bool) {
        let mut steps = Steps::new(true, ctx.options.dry_run);
        let mut report = serde_json::Map::new();
        let mut never = |_: &str, _: &Value| -> bool { panic!("no prompt off a terminal") };
        let went_on = drive(
            ctx,
            &manifest(),
            &[policy()],
            backend,
            &mut steps,
            &mut report,
            &mut never,
        )
        .await;
        (steps, went_on)
    }

    fn statuses(steps: &Steps) -> Vec<(&'static str, StepStatus)> {
        steps
            .outcomes
            .iter()
            .map(|outcome| (outcome.step, outcome.status))
            .collect()
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(future)
    }

    #[test]
    fn a_fresh_project_is_bootstrapped_and_its_policies_applied() {
        let ctx = context(UpOptions::default());
        let mut backend = FakeBackend {
            policy_diff: with_pending_policy(),
            ..Default::default()
        };
        let (steps, went_on) = block_on(drive_once(&ctx, &mut backend));
        assert!(went_on);
        assert_eq!(
            statuses(&steps),
            vec![
                ("schema", StepStatus::Ok),
                ("catalog", StepStatus::Done),
                ("policies", StepStatus::Done),
            ]
        );
        assert!(backend.calls.contains(&"bootstrap".to_string()));
        assert!(
            backend
                .calls
                .contains(&"reconcile_policies apply=true".to_string())
        );
        assert_eq!(steps.exit_code(), 0);
    }

    #[test]
    fn a_rerun_changes_nothing() {
        let ctx = context(UpOptions::default());
        let mut backend = FakeBackend {
            active: Some(("cat-1".into(), manifest().checksum_sha256)),
            ..Default::default()
        };
        let (steps, went_on) = block_on(drive_once(&ctx, &mut backend));
        assert!(went_on);
        assert_eq!(
            statuses(&steps),
            vec![
                ("schema", StepStatus::Ok),
                ("catalog", StepStatus::Skipped),
                ("policies", StepStatus::Skipped),
            ]
        );
        assert!(!backend.calls.contains(&"bootstrap".to_string()));
        assert!(
            !backend
                .calls
                .iter()
                .any(|call| call.ends_with("apply=true"))
        );
        assert_eq!(steps.exit_code(), 0);
    }

    #[test]
    fn a_differing_active_catalog_stops_for_review_with_exact_commands() {
        let ctx = context(UpOptions::default());
        let mut backend = FakeBackend {
            active: Some(("active-1".into(), "c".repeat(64))),
            drift: vec!["column notes.body is missing".into()],
            ..Default::default()
        };
        let (steps, went_on) = block_on(drive_once(&ctx, &mut backend));
        assert!(!went_on);
        assert_eq!(
            statuses(&steps),
            vec![("schema", StepStatus::Ok), ("catalog", StepStatus::Next)]
        );
        assert_eq!(steps.exit_code(), 3);
        let catalog = &steps.outcomes[1];

        // The review command is a complete `catalog transition` invocation.
        let review: Vec<String> = catalog
            .review_command
            .as_deref()
            .expect("review command")
            .split_whitespace()
            .skip(1)
            .map(str::to_string)
            .collect();
        let parsed = super::super::catalog_transition::parse(&review).expect("review parses");
        assert_eq!(parsed.action, TransitionAction::Status);
        assert_eq!(parsed.run_id.as_deref(), Some("run-1"));

        // The next command is `udb up` approving exactly that run.
        let next: Vec<String> = catalog
            .next_command
            .as_deref()
            .expect("next command")
            .split_whitespace()
            .skip(1)
            .map(str::to_string)
            .collect();
        let Command::Ops(OpsCommand::Up(options)) = parse_args(&next).0 else {
            panic!("the next command parses as udb up")
        };
        assert_eq!(options.approve, "run-1");
        assert_eq!(options.project, "notes");
        let _ = fs::remove_dir_all(&ctx.transitions_dir);
    }

    #[test]
    fn yes_never_approves_a_reviewed_transition() {
        let ctx = context(UpOptions {
            yes: true,
            ..Default::default()
        });
        let mut backend = FakeBackend {
            active: Some(("active-1".into(), "c".repeat(64))),
            ..Default::default()
        };
        let (steps, _) = block_on(drive_once(&ctx, &mut backend));
        assert_eq!(steps.exit_code(), 3);
        assert!(
            !backend.calls.iter().any(|call| call.contains("Approve")),
            "{:?}",
            backend.calls
        );
        let _ = fs::remove_dir_all(&ctx.transitions_dir);
    }

    #[test]
    fn approve_naming_the_run_finishes_the_transition_and_keeps_response_files() {
        let ctx = context(UpOptions {
            approve: "run-1".into(),
            ..Default::default()
        });
        let mut backend = FakeBackend {
            active: Some(("active-1".into(), "c".repeat(64))),
            ..Default::default()
        };
        let (steps, went_on) = block_on(drive_once(&ctx, &mut backend));
        assert!(went_on, "{:?}", steps.outcomes);
        assert_eq!(steps.outcomes[1].status, StepStatus::Done);
        assert!(steps.outcomes[1].detail.contains("staged-1"));
        let transitions: Vec<&String> = backend
            .calls
            .iter()
            .filter(|call| call.starts_with("transition"))
            .collect();
        assert_eq!(
            transitions,
            vec![
                "transition Status",
                "transition Plan",
                "transition Approve",
                "transition Apply",
                "transition Stage",
                "transition Activate",
            ]
        );
        // plan.json and approval.json are written by the real commands; the
        // rest are written by `udb up` itself.
        let dir = fs::read_dir(&ctx.transitions_dir)
            .expect("transition dir")
            .next()
            .expect("one run dir")
            .expect("entry")
            .path();
        for name in [
            "manifest.json",
            "applied.json",
            "staged.json",
            "activated.json",
        ] {
            assert!(dir.join(name).exists(), "{name}");
        }
        let _ = fs::remove_dir_all(&ctx.transitions_dir);
    }

    #[test]
    fn production_without_yes_changes_nothing() {
        let mut ctx = context(UpOptions::default());
        ctx.production = true;
        let mut backend = FakeBackend {
            policy_diff: with_pending_policy(),
            ..Default::default()
        };
        let (steps, went_on) = block_on(drive_once(&ctx, &mut backend));
        assert!(!went_on);
        let catalog = &steps.outcomes[1];
        assert_eq!(catalog.status, StepStatus::Next);
        assert!(
            catalog
                .next_command
                .as_deref()
                .is_some_and(|command| command.ends_with("--yes")),
            "{catalog:?}"
        );
        assert!(!backend.calls.contains(&"bootstrap".to_string()));
        assert_eq!(steps.exit_code(), 3);
    }

    #[test]
    fn schema_drift_under_an_unchanged_catalog_names_the_migration() {
        let ctx = context(UpOptions::default());
        let mut backend = FakeBackend {
            drift: vec!["table notes.note is missing".into()],
            ..Default::default()
        };
        let (steps, _) = block_on(drive_once(&ctx, &mut backend));
        assert_eq!(statuses(&steps), vec![("schema", StepStatus::Next)]);
        assert!(
            steps.outcomes[0]
                .next_command
                .as_deref()
                .is_some_and(|command| command.contains("UDB_MIGRATE_ENABLED=true udb serve"))
        );
        let mut ctx = ctx;
        ctx.production = true;
        let (steps, _) = block_on(drive_once(&ctx, &mut backend));
        assert!(
            steps.outcomes[0]
                .next_command
                .as_deref()
                .is_some_and(|command| command.contains("--emit-approval-plan"))
        );
    }

    #[test]
    fn dry_run_reports_every_pending_change_and_exits_zero() {
        let ctx = context(UpOptions {
            dry_run: true,
            ..Default::default()
        });
        let mut backend = FakeBackend {
            policy_diff: with_pending_policy(),
            ..Default::default()
        };
        let (steps, went_on) = block_on(drive_once(&ctx, &mut backend));
        assert!(went_on);
        assert_eq!(
            statuses(&steps),
            vec![
                ("schema", StepStatus::Ok),
                ("catalog", StepStatus::Next),
                ("policies", StepStatus::Next),
            ]
        );
        assert!(!backend.calls.contains(&"bootstrap".to_string()));
        assert!(
            !backend
                .calls
                .iter()
                .any(|call| call.ends_with("apply=true"))
        );
        assert_eq!(steps.exit_code(), 0);
    }

    #[test]
    fn missing_inputs_are_a_usage_refusal_and_advisory_steps_never_stop() {
        let mut steps = Steps::new(true, false);
        assert!(!steps.record(StepOutcome::next("check", "lint", "udb check proto", None)));
        assert_eq!(steps.exit_code(), 0);
        assert!(steps.record(StepOutcome::failed("inputs", "no tenant is set")));
        assert_eq!(steps.exit_code(), 2);
    }
}
