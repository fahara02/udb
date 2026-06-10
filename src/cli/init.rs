//! `udb init` command runner.

use std::fs;
use std::path::PathBuf;

use udb::init::{
    FindingSeverity, InitOptions, InitPlanError, InitProfile, ProtoStrategy, apply_init_plan,
    backend_catalog, build_init_plan, default_backend_ids_for_features, framework_catalog,
    native_service_catalog, revert_last_init,
};
use udb::tui;

use super::init_prompt::{InitPromptCatalog, run_init_prompts};
use super::output_json;

#[derive(Debug)]
pub(crate) enum InitRunError {
    ConfigRead {
        path: String,
        source: std::io::Error,
    },
    ConfigParse {
        path: String,
        source: serde_yaml::Error,
    },
    Plan(InitPlanError),
    Apply(std::io::Error),
    Prompt(super::init_prompt::InitPromptError),
    Tui(tui::TuiError),
    InvalidArgs(String),
}

impl std::fmt::Display for InitRunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConfigRead { path, source } => {
                write!(f, "failed to read init config {path}: {source}")
            }
            Self::ConfigParse { path, source } => {
                write!(f, "failed to parse init config {path}: {source}")
            }
            Self::Plan(source) => write!(f, "{source}"),
            Self::Apply(source) => write!(f, "failed to apply init plan: {source}"),
            Self::Prompt(source) => write!(f, "{source}"),
            Self::Tui(source) => write!(f, "{source}"),
            Self::InvalidArgs(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for InitRunError {}

pub(crate) fn run(args: &super::InitArgs) -> Result<i32, InitRunError> {
    let cwd = std::env::current_dir().map_err(InitRunError::Apply)?;
    if args.revert {
        let receipt = revert_last_init(&cwd).map_err(InitRunError::Apply)?;
        output_json(&receipt, "init revert receipt");
        return Ok(0);
    }

    let mut options = if let Some(config) = args.config.as_deref() {
        read_config(config)?
    } else {
        InitOptions::default()
    };

    let mut prompt_confirmed = None;
    if args.no_tui && !args.confirmed && !args.json_plan && !args.dry_run {
        let selection = run_init_prompts(default_prompt_catalog()).map_err(InitRunError::Prompt)?;
        if let Some(profile) = parse_profile(Some(&selection.profile)) {
            options.profile = profile;
        }
        options.framework = selection.framework;
        options.backends = selection.backends;
        options.native_services = selection.native_services;
        options.features = selection.features;
        prompt_confirmed = Some(selection.confirm_apply);
    }

    if let Some(profile) = args.profile.as_deref() {
        options.profile = parse_profile(Some(profile)).ok_or_else(|| {
            InitRunError::InvalidArgs(format!(
                "unknown init profile `{profile}`; expected local, dev, ci, or prod"
            ))
        })?;
    }
    if args.framework.is_some() {
        options.framework = args.framework.clone();
    }
    if !args.backends.is_empty() {
        options.backends = args.backends.clone();
    }
    if !args.native_services.is_empty() {
        options.native_services = args.native_services.clone();
    }
    if !args.features.is_empty() {
        options.features = args.features.clone();
    }
    if let Some(strategy) = args.proto_strategy.as_deref() {
        options.proto_strategy = Some(parse_proto_strategy(Some(strategy)).ok_or_else(|| {
            InitRunError::InvalidArgs(format!(
                "unknown proto strategy `{strategy}`; expected starter, existing, or skip"
            ))
        })?);
    }
    let confirmed = args.confirmed || prompt_confirmed.unwrap_or(false);
    options.dry_run = args.dry_run || args.json_plan || (!confirmed && !args.tui);

    let result = build_init_plan(&cwd, options).map_err(InitRunError::Plan)?;
    let confirmed = if args.tui && !args.no_tui {
        run_tui_preview(&result, args.force).map_err(InitRunError::Tui)?
    } else {
        confirmed
    };
    let has_errors = result
        .findings
        .iter()
        .any(|finding| finding.severity == FindingSeverity::Error);

    if args.json_plan || args.dry_run || !confirmed {
        output_json(&result, "init plan");
        if !confirmed && !args.json_plan && !args.dry_run {
            eprintln!("init: dry-run only; pass --yes to apply the generated plan");
        }
        return Ok(0);
    }
    if has_errors && !args.force {
        output_json(&result, "init plan");
        eprintln!("init: plan has error findings; fix them or pass --force to apply anyway");
        return Ok(1);
    }

    let receipt =
        apply_init_plan(PathBuf::from(&cwd), &result, false).map_err(InitRunError::Apply)?;
    output_json(&receipt, "init apply receipt");
    Ok(0)
}

fn run_tui_preview(result: &udb::init::InitPlanResult, force: bool) -> tui::TuiResult<bool> {
    tui::run_init_tui(InitPreviewDriver {
        snapshot: tui_snapshot(result),
        has_errors: result
            .findings
            .iter()
            .any(|finding| finding.severity == FindingSeverity::Error),
        force,
        apply: false,
        done: false,
    })
}

struct InitPreviewDriver {
    snapshot: tui::InitSnapshot,
    has_errors: bool,
    force: bool,
    apply: bool,
    done: bool,
}

impl tui::InitTuiDriver for InitPreviewDriver {
    type Output = bool;

    fn snapshot(&self) -> tui::InitSnapshot {
        self.snapshot.clone()
    }

    fn handle_event(&mut self, event: tui::InitEvent) -> tui::TuiResult<()> {
        match event {
            tui::InitEvent::Apply | tui::InitEvent::Input(tui::InitInput::Char('a')) => {
                if self.has_errors && !self.force {
                    self.snapshot.status_line =
                        "Plan has errors; quit and fix them or rerun with --force".to_string();
                } else {
                    self.apply = true;
                    self.done = true;
                }
            }
            tui::InitEvent::Cancel
            | tui::InitEvent::Input(tui::InitInput::Esc | tui::InitInput::CtrlC)
            | tui::InitEvent::Input(tui::InitInput::Char('q')) => {
                self.apply = false;
                self.done = true;
            }
            tui::InitEvent::Next => {
                self.snapshot.status_line = "Press a to apply or q to quit".to_string();
            }
            _ => {}
        }
        Ok(())
    }

    fn should_quit(&self) -> bool {
        self.done
    }

    fn finish(self) -> tui::TuiResult<Self::Output> {
        Ok(self.apply)
    }
}

fn tui_snapshot(result: &udb::init::InitPlanResult) -> tui::InitSnapshot {
    let has_errors = result
        .findings
        .iter()
        .any(|finding| finding.severity == FindingSeverity::Error);
    tui::InitSnapshot {
        title: "UDB init".to_string(),
        cwd: Some(result.plan.workspace.root.clone()),
        mode: "plan preview".to_string(),
        warnings: result
            .findings
            .iter()
            .map(|finding| format!("{:?}: {}", finding.severity, finding.message))
            .collect(),
        steps: vec![
            tui::InitStep {
                id: "scan".to_string(),
                label: "Workspace scan".to_string(),
                status: tui::InitStepStatus::Complete,
                validation_count: 0,
            },
            tui::InitStep {
                id: "plan".to_string(),
                label: "Plan preview".to_string(),
                status: if has_errors {
                    tui::InitStepStatus::Error
                } else {
                    tui::InitStepStatus::Active
                },
                validation_count: result.findings.len(),
            },
            tui::InitStep {
                id: "sdk".to_string(),
                label: "SDK/native bootstrap preflight".to_string(),
                status: if result
                    .plan
                    .verification_steps
                    .iter()
                    .any(|step| step.contains("udb sdk init"))
                {
                    tui::InitStepStatus::Warning
                } else {
                    tui::InitStepStatus::Complete
                },
                validation_count: result
                    .plan
                    .verification_steps
                    .iter()
                    .filter(|step| step.contains("udb sdk init"))
                    .count(),
            },
            tui::InitStep {
                id: "apply".to_string(),
                label: "Apply".to_string(),
                status: tui::InitStepStatus::Pending,
                validation_count: 0,
            },
        ],
        facts: vec![
            format!("root: {:?}", result.plan.workspace.root_kind),
            format!("framework: {:?}", result.plan.selections.framework),
            format!("proto: {:?}", result.plan.proto_strategy),
            format!("dbops: {:?}", result.plan.dbops_policy),
        ],
        selections: vec![
            format!("backends: {}", result.plan.selections.backends.join(", ")),
            format!(
                "native services: {}",
                result.plan.selections.native_services.join(", ")
            ),
            format!("features: {}", result.plan.selections.features.join(", ")),
        ],
        plan_summary: result
            .plan
            .mutations
            .iter()
            .map(|mutation| format!("{}: {}", mutation.path.display(), mutation.summary))
            .chain(
                result
                    .plan
                    .generator_steps
                    .iter()
                    .map(|step| format!("generator: {step}")),
            )
            .chain(
                result
                    .plan
                    .verification_steps
                    .iter()
                    .map(|step| format!("verify: {step}")),
            )
            .collect(),
        apply_log: Vec::new(),
        action_bar: vec![tui::InitAction::Apply, tui::InitAction::Cancel],
        status_line: if has_errors {
            "Plan has errors; apply is blocked unless --force is supplied".to_string()
        } else {
            "Press a to apply or q to quit".to_string()
        },
    }
}

fn default_prompt_catalog() -> InitPromptCatalog {
    let frameworks = framework_catalog()
        .into_iter()
        .map(|framework| framework.id)
        .collect::<Vec<_>>();
    let backends = backend_catalog()
        .into_iter()
        .map(|backend| backend.id)
        .collect::<Vec<_>>();
    let native_services = native_service_catalog()
        .into_iter()
        .map(|service| service.id)
        .collect::<Vec<_>>();
    InitPromptCatalog {
        frameworks,
        backends,
        native_services,
        features: vec![
            "auth".to_string(),
            "vector".to_string(),
            "storage".to_string(),
            "dbops".to_string(),
        ],
        default_backends: default_backend_ids_for_features(&[]),
        default_native_services: native_service_catalog()
            .into_iter()
            .filter(|service| service.default_enabled)
            .map(|service| service.id)
            .collect(),
        ..InitPromptCatalog::default()
    }
}

fn read_config(path: &str) -> Result<InitOptions, InitRunError> {
    let raw = fs::read_to_string(path).map_err(|source| InitRunError::ConfigRead {
        path: path.to_string(),
        source,
    })?;
    serde_yaml::from_str(&raw).map_err(|source| InitRunError::ConfigParse {
        path: path.to_string(),
        source,
    })
}

fn parse_profile(value: Option<&str>) -> Option<InitProfile> {
    match value?.trim().to_ascii_lowercase().as_str() {
        "local" => Some(InitProfile::Local),
        "dev" => Some(InitProfile::Dev),
        "ci" => Some(InitProfile::Ci),
        "prod" | "production" => Some(InitProfile::Prod),
        _ => None,
    }
}

fn parse_proto_strategy(value: Option<&str>) -> Option<ProtoStrategy> {
    match value?.trim().to_ascii_lowercase().as_str() {
        "starter" | "create" | "create-starter" => Some(ProtoStrategy::CreateStarter),
        "existing" | "use-existing" => Some(ProtoStrategy::UseExisting),
        "skip" | "none" => Some(ProtoStrategy::Skip),
        _ => None,
    }
}
