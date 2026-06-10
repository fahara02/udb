use crate::init::catalog::{
    BackendRole, backend_catalog, default_backend_ids_for_features,
    expand_native_service_selection, framework_catalog, native_service_catalog,
    normalize_backend_id, normalize_framework_id,
};
use crate::init::mutations::{MutationAction, MutationKind, PlanMutation, WarningLevel};
use crate::init::workspace::{DetectedLanguage, RootKind, WorkspaceScan, scan_workspace};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InitProfile {
    Local,
    Dev,
    Ci,
    Prod,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtoStrategy {
    CreateStarter,
    UseExisting,
    Skip,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DbOpsPolicy {
    CreateDefaults,
    UseExisting,
    Skip,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationFinding {
    pub code: String,
    pub severity: FindingSeverity,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct InitOptions {
    pub profile: InitProfile,
    pub framework: Option<String>,
    pub backends: Vec<String>,
    pub native_services: Vec<String>,
    pub features: Vec<String>,
    pub proto_strategy: Option<ProtoStrategy>,
    pub dbops_policy: Option<DbOpsPolicy>,
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitSelections {
    pub framework: Option<String>,
    pub backends: Vec<String>,
    pub native_services: Vec<String>,
    pub features: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitPlan {
    pub profile: InitProfile,
    pub workspace: WorkspaceScan,
    pub selections: InitSelections,
    pub proto_strategy: ProtoStrategy,
    pub dbops_policy: DbOpsPolicy,
    pub mutations: Vec<PlanMutation>,
    pub generator_steps: Vec<String>,
    pub verification_steps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitPlanResult {
    pub plan: InitPlan,
    pub findings: Vec<ValidationFinding>,
    pub dry_run: bool,
}

#[derive(Debug)]
pub enum InitPlanError {
    WorkspaceScan {
        path: PathBuf,
        source: std::io::Error,
    },
    Serialize(serde_json::Error),
}

impl Default for InitOptions {
    fn default() -> Self {
        Self {
            profile: InitProfile::Local,
            framework: None,
            backends: Vec::new(),
            native_services: Vec::new(),
            features: Vec::new(),
            proto_strategy: None,
            dbops_policy: None,
            dry_run: true,
        }
    }
}

impl InitPlanResult {
    pub fn to_json_pretty(&self) -> Result<String, InitPlanError> {
        serde_json::to_string_pretty(self).map_err(InitPlanError::Serialize)
    }
}

pub fn build_init_plan(
    cwd: impl AsRef<Path>,
    options: InitOptions,
) -> Result<InitPlanResult, InitPlanError> {
    let cwd = cwd.as_ref();
    let workspace = scan_workspace(cwd).map_err(|source| InitPlanError::WorkspaceScan {
        path: cwd.to_path_buf(),
        source,
    })?;
    let (selections, mut findings) = resolve_selections(&workspace, &options);
    let proto_strategy = options
        .proto_strategy
        .clone()
        .unwrap_or_else(|| default_proto_strategy(&workspace));
    let dbops_policy = options
        .dbops_policy
        .clone()
        .unwrap_or_else(|| default_dbops_policy(&workspace));

    findings.extend(validate(
        &workspace,
        &selections,
        &proto_strategy,
        &dbops_policy,
        &options.profile,
    ));
    let mutations = build_mutations(
        cwd,
        &workspace,
        &selections,
        &proto_strategy,
        &dbops_policy,
        &mut findings,
    );
    let generator_steps = build_generator_steps(&selections, &proto_strategy);
    let verification_steps = build_verification_steps(&workspace, &selections, &dbops_policy);
    sort_findings(&mut findings);

    Ok(InitPlanResult {
        plan: InitPlan {
            profile: options.profile,
            workspace,
            selections,
            proto_strategy,
            dbops_policy,
            mutations,
            generator_steps,
            verification_steps,
        },
        findings,
        dry_run: options.dry_run,
    })
}

fn resolve_selections(
    workspace: &WorkspaceScan,
    options: &InitOptions,
) -> (InitSelections, Vec<ValidationFinding>) {
    let mut findings = Vec::new();
    let framework = options
        .framework
        .clone()
        .map(|framework| normalize_framework_id(&framework))
        .or_else(|| workspace.primary_framework_id().map(ToString::to_string));

    let mut features = options.features.clone();
    normalize_unique_with(&mut features, |feature| {
        feature.trim().to_ascii_lowercase().replace(' ', "-")
    });

    let mut backends = if options.backends.is_empty() {
        default_backend_ids_for_features(&features)
    } else {
        options
            .backends
            .iter()
            .map(|backend| normalize_backend_id(backend))
            .collect()
    };
    normalize_unique_with(&mut backends, |backend| normalize_backend_id(backend));

    let mut native_services = if options.native_services.is_empty() {
        native_service_catalog()
            .into_iter()
            .filter(|service| service.default_enabled)
            .map(|service| service.id)
            .collect()
    } else {
        options
            .native_services
            .iter()
            .flat_map(|service| expand_native_service_selection(service))
            .collect()
    };
    normalize_unique_with(&mut native_services, |service| {
        expand_native_service_selection(service)
            .into_iter()
            .next()
            .unwrap_or_default()
    });
    infer_required_backends(&mut backends, &native_services, &features, &mut findings);

    (
        InitSelections {
            framework,
            backends,
            native_services,
            features,
        },
        findings,
    )
}

fn validate(
    workspace: &WorkspaceScan,
    selections: &InitSelections,
    proto_strategy: &ProtoStrategy,
    dbops_policy: &DbOpsPolicy,
    profile: &InitProfile,
) -> Vec<ValidationFinding> {
    let mut findings = Vec::new();
    let backend_ids: BTreeSet<_> = backend_catalog()
        .into_iter()
        .map(|entry| entry.id)
        .collect();
    let native_ids: BTreeSet<_> = native_service_catalog()
        .into_iter()
        .map(|entry| entry.id)
        .collect();
    let framework_ids: BTreeSet<_> = framework_catalog()
        .into_iter()
        .map(|entry| entry.id)
        .collect();

    if let Some(framework) = &selections.framework {
        if !framework_ids.contains(framework) {
            findings.push(error(
                "unknown_framework",
                format!("Unknown framework selection `{framework}`."),
            ));
        } else {
            validate_framework_fit(workspace, framework, &mut findings);
        }
    } else if workspace.root_kind != RootKind::UdbRepo {
        findings.push(info(
            "framework_not_selected",
            "No framework was detected or selected; plan will stay framework-neutral.",
        ));
    }

    for backend in &selections.backends {
        if !backend_ids.contains(backend) {
            findings.push(error(
                "unknown_backend",
                format!("Unknown backend selection `{backend}`."),
            ));
        }
    }
    for service in &selections.native_services {
        if !native_ids.contains(service) {
            findings.push(error(
                "unknown_native_service",
                format!("Unknown native service selection `{service}`."),
            ));
        }
    }

    if !selections
        .backends
        .iter()
        .any(|backend| is_canonical_backend(backend))
    {
        findings.push(warning(
            "canonical_backend_not_selected",
            "No canonical SQL backend is selected; DB ops and native services usually need one.",
        ));
    }
    if selections
        .backends
        .iter()
        .any(|backend| backend == "cassandra")
    {
        findings.push(warning(
            "cassandra_partial_generation",
            "Cassandra may have partial generation or migration support for selected workflows.",
        ));
    }
    if selections
        .backends
        .iter()
        .any(|backend| matches!(backend.as_str(), "s3" | "azureblob" | "gcs" | "pinecone"))
    {
        findings.push(warning(
            "secret_backends_selected",
            "Cloud backends require secret prompts; init must not write secrets into tracked files.",
        ));
    }
    if *proto_strategy == ProtoStrategy::CreateStarter && !workspace.proto_roots.is_empty() {
        findings.push(warning(
            "proto_exists",
            "Existing proto workspace detected; creating starter proto may need review.",
        ));
    }
    if *dbops_policy == DbOpsPolicy::CreateDefaults && !workspace.dbops_dirs.is_empty() {
        findings.push(info(
            "dbops_exists",
            "Existing DB ops directories detected; defaults should be merged idempotently.",
        ));
    }
    validate_backend_roles(selections, &mut findings);
    validate_existing_configs(workspace, &mut findings);
    validate_profile(profile, selections, &mut findings);

    findings
}

fn build_mutations(
    root: &Path,
    workspace: &WorkspaceScan,
    selections: &InitSelections,
    proto_strategy: &ProtoStrategy,
    dbops_policy: &DbOpsPolicy,
    findings: &mut Vec<ValidationFinding>,
) -> Vec<PlanMutation> {
    let mut mutations = vec![
        PlanMutation::new(
            root,
            ".udb",
            MutationKind::CreateDir,
            MutationAction::Create,
            "udb-state-dir",
            WarningLevel::None,
            "create init state directory",
        ),
        PlanMutation::new(
            root,
            ".udb/init-state.json",
            MutationKind::CreateFile,
            MutationAction::Create,
            "init-state",
            WarningLevel::Info,
            "persist resumable init state",
        ),
        PlanMutation::new(
            root,
            ".udb/init-plan.json",
            MutationKind::CreateFile,
            MutationAction::Create,
            "init-plan",
            WarningLevel::Info,
            "persist dry-run plan preview",
        ),
        PlanMutation::new(
            root,
            ".env.example",
            MutationKind::ManagedBlock,
            MutationAction::Merge,
            "env-example",
            WarningLevel::Info,
            "merge non-secret UDB example variables",
        ),
        PlanMutation::new(
            root,
            "configs/udb.init.backends.yaml",
            MutationKind::OverlayFile,
            MutationAction::Merge,
            "backend-instances",
            WarningLevel::Info,
            "write selected backend instance overlay",
        ),
        PlanMutation::new(
            root,
            "configs/udb.init.services.yaml",
            MutationKind::OverlayFile,
            MutationAction::Merge,
            "native-services",
            WarningLevel::Info,
            "write selected native service overlay",
        ),
    ];

    if *proto_strategy != ProtoStrategy::Skip {
        mutations.push(PlanMutation::new(
            root,
            "buf.gen.yaml",
            MutationKind::OverlayFile,
            MutationAction::Merge,
            "buf-gen",
            WarningLevel::Info,
            "verify or add SDK generation outputs",
        ));
    }
    if *proto_strategy == ProtoStrategy::CreateStarter {
        mutations.push(PlanMutation::new(
            root,
            "proto",
            MutationKind::CreateDir,
            MutationAction::Create,
            "proto-root",
            WarningLevel::None,
            "create starter proto directory",
        ));
    }

    if *dbops_policy != DbOpsPolicy::Skip {
        mutations.push(PlanMutation::new(
            root,
            "configs/udb.init.database.yaml",
            MutationKind::OverlayFile,
            MutationAction::Merge,
            "database-config",
            WarningLevel::Info,
            "write DB ops defaults overlay",
        ));
        for dir in ["db/migration", "db/seeders", "db/bootstrap"] {
            mutations.push(PlanMutation::new(
                root,
                dir,
                MutationKind::CreateDir,
                MutationAction::Create,
                format!("dbops-{}", dir.replace('/', "-")),
                WarningLevel::None,
                "ensure DB ops directory exists",
            ));
        }
    }

    if needs_local_compose(selections) {
        mutations.push(PlanMutation::new(
            root,
            "docker-compose.udb.yml",
            MutationKind::OverlayFile,
            MutationAction::Create,
            "compose-overlay",
            WarningLevel::Info,
            "create local Docker Compose overlay for selected services",
        ));
    }

    if workspace.root_kind == RootKind::UdbRepo {
        findings.push(info(
            "udb_repo_detected",
            "UDB repository detected; root Cargo.toml changes are intentionally omitted from this pure plan.",
        ));
    }

    mutations
}

fn build_generator_steps(
    selections: &InitSelections,
    proto_strategy: &ProtoStrategy,
) -> Vec<String> {
    let mut steps = Vec::new();
    if *proto_strategy != ProtoStrategy::Skip {
        steps.push("run proto export/generation for selected framework".to_string());
    }
    if !selections.native_services.is_empty() {
        steps.push("render native service descriptor selections".to_string());
    }
    steps
}

fn build_verification_steps(
    workspace: &WorkspaceScan,
    selections: &InitSelections,
    dbops_policy: &DbOpsPolicy,
) -> Vec<String> {
    let mut steps = vec![
        "verify init plan idempotency keys".to_string(),
        "validate existing YAML config syntax before overlay apply".to_string(),
        "write reversible init receipt for --revert".to_string(),
    ];
    if *dbops_policy != DbOpsPolicy::Skip {
        steps.push("verify DB ops directories and migration config".to_string());
    }
    if !selections.backends.is_empty() {
        steps.push("verify selected backend instance config".to_string());
    }
    for language in sdk_bootstrap_languages(workspace, selections) {
        steps.push(format!(
            "run `udb sdk init --lang {language}` and install any missing SDK prerequisites"
        ));
    }
    if !selections.native_services.is_empty() {
        steps.push(
            "run `udb sdk init --lang all` and install missing native feature tools (buf, cmake, Perl/OpenSSL, FFmpeg, ghz)"
                .to_string(),
        );
    }
    steps
}

fn sdk_bootstrap_languages(
    workspace: &WorkspaceScan,
    selections: &InitSelections,
) -> Vec<&'static str> {
    let mut languages = BTreeSet::new();
    if let Some(framework) = selections.framework.as_deref() {
        if let Some(entry) = framework_catalog()
            .into_iter()
            .find(|entry| entry.id == framework)
        {
            languages.insert(canonical_sdk_language(&entry.language));
        }
    }
    for language in &workspace.languages {
        languages.insert(language_id(language));
    }
    languages
        .into_iter()
        .filter(|language| {
            matches!(
                *language,
                "typescript" | "python" | "go" | "java" | "csharp" | "php"
            )
        })
        .collect()
}

fn default_proto_strategy(workspace: &WorkspaceScan) -> ProtoStrategy {
    if workspace.proto_roots.is_empty() {
        ProtoStrategy::CreateStarter
    } else {
        ProtoStrategy::UseExisting
    }
}

fn default_dbops_policy(workspace: &WorkspaceScan) -> DbOpsPolicy {
    if workspace.dbops_dirs.is_empty() {
        DbOpsPolicy::CreateDefaults
    } else {
        DbOpsPolicy::UseExisting
    }
}

fn needs_local_compose(selections: &InitSelections) -> bool {
    selections.backends.iter().any(|backend| {
        matches!(
            backend.as_str(),
            "postgres" | "redis" | "qdrant" | "minio" | "mysql" | "mongodb"
        )
    })
}

fn normalize_unique_with<F>(items: &mut Vec<String>, normalize: F)
where
    F: Fn(&str) -> String,
{
    for item in items.iter_mut() {
        *item = normalize(item);
    }
    items.retain(|item| !item.is_empty());
    items.sort();
    items.dedup();
}

fn infer_required_backends(
    backends: &mut Vec<String>,
    native_services: &[String],
    features: &[String],
    findings: &mut Vec<ValidationFinding>,
) {
    if !native_services.is_empty() && !backends.iter().any(|backend| is_canonical_backend(backend))
    {
        push_inferred_backend(
            backends,
            "postgres",
            "native_services_need_canonical_store",
            findings,
        );
    }
    if needs_object_backend(native_services, features)
        && !backends
            .iter()
            .any(|backend| backend_role(backend) == Some(BackendRole::Object))
    {
        push_inferred_backend(backends, "minio", "object_backend_inferred", findings);
    }
    if needs_vector_backend(features)
        && !backends
            .iter()
            .any(|backend| backend_role(backend) == Some(BackendRole::Vector))
    {
        push_inferred_backend(backends, "qdrant", "vector_backend_inferred", findings);
    }
    normalize_unique_with(backends, |backend| normalize_backend_id(backend));
}

fn push_inferred_backend(
    backends: &mut Vec<String>,
    backend: &str,
    code: &str,
    findings: &mut Vec<ValidationFinding>,
) {
    if !backends.iter().any(|item| item == backend) {
        backends.push(backend.to_string());
        findings.push(info(
            code,
            format!("Added `{backend}` because the selected services/features require it."),
        ));
    }
}

fn validate_framework_fit(
    workspace: &WorkspaceScan,
    framework: &str,
    findings: &mut Vec<ValidationFinding>,
) {
    let Some(expected_language) = framework_catalog()
        .into_iter()
        .find(|entry| entry.id == framework)
        .map(|entry| entry.language)
    else {
        return;
    };
    if workspace.languages.is_empty() || workspace.root_kind == RootKind::UdbRepo {
        return;
    }
    if !workspace
        .languages
        .iter()
        .any(|language| language_id(language) == expected_language)
    {
        findings.push(warning(
            "framework_language_mismatch",
            format!(
                "Selected framework `{framework}` does not match detected languages {:?}.",
                workspace.languages
            ),
        ));
    }
}

fn validate_backend_roles(selections: &InitSelections, findings: &mut Vec<ValidationFinding>) {
    let canonical_count = selections
        .backends
        .iter()
        .filter(|backend| is_canonical_backend(backend))
        .count();
    if canonical_count > 1 {
        findings.push(warning(
            "multiple_canonical_backends",
            "Multiple canonical SQL backends are selected; confirm DB ops should target all of them.",
        ));
    }
    if needs_object_backend(&selections.native_services, &selections.features)
        && !selections
            .backends
            .iter()
            .any(|backend| backend_role(backend) == Some(BackendRole::Object))
    {
        findings.push(error(
            "object_backend_required",
            "Storage or asset services require an object backend such as minio, s3, azureblob, or gcs.",
        ));
    }
}

fn validate_existing_configs(workspace: &WorkspaceScan, findings: &mut Vec<ValidationFinding>) {
    for relative in &workspace.config_files {
        if relative.extension().and_then(|ext| ext.to_str()) != Some("yaml") {
            continue;
        }
        let path = workspace.root.join(relative);
        match std::fs::read_to_string(&path) {
            Ok(raw) => {
                if let Err(source) = serde_yaml::from_str::<serde_yaml::Value>(&raw) {
                    findings.push(error(
                        "config_yaml_invalid",
                        format!(
                            "Existing config `{}` is not valid YAML: {source}.",
                            relative.display()
                        ),
                    ));
                }
            }
            Err(source) => findings.push(warning(
                "config_unreadable",
                format!(
                    "Existing config `{}` could not be read: {source}.",
                    relative.display()
                ),
            )),
        }
    }
}

fn validate_profile(
    profile: &InitProfile,
    selections: &InitSelections,
    findings: &mut Vec<ValidationFinding>,
) {
    if *profile == InitProfile::Prod {
        if selections
            .backends
            .iter()
            .any(|backend| backend == "sqlite")
        {
            findings.push(error(
                "sqlite_prod",
                "SQLite is not accepted for the prod profile.",
            ));
        }
        if selections
            .backends
            .iter()
            .any(|backend| matches!(backend.as_str(), "minio" | "qdrant"))
        {
            findings.push(warning(
                "local_backend_in_prod",
                "Prod profile includes local-style backends; verify managed services and secrets before deploy.",
            ));
        }
    }
}

fn needs_object_backend(native_services: &[String], features: &[String]) -> bool {
    native_services
        .iter()
        .any(|service| matches!(service.as_str(), "storage" | "asset"))
        || features
            .iter()
            .any(|feature| matches!(feature.as_str(), "storage" | "asset" | "assets" | "object"))
}

fn needs_vector_backend(features: &[String]) -> bool {
    features
        .iter()
        .any(|feature| matches!(feature.as_str(), "vector" | "search" | "embeddings"))
}

fn is_canonical_backend(backend: &str) -> bool {
    backend_role(backend) == Some(BackendRole::Canonical)
}

fn backend_role(backend: &str) -> Option<BackendRole> {
    backend_catalog()
        .into_iter()
        .find(|entry| entry.id == backend)
        .map(|entry| entry.role)
}

fn language_id(language: &DetectedLanguage) -> &'static str {
    match language {
        DetectedLanguage::Php => "php",
        DetectedLanguage::Python => "python",
        DetectedLanguage::TypeScript | DetectedLanguage::JavaScript => "typescript",
        DetectedLanguage::Go => "go",
        DetectedLanguage::Java => "java",
        DetectedLanguage::CSharp => "csharp",
        DetectedLanguage::Rust => "rust",
    }
}

fn canonical_sdk_language(language: &str) -> &'static str {
    match language.trim().to_ascii_lowercase().as_str() {
        "ts" | "typescript" | "javascript" | "node" => "typescript",
        "py" | "python" => "python",
        "go" | "golang" => "go",
        "java" | "jvm" => "java",
        "cs" | "c#" | "csharp" | "dotnet" => "csharp",
        "php" | "laravel" | "symfony" => "php",
        _ => "unknown",
    }
}

fn sort_findings(findings: &mut [ValidationFinding]) {
    findings.sort_by_key(|finding| match finding.severity {
        FindingSeverity::Error => 0,
        FindingSeverity::Warning => 1,
        FindingSeverity::Info => 2,
    });
}

fn info(code: &str, message: impl Into<String>) -> ValidationFinding {
    finding(code, FindingSeverity::Info, message)
}

fn warning(code: &str, message: impl Into<String>) -> ValidationFinding {
    finding(code, FindingSeverity::Warning, message)
}

fn error(code: &str, message: impl Into<String>) -> ValidationFinding {
    finding(code, FindingSeverity::Error, message)
}

fn finding(code: &str, severity: FindingSeverity, message: impl Into<String>) -> ValidationFinding {
    ValidationFinding {
        code: code.to_string(),
        severity,
        message: message.into(),
    }
}

impl fmt::Display for InitPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkspaceScan { path, source } => {
                write!(f, "failed to scan workspace {}: {source}", path.display())
            }
            Self::Serialize(source) => write!(f, "failed to serialize init plan: {source}"),
        }
    }
}

impl std::error::Error for InitPlanError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn builds_json_serializable_plan_for_empty_workspace() {
        let root = temp_root("empty");
        let result = build_init_plan(&root, InitOptions::default()).unwrap();

        assert_eq!(result.plan.proto_strategy, ProtoStrategy::CreateStarter);
        assert_eq!(result.plan.dbops_policy, DbOpsPolicy::CreateDefaults);
        assert!(
            result
                .plan
                .selections
                .backends
                .contains(&"postgres".to_string())
        );
        assert!(result.to_json_pretty().unwrap().contains("\"mutations\""));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn detects_existing_proto_and_dbops() {
        let root = temp_root("existing");
        fs::create_dir_all(root.join("proto")).unwrap();
        fs::create_dir_all(root.join("db/migration")).unwrap();
        File::create(root.join("buf.yaml")).unwrap();

        let result = build_init_plan(&root, InitOptions::default()).unwrap();

        assert_eq!(result.plan.proto_strategy, ProtoStrategy::UseExisting);
        assert_eq!(result.plan.dbops_policy, DbOpsPolicy::UseExisting);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unknown_backend_produces_error_finding() {
        let root = temp_root("bad-backend");
        let result = build_init_plan(
            &root,
            InitOptions {
                backends: vec!["not-a-backend".to_string()],
                ..InitOptions::default()
            },
        )
        .unwrap();

        assert!(result.findings.iter().any(|finding| {
            finding.code == "unknown_backend" && finding.severity == FindingSeverity::Error
        }));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn aliases_expand_and_infer_required_backends() {
        let root = temp_root("aliases");
        let result = build_init_plan(
            &root,
            InitOptions {
                framework: Some("react".to_string()),
                backends: vec!["pg".to_string()],
                native_services: vec!["auth".to_string(), "assets".to_string()],
                features: vec!["embeddings".to_string()],
                ..InitOptions::default()
            },
        )
        .unwrap();

        assert_eq!(
            result.plan.selections.framework.as_deref(),
            Some("react-vite")
        );
        assert!(
            result
                .plan
                .selections
                .backends
                .contains(&"postgres".to_string())
        );
        assert!(
            result
                .plan
                .selections
                .backends
                .contains(&"minio".to_string())
        );
        assert!(
            result
                .plan
                .selections
                .backends
                .contains(&"qdrant".to_string())
        );
        assert!(
            result
                .plan
                .selections
                .native_services
                .contains(&"authn".to_string())
        );
        assert!(
            result
                .plan
                .selections
                .native_services
                .contains(&"authz".to_string())
        );
        assert!(
            result
                .plan
                .selections
                .native_services
                .contains(&"asset".to_string())
        );
        assert!(
            result
                .plan
                .selections
                .native_services
                .contains(&"storage".to_string())
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn init_plan_surfaces_sdk_bootstrap_for_selected_framework_language() {
        let root = temp_root("sdk-bootstrap-framework");
        let result = build_init_plan(
            &root,
            InitOptions {
                framework: Some("laravel".to_string()),
                ..InitOptions::default()
            },
        )
        .unwrap();

        assert!(result.plan.verification_steps.iter().any(|step| {
            step == "run `udb sdk init --lang php` and install any missing SDK prerequisites"
        }));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn init_plan_surfaces_sdk_bootstrap_for_detected_languages() {
        let root = temp_root("sdk-bootstrap-detected");
        File::create(root.join("package.json")).unwrap();
        File::create(root.join("go.mod")).unwrap();

        let result = build_init_plan(&root, InitOptions::default()).unwrap();

        assert!(result.plan.verification_steps.iter().any(|step| {
            step == "run `udb sdk init --lang typescript` and install any missing SDK prerequisites"
        }));
        assert!(result.plan.verification_steps.iter().any(|step| {
            step == "run `udb sdk init --lang go` and install any missing SDK prerequisites"
        }));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn init_plan_surfaces_native_toolchain_preflight_for_native_services() {
        let root = temp_root("native-toolchain-preflight");
        let result = build_init_plan(
            &root,
            InitOptions {
                native_services: vec!["authn".to_string()],
                ..InitOptions::default()
            },
        )
        .unwrap();

        assert!(result.plan.verification_steps.iter().any(|step| {
            step == "run `udb sdk init --lang all` and install missing native feature tools (buf, cmake, Perl/OpenSSL, FFmpeg, ghz)"
        }));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn invalid_existing_yaml_is_error_finding() {
        let root = temp_root("invalid-yaml");
        fs::create_dir_all(root.join("configs")).unwrap();
        fs::write(root.join("configs/database.yaml"), "app_name: [broken\n").unwrap();

        let result = build_init_plan(&root, InitOptions::default()).unwrap();

        assert!(result.findings.iter().any(|finding| {
            finding.code == "config_yaml_invalid" && finding.severity == FindingSeverity::Error
        }));

        fs::remove_dir_all(root).unwrap();
    }

    fn temp_root(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("udb-init-plan-{name}-{suffix}"));
        fs::create_dir_all(&root).unwrap();
        root
    }
}
