use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::ast::ProtoSchema;
use crate::generation::{
    CatalogManifest, DsnGenerationConfig, GeneratedArtifact, SqlGenerationConfig, UnifiedDsn,
    generate_bootstrap_sql, generate_delta_sql, generate_unified_dsn_catalog,
};
use crate::observability::{MetricLabels, TraceContext};
use crate::provisioning::try_build_provisioning_plan;

use super::diff::{ChangeOperation, ChangeSafety, diff_manifests};
use super::diff_backends::diff_all_backends;

/// FSM states tracked inside a `MigrationPlan`.
///
/// This enum mirrors `FsmState` in `crate::engine` but is intentionally kept
/// separate so that serialised `MigrationPlan` JSON is self-contained and does
/// not require a live `Engine` instance to interpret.
///
/// The full state graph (including IDLE, INITIALISING, RECOVERING, etc.) lives
/// in `crate::engine::FsmState`; this enum covers only the states that appear
/// in the execution path of a planned run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationFsmState {
    /// Quiescent — included in the plan to represent the starting point.
    Idle,
    /// Bootstrap tracker tables, acquire advisory lock.
    Initialising,
    /// Parse `.proto` files and build the desired AST.
    LoadProtoState,
    /// Compare parsed AST checksum against the ledger.
    ProtoChecksumLint,
    /// Diff old manifest vs new to derive required changes.
    PlanProtoDiff,
    /// Generate SQL delta files from the diff result.
    GenerateSql,
    /// Verify checksums of all SQL files in the migration directory.
    ChecksumLint,
    /// Apply SQL migration files against the live database.
    Applying,
    /// Run the proto linter against the live `information_schema`.
    Linting,
    /// Apply safe DDL repairs identified by the linter.
    AutoAltering,
    /// Cross-check the live DB topology against the desired proto AST.
    Verifying,
    /// Migration run completed and ledger entries recorded.
    Completed,
    /// Unrecoverable error — FSM has halted.
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MigrationPlanConfig {
    pub sql: SqlGenerationConfig,
    pub dsn: DsnGenerationConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MigrationPlan {
    pub generated_at_unix: u64,
    pub trace: TraceContext,
    pub states: Vec<MigrationFsmState>,
    pub manifest: CatalogManifest,
    pub changes: Vec<ChangeOperation>,
    pub auto_count: usize,
    pub blocked_count: usize,
    pub operations_hash: String,
    pub sql_artifacts: Vec<GeneratedArtifact>,
    pub resource_actions: Vec<ResourceAction>,
    pub dsn_entries: Vec<UnifiedDsn>,
    pub ledgers: Vec<LedgerEntry>,
    pub blocked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ResourceAction {
    pub action: String,
    pub rollback_action: String,
    pub trace: TraceContext,
    pub metric_labels: MetricLabels,
    pub tier: String,
    pub store_kind: String,
    pub backend: String,
    pub resource_kind: String,
    pub resource_name: String,
    pub resource_uri: String,
    pub owner_schema: String,
    pub owner_table: String,
    pub dsn: String,
    pub parameters: Vec<crate::provisioning::ProvisioningParameter>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LedgerEntry {
    pub ledger: String,
    pub checksum_sha256: String,
    pub description: String,
}

/// The ONE canonical migration change set for a `previous → manifest` transition.
///
/// This is the SINGLE source of truth shared by the plan PRODUCER (`udb plan` via
/// [`build_migration_plan`]) and the plan CONSUMER (the `serve` startup approval
/// gate, `control::lifecycle`). Both must derive the identical operation set from
/// the same prior/current manifests, or the approval gate rejects its own tool's
/// output with a `CountMismatch` (the producer/consumer divergence). It is:
///
/// - `diff_manifests` — Postgres-style relational DDL; PLUS
/// - `diff_all_backends` — the per-backend deltas (Qdrant, MongoDB, Neo4j,
///   ClickHouse, S3/MinIO collection/index/lifecycle changes) that relational
///   diffing does not cover.
///
/// Disjoint `ChangeKind` sets, so a plain concatenation is correct; every op
/// flows through the same finalize/generate/apply pipeline.
pub fn canonical_change_set(
    previous: Option<&CatalogManifest>,
    manifest: &CatalogManifest,
) -> Vec<ChangeOperation> {
    let mut changes = diff_manifests(previous, manifest);
    changes.extend(diff_all_backends(previous, manifest));
    changes
}

pub fn build_migration_plan(
    previous: Option<&CatalogManifest>,
    schemas: &[ProtoSchema],
    config: &MigrationPlanConfig,
) -> Result<MigrationPlan, serde_json::Error> {
    let manifest = CatalogManifest::from_schemas(schemas)?;
    // Producer side of the shared canonical change set (see `canonical_change_set`);
    // `serve` computes the SAME set so an approved plan this emits is one `serve`
    // will accept.
    let changes = canonical_change_set(previous, &manifest);
    let blocked = changes.iter().any(|change| {
        matches!(
            change.safety,
            ChangeSafety::Blocked | ChangeSafety::RequiresReview
        )
    });
    let auto_count = changes
        .iter()
        .filter(|change| change.safety == ChangeSafety::SafeAuto)
        .count();
    let blocked_count = changes.len() - auto_count;
    let operations_hash = operations_hash(&changes);
    let sql_artifacts = if previous.is_some() {
        generate_delta_sql(&manifest, &changes, &config.sql)
    } else {
        generate_bootstrap_sql(schemas, &config.sql)?
    };
    let dsn_catalog = generate_unified_dsn_catalog(schemas, &config.dsn)?;
    let resource_actions = build_resource_actions(&manifest, &dsn_catalog.entries)?;

    // Full FSM path — matches the Go service execution lifecycle.
    let mut states = vec![
        MigrationFsmState::Idle,
        MigrationFsmState::Initialising,
        MigrationFsmState::LoadProtoState,
        MigrationFsmState::ProtoChecksumLint,
        MigrationFsmState::PlanProtoDiff,
        MigrationFsmState::GenerateSql,
        MigrationFsmState::ChecksumLint,
    ];
    if blocked {
        states.push(MigrationFsmState::Error);
    } else {
        states.extend([
            MigrationFsmState::Applying,
            MigrationFsmState::Linting,
            MigrationFsmState::Verifying,
            MigrationFsmState::Completed,
        ]);
    }

    let ledgers = vec![
        LedgerEntry {
            ledger: "schema_migrations".to_string(),
            checksum_sha256: sql_checksum(&sql_artifacts),
            description: "checksum of generated SQL artifacts".to_string(),
        },
        LedgerEntry {
            ledger: "proto_schema_versions".to_string(),
            checksum_sha256: manifest.checksum_sha256.clone(),
            description: "checksum of parsed proto AST manifest".to_string(),
        },
        LedgerEntry {
            ledger: "resource_actions".to_string(),
            checksum_sha256: resource_actions_checksum(&resource_actions)?,
            description: "checksum of universal backend provisioning actions".to_string(),
        },
    ];

    Ok(MigrationPlan {
        generated_at_unix: generated_at_unix(),
        trace: TraceContext::default(),
        states,
        manifest,
        changes,
        auto_count,
        blocked_count,
        operations_hash,
        sql_artifacts,
        resource_actions,
        dsn_entries: dsn_catalog.entries,
        ledgers,
        blocked,
    })
}

fn build_resource_actions(
    manifest: &CatalogManifest,
    dsn_entries: &[UnifiedDsn],
) -> Result<Vec<ResourceAction>, serde_json::Error> {
    let plan = try_build_provisioning_plan(manifest, dsn_entries).map_err(|err| {
        serde_json::Error::io(std::io::Error::new(std::io::ErrorKind::InvalidInput, err))
    })?;
    Ok(plan
        .actions
        .into_iter()
        .map(|action| ResourceAction {
            action: action.action,
            rollback_action: action.rollback_action,
            trace: action.trace,
            metric_labels: action.metric_labels,
            tier: action.tier,
            store_kind: action.store_kind,
            backend: action.backend,
            resource_kind: action.resource_kind,
            resource_name: action.resource_name,
            resource_uri: action.resource_uri,
            owner_schema: action.owner_schema,
            owner_table: action.owner_table,
            dsn: action.dsn,
            parameters: action.parameters,
        })
        .collect())
}

fn resource_actions_checksum(actions: &[ResourceAction]) -> Result<String, serde_json::Error> {
    use sha2::{Digest, Sha256};

    let mut canonical = actions.iter().collect::<Vec<_>>();
    canonical.sort_by(|a, b| {
        (
            a.tier.as_str(),
            a.backend.as_str(),
            a.owner_schema.as_str(),
            a.owner_table.as_str(),
            a.resource_kind.as_str(),
            a.resource_name.as_str(),
            a.action.as_str(),
        )
            .cmp(&(
                b.tier.as_str(),
                b.backend.as_str(),
                b.owner_schema.as_str(),
                b.owner_table.as_str(),
                b.resource_kind.as_str(),
                b.resource_name.as_str(),
                b.action.as_str(),
            ))
    });
    let json = serde_json::to_vec(&canonical)?;
    let digest = Sha256::digest(json);
    Ok(format!("sha256:{digest:x}"))
}

fn sql_checksum(artifacts: &[GeneratedArtifact]) -> String {
    use sha2::{Digest, Sha256};

    let mut canonical = artifacts.iter().collect::<Vec<_>>();
    canonical.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    let mut hasher = Sha256::new();
    for artifact in canonical {
        hasher.update(artifact.rel_path.as_bytes());
        hasher.update(b"\0");
        hasher.update(artifact.content.as_bytes());
        hasher.update(b"\0");
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn operations_hash(changes: &[ChangeOperation]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    for change in changes {
        hasher.update(change.fingerprint.as_bytes());
        hasher.update(b"\0");
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn generated_at_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}
