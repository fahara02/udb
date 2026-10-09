//! Immutable candidate planning and native evidence for explicitly reviewed catalogs.
//! Ordinary backward compatibility never consumes this authority implicitly.
use super::catalog_admin::*;
use super::*;
use crate::migration::diff::{ChangeKind, ChangeOperation, ChangeSafety};
use crate::runtime::system::SystemCatalogConfig;

const TRANSITIONS_TABLE: &str = "udb_catalog_reviewed_transitions";
const MAX_TRANSACTIONAL_ARTIFACT_BYTES: usize = 1024 * 1024;

#[derive(Debug, PartialEq)]
struct ReviewedIndexShape {
    valid: bool,
    ready: bool,
    live: bool,
    unique_index: bool,
    immediate: bool,
    method: String,
    keys: Vec<String>,
    include_columns: Vec<String>,
    predicate: String,
    plain_keys: bool,
    key_options: String,
    operator_classes: String,
    collations: String,
    parameters: Vec<String>,
    nulls_not_distinct: bool,
}

impl<'row> sqlx::FromRow<'row, sqlx::postgres::PgRow> for ReviewedIndexShape {
    fn from_row(row: &'row sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        Ok(Self {
            valid: row.try_get("valid")?,
            ready: row.try_get("ready")?,
            live: row.try_get("live")?,
            unique_index: row.try_get("unique_index")?,
            immediate: row.try_get("immediate")?,
            method: row.try_get("method")?,
            keys: row.try_get("keys")?,
            include_columns: row.try_get("include_columns")?,
            predicate: row.try_get("predicate")?,
            plain_keys: row.try_get("plain_keys")?,
            key_options: row.try_get("key_options")?,
            operator_classes: row.try_get("operator_classes")?,
            collations: row.try_get("collations")?,
            parameters: row.try_get("parameters")?,
            nulls_not_distinct: row.try_get("nulls_not_distinct")?,
        })
    }
}

/// Callers must supply the authenticated tenant and canonical verified subject.
/// The target is immutable before staging; its outer integrity is distinct from
/// the manifest's inner semantic checksum and the catalog's raw request selector.
#[derive(Debug, Clone)]
pub struct ReviewedCatalogPlanRequest {
    pub tenant_id: String,
    pub candidate_manifest_json: Vec<u8>,
    pub expected_active_catalog_id: String,
    pub expected_active_manifest_integrity_sha256: String,
    pub idempotency_key: String,
    pub actor: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct ReviewedCatalogPlan {
    tenant_id: String,
    project_id: String,
    expected_active_catalog_id: Uuid,
    expected_active_catalog_version: String,
    expected_active_manifest_integrity_sha256: String,
    target_manifest_integrity_sha256: String,
    target_schema_checksum_sha256: String,
    target_manifest: CatalogManifest,
    changes: Vec<ChangeOperation>,
    artifacts: Vec<GeneratedArtifact>,
    operations_hash: String,
    reviewed_operation_fingerprints: Vec<String>,
    target_instance: String,
    target_provenance_sha256: String,
    planned_by: String,
    request_fingerprint: String,
}

fn relation() -> String {
    let config = SystemCatalogConfig::default();
    format!(
        "{}.{}",
        qi_runtime(&config.cdc.system_schema),
        qi_runtime(TRANSITIONS_TABLE)
    )
}

fn refusal(code: &'static str, message: impl Into<String>) -> tonic::Status {
    catalog_admin_schema_status("reviewed_catalog_transition", code, message)
}

pub(super) fn require_reviewed_identity(tenant: &str, actor: &str) -> Result<(), tonic::Status> {
    if tenant.trim().is_empty() || actor.trim().is_empty() {
        return Err(refusal(
            "reviewed_verified_identity_required",
            "reviewed transitions require an authenticated tenant and canonical verified subject",
        ));
    }
    Ok(())
}

fn change_hash(changes: &[ChangeOperation]) -> String {
    let mut hash = Sha256::new();
    for change in changes {
        hash.update(change.fingerprint.as_bytes());
        hash.update(b"\0");
    }
    format!("sha256:{:x}", hash.finalize())
}

fn plan_integrity(plan: &ReviewedCatalogPlan) -> Result<String, tonic::Status> {
    let raw = serde_json::to_vec(plan)
        .map_err(|err| catalog_admin_internal_status("reviewed_plan_encode", err.to_string()))?;
    Ok(catalog_request_fingerprint(&[
        b"REVIEWED_CATALOG_PLAN_V1",
        &raw,
    ]))
}

fn validate_candidate(project: &str, manifest: &CatalogManifest) -> Result<String, tonic::Status> {
    let expected = crate::generation::manifest::catalog_checksum_sha256(manifest)
        .map_err(|err| refusal("reviewed_target_invalid", err.to_string()))?;
    let lint = crate::generation::lint_catalog(manifest);
    if manifest.checksum_sha256.trim() != expected
        || !manifest.validation_errors.is_empty()
        || lint
            .items
            .iter()
            .any(|item| matches!(item.severity, crate::generation::LintSeverity::Error))
    {
        return Err(refusal(
            "reviewed_target_invalid",
            "candidate failed canonical checksum or catalog validation",
        ));
    }
    catalog_manifest_integrity_sha256("reviewed_catalog_transition", project, manifest)
}

fn foreign_key_action(action: &str) -> Result<&'static str, tonic::Status> {
    match action.trim().to_ascii_uppercase().as_str() {
        "" | "NO ACTION" => Ok("a"),
        "RESTRICT" => Ok("r"),
        "CASCADE" => Ok("c"),
        "SET NULL" => Ok("n"),
        "SET DEFAULT" => Ok("d"),
        _ => Err(refusal(
            "reviewed_foreign_key_action_unsupported",
            "foreign key action lacks exact native reviewed proof",
        )),
    }
}

fn allowed_changes(
    base: &CatalogManifest,
    target: &CatalogManifest,
    changes: &[ChangeOperation],
) -> Result<(), tonic::Status> {
    for change in changes {
        if change.safety == ChangeSafety::Blocked || change.data_destructive {
            return Err(refusal(
                "reviewed_transition_blocked",
                "blocked or data-destructive changes cannot use reviewed catalog activation",
            ));
        }
        // These operations have native transactional SQL and observable target
        // verification. New backend/nontransactional capabilities need their own
        // durable application proof rather than a catalog-only approval bypass.
        if !matches!(
            change.kind,
            ChangeKind::HintWarning
                | ChangeKind::AddSchema
                | ChangeKind::CreateTable
                | ChangeKind::AddColumn
                | ChangeKind::DropNotNull
                | ChangeKind::AddForeignKey
                | ChangeKind::CreateIndex
                | ChangeKind::DropIndex
                | ChangeKind::AlterTableSecurity
        ) {
            return Err(refusal(
                "reviewed_operation_unsupported",
                format!("native reviewed proof is unavailable for {:?}", change.kind),
            ));
        }
        if change.kind == ChangeKind::AlterTableSecurity {
            let old = base.table(&change.schema, &change.table).ok_or_else(|| {
                refusal(
                    "reviewed_target_invalid",
                    "security change lacks an exact prior table",
                )
            })?;
            let new = target.table(&change.schema, &change.table).ok_or_else(|| {
                refusal(
                    "reviewed_target_invalid",
                    "security change lacks an exact candidate table",
                )
            })?;
            let mut old_security = old.table_security.clone();
            let mut new_security = new.table_security.clone();
            // Retention class is catalog compliance metadata, not physical DDL.
            // Other isolation/encryption changes cannot be recorded as applied
            // by the SQL renderer (which intentionally emits no SQL for them).
            old_security.retention_class.clear();
            new_security.retention_class.clear();
            if old_security != new_security {
                return Err(refusal(
                    "reviewed_security_application_unsupported",
                    "only exact reviewed retention-class metadata changes have native catalog proof",
                ));
            }
        }
        if !matches!(change.kind, ChangeKind::HintWarning | ChangeKind::AddSchema) {
            let table = target.table(&change.schema, &change.table).ok_or_else(|| {
                refusal(
                    "reviewed_target_invalid",
                    "changed table is absent from candidate",
                )
            })?;
            if change.kind == ChangeKind::CreateTable
                && (!table.foreign_keys.is_empty()
                    || !table.checks.is_empty()
                    || !table.triggers.is_empty()
                    || !table.materialized_views.is_empty()
                    || !table.sql_artifacts.is_empty()
                    || !table.partition_strategy.is_empty())
            {
                return Err(refusal(
                    "reviewed_table_verification_unsupported",
                    "new table has physical objects without native reviewed proof",
                ));
            }
            for column in &table.columns {
                if column.generated
                    || column.is_identity
                    || column.auto_increment
                    || !column.enum_values.is_empty()
                    || !column.collation.is_empty()
                {
                    return Err(refusal(
                        "reviewed_column_verification_unsupported",
                        "affected table has generated, identity, enum, serial or collation properties without native reviewed proof",
                    ));
                }
            }
            if table.enable_rls
                && table.rls_policies.is_empty()
                && (crate::generation::sql::resolve_tenant_column(table).is_some()
                    || crate::generation::sql::resolve_project_column(table).is_some())
            {
                return Err(refusal(
                    "reviewed_policy_verification_unsupported",
                    "affected RLS table requires explicit policy definitions for native reviewed proof",
                ));
            }
            for foreign_key in &table.foreign_keys {
                if foreign_key.not_valid
                    || foreign_key.initially_deferred && !foreign_key.deferrable
                {
                    return Err(refusal(
                        "reviewed_foreign_key_application_incomplete",
                        "affected tables require validated and consistent foreign key enforcement",
                    ));
                }
                foreign_key_action(&foreign_key.on_update)?;
                foreign_key_action(&foreign_key.on_delete)?;
            }
            if change.kind == ChangeKind::AddForeignKey {
                if !table.foreign_keys.iter().any(|foreign_key| {
                    crate::generation::sql::derive_fk_name(&table.table, foreign_key)
                        == change.object_name
                }) {
                    return Err(refusal(
                        "reviewed_target_invalid",
                        "new foreign key is absent from the immutable candidate",
                    ));
                }
            }
            for index in table.indexes.iter().filter(|index| {
                change.kind == ChangeKind::CreateTable
                    || change.kind == ChangeKind::CreateIndex
                        && crate::generation::sql::derive_index_name(table, index)
                            == change.object_name
            }) {
                if index.concurrent
                    || index.unique
                        && !index.method.is_empty()
                        && !index.method.eq_ignore_ascii_case("btree")
                    || !index.operator_class.is_empty()
                    || !index.index_params.is_empty()
                    || index.columns.iter().any(|name| {
                        !table
                            .columns
                            .iter()
                            .any(|column| column.column_name == *name)
                    })
                {
                    return Err(refusal(
                        "reviewed_index_verification_unsupported",
                        "new or changed index requires unsupported nontransactional/expression/operator proof",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn native_artifacts(run_id: Uuid, plan: &ReviewedCatalogPlan) -> Vec<GeneratedArtifact> {
    plan.artifacts
        .iter()
        .map(|artifact| {
            let mut artifact = artifact.clone();
            artifact.rel_path = format!("catalog-transition/{run_id}/{}", artifact.rel_path);
            artifact
        })
        .collect()
}

pub(super) fn reviewed_sql_artifact(
    payload: &serde_json::Value,
) -> Result<GeneratedArtifact, tonic::Status> {
    let artifact: GeneratedArtifact =
        serde_json::from_value(payload.get("artifact").cloned().ok_or_else(|| {
            refusal(
                "reviewed_artifact_invalid",
                "missing immutable generated artifact",
            )
        })?)
        .map_err(|err| refusal("reviewed_artifact_invalid", err.to_string()))?;
    if !artifact.rel_path.starts_with("catalog-transition/")
        || artifact.content.len() > MAX_TRANSACTIONAL_ARTIFACT_BYTES
        || !artifact.content.trim_end().ends_with("COMMIT;")
        || !artifact.content.contains("\nBEGIN;\n")
    {
        return Err(refusal(
            "reviewed_artifact_unsupported",
            "native reviewed artifacts must use the atomic transactional applier",
        ));
    }
    Ok(artifact)
}

impl DataBrokerRuntime {
    pub(super) async fn ensure_catalog_transition_storage(&self) -> Result<(), tonic::Status> {
        let config = SystemCatalogConfig::default();
        let rel = relation();
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {rel} (
            run_id UUID PRIMARY KEY REFERENCES {}(run_id),
            tenant_id TEXT NOT NULL CHECK (tenant_id <> ''),
            project_id TEXT NOT NULL CHECK (project_id <> ''),
            idempotency_key TEXT NOT NULL CHECK (idempotency_key <> ''),
            plan_json JSONB NOT NULL, plan_integrity_sha256 TEXT NOT NULL,
            approved_by TEXT NOT NULL DEFAULT '', approved_at TIMESTAMPTZ,
            applied_by TEXT NOT NULL DEFAULT '',
            applied_operations_hash TEXT NOT NULL DEFAULT '',
            application_evidence_sha256 TEXT NOT NULL DEFAULT '', applied_at TIMESTAMPTZ,
            staged_catalog_id UUID UNIQUE, compatibility_evidence_sha256 TEXT NOT NULL DEFAULT '',
            UNIQUE(tenant_id,project_id,idempotency_key))",
            config.migration_runs_relation()
        ))
        .execute(self.pg_pool()?)
        .await
        .map_err(|err| {
            catalog_admin_internal_status("reviewed_transition_storage", err.to_string())
        })?;
        Ok(())
    }

    pub(super) async fn reviewed_catalog_plan_exists(
        &self,
        id: Uuid,
    ) -> Result<bool, tonic::Status> {
        let pool = self.pg_pool()?;
        let rel = relation();
        let present: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(&rel)
            .fetch_one(pool)
            .await
            .map_err(|err| {
                catalog_admin_internal_status("reviewed_storage_lookup", err.to_string())
            })?;
        if !present {
            return Ok(false);
        }
        sqlx::query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM {rel} WHERE run_id=$1)"
        ))
        .bind(id)
        .fetch_one(pool)
        .await
        .map_err(|err| catalog_admin_internal_status("reviewed_plan_lookup", err.to_string()))
    }

    async fn load_reviewed_plan(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        id: Uuid,
    ) -> Result<Option<ReviewedCatalogPlan>, tonic::Status> {
        let present: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(relation())
            .fetch_one(&mut **tx)
            .await
            .map_err(|err| {
                catalog_admin_internal_status("reviewed_storage_lookup", err.to_string())
            })?;
        if !present {
            return Ok(None);
        }
        let row: Option<(String,String,String,String)> = sqlx::query_as(&format!(
            "SELECT plan_json::TEXT,plan_integrity_sha256,tenant_id,project_id FROM {} WHERE run_id=$1", relation()))
            .bind(id).fetch_optional(&mut **tx).await.map_err(|err|
                catalog_admin_internal_status("reviewed_plan_load", err.to_string()))?;
        row.map(|(raw, integrity, tenant, project)| {
            let plan: ReviewedCatalogPlan = serde_json::from_str(&raw)
                .map_err(|err| refusal("reviewed_plan_invalid", err.to_string()))?;
            if plan_integrity(&plan)? != integrity
                || plan.tenant_id != tenant
                || plan.project_id != project
                || validate_candidate(&project, &plan.target_manifest)?
                    != plan.target_manifest_integrity_sha256
                || plan.target_manifest.checksum_sha256 != plan.target_schema_checksum_sha256
                || change_hash(&plan.changes) != plan.operations_hash
            {
                return Err(refusal(
                    "reviewed_plan_integrity_mismatch",
                    "durable candidate plan integrity is invalid",
                ));
            }
            Ok(plan)
        })
        .transpose()
    }

    pub async fn plan_reviewed_catalog_transition(
        &self,
        project_id: &str,
        request: &ReviewedCatalogPlanRequest,
        dry_run: bool,
    ) -> Result<String, tonic::Status> {
        require_reviewed_identity(&request.tenant_id, &request.actor)?;
        let project = canonical_catalog_project_id(project_id)?;
        if request.idempotency_key.trim().is_empty() {
            return Err(catalog_admin_invalid_field(
                "idempotency_key",
                "must be non-empty",
                "candidate planning requires idempotency",
            ));
        }
        let base_id = request
            .expected_active_catalog_id
            .parse::<Uuid>()
            .map_err(|_| {
                refusal(
                    "reviewed_base_required",
                    "candidate planning requires an exact ACTIVE catalog UUID",
                )
            })?;
        let target: CatalogManifest = serde_json::from_slice(&request.candidate_manifest_json)
            .map_err(|err| refusal("reviewed_target_invalid", err.to_string()))?;
        let target_integrity = validate_candidate(&project, &target)?;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        self.ensure_catalog_transition_storage().await?;
        ensure_migration_payload_json_column(pool, &config.migration_op_ledger_relation()).await?;
        ensure_migration_run_provenance_columns(pool, &config.migration_runs_relation()).await?;
        let mut tx = pool
            .begin()
            .await
            .map_err(|err| catalog_admin_internal_status("reviewed_plan_begin", err.to_string()))?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,534154))")
            .bind(&project)
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                catalog_admin_internal_status("reviewed_project_lock", err.to_string())
            })?;
        let request_fingerprint = catalog_request_fingerprint(&[
            b"REVIEWED_PLAN_REQUEST_V1",
            request.tenant_id.as_bytes(),
            project.as_bytes(),
            base_id.as_bytes(),
            request.expected_active_manifest_integrity_sha256.as_bytes(),
            target_integrity.as_bytes(),
            request.actor.as_bytes(),
            if dry_run { b"DRY_RUN" } else { b"PREFLIGHT" },
        ]);
        let prior: Option<Uuid> = sqlx::query_scalar(&format!(
            "SELECT run_id FROM {} WHERE tenant_id=$1 AND project_id=$2 AND idempotency_key=$3",
            relation()
        ))
        .bind(&request.tenant_id)
        .bind(&project)
        .bind(request.idempotency_key.trim())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| catalog_admin_internal_status("reviewed_plan_replay", err.to_string()))?;
        if let Some(id) = prior {
            let plan = self.load_reviewed_plan(&mut tx, id).await?.ok_or_else(|| {
                refusal("reviewed_plan_missing", "idempotency row has no candidate")
            })?;
            if plan.request_fingerprint != request_fingerprint {
                return Err(refusal(
                    "reviewed_plan_idempotency_conflict",
                    "candidate key already records a different exact request",
                ));
            }
            tx.commit().await.map_err(|err| {
                catalog_admin_internal_status("reviewed_plan_commit", err.to_string())
            })?;
            return Ok(id.to_string());
        }
        let active = self
            .load_active_catalog_for_project(&project)
            .await?
            .ok_or_else(|| {
                refusal(
                    "reviewed_base_missing",
                    "candidate planning requires a proven ACTIVE catalog",
                )
            })?;
        let base_integrity = catalog_manifest_integrity_sha256(
            "reviewed_catalog_transition",
            &project,
            &active.manifest,
        )?;
        if active.catalog_id != base_id.to_string()
            || base_integrity != request.expected_active_manifest_integrity_sha256
        {
            return Err(refusal(
                "reviewed_base_changed",
                "exact ACTIVE catalog UUID/integrity does not match candidate request",
            ));
        }
        let changes = crate::migration::plan::canonical_change_set(Some(&active.manifest), &target);
        allowed_changes(&active.manifest, &target, &changes)?;
        let artifacts = crate::generation::generate_review_delta_sql(
            &target,
            &changes,
            &crate::generation::SqlGenerationConfig::default(),
        );
        // Reject nontransactional work before any approval or physical mutation.
        for artifact in &artifacts {
            let mut check = artifact.clone();
            check.rel_path = format!("catalog-transition/check/{}", check.rel_path);
            reviewed_sql_artifact(&serde_json::json!({"artifact":check}))?;
        }
        let mut reviewed_operation_fingerprints: Vec<String> = changes
            .iter()
            .filter(|change| change.safety == ChangeSafety::RequiresReview)
            .map(|change| change.fingerprint.clone())
            .collect();
        reviewed_operation_fingerprints.sort();
        reviewed_operation_fingerprints.dedup();
        let target_route = self.project_postgres_write_target(&project, None).await?;
        let plan = ReviewedCatalogPlan {
            tenant_id: request.tenant_id.clone(),
            project_id: project.clone(),
            expected_active_catalog_id: base_id,
            expected_active_catalog_version: active.version.clone(),
            expected_active_manifest_integrity_sha256: base_integrity,
            target_manifest_integrity_sha256: target_integrity,
            target_schema_checksum_sha256: target.checksum_sha256.clone(),
            target_manifest: target,
            operations_hash: change_hash(&changes),
            changes,
            artifacts,
            reviewed_operation_fingerprints,
            target_instance: target_route.instance,
            target_provenance_sha256: target_route.provenance_sha256,
            planned_by: request.actor.clone(),
            request_fingerprint,
        };
        let id = Uuid::new_v4();
        sqlx::query(&format!(
            "INSERT INTO {}(run_id,project_id,catalog_version,catalog_id,catalog_checksum_sha256,
            target_backend,target_instance,target_provenance_sha256,state,operations_hash)
            VALUES($1,$2,$3,$4,$5,'postgres',$6,$7,$8,$9)",
            config.migration_runs_relation()
        ))
        .bind(id)
        .bind(&project)
        .bind(&active.version)
        .bind(base_id)
        .bind(&active.checksum_sha256)
        .bind(&plan.target_instance)
        .bind(&plan.target_provenance_sha256)
        .bind(if dry_run { "DRY_RUN" } else { "PREFLIGHT" })
        .bind(&plan.operations_hash)
        .execute(&mut *tx)
        .await
        .map_err(|err| catalog_admin_internal_status("reviewed_run_insert", err.to_string()))?;
        sqlx::query(&format!("INSERT INTO {}(run_id,tenant_id,project_id,idempotency_key,plan_json,plan_integrity_sha256)
            VALUES($1,$2,$3,$4,$5::JSONB,$6)",relation()))
            .bind(id).bind(&plan.tenant_id).bind(&project).bind(request.idempotency_key.trim())
            .bind(serde_json::to_value(&plan).map_err(|err| catalog_admin_internal_status("reviewed_plan_encode",err.to_string()))?)
            .bind(plan_integrity(&plan)?).execute(&mut *tx).await.map_err(|err| catalog_admin_internal_status("reviewed_plan_insert",err.to_string()))?;
        for (index, artifact) in native_artifacts(id, &plan).into_iter().enumerate() {
            sqlx::query(&format!("INSERT INTO {}(run_id,operation_index,backend,resource_uri,operation_kind,status,payload_json)
                VALUES($1,$2,'postgres',$3,'reviewed_sql','PENDING',$4::JSONB)",config.migration_op_ledger_relation()))
                .bind(id).bind(index as i32).bind(&artifact.rel_path)
                .bind(serde_json::json!({"artifact":artifact})).execute(&mut *tx).await.map_err(|err|
                    catalog_admin_internal_status("reviewed_operation_insert",err.to_string()))?;
        }
        tx.commit().await.map_err(|err| {
            catalog_admin_internal_status("reviewed_plan_commit", err.to_string())
        })?;
        Ok(id.to_string())
    }

    pub async fn approve_reviewed_catalog_transition(
        &self,
        project_id: &str,
        run_id: &str,
        approval_token: &str,
        tenant_id: &str,
        actor: &str,
        expected_operations_hash: &str,
        reviewed_operation_fingerprints: &[String],
    ) -> Result<String, tonic::Status> {
        require_reviewed_identity(tenant_id, actor)?;
        validate_approval_token_for_plan(approval_token)?;
        let project = canonical_catalog_project_id(project_id)?;
        let id = parse_migration_run_id(run_id)?;
        self.ensure_catalog_transition_storage().await?;
        let config = SystemCatalogConfig::default();
        let pool = self.pg_pool()?;
        ensure_migration_runs_approved_state(
            pool,
            &config.migration_runs_relation(),
            &config.migration_runs_table,
        )
        .await?;
        let mut tx = pool.begin().await.map_err(|err| {
            catalog_admin_internal_status("reviewed_approval_begin", err.to_string())
        })?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,534154))")
            .bind(&project)
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                catalog_admin_internal_status("reviewed_project_lock", err.to_string())
            })?;
        let plan = self
            .load_reviewed_plan(&mut tx, id)
            .await?
            .ok_or_else(|| refusal("reviewed_plan_missing", "candidate plan not found"))?;
        if plan.project_id != project || plan.tenant_id != tenant_id {
            return Err(refusal(
                "reviewed_scope_mismatch",
                "candidate belongs to a different tenant/project",
            ));
        }
        let mut coverage = reviewed_operation_fingerprints.to_vec();
        coverage.sort();
        if coverage.windows(2).any(|pair| pair[0] == pair[1])
            || coverage != plan.reviewed_operation_fingerprints
            || expected_operations_hash != plan.operations_hash
        {
            return Err(refusal(
                "reviewed_operation_coverage_mismatch",
                "approval must cover the exact unique reviewed fingerprints and full operations hash",
            ));
        }
        let row: (String, String, String) = sqlx::query_as(&format!(
            "SELECT r.state,r.approval_token,t.approved_by FROM {} r JOIN {} t USING(run_id)
             WHERE r.run_id=$1 AND r.project_id=$2 FOR UPDATE",
            config.migration_runs_relation(),
            relation()
        ))
        .bind(id)
        .bind(&project)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| catalog_admin_internal_status("reviewed_approval_load", err.to_string()))?;
        if !row.2.is_empty() {
            if row.2 != actor || row.1.is_empty() {
                return Err(refusal(
                    "reviewed_approval_replay_conflict",
                    "approval already belongs to another verified actor or lacks its durable token",
                ));
            }
            tx.commit().await.map_err(|err| {
                catalog_admin_internal_status("reviewed_approval_commit", err.to_string())
            })?;
            return Ok(row.1);
        }
        if row.0 != "PREFLIGHT" || !row.1.is_empty() {
            return Err(refusal(
                "reviewed_approval_state_invalid",
                "only an immutable preflight candidate can be approved",
            ));
        }
        let active = self
            .load_active_catalog_for_project(&project)
            .await?
            .ok_or_else(|| refusal("reviewed_base_changed", "ACTIVE catalog is absent"))?;
        self.validate_reviewed_plan_base(&plan, &active)?;
        sqlx::query(&format!(
            "UPDATE {} SET state='APPROVED',approval_token=$2,error='' WHERE run_id=$1",
            config.migration_runs_relation()
        ))
        .bind(id)
        .bind(approval_token.trim())
        .execute(&mut *tx)
        .await
        .map_err(|err| catalog_admin_internal_status("reviewed_approval_store", err.to_string()))?;
        sqlx::query(&format!(
            "UPDATE {} SET approved_by=$2,approved_at=NOW() WHERE run_id=$1",
            relation()
        ))
        .bind(id)
        .bind(actor)
        .execute(&mut *tx)
        .await
        .map_err(|err| catalog_admin_internal_status("reviewed_actor_store", err.to_string()))?;
        tx.commit().await.map_err(|err| {
            catalog_admin_internal_status("reviewed_approval_commit", err.to_string())
        })?;
        Ok(approval_token.trim().to_string())
    }

    fn validate_reviewed_plan_base(
        &self,
        plan: &ReviewedCatalogPlan,
        active: &ProjectCatalogRecord,
    ) -> Result<(), tonic::Status> {
        if active.catalog_id != plan.expected_active_catalog_id.to_string()
            || catalog_manifest_integrity_sha256(
                "reviewed_catalog_transition",
                &plan.project_id,
                &active.manifest,
            )? != plan.expected_active_manifest_integrity_sha256
        {
            return Err(refusal(
                "reviewed_base_changed",
                "exact proven ACTIVE catalog no longer matches the immutable candidate base",
            ));
        }
        let changes = crate::migration::plan::canonical_change_set(
            Some(&active.manifest),
            &plan.target_manifest,
        );
        allowed_changes(&active.manifest, &plan.target_manifest, &changes)?;
        if changes != plan.changes {
            return Err(refusal(
                "reviewed_operation_coverage_mismatch",
                "canonical base-to-target operations changed",
            ));
        }
        Ok(())
    }

    pub(super) async fn reviewed_catalog_apply_preflight(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        project: &str,
        id: Uuid,
        caller: Option<(&str, &str)>,
        active: &ProjectCatalogRecord,
        target: &ProjectPostgresWriteTarget,
    ) -> Result<Option<ReviewedCatalogPlan>, tonic::Status> {
        let Some(plan) = self.load_reviewed_plan(tx, id).await? else {
            return Ok(None);
        };
        let (tenant, actor) = caller.ok_or_else(|| {
            refusal(
                "reviewed_verified_identity_required",
                "reviewed application requires authenticated caller identity",
            )
        })?;
        require_reviewed_identity(tenant, actor)?;
        if plan.project_id != project || plan.tenant_id != tenant {
            return Err(refusal(
                "reviewed_scope_mismatch",
                "candidate belongs to another tenant/project",
            ));
        }
        self.validate_reviewed_plan_base(&plan, active)?;
        if plan.target_instance != target.instance
            || plan.target_provenance_sha256 != target.provenance_sha256
        {
            return Err(refusal(
                "reviewed_target_authority_changed",
                "candidate routed write target changed",
            ));
        }
        let approved: bool = sqlx::query_scalar(&format!(
            "SELECT approved_by<>'' AND approved_at IS NOT NULL FROM {} WHERE run_id=$1",
            relation()
        ))
        .bind(id)
        .fetch_one(&mut **tx)
        .await
        .map_err(|err| {
            catalog_admin_internal_status("reviewed_approval_verify", err.to_string())
        })?;
        if !approved {
            return Err(refusal(
                "reviewed_approval_missing",
                "candidate lacks actual durable actor approval",
            ));
        }
        let operations_hash: String = sqlx::query_scalar(&format!(
            "SELECT operations_hash FROM {} WHERE run_id=$1 AND project_id=$2",
            SystemCatalogConfig::default().migration_runs_relation()
        ))
        .bind(id)
        .bind(project)
        .fetch_one(&mut **tx)
        .await
        .map_err(|err| {
            catalog_admin_internal_status("reviewed_run_hash_verify", err.to_string())
        })?;
        if operations_hash != plan.operations_hash {
            return Err(refusal(
                "reviewed_native_operations_mismatch",
                "native run operations hash differs from the immutable candidate",
            ));
        }
        Ok(Some(plan))
    }

    pub(super) async fn replay_reviewed_completed_application(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        project: &str,
        id: Uuid,
        caller: Option<(&str, &str)>,
        provided_token: &str,
        stored_token: &str,
    ) -> Result<bool, tonic::Status> {
        let Some(plan) = self.load_reviewed_plan(tx, id).await? else {
            return Ok(false);
        };
        let (tenant, actor) = caller.ok_or_else(|| {
            refusal(
                "reviewed_verified_identity_required",
                "reviewed application replay requires authenticated identity",
            )
        })?;
        require_reviewed_identity(tenant, actor)?;
        if plan.tenant_id != tenant
            || plan.project_id != project
            || !migration_approval_tokens_match(provided_token, stored_token)
        {
            return Err(refusal(
                "reviewed_apply_replay_conflict",
                "reviewed application replay has foreign scope or approval token",
            ));
        }
        let target = self
            .project_postgres_write_target(project, Some(&plan.target_instance))
            .await?;
        if target.provenance_sha256 != plan.target_provenance_sha256
            || !self.verified_reviewed_target(&plan, &target.pool).await?
        {
            return Err(refusal(
                "reviewed_target_verification_failed",
                "completed reviewed application no longer verifies against its exact target",
            ));
        }
        let row: (String, String, String, bool, Option<Uuid>) = sqlx::query_as(&format!(
            "SELECT t.approved_by,t.applied_operations_hash,t.application_evidence_sha256,
                t.approved_at IS NOT NULL AND t.applied_at IS NOT NULL AND r.finished_at IS NOT NULL
                AND r.operations_hash=$2 AND r.error='',t.staged_catalog_id
             FROM {} t JOIN {} r USING(run_id) WHERE t.run_id=$1",
            relation(),
            SystemCatalogConfig::default().migration_runs_relation()
        ))
        .bind(id)
        .bind(&plan.operations_hash)
        .fetch_one(&mut **tx)
        .await
        .map_err(|err| {
            catalog_admin_internal_status("reviewed_apply_replay_evidence", err.to_string())
        })?;
        if row.0.is_empty()
            || row.1 != plan.operations_hash
            || !row.3
            || self
                .reviewed_application_evidence(tx, id, &plan, &target.pool)
                .await?
                != row.2
        {
            return Err(refusal(
                "reviewed_application_evidence_mismatch",
                "completed native application evidence is absent or changed",
            ));
        }
        let active = self
            .load_active_catalog_for_project(project)
            .await?
            .ok_or_else(|| {
                refusal(
                    "reviewed_base_changed",
                    "completed application replay requires a proven ACTIVE catalog",
                )
            })?;
        let active_id = active
            .catalog_id
            .parse::<Uuid>()
            .map_err(|_| refusal("reviewed_base_changed", "ACTIVE catalog UUID is invalid"))?;
        if active_id != plan.expected_active_catalog_id && Some(active_id) != row.4 {
            return Err(refusal(
                "reviewed_base_changed",
                "completed application no longer binds the current ACTIVE base or activated target",
            ));
        }
        Ok(true)
    }

    pub(super) fn validate_reviewed_native_operations(
        &self,
        id: Uuid,
        plan: &ReviewedCatalogPlan,
        rows: &[(i64, i32, String, String, String, serde_json::Value)],
    ) -> Result<(), tonic::Status> {
        let artifacts = native_artifacts(id, plan);
        if rows.len() != artifacts.len() {
            return Err(refusal(
                "reviewed_native_operations_mismatch",
                "native operation ledger is incomplete",
            ));
        }
        for (index, (row, artifact)) in rows.iter().zip(artifacts).enumerate() {
            if row.1 != index as i32
                || row.2 != "postgres"
                || row.3 != artifact.rel_path
                || row.4 != "reviewed_sql"
                || reviewed_sql_artifact(&row.5)? != artifact
            {
                return Err(refusal(
                    "reviewed_native_operations_mismatch",
                    "native operation ledger differs from the immutable candidate",
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn reviewed_artifact_applied(
        &self,
        pool: &PgPool,
        payload: &serde_json::Value,
    ) -> Result<bool, tonic::Status> {
        let artifact = reviewed_sql_artifact(payload)?;
        let row: Option<(String, String)> =
            sqlx::query_as("SELECT checksum,state FROM public.schema_migrations WHERE filename=$1")
                .bind(&artifact.rel_path)
                .fetch_optional(pool)
                .await
                .map_err(|err| {
                    catalog_admin_internal_status("reviewed_target_receipt", err.to_string())
                })?;
        match row {
            Some((checksum, state))
                if checksum == artifact_content_checksum(&artifact.content)
                    && state == "applied" =>
            {
                Ok(true)
            }
            Some(_) => Err(refusal(
                "reviewed_target_receipt_mismatch",
                "target artifact receipt does not match the immutable generated content",
            )),
            None => Ok(false),
        }
    }

    async fn verified_reviewed_target(
        &self,
        plan: &ReviewedCatalogPlan,
        pool: &PgPool,
    ) -> Result<bool, tonic::Status> {
        if !self
            .verify_postgres_manifest_drift_on_pool(&plan.target_manifest, pool)
            .await?
            .is_empty()
        {
            return Ok(false);
        }
        for change in plan
            .changes
            .iter()
            .filter(|change| change.kind == ChangeKind::AddSchema)
        {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname::TEXT=$1)",
            )
            .bind(&change.schema)
            .fetch_one(pool)
            .await
            .map_err(|err| {
                catalog_admin_internal_status("reviewed_schema_verification", err.to_string())
            })?;
            if !exists {
                return Ok(false);
            }
        }
        let schemas: Vec<String> = plan
            .target_manifest
            .tables
            .iter()
            .map(|table| table.schema.clone())
            .collect();
        let actual_indexes: std::collections::HashSet<(String, String, String)> =
            sqlx::query_as::<_, (String, String, String)>(
                "SELECT n.nspname::TEXT,t.relname::TEXT,x.relname::TEXT FROM pg_catalog.pg_index i
                 JOIN pg_catalog.pg_class x ON x.oid=i.indexrelid JOIN pg_catalog.pg_class t ON t.oid=i.indrelid
                 JOIN pg_catalog.pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname::TEXT=ANY($1)
                 AND i.indisvalid AND i.indisready")
                .bind(&schemas).fetch_all(pool).await.map_err(|err|
                    catalog_admin_internal_status("reviewed_scoped_index_verification", err.to_string()))?
                .into_iter().collect();
        // The canonical desired-manifest verifier covers required objects. A
        // reviewed removal must additionally prove the old named object absent.
        for change in plan
            .changes
            .iter()
            .filter(|change| change.kind == ChangeKind::DropIndex)
        {
            let exists:bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname::TEXT=$1 AND c.relname::TEXT=$2 AND c.relkind IN ('i','I'))")
                .bind(&change.schema).bind(&change.object_name).fetch_one(pool).await.map_err(|err| catalog_admin_internal_status("reviewed_drop_verification",err.to_string()))?;
            // A replacement can intentionally reuse the same index name. Its
            // exact desired shape is checked below, rather than demanding loss.
            let replacement = plan.changes.iter().any(|next| {
                next.kind == ChangeKind::CreateIndex
                    && next.schema == change.schema
                    && next.object_name == change.object_name
            });
            if exists && !replacement {
                return Ok(false);
            }
        }
        for table in &plan.target_manifest.tables {
            let affected_table = plan.changes.iter().any(|change| {
                change.kind != ChangeKind::HintWarning
                    && change.schema == table.schema
                    && change.table == table.table
            });
            let added_columns: Vec<&crate::generation::manifest::ManifestColumn> = table
                .columns
                .iter()
                .filter(|column| {
                    affected_table
                        || plan.changes.iter().any(|change| {
                            change.kind == ChangeKind::AddColumn
                                && change.schema == table.schema
                                && change.table == table.table
                                && change.column == column.column_name
                        })
                })
                .collect();
            let changed_indexes: Vec<&crate::generation::manifest::ManifestIndex> = table
                .indexes
                .iter()
                .filter(|index| {
                    let name = crate::generation::sql::derive_index_name(table, index);
                    plan.changes.iter().any(|change| {
                        change.schema == table.schema
                            && change.table == table.table
                            && (change.kind == ChangeKind::CreateTable
                                || change.kind == ChangeKind::CreateIndex
                                    && change.object_name == name)
                    })
                })
                .collect();
            if !added_columns.is_empty()
                && !self
                    .verify_reviewed_table_shape(
                        pool,
                        table,
                        &added_columns,
                        affected_table,
                        &changed_indexes,
                    )
                    .await?
            {
                return Ok(false);
            }
            for index in &table.indexes {
                let name = crate::generation::sql::derive_index_name(table, index);
                if !actual_indexes.contains(&(
                    table.schema.clone(),
                    table.table.clone(),
                    name.clone(),
                )) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    async fn verify_reviewed_table_shape(
        &self,
        pool: &PgPool,
        table: &ManifestTable,
        columns: &[&crate::generation::manifest::ManifestColumn],
        complete_shape: bool,
        indexes: &[&crate::generation::manifest::ManifestIndex],
    ) -> Result<bool, tonic::Status> {
        let mut tx = pool.begin().await.map_err(|err| {
            catalog_admin_internal_status("reviewed_shape_begin", err.to_string())
        })?;
        sqlx::raw_sql("SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='60s'")
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                catalog_admin_internal_status("reviewed_shape_deadlines", err.to_string())
            })?;
        let live: Option<(i64, String, String, bool, bool)> = sqlx::query_as(
            "SELECT c.oid::BIGINT,c.relkind::TEXT,c.relpersistence::TEXT,c.relrowsecurity,c.relforcerowsecurity
             FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
             WHERE n.nspname::TEXT=$1 AND c.relname::TEXT=$2")
            .bind(&table.schema).bind(&table.table).fetch_optional(&mut *tx).await.map_err(|err|
                catalog_admin_internal_status("reviewed_relation_shape", err.to_string()))?;
        let Some((live_oid, kind, persistence, rls, force_rls)) = live else {
            return Ok(false);
        };
        if !matches!(kind.as_str(), "r" | "p")
            || complete_shape
                && (kind
                    != if table.partition_strategy.is_empty() {
                        "r"
                    } else {
                        "p"
                    }
                    || persistence != if table.unlogged { "u" } else { "p" }
                    || rls != table.enable_rls
                    || force_rls != table.force_rls)
        {
            return Ok(false);
        }
        // PostgreSQL itself canonicalizes type modifiers and SQL expressions;
        // this avoids treating CURRENT_TIMESTAMP, casts, or SQL whitespace as
        // caller-asserted equality. Temporary definitions are rolled back and
        // no candidate defaults or row-policy predicates are evaluated.
        let temp_name = format!("udb_review_{}", Uuid::new_v4().simple());
        let temp_rel = format!("pg_temp.{}", qi_runtime(&temp_name));
        let mut definitions: Vec<String> = columns
            .iter()
            .map(|column| {
                let mut expected = (**column).clone();
                // The real table-level primary key makes its columns NOT NULL.
                expected.not_null |=
                    expected.is_primary || table.primary_key.contains(&expected.column_name);
                crate::generation::sql::render_column(&expected)
            })
            .collect();
        if complete_shape {
            definitions.extend(
                table
                    .checks
                    .iter()
                    .map(|check| format!("CHECK ({})", check.expression)),
            );
        }
        sqlx::query(&format!(
            "CREATE TEMP TABLE {} ({}) ON COMMIT DROP",
            qi_runtime(&temp_name),
            definitions.join(",")
        ))
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            catalog_admin_internal_status("reviewed_expected_shape_parse", err.to_string())
        })?;
        let temp_oid: i64 = sqlx::query_scalar("SELECT to_regclass($1)::OID::BIGINT")
            .bind(&temp_rel)
            .fetch_one(&mut *tx)
            .await
            .map_err(|err| {
                catalog_admin_internal_status("reviewed_expected_shape_lookup", err.to_string())
            })?;
        let column_rows:Vec<(i64,String,String,bool,String,i64)> = sqlx::query_as(
            "SELECT a.attrelid::BIGINT,a.attname::TEXT,pg_catalog.format_type(a.atttypid,a.atttypmod),a.attnotnull,
             COALESCE(pg_catalog.pg_get_expr(d.adbin,d.adrelid),''),a.attcollation::BIGINT FROM pg_catalog.pg_attribute a
             LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
             WHERE a.attrelid::BIGINT=ANY($1) AND a.attnum>0 AND NOT a.attisdropped")
            .bind(vec![live_oid,temp_oid]).fetch_all(&mut *tx).await.map_err(|err|
                catalog_admin_internal_status("reviewed_column_shape",err.to_string()))?;
        for column in columns {
            let expected = column_rows
                .iter()
                .find(|row| row.0 == temp_oid && row.1 == column.column_name);
            let actual = column_rows
                .iter()
                .find(|row| row.0 == live_oid && row.1 == column.column_name);
            if !matches!((expected,actual),(Some(e),Some(a)) if (e.2.as_str(),e.3,e.4.as_str(),e.5)==(a.2.as_str(),a.3,a.4.as_str(),a.5))
            {
                return Ok(false);
            }
        }
        if complete_shape {
            let checks: Vec<(i64, String, bool)> = sqlx::query_as(
                "SELECT conrelid::BIGINT,pg_catalog.pg_get_expr(conbin,conrelid),convalidated
                 FROM pg_catalog.pg_constraint WHERE conrelid::BIGINT=ANY($1) AND contype='c'",
            )
            .bind(vec![live_oid, temp_oid])
            .fetch_all(&mut *tx)
            .await
            .map_err(|err| {
                catalog_admin_internal_status("reviewed_check_shape", err.to_string())
            })?;
            let mut expected_checks: Vec<&str> = checks
                .iter()
                .filter(|row| row.0 == temp_oid)
                .map(|row| row.1.as_str())
                .collect();
            let mut live_checks: Vec<&str> = checks
                .iter()
                .filter(|row| row.0 == live_oid && row.2)
                .map(|row| row.1.as_str())
                .collect();
            expected_checks.sort();
            live_checks.sort();
            if expected_checks != live_checks
                || checks.iter().any(|row| row.0 == live_oid && !row.2)
            {
                return Ok(false);
            }
            for column in table
                .columns
                .iter()
                .filter(|column| column.unique && !column.is_primary)
            {
                let unique:bool=sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_attribute a
                     ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0] WHERE i.indrelid::BIGINT=$1
                     AND i.indisunique AND i.indisvalid AND i.indisready AND i.indnkeyatts=1
                     AND i.indpred IS NULL AND a.attname::TEXT=$2)")
                    .bind(live_oid).bind(&column.column_name).fetch_one(&mut *tx).await.map_err(|err|
                        catalog_admin_internal_status("reviewed_unique_column_shape",err.to_string()))?;
                if !unique {
                    return Ok(false);
                }
            }
            let primary:Option<Vec<String>> = sqlx::query_scalar(
                "SELECT ARRAY(SELECT a.attname::TEXT FROM unnest(p.conkey) WITH ORDINALITY k(attnum,pos)
                 JOIN pg_catalog.pg_attribute a ON a.attrelid=p.conrelid AND a.attnum=k.attnum ORDER BY k.pos)
                 FROM pg_catalog.pg_constraint p WHERE p.conrelid::BIGINT=$1 AND p.contype='p'")
                .bind(live_oid).fetch_optional(&mut *tx).await.map_err(|err|
                    catalog_admin_internal_status("reviewed_primary_key_shape",err.to_string()))?;
            if primary.unwrap_or_default() != table.primary_key {
                return Ok(false);
            }
            // Resolve both column arrays through their own relation authority;
            // a constraint name or deparsed REFERENCES string cannot establish
            // ordered keys, actions, deferrability or effective enforcement.
            let foreign_keys:Vec<(String,Vec<String>,String,String,Vec<String>,String,String,String,bool,bool,bool,bool,bool,bool)> = sqlx::query_as(
                "SELECT c.conname::TEXT,
                 ARRAY(SELECT a.attname::TEXT FROM unnest(c.conkey) WITH ORDINALITY k(attnum,pos)
                     JOIN pg_catalog.pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.attnum ORDER BY k.pos),
                 n.nspname::TEXT,r.relname::TEXT,
                 ARRAY(SELECT a.attname::TEXT FROM unnest(c.confkey) WITH ORDINALITY k(attnum,pos)
                     JOIN pg_catalog.pg_attribute a ON a.attrelid=c.confrelid AND a.attnum=k.attnum ORDER BY k.pos),
                 c.confmatchtype::TEXT,c.confupdtype::TEXT,c.confdeltype::TEXT,
                 c.condeferrable,c.condeferred,c.convalidated,
                 COALESCE((to_jsonb(c)->>'conenforced')::BOOLEAN,true),
                 EXISTS(SELECT 1 FROM pg_catalog.pg_trigger g WHERE g.tgconstraint=c.oid)
                     AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger g WHERE g.tgconstraint=c.oid
                                    AND g.tgenabled NOT IN ('O','A')),
                 COALESCE((to_jsonb(c)->'confdelsetcols')='null'::JSONB,true)
                 FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_class r ON r.oid=c.confrelid
                 JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
                 WHERE c.conrelid::BIGINT=$1 AND c.contype='f'")
                .bind(live_oid).fetch_all(&mut *tx).await.map_err(|err|
                    catalog_admin_internal_status("reviewed_foreign_key_shape",err.to_string()))?;
            if foreign_keys.len() != table.foreign_keys.len() {
                return Ok(false);
            }
            for foreign_key in &table.foreign_keys {
                let name = crate::generation::sql::derive_fk_name(&table.table, foreign_key);
                let Some(actual) = foreign_keys.iter().find(|row| row.0 == name) else {
                    return Ok(false);
                };
                if actual.1 != foreign_key.columns
                    || actual.2 != foreign_key.ref_schema
                    || actual.3 != foreign_key.ref_table
                    || actual.4 != foreign_key.ref_columns
                    || actual.5 != "s"
                    || actual.6 != foreign_key_action(&foreign_key.on_update)?
                    || actual.7 != foreign_key_action(&foreign_key.on_delete)?
                    || actual.8 != foreign_key.deferrable
                    || actual.9 != foreign_key.initially_deferred
                    || !actual.10
                    || !actual.11
                    || !actual.12
                    || !actual.13
                {
                    return Ok(false);
                }
            }
            for policy in &table.rls_policies {
                let command = match policy.command.trim().to_ascii_uppercase().as_str() {
                    "" | "ALL" => "ALL",
                    "SELECT" => "SELECT",
                    "INSERT" => "INSERT",
                    "UPDATE" => "UPDATE",
                    "DELETE" => "DELETE",
                    _ => {
                        return Err(refusal(
                            "reviewed_policy_invalid",
                            "candidate policy has an invalid command",
                        ));
                    }
                };
                let using = if policy.using_expression.is_empty() {
                    String::new()
                } else {
                    format!(" USING ({})", policy.using_expression)
                };
                let check = if policy.with_check.is_empty() {
                    String::new()
                } else {
                    format!(" WITH CHECK ({})", policy.with_check)
                };
                sqlx::query(&format!(
                    "CREATE POLICY {} ON {temp_rel} AS {} FOR {command}{using}{check}",
                    qi_runtime(&policy.name),
                    if policy.permissive {
                        "PERMISSIVE"
                    } else {
                        "RESTRICTIVE"
                    }
                ))
                .execute(&mut *tx)
                .await
                .map_err(|err| {
                    catalog_admin_internal_status("reviewed_expected_policy_parse", err.to_string())
                })?;
            }
            let policies:Vec<(i64,String,String,bool,bool,String,String)> = sqlx::query_as(
                "SELECT polrelid::BIGINT,polname::TEXT,polcmd::TEXT,polpermissive,polroles=ARRAY[0::OID],
                 COALESCE(pg_catalog.pg_get_expr(polqual,polrelid),''),COALESCE(pg_catalog.pg_get_expr(polwithcheck,polrelid),'')
                 FROM pg_catalog.pg_policy WHERE polrelid::BIGINT=ANY($1)")
                .bind(vec![live_oid,temp_oid]).fetch_all(&mut *tx).await.map_err(|err|
                    catalog_admin_internal_status("reviewed_row_policy_shape",err.to_string()))?;
            // An extra permissive policy changes the OR union and is not exact
            // target authority even if all expected policy names are present.
            if policies.iter().filter(|row| row.0 == live_oid).count() != table.rls_policies.len() {
                return Ok(false);
            }
            for policy in &table.rls_policies {
                let expected = policies
                    .iter()
                    .find(|row| row.0 == temp_oid && row.1 == policy.name);
                let actual = policies
                    .iter()
                    .find(|row| row.0 == live_oid && row.1 == policy.name);
                if !matches!((expected,actual),(Some(e),Some(a)) if e.2==a.2 && e.3==a.3 && e.4==a.4 && e.5==a.5 && e.6==a.6)
                {
                    return Ok(false);
                }
            }
        }
        // Build the canonical desired index on the parsed empty TEMP table.
        // PostgreSQL resolves predicates, key options, operator classes and
        // collations in the same database as the actual target. A matching name
        // and column list alone cannot authorize a pre-applied receipt.
        for index in indexes {
            let live_name = crate::generation::sql::derive_index_name(table, index);
            let mut expected_table = table.clone();
            expected_table.schema = "pg_temp".to_string();
            expected_table.table = temp_name.clone();
            let mut expected_index = (**index).clone();
            expected_index.name = format!("udb_review_index_{}", Uuid::new_v4().simple());
            let expected_sql =
                crate::generation::sql::render_index_in_tx(&expected_table, &expected_index);
            sqlx::raw_sql(&expected_sql)
                .execute(&mut *tx)
                .await
                .map_err(|err| {
                    catalog_admin_internal_status("reviewed_expected_index_parse", err.to_string())
                })?;
            let shape_sql = "SELECT i.indisvalid AS valid,i.indisready AS ready,i.indislive AS live,
                i.indisunique AS unique_index,i.indimmediate AS immediate,am.amname::TEXT AS method,
                ARRAY(SELECT a.attname::TEXT FROM unnest(i.indkey::SMALLINT[]) WITH ORDINALITY k(attnum,pos)
                    JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
                    WHERE k.pos<=i.indnkeyatts ORDER BY k.pos) AS keys,
                ARRAY(SELECT a.attname::TEXT FROM unnest(i.indkey::SMALLINT[]) WITH ORDINALITY k(attnum,pos)
                    JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
                    WHERE k.pos>i.indnkeyatts ORDER BY k.pos) AS include_columns,
                COALESCE(pg_catalog.pg_get_expr(i.indpred,i.indrelid),'') AS predicate,
                i.indexprs IS NULL AS plain_keys,i.indoption::TEXT AS key_options,
                i.indclass::TEXT AS operator_classes,i.indcollation::TEXT AS collations,
                COALESCE(x.reloptions,ARRAY[]::TEXT[]) AS parameters,
                COALESCE((to_jsonb(i)->>'indnullsnotdistinct')::BOOLEAN,false) AS nulls_not_distinct
                FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class x ON x.oid=i.indexrelid
                JOIN pg_catalog.pg_am am ON am.oid=x.relam
                WHERE i.indrelid::BIGINT=$1 AND x.relname::TEXT=$2";
            let expected: ReviewedIndexShape = sqlx::query_as(shape_sql)
                .bind(temp_oid)
                .bind(&expected_index.name)
                .fetch_one(&mut *tx)
                .await
                .map_err(|err| {
                    catalog_admin_internal_status("reviewed_expected_index_shape", err.to_string())
                })?;
            let actual: Option<ReviewedIndexShape> = sqlx::query_as(shape_sql)
                .bind(live_oid)
                .bind(&live_name)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|err| {
                    catalog_admin_internal_status("reviewed_index_shape", err.to_string())
                })?;
            if actual.as_ref() != Some(&expected) {
                return Ok(false);
            }
        }
        tx.rollback().await.map_err(|err| {
            catalog_admin_internal_status("reviewed_expected_shape_rollback", err.to_string())
        })?;
        Ok(true)
    }

    pub(super) async fn record_verified_preapplied_catalog(
        &self,
        id: Uuid,
        plan: &ReviewedCatalogPlan,
        pool: &PgPool,
    ) -> Result<(), tonic::Status> {
        pool.execute(crate::control::tracker::DDL_SCHEMA_MIGRATIONS)
            .await
            .map_err(|err| {
                catalog_admin_internal_status("reviewed_target_ledger_bootstrap", err.to_string())
            })?;
        if !self.verified_reviewed_target(plan, pool).await? {
            return Ok(());
        }
        let mut tx = pool.begin().await.map_err(|err| {
            catalog_admin_internal_status("reviewed_native_verification_begin", err.to_string())
        })?;
        for artifact in native_artifacts(id, plan) {
            // This is an actual broker verification receipt, explicitly tagged
            // VERIFIED. It does not assert that the broker executed external DDL.
            sqlx::query("INSERT INTO public.schema_migrations(filename,checksum,state,migration_kind,proto_manifest_checksum,source_schema,source_table,operation_kind)
                VALUES($1,$2,'applied','reviewed_native_verification',$3,$4,$5,'verified_preapplied') ON CONFLICT(filename) DO NOTHING")
                .bind(&artifact.rel_path).bind(artifact_content_checksum(&artifact.content)).bind(&plan.target_schema_checksum_sha256)
                .bind(&artifact.schema).bind(&artifact.table).execute(&mut *tx).await.map_err(|err| catalog_admin_internal_status("reviewed_native_verification_store",err.to_string()))?;
        }
        tx.commit().await.map_err(|err| {
            catalog_admin_internal_status("reviewed_native_verification_commit", err.to_string())
        })?;
        Ok(())
    }

    pub(super) async fn complete_reviewed_catalog_application(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        id: Uuid,
        plan: &ReviewedCatalogPlan,
        pool: &PgPool,
        actor: &str,
    ) -> Result<(), tonic::Status> {
        require_reviewed_identity(&plan.tenant_id, actor)?;
        if !self.verified_reviewed_target(plan, pool).await? {
            return Err(refusal(
                "reviewed_target_verification_failed",
                "native applied target does not exactly verify",
            ));
        }
        sqlx::query(&format!(
            "UPDATE {} SET applied_by=$2 WHERE run_id=$1 AND applied_by=''",
            relation()
        ))
        .bind(id)
        .bind(actor)
        .execute(&mut **tx)
        .await
        .map_err(|err| {
            catalog_admin_internal_status("reviewed_application_actor", err.to_string())
        })?;
        let evidence = self
            .reviewed_application_evidence(tx, id, plan, pool)
            .await?;
        let rows = sqlx::query(&format!("UPDATE {} SET applied_operations_hash=$2,application_evidence_sha256=$3,applied_at=NOW() WHERE run_id=$1 AND approved_by<>'' AND approved_at IS NOT NULL",relation()))
            .bind(id).bind(&plan.operations_hash).bind(evidence).execute(&mut **tx).await.map_err(|err| catalog_admin_internal_status("reviewed_application_complete",err.to_string()))?;
        if rows.rows_affected() != 1 {
            return Err(refusal(
                "reviewed_approval_missing",
                "durable approved candidate disappeared before completion",
            ));
        }
        Ok(())
    }

    async fn reviewed_application_evidence(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        id: Uuid,
        plan: &ReviewedCatalogPlan,
        pool: &PgPool,
    ) -> Result<String, tonic::Status> {
        let config = SystemCatalogConfig::default();
        let phases:(i64,i64) = sqlx::query_as(&format!(
            "SELECT COUNT(*)::BIGINT,COUNT(*) FILTER(WHERE status='completed' AND phase IN ('prepare','backfill','validate','switch','cleanup'))::BIGINT FROM {} WHERE run_id=$1",
            migration_phase_ledger_relation(&config)))
            .bind(id.to_string()).fetch_one(&mut **tx).await.map_err(|err|
                catalog_admin_internal_status("reviewed_phase_evidence",err.to_string()))?;
        if phases != (5, 5) {
            return Err(refusal(
                "reviewed_application_incomplete",
                "all five native migration phases must have durable completed evidence",
            ));
        }
        let actor: String = sqlx::query_scalar(&format!(
            "SELECT applied_by FROM {} WHERE run_id=$1",
            relation()
        ))
        .bind(id)
        .fetch_one(&mut **tx)
        .await
        .map_err(|err| {
            catalog_admin_internal_status("reviewed_application_actor", err.to_string())
        })?;
        if actor.is_empty() {
            return Err(refusal(
                "reviewed_application_incomplete",
                "native application has no verified completing actor",
            ));
        }
        let rows:Vec<(i32,String,serde_json::Value)> = sqlx::query_as(&format!(
            "SELECT operation_index,status,payload_json FROM {} WHERE run_id=$1 ORDER BY operation_index",config.migration_op_ledger_relation()))
            .bind(id).fetch_all(&mut **tx).await.map_err(|err| catalog_admin_internal_status("reviewed_application_ledger_verify",err.to_string()))?;
        let artifacts = native_artifacts(id, plan);
        if rows.len() != artifacts.len() {
            return Err(refusal(
                "reviewed_application_incomplete",
                "native ledger is incomplete",
            ));
        }
        let mut proofs = Vec::new();
        for (index, ((position, status, payload), artifact)) in
            rows.iter().zip(&artifacts).enumerate()
        {
            if *position != index as i32
                || !matches!(status.as_str(), "APPLIED" | "VERIFIED" | "SKIPPED")
                || reviewed_sql_artifact(payload)? != *artifact
                || !self.reviewed_artifact_applied(pool, payload).await?
            {
                return Err(refusal(
                    "reviewed_application_incomplete",
                    "native target/application receipt is incomplete or mismatched",
                ));
            }
            proofs.push(
                serde_json::json!({"index":position,"status":status,"filename":artifact.rel_path,
                "checksum":artifact_content_checksum(&artifact.content)}),
            );
        }
        let raw = serde_json::to_vec(&proofs).map_err(|err| {
            catalog_admin_internal_status("reviewed_application_encode", err.to_string())
        })?;
        Ok(catalog_request_fingerprint(&[
            b"REVIEWED_NATIVE_APPLICATION_V1",
            id.as_bytes(),
            plan.operations_hash.as_bytes(),
            plan.target_manifest_integrity_sha256.as_bytes(),
            plan.target_provenance_sha256.as_bytes(),
            actor.as_bytes(),
            &raw,
        ]))
    }

    pub(super) async fn reviewed_catalog_compatibility_evidence(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        operation: &'static str,
        tenant: &str,
        project: &str,
        run_id: &str,
        staged: Option<Uuid>,
        level: &str,
        base_id: Option<Uuid>,
        base_integrity: &str,
        base_manifest: Option<&CatalogManifest>,
        target_version: &str,
        target_integrity: &str,
        target: &CatalogManifest,
        require_base: bool,
    ) -> Result<String, tonic::Status> {
        let id = parse_migration_run_id(run_id)?;
        let plan = self
            .load_reviewed_plan(tx, id)
            .await?
            .ok_or_else(|| refusal("reviewed_plan_missing", "reviewed candidate is absent"))?;
        if tenant.trim().is_empty() || plan.tenant_id != tenant || plan.project_id != project {
            return Err(refusal(
                "reviewed_scope_mismatch",
                "reviewed evidence belongs to a different tenant/project",
            ));
        }
        if level != "backward"
            || !catalog_transition_is_compatible(
                level,
                &plan.expected_active_catalog_version,
                target_version,
            )
            || target_integrity != plan.target_manifest_integrity_sha256
            || target != &plan.target_manifest
        {
            return Err(refusal(
                "reviewed_target_mismatch",
                "reviewed workflow requires backward mode and the exact immutable candidate",
            ));
        }
        if require_base {
            if base_id != Some(plan.expected_active_catalog_id)
                || base_integrity != plan.expected_active_manifest_integrity_sha256
            {
                return Err(refusal(
                    "reviewed_base_changed",
                    "ACTIVE catalog changed after candidate planning",
                ));
            }
            let base = base_manifest
                .ok_or_else(|| refusal("reviewed_base_changed", "exact catalog base is absent"))?;
            allowed_changes(base, target, &plan.changes)?;
            if crate::migration::plan::canonical_change_set(Some(base), target) != plan.changes {
                return Err(refusal(
                    "reviewed_operation_coverage_mismatch",
                    "reviewed canonical operation coverage changed",
                ));
            }
        }
        let row:(String,String,String,String,Option<Uuid>,String,bool) = sqlx::query_as(&format!(
            "SELECT r.state,t.approved_by,t.applied_operations_hash,t.application_evidence_sha256,t.staged_catalog_id,
                t.compatibility_evidence_sha256,t.approved_at IS NOT NULL AND t.applied_at IS NOT NULL AND r.finished_at IS NOT NULL
                AND r.operations_hash=$3 AND r.error=''
             FROM {} t JOIN {} r USING(run_id) WHERE t.run_id=$1 AND r.project_id=$2",relation(),SystemCatalogConfig::default().migration_runs_relation()))
            .bind(id).bind(project).bind(&plan.operations_hash).fetch_one(&mut **tx).await.map_err(|err| catalog_admin_internal_status("reviewed_evidence_load",err.to_string()))?;
        if row.0 != "COMPLETED"
            || row.1.is_empty()
            || row.2 != plan.operations_hash
            || row.3.is_empty()
            || !row.6
            || (staged.is_some() && row.4 != staged)
        {
            return Err(refusal(
                "reviewed_application_incomplete",
                "reviewed transition requires actual approved COMPLETED native application and exact staged binding",
            ));
        }
        let target_route = self
            .project_postgres_write_target(project, Some(&plan.target_instance))
            .await?;
        if target_route.provenance_sha256 != plan.target_provenance_sha256 {
            return Err(refusal(
                "reviewed_target_authority_changed",
                "routed native target authority changed",
            ));
        }
        if self
            .reviewed_application_evidence(tx, id, &plan, &target_route.pool)
            .await?
            != row.3
        {
            return Err(refusal(
                "reviewed_application_evidence_mismatch",
                "native application receipts no longer match durable completion evidence",
            ));
        }
        // Activation rechecks real target state; a formerly completed run is not
        // a license to publish after physical schema corruption or rollback.
        if operation != "catalog_provenance_upgrade"
            && !self
                .verified_reviewed_target(&plan, &target_route.pool)
                .await?
        {
            return Err(refusal(
                "reviewed_target_verification_failed",
                "reviewed target no longer verifies",
            ));
        }
        let evidence = catalog_request_fingerprint(&[
            b"REVIEWED_CATALOG_COMPATIBILITY_V1",
            tenant.as_bytes(),
            project.as_bytes(),
            id.as_bytes(),
            plan.expected_active_catalog_id.as_bytes(),
            plan.expected_active_manifest_integrity_sha256.as_bytes(),
            target_version.as_bytes(),
            target_integrity.as_bytes(),
            plan.operations_hash.as_bytes(),
            row.1.as_bytes(),
            row.3.as_bytes(),
        ]);
        if staged.is_some() && row.5 != evidence {
            return Err(refusal(
                "reviewed_compatibility_evidence_mismatch",
                "staged reviewed evidence differs from canonical approval/application",
            ));
        }
        Ok(evidence)
    }

    pub(super) async fn bind_reviewed_staged_catalog(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        tenant: &str,
        project: &str,
        run_id: &str,
        catalog_id: Uuid,
        evidence: &str,
    ) -> Result<(), tonic::Status> {
        let rows = sqlx::query(&format!(
            "UPDATE {} SET staged_catalog_id=$4,compatibility_evidence_sha256=$5
            WHERE run_id=$1 AND tenant_id=$2 AND project_id=$3 AND staged_catalog_id IS NULL",
            relation()
        ))
        .bind(parse_migration_run_id(run_id)?)
        .bind(tenant)
        .bind(project)
        .bind(catalog_id)
        .bind(evidence)
        .execute(&mut **tx)
        .await
        .map_err(|err| catalog_admin_internal_status("reviewed_stage_binding", err.to_string()))?;
        if rows.rows_affected() != 1 {
            return Err(refusal(
                "reviewed_stage_binding_conflict",
                "candidate already has a different staged catalog binding",
            ));
        }
        Ok(())
    }

    pub(super) async fn validate_reviewed_catalog_replay(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        tenant: &str,
        project: &str,
        run: &str,
        catalog_id: Uuid,
        operation: &'static str,
    ) -> Result<(), tonic::Status> {
        let target = self
            .load_catalog_record_for_project(project, catalog_id)
            .await?;
        let active = self
            .load_active_catalog_for_project(project)
            .await?
            .ok_or_else(|| {
                refusal(
                    "reviewed_base_changed",
                    "reviewed replay requires a proven ACTIVE catalog",
                )
            })?;
        if operation == "activate_catalog" && active.catalog_id != catalog_id.to_string() {
            return Err(refusal(
                "reviewed_base_changed",
                "replayed activation no longer identifies the project ACTIVE catalog",
            ));
        }
        self.reviewed_catalog_compatibility_evidence(
            tx,
            operation,
            tenant,
            project,
            run,
            Some(catalog_id),
            &target.compatibility_level,
            Some(
                active.catalog_id.parse().map_err(|_| {
                    refusal("reviewed_base_changed", "ACTIVE catalog UUID is invalid")
                })?,
            ),
            &active.manifest_integrity_sha256,
            Some(&active.manifest),
            &target.version,
            &target.manifest_integrity_sha256,
            &target.manifest,
            active.catalog_id != catalog_id.to_string(),
        )
        .await?;
        Ok(())
    }

    pub(super) async fn reviewed_catalog_binding(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        id: Uuid,
    ) -> Result<Option<(String, String)>, tonic::Status> {
        sqlx::query_as(&format!(
            "SELECT tenant_id,run_id::TEXT FROM {} WHERE staged_catalog_id=$1",
            relation()
        ))
        .bind(id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|err| catalog_admin_internal_status("reviewed_catalog_binding", err.to_string()))
    }

    pub(super) async fn require_ordinary_catalog_transition(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        id: Uuid,
    ) -> Result<(), tonic::Status> {
        let present: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(relation())
            .fetch_one(&mut **tx)
            .await
            .map_err(|err| {
                catalog_admin_internal_status("reviewed_storage_lookup", err.to_string())
            })?;
        if present && self.reviewed_catalog_binding(tx, id).await?.is_some() {
            return Err(refusal(
                "reviewed_transition_reference_required",
                "this catalog requires its explicit reviewed migration reference",
            ));
        }
        Ok(())
    }

    pub async fn get_migration_status_for_caller(
        &self,
        project_id: &str,
        run_id: &str,
        tenant_id: &str,
    ) -> Result<serde_json::Value, tonic::Status> {
        let status = self.get_migration_status(project_id, run_id).await?;
        if let Some(evidence) = status.get("reviewed_catalog_transition") {
            if tenant_id.trim().is_empty()
                || evidence.get("tenant_id").and_then(|v| v.as_str()) != Some(tenant_id)
            {
                return Err(refusal(
                    "reviewed_scope_mismatch",
                    "reviewed run belongs to another tenant",
                ));
            }
        }
        Ok(status)
    }

    pub(super) async fn reviewed_catalog_status(
        &self,
        project: &str,
        id: Uuid,
    ) -> Result<Option<serde_json::Value>, tonic::Status> {
        if !self.reviewed_catalog_plan_exists(id).await? {
            return Ok(None);
        }
        let mut tx = self.pg_pool()?.begin().await.map_err(|err| {
            catalog_admin_internal_status("reviewed_status_begin", err.to_string())
        })?;
        let plan = self
            .load_reviewed_plan(&mut tx, id)
            .await?
            .ok_or_else(|| refusal("reviewed_plan_missing", "candidate absent"))?;
        if plan.project_id != project {
            return Err(refusal(
                "reviewed_scope_mismatch",
                "candidate belongs to another project",
            ));
        }
        let row:(String,i64,String,String,String,i64) = sqlx::query_as(&format!(
            "SELECT t.approved_by,COALESCE(EXTRACT(EPOCH FROM t.approved_at)::BIGINT,0),r.state,
                t.applied_operations_hash,t.application_evidence_sha256,COALESCE(EXTRACT(EPOCH FROM t.applied_at)::BIGINT,0)
             FROM {} t JOIN {} r USING(run_id) WHERE t.run_id=$1",relation(),SystemCatalogConfig::default().migration_runs_relation()))
            .bind(id).fetch_one(&mut *tx).await.map_err(|err| catalog_admin_internal_status("reviewed_status_load",err.to_string()))?;
        Ok(Some(
            serde_json::json!({"run_id":id.to_string(),"tenant_id":plan.tenant_id,"project_id":project,
            "expected_active_catalog_id":plan.expected_active_catalog_id.to_string(),
            "expected_active_manifest_integrity_sha256":plan.expected_active_manifest_integrity_sha256,
            "target_manifest_integrity_sha256":plan.target_manifest_integrity_sha256,
            "target_schema_checksum_sha256":plan.target_schema_checksum_sha256,"operations_hash":plan.operations_hash,
            "reviewed_operation_fingerprints":plan.reviewed_operation_fingerprints,"approved_by":row.0,"approved_at_unix":row.1,
            "application_state":row.2,"applied_operations_hash":row.3,"application_evidence_sha256":row.4,"applied_at_unix":row.5}),
        ))
    }
}
