//! Continuation `impl DataBrokerRuntime` block (Phase F split of core.rs).
use super::*;

enum MigrationApplyOutcome {
    Applied,
    Verified,
}

fn migration_approval_tokens_match(provided: &str, stored: &str) -> bool {
    !provided.trim().is_empty()
        && !stored.trim().is_empty()
        && crate::runtime::authn::constant_time_eq(provided.trim(), stored.trim())
}

fn catalog_admin_invalid_field(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [(field.into(), description.into())],
    )
}

fn catalog_admin_policy_status(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::policy_status(operation, policy_decision_id, message)
}

fn catalog_admin_policy_not_found_status(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::NotFound,
        operation,
        policy_decision_id,
        message,
    )
}

fn catalog_admin_schema_status(
    operation: &'static str,
    schema_code: &'static str,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::FailedPrecondition,
        "catalog",
        operation,
        schema_code,
        message,
    )
}

fn catalog_admin_not_found_status(
    operation: &'static str,
    schema_code: &'static str,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "catalog",
        operation,
        schema_code,
        message,
    )
}

fn migration_run_not_found_status(operation: &'static str) -> tonic::Status {
    catalog_admin_not_found_status(
        operation,
        "migration_run_not_found",
        "migration run not found",
    )
}

fn dlq_event_not_found_status(
    operation: &'static str,
    message: impl Into<String>,
) -> tonic::Status {
    catalog_admin_not_found_status(operation, "dlq_event_not_found", message)
}

fn dlq_event_not_found_or_not_replayable_status() -> tonic::Status {
    catalog_admin_not_found_status(
        "replay_dlq_event",
        "dlq_event_not_found_or_not_replayable",
        "DLQ event not found or not replayable",
    )
}

fn catalog_admin_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::capability_status(
        "catalog_admin",
        operation,
        capability_required,
        message,
    )
}

fn catalog_admin_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status("catalog_admin", operation, message)
}

fn migration_approval_token_required_status() -> tonic::Status {
    catalog_admin_policy_status(
        "apply_migration",
        "approval_token_required",
        "approval_token is required",
    )
}

fn migration_apply_state_not_approved_status() -> tonic::Status {
    catalog_admin_policy_status(
        "apply_migration",
        "migration_run_not_approved",
        "migration run must be approved before apply and not be terminal",
    )
}

fn migration_apply_token_mismatch_status() -> tonic::Status {
    catalog_admin_policy_status(
        "apply_migration",
        "approval_token_mismatch",
        "approval_token is missing or does not match the approved token",
    )
}

fn migration_approve_not_preflight_status() -> tonic::Status {
    catalog_admin_policy_status(
        "approve_migration_plan",
        "migration_run_not_preflight",
        "migration run not found, not in PREFLIGHT, or already approved",
    )
}

fn migration_apply_preflight_status(errors: &[String]) -> tonic::Status {
    catalog_admin_schema_status(
        "apply_migration",
        "migration_apply_preflight_failed",
        format!("apply_migration preflight failed: {}", errors.join("; ")),
    )
}

fn migration_apply_state_changed_status() -> tonic::Status {
    catalog_admin_policy_status(
        "apply_migration",
        "migration_approval_state_changed",
        "migration run approval state changed before apply",
    )
}

fn migration_apply_phase_refused_status(error: impl Into<String>) -> tonic::Status {
    catalog_admin_policy_status("apply_migration", "migration_phase_refused", error)
}

fn dlq_missing_topic_status() -> tonic::Status {
    catalog_admin_schema_status(
        "replay_dlq_event",
        "dlq_event_missing_topic",
        "DLQ event has no topic and cannot be replayed",
    )
}

fn system_store_unregistered_status() -> tonic::Status {
    catalog_admin_capability_status(
        "system_store",
        "canonical_system_store",
        "no canonical store is registered; saga/audit admin requires a provisioned \
         Postgres / MySQL / SQLite system store (udb_system)",
    )
}

fn parse_catalog_manifest_json(manifest_json: &[u8]) -> Result<serde_json::Value, tonic::Status> {
    let manifest_json_str = std::str::from_utf8(manifest_json).map_err(|_| {
        catalog_admin_invalid_field(
            "manifest_json",
            "must be valid UTF-8",
            "manifest_json is not valid UTF-8",
        )
    })?;
    serde_json::from_str(manifest_json_str).map_err(|err| {
        catalog_admin_invalid_field(
            "manifest_json",
            "must be valid JSON",
            format!("manifest_json parse error: {err}"),
        )
    })
}

fn validate_approval_token_for_plan(approval_token: &str) -> Result<(), tonic::Status> {
    if approval_token.trim().is_empty() {
        return Err(catalog_admin_invalid_field(
            "approval_token",
            "must be non-empty",
            "approval_token must not be empty",
        ));
    }
    Ok(())
}

fn parse_migration_run_id(run_id: &str) -> Result<Uuid, tonic::Status> {
    run_id.parse().map_err(|_| {
        catalog_admin_invalid_field("run_id", "must be a UUID", "run_id must be a UUID")
    })
}

const ALLOWED_DLQ_STATUSES: &[&str] = &["OPEN", "REPLAYED", "DISMISSED", "QUARANTINED", "RETRYING"];

fn parse_dlq_status_filter(status_filter: &str) -> Result<String, tonic::Status> {
    let status_filter = status_filter.trim().to_ascii_uppercase();
    if !status_filter.is_empty() && !ALLOWED_DLQ_STATUSES.contains(&status_filter.as_str()) {
        return Err(catalog_admin_invalid_field(
            "status_filter",
            format!("must be one of {}", ALLOWED_DLQ_STATUSES.join(", ")),
            format!(
                "invalid status_filter '{status_filter}'; must be one of {:?}",
                ALLOWED_DLQ_STATUSES
            ),
        ));
    }
    Ok(status_filter)
}

fn parse_dlq_id(dlq_id: &str) -> Result<Uuid, tonic::Status> {
    dlq_id.parse().map_err(|_| {
        catalog_admin_invalid_field("dlq_id", "must be a UUID", "dlq_id must be a UUID")
    })
}

/// Fast-fail guard shared by `apply_migration` (before any ledger access) and
/// [`validate_migration_apply_state_and_token`]: an empty approval token can
/// never apply a migration.
fn require_migration_approval_token(approval_token: &str) -> Result<(), tonic::Status> {
    if approval_token.trim().is_empty() {
        return Err(migration_approval_token_required_status());
    }
    Ok(())
}

/// Pure state+token validation gate for `DataBrokerRuntime::apply_migration`:
/// the run must be in a non-terminal approved state (`APPROVED`, or
/// `APPLYING`/`VERIFYING` when resuming a partially-applied run) and the
/// caller-provided approval token must match the stored token
/// (constant-time). Extracted from `apply_migration` so its reject paths are
/// unit-testable without a live PostgreSQL ledger; `apply_migration` is the
/// only serving-path caller.
fn validate_migration_apply_state_and_token(
    state: &str,
    provided_token: &str,
    stored_token: &str,
) -> Result<(), tonic::Status> {
    require_migration_approval_token(provided_token)?;
    if !matches!(state, "APPROVED" | "APPLYING" | "VERIFYING") {
        return Err(migration_apply_state_not_approved_status());
    }
    if !migration_approval_tokens_match(provided_token, stored_token) {
        return Err(migration_apply_token_mismatch_status());
    }
    Ok(())
}

fn migration_payload_has_executable_sql(content: &str) -> bool {
    content.lines().any(|line| {
        let trimmed = line.trim_start();
        !trimmed.is_empty() && !trimmed.starts_with("--")
    })
}

fn migration_resource_name(payload: &serde_json::Value) -> String {
    payload
        .get("resource_name")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn migration_resource_spec_json(payload: &serde_json::Value) -> String {
    payload
        .get("spec")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}))
        .to_string()
}

async fn ensure_migration_payload_json_column(
    pool: &sqlx::PgPool,
    ledger_rel: &str,
) -> Result<(), tonic::Status> {
    let alter = format!(
        "ALTER TABLE {ledger_rel}
         ADD COLUMN IF NOT EXISTS payload_json JSONB NOT NULL DEFAULT '{{}}'::JSONB"
    );
    sqlx::query(&alter).execute(pool).await.map_err(|err| {
        catalog_admin_internal_status(
            "migration_audit_payload_json_upgrade",
            format!("migration audit payload_json upgrade failed: {err}"),
        )
    })?;

    // One-time backfill of the legacy `rollback_json` column into the renamed
    // `payload_json`. Only ledgers created by an OLD schema have `rollback_json`;
    // a table created by the current DDL never does, so an unconditional UPDATE
    // referencing it fails with `column "rollback_json" does not exist` and
    // aborts PlanMigration (which in turn blocks Apply/Status/Approve). Guard the
    // UPDATE on the column's existence (pg_attribute) so it is a no-op when there
    // is nothing to migrate. Mirrors the canonical-store backfill in
    // postgres_migration_audit.rs. bug_report.md A5.
    let backfill = format!(
        "DO $$
         BEGIN
           IF EXISTS (
             SELECT 1 FROM pg_attribute
             WHERE attrelid = to_regclass('{ledger_rel}')
               AND attname = 'rollback_json'
               AND NOT attisdropped
           ) THEN
             UPDATE {ledger_rel}
             SET payload_json = rollback_json
             WHERE payload_json = '{{}}'::JSONB
               AND rollback_json IS NOT NULL
               AND rollback_json <> '{{}}'::JSONB;
           END IF;
         END $$;"
    );
    sqlx::query(&backfill).execute(pool).await.map_err(|err| {
        catalog_admin_internal_status(
            "migration_audit_payload_json_backfill",
            format!("migration audit payload_json backfill failed: {err}"),
        )
    })?;
    Ok(())
}

async fn ensure_migration_runs_approved_state(
    pool: &sqlx::PgPool,
    runs_rel: &str,
    runs_table: &str,
) -> Result<(), tonic::Status> {
    let auto_constraint = qi_runtime(&format!("{runs_table}_state_check"));
    let named_constraint = qi_runtime(&format!("chk_{runs_table}_state"));
    for ddl in [
        format!("ALTER TABLE {runs_rel} DROP CONSTRAINT IF EXISTS {auto_constraint}"),
        format!("ALTER TABLE {runs_rel} DROP CONSTRAINT IF EXISTS {named_constraint}"),
        format!(
            "ALTER TABLE {runs_rel} ADD CONSTRAINT {named_constraint}
             CHECK (state IN ('DRY_RUN','PREFLIGHT','APPROVED','APPLYING','VERIFYING','COMPLETED','ERROR','DEAD_LETTER'))"
        ),
    ] {
        sqlx::query(&ddl).execute(pool).await.map_err(|err| {
            catalog_admin_internal_status(
                "migration_audit_approved_state_upgrade",
                format!("migration audit approved-state upgrade failed: {err}"),
            )
        })?;
    }
    Ok(())
}

fn postgres_create_table_payload(
    table: &ManifestTable,
    manifest_checksum: &str,
) -> serde_json::Value {
    let content = crate::generation::sql::render_bootstrap_table(
        table,
        manifest_checksum,
        &crate::generation::SqlGenerationConfig::default(),
    );
    serde_json::json!({
        "action": "create_table",
        "schema": table.schema,
        "table": table.table,
        "checksum": table.checksum_sha256,
        "content": content,
    })
}

fn migration_phase_ledger_relation(config: &crate::runtime::system::SystemCatalogConfig) -> String {
    format!(
        "{}.{}",
        qi_runtime(&config.cdc.system_schema),
        qi_runtime("udb_migration_phase_ledger")
    )
}

fn migration_artifact_payload(
    artifact: &GeneratedArtifact,
) -> Result<(i64, String, String, String, serde_json::Value), crate::migration::ApplyError> {
    let wrapper: serde_json::Value = serde_json::from_str(&artifact.content).map_err(|err| {
        crate::migration::ApplyError::BackendRejected {
            backend: "catalog_admin".to_string(),
            message: format!("invalid migration artifact payload: {err}"),
        }
    })?;
    let op_id = wrapper
        .get("op_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| crate::migration::ApplyError::BackendRejected {
            backend: "catalog_admin".to_string(),
            message: "migration artifact missing op_id".to_string(),
        })?;
    let backend = wrapper
        .get("backend")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let resource_uri = wrapper
        .get("resource_uri")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let operation_kind = wrapper
        .get("operation_kind")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let payload = wrapper
        .get("payload")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    Ok((op_id, backend, resource_uri, operation_kind, payload))
}

struct CatalogMigrationApplyTarget<'a> {
    runtime: &'a DataBrokerRuntime,
    pool: &'a PgPool,
}

impl crate::migration::ApplyTarget for CatalogMigrationApplyTarget<'_> {
    fn backend_name(&self) -> &'static str {
        "catalog_admin"
    }

    fn apply_artifact<'a>(
        &'a self,
        artifact: &'a GeneratedArtifact,
    ) -> crate::migration::ApplyFuture<'a, ()> {
        Box::pin(async move {
            let (op_id, backend, resource_uri, operation_kind, payload) =
                migration_artifact_payload(artifact)?;
            self.runtime
                .execute_migration_apply_op(
                    self.pool,
                    op_id,
                    &backend,
                    &resource_uri,
                    &operation_kind,
                    &payload,
                )
                .await
                .map(|_| ())
                .map_err(|message| crate::migration::ApplyError::BackendRejected {
                    backend,
                    message,
                })
        })
    }

    fn verify_applied<'a>(
        &'a self,
        artifact: &'a GeneratedArtifact,
    ) -> crate::migration::ApplyFuture<'a, bool> {
        Box::pin(async move {
            let (_op_id, backend, _resource_uri, operation_kind, payload) =
                migration_artifact_payload(artifact)?;
            match (backend.as_str(), operation_kind.as_str()) {
                ("postgres", "create_table" | "verify_table") => {
                    let schema = payload
                        .get("schema")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    let table = payload
                        .get("table")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    let exists: bool = sqlx::query_scalar(
                        "SELECT EXISTS (
                             SELECT 1 FROM information_schema.tables
                             WHERE table_schema = $1 AND table_name = $2
                         )",
                    )
                    .bind(schema)
                    .bind(table)
                    .fetch_one(self.pool)
                    .await
                    .map_err(|err| crate::migration::ApplyError::Unreachable(err.to_string()))?;
                    Ok(exists)
                }
                (_, "verify_resource" | "ensure_resource") => {
                    let resource_name = migration_resource_name(&payload);
                    let resources = self
                        .runtime
                        .list_resources_backend(&backend)
                        .await
                        .map_err(|err| {
                            crate::migration::ApplyError::Unreachable(err.to_string())
                        })?;
                    Ok(resources.iter().any(|value| value == &resource_name))
                }
                (_, "drop_resource") => {
                    let resource_name = migration_resource_name(&payload);
                    let resources = self
                        .runtime
                        .list_resources_backend(&backend)
                        .await
                        .map_err(|err| {
                            crate::migration::ApplyError::Unreachable(err.to_string())
                        })?;
                    Ok(!resources.iter().any(|value| value == &resource_name))
                }
                ("postgres", "apply_sql") => Ok(false),
                _ => Err(crate::migration::ApplyError::BackendRejected {
                    backend,
                    message: format!("unsupported migration operation '{operation_kind}'"),
                }),
            }
        })
    }
}

struct ExistingRunMigrationAuditSink<'a> {
    pool: &'a PgPool,
    runs_rel: String,
    ledger_rel: String,
    run_id: Uuid,
    op_ids: Vec<i64>,
    op_kinds: Vec<String>,
}

impl crate::migration::MigrationAuditSink for ExistingRunMigrationAuditSink<'_> {
    fn start_run<'a>(
        &'a self,
        _catalog_version: &'a str,
        _operations_hash: &'a str,
    ) -> crate::migration::ApplyFuture<'a, String> {
        Box::pin(async move { Ok(self.run_id.to_string()) })
    }

    fn record_op<'a>(
        &'a self,
        _run_id: &'a str,
        index: usize,
        result: &'a crate::migration::ArtifactApplyResult,
    ) -> crate::migration::ApplyFuture<'a, ()> {
        Box::pin(async move {
            let op_id = self.op_ids.get(index).copied().ok_or_else(|| {
                crate::migration::ApplyError::Io(format!(
                    "artifact result index {index} has no matching planned op"
                ))
            })?;
            let op_kind = self.op_kinds.get(index).map(String::as_str).unwrap_or("");
            let status = if result.error.is_some() {
                "FAILED"
            } else if result.skipped && op_kind.starts_with("verify") {
                "VERIFIED"
            } else if result.skipped {
                "SKIPPED"
            } else if op_kind.starts_with("verify") {
                "VERIFIED"
            } else {
                "APPLIED"
            };
            let error = result.error.clone().unwrap_or_default();
            sqlx::query(&format!(
                "UPDATE {}
                 SET status = $1,
                     error = $2,
                     applied_at = CASE WHEN $1 = 'APPLIED' THEN NOW() ELSE applied_at END
                 WHERE id = $3",
                self.ledger_rel
            ))
            .bind(status)
            .bind(error)
            .bind(op_id)
            .execute(self.pool)
            .await
            .map_err(|err| crate::migration::ApplyError::Io(err.to_string()))?;
            Ok(())
        })
    }

    fn finish_run<'a>(
        &'a self,
        _run_id: &'a str,
        state: &'a str,
        error: &'a str,
    ) -> crate::migration::ApplyFuture<'a, ()> {
        Box::pin(async move {
            let (next_state, finished) = if state == "COMPLETED" {
                ("VERIFYING", false)
            } else {
                ("ERROR", true)
            };
            let sql = if finished {
                format!(
                    "UPDATE {}
                     SET state = $1, error = $2, finished_at = NOW()
                     WHERE run_id = $3",
                    self.runs_rel
                )
            } else {
                format!(
                    "UPDATE {}
                     SET state = $1, error = $2
                     WHERE run_id = $3",
                    self.runs_rel
                )
            };
            sqlx::query(&sql)
                .bind(next_state)
                .bind(error)
                .bind(self.run_id)
                .execute(self.pool)
                .await
                .map_err(|err| crate::migration::ApplyError::Io(err.to_string()))?;
            Ok(())
        })
    }
}

impl DataBrokerRuntime {
    fn abac_policy_relation(&self) -> String {
        let schema = if self.config.abac_schema.trim().is_empty() {
            "udb_system"
        } else {
            self.config.abac_schema.as_str()
        };
        let table = if self.config.abac_table.trim().is_empty() {
            "udb_abac_policies"
        } else {
            self.config.abac_table.as_str()
        };
        format!("{}.{}", qi_runtime(schema), qi_runtime(table))
    }

    fn preflight_migration_apply_ops(
        &self,
        ops: &[(i64, String, String, String, serde_json::Value)],
    ) -> Vec<String> {
        ops.iter()
            .filter_map(|(op_id, backend, resource_uri, operation_kind, payload)| {
                self.validate_migration_apply_op(
                    *op_id,
                    backend,
                    resource_uri,
                    operation_kind,
                    payload,
                )
                .err()
            })
            .collect()
    }

    fn validate_migration_apply_op(
        &self,
        op_id: i64,
        backend: &str,
        resource_uri: &str,
        operation_kind: &str,
        payload: &serde_json::Value,
    ) -> Result<(), String> {
        match (backend, operation_kind) {
            ("postgres", "apply_sql" | "create_table") => {
                let content = payload
                    .get("content")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                if !migration_payload_has_executable_sql(content) {
                    Err(format!(
                        "operation {op_id} for {resource_uri} has no executable SQL"
                    ))
                } else {
                    Ok(())
                }
            }
            ("postgres", "verify_table") => Ok(()),
            (_, "ensure_resource" | "verify_resource" | "drop_resource") => {
                if crate::backend::plugin_for(backend).is_none() {
                    return Err(format!(
                        "operation {op_id} targets backend '{backend}' which is not available in this binary"
                    ));
                }
                let resource_name = payload
                    .get("resource_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .trim();
                if resource_name.is_empty() {
                    return Err(format!(
                        "operation {op_id} for backend '{backend}' has no resource_name"
                    ));
                }
                Ok(())
            }
            _ => Err(format!(
                "operation {op_id} for backend '{backend}' kind '{operation_kind}' is not supported by the migration apply registry"
            )),
        }
    }

    async fn execute_migration_apply_op(
        &self,
        pool: &PgPool,
        op_id: i64,
        backend: &str,
        resource_uri: &str,
        operation_kind: &str,
        payload: &serde_json::Value,
    ) -> Result<MigrationApplyOutcome, String> {
        self.validate_migration_apply_op(op_id, backend, resource_uri, operation_kind, payload)?;
        match (backend, operation_kind) {
            ("postgres", "apply_sql" | "create_table") => {
                let content = payload
                    .get("content")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                sqlx::query(content)
                    .execute(pool)
                    .await
                    .map(|_| MigrationApplyOutcome::Applied)
                    .map_err(|err| format!("SQL execution failed for {resource_uri}: {err}"))
            }
            ("postgres", "verify_table") => {
                let schema = payload
                    .get("schema")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let table = payload
                    .get("table")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let exists: bool = sqlx::query_scalar(
                    "SELECT EXISTS (
                         SELECT 1 FROM information_schema.tables
                         WHERE table_schema = $1 AND table_name = $2
                     )",
                )
                .bind(schema)
                .bind(table)
                .fetch_one(pool)
                .await
                .map_err(|err| {
                    format!("table verification query failed for {resource_uri}: {err}")
                })?;
                if exists {
                    Ok(MigrationApplyOutcome::Verified)
                } else {
                    Err(format!("table {schema}.{table} does not exist"))
                }
            }
            (_, "ensure_resource") => {
                let resource_name = migration_resource_name(payload);
                let spec_json = migration_resource_spec_json(payload);
                self.ensure_resource_backend(backend, &resource_name, &spec_json)
                    .await
                    .map(|_| MigrationApplyOutcome::Applied)
                    .map_err(|err| err.to_string())
            }
            (_, "verify_resource") => {
                let resource_name = migration_resource_name(payload);
                let resources = self
                    .list_resources_backend(backend)
                    .await
                    .map_err(|err| err.to_string())?;
                if resources.iter().any(|value| value == &resource_name) {
                    Ok(MigrationApplyOutcome::Verified)
                } else {
                    Err(format!(
                        "resource '{resource_name}' was not found on backend '{backend}'"
                    ))
                }
            }
            (_, "drop_resource") => {
                let resource_name = migration_resource_name(payload);
                self.drop_resource_backend(backend, &resource_name)
                    .await
                    .map(|_| MigrationApplyOutcome::Applied)
                    .map_err(|err| err.to_string())
            }
            _ => unreachable!("migration apply op was preflighted"),
        }
    }

    /// Stage a new catalog version.  Returns the assigned catalog_id.
    pub async fn stage_catalog(
        &self,
        project_id: &str,
        version: &str,
        manifest_json: &[u8],
        reason: &str,
    ) -> Result<String, tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let manifest_value = parse_catalog_manifest_json(manifest_json)?;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        use sha2::{Digest, Sha256};
        let checksum = format!("{:x}", Sha256::digest(manifest_json));
        let cat_rel = config.catalog_versions_relation();
        let reload_rel = config.catalog_reload_log_relation();
        let catalog_id: Uuid = sqlx::query_scalar(&format!(
            "INSERT INTO {cat_rel}
                 (project_id, version, checksum_sha256, manifest_json, status)
             VALUES ($1, $2, $3, $4::JSONB, 'STAGED')
             RETURNING catalog_id"
        ))
        .bind(project_id)
        .bind(version)
        .bind(&checksum)
        .bind(manifest_value.to_string())
        .fetch_one(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status("stage_catalog", format!("stage_catalog failed: {err}"))
        })?;
        let _ = sqlx::query(&format!(
            "INSERT INTO {reload_rel}
                 (project_id, catalog_id, version, checksum_sha256, action, message)
             VALUES ($1, $2, $3, $4, 'STAGE', $5)"
        ))
        .bind(project_id)
        .bind(catalog_id)
        .bind(version)
        .bind(&checksum)
        .bind(reason)
        .execute(pool)
        .await;
        Ok(catalog_id.to_string())
    }

    /// Activate a catalog version (marks it ACTIVE, records activation log).
    pub async fn activate_catalog(
        &self,
        project_id: &str,
        catalog_id: &str,
        reason: &str,
        actor: &str,
    ) -> Result<(), tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let cat_rel = config.catalog_versions_relation();
        let log_rel = config.catalog_activation_log_relation();
        let binding_rel = config.project_catalog_bindings_relation();
        let reload_rel = config.catalog_reload_log_relation();
        let id: Uuid = if catalog_id.trim().is_empty() {
            sqlx::query_scalar(&format!(
                "SELECT catalog_id FROM {cat_rel}
                 WHERE project_id = $1 AND status = 'STAGED'
                 ORDER BY created_at DESC LIMIT 1"
            ))
            .bind(project_id)
            .fetch_optional(pool)
            .await
            .map_err(|err| {
                catalog_admin_internal_status(
                    "activate_catalog_lookup",
                    format!("activate_catalog lookup failed: {err}"),
                )
            })?
            .ok_or_else(|| {
                catalog_admin_not_found_status(
                    "activate_catalog",
                    "staged_catalog_not_found",
                    "no staged catalog found for project",
                )
            })?
        } else if let Ok(id) = catalog_id.parse::<Uuid>() {
            id
        } else {
            sqlx::query_scalar(&format!(
                "SELECT catalog_id FROM {cat_rel}
                 WHERE project_id = $1 AND version = $2 AND status = 'STAGED'
                 ORDER BY created_at DESC LIMIT 1"
            ))
            .bind(project_id)
            .bind(catalog_id)
            .fetch_optional(pool)
            .await
            .map_err(|err| {
                catalog_admin_internal_status(
                    "activate_catalog_lookup",
                    format!("activate_catalog lookup failed: {err}"),
                )
            })?
            .ok_or_else(|| {
                catalog_admin_not_found_status(
                    "activate_catalog",
                    "staged_catalog_version_not_found",
                    "staged catalog version not found",
                )
            })?
        };
        // Activation mutates five tables; run them in a single transaction so a
        // mid-sequence failure cannot leave the catalog with no ACTIVE row (or
        // two). The transaction is dropped (rolled back) on any early return.
        let mut tx = pool.begin().await.map_err(|err| {
            catalog_admin_internal_status(
                "activate_catalog_begin",
                format!("activate_catalog begin failed: {err}"),
            )
        })?;
        // Find current active version for this project
        let from_version: Option<String> = sqlx::query_scalar(&format!(
            "SELECT version FROM {cat_rel}
             WHERE project_id = $1 AND status = 'ACTIVE'
             ORDER BY activated_at DESC LIMIT 1"
        ))
        .bind(project_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "activate_catalog_lookup",
                format!("activate_catalog lookup failed: {err}"),
            )
        })?;
        // Deactivate old active
        sqlx::query(&format!(
            "UPDATE {cat_rel} SET status = 'ROLLED_BACK'
             WHERE project_id = $1 AND status = 'ACTIVE'"
        ))
        .bind(project_id)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "activate_catalog_deactivate",
                format!("activate_catalog deactivate failed: {err}"),
            )
        })?;
        // Activate new
        let rows = sqlx::query(&format!(
            "UPDATE {cat_rel}
             SET status = 'ACTIVE', activated_at = NOW()
             WHERE catalog_id = $1 AND project_id = $2 AND status = 'STAGED'"
        ))
        .bind(id)
        .bind(project_id)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "activate_catalog_update",
                format!("activate_catalog update failed: {err}"),
            )
        })?;
        if rows.rows_affected() == 0 {
            return Err(catalog_admin_not_found_status(
                "activate_catalog",
                "staged_catalog_not_found",
                "catalog not found or not in STAGED status",
            ));
        }
        // Log activation
        let to_version_row: (String, String) = sqlx::query_as(&format!(
            "SELECT version, checksum_sha256 FROM {cat_rel} WHERE catalog_id = $1"
        ))
        .bind(id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "activate_catalog_version_fetch",
                format!("activate_catalog version fetch failed: {err}"),
            )
        })?;
        sqlx::query(&format!(
            "INSERT INTO {log_rel}
                 (project_id, from_version, to_version, actor, reason)
             VALUES ($1, $2, $3, $4, $5)"
        ))
        .bind(project_id)
        .bind(from_version.unwrap_or_default())
        .bind(&to_version_row.0)
        .bind(actor)
        .bind(reason)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "activate_catalog_log",
                format!("activate_catalog log failed: {err}"),
            )
        })?;
        sqlx::query(&format!(
            "INSERT INTO {binding_rel}
                 (project_id, active_catalog_id, active_version, active_checksum_sha256, compatibility_level, updated_at)
             VALUES ($1, $2, $3, $4, 'backward', NOW())
             ON CONFLICT (project_id) DO UPDATE
                SET active_catalog_id = EXCLUDED.active_catalog_id,
                    active_version = EXCLUDED.active_version,
                    active_checksum_sha256 = EXCLUDED.active_checksum_sha256,
                    updated_at = NOW()"
        ))
        .bind(project_id)
        .bind(id)
        .bind(&to_version_row.0)
        .bind(&to_version_row.1)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "activate_catalog_project_binding",
                format!("activate_catalog project binding failed: {err}"),
            )
        })?;
        sqlx::query(&format!(
            "INSERT INTO {reload_rel}
                 (project_id, catalog_id, version, checksum_sha256, action, result, message)
             VALUES ($1, $2, $3, $4, 'ACTIVATE', 'ok', $5)"
        ))
        .bind(project_id)
        .bind(id)
        .bind(&to_version_row.0)
        .bind(&to_version_row.1)
        .bind(reason)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "activate_catalog_reload_log",
                format!("activate_catalog reload log failed: {err}"),
            )
        })?;
        tx.commit().await.map_err(|err| {
            catalog_admin_internal_status(
                "activate_catalog_commit",
                format!("activate_catalog commit failed: {err}"),
            )
        })?;
        Ok(())
    }

    /// Rollback the active catalog version.
    pub async fn rollback_catalog(
        &self,
        project_id: &str,
        target_catalog_id: &str,
        reason: &str,
        actor: &str,
    ) -> Result<(), tonic::Status> {
        self.activate_catalog(project_id, target_catalog_id, reason, actor)
            .await
    }

    /// Return all catalog versions for a project.
    pub async fn get_catalog_versions(
        &self,
        project_id: &str,
    ) -> Result<Vec<serde_json::Value>, tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let cat_rel = config.catalog_versions_relation();
        let rows = sqlx::query(&format!(
            "SELECT catalog_id, version, status, checksum_sha256,
                    EXTRACT(EPOCH FROM created_at)::BIGINT AS created_at_unix,
                    EXTRACT(EPOCH FROM activated_at)::BIGINT AS activated_at_unix
             FROM {cat_rel}
             WHERE project_id = $1
             ORDER BY created_at DESC"
        ))
        .bind(project_id)
        .fetch_all(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "get_catalog_versions",
                format!("get_catalog_versions failed: {err}"),
            )
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(serde_json::json!({
                "catalog_id": row.try_get::<Uuid,_>("catalog_id").map(|u| u.to_string()).unwrap_or_default(),
                "version": row.try_get::<String,_>("version").unwrap_or_default(),
                "status": row.try_get::<String,_>("status").unwrap_or_default(),
                "checksum_sha256": row.try_get::<String,_>("checksum_sha256").unwrap_or_default(),
                "created_at_unix": row.try_get::<i64,_>("created_at_unix").unwrap_or_default(),
                "activated_at_unix": row.try_get::<Option<i64>,_>("activated_at_unix").unwrap_or_default(),
            }));
        }
        Ok(out)
    }

    // ── Phase 1.3 — Migration apply engine ───────────────────────────────────

    /// Create a migration plan (dry-run or real) and store it as a run record.
    ///
    /// Loads the ACTIVE `CatalogManifest` for `project_id` from the
    /// `catalog_versions` table, diffs it against the live PostgreSQL schema
    /// using `information_schema`, and writes one row per resource into
    /// `migration_op_ledger`.  The `payload_json` column stores a JSON object
    /// `{"content": "<comment or DDL>", "checksum": "<sha>"}` that
    /// `apply_migration` reads to drive execution.
    pub async fn plan_migration(
        &self,
        project_id: &str,
        dry_run: bool,
    ) -> Result<String, tonic::Status> {
        use crate::generation::CatalogManifest;
        use crate::runtime::system::SystemCatalogConfig;
        use sha2::{Digest, Sha256};
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let runs_rel = config.migration_runs_relation();
        let ledger_rel = config.migration_op_ledger_relation();
        let cat_rel = config.catalog_versions_relation();
        ensure_migration_payload_json_column(pool, &ledger_rel).await?;

        // ── 1. Load the ACTIVE catalog manifest for this project ──────────────
        let manifest_row: Option<(String, String)> = sqlx::query_as(&format!(
            "SELECT version, manifest_json::TEXT
             FROM {cat_rel}
             WHERE project_id = $1 AND status = 'ACTIVE'
             ORDER BY activated_at DESC LIMIT 1"
        ))
        .bind(project_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "plan_migration_manifest_load",
                format!("plan_migration manifest load failed: {err}"),
            )
        })?;

        // ── 2. Build operations list ──────────────────────────────────────────
        // Each entry: (backend, resource_uri, operation_kind, structured payload)
        let mut operations: Vec<(String, String, String, serde_json::Value)> = Vec::new();
        let catalog_version;

        if let Some((version, json_str)) = manifest_row {
            catalog_version = version;
            let manifest: CatalogManifest = serde_json::from_str(&json_str).map_err(|err| {
                catalog_admin_internal_status(
                    "plan_migration_manifest_parse",
                    format!("plan_migration manifest parse failed: {err}"),
                )
            })?;

            // Bulk-fetch existing tables so we can diff in memory.
            let schema_names: Vec<&str> = manifest
                .tables
                .iter()
                .map(|t| t.schema.as_str())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();

            let existing_tables: std::collections::HashSet<(String, String)> =
                if schema_names.is_empty() {
                    Default::default()
                } else {
                    sqlx::query_as::<_, (String, String)>(
                        "SELECT table_schema, table_name
                         FROM information_schema.tables
                         WHERE table_schema = ANY($1)",
                    )
                    .bind(&schema_names)
                    .fetch_all(pool)
                    .await
                    .map_err(|err| {
                        catalog_admin_internal_status(
                            "plan_migration_schema_check",
                            format!("plan_migration schema check failed: {err}"),
                        )
                    })?
                    .into_iter()
                    .collect()
                };

            // One operation per manifest table.
            for table in &manifest.tables {
                let exists = existing_tables.contains(&(table.schema.clone(), table.table.clone()));
                let (op_kind, payload) = if exists {
                    (
                        "verify_table",
                        serde_json::json!({
                            "action": "verify_table",
                            "schema": table.schema,
                            "table": table.table,
                            "checksum": table.checksum_sha256,
                        }),
                    )
                } else {
                    (
                        "create_table",
                        postgres_create_table_payload(table, &manifest.checksum_sha256),
                    )
                };
                operations.push((
                    "postgres".to_string(),
                    format!("postgres://{}/{}", table.schema, table.table),
                    op_kind.to_string(),
                    payload,
                ));
            }

            // One structured verify operation per non-postgres store.
            for store in &manifest.stores {
                let (backend, uri_scheme) = match store.backend.as_str() {
                    "qdrant" => ("qdrant", "qdrant"),
                    "s3" | "minio" => ("minio", "s3"),
                    "redis" => ("redis", "redis"),
                    "mongodb" => ("mongodb", "mongodb"),
                    "neo4j" => ("neo4j", "neo4j"),
                    "clickhouse" => ("clickhouse", "clickhouse"),
                    _ => continue,
                };
                operations.push((
                    backend.to_string(),
                    format!("{uri_scheme}://{}", store.resource_name),
                    "verify_resource".to_string(),
                    serde_json::json!({
                        "action": "verify_resource",
                        "backend": backend,
                        "resource_name": store.resource_name,
                        "checksum": manifest.checksum_sha256,
                        "spec": store,
                    }),
                ));
            }
        } else {
            // No active catalog staged yet — plan is empty.
            catalog_version = String::new();
        }

        // ── 3. Compute operations hash ────────────────────────────────────────
        let operations_hash = {
            let mut hasher = Sha256::new();
            for (backend, uri, kind, payload) in &operations {
                hasher.update(format!("{backend}:{uri}:{kind}:{payload}").as_bytes());
            }
            format!("{:x}", hasher.finalize())
        };

        // ── 4. Insert the run record ──────────────────────────────────────────
        let state = if dry_run { "DRY_RUN" } else { "PREFLIGHT" };
        let run_id: Uuid = sqlx::query_scalar(&format!(
            "INSERT INTO {runs_rel}
                 (project_id, catalog_version, state, operations_hash)
             VALUES ($1, $2, $3, $4)
             RETURNING run_id"
        ))
        .bind(project_id)
        .bind(&catalog_version)
        .bind(state)
        .bind(&operations_hash)
        .fetch_one(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "plan_migration_run_insert",
                format!("plan_migration run insert failed: {err}"),
            )
        })?;

        // ── 5. Write operations to migration_op_ledger ────────────────────────
        for (idx, (backend, resource_uri, operation_kind, payload)) in operations.iter().enumerate()
        {
            sqlx::query(&format!(
                "INSERT INTO {ledger_rel}
                     (run_id, operation_index, backend, resource_uri,
                      operation_kind, status, payload_json)
                 VALUES ($1, $2, $3, $4, $5, 'PENDING', $6::JSONB)"
            ))
            .bind(run_id)
            .bind(idx as i32)
            .bind(backend)
            .bind(resource_uri)
            .bind(operation_kind)
            .bind(payload.to_string())
            .execute(pool)
            .await
            .map_err(|err| {
                catalog_admin_internal_status(
                    "plan_migration_op_insert",
                    format!("plan_migration op insert failed: {err}"),
                )
            })?;
        }

        Ok(run_id.to_string())
    }

    /// Persist an approval token for a planned migration run and transition it
    /// to the explicit APPROVED state.
    pub async fn approve_migration_plan(
        &self,
        project_id: &str,
        run_id: &str,
        approval_token: &str,
    ) -> Result<(), tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        validate_approval_token_for_plan(approval_token)?;
        let id: Uuid = parse_migration_run_id(run_id)?;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let runs_rel = config.migration_runs_relation();
        ensure_migration_runs_approved_state(pool, &runs_rel, &config.migration_runs_table).await?;

        let rows = sqlx::query(&format!(
            "UPDATE {runs_rel}
             SET state = 'APPROVED', approval_token = $1, error = ''
             WHERE run_id = $2 AND project_id = $3
               AND state = 'PREFLIGHT'
               AND approval_token = ''"
        ))
        .bind(approval_token.trim())
        .bind(id)
        .bind(project_id)
        .execute(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "approve_migration_plan",
                format!("approve_migration_plan failed: {err}"),
            )
        })?;
        if rows.rows_affected() == 0 {
            return Err(migration_approve_not_preflight_status());
        }
        Ok(())
    }

    /// Apply a previously-planned migration run.
    pub async fn apply_migration(
        &self,
        project_id: &str,
        run_id: &str,
        approval_token: &str,
    ) -> Result<(), tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        require_migration_approval_token(approval_token)?;
        let id: Uuid = parse_migration_run_id(run_id)?;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let runs_rel = config.migration_runs_relation();
        let ledger_rel = config.migration_op_ledger_relation();
        let phase_ledger_rel = migration_phase_ledger_relation(&config);
        ensure_migration_runs_approved_state(pool, &runs_rel, &config.migration_runs_table).await?;
        ensure_migration_payload_json_column(pool, &ledger_rel).await?;

        let run_row = sqlx::query(&format!(
            "SELECT state, approval_token, catalog_version, operations_hash
             FROM {runs_rel}
             WHERE run_id = $1 AND project_id = $2"
        ))
        .bind(id)
        .bind(project_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "apply_migration_run_fetch",
                format!("apply_migration run fetch failed: {err}"),
            )
        })?
        .ok_or_else(|| migration_run_not_found_status("apply_migration"))?;
        let state = run_row.try_get::<String, _>("state").unwrap_or_default();
        let stored_token = run_row
            .try_get::<String, _>("approval_token")
            .unwrap_or_default();
        validate_migration_apply_state_and_token(&state, approval_token, &stored_token)?;
        let catalog_version = run_row
            .try_get::<String, _>("catalog_version")
            .unwrap_or_default();
        let operations_hash = run_row
            .try_get::<String, _>("operations_hash")
            .unwrap_or_default();

        // Fetch and preflight all planned operations before moving the run into
        // APPLYING. On resume, previously APPLIED/VERIFIED/SKIPPED rows are
        // included so the phased engine can validate them without losing the
        // original operation ordering.
        let planned: Vec<(i64, i32, String, String, String, serde_json::Value)> =
            sqlx::query_as(&format!(
                "SELECT id, operation_index, backend, resource_uri, operation_kind, payload_json
                 FROM {ledger_rel}
                 WHERE run_id = $1
                   AND status IN ('PENDING','APPLIED','VERIFIED','SKIPPED','FAILED')
                 ORDER BY operation_index ASC"
            ))
            .bind(id)
            .fetch_all(pool)
            .await
            .map_err(|err| {
                catalog_admin_internal_status(
                    "apply_migration_fetch_ops",
                    format!("apply_migration fetch ops failed: {err}"),
                )
            })?;
        let preflight_ops: Vec<(i64, String, String, String, serde_json::Value)> = planned
            .iter()
            .map(
                |(op_id, _idx, backend, resource_uri, operation_kind, payload)| {
                    (
                        *op_id,
                        backend.clone(),
                        resource_uri.clone(),
                        operation_kind.clone(),
                        payload.clone(),
                    )
                },
            )
            .collect();
        let preflight_errors = self.preflight_migration_apply_ops(&preflight_ops);
        if !preflight_errors.is_empty() {
            return Err(migration_apply_preflight_status(&preflight_errors));
        }

        let rows = sqlx::query(&format!(
            "UPDATE {runs_rel}
             SET state = 'APPLYING',
                 started_at = CASE WHEN state = 'APPROVED' THEN NOW() ELSE started_at END,
                 finished_at = NULL,
                 error = ''
             WHERE run_id = $2 AND project_id = $3
               AND state IN ('APPROVED','APPLYING','VERIFYING')
               AND approval_token = $1"
        ))
        .bind(stored_token)
        .bind(id)
        .bind(project_id)
        .execute(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "apply_migration_state_update",
                format!("apply_migration failed: {err}"),
            )
        })?;
        if rows.rows_affected() == 0 {
            return Err(migration_apply_state_changed_status());
        }

        let artifacts: Vec<GeneratedArtifact> = planned
            .iter()
            .map(
                |(op_id, operation_index, backend, resource_uri, operation_kind, payload)| {
                    GeneratedArtifact {
                        rel_path: resource_uri.clone(),
                        kind: operation_kind.clone(),
                        schema: payload
                            .get("schema")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        table: payload
                            .get("table")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        content: serde_json::json!({
                            "op_id": op_id,
                            "operation_index": operation_index,
                            "backend": backend,
                            "resource_uri": resource_uri,
                            "operation_kind": operation_kind,
                            "payload": payload,
                        })
                        .to_string(),
                    }
                },
            )
            .collect();
        let op_ids: Vec<i64> = planned
            .iter()
            .map(
                |(op_id, _operation_index, _backend, _resource_uri, _operation_kind, _payload)| {
                    *op_id
                },
            )
            .collect();
        let op_kinds: Vec<String> = planned
            .iter()
            .map(
                |(_op_id, _idx, _backend, _resource_uri, operation_kind, _payload)| {
                    operation_kind.clone()
                },
            )
            .collect();
        let target = CatalogMigrationApplyTarget {
            runtime: self,
            pool,
        };
        let sink = ExistingRunMigrationAuditSink {
            pool,
            runs_rel: runs_rel.clone(),
            ledger_rel: ledger_rel.clone(),
            run_id: id,
            op_ids,
            op_kinds,
        };
        let phase_ledger = crate::migration::phase_runner::PostgresPhaseLedger::new(
            (*pool).clone(),
            phase_ledger_rel,
        );
        let phased = crate::migration::apply_artifacts_phased(
            run_id,
            &target,
            &artifacts,
            &sink,
            &phase_ledger,
            &catalog_version,
            &operations_hash,
        )
        .await;

        let outcome = match phased {
            Ok((outcome, _results)) => outcome,
            Err(err) => {
                sqlx::query(&format!(
                    "UPDATE {runs_rel}
                     SET state = 'ERROR', finished_at = NOW(), error = $1
                     WHERE run_id = $2"
                ))
                .bind(&err)
                .bind(id)
                .execute(pool)
                .await
                .map_err(|update_err| {
                    catalog_admin_internal_status(
                        "apply_migration_phase_failure_finalize",
                        format!(
                            "apply_migration phase failure '{err}', and finalize failed: {update_err}"
                        ),
                    )
                })?;
                return Err(catalog_admin_internal_status(
                    "apply_migration_phased_runner",
                    format!("apply_migration phased runner failed: {err}"),
                ));
            }
        };

        match outcome {
            crate::migration::phase_runner::RunnerOutcome::Completed { .. } => {}
            crate::migration::phase_runner::RunnerOutcome::Paused { phase, error, .. } => {
                sqlx::query(&format!(
                    "UPDATE {runs_rel}
                     SET state = 'ERROR', finished_at = NOW(), error = $1
                     WHERE run_id = $2"
                ))
                .bind(format!("phase {} paused: {error}", phase.as_str()))
                .bind(id)
                .execute(pool)
                .await
                .map_err(|err| {
                    catalog_admin_internal_status(
                        "apply_migration_finalize",
                        format!("apply_migration finalize failed: {err}"),
                    )
                })?;
                return Err(catalog_admin_internal_status(
                    "apply_migration_paused",
                    format!(
                        "apply_migration paused in phase {}: {error}",
                        phase.as_str()
                    ),
                ));
            }
            crate::migration::phase_runner::RunnerOutcome::Refused { phase, .. } => {
                let error = format!(
                    "phase {} refused by capability/ledger state",
                    phase.as_str()
                );
                sqlx::query(&format!(
                    "UPDATE {runs_rel}
                     SET state = 'ERROR', finished_at = NOW(), error = $1
                     WHERE run_id = $2"
                ))
                .bind(&error)
                .bind(id)
                .execute(pool)
                .await
                .map_err(|err| {
                    catalog_admin_internal_status(
                        "apply_migration_finalize",
                        format!("apply_migration finalize failed: {err}"),
                    )
                })?;
                return Err(migration_apply_phase_refused_status(error));
            }
        }

        sqlx::query(&format!(
            "UPDATE {runs_rel}
             SET state = 'COMPLETED', finished_at = NOW(), error = ''
             WHERE run_id = $1"
        ))
        .bind(id)
        .execute(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "apply_migration_finalize",
                format!("apply_migration finalize failed: {err}"),
            )
        })?;
        Ok(())
    }

    /// Get the status of a migration run.
    pub async fn get_migration_status(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> Result<serde_json::Value, tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let id: Uuid = parse_migration_run_id(run_id)?;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let runs_rel = config.migration_runs_relation();
        let row = sqlx::query(&format!(
            "SELECT run_id, project_id, catalog_version, state, started_at, finished_at, error
             FROM {runs_rel}
             WHERE run_id = $1 AND project_id = $2"
        ))
        .bind(id)
        .bind(project_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "get_migration_status",
                format!("get_migration_status failed: {err}"),
            )
        })?
        .ok_or_else(|| migration_run_not_found_status("get_migration_status"))?;
        Ok(serde_json::json!({
            "run_id": row.try_get::<Uuid,_>("run_id").map(|u| u.to_string()).unwrap_or_default(),
            "project_id": row.try_get::<String,_>("project_id").unwrap_or_default(),
            "catalog_version": row.try_get::<String,_>("catalog_version").unwrap_or_default(),
            "state": row.try_get::<String,_>("state").unwrap_or_default(),
            "error": row.try_get::<String,_>("error").unwrap_or_default(),
        }))
    }

    // ── Phase 2.2 — DLQ management ───────────────────────────────────────────

    /// List DLQ events, optionally filtered by topic and status.
    ///
    /// GAP 26 fix: all filter values are now bound with `push_bind` via `QueryBuilder`;
    /// no string interpolation from gRPC-supplied strings into SQL.
    pub async fn list_dlq_events(
        &self,
        topic: &str,
        status_filter: &str,
        limit: i64,
        page_token: &str,
        tenant_id: &str,
        project_id: &str,
    ) -> Result<Vec<serde_json::Value>, tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        use sqlx::QueryBuilder;
        let status_filter = parse_dlq_status_filter(status_filter)?;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let dlq_rel = config.dlq_relation();

        let offset: i64 = page_token.parse().unwrap_or(0);

        // Build parameterised query — never interpolate caller strings
        let mut qb = QueryBuilder::new(
            "SELECT dlq_id, event_id, topic, tenant_id, project_id, payload, error_type, error_message,
                    status, EXTRACT(EPOCH FROM created_at)::BIGINT AS created_at_unix,
                    EXTRACT(EPOCH FROM updated_at)::BIGINT AS updated_at_unix,
                    COUNT(*) OVER() AS total_count
             FROM ",
        );
        qb.push(&dlq_rel);

        let mut first_filter = true;
        if !tenant_id.trim().is_empty() {
            qb.push(if first_filter { " WHERE " } else { " AND " });
            first_filter = false;
            qb.push("tenant_id = ");
            qb.push_bind(tenant_id);
        }
        if !project_id.trim().is_empty() {
            qb.push(if first_filter { " WHERE " } else { " AND " });
            first_filter = false;
            qb.push("project_id = ");
            qb.push_bind(project_id);
        }
        if !topic.is_empty() {
            qb.push(if first_filter { " WHERE " } else { " AND " });
            first_filter = false;
            qb.push("topic = ");
            qb.push_bind(topic);
        }
        if !status_filter.is_empty() {
            qb.push(if first_filter { " WHERE " } else { " AND " });
            qb.push("status = ");
            qb.push_bind(&status_filter);
        }

        qb.push(" ORDER BY created_at DESC LIMIT ");
        qb.push_bind(limit);
        qb.push(" OFFSET ");
        qb.push_bind(offset);

        let rows = qb.build().fetch_all(pool).await.map_err(|err| {
            catalog_admin_internal_status(
                "list_dlq_events",
                format!("list_dlq_events failed: {err}"),
            )
        })?;
        Ok(rows.iter().map(|row| serde_json::json!({
            "dlq_id": row.try_get::<Uuid,_>("dlq_id").map(|u| u.to_string()).unwrap_or_default(),
            "event_id": row.try_get::<Uuid,_>("event_id").map(|u| u.to_string()).unwrap_or_default(),
            "topic": row.try_get::<String,_>("topic").unwrap_or_default(),
            "tenant_id": row.try_get::<String,_>("tenant_id").unwrap_or_default(),
            "project_id": row.try_get::<String,_>("project_id").unwrap_or_default(),
            "payload_json": row.try_get::<serde_json::Value,_>("payload").map(|v| v.to_string()).unwrap_or_default(),
            "error_type": row.try_get::<String,_>("error_type").unwrap_or_default(),
            "error_message": row.try_get::<String,_>("error_message").unwrap_or_default(),
            "status": row.try_get::<String,_>("status").unwrap_or_default(),
            "created_at_unix": row.try_get::<i64,_>("created_at_unix").unwrap_or_default(),
            "updated_at_unix": row.try_get::<i64,_>("updated_at_unix").unwrap_or_default(),
            "total_count": row.try_get::<i64,_>("total_count").unwrap_or_default(),
        })).collect())
    }

    /// Fetch one DLQ event by id.
    pub async fn get_dlq_event(
        &self,
        dlq_id: &str,
        tenant_id: &str,
        project_id: &str,
    ) -> Result<serde_json::Value, tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let id: Uuid = parse_dlq_id(dlq_id)?;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let dlq_rel = config.dlq_relation();
        let row = sqlx::query(&format!(
            "SELECT dlq_id, event_id, topic, tenant_id, project_id, payload, error_type, error_message,
                    retry_count, last_retry_at, next_retry_at, status,
                    EXTRACT(EPOCH FROM created_at)::BIGINT AS created_at_unix,
                    EXTRACT(EPOCH FROM updated_at)::BIGINT AS updated_at_unix
             FROM {dlq_rel}
             WHERE dlq_id = $1 AND tenant_id = $2 AND project_id = $3"
        ))
        .bind(id)
        .bind(tenant_id)
        .bind(project_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "get_dlq_event",
                format!("get_dlq_event failed: {err}"),
            )
        })?
        .ok_or_else(|| {
            dlq_event_not_found_status("get_dlq_event", format!("DLQ event {dlq_id} not found"))
        })?;
        Ok(serde_json::json!({
            "dlq_id": row.try_get::<Uuid,_>("dlq_id").map(|u| u.to_string()).unwrap_or_default(),
            "event_id": row.try_get::<Uuid,_>("event_id").map(|u| u.to_string()).unwrap_or_default(),
            "topic": row.try_get::<String,_>("topic").unwrap_or_default(),
            "tenant_id": row.try_get::<String,_>("tenant_id").unwrap_or_default(),
            "project_id": row.try_get::<String,_>("project_id").unwrap_or_default(),
            "payload_json": row.try_get::<serde_json::Value,_>("payload").map(|v| v.to_string()).unwrap_or_default(),
            "error_type": row.try_get::<String,_>("error_type").unwrap_or_default(),
            "error_message": row.try_get::<String,_>("error_message").unwrap_or_default(),
            "retry_count": row.try_get::<i32,_>("retry_count").unwrap_or_default(),
            "status": row.try_get::<String,_>("status").unwrap_or_default(),
            "created_at_unix": row.try_get::<i64,_>("created_at_unix").unwrap_or_default(),
            "updated_at_unix": row.try_get::<i64,_>("updated_at_unix").unwrap_or_default(),
        }))
    }

    /// Replay one DLQ event by re-enqueueing its failed payload into the outbox.
    pub async fn replay_dlq_event(
        &self,
        dlq_id: &str,
        preserve_event_id: bool,
        tenant_id: &str,
        project_id: &str,
    ) -> Result<String, tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let id: Uuid = parse_dlq_id(dlq_id)?;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let dlq_rel = config.dlq_relation();
        let outbox_rel = config.cdc.outbox_relation();
        let row = sqlx::query(&format!(
            "SELECT event_id, topic, payload, retry_count
             FROM {dlq_rel}
             WHERE dlq_id = $1 AND tenant_id = $2 AND project_id = $3 AND status IN ('OPEN','RETRYING','QUARANTINED')"
        ))
        .bind(id)
        .bind(tenant_id)
        .bind(project_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "replay_dlq_event_lookup",
                format!("replay_dlq_event lookup failed: {err}"),
            )
        })?
        .ok_or_else(dlq_event_not_found_or_not_replayable_status)?;

        let original_event_id: Uuid = row.try_get("event_id").map_err(|err| {
            catalog_admin_internal_status(
                "replay_dlq_event_id_decode",
                format!("DLQ event_id decode failed: {err}"),
            )
        })?;
        let retry_count = row.try_get::<i32, _>("retry_count").unwrap_or_default();
        let stored_topic = row.try_get::<String, _>("topic").unwrap_or_default();
        let stored_payload: serde_json::Value = row.try_get("payload").map_err(|err| {
            catalog_admin_internal_status(
                "replay_dlq_payload_decode",
                format!("DLQ payload decode failed: {err}"),
            )
        })?;
        let payload = stored_payload
            .get("failed_event")
            .cloned()
            .unwrap_or_else(|| stored_payload.clone());
        let topic = if stored_topic.trim().is_empty() {
            payload
                .get("topic")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string()
        } else {
            stored_topic
        };
        if topic.trim().is_empty() {
            return Err(dlq_missing_topic_status());
        }
        let replay_event_id = if preserve_event_id {
            original_event_id
        } else {
            Uuid::new_v4()
        };
        let partition_key = payload
            .get("partition_key")
            .or_else(|| payload.get("document_id"))
            .or_else(|| payload.get("event_id"))
            .and_then(|value| value.as_str())
            .unwrap_or(dlq_id)
            .to_string();

        let mut tx = pool.begin().await.map_err(|err| {
            catalog_admin_internal_status(
                "replay_dlq_begin",
                format!("DLQ replay begin failed: {err}"),
            )
        })?;
        // ONE shared insert path (`cdc::insert_outbox_row`): the DLQ replay re-enqueues
        // with the SAME SQL + bind types as every other writer. `replay_event_id` is
        // already a `Uuid` (Copy), and the canonical insert binds `$1::UUID` — folding
        // the prior bare-`$1` insert into the shared shape so it can't drift (§4).
        crate::runtime::cdc::insert_outbox_row(
            &mut *tx,
            &outbox_rel,
            replay_event_id,
            &topic,
            &partition_key,
            &payload,
        )
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "replay_dlq_enqueue",
                format!("DLQ replay enqueue failed: {err}"),
            )
        })?;
        sqlx::query(&format!(
            "UPDATE {dlq_rel}
             SET status = 'REPLAYED',
                 retry_count = $1,
                 last_retry_at = NOW(),
                 updated_at = NOW()
             WHERE dlq_id = $2"
        ))
        .bind(retry_count + 1)
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "replay_dlq_update",
                format!("DLQ replay update failed: {err}"),
            )
        })?;
        tx.commit().await.map_err(|err| {
            catalog_admin_internal_status(
                "replay_dlq_commit",
                format!("DLQ replay commit failed: {err}"),
            )
        })?;
        Ok(replay_event_id.to_string())
    }

    /// Update a DLQ event's status.
    pub async fn update_dlq_status(
        &self,
        dlq_id: &str,
        new_status: &str,
        tenant_id: &str,
        project_id: &str,
    ) -> Result<(), tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let id: Uuid = parse_dlq_id(dlq_id)?;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let dlq_rel = config.dlq_relation();
        let rows = sqlx::query(&format!(
            "UPDATE {dlq_rel} SET status = $1, updated_at = NOW() WHERE dlq_id = $2 AND tenant_id = $3 AND project_id = $4"
        ))
        .bind(new_status)
        .bind(id)
        .bind(tenant_id)
        .bind(project_id)
        .execute(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "update_dlq_status",
                format!("update_dlq_status failed: {err}"),
            )
        })?;
        if rows.rows_affected() == 0 {
            return Err(dlq_event_not_found_status(
                "update_dlq_status",
                format!("DLQ event {dlq_id} not found"),
            ));
        }
        Ok(())
    }

    // ── Phase 2.3 — CDC control plane ────────────────────────────────────────

    /// Get CDC status for a slot.
    pub async fn get_cdc_status(
        &self,
        slot_name: &str,
        tenant_id: &str,
        project_id: &str,
    ) -> Result<serde_json::Value, tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let ctrl_rel = config.cdc_control_relation();
        let out_rel = config.cdc.outbox_relation();
        let row = sqlx::query(&format!(
            // The transactional outbox is append-and-consume: the CDC tailer reads it
            // via logical replication and rows are pruned after publish — there is no
            // per-row `dispatched_at` column. Outbox depth is therefore the count of
            // rows currently present (the pending/unconsumed backlog).
            "SELECT c.slot_name, c.paused, c.pause_reason,
                    (SELECT COUNT(*) FROM {out_rel}) AS outbox_depth
             FROM {ctrl_rel} c
             WHERE c.slot_name = $1 AND c.tenant_id = $2 AND c.project_id = $3"
        ))
        .bind(slot_name)
        .bind(tenant_id)
        .bind(project_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status("get_cdc_status", format!("get_cdc_status failed: {err}"))
        })?;
        match row {
            Some(r) => Ok(serde_json::json!({
                "slot_name": r.try_get::<String,_>("slot_name").unwrap_or_default(),
                "paused": r.try_get::<bool,_>("paused").unwrap_or(false),
                "pause_reason": r.try_get::<String,_>("pause_reason").unwrap_or_default(),
                "outbox_depth": r.try_get::<i64,_>("outbox_depth").unwrap_or(0),
            })),
            None => {
                // Slot not registered — return a default healthy status
                Ok(serde_json::json!({
                    "slot_name": slot_name,
                    "paused": false,
                    "pause_reason": "",
                    "outbox_depth": 0,
                }))
            }
        }
    }

    /// Pause CDC on a slot.
    pub async fn pause_cdc(
        &self,
        slot_name: &str,
        reason: &str,
        tenant_id: &str,
        project_id: &str,
    ) -> Result<(), tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let ctrl_rel = config.cdc_control_relation();
        sqlx::query(&format!(
            "INSERT INTO {ctrl_rel} (slot_name, tenant_id, project_id, paused, pause_reason)
             VALUES ($1, $2, $3, TRUE, $4)
             ON CONFLICT (slot_name) DO UPDATE
                SET paused = TRUE, pause_reason = EXCLUDED.pause_reason, updated_at = NOW()
                WHERE {ctrl_rel}.tenant_id = EXCLUDED.tenant_id AND {ctrl_rel}.project_id = EXCLUDED.project_id"
        ))
        .bind(slot_name)
        .bind(tenant_id)
        .bind(project_id)
        .bind(reason)
        .execute(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status("pause_cdc", format!("pause_cdc failed: {err}"))
        })?;
        Ok(())
    }

    /// Resume a paused CDC slot.
    pub async fn resume_cdc(
        &self,
        slot_name: &str,
        tenant_id: &str,
        project_id: &str,
    ) -> Result<(), tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let ctrl_rel = config.cdc_control_relation();
        sqlx::query(&format!(
            "INSERT INTO {ctrl_rel} (slot_name, tenant_id, project_id, paused, pause_reason)
             VALUES ($1, $2, $3, FALSE, '')
             ON CONFLICT (slot_name) DO UPDATE
                SET paused = FALSE, pause_reason = '', updated_at = NOW()
                WHERE {ctrl_rel}.tenant_id = EXCLUDED.tenant_id AND {ctrl_rel}.project_id = EXCLUDED.project_id"
        ))
        .bind(slot_name)
        .bind(tenant_id)
        .bind(project_id)
        .execute(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status("resume_cdc", format!("resume_cdc failed: {err}"))
        })?;
        Ok(())
    }

    /// Request a leader step-down on the CDC slot.
    pub async fn stepdown_cdc_leader(
        &self,
        slot_name: &str,
        tenant_id: &str,
        project_id: &str,
    ) -> Result<(), tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let ctrl_rel = config.cdc_control_relation();
        sqlx::query(&format!(
            "INSERT INTO {ctrl_rel} (slot_name, tenant_id, project_id, paused, requested_leader_stepdown_at)
             VALUES ($1, $2, $3, FALSE, NOW())
             ON CONFLICT (slot_name) DO UPDATE
                SET requested_leader_stepdown_at = NOW(), updated_at = NOW()
                WHERE {ctrl_rel}.tenant_id = EXCLUDED.tenant_id AND {ctrl_rel}.project_id = EXCLUDED.project_id"
        ))
        .bind(slot_name)
        .bind(tenant_id)
        .bind(project_id)
        .execute(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "stepdown_cdc_leader",
                format!("stepdown_cdc_leader failed: {err}"),
            )
        })?;
        Ok(())
    }

    // ── Phase 3.2 — Saga admin ───────────────────────────────────────────────

    /// List sagas — delegates to the saga module.
    /// NW1-3c: routes through the SystemStores registry instead of
    /// the PG pool. A deployment without a registered canonical
    /// store returns `Unavailable`.
    pub async fn list_sagas_admin(
        &self,
        tenant_id: &str,
        status_filter: &str,
        tx_id_filter: &str,
        correlation_id_filter: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<crate::runtime::saga::SagaAdminRecord>, tonic::Status> {
        let store = self.default_system_stores_or_unready()?;
        let config = crate::runtime::system::SystemCatalogConfig::default();
        crate::runtime::saga::list_sagas(
            store.as_ref(),
            &config,
            tenant_id,
            status_filter,
            tx_id_filter,
            correlation_id_filter,
            limit,
            offset,
        )
        .await
    }

    /// Get a single saga by ID.
    pub async fn get_saga_admin(
        &self,
        saga_id: &str,
    ) -> Result<crate::runtime::saga::SagaAdminRecord, tonic::Status> {
        let store = self.default_system_stores_or_unready()?;
        let config = crate::runtime::system::SystemCatalogConfig::default();
        crate::runtime::saga::get_saga(store.as_ref(), &config, saga_id).await
    }

    /// Retry compensation for a saga.
    pub async fn retry_saga_compensation_admin(&self, saga_id: &str) -> Result<(), tonic::Status> {
        let store = self.default_system_stores_or_unready()?;
        let config = crate::runtime::system::SystemCatalogConfig::default();
        crate::runtime::saga::retry_saga_compensation(store.as_ref(), &config, saga_id).await
    }

    /// Mark a saga as reviewed.
    pub async fn mark_saga_reviewed_admin(&self, saga_id: &str) -> Result<(), tonic::Status> {
        let store = self.default_system_stores_or_unready()?;
        let config = crate::runtime::system::SystemCatalogConfig::default();
        crate::runtime::saga::mark_saga_reviewed(store.as_ref(), &config, saga_id).await
    }

    /// Resolves the default `SystemStores` trait object, or returns gRPC
    /// `FailedPrecondition` (NOT `Unavailable`) when no canonical store is
    /// registered.
    ///
    /// Rationale (S2): "no canonical store registered" is a permanent
    /// configuration/provisioning state, not a transient outage — the caller
    /// must NOT retry until an operator fixes it. `Unavailable` is in the SDK
    /// retry set, so returning it here made an unprovisioned broker look like a
    /// ~0.7 s "slow RPC" (4 retries × backoff) on the benchmark dashboard
    /// instead of a clear, fail-fast precondition error. `FailedPrecondition`
    /// is the correct gRPC code for "system not in the required state."
    fn default_system_stores_or_unready(
        &self,
    ) -> Result<std::sync::Arc<dyn crate::runtime::canonical_store::SystemStores>, tonic::Status>
    {
        self.default_system_stores()
            .ok_or_else(|| system_store_unregistered_status())
    }

    // ── Phase 5.1 — Policy admin ─────────────────────────────────────────────

    /// List all ABAC policies from the DB.
    pub async fn list_policies(
        &self,
        include_disabled: bool,
    ) -> Result<Vec<serde_json::Value>, tonic::Status> {
        let pool = self.pg_pool()?;
        let abac_table = self.abac_policy_relation();
        let enabled_clause = if include_disabled {
            ""
        } else {
            "WHERE enabled = TRUE"
        };
        let rows = sqlx::query(&format!(
            "SELECT policy_id, effect, service_identity, tenant_id, purpose,
                    message_type, operation, required_scope, priority, enabled
             FROM {abac_table}
             {enabled_clause}
             ORDER BY priority DESC, policy_id ASC"
        ))
        .fetch_all(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status("list_policies", format!("list_policies failed: {err}"))
        })?;
        Ok(rows.iter().map(|row| serde_json::json!({
            "policy_id": row.try_get::<i64,_>("policy_id").unwrap_or_default(),
            "effect": row.try_get::<String,_>("effect").unwrap_or_default(),
            "service_identity": row.try_get::<String,_>("service_identity").unwrap_or_default(),
            "tenant_id": row.try_get::<String,_>("tenant_id").unwrap_or_default(),
            "purpose": row.try_get::<String,_>("purpose").unwrap_or_default(),
            "message_type": row.try_get::<String,_>("message_type").unwrap_or_default(),
            "operation": row.try_get::<String,_>("operation").unwrap_or_default(),
            "required_scope": row.try_get::<String,_>("required_scope").unwrap_or_default(),
            "priority": row.try_get::<i32,_>("priority").unwrap_or_default(),
            "enabled": row.try_get::<bool,_>("enabled").unwrap_or(true),
        })).collect())
    }

    pub async fn list_policies_page(
        &self,
        include_disabled: bool,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<serde_json::Value>, i64), tonic::Status> {
        let pool = self.pg_pool()?;
        let abac_table = self.abac_policy_relation();
        let enabled_clause = if include_disabled {
            ""
        } else {
            "WHERE enabled = TRUE"
        };
        let rows = sqlx::query(&format!(
            "SELECT policy_id, effect, service_identity, tenant_id, purpose,
                    message_type, operation, required_scope, priority, enabled,
                    COUNT(*) OVER() AS total_count
             FROM {abac_table}
             {enabled_clause}
             ORDER BY priority DESC, policy_id ASC
             LIMIT $1 OFFSET $2"
        ))
        .bind(limit.max(1))
        .bind(offset.max(0))
        .fetch_all(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "list_policies_page",
                format!("list_policies_page failed: {err}"),
            )
        })?;
        let total = rows
            .first()
            .and_then(|row| row.try_get::<i64, _>("total_count").ok())
            .unwrap_or(0);
        Ok((rows.iter().map(|row| serde_json::json!({
            "policy_id": row.try_get::<i64,_>("policy_id").unwrap_or_default(),
            "effect": row.try_get::<String,_>("effect").unwrap_or_default(),
            "service_identity": row.try_get::<String,_>("service_identity").unwrap_or_default(),
            "tenant_id": row.try_get::<String,_>("tenant_id").unwrap_or_default(),
            "purpose": row.try_get::<String,_>("purpose").unwrap_or_default(),
            "message_type": row.try_get::<String,_>("message_type").unwrap_or_default(),
            "operation": row.try_get::<String,_>("operation").unwrap_or_default(),
            "required_scope": row.try_get::<String,_>("required_scope").unwrap_or_default(),
            "priority": row.try_get::<i32,_>("priority").unwrap_or_default(),
            "enabled": row.try_get::<bool,_>("enabled").unwrap_or(true),
        })).collect(), total))
    }

    /// Upsert a policy.  Returns the new/updated policy_id.
    #[allow(clippy::too_many_arguments)]
    pub async fn put_policy(
        &self,
        effect: &str,
        service_identity: &str,
        tenant_id: &str,
        purpose: &str,
        message_type: &str,
        operation: &str,
        required_scope: &str,
        priority: i32,
        enabled: bool,
        policy_id: Option<i64>,
    ) -> Result<i64, tonic::Status> {
        let pool = self.pg_pool()?;
        let abac_table = self.abac_policy_relation();
        let new_id: i64 = if let Some(id) = policy_id {
            sqlx::query_scalar(&format!(
                "UPDATE {abac_table}
                 SET effect=$1, service_identity=$2, tenant_id=$3, purpose=$4,
                     message_type=$5, operation=$6, required_scope=$7, priority=$8, enabled=$9
                 WHERE policy_id=$10
                 RETURNING policy_id"
            ))
            .bind(effect)
            .bind(service_identity)
            .bind(tenant_id)
            .bind(purpose)
            .bind(message_type)
            .bind(operation)
            .bind(required_scope)
            .bind(priority)
            .bind(enabled)
            .bind(id)
            .fetch_one(pool)
            .await
            .map_err(|err| {
                catalog_admin_internal_status(
                    "put_policy_update",
                    format!("put_policy update failed: {err}"),
                )
            })?
        } else {
            sqlx::query_scalar(&format!(
                "INSERT INTO {abac_table}
                     (effect, service_identity, tenant_id, purpose, message_type,
                      operation, required_scope, priority, enabled)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                 RETURNING policy_id"
            ))
            .bind(effect)
            .bind(service_identity)
            .bind(tenant_id)
            .bind(purpose)
            .bind(message_type)
            .bind(operation)
            .bind(required_scope)
            .bind(priority)
            .bind(enabled)
            .fetch_one(pool)
            .await
            .map_err(|err| {
                catalog_admin_internal_status(
                    "put_policy_insert",
                    format!("put_policy insert failed: {err}"),
                )
            })?
        };
        Ok(new_id)
    }

    /// Delete a policy by ID.
    pub async fn delete_policy(&self, policy_id: i64) -> Result<(), tonic::Status> {
        let pool = self.pg_pool()?;
        let abac_table = self.abac_policy_relation();
        let rows = sqlx::query(&format!("DELETE FROM {abac_table} WHERE policy_id = $1"))
            .bind(policy_id)
            .execute(pool)
            .await
            .map_err(|err| {
                catalog_admin_internal_status(
                    "delete_policy",
                    format!("delete_policy failed: {err}"),
                )
            })?;
        if rows.rows_affected() == 0 {
            return Err(catalog_admin_policy_not_found_status(
                "delete_policy",
                "policy_not_found",
                format!("policy {policy_id} not found"),
            ));
        }
        Ok(())
    }

    /// Lint policies: detect obvious conflicts and dead rules.
    pub async fn lint_policies(&self) -> Result<Vec<String>, tonic::Status> {
        let policies = self.list_policies(true).await?;
        let mut findings = Vec::new();
        // Detect allow+deny pairs with identical selectors
        for (i, a) in policies.iter().enumerate() {
            for b in policies.iter().skip(i + 1) {
                let same_selector = a["service_identity"] == b["service_identity"]
                    && a["tenant_id"] == b["tenant_id"]
                    && a["message_type"] == b["message_type"]
                    && a["operation"] == b["operation"];
                if same_selector && a["effect"] != b["effect"] {
                    findings.push(format!(
                        "conflicting allow+deny policies: ids {} and {}",
                        a["policy_id"], b["policy_id"]
                    ));
                }
            }
        }
        if findings.is_empty() {
            findings.push("lint passed: no conflicts detected".to_string());
        }
        Ok(findings)
    }

    // ── Phase 5.2 — Audit trail ──────────────────────────────────────────────

    /// Write an entry to the admin audit log.
    ///
    /// NW1-3d: routes through `AdminAuditStore::append_admin_audit`.
    /// The atomic chain-append (latest-hash read + canonical hash
    /// compute + INSERT with the chain lock held) is owned by the
    /// trait impl. The hash function is the same canonical one
    /// (`compute_admin_audit_hash`) used by `verify_admin_audit_chain`,
    /// so chains written here verify cleanly via the trait's verify
    /// method too.
    #[allow(clippy::too_many_arguments)]
    pub async fn write_audit_log(
        &self,
        actor: &str,
        operation: &str,
        target: &str,
        request_json: &serde_json::Value,
        result: &str,
        tenant_id: &str,
        project_id: &str,
        correlation_id: &str,
    ) -> Result<(), tonic::Status> {
        use crate::runtime::canonical_store::system_store::{AdminAuditInsert, AdminAuditStore};
        let Some(store) = self.default_system_stores() else {
            // Audit log is best-effort; if no canonical store is
            // registered (slim deployments) we silently drop the
            // audit entry — same behavior the pre-NW1-3d
            // "no pg_pool" path had.
            return Ok(());
        };
        let signer_key_id = crate::runtime::security::SecurityConfig::current()
            .current_encryption_key_id
            .trim()
            .to_string();
        let signer_key_id = if signer_key_id.is_empty() {
            "default".to_string()
        } else {
            signer_key_id
        };
        let entry = AdminAuditInsert {
            actor: actor.to_string(),
            operation: operation.to_string(),
            target: target.to_string(),
            request_json: request_json.clone(),
            result: result.to_string(),
            tenant_id: tenant_id.to_string(),
            project_id: project_id.to_string(),
            correlation_id: correlation_id.to_string(),
            signer_key_id,
            external_anchor: String::new(),
        };
        AdminAuditStore::append_admin_audit(store.as_ref(), &entry)
            .await
            .map(|_| ())
            .map_err(|err| {
                tracing::warn!(error=?err, "audit log write failed (non-fatal)");
                catalog_admin_internal_status(
                    "write_audit_log",
                    format!("write_audit_log failed: {err}"),
                )
            })
    }

    // ── Phase 6.1 — Project registry ─────────────────────────────────────────

    /// Ensure a project exists in the registry; upsert if already present.
    pub async fn ensure_project(
        &self,
        project_id: &str,
        name: &str,
        cdc_topic_prefix: &str,
    ) -> Result<(), tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let proj_rel = format!(
            "\"{}\".\"{}\"",
            config.cdc.system_schema, config.projects_table
        );
        sqlx::query(&format!(
            "INSERT INTO {proj_rel} (project_id, name, cdc_topic_prefix)
             VALUES ($1,$2,$3)
             ON CONFLICT (project_id) DO UPDATE
                SET name = EXCLUDED.name, cdc_topic_prefix = EXCLUDED.cdc_topic_prefix"
        ))
        .bind(project_id)
        .bind(name)
        .bind(cdc_topic_prefix)
        .execute(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status("ensure_project", format!("ensure_project failed: {err}"))
        })?;
        Ok(())
    }

    /// List all projects.
    pub async fn list_projects(&self) -> Result<Vec<serde_json::Value>, tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let proj_rel = format!(
            "\"{}\".\"{}\"",
            config.cdc.system_schema, config.projects_table
        );
        let rows = sqlx::query(&format!(
            "SELECT project_id, name, cdc_topic_prefix, active_catalog_version,
                    EXTRACT(EPOCH FROM created_at)::BIGINT AS created_at_unix
             FROM {proj_rel}
             ORDER BY created_at ASC"
        ))
        .fetch_all(pool)
        .await
        .map_err(|err| {
            catalog_admin_internal_status("list_projects", format!("list_projects failed: {err}"))
        })?;
        Ok(rows.iter().map(|row| serde_json::json!({
            "project_id": row.try_get::<String,_>("project_id").unwrap_or_default(),
            "name": row.try_get::<String,_>("name").unwrap_or_default(),
            "cdc_topic_prefix": row.try_get::<String,_>("cdc_topic_prefix").unwrap_or_default(),
            "active_catalog_version": row.try_get::<String,_>("active_catalog_version").unwrap_or_default(),
            "created_at_unix": row.try_get::<i64,_>("created_at_unix").unwrap_or_default(),
        })).collect())
    }

    /// Paginated migration run list for the operator console.
    pub async fn list_migration_runs(
        &self,
        project_id: &str,
        state_filter: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<serde_json::Value>, tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let run_rel = config.migration_runs_relation();
        let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "SELECT run_id, project_id, catalog_version, state,
                    EXTRACT(EPOCH FROM started_at)::BIGINT AS started_at_unix,
                    EXTRACT(EPOCH FROM finished_at)::BIGINT AS finished_at_unix,
                    error
             FROM ",
        );
        qb.push(run_rel);
        let mut where_written = false;
        if !project_id.trim().is_empty() {
            qb.push(if where_written { " AND " } else { " WHERE " });
            where_written = true;
            qb.push("project_id = ");
            qb.push_bind(project_id);
        }
        if !state_filter.trim().is_empty() {
            qb.push(if where_written { " AND " } else { " WHERE " });
            qb.push("state = ");
            qb.push_bind(state_filter);
        }
        qb.push(" ORDER BY started_at DESC LIMIT ");
        qb.push_bind(limit.max(1));
        qb.push(" OFFSET ");
        qb.push_bind(offset.max(0));
        let rows = qb.build().fetch_all(pool).await.map_err(|err| {
            catalog_admin_internal_status(
                "list_migration_runs",
                format!("list_migration_runs failed: {err}"),
            )
        })?;
        Ok(rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "run_id": row.try_get::<Uuid,_>("run_id").map(|id| id.to_string()).unwrap_or_default(),
                    "project_id": row.try_get::<String,_>("project_id").unwrap_or_default(),
                    "catalog_version": row.try_get::<String,_>("catalog_version").unwrap_or_default(),
                    "state": row.try_get::<String,_>("state").unwrap_or_default(),
                    "started_at_unix": row.try_get::<i64,_>("started_at_unix").unwrap_or_default(),
                    "finished_at_unix": row.try_get::<i64,_>("finished_at_unix").unwrap_or_default(),
                    "error": row.try_get::<String,_>("error").unwrap_or_default(),
                })
            })
            .collect())
    }

    /// Paginated admin audit log list for the operator console.
    ///
    /// NW1-3d: routes through `AdminAuditStore::list_admin_audit`.
    /// The wire shape returned (one JSON object per row with the
    /// existing field set) is preserved exactly so the operator
    /// console doesn't see a contract change.
    #[allow(clippy::too_many_arguments)]
    pub async fn list_admin_audit_logs(
        &self,
        operation_filter: &str,
        actor_filter: &str,
        tenant_id_filter: &str,
        project_id_filter: &str,
        limit: i64,
        offset: i64,
        redact: bool,
    ) -> Result<Vec<serde_json::Value>, tonic::Status> {
        use crate::runtime::canonical_store::system_store::{
            AdminAuditListFilter, AdminAuditStore,
        };
        let store = self.default_system_stores_or_unready()?;
        let filter = AdminAuditListFilter {
            operation: (!operation_filter.trim().is_empty()).then(|| operation_filter.to_string()),
            actor: (!actor_filter.trim().is_empty()).then(|| actor_filter.to_string()),
            tenant_id: (!tenant_id_filter.trim().is_empty()).then(|| tenant_id_filter.to_string()),
            project_id: (!project_id_filter.trim().is_empty())
                .then(|| project_id_filter.to_string()),
            limit: limit.max(1),
            offset: offset.max(0),
            redact_request_json: redact,
        };
        let rows = AdminAuditStore::list_admin_audit(store.as_ref(), &filter)
            .await
            .map_err(|err| {
                catalog_admin_internal_status(
                    "list_admin_audit_logs",
                    format!("list_admin_audit_logs failed: {err}"),
                )
            })?;
        Ok(rows
            .into_iter()
            .map(|row| {
                serde_json::json!({
                    "audit_id": row.audit_id.to_string(),
                    "actor": row.actor,
                    "operation": row.operation,
                    "target": row.target,
                    "request_json": row.request_json,
                    "result": row.result,
                    "tenant_id": row.tenant_id,
                    "project_id": row.project_id,
                    "correlation_id": row.correlation_id,
                    "previous_hash": row.previous_hash,
                    "current_hash": row.current_hash,
                    "signer_key_id": row.signer_key_id,
                    "external_anchor": row.external_anchor,
                    "created_at_unix": row.created_at.timestamp(),
                })
            })
            .collect())
    }

    /// Verify the admin audit log hash chain from oldest to newest.
    ///
    /// NW1-3d: routes through `AdminAuditStore::verify_admin_audit_chain`.
    /// The trait impl streams 1,000-row pages and uses the shared
    /// `verify_admin_audit_chain_step` helper — same canonical hash
    /// function and same `previous_hash`/`current_hash` checks the
    /// pre-NW1-3d code used. The output JSON shape is preserved
    /// (passed / checked_count / first_broken_audit_id / reason /
    /// expected_* / actual_* / last_hash) so the operator console
    /// doesn't see a wire-contract change.
    pub async fn verify_admin_audit_log_chain(
        &self,
        limit: i64,
    ) -> Result<serde_json::Value, tonic::Status> {
        use crate::runtime::canonical_store::system_store::{
            AdminAuditChainReport, AdminAuditStore,
        };
        let store = self.default_system_stores_or_unready()?;
        let limit_opt = if limit > 0 { Some(limit) } else { None };
        let report = AdminAuditStore::verify_admin_audit_chain(store.as_ref(), limit_opt)
            .await
            .map_err(|err| {
                catalog_admin_internal_status(
                    "verify_admin_audit_log_chain",
                    format!("verify_admin_audit_log_chain failed: {err}"),
                )
            })?;
        Ok(match report {
            AdminAuditChainReport::Passed {
                checked_count,
                last_hash,
            } => serde_json::json!({
                "passed": true,
                "checked_count": checked_count as i32,
                "first_broken_audit_id": "",
                "reason": "",
                "expected_previous_hash": "",
                "actual_previous_hash": "",
                "expected_current_hash": "",
                "actual_current_hash": "",
                "last_hash": last_hash,
            }),
            AdminAuditChainReport::Failed {
                checked_count,
                first_broken_audit_id,
                reason,
                expected_previous_hash,
                actual_previous_hash,
                expected_current_hash,
                actual_current_hash,
            } => serde_json::json!({
                "passed": false,
                "checked_count": checked_count as i32,
                "first_broken_audit_id": first_broken_audit_id.to_string(),
                "reason": reason.as_str(),
                "expected_previous_hash": expected_previous_hash,
                "actual_previous_hash": actual_previous_hash,
                "expected_current_hash": expected_current_hash,
                "actual_current_hash": actual_current_hash,
                "last_hash": "",
            }),
        })
    }
}

#[cfg(test)]
mod migration_approval_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed error detail trailer");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_single_field_violation(status: &tonic::Status, field: &str, description: &str) {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert!(!detail.retryable);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_policy_detail(
        status: &tonic::Status,
        operation: &str,
        policy_decision_id: &str,
        message: &str,
    ) {
        assert_policy_detail_with_code(
            status,
            tonic::Code::FailedPrecondition,
            operation,
            policy_decision_id,
            message,
        );
    }

    fn assert_policy_detail_with_code(
        status: &tonic::Status,
        code: tonic::Code,
        operation: &str,
        policy_decision_id: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), code);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_schema_detail(
        status: &tonic::Status,
        operation: &str,
        schema_code: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "catalog");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, schema_code);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_not_found_schema_detail(
        status: &tonic::Status,
        operation: &str,
        schema_code: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "catalog");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, schema_code);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_capability_detail(
        status: &tonic::Status,
        backend: &str,
        operation: &str,
        capability_required: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, backend);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability_required);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_internal_detail(status: &tonic::Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "catalog_admin");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn approval_token_match_rejects_empty_and_wrong_tokens() {
        assert!(!migration_approval_tokens_match("", "stored-token"));
        assert!(!migration_approval_tokens_match("provided-token", ""));
        assert!(!migration_approval_tokens_match(
            "wrong-token",
            "stored-token"
        ));
        assert!(migration_approval_tokens_match(
            "stored-token",
            "stored-token"
        ));
    }

    #[test]
    fn catalog_admin_manifest_validation_carries_field_violations() {
        let utf8 = parse_catalog_manifest_json(&[0xff]).unwrap_err();
        assert_eq!(utf8.message(), "manifest_json is not valid UTF-8");
        assert_single_field_violation(&utf8, "manifest_json", "must be valid UTF-8");

        let json = parse_catalog_manifest_json(b"{").unwrap_err();
        assert!(
            json.message().starts_with("manifest_json parse error:"),
            "unexpected message: {}",
            json.message()
        );
        assert_single_field_violation(&json, "manifest_json", "must be valid JSON");
    }

    #[test]
    fn catalog_admin_id_and_filter_validation_carries_field_violations() {
        let token = validate_approval_token_for_plan(" ").unwrap_err();
        assert_eq!(token.message(), "approval_token must not be empty");
        assert_single_field_violation(&token, "approval_token", "must be non-empty");

        let run_id = parse_migration_run_id("not-a-uuid").unwrap_err();
        assert_eq!(run_id.message(), "run_id must be a UUID");
        assert_single_field_violation(&run_id, "run_id", "must be a UUID");

        let dlq_id = parse_dlq_id("not-a-uuid").unwrap_err();
        assert_eq!(dlq_id.message(), "dlq_id must be a UUID");
        assert_single_field_violation(&dlq_id, "dlq_id", "must be a UUID");

        let status = parse_dlq_status_filter("bogus").unwrap_err();
        assert_eq!(
            status.message(),
            "invalid status_filter 'BOGUS'; must be one of [\"OPEN\", \"REPLAYED\", \"DISMISSED\", \"QUARANTINED\", \"RETRYING\"]"
        );
        assert_single_field_violation(
            &status,
            "status_filter",
            "must be one of OPEN, REPLAYED, DISMISSED, QUARANTINED, RETRYING",
        );
    }

    /// Audit item 10: `apply_migration`'s reject paths, exercised through the
    /// extracted validator the serving path now calls
    /// (`validate_migration_apply_state_and_token`), so the empty-token,
    /// wrong-token and non-APPROVED-state rejections are unit-tested without a
    /// live PostgreSQL ledger.
    #[test]
    fn apply_migration_validation_rejects_empty_token() {
        let err = validate_migration_apply_state_and_token("APPROVED", "", "stored-token")
            .expect_err("empty approval_token must be rejected");
        assert_policy_detail(
            &err,
            "apply_migration",
            "approval_token_required",
            "approval_token is required",
        );

        let err = validate_migration_apply_state_and_token("APPROVED", "   ", "stored-token")
            .expect_err("whitespace-only approval_token must be rejected");
        assert_policy_detail(
            &err,
            "apply_migration",
            "approval_token_required",
            "approval_token is required",
        );
    }

    #[test]
    fn apply_migration_validation_rejects_wrong_token() {
        let err =
            validate_migration_apply_state_and_token("APPROVED", "wrong-token", "stored-token")
                .expect_err("mismatched approval_token must be rejected");
        assert_policy_detail(
            &err,
            "apply_migration",
            "approval_token_mismatch",
            "approval_token is missing or does not match the approved token",
        );

        // A run whose stored token was never set must also fail closed.
        let err = validate_migration_apply_state_and_token("APPROVED", "provided-token", "")
            .expect_err("missing stored token must be rejected");
        assert_policy_detail(
            &err,
            "apply_migration",
            "approval_token_mismatch",
            "approval_token is missing or does not match the approved token",
        );
    }

    #[test]
    fn apply_migration_validation_rejects_non_approved_states() {
        for state in [
            "DRY_RUN",
            "PREFLIGHT",
            "COMPLETED",
            "ERROR",
            "DEAD_LETTER",
            "",
        ] {
            let err =
                validate_migration_apply_state_and_token(state, "stored-token", "stored-token")
                    .expect_err("non-approved migration state must be rejected");
            assert_policy_detail(
                &err,
                "apply_migration",
                "migration_run_not_approved",
                "migration run must be approved before apply and not be terminal",
            );
        }
    }

    #[test]
    fn catalog_admin_failed_preconditions_carry_typed_detail() {
        assert_policy_detail(
            &migration_approve_not_preflight_status(),
            "approve_migration_plan",
            "migration_run_not_preflight",
            "migration run not found, not in PREFLIGHT, or already approved",
        );
        assert_schema_detail(
            &migration_apply_preflight_status(&["operation 7 has no executable SQL".to_string()]),
            "apply_migration",
            "migration_apply_preflight_failed",
            "apply_migration preflight failed: operation 7 has no executable SQL",
        );
        assert_policy_detail(
            &migration_apply_state_changed_status(),
            "apply_migration",
            "migration_approval_state_changed",
            "migration run approval state changed before apply",
        );
        assert_policy_detail(
            &migration_apply_phase_refused_status("phase Apply refused by capability/ledger state"),
            "apply_migration",
            "migration_phase_refused",
            "phase Apply refused by capability/ledger state",
        );
        assert_schema_detail(
            &dlq_missing_topic_status(),
            "replay_dlq_event",
            "dlq_event_missing_topic",
            "DLQ event has no topic and cannot be replayed",
        );
        assert_capability_detail(
            &system_store_unregistered_status(),
            "catalog_admin",
            "system_store",
            "canonical_system_store",
            "no canonical store is registered; saga/audit admin requires a provisioned Postgres / MySQL / SQLite system store (udb_system)",
        );
    }

    #[test]
    fn catalog_admin_activate_not_found_statuses_carry_schema_detail() {
        for (schema_code, message) in [
            (
                "staged_catalog_not_found",
                "no staged catalog found for project",
            ),
            (
                "staged_catalog_version_not_found",
                "staged catalog version not found",
            ),
            (
                "staged_catalog_not_found",
                "catalog not found or not in STAGED status",
            ),
        ] {
            assert_not_found_schema_detail(
                &catalog_admin_not_found_status("activate_catalog", schema_code, message),
                "activate_catalog",
                schema_code,
                message,
            );
        }
    }

    #[test]
    fn catalog_admin_migration_dlq_not_found_statuses_carry_schema_detail() {
        let dlq_id = "11111111-1111-4111-8111-111111111111";
        for (status, operation, schema_code, message) in [
            (
                migration_run_not_found_status("apply_migration"),
                "apply_migration",
                "migration_run_not_found",
                "migration run not found".to_string(),
            ),
            (
                migration_run_not_found_status("get_migration_status"),
                "get_migration_status",
                "migration_run_not_found",
                "migration run not found".to_string(),
            ),
            (
                dlq_event_not_found_status(
                    "get_dlq_event",
                    format!("DLQ event {dlq_id} not found"),
                ),
                "get_dlq_event",
                "dlq_event_not_found",
                format!("DLQ event {dlq_id} not found"),
            ),
            (
                dlq_event_not_found_or_not_replayable_status(),
                "replay_dlq_event",
                "dlq_event_not_found_or_not_replayable",
                "DLQ event not found or not replayable".to_string(),
            ),
            (
                dlq_event_not_found_status(
                    "update_dlq_status",
                    format!("DLQ event {dlq_id} not found"),
                ),
                "update_dlq_status",
                "dlq_event_not_found",
                format!("DLQ event {dlq_id} not found"),
            ),
        ] {
            assert_not_found_schema_detail(&status, operation, schema_code, &message);
        }
    }

    #[test]
    fn catalog_admin_internal_status_carries_typed_detail() {
        assert_internal_detail(
            &catalog_admin_internal_status(
                "activate_catalog_lookup",
                "activate_catalog lookup failed: broken",
            ),
            "activate_catalog_lookup",
            "activate_catalog lookup failed: broken",
        );
        assert_internal_detail(
            &catalog_admin_internal_status(
                "replay_dlq_enqueue",
                "DLQ replay enqueue failed: broken",
            ),
            "replay_dlq_enqueue",
            "DLQ replay enqueue failed: broken",
        );
    }

    #[test]
    fn catalog_admin_policy_delete_not_found_carries_policy_detail() {
        assert_policy_detail_with_code(
            &catalog_admin_policy_not_found_status(
                "delete_policy",
                "policy_not_found",
                "policy 42 not found",
            ),
            tonic::Code::NotFound,
            "delete_policy",
            "policy_not_found",
            "policy 42 not found",
        );
    }

    #[test]
    fn apply_migration_validation_accepts_resumable_states_with_matching_token() {
        for state in ["APPROVED", "APPLYING", "VERIFYING"] {
            validate_migration_apply_state_and_token(state, "stored-token", "stored-token")
                .unwrap_or_else(|err| {
                    panic!("state {state:?} with matching token must pass: {err}")
                });
        }
    }

    #[test]
    fn create_table_payload_contains_executable_bootstrap_ddl() {
        let mut table = crate::generation::ManifestTable {
            schema: "public".to_string(),
            table: "widgets".to_string(),
            checksum_sha256: "table-sha".to_string(),
            columns: vec![crate::generation::ManifestColumn {
                field_name: "id".to_string(),
                column_name: "id".to_string(),
                proto_type: "string".to_string(),
                sql_type: "TEXT".to_string(),
                not_null: true,
                ..Default::default()
            }],
            primary_key: vec!["id".to_string()],
            ..Default::default()
        };
        table.table_security.tenant_isolation_mode = "none".to_string();

        let payload = postgres_create_table_payload(&table, "manifest-sha");
        let content = payload
            .get("content")
            .and_then(|value| value.as_str())
            .unwrap_or_default();

        assert!(!content.trim().is_empty());
        assert!(migration_payload_has_executable_sql(content));
        assert!(content.contains("CREATE TABLE IF NOT EXISTS"));
        assert!(content.contains("\"public\".\"widgets\""));
        assert!(content.contains("PRIMARY KEY"));
    }
}
