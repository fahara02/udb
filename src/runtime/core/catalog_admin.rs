//! Continuation `impl DataBrokerRuntime` block (Phase F split of core.rs).
use super::*;

enum MigrationApplyOutcome {
    Applied,
    Verified,
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
                if content.trim().is_empty() || content.trim_start().starts_with("--") {
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
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let manifest_json_str = std::str::from_utf8(manifest_json)
            .map_err(|_| tonic::Status::invalid_argument("manifest_json is not valid UTF-8"))?;
        let manifest_value: serde_json::Value =
            serde_json::from_str(manifest_json_str).map_err(|err| {
                tonic::Status::invalid_argument(format!("manifest_json parse error: {err}"))
            })?;
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
        .map_err(|err| tonic::Status::internal(format!("stage_catalog failed: {err}")))?;
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
                tonic::Status::internal(format!("activate_catalog lookup failed: {err}"))
            })?
            .ok_or_else(|| tonic::Status::not_found("no staged catalog found for project"))?
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
                tonic::Status::internal(format!("activate_catalog lookup failed: {err}"))
            })?
            .ok_or_else(|| tonic::Status::not_found("staged catalog version not found"))?
        };
        // Activation mutates five tables; run them in a single transaction so a
        // mid-sequence failure cannot leave the catalog with no ACTIVE row (or
        // two). The transaction is dropped (rolled back) on any early return.
        let mut tx = pool.begin().await.map_err(|err| {
            tonic::Status::internal(format!("activate_catalog begin failed: {err}"))
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
        .map_err(|err| tonic::Status::internal(format!("activate_catalog lookup failed: {err}")))?;
        // Deactivate old active
        sqlx::query(&format!(
            "UPDATE {cat_rel} SET status = 'ROLLED_BACK'
             WHERE project_id = $1 AND status = 'ACTIVE'"
        ))
        .bind(project_id)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            tonic::Status::internal(format!("activate_catalog deactivate failed: {err}"))
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
        .map_err(|err| tonic::Status::internal(format!("activate_catalog update failed: {err}")))?;
        if rows.rows_affected() == 0 {
            return Err(tonic::Status::not_found(
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
            tonic::Status::internal(format!("activate_catalog version fetch failed: {err}"))
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
        .map_err(|err| tonic::Status::internal(format!("activate_catalog log failed: {err}")))?;
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
            tonic::Status::internal(format!("activate_catalog project binding failed: {err}"))
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
            tonic::Status::internal(format!("activate_catalog reload log failed: {err}"))
        })?;
        tx.commit().await.map_err(|err| {
            tonic::Status::internal(format!("activate_catalog commit failed: {err}"))
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
        .map_err(|err| tonic::Status::internal(format!("get_catalog_versions failed: {err}")))?;
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
    /// `migration_op_ledger`.  The `rollback_json` column stores a JSON object
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
            tonic::Status::internal(format!("plan_migration manifest load failed: {err}"))
        })?;

        // ── 2. Build operations list ──────────────────────────────────────────
        // Each entry: (backend, resource_uri, operation_kind, structured payload)
        let mut operations: Vec<(String, String, String, serde_json::Value)> = Vec::new();
        let catalog_version;

        if let Some((version, json_str)) = manifest_row {
            catalog_version = version;
            let manifest: CatalogManifest = serde_json::from_str(&json_str).map_err(|err| {
                tonic::Status::internal(format!("plan_migration manifest parse failed: {err}"))
            })?;

            // Bulk-fetch existing tables so we can diff in memory.
            let schema_names: Vec<&str> = manifest
                .tables
                .iter()
                .map(|t| t.schema.as_str())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();

            let existing_tables: std::collections::HashSet<(String, String)> = if schema_names
                .is_empty()
            {
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
                    tonic::Status::internal(format!("plan_migration schema check failed: {err}"))
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
                        serde_json::json!({
                            "action": "create_table",
                            "schema": table.schema,
                            "table": table.table,
                            "checksum": table.checksum_sha256,
                            "content": "",
                            "error": "missing executable SQL; run sync-migrations --backend postgres to generate full DDL",
                        }),
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
            tonic::Status::internal(format!("plan_migration run insert failed: {err}"))
        })?;

        // ── 5. Write operations to migration_op_ledger ────────────────────────
        for (idx, (backend, resource_uri, operation_kind, payload)) in operations.iter().enumerate()
        {
            sqlx::query(&format!(
                "INSERT INTO {ledger_rel}
                     (run_id, operation_index, backend, resource_uri,
                      operation_kind, status, rollback_json)
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
                tonic::Status::internal(format!("plan_migration op insert failed: {err}"))
            })?;
        }

        Ok(run_id.to_string())
    }

    /// Apply a previously-planned migration run.
    pub async fn apply_migration(
        &self,
        project_id: &str,
        run_id: &str,
        approval_token: &str,
    ) -> Result<(), tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let runs_rel = config.migration_runs_relation();
        let ledger_rel = config.migration_op_ledger_relation();
        let id: Uuid = run_id
            .parse()
            .map_err(|_| tonic::Status::invalid_argument("run_id must be a UUID"))?;
        // Fetch and preflight all pending operations before moving the run into
        // APPLYING. This keeps unsupported backend work fail-closed without
        // partially mutating run state.
        let pending: Vec<(i64, String, String, String, serde_json::Value)> =
            sqlx::query_as(&format!(
                "SELECT id, backend, resource_uri, operation_kind, rollback_json
                 FROM {ledger_rel}
                 WHERE run_id = $1 AND status = 'PENDING'
                 ORDER BY operation_index ASC"
            ))
            .bind(id)
            .fetch_all(pool)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!("apply_migration fetch ops failed: {err}"))
            })?;
        let preflight_errors = self.preflight_migration_apply_ops(&pending);
        if !preflight_errors.is_empty() {
            return Err(tonic::Status::failed_precondition(format!(
                "apply_migration preflight failed: {}",
                preflight_errors.join("; ")
            )));
        }

        let rows = sqlx::query(&format!(
            "UPDATE {runs_rel}
             SET state = 'APPLYING', approval_token = $1, started_at = NOW()
             WHERE run_id = $2 AND project_id = $3
               AND state IN ('DRY_RUN','PREFLIGHT')"
        ))
        .bind(approval_token)
        .bind(id)
        .bind(project_id)
        .execute(pool)
        .await
        .map_err(|err| tonic::Status::internal(format!("apply_migration failed: {err}")))?;
        if rows.rows_affected() == 0 {
            return Err(tonic::Status::not_found(
                "migration run not found or not in a plannable state",
            ));
        }

        let mut error_msgs: Vec<String> = Vec::new();

        for (op_id, backend, resource_uri, operation_kind, payload) in &pending {
            let result = self
                .execute_migration_apply_op(
                    pool,
                    *op_id,
                    backend,
                    resource_uri,
                    operation_kind,
                    payload,
                )
                .await;

            let (new_status, error_str) = match &result {
                Ok(MigrationApplyOutcome::Applied) => ("APPLIED", String::new()),
                Ok(MigrationApplyOutcome::Verified) => ("VERIFIED", String::new()),
                Err(msg) => {
                    error_msgs.push(msg.clone());
                    ("FAILED", msg.clone())
                }
            };

            sqlx::query(&format!(
                "UPDATE {ledger_rel}
                 SET status = $1,
                     error  = $2,
                     applied_at = CASE WHEN $1 = 'APPLIED' THEN NOW() ELSE NULL END
                 WHERE id = $3"
            ))
            .bind(new_status)
            .bind(&error_str)
            .bind(op_id)
            .execute(pool)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!("apply_migration ledger update failed: {err}"))
            })?;
        }

        // ── Finalise the run ──────────────────────────────────────────────────
        let (final_state, final_error) = if error_msgs.is_empty() {
            ("COMPLETED", String::new())
        } else {
            ("ERROR", error_msgs.join("; "))
        };
        sqlx::query(&format!(
            "UPDATE {runs_rel}
             SET state = $1, finished_at = NOW(), error = $2
             WHERE run_id = $3"
        ))
        .bind(final_state)
        .bind(&final_error)
        .bind(id)
        .execute(pool)
        .await
        .map_err(|err| {
            tonic::Status::internal(format!("apply_migration finalize failed: {err}"))
        })?;

        if !error_msgs.is_empty() {
            return Err(tonic::Status::internal(format!(
                "apply_migration: {} operation(s) failed — {}",
                error_msgs.len(),
                final_error
            )));
        }
        Ok(())
    }

    /// Get the status of a migration run.
    pub async fn get_migration_status(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> Result<serde_json::Value, tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let runs_rel = config.migration_runs_relation();
        let id: Uuid = run_id
            .parse()
            .map_err(|_| tonic::Status::invalid_argument("run_id must be a UUID"))?;
        let row = sqlx::query(&format!(
            "SELECT run_id, project_id, catalog_version, state, started_at, finished_at, error
             FROM {runs_rel}
             WHERE run_id = $1 AND project_id = $2"
        ))
        .bind(id)
        .bind(project_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| tonic::Status::internal(format!("get_migration_status failed: {err}")))?
        .ok_or_else(|| tonic::Status::not_found("migration run not found"))?;
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
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let dlq_rel = config.dlq_relation();

        // Validate status_filter against the known enum values
        const ALLOWED_DLQ_STATUSES: &[&str] =
            &["OPEN", "REPLAYED", "DISMISSED", "QUARANTINED", "RETRYING"];
        let status_filter = status_filter.trim().to_ascii_uppercase();
        if !status_filter.is_empty() && !ALLOWED_DLQ_STATUSES.contains(&status_filter.as_str()) {
            return Err(tonic::Status::invalid_argument(format!(
                "invalid status_filter '{status_filter}'; must be one of {:?}",
                ALLOWED_DLQ_STATUSES
            )));
        }

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

        let rows = qb
            .build()
            .fetch_all(pool)
            .await
            .map_err(|err| tonic::Status::internal(format!("list_dlq_events failed: {err}")))?;
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
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let dlq_rel = config.dlq_relation();
        let id: Uuid = dlq_id
            .parse()
            .map_err(|_| tonic::Status::invalid_argument("dlq_id must be a UUID"))?;
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
        .map_err(|err| tonic::Status::internal(format!("get_dlq_event failed: {err}")))?
        .ok_or_else(|| tonic::Status::not_found(format!("DLQ event {dlq_id} not found")))?;
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
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let dlq_rel = config.dlq_relation();
        let outbox_rel = config.cdc.outbox_relation();
        let id: Uuid = dlq_id
            .parse()
            .map_err(|_| tonic::Status::invalid_argument("dlq_id must be a UUID"))?;
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
        .map_err(|err| tonic::Status::internal(format!("replay_dlq_event lookup failed: {err}")))?
        .ok_or_else(|| tonic::Status::not_found("DLQ event not found or not replayable"))?;

        let original_event_id: Uuid = row
            .try_get("event_id")
            .map_err(|err| tonic::Status::internal(format!("DLQ event_id decode failed: {err}")))?;
        let retry_count = row.try_get::<i32, _>("retry_count").unwrap_or_default();
        let stored_topic = row.try_get::<String, _>("topic").unwrap_or_default();
        let stored_payload: serde_json::Value = row
            .try_get("payload")
            .map_err(|err| tonic::Status::internal(format!("DLQ payload decode failed: {err}")))?;
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
            return Err(tonic::Status::failed_precondition(
                "DLQ event has no topic and cannot be replayed",
            ));
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

        let mut tx = pool
            .begin()
            .await
            .map_err(|err| tonic::Status::internal(format!("DLQ replay begin failed: {err}")))?;
        sqlx::query(&format!(
            "INSERT INTO {outbox_rel}
                 (event_id, topic, partition_key, payload, created_at)
             VALUES ($1, $2, $3, $4::JSONB, NOW())"
        ))
        .bind(replay_event_id)
        .bind(&topic)
        .bind(&partition_key)
        .bind(payload.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|err| tonic::Status::internal(format!("DLQ replay enqueue failed: {err}")))?;
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
        .map_err(|err| tonic::Status::internal(format!("DLQ replay update failed: {err}")))?;
        tx.commit()
            .await
            .map_err(|err| tonic::Status::internal(format!("DLQ replay commit failed: {err}")))?;
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
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let dlq_rel = config.dlq_relation();
        let id: Uuid = dlq_id
            .parse()
            .map_err(|_| tonic::Status::invalid_argument("dlq_id must be a UUID"))?;
        let rows = sqlx::query(&format!(
            "UPDATE {dlq_rel} SET status = $1, updated_at = NOW() WHERE dlq_id = $2 AND tenant_id = $3 AND project_id = $4"
        ))
        .bind(new_status)
        .bind(id)
        .bind(tenant_id)
        .bind(project_id)
        .execute(pool)
        .await
        .map_err(|err| tonic::Status::internal(format!("update_dlq_status failed: {err}")))?;
        if rows.rows_affected() == 0 {
            return Err(tonic::Status::not_found(format!(
                "DLQ event {dlq_id} not found"
            )));
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
            "SELECT c.slot_name, c.paused, c.pause_reason,
                    (SELECT COUNT(*) FROM {out_rel} WHERE dispatched_at IS NULL) AS outbox_depth
             FROM {ctrl_rel} c
             WHERE c.slot_name = $1 AND c.tenant_id = $2 AND c.project_id = $3"
        ))
        .bind(slot_name)
        .bind(tenant_id)
        .bind(project_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| tonic::Status::internal(format!("get_cdc_status failed: {err}")))?;
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
        .map_err(|err| tonic::Status::internal(format!("pause_cdc failed: {err}")))?;
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
        .map_err(|err| tonic::Status::internal(format!("resume_cdc failed: {err}")))?;
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
        .map_err(|err| tonic::Status::internal(format!("stepdown_cdc_leader failed: {err}")))?;
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
        let store = self.default_system_stores_or_unavailable()?;
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
        let store = self.default_system_stores_or_unavailable()?;
        let config = crate::runtime::system::SystemCatalogConfig::default();
        crate::runtime::saga::get_saga(store.as_ref(), &config, saga_id).await
    }

    /// Retry compensation for a saga.
    pub async fn retry_saga_compensation_admin(&self, saga_id: &str) -> Result<(), tonic::Status> {
        let store = self.default_system_stores_or_unavailable()?;
        let config = crate::runtime::system::SystemCatalogConfig::default();
        crate::runtime::saga::retry_saga_compensation(store.as_ref(), &config, saga_id).await
    }

    /// Mark a saga as reviewed.
    pub async fn mark_saga_reviewed_admin(&self, saga_id: &str) -> Result<(), tonic::Status> {
        let store = self.default_system_stores_or_unavailable()?;
        let config = crate::runtime::system::SystemCatalogConfig::default();
        crate::runtime::saga::mark_saga_reviewed(store.as_ref(), &config, saga_id).await
    }

    /// NW1-3c helper: resolves the default `SystemStores` trait
    /// object or returns gRPC `Unavailable` for callers that need a
    /// canonical store registered.
    fn default_system_stores_or_unavailable(
        &self,
    ) -> Result<std::sync::Arc<dyn crate::runtime::canonical_store::SystemStores>, tonic::Status>
    {
        self.default_system_stores().ok_or_else(|| {
            tonic::Status::unavailable(
                "no canonical store is registered; saga admin requires Postgres / MySQL / SQLite",
            )
        })
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
        .map_err(|err| tonic::Status::internal(format!("list_policies failed: {err}")))?;
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
        .map_err(|err| tonic::Status::internal(format!("list_policies_page failed: {err}")))?;
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
            .map_err(|err| tonic::Status::internal(format!("put_policy update failed: {err}")))?
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
            .map_err(|err| tonic::Status::internal(format!("put_policy insert failed: {err}")))?
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
            .map_err(|err| tonic::Status::internal(format!("delete_policy failed: {err}")))?;
        if rows.rows_affected() == 0 {
            return Err(tonic::Status::not_found(format!(
                "policy {policy_id} not found"
            )));
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
                tonic::Status::internal(format!("write_audit_log failed: {err}"))
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
        .map_err(|err| tonic::Status::internal(format!("ensure_project failed: {err}")))?;
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
        .map_err(|err| tonic::Status::internal(format!("list_projects failed: {err}")))?;
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
        let rows =
            qb.build().fetch_all(pool).await.map_err(|err| {
                tonic::Status::internal(format!("list_migration_runs failed: {err}"))
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
        let store = self.default_system_stores_or_unavailable()?;
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
                tonic::Status::internal(format!("list_admin_audit_logs failed: {err}"))
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
        let store = self.default_system_stores_or_unavailable()?;
        let limit_opt = if limit > 0 { Some(limit) } else { None };
        let report = AdminAuditStore::verify_admin_audit_chain(store.as_ref(), limit_opt)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!("verify_admin_audit_log_chain failed: {err}"))
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

#[derive(Clone, Debug)]
struct AdminAuditChainRow {
    audit_id: String,
    actor: String,
    operation: String,
    target: String,
    request_json: serde_json::Value,
    result: String,
    tenant_id: String,
    project_id: String,
    correlation_id: String,
    previous_hash: String,
    current_hash: String,
    signer_key_id: String,
    external_anchor: String,
}

#[derive(Clone, Debug, Default)]
struct AdminAuditChainVerification {
    passed: bool,
    checked_count: i32,
    first_broken_audit_id: String,
    reason: String,
    expected_previous_hash: String,
    actual_previous_hash: String,
    expected_current_hash: String,
    actual_current_hash: String,
    last_hash: String,
}

impl AdminAuditChainVerification {
    fn passed(checked_count: i32, last_hash: String) -> Self {
        Self {
            passed: true,
            checked_count,
            last_hash,
            ..Self::default()
        }
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "passed": self.passed,
            "checked_count": self.checked_count,
            "first_broken_audit_id": self.first_broken_audit_id,
            "reason": self.reason,
            "expected_previous_hash": self.expected_previous_hash,
            "actual_previous_hash": self.actual_previous_hash,
            "expected_current_hash": self.expected_current_hash,
            "actual_current_hash": self.actual_current_hash,
            "last_hash": self.last_hash,
        })
    }
}

#[cfg(test)]
fn verify_admin_audit_rows(rows: &[AdminAuditChainRow]) -> AdminAuditChainVerification {
    let mut previous = String::new();
    for (idx, row) in rows.iter().enumerate() {
        match verify_admin_audit_row(row, &previous, idx as i32) {
            Ok(current_hash) => previous = current_hash,
            Err(result) => return result,
        }
    }
    AdminAuditChainVerification::passed(rows.len() as i32, previous)
}

fn verify_admin_audit_row(
    row: &AdminAuditChainRow,
    expected_previous_hash: &str,
    checked_count: i32,
) -> Result<String, AdminAuditChainVerification> {
    if row.previous_hash != expected_previous_hash {
        return Err(AdminAuditChainVerification {
            passed: false,
            checked_count,
            first_broken_audit_id: row.audit_id.clone(),
            reason: "previous_hash_mismatch".to_string(),
            expected_previous_hash: expected_previous_hash.to_string(),
            actual_previous_hash: row.previous_hash.clone(),
            ..AdminAuditChainVerification::default()
        });
    }
    let expected_current_hash = admin_audit_hash(
        &row.previous_hash,
        &row.actor,
        &row.operation,
        &row.target,
        &row.request_json,
        &row.result,
        &row.tenant_id,
        &row.project_id,
        &row.correlation_id,
        &row.signer_key_id,
        &row.external_anchor,
    );
    if row.current_hash.is_empty() {
        return Err(AdminAuditChainVerification {
            passed: false,
            checked_count,
            first_broken_audit_id: row.audit_id.clone(),
            reason: "missing_current_hash".to_string(),
            expected_current_hash,
            actual_current_hash: row.current_hash.clone(),
            ..AdminAuditChainVerification::default()
        });
    }
    if row.current_hash != expected_current_hash {
        return Err(AdminAuditChainVerification {
            passed: false,
            checked_count,
            first_broken_audit_id: row.audit_id.clone(),
            reason: "current_hash_mismatch".to_string(),
            expected_current_hash,
            actual_current_hash: row.current_hash.clone(),
            ..AdminAuditChainVerification::default()
        });
    }
    Ok(row.current_hash.clone())
}

#[allow(clippy::too_many_arguments)]
fn admin_audit_hash(
    previous_hash: &str,
    actor: &str,
    operation: &str,
    target: &str,
    request_json: &serde_json::Value,
    result: &str,
    tenant_id: &str,
    project_id: &str,
    correlation_id: &str,
    signer_key_id: &str,
    external_anchor: &str,
) -> String {
    use sha2::{Digest, Sha256};

    let canonical = serde_json::json!({
        "previous_hash": previous_hash,
        "actor": actor,
        "operation": operation,
        "target": target,
        "request_json": request_json,
        "result": result,
        "tenant_id": tenant_id,
        "project_id": project_id,
        "correlation_id": correlation_id,
        "signer_key_id": signer_key_id,
        "external_anchor": external_anchor,
    });
    // A-D pass (2026-05-30) bonus fix: `serde_json::Value::Object`
    // preserves insertion order when ANY transitive dep enables the
    // `preserve_order` feature (the `bson` crate does, via
    // `mongodb-driver`). That means two semantically-equal request
    // payloads could hash differently depending on caller key
    // order — silently breaking the tamper-evident audit chain.
    // Walk the tree and re-emit every object with sorted keys
    // before hashing so the result is canonical regardless of the
    // active serde_json feature set.
    let sorted = sort_json_keys(&canonical);
    let encoded = serde_json::to_vec(&sorted).unwrap_or_default();
    format!("{:x}", Sha256::digest(encoded))
}

/// Recursively rewrite every JSON object with keys sorted
/// lexicographically. Arrays and scalars pass through unchanged.
/// Used by `admin_audit_hash` to guarantee canonical bytes
/// regardless of `serde_json`'s `preserve_order` feature.
fn sort_json_keys(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(map) => {
            let mut sorted: std::collections::BTreeMap<&String, serde_json::Value> =
                std::collections::BTreeMap::new();
            for (k, val) in map.iter() {
                sorted.insert(k, sort_json_keys(val));
            }
            let mut out = serde_json::Map::with_capacity(sorted.len());
            for (k, val) in sorted {
                out.insert(k.clone(), val);
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(sort_json_keys).collect())
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod audit_hash_tests {
    use super::*;

    #[test]
    fn admin_audit_hash_links_previous_hash() {
        let request = serde_json::json!({"target": "catalog"});
        let first = admin_audit_hash(
            "",
            "actor",
            "StageCatalog",
            "catalog:v1",
            &request,
            "ok",
            "tenant",
            "project",
            "corr",
            "key-1",
            "",
        );
        let second = admin_audit_hash(
            &first,
            "actor",
            "ActivateCatalog",
            "catalog:v1",
            &request,
            "ok",
            "tenant",
            "project",
            "corr",
            "key-1",
            "",
        );
        assert_ne!(first, second);
        assert_eq!(first.len(), 64);
        assert_eq!(second.len(), 64);
    }

    #[test]
    fn admin_audit_hash_is_canonical_for_json_key_order() {
        let left = serde_json::json!({"a": 1, "b": 2});
        let right = serde_json::json!({"b": 2, "a": 1});
        assert_eq!(
            admin_audit_hash("", "a", "op", "t", &left, "ok", "tenant", "p", "c", "k", ""),
            admin_audit_hash(
                "", "a", "op", "t", &right, "ok", "tenant", "p", "c", "k", ""
            )
        );
    }

    fn audit_row(
        audit_id: &str,
        operation: &str,
        previous_hash: &str,
        request_json: serde_json::Value,
    ) -> AdminAuditChainRow {
        let current_hash = admin_audit_hash(
            previous_hash,
            "actor",
            operation,
            "target",
            &request_json,
            "ok",
            "tenant",
            "project",
            "corr",
            "key-1",
            "",
        );
        AdminAuditChainRow {
            audit_id: audit_id.to_string(),
            actor: "actor".to_string(),
            operation: operation.to_string(),
            target: "target".to_string(),
            request_json,
            result: "ok".to_string(),
            tenant_id: "tenant".to_string(),
            project_id: "project".to_string(),
            correlation_id: "corr".to_string(),
            previous_hash: previous_hash.to_string(),
            current_hash,
            signer_key_id: "key-1".to_string(),
            external_anchor: String::new(),
        }
    }

    #[test]
    fn verify_admin_audit_rows_accepts_valid_chain() {
        let first = audit_row("first", "StageCatalog", "", serde_json::json!({"a": 1}));
        let second = audit_row(
            "second",
            "ActivateCatalog",
            &first.current_hash,
            serde_json::json!({"a": 2}),
        );
        let second_hash = second.current_hash.clone();
        let result = verify_admin_audit_rows(&[first, second]);
        assert!(result.passed);
        assert_eq!(result.checked_count, 2);
        assert_eq!(result.last_hash, second_hash);
        assert_eq!(result.last_hash.len(), 64);
    }

    #[test]
    fn verify_admin_audit_rows_rejects_current_hash_mismatch() {
        let mut row = audit_row("first", "StageCatalog", "", serde_json::json!({"a": 1}));
        row.result = "tampered".to_string();
        let result = verify_admin_audit_rows(&[row]);
        assert!(!result.passed);
        assert_eq!(result.reason, "current_hash_mismatch");
        assert_eq!(result.first_broken_audit_id, "first");
    }

    #[test]
    fn verify_admin_audit_rows_rejects_previous_hash_mismatch() {
        let first = audit_row("first", "StageCatalog", "", serde_json::json!({"a": 1}));
        let second = audit_row(
            "second",
            "ActivateCatalog",
            "wrong-previous",
            serde_json::json!({"a": 2}),
        );
        let result = verify_admin_audit_rows(&[first, second]);
        assert!(!result.passed);
        assert_eq!(result.reason, "previous_hash_mismatch");
        assert_eq!(result.first_broken_audit_id, "second");
    }
}
