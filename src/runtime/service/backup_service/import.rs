//! The tenant restore IMPORT machinery for the native `BackupService`:
//! `restore_tenant` (shared movement gate, fresh-target guard, manifest load +
//! integrity verification, decrypt-at-rest, FK-ordered reinsert, cross-tenant
//! unique/foreign-key remapping, journal + outbox) plus the restore-remap model
//! and the Postgres unique-index probe it uses. Extracted verbatim from the
//! former god file — the SQL, crypto, and remap contracts are byte-for-byte
//! identical; `svc` replaces `&self`.

use std::collections::{HashMap, HashSet};

use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::proto::udb::core::backup::services::v1 as backup_pb;
use crate::runtime::channels::OperationChannel;
use crate::runtime::core::tenant_purge::plan_tenant_purge;
use crate::runtime::executor_utils::qi_runtime;
use crate::runtime::native_catalog::native_model;
use crate::runtime::tenant_movement::{
    TenantMovementOperation, TenantMovementRequest, tenant_movement_policy_status,
    validate_tenant_movement_scope,
};

use super::super::native_helpers::{
    admit_on as native_admit_on, parse_uuid, project_scoped_native_service_context,
    validate_request_tenant,
};
use super::BackupServiceImpl;
use super::config::{BACKUP_RUN_MSG, KIND_RESTORE, TOPIC_BACKUP_RESTORED};
use super::errors::{
    backup_internal_status, backup_not_found_status, backup_run_location_missing_status,
    backup_run_missing_object_prefix_status, backup_topology_mismatch_status,
    ensure_target_is_fresh_in, restore_cross_tenant_admin_required_status,
    restore_manifest_integrity_status,
};
use super::events::emit_event;
use super::model::{qualified_relation, run_location_from_json, run_summary_from_json, sha256_hex};
use super::store::{journal_run, journal_run_started, run_read_by_id};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RestoreColumnKey {
    schema: String,
    table: String,
    column: String,
}

type RestoreValueRemaps = HashMap<RestoreColumnKey, HashMap<String, String>>;

pub(crate) fn is_restore_journal_relation(schema: &str, table: &str) -> bool {
    let run_model = native_model(BACKUP_RUN_MSG, &["backup_id"]);
    qualified_relation(schema, table) == run_model.relation
}

fn manifest_table_by_relation<'a>(
    manifest: &'a CatalogManifest,
    schema: &str,
    table: &str,
) -> Option<&'a ManifestTable> {
    manifest
        .tables
        .iter()
        .find(|candidate| candidate.schema == schema && candidate.table == table)
}

fn manifest_column<'a>(table: &'a ManifestTable, column: &str) -> Option<&'a ManifestColumn> {
    table
        .columns
        .iter()
        .find(|candidate| candidate.column_name == column)
}

fn unique_restore_columns(table: &ManifestTable, tenant_column: &str) -> Vec<String> {
    let mut columns = Vec::new();
    let mut seen = HashSet::new();
    let fk_columns: HashSet<&str> = table
        .foreign_keys
        .iter()
        .flat_map(|fk| fk.columns.iter().map(String::as_str))
        .collect();
    let mut add_column = |column: &str| {
        if column != tenant_column
            && !fk_columns.contains(column)
            && seen.insert(column.to_string())
        {
            columns.push(column.to_string());
        }
    };

    if !table
        .primary_key
        .iter()
        .any(|column| column == tenant_column)
    {
        for column in &table.primary_key {
            add_column(column);
        }
    }
    for column in &table.columns {
        if column.unique {
            add_column(&column.column_name);
        }
    }
    for index in &table.indexes {
        if index.unique && !index.columns.iter().any(|column| column == tenant_column) {
            for column in &index.columns {
                add_column(column);
            }
        }
    }

    columns
}

fn restored_unique_value(
    column: Option<&ManifestColumn>,
    target_tenant_id: &str,
    old_value: &str,
) -> String {
    let sql_type = column
        .map(|column| column.sql_type.as_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if sql_type.contains("uuid") && Uuid::parse_str(old_value).is_ok() {
        return Uuid::new_v4().to_string();
    }

    let target = target_tenant_id.replace('-', "");
    let nonce = Uuid::new_v4().simple().to_string();
    let mut value = format!(
        "restored-{}-{}",
        &target[..12.min(target.len())],
        &nonce[..16]
    );
    if let Some(limit) = varchar_limit(&sql_type)
        && value.len() > limit
    {
        value.truncate(limit);
    }
    value
}

fn can_remap_unique_value(column: Option<&ManifestColumn>) -> bool {
    let sql_type = column
        .map(|column| column.sql_type.as_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    sql_type.contains("uuid")
        || sql_type.contains("char")
        || sql_type.contains("text")
        || sql_type.is_empty()
}

fn varchar_limit(sql_type: &str) -> Option<usize> {
    let open = sql_type.find('(')?;
    let close = sql_type[open + 1..].find(')')? + open + 1;
    sql_type[open + 1..close].trim().parse().ok()
}

fn apply_parent_restore_remaps(
    row: &mut serde_json::Map<String, serde_json::Value>,
    table: &ManifestTable,
    remaps: &RestoreValueRemaps,
) {
    for fk in &table.foreign_keys {
        for (column, ref_column) in fk.columns.iter().zip(fk.ref_columns.iter()) {
            let Some(serde_json::Value::String(old_value)) = row.get(column) else {
                continue;
            };
            let key = RestoreColumnKey {
                schema: fk.ref_schema.clone(),
                table: fk.ref_table.clone(),
                column: ref_column.clone(),
            };
            let Some(new_value) = remaps.get(&key).and_then(|values| values.get(old_value)) else {
                continue;
            };
            row.insert(column.clone(), serde_json::Value::String(new_value.clone()));
        }
    }
}

fn apply_cross_tenant_restore_remaps(
    row: &mut serde_json::Map<String, serde_json::Value>,
    table: &ManifestTable,
    tenant_column: &str,
    target_tenant_id: &str,
    remaps: &mut RestoreValueRemaps,
    extra_unique_columns: &[String],
) {
    let mut columns = unique_restore_columns(table, tenant_column);
    let mut seen: HashSet<String> = columns.iter().cloned().collect();
    for column in extra_unique_columns {
        if seen.insert(column.clone()) {
            columns.push(column.clone());
        }
    }
    for column in columns {
        let Some(serde_json::Value::String(old_value)) = row.get(&column) else {
            continue;
        };
        if old_value.is_empty() {
            continue;
        }
        let column_meta = manifest_column(table, &column);
        if !can_remap_unique_value(column_meta) {
            continue;
        }
        let key = RestoreColumnKey {
            schema: table.schema.clone(),
            table: table.table.clone(),
            column: column.clone(),
        };
        let values = remaps.entry(key).or_default();
        let new_value = values
            .entry(old_value.clone())
            .or_insert_with(|| restored_unique_value(column_meta, target_tenant_id, old_value))
            .clone();
        row.insert(column, serde_json::Value::String(new_value));
    }
}

async fn postgres_unique_restore_columns(
    pool: &PgPool,
    schema: &str,
    table: &str,
    tenant_column: &str,
    table_meta: &ManifestTable,
) -> Result<Vec<String>, Status> {
    let fk_columns: HashSet<&str> = table_meta
        .foreign_keys
        .iter()
        .flat_map(|fk| fk.columns.iter().map(String::as_str))
        .collect();
    let sql = r#"
        SELECT cols
        FROM (
          SELECT i.indexrelid, array_agg(a.attname::text ORDER BY k.ordinality) AS cols
          FROM pg_catalog.pg_index i
          JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ordinality) ON true
          JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
          WHERE i.indisunique
            AND n.nspname = $1
            AND c.relname = $2
            AND k.attnum > 0
          GROUP BY i.indexrelid
        ) unique_indexes
        WHERE NOT ($3 = ANY(cols))
    "#;
    let rows = sqlx::query(sql)
        .bind(schema)
        .bind(table)
        .bind(tenant_column)
        .fetch_all(pool)
        .await
        .map_err(|err| {
            backup_internal_status(
                "restore_unique_index_probe",
                format!("restore unique-index probe failed for {schema}.{table}: {err}"),
            )
        })?;
    let mut columns = Vec::new();
    let mut seen = HashSet::new();
    for row in rows {
        let cols: Vec<String> = row.try_get("cols").map_err(|err| {
            backup_internal_status(
                "restore_unique_index_probe",
                format!("restore unique-index row decode failed for {schema}.{table}: {err}"),
            )
        })?;
        for column in cols {
            if column != tenant_column
                && !fk_columns.contains(column.as_str())
                && seen.insert(column.clone())
            {
                columns.push(column);
            }
        }
    }
    Ok(columns)
}

pub(crate) async fn restore_tenant(
    svc: &BackupServiceImpl,
    request: Request<backup_pb::RestoreTenantRequest>,
) -> Result<Response<backup_pb::RestoreTenantResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let source_tenant_id = req.source_tenant_id.trim().to_string();
    let target_tenant_id = req.target_tenant_id.trim().to_string();
    let backup_id = req.backup_id.trim().to_string();
    if source_tenant_id.is_empty() || target_tenant_id.is_empty() || backup_id.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "source_tenant_id, target_tenant_id and backup_id are required",
            [
                ("source_tenant_id", "must be a non-empty source tenant id"),
                ("target_tenant_id", "must be a non-empty target tenant id"),
                ("backup_id", "must be a non-empty backup id"),
            ],
        ));
    }
    // A cross-tenant restore is any restore whose target differs from the source
    // OR whose caller explicitly asked to cross the boundary. It moves one
    // tenant's raw rows into another and its cross-tenant privilege is derived
    // from the caller's VALIDATED claim — a genuine cross-tenant / platform admin,
    // the only identity authorized over BOTH the source and the target tenant.
    // The wire `allow_cross_tenant` bool is a caller intent hint, NEVER the
    // authorization (that would let a tenant-scoped caller self-grant the move).
    let cross_tenant = req.allow_cross_tenant || source_tenant_id != target_tenant_id;
    let claim_present = crate::runtime::service::method_security::claim_context_present();
    let claim = crate::runtime::service::method_security::current_claim_context();
    let privileged_cross_tenant = if cross_tenant {
        // Over-the-wire requests always carry a validated claim; a caller that is
        // not a cross-tenant admin is DENIED fail-closed. (An in-process / loopback
        // caller carries no claim context and is not privileged either, so the
        // shared movement validator below still fails closed unless source ==
        // target.)
        if claim_present && !claim.is_cross_tenant_admin() {
            return Err(restore_cross_tenant_admin_required_status());
        }
        claim.is_cross_tenant_admin()
    } else {
        // Same-tenant restore: the body/header/claim tenant must match the target
        // exactly, so a tenant-A caller cannot smuggle tenant B in the body.
        validate_request_tenant(&metadata, &target_tenant_id)?;
        false
    };
    // DESTRUCTIVE: a missing confirmation token fails CLOSED.
    if req.confirmation_token.trim().is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "RestoreTenant overwrites a tenant's data; confirmation_token is required",
            [(
                "confirmation_token",
                "must be present to restore over tenant data",
            )],
        ));
    }

    // SHARED fail-closed movement validator — RestoreImport. The cross-tenant
    // privilege comes from the caller's CLAIM (above), never the wire bool, so a
    // move into a differing target still fails closed for a non-admin caller.
    let movement = TenantMovementRequest {
        operation: TenantMovementOperation::RestoreImport,
        tenant_id: &source_tenant_id,
        target_tenant_id: Some(&target_tenant_id),
        tenant_filter_present: true,
        privileged_cross_tenant,
    };
    validate_tenant_movement_scope(&movement)
        .map_err(|err| tenant_movement_policy_status(movement.operation, err))?;

    let runtime = svc.require_runtime()?;
    let _ = parse_uuid("source_tenant_id", &source_tenant_id)?;
    let _ = parse_uuid("target_tenant_id", &target_tenant_id)?;
    let mut context = project_scoped_native_service_context(&metadata, &target_tenant_id);
    let binding = svc.resolve_project_snapshot(&context.project_id)?;
    context.project_id = binding.project_id.clone();
    let pool = &binding.pool;
    let manifest = binding.manifest.as_ref();

    // Resolve the source run's object prefix from the durable journal.
    let mut source_ctx = project_scoped_native_service_context(&metadata, &source_tenant_id);
    source_ctx.project_id = binding.project_id.clone();
    let run_row = runtime
        .native_entity_read_for_service(
            "backup",
            &source_ctx,
            run_read_by_id(&source_tenant_id, &backup_id),
        )
        .await?
        .first()
        .cloned()
        .ok_or_else(|| {
            backup_not_found_status(
                "restore_tenant",
                "backup_run_not_found",
                "backup run not found for source tenant",
            )
        })?;
    let run = run_summary_from_json(&run_row);
    let location = run_location_from_json(&run_row)
        .ok_or_else(|| backup_run_location_missing_status("restore_tenant"))?;
    if location.project_id != binding.project_id {
        return Err(backup_topology_mismatch_status(
            "restore_tenant",
            format!(
                "backup belongs to project '{}' but restore resolved project '{}'",
                location.project_id, binding.project_id
            ),
        ));
    }
    if location.catalog_checksum != binding.catalog_checksum {
        return Err(backup_topology_mismatch_status(
            "restore_tenant",
            format!(
                "backup catalog checksum '{}' does not match active project catalog '{}'",
                location.catalog_checksum, binding.catalog_checksum
            ),
        ));
    }
    if location.postgres_instance != binding.postgres_instance {
        return Err(backup_topology_mismatch_status(
            "restore_tenant",
            format!(
                "backup canonical Postgres instance '{}' does not match restore instance '{}'",
                location.postgres_instance, binding.postgres_instance
            ),
        ));
    }
    let object_prefix = run.object_prefix.trim().to_string();
    if object_prefix.is_empty() {
        return Err(backup_run_missing_object_prefix_status());
    }

    // Enumerate the tenant-owned tables via the SHARED planner. The FRESH-target
    // guard (refuse to write over a live tenant) runs INSIDE the restore tx below
    // — a pre-tx probe races a concurrent write that lands between the check and
    // the inserts, so the authoritative emptiness check must share the tx.
    let plan = plan_tenant_purge(manifest);

    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "backup",
        OperationChannel::Admin,
        &target_tenant_id,
        None,
    )
    .await?;

    // Load + verify the run manifest from the immutable location persisted in
    // BackupRun before artifact writes. Legacy rows without that location fail
    // explicitly above; mutable process defaults are never used as a locator.
    let manifest_key = location.manifest_key.clone();
    let run_backend = location.object_backend.clone();
    let run_bucket = location.object_bucket.clone();
    let manifest_get = crate::runtime::core::setup_data::object_request_json(
        "get",
        &run_bucket,
        &manifest_key,
        "",
    );
    let manifest_bytes = runtime
        .get_object_backend_target_for_project(
            &run_backend,
            None,
            &context.project_id,
            &manifest_get,
        )
        .await?;
    // Integrity: the manifest is the anchor restore trusts before reading ANY
    // table artifact it lists. Verify its bytes against the checksum the backup
    // recorded in the durable journal (`run.manifest_checksum`) BEFORE trusting a
    // single entry. An empty recorded checksum (legacy or tampered journal row)
    // is a verification FAILURE, never a skip — fail closed.
    let recorded_manifest_checksum = run.manifest_checksum.trim();
    if recorded_manifest_checksum.is_empty()
        || sha256_hex(&manifest_bytes) != recorded_manifest_checksum
    {
        return Err(restore_manifest_integrity_status());
    }
    let manifest_value: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).map_err(|err| {
            backup_internal_status(
                "restore_manifest_parse",
                format!("restore manifest parse failed: {err}"),
            )
        })?;
    for (field, expected) in [
        ("project_id", binding.project_id.as_str()),
        ("catalog_checksum", binding.catalog_checksum.as_str()),
        ("postgres_instance", binding.postgres_instance.as_str()),
    ] {
        let actual = manifest_value
            .get(field)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if actual != expected {
            return Err(backup_topology_mismatch_status(
                "restore_tenant",
                format!(
                    "backup manifest {field} '{actual}' does not match resolved value '{expected}'"
                ),
            ));
        }
    }
    let object_backend = manifest_value
        .get("object_backend")
        .and_then(|v| v.as_str())
        .filter(|v| !v.trim().is_empty())
        .map(str::to_string)
        .unwrap_or(run_backend);
    let object_bucket = manifest_value
        .get("object_bucket")
        .and_then(|v| v.as_str())
        .filter(|v| !v.trim().is_empty())
        .map(str::to_string)
        .unwrap_or(run_bucket);
    let manifest_tables = manifest_value
        .get("tables")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // Restore in REVERSE planner order: the planner emits children→parents
    // (safe for delete); inserting parents→children satisfies FK constraints.
    let mut ordered_tables = manifest_tables;
    ordered_tables.reverse();

    let restore_id = Uuid::new_v4().to_string();
    let restore_metadata = serde_json::json!({
        "source_backup_id": backup_id,
        "object_backend": location.object_backend,
        "object_bucket": location.object_bucket,
        "manifest_key": location.manifest_key,
        "project_id": binding.project_id,
        "catalog_version": binding.catalog_version,
        "catalog_checksum": binding.catalog_checksum,
        "manifest_catalog_checksum": binding.manifest_checksum,
        "postgres_instance": binding.postgres_instance,
    });
    journal_run_started(
        runtime,
        &context,
        &restore_id,
        &target_tenant_id,
        KIND_RESTORE,
        &object_prefix,
        &restore_metadata,
    )
    .await?;

    let mut restored_rows: i64 = 0;
    let mut restored_table_count: i32 = 0;
    let cross_tenant_restore = source_tenant_id != target_tenant_id;
    let mut restore_remaps: RestoreValueRemaps = HashMap::new();
    let mut tx = pool.begin().await.map_err(|err| {
        backup_internal_status(
            "restore_transaction_begin",
            format!("failed to begin restore transaction: {err}"),
        )
    })?;

    // FRESH-target guard (authoritative): refuse to write over a live tenant, and
    // probe INSIDE the restore tx so a write that lands between an earlier check
    // and the inserts cannot slip a live tenant under the restore. Any existing
    // row aborts the tx (rolled back on the early return). Same tenant-owned table
    // set as the backup, now transactional.
    let mut existing_rows: u64 = 0;
    let mut occupied_relations: Vec<String> = Vec::new();
    for target in &plan.targets {
        // The freshness guard exists to refuse restoring OVER a live tenant's DATA.
        // Platform bookkeeping that the broker writes as a SIDE EFFECT of serving the
        // restore call itself is not tenant data: metering records a usage event for
        // the target tenant the moment the RPC is admitted, so an otherwise pristine
        // target always looked "live" and every cross-tenant restore was refused.
        // These relations are also not restored from the backup, so skipping them
        // cannot mask real data loss.
        if is_platform_bookkeeping_relation(&target.schema, &target.table) {
            continue;
        }
        let rel = qualified_relation(&target.schema, &target.table);
        // `journal_run_started` deliberately precedes the transaction so a failed
        // restore remains observable. Its target-scoped BackupRun is therefore the
        // one legitimate row an otherwise fresh target owns at this point. Exclude
        // only THIS restore id; an older backup/restore journal still proves the
        // target is not pristine and remains a refusal.
        let current_restore_journal = is_restore_journal_relation(&target.schema, &target.table);
        let probe_sql = if current_restore_journal {
            let run_model = native_model(BACKUP_RUN_MSG, &["backup_id"]);
            format!(
                "SELECT 1 FROM {rel} WHERE {col}::text = $1 AND {backup_id}::text <> $2 LIMIT 1",
                col = qi_runtime(&target.tenant_column),
                backup_id = run_model.q("backup_id"),
            )
        } else {
            format!(
                "SELECT 1 FROM {rel} WHERE {col}::text = $1 LIMIT 1",
                col = qi_runtime(&target.tenant_column),
            )
        };
        let present: Option<i32> = if current_restore_journal {
            sqlx::query_scalar(&probe_sql)
                .bind(&target_tenant_id)
                .bind(&restore_id)
                .fetch_optional(&mut *tx)
                .await
        } else {
            sqlx::query_scalar(&probe_sql)
                .bind(&target_tenant_id)
                .fetch_optional(&mut *tx)
                .await
        }
        .map_err(|err| {
            backup_internal_status(
                "restore_freshness_probe",
                format!(
                    "restore freshness probe failed on {}.{}: {err}",
                    target.schema, target.table
                ),
            )
        })?;
        if present.is_some() {
            existing_rows += 1;
            occupied_relations.push(rel.clone());
        }
    }
    ensure_target_is_fresh_in(existing_rows, &occupied_relations)?;

    for entry in &ordered_tables {
        let schema = entry.get("schema").and_then(|v| v.as_str()).unwrap_or("");
        let table = entry.get("table").and_then(|v| v.as_str()).unwrap_or("");
        let tenant_column = entry
            .get("tenant_column")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let object_key = entry
            .get("object_key")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let expected_checksum = entry
            .get("checksum_sha256")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if schema.is_empty() || table.is_empty() || object_key.is_empty() {
            continue;
        }
        let manifest_table = manifest_table_by_relation(manifest, schema, table);
        let db_unique_restore_columns = if cross_tenant_restore {
            match manifest_table {
                Some(table_meta) => {
                    postgres_unique_restore_columns(pool, schema, table, tenant_column, table_meta)
                        .await?
                }
                None => Vec::new(),
            }
        } else {
            Vec::new()
        };
        let get_req = crate::runtime::core::setup_data::object_request_json(
            "get",
            &object_bucket,
            object_key,
            "",
        );
        let bytes = runtime
            .get_object_backend_target_for_project(
                &object_backend,
                None,
                &context.project_id,
                &get_req,
            )
            .await?;
        // Integrity: the encrypted artifact must match the manifest checksum. An
        // empty expected checksum is a verification FAILURE (fail closed), never a
        // skip — a tampered manifest could blank it to bypass the check.
        if expected_checksum.is_empty() || sha256_hex(&bytes) != expected_checksum {
            return Err(Status::data_loss(format!(
                "restore integrity check failed for {schema}.{table} (checksum mismatch)"
            )));
        }
        let ciphertext = String::from_utf8(bytes).map_err(|err| {
            backup_internal_status(
                "restore_artifact_utf8",
                format!("restore artifact is not valid UTF-8: {err}"),
            )
        })?;
        let jsonl = runtime.decrypt_secret_at_rest(&ciphertext).map_err(|err| {
            backup_internal_status(
                "restore_decrypt_artifact",
                format!("restore decryption failed: {err}"),
            )
        })?;
        let rel = qualified_relation(schema, table);
        // `jsonb_populate_record` casts the row JSON into the table's row type,
        // so every column type is handled by Postgres without hand-mapping.
        let insert_sql =
            format!("INSERT INTO {rel} SELECT (jsonb_populate_record(NULL::{rel}, $1::jsonb)).*");
        for line in jsonl.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let mut value: serde_json::Value = serde_json::from_str(line).map_err(|err| {
                backup_internal_status(
                    "restore_row_parse",
                    format!("restore row parse failed for {schema}.{table}: {err}"),
                )
            })?;
            // Rewrite the tenant column to the target on insert.
            if let Some(obj) = value.as_object_mut()
                && !tenant_column.is_empty()
            {
                if cross_tenant_restore && let Some(table_meta) = manifest_table {
                    apply_parent_restore_remaps(obj, table_meta, &restore_remaps);
                }
                obj.insert(
                    tenant_column.to_string(),
                    serde_json::Value::String(target_tenant_id.clone()),
                );
                if cross_tenant_restore && let Some(table_meta) = manifest_table {
                    apply_cross_tenant_restore_remaps(
                        obj,
                        table_meta,
                        tenant_column,
                        &target_tenant_id,
                        &mut restore_remaps,
                        &db_unique_restore_columns,
                    );
                }
            }
            // Bind the row as text and cast to jsonb in SQL ($1::jsonb), so the
            // bind never depends on the sqlx json feature being enabled.
            let row_json = serde_json::to_string(&value).map_err(|err| {
                backup_internal_status(
                    "restore_row_reserialize",
                    format!("restore row reserialize failed: {err}"),
                )
            })?;
            sqlx::query(&insert_sql)
                .bind(row_json)
                .execute(&mut *tx)
                .await
                .map_err(|err| {
                    backup_internal_status(
                        "restore_insert_row",
                        format!("restore insert failed for {schema}.{table}: {err}"),
                    )
                })?;
            restored_rows += 1;
        }
        restored_table_count += 1;
    }
    tx.commit().await.map_err(|err| {
        backup_internal_status(
            "restore_transaction_commit",
            format!("failed to commit restore transaction: {err}"),
        )
    })?;

    journal_run(
        runtime,
        &context,
        &restore_id,
        &target_tenant_id,
        KIND_RESTORE,
        &object_prefix,
        &run.manifest_checksum,
        restored_table_count as i64,
        restored_rows,
        0,
        &source_tenant_id,
        &target_tenant_id,
        &restore_metadata,
    )
    .await?;

    emit_event(
        svc,
        TOPIC_BACKUP_RESTORED,
        &target_tenant_id,
        &target_tenant_id,
        &context.project_id,
        &restore_id,
        serde_json::json!({
            "backup_id": restore_id,
            "source_backup_id": backup_id,
            "source_tenant_id": source_tenant_id,
            "target_tenant_id": target_tenant_id,
            "object_prefix": object_prefix,
            "restored_table_count": restored_table_count,
            "restored_rows": restored_rows,
        }),
    )
    .await;

    Ok(Response::new(backup_pb::RestoreTenantResponse {
        backup_id: restore_id,
        source_object_prefix: object_prefix,
        restored_table_count,
        restored_rows,
        message: "tenant restored".to_string(),
        error: None,
    }))
}

/// Relations the broker writes as a side effect of serving a request, rather than
/// tenant-authored data. The restore freshness probe must ignore them: they are
/// populated by the restore call itself (metering admits and records usage before the
/// probe runs), and none of them is restored from a backup, so skipping them cannot
/// hide a live tenant's real rows.
fn is_platform_bookkeeping_relation(schema: &str, table: &str) -> bool {
    matches!(
        (schema.trim(), table.trim()),
        ("udb_metering", "usage_events")
    )
}
