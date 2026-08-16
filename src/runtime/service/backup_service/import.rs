//! The tenant restore IMPORT machinery for the native `BackupService`:
//! `restore_tenant` (shared movement gate, fresh-target guard, manifest load +
//! integrity verification, decrypt-at-rest, FK-ordered reinsert, cross-tenant
//! unique/foreign-key remapping, journal + outbox) plus the restore-remap model
//! and the Postgres unique-index probe it uses. Extracted verbatim from the
//! former god file — the SQL, crypto, and remap contracts are byte-for-byte
//! identical; `svc` replaces `&self`.

use std::collections::{HashMap, HashSet};

use sqlx::{PgConnection, Row};
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

type RestoreValueRemaps = HashMap<RestoreColumnKey, HashMap<String, serde_json::Value>>;

#[derive(Debug, Clone, PartialEq, Eq)]
enum RestoreRemapAuthority {
    GeneratedText {
        uuid: bool,
        max_len: Option<usize>,
    },
    OwnedSequence(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RestoreColumnRemapPlan {
    column: String,
    authority: RestoreRemapAuthority,
    /// Empty means every restored row. Otherwise the column is remapped only
    /// when at least one exact live/manifest partial-index predicate is true.
    predicates: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct PreparedRestoreValue {
    key: RestoreColumnKey,
    old_value_key: String,
    new_value: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UniqueRestoreKey {
    name: String,
    columns: Vec<String>,
    has_expressions: bool,
    predicate: Option<String>,
    nulls_not_distinct: bool,
}

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

pub(crate) fn scope_restore_row_project(
    row: &mut serde_json::Map<String, serde_json::Value>,
    project_column: &str,
    project_id: &str,
    schema: &str,
    table: &str,
) -> Result<(), Status> {
    if project_column.is_empty() {
        return Ok(());
    }
    let source_project = row
        .get(project_column)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if source_project != project_id {
        return Err(backup_topology_mismatch_status(
            "restore_tenant",
            format!(
                "backup row project '{source_project}' for {schema}.{table} does not match restore project '{project_id}'"
            ),
        ));
    }
    row.insert(
        project_column.to_string(),
        serde_json::Value::String(project_id.to_string()),
    );
    Ok(())
}

fn parse_scoped_restore_row(
    line: &str,
    schema: &str,
    table: &str,
    tenant_column: &str,
    target_tenant_id: &str,
    project_column: &str,
    project_id: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, Status> {
    let value: serde_json::Value = serde_json::from_str(line).map_err(|err| {
        backup_internal_status(
            "restore_row_parse",
            format!("restore row parse failed for {schema}.{table}: {err}"),
        )
    })?;
    let mut row = value.as_object().cloned().ok_or_else(|| {
        backup_topology_mismatch_status(
            "restore_tenant",
            format!("restore row for {schema}.{table} is not a JSON object"),
        )
    })?;
    scope_restore_row_project(&mut row, project_column, project_id, schema, table)?;
    if !tenant_column.is_empty() {
        row.insert(
            tenant_column.to_string(),
            serde_json::Value::String(target_tenant_id.to_string()),
        );
    }
    Ok(row)
}

fn manifest_unique_restore_keys(table: &ManifestTable) -> Vec<UniqueRestoreKey> {
    let mut keys = Vec::new();
    if !table.primary_key.is_empty() {
        keys.push(UniqueRestoreKey {
            name: format!("manifest:{}_pkey", table.table),
            columns: table.primary_key.clone(),
            has_expressions: false,
            predicate: None,
            nulls_not_distinct: false,
        });
    }
    for column in &table.columns {
        if column.unique {
            keys.push(UniqueRestoreKey {
                name: format!("manifest:{}_{}_key", table.table, column.column_name),
                columns: vec![column.column_name.clone()],
                has_expressions: false,
                predicate: None,
                nulls_not_distinct: false,
            });
        }
    }
    for (position, index) in table.indexes.iter().enumerate() {
        if index.unique {
            keys.push(UniqueRestoreKey {
                name: if index.name.trim().is_empty() {
                    format!("manifest:{}_unique_{position}", table.table)
                } else {
                    format!("manifest:{}", index.name.trim())
                },
                columns: index.columns.clone(),
                has_expressions: false,
                predicate: (!index.where_clause.trim().is_empty())
                    .then(|| index.where_clause.trim().to_string()),
                nulls_not_distinct: false,
            });
        }
    }
    keys
}

const MIN_BOUNDED_TEXT_RESTORE_LENGTH: usize = 33;
const MAX_RESTORE_REMAP_PLANS_PER_TABLE: usize = 256;
const MAX_PARTIAL_RESTORE_PREDICATE_ITERATIONS: usize = 64;

fn restored_unique_value(
    authority: &RestoreRemapAuthority,
    target_tenant_id: &str,
) -> String {
    let RestoreRemapAuthority::GeneratedText { uuid, max_len } = authority else {
        unreachable!("text restore values require generated-text authority");
    };
    if *uuid {
        return Uuid::new_v4().to_string();
    }

    if max_len.is_some() {
        // A bounded text authority is accepted only when all 128 random bits fit;
        // retain an alphabetic prefix so maintained identifiers such as User.username
        // keep their lexical CHECK while never truncating the uniqueness suffix.
        return format!("r{}", Uuid::new_v4().simple());
    }
    let target = target_tenant_id.replace('-', "");
    let nonce = Uuid::new_v4().simple().to_string();
    format!(
        "restored-{}-{}",
        &target[..12.min(target.len())],
        &nonce[..16]
    )
}

fn varchar_limit(sql_type: &str) -> Option<usize> {
    let open = sql_type.find('(')?;
    let close = sql_type[open + 1..].find(')')? + open + 1;
    sql_type[open + 1..close].trim().parse().ok()
}

fn restore_value_key(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value) => Some(format!("s:{value}")),
        serde_json::Value::Number(value) => Some(format!("n:{value}")),
        _ => None,
    }
}

fn restore_remap_authority(
    schema: &str,
    table: &str,
    column: &str,
    data_type: &str,
    sequence_name: Option<String>,
) -> Result<RestoreRemapAuthority, Status> {
    let normalized_type = data_type.trim().to_ascii_lowercase();
    if normalized_type == "uuid" {
        return Ok(RestoreRemapAuthority::GeneratedText {
            uuid: true,
            max_len: None,
        });
    }
    if normalized_type.contains("text") || normalized_type.contains("char") {
        let max_len = varchar_limit(&normalized_type);
        if let Some(limit) = max_len
            && limit < MIN_BOUNDED_TEXT_RESTORE_LENGTH
        {
            return Err(backup_topology_mismatch_status(
                "restore_tenant",
                format!(
                    "unique text column {schema}.{table}.{column} has bounded width {limit}, below the {MIN_BOUNDED_TEXT_RESTORE_LENGTH}-character collision-safe restore authority"
                ),
            ));
        }
        return Ok(RestoreRemapAuthority::GeneratedText {
            uuid: false,
            max_len,
        });
    }
    if matches!(normalized_type.as_str(), "smallint" | "integer" | "bigint") {
        return sequence_name
            .map(RestoreRemapAuthority::OwnedSequence)
            .ok_or_else(|| {
                backup_topology_mismatch_status(
                    "restore_tenant",
                    format!(
                        "numeric unique column {schema}.{table}.{column} has no trusted owned serial/identity sequence"
                    ),
                )
            });
    }
    Err(backup_topology_mismatch_status(
        "restore_tenant",
        format!(
            "unique column {schema}.{table}.{column} uses unsupported restore type '{data_type}'"
        ),
    ))
}

fn apply_parent_restore_remaps(
    row: &mut serde_json::Map<String, serde_json::Value>,
    table: &ManifestTable,
    remaps: &RestoreValueRemaps,
    expected_remaps: &HashSet<RestoreColumnKey>,
    restore_relation_positions: &HashMap<(String, String), usize>,
    current_position: usize,
) -> Result<(), Status> {
    for fk in &table.foreign_keys {
        for (column, ref_column) in fk.columns.iter().zip(fk.ref_columns.iter()) {
            let Some(old_value) = row.get(column) else {
                continue;
            };
            if old_value.is_null() {
                continue;
            }
            let key = RestoreColumnKey {
                schema: fk.ref_schema.clone(),
                table: fk.ref_table.clone(),
                column: ref_column.clone(),
            };
            let Some(values) = remaps.get(&key) else {
                if expected_remaps.contains(&key) {
                    return Err(backup_topology_mismatch_status(
                        "restore_tenant",
                        format!(
                            "foreign-key restore value for {}.{}.{} requires a planned parent remap that was not preallocated",
                            table.schema, table.table, column
                        ),
                    ));
                }
                if restore_relation_positions
                    .get(&(fk.ref_schema.clone(), fk.ref_table.clone()))
                    .is_some_and(|position| *position > current_position)
                {
                    return Err(backup_topology_mismatch_status(
                        "restore_tenant",
                        format!(
                            "foreign-key restore value for {}.{}.{} references {}.{} later in restore order",
                            table.schema,
                            table.table,
                            column,
                            fk.ref_schema,
                            fk.ref_table
                        ),
                    ));
                }
                continue;
            };
            let old_value_key = restore_value_key(old_value).ok_or_else(|| {
                backup_topology_mismatch_status(
                    "restore_tenant",
                    format!(
                        "foreign-key restore value for {}.{}.{} is not a supported scalar",
                        table.schema, table.table, column
                    ),
                )
            })?;
            let new_value = values.get(&old_value_key).ok_or_else(|| {
                backup_topology_mismatch_status(
                    "restore_tenant",
                    format!(
                        "foreign-key restore value for {}.{}.{} has no exact parent remap",
                        table.schema, table.table, column
                    ),
                )
            })?;
            row.insert(column.clone(), new_value.clone());
        }
    }
    Ok(())
}

fn foreign_key_remap_available(
    table: &ManifestTable,
    column: &str,
    remaps: &RestoreValueRemaps,
    expected_remaps: &HashSet<RestoreColumnKey>,
    current_plans: &[RestoreColumnRemapPlan],
) -> bool {
    table.foreign_keys.iter().any(|fk| {
        fk.columns
            .iter()
            .zip(fk.ref_columns.iter())
            .any(|(local, referenced)| {
                if local == column {
                    let key = RestoreColumnKey {
                        schema: fk.ref_schema.clone(),
                        table: fk.ref_table.clone(),
                        column: referenced.clone(),
                    };
                    remaps.contains_key(&key)
                        || expected_remaps.contains(&key)
                        || (fk.ref_schema == table.schema
                            && fk.ref_table == table.table
                            && current_plans.iter().any(|plan| {
                                plan.column.as_str() == referenced.as_str()
                                    && plan.predicates.is_empty()
                            }))
                } else {
                    false
                }
            })
    })
}

async fn restore_plan_states(
    conn: &mut PgConnection,
    row: &serde_json::Map<String, serde_json::Value>,
    table: &ManifestTable,
    plans: &[RestoreColumnRemapPlan],
    prepared: &[Option<PreparedRestoreValue>],
) -> Result<Vec<bool>, Status> {
    if plans.is_empty() {
        return Ok(Vec::new());
    }
    let expressions = plans
        .iter()
        .zip(prepared.iter())
        .map(|(plan, prepared)| {
            if prepared.is_none() {
                "FALSE".to_string()
            } else if plan.predicates.is_empty() {
                "TRUE".to_string()
            } else {
                format!(
                    "COALESCE(({}), FALSE)",
                    plan.predicates
                        .iter()
                        .map(|predicate| format!("({predicate})"))
                        .collect::<Vec<_>>()
                        .join(" OR ")
                )
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let relation = qualified_relation(&table.schema, &table.table);
    let sql = format!(
        "SELECT ARRAY[{expressions}]::boolean[] FROM jsonb_populate_record(NULL::{relation}, $1::jsonb)"
    );
    let row_json = serde_json::to_string(row).map_err(|err| {
        backup_internal_status(
            "restore_partial_unique_predicate",
            format!(
                "failed to serialize restore row for partial unique-index evaluation on {}.{}: {err}",
                table.schema, table.table
            ),
        )
    })?;
    let states: Vec<bool> = sqlx::query_scalar(&sql)
        .bind(row_json)
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| {
            backup_topology_mismatch_status(
                "restore_tenant",
                format!(
                    "partial unique-index predicate could not be evaluated for {}.{}: {err}",
                    table.schema, table.table
                ),
            )
        })?;
    if states.len() != plans.len() {
        return Err(backup_internal_status(
            "restore_partial_unique_predicate",
            format!(
                "partial unique-index evaluation for {}.{} returned {} states for {} plans",
                table.schema,
                table.table,
                states.len(),
                plans.len()
            ),
        ));
    }
    Ok(states)
}

fn restore_candidate_row(
    original: &serde_json::Map<String, serde_json::Value>,
    plans: &[RestoreColumnRemapPlan],
    prepared: &[Option<PreparedRestoreValue>],
    applied: &[bool],
) -> serde_json::Map<String, serde_json::Value> {
    let mut candidate = original.clone();
    for (position, value) in prepared.iter().enumerate() {
        if applied[position]
            && let Some(value) = value
        {
            candidate.insert(plans[position].column.clone(), value.new_value.clone());
        }
    }
    candidate
}

async fn prepare_restore_value(
    conn: &mut PgConnection,
    row: &serde_json::Map<String, serde_json::Value>,
    table: &ManifestTable,
    target_tenant_id: &str,
    remaps: &RestoreValueRemaps,
    plan: &RestoreColumnRemapPlan,
) -> Result<Option<PreparedRestoreValue>, Status> {
    let Some(old_value) = row.get(&plan.column) else {
        if manifest_column(table, &plan.column).is_some_and(|column| column.not_null) {
            return Err(backup_topology_mismatch_status(
                "restore_tenant",
                format!(
                    "restore row for {}.{} omits non-null unique column {}",
                    table.schema, table.table, plan.column
                ),
            ));
        }
        return Ok(None);
    };
    if old_value.is_null() {
        if manifest_column(table, &plan.column).is_some_and(|column| column.not_null) {
            return Err(backup_topology_mismatch_status(
                "restore_tenant",
                format!(
                    "restore row for {}.{} has null in non-null unique column {}",
                    table.schema, table.table, plan.column
                ),
            ));
        }
        return Ok(None);
    }
    let old_value_key = restore_value_key(old_value).ok_or_else(|| {
        backup_topology_mismatch_status(
            "restore_tenant",
            format!(
                "unique restore value for {}.{}.{} is not a supported scalar",
                table.schema, table.table, plan.column
            ),
        )
    })?;
    let key = RestoreColumnKey {
        schema: table.schema.clone(),
        table: table.table.clone(),
        column: plan.column.clone(),
    };
    let existing_values = remaps.get(&key);
    let new_value = if let Some(value) =
        existing_values.and_then(|values| values.get(&old_value_key))
    {
        value.clone()
    } else {
        match &plan.authority {
            RestoreRemapAuthority::GeneratedText { .. } => {
                old_value.as_str().ok_or_else(|| {
                    backup_topology_mismatch_status(
                        "restore_tenant",
                        format!(
                            "text restore authority for {}.{}.{} received a non-text value",
                            table.schema, table.table, plan.column
                        ),
                    )
                })?;
                let mut generated = restored_unique_value(&plan.authority, target_tenant_id);
                while existing_values.is_some_and(|values| {
                    values
                        .values()
                        .any(|value| value.as_str() == Some(&generated))
                }) {
                    generated = restored_unique_value(&plan.authority, target_tenant_id);
                }
                serde_json::Value::String(generated)
            }
            RestoreRemapAuthority::OwnedSequence(sequence) => {
                if !old_value.is_number() {
                    return Err(backup_topology_mismatch_status(
                        "restore_tenant",
                        format!(
                            "numeric restore authority for {}.{}.{} received a non-numeric value",
                            table.schema, table.table, plan.column
                        ),
                    ));
                }
                let next_value: i64 =
                    sqlx::query_scalar("SELECT nextval($1::text::regclass)::bigint")
                        .bind(sequence)
                        .fetch_one(&mut *conn)
                        .await
                        .map_err(|err| {
                            backup_internal_status(
                                "restore_identity_allocate",
                                format!(
                                    "failed to allocate restore identity for {}.{}.{}: {err}",
                                    table.schema, table.table, plan.column
                                ),
                            )
                        })?;
                serde_json::Value::Number(next_value.into())
            }
        }
    };
    Ok(Some(PreparedRestoreValue {
        key,
        old_value_key,
        new_value,
    }))
}

async fn preallocate_unconditional_restore_row(
    conn: &mut PgConnection,
    row: &serde_json::Map<String, serde_json::Value>,
    table: &ManifestTable,
    target_tenant_id: &str,
    remaps: &mut RestoreValueRemaps,
    plans: &[RestoreColumnRemapPlan],
) -> Result<(), Status> {
    for plan in plans.iter().filter(|plan| plan.predicates.is_empty()) {
        if let Some(value) =
            prepare_restore_value(conn, row, table, target_tenant_id, remaps, plan).await?
        {
            remaps
                .entry(value.key)
                .or_default()
                .entry(value.old_value_key)
                .or_insert(value.new_value);
        }
    }
    Ok(())
}

fn defer_self_reference_values(
    row: &mut serde_json::Map<String, serde_json::Value>,
    table: &ManifestTable,
    remaps: &RestoreValueRemaps,
) -> Result<Option<String>, Status> {
    let mut self_columns: Vec<&str> = table
        .foreign_keys
        .iter()
        .filter(|fk| fk.ref_schema == table.schema && fk.ref_table == table.table)
        .flat_map(|fk| fk.columns.iter().zip(fk.ref_columns.iter()))
        .filter(|(_, referenced)| {
            remaps.contains_key(&RestoreColumnKey {
                schema: table.schema.clone(),
                table: table.table.clone(),
                column: (*referenced).clone(),
            })
        })
        .map(|(local, _)| local.as_str())
        .filter(|local| row.get(*local).is_some_and(|value| !value.is_null()))
        .collect();
    self_columns.sort_unstable();
    self_columns.dedup();
    if self_columns.is_empty() {
        return Ok(None);
    }
    if table.primary_key.is_empty() {
        return Err(backup_topology_mismatch_status(
            "restore_tenant",
            format!(
                "self-referencing restore table {}.{} has no primary key for deferred binding",
                table.schema, table.table
            ),
        ));
    }
    for column in &self_columns {
        if manifest_column(table, column).is_some_and(|column| column.not_null) {
            return Err(backup_topology_mismatch_status(
                "restore_tenant",
                format!(
                    "self-referencing restore column {}.{}.{} is non-null and cannot be safely deferred",
                    table.schema, table.table, column
                ),
            ));
        }
    }
    let final_row = serde_json::to_string(row).map_err(|err| {
        backup_internal_status(
            "restore_self_reference_defer",
            format!(
                "failed to serialize self-referencing restore row for {}.{}: {err}",
                table.schema, table.table
            ),
        )
    })?;
    for column in self_columns {
        row.insert(column.to_string(), serde_json::Value::Null);
    }
    Ok(Some(final_row))
}

async fn apply_deferred_self_references(
    conn: &mut PgConnection,
    table: &ManifestTable,
    rows: &[String],
) -> Result<(), Status> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut self_columns: Vec<&str> = table
        .foreign_keys
        .iter()
        .filter(|fk| fk.ref_schema == table.schema && fk.ref_table == table.table)
        .flat_map(|fk| fk.columns.iter().map(String::as_str))
        .collect();
    self_columns.sort_unstable();
    self_columns.dedup();
    let relation = qualified_relation(&table.schema, &table.table);
    let assignments = self_columns
        .iter()
        .map(|column| {
            let column = qi_runtime(column);
            format!("{column} = source.{column}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let identity = table
        .primary_key
        .iter()
        .map(|column| {
            let column = qi_runtime(column);
            format!("target.{column} IS NOT DISTINCT FROM source.{column}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "UPDATE {relation} AS target SET {assignments} FROM (SELECT (jsonb_populate_record(NULL::{relation}, $1::jsonb)).*) AS source WHERE {identity}"
    );
    for row in rows {
        let updated = sqlx::query(&sql)
            .bind(row)
            .execute(&mut *conn)
            .await
            .map_err(|err| {
                backup_internal_status(
                    "restore_self_reference_bind",
                    format!(
                        "failed to bind deferred self-reference for {}.{}: {err}",
                        table.schema, table.table
                    ),
                )
            })?;
        if updated.rows_affected() != 1 {
            return Err(backup_topology_mismatch_status(
                "restore_tenant",
                format!(
                    "deferred self-reference for {}.{} matched {} rows instead of one",
                    table.schema,
                    table.table,
                    updated.rows_affected()
                ),
            ));
        }
    }
    Ok(())
}

async fn apply_cross_tenant_restore_remaps(
    conn: &mut PgConnection,
    row: &mut serde_json::Map<String, serde_json::Value>,
    table: &ManifestTable,
    target_tenant_id: &str,
    remaps: &mut RestoreValueRemaps,
    plans: &[RestoreColumnRemapPlan],
) -> Result<(), Status> {
    let original = row.clone();
    let mut prepared = Vec::with_capacity(plans.len());
    for plan in plans {
        prepared.push(
            prepare_restore_value(conn, &original, table, target_tenant_id, remaps, plan).await?,
        );
    }

    // Resolve partial-index membership against a stable candidate row. Starting
    // with unconditional plans yields the least fixed point: rows outside a
    // partial key stay unchanged unless another required remap makes the final
    // row enter that predicate. Oscillating predicates are rejected rather than
    // depending on manifest/pg_index iteration order.
    let mut applied: Vec<bool> = plans
        .iter()
        .zip(prepared.iter())
        .map(|(plan, prepared)| plan.predicates.is_empty() && prepared.is_some())
        .collect();
    let mut seen_states = HashSet::new();
    for _ in 0..MAX_PARTIAL_RESTORE_PREDICATE_ITERATIONS {
        if !seen_states.insert(applied.clone()) {
            return Err(backup_topology_mismatch_status(
                "restore_tenant",
                format!(
                    "partial unique-index predicates for {}.{} do not converge on one restore row",
                    table.schema, table.table
                ),
            ));
        }
        let candidate = restore_candidate_row(&original, plans, &prepared, &applied);
        let next = restore_plan_states(conn, &candidate, table, plans, &prepared).await?;
        if next == applied {
            *row = candidate;
            for (position, value) in prepared.into_iter().enumerate() {
                if applied[position]
                    && let Some(value) = value
                {
                    remaps
                        .entry(value.key)
                        .or_default()
                        .entry(value.old_value_key)
                        .or_insert(value.new_value);
                }
            }
            return Ok(());
        }
        applied = next;
    }
    Err(backup_topology_mismatch_status(
        "restore_tenant",
        format!(
            "partial unique-index predicates for {}.{} exceeded the bounded convergence limit of {MAX_PARTIAL_RESTORE_PREDICATE_ITERATIONS}",
            table.schema, table.table
        ),
    ))
}

fn plan_protects_unique_key(
    plans: &[RestoreColumnRemapPlan],
    column: &str,
    predicate: Option<&str>,
) -> bool {
    plans.iter().any(|plan| {
        plan.column == column
            && (plan.predicates.is_empty()
                || predicate.is_some_and(|predicate| {
                    plan.predicates
                        .iter()
                        .any(|planned| planned == predicate)
                }))
    })
}

fn add_restore_remap_plan(
    plans: &mut Vec<RestoreColumnRemapPlan>,
    column: String,
    authority: RestoreRemapAuthority,
    predicate: Option<String>,
) {
    if let Some(plan) = plans.iter_mut().find(|plan| plan.column == column) {
        if predicate.is_none() {
            plan.predicates.clear();
        } else if !plan.predicates.is_empty() {
            let predicate = predicate.expect("checked partial predicate");
            if !plan.predicates.contains(&predicate) {
                plan.predicates.push(predicate);
            }
        }
        return;
    }
    plans.push(RestoreColumnRemapPlan {
        column,
        authority,
        predicates: predicate.into_iter().collect(),
    });
}

async fn postgres_live_unique_restore_keys(
    conn: &mut PgConnection,
    schema: &str,
    table: &str,
) -> Result<Vec<UniqueRestoreKey>, Status> {
    let sql = r#"
        SELECT idx.relname::text AS index_name,
               COALESCE(
                 array_agg(a.attname::text ORDER BY k.ordinality)
                   FILTER (WHERE k.attnum > 0),
                 ARRAY[]::text[]
               ) AS cols,
               bool_or(k.attnum = 0) AS has_expressions,
               pg_get_expr(i.indpred, i.indrelid, true) AS predicate,
               COALESCE(
                 (to_jsonb(i)->>'indnullsnotdistinct')::boolean,
                 false
               ) AS nulls_not_distinct
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
        JOIN pg_catalog.pg_class idx ON idx.oid = i.indexrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ordinality) ON true
        LEFT JOIN pg_catalog.pg_attribute a
          ON a.attrelid = c.oid AND a.attnum = k.attnum AND k.attnum > 0
        WHERE i.indisunique
          AND i.indisvalid
          AND i.indisready
          AND n.nspname = $1
          AND c.relname = $2
          AND k.ordinality <= i.indnkeyatts
        GROUP BY i.indexrelid, idx.relname, i.indpred, i.indrelid,
                 COALESCE(
                   (to_jsonb(i)->>'indnullsnotdistinct')::boolean,
                   false
                 )
    "#;
    let rows = sqlx::query(sql)
        .bind(schema)
        .bind(table)
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| {
            backup_internal_status(
                "restore_unique_index_probe",
                format!("restore unique-index probe failed for {schema}.{table}: {err}"),
            )
        })?;
    let mut unique_indexes = Vec::with_capacity(rows.len());
    for row in rows {
        let index_name: String = row.try_get("index_name").map_err(|err| {
            backup_internal_status(
                "restore_unique_index_probe",
                format!("restore unique-index name decode failed for {schema}.{table}: {err}"),
            )
        })?;
        let cols: Vec<String> = row.try_get("cols").map_err(|err| {
            backup_internal_status(
                "restore_unique_index_probe",
                format!("restore unique-index row decode failed for {schema}.{table}: {err}"),
            )
        })?;
        let has_expressions: bool = row.try_get("has_expressions").map_err(|err| {
            backup_internal_status(
                "restore_unique_index_probe",
                format!(
                    "restore unique-index expression decode failed for {schema}.{table}: {err}"
                ),
            )
        })?;
        let predicate: Option<String> = row.try_get("predicate").map_err(|err| {
            backup_internal_status(
                "restore_unique_index_probe",
                format!(
                    "restore unique-index predicate decode failed for {schema}.{table}: {err}"
                ),
            )
        })?;
        let nulls_not_distinct: bool = row.try_get("nulls_not_distinct").map_err(|err| {
            backup_internal_status(
                "restore_unique_index_probe",
                format!(
                    "restore unique-index null policy decode failed for {schema}.{table}: {err}"
                ),
            )
        })?;
        unique_indexes.push(UniqueRestoreKey {
            name: format!("live:{index_name}"),
            columns: cols,
            has_expressions,
            predicate: predicate.and_then(|predicate| {
                let predicate = predicate.trim().to_string();
                (!predicate.is_empty()).then_some(predicate)
            }),
            nulls_not_distinct,
        });
    }
    Ok(unique_indexes)
}

async fn postgres_restore_remap_authority(
    conn: &mut PgConnection,
    schema: &str,
    table: &str,
    column: &str,
) -> Result<RestoreRemapAuthority, Status> {
    let authority_sql = r#"
        SELECT format_type(a.atttypid, a.atttypmod) AS data_type,
               owned.sequence_name
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
        LEFT JOIN LATERAL (
          SELECT format('%I.%I', seq_ns.nspname, seq.relname) AS sequence_name
          FROM pg_catalog.pg_depend d
          JOIN pg_catalog.pg_class seq ON seq.oid = d.objid AND seq.relkind = 'S'
          JOIN pg_catalog.pg_namespace seq_ns ON seq_ns.oid = seq.relnamespace
          WHERE d.classid = 'pg_class'::regclass
            AND d.refclassid = 'pg_class'::regclass
            AND d.refobjid = c.oid
            AND d.refobjsubid = a.attnum
            AND d.deptype IN ('a', 'i')
          ORDER BY CASE d.deptype WHEN 'i' THEN 0 ELSE 1 END
          LIMIT 1
        ) owned ON true
        WHERE n.nspname = $1
          AND c.relname = $2
          AND c.relkind IN ('r', 'p')
          AND a.attname = $3
          AND a.attnum > 0
          AND NOT a.attisdropped
    "#;
    let authority = sqlx::query(authority_sql)
        .bind(schema)
        .bind(table)
        .bind(column)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| {
            backup_internal_status(
                "restore_identity_authority_probe",
                format!(
                    "restore identity-authority probe failed for {schema}.{table}.{column}: {err}"
                ),
            )
        })?
        .ok_or_else(|| {
            backup_topology_mismatch_status(
                "restore_tenant",
                format!("live unique column {schema}.{table}.{column} could not be resolved"),
            )
        })?;
    let data_type: String = authority.try_get("data_type").map_err(|err| {
        backup_internal_status(
            "restore_identity_authority_probe",
            format!(
                "restore identity-authority type decode failed for {schema}.{table}.{column}: {err}"
            ),
        )
    })?;
    let sequence_name: Option<String> = authority.try_get("sequence_name").map_err(|err| {
        backup_internal_status(
            "restore_identity_authority_probe",
            format!(
                "restore identity-authority sequence decode failed for {schema}.{table}.{column}: {err}"
            ),
        )
    })?;
    restore_remap_authority(schema, table, column, &data_type, sequence_name)
}

async fn postgres_restore_remap_plans(
    conn: &mut PgConnection,
    schema: &str,
    table: &str,
    tenant_column: &str,
    table_meta: &ManifestTable,
    remaps: &RestoreValueRemaps,
    expected_remaps: &HashSet<RestoreColumnKey>,
) -> Result<Vec<RestoreColumnRemapPlan>, Status> {
    let mut unique_keys = manifest_unique_restore_keys(table_meta);
    unique_keys.extend(postgres_live_unique_restore_keys(conn, schema, table).await?);
    let fk_columns: HashSet<&str> = table_meta
        .foreign_keys
        .iter()
        .flat_map(|fk| fk.columns.iter().map(String::as_str))
        .collect();
    let project_column = crate::generation::sql::resolve_project_column(table_meta);
    let mut plans = Vec::new();

    for key in unique_keys {
        if key.columns.iter().any(|column| column == tenant_column) {
            continue;
        }
        let predicate = key.predicate.as_deref();
        if key.columns.iter().any(|column| {
            let guaranteed_non_null = manifest_column(table_meta, column)
                .is_some_and(|column| column.not_null);
            (!key.nulls_not_distinct || guaranteed_non_null)
                && (plan_protects_unique_key(&plans, column, predicate)
                    || foreign_key_remap_available(
                        table_meta,
                        column,
                        remaps,
                        expected_remaps,
                        &plans,
                    ))
        }) {
            continue;
        }

        let mut last_refusal = None;
        let mut selected = None;
        for column in &key.columns {
            if column == tenant_column
                || project_column == Some(column.as_str())
                || fk_columns.contains(column.as_str())
            {
                continue;
            }
            if manifest_column(table_meta, column).is_none() {
                last_refusal = Some(backup_topology_mismatch_status(
                    "restore_tenant",
                    format!(
                        "unique key {} references live column {schema}.{table}.{column} absent from the active catalog",
                        key.name
                    ),
                ));
                continue;
            }
            if key.nulls_not_distinct
                && !manifest_column(table_meta, column).is_some_and(|column| column.not_null)
            {
                last_refusal = Some(backup_topology_mismatch_status(
                    "restore_tenant",
                    format!(
                        "unique key {} uses NULLS NOT DISTINCT but candidate {schema}.{table}.{column} is nullable",
                        key.name
                    ),
                ));
                continue;
            }
            match postgres_restore_remap_authority(conn, schema, table, column).await {
                Ok(authority) => {
                    selected = Some((column.clone(), authority));
                    break;
                }
                Err(refusal) if refusal.code() == tonic::Code::Internal => return Err(refusal),
                Err(refusal) => last_refusal = Some(refusal),
            }
        }

        if let Some((column, authority)) = selected {
            add_restore_remap_plan(&mut plans, column, authority, key.predicate);
            if plans.len() > MAX_RESTORE_REMAP_PLANS_PER_TABLE {
                return Err(backup_topology_mismatch_status(
                    "restore_tenant",
                    format!(
                        "restore planning for {schema}.{table} exceeded the bounded limit of {MAX_RESTORE_REMAP_PLANS_PER_TABLE} remap columns"
                    ),
                ));
            }
            continue;
        }
        if key.has_expressions {
            return Err(backup_topology_mismatch_status(
                "restore_tenant",
                format!(
                    "unique key {} on {schema}.{table} contains an unprotected expression key",
                    key.name
                ),
            ));
        }
        return Err(last_refusal.unwrap_or_else(|| {
            backup_topology_mismatch_status(
                "restore_tenant",
                format!(
                    "unique key {} on {schema}.{table} has no trusted remap authority",
                    key.name
                ),
            )
        }));
    }
    Ok(plans)
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
            run_read_by_id(&source_tenant_id, &binding.project_id, &backup_id),
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
    if run.project_id != binding.project_id {
        return Err(backup_topology_mismatch_status(
            "restore_tenant",
            "backup run project does not match the active restore project",
        ));
    }
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
    let restore_relation_positions: HashMap<(String, String), usize> = ordered_tables
        .iter()
        .enumerate()
        .filter_map(|(position, entry)| {
            Some((
                (
                    entry.get("schema")?.as_str()?.to_string(),
                    entry.get("table")?.as_str()?.to_string(),
                ),
                position,
            ))
        })
        .collect();

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
        &binding.project_id,
        KIND_RESTORE,
        &object_prefix,
        &restore_metadata,
    )
    .await?;

    let mut restored_rows: i64 = 0;
    let mut restored_table_count: i32 = 0;
    let cross_tenant_restore = source_tenant_id != target_tenant_id;
    let mut restore_remaps: RestoreValueRemaps = HashMap::new();
    let mut expected_restore_remaps: HashSet<RestoreColumnKey> = HashSet::new();
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
        let table_meta = manifest_table_by_relation(manifest, &target.schema, &target.table)
            .ok_or_else(|| {
                backup_internal_status(
                    "restore_freshness_project_scope",
                    format!(
                        "purge planner target {}.{} is absent from the active project manifest",
                        target.schema, target.table
                    ),
                )
            })?;
        let project_column = crate::generation::sql::resolve_project_column(table_meta);
        // `journal_run_started` deliberately precedes the transaction so a failed
        // restore remains observable. Its target-scoped BackupRun is therefore the
        // one legitimate row an otherwise fresh target owns at this point. Exclude
        // only THIS restore id; an older backup/restore journal still proves the
        // target is not pristine and remains a refusal.
        let current_restore_journal = is_restore_journal_relation(&target.schema, &target.table);
        let present: Option<i32> = match (current_restore_journal, project_column) {
            (true, Some(project_column)) => {
                let run_model = native_model(BACKUP_RUN_MSG, &["backup_id"]);
                let sql = format!(
                    "SELECT 1 FROM {rel} WHERE {tenant_col}::text = $1 AND {project_col}::text = $2 AND {backup_id}::text <> $3 LIMIT 1",
                    tenant_col = qi_runtime(&target.tenant_column),
                    project_col = qi_runtime(project_column),
                    backup_id = run_model.q("backup_id"),
                );
                sqlx::query_scalar(&sql)
                    .bind(&target_tenant_id)
                    .bind(&binding.project_id)
                    .bind(&restore_id)
                    .fetch_optional(&mut *tx)
                    .await
            }
            (false, Some(project_column)) => {
                let sql = format!(
                    "SELECT 1 FROM {rel} WHERE {tenant_col}::text = $1 AND {project_col}::text = $2 LIMIT 1",
                    tenant_col = qi_runtime(&target.tenant_column),
                    project_col = qi_runtime(project_column),
                );
                sqlx::query_scalar(&sql)
                    .bind(&target_tenant_id)
                    .bind(&binding.project_id)
                    .fetch_optional(&mut *tx)
                    .await
            }
            (true, None) => {
                let run_model = native_model(BACKUP_RUN_MSG, &["backup_id"]);
                let sql = format!(
                    "SELECT 1 FROM {rel} WHERE {tenant_col}::text = $1 AND {backup_id}::text <> $2 LIMIT 1",
                    tenant_col = qi_runtime(&target.tenant_column),
                    backup_id = run_model.q("backup_id"),
                );
                sqlx::query_scalar(&sql)
                    .bind(&target_tenant_id)
                    .bind(&restore_id)
                    .fetch_optional(&mut *tx)
                    .await
            }
            (false, None) => {
                let sql = format!(
                    "SELECT 1 FROM {rel} WHERE {tenant_col}::text = $1 LIMIT 1",
                    tenant_col = qi_runtime(&target.tenant_column),
                );
                sqlx::query_scalar(&sql)
                    .bind(&target_tenant_id)
                    .fetch_optional(&mut *tx)
                    .await
            }
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

    for (restore_position, entry) in ordered_tables.iter().enumerate() {
        let schema = entry.get("schema").and_then(|v| v.as_str()).unwrap_or("");
        let table = entry.get("table").and_then(|v| v.as_str()).unwrap_or("");
        let tenant_column = entry
            .get("tenant_column")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let declared_project_column = entry
            .get("project_column")
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
        let manifest_table = manifest_table_by_relation(manifest, schema, table).ok_or_else(|| {
            backup_topology_mismatch_status(
                "restore_tenant",
                format!(
                    "backup manifest relation {schema}.{table} is absent from the active catalog"
                ),
            )
        })?;
        let project_column =
            crate::generation::sql::resolve_project_column(manifest_table).unwrap_or_default();
        if declared_project_column != project_column {
            return Err(backup_topology_mismatch_status(
                "restore_tenant",
                format!(
                    "backup manifest project column '{}' for {schema}.{table} does not match active catalog column '{}'",
                    declared_project_column, project_column
                ),
            ));
        }
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
        let table_has_rows = jsonl.lines().any(|line| !line.trim().is_empty());
        let restore_remap_plans = if cross_tenant_restore && table_has_rows {
            postgres_restore_remap_plans(
                &mut *tx,
                schema,
                table,
                tenant_column,
                manifest_table,
                &restore_remaps,
                &expected_restore_remaps,
            )
            .await?
        } else {
            Vec::new()
        };
        if cross_tenant_restore {
            expected_restore_remaps.extend(
                restore_remap_plans
                    .iter()
                    .filter(|plan| plan.predicates.is_empty())
                    .map(|plan| RestoreColumnKey {
                        schema: schema.to_string(),
                        table: table.to_string(),
                        column: plan.column.clone(),
                    }),
            );
            for line in jsonl.lines().map(str::trim).filter(|line| !line.is_empty()) {
                let row = parse_scoped_restore_row(
                    line,
                    schema,
                    table,
                    tenant_column,
                    &target_tenant_id,
                    project_column,
                    &binding.project_id,
                )?;
                preallocate_unconditional_restore_row(
                    &mut *tx,
                    &row,
                    manifest_table,
                    &target_tenant_id,
                    &mut restore_remaps,
                    &restore_remap_plans,
                )
                .await?;
            }
        }
        let rel = qualified_relation(schema, table);
        // `jsonb_populate_record` casts the row JSON into the table's row type,
        // so every column type is handled by Postgres without hand-mapping.
        let insert_sql =
            format!("INSERT INTO {rel} SELECT (jsonb_populate_record(NULL::{rel}, $1::jsonb)).*");
        let mut deferred_self_reference_rows = Vec::new();
        for line in jsonl.lines().map(str::trim).filter(|line| !line.is_empty()) {
            let mut obj = parse_scoped_restore_row(
                line,
                schema,
                table,
                tenant_column,
                &target_tenant_id,
                project_column,
                &binding.project_id,
            )?;
            if cross_tenant_restore {
                apply_parent_restore_remaps(
                    &mut obj,
                    manifest_table,
                    &restore_remaps,
                    &expected_restore_remaps,
                    &restore_relation_positions,
                    restore_position,
                )?;
            }
            if cross_tenant_restore {
                apply_cross_tenant_restore_remaps(
                    &mut *tx,
                    &mut obj,
                    manifest_table,
                    &target_tenant_id,
                    &mut restore_remaps,
                    &restore_remap_plans,
                )
                .await?;
            }
            if cross_tenant_restore
                && let Some(final_row) =
                    defer_self_reference_values(&mut obj, manifest_table, &restore_remaps)?
            {
                deferred_self_reference_rows.push(final_row);
            }
            // Bind the row as text and cast to jsonb in SQL ($1::jsonb), so the
            // bind never depends on the sqlx json feature being enabled.
            let row_json = serde_json::to_string(&obj).map_err(|err| {
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
        apply_deferred_self_references(
            &mut *tx,
            manifest_table,
            &deferred_self_reference_rows,
        )
        .await?;
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
        &binding.project_id,
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
            "project_id": binding.project_id,
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

#[cfg(test)]
mod restore_remap_tests {
    use super::*;
    use crate::generation::{ManifestForeignKey, ManifestIndex};

    #[test]
    fn numeric_parent_identity_remap_preserves_child_fk_type() {
        let key = RestoreColumnKey {
            schema: "app".to_string(),
            table: "parents".to_string(),
            column: "parent_id".to_string(),
        };
        let mut remaps = RestoreValueRemaps::new();
        remaps.entry(key).or_default().insert(
            restore_value_key(&serde_json::json!(7)).expect("numeric restore key"),
            serde_json::json!(41),
        );
        let child = ManifestTable {
            schema: "app".to_string(),
            table: "children".to_string(),
            foreign_keys: vec![ManifestForeignKey {
                columns: vec!["parent_id".to_string()],
                ref_schema: "app".to_string(),
                ref_table: "parents".to_string(),
                ref_columns: vec!["parent_id".to_string()],
                ..ManifestForeignKey::default()
            }],
            ..ManifestTable::default()
        };
        let mut row = serde_json::json!({"parent_id": 7})
            .as_object()
            .expect("child row")
            .clone();

        apply_parent_restore_remaps(
            &mut row,
            &child,
            &remaps,
            &HashSet::new(),
            &HashMap::new(),
            0,
        )
        .expect("known parent identity must remap");

        assert_eq!(row["parent_id"], serde_json::json!(41));
        assert!(row["parent_id"].is_number());
        assert_ne!(
            restore_value_key(&serde_json::json!(7)),
            restore_value_key(&serde_json::json!("7")),
            "numeric and text identities must never share a remap namespace"
        );
    }

    #[test]
    fn numeric_restore_authority_requires_an_owned_sequence() {
        let denied = restore_remap_authority("app", "parents", "parent_id", "bigint", None)
            .expect_err("an unowned numeric identity must fail closed");
        assert_eq!(denied.code(), tonic::Code::FailedPrecondition);
        assert!(
            denied
                .message()
                .contains("no trusted owned serial/identity sequence")
        );

        assert_eq!(
            restore_remap_authority(
                "app",
                "parents",
                "parent_id",
                "bigint",
                Some("app.parents_parent_id_seq".to_string()),
            )
            .expect("owned sequence is trusted"),
            RestoreRemapAuthority::OwnedSequence("app.parents_parent_id_seq".to_string())
        );
    }

    #[test]
    fn missing_expected_parent_identity_fails_closed() {
        let key = RestoreColumnKey {
            schema: "app".to_string(),
            table: "parents".to_string(),
            column: "parent_id".to_string(),
        };
        let mut remaps = RestoreValueRemaps::new();
        remaps.insert(key, HashMap::new());
        let child = ManifestTable {
            schema: "app".to_string(),
            table: "children".to_string(),
            foreign_keys: vec![ManifestForeignKey {
                columns: vec!["parent_id".to_string()],
                ref_schema: "app".to_string(),
                ref_table: "parents".to_string(),
                ref_columns: vec!["parent_id".to_string()],
                ..ManifestForeignKey::default()
            }],
            ..ManifestTable::default()
        };
        let mut row = serde_json::json!({"parent_id": 7})
            .as_object()
            .expect("child row")
            .clone();

        let denied = apply_parent_restore_remaps(
            &mut row,
            &child,
            &remaps,
            &HashSet::new(),
            &HashMap::new(),
            0,
        )
        .expect_err("a declared parent remap cannot silently omit one child identity");
        assert_eq!(denied.code(), tonic::Code::FailedPrecondition);
        assert!(denied.message().contains("no exact parent remap"));
    }

    #[test]
    fn self_reference_uses_preallocated_identity_for_a_later_row() {
        let old_parent = Uuid::new_v4().to_string();
        let new_parent = Uuid::new_v4().to_string();
        let key = RestoreColumnKey {
            schema: "udb_authn".to_string(),
            table: "users".to_string(),
            column: "user_id".to_string(),
        };
        let mut remaps = RestoreValueRemaps::new();
        remaps.entry(key.clone()).or_default().insert(
            restore_value_key(&serde_json::json!(old_parent.clone())).expect("user restore key"),
            serde_json::json!(new_parent.clone()),
        );
        let users = ManifestTable {
            schema: "udb_authn".to_string(),
            table: "users".to_string(),
            foreign_keys: vec![ManifestForeignKey {
                columns: vec!["created_by".to_string()],
                ref_schema: "udb_authn".to_string(),
                ref_table: "users".to_string(),
                ref_columns: vec!["user_id".to_string()],
                ..ManifestForeignKey::default()
            }],
            ..ManifestTable::default()
        };
        let mut row = serde_json::json!({"created_by": old_parent})
            .as_object()
            .expect("self-referencing user")
            .clone();

        apply_parent_restore_remaps(
            &mut row,
            &users,
            &remaps,
            &HashSet::from([key]),
            &HashMap::from([(("udb_authn".to_string(), "users".to_string()), 0)]),
            0,
        )
        .expect("preallocated self identity must be available before row order matters");
        assert_eq!(row["created_by"], serde_json::json!(new_parent));
    }

    #[test]
    fn absent_planned_or_later_parent_remap_fails_closed() {
        let key = RestoreColumnKey {
            schema: "app".to_string(),
            table: "parents".to_string(),
            column: "parent_id".to_string(),
        };
        let child = ManifestTable {
            schema: "app".to_string(),
            table: "children".to_string(),
            foreign_keys: vec![ManifestForeignKey {
                columns: vec!["parent_id".to_string()],
                ref_schema: "app".to_string(),
                ref_table: "parents".to_string(),
                ref_columns: vec!["parent_id".to_string()],
                ..ManifestForeignKey::default()
            }],
            ..ManifestTable::default()
        };
        let original = serde_json::json!({"parent_id": 7})
            .as_object()
            .expect("child row")
            .clone();

        let mut planned_row = original.clone();
        let planned = apply_parent_restore_remaps(
            &mut planned_row,
            &child,
            &RestoreValueRemaps::new(),
            &HashSet::from([key]),
            &HashMap::new(),
            0,
        )
        .expect_err("a planned-but-absent mapping cannot retain the source identity");
        assert!(planned.message().contains("not preallocated"));

        let mut reversed_row = original;
        let reversed = apply_parent_restore_remaps(
            &mut reversed_row,
            &child,
            &RestoreValueRemaps::new(),
            &HashSet::new(),
            &HashMap::from([(("app".to_string(), "parents".to_string()), 1)]),
            0,
        )
        .expect_err("a parent restored later cannot be silently cross-tenant-bound");
        assert!(reversed.message().contains("later in restore order"));
    }

    #[test]
    fn unsupported_standalone_unique_partition_value_still_fails_closed() {
        let denied = restore_remap_authority(
            "udb_notification",
            "notification_logs",
            "created_at",
            "timestamp with time zone",
            None,
        )
        .expect_err("an independently unique timestamp must remain unsupported");
        assert_eq!(denied.code(), tonic::Code::FailedPrecondition);
        assert!(denied.message().contains("unsupported restore type"));
    }

    #[test]
    fn manifest_composite_unique_keys_are_not_flattened() {
        let table = ManifestTable {
            table: "notification_logs".to_string(),
            primary_key: vec!["log_id".to_string(), "created_at".to_string()],
            indexes: vec![ManifestIndex {
                name: "notification_logs_partial".to_string(),
                unique: true,
                columns: vec!["log_id".to_string(), "created_at".to_string()],
                where_clause: "status = 'active'".to_string(),
                ..ManifestIndex::default()
            }],
            ..ManifestTable::default()
        };

        let keys = manifest_unique_restore_keys(&table);
        assert_eq!(keys.len(), 2);
        assert_eq!(
            keys[0].columns,
            vec!["log_id".to_string(), "created_at".to_string()]
        );
        assert_eq!(keys[1].predicate.as_deref(), Some("status = 'active'"));
    }

    #[test]
    fn partial_plan_does_not_protect_an_unconditional_key() {
        let mut plans = Vec::new();
        let authority = RestoreRemapAuthority::GeneratedText {
            uuid: true,
            max_len: None,
        };
        add_restore_remap_plan(
            &mut plans,
            "user_id".to_string(),
            authority.clone(),
            Some("email <> ''".to_string()),
        );
        assert!(plan_protects_unique_key(
            &plans,
            "user_id",
            Some("email <> ''")
        ));
        assert!(!plan_protects_unique_key(&plans, "user_id", None));

        add_restore_remap_plan(&mut plans, "user_id".to_string(), authority, None);
        assert!(plan_protects_unique_key(&plans, "user_id", None));
        assert!(plans[0].predicates.is_empty());
    }

    #[test]
    fn partial_predicate_candidate_includes_later_unconditional_remaps() {
        let authority = RestoreRemapAuthority::GeneratedText {
            uuid: false,
            max_len: Some(80),
        };
        let plans = vec![
            RestoreColumnRemapPlan {
                column: "email".to_string(),
                authority: authority.clone(),
                predicates: vec!["alias <> ''".to_string()],
            },
            RestoreColumnRemapPlan {
                column: "alias".to_string(),
                authority,
                predicates: Vec::new(),
            },
        ];
        let prepared = vec![
            Some(PreparedRestoreValue {
                key: RestoreColumnKey {
                    schema: "app".to_string(),
                    table: "users".to_string(),
                    column: "email".to_string(),
                },
                old_value_key: "s:source@example.test".to_string(),
                new_value: serde_json::json!("r00000000000000000000000000000000"),
            }),
            Some(PreparedRestoreValue {
                key: RestoreColumnKey {
                    schema: "app".to_string(),
                    table: "users".to_string(),
                    column: "alias".to_string(),
                },
                old_value_key: "s:".to_string(),
                new_value: serde_json::json!("r11111111111111111111111111111111"),
            }),
        ];
        let original = serde_json::json!({
            "email": "source@example.test",
            "alias": ""
        })
        .as_object()
        .expect("restore row")
        .clone();

        let candidate = restore_candidate_row(&original, &plans, &prepared, &[false, true]);
        assert_eq!(candidate["email"], serde_json::json!("source@example.test"));
        assert_eq!(
            candidate["alias"],
            serde_json::json!("r11111111111111111111111111111111")
        );
    }

    #[test]
    fn bounded_text_restore_authority_retains_full_entropy_and_username_lexical_check() {
        let denied = restore_remap_authority(
            "app",
            "short_codes",
            "code",
            "character varying(22)",
            None,
        )
        .expect_err("truncating away random uniqueness must fail closed");
        assert_eq!(denied.code(), tonic::Code::FailedPrecondition);
        assert!(denied.message().contains("below the 33-character"));

        let authority = restore_remap_authority(
            "app",
            "wide_codes",
            "code",
            "character varying(33)",
            None,
        )
        .expect("a complete 128-bit bounded encoding is supported");
        let first = restored_unique_value(&authority, "tenant-a");
        let second = restored_unique_value(&authority, "tenant-a");
        assert_eq!(first.len(), 33);
        assert_eq!(second.len(), 33);
        assert!(first.starts_with('r'));
        assert!(second.starts_with('r'));
        assert!(first.chars().next().is_some_and(|ch| ch.is_ascii_lowercase()));
        assert!(
            first
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit()),
            "the maintained User.username lexical CHECK must accept the remap"
        );
        assert_ne!(first, second);
    }

    #[test]
    fn foreign_key_remap_can_protect_a_unique_child_key() {
        let parent_key = RestoreColumnKey {
            schema: "app".to_string(),
            table: "parents".to_string(),
            column: "parent_id".to_string(),
        };
        let remaps = HashMap::from([(parent_key, HashMap::new())]);
        let child = ManifestTable {
            foreign_keys: vec![ManifestForeignKey {
                columns: vec!["parent_id".to_string()],
                ref_schema: "app".to_string(),
                ref_table: "parents".to_string(),
                ref_columns: vec!["parent_id".to_string()],
                ..ManifestForeignKey::default()
            }],
            ..ManifestTable::default()
        };
        assert!(foreign_key_remap_available(
            &child,
            "parent_id",
            &remaps,
            &HashSet::new(),
            &[]
        ));
    }
}
