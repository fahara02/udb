#![allow(clippy::result_large_err)]

//! PostgreSQL query building helpers: plan execution, row binding, join fusion,
//! and record serialisation. `rows_to_record_set` and its encryption-aware
//! siblings remain in `core.rs` because they depend on the private
//! `EncryptionMetrics` type.

use serde_json::Value as JsonValue;
use sqlx::Postgres;
use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use uuid::Uuid;

use crate::broker::{RequestContext, table_for_message};
use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::proto::{Mutation, SelectRequest, UpsertRequest};

use super::executor_utils::{
    json_f64, json_i64, json_scalar_to_string, qi_runtime, reject_plan, struct_to_json,
};

// ── Transaction plan execution ────────────────────────────────────────────────

pub(crate) async fn execute_tx_plan(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    manifest: &CatalogManifest,
    message_type: &str,
    sql: &str,
    columns: &[String],
    values: &[JsonValue],
    errors: &[String],
) -> Result<u64, tonic::Status> {
    reject_plan(errors)?;
    let table = table_for_message(manifest, message_type)
        .ok_or_else(|| tonic::Status::invalid_argument("unknown message_type"))?;
    let query = bind_values(sqlx::query(sql), table, columns, values)?;
    let result = query
        .execute(&mut **tx)
        .await
        .map_err(|err| tonic::Status::internal(format!("transaction mutation failed: {err}")))?;
    Ok(result.rows_affected())
}

// ── Join fusion ───────────────────────────────────────────────────────────────

pub(crate) struct JoinFusionPlan {
    pub(crate) sql: String,
    pub(crate) bindings: Vec<(ManifestColumn, JsonValue)>,
}

pub(crate) fn build_join_fusion_sql(
    manifest: &CatalogManifest,
    request: &SelectRequest,
    context: &RequestContext,
    filter: &JsonValue,
) -> Result<JoinFusionPlan, tonic::Status> {
    if context.tenant_id.trim().is_empty() {
        return Err(tonic::Status::invalid_argument(
            "tenant_id is required for join fusion",
        ));
    }
    let message_types = split_join_message_types(&request.message_type);
    if message_types.len() < 2 {
        return Err(tonic::Status::invalid_argument(
            "join fusion requires at least two message types",
        ));
    }
    let tables = message_types
        .iter()
        .map(|message_type| {
            table_for_message(manifest, message_type).ok_or_else(|| {
                tonic::Status::invalid_argument(format!("unknown message_type {message_type}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let aliases = (0..tables.len())
        .map(|idx| format!("t{idx}"))
        .collect::<Vec<_>>();
    let select_list = join_select_list(&tables, &aliases, &request.fields)?;
    let mut sql = format!(
        "SELECT {} FROM {}.{} {}",
        select_list.join(", "),
        qi_runtime(&tables[0].schema),
        qi_runtime(&tables[0].table),
        qi_runtime(&aliases[0])
    );
    for idx in 1..tables.len() {
        let join = find_join_edge(
            &tables[0..idx],
            &aliases[0..idx],
            tables[idx],
            &aliases[idx],
        )
        .ok_or_else(|| {
            tonic::Status::invalid_argument(format!(
                "no foreign key path found for join fusion target {}",
                message_types[idx]
            ))
        })?;
        sql.push_str(" JOIN ");
        sql.push_str(&format!(
            "{}.{} {} ON {}",
            qi_runtime(&tables[idx].schema),
            qi_runtime(&tables[idx].table),
            qi_runtime(&aliases[idx]),
            join
        ));
    }

    let mut bindings = Vec::new();
    let mut predicates = Vec::new();
    if let Some(column) = tenant_column_ref(tables[0]) {
        bindings.push((column.clone(), JsonValue::String(context.tenant_id.clone())));
        predicates.push(format!(
            "{}.{} = ${}",
            qi_runtime(&aliases[0]),
            qi_runtime(&column.column_name),
            bindings.len()
        ));
    }
    if let JsonValue::Object(map) = filter {
        for (field, value) in map {
            if field.starts_with('$') || value.is_object() || value.is_array() {
                return Err(tonic::Status::invalid_argument(
                    "join fusion supports only simple equality filters",
                ));
            }
            let (table_idx, column_name) = parse_join_field(field, &message_types)?;
            let column = tables[table_idx]
                .columns
                .iter()
                .find(|column| column.column_name == column_name)
                .ok_or_else(|| {
                    tonic::Status::invalid_argument(format!("unknown join filter field {field}"))
                })?;
            bindings.push((column.clone(), value.clone()));
            predicates.push(format!(
                "{}.{} = ${}",
                qi_runtime(&aliases[table_idx]),
                qi_runtime(&column.column_name),
                bindings.len()
            ));
        }
    }
    if !predicates.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&predicates.join(" AND "));
    }
    if request.limit > 0 {
        sql.push_str(&format!(" LIMIT {}", request.limit));
    }
    Ok(JoinFusionPlan { sql, bindings })
}

pub(crate) fn split_join_message_types(message_type: &str) -> Vec<String> {
    message_type
        .split([',', '+'])
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(ToString::to_string)
        .collect()
}

pub(crate) fn is_join_fusion_message_type(message_type: &str) -> bool {
    split_join_message_types(message_type).len() > 1
}

fn join_select_list(
    tables: &[&ManifestTable],
    aliases: &[String],
    fields: &[String],
) -> Result<Vec<String>, tonic::Status> {
    if fields.is_empty() {
        return Ok(tables
            .iter()
            .zip(aliases)
            .flat_map(|(table, alias)| {
                table.columns.iter().map(move |column| {
                    format!(
                        "{}.{} AS {}",
                        qi_runtime(alias),
                        qi_runtime(&column.column_name),
                        qi_runtime(&format!("{}__{}", table.message_name, column.column_name))
                    )
                })
            })
            .collect());
    }
    fields
        .iter()
        .map(|field| {
            let message_types = tables
                .iter()
                .map(|table| table.message_name.clone())
                .collect::<Vec<_>>();
            let (table_idx, column_name) = parse_join_field(field, &message_types)?;
            if !tables[table_idx]
                .columns
                .iter()
                .any(|column| column.column_name == column_name)
            {
                return Err(tonic::Status::invalid_argument(format!(
                    "unknown join selected field {field}"
                )));
            }
            Ok(format!(
                "{}.{} AS {}",
                qi_runtime(&aliases[table_idx]),
                qi_runtime(&column_name),
                qi_runtime(&field.replace('.', "__"))
            ))
        })
        .collect()
}

fn parse_join_field(
    field: &str,
    message_types: &[String],
) -> Result<(usize, String), tonic::Status> {
    if let Some((message_type, column)) = field.split_once('.') {
        let idx = message_types
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(message_type))
            .ok_or_else(|| {
                tonic::Status::invalid_argument(format!("unknown join field prefix {message_type}"))
            })?;
        return Ok((idx, column.to_ascii_lowercase()));
    }
    Ok((0, field.to_ascii_lowercase()))
}

fn find_join_edge(
    prior_tables: &[&ManifestTable],
    prior_aliases: &[String],
    next_table: &ManifestTable,
    next_alias: &str,
) -> Option<String> {
    for (prior, prior_alias) in prior_tables.iter().zip(prior_aliases) {
        for fk in &prior.foreign_keys {
            if fk.ref_schema == next_table.schema && fk.ref_table == next_table.table {
                return Some(join_predicate(
                    prior_alias,
                    &fk.columns,
                    next_alias,
                    &fk.ref_columns,
                ));
            }
        }
        for fk in &next_table.foreign_keys {
            if fk.ref_schema == prior.schema && fk.ref_table == prior.table {
                return Some(join_predicate(
                    next_alias,
                    &fk.columns,
                    prior_alias,
                    &fk.ref_columns,
                ));
            }
        }
    }
    None
}

fn join_predicate(
    left_alias: &str,
    left_columns: &[String],
    right_alias: &str,
    right_columns: &[String],
) -> String {
    left_columns
        .iter()
        .zip(right_columns)
        .map(|(left, right)| {
            format!(
                "{}.{} = {}.{}",
                qi_runtime(left_alias),
                qi_runtime(left),
                qi_runtime(right_alias),
                qi_runtime(right)
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

pub(crate) fn tenant_column_ref(table: &ManifestTable) -> Option<&ManifestColumn> {
    table.columns.iter().find(|column| {
        matches!(
            column.column_name.as_str(),
            "tenant_id" | "org_id" | "institution_id"
        )
    })
}

// ── Parameter binding ─────────────────────────────────────────────────────────

pub(crate) fn bind_values<'q>(
    mut query: Query<'q, Postgres, PgArguments>,
    table: &ManifestTable,
    columns: &[String],
    values: &[JsonValue],
) -> Result<Query<'q, Postgres, PgArguments>, tonic::Status> {
    if columns.len() != values.len() {
        return Err(tonic::Status::invalid_argument(format!(
            "parameter mismatch: {} columns, {} values",
            columns.len(),
            values.len()
        )));
    }
    for (column_name, value) in columns.iter().zip(values.iter()) {
        let column = table
            .columns
            .iter()
            .find(|column| column.column_name == *column_name);
        query = bind_one(query, column, value)?;
    }
    Ok(query)
}

pub(crate) fn bind_one<'q>(
    query: Query<'q, Postgres, PgArguments>,
    column: Option<&ManifestColumn>,
    value: &JsonValue,
) -> Result<Query<'q, Postgres, PgArguments>, tonic::Status> {
    let sql_type = column
        .map(|column| column.sql_type.to_ascii_uppercase())
        .unwrap_or_default();
    if value.is_null() {
        return Ok(query.bind(Option::<String>::None));
    }
    if sql_type.contains("JSON") {
        return Ok(query.bind(sqlx::types::Json(value.clone())));
    }
    if sql_type == "UUID" {
        let parsed = value
            .as_str()
            .ok_or_else(|| tonic::Status::invalid_argument("UUID value must be a string"))?
            .parse::<Uuid>()
            .map_err(|err| tonic::Status::invalid_argument(format!("invalid UUID: {err}")))?;
        return Ok(query.bind(parsed));
    }
    // Array value — used for `$in` / `col = ANY($N)` filters. (#121)
    // Bind a *typed* array matching the column type: PostgreSQL does NOT
    // implicitly cast a `text[]` element to the column type inside `= ANY`, so a
    // `uuid`/`int`/`numeric` column compared against a text array fails at
    // execution. Typed arrays keep the predicate index-usable (no `::type` cast
    // on the column needed).
    if let JsonValue::Array(items) = value {
        if sql_type == "UUID" {
            let mut arr: Vec<Uuid> = Vec::with_capacity(items.len());
            for item in items {
                let parsed = item
                    .as_str()
                    .ok_or_else(|| {
                        tonic::Status::invalid_argument("UUID $in value must be a string")
                    })?
                    .parse::<Uuid>()
                    .map_err(|err| {
                        tonic::Status::invalid_argument(format!("invalid UUID in $in: {err}"))
                    })?;
                arr.push(parsed);
            }
            return Ok(query.bind(arr));
        }
        if sql_type.contains("INT") || sql_type.contains("SERIAL") {
            let mut arr: Vec<i64> = Vec::with_capacity(items.len());
            for item in items {
                arr.push(json_i64(item)?);
            }
            return Ok(query.bind(arr));
        }
        if sql_type.contains("REAL")
            || sql_type.contains("DOUBLE")
            || sql_type.contains("FLOAT")
            || sql_type.contains("NUMERIC")
            || sql_type.contains("DECIMAL")
        {
            let mut arr: Vec<f64> = Vec::with_capacity(items.len());
            for item in items {
                arr.push(json_f64(item)?);
            }
            return Ok(query.bind(arr));
        }
        if sql_type.contains("BOOL") {
            let arr: Vec<bool> = items.iter().map(|i| i.as_bool().unwrap_or(false)).collect();
            return Ok(query.bind(arr));
        }
        // text / varchar / enum / timestamp and friends: a text[] compares
        // correctly for text-typed columns (`text = ANY(text[])`).
        let arr: Vec<String> = items.iter().map(json_scalar_to_string).collect();
        return Ok(query.bind(arr));
    }
    if sql_type.contains("BOOL") {
        return Ok(query.bind(value.as_bool().unwrap_or(false)));
    }
    if sql_type.contains("INT") || sql_type.contains("BIGSERIAL") || sql_type.contains("SERIAL") {
        return Ok(query.bind(json_i64(value)?));
    }
    if sql_type.contains("REAL")
        || sql_type.contains("DOUBLE")
        || sql_type.contains("FLOAT")
        || sql_type.contains("NUMERIC")
        || sql_type.contains("DECIMAL")
    {
        return Ok(query.bind(json_f64(value)?));
    }
    Ok(query.bind(json_scalar_to_string(value)))
}

// ── Record serialisation ──────────────────────────────────────────────────────

pub(crate) fn upsert_record_json(request: &UpsertRequest) -> Result<JsonValue, tonic::Status> {
    if let Some(payload) = &request.payload {
        return Ok(struct_to_json(payload));
    }
    if !request.record_json.is_empty() {
        return serde_json::from_slice(&request.record_json).map_err(|err| {
            tonic::Status::invalid_argument(format!("record_json must be valid JSON: {err}"))
        });
    }
    Err(tonic::Status::invalid_argument(
        "payload or record_json is required",
    ))
}

pub(crate) fn mutation_record_json(mutation: &Mutation) -> Result<JsonValue, tonic::Status> {
    if let Some(payload) = &mutation.payload {
        return Ok(struct_to_json(payload));
    }
    if !mutation.record_json.is_empty() {
        return serde_json::from_slice(&mutation.record_json).map_err(|err| {
            tonic::Status::invalid_argument(format!("record_json must be valid JSON: {err}"))
        });
    }
    Err(tonic::Status::invalid_argument(
        "payload or record_json is required",
    ))
}

pub(crate) fn record_values(
    record: &JsonValue,
    columns: &[String],
) -> Result<Vec<JsonValue>, tonic::Status> {
    let object = record
        .as_object()
        .ok_or_else(|| tonic::Status::invalid_argument("record must be a JSON object"))?;
    Ok(columns
        .iter()
        .map(|column| object.get(column).cloned().unwrap_or(JsonValue::Null))
        .collect())
}

pub(crate) fn filter_bind_values(filter: &JsonValue) -> Vec<JsonValue> {
    let mut out = Vec::new();
    collect_filter_values(filter, &mut out);
    out
}

fn collect_filter_values(value: &JsonValue, out: &mut Vec<JsonValue>) {
    match value {
        JsonValue::Object(map) => {
            for (key, nested) in map {
                let normalized = key.to_ascii_lowercase();
                if matches!(normalized.as_str(), "$and" | "and" | "$or" | "or") {
                    collect_filter_values(nested, out);
                } else if normalized.starts_with('$') {
                    // Skip the null predicates — `$is_null`/`$not_null` compile to
                    // `IS NULL` / `IS NOT NULL`, which bind no SQL parameter. Pushing
                    // a value for them would desync the placeholder↔value count and
                    // make `bind_values` fail (or bind onto the wrong $N).
                    if !matches!(normalized.as_str(), "$is_null" | "$not_null") {
                        out.push(nested.clone());
                    }
                } else if let JsonValue::Object(_) = nested {
                    collect_filter_values(nested, out);
                } else {
                    out.push(nested.clone());
                }
            }
        }
        JsonValue::Array(items) => {
            for item in items {
                collect_filter_values(item, out);
            }
        }
        _ => {}
    }
}
