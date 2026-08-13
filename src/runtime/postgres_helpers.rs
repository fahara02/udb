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

use crate::broker::{RequestContext, resolve_table_for_message};
use crate::generation::sql::{resolve_tenant_column_ref, table_requires_tenant_column};
use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::proto::{Mutation, SelectRequest, UpsertRequest};

use super::executor_utils::{
    invalid_argument_fields, json_scalar_to_string, qi_runtime, reject_plan, struct_to_json,
};

fn postgres_invalid_field(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    invalid_argument_fields(message, [(field.into(), description.into())])
}

fn join_fusion_missing_tenant_column_status(table: &ManifestTable) -> tonic::Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::FailedPrecondition,
        "postgres",
        "join_fusion",
        "tenant_column_required",
        format!(
            "join fusion cannot safely select scoped table {}.{} without a tenant column",
            table.schema, table.table
        ),
    )
}

fn postgres_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status("postgres", operation, message)
}

/// Bind a JSON value to a SQL integer column.
///
/// Accepts a JSON integer, a decimal string, and — critically — a LOSSLESSLY integral
/// double. protobuf `Struct` has a single numeric kind (double), so a client that sends
/// a native integer (Go `25`, JS `25`) arrives here as `25.0`. Rejecting that made the
/// two mutation verbs disagree on the same column: Upsert accepted the native integer
/// while Update failed `expected integer, got 25.0`, so callers had to remember which
/// verb needed a decimal string. Strictness is still preserved where it matters — a
/// fractional or out-of-range double is refused rather than silently truncated, matching
/// the rule UDB's own generated SDK helpers already apply.
fn postgres_json_i64(field: &str, value: &JsonValue) -> Result<i64, tonic::Status> {
    value
        .as_i64()
        .or_else(|| value.as_str()?.parse().ok())
        .or_else(|| {
            let number = value.as_f64()?;
            let truncated = number as i64;
            (truncated as f64 == number).then_some(truncated)
        })
        .ok_or_else(|| {
            postgres_invalid_field(
                field,
                "must be an integer, an integer string, or a whole number",
                format!("expected integer, got {value}"),
            )
        })
}

fn postgres_json_f64(field: &str, value: &JsonValue) -> Result<f64, tonic::Status> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse().ok())
        .ok_or_else(|| {
            postgres_invalid_field(
                field,
                "must be a number or numeric string",
                format!("expected number, got {value}"),
            )
        })
}

/// The bare type name of an uppercased SQL declaration, with any parameter
/// list, array suffix or trailing modifier removed: `GEOGRAPHY(POINT,4326)` and
/// `BIGINT[]` reduce to `GEOGRAPHY` and `BIGINT`.
fn postgres_base_sql_type(sql_type: &str) -> &str {
    sql_type
        .trim()
        .split(|character: char| character == '(' || character == '[' || character.is_whitespace())
        .next()
        .unwrap_or_default()
}

/// Whether a declared PostgreSQL type is an integer scalar (or integer array).
///
/// Type dispatch must classify the base type token, never search the complete
/// declaration. `GEOGRAPHY(POINT,4326)` contains the letters `INT` inside
/// `POINT`; substring matching therefore sent a valid EWKB string through the
/// integer parser and made every served geography Update fail.
fn postgres_is_integer_type(sql_type: &str) -> bool {
    matches!(
        postgres_base_sql_type(sql_type),
        "SMALLINT"
            | "INT2"
            | "INTEGER"
            | "INT"
            | "INT4"
            | "BIGINT"
            | "INT8"
            | "SMALLSERIAL"
            | "SERIAL2"
            | "SERIAL"
            | "SERIAL4"
            | "BIGSERIAL"
            | "SERIAL8"
    )
}

/// Whether a column's declared type has a canonical TEXT input form but NO
/// assignment cast from a bound `text` parameter, so its placeholder must carry
/// a `::TYPE` cast in the emitted SQL.
///
/// This mirrors the class the Postgres IR compiler casts with
/// `cast_compare_placeholder` (`$n::GEOGRAPHY`, `$n::INET`, …). Binding such a
/// value as a plain Rust `String` fails to PLAN with SQLSTATE 42804 ("column is
/// of type geography but expression is of type text"); naming the concrete type
/// to sqlx instead trades that for SQLSTATE 22P03, because sqlx sends parameters
/// in BINARY format and PostgreSQL would hand the client's hex-EWKB *text* to
/// `geography_recv`. The cast is what makes the server apply the type's own TEXT
/// input function, so the value stays a text bind here.
fn postgres_type_needs_sql_cast(sql_type: &str) -> bool {
    matches!(
        postgres_base_sql_type(sql_type),
        "GEOGRAPHY" | "GEOMETRY" | "INET" | "CIDR" | "MACADDR" | "MACADDR8"
    )
}

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
    let table = resolve_table_for_message(manifest, message_type).map_err(|error| {
        postgres_invalid_field(
            "message_type",
            "must match exactly one manifest table message type",
            error.to_string(),
        )
    })?;
    let query = bind_values(sqlx::query(sql), table, columns, values)?;
    let result = query.execute(&mut **tx).await.map_err(|err| {
        postgres_internal_status(
            "execute_tx_plan",
            format!("transaction mutation failed: {err}"),
        )
    })?;
    Ok(result.rows_affected())
}

// ── Join fusion ───────────────────────────────────────────────────────────────

pub(crate) struct JoinFusionPlan {
    pub(crate) sql: String,
    pub(crate) bindings: Vec<(ManifestColumn, JsonValue)>,
    /// Columns to mask, in this plan's ALIASED output names
    /// (`"{Message}__{column}"`). The single-entity path masks on bare column
    /// names, which never match a fused row.
    pub(crate) masked_columns: Vec<String>,
}

pub(crate) fn build_join_fusion_sql(
    manifest: &CatalogManifest,
    request: &SelectRequest,
    context: &RequestContext,
    filter: &JsonValue,
) -> Result<JoinFusionPlan, tonic::Status> {
    if context.tenant_id.trim().is_empty() {
        return Err(postgres_invalid_field(
            "tenant_id",
            "must be non-empty for join fusion",
            "tenant_id is required for join fusion",
        ));
    }
    // `select` short-circuits to join fusion BEFORE it plans anything, so every
    // check the planner performs has to be repeated here or it simply does not
    // happen for a fused read.
    if context.purpose.trim().is_empty() {
        return Err(postgres_invalid_field(
            "purpose",
            "must be non-empty for join fusion",
            "purpose is required for join fusion",
        ));
    }
    if !context
        .scopes
        .iter()
        .any(|scope| scope == "udb:read" || scope == "udb:*" || scope == "*")
    {
        return Err(postgres_invalid_field(
            "scopes",
            "must include udb:read",
            "scope udb:read is required for join fusion",
        ));
    }
    let message_types = split_join_message_types(&request.message_type);
    if message_types.len() < 2 {
        return Err(postgres_invalid_field(
            "message_type",
            "must contain at least two comma- or plus-separated message types",
            "join fusion requires at least two message types",
        ));
    }
    let tables = message_types
        .iter()
        .map(|message_type| {
            resolve_table_for_message(manifest, message_type).map_err(|error| {
                postgres_invalid_field(
                    "message_type",
                    "must match exactly one manifest table message type",
                    error.to_string(),
                )
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
            postgres_invalid_field(
                "message_type",
                "joined message types must have a foreign key path",
                format!(
                    "no foreign key path found for join fusion target {}",
                    message_types[idx]
                ),
            )
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
    for (table_idx, table) in tables.iter().enumerate() {
        // C22 per-table ABAC: a table declaring `required_scope` demands it on
        // every other read path, and demanded nothing here.
        let required = table.required_scope.trim();
        if !required.is_empty()
            && !context
                .scopes
                .iter()
                .any(|scope| scope == required || scope == "udb:*" || scope == "*")
        {
            return Err(postgres_invalid_field(
                "scopes",
                "must include the table's required_scope",
                format!("scope {required} is required for this table"),
            ));
        }
        match tenant_column_ref(table) {
            Some(column) => {
                bindings.push((column.clone(), JsonValue::String(context.tenant_id.clone())));
                predicates.push(format!(
                    "{}.{} = ${}",
                    qi_runtime(&aliases[table_idx]),
                    qi_runtime(&column.column_name),
                    bindings.len()
                ));
            }
            None => {
                if table_requires_tenant_column(table) {
                    return Err(join_fusion_missing_tenant_column_status(table));
                }
            }
        }
        // Project isolation, mirroring tenant. Absent entirely until now, so a
        // fused read spanned every project in the caller's tenant.
        if !context.project_id.trim().is_empty()
            && let Some(name) = crate::generation::sql::resolve_project_column(table)
            && let Some(column) = table.columns.iter().find(|c| c.column_name == name)
        {
            bindings.push((column.clone(), JsonValue::String(context.project_id.clone())));
            predicates.push(format!(
                "{}.{} = ${}",
                qi_runtime(&aliases[table_idx]),
                qi_runtime(&column.column_name),
                bindings.len()
            ));
        }
        // Default-exclude tombstoned rows, as the single-entity read does.
        // Malformed metadata fails closed rather than returning deleted rows.
        if table.soft_delete {
            let column = table.soft_delete_column.trim();
            if column.is_empty() || !table.columns.iter().any(|c| c.column_name == column) {
                return Err(postgres_invalid_field(
                    "message_type",
                    "soft-delete table must declare a real soft_delete_column",
                    format!(
                        "{}.{} soft_delete_column '{}' is not a declared column",
                        table.schema, table.table, table.soft_delete_column
                    ),
                ));
            }
            predicates.push(format!(
                "{}.{} IS NULL",
                qi_runtime(&aliases[table_idx]),
                qi_runtime(column)
            ));
        }
    }
    if let JsonValue::Object(map) = filter {
        for (field, value) in map {
            if field.starts_with('$') || value.is_object() || value.is_array() {
                return Err(postgres_invalid_field(
                    "filter",
                    "must contain only simple equality fields for join fusion",
                    "join fusion supports only simple equality filters",
                ));
            }
            let (table_idx, column_name) = parse_join_field(field, &message_types)?;
            let column = tables[table_idx]
                .columns
                .iter()
                .find(|column| column.column_name == column_name)
                .ok_or_else(|| {
                    postgres_invalid_field(
                        "filter",
                        "must reference a column from one of the joined tables",
                        format!("unknown join filter field {field}"),
                    )
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
    // Mask set in this plan's ALIASED output names. Computed over every joined
    // column, not just the projected ones — a name absent from the row simply
    // never matches, and an explicitly requested PII field must still be masked.
    let masked_columns = tables
        .iter()
        .flat_map(|table| {
            table
                .columns
                .iter()
                .filter(|column| column.security.is_pii || column.security.mask_in_logs)
                .map(move |column| format!("{}__{}", table.message_name, column.column_name))
        })
        .collect::<Vec<_>>();

    Ok(JoinFusionPlan {
        sql,
        bindings,
        masked_columns,
    })
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
        // Mirror the single-entity read: a default projection never ships PII or
        // raw ciphertext. Join fusion used to select EVERY column of EVERY joined
        // table, so a `message_type` with a comma returned columns the same
        // caller's `Select` had dropped before they left the database.
        return Ok(tables
            .iter()
            .zip(aliases)
            .flat_map(|(table, alias)| {
                table
                    .columns
                    .iter()
                    .filter(|column| !column.security.is_pii && !column.security.is_encrypted)
                    .map(move |column| {
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
                return Err(postgres_invalid_field(
                    "fields",
                    "must reference columns from the joined tables",
                    format!("unknown join selected field {field}"),
                ));
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
                postgres_invalid_field(
                    "field",
                    "prefix must match one of the joined message types",
                    format!("unknown join field prefix {message_type}"),
                )
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
    resolve_tenant_column_ref(table)
}

// ── Parameter binding ─────────────────────────────────────────────────────────

pub(crate) fn bind_values<'q>(
    mut query: Query<'q, Postgres, PgArguments>,
    table: &ManifestTable,
    columns: &[String],
    values: &[JsonValue],
) -> Result<Query<'q, Postgres, PgArguments>, tonic::Status> {
    if columns.len() != values.len() {
        return Err(postgres_invalid_field(
            "values",
            "number of values must match number of columns",
            format!(
                "parameter mismatch: {} columns, {} values",
                columns.len(),
                values.len()
            ),
        ));
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
    // DAT-001: a JSON/JSONB target column stores the value VERBATIM — including a
    // top-level JSON array or object. This decision MUST precede the filter-array
    // path below: otherwise a genuine JSON array destined for a JSONB entity
    // column is misrouted into the typed `text[]` bind and PostgreSQL rejects the
    // mutation with SQLSTATE 42846. Typed SQL-array binding (the branch below)
    // stays for a scalar column in a `$in` / `= ANY` filter context only.
    if sql_type.contains("JSON") {
        return Ok(query.bind(sqlx::types::Json(strip_nul_json(value))));
    }
    // W6 / bug #2 (bytes round-trip): a protobuf `bytes` field maps to a `BYTEA`
    // column (`infer_sql_type`), and every JSON rendering of proto `bytes` is a
    // base64 STRING — the proto3-canonical JSON the SDKs emit AND the base64 the
    // SELECT serializer writes back (the BYTEA read branch in `core/mod.rs`).
    // Without this branch that base64 ASCII falls through to the scalar text-bind
    // below, so PostgreSQL either rejects it (42804 datatype_mismatch) or stores
    // the base64 characters verbatim — never the real bytes. Base64-DECODE
    // symmetrically and bind the native `bytea` type so a write↔read round-trips.
    // Checked before the array branch so a BYTEA target is never misrouted into
    // the typed `text[]` filter-array path. NULL binds a typed `Option<Vec<u8>>`
    // NULL; a present-but-empty value ("" decodes to zero-length bytes, or an
    // empty `[]`) binds empty bytes, distinct from NULL — mirroring the read side
    // where NULL serializes to `null` and empty bytes serialize to `""`.
    if sql_type.contains("BYTEA") {
        use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
        return match value {
            JsonValue::Null => Ok(query.bind(Option::<Vec<u8>>::None)),
            JsonValue::String(raw) => B64
                .decode(raw.trim())
                .map(|bytes| query.bind(bytes))
                .map_err(|err| {
                    postgres_invalid_field(
                        "value",
                        "bytea value must be a base64-encoded string",
                        format!("invalid base64 for bytea value: {err}"),
                    )
                }),
            JsonValue::Array(items) if items.is_empty() => Ok(query.bind(Vec::<u8>::new())),
            _ => Err(postgres_invalid_field(
                "value",
                "bytea value must be a base64 string or null",
                "bytea value must be a base64 string or null",
            )),
        };
    }
    // Array value — used for `$in` / `col = ANY($N)` filters. (#121)
    // Bind a *typed* array matching the column type: PostgreSQL does NOT
    // implicitly cast a `text[]` element to the column type inside `= ANY`, so a
    // `uuid`/`int`/`numeric` column compared against a text array fails at
    // execution. Typed arrays keep the predicate index-usable (no `::type` cast
    // on the column needed). Checked before the scalar branches below so a
    // `uuid`/temporal array isn't misrouted into the scalar (single-value) path.
    if let JsonValue::Array(items) = value {
        if sql_type == "UUID" {
            let mut arr: Vec<Uuid> = Vec::with_capacity(items.len());
            for item in items {
                let parsed = item
                    .as_str()
                    .ok_or_else(|| {
                        postgres_invalid_field(
                            "value",
                            "UUID $in array values must be strings",
                            "UUID $in value must be a string",
                        )
                    })?
                    .parse::<Uuid>()
                    .map_err(|err| {
                        postgres_invalid_field(
                            "value",
                            "UUID $in array values must be valid UUID strings",
                            format!("invalid UUID in $in: {err}"),
                        )
                    })?;
                arr.push(parsed);
            }
            return Ok(query.bind(arr));
        }
        if postgres_is_integer_type(&sql_type) {
            let mut arr: Vec<i64> = Vec::with_capacity(items.len());
            for item in items {
                arr.push(postgres_json_i64("value", item)?);
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
                arr.push(postgres_json_f64("value", item)?);
            }
            return Ok(query.bind(arr));
        }
        if sql_type.contains("BOOL") {
            let arr: Vec<bool> = items.iter().map(|i| i.as_bool().unwrap_or(false)).collect();
            return Ok(query.bind(arr));
        }
        // text / varchar / enum / timestamp and friends: a text[] compares
        // correctly for text-typed columns (`text = ANY(text[])`).
        let arr: Vec<String> = items
            .iter()
            .map(json_scalar_to_string)
            .map(|s| strip_nul(&s))
            .collect();
        return Ok(query.bind(arr));
    }

    // Typed scalar binds for the column types that have NO implicit/assignment
    // cast from `text` in PostgreSQL: uuid, timestamptz, timestamp, date.
    // Binding one of these as text — or as a *text-typed* NULL, which the old
    // fallback did — makes an INSERT/upsert that supplies the column fail to
    // PLAN with SQLSTATE 42804 (datatype_mismatch), which surfaced only as an
    // opaque INTERNAL. A fresh INSERT omits the server-defaulted audit columns
    // (created_at/updated_at fill from CURRENT_TIMESTAMP) so never hit this; a
    // read-modify-write (Select → edit one field → Upsert) re-supplies those
    // columns as the RFC-3339 strings the SELECT serializer emits, and did.
    // Bind the real Rust type, mirroring the compiler bind path in
    // `core/helpers.rs`.
    if sql_type == "UUID" {
        return match value {
            JsonValue::Null => Ok(query.bind(Option::<Uuid>::None)),
            JsonValue::String(raw) if raw.trim().is_empty() => Ok(query.bind(Option::<Uuid>::None)),
            JsonValue::String(raw) => {
                raw.parse::<Uuid>()
                    .map(|uuid| query.bind(uuid))
                    .map_err(|err| {
                        postgres_invalid_field(
                            "value",
                            "UUID value must be a valid UUID string",
                            format!("invalid UUID: {err}"),
                        )
                    })
            }
            _ => Err(postgres_invalid_field(
                "value",
                "UUID value must be a string",
                "UUID value must be a string",
            )),
        };
    }
    if sql_type.contains("TIMESTAMPTZ") || sql_type.contains("TIMESTAMP WITH TIME ZONE") {
        return match value {
            JsonValue::Null => Ok(query.bind(Option::<chrono::DateTime<chrono::Utc>>::None)),
            JsonValue::String(raw) if raw.trim().is_empty() => {
                Ok(query.bind(Option::<chrono::DateTime<chrono::Utc>>::None))
            }
            JsonValue::String(raw) => chrono::DateTime::parse_from_rfc3339(raw)
                .map(|dt| query.bind(dt.with_timezone(&chrono::Utc)))
                .map_err(|err| {
                    postgres_invalid_field(
                        "value",
                        "timestamptz value must be an RFC3339 string",
                        format!("timestamptz value must be an RFC3339 string: {err}"),
                    )
                }),
            _ => Err(postgres_invalid_field(
                "value",
                "timestamptz value must be a string or null",
                "timestamptz value must be a string or null",
            )),
        };
    }
    if sql_type.contains("TIMESTAMP") {
        return match value {
            JsonValue::Null => Ok(query.bind(Option::<chrono::NaiveDateTime>::None)),
            JsonValue::String(raw) if raw.trim().is_empty() => {
                Ok(query.bind(Option::<chrono::NaiveDateTime>::None))
            }
            JsonValue::String(raw) => parse_naive_datetime(raw)
                .map(|dt| query.bind(dt))
                .ok_or_else(|| {
                    postgres_invalid_field(
                        "value",
                        "timestamp value must be an ISO-8601 string or null",
                        "timestamp value must be an ISO-8601 string or null",
                    )
                }),
            _ => Err(postgres_invalid_field(
                "value",
                "timestamp value must be a string or null",
                "timestamp value must be a string or null",
            )),
        };
    }
    if sql_type.contains("DATE") {
        return match value {
            JsonValue::Null => Ok(query.bind(Option::<chrono::NaiveDate>::None)),
            JsonValue::String(raw) if raw.trim().is_empty() => {
                Ok(query.bind(Option::<chrono::NaiveDate>::None))
            }
            JsonValue::String(raw) => raw
                .parse::<chrono::NaiveDate>()
                .map(|date| query.bind(date))
                .map_err(|err| {
                    postgres_invalid_field(
                        "value",
                        "date value must be a valid date string",
                        format!("invalid date: {err}"),
                    )
                }),
            _ => Err(postgres_invalid_field(
                "value",
                "date value must be a string or null",
                "date value must be a string or null",
            )),
        };
    }

    // PostGIS spatial and network-address columns take the text bind below, but
    // ONLY because their placeholder carries a `::TYPE` cast in the emitted SQL
    // (see `postgres_type_needs_sql_cast`). A NULL must still bind as a text
    // NULL rather than the typed NULLs chosen further down, so that
    // `$n::GEOGRAPHY` receives something it can cast.
    if postgres_type_needs_sql_cast(&sql_type) {
        return match value {
            JsonValue::Null => Ok(query.bind(Option::<String>::None)),
            JsonValue::String(raw) if raw.trim().is_empty() => {
                Ok(query.bind(Option::<String>::None))
            }
            JsonValue::String(raw) => Ok(query.bind(strip_nul(raw))),
            _ => Err(postgres_invalid_field(
                "value",
                "value must be a string or null",
                format!(
                    "{} value must be a string or null",
                    postgres_base_sql_type(&sql_type).to_ascii_lowercase()
                ),
            )),
        };
    }

    // Remaining scalar types. A NULL binds as the matching nullable Rust type so
    // the parameter's Postgres type is unambiguous (bool/int/float each get a
    // typed NULL; text/varchar/enum accept a plain text NULL).
    if value.is_null() {
        if sql_type.contains("BOOL") {
            return Ok(query.bind(Option::<bool>::None));
        }
        if postgres_is_integer_type(&sql_type) {
            return Ok(query.bind(Option::<i64>::None));
        }
        if sql_type.contains("REAL")
            || sql_type.contains("DOUBLE")
            || sql_type.contains("FLOAT")
            || sql_type.contains("NUMERIC")
            || sql_type.contains("DECIMAL")
        {
            return Ok(query.bind(Option::<f64>::None));
        }
        return Ok(query.bind(Option::<String>::None));
    }
    if sql_type.contains("BOOL") {
        return Ok(query.bind(value.as_bool().unwrap_or(false)));
    }
    if postgres_is_integer_type(&sql_type) {
        return Ok(query.bind(postgres_json_i64("value", value)?));
    }
    if sql_type.contains("REAL")
        || sql_type.contains("DOUBLE")
        || sql_type.contains("FLOAT")
        || sql_type.contains("NUMERIC")
        || sql_type.contains("DECIMAL")
    {
        return Ok(query.bind(postgres_json_f64("value", value)?));
    }
    Ok(query.bind(strip_nul(&json_scalar_to_string(value))))
}

/// Parse the datetime string forms the SELECT serializer can emit for a
/// `TIMESTAMP` (no time zone) column back into a `NaiveDateTime`. `row_value_to_json`
/// renders these via `NaiveDateTime::to_string()` (space-separated), but accept the
/// ISO `T` form too so a client-built payload round-trips as well.
fn parse_naive_datetime(raw: &str) -> Option<chrono::NaiveDateTime> {
    const FORMATS: &[&str] = &[
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ];
    FORMATS
        .iter()
        .find_map(|fmt| chrono::NaiveDateTime::parse_from_str(raw, fmt).ok())
}

/// A NUL (`0x00`) byte cannot be stored in a Postgres `text`/`varchar`/`json(b)`
/// value. Strip it at the typed-record bind edge so a hostile/garbage byte cannot
/// fault the whole upsert with Postgres' UTF-8 NUL rejection (B14). This path does
/// NOT go through `bind_generic_pg_param` (which already strips), so it needs its
/// own guard.
fn strip_nul(s: &str) -> String {
    if s.contains('\u{0}') {
        s.replace('\u{0}', "")
    } else {
        s.to_string()
    }
}

fn strip_nul_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::String(s) if s.contains('\u{0}') => JsonValue::String(s.replace('\u{0}', "")),
        JsonValue::Array(items) => JsonValue::Array(items.iter().map(strip_nul_json).collect()),
        JsonValue::Object(map) => JsonValue::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), strip_nul_json(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}

// ── Record serialisation ──────────────────────────────────────────────────────

pub(crate) fn upsert_record_json(request: &UpsertRequest) -> Result<JsonValue, tonic::Status> {
    if let Some(payload) = &request.payload {
        return Ok(struct_to_json(payload));
    }
    if !request.record_json.is_empty() {
        return serde_json::from_slice(&request.record_json).map_err(|err| {
            postgres_invalid_field(
                "record_json",
                "must contain valid JSON",
                format!("record_json must be valid JSON: {err}"),
            )
        });
    }
    Err(postgres_invalid_field(
        "payload",
        "payload or record_json must be provided",
        "payload or record_json is required",
    ))
}

pub(crate) fn mutation_record_json(mutation: &Mutation) -> Result<JsonValue, tonic::Status> {
    if let Some(payload) = &mutation.payload {
        return Ok(struct_to_json(payload));
    }
    if !mutation.record_json.is_empty() {
        return serde_json::from_slice(&mutation.record_json).map_err(|err| {
            postgres_invalid_field(
                "record_json",
                "must contain valid JSON",
                format!("record_json must be valid JSON: {err}"),
            )
        });
    }
    Err(postgres_invalid_field(
        "payload",
        "payload or record_json must be provided",
        "payload or record_json is required",
    ))
}

pub(crate) fn record_values(
    record: &JsonValue,
    columns: &[String],
) -> Result<Vec<JsonValue>, tonic::Status> {
    let object = record.as_object().ok_or_else(|| {
        postgres_invalid_field(
            "record",
            "must be a JSON object",
            "record must be a JSON object",
        )
    })?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::{CatalogManifest, ManifestForeignKey, ManifestTableSecurity};
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use serde_json::json;

    #[test]
    fn geography_point_is_not_classified_as_an_integer_type() {
        // Every supported integer spelling, scalar and array, still classifies.
        for integer in [
            "SMALLINT",
            "INT2",
            "INTEGER",
            "INT",
            "INT4",
            "BIGINT",
            "INT8",
            "SMALLSERIAL",
            "SERIAL2",
            "SERIAL",
            "SERIAL4",
            "BIGSERIAL",
            "SERIAL8",
            "BIGINT[]",
            "INTEGER NOT NULL",
        ] {
            assert!(
                postgres_is_integer_type(integer),
                "{integer} must classify as an integer type"
            );
        }

        // The defect: these all contain `INT` but are not integer columns.
        for other in [
            "GEOGRAPHY(POINT,4326)",
            "GEOMETRY(POINT,4326)",
            "GEOMETRY(MULTIPOINT,4326)",
            "GEOGRAPHY(POINT,4326)[]",
            "POINT",
            "INTERVAL",
        ] {
            assert!(
                !postgres_is_integer_type(other),
                "{other} must not classify as an integer type"
            );
        }
    }

    /// `Update` has no bridged IR emitter, so its SQL comes from the planner and
    /// its values bind through `bind_one`. Both halves must agree: the planner
    /// casts the placeholder (`$n::GEOGRAPHY`) and this binder must hand that
    /// cast a TEXT value, never a typed NULL or a stringified number.
    #[test]
    fn no_text_cast_columns_bind_as_text_for_their_sql_cast() {
        for declaration in [
            "GEOGRAPHY(POINT,4326)",
            "GEOGRAPHY",
            "GEOMETRY(POINT,4326)",
            "INET",
            "CIDR",
            "MACADDR",
            "MACADDR8",
        ] {
            assert!(
                postgres_type_needs_sql_cast(declaration),
                "{declaration} must be bound for a SQL cast"
            );
        }
        // Handled natively by the branches above — must keep those binds.
        for other in [
            "TEXT",
            "INTEGER",
            "JSONB",
            "UUID",
            "TIMESTAMPTZ",
            "DATE",
            "BYTEA",
        ] {
            assert!(!postgres_type_needs_sql_cast(other), "{other}");
        }

        // A geography column must never reach the plain-text fallback, and a
        // non-string payload fails closed rather than being stringified.
        let mut geography = col("location");
        geography.sql_type = "GEOGRAPHY(POINT,4326)".to_string();
        for value in [
            json!("0101000020e6100000fc1873d7129a564080b74082e2c73740"),
            json!("SRID=4326;POINT(90.40725 23.7995375)"),
            json!(null),
            json!(""),
        ] {
            assert!(
                bind_one(sqlx::query("SELECT $1"), Some(&geography), &value).is_ok(),
                "geography must bind {value}"
            );
        }
        let err = expect_status(bind_one(
            sqlx::query("SELECT $1"),
            Some(&geography),
            &json!(42),
        ));
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "geography value must be a string or null");
    }

    /// BINDING GUARD for the geography Update fix — runs on every build, with no
    /// live database.
    ///
    /// The fix has two halves that must stay in sync:
    ///   1. the planner casts the assignment placeholder (`$n::GEOGRAPHY`), and
    ///   2. this binder hands that cast a TEXT value.
    ///
    /// Change either half alone and the served Update silently breaks again —
    /// SQLSTATE 42804 if the cast is dropped, SQLSTATE 22P03 if the bind stops
    /// being text — and only a PostGIS-enabled live run would notice. This test
    /// fails the build the moment they disagree.
    #[test]
    fn every_text_bound_column_type_is_cast_by_the_update_planner() {
        for declaration in [
            "GEOGRAPHY(POINT,4326)",
            "GEOMETRY(POINT,4326)",
            "GEOGRAPHY",
            "GEOMETRY",
            "INET",
            "CIDR",
            "MACADDR",
            "MACADDR8",
        ] {
            assert!(
                postgres_type_needs_sql_cast(declaration),
                "{declaration} must bind as text"
            );
            let rendered =
                crate::ir::compile::postgres::cast_placeholder_to_column_type(declaration, "$1");
            assert_ne!(
                rendered, "$1",
                "{declaration} binds as text, so the planner MUST cast its placeholder \
                 or the served Update fails with SQLSTATE 42804"
            );
            assert_eq!(
                rendered,
                format!(
                    "$1::{}",
                    postgres_base_sql_type(&declaration.to_ascii_uppercase())
                ),
                "the cast must name the column's base type"
            );
        }
    }

    /// SOURCE RATCHET: the defect was a substring type classifier
    /// (`sql_type.contains("INT")` matching the `INT` inside `POINT`). Classify
    /// the base type token instead. This fails the build if substring
    /// classification is reintroduced for the integer/serial family.
    #[test]
    fn integer_dispatch_never_returns_to_substring_matching() {
        // Scan the PRODUCTION half only. `include_str!` yields the raw file, so
        // the patterns this test names would otherwise match themselves.
        let production = include_str!("postgres_helpers.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production half of the module");
        for forbidden in [
            "contains(\"INT\")",
            "contains(\"INTEGER\")",
            "contains(\"INT2\")",
            "contains(\"INT4\")",
            "contains(\"INT8\")",
            "contains(\"SERIAL\")",
            "contains(\"BIGSERIAL\")",
            "contains(\"GEOGRAPHY\")",
            "contains(\"GEOMETRY\")",
        ] {
            assert!(
                !production.contains(forbidden),
                "{forbidden} is a substring type classifier, and \
                 `GEOGRAPHY(POINT,4326)` contains `INT` inside `POINT`. Use \
                 postgres_base_sql_type() and match the base type token instead."
            );
        }
    }

    /// The cast is only correct if the value arrives as TEXT. A typed NULL would
    /// make `$n::GEOGRAPHY` fail, so NULL must bind as a text NULL here.
    #[test]
    fn cast_bound_columns_bind_null_as_text_not_as_a_typed_null() {
        let mut inet = col("last_seen_ip");
        inet.sql_type = "INET".to_string();
        for value in [json!(null), json!("")] {
            assert!(
                bind_one(sqlx::query("SELECT $1::INET"), Some(&inet), &value).is_ok(),
                "inet must bind {value} as a castable text null"
            );
        }
    }
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

    fn assert_schema_detail(
        status: &tonic::Status,
        backend: &str,
        operation: &str,
        schema_code: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, backend);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, schema_code);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_internal_detail(
        status: &tonic::Status,
        backend: &str,
        operation: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, backend);
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn expect_status<T>(result: Result<T, tonic::Status>) -> tonic::Status {
        match result {
            Ok(_) => panic!("expected validation status"),
            Err(status) => status,
        }
    }

    fn ctx() -> RequestContext {
        RequestContext {
            tenant_id: "acme".to_string(),
            ..RequestContext::default()
        }
    }

    fn col(name: &str) -> ManifestColumn {
        ManifestColumn {
            field_name: name.to_string(),
            column_name: name.to_string(),
            proto_type: "string".to_string(),
            sql_type: "text".to_string(),
            ..ManifestColumn::default()
        }
    }

    fn tenant_col(field_name: &str, column_name: &str, flagged: bool) -> ManifestColumn {
        ManifestColumn {
            field_name: field_name.to_string(),
            column_name: column_name.to_string(),
            proto_type: "string".to_string(),
            sql_type: "text".to_string(),
            is_tenant_column: flagged,
            ..ManifestColumn::default()
        }
    }

    fn table(message: &str, physical: &str, columns: Vec<ManifestColumn>) -> ManifestTable {
        ManifestTable {
            message_name: format!("acme.test.v1.{message}"),
            schema: "public".to_string(),
            table: physical.to_string(),
            columns,
            primary_key: vec!["id".to_string()],
            ..ManifestTable::default()
        }
    }

    fn join_manifest(mut left: ManifestTable, right: ManifestTable) -> CatalogManifest {
        left.foreign_keys.push(ManifestForeignKey {
            name: "fk_right".to_string(),
            columns: vec!["right_id".to_string()],
            ref_schema: right.schema.clone(),
            ref_table: right.table.clone(),
            ref_columns: vec!["id".to_string()],
            ..ManifestForeignKey::default()
        });
        CatalogManifest {
            tables: vec![left, right],
            ..CatalogManifest::default()
        }
    }

    fn join_request() -> SelectRequest {
        SelectRequest {
            message_type: "Left,Right".to_string(),
            limit: 25,
            ..SelectRequest::default()
        }
    }

    #[test]
    fn tenant_column_ref_prefers_declared_table_security_column() {
        let mut table = table(
            "Left",
            "lefts",
            vec![
                col("id"),
                tenant_col("tenant_id", "tenant_id", true),
                tenant_col("account", "account_id", false),
            ],
        );
        table.table_security = ManifestTableSecurity {
            tenant_column: "account".to_string(),
            ..ManifestTableSecurity::default()
        };

        let resolved = tenant_column_ref(&table).expect("tenant column");

        assert_eq!(resolved.column_name, "account_id");
    }

    #[test]
    fn tenant_column_ref_uses_system_and_legacy_names() {
        let system = table(
            "Left",
            "lefts",
            vec![col("id"), tenant_col("_tenant_id", "_tenant_id", false)],
        );
        assert_eq!(
            tenant_column_ref(&system).map(|column| column.column_name.as_str()),
            Some("_tenant_id")
        );

        let legacy = table(
            "Left",
            "lefts",
            vec![col("id"), tenant_col("org_id", "organization_id", false)],
        );
        assert_eq!(
            tenant_column_ref(&legacy).map(|column| column.column_name.as_str()),
            Some("organization_id")
        );
    }

    #[test]
    fn join_fusion_adds_tenant_predicate_for_every_joined_tenant_table() {
        let mut left = table(
            "Left",
            "lefts",
            vec![
                col("id"),
                col("right_id"),
                tenant_col("tenant_id", "tenant_id", true),
            ],
        );
        left.enable_rls = true;
        let mut right = table(
            "Right",
            "rights",
            vec![col("id"), tenant_col("tenant_id", "tenant_id", true)],
        );
        right.enable_rls = true;
        let manifest = join_manifest(left, right);

        let plan =
            build_join_fusion_sql(&manifest, &join_request(), &ctx(), &JsonValue::Null).unwrap();

        assert!(
            plan.sql.contains(r#""t0"."tenant_id" = $1"#),
            "{}",
            plan.sql
        );
        assert!(
            plan.sql.contains(r#""t1"."tenant_id" = $2"#),
            "{}",
            plan.sql
        );
        assert_eq!(plan.bindings.len(), 2);
        assert_eq!(plan.bindings[0].1, JsonValue::String("acme".to_string()));
        assert_eq!(plan.bindings[1].1, JsonValue::String("acme".to_string()));
    }

    #[test]
    fn join_fusion_fails_closed_for_scoped_table_without_tenant_column() {
        let mut left = table("Left", "lefts", vec![col("id"), col("right_id")]);
        left.enable_rls = true;
        let right = table("Right", "rights", vec![col("id")]);
        let manifest = join_manifest(left, right);

        let err = build_join_fusion_sql(&manifest, &join_request(), &ctx(), &JsonValue::Null)
            .err()
            .expect("scoped table without tenant column must fail closed");

        assert_schema_detail(
            &err,
            "postgres",
            "join_fusion",
            "tenant_column_required",
            "join fusion cannot safely select scoped table public.lefts without a tenant column",
        );
    }

    #[test]
    fn postgres_internal_status_carries_typed_detail() {
        let status = postgres_internal_status("execute_tx_plan", "transaction mutation failed");

        assert_internal_detail(
            &status,
            "postgres",
            "execute_tx_plan",
            "transaction mutation failed",
        );
    }

    #[test]
    fn join_fusion_validation_carries_field_violations() {
        let mut request = join_request();
        request.message_type = "Left".to_string();
        let manifest = CatalogManifest::default();

        let err = build_join_fusion_sql(&manifest, &request, &ctx(), &JsonValue::Null)
            .err()
            .expect("single message join must fail");

        assert_single_field_violation(
            &err,
            "message_type",
            "must contain at least two comma- or plus-separated message types",
        );

        request.message_type = "Left,Missing".to_string();
        let manifest = CatalogManifest {
            tables: vec![table("Left", "lefts", vec![col("id")])],
            ..CatalogManifest::default()
        };
        let err = build_join_fusion_sql(&manifest, &request, &ctx(), &JsonValue::Null)
            .err()
            .expect("unknown message type must fail");

        assert_single_field_violation(
            &err,
            "message_type",
            "must match exactly one manifest table message type",
        );
    }

    /// DAT-001: a top-level JSON array destined for a JSON/JSONB column must bind
    /// as JSON (verbatim), NOT be misrouted into the typed `text[]` filter-array
    /// path (which fails at PostgreSQL with 42846). The JSON decision must precede
    /// the array branch, so a JSONB column accepts an array of strings, an array
    /// of objects, and an empty array without string-wrapping — while a scalar
    /// column still gets its typed `$in` array.
    #[test]
    fn postgres_binds_json_arrays_into_jsonb_columns() {
        let mut jsonb = col("permissions_json");
        jsonb.sql_type = "JSONB".to_string();
        // Array of strings, array of objects, and an empty array all bind OK for a
        // JSONB target (before the fix these hit the typed text[] path).
        for value in [
            serde_json::json!(["partner.fleet.read", "partner.fleet.write"]),
            serde_json::json!([{"role": "reader"}, {"role": "writer"}]),
            serde_json::json!([]),
            serde_json::json!({"nested": ["a", "b"]}),
        ] {
            assert!(
                bind_one(sqlx::query("SELECT $1"), Some(&jsonb), &value).is_ok(),
                "JSONB column must accept JSON value {value} without a text[] misbind"
            );
        }
        // A scalar UUID column still routes an array into the typed $in path and
        // rejects non-string elements there (proves the array branch is intact).
        let mut uuid_col = col("id");
        uuid_col.sql_type = "UUID".to_string();
        assert!(
            bind_one(
                sqlx::query("SELECT $1"),
                Some(&uuid_col),
                &serde_json::json!([1])
            )
            .is_err(),
            "a scalar UUID $in array must still validate its elements"
        );
    }

    /// W6 / bug #2: a `BYTEA` column receives proto `bytes` as a base64 STRING
    /// (the SDKs' proto3-JSON encoding AND the SELECT serializer's base64 read
    /// output). The bind side must base64-DECODE it into native `bytea`, symmetric
    /// with the read path — NULL, a present-but-empty value, and an empty `[]` all
    /// bind, while malformed base64 fails closed with a typed invalid-argument
    /// violation instead of silently binding the raw ASCII.
    #[test]
    fn postgres_binds_bytea_column_from_base64() {
        let mut bytea = col("blob");
        bytea.sql_type = "BYTEA".to_string();

        for value in [
            serde_json::json!("aGVsbG8="), // base64 of "hello"
            JsonValue::Null,
            serde_json::json!(""), // present-but-empty → zero-length bytes, not NULL
            serde_json::json!([]),
        ] {
            assert!(
                bind_one(sqlx::query("SELECT $1"), Some(&bytea), &value).is_ok(),
                "BYTEA column must accept base64/null/empty value {value}"
            );
        }

        // Malformed base64 fails closed rather than binding the base64 ASCII text.
        let err = expect_status(bind_one(
            sqlx::query("SELECT $1"),
            Some(&bytea),
            &serde_json::json!("not valid base64 !!!"),
        ));
        assert_single_field_violation(&err, "value", "bytea value must be a base64-encoded string");
    }

    #[test]
    fn postgres_bind_validation_carries_field_violations() {
        let table = table("Left", "lefts", vec![col("id")]);
        let err = expect_status(bind_values(
            sqlx::query("SELECT 1"),
            &table,
            &["id".to_string()],
            &[],
        ));

        assert_single_field_violation(
            &err,
            "values",
            "number of values must match number of columns",
        );

        let mut uuid_col = col("id");
        uuid_col.sql_type = "UUID".to_string();
        let err = expect_status(bind_one(
            sqlx::query("SELECT $1"),
            Some(&uuid_col),
            &serde_json::json!([1]),
        ));

        assert_single_field_violation(&err, "value", "UUID $in array values must be strings");

        let mut int_col = col("age");
        int_col.sql_type = "BIGINT".to_string();
        let err = expect_status(bind_one(
            sqlx::query("SELECT $1"),
            Some(&int_col),
            &serde_json::json!("not-an-int"),
        ));

        // The constraint now also admits a whole number: protobuf Struct delivers a
        // client's native integer as a double, so 25.0 must bind like 25 (see
        // integer_binding_accepts_a_native_client_integer_from_protobuf_struct). A
        // non-numeric string is still refused.
        assert_single_field_violation(
            &err,
            "value",
            "must be an integer, an integer string, or a whole number",
        );
    }

    #[test]
    fn record_json_validation_carries_field_violations() {
        let upsert = UpsertRequest {
            record_json: b"{".to_vec(),
            ..UpsertRequest::default()
        };
        let err = upsert_record_json(&upsert)
            .err()
            .expect("invalid record_json must fail");

        assert_single_field_violation(&err, "record_json", "must contain valid JSON");

        let mutation = Mutation::default();
        let err = mutation_record_json(&mutation)
            .err()
            .expect("missing mutation payload must fail");

        assert_single_field_violation(&err, "payload", "payload or record_json must be provided");

        let err = record_values(&JsonValue::Array(Vec::new()), &["id".to_string()])
            .err()
            .expect("non-object record must fail");

        assert_single_field_violation(&err, "record", "must be a JSON object");
    }

    /// Regression: Upsert and Update must agree on the SAME column. protobuf `Struct`
    /// carries every number as a double, so a native client integer reaches the binder
    /// as `25.0`; Update used to refuse it ("expected integer, got 25.0") while Upsert
    /// accepted it, which made the correct encoding depend on which verb you called.
    #[test]
    fn integer_binding_accepts_a_native_client_integer_from_protobuf_struct() {
        for value in [json!(25), json!(25.0), json!("25")] {
            assert_eq!(
                postgres_json_i64("bed_count", &value).expect("integral value must bind"),
                25,
                "value {value} must bind as 25 on every mutation verb"
            );
        }
        assert_eq!(
            postgres_json_i64("zero", &json!(0.0)).expect("0.0 binds"),
            0
        );
        assert_eq!(
            postgres_json_i64("negative", &json!(-7.0)).expect("-7.0 binds"),
            -7
        );

        // Still strict where it matters: a FRACTIONAL double is a real precision loss
        // and must be refused, never silently truncated to 25.
        let err = postgres_json_i64("bed_count", &json!(25.5))
            .expect_err("a fractional double must not bind to an integer column");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}
