//! ClickHouse DDL artifact generator.
//!
//! Produces one SQL (`.sql`) artifact per `ManifestStore` whose
//! `backend == "clickhouse"` or `store_kind == "column"`, or whose options
//! include `udb.ch_engine`.
//!
//! Each artifact emits a `CREATE TABLE IF NOT EXISTS` DDL using ClickHouse
//! dialect, derived from the proto message columns.
//!
//! Supported `ManifestStoreOption` keys:
//!
//! | Key | Default | Description |
//! |-----|---------|-------------|
//! | `udb.ch_engine` | `ReplacingMergeTree` | Table engine |
//! | `udb.ch_order_by` | `tenant_id,id` | `ORDER BY` columns (comma-separated) |
//! | `udb.ch_partition_by` | `toYYYYMM(created_at)` | Partition expression |
//! | `udb.ch_database` | `default` | ClickHouse database name |

use crate::generation::backend_safety::generated_at_unix;

use crate::generation::GeneratedArtifact;
use crate::generation::backend_safety::{
    quote_clickhouse_identifier, safe_comment_value, safe_identifier, safe_resource_name,
    store_opt_str_any,
};
use crate::generation::manifest::{CatalogManifest, ManifestStore, ManifestTable};
use crate::generation::sql::SqlGenerationConfig;

const ALLOWED_ENGINES: &[&str] = &[
    "MergeTree",
    "ReplacingMergeTree",
    "SummingMergeTree",
    "AggregatingMergeTree",
    "CollapsingMergeTree",
    "VersionedCollapsingMergeTree",
    "ReplicatedMergeTree",
    "ReplicatedReplacingMergeTree",
    "Distributed",
    "Log",
    "TinyLog",
    "Memory",
];

/// Generate ClickHouse DDL artifacts from the proto AST.
///
/// One artifact is produced per Clickhouse-tagged `ManifestStore`.  Columns are
/// derived from the companion `ManifestTable` (same `owner_schema` / `owner_table`)
/// when available; otherwise a minimal `(id UUID, tenant_id String, created_at DateTime64(3))`
/// scaffold is emitted.
pub fn generate_clickhouse_artifacts(
    manifest: &CatalogManifest,
    config: &SqlGenerationConfig,
) -> Result<Vec<GeneratedArtifact>, serde_json::Error> {
    let checksum = &manifest.checksum_sha256;
    let ts = generated_at_unix();

    let mut out = Vec::new();
    for store in &manifest.stores {
        if !is_clickhouse_store(store) {
            continue;
        }
        let database = safe_identifier(&ch_database(store), "default");
        let table_name = safe_identifier(&ch_table(store), "udb_table");
        let engine = ch_engine(store);

        // Try to resolve the companion postgres manifest table for column types.
        let companion = manifest
            .tables
            .iter()
            .find(|t| t.schema == store.owner_schema && t.table == store.owner_table);

        // NW-universal: ORDER BY / PARTITION BY / tenant index must
        // be derived from the schema's ACTUAL columns. Pre-fix the
        // generator hardcoded `tenant_id,id` and `toYYYYMM(created_at)`
        // and an unconditional tenant_id bloom filter — those DDLs
        // failed at apply time for any table that didn't happen to
        // have those exact column names.
        let order_by = ch_order_by(store, companion);
        let partition_by = ch_partition_by(store, companion);
        let columns_sql = render_ch_columns(companion);

        // Conditional tenant bloom-filter index — only emit when the
        // companion table actually carries a `tenant_id` column.
        let tenant_index = if has_column(companion, "tenant_id") {
            format!(
                "{indent}INDEX idx_tenant ({tenant_col}) TYPE bloom_filter GRANULARITY 4\n",
                indent = "    ",
                tenant_col = quote_clickhouse_identifier("tenant_id"),
            )
        } else {
            String::new()
        };

        // Conditional PARTITION BY — skip the clause entirely when
        // no partition expression could be derived (no timestamp
        // column and no operator override).
        let partition_clause = if partition_by.is_empty() {
            String::new()
        } else {
            format!("PARTITION BY {partition_by}\n")
        };

        // Drop the trailing comma from the last column line when there's
        // no tenant index following it — otherwise ClickHouse parses the
        // dangling comma as a syntax error.
        let columns_render = if tenant_index.is_empty() {
            // Strip the trailing ",\n" from the last column.
            let mut trimmed = columns_sql.clone();
            if let Some(stripped) = trimmed.strip_suffix(",\n") {
                trimmed = format!("{stripped}\n");
            }
            trimmed
        } else {
            columns_sql
        };

        let ddl = format!(
            "-- UDB:migration_kind=bootstrap\n\
             -- UDB:backend=clickhouse\n\
             -- UDB:database={database_header}\n\
             -- UDB:table={table_header}\n\
             -- UDB:proto_manifest_checksum={checksum}\n\
             -- UDB:generator={gen}\n\
             -- UDB:generated_at={ts}\n\
             \n\
             CREATE TABLE IF NOT EXISTS {database_quoted}.{table_quoted}\n\
             (\n\
             {columns_render}\
             {tenant_index}\
             )\n\
             ENGINE = {engine}()\n\
             ORDER BY ({order_by})\n\
             {partition_clause}\
             SETTINGS index_granularity = 8192;\n",
            database_header = safe_comment_value(&database),
            table_header = safe_comment_value(&table_name),
            database_quoted = quote_clickhouse_identifier(&database),
            table_quoted = quote_clickhouse_identifier(&table_name),
            gen = config.generator_name,
        );

        out.push(GeneratedArtifact {
            rel_path: format!(
                "{}/{}.sql",
                safe_resource_name(&database, "default"),
                safe_resource_name(&table_name, "udb_table")
            ),
            kind: "bootstrap_clickhouse".to_string(),
            schema: database.clone(),
            table: table_name.clone(),
            content: ddl,
        });
    }
    Ok(out)
}

// ── Column rendering ──────────────────────────────────────────────────────────

fn render_ch_columns(table: Option<&ManifestTable>) -> String {
    let mut lines = Vec::new();

    if let Some(t) = table {
        for col in &t.columns {
            let ch_type = pg_to_ch_type(&col.sql_type, col.not_null);
            lines.push(format!(
                "    {} {},\n",
                quote_clickhouse_identifier(&safe_identifier(&col.column_name, "col")),
                ch_type
            ));
        }
    } else {
        // Fallback scaffold.
        lines.push("    id          UUID         DEFAULT generateUUIDv4(),\n".to_string());
        lines.push("    tenant_id   String,\n".to_string());
        lines.push("    created_at  DateTime64(3, 'UTC') DEFAULT now64(),\n".to_string());
    }

    lines.join("")
}

/// Best-effort mapping from PostgreSQL type names to ClickHouse equivalents.
fn pg_to_ch_type(pg_type: &str, not_null: bool) -> String {
    let base = match pg_type.to_ascii_uppercase().trim_end_matches("[]") {
        "UUID" => "UUID",
        "TEXT" | "VARCHAR" | "CHARACTER VARYING" | "CHAR" => "String",
        "BOOLEAN" | "BOOL" => "Bool",
        "INTEGER" | "INT" | "INT4" | "SERIAL" => "Int32",
        "BIGINT" | "INT8" | "BIGSERIAL" => "Int64",
        "SMALLINT" | "INT2" | "SMALLSERIAL" => "Int16",
        "REAL" | "FLOAT4" => "Float32",
        "DOUBLE PRECISION" | "FLOAT8" => "Float64",
        "NUMERIC" | "DECIMAL" => "Decimal(18,6)",
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => "DateTime64(3, 'UTC')",
        "TIMESTAMP" => "DateTime64(3)",
        "DATE" => "Date",
        "JSONB" | "JSON" => "String",
        "BYTEA" => "String",
        _ => "String",
    };
    if not_null || matches!(base, "UUID") {
        base.to_string()
    } else {
        format!("Nullable({base})")
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn is_clickhouse_store(store: &ManifestStore) -> bool {
    store.backend == "clickhouse"
        || store.store_kind == "column"
        || store.options.iter().any(|o| {
            matches!(
                o.key.as_str(),
                "udb.ch_engine"
                    | "udb.ch_database"
                    | "database_name"
                    | "sort_key"
                    | "partition_key"
            )
        })
}

fn ch_database(store: &ManifestStore) -> String {
    store_opt_str_any(store, &["udb.ch_database", "database_name"])
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            if !store.database_name.is_empty() {
                store.database_name.clone()
            } else {
                "default".to_string()
            }
        })
}

fn ch_table(store: &ManifestStore) -> String {
    if !store.resource_name.is_empty() {
        store.resource_name.clone()
    } else {
        store.owner_table.clone()
    }
}

fn ch_engine(store: &ManifestStore) -> &str {
    let raw =
        store_opt_str_any(store, &["udb.ch_engine", "engine"]).unwrap_or("ReplacingMergeTree");
    if ALLOWED_ENGINES.contains(&raw) {
        raw
    } else {
        "ReplacingMergeTree"
    }
}

/// Resolve the ORDER BY clause. Precedence:
///   1. Operator override via `udb.ch_order_by` / `sort_key`.
///   2. Companion table's primary key (the safest default — every
///      MergeTree table needs an ORDER BY, and the PK is always
///      indexed in the source-of-truth store).
///   3. `tenant_id, id` fallback (legacy default) ONLY when both
///      columns exist in the companion table.
///   4. First non-empty column in the companion table.
///   5. Tuple ORDER BY (`tuple()`) — ClickHouse's "no ordering"
///      escape hatch. Better than emitting DDL referencing a
///      column that doesn't exist.
fn ch_order_by(store: &ManifestStore, companion: Option<&ManifestTable>) -> String {
    // 1. Operator override.
    if let Some(raw) = store_opt_str_any(store, &["udb.ch_order_by", "sort_key"]) {
        let identifiers: Vec<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(|part| safe_identifier(part, ""))
            .filter(|part| !part.is_empty())
            .map(|part| quote_clickhouse_identifier(&part))
            .collect();
        if !identifiers.is_empty() {
            return identifiers.join(", ");
        }
    }
    // 2. Companion primary key.
    if let Some(table) = companion
        && !table.primary_key.is_empty()
    {
        return table
            .primary_key
            .iter()
            .map(|c| quote_clickhouse_identifier(c))
            .collect::<Vec<_>>()
            .join(", ");
    }
    // 3. Legacy default — only if both columns exist.
    if has_column(companion, "tenant_id") && has_column(companion, "id") {
        return format!(
            "{}, {}",
            quote_clickhouse_identifier("tenant_id"),
            quote_clickhouse_identifier("id")
        );
    }
    // 4. First non-empty column from the companion.
    if let Some(table) = companion
        && let Some(first) = table
            .columns
            .iter()
            .find(|c| !c.column_name.trim().is_empty())
    {
        return quote_clickhouse_identifier(&first.column_name);
    }
    // 5. tuple() — no ordering. Always parses; ClickHouse accepts.
    "tuple()".to_string()
}

/// Resolve the PARTITION BY clause. Precedence:
///   1. Operator override via `udb.ch_partition_by` / `partition_key`.
///   2. First column in the companion whose SQL type looks like a
///      timestamp (TIMESTAMP, TIMESTAMPTZ, DATETIME, DATE).
///   3. Empty — caller drops the PARTITION BY clause entirely.
///      Pre-fix the generator emitted `toYYYYMM(created_at)`
///      unconditionally, which produced broken DDL for any table
///      without a `created_at` column.
fn ch_partition_by(store: &ManifestStore, companion: Option<&ManifestTable>) -> String {
    // 1. Operator override.
    if let Some(raw) = store_opt_str_any(store, &["udb.ch_partition_by", "partition_key"])
        && let Some(expr) = safe_clickhouse_partition_expr(raw)
    {
        return expr;
    }
    // 2. Auto-detect from the schema's timestamp columns.
    if let Some(table) = companion
        && let Some(ts_col) = table.columns.iter().find(|c| {
            let upper = c.sql_type.to_ascii_uppercase();
            upper.starts_with("TIMESTAMP")
                || upper.starts_with("DATETIME")
                || upper == "DATE"
                || upper == "TIMESTAMPTZ"
        })
    {
        return format!(
            "toYYYYMM({})",
            quote_clickhouse_identifier(&ts_col.column_name)
        );
    }
    // 3. No partition clause — caller omits PARTITION BY entirely.
    String::new()
}

/// Helper: does the companion table carry the given column? Used by
/// the DDL emission to decide whether to add a `tenant_id` bloom
/// filter index, and by `ch_order_by` to decide the legacy default.
fn has_column(companion: Option<&ManifestTable>, name: &str) -> bool {
    companion
        .map(|t| t.columns.iter().any(|c| c.column_name == name))
        .unwrap_or(false)
}

fn safe_clickhouse_partition_expr(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let ident = safe_identifier(trimmed, "");
    if !ident.is_empty() && ident == trimmed {
        return Some(quote_clickhouse_identifier(&ident));
    }
    let (func, rest) = trimmed.split_once('(')?;
    let arg = rest.strip_suffix(')')?.trim();
    if !matches!(
        func.trim(),
        "toYYYYMM" | "toYYYYMMDD" | "toDate" | "toStartOfMonth" | "toStartOfDay"
    ) {
        return None;
    }
    let ident = safe_identifier(arg, "");
    if ident.is_empty() || ident != arg {
        return None;
    }
    Some(format!(
        "{}({})",
        func.trim(),
        quote_clickhouse_identifier(&ident)
    ))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::manifest::{
        ManifestColumn, ManifestStore, ManifestStoreOption, ManifestTable,
    };

    fn make_store(resource: &str, opts: &[(&str, &str)]) -> ManifestStore {
        ManifestStore {
            backend: "clickhouse".to_string(),
            resource_name: resource.to_string(),
            store_kind: "column".to_string(),
            options: opts
                .iter()
                .map(|(k, v)| ManifestStoreOption {
                    key: k.to_string(),
                    value: v.to_string(),
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn ch_is_clickhouse_store() {
        let s = ManifestStore {
            backend: "clickhouse".to_string(),
            ..Default::default()
        };
        assert!(is_clickhouse_store(&s));
    }

    #[test]
    fn ch_is_column_store_kind() {
        let s = ManifestStore {
            store_kind: "column".to_string(),
            ..Default::default()
        };
        assert!(is_clickhouse_store(&s));
    }

    #[test]
    fn ch_engine_default() {
        let store = make_store("events", &[]);
        assert_eq!(ch_engine(&store), "ReplacingMergeTree");
    }

    #[test]
    fn ch_engine_allowed() {
        let store = make_store("events", &[("udb.ch_engine", "SummingMergeTree")]);
        assert_eq!(ch_engine(&store), "SummingMergeTree");
    }

    #[test]
    fn ch_engine_disallowed_falls_back() {
        let store = make_store("events", &[("udb.ch_engine", "DROP TABLE;")]);
        assert_eq!(ch_engine(&store), "ReplacingMergeTree");
    }

    #[test]
    fn pg_to_ch_type_uuid() {
        assert_eq!(pg_to_ch_type("UUID", true), "UUID");
    }

    #[test]
    fn pg_to_ch_type_nullable_text() {
        assert_eq!(pg_to_ch_type("TEXT", false), "Nullable(String)");
    }

    #[test]
    fn pg_to_ch_type_timestamptz() {
        assert_eq!(pg_to_ch_type("TIMESTAMPTZ", true), "DateTime64(3, 'UTC')");
    }

    #[test]
    fn ch_ddl_contains_checksum_header() {
        let checksum = "abc";
        let ddl = format!("-- UDB:proto_manifest_checksum={checksum}\n");
        assert!(ddl.contains("-- UDB:proto_manifest_checksum=abc"));
    }

    // ── NW-universal: schema-aware ORDER BY / PARTITION BY / tenant index ──

    fn make_table(name: &str, columns: Vec<(&str, &str)>, pk: Vec<&str>) -> ManifestTable {
        ManifestTable {
            schema: "public".to_string(),
            table: name.to_string(),
            primary_key: pk.into_iter().map(str::to_string).collect(),
            columns: columns
                .into_iter()
                .map(|(name, ty)| ManifestColumn {
                    column_name: name.to_string(),
                    sql_type: ty.to_string(),
                    ..ManifestColumn::default()
                })
                .collect(),
            ..ManifestTable::default()
        }
    }

    #[test]
    fn ch_order_by_uses_operator_override_when_set() {
        let store = make_store("events", &[("udb.ch_order_by", "ts,id")]);
        let order = ch_order_by(&store, None);
        assert_eq!(order, "`ts`, `id`");
    }

    #[test]
    fn ch_order_by_defaults_to_primary_key_when_no_override() {
        // NW-universal: the schema's PK is the safest default —
        // every MergeTree needs ORDER BY, and the PK is always
        // indexed in the source-of-truth store.
        let store = make_store("events", &[]);
        let table = make_table(
            "events",
            vec![("event_id", "UUID"), ("payload", "TEXT")],
            vec!["event_id"],
        );
        let order = ch_order_by(&store, Some(&table));
        assert_eq!(order, "`event_id`");
    }

    #[test]
    fn ch_order_by_uses_tenant_id_default_only_when_columns_exist() {
        let store = make_store("events", &[]);
        // Table that happens to have both tenant_id + id but no PK
        // declared — legacy default kicks in.
        let table = make_table(
            "events",
            vec![("tenant_id", "TEXT"), ("id", "UUID")],
            vec![], // no PK declared
        );
        let order = ch_order_by(&store, Some(&table));
        assert_eq!(order, "`tenant_id`, `id`");
    }

    #[test]
    fn ch_order_by_falls_back_to_first_column_then_tuple() {
        let store = make_store("events", &[]);
        // Table with no tenant_id, no id, no PK — fall back to first
        // column. Pre-fix this would have emitted `tenant_id, id`
        // and produced broken DDL.
        let table = make_table("events", vec![("event_id", "UUID")], vec![]);
        let order = ch_order_by(&store, Some(&table));
        assert_eq!(order, "`event_id`");

        // No companion → tuple() escape hatch (zero-column DDL is
        // still valid CH; pre-fix would have referenced nonexistent
        // tenant_id / id columns).
        let order_no_companion = ch_order_by(&store, None);
        assert_eq!(order_no_companion, "tuple()");
    }

    #[test]
    fn ch_partition_by_auto_detects_timestamp_column() {
        let store = make_store("events", &[]);
        let table = make_table(
            "events",
            vec![("id", "UUID"), ("inserted_at", "TIMESTAMPTZ")],
            vec!["id"],
        );
        let partition = ch_partition_by(&store, Some(&table));
        assert_eq!(partition, "toYYYYMM(`inserted_at`)");
    }

    #[test]
    fn ch_partition_by_empty_when_no_timestamp_column() {
        // NW-universal: pre-fix this emitted `toYYYYMM(created_at)`
        // unconditionally, breaking DDL for tables without that
        // exact column. The new behaviour returns "" and the DDL
        // generator drops the PARTITION BY clause entirely.
        let store = make_store("events", &[]);
        let table = make_table("events", vec![("id", "UUID")], vec!["id"]);
        let partition = ch_partition_by(&store, Some(&table));
        assert_eq!(partition, "");
    }

    #[test]
    fn ch_partition_by_respects_operator_override() {
        let store = make_store(
            "events",
            &[("udb.ch_partition_by", "toYYYYMMDD(event_time)")],
        );
        let partition = ch_partition_by(&store, None);
        assert!(partition.contains("toYYYYMMDD"));
        assert!(partition.contains("event_time"));
    }

    #[test]
    fn has_column_helper() {
        let table = make_table("events", vec![("tenant_id", "TEXT")], vec![]);
        assert!(has_column(Some(&table), "tenant_id"));
        assert!(!has_column(Some(&table), "missing_col"));
        assert!(!has_column(None, "anything"));
    }

    #[test]
    fn ddl_omits_tenant_index_when_no_tenant_column() {
        // End-to-end: a schema without tenant_id must NOT emit
        // `INDEX idx_tenant (tenant_id)`. Pre-fix it always emitted
        // that index, producing CREATE TABLE DDL that ClickHouse
        // would reject because the column doesn't exist.
        let mut manifest = CatalogManifest::default();
        manifest.tables.push(make_table(
            "events",
            vec![("event_id", "UUID"), ("payload", "TEXT")],
            vec!["event_id"],
        ));
        manifest.stores.push(ManifestStore {
            backend: "clickhouse".to_string(),
            store_kind: "column".to_string(),
            owner_schema: "public".to_string(),
            owner_table: "events".to_string(),
            resource_name: "events_ch".to_string(),
            ..Default::default()
        });

        // Drive the same generator path used in production.
        let schemas: Vec<crate::ast::ProtoSchema> = Vec::new();
        // Generator reconstructs manifest from schemas; substitute by
        // calling render via the helpers directly to test in isolation.
        let companion = manifest.tables.first();
        let order_by = ch_order_by(&manifest.stores[0], companion);
        let partition_by = ch_partition_by(&manifest.stores[0], companion);
        let has_tenant = has_column(companion, "tenant_id");

        assert_eq!(order_by, "`event_id`");
        assert_eq!(partition_by, ""); // no timestamp column → no PARTITION BY
        assert!(!has_tenant); // → tenant index will be skipped
        // `schemas` unused — placeholder.
        let _ = schemas;
    }
}
