use crate::generation::GeneratedArtifact;
use crate::generation::manifest::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::generation::sql::SqlGenerationConfig;

// These take the already-built `&CatalogManifest` (not raw schemas) so a
// multi-backend sync builds the manifest once and threads it to every backend
// instead of rebuilding it per backend via `from_schemas` (#82). The `Result`
// return is retained for signature uniformity with the other generators.

pub fn generate_mysql_artifacts(
    manifest: &CatalogManifest,
    config: &SqlGenerationConfig,
) -> Result<Vec<GeneratedArtifact>, serde_json::Error> {
    Ok(manifest
        .tables
        .iter()
        .map(|table| sql_artifact(table, "mysql", config, render_mysql_table(table)))
        .collect())
}

pub fn generate_sqlite_artifacts(
    manifest: &CatalogManifest,
    config: &SqlGenerationConfig,
) -> Result<Vec<GeneratedArtifact>, serde_json::Error> {
    Ok(manifest
        .tables
        .iter()
        .map(|table| sql_artifact(table, "sqlite", config, render_sqlite_table(table)))
        .collect())
}

pub fn generate_mssql_artifacts(
    manifest: &CatalogManifest,
    config: &SqlGenerationConfig,
) -> Result<Vec<GeneratedArtifact>, serde_json::Error> {
    Ok(manifest
        .tables
        .iter()
        .map(|table| sql_artifact(table, "mssql", config, render_mssql_table(table)))
        .collect())
}

fn sql_artifact(
    table: &ManifestTable,
    backend: &str,
    config: &SqlGenerationConfig,
    content: String,
) -> GeneratedArtifact {
    GeneratedArtifact {
        rel_path: format!(
            "{}/001_{}.sql",
            safe_ident(&table.schema),
            safe_ident(&table.table)
        ),
        kind: format!("bootstrap_{backend}"),
        schema: table.schema.clone(),
        table: table.table.clone(),
        content: format!(
            "-- UDB:migration_kind=bootstrap\n-- UDB:backend={backend}\n-- UDB:generator={}\n\n{content}\n",
            config.generator_name
        ),
    }
}

fn render_mysql_table(table: &ManifestTable) -> String {
    let mut lines = table
        .columns
        .iter()
        .map(|column| {
            format!(
                "    {} {}{}{}",
                bt(&column.column_name),
                mysql_type(column),
                null_clause(column),
                default_clause(column)
            )
        })
        .collect::<Vec<_>>();
    if !table.primary_key.is_empty() {
        lines.push(format!(
            "    PRIMARY KEY ({})",
            table
                .primary_key
                .iter()
                .map(|c| bt(c))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    format!(
        "CREATE TABLE IF NOT EXISTS {} (\n{}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
        bt(&table.table),
        lines.join(",\n")
    )
}

fn render_sqlite_table(table: &ManifestTable) -> String {
    let mut lines = table
        .columns
        .iter()
        .map(|column| {
            format!(
                "    {} {}{}{}",
                dq(&column.column_name),
                sqlite_type(column),
                null_clause(column),
                default_clause(column)
            )
        })
        .collect::<Vec<_>>();
    if !table.primary_key.is_empty() {
        lines.push(format!(
            "    PRIMARY KEY ({})",
            table
                .primary_key
                .iter()
                .map(|c| dq(c))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    format!(
        "CREATE TABLE IF NOT EXISTS {} (\n{}\n);",
        dq(&table.table),
        lines.join(",\n")
    )
}

fn render_mssql_table(table: &ManifestTable) -> String {
    let schema = safe_ident(&table.schema);
    let table_name = safe_ident(&table.table);
    let mut lines = table
        .columns
        .iter()
        .map(|column| {
            format!(
                "    {} {}{}{}",
                br(&column.column_name),
                mssql_type(column),
                null_clause(column),
                default_clause(column)
            )
        })
        .collect::<Vec<_>>();
    if !table.primary_key.is_empty() {
        lines.push(format!(
            "    CONSTRAINT {} PRIMARY KEY ({})",
            br(&format!("pk_{table_name}")),
            table
                .primary_key
                .iter()
                .map(|c| br(c))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    format!(
        "IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA {schema_quoted}');\nIF OBJECT_ID(N'{schema}.{table_name}', N'U') IS NULL\nBEGIN\nCREATE TABLE {schema_quoted}.{table_quoted} (\n{}\n);\nEND;",
        lines.join(",\n"),
        schema_quoted = br(&schema),
        table_quoted = br(&table_name),
    )
}

fn mysql_type(column: &ManifestColumn) -> String {
    match column.sql_type.to_ascii_lowercase().as_str() {
        "uuid" => "CHAR(36)".to_string(),
        "text" => "TEXT".to_string(),
        "boolean" | "bool" => "BOOLEAN".to_string(),
        "json" | "jsonb" => "JSON".to_string(),
        "timestamptz" | "timestamp with time zone" => "TIMESTAMP".to_string(),
        other if other.is_empty() => "TEXT".to_string(),
        _ => column.sql_type.clone(),
    }
}

fn sqlite_type(column: &ManifestColumn) -> String {
    match column.sql_type.to_ascii_lowercase().as_str() {
        "uuid" | "text" | "json" | "jsonb" | "timestamptz" | "timestamp with time zone" => {
            "TEXT".to_string()
        }
        "bigint" | "integer" | "int" | "serial" | "bigserial" => "INTEGER".to_string(),
        "boolean" | "bool" => "INTEGER".to_string(),
        "numeric" | "decimal" | "double precision" | "real" => "REAL".to_string(),
        other if other.is_empty() => "TEXT".to_string(),
        _ => column.sql_type.clone(),
    }
}

fn mssql_type(column: &ManifestColumn) -> String {
    match column.sql_type.to_ascii_lowercase().as_str() {
        "uuid" => "UNIQUEIDENTIFIER".to_string(),
        "text" => "NVARCHAR(MAX)".to_string(),
        "boolean" | "bool" => "BIT".to_string(),
        "json" | "jsonb" => "NVARCHAR(MAX)".to_string(),
        "timestamptz" | "timestamp with time zone" => "DATETIMEOFFSET".to_string(),
        other if other.is_empty() => "NVARCHAR(MAX)".to_string(),
        _ => column.sql_type.clone(),
    }
}

fn null_clause(column: &ManifestColumn) -> &'static str {
    if column.not_null || column.is_primary {
        " NOT NULL"
    } else {
        ""
    }
}

fn default_clause(column: &ManifestColumn) -> String {
    if column.default_value.trim().is_empty() {
        String::new()
    } else {
        format!(" DEFAULT {}", column.default_value)
    }
}

fn safe_ident(value: &str) -> String {
    let out = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if out.is_empty() {
        "udb".to_string()
    } else {
        out
    }
}

fn dq(value: &str) -> String {
    format!("\"{}\"", safe_ident(value))
}

fn bt(value: &str) -> String {
    format!("`{}`", safe_ident(value))
}

fn br(value: &str) -> String {
    format!("[{}]", safe_ident(value))
}
