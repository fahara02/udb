// src/tracker.rs — DDL for the migration ledger / tracker tables.
//
// These `CREATE TABLE` statements are executed by the Go UDB service during the
// `INITIALISING` FSM phase.  The Rust library exposes them as string constants so
// they can be emitted by the `tracker-ddl` CLI command for inspection and
// for bootstrapping environments that lack the tables.
//
// Aligned with:
//   - legacy_sql  legacycore/db/migrate.go  (ddlMigrationTable, ddlProtoSchemaVersions,
//                                        ddlErrorLogTable, ddlRuntimeStateTable)
//   - UDB spec §16.3.2                  (Double-Entry Ledger)

// ── Ledger 1: Applied migration file tracker ──────────────────────────────────

pub const DEFAULT_LEDGER_SCHEMA: &str = "public";

/// DDL for `public.schema_migrations`.
///
/// Tracks every SQL file that has been applied, its SHA-256 checksum, timing,
/// and linkage back to the proto manifest checksum that generated it.
/// Mirrors `ddlMigrationTable` in legacy_sql `migrate.go`.
pub const DDL_SCHEMA_MIGRATIONS: &str = r#"
CREATE TABLE IF NOT EXISTS public.schema_migrations (
    id                      BIGSERIAL    PRIMARY KEY,
    filename                TEXT         NOT NULL UNIQUE,
    checksum                TEXT         NOT NULL,
    state                   TEXT         NOT NULL DEFAULT 'applied',
    applied_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    applied_by              TEXT         NOT NULL DEFAULT current_user,
    duration_ms             INTEGER      NULL,
    migration_kind          TEXT         NOT NULL DEFAULT 'baseline',
    proto_manifest_checksum TEXT         NULL,
    proto_table_checksum    TEXT         NULL,
    source_schema           TEXT         NULL,
    source_table            TEXT         NULL,
    operation_kind          TEXT         NULL
);

CREATE INDEX IF NOT EXISTS idx_schema_migrations_filename
    ON public.schema_migrations (filename);

CREATE INDEX IF NOT EXISTS idx_schema_migrations_state
    ON public.schema_migrations (state);

CREATE INDEX IF NOT EXISTS idx_schema_migrations_kind
    ON public.schema_migrations (migration_kind);

-- Upgrade guard: extend the table when migrating from a baseline schema.
ALTER TABLE public.schema_migrations
    ADD COLUMN IF NOT EXISTS migration_kind          TEXT NOT NULL DEFAULT 'baseline',
    ADD COLUMN IF NOT EXISTS proto_manifest_checksum TEXT NULL,
    ADD COLUMN IF NOT EXISTS proto_table_checksum    TEXT NULL,
    ADD COLUMN IF NOT EXISTS source_schema           TEXT NULL,
    ADD COLUMN IF NOT EXISTS source_table            TEXT NULL,
    ADD COLUMN IF NOT EXISTS operation_kind          TEXT NULL;
"#;

// ── Ledger 2: Proto AST manifest tracker ─────────────────────────────────────

/// DDL for `public.proto_schema_versions`.
///
/// Tracks every compiled proto AST manifest and its SHA-256 checksum.  The
/// `PROTO_CHECKSUM_LINT` FSM phase queries this table to detect schema drift.
/// Mirrors `ddlProtoSchemaVersions` in legacy_sql `migrate.go`.
pub const DDL_PROTO_SCHEMA_VERSIONS: &str = r#"
CREATE TABLE IF NOT EXISTS public.proto_schema_versions (
    id                BIGSERIAL    PRIMARY KEY,
    manifest_checksum TEXT         NOT NULL UNIQUE,
    manifest_json     JSONB        NOT NULL,
    generator_version TEXT         NOT NULL DEFAULT '1',
    generated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    applied_at        TIMESTAMPTZ  NULL,
    applied_by_file   TEXT         NULL
);

CREATE INDEX IF NOT EXISTS idx_proto_schema_versions_checksum
    ON public.proto_schema_versions (manifest_checksum);

CREATE INDEX IF NOT EXISTS idx_proto_schema_versions_applied_at
    ON public.proto_schema_versions (applied_at)
    WHERE applied_at IS NOT NULL;

-- Composite index to accelerate the ORDER BY applied_at DESC NULLS LAST, id DESC
-- used by load_last_manifest() on every startup. Without this, the query does a
-- full sequential scan of the JSONB column even though only 1 row is needed.
CREATE INDEX IF NOT EXISTS idx_proto_schema_versions_applied_at_id
    ON public.proto_schema_versions (applied_at DESC NULLS LAST, id DESC);
"#;

// ── Error log ─────────────────────────────────────────────────────────────────

/// DDL for `public.migration_error_log`.
///
/// Persists every error the migration FSM encounters, together with the
/// FSM state, file, run ID, and an optional JSON detail blob.
/// Mirrors `ddlErrorLogTable` in legacy_sql `migrate.go`.
pub const DDL_MIGRATION_ERROR_LOG: &str = r#"
CREATE TABLE IF NOT EXISTS public.migration_error_log (
    id            BIGSERIAL    PRIMARY KEY,
    run_id        TEXT         NOT NULL DEFAULT '',
    failed_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    fsm_state     TEXT         NOT NULL,
    filename      TEXT         NULL,
    error_message TEXT         NOT NULL,
    error_detail  JSONB        NULL,
    resolved_at   TIMESTAMPTZ  NULL,
    recovery_note TEXT         NULL
);

CREATE INDEX IF NOT EXISTS idx_migration_error_log_run_id
    ON public.migration_error_log (run_id);

CREATE INDEX IF NOT EXISTS idx_migration_error_log_failed_at
    ON public.migration_error_log (failed_at DESC);

CREATE INDEX IF NOT EXISTS idx_migration_error_log_unresolved
    ON public.migration_error_log (id)
    WHERE resolved_at IS NULL;
"#;

// ── Runtime state ─────────────────────────────────────────────────────────────

/// DDL for `public.migration_runtime_state`.
///
/// A single-row (singleton=TRUE) table that holds the current FSM state and
/// the run ID of any in-progress migration.  The Go UDB service upserts this
/// row after every state transition so that health-check endpoints can expose
/// live migration progress.
pub const DDL_MIGRATION_RUNTIME_STATE: &str = r#"
CREATE TABLE IF NOT EXISTS public.migration_runtime_state (
    singleton     BOOLEAN      PRIMARY KEY DEFAULT TRUE,
    run_id        TEXT         NOT NULL DEFAULT '',
    current_state TEXT         NOT NULL DEFAULT 'IDLE',
    active_file   TEXT         NULL,
    retry_count   INTEGER      NOT NULL DEFAULT 0,
    last_note     TEXT         NULL,
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT migration_runtime_state_singleton_check CHECK (singleton = TRUE)
);

-- Insert the default IDLE row on first bootstrap.
INSERT INTO public.migration_runtime_state (singleton) VALUES (TRUE)
    ON CONFLICT (singleton) DO NOTHING;
"#;

// ── Convenience: emit all DDL in order ───────────────────────────────────────

/// All tracker DDL statements in the order they must be executed.
///
/// Execute them sequentially during the `INITIALISING` FSM phase.
pub const ALL_TRACKER_DDL: &[&str] = &[
    DDL_SCHEMA_MIGRATIONS,
    DDL_PROTO_SCHEMA_VERSIONS,
    DDL_MIGRATION_ERROR_LOG,
    DDL_MIGRATION_RUNTIME_STATE,
];

/// Returns all tracker DDL joined by a blank line separator, ready for
/// output by the `tracker-ddl` CLI command or for direct `psql` execution.
pub fn all_tracker_ddl_sql() -> String {
    all_tracker_ddl_sql_for_schema(DEFAULT_LEDGER_SCHEMA)
}

pub fn all_tracker_ddl_sql_for_schema(schema: &str) -> String {
    let schema = if schema.trim().is_empty() {
        DEFAULT_LEDGER_SCHEMA
    } else {
        schema.trim()
    };
    let escaped = schema.replace('"', "\"\"");
    let mut ddl = format!("CREATE SCHEMA IF NOT EXISTS \"{escaped}\";\n\n");
    ddl.push_str(
        &ALL_TRACKER_DDL
            .join("\n")
            .replace("public.", &format!("\"{escaped}\".")),
    );
    ddl
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_strings_are_non_empty() {
        for ddl in ALL_TRACKER_DDL {
            assert!(!ddl.trim().is_empty(), "tracker DDL must not be empty");
            assert!(
                ddl.to_uppercase().contains("CREATE TABLE IF NOT EXISTS"),
                "each tracker DDL must contain CREATE TABLE IF NOT EXISTS"
            );
        }
    }

    #[test]
    fn all_tracker_ddl_sql_contains_all_tables() {
        let combined = all_tracker_ddl_sql();
        for table in &[
            "schema_migrations",
            "proto_schema_versions",
            "migration_error_log",
            "migration_runtime_state",
        ] {
            assert!(
                combined.contains(table),
                "combined DDL must reference table {table}"
            );
        }
    }
}
