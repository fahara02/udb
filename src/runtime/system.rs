use serde::Serialize;
use sqlx::{Executor, PgPool};
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;

use crate::cdc::CdcConfig;
use crate::runtime::config::UdbConfig;
use crate::runtime::executor_utils::{env_identifier, qi_runtime as qi};

// #214: stored behind an `ArcSwap` so the hot path (`current_arc`) reads the
// installed config with a lock-free refcount bump instead of cloning all ~18
// `String` fields under a Mutex on every call.
static INSTALLED_SYSTEM_CATALOG_CONFIG: OnceLock<ArcSwap<SystemCatalogConfig>> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct SystemCatalogConfig {
    pub cdc: CdcConfig,
    pub abac_schema: String,
    pub abac_table: String,
    pub saga_table: String,
    pub xa_ledger_table: String,
    // Phase 1.1 — Catalog versioning
    pub catalog_versions_table: String,
    pub catalog_activation_log_table: String,
    pub project_catalog_bindings_table: String,
    pub catalog_reload_log_table: String,
    // Phase 1.3 — Migration apply engine
    pub migration_runs_table: String,
    pub migration_op_ledger_table: String,
    // Phase 2.1 — CDC event journal
    pub cdc_journal_table: String,
    // Phase 2.2 — DLQ
    pub dlq_table: String,
    // Phase 2.3 — CDC control
    pub cdc_control_table: String,
    // Phase 7 — Topic policy
    pub topic_policy_table: String,
    // Phase 5.2 — Admin audit log
    pub admin_audit_log_table: String,
    // Phase 6.1 — Project registry
    pub projects_table: String,
    // U3 — Projection materialization engine
    pub projection_tasks_table: String,
    // KEYSTONE (lane 05) — durable idempotency dedup for keyed data-plane mutations
    pub idempotency_keys_table: String,
    // #5 — durable, broker-maintained opaque row revision / ETag map, keyed by
    // (tenant, project, message_type, primary-key-tuple) → monotonic revision.
    pub row_revisions_table: String,
}

impl Default for SystemCatalogConfig {
    fn default() -> Self {
        Self::current()
    }
}

impl SystemCatalogConfig {
    /// The process-global config cell, lazily seeded from env on first touch.
    fn cell() -> &'static ArcSwap<SystemCatalogConfig> {
        INSTALLED_SYSTEM_CATALOG_CONFIG
            .get_or_init(|| ArcSwap::from_pointee(Self::from_env_uninstalled()))
    }

    pub fn install_global(config: Self) {
        Self::cell().store(Arc::new(config));
    }

    /// Shared handle to the installed config (#214) — a lock-free `Arc` clone,
    /// not a deep copy. Prefer this on per-request hot paths; access fields via
    /// deref. `current()` is the owned-clone variant kept for callers that need
    /// `SystemCatalogConfig` by value.
    pub fn current_arc() -> Arc<SystemCatalogConfig> {
        Self::cell().load_full()
    }

    pub fn current() -> Self {
        (*Self::current_arc()).clone()
    }

    pub fn from_udb_config(config: &UdbConfig) -> Self {
        let defaults = crate::runtime::config::SystemCatalogSettings::default();
        Self {
            cdc: config.cdc.clone(),
            abac_schema: non_empty(&config.abac_schema, "udb_system"),
            abac_table: non_empty(&config.abac_table, "udb_abac_policies"),
            saga_table: non_empty(&config.system_catalog.saga_table, &defaults.saga_table),
            xa_ledger_table: non_empty(
                &config.system_catalog.xa_ledger_table,
                &defaults.xa_ledger_table,
            ),
            catalog_versions_table: non_empty(
                &config.system_catalog.catalog_versions_table,
                &defaults.catalog_versions_table,
            ),
            catalog_activation_log_table: non_empty(
                &config.system_catalog.catalog_activation_log_table,
                &defaults.catalog_activation_log_table,
            ),
            project_catalog_bindings_table: non_empty(
                &config.system_catalog.project_catalog_bindings_table,
                &defaults.project_catalog_bindings_table,
            ),
            catalog_reload_log_table: non_empty(
                &config.system_catalog.catalog_reload_log_table,
                &defaults.catalog_reload_log_table,
            ),
            migration_runs_table: non_empty(
                &config.system_catalog.migration_runs_table,
                &defaults.migration_runs_table,
            ),
            migration_op_ledger_table: non_empty(
                &config.system_catalog.migration_op_ledger_table,
                &defaults.migration_op_ledger_table,
            ),
            cdc_journal_table: non_empty(
                &config.system_catalog.cdc_journal_table,
                &defaults.cdc_journal_table,
            ),
            dlq_table: non_empty(&config.system_catalog.dlq_table, &defaults.dlq_table),
            cdc_control_table: non_empty(
                &config.system_catalog.cdc_control_table,
                &defaults.cdc_control_table,
            ),
            topic_policy_table: non_empty(
                &config.system_catalog.topic_policy_table,
                &defaults.topic_policy_table,
            ),
            admin_audit_log_table: non_empty(
                &config.system_catalog.admin_audit_log_table,
                &defaults.admin_audit_log_table,
            ),
            projects_table: non_empty(
                &config.system_catalog.projects_table,
                &defaults.projects_table,
            ),
            projection_tasks_table: non_empty(
                &config.system_catalog.projection_tasks_table,
                &defaults.projection_tasks_table,
            ),
            // KEYSTONE (lane 05): the dedup relation is not part of the legacy
            // `SystemCatalogSettings` config block, so it is resolved straight
            // from its env var / default here (single-sourced with
            // `from_env_uninstalled`), avoiding a cross-lane config-struct edit.
            idempotency_keys_table: env_identifier(
                "UDB_IDEMPOTENCY_KEYS_TABLE",
                "udb_idempotency_keys",
            ),
            // #5: resolved straight from its env var / default (single-sourced with
            // `from_env_uninstalled`), like `idempotency_keys_table` — not part of
            // the legacy `SystemCatalogSettings` config block.
            row_revisions_table: env_identifier("UDB_ROW_REVISIONS_TABLE", "udb_row_revisions"),
        }
    }

    fn from_env_uninstalled() -> Self {
        Self {
            cdc: CdcConfig::current(),
            abac_schema: env_identifier("UDB_ABAC_SCHEMA", "udb_system"),
            abac_table: env_identifier("UDB_ABAC_TABLE", "udb_abac_policies"),
            saga_table: env_identifier("UDB_SAGA_TABLE", "udb_saga_coordinator"),
            xa_ledger_table: env_identifier("UDB_XA_LEDGER_TABLE", "udb_xa_ledger"),
            catalog_versions_table: env_identifier(
                "UDB_CATALOG_VERSIONS_TABLE",
                "udb_catalog_versions",
            ),
            catalog_activation_log_table: env_identifier(
                "UDB_CATALOG_ACTIVATION_LOG_TABLE",
                "udb_catalog_activation_log",
            ),
            project_catalog_bindings_table: env_identifier(
                "UDB_PROJECT_CATALOG_BINDINGS_TABLE",
                "udb_project_catalog_bindings",
            ),
            catalog_reload_log_table: env_identifier(
                "UDB_CATALOG_RELOAD_LOG_TABLE",
                "udb_catalog_reload_log",
            ),
            migration_runs_table: env_identifier("UDB_MIGRATION_RUNS_TABLE", "udb_migration_runs"),
            migration_op_ledger_table: env_identifier(
                "UDB_MIGRATION_OP_LEDGER_TABLE",
                "udb_migration_op_ledger",
            ),
            cdc_journal_table: env_identifier("UDB_CDC_JOURNAL_TABLE", "udb_cdc_event_journal"),
            dlq_table: env_identifier("UDB_DLQ_TABLE", "udb_cdc_dlq_events"),
            cdc_control_table: env_identifier("UDB_CDC_CONTROL_TABLE", "udb_cdc_control"),
            topic_policy_table: env_identifier("UDB_TOPIC_POLICY_TABLE", "udb_topic_policy"),
            admin_audit_log_table: env_identifier(
                "UDB_ADMIN_AUDIT_LOG_TABLE",
                "udb_admin_audit_log",
            ),
            projects_table: env_identifier("UDB_PROJECTS_TABLE", "udb_projects"),
            projection_tasks_table: env_identifier(
                "UDB_PROJECTION_TASKS_TABLE",
                "udb_projection_tasks",
            ),
            idempotency_keys_table: env_identifier(
                "UDB_IDEMPOTENCY_KEYS_TABLE",
                "udb_idempotency_keys",
            ),
            row_revisions_table: env_identifier("UDB_ROW_REVISIONS_TABLE", "udb_row_revisions"),
        }
    }

    /// Build a config with a single custom schema for all UDB system tables.
    /// Useful for integration tests that want to isolate tables under a
    /// unique schema prefix.
    pub fn with_schema(schema: &str) -> Self {
        let mut cfg = Self::default();
        cfg.cdc.system_schema = schema.to_string();
        cfg.abac_schema = schema.to_string();
        cfg
    }

    /// Return the DDL statements as a `Vec<String>` (one per SQL statement).
    /// This is the instance-method form; the free function `system_catalog_ddl`
    /// accepts a config reference and returns the joined string.
    pub fn system_catalog_ddl(&self) -> Vec<String> {
        system_catalog_statements(self)
    }

    fn abac_relation(&self) -> String {
        relation(&self.abac_schema, &self.abac_table)
    }

    pub(crate) fn saga_relation(&self) -> String {
        relation(&self.cdc.system_schema, &self.saga_table)
    }

    pub(crate) fn xa_ledger_relation(&self) -> String {
        relation(&self.cdc.system_schema, &self.xa_ledger_table)
    }

    pub(crate) fn catalog_versions_relation(&self) -> String {
        relation(&self.cdc.system_schema, &self.catalog_versions_table)
    }

    pub(crate) fn catalog_activation_log_relation(&self) -> String {
        relation(&self.cdc.system_schema, &self.catalog_activation_log_table)
    }

    pub(crate) fn project_catalog_bindings_relation(&self) -> String {
        relation(
            &self.cdc.system_schema,
            &self.project_catalog_bindings_table,
        )
    }

    pub(crate) fn catalog_reload_log_relation(&self) -> String {
        relation(&self.cdc.system_schema, &self.catalog_reload_log_table)
    }
    pub(crate) fn migration_runs_relation(&self) -> String {
        relation(&self.cdc.system_schema, &self.migration_runs_table)
    }
    pub(crate) fn migration_op_ledger_relation(&self) -> String {
        relation(&self.cdc.system_schema, &self.migration_op_ledger_table)
    }

    pub fn cdc_journal_relation(&self) -> String {
        relation(&self.cdc.system_schema, &self.cdc_journal_table)
    }

    pub fn dlq_relation(&self) -> String {
        relation(&self.cdc.system_schema, &self.dlq_table)
    }

    pub fn cdc_control_relation(&self) -> String {
        relation(&self.cdc.system_schema, &self.cdc_control_table)
    }

    pub fn topic_policy_relation(&self) -> String {
        relation(&self.cdc.system_schema, &self.topic_policy_table)
    }

    fn admin_audit_log_relation(&self) -> String {
        relation(&self.cdc.system_schema, &self.admin_audit_log_table)
    }

    fn projects_relation(&self) -> String {
        relation(&self.cdc.system_schema, &self.projects_table)
    }

    pub(crate) fn projection_tasks_relation(&self) -> String {
        relation(&self.cdc.system_schema, &self.projection_tasks_table)
    }

    pub(crate) fn idempotency_keys_relation(&self) -> String {
        relation(&self.cdc.system_schema, &self.idempotency_keys_table)
    }

    pub(crate) fn row_revisions_relation(&self) -> String {
        relation(&self.cdc.system_schema, &self.row_revisions_table)
    }
}

pub async fn ensure_system_catalog(pool: &PgPool) -> Result<SystemCatalogReport, tonic::Status> {
    let config = SystemCatalogConfig::current();
    let statements = system_catalog_statements(&config);
    let mut tx = pool.begin().await.map_err(|err| {
        system_catalog_internal_status(
            "bootstrap_begin",
            format!("failed to start UDB system catalog bootstrap: {err}"),
        )
    })?;

    for statement in &statements {
        tx.execute(statement.as_str()).await.map_err(|err| {
            system_catalog_internal_status(
                "bootstrap_statement",
                format!("failed to bootstrap UDB system catalog: {err}"),
            )
        })?;
    }

    tx.commit().await.map_err(|err| {
        system_catalog_internal_status(
            "bootstrap_commit",
            format!("failed to commit UDB system catalog bootstrap: {err}"),
        )
    })?;

    // `CREATE TABLE IF NOT EXISTS` is a NO-OP against a pre-existing relation of
    // any shape, so the loop above proves these tables are creatable and NOT that
    // they are usable. Every relation here is operator-overridable (`UDB_*_TABLE`),
    // so pointing one at an existing table of the wrong shape bootstraps clean and
    // then fails on first write - the broker comes up healthy and every CDC,
    // outbox or DLQ write fails afterwards.
    //
    // The durable audit sink shipped exactly that defect twice. Verify here
    // instead, at the one chokepoint that owns all of these tables, so a bad
    // relation is named at STARTUP with its missing columns rather than surfacing
    // later as a raw Postgres error under load.
    for statement in &statements {
        let Some((relation, columns)) = declared_table_columns(statement) else {
            continue;
        };
        let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        crate::runtime::core::audit::verify_table_columns(pool, &relation, &refs)
            .await
            .map_err(|err| {
                system_catalog_internal_status(
                    "bootstrap_shape",
                    format!(
                        "UDB system table {relation} exists but does not match the shape UDB \
                         requires: {err}. It was not created by this bootstrap - point the \
                         corresponding UDB_*_TABLE override at a dedicated relation, or drop the \
                         conflicting table"
                    ),
                )
            })?;
    }

    Ok(SystemCatalogReport {
        schema: config.cdc.system_schema,
        statements_applied: statements.len(),
    })
}

/// Extract `(relation, column_names)` from a `CREATE TABLE IF NOT EXISTS` DDL
/// statement, or `None` for anything else.
///
/// Deliberately conservative: this drives a check that can REFUSE TO START the
/// broker, so any statement it cannot parse unambiguously yields `None` and is
/// skipped rather than risking a false refusal on a healthy deployment. Reading
/// the column list off the DDL keeps the check honest - there is no second,
/// hand-maintained list to drift out of sync with the schema.
fn declared_table_columns(statement: &str) -> Option<(String, Vec<String>)> {
    let trimmed = statement.trim();
    let rest = trimmed.strip_prefix("CREATE TABLE IF NOT EXISTS ")?;
    let open = rest.find('(')?;
    let relation = rest[..open].trim().to_string();
    if relation.is_empty() {
        return None;
    }
    // Body between the OUTERMOST parens; inner parens belong to types like
    // VARCHAR(80) and to CHECK expressions.
    let body = rest[open + 1..].trim_end();
    let body = body.strip_suffix(')')?;

    let mut columns = Vec::new();
    let mut depth = 0usize;
    let mut part = String::new();
    let mut parts = Vec::new();
    for ch in body.chars() {
        match ch {
            '(' => {
                depth += 1;
                part.push(ch);
            }
            ')' => {
                depth = depth.checked_sub(1)?;
                part.push(ch);
            }
            ',' if depth == 0 => parts.push(std::mem::take(&mut part)),
            _ => part.push(ch),
        }
    }
    if depth != 0 {
        return None;
    }
    parts.push(part);

    for raw in parts {
        let piece = raw.trim();
        if piece.is_empty() {
            continue;
        }
        let first = piece.split_whitespace().next()?;
        // Table-level constraints are not columns.
        if matches!(
            first.to_ascii_uppercase().as_str(),
            "PRIMARY" | "FOREIGN" | "UNIQUE" | "CHECK" | "CONSTRAINT" | "EXCLUDE" | "LIKE"
        ) {
            continue;
        }
        let name = first.trim_matches('"');
        // Anything that does not look like a bare identifier means this DDL uses a
        // form this parser does not model; skip the whole statement rather than
        // guess a column name that does not exist.
        if name.is_empty() || !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return None;
        }
        columns.push(name.to_string());
    }
    if columns.is_empty() {
        return None;
    }
    Some((relation, columns))
}

pub async fn inspect_system_catalog(
    pool: &PgPool,
) -> Result<SystemCatalogInspection, tonic::Status> {
    inspect_system_catalog_with_config(pool, &SystemCatalogConfig::default()).await
}

pub async fn inspect_system_catalog_with_config(
    pool: &PgPool,
    config: &SystemCatalogConfig,
) -> Result<SystemCatalogInspection, tonic::Status> {
    let expected = expected_relations(config);
    let mut relations = Vec::with_capacity(expected.len());
    let mut missing = Vec::new();

    for relation in expected {
        let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::TEXT")
            .bind(&relation.quoted_relation)
            .fetch_one(pool)
            .await
            .map_err(|err| {
                system_catalog_internal_status(
                    "inspect_relation",
                    format!(
                        "failed to inspect UDB system catalog relation {}: {err}",
                        relation.display_relation
                    ),
                )
            })?;
        let exists = exists.is_some();
        if !exists {
            missing.push(relation.display_relation.clone());
        }
        relations.push(SystemCatalogRelationStatus {
            role: relation.role,
            relation: relation.display_relation,
            exists,
        });
    }

    Ok(SystemCatalogInspection {
        schema: config.cdc.system_schema.clone(),
        ok: missing.is_empty(),
        relations,
        missing,
    })
}

pub fn default_system_catalog_ddl() -> String {
    system_catalog_ddl(&SystemCatalogConfig::default())
}

pub fn system_catalog_ddl(config: &SystemCatalogConfig) -> String {
    let mut ddl = system_catalog_statements(config).join(";\n\n");
    ddl.push_str(";\n");
    ddl
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemCatalogReport {
    pub schema: String,
    pub statements_applied: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SystemCatalogInspection {
    pub schema: String,
    pub ok: bool,
    pub relations: Vec<SystemCatalogRelationStatus>,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SystemCatalogRelationStatus {
    pub role: String,
    pub relation: String,
    pub exists: bool,
}

#[derive(Debug, Clone)]
struct ExpectedRelation {
    role: String,
    display_relation: String,
    quoted_relation: String,
}

fn expected_relations(config: &SystemCatalogConfig) -> Vec<ExpectedRelation> {
    let mut relations = vec![
        expected_relation(
            "cdc_outbox",
            &config.cdc.system_schema,
            &config.cdc.outbox_table,
        ),
        expected_relation(
            "cdc_offsets",
            &config.cdc.system_schema,
            &config.cdc.offsets_table,
        ),
        expected_relation(
            "cdc_lock_log",
            &config.cdc.system_schema,
            &config.cdc.lock_log_table,
        ),
        expected_relation("abac_policies", &config.abac_schema, &config.abac_table),
        expected_relation(
            "saga_coordinator",
            &config.cdc.system_schema,
            &config.saga_table,
        ),
        expected_relation(
            "xa_ledger",
            &config.cdc.system_schema,
            &config.xa_ledger_table,
        ),
        // Phase 1.1 — catalog versioning
        expected_relation(
            "catalog_versions",
            &config.cdc.system_schema,
            &config.catalog_versions_table,
        ),
        expected_relation(
            "catalog_activation_log",
            &config.cdc.system_schema,
            &config.catalog_activation_log_table,
        ),
        expected_relation(
            "project_catalog_bindings",
            &config.cdc.system_schema,
            &config.project_catalog_bindings_table,
        ),
        expected_relation(
            "catalog_reload_log",
            &config.cdc.system_schema,
            &config.catalog_reload_log_table,
        ),
        // Phase 1.3 — migration apply engine
        expected_relation(
            "migration_runs",
            &config.cdc.system_schema,
            &config.migration_runs_table,
        ),
        expected_relation(
            "migration_op_ledger",
            &config.cdc.system_schema,
            &config.migration_op_ledger_table,
        ),
        // Phase 2.1 — CDC event journal
        expected_relation(
            "cdc_event_journal",
            &config.cdc.system_schema,
            &config.cdc_journal_table,
        ),
        // Phase 2.2 — DLQ
        expected_relation(
            "cdc_dlq_events",
            &config.cdc.system_schema,
            &config.dlq_table,
        ),
        // Phase 2.3 — CDC control
        expected_relation(
            "cdc_control",
            &config.cdc.system_schema,
            &config.cdc_control_table,
        ),
        // Phase 7 — Topic policy
        expected_relation(
            "topic_policy",
            &config.cdc.system_schema,
            &config.topic_policy_table,
        ),
        // Phase 5.2 — admin audit log
        expected_relation(
            "admin_audit_log",
            &config.cdc.system_schema,
            &config.admin_audit_log_table,
        ),
        // Phase 6.1 — project registry
        expected_relation(
            "projects",
            &config.cdc.system_schema,
            &config.projects_table,
        ),
        // U3 — projection materialization tasks
        expected_relation(
            "projection_tasks",
            &config.cdc.system_schema,
            &config.projection_tasks_table,
        ),
        // KEYSTONE (lane 05) — durable idempotency dedup keys
        expected_relation(
            "idempotency_keys",
            &config.cdc.system_schema,
            &config.idempotency_keys_table,
        ),
        // #5 — durable opaque row revision / ETag map
        expected_relation(
            "row_revisions",
            &config.cdc.system_schema,
            &config.row_revisions_table,
        ),
    ];

    if let Ok(manifest) = crate::runtime::native_catalog::native_service_manifest() {
        relations.extend(
            manifest
                .tables
                .iter()
                .filter(|table| table.schema.starts_with("udb_"))
                .map(|table| {
                    expected_relation(
                        &format!("native_{}_{}", table.schema, table.table),
                        &table.schema,
                        &table.table,
                    )
                }),
        );
    }

    relations
}

fn expected_relation(role: &str, schema: &str, table: &str) -> ExpectedRelation {
    ExpectedRelation {
        role: role.to_string(),
        display_relation: format!("{schema}.{table}"),
        quoted_relation: relation(schema, table),
    }
}

/// Public re-export used by lifecycle.rs to run system catalog DDL on a
/// specific connection (same one holding the advisory lock).
pub fn system_catalog_statements_public() -> Vec<String> {
    system_catalog_statements(&SystemCatalogConfig::default())
}

fn system_catalog_statements(config: &SystemCatalogConfig) -> Vec<String> {
    let system_schema = qi(&config.cdc.system_schema);
    let abac_schema = qi(&config.abac_schema);
    let mut statements = vec![format!("CREATE SCHEMA IF NOT EXISTS {system_schema}")];
    if config.abac_schema != config.cdc.system_schema {
        statements.push(format!("CREATE SCHEMA IF NOT EXISTS {abac_schema}"));
    }
    statements.extend([
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                event_id UUID PRIMARY KEY,
                event_seq BIGSERIAL NOT NULL UNIQUE,
                topic TEXT NOT NULL,
                partition_key TEXT NOT NULL DEFAULT '',
                payload JSONB NOT NULL,
                headers JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                delivery_state TEXT NOT NULL DEFAULT 'pending'
                    CHECK (delivery_state IN ('pending','publishing','published','acked','dlq')),
                publishing_started_at TIMESTAMPTZ,
                published_at TIMESTAMPTZ,
                acked_at TIMESTAMPTZ,
                dlq_at TIMESTAMPTZ,
                producer_epoch BIGINT NOT NULL DEFAULT 0,
                transactional_id TEXT NOT NULL DEFAULT '',
                kafka_partition INTEGER,
                kafka_offset BIGINT,
                last_error TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            config.cdc.outbox_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS event_seq BIGSERIAL",
            config.cdc.outbox_relation()
        ),
        format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (event_seq)",
            qi(&format!("idx_{}_event_seq", config.cdc.outbox_table)),
            config.cdc.outbox_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS delivery_state TEXT NOT NULL DEFAULT 'pending'",
            config.cdc.outbox_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS publishing_started_at TIMESTAMPTZ",
            config.cdc.outbox_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ",
            config.cdc.outbox_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS acked_at TIMESTAMPTZ",
            config.cdc.outbox_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS dlq_at TIMESTAMPTZ",
            config.cdc.outbox_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS producer_epoch BIGINT NOT NULL DEFAULT 0",
            config.cdc.outbox_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS transactional_id TEXT NOT NULL DEFAULT ''",
            config.cdc.outbox_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS kafka_partition INTEGER",
            config.cdc.outbox_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS kafka_offset BIGINT",
            config.cdc.outbox_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT ''",
            config.cdc.outbox_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (created_at)",
            qi(&format!("idx_{}_created_at", config.cdc.outbox_table)),
            config.cdc.outbox_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (topic, created_at)",
            qi(&format!("idx_{}_topic_created_at", config.cdc.outbox_table)),
            config.cdc.outbox_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (delivery_state, created_at)",
            qi(&format!("idx_{}_state_created_at", config.cdc.outbox_table)),
            config.cdc.outbox_relation()
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                slot_name TEXT PRIMARY KEY,
                last_lsn BIGINT NOT NULL DEFAULT 0,
                last_event_id UUID,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            config.cdc.offsets_relation()
        ),
        // D (2026-05-30): non-PG CDC sources (Mongo change-stream
        // resume tokens, MySQL binlog file:position) need a string
        // offset; add the column idempotently so the schema works
        // for both the original WAL path and the generic
        // `tail_source` path.
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS last_offset TEXT",
            config.cdc.offsets_relation()
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY,
                lock_key BIGINT NOT NULL,
                holder_host TEXT NOT NULL,
                acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                fencing_token BIGINT NOT NULL DEFAULT 0
            )",
            config.cdc.lock_log_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 0",
            config.cdc.lock_log_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (lock_key, acquired_at DESC)",
            qi(&format!("idx_{}_lock_key", config.cdc.lock_log_table)),
            config.cdc.lock_log_relation()
        ),
        // The CDC leader election upserts `ON CONFLICT (lock_key)`, which requires
        // a UNIQUE constraint on lock_key — without it the upsert errors and the
        // tailer never acquires leadership (one lock row per lock_key).
        format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (lock_key)",
            qi(&format!("uq_{}_lock_key", config.cdc.lock_log_table)),
            config.cdc.lock_log_relation()
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                policy_id BIGSERIAL PRIMARY KEY,
                effect TEXT NOT NULL DEFAULT 'allow'
                    CHECK (LOWER(effect) IN ('allow', 'deny')),
                service_identity TEXT NOT NULL DEFAULT '*',
                tenant_id TEXT NOT NULL DEFAULT '*',
                purpose TEXT NOT NULL DEFAULT '*',
                message_type TEXT NOT NULL DEFAULT '*',
                operation TEXT NOT NULL DEFAULT '*',
                required_scope TEXT NOT NULL DEFAULT '',
                priority INTEGER NOT NULL DEFAULT 0,
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            config.abac_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (enabled, priority DESC, policy_id ASC)",
            qi(&format!("idx_{}_enabled_priority", config.abac_table)),
            config.abac_relation()
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                saga_id UUID PRIMARY KEY,
                tx_id TEXT NOT NULL DEFAULT '',
                tenant_id TEXT NOT NULL DEFAULT '',
                correlation_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                backend_instance TEXT NOT NULL DEFAULT '',
                operation TEXT NOT NULL DEFAULT '',
                current_step INTEGER NOT NULL DEFAULT 0,
                retry_count INTEGER NOT NULL DEFAULT 0,
                recovery_attempts INTEGER NOT NULL DEFAULT 0,
                compensation_status TEXT NOT NULL DEFAULT 'none',
                steps JSONB NOT NULL DEFAULT '[]'::JSONB,
                compensations JSONB NOT NULL DEFAULT '[]'::JSONB,
                last_error TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            config.saga_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS backend_instance TEXT NOT NULL DEFAULT ''",
            config.saga_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS operation TEXT NOT NULL DEFAULT ''",
            config.saga_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0",
            config.saga_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS recovery_attempts INTEGER NOT NULL DEFAULT 0",
            config.saga_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS compensation_status TEXT NOT NULL DEFAULT 'none'",
            config.saga_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (tenant_id, status, updated_at DESC)",
            qi(&format!("idx_{}_tenant_status", config.saga_table)),
            config.saga_relation()
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                xid TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL DEFAULT '',
                project_id TEXT NOT NULL DEFAULT '',
                origin_rpc TEXT NOT NULL DEFAULT '',
                correlation_id TEXT NOT NULL DEFAULT '',
                participants JSONB NOT NULL DEFAULT '[]'::JSONB,
                decision TEXT NOT NULL DEFAULT 'in_doubt'
                    CHECK (decision IN ('committed','rolled_back','in_doubt')),
                reason TEXT NOT NULL DEFAULT '',
                recovery_attempts INTEGER NOT NULL DEFAULT 0,
                decided_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            config.xa_ledger_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (decision, updated_at)",
            qi(&format!("idx_{}_decision_updated", config.xa_ledger_table)),
            config.xa_ledger_relation()
        ),
        // ── Phase 1.1 — Catalog version tables ───────────────────────────────
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                catalog_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                project_id TEXT NOT NULL DEFAULT '',
                version TEXT NOT NULL,
                checksum_sha256 TEXT NOT NULL,
                manifest_json JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                manifest_integrity_sha256 TEXT NOT NULL DEFAULT '',
                schema_checksum_sha256 TEXT NOT NULL DEFAULT '',
                checksum_kind TEXT NOT NULL DEFAULT 'raw_request_sha256_v1',
                compatibility_level TEXT NOT NULL DEFAULT 'backward'
                    CHECK (compatibility_level IN ('exact','backward','none')),
                validation_checksum_sha256 TEXT NOT NULL DEFAULT '',
                compatibility_evidence_sha256 TEXT NOT NULL DEFAULT '',
                validated_against_catalog_id UUID,
                validated_against_checksum_sha256 TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'STAGED'
                    CHECK (status IN ('STAGED','ACTIVE','ROLLED_BACK','REJECTED')),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                activated_at TIMESTAMPTZ
            )",
            config.catalog_versions_relation()
        ),
        // Guard: add manifest_json to tables created by an older UDB binary that
        // predates this column.  DEFAULT '{{}}' keeps NOT NULL satisfied for any
        // existing rows while the real value is written on the next save_manifest.
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS manifest_json JSONB NOT NULL DEFAULT '{{}}'::JSONB",
            config.catalog_versions_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS activated_at TIMESTAMPTZ",
            config.catalog_versions_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS manifest_integrity_sha256 TEXT NOT NULL DEFAULT ''",
            config.catalog_versions_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS schema_checksum_sha256 TEXT NOT NULL DEFAULT ''",
            config.catalog_versions_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS checksum_kind TEXT NOT NULL DEFAULT 'raw_request_sha256_v1'",
            config.catalog_versions_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS compatibility_level TEXT NOT NULL DEFAULT 'backward'",
            config.catalog_versions_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS validation_checksum_sha256 TEXT NOT NULL DEFAULT ''",
            config.catalog_versions_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS compatibility_evidence_sha256 TEXT NOT NULL DEFAULT ''",
            config.catalog_versions_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS validated_against_catalog_id UUID",
            config.catalog_versions_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS validated_against_checksum_sha256 TEXT NOT NULL DEFAULT ''",
            config.catalog_versions_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (project_id, status, created_at DESC)",
            qi(&format!("idx_{}_project_status", config.catalog_versions_table)),
            config.catalog_versions_relation()
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY,
                project_id TEXT NOT NULL DEFAULT '',
                from_version TEXT NOT NULL DEFAULT '',
                to_version TEXT NOT NULL,
                actor TEXT NOT NULL DEFAULT '',
                reason TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            config.catalog_activation_log_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (project_id, created_at DESC)",
            qi(&format!("idx_{}_project", config.catalog_activation_log_table)),
            config.catalog_activation_log_relation()
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                project_id TEXT PRIMARY KEY,
                active_catalog_id UUID REFERENCES {} (catalog_id) ON DELETE SET NULL,
                active_version TEXT NOT NULL DEFAULT '',
                active_checksum_sha256 TEXT NOT NULL DEFAULT '',
                compatibility_level TEXT NOT NULL DEFAULT 'backward'
                    CHECK (compatibility_level IN ('exact','backward','none')),
                compatibility_evidence_sha256 TEXT NOT NULL DEFAULT '',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            config.project_catalog_bindings_relation(),
            config.catalog_versions_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (active_version)",
            qi(&format!(
                "idx_{}_active_version",
                config.project_catalog_bindings_table
            )),
            config.project_catalog_bindings_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS compatibility_evidence_sha256 TEXT NOT NULL DEFAULT ''",
            config.project_catalog_bindings_relation()
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                reload_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                reload_seq BIGSERIAL UNIQUE,
                project_id TEXT NOT NULL DEFAULT '',
                catalog_id UUID,
                version TEXT NOT NULL DEFAULT '',
                checksum_sha256 TEXT NOT NULL DEFAULT '',
                instance_id TEXT NOT NULL DEFAULT '',
                action TEXT NOT NULL CHECK (action IN ('STAGE','ACTIVATE','ROLLBACK','RELOAD','REJECT')),
                result TEXT NOT NULL DEFAULT 'ok',
                message TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            config.catalog_reload_log_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (project_id, created_at DESC)",
            qi(&format!("idx_{}_project", config.catalog_reload_log_table)),
            config.catalog_reload_log_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS idempotency_key TEXT NOT NULL DEFAULT ''",
            config.catalog_reload_log_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS request_fingerprint TEXT NOT NULL DEFAULT ''",
            config.catalog_reload_log_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS actor TEXT NOT NULL DEFAULT ''",
            config.catalog_reload_log_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS compatibility_evidence_sha256 TEXT NOT NULL DEFAULT ''",
            config.catalog_reload_log_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS reload_seq BIGSERIAL",
            config.catalog_reload_log_relation()
        ),
        format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (reload_seq)",
            qi(&format!("uq_{}_reload_seq", config.catalog_reload_log_table)),
            config.catalog_reload_log_relation()
        ),
        // Canonicalize legacy blank/whitespace project ids only when doing so
        // cannot merge distinct authorities. Duplicate ACTIVE rows are repaired
        // solely from the exact durable binding; every unproven split fails with
        // an actionable startup error before the unique/check constraints land.
        format!(
            "DO $udb$
             DECLARE
               collisions TEXT;
               invalid_active TEXT;
             BEGIN
               SELECT string_agg(normalized_project, ',') INTO collisions
               FROM (
                 SELECT COALESCE(NULLIF(BTRIM(project_id), ''), 'default') AS normalized_project
                 FROM {cat_rel}
                 GROUP BY COALESCE(NULLIF(BTRIM(project_id), ''), 'default')
                 HAVING COUNT(DISTINCT project_id) > 1
               ) AS collision_set;
               IF collisions IS NOT NULL THEN
                 RAISE EXCEPTION 'catalog project normalization collision(s): %', collisions
                   USING HINT = 'repair catalog_versions/project bindings explicitly before starting UDB';
               END IF;
               SELECT string_agg(normalized_project, ',') INTO collisions
               FROM (
                 SELECT COALESCE(NULLIF(BTRIM(project_id), ''), 'default') AS normalized_project
                 FROM {binding_rel}
                 GROUP BY COALESCE(NULLIF(BTRIM(project_id), ''), 'default')
                 HAVING COUNT(*) > 1
               ) AS collision_set;
               IF collisions IS NOT NULL THEN
                 RAISE EXCEPTION 'catalog binding normalization collision(s): %', collisions
                   USING HINT = 'repair project_catalog_bindings explicitly before starting UDB';
               END IF;

               UPDATE {cat_rel}
                  SET project_id = COALESCE(NULLIF(BTRIM(project_id), ''), 'default')
                WHERE project_id <> COALESCE(NULLIF(BTRIM(project_id), ''), 'default');
               UPDATE {binding_rel}
                  SET project_id = COALESCE(NULLIF(BTRIM(project_id), ''), 'default')
                WHERE project_id <> COALESCE(NULLIF(BTRIM(project_id), ''), 'default');
               UPDATE {activation_rel}
                  SET project_id = COALESCE(NULLIF(BTRIM(project_id), ''), 'default')
                WHERE project_id <> COALESCE(NULLIF(BTRIM(project_id), ''), 'default');
               UPDATE {reload_rel}
                  SET project_id = COALESCE(NULLIF(BTRIM(project_id), ''), 'default')
                WHERE project_id <> COALESCE(NULLIF(BTRIM(project_id), ''), 'default');

               UPDATE {cat_rel} AS candidate
                  SET status = 'ROLLED_BACK'
                 FROM {binding_rel} AS binding
                WHERE candidate.project_id = binding.project_id
                  AND candidate.status = 'ACTIVE'
                  AND candidate.catalog_id <> binding.active_catalog_id
                  AND EXISTS (
                    SELECT 1 FROM {cat_rel} AS chosen
                     WHERE chosen.catalog_id = binding.active_catalog_id
                       AND chosen.project_id = binding.project_id
                       AND chosen.status = 'ACTIVE'
                  );

               SELECT string_agg(project_id, ',') INTO invalid_active
               FROM (
                 SELECT active.project_id
                   FROM {cat_rel} AS active
                   LEFT JOIN {binding_rel} AS binding
                     ON binding.project_id = active.project_id
                    AND binding.active_catalog_id = active.catalog_id
                    AND binding.active_version = active.version
                    AND binding.active_checksum_sha256 = active.checksum_sha256
                    AND binding.compatibility_level = active.compatibility_level
                    AND binding.compatibility_evidence_sha256 = active.compatibility_evidence_sha256
                    AND (
                      binding.compatibility_evidence_sha256 = ''
                      OR EXISTS (
                        SELECT 1 FROM {reload_rel} AS reload
                         WHERE reload.project_id = binding.project_id
                           AND reload.catalog_id = binding.active_catalog_id
                           AND reload.version = binding.active_version
                           AND reload.checksum_sha256 = binding.active_checksum_sha256
                           AND reload.compatibility_evidence_sha256 = binding.compatibility_evidence_sha256
                           AND reload.action IN ('ACTIVATE', 'ROLLBACK', 'RELOAD')
                           AND reload.result = 'ok'
                      )
                    )
                  WHERE active.status = 'ACTIVE'
                  GROUP BY active.project_id
                 HAVING COUNT(*) <> 1 OR COUNT(binding.project_id) <> 1
               ) AS invalid_set;
               IF invalid_active IS NOT NULL THEN
                 RAISE EXCEPTION 'catalog ACTIVE/binding provenance invalid for project(s): %', invalid_active
                   USING HINT = 'select one authoritative catalog_id per project and repair project_catalog_bindings';
               END IF;

               SELECT string_agg(binding.project_id, ',') INTO invalid_active
                 FROM {binding_rel} AS binding
                 LEFT JOIN {cat_rel} AS active
                   ON active.project_id = binding.project_id
                  AND active.catalog_id = binding.active_catalog_id
                  AND active.version = binding.active_version
                  AND active.checksum_sha256 = binding.active_checksum_sha256
                  AND active.compatibility_level = binding.compatibility_level
                  AND active.compatibility_evidence_sha256 = binding.compatibility_evidence_sha256
                  AND active.status = 'ACTIVE'
                  AND (
                    binding.compatibility_evidence_sha256 = ''
                    OR EXISTS (
                      SELECT 1 FROM {reload_rel} AS reload
                       WHERE reload.project_id = binding.project_id
                         AND reload.catalog_id = binding.active_catalog_id
                         AND reload.version = binding.active_version
                         AND reload.checksum_sha256 = binding.active_checksum_sha256
                         AND reload.compatibility_evidence_sha256 = binding.compatibility_evidence_sha256
                         AND reload.action IN ('ACTIVATE', 'ROLLBACK', 'RELOAD')
                         AND reload.result = 'ok'
                    )
                  )
                WHERE active.catalog_id IS NULL;
               IF invalid_active IS NOT NULL THEN
                 RAISE EXCEPTION 'catalog binding/ACTIVE provenance invalid for project(s): %', invalid_active
                   USING HINT = 'repair or remove orphan project_catalog_bindings before starting UDB';
               END IF;
             END;
             $udb$",
            cat_rel = config.catalog_versions_relation(),
            binding_rel = config.project_catalog_bindings_relation(),
            activation_rel = config.catalog_activation_log_relation(),
            reload_rel = config.catalog_reload_log_relation()
        ),
        format!(
            "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}",
            config.catalog_versions_relation(),
            qi(&format!("chk_{}_canonical_project", config.catalog_versions_table))
        ),
        format!(
            "ALTER TABLE {} ADD CONSTRAINT {} CHECK (project_id = BTRIM(project_id) AND project_id <> '')",
            config.catalog_versions_relation(),
            qi(&format!("chk_{}_canonical_project", config.catalog_versions_table))
        ),
        format!(
            "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}",
            config.catalog_versions_relation(),
            qi(&format!("chk_{}_checksum_kind", config.catalog_versions_table))
        ),
        format!(
            "ALTER TABLE {} ADD CONSTRAINT {} CHECK (checksum_kind = 'raw_request_sha256_v1')",
            config.catalog_versions_relation(),
            qi(&format!("chk_{}_checksum_kind", config.catalog_versions_table))
        ),
        format!(
            "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}",
            config.catalog_versions_relation(),
            qi(&format!(
                "chk_{}_compatibility_level",
                config.catalog_versions_table
            ))
        ),
        format!(
            "ALTER TABLE {} ADD CONSTRAINT {} CHECK (compatibility_level IN ('exact','backward','none'))",
            config.catalog_versions_relation(),
            qi(&format!(
                "chk_{}_compatibility_level",
                config.catalog_versions_table
            ))
        ),
        format!(
            "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}",
            config.project_catalog_bindings_relation(),
            qi(&format!("chk_{}_canonical_project", config.project_catalog_bindings_table))
        ),
        format!(
            "ALTER TABLE {} ADD CONSTRAINT {} CHECK (project_id = BTRIM(project_id) AND project_id <> '')",
            config.project_catalog_bindings_relation(),
            qi(&format!("chk_{}_canonical_project", config.project_catalog_bindings_table))
        ),
        format!(
            "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}",
            config.project_catalog_bindings_relation(),
            qi(&format!(
                "chk_{}_compatibility_level",
                config.project_catalog_bindings_table
            ))
        ),
        format!(
            "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}",
            config.project_catalog_bindings_relation(),
            qi(&format!(
                "fk_{}_active_catalog",
                config.project_catalog_bindings_table
            ))
        ),
        format!(
            "ALTER TABLE {} ADD CONSTRAINT {} CHECK (compatibility_level IN ('exact','backward','none'))",
            config.project_catalog_bindings_relation(),
            qi(&format!(
                "chk_{}_compatibility_level",
                config.project_catalog_bindings_table
            ))
        ),
        format!(
            "ALTER TABLE {} ALTER COLUMN active_catalog_id SET NOT NULL",
            config.project_catalog_bindings_relation()
        ),
        format!(
            "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}",
            config.project_catalog_bindings_relation(),
            qi(&format!(
                "{}_active_catalog_id_fkey",
                config.project_catalog_bindings_table
            ))
        ),
        format!(
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY (active_catalog_id) REFERENCES {} (catalog_id) ON DELETE RESTRICT",
            config.project_catalog_bindings_relation(),
            qi(&format!(
                "fk_{}_active_catalog",
                config.project_catalog_bindings_table
            )),
            config.catalog_versions_relation()
        ),
        format!(
            "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}",
            config.catalog_reload_log_relation(),
            qi(&format!("chk_{}_canonical_project", config.catalog_reload_log_table))
        ),
        format!(
            "ALTER TABLE {} ADD CONSTRAINT {} CHECK (project_id = BTRIM(project_id) AND project_id <> '')",
            config.catalog_reload_log_relation(),
            qi(&format!("chk_{}_canonical_project", config.catalog_reload_log_table))
        ),
        // One project has exactly one durable ACTIVE catalog. The transition
        // path also takes a project advisory lock; this index is the final
        // fail-closed invariant if another writer bypasses that path.
        format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (project_id) WHERE status = 'ACTIVE'",
            qi(&format!("uq_{}_one_active_project", config.catalog_versions_table)),
            config.catalog_versions_relation()
        ),
        format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (project_id, action, idempotency_key) WHERE idempotency_key <> ''",
            qi(&format!("uq_{}_project_action_idem", config.catalog_reload_log_table)),
            config.catalog_reload_log_relation()
        ),
        // ── Phase 1.3 — Migration apply engine tables ─────────────────────────
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                project_id TEXT NOT NULL DEFAULT '',
                catalog_version TEXT NOT NULL DEFAULT '',
                catalog_id UUID,
                catalog_checksum_sha256 TEXT NOT NULL DEFAULT '',
                target_backend TEXT NOT NULL DEFAULT '',
                target_instance TEXT NOT NULL DEFAULT '',
                target_provenance_sha256 TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL DEFAULT 'DRY_RUN'
                    CHECK (state IN ('DRY_RUN','PREFLIGHT','APPROVED','APPLYING','VERIFYING','COMPLETED','ERROR','DEAD_LETTER')),
                operations_hash TEXT NOT NULL DEFAULT '',
                approval_token TEXT NOT NULL DEFAULT '',
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                finished_at TIMESTAMPTZ,
                error TEXT NOT NULL DEFAULT ''
            )",
            config.migration_runs_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS catalog_id UUID",
            config.migration_runs_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS catalog_checksum_sha256 TEXT NOT NULL DEFAULT ''",
            config.migration_runs_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS target_backend TEXT NOT NULL DEFAULT ''",
            config.migration_runs_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS target_instance TEXT NOT NULL DEFAULT ''",
            config.migration_runs_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS target_provenance_sha256 TEXT NOT NULL DEFAULT ''",
            config.migration_runs_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (project_id, state, started_at DESC)",
            qi(&format!("idx_{}_project_state", config.migration_runs_table)),
            config.migration_runs_relation()
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY,
                run_id UUID NOT NULL REFERENCES {} (run_id) ON DELETE CASCADE,
                operation_index INTEGER NOT NULL,
                backend TEXT NOT NULL DEFAULT 'postgres',
                resource_uri TEXT NOT NULL DEFAULT '',
                operation_kind TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING','APPLIED','VERIFIED','SKIPPED','FAILED','ROLLED_BACK')),
                payload_json JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                error TEXT NOT NULL DEFAULT '',
                applied_at TIMESTAMPTZ
            )",
            config.migration_op_ledger_relation(),
            config.migration_runs_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (run_id, operation_index)",
            qi(&format!("idx_{}_run_idx", config.migration_op_ledger_table)),
            config.migration_op_ledger_relation()
        ),
        // ── Phase 2.1 — CDC event journal ─────────────────────────────────────
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                event_id UUID PRIMARY KEY,
                topic TEXT NOT NULL,
                partition_key TEXT NOT NULL DEFAULT '',
                payload JSONB NOT NULL,
                published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                kafka_partition INTEGER,
                kafka_offset BIGINT,
                delivery_state TEXT NOT NULL DEFAULT 'published'
                    CHECK (delivery_state IN ('published','acked','dlq')),
                acked_at TIMESTAMPTZ,
                producer_epoch BIGINT NOT NULL DEFAULT 0,
                transactional_id TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '',
                schema_uri TEXT NOT NULL DEFAULT '',
                expires_at TIMESTAMPTZ
            )",
            config.cdc_journal_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS delivery_state TEXT NOT NULL DEFAULT 'published'",
            config.cdc_journal_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS acked_at TIMESTAMPTZ",
            config.cdc_journal_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS producer_epoch BIGINT NOT NULL DEFAULT 0",
            config.cdc_journal_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS transactional_id TEXT NOT NULL DEFAULT ''",
            config.cdc_journal_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT ''",
            config.cdc_journal_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (topic, published_at)",
            qi(&format!("idx_{}_topic_published", config.cdc_journal_table)),
            config.cdc_journal_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (expires_at) WHERE expires_at IS NOT NULL",
            qi(&format!("idx_{}_expires", config.cdc_journal_table)),
            config.cdc_journal_relation()
        ),
        // ── Phase 2.2 — DLQ events ────────────────────────────────────────────
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                dlq_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                event_id UUID NOT NULL,
                topic TEXT NOT NULL,
                tenant_id TEXT NOT NULL DEFAULT '',
                project_id TEXT NOT NULL DEFAULT '',
                payload JSONB NOT NULL,
                error_type TEXT NOT NULL DEFAULT '',
                error_message TEXT NOT NULL DEFAULT '',
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_retry_at TIMESTAMPTZ,
                next_retry_at TIMESTAMPTZ,
                status TEXT NOT NULL DEFAULT 'OPEN'
                    CHECK (status IN ('OPEN','REPLAYED','DISMISSED','QUARANTINED','RETRYING')),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            config.dlq_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (topic, status, created_at DESC)",
            qi(&format!("idx_{}_topic_status", config.dlq_table)),
            config.dlq_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT ''",
            config.dlq_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS project_id TEXT NOT NULL DEFAULT ''",
            config.dlq_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (tenant_id, project_id, topic, status, created_at DESC)",
            qi(&format!("idx_{}_tenant_project_topic_status", config.dlq_table)),
            config.dlq_relation()
        ),
        format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (event_id)",
            qi(&format!("idx_{}_event_id_unique", config.dlq_table)),
            config.dlq_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (next_retry_at) WHERE status = 'RETRYING' AND next_retry_at IS NOT NULL",
            qi(&format!("idx_{}_next_retry", config.dlq_table)),
            config.dlq_relation()
        ),
        // ── Phase 2.3 — CDC control plane ─────────────────────────────────────
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                slot_name TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL DEFAULT '',
                project_id TEXT NOT NULL DEFAULT '',
                paused BOOLEAN NOT NULL DEFAULT FALSE,
                pause_reason TEXT NOT NULL DEFAULT '',
                requested_leader_stepdown_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            config.cdc_control_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT ''",
            config.cdc_control_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS project_id TEXT NOT NULL DEFAULT ''",
            config.cdc_control_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (tenant_id, project_id, slot_name)",
            qi(&format!("idx_{}_tenant_project_slot", config.cdc_control_table)),
            config.cdc_control_relation()
        ),
        // ── Phase 7 — Topic policy ────────────────────────────────────────────
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                policy_id BIGSERIAL PRIMARY KEY,
                topic TEXT NOT NULL,
                tenant_id TEXT NOT NULL DEFAULT '*',
                owning_project TEXT NOT NULL DEFAULT '',
                owning_service TEXT NOT NULL DEFAULT '',
                schema_uri TEXT NOT NULL DEFAULT '',
                redaction_mode TEXT NOT NULL DEFAULT 'mask'
                    CHECK (redaction_mode IN ('mask','drop','hash')),
                redaction_version INTEGER NOT NULL DEFAULT 1,
                retention_class TEXT NOT NULL DEFAULT 'standard'
                    CHECK (retention_class IN ('ephemeral','standard','long_term','permanent')),
                max_retry_attempts INTEGER NOT NULL DEFAULT 3,
                retry_delay_secs INTEGER[] NOT NULL DEFAULT ARRAY[10, 60, 300],
                dlq_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (topic)
            )",
            config.topic_policy_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT '*'",
            config.topic_policy_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS redaction_mode TEXT NOT NULL DEFAULT 'mask'",
            config.topic_policy_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS redaction_version INTEGER NOT NULL DEFAULT 1",
            config.topic_policy_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (owning_project, enabled)",
            qi(&format!("idx_{}_project", config.topic_policy_table)),
            config.topic_policy_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (tenant_id, topic, enabled)",
            qi(&format!("idx_{}_tenant_topic", config.topic_policy_table)),
            config.topic_policy_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (owning_service, enabled)",
            qi(&format!("idx_{}_service", config.topic_policy_table)),
            config.topic_policy_relation()
        ),
        // ── Phase 5.2 — Admin audit log ───────────────────────────────────────
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                actor TEXT NOT NULL DEFAULT '',
                operation TEXT NOT NULL,
                target TEXT NOT NULL DEFAULT '',
                request_json JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                result TEXT NOT NULL DEFAULT 'ok',
                tenant_id TEXT NOT NULL DEFAULT '',
                project_id TEXT NOT NULL DEFAULT '',
                correlation_id TEXT NOT NULL DEFAULT '',
                previous_hash TEXT NOT NULL DEFAULT '',
                current_hash TEXT NOT NULL DEFAULT '',
                signer_key_id TEXT NOT NULL DEFAULT '',
                external_anchor TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            config.admin_audit_log_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS previous_hash TEXT NOT NULL DEFAULT ''",
            config.admin_audit_log_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS current_hash TEXT NOT NULL DEFAULT ''",
            config.admin_audit_log_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS signer_key_id TEXT NOT NULL DEFAULT ''",
            config.admin_audit_log_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS external_anchor TEXT NOT NULL DEFAULT ''",
            config.admin_audit_log_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (operation, created_at DESC)",
            qi(&format!("idx_{}_op", config.admin_audit_log_table)),
            config.admin_audit_log_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (current_hash)",
            qi(&format!("idx_{}_hash", config.admin_audit_log_table)),
            config.admin_audit_log_relation()
        ),
        // ── Phase 6.1 — Project registry ─────────────────────────────────────
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                project_id TEXT PRIMARY KEY,
                name TEXT NOT NULL DEFAULT '',
                system_schema TEXT NOT NULL DEFAULT 'udb_system',
                abac_schema TEXT NOT NULL DEFAULT 'udb_system',
                cdc_topic_prefix TEXT NOT NULL DEFAULT '',
                active_catalog_version TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            config.projects_relation()
        ),
        // ── U3 — Projection materialization tasks ───────────────────────────────────
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                task_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                idempotency_key TEXT NOT NULL,
                project_id TEXT NOT NULL DEFAULT '',
                manifest_checksum TEXT NOT NULL DEFAULT '',
                message_type TEXT NOT NULL DEFAULT '',
                source_schema TEXT NOT NULL DEFAULT '',
                source_table TEXT NOT NULL DEFAULT '',
                source_row_key JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                operation TEXT NOT NULL DEFAULT 'upsert'
                    CHECK (operation IN ('upsert','delete')),
                target_backend TEXT NOT NULL DEFAULT '',
                target_instance TEXT NOT NULL DEFAULT '',
                projection_kind TEXT NOT NULL DEFAULT '',
                resource_name TEXT NOT NULL DEFAULT '',
                target_options JSONB NOT NULL DEFAULT '[]'::JSONB,
                source_payload JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                source_checksum TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING','IN_PROGRESS','COMPLETED','FAILED','DEAD_LETTER')),
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                next_retry_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                completed_at TIMESTAMPTZ,
                UNIQUE (idempotency_key)
            )",
            config.projection_tasks_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS source_schema TEXT NOT NULL DEFAULT ''",
            config.projection_tasks_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS target_options JSONB NOT NULL DEFAULT '[]'::JSONB",
            config.projection_tasks_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS source_checksum TEXT NOT NULL DEFAULT ''",
            config.projection_tasks_relation()
        ),
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ",
            config.projection_tasks_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (status, created_at)",
            qi(&format!("idx_{}_status", config.projection_tasks_table)),
            config.projection_tasks_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (target_backend, target_instance, status)",
            qi(&format!("idx_{}_backend_status", config.projection_tasks_table)),
            config.projection_tasks_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (next_retry_at) WHERE status = 'FAILED' AND next_retry_at IS NOT NULL",
            qi(&format!("idx_{}_next_retry", config.projection_tasks_table)),
            config.projection_tasks_relation()
        ),
        // KEYSTONE (lane 05) — durable, tenant+project-scoped dedup for keyed
        // data-plane mutations. `dedup_key` is the salted SHA-256 of
        // (tenant_id, project_id, message_type, operation, idempotency_key) so the
        // PRIMARY KEY itself enforces per-tenant+project+type+operation uniqueness;
        // `response_json` carries the first writer's MutationResponse summary so a
        // replay returns the original body (not an empty one) without re-running the
        // write. `request_hash` is the canonical SHA-256 over the first writer's
        // *authoritative* inputs (normalized filter/record/changes/mask/increments/
        // conflict-strategy/expected, minus transport-only fields) so a later request
        // that reuses the same key with DIFFERENT inputs is rejected as a conflict
        // instead of silently replaying a success for an op that never ran.
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                dedup_key TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                message_type TEXT NOT NULL,
                operation TEXT NOT NULL,
                request_hash TEXT,
                response_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            config.idempotency_keys_relation()
        ),
        // Upgrade guard: `CREATE TABLE IF NOT EXISTS` never evolves an existing
        // table, so a dedup relation bootstrapped by an older UDB (whose DDL lacked
        // `request_hash`) self-heals on the next bootstrap. Nullable: legacy rows
        // written before this upgrade carry no hash and are replayed best-effort.
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS request_hash TEXT",
            config.idempotency_keys_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (tenant_id, project_id, created_at)",
            qi(&format!("idx_{}_tenant_created", config.idempotency_keys_table)),
            config.idempotency_keys_relation()
        ),
        // #5 — broker-maintained opaque row revision / ETag map. `revision_key` is
        // the salted SHA-256 of (tenant_id, project_id, message_type, primary-key
        // tuple) so the PRIMARY KEY enforces per-(tenant,project,type,row) identity
        // and a lookup can never cross tenants (you can only recompute a key for a
        // tenant you are already scoped to). `revision` is a monotonically
        // INCREASING counter bumped in the SAME transaction as every single-row
        // logical mutation, so an opaque token is ABA-safe (never reused/decreased).
        // `row_key` is the canonical primary-key tuple, kept for diagnostics only.
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                revision_key TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                message_type TEXT NOT NULL,
                row_key TEXT NOT NULL DEFAULT '',
                revision BIGINT NOT NULL DEFAULT 1,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            config.row_revisions_relation()
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (tenant_id, project_id, message_type)",
            qi(&format!("idx_{}_tenant_type", config.row_revisions_table)),
            config.row_revisions_relation()
        ),
    ]);
    // UDB-owned native-service tables are migrated through the normal proto →
    // manifest → diff/apply engine (see `run_startup_lifecycle`, which merges the
    // native manifest), not created by hand here. Bootstrap only needs to ensure
    // the native namespaces exist before that migration runs; the schema set is
    // derived from the proto manifest, so proto stays the source of truth.
    if crate::runtime::native_catalog::native_services_enabled() {
        for schema in crate::runtime::native_catalog::native_schema_names() {
            statements.push(format!("CREATE SCHEMA IF NOT EXISTS {}", qi(&schema)));
        }
    }
    statements
}

fn relation(schema: &str, table: &str) -> String {
    format!("{}.{}", qi(schema), qi(table))
}

fn non_empty(value: &str, fallback: &str) -> String {
    if value.trim().is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

fn system_catalog_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status("system_catalog", operation, message)
}

// `qi`, `env_identifier`, and `is_identifier` are imported from
// `runtime::executor_utils` (single-sourced).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_internal_detail(status: &tonic::Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "system_catalog");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
    }

    #[test]
    fn system_catalog_internal_status_carries_typed_detail() {
        let status = system_catalog_internal_status(
            "bootstrap_begin",
            "failed to start UDB system catalog bootstrap",
        );
        assert_internal_detail(
            &status,
            "bootstrap_begin",
            "failed to start UDB system catalog bootstrap",
        );
    }

    #[test]
    fn system_catalog_bootstraps_stage1_auth_schemas_not_tables() {
        let sql = system_catalog_statements_public().join("\n");
        // Native-service TABLES are migrated through the proto → manifest →
        // diff/apply engine (`run_startup_lifecycle` merges the native manifest),
        // NOT created by the bootstrap statement list. Bootstrap only ensures the
        // native namespaces exist first; the schema set is derived from proto.
        for schema in ["udb_authn", "udb_authz"] {
            assert!(
                sql.contains(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\"")),
                "system catalog bootstrap must create native schema {schema}"
            );
        }
        // It must no longer hand-write (or proto-bootstrap) native table DDL here.
        assert!(
            !sql.contains("\"udb_authn\".\"users\"")
                && !sql.contains("\"udb_authz\".\"policy_rules\""),
            "native tables must come from the migration engine, not the bootstrap statement list"
        );
    }

    #[test]
    fn system_catalog_sql_is_neutral_and_configurable() {
        let config = SystemCatalogConfig {
            cdc: CdcConfig {
                system_schema: "platform_system".to_string(),
                outbox_table: "events_outbox".to_string(),
                offsets_table: "cdc_offsets".to_string(),
                lock_log_table: "cdc_locks".to_string(),
                ..CdcConfig::default()
            },
            abac_schema: "security".to_string(),
            abac_table: "policies".to_string(),
            saga_table: "sagas".to_string(),
            ..SystemCatalogConfig::default()
        };

        let sql = system_catalog_ddl(&config);

        assert!(sql.contains("CREATE SCHEMA IF NOT EXISTS \"platform_system\""));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"platform_system\".\"events_outbox\""));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"platform_system\".\"cdc_offsets\""));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"security\".\"policies\""));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"platform_system\".\"sagas\""));
        assert!(sql.ends_with(";\n"));
        assert!(!sql.contains("example"));
        assert!(!sql.contains("ocr"));
    }

    #[test]
    fn expected_relations_match_runtime_catalog_roles() {
        let config = SystemCatalogConfig::default();
        let relations = expected_relations(&config);
        let roles = relations
            .iter()
            .map(|relation| relation.role.as_str())
            .collect::<Vec<_>>();

        for role in [
            "cdc_outbox",
            "cdc_offsets",
            "cdc_lock_log",
            "abac_policies",
            "saga_coordinator",
            "catalog_versions",
            "catalog_activation_log",
            "project_catalog_bindings",
            "catalog_reload_log",
            "migration_runs",
            "migration_op_ledger",
            "cdc_event_journal",
            "cdc_dlq_events",
            "cdc_control",
            "topic_policy",
            "admin_audit_log",
            "projects",
            "projection_tasks",
            "idempotency_keys",
            "row_revisions",
            "native_udb_authn_users",
            "native_udb_authn_sessions",
            "native_udb_authn_otps",
            "native_udb_authn_api_keys",
            "native_udb_authz_policy_rules",
            "native_udb_authz_roles",
            "native_udb_authz_user_roles",
        ] {
            assert!(
                roles.contains(&role),
                "missing expected relation role {role}"
            );
        }
        assert!(relations.iter().all(|relation| {
            relation.display_relation.starts_with("udb_system.")
                || relation.display_relation.starts_with("udb_authn.")
                || relation.display_relation.starts_with("udb_authz.")
                || relation.display_relation.starts_with("udb_apikey.")
                || relation.display_relation.starts_with("udb_tenant.")
                || relation.display_relation.starts_with("udb_notification.")
                || relation.display_relation.starts_with("udb_analytics.")
                || relation.display_relation.starts_with("udb_idp.")
                || relation.display_relation.starts_with("udb_storage.")
                || relation.display_relation.starts_with("udb_asset.")
                || relation.display_relation.starts_with("udb_webrtc.")
                || relation.display_relation.starts_with("udb_control.")
                // Phase 9 native services (cache/livequery hold no canonical entity).
                || relation.display_relation.starts_with("udb_lock.")
                || relation.display_relation.starts_with("udb_scheduler.")
                || relation.display_relation.starts_with("udb_vault.")
                || relation.display_relation.starts_with("udb_webhook.")
                || relation.display_relation.starts_with("udb_backup.")
                || relation.display_relation.starts_with("udb_search.")
                || relation.display_relation.starts_with("udb_config.")
                || relation.display_relation.starts_with("udb_metering.")
                || relation.display_relation.starts_with("udb_workflow.")
                || relation.display_relation.starts_with("udb_embedding.")
        }));
    }

    /// The shape check in `ensure_system_catalog` can REFUSE TO START the broker,
    /// so a parser bug here would brick every healthy deployment. Run the parser
    /// over every real bootstrap statement and assert it either declines cleanly
    /// or returns plausible identifiers - never an invented column name.
    #[test]
    fn declared_table_columns_parses_every_real_bootstrap_statement() {
        let statements = system_catalog_statements(&SystemCatalogConfig::default());
        let mut parsed = 0usize;
        for stmt in &statements {
            let Some((relation, columns)) = declared_table_columns(stmt) else {
                // Only non-CREATE-TABLE statements (CREATE SCHEMA, indexes) may decline.
                assert!(
                    !stmt.trim_start().starts_with("CREATE TABLE IF NOT EXISTS "),
                    "a real CREATE TABLE statement failed to parse, so its shape would go                      unverified: {stmt}"
                );
                continue;
            };
            parsed += 1;
            assert!(!relation.is_empty(), "empty relation from: {stmt}");
            assert!(!columns.is_empty(), "no columns parsed from: {stmt}");
            for c in &columns {
                assert!(
                    c.chars().all(|ch| ch.is_alphanumeric() || ch == '_'),
                    "implausible column name {c:?} parsed from: {stmt}"
                );
                let up = c.to_ascii_uppercase();
                assert!(
                    !matches!(
                        up.as_str(),
                        "PRIMARY" | "UNIQUE" | "CHECK" | "CONSTRAINT" | "FOREIGN"
                    ),
                    "constraint keyword {c:?} mistaken for a column in: {stmt}"
                );
                // Every parsed column must actually appear in the DDL text.
                assert!(
                    stmt.contains(c.as_str()),
                    "column {c:?} not present in: {stmt}"
                );
            }
        }
        assert!(
            parsed >= 20,
            "expected the real bootstrap to yield many tables, got {parsed}"
        );
    }

    /// Nested parens (VARCHAR(80), NUMERIC(10,2)) must not split a column, and
    /// table-level constraints must not be mistaken for columns.
    #[test]
    fn declared_table_columns_handles_nested_parens_and_constraints() {
        let ddl = "CREATE TABLE IF NOT EXISTS \"s\".\"t\" (             id UUID PRIMARY KEY,             amount NUMERIC(10,2) NOT NULL DEFAULT 0,             label VARCHAR(80) NOT NULL DEFAULT '',             PRIMARY KEY (id, label),             CHECK (amount >= 0) )";
        let (rel, cols) = declared_table_columns(ddl).expect("must parse");
        assert_eq!(rel, "\"s\".\"t\"");
        assert_eq!(cols, vec!["id", "amount", "label"]);
    }

    #[test]
    fn declared_table_columns_declines_non_create_table() {
        assert!(declared_table_columns("CREATE SCHEMA IF NOT EXISTS udb_system").is_none());
        assert!(declared_table_columns("CREATE INDEX IF NOT EXISTS i ON t (c)").is_none());
    }
}
