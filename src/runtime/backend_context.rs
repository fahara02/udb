//! Backend request-context enforcement (NW-deep parity).
//!
//! Pre-NW-deep, only Postgres enforced `RequestContext` — its executor
//! wrapped every dispatched SQL in a transaction and called
//! `set_request_local_settings` so RLS policies could see
//! `app.current_tenant_id` / `app.current_project_id` / etc. Every other
//! executor (MySQL, SQLite, ClickHouse, MongoDB, Neo4j, Redis, Qdrant,
//! S3) silently dropped the context. Multi-tenancy on those backends was
//! therefore application-trust, not broker-enforced.
//!
//! This module supplies a uniform contract so every executor can apply
//! the active context in its own native idiom:
//!
//! - SQL backends (PG/MySQL/SQLite/CH): SET session variables that
//!   policies/views can read.
//! - Document / graph backends (Mongo/Neo4j): emit a context predicate
//!   (filter prefix or Cypher parameter binding) that callers AND
//!   together with their own filters.
//! - KV / object backends (Redis/Qdrant/S3): emit a key namespace prefix
//!   that callers prepend to keys / bucket paths.
//!
//! The trait's outputs are **structured**, not free-form SQL — each
//! executor lowers `AppliedContext` to the right shape during dispatch.
//! That keeps the enforcement decision in one place per backend.
//!
//! ## Effect levels
//!
//! Every `enforce` call returns a `ContextEffect` so the operator can
//! see how strongly the context was applied:
//!
//! - `Enforced` — the broker actively constrains backend operations
//!   (SQL session vars set; key prefix prepended; filter ANDed in).
//! - `Advisory` — the context is recorded for audit / observability
//!   but not enforced at the protocol layer (e.g. Mongo without
//!   `$jsonSchema` validators on the collection).
//! - `Unsupported { reason }` — backend can't carry the context
//!   meaningfully. Surfaces in metrics so the operator knows when
//!   their RLS posture is application-trust.
//!
//! The capability matrix's `supports_rls` flag now reflects whether
//! the executor implements `Enforced` (true) or stops at `Advisory`
//! (still true if context is recorded somewhere) or `Unsupported`
//! (false).

use std::collections::BTreeMap;

use crate::broker::RequestContext;

/// The structured context that each backend's enforcer lowers to its
/// native form. Compiled once per request and reused across multiple
/// operations against the same backend.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppliedContext {
    /// Tenant identifier — empty when not provided (treated as
    /// `default`).
    pub tenant_id: String,
    /// Project identifier within the tenant.
    pub project_id: String,
    /// Why this request is being made — used by audit / purpose
    /// limitation policies. Empty when unspecified.
    pub purpose: String,
    /// Comma-separated scope list. The caller-side authorisation
    /// already validated the scopes; the broker emits them so backend
    /// policies can introspect (e.g. read-only scope → read-only view).
    pub scopes: String,
    /// Correlation id propagated to backend logs / spans.
    pub correlation_id: String,
    /// Additional key/value attributes the planner added (e.g.
    /// `replica_pin`, `cache_bypass`). Backends that can record arbitrary
    /// metadata (Mongo session metadata, Neo4j tx metadata) propagate
    /// these verbatim.
    pub attributes: BTreeMap<String, String>,
}

impl AppliedContext {
    /// Build from the active `RequestContext`. Always succeeds — a fully
    /// empty context is valid (treated as `default` tenant by the
    /// SQL-side `current_setting('app.current_tenant_id', true)` reads).
    pub fn from_request(ctx: &RequestContext) -> Self {
        Self {
            tenant_id: ctx.tenant_id.clone(),
            project_id: ctx.project_id.clone(),
            purpose: ctx.purpose.clone(),
            scopes: ctx.scopes.join(","),
            correlation_id: ctx.correlation_id.clone(),
            attributes: BTreeMap::new(),
        }
    }

    /// Convenience: is anything meaningful set? Used by enforcers to
    /// short-circuit when nothing constrains the request.
    pub fn is_empty(&self) -> bool {
        self.tenant_id.is_empty()
            && self.project_id.is_empty()
            && self.purpose.is_empty()
            && self.scopes.is_empty()
            && self.correlation_id.is_empty()
            && self.attributes.is_empty()
    }
}

/// How strongly the backend applied the context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextEffect {
    /// Backend actively constrains operations based on the context.
    /// SQL backends with session vars, KV/object stores with key
    /// prefix scoping, document stores with filter prefixes all land
    /// here.
    Enforced { mechanism: String },
    /// Context is recorded (audit, tx metadata, span) but not used to
    /// constrain operations. Useful for observability without breaking
    /// existing deployments where the operator hasn't configured
    /// per-tenant collections / databases.
    Advisory { recorded_in: String },
    /// Backend can't carry the context. Surfaces in metrics so the
    /// operator sees their actual RLS posture rather than assuming
    /// universal enforcement.
    Unsupported { reason: String },
}

impl ContextEffect {
    pub fn is_enforced(&self) -> bool {
        matches!(self, Self::Enforced { .. })
    }
    pub fn is_unsupported(&self) -> bool {
        matches!(self, Self::Unsupported { .. })
    }
}

/// Backend-specific request-context applicator. Implementations live
/// next to each executor (e.g. `runtime/executors/postgres_context.rs`)
/// so the SQL/JSON/Cypher emission stays close to the dispatch path.
///
/// Object-safe so the runtime can hold heterogeneous applicators in a
/// `Vec<Box<dyn BackendContextEnforcer>>` for fan-out enforcement.
pub trait BackendContextEnforcer: Send + Sync {
    /// Backend label (canonical lowercase: `"postgres"`, `"mysql"`, …).
    fn backend_label(&self) -> &str;

    /// Apply the context and report the effect. Implementations either
    /// emit SQL session variables (SQL backends), prepend filter
    /// fragments (document backends), or produce a key prefix (KV /
    /// object stores).
    ///
    /// The applied state is intentionally short-lived — the runtime
    /// constructs an enforcer per request and discards it after the
    /// operation completes. Persisting session vars across requests
    /// would defeat the per-request RLS guarantee.
    fn enforce(&self, ctx: &AppliedContext) -> ContextEffect;
}

/// Built-in default for SQL backends that talk to a session-aware
/// driver (PG, MySQL, ClickHouse) — emits a list of `SET` statements
/// the executor prepends to the user SQL inside a request-scoped
/// transaction.
///
/// Returns SQL fragments, not a connection-bound effect — actually
/// issuing the SETs is the executor's responsibility (it owns the
/// connection / transaction). This keeps the enforcer testable without
/// a live database.
pub fn render_sql_session_settings(ctx: &AppliedContext, dialect: SqlDialect) -> Vec<String> {
    if ctx.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let pairs = [
        ("app.current_tenant_id", ctx.tenant_id.as_str()),
        ("app.current_project_id", ctx.project_id.as_str()),
        ("app.current_purpose", ctx.purpose.as_str()),
        ("app.current_scopes", ctx.scopes.as_str()),
        ("app.current_correlation_id", ctx.correlation_id.as_str()),
    ];
    for (key, value) in pairs {
        if value.is_empty() {
            continue;
        }
        match dialect {
            SqlDialect::Postgres => {
                // Postgres uses `SET LOCAL` inside the transaction so
                // the setting is rolled back automatically. Quoting is
                // single-quote escape via doubling — the same shape the
                // existing `set_request_local_settings` uses.
                let escaped = value.replace('\'', "''");
                out.push(format!("SET LOCAL {key} = '{escaped}'"));
            }
            SqlDialect::Mysql => {
                // MySQL uses session vars: `SET @app_current_tenant_id =
                // '<value>'`. Dots aren't legal in user-variable names,
                // so we munge the key to underscores.
                let var_name = key.replace('.', "_");
                let escaped = value.replace('\'', "''");
                out.push(format!("SET @{var_name} = '{escaped}'"));
            }
            SqlDialect::Sqlite => {
                // SQLite has no per-connection settings outside PRAGMAs,
                // and PRAGMAs don't accept string values for arbitrary
                // keys. The convention is a temporary table named
                // `_udb_context(key TEXT PRIMARY KEY, value TEXT)`
                // populated each request — RLS-style views read from
                // it. The executor creates the temp table on first
                // touch.
                let escaped_key = key.replace('\'', "''");
                let escaped_value = value.replace('\'', "''");
                out.push(format!(
                    "INSERT OR REPLACE INTO _udb_context(key, value) \
                     VALUES ('{escaped_key}', '{escaped_value}')"
                ));
            }
            SqlDialect::Clickhouse => {
                // ClickHouse has session-scoped settings via the HTTP
                // API; the executor sends them as query parameters
                // alongside the SQL. We render them as `SET <key> =
                // '<value>'` here so the executor can emit them inline
                // when the driver doesn't support per-query settings.
                let escaped_key = key.replace('.', "_");
                let escaped_value = value.replace('\'', "''");
                out.push(format!("SET {escaped_key} = '{escaped_value}'"));
            }
            SqlDialect::Mssql => {
                // A3 (2026-05-30): T-SQL exposes a per-connection
                // key/value store via `sp_set_session_context` +
                // `SESSION_CONTEXT()`. Operator-installed RLS
                // policies / views read that to filter rows. Keys
                // are coerced to underscored form so they parse as
                // sysname identifiers. `@read_only = 1` makes the
                // value immutable for the rest of the connection
                // — the broker is the sole writer, so this stops a
                // misbehaving stored procedure from rewriting the
                // tenant ID mid-request.
                let key_id = key.replace('.', "_");
                // T-SQL string literals: double the single quote to
                // escape. Names go in N'...' (nvarchar literal).
                let escaped_key = key_id.replace('\'', "''");
                let escaped_value = value.replace('\'', "''");
                out.push(format!(
                    "EXEC sp_set_session_context @key = N'{escaped_key}', \
                     @value = N'{escaped_value}', @read_only = 1"
                ));
            }
        }
    }
    out
}

/// SQL dialect selector for `render_sql_session_settings`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Postgres,
    Mysql,
    Sqlite,
    Clickhouse,
    /// A3 (2026-05-30): Microsoft SQL Server T-SQL dialect.
    /// `sp_set_session_context` populates `SESSION_CONTEXT()` which
    /// row-level-security policies and predicates read at query time.
    Mssql,
}

/// Built-in default for document backends — render a JSON filter
/// fragment the executor ANDs together with the caller's query filter.
/// Returns `None` when the context is empty (no constraint to add).
///
/// The convention is that documents carry top-level `_tenant_id` and
/// `_project_id` fields; the broker-issued writes will include them.
/// Operators who want stricter enforcement can add a `$jsonSchema`
/// validator on the collection.
pub fn render_document_filter_prefix(ctx: &AppliedContext) -> Option<serde_json::Value> {
    if ctx.tenant_id.is_empty() && ctx.project_id.is_empty() {
        return None;
    }
    let mut prefix = serde_json::Map::new();
    if !ctx.tenant_id.is_empty() {
        prefix.insert(
            "_tenant_id".into(),
            serde_json::Value::String(ctx.tenant_id.clone()),
        );
    }
    if !ctx.project_id.is_empty() {
        prefix.insert(
            "_project_id".into(),
            serde_json::Value::String(ctx.project_id.clone()),
        );
    }
    Some(serde_json::Value::Object(prefix))
}

/// Built-in default for KV / object stores — render a key namespace
/// prefix the executor prepends to keys / object paths. Returns an
/// empty string when the context is empty so prefix-less callers still
/// work.
pub fn render_kv_key_prefix(ctx: &AppliedContext) -> String {
    if ctx.tenant_id.is_empty() && ctx.project_id.is_empty() {
        return String::new();
    }
    let tenant = if ctx.tenant_id.is_empty() {
        "default"
    } else {
        ctx.tenant_id.as_str()
    };
    let project = if ctx.project_id.is_empty() {
        "default"
    } else {
        ctx.project_id.as_str()
    };
    format!("t:{tenant}/p:{project}/")
}

/// Built-in default for Cypher (Neo4j) — render a parameter map the
/// executor includes in every Cypher request. Cypher itself can read
/// parameters via `$ctx_tenant_id` and AND them into MATCH clauses.
pub fn render_cypher_context_parameters(
    ctx: &AppliedContext,
) -> std::collections::HashMap<String, serde_json::Value> {
    let mut params = std::collections::HashMap::new();
    if !ctx.tenant_id.is_empty() {
        params.insert(
            "ctx_tenant_id".to_string(),
            serde_json::Value::String(ctx.tenant_id.clone()),
        );
    }
    if !ctx.project_id.is_empty() {
        params.insert(
            "ctx_project_id".to_string(),
            serde_json::Value::String(ctx.project_id.clone()),
        );
    }
    if !ctx.purpose.is_empty() {
        params.insert(
            "ctx_purpose".to_string(),
            serde_json::Value::String(ctx.purpose.clone()),
        );
    }
    params
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_context_renders_no_sql() {
        let ctx = AppliedContext::default();
        assert!(render_sql_session_settings(&ctx, SqlDialect::Postgres).is_empty());
        assert!(render_sql_session_settings(&ctx, SqlDialect::Mysql).is_empty());
    }

    #[test]
    fn postgres_uses_set_local_with_double_single_escape() {
        let ctx = AppliedContext {
            tenant_id: "acme'corp".into(),
            project_id: "p1".into(),
            ..Default::default()
        };
        let stmts = render_sql_session_settings(&ctx, SqlDialect::Postgres);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("SET LOCAL app.current_tenant_id = 'acme''corp'"));
        assert!(stmts[1].contains("SET LOCAL app.current_project_id = 'p1'"));
    }

    #[test]
    fn mysql_munges_dots_to_underscores_and_uses_user_vars() {
        let ctx = AppliedContext {
            tenant_id: "acme".into(),
            project_id: "p1".into(),
            ..Default::default()
        };
        let stmts = render_sql_session_settings(&ctx, SqlDialect::Mysql);
        assert!(stmts[0].contains("SET @app_current_tenant_id = 'acme'"));
        assert!(stmts[1].contains("SET @app_current_project_id = 'p1'"));
    }

    #[test]
    fn sqlite_inserts_into_context_temp_table() {
        let ctx = AppliedContext {
            tenant_id: "acme".into(),
            project_id: "p1".into(),
            ..Default::default()
        };
        let stmts = render_sql_session_settings(&ctx, SqlDialect::Sqlite);
        assert!(stmts[0].contains("INSERT OR REPLACE INTO _udb_context"));
        assert!(stmts[0].contains("VALUES ('app.current_tenant_id', 'acme')"));
    }

    #[test]
    fn mssql_emits_sp_set_session_context_with_nvarchar_literals() {
        let ctx = AppliedContext {
            tenant_id: "acme'corp".into(), // includes a single-quote
            project_id: "p1".into(),
            ..Default::default()
        };
        let stmts = render_sql_session_settings(&ctx, SqlDialect::Mssql);
        assert_eq!(stmts.len(), 2);
        // A3: keys are underscored (no dots) and string literals are
        // doubly-escaped + prefixed with `N` for nvarchar.
        assert!(
            stmts[0].contains(
                "EXEC sp_set_session_context @key = N'app_current_tenant_id', \
                 @value = N'acme''corp', @read_only = 1"
            ),
            "got: {}",
            stmts[0]
        );
        assert!(stmts[1].contains("@key = N'app_current_project_id'"));
        assert!(stmts[1].contains("@value = N'p1'"));
        assert!(stmts[1].contains("@read_only = 1"));
    }

    #[test]
    fn document_filter_prefix_emits_underscore_fields() {
        let ctx = AppliedContext {
            tenant_id: "acme".into(),
            project_id: "p1".into(),
            ..Default::default()
        };
        let prefix = render_document_filter_prefix(&ctx).unwrap();
        assert_eq!(prefix["_tenant_id"], "acme");
        assert_eq!(prefix["_project_id"], "p1");
    }

    #[test]
    fn kv_key_prefix_uses_default_when_field_missing() {
        let ctx = AppliedContext {
            tenant_id: "acme".into(),
            ..Default::default()
        };
        let prefix = render_kv_key_prefix(&ctx);
        // tenant set, project empty → "default" sentinel for project.
        assert_eq!(prefix, "t:acme/p:default/");
    }

    #[test]
    fn kv_key_prefix_empty_when_nothing_set() {
        let ctx = AppliedContext::default();
        assert_eq!(render_kv_key_prefix(&ctx), "");
    }

    #[test]
    fn cypher_parameters_skip_empty_fields() {
        let ctx = AppliedContext {
            tenant_id: "acme".into(),
            ..Default::default()
        };
        let params = render_cypher_context_parameters(&ctx);
        assert_eq!(params.len(), 1);
        assert_eq!(params["ctx_tenant_id"], "acme");
        assert!(!params.contains_key("ctx_project_id"));
    }

    #[test]
    fn context_effect_classifies_correctly() {
        let enforced = ContextEffect::Enforced {
            mechanism: "SET LOCAL".into(),
        };
        assert!(enforced.is_enforced());
        assert!(!enforced.is_unsupported());

        let advisory = ContextEffect::Advisory {
            recorded_in: "tx_metadata".into(),
        };
        assert!(!advisory.is_enforced());
        assert!(!advisory.is_unsupported());

        let unsupported = ContextEffect::Unsupported {
            reason: "backend has no session settings".into(),
        };
        assert!(!unsupported.is_enforced());
        assert!(unsupported.is_unsupported());
    }
}
