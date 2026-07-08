//! PostgreSQL generic-dispatch executor.
//!
//! `BackendExecutor` over a `PgPool`. Stateless leaf I/O on an
//! **already-selected** pool: the orchestration layer (`DataBrokerRuntime`)
//! resolves the instance pool via `pg_pool_for_instance` and hands it here. The
//! §9.1 cross-cutting concerns — replica routing, read/write consistency,
//! read-through cache, field encryption — live in the typed `select`/`upsert`
//! RPCs, NOT in this generic-dispatch path, so they are intentionally absent here.
//!
//! ## U7 — RLS context on the generic-dispatch path
//!
//! Before U7 the generic dispatch path ran SQL directly against the pool, so
//! the transaction-local `app.current_tenant_id` / `app.current_project_id`
//! settings that the typed `Select`/`Upsert` RPCs install were **never set**
//! for generic dispatch — meaning RLS policies that read those settings
//! silently failed open. U7 wraps `query`/`mutate` in a request-scoped
//! transaction when the executor is built with a `RequestContext`, calls
//! `set_request_local_settings` inside that transaction, then runs the SQL
//! on the same `tx`. The cost is one extra round-trip (`BEGIN` + n×
//! `set_config` + `COMMIT`) but it makes RLS the same on the generic path
//! as on the typed path.
//!
//! Probes (health/ping) and other internal callers still construct an
//! executor without context — `with_pool` — and skip the transaction.

use std::sync::Arc;

use serde_json::Value as JsonValue;
use sqlx::PgPool;

use crate::broker::RequestContext;
use crate::runtime::core::{
    bind_typed_generic_pg_params, dispatch_param_types, dispatch_params, pg_rows_to_json,
    set_request_local_settings, validate_pg_mutation_sql, validate_pg_read_sql,
};
use crate::runtime::executor_utils::{
    build_probe, capability_status, invalid_argument_fields, json_required_str,
    sqlx_error_to_status, with_executor_timeout,
};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

pub(crate) struct PostgresExecutor {
    pub(crate) pool: PgPool,
    /// Optional request context. When `Some`, `query`/`mutate` wrap the SQL
    /// in a transaction and install `app.current_tenant_id` /
    /// `app.current_project_id` / `app.current_purpose` etc. transaction-
    /// locally before running the user statement, so RLS policies that
    /// read those settings are enforced. When `None` (probe paths) the
    /// SQL runs directly against the pool.
    pub(crate) context: Option<Arc<RequestContext>>,
}

impl PostgresExecutor {
    /// Construct a context-less executor — used by probes/health/dispatcher
    /// fallbacks where no `RequestContext` is available. RLS settings are
    /// **not** installed.
    pub(crate) fn with_pool(pool: PgPool) -> Self {
        Self {
            pool,
            context: None,
        }
    }

    /// Construct a context-bound executor that wraps every generic
    /// dispatch in a request-scoped transaction with RLS settings.
    pub(crate) fn with_context(pool: PgPool, context: Arc<RequestContext>) -> Self {
        Self {
            pool,
            context: Some(context),
        }
    }
}

impl crate::runtime::backend_context::BackendContextEnforcer for PostgresExecutor {
    fn backend_label(&self) -> &str {
        "postgres"
    }

    fn enforce(
        &self,
        ctx: &crate::runtime::backend_context::AppliedContext,
    ) -> crate::runtime::backend_context::ContextEffect {
        // PostgresExecutor applies `set_request_local_settings` inside
        // its per-request transaction, so RLS policies that read
        // `current_setting('app.current_tenant_id', true)` see the
        // active tenant. This is the gold-standard enforcement path.
        crate::runtime::backend_context::enforce_with_mechanism(
            ctx,
            "SET LOCAL app.current_* in request-scoped transaction (RLS policies)",
        )
    }
}

impl BackendHealth for PostgresExecutor {
    async fn ping(&self) -> Result<(), String> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

fn invalid_postgres_request_json_status(err: serde_json::Error) -> tonic::Status {
    invalid_argument_fields(
        format!("invalid request json: {err}"),
        [(
            "request_json",
            "must be valid JSON for PostgreSQL generic dispatch",
        )],
    )
}

fn postgres_executor_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status("postgres", operation, message)
}

fn encode_postgres_response(
    operation: &'static str,
    value: &JsonValue,
) -> Result<String, tonic::Status> {
    serde_json::to_string(value)
        .map_err(|e| postgres_executor_internal_status(operation, e.to_string()))
}

impl QueryExecutor for PostgresExecutor {
    /// `{"sql":"SELECT ...","params":[...]}` — read-only single statement.
    ///
    /// When `self.context` is set, the SQL runs inside a request-scoped
    /// transaction with `set_request_local_settings` applied first, so RLS
    /// policies see the tenant/project/purpose values. Without a context,
    /// the SQL runs directly on the pool (probe/internal callers).
    async fn query(&self, request_json: &str) -> Result<String, tonic::Status> {
        with_executor_timeout("PostgreSQL", "query", async {
            let spec: JsonValue =
                serde_json::from_str(request_json).map_err(invalid_postgres_request_json_status)?;
            let sql = json_required_str(&spec, "sql")?;
            validate_pg_read_sql(sql)?;
            let params = dispatch_params(&spec)?;
            let param_types = dispatch_param_types(&spec)?;

            let rows = if let Some(ctx) = &self.context {
                let mut tx = self.pool.begin().await.map_err(|err| {
                    postgres_executor_internal_status(
                        "query_transaction_start",
                        format!("PostgreSQL transaction start failed: {err}"),
                    )
                })?;
                set_request_local_settings(&mut tx, ctx).await?;
                let rows = bind_typed_generic_pg_params(
                    sqlx::query(sql),
                    &params,
                    param_types.as_deref(),
                )?
                .fetch_all(&mut *tx)
                .await
                .map_err(|err| {
                    postgres_executor_internal_status(
                        "query",
                        format!("PostgreSQL generic query failed: {err}"),
                    )
                })?;
                tx.commit().await.map_err(|err| {
                    postgres_executor_internal_status(
                        "query_transaction_commit",
                        format!("PostgreSQL transaction commit failed: {err}"),
                    )
                })?;
                rows
            } else {
                bind_typed_generic_pg_params(sqlx::query(sql), &params, param_types.as_deref())?
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|err| {
                        postgres_executor_internal_status(
                            "query",
                            format!("PostgreSQL generic query failed: {err}"),
                        )
                    })?
            };
            let rows_json = JsonValue::Array(pg_rows_to_json(rows)?);
            encode_postgres_response("query_response_encode", &rows_json)
        })
        .await
    }
}

impl MutationExecutor for PostgresExecutor {
    /// `{"sql":"INSERT|UPDATE|DELETE ...","params":[...],"return_rows"?}`.
    ///
    /// Mirrors `query`: when `self.context` is set, the mutation runs
    /// inside a request-scoped transaction with `set_request_local_settings`
    /// applied first so RLS / row-level write policies see the same
    /// tenant/project/purpose values as the typed Upsert RPC.
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status> {
        with_executor_timeout("PostgreSQL", "mutate", async {
            let spec: JsonValue =
                serde_json::from_str(request_json).map_err(invalid_postgres_request_json_status)?;
            let sql = json_required_str(&spec, "sql")?;
            validate_pg_mutation_sql(sql)?;
            let params = dispatch_params(&spec)?;
            let param_types = dispatch_param_types(&spec)?;
            let return_rows = spec
                .get("return_rows")
                .and_then(JsonValue::as_bool)
                .unwrap_or_else(|| sql.to_ascii_lowercase().contains(" returning "));

            if let Some(ctx) = &self.context {
                let mut tx = self.pool.begin().await.map_err(|err| {
                    postgres_executor_internal_status(
                        "mutate_transaction_start",
                        format!("PostgreSQL transaction start failed: {err}"),
                    )
                })?;
                set_request_local_settings(&mut tx, ctx).await?;
                let result = if return_rows {
                    let rows = bind_typed_generic_pg_params(
                        sqlx::query(sql),
                        &params,
                        param_types.as_deref(),
                    )?
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|err| {
                        sqlx_error_to_status("PostgreSQL generic mutation failed", &err)
                    })?;
                    let rows_json = pg_rows_to_json(rows)?;
                    serde_json::json!({
                        "affected_rows": rows_json.len(),
                        "rows": rows_json
                    })
                    .to_string()
                } else {
                    let result = bind_typed_generic_pg_params(
                        sqlx::query(sql),
                        &params,
                        param_types.as_deref(),
                    )?
                    .execute(&mut *tx)
                    .await
                    .map_err(|err| {
                        sqlx_error_to_status("PostgreSQL generic mutation failed", &err)
                    })?;
                    serde_json::json!({ "affected_rows": result.rows_affected() }).to_string()
                };
                tx.commit().await.map_err(|err| {
                    postgres_executor_internal_status(
                        "mutate_transaction_commit",
                        format!("PostgreSQL transaction commit failed: {err}"),
                    )
                })?;
                return Ok(result);
            }

            if return_rows {
                let rows = bind_typed_generic_pg_params(
                    sqlx::query(sql),
                    &params,
                    param_types.as_deref(),
                )?
                .fetch_all(&self.pool)
                .await
                .map_err(|err| sqlx_error_to_status("PostgreSQL generic mutation failed", &err))?;
                let rows_json = pg_rows_to_json(rows)?;
                return Ok(serde_json::json!({
                    "affected_rows": rows_json.len(),
                    "rows": rows_json
                })
                .to_string());
            }
            let result =
                bind_typed_generic_pg_params(sqlx::query(sql), &params, param_types.as_deref())?
                    .execute(&self.pool)
                    .await
                    .map_err(|err| {
                        sqlx_error_to_status("PostgreSQL generic mutation failed", &err)
                    })?;
            Ok(serde_json::json!({ "affected_rows": result.rows_affected() }).to_string())
        })
        .await
    }
}

impl SearchExecutor for PostgresExecutor {
    async fn search(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(capability_status(
            "postgres",
            "search",
            "vector_search",
            "postgres does not support vector search; use query",
        ))
    }
}

impl ObjectExecutor for PostgresExecutor {
    async fn get_object(&self, _request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(capability_status(
            "postgres",
            "get_object",
            "object_store",
            "postgres is not an object store",
        ))
    }
    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(capability_status(
            "postgres",
            "put_object",
            "object_store",
            "postgres is not an object store",
        ))
    }
}

impl ResourceAdminExecutor for PostgresExecutor {
    // Relational DDL flows through the catalog migration path, not generic
    // resource admin.
    async fn ensure_resource(
        &self,
        _resource_name: &str,
        _spec_json: &str,
    ) -> Result<(), tonic::Status> {
        Err(capability_status(
            "postgres",
            "ensure_resource",
            "resource_lifecycle",
            "postgres resource lifecycle is managed via catalog migrations",
        ))
    }
    async fn drop_resource(&self, _resource_name: &str) -> Result<(), tonic::Status> {
        Err(capability_status(
            "postgres",
            "drop_resource",
            "resource_lifecycle",
            "postgres resource lifecycle is managed via catalog migrations",
        ))
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        Err(capability_status(
            "postgres",
            "list_resources",
            "resource_lifecycle",
            "postgres resource lifecycle is managed via catalog migrations",
        ))
    }
}

impl BackendExecutor for PostgresExecutor {
    // Generic cross-statement transactions are coordinated by the runtime
    // (saga/outbox + the typed transaction RPC); the generic-dispatch executor
    // does not expose a bespoke transaction here.
    async fn transaction(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(capability_status(
            "postgres",
            "transaction",
            "typed_transaction_rpc",
            "use the typed transaction RPC for relational transactions",
        ))
    }
    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(build_probe(
            "postgres",
            <Self as BackendHealth>::ping(self).await,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use prost::Message as _;

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
        assert_eq!(detail.backend, "postgres");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
    }

    #[test]
    fn postgres_executor_internal_status_carries_typed_detail() {
        let status = postgres_executor_internal_status(
            "query_response_encode",
            "PostgreSQL response encode failed",
        );
        assert_internal_detail(
            &status,
            "query_response_encode",
            "PostgreSQL response encode failed",
        );
    }

    #[test]
    fn request_json_validation_carries_field_violation() {
        let err = serde_json::from_str::<JsonValue>("{")
            .map_err(invalid_postgres_request_json_status)
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().starts_with("invalid request json:"));
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "request_json");
    }
}
