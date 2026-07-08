//! Native-service persistence discovery seam (extend_udb.md, P0).
//!
//! Native control-plane services historically grabbed the process-global primary
//! pool via [`DataBrokerRuntime::pg_pool`], pinning every service to one Postgres
//! connection with no health/weight routing and no failover. This module introduces
//! the single discovery seam every `build_*_service` consults instead, so native
//! services inherit the data plane's instance selection (replica reads, weighted
//! routing, circuit-breaker skipping) — reusing [`DataBrokerRuntime::
//! choose_instance_name_for_project`] and [`DataBrokerRuntime::pg_pool_for_instance`]
//! rather than a parallel mechanism.
//!
//! The backend is DERIVED FROM PROTO by default — the native-service contracts
//! (`requires_postgres` &c.) are the authority, read via
//! [`crate::runtime::service::native_store_binding`] off the embedded descriptor, not
//! a hand-picked constant (directive #1). `UDB_NATIVE_STORE` is an explicit operator
//! OVERRIDE of that proto default, not the source of truth.
//!
//! Postgres-only and behavior-preserving today: a single-primary deployment resolves
//! to the same pool it used before. The persistence handle is the backend-neutral
//! [`NativeEntityStore`] (P1, Postgres impl); per-backend impls + the per-service SQL
//! migration onto neutral operations are P2–P4 in extend_udb.md.
use super::*;
use crate::ir::{
    AggregateExpr, AggregateFunc, LogicalAggregate, LogicalDelete, LogicalFilter, LogicalRead,
    LogicalRecord, LogicalUpdate, LogicalWrite,
};
use crate::runtime::service::native_entity_store::NativeEntityStore;
use std::sync::OnceLock;

#[allow(dead_code)]
pub(crate) enum NativeEntityTransactionOp {
    Write(LogicalWrite),
    Update(LogicalUpdate),
    Delete(LogicalDelete),
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct NativeEntityTransactionStepResult {
    pub(crate) affected_rows: u64,
    pub(crate) rows: Vec<serde_json::Value>,
}

/// Normalize an operator override label: trim + lowercase, `None` when unset/blank.
/// Pure (no env / no OnceLock) so it is unit-testable.
fn normalize_store_override(raw: Option<&str>) -> Option<String> {
    raw.map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
}

/// The `UDB_NATIVE_STORE` operator override, resolved ONCE (directive #5). `None`
/// when unset/blank, in which case proto is the authority. A non-postgres override is
/// warned here (only postgres is implemented in P0) so it is logged exactly once.
fn store_override() -> Option<&'static str> {
    static OVERRIDE: OnceLock<Option<String>> = OnceLock::new();
    OVERRIDE
        .get_or_init(|| {
            let resolved =
                normalize_store_override(std::env::var("UDB_NATIVE_STORE").ok().as_deref());
            if let Some(backend) = resolved.as_deref() {
                if backend != "postgres" {
                    tracing::warn!(
                        backend = %backend,
                        "UDB_NATIVE_STORE overrides the proto-derived native store, but only \
                         'postgres' is implemented; native services fail closed for it — see extend_udb.md"
                    );
                }
            }
            resolved
        })
        .as_deref()
}

/// The global native-store backend: the operator override if set, else the proto
/// default derived from the `native_service` contracts ([`native_store_binding::
/// proto_native_store_backend`]). Used when no specific service is in scope.
fn native_store_backend() -> &'static str {
    store_override().unwrap_or_else(|| {
        match crate::runtime::service::native_store_binding::proto_native_store_backend() {
            "" => "postgres",
            derived => derived,
        }
    })
}

/// Parse the `UDB_NATIVE_STORE_PROJECTS` per-project placement map
/// (`project_a=mysql,project_b=neo4j`). Pure (no env / no OnceLock) so it is
/// unit-testable; trims keys, lowercases backends, drops blank/malformed entries.
fn parse_project_overrides(raw: Option<&str>) -> std::collections::HashMap<String, String> {
    raw.unwrap_or("")
        .split(',')
        .filter_map(|entry| {
            let (project, backend) = entry.split_once('=')?;
            let project = project.trim();
            let backend = backend.trim().to_ascii_lowercase();
            if project.is_empty() || backend.is_empty() {
                None
            } else {
                Some((project.to_string(), backend))
            }
        })
        .collect()
}

fn empty_native_entity_transaction_status() -> tonic::Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        "native entity transaction requires at least one operation",
        [("ops", "must contain at least one operation")],
    )
}

fn native_store_capability_status(
    backend: impl Into<String>,
    operation: &'static str,
    capability_required: &'static str,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::capability_status(
        backend,
        operation,
        capability_required,
        message,
    )
}

fn native_store_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status("native_store", operation, message)
}

fn native_entity_update_not_found_status() -> tonic::Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "native_entity",
        "native_entity_update",
        "native_entity_update_not_found",
        "native entity update affected no rows",
    )
}

/// P4 per-project placement: `project_id → backend` overrides, resolved ONCE from
/// `UDB_NATIVE_STORE_PROJECTS`. Lets a deployment pin one tenant/project's native
/// state to a different backend than the default (e.g. a regulated project on its own
/// SQL backend) while everything else follows the proto-derived binding.
fn project_store_overrides() -> &'static std::collections::HashMap<String, String> {
    static MAP: OnceLock<std::collections::HashMap<String, String>> = OnceLock::new();
    MAP.get_or_init(|| {
        parse_project_overrides(std::env::var("UDB_NATIVE_STORE_PROJECTS").ok().as_deref())
    })
}

/// The backend a SPECIFIC native service persists to for a given project. Resolution
/// order: operator global override (`UDB_NATIVE_STORE`, blanket) → per-project
/// placement (`UDB_NATIVE_STORE_PROJECTS`, P4) → per-service proto binding
/// ([`native_store_binding::native_service_store_backend`]) → global proto default.
fn native_store_backend_for_service(service_id: &str, project_id: &str) -> &'static str {
    if let Some(over) = store_override() {
        return over;
    }
    if !project_id.is_empty() {
        if let Some(backend) = project_store_overrides().get(project_id) {
            return backend.as_str();
        }
    }
    crate::runtime::service::native_store_binding::native_service_store_backend(service_id)
        .unwrap_or_else(native_store_backend)
}

impl DataBrokerRuntime {
    fn native_entity_dispatch_target(
        &self,
        service_id: &str,
        project_id: &str,
        write: bool,
    ) -> Result<
        (
            crate::runtime::core::ResolvedExecutorTarget,
            crate::backend::BackendKind,
        ),
        tonic::Status,
    > {
        let backend = native_store_backend_for_service(service_id, project_id);
        let selected_instance = self.choose_instance_name_for_project(backend, write, project_id);
        let target = self.backend_executor_for_project(backend, selected_instance, project_id)?;
        let kind = crate::backend::BackendKind::from_token(&target.backend).ok_or_else(|| {
            native_store_capability_status(
                target.backend.clone(),
                "compiler_lookup",
                "neutral_ir_compiler",
                format!(
                    "native entity backend '{}' has no neutral-IR compiler",
                    target.backend
                ),
            )
        })?;
        Ok((target, kind))
    }

    fn native_entity_compile_context<'a>(
        context: &'a crate::RequestContext,
        instance: Option<&'a str>,
    ) -> crate::ir::compile::CompileContext<'a> {
        let mut ctx = crate::ir::compile::CompileContext::new(
            crate::runtime::native_catalog::native_manifest(),
        )
        .with_tenant(&context.tenant_id)
        .with_project(&context.project_id);
        if let Some(instance) = instance.filter(|value| !value.trim().is_empty()) {
            ctx = ctx.with_instance(instance);
        }
        ctx
    }

    async fn execute_native_entity_dispatch(
        &self,
        context: &crate::RequestContext,
        target_backend: &str,
        target_instance: Option<&str>,
        write: bool,
        compiled: crate::runtime::service::handlers_data::CompiledDispatchRequest,
    ) -> Result<String, tonic::Status> {
        use crate::runtime::executors::{MutationExecutor, QueryExecutor};

        let mut dispatch_context = context.clone();
        dispatch_context.target_backend = target_backend.to_string();
        dispatch_context.target_instance = target_instance.unwrap_or_default().to_string();
        let executor = self.resolve_dispatch_executor(
            target_backend,
            target_instance,
            write,
            tonic::Code::FailedPrecondition,
            Some(&dispatch_context),
        )?;
        let result = match compiled.operation.as_str() {
            "query" => executor.query(&compiled.spec_json).await,
            "mutate" => executor.mutate(&compiled.spec_json).await,
            other => Err(native_store_capability_status(
                target_backend.to_string(),
                "compiled_dispatch",
                "native_entity_dispatch_operation",
                format!("native entity dispatch compiled unsupported operation '{other}'"),
            )),
        };
        // Circuit-breaker feed (B11): only a genuine *unreachability* failure may
        // count against the backend's circuit breaker. A request-shape error — a
        // bad UUID/timestamp bind, a constraint violation, a type mismatch — means
        // the backend RESPONDED and is healthy; counting it would open the breaker
        // and then `backend_executor_for_project` would reject the (connected)
        // postgres for every subsequent native RPC, *including reads*, surfacing the
        // misleading "backend executor 'postgres:default' is not registered". So we
        // treat only `Unavailable`/`DeadlineExceeded` as health failures; any other
        // response (success or a deterministic error) marks the backend reachable,
        // which resets the breaker. A connectivity failure surfaced as
        // `Unavailable`/`DeadlineExceeded` still trips the breaker; a SQL/Database
        // error (backend reachable) no longer does.
        let reachable = result.as_ref().map_or_else(
            |status| {
                !matches!(
                    status.code(),
                    tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
                )
            },
            |_| true,
        );
        self.record_backend_result(target_backend, target_instance, reachable);
        result
    }

    fn native_entity_rows(result_json: &str) -> Result<Vec<serde_json::Value>, tonic::Status> {
        let value: serde_json::Value = serde_json::from_str(result_json).map_err(|err| {
            native_store_internal_status(
                "native_entity_rows_decode",
                format!("native entity JSON decode failed: {err}"),
            )
        })?;
        match value {
            serde_json::Value::Array(rows) => Ok(rows),
            serde_json::Value::Object(mut obj) => {
                if let Some(serde_json::Value::Array(rows)) = obj.remove("rows") {
                    Ok(rows)
                } else {
                    Err(native_store_internal_status(
                        "native_entity_rows_shape",
                        "native entity query returned a non-row JSON object",
                    ))
                }
            }
            _ => Err(native_store_internal_status(
                "native_entity_rows_shape",
                "native entity query returned non-array JSON",
            )),
        }
    }

    fn native_entity_mutation_result(
        result_json: &str,
    ) -> Result<(u64, Vec<serde_json::Value>), tonic::Status> {
        let value: serde_json::Value = serde_json::from_str(result_json).map_err(|err| {
            native_store_internal_status(
                "native_entity_mutation_decode",
                format!("native entity JSON decode failed: {err}"),
            )
        })?;
        match value {
            serde_json::Value::Array(rows) => Ok((rows.len() as u64, rows)),
            serde_json::Value::Object(mut obj) => {
                let affected_rows = obj
                    .remove("affected_rows")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0);
                let rows = match obj.remove("rows") {
                    Some(serde_json::Value::Array(rows)) => rows,
                    Some(_) => {
                        return Err(native_store_internal_status(
                            "native_entity_mutation_shape",
                            "native entity mutation returned non-array rows",
                        ));
                    }
                    None => Vec::new(),
                };
                Ok((affected_rows, rows))
            }
            _ => Err(native_store_internal_status(
                "native_entity_mutation_shape",
                "native entity mutation returned non-object JSON",
            )),
        }
    }

    /// Execute a typed native-entity read through the same neutral IR compiler and
    /// backend-native dispatch executor used by GenericDispatch. This is the P4
    /// data-plane path for UDB-owned proto entities: no service-local SQL and no
    /// generic KV side table.
    pub(crate) async fn native_entity_read_for_service(
        &self,
        service_id: &str,
        context: &crate::RequestContext,
        op: LogicalRead,
    ) -> Result<Vec<serde_json::Value>, tonic::Status> {
        let (target, kind) =
            self.native_entity_dispatch_target(service_id, &context.project_id, false)?;
        let compile_ctx = Self::native_entity_compile_context(context, target.instance.as_deref());
        let compiled = crate::runtime::service::handlers_data::compile_logical_read_dispatch(
            &kind,
            &op,
            &compile_ctx,
        )?;
        let result = self
            .execute_native_entity_dispatch(
                context,
                &target.backend,
                target.instance.as_deref(),
                false,
                compiled,
            )
            .await?;
        Self::native_entity_rows(&result)
    }

    /// Execute a typed native-entity aggregate through the same neutral IR
    /// compiler/executor path as reads. Callers use this for exact counts and
    /// read-model summaries instead of leaking `COUNT(*) OVER()`/`GROUP BY`
    /// Postgres SQL into native services.
    pub(crate) async fn native_entity_aggregate_for_service(
        &self,
        service_id: &str,
        context: &crate::RequestContext,
        op: LogicalAggregate,
    ) -> Result<Vec<serde_json::Value>, tonic::Status> {
        let (target, kind) =
            self.native_entity_dispatch_target(service_id, &context.project_id, false)?;
        let compile_ctx = Self::native_entity_compile_context(context, target.instance.as_deref());
        let compiled = crate::runtime::service::handlers_data::compile_logical_aggregate_dispatch(
            &kind,
            &op,
            &compile_ctx,
        )?;
        let result = self
            .execute_native_entity_dispatch(
                context,
                &target.backend,
                target.instance.as_deref(),
                false,
                compiled,
            )
            .await?;
        Self::native_entity_rows(&result)
    }

    /// Execute a typed native-entity write through the canonical compiler/executor
    /// path. The caller supplies a [`LogicalRecord`] so field names stay proto
    /// logical names; backend columns/collections/properties are resolved from the
    /// embedded native catalog manifest.
    pub(crate) async fn native_entity_write_for_service(
        &self,
        service_id: &str,
        context: &crate::RequestContext,
        message_type: &str,
        record: LogicalRecord,
        conflict: crate::ir::ConflictStrategy,
    ) -> Result<String, tonic::Status> {
        let op = LogicalWrite {
            message_type: message_type.to_string(),
            records: vec![record],
            conflict,
            return_fields: Vec::new(),
        };
        self.native_entity_write_op_for_service(service_id, context, op)
            .await
    }

    /// Execute a complete typed native-entity write op. This is the shared path for
    /// native services that need batch writes or `return_fields` without reaching
    /// into compiler internals.
    #[allow(dead_code)]
    pub(crate) async fn native_entity_write_op_for_service(
        &self,
        service_id: &str,
        context: &crate::RequestContext,
        op: LogicalWrite,
    ) -> Result<String, tonic::Status> {
        let (target, kind) =
            self.native_entity_dispatch_target(service_id, &context.project_id, true)?;
        let compile_ctx = Self::native_entity_compile_context(context, target.instance.as_deref());
        let compiled = crate::runtime::service::handlers_data::compile_logical_write_dispatch(
            &kind,
            &op,
            &compile_ctx,
        )?;
        self.execute_native_entity_dispatch(
            context,
            &target.backend,
            target.instance.as_deref(),
            true,
            compiled,
        )
        .await
    }

    /// Execute a typed native-entity write and parse `RETURNING`/row-returning
    /// mutation output. Callers must set `return_fields` deliberately; empty return
    /// fields still execute the write but generally return no rows.
    #[allow(dead_code)]
    pub(crate) async fn native_entity_write_for_service_returning(
        &self,
        service_id: &str,
        context: &crate::RequestContext,
        message_type: &str,
        record: LogicalRecord,
        conflict: crate::ir::ConflictStrategy,
        return_fields: Vec<String>,
    ) -> Result<Vec<serde_json::Value>, tonic::Status> {
        let op = LogicalWrite {
            message_type: message_type.to_string(),
            records: vec![record],
            conflict,
            return_fields,
        };
        let result = self
            .native_entity_write_op_for_service(service_id, context, op)
            .await?;
        Self::native_entity_rows(&result)
    }

    /// Execute a typed conditional native-entity update. The compiler enforces a
    /// non-empty filter and typed assignments; `require_affected` is checked here
    /// against the executor's affected-row count so CAS/ownership updates fail closed.
    #[allow(dead_code)]
    pub(crate) async fn native_entity_update_for_service(
        &self,
        service_id: &str,
        context: &crate::RequestContext,
        op: LogicalUpdate,
    ) -> Result<(u64, Vec<serde_json::Value>), tonic::Status> {
        let require_affected = op.require_affected;
        let (target, kind) =
            self.native_entity_dispatch_target(service_id, &context.project_id, true)?;
        let compile_ctx = Self::native_entity_compile_context(context, target.instance.as_deref());
        let compiled = crate::runtime::service::handlers_data::compile_logical_update_dispatch(
            &kind,
            &op,
            &compile_ctx,
        )?;
        let result = self
            .execute_native_entity_dispatch(
                context,
                &target.backend,
                target.instance.as_deref(),
                true,
                compiled,
            )
            .await?;
        let parsed = Self::native_entity_mutation_result(&result)?;
        if require_affected && parsed.0 == 0 {
            return Err(native_entity_update_not_found_status());
        }
        Ok(parsed)
    }

    /// Execute several typed native-entity mutations in one selected Postgres
    /// transaction. This is the P6.7 substrate for native services that need
    /// atomic create/bind/audit or lifecycle transitions without using the raw
    /// GenericDispatch `transaction` RPC.
    #[allow(dead_code)]
    pub(crate) async fn native_entity_transaction_for_service(
        &self,
        service_id: &str,
        context: &crate::RequestContext,
        ops: Vec<NativeEntityTransactionOp>,
    ) -> Result<Vec<NativeEntityTransactionStepResult>, tonic::Status> {
        if ops.is_empty() {
            return Err(empty_native_entity_transaction_status());
        }

        let (target, kind) =
            self.native_entity_dispatch_target(service_id, &context.project_id, true)?;
        if kind != crate::backend::BackendKind::Postgres || target.backend != "postgres" {
            return Err(native_store_capability_status(
                target.backend.clone(),
                "typed_transaction",
                "postgres_native_transaction",
                format!(
                    "native typed transactions are currently implemented only for postgres, got '{}'",
                    target.backend
                ),
            ));
        }
        let compile_ctx = Self::native_entity_compile_context(context, target.instance.as_deref());
        let mut compiled_steps = Vec::with_capacity(ops.len());
        for op in ops {
            let compiled = match op {
                NativeEntityTransactionOp::Write(op) => {
                    crate::runtime::service::handlers_data::compile_logical_write_dispatch(
                        &kind,
                        &op,
                        &compile_ctx,
                    )?
                }
                NativeEntityTransactionOp::Update(op) => {
                    crate::runtime::service::handlers_data::compile_logical_update_dispatch(
                        &kind,
                        &op,
                        &compile_ctx,
                    )?
                }
                NativeEntityTransactionOp::Delete(op) => {
                    crate::runtime::service::handlers_data::compile_logical_delete_dispatch(
                        &kind,
                        &op,
                        &compile_ctx,
                    )?
                }
            };
            if compiled.operation != "mutate" {
                return Err(native_store_capability_status(
                    target.backend.clone(),
                    "typed_transaction_compile",
                    "native_entity_mutation_dispatch",
                    format!(
                        "native entity transaction compiled unsupported operation '{}'",
                        compiled.operation
                    ),
                ));
            }
            compiled_steps.push(compiled);
        }

        let pool = self
            .pg_pool_for_instance(target.instance.as_deref())?
            .clone();
        let mut tx = pool.begin().await.map_err(|err| {
            native_store_internal_status(
                "native_entity_transaction_start",
                format!("native entity transaction start failed: {err}"),
            )
        })?;
        crate::runtime::core::set_request_local_settings(&mut tx, context).await?;

        let mut results = Vec::with_capacity(compiled_steps.len());
        for compiled in compiled_steps {
            let spec: serde_json::Value =
                serde_json::from_str(&compiled.spec_json).map_err(|err| {
                    native_store_internal_status(
                        "compiled_native_mutation_json",
                        format!("compiled native mutation JSON failed: {err}"),
                    )
                })?;
            let sql = spec
                .get("sql")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    native_store_capability_status(
                        target.backend.clone(),
                        "typed_transaction_sql",
                        "postgres_sql_mutation",
                        "native entity transaction compiled a non-SQL mutation",
                    )
                })?;
            crate::runtime::core::validate_pg_mutation_sql(sql)?;
            let params = crate::runtime::core::dispatch_params(&spec)?;
            let param_types = crate::runtime::core::dispatch_param_types(&spec)?;
            let return_rows = spec
                .get("return_rows")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or_else(|| sql.to_ascii_lowercase().contains(" returning "));

            if return_rows {
                let rows = crate::runtime::core::bind_typed_generic_pg_params(
                    sqlx::query(sql),
                    &params,
                    param_types.as_deref(),
                )?
                .fetch_all(&mut *tx)
                .await
                .map_err(|err| {
                    native_store_internal_status(
                        "native_entity_transaction_mutation",
                        format!("native entity transaction mutation failed: {err}"),
                    )
                })?;
                let rows = crate::runtime::core::pg_rows_to_json(rows)?;
                results.push(NativeEntityTransactionStepResult {
                    affected_rows: rows.len() as u64,
                    rows,
                });
            } else {
                let result = crate::runtime::core::bind_typed_generic_pg_params(
                    sqlx::query(sql),
                    &params,
                    param_types.as_deref(),
                )?
                .execute(&mut *tx)
                .await
                .map_err(|err| {
                    native_store_internal_status(
                        "native_entity_transaction_mutation",
                        format!("native entity transaction mutation failed: {err}"),
                    )
                })?;
                results.push(NativeEntityTransactionStepResult {
                    affected_rows: result.rows_affected(),
                    rows: Vec::new(),
                });
            }
        }

        tx.commit().await.map_err(|err| {
            native_store_internal_status(
                "native_entity_transaction_commit",
                format!("native entity transaction commit failed: {err}"),
            )
        })?;
        Ok(results)
    }

    /// Count native entities matching `filter`, through the typed path on the bound
    /// backend. Reuses [`Self::native_entity_read_for_service`]; no IR-level aggregate
    /// yet, so it reads the matching rows — fine for per-tenant-bounded native tables,
    /// not for data-plane-scale sets (P4.D).
    #[allow(dead_code)]
    pub(crate) async fn native_entity_count_for_service(
        &self,
        service_id: &str,
        context: &crate::RequestContext,
        message_type: &str,
        filter: Option<LogicalFilter>,
    ) -> Result<i64, tonic::Status> {
        let op = LogicalAggregate {
            message_type: message_type.to_string(),
            filter,
            group_by: Vec::new(),
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Count,
                field: "*".to_string(),
                alias: "total_count".to_string(),
            }],
            having: None,
            sort: Vec::new(),
            pagination: None,
        };
        let rows = self
            .native_entity_aggregate_for_service(service_id, context, op)
            .await?;
        Ok(rows
            .first()
            .and_then(|row| row.get("total_count"))
            .and_then(|value| match value {
                serde_json::Value::Number(number) => number.as_i64(),
                serde_json::Value::String(value) => value.parse::<i64>().ok(),
                _ => None,
            })
            .unwrap_or(0))
    }

    /// Sum an integer `field` across native entities matching `filter` (per-tenant
    /// quota checks, e.g. total stored bytes).
    #[allow(dead_code)]
    pub(crate) async fn native_entity_sum_i64_for_service(
        &self,
        service_id: &str,
        context: &crate::RequestContext,
        message_type: &str,
        filter: Option<LogicalFilter>,
        field: &str,
    ) -> Result<i64, tonic::Status> {
        let op = LogicalAggregate {
            message_type: message_type.to_string(),
            filter,
            group_by: Vec::new(),
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                field: field.to_string(),
                alias: "total_sum".to_string(),
            }],
            having: None,
            sort: Vec::new(),
            pagination: None,
        };
        let rows = self
            .native_entity_aggregate_for_service(service_id, context, op)
            .await?;
        Ok(rows
            .first()
            .and_then(|row| row.get("total_sum"))
            .and_then(|value| match value {
                serde_json::Value::Number(number) => number.as_i64(),
                serde_json::Value::String(value) => value.parse::<i64>().ok(),
                _ => None,
            })
            .unwrap_or(0))
    }

    /// Acquire a portable per-tenant advisory lease on the default canonical store —
    /// the backend-neutral `udb_advisory_leases` serialization primitive the recovery
    /// workers already use — so a native service can serialize a quota check-then-write
    /// without a cross-backend transaction. `Ok(true)` on acquisition; best-effort
    /// `Ok(true)` when no canonical store is registered (proceed unserialized rather
    /// than fail the RPC).
    #[allow(dead_code)]
    pub(crate) async fn try_acquire_native_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        ttl: std::time::Duration,
    ) -> Result<bool, tonic::Status> {
        let Some(store) = self.default_system_stores() else {
            return Ok(true);
        };
        store.ensure_advisory_lease_table().await.map_err(|e| {
            native_store_internal_status(
                "native_advisory_lease",
                format!("native advisory lease table ensure failed: {e}"),
            )
        })?;
        store
            .try_acquire_advisory_lease(lease_name, owner_id, ttl)
            .await
            .map_err(|e| {
                native_store_internal_status(
                    "native_advisory_lease",
                    format!("native advisory lease failed: {e}"),
                )
            })
    }

    /// Release a native advisory lease from [`Self::try_acquire_native_lease`].
    #[allow(dead_code)]
    pub(crate) async fn release_native_lease(&self, lease_name: &str, owner_id: &str) {
        if let Some(store) = self.default_system_stores() {
            let _ = store.release_advisory_lease(lease_name, owner_id).await;
        }
    }

    /// Execute a typed native-entity delete through the canonical compiler/executor
    /// path. The [`LogicalDelete`] contract requires a filter, so native services
    /// cannot accidentally issue an unbounded backend delete.
    #[allow(dead_code)]
    pub(crate) async fn native_entity_delete_for_service(
        &self,
        service_id: &str,
        context: &crate::RequestContext,
        op: LogicalDelete,
    ) -> Result<String, tonic::Status> {
        let (target, kind) =
            self.native_entity_dispatch_target(service_id, &context.project_id, true)?;
        let compile_ctx = Self::native_entity_compile_context(context, target.instance.as_deref());
        let compiled = crate::runtime::service::handlers_data::compile_logical_delete_dispatch(
            &kind,
            &op,
            &compile_ctx,
        )?;
        self.execute_native_entity_dispatch(
            context,
            &target.backend,
            target.instance.as_deref(),
            true,
            compiled,
        )
        .await
    }

    /// Execute a typed native-entity delete and parse `RETURNING`/row-returning
    /// mutation output.
    #[allow(dead_code)]
    pub(crate) async fn native_entity_delete_rows_for_service(
        &self,
        service_id: &str,
        context: &crate::RequestContext,
        op: LogicalDelete,
    ) -> Result<Vec<serde_json::Value>, tonic::Status> {
        let result = self
            .native_entity_delete_for_service(service_id, context, op)
            .await?;
        Self::native_entity_rows(&result)
    }

    /// Resolve the Postgres pool a native control-plane service should persist to,
    /// chosen dynamically from the configured instances (health- and weight-aware,
    /// circuit-breaker-filtered) instead of the process-global primary pool. The
    /// backend is resolved from the service's proto-declared `native_service` binding
    /// (P1 per-service authority), so a service annotated for a different canonical
    /// store routes there with no wiring change. Pass `write = false` to prefer a read
    /// replica; `project_id` may be empty at build-time acquisition.
    ///
    /// P0: Postgres-only — fails closed for any other backend rather than silently
    /// persisting to the wrong/absent one. Falls back to the default pool when no
    /// multi-instance routing applies, so single-primary deployments see no change.
    pub(crate) fn native_store_pool_for_service(
        &self,
        service_id: &str,
        write: bool,
        project_id: &str,
    ) -> Result<PgPool, tonic::Status> {
        let store = self.native_entity_store_for_service(service_id, write, project_id)?;
        store.pg_pool().cloned().ok_or_else(|| {
            native_store_capability_status(
                store.backend().to_string(),
                "pool_lookup",
                "postgres_pool",
                format!(
                    "native store backend '{}' exposes no Postgres pool (neutral-operation \
                 migration pending); see extend_udb.md",
                    store.backend()
                ),
            )
        })
    }

    /// Resolve the backend-neutral [`NativeEntityStore`] for a native service: the
    /// backend comes from the service's proto `native_service` binding, the instance
    /// from health/weight-aware selection. P1: Postgres reference impl; fails closed
    /// (typed `failed_precondition`) for backends whose store impl is not yet wired.
    pub(crate) fn native_entity_store_for_service(
        &self,
        service_id: &str,
        write: bool,
        project_id: &str,
    ) -> Result<std::sync::Arc<dyn NativeEntityStore>, tonic::Status> {
        use crate::runtime::service::native_entity_store as nes;
        match native_store_backend_for_service(service_id, project_id) {
            "postgres" => {
                let instance = self.choose_instance_name_for_project("postgres", write, project_id);
                let pool = self.pg_pool_for_instance(instance).cloned()?;
                Ok(nes::PostgresNativeEntityStore::new(pool))
            }
            #[cfg(feature = "sqlite")]
            "sqlite" => {
                let instance = self
                    .choose_instance_name_for_project("sqlite", write, project_id)
                    .unwrap_or("primary");
                let pool = self
                    .sqlite_pool_for_instance(instance)
                    .cloned()
                    .ok_or_else(|| {
                        native_store_capability_status(
                            "sqlite",
                            "store_lookup",
                            "sqlite_native_store",
                            "sqlite native store is not configured",
                        )
                    })?;
                Ok(nes::SqliteNativeEntityStore::new(pool))
            }
            #[cfg(feature = "mysql")]
            "mysql" => {
                let instance = self
                    .choose_instance_name_for_project("mysql", write, project_id)
                    .unwrap_or("primary");
                let pool = self
                    .mysql_pool_for_instance(instance)
                    .cloned()
                    .ok_or_else(|| {
                        native_store_capability_status(
                            "mysql",
                            "store_lookup",
                            "mysql_native_store",
                            "mysql native store is not configured",
                        )
                    })?;
                Ok(nes::MySqlNativeEntityStore::new(pool))
            }
            #[cfg(feature = "neo4j")]
            "neo4j" => {
                let instance = self.choose_instance_name_for_project("neo4j", write, project_id);
                let executor = self.neo4j_for_instance(instance)?.clone();
                Ok(nes::Neo4jNativeEntityStore::new(executor))
            }
            #[cfg(feature = "mongodb")]
            "mongodb" => {
                let instance = self.choose_instance_name_for_project("mongodb", write, project_id);
                let executor = self.mongodb_for_instance(instance)?.clone();
                Ok(nes::MongoDbNativeEntityStore::new(executor))
            }
            other => Err(native_store_capability_status(
                other.to_string(),
                "store_lookup",
                "native_entity_store",
                format!(
                    "native-service persistence backend '{other}' is not implemented; see extend_udb.md"
                ),
            )),
        }
    }

    /// Self-check that native persistence is actually writable+readable on the backend
    /// the service's proto binding resolves to (capability honesty, directive #4): a
    /// real KV round-trip on a throwaway key. Returns the resolved backend label on
    /// success. Used by the native-store health diagnostic and the live conformance
    /// test, so the multi-backend persistence path is exercised on the served path.
    pub(crate) async fn native_store_self_check(
        &self,
        service_id: &str,
        project_id: &str,
    ) -> Result<&'static str, tonic::Status> {
        let store = self.native_entity_store_for_service(service_id, true, project_id)?;
        let key = format!("__native_store_self_check__{service_id}");
        crate::runtime::service::native_entity_store::kv_roundtrip_probe(
            store.as_ref(),
            &key,
            "ok",
        )
        .await?;
        Ok(store.backend())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        native_entity_update_not_found_status, native_store_capability_status,
        native_store_internal_status, normalize_store_override, parse_project_overrides,
    };
    use crate::backend::BackendKind;
    use crate::ir::{
        ComparisonOp, ConflictStrategy, LogicalDelete, LogicalFilter, LogicalPagination,
        LogicalProjection, LogicalRead, LogicalRecord, LogicalValue, LogicalWrite,
    };
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use prost::Message as _;

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

    fn assert_capability_detail(
        status: &tonic::Status,
        backend: &str,
        operation: &str,
        capability_required: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, backend);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability_required);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_schema_detail(
        status: &tonic::Status,
        backend: &str,
        operation: &str,
        schema_code: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::NotFound);
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

    fn assert_internal_detail(status: &tonic::Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "native_store");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn native_entity_update_miss_carries_schema_detail() {
        assert_schema_detail(
            &native_entity_update_not_found_status(),
            "native_entity",
            "native_entity_update",
            "native_entity_update_not_found",
            "native entity update affected no rows",
        );
    }

    #[test]
    fn native_store_internal_status_carries_typed_detail() {
        let status = native_store_internal_status(
            "native_entity_rows_decode",
            "native entity JSON decode failed: expected value",
        );
        assert_internal_detail(
            &status,
            "native_entity_rows_decode",
            "native entity JSON decode failed: expected value",
        );

        let status = native_store_internal_status(
            "native_entity_transaction_commit",
            "native entity transaction commit failed: closed",
        );
        assert_internal_detail(
            &status,
            "native_entity_transaction_commit",
            "native entity transaction commit failed: closed",
        );
    }

    #[test]
    fn native_store_missing_capabilities_carry_typed_detail() {
        for (backend, operation, capability_required, message) in [
            (
                "mysql",
                "store_lookup",
                "mysql_native_store",
                "mysql native store is not configured",
            ),
            (
                "sqlite",
                "store_lookup",
                "sqlite_native_store",
                "sqlite native store is not configured",
            ),
            (
                "neo4j",
                "pool_lookup",
                "postgres_pool",
                "native store backend 'neo4j' exposes no Postgres pool (neutral-operation migration pending); see extend_udb.md",
            ),
            (
                "vault",
                "store_lookup",
                "native_entity_store",
                "native-service persistence backend 'vault' is not implemented; see extend_udb.md",
            ),
            (
                "postgres",
                "typed_transaction_sql",
                "postgres_sql_mutation",
                "native entity transaction compiled a non-SQL mutation",
            ),
        ] {
            let status =
                native_store_capability_status(backend, operation, capability_required, message);
            assert_capability_detail(&status, backend, operation, capability_required, message);
        }
    }

    #[test]
    fn project_overrides_parse_trim_lowercase_and_drop_malformed() {
        let map = parse_project_overrides(Some(" projA = MySQL , projB=neo4j ,,bad,=x,y= "));
        assert_eq!(map.get("projA").map(String::as_str), Some("mysql"));
        assert_eq!(map.get("projB").map(String::as_str), Some("neo4j"));
        assert_eq!(map.len(), 2); // "bad" (no =), "=x" (blank key), "y=" (blank val) dropped
        assert!(parse_project_overrides(None).is_empty());
    }

    #[test]
    fn store_override_trims_lowercases_and_drops_blank() {
        // Unset / blank → no override (proto authority applies).
        assert_eq!(normalize_store_override(None), None);
        assert_eq!(normalize_store_override(Some("")), None);
        assert_eq!(normalize_store_override(Some("   ")), None);
        // Set → normalized override.
        assert_eq!(
            normalize_store_override(Some("postgres")),
            Some("postgres".to_string())
        );
        assert_eq!(
            normalize_store_override(Some(" Postgres ")),
            Some("postgres".to_string())
        );
        assert_eq!(
            normalize_store_override(Some("MySQL")),
            Some("mysql".to_string())
        );
    }

    #[tokio::test]
    async fn empty_native_entity_transaction_carries_field_violation() {
        let runtime = super::DataBrokerRuntime::planning_only();
        let context = crate::RequestContext::default();
        let ops: Vec<super::NativeEntityTransactionOp> = Vec::new();

        let status = runtime
            .native_entity_transaction_for_service("udb.core.test", &context, ops)
            .await
            .expect_err("empty transaction must fail before dispatch");

        assert_eq!(
            status.message(),
            "native entity transaction requires at least one operation"
        );
        assert_single_field_violation(&status, "ops", "must contain at least one operation");
    }

    #[test]
    fn tenant_config_native_entity_ir_compiles_to_dispatch() {
        const MSG: &str = "udb.core.tenant.entity.v1.TenantConfig";

        let context = crate::RequestContext {
            tenant_id: "11111111-1111-4111-8111-111111111111".to_string(),
            project_id: "project-a".to_string(),
            ..crate::RequestContext::default()
        };
        let compile_ctx =
            super::DataBrokerRuntime::native_entity_compile_context(&context, Some("primary"));
        let read = LogicalRead {
            message_type: MSG.to_string(),
            filter: Some(LogicalFilter::Comparison {
                field: "tenant_id".to_string(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String(context.tenant_id.clone()),
            }),
            projection: Some(LogicalProjection::fields([
                "id".to_string(),
                "tenant_id".to_string(),
                "config_key".to_string(),
                "config_value".to_string(),
                "type".to_string(),
                "description".to_string(),
            ])),
            sort: Vec::new(),
            include: Vec::new(),
            pagination: Some(LogicalPagination::limit(10)),
        };
        let read_dispatch = crate::runtime::service::handlers_data::compile_logical_read_dispatch(
            &BackendKind::Postgres,
            &read,
            &compile_ctx,
        )
        .expect("native TenantConfig read should compile");
        assert_eq!(read_dispatch.operation, "query");
        assert!(read_dispatch.spec_json.contains("tenant_configs"));
        assert!(read_dispatch.spec_json.contains("compiler_mediated"));

        let mut record = LogicalRecord::new();
        record.insert(
            "id".to_string(),
            LogicalValue::String("22222222-2222-4222-8222-222222222222".to_string()),
        );
        record.insert(
            "tenant_id".to_string(),
            LogicalValue::String(context.tenant_id.clone()),
        );
        record.insert(
            "config_key".to_string(),
            LogicalValue::String("theme".into()),
        );
        record.insert(
            "config_value".to_string(),
            LogicalValue::String("dark".into()),
        );
        record.insert("type".to_string(), LogicalValue::String("STRING".into()));
        record.insert(
            "description".to_string(),
            LogicalValue::String(String::new()),
        );
        let write = LogicalWrite {
            message_type: MSG.to_string(),
            records: vec![record],
            conflict: ConflictStrategy::update(vec![
                "tenant_id".to_string(),
                "config_key".to_string(),
                "config_value".to_string(),
                "type".to_string(),
                "description".to_string(),
            ]),
            return_fields: Vec::new(),
        };
        let write_dispatch =
            crate::runtime::service::handlers_data::compile_logical_write_dispatch(
                &BackendKind::Postgres,
                &write,
                &compile_ctx,
            )
            .expect("native TenantConfig write should compile");
        assert_eq!(write_dispatch.operation, "mutate");
        assert!(write_dispatch.spec_json.contains("tenant_configs"));
        assert!(write_dispatch.spec_json.contains("compiler_mediated"));

        let delete = LogicalDelete {
            message_type: MSG.to_string(),
            filter: LogicalFilter::Comparison {
                field: "id".to_string(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("22222222-2222-4222-8222-222222222222".to_string()),
            },
            return_fields: Vec::new(),
        };
        let delete_dispatch =
            crate::runtime::service::handlers_data::compile_logical_delete_dispatch(
                &BackendKind::Postgres,
                &delete,
                &compile_ctx,
            )
            .expect("native TenantConfig delete should compile");
        assert_eq!(delete_dispatch.operation, "mutate");
        assert!(delete_dispatch.spec_json.contains("tenant_configs"));
        assert!(delete_dispatch.spec_json.contains("compiler_mediated"));
    }
}
