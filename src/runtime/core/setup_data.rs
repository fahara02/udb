//! Continuation `impl DataBrokerRuntime` block (Phase F split of core.rs).
//!
//! Also home to the per-backend `register_*` functions invoked by
//! `backend::plugins::*` via the U2 plugin loop. These live here (not in
//! `backend/`) so they have descendant-module access to `DataBrokerRuntime`'s
//! private `pg_*`/`redis`/`qdrant_*`/etc. fields (§9.5).
use super::*;
use crate::backend::plugin::RegisterCtx;

fn setup_data_invalid_field(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [(field.into(), description.into())],
    )
}

fn unknown_message_type_status() -> tonic::Status {
    setup_data_invalid_field(
        "message_type",
        "must match a manifest table message type",
        "unknown message_type",
    )
}

/// fix_plan §4.1: manifest-aware lookup failure — an AMBIGUOUS convenience
/// identity (a short name shared by multiple catalog packages) names every
/// candidate FQN so the caller can qualify, instead of a bare "unknown".
fn message_type_lookup_status(
    manifest: &crate::generation::CatalogManifest,
    message_type: &str,
) -> tonic::Status {
    setup_data_invalid_field(
        "message_type",
        "must match exactly one manifest table message type",
        crate::planning::broker::describe_table_lookup_miss(manifest, message_type),
    )
}

/// GO-005: value equality for a compare-and-swap assertion that treats an
/// integer and its float form as equal. A `google.protobuf.Struct` carries every
/// number as an f64, so a JSON `8` decoded from an INTEGER column must still
/// match an asserted `8.0` — the same int-vs-float trap the chunk-seq round trip
/// hit. Non-numeric values fall back to exact structural equality.
fn json_values_match(have: &JsonValue, want: &JsonValue) -> bool {
    match (have, want) {
        (JsonValue::Number(a), JsonValue::Number(b)) => match (a.as_f64(), b.as_f64()) {
            (Some(x), Some(y)) => x == y,
            _ => a == b,
        },
        _ => have == want,
    }
}

fn empty_object_stream_status() -> tonic::Status {
    setup_data_invalid_field(
        "stream",
        "object upload stream must contain at least one chunk",
        "empty object stream",
    )
}

fn unsupported_presign_method_status() -> tonic::Status {
    setup_data_invalid_field(
        "method",
        "presigned URLs support only PUT or GET",
        "presigned URLs support only PUT or GET",
    )
}

fn invalid_part_count_status() -> tonic::Status {
    setup_data_invalid_field(
        "part_count",
        "multipart upload part_count must be positive",
        "part_count must be positive",
    )
}

fn invalid_presign_ttl_status(err: impl std::fmt::Display) -> tonic::Status {
    setup_data_invalid_field(
        "ttl_seconds",
        "must produce a valid presign expiration",
        format!("invalid presign ttl: {err}"),
    )
}

fn setup_data_capability_status(
    backend: impl Into<String>,
    operation: impl Into<String>,
    capability_required: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::capability_status(
        backend,
        operation,
        capability_required,
        message,
    )
}

fn setup_data_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status("setup_data", operation, message)
}

// Serving callers live in the `#[cfg(not(feature = "qdrant"))]` vector arms;
// the unconditional pin test `setup_data_vector_object_capability_refusals_
// carry_detail` keeps the disabled-path ErrorDetail contract alive even in
// qdrant-on builds — hence allow(dead_code) only there, so a qdrant-off build
// still detects rot if the serving arm disappears.
#[cfg_attr(feature = "qdrant", allow(dead_code))]
fn qdrant_vector_feature_status(operation: &'static str) -> tonic::Status {
    setup_data_capability_status(
        "qdrant",
        operation,
        "qdrant_feature",
        "qdrant/vector feature is not enabled",
    )
}

fn vector_hybrid_qdrant_only_status(backend: &str) -> tonic::Status {
    setup_data_capability_status(
        "qdrant",
        "vector_hybrid_search",
        "qdrant_backend",
        format!("vector hybrid search is only wired for qdrant, not '{backend}'"),
    )
}

// Serving callers live in the `#[cfg(not(any(s3|gcs|azureblob)))]` object
// arms; the pin test keeps the contract alive in object-enabled builds.
#[cfg_attr(
    any(feature = "s3", feature = "gcs", feature = "azureblob"),
    allow(dead_code)
)]
fn no_object_store_feature_status(operation: &'static str) -> tonic::Status {
    setup_data_capability_status(
        "object_store",
        operation,
        "object_store_feature",
        "no object-store feature (s3/gcs/azureblob) is enabled",
    )
}

// Serving callers live in `#[cfg(not(feature = "s3"))]` arms; pin-tested.
#[cfg_attr(feature = "s3", allow(dead_code))]
fn s3_object_feature_status(operation: &'static str) -> tonic::Status {
    setup_data_capability_status(
        "s3",
        operation,
        "s3_feature",
        "s3/object-store feature is not enabled",
    )
}

// Only reachable from the `#[cfg(not(feature = "s3"))]` arms of the object
// put/get dispatch — gate the definition the same way so the default (s3-on)
// build doesn't carry a dead fn.
#[cfg(not(feature = "s3"))]
fn s3_minio_feature_status(operation: &'static str) -> tonic::Status {
    setup_data_capability_status(
        "s3",
        operation,
        "s3_feature",
        "s3/minio feature is not enabled",
    )
}

// Serving callers live in `#[cfg(not(feature = "gcs"))]` arms; pin-tested.
#[cfg_attr(feature = "gcs", allow(dead_code))]
fn gcs_feature_status(operation: &'static str) -> tonic::Status {
    setup_data_capability_status(
        "gcs",
        operation,
        "gcs_feature",
        "gcs feature is not enabled",
    )
}

// Only reachable from the `#[cfg(not(feature = "azureblob"))]` object arms and
// (unlike its siblings) not pin-tested — gate it exactly like its callers.
#[cfg(not(feature = "azureblob"))]
fn azureblob_feature_status(operation: &'static str) -> tonic::Status {
    setup_data_capability_status(
        "azureblob",
        operation,
        "azureblob_feature",
        "azureblob feature is not enabled",
    )
}

fn object_instance_missing_status(
    backend: &'static str,
    operation: &'static str,
    instance: &str,
) -> tonic::Status {
    setup_data_capability_status(
        backend,
        operation,
        "configured_instance",
        format!("{backend} instance '{instance}' is not configured"),
    )
}

fn unsupported_object_backend_status(operation: &'static str, backend: &str) -> tonic::Status {
    setup_data_capability_status(
        backend,
        operation,
        "supported_object_backend",
        format!("unsupported object backend '{backend}'"),
    )
}

#[cfg(any(feature = "s3", feature = "gcs", feature = "azureblob"))]
fn typed_object_backend_required_status(backend: &str) -> tonic::Status {
    setup_data_capability_status(
        backend,
        "typed_object_rpc",
        "object_store_backend",
        format!(
            "typed object RPCs require an object-store backend, but the \
             store is configured for '{backend}'"
        ),
    )
}

impl DataBrokerRuntime {
    pub(crate) fn record_vector_resource_backend(
        &self,
        project_id: &str,
        collection: &str,
        backend: &str,
        instance: Option<&str>,
    ) {
        let key = vector_route_key(project_id, collection);
        let route = ResolvedBackendSelector {
            backend: backend.to_ascii_lowercase(),
            instance: instance.map(str::to_string),
        };
        if let Ok(mut routes) = self.vector_resource_routes.lock() {
            routes.insert(key, route);
        }
    }

    fn vector_resource_backend(
        &self,
        manifest: &CatalogManifest,
        project_id: &str,
        collection: &str,
    ) -> Option<ResolvedBackendSelector> {
        let key = vector_route_key(project_id, collection);
        if let Ok(routes) = self.vector_resource_routes.lock()
            && let Some(route) = routes.get(&key)
        {
            return Some(route.clone());
        }
        manifest
            .stores
            .iter()
            .find(|store| store.store_kind == "vector" && store.resource_name == collection)
            .map(|store| ResolvedBackendSelector {
                backend: store.backend.to_ascii_lowercase(),
                instance: None,
            })
    }

    pub async fn try_from_config(config: UdbConfig) -> Result<Self, String> {
        let validation = config.validate();
        if !validation.passed {
            return Err(format!(
                "UDB config validation failed: {}",
                validation.errors.join("; ")
            ));
        }
        Ok(Self::from_config_unchecked(config).await)
    }

    pub async fn from_config(config: UdbConfig) -> Self {
        Self::from_config_unchecked(config).await
    }

    async fn from_config_unchecked(config: UdbConfig) -> Self {
        let mut runtime = Self {
            channels: crate::runtime::channels::ChannelManager::from_settings(&config.channels),
            config: config.clone(),
            ..Self::default()
        };
        let mut report = RuntimeInitReport::default();
        let mut instance_config = effective_backend_instance_config(&config);
        merge_runtime_env_backend_instances(&mut instance_config);
        let app_name = effective_app_name(&config);

        crate::runtime::cdc::CdcConfig::install_global(config.cdc.clone());
        crate::runtime::security::SecurityConfig::install_global(config.security.clone());
        crate::runtime::native_catalog::install_native_services_settings(
            config.native_services.clone(),
        );
        if config.security.allow_header_scopes {
            tracing::warn!(
                "UDB_ALLOW_HEADER_SCOPES is enabled: request scopes are trusted from the \
                 x-scopes header. This is a DEV-ONLY fallback and is rejected by production \
                 validation — do not enable it in production."
            );
        }
        crate::runtime::system::SystemCatalogConfig::install_global(
            crate::runtime::system::SystemCatalogConfig::from_udb_config(&config),
        );

        // U2 step 3: each compiled backend plugin owns its default and named
        // instance registration. `from_config` only drives the inventory.
        {
            let mut ctx = crate::backend::plugin::RegisterCtx {
                config: &config,
                instance_config: &instance_config,
                app_name: &app_name,
                runtime: &mut runtime,
                report: &mut report,
            };
            // Startup probe budget: a configured-but-unreachable / pathologically
            // slow driver connect (e.g. MongoDB's 30s server-selection, a SQL pool
            // acquire-timeout) must not SERIALIZE the whole startup. Bound each
            // backend's registration; on timeout the backend degrades to
            // "unavailable" and the broker keeps going (same fail-open posture as
            // the panic guard below). Override with UDB_BACKEND_STARTUP_PROBE_SECS
            // (0/unset → default). This is why a multi-backend deploy with one slow
            // backend no longer pays that backend's full driver timeout at boot.
            let probe_budget = std::time::Duration::from_secs(
                std::env::var("UDB_BACKEND_STARTUP_PROBE_SECS")
                    .ok()
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .filter(|s| *s > 0)
                    .unwrap_or(8),
            );
            // The MANDATORY primary Postgres gets its own, larger floor: it is not
            // an optional backend that may silently degrade — the startup health
            // gate hard-exits (container crash-loop) when its pool is missing. A
            // reachable managed PG (Neon/Supabase serverless cold start + eager
            // min_connections) measured ~16s to register, well over the 8s
            // optional-backend probe, so the short cap would strand it. Floor the
            // primary's budget at DEFAULT_PG_STARTUP_PROBE_SECS; override with
            // UDB_PG_STARTUP_PROBE_SECS, and never go below the general probe.
            const DEFAULT_PG_STARTUP_PROBE_SECS: u64 = 120;
            let pg_probe_budget = std::time::Duration::from_secs(
                std::env::var("UDB_PG_STARTUP_PROBE_SECS")
                    .ok()
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .filter(|s| *s > 0)
                    .unwrap_or(DEFAULT_PG_STARTUP_PROBE_SECS),
            )
            .max(probe_budget);
            for plugin in crate::backend::all_plugins() {
                // C4 (bug_report.md): a backend's startup registration — driver
                // connect / metadata fetch to a configured-but-UNREACHABLE server
                // (MSSQL/MySQL/Cassandra/etc.) — must never abort the whole broker.
                // The register_* error handling already degrades on a returned
                // Err, but a driver that PANICS during connect would unwind past
                // here and take the process down. Isolate each plugin in
                // catch_unwind so a panic degrades to "backend unavailable" and the
                // broker still serves every reachable backend. (A non-unwinding
                // driver abort/OOM is not catchable here and needs the driver-level
                // fix; this closes the panic vector.)
                use futures::FutureExt as _;
                let kind = plugin.kind();
                // Postgres is the mandatory primary the health gate depends on, so
                // it uses the larger pg_probe_budget (see above); the optional
                // backends keep the short probe_budget they were meant for.
                let budget = if kind == crate::backend::BackendKind::Postgres {
                    pg_probe_budget
                } else {
                    probe_budget
                };
                match tokio::time::timeout(
                    budget,
                    std::panic::AssertUnwindSafe(plugin.register(&mut ctx)).catch_unwind(),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => {
                        tracing::warn!(
                            backend = ?kind,
                            "backend registration panicked at startup; backend marked unavailable (broker continues)"
                        );
                        ctx.report.warnings.push(format!(
                            "backend {kind:?} registration panicked at startup; backend marked \
                             unavailable (broker continues)"
                        ));
                    }
                    Err(_elapsed) => {
                        // The slow driver future was just cancelled, so it never
                        // logged its own "unavailable" line — announce the bound
                        // here so a degraded backend is never silent.
                        tracing::warn!(
                            backend = ?kind,
                            probe_budget_secs = budget.as_secs(),
                            "backend registration exceeded the startup probe budget; backend marked \
                             unavailable (broker continues — fix connectivity or raise \
                             UDB_BACKEND_STARTUP_PROBE_SECS)"
                        );
                        ctx.report.warnings.push(format!(
                            "backend {kind:?} registration exceeded the {}s startup probe budget; \
                             backend marked unavailable (broker continues — fix connectivity or raise \
                             UDB_BACKEND_STARTUP_PROBE_SECS)",
                            budget.as_secs()
                        ));
                    }
                }
            }
        }
        // Store registration makes "first registered wins" the default SystemStores,
        // which is a non-Postgres store (e.g. redis:default) whenever a cache/other
        // backend registers before Postgres. The PG direct-outbox write path requires
        // the primary Postgres SystemStores to be the default so outbox write-receipts
        // read `outbox_max_seq` from the same store native services write to — promote
        // postgres:primary explicitly before the consistency guard asserts it.
        if runtime.pg_pool.is_some()
            && let Ok(mut stores) = runtime.canonical_stores.lock()
        {
            let _ = stores.set_default("postgres", "primary");
        }
        assert_pg_outbox_receipt_store_consistency(&runtime);

        // 3.5: enforce the operator-declared deployment-tier floor now that every
        // store has registered. Fail-closed at boot if any canonical store is
        // below the declared UDB_DEPLOYMENT_TIER.
        assert_deployment_tier_floor(&runtime);

        // S1: all store registration is done; record whether a FULL
        // canonical system store (saga / admin-audit / migration / projection
        // tables) actually registered as the default. If a relational write
        // backend is configured but no full store registered, `udb_system` was
        // not provisioned (`ensure_full_system_store_tables` failed) and the
        // saga/audit/admin RPCs would return `FAILED_PRECONDITION` at request
        // time. Surface it LOUDLY at boot + as a failing readiness fact
        // (`slo::build_readiness_facts`) instead of silently per-RPC.
        report.full_system_store_registered = runtime
            .canonical_stores
            .lock()
            .ok()
            .and_then(|stores| stores.default_full_store())
            .is_some();
        let relational_store_expected = report.postgres_configured
            || report.mysql_configured
            || report.sqlite_configured
            || report.mssql_configured;
        if relational_store_expected && !report.full_system_store_registered {
            tracing::error!(
                "no canonical system store registered despite a relational backend being \
                 configured — udb_system is likely not provisioned \
                 (ensure_full_system_store_tables failed). Saga/audit/admin RPCs will return \
                 FAILED_PRECONDITION until this is fixed."
            );
            report.warnings.push(
                "CRITICAL: no canonical system store registered (udb_system not provisioned?); \
                 saga/audit/admin RPCs return FAILED_PRECONDITION"
                    .to_string(),
            );
        }

        match EncryptionRuntime::from_settings(&config.encryption).await {
            Ok(Some(encryption)) => {
                report.encryption_configured = true;
                runtime.encryption = Some(encryption);
            }
            Ok(None) => {}
            Err(err) => report
                .warnings
                .push(format!("field-level encryption disabled: {err}")),
        }

        let mut runtime_instances = runtime_backend_instances(&instance_config, &report, &runtime);
        reconcile_dispatch_factories(&mut runtime_instances, &runtime, &mut report.warnings);
        report.backend_instances = runtime_instances.clone();
        runtime.executor_registry = build_executor_registry(&runtime_instances);
        runtime.backend_instances = runtime_instances;
        runtime.report = report;
        runtime
    }

    pub async fn try_from_env() -> Result<Self, String> {
        Self::try_from_config(UdbConfig::from_merged_env()).await
    }

    pub async fn from_env() -> Self {
        Self::from_config(UdbConfig::from_merged_env()).await
    }

    pub async fn select(
        &self,
        manifest: &CatalogManifest,
        request: SelectRequest,
        metadata_context: RequestContext,
    ) -> Result<
        (
            RecordSet,
            Option<crate::runtime::consistency::StaleReadWarning>,
        ),
        tonic::Status,
    > {
        let mut request = request;
        // GAP 16: Prevent unbounded SELECT queries that return millions of rows.
        let default_limit = if self.config.default_limit > 0 {
            self.config.default_limit
        } else {
            100
        };
        let max_limit = if self.config.max_limit > 0 {
            self.config.max_limit
        } else {
            1000
        };
        if request.limit <= 0 {
            request.limit = default_limit;
        } else if request.limit > max_limit {
            request.limit = max_limit;
        }

        let context = merge_context(request.context.as_ref(), metadata_context);
        let filter = request
            .filter
            .as_ref()
            .map(struct_to_json)
            .unwrap_or(JsonValue::Null);
        if is_join_fusion_message_type(&request.message_type) {
            return self
                .select_join_fusion(manifest, request, context, filter)
                .await;
        }
        let sort = request
            .sort
            .iter()
            .map(|sort| SortSpec {
                field: sort.field.clone(),
                descending: sort.descending,
            })
            .collect::<Vec<_>>();
        let plan_request = SelectPlanRequest {
            context: context.clone(),
            message_type: request.message_type.clone(),
            filter: filter.clone(),
            fields: request.fields.clone(),
            limit: request.limit,
            sort,
        };
        let plan = build_select_query_plan(manifest, &plan_request);
        reject_plan(&plan.errors)?;
        let bypass_read = request
            .cache
            .as_ref()
            .map(|cache| cache.bypass_read)
            .unwrap_or(false);
        let bypass_write = request
            .cache
            .as_ref()
            .map(|cache| cache.bypass_write)
            .unwrap_or(false);
        // Only compute the read cache key (filter JSON serialize + SHA256) when the
        // cache will actually be consulted (read) or populated (write). When both are
        // bypassed the key is pure waste, so skip the hashing entirely.
        let cache_key = if bypass_read && bypass_write {
            None
        } else {
            Some(cache_key(
                "select",
                &request.message_type,
                &context,
                &manifest.checksum_sha256,
                &filter,
                &request.fields,
            ))
        };
        if !bypass_read
            && let Some(cache_key) = cache_key.as_deref()
            && let Some(cached) = self
                .cache_get_fresh(cache_key, &manifest.checksum_sha256, &context)
                .await
        {
            return Ok((cached_record_set(cached), None));
        }

        let table = resolve_table_for_message(manifest, &request.message_type)
            .map_err(|_| message_type_lookup_status(manifest, &request.message_type))?;
        let routed_pool = self
            .pg_select_pool_for_table_routed(table, &context)
            .await?;
        let routed_warning = routed_pool.warning().cloned();
        let pool = routed_pool.pool();
        // 03.2.1.2: capture the typed stale-read warning side-channel (the proto
        // `RecordSet` cannot carry it) so the handler can emit the response header.
        let fence_warning = self
            .enforce_read_fence(
                &context,
                "postgres",
                if context.target_instance.trim().is_empty() {
                    "selected"
                } else {
                    context.target_instance.trim()
                },
            )
            .await?;
        let stale_warning = routed_warning.or(fence_warning);
        // READ fast-path: a read-only SELECT does NOT need a transaction. We
        // acquire ONE pooled connection, install the RLS context as SESSION
        // settings (is_local=false) on it, run the SELECT on that SAME
        // connection, then ALWAYS reset those session GUCs before the
        // connection returns to the pool — on BOTH the success and error path.
        // This drops the BEGIN+COMMIT round-trips while keeping RLS isolation
        // byte-identical (same keys/values as the write path).
        let mut conn = pool.acquire().await.map_err(|e| {
            setup_data_internal_status(
                "select_connection_acquire",
                format!("PG connection acquire failed: {e}"),
            )
        })?;
        set_request_local_settings_conn(&mut conn, &context).await?;
        // 2.4 merge: prefer the bridged neutral-IR emission (live row parity is
        // pinned by the planner/IR A-B oracle); the planner SQL stays as the
        // fallback for planner-only filter shapes neutral IR cannot represent.
        let bridged = bridged_pg_select_statement(manifest, &plan_request);
        let values = filter_bind_values(&filter);
        let query = match bridged.as_ref() {
            Some(stmt) => bind_typed_generic_pg_params(
                sqlx::query(&stmt.sql),
                &stmt.params,
                Some(&stmt.param_types),
            )?,
            None => bind_values(
                sqlx::query(&plan.sql),
                table,
                &plan.parameter_columns,
                &values,
            )?,
        };
        // Capture the SELECT result WITHOUT early-`?`-returning, so the reset
        // below runs unconditionally even on query failure (leak-safety).
        let rows_result = query.fetch_all(&mut *conn).await.map_err(|err| {
            setup_data_internal_status("select_query", format!("PostgreSQL select failed: {err}"))
        });
        let reset_result = reset_request_local_settings_conn(&mut conn, &context).await;
        // Leak-safety teardown: if the RESET succeeded the connection is clean
        // and may recycle into the pool (plain drop). If the RESET FAILED the
        // connection may still carry this request's tenant GUCs, so we MUST NOT
        // hand it back clean — `detach()` removes it from pool accounting and
        // dropping the detached connection closes the underlying socket instead
        // of recycling a dirty session. (Defense in depth only — every path
        // re-applies its own context before querying.)
        if reset_result.is_ok() {
            drop(conn);
        } else {
            drop(conn.detach());
        }
        let rows = rows_result?;
        reset_result?;
        let record_set = rows_to_record_set(
            rows,
            Some(table),
            &plan.masked_columns,
            &context,
            self.encryption.as_ref(),
            &self.encryption_metrics,
        )?;
        if !bypass_write && let Some(cache_key) = cache_key.as_deref() {
            let ttl = request
                .cache
                .as_ref()
                .map(|cache| cache.ttl_seconds)
                .filter(|ttl| *ttl > 0)
                .unwrap_or(300) as u64;
            let _ = self
                .cache_set_stamped_from_pool(
                    cache_key,
                    &record_set.records_json,
                    ttl,
                    &manifest.checksum_sha256,
                    &pool,
                )
                .await;
        }
        Ok((record_set, stale_warning))
    }

    pub(crate) async fn select_join_fusion(
        &self,
        manifest: &CatalogManifest,
        request: SelectRequest,
        context: RequestContext,
        filter: JsonValue,
    ) -> Result<
        (
            RecordSet,
            Option<crate::runtime::consistency::StaleReadWarning>,
        ),
        tonic::Status,
    > {
        let plan = build_join_fusion_sql(manifest, &request, &context, &filter)?;
        let mut query = sqlx::query(&plan.sql);
        for (column, value) in &plan.bindings {
            query = bind_one(query, Some(column), value)?;
        }
        let primary_required = context.requires_primary_read()
            || super::accessors::read_fence_requires_primary(&context);
        let routed_pool = self.pg_read_pool_routed(&context, primary_required).await?;
        let routed_warning = routed_pool.warning().cloned();
        let pool = routed_pool.pool();
        // 03.2.1.3: same typed stale-read warning side-channel as `select`.
        let fence_warning = self
            .enforce_read_fence(
                &context,
                "postgres",
                if context.target_instance.trim().is_empty() {
                    "selected"
                } else {
                    context.target_instance.trim()
                },
            )
            .await?;
        let stale_warning = routed_warning.or(fence_warning);
        // READ fast-path (see `select`): no transaction. Acquire one pooled
        // connection, install RLS context as SESSION settings, run the join
        // SELECT, then ALWAYS reset the session GUCs before the connection
        // returns to the pool — on success AND error.
        let mut conn = pool.acquire().await.map_err(|e| {
            setup_data_internal_status(
                "join_connection_acquire",
                format!("PG connection acquire failed: {e}"),
            )
        })?;
        set_request_local_settings_conn(&mut conn, &context).await?;
        // Capture the SELECT result WITHOUT early-`?`-returning so the reset
        // runs unconditionally even on query failure (leak-safety).
        let rows_result = query.fetch_all(&mut *conn).await.map_err(|err| {
            setup_data_internal_status(
                "join_query",
                format!("PostgreSQL join select failed: {err}"),
            )
        });
        let reset_result = reset_request_local_settings_conn(&mut conn, &context).await;
        // Recycle the connection only if it was cleaned; otherwise close it
        // (detach) so a dirty session is never handed to the next request.
        if reset_result.is_ok() {
            drop(conn);
        } else {
            drop(conn.detach());
        }
        let rows = rows_result?;
        reset_result?;
        let record_set = rows_to_record_set(
            rows,
            None,
            &[],
            &context,
            self.encryption.as_ref(),
            &self.encryption_metrics,
        )?;
        Ok((record_set, stale_warning))
    }

    pub async fn upsert(
        &self,
        manifest: &CatalogManifest,
        request: UpsertRequest,
        metadata_context: RequestContext,
    ) -> Result<MutationResponse, tonic::Status> {
        let context = merge_context(request.context.as_ref(), metadata_context);
        let record = upsert_record_json(&request)?;
        let plan_request = UpsertPlanRequest {
            context: context.clone(),
            message_type: request.message_type.clone(),
            record: record.clone(),
            conflict_fields: request.conflict_fields.clone(),
            return_record: request.return_record,
            bypass_cache_write: request
                .cache
                .as_ref()
                .map(|cache| cache.bypass_write)
                .unwrap_or(false),
        };
        let plan = build_upsert_plan(manifest, &plan_request);
        reject_plan(&plan.errors)?;
        let pool = self.pg_pool()?;
        let mut tx = pool.begin().await.map_err(|e| {
            setup_data_internal_status(
                "upsert_transaction_begin",
                format!("PG transaction begin failed: {e}"),
            )
        })?;
        set_request_local_settings(&mut tx, &context).await?;
        // KEYSTONE (lane 05): keyed-only durable dedup. Fires ONLY when an
        // idempotency_key is supplied — keyless writes (the hot-path majority)
        // take zero extra SQL and zero behavioral change. The claim runs in THIS
        // write tx, so a dedup-store failure aborts the whole write (fail-closed).
        let dedup_ctx = {
            let key = idempotency_key_for_dedup(&request.idempotency_key)?;
            if let Some(key) = key {
                let config = crate::runtime::system::SystemCatalogConfig::current();
                let dedup_key = idempotency_dedup_key(
                    &context.tenant_id,
                    &context.project_id,
                    &request.message_type,
                    "upsert",
                    key,
                );
                let claim = claim_idempotency_key_in_tx(
                    &mut tx,
                    &config,
                    &dedup_key,
                    &context.tenant_id,
                    &context.project_id,
                    &request.message_type,
                    "upsert",
                )
                .await?;
                if !claim.fresh {
                    // Replay: do NOT run the write. Drop the tx (rolls back the
                    // dedup re-read) and return the stored original response.
                    drop(tx);
                    return mutation_response_from_idempotency_json_for_claim(
                        &claim.prior_response_json,
                        &context.tenant_id,
                        &context.project_id,
                        &request.message_type,
                    );
                }
                Some(IdempotencyPersistContext {
                    config,
                    dedup_key,
                    tenant_id: context.tenant_id.clone(),
                    project_id: context.project_id.clone(),
                    message_type: request.message_type.clone(),
                    operation: "upsert",
                })
            } else {
                None
            }
        };
        let table = resolve_table_for_message(manifest, &request.message_type)
            .map_err(|_| message_type_lookup_status(manifest, &request.message_type))?;
        // #117: rewrite proto `field_name` record keys to physical `column_name`s
        // so encryption + binding (keyed by `plan.parameter_columns`, which the
        // planner already resolved) find each value.
        let record = crate::broker::normalize_record_keys(table, &record);
        // GO-005 (compare-and-swap): when the caller asserts an `expected`
        // column=value precondition, evaluate it in THIS write tx — after the
        // tenant/RLS GUCs are installed (line above) and holding a row lock — so
        // two racing writers with the same expected version cannot both commit.
        // A mismatch or absent row returns FAILED_PRECONDITION and mutates
        // nothing (no projection/CDC/outbox, since we return before the write).
        // No-op (zero extra SQL) when `expected` is unset — the hot path.
        self.enforce_upsert_precondition(&mut tx, table, &request, &record, &context)
            .await?;
        let encrypted_record = self.encrypt_record_for_table(table, &record)?;
        // 2.4 merge: lower the ALREADY key-normalized, encrypted record so the
        // compiled parameter values match what the planner path binds; the
        // planner SQL stays as the fallback for conflict/record shapes neutral
        // IR cannot represent (live row parity pinned by the A-B oracle).
        let bridged = bridged_pg_upsert_statement(
            manifest,
            &UpsertPlanRequest {
                record: encrypted_record.clone(),
                ..plan_request.clone()
            },
        );
        let values = record_values(&encrypted_record, &plan.parameter_columns)?;
        let query = match bridged.as_ref() {
            Some(stmt) => bind_typed_generic_pg_params(
                sqlx::query(&stmt.sql),
                &stmt.params,
                Some(&stmt.param_types),
            )?,
            None => bind_values(
                sqlx::query(&plan.sql),
                table,
                &plan.parameter_columns,
                &values,
            )?,
        };

        let (affected_rows, record_json) = if request.return_record {
            let row = query.fetch_optional(&mut *tx).await.map_err(|err| {
                tracing::error!(
                    sql = %plan.sql,
                    message_type = %request.message_type,
                    tenant_id = %context.tenant_id,
                    "PostgreSQL upsert statement failed"
                );
                crate::runtime::executor_utils::sqlx_error_to_status(
                    "PostgreSQL upsert failed",
                    &err,
                )
            })?;
            match row {
                Some(row) => {
                    let record_set = rows_to_record_set(
                        vec![row],
                        Some(table),
                        &[],
                        &context,
                        self.encryption.as_ref(),
                        &self.encryption_metrics,
                    )?;
                    (1, returned_record_json_or_status(&record_set.records_json)?)
                }
                None => (0, Vec::new()),
            }
        } else {
            let result = query.execute(&mut *tx).await.map_err(|err| {
                tracing::error!(
                    sql = %plan.sql,
                    message_type = %request.message_type,
                    tenant_id = %context.tenant_id,
                    "PostgreSQL upsert statement failed"
                );
                crate::runtime::executor_utils::sqlx_error_to_status(
                    "PostgreSQL upsert failed",
                    &err,
                )
            })?;
            (result.rows_affected() as i64, Vec::new())
        };
        let mut projection_task_ids = Vec::new();
        if affected_rows > 0 {
            let projection_plans =
                crate::runtime::projection::ProjectionPlan::from_manifest(manifest);
            projection_task_ids =
                crate::runtime::projection::ProjectionEngine::enqueue_write_tasks_tx(
                    &mut tx,
                    &crate::runtime::system::SystemCatalogConfig::current(),
                    &context.tenant_id,
                    &request.message_type,
                    "upsert",
                    &record,
                    &projection_plans,
                )
                .await
                .map_err(|err| {
                    setup_data_internal_status(
                        "upsert_projection_task_enqueue",
                        format!("projection task enqueue failed: {err}"),
                    )
                })?;
            // mutations→CDC: emit a transactional-outbox change event for
            // CDC-enabled entities IN THE SAME TX, so a real mutation flows
            // outbox→tailer→Kafka/journal→PublishCDC subscribers (not only the
            // explicit EnqueueOutboxEvent path). Atomic with the write.
            self.emit_cdc_outbox_on_mutation(
                &mut tx,
                manifest,
                &request.message_type,
                "upsert",
                &record,
                &context,
            )
            .await?;
        }
        // NW1-3e: route through SystemStores trait.
        let receipt = match self.default_system_stores_clone() {
            Some(store) => {
                crate::runtime::consistency_fence::build_write_receipt(
                    store.as_ref(),
                    &manifest.checksum_sha256,
                    projection_task_ids,
                )
                .await
            }
            None => crate::runtime::consistency::WriteReceipt {
                source_lsn: String::new(),
                outbox_seq: 0,
                projection_task_ids,
                manifest_checksum: manifest.checksum_sha256.clone(),
                written_at_unix_ms: unix_millis(),
            },
        };
        let write_receipt_json = write_receipt_json_or_status(&receipt)?;
        let resource_uri = mutation_response_resource_uri_or_fallback(
            &context,
            &request.message_type,
            table,
            &record,
            &plan.resource_uri,
            dedup_ctx.is_some(),
        )?;
        let response = MutationResponse {
            mutation_id: Uuid::new_v4().to_string(),
            resource_uri,
            checksum_sha256: checksum_json(&record),
            record_json,
            affected_rows,
            // Fresh path only — the duplicate path returned early in the dedup
            // block above, so `false` here is now truthful.
            was_duplicate: false,
            write_receipt_json,
            write_receipt: Some(receipt.to_proto()),
            ..MutationResponse::default()
        };
        // KEYSTONE (lane 05): persist the first writer's response summary into the
        // dedup row IN THE SAME TX, so a replay returns the original body (not an
        // empty one). Keyed writes only.
        if let Some(dedup_ctx) = dedup_ctx.as_ref() {
            persist_idempotency_response_in_tx(&mut tx, dedup_ctx, &response).await?;
        }
        tx.commit().await.map_err(|err| {
            setup_data_internal_status(
                "upsert_commit",
                format!("PostgreSQL upsert commit failed: {err}"),
            )
        })?;

        let _ = self
            .cache_delete_pattern(&cache_invalidation_pattern("select", &request.message_type))
            .await;
        Ok(response)
    }

    /// GO-005: evaluate an optional compare-and-swap precondition for an upsert.
    ///
    /// Returns `Ok(())` immediately when `request.expected` is unset/empty, so
    /// the hot path pays zero extra SQL. Otherwise it locates the target row by
    /// the SAME key the upsert conflicts on — the caller's `conflict_fields`,
    /// else the manifest primary key — locks it `FOR UPDATE` inside the caller's
    /// write transaction (so concurrent CAS writers serialize on the row and the
    /// already-installed tenant/RLS GUCs still apply), decrypts it, and asserts
    /// each `expected` field equals the current value. A missing row or any
    /// mismatch is a `FAILED_PRECONDITION`; the caller returns before the write,
    /// so nothing is mutated, projected, or emitted to the CDC outbox.
    async fn enforce_upsert_precondition(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table: &ManifestTable,
        request: &UpsertRequest,
        normalized_record: &JsonValue,
        context: &RequestContext,
    ) -> Result<(), tonic::Status> {
        let expected = match request
            .expected
            .as_ref()
            .filter(|expected| !expected.fields.is_empty())
        {
            Some(expected) => expected,
            None => return Ok(()),
        };
        let resolver = crate::planning::broker::column_resolver(table);
        // The precondition key mirrors the upsert conflict target so it locks the
        // exact row the write would touch.
        let key_columns: Vec<String> = if request.conflict_fields.is_empty() {
            table.primary_key.clone()
        } else {
            request
                .conflict_fields
                .iter()
                .map(|field| crate::planning::broker::resolve_column(&resolver, field))
                .collect()
        };
        if key_columns.is_empty() {
            return Err(crate::runtime::executor_utils::failed_precondition_fields(
                "compare-and-swap precondition requires conflict_fields or a manifest primary key",
                [(
                    "expected".to_string(),
                    "no key columns are available to locate the current row".to_string(),
                )],
            ));
        }
        let key_values = record_values(normalized_record, &key_columns)?;
        let predicate = key_columns
            .iter()
            .enumerate()
            .map(|(idx, column)| format!("\"{column}\" = ${}", idx + 1))
            .collect::<Vec<_>>()
            .join(" AND ");
        // FOR UPDATE takes the row lock that serializes racing CAS writers; the
        // read runs under the tenant/RLS GUCs installed earlier in this tx.
        let sql = format!(
            "SELECT * FROM \"{schema}\".\"{table}\" WHERE {predicate} FOR UPDATE",
            schema = table.schema,
            table = table.table,
        );
        let query = bind_values(sqlx::query(&sql), table, &key_columns, &key_values)?;
        let row = query.fetch_optional(&mut **tx).await.map_err(|err| {
            crate::runtime::executor_utils::sqlx_error_to_status(
                "compare-and-swap precondition read failed",
                &err,
            )
        })?;
        let Some(row) = row else {
            return Err(crate::runtime::executor_utils::failed_precondition_fields(
                "compare-and-swap precondition failed: the target row does not exist",
                [(
                    "expected".to_string(),
                    "no row matches the upsert key".to_string(),
                )],
            ));
        };
        // Decrypt so the assertion compares plaintext, matching what the caller
        // supplied — encrypted-at-rest columns are handled transparently here.
        let record_set = rows_to_record_set(
            vec![row],
            Some(table),
            &[],
            context,
            self.encryption.as_ref(),
            &self.encryption_metrics,
        )?;
        let current: JsonValue = record_set
            .records_json
            .first()
            .map(|bytes| serde_json::from_slice(bytes).unwrap_or(JsonValue::Null))
            .unwrap_or(JsonValue::Null);
        let current = current.as_object();
        let expected_json = crate::runtime::executor_utils::struct_to_json(expected);
        if let Some(expected_obj) = expected_json.as_object() {
            for (field, want) in expected_obj {
                // The decrypted row is keyed by physical column name; resolve the
                // caller's field name and fall back to the raw key for callers
                // that assert on a column name directly.
                let column = crate::planning::broker::resolve_column(&resolver, field);
                let have = current
                    .and_then(|obj| obj.get(&column).or_else(|| obj.get(field)))
                    .unwrap_or(&JsonValue::Null);
                if !json_values_match(have, want) {
                    return Err(crate::runtime::executor_utils::failed_precondition_fields(
                        "compare-and-swap precondition failed: a field did not match the current row",
                        [(
                            field.clone(),
                            "the current value differs from the expected value".to_string(),
                        )],
                    ));
                }
            }
        }
        Ok(())
    }

    /// mutations→CDC (bug_report.md §R "kafka is not used"): emit a transactional
    /// outbox change event for a CDC-enabled entity, IN THE GIVEN TX so it is
    /// atomic with the data write. No-op when the entity has no `cdc_topic`, or
    /// when a tenant-scoped (`udb.*`) topic has no tenant to scope the event to
    /// (it could never reach a tenant-scoped subscriber). The envelope carries a
    /// top-level `tenant_id`/`project_id` so `stream_cdc`'s scope filter admits it,
    /// and the operation + record so subscribers see the change. A DB failure here
    /// rolls back the whole mutation (transactional-outbox atomicity).
    async fn emit_cdc_outbox_on_mutation(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        manifest: &CatalogManifest,
        message_type: &str,
        operation: &str,
        record: &JsonValue,
        context: &RequestContext,
    ) -> Result<(), tonic::Status> {
        // Resolve via the SAME index the mutation used (case-insensitive, full or
        // leaf message name) so the emit gate matches exactly what was written —
        // an exact `==` missed the entity and silently skipped the event.
        let table = resolve_table_for_message(manifest, message_type)
            .map_err(|_| message_type_lookup_status(manifest, message_type))?;
        let topic = table.cdc_topic.trim();
        if topic.is_empty() {
            return Ok(());
        }
        // When CDC delivery is disabled (UDB_CDC_ENABLED=false) nothing drains the
        // outbox — neither the Kafka tailer nor the in-process stream both live on
        // the tailer-fed broadcast — so writing the row would only accumulate
        // unbounded `outbox_events` with no consumer. Skip the write entirely; the
        // operator has opted out of change-event delivery.
        if !crate::runtime::cdc::cdc_delivery_enabled() {
            return Ok(());
        }
        // Tenant-scoped topics can't reach a subscriber without a tenant; skip.
        if crate::runtime::cdc::tenant_scoped_topic(topic) && context.tenant_id.trim().is_empty() {
            tracing::debug!(
                topic,
                message_type,
                "[cdc] skip mutation event: tenant-scoped topic with no tenant_id"
            );
            return Ok(());
        }
        // Partition key = the record's first primary-key value (column-keyed at this
        // point), falling back to a fresh id so the row is always partitionable.
        let partition_key = table
            .primary_key
            .first()
            .and_then(|pk| record.get(pk.as_str()))
            .map(crate::runtime::executor_utils::json_scalar_to_string)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let event_id = Uuid::new_v4();
        let envelope = serde_json::json!({
            "event_id": event_id.to_string(),
            "event_type": topic,
            "topic": topic,
            "tenant_id": context.tenant_id,
            "project_id": context.project_id,
            "operation": operation,
            "message_type": message_type,
            "document_id": partition_key,
            "correlation_id": partition_key,
            "occurred_at": chrono::Utc::now().to_rfc3339(),
            "payload": record,
        });
        let outbox_relation = self.config.cdc.outbox_relation();
        crate::runtime::cdc::insert_outbox_row(
            &mut **tx,
            &outbox_relation,
            event_id,
            topic,
            &partition_key,
            &envelope,
        )
        .await
        .map_err(|err| {
            setup_data_internal_status("cdc_outbox_emit", format!("CDC outbox emit failed: {err}"))
        })
    }

    pub async fn delete(
        &self,
        manifest: &CatalogManifest,
        message_type: &str,
        filter: JsonValue,
        context: RequestContext,
        idempotency_key: String,
    ) -> Result<MutationResponse, tonic::Status> {
        let plan_request = DeletePlanRequest {
            context: context.clone(),
            message_type: message_type.to_string(),
            filter: filter.clone(),
        };
        let plan = build_delete_plan(manifest, &plan_request);
        reject_plan(&plan.errors)?;
        let pool = self.pg_pool()?;
        let table = resolve_table_for_message(manifest, message_type)
            .map_err(|_| message_type_lookup_status(manifest, message_type))?;
        // 2.4 merge: prefer the bridged neutral-IR emission; the planner SQL
        // stays as the fallback for planner-only filter shapes (live row parity
        // pinned by the A-B oracle).
        let bridged = bridged_pg_delete_statement(manifest, &plan_request);
        let values = filter_bind_values(&filter);
        let query = match bridged.as_ref() {
            Some(stmt) => bind_typed_generic_pg_params(
                sqlx::query(&stmt.sql),
                &stmt.params,
                Some(&stmt.param_types),
            )?,
            None => bind_values(
                sqlx::query(&plan.sql),
                table,
                &plan.parameter_columns,
                &values,
            )?,
        };
        let mut tx = pool.begin().await.map_err(|e| {
            setup_data_internal_status(
                "delete_transaction_begin",
                format!("PG transaction begin failed: {e}"),
            )
        })?;
        set_request_local_settings(&mut tx, &context).await?;
        // KEYSTONE (lane 05): keyed-only durable dedup, same-tx, fail-closed.
        // Keyless deletes are unaffected.
        let dedup_ctx = {
            let key = idempotency_key_for_dedup(&idempotency_key)?;
            if let Some(key) = key {
                let config = crate::runtime::system::SystemCatalogConfig::current();
                let dedup_key = idempotency_dedup_key(
                    &context.tenant_id,
                    &context.project_id,
                    message_type,
                    "delete",
                    key,
                );
                let claim = claim_idempotency_key_in_tx(
                    &mut tx,
                    &config,
                    &dedup_key,
                    &context.tenant_id,
                    &context.project_id,
                    message_type,
                    "delete",
                )
                .await?;
                if !claim.fresh {
                    drop(tx);
                    return mutation_response_from_idempotency_json_for_claim(
                        &claim.prior_response_json,
                        &context.tenant_id,
                        &context.project_id,
                        message_type,
                    );
                }
                Some(IdempotencyPersistContext {
                    config,
                    dedup_key,
                    tenant_id: context.tenant_id.clone(),
                    project_id: context.project_id.clone(),
                    message_type: message_type.to_string(),
                    operation: "delete",
                })
            } else {
                None
            }
        };
        let result = query.execute(&mut *tx).await.map_err(|err| {
            setup_data_internal_status("delete_query", format!("PostgreSQL delete failed: {err}"))
        })?;
        let mut projection_task_ids = Vec::new();
        if result.rows_affected() > 0 {
            let projection_plans =
                crate::runtime::projection::ProjectionPlan::from_manifest(manifest);
            projection_task_ids =
                crate::runtime::projection::ProjectionEngine::enqueue_write_tasks_tx(
                    &mut tx,
                    &crate::runtime::system::SystemCatalogConfig::current(),
                    &context.tenant_id,
                    message_type,
                    "delete",
                    &filter,
                    &projection_plans,
                )
                .await
                .map_err(|err| {
                    setup_data_internal_status(
                        "delete_projection_task_enqueue",
                        format!("projection task enqueue failed: {err}"),
                    )
                })?;
            // mutations→CDC: emit a delete change event for CDC-enabled entities in
            // the same tx (the filter carries the deleted row's key).
            self.emit_cdc_outbox_on_mutation(
                &mut tx,
                manifest,
                message_type,
                "delete",
                &filter,
                &context,
            )
            .await?;
        }
        // NW1-3e: route through SystemStores trait.
        let receipt = match self.default_system_stores_clone() {
            Some(store) => {
                crate::runtime::consistency_fence::build_write_receipt(
                    store.as_ref(),
                    &manifest.checksum_sha256,
                    projection_task_ids,
                )
                .await
            }
            None => crate::runtime::consistency::WriteReceipt {
                source_lsn: String::new(),
                outbox_seq: 0,
                projection_task_ids,
                manifest_checksum: manifest.checksum_sha256.clone(),
                written_at_unix_ms: unix_millis(),
            },
        };
        let write_receipt_json = write_receipt_json_or_status(&receipt)?;
        let resource_uri = mutation_response_resource_uri_or_fallback(
            &context,
            message_type,
            table,
            &filter,
            &plan.resource_uri,
            dedup_ctx.is_some(),
        )?;
        let response = MutationResponse {
            mutation_id: Uuid::new_v4().to_string(),
            resource_uri,
            affected_rows: result.rows_affected() as i64,
            // Fresh path only — a replayed keyed delete returned early above with
            // was_duplicate=true, so the default `false` here is truthful.
            was_duplicate: false,
            write_receipt_json,
            write_receipt: Some(receipt.to_proto()),
            ..MutationResponse::default()
        };
        // KEYSTONE (lane 05): persist the first writer's response summary in-tx so
        // a replay returns the original body. Keyed deletes only.
        if let Some(dedup_ctx) = dedup_ctx.as_ref() {
            persist_idempotency_response_in_tx(&mut tx, dedup_ctx, &response).await?;
        }
        tx.commit().await.map_err(|err| {
            setup_data_internal_status(
                "delete_commit",
                format!("PostgreSQL delete commit failed: {err}"),
            )
        })?;
        let _ = self
            .cache_delete_pattern(&cache_invalidation_pattern("select", message_type))
            .await;
        Ok(response)
    }

    pub async fn vector_search(
        &self,
        manifest: &CatalogManifest,
        request: VectorSearchRequest,
        metadata_context: RequestContext,
    ) -> Result<VectorSet, tonic::Status> {
        #[cfg(not(feature = "qdrant"))]
        {
            let _ = (manifest, request, metadata_context);
            return Err(qdrant_vector_feature_status("vector_search"));
        }
        #[cfg(feature = "qdrant")]
        {
            let context = merge_context(request.context.as_ref(), metadata_context);
            let filter = request
                .filter
                .as_ref()
                .map(struct_to_json)
                .unwrap_or(JsonValue::Null);
            let plan = build_vector_search_plan(
                manifest,
                &VectorSearchPlanRequest {
                    context: context.clone(),
                    collection: request.collection.clone(),
                    vector_dimension: request.vector.len(),
                    filter: filter.clone(),
                    limit: request.limit,
                },
            );
            let route =
                self.vector_resource_backend(manifest, &context.project_id, &request.collection);
            reject_vector_plan_errors(&plan.errors, route.is_some())?;
            let target = route.unwrap_or_else(|| ResolvedBackendSelector {
                backend: plan.backend.to_ascii_lowercase(),
                instance: None,
            });
            let target_instance = if context.target_instance.trim().is_empty() {
                target.instance.as_deref().or_else(|| {
                    self.choose_instance_name_for_project(
                        &target.backend,
                        false,
                        &context.project_id,
                    )
                })
            } else {
                Some(context.target_instance.as_str())
            };
            // 03.3.3.1: honour the read fence before vector dispatch. Vector
            // backends are projection-backed, so the projection-task fence is the
            // meaningful component. Empty fences short-circuit; a hard-fail mode
            // errors; a soft stale warning is discarded (no warning channel on the
            // VectorSet response).
            let _ = self
                .enforce_read_fence(
                    &context,
                    &target.backend,
                    target_instance.unwrap_or("selected"),
                )
                .await?;
            if target.backend == "qdrant" {
                let qdrant =
                    self.qdrant_for_instance_for_project(target_instance, &context.project_id)?;
                qdrant.search(&request, filter).await
            } else {
                self.vector_search_dispatch_target(&target.backend, target_instance, &request)
                    .await
            }
        }
    }

    pub async fn vector_hybrid_search(
        &self,
        manifest: &CatalogManifest,
        request: VectorHybridSearchRequest,
        metadata_context: RequestContext,
    ) -> Result<VectorSet, tonic::Status> {
        #[cfg(not(feature = "qdrant"))]
        {
            let _ = (manifest, request, metadata_context);
            return Err(qdrant_vector_feature_status("vector_hybrid_search"));
        }
        #[cfg(feature = "qdrant")]
        {
            // Re-use the dense vector plan for validation (collection, dimension, tenant checks).
            let context = merge_context(request.context.as_ref(), metadata_context);
            let filter = request
                .filter
                .as_ref()
                .map(struct_to_json)
                .unwrap_or(JsonValue::Null);
            let plan = build_vector_search_plan(
                manifest,
                &VectorSearchPlanRequest {
                    context: context.clone(),
                    collection: request.collection.clone(),
                    vector_dimension: request.vector.len(),
                    filter: filter.clone(),
                    limit: request.limit,
                },
            );
            reject_plan(&plan.errors)?;
            if !plan.backend.trim().is_empty() && !plan.backend.eq_ignore_ascii_case("qdrant") {
                return Err(vector_hybrid_qdrant_only_status(&plan.backend));
            }
            let target_instance = if context.target_instance.trim().is_empty() {
                self.choose_instance_name_for_project("qdrant", false, &context.project_id)
            } else {
                Some(context.target_instance.as_str())
            };

            // 03.3.4.1: honour the read fence before hybrid vector dispatch (same
            // projection-task fence semantics as `vector_search`). Empty fences
            // short-circuit; a hard-fail mode errors; a soft stale warning is
            // discarded (no warning channel on the VectorSet response).
            let _ = self
                .enforce_read_fence(&context, "qdrant", target_instance.unwrap_or("selected"))
                .await?;

            // When text_query is empty, delegate to standard dense search.
            if request.text_query.trim().is_empty() {
                let dense = VectorSearchRequest {
                    context: request.context.clone(),
                    collection: request.collection,
                    vector: request.vector,
                    filter: request.filter,
                    limit: request.limit,
                    score_threshold: 0.0,
                    with_payload: request.with_payload,
                    with_vector: request.with_vector,
                    vector_name: request.vector_name,
                    quantization_rescore: request.quantization_rescore,
                };
                return self
                    .qdrant_for_instance_for_project(target_instance, &context.project_id)?
                    .search(&dense, filter)
                    .await;
            }

            // Full hybrid: Qdrant native RRF with local lexical re-ranking fallback.
            self.qdrant_for_instance_for_project(target_instance, &context.project_id)?
                .hybrid_search(&request, filter)
                .await
        }
    }

    pub async fn vector_upsert(
        &self,
        manifest: &CatalogManifest,
        request: VectorUpsertRequest,
        metadata_context: RequestContext,
    ) -> Result<MutationResponse, tonic::Status> {
        #[cfg(not(feature = "qdrant"))]
        {
            let _ = (manifest, request, metadata_context);
            return Err(qdrant_vector_feature_status("vector_upsert"));
        }
        #[cfg(feature = "qdrant")]
        {
            let context = merge_context(request.context.as_ref(), metadata_context);
            let payloads = request
                .points
                .iter()
                .map(|point| {
                    point
                        .payload
                        .as_ref()
                        .map(struct_to_json)
                        .unwrap_or(JsonValue::Null)
                })
                .collect::<Vec<_>>();
            let dimensions = request
                .points
                .iter()
                .map(|point| point.vector.len())
                .collect::<Vec<_>>();
            let plan = build_vector_upsert_plan(
                manifest,
                &VectorUpsertPlanRequest {
                    context: context.clone(),
                    collection: request.collection.clone(),
                    point_dimensions: dimensions,
                    payloads,
                },
            );
            let route =
                self.vector_resource_backend(manifest, &context.project_id, &request.collection);
            reject_vector_plan_errors(&plan.errors, route.is_some())?;
            let target = route.unwrap_or_else(|| ResolvedBackendSelector {
                backend: plan.backend.to_ascii_lowercase(),
                instance: None,
            });
            let target_instance = if context.target_instance.trim().is_empty() {
                target.instance.as_deref().or_else(|| {
                    self.choose_instance_name_for_project(
                        &target.backend,
                        true,
                        &context.project_id,
                    )
                })
            } else {
                Some(context.target_instance.as_str())
            };
            if target.backend == "qdrant" {
                let qdrant =
                    self.qdrant_for_instance_for_project(target_instance, &context.project_id)?;
                qdrant.upsert(&request).await?;
            } else {
                self.vector_upsert_dispatch_target(&target.backend, target_instance, &request)
                    .await?;
            }
            Ok(MutationResponse {
                mutation_id: Uuid::new_v4().to_string(),
                resource_uri: format!("vector://{}", request.collection),
                affected_rows: request.points.len() as i64,
                ..MutationResponse::default()
            })
        }
    }

    async fn vector_search_dispatch_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        request: &VectorSearchRequest,
    ) -> Result<VectorSet, tonic::Status> {
        use crate::runtime::executors::SearchExecutor;
        let spec = vector_search_dispatch_spec(backend, request)?;
        let executor = self.resolve_dispatch_executor(
            backend,
            instance,
            false,
            tonic::Code::FailedPrecondition,
            None,
        )?;
        let raw = SearchExecutor::search(&executor, &spec).await?;
        parse_vector_search_response(backend, &raw)
    }

    async fn vector_upsert_dispatch_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        request: &VectorUpsertRequest,
    ) -> Result<(), tonic::Status> {
        use crate::runtime::executors::MutationExecutor;
        let executor = self.resolve_dispatch_executor(
            backend,
            instance,
            true,
            tonic::Code::FailedPrecondition,
            None,
        )?;
        for point in &request.points {
            let spec = vector_upsert_dispatch_spec(backend, &request.collection, point)?;
            MutationExecutor::mutate(&executor, &spec).await?;
        }
        Ok(())
    }

    pub async fn put_object(
        &self,
        manifest: &CatalogManifest,
        mut stream: tonic::Streaming<Chunk>,
        metadata_context: RequestContext,
    ) -> Result<MutationResponse, tonic::Status> {
        #[cfg(not(any(feature = "s3", feature = "gcs", feature = "azureblob")))]
        {
            let _ = (manifest, &mut stream, metadata_context);
            return Err(no_object_store_feature_status("put_object"));
        }
        #[cfg(any(feature = "s3", feature = "gcs", feature = "azureblob"))]
        {
            // A.6: pull only the FIRST chunk (it carries bucket/key/content-type/
            // context + the first body slice); the remainder of the gRPC stream is
            // forwarded straight into the backing store without buffering the whole
            // object. Size is bounded cumulatively by `UDB_MAX_OBJECT_BYTES`.
            let first = match stream.next().await {
                Some(chunk) => chunk?,
                None => return Err(empty_object_stream_status()),
            };
            let context = merge_context(first.context.as_ref(), metadata_context);
            let plan = build_object_stream_plan(
                manifest,
                &ObjectStreamPlanRequest {
                    context: context.clone(),
                    bucket: first.bucket.clone(),
                    object_key: first.object_key.clone(),
                    method: "PUT".to_string(),
                    chunk_count: 1,
                    // Pre-flight only sees the FIRST chunk, so it cannot know
                    // whether the whole stream ends with `final_chunk=true` — a
                    // multi-chunk PUT (first chunk non-final) would wrongly fail
                    // here. The real terminator is the gRPC stream closing, which
                    // `stream_put_object` consumes; so the whole-stream invariant
                    // is satisfied by construction and must not gate the pre-flight.
                    final_chunk_seen: true,
                    content_type: first.content_type.clone(),
                },
            );
            reject_plan(&plan.errors)?;
            ensure_typed_object_backend(&plan.backend)?;
            let backend = plan.backend.trim().to_ascii_lowercase();
            let bucket = first.bucket.clone();
            let object_key = first.object_key.clone();
            let request_json =
                object_request_json("put", &bucket, &object_key, &first.content_type);
            let max_bytes = crate::runtime::config::max_object_bytes();
            let first_data = first.data;
            let project = context.project_id.clone();

            match backend.as_str() {
                "" | "s3" | "minio" => {
                    #[cfg(feature = "s3")]
                    {
                        let target_instance = if context.target_instance.trim().is_empty() {
                            self.choose_instance_name_for_project("minio", true, &project)
                                .or_else(|| {
                                    self.choose_instance_name_for_project("s3", true, &project)
                                })
                        } else {
                            Some(context.target_instance.as_str())
                        };
                        let client = self
                            .s3_for_instance_for_project(target_instance, &project)?
                            .clone();
                        let executor = crate::runtime::executors::s3::S3Executor(client);
                        stream_put_object(
                            &executor,
                            &request_json,
                            first_data,
                            stream,
                            max_bytes,
                            &backend,
                            &bucket,
                            &object_key,
                        )
                        .await?;
                    }
                    #[cfg(not(feature = "s3"))]
                    return Err(s3_minio_feature_status("put_object"));
                }
                "gcs" => {
                    #[cfg(feature = "gcs")]
                    {
                        let instance = if context.target_instance.trim().is_empty() {
                            "primary"
                        } else {
                            context.target_instance.as_str()
                        };
                        let client = self
                            .gcs_for_instance(instance)
                            .ok_or_else(|| {
                                object_instance_missing_status("gcs", "put_object", instance)
                            })?
                            .clone();
                        let executor = crate::runtime::executors::gcs::GcsExecutor::new(client);
                        stream_put_object(
                            &executor,
                            &request_json,
                            first_data,
                            stream,
                            max_bytes,
                            &backend,
                            &bucket,
                            &object_key,
                        )
                        .await?;
                    }
                    #[cfg(not(feature = "gcs"))]
                    return Err(gcs_feature_status("put_object"));
                }
                "azureblob" => {
                    #[cfg(feature = "azureblob")]
                    {
                        let instance = if context.target_instance.trim().is_empty() {
                            "primary"
                        } else {
                            context.target_instance.as_str()
                        };
                        let client = self
                            .azureblob_for_instance(instance)
                            .ok_or_else(|| {
                                object_instance_missing_status("azureblob", "put_object", instance)
                            })?
                            .clone();
                        let executor =
                            crate::runtime::executors::azureblob::AzureBlobExecutor::new(client);
                        stream_put_object(
                            &executor,
                            &request_json,
                            first_data,
                            stream,
                            max_bytes,
                            &backend,
                            &bucket,
                            &object_key,
                        )
                        .await?;
                    }
                    #[cfg(not(feature = "azureblob"))]
                    return Err(azureblob_feature_status("put_object"));
                }
                other => {
                    return Err(unsupported_object_backend_status("put_object", other));
                }
            }

            Ok(MutationResponse {
                mutation_id: Uuid::new_v4().to_string(),
                resource_uri: plan.resource_uri,
                affected_rows: 1,
                ..MutationResponse::default()
            })
        }
    }

    pub async fn get_object(
        &self,
        manifest: &CatalogManifest,
        request: crate::proto::ObjectRequest,
        metadata_context: RequestContext,
    ) -> Result<
        std::pin::Pin<
            Box<dyn tokio_stream::Stream<Item = Result<Chunk, tonic::Status>> + Send + 'static>,
        >,
        tonic::Status,
    > {
        #[cfg(not(any(feature = "s3", feature = "gcs", feature = "azureblob")))]
        {
            let _ = (manifest, request, metadata_context);
            return Err(no_object_store_feature_status("get_object"));
        }
        #[cfg(any(feature = "s3", feature = "gcs", feature = "azureblob"))]
        {
            let context = merge_context(request.context.as_ref(), metadata_context);
            let plan = build_object_stream_plan(
                manifest,
                &ObjectStreamPlanRequest {
                    context: context.clone(),
                    bucket: request.bucket.clone(),
                    object_key: request.object_key.clone(),
                    method: "GET".to_string(),
                    chunk_count: 1,
                    final_chunk_seen: true,
                    content_type: String::new(),
                },
            );
            reject_plan(&plan.errors)?;
            ensure_typed_object_backend(&plan.backend)?;
            let backend = plan.backend.trim().to_ascii_lowercase();
            // 03.3.2.1: honour the read fence on the object read entrypoint.
            // Empty fences short-circuit (no hot-path cost). A hard-fail mode
            // (Strong/ReadYourWrites, or own un-projected write) propagates as an
            // error; a soft stale warning is discarded — the object response is a
            // byte stream with no warning channel.
            let _ = self
                .enforce_read_fence(
                    &context,
                    &backend,
                    if context.target_instance.trim().is_empty() {
                        "selected"
                    } else {
                        context.target_instance.trim()
                    },
                )
                .await?;
            let bucket = request.bucket.clone();
            let object_key = request.object_key.clone();
            let request_json = object_request_json("get", &bucket, &object_key, "");
            let project = context.project_id.clone();

            // A.6: hand back the executor's streaming download wrapped into gRPC
            // `Chunk`s — the body is never fully buffered in UDB.
            let chunk_stream = match backend.as_str() {
                "" | "s3" | "minio" => {
                    #[cfg(feature = "s3")]
                    {
                        use crate::runtime::executors::ObjectExecutor as _;
                        let target_instance = if context.target_instance.trim().is_empty() {
                            self.choose_instance_name_for_project("minio", false, &project)
                                .or_else(|| {
                                    self.choose_instance_name_for_project("s3", false, &project)
                                })
                        } else {
                            Some(context.target_instance.as_str())
                        };
                        let client = self
                            .s3_for_instance_for_project(target_instance, &project)?
                            .clone();
                        let executor = crate::runtime::executors::s3::S3Executor(client);
                        let src = executor.get_object_stream(&request_json).await?;
                        byte_stream_to_chunk_stream(src, bucket, object_key, backend.clone())
                    }
                    #[cfg(not(feature = "s3"))]
                    return Err(s3_minio_feature_status("get_object"));
                }
                "gcs" => {
                    #[cfg(feature = "gcs")]
                    {
                        use crate::runtime::executors::ObjectExecutor as _;
                        let instance = if context.target_instance.trim().is_empty() {
                            "primary"
                        } else {
                            context.target_instance.as_str()
                        };
                        let client = self
                            .gcs_for_instance(instance)
                            .ok_or_else(|| {
                                object_instance_missing_status("gcs", "get_object", instance)
                            })?
                            .clone();
                        let executor = crate::runtime::executors::gcs::GcsExecutor::new(client);
                        let src = executor.get_object_stream(&request_json).await?;
                        byte_stream_to_chunk_stream(src, bucket, object_key, backend.clone())
                    }
                    #[cfg(not(feature = "gcs"))]
                    return Err(gcs_feature_status("get_object"));
                }
                "azureblob" => {
                    #[cfg(feature = "azureblob")]
                    {
                        use crate::runtime::executors::ObjectExecutor as _;
                        let instance = if context.target_instance.trim().is_empty() {
                            "primary"
                        } else {
                            context.target_instance.as_str()
                        };
                        let client = self
                            .azureblob_for_instance(instance)
                            .ok_or_else(|| {
                                object_instance_missing_status("azureblob", "get_object", instance)
                            })?
                            .clone();
                        let executor =
                            crate::runtime::executors::azureblob::AzureBlobExecutor::new(client);
                        let src = executor.get_object_stream(&request_json).await?;
                        byte_stream_to_chunk_stream(src, bucket, object_key, backend.clone())
                    }
                    #[cfg(not(feature = "azureblob"))]
                    return Err(azureblob_feature_status("get_object"));
                }
                other => {
                    return Err(unsupported_object_backend_status("get_object", other));
                }
            };
            Ok(chunk_stream)
        }
    }

    pub async fn generate_presigned_url(
        &self,
        manifest: &CatalogManifest,
        request: UrlRequest,
        metadata_context: RequestContext,
    ) -> Result<UrlResponse, tonic::Status> {
        #[cfg(not(feature = "s3"))]
        {
            let _ = (manifest, request, metadata_context);
            return Err(s3_object_feature_status("generate_presigned_url"));
        }
        #[cfg(feature = "s3")]
        {
            let context = merge_context(request.context.as_ref(), metadata_context);
            let decision = evaluate_object_access(
                manifest,
                &ObjectAccessRequest {
                    context: context.clone(),
                    bucket: request.bucket.clone(),
                    object_key: request.object_key.clone(),
                    method: request.method.clone(),
                    presigned: true,
                },
            );
            reject_plan(&decision.errors)?;
            let method = request.method.to_ascii_uppercase();
            if method != "PUT" && method != "GET" {
                return Err(unsupported_presign_method_status());
            }
            let target_instance = if context.target_instance.trim().is_empty() {
                let write = method == "PUT";
                self.choose_instance_name_for_project("minio", write, &context.project_id)
                    .or_else(|| {
                        self.choose_instance_name_for_project("s3", write, &context.project_id)
                    })
            } else {
                Some(context.target_instance.as_str())
            };
            let s3 = self.s3_for_instance_for_project(target_instance, &context.project_id)?;
            let ttl = bounded_ttl(request.ttl_seconds);
            let url = presign_s3_url(
                &s3,
                &request.bucket,
                &request.object_key,
                &method,
                &request.content_type,
                ttl,
            )
            .await?;
            Ok(UrlResponse {
                url,
                expires_at_unix: unix_now() + ttl as i64,
            })
        }
    }

    /// Upsert vector points into a collection WITHOUT manifest/policy evaluation
    /// (admin/native path). Ensures the collection exists (creating it at the
    /// given dimension, cosine distance) then upserts. Used by the asset service
    /// to push `EMBED`-step vectors into the configured vector backend.
    pub async fn vector_upsert_backend_target(
        &self,
        instance: Option<&str>,
        project_id: &str,
        collection: &str,
        dimension: i32,
        points: Vec<VectorPointMutation>,
    ) -> Result<(), tonic::Status> {
        #[cfg(not(feature = "qdrant"))]
        {
            let _ = (instance, project_id, collection, dimension, points);
            Err(qdrant_vector_feature_status("vector_upsert_backend_target"))
        }
        #[cfg(feature = "qdrant")]
        {
            use crate::generation::manifest::{ManifestStore, ManifestStoreOption};
            let mut vector_names = points
                .iter()
                .map(|point| point.vector_name.trim())
                .filter(|name| !name.is_empty())
                .collect::<Vec<_>>();
            vector_names.sort_unstable();
            vector_names.dedup();
            if !vector_names.is_empty()
                && points
                    .iter()
                    .any(|point| point.vector_name.trim().is_empty())
            {
                return Err(setup_data_invalid_field(
                    "points.vector_name",
                    "one collection upsert cannot mix unnamed and named vector spaces",
                    "mixed unnamed and named vector batch",
                ));
            }
            let project = project_id.trim();
            let project = if project.is_empty() {
                crate::runtime::catalog::DEFAULT_PROJECT_ID
            } else {
                project
            };
            let client = self.qdrant_for_instance_for_project(instance, project)?;
            let store = ManifestStore {
                store_kind: "vector".to_string(),
                backend: "qdrant".to_string(),
                logical_name: collection.to_string(),
                database_name: String::new(),
                namespace: String::new(),
                resource_name: collection.to_string(),
                dsn_env_key: String::new(),
                dsn: String::new(),
                owner_schema: String::new(),
                owner_table: String::new(),
                payload_schema_json: String::new(),
                options: vec![
                    ManifestStoreOption {
                        key: "dimension".to_string(),
                        value: dimension.to_string(),
                    },
                    ManifestStoreOption {
                        key: "distance".to_string(),
                        value: "Cosine".to_string(),
                    },
                    ManifestStoreOption {
                        key: "vector_names_json".to_string(),
                        value: serde_json::to_string(&vector_names).unwrap_or_default(),
                    },
                ],
            };
            client.ensure_collection(&store).await?;
            client
                .upsert(&VectorUpsertRequest {
                    context: None,
                    collection: collection.to_string(),
                    points,
                    idempotency_key: String::new(),
                })
                .await?;
            Ok(())
        }
    }

    pub async fn vector_ensure_backend_kind_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        project_id: &str,
        collection: &str,
        dimension: i32,
        distance: &str,
        output_dtype: &str,
        vector_names: &[String],
    ) -> Result<(), tonic::Status> {
        if backend.eq_ignore_ascii_case("qdrant") {
            #[cfg(feature = "qdrant")]
            {
                use crate::generation::manifest::{ManifestStore, ManifestStoreOption};
                let project = if project_id.trim().is_empty() {
                    crate::runtime::catalog::DEFAULT_PROJECT_ID
                } else {
                    project_id.trim()
                };
                let client = self.qdrant_for_instance_for_project(instance, project)?;
                let store = ManifestStore {
                    store_kind: "vector".to_string(),
                    backend: "qdrant".to_string(),
                    logical_name: collection.to_string(),
                    resource_name: collection.to_string(),
                    options: vec![
                        ManifestStoreOption {
                            key: "dimension".to_string(),
                            value: dimension.to_string(),
                        },
                        ManifestStoreOption {
                            key: "distance".to_string(),
                            value: distance.to_string(),
                        },
                        ManifestStoreOption {
                            key: "output_dtype".to_string(),
                            value: output_dtype.to_string(),
                        },
                        ManifestStoreOption {
                            key: "vector_names_json".to_string(),
                            value: serde_json::to_string(vector_names).unwrap_or_default(),
                        },
                    ],
                    ..ManifestStore::default()
                };
                return client.ensure_collection(&store).await;
            }
            #[cfg(not(feature = "qdrant"))]
            return Err(qdrant_vector_feature_status(
                "vector_ensure_backend_kind_target",
            ));
        }
        let spec = serde_json::json!({
            "dimension": dimension,
            "distance": distance,
            "output_dtype": output_dtype,
            "vector_names": vector_names,
        });
        self.ensure_resource_backend_target(
            &backend.to_ascii_lowercase(),
            instance,
            collection,
            &spec.to_string(),
        )
        .await
    }

    pub async fn vector_upsert_existing_backend_kind_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        project_id: &str,
        collection: &str,
        points: Vec<VectorPointMutation>,
    ) -> Result<(), tonic::Status> {
        if backend.eq_ignore_ascii_case("qdrant") {
            #[cfg(feature = "qdrant")]
            {
                let project = if project_id.trim().is_empty() {
                    crate::runtime::catalog::DEFAULT_PROJECT_ID
                } else {
                    project_id.trim()
                };
                let client = self.qdrant_for_instance_for_project(instance, project)?;
                return client
                    .upsert(&VectorUpsertRequest {
                        context: None,
                        collection: collection.to_string(),
                        points,
                        idempotency_key: String::new(),
                    })
                    .await;
            }
            #[cfg(not(feature = "qdrant"))]
            return Err(qdrant_vector_feature_status(
                "vector_upsert_existing_backend_kind_target",
            ));
        }
        let _ = project_id;
        self.vector_upsert_dispatch_target(
            &backend.to_ascii_lowercase(),
            instance,
            &VectorUpsertRequest {
                context: None,
                collection: collection.to_string(),
                points,
                idempotency_key: String::new(),
            },
        )
        .await
    }

    /// Backend-neutral native vector upsert. Qdrant keeps its collection ensure
    /// path; Elasticsearch/Weaviate/Pinecone reuse the existing typed dispatch.
    pub async fn vector_upsert_backend_kind_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        project_id: &str,
        collection: &str,
        dimension: i32,
        distance: &str,
        output_dtype: &str,
        points: Vec<VectorPointMutation>,
    ) -> Result<(), tonic::Status> {
        if backend.eq_ignore_ascii_case("qdrant") {
            let mut vector_names = points
                .iter()
                .map(|point| point.vector_name.trim().to_string())
                .filter(|name| !name.is_empty())
                .collect::<Vec<_>>();
            vector_names.sort_unstable();
            vector_names.dedup();
            self.vector_ensure_backend_kind_target(
                backend,
                instance,
                project_id,
                collection,
                dimension,
                distance,
                output_dtype,
                &vector_names,
            )
            .await?;
            return self
                .vector_upsert_existing_backend_kind_target(
                    backend, instance, project_id, collection, points,
                )
                .await;
        }
        let _ = (project_id, dimension, distance, output_dtype);
        self.vector_upsert_dispatch_target(
            &backend.to_ascii_lowercase(),
            instance,
            &VectorUpsertRequest {
                context: None,
                collection: collection.to_string(),
                points,
                idempotency_key: String::new(),
            },
        )
        .await
    }

    pub async fn vector_search_backend_kind_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        project_id: &str,
        request: &VectorSearchRequest,
    ) -> Result<VectorSet, tonic::Status> {
        if backend.eq_ignore_ascii_case("qdrant") {
            #[cfg(feature = "qdrant")]
            {
                let project = if project_id.trim().is_empty() {
                    crate::runtime::catalog::DEFAULT_PROJECT_ID
                } else {
                    project_id.trim()
                };
                let client = self.qdrant_for_instance_for_project(instance, project)?;
                let filter = request
                    .filter
                    .as_ref()
                    .map(struct_to_json)
                    .unwrap_or(JsonValue::Null);
                return client.search(request, filter).await;
            }
            #[cfg(not(feature = "qdrant"))]
            return Err(qdrant_vector_feature_status(
                "vector_search_backend_kind_target",
            ));
        }
        let _ = project_id;
        self.vector_search_dispatch_target(&backend.to_ascii_lowercase(), instance, request)
            .await
    }

    pub async fn vector_hybrid_backend_kind_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        project_id: &str,
        request: &VectorHybridSearchRequest,
    ) -> Result<VectorSet, tonic::Status> {
        if backend.eq_ignore_ascii_case("qdrant") {
            #[cfg(feature = "qdrant")]
            {
                let project = if project_id.trim().is_empty() {
                    crate::runtime::catalog::DEFAULT_PROJECT_ID
                } else {
                    project_id.trim()
                };
                let client = self.qdrant_for_instance_for_project(instance, project)?;
                let filter = request
                    .filter
                    .as_ref()
                    .map(struct_to_json)
                    .unwrap_or(JsonValue::Null);
                return client.hybrid_search(request, filter).await;
            }
            #[cfg(not(feature = "qdrant"))]
            return Err(qdrant_vector_feature_status(
                "vector_hybrid_backend_kind_target",
            ));
        }
        // Portable stores without a native sparse fusion endpoint still provide
        // the dense candidate stage; callers can request weighted/rerank stages
        // above this seam without silently targeting Qdrant.
        self.vector_search_backend_kind_target(
            backend,
            instance,
            project_id,
            &VectorSearchRequest {
                context: request.context.clone(),
                collection: request.collection.clone(),
                vector: request.vector.clone(),
                filter: request.filter.clone(),
                limit: request.limit,
                score_threshold: 0.0,
                with_payload: request.with_payload,
                with_vector: request.with_vector,
                vector_name: request.vector_name.clone(),
                quantization_rescore: request.quantization_rescore,
            },
        )
        .await
    }

    pub async fn vector_swap_alias_backend_target(
        &self,
        instance: Option<&str>,
        project_id: &str,
        alias: &str,
        collection: &str,
    ) -> Result<(), tonic::Status> {
        #[cfg(feature = "qdrant")]
        {
            let project = if project_id.trim().is_empty() {
                crate::runtime::catalog::DEFAULT_PROJECT_ID
            } else {
                project_id.trim()
            };
            return self
                .qdrant_for_instance_for_project(instance, project)?
                .swap_collection_alias(alias, collection)
                .await;
        }
        #[cfg(not(feature = "qdrant"))]
        {
            let _ = (instance, project_id, alias, collection);
            Err(qdrant_vector_feature_status(
                "vector_swap_alias_backend_target",
            ))
        }
    }

    /// Delete vector points by id WITHOUT manifest/policy evaluation (admin/native
    /// path; parity with `vector_upsert_backend_target`). Used by the asset service
    /// to remove an embedding when its pipeline fails. No-op for an empty id set.
    pub async fn vector_delete_backend_target(
        &self,
        instance: Option<&str>,
        project_id: &str,
        collection: &str,
        point_ids: Vec<String>,
    ) -> Result<(), tonic::Status> {
        #[cfg(not(feature = "qdrant"))]
        {
            let _ = (instance, project_id, collection, point_ids);
            Err(qdrant_vector_feature_status("vector_delete_backend_target"))
        }
        #[cfg(feature = "qdrant")]
        {
            if point_ids.is_empty() {
                return Ok(());
            }
            let project = project_id.trim();
            let project = if project.is_empty() {
                crate::runtime::catalog::DEFAULT_PROJECT_ID
            } else {
                project
            };
            let client = self.qdrant_for_instance_for_project(instance, project)?;
            client.delete_points(collection, &point_ids).await
        }
    }

    /// Delete every point in a collection whose payload matches `filter`, through
    /// the SAME shared vector seam as [`Self::vector_delete_backend_target`]
    /// (never a second vector client). Used by the embedding source-teardown pass
    /// to erase a deleted source's vectors by their `{_tenant_id, _source}` tags —
    /// retention-independent, so a source whose `udb.embedding.work.v1` journal
    /// events were purged is still fully erased. The caller MUST scope the filter
    /// to a verified tenant (an under-scoped filter could delete another tenant's
    /// vectors); this seam trusts the filter it is given.
    pub async fn vector_delete_by_filter_backend_target(
        &self,
        instance: Option<&str>,
        project_id: &str,
        collection: &str,
        filter: serde_json::Value,
    ) -> Result<(), tonic::Status> {
        #[cfg(not(feature = "qdrant"))]
        {
            let _ = (instance, project_id, collection, filter);
            Err(qdrant_vector_feature_status(
                "vector_delete_by_filter_backend_target",
            ))
        }
        #[cfg(feature = "qdrant")]
        {
            let project = project_id.trim();
            let project = if project.is_empty() {
                crate::runtime::catalog::DEFAULT_PROJECT_ID
            } else {
                project
            };
            let client = self.qdrant_for_instance_for_project(instance, project)?;
            client.delete_by_filter(collection, filter).await
        }
    }

    /// Mint a presigned object URL WITHOUT manifest/policy evaluation — the
    /// admin/native path (mirrors `*_object_backend_target`). Used by the native
    /// storage service, which owns its own bucket. `method` is "PUT" or "GET".
    /// Returns `(url, expires_at_unix)`.
    pub async fn presign_object_backend_target(
        &self,
        backend_target: &str,
        project_id: &str,
        bucket: &str,
        object_key: &str,
        method: &str,
        content_type: &str,
        ttl_seconds: i32,
    ) -> Result<(String, i64), tonic::Status> {
        #[cfg(not(feature = "s3"))]
        {
            let _ = (
                backend_target,
                project_id,
                bucket,
                object_key,
                method,
                content_type,
                ttl_seconds,
            );
            Err(s3_object_feature_status("presign_object_backend_target"))
        }
        #[cfg(feature = "s3")]
        {
            let method = method.to_ascii_uppercase();
            if method != "PUT" && method != "GET" {
                return Err(unsupported_presign_method_status());
            }
            let project = project_id.trim();
            let project = if project.is_empty() {
                crate::runtime::catalog::DEFAULT_PROJECT_ID
            } else {
                project
            };
            let write = method == "PUT";
            let target = backend_target.trim();
            let target_lower = target.to_ascii_lowercase();
            let target_instance = match target_lower.as_str() {
                "" | "minio" => self.choose_instance_name_for_project("minio", write, project),
                "s3" => self.choose_instance_name_for_project("s3", write, project),
                _ => Some(target),
            };
            let s3 = self.s3_for_instance_for_project(target_instance, project)?;
            let ttl = bounded_ttl(ttl_seconds);
            let url = presign_s3_url(&s3, bucket, object_key, &method, content_type, ttl).await?;
            Ok((url, unix_now() + ttl as i64))
        }
    }

    /// Check object presence WITHOUT manifest/policy evaluation — the native
    /// storage finalize path uses this after a presigned upload before marking
    /// metadata ACTIVE. S3/MinIO only; metadata-only deployments skip the check.
    ///
    /// `Ok(None)` = object absent (a bodiless S3 404); `Ok(Some((size, etag)))` =
    /// present, returning the HEAD `content_length` + `e_tag` so the finalize path
    /// can verify a truncated/wrong upload.
    pub async fn object_exists_backend_target(
        &self,
        backend_target: &str,
        project_id: &str,
        bucket: &str,
        object_key: &str,
    ) -> Result<Option<(i64, String)>, tonic::Status> {
        #[cfg(not(feature = "s3"))]
        {
            let _ = (backend_target, project_id, bucket, object_key);
            Err(s3_object_feature_status("object_exists_backend_target"))
        }
        #[cfg(feature = "s3")]
        {
            let project = project_id.trim();
            let project = if project.is_empty() {
                crate::runtime::catalog::DEFAULT_PROJECT_ID
            } else {
                project
            };
            let target = backend_target.trim().to_ascii_lowercase();
            // Q#7 (bug_report.md): existence-after-write must HEAD the SAME instance
            // the object was WRITTEN to. PutObject resolves the write instance
            // (`choose_instance_name_for_project(.., write=true, ..)`); resolving a
            // read replica here (`write=false`) can target a different MinIO/S3
            // instance than the upload landed on → spurious not-found / service
            // error on FinalizeUpload. Use the write instance for the HEAD.
            let target_instance = match target.as_str() {
                "" | "minio" => self.choose_instance_name_for_project("minio", true, project),
                "s3" => self.choose_instance_name_for_project("s3", true, project),
                instance => Some(instance),
            };
            let s3 = self.s3_for_instance_for_project(target_instance, project)?;
            match s3.head_object().bucket(bucket).key(object_key).send().await {
                Ok(head) => {
                    // Surface the HEAD size + ETag so FinalizeUpload can verify a
                    // truncated/wrong upload. `content_length`/`e_tag` are optional
                    // on the SDK output; absent → 0 / "".
                    let size = head.content_length().unwrap_or(0);
                    let etag = head.e_tag().unwrap_or_default().to_string();
                    Ok(Some((size, etag)))
                }
                Err(err) => {
                    // S3 answers a HEAD for a missing object (or bucket) with a
                    // BODILESS 404, so the SDK error's Display is a generic
                    // "service error" with NO "NotFound"/"NoSuchKey"/"404" text —
                    // string-matching it misclassifies a plain absent object as a
                    // service failure (the FinalizeUpload bug). The NotFound signal
                    // lives ONLY in the typed service error, so classify on that:
                    // a 404 → not present (Ok(None)); anything else (auth, network,
                    // endpoint) → a real failure.
                    let not_found = err
                        .as_service_error()
                        .map(|svc| svc.is_not_found())
                        .unwrap_or(false);
                    if not_found {
                        Ok(None)
                    } else {
                        Err(crate::runtime::executor_utils::backend_transport_status(
                            "S3",
                            "object head",
                            err,
                        ))
                    }
                }
            }
        }
    }

    pub async fn initiate_multipart_upload(
        &self,
        manifest: &CatalogManifest,
        request: MultipartUploadRequest,
        metadata_context: RequestContext,
    ) -> Result<MultipartUploadResponse, tonic::Status> {
        #[cfg(not(feature = "s3"))]
        {
            let _ = (manifest, request, metadata_context);
            return Err(s3_object_feature_status("initiate_multipart_upload"));
        }
        #[cfg(feature = "s3")]
        {
            let context = merge_context(request.context.as_ref(), metadata_context);
            let decision = evaluate_object_access(
                manifest,
                &ObjectAccessRequest {
                    context: context.clone(),
                    bucket: request.bucket.clone(),
                    object_key: request.object_key.clone(),
                    method: "PUT".to_string(),
                    presigned: true,
                },
            );
            reject_plan(&decision.errors)?;
            if request.part_count <= 0 {
                return Err(invalid_part_count_status());
            }
            let target_instance = if context.target_instance.trim().is_empty() {
                self.choose_instance_name_for_project("minio", true, &context.project_id)
                    .or_else(|| {
                        self.choose_instance_name_for_project("s3", true, &context.project_id)
                    })
            } else {
                Some(context.target_instance.as_str())
            };
            let s3 = self.s3_for_instance_for_project(target_instance, &context.project_id)?;
            let upload = s3
                .create_multipart_upload()
                .bucket(&request.bucket)
                .key(&request.object_key)
                .set_content_type(if request.content_type.is_empty() {
                    None
                } else {
                    Some(request.content_type.clone())
                })
                .send()
                .await
                .map_err(|err| {
                    crate::runtime::executor_utils::backend_transport_status(
                        "S3",
                        "multipart init",
                        err,
                    )
                })?;
            let upload_id = upload.upload_id().unwrap_or_default().to_string();
            let ttl = bounded_ttl(request.ttl_seconds);
            let config = PresigningConfig::expires_in(Duration::from_secs(ttl))
                .map_err(invalid_presign_ttl_status)?;
            let mut part_urls = Vec::new();
            for part_number in 1..=request.part_count {
                let url = s3
                    .upload_part()
                    .bucket(&request.bucket)
                    .key(&request.object_key)
                    .upload_id(&upload_id)
                    .part_number(part_number)
                    .presigned(config.clone())
                    .await
                    .map_err(|err| {
                        crate::runtime::executor_utils::backend_transport_status(
                            "S3",
                            "part presign",
                            err,
                        )
                    })?;
                part_urls.push(url.uri().to_string());
            }
            Ok(MultipartUploadResponse {
                upload_id,
                part_urls,
                expires_at_unix: unix_now() + ttl as i64,
            })
        }
    }
}

// ── KEYSTONE (lane 05): durable, fail-closed, tenant+project-scoped idempotency
// dedup for keyed data-plane mutations ─────────────────────────────────────────

/// Result of an atomic same-tx idempotency-key claim.
///
/// `fresh == true` means THIS caller inserted the row and owns the write; the
/// caller must run the data write and then persist its response summary into the
/// row before committing. `fresh == false` means the key already existed (a
/// replay): the caller MUST NOT run the write and instead returns the stored
/// `prior_response_json` with `was_duplicate = true`.
struct IdempotencyClaim {
    fresh: bool,
    prior_response_json: JsonValue,
}

struct IdempotencyPersistContext {
    config: crate::runtime::system::SystemCatalogConfig,
    dedup_key: String,
    tenant_id: String,
    project_id: String,
    message_type: String,
    operation: &'static str,
}

/// Tenant+project-scoped salted dedup key. Returns the hex SHA-256 of
/// `"{tenant}\0{project}\0{message_type}\0{operation}\0{key}"` — NEVER the bare
/// client key, so two tenants (or projects) reusing `"key-1"` cannot collide
/// (RLS/tenant-scope guardrail), and two mutation RPCs sharing a caller key do
/// not replay each other's response. Mirrors the `checksum_str` hashing already
/// used for receipts.
fn idempotency_dedup_key(
    tenant_id: &str,
    project_id: &str,
    message_type: &str,
    operation: &str,
    key: &str,
) -> String {
    crate::runtime::executor_utils::checksum_str(&format!(
        "{tenant_id}\0{project_id}\0{message_type}\0{operation}\0{key}"
    ))
}

fn idempotency_key_for_dedup(key: &str) -> Result<Option<&str>, tonic::Status> {
    if key.is_empty() {
        return Ok(None);
    }
    if key.chars().any(|ch| ch.is_whitespace() || ch.is_control()) {
        return Err(setup_data_invalid_field(
            "idempotency_key",
            "must be empty or contain no whitespace or control characters",
            "idempotency_key must be empty or contain no whitespace or control characters",
        ));
    }
    Ok(Some(key))
}

/// Atomically claim a dedup key INSIDE the caller's write transaction, mirroring
/// the proven `projection/mod.rs::insert_task_if_absent_on` ON CONFLICT CTE.
///
/// Because the INSERT runs in the same `sqlx::Transaction` as the data write, a
/// dedup-table failure naturally aborts the whole tx (fail-closed by
/// construction): the write cannot commit without this INSERT succeeding, and a
/// conflict means we never run the write. Any claim SQL failure is surfaced as a
/// retryable `UNAVAILABLE` dedup-store error so SDKs can safely retry the SAME
/// keyed mutation while the tx is dropped fail-closed.
async fn claim_idempotency_key_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    config: &crate::runtime::system::SystemCatalogConfig,
    dedup_key: &str,
    tenant_id: &str,
    project_id: &str,
    message_type: &str,
    operation: &str,
) -> Result<IdempotencyClaim, tonic::Status> {
    let rel = config.idempotency_keys_relation();
    let sql = idempotency_claim_sql(&rel);
    let row: Option<(bool, JsonValue)> = sqlx::query_as(&sql)
        .bind(dedup_key)
        .bind(tenant_id)
        .bind(project_id)
        .bind(message_type)
        .bind(operation)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|err| idempotency_dedup_claim_status(&err))?;
    let Some(row) = row else {
        return Err(setup_data_internal_status(
            "idempotency_dedup_claim_shape",
            "idempotency dedup claim returned no row for matching scope",
        ));
    };
    Ok(IdempotencyClaim {
        fresh: row.0,
        prior_response_json: row.1,
    })
}

fn idempotency_dedup_claim_status(err: &sqlx::Error) -> tonic::Status {
    tracing::error!(
        error = %err,
        "idempotency dedup claim failed; keyed mutation refused fail-closed"
    );
    crate::runtime::executor_utils::retryable_status(
        "postgres",
        "idempotency_dedup_claim",
        250,
        "idempotency dedup claim failed: dedup store unavailable (fail-closed)",
    )
}

fn idempotency_claim_sql(rel: &str) -> String {
    format!(
        "WITH ins AS (
             INSERT INTO {rel}
                 (dedup_key, tenant_id, project_id, message_type, operation, response_json)
             VALUES ($1, $2, $3, $4, $5, '{{}}'::jsonb)
             ON CONFLICT (dedup_key) DO NOTHING
             RETURNING true AS inserted, response_json
         )
         SELECT inserted, response_json FROM ins
         UNION ALL
         SELECT false AS inserted, response_json
         FROM {rel}
         WHERE dedup_key = $1
           AND tenant_id = $2
           AND project_id = $3
           AND message_type = $4
           AND operation = $5
           AND NOT EXISTS (SELECT 1 FROM ins)
         LIMIT 1"
    )
}

/// Persist the first writer's `MutationResponse` summary into the dedup row,
/// inside the same write tx, so a later replay returns the original body.
/// `record_json` (the protobuf field) is `bytes` (a single blob); we base64 it
/// for JSON-safe round-tripping.
fn mutation_response_idempotency_json(
    response: &MutationResponse,
) -> Result<JsonValue, tonic::Status> {
    use base64::Engine as _;
    let record_json_b64 = base64::engine::general_purpose::STANDARD.encode(&response.record_json);
    let summary = serde_json::json!({
        "mutation_id": response.mutation_id,
        "resource_uri": response.resource_uri,
        "checksum_sha256": response.checksum_sha256,
        "record_json": record_json_b64,
        "affected_rows": response.affected_rows,
        "write_receipt_json": response.write_receipt_json,
    });
    idempotency_response_write_receipt_lockstep(response, &summary)?;
    mutation_response_from_idempotency_json(&summary)?;
    Ok(summary)
}

fn mutation_response_idempotency_json_for_claim(
    response: &MutationResponse,
    tenant_id: &str,
    project_id: &str,
    message_type: &str,
) -> Result<JsonValue, tonic::Status> {
    let mut summary = mutation_response_idempotency_json(response)?;
    let Some(object) = summary.as_object_mut() else {
        return Err(setup_data_internal_status(
            "idempotency_response_summary_shape",
            "idempotency response summary must be a JSON object",
        ));
    };
    object.insert(
        "tenant_id".to_string(),
        JsonValue::String(tenant_id.to_string()),
    );
    object.insert(
        "project_id".to_string(),
        JsonValue::String(project_id.to_string()),
    );
    object.insert(
        "message_type".to_string(),
        JsonValue::String(message_type.to_string()),
    );
    mutation_response_from_idempotency_json_for_claim(
        &summary,
        tenant_id,
        project_id,
        message_type,
    )?;
    Ok(summary)
}

fn idempotency_response_write_receipt_lockstep(
    response: &MutationResponse,
    summary: &JsonValue,
) -> Result<(), tonic::Status> {
    let (_, summary_receipt) = idempotency_replay_write_receipt(summary)?;
    let Some(response_receipt) = response.write_receipt.as_ref() else {
        return Err(setup_data_internal_status(
            "idempotency_response_write_receipt_missing",
            "idempotency response summary requires typed write_receipt before persist",
        ));
    };
    let response_receipt = crate::runtime::consistency::WriteReceipt::from_proto(response_receipt);
    if response_receipt != summary_receipt {
        return Err(setup_data_internal_status(
            "idempotency_response_write_receipt_mismatch",
            "idempotency response summary typed write_receipt must match write_receipt_json before persist",
        ));
    }
    Ok(())
}

fn idempotency_response_persist_row_count_status(rows_affected: u64) -> Result<(), tonic::Status> {
    if rows_affected == 1 {
        return Ok(());
    }
    Err(setup_data_internal_status(
        "idempotency_response_persist_row_count",
        format!("idempotency response persist affected {rows_affected} rows; expected exactly one"),
    ))
}

fn write_receipt_json_or_status(
    receipt: &crate::runtime::consistency::WriteReceipt,
) -> Result<String, tonic::Status> {
    serde_json::to_string(receipt).map_err(|err| {
        setup_data_internal_status(
            "write_receipt_json_encode",
            format!("write receipt JSON serialization failed: {err}"),
        )
    })
}

fn mutation_response_resource_uri(
    context: &RequestContext,
    message_type: &str,
    table: &ManifestTable,
    identity_source: &JsonValue,
) -> Result<String, tonic::Status> {
    let tenant = mutation_resource_uri_token("tenant_id", &context.tenant_id)?;
    let message = mutation_resource_uri_token("message_type", message_type)?;
    let resource_id = mutation_resource_id_from_json(table, identity_source)?;
    Ok(format!("udb://{tenant}/{message}/{resource_id}"))
}

fn mutation_response_resource_uri_or_fallback(
    context: &RequestContext,
    message_type: &str,
    table: &ManifestTable,
    identity_source: &JsonValue,
    fallback_resource_uri: &str,
    require_data_plane_uri: bool,
) -> Result<String, tonic::Status> {
    match mutation_response_resource_uri(context, message_type, table, identity_source) {
        Ok(uri) => Ok(uri),
        Err(err) if require_data_plane_uri => Err(err),
        Err(_) => Ok(fallback_resource_uri.to_string()),
    }
}

fn mutation_resource_uri_token(label: &str, value: &str) -> Result<String, tonic::Status> {
    if value.trim().is_empty() {
        return Err(setup_data_internal_status(
            "mutation_resource_uri_token",
            format!("mutation response resource_uri {label} must be non-empty"),
        ));
    }
    if value != value.trim() || value.chars().any(char::is_whitespace) || value.contains('/') {
        return Err(setup_data_internal_status(
            "mutation_resource_uri_token",
            format!("mutation response resource_uri {label} must be an unpadded path token"),
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(setup_data_internal_status(
            "mutation_resource_uri_token",
            format!("mutation response resource_uri {label} must not contain control characters"),
        ));
    }
    Ok(value.to_string())
}

fn mutation_resource_id_from_json(
    table: &ManifestTable,
    identity_source: &JsonValue,
) -> Result<String, tonic::Status> {
    for primary_key in &table.primary_key {
        if let Some(value) = manifest_json_value(table, identity_source, primary_key) {
            return mutation_resource_id_value("primary key", value);
        }
    }

    let Some(object) = identity_source.as_object() else {
        return Err(setup_data_internal_status(
            "mutation_resource_uri_identity_source",
            "mutation response resource_uri identity source must be a JSON object",
        ));
    };
    let mut fields = object.iter().collect::<Vec<_>>();
    fields.sort_by(|left, right| left.0.cmp(right.0));

    if let Some((field, value)) = mutation_identity_field_from_json(identity_source)? {
        return mutation_resource_id_value(field, value);
    }

    for (field, value) in fields {
        let name = field.to_ascii_lowercase();
        if name != "tenant_id"
            && name != "project_id"
            && let Some(token) = mutation_resource_id_scalar(value)
        {
            return mutation_resource_uri_token(field, &token);
        }
    }

    Err(setup_data_internal_status(
        "mutation_resource_uri_identity_required",
        "mutation response resource_uri requires a scalar primary key or identity field",
    ))
}

fn manifest_json_value<'a>(
    table: &'a ManifestTable,
    identity_source: &'a JsonValue,
    column_name: &str,
) -> Option<&'a JsonValue> {
    let object = identity_source.as_object()?;
    if let Some(value) = object.get(column_name) {
        return Some(value);
    }
    let direct = table
        .columns
        .iter()
        .find(|column| column.column_name == column_name)
        .and_then(|column| {
            (!column.field_name.is_empty())
                .then(|| object.get(&column.field_name))
                .flatten()
        });
    if direct.is_some() {
        return direct;
    }

    let mut found = None;
    for key in ["$and", "and"] {
        let Some(JsonValue::Array(items)) = object.get(key) else {
            continue;
        };
        for item in items {
            let Some(value) = manifest_json_value(table, item, column_name) else {
                continue;
            };
            if found.is_some() {
                return None;
            }
            found = Some(value);
        }
    }
    found
}

fn mutation_identity_field_from_json<'a>(
    identity_source: &'a JsonValue,
) -> Result<Option<(&'a str, &'a JsonValue)>, tonic::Status> {
    let mut found = None;
    mutation_collect_identity_field(identity_source, &mut found)?;
    Ok(found)
}

fn mutation_collect_identity_field<'a>(
    identity_source: &'a JsonValue,
    found: &mut Option<(&'a str, &'a JsonValue)>,
) -> Result<(), tonic::Status> {
    let Some(object) = identity_source.as_object() else {
        return Ok(());
    };
    let mut fields = object.iter().collect::<Vec<_>>();
    fields.sort_by(|left, right| left.0.cmp(right.0));
    for (field, value) in fields {
        let name = field.to_ascii_lowercase();
        if (name == "id" || name.ends_with("_id")) && name != "tenant_id" && name != "project_id" {
            if found.is_some() {
                return Err(setup_data_internal_status(
                    "mutation_resource_uri_identity_ambiguous",
                    "mutation response resource_uri identity field is ambiguous",
                ));
            }
            *found = Some((field.as_str(), value));
        }
    }
    for key in ["$and", "and"] {
        let Some(JsonValue::Array(items)) = object.get(key) else {
            continue;
        };
        for item in items {
            mutation_collect_identity_field(item, found)?;
        }
    }
    Ok(())
}

fn mutation_resource_id_value(label: &str, value: &JsonValue) -> Result<String, tonic::Status> {
    let token = if let Some(token) = mutation_resource_id_scalar(value) {
        token
    } else if let Some(eq_value) = mutation_resource_id_eq_value(value) {
        mutation_resource_id_scalar(eq_value).ok_or_else(|| {
            setup_data_internal_status(
                "mutation_resource_uri_equality_scalar",
                format!("mutation response resource_uri {label} equality value must be scalar"),
            )
        })?
    } else {
        return Err(setup_data_internal_status(
            "mutation_resource_uri_scalar_equality_required",
            format!("mutation response resource_uri {label} must be a scalar equality value"),
        ));
    };
    mutation_resource_uri_token(label, &token)
}

fn mutation_resource_id_eq_value(value: &JsonValue) -> Option<&JsonValue> {
    let JsonValue::Object(map) = value else {
        return None;
    };
    if map.len() != 1 {
        return None;
    }
    map.get("$eq").or_else(|| map.get("="))
}

fn mutation_resource_id_scalar(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::Null | JsonValue::Array(_) | JsonValue::Object(_) => None,
        _ => Some(crate::runtime::executor_utils::json_scalar_to_string(value)),
    }
}

fn returned_record_json_or_status(records_json: &[Vec<u8>]) -> Result<Vec<u8>, tonic::Status> {
    records_json.first().cloned().ok_or_else(|| {
        setup_data_internal_status(
            "upsert_returning_record_json",
            "PostgreSQL upsert RETURNING row decoded without record_json",
        )
    })
}

fn idempotency_response_persist_sql(rel: &str) -> String {
    format!(
        "UPDATE {rel}
         SET response_json = $1
         WHERE dedup_key = $2
           AND tenant_id = $3
           AND project_id = $4
           AND message_type = $5
           AND operation = $6"
    )
}

async fn persist_idempotency_response_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dedup_ctx: &IdempotencyPersistContext,
    response: &MutationResponse,
) -> Result<(), tonic::Status> {
    let summary = mutation_response_idempotency_json_for_claim(
        response,
        &dedup_ctx.tenant_id,
        &dedup_ctx.project_id,
        &dedup_ctx.message_type,
    )?;
    let rel = dedup_ctx.config.idempotency_keys_relation();
    let sql = idempotency_response_persist_sql(&rel);
    let result = sqlx::query(&sql)
        .bind(&summary)
        .bind(&dedup_ctx.dedup_key)
        .bind(&dedup_ctx.tenant_id)
        .bind(&dedup_ctx.project_id)
        .bind(&dedup_ctx.message_type)
        .bind(dedup_ctx.operation)
        .execute(&mut **tx)
        .await
        .map_err(|err| {
            crate::runtime::executor_utils::sqlx_error_to_status(
                "idempotency response persist failed",
                &err,
            )
        })?;
    idempotency_response_persist_row_count_status(result.rows_affected())?;
    Ok(())
}

fn idempotency_replay_response_status(message: impl Into<String>) -> tonic::Status {
    setup_data_internal_status(
        "idempotency_replay_response",
        format!("idempotency replay response invalid: {}", message.into()),
    )
}

fn idempotency_replay_string(
    prior: &JsonValue,
    field: &str,
    allow_empty: bool,
) -> Result<String, tonic::Status> {
    let value = prior
        .get(field)
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            idempotency_replay_response_status(format!("missing string field {field}"))
        })?;
    if !allow_empty && value.is_empty() {
        return Err(idempotency_replay_response_status(format!(
            "empty string field {field}"
        )));
    }
    Ok(value.to_string())
}

fn idempotency_replay_i64(prior: &JsonValue, field: &str) -> Result<i64, tonic::Status> {
    let value = prior
        .get(field)
        .and_then(|value| value.as_i64())
        .ok_or_else(|| {
            idempotency_replay_response_status(format!("missing integer field {field}"))
        })?;
    if value < 0 {
        return Err(idempotency_replay_response_status(format!(
            "negative integer field {field}"
        )));
    }
    Ok(value)
}

fn idempotency_replay_mutation_id(prior: &JsonValue) -> Result<String, tonic::Status> {
    let mutation_id = idempotency_replay_string(prior, "mutation_id", false)?;
    let parsed = Uuid::parse_str(&mutation_id)
        .map_err(|err| idempotency_replay_response_status(format!("invalid mutation_id: {err}")))?;
    if parsed.to_string() != mutation_id {
        return Err(idempotency_replay_response_status(
            "mutation_id must be a canonical lowercase UUID",
        ));
    }
    Ok(mutation_id)
}

fn idempotency_replay_checksum(prior: &JsonValue) -> Result<String, tonic::Status> {
    let checksum = idempotency_replay_string(prior, "checksum_sha256", true)?;
    validate_idempotency_sha256_token("checksum_sha256", &checksum, true)?;
    Ok(checksum)
}

fn validate_idempotency_sha256_token(
    field: &str,
    checksum: &str,
    allow_empty: bool,
) -> Result<(), tonic::Status> {
    if checksum.is_empty() {
        if allow_empty {
            return Ok(());
        }
        return Err(idempotency_replay_response_status(format!(
            "{field} must be non-empty"
        )));
    }
    let Some(hex) = checksum.strip_prefix("sha256:") else {
        let message = if allow_empty {
            format!("{field} must be empty or start with sha256:")
        } else {
            format!("{field} must start with sha256:")
        };
        return Err(idempotency_replay_response_status(message));
    };
    if hex.len() != 64
        || !hex
            .chars()
            .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase())
    {
        let message = if allow_empty {
            format!("{field} must be empty or sha256:<64 lowercase hex>")
        } else {
            format!("{field} must be sha256:<64 lowercase hex>")
        };
        return Err(idempotency_replay_response_status(message));
    }
    Ok(())
}

fn idempotency_replay_resource_uri(prior: &JsonValue) -> Result<String, tonic::Status> {
    let resource_uri = idempotency_replay_string(prior, "resource_uri", false)?;
    let _ = idempotency_replay_resource_uri_scope(&resource_uri)?;
    Ok(resource_uri)
}

fn idempotency_replay_resource_uri_scope(
    resource_uri: &str,
) -> Result<(&str, &str), tonic::Status> {
    const PREFIX: &str = "udb://";
    if !resource_uri.starts_with(PREFIX) {
        return Err(idempotency_replay_response_status(
            "resource_uri must start with udb://",
        ));
    }
    if resource_uri.chars().any(char::is_whitespace) {
        return Err(idempotency_replay_response_status(
            "resource_uri must not include whitespace",
        ));
    }
    if resource_uri.chars().any(char::is_control) {
        return Err(idempotency_replay_response_status(
            "resource_uri must not contain control characters",
        ));
    }
    let rest = &resource_uri[PREFIX.len()..];
    let Some((authority, path)) = rest.split_once('/') else {
        return Err(idempotency_replay_response_status(
            "resource_uri must include non-empty authority and path",
        ));
    };
    if authority.is_empty() || path.is_empty() {
        return Err(idempotency_replay_response_status(
            "resource_uri must include non-empty authority and path",
        ));
    }
    let path_segments = path.split('/').collect::<Vec<_>>();
    if path_segments.len() != 2 || path_segments.iter().any(|segment| segment.is_empty()) {
        return Err(idempotency_replay_response_status(
            "resource_uri path must include message type and resource id",
        ));
    }
    Ok((authority, path_segments[0]))
}

fn idempotency_replay_resource_uri_matches_claim(
    resource_uri: &str,
    tenant_id: &str,
    message_type: &str,
) -> Result<(), tonic::Status> {
    let (authority, message) = idempotency_replay_resource_uri_scope(resource_uri)?;
    if authority != tenant_id {
        return Err(idempotency_replay_response_status(
            "resource_uri authority must match idempotency claim tenant_id",
        ));
    }
    if message != message_type {
        return Err(idempotency_replay_response_status(
            "resource_uri message type must match idempotency claim message_type",
        ));
    }
    Ok(())
}

fn idempotency_replay_project_matches_claim(
    prior: &JsonValue,
    project_id: &str,
) -> Result<(), tonic::Status> {
    let stored_project = idempotency_replay_string(prior, "project_id", true)?;
    if stored_project != project_id {
        return Err(idempotency_replay_response_status(
            "project_id must match idempotency claim project_id",
        ));
    }
    Ok(())
}

fn idempotency_replay_scope_matches_claim(
    prior: &JsonValue,
    tenant_id: &str,
    project_id: &str,
    message_type: &str,
) -> Result<(), tonic::Status> {
    let stored_tenant = idempotency_replay_string(prior, "tenant_id", false)?;
    if stored_tenant != tenant_id {
        return Err(idempotency_replay_response_status(
            "tenant_id must match idempotency claim tenant_id",
        ));
    }
    idempotency_replay_project_matches_claim(prior, project_id)?;
    let stored_message = idempotency_replay_string(prior, "message_type", false)?;
    if stored_message != message_type {
        return Err(idempotency_replay_response_status(
            "message_type must match idempotency claim message_type",
        ));
    }
    Ok(())
}

struct IdempotencyJsonNoDuplicateKeys<'a> {
    label: &'a str,
}

struct IdempotencyJsonNoDuplicateKeysVisitor<'a> {
    label: &'a str,
}

impl<'de> serde::de::DeserializeSeed<'de> for IdempotencyJsonNoDuplicateKeys<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_any(IdempotencyJsonNoDuplicateKeysVisitor { label: self.label })
    }
}

impl<'de> serde::de::Visitor<'de> for IdempotencyJsonNoDuplicateKeysVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_string<E>(self, _value: String) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        serde::de::DeserializeSeed::deserialize(
            IdempotencyJsonNoDuplicateKeys { label: self.label },
            deserializer,
        )
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        while seq
            .next_element_seed(IdempotencyJsonNoDuplicateKeys { label: self.label })?
            .is_some()
        {}
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut seen = std::collections::HashSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !seen.insert(key.clone()) {
                return Err(<A::Error as serde::de::Error>::custom(format!(
                    "{} must not contain duplicate JSON key {key:?}",
                    self.label
                )));
            }
            map.next_value_seed(IdempotencyJsonNoDuplicateKeys { label: self.label })?;
        }
        Ok(())
    }
}

fn validate_idempotency_json_no_duplicate_keys(
    label: &str,
    json: &[u8],
) -> Result<(), tonic::Status> {
    let mut deserializer = serde_json::Deserializer::from_slice(json);
    serde::de::DeserializeSeed::deserialize(
        IdempotencyJsonNoDuplicateKeys { label },
        &mut deserializer,
    )
    .map_err(|err| idempotency_replay_response_status(err.to_string()))
}

fn idempotency_replay_record_json(prior: &JsonValue) -> Result<Vec<u8>, tonic::Status> {
    use base64::Engine as _;
    let record_json_b64 = idempotency_replay_string(prior, "record_json", true)?;
    let record_json = base64::engine::general_purpose::STANDARD
        .decode(record_json_b64)
        .map_err(|err| idempotency_replay_response_status(format!("invalid record_json: {err}")))?;
    if record_json.is_empty() {
        return Ok(record_json);
    }
    validate_idempotency_json_no_duplicate_keys("record_json", &record_json)?;
    let value = serde_json::from_slice::<JsonValue>(&record_json).map_err(|err| {
        idempotency_replay_response_status(format!(
            "record_json must be a valid JSON object: {err}"
        ))
    })?;
    let Some(object) = value.as_object() else {
        return Err(idempotency_replay_response_status(
            "record_json must be a JSON object",
        ));
    };
    if object.is_empty() {
        return Err(idempotency_replay_response_status(
            "record_json must be a non-empty JSON object",
        ));
    }
    Ok(record_json)
}

fn validate_idempotency_replay_write_receipt(
    receipt: &crate::runtime::consistency::WriteReceipt,
) -> Result<(), tonic::Status> {
    // `outbox_seq` is `u64`, so non-negativity is a type-level guarantee: the
    // only path into this validator is `serde_json::from_str::<WriteReceipt>`
    // (`idempotency_replay_write_receipt_from_raw`), which rejects a negative
    // JSON integer at decode ("invalid write_receipt_json: invalid value").
    if receipt.written_at_unix_ms <= 0 {
        return Err(idempotency_replay_response_status(
            "write_receipt_json written_at_unix_ms must be positive",
        ));
    }
    if receipt.source_lsn.trim().is_empty() {
        return Err(idempotency_replay_response_status(
            "write_receipt_json source_lsn must be non-empty",
        ));
    }
    if receipt.source_lsn != receipt.source_lsn.trim()
        || receipt.source_lsn.chars().any(char::is_whitespace)
    {
        return Err(idempotency_replay_response_status(
            "write_receipt_json source_lsn must not include whitespace",
        ));
    }
    if receipt.source_lsn.chars().any(char::is_control) {
        return Err(idempotency_replay_response_status(
            "write_receipt_json source_lsn must not contain control characters",
        ));
    }
    if receipt.manifest_checksum.trim().is_empty() {
        return Err(idempotency_replay_response_status(
            "write_receipt_json manifest_checksum must be non-empty",
        ));
    }
    if receipt.manifest_checksum != receipt.manifest_checksum.trim() {
        return Err(idempotency_replay_response_status(
            "write_receipt_json manifest_checksum must not include surrounding whitespace",
        ));
    }
    validate_idempotency_sha256_token(
        "write_receipt_json manifest_checksum",
        &receipt.manifest_checksum,
        false,
    )?;
    for (index, task_id) in receipt.projection_task_ids.iter().enumerate() {
        if task_id.trim().is_empty()
            || task_id != task_id.trim()
            || task_id.chars().any(char::is_whitespace)
        {
            return Err(idempotency_replay_response_status(format!(
                "write_receipt_json projection_task_ids[{index}] must be non-empty and contain no whitespace"
            )));
        }
        if task_id.chars().any(char::is_control) {
            return Err(idempotency_replay_response_status(format!(
                "write_receipt_json projection_task_ids[{index}] must not contain control characters"
            )));
        }
    }
    Ok(())
}

const IDEMPOTENCY_WRITE_RECEIPT_FIELDS: [&str; 5] = [
    "source_lsn",
    "outbox_seq",
    "projection_task_ids",
    "manifest_checksum",
    "written_at_unix_ms",
];

fn validate_idempotency_replay_write_receipt_object(
    write_receipt_json: &str,
) -> Result<(), tonic::Status> {
    let value = serde_json::from_str::<JsonValue>(write_receipt_json).map_err(|err| {
        idempotency_replay_response_status(format!("invalid write_receipt_json: {err}"))
    })?;
    let Some(object) = value.as_object() else {
        return Err(idempotency_replay_response_status(
            "write_receipt_json must be a JSON object",
        ));
    };
    for field in IDEMPOTENCY_WRITE_RECEIPT_FIELDS {
        if !object.contains_key(field) {
            return Err(idempotency_replay_response_status(format!(
                "write_receipt_json missing field {field}"
            )));
        }
    }
    for field in object.keys() {
        if !IDEMPOTENCY_WRITE_RECEIPT_FIELDS.contains(&field.as_str()) {
            return Err(idempotency_replay_response_status(format!(
                "write_receipt_json unexpected field {field}"
            )));
        }
    }
    Ok(())
}

fn idempotency_replay_write_receipt_from_raw(
    raw: &str,
) -> Result<crate::runtime::consistency::WriteReceipt, tonic::Status> {
    let write_receipt = serde_json::from_str::<crate::runtime::consistency::WriteReceipt>(raw)
        .map_err(|err| {
            idempotency_replay_response_status(format!("invalid write_receipt_json: {err}"))
        })?;
    validate_idempotency_replay_write_receipt(&write_receipt)?;
    Ok(write_receipt)
}

fn idempotency_replay_write_receipt(
    prior: &JsonValue,
) -> Result<(String, crate::runtime::consistency::WriteReceipt), tonic::Status> {
    let write_receipt_json = idempotency_replay_string(prior, "write_receipt_json", false)?;
    if write_receipt_json != write_receipt_json.trim() {
        return Err(idempotency_replay_response_status(
            "write_receipt_json must not include surrounding whitespace",
        ));
    }
    validate_idempotency_json_no_duplicate_keys(
        "write_receipt_json",
        write_receipt_json.as_bytes(),
    )?;
    validate_idempotency_replay_write_receipt_object(&write_receipt_json)?;
    let write_receipt = idempotency_replay_write_receipt_from_raw(&write_receipt_json)?;
    Ok((write_receipt_json, write_receipt))
}

/// Reconstruct a `MutationResponse` (with `was_duplicate = true`) from a dedup
/// row's stored summary on the replay path. Corrupt/incomplete summaries fail
/// closed instead of returning a misleading empty duplicate response.
fn mutation_response_from_idempotency_json(
    prior: &JsonValue,
) -> Result<MutationResponse, tonic::Status> {
    let (write_receipt_json, write_receipt) = idempotency_replay_write_receipt(prior)?;
    Ok(MutationResponse {
        mutation_id: idempotency_replay_mutation_id(prior)?,
        resource_uri: idempotency_replay_resource_uri(prior)?,
        checksum_sha256: idempotency_replay_checksum(prior)?,
        record_json: idempotency_replay_record_json(prior)?,
        affected_rows: idempotency_replay_i64(prior, "affected_rows")?,
        was_duplicate: true,
        write_receipt_json,
        write_receipt: Some(write_receipt.to_proto()),
        ..MutationResponse::default()
    })
}

fn mutation_response_from_idempotency_json_for_claim(
    prior: &JsonValue,
    tenant_id: &str,
    project_id: &str,
    message_type: &str,
) -> Result<MutationResponse, tonic::Status> {
    idempotency_replay_scope_matches_claim(prior, tenant_id, project_id, message_type)?;
    let response = mutation_response_from_idempotency_json(prior)?;
    idempotency_replay_resource_uri_matches_claim(&response.resource_uri, tenant_id, message_type)?;
    Ok(response)
}

/// Add convention/env-derived Tier 4+ runtime instances that are not expressible
/// as legacy top-level config blocks. Explicit file/env `backend_instances`
/// remain authoritative: if the operator declared any instance for a backend,
/// this helper does not add a default for that backend.
fn merge_runtime_env_backend_instances(instance_config: &mut BackendInstanceConfig) {
    let env_instances = BackendInstanceConfig::from_env();
    merge_runtime_backend_instances(instance_config, env_instances);
    instance_config.resolve_env_dsns();
}

fn merge_runtime_backend_instances(
    instance_config: &mut BackendInstanceConfig,
    env_instances: BackendInstanceConfig,
) {
    for env_instance in env_instances.instances {
        let Some(kind) = env_instance.canonical_backend() else {
            continue;
        };
        if !matches!(
            kind,
            crate::backend::BackendKind::Mongodb | crate::backend::BackendKind::Clickhouse
        ) {
            continue;
        }
        if !env_instance.enabled || !env_instance.is_configured() {
            continue;
        }
        if instance_config
            .instances
            .iter()
            .any(|instance| instance.canonical_backend() == Some(kind.clone()))
        {
            continue;
        }
        instance_config.instances.push(env_instance);
    }
}

// ── Per-backend register functions (U2 step 3) ────────────────────────────────
//
// Each `register_*` mirrors the inline setup block it replaced. The plugin's
// `register` method just calls the matching function here; from_config drives
// the whole list through `for plugin in all_plugins() { plugin.register(ctx) }`.

const PROJECTION_SYSTEM_STORE_OPT_IN_ENV: &str = "UDB_ALLOW_PROJECTION_SYSTEM_STORE";

fn projection_system_store_opt_in_enabled() -> bool {
    projection_system_store_opt_in_value(std::env::var(PROJECTION_SYSTEM_STORE_OPT_IN_ENV).ok())
}

fn projection_system_store_opt_in_value(value: Option<String>) -> bool {
    matches!(value.as_deref().map(str::trim), Some("1"))
}

fn full_canonical_store_requires_opt_in(kind: &crate::backend::BackendKind) -> bool {
    !kind.role().can_host_system_tables() || matches!(kind, crate::backend::BackendKind::Clickhouse)
}

fn ensure_full_canonical_store_registration_allowed(
    kind: crate::backend::BackendKind,
) -> Result<(), String> {
    if !full_canonical_store_requires_opt_in(&kind) || projection_system_store_opt_in_enabled() {
        return Ok(());
    }
    Err(format!(
        "{kind:?} canonical SystemStores registration refused: backend role is '{}' \
         and/or the store has a single-writer caveat; set {PROJECTION_SYSTEM_STORE_OPT_IN_ENV}=1 \
         to opt in explicitly",
        kind.role().as_str()
    ))
}

async fn ensure_full_system_store_tables<S>(store: &S) -> Result<(), String>
where
    S: crate::runtime::canonical_store::SystemStores + ?Sized,
{
    crate::runtime::canonical_store::CanonicalStore::ensure_system_tables(store).await?;
    crate::runtime::canonical_store::system_store::ProjectionTaskStore::ensure_projection_tables(
        store,
    )
    .await
    .map_err(|err| format!("ensure_projection_tables failed: {err}"))?;
    crate::runtime::canonical_store::system_store::SagaStore::ensure_saga_tables(store)
        .await
        .map_err(|err| format!("ensure_saga_tables failed: {err}"))?;
    crate::runtime::canonical_store::system_store::AdminAuditStore::ensure_admin_audit_tables(
        store,
    )
    .await
    .map_err(|err| format!("ensure_admin_audit_tables failed: {err}"))?;
    crate::runtime::canonical_store::system_store::MigrationAuditStore::ensure_migration_audit_tables(
        store,
    )
    .await
    .map_err(|err| format!("ensure_migration_audit_tables failed: {err}"))?;
    Ok(())
}

fn pg_outbox_receipt_store_mismatch(
    pg_write_path_configured: bool,
    default_store: Option<(&str, &str)>,
) -> Option<String> {
    if !pg_write_path_configured {
        return None;
    }
    let Some((backend, instance)) = default_store else {
        return None;
    };
    if backend.eq_ignore_ascii_case("postgres") && instance == "primary" {
        return None;
    }
    Some(format!(
        "production outbox consistency assertion failed: native services write directly to the \
         primary Postgres outbox, but write receipts would read outbox_max_seq from \
         {backend}:{instance}; configure the primary Postgres SystemStores as the default or \
         disable the PG direct outbox path"
    ))
}

fn assert_pg_outbox_receipt_store_consistency(runtime: &DataBrokerRuntime) {
    let default_store = runtime.default_system_stores();
    let default_store_labels = default_store.as_ref().map(|store| {
        (
            crate::runtime::canonical_store::CanonicalStore::backend_label(store.as_ref()),
            crate::runtime::canonical_store::CanonicalStore::instance_name(store.as_ref()),
        )
    });
    if let Some(message) =
        pg_outbox_receipt_store_mismatch(runtime.pg_pool.is_some(), default_store_labels)
    {
        panic!("{message}");
    }
}

/// Operator-declared deployment tier (`UDB_DEPLOYMENT_TIER`), resolved EXACTLY
/// once (master-plan 3.5). `None` = no tier declared, so the startup tier floor
/// is not enforced (the dev default). An unrecognised non-empty value is a hard
/// startup failure — the operator clearly intended to declare a tier.
pub(crate) fn declared_deployment_tier() -> Option<crate::backend::ControlPlaneHaLevel> {
    static DECLARED: std::sync::OnceLock<Option<crate::backend::ControlPlaneHaLevel>> =
        std::sync::OnceLock::new();
    *DECLARED.get_or_init(|| {
        let raw = std::env::var("UDB_DEPLOYMENT_TIER").ok()?;
        if raw.trim().is_empty() {
            return None;
        }
        match crate::backend::ControlPlaneHaLevel::parse_deployment_tier(&raw) {
            Some(tier) => Some(tier),
            // Fail-closed: a typo'd tier must not silently degrade to "no floor".
            None => panic!(
                "UDB_DEPLOYMENT_TIER='{}' is not a recognised deployment tier \
                 (expected one of dev_single_node|system_store_capable|ha_canonical)",
                raw.trim()
            ),
        }
    })
}

/// Pure deployment-tier floor check (master-plan 3.5). Returns the registered
/// canonical stores whose control-plane HA level is BELOW the operator-declared
/// deployment tier. Mirrors `SecurityConfig::validate_compliance_profile`'s
/// shape — `Ok(())` when every store satisfies the floor, else the
/// human-readable violations. No I/O, no env reads; unit-testable in isolation.
fn validate_deployment_tier_floor(
    declared: crate::backend::ControlPlaneHaLevel,
    registered: &[(String, crate::backend::ControlPlaneHaLevel)],
) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();
    for (label, level) in registered {
        if *level < declared {
            errors.push(format!(
                "canonical store '{label}' has control-plane HA level '{}', below the declared \
                 deployment tier '{}' (UDB_DEPLOYMENT_TIER) — refusing to start",
                level.as_str(),
                declared.as_str(),
            ));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Startup deployment-tier gate (master-plan 3.5). When the operator declares a
/// minimum `UDB_DEPLOYMENT_TIER`, refuse to start if any canonical store
/// registered below it — a misconfiguration must fail at boot, not at 3am.
/// Fail-closed: like [`assert_pg_outbox_receipt_store_consistency`] above, a
/// violation aborts runtime construction rather than degrading silently. The
/// per-backend classification is read from the single source of truth,
/// `BackendKind::control_plane_ha_level()`.
fn assert_deployment_tier_floor(runtime: &DataBrokerRuntime) {
    let Some(declared) = declared_deployment_tier() else {
        return;
    };
    let registered_levels: Vec<(String, crate::backend::ControlPlaneHaLevel)> = runtime
        .canonical_stores
        .lock()
        .map(|stores| {
            stores
                .registered_keys()
                .into_iter()
                .filter_map(|(label, instance)| {
                    crate::backend::BackendKind::from_store_kind("", &label)
                        .or_else(|| crate::backend::BackendKind::from_token(&label))
                        .map(|kind| (format!("{label}:{instance}"), kind.control_plane_ha_level()))
                })
                .collect()
        })
        .unwrap_or_default();
    if let Err(violations) = validate_deployment_tier_floor(declared, &registered_levels) {
        panic!(
            "deployment tier '{}' not satisfied at startup: {}",
            declared.as_str(),
            violations.join("; ")
        );
    }
}

#[cfg(test)]
mod setup_data_validation_tests {
    use super::{
        empty_object_stream_status, gcs_feature_status, invalid_part_count_status,
        invalid_presign_ttl_status, json_values_match, no_object_store_feature_status,
        object_instance_missing_status, qdrant_vector_feature_status, s3_object_feature_status,
        setup_data_internal_status, unknown_message_type_status, unsupported_object_backend_status,
        unsupported_presign_method_status, vector_hybrid_qdrant_only_status,
        vector_search_dispatch_spec, vector_upsert_dispatch_spec,
    };
    use crate::proto::{ErrorDetail, ErrorKind, VectorPointMutation, VectorSearchRequest};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

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

    fn assert_internal_detail(status: &tonic::Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "setup_data");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn go005_cas_value_match_is_numeric_coercing() {
        use serde_json::json;
        // The int-vs-float trap: a Struct-carried assertion arrives as f64, the
        // decoded row column as an integer — they must still compare equal.
        assert!(json_values_match(&json!(8), &json!(8.0)));
        assert!(json_values_match(&json!(8.0), &json!(8)));
        assert!(json_values_match(&json!(7), &json!(7)));
        // Genuine mismatches (the losing CAS writer) must NOT match.
        assert!(!json_values_match(&json!(7), &json!(8)));
        assert!(!json_values_match(&json!(7), &json!(8.0)));
        // Non-numeric values fall back to exact equality.
        assert!(json_values_match(&json!("ACTIVE"), &json!("ACTIVE")));
        assert!(!json_values_match(&json!("ACTIVE"), &json!("RETIRED")));
        assert!(json_values_match(&json!(true), &json!(true)));
        assert!(!json_values_match(&json!(true), &json!(false)));
        // A present value never matches an absent (null) assertion target.
        assert!(!json_values_match(&serde_json::Value::Null, &json!(1)));
    }

    #[test]
    fn setup_data_boundary_validation_carries_field_violations() {
        assert_single_field_violation(
            &unknown_message_type_status(),
            "message_type",
            "must match a manifest table message type",
        );
        assert_single_field_violation(
            &empty_object_stream_status(),
            "stream",
            "object upload stream must contain at least one chunk",
        );
        assert_single_field_violation(
            &unsupported_presign_method_status(),
            "method",
            "presigned URLs support only PUT or GET",
        );
        assert_single_field_violation(
            &invalid_part_count_status(),
            "part_count",
            "multipart upload part_count must be positive",
        );
        assert_single_field_violation(
            &invalid_presign_ttl_status("too long"),
            "ttl_seconds",
            "must produce a valid presign expiration",
        );
    }

    #[test]
    fn setup_data_vector_object_capability_refusals_carry_detail() {
        assert_capability_detail(
            &qdrant_vector_feature_status("vector_search"),
            "qdrant",
            "vector_search",
            "qdrant_feature",
            "qdrant/vector feature is not enabled",
        );
        assert_capability_detail(
            &vector_hybrid_qdrant_only_status("pinecone"),
            "qdrant",
            "vector_hybrid_search",
            "qdrant_backend",
            "vector hybrid search is only wired for qdrant, not 'pinecone'",
        );
        assert_capability_detail(
            &no_object_store_feature_status("put_object"),
            "object_store",
            "put_object",
            "object_store_feature",
            "no object-store feature (s3/gcs/azureblob) is enabled",
        );
        assert_capability_detail(
            &s3_object_feature_status("generate_presigned_url"),
            "s3",
            "generate_presigned_url",
            "s3_feature",
            "s3/object-store feature is not enabled",
        );
        assert_capability_detail(
            &gcs_feature_status("get_object"),
            "gcs",
            "get_object",
            "gcs_feature",
            "gcs feature is not enabled",
        );
        assert_capability_detail(
            &object_instance_missing_status("gcs", "put_object", "archive"),
            "gcs",
            "put_object",
            "configured_instance",
            "gcs instance 'archive' is not configured",
        );
        assert_capability_detail(
            &unsupported_object_backend_status("put_object", "postgres"),
            "postgres",
            "put_object",
            "supported_object_backend",
            "unsupported object backend 'postgres'",
        );
    }

    #[test]
    fn setup_data_typed_dispatch_backend_refusals_carry_capability_detail() {
        let vector_search = vector_search_dispatch_spec(
            "redis",
            &VectorSearchRequest {
                context: None,
                collection: "Docs".to_string(),
                vector: vec![0.1, 0.2],
                filter: None,
                limit: 10,
                score_threshold: 0.0,
                with_payload: false,
                with_vector: false,
                vector_name: String::new(),
                quantization_rescore: false,
            },
        )
        .expect_err("unsupported typed vector backend must fail closed");
        assert_capability_detail(
            &vector_search,
            "redis",
            "typed_vector_search",
            "typed_vector_search_backend",
            "typed vector search is not wired for backend 'redis'",
        );

        let vector_upsert = vector_upsert_dispatch_spec(
            "redis",
            "Docs",
            &VectorPointMutation {
                id: "p1".to_string(),
                vector: vec![0.1, 0.2],
                payload: None,
                vector_name: String::new(),
            },
        )
        .expect_err("unsupported typed vector upsert backend must fail closed");
        assert_capability_detail(
            &vector_upsert,
            "redis",
            "typed_vector_upsert",
            "typed_vector_upsert_backend",
            "typed vector upsert is not wired for backend 'redis'",
        );
    }

    #[test]
    fn setup_data_internal_status_carries_typed_detail() {
        assert_internal_detail(
            &setup_data_internal_status("select_query", "PostgreSQL select failed: broken"),
            "select_query",
            "PostgreSQL select failed: broken",
        );
        assert_internal_detail(
            &setup_data_internal_status(
                "idempotency_replay_response",
                "idempotency replay response invalid: broken",
            ),
            "idempotency_replay_response",
            "idempotency replay response invalid: broken",
        );
    }

    #[cfg(any(feature = "s3", feature = "gcs", feature = "azureblob"))]
    #[test]
    fn setup_data_typed_object_backend_refusal_carries_capability_detail() {
        assert_capability_detail(
            &super::typed_object_backend_required_status("postgres"),
            "postgres",
            "typed_object_rpc",
            "object_store_backend",
            "typed object RPCs require an object-store backend, but the store is configured for 'postgres'",
        );
    }
}

#[cfg(test)]
mod setup_data_consistency_tests {
    use super::{
        RequestContext, full_canonical_store_requires_opt_in, idempotency_claim_sql,
        idempotency_dedup_claim_status, idempotency_dedup_key, idempotency_key_for_dedup,
        idempotency_response_persist_row_count_status, idempotency_response_persist_sql,
        merge_runtime_backend_instances, mutation_response_from_idempotency_json,
        mutation_response_from_idempotency_json_for_claim, mutation_response_idempotency_json,
        mutation_response_resource_uri, mutation_response_resource_uri_or_fallback,
        pg_outbox_receipt_store_mismatch, projection_system_store_opt_in_value,
        returned_record_json_or_status, validate_deployment_tier_floor,
        write_receipt_json_or_status,
    };
    use crate::backend::ControlPlaneHaLevel;
    use crate::proto::{ErrorDetail, ErrorKind, MutationResponse};
    use crate::runtime::config::{BackendInstance, BackendInstanceConfig, BackendInstanceRole};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use base64::Engine as _;

    fn decode_error_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed error detail trailer");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    #[test]
    fn deployment_tier_floor_refuses_stores_below_declared_tier() {
        // A declared HA-canonical tier accepts HA-canonical stores (incl. the
        // decision-welded ClickHouse + vector stores) but refuses a dev/embedded
        // store — fail at boot, not at 3am.
        let registered = vec![
            (
                "postgres:primary".to_string(),
                ControlPlaneHaLevel::HaCanonical,
            ),
            (
                "clickhouse:analytics".to_string(),
                ControlPlaneHaLevel::HaCanonical,
            ),
            (
                "qdrant:vectors".to_string(),
                ControlPlaneHaLevel::HaCanonical,
            ),
            (
                "sqlite:local".to_string(),
                ControlPlaneHaLevel::DevSingleNode,
            ),
        ];

        // HA-canonical floor: the sqlite store violates it.
        let err = validate_deployment_tier_floor(ControlPlaneHaLevel::HaCanonical, &registered)
            .expect_err("sqlite must be refused below an HA-canonical floor");
        assert_eq!(err.len(), 1, "only the sqlite store is below the floor");
        assert!(err[0].contains("sqlite:local"));
        assert!(err[0].contains("ha_canonical"));

        // SystemStoreCapable floor: still refuses the dev_single_node sqlite store.
        assert!(
            validate_deployment_tier_floor(ControlPlaneHaLevel::SystemStoreCapable, &registered)
                .is_err()
        );

        // DevSingleNode floor: every store now satisfies it (>= dev_single_node).
        assert!(
            validate_deployment_tier_floor(ControlPlaneHaLevel::DevSingleNode, &registered).is_ok()
        );

        // Empty registry trivially satisfies any declared tier.
        assert!(validate_deployment_tier_floor(ControlPlaneHaLevel::HaCanonical, &[]).is_ok());
    }

    #[test]
    fn idempotency_dedup_key_is_tenant_and_project_scoped() {
        // KEYSTONE (lane 05): the salted dedup key MUST differ across tenants and
        // projects for the same client key, and across operation names for the
        // same entity, or independent mutation RPCs could replay each other.
        // Verifies 05.1.4.2 / the scoping guarantee asserted served-path by
        // 05.6.2.1.
        let key = "key-1";
        let mt = "Payment";
        let op = "upsert";
        let a = idempotency_dedup_key("tenant-a", "proj-1", mt, op, key);
        let b = idempotency_dedup_key("tenant-b", "proj-1", mt, op, key);
        let c = idempotency_dedup_key("tenant-a", "proj-2", mt, op, key);
        let d = idempotency_dedup_key("tenant-a", "proj-1", "Invoice", op, key);
        let e = idempotency_dedup_key("tenant-a", "proj-1", mt, "delete", key);
        let f = idempotency_dedup_key("tenant-a", "proj-1", mt, op, " key-1 ");
        assert_ne!(a, b, "distinct tenants must not collide");
        assert_ne!(a, c, "distinct projects must not collide");
        assert_ne!(a, d, "distinct message types must not collide");
        assert_ne!(a, e, "distinct operations must not collide");
        assert_ne!(a, f, "caller key bytes must not be trim-normalized");
        // Same inputs are stable (deterministic) and never the bare client key.
        assert_eq!(a, idempotency_dedup_key("tenant-a", "proj-1", mt, op, key));
        assert!(a.starts_with("sha256:"));
        assert!(!a.contains(key), "must never embed the bare client key");
        // A NUL-boundary collision guard: ("a","bc",..) vs ("ab","c",..) must
        // not hash equal (the \0 separators prevent field-shifting collisions).
        let shift1 = idempotency_dedup_key("a", "bc", mt, op, key);
        let shift2 = idempotency_dedup_key("ab", "c", mt, op, key);
        assert_ne!(
            shift1, shift2,
            "NUL separators must prevent field-shift collisions"
        );
    }

    #[test]
    fn idempotency_key_for_dedup_rejects_whitespace_and_control_tokens() {
        assert_eq!(
            idempotency_key_for_dedup("").expect("empty key is keyless"),
            None
        );
        assert_eq!(
            idempotency_key_for_dedup("key-1").expect("canonical key is accepted"),
            Some("key-1")
        );

        for key in [
            " ",
            " key-1",
            "key-1 ",
            "key 1",
            "key\t1",
            "key\n1",
            "\0",
            "key\0",
            "key\u{0007}1",
        ] {
            let err = idempotency_key_for_dedup(key)
                .expect_err("invalid idempotency key token must fail closed");
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
            assert!(err.message().contains("idempotency_key"));
            assert!(err.message().contains("whitespace"));
            assert!(err.message().contains("control"));
            let detail = decode_error_detail(&err);
            assert_eq!(detail.kind, ErrorKind::Validation as i32);
            assert!(!detail.retryable);
            assert_eq!(detail.field_violations.len(), 1);
            assert_eq!(detail.field_violations[0].field, "idempotency_key");
            assert_eq!(
                detail.field_violations[0].description,
                "must be empty or contain no whitespace or control characters"
            );
        }
    }

    fn resource_uri_test_table() -> crate::generation::ManifestTable {
        crate::generation::ManifestTable {
            message_name: "Invoice".to_string(),
            primary_key: vec!["invoice_id".to_string()],
            columns: vec![
                crate::generation::ManifestColumn {
                    field_name: "invoiceId".to_string(),
                    column_name: "invoice_id".to_string(),
                    is_primary: true,
                    ..crate::generation::ManifestColumn::default()
                },
                crate::generation::ManifestColumn {
                    field_name: "tenant_id".to_string(),
                    column_name: "tenant_id".to_string(),
                    is_tenant_column: true,
                    ..crate::generation::ManifestColumn::default()
                },
                crate::generation::ManifestColumn {
                    field_name: "project_id".to_string(),
                    column_name: "project_id".to_string(),
                    is_project_column: true,
                    ..crate::generation::ManifestColumn::default()
                },
                crate::generation::ManifestColumn {
                    field_name: "status".to_string(),
                    column_name: "status".to_string(),
                    ..crate::generation::ManifestColumn::default()
                },
            ],
            ..crate::generation::ManifestTable::default()
        }
    }

    #[test]
    fn mutation_response_resource_uri_uses_data_plane_identity() {
        let table = resource_uri_test_table();
        let context = RequestContext {
            tenant_id: "tenant-a".to_string(),
            project_id: "project-a".to_string(),
            ..RequestContext::default()
        };

        let from_column = mutation_response_resource_uri(
            &context,
            "Invoice",
            &table,
            &serde_json::json!({"invoice_id": "inv-1", "tenant_id": "tenant-a"}),
        )
        .expect("primary key column should build data-plane URI");
        assert_eq!(from_column, "udb://tenant-a/Invoice/inv-1");

        let from_field_alias = mutation_response_resource_uri(
            &context,
            "Invoice",
            &table,
            &serde_json::json!({"invoiceId": "inv-2", "tenant_id": "tenant-a"}),
        )
        .expect("primary key field alias should build data-plane URI");
        assert_eq!(from_field_alias, "udb://tenant-a/Invoice/inv-2");

        let from_eq_filter = mutation_response_resource_uri(
            &context,
            "Invoice",
            &table,
            &serde_json::json!({"invoice_id": {"$eq": "inv-3"}, "tenant_id": "tenant-a"}),
        )
        .expect("primary key equality filter should build data-plane URI");
        assert_eq!(from_eq_filter, "udb://tenant-a/Invoice/inv-3");

        let from_and_filter = mutation_response_resource_uri(
            &context,
            "Invoice",
            &table,
            &serde_json::json!({
                "and": [
                    {"tenant_id": "tenant-a"},
                    {"invoiceId": {"=": "inv-4"}}
                ]
            }),
        )
        .expect("primary key equality inside AND filter should build data-plane URI");
        assert_eq!(from_and_filter, "udb://tenant-a/Invoice/inv-4");
    }

    #[test]
    fn mutation_response_resource_uri_falls_back_to_identity_fields() {
        let table = crate::generation::ManifestTable::default();
        let context = RequestContext {
            tenant_id: "tenant-a".to_string(),
            ..RequestContext::default()
        };
        let uri = mutation_response_resource_uri(
            &context,
            "Invoice",
            &table,
            &serde_json::json!({
                "tenant_id": "tenant-a",
                "customer_id": "cust-1",
                "total_cents": 42
            }),
        )
        .expect("identity field should build data-plane URI");
        assert_eq!(uri, "udb://tenant-a/Invoice/cust-1");

        let and_uri = mutation_response_resource_uri(
            &context,
            "Invoice",
            &table,
            &serde_json::json!({
                "and": [
                    {"tenant_id": "tenant-a"},
                    {"customer_id": {"$eq": "cust-2"}}
                ]
            }),
        )
        .expect("identity field inside AND filter should build data-plane URI");
        assert_eq!(and_uri, "udb://tenant-a/Invoice/cust-2");
    }

    #[test]
    fn mutation_response_resource_uri_rejects_ambiguous_identity_tokens() {
        let table = resource_uri_test_table();
        let context = RequestContext {
            tenant_id: "tenant-a".to_string(),
            ..RequestContext::default()
        };

        for record in [
            serde_json::json!({"invoice_id": ""}),
            serde_json::json!({"invoice_id": " inv-1 "}),
            serde_json::json!({"invoice_id": "inv 1"}),
            serde_json::json!({"invoice_id": "inv\u{0000}1"}),
            serde_json::json!({"invoice_id": {"$in": ["inv-1"]}}),
            serde_json::json!({"invoice_id": {"eq": "inv-1"}}),
            serde_json::json!({"or": [{"invoice_id": "inv-1"}, {"invoice_id": "inv-2"}]}),
            serde_json::json!({
                "and": [
                    {"customer_id": "cust-1"},
                    {"account_id": "acct-1"}
                ]
            }),
        ] {
            let err = mutation_response_resource_uri(&context, "Invoice", &table, &record)
                .expect_err("ambiguous resource identity must fail closed");
            assert_eq!(err.code(), tonic::Code::Internal);
            assert!(err.message().contains("mutation response resource_uri"));
        }
    }

    #[test]
    fn mutation_response_resource_uri_fallback_is_keyless_only() {
        let table = resource_uri_test_table();
        let context = RequestContext {
            tenant_id: "tenant-a".to_string(),
            ..RequestContext::default()
        };
        let bulk_filter = serde_json::json!({
            "tenant_id": "tenant-a",
            "project_id": "project-a"
        });

        let keyless = mutation_response_resource_uri_or_fallback(
            &context,
            "Invoice",
            &table,
            &bulk_filter,
            "sql://billing/invoices",
            false,
        )
        .expect("keyless bulk delete may keep planner resource URI");
        assert_eq!(keyless, "sql://billing/invoices");

        let keyed = mutation_response_resource_uri_or_fallback(
            &context,
            "Invoice",
            &table,
            &bulk_filter,
            "sql://billing/invoices",
            true,
        )
        .expect_err("keyed first-writer summary must require data-plane resource URI");
        assert_eq!(keyed.code(), tonic::Code::Internal);
        assert!(keyed.message().contains("mutation response resource_uri"));
    }

    #[test]
    fn idempotency_claim_sql_suppresses_replay_arm_after_insert() {
        let sql = idempotency_claim_sql("udb_idempotency_keys");
        assert!(sql.contains("WITH ins AS ("));
        assert!(sql.contains("ON CONFLICT (dedup_key) DO NOTHING"));
        assert!(sql.contains("RETURNING true AS inserted, response_json"));
        assert!(sql.contains("SELECT false AS inserted, response_json"));
        assert!(sql.contains("AND NOT EXISTS (SELECT 1 FROM ins)"));
        assert!(
            sql.find("RETURNING true AS inserted, response_json")
                < sql.find("AND NOT EXISTS (SELECT 1 FROM ins)"),
            "fresh insert claim must suppress the fallback replay arm"
        );
    }

    #[test]
    fn idempotency_dedup_claim_status_is_retryable_unavailable() {
        let status = idempotency_dedup_claim_status(&sqlx::Error::RowNotFound);
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(
            status.message().contains("idempotency dedup claim failed"),
            "status must identify the idempotency dedup subsystem"
        );
        let detail = decode_error_detail(&status);
        assert_eq!(detail.backend, "postgres");
        assert_eq!(detail.operation, "idempotency_dedup_claim");
        assert_eq!(detail.kind, ErrorKind::Retryable as i32);
        assert!(detail.retryable);
        assert_eq!(detail.retry_after_ms, 250);
    }

    #[test]
    fn idempotency_replay_response_restores_first_writer_summary() {
        let encoded_record =
            base64::engine::general_purpose::STANDARD.encode(br#"{"id":"rec-1","v":7}"#);
        let prior = serde_json::json!({
            "mutation_id": "11111111-1111-4111-8111-111111111111",
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "message_type": "Payment",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": encoded_record,
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[\"p1\"],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });

        let replay =
            mutation_response_from_idempotency_json(&prior).expect("stored replay summary decodes");
        assert!(replay.was_duplicate, "replays must be marked duplicate");
        assert_eq!(replay.mutation_id, "11111111-1111-4111-8111-111111111111");
        assert_eq!(replay.resource_uri, "udb://tenant-a/Payment/rec-1");
        assert_eq!(
            replay.checksum_sha256,
            "sha256:1111111111111111111111111111111111111111111111111111111111111111"
        );
        assert_eq!(replay.record_json, br#"{"id":"rec-1","v":7}"#);
        assert_eq!(replay.affected_rows, 1);
        assert_eq!(
            replay.write_receipt_json,
            "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[\"p1\"],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        );
        let typed_receipt = replay
            .write_receipt
            .as_ref()
            .map(crate::runtime::consistency::WriteReceipt::from_proto)
            .expect("replay should restore typed write_receipt from stored JSON");
        assert_eq!(typed_receipt.source_lsn, "0/1A2B");
        assert_eq!(typed_receipt.outbox_seq, 9);
        assert_eq!(typed_receipt.projection_task_ids, vec!["p1".to_string()]);
    }

    #[test]
    fn idempotency_response_json_roundtrips_full_mutation_response() {
        let receipt = crate::runtime::consistency::WriteReceipt {
            source_lsn: "0/2B3C".to_string(),
            outbox_seq: 12,
            projection_task_ids: vec!["projection-1".to_string()],
            manifest_checksum:
                "sha256:2222222222222222222222222222222222222222222222222222222222222222"
                    .to_string(),
            written_at_unix_ms: 1_700_000_000_123,
        };
        let receipt_json = serde_json::to_string(&receipt).expect("receipt JSON serializes");
        let first = MutationResponse {
            mutation_id: "22222222-2222-4222-8222-222222222222".to_string(),
            resource_uri: "udb://tenant-a/Payment/batch-1".to_string(),
            checksum_sha256:
                "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
            record_json: br#"{"id":"batch-1","amount":42}"#.to_vec(),
            affected_rows: 1,
            was_duplicate: false,
            write_receipt_json: receipt_json.clone(),
            write_receipt: Some(receipt.to_proto()),
            ..MutationResponse::default()
        };

        let stored = mutation_response_idempotency_json(&first)
            .expect("first-writer idempotency summary is replay-decodable");
        let replay = mutation_response_from_idempotency_json(&stored)
            .expect("stored replay summary decodes");

        assert!(
            replay.was_duplicate,
            "stored replays must be marked duplicate"
        );
        assert_eq!(replay.mutation_id, first.mutation_id);
        assert_eq!(replay.resource_uri, first.resource_uri);
        assert_eq!(replay.checksum_sha256, first.checksum_sha256);
        assert_eq!(replay.record_json, first.record_json);
        assert_eq!(replay.affected_rows, first.affected_rows);
        assert_eq!(replay.write_receipt_json, receipt_json);
        let typed_receipt = replay
            .write_receipt
            .as_ref()
            .map(crate::runtime::consistency::WriteReceipt::from_proto)
            .expect("replay should restore typed write_receipt from stored JSON");
        assert_eq!(typed_receipt.source_lsn, receipt.source_lsn);
        assert_eq!(typed_receipt.outbox_seq, receipt.outbox_seq);
        assert_eq!(
            typed_receipt.projection_task_ids,
            receipt.projection_task_ids
        );
        assert_eq!(typed_receipt.manifest_checksum, receipt.manifest_checksum);
        assert_eq!(typed_receipt.written_at_unix_ms, receipt.written_at_unix_ms);
    }

    #[test]
    fn idempotency_replay_response_is_claim_scoped() {
        let encoded_record =
            base64::engine::general_purpose::STANDARD.encode(br#"{"id":"rec-1","v":7}"#);
        let prior = serde_json::json!({
            "mutation_id": "11111111-1111-4111-8111-111111111111",
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "message_type": "Payment",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": encoded_record,
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });

        let replay = mutation_response_from_idempotency_json_for_claim(
            &prior,
            "tenant-a",
            "project-a",
            "Payment",
        )
        .expect("stored replay summary matches the dedup claim scope");
        assert!(
            replay.was_duplicate,
            "claim-scoped replay remains duplicate"
        );

        let wrong_tenant = mutation_response_from_idempotency_json_for_claim(
            &prior,
            "tenant-b",
            "project-a",
            "Payment",
        )
        .expect_err("stored replay summary with wrong tenant must fail closed");
        assert_eq!(wrong_tenant.code(), tonic::Code::Internal);
        assert!(
            wrong_tenant
                .message()
                .contains("tenant_id must match idempotency claim tenant_id")
        );

        let wrong_project = mutation_response_from_idempotency_json_for_claim(
            &prior,
            "tenant-a",
            "project-b",
            "Payment",
        )
        .expect_err("stored replay summary with wrong project must fail closed");
        assert_eq!(wrong_project.code(), tonic::Code::Internal);
        assert!(
            wrong_project
                .message()
                .contains("project_id must match idempotency claim project_id")
        );

        let wrong_message = mutation_response_from_idempotency_json_for_claim(
            &prior,
            "tenant-a",
            "project-a",
            "Invoice",
        )
        .expect_err("stored replay summary with wrong message type must fail closed");
        assert_eq!(wrong_message.code(), tonic::Code::Internal);
        assert!(
            wrong_message
                .message()
                .contains("message_type must match idempotency claim message_type")
        );
    }

    #[test]
    fn idempotency_response_json_must_be_replay_decodable_before_persist() {
        let bad = MutationResponse {
            mutation_id: "33333333-3333-4333-8333-333333333333".to_string(),
            resource_uri: "udb://tenant-a/Payment/rec-1".to_string(),
            checksum_sha256:
                "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
            record_json: br#"{"id":"rec-1"}"#.to_vec(),
            affected_rows: 1,
            was_duplicate: false,
            write_receipt_json: "not-json".to_string(),
            ..MutationResponse::default()
        };

        let err = mutation_response_idempotency_json(&bad)
            .expect_err("idempotency response summary must be replay-decodable before persist");
        assert_eq!(err.code(), tonic::Code::Internal);
    }

    #[test]
    fn idempotency_response_json_requires_typed_receipt_lockstep_before_persist() {
        let receipt = crate::runtime::consistency::WriteReceipt {
            source_lsn: "0/3C4D".to_string(),
            outbox_seq: 13,
            projection_task_ids: vec!["projection-2".to_string()],
            manifest_checksum:
                "sha256:3333333333333333333333333333333333333333333333333333333333333333"
                    .to_string(),
            written_at_unix_ms: 1_700_000_000_456,
        };
        let receipt_json = serde_json::to_string(&receipt).expect("receipt JSON serializes");
        let missing_typed = MutationResponse {
            mutation_id: "44444444-4444-4444-8444-444444444444".to_string(),
            resource_uri: "udb://tenant-a/Payment/rec-1".to_string(),
            checksum_sha256:
                "sha256:4444444444444444444444444444444444444444444444444444444444444444"
                    .to_string(),
            record_json: Vec::new(),
            affected_rows: 1,
            was_duplicate: false,
            write_receipt_json: receipt_json.clone(),
            write_receipt: None,
            ..MutationResponse::default()
        };

        let missing_err = mutation_response_idempotency_json(&missing_typed)
            .expect_err("typed write_receipt must be present before persist");
        assert_eq!(missing_err.code(), tonic::Code::Internal);
        assert!(
            missing_err
                .message()
                .contains("requires typed write_receipt before persist")
        );

        let mut other_receipt = receipt.clone();
        other_receipt.outbox_seq += 1;
        let mismatched_typed = MutationResponse {
            write_receipt: Some(other_receipt.to_proto()),
            ..missing_typed
        };
        let mismatch_err = mutation_response_idempotency_json(&mismatched_typed)
            .expect_err("typed write_receipt must match write_receipt_json before persist");
        assert_eq!(mismatch_err.code(), tonic::Code::Internal);
        assert!(
            mismatch_err
                .message()
                .contains("typed write_receipt must match write_receipt_json before persist")
        );
    }

    #[test]
    fn idempotency_replay_response_rejects_corrupt_summary() {
        let missing = mutation_response_from_idempotency_json(&serde_json::json!({}))
            .expect_err("empty stored replay summary must fail closed");
        assert_eq!(missing.code(), tonic::Code::Internal);
        assert!(
            missing
                .message()
                .contains("idempotency replay response invalid")
        );
        assert!(
            missing
                .message()
                .contains("missing string field write_receipt_json")
        );

        let invalid_mutation_id = serde_json::json!({
            "mutation_id": "not-a-uuid",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let invalid_mutation_id_err = mutation_response_from_idempotency_json(&invalid_mutation_id)
            .expect_err("malformed stored mutation_id must fail closed");
        assert_eq!(invalid_mutation_id_err.code(), tonic::Code::Internal);
        assert!(
            invalid_mutation_id_err
                .message()
                .contains("invalid mutation_id")
        );

        let uppercase_mutation_id = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-AAAAAAAAAAAA",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let uppercase_mutation_id_err =
            mutation_response_from_idempotency_json(&uppercase_mutation_id)
                .expect_err("uppercase stored mutation_id must fail closed");
        assert_eq!(uppercase_mutation_id_err.code(), tonic::Code::Internal);
        assert!(
            uppercase_mutation_id_err
                .message()
                .contains("mutation_id must be a canonical lowercase UUID")
        );

        let compact_mutation_id = serde_json::json!({
            "mutation_id": "33333333333343338333333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let compact_mutation_id_err = mutation_response_from_idempotency_json(&compact_mutation_id)
            .expect_err("compact stored mutation_id must fail closed");
        assert_eq!(compact_mutation_id_err.code(), tonic::Code::Internal);
        assert!(
            compact_mutation_id_err
                .message()
                .contains("mutation_id must be a canonical lowercase UUID")
        );

        let invalid_record = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "not base64!",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let invalid_record_err = mutation_response_from_idempotency_json(&invalid_record)
            .expect_err("invalid stored record_json must fail closed");
        assert_eq!(invalid_record_err.code(), tonic::Code::Internal);
        assert!(invalid_record_err.message().contains("invalid record_json"));

        let duplicate_record_json =
            base64::engine::general_purpose::STANDARD.encode(br#"{"id":"rec-1","id":"rec-2"}"#);
        let duplicate_record = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": duplicate_record_json,
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let duplicate_record_err = mutation_response_from_idempotency_json(&duplicate_record)
            .expect_err("stored record_json duplicate key must fail closed");
        assert_eq!(duplicate_record_err.code(), tonic::Code::Internal);
        assert!(
            duplicate_record_err
                .message()
                .contains("record_json must not contain duplicate JSON key")
        );

        let non_json_record = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "bm90LWpzb24=",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let non_json_record_err = mutation_response_from_idempotency_json(&non_json_record)
            .expect_err("stored record_json non-JSON payload must fail closed");
        assert_eq!(non_json_record_err.code(), tonic::Code::Internal);

        let array_record = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "W10=",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let array_record_err = mutation_response_from_idempotency_json(&array_record)
            .expect_err("stored record_json array payload must fail closed");
        assert_eq!(array_record_err.code(), tonic::Code::Internal);
        assert!(
            array_record_err
                .message()
                .contains("record_json must be a JSON object")
        );

        let empty_object_record = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "e30=",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let empty_object_record_err = mutation_response_from_idempotency_json(&empty_object_record)
            .expect_err("stored record_json empty object payload must fail closed");
        assert_eq!(empty_object_record_err.code(), tonic::Code::Internal);
        assert!(
            empty_object_record_err
                .message()
                .contains("record_json must be a non-empty JSON object")
        );

        let invalid_receipt = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "not-json"
        });
        let invalid_receipt_err = mutation_response_from_idempotency_json(&invalid_receipt)
            .expect_err("invalid stored write_receipt_json must fail closed");
        assert_eq!(invalid_receipt_err.code(), tonic::Code::Internal);

        let duplicate_receipt_key = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"outbox_seq\":10,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let duplicate_receipt_key_err =
            mutation_response_from_idempotency_json(&duplicate_receipt_key)
                .expect_err("stored write_receipt_json duplicate key must fail closed");
        assert_eq!(duplicate_receipt_key_err.code(), tonic::Code::Internal);
        assert!(
            duplicate_receipt_key_err
                .message()
                .contains("write_receipt_json must not contain duplicate JSON key")
        );

        let missing_receipt_field = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"written_at_unix_ms\":1700000000000}"
        });
        let missing_receipt_field_err =
            mutation_response_from_idempotency_json(&missing_receipt_field)
                .expect_err("stored write_receipt_json missing field must fail closed");
        assert_eq!(missing_receipt_field_err.code(), tonic::Code::Internal);
        assert!(
            missing_receipt_field_err
                .message()
                .contains("write_receipt_json missing field manifest_checksum")
        );

        let unexpected_receipt_field = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000,\"shadow_fence\":\"0/FFFF\"}"
        });
        let unexpected_receipt_field_err =
            mutation_response_from_idempotency_json(&unexpected_receipt_field)
                .expect_err("stored write_receipt_json unexpected field must fail closed");
        assert_eq!(unexpected_receipt_field_err.code(), tonic::Code::Internal);
        assert!(
            unexpected_receipt_field_err
                .message()
                .contains("write_receipt_json unexpected field shadow_fence")
        );

        let padded_receipt_json = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": " {\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000} "
        });
        let padded_receipt_json_err = mutation_response_from_idempotency_json(&padded_receipt_json)
            .expect_err("stored write_receipt_json padding must fail closed");
        assert_eq!(padded_receipt_json_err.code(), tonic::Code::Internal);
        assert!(
            padded_receipt_json_err
                .message()
                .contains("write_receipt_json must not include surrounding whitespace")
        );

        let negative_affected_rows = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": -1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let negative_rows_err = mutation_response_from_idempotency_json(&negative_affected_rows)
            .expect_err("negative stored affected_rows must fail closed");
        assert_eq!(negative_rows_err.code(), tonic::Code::Internal);
        assert!(
            negative_rows_err
                .message()
                .contains("negative integer field affected_rows")
        );

        let invalid_checksum = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "not-a-sha-token",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let invalid_checksum_err = mutation_response_from_idempotency_json(&invalid_checksum)
            .expect_err("malformed stored checksum_sha256 must fail closed");
        assert_eq!(invalid_checksum_err.code(), tonic::Code::Internal);
        assert!(
            invalid_checksum_err
                .message()
                .contains("checksum_sha256 must be empty or start with sha256:")
        );

        let short_checksum = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:abc",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let short_checksum_err = mutation_response_from_idempotency_json(&short_checksum)
            .expect_err("short stored checksum_sha256 must fail closed");
        assert_eq!(short_checksum_err.code(), tonic::Code::Internal);
        assert!(
            short_checksum_err
                .message()
                .contains("checksum_sha256 must be empty or sha256:<64 lowercase hex>")
        );

        let uppercase_checksum = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let uppercase_checksum_err = mutation_response_from_idempotency_json(&uppercase_checksum)
            .expect_err("uppercase stored checksum_sha256 must fail closed");
        assert_eq!(uppercase_checksum_err.code(), tonic::Code::Internal);
        assert!(
            uppercase_checksum_err
                .message()
                .contains("checksum_sha256 must be empty or sha256:<64 lowercase hex>")
        );

        let invalid_resource_uri = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let invalid_resource_uri_err =
            mutation_response_from_idempotency_json(&invalid_resource_uri)
                .expect_err("malformed stored resource_uri must fail closed");
        assert_eq!(invalid_resource_uri_err.code(), tonic::Code::Internal);
        assert!(
            invalid_resource_uri_err
                .message()
                .contains("resource_uri must start with udb://")
        );

        let short_resource_uri = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let short_resource_uri_err = mutation_response_from_idempotency_json(&short_resource_uri)
            .expect_err("stored resource_uri without path must fail closed");
        assert_eq!(short_resource_uri_err.code(), tonic::Code::Internal);
        assert!(
            short_resource_uri_err
                .message()
                .contains("resource_uri must include non-empty authority and path")
        );

        let collection_resource_uri = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let collection_resource_uri_err =
            mutation_response_from_idempotency_json(&collection_resource_uri)
                .expect_err("stored resource_uri without resource id must fail closed");
        assert_eq!(collection_resource_uri_err.code(), tonic::Code::Internal);
        assert!(
            collection_resource_uri_err
                .message()
                .contains("resource_uri path must include message type and resource id")
        );

        let trailing_resource_uri = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let trailing_resource_uri_err =
            mutation_response_from_idempotency_json(&trailing_resource_uri)
                .expect_err("stored resource_uri empty resource id must fail closed");
        assert_eq!(trailing_resource_uri_err.code(), tonic::Code::Internal);
        assert!(
            trailing_resource_uri_err
                .message()
                .contains("resource_uri path must include message type and resource id")
        );

        let extra_segment_resource_uri = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1/extra",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let extra_segment_resource_uri_err =
            mutation_response_from_idempotency_json(&extra_segment_resource_uri)
                .expect_err("stored resource_uri extra segment must fail closed");
        assert_eq!(extra_segment_resource_uri_err.code(), tonic::Code::Internal);
        assert!(
            extra_segment_resource_uri_err
                .message()
                .contains("resource_uri path must include message type and resource id")
        );

        let whitespace_resource_uri = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec 1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let whitespace_resource_uri_err =
            mutation_response_from_idempotency_json(&whitespace_resource_uri)
                .expect_err("stored resource_uri whitespace must fail closed");
        assert_eq!(whitespace_resource_uri_err.code(), tonic::Code::Internal);
        assert!(
            whitespace_resource_uri_err
                .message()
                .contains("resource_uri must not include whitespace")
        );

        let control_resource_uri = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1\u{0000}",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let control_resource_uri_err =
            mutation_response_from_idempotency_json(&control_resource_uri)
                .expect_err("stored resource_uri control character must fail closed");
        assert_eq!(control_resource_uri_err.code(), tonic::Code::Internal);
        assert!(
            control_resource_uri_err
                .message()
                .contains("resource_uri must not contain control characters")
        );

        let invalid_receipt_timestamp = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":0}"
        });
        let invalid_receipt_timestamp_err =
            mutation_response_from_idempotency_json(&invalid_receipt_timestamp)
                .expect_err("stored write_receipt_json timestamp must fail closed");
        assert_eq!(invalid_receipt_timestamp_err.code(), tonic::Code::Internal);
        assert!(
            invalid_receipt_timestamp_err
                .message()
                .contains("write_receipt_json written_at_unix_ms must be positive")
        );

        let empty_receipt_lsn = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let empty_receipt_lsn_err = mutation_response_from_idempotency_json(&empty_receipt_lsn)
            .expect_err("stored write_receipt_json empty source_lsn must fail closed");
        assert_eq!(empty_receipt_lsn_err.code(), tonic::Code::Internal);
        assert!(
            empty_receipt_lsn_err
                .message()
                .contains("write_receipt_json source_lsn must be non-empty")
        );

        let spaced_receipt_lsn = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0 /1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let spaced_receipt_lsn_err = mutation_response_from_idempotency_json(&spaced_receipt_lsn)
            .expect_err("stored write_receipt_json source_lsn whitespace must fail closed");
        assert_eq!(spaced_receipt_lsn_err.code(), tonic::Code::Internal);
        assert!(
            spaced_receipt_lsn_err
                .message()
                .contains("write_receipt_json source_lsn must not include whitespace")
        );

        let control_receipt_lsn = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\\u0000\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let control_receipt_lsn_err = mutation_response_from_idempotency_json(&control_receipt_lsn)
            .expect_err("stored write_receipt_json source_lsn control character must fail closed");
        assert_eq!(control_receipt_lsn_err.code(), tonic::Code::Internal);
        assert!(
            control_receipt_lsn_err
                .message()
                .contains("write_receipt_json source_lsn must not contain control characters")
        );

        let padded_receipt_manifest = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\" sha256:1111111111111111111111111111111111111111111111111111111111111111 \",\"written_at_unix_ms\":1700000000000}"
        });
        let padded_receipt_manifest_err =
            mutation_response_from_idempotency_json(&padded_receipt_manifest)
                .expect_err("stored write_receipt_json manifest checksum padding must fail closed");
        assert_eq!(padded_receipt_manifest_err.code(), tonic::Code::Internal);
        assert!(padded_receipt_manifest_err.message().contains(
            "write_receipt_json manifest_checksum must not include surrounding whitespace"
        ));

        let invalid_receipt_manifest = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"not-a-sha-token\",\"written_at_unix_ms\":1700000000000}"
        });
        let invalid_receipt_manifest_err =
            mutation_response_from_idempotency_json(&invalid_receipt_manifest)
                .expect_err("stored write_receipt_json manifest checksum prefix must fail closed");
        assert_eq!(invalid_receipt_manifest_err.code(), tonic::Code::Internal);
        assert!(
            invalid_receipt_manifest_err
                .message()
                .contains("write_receipt_json manifest_checksum must start with sha256:")
        );

        let short_receipt_manifest = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:abc\",\"written_at_unix_ms\":1700000000000}"
        });
        let short_receipt_manifest_err =
            mutation_response_from_idempotency_json(&short_receipt_manifest)
                .expect_err("stored write_receipt_json manifest checksum shape must fail closed");
        assert_eq!(short_receipt_manifest_err.code(), tonic::Code::Internal);
        assert!(
            short_receipt_manifest_err
                .message()
                .contains("write_receipt_json manifest_checksum must be sha256:<64 lowercase hex>")
        );

        let uppercase_receipt_manifest = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[],\"manifest_checksum\":\"sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"written_at_unix_ms\":1700000000000}"
        });
        let uppercase_receipt_manifest_err =
            mutation_response_from_idempotency_json(&uppercase_receipt_manifest)
                .expect_err("stored write_receipt_json manifest checksum case must fail closed");
        assert_eq!(uppercase_receipt_manifest_err.code(), tonic::Code::Internal);
        assert!(
            uppercase_receipt_manifest_err
                .message()
                .contains("write_receipt_json manifest_checksum must be sha256:<64 lowercase hex>")
        );

        let invalid_receipt_task = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[\" task-1 \"],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let invalid_receipt_task_err =
            mutation_response_from_idempotency_json(&invalid_receipt_task)
                .expect_err("stored write_receipt_json task id padding must fail closed");
        assert_eq!(invalid_receipt_task_err.code(), tonic::Code::Internal);
        assert!(invalid_receipt_task_err.message().contains(
            "write_receipt_json projection_task_ids[0] must be non-empty and contain no whitespace"
        ));

        let spaced_receipt_task = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[\"task 1\"],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let spaced_receipt_task_err = mutation_response_from_idempotency_json(&spaced_receipt_task)
            .expect_err("stored write_receipt_json task id whitespace must fail closed");
        assert_eq!(spaced_receipt_task_err.code(), tonic::Code::Internal);
        assert!(spaced_receipt_task_err.message().contains(
            "write_receipt_json projection_task_ids[0] must be non-empty and contain no whitespace"
        ));

        let control_receipt_task = serde_json::json!({
            "mutation_id": "33333333-3333-4333-8333-333333333333",
            "resource_uri": "udb://tenant-a/Payment/rec-1",
            "checksum_sha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "record_json": "",
            "affected_rows": 1,
            "write_receipt_json": "{\"source_lsn\":\"0/1A2B\",\"outbox_seq\":9,\"projection_task_ids\":[\"task-1\\u0000\"],\"manifest_checksum\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"written_at_unix_ms\":1700000000000}"
        });
        let control_receipt_task_err =
            mutation_response_from_idempotency_json(&control_receipt_task)
                .expect_err("stored write_receipt_json task id control character must fail closed");
        assert_eq!(control_receipt_task_err.code(), tonic::Code::Internal);
        assert!(control_receipt_task_err.message().contains(
            "write_receipt_json projection_task_ids[0] must not contain control characters"
        ));
    }

    #[test]
    fn idempotency_response_persist_requires_exactly_one_row() {
        assert!(idempotency_response_persist_row_count_status(1).is_ok());
        let missing = idempotency_response_persist_row_count_status(0)
            .expect_err("missing dedup row must fail closed");
        assert_eq!(missing.code(), tonic::Code::Internal);
        assert!(missing.message().contains("affected 0 rows"));
        assert!(missing.message().contains("expected exactly one"));
        let duplicate = idempotency_response_persist_row_count_status(2)
            .expect_err("duplicate dedup rows must fail closed");
        assert_eq!(duplicate.code(), tonic::Code::Internal);
        assert!(duplicate.message().contains("affected 2 rows"));
        assert!(duplicate.message().contains("expected exactly one"));
    }

    #[test]
    fn idempotency_response_persist_update_is_scope_bound() {
        let sql = idempotency_response_persist_sql("udb_idempotency_keys");
        assert!(sql.contains("UPDATE udb_idempotency_keys"));
        assert!(sql.contains("SET response_json = $1"));
        assert!(sql.contains("WHERE dedup_key = $2"));
        assert!(sql.contains("AND tenant_id = $3"));
        assert!(sql.contains("AND project_id = $4"));
        assert!(sql.contains("AND message_type = $5"));
        assert!(sql.contains("AND operation = $6"));
    }

    #[test]
    fn write_receipt_json_serialization_is_not_silent_default() {
        let receipt = crate::runtime::consistency::WriteReceipt {
            source_lsn: "0/3C4D".to_string(),
            outbox_seq: 21,
            projection_task_ids: vec!["projection-a".to_string()],
            manifest_checksum:
                "sha256:3333333333333333333333333333333333333333333333333333333333333333"
                    .to_string(),
            written_at_unix_ms: 1_700_000_000_456,
        };
        let json = write_receipt_json_or_status(&receipt).expect("receipt JSON serializes");
        assert!(!json.is_empty());
        assert!(json.contains("0/3C4D"));
        let decoded: crate::runtime::consistency::WriteReceipt =
            serde_json::from_str(&json).expect("receipt JSON decodes");
        assert_eq!(decoded.source_lsn, receipt.source_lsn);
        assert_eq!(decoded.outbox_seq, receipt.outbox_seq);
        assert_eq!(decoded.projection_task_ids, receipt.projection_task_ids);
        assert_eq!(decoded.manifest_checksum, receipt.manifest_checksum);
        assert_eq!(decoded.written_at_unix_ms, receipt.written_at_unix_ms);
    }

    #[test]
    fn return_record_json_decode_is_not_silent_default() {
        let err = returned_record_json_or_status(&[])
            .expect_err("empty RETURNING decode must fail closed");
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(
            err.message()
                .contains("RETURNING row decoded without record_json")
        );

        let record = br#"{"id":"rec-1"}"#.to_vec();
        assert_eq!(
            returned_record_json_or_status(std::slice::from_ref(&record))
                .expect("returned record_json decodes"),
            record
        );
    }

    #[test]
    fn projection_system_store_opt_in_requires_literal_one() {
        assert!(projection_system_store_opt_in_value(Some("1".to_string())));
        assert!(projection_system_store_opt_in_value(Some(
            " 1 ".to_string()
        )));
        assert!(!projection_system_store_opt_in_value(None));
        assert!(!projection_system_store_opt_in_value(Some(
            "true".to_string()
        )));
        assert!(!projection_system_store_opt_in_value(Some("0".to_string())));
    }

    #[test]
    fn projection_role_and_clickhouse_stores_require_explicit_opt_in() {
        assert!(full_canonical_store_requires_opt_in(
            &crate::backend::BackendKind::Qdrant
        ));
        assert!(full_canonical_store_requires_opt_in(
            &crate::backend::BackendKind::Pinecone
        ));
        assert!(full_canonical_store_requires_opt_in(
            &crate::backend::BackendKind::Weaviate
        ));
        assert!(full_canonical_store_requires_opt_in(
            &crate::backend::BackendKind::Elasticsearch
        ));
        assert!(full_canonical_store_requires_opt_in(
            &crate::backend::BackendKind::Clickhouse
        ));
    }

    #[test]
    fn pg_direct_outbox_requires_postgres_primary_receipt_store() {
        assert!(pg_outbox_receipt_store_mismatch(false, Some(("mysql", "primary"))).is_none());
        assert!(pg_outbox_receipt_store_mismatch(true, None).is_none());
        assert!(pg_outbox_receipt_store_mismatch(true, Some(("postgres", "primary"))).is_none());
        assert!(pg_outbox_receipt_store_mismatch(true, Some(("mysql", "primary"))).is_some());
        assert!(pg_outbox_receipt_store_mismatch(true, Some(("postgres", "analytics"))).is_some());
    }

    #[test]
    fn runtime_setup_adds_mongodb_and_clickhouse_env_instances() {
        let mut config = BackendInstanceConfig {
            instances: vec![BackendInstance {
                name: "primary".to_string(),
                backend: "postgres".to_string(),
                role: BackendInstanceRole::ReadWrite,
                dsn: Some("postgres://localhost/udb".to_string()),
                dsn_env: None,
                ..BackendInstance::default()
            }],
        };
        let env_instances = BackendInstanceConfig {
            instances: vec![
                BackendInstance {
                    name: "default".to_string(),
                    backend: "mongodb".to_string(),
                    role: BackendInstanceRole::ReadWrite,
                    dsn: Some("mongodb://localhost:27017/udb".to_string()),
                    dsn_env: None,
                    ..BackendInstance::default()
                },
                BackendInstance {
                    name: "default".to_string(),
                    backend: "clickhouse".to_string(),
                    role: BackendInstanceRole::Read,
                    dsn: Some("http://localhost:8123/default".to_string()),
                    dsn_env: None,
                    ..BackendInstance::default()
                },
            ],
        };
        merge_runtime_backend_instances(&mut config, env_instances);

        assert!(config.instances.iter().any(|instance| {
            instance.backend == "mongodb" && instance.name == "default" && instance.dsn.is_some()
        }));
        assert!(config.instances.iter().any(|instance| {
            instance.backend == "clickhouse" && instance.name == "default" && instance.dsn.is_some()
        }));
    }

    #[test]
    fn runtime_setup_does_not_clobber_explicit_backend_instance() {
        let mut config = BackendInstanceConfig {
            instances: vec![BackendInstance {
                name: "analytics".to_string(),
                backend: "clickhouse".to_string(),
                role: BackendInstanceRole::Read,
                dsn: Some("http://clickhouse:8123/analytics".to_string()),
                dsn_env: None,
                ..BackendInstance::default()
            }],
        };
        let env_instances = BackendInstanceConfig {
            instances: vec![BackendInstance {
                name: "default".to_string(),
                backend: "clickhouse".to_string(),
                role: BackendInstanceRole::Read,
                dsn: Some("http://localhost:8123/default".to_string()),
                dsn_env: None,
                ..BackendInstance::default()
            }],
        };
        merge_runtime_backend_instances(&mut config, env_instances);

        let clickhouse_instances = config
            .instances
            .iter()
            .filter(|instance| instance.backend == "clickhouse")
            .count();
        assert_eq!(clickhouse_instances, 1);
        assert_eq!(config.instances[0].name, "analytics");
    }
}

/// Wire the PostgreSQL primary pool (and any replicas) into the runtime.
///
/// Mirrors the inline block previously at the head of `from_config`. No
/// behavior change: same pool options, same warning text, same instance map
/// keys ("primary" for the primary pool, "replica-N" for replicas).
pub(crate) async fn register_postgres(ctx: &mut RegisterCtx<'_>) {
    let RegisterCtx {
        config,
        instance_config,
        app_name,
        runtime,
        report,
        ..
    } = ctx;
    let app_name: &str = app_name;
    // GAP 2: Full pool hardening with timeouts, health-check, app_name.
    let acquire_timeout = Duration::from_secs(if config.primary.acquire_timeout_secs > 0 {
        config.primary.acquire_timeout_secs
    } else {
        10
    });
    let idle_timeout = Duration::from_secs(if config.primary.conn_max_idle_secs > 0 {
        config.primary.conn_max_idle_secs
    } else {
        600
    });
    let max_lifetime = Duration::from_secs(if config.primary.conn_max_lifetime_secs > 0 {
        config.primary.conn_max_lifetime_secs
    } else {
        1800
    });
    if let Some(dsn) = postgres_dsn_from_config(&config.primary) {
        match connect_pg_pool_from_config(&dsn, app_name, &config.primary).await {
            Ok(pool) => {
                tracing::info!(
                    app_name = %app_name,
                    min_conn = if config.primary.min_connections > 0 { config.primary.min_connections } else { 5 },
                    max_conn = if config.primary.max_open_conns > 0 { config.primary.max_open_conns } else { 50 },
                    acquire_timeout_secs = acquire_timeout.as_secs(),
                    idle_timeout_secs = idle_timeout.as_secs(),
                    max_lifetime_secs = max_lifetime.as_secs(),
                    "PostgreSQL primary pool initialised"
                );
                report.postgres_configured = true;
                runtime
                    .pg_instances
                    .insert("primary".to_string(), pool.clone());
                runtime.connections.register_postgres(
                    "primary",
                    "read_write",
                    pool.clone(),
                    HashMap::new(),
                );
                // NW1-2 + NW1-3: register a `PostgresCanonicalStore`
                // wrapping the primary PG pool, in the SystemStores
                // (rich) view so NW1 step 3+ call sites can pull the
                // ProjectionTaskStore / SagaStore / AdminAuditStore /
                // MigrationAuditStore methods off the same trait
                // object.
                #[cfg(feature = "postgres")]
                {
                    use crate::runtime::canonical_store::SystemStores;
                    use crate::runtime::canonical_store::postgres::PostgresCanonicalStore;
                    use crate::runtime::cdc::CdcConfig;
                    let outbox_relation = CdcConfig::current().outbox_relation();
                    let store =
                        PostgresCanonicalStore::new(pool.clone(), "primary", outbox_relation);
                    match ensure_full_system_store_tables(&store).await {
                        Ok(()) => {
                            let store: std::sync::Arc<dyn SystemStores> =
                                std::sync::Arc::new(store);
                            runtime.register_full_canonical_store(store);
                        }
                        Err(err) => {
                            tracing::error!(
                                error = %err,
                                "PostgreSQL canonical store not registered \
                                 (ensure_full_system_store_tables failed)"
                            );
                            report.warnings.push(format!(
                                "PostgreSQL canonical store not registered \
                                 (ensure_full_system_store_tables failed): {err}"
                            ));
                        }
                    }
                }
                runtime.pg_pool = Some(pool);
            }
            Err(err) => report
                .warnings
                .push(format!("PostgreSQL unavailable: {err}")),
        }
    }

    // Optional read-replica pools. Legacy env values are folded into
    // `UdbConfig.pg_replica_dsns` during config merge.
    let replica_dsns = config.pg_replica_dsns.clone();
    if !replica_dsns.is_empty() {
        let replica_strategy = PgReplicaStrategy::from_value(&config.pg_replica_strategy);
        let replica_max_lag = Duration::from_secs(config.pg_replica_max_lag_secs.max(3));
        let replica_fail_open = config.pg_replica_fail_open;
        let mut replicas = Vec::new();

        for (idx, replica_dsn) in replica_dsns.iter().enumerate() {
            let label = format!("replica-{}", idx + 1);
            let replica_app = format!("{}-{}", app_name, label);
            let replica_cs = append_application_name(replica_dsn, &replica_app);
            match PgPoolOptions::new()
                .min_connections(if config.pg_replica_min_connections > 0 {
                    config.pg_replica_min_connections
                } else {
                    config.primary.min_connections.max(2) as u32
                })
                .max_connections(if config.pg_replica_max_connections > 0 {
                    config.pg_replica_max_connections
                } else if config.primary.max_open_conns > 0 {
                    config.primary.max_open_conns as u32
                } else {
                    50
                })
                .acquire_timeout(acquire_timeout)
                .idle_timeout(idle_timeout)
                .max_lifetime(max_lifetime)
                .test_before_acquire(true)
                .connect(&replica_cs)
                .await
            {
                Ok(pool) => {
                    tracing::info!(
                        replica = %label,
                        strategy = replica_strategy.as_str(),
                        max_lag_secs = replica_max_lag.as_secs(),
                        "PostgreSQL replica pool initialised"
                    );
                    runtime.connections.register_postgres(
                        &label,
                        "read",
                        pool.clone(),
                        HashMap::from([(
                            "replica_strategy".to_string(),
                            replica_strategy.as_str().to_string(),
                        )]),
                    );
                    replicas.push(PgReplicaPool::new(label, pool));
                }
                Err(err) => report.warnings.push(format!(
                    "PostgreSQL replica pool {} unavailable: {err}",
                    idx + 1
                )),
            }
        }

        if !replicas.is_empty() {
            let manager = PgReplicaManager::new(
                replicas,
                replica_strategy,
                replica_max_lag,
                replica_fail_open,
            );
            let health_interval =
                Duration::from_secs(config.pg_replica_health_interval_secs.max(10));
            manager.refresh_health_once().await;
            manager.start_health_task(health_interval);
            runtime.pg_replicas = manager;
        }
    }

    for instance in instance_config.active().filter(|instance| {
        instance_matches_backend(instance, crate::backend::BackendKind::Postgres)
    }) {
        if runtime.pg_instances.contains_key(&instance.name) {
            continue;
        }
        let Some(dsn) = instance.resolve_dsn() else {
            continue;
        };
        let instance_app_name = format!("{}-{}", app_name, instance.name);
        match connect_pg_pool_from_config(&dsn, &instance_app_name, &config.primary).await {
            Ok(pool) => {
                tracing::info!(
                    instance = %instance.name,
                    app_name = %instance_app_name,
                    "PostgreSQL named instance pool initialised"
                );
                report.postgres_configured = true;
                if runtime.pg_pool.is_none() && instance.name == "primary" {
                    runtime.pg_pool = Some(pool.clone());
                }
                runtime.connections.register_postgres(
                    &instance.name,
                    instance.role.as_str(),
                    pool.clone(),
                    instance_labels(instance),
                );
                runtime.pg_instances.insert(instance.name.clone(), pool);
            }
            Err(err) => report.warnings.push(format!(
                "PostgreSQL instance {} unavailable: {err}",
                instance.name
            )),
        }
    }
}

/// NW3-1 — register the MySQL primary pool + canonical store + executor.
///
/// Reads `UDB_MYSQL_DSN` from the environment (the canonical token
/// pinned by `BackendKind::Mysql::dsn_env_var()`). If absent, the
/// plugin is a no-op and the runtime continues without MySQL — same
/// soft-fail pattern Postgres uses.
///
/// On success the MySQL pool is:
/// - stored as `runtime.mysql_pool` for direct access from dispatch.
/// - registered under instance name `"primary"` in `mysql_instances`.
/// - wrapped in a `MysqlCanonicalStore` and registered in the
///   `CanonicalStoreRegistry` so projection / saga / admin audit /
///   migration audit routes work against MySQL out of the box.
#[cfg(feature = "mysql")]
pub(crate) async fn register_mysql(ctx: &mut RegisterCtx<'_>) {
    let RegisterCtx {
        runtime, report, ..
    } = ctx;
    let Some(dsn) = std::env::var("UDB_MYSQL_DSN")
        .ok()
        .filter(|s| !s.trim().is_empty())
    else {
        // No DSN configured — MySQL is opt-in.
        return;
    };
    // sqlx-mysql pool. Defaults match the Postgres min/max/timeout
    // pattern: 5 / 50 / 10s acquire.
    let pool_options = sqlx::mysql::MySqlPoolOptions::new()
        .min_connections(5)
        .max_connections(50)
        .acquire_timeout(Duration::from_secs(10))
        .idle_timeout(Some(Duration::from_secs(600)))
        .max_lifetime(Some(Duration::from_secs(1800)));
    match pool_options.connect(&dsn).await {
        Ok(pool) => {
            tracing::info!("MySQL primary pool initialised");
            report.mysql_configured = true;
            runtime
                .mysql_instances
                .insert("primary".to_string(), pool.clone());
            // Register the MySQL canonical store under the same key.
            // Slim deployments will pick this up if Postgres isn't
            // configured.
            {
                use crate::runtime::canonical_store::SystemStores;
                use crate::runtime::canonical_store::mysql::MysqlCanonicalStore;
                use crate::runtime::cdc::CdcConfig;
                // #115: MySQL uses backtick identifiers within the connected
                // database — not the Postgres double-quoted `schema.table`.
                let outbox_relation = CdcConfig::current().outbox_relation_mysql();
                let store = MysqlCanonicalStore::new(pool.clone(), "primary", outbox_relation);
                match ensure_full_system_store_tables(&store).await {
                    Ok(()) => {
                        let store: std::sync::Arc<dyn SystemStores> = std::sync::Arc::new(store);
                        runtime.register_full_canonical_store(store);
                    }
                    Err(err) => {
                        report.warnings.push(format!(
                            "MySQL canonical store not registered \
                             (ensure_full_system_store_tables failed): {err}"
                        ));
                    }
                }
            }
            runtime.mysql_pool = Some(pool);
        }
        Err(err) => {
            report.warnings.push(format!("MySQL unavailable: {err}"));
        }
    }
}

/// NW3-2 — register the SQLite primary pool + canonical store +
/// executor.
///
/// Reads `UDB_SQLITE_DSN` from the environment. Accepts both file
/// DSNs (`sqlite://path/to/db.sqlite`) and the in-memory form
/// (`sqlite::memory:` or `:memory:`).
#[cfg(feature = "sqlite")]
pub(crate) async fn register_sqlite(ctx: &mut RegisterCtx<'_>) {
    let RegisterCtx {
        runtime, report, ..
    } = ctx;
    let Some(dsn) = std::env::var("UDB_SQLITE_DSN")
        .ok()
        .filter(|s| !s.trim().is_empty())
    else {
        return;
    };
    // `:memory:` is per-connection in SQLite; if the operator picked
    // the in-memory DSN, force max_connections=1 so the schema and
    // data survive across operations on a single shared connection.
    let max_connections = if dsn.contains(":memory:") || dsn.contains("memory:") {
        1
    } else {
        50
    };
    let pool_options = sqlx::sqlite::SqlitePoolOptions::new()
        .min_connections(1)
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(10))
        .idle_timeout(Some(Duration::from_secs(600)))
        .max_lifetime(Some(Duration::from_secs(1800)));
    match pool_options.connect(&dsn).await {
        Ok(pool) => {
            tracing::info!(
                dsn_kind = if max_connections == 1 {
                    "memory"
                } else {
                    "file"
                },
                "SQLite primary pool initialised"
            );
            report.sqlite_configured = true;
            runtime
                .sqlite_instances
                .insert("primary".to_string(), pool.clone());
            // SQLite canonical store (closes the long-pending
            // in-memory test profile too).
            {
                use crate::runtime::canonical_store::SystemStores;
                use crate::runtime::canonical_store::sqlite::SqliteCanonicalStore;
                use crate::runtime::cdc::CdcConfig;
                // #115: SQLite has no schemas and its store validates a bare
                // `[A-Za-z0-9_]+` table name — pass the unquoted table.
                let outbox_table = CdcConfig::current().outbox_table_bare();
                let store = SqliteCanonicalStore::new(pool.clone(), "primary", outbox_table);
                match ensure_full_system_store_tables(&store).await {
                    Ok(()) => {
                        let store: std::sync::Arc<dyn SystemStores> = std::sync::Arc::new(store);
                        runtime.register_full_canonical_store(store);
                    }
                    Err(err) => {
                        report.warnings.push(format!(
                            "SQLite canonical store not registered \
                             (ensure_full_system_store_tables failed): {err}"
                        ));
                    }
                }
            }
            runtime.sqlite_pool = Some(pool);
        }
        Err(err) => {
            report.warnings.push(format!("SQLite unavailable: {err}"));
        }
    }
}

/// C9: register the primary Elasticsearch HTTP client. Reads
/// `UDB_ELASTIC_DSN` (canonical token pinned by
/// `BackendKind::Elasticsearch::dsn_env_var()`). DSN forms accepted:
///
/// - `http://host:9200` / `https://host:9200` — no auth
/// - `http://user:pass@host:9200` — Basic auth
/// - `apikey://<base64>@host:9200` — sentinel for API-key deployments
///
/// If the env var is absent, ES is skipped silently (opt-in).
#[cfg(feature = "elasticsearch")]
pub(crate) async fn register_elasticsearch(ctx: &mut RegisterCtx<'_>) {
    use crate::runtime::executors::elasticsearch::ElasticsearchHttpClient;
    let RegisterCtx {
        runtime, report, ..
    } = ctx;
    let Some(raw_dsn) = std::env::var("UDB_ELASTIC_DSN")
        .ok()
        .filter(|s| !s.trim().is_empty())
    else {
        return;
    };
    // Parse the DSN. The sentinel scheme `apikey://` strips into
    // (base_url, ElasticsearchAuth::ApiKey); standard URLs with
    // userinfo become Basic auth; bare URLs are no-auth.
    let (base_url, auth) = parse_elasticsearch_dsn(&raw_dsn);
    let client = ElasticsearchHttpClient::new(base_url, auth);
    // Smoke ping deferred to first real call — startup shouldn't block
    // on ES reachability (matches the Qdrant / Mongo pattern).
    report.elasticsearch_configured = true;
    runtime
        .elasticsearch_instances
        .insert("primary".to_string(), client.clone());
    if let Err(err) =
        ensure_full_canonical_store_registration_allowed(crate::backend::BackendKind::Elasticsearch)
    {
        report
            .warnings
            .push(format!("{err}; search executor remains available"));
    } else {
        use crate::runtime::canonical_store::CanonicalStore;
        use crate::runtime::canonical_store::SystemStores;
        use crate::runtime::canonical_store::vector_system::VectorSystemCanonicalStore;

        let store = VectorSystemCanonicalStore::new_elasticsearch(client.clone(), "primary");
        match CanonicalStore::ensure_system_tables(&store).await {
            Ok(()) => {
                let store: std::sync::Arc<dyn SystemStores> = std::sync::Arc::new(store);
                runtime.register_full_canonical_store(store);
                tracing::info!("Elasticsearch canonical SystemStores registered");
            }
            Err(err) => report.warnings.push(format!(
                "Elasticsearch canonical store not registered: {err}; search executor remains available"
            )),
        }
    }
    runtime.elasticsearch = Some(client);
}

#[cfg(feature = "elasticsearch")]
pub(crate) fn parse_elasticsearch_dsn(
    raw: &str,
) -> (
    String,
    crate::runtime::executors::elasticsearch::ElasticsearchAuth,
) {
    use crate::runtime::executors::elasticsearch::ElasticsearchAuth;
    let trimmed = raw.trim();
    // `apikey://<base64>@host:port` — strip the api key out, leaving
    // a plain http URL.
    if let Some(rest) = trimmed.strip_prefix("apikey://")
        && let Some((key, host)) = rest.split_once('@')
    {
        // Default to https for the rebuilt URL — Elastic Cloud always
        // uses TLS.
        return (
            format!("https://{host}"),
            ElasticsearchAuth::ApiKey(key.to_string()),
        );
    }
    // Standard URL with optional userinfo: `scheme://user:pass@host:port`.
    if let Some(scheme_pos) = trimmed.find("://") {
        let scheme = &trimmed[..scheme_pos];
        let after = &trimmed[scheme_pos + 3..];
        if let Some((auth_part, host_part)) = after.split_once('@')
            && let Some((user, pass)) = auth_part.split_once(':')
        {
            return (
                format!("{scheme}://{host_part}"),
                ElasticsearchAuth::Basic {
                    username: user.to_string(),
                    password: pass.to_string(),
                },
            );
        }
    }
    (trimmed.to_string(), ElasticsearchAuth::None)
}

/// C9: register the primary Memcached client. Reads
/// `UDB_MEMCACHED_DSN` (canonical token pinned by
/// `BackendKind::Memcached::dsn_env_var()`). DSN form:
/// `memcache://host:port?timeout=10&tcp_nodelay=true` — the canonical
/// `memcache` crate URL syntax. The crate also accepts
/// `memcache+udp://`, `memcache+tls://`, and `memcache+unix://`.
///
/// Skipped silently when the env var is absent (opt-in).
#[cfg(feature = "memcached")]
pub(crate) async fn register_memcached(ctx: &mut RegisterCtx<'_>) {
    use crate::runtime::executors::memcached::MemcachedClient;
    let RegisterCtx {
        runtime, report, ..
    } = ctx;
    let Some(dsn) = std::env::var("UDB_MEMCACHED_DSN")
        .ok()
        .filter(|s| !s.trim().is_empty())
    else {
        return;
    };
    // D.9: unix-domain sockets don't exist on Windows. Reject a `memcache+unix://`
    // DSN there with a clear, actionable message instead of an opaque connect
    // failure / undefined path. Memcached is an optional cache, so this fail-soft
    // (broker keeps running without the cache) — but the reason is explicit.
    if cfg!(windows) && is_unix_socket_memcache_dsn(&dsn) {
        report.warnings.push(format!(
            "UDB_MEMCACHED_DSN {} uses a unix-domain socket, which is unsupported on \
             Windows; use a TCP DSN (memcache://host:port) instead — cache disabled",
            redact_dsn(&dsn)
        ));
        return;
    }
    // `MemcachedClient::connect` is sync — spawn_blocking so we
    // don't tie up the async runtime.
    let dsn_owned = dsn.clone();
    let result = tokio::task::spawn_blocking(move || MemcachedClient::connect(&dsn_owned))
        .await
        .ok()
        .and_then(|r| r.ok());
    if let Some(client) = result {
        tracing::info!(dsn = %redact_dsn(&dsn), "Memcached primary initialised");
        report.memcached_configured = true;
        runtime
            .memcached_instances
            .insert("primary".to_string(), client.clone());
        runtime.memcached = Some(client);
    } else {
        report.warnings.push(format!(
            "Memcached unavailable at {} (kept running without cache)",
            redact_dsn(&dsn)
        ));
    }
}

/// Strip userinfo / API keys from a DSN before logging — defence in
/// depth for accidentally credentialed Memcached URLs.
#[cfg(feature = "memcached")]
fn redact_dsn(dsn: &str) -> String {
    if let Some(scheme_end) = dsn.find("://") {
        let after = &dsn[scheme_end + 3..];
        if let Some(at_pos) = after.find('@') {
            return format!("{}://***@{}", &dsn[..scheme_end], &after[at_pos + 1..]);
        }
    }
    dsn.to_string()
}

/// D.9: whether a memcached DSN targets a unix-domain socket. Pulled out so the
/// Windows-rejection guard is unit-testable independent of the host platform.
#[cfg(feature = "memcached")]
fn is_unix_socket_memcache_dsn(dsn: &str) -> bool {
    let dsn = dsn.trim_start();
    dsn.starts_with("memcache+unix://") || dsn.starts_with("unix://")
}

#[cfg(all(test, feature = "memcached"))]
mod memcached_dsn_tests {
    use super::{is_unix_socket_memcache_dsn, redact_dsn};

    #[test]
    fn detects_unix_socket_dsn_forms() {
        assert!(is_unix_socket_memcache_dsn(
            "memcache+unix:///var/run/memcached.sock"
        ));
        assert!(is_unix_socket_memcache_dsn("  unix:///tmp/x.sock"));
        assert!(!is_unix_socket_memcache_dsn("memcache://127.0.0.1:11211"));
        assert!(!is_unix_socket_memcache_dsn("memcache+tls://host:11211"));
        assert!(!is_unix_socket_memcache_dsn("memcache+udp://host:11211"));
    }

    #[test]
    fn redact_dsn_strips_userinfo_across_uri_forms() {
        assert_eq!(
            redact_dsn("memcache://user:pass@host:11211"),
            "memcache://***@host:11211"
        );
        assert_eq!(
            redact_dsn("memcache+tls://u:p@h:11211?x=1"),
            "memcache+tls://***@h:11211?x=1"
        );
        // No userinfo → unchanged (incl. the unix-socket path form).
        assert_eq!(
            redact_dsn("memcache://127.0.0.1:11211"),
            "memcache://127.0.0.1:11211"
        );
        assert_eq!(
            redact_dsn("memcache+unix:///var/run/m.sock"),
            "memcache+unix:///var/run/m.sock"
        );
    }
}

/// C9: register the SQL Server client. Reads `UDB_MSSQL_DSN`
/// (canonical token pinned by `BackendKind::Mssql::dsn_env_var()`).
/// Tiberius accepts the ADO connection string format directly:
/// `Server=host,1433;Database=mydb;User=sa;Password=…;
/// TrustServerCertificate=true`.
///
/// The client is constructed lazily — no TCP connection happens
/// at register time. First call to `with_client` opens the
/// connection and caches it.
#[cfg(feature = "mssql")]
pub(crate) async fn register_mssql(ctx: &mut RegisterCtx<'_>) {
    let RegisterCtx {
        instance_config,
        runtime,
        report,
        ..
    } = ctx;
    for instance in instance_config
        .active()
        .filter(|instance| instance_matches_backend(instance, crate::backend::BackendKind::Mssql))
    {
        if runtime.mssql_instances.contains_key(&instance.name) {
            continue;
        }
        if let Some(client) = mssql_executor_from_instance(instance) {
            tracing::info!(
                instance = %instance.name,
                "SQL Server client constructed; ensuring configured database exists"
            );
            if let Err(err) = client.ensure_database_exists().await {
                report.warnings.push(format!(
                    "SQL Server instance {} unavailable during database bootstrap: {err}",
                    instance.name
                ));
                continue;
            }
            report.mssql_configured = true;
            if runtime.mssql.is_none() {
                runtime.mssql = Some(client.clone());
            }
            // B.8: register the SQL Server canonical store for the primary
            // instance. Fail-closed — only register if `ensure_system_tables`
            // succeeds (a reachable, permissioned SQL Server), mirroring the
            // PG/MySQL canonical registration but gated as the doc requires.
            if instance.name == "primary" {
                use crate::runtime::canonical_store::SystemStores;
                use crate::runtime::canonical_store::mssql::MssqlCanonicalStore;
                use crate::runtime::cdc::CdcConfig;
                let outbox_relation = CdcConfig::current().outbox_relation_mssql();
                let store = MssqlCanonicalStore::new(client.clone(), "primary", outbox_relation);
                match ensure_full_system_store_tables(&store).await {
                    Ok(()) => {
                        let store: std::sync::Arc<dyn SystemStores> = std::sync::Arc::new(store);
                        runtime.register_full_canonical_store(store);
                        tracing::info!("SQL Server canonical store registered (B.8)");
                    }
                    Err(err) => {
                        report.warnings.push(format!(
                            "SQL Server canonical store not registered \
                             (ensure_full_system_store_tables failed): {err}"
                        ));
                    }
                }
            }
            runtime
                .mssql_instances
                .insert(instance.name.clone(), client);
        }
    }
}

/// C9: register Weaviate. DSN form:
/// `http://host:8080` (no auth) or
/// `apikey://<api-key>@host:8080` (Weaviate Cloud).
#[cfg(feature = "weaviate")]
pub(crate) async fn register_weaviate(ctx: &mut RegisterCtx<'_>) {
    use crate::runtime::executors::weaviate::WeaviateHttpClient;
    let RegisterCtx {
        runtime, report, ..
    } = ctx;
    let Some(raw) = std::env::var("UDB_WEAVIATE_DSN")
        .ok()
        .filter(|s| !s.trim().is_empty())
    else {
        return;
    };
    let (base, api_key) = parse_weaviate_dsn(&raw);
    let client = WeaviateHttpClient::new(base, api_key);
    report.weaviate_configured = true;
    runtime
        .weaviate_instances
        .insert("primary".to_string(), client.clone());
    if let Err(err) =
        ensure_full_canonical_store_registration_allowed(crate::backend::BackendKind::Weaviate)
    {
        report
            .warnings
            .push(format!("{err}; vector executor remains available"));
    } else {
        use crate::runtime::canonical_store::CanonicalStore;
        use crate::runtime::canonical_store::SystemStores;
        use crate::runtime::canonical_store::vector_system::VectorSystemCanonicalStore;

        let store = VectorSystemCanonicalStore::new_weaviate(client.clone(), "primary");
        match CanonicalStore::ensure_system_tables(&store).await {
            Ok(()) => {
                let store: std::sync::Arc<dyn SystemStores> = std::sync::Arc::new(store);
                runtime.register_full_canonical_store(store);
                tracing::info!("Weaviate canonical SystemStores registered");
            }
            Err(err) => report.warnings.push(format!(
                "Weaviate canonical store not registered: {err}; vector executor remains available"
            )),
        }
    }
    runtime.weaviate = Some(client);
}

#[cfg(feature = "weaviate")]
pub(crate) fn parse_weaviate_dsn(raw: &str) -> (String, Option<String>) {
    let trimmed = raw.trim();
    if let Some(rest) = trimmed.strip_prefix("apikey://")
        && let Some((key, host)) = rest.split_once('@')
    {
        return (format!("https://{host}"), Some(key.to_string()));
    }
    (trimmed.to_string(), None)
}

/// C9: register Pinecone. DSN form:
/// `apikey://<api-key>@<index>-<project>.svc.<env>.pinecone.io`
/// — Pinecone always uses HTTPS + an API key.
#[cfg(feature = "pinecone")]
pub(crate) async fn register_pinecone(ctx: &mut RegisterCtx<'_>) {
    use crate::runtime::executors::pinecone::PineconeHttpClient;
    let RegisterCtx {
        runtime, report, ..
    } = ctx;
    let Some(raw) = std::env::var("UDB_PINECONE_DSN")
        .ok()
        .filter(|s| !s.trim().is_empty())
    else {
        return;
    };
    let Some(rest) = raw.strip_prefix("apikey://") else {
        tracing::warn!("UDB_PINECONE_DSN must be of the form apikey://<key>@<host>; ignoring");
        return;
    };
    let Some((key, host)) = rest.split_once('@') else {
        tracing::warn!("UDB_PINECONE_DSN: malformed (missing @host); ignoring");
        return;
    };
    let client = PineconeHttpClient::new(format!("https://{host}"), key);
    report.pinecone_configured = true;
    runtime
        .pinecone_instances
        .insert("primary".to_string(), client.clone());
    if let Err(err) =
        ensure_full_canonical_store_registration_allowed(crate::backend::BackendKind::Pinecone)
    {
        report
            .warnings
            .push(format!("{err}; vector executor remains available"));
    } else {
        use crate::runtime::canonical_store::CanonicalStore;
        use crate::runtime::canonical_store::SystemStores;
        use crate::runtime::canonical_store::vector_system::VectorSystemCanonicalStore;

        let store = VectorSystemCanonicalStore::new_pinecone(client.clone(), "primary");
        match CanonicalStore::ensure_system_tables(&store).await {
            Ok(()) => {
                let store: std::sync::Arc<dyn SystemStores> = std::sync::Arc::new(store);
                runtime.register_full_canonical_store(store);
                tracing::info!("Pinecone canonical SystemStores registered");
            }
            Err(err) => report.warnings.push(format!(
                "Pinecone canonical store not registered: {err}; vector executor remains available"
            )),
        }
    }
    runtime.pinecone = Some(client);
}

/// C9: register Cassandra / ScyllaDB. DSN forms:
///   `host:9042`                      → single node, no auth
///   `host1:9042,host2:9042`          → multi-node bootstrap
///   `user:pass@host:9042`            → password authenticator
#[cfg(feature = "cassandra")]
pub(crate) async fn register_cassandra(ctx: &mut RegisterCtx<'_>) {
    let RegisterCtx {
        instance_config,
        runtime,
        report,
        ..
    } = ctx;
    for instance in instance_config.active().filter(|instance| {
        instance_matches_backend(instance, crate::backend::BackendKind::Cassandra)
    }) {
        if runtime.cassandra_instances.contains_key(&instance.name) {
            continue;
        }
        match cassandra_executor_from_instance(instance).await {
            Ok(Some(client)) => {
                report.cassandra_configured = true;
                if runtime.cassandra.is_none() {
                    runtime.cassandra = Some(client.clone());
                }
                // B.10a: register the Cassandra canonical store for the primary
                // instance. Fail-closed on ensure_system_tables (keyspace +
                // tables created via LWT-safe idempotent DDL).
                if instance.name == "primary" {
                    use crate::runtime::canonical_store::CanonicalStore;
                    use crate::runtime::canonical_store::SystemStores;
                    use crate::runtime::canonical_store::cassandra::CassandraCanonicalStore;
                    let store = CassandraCanonicalStore::new(
                        client.clone(),
                        "primary",
                        "udb",
                        "udb_outbox_events",
                    );
                    match CanonicalStore::ensure_system_tables(&store).await {
                        Ok(()) => {
                            let store: std::sync::Arc<dyn SystemStores> =
                                std::sync::Arc::new(store);
                            runtime.register_full_canonical_store(store);
                            tracing::info!("Cassandra canonical store registered (B.10a)");
                        }
                        Err(err) => report.warnings.push(format!(
                            "Cassandra canonical store not registered \
                             (ensure_system_tables failed): {err}"
                        )),
                    }
                }
                runtime
                    .cassandra_instances
                    .insert(instance.name.clone(), client);
            }
            Ok(None) => {}
            Err(err) => {
                report.warnings.push(format!(
                    "Cassandra instance '{}' unavailable: {err}",
                    instance.name
                ));
            }
        }
    }
}

/// C9: register Azure Blob Storage. DSN form:
///   `account=<name>;key=<base64>`
/// Parses out the account + access key; uses
/// `StorageCredentials::access_key` for auth.
#[cfg(feature = "azureblob")]
pub(crate) fn parse_azureblob_dsn(dsn: &str) -> Option<(String, String)> {
    let mut account = String::new();
    let mut key = String::new();
    for kv in dsn.split(';') {
        if let Some((k, v)) = kv.split_once('=') {
            match k.trim().to_lowercase().as_str() {
                "account" | "accountname" => account = v.trim().to_string(),
                "key" | "accountkey" => key = v.trim().to_string(),
                _ => {}
            }
        }
    }
    if account.is_empty() || key.is_empty() {
        return None;
    }
    Some((account, key))
}

#[cfg(feature = "azureblob")]
pub(crate) async fn register_azureblob(ctx: &mut RegisterCtx<'_>) {
    use crate::runtime::executors::azureblob::AzureBlobClient;
    let RegisterCtx {
        runtime, report, ..
    } = ctx;
    let Some(dsn) = std::env::var("UDB_AZUREBLOB_DSN")
        .ok()
        .filter(|s| !s.trim().is_empty())
    else {
        return;
    };
    let Some((account, key)) = parse_azureblob_dsn(&dsn) else {
        report.warnings.push(format!(
            "Azure Blob DSN missing account/key (expected `account=…;key=…`)"
        ));
        return;
    };
    let client = AzureBlobClient::from_account_key(&account, &key);
    report.azureblob_configured = true;
    runtime
        .azureblob_instances
        .insert("primary".to_string(), client.clone());
    runtime.azureblob = Some(client);
}

/// C9: register Google Cloud Storage. DSN form: just the GCP project
/// ID — auth uses Application Default Credentials so the operator
/// sets `GOOGLE_APPLICATION_CREDENTIALS` to a service-account JSON
/// path (or runs in a GCE/GKE workload-identity context).
#[cfg(feature = "gcs")]
pub(crate) async fn register_gcs(ctx: &mut RegisterCtx<'_>) {
    use crate::runtime::executors::gcs::GcsClient;
    let RegisterCtx {
        runtime, report, ..
    } = ctx;
    let Some(project) = std::env::var("UDB_GCS_DSN")
        .ok()
        .filter(|s| !s.trim().is_empty())
    else {
        return;
    };
    match GcsClient::new(&project).await {
        Ok(client) => {
            report.gcs_configured = true;
            runtime
                .gcs_instances
                .insert("primary".to_string(), client.clone());
            runtime.gcs = Some(client);
        }
        Err(err) => {
            report.warnings.push(format!("GCS unavailable: {err}"));
        }
    }
}

/// Wire the default Redis client into the runtime (when configured).
#[cfg(feature = "redis")]
pub(crate) async fn register_redis(ctx: &mut RegisterCtx<'_>) {
    let RegisterCtx {
        config,
        instance_config,
        runtime,
        report,
        ..
    } = ctx;
    if let Some(redis_config) = &config.redis
        && let Some(dsn) = redis_dsn_from_config(redis_config)
    {
        match redis::Client::open(dsn) {
            Ok(client) => {
                report.redis_configured = true;
                runtime
                    .redis_instances
                    .insert("default".to_string(), client.clone());
                runtime.connections.register_redis(
                    "default",
                    "read_write",
                    client.clone(),
                    HashMap::new(),
                );
                {
                    use crate::runtime::canonical_store::CanonicalStore;
                    use crate::runtime::canonical_store::SystemStores;
                    use crate::runtime::canonical_store::redis::RedisCanonicalStore;
                    let store = RedisCanonicalStore::new(client.clone(), "default");
                    match CanonicalStore::ensure_system_tables(&store).await {
                        Ok(()) => {
                            let store: std::sync::Arc<dyn SystemStores> =
                                std::sync::Arc::new(store);
                            runtime.register_full_canonical_store(store);
                            tracing::info!(
                                "Redis canonical store registered with durable AOF profile (B.14)"
                            );
                        }
                        Err(err) => report.warnings.push(format!(
                            "Redis canonical store not registered: {err}; cache executor remains available"
                        )),
                    }
                }
                runtime.redis = Some(client);
            }
            Err(err) => report.warnings.push(format!("Redis disabled: {err}")),
        }
    }

    for instance in instance_config
        .active()
        .filter(|instance| instance_matches_backend(instance, crate::backend::BackendKind::Redis))
    {
        if runtime.redis_instances.contains_key(&instance.name) {
            continue;
        }
        let Some(dsn) = instance.resolve_dsn() else {
            continue;
        };
        match redis::Client::open(dsn) {
            Ok(client) => {
                report.redis_configured = true;
                if runtime.redis.is_none() {
                    runtime.redis = Some(client.clone());
                }
                runtime.connections.register_redis(
                    &instance.name,
                    instance.role.as_str(),
                    client.clone(),
                    instance_labels(instance),
                );
                {
                    use crate::runtime::canonical_store::CanonicalStore;
                    use crate::runtime::canonical_store::SystemStores;
                    use crate::runtime::canonical_store::redis::RedisCanonicalStore;
                    let store = RedisCanonicalStore::new(client.clone(), instance.name.clone());
                    match CanonicalStore::ensure_system_tables(&store).await {
                        Ok(()) => {
                            let store: std::sync::Arc<dyn SystemStores> =
                                std::sync::Arc::new(store);
                            runtime.register_full_canonical_store(store);
                            tracing::info!(
                                instance = %instance.name,
                                "Redis canonical store registered with durable AOF profile (B.14)"
                            );
                        }
                        Err(err) => report.warnings.push(format!(
                            "Redis instance {} canonical store not registered: {err}; cache executor remains available",
                            instance.name
                        )),
                    }
                }
                runtime
                    .redis_instances
                    .insert(instance.name.clone(), client);
            }
            Err(err) => report
                .warnings
                .push(format!("Redis instance {} disabled: {err}", instance.name)),
        }
    }
}

/// Wire the default Qdrant client into the runtime (when configured).
#[cfg(feature = "qdrant")]
pub(crate) async fn register_qdrant(ctx: &mut RegisterCtx<'_>) {
    let RegisterCtx {
        config,
        instance_config,
        runtime,
        report,
        ..
    } = ctx;
    if let Some(qdrant_config) = &config.qdrant
        && let Some(url) = qdrant_url_from_config(qdrant_config)
    {
        let qdrant_http =
            crate::runtime::executors::http::HttpClientSpec::with_timeout(Duration::from_secs(30))
                .build();
        report.qdrant_configured = true;
        let client = QdrantHttpClient {
            base_url: url.trim_end_matches('/').to_string(),
            // Store the TRIMMED key: an untrimmed value (e.g. a trailing CRLF
            // `\r` from a Windows `.env`) becomes an invalid HTTP header value and
            // surfaces only as an opaque reqwest "builder error" (UDB_FRICTION §3).
            api_key: {
                let trimmed = qdrant_config.api_key.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_string())
            },
            http: qdrant_http,
        };
        runtime
            .qdrant_instances
            .insert("default".to_string(), client.clone());
        runtime.connections.register_qdrant(
            "default",
            "read_write",
            client.clone(),
            HashMap::new(),
        );
        if let Err(err) =
            ensure_full_canonical_store_registration_allowed(crate::backend::BackendKind::Qdrant)
        {
            report
                .warnings
                .push(format!("{err}; vector executor remains available"));
        } else {
            use crate::runtime::canonical_store::CanonicalStore;
            use crate::runtime::canonical_store::SystemStores;
            use crate::runtime::canonical_store::qdrant::QdrantCanonicalStore;

            let store = QdrantCanonicalStore::new(client.clone(), "default");
            match CanonicalStore::ensure_system_tables(&store).await {
                Ok(()) => {
                    let store: std::sync::Arc<dyn SystemStores> = std::sync::Arc::new(store);
                    runtime.register_full_canonical_store(store);
                    tracing::info!(
                        "Qdrant canonical SystemStores registered for default instance"
                    );
                }
                Err(err) => report.warnings.push(format!(
                    "Qdrant default canonical store not registered: {err}; vector executor remains available"
                )),
            }
        }
        runtime.qdrant = Some(client);
    }

    for instance in instance_config
        .active()
        .filter(|instance| instance_matches_backend(instance, crate::backend::BackendKind::Qdrant))
    {
        if runtime.qdrant_instances.contains_key(&instance.name) {
            continue;
        }
        if let Some(client) = qdrant_client_from_instance(instance) {
            report.qdrant_configured = true;
            if runtime.qdrant.is_none() {
                runtime.qdrant = Some(client.clone());
            }
            runtime.connections.register_qdrant(
                &instance.name,
                instance.role.as_str(),
                client.clone(),
                instance_labels(instance),
            );
            if let Err(err) = ensure_full_canonical_store_registration_allowed(
                crate::backend::BackendKind::Qdrant,
            ) {
                report
                    .warnings
                    .push(format!("{err}; vector executor remains available"));
            } else {
                use crate::runtime::canonical_store::CanonicalStore;
                use crate::runtime::canonical_store::SystemStores;
                use crate::runtime::canonical_store::qdrant::QdrantCanonicalStore;

                let store = QdrantCanonicalStore::new(client.clone(), instance.name.clone());
                match CanonicalStore::ensure_system_tables(&store).await {
                    Ok(()) => {
                        let store: std::sync::Arc<dyn SystemStores> = std::sync::Arc::new(store);
                        runtime.register_full_canonical_store(store);
                        tracing::info!(
                            instance = %instance.name,
                            "Qdrant canonical SystemStores registered for instance"
                        );
                    }
                    Err(err) => report.warnings.push(format!(
                        "Qdrant instance {} canonical store not registered: {err}; vector executor remains available",
                        instance.name
                    )),
                }
            }
            runtime
                .qdrant_instances
                .insert(instance.name.clone(), client);
        }
    }
}

/// Wire the default S3/MinIO client into the runtime (when configured).
#[cfg(feature = "s3")]
pub(crate) async fn register_s3(ctx: &mut RegisterCtx<'_>) {
    let RegisterCtx {
        config,
        instance_config,
        runtime,
        report,
        ..
    } = ctx;
    if let Some(minio_config) = &config.minio
        && minio_config.is_configured()
    {
        match s3_client_from_config(minio_config).await {
            Ok(client) => {
                report.s3_configured = true;
                runtime
                    .s3_instances
                    .insert("default".to_string(), client.clone());
                runtime.connections.register_s3(
                    "minio",
                    "default",
                    "read_write",
                    client.clone(),
                    HashMap::new(),
                );
                runtime.s3 = Some(client);
            }
            Err(err) => {
                report
                    .warnings
                    .push(format!("S3/MinIO endpoint configured but {err}"));
            }
        }
    }

    for instance in instance_config.active().filter(|instance| {
        instance_matches_backend(instance, crate::backend::BackendKind::Minio)
            || instance_matches_backend(instance, crate::backend::BackendKind::S3)
    }) {
        if runtime.s3_instances.contains_key(&instance.name) {
            continue;
        }
        match s3_client_from_instance(instance).await {
            Ok(Some(client)) => {
                report.s3_configured = true;
                if runtime.s3.is_none() {
                    runtime.s3 = Some(client.clone());
                }
                let backend = instance
                    .canonical_backend()
                    .map(|kind| kind.as_str().to_string())
                    .unwrap_or_else(|| "minio".to_string());
                runtime.connections.register_s3(
                    &backend,
                    &instance.name,
                    instance.role.as_str(),
                    client.clone(),
                    instance_labels(instance),
                );
                runtime.s3_instances.insert(instance.name.clone(), client);
            }
            Ok(None) => {}
            Err(err) => report.warnings.push(format!(
                "S3/MinIO instance {} disabled: {err}",
                instance.name
            )),
        }
    }
}

/// Wire MongoDB executors into the runtime.
#[cfg(feature = "mongodb")]
pub(crate) async fn register_mongodb(ctx: &mut RegisterCtx<'_>) {
    let RegisterCtx {
        instance_config,
        runtime,
        report,
        ..
    } = ctx;

    for instance in instance_config
        .active()
        .filter(|instance| instance_matches_backend(instance, crate::backend::BackendKind::Mongodb))
    {
        if runtime.mongodb_instances.contains_key(&instance.name) {
            continue;
        }
        match mongodb_executor_from_instance(instance).await {
            Ok(Some(executor)) => {
                report.mongodb_configured = true;
                if runtime.mongodb.is_none() {
                    runtime.mongodb = Some(executor.clone());
                }
                runtime.connections.register_mongodb(
                    &instance.name,
                    instance.role.as_str(),
                    executor.clone(),
                    instance_labels(instance),
                );
                // B.9: register the native MongoDB canonical store for the
                // primary instance. Fail-closed — requires the mongodb-native
                // build, a replica-set/sharded topology (standalone mongod stays
                // projection), and a successful ensure_system_tables.
                #[cfg(feature = "mongodb-native")]
                if instance.name == "primary" {
                    use crate::runtime::canonical_store::CanonicalStore;
                    use crate::runtime::canonical_store::SystemStores;
                    use crate::runtime::canonical_store::mongodb::MongoDbCanonicalStore;
                    use mongodb_driver::bson::doc;
                    // Topology guard via the driver's `hello` (replica set →
                    // setName, sharded → msg=="isdbgrid"); standalone mongod
                    // cannot run the session transactions the canonical store
                    // needs, so it stays projection-only.
                    let topology_ok = match executor.native_database() {
                        Some(db) => match db.run_command(doc! { "hello": 1 }).await {
                            Ok(hello) => {
                                hello.contains_key("setName")
                                    || hello
                                        .get_str("msg")
                                        .map(|m| m == "isdbgrid")
                                        .unwrap_or(false)
                            }
                            Err(_) => false,
                        },
                        None => false,
                    };
                    if !topology_ok {
                        report.warnings.push(
                            "MongoDB canonical store not registered: native canonical \
                             storage requires a replica set or sharded cluster"
                                .to_string(),
                        );
                    } else if let Some(store) = MongoDbCanonicalStore::from_executor(
                        &executor,
                        "primary",
                        "udb_outbox_events",
                    ) {
                        match CanonicalStore::ensure_system_tables(&store).await {
                            Ok(()) => {
                                let store: std::sync::Arc<dyn SystemStores> =
                                    std::sync::Arc::new(store);
                                runtime.register_full_canonical_store(store);
                                tracing::info!("MongoDB native canonical store registered (B.9)");
                            }
                            Err(err) => report.warnings.push(format!(
                                "MongoDB canonical store not registered \
                                 (ensure_system_tables failed): {err}"
                            )),
                        }
                    }
                }
                runtime
                    .mongodb_instances
                    .insert(instance.name.clone(), executor);
            }
            Ok(None) => {}
            Err(err) => report.warnings.push(format!(
                "MongoDB instance {} disabled: {err}",
                instance.name
            )),
        }
    }
}

/// Wire Neo4j executors into the runtime.
#[cfg(feature = "neo4j")]
pub(crate) async fn register_neo4j(ctx: &mut RegisterCtx<'_>) {
    let RegisterCtx {
        instance_config,
        runtime,
        report,
        ..
    } = ctx;

    for instance in instance_config
        .active()
        .filter(|instance| instance_matches_backend(instance, crate::backend::BackendKind::Neo4j))
    {
        if runtime.neo4j_instances.contains_key(&instance.name) {
            continue;
        }
        if let Some(executor) = neo4j_executor_from_instance(instance) {
            report.neo4j_configured = true;
            if runtime.neo4j.is_none() {
                runtime.neo4j = Some(executor.clone());
            }
            runtime.connections.register_neo4j(
                &instance.name,
                instance.role.as_str(),
                executor.clone(),
                instance_labels(instance),
            );
            // B.10b: register the Neo4j canonical store for the primary instance.
            // Fail-closed on ensure_system_tables (constraints/indexes created via
            // idempotent Cypher).
            #[cfg(feature = "neo4j")]
            if instance.name == "primary" {
                use crate::runtime::canonical_store::CanonicalStore;
                use crate::runtime::canonical_store::SystemStores;
                use crate::runtime::canonical_store::neo4j::Neo4jCanonicalStore;
                let store = Neo4jCanonicalStore::new(executor.clone(), "primary");
                match CanonicalStore::ensure_system_tables(&store).await {
                    Ok(()) => {
                        let store: std::sync::Arc<dyn SystemStores> = std::sync::Arc::new(store);
                        runtime.register_full_canonical_store(store);
                        tracing::info!("Neo4j canonical store registered (B.10b)");
                    }
                    Err(err) => report.warnings.push(format!(
                        "Neo4j canonical store not registered \
                         (ensure_system_tables failed): {err}"
                    )),
                }
            }
            runtime
                .neo4j_instances
                .insert(instance.name.clone(), executor);
        }
    }
}

/// Wire ClickHouse executors into the runtime.
#[cfg(feature = "clickhouse")]
pub(crate) async fn register_clickhouse(ctx: &mut RegisterCtx<'_>) {
    let RegisterCtx {
        instance_config,
        runtime,
        report,
        ..
    } = ctx;

    for instance in instance_config.active().filter(|instance| {
        instance_matches_backend(instance, crate::backend::BackendKind::Clickhouse)
    }) {
        if runtime.clickhouse_instances.contains_key(&instance.name) {
            continue;
        }
        if let Some(executor) = clickhouse_executor_from_instance(instance) {
            report.clickhouse_configured = true;
            if runtime.clickhouse.is_none() {
                runtime.clickhouse = Some(executor.clone());
            }
            runtime.connections.register_clickhouse(
                &instance.name,
                instance.role.as_str(),
                executor.clone(),
                instance_labels(instance),
            );
            // B.10c: register the ClickHouse canonical store for the primary
            // instance. Fail-closed on ensure_system_tables (MergeTree +
            // ReplacingMergeTree tables created via idempotent DDL). The store's
            // sequence/lease CAS is single-writer only, so production promotion
            // requires the same explicit opt-in used for Projection-role stores.
            #[cfg(feature = "clickhouse")]
            if instance.name == "primary" {
                if let Err(err) = ensure_full_canonical_store_registration_allowed(
                    crate::backend::BackendKind::Clickhouse,
                ) {
                    report
                        .warnings
                        .push(format!("{err}; ClickHouse executor remains available"));
                } else {
                    use crate::runtime::canonical_store::CanonicalStore;
                    use crate::runtime::canonical_store::SystemStores;
                    use crate::runtime::canonical_store::clickhouse::ClickHouseCanonicalStore;
                    let db = executor.database().to_string();
                    let store = ClickHouseCanonicalStore::new(executor.clone(), "primary", db);
                    match CanonicalStore::ensure_system_tables(&store).await {
                        Ok(()) => {
                            let store: std::sync::Arc<dyn SystemStores> =
                                std::sync::Arc::new(store);
                            runtime.register_full_canonical_store(store);
                            tracing::info!("ClickHouse canonical store registered (B.10c opt-in)");
                        }
                        Err(err) => report.warnings.push(format!(
                            "ClickHouse canonical store not registered \
                             (ensure_system_tables failed): {err}"
                        )),
                    }
                }
            }
            runtime
                .clickhouse_instances
                .insert(instance.name.clone(), executor);
        }
    }
}

fn instance_matches_backend(instance: &BackendInstance, kind: crate::backend::BackendKind) -> bool {
    instance
        .canonical_backend()
        .map(|candidate| candidate == kind)
        .unwrap_or(false)
}

fn instance_labels(instance: &BackendInstance) -> HashMap<String, String> {
    instance
        .labels
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn vector_route_key(project_id: &str, collection: &str) -> String {
    format!("{}:{}", project_id.trim(), collection.trim())
}

fn reject_vector_plan_errors(
    errors: &[String],
    routed_ad_hoc_collection: bool,
) -> Result<(), tonic::Status> {
    if !routed_ad_hoc_collection {
        return reject_plan(errors);
    }
    let remaining = errors
        .iter()
        .filter(|error| !error.starts_with("unknown vector collection "))
        .cloned()
        .collect::<Vec<_>>();
    reject_plan(&remaining)
}

fn vector_search_dispatch_spec(
    backend: &str,
    request: &VectorSearchRequest,
) -> Result<String, tonic::Status> {
    let limit = if request.limit > 0 { request.limit } else { 10 };
    let spec = match backend {
        "elasticsearch" => serde_json::json!({
            "method": "POST",
            "path": format!("/{}/_search", request.collection.to_ascii_lowercase()),
            "body": {
                "size": limit,
                "query": {
                    "script_score": {
                        "query": { "match_all": {} },
                        "script": {
                            "source": "cosineSimilarity(params.query_vector, 'vector') + 1.0",
                            "params": { "query_vector": request.vector }
                        }
                    }
                }
            }
        }),
        "weaviate" => {
            let class_name = vector_weaviate_class_name(&request.collection);
            serde_json::json!({
                "method": "POST",
                "path": "/v1/graphql",
                "body": {
                    "query": format!(
                        "{{ Get {{ {class_name}(nearVector: {{ vector: {:?} }}, limit: {limit}) {{ _additional {{ id distance certainty }} }} }} }}",
                        request.vector
                    )
                }
            })
        }
        "pinecone" => serde_json::json!({
            "method": "POST",
            "path": "/query",
            "body": {
                "vector": request.vector,
                "topK": limit,
                "includeMetadata": request.with_payload
            }
        }),
        other => {
            return Err(setup_data_capability_status(
                other,
                "typed_vector_search",
                "typed_vector_search_backend",
                format!("typed vector search is not wired for backend '{other}'"),
            ));
        }
    };
    serde_json::to_string(&spec)
        .map_err(|err| setup_data_internal_status("vector_search_spec_encode", err.to_string()))
}

fn vector_upsert_dispatch_spec(
    backend: &str,
    collection: &str,
    point: &VectorPointMutation,
) -> Result<String, tonic::Status> {
    let payload = point
        .payload
        .as_ref()
        .map(struct_to_json)
        .unwrap_or(JsonValue::Null);
    let spec = match backend {
        "elasticsearch" => {
            let mut body = serde_json::Map::new();
            body.insert("vector".to_string(), serde_json::json!(point.vector));
            body.insert("payload".to_string(), payload);
            serde_json::json!({
                "method": "PUT",
                "path": format!(
                    "/{}/_doc/{}?refresh=true",
                    collection.to_ascii_lowercase(),
                    urlencoding::encode(&point.id)
                ),
                "body": JsonValue::Object(body)
            })
        }
        "weaviate" => {
            let class_name = vector_weaviate_class_name(collection);
            serde_json::json!({
                "method": "POST",
                "path": "/v1/objects",
                "body": {
                    "class": class_name,
                    "properties": payload,
                    "vector": point.vector
                }
            })
        }
        "pinecone" => serde_json::json!({
            "method": "POST",
            "path": "/vectors/upsert",
            "body": {
                "vectors": [{
                    "id": point.id,
                    "values": point.vector,
                    "metadata": payload
                }]
            }
        }),
        other => {
            return Err(setup_data_capability_status(
                other,
                "typed_vector_upsert",
                "typed_vector_upsert_backend",
                format!("typed vector upsert is not wired for backend '{other}'"),
            ));
        }
    };
    serde_json::to_string(&spec)
        .map_err(|err| setup_data_internal_status("vector_upsert_spec_encode", err.to_string()))
}

fn parse_vector_search_response(backend: &str, raw: &str) -> Result<VectorSet, tonic::Status> {
    let parsed: JsonValue = serde_json::from_str(raw).map_err(|err| {
        setup_data_internal_status(
            "vector_search_response_parse",
            format!("vector search response parse: {err}"),
        )
    })?;
    let points = match backend {
        "elasticsearch" => parsed
            .pointer("/hits/hits")
            .and_then(JsonValue::as_array)
            .map(|hits| {
                hits.iter()
                    .map(|hit| VectorPoint {
                        id: hit
                            .get("_id")
                            .and_then(JsonValue::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        score: hit.get("_score").and_then(JsonValue::as_f64).unwrap_or(0.0) as f32,
                        payload: hit.get("_source").cloned().and_then(json_into_struct),
                        vector: Vec::new(),
                        vector_name: String::new(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        "pinecone" => parsed
            .get("matches")
            .and_then(JsonValue::as_array)
            .map(|matches| {
                matches
                    .iter()
                    .map(|item| VectorPoint {
                        id: item
                            .get("id")
                            .and_then(JsonValue::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        score: item.get("score").and_then(JsonValue::as_f64).unwrap_or(0.0) as f32,
                        payload: item.get("metadata").cloned().and_then(json_into_struct),
                        vector: item
                            .get("values")
                            .and_then(JsonValue::as_array)
                            .map(|values| {
                                values
                                    .iter()
                                    .filter_map(JsonValue::as_f64)
                                    .map(|value| value as f32)
                                    .collect()
                            })
                            .unwrap_or_default(),
                        vector_name: String::new(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        "weaviate" => Vec::new(),
        _ => Vec::new(),
    };
    Ok(VectorSet { points })
}

fn vector_weaviate_class_name(resource_name: &str) -> String {
    let mut out = String::new();
    for ch in resource_name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
        } else if ch == '_' || ch == '-' {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push_str("UdbVector");
    }
    if !out.chars().next().is_some_and(|ch| ch.is_ascii_uppercase()) {
        out.insert_str(0, "Udb");
    }
    out
}

/// The gRPC `Chunk` stream returned by the typed `GetObject` path.
#[cfg(any(feature = "s3", feature = "gcs", feature = "azureblob"))]
type ObjectChunkStream = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<Chunk, tonic::Status>> + Send + 'static>,
>;

/// Build the alias-rich request JSON the object executors parse (each reads its
/// own bucket/key aliases; including every alias keeps the call backend-agnostic).
#[cfg(any(feature = "s3", feature = "gcs", feature = "azureblob"))]
/// Build a presigned S3/MinIO URL for `method` ("PUT"/"GET") — the single home
/// for the presign call, shared by `generate_presigned_url` (manifest/policy path)
/// and `presign_object_backend_target` (admin/native path).
#[cfg(feature = "s3")]
async fn presign_s3_url(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    object_key: &str,
    method: &str,
    content_type: &str,
    ttl: u64,
) -> Result<String, tonic::Status> {
    if method != "PUT" && method != "GET" {
        return Err(unsupported_presign_method_status());
    }
    let config =
        aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(ttl))
            .map_err(invalid_presign_ttl_status)?;
    // The PUT and GET presign calls return different SdkError<…> error types, so
    // each branch maps its own result to the shared `String` URL (or a
    // `tonic::Status`) before the `if`/`else` joins — keeping both arms the same
    // type.
    if method == "PUT" {
        s3.put_object()
            .bucket(bucket)
            .key(object_key)
            .set_content_type(if content_type.is_empty() {
                None
            } else {
                Some(content_type.to_string())
            })
            .presigned(config)
            .await
            .map(|p| p.uri().to_string())
            .map_err(|err| {
                crate::runtime::executor_utils::backend_transport_status("S3", "presign", err)
            })
    } else {
        s3.get_object()
            .bucket(bucket)
            .key(object_key)
            .presigned(config)
            .await
            .map(|p| p.uri().to_string())
            .map_err(|err| {
                crate::runtime::executor_utils::backend_transport_status("S3", "presign", err)
            })
    }
}

pub(crate) fn object_request_json(
    op: &str,
    bucket: &str,
    object_key: &str,
    content_type: &str,
) -> String {
    let mut value = serde_json::json!({
        "op": op,
        "bucket": bucket,
        "container": bucket,
        "object_key": object_key,
        "key": object_key,
        "object": object_key,
        "blob": object_key,
    });
    if !content_type.trim().is_empty() {
        value["content_type"] = serde_json::Value::String(content_type.to_string());
    }
    value.to_string()
}

/// Adapt a gRPC `Streaming<Chunk>` (first chunk already pulled, to read
/// bucket/key/context) into the [`ExecutorByteStream`] an object executor's
/// `put_object_stream` consumes. Enforces `UDB_MAX_OBJECT_BYTES` cumulatively and
/// records bytes/chunks seen for tracing. `Chunk.data` is `bytes::Bytes` (A.5), so
/// forwarding is zero-copy.
#[cfg(any(feature = "s3", feature = "gcs", feature = "azureblob"))]
fn grpc_put_byte_stream(
    first: bytes::Bytes,
    rest: tonic::Streaming<Chunk>,
    max_bytes: u64,
    bytes_seen: std::sync::Arc<std::sync::atomic::AtomicU64>,
    chunks_seen: std::sync::Arc<std::sync::atomic::AtomicU64>,
) -> crate::runtime::executors::ExecutorByteStream {
    use std::sync::atomic::Ordering;
    Box::pin(async_stream::try_stream! {
        let mut total = first.len() as u64;
        if total > max_bytes {
            Err(crate::runtime::executor_utils::quota_refusal_status(
                "object",
                "grpc object stream size",
                format!("object exceeds UDB_MAX_OBJECT_BYTES ({max_bytes})"),
            ))?;
        }
        bytes_seen.store(total, Ordering::Relaxed);
        chunks_seen.store(1, Ordering::Relaxed);
        yield first;
        let mut rest = rest;
        while let Some(chunk) = rest.next().await {
            let chunk = chunk?;
            total += chunk.data.len() as u64;
            if total > max_bytes {
                Err(crate::runtime::executor_utils::quota_refusal_status(
                    "object",
                    "grpc object stream size",
                    format!("object exceeds UDB_MAX_OBJECT_BYTES ({max_bytes})"),
                ))?;
            }
            bytes_seen.store(total, Ordering::Relaxed);
            chunks_seen.fetch_add(1, Ordering::Relaxed);
            yield chunk.data;
        }
    })
}

/// Wrap an executor's download [`ExecutorByteStream`] into the gRPC `Chunk` stream
/// the typed `GetObject` returns: marks the final chunk and logs bytes/chunks
/// streamed when the stream completes.
#[cfg(any(feature = "s3", feature = "gcs", feature = "azureblob"))]
fn byte_stream_to_chunk_stream(
    src: crate::runtime::executors::ExecutorByteStream,
    bucket: String,
    object_key: String,
    backend: String,
) -> ObjectChunkStream {
    Box::pin(async_stream::try_stream! {
        let mut src = src;
        let mut pending: Option<bytes::Bytes> = None;
        let mut bytes_total: u64 = 0;
        let mut chunk_count: u64 = 0;
        while let Some(item) = src.next().await {
            let data = item?;
            bytes_total += data.len() as u64;
            chunk_count += 1;
            if let Some(prev) = pending.replace(data) {
                yield Chunk {
                    bucket: bucket.clone(),
                    object_key: object_key.clone(),
                    data: prev,
                    final_chunk: false,
                    ..Chunk::default()
                };
            }
        }
        let last = pending.unwrap_or_default();
        tracing::info!(
            target: "udb::object",
            backend = %backend, bucket = %bucket, object_key = %object_key,
            chunks = chunk_count, bytes = bytes_total,
            "get_object streamed"
        );
        yield Chunk {
            bucket,
            object_key,
            data: last,
            final_chunk: true,
            ..Chunk::default()
        };
    })
}

/// Generic typed `PutObject` over any object executor: stream the chunks in,
/// enforce the size ceiling, log what was streamed.
#[cfg(any(feature = "s3", feature = "gcs", feature = "azureblob"))]
async fn stream_put_object<E: crate::runtime::executors::ObjectExecutor>(
    executor: &E,
    request_json: &str,
    first: bytes::Bytes,
    rest: tonic::Streaming<Chunk>,
    max_bytes: u64,
    backend: &str,
    bucket: &str,
    object_key: &str,
) -> Result<(), tonic::Status> {
    use std::sync::atomic::Ordering;
    let bytes_seen = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let chunks_seen = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let byte_stream = grpc_put_byte_stream(
        first,
        rest,
        max_bytes,
        bytes_seen.clone(),
        chunks_seen.clone(),
    );
    executor
        .put_object_stream(request_json, byte_stream)
        .await?;
    tracing::info!(
        target: "udb::object",
        backend = %backend, bucket = %bucket, object_key = %object_key,
        chunks = chunks_seen.load(Ordering::Relaxed),
        bytes = bytes_seen.load(Ordering::Relaxed),
        "put_object streamed"
    );
    Ok(())
}

/// Typed object RPCs (`GetObject`/`PutObject`) stream through object-store
/// backends declared by the backend capability metadata. Reject a store whose
/// manifest declares some other backend instead of silently using a default.
/// (`GeneratePresignedUrl` remains S3/MinIO-only — presigning is provider-specific.)
#[cfg(any(feature = "s3", feature = "gcs", feature = "azureblob"))]
fn ensure_typed_object_backend(backend: &str) -> Result<(), tonic::Status> {
    let normalized = backend.trim().to_ascii_lowercase();
    if normalized.is_empty()
        || crate::backend::BackendKind::from_token(&normalized)
            .map(|kind| kind.capabilities_v2().is_object_store)
            .unwrap_or(false)
    {
        Ok(())
    } else {
        Err(typed_object_backend_required_status(backend))
    }
}
