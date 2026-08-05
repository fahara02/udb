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

/// bug #8.2 — a `cdc_required` mutation whose change event cannot be durably
/// enqueued. `FAILED_PRECONDITION` with the `cdc_required` field named so the
/// caller learns which contract it violated and why delivery was impossible.
fn cdc_required_undeliverable_status(
    message_type: &str,
    reason: impl std::fmt::Display,
) -> tonic::Status {
    crate::runtime::executor_utils::failed_precondition_fields(
        format!("cdc_required mutation on {message_type} cannot be delivered: {reason}"),
        [("cdc_required".to_string(), reason.to_string())],
    )
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
        // P-1: keyset pagination engages only for an EXPLICITLY bounded read
        // (`limit > 0` BEFORE the default-limit clamp below), so unbounded selects
        // are unchanged. `page_token` continues an existing walk.
        let paginate = request.limit > 0;
        let page_token = request.page_token.trim().to_string();
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
        // P-1: when paginating, resolve the physical sort keys, extend them with
        // primary-key tiebreakers for a TOTAL order, decode any incoming cursor,
        // and inject the lexicographic "after" predicate into the filter (reusing
        // the validated wire grammar + tenant/RLS scoping, not new SQL).
        use crate::runtime::core::pagination;
        let mut filter = filter;
        // W11: route plaintext equality on encrypted columns through the
        // blind index before planning (planner still fails closed on shapes
        // the rewrite cannot express).
        if let Ok(table_for_encryption) = resolve_table_for_message(manifest, &request.message_type)
        {
            filter = self.rewrite_encrypted_equality_filters(
                table_for_encryption,
                &filter,
                &context.tenant_id,
            );
        }
        let mut fields = request.fields.clone();
        // #5: an include_revision read must carry every primary-key column in its
        // projection so each returned row can be keyed against the revision map (a
        // SELECT * — empty `fields` — already has them). Mirrors the pagination
        // cursor-key injection below. `revision_pk_columns` is the ordered PK used
        // to rebuild each row's revision key after the rows come back.
        let revision_pk_columns: Vec<String> = if request.include_revision {
            match resolve_table_for_message(manifest, &request.message_type) {
                Ok(table) => {
                    if !fields.is_empty() {
                        for pk in &table.primary_key {
                            if !fields.iter().any(|field| field == pk) {
                                fields.push(pk.clone());
                            }
                        }
                    }
                    table.primary_key.clone()
                }
                Err(_) => Vec::new(),
            }
        } else {
            Vec::new()
        };
        // P-1 / #7: `cursor_keys` carries the total-order keys, `page_query_digest`
        // the versioned digest of the query SHAPE (normalized filter, resolved sort,
        // caller projection) that minted the token. Both are set together (Some) iff
        // paginating, so a next-token is bound to the exact query it walks and a
        // token from a different filter/sort/projection is refused on decode.
        let (cursor_keys, page_query_digest): (Option<Vec<pagination::CursorKey>>, Option<String>) =
            if paginate {
                let table_for_keys = resolve_table_for_message(manifest, &request.message_type)
                    .map_err(|_| message_type_lookup_status(manifest, &request.message_type))?;
                let resolver = crate::planning::broker::column_resolver(table_for_keys);
                let resolved_sort: Vec<(String, bool)> = request
                    .sort
                    .iter()
                    .map(|s| {
                        (
                            crate::planning::broker::resolve_column(&resolver, &s.field),
                            s.descending,
                        )
                    })
                    .collect();
                let keys =
                    pagination::total_order_keys(&resolved_sort, &table_for_keys.primary_key);
                // #7: digest the query shape from the NORMALIZED base filter (BEFORE
                // the cursor predicate is injected below — otherwise the digest would
                // depend on which page we are on), the resolved sort, and the caller's
                // projection. A continuation request reproduces this exact digest only
                // when the query is unchanged.
                let query_digest = pagination::query_digest(
                    &crate::planning::broker::normalize_filter_keys(&resolver, &filter),
                    &resolved_sort,
                    &request.fields,
                );
                // The cursor + next-token need every key column present in the row, so
                // force them into an explicit projection (SELECT * already has them).
                if !fields.is_empty() {
                    for key in &keys {
                        if !fields.iter().any(|f| f == &key.column) {
                            fields.push(key.column.clone());
                        }
                    }
                }
                if !page_token.is_empty() {
                    let decoded = pagination::decode_page_token(
                        &page_token,
                        &context.tenant_id,
                        &request.message_type,
                        &query_digest,
                    )
                    .map_err(|msg| setup_data_invalid_field("page_token", &msg, &msg))?;
                    let values = pagination::cursor_values_for_keys(&keys, &decoded)
                        .map_err(|msg| setup_data_invalid_field("page_token", &msg, &msg))?;
                    let cursor_pred = pagination::build_cursor_predicate(&keys, &values);
                    filter = match filter {
                        JsonValue::Null => cursor_pred,
                        existing => serde_json::json!({ "$and": [existing, cursor_pred] }),
                    };
                }
                (Some(keys), Some(query_digest))
            } else {
                (None, None)
            };
        // The SQL sort is the total-order keys when paginating, else the caller's.
        let sort = match &cursor_keys {
            Some(keys) => keys
                .iter()
                .map(|key| SortSpec {
                    field: key.column.clone(),
                    descending: key.descending,
                })
                .collect::<Vec<_>>(),
            None => request
                .sort
                .iter()
                .map(|sort| SortSpec {
                    field: sort.field.clone(),
                    descending: sort.descending,
                })
                .collect::<Vec<_>>(),
        };
        let plan_request = SelectPlanRequest {
            context: context.clone(),
            message_type: request.message_type.clone(),
            filter: filter.clone(),
            fields: fields.clone(),
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
        // `limit`/`sort` are part of the read cache identity (X-1).
        let sort_repr = request
            .sort
            .iter()
            .map(|sort| format!("{}:{}", sort.field, sort.descending))
            .collect::<Vec<_>>()
            .join(",");
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
                request.limit,
                &sort_repr,
            ))
        };
        // 03.2.1.2: capture the typed stale-read warning side-channel (the proto
        // `RecordSet` cannot carry it) so the handler can emit the response header.
        // Enforced BEFORE the cache-hit return (X-2): a cache hit must not skip the
        // consistency fence or silently drop the stale-read warning. The fence
        // needs only `context`, so it runs ahead of table/pool resolution.
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
        // P-1: paginated reads skip the read cache — a cache hit returns
        // `cached_record_set` with no next_page_token, which would break the walk.
        if !bypass_read
            && !paginate
            // #5: an include_revision read skips the record cache — a cached
            // RecordSet carries no per-row revisions, so serving it would drop the
            // tokens the caller asked for (mirrors the paginated-read cache skip).
            && !request.include_revision
            && let Some(cache_key) = cache_key.as_deref()
            && let Some(cached) = self
                .cache_get_fresh(cache_key, &manifest.checksum_sha256, &context)
                .await
        {
            return Ok((cached_record_set(cached), fence_warning));
        }

        let table = resolve_table_for_message(manifest, &request.message_type)
            .map_err(|_| message_type_lookup_status(manifest, &request.message_type))?;
        let routed_pool = self
            .pg_select_pool_for_table_routed(table, &context)
            .await?;
        let routed_warning = routed_pool.warning().cloned();
        let pool = routed_pool.pool();
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
        // X-3: bind from the NORMALIZED filter (physical column keys), matching
        // what the planner compiled into `plan.sql`. The runtime filter carries
        // proto FIELD names, and BTreeMap iteration is lexical — so an alias whose
        // `field_name` and `column_name` sort differently made `plan.parameter_columns`
        // (normalized order) disagree with the raw-filter value order, binding the
        // wrong value to the wrong column. The bridged path binds its own params
        // and is unaffected.
        let normalized_filter = crate::planning::broker::normalize_filter_keys(
            &crate::planning::broker::column_resolver(table),
            &filter,
        );
        let values = filter_bind_values(&normalized_filter);
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
        let mut record_set = rows_to_record_set(
            rows,
            Some(table),
            &plan.masked_columns,
            &context,
            self.encryption.as_ref(),
            &self.encryption_metrics,
        )?;
        // P-1: mint next_page_token when this is a FULL page (more rows may exist)
        // and every cursor key is present in the last row. An empty token signals
        // the last page (AIP-158). Paginated reads never touch the read cache
        // (below), so a cache hit can't drop the cursor.
        if let Some(keys) = &cursor_keys
            && (record_set.records_json.len() as i32) >= request.limit
            && let Some(last) = record_set.records_json.last()
            && let Ok(JsonValue::Object(row)) = serde_json::from_slice::<JsonValue>(last)
            && let Some(cursor) = pagination::cursor_values_from_row(keys, &row)
        {
            record_set.next_page_token = pagination::encode_page_token(
                &context.tenant_id,
                &request.message_type,
                page_query_digest.as_deref().unwrap_or_default(),
                &cursor,
            );
        }
        // #5: surface each returned row's opaque revision when the caller asked for
        // it. One batched lookup keyed on the salted revision keys of the returned
        // primary keys; a row with no revision entry (never mutated since tracking
        // was enabled) gets an empty slot. The output is index-aligned with
        // `records_json`, so a client can pair a row with its CAS token.
        if request.include_revision {
            let config = crate::runtime::system::SystemCatalogConfig::current();
            let mut revision_keys: Vec<String> = Vec::with_capacity(record_set.records_json.len());
            let mut per_row_key: Vec<Option<String>> =
                Vec::with_capacity(record_set.records_json.len());
            for bytes in &record_set.records_json {
                let row: JsonValue = serde_json::from_slice(bytes).unwrap_or(JsonValue::Null);
                let pk_values: Vec<JsonValue> = revision_pk_columns
                    .iter()
                    .map(|col| row.get(col).cloned().unwrap_or(JsonValue::Null))
                    .collect();
                if revision_pk_columns.is_empty() || pk_values.iter().any(JsonValue::is_null) {
                    per_row_key.push(None);
                } else {
                    let key = row_revision_key(
                        &context.tenant_id,
                        &context.project_id,
                        &request.message_type,
                        &pk_tuple_canonical(&pk_values),
                    );
                    per_row_key.push(Some(key.clone()));
                    revision_keys.push(key);
                }
            }
            let revisions = self.load_row_revisions(&config, &revision_keys).await?;
            record_set.record_revisions = per_row_key
                .into_iter()
                .map(|key| {
                    key.and_then(|key| revisions.get(&key).map(i64::to_string))
                        .unwrap_or_default()
                })
                .collect();
        }
        if !bypass_write
            && !paginate
            // #5: never populate the record cache from an include_revision read —
            // the entry would carry revisions a later plain read never asked for.
            && !request.include_revision
            && let Some(cache_key) = cache_key.as_deref()
        {
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
        // gate 25 (lock-fencing-at-commit): when the caller supplied a lock_name +
        // fencing_token, validate the token against the LockService's durable row
        // in THIS tx BEFORE any dedup/write/CDC work — a stale token (a writer that
        // outlived its lease) is fenced off fail-closed with zero side effect. No-op
        // when lock_name is empty (the hot-path majority).
        self.enforce_fencing_token_in_tx(
            &mut tx,
            &context.tenant_id,
            &request.lock_name,
            request.fencing_token,
        )
        .await?;
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
                // #6: bind the claim to the authoritative inputs (record + conflict
                // target + expected precondition) so a key reused with different
                // inputs is a conflict, not a bogus replay.
                let request_hash = idempotency_request_hash_upsert(&request, &record);
                let claim = claim_idempotency_key_in_tx(
                    &mut tx,
                    &config,
                    &dedup_key,
                    &context.tenant_id,
                    &context.project_id,
                    &request.message_type,
                    "upsert",
                    &request_hash,
                )
                .await?;
                if !claim.fresh {
                    // Replay OR conflict: do NOT run the write. Drop the tx (rolls
                    // back the dedup re-read). If the first writer's stored hash
                    // differs from THIS request's hash, the key was reused with
                    // different inputs — refuse (non-disclosing). Otherwise return
                    // the stored original response. A legacy row with no stored hash
                    // (pre-upgrade) is replayed best-effort.
                    drop(tx);
                    if let Some(prior_hash) = claim.prior_request_hash.as_deref()
                        && prior_hash != request_hash
                    {
                        return Err(idempotency_request_mismatch_status());
                    }
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
        // #5: opaque revision of the upserted row after the write (empty when the
        // upsert was a no-op — 0 affected). Bumped in THIS tx so it commits atomically.
        let mut row_revision = String::new();
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
            // #5: bump the row's opaque revision in the SAME tx and surface it on
            // the response. Keyed by the (normalized, column-keyed) primary key of
            // the record we just wrote — the exact tuple a later conditional
            // Update/Delete or an include_revision Select will look up.
            let pk_values = record_values(&record, &table.primary_key)?;
            let revision = bump_row_revision_in_tx(
                &mut tx,
                &crate::runtime::system::SystemCatalogConfig::current(),
                &context.tenant_id,
                &context.project_id,
                &request.message_type,
                &pk_values,
            )
            .await?;
            row_revision = revision.to_string();
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
            // #5: opaque revision of the upserted row (empty on a 0-affected no-op).
            revision: row_revision,
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
        // F-1: audit the committed mutation to the configured sink (was a total
        // no-op — build_audit_event had no emitter).
        crate::runtime::core::audit::emit_audit(
            &self.config.audit_sink,
            &crate::planning::broker::build_audit_event(
                &context,
                "upsert",
                &response.resource_uri,
                &manifest.checksum_sha256,
            ),
            self.pg_pool.as_ref(),
        );
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
        self.enforce_cas_precondition(tx, table, expected, &key_columns, &key_values, context)
            .await
    }

    /// G-2: evaluate an optional compare-and-swap precondition for a DELETE.
    /// Unset/empty `expected` returns immediately. Otherwise the delete filter
    /// must pin EVERY primary-key column by equality (so the precondition targets
    /// exactly one row); that row is locked `FOR UPDATE` and each `expected` field
    /// asserted, identically to the upsert CAS. On mismatch or a missing row the
    /// caller returns before the delete, so nothing is removed.
    pub(crate) async fn enforce_delete_precondition(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table: &ManifestTable,
        expected: &prost_types::Struct,
        normalized_filter: &JsonValue,
        context: &RequestContext,
    ) -> Result<(), tonic::Status> {
        let key_columns = table.primary_key.clone();
        if key_columns.is_empty() {
            return Err(crate::runtime::executor_utils::failed_precondition_fields(
                "conditional delete requires a manifest primary key to locate the row",
                [(
                    "expected".to_string(),
                    "table has no primary key".to_string(),
                )],
            ));
        }
        let key_values = pk_equality_values_from_filter(normalized_filter, &key_columns)?;
        self.enforce_cas_precondition(tx, table, expected, &key_columns, &key_values, context)
            .await
    }

    /// Shared compare-and-swap core (upsert GO-005 + delete G-2): locate the row
    /// by `key_columns = key_values` with `FOR UPDATE` inside the caller's tx,
    /// decrypt it, and assert each `expected` field equals the current value.
    /// `FAILED_PRECONDITION` on a missing row or any mismatch.
    pub(crate) async fn enforce_cas_precondition(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table: &ManifestTable,
        expected: &prost_types::Struct,
        key_columns: &[String],
        key_values: &[JsonValue],
        context: &RequestContext,
    ) -> Result<(), tonic::Status> {
        let resolver = crate::planning::broker::column_resolver(table);
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
        let query = bind_values(sqlx::query(&sql), table, key_columns, key_values)?;
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
                    "no row matches the compare-and-swap key".to_string(),
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

    /// gate 25 (lock-fencing-at-commit): validate a caller's `fencing_token` for
    /// `lock_name` against the LockService's durable lock row, IN THE CALLER'S
    /// write transaction. No-op when `lock_name` is empty (fencing not requested).
    ///
    /// The decision reuses the LockService's own `ensure_fencing_token_fresh`
    /// (re-exported, so no lock logic is duplicated here); this method only does a
    /// tenant-scoped read of the durable row the LockService owns. The row is read
    /// `FOR UPDATE`, so a concurrent re-grant that bumps the token cannot commit
    /// between our read and our commit — closing the token TOCTOU that fencing
    /// exists to prevent (a stale writer that outlived its lease). A stale token
    /// OR a lapsed/released lease is rejected fail-closed; because this runs BEFORE
    /// the write/CDC/audit/idempotency work, a rejection rolls the tx back with no
    /// side effect. The lock table is resolved from the native-service manifest, so
    /// the schema/table/column names stay single-sourced (no hardcoded lock DDL).
    async fn enforce_fencing_token_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        tenant_id: &str,
        lock_name: &str,
        fencing_token: i64,
    ) -> Result<(), tonic::Status> {
        let lock_name = lock_name.trim();
        if lock_name.is_empty() {
            return Ok(());
        }
        let native = crate::runtime::native_catalog::native_service_manifest().map_err(|err| {
            setup_data_internal_status(
                "fencing_lock_manifest",
                format!("native-service manifest unavailable for lock fencing: {err}"),
            )
        })?;
        let lock_table =
            resolve_table_for_message(native, crate::runtime::service::lock_service::LOCK_MSG)
                .map_err(|_| {
                    setup_data_internal_status(
                        "fencing_lock_table",
                        "the LockService entity is not present in the native manifest",
                    )
                })?;
        // Newest acquisition wins (one live row per (tenant, lock_name)); FOR UPDATE
        // serializes against a concurrent re-grant that would bump the token.
        let sql = format!(
            "SELECT fencing_token, status, expires_at \
             FROM \"{schema}\".\"{table}\" \
             WHERE tenant_id = $1 AND lock_name = $2 \
             ORDER BY acquired_at DESC LIMIT 1 FOR UPDATE",
            schema = lock_table.schema,
            table = lock_table.table,
        );
        let row: Option<(i64, String, Option<chrono::DateTime<chrono::Utc>>)> =
            sqlx::query_as(&sql)
                .bind(tenant_id)
                .bind(lock_name)
                .fetch_optional(&mut **tx)
                .await
                .map_err(|err| {
                    crate::runtime::executor_utils::sqlx_error_to_status(
                        "lock fencing read failed",
                        &err,
                    )
                })?;
        let Some((stored_token, status, expires_at)) = row else {
            return Err(fencing_lock_absent_status(lock_name));
        };
        // Lease liveness: a released/expired lease means the writer outlived its
        // lease and must be fenced even if no newer holder has bumped the token yet.
        let lapsed = expires_at
            .map(|expires| expires <= chrono::Utc::now())
            .unwrap_or(true);
        if status != "HELD" || lapsed {
            return Err(fencing_lease_lost_status(lock_name));
        }
        crate::runtime::service::lock_service::ensure_fencing_token_fresh(
            fencing_token,
            stored_token,
        )
    }

    /// #5: batch-load opaque revisions for a set of revision keys, keyed by
    /// `revision_key`. Runs on the pool directly — no RLS/tenant GUCs are needed
    /// because each key is a salted hash of (tenant, project, message_type, PK), so
    /// a caller can only ever match keys it is already scoped to compute.
    async fn load_row_revisions(
        &self,
        config: &crate::runtime::system::SystemCatalogConfig,
        revision_keys: &[String],
    ) -> Result<std::collections::HashMap<String, i64>, tonic::Status> {
        if revision_keys.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let pool = self.pg_pool()?;
        let rel = config.row_revisions_relation();
        let sql = format!("SELECT revision_key, revision FROM {rel} WHERE revision_key = ANY($1)");
        let rows: Vec<(String, i64)> = sqlx::query_as(&sql)
            .bind(revision_keys.to_vec())
            .fetch_all(pool)
            .await
            .map_err(|err| row_revision_store_status("row_revision_lookup", &err))?;
        Ok(rows.into_iter().collect())
    }

    /// mutations→CDC (bug_report.md §R "kafka is not used"): emit a transactional
    /// outbox change event for a CDC-enabled entity, IN THE GIVEN TX so it is
    /// atomic with the data write. No-op when the entity has no `cdc_topic`, or
    /// when a tenant-scoped (`udb.*`) topic has no tenant to scope the event to
    /// (it could never reach a tenant-scoped subscriber). The envelope carries a
    /// top-level `tenant_id`/`project_id` so `stream_cdc`'s scope filter admits it,
    /// and the operation + record so subscribers see the change. A DB failure here
    /// rolls back the whole mutation (transactional-outbox atomicity).
    pub(crate) async fn emit_cdc_outbox_on_mutation(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        manifest: &CatalogManifest,
        message_type: &str,
        operation: &str,
        record: &JsonValue,
        context: &RequestContext,
    ) -> Result<(), tonic::Status> {
        // Best-effort delivery (the historical default): the CDC-disabled /
        // no-cdc_topic / no-tenant cases skip silently. `cdc_required = false`
        // preserves the exact behaviour every existing caller relied on.
        self.emit_cdc_outbox_on_mutation_checked(
            tx,
            manifest,
            message_type,
            operation,
            record,
            context,
            false,
        )
        .await
    }

    /// Required-delivery variant of [`Self::emit_cdc_outbox_on_mutation`] (bug
    /// #8.2). Identical, except that when `cdc_required` is true a change event
    /// that CANNOT be durably enqueued FAILS CLOSED with `FAILED_PRECONDITION`
    /// (aborting the caller's tx) instead of skipping: CDC delivery disabled, the
    /// entity declaring no `cdc_topic`, or a tenant-scoped topic with no tenant to
    /// route to. The outbox INSERT failure already errors in BOTH modes
    /// (transactional-outbox atomicity), so the enqueue itself is unconditionally
    /// fail-closed once we reach it.
    pub(crate) async fn emit_cdc_outbox_on_mutation_checked(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        manifest: &CatalogManifest,
        message_type: &str,
        operation: &str,
        record: &JsonValue,
        context: &RequestContext,
        cdc_required: bool,
    ) -> Result<(), tonic::Status> {
        // Resolve via the SAME index the mutation used (case-insensitive, full or
        // leaf message name) so the emit gate matches exactly what was written —
        // an exact `==` missed the entity and silently skipped the event.
        let table = resolve_table_for_message(manifest, message_type)
            .map_err(|_| message_type_lookup_status(manifest, message_type))?;
        let topic = table.cdc_topic.trim();
        if topic.is_empty() {
            if cdc_required {
                return Err(cdc_required_undeliverable_status(
                    message_type,
                    "the entity declares no cdc_topic, so no change event can be delivered",
                ));
            }
            return Ok(());
        }
        // When CDC delivery is disabled (UDB_CDC_ENABLED=false) nothing drains the
        // outbox — neither the Kafka tailer nor the in-process stream both live on
        // the tailer-fed broadcast — so writing the row would only accumulate
        // unbounded `outbox_events` with no consumer. Skip the write entirely; the
        // operator has opted out of change-event delivery. A `cdc_required`
        // mutation cannot tolerate that opt-out, so it fails closed instead.
        if !crate::runtime::cdc::cdc_delivery_enabled() {
            if cdc_required {
                return Err(cdc_required_undeliverable_status(
                    message_type,
                    "CDC delivery is disabled (UDB_CDC_ENABLED=false); a cdc_required mutation cannot be honoured",
                ));
            }
            return Ok(());
        }
        // Tenant-scoped topics can't reach a subscriber without a tenant; skip
        // (or fail closed when the caller requires delivery).
        if crate::runtime::cdc::tenant_scoped_topic(topic) && context.tenant_id.trim().is_empty() {
            if cdc_required {
                return Err(cdc_required_undeliverable_status(
                    message_type,
                    "tenant-scoped topic has no tenant_id to route the change event to",
                ));
            }
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
        // Bug #8.3/#8.4: the correlation_id MUST be the caller's verified
        // correlation_id, NOT the partition key (which is the record PK) — the old
        // code fabricated correlation from the PK, destroying request tracing. The
        // partition key is the `document_id`. The envelope also carries the verified
        // actor/trace attribution (user_id / service_identity / purpose /
        // decision_id) so downstream CDC consumers and the audit trail see WHO/WHY,
        // not just tenant/project. Empty fields are omitted by falling back to the
        // stamped context values (already verified, never body-supplied).
        let correlation_id = if context.correlation_id.trim().is_empty() {
            partition_key.clone()
        } else {
            context.correlation_id.clone()
        };
        let envelope = serde_json::json!({
            "event_id": event_id.to_string(),
            "event_type": topic,
            "topic": topic,
            "tenant_id": context.tenant_id,
            "project_id": context.project_id,
            "operation": operation,
            "message_type": message_type,
            "document_id": partition_key,
            "correlation_id": correlation_id,
            "user_id": context.user_id,
            "service_identity": context.service_identity,
            "purpose": context.purpose,
            "decision_id": context.decision_id,
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
        // G-2: optional compare-and-swap precondition — delete only if the
        // primary-key-identified row still equals these fields.
        expected: Option<prost_types::Struct>,
        // #5 + gate 25: optional opaque-revision precondition and lock-fencing.
        guards: MutationGuards,
    ) -> Result<MutationResponse, tonic::Status> {
        let filter = match resolve_table_for_message(manifest, message_type) {
            Ok(table_for_encryption) => self.rewrite_encrypted_equality_filters(
                table_for_encryption,
                &filter,
                &context.tenant_id,
            ),
            Err(_) => filter,
        };
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
        // X-3: bind from the NORMALIZED filter (physical column keys), matching
        // what the planner compiled into `plan.sql`. The runtime filter carries
        // proto FIELD names, and BTreeMap iteration is lexical — so an alias whose
        // `field_name` and `column_name` sort differently made `plan.parameter_columns`
        // (normalized order) disagree with the raw-filter value order, binding the
        // wrong value to the wrong column. The bridged path binds its own params
        // and is unaffected.
        let normalized_filter = crate::planning::broker::normalize_filter_keys(
            &crate::planning::broker::column_resolver(table),
            &filter,
        );
        let values = filter_bind_values(&normalized_filter);
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
        // gate 25 (lock-fencing-at-commit): validate the fencing token (if any)
        // BEFORE the dedup claim/CAS/delete, so a fenced writer rolls the tx back
        // with no dedup/delete/CDC/audit side effect. No-op when lock_name is empty.
        self.enforce_fencing_token_in_tx(
            &mut tx,
            &context.tenant_id,
            &guards.lock_name,
            guards.fencing_token,
        )
        .await?;
        // #1: the idempotency claim runs FIRST — BEFORE the CAS precondition and
        // the delete — matching the Upsert ordering. A non-fresh claim takes the
        // replay/conflict path, so a response-loss retry AFTER the row was already
        // deleted returns the stored response instead of a spurious
        // FAILED_PRECONDITION. Only a FRESH claim proceeds to the precondition; a
        // precondition failure below drops the tx, which rolls back this fresh claim
        // (same-tx atomicity), so a failed attempt does not burn the key.
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
                // #6: bind the claim to the authoritative inputs (normalized filter
                // + expected precondition).
                let request_hash =
                    idempotency_request_hash_delete(&normalized_filter, expected.as_ref());
                let claim = claim_idempotency_key_in_tx(
                    &mut tx,
                    &config,
                    &dedup_key,
                    &context.tenant_id,
                    &context.project_id,
                    message_type,
                    "delete",
                    &request_hash,
                )
                .await?;
                if !claim.fresh {
                    drop(tx);
                    if let Some(prior_hash) = claim.prior_request_hash.as_deref()
                        && prior_hash != request_hash
                    {
                        return Err(idempotency_request_mismatch_status());
                    }
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
        // G-2: enforce the compare-and-swap precondition (if any) AFTER a fresh
        // claim but BEFORE the delete, so a stale precondition fails fast and the
        // tx rolls back with nothing removed — and the fresh claim rolls back with
        // it. Uses the normalized filter so key columns resolve to physical names.
        if let Some(expected) = expected
            .as_ref()
            .filter(|expected| !expected.fields.is_empty())
        {
            self.enforce_delete_precondition(
                &mut tx,
                table,
                expected,
                &normalized_filter,
                &context,
            )
            .await?;
        }
        // #5: opaque-revision precondition (if any), AFTER a fresh claim but BEFORE
        // the delete. Requires the filter to pin every primary-key column by
        // equality (single row) — the same single-row boundary as the field-map
        // CAS — so the revision assertion targets exactly one row. Mismatch / an
        // untracked row rolls the tx back with nothing removed.
        if !guards.expected_revision.trim().is_empty() {
            let pk_values = pk_equality_values_from_filter(&normalized_filter, &table.primary_key)?;
            enforce_expected_revision_in_tx(
                &mut tx,
                &crate::runtime::system::SystemCatalogConfig::current(),
                &context.tenant_id,
                &context.project_id,
                message_type,
                &pk_values,
                &guards.expected_revision,
            )
            .await?;
        }
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
        // F-1: audit the committed delete to the configured sink.
        crate::runtime::core::audit::emit_audit(
            &self.config.audit_sink,
            &crate::planning::broker::build_audit_event(
                &context,
                "delete",
                &response.resource_uri,
                &manifest.checksum_sha256,
            ),
            self.pg_pool.as_ref(),
        );
        Ok(response)
    }

    /// Partial update (W7): SET the named columns and/or apply atomic
    /// increments on the rows matched by `filter` — one UPDATE statement, no
    /// full-record resend, no read-modify-write counter window. Shares the
    /// delete/upsert machinery end to end: planner isolation checks, typed
    /// binds, CAS precondition, keyed idempotent replay, projection enqueue,
    /// CDC outbox, consistency receipt, cache invalidation, audit emission.
    #[allow(clippy::too_many_arguments)]
    /// Shared core of a partial UPDATE inside an ALREADY-OPEN PG transaction:
    /// plan → bind → execute (RETURNING when projections exist or a record is
    /// wanted) → enqueue projection tasks (each post-update row as an `upsert`)
    /// → emit the CDC change event. Both the unary [`update`](Self::update) and
    /// the `BeginTx` `update` operation call this, so the two write paths cannot
    /// diverge on the post-write side-effects (side-effect parity).
    ///
    /// `filter` MUST already be encryption-rewritten by the caller (via
    /// `rewrite_encrypted_equality_filters`); the helper does not re-apply it,
    /// as that rewrite is not idempotent. Returns
    /// `(affected_rows, record_json, projection_task_ids)`; `record_json` is
    /// empty unless `return_record` is set. Runs no CAS / idempotency / commit —
    /// those stay with the caller that owns the transaction lifecycle.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn execute_update_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        manifest: &CatalogManifest,
        message_type: &str,
        filter: &JsonValue,
        changes: &JsonValue,
        increments: &[(String, f64)],
        context: &RequestContext,
        return_record: bool,
    ) -> Result<(i64, Vec<u8>, Vec<String>), tonic::Status> {
        // Best-effort CDC (the historical default): `cdc_required = false`.
        self.execute_update_in_tx_checked(
            tx,
            manifest,
            message_type,
            filter,
            changes,
            increments,
            context,
            return_record,
            false,
        )
        .await
    }

    /// Required-CDC-delivery variant of [`Self::execute_update_in_tx`] (bug
    /// #8.2). When `cdc_required` is true, an update whose change event cannot be
    /// durably enqueued fails closed via
    /// [`Self::emit_cdc_outbox_on_mutation_checked`]; otherwise identical.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn execute_update_in_tx_checked(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        manifest: &CatalogManifest,
        message_type: &str,
        filter: &JsonValue,
        changes: &JsonValue,
        increments: &[(String, f64)],
        context: &RequestContext,
        return_record: bool,
        cdc_required: bool,
    ) -> Result<(i64, Vec<u8>, Vec<String>), tonic::Status> {
        // Projection targets re-materialize from the task's source_payload, so an
        // update that feeds projections must return the post-update rows: plan
        // RETURNING whenever this message type has projection targets, not only
        // when the caller asked for the record back.
        let projection_plans = crate::runtime::projection::ProjectionPlan::from_manifest(manifest);
        let has_projections = projection_plans.iter().any(|plan| {
            crate::runtime::projection::message_type_matches(&plan.message_type, message_type)
        });
        let need_rows = return_record || has_projections;
        let plan_request = crate::planning::broker::UpdatePlanRequest {
            context: context.clone(),
            message_type: message_type.to_string(),
            filter: filter.clone(),
            changes: changes.clone(),
            increments: increments.to_vec(),
            return_record: need_rows,
        };
        let plan = crate::planning::broker::build_update_plan(manifest, &plan_request);
        reject_plan(&plan.errors)?;
        let table = resolve_table_for_message(manifest, message_type)
            .map_err(|_| message_type_lookup_status(manifest, message_type))?;
        let resolver = crate::planning::broker::column_resolver(table);
        let normalized_filter = crate::planning::broker::normalize_filter_keys(&resolver, filter);
        // Bind order is the plan's contract: changes (sorted by physical
        // column — the same normalized_update_changes the planner used), then
        // increments in request order, then the filter values.
        let mut bind_errors = Vec::new();
        let ordered_changes = crate::planning::broker::normalized_update_changes(
            changes,
            &resolver,
            &mut bind_errors,
        );
        if !bind_errors.is_empty() {
            // Unreachable after reject_plan, but never bind on a divergent view.
            return Err(setup_data_invalid_field(
                "changes",
                bind_errors.join("; "),
                "update changes rejected",
            ));
        }
        let empty = serde_json::Map::new();
        let changes_object = changes.as_object().unwrap_or(&empty);
        let mut values: Vec<JsonValue> = ordered_changes
            .iter()
            .map(|(_, raw_key)| {
                changes_object
                    .get(raw_key)
                    .cloned()
                    .unwrap_or(JsonValue::Null)
            })
            .collect();
        values.extend(increments.iter().map(|(_, delta)| serde_json::json!(delta)));
        values.extend(filter_bind_values(&normalized_filter));
        let query = bind_values(
            sqlx::query(&plan.sql),
            table,
            &plan.parameter_columns,
            &values,
        )?;

        let (affected_rows, record_json, updated_rows_json) = if need_rows {
            let rows = query.fetch_all(&mut **tx).await.map_err(|err| {
                crate::runtime::executor_utils::sqlx_error_to_status(
                    "PostgreSQL update failed",
                    &err,
                )
            })?;
            if rows.is_empty() {
                (0, Vec::new(), Vec::new())
            } else {
                let record_set = rows_to_record_set(
                    rows,
                    Some(table),
                    &[],
                    context,
                    self.encryption.as_ref(),
                    &self.encryption_metrics,
                )?;
                let record_json = if return_record {
                    returned_record_json_or_status(&record_set.records_json)?
                } else {
                    Vec::new()
                };
                let updated_rows_json = record_set
                    .records_json
                    .iter()
                    .map(|bytes| serde_json::from_slice::<JsonValue>(bytes))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|err| {
                        setup_data_internal_status(
                            "update_projection_row_decode",
                            format!("updated row decode failed: {err}"),
                        )
                    })?;
                (
                    record_set.records_json.len() as i64,
                    record_json,
                    updated_rows_json,
                )
            }
        } else {
            let result = query.execute(&mut **tx).await.map_err(|err| {
                crate::runtime::executor_utils::sqlx_error_to_status(
                    "PostgreSQL update failed",
                    &err,
                )
            })?;
            (result.rows_affected() as i64, Vec::new(), Vec::new())
        };

        let mut projection_task_ids = Vec::new();
        if affected_rows > 0 {
            // Projection tasks know two operations, 'upsert' and 'delete'; an
            // update enqueues each POST-UPDATE row as an 'upsert' task (the table
            // CHECK enforces that pair and the worker projects source_payload
            // verbatim).
            for row_json in &updated_rows_json {
                projection_task_ids.extend(
                    crate::runtime::projection::ProjectionEngine::enqueue_write_tasks_tx(
                        &mut *tx,
                        &crate::runtime::system::SystemCatalogConfig::current(),
                        &context.tenant_id,
                        message_type,
                        "upsert",
                        row_json,
                        &projection_plans,
                    )
                    .await
                    .map_err(|err| {
                        setup_data_internal_status(
                            "update_projection_task_enqueue",
                            format!("projection task enqueue failed: {err}"),
                        )
                    })?,
                );
            }
            projection_task_ids.sort();
            projection_task_ids.dedup();
            // CDC change event carries the row identity AND the delta so
            // consumers see what changed without a read-back.
            let cdc_payload = serde_json::json!({
                "filter": filter,
                "changes": changes,
                "increments": increments
                    .iter()
                    .map(|(column, delta)| serde_json::json!({"column": column, "delta": delta}))
                    .collect::<Vec<_>>(),
            });
            self.emit_cdc_outbox_on_mutation_checked(
                &mut *tx,
                manifest,
                message_type,
                "update",
                &cdc_payload,
                context,
                cdc_required,
            )
            .await?;
        }
        Ok((affected_rows, record_json, projection_task_ids))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update(
        &self,
        manifest: &CatalogManifest,
        message_type: &str,
        filter: JsonValue,
        changes: JsonValue,
        increments: Vec<(String, f64)>,
        context: RequestContext,
        idempotency_key: String,
        expected: Option<prost_types::Struct>,
        return_record: bool,
        // #5 + gate 25: optional opaque-revision precondition and lock-fencing.
        guards: MutationGuards,
    ) -> Result<MutationResponse, tonic::Status> {
        let filter = match resolve_table_for_message(manifest, message_type) {
            Ok(table_for_encryption) => self.rewrite_encrypted_equality_filters(
                table_for_encryption,
                &filter,
                &context.tenant_id,
            ),
            Err(_) => filter,
        };
        let pool = self.pg_pool()?;
        let table = resolve_table_for_message(manifest, message_type)
            .map_err(|_| message_type_lookup_status(manifest, message_type))?;
        let resolver = crate::planning::broker::column_resolver(table);
        let normalized_filter = crate::planning::broker::normalize_filter_keys(&resolver, &filter);

        let mut tx = pool.begin().await.map_err(|e| {
            setup_data_internal_status(
                "update_transaction_begin",
                format!("PG transaction begin failed: {e}"),
            )
        })?;
        set_request_local_settings(&mut tx, &context).await?;
        // gate 25 (lock-fencing-at-commit): validate the fencing token (if any)
        // BEFORE the dedup claim/CAS/write. No-op when lock_name is empty.
        self.enforce_fencing_token_in_tx(
            &mut tx,
            &context.tenant_id,
            &guards.lock_name,
            guards.fencing_token,
        )
        .await?;
        // #1: the idempotency claim runs FIRST — BEFORE the CAS precondition and
        // the update — matching Upsert. A non-fresh claim takes the replay/conflict
        // path (a response-loss retry AFTER the row already changed returns the
        // stored response, not a spurious FAILED_PRECONDITION); only a FRESH claim
        // proceeds to the precondition, and a precondition failure drops the tx,
        // rolling back the fresh claim (same-tx atomicity).
        // Keyed-only durable dedup, same-tx, fail-closed (lane 05).
        let dedup_ctx = {
            let key = idempotency_key_for_dedup(&idempotency_key)?;
            if let Some(key) = key {
                let config = crate::runtime::system::SystemCatalogConfig::current();
                let dedup_key = idempotency_dedup_key(
                    &context.tenant_id,
                    &context.project_id,
                    message_type,
                    "update",
                    key,
                );
                // #6: bind the claim to the authoritative inputs (normalized filter
                // + changes/mask + increments + expected precondition).
                let request_hash = idempotency_request_hash_update(
                    &normalized_filter,
                    &changes,
                    &increments,
                    expected.as_ref(),
                );
                let claim = claim_idempotency_key_in_tx(
                    &mut tx,
                    &config,
                    &dedup_key,
                    &context.tenant_id,
                    &context.project_id,
                    message_type,
                    "update",
                    &request_hash,
                )
                .await?;
                if !claim.fresh {
                    drop(tx);
                    if let Some(prior_hash) = claim.prior_request_hash.as_deref()
                        && prior_hash != request_hash
                    {
                        return Err(idempotency_request_mismatch_status());
                    }
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
                    operation: "update",
                })
            } else {
                None
            }
        };
        // CAS precondition AFTER a fresh claim but BEFORE the write — a stale
        // precondition fails fast with nothing written, and the fresh claim rolls
        // back with the tx. Row located by primary key from the normalized filter
        // (same as delete).
        if let Some(expected) = expected
            .as_ref()
            .filter(|expected| !expected.fields.is_empty())
        {
            self.enforce_delete_precondition(
                &mut tx,
                table,
                expected,
                &normalized_filter,
                &context,
            )
            .await?;
        }
        // #5: opaque-revision precondition (if any) — same single-row (PK-pinned)
        // boundary as the field-map CAS. Located by primary key from the normalized
        // filter; a mismatch / untracked row rolls the tx back with nothing written.
        if !guards.expected_revision.trim().is_empty() {
            let pk_values = pk_equality_values_from_filter(&normalized_filter, &table.primary_key)?;
            enforce_expected_revision_in_tx(
                &mut tx,
                &crate::runtime::system::SystemCatalogConfig::current(),
                &context.tenant_id,
                &context.project_id,
                message_type,
                &pk_values,
                &guards.expected_revision,
            )
            .await?;
        }
        // Plan → bind → execute → projection enqueue → CDC emit, shared verbatim
        // with the BeginTx `update` operation so neither path can drift on the
        // post-write side-effects.
        let (affected_rows, record_json, projection_task_ids) = self
            .execute_update_in_tx(
                &mut tx,
                manifest,
                message_type,
                &filter,
                &changes,
                &increments,
                &context,
                return_record,
            )
            .await?;
        // #5: bump + surface the opaque revision for a SINGLE-ROW (primary-key
        // pinned) update. Revision is a single-row optimistic-concurrency primitive
        // (like the CAS above), so a multi-row range update leaves it empty and
        // untracked — `pk_equality_values_from_filter` returns Err there, which we
        // treat as "not single-row" and skip (never surface a partial/ambiguous
        // token). A revision-tracked update ALWAYS resolves to one row when
        // expected_revision was asserted (that path required the pinned filter).
        let mut row_revision = String::new();
        if affected_rows > 0
            && let Ok(pk_values) =
                pk_equality_values_from_filter(&normalized_filter, &table.primary_key)
        {
            let revision = bump_row_revision_in_tx(
                &mut tx,
                &crate::runtime::system::SystemCatalogConfig::current(),
                &context.tenant_id,
                &context.project_id,
                message_type,
                &pk_values,
            )
            .await?;
            row_revision = revision.to_string();
        }
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
        // Plan-level resource URI (sql://schema/table) — the plan is now built
        // inside `execute_update_in_tx`, so recompute the same value here.
        let plan_resource_uri = format!("sql://{}/{}", table.schema, table.table);
        let resource_uri = mutation_response_resource_uri_or_fallback(
            &context,
            message_type,
            table,
            &filter,
            &plan_resource_uri,
            dedup_ctx.is_some(),
        )?;
        let response = MutationResponse {
            mutation_id: Uuid::new_v4().to_string(),
            resource_uri,
            affected_rows,
            was_duplicate: false,
            record_json,
            write_receipt_json,
            write_receipt: Some(receipt.to_proto()),
            // #5: bumped revision for a single-row update; empty for multi-row.
            revision: row_revision,
            ..MutationResponse::default()
        };
        if let Some(dedup_ctx) = dedup_ctx.as_ref() {
            persist_idempotency_response_in_tx(&mut tx, dedup_ctx, &response).await?;
        }
        tx.commit().await.map_err(|err| {
            setup_data_internal_status(
                "update_commit",
                format!("PostgreSQL update commit failed: {err}"),
            )
        })?;
        let _ = self
            .cache_delete_pattern(&cache_invalidation_pattern("select", message_type))
            .await;
        crate::runtime::core::audit::emit_audit(
            &self.config.audit_sink,
            &crate::planning::broker::build_audit_event(
                &context,
                "update",
                &response.resource_uri,
                &manifest.checksum_sha256,
            ),
            self.pg_pool.as_ref(),
        );
        Ok(response)
    }

    /// gate 23 — bounded, tenant-scoped, request-hash-idempotent bulk
    /// compare-and-swap. One write transaction applies a bounded batch of
    /// single-row conditional updates: each item targets exactly one row (its
    /// `filter` must pin the full primary key), and is applied only if BOTH its
    /// optional opaque-revision precondition (#5) and its optional field-map CAS
    /// (`expected`) hold. A per-item precondition MISMATCH is COUNTED as a conflict
    /// (not a batch error), so the batch is safe to retry after a partial failure —
    /// reuse `idempotency_key` and the durable dedup machinery replays the original
    /// counts instead of re-applying. Every applied row keeps the unary Update
    /// path's projection + CDC-outbox side effects (via `execute_update_in_tx`) and
    /// bumps its opaque revision; the batch emits one audit event on commit.
    pub async fn bulk_cas(
        &self,
        manifest: &CatalogManifest,
        request: crate::proto::BulkCasRequest,
        metadata_context: RequestContext,
    ) -> Result<crate::proto::BulkCasResponse, tonic::Status> {
        let context = merge_context(request.context.as_ref(), metadata_context);
        // Bound the batch: clamp the caller's explicit ceiling to the server max,
        // then reject an over-ceiling or empty batch fail-closed BEFORE any work.
        let ceiling = bulk_cas_effective_ceiling(request.max_rows);
        if request.items.is_empty() {
            return Err(setup_data_invalid_field(
                "items",
                "must contain at least one item",
                "bulk CAS requires at least one item",
            ));
        }
        if request.items.len() > ceiling {
            return Err(setup_data_invalid_field(
                "items",
                format!("must not exceed the {ceiling}-row ceiling"),
                format!(
                    "bulk CAS batch of {} exceeds the {ceiling}-row ceiling",
                    request.items.len()
                ),
            ));
        }
        // Every item must actually write something (changes and/or increments) —
        // a pure assertion has no place in a MUTATION batch. Fail fast + bounded.
        for (index, item) in request.items.iter().enumerate() {
            let has_changes = item
                .changes
                .as_ref()
                .map(|changes| !changes.fields.is_empty())
                .unwrap_or(false);
            if !has_changes && item.increments.is_empty() {
                return Err(setup_data_invalid_field(
                    "items",
                    "each item must set at least one column or increment",
                    format!("bulk CAS item {index} has neither changes nor increments"),
                ));
            }
        }
        let table = resolve_table_for_message(manifest, &request.message_type)
            .map_err(|_| message_type_lookup_status(manifest, &request.message_type))?;
        let resolver = crate::planning::broker::column_resolver(table);
        let config = crate::runtime::system::SystemCatalogConfig::current();
        let pool = self.pg_pool()?;
        let mut tx = pool.begin().await.map_err(|e| {
            setup_data_internal_status(
                "bulk_cas_transaction_begin",
                format!("PG transaction begin failed: {e}"),
            )
        })?;
        set_request_local_settings(&mut tx, &context).await?;
        // Whole-batch durable idempotency (reuses the keyed-mutation dedup claim).
        let dedup_ctx = {
            let key = idempotency_key_for_dedup(&request.idempotency_key)?;
            if let Some(key) = key {
                let dedup_key = idempotency_dedup_key(
                    &context.tenant_id,
                    &context.project_id,
                    &request.message_type,
                    "bulk_cas",
                    key,
                );
                let request_hash =
                    idempotency_request_hash_bulk_cas(&request.message_type, &request.items);
                let claim = claim_idempotency_key_in_tx(
                    &mut tx,
                    &config,
                    &dedup_key,
                    &context.tenant_id,
                    &context.project_id,
                    &request.message_type,
                    "bulk_cas",
                    &request_hash,
                )
                .await?;
                if !claim.fresh {
                    drop(tx);
                    if let Some(prior_hash) = claim.prior_request_hash.as_deref()
                        && prior_hash != request_hash
                    {
                        return Err(idempotency_request_mismatch_status());
                    }
                    return bulk_cas_response_from_idempotency_json(&claim.prior_response_json);
                }
                Some(IdempotencyPersistContext {
                    config: config.clone(),
                    dedup_key,
                    tenant_id: context.tenant_id.clone(),
                    project_id: context.project_id.clone(),
                    message_type: request.message_type.clone(),
                    operation: "bulk_cas",
                })
            } else {
                None
            }
        };
        let mut results: Vec<crate::proto::BulkCasItemResult> =
            Vec::with_capacity(request.items.len());
        let mut matched = 0i32;
        let mut changed = 0i32;
        let mut conflicted = 0i32;
        let mut projection_task_ids: Vec<String> = Vec::new();
        for item in &request.items {
            let filter_json = item
                .filter
                .as_ref()
                .map(struct_to_json)
                .unwrap_or(JsonValue::Null);
            let filter =
                self.rewrite_encrypted_equality_filters(table, &filter_json, &context.tenant_id);
            let normalized_filter =
                crate::planning::broker::normalize_filter_keys(&resolver, &filter);
            // Single-row addressing: every PK column pinned by equality.
            let pk_values = pk_equality_values_from_filter(&normalized_filter, &table.primary_key)?;
            let mut item_result = crate::proto::BulkCasItemResult::default();
            // Lock + read the target row so the CAS eval and the write are atomic.
            let row = self
                .read_locked_row_json(&mut tx, table, &table.primary_key, &pk_values, &context)
                .await?;
            match row {
                None => {
                    conflicted += 1;
                    item_result.conflicted = true;
                }
                Some(row_json) => {
                    matched += 1;
                    item_result.matched = true;
                    let field_ok = bulk_cas_field_precondition_holds(
                        &row_json,
                        item.expected.as_ref(),
                        &resolver,
                    );
                    let revision_ok = if item.expected_revision.trim().is_empty() {
                        true
                    } else {
                        check_expected_revision_in_tx(
                            &mut tx,
                            &config,
                            &context.tenant_id,
                            &context.project_id,
                            &request.message_type,
                            &pk_values,
                            &item.expected_revision,
                        )
                        .await?
                    };
                    if field_ok && revision_ok {
                        let changes_json = item
                            .changes
                            .as_ref()
                            .map(struct_to_json)
                            .unwrap_or_else(|| serde_json::json!({}));
                        let increments: Vec<(String, f64)> = item
                            .increments
                            .iter()
                            .map(|inc| (inc.column.clone(), inc.delta))
                            .collect();
                        let (affected, _record_json, task_ids) = self
                            .execute_update_in_tx(
                                &mut tx,
                                manifest,
                                &request.message_type,
                                &filter,
                                &changes_json,
                                &increments,
                                &context,
                                false,
                            )
                            .await?;
                        projection_task_ids.extend(task_ids);
                        if affected > 0 {
                            let revision = bump_row_revision_in_tx(
                                &mut tx,
                                &config,
                                &context.tenant_id,
                                &context.project_id,
                                &request.message_type,
                                &pk_values,
                            )
                            .await?;
                            item_result.revision = revision.to_string();
                        }
                        changed += 1;
                        item_result.changed = true;
                    } else {
                        conflicted += 1;
                        item_result.conflicted = true;
                    }
                }
            }
            results.push(item_result);
        }
        projection_task_ids.sort();
        projection_task_ids.dedup();
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
        // Persist the batch's counts into the dedup row (in-tx) so a keyed retry
        // replays the original counts instead of re-applying (request-hash idempotent).
        if let Some(dedup_ctx) = dedup_ctx.as_ref() {
            persist_bulk_cas_idempotency_in_tx(
                &mut tx,
                dedup_ctx,
                matched,
                changed,
                conflicted,
                &write_receipt_json,
            )
            .await?;
        }
        tx.commit().await.map_err(|err| {
            setup_data_internal_status(
                "bulk_cas_commit",
                format!("PostgreSQL bulk CAS commit failed: {err}"),
            )
        })?;
        let _ = self
            .cache_delete_pattern(&cache_invalidation_pattern("select", &request.message_type))
            .await;
        // One audit event for the committed batch (the resource is the table).
        crate::runtime::core::audit::emit_audit(
            &self.config.audit_sink,
            &crate::planning::broker::build_audit_event(
                &context,
                "bulk_cas",
                &format!("sql://{}/{}", table.schema, table.table),
                &manifest.checksum_sha256,
            ),
            self.pg_pool.as_ref(),
        );
        Ok(crate::proto::BulkCasResponse {
            matched,
            changed,
            conflicted,
            write_receipt_json,
            results,
        })
    }

    /// gate 23 helper: lock (`FOR UPDATE`) and read a single row by its primary key
    /// inside the caller's tx, returning the DECRYPTED row JSON (or `None` when no
    /// row matches). Shares the row-read + decrypt shape of `enforce_cas_precondition`
    /// but never errors on a "missing row" — the bulk path COUNTS that as a conflict.
    async fn read_locked_row_json(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table: &ManifestTable,
        key_columns: &[String],
        key_values: &[JsonValue],
        context: &RequestContext,
    ) -> Result<Option<JsonValue>, tonic::Status> {
        let predicate = key_columns
            .iter()
            .enumerate()
            .map(|(idx, column)| format!("\"{column}\" = ${}", idx + 1))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!(
            "SELECT * FROM \"{schema}\".\"{table}\" WHERE {predicate} FOR UPDATE",
            schema = table.schema,
            table = table.table,
        );
        let query = bind_values(sqlx::query(&sql), table, key_columns, key_values)?;
        let row = query.fetch_optional(&mut **tx).await.map_err(|err| {
            crate::runtime::executor_utils::sqlx_error_to_status("bulk CAS row read failed", &err)
        })?;
        let Some(row) = row else {
            return Ok(None);
        };
        let record_set = rows_to_record_set(
            vec![row],
            Some(table),
            &[],
            context,
            self.encryption.as_ref(),
            &self.encryption_metrics,
        )?;
        let json = record_set
            .records_json
            .first()
            .map(|bytes| serde_json::from_slice(bytes).unwrap_or(JsonValue::Null))
            .unwrap_or(JsonValue::Null);
        Ok(Some(json))
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
                // C7: AND the tenant/project scope into the Qdrant filter so a
                // direct vector search cannot read another tenant's points in a
                // shared collection. Mirrors the write-side payload stamp.
                let scoped_filter =
                    qdrant_and_tenant_filter(filter, &context.tenant_id, &context.project_id);
                qdrant.search(&request, scoped_filter).await
            } else {
                // F2: the ES/Weaviate/Pinecone dispatch translates request.filter
                // per-backend but never sees the CONTEXT tenant, so forwarding the
                // raw request read every tenant's points in a shared collection.
                // Merge the tenant/project scope into the neutral filter Struct
                // first — the same scope the qdrant branch ANDs in.
                let mut scoped_request = request.clone();
                scoped_request.filter =
                    scoped_generic_vector_filter(filter, &context.tenant_id, &context.project_id);
                self.vector_search_dispatch_target(
                    &target.backend,
                    target_instance,
                    &scoped_request,
                )
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

            // F1: AND the tenant/project scope into the hybrid filter. The dense
            // `vector_search` path already scopes (C7), but the hybrid and
            // empty-text legs previously forwarded the RAW caller filter, reading
            // across tenants in a shared collection. Scope ONCE, use on both legs.
            let scoped_filter =
                qdrant_and_tenant_filter(filter, &context.tenant_id, &context.project_id);

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
                    .search(&dense, scoped_filter)
                    .await;
            }

            // Full hybrid: Qdrant native RRF with local lexical re-ranking fallback.
            self.qdrant_for_instance_for_project(target_instance, &context.project_id)?
                .hybrid_search(&request, scoped_filter)
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
            // C7: stamp `_tenant_id`/`_project_id` into every point payload before
            // dispatch — for ALL vector backends, Qdrant included. The direct
            // Qdrant RPC does NOT stamp on its own (the executor writes the payload
            // verbatim), so hoisting the stamp ABOVE the backend split is what
            // actually enforces tenant isolation on a shared collection; it pairs
            // with the search-side tenant filter so writes and reads agree.
            let stamped = stamp_generic_vector_point_payloads(
                &request,
                &context.tenant_id,
                &context.project_id,
            );
            if target.backend == "qdrant" {
                let qdrant =
                    self.qdrant_for_instance_for_project(target_instance, &context.project_id)?;
                qdrant.upsert(&stamped).await?;
            } else {
                self.vector_upsert_dispatch_target(&target.backend, target_instance, &stamped)
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
        // Vector arm: ES scores carry the `+ 1.0` cosine offset, so normalize back.
        parse_vector_search_response(backend, &raw, true)
    }

    /// Execute a mediated FULL-TEXT-ONLY (lexical, no query vector) search
    /// (`SEARCH_MODE_TEXT`). The query text is threaded as a plain argument
    /// because `VectorSearchRequest` carries no text field and this path must not
    /// require a proto change — `request.vector` is empty. The tenant scope in
    /// `request.filter` is the SECURITY boundary and is AND'd into the generated
    /// backend query (see [`text_search_dispatch_spec`]). Only Elasticsearch is
    /// wired (BM25 `multi_match` over the stamped `payload.*`); every other
    /// backend — including Qdrant text-only, whose lexical match needs a payload
    /// full-text field index that may be absent — fails closed with a typed
    /// capability error. `backend` is the index's authoritative backend (validated
    /// at `CreateIndex`); a registered route only overrides the target instance.
    pub async fn vector_text_search(
        &self,
        manifest: &CatalogManifest,
        backend: &str,
        request: VectorSearchRequest,
        query_text: String,
        metadata_context: RequestContext,
    ) -> Result<VectorSet, tonic::Status> {
        #[cfg(not(feature = "qdrant"))]
        {
            let _ = (manifest, backend, request, query_text, metadata_context);
            return Err(qdrant_vector_feature_status("vector_text_search"));
        }
        #[cfg(feature = "qdrant")]
        {
            let backend = backend.to_ascii_lowercase();
            let context = merge_context(request.context.as_ref(), metadata_context);
            // Full-text-only carries NO query vector, so the vector-dimension plan
            // check is intentionally bypassed (a 0-length vector would otherwise
            // trip "dimension mismatch"); the tenant filter still governs. Resolve
            // only the target instance (route override → project default) — the
            // index backend is authoritative.
            let route =
                self.vector_resource_backend(manifest, &context.project_id, &request.collection);
            let route_instance = route.as_ref().and_then(|route| route.instance.as_deref());
            let target_instance = if context.target_instance.trim().is_empty() {
                route_instance.or_else(|| {
                    self.choose_instance_name_for_project(&backend, false, &context.project_id)
                })
            } else {
                Some(context.target_instance.as_str())
            };
            // Honour the read fence before dispatch (same projection-task fence
            // semantics as `vector_search`).
            let _ = self
                .enforce_read_fence(&context, &backend, target_instance.unwrap_or("selected"))
                .await?;
            self.text_search_dispatch_target(&backend, target_instance, &request, &query_text)
                .await
        }
    }

    async fn text_search_dispatch_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        request: &VectorSearchRequest,
        query_text: &str,
    ) -> Result<VectorSet, tonic::Status> {
        use crate::runtime::executors::SearchExecutor;
        let spec = text_search_dispatch_spec(backend, request, query_text)?;
        let executor = self.resolve_dispatch_executor(
            backend,
            instance,
            false,
            tonic::Code::FailedPrecondition,
            None,
        )?;
        let raw = SearchExecutor::search(&executor, &spec).await?;
        // Text arm: ES `_score` is BM25 relevance (no cosine offset), so it is
        // passed through unchanged.
        parse_vector_search_response(backend, &raw, false)
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
            // OBJ1/2/3: physically namespace the key by the verified tenant before
            // it reaches the backing store. Every downstream use here is
            // executor-facing; the client-facing `resource_uri` comes from `plan`
            // (unscoped), so the prefix stays an internal storage detail.
            let object_key = tenant_scoped_object_key(&context, &first.object_key);
            let mut request_json =
                object_request_json("put", &bucket, &object_key, &first.content_type);
            // Enforce the object store's `server_side_encryption` annotation on the
            // wire: the S3/MinIO executor honors this flag (SSE-S3/AES-256); GCS
            // and Azure Blob encrypt at rest unconditionally by platform default,
            // so the requirement is satisfied there without an explicit header.
            // Previously the planner computed `requires_server_side_encryption`
            // but nothing consumed it, so the annotation was a silent no-op.
            if plan.requires_server_side_encryption {
                request_json = object_request_json_require_sse(&request_json);
            }
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
            // OBJ1/2/3: read from the tenant-namespaced physical key; the `Chunk`
            // echo keeps the caller's original `object_key` (the prefix is an
            // internal storage detail, not something the client asked for).
            let physical_key = tenant_scoped_object_key(&context, &object_key);
            let request_json = object_request_json("get", &bucket, &physical_key, "");
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
            // OBJ1/2/3: the presigned URL must target the tenant-namespaced
            // physical key so a caller cannot mint a URL for another tenant's
            // object by presenting its bucket+key.
            let physical_key = tenant_scoped_object_key(&context, &request.object_key);
            let url = presign_s3_url(
                &s3,
                &request.bucket,
                &physical_key,
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
            // OBJ1/2/3: every part URL and the upload itself must target the
            // tenant-namespaced physical key so a multipart upload cannot land on
            // (or presign into) another tenant's object.
            let physical_key = tenant_scoped_object_key(&context, &request.object_key);
            let upload = s3
                .create_multipart_upload()
                .bucket(&request.bucket)
                .key(&physical_key)
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
                    .key(&physical_key)
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

// ── #5 (opaque row revision / ETag) + gate 25 (lock fencing) shared helpers ────

/// Optional per-mutation guards threaded from the request into the unary
/// Update/Delete paths (Upsert reads them straight off its request). All-default
/// (every field empty/zero) preserves the prior behaviour exactly.
#[derive(Debug, Default, Clone)]
pub(crate) struct MutationGuards {
    /// #5: opaque revision the addressed row must currently hold. Empty = not
    /// asserted (no revision precondition).
    pub(crate) expected_revision: String,
    /// gate 25: advisory-lock name to fence against. Empty = no fencing.
    pub(crate) lock_name: String,
    /// gate 25: the caller's monotonic fencing token for `lock_name`.
    pub(crate) fencing_token: i64,
}

/// Canonical, type-stable token for a single primary-key value used to build a
/// revision-map key. Integer-valued numbers collapse to `i:<n>` regardless of
/// whether the value arrived as an INTEGER column (`8`) or as a
/// `google.protobuf.Struct` f64 (`8.0`), so the bump-side key (built from the
/// written record) and the read-side key (built from the returned row) never
/// diverge on the int-vs-float trap — the same trap `json_values_match` guards.
fn pk_value_token(value: &JsonValue) -> String {
    match value {
        JsonValue::Number(n) => {
            let f = n.as_f64().unwrap_or(f64::NAN);
            if f.is_finite() && f.fract() == 0.0 && f.abs() < 9.0e15 {
                format!("i:{}", f as i64)
            } else {
                format!("f:{n}")
            }
        }
        JsonValue::String(s) => format!("s:{s}"),
        JsonValue::Bool(b) => format!("b:{b}"),
        JsonValue::Null => "z:".to_string(),
        other => format!("j:{}", canonical_json_string(other)),
    }
}

/// Canonical primary-key-tuple string (NUL-separated so field boundaries cannot
/// shift-collide), used both as the diagnostic `row_key` and as the key material.
fn pk_tuple_canonical(pk_values: &[JsonValue]) -> String {
    pk_values
        .iter()
        .map(pk_value_token)
        .collect::<Vec<_>>()
        .join("\u{0}")
}

/// Salted, tenant+project+message-type-scoped revision key — NEVER the bare PK,
/// so a lookup can only ever match rows the caller is already scoped to, and two
/// tenants sharing a PK value cannot collide. Mirrors `idempotency_dedup_key`.
fn row_revision_key(
    tenant_id: &str,
    project_id: &str,
    message_type: &str,
    pk_canonical: &str,
) -> String {
    crate::runtime::executor_utils::checksum_str(&format!(
        "{tenant_id}\0{project_id}\0{message_type}\0{pk_canonical}"
    ))
}

/// #5: bump (or create at 1) the opaque revision of ONE row IN THE CALLER'S write
/// tx, returning the NEW revision. Monotonic (`revision = revision + 1` on
/// conflict), so an opaque token is ABA-safe (never reused or decreased). A SQL
/// failure is surfaced as a retryable, fail-closed error — the mutation cannot
/// commit without the bump, mirroring the idempotency claim's same-tx atomicity.
async fn bump_row_revision_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    config: &crate::runtime::system::SystemCatalogConfig,
    tenant_id: &str,
    project_id: &str,
    message_type: &str,
    pk_values: &[JsonValue],
) -> Result<i64, tonic::Status> {
    let pk_canonical = pk_tuple_canonical(pk_values);
    let revision_key = row_revision_key(tenant_id, project_id, message_type, &pk_canonical);
    let rel = config.row_revisions_relation();
    let sql = format!(
        "INSERT INTO {rel} AS rev \
             (revision_key, tenant_id, project_id, message_type, row_key, revision, updated_at) \
         VALUES ($1, $2, $3, $4, $5, 1, NOW()) \
         ON CONFLICT (revision_key) \
         DO UPDATE SET revision = rev.revision + 1, updated_at = NOW() \
         RETURNING revision"
    );
    let revision: i64 = sqlx::query_scalar(&sql)
        .bind(&revision_key)
        .bind(tenant_id)
        .bind(project_id)
        .bind(message_type)
        .bind(&pk_canonical)
        .fetch_one(&mut **tx)
        .await
        .map_err(|err| row_revision_store_status("row_revision_bump", &err))?;
    Ok(revision)
}

/// #5: assert a caller's `expected_revision` against the CURRENT opaque revision
/// of the primary-key-identified row, locking the revision row `FOR UPDATE` in the
/// caller's write tx so concurrent CAS writers serialize on it (ABA-safe: the
/// revision only increases, and the loser reads the bumped value and fails). A
/// missing revision entry OR any mismatch is a NON-DISCLOSING `FAILED_PRECONDITION`
/// (it never reveals a foreign row's existence or value).
async fn enforce_expected_revision_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    config: &crate::runtime::system::SystemCatalogConfig,
    tenant_id: &str,
    project_id: &str,
    message_type: &str,
    pk_values: &[JsonValue],
    expected_revision: &str,
) -> Result<(), tonic::Status> {
    let expected: i64 = expected_revision.trim().parse().map_err(|_| {
        setup_data_invalid_field(
            "expected_revision",
            "must be an opaque revision token previously returned by the broker",
            "expected_revision is not a valid revision token",
        )
    })?;
    let pk_canonical = pk_tuple_canonical(pk_values);
    let revision_key = row_revision_key(tenant_id, project_id, message_type, &pk_canonical);
    let rel = config.row_revisions_relation();
    let sql = format!("SELECT revision FROM {rel} WHERE revision_key = $1 FOR UPDATE");
    let current: Option<i64> = sqlx::query_scalar(&sql)
        .bind(&revision_key)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|err| row_revision_store_status("row_revision_cas_read", &err))?;
    match current {
        Some(revision) if revision == expected => Ok(()),
        _ => Err(row_revision_precondition_failed_status()),
    }
}

/// NON-DISCLOSING typed refusal for an `expected_revision` mismatch (#5). Names
/// only the contract violation; never leaks the current revision or row.
fn row_revision_precondition_failed_status() -> tonic::Status {
    crate::runtime::executor_utils::failed_precondition_fields(
        "revision precondition failed: the row's current revision differs from expected_revision, or the row is not revision-tracked",
        [(
            "expected_revision".to_string(),
            "the current revision differs from the expected revision".to_string(),
        )],
    )
}

/// Fail-closed, retryable status for a revision-store SQL failure. The keyed
/// mutation is refused (never silently skipped) so read-your-writes and CAS
/// callers can retry the SAME request while the tx is dropped fail-closed.
fn row_revision_store_status(operation: &'static str, err: &sqlx::Error) -> tonic::Status {
    tracing::error!(
        error = %err,
        operation,
        "row revision store operation failed; mutation refused fail-closed"
    );
    crate::runtime::executor_utils::retryable_status(
        "postgres",
        operation,
        250,
        "row revision store unavailable (fail-closed)",
    )
}

/// gate 25: no durable lock row exists for a caller that asked to be fenced.
/// Fail-closed — a caller presenting a fencing token for a lock that is not held
/// must not be allowed to write (the lease it believes it holds is gone).
fn fencing_lock_absent_status(lock_name: &str) -> tonic::Status {
    crate::runtime::executor_utils::failed_precondition_fields(
        "lock fencing failed: no active lock by that name to fence against",
        [(
            "lock_name".to_string(),
            format!("no durable lock row exists for '{lock_name}' in this tenant"),
        )],
    )
}

/// gate 25: the lock's lease has lapsed or been released — the writer outlived
/// its lease and is fenced off fail-closed.
fn fencing_lease_lost_status(lock_name: &str) -> tonic::Status {
    crate::runtime::executor_utils::failed_precondition_fields(
        "lock fencing failed: the lock lease has lapsed or been released (the writer outlived its lease)",
        [(
            "fencing_token".to_string(),
            format!("the lease for '{lock_name}' is no longer HELD"),
        )],
    )
}

// ── gate 23 (bounded bulk compare-and-swap) helpers ─────────────────────────────

/// Server ceiling on a single bulk-CAS batch — bounds the write tx and the
/// per-request memory. A caller's explicit `max_rows` is clamped INTO this.
const BULK_CAS_MAX_ROWS: usize = 1000;

/// Clamp the caller's explicit row ceiling into `[1, BULK_CAS_MAX_ROWS]`; a
/// non-positive request means "use the server maximum".
fn bulk_cas_effective_ceiling(requested: i32) -> usize {
    if requested <= 0 {
        BULK_CAS_MAX_ROWS
    } else {
        (requested as usize).min(BULK_CAS_MAX_ROWS)
    }
}

/// Canonical authoritative-input hash for a bulk-CAS batch (message type + every
/// item's filter/changes/increments/preconditions), so a keyed retry with the
/// SAME batch replays, and a key reused with a DIFFERENT batch is a conflict.
fn idempotency_request_hash_bulk_cas(
    message_type: &str,
    items: &[crate::proto::BulkCasItem],
) -> String {
    let authoritative = serde_json::json!({
        "message_type": message_type,
        "items": items
            .iter()
            .map(|item| {
                serde_json::json!({
                    "filter": item
                        .filter
                        .as_ref()
                        .map(crate::runtime::executor_utils::struct_to_json)
                        .unwrap_or(JsonValue::Null),
                    "changes": item
                        .changes
                        .as_ref()
                        .map(crate::runtime::executor_utils::struct_to_json)
                        .unwrap_or(JsonValue::Null),
                    "expected_revision": item.expected_revision,
                    "expected": idempotency_expected_json(item.expected.as_ref()),
                    "increments": item
                        .increments
                        .iter()
                        .map(|inc| serde_json::json!([inc.column, inc.delta]))
                        .collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>(),
    });
    idempotency_request_hash("bulk_cas", &authoritative)
}

/// gate 23: NON-erroring opaque-revision precondition check — returns whether it
/// HOLDS, locking the revision row FOR UPDATE. A parse failure IS an error (a
/// malformed token); a "row not tracked" or a mismatch returns `Ok(false)` so the
/// bulk path counts it as a conflict rather than aborting the whole batch.
async fn check_expected_revision_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    config: &crate::runtime::system::SystemCatalogConfig,
    tenant_id: &str,
    project_id: &str,
    message_type: &str,
    pk_values: &[JsonValue],
    expected_revision: &str,
) -> Result<bool, tonic::Status> {
    let expected: i64 = expected_revision.trim().parse().map_err(|_| {
        setup_data_invalid_field(
            "expected_revision",
            "must be an opaque revision token previously returned by the broker",
            "expected_revision is not a valid revision token",
        )
    })?;
    let pk_canonical = pk_tuple_canonical(pk_values);
    let revision_key = row_revision_key(tenant_id, project_id, message_type, &pk_canonical);
    let rel = config.row_revisions_relation();
    let sql = format!("SELECT revision FROM {rel} WHERE revision_key = $1 FOR UPDATE");
    let current: Option<i64> = sqlx::query_scalar(&sql)
        .bind(&revision_key)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|err| row_revision_store_status("row_revision_cas_read", &err))?;
    Ok(current == Some(expected))
}

/// gate 23: evaluate an optional field-map compare-and-swap precondition against
/// an already-read (decrypted) row, returning whether it HOLDS. Empty precondition
/// holds trivially. Reuses the same field resolution + int/float-tolerant value
/// match (`json_values_match`) as `enforce_cas_precondition`.
fn bulk_cas_field_precondition_holds(
    row: &JsonValue,
    expected: Option<&prost_types::Struct>,
    resolver: &std::collections::HashMap<String, String>,
) -> bool {
    let Some(expected) = expected.filter(|expected| !expected.fields.is_empty()) else {
        return true;
    };
    let current = row.as_object();
    let expected_json = crate::runtime::executor_utils::struct_to_json(expected);
    let Some(expected_obj) = expected_json.as_object() else {
        return true;
    };
    for (field, want) in expected_obj {
        let column = crate::planning::broker::resolve_column(resolver, field);
        let have = current
            .and_then(|obj| obj.get(&column).or_else(|| obj.get(field)))
            .unwrap_or(&JsonValue::Null);
        if !json_values_match(have, want) {
            return false;
        }
    }
    true
}

/// gate 23: persist the batch's counts into the dedup row (in the caller's tx) so
/// a keyed retry replays them (never re-applies). Stored in the same JSONB
/// `response_json` column the keyed unary mutations use, via the shared persist SQL.
async fn persist_bulk_cas_idempotency_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dedup_ctx: &IdempotencyPersistContext,
    matched: i32,
    changed: i32,
    conflicted: i32,
    write_receipt_json: &str,
) -> Result<(), tonic::Status> {
    let summary = serde_json::json!({
        "op": "bulk_cas",
        "matched": matched,
        "changed": changed,
        "conflicted": conflicted,
        "write_receipt_json": write_receipt_json,
    });
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
                "bulk CAS idempotency persist failed",
                &err,
            )
        })?;
    idempotency_response_persist_row_count_status(result.rows_affected())?;
    Ok(())
}

/// gate 23: reconstruct a `BulkCasResponse` (counts only; per-item results are not
/// re-derived on replay) from a replayed dedup `response_json`. A legacy/empty row
/// yields a typed internal error rather than a bogus zero-count success.
fn bulk_cas_response_from_idempotency_json(
    prior: &JsonValue,
) -> Result<crate::proto::BulkCasResponse, tonic::Status> {
    let counts = ["matched", "changed", "conflicted"]
        .iter()
        .map(|field| {
            prior
                .get(field)
                .and_then(JsonValue::as_i64)
                .map(|value| value as i32)
                .ok_or_else(|| {
                    idempotency_replay_response_status(format!("missing bulk CAS count {field}"))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let write_receipt_json = prior
        .get("write_receipt_json")
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_string();
    Ok(crate::proto::BulkCasResponse {
        matched: counts[0],
        changed: counts[1],
        conflicted: counts[2],
        write_receipt_json,
        results: Vec::new(),
    })
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
    /// The first writer's canonical request hash (#6). `None` only for a legacy
    /// row written before the `request_hash` column existed; such rows are
    /// replayed best-effort (no mismatch can be proven). For a fresh claim this
    /// is the row we just inserted, so it is never inspected.
    prior_request_hash: Option<String>,
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

/// Version tag for the idempotency request hash (#6). Bump to force a
/// conservative mismatch (typed conflict, never a bogus replay) if the set of
/// authoritative inputs covered below ever changes shape.
const IDEMPOTENCY_REQUEST_HASH_VERSION: u32 = 1;

/// Serialize a JSON value with object keys sorted recursively, so structurally
/// equal values produce byte-identical output regardless of key insertion order
/// (robust even if serde_json's `preserve_order` feature is enabled elsewhere in
/// the workspace). Array order is PRESERVED — it is semantic.
fn canonical_json_string(value: &JsonValue) -> String {
    fn canonical(value: &JsonValue) -> JsonValue {
        match value {
            JsonValue::Object(map) => {
                let mut entries: Vec<(&String, &JsonValue)> = map.iter().collect();
                entries.sort_by(|a, b| a.0.cmp(b.0));
                JsonValue::Object(
                    entries
                        .into_iter()
                        .map(|(key, val)| (key.clone(), canonical(val)))
                        .collect(),
                )
            }
            JsonValue::Array(items) => JsonValue::Array(items.iter().map(canonical).collect()),
            other => other.clone(),
        }
    }
    serde_json::to_string(&canonical(value)).unwrap_or_default()
}

/// Canonical SHA-256 over the mutation-authoritative inputs of a keyed write
/// (#6). Two requests that reuse the same idempotency key but carry DIFFERENT
/// authoritative inputs (filter / record / changes / mask / increments /
/// conflict strategy / expected precondition) MUST hash differently, so the
/// replay path can reject the mismatch instead of returning the first writer's
/// success for an op that never ran. `authoritative` MUST exclude transport-only
/// fields (correlation id, deadlines, cache toggles, `return_record`) — those do
/// not change the write and must not rotate the hash. Object keys are
/// canonicalized so key ordering is not significant.
fn idempotency_request_hash(operation: &str, authoritative: &JsonValue) -> String {
    let payload = serde_json::json!({
        "v": IDEMPOTENCY_REQUEST_HASH_VERSION,
        "op": operation,
        "in": authoritative,
    });
    crate::runtime::executor_utils::checksum_str(&canonical_json_string(&payload))
}

/// Render an optional CAS `expected` precondition to canonical JSON for hashing.
/// An unset or empty precondition is `null` (identical to a request that never
/// asserted one), so adding/removing a precondition rotates the hash.
fn idempotency_expected_json(expected: Option<&prost_types::Struct>) -> JsonValue {
    match expected.filter(|expected| !expected.fields.is_empty()) {
        Some(expected) => crate::runtime::executor_utils::struct_to_json(expected),
        None => JsonValue::Null,
    }
}

/// Authoritative-input hash for a keyed `Upsert`: the record to write, the
/// conflict target, and any CAS precondition. `return_record`/cache are
/// transport-only and excluded.
fn idempotency_request_hash_upsert(request: &UpsertRequest, record: &JsonValue) -> String {
    let authoritative = serde_json::json!({
        "record": record,
        "conflict_fields": request.conflict_fields,
        "expected": idempotency_expected_json(request.expected.as_ref()),
    });
    idempotency_request_hash("upsert", &authoritative)
}

/// Authoritative-input hash for a keyed `Delete`: the normalized filter (which
/// rows) and any CAS precondition.
fn idempotency_request_hash_delete(
    normalized_filter: &JsonValue,
    expected: Option<&prost_types::Struct>,
) -> String {
    let authoritative = serde_json::json!({
        "filter": normalized_filter,
        "expected": idempotency_expected_json(expected),
    });
    idempotency_request_hash("delete", &authoritative)
}

/// Authoritative-input hash for a keyed `Update`: the normalized filter (which
/// rows), the assignments/mask, the numeric increments, and any CAS precondition.
fn idempotency_request_hash_update(
    normalized_filter: &JsonValue,
    changes: &JsonValue,
    increments: &[(String, f64)],
    expected: Option<&prost_types::Struct>,
) -> String {
    let authoritative = serde_json::json!({
        "filter": normalized_filter,
        "changes": changes,
        "increments": increments
            .iter()
            .map(|(column, delta)| serde_json::json!([column, delta]))
            .collect::<Vec<_>>(),
        "expected": idempotency_expected_json(expected),
    });
    idempotency_request_hash("update", &authoritative)
}

/// NON-DISCLOSING typed refusal for a keyed mutation whose idempotency key was
/// already claimed by a request with DIFFERENT authoritative inputs (#6). The
/// message names ONLY the contract violation — it must never leak the first
/// writer's stored inputs or response.
fn idempotency_request_mismatch_status() -> tonic::Status {
    crate::runtime::executor_utils::failed_precondition_fields(
        "idempotency_key was already used for a different request; reuse a key only to retry an identical request",
        [(
            "idempotency_key".to_string(),
            "the same idempotency_key was already claimed by a request with different inputs"
                .to_string(),
        )],
    )
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
    // #6: the current request's canonical authoritative-input hash. Stored on the
    // FRESH insert (the winner's identity) and NEVER overwritten by a later
    // conflicting claim, so `RETURNING request_hash` on a replay yields the first
    // writer's hash for the caller to compare against.
    request_hash: &str,
) -> Result<IdempotencyClaim, tonic::Status> {
    let rel = config.idempotency_keys_relation();
    let sql = idempotency_claim_sql(&rel);
    let row: Option<(bool, JsonValue, Option<String>)> = sqlx::query_as(&sql)
        .bind(dedup_key)
        .bind(tenant_id)
        .bind(project_id)
        .bind(message_type)
        .bind(operation)
        .bind(request_hash)
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
        prior_request_hash: row.2,
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

/// Extract the equality value for each key column from a normalized filter, for
/// conditional delete (G-2). Every key column MUST be pinned by equality — bare
/// (`{"col": v}`) or `{"col": {"$eq": v}}` — otherwise the precondition cannot
/// target a single row deterministically and the delete is refused. This is the
/// deliberately conservative semantic: CAS-delete is "remove THIS row if it still
/// looks like this", not "remove whatever this range matches, conditionally".
fn pk_equality_values_from_filter(
    filter: &JsonValue,
    key_columns: &[String],
) -> Result<Vec<JsonValue>, tonic::Status> {
    let reject = |column: &str, why: &str| {
        crate::runtime::executor_utils::failed_precondition_fields(
            "conditional delete requires an equality filter on every primary-key column",
            [(column.to_string(), why.to_string())],
        )
    };
    let object = filter
        .as_object()
        .ok_or_else(|| reject("filter", "filter must be a JSON object"))?;
    let mut values = Vec::with_capacity(key_columns.len());
    for column in key_columns {
        let raw = object.get(column).ok_or_else(|| {
            reject(
                column,
                "primary-key column is not constrained by the filter",
            )
        })?;
        let value = match raw {
            JsonValue::Object(inner) => {
                // Only a lone {"$eq": v} is a single-row equality; any other
                // operator (ranges, IN, …) can match multiple rows and is unsafe
                // for a single-row compare-and-swap.
                match (inner.len(), inner.get("$eq").or_else(|| inner.get("eq"))) {
                    (1, Some(v)) => v.clone(),
                    _ => {
                        return Err(reject(
                            column,
                            "primary-key column must be pinned by equality, not an operator",
                        ));
                    }
                }
            }
            other => other.clone(),
        };
        values.push(value);
    }
    Ok(values)
}

fn idempotency_claim_sql(rel: &str) -> String {
    // Single-statement claim that ALWAYS returns exactly one row for the winner's
    // scope, and blocks correctly under READ COMMITTED.
    //
    // The previous `DO NOTHING` + `UNION ALL SELECT` form had a concurrent-
    // duplicate race: when T2 conflicts, `DO NOTHING` returns nothing and the
    // `SELECT` half still runs on T2's PRE-COMMIT snapshot, so it does not see
    // T1's row → zero rows → the caller returned INTERNAL for the exact case
    // idempotency exists to handle (UDB-DB-READINESS / F-2).
    //
    // `ON CONFLICT DO UPDATE` instead takes a row lock on the conflicting tuple,
    // so T2 WAITS for T1 to commit or abort. On T1 commit, the touch fires on the
    // now-visible row and RETURNING yields T1's committed `response_json` (T1
    // persists it earlier in the same tx). On T1 abort, T2's INSERT succeeds.
    // `xmax = 0` distinguishes a fresh insert (this statement) from a replay.
    //
    // The scope predicate on the UPDATE preserves the old behaviour for a genuine
    // cross-scope `dedup_key` collision: the touch is skipped, RETURNING is empty,
    // and the caller still surfaces the "no row for matching scope" error.
    // `idem.response_json = idem.response_json` is a self-touch that never
    // overwrites the winner's stored body.
    //
    // #6: `request_hash` ($6) is written ONLY on the fresh INSERT and the DO UPDATE
    // deliberately does not touch it, so a replay's `RETURNING request_hash` yields
    // the FIRST writer's hash. The caller compares it to the current request's hash
    // and refuses a key reused with different authoritative inputs (instead of
    // replaying a bogus success for an op that never ran).
    format!(
        "INSERT INTO {rel} AS idem
             (dedup_key, tenant_id, project_id, message_type, operation, request_hash, response_json)
         VALUES ($1, $2, $3, $4, $5, $6, '{{}}'::jsonb)
         ON CONFLICT (dedup_key) DO UPDATE
             SET response_json = idem.response_json
             WHERE idem.tenant_id = $2
               AND idem.project_id = $3
               AND idem.message_type = $4
               AND idem.operation = $5
         RETURNING (xmax = 0) AS inserted, response_json, request_hash"
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
        empty_object_stream_status, es_payload_filter_terms, gcs_feature_status,
        invalid_part_count_status, invalid_presign_ttl_status, json_values_match,
        no_object_store_feature_status, object_instance_missing_status,
        parse_vector_search_response, pinecone_metadata_filter, qdrant_vector_feature_status,
        s3_object_feature_status, setup_data_internal_status, text_search_dispatch_spec,
        unknown_message_type_status, unsupported_object_backend_status,
        unsupported_presign_method_status, vector_hybrid_qdrant_only_status,
        vector_search_dispatch_spec, vector_upsert_dispatch_spec, weaviate_where_arg,
    };
    use crate::proto::{ErrorDetail, ErrorKind, VectorPointMutation, VectorSearchRequest};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    /// Pin: the PUT handler stamps `server_side_encryption: true` into the object
    /// request spec only when the plan requires it, without disturbing the other
    /// spec fields. The S3 executor reads exactly this flag to request SSE-S3, so
    /// dropping the stamp reintroduces the silent-plaintext bug.
    #[test]
    fn object_request_json_require_sse_sets_the_flag() {
        let base = super::object_request_json("put", "bkt", "key/1", "text/plain");
        // Pre-fix shape carries no SSE flag.
        let before: serde_json::Value = serde_json::from_str(&base).unwrap();
        assert!(before.get("server_side_encryption").is_none());

        let stamped = super::object_request_json_require_sse(&base);
        let after: serde_json::Value = serde_json::from_str(&stamped).unwrap();
        assert_eq!(after["server_side_encryption"], serde_json::json!(true));
        // Other fields survive the stamp.
        assert_eq!(after["bucket"], serde_json::json!("bkt"));
        assert_eq!(after["object_key"], serde_json::json!("key/1"));
        assert_eq!(after["content_type"], serde_json::json!("text/plain"));
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

    #[test]
    fn es_vector_search_injects_the_tenant_filter() {
        // Regression (cross-tenant read leak): the Elasticsearch arm previously
        // ran `match_all` and ignored `request.filter`, returning other tenants'
        // documents. The generated `_search` body must now AND in a `term`
        // filter on `payload._tenant_id.keyword` for the caller's tenant, with
        // the vector similarity preserved under `bool.must`.
        let filter = crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
            "must": [{ "key": "_tenant_id", "match": { "value": "acme" } }]
        }));
        let spec = vector_search_dispatch_spec(
            "elasticsearch",
            &VectorSearchRequest {
                context: None,
                collection: "Docs".to_string(),
                vector: vec![0.1, 0.2],
                filter,
                limit: 10,
                score_threshold: 0.0,
                with_payload: true,
                with_vector: false,
                vector_name: String::new(),
                quantization_rescore: false,
            },
        )
        .expect("es vector search spec");
        let json: serde_json::Value = serde_json::from_str(&spec).expect("spec json");
        assert_eq!(
            json["body"]["query"]["bool"]["filter"],
            serde_json::json!([{ "term": { "payload._tenant_id.keyword": "acme" } }]),
            "ES vector search must scope to the caller's tenant"
        );
        assert!(
            json["body"]["query"]["bool"]["must"][0]["script_score"].is_object(),
            "the vector similarity must still be applied"
        );
    }

    #[test]
    fn es_vector_search_without_a_filter_adds_no_terms() {
        // No filter (direct, non-tenant-scoped caller) → empty filter list, i.e.
        // unchanged from the historical behavior; the search-service path always
        // supplies the tenant filter (fail-closed) so this branch is not a leak.
        let spec = vector_search_dispatch_spec(
            "elasticsearch",
            &VectorSearchRequest {
                context: None,
                collection: "Docs".to_string(),
                vector: vec![0.1],
                filter: None,
                limit: 5,
                score_threshold: 0.0,
                with_payload: false,
                with_vector: false,
                vector_name: String::new(),
                quantization_rescore: false,
            },
        )
        .expect("es vector search spec");
        let json: serde_json::Value = serde_json::from_str(&spec).expect("spec json");
        assert_eq!(
            json["body"]["query"]["bool"]["filter"],
            serde_json::json!([])
        );
    }

    fn tenant_scoped_filter() -> Option<prost_types::Struct> {
        crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
            "must": [{ "key": "_tenant_id", "match": { "value": "acme" } }]
        }))
    }

    #[test]
    fn es_full_text_search_builds_multi_match_and_tenant_filter() {
        // SEARCH_MODE_TEXT (lexical-only, no query vector): the generated ES
        // `_search` must carry a BM25 `multi_match` over `payload.*` AND AND-in a
        // `term` filter on `payload._tenant_id.keyword` for the caller's tenant
        // (the security boundary), mirroring the vector arm. `query_text` is
        // threaded separately because VectorSearchRequest has no text field.
        let spec = text_search_dispatch_spec(
            "elasticsearch",
            &VectorSearchRequest {
                context: None,
                collection: "Docs".to_string(),
                vector: Vec::new(),
                filter: tenant_scoped_filter(),
                limit: 7,
                score_threshold: 0.0,
                with_payload: true,
                with_vector: false,
                vector_name: String::new(),
                quantization_rescore: false,
            },
            "quarterly revenue",
        )
        .expect("es full-text search spec");
        let json: serde_json::Value = serde_json::from_str(&spec).expect("spec json");
        assert_eq!(json["body"]["size"], serde_json::json!(7));
        let multi_match = &json["body"]["query"]["bool"]["must"][0]["multi_match"];
        assert_eq!(
            multi_match["query"],
            serde_json::json!("quarterly revenue"),
            "the lexical query text must be applied"
        );
        assert_eq!(multi_match["fields"], serde_json::json!(["payload.*"]));
        assert_eq!(
            json["body"]["query"]["bool"]["filter"],
            serde_json::json!([{ "term": { "payload._tenant_id.keyword": "acme" } }]),
            "ES full-text search must scope to the caller's tenant"
        );
    }

    #[test]
    fn es_full_text_search_without_a_filter_adds_no_terms() {
        // No filter (direct, non-tenant-scoped caller) → empty filter list; the
        // search-service path always supplies the tenant filter (fail-closed) so
        // this branch is not a leak. A non-positive limit falls back to 10.
        let spec = text_search_dispatch_spec(
            "elasticsearch",
            &VectorSearchRequest {
                context: None,
                collection: "Docs".to_string(),
                vector: Vec::new(),
                filter: None,
                limit: 0,
                score_threshold: 0.0,
                with_payload: false,
                with_vector: false,
                vector_name: String::new(),
                quantization_rescore: false,
            },
            "hello",
        )
        .expect("es full-text search spec");
        let json: serde_json::Value = serde_json::from_str(&spec).expect("spec json");
        assert_eq!(
            json["body"]["query"]["bool"]["filter"],
            serde_json::json!([])
        );
        assert_eq!(json["body"]["size"], serde_json::json!(10));
    }

    #[test]
    fn text_search_unsupported_backend_fails_closed() {
        // Qdrant text-only (and every non-ES backend) must fail closed with a
        // typed capability detail rather than routing a degraded/empty query.
        let err = text_search_dispatch_spec(
            "qdrant",
            &VectorSearchRequest {
                context: None,
                collection: "Docs".to_string(),
                vector: Vec::new(),
                filter: None,
                limit: 10,
                score_threshold: 0.0,
                with_payload: false,
                with_vector: false,
                vector_name: String::new(),
                quantization_rescore: false,
            },
            "hello",
        )
        .expect_err("qdrant text-only full-text search must fail closed");
        assert_capability_detail(
            &err,
            "qdrant",
            "typed_full_text_search",
            "typed_full_text_search_backend",
            "typed full-text search is not wired for backend 'qdrant'",
        );
    }

    #[test]
    fn weaviate_vector_search_injects_the_tenant_where() {
        // Regression: the Weaviate arm ignored request.filter (same cross-tenant
        // leak class as ES). The GraphQL must now carry a `where` scoping to the
        // caller's tenant.
        let spec = vector_search_dispatch_spec(
            "weaviate",
            &VectorSearchRequest {
                context: None,
                collection: "Docs".to_string(),
                vector: vec![0.1, 0.2],
                filter: tenant_scoped_filter(),
                limit: 10,
                score_threshold: 0.0,
                with_payload: true,
                with_vector: false,
                vector_name: String::new(),
                quantization_rescore: false,
            },
        )
        .expect("weaviate vector search spec");
        let json: serde_json::Value = serde_json::from_str(&spec).expect("spec json");
        let query = json["body"]["query"].as_str().expect("graphql query");
        assert!(
            query.contains("where:") && query.contains("_tenant_id") && query.contains("acme"),
            "weaviate query must scope to the tenant: {query}"
        );
    }

    #[test]
    fn pinecone_vector_search_injects_the_tenant_filter() {
        // Regression: the Pinecone arm ignored request.filter. The /query body must
        // now AND a metadata filter on the caller's tenant.
        let spec = vector_search_dispatch_spec(
            "pinecone",
            &VectorSearchRequest {
                context: None,
                collection: "Docs".to_string(),
                vector: vec![0.1, 0.2],
                filter: tenant_scoped_filter(),
                limit: 10,
                score_threshold: 0.0,
                with_payload: true,
                with_vector: false,
                vector_name: String::new(),
                quantization_rescore: false,
            },
        )
        .expect("pinecone vector search spec");
        let json: serde_json::Value = serde_json::from_str(&spec).expect("spec json");
        assert_eq!(
            json["body"]["filter"],
            serde_json::json!({ "_tenant_id": { "$eq": "acme" } }),
            "pinecone query must scope to the tenant"
        );
    }

    #[cfg(feature = "qdrant")]
    #[test]
    fn stamp_generic_vector_point_payloads_tags_every_point() {
        // The generic (non-Qdrant) dispatch must stamp the tenant/project tag the
        // ES search then filters on — mirroring the Qdrant write-time stamp.
        use crate::proto::VectorUpsertRequest;
        let req = VectorUpsertRequest {
            context: None,
            collection: "Docs".to_string(),
            points: vec![VectorPointMutation {
                id: "p1".to_string(),
                vector: vec![0.1],
                payload: crate::runtime::executor_utils::json_to_struct(
                    &serde_json::json!({ "body": "hi" }),
                ),
                vector_name: String::new(),
            }],
            idempotency_key: String::new(),
        };
        let stamped = super::stamp_generic_vector_point_payloads(&req, "acme", "billing");
        let payload = crate::runtime::executor_utils::struct_to_json(
            stamped.points[0]
                .payload
                .as_ref()
                .expect("stamped payload present"),
        );
        assert_eq!(payload["_tenant_id"], serde_json::json!("acme"));
        assert_eq!(payload["_project_id"], serde_json::json!("billing"));
        // The original payload field survives the stamp.
        assert_eq!(payload["body"], serde_json::json!("hi"));
    }

    fn parent_window_filter() -> Option<prost_types::Struct> {
        crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
            "must": [
                { "key": "_tenant_id", "match": { "value": "acme" } },
                { "key": "_parent_pk", "match": { "any": ["row-1", "row-2"] } }
            ]
        }))
    }

    #[test]
    fn es_filter_translates_match_any_into_a_terms_clause() {
        // The parent-window gather scopes `_parent_pk` with `match.any`; previously
        // `struct_filter_equality_terms` skipped it (no `value`) so the gather ran
        // UNSCOPED. It must now become an ES `terms` clause over the OR-set while the
        // tenant equality stays a `term` — both within the tenant.
        let clauses = es_payload_filter_terms(parent_window_filter().as_ref());
        assert!(
            clauses.contains(&serde_json::json!({
                "term": { "payload._tenant_id.keyword": "acme" }
            })),
            "tenant equality term must remain: {clauses:?}"
        );
        assert!(
            clauses.contains(&serde_json::json!({
                "terms": { "payload._parent_pk.keyword": ["row-1", "row-2"] }
            })),
            "match.any must become a terms clause, not be dropped: {clauses:?}"
        );
    }

    #[test]
    fn weaviate_where_translates_match_any_into_an_or() {
        // `match.any` → a weaviate `Or` of `Equal` operands so the gather stays
        // scoped; combined with the tenant equality under a top-level `And`.
        let arg = weaviate_where_arg(parent_window_filter().as_ref());
        assert!(arg.contains("operator: And"), "combined under And: {arg}");
        assert!(arg.contains("operator: Or"), "any-set becomes an Or: {arg}");
        assert!(
            arg.contains("row-1") && arg.contains("row-2"),
            "both any values present: {arg}"
        );
        assert!(arg.contains("_tenant_id"), "tenant scope preserved: {arg}");
    }

    #[test]
    fn pinecone_filter_translates_match_any_into_in() {
        // `match.any` → Pinecone `$in`; tenant equality stays `$eq`.
        let filter = pinecone_metadata_filter(parent_window_filter().as_ref());
        assert_eq!(
            filter,
            serde_json::json!({
                "_tenant_id": { "$eq": "acme" },
                "_parent_pk": { "$in": ["row-1", "row-2"] }
            }),
            "match.any must become a $in set, not be dropped"
        );
    }

    #[test]
    fn es_vector_hits_unwrap_nested_payload_and_normalize_cosine() {
        // ES upserts store `{ "vector":[…], "payload":{…stamped…} }`. The vector-arm
        // hit must (a) lift the nested `payload` object as the point payload — so the
        // stamped provenance/tenant keys sit at the top level like qdrant — (b) NOT
        // return the raw dense `vector`, and (c) subtract the `+1.0` cosine offset so
        // the score is a comparable cosine (`1.6 → 0.6`).
        let raw = serde_json::json!({
            "hits": { "hits": [{
                "_id": "row-1#chunk:0",
                "_score": 1.6,
                "_source": {
                    "vector": [0.1, 0.2, 0.3],
                    "payload": {
                        "_tenant_id": "acme",
                        "_parent_pk": "row-1",
                        "_chunk_text": "hello"
                    }
                }
            }] }
        })
        .to_string();
        let set = parse_vector_search_response("elasticsearch", &raw, true)
            .expect("es vector response parses");
        assert_eq!(set.points.len(), 1);
        let point = &set.points[0];
        assert_eq!(point.id, "row-1#chunk:0");
        assert!(
            (point.score - 0.6).abs() < 1e-5,
            "cosine offset must be stripped: {}",
            point.score
        );
        assert!(point.vector.is_empty(), "dense vector must not be returned");
        let payload = crate::runtime::executor_utils::struct_to_json(
            point.payload.as_ref().expect("payload lifted"),
        );
        assert_eq!(payload["_tenant_id"], serde_json::json!("acme"));
        assert_eq!(payload["_parent_pk"], serde_json::json!("row-1"));
        assert_eq!(payload["_chunk_text"], serde_json::json!("hello"));
        // The raw dense vector must NOT leak into the caller-facing payload.
        assert!(payload.get("vector").is_none(), "vector leaked: {payload}");
    }

    #[test]
    fn es_text_hits_pass_bm25_score_through_unchanged() {
        // The text/BM25 arm passes `es_cosine_offset = false`: `_score` is relevance,
        // not offset cosine, so it must NOT have 1.0 subtracted.
        let raw = serde_json::json!({
            "hits": { "hits": [{
                "_id": "row-9",
                "_score": 4.25,
                "_source": { "payload": { "_tenant_id": "acme" } }
            }] }
        })
        .to_string();
        let set =
            parse_vector_search_response("elasticsearch", &raw, false).expect("es text parses");
        assert!((set.points[0].score - 4.25).abs() < 1e-5);
    }

    #[test]
    fn weaviate_hits_parse_into_points_with_cosine_score_and_payload() {
        // Regression (hollow): the weaviate arm returned `Vec::new()` so every
        // retrieval was empty even though the query dispatched. It must now parse the
        // GraphQL `data.Get.<class>` array: id from `_additional.id`, cosine score
        // from `distance` (`1 - 0.2 = 0.8`), and the stored properties (minus the
        // reserved `_additional`) as the point payload.
        let raw = serde_json::json!({
            "data": { "Get": { "UdbDocs": [{
                "_additional": { "id": "row-7", "distance": 0.2, "certainty": 0.9 },
                "_tenant_id": "acme",
                "_project_id": "billing"
            }] } }
        })
        .to_string();
        let set = parse_vector_search_response("weaviate", &raw, false)
            .expect("weaviate response parses");
        assert_eq!(set.points.len(), 1, "weaviate hit must not be dropped");
        let point = &set.points[0];
        assert_eq!(point.id, "row-7");
        assert!(
            (point.score - 0.8).abs() < 1e-5,
            "cosine distance must map to similarity: {}",
            point.score
        );
        assert!(point.vector.is_empty(), "dense vector must not be returned");
        let payload = crate::runtime::executor_utils::struct_to_json(
            point.payload.as_ref().expect("payload present"),
        );
        assert_eq!(payload["_tenant_id"], serde_json::json!("acme"));
        // The reserved `_additional` metadata must never leak into the payload.
        assert!(
            payload.get("_additional").is_none(),
            "meta leaked: {payload}"
        );
    }
}

#[cfg(test)]
mod setup_data_consistency_tests {
    use super::{
        RequestContext, bulk_cas_effective_ceiling, bulk_cas_field_precondition_holds,
        bulk_cas_response_from_idempotency_json, fencing_lease_lost_status,
        fencing_lock_absent_status, full_canonical_store_requires_opt_in, idempotency_claim_sql,
        idempotency_dedup_claim_status, idempotency_dedup_key, idempotency_key_for_dedup,
        idempotency_request_hash_bulk_cas, idempotency_request_hash_delete,
        idempotency_request_hash_update, idempotency_request_hash_upsert,
        idempotency_request_mismatch_status, idempotency_response_persist_row_count_status,
        idempotency_response_persist_sql, merge_runtime_backend_instances,
        mutation_response_from_idempotency_json, mutation_response_from_idempotency_json_for_claim,
        mutation_response_idempotency_json, mutation_response_resource_uri,
        mutation_response_resource_uri_or_fallback, pg_outbox_receipt_store_mismatch,
        pk_equality_values_from_filter, pk_tuple_canonical, pk_value_token,
        projection_system_store_opt_in_value, returned_record_json_or_status, row_revision_key,
        row_revision_precondition_failed_status, validate_deployment_tier_floor,
        write_receipt_json_or_status,
    };
    use crate::backend::ControlPlaneHaLevel;
    use crate::proto::{BulkCasItem, ErrorDetail, ErrorKind, MutationResponse, UpsertRequest};
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

    // ── #5 (opaque row revision / ETag) + gate 23/25 unit tests ──────────────

    #[test]
    fn pk_value_token_collapses_int_and_float_forms() {
        // The revision-map key MUST be identical whether a numeric PK arrives as an
        // INTEGER column (`8`) or a google.protobuf.Struct f64 (`8.0`) — otherwise
        // the bump-side key (from the written record) and the read-side key (from
        // the returned row) diverge for integer PKs supplied via a Struct payload.
        assert_eq!(
            pk_value_token(&serde_json::json!(8)),
            pk_value_token(&serde_json::json!(8.0))
        );
        assert_eq!(pk_value_token(&serde_json::json!(8)), "i:8");
        // Strings / genuine floats stay distinct + typed (no "8" == 8 confusion).
        assert_ne!(
            pk_value_token(&serde_json::json!("8")),
            pk_value_token(&serde_json::json!(8))
        );
        assert_eq!(pk_value_token(&serde_json::json!("abc")), "s:abc");
        assert!(pk_value_token(&serde_json::json!(8.5)).starts_with("f:"));
    }

    #[test]
    fn pk_tuple_canonical_is_nul_separated_and_shift_safe() {
        // ("a","bc") vs ("ab","c") must not collide (the NUL boundary prevents a
        // composite PK from being confused with a shifted one).
        let a = pk_tuple_canonical(&[serde_json::json!("a"), serde_json::json!("bc")]);
        let b = pk_tuple_canonical(&[serde_json::json!("ab"), serde_json::json!("c")]);
        assert_ne!(a, b);
    }

    #[test]
    fn row_revision_key_is_scoped_and_never_bare_pk() {
        let pk = "s:row-1";
        let a = row_revision_key("t-a", "p-1", "Invoice", pk);
        assert_ne!(
            a,
            row_revision_key("t-b", "p-1", "Invoice", pk),
            "tenant isolation"
        );
        assert_ne!(
            a,
            row_revision_key("t-a", "p-2", "Invoice", pk),
            "project isolation"
        );
        assert_ne!(
            a,
            row_revision_key("t-a", "p-1", "Payment", pk),
            "message-type isolation"
        );
        assert_eq!(
            a,
            row_revision_key("t-a", "p-1", "Invoice", pk),
            "deterministic"
        );
        assert!(a.starts_with("sha256:"));
        assert!(!a.contains("row-1"), "must never embed the bare PK");
    }

    #[test]
    fn bulk_cas_effective_ceiling_is_bounded() {
        assert_eq!(bulk_cas_effective_ceiling(0), 1000, "unset → server max");
        assert_eq!(
            bulk_cas_effective_ceiling(-5),
            1000,
            "non-positive → server max"
        );
        assert_eq!(bulk_cas_effective_ceiling(10), 10, "explicit under ceiling");
        assert_eq!(
            bulk_cas_effective_ceiling(100_000),
            1000,
            "clamped to server max"
        );
    }

    #[test]
    fn bulk_cas_request_hash_binds_authoritative_inputs() {
        let item = |revision: &str| BulkCasItem {
            filter: None,
            changes: None,
            expected_revision: revision.to_string(),
            expected: None,
            increments: Vec::new(),
        };
        let a = idempotency_request_hash_bulk_cas("Invoice", &[item("1")]);
        assert_eq!(
            a,
            idempotency_request_hash_bulk_cas("Invoice", &[item("1")]),
            "stable for identical batches"
        );
        assert_ne!(
            a,
            idempotency_request_hash_bulk_cas("Invoice", &[item("2")]),
            "revision precondition is authoritative"
        );
        assert_ne!(
            a,
            idempotency_request_hash_bulk_cas("Payment", &[item("1")]),
            "message type is authoritative"
        );
    }

    #[test]
    fn bulk_cas_field_precondition_evaluates_without_erroring() {
        let row = serde_json::json!({"status": "active", "version": 3});
        let resolver: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        // No precondition → holds.
        assert!(bulk_cas_field_precondition_holds(&row, None, &resolver));
        let want = |field: &str, kind: prost_types::value::Kind| prost_types::Struct {
            fields: std::collections::BTreeMap::from([(
                field.to_string(),
                prost_types::Value { kind: Some(kind) },
            )]),
        };
        // Matching field → holds; mismatched field → conflict (false, NOT an error).
        let ok = want(
            "status",
            prost_types::value::Kind::StringValue("active".into()),
        );
        assert!(bulk_cas_field_precondition_holds(
            &row,
            Some(&ok),
            &resolver
        ));
        let bad = want(
            "status",
            prost_types::value::Kind::StringValue("archived".into()),
        );
        assert!(!bulk_cas_field_precondition_holds(
            &row,
            Some(&bad),
            &resolver
        ));
        // int/float tolerance: an INTEGER column 3 matches an asserted 3.0.
        let num = want("version", prost_types::value::Kind::NumberValue(3.0));
        assert!(bulk_cas_field_precondition_holds(
            &row,
            Some(&num),
            &resolver
        ));
    }

    #[test]
    fn bulk_cas_response_replays_counts_or_fails_closed() {
        let ok = serde_json::json!({
            "matched": 5, "changed": 3, "conflicted": 2, "write_receipt_json": "{}"
        });
        let resp = bulk_cas_response_from_idempotency_json(&ok).expect("valid replay decodes");
        assert_eq!((resp.matched, resp.changed, resp.conflicted), (5, 3, 2));
        assert!(
            resp.results.is_empty(),
            "per-item results are not re-derived on replay"
        );
        // A legacy/empty row fails closed rather than replaying a bogus 0-count.
        let err = bulk_cas_response_from_idempotency_json(&serde_json::json!({}))
            .expect_err("missing counts must fail closed");
        assert_eq!(err.code(), tonic::Code::Internal);
    }

    #[test]
    fn revision_and_fencing_refusals_are_typed_precondition_errors() {
        for status in [
            row_revision_precondition_failed_status(),
            fencing_lock_absent_status("orders"),
            fencing_lease_lost_status("orders"),
        ] {
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            let detail = decode_error_detail(&status);
            assert_eq!(detail.kind, ErrorKind::Validation as i32);
            assert!(!detail.retryable);
            assert_eq!(detail.field_violations.len(), 1);
        }
        // Non-disclosing: names only the contract, never a current revision/value.
        let rev = row_revision_precondition_failed_status();
        assert!(rev.message().contains("revision precondition failed"));
        assert!(
            !rev.message()
                .to_ascii_lowercase()
                .contains("current revision is")
        );
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

    // G-2: conditional delete must refuse to run unless every PK column is pinned
    // by equality — otherwise the CAS cannot target one row deterministically.
    #[test]
    fn pk_equality_values_from_filter_requires_equality_on_every_pk() {
        let pk = vec!["id".to_string(), "tenant_id".to_string()];

        // Bare equality on both PK columns → extracted in column order.
        let ok = serde_json::json!({"id": "r1", "tenant_id": "t1", "extra": 9});
        let vals = pk_equality_values_from_filter(&ok, &pk).expect("bare equality ok");
        assert_eq!(vals, vec![serde_json::json!("r1"), serde_json::json!("t1")]);

        // {"$eq": v} form is accepted.
        let eqform = serde_json::json!({"id": {"$eq": "r1"}, "tenant_id": "t1"});
        assert!(pk_equality_values_from_filter(&eqform, &pk).is_ok());

        // A missing PK column is refused.
        let missing = serde_json::json!({"id": "r1"});
        assert_eq!(
            pk_equality_values_from_filter(&missing, &pk)
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );

        // A non-equality operator on a PK column is refused (could match many rows).
        let range = serde_json::json!({"id": {"$gt": "r0"}, "tenant_id": "t1"});
        assert_eq!(
            pk_equality_values_from_filter(&range, &pk)
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[test]
    fn idempotency_claim_sql_uses_blocking_do_update_returning_one_row() {
        // F-2: the claim must ALWAYS return exactly one row and block on a
        // concurrent uncommitted inserter, so a concurrent duplicate replays the
        // winner's response instead of erroring INTERNAL. `DO UPDATE` (not
        // `DO NOTHING`) takes the row lock; `xmax = 0` flags a fresh insert.
        let sql = idempotency_claim_sql("udb_idempotency_keys");
        assert!(sql.contains("ON CONFLICT (dedup_key) DO UPDATE"));
        assert!(sql.contains("RETURNING (xmax = 0) AS inserted, response_json"));
        // A self-touch that never overwrites the winner's stored body.
        assert!(sql.contains("SET response_json = idem.response_json"));
        // The old racy form must be gone.
        assert!(!sql.contains("DO NOTHING"));
        assert!(!sql.contains("NOT EXISTS (SELECT 1 FROM ins)"));
        // The scope guard is preserved for a genuine cross-scope key collision.
        assert!(sql.contains("WHERE idem.tenant_id = $2"));
        // #6: the claim writes the request hash on the fresh INSERT ($6) and
        // RETURNS it so a replay can be told apart from a same-key conflict.
        assert!(sql.contains(
            "(dedup_key, tenant_id, project_id, message_type, operation, request_hash, response_json)"
        ));
        assert!(sql.contains("VALUES ($1, $2, $3, $4, $5, $6, '{}'::jsonb)"));
        assert!(sql.contains("RETURNING (xmax = 0) AS inserted, response_json, request_hash"));
        // The DO UPDATE must NEVER overwrite the winner's stored request_hash.
        assert!(!sql.contains("SET request_hash"));
    }

    #[test]
    fn idempotency_request_hash_is_input_sensitive_and_canonical() {
        // #6: the same key reused with DIFFERENT authoritative inputs must hash
        // differently; the same inputs (regardless of JSON key order) must hash
        // identically; and transport-only fields must not participate.
        let record = serde_json::json!({"id": "r1", "amount": 10});
        let record_reordered = serde_json::json!({"amount": 10, "id": "r1"});
        let mut request = UpsertRequest {
            message_type: "Payment".to_string(),
            idempotency_key: "key-1".to_string(),
            conflict_fields: vec!["id".to_string()],
            ..Default::default()
        };
        let base = idempotency_request_hash_upsert(&request, &record);
        // Key order does not matter (canonicalized).
        assert_eq!(
            base,
            idempotency_request_hash_upsert(&request, &record_reordered)
        );
        // A different record rotates the hash.
        assert_ne!(
            base,
            idempotency_request_hash_upsert(
                &request,
                &serde_json::json!({"id": "r1", "amount": 11})
            )
        );
        // A different conflict target rotates the hash.
        request.conflict_fields = vec!["id".to_string(), "tenant".to_string()];
        assert_ne!(base, idempotency_request_hash_upsert(&request, &record));
        request.conflict_fields = vec!["id".to_string()];
        // Operations are namespaced: an identical filter under delete vs update
        // must not collide.
        let filter = serde_json::json!({"id": {"$eq": "r1"}});
        assert_ne!(
            idempotency_request_hash_delete(&filter, None),
            idempotency_request_hash_update(&filter, &serde_json::json!({}), &[], None)
        );
        // Update increments and changes participate.
        let u_base = idempotency_request_hash_update(
            &filter,
            &serde_json::json!({"status": "paid"}),
            &[("balance".to_string(), 5.0)],
            None,
        );
        assert_ne!(
            u_base,
            idempotency_request_hash_update(
                &filter,
                &serde_json::json!({"status": "paid"}),
                &[("balance".to_string(), 6.0)],
                None,
            )
        );
        assert_ne!(
            u_base,
            idempotency_request_hash_update(
                &filter,
                &serde_json::json!({"status": "void"}),
                &[("balance".to_string(), 5.0)],
                None,
            )
        );
    }

    #[test]
    fn idempotency_request_mismatch_status_is_non_disclosing_precondition() {
        let status = idempotency_request_mismatch_status();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        // Must name the contract violation, not leak any stored/foreign state.
        assert!(status.message().contains("idempotency_key"));
        assert!(status.message().contains("different request"));
        let detail = decode_error_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert!(!detail.retryable);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "idempotency_key");
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

/// Translate the neutral qdrant-style filter (`{"must":[{"key":…,"match":{"value":…}}]}`)
/// into Elasticsearch `term` clauses over the stored `payload.<key>` field.
///
/// The tenant scope (`_tenant_id`) is the SECURITY boundary: previously the ES
/// arm ran `match_all` and ignored `request.filter` entirely, returning other
/// tenants' documents. Each equality `must` term becomes a `term` on
/// `payload.<key>.keyword` — the `.keyword` sub-field is used so exact-match
/// works under Elasticsearch's default dynamic string mapping (a bare `term` on
/// an analyzed `text` field would tokenise a UUID and mis-match). Non-equality
/// operators are not translated (they would only ever broaden results WITHIN the
/// tenant, never cross it). Docs are tenant-stamped at write time by
/// [`stamp_generic_vector_point_payloads`], so `payload._tenant_id` exists to
/// match against.
fn es_payload_filter_terms(filter: Option<&prost_types::Struct>) -> Vec<JsonValue> {
    let mut clauses: Vec<JsonValue> = struct_filter_equality_terms(filter)
        .into_iter()
        .map(|(key, value)| {
            let mut term_field = serde_json::Map::new();
            term_field.insert(format!("payload.{key}.keyword"), value);
            serde_json::json!({ "term": JsonValue::Object(term_field) })
        })
        .collect();
    // `match.any` (the parent-window gather scoping `_parent_pk` to the selected
    // parents) becomes an ES `terms` clause so the OR-set still narrows the query
    // WITHIN the tenant — previously it was dropped and the gather ran unscoped.
    for (key, values) in struct_filter_any_terms(filter) {
        let mut terms_field = serde_json::Map::new();
        terms_field.insert(format!("payload.{key}.keyword"), JsonValue::Array(values));
        clauses.push(serde_json::json!({ "terms": JsonValue::Object(terms_field) }));
    }
    clauses
}

/// Extract equality `(key, value)` pairs from the neutral qdrant-style filter
/// (`{"must":[{"key":…,"match":{"value":…}}]}`). The tenant scope (`_tenant_id`)
/// is the security boundary each generic-HTTP backend must AND into its query;
/// non-equality operators are not translated (they only broaden WITHIN a tenant).
/// Docs are tenant-stamped at write time by [`stamp_generic_vector_point_payloads`].
fn struct_filter_equality_terms(filter: Option<&prost_types::Struct>) -> Vec<(String, JsonValue)> {
    let Some(filter) = filter else {
        return Vec::new();
    };
    let json = struct_to_json(filter);
    let Some(must) = json.get("must").and_then(JsonValue::as_array) else {
        return Vec::new();
    };
    let mut terms = Vec::new();
    for clause in must {
        if let (Some(key), Some(value)) = (
            clause.get("key").and_then(JsonValue::as_str),
            clause.get("match").and_then(|m| m.get("value")),
        ) {
            terms.push((key.to_string(), value.clone()));
        }
    }
    terms
}

/// Extract `match.any` OR-set `(key, [values…])` clauses from the neutral
/// qdrant-style filter (`{"must":[{"key":…,"match":{"any":[…]}}]}`). The
/// parent-window neighbor gather scopes `_parent_pk` this way (match ANY of the
/// selected parents in ONE query); [`struct_filter_equality_terms`] only reads
/// `match.value`, so without this the `any` clause was silently dropped and the
/// gather ran unscoped over the whole collection. Each backend translator ANDs
/// these in as its native OR/terms/`$in` primitive. Empty `any` lists are skipped.
fn struct_filter_any_terms(filter: Option<&prost_types::Struct>) -> Vec<(String, Vec<JsonValue>)> {
    let Some(filter) = filter else {
        return Vec::new();
    };
    let json = struct_to_json(filter);
    let Some(must) = json.get("must").and_then(JsonValue::as_array) else {
        return Vec::new();
    };
    let mut terms = Vec::new();
    for clause in must {
        if let (Some(key), Some(any)) = (
            clause.get("key").and_then(JsonValue::as_str),
            clause
                .get("match")
                .and_then(|m| m.get("any"))
                .and_then(JsonValue::as_array),
        ) {
            if !any.is_empty() {
                terms.push((key.to_string(), any.clone()));
            }
        }
    }
    terms
}

/// Build a Weaviate GraphQL `where:` argument (with a trailing comma so it slots
/// before `limit:`) enforcing the tenant/equality terms. Weaviate stores the
/// stamped payload as top-level properties, so the path is the bare key. Empty
/// terms → empty string (unchanged behaviour). Values are emitted as `valueText`.
fn weaviate_where_arg(filter: Option<&prost_types::Struct>) -> String {
    let equality = struct_filter_equality_terms(filter);
    let any = struct_filter_any_terms(filter);
    let operand = |key: &str, value: &JsonValue| -> String {
        let text = value
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| value.to_string());
        format!("{{ path: [{key:?}], operator: Equal, valueText: {text:?} }}")
    };
    let mut operands: Vec<String> = equality
        .iter()
        .map(|(key, value)| operand(key, value))
        .collect();
    // `match.any` → an OR of Equal operands on the same path, so the parent-window
    // gather (or any multi-value clause) stays scoped instead of being dropped.
    for (key, values) in &any {
        let inner = values
            .iter()
            .map(|value| operand(key, value))
            .collect::<Vec<_>>()
            .join(", ");
        operands.push(format!("{{ operator: Or, operands: [{inner}] }}"));
    }
    match operands.as_slice() {
        [] => String::new(),
        [single] => format!("where: {single}, "),
        many => {
            let joined = many.join(", ");
            format!("where: {{ operator: And, operands: [{joined}] }}, ")
        }
    }
}

/// Build a Pinecone metadata `filter` object (`{"_tenant_id": {"$eq": …}, …}`)
/// enforcing the tenant/equality terms over the stamped metadata. Empty terms →
/// `null` (the caller omits the field, unchanged behaviour).
fn pinecone_metadata_filter(filter: Option<&prost_types::Struct>) -> JsonValue {
    let equality = struct_filter_equality_terms(filter);
    let any = struct_filter_any_terms(filter);
    if equality.is_empty() && any.is_empty() {
        return JsonValue::Null;
    }
    let mut map = serde_json::Map::new();
    for (key, value) in equality {
        let mut eq = serde_json::Map::new();
        eq.insert("$eq".to_string(), value);
        map.insert(key, JsonValue::Object(eq));
    }
    // `match.any` → Pinecone's `$in` set membership, keeping the parent-window
    // gather scoped instead of dropping the clause (unscoped cross-parent read).
    for (key, values) in any {
        let mut in_op = serde_json::Map::new();
        in_op.insert("$in".to_string(), JsonValue::Array(values));
        map.insert(key, JsonValue::Object(in_op));
    }
    JsonValue::Object(map)
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
                    // Wrap the vector similarity in a bool so the tenant-scope
                    // (and any equality) filter is AND'd in server-side. Without
                    // the `filter` clause this arm leaked across tenants.
                    "bool": {
                        "must": [{
                            "script_score": {
                                "query": { "match_all": {} },
                                "script": {
                                    "source": "cosineSimilarity(params.query_vector, 'vector') + 1.0",
                                    "params": { "query_vector": request.vector }
                                }
                            }
                        }],
                        "filter": es_payload_filter_terms(request.filter.as_ref())
                    }
                }
            }
        }),
        "weaviate" => {
            let class_name = vector_weaviate_class_name(&request.collection);
            // Tenant scope (and any equality term) AND'd into the GraphQL `where`;
            // previously the arm ignored request.filter (cross-tenant leak class).
            let where_arg = weaviate_where_arg(request.filter.as_ref());
            serde_json::json!({
                "method": "POST",
                "path": "/v1/graphql",
                "body": {
                    // Weaviate GraphQL requires every returned property be named
                    // explicitly (no wildcard). `_tenant_id`/`_project_id` are the
                    // isolation keys the class schema always declares, so selecting
                    // them is safe for both the embedding and the generic vector
                    // path; the parser lifts them into the point payload and the
                    // caller-facing strip removes them again.
                    // TODO(leader-wire): to also surface embedding provenance
                    // (`_parent_pk`/`_chunk_seq`/`_chunk_text`/…) on weaviate reads,
                    // declare those properties in the weaviate class schema
                    // (executors/weaviate.rs `ensure_resource`) and add them to this
                    // selection — selecting undeclared props errors the whole query.
                    "query": format!(
                        "{{ Get {{ {class_name}(nearVector: {{ vector: {:?} }}, {where_arg}limit: {limit}) {{ _tenant_id _project_id _additional {{ id distance certainty }} }} }} }}",
                        request.vector
                    )
                }
            })
        }
        "pinecone" => {
            let mut body = serde_json::Map::new();
            body.insert("vector".to_string(), serde_json::json!(request.vector));
            body.insert("topK".to_string(), serde_json::json!(limit));
            body.insert(
                "includeMetadata".to_string(),
                serde_json::json!(request.with_payload),
            );
            // Tenant scope AND'd into the Pinecone metadata `filter`; previously the
            // arm ignored request.filter (cross-tenant leak class).
            let metadata_filter = pinecone_metadata_filter(request.filter.as_ref());
            if !metadata_filter.is_null() {
                body.insert("filter".to_string(), metadata_filter);
            }
            serde_json::json!({
                "method": "POST",
                "path": "/query",
                "body": JsonValue::Object(body)
            })
        }
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

/// Build a mediated FULL-TEXT-ONLY (lexical, no query vector) search spec for
/// `SEARCH_MODE_TEXT`. Mirrors [`vector_search_dispatch_spec`] but scores by BM25
/// relevance instead of vector similarity. The tenant scope carried in
/// `request.filter` is the SECURITY boundary and is AND'd into the generated query
/// via [`es_payload_filter_terms`] (identical translation to the vector arm), so a
/// caller can never widen past their tenant. Only Elasticsearch is wired: a
/// `multi_match` (`best_fields`) over the stamped `payload.*` text — the same
/// payload the vector upsert stores and stamps `_tenant_id` into — wrapped in
/// `bool.must` with the tenant `filter`. Qdrant text-only needs a payload
/// full-text field index that may be absent, so it (and every other backend)
/// fails closed with a typed capability error rather than returning a silent
/// empty/degraded result. `query_text` is threaded separately because
/// `VectorSearchRequest` carries no text field.
fn text_search_dispatch_spec(
    backend: &str,
    request: &VectorSearchRequest,
    query_text: &str,
) -> Result<String, tonic::Status> {
    let limit = if request.limit > 0 { request.limit } else { 10 };
    let spec = match backend {
        "elasticsearch" => serde_json::json!({
            "method": "POST",
            "path": format!("/{}/_search", request.collection.to_ascii_lowercase()),
            "body": {
                "size": limit,
                "query": {
                    // BM25 lexical relevance wrapped in a bool so the tenant-scope
                    // (and any equality) filter is AND'd in server-side — the same
                    // isolation boundary the vector arm enforces.
                    "bool": {
                        "must": [{
                            "multi_match": {
                                "query": query_text,
                                "fields": ["payload.*"],
                                "type": "best_fields"
                            }
                        }],
                        "filter": es_payload_filter_terms(request.filter.as_ref())
                    }
                }
            }
        }),
        other => {
            return Err(setup_data_capability_status(
                other,
                "typed_full_text_search",
                "typed_full_text_search_backend",
                format!("typed full-text search is not wired for backend '{other}'"),
            ));
        }
    };
    serde_json::to_string(&spec)
        .map_err(|err| setup_data_internal_status("text_search_spec_encode", err.to_string()))
}

/// Return a clone of `request` with `_tenant_id`/`_project_id` stamped onto every
/// point payload, mirroring the Qdrant executor's write-time tenant stamp so the
/// generic HTTP backends (Elasticsearch/Weaviate/Pinecone) store a tenant tag the
/// search filter can enforce isolation against. Empty tenant/project are skipped
/// (never stamped as blank). A missing/non-object payload becomes a fresh object
/// carrying only the tags.
#[cfg(feature = "qdrant")]
/// AND the active tenant/project into a Qdrant filter body so a direct
/// `vector_search` cannot read another tenant's points from a shared collection
/// (C7). Preserves any caller filter as a nested condition and matches the
/// `_tenant_id`/`_project_id` keys written by
/// [`stamp_generic_vector_point_payloads`], so writes and reads agree. Returns
/// `Null` when there is nothing to scope by.
#[cfg(feature = "qdrant")]
fn qdrant_and_tenant_filter(
    user_filter: JsonValue,
    tenant_id: &str,
    project_id: &str,
) -> JsonValue {
    let mut must: Vec<JsonValue> = Vec::new();
    // Preserve the caller filter as a nested filter condition (Qdrant allows a
    // full filter object inside `must`).
    if user_filter.as_object().is_some_and(|m| !m.is_empty()) {
        must.push(user_filter);
    }
    if !tenant_id.trim().is_empty() {
        must.push(serde_json::json!({"key": "_tenant_id", "match": {"value": tenant_id}}));
    }
    if !project_id.trim().is_empty() {
        must.push(serde_json::json!({"key": "_project_id", "match": {"value": project_id}}));
    }
    if must.is_empty() {
        return JsonValue::Null;
    }
    serde_json::json!({ "must": must })
}

/// F2: merge the CONTEXT tenant/project into the neutral qdrant-style filter as
/// FLAT `must` equality clauses so the per-backend translators
/// (`es_payload_filter_terms`/`weaviate_where_arg`/`pinecone_metadata_filter`)
/// emit the `_tenant_id`/`_project_id` predicate that pairs with the write-side
/// `stamp_generic_vector_point_payloads` stamp. Unlike `qdrant_and_tenant_filter`
/// (which NESTS the caller filter for Qdrant's native nested-filter support), this
/// keeps clauses flat because the generic translators only read top-level
/// `{key, match}` clauses. No-context (both empty, no caller filter) → `{"must":[]}`
/// → translators emit zero clauses = identical to today (no regression).
#[cfg(feature = "qdrant")]
fn scoped_generic_vector_filter(
    mut user_filter: JsonValue,
    tenant_id: &str,
    project_id: &str,
) -> Option<prost_types::Struct> {
    if !user_filter.is_object() {
        user_filter = JsonValue::Object(serde_json::Map::new());
    }
    if let Some(obj) = user_filter.as_object_mut() {
        let must = obj
            .entry("must".to_string())
            .or_insert_with(|| JsonValue::Array(Vec::new()));
        if let Some(arr) = must.as_array_mut() {
            if !tenant_id.trim().is_empty() {
                arr.push(serde_json::json!({"key": "_tenant_id", "match": {"value": tenant_id}}));
            }
            if !project_id.trim().is_empty() {
                arr.push(serde_json::json!({"key": "_project_id", "match": {"value": project_id}}));
            }
        }
    }
    crate::runtime::executor_utils::json_to_struct(&user_filter)
}

#[cfg(all(test, feature = "qdrant"))]
mod qdrant_tenant_filter_tests {
    use super::qdrant_and_tenant_filter;
    use serde_json::json;

    #[test]
    fn ands_tenant_and_project_into_must() {
        let f = qdrant_and_tenant_filter(json!({}), "t1", "p1");
        let must = f["must"].as_array().expect("must array");
        assert!(
            must.iter()
                .any(|c| c["key"] == "_tenant_id" && c["match"]["value"] == "t1"),
            "tenant clause missing: {f}"
        );
        assert!(
            must.iter()
                .any(|c| c["key"] == "_project_id" && c["match"]["value"] == "p1")
        );
    }

    #[test]
    fn preserves_user_filter_as_nested_condition() {
        let user = json!({"must":[{"key":"kind","match":{"value":"doc"}}]});
        let f = qdrant_and_tenant_filter(user.clone(), "t1", "");
        let must = f["must"].as_array().unwrap();
        assert_eq!(must[0], user, "user filter must be preserved");
        assert!(must.iter().any(|c| c["key"] == "_tenant_id"));
    }

    #[test]
    fn empty_context_returns_null() {
        assert!(qdrant_and_tenant_filter(json!({}), "", "").is_null());
    }
}

fn stamp_generic_vector_point_payloads(
    request: &VectorUpsertRequest,
    tenant_id: &str,
    project_id: &str,
) -> VectorUpsertRequest {
    let mut stamped = request.clone();
    for point in &mut stamped.points {
        let mut payload = point
            .payload
            .as_ref()
            .map(struct_to_json)
            .unwrap_or_else(|| JsonValue::Object(serde_json::Map::new()));
        if !payload.is_object() {
            payload = JsonValue::Object(serde_json::Map::new());
        }
        if let Some(object) = payload.as_object_mut() {
            if !tenant_id.trim().is_empty() {
                object.insert(
                    "_tenant_id".to_string(),
                    JsonValue::String(tenant_id.to_string()),
                );
            }
            if !project_id.trim().is_empty() {
                object.insert(
                    "_project_id".to_string(),
                    JsonValue::String(project_id.to_string()),
                );
            }
        }
        point.payload = crate::runtime::executor_utils::json_to_struct(&payload);
    }
    stamped
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

fn parse_vector_search_response(
    backend: &str,
    raw: &str,
    es_cosine_offset: bool,
) -> Result<VectorSet, tonic::Status> {
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
                        // The vector arm scores `cosineSimilarity(...) + 1.0`
                        // (range ~0..2) so ES never returns negative scores; strip
                        // the +1.0 offset back to cosine (−1..1) so the score is
                        // comparable to the retrieval cosine floor and the other
                        // backends. The text/BM25 arm passes `es_cosine_offset =
                        // false` (its `_score` is BM25 relevance, not offset cosine).
                        score: hit
                            .get("_score")
                            .and_then(JsonValue::as_f64)
                            .map(|raw| (if es_cosine_offset { raw - 1.0 } else { raw }) as f32)
                            .unwrap_or(0.0),
                        // Upserts store `{ "vector":[…], "payload":{…stamped…} }`, so
                        // the stamped provenance/tenant keys live UNDER `_source.
                        // payload` (not at `_source` top level). Lift that nested
                        // object as the point payload so provenance reads and the
                        // caller-facing tenant strip work uniformly with qdrant — and
                        // the sibling raw dense `vector` is left behind, never
                        // returned to callers.
                        payload: hit
                            .pointer("/_source/payload")
                            .cloned()
                            .and_then(json_into_struct),
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
        "weaviate" => parsed
            .pointer("/data/Get")
            .and_then(JsonValue::as_object)
            // The GraphQL `Get` object carries exactly one key — the queried class —
            // whose value is the hit array. Take that single class array without
            // needing to re-derive the (request-side) class name here.
            .and_then(|get| get.values().next())
            .and_then(JsonValue::as_array)
            .map(|objects| {
                objects
                    .iter()
                    .map(|object| {
                        let additional = object.get("_additional");
                        VectorPoint {
                            id: additional
                                .and_then(|meta| meta.get("id"))
                                .and_then(JsonValue::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            score: additional.map(weaviate_cosine_score).unwrap_or(0.0),
                            // Weaviate stores the stamped payload as top-level object
                            // properties; lift every returned property EXCEPT the
                            // reserved `_additional` metadata into the point payload,
                            // matching the qdrant point shape (tenant strip applies
                            // uniformly). The query never selects the dense vector, so
                            // it is not returned to callers.
                            payload: weaviate_point_payload(object),
                            vector: Vec::new(),
                            vector_name: String::new(),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    };
    Ok(VectorSet { points })
}

/// Convert Weaviate's `_additional` distance/certainty into a COSINE similarity so
/// the score is comparable to the retrieval cosine floor and the other backends.
/// Weaviate cosine `distance` = 1 − cosine_similarity (∈ 0..2), and `certainty` =
/// (2 − distance)/2 (∈ 0..1) ⇒ cosine_similarity = 2·certainty − 1. Prefer the
/// exact `distance`, fall back to `certainty`, else 0.0.
fn weaviate_cosine_score(additional: &JsonValue) -> f32 {
    if let Some(distance) = additional.get("distance").and_then(JsonValue::as_f64) {
        (1.0 - distance) as f32
    } else if let Some(certainty) = additional.get("certainty").and_then(JsonValue::as_f64) {
        (2.0 * certainty - 1.0) as f32
    } else {
        0.0
    }
}

/// Lift a Weaviate GraphQL hit object's stored properties (all keys except the
/// reserved `_additional` metadata block) into a point payload `Struct`, so a
/// weaviate hit carries the same stamped payload shape a qdrant hit does. An
/// object with no properties beyond `_additional` yields `None`.
fn weaviate_point_payload(object: &JsonValue) -> Option<prost_types::Struct> {
    let properties = object.as_object()?;
    let mut payload = serde_json::Map::new();
    for (key, value) in properties {
        if key != "_additional" {
            payload.insert(key.clone(), value.clone());
        }
    }
    if payload.is_empty() {
        return None;
    }
    json_into_struct(JsonValue::Object(payload))
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

/// Stamp an object PUT request spec with the server-side-encryption requirement
/// so the backend executor applies it on write. Kept separate from
/// `object_request_json` so the many non-PUT / non-SSE call sites are untouched;
/// only the object PUT path (which resolves `ObjectStreamPlan`) opts in, gated
/// on `requires_server_side_encryption`. A parse failure returns the input
/// unchanged rather than dropping the request.
pub(crate) fn object_request_json_require_sse(request_json: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(request_json) {
        Ok(mut value) => {
            value["server_side_encryption"] = serde_json::Value::Bool(true);
            value.to_string()
        }
        Err(_) => request_json.to_string(),
    }
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
