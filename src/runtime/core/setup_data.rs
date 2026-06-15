//! Continuation `impl DataBrokerRuntime` block (Phase F split of core.rs).
//!
//! Also home to the per-backend `register_*` functions invoked by
//! `backend::plugins::*` via the U2 plugin loop. These live here (not in
//! `backend/`) so they have descendant-module access to `DataBrokerRuntime`'s
//! private `pg_*`/`redis`/`qdrant_*`/etc. fields (§9.5).
use super::*;
use crate::backend::plugin::RegisterCtx;

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

    pub async fn load_abac_policies(&self) -> Vec<AbacPolicy> {
        // Backward-compatible wrapper: a transient query error degrades to an empty
        // set for callers that can't act on it (initial load), but the error is now
        // LOGGED, not silently swallowed. bug_report.md J.
        match self.try_load_abac_policies().await {
            Ok(policies) => policies,
            Err(err) => {
                tracing::error!(error = %err, "ABAC policy load failed; using empty set");
                Vec::new()
            }
        }
    }

    /// Load ABAC policies, surfacing a DB query failure as `Err` so the periodic
    /// refresh retains stale policies on a transient error instead of mistaking it
    /// for a genuine empty set — the authz-snapshot flapping under CDC pool
    /// contention (`ABAC policy refresh returned empty set`). bug_report.md J.
    pub async fn try_load_abac_policies(&self) -> Result<Vec<AbacPolicy>, String> {
        if let Some(raw) = self.config.abac_policies_json.as_ref() {
            match serde_json::from_str::<Vec<AbacPolicy>>(raw) {
                Ok(policies) => return Ok(policies),
                Err(err) => tracing::warn!("failed to parse UDB_ABAC_POLICIES_JSON: {err}"),
            }
        }
        let Some(pool) = &self.pg_pool else {
            return Ok(Vec::new());
        };
        let abac_schema = if self.config.abac_schema.trim().is_empty() {
            "udb_system"
        } else {
            self.config.abac_schema.as_str()
        };
        let abac_table_name = if self.config.abac_table.trim().is_empty() {
            "udb_abac_policies"
        } else {
            self.config.abac_table.as_str()
        };
        let abac_table = format!(
            "{}.{}",
            qi_runtime(abac_schema),
            qi_runtime(abac_table_name)
        );
        let sql = format!(
            "SELECT effect, service_identity, tenant_id, purpose, message_type, operation, required_scope
             FROM {abac_table}
             WHERE enabled = TRUE
             ORDER BY priority DESC, policy_id ASC"
        );
        let rows = sqlx::query(&sql)
            .fetch_all(pool)
            .await
            .map_err(|e| format!("ABAC policy query failed: {e}"))?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let effect = row
                    .try_get::<String, _>("effect")
                    .unwrap_or_else(|_| "allow".to_string());
                AbacPolicy {
                    effect: if effect.eq_ignore_ascii_case("deny") {
                        PolicyEffect::Deny
                    } else {
                        PolicyEffect::Allow
                    },
                    service_identity: row
                        .try_get("service_identity")
                        .unwrap_or_else(|_| "*".to_string()),
                    tenant_id: row.try_get("tenant_id").unwrap_or_else(|_| "*".to_string()),
                    purpose: row.try_get("purpose").unwrap_or_else(|_| "*".to_string()),
                    message_type: row
                        .try_get("message_type")
                        .unwrap_or_else(|_| "*".to_string()),
                    operation: row.try_get("operation").unwrap_or_else(|_| "*".to_string()),
                    required_scope: row.try_get("required_scope").unwrap_or_default(),
                }
            })
            .collect())
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
                if std::panic::AssertUnwindSafe(plugin.register(&mut ctx))
                    .catch_unwind()
                    .await
                    .is_err()
                {
                    ctx.report.warnings.push(format!(
                        "backend {kind:?} registration panicked at startup; backend marked \
                         unavailable (broker continues)"
                    ));
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

        // S1 (luna): all store registration is done; record whether a FULL
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
    ) -> Result<RecordSet, tonic::Status> {
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
            return Ok(cached_record_set(cached));
        }

        let table = table_for_message(manifest, &request.message_type)
            .ok_or_else(|| tonic::Status::invalid_argument("unknown message_type"))?;
        let pool = self.pg_select_pool_for_table(table, &context)?;
        self.enforce_read_fence(
            &context,
            &pool,
            "postgres",
            if context.target_instance.trim().is_empty() {
                "selected"
            } else {
                context.target_instance.trim()
            },
        )
        .await?;
        // READ fast-path: a read-only SELECT does NOT need a transaction. We
        // acquire ONE pooled connection, install the RLS context as SESSION
        // settings (is_local=false) on it, run the SELECT on that SAME
        // connection, then ALWAYS reset those session GUCs before the
        // connection returns to the pool — on BOTH the success and error path.
        // This drops the BEGIN+COMMIT round-trips while keeping RLS isolation
        // byte-identical (same keys/values as the write path).
        let mut conn = pool
            .acquire()
            .await
            .map_err(|e| tonic::Status::internal(format!("PG connection acquire failed: {e}")))?;
        set_request_local_settings_conn(&mut conn, &context).await?;
        let values = filter_bind_values(&filter);
        let query = bind_values(
            sqlx::query(&plan.sql),
            table,
            &plan.parameter_columns,
            &values,
        )?;
        // Capture the SELECT result WITHOUT early-`?`-returning, so the reset
        // below runs unconditionally even on query failure (leak-safety).
        let rows_result = query
            .fetch_all(&mut *conn)
            .await
            .map_err(|err| tonic::Status::internal(format!("PostgreSQL select failed: {err}")));
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
        Ok(record_set)
    }

    pub(crate) async fn select_join_fusion(
        &self,
        manifest: &CatalogManifest,
        request: SelectRequest,
        context: RequestContext,
        filter: JsonValue,
    ) -> Result<RecordSet, tonic::Status> {
        let plan = build_join_fusion_sql(manifest, &request, &context, &filter)?;
        let mut query = sqlx::query(&plan.sql);
        for (column, value) in &plan.bindings {
            query = bind_one(query, Some(column), value)?;
        }
        let pool = self
            .pg_read_pool_for_context(&context)
            .ok_or_else(|| tonic::Status::unavailable("PostgreSQL backend is not configured"))?;
        self.enforce_read_fence(
            &context,
            &pool,
            "postgres",
            if context.target_instance.trim().is_empty() {
                "selected"
            } else {
                context.target_instance.trim()
            },
        )
        .await?;
        // READ fast-path (see `select`): no transaction. Acquire one pooled
        // connection, install RLS context as SESSION settings, run the join
        // SELECT, then ALWAYS reset the session GUCs before the connection
        // returns to the pool — on success AND error.
        let mut conn = pool
            .acquire()
            .await
            .map_err(|e| tonic::Status::internal(format!("PG connection acquire failed: {e}")))?;
        set_request_local_settings_conn(&mut conn, &context).await?;
        // Capture the SELECT result WITHOUT early-`?`-returning so the reset
        // runs unconditionally even on query failure (leak-safety).
        let rows_result = query.fetch_all(&mut *conn).await.map_err(|err| {
            tonic::Status::internal(format!("PostgreSQL join select failed: {err}"))
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
        rows_to_record_set(
            rows,
            None,
            &[],
            &context,
            self.encryption.as_ref(),
            &self.encryption_metrics,
        )
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
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| tonic::Status::internal(format!("PG transaction begin failed: {e}")))?;
        set_request_local_settings(&mut tx, &context).await?;
        let table = table_for_message(manifest, &request.message_type)
            .ok_or_else(|| tonic::Status::invalid_argument("unknown message_type"))?;
        // #117: rewrite proto `field_name` record keys to physical `column_name`s
        // so encryption + binding (keyed by `plan.parameter_columns`, which the
        // planner already resolved) find each value.
        let record = crate::broker::normalize_record_keys(table, &record);
        let encrypted_record = self.encrypt_record_for_table(table, &record)?;
        let values = record_values(&encrypted_record, &plan.parameter_columns)?;
        let query = bind_values(
            sqlx::query(&plan.sql),
            table,
            &plan.parameter_columns,
            &values,
        )?;

        let (affected_rows, record_json) = if request.return_record {
            let row = query.fetch_optional(&mut *tx).await.map_err(|err| {
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
                    (
                        1,
                        record_set.records_json.first().cloned().unwrap_or_default(),
                    )
                }
                None => (0, Vec::new()),
            }
        } else {
            let result = query.execute(&mut *tx).await.map_err(|err| {
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
                    tonic::Status::internal(format!("projection task enqueue failed: {err}"))
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
        tx.commit().await.map_err(|err| {
            tonic::Status::internal(format!("PostgreSQL upsert commit failed: {err}"))
        })?;

        let _ = self
            .cache_delete_pattern(&cache_invalidation_pattern("select", &request.message_type))
            .await;
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
        Ok(MutationResponse {
            mutation_id: Uuid::new_v4().to_string(),
            resource_uri: plan.resource_uri,
            checksum_sha256: checksum_json(&record),
            record_json,
            affected_rows,
            was_duplicate: false,
            write_receipt_json: serde_json::to_string(&receipt).unwrap_or_default(),
            ..MutationResponse::default()
        })
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
        let Some(table) = table_for_message(manifest, message_type) else {
            return Ok(());
        };
        let topic = table.cdc_topic.trim();
        if topic.is_empty() {
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
        .map_err(|err| tonic::Status::internal(format!("CDC outbox emit failed: {err}")))
    }

    pub async fn delete(
        &self,
        manifest: &CatalogManifest,
        message_type: &str,
        filter: JsonValue,
        context: RequestContext,
    ) -> Result<MutationResponse, tonic::Status> {
        let plan = build_delete_plan(
            manifest,
            &DeletePlanRequest {
                context: context.clone(),
                message_type: message_type.to_string(),
                filter: filter.clone(),
            },
        );
        reject_plan(&plan.errors)?;
        let pool = self.pg_pool()?;
        let table = table_for_message(manifest, message_type)
            .ok_or_else(|| tonic::Status::invalid_argument("unknown message_type"))?;
        let values = filter_bind_values(&filter);
        let query = bind_values(
            sqlx::query(&plan.sql),
            table,
            &plan.parameter_columns,
            &values,
        )?;
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| tonic::Status::internal(format!("PG transaction begin failed: {e}")))?;
        set_request_local_settings(&mut tx, &context).await?;
        let result = query
            .execute(&mut *tx)
            .await
            .map_err(|err| tonic::Status::internal(format!("PostgreSQL delete failed: {err}")))?;
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
                    tonic::Status::internal(format!("projection task enqueue failed: {err}"))
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
        tx.commit().await.map_err(|err| {
            tonic::Status::internal(format!("PostgreSQL delete commit failed: {err}"))
        })?;
        let _ = self
            .cache_delete_pattern(&cache_invalidation_pattern("select", message_type))
            .await;
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
        Ok(MutationResponse {
            mutation_id: Uuid::new_v4().to_string(),
            resource_uri: plan.resource_uri,
            affected_rows: result.rows_affected() as i64,
            write_receipt_json: serde_json::to_string(&receipt).unwrap_or_default(),
            ..MutationResponse::default()
        })
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
            return Err(tonic::Status::failed_precondition(
                "qdrant/vector feature is not enabled",
            ));
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
            return Err(tonic::Status::failed_precondition(
                "qdrant/vector feature is not enabled",
            ));
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
                return Err(tonic::Status::failed_precondition(format!(
                    "vector hybrid search is only wired for qdrant, not '{}'",
                    plan.backend
                )));
            }
            let target_instance = if context.target_instance.trim().is_empty() {
                self.choose_instance_name_for_project("qdrant", false, &context.project_id)
            } else {
                Some(context.target_instance.as_str())
            };

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
            return Err(tonic::Status::failed_precondition(
                "qdrant/vector feature is not enabled",
            ));
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
            return Err(tonic::Status::failed_precondition(
                "no object-store feature (s3/gcs/azureblob) is enabled",
            ));
        }
        #[cfg(any(feature = "s3", feature = "gcs", feature = "azureblob"))]
        {
            // A.6: pull only the FIRST chunk (it carries bucket/key/content-type/
            // context + the first body slice); the remainder of the gRPC stream is
            // forwarded straight into the backing store without buffering the whole
            // object. Size is bounded cumulatively by `UDB_MAX_OBJECT_BYTES`.
            let first = match stream.next().await {
                Some(chunk) => chunk?,
                None => return Err(tonic::Status::invalid_argument("empty object stream")),
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
                    return Err(tonic::Status::failed_precondition(
                        "s3/minio feature is not enabled",
                    ));
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
                                tonic::Status::failed_precondition(format!(
                                    "gcs instance '{instance}' is not configured"
                                ))
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
                    return Err(tonic::Status::failed_precondition(
                        "gcs feature is not enabled",
                    ));
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
                                tonic::Status::failed_precondition(format!(
                                    "azureblob instance '{instance}' is not configured"
                                ))
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
                    return Err(tonic::Status::failed_precondition(
                        "azureblob feature is not enabled",
                    ));
                }
                other => {
                    return Err(tonic::Status::failed_precondition(format!(
                        "unsupported object backend '{other}'"
                    )));
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
            return Err(tonic::Status::failed_precondition(
                "no object-store feature (s3/gcs/azureblob) is enabled",
            ));
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
                    return Err(tonic::Status::failed_precondition(
                        "s3/minio feature is not enabled",
                    ));
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
                                tonic::Status::failed_precondition(format!(
                                    "gcs instance '{instance}' is not configured"
                                ))
                            })?
                            .clone();
                        let executor = crate::runtime::executors::gcs::GcsExecutor::new(client);
                        let src = executor.get_object_stream(&request_json).await?;
                        byte_stream_to_chunk_stream(src, bucket, object_key, backend.clone())
                    }
                    #[cfg(not(feature = "gcs"))]
                    return Err(tonic::Status::failed_precondition(
                        "gcs feature is not enabled",
                    ));
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
                                tonic::Status::failed_precondition(format!(
                                    "azureblob instance '{instance}' is not configured"
                                ))
                            })?
                            .clone();
                        let executor =
                            crate::runtime::executors::azureblob::AzureBlobExecutor::new(client);
                        let src = executor.get_object_stream(&request_json).await?;
                        byte_stream_to_chunk_stream(src, bucket, object_key, backend.clone())
                    }
                    #[cfg(not(feature = "azureblob"))]
                    return Err(tonic::Status::failed_precondition(
                        "azureblob feature is not enabled",
                    ));
                }
                other => {
                    return Err(tonic::Status::failed_precondition(format!(
                        "unsupported object backend '{other}'"
                    )));
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
            return Err(tonic::Status::failed_precondition(
                "s3/object-store feature is not enabled",
            ));
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
                return Err(tonic::Status::invalid_argument(
                    "presigned URLs support only PUT or GET",
                ));
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
            Err(tonic::Status::failed_precondition(
                "qdrant/vector feature is not enabled",
            ))
        }
        #[cfg(feature = "qdrant")]
        {
            use crate::generation::manifest::{ManifestStore, ManifestStoreOption};
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
            Err(tonic::Status::failed_precondition(
                "qdrant/vector feature is not enabled",
            ))
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
            Err(tonic::Status::failed_precondition(
                "s3/object-store feature is not enabled",
            ))
        }
        #[cfg(feature = "s3")]
        {
            let method = method.to_ascii_uppercase();
            if method != "PUT" && method != "GET" {
                return Err(tonic::Status::invalid_argument(
                    "presigned URLs support only PUT or GET",
                ));
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
    pub async fn object_exists_backend_target(
        &self,
        backend_target: &str,
        project_id: &str,
        bucket: &str,
        object_key: &str,
    ) -> Result<bool, tonic::Status> {
        #[cfg(not(feature = "s3"))]
        {
            let _ = (backend_target, project_id, bucket, object_key);
            Err(tonic::Status::failed_precondition(
                "s3/object-store feature is not enabled",
            ))
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
                Ok(_) => Ok(true),
                Err(err) => {
                    // S3 answers a HEAD for a missing object (or bucket) with a
                    // BODILESS 404, so the SDK error's Display is a generic
                    // "service error" with NO "NotFound"/"NoSuchKey"/"404" text —
                    // string-matching it misclassifies a plain absent object as a
                    // service failure (the FinalizeUpload bug). The NotFound signal
                    // lives ONLY in the typed service error, so classify on that:
                    // a 404 → not present (Ok(false)); anything else (auth, network,
                    // endpoint) → a real failure.
                    let not_found = err
                        .as_service_error()
                        .map(|svc| svc.is_not_found())
                        .unwrap_or(false);
                    if not_found {
                        Ok(false)
                    } else {
                        Err(tonic::Status::unavailable(format!(
                            "S3 object head failed: {err}"
                        )))
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
            return Err(tonic::Status::failed_precondition(
                "s3/object-store feature is not enabled",
            ));
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
                return Err(tonic::Status::invalid_argument(
                    "part_count must be positive",
                ));
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
                    tonic::Status::unavailable(format!("S3 multipart init failed: {err}"))
                })?;
            let upload_id = upload.upload_id().unwrap_or_default().to_string();
            let ttl = bounded_ttl(request.ttl_seconds);
            let config = PresigningConfig::expires_in(Duration::from_secs(ttl)).map_err(|err| {
                tonic::Status::invalid_argument(format!("invalid presign ttl: {err}"))
            })?;
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
                        tonic::Status::unavailable(format!("S3 part presign failed: {err}"))
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

#[cfg(test)]
mod setup_data_consistency_tests {
    use super::{
        full_canonical_store_requires_opt_in, merge_runtime_backend_instances,
        pg_outbox_receipt_store_mismatch, projection_system_store_opt_in_value,
    };
    use crate::runtime::config::{BackendInstance, BackendInstanceConfig, BackendInstanceRole};

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
fn parse_elasticsearch_dsn(
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
    let (base, api_key) = if let Some(rest) = raw.strip_prefix("apikey://")
        && let Some((key, host)) = rest.split_once('@')
    {
        (format!("https://{host}"), Some(key.to_string()))
    } else {
        (raw, None)
    };
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
        report.warnings.push(format!(
            "Azure Blob DSN missing account/key (expected `account=…;key=…`)"
        ));
        return;
    }
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
            api_key: (!qdrant_config.api_key.trim().is_empty())
                .then(|| qdrant_config.api_key.clone())
                .filter(|value| !value.trim().is_empty()),
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
            return Err(tonic::Status::failed_precondition(format!(
                "typed vector search is not wired for backend '{other}'"
            )));
        }
    };
    serde_json::to_string(&spec).map_err(|err| tonic::Status::internal(err.to_string()))
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
            return Err(tonic::Status::failed_precondition(format!(
                "typed vector upsert is not wired for backend '{other}'"
            )));
        }
    };
    serde_json::to_string(&spec).map_err(|err| tonic::Status::internal(err.to_string()))
}

fn parse_vector_search_response(backend: &str, raw: &str) -> Result<VectorSet, tonic::Status> {
    let parsed: JsonValue = serde_json::from_str(raw)
        .map_err(|err| tonic::Status::internal(format!("vector search response parse: {err}")))?;
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
        return Err(tonic::Status::invalid_argument(
            "presigned URLs support only PUT or GET",
        ));
    }
    let config =
        aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(ttl))
            .map_err(|err| tonic::Status::invalid_argument(format!("invalid presign ttl: {err}")))?;
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
            .map_err(|err| tonic::Status::unavailable(format!("S3 presign failed: {err}")))
    } else {
        s3.get_object()
            .bucket(bucket)
            .key(object_key)
            .presigned(config)
            .await
            .map(|p| p.uri().to_string())
            .map_err(|err| tonic::Status::unavailable(format!("S3 presign failed: {err}")))
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
            Err(tonic::Status::resource_exhausted(format!(
                "object exceeds UDB_MAX_OBJECT_BYTES ({max_bytes})"
            )))?;
        }
        bytes_seen.store(total, Ordering::Relaxed);
        chunks_seen.store(1, Ordering::Relaxed);
        yield first;
        let mut rest = rest;
        while let Some(chunk) = rest.next().await {
            let chunk = chunk?;
            total += chunk.data.len() as u64;
            if total > max_bytes {
                Err(tonic::Status::resource_exhausted(format!(
                    "object exceeds UDB_MAX_OBJECT_BYTES ({max_bytes})"
                )))?;
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
        Err(tonic::Status::failed_precondition(format!(
            "typed object RPCs require an object-store backend, but the \
             store is configured for '{backend}'"
        )))
    }
}
