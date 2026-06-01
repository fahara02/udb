//! Continuation `impl DataBrokerRuntime` block (Phase F split of core.rs).
//!
//! Also home to the per-backend `register_*` functions invoked by
//! `backend::plugins::*` via the U2 plugin loop. These live here (not in
//! `backend/`) so they have descendant-module access to `DataBrokerRuntime`'s
//! private `pg_*`/`redis`/`qdrant_*`/etc. fields (§9.5).
use super::*;
use crate::backend::plugin::RegisterCtx;

impl DataBrokerRuntime {
    pub async fn load_abac_policies(&self) -> Vec<AbacPolicy> {
        if let Some(raw) = self.config.abac_policies_json.as_ref() {
            match serde_json::from_str::<Vec<AbacPolicy>>(raw) {
                Ok(policies) => return policies,
                Err(err) => tracing::warn!("failed to parse UDB_ABAC_POLICIES_JSON: {err}"),
            }
        }
        let Some(pool) = &self.pg_pool else {
            return Vec::new();
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
        let rows = sqlx::query(&sql).fetch_all(pool).await;
        let Ok(rows) = rows else {
            return Vec::new();
        };
        rows.into_iter()
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
            .collect()
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
        let instance_config = effective_backend_instance_config(&config);
        let app_name = effective_app_name(&config);

        crate::runtime::cdc::CdcConfig::install_global(config.cdc.clone());
        crate::runtime::security::SecurityConfig::install_global(config.security.clone());
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
                plugin.register(&mut ctx).await;
            }
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

        let runtime_instances = runtime_backend_instances(&instance_config, &report, &runtime);
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
        let cache_key = cache_key(
            "select",
            &request.message_type,
            &context,
            &manifest.checksum_sha256,
            &filter,
            &request.fields,
        );
        let bypass_read = request
            .cache
            .as_ref()
            .map(|cache| cache.bypass_read)
            .unwrap_or(false);
        if !bypass_read
            && let Some(cached) = self
                .cache_get_fresh(&cache_key, &manifest.checksum_sha256, &context)
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
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| tonic::Status::internal(format!("PG transaction begin failed: {e}")))?;
        set_request_local_settings(&mut tx, &context).await?;
        let values = filter_bind_values(&filter);
        let query = bind_values(
            sqlx::query(&plan.sql),
            table,
            &plan.parameter_columns,
            &values,
        )?;
        let rows = query
            .fetch_all(&mut *tx)
            .await
            .map_err(|err| tonic::Status::internal(format!("PostgreSQL select failed: {err}")))?;
        tx.commit().await.map_err(|err| {
            tonic::Status::internal(format!("PostgreSQL select commit failed: {err}"))
        })?;
        let record_set = rows_to_record_set(
            rows,
            Some(table),
            &plan.masked_columns,
            &context,
            self.encryption.as_ref(),
            &self.encryption_metrics,
        )?;
        let bypass_write = request
            .cache
            .as_ref()
            .map(|cache| cache.bypass_write)
            .unwrap_or(false);
        if !bypass_write {
            let ttl = request
                .cache
                .as_ref()
                .map(|cache| cache.ttl_seconds)
                .filter(|ttl| *ttl > 0)
                .unwrap_or(300) as u64;
            let _ = self
                .cache_set_stamped_from_pool(
                    &cache_key,
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
        let rows = query.fetch_all(&pool).await.map_err(|err| {
            tonic::Status::internal(format!("PostgreSQL join select failed: {err}"))
        })?;
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
                tonic::Status::internal(format!("PostgreSQL upsert failed: {err}"))
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
                tonic::Status::internal(format!("PostgreSQL upsert failed: {err}"))
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
            reject_plan(&plan.errors)?;
            ensure_typed_vector_backend(&plan.backend)?;
            let target_instance = if context.target_instance.trim().is_empty() {
                self.choose_instance_name_for_project("qdrant", false, &context.project_id)
            } else {
                Some(context.target_instance.as_str())
            };
            let qdrant =
                self.qdrant_for_instance_for_project(target_instance, &context.project_id)?;
            qdrant.search(&request, filter).await
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
            ensure_typed_vector_backend(&plan.backend)?;
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
            reject_plan(&plan.errors)?;
            ensure_typed_vector_backend(&plan.backend)?;
            let target_instance = if context.target_instance.trim().is_empty() {
                self.choose_instance_name_for_project("qdrant", true, &context.project_id)
            } else {
                Some(context.target_instance.as_str())
            };
            let qdrant =
                self.qdrant_for_instance_for_project(target_instance, &context.project_id)?;
            qdrant.upsert(&request).await?;
            Ok(MutationResponse {
                mutation_id: Uuid::new_v4().to_string(),
                resource_uri: format!("vector://{}", request.collection),
                affected_rows: request.points.len() as i64,
                ..MutationResponse::default()
            })
        }
    }

    pub async fn put_object(
        &self,
        manifest: &CatalogManifest,
        mut stream: tonic::Streaming<Chunk>,
        metadata_context: RequestContext,
    ) -> Result<MutationResponse, tonic::Status> {
        #[cfg(not(feature = "s3"))]
        {
            let _ = (manifest, &mut stream, metadata_context);
            return Err(tonic::Status::failed_precondition(
                "s3/object-store feature is not enabled",
            ));
        }
        #[cfg(feature = "s3")]
        {
            let mut chunks = Vec::new();
            let mut first: Option<Chunk> = None;
            let mut total = 0usize;
            let mut final_chunk_seen = false;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                if first.is_none() {
                    first = Some(chunk.clone());
                }
                total += chunk.data.len();
                if total > INLINE_OBJECT_LIMIT_BYTES {
                    return Err(tonic::Status::resource_exhausted(
                        "Use GeneratePresignedUrl for files > 1MB",
                    ));
                }
                final_chunk_seen |= chunk.final_chunk;
                chunks.extend_from_slice(&chunk.data);
            }
            let first =
                first.ok_or_else(|| tonic::Status::invalid_argument("empty object stream"))?;
            let context = merge_context(first.context.as_ref(), metadata_context);
            let plan = build_object_stream_plan(
                manifest,
                &ObjectStreamPlanRequest {
                    context: context.clone(),
                    bucket: first.bucket.clone(),
                    object_key: first.object_key.clone(),
                    method: "PUT".to_string(),
                    chunk_count: 1,
                    final_chunk_seen,
                    content_type: first.content_type.clone(),
                },
            );
            reject_plan(&plan.errors)?;
            ensure_typed_object_backend(&plan.backend)?;
            let target_instance = if context.target_instance.trim().is_empty() {
                self.choose_instance_name_for_project("minio", true, &context.project_id)
                    .or_else(|| {
                        self.choose_instance_name_for_project("s3", true, &context.project_id)
                    })
            } else {
                Some(context.target_instance.as_str())
            };
            let s3 = self.s3_for_instance_for_project(target_instance, &context.project_id)?;
            s3.put_object()
                .bucket(&first.bucket)
                .key(&first.object_key)
                .set_content_type(if first.content_type.is_empty() {
                    None
                } else {
                    Some(first.content_type)
                })
                .body(ByteStream::from(chunks))
                .send()
                .await
                .map_err(|err| {
                    tonic::Status::unavailable(format!("S3 put_object failed: {err}"))
                })?;
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
            let target_instance = if context.target_instance.trim().is_empty() {
                self.choose_instance_name_for_project("minio", false, &context.project_id)
                    .or_else(|| {
                        self.choose_instance_name_for_project("s3", false, &context.project_id)
                    })
            } else {
                Some(context.target_instance.as_str())
            };
            let s3 = self.s3_for_instance_for_project(target_instance, &context.project_id)?;
            let bucket = request.bucket.clone();
            let object_key = request.object_key.clone();
            let output = s3
                .get_object()
                .bucket(&bucket)
                .key(&object_key)
                .send()
                .await
                .map_err(|err| {
                    tonic::Status::unavailable(format!("S3 get_object failed: {err}"))
                })?;
            let stream = async_stream::try_stream! {
                use tokio::io::AsyncReadExt;

                let mut reader = output.body.into_async_read();
                let mut buf = vec![0u8; GET_OBJECT_CHUNK_BYTES];
                let mut pending: Option<Vec<u8>> = None;
                loop {
                    let read = reader
                        .read(&mut buf)
                        .await
                        .map_err(|err| tonic::Status::unavailable(format!("S3 body read failed: {err}")))?;
                    if read == 0 {
                        if let Some(data) = pending.take() {
                            yield Chunk {
                                bucket: bucket.clone(),
                                object_key: object_key.clone(),
                                data,
                                final_chunk: true,
                                ..Chunk::default()
                            };
                        }
                        break;
                    }
                    let current = buf[..read].to_vec();
                    if let Some(data) = pending.replace(current) {
                        yield Chunk {
                            bucket: bucket.clone(),
                            object_key: object_key.clone(),
                            data,
                            final_chunk: false,
                            ..Chunk::default()
                        };
                    }
                }
            };
            Ok(Box::pin(stream)
                as std::pin::Pin<
                    Box<
                        dyn tokio_stream::Stream<Item = Result<Chunk, tonic::Status>>
                            + Send
                            + 'static,
                    >,
                >)
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
            let target_instance = if context.target_instance.trim().is_empty() {
                let write = method == "PUT" || method == "POST";
                self.choose_instance_name_for_project("minio", write, &context.project_id)
                    .or_else(|| {
                        self.choose_instance_name_for_project("s3", write, &context.project_id)
                    })
            } else {
                Some(context.target_instance.as_str())
            };
            let s3 = self.s3_for_instance_for_project(target_instance, &context.project_id)?;
            let ttl = bounded_ttl(request.ttl_seconds);
            let config = PresigningConfig::expires_in(Duration::from_secs(ttl)).map_err(|err| {
                tonic::Status::invalid_argument(format!("invalid presign ttl: {err}"))
            })?;
            let url = if method == "PUT" {
                s3.put_object()
                    .bucket(&request.bucket)
                    .key(&request.object_key)
                    .set_content_type(if request.content_type.is_empty() {
                        None
                    } else {
                        Some(request.content_type)
                    })
                    .presigned(config)
                    .await
                    .map(|presigned| presigned.uri().to_string())
                    .map_err(|err| {
                        tonic::Status::unavailable(format!("S3 presign failed: {err}"))
                    })?
            } else {
                s3.get_object()
                    .bucket(&request.bucket)
                    .key(&request.object_key)
                    .presigned(config)
                    .await
                    .map(|presigned| presigned.uri().to_string())
                    .map_err(|err| {
                        tonic::Status::unavailable(format!("S3 presign failed: {err}"))
                    })?
            };
            Ok(UrlResponse {
                url,
                expires_at_unix: unix_now() + ttl as i64,
            })
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

// ── Per-backend register functions (U2 step 3) ────────────────────────────────
//
// Each `register_*` mirrors the inline setup block it replaced. The plugin's
// `register` method just calls the matching function here; from_config drives
// the whole list through `for plugin in all_plugins() { plugin.register(ctx) }`.

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
                    let store: std::sync::Arc<dyn SystemStores> = std::sync::Arc::new(
                        PostgresCanonicalStore::new(pool.clone(), "primary", outbox_relation),
                    );
                    runtime.register_full_canonical_store(store);
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
                let store: std::sync::Arc<dyn SystemStores> = std::sync::Arc::new(
                    MysqlCanonicalStore::new(pool.clone(), "primary", outbox_relation),
                );
                runtime.register_full_canonical_store(store);
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
                let store: std::sync::Arc<dyn SystemStores> = std::sync::Arc::new(
                    SqliteCanonicalStore::new(pool.clone(), "primary", outbox_table),
                );
                runtime.register_full_canonical_store(store);
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
    use crate::runtime::executors::mssql::MssqlClient;
    let RegisterCtx {
        runtime, report, ..
    } = ctx;
    let Some(ado) = std::env::var("UDB_MSSQL_DSN")
        .ok()
        .filter(|s| !s.trim().is_empty())
    else {
        return;
    };
    let client = MssqlClient::new(ado);
    tracing::info!("SQL Server client constructed (connection deferred to first use)");
    report.mssql_configured = true;
    runtime
        .mssql_instances
        .insert("primary".to_string(), client.clone());
    runtime.mssql = Some(client);
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
    runtime.pinecone = Some(client);
}

/// C9: register Cassandra / ScyllaDB. DSN forms:
///   `host:9042`                      → single node, no auth
///   `host1:9042,host2:9042`          → multi-node bootstrap
///   `user:pass@host:9042`            → password authenticator
#[cfg(feature = "cassandra")]
pub(crate) async fn register_cassandra(ctx: &mut RegisterCtx<'_>) {
    use crate::runtime::executors::cassandra::CassandraClient;
    let RegisterCtx {
        runtime, report, ..
    } = ctx;
    let Some(dsn) = std::env::var("UDB_CASSANDRA_DSN")
        .ok()
        .filter(|s| !s.trim().is_empty())
    else {
        return;
    };
    match CassandraClient::connect(&dsn).await {
        Ok(client) => {
            report.cassandra_configured = true;
            runtime
                .cassandra_instances
                .insert("primary".to_string(), client.clone());
            runtime.cassandra = Some(client);
        }
        Err(err) => {
            report
                .warnings
                .push(format!("Cassandra unavailable: {err}"));
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

/// #126: typed vector RPCs (`VectorSearch`/`VectorUpsert`/`VectorHybridSearch`)
/// are served by Qdrant only. Weaviate/Pinecone/Elasticsearch advertise vector
/// support but are reachable through `GenericDispatch` (vector REST), not these
/// typed RPCs. Reject a collection whose manifest declares a different backend
/// instead of silently serving it from Qdrant (which would query an unrelated /
/// empty collection). An empty/`qdrant` backend is accepted.
#[cfg(feature = "qdrant")]
fn ensure_typed_vector_backend(backend: &str) -> Result<(), tonic::Status> {
    let normalized = backend.trim().to_ascii_lowercase();
    if normalized.is_empty() || normalized == "qdrant" {
        Ok(())
    } else {
        Err(tonic::Status::failed_precondition(format!(
            "vector collection is configured for backend '{backend}', but typed vector RPCs are \
             served by Qdrant only; use GenericDispatch (vector REST) to reach '{backend}'"
        )))
    }
}

/// #127: typed object RPCs (`GetObject`/`PutObject`/`GeneratePresignedUrl`) are
/// served by the S3/MinIO client only. Azure Blob / GCS advertise object-store
/// support but are reachable through `GenericDispatch` / their executors, not
/// these typed RPCs. Reject a store whose manifest declares a different backend
/// instead of silently using S3/MinIO. Empty / `s3` / `minio` are accepted.
#[cfg(feature = "s3")]
fn ensure_typed_object_backend(backend: &str) -> Result<(), tonic::Status> {
    let normalized = backend.trim().to_ascii_lowercase();
    if matches!(normalized.as_str(), "" | "s3" | "minio") {
        Ok(())
    } else {
        Err(tonic::Status::failed_precondition(format!(
            "object store is configured for backend '{backend}', but typed object RPCs are served \
             by S3/MinIO only; use GenericDispatch / the object executor to reach '{backend}'"
        )))
    }
}
