//! Continuation `impl DataBrokerRuntime` block (Phase F split of core.rs).
use super::*;

impl DataBrokerRuntime {
    pub async fn begin_tx(
        &self,
        manifest: &CatalogManifest,
        mut stream: tonic::Streaming<Mutation>,
        metadata_context: RequestContext,
    ) -> Vec<Result<TxStatus, tonic::Status>> {
        let Some(pool) = &self.pg_pool else {
            return vec![Err(tonic::Status::unavailable(
                "PostgreSQL backend is not configured",
            ))];
        };
        let mut mutations = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(mutation) => mutations.push(mutation),
                Err(err) => return vec![Err(err)],
            }
        }
        if mutations.is_empty() {
            return vec![Err(tonic::Status::invalid_argument(
                "transaction stream requires at least one mutation",
            ))];
        }
        let tx_id = mutations
            .iter()
            .find(|mutation| !mutation.tx_id.is_empty())
            .map(|mutation| mutation.tx_id.clone())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        if mutations.iter().any(|mutation| mutation.rollback) {
            return vec![Ok(TxStatus {
                state: crate::proto::tx_status::State::TxStateRolledBack as i32,
                tx_id,
                message: "rollback requested".to_string(),
                ..TxStatus::default()
            })];
        }
        // B (2026-05-30): capture the resolved strategy so the commit
        // path can choose between plain COMMIT and the live 2PC
        // protocol (PREPARE TRANSACTION + COMMIT PREPARED).
        let strategy = match requested_tx_strategy(&metadata_context, &mutations) {
            Ok(s) => s,
            Err(err) => return vec![Err(err)],
        };
        if let Err(err) = validate_tx_strategy(strategy, &mutations) {
            return vec![Err(err)];
        }
        let commit = mutations.iter().any(|mutation| mutation.commit);
        let mutation_count = mutations
            .iter()
            .filter(|m| !m.commit && !m.rollback)
            .count();

        // Persist saga state before opening the PG transaction (saga tracking is best-effort).
        let saga_id = self
            .saga_begin(
                &tx_id,
                mutation_count,
                &metadata_context.tenant_id,
                &metadata_context.correlation_id,
                classify_tx_semantics(&mutations).as_str(),
                tx_backend_instance(&mutations).unwrap_or_default().as_str(),
            )
            .await;

        let mut tx = match pool.begin().await {
            Ok(tx) => tx,
            Err(err) => {
                if let Some(ref sid) = saga_id {
                    self.saga_set_status(sid, "failed").await;
                }
                return vec![Err(tonic::Status::unavailable(format!(
                    "PostgreSQL begin failed: {err}"
                )))];
            }
        };
        let mut statuses = Vec::new();
        let mut qdrant_compensations: Vec<(String, Vec<String>)> = Vec::new();
        let mut s3_compensations: Vec<(String, String)> = Vec::new();
        let projection_plans = crate::runtime::projection::ProjectionPlan::from_manifest(manifest);
        let projection_config = crate::runtime::system::SystemCatalogConfig::current();
        let mut step_index: usize = 0;
        for mutation in mutations.iter().filter(|mutation| !mutation.commit) {
            let context = merge_context(mutation.context.as_ref(), metadata_context.clone());
            let operation = mutation.operation.to_ascii_lowercase();
            let result = if operation == "upsert" {
                let record = mutation_record_json(mutation);
                match record {
                    Ok(record) => {
                        let Some(table) = table_for_message(manifest, &mutation.message_type)
                        else {
                            if let Some(ref sid) = saga_id {
                                self.saga_set_status(sid, "failed").await;
                            }
                            return vec![Err(tonic::Status::invalid_argument(
                                "unknown message_type",
                            ))];
                        };
                        let encrypted_record = match self.encrypt_record_for_table(table, &record) {
                            Ok(record) => record,
                            Err(err) => {
                                let _ = tx.rollback().await;
                                if let Some(ref sid) = saga_id {
                                    self.saga_set_status(sid, "failed").await;
                                }
                                statuses.push(Err(err));
                                return statuses;
                            }
                        };
                        let plan = build_upsert_plan(
                            manifest,
                            &UpsertPlanRequest {
                                context: context.clone(),
                                message_type: mutation.message_type.clone(),
                                record: record.clone(),
                                ..UpsertPlanRequest::default()
                            },
                        );
                        let affected = execute_tx_plan(
                            &mut tx,
                            manifest,
                            &mutation.message_type,
                            &plan.sql,
                            &plan.parameter_columns,
                            &record_values(&encrypted_record, &plan.parameter_columns)
                                .unwrap_or_default(),
                            &plan.errors,
                        )
                        .await;
                        match affected {
                            Ok(affected) => {
                                if affected > 0
                                    && let Err(err) = crate::runtime::projection::ProjectionEngine::enqueue_write_tasks_tx(
                                        &mut tx,
                                        &projection_config,
                                        &context.tenant_id,
                                        &mutation.message_type,
                                        "upsert",
                                        &record,
                                        &projection_plans,
                                    )
                                    .await
                                {
                                    Err(tonic::Status::internal(format!(
                                        "projection task enqueue failed: {err}"
                                    )))
                                } else {
                                    Ok(affected)
                                }
                            }
                            Err(err) => Err(err),
                        }
                    }
                    Err(err) => Err(err),
                }
            } else if operation == "delete" {
                let filter = mutation
                    .filter
                    .as_ref()
                    .map(struct_to_json)
                    .unwrap_or(JsonValue::Null);
                let plan = build_delete_plan(
                    manifest,
                    &DeletePlanRequest {
                        context: context.clone(),
                        message_type: mutation.message_type.clone(),
                        filter: filter.clone(),
                    },
                );
                let affected = execute_tx_plan(
                    &mut tx,
                    manifest,
                    &mutation.message_type,
                    &plan.sql,
                    &plan.parameter_columns,
                    &filter_bind_values(&filter),
                    &plan.errors,
                )
                .await;
                match affected {
                    Ok(affected) => {
                        if affected > 0
                            && let Err(err) = crate::runtime::projection::ProjectionEngine::enqueue_write_tasks_tx(
                                &mut tx,
                                &projection_config,
                                &context.tenant_id,
                                &mutation.message_type,
                                "delete",
                                &filter,
                                &projection_plans,
                            )
                            .await
                        {
                            Err(tonic::Status::internal(format!(
                                "projection task enqueue failed: {err}"
                            )))
                        } else {
                            Ok(affected)
                        }
                    }
                    Err(err) => Err(err),
                }
            } else if operation == "vector_upsert" {
                let request = VectorUpsertRequest {
                    context: mutation.context.clone(),
                    collection: mutation.collection.clone(),
                    points: mutation.vector_points.clone(),
                    idempotency_key: mutation.idempotency_key.clone(),
                };
                let point_ids = request
                    .points
                    .iter()
                    .map(|point| point.id.clone())
                    .filter(|id| !id.is_empty())
                    .collect::<Vec<_>>();
                match self
                    .vector_upsert(manifest, request, metadata_context.clone())
                    .await
                {
                    Ok(response) => {
                        if !point_ids.is_empty() {
                            qdrant_compensations.push((mutation.collection.clone(), point_ids));
                        }
                        Ok(response.affected_rows as u64)
                    }
                    Err(err) => Err(err),
                }
            } else if operation == "put_object" {
                match self
                    .put_tx_object(manifest, mutation, metadata_context.clone())
                    .await
                {
                    Ok(affected) => {
                        s3_compensations
                            .push((mutation.bucket.clone(), mutation.object_key.clone()));
                        Ok(affected)
                    }
                    Err(err) => Err(err),
                }
            } else if operation == "enqueue_outbox_event" {
                let cdc_config = self.config.cdc.clone();
                let topic = if mutation.collection.trim().is_empty() {
                    mutation.message_type.as_str()
                } else {
                    mutation.collection.as_str()
                };
                let policy_decision = match self
                    .topic_policy_allows(topic, &context.project_id, &context.tenant_id)
                    .await
                {
                    Ok(decision) => decision,
                    Err(err) => return vec![Err(err)],
                };
                if policy_decision == Some(false)
                    || (!cdc_config.valid_topics.is_empty()
                        && !cdc_config.valid_topics.iter().any(|t| t == topic)
                        && policy_decision.is_none())
                {
                    Err(tonic::Status::invalid_argument(format!(
                        "topic '{topic}' is not in the registered topic registry; \
                         configure UDB_CDC_VALID_TOPICS or use an allowed topic"
                    )))
                } else {
                    let payload = mutation
                        .payload
                        .as_ref()
                        .map(struct_to_json)
                        .ok_or_else(|| {
                            tonic::Status::invalid_argument(
                                "enqueue_outbox_event transaction mutation requires payload",
                            )
                        });
                    match payload {
                        Ok(payload) => {
                            let schema_uri = if mutation.content_type.trim().is_empty() {
                                None
                            } else {
                                Some(mutation.content_type.as_str())
                            };
                            let payload = crate::runtime::cdc::apply_manifest_cdc_redaction(
                                manifest,
                                &mutation.message_type,
                                topic,
                                schema_uri,
                                payload,
                                cdc_config.redaction_mode,
                                cdc_config.redaction_version,
                            );
                            match prepare_outbox_envelope(
                                topic,
                                &mutation.object_key,
                                payload,
                                schema_uri,
                            ) {
                                Ok((event_id_uuid, _, enriched)) => {
                                    let outbox_relation = cdc_config.outbox_relation();
                                    let sql = format!(
                                        "INSERT INTO {outbox_relation} \
                                         (event_id, topic, partition_key, payload, created_at) \
                                         VALUES ($1::UUID, $2, $3, $4::JSONB, NOW())"
                                    );
                                    sqlx::query(&sql)
                                        .bind(event_id_uuid)
                                        .bind(topic)
                                        .bind(&mutation.object_key)
                                        .bind(enriched.to_string())
                                        .execute(&mut *tx)
                                        .await
                                        .map(|result| result.rows_affected())
                                        .map_err(|err| {
                                            tonic::Status::internal(format!(
                                                "failed to enqueue tx event: {err}"
                                            ))
                                        })
                                }
                                Err(err) => Err(err),
                            }
                        }
                        Err(err) => Err(err),
                    }
                }
            } else {
                Err(tonic::Status::invalid_argument(format!(
                    "unsupported transaction operation {}",
                    mutation.operation
                )))
            };
            match result {
                Ok(affected) => {
                    // Record the successful step in the saga.
                    if let Some(ref sid) = saga_id {
                        let compensation = match operation.as_str() {
                            "vector_upsert" => serde_json::json!({
                                "backend": "qdrant",
                                "operation": "delete_points",
                                "resource_uri": format!("qdrant://{}", mutation.collection),
                                "payload": {
                                    "collection": mutation.collection,
                                    "point_ids": mutation.vector_points.iter()
                                        .map(|point| point.id.clone())
                                        .filter(|id| !id.is_empty())
                                        .collect::<Vec<_>>()
                                }
                            }),
                            "put_object" => serde_json::json!({
                                "backend": "s3",
                                "operation": "delete_object",
                                "resource_uri": format!("s3://{}/{}", mutation.bucket, mutation.object_key),
                                "payload": {
                                    "bucket": mutation.bucket,
                                    "key": mutation.object_key,
                                }
                            }),
                            "enqueue_outbox_event" => serde_json::json!({
                                "backend": "postgres",
                                "operation": "delete_outbox_event",
                                "resource_uri": "udb://outbox/event",
                                "payload": {
                                    "idempotency_key": mutation.idempotency_key,
                                    "message_type": mutation.message_type,
                                }
                            }),
                            _ => serde_json::json!({
                                "backend": "postgres",
                                "operation": "rollback_sql_tx",
                                "resource_uri": format!("postgres://{}", mutation.message_type),
                                "payload": {
                                    "message_type": mutation.message_type,
                                    "note": "rolled back by PostgreSQL transaction boundary"
                                }
                            }),
                        };
                        self.saga_record_step(
                            sid,
                            step_index,
                            &operation,
                            &mutation.message_type,
                            &compensation.to_string(),
                        )
                        .await;
                    }
                    step_index += 1;
                    statuses.push(Ok(TxStatus {
                        state: crate::proto::tx_status::State::TxStateOpen as i32,
                        tx_id: tx_id.clone(),
                        mutation_id: Uuid::new_v4().to_string(),
                        message: format!("{affected} row(s) affected"),
                    }))
                }
                Err(err) => {
                    let _ = tx.rollback().await;
                    let compensation_message = self
                        .compensate_external_side_effects(&qdrant_compensations, &s3_compensations)
                        .await
                        .unwrap_or_else(|| "no external compensation was needed".to_string());
                    if let Some(ref sid) = saga_id {
                        self.saga_set_status(sid, "compensated").await;
                    }
                    statuses.push(Err(tonic::Status::internal(format!(
                        "{}; {}",
                        err.message(),
                        compensation_message
                    ))));
                    return statuses;
                }
            }
        }
        if commit {
            // B (2026-05-30): live 2PC path — when the caller asked
            // for `tx_strategy=two_phase` AND the operator enabled
            // `UDB_2PC_ENABLED=true`, replace the plain COMMIT with
            // the prepared-transaction protocol so the PG
            // participant is durably PREPARED before any visible
            // commit. The actual second-phase COMMIT PREPARED runs
            // on a fresh connection from the pool — the prepared
            // state survives PG client disconnects, which is the
            // whole point of 2PC.
            if strategy == TxStrategy::TwoPhase && super::two_phase_runtime_enabled() {
                // PHASE 1: PREPARE TRANSACTION 'xid'. This consumes
                // the in-flight tx and leaves the row locks held in
                // PostgreSQL's prepared_xacts catalog. The xid is
                // generated locally; we use the request's tx_id for
                // traceability, sanitised to PG's 200-char ASCII
                // identifier limit.
                let xid = format!("udb_{}", tx_id.replace('-', ""));
                let xid_quoted = xid.replace('\'', "''");
                let prepare_sql = format!("PREPARE TRANSACTION '{xid_quoted}'");
                if let Err(err) = sqlx::query(&prepare_sql).execute(&mut *tx).await {
                    let _ = tx.rollback().await;
                    let comp_msg = self
                        .compensate_external_side_effects(&qdrant_compensations, &s3_compensations)
                        .await
                        .unwrap_or_else(|| "no external compensation was needed".to_string());
                    if let Some(ref sid) = saga_id {
                        self.saga_set_status(sid, "compensated").await;
                    }
                    statuses.push(Err(tonic::Status::internal(format!(
                        "PostgreSQL PREPARE TRANSACTION failed: {err}; {comp_msg}"
                    ))));
                    return statuses;
                }
                // After PREPARE TRANSACTION, the tx handle is
                // detached from the connection. Drop it without
                // calling commit/rollback — both would error.
                drop(tx);

                // PHASE 2: COMMIT PREPARED on a fresh connection.
                // If this fails, the transaction is IN-DOUBT: the
                // prepared state is durable but uncommitted. The
                // recovery worker (`XaInDoubtParticipant` for
                // Postgres) drives stragglers. Surface InDoubt so
                // the operator knows.
                let commit_sql = format!("COMMIT PREPARED '{xid_quoted}'");
                match sqlx::query(&commit_sql).execute(pool).await {
                    Ok(_) => {
                        if let Some(ref sid) = saga_id {
                            self.saga_set_status(sid, "committed").await;
                        }
                        statuses.push(Ok(TxStatus {
                            state: crate::proto::tx_status::State::TxStateCommitted as i32,
                            tx_id,
                            message: format!("committed (2pc xid={xid})"),
                            ..TxStatus::default()
                        }));
                    }
                    Err(err) => {
                        if let Some(ref sid) = saga_id {
                            // IN-DOUBT — don't mark compensated, the
                            // tx is half-committed at the RM. The
                            // recovery worker will drive resolution.
                            self.saga_set_status(sid, "in_doubt").await;
                        }
                        statuses.push(Err(tonic::Status::aborted(format!(
                            "PostgreSQL COMMIT PREPARED failed for xid {xid}: {err}; \
                             transaction is IN-DOUBT and will be resolved by the XA recovery worker"
                        ))));
                    }
                }
            } else {
                match tx.commit().await {
                    Ok(()) => {
                        if let Some(ref sid) = saga_id {
                            self.saga_set_status(sid, "committed").await;
                        }
                        statuses.push(Ok(TxStatus {
                            state: crate::proto::tx_status::State::TxStateCommitted as i32,
                            tx_id,
                            message: "committed".to_string(),
                            ..TxStatus::default()
                        }))
                    }
                    Err(err) => {
                        let comp_msg = self
                            .compensate_external_side_effects(
                                &qdrant_compensations,
                                &s3_compensations,
                            )
                            .await
                            .unwrap_or_else(|| "no external compensation was needed".to_string());
                        if let Some(ref sid) = saga_id {
                            self.saga_set_status(sid, "compensated").await;
                        }
                        statuses.push(Err(tonic::Status::internal(format!(
                            "PostgreSQL commit failed: {err}; {comp_msg}"
                        ))))
                    }
                }
            }
        } else {
            let _ = tx.rollback().await;
            let compensation_message = self
                .compensate_external_side_effects(&qdrant_compensations, &s3_compensations)
                .await;
            if let Some(ref sid) = saga_id {
                self.saga_set_status(sid, "compensated").await;
            }
            statuses.push(Ok(TxStatus {
                state: crate::proto::tx_status::State::TxStateRolledBack as i32,
                tx_id,
                message: compensation_message.unwrap_or_else(|| {
                    "stream closed without commit=true; rolled back".to_string()
                }),
                ..TxStatus::default()
            }));
        }
        statuses
    }

    pub fn publish_cdc(&self, _request: CdcSubscriptionRequest) -> Vec<CdcEnvelope> {
        Vec::new()
    }

    pub async fn create_materialized_view(
        &self,
        manifest: &CatalogManifest,
        request: ViewDefinition,
        metadata_context: RequestContext,
    ) -> Result<MutationResponse, tonic::Status> {
        let context = merge_context(request.context.as_ref(), metadata_context);
        if !context
            .scopes
            .iter()
            .any(|scope| scope == "udb:admin" || scope == "*")
        {
            return Err(tonic::Status::permission_denied(
                "scope udb:admin is required",
            ));
        }
        validate_identifier(&request.schema, "schema")?;
        validate_identifier(&request.name, "name")?;
        let declared = declared_materialized_view(manifest, &request.schema, &request.name)
            .ok_or_else(|| {
                tonic::Status::invalid_argument(format!(
                    "materialized view {}.{} is not declared in the proto AST",
                    request.schema, request.name
                ))
            })?;
        // Always use the trusted proto-AST query. If the caller supplies a query, it must
        // normalize to the same string as the declared query (whitespace differences only).
        // We then use `declared.query` so the SQL always comes from the authoritative source.
        let query = if request.query.trim().is_empty()
            || normalize_sql(&request.query) == normalize_sql(&declared.query)
        {
            declared.query.as_str()
        } else {
            return Err(tonic::Status::invalid_argument(
                "materialized view query does not match the proto AST declaration",
            ));
        };
        let with_data = request.with_data || declared.with_data;
        let sql = format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS \"{}\"{}\" AS {} {}",
            request.schema.replace('"', "\"\""),
            request.name.replace('"', "\"\""),
            query,
            if with_data { "" } else { "WITH NO DATA" }
        );
        let result = sqlx::query(&sql)
            .execute(self.pg_pool()?)
            .await
            .map_err(|err| tonic::Status::internal(format!("create view failed: {err}")))?;
        let response = MutationResponse {
            mutation_id: Uuid::new_v4().to_string(),
            resource_uri: format!("materialized-view://{}/{}", request.schema, request.name),
            affected_rows: result.rows_affected() as i64,
            ..MutationResponse::default()
        };
        if request.ttl_days > 0 {
            self.schedule_materialized_view_refresh(
                request.schema.clone(),
                request.name.clone(),
                request.ttl_days,
            );
        }
        Ok(response)
    }

    pub fn start_materialized_view_refresh(&self, manifest: &CatalogManifest) -> usize {
        let ttl_days = if self.config.materialized_view_ttl_days == 0 {
            7
        } else {
            self.config.materialized_view_ttl_days
        };
        if ttl_days <= 0 {
            return 0;
        }
        let mut scheduled = 0usize;
        for view in manifest
            .tables
            .iter()
            .flat_map(|table| table.materialized_views.iter())
        {
            self.schedule_materialized_view_refresh(
                view.schema.clone(),
                view.name.clone(),
                ttl_days,
            );
            scheduled += 1;
        }
        scheduled
    }

    pub fn schedule_materialized_view_refresh(&self, schema: String, name: String, ttl_days: i32) {
        if self.pg_pool.is_none() || ttl_days <= 0 {
            return;
        }
        let runtime = self.clone();
        let seconds = (ttl_days as u64).saturating_mul(86_400).max(60);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(seconds));
            loop {
                interval.tick().await;
                if let Err(err) = runtime.refresh_materialized_view(&schema, &name).await {
                    tracing::warn!(
                        schema = schema,
                        view = name,
                        error = %err,
                        "materialized view refresh failed"
                    );
                }
            }
        });
    }

    pub async fn refresh_materialized_view(
        &self,
        schema: &str,
        name: &str,
    ) -> Result<(), tonic::Status> {
        validate_identifier(schema, "schema")?;
        validate_identifier(name, "name")?;
        let sql = format!(
            "REFRESH MATERIALIZED VIEW CONCURRENTLY {}.{}",
            qi_runtime(schema),
            qi_runtime(name)
        );
        sqlx::query(&sql)
            .execute(self.pg_pool()?)
            .await
            .map_err(|err| tonic::Status::internal(format!("refresh view failed: {err}")))?;
        Ok(())
    }

    pub(crate) fn pg_pool(&self) -> Result<&PgPool, tonic::Status> {
        self.pg_pool
            .as_ref()
            .ok_or_else(|| tonic::Status::unavailable("PostgreSQL backend is not configured"))
    }

    pub(crate) fn pg_select_pool_for_table(
        &self,
        table: &crate::generation::ManifestTable,
        context: &RequestContext,
    ) -> Result<PgPool, tonic::Status> {
        if !context.target_instance.trim().is_empty() {
            return self.pg_read_pool_for_context_checked(context);
        }
        if table.replica_hint.eq_ignore_ascii_case("primary") || context.requires_primary_read() {
            return self.pg_pool().cloned();
        }
        self.pg_read_pool_for_context_checked(context)
    }

    #[cfg(feature = "qdrant")]
    pub(crate) fn qdrant(&self) -> Result<&QdrantHttpClient, tonic::Status> {
        self.qdrant
            .as_ref()
            .ok_or_else(|| tonic::Status::unavailable("Qdrant backend is not configured"))
    }

    #[cfg(feature = "s3")]
    pub(crate) fn s3(&self) -> Result<&aws_sdk_s3::Client, tonic::Status> {
        self.s3
            .as_ref()
            .ok_or_else(|| tonic::Status::unavailable("S3/MinIO backend is not configured"))
    }

    // Read-through cache (orchestration, §9.1). When the `redis` feature is off the
    // cache is a no-op so the typed select/upsert paths compile and run unchanged.
    #[cfg(feature = "redis")]
    pub(crate) async fn cache_get_fresh(
        &self,
        key: &str,
        manifest_checksum: &str,
        context: &RequestContext,
    ) -> Option<Vec<Vec<u8>>> {
        let client = self.redis.as_ref()?;
        let mut conn = client.get_multiplexed_async_connection().await.ok()?;
        let data = match conn.get::<_, Option<Vec<u8>>>(key).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                self.cache_metrics.miss();
                return None;
            }
            Err(_) => return None,
        };
        let min_lsn = serde_json::from_str::<crate::runtime::consistency::ReadFence>(
            &context.read_fence_json,
        )
        .ok()
        .and_then(|fence| crate::runtime::consistency_fence::parse_pg_lsn(&fence.min_outbox_lsn));

        if let Ok(stamped) = serde_json::from_slice::<
            crate::runtime::consistency_fence::StampedCacheValue<Vec<Vec<u8>>>,
        >(&data)
        {
            if stamped.is_fresh_against(manifest_checksum, min_lsn) {
                self.cache_metrics.hit();
                return Some(stamped.value);
            }
            self.cache_metrics.miss();
            return None;
        }

        if min_lsn.is_some() {
            self.cache_metrics.miss();
            return None;
        }
        match serde_json::from_slice(&data) {
            Ok(value) => {
                self.cache_metrics.hit();
                Some(value)
            }
            Err(_) => {
                self.cache_metrics.miss();
                None
            }
        }
    }

    #[cfg(not(feature = "redis"))]
    pub(crate) async fn cache_get_fresh(
        &self,
        _key: &str,
        _manifest_checksum: &str,
        _context: &RequestContext,
    ) -> Option<Vec<Vec<u8>>> {
        None
    }

    #[cfg(feature = "redis")]
    pub(crate) async fn cache_set(
        &self,
        key: &str,
        records: &[Vec<u8>],
        ttl: u64,
    ) -> Result<(), String> {
        let Some(client) = &self.redis else {
            return Ok(());
        };
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;
        let data = serde_json::to_vec(records).unwrap_or_default();
        conn.set_ex::<_, _, ()>(key, data, ttl)
            .await
            .map_err(|e| e.to_string())
    }

    #[cfg(feature = "redis")]
    pub(crate) async fn cache_set_stamped_from_pool(
        &self,
        key: &str,
        records: &[Vec<u8>],
        ttl: u64,
        manifest_checksum: &str,
        source_pool: &PgPool,
    ) -> Result<(), String> {
        let source_lsn = sqlx::query_scalar::<_, String>(
            "SELECT COALESCE(pg_last_wal_replay_lsn()::TEXT, pg_current_wal_lsn()::TEXT)",
        )
        .fetch_optional(source_pool)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
        self.cache_set_stamped_with_lsn(key, records, ttl, manifest_checksum, source_lsn)
            .await
    }

    #[cfg(not(feature = "redis"))]
    pub(crate) async fn cache_set_stamped_from_pool(
        &self,
        _key: &str,
        _records: &[Vec<u8>],
        _ttl: u64,
        _manifest_checksum: &str,
        _source_pool: &PgPool,
    ) -> Result<(), String> {
        Ok(())
    }

    #[cfg(feature = "redis")]
    async fn cache_set_stamped_with_lsn(
        &self,
        key: &str,
        records: &[Vec<u8>],
        ttl: u64,
        manifest_checksum: &str,
        source_lsn: String,
    ) -> Result<(), String> {
        let Some(client) = &self.redis else {
            return Ok(());
        };
        let stamp = crate::runtime::consistency_fence::RedisCacheStamp {
            manifest_checksum: manifest_checksum.to_string(),
            source_lsn,
            projection_version: crate::runtime::cdc::CdcConfig::current().producer_epoch,
            written_at_unix_ms: unix_millis(),
        };
        let wrapped =
            crate::runtime::consistency_fence::StampedCacheValue::new(stamp, records.to_vec());
        let data = serde_json::to_vec(&wrapped).map_err(|e| e.to_string())?;
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;
        conn.set_ex::<_, _, ()>(key, data, ttl)
            .await
            .map_err(|e| e.to_string())
    }

    #[cfg(not(feature = "redis"))]
    pub(crate) async fn cache_set(
        &self,
        _key: &str,
        _records: &[Vec<u8>],
        _ttl: u64,
    ) -> Result<(), String> {
        Ok(())
    }

    #[cfg(not(feature = "redis"))]
    pub(crate) async fn cache_delete_pattern(&self, _pattern: &str) -> Result<(), String> {
        Ok(())
    }

    #[cfg(feature = "redis")]
    pub(crate) async fn cache_delete_pattern(&self, pattern: &str) -> Result<(), String> {
        let Some(client) = &self.redis else {
            return Ok(());
        };
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;
        let mut cursor = 0_u64;
        loop {
            let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(500_u32)
                .query_async(&mut conn)
                .await
                .map_err(|e| e.to_string())?;
            if !keys.is_empty() {
                let deleted = redis::cmd("DEL")
                    .arg(&keys)
                    .query_async::<u64>(&mut conn)
                    .await
                    .unwrap_or_default();
                self.cache_metrics.invalidated(deleted);
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        Ok(())
    }

    pub(crate) fn encrypt_record_for_table(
        &self,
        table: &ManifestTable,
        record: &JsonValue,
    ) -> Result<JsonValue, tonic::Status> {
        let Some(encryption) = &self.encryption else {
            if table.columns.iter().any(is_encrypted_column) {
                self.encryption_metrics.record("encrypt", false);
                return Err(tonic::Status::failed_precondition(format!(
                    "table {}.{} contains encrypted columns but UDB encryption key is not configured",
                    table.schema, table.table
                )));
            }
            return Ok(record.clone());
        };
        let Some(object) = record.as_object() else {
            return Ok(record.clone());
        };
        let mut encrypted = object.clone();
        for column in table
            .columns
            .iter()
            .filter(|column| is_encrypted_column(column))
        {
            let Some(value) = encrypted.get(&column.column_name).cloned() else {
                continue;
            };
            if value.is_null() || json_is_ciphertext(&value) {
                continue;
            }
            match encryption.encrypt_json_value(&value) {
                Ok(ciphertext) => {
                    self.encryption_metrics.record("encrypt", true);
                    encrypted.insert(column.column_name.clone(), JsonValue::String(ciphertext));
                }
                Err(err) => {
                    self.encryption_metrics.record("encrypt", false);
                    return Err(tonic::Status::internal(format!(
                        "failed to encrypt {}.{}: {err}",
                        table.table, column.column_name
                    )));
                }
            }
        }
        Ok(JsonValue::Object(encrypted))
    }

    pub(crate) async fn put_tx_object(
        &self,
        manifest: &CatalogManifest,
        mutation: &Mutation,
        metadata_context: RequestContext,
    ) -> Result<u64, tonic::Status> {
        #[cfg(not(feature = "s3"))]
        {
            let _ = (manifest, mutation, metadata_context);
            return Err(tonic::Status::failed_precondition(
                "s3/object-store feature is not enabled",
            ));
        }
        #[cfg(feature = "s3")]
        {
            if mutation.object_data.len() > INLINE_OBJECT_LIMIT_BYTES {
                return Err(tonic::Status::resource_exhausted(
                    "Use GeneratePresignedUrl for files > 1MB",
                ));
            }
            let context = merge_context(mutation.context.as_ref(), metadata_context);
            let plan = build_object_stream_plan(
                manifest,
                &ObjectStreamPlanRequest {
                    context,
                    bucket: mutation.bucket.clone(),
                    object_key: mutation.object_key.clone(),
                    method: "PUT".to_string(),
                    chunk_count: 1,
                    final_chunk_seen: true,
                    content_type: mutation.content_type.clone(),
                },
            );
            reject_plan(&plan.errors)?;
            let s3 = self.s3()?;
            s3.put_object()
                .bucket(&mutation.bucket)
                .key(&mutation.object_key)
                .set_content_type(if mutation.content_type.is_empty() {
                    None
                } else {
                    Some(mutation.content_type.clone())
                })
                .body(ByteStream::from(mutation.object_data.clone()))
                .send()
                .await
                .map_err(|err| {
                    tonic::Status::unavailable(format!("S3 put_object failed: {err}"))
                })?;
            Ok(1)
        }
    }

    #[cfg(not(feature = "qdrant"))]
    pub(crate) async fn compensate_qdrant_upserts(
        &self,
        compensations: &[(String, Vec<String>)],
    ) -> Option<String> {
        if compensations.is_empty() {
            None
        } else {
            Some(
                "rolled back PostgreSQL; Qdrant compensation skipped because qdrant feature is not enabled"
                    .to_string(),
            )
        }
    }

    #[cfg(feature = "qdrant")]
    pub(crate) async fn compensate_qdrant_upserts(
        &self,
        compensations: &[(String, Vec<String>)],
    ) -> Option<String> {
        if compensations.is_empty() {
            return None;
        }
        let Some(qdrant) = &self.qdrant else {
            return Some(
                "rolled back PostgreSQL; Qdrant compensation skipped because Qdrant is not configured"
                    .to_string(),
            );
        };
        let mut deleted = 0usize;
        let mut failures = Vec::new();
        const MAX_ATTEMPTS: u32 = 3;
        for (collection, point_ids) in compensations.iter().rev() {
            let mut last_err = String::new();
            let mut succeeded = false;
            for attempt in 1..=MAX_ATTEMPTS {
                match qdrant.delete_points(collection, point_ids).await {
                    Ok(()) => {
                        deleted += point_ids.len();
                        succeeded = true;
                        break;
                    }
                    Err(err) => {
                        last_err = format!("{err}");
                        if attempt < MAX_ATTEMPTS {
                            tracing::warn!(
                                collection = %collection,
                                attempt,
                                error = %err,
                                "Qdrant compensation failed; retrying"
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(
                                200 * u64::from(attempt),
                            ))
                            .await;
                        }
                    }
                }
            }
            if !succeeded {
                failures.push(format!(
                    "{collection}: {last_err} (after {MAX_ATTEMPTS} attempts)"
                ));
            }
        }
        if failures.is_empty() {
            Some(format!(
                "rolled back PostgreSQL and compensated {deleted} Qdrant point(s)"
            ))
        } else {
            Some(format!(
                "rolled back PostgreSQL; compensated {deleted} Qdrant point(s); compensation failures: {}",
                failures.join("; ")
            ))
        }
    }

    #[cfg(not(feature = "s3"))]
    pub(crate) async fn compensate_s3_puts(
        &self,
        _compensations: &[(String, String)],
    ) -> Option<String> {
        None
    }

    #[cfg(feature = "s3")]
    pub(crate) async fn compensate_s3_puts(
        &self,
        compensations: &[(String, String)],
    ) -> Option<String> {
        if compensations.is_empty() {
            return None;
        }
        let Some(s3) = &self.s3 else {
            return Some("S3 compensation skipped because S3/MinIO is not configured".to_string());
        };
        let mut deleted = 0usize;
        let mut failures = Vec::new();
        const MAX_ATTEMPTS: u32 = 3;
        for (bucket, object_key) in compensations.iter().rev() {
            let mut last_err = String::new();
            let mut succeeded = false;
            for attempt in 1..=MAX_ATTEMPTS {
                match s3
                    .delete_object()
                    .bucket(bucket)
                    .key(object_key)
                    .send()
                    .await
                {
                    Ok(_) => {
                        deleted += 1;
                        succeeded = true;
                        break;
                    }
                    Err(err) => {
                        last_err = format!("{err}");
                        if attempt < MAX_ATTEMPTS {
                            tracing::warn!(
                                bucket = %bucket,
                                key = %object_key,
                                attempt,
                                error = %err,
                                "S3 compensation failed; retrying"
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(
                                200 * u64::from(attempt),
                            ))
                            .await;
                        }
                    }
                }
            }
            if !succeeded {
                failures.push(format!(
                    "s3://{bucket}/{object_key}: {last_err} (after {MAX_ATTEMPTS} attempts)"
                ));
            }
        }
        if failures.is_empty() {
            Some(format!("compensated {deleted} S3/MinIO object(s)"))
        } else {
            Some(format!(
                "compensated {deleted} S3/MinIO object(s); compensation failures: {}",
                failures.join("; ")
            ))
        }
    }

    pub(crate) async fn compensate_external_side_effects(
        &self,
        qdrant_compensations: &[(String, Vec<String>)],
        s3_compensations: &[(String, String)],
    ) -> Option<String> {
        let mut parts = Vec::new();
        if let Some(message) = self.compensate_qdrant_upserts(qdrant_compensations).await {
            parts.push(message);
        }
        if let Some(message) = self.compensate_s3_puts(s3_compensations).await {
            parts.push(message);
        }
        if parts.is_empty() {
            None
        } else {
            Some(parts.join("; "))
        }
    }

    // ── Probe methods (used by doctor and GetHealthReport) ────────────────────

    /// Probe Redis by sending a PING command.
    #[cfg(feature = "redis")]
    pub async fn probe_redis_ping(&self) -> BackendProbeResult {
        let start = std::time::Instant::now();
        let client = match &self.redis {
            Some(c) => c,
            None => {
                return BackendProbeResult {
                    backend: "redis".into(),
                    ok: false,
                    latency_ms: 0,
                    error: Some("Redis is not configured".into()),
                };
            }
        };
        match client.get_multiplexed_async_connection().await {
            Err(e) => BackendProbeResult {
                backend: "redis".into(),
                ok: false,
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some(format!("connection failed: {e}")),
            },
            Ok(mut conn) => match redis::cmd("PING").query_async::<String>(&mut conn).await {
                Ok(pong) if pong.eq_ignore_ascii_case("PONG") => BackendProbeResult {
                    backend: "redis".into(),
                    ok: true,
                    latency_ms: start.elapsed().as_millis() as u64,
                    error: None,
                },
                Ok(resp) => BackendProbeResult {
                    backend: "redis".into(),
                    ok: false,
                    latency_ms: start.elapsed().as_millis() as u64,
                    error: Some(format!("unexpected PING response: {resp}")),
                },
                Err(e) => BackendProbeResult {
                    backend: "redis".into(),
                    ok: false,
                    latency_ms: start.elapsed().as_millis() as u64,
                    error: Some(format!("PING failed: {e}")),
                },
            },
        }
    }

    /// Probe Qdrant by listing collections endpoint.
    #[cfg(not(feature = "qdrant"))]
    pub async fn probe_qdrant_collections(&self) -> BackendProbeResult {
        BackendProbeResult {
            backend: "qdrant".into(),
            ok: false,
            latency_ms: 0,
            error: Some("qdrant/vector feature is not enabled".into()),
        }
    }

    #[cfg(feature = "qdrant")]
    pub async fn probe_qdrant_collections(&self) -> BackendProbeResult {
        let start = std::time::Instant::now();
        let qdrant = match &self.qdrant {
            Some(q) => q,
            None => {
                return BackendProbeResult {
                    backend: "qdrant".into(),
                    ok: false,
                    latency_ms: 0,
                    error: Some("Qdrant is not configured".into()),
                };
            }
        };
        let url = format!("{}/collections", qdrant.base_url);
        let mut req = qdrant.http.get(&url);
        if let Some(api_key) = &qdrant.api_key {
            req = req.header("api-key", api_key);
        }
        match req.send().await {
            Err(e) => BackendProbeResult {
                backend: "qdrant".into(),
                ok: false,
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some(format!("HTTP request failed: {e}")),
            },
            Ok(resp) => {
                let ok = resp.status().is_success();
                let error = if ok {
                    None
                } else {
                    Some(format!("HTTP {}", resp.status()))
                };
                BackendProbeResult {
                    backend: "qdrant".into(),
                    ok,
                    latency_ms: start.elapsed().as_millis() as u64,
                    error,
                }
            }
        }
    }

    /// Probe S3/MinIO by calling list_buckets.
    #[cfg(feature = "s3")]
    pub async fn probe_s3_access(&self) -> BackendProbeResult {
        let start = std::time::Instant::now();
        let s3 = match &self.s3 {
            Some(s3) => s3,
            None => {
                return BackendProbeResult {
                    backend: "s3".into(),
                    ok: false,
                    latency_ms: 0,
                    error: Some("S3/MinIO is not configured".into()),
                };
            }
        };
        match s3.list_buckets().send().await {
            Ok(_) => BackendProbeResult {
                backend: "s3".into(),
                ok: true,
                latency_ms: start.elapsed().as_millis() as u64,
                error: None,
            },
            Err(e) => BackendProbeResult {
                backend: "s3".into(),
                ok: false,
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some(format!("list_buckets failed: {e}")),
            },
        }
    }

    /// Probe Kafka by fetching broker metadata with a short-lived consumer.
    #[cfg(feature = "kafka")]
    pub fn probe_kafka_metadata(&self) -> BackendProbeResult {
        let start = std::time::Instant::now();
        let brokers = match self
            .config
            .kafka_brokers
            .clone()
            .filter(|v| !v.trim().is_empty())
        {
            Some(value) => value,
            None => {
                return BackendProbeResult {
                    backend: "kafka".into(),
                    ok: false,
                    latency_ms: 0,
                    error: Some(
                        "UDB_KAFKA_BROKERS / KAFKA_BROKERS is not set; CDC tailer disabled".into(),
                    ),
                };
            }
        };
        let consumer: BaseConsumer = match ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", "udb-doctor")
            .set("enable.auto.commit", "false")
            .set("session.timeout.ms", "6000")
            .create()
        {
            Ok(consumer) => consumer,
            Err(err) => {
                return BackendProbeResult {
                    backend: "kafka".into(),
                    ok: false,
                    latency_ms: start.elapsed().as_millis() as u64,
                    error: Some(format!("Kafka consumer init failed: {err}")),
                };
            }
        };
        match consumer.fetch_metadata(None, Duration::from_secs(3)) {
            Ok(metadata) => BackendProbeResult {
                backend: "kafka".into(),
                ok: !metadata.brokers().is_empty(),
                latency_ms: start.elapsed().as_millis() as u64,
                error: if metadata.brokers().is_empty() {
                    Some("Kafka metadata returned no brokers".into())
                } else {
                    None
                },
            },
            Err(err) => BackendProbeResult {
                backend: "kafka".into(),
                ok: false,
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some(format!("Kafka metadata fetch failed: {err}")),
            },
        }
    }
}
