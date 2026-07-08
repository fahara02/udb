//! Continuation `impl DataBrokerRuntime` block (Phase F split of core.rs).
use super::*;
#[cfg(any(feature = "mongodb", feature = "neo4j", feature = "clickhouse"))]
use crate::runtime::executors::BackendHealth;

fn probe_dispatch_invalid_field(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [(field.into(), description.into())],
    )
}

fn outbox_topic_not_allowed_status(topic: &str) -> tonic::Status {
    probe_dispatch_invalid_field(
        "topic",
        "must be allowed by topic policy or UDB_CDC_VALID_TOPICS",
        format!(
            "topic '{topic}' is not in the registered topic registry; \
                 configure UDB_CDC_VALID_TOPICS or use an allowed topic"
        ),
    )
}

fn unknown_probe_backend_status(backend: &str) -> tonic::Status {
    probe_dispatch_invalid_field(
        "backend",
        "must be one of postgres, redis, mongodb, neo4j, clickhouse, qdrant, s3, or minio",
        format!(
            "unknown backend '{backend}'; valid: postgres, redis, mongodb, neo4j, clickhouse, qdrant, s3, minio"
        ),
    )
}

fn probe_backend_not_configured_status(
    backend: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> tonic::Status {
    crate::runtime::executor_utils::capability_status(backend, "ping", capability_required, message)
}

fn probe_dispatch_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status("probe_dispatch", operation, message)
}

impl DataBrokerRuntime {
    pub fn configured_probe_backends(
        &self,
        include_postgres: bool,
    ) -> Vec<crate::backend::BackendKind> {
        use crate::backend::BackendKind;

        let init = self.init_report();
        BackendKind::all_known()
            .iter()
            .cloned()
            .filter(|kind| kind.has_runtime_probe())
            .filter(|kind| match kind {
                BackendKind::Postgres => include_postgres && init.postgres_configured,
                BackendKind::Redis => init.redis_configured,
                BackendKind::Qdrant => init.qdrant_configured,
                BackendKind::Minio | BackendKind::S3 => init.s3_configured,
                BackendKind::Mongodb => init.mongodb_configured,
                BackendKind::Neo4j => init.neo4j_configured,
                BackendKind::Clickhouse => init.clickhouse_configured,
                _ => false,
            })
            .collect()
    }

    pub async fn probe_backend(&self, kind: crate::backend::BackendKind) -> BackendProbeResult {
        use crate::backend::BackendKind;

        let mut result = match &kind {
            BackendKind::Postgres => self.probe_postgres().await,
            #[cfg(feature = "redis")]
            BackendKind::Redis => self.probe_redis_ping().await,
            BackendKind::Qdrant => self.probe_qdrant_collections().await,
            #[cfg(feature = "s3")]
            BackendKind::Minio | BackendKind::S3 => self.probe_s3_access().await,
            BackendKind::Mongodb => self.probe_mongodb_ping().await,
            BackendKind::Neo4j => self.probe_neo4j_ping().await,
            BackendKind::Clickhouse => self.probe_clickhouse_ping().await,
            _ => BackendProbeResult {
                backend: kind.as_str().to_string(),
                ok: false,
                latency_ms: 0,
                error: Some("backend has no live runtime probe".to_string()),
            },
        };
        result.backend = kind.as_str().to_string();
        result
    }

    /// Probe MongoDB Atlas Data API by calling the `ping` command.
    #[cfg(not(feature = "mongodb"))]
    pub async fn probe_mongodb_ping(&self) -> BackendProbeResult {
        BackendProbeResult {
            backend: "mongodb".into(),
            ok: false,
            latency_ms: 0,
            error: Some("mongodb feature is not enabled".into()),
        }
    }

    #[cfg(feature = "mongodb")]
    pub async fn probe_mongodb_ping(&self) -> BackendProbeResult {
        let start = std::time::Instant::now();
        let executor = match &self.mongodb {
            Some(e) => e,
            None => {
                return BackendProbeResult {
                    backend: "mongodb".into(),
                    ok: false,
                    latency_ms: 0,
                    error: Some("MongoDB is not configured".into()),
                };
            }
        };
        match executor.ping().await {
            Ok(()) => BackendProbeResult {
                backend: "mongodb".into(),
                ok: true,
                latency_ms: start.elapsed().as_millis() as u64,
                error: None,
            },
            Err(e) => BackendProbeResult {
                backend: "mongodb".into(),
                ok: false,
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some(e),
            },
        }
    }

    /// Probe Neo4j by issuing a lightweight Cypher query via the REST API.
    #[cfg(not(feature = "neo4j"))]
    pub async fn probe_neo4j_ping(&self) -> BackendProbeResult {
        BackendProbeResult {
            backend: "neo4j".into(),
            ok: false,
            latency_ms: 0,
            error: Some("neo4j feature is not enabled".into()),
        }
    }

    #[cfg(feature = "neo4j")]
    pub async fn probe_neo4j_ping(&self) -> BackendProbeResult {
        let start = std::time::Instant::now();
        let executor = match &self.neo4j {
            Some(e) => e,
            None => {
                return BackendProbeResult {
                    backend: "neo4j".into(),
                    ok: false,
                    latency_ms: 0,
                    error: Some("Neo4j is not configured".into()),
                };
            }
        };
        match executor.ping().await {
            Ok(()) => BackendProbeResult {
                backend: "neo4j".into(),
                ok: true,
                latency_ms: start.elapsed().as_millis() as u64,
                error: None,
            },
            Err(e) => BackendProbeResult {
                backend: "neo4j".into(),
                ok: false,
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some(e),
            },
        }
    }

    /// Probe ClickHouse by issuing `SELECT 1`.
    #[cfg(not(feature = "clickhouse"))]
    pub async fn probe_clickhouse_ping(&self) -> BackendProbeResult {
        BackendProbeResult {
            backend: "clickhouse".into(),
            ok: false,
            latency_ms: 0,
            error: Some("clickhouse feature is not enabled".into()),
        }
    }

    #[cfg(feature = "clickhouse")]
    pub async fn probe_clickhouse_ping(&self) -> BackendProbeResult {
        let start = std::time::Instant::now();
        let executor = match &self.clickhouse {
            Some(e) => e,
            None => {
                return BackendProbeResult {
                    backend: "clickhouse".into(),
                    ok: false,
                    latency_ms: 0,
                    error: Some("ClickHouse is not configured".into()),
                };
            }
        };
        match executor.ping().await {
            Ok(()) => BackendProbeResult {
                backend: "clickhouse".into(),
                ok: true,
                latency_ms: start.elapsed().as_millis() as u64,
                error: None,
            },
            Err(e) => BackendProbeResult {
                backend: "clickhouse".into(),
                ok: false,
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some(e),
            },
        }
    }

    /// Probe PostgreSQL with a lightweight round-trip query.
    pub async fn probe_postgres(&self) -> BackendProbeResult {
        let start = std::time::Instant::now();
        let pool = match &self.pg_pool {
            Some(p) => p,
            None => {
                return BackendProbeResult {
                    backend: "postgres".into(),
                    ok: false,
                    latency_ms: 0,
                    error: Some("PostgreSQL is not configured".into()),
                };
            }
        };
        match sqlx::query("SELECT 1 AS udb_probe").fetch_one(pool).await {
            Ok(_) => BackendProbeResult {
                backend: "postgres".into(),
                ok: true,
                latency_ms: start.elapsed().as_millis() as u64,
                error: None,
            },
            Err(e) => BackendProbeResult {
                backend: "postgres".into(),
                ok: false,
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some(format!("probe failed: {e}")),
            },
        }
    }

    /// Check PostgreSQL privileges required by UDB (CREATE SCHEMA, CREATE TABLE,
    /// CREATE PUBLICATION, replication slot access, advisory lock access).
    pub async fn check_postgres_privileges(&self) -> PostgresPrivilegeReport {
        let pool = match &self.pg_pool {
            Some(p) => p,
            None => {
                return PostgresPrivilegeReport {
                    checked: false,
                    errors: vec!["PostgreSQL is not configured".into()],
                    ..Default::default()
                };
            }
        };
        let mut report = PostgresPrivilegeReport {
            checked: true,
            ..Default::default()
        };

        // CREATE SCHEMA — needs CREATE on the current database
        match sqlx::query_scalar::<_, bool>(
            "SELECT has_database_privilege(current_user, current_database(), 'CREATE')",
        )
        .fetch_one(pool)
        .await
        {
            Ok(v) => report.create_schema = v,
            Err(e) => report.errors.push(format!("CREATE SCHEMA check: {e}")),
        }

        // CREATE TABLE — needs CREATE on the configured system schema (or public).
        let sys_schema = crate::runtime::system::SystemCatalogConfig::default()
            .cdc
            .system_schema;
        let create_table_sql = "SELECT coalesce(\
                 (SELECT has_schema_privilege(current_user, $1, 'CREATE')), FALSE) \
             OR coalesce(\
                 (SELECT has_schema_privilege(current_user, 'public', 'CREATE')), FALSE)";
        match sqlx::query_scalar::<_, bool>(create_table_sql)
            .bind(&sys_schema)
            .fetch_one(pool)
            .await
        {
            Ok(v) => report.create_table = v,
            Err(e) => report.errors.push(format!("CREATE TABLE check: {e}")),
        }

        // CREATE PUBLICATION — requires superuser or replication role
        // pg_publication_admin (PG16+) also qualifies; fall back gracefully.
        let pub_sql = "SELECT (rolsuper OR rolreplication) AS ok \
                       FROM pg_roles WHERE rolname = current_user";
        match sqlx::query_scalar::<_, bool>(pub_sql)
            .fetch_optional(pool)
            .await
        {
            Ok(Some(v)) => report.create_publication = v,
            Ok(None) => {
                // Role not found — try pg_publication_admin (PG16+) via pg_has_role
                let fallback = "SELECT pg_has_role(current_user, 'pg_publication_admin', 'USAGE')";
                match sqlx::query_scalar::<_, bool>(fallback)
                    .fetch_one(pool)
                    .await
                {
                    Ok(v) => report.create_publication = v,
                    Err(_) => report.create_publication = false,
                }
            }
            Err(e) => report.errors.push(format!("CREATE PUBLICATION check: {e}")),
        }

        // Replication slot — requires superuser or rolreplication
        match sqlx::query_scalar::<_, bool>(
            "SELECT (rolsuper OR rolreplication) AS ok FROM pg_roles WHERE rolname = current_user",
        )
        .fetch_optional(pool)
        .await
        {
            Ok(Some(v)) => report.replication_slot = v,
            Ok(None) => report.replication_slot = false,
            Err(e) => report.errors.push(format!("Replication slot check: {e}")),
        }

        // Advisory lock — try to acquire and immediately release.
        // hashtext() returns INT4; cast to BIGINT for pg_try_advisory_lock.
        match sqlx::query_scalar::<_, bool>(
            "SELECT pg_try_advisory_lock(hashtext('udb_runtime')::BIGINT)",
        )
        .fetch_one(pool)
        .await
        {
            Ok(acquired) => {
                report.advisory_lock = acquired;
                if acquired {
                    let _ =
                        sqlx::query("SELECT pg_advisory_unlock(hashtext('udb_runtime')::BIGINT)")
                            .execute(pool)
                            .await;
                }
            }
            Err(e) => report.errors.push(format!("Advisory lock check: {e}")),
        }

        report
    }

    // ── Admin API helpers ─────────────────────────────────────────────────────

    /// Return a JSON-serialisable representation of the loaded catalog manifest,
    /// optionally redacting PII field metadata.
    pub fn catalog_manifest_json(
        &self,
        manifest: &CatalogManifest,
        redact: bool,
    ) -> serde_json::Value {
        if redact {
            // Produce a redacted manifest: strip column-level encryption/PII flags.
            let tables: Vec<serde_json::Value> = manifest
                .tables
                .iter()
                .map(|t| {
                    serde_json::json!({
                        "table": t.table,
                        "schema": t.schema,
                        "message_type": t.message_name,
                        "column_count": t.columns.len(),
                        "has_pii": t.columns.iter().any(|c| c.security.is_pii),
                    })
                })
                .collect();
            serde_json::json!({
                "table_count": manifest.tables.len(),
                "store_count": manifest.stores.len(),
                "tables": tables,
            })
        } else {
            serde_json::to_value(manifest).unwrap_or(serde_json::Value::Null)
        }
    }

    /// Persist saga state at the start of a `BeginTx` call and return the saga_id.
    pub(crate) async fn saga_begin(
        &self,
        tx_id: &str,
        mutation_count: usize,
        tenant_id: &str,
        correlation_id: &str,
        operation: &str,
        backend_instance: &str,
    ) -> Option<String> {
        let pool = self.pg_pool.as_ref()?;
        let config = crate::runtime::system::SystemCatalogConfig::default();
        let saga_id = Uuid::new_v4().to_string();
        let saga_relation = format!("\"{}\".\"{}\"", config.cdc.system_schema, config.saga_table);
        // Store tenant_id and correlation_id so the saga index on (tenant_id, status)
        // can serve operational queries without a full table scan.
        let sql = format!(
            "INSERT INTO {saga_relation} \
             (saga_id, tx_id, tenant_id, correlation_id, backend_instance, operation, retry_count, compensation_status, steps, current_step, status, compensations, created_at) \
             VALUES ($1::UUID, $2, $3, $4, $5, $6, 0, 'none', '[]'::JSONB, 0, 'in_progress', '[]'::JSONB, NOW()) \
             ON CONFLICT (saga_id) DO NOTHING"
        );
        match sqlx::query(&sql)
            .bind(&saga_id)
            .bind(tx_id)
            .bind(tenant_id)
            .bind(correlation_id)
            .bind(backend_instance)
            .bind(operation)
            .execute(pool)
            .await
        {
            Ok(_) => {
                tracing::debug!(
                    saga_id = saga_id,
                    tx_id = tx_id,
                    mutation_count = mutation_count,
                    "saga started"
                );
                Some(saga_id)
            }
            Err(e) => {
                tracing::warn!(tx_id = tx_id, error = %e, "failed to persist saga state; continuing without saga tracking");
                None
            }
        }
    }

    /// Append a step record to the saga in the database.
    pub(crate) async fn saga_record_step(
        &self,
        saga_id: &str,
        step_index: usize,
        operation: &str,
        message_type: &str,
        compensation: &str,
    ) {
        let pool = match &self.pg_pool {
            Some(p) => p,
            None => return,
        };
        let config = crate::runtime::system::SystemCatalogConfig::default();
        let saga_relation = format!("\"{}\".\"{}\"", config.cdc.system_schema, config.saga_table);
        let compensation_value = serde_json::from_str::<serde_json::Value>(compensation)
            .unwrap_or_else(|_| serde_json::json!({"legacy_descriptor": compensation}));
        let compensation_backend = compensation_value
            .get("backend")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown");
        let compensation_operation = compensation_value
            .get("operation")
            .and_then(|value| value.as_str())
            .unwrap_or("noop");
        let compensation_resource_uri = compensation_value
            .get("resource_uri")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let step = serde_json::json!({
            "step": step_index,
            "operation": operation,
            "message_type": message_type,
            "compensation": compensation_value,
        });
        let compensation_json = serde_json::json!({
            "step": step_index,
            "backend": compensation_backend,
            "operation": compensation_operation,
            "message_type": message_type,
            "resource_uri": compensation_resource_uri,
            "payload": compensation_value,
        });
        let sql = format!(
            "UPDATE {saga_relation} \
             SET steps = steps || $1::JSONB,
                 compensations = compensations || $2::JSONB,
                 current_step = $3,
                 updated_at = NOW() \
             WHERE saga_id = $4::UUID"
        );
        if let Err(e) = sqlx::query(&sql)
            .bind(step.to_string())
            .bind(compensation_json.to_string())
            .bind(step_index as i32)
            .bind(saga_id)
            .execute(pool)
            .await
        {
            tracing::warn!(saga_id = saga_id, step = step_index, error = %e, "failed to record saga step");
        }
    }

    /// Update the saga terminal status (COMMITTED, COMPENSATED, FAILED, INDETERMINATE).
    pub(crate) async fn saga_set_status(&self, saga_id: &str, status: &str) {
        let pool = match &self.pg_pool {
            Some(p) => p,
            None => return,
        };
        let config = crate::runtime::system::SystemCatalogConfig::default();
        let saga_relation = format!("\"{}\".\"{}\"", config.cdc.system_schema, config.saga_table);
        let compensation_status = match status {
            "compensated" => "completed",
            "failed_compensation" => "failed",
            "manual_review" => "manual_review",
            _ => "none",
        };
        let sql = format!(
            "UPDATE {saga_relation}
             SET status = $1, compensation_status = $2, updated_at = NOW()
             WHERE saga_id = $3::UUID"
        );
        if let Err(e) = sqlx::query(&sql)
            .bind(status)
            .bind(compensation_status)
            .bind(saga_id)
            .execute(pool)
            .await
        {
            tracing::warn!(saga_id = saga_id, status = status, error = %e, "failed to update saga status");
        }
    }

    /// On startup: mark any sagas left IN_PROGRESS from a previous crash as INDETERMINATE.
    pub async fn mark_indeterminate_sagas(&self) {
        let pool = match &self.pg_pool {
            Some(p) => p,
            None => return,
        };
        let config = crate::runtime::system::SystemCatalogConfig::default();
        let saga_relation = format!("\"{}\".\"{}\"", config.cdc.system_schema, config.saga_table);
        let sql = format!(
            "UPDATE {saga_relation} SET status = 'indeterminate', updated_at = NOW() WHERE status = 'in_progress'"
        );
        match sqlx::query(&sql).execute(pool).await {
            Ok(result) => {
                let count = result.rows_affected();
                if count > 0 {
                    tracing::warn!(
                        count = count,
                        "marked {count} in-progress saga(s) as INDETERMINATE after crash recovery"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to mark indeterminate sagas on startup");
            }
        }
    }

    // Transaction semantics are explicit:
    // - relational-only mutations remain a single PostgreSQL ACID transaction;
    // - any vector/object/backend side effect is tracked as a cross-backend saga
    //   and compensated best-effort if a later step fails.

    /// Enqueue an event directly into the outbox table without going through BeginTx.
    /// Enforces neutral envelope validation and idempotency.
    pub async fn enqueue_outbox_event(
        &self,
        topic: &str,
        partition_key: &str,
        payload: serde_json::Value,
        schema_uri: Option<&str>,
        idempotency_key: Option<&str>,
        valid_topics: &[String],
        context: &RequestContext,
    ) -> Result<EnqueueOutboxEventResult, tonic::Status> {
        let pool = self.pg_pool()?;

        // Validate topic against the database policy table when populated, then
        // fall back to the env-provided allow list for smaller deployments.
        let policy_decision = self
            .topic_policy_allows(topic, &context.project_id, &context.tenant_id)
            .await?;
        if policy_decision == Some(false)
            || (policy_decision.is_none()
                && !valid_topics.is_empty()
                && !valid_topics.iter().any(|t| t == topic))
        {
            return Err(outbox_topic_not_allowed_status(topic));
        }

        let (event_id_uuid, event_id, enriched) =
            prepare_outbox_envelope(topic, partition_key, payload, schema_uri)?;

        // Idempotency key check (Redis; degrades gracefully if Redis unavailable).
        if let Some(ikey) = idempotency_key
            && !ikey.is_empty()
        {
            let redis_key = format!("idempotency:udb-enqueue:{ikey}");
            #[cfg(not(feature = "redis"))]
            let _ = &redis_key;
            #[cfg(feature = "redis")]
            if let Some(client) = &self.redis {
                match client.get_multiplexed_async_connection().await {
                    Ok(mut conn) => {
                        let exists: Option<String> = conn.get(&redis_key).await.unwrap_or(None);
                        if let Some(event_id) = exists {
                            return Ok(EnqueueOutboxEventResult {
                                event_id,
                                enqueued: false,
                                was_duplicate: true,
                            });
                        }
                    }
                    Err(e) => {
                        // KEYSTONE (lane 05): fail CLOSED, not open. This block is
                        // reached ONLY for a keyed enqueue (gated by `Some(ikey)` +
                        // `!ikey.is_empty()`), so refusing here cannot regress the
                        // keyless hot path. Proceeding without the dedup guard would
                        // risk a silent duplicate enqueue on a retry — the integrity
                        // trade is: lose availability on keyed enqueues when the
                        // dedup store is down, preserve exactly-once intent.
                        tracing::warn!(
                            idempotency_key = ikey,
                            error = %e,
                            "Redis unavailable for idempotency check; refusing keyed enqueue (fail-closed)"
                        );
                        return Err(crate::runtime::executor_utils::retryable_status(
                            "redis",
                            "idempotency_dedup_check",
                            crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                            "idempotency dedup store unavailable; keyed enqueue refused (fail-closed)",
                        ));
                    }
                }
            }
        }
        let cdc_config = self.config.cdc.clone();

        let outbox_relation = cdc_config.outbox_relation();
        // ONE shared insert path (`cdc::insert_outbox_row`) — same SQL + bind types as
        // every other outbox writer, so the §4 prepared-statement collision can't recur.
        crate::runtime::cdc::insert_outbox_row(
            pool,
            &outbox_relation,
            event_id_uuid,
            topic,
            partition_key,
            &enriched,
        )
        .await
        .map_err(|e| {
            probe_dispatch_internal_status(
                "enqueue_outbox_event",
                format!("failed to enqueue event: {e}"),
            )
        })?;

        // Store idempotency key in Redis with 7-day TTL.
        if let Some(ikey) = idempotency_key
            && !ikey.is_empty()
        {
            let redis_key = format!("idempotency:udb-enqueue:{ikey}");
            #[cfg(not(feature = "redis"))]
            let _ = &redis_key;
            #[cfg(feature = "redis")]
            if let Some(client) = &self.redis
                && let Ok(mut conn) = client.get_multiplexed_async_connection().await
            {
                // KEYSTONE (lane 05): the outbox row is ALREADY inserted above
                // (pool-based, not a tx — it cannot be un-inserted here), so a
                // failed set_ex leaves this key untracked → a later replay would
                // double-insert. That is the irreducible gap of the pool-based
                // outbox dedup. We no longer silently discard the result: log at
                // error! with the key so the lost-tracking is observable, rather
                // than claiming the duplicate guard was written when it was not.
                let store_result: redis::RedisResult<()> = conn
                    .set_ex(&redis_key, &event_id, self.config.cdc.idempotency_ttl_secs)
                    .await;
                if let Err(e) = store_result {
                    tracing::error!(
                        idempotency_key = ikey,
                        event_id = %event_id,
                        error = %e,
                        "failed to persist idempotency guard after enqueue; key is now untracked — a retry may double-insert"
                    );
                }
            }
        }

        tracing::info!(
            event_id = event_id,
            topic = topic,
            partition_key = partition_key,
            "outbox event enqueued"
        );

        Ok(EnqueueOutboxEventResult {
            event_id,
            enqueued: true,
            was_duplicate: false,
        })
    }

    pub(crate) async fn topic_policy_allows(
        &self,
        topic: &str,
        project_id: &str,
        tenant_id: &str,
    ) -> Result<Option<bool>, tonic::Status> {
        use crate::runtime::system::SystemCatalogConfig;
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::default();
        let policy_rel = config.topic_policy_relation();
        let rows = sqlx::query(&format!(
            "SELECT topic, tenant_id, owning_project, enabled FROM {policy_rel} ORDER BY created_at ASC"
        ))
        .fetch_all(pool)
        .await
        .map_err(|err| {
            probe_dispatch_internal_status(
                "topic_policy_allows",
                format!("topic policy query failed: {err}"),
            )
        })?;
        if rows.is_empty() {
            return Ok(None);
        }
        let allowed = rows.iter().any(|row| {
            let pattern = row.try_get::<String, _>("topic").unwrap_or_default();
            let policy_tenant = row
                .try_get::<String, _>("tenant_id")
                .unwrap_or_else(|_| "*".to_string());
            let owning_project = row
                .try_get::<String, _>("owning_project")
                .unwrap_or_default();
            let enabled = row.try_get::<bool, _>("enabled").unwrap_or(false);
            enabled
                && wildmatch::WildMatch::new(&pattern).matches(topic)
                && (policy_tenant == "*" || policy_tenant == tenant_id)
                && (owning_project.is_empty()
                    || owning_project == "*"
                    || owning_project == project_id)
        });
        Ok(Some(allowed))
    }

    // ── Phase 0.2 — Generic resource admin ───────────────────────────────────

    /// Ping a backend to check connectivity.
    pub async fn ping_backend(&self, backend: &str) -> Result<(), tonic::Status> {
        self.ping_backend_target(backend, None).await
    }

    pub async fn ping_backend_target(
        &self,
        backend: &str,
        instance: Option<&str>,
    ) -> Result<(), tonic::Status> {
        match backend {
            "postgres" | "pg" | "postgresql" => {
                self.pg_pool_for_instance(instance)?
                    .acquire()
                    .await
                    .map_err(|err| {
                        crate::runtime::executor_utils::backend_transport_status(
                            "postgres", "ping", err,
                        )
                    })?;
                Ok(())
            }
            #[cfg(feature = "redis")]
            "redis" => {
                let redis = self.redis_for_instance(instance)?;
                redis
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|err| {
                        crate::runtime::executor_utils::backend_transport_status(
                            "redis", "ping", err,
                        )
                    })?;
                Ok(())
            }
            #[cfg(feature = "mongodb")]
            "mongodb" | "mongo" => {
                if self.mongodb_for_instance(instance).is_ok() {
                    Ok(())
                } else {
                    Err(probe_backend_not_configured_status(
                        "mongodb",
                        "mongodb_backend",
                        "mongodb not configured",
                    ))
                }
            }
            #[cfg(feature = "neo4j")]
            "neo4j" => {
                if self.neo4j_for_instance(instance).is_ok() {
                    Ok(())
                } else {
                    Err(probe_backend_not_configured_status(
                        "neo4j",
                        "neo4j_backend",
                        "neo4j not configured",
                    ))
                }
            }
            #[cfg(feature = "clickhouse")]
            "clickhouse" => {
                if self.clickhouse_for_instance(instance).is_ok() {
                    Ok(())
                } else {
                    Err(probe_backend_not_configured_status(
                        "clickhouse",
                        "clickhouse_backend",
                        "clickhouse not configured",
                    ))
                }
            }
            #[cfg(feature = "qdrant")]
            "qdrant" => {
                let client = self.qdrant_for_instance(instance).map_err(|_| {
                    probe_backend_not_configured_status(
                        "qdrant",
                        "qdrant_backend",
                        "qdrant not configured",
                    )
                })?;
                let qexec = QdrantExecutor(client.clone());
                <QdrantExecutor as crate::runtime::executors::BackendHealth>::ping(&qexec)
                    .await
                    .map_err(|err| {
                        crate::runtime::executor_utils::backend_transport_status(
                            "qdrant", "ping", err,
                        )
                    })
            }
            #[cfg(feature = "s3")]
            "s3" | "minio" => {
                if self.s3_for_instance(instance).is_ok() {
                    Ok(())
                } else {
                    Err(probe_backend_not_configured_status(
                        "s3",
                        "s3_backend",
                        "s3/minio not configured",
                    ))
                }
            }
            other => Err(unknown_probe_backend_status(other)),
        }
    }

    /// Phase E — registry-style resolution of a `(backend, instance)` target to a
    /// ready `DispatchExecutor`, replacing the per-operation `match backend` arms.
    /// This is the **single** dispatch decision point; the per-op `*_backend_target`
    /// methods just call the matching trait method on the returned handle. Per §9.1
    /// it stays in orchestration: it selects the pool/instance (incl. write-instance
    /// routing for mutations) and hands a stateless leaf executor to the caller —
    /// replica/cache/encryption/channel/breaker logic lives in the typed RPC paths,
    /// not here.
    pub(crate) fn resolve_dispatch_executor(
        &self,
        backend: &str,
        instance: Option<&str>,
        write: bool,
        unknown_code: tonic::Code,
        context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::DispatchExecutor, tonic::Status> {
        use crate::backend::BackendKind;
        // Normalise dispatch aliases to typed identity (preserves the old arms'
        // `pg`/`postgresql`/`mongo` tolerance) — see [[backend-token-contract]].
        // Once aliases are unified into `BackendKind::from_token`, this can drop
        // to just `from_token(backend)`.
        let kind = match backend {
            "postgres" | "pg" | "postgresql" => Some(BackendKind::Postgres),
            "mongodb" | "mongo" => Some(BackendKind::Mongodb),
            other => BackendKind::from_token(other),
        };
        // U2 step 5: the per-backend construction match has moved into each
        // plugin's `DispatchFactory` impl. The resolver looks the factory up
        // by `BackendKind` and delegates; adding a backend means one plugin
        // module + one factory impl, no edits here.
        //
        // U7: `context` is forwarded to the factory so the Postgres plugin can
        // bake it into the executor (transaction-scoped `set_request_local_settings`).
        // Other plugins ignore it.
        let factory = kind
            .as_ref()
            .and_then(crate::runtime::executors::handle::dispatch_factory_for)
            .ok_or_else(|| {
                tonic::Status::new(
                    unknown_code,
                    format!("backend '{backend}' has no generic-dispatch executor"),
                )
            })?;
        let executor = factory.build_dispatch_executor(self, instance, write, context)?;
        // Item 4: record the backend's RequestContext enforcement posture for the
        // request we just resolved. With a context present this reports
        // Enforced/Advisory/Unsupported per backend; probe/admin paths (no
        // context) yield Advisory (no-op). Emitted to tracing so operators can
        // observe RLS posture without changing the dispatch result.
        if let Some(ctx) = context {
            use crate::runtime::backend_context::BackendContextEnforcer;
            let applied = crate::runtime::backend_context::AppliedContext::from_request(ctx);
            let effect = executor.enforce(&applied);
            tracing::debug!(
                backend = %backend,
                effect = ?effect,
                "backend context enforcement evaluated for dispatch"
            );
        }
        Ok(executor)
    }

    /// Execute a generic read/query operation against a configured backend.
    ///
    /// `request_json` is intentionally backend-shaped but stable:
    /// - ClickHouse: `{"sql":"SELECT ..."}`
    /// - MongoDB: `{"collection":"name","filter":{},"projection":{},"limit":100}`
    /// - Neo4j: `{"cypher":"MATCH ...","parameters":{}}` or
    ///   `{"label":"Patient","filter":{},"limit":100}`
    pub async fn query_backend(
        &self,
        backend: &str,
        request_json: &str,
    ) -> Result<String, tonic::Status> {
        self.query_backend_target(backend, None, request_json).await
    }

    pub async fn query_backend_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        request_json: &str,
    ) -> Result<String, tonic::Status> {
        // Validate well-formed JSON before dispatch; each executor re-parses shape.
        parse_dispatch_json(request_json)?;
        use crate::runtime::executors::QueryExecutor;
        QueryExecutor::query(
            &self.resolve_dispatch_executor(
                backend,
                instance,
                false,
                tonic::Code::FailedPrecondition,
                // Probe/admin paths: no request context, so RLS settings
                // aren't installed (these are infrastructure calls).
                None,
            )?,
            request_json,
        )
        .await
    }

    /// Execute a generic write/mutation operation against a configured backend.
    ///
    /// Supported shapes:
    /// - ClickHouse: `{"sql":"CREATE ..."}`
    ///   or `{"table":"events","rows":[{...}]}`
    /// - MongoDB: `{"operation":"insert|update|delete", ...}`
    /// - Neo4j: `{"operation":"cypher|create_node|update_node|delete_node|create_relationship", ...}`
    pub async fn mutate_backend(
        &self,
        backend: &str,
        request_json: &str,
    ) -> Result<String, tonic::Status> {
        self.mutate_backend_target(backend, None, request_json)
            .await
    }

    pub async fn mutate_backend_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        request_json: &str,
    ) -> Result<String, tonic::Status> {
        // Validate well-formed JSON before dispatch; each executor re-parses shape.
        parse_dispatch_json(request_json)?;
        use crate::runtime::executors::MutationExecutor;
        // `write = true` applies write-instance routing inside the resolver.
        MutationExecutor::mutate(
            &self.resolve_dispatch_executor(
                backend,
                instance,
                true,
                tonic::Code::FailedPrecondition,
                // Probe/admin paths: no request context, so RLS settings
                // aren't installed (these are infrastructure calls).
                None,
            )?,
            request_json,
        )
        .await
    }

    /// Execute a generic vector search operation.
    ///
    /// Qdrant request JSON accepts:
    /// `collection`, `vector`, `filter`, `limit`, `score_threshold`,
    /// `with_payload`, optional `text_query`, and optional `fusion_weights`.
    pub async fn search_backend(
        &self,
        backend: &str,
        request_json: &str,
    ) -> Result<String, tonic::Status> {
        self.search_backend_target(backend, None, request_json)
            .await
    }

    pub async fn search_backend_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        request_json: &str,
    ) -> Result<String, tonic::Status> {
        // Validate the request is well-formed JSON before dispatch; the executor
        // re-parses the backend-specific shape.
        parse_dispatch_json(request_json)?;
        use crate::runtime::executors::SearchExecutor;
        SearchExecutor::search(
            &self.resolve_dispatch_executor(
                backend,
                instance,
                false,
                tonic::Code::FailedPrecondition,
                // Probe/admin paths: no request context, so RLS settings
                // aren't installed (these are infrastructure calls).
                None,
            )?,
            request_json,
        )
        .await
    }

    /// Read an object from an object-store backend.
    pub async fn get_object_backend(
        &self,
        backend: &str,
        request_json: &str,
    ) -> Result<Vec<u8>, tonic::Status> {
        self.get_object_backend_target(backend, None, request_json)
            .await
    }

    pub async fn get_object_backend_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        request_json: &str,
    ) -> Result<Vec<u8>, tonic::Status> {
        self.get_object_backend_target_for_project(
            backend,
            instance,
            crate::runtime::catalog::DEFAULT_PROJECT_ID,
            request_json,
        )
        .await
    }

    pub async fn get_object_backend_target_for_project(
        &self,
        backend: &str,
        instance: Option<&str>,
        project_id: &str,
        request_json: &str,
    ) -> Result<Vec<u8>, tonic::Status> {
        // Validate well-formed JSON before dispatch; the executor re-parses shape.
        parse_dispatch_json(request_json)?;
        use crate::runtime::executors::ObjectExecutor;
        #[cfg(not(feature = "s3"))]
        let _ = project_id;
        #[cfg(feature = "s3")]
        if matches!(backend, "s3" | "minio") {
            let project = project_id.trim();
            let project = if project.is_empty() {
                crate::runtime::catalog::DEFAULT_PROJECT_ID
            } else {
                project
            };
            let target_instance = instance
                .filter(|value| !value.trim().is_empty())
                .or_else(|| {
                    self.choose_instance_name_for_project("minio", false, project)
                        .or_else(|| self.choose_instance_name_for_project("s3", false, project))
                });
            return ObjectExecutor::get_object(
                &crate::runtime::executors::s3::S3Executor(
                    self.s3_for_instance_for_project(target_instance, project)?
                        .clone(),
                ),
                request_json,
            )
            .await;
        }
        ObjectExecutor::get_object(
            &self.resolve_dispatch_executor(
                backend,
                instance,
                false,
                tonic::Code::FailedPrecondition,
                // Probe/admin paths: no request context, so RLS settings
                // aren't installed (these are infrastructure calls).
                None,
            )?,
            request_json,
        )
        .await
    }

    /// Stream an object's bytes from an object-store backend WITHOUT buffering
    /// the whole object in UDB. Mirrors [`get_object_backend_target`] but returns
    /// the executor's bounded byte stream (the same primitive the data-plane
    /// `GetObject` uses) so callers can forward chunks straight to the wire.
    /// Used by the native `StorageService::DownloadFile` fallback.
    pub async fn get_object_stream_backend_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        project_id: &str,
        request_json: &str,
    ) -> Result<crate::runtime::executors::ExecutorByteStream, tonic::Status> {
        // Validate well-formed JSON before dispatch; the executor re-parses shape.
        parse_dispatch_json(request_json)?;
        use crate::runtime::executors::ObjectExecutor;
        #[cfg(not(feature = "s3"))]
        let _ = project_id;
        #[cfg(feature = "s3")]
        if matches!(backend, "s3" | "minio") {
            let project = project_id.trim();
            let project = if project.is_empty() {
                crate::runtime::catalog::DEFAULT_PROJECT_ID
            } else {
                project
            };
            let target_instance = instance
                .filter(|value| !value.trim().is_empty())
                .or_else(|| {
                    self.choose_instance_name_for_project("minio", false, project)
                        .or_else(|| self.choose_instance_name_for_project("s3", false, project))
                });
            return ObjectExecutor::get_object_stream(
                &crate::runtime::executors::s3::S3Executor(
                    self.s3_for_instance_for_project(target_instance, project)?
                        .clone(),
                ),
                request_json,
            )
            .await;
        }
        ObjectExecutor::get_object_stream(
            &self.resolve_dispatch_executor(
                backend,
                instance,
                false,
                tonic::Code::FailedPrecondition,
                // Probe/admin paths: no request context, so RLS settings
                // aren't installed (these are infrastructure calls).
                None,
            )?,
            request_json,
        )
        .await
    }

    /// Write an object to an object-store backend.
    pub async fn put_object_backend(
        &self,
        backend: &str,
        request_json: &str,
        bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        self.put_object_backend_target(backend, None, request_json, bytes)
            .await
    }

    pub async fn put_object_backend_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        request_json: &str,
        bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        self.put_object_backend_target_for_project(
            backend,
            instance,
            crate::runtime::catalog::DEFAULT_PROJECT_ID,
            request_json,
            bytes,
        )
        .await
    }

    pub async fn put_object_backend_target_for_project(
        &self,
        backend: &str,
        instance: Option<&str>,
        project_id: &str,
        request_json: &str,
        bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        // Validate well-formed JSON before dispatch; the executor re-parses shape.
        parse_dispatch_json(request_json)?;
        use crate::runtime::executors::ObjectExecutor;
        #[cfg(not(feature = "s3"))]
        let _ = project_id;
        #[cfg(feature = "s3")]
        if matches!(backend, "s3" | "minio") {
            let project = project_id.trim();
            let project = if project.is_empty() {
                crate::runtime::catalog::DEFAULT_PROJECT_ID
            } else {
                project
            };
            let target_instance = instance
                .filter(|value| !value.trim().is_empty())
                .or_else(|| {
                    self.choose_instance_name_for_project("minio", true, project)
                        .or_else(|| self.choose_instance_name_for_project("s3", true, project))
                });
            return ObjectExecutor::put_object(
                &crate::runtime::executors::s3::S3Executor(
                    self.s3_for_instance_for_project(target_instance, project)?
                        .clone(),
                ),
                request_json,
                bytes,
            )
            .await;
        }
        ObjectExecutor::put_object(
            &self.resolve_dispatch_executor(
                backend,
                instance,
                false,
                tonic::Code::FailedPrecondition,
                // Probe/admin paths: no request context, so RLS settings
                // aren't installed (these are infrastructure calls).
                None,
            )?,
            request_json,
            bytes,
        )
        .await
    }

    /// Remove an object from an object-store backend.
    pub async fn delete_object_backend_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        project_id: &str,
        request_json: &str,
    ) -> Result<(), tonic::Status> {
        parse_dispatch_json(request_json)?;
        use crate::runtime::executors::ObjectExecutor;
        #[cfg(feature = "s3")]
        if matches!(backend, "s3" | "minio") {
            let project = project_id.trim();
            let project = if project.is_empty() {
                crate::runtime::catalog::DEFAULT_PROJECT_ID
            } else {
                project
            };
            let target_instance = instance
                .filter(|value| !value.trim().is_empty())
                .or_else(|| {
                    self.choose_instance_name_for_project("minio", true, project)
                        .or_else(|| self.choose_instance_name_for_project("s3", true, project))
                });
            return ObjectExecutor::delete_object(
                &crate::runtime::executors::s3::S3Executor(
                    self.s3_for_instance_for_project(target_instance, project)?
                        .clone(),
                ),
                request_json,
            )
            .await;
        }
        ObjectExecutor::delete_object(
            &self.resolve_dispatch_executor(
                backend,
                instance,
                false,
                tonic::Code::FailedPrecondition,
                None,
            )?,
            request_json,
        )
        .await
    }

    /// Ensure a named resource exists on the given backend.
    pub async fn ensure_resource_backend(
        &self,
        backend: &str,
        resource_name: &str,
        spec_json: &str,
    ) -> Result<(), tonic::Status> {
        self.ensure_resource_backend_target(backend, None, resource_name, spec_json)
            .await
    }

    pub async fn ensure_resource_backend_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        resource_name: &str,
        spec_json: &str,
    ) -> Result<(), tonic::Status> {
        use crate::runtime::executors::ResourceAdminExecutor;
        ResourceAdminExecutor::ensure_resource(
            &self.resolve_dispatch_executor(
                backend,
                instance,
                false,
                tonic::Code::InvalidArgument,
                None,
            )?,
            resource_name,
            spec_json,
        )
        .await
    }

    /// Drop a named resource on the given backend.
    pub async fn drop_resource_backend(
        &self,
        backend: &str,
        resource_name: &str,
    ) -> Result<(), tonic::Status> {
        self.drop_resource_backend_target(backend, None, resource_name)
            .await
    }

    pub async fn drop_resource_backend_target(
        &self,
        backend: &str,
        instance: Option<&str>,
        resource_name: &str,
    ) -> Result<(), tonic::Status> {
        use crate::runtime::executors::ResourceAdminExecutor;
        ResourceAdminExecutor::drop_resource(
            &self.resolve_dispatch_executor(
                backend,
                instance,
                false,
                tonic::Code::InvalidArgument,
                None,
            )?,
            resource_name,
        )
        .await
    }

    /// List resources on the given backend.
    pub async fn list_resources_backend(
        &self,
        backend: &str,
    ) -> Result<Vec<String>, tonic::Status> {
        self.list_resources_backend_target(backend, None).await
    }

    pub async fn list_resources_backend_target(
        &self,
        backend: &str,
        instance: Option<&str>,
    ) -> Result<Vec<String>, tonic::Status> {
        use crate::runtime::executors::ResourceAdminExecutor;
        ResourceAdminExecutor::list_resources(&self.resolve_dispatch_executor(
            backend,
            instance,
            false,
            tonic::Code::InvalidArgument,
            None,
        )?)
        .await
    }

    // ── Phase 1.2 — Catalog stage / activate / rollback ──────────────────────
}

#[cfg(test)]
mod probe_dispatch_validation_tests {
    use super::*;
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

    fn assert_internal_detail(status: &tonic::Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "probe_dispatch");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
    }

    #[test]
    fn probe_dispatch_validation_carries_field_violations() {
        assert_single_field_violation(
            &outbox_topic_not_allowed_status("private.topic"),
            "topic",
            "must be allowed by topic policy or UDB_CDC_VALID_TOPICS",
        );
        assert_single_field_violation(
            &unknown_probe_backend_status("bogus"),
            "backend",
            "must be one of postgres, redis, mongodb, neo4j, clickhouse, qdrant, s3, or minio",
        );
    }

    #[test]
    fn probe_dispatch_internal_status_carries_typed_detail() {
        let status =
            probe_dispatch_internal_status("topic_policy_allows", "topic policy query failed");
        assert_internal_detail(&status, "topic_policy_allows", "topic policy query failed");
    }
}
