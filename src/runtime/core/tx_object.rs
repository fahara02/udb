//! Continuation `impl DataBrokerRuntime` block (Phase F split of core.rs).
use super::*;

const COMPENSATION_RETRY_MAX_ATTEMPTS: u32 = 3;
const COMPENSATION_RETRY_BASE_DELAY_MS: u64 = 200;
/// Cap on in-memory qdrant/s3 compensation entries per transaction (#208). Past
/// this, compensations are NOT buffered in memory — every step already persists
/// its compensation as a durable saga step (`saga_record_step`), so the saga
/// recovery worker compensates the overflow from the ledger. Bounds memory for
/// pathologically large batches without losing rollback coverage.
const MAX_COMPENSATIONS_PER_TX: usize = 1000;

#[derive(Debug, Clone)]
struct PendingSagaStep {
    step_index: usize,
    operation: String,
    message_type: String,
    compensation_json: String,
}

/// The distinct select-cache invalidation patterns for the tables a
/// transaction mutated. Mirrors the unary upsert/update/delete paths, which
/// each drop `cache_invalidation_pattern("select", message_type)` after their
/// own commit; the transaction path must drop the same set or a cached Select
/// keeps serving pre-transaction rows. `tx_mutations` is already filtered to
/// the applied (non-`commit`) mutations. Empty message types are skipped and
/// duplicates collapsed so one pattern is issued per touched table.
fn select_invalidation_patterns(tx_mutations: &[&Mutation]) -> Vec<String> {
    let mut types: Vec<String> = tx_mutations
        .iter()
        .map(|mutation| mutation.message_type.clone())
        .filter(|message_type| !message_type.trim().is_empty())
        .collect();
    types.sort();
    types.dedup();
    types
        .iter()
        .map(|message_type| {
            crate::runtime::executor_utils::cache_invalidation_pattern("select", message_type)
        })
        .collect()
}

/// True when a `begin_tx` run actually committed (plain COMMIT or 2PC). Both
/// success arms push `TxStateCommitted`; a rolled-back / in-doubt / failed run
/// never does, so cache invalidation gates on this to avoid dropping cache for
/// a transaction whose writes were not durable.
fn transaction_committed(statuses: &[Result<TxStatus, tonic::Status>]) -> bool {
    statuses.iter().any(|entry| {
        matches!(
            entry,
            Ok(tx_status)
                if tx_status.state == crate::proto::tx_status::State::TxStateCommitted as i32
        )
    })
}

fn tx_object_invalid_field(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [(field.into(), description.into())],
    )
}

fn tx_object_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status("tx_object", operation, message)
}

fn mysql_xa_plan_replay_status(err: impl std::fmt::Display) -> tonic::Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::FailedPrecondition,
        "mysql",
        "xa_plan_replay",
        "mysql_xa_plan_replay",
        format!(
            "2PC refused before side effects: plan SQL cannot be replayed on the MySQL XA participant: {err}"
        ),
    )
}

fn xa_unsupported_participants_status(unsupported: &[String]) -> tonic::Status {
    crate::runtime::executor_utils::capability_status(
        "transaction",
        "xa_commit",
        "xa_participants",
        format!(
            "2PC refused before side effects: unsupported participants {}",
            unsupported.join(", ")
        ),
    )
}

fn record_encryption_key_missing_status(schema: &str, table: &str) -> tonic::Status {
    crate::runtime::executor_utils::capability_status(
        "encryption",
        "record_encryption",
        "udb_encryption_key",
        format!(
            "table {schema}.{table} contains encrypted columns but UDB encryption key is not configured"
        ),
    )
}

// Serving caller lives in the `#[cfg(not(feature = "s3"))]` put_tx_object arm;
// the unconditional typed-detail pin test keeps the disabled-path contract
// alive in s3-on builds.
#[cfg_attr(feature = "s3", allow(dead_code))]
fn s3_feature_disabled_status() -> tonic::Status {
    crate::runtime::executor_utils::capability_status(
        "s3",
        "put_tx_object",
        "s3_feature",
        "s3/object-store feature is not enabled",
    )
}

fn materialized_view_admin_scope_required_status() -> tonic::Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        "create_materialized_view",
        "admin_scope_required",
        "scope udb:admin is required",
    )
}

fn validate_tx_identifier(value: &str, label: &str) -> Result<(), tonic::Status> {
    if value.is_empty()
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        || value.starts_with(|ch: char| ch.is_ascii_digit())
    {
        return Err(tx_object_invalid_field(
            label,
            "must be a valid SQL identifier",
            format!("{label} '{value}' is not a valid SQL identifier"),
        ));
    }
    Ok(())
}

fn compensation_retry_delay(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_millis(COMPENSATION_RETRY_BASE_DELAY_MS * u64::from(attempt))
}

fn saga_compensation_for_mutation(operation: &str, mutation: &Mutation) -> String {
    let compensation = match operation {
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
    compensation.to_string()
}

/// bug #8.1 — decide whether a transactional mutation carries a compare-and-swap
/// precondition that must run, rejecting `expected` on operations without CAS
/// semantics (rejected, never silently ignored — mirroring the unary paths,
/// which only accept `expected` on upsert/update/delete). Returns `Ok(false)`
/// for the hot path (no `expected`), `Ok(true)` when CAS must be enforced, and
/// an `InvalidArgument` when `expected` is set on an unsupported operation.
/// Split out as a pure function so the guard is unit-testable without a live tx.
fn tx_cas_required(mutation: &Mutation, operation: &str) -> Result<bool, tonic::Status> {
    let has_expected = mutation
        .expected
        .as_ref()
        .map(|expected| !expected.fields.is_empty())
        .unwrap_or(false);
    if !has_expected {
        return Ok(false);
    }
    if !matches!(operation, "upsert" | "update" | "delete") {
        return Err(tx_object_invalid_field(
            "expected",
            "compare-and-swap is only supported for upsert, update, and delete",
            format!(
                "operation '{operation}' does not support an `expected` compare-and-swap precondition"
            ),
        ));
    }
    Ok(true)
}

impl DataBrokerRuntime {
    pub async fn begin_tx(
        &self,
        manifest: &CatalogManifest,
        mut stream: tonic::Streaming<Mutation>,
        metadata_context: RequestContext,
    ) -> Vec<Result<TxStatus, tonic::Status>> {
        let Some(pool) = &self.pg_pool else {
            return vec![Err(crate::runtime::executor_utils::capability_status(
                "postgres",
                "begin_tx",
                "postgres_backend",
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
            return vec![Err(tx_object_invalid_field(
                "mutations",
                "transaction stream must contain at least one mutation",
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
                return vec![Err(crate::runtime::executor_utils::retryable_status(
                    "postgres",
                    "transaction_begin",
                    crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                    format!("PostgreSQL begin failed: {err}"),
                ))];
            }
        };
        let mut statuses = Vec::new();
        let mut qdrant_compensations: Vec<(String, Vec<String>)> = Vec::new();
        // (bucket, object_key, resolved_instance, project_id) — the instance and
        // project are captured so rollback deletes from the SAME store the PUT
        // landed in, not the default S3 client.
        let mut s3_compensations: Vec<(String, String, Option<String>, String)> = Vec::new();
        // #208: once the in-memory compensation buffers hit the cap, overflow
        // spills to the durable saga ledger (recorded per-step below). Logged
        // once so the cap isn't a silent truncation.
        let mut compensations_spilled = false;
        let projection_plans = crate::runtime::projection::ProjectionPlan::from_manifest(manifest);
        let projection_config = crate::runtime::system::SystemCatalogConfig::current_arc();
        let tx_mutations: Vec<&Mutation> = mutations
            .iter()
            .filter(|mutation| !mutation.commit)
            .collect();
        let mut pending_saga_steps: Vec<PendingSagaStep> = Vec::new();
        // Per-mutation affected-row counts, so the post-commit audit loop skips a
        // mutation that changed nothing (audit integrity — see build_audit_event).
        let mut audit_affected: Vec<u64> = vec![0; tx_mutations.len()];
        // Item 23: when this transaction will commit via live 2PC and MySQL
        // instances are configured, capture every executed plan statement so
        // `XaMysqlParticipant` can replay it (translated to MySQL dialect)
        // between `XA START` and `XA END` on each MySQL participant.
        #[cfg(feature = "mysql")]
        let mysql_xa_capture = commit
            && strategy == TxStrategy::TwoPhase
            && super::two_phase_runtime_enabled()
            && !self.mysql_instances.is_empty();
        #[cfg(feature = "mysql")]
        let mut mysql_xa_statements: Vec<(String, Vec<JsonValue>)> = Vec::new();
        for (mutation_index, mutation) in tx_mutations.iter().enumerate() {
            let context = merge_context(mutation.context.as_ref(), metadata_context.clone());
            let operation = mutation.operation.to_ascii_lowercase();
            // bug #8.1: transactional compare-and-swap. When the mutation carries a
            // non-empty `expected`, assert it against the current row under this
            // tx's row lock + RLS fencing BEFORE the write, reusing the SAME unary
            // CAS helpers (enforce_cas_precondition / enforce_delete_precondition).
            // A mismatch surfaces as this mutation's `result` error, which the
            // failure arm below turns into a full-transaction rollback + compensation
            // exactly like any other mutation failure — so a stale precondition
            // aborts the whole tx with nothing written.
            let cas_check = self
                .enforce_tx_cas_precondition(&mut tx, manifest, mutation, &operation, &context)
                .await;
            let result = if let Err(err) = cas_check {
                Err(err)
            } else if operation == "upsert" {
                let record = mutation_record_json(mutation);
                match record {
                    Ok(record) => {
                        let encrypted_record =
                            match resolve_table_for_message(manifest, &mutation.message_type) {
                                Ok(table) => {
                                    // #117: resolve proto field_name record keys to
                                    // physical column_names before encrypt + bind.
                                    let record =
                                        crate::broker::normalize_record_keys(table, &record);
                                    self.encrypt_record_for_table(table, &record)
                                }
                                Err(error) => Err(tx_object_invalid_field(
                                    "message_type",
                                    "must match exactly one manifest table message type",
                                    error.to_string(),
                                )),
                            };
                        match encrypted_record {
                            Ok(encrypted_record) => {
                                let plan = build_upsert_plan(
                                    manifest,
                                    &UpsertPlanRequest {
                                        context: context.clone(),
                                        message_type: mutation.message_type.clone(),
                                        record: record.clone(),
                                        ..UpsertPlanRequest::default()
                                    },
                                );
                                let bind_values =
                                    record_values(&encrypted_record, &plan.parameter_columns)
                                        .unwrap_or_default();
                                let affected = execute_tx_plan(
                                    &mut tx,
                                    manifest,
                                    &mutation.message_type,
                                    &plan.sql,
                                    &plan.parameter_columns,
                                    &bind_values,
                                    &plan.errors,
                                )
                                .await;
                                match affected {
                                    Ok(affected) => {
                                        #[cfg(feature = "mysql")]
                                        if mysql_xa_capture {
                                            mysql_xa_statements
                                                .push((plan.sql.clone(), bind_values.clone()));
                                        }
                                        if affected == 0 {
                                            Ok(affected)
                                        } else if let Err(err) = crate::runtime::projection::ProjectionEngine::enqueue_write_tasks_tx(
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
                                            Err(tx_object_internal_status(
                                                "enqueue_projection_tasks",
                                                format!("projection task enqueue failed: {err}"),
                                            ))
                                        // mutations→CDC: emit the transactional-outbox
                                        // change event IN THE SAME TX, exactly as the
                                        // unary upsert does (setup_data.rs). Without
                                        // this, CDC-enabled entities written through
                                        // BeginTx emit no event — subscribers, journal
                                        // readers, and CDC-driven projections silently
                                        // miss every transactional write. A failure
                                        // rolls back the mutation (outbox atomicity).
                                        } else if let Err(err) = self
                                            .emit_cdc_outbox_on_mutation_checked(
                                                &mut tx,
                                                manifest,
                                                &mutation.message_type,
                                                "upsert",
                                                &record,
                                                &context,
                                                mutation.cdc_required,
                                            )
                                            .await
                                        {
                                            Err(err)
                                        } else {
                                            Ok(affected)
                                        }
                                    }
                                    Err(err) => Err(err),
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
                let bind_values = filter_bind_values(&filter);
                let affected = execute_tx_plan(
                    &mut tx,
                    manifest,
                    &mutation.message_type,
                    &plan.sql,
                    &plan.parameter_columns,
                    &bind_values,
                    &plan.errors,
                )
                .await;
                match affected {
                    Ok(affected) => {
                        #[cfg(feature = "mysql")]
                        if mysql_xa_capture {
                            mysql_xa_statements.push((plan.sql.clone(), bind_values.clone()));
                        }
                        if affected == 0 {
                            Ok(affected)
                        } else if let Err(err) =
                            crate::runtime::projection::ProjectionEngine::enqueue_write_tasks_tx(
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
                            Err(tx_object_internal_status(
                                "enqueue_projection_tasks",
                                format!("projection task enqueue failed: {err}"),
                            ))
                        // mutations→CDC: emit the delete change event in the same
                        // tx (filter carries the deleted row's key), matching the
                        // unary delete path so BeginTx deletes are not invisible
                        // to CDC subscribers and journal readers.
                        } else if let Err(err) = self
                            .emit_cdc_outbox_on_mutation_checked(
                                &mut tx,
                                manifest,
                                &mutation.message_type,
                                "delete",
                                &filter,
                                &context,
                                mutation.cdc_required,
                            )
                            .await
                        {
                            Err(err)
                        } else {
                            Ok(affected)
                        }
                    }
                    Err(err) => Err(err),
                }
            } else if operation == "update" {
                // Partial update inside the transaction: SET named columns and/or
                // apply atomic increments on the rows matched by `filter`. Shares
                // the exact plan→bind→execute→projection→CDC core with the unary
                // update() via `execute_update_in_tx`, so a transactional update
                // performs the same side-effects (projection enqueue + CDC event)
                // and cannot diverge. Select-cache invalidation + audit for the
                // touched message_type happen in the post-commit block below.
                let filter_raw = mutation
                    .filter
                    .as_ref()
                    .map(struct_to_json)
                    .unwrap_or(JsonValue::Null);
                let changes = mutation
                    .changes
                    .as_ref()
                    .map(struct_to_json)
                    .unwrap_or(JsonValue::Null);
                let increments: Vec<(String, f64)> = mutation
                    .increments
                    .iter()
                    .map(|inc| (inc.column.clone(), inc.delta))
                    .collect();
                // Encryption-rewrite the equality filter exactly as unary update()
                // does before planning; the shared helper does not re-apply it.
                let filter = match resolve_table_for_message(manifest, &mutation.message_type) {
                    Ok(table) => self.rewrite_encrypted_equality_filters(
                        table,
                        &filter_raw,
                        &context.tenant_id,
                    ),
                    Err(_) => filter_raw,
                };
                // return_record=false: a TxStatus carries no per-row body; the
                // helper still RETURNs internally when projections need the rows.
                self.execute_update_in_tx_checked(
                    &mut tx,
                    manifest,
                    &mutation.message_type,
                    &filter,
                    &changes,
                    &increments,
                    &context,
                    false,
                    mutation.cdc_required,
                )
                .await
                .map(|(affected, _record_json, _task_ids)| affected as u64)
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
                            if qdrant_compensations.len() < MAX_COMPENSATIONS_PER_TX {
                                qdrant_compensations.push((mutation.collection.clone(), point_ids));
                            } else if !compensations_spilled {
                                compensations_spilled = true;
                                tracing::warn!(
                                    tx_id = %tx_id,
                                    cap = MAX_COMPENSATIONS_PER_TX,
                                    "compensation buffer cap reached; overflow spills to the durable saga ledger for recovery-worker compensation"
                                );
                            }
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
                    Ok((affected, instance, project_id)) => {
                        if s3_compensations.len() < MAX_COMPENSATIONS_PER_TX {
                            s3_compensations.push((
                                mutation.bucket.clone(),
                                mutation.object_key.clone(),
                                instance,
                                project_id,
                            ));
                        } else if !compensations_spilled {
                            compensations_spilled = true;
                            tracing::warn!(
                                tx_id = %tx_id,
                                cap = MAX_COMPENSATIONS_PER_TX,
                                "compensation buffer cap reached; overflow spills to the durable saga ledger for recovery-worker compensation"
                            );
                        }
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
                match self
                    .topic_policy_allows(topic, &context.project_id, &context.tenant_id)
                    .await
                {
                    Ok(policy_decision) => {
                        if policy_decision == Some(false)
                            || (!cdc_config.valid_topics.is_empty()
                                && !cdc_config.valid_topics.iter().any(|t| t == topic)
                                && policy_decision.is_none())
                        {
                            Err(tx_object_invalid_field(
                                "topic",
                                "must be allowed by topic policy or UDB_CDC_VALID_TOPICS",
                                format!(
                                    "topic '{topic}' is not in the registered topic registry; \
                                 configure UDB_CDC_VALID_TOPICS or use an allowed topic"
                                ),
                            ))
                        } else {
                            let payload =
                                mutation.payload.as_ref().map(struct_to_json).ok_or_else(|| {
                                    tx_object_invalid_field(
                                        "payload",
                                        "enqueue_outbox_event transaction mutations require payload",
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
                                    let mut payload =
                                        crate::runtime::cdc::apply_manifest_cdc_redaction(
                                            manifest,
                                            &mutation.message_type,
                                            topic,
                                            schema_uri,
                                            payload,
                                            cdc_config.redaction_mode,
                                            cdc_config.redaction_version,
                                        );
                                    if let Some(obj) = payload.as_object_mut() {
                                        if !context.tenant_id.trim().is_empty() {
                                            obj.entry("tenant_id".to_string()).or_insert_with(
                                                || {
                                                    serde_json::Value::String(
                                                        context.tenant_id.clone(),
                                                    )
                                                },
                                            );
                                        }
                                        if !context.project_id.trim().is_empty() {
                                            obj.entry("project_id".to_string()).or_insert_with(
                                                || {
                                                    serde_json::Value::String(
                                                        context.project_id.clone(),
                                                    )
                                                },
                                            );
                                        }
                                    }
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
                                                    tx_object_internal_status(
                                                        "enqueue_tx_event",
                                                        format!(
                                                            "failed to enqueue tx event: {err}"
                                                        ),
                                                    )
                                                })
                                        }
                                        Err(err) => Err(err),
                                    }
                                }
                                Err(err) => Err(err),
                            }
                        }
                    }
                    Err(err) => Err(err),
                }
            } else {
                Err(tx_object_invalid_field(
                    "operation",
                    "must be one of upsert, update, delete, vector_upsert, put_object, or enqueue_outbox_event",
                    format!("unsupported transaction operation {}", mutation.operation),
                ))
            };
            match result {
                Ok(affected) => {
                    audit_affected[mutation_index] = affected;
                    pending_saga_steps.push(PendingSagaStep {
                        step_index: pending_saga_steps.len(),
                        operation: operation.clone(),
                        message_type: mutation.message_type.clone(),
                        compensation_json: saga_compensation_for_mutation(&operation, mutation),
                    });
                    statuses.push(Ok(TxStatus {
                        state: crate::proto::tx_status::State::TxStateOpen as i32,
                        tx_id: tx_id.clone(),
                        mutation_id: Uuid::new_v4().to_string(),
                        message: format!("{affected} row(s) affected"),
                        ..TxStatus::default()
                    }))
                }
                Err(err) => {
                    let _ = tx.rollback().await;
                    let compensation_message = self
                        .compensate_external_side_effects(&qdrant_compensations, &s3_compensations)
                        .await
                        .unwrap_or_else(|| "no external compensation was needed".to_string());
                    if let Some(ref sid) = saga_id {
                        if compensations_spilled {
                            self.saga_record_pending_steps(sid, &pending_saga_steps)
                                .await;
                            self.saga_set_status(sid, "in_doubt").await;
                        } else {
                            self.saga_set_status(sid, "compensated").await;
                        }
                    }
                    statuses.push(Err(tx_object_internal_status(
                        "mutation_failure_compensation",
                        format!("{}; {}", err.message(), compensation_message),
                    )));
                    for skipped in tx_mutations.iter().skip(mutation_index + 1) {
                        statuses.push(Ok(TxStatus {
                            state: crate::proto::tx_status::State::TxStateRolledBack as i32,
                            tx_id: tx_id.clone(),
                            mutation_id: Uuid::new_v4().to_string(),
                            message: format!(
                                "not executed because an earlier mutation failed: {} {}",
                                skipped.operation, skipped.message_type
                            ),
                            ..TxStatus::default()
                        }));
                    }
                    return statuses;
                }
            }
        }
        if commit {
            if strategy == TxStrategy::TwoPhase && super::two_phase_runtime_enabled() {
                let sys_config = crate::runtime::system::SystemCatalogConfig::current_arc();
                if let Err(err) =
                    crate::runtime::xa_recovery::ensure_xa_ledger_table(pool, &sys_config).await
                {
                    let _ = tx.rollback().await;
                    if let Some(ref sid) = saga_id {
                        self.saga_set_status(sid, "failed").await;
                    }
                    statuses.push(Err(tx_object_internal_status(
                        "xa_ledger_unavailable",
                        format!("XA ledger is unavailable: {err}"),
                    )));
                    return statuses;
                }
                // Item 23: configured MySQL instances join the 2PC as XA
                // participants, replaying the captured plan statements
                // translated to MySQL dialect. Translation failures fail
                // CLOSED here — before any PREPARE side effect is issued.
                #[cfg(feature = "mysql")]
                let mysql_participants: Vec<
                    Box<dyn crate::runtime::xa::XaParticipant>,
                > = if mysql_xa_capture {
                    let mut translated: Vec<(String, Vec<JsonValue>)> =
                        Vec::with_capacity(mysql_xa_statements.len());
                    let mut translation_error: Option<String> = None;
                    for (sql, values) in &mysql_xa_statements {
                        match crate::runtime::xa::translate_pg_plan_sql_to_mysql(sql) {
                            Ok(mysql_sql) => translated.push((mysql_sql, values.clone())),
                            Err(err) => {
                                translation_error = Some(err);
                                break;
                            }
                        }
                    }
                    if let Some(err) = translation_error {
                        let _ = tx.rollback().await;
                        if let Some(ref sid) = saga_id {
                            self.saga_set_status(sid, "failed").await;
                        }
                        statuses.push(Err(mysql_xa_plan_replay_status(err)));
                        return statuses;
                    }
                    let mut instance_names: Vec<String> =
                        self.mysql_instances.keys().cloned().collect();
                    instance_names.sort();
                    instance_names
                        .into_iter()
                        .filter_map(|name| {
                            self.mysql_instances.get(&name).map(|mysql_pool| {
                                Box::new(crate::runtime::xa::XaMysqlParticipant::new(
                                    name.clone(),
                                    mysql_pool.clone(),
                                    translated.clone(),
                                ))
                                    as Box<dyn crate::runtime::xa::XaParticipant>
                            })
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                let participant = crate::runtime::xa_postgres::ActivePostgresXaParticipant::new(
                    "primary",
                    pool.clone(),
                    tx,
                );
                #[cfg_attr(not(feature = "mysql"), allow(unused_mut))]
                let mut participants: Vec<
                    Box<dyn crate::runtime::xa::XaParticipant>,
                > = vec![Box::new(participant)];
                #[cfg(feature = "mysql")]
                participants.extend(mysql_participants);
                let xa_request = crate::runtime::xa::XaRequest {
                    tenant_id: metadata_context.tenant_id.clone(),
                    project_id: metadata_context.project_id.clone(),
                    origin_rpc: "BeginTx".to_string(),
                    correlation_id: metadata_context.correlation_id.clone(),
                };
                // Item 5: write-ahead decision record — the commit intent is
                // durably inserted into the XA ledger BEFORE the first
                // COMMIT PREPARED, so a crash mid-PHASE-2 leaves a
                // commit-intent row the presumed-abort sweep must honour.
                let write_ahead_pool = pool.clone();
                let write_ahead_config = sys_config.clone();
                let xa_result = crate::runtime::xa::XaCoordinator::execute_with_write_ahead(
                    xa_request,
                    participants,
                    &crate::backend::capability_matrix(),
                    move |entry| async move {
                        crate::runtime::xa_recovery::record_xa_ledger_entry(
                            &write_ahead_pool,
                            &write_ahead_config,
                            &entry,
                        )
                        .await
                    },
                )
                .await;
                match xa_result {
                    Ok(outcome) => {
                        if let Err(err) = crate::runtime::xa_recovery::record_xa_ledger_entry(
                            pool,
                            &sys_config,
                            &outcome.ledger,
                        )
                        .await
                        {
                            if let Some(ref sid) = saga_id {
                                self.saga_set_status(sid, "in_doubt").await;
                            }
                            statuses.push(Err(tx_object_internal_status(
                                "xa_ledger_commit_record",
                                format!(
                                    "XA committed but ledger write failed for xid {}: {err}",
                                    outcome.ledger.xid
                                ),
                            )));
                            return statuses;
                        }
                        if let Some(ref sid) = saga_id {
                            self.saga_record_pending_steps(sid, &pending_saga_steps)
                                .await;
                            self.saga_set_status(sid, "committed").await;
                        }
                        statuses.push(Ok(TxStatus {
                            state: crate::proto::tx_status::State::TxStateCommitted as i32,
                            tx_id,
                            message: format!("committed (2pc xid={})", outcome.ledger.xid),
                            ..TxStatus::default()
                        }));
                    }
                    Err(crate::runtime::xa::XaError::CapabilityRefused { unsupported }) => {
                        if let Some(ref sid) = saga_id {
                            self.saga_set_status(sid, "failed").await;
                        }
                        statuses.push(Err(xa_unsupported_participants_status(&unsupported)));
                    }
                    Err(crate::runtime::xa::XaError::PrepareFailed { ledger, .. }) => {
                        let _ = crate::runtime::xa_recovery::record_xa_ledger_entry(
                            pool,
                            &sys_config,
                            &ledger,
                        )
                        .await;
                        if let Some(ref sid) = saga_id {
                            self.saga_set_status(sid, "failed").await;
                        }
                        statuses.push(Err(
                            crate::runtime::executor_utils::retryable_aborted_status(
                                "transaction",
                                "xa prepare",
                                0,
                                format!(
                                    "2PC PREPARE failed for xid {}: {}",
                                    ledger.xid, ledger.reason
                                ),
                            ),
                        ));
                    }
                    Err(crate::runtime::xa::XaError::InDoubt { ledger }) => {
                        let ledger_err = crate::runtime::xa_recovery::record_xa_ledger_entry(
                            pool,
                            &sys_config,
                            &ledger,
                        )
                        .await
                        .err();
                        if let Some(ref sid) = saga_id {
                            self.saga_set_status(sid, "in_doubt").await;
                        }
                        let ledger_note = ledger_err
                            .map(|err| format!("; additionally XA ledger write failed: {err}"))
                            .unwrap_or_default();
                        statuses.push(Err(
                            crate::runtime::executor_utils::retryable_aborted_status(
                                "transaction",
                                "xa in-doubt recovery",
                                0,
                                format!(
                                    "2PC xid {} is IN-DOUBT and will be resolved by XA recovery{}",
                                    ledger.xid, ledger_note
                                ),
                            ),
                        ));
                    }
                }
            } else {
                match tx.commit().await {
                    Ok(()) => {
                        if let Some(ref sid) = saga_id {
                            self.saga_record_pending_steps(sid, &pending_saga_steps)
                                .await;
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
                            if compensations_spilled {
                                self.saga_record_pending_steps(sid, &pending_saga_steps)
                                    .await;
                                self.saga_set_status(sid, "in_doubt").await;
                            } else {
                                self.saga_set_status(sid, "compensated").await;
                            }
                        }
                        statuses.push(Err(tx_object_internal_status(
                            "postgres_commit_compensation",
                            format!("PostgreSQL commit failed: {err}; {comp_msg}"),
                        )))
                    }
                }
            }
        } else {
            if let Err(err) = tx.rollback().await {
                tracing::warn!("PostgreSQL rollback failed after stream close: {err}");
            }
            let compensation_message = self
                .compensate_external_side_effects(&qdrant_compensations, &s3_compensations)
                .await;
            if let Some(ref sid) = saga_id {
                if compensations_spilled {
                    self.saga_record_pending_steps(sid, &pending_saga_steps)
                        .await;
                    self.saga_set_status(sid, "in_doubt").await;
                } else {
                    self.saga_set_status(sid, "compensated").await;
                }
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
        // Read-your-writes through cache: the unary upsert/update/delete paths
        // each drop their message_type's cached selects right after commit
        // (setup_data.rs). A transaction mutates the same rows through this
        // separate path, so it must invalidate the same select-cache entries on
        // commit — otherwise a cached full-key Select keeps serving the
        // pre-transaction row until the entry's TTL lapses (the exact staleness
        // an admin edit-then-read hit through BeginTx). Gate on an actual
        // commit (both plain COMMIT and 2PC push TxStateCommitted; a rolled-back
        // / in-doubt / failed run never does). Best-effort like the unary paths'
        // `let _ =`: a Redis hiccup must not fail an already-committed write —
        // the stamped entries still expire on TTL and the checksum guards
        // migrations.
        if transaction_committed(&statuses) {
            // Read-your-writes fence for the committed transaction: attach a
            // WriteReceipt (source LSN + outbox seq + manifest checksum) to the
            // committed TxStatus so a client can fence a following read exactly
            // as it does with MutationResponse.write_receipt on the unary verbs
            // (previously BeginTx returned no receipt at all). The source LSN is
            // read post-commit, so it reflects this transaction's durable
            // position. projection_task_ids are left empty: the per-task fence is
            // not threaded through the multi-mutation loop, and the LSN fence is
            // the primary read-your-writes guarantee.
            let receipt = match self.default_system_stores_clone() {
                Some(store) => {
                    crate::runtime::consistency_fence::build_write_receipt(
                        store.as_ref(),
                        &manifest.checksum_sha256,
                        Vec::new(),
                    )
                    .await
                }
                None => crate::runtime::consistency::WriteReceipt::empty(),
            };
            let receipt_proto = receipt.to_proto();
            for status in statuses.iter_mut() {
                if let Ok(tx_status) = status
                    && tx_status.state == crate::proto::tx_status::State::TxStateCommitted as i32
                {
                    tx_status.write_receipt = Some(receipt_proto.clone());
                }
            }
            for pattern in select_invalidation_patterns(&tx_mutations) {
                let _ = self.cache_delete_pattern(&pattern).await;
            }
            // Audit each committed SQL mutation to the configured sink, mirroring
            // the unary upsert/delete paths (setup_data.rs) — a write through
            // BeginTx must leave the same audit trail, or transactional writes
            // are a compliance blind spot. Resource URI is the table-level
            // sql://schema/table (a transaction spans multiple records, so a
            // record-level URI like the unary path's is not meaningful here).
            for (mutation_index, mutation) in tx_mutations.iter().enumerate() {
                let operation = mutation.operation.to_ascii_lowercase();
                if !matches!(operation.as_str(), "upsert" | "update" | "delete") {
                    continue;
                }
                let audit_context =
                    merge_context(mutation.context.as_ref(), metadata_context.clone());
                let resource_uri = resolve_table_for_message(manifest, &mutation.message_type)
                    .map(|table| format!("sql://{}/{}", table.schema, table.table))
                    .unwrap_or_else(|_| mutation.message_type.clone());
                // Only audit a tx mutation that actually changed a row.
                if let Some(event) = crate::planning::broker::build_audit_event(
                    &audit_context,
                    &operation,
                    &resource_uri,
                    &manifest.checksum_sha256,
                    audit_affected[mutation_index] as i64,
                ) {
                    crate::runtime::core::audit::emit_audit(
                        &self.config.audit_sink,
                        &event,
                        self.pg_pool.as_ref(),
                    );
                }
            }
        }
        statuses
    }

    async fn saga_record_pending_steps(&self, saga_id: &str, steps: &[PendingSagaStep]) {
        for step in steps {
            self.saga_record_step(
                saga_id,
                step.step_index,
                &step.operation,
                &step.message_type,
                &step.compensation_json,
            )
            .await;
        }
    }

    /// bug #8.1 — enforce the transactional compare-and-swap precondition for one
    /// mutation, reusing the exact unary CAS helpers so the two write paths cannot
    /// drift: `enforce_cas_precondition` (upsert, keyed by the primary key — the
    /// transactional upsert carries no conflict_fields, so its conflict target is
    /// always the PK, matching `enforce_upsert_precondition`'s empty-conflict_fields
    /// branch) and `enforce_delete_precondition` (update/delete, keyed by PK
    /// equality from the encryption-rewritten, normalized filter — identical to the
    /// unary update/delete CAS). The SELECT ... FOR UPDATE is tenant/RLS-fenced by
    /// installing the request-local GUCs on `tx` first: the tx apply loop otherwise
    /// fences its writes through SQL-baked tenant predicates and never installs the
    /// GUC, so without this the CAS read would not be tenant-scoped. `SET LOCAL` is
    /// transaction-scoped. No-op (no extra SQL, no GUC) when `expected` is unset.
    async fn enforce_tx_cas_precondition(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        manifest: &CatalogManifest,
        mutation: &Mutation,
        operation: &str,
        context: &RequestContext,
    ) -> Result<(), tonic::Status> {
        if !tx_cas_required(mutation, operation)? {
            return Ok(());
        }
        // `tx_cas_required` returned true, so `expected` is Some and non-empty.
        let expected = mutation
            .expected
            .as_ref()
            .expect("tx_cas_required guarantees a non-empty expected precondition");
        let table =
            resolve_table_for_message(manifest, &mutation.message_type).map_err(|error| {
                tx_object_invalid_field(
                    "message_type",
                    "must match exactly one manifest table message type",
                    error.to_string(),
                )
            })?;
        // Fence the CAS read to the caller's tenant, exactly as the unary CAS runs
        // after `set_request_local_settings`.
        set_request_local_settings(tx, context).await?;
        if operation == "upsert" {
            let record = mutation_record_json(mutation)?;
            let normalized = crate::broker::normalize_record_keys(table, &record);
            let key_columns = table.primary_key.clone();
            if key_columns.is_empty() {
                return Err(crate::runtime::executor_utils::failed_precondition_fields(
                    "compare-and-swap precondition requires a manifest primary key",
                    [(
                        "expected".to_string(),
                        "no key columns are available to locate the current row".to_string(),
                    )],
                ));
            }
            let key_values = record_values(&normalized, &key_columns)?;
            self.enforce_cas_precondition(tx, table, expected, &key_columns, &key_values, context)
                .await
        } else {
            // update | delete: locate the row by primary-key equality from the
            // encryption-rewritten, normalized filter — the same helper + filter
            // shape the unary update()/delete() CAS uses.
            let filter_raw = mutation
                .filter
                .as_ref()
                .map(struct_to_json)
                .unwrap_or(JsonValue::Null);
            let filter =
                self.rewrite_encrypted_equality_filters(table, &filter_raw, &context.tenant_id);
            let normalized_filter = crate::planning::broker::normalize_filter_keys(
                &crate::planning::broker::column_resolver(table),
                &filter,
            );
            self.enforce_delete_precondition(tx, table, expected, &normalized_filter, context)
                .await
        }
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
            return Err(materialized_view_admin_scope_required_status());
        }
        validate_tx_identifier(&request.schema, "schema")?;
        validate_tx_identifier(&request.name, "name")?;
        let declared = declared_materialized_view(manifest, &request.schema, &request.name)
            .ok_or_else(|| {
                tx_object_invalid_field(
                    "view",
                    "must be declared in the proto AST",
                    format!(
                        "materialized view {}.{} is not declared in the proto AST",
                        request.schema, request.name
                    ),
                )
            })?;
        // Always use the trusted proto-AST query. If the caller supplies a query, it must
        // normalize to the same string as the declared query (whitespace differences only).
        // We then use `declared.query` so the SQL always comes from the authoritative source.
        let query = if request.query.trim().is_empty()
            || normalize_sql(&request.query) == normalize_sql(&declared.query)
        {
            declared.query.as_str()
        } else {
            return Err(tx_object_invalid_field(
                "query",
                "must match the proto AST declaration",
                "materialized view query does not match the proto AST declaration",
            ));
        };
        let with_data = request.with_data || declared.with_data;
        let sql = format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS \"{}\".\"{}\" AS {} {}",
            request.schema.replace('"', "\"\""),
            request.name.replace('"', "\"\""),
            query,
            if with_data { "" } else { "WITH NO DATA" }
        );
        let result = sqlx::query(&sql)
            .execute(self.pg_pool()?)
            .await
            .map_err(|err| {
                tx_object_internal_status(
                    "create_materialized_view",
                    format!("create view failed: {err}"),
                )
            })?;
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
            // Supervised refresh loop: a single warn per failure hid persistent
            // staling. Track consecutive failures and escalate to ERROR after a
            // threshold so operators are alerted that the view is serving stale
            // data (rather than discovering it manually).
            const STALE_ALERT_THRESHOLD: u32 = 3;
            let mut interval = tokio::time::interval(Duration::from_secs(seconds));
            let mut consecutive_failures: u32 = 0;
            tracing::info!(
                schema = %schema,
                view = %name,
                interval_secs = seconds,
                "materialized view refresh scheduler started"
            );
            loop {
                interval.tick().await;
                match runtime.refresh_materialized_view(&schema, &name).await {
                    Ok(()) => consecutive_failures = 0,
                    Err(err) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        if consecutive_failures >= STALE_ALERT_THRESHOLD {
                            tracing::error!(
                                schema = %schema,
                                view = %name,
                                consecutive_failures,
                                error = %err,
                                "materialized view is staling: refresh has failed repeatedly — operator attention required"
                            );
                        } else {
                            tracing::warn!(
                                schema = %schema,
                                view = %name,
                                error = %err,
                                "materialized view refresh failed"
                            );
                        }
                    }
                }
            }
        });
    }

    pub async fn refresh_materialized_view(
        &self,
        schema: &str,
        name: &str,
    ) -> Result<(), tonic::Status> {
        validate_tx_identifier(schema, "schema")?;
        validate_tx_identifier(name, "name")?;
        let sql = format!(
            "REFRESH MATERIALIZED VIEW CONCURRENTLY {}.{}",
            qi_runtime(schema),
            qi_runtime(name)
        );
        if let Err(err) = sqlx::query(&sql).execute(self.pg_pool()?).await {
            let error_text = err.to_string();
            if !is_unpopulated_materialized_view_refresh_error(&error_text) {
                return Err(tx_object_internal_status(
                    "refresh_materialized_view",
                    format!("refresh view failed: {err}"),
                ));
            }
            let sql = format!(
                "REFRESH MATERIALIZED VIEW {}.{}",
                qi_runtime(schema),
                qi_runtime(name)
            );
            sqlx::query(&sql)
                .execute(self.pg_pool()?)
                .await
                .map_err(|err| {
                    tx_object_internal_status(
                        "initial_refresh_materialized_view",
                        format!("initial refresh view failed: {err}"),
                    )
                })?;
        }
        Ok(())
    }

    pub(crate) fn pg_pool(&self) -> Result<&PgPool, tonic::Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            crate::runtime::executor_utils::capability_status(
                "postgres",
                "pool",
                "postgres_backend",
                "PostgreSQL backend is not configured",
            )
        })
    }

    pub(crate) async fn pg_select_pool_for_table_routed(
        &self,
        table: &crate::generation::ManifestTable,
        context: &RequestContext,
    ) -> Result<crate::runtime::core::RoutedReadPool, tonic::Status> {
        let primary_required = table.replica_hint.eq_ignore_ascii_case("primary")
            || context.requires_primary_read()
            || super::accessors::read_fence_requires_primary(context);
        self.pg_read_pool_routed(context, primary_required).await
    }

    #[cfg(feature = "qdrant")]
    pub(crate) fn qdrant(&self) -> Result<&QdrantHttpClient, tonic::Status> {
        self.qdrant.as_ref().ok_or_else(|| {
            crate::runtime::executor_utils::capability_status(
                "qdrant",
                "client",
                "qdrant_backend",
                "Qdrant backend is not configured",
            )
        })
    }

    #[cfg(feature = "s3")]
    pub(crate) fn s3(&self) -> Result<&aws_sdk_s3::Client, tonic::Status> {
        self.s3.as_ref().ok_or_else(|| {
            crate::runtime::executor_utils::capability_status(
                "s3",
                "client",
                "s3_backend",
                "S3/MinIO backend is not configured",
            )
        })
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

    /// W11 read side: transparently rewrite STRING-equality predicates on
    /// encrypted columns to their blind-index sibling + tenant-scoped HMAC
    /// token, at every depth of the filter tree. Consumers filter plaintext;
    /// the broker routes the lookup through the index. Shapes the rewrite
    /// cannot express (operators, non-string values, no declared sibling)
    /// fall through untouched and take the planner's fail-closed typed error.
    pub(crate) fn rewrite_encrypted_equality_filters(
        &self,
        table: &ManifestTable,
        filter: &JsonValue,
        tenant_id: &str,
    ) -> JsonValue {
        let Some(encryption) = &self.encryption else {
            return filter.clone();
        };
        let mut index_by_column: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        for column in table.columns.iter().filter(|c| is_encrypted_column(c)) {
            let index_column = format!("{}_idx", column.column_name);
            if table
                .columns
                .iter()
                .any(|c| c.column_name == index_column && c.security.is_blind_index)
            {
                index_by_column.insert(column.column_name.clone(), index_column);
                if column.field_name != column.column_name {
                    index_by_column.insert(
                        column.field_name.clone(),
                        format!("{}_idx", column.column_name),
                    );
                }
            }
        }
        if index_by_column.is_empty() {
            return filter.clone();
        }
        fn walk(
            value: &JsonValue,
            map: &std::collections::BTreeMap<String, String>,
            encryption: &crate::runtime::encryption::EncryptionRuntime,
            tenant: &str,
        ) -> JsonValue {
            match value {
                JsonValue::Object(object) => {
                    let mut out = serde_json::Map::new();
                    for (key, nested) in object {
                        let lowered = key.to_ascii_lowercase();
                        if matches!(lowered.as_str(), "$and" | "and" | "$or" | "or") {
                            out.insert(key.clone(), walk(nested, map, encryption, tenant));
                            continue;
                        }
                        match (map.get(&lowered), nested.as_str()) {
                            (Some(index_column), Some(plaintext)) => {
                                match encryption.blind_index_token(tenant, plaintext) {
                                    Some(token) => {
                                        out.insert(index_column.clone(), JsonValue::String(token));
                                    }
                                    None => {
                                        out.insert(key.clone(), nested.clone());
                                    }
                                }
                            }
                            _ => {
                                out.insert(key.clone(), nested.clone());
                            }
                        }
                    }
                    JsonValue::Object(out)
                }
                JsonValue::Array(items) => JsonValue::Array(
                    items
                        .iter()
                        .map(|item| walk(item, map, encryption, tenant))
                        .collect(),
                ),
                other => other.clone(),
            }
        }
        walk(filter, &index_by_column, encryption, tenant_id)
    }

    pub(crate) fn encrypt_record_for_table(
        &self,
        table: &ManifestTable,
        record: &JsonValue,
    ) -> Result<JsonValue, tonic::Status> {
        let Some(encryption) = &self.encryption else {
            if table.columns.iter().any(is_encrypted_column) {
                self.encryption_metrics.record("encrypt", false);
                return Err(record_encryption_key_missing_status(
                    &table.schema,
                    &table.table,
                ));
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
            // W11: populate the blind-index sibling (`<col>_idx`, declared
            // is_blind_index) from the PLAINTEXT before encryption, so equality
            // lookups go through the deterministic tenant-scoped HMAC token and
            // consumers stop hand-deriving it. Caller-provided idx values win.
            let index_column = format!("{}_idx", column.column_name);
            if table
                .columns
                .iter()
                .any(|c| c.column_name == index_column && c.security.is_blind_index)
                && encrypted
                    .get(&index_column)
                    .map(|existing| existing.is_null())
                    .unwrap_or(true)
                && let Some(plaintext) = value.as_str()
            {
                let tenant = table_tenant_value(table, &encrypted);
                if let Some(token) = encryption.blind_index_token(&tenant, plaintext) {
                    encrypted.insert(index_column, JsonValue::String(token));
                }
            }
            match encryption.encrypt_json_value(&value) {
                Ok(ciphertext) => {
                    self.encryption_metrics.record("encrypt", true);
                    encrypted.insert(column.column_name.clone(), JsonValue::String(ciphertext));
                }
                Err(err) => {
                    self.encryption_metrics.record("encrypt", false);
                    return Err(tx_object_internal_status(
                        "encrypt_record_column",
                        format!(
                            "failed to encrypt {}.{}: {err}",
                            table.table, column.column_name
                        ),
                    ));
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
    ) -> Result<(u64, Option<String>, String), tonic::Status> {
        #[cfg(not(feature = "s3"))]
        {
            let _ = (manifest, mutation, metadata_context);
            return Err(s3_feature_disabled_status());
        }
        #[cfg(feature = "s3")]
        {
            if mutation.object_data.len() > INLINE_OBJECT_LIMIT_BYTES {
                return Err(crate::runtime::executor_utils::quota_refusal_status(
                    "object",
                    "transaction inline object size",
                    "Use GeneratePresignedUrl for files > 1MB",
                ));
            }
            let context = merge_context(mutation.context.as_ref(), metadata_context);
            let plan = build_object_stream_plan(
                manifest,
                &ObjectStreamPlanRequest {
                    context: context.clone(),
                    bucket: mutation.bucket.clone(),
                    object_key: mutation.object_key.clone(),
                    method: "PUT".to_string(),
                    chunk_count: 1,
                    final_chunk_seen: true,
                    content_type: mutation.content_type.clone(),
                },
            );
            reject_plan(&plan.errors)?;
            // Honor target_instance routing (and per-project instance selection)
            // exactly like setup_data.rs, instead of always using the default S3
            // client — otherwise a transaction PUT lands in the wrong instance.
            let target_instance = if context.target_instance.trim().is_empty() {
                self.choose_instance_name_for_project("minio", true, &context.project_id)
                    .or_else(|| {
                        self.choose_instance_name_for_project("s3", true, &context.project_id)
                    })
            } else {
                Some(context.target_instance.as_str())
            };
            let s3 = self.s3_for_instance_for_project(target_instance, &context.project_id)?;
            let mut put = s3
                .put_object()
                .bucket(&mutation.bucket)
                .key(&mutation.object_key)
                .set_content_type(if mutation.content_type.is_empty() {
                    None
                } else {
                    Some(mutation.content_type.clone())
                })
                .body(ByteStream::from(mutation.object_data.clone()));
            // Enforce the store's server_side_encryption annotation on the wire,
            // matching the non-transactional put_object path (SSE-S3/AES-256).
            if plan.requires_server_side_encryption {
                put = put.server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256);
            }
            put.send().await.map_err(|err| {
                crate::runtime::executor_utils::backend_transport_status("S3", "put_object", err)
            })?;
            Ok((
                1,
                target_instance.map(str::to_string),
                context.project_id.clone(),
            ))
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
        for (collection, point_ids) in compensations.iter().rev() {
            let mut last_err = String::new();
            let mut succeeded = false;
            for attempt in 1..=COMPENSATION_RETRY_MAX_ATTEMPTS {
                match qdrant.delete_points(collection, point_ids).await {
                    Ok(()) => {
                        deleted += point_ids.len();
                        succeeded = true;
                        break;
                    }
                    Err(err) => {
                        last_err = format!("{err}");
                        if attempt < COMPENSATION_RETRY_MAX_ATTEMPTS {
                            tracing::warn!(
                                collection = %collection,
                                attempt,
                                error = %err,
                                "Qdrant compensation failed; retrying"
                            );
                            tokio::time::sleep(compensation_retry_delay(attempt)).await;
                        }
                    }
                }
            }
            if !succeeded {
                failures.push(format!(
                    "{collection}: {last_err} (after {COMPENSATION_RETRY_MAX_ATTEMPTS} attempts)"
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
        _compensations: &[(String, String, Option<String>, String)],
    ) -> Option<String> {
        None
    }

    #[cfg(feature = "s3")]
    pub(crate) async fn compensate_s3_puts(
        &self,
        compensations: &[(String, String, Option<String>, String)],
    ) -> Option<String> {
        if compensations.is_empty() {
            return None;
        }
        if self.s3.is_none() && self.s3_instances.is_empty() {
            return Some("S3 compensation skipped because S3/MinIO is not configured".to_string());
        }
        let mut deleted = 0usize;
        let mut failures = Vec::new();
        for (bucket, object_key, instance, project_id) in compensations.iter().rev() {
            // Resolve the SAME instance the PUT used so we delete from the right
            // store; fall back to default resolution when none was recorded.
            let s3 = match self.s3_for_instance_for_project(instance.as_deref(), project_id) {
                Ok(client) => client,
                Err(err) => {
                    failures.push(format!("s3://{bucket}/{object_key}: {err}"));
                    continue;
                }
            };
            let mut last_err = String::new();
            let mut succeeded = false;
            for attempt in 1..=COMPENSATION_RETRY_MAX_ATTEMPTS {
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
                        if attempt < COMPENSATION_RETRY_MAX_ATTEMPTS {
                            tracing::warn!(
                                bucket = %bucket,
                                key = %object_key,
                                attempt,
                                error = %err,
                                "S3 compensation failed; retrying"
                            );
                            tokio::time::sleep(compensation_retry_delay(attempt)).await;
                        }
                    }
                }
            }
            if !succeeded {
                failures.push(format!(
                    "s3://{bucket}/{object_key}: {last_err} (after {COMPENSATION_RETRY_MAX_ATTEMPTS} attempts)"
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
        s3_compensations: &[(String, String, Option<String>, String)],
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

fn is_unpopulated_materialized_view_refresh_error(error_text: &str) -> bool {
    let text = error_text.to_ascii_lowercase();
    text.contains("concurrently")
        && (text.contains("has not been populated")
            || text.contains("not been populated")
            || text.contains("cannot refresh materialized view"))
}

#[cfg(test)]
mod materialized_view_refresh_tests {
    use super::*;
    use crate::generation::ManifestMaterializedView;
    use crate::proto::{ErrorDetail, ErrorKind};
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

    fn assert_typed_detail(
        status: &tonic::Status,
        kind: ErrorKind,
        backend: &str,
        operation: &str,
        capability_required: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, kind as i32);
        assert_eq!(detail.backend, backend);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability_required);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_policy_detail(
        status: &tonic::Status,
        operation: &str,
        policy_decision_id: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_internal_detail(status: &tonic::Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "tx_object");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn admin_context() -> RequestContext {
        RequestContext {
            scopes: vec!["udb:admin".to_string()],
            ..RequestContext::default()
        }
    }

    #[test]
    fn detects_unpopulated_materialized_view_concurrent_refresh_error() {
        assert!(is_unpopulated_materialized_view_refresh_error(
            "ERROR: CONCURRENTLY cannot be used when the materialized view has not been populated"
        ));
        assert!(!is_unpopulated_materialized_view_refresh_error(
            "ERROR: could not create unique index for materialized view"
        ));
    }

    #[test]
    fn tx_object_local_validation_carries_field_violations() {
        let err = validate_tx_identifier("1bad", "schema").expect_err("invalid identifier");
        assert_single_field_violation(&err, "schema", "must be a valid SQL identifier");

        let err = tx_object_invalid_field(
            "operation",
            "must be one of upsert, update, delete, vector_upsert, put_object, or enqueue_outbox_event",
            "unsupported transaction operation noop",
        );
        assert_single_field_violation(
            &err,
            "operation",
            "must be one of upsert, update, delete, vector_upsert, put_object, or enqueue_outbox_event",
        );
    }

    #[test]
    fn tx_object_failed_preconditions_carry_typed_detail() {
        let replay = mysql_xa_plan_replay_status("unsupported SQL");
        assert_typed_detail(
            &replay,
            ErrorKind::Schema,
            "mysql",
            "xa_plan_replay",
            "mysql_xa_plan_replay",
            "2PC refused before side effects: plan SQL cannot be replayed on the MySQL XA participant: unsupported SQL",
        );

        let unsupported =
            xa_unsupported_participants_status(&["mysql".to_string(), "qdrant".to_string()]);
        assert_typed_detail(
            &unsupported,
            ErrorKind::Capability,
            "transaction",
            "xa_commit",
            "xa_participants",
            "2PC refused before side effects: unsupported participants mysql, qdrant",
        );

        let encryption = record_encryption_key_missing_status("private", "accounts");
        assert_typed_detail(
            &encryption,
            ErrorKind::Capability,
            "encryption",
            "record_encryption",
            "udb_encryption_key",
            "table private.accounts contains encrypted columns but UDB encryption key is not configured",
        );

        let s3_feature = s3_feature_disabled_status();
        assert_typed_detail(
            &s3_feature,
            ErrorKind::Capability,
            "s3",
            "put_tx_object",
            "s3_feature",
            "s3/object-store feature is not enabled",
        );
    }

    #[test]
    fn tx_object_internal_status_carries_typed_detail() {
        let err = tx_object_internal_status(
            "enqueue_projection_tasks",
            "projection task enqueue failed: queue unavailable",
        );
        assert_internal_detail(
            &err,
            "enqueue_projection_tasks",
            "projection task enqueue failed: queue unavailable",
        );

        let err = tx_object_internal_status(
            "xa_ledger_commit_record",
            "XA committed but ledger write failed for xid xa-1: unavailable",
        );
        assert_internal_detail(
            &err,
            "xa_ledger_commit_record",
            "XA committed but ledger write failed for xid xa-1: unavailable",
        );
    }

    #[tokio::test]
    async fn create_materialized_view_admin_scope_denial_carries_policy_detail() {
        let runtime = DataBrokerRuntime::default();
        let err = runtime
            .create_materialized_view(
                &CatalogManifest::default(),
                ViewDefinition::default(),
                RequestContext::default(),
            )
            .await
            .expect_err("admin scope denial must fail before validation or pg pool");

        assert_policy_detail(
            &err,
            "create_materialized_view",
            "admin_scope_required",
            "scope udb:admin is required",
        );
    }

    #[tokio::test]
    async fn create_materialized_view_validation_carries_field_violations() {
        let runtime = DataBrokerRuntime::default();
        let manifest = CatalogManifest::default();
        let request = ViewDefinition {
            schema: "public".to_string(),
            name: "missing_view".to_string(),
            ..ViewDefinition::default()
        };

        let err = runtime
            .create_materialized_view(&manifest, request, admin_context())
            .await
            .expect_err("undeclared materialized view must fail before pg pool");

        assert_single_field_violation(&err, "view", "must be declared in the proto AST");

        let manifest = CatalogManifest {
            tables: vec![ManifestTable {
                materialized_views: vec![ManifestMaterializedView {
                    schema: "public".to_string(),
                    name: "orders_mv".to_string(),
                    query: "SELECT 1".to_string(),
                    with_data: false,
                }],
                ..ManifestTable::default()
            }],
            ..CatalogManifest::default()
        };
        let request = ViewDefinition {
            schema: "public".to_string(),
            name: "orders_mv".to_string(),
            query: "SELECT 2".to_string(),
            ..ViewDefinition::default()
        };

        let err = runtime
            .create_materialized_view(&manifest, request, admin_context())
            .await
            .expect_err("mismatched materialized view query must fail before pg pool");

        assert_single_field_violation(&err, "query", "must match the proto AST declaration");
    }

    /// Pin: a transaction issues exactly one select-cache invalidation pattern
    /// per distinct table it touched — the same drop the unary upsert/update/
    /// delete paths do — with empty message types skipped. Reverting the
    /// begin_tx invalidation (or narrowing it to the wrong scope) regresses the
    /// read-your-writes-through-cache guarantee this pins.
    #[test]
    fn select_invalidation_patterns_dedups_and_skips_empty_message_types() {
        let invoice = Mutation {
            message_type: "acme.billing.v1.Invoice".to_string(),
            operation: "upsert".to_string(),
            ..Mutation::default()
        };
        // Same table touched twice in one tx -> a single pattern.
        let invoice_again = invoice.clone();
        let product = Mutation {
            message_type: "acme.billing.v1.Product".to_string(),
            operation: "delete".to_string(),
            ..Mutation::default()
        };
        // A whitespace/empty message type (non-entity op) contributes nothing.
        let empty = Mutation {
            message_type: "   ".to_string(),
            operation: "upsert".to_string(),
            ..Mutation::default()
        };
        let refs: Vec<&Mutation> = vec![&invoice, &product, &invoice_again, &empty];
        assert_eq!(
            select_invalidation_patterns(&refs),
            vec![
                "udb:select:*:*:*:acme.billing.v1.Invoice:*".to_string(),
                "udb:select:*:*:*:acme.billing.v1.Product:*".to_string(),
            ],
            "one deduped select pattern per touched table, empties skipped"
        );
    }

    /// Pin: cache invalidation fires ONLY for a transaction that actually
    /// committed (plain COMMIT or 2PC both push TxStateCommitted). A rolled-back
    /// / failed run must not drop cache for writes that never became durable.
    #[test]
    fn transaction_committed_is_true_only_when_a_commit_status_is_present() {
        let committed = vec![Ok(TxStatus {
            state: crate::proto::tx_status::State::TxStateCommitted as i32,
            ..TxStatus::default()
        })];
        assert!(transaction_committed(&committed));

        let rolled_back = vec![Ok(TxStatus {
            state: crate::proto::tx_status::State::TxStateRolledBack as i32,
            ..TxStatus::default()
        })];
        assert!(!transaction_committed(&rolled_back));

        let errored: Vec<Result<TxStatus, tonic::Status>> =
            vec![Err(tx_object_internal_status("commit", "commit failed"))];
        assert!(!transaction_committed(&errored));

        // A committed status anywhere in the batch counts.
        let mixed = vec![
            Err(tx_object_internal_status("commit", "partial")),
            Ok(TxStatus {
                state: crate::proto::tx_status::State::TxStateCommitted as i32,
                ..TxStatus::default()
            }),
        ];
        assert!(transaction_committed(&mixed));
    }

    /// bug #8.1 pin: an `expected` compare-and-swap precondition set on an
    /// operation without CAS semantics (vector_upsert/put_object/enqueue_outbox_
    /// event) is REJECTED with an InvalidArgument naming `expected`, never
    /// silently ignored. Reverting the guard (accepting/ignoring `expected` on
    /// those ops) regresses this.
    #[test]
    fn tx_cas_required_rejects_expected_on_non_cas_operation() {
        let expected =
            crate::runtime::executor_utils::json_to_struct(&serde_json::json!({"version": 1}));
        assert!(expected.is_some(), "fixture: expected struct must build");
        let mutation = Mutation {
            operation: "vector_upsert".to_string(),
            message_type: "acme.billing.v1.Invoice".to_string(),
            expected,
            ..Mutation::default()
        };
        let err = tx_cas_required(&mutation, "vector_upsert")
            .expect_err("expected on a non-CAS operation must be rejected, not ignored");
        assert_single_field_violation(
            &err,
            "expected",
            "compare-and-swap is only supported for upsert, update, and delete",
        );
    }

    /// bug #8.1 pin: CAS is gated exactly on a NON-EMPTY `expected` for the three
    /// CAS-capable operations, and is a no-op (the hot path) otherwise — an
    /// unset OR empty `expected` never triggers a precondition SELECT.
    #[test]
    fn tx_cas_required_gates_only_supported_ops_and_is_noop_without_expected() {
        // Unset `expected` -> never required, for every operation.
        for op in ["upsert", "update", "delete", "vector_upsert", "put_object"] {
            let mutation = Mutation {
                operation: op.to_string(),
                ..Mutation::default()
            };
            assert!(
                !tx_cas_required(&mutation, op).expect("unset expected must never error"),
                "unset expected -> CAS not required for {op}"
            );
        }
        // Empty `expected` (present but no fields) is also a no-op.
        let empty = Mutation {
            operation: "upsert".to_string(),
            expected: crate::runtime::executor_utils::json_to_struct(&serde_json::json!({})),
            ..Mutation::default()
        };
        assert!(
            !tx_cas_required(&empty, "upsert").expect("empty expected must never error"),
            "empty expected -> CAS not required"
        );
        // Non-empty `expected` -> required for each CAS-capable operation.
        let expected =
            crate::runtime::executor_utils::json_to_struct(&serde_json::json!({"version": 1}));
        for op in ["upsert", "update", "delete"] {
            let mutation = Mutation {
                operation: op.to_string(),
                expected: expected.clone(),
                ..Mutation::default()
            };
            assert!(
                tx_cas_required(&mutation, op).expect("supported op must not error"),
                "non-empty expected -> CAS required for {op}"
            );
        }
    }
}

/// The record's tenant value (empty when the table has no tenant column or the
/// record does not carry it) — the blind-index token's tenant scope (W11).
fn table_tenant_value(
    table: &ManifestTable,
    record: &serde_json::Map<String, JsonValue>,
) -> String {
    let tenant_column = table.table_security.tenant_column.trim();
    if tenant_column.is_empty() {
        return String::new();
    }
    record
        .get(tenant_column)
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string()
}
