//! `BulkUpsert`: an audited bulk load of whole rows into one table.
//!
//! One batch is ONE write transaction. Every row passes through the same
//! chokepoints as a single `Upsert` — scope autofill, the planner's
//! validation (tenant/project binding, unknown fields, the shared-table write
//! rule), client-ciphertext refusal, canonicalisation, encryption and the
//! request's RLS settings — and only the per-row COST changes:
//!
//! * rows are written with multi-row `INSERT … ON CONFLICT` statements built
//!   from the planner's own single-row statement (same column list, same
//!   placeholder casts, same conflict action and scope guard), as many rows
//!   per statement as PostgreSQL's 65535-parameter limit allows;
//! * row revisions are bumped and projection tasks are enqueued with one
//!   statement each for the whole batch;
//! * the batch emits ONE change event (operation `bulk_upsert`) and ONE audit
//!   record that summarise it (row count, key hash, batch id).
//!
//! Statements per batch therefore grow with `ceil(rows * columns / 65535)`,
//! not with rows; the response reports the exact count.

use super::*;
use sha2::Digest as _;

/// PostgreSQL's limit on bind parameters in one statement.
const PG_MAX_BIND_PARAMS: usize = 65_535;

/// Longest caller-supplied batch label.
const BULK_BATCH_ID_MAX_CHARS: usize = 128;

/// Change-event / audit operation name of a bulk batch.
const BULK_UPSERT_OPERATION: &str = "bulk_upsert";

/// Rate-limit units a batch of `rows` costs: `ceil(rows / rows_per_unit)`,
/// at least one.
pub(crate) fn bulk_upsert_rate_cost(rows: usize, rows_per_unit: u32) -> u64 {
    let per_unit = u64::from(rows_per_unit.max(1));
    (rows as u64).div_ceil(per_unit).max(1)
}

/// Rows one INSERT may carry when each row binds `columns` parameters.
fn rows_per_statement(columns: usize) -> usize {
    (PG_MAX_BIND_PARAMS / columns.max(1)).max(1)
}

/// Split the planner's single-row upsert SQL into
/// `("INSERT INTO … (cols) VALUES ", "($1, $2::T, …)", " ON CONFLICT …")`.
///
/// The bulk statement is built FROM the planner's statement, so the column
/// list, the per-column placeholder casts and the conflict action (including
/// the tenant/project scope guard) are exactly the ones a single Upsert runs.
/// Any other shape is refused rather than guessed at.
fn split_single_row_upsert_sql(sql: &str, columns: usize) -> Result<(&str, &str, &str), String> {
    const VALUES: &str = ") VALUES (";
    let values_at = sql
        .find(VALUES)
        .ok_or_else(|| format!("planner upsert SQL has no VALUES list: {sql}"))?;
    let tuple_start = values_at + VALUES.len() - 1;
    let tail_start = sql[tuple_start..]
        .find(" ON CONFLICT (")
        .map(|offset| tuple_start + offset)
        .ok_or_else(|| format!("planner upsert SQL has no ON CONFLICT clause: {sql}"))?;
    let tuple = &sql[tuple_start..tail_start];
    if !tuple.starts_with('(') || !tuple.ends_with(')') {
        return Err(format!(
            "planner upsert SQL VALUES tuple is malformed: {sql}"
        ));
    }
    let tail = &sql[tail_start..];
    if tail.contains(" RETURNING") {
        return Err(format!("planner upsert SQL already returns rows: {sql}"));
    }
    let placeholders = placeholder_numbers(tuple);
    let expected: Vec<usize> = (1..=columns).collect();
    if placeholders != expected {
        return Err(format!(
            "planner upsert SQL binds {placeholders:?}, expected $1..${columns}: {sql}"
        ));
    }
    Ok((&sql[..tuple_start], tuple, tail))
}

/// Every `$n` placeholder in `text`, in order.
fn placeholder_numbers(text: &str) -> Vec<usize> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'$' {
            let start = index + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end > start
                && let Ok(number) = text[start..end].parse::<usize>()
            {
                out.push(number);
            }
            index = end.max(index + 1);
        } else {
            index += 1;
        }
    }
    out
}

/// `tuple` with every `$n` replaced by `$(n + offset)`.
fn renumber_placeholders(tuple: &str, offset: usize) -> String {
    let bytes = tuple.as_bytes();
    let mut out = String::with_capacity(tuple.len() + 8);
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'$' {
            let start = index + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if let (true, Ok(number)) = (end > start, tuple[start..end].parse::<usize>()) {
                out.push('$');
                out.push_str(&(number + offset).to_string());
                index = end;
                continue;
            }
        }
        // Placeholders and casts are ASCII; copy any other char whole.
        let ch = tuple[index..].chars().next().unwrap_or('\u{0}');
        out.push(ch);
        index += ch.len_utf8().max(1);
    }
    out
}

/// The multi-row statement for `rows` rows of `columns` parameters each.
fn multi_row_upsert_sql(
    single_row_sql: &str,
    columns: usize,
    rows: usize,
    returning: &str,
) -> Result<String, String> {
    let (head, tuple, tail) = split_single_row_upsert_sql(single_row_sql, columns)?;
    let mut sql = String::with_capacity(head.len() + rows * (tuple.len() + 8) + tail.len());
    sql.push_str(head);
    for row in 0..rows {
        if row > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&renumber_placeholders(tuple, row * columns));
    }
    sql.push_str(tail);
    sql.push_str(returning);
    Ok(sql)
}

/// Whether a conflict-key value may appear in the change event / response.
fn key_column_is_sensitive(table: &ManifestTable, column: &str) -> bool {
    table
        .columns
        .iter()
        .find(|candidate| candidate.column_name == column)
        .is_some_and(|candidate| {
            candidate.encrypted
                || candidate.security.is_pii
                || candidate.security.is_encrypted
                || candidate.security.is_blind_index
                || candidate.security.mask_in_logs
        })
}

/// The batch's key digest: SHA-256 (hex) of each row's key tuple (the same
/// token form the row-revision map uses, parts joined by U+001F) joined by
/// `\n`, in request order.
fn keys_sha256(key_tuples: &[String]) -> String {
    let mut hasher = sha2::Sha256::new();
    for (index, tuple) in key_tuples.iter().enumerate() {
        if index > 0 {
            hasher.update(b"\n");
        }
        hasher.update(tuple.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn bulk_refusal(
    reason: crate::runtime::error_reasons::ErrorReason,
    field: &str,
    description: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::error_reasons::Refusal::new(reason, message)
        .field(field, description)
        .into_status()
}

/// The batch label: the caller's, checked, or a fresh UUID.
fn resolve_batch_id(batch_id: &str) -> Result<String, tonic::Status> {
    if batch_id.is_empty() {
        return Ok(Uuid::new_v4().to_string());
    }
    if batch_id.chars().count() > BULK_BATCH_ID_MAX_CHARS
        || batch_id
            .chars()
            .any(|ch| ch.is_whitespace() || ch.is_control())
    {
        return Err(setup_data_invalid_field(
            "batch_id",
            format!(
                "must be empty or at most {BULK_BATCH_ID_MAX_CHARS} characters with no whitespace or control characters"
            ),
            "batch_id is not a valid batch label",
        ));
    }
    Ok(batch_id.to_string())
}

/// One prepared row: its bind values and its conflict-key tuple.
struct PreparedRow {
    values: Vec<JsonValue>,
    key_tuple: String,
    key_has_null: bool,
}

impl DataBrokerRuntime {
    /// Write one `BulkUpsert` batch (see the module docs). All rows or none.
    pub(crate) async fn bulk_upsert(
        &self,
        manifest: &CatalogManifest,
        request: crate::proto::BulkUpsertRequest,
        metadata_context: RequestContext,
    ) -> Result<crate::proto::BulkUpsertResponse, tonic::Status> {
        let context = merge_context(request.context.as_ref(), metadata_context);
        typed_relational_backend_guard(&context, &self.backend_instances, BULK_UPSERT_OPERATION)?;
        let row_count = request.records_json.len();
        if row_count == 0 {
            return Err(setup_data_invalid_field(
                "records_json",
                "must contain at least one row",
                "BulkUpsert requires at least one row",
            ));
        }
        let max_rows = self.config.service.bulk_upsert_max_rows.max(1);
        if row_count > max_rows {
            return Err(bulk_refusal(
                crate::runtime::error_reasons::BULK_BATCH_TOO_LARGE,
                "records_json",
                format!("must not exceed {max_rows} rows (UDB_BULK_UPSERT_MAX_ROWS)"),
                format!(
                    "BulkUpsert batch of {row_count} rows exceeds the {max_rows}-row maximum; nothing was written"
                ),
            ));
        }
        let batch_id = resolve_batch_id(&request.batch_id)?;
        let table = resolve_table_for_message(manifest, &request.message_type)
            .map_err(|_| message_type_lookup_status(manifest, &request.message_type))?;

        // ── Per-row chokepoints (CPU only, no SQL) ───────────────────────────
        let mut parsed_rows: Vec<JsonValue> = Vec::with_capacity(row_count);
        let mut final_plan: Option<crate::broker::SqlOperationPlan> = None;
        let mut first_shape: Option<Vec<String>> = None;
        let mut conflict_columns: Vec<String> = Vec::new();
        let mut prepared: Vec<PreparedRow> = Vec::with_capacity(row_count);
        for (index, raw) in request.records_json.iter().enumerate() {
            let at_row = |status: tonic::Status| {
                crate::runtime::executor_utils::prefix_status(&format!("bulk row {index}"), status)
            };
            let mut record: JsonValue = serde_json::from_slice(raw).map_err(|err| {
                setup_data_invalid_field(
                    format!("records_json[{index}]"),
                    "must be one JSON object",
                    format!("bulk row {index}: records_json must be valid JSON: {err}"),
                )
            })?;
            if !record.is_object() {
                return Err(setup_data_invalid_field(
                    format!("records_json[{index}]"),
                    "must be one JSON object",
                    format!("bulk row {index}: a row must be a JSON object"),
                ));
            }
            crate::runtime::core::scope_autofill::autofill_record(table, &mut record, &context)
                .map_err(at_row)?;
            let plan_request = UpsertPlanRequest {
                context: context.clone(),
                message_type: request.message_type.clone(),
                record: record.clone(),
                conflict_fields: request.conflict_fields.clone(),
                insert_only: false,
                return_record: false,
                bypass_cache_write: false,
            };
            // The planner's validation: tenant/project binding, unknown
            // fields, required scope and the shared-table write rule.
            let validation = build_upsert_plan(manifest, &plan_request);
            reject_plan(&validation.errors).map_err(at_row)?;
            if index == 0 {
                conflict_columns = validation.conflict_columns.clone();
            }
            let normalized = crate::broker::normalize_record_keys(table, &record);
            crate::runtime::executor_utils::validate_client_encrypted_write(
                &table.columns,
                &normalized,
            )
            .map_err(at_row)?;
            let canonical = crate::runtime::executor_utils::canonical_record(
                table,
                &normalized,
                crate::runtime::executor_utils::JsonText::AsSent,
            )
            .map_err(at_row)?;
            let encrypted = self
                .encrypt_record_for_table(table, &canonical)
                .map_err(at_row)?;
            let planner_record =
                crate::runtime::executor_utils::json_column_strings_parsed(table, &encrypted)
                    .map_err(at_row)?;
            // Every row must bind the same columns: one statement shape for
            // the batch, and no row silently gains or loses a column.
            let mut shape: Vec<String> = planner_record
                .as_object()
                .map(|object| object.keys().cloned().collect())
                .unwrap_or_default();
            shape.sort();
            match &first_shape {
                None => {
                    let plan = build_upsert_plan(
                        manifest,
                        &UpsertPlanRequest {
                            record: planner_record.clone(),
                            ..plan_request
                        },
                    );
                    reject_plan(&plan.errors).map_err(at_row)?;
                    if plan.parameter_columns.is_empty() {
                        return Err(setup_data_invalid_field(
                            "records_json",
                            "rows must carry at least one writable column",
                            "BulkUpsert rows carry no writable column",
                        ));
                    }
                    final_plan = Some(plan);
                    first_shape = Some(shape);
                }
                Some(expected) if *expected != shape => {
                    let missing: Vec<&String> =
                        expected.iter().filter(|key| !shape.contains(key)).collect();
                    let extra: Vec<&String> =
                        shape.iter().filter(|key| !expected.contains(key)).collect();
                    return Err(bulk_refusal(
                        crate::runtime::error_reasons::BULK_ROW_SHAPE_MISMATCH,
                        &format!("records_json[{index}]"),
                        format!(
                            "must carry the same fields as row 0 (missing {missing:?}, extra {extra:?})"
                        ),
                        format!(
                            "bulk row {index} carries a different set of fields than row 0; nothing was written"
                        ),
                    ));
                }
                Some(_) => {}
            }
            let plan = final_plan
                .as_ref()
                .expect("the first row always builds the batch plan");
            let values = record_values(&planner_record, &plan.parameter_columns)?;
            let key_values = record_values(&canonical, &conflict_columns)?;
            prepared.push(PreparedRow {
                values,
                key_tuple: row_key_text(&pk_tuple_canonical(&key_values)),
                key_has_null: key_values.iter().any(JsonValue::is_null),
            });
            parsed_rows.push(canonical);
        }
        let plan = final_plan.expect("a non-empty batch builds its plan");

        // Two rows with one conflict key would write the same row twice (and
        // PostgreSQL refuses that inside one statement). Refuse up front.
        {
            let mut seen: std::collections::HashMap<&str, usize> =
                std::collections::HashMap::with_capacity(prepared.len());
            for (index, row) in prepared.iter().enumerate() {
                if row.key_has_null {
                    continue;
                }
                if let Some(first) = seen.insert(row.key_tuple.as_str(), index) {
                    return Err(bulk_refusal(
                        crate::runtime::error_reasons::BULK_DUPLICATE_KEY,
                        &format!("records_json[{index}]"),
                        format!(
                            "repeats the conflict key ({}) of row {first}",
                            conflict_columns.join(", ")
                        ),
                        format!(
                            "bulk rows {first} and {index} carry the same conflict key; nothing was written"
                        ),
                    ));
                }
            }
        }
        let key_tuples: Vec<String> = prepared.iter().map(|row| row.key_tuple.clone()).collect();
        let keys_digest = keys_sha256(&key_tuples);
        let keys_disclosable = !conflict_columns
            .iter()
            .any(|column| key_column_is_sensitive(table, column));
        let (first_key, last_key) = if keys_disclosable {
            (
                key_tuples.first().cloned().unwrap_or_default(),
                key_tuples.last().cloned().unwrap_or_default(),
            )
        } else {
            (String::new(), String::new())
        };

        let projection_plans = crate::runtime::projection::ProjectionPlan::from_manifest(manifest);
        let projects_this_message = projection_plans.iter().any(|projection| {
            crate::runtime::projection::message_type_matches(
                &projection.message_type,
                &request.message_type,
            )
        });
        // Rows come back so revisions use the stored key (database-assigned
        // keys included) and projections carry the stored row, as replay does.
        let returning = if projects_this_message {
            " RETURNING *".to_string()
        } else if !table.primary_key.is_empty() {
            format!(
                " RETURNING {}",
                table
                    .primary_key
                    .iter()
                    .map(|column| crate::runtime::executor_utils::qi_runtime(column))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            String::new()
        };
        let declared_columns: Vec<Option<&crate::generation::ManifestColumn>> = plan
            .parameter_columns
            .iter()
            .map(|name| {
                table
                    .columns
                    .iter()
                    .find(|column| column.column_name == *name)
            })
            .collect();
        let columns = plan.parameter_columns.len();
        let chunk_rows = rows_per_statement(columns);
        let guarded_update = plan.sql.contains(" DO UPDATE SET ");

        // ── The transaction ──────────────────────────────────────────────────
        let config = crate::runtime::system::SystemCatalogConfig::current();
        let pool = self.pg_pool()?;
        let mut tx = pool.begin().await.map_err(|e| {
            setup_data_internal_status(
                "bulk_upsert_transaction_begin",
                format!("PG transaction begin failed: {e}"),
            )
        })?;
        set_request_local_settings(&mut tx, &context).await?;
        let mut statement_count: u32 = 1;
        let dedup_ctx = match idempotency_key_for_dedup(&request.idempotency_key)? {
            Some(key) => {
                let dedup_key = idempotency_dedup_key(
                    &context.tenant_id,
                    &context.project_id,
                    &request.message_type,
                    BULK_UPSERT_OPERATION,
                    key,
                );
                let request_hash = idempotency_request_hash(
                    BULK_UPSERT_OPERATION,
                    &serde_json::json!({
                        "message_type": request.message_type,
                        "conflict_fields": request.conflict_fields,
                        "batch_id": request.batch_id,
                        "rows": parsed_rows,
                    }),
                );
                let claim = claim_idempotency_key_in_tx(
                    &mut tx,
                    &config,
                    &dedup_key,
                    &context.tenant_id,
                    &context.project_id,
                    &request.message_type,
                    BULK_UPSERT_OPERATION,
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
                    return bulk_upsert_response_from_idempotency_json(&claim.prior_response_json);
                }
                statement_count += 1;
                Some(IdempotencyPersistContext {
                    config: config.clone(),
                    dedup_key,
                    tenant_id: context.tenant_id.clone(),
                    project_id: context.project_id.clone(),
                    message_type: request.message_type.clone(),
                    operation: BULK_UPSERT_OPERATION,
                })
            }
            None => None,
        };

        let mut affected_rows: i64 = 0;
        let mut returned_rows: Vec<JsonValue> = Vec::new();
        for chunk in prepared.chunks(chunk_rows) {
            let sql = multi_row_upsert_sql(&plan.sql, columns, chunk.len(), &returning)
                .map_err(|err| setup_data_internal_status("bulk_upsert_statement", err))?;
            let mut query = sqlx::query(&sql);
            for row in chunk {
                for (column, value) in declared_columns.iter().zip(&row.values) {
                    query = bind_one(query, *column, value)?;
                }
            }
            let outcome = if returning.is_empty() {
                query
                    .execute(&mut *tx)
                    .await
                    .map(|result| (result.rows_affected() as i64, Vec::new()))
            } else {
                query
                    .fetch_all(&mut *tx)
                    .await
                    .map(|rows| (rows.len() as i64, rows))
            };
            let (chunk_affected, rows) = outcome.map_err(|err| {
                tracing::error!(
                    message_type = %request.message_type,
                    tenant_id = %context.tenant_id,
                    batch_id = %batch_id,
                    rows = chunk.len(),
                    cause = %crate::runtime::executor_utils::sqlx_error_log_summary(&err),
                    "PostgreSQL bulk upsert statement failed; the batch is rolled back"
                );
                crate::runtime::executor_utils::sqlx_error_to_status(
                    "PostgreSQL bulk upsert failed; nothing was written",
                    &err,
                )
            })?;
            returned_rows.extend(stored_row_values(&rows)?);
            statement_count += 1;
            // A guarded DO UPDATE affects exactly one row per record unless the
            // conflicting row belongs to another tenant/project: fewer rows
            // means the batch reached outside the caller's scope. Refuse it
            // (the transaction rolls back on drop), as a single Upsert does.
            if guarded_update
                && chunk_affected < chunk.len() as i64
                && let Some(status) = upsert_scope_guard_refusal(&sql, 0)
            {
                return Err(crate::runtime::executor_utils::prefix_status(
                    "bulk upsert",
                    status,
                ));
            }
            affected_rows += chunk_affected;
        }

        let mut projection_task_ids = Vec::new();
        let mut event_id = String::new();
        if affected_rows > 0 {
            // Row revisions: one statement for the whole batch, keyed exactly
            // like a single write's bump, so a later conditional write sees
            // that the bulk load changed the row.
            if !table.primary_key.is_empty() && !returned_rows.is_empty() {
                let mut revision_keys = Vec::with_capacity(returned_rows.len());
                let mut row_keys = Vec::with_capacity(returned_rows.len());
                let mut seen = std::collections::HashSet::with_capacity(returned_rows.len());
                for row in &returned_rows {
                    let pk_values = record_values(row, &table.primary_key)?;
                    if pk_values.iter().any(JsonValue::is_null) {
                        continue;
                    }
                    let pk_canonical = pk_tuple_canonical(&pk_values);
                    let revision_key = row_revision_key(
                        &context.tenant_id,
                        &context.project_id,
                        &request.message_type,
                        &pk_canonical,
                    );
                    if seen.insert(revision_key.clone()) {
                        revision_keys.push(revision_key);
                        row_keys.push(row_key_text(&pk_canonical));
                    }
                }
                if !revision_keys.is_empty() {
                    bump_row_revisions_batch_in_tx(
                        &mut tx,
                        &config,
                        &context.tenant_id,
                        &context.project_id,
                        &request.message_type,
                        revision_keys,
                        row_keys,
                    )
                    .await?;
                    statement_count += 1;
                }
            }
            if projects_this_message {
                let (task_ids, statements) =
                    crate::runtime::projection::ProjectionEngine::enqueue_write_tasks_batch_tx(
                        &mut tx,
                        &config,
                        crate::runtime::projection::task_project_id(&context.project_id),
                        &request.message_type,
                        "upsert",
                        &returned_rows,
                        &projection_plans,
                    )
                    .await
                    .map_err(|err| {
                        setup_data_internal_status(
                            "bulk_upsert_projection_task_enqueue",
                            format!("projection task enqueue failed: {err}"),
                        )
                    })?;
                projection_task_ids = task_ids;
                statement_count += statements;
            }
            // ONE change event for the batch, through the same gate, envelope,
            // redaction and outbox INSERT as a single write's event.
            let summary = serde_json::json!({
                "batch_id": batch_id,
                "message_type": request.message_type,
                "row_count": row_count,
                "affected_rows": affected_rows,
                "keys_sha256": keys_digest,
                "first_key": first_key,
                "last_key": last_key,
                "conflict_columns": conflict_columns,
            });
            if let Some(id) = self
                .emit_cdc_outbox_event_checked(
                    &mut tx,
                    manifest,
                    &request.message_type,
                    BULK_UPSERT_OPERATION,
                    &summary,
                    &context,
                    false,
                    Some(&batch_id),
                )
                .await?
            {
                event_id = id.to_string();
                statement_count += 1;
            }
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
        let mut response = crate::proto::BulkUpsertResponse {
            batch_id: batch_id.clone(),
            row_count: row_count as u32,
            affected_rows,
            keys_sha256: keys_digest.clone(),
            first_key,
            last_key,
            write_receipt_json,
            statement_count,
            event_id,
            was_duplicate: false,
        };
        if let Some(dedup_ctx) = dedup_ctx.as_ref() {
            response.statement_count += 1;
            persist_bulk_upsert_idempotency_in_tx(&mut tx, dedup_ctx, &response).await?;
        }
        tx.commit().await.map_err(|err| {
            crate::runtime::executor_utils::sqlx_error_to_status(
                "PostgreSQL bulk upsert commit failed",
                &err,
            )
        })?;

        let _ = self
            .cache_delete_pattern(&cache_invalidation_pattern(
                "select",
                &crate::runtime::executor_utils::cache_message_identity(
                    manifest,
                    &request.message_type,
                ),
            ))
            .await;
        // ONE audit record for the committed batch: the table, the batch id
        // and the row count in the resource, the key digest as checksum.
        if let Some(event) = crate::planning::broker::build_audit_event(
            &context,
            BULK_UPSERT_OPERATION,
            &format!(
                "sql://{}/{}?batch_id={}&rows={}",
                table.schema, table.table, batch_id, affected_rows
            ),
            &keys_digest,
            affected_rows,
        ) {
            crate::runtime::core::audit::emit_audit(
                &self.config.audit_sink,
                &event,
                self.pg_pool.as_ref(),
            );
        }
        Ok(response)
    }
}

/// Bump the opaque revision of many rows with ONE statement (see
/// `bump_row_revision_in_tx` for the single-row contract this mirrors).
async fn bump_row_revisions_batch_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    config: &crate::runtime::system::SystemCatalogConfig,
    tenant_id: &str,
    project_id: &str,
    message_type: &str,
    revision_keys: Vec<String>,
    row_keys: Vec<String>,
) -> Result<(), tonic::Status> {
    let rel = config.row_revisions_relation();
    let sql = format!(
        "INSERT INTO {rel} AS rev \
             (revision_key, tenant_id, project_id, message_type, row_key, revision, updated_at) \
         SELECT u.revision_key, $3, $4, $5, u.row_key, 1, NOW() \
         FROM UNNEST($1::text[], $2::text[]) AS u(revision_key, row_key) \
         ON CONFLICT (revision_key) \
         DO UPDATE SET revision = rev.revision + 1, updated_at = NOW()"
    );
    sqlx::query(&sql)
        .bind(revision_keys)
        .bind(row_keys)
        .bind(tenant_id)
        .bind(project_id)
        .bind(message_type)
        .execute(&mut **tx)
        .await
        .map_err(|err| row_revision_store_status("row_revision_bump_batch", &err))?;
    Ok(())
}

/// Store the batch's response in its idempotency row (same transaction), so
/// a keyed retry replays it instead of writing again.
async fn persist_bulk_upsert_idempotency_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dedup_ctx: &IdempotencyPersistContext,
    response: &crate::proto::BulkUpsertResponse,
) -> Result<(), tonic::Status> {
    let summary = serde_json::json!({
        "op": BULK_UPSERT_OPERATION,
        "batch_id": response.batch_id,
        "row_count": response.row_count,
        "affected_rows": response.affected_rows,
        "keys_sha256": response.keys_sha256,
        "first_key": response.first_key,
        "last_key": response.last_key,
        "write_receipt_json": response.write_receipt_json,
        "statement_count": response.statement_count,
        "event_id": response.event_id,
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
                "bulk upsert idempotency persist failed",
                &err,
            )
        })?;
    idempotency_response_persist_row_count_status(result.rows_affected())
}

/// Rebuild the original response of a replayed batch (`was_duplicate`).
fn bulk_upsert_response_from_idempotency_json(
    prior: &JsonValue,
) -> Result<crate::proto::BulkUpsertResponse, tonic::Status> {
    if prior.get("op").and_then(JsonValue::as_str) != Some(BULK_UPSERT_OPERATION) {
        return Err(idempotency_replay_response_status(
            "the stored response is not a bulk upsert response",
        ));
    }
    let text = |field: &str| {
        prior
            .get(field)
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let number = |field: &str| {
        prior.get(field).and_then(JsonValue::as_i64).ok_or_else(|| {
            idempotency_replay_response_status(format!("missing bulk upsert field {field}"))
        })
    };
    Ok(crate::proto::BulkUpsertResponse {
        batch_id: text("batch_id"),
        row_count: number("row_count")? as u32,
        affected_rows: number("affected_rows")?,
        keys_sha256: text("keys_sha256"),
        first_key: text("first_key"),
        last_key: text("last_key"),
        write_receipt_json: text("write_receipt_json"),
        statement_count: number("statement_count")? as u32,
        event_id: text("event_id"),
        was_duplicate: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest as _;

    #[test]
    fn rate_cost_is_size_aware_and_at_least_one() {
        assert_eq!(bulk_upsert_rate_cost(1, 1000), 1);
        assert_eq!(bulk_upsert_rate_cost(1000, 1000), 1);
        assert_eq!(bulk_upsert_rate_cost(1001, 1000), 2);
        assert_eq!(bulk_upsert_rate_cost(10_000, 1000), 10);
        assert_eq!(bulk_upsert_rate_cost(0, 1000), 1);
        // A zero unit size is treated as one row per unit, never a division by zero.
        assert_eq!(bulk_upsert_rate_cost(7, 0), 7);
    }

    #[test]
    fn statements_hold_as_many_rows_as_the_parameter_limit_allows() {
        assert_eq!(rows_per_statement(1), 65_535);
        assert_eq!(rows_per_statement(4), 16_383);
        assert_eq!(rows_per_statement(20), 3_276);
        assert_eq!(rows_per_statement(70_000), 1);
        // 10 000 rows of 4 columns: one statement.
        assert_eq!(10_000usize.div_ceil(rows_per_statement(4)), 1);
    }

    const PLANNER_SQL: &str = "INSERT INTO \"s\".\"t\" (\"day\", \"id\", \"tenant_id\") \
        VALUES ($1::DATE[], $2, $3) ON CONFLICT (\"id\") DO UPDATE SET \"day\" = EXCLUDED.\"day\" \
        WHERE \"t\".\"tenant_id\" IS NOT DISTINCT FROM EXCLUDED.\"tenant_id\"";

    #[test]
    fn multi_row_statement_keeps_the_planner_casts_and_conflict_action() {
        let sql = multi_row_upsert_sql(PLANNER_SQL, 3, 3, " RETURNING \"id\"").unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"s\".\"t\" (\"day\", \"id\", \"tenant_id\") VALUES \
             ($1::DATE[], $2, $3), ($4::DATE[], $5, $6), ($7::DATE[], $8, $9) \
             ON CONFLICT (\"id\") DO UPDATE SET \"day\" = EXCLUDED.\"day\" \
             WHERE \"t\".\"tenant_id\" IS NOT DISTINCT FROM EXCLUDED.\"tenant_id\" RETURNING \"id\""
        );
        assert_eq!(placeholder_numbers(&sql), (1..=9).collect::<Vec<_>>());
    }

    #[test]
    fn numeric_casts_with_digits_are_not_renumbered() {
        assert_eq!(
            renumber_placeholders("($1::NUMERIC(10,2), $2::VARCHAR(255))", 10),
            "($11::NUMERIC(10,2), $12::VARCHAR(255))"
        );
    }

    #[test]
    fn unexpected_planner_shapes_are_refused_not_guessed() {
        // No ON CONFLICT (an insert-only plan) is not a bulk upsert plan.
        assert!(split_single_row_upsert_sql("INSERT INTO t (a) VALUES ($1)", 1).is_err());
        // A placeholder count that does not match the column count.
        assert!(
            split_single_row_upsert_sql(
                "INSERT INTO t (a, b) VALUES ($1) ON CONFLICT (a) DO NOTHING",
                2
            )
            .is_err()
        );
        // A plan that already returns rows.
        assert!(
            split_single_row_upsert_sql(
                "INSERT INTO t (a) VALUES ($1) ON CONFLICT (a) DO NOTHING RETURNING *",
                1
            )
            .is_err()
        );
        assert!(split_single_row_upsert_sql(PLANNER_SQL, 3).is_ok());
    }

    #[test]
    fn key_digest_uses_revision_tokens_in_request_order() {
        let a = row_key_text(&pk_tuple_canonical(&[
            serde_json::json!("x"),
            serde_json::json!(5),
        ]));
        assert_eq!(a, "s:x\u{1f}i:5");
        let b = row_key_text(&pk_tuple_canonical(&[serde_json::json!(true)]));
        assert_eq!(b, "b:true");
        let digest = keys_sha256(&[a.clone(), b.clone()]);
        let expected = format!("{:x}", sha2::Sha256::digest(format!("{a}\n{b}").as_bytes()));
        assert_eq!(digest, expected);
        assert_ne!(digest, keys_sha256(&[b, a]), "order is part of the digest");
    }

    #[test]
    fn batch_labels_are_bounded_and_printable() {
        assert!(resolve_batch_id("countries-2026-11/0007").is_ok());
        assert_eq!(resolve_batch_id("").unwrap().len(), 36);
        assert!(resolve_batch_id("has space").is_err());
        assert!(resolve_batch_id("tab\there").is_err());
        assert!(resolve_batch_id(&"x".repeat(129)).is_err());
        assert!(resolve_batch_id(&"x".repeat(128)).is_ok());
    }

    #[test]
    fn replay_requires_a_bulk_upsert_record() {
        assert!(
            bulk_upsert_response_from_idempotency_json(&serde_json::json!({"op":"bulk_cas"}))
                .is_err()
        );
        let replay = bulk_upsert_response_from_idempotency_json(&serde_json::json!({
            "op": "bulk_upsert", "batch_id": "b", "row_count": 3, "affected_rows": 3,
            "keys_sha256": "k", "first_key": "s:a", "last_key": "s:c",
            "write_receipt_json": "{}", "statement_count": 4, "event_id": "e"
        }))
        .unwrap();
        assert!(replay.was_duplicate);
        assert_eq!((replay.row_count, replay.affected_rows), (3, 3));
    }
}
