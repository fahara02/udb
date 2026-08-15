//! The six `MeteringService` RPC handlers plus the two service-scoped helpers
//! (`windowed_usage` durable aggregate, `emit_quota_changed` outbox). Extracted
//! from the trait impl; `mod.rs` delegates one line to each. The tenant is always
//! taken from the VERIFIED claim, never the request body.

use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::metering::services::v1 as metering_pb;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    NativeEventContext, admit_on as native_admit_on, enqueue_outbox_event_with_context,
    native_next_page_token, native_offset_page_window, native_service_context, non_empty_json,
    tenant_only_native_service_context, validate_request_scope, validate_request_tenant,
};
use super::MeteringServiceImpl;
use super::calc::{bump_revision, now_unix, quota_decision, window_start_unix};
use super::config::{
    DEFAULT_LIST_LIMIT, DEFAULT_WINDOW_SECONDS, MAX_LIST_LIMIT, QUOTA_RULE_MSG, TOPIC_QUOTA_CHANGED,
};
use super::errors::{
    metering_capability_status, metering_internal_status, metering_nonnegative_field,
    metering_required_field,
};
use super::store::{
    install_metering_tenant_scope_sql, quota_conflict, quota_list_read, quota_read_exact,
    quota_record, quota_state_from_json, stored_quota_from_json, windowed_usage_sum_sql,
};

/// The durable windowed usage SUM for (tenant, metric). `Ok(used)` on success;
/// `Err` is propagated so callers can choose their availability posture.
pub(crate) async fn windowed_usage(
    svc: &MeteringServiceImpl,
    runtime: &DataBrokerRuntime,
    context: &crate::RequestContext,
    tenant_id: &str,
    metric: &str,
    window_start: i64,
) -> Result<i64, Status> {
    // Install the tenant RLS GUC before scanning usage_events. Installing it
    // inside the aggregate predicate is too late for PostgreSQL RLS planning:
    // the policy may evaluate current_setting(...) before the user WHERE
    // expression runs, which under-reports live usage as zero.
    let _ = (runtime, context);
    let Some(pool) = svc.pg_pool.as_ref() else {
        return Err(metering_capability_status(
            "query_usage",
            "postgres_store",
            "metering usage pool is not configured",
        ));
    };
    let mut tx = pool.begin().await.map_err(|err| {
        metering_internal_status(
            "windowed_usage_begin",
            format!("windowed usage transaction failed: {err}"),
        )
    })?;
    sqlx::query(install_metering_tenant_scope_sql())
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            metering_internal_status(
                "windowed_usage_tenant_scope",
                format!("windowed usage tenant scope failed: {err}"),
            )
        })?;
    let total: i64 = sqlx::query_scalar(windowed_usage_sum_sql())
        .bind(tenant_id)
        .bind(metric)
        .bind(window_start)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| {
            metering_internal_status(
                "windowed_usage_aggregate",
                format!("windowed usage aggregate failed: {err}"),
            )
        })?;
    tx.commit().await.map_err(|err| {
        metering_internal_status(
            "windowed_usage_commit",
            format!("windowed usage transaction commit failed: {err}"),
        )
    })?;
    Ok(total)
}

/// Emit the per-mutation versioned dot-topic outbox event (best-effort).
pub(crate) async fn emit_quota_changed(
    svc: &MeteringServiceImpl,
    tenant_id: &str,
    project_id: &str,
    metric: &str,
    revision: i64,
) {
    let Some(pool) = svc.pg_pool.as_ref() else {
        return;
    };
    let payload = serde_json::json!({
        "tenant_id": tenant_id,
        "project_id": project_id,
        "metric": metric,
        "revision": revision,
    });
    enqueue_outbox_event_with_context(
        pool,
        svc.outbox_relation.as_deref(),
        TOPIC_QUOTA_CHANGED,
        metric,
        tenant_id,
        project_id,
        payload,
        NativeEventContext {
            target_resource: metric.to_string(),
            ..NativeEventContext::default()
        },
        Some(&svc.metrics),
    )
    .await;
}

pub(crate) async fn record_usage(
    svc: &MeteringServiceImpl,
    request: Request<metering_pb::RecordUsageRequest>,
) -> Result<Response<metering_pb::RecordUsageResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // Cross-tenant guard FIRST: the body tenant_id must match the verified
    // claim/header. After this passes, the body value IS the verified tenant.
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let method = req.method.trim().to_string();
    if method.is_empty() {
        return Err(metering_required_field(
            "method",
            "must be a non-empty usage method",
            "method is required",
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "metering",
        OperationChannel::Write,
        &tenant_id,
        None,
    )
    .await?;

    // No store → metering is best-effort; never an error to the caller.
    let Some(pool) = svc.pg_pool.as_ref() else {
        return Ok(Response::new(metering_pb::RecordUsageResponse {
            recorded: false,
            message: "no metering store configured".to_string(),
            error: None,
        }));
    };

    let occurred = if req.occurred_at_unix > 0 {
        req.occurred_at_unix
    } else {
        now_unix()
    };
    // Explicit ingest is itself the requested durable operation. It must fail
    // the RPC when PostgreSQL rejects the append; only automatic admission
    // telemetry uses the separate fail-open wrapper.
    super::admission::record_usage_strict(
        pool,
        &tenant_id,
        req.principal_id.trim(),
        &method,
        req.unit.trim(),
        req.quantity,
        occurred,
    )
    .await?;

    Ok(Response::new(metering_pb::RecordUsageResponse {
        recorded: true,
        message: "usage recorded".to_string(),
        error: None,
    }))
}

pub(crate) async fn query_usage(
    svc: &MeteringServiceImpl,
    request: Request<metering_pb::QueryUsageRequest>,
) -> Result<Response<metering_pb::QueryUsageResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let metric = req.metric.trim().to_string();
    if metric.is_empty() {
        return Err(metering_required_field(
            "metric",
            "must be a non-empty metric name",
            "metric is required",
        ));
    }
    let window_seconds = if req.window_seconds > 0 {
        req.window_seconds
    } else {
        DEFAULT_WINDOW_SECONDS
    };
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "metering",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = tenant_only_native_service_context(&metadata, &tenant_id);

    let to_unix = now_unix();
    let from_unix = window_start_unix(to_unix, window_seconds);
    let used = windowed_usage(svc, runtime, &context, &tenant_id, &metric, from_unix)
        .await
        .map_err(|err| {
            tracing::warn!(
                target: "udb::metering",
                error = %err,
                tenant_id = %tenant_id,
                metric = %metric,
                "windowed usage aggregate failed; refusing to fabricate usage total",
            );
            err
        })?;

    Ok(Response::new(metering_pb::QueryUsageResponse {
        metric,
        used,
        window_seconds,
        from_unix,
        to_unix,
        message: String::new(),
        error: None,
    }))
}

pub(crate) async fn put_quota(
    svc: &MeteringServiceImpl,
    request: Request<metering_pb::PutQuotaRequest>,
) -> Result<Response<metering_pb::PutQuotaResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let project_id = req.project_id.trim().to_string();
    let metric = req.metric.trim().to_string();
    if metric.is_empty() {
        return Err(metering_required_field(
            "metric",
            "must be a non-empty metric name",
            "metric is required",
        ));
    }
    if req.limit_value < 0 {
        return Err(metering_nonnegative_field(
            "limit_value",
            "limit_value must be >= 0",
        ));
    }
    if req.window_seconds < 0 {
        return Err(metering_nonnegative_field(
            "window_seconds",
            "window_seconds must be >= 0",
        ));
    }
    let window_seconds = if req.window_seconds > 0 {
        req.window_seconds
    } else {
        DEFAULT_WINDOW_SECONDS
    };
    let metadata_json = non_empty_json(&req.metadata_json);

    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "metering",
        OperationChannel::Write,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, &project_id);

    // Existing row at this exact scope (for stable quota_id + revision bump).
    let existing = runtime
        .native_entity_read_for_service(
            "metering",
            &context,
            quota_read_exact(&tenant_id, &project_id, &metric),
        )
        .await?
        .first()
        .map(stored_quota_from_json);

    let quota_id = existing
        .as_ref()
        .map(|q| q.quota_id.clone())
        .filter(|id| !id.trim().is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let revision = bump_revision(existing.as_ref().map(|q| q.revision).unwrap_or(0));

    runtime
        .native_entity_write_for_service(
            "metering",
            &context,
            QUOTA_RULE_MSG,
            quota_record(
                &quota_id,
                &tenant_id,
                &project_id,
                &metric,
                req.limit_value,
                window_seconds,
                req.enabled,
                revision,
                &metadata_json,
            ),
            quota_conflict(),
        )
        .await?;

    emit_quota_changed(svc, &tenant_id, &project_id, &metric, revision).await;

    Ok(Response::new(metering_pb::PutQuotaResponse {
        stored: true,
        metric,
        revision,
        message: "quota stored".to_string(),
        error: None,
    }))
}

pub(crate) async fn get_quota(
    svc: &MeteringServiceImpl,
    request: Request<metering_pb::GetQuotaRequest>,
) -> Result<Response<metering_pb::GetQuotaResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let project_id = req.project_id.trim().to_string();
    let metric = req.metric.trim().to_string();
    if metric.is_empty() {
        return Err(metering_required_field(
            "metric",
            "must be a non-empty metric name",
            "metric is required",
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "metering",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, &project_id);

    let found = runtime
        .native_entity_read_for_service(
            "metering",
            &context,
            quota_read_exact(&tenant_id, &project_id, &metric),
        )
        .await?
        .first()
        .map(|row| quota_state_from_json(row, &tenant_id));

    Ok(Response::new(metering_pb::GetQuotaResponse {
        found: found.is_some(),
        quota: found,
        message: String::new(),
        error: None,
    }))
}

pub(crate) async fn list_quotas(
    svc: &MeteringServiceImpl,
    request: Request<metering_pb::ListQuotasRequest>,
) -> Result<Response<metering_pb::ListQuotasResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let project_id = req.project_id.trim().to_string();
    let legacy_limit = if req.limit == 0 {
        DEFAULT_LIST_LIMIT
    } else {
        req.limit.min(MAX_LIST_LIMIT)
    };
    let requested_page_size = if req.page_size > 0 {
        req.page_size
    } else {
        legacy_limit as i32
    };
    let page_window = native_offset_page_window(
        1,
        requested_page_size,
        &req.page_token,
        DEFAULT_LIST_LIMIT as i32,
    );
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "metering",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, &project_id);

    let rows = runtime
        .native_entity_read_for_service(
            "metering",
            &context,
            quota_list_read(
                &tenant_id,
                &project_id,
                page_window.offset as u64,
                (page_window.limit as u32).min(MAX_LIST_LIMIT),
            ),
        )
        .await?;
    let quotas = rows
        .iter()
        .map(|row| quota_state_from_json(row, &tenant_id))
        .collect::<Vec<_>>();
    let next_page_token =
        native_next_page_token(page_window.offset, page_window.limit, quotas.len());

    Ok(Response::new(metering_pb::ListQuotasResponse {
        quotas,
        message: String::new(),
        error: None,
        next_page_token,
    }))
}

pub(crate) async fn check_quota(
    svc: &MeteringServiceImpl,
    request: Request<metering_pb::CheckQuotaRequest>,
) -> Result<Response<metering_pb::CheckQuotaResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let project_id = req.project_id.trim().to_string();
    let metric = req.metric.trim().to_string();
    if metric.is_empty() {
        return Err(metering_required_field(
            "metric",
            "must be a non-empty metric name",
            "metric is required",
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "metering",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, &project_id);

    // Resolve the governing rule. No enabled rule → unenforced (allowed).
    let rule = runtime
        .native_entity_read_for_service(
            "metering",
            &context,
            quota_read_exact(&tenant_id, &project_id, &metric),
        )
        .await?
        .first()
        .map(stored_quota_from_json)
        .filter(|q| q.enabled);

    let Some(rule) = rule else {
        return Ok(Response::new(metering_pb::CheckQuotaResponse {
            allowed: true,
            used: 0,
            limit_value: 0,
            remaining: 0,
            unlimited: true,
            message: "no enabled quota rule for metric".to_string(),
            error: None,
        }));
    };

    let window_seconds = if rule.window_seconds > 0 {
        rule.window_seconds
    } else {
        DEFAULT_WINDOW_SECONDS
    };
    let window_start = window_start_unix(now_unix(), window_seconds);

    match windowed_usage(svc, runtime, &context, &tenant_id, &metric, window_start).await {
        Ok(used) => {
            let (allowed, remaining) = quota_decision(used, rule.limit_value);
            Ok(Response::new(metering_pb::CheckQuotaResponse {
                allowed,
                used,
                limit_value: rule.limit_value,
                remaining,
                unlimited: false,
                message: if allowed {
                    "within quota".to_string()
                } else {
                    "quota exceeded".to_string()
                },
                error: None,
            }))
        }
        Err(err) => {
            tracing::warn!(
                target: "udb::metering",
                error = %err,
                tenant_id = %tenant_id,
                metric = %metric,
                "quota usage aggregate failed; failing closed",
            );
            Err(crate::runtime::executor_utils::retryable_status(
                "metering",
                "quota_aggregate",
                crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                "quota usage aggregate unavailable; failing closed",
            ))
        }
    }
}
