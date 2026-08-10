//! The twelve `NotificationService` RPC handlers, extracted from the trait impl as
//! free `pub(crate) async fn`s taking `svc` where the trait method took `&self`.
//! `mod.rs` delegates one line to each. Bodies are verbatim — the same
//! cross-tenant scope guards, per-tenant admission, hybrid-tenant template
//! resolution, `{{placeholder}}` rendering, transitional Postgres upserts, and
//! outbox emission as the former god file.

use sqlx::Row;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::ConflictStrategy;
use crate::proto::udb::core::notification::entity::v1 as notif_entity_pb;
use crate::proto::udb::core::notification::services::v1 as notif_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    admit_on as native_admit_on, metadata_tenant_id, native_page_response, native_page_window,
    native_service_context, parse_uuid, tenant_only_native_service_context,
    validate_request_scope, validate_request_tenant,
};
use super::NotificationServiceImpl;
use super::config::{
    DEFAULT_PAGE_SIZE, LOG_MSG, PREFERENCE_MSG, TEMPLATE_MSG, TEST_FORCE_FAILED_SENTINEL,
    VARIABLE_MISSING, test_mode_enabled,
};
use super::errors::{
    notification_internal_status, notification_log_not_found_status,
    notification_not_retryable_status, notification_required_field,
    notification_schema_not_found_status, notification_template_not_found_status,
    notification_tenant_metadata_required_status, status_with_reason,
};
use super::model::{
    channel_from_db, channel_send_decision, channel_to_db, deliverable_channels,
    delivery_attempt_from_row, json_i64_field, json_object, json_string_field, log_from_json,
    log_from_row, log_model, log_select_projection, preference_from_json_row, preference_from_row,
    preference_model, preference_select_projection, render_template, status_to_db,
    template_from_json_row, template_from_row, template_locale_or_default, template_model,
    template_select_projection,
};
use super::store::{
    delivery_stats_aggregate, is_notification_opted_out, log_transition_allowed_prev,
    notification_log_filter, notification_log_list_read, notification_log_read,
    notification_log_record, preference_list_filter, preference_list_read, preference_read,
    recipient_opted_out_db, reset_delivery_attempts_for_retry, suppress_log_if_pending,
    template_read, template_scope_filter, transition_log_status, write_delivery_attempt,
};

pub(crate) async fn send_notification(
    svc: &NotificationServiceImpl,
    request: Request<notif_pb::SendNotificationRequest>,
) -> Result<Response<notif_pb::SendNotificationResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
    if req.event_type.trim().is_empty() {
        return Err(notification_required_field(
            "event_type",
            "must be a non-empty notification event type",
            "event_type is required",
        ));
    }
    // Per-tenant fair admission (Write budget) so one tenant's send flood
    // can't starve the shared control plane.
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "notification",
        OperationChannel::Write,
        &req.tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &req.tenant_id, &req.project_id);
    // Default to EMAIL when the caller did not pin channels; one log per channel.
    let channels = if req.channels.is_empty() {
        vec![notif_entity_pb::NotificationChannel::Email as i32]
    } else {
        req.channels.clone()
    };
    let locale = template_locale_or_default(&req.locale)?;
    let mut logs = Vec::with_capacity(channels.len());
    for channel in channels.iter().copied() {
        // Resolve the active template for this (event_type, channel, locale,
        // tenant) scope. Identity/tenant come from the verified context only.
        let channel_db = channel_to_db(channel);
        let template_filter = template_scope_filter(
            &req.tenant_id,
            &req.event_type,
            channel_db,
            Some(&locale),
            true,
        );
        let template_rows = runtime
            .native_entity_read_hybrid_tenant_for_service(
                "notification",
                &context,
                template_read(template_filter, 0, 1),
            )
            .await?;
        let template = match template_rows.first() {
            Some(row) => template_from_json_row(row),
            None => {
                return Err(notification_template_not_found_status(
                    "send_notification",
                    format!(
                        "no active notification template for event '{}' channel '{}' locale '{}'",
                        req.event_type, channel_db, locale
                    ),
                ));
            }
        };
        // Render subject/body against the request variables; an unsatisfied
        // `{{placeholder}}` fails closed naming the missing variable.
        let rendered_subject = render_template(&template.subject_template, &req.variables)
            .map_err(|field| {
                status_with_reason(
                    crate::runtime::executor_utils::invalid_argument_fields(
                        format!("template variable '{field}' is required but was not provided"),
                        [(
                            format!("variables.{field}"),
                            "template variable is required but was not provided",
                        )],
                    ),
                    VARIABLE_MISSING,
                    &[("error-variable", field.as_str())],
                )
            })?;
        let rendered_body =
            render_template(&template.body_template, &req.variables).map_err(|field| {
                status_with_reason(
                    crate::runtime::executor_utils::invalid_argument_fields(
                        format!("template variable '{field}' is required but was not provided"),
                        [(
                            format!("variables.{field}"),
                            "template variable is required but was not provided",
                        )],
                    ),
                    VARIABLE_MISSING,
                    &[("error-variable", field.as_str())],
                )
            })?;
        let opted_out = is_notification_opted_out(
            runtime,
            &context,
            &req.recipient_id,
            &req.tenant_id,
            channel,
            &req.event_type,
        )
        .await?;
        let (mut status_db, mut status_pb) = channel_send_decision(opted_out);
        // Test-only forced-FAILED path (TODO 04.4.2.2): gated false in prod.
        let mut error_message = String::new();
        if test_mode_enabled() && req.resource_type == TEST_FORCE_FAILED_SENTINEL {
            status_db = "FAILED";
            status_pb = notif_entity_pb::NotificationStatus::Failed as i32;
            error_message = "forced FAILED by UDB_NOTIFICATION_TEST_MODE harness".to_string();
        }
        let log_id = Uuid::new_v4().to_string();
        let log = notif_entity_pb::NotificationLog {
            log_id,
            template_id: template.template_id.clone(),
            event_type: req.event_type.clone(),
            channel,
            recipient_id: req.recipient_id.clone(),
            recipient_address: req.recipient_address.clone(),
            tenant_id: req.tenant_id.clone(),
            project_id: req.project_id.clone(),
            resource_type: req.resource_type.clone(),
            resource_id: req.resource_id.clone(),
            resource_name: req.resource_name.clone(),
            correlation_id: req.correlation_id.clone(),
            status: status_pb,
            error_message,
            rendered_subject,
            rendered_body,
            ..Default::default()
        };
        runtime
            .native_entity_write_for_service(
                "notification",
                &context,
                LOG_MSG,
                notification_log_record(&log, status_db),
                ConflictStrategy::Error,
            )
            .await?;
        logs.push(log);
    }
    let delivery_channels = deliverable_channels(&logs);
    if !delivery_channels.is_empty() {
        if let Some(pool) = svc.pg_pool.as_ref() {
            let primary_log_id = logs
                .iter()
                .find(|log| log.status == notif_entity_pb::NotificationStatus::Pending as i32)
                .map(|log| log.log_id.clone())
                .unwrap_or_default();
            svc.emit_sent_event(
                pool,
                &primary_log_id,
                &req.event_type,
                &req.recipient_id,
                &req.tenant_id,
                &req.project_id,
                &delivery_channels,
                false,
            )
            .await;
        }
    }
    Ok(Response::new(notif_pb::SendNotificationResponse { logs }))
}

pub(crate) async fn get_notification(
    svc: &NotificationServiceImpl,
    request: Request<notif_pb::GetNotificationRequest>,
) -> Result<Response<notif_pb::GetNotificationResponse>, Status> {
    let metadata = request.metadata().clone();
    let scoped_tenant = metadata_tenant_id(&metadata)
        .ok_or_else(|| notification_tenant_metadata_required_status("get_notification"))?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "notification",
        OperationChannel::Read,
        &scoped_tenant,
        None,
    )
    .await?;
    let req = request.into_inner();
    let log_id = parse_uuid("log_id", &req.log_id)?.to_string();
    let runtime = svc.require_runtime()?;
    // The log read already pins log_id + tenant. native_service_context would add
    // the x-udb-project-id header as a further predicate, so a caller whose header
    // project differs from the row's stored project got NOT_FOUND for a row it owns.
    let context = tenant_only_native_service_context(&metadata, &scoped_tenant);
    let rows = runtime
        .native_entity_read_for_service(
            "notification",
            &context,
            notification_log_read(&log_id, &scoped_tenant),
        )
        .await?;
    let log = match rows.first() {
        Some(row) => Some(log_from_json(row)),
        None => {
            return Err(notification_schema_not_found_status(
                "get_notification",
                "notification_not_found",
                "notification not found",
            ));
        }
    };
    Ok(Response::new(notif_pb::GetNotificationResponse { log }))
}

pub(crate) async fn list_notifications(
    svc: &NotificationServiceImpl,
    request: Request<notif_pb::ListNotificationsRequest>,
) -> Result<Response<notif_pb::ListNotificationsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "notification",
        OperationChannel::Read,
        &req.tenant_id,
        None,
    )
    .await?;
    let page = native_page_window(req.page.as_ref(), DEFAULT_PAGE_SIZE);
    let channel = if req.channel == 0 {
        String::new()
    } else {
        channel_to_db(req.channel).to_string()
    };
    let status = if req.status == 0 {
        String::new()
    } else {
        // Map status enum to its stored short name for filtering.
        match notif_entity_pb::NotificationStatus::try_from(req.status) {
            Ok(notif_entity_pb::NotificationStatus::Pending) => "PENDING",
            Ok(notif_entity_pb::NotificationStatus::Sent) => "SENT",
            Ok(notif_entity_pb::NotificationStatus::Delivered) => "DELIVERED",
            Ok(notif_entity_pb::NotificationStatus::Failed) => "FAILED",
            Ok(notif_entity_pb::NotificationStatus::Suppressed) => "SUPPRESSED",
            _ => "",
        }
        .to_string()
    };
    let filter = notification_log_filter(
        &req.tenant_id,
        &req.project_id,
        &req.recipient_id,
        &req.event_type,
        &channel,
        &status,
        &req.resource_type,
        &req.resource_id,
    );
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &req.tenant_id, &req.project_id);
    let total = runtime
        .native_entity_count_for_service("notification", &context, LOG_MSG, Some(filter.clone()))
        .await?;
    let rows = runtime
        .native_entity_read_for_service(
            "notification",
            &context,
            notification_log_list_read(filter, page.offset as u64, page.limit as u32),
        )
        .await?;
    let logs = rows.iter().map(log_from_json).collect();
    Ok(Response::new(notif_pb::ListNotificationsResponse {
        logs,
        page: Some(native_page_response(
            req.page.as_ref(),
            total,
            DEFAULT_PAGE_SIZE,
        )),
    }))
}

pub(crate) async fn retry_notification(
    svc: &NotificationServiceImpl,
    request: Request<notif_pb::RetryNotificationRequest>,
) -> Result<Response<notif_pb::RetryNotificationResponse>, Status> {
    let metadata = request.metadata().clone();
    let scoped_tenant = metadata_tenant_id(&metadata)
        .ok_or_else(|| notification_tenant_metadata_required_status("retry_notification"))?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "notification",
        OperationChannel::Write,
        &scoped_tenant,
        None,
    )
    .await?;
    let req = request.into_inner();
    let log_id = parse_uuid("log_id", &req.log_id)?;
    // Transitional: retry needs a status-gated conditional update with a
    // retry_count reset and RETURNING. The current typed write helper cannot
    // express status-gated updates, so this path remains capability-gated to PG.
    let pool = svc.require_pool()?;
    let m = log_model();
    let rel = m.relation.clone();
    let projection = log_select_projection(&m);
    // Only FAILED notifications are retryable. A SUPPRESSED row is an opt-out and
    // MUST NOT be resurrected (re-queuing it would re-deliver to an opted-out
    // recipient — a compliance bypass), and re-queuing a PENDING/SENT/DELIVERED
    // row would double-send. `retry_count` bumps as the manual-retry counter; the
    // bounded auto-retry budget is tracked separately by the delivery attempt
    // row's `attempt_count`, not this column. Guard in the WHERE so a non-FAILED
    // row yields no update (→ notification_not_retryable_status).
    //
    // The FAILED → PENDING flip and the attempt-budget reset run in ONE
    // transaction so the delivery worker (a separate connection) never observes a
    // resurrected PENDING log still paired with an exhausted attempt row — that
    // window is exactly what made the next worker pass instantly re-fail and
    // re-emit the dead-letter event (a double dead-letter).
    let mut tx = pool.begin().await.map_err(|err| {
        notification_internal_status(
            "retry_notification_begin",
            format!("retry notification failed: {err}"),
        )
    })?;
    let row = sqlx::query(&format!(
        "UPDATE {rel} SET {status} = 'PENDING', {retry} = {retry} + 1 \
         WHERE {log_id} = $1::UUID AND {tenant_id} = $2 AND {status} = 'FAILED' \
         RETURNING {projection}",
        status = m.q("status"),
        retry = m.q("retry_count"),
        log_id = m.q("log_id"),
        tenant_id = m.q("tenant_id"),
    ))
    .bind(log_id)
    .bind(&scoped_tenant)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|err| {
        notification_internal_status(
            "retry_notification_update",
            format!("retry notification failed: {err}"),
        )
    })?;
    let mut log = match row {
        Some(row) => log_from_row(&row)?,
        None => {
            // No FAILED row moved → nothing to reset; the tx rolls back on drop.
            return Err(notification_not_retryable_status());
        }
    };
    // Grant a FRESH retry budget: reset the attempt tracking so this manual retry
    // runs a full bounded-retry cycle rather than instantly exhausting and
    // dead-lettering. Keeps the `retry_count` bump above (the manual-retry counter).
    reset_delivery_attempts_for_retry(&mut *tx, log_id, &scoped_tenant)
        .await
        .map_err(|err| {
            notification_internal_status(
                "retry_notification_reset_attempts",
                format!("retry notification failed: {err}"),
            )
        })?;
    // Opt-out enforced on the retry path too (not only at send): if the recipient
    // opted out of this (channel, event) since the original send, SUPPRESS rather
    // than re-queue for delivery. Fail-closed — an opt-out lookup error aborts the
    // retry (INTERNAL) instead of risking delivery to an opted-out recipient.
    let opted_out = match Uuid::parse_str(log.recipient_id.trim()) {
        Ok(recipient_uuid) => recipient_opted_out_db(
            &mut *tx,
            recipient_uuid,
            &scoped_tenant,
            log.channel,
            &log.event_type,
        )
        .await
        .map_err(|err| {
            notification_internal_status(
                "retry_notification_optout",
                format!("retry opt-out check failed: {err}"),
            )
        })?,
        // No user-scoped recipient id → no preference key → nothing to suppress on.
        Err(_) => false,
    };
    if opted_out {
        // Mark SUPPRESSED (moving the just-set PENDING row) and do NOT emit a sent
        // event — nothing is handed to a provider for an opted-out recipient.
        suppress_log_if_pending(&mut *tx, log_id, &scoped_tenant)
            .await
            .map_err(|err| {
                notification_internal_status(
                    "retry_notification_suppress",
                    format!("retry notification failed: {err}"),
                )
            })?;
        tx.commit().await.map_err(|err| {
            notification_internal_status(
                "retry_notification_commit",
                format!("retry notification failed: {err}"),
            )
        })?;
        log.status = notif_entity_pb::NotificationStatus::Suppressed as i32;
        return Ok(Response::new(notif_pb::RetryNotificationResponse {
            log: Some(log),
        }));
    }
    tx.commit().await.map_err(|err| {
        notification_internal_status(
            "retry_notification_commit",
            format!("retry notification failed: {err}"),
        )
    })?;
    svc.emit_sent_event(
        pool,
        &log.log_id,
        &log.event_type,
        &log.recipient_id,
        &log.tenant_id,
        &log.project_id,
        &[log.channel],
        true,
    )
    .await;
    Ok(Response::new(notif_pb::RetryNotificationResponse {
        log: Some(log),
    }))
}

pub(crate) async fn report_delivery(
    svc: &NotificationServiceImpl,
    request: Request<notif_pb::ReportDeliveryRequest>,
) -> Result<Response<notif_pb::ReportDeliveryResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // Tenant comes from the verified claim: a foreign tenant_id in the body is
    // rejected before any store access (mirrors the rest of the service).
    validate_request_tenant(&metadata, &req.tenant_id)?;
    if req.log_id.trim().is_empty() {
        return Err(notification_required_field(
            "log_id",
            "must be a non-empty notification log id",
            "log_id is required",
        ));
    }
    // Fail closed on a caller that omits a terminal delivery status.
    let status_db = status_to_db(req.status);
    if status_db == "UNSPECIFIED" {
        return Err(notification_required_field(
            "status",
            "must be one of SENT, DELIVERED, FAILED, or PENDING",
            "a terminal delivery status (SENT|DELIVERED|FAILED|PENDING) is required",
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "notification",
        OperationChannel::Write,
        &req.tenant_id,
        None,
    )
    .await?;
    let log_id = parse_uuid("log_id", &req.log_id)?;
    let channel_db = channel_to_db(req.channel);
    let provider = req.provider.trim();
    let pool = svc.require_pool()?;
    // Verify the log belongs to the caller's tenant BEFORE recording a delivery
    // attempt for it. Without this, a tenant could stamp a terminal attempt on
    // another tenant's `log_id`; the delivery worker's dedup would then treat
    // that tenant's still-PENDING notification as already delivered and silently
    // drop it (cross-tenant denial-of-delivery). The body `tenant_id` is already
    // pinned to the verified claim by `validate_request_tenant` above, so this
    // read is scoped to the caller's own tenant.
    {
        let lm = log_model();
        let owns = sqlx::query(&format!(
            "SELECT 1 FROM {rel} WHERE {log_id} = $1::UUID AND {tenant_id} = $2 LIMIT 1",
            rel = lm.relation,
            log_id = lm.q("log_id"),
            tenant_id = lm.q("tenant_id"),
        ))
        .bind(log_id)
        .bind(&req.tenant_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            notification_internal_status(
                "report_delivery_ownership",
                format!("report delivery ownership check failed: {err}"),
            )
        })?;
        if owns.is_none() {
            return Err(notification_log_not_found_status("report_delivery"));
        }
    }
    // Upsert the durable per-(notification, channel, provider) delivery record.
    let row = write_delivery_attempt(
        pool,
        log_id,
        &req.tenant_id,
        channel_db,
        provider,
        status_db,
        &req.error_message,
        &req.provider_message_id,
    )
    .await
    .map_err(|err| {
        notification_internal_status(
            "report_delivery_attempt",
            format!("report delivery failed: {err}"),
        )
    })?;
    let attempt = row.as_ref().map(delivery_attempt_from_row).transpose()?;
    // A client-reported success moves the log forward (PENDING→SENT→DELIVERED) so
    // GetNotification/GetDeliveryStats reflect the real outcome. Best-effort: a
    // transition failure must not fail the delivery report itself.
    let allowed_prev = log_transition_allowed_prev(status_db);
    if !allowed_prev.is_empty() {
        if let Err(err) =
            transition_log_status(pool, log_id, &req.tenant_id, status_db, allowed_prev).await
        {
            tracing::warn!(
                log_id = %req.log_id,
                error = %err,
                "notification log status transition failed"
            );
        }
    }
    // Emit `udb.notification.delivery.<status>.v1` (best-effort; no secrets).
    let project_id = native_service_context(&metadata, &req.tenant_id, "").project_id;
    svc.emit_delivery_event(
        pool,
        &req.log_id,
        &req.tenant_id,
        &project_id,
        channel_db,
        provider,
        status_db,
        &req.provider_message_id,
    )
    .await;
    Ok(Response::new(notif_pb::ReportDeliveryResponse { attempt }))
}

pub(crate) async fn upsert_template(
    svc: &NotificationServiceImpl,
    request: Request<notif_pb::UpsertTemplateRequest>,
) -> Result<Response<notif_pb::UpsertTemplateResponse>, Status> {
    let req = request.into_inner();
    if req.event_type.trim().is_empty() {
        return Err(notification_required_field(
            "event_type",
            "must be a non-empty notification event type",
            "event_type is required",
        ));
    }
    // Platform-global template write (no body tenant); bound on the shared
    // base Write budget so a template-write flood can't starve the control plane.
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "notification",
        OperationChannel::Write,
        "",
        None,
    )
    .await?;
    // Transitional: global template upsert conflicts on (event_type, channel),
    // not the manifest primary key, and returns the stored row.
    let pool = svc.require_pool()?;
    let m = template_model();
    let rel = m.relation.clone();
    let locale = template_locale_or_default(&req.locale)?;
    let projection = template_select_projection(&m);
    // Hybrid tenant model (F4.3): this control-plane write path has no tenant
    // scope in the request, so it always writes a platform-global default
    // (tenant_id = NULL). The unique index stays on (event_type, channel) so
    // global upserts keep deduping correctly (Postgres treats NULLs as
    // distinct, so a tenant_id-bearing unique index would break ON CONFLICT
    // for global rows). TODO: when a per-tenant override write path lands,
    // split this into partial unique indexes — (event_type, channel) WHERE
    // tenant_id IS NULL for globals and (event_type, channel, tenant_id)
    // WHERE tenant_id IS NOT NULL for overrides — and bind the caller tenant.
    let row = sqlx::query(&format!(
        "INSERT INTO {rel} \
         ({template_id}, {event_type}, {channel}, {subject}, {body}, {locale}, {is_active}, {tenant_id}) \
         VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, NULL) \
         ON CONFLICT ({event_type}, {channel}) \
         DO UPDATE SET {subject} = EXCLUDED.{subject}, {body} = EXCLUDED.{body}, \
                       {locale} = EXCLUDED.{locale}, {is_active} = EXCLUDED.{is_active} \
         RETURNING {projection}",
        template_id = m.q("template_id"),
        event_type = m.q("event_type"),
        channel = m.q("channel"),
        subject = m.q("subject_template"),
        body = m.q("body_template"),
        locale = m.q("locale"),
        is_active = m.q("is_active"),
        tenant_id = m.q("tenant_id"),
    ))
    .bind(&req.event_type)
    .bind(channel_to_db(req.channel))
    .bind(&req.subject_template)
    .bind(&req.body_template)
    .bind(&locale)
    .bind(req.is_active)
    .fetch_one(pool)
    .await
    .map_err(|err| {
        notification_internal_status(
            "upsert_template_query",
            format!("upsert template failed: {err}"),
        )
    })?;
    Ok(Response::new(notif_pb::UpsertTemplateResponse {
        template: Some(template_from_row(&row)?),
    }))
}

pub(crate) async fn get_template(
    svc: &NotificationServiceImpl,
    request: Request<notif_pb::GetTemplateRequest>,
) -> Result<Response<notif_pb::GetTemplateResponse>, Status> {
    let metadata = request.metadata().clone();
    let scoped_tenant = metadata_tenant_id(&metadata)
        .ok_or_else(|| notification_tenant_metadata_required_status("get_template"))?;
    let req = request.into_inner();
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "notification",
        OperationChannel::Read,
        &scoped_tenant,
        None,
    )
    .await?;
    let locale = template_locale_or_default(&req.locale)?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &scoped_tenant, "");
    let filter = template_scope_filter(
        &scoped_tenant,
        &req.event_type,
        channel_to_db(req.channel),
        Some(&locale),
        false,
    );
    let rows = runtime
        .native_entity_read_hybrid_tenant_for_service(
            "notification",
            &context,
            template_read(filter, 0, 1),
        )
        .await?;
    let template = match rows.first() {
        Some(row) => Some(template_from_json_row(row)),
        None => {
            return Err(notification_template_not_found_status(
                "get_template",
                "template not found",
            ));
        }
    };
    Ok(Response::new(notif_pb::GetTemplateResponse { template }))
}

pub(crate) async fn list_templates(
    svc: &NotificationServiceImpl,
    request: Request<notif_pb::ListTemplatesRequest>,
) -> Result<Response<notif_pb::ListTemplatesResponse>, Status> {
    let metadata = request.metadata().clone();
    let scoped_tenant = metadata_tenant_id(&metadata)
        .ok_or_else(|| notification_tenant_metadata_required_status("list_templates"))?;
    let req = request.into_inner();
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "notification",
        OperationChannel::Read,
        &scoped_tenant,
        None,
    )
    .await?;
    let page = native_page_window(req.page.as_ref(), DEFAULT_PAGE_SIZE);
    let channel = if req.channel == 0 {
        String::new()
    } else {
        channel_to_db(req.channel).to_string()
    };
    let filter = template_scope_filter(
        &scoped_tenant,
        &req.event_type,
        &channel,
        None,
        req.active_only,
    );
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &scoped_tenant, "");
    let total = runtime
        .native_entity_count_for_service(
            "notification",
            &context,
            TEMPLATE_MSG,
            Some(filter.clone()),
        )
        .await?;
    let rows = runtime
        .native_entity_read_hybrid_tenant_for_service(
            "notification",
            &context,
            template_read(filter, page.offset as u64, page.limit as u32),
        )
        .await?;
    let templates = rows.iter().map(template_from_json_row).collect();
    Ok(Response::new(notif_pb::ListTemplatesResponse {
        templates,
        page: Some(native_page_response(
            req.page.as_ref(),
            total,
            DEFAULT_PAGE_SIZE,
        )),
    }))
}

pub(crate) async fn get_delivery_stats(
    svc: &NotificationServiceImpl,
    request: Request<notif_pb::GetDeliveryStatsRequest>,
) -> Result<Response<notif_pb::GetDeliveryStatsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "notification",
        OperationChannel::Read,
        &req.tenant_id,
        None,
    )
    .await?;
    if req.date_from.trim().is_empty() && req.date_to.trim().is_empty() {
        let runtime = svc.require_runtime()?;
        let context = native_service_context(&metadata, &req.tenant_id, "");
        let rows = runtime
            .native_entity_aggregate_for_service(
                "notification",
                &context,
                delivery_stats_aggregate(&req.tenant_id, &req.event_type),
            )
            .await?;
        let (mut total_sent, mut total_delivered, mut total_failed) = (0i64, 0i64, 0i64);
        let mut by_channel = std::collections::BTreeMap::<i32, notif_pb::ChannelStats>::new();
        for row in &rows {
            let row = json_object(row);
            let channel = channel_from_db(&json_string_field(row, "channel"));
            let status = json_string_field(row, "status");
            let n = json_i64_field(row, "n");
            let entry = by_channel
                .entry(channel)
                .or_insert_with(|| notif_pb::ChannelStats {
                    channel,
                    ..Default::default()
                });
            match status.as_str() {
                "SENT" => {
                    entry.sent += n;
                    total_sent += n;
                }
                "DELIVERED" => {
                    entry.sent += n;
                    entry.delivered += n;
                    total_sent += n;
                    total_delivered += n;
                }
                "FAILED" => {
                    entry.failed += n;
                    total_failed += n;
                }
                "SUPPRESSED" => {
                    entry.suppressed += n;
                }
                _ => {}
            }
        }
        let mut by_channel = by_channel.into_values().collect::<Vec<_>>();
        for entry in &mut by_channel {
            entry.delivery_rate = if entry.sent > 0 {
                entry.delivered as f64 / entry.sent as f64
            } else {
                0.0
            };
        }
        let overall_delivery_rate = if total_sent > 0 {
            total_delivered as f64 / total_sent as f64
        } else {
            0.0
        };
        return Ok(Response::new(notif_pb::GetDeliveryStatsResponse {
            total_sent,
            total_delivered,
            total_failed,
            overall_delivery_rate,
            by_channel,
        }));
    }
    // Transitional: date-window filters require backend date casts; the
    // no-window per-channel aggregate uses LogicalAggregate above.
    let pool = svc.require_pool()?;
    let m = log_model();
    let rel = m.relation.clone();
    // Per-channel aggregation, honoring the optional tenant/event filters and
    // the optional [date_from, date_to] window (YYYY-MM-DD, `to` inclusive of
    // the whole day). `sent` counts successful hand-offs (SENT|DELIVERED).
    let rows = sqlx::query(&format!(
        "SELECT {channel} AS channel, \
           COUNT(*) FILTER (WHERE {status} IN ('SENT','DELIVERED')) AS sent, \
           COUNT(*) FILTER (WHERE {status} = 'DELIVERED') AS delivered, \
           COUNT(*) FILTER (WHERE {status} = 'FAILED') AS failed, \
           COUNT(*) FILTER (WHERE {status} = 'SUPPRESSED') AS suppressed \
         FROM {rel} \
         WHERE ($1 = '' OR {tenant} = $1) AND ($2 = '' OR {event} = $2) \
           AND ($3 = '' OR {created} >= $3::date) \
           AND ($4 = '' OR {created} < ($4::date + 1)) \
         GROUP BY {channel} \
         ORDER BY {channel}",
        channel = m.q("channel"),
        status = m.q("status"),
        tenant = m.q("tenant_id"),
        event = m.q("event_type"),
        created = m.q("created_at"),
    ))
    .bind(&req.tenant_id)
    .bind(&req.event_type)
    .bind(&req.date_from)
    .bind(&req.date_to)
    .fetch_all(pool)
    .await
    .map_err(|err| {
        notification_internal_status(
            "delivery_stats_query",
            format!("delivery stats failed: {err}"),
        )
    })?;

    let (mut total_sent, mut total_delivered, mut total_failed) = (0i64, 0i64, 0i64);
    let mut by_channel = Vec::with_capacity(rows.len());
    for row in &rows {
        let channel: String = row.try_get("channel").unwrap_or_default();
        let sent: i64 = row.try_get("sent").unwrap_or(0);
        let delivered: i64 = row.try_get("delivered").unwrap_or(0);
        let failed: i64 = row.try_get("failed").unwrap_or(0);
        let suppressed: i64 = row.try_get("suppressed").unwrap_or(0);
        total_sent += sent;
        total_delivered += delivered;
        total_failed += failed;
        by_channel.push(notif_pb::ChannelStats {
            channel: channel_from_db(&channel),
            sent,
            delivered,
            failed,
            suppressed,
            delivery_rate: if sent > 0 {
                delivered as f64 / sent as f64
            } else {
                0.0
            },
        });
    }
    let overall_delivery_rate = if total_sent > 0 {
        total_delivered as f64 / total_sent as f64
    } else {
        0.0
    };
    Ok(Response::new(notif_pb::GetDeliveryStatsResponse {
        total_sent,
        total_delivered,
        total_failed,
        overall_delivery_rate,
        by_channel,
    }))
}

pub(crate) async fn set_preference(
    svc: &NotificationServiceImpl,
    request: Request<notif_pb::SetPreferenceRequest>,
) -> Result<Response<notif_pb::SetPreferenceResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "notification",
        OperationChannel::Write,
        &req.tenant_id,
        None,
    )
    .await?;
    let user_id = parse_uuid("user_id", &req.user_id)?;
    // `tenant_id` is a VARCHAR(120) NOT NULL slug/id — bind it as text, not a
    // UUID (the column is not UUID-typed), and reject empty to honor NOT NULL.
    if req.tenant_id.trim().is_empty() {
        return Err(notification_required_field(
            "tenant_id",
            "must be a non-empty tenant id",
            "tenant_id is required",
        ));
    }
    // Transitional: public upsert conflicts on (user_id, channel, event_type),
    // not the manifest primary key. Keep exact Postgres semantics until the
    // typed write helper can target alternate unique keys.
    let pool = svc.require_pool()?;
    let m = preference_model();
    let rel = m.relation.clone();
    let projection = preference_select_projection(&m);
    let row = sqlx::query(&format!(
        "INSERT INTO {rel} \
         ({preference_id}, {user_id}, {tenant_id}, {channel}, {event_type}, {is_opted_out}) \
         VALUES (gen_random_uuid(), $1::UUID, $2, $3, $4, $5) \
         ON CONFLICT ({user_id}, {channel}, {event_type}) \
         DO UPDATE SET {is_opted_out} = EXCLUDED.{is_opted_out} \
         RETURNING {projection}",
        preference_id = m.q("preference_id"),
        user_id = m.q("user_id"),
        tenant_id = m.q("tenant_id"),
        channel = m.q("channel"),
        event_type = m.q("event_type"),
        is_opted_out = m.q("is_opted_out"),
    ))
    .bind(user_id)
    .bind(&req.tenant_id)
    .bind(channel_to_db(req.channel))
    .bind(&req.event_type)
    .bind(req.is_opted_out)
    .fetch_one(pool)
    .await
    .map_err(|err| {
        notification_internal_status(
            "set_preference_query",
            format!("set preference failed: {err}"),
        )
    })?;
    Ok(Response::new(notif_pb::SetPreferenceResponse {
        preference: Some(preference_from_row(&row)?),
    }))
}

pub(crate) async fn get_preference(
    svc: &NotificationServiceImpl,
    request: Request<notif_pb::GetPreferenceRequest>,
) -> Result<Response<notif_pb::GetPreferenceResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "notification",
        OperationChannel::Read,
        &req.tenant_id,
        None,
    )
    .await?;
    let user_id = parse_uuid("user_id", &req.user_id)?.to_string();
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &req.tenant_id, "");
    let rows = runtime
        .native_entity_read_for_service(
            "notification",
            &context,
            preference_read(&user_id, &req.tenant_id, req.channel, &req.event_type),
        )
        .await?;
    let preference = match rows.first() {
        Some(row) => Some(preference_from_json_row(row)),
        None => {
            return Err(notification_schema_not_found_status(
                "get_preference",
                "notification_preference_not_found",
                "preference not found",
            ));
        }
    };
    Ok(Response::new(notif_pb::GetPreferenceResponse {
        preference,
    }))
}

pub(crate) async fn list_preferences(
    svc: &NotificationServiceImpl,
    request: Request<notif_pb::ListPreferencesRequest>,
) -> Result<Response<notif_pb::ListPreferencesResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "notification",
        OperationChannel::Read,
        &req.tenant_id,
        None,
    )
    .await?;
    let user_id = parse_uuid("user_id", &req.user_id)?;
    let page = native_page_window(req.page.as_ref(), DEFAULT_PAGE_SIZE);
    let user_id = user_id.to_string();
    let filter = preference_list_filter(&user_id, &req.tenant_id);
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &req.tenant_id, "");
    let total = runtime
        .native_entity_count_for_service(
            "notification",
            &context,
            PREFERENCE_MSG,
            Some(filter.clone()),
        )
        .await?;
    let rows = runtime
        .native_entity_read_for_service(
            "notification",
            &context,
            preference_list_read(filter, page.offset as u64, page.limit as u32),
        )
        .await?;
    let preferences = rows.iter().map(preference_from_json_row).collect();
    Ok(Response::new(notif_pb::ListPreferencesResponse {
        preferences,
        page: Some(native_page_response(
            req.page.as_ref(),
            total,
            DEFAULT_PAGE_SIZE,
        )),
    }))
}
