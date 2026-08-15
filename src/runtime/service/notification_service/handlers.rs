//! The twelve `NotificationService` RPC handlers, extracted from the trait impl as
//! free `pub(crate) async fn`s taking `svc` where the trait method took `&self`.
//! `mod.rs` delegates one line to each. Bodies are verbatim — the same
//! cross-tenant scope guards, per-tenant admission, hybrid-tenant template
//! resolution, `{{placeholder}}` rendering, transitional Postgres upserts, and
//! outbox emission as the former god file.

use sqlx::Row;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::{ConflictStrategy, LogicalWrite};
use crate::proto::udb::core::notification::entity::v1 as notif_entity_pb;
use crate::proto::udb::core::notification::services::v1 as notif_pb;
use crate::runtime::channels::OperationChannel;
use crate::runtime::core::native_store::NativeEntityTransactionOp;

use super::super::native_helpers::{
    admit_on as native_admit_on, metadata_project_id, metadata_tenant_id, native_page_response,
    native_page_window, parse_uuid, project_scoped_native_service_context, validate_request_tenant,
    validated_native_service_context,
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
use super::events::{
    NotificationDeliveryEvent, enqueue_delivery_event_in_tx, enqueue_sent_event_in_tx,
    enqueue_suppressed_event_in_tx, sent_event_transaction_op, suppressed_event_transaction_op,
};
use super::model::{
    channel_from_db, channel_send_decision, channel_to_db, delivery_attempt_from_row,
    json_i64_field, json_object, json_string_field, log_from_json, log_from_row, log_model,
    log_select_projection, preference_from_json_row, preference_from_row, preference_model,
    preference_select_projection, render_template, status_to_db, template_from_json_row,
    template_from_row, template_locale_or_default, template_model, template_select_projection,
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
    let context = validated_native_service_context(&metadata, &req.tenant_id, &req.project_id)?;
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
    let (context, _pool) = svc.resolve_project_store(context, true, "send_notification")?;
    // Persist the project the authenticated request context actually resolved.
    // SDKs commonly carry project scope in x-udb-project-id while leaving the
    // duplicate body field empty; using req.project_id below silently erased
    // that scope from the NotificationLog and its Kafka/outbox envelope.
    let project_id = context.project_id.clone();
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
            &context.project_id,
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
        let (_, mut status_pb) = channel_send_decision(opted_out);
        // Test-only forced-FAILED path (TODO 04.4.2.2): gated false in prod.
        let mut error_message = String::new();
        if test_mode_enabled() && req.resource_type == TEST_FORCE_FAILED_SENTINEL {
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
            project_id: project_id.clone(),
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
        logs.push(log);
    }
    let records = logs
        .iter()
        .map(|log| notification_log_record(log, status_to_db(log.status)))
        .collect();
    let mut transaction_ops = vec![NativeEntityTransactionOp::Write(LogicalWrite {
        message_type: LOG_MSG.to_string(),
        records,
        conflict: ConflictStrategy::Error,
        return_fields: Vec::new(),
    })];
    for log in &logs {
        let event_op = if log.status == notif_entity_pb::NotificationStatus::Pending as i32 {
            sent_event_transaction_op(svc.outbox_relation.as_deref(), log)
        } else if log.status == notif_entity_pb::NotificationStatus::Suppressed as i32 {
            suppressed_event_transaction_op(svc.outbox_relation.as_deref(), log)
        } else {
            Ok(None)
        }
        .map_err(|err| {
            notification_internal_status(
                "send_notification_outbox_envelope",
                format!("send notification failed: {err}"),
            )
        })?;
        if let Some(event_op) = event_op {
            transaction_ops.push(event_op);
        }
    }
    // All channel logs and the configured sent-event outbox row are one commit
    // on the project-selected native store. A later channel or outbox failure can
    // no longer leave a partial customer request durable.
    runtime
        .native_entity_transaction_for_service("notification", &context, transaction_ops)
        .await?;
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
    // Project metadata/claim selects both the physical store and the logical row
    // predicate. A same-tenant caller in another project must not read this log.
    let context = project_scoped_native_service_context(&metadata, &scoped_tenant);
    let (context, _pool) = svc.resolve_project_store(context, false, "get_notification")?;
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
    let context = validated_native_service_context(&metadata, &req.tenant_id, &req.project_id)?;
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
    let (context, _pool) = svc.resolve_project_store(context, false, "list_notifications")?;
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
    let body_project = req
        .context
        .as_ref()
        .and_then(|context| context.tenant.as_ref())
        .map(|tenant| tenant.project_id.as_str())
        .unwrap_or_default();
    let context = validated_native_service_context(&metadata, &scoped_tenant, body_project)?;
    // Transitional: retry needs a status-gated conditional update with a
    // retry_count reset and RETURNING. The current typed write helper cannot
    // express status-gated updates, so this path remains capability-gated to PG.
    let (context, pool) = svc.resolve_project_store(context, true, "retry_notification")?;
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
    crate::runtime::core::set_request_local_settings(&mut tx, &context).await?;
    let row = sqlx::query(&format!(
        "UPDATE {rel} SET {status} = 'PENDING', {retry} = {retry} + 1 \
         WHERE {log_id} = $1::UUID AND {tenant_id} = $2 AND {project_id} = $3 \
           AND {status} = 'FAILED' \
         RETURNING {projection}",
        status = m.q("status"),
        retry = m.q("retry_count"),
        log_id = m.q("log_id"),
        tenant_id = m.q("tenant_id"),
        project_id = m.q("project_id"),
    ))
    .bind(log_id)
    .bind(&scoped_tenant)
    .bind(&context.project_id)
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
    reset_delivery_attempts_for_retry(&mut *tx, log_id, &scoped_tenant, &context.project_id)
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
            &context.project_id,
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
        let suppressed =
            suppress_log_if_pending(&mut *tx, log_id, &scoped_tenant, &context.project_id)
                .await
                .map_err(|err| {
                    notification_internal_status(
                        "retry_notification_suppress",
                        format!("retry notification failed: {err}"),
                    )
                })?;
        if !suppressed {
            return Err(notification_internal_status(
                "retry_notification_suppress",
                "retry notification failed: pending log was not suppressible",
            ));
        }
        log.status = notif_entity_pb::NotificationStatus::Suppressed as i32;
        enqueue_suppressed_event_in_tx(&mut *tx, svc.outbox_relation.as_deref(), &log)
            .await
            .map_err(|err| {
                notification_internal_status(
                    "retry_notification_suppressed_outbox",
                    format!("retry notification failed: {err}"),
                )
            })?;
        tx.commit().await.map_err(|err| {
            notification_internal_status(
                "retry_notification_commit",
                format!("retry notification failed: {err}"),
            )
        })?;
        return Ok(Response::new(notif_pb::RetryNotificationResponse {
            log: Some(log),
        }));
    }
    enqueue_sent_event_in_tx(&mut *tx, svc.outbox_relation.as_deref(), &log)
        .await
        .map_err(|err| {
            notification_internal_status(
                "retry_notification_outbox",
                format!("retry notification failed: {err}"),
            )
        })?;
    tx.commit().await.map_err(|err| {
        notification_internal_status(
            "retry_notification_commit",
            format!("retry notification failed: {err}"),
        )
    })?;
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
    // Tenant and the optional body context project are both bound to the verified
    // metadata/claim scope before a physical store can be selected.
    let body_project = req
        .context
        .as_ref()
        .and_then(|context| context.tenant.as_ref())
        .map(|tenant| tenant.project_id.as_str())
        .unwrap_or_default();
    let context = validated_native_service_context(&metadata, &req.tenant_id, body_project)?;
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
    let channel_db = channel_to_db(req.channel);
    if channel_db == "UNSPECIFIED" {
        return Err(notification_required_field(
            "channel",
            "must be a concrete notification channel",
            "channel is required",
        ));
    }
    let provider = req.provider.trim();
    if provider.is_empty() {
        return Err(notification_required_field(
            "provider",
            "must be a non-empty delivery provider",
            "provider is required",
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
    let (context, pool) = svc.resolve_project_store(context, true, "report_delivery")?;
    let mut tx = pool.begin().await.map_err(|err| {
        notification_internal_status(
            "report_delivery_begin",
            format!("report delivery failed: {err}"),
        )
    })?;
    crate::runtime::core::set_request_local_settings(&mut tx, &context).await?;
    // Verify the log belongs to the caller's tenant BEFORE recording a delivery
    // attempt for it. Without this, a tenant could stamp a terminal attempt on
    // another tenant's `log_id`; the delivery worker's dedup would then treat
    // that tenant's still-PENDING notification as already delivered and silently
    // drop it (cross-tenant denial-of-delivery). The body `tenant_id` is already
    // pinned to the verified claim by `validate_request_tenant` above, so this
    // read is scoped to the caller's own tenant.
    let (project_id, stored_channel, template_id, event_type, correlation_id) = {
        let lm = log_model();
        let stored_scope = sqlx::query_as::<_, (String, String, String, String, String)>(&format!(
            "SELECT COALESCE({project_id}::TEXT, ''), {channel}::TEXT, \
                    COALESCE({template_id}::TEXT, ''), COALESCE({event_type}::TEXT, ''), \
                    COALESCE({correlation_id}::TEXT, '') FROM {rel} \
             WHERE {log_id} = $1::UUID AND {tenant_id} = $2 AND {project_id} = $3 \
             FOR UPDATE",
            rel = lm.relation,
            project_id = lm.q("project_id"),
            channel = lm.q("channel"),
            template_id = lm.q("template_id"),
            event_type = lm.q("event_type"),
            correlation_id = lm.q("correlation_id"),
            log_id = lm.q("log_id"),
            tenant_id = lm.q("tenant_id"),
        ))
        .bind(log_id)
        .bind(&req.tenant_id)
        .bind(&context.project_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| {
            notification_internal_status(
                "report_delivery_ownership",
                format!("report delivery ownership check failed: {err}"),
            )
        })?;
        stored_scope.ok_or_else(|| notification_log_not_found_status("report_delivery"))?
    };
    if stored_channel != channel_db {
        return Err(notification_required_field(
            "channel",
            "must match the notification log channel",
            "channel does not match notification log",
        ));
    }
    // Upsert the durable per-(notification, channel, provider) delivery record.
    let row = write_delivery_attempt(
        &mut *tx,
        log_id,
        &req.tenant_id,
        &context.project_id,
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
    let attempt_count = row
        .as_ref()
        .and_then(|row| row.try_get::<i32, _>("attempt_count").ok())
        .unwrap_or(0);
    let attempt = row.as_ref().map(delivery_attempt_from_row).transpose()?;
    // A client-reported success moves the log forward (PENDING→SENT→DELIVERED) so
    // GetNotification/GetDeliveryStats reflect the real outcome. This transition,
    // the attempt row, and the outbox event are deliberately one transaction.
    let allowed_prev = log_transition_allowed_prev(status_db);
    if !allowed_prev.is_empty() {
        transition_log_status(
            &mut *tx,
            log_id,
            &req.tenant_id,
            &context.project_id,
            status_db,
            allowed_prev,
        )
        .await
        .map_err(|err| {
            notification_internal_status(
                "report_delivery_transition",
                format!("report delivery failed: {err}"),
            )
        })?;
    }
    // Use the canonical project stored with the log after proving it equals the
    // caller's verified project scope. The outbox insert stays in this same tx.
    enqueue_delivery_event_in_tx(
        &mut *tx,
        svc.outbox_relation.as_deref(),
        &NotificationDeliveryEvent {
            log_id: &req.log_id,
            template_id: &template_id,
            event_type: &event_type,
            tenant_id: &req.tenant_id,
            project_id: &project_id,
            correlation_id: &correlation_id,
            channel_db,
            provider,
            status_db,
            provider_message_id: &req.provider_message_id,
            error_detail: &req.error_message,
            retry_attempt: attempt_count.saturating_sub(1),
            will_retry: status_db == "FAILED",
        },
    )
    .await
    .map_err(|err| {
        notification_internal_status(
            "report_delivery_outbox",
            format!("report delivery failed: {err}"),
        )
    })?;
    tx.commit().await.map_err(|err| {
        notification_internal_status(
            "report_delivery_commit",
            format!("report delivery failed: {err}"),
        )
    })?;
    Ok(Response::new(notif_pb::ReportDeliveryResponse { attempt }))
}

pub(crate) async fn upsert_template(
    svc: &NotificationServiceImpl,
    request: Request<notif_pb::UpsertTemplateRequest>,
) -> Result<Response<notif_pb::UpsertTemplateResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    if req.event_type.trim().is_empty() {
        return Err(notification_required_field(
            "event_type",
            "must be a non-empty notification event type",
            "event_type is required",
        ));
    }
    let scoped_tenant = metadata_tenant_id(&metadata).unwrap_or_default();
    let body_scope = req
        .context
        .as_ref()
        .and_then(|context| context.tenant.as_ref());
    let body_tenant = body_scope
        .map(|tenant| tenant.tenant_id.as_str())
        .filter(|tenant| !tenant.trim().is_empty())
        .unwrap_or(scoped_tenant.as_str());
    let body_project = body_scope
        .map(|tenant| tenant.project_id.as_str())
        .unwrap_or_default();
    let context = if body_tenant.trim().is_empty() {
        // Preserve the pre-existing in-process bootstrap/test seam: direct calls
        // have no auth metadata and therefore target the explicit default project.
        // Served RPCs are still tenant-gated by method security before this handler.
        crate::RequestContext {
            project_id: if body_project.trim().is_empty() {
                metadata_project_id(&metadata).unwrap_or_default()
            } else {
                body_project.to_string()
            },
            ..crate::RequestContext::default()
        }
    } else {
        validated_native_service_context(&metadata, body_tenant, body_project)?
    };
    // A request-context tenant creates a per-tenant override; a legacy/bootstrap
    // request without it creates the project-global default. Both are owned by
    // the exact active project selected below.
    let template_tenant = body_scope
        .map(|tenant| tenant.tenant_id.trim())
        .filter(|tenant| !tenant.is_empty());
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "notification",
        OperationChannel::Write,
        &scoped_tenant,
        None,
    )
    .await?;
    let (context, pool) = svc.resolve_project_store(context, true, "upsert_template")?;
    let m = template_model();
    let rel = m.relation.clone();
    let locale = template_locale_or_default(&req.locale)?;
    let projection = template_select_projection(&m);
    let mut tx = pool.begin().await.map_err(|err| {
        notification_internal_status(
            "upsert_template_begin",
            format!("upsert template failed: {err}"),
        )
    })?;
    crate::runtime::core::set_request_local_settings(&mut tx, &context).await?;
    let conflict_target = if template_tenant.is_some() {
        format!(
            "({tenant_id}, {project_id}, {event_type}, {channel}) \
             WHERE {tenant_id} IS NOT NULL AND {deleted_at} IS NULL",
            tenant_id = m.q("tenant_id"),
            project_id = m.q("project_id"),
            event_type = m.q("event_type"),
            channel = m.q("channel"),
            deleted_at = m.q("deleted_at"),
        )
    } else {
        format!(
            "({project_id}, {event_type}, {channel}) \
             WHERE {tenant_id} IS NULL AND {deleted_at} IS NULL",
            project_id = m.q("project_id"),
            event_type = m.q("event_type"),
            channel = m.q("channel"),
            tenant_id = m.q("tenant_id"),
            deleted_at = m.q("deleted_at"),
        )
    };
    let row = sqlx::query(&format!(
        "INSERT INTO {rel} \
         ({template_id}, {event_type}, {channel}, {subject}, {body}, {locale}, {is_active}, {tenant_id}, {project_id}) \
         VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8) \
         ON CONFLICT {conflict_target} \
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
        project_id = m.q("project_id"),
    ))
    .bind(&req.event_type)
    .bind(channel_to_db(req.channel))
    .bind(&req.subject_template)
    .bind(&req.body_template)
    .bind(&locale)
    .bind(req.is_active)
    .bind(template_tenant)
    .bind(&context.project_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|err| {
        notification_internal_status(
            "upsert_template_query",
            format!("upsert template failed: {err}"),
        )
    })?;
    let template = template_from_row(&row)?;
    tx.commit().await.map_err(|err| {
        notification_internal_status(
            "upsert_template_commit",
            format!("upsert template failed: {err}"),
        )
    })?;
    Ok(Response::new(notif_pb::UpsertTemplateResponse {
        template: Some(template),
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
    let context = project_scoped_native_service_context(&metadata, &scoped_tenant);
    let (context, _pool) = svc.resolve_project_store(context, false, "get_template")?;
    let filter = template_scope_filter(
        &scoped_tenant,
        &context.project_id,
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
    let runtime = svc.require_runtime()?;
    let context = project_scoped_native_service_context(&metadata, &scoped_tenant);
    let (context, _pool) = svc.resolve_project_store(context, false, "list_templates")?;
    let filter = template_scope_filter(
        &scoped_tenant,
        &context.project_id,
        &req.event_type,
        &channel,
        None,
        req.active_only,
    );
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
    let context = project_scoped_native_service_context(&metadata, &req.tenant_id);
    let (context, pool) = svc.resolve_project_store(context, false, "get_delivery_stats")?;
    if req.date_from.trim().is_empty() && req.date_to.trim().is_empty() {
        let runtime = svc.require_runtime()?;
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
    let m = log_model();
    let rel = m.relation.clone();
    let mut tx = pool.begin().await.map_err(|err| {
        notification_internal_status(
            "get_delivery_stats_begin",
            format!("get delivery stats failed: {err}"),
        )
    })?;
    crate::runtime::core::set_request_local_settings(&mut tx, &context).await?;
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
           AND {project} = $5 \
         GROUP BY {channel} \
         ORDER BY {channel}",
        channel = m.q("channel"),
        status = m.q("status"),
        tenant = m.q("tenant_id"),
        project = m.q("project_id"),
        event = m.q("event_type"),
        created = m.q("created_at"),
    ))
    .bind(&req.tenant_id)
    .bind(&req.event_type)
    .bind(&req.date_from)
    .bind(&req.date_to)
    .bind(&context.project_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|err| {
        notification_internal_status(
            "delivery_stats_query",
            format!("delivery stats failed: {err}"),
        )
    })?;

    let (mut total_sent, mut total_delivered, mut total_failed) = (0i64, 0i64, 0i64);
    tx.commit().await.map_err(|err| {
        notification_internal_status(
            "get_delivery_stats_commit",
            format!("get delivery stats failed: {err}"),
        )
    })?;
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
    let body_project = req
        .context
        .as_ref()
        .and_then(|context| context.tenant.as_ref())
        .map(|tenant| tenant.project_id.as_str())
        .unwrap_or_default();
    let context = validated_native_service_context(&metadata, &req.tenant_id, body_project)?;
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
    // The raw upsert uses the manifest-declared tenant+project business key and
    // runs with request-local RLS settings on the already pinned project pool.
    let (context, pool) = svc.resolve_project_store(context, true, "set_preference")?;
    let m = preference_model();
    let rel = m.relation.clone();
    let projection = preference_select_projection(&m);
    let mut tx = pool.begin().await.map_err(|err| {
        notification_internal_status(
            "set_preference_begin",
            format!("set preference failed: {err}"),
        )
    })?;
    crate::runtime::core::set_request_local_settings(&mut tx, &context).await?;
    let row = sqlx::query(&format!(
        "INSERT INTO {rel} \
         ({preference_id}, {user_id}, {tenant_id}, {project_id}, {channel}, {event_type}, {is_opted_out}) \
         VALUES (gen_random_uuid(), $1::UUID, $2, $3, $4, $5, $6) \
         ON CONFLICT ({tenant_id}, {project_id}, {user_id}, {channel}, {event_type}) \
         DO UPDATE SET {is_opted_out} = EXCLUDED.{is_opted_out} \
         RETURNING {projection}",
        preference_id = m.q("preference_id"),
        user_id = m.q("user_id"),
        tenant_id = m.q("tenant_id"),
        project_id = m.q("project_id"),
        channel = m.q("channel"),
        event_type = m.q("event_type"),
        is_opted_out = m.q("is_opted_out"),
    ))
    .bind(user_id)
    .bind(&req.tenant_id)
    .bind(&context.project_id)
    .bind(channel_to_db(req.channel))
    .bind(&req.event_type)
    .bind(req.is_opted_out)
    .fetch_one(&mut *tx)
    .await
    .map_err(|err| {
        notification_internal_status(
            "set_preference_query",
            format!("set preference failed: {err}"),
        )
    })?;
    let preference = preference_from_row(&row)?;
    tx.commit().await.map_err(|err| {
        notification_internal_status(
            "set_preference_commit",
            format!("set preference failed: {err}"),
        )
    })?;
    Ok(Response::new(notif_pb::SetPreferenceResponse {
        preference: Some(preference),
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
    let context = project_scoped_native_service_context(&metadata, &req.tenant_id);
    let (context, _pool) = svc.resolve_project_store(context, false, "get_preference")?;
    let rows = runtime
        .native_entity_read_for_service(
            "notification",
            &context,
            preference_read(
                &user_id,
                &req.tenant_id,
                &context.project_id,
                req.channel,
                &req.event_type,
            ),
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
    let runtime = svc.require_runtime()?;
    let context = project_scoped_native_service_context(&metadata, &req.tenant_id);
    let (context, _pool) = svc.resolve_project_store(context, false, "list_preferences")?;
    let filter = preference_list_filter(&user_id, &req.tenant_id, &context.project_id);
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
