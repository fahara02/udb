//! The six `WebhookService` RPC handlers, extracted from the trait impl as free
//! `pub(crate) async fn`s taking `svc` where the trait method took `&self`.
//! `mod.rs` delegates one line to each. Bodies are verbatim — the same
//! cross-tenant scope guard, WRITE-time SSRF validation, bespoke transitional SQL,
//! and outbox event emission as the former god file.

use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::webhook::services::v1 as webhook_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    admit_on as native_admit_on, native_next_page_token_for_total, native_offset_page_window,
    non_empty_json, parse_uuid, update_mask_allows, update_mask_path_set, validate_request_tenant,
};
use super::WebhookServiceImpl;
use super::config::{
    TOPIC_ENDPOINT_CREATED, TOPIC_ENDPOINT_DELETED, TOPIC_ENDPOINT_UPDATED, clamp_max_attempts,
};
use super::errors::{
    webhook_endpoint_not_found_status, webhook_internal_status, webhook_required_field,
};
use super::events::emit_event;
use super::model::{
    delivery_from_row, delivery_model, delivery_select_projection, endpoint_from_row,
    endpoint_model, endpoint_select_projection,
};
use super::security::{generate_signing_secret, validate_webhook_target_url};

pub(crate) async fn create_endpoint(
    svc: &WebhookServiceImpl,
    request: Request<webhook_pb::CreateEndpointRequest>,
) -> Result<Response<webhook_pb::CreateEndpointResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // Cross-tenant guard FIRST: the body tenant_id must match the verified
    // claim/header. After this passes, the body value IS the verified tenant.
    validate_request_tenant(&metadata, &req.tenant_id)?;
    if req.url.trim().is_empty() {
        return Err(webhook_required_field(
            "url",
            "must be a non-empty HTTPS webhook URL",
            "url is required",
        ));
    }
    // SSRF guard at WRITE time: https-only, no private/loopback/CGNAT host.
    validate_webhook_target_url(&req.url)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "webhook",
        OperationChannel::Admin,
        &req.tenant_id,
        None,
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let pool = svc.require_pool()?;
    let m = endpoint_model();
    let rel = m.relation.clone();
    let endpoint_id = Uuid::new_v4().to_string();
    let secret = if req.signing_secret.trim().is_empty() {
        generate_signing_secret()
    } else {
        req.signing_secret.trim().to_string()
    };
    let topic_pattern = {
        let p = req.topic_pattern.trim();
        if p.is_empty() {
            "*".to_string()
        } else {
            p.to_string()
        }
    };
    let max_attempts = clamp_max_attempts(req.max_attempts);
    let metadata_json = non_empty_json(&req.metadata_json);

    sqlx::query(&format!(
        "INSERT INTO {rel} \
         ({endpoint_id}, {tenant_id}, {url}, {topic_pattern}, {signing_secret}, {active}, {description}, {max_attempts}, {metadata_json}) \
         VALUES ($1::UUID, $2::UUID, $3, $4, $5, true, NULLIF($6, ''), $7, $8::JSONB)",
        endpoint_id = m.q("endpoint_id"),
        tenant_id = m.q("tenant_id"),
        url = m.q("url"),
        topic_pattern = m.q("topic_pattern"),
        signing_secret = m.q("signing_secret"),
        active = m.q("active"),
        description = m.q("description"),
        max_attempts = m.q("max_attempts"),
        metadata_json = m.q("metadata_json"),
    ))
    .bind(&endpoint_id)
    .bind(&tenant_id)
    .bind(req.url.trim())
    .bind(&topic_pattern)
    .bind(&secret)
    .bind(req.description.trim())
    .bind(max_attempts)
    .bind(&metadata_json)
    .execute(pool)
    .await
    .map_err(|err| {
        webhook_internal_status(
            "create_webhook_endpoint",
            format!("create webhook endpoint failed: {err}"),
        )
    })?;

    emit_event(
        svc,
        TOPIC_ENDPOINT_CREATED,
        &endpoint_id,
        &tenant_id,
        serde_json::json!({
            "tenant_id": tenant_id,
            "endpoint_id": endpoint_id,
            "topic_pattern": topic_pattern,
        }),
    )
    .await;

    Ok(Response::new(webhook_pb::CreateEndpointResponse {
        endpoint_id,
        // Returned ONCE here; never surfaced by reads again.
        signing_secret: secret,
        message: "webhook endpoint created".to_string(),
        error: None,
    }))
}

pub(crate) async fn get_endpoint(
    svc: &WebhookServiceImpl,
    request: Request<webhook_pb::GetEndpointRequest>,
) -> Result<Response<webhook_pb::GetEndpointResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "webhook",
        OperationChannel::Read,
        &req.tenant_id,
        None,
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let endpoint_id = parse_uuid("endpoint_id", &req.endpoint_id)?.to_string();
    let pool = svc.require_pool()?;
    let m = endpoint_model();
    let rel = m.relation.clone();
    let projection = endpoint_select_projection(&m);
    let row = sqlx::query(&format!(
        "SELECT {projection} FROM {rel} \
         WHERE {endpoint_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL",
        endpoint_id = m.q("endpoint_id"),
        tenant_id = m.q("tenant_id"),
        deleted = m.q("deleted_at"),
    ))
    .bind(&endpoint_id)
    .bind(&tenant_id)
    .fetch_optional(pool)
    .await
    .map_err(|err| {
        webhook_internal_status(
            "get_webhook_endpoint",
            format!("get webhook endpoint failed: {err}"),
        )
    })?;
    let endpoint = row
        .as_ref()
        .map(endpoint_from_row)
        .transpose()?
        .ok_or_else(|| webhook_endpoint_not_found_status("get_endpoint"))?;
    Ok(Response::new(webhook_pb::GetEndpointResponse {
        endpoint: Some(endpoint),
        error: None,
    }))
}

pub(crate) async fn list_endpoints(
    svc: &WebhookServiceImpl,
    request: Request<webhook_pb::ListEndpointsRequest>,
) -> Result<Response<webhook_pb::ListEndpointsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "webhook",
        OperationChannel::Read,
        &req.tenant_id,
        None,
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let pool = svc.require_pool()?;
    let m = endpoint_model();
    let rel = m.relation.clone();
    let projection = endpoint_select_projection(&m);
    let page_window = native_offset_page_window(req.page, req.page_size, &req.page_token, 50);
    let active_clause = if req.active_only {
        format!("AND {active} = true", active = m.q("active"))
    } else {
        String::new()
    };
    let where_clause = format!(
        "WHERE {tenant_id} = $1::UUID AND {deleted} IS NULL {active_clause}",
        tenant_id = m.q("tenant_id"),
        deleted = m.q("deleted_at"),
    );
    let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {rel} {where_clause}"))
        .bind(&tenant_id)
        .fetch_one(pool)
        .await
        .map_err(|err| {
            webhook_internal_status(
                "list_webhook_endpoints_count",
                format!("count webhook endpoints failed: {err}"),
            )
        })?;
    let rows = sqlx::query(&format!(
        "SELECT {projection} FROM {rel} {where_clause} \
         ORDER BY {endpoint_id} LIMIT $2 OFFSET $3",
        endpoint_id = m.q("endpoint_id"),
    ))
    .bind(&tenant_id)
    .bind(page_window.limit_i64())
    .bind(page_window.offset_i64())
    .fetch_all(pool)
    .await
    .map_err(|err| {
        webhook_internal_status(
            "list_webhook_endpoints",
            format!("list webhook endpoints failed: {err}"),
        )
    })?;
    let mut endpoints = Vec::with_capacity(rows.len());
    for row in &rows {
        endpoints.push(endpoint_from_row(row)?);
    }
    Ok(Response::new(webhook_pb::ListEndpointsResponse {
        endpoints,
        total_count: total as i32,
        error: None,
        next_page_token: native_next_page_token_for_total(
            page_window.offset,
            page_window.limit,
            total,
        ),
    }))
}

pub(crate) async fn update_endpoint(
    svc: &WebhookServiceImpl,
    request: Request<webhook_pb::UpdateEndpointRequest>,
) -> Result<Response<webhook_pb::UpdateEndpointResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    // A changed URL is SSRF-revalidated before it can be stored.
    if !req.url.trim().is_empty() {
        validate_webhook_target_url(&req.url)?;
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "webhook",
        OperationChannel::Admin,
        &req.tenant_id,
        None,
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let endpoint_id = parse_uuid("endpoint_id", &req.endpoint_id)?.to_string();
    let pool = svc.require_pool()?;
    let m = endpoint_model();
    let rel = m.relation.clone();
    let update_mask = update_mask_path_set(
        req.update_mask.as_ref(),
        &[
            "url",
            "topic_pattern",
            "description",
            "active",
            "max_attempts",
        ],
    )?;
    let update_url = update_mask_allows(&update_mask, "url", !req.url.trim().is_empty());
    let update_topic_pattern = update_mask_allows(
        &update_mask,
        "topic_pattern",
        !req.topic_pattern.trim().is_empty(),
    );
    let update_description = update_mask_allows(
        &update_mask,
        "description",
        !req.description.trim().is_empty(),
    );
    let update_active = update_mask_allows(&update_mask, "active", true);
    let update_max_attempts =
        update_mask_allows(&update_mask, "max_attempts", req.max_attempts > 0);
    let max_attempts = if update_max_attempts && req.max_attempts > 0 {
        clamp_max_attempts(req.max_attempts)
    } else {
        0
    };
    let result = sqlx::query(&format!(
        "UPDATE {rel} SET \
           {url} = CASE WHEN $2 THEN $3 ELSE {url} END, \
           {topic_pattern} = CASE WHEN $4 THEN $5 ELSE {topic_pattern} END, \
           {description} = CASE WHEN $6 THEN $7 ELSE {description} END, \
           {active} = CASE WHEN $8 THEN $9 ELSE {active} END, \
           {max_attempts} = CASE WHEN $10 THEN $11 ELSE {max_attempts} END \
         WHERE {endpoint_id} = $1::UUID AND {tenant_id} = $12::UUID AND {deleted} IS NULL",
        url = m.q("url"),
        topic_pattern = m.q("topic_pattern"),
        description = m.q("description"),
        active = m.q("active"),
        max_attempts = m.q("max_attempts"),
        endpoint_id = m.q("endpoint_id"),
        tenant_id = m.q("tenant_id"),
        deleted = m.q("deleted_at"),
    ))
    .bind(&endpoint_id)
    .bind(update_url)
    .bind(req.url.trim())
    .bind(update_topic_pattern)
    .bind(req.topic_pattern.trim())
    .bind(update_description)
    .bind(req.description.trim())
    .bind(update_active)
    .bind(req.active)
    .bind(update_max_attempts)
    .bind(max_attempts)
    .bind(&tenant_id)
    .execute(pool)
    .await
    .map_err(|err| {
        webhook_internal_status(
            "update_webhook_endpoint",
            format!("update webhook endpoint failed: {err}"),
        )
    })?;
    if result.rows_affected() == 0 {
        return Err(webhook_endpoint_not_found_status("update_endpoint"));
    }
    emit_event(
        svc,
        TOPIC_ENDPOINT_UPDATED,
        &endpoint_id,
        &tenant_id,
        serde_json::json!({ "tenant_id": tenant_id, "endpoint_id": endpoint_id }),
    )
    .await;
    Ok(Response::new(webhook_pb::UpdateEndpointResponse {
        message: "webhook endpoint updated".to_string(),
        error: None,
    }))
}

pub(crate) async fn delete_endpoint(
    svc: &WebhookServiceImpl,
    request: Request<webhook_pb::DeleteEndpointRequest>,
) -> Result<Response<webhook_pb::DeleteEndpointResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "webhook",
        OperationChannel::Admin,
        &req.tenant_id,
        None,
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let endpoint_id = parse_uuid("endpoint_id", &req.endpoint_id)?.to_string();
    let pool = svc.require_pool()?;
    let m = endpoint_model();
    let rel = m.relation.clone();
    // Soft delete: stop delivering to the endpoint, keep the audit row.
    let result = sqlx::query(&format!(
        "UPDATE {rel} SET {deleted} = now(), {active} = false \
         WHERE {endpoint_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL",
        deleted = m.q("deleted_at"),
        active = m.q("active"),
        endpoint_id = m.q("endpoint_id"),
        tenant_id = m.q("tenant_id"),
    ))
    .bind(&endpoint_id)
    .bind(&tenant_id)
    .execute(pool)
    .await
    .map_err(|err| {
        webhook_internal_status(
            "delete_webhook_endpoint",
            format!("delete webhook endpoint failed: {err}"),
        )
    })?;
    if result.rows_affected() == 0 {
        return Err(webhook_endpoint_not_found_status("delete_endpoint"));
    }
    emit_event(
        svc,
        TOPIC_ENDPOINT_DELETED,
        &endpoint_id,
        &tenant_id,
        serde_json::json!({ "tenant_id": tenant_id, "endpoint_id": endpoint_id }),
    )
    .await;
    Ok(Response::new(webhook_pb::DeleteEndpointResponse {
        message: "webhook endpoint deleted".to_string(),
        error: None,
    }))
}

pub(crate) async fn list_deliveries(
    svc: &WebhookServiceImpl,
    request: Request<webhook_pb::ListDeliveriesRequest>,
) -> Result<Response<webhook_pb::ListDeliveriesResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "webhook",
        OperationChannel::Read,
        &req.tenant_id,
        None,
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    // Optional endpoint filter; an empty value lists all endpoints' deliveries.
    let endpoint_filter = if req.endpoint_id.trim().is_empty() {
        String::new()
    } else {
        parse_uuid("endpoint_id", &req.endpoint_id)?.to_string()
    };
    let status_filter = req.status.trim().to_ascii_uppercase();
    let pool = svc.require_pool()?;
    let m = delivery_model();
    let rel = m.relation.clone();
    let projection = delivery_select_projection(&m);
    let page_window = native_offset_page_window(req.page, req.page_size, &req.page_token, 50);
    let where_clause = format!(
        "WHERE {tenant_id} = $1::UUID \
         AND ($2 = '' OR {endpoint_id} = $2::UUID) \
         AND ($3 = '' OR {status} = $3)",
        tenant_id = m.q("tenant_id"),
        endpoint_id = m.q("endpoint_id"),
        status = m.q("status"),
    );
    let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {rel} {where_clause}"))
        .bind(&tenant_id)
        .bind(&endpoint_filter)
        .bind(&status_filter)
        .fetch_one(pool)
        .await
        .map_err(|err| {
            webhook_internal_status(
                "list_webhook_deliveries_count",
                format!("count webhook deliveries failed: {err}"),
            )
        })?;
    let rows = sqlx::query(&format!(
        "SELECT {projection} FROM {rel} {where_clause} \
         ORDER BY {delivered_at} DESC NULLS LAST, {delivery_id} LIMIT $4 OFFSET $5",
        delivered_at = m.q("delivered_at"),
        delivery_id = m.q("delivery_id"),
    ))
    .bind(&tenant_id)
    .bind(&endpoint_filter)
    .bind(&status_filter)
    .bind(page_window.limit_i64())
    .bind(page_window.offset_i64())
    .fetch_all(pool)
    .await
    .map_err(|err| {
        webhook_internal_status(
            "list_webhook_deliveries",
            format!("list webhook deliveries failed: {err}"),
        )
    })?;
    let mut deliveries = Vec::with_capacity(rows.len());
    for row in &rows {
        deliveries.push(delivery_from_row(row)?);
    }
    Ok(Response::new(webhook_pb::ListDeliveriesResponse {
        deliveries,
        total_count: total as i32,
        error: None,
        next_page_token: native_next_page_token_for_total(
            page_window.offset,
            page_window.limit,
            total,
        ),
    }))
}
