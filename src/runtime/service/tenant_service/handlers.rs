//! The seven `TenantService` RPC handlers, extracted from the trait impl as free
//! `pub(crate) async fn`s taking `svc` where the trait method took `&self`.
//! `mod.rs` delegates one line to each. Bodies are verbatim — the same admission,
//! tenant-scope validation, bespoke transitional SQL, and contract-declared event
//! emission as the former god file.

use sqlx::Row;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::ConflictStrategy;
use crate::proto::udb::core::tenant::services::v1 as tenant_pb;
use crate::runtime::channels::OperationChannel;
use crate::runtime::service::method_security::{claim_context_present, current_claim_context};
use crate::runtime::tenant_movement::{
    TenantMovementOperation, TenantMovementRequest, tenant_movement_policy_status,
    validate_tenant_movement_scope,
};

use super::super::native_helpers::{
    MAX_LIST_ROWS, admit_on as native_admit_on, native_next_page_token_for_total,
    native_offset_page_window, non_empty_json, parse_uuid, tenant_only_native_service_context,
    update_mask_allows, update_mask_path_set, validate_request_tenant,
};
use super::TenantServiceImpl;
use super::config::{
    DEFAULT_TENANT_LIST_PAGE_SIZE, DEFAULT_TENANT_TYPE_DB, EVENT_OP_TENANT_PURGE,
    EVENT_TYPE_TENANT_CONFIG_UPDATED, EVENT_TYPE_TENANT_CREATED, EVENT_TYPE_TENANT_UPDATED,
    TENANT_CONFIG_MSG, TENANT_STATUS_ACTIVE_DB, TOPIC_TENANT_CONFIG_UPDATED, TOPIC_TENANT_CREATED,
    TOPIC_TENANT_PURGED, TOPIC_TENANT_UPDATED,
};
use super::errors::{
    tenant_already_exists_status, tenant_field_violation, tenant_internal_status,
    tenant_not_found_status, tenant_required_field, validate_create_tenant_required_fields,
};
use super::events::{
    emit_event, emit_event_in_tx, tenant_config_event_payload, tenant_lifecycle_event_payload,
};
use super::gate;
use super::model::{
    config_type_to_db, tenant_config_from_json, tenant_from_json, tenant_from_row, tenant_model,
    tenant_select_projection, tenant_status_to_db, tenant_type_to_db,
};
use super::store::{
    list_tenants_scope, list_tenants_subtree_predicate, tenant_config_read, tenant_config_record,
    tenant_read_by_id,
};

pub(crate) async fn create_tenant(
    svc: &TenantServiceImpl,
    request: Request<tenant_pb::CreateTenantRequest>,
) -> Result<Response<tenant_pb::CreateTenantResponse>, Status> {
    let req = request.into_inner();
    validate_create_tenant_required_fields(&req.code, &req.name)?;
    // M11 — parent authz (no DB access yet): reject creating a child under a
    // tenant the caller does not own BEFORE charging admission or touching the
    // store. Only a genuine cross-tenant admin may parent under an arbitrary
    // tenant; every other authenticated caller may parent ONLY under its own
    // VALIDATED claim tenant — so a tenant-A token cannot graft children under a
    // victim tenant B. An in-process/trusted caller (no claim context) is not
    // gated here, matching the other native handlers.
    let claim = current_claim_context();
    let parent = req.parent_tenant_id.trim().to_string();
    if !parent.is_empty() {
        // Reject a malformed/garbage parent id up front (fail closed).
        parse_uuid("parent_tenant_id", &parent)?;
        if claim_context_present()
            && !claim.is_cross_tenant_admin()
            && claim.tenant_id.trim() != parent
        {
            return Err(crate::runtime::executor_utils::policy_status_with_code(
                tonic::Code::PermissionDenied,
                "create_tenant",
                "parent_tenant_forbidden",
                "cannot create a tenant under a parent you do not own",
            ));
        }
    }
    // Per-tenant fair admission. CreateTenant has no body tenant_id yet, so it
    // scopes to the parent tenant when supplied (else the shared base budget).
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "tenant",
        OperationChannel::Admin,
        &req.parent_tenant_id,
        None,
    )
    .await?;
    let pool = svc.require_pool()?;
    let m = tenant_model();
    let rel = m.relation.clone();
    // M11 — the parent must actually exist (and not be soft-deleted): reject a
    // dangling/unknown parent so a child is never orphaned under a non-existent
    // tenant. Runs after the pool is available; `parent` already passed the UUID
    // + ownership checks above.
    if !parent.is_empty() {
        let parent_exists: bool = sqlx::query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM {rel} \
             WHERE {tenant_id} = $1::UUID AND {deleted_at} IS NULL)",
            tenant_id = m.q("tenant_id"),
            deleted_at = m.q("deleted_at"),
        ))
        .bind(&parent)
        .fetch_one(pool)
        .await
        .map_err(|err| {
            tenant_internal_status(
                "create_tenant_parent_check",
                format!("parent tenant existence check failed: {err}"),
            )
        })?;
        if !parent_exists {
            return Err(tenant_field_violation(
                "parent_tenant_id",
                "referenced parent tenant does not exist",
                "parent tenant does not exist",
            ));
        }
    }
    let tenant_id = Uuid::new_v4().to_string();
    let kind = tenant_type_to_db(&req.r#type, DEFAULT_TENANT_TYPE_DB)?;
    let config = non_empty_json(&req.config);
    let branding = non_empty_json(&req.branding);
    // Idempotent on the unique `code`: a repeated CreateTenant with the same
    // code is a no-op insert (ON CONFLICT DO NOTHING). M22 — `RETURNING tenant_id`
    // distinguishes a REAL insert from the conflict no-op: `Some` = a row was
    // created (emit `tenant.created`, return its id); `None` = the code already
    // existed, so nothing was created (NO spurious event, and no existing UUID is
    // disclosed to a non-owner below).
    //
    // P4 transitional path: the current native LogicalWrite conflict target is
    // the message primary key (`tenant_id`), not the alternate unique `code`.
    // Keep this bespoke insert until alternate-conflict/upsert-by-code is
    // expressible in the IR; falling back to primary-key conflict would break
    // CreateTenant idempotency.
    // The tenant row and its `tenant.created` event commit TOGETHER. Emitting
    // after the insert had already committed left a window where the tenant
    // existed with no event ever enqueued and nothing to re-derive it, so a
    // downstream provisioning consumer silently never learned about it. The
    // outbox insert below runs through this same transaction.
    let mut tx = pool.begin().await.map_err(|err| {
        tenant_internal_status("create_tenant", format!("create tenant failed: {err}"))
    })?;
    let inserted_id: Option<String> = sqlx::query_scalar(&format!(
        "INSERT INTO {rel} \
         ({tenant_id}, {code}, {name}, {type_col}, {status}, {parent}, {config}, {branding}) \
         VALUES ($1::UUID, $2, $3, $4, '{status_active}', NULLIF($5, '')::UUID, $6::JSONB, $7::JSONB) \
         ON CONFLICT ({code}) DO NOTHING \
         RETURNING {tenant_id}::text",
        status_active = TENANT_STATUS_ACTIVE_DB,
        tenant_id = m.q("tenant_id"),
        code = m.q("code"),
        name = m.q("name"),
        type_col = m.q("type"),
        status = m.q("status"),
        parent = m.q("parent_tenant_id"),
        config = m.q("config"),
        branding = m.q("branding"),
    ))
    .bind(&tenant_id)
    .bind(&req.code)
    .bind(&req.name)
    .bind(&kind)
    .bind(&req.parent_tenant_id)
    .bind(&config)
    .bind(&branding)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|err| {
        tenant_internal_status("create_tenant", format!("create tenant failed: {err}"))
    })?;

    if let Some(created_id) = inserted_id {
        // M22 — a row was actually created; its status is the column default
        // ACTIVE. Prime the fast suspension signal (so a later suspend can revoke
        // its tokens) and emit the contract-declared `tenant.created` event ONLY
        // now (never on the conflict no-op path). Payload carries identifiers +
        // status only (no secrets), same shape as PurgeTenant.
        emit_event_in_tx(
            svc,
            &mut tx,
            TOPIC_TENANT_CREATED,
            EVENT_TYPE_TENANT_CREATED,
            &created_id,
            &created_id,
            tenant_lifecycle_event_payload(&created_id, &req.code, TENANT_STATUS_ACTIVE_DB),
        )
        .await
        .map_err(|err| {
            tenant_internal_status(
                "create_tenant_event",
                format!("create tenant event enqueue failed: {err}"),
            )
        })?;
        tx.commit().await.map_err(|err| {
            tenant_internal_status(
                "create_tenant",
                format!("create tenant commit failed: {err}"),
            )
        })?;
        // Primed only once the row is durable: the in-memory suspension signal
        // must not claim a tenant that a rolled-back transaction never created.
        gate::mark_tenant_status(&created_id, TENANT_STATUS_ACTIVE_DB);
        return Ok(Response::new(tenant_pb::CreateTenantResponse {
            tenant_id: created_id,
            message: "tenant created".to_string(),
            error: None,
        }));
    }

    // Nothing was inserted, so the transaction holds no work; close it before the
    // read-only disclosure path below, which runs on the pool.
    tx.rollback().await.map_err(|err| {
        tenant_internal_status(
            "create_tenant",
            format!("create tenant conflict rollback failed: {err}"),
        )
    })?;

    // M22 — the code already exists and nothing was created. Do NOT emit a
    // spurious event. Idempotent disclosure of the surviving row's canonical id
    // is allowed ONLY to a caller that OWNS it: an in-process/trusted caller (no
    // claim context), a genuine cross-tenant admin, or the tenant itself (claim
    // tenant == the surviving row). Every other caller gets an OPAQUE
    // ALREADY_EXISTS that leaks neither the existing UUID nor its status.
    let owner_view = !claim_context_present() || claim.is_cross_tenant_admin();
    let resolved = sqlx::query(&format!(
        "SELECT {tenant_id}::text AS tenant_id \
         FROM {rel} WHERE {code} = $1 AND {deleted_at} IS NULL",
        tenant_id = m.q("tenant_id"),
        code = m.q("code"),
        deleted_at = m.q("deleted_at"),
    ))
    .bind(&req.code)
    .fetch_optional(pool)
    .await
    .map_err(|err| {
        tenant_internal_status(
            "resolve_tenant_after_create",
            format!("resolve tenant after create failed: {err}"),
        )
    })?;
    // A conflict on the unique code with no surviving active row (e.g. the
    // colliding row is soft-deleted) reveals nothing either.
    let Some(resolved) = resolved else {
        return Err(tenant_already_exists_status());
    };
    let decode = |e: sqlx::Error| {
        tenant_internal_status(
            "resolve_tenant_after_create",
            format!("decode tenant after create failed: {e}"),
        )
    };
    let canonical_id: String = resolved.try_get("tenant_id").map_err(decode)?;
    let caller_owns = owner_view || claim.tenant_id.trim() == canonical_id;
    if !caller_owns {
        return Err(tenant_already_exists_status());
    }
    Ok(Response::new(tenant_pb::CreateTenantResponse {
        tenant_id: canonical_id,
        message: "tenant already exists".to_string(),
        error: None,
    }))
}

/// Account/tenant HARD-DELETE with ripple (GDPR right-to-be-forgotten). Hard-
/// deletes every row the tenant owns across all manifest entity tables in one
/// transaction (children->parents), reports tenant-less tables as excluded,
/// then records the tenant-level revocation cutoff so pre-delete tokens are
/// rejected. DESTRUCTIVE + irreversible: requires an explicit confirmation
/// token and a body tenant_id matching the verified claim.
pub(crate) async fn purge_tenant(
    svc: &TenantServiceImpl,
    request: Request<tenant_pb::PurgeTenantRequest>,
) -> Result<Response<tenant_pb::PurgeTenantResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let tenant_id = req.tenant_id.trim().to_string();
    if tenant_id.is_empty() {
        return Err(tenant_required_field(
            "tenant_id",
            "must be a non-empty tenant id",
            "tenant_id is required",
        ));
    }
    // DESTRUCTIVE: an empty/missing confirmation token fails CLOSED — the purge
    // is irreversible, so it never runs without explicit confirmation.
    if req.confirmation_token.trim().is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "PurgeTenant is an irreversible hard delete; confirmation_token is required",
            [("confirmation_token", "must be present to purge tenant data")],
        ));
    }
    // No cross-tenant purge: the body tenant_id must match the verified claim.
    validate_request_tenant(&metadata, &tenant_id)?;
    let movement = TenantMovementRequest {
        operation: TenantMovementOperation::TenantPurge,
        tenant_id: &tenant_id,
        target_tenant_id: None,
        tenant_filter_present: true,
        privileged_cross_tenant: false,
    };
    validate_tenant_movement_scope(&movement)
        .map_err(|err| tenant_movement_policy_status(movement.operation, err))?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "tenant",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let pool = svc.require_pool()?;
    let manifest = svc.require_manifest()?;
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // The hard delete of the tenant's principal/session/token-family rows
    // makes validation's persisted-state checks reject their tokens; when
    // Redis is wired, the tenant cutoff denylist accelerates that cluster-wide
    // before each node reaches the durable read.
    let report = {
        #[cfg(feature = "redis")]
        {
            crate::runtime::core::purge_tenant(
                pool,
                manifest,
                &tenant_id,
                &[],
                svc.jti_denylist.as_ref(),
                now_unix,
            )
            .await?
        }
        #[cfg(not(feature = "redis"))]
        {
            crate::runtime::core::purge_tenant(pool, manifest, &tenant_id, &[], now_unix).await?
        }
    };
    let purged_payload = report
        .purged
        .iter()
        .map(|p| {
            serde_json::json!({
                "schema": &p.schema,
                "table": &p.table,
                "tenant_column": &p.tenant_column,
                "deleted": p.deleted,
            })
        })
        .collect::<Vec<_>>();
    let excluded_payload = report
        .excluded
        .iter()
        .map(|e| {
            serde_json::json!({
                "schema": &e.schema,
                "table": &e.table,
                "reason": &e.reason,
            })
        })
        .collect::<Vec<_>>();
    emit_event(
        svc,
        TOPIC_TENANT_PURGED,
        EVENT_OP_TENANT_PURGE,
        &tenant_id,
        &tenant_id,
        serde_json::json!({
            "tenant_id": &tenant_id,
            "purged": purged_payload,
            "excluded": excluded_payload,
            "total_deleted": report.total_deleted,
            "tenant_denylisted": report.tenant_denylisted,
            "principals_denylisted": report.principals_denylisted,
        }),
    )
    .await;
    Ok(Response::new(tenant_pb::PurgeTenantResponse {
        tenant_id: report.tenant_id,
        purged: report
            .purged
            .into_iter()
            .map(|p| tenant_pb::PurgedTableCount {
                schema: p.schema,
                table: p.table,
                tenant_column: p.tenant_column,
                deleted: p.deleted,
            })
            .collect(),
        excluded: report
            .excluded
            .into_iter()
            .map(|e| tenant_pb::PurgeExcludedTable {
                schema: e.schema,
                table: e.table,
                reason: e.reason,
            })
            .collect(),
        total_deleted: report.total_deleted,
        tenant_denylisted: report.tenant_denylisted,
        principals_denylisted: report.principals_denylisted as u32,
        message: "tenant purged".to_string(),
        error: None,
    }))
}

pub(crate) async fn get_tenant(
    svc: &TenantServiceImpl,
    request: Request<tenant_pb::GetTenantRequest>,
) -> Result<Response<tenant_pb::GetTenantResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "tenant",
        OperationChannel::Read,
        &req.tenant_id,
        None,
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;
    let mut rows = runtime
        .native_entity_read_for_service("tenant", &context, tenant_read_by_id(&tenant_id))
        .await?;
    let tenant = rows
        .pop()
        .map(|row| tenant_from_json(&row))
        .ok_or_else(|| tenant_not_found_status("get_tenant"))?;
    Ok(Response::new(tenant_pb::GetTenantResponse {
        tenant: Some(tenant),
        error: None,
    }))
}

pub(crate) async fn list_tenants(
    svc: &TenantServiceImpl,
    request: Request<tenant_pb::ListTenantsRequest>,
) -> Result<Response<tenant_pb::ListTenantsResponse>, Status> {
    let req = request.into_inner();
    // Scope the listing to the VALIDATED claim identity (there is no body
    // tenant to spoof): a cross-tenant admin keeps the unscoped platform list;
    // every other caller sees only its own tenant row + direct children, and
    // admission is charged to the caller's real claim tenant (never "").
    let claim = current_claim_context();
    let scope = list_tenants_scope(claim_context_present(), &claim)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "tenant",
        OperationChannel::Read,
        &scope.admit_tenant,
        None,
    )
    .await?;
    let pool = svc.require_pool()?;
    let m = tenant_model();
    let rel = m.relation.clone();
    let projection = tenant_select_projection(&m);
    let type_filter = tenant_type_to_db(&req.r#type, "")?;
    let status_filter = tenant_status_to_db(&req.status, "")?;
    let page_window = native_offset_page_window(
        req.page,
        req.page_size,
        &req.page_token,
        DEFAULT_TENANT_LIST_PAGE_SIZE,
    );
    // P4 transitional path: `ListTenants` returns an exact `total_count`.
    // The service helper currently exposes typed `LogicalRead`, not aggregate
    // count, so keep the existing SQL list/count path rather than deriving an
    // approximate count from the current page.
    let mut where_clause = format!(
        "WHERE {deleted} IS NULL AND ($1 = '' OR {type_col} = $1) AND ($2 = '' OR {status} = $2)",
        deleted = m.q("deleted_at"),
        type_col = m.q("type"),
        status = m.q("status"),
    );
    if scope.subtree_of.is_some() {
        where_clause.push_str(&format!(
            " AND {}",
            list_tenants_subtree_predicate(&m.q("tenant_id"), &m.q("parent_tenant_id"), "$3")
        ));
    }
    let count_sql = format!("SELECT COUNT(*) FROM {rel} {where_clause}");
    let mut count_query = sqlx::query_scalar(&count_sql)
        .bind(&type_filter)
        .bind(&status_filter);
    if let Some(subtree) = scope.subtree_of.as_ref() {
        count_query = count_query.bind(subtree);
    }
    let total: i64 = count_query.fetch_one(pool).await.map_err(|err| {
        tenant_internal_status("list_tenants_count", format!("count tenants failed: {err}"))
    })?;
    // The subtree bind shifts LIMIT/OFFSET one placeholder to the right.
    let (limit_bind, offset_bind) = if scope.subtree_of.is_some() {
        ("$4", "$5")
    } else {
        ("$3", "$4")
    };
    let list_sql = format!(
        "SELECT {projection} FROM {rel} {where_clause} \
         ORDER BY {code} LIMIT {limit_bind} OFFSET {offset_bind}",
        code = m.q("code"),
    );
    let mut list_query = sqlx::query(&list_sql)
        .bind(&type_filter)
        .bind(&status_filter);
    if let Some(subtree) = scope.subtree_of.as_ref() {
        list_query = list_query.bind(subtree);
    }
    let rows = list_query
        .bind(page_window.limit_i64())
        .bind(page_window.offset_i64())
        .fetch_all(pool)
        .await
        .map_err(|err| {
            tenant_internal_status("list_tenants", format!("list tenants failed: {err}"))
        })?;
    let mut tenants = Vec::with_capacity(rows.len());
    for row in &rows {
        tenants.push(tenant_from_row(row)?);
    }
    Ok(Response::new(tenant_pb::ListTenantsResponse {
        tenants,
        total_count: total as i32,
        error: None,
        next_page_token: native_next_page_token_for_total(
            page_window.offset,
            page_window.limit,
            total,
        ),
    }))
}

pub(crate) async fn update_tenant(
    svc: &TenantServiceImpl,
    request: Request<tenant_pb::UpdateTenantRequest>,
) -> Result<Response<tenant_pb::UpdateTenantResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "tenant",
        OperationChannel::Admin,
        &req.tenant_id,
        None,
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
    let update_mask = update_mask_path_set(
        req.update_mask.as_ref(),
        &["name", "status", "config", "branding"],
    )?;
    let update_name = update_mask_allows(&update_mask, "name", !req.name.trim().is_empty());
    let update_status = update_mask_allows(&update_mask, "status", !req.status.trim().is_empty());
    let update_config = update_mask_allows(&update_mask, "config", !req.config.trim().is_empty());
    let update_branding =
        update_mask_allows(&update_mask, "branding", !req.branding.trim().is_empty());
    let status = tenant_status_to_db(&req.status, "")?;
    let pool = svc.require_pool()?;
    let m = tenant_model();
    let rel = m.relation.clone();
    // P4 transitional path: native LogicalWrite is currently upsert-by-primary-key,
    // while this RPC is update-only and must not create or revive a deleted row.
    // Keep the predicate-bearing SQL until the IR/service helper can express an
    // update with `WHERE tenant_id = ? AND deleted_at IS NULL`.
    // RETURNING the post-update code/status feeds the contract-declared event
    // below with the row's REAL values (no extra roundtrip, no guessed status).
    let updated = sqlx::query(&format!(
        "UPDATE {rel} SET \
           {name} = CASE WHEN $2 THEN $3 ELSE {name} END, \
           {status} = CASE WHEN $4 THEN $5 ELSE {status} END, \
           {config} = CASE WHEN $6 THEN $7::JSONB ELSE {config} END, \
           {branding} = CASE WHEN $8 THEN $9::JSONB ELSE {branding} END \
         WHERE {tenant_id} = $1::UUID AND {deleted} IS NULL \
         RETURNING {code} AS code, {status} AS status",
        name = m.q("name"),
        status = m.q("status"),
        config = m.q("config"),
        branding = m.q("branding"),
        tenant_id = m.q("tenant_id"),
        deleted = m.q("deleted_at"),
        code = m.q("code"),
    ))
    .bind(tenant_id)
    .bind(update_name)
    .bind(&req.name)
    .bind(update_status)
    .bind(&status)
    .bind(update_config)
    .bind(req.config.trim())
    .bind(update_branding)
    .bind(req.branding.trim())
    .fetch_optional(pool)
    .await
    .map_err(|err| {
        tenant_internal_status("update_tenant", format!("update tenant failed: {err}"))
    })?;
    let Some(updated) = updated else {
        return Err(tenant_not_found_status("update_tenant"));
    };
    let decode = |e: sqlx::Error| {
        tenant_internal_status(
            "update_tenant",
            format!("decode updated tenant failed: {e}"),
        )
    };
    let code: String = updated.try_get("code").map_err(decode)?;
    let stored_status: String = updated.try_get("status").map_err(decode)?;
    let tenant_id = tenant_id.to_string();
    // H10 — record the tenant's NEW durable status in the process suspension
    // signal so a transition to SUSPENDED/INACTIVE revokes this tenant's LIVE
    // bearer tokens at the request gate immediately, instead of letting them run
    // for their full TTL. The shared method-security layer consults this via
    // `gate::tenant_status_gate` before dispatch (see its `TODO(leader-wire)`).
    gate::mark_tenant_status(&tenant_id, &stored_status);
    // Contract-declared tenant event (tenant_service.proto UpdateTenant
    // method_event_contract, partition key = tenant_id). Identifiers + status
    // only — never the config/branding bodies.
    emit_event(
        svc,
        TOPIC_TENANT_UPDATED,
        EVENT_TYPE_TENANT_UPDATED,
        &tenant_id,
        &tenant_id,
        tenant_lifecycle_event_payload(&tenant_id, &code, &stored_status),
    )
    .await;
    Ok(Response::new(tenant_pb::UpdateTenantResponse {
        message: "tenant updated".to_string(),
        error: None,
    }))
}

pub(crate) async fn get_tenant_config(
    svc: &TenantServiceImpl,
    request: Request<tenant_pb::GetTenantConfigRequest>,
) -> Result<Response<tenant_pb::GetTenantConfigResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "tenant",
        OperationChannel::Read,
        &req.tenant_id,
        None,
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;
    let rows = runtime
        .native_entity_read_for_service(
            "tenant",
            &context,
            tenant_config_read(&tenant_id, None, MAX_LIST_ROWS as u32),
        )
        .await?;
    let mut configs = rows
        .iter()
        .map(|row| tenant_config_from_json(row, &tenant_id))
        .collect::<Vec<_>>();
    configs.sort_by(|a, b| a.config_key.cmp(&b.config_key));
    Ok(Response::new(tenant_pb::GetTenantConfigResponse {
        configs,
        error: None,
    }))
}

pub(crate) async fn update_tenant_config(
    svc: &TenantServiceImpl,
    request: Request<tenant_pb::UpdateTenantConfigRequest>,
) -> Result<Response<tenant_pb::UpdateTenantConfigResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "tenant",
        OperationChannel::Admin,
        &req.tenant_id,
        None,
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
    if req.config_key.trim().is_empty() {
        return Err(tenant_required_field(
            "config_key",
            "must be a non-empty config key",
            "config_key is required",
        ));
    }
    let kind = config_type_to_db(&req.r#type, "STRING")?;
    let context = tenant_only_native_service_context(&metadata, &tenant_id);
    let runtime = svc.require_runtime()?;
    let existing = runtime
        .native_entity_read_for_service(
            "tenant",
            &context,
            tenant_config_read(&tenant_id, Some(req.config_key.trim()), 1),
        )
        .await?;
    let id = existing
        .first()
        .map(|row| tenant_config_from_json(row, &tenant_id).id)
        .filter(|id| !id.trim().is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    runtime
        .native_entity_write_for_service(
            "tenant",
            &context,
            TENANT_CONFIG_MSG,
            tenant_config_record(id, &tenant_id, &req, kind),
            ConflictStrategy::update(vec![
                "tenant_id".to_string(),
                "config_key".to_string(),
                "config_value".to_string(),
                "type".to_string(),
                "description".to_string(),
            ]),
        )
        .await?;
    // Contract-declared tenant event (tenant_service.proto UpdateTenantConfig
    // method_event_contract, partition key = tenant_id). Payload carries the
    // config KEY only — the value may hold secrets and never reaches the outbox.
    emit_event(
        svc,
        TOPIC_TENANT_CONFIG_UPDATED,
        EVENT_TYPE_TENANT_CONFIG_UPDATED,
        &tenant_id,
        &tenant_id,
        tenant_config_event_payload(&tenant_id, req.config_key.trim()),
    )
    .await;
    Ok(Response::new(tenant_pb::UpdateTenantConfigResponse {
        message: "tenant config updated".to_string(),
        error: None,
    }))
}
