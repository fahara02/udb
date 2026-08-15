//! The five `ConfigService` RPC handlers (put/get/list/delete/evaluate). Extracted
//! from the trait impl; `mod.rs` delegates one line to each. Every handler takes
//! the tenant from the VERIFIED claim (never the request body) and mediates all
//! store access through the neutral IR runtime.

use std::collections::HashMap;

use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::LogicalDelete;
use crate::proto::udb::core::config::services::v1 as config_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    admit_on as native_admit_on, native_next_page_token, native_offset_page_window,
    non_empty_json, validated_native_service_context,
};
use super::ConfigServiceImpl;
use super::codec::{flag_val_to_proto, flag_val_to_stored, proto_to_flag_val};
use super::config::{CONFIG_MSG, DEFAULT_LIST_LIMIT, MAX_LIST_LIMIT, eval_ttl_seconds};
use super::errors::{ensure_evaluate_key_limit, require_flag_key};
use super::eval::{EvalContext, EvalFlag, bump_revision, evaluate_flag, resolve_flag};
use super::events::{emit_flag_changed, event_actor};
use super::store::{
    eval_flag_from_json, flag_candidates_batch_read, flag_conflict, flag_filter, flag_list_read,
    flag_read_exact, flag_record, flag_state_from_json,
};

pub(crate) async fn put_flag(
    svc: &ConfigServiceImpl,
    request: Request<config_pb::PutFlagRequest>,
) -> Result<Response<config_pb::PutFlagResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // Scope guard FIRST: body tenant/project must match the verified claim and
    // request metadata before either value reaches the runtime context.
    let tenant_id = req.tenant_id.trim().to_string();
    let project_id = req.project_id.trim().to_string();
    let context = validated_native_service_context(&metadata, &tenant_id, &project_id)?;
    let environment = req.environment.trim().to_string();
    let flag_key = require_flag_key(&req.flag_key)?;
    let value = proto_to_flag_val(&req.value)?;
    let (value_type, value_json) = flag_val_to_stored(&value);
    let rollout_percentage = req.rollout_percentage.clamp(0, 100);
    let rollout_context_key = req.rollout_context_key.trim().to_string();
    let metadata_json = non_empty_json(&req.metadata_json);

    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "config",
        OperationChannel::Write,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;

    // Existing row at this exact scope (for stable flag_id + revision bump).
    let existing = runtime
        .native_entity_read_for_service(
            "config",
            &context,
            flag_read_exact(&tenant_id, &project_id, &environment, &flag_key),
        )
        .await?
        .first()
        .map(eval_flag_from_json);

    let flag_id = existing
        .as_ref()
        .map(|f| f.flag_id.clone())
        .filter(|id| !id.trim().is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let revision = bump_revision(existing.as_ref().map(|f| f.revision).unwrap_or(0));

    runtime
        .native_entity_write_for_service(
            "config",
            &context,
            CONFIG_MSG,
            flag_record(
                &flag_id,
                &tenant_id,
                &project_id,
                &environment,
                &flag_key,
                &value_type,
                &value_json,
                req.enabled,
                rollout_percentage,
                &rollout_context_key,
                revision,
                &metadata_json,
            ),
            flag_conflict(),
        )
        .await?;

    let actor = event_actor();
    emit_flag_changed(svc, &tenant_id, &project_id, &flag_key, &actor, revision).await;

    Ok(Response::new(config_pb::PutFlagResponse {
        stored: true,
        flag_key,
        revision,
        message: "flag stored".to_string(),
        error: None,
    }))
}

pub(crate) async fn get_flag(
    svc: &ConfigServiceImpl,
    request: Request<config_pb::GetFlagRequest>,
) -> Result<Response<config_pb::GetFlagResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let tenant_id = req.tenant_id.trim().to_string();
    let project_id = req.project_id.trim().to_string();
    let context = validated_native_service_context(&metadata, &tenant_id, &project_id)?;
    let environment = req.environment.trim().to_string();
    let flag_key = require_flag_key(&req.flag_key)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "config",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;

    let found = runtime
        .native_entity_read_for_service(
            "config",
            &context,
            flag_read_exact(&tenant_id, &project_id, &environment, &flag_key),
        )
        .await?
        .first()
        .map(|row| flag_state_from_json(row, &tenant_id));

    Ok(Response::new(config_pb::GetFlagResponse {
        found: found.is_some(),
        flag: found,
        message: String::new(),
        error: None,
    }))
}

pub(crate) async fn list_flags(
    svc: &ConfigServiceImpl,
    request: Request<config_pb::ListFlagsRequest>,
) -> Result<Response<config_pb::ListFlagsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let tenant_id = req.tenant_id.trim().to_string();
    let project_id = req.project_id.trim().to_string();
    let context = validated_native_service_context(&metadata, &tenant_id, &project_id)?;
    let environment = req.environment.trim().to_string();
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
        "config",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;

    let project_filter = (!project_id.is_empty()).then_some(project_id.as_str());
    let env_filter = (!environment.is_empty()).then_some(environment.as_str());
    let flags = runtime
        .native_entity_read_for_service(
            "config",
            &context,
            flag_list_read(
                &tenant_id,
                project_filter,
                env_filter,
                page_window.offset as u64,
                (page_window.limit as u32).min(MAX_LIST_LIMIT),
            ),
        )
        .await?
        .iter()
        .map(|row| flag_state_from_json(row, &tenant_id))
        .collect::<Vec<_>>();
    let next_page_token =
        native_next_page_token(page_window.offset, page_window.limit, flags.len());

    Ok(Response::new(config_pb::ListFlagsResponse {
        flags,
        message: String::new(),
        error: None,
        next_page_token,
    }))
}

pub(crate) async fn delete_flag(
    svc: &ConfigServiceImpl,
    request: Request<config_pb::DeleteFlagRequest>,
) -> Result<Response<config_pb::DeleteFlagResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let tenant_id = req.tenant_id.trim().to_string();
    let project_id = req.project_id.trim().to_string();
    let context = validated_native_service_context(&metadata, &tenant_id, &project_id)?;
    let environment = req.environment.trim().to_string();
    let flag_key = require_flag_key(&req.flag_key)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "config",
        OperationChannel::Write,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;

    let existing = runtime
        .native_entity_read_for_service(
            "config",
            &context,
            flag_read_exact(&tenant_id, &project_id, &environment, &flag_key),
        )
        .await?
        .first()
        .map(eval_flag_from_json);

    let Some(existing) = existing else {
        // Idempotent: nothing to delete.
        return Ok(Response::new(config_pb::DeleteFlagResponse {
            deleted: true,
            revision: 0,
            message: "flag not found".to_string(),
            error: None,
        }));
    };

    runtime
        .native_entity_delete_for_service(
            "config",
            &context,
            LogicalDelete {
                message_type: CONFIG_MSG.to_string(),
                filter: flag_filter(
                    &tenant_id,
                    Some(&project_id),
                    Some(&environment),
                    Some(&flag_key),
                ),
                return_fields: Vec::new(),
            },
        )
        .await?;

    // A delete is a config change: bump the revision in the emitted event.
    let revision = bump_revision(existing.revision);
    let actor = event_actor();
    emit_flag_changed(svc, &tenant_id, &project_id, &flag_key, &actor, revision).await;

    Ok(Response::new(config_pb::DeleteFlagResponse {
        deleted: true,
        revision,
        message: "flag deleted".to_string(),
        error: None,
    }))
}

pub(crate) async fn evaluate_flags(
    svc: &ConfigServiceImpl,
    request: Request<config_pb::EvaluateFlagsRequest>,
) -> Result<Response<config_pb::EvaluateFlagsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let tenant_id = req.tenant_id.trim().to_string();
    ensure_evaluate_key_limit(req.keys.len())?;
    let ctx_pb = req.context.unwrap_or_default();
    let eval_ctx = EvalContext {
        project_id: ctx_pb.project_id.trim().to_string(),
        environment: ctx_pb.environment.trim().to_string(),
        attributes: ctx_pb.attributes,
    };
    let context =
        validated_native_service_context(&metadata, &tenant_id, &eval_ctx.project_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "config",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;

    // Trimmed, non-empty, de-duplicated keys — preserves the previous
    // per-key loop's semantics (blank keys skipped; a duplicate key would
    // evaluate identically, so it is read and resolved once). The key count
    // is already gated by `ensure_evaluate_key_limit` above.
    let mut keys: Vec<String> = Vec::with_capacity(req.keys.len());
    for key in &req.keys {
        let key = key.trim();
        if key.is_empty() || keys.iter().any(|seen| seen == key) {
            continue;
        }
        keys.push(key.to_string());
    }

    let mut values: HashMap<String, config_pb::FlagValue> = HashMap::new();
    let mut config_revision: i64 = 0;
    if !keys.is_empty() {
        // ONE tenant-scoped mediated read for every requested key (was one
        // read per key — up to MAX_EVALUATE_KEYS sequential round-trips).
        let rows = runtime
            .native_entity_read_for_service(
                "config",
                &context,
                flag_candidates_batch_read(&tenant_id, &keys),
            )
            .await?;
        // Bucket candidates per key, then resolve in memory through the
        // same PURE core (`resolve_flag` scope precedence + `evaluate_flag`
        // rollout hashing) the per-key loop used — semantics unchanged.
        let mut candidates_by_key: HashMap<String, Vec<EvalFlag>> =
            HashMap::with_capacity(keys.len());
        for row in &rows {
            let flag = eval_flag_from_json(row);
            candidates_by_key
                .entry(flag.flag_key.clone())
                .or_default()
                .push(flag);
        }
        for key in &keys {
            let Some(candidates) = candidates_by_key.get(key.as_str()) else {
                continue;
            };
            if let Some(flag) = resolve_flag(candidates, &eval_ctx) {
                config_revision = config_revision.max(flag.revision);
                let resolved = evaluate_flag(flag, &eval_ctx);
                values.insert(key.clone(), flag_val_to_proto(&resolved));
            }
        }
    }

    Ok(Response::new(config_pb::EvaluateFlagsResponse {
        values,
        server_ttl_seconds: eval_ttl_seconds(),
        config_revision,
        message: String::new(),
        error: None,
    }))
}
