//! The eight `AssetService` RPC handlers (create/get pipeline-definition,
//! register/list/get asset, start/get pipeline, complete step) extracted from the
//! trait impl as free `pub(crate) async fn`s taking `svc` where the trait method
//! took `&self`. `mod.rs` delegates one line to each. Bodies are verbatim — the
//! same scope guard, per-tenant admission, native-entity dispatch, in-process
//! step execution, vector upsert, and outbox emission as the former god file.

use sqlx::Row;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::ConflictStrategy;
use crate::proto::udb::core::asset::entity::v1 as asset_entity_pb;
use crate::proto::udb::core::asset::services::v1 as asset_pb;
use crate::runtime::channels::OperationChannel;
use crate::runtime::service::native_helpers::{
    admit_on as native_admit_on, emit_payload_event, native_next_page_token_for_total,
    native_offset_page_window, native_service_context, non_empty_json, parse_uuid,
    validate_request_scope, validate_request_tenant,
};

use super::AssetServiceImpl;
use super::config::{
    ASSET_MSG, ASSET_REGISTERED_TOPIC, PIPELINE_ALREADY_STARTED, PIPELINE_DEFINITION_INVALID,
    PIPELINE_DEFINITION_MSG, PIPELINE_STARTED_TOPIC, PIPELINE_STEP_COMPLETED_TOPIC,
    STEP_TYPE_UNSUPPORTED,
};
use super::errors::{
    active_storage_file_required_status, asset_internal_status, asset_invalid_field,
    asset_required_field, asset_schema_not_found_status, status_with_reason,
};
use super::execution::advance_instance;
use super::model::{
    asset_from_json, asset_model, asset_status_to_db, pipeline_definition_from_json,
    pipeline_definition_model, pipeline_instance_from_row, pipeline_instance_model,
    pipeline_instance_select_projection, pipeline_step_from_row, pipeline_step_model,
    pipeline_step_select_projection, step_status_from_db, step_status_to_db, step_type_from_db,
    step_type_to_db,
};
use super::steps::{StepContext, StepOutcome, is_byte_step, parse_byte_step_params, step_registry};
use super::store::{
    asset_read, asset_record, pipeline_definition_read, pipeline_definition_record,
};

pub(crate) async fn create_pipeline_definition(
    svc: &AssetServiceImpl,
    request: Request<asset_pb::CreatePipelineDefinitionRequest>,
) -> Result<Response<asset_pb::CreatePipelineDefinitionResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    // Per-tenant fair admission (Write budget) so one tenant's definition
    // writes can't starve the shared pool.
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "asset",
        OperationChannel::Write,
        &req.tenant_id,
        Some(""),
    )
    .await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
    if req.name.trim().is_empty() {
        return Err(asset_required_field(
            "name",
            "must be a non-empty pipeline definition name",
            "name is required",
        ));
    }
    let steps = {
        let s = req.steps.trim();
        if s.is_empty() {
            "[]".to_string()
        } else {
            serde_json::from_str::<serde_json::Value>(s).map_err(|e| {
                asset_invalid_field(
                    "steps",
                    "must be valid JSON",
                    format!("steps must be valid JSON: {e}"),
                )
            })?;
            s.to_string()
        }
    };
    let version = if req.version > 0 { req.version } else { 1 };
    let definition_id = Uuid::new_v4().to_string();
    let context = native_service_context(&metadata, &req.tenant_id, "");
    svc.require_runtime()?
        .native_entity_write_for_service(
            "asset",
            &context,
            PIPELINE_DEFINITION_MSG,
            pipeline_definition_record(
                &definition_id,
                &tenant_id.to_string(),
                &req,
                &steps,
                version,
            )?,
            ConflictStrategy::Error,
        )
        .await
        .map_err(|err| {
            crate::runtime::executor_utils::prefix_status("create pipeline definition failed", err)
        })?;
    Ok(Response::new(asset_pb::CreatePipelineDefinitionResponse {
        definition_id,
        message: "pipeline definition created".to_string(),
        error: None,
    }))
}

pub(crate) async fn get_pipeline_definition(
    svc: &AssetServiceImpl,
    request: Request<asset_pb::GetPipelineDefinitionRequest>,
) -> Result<Response<asset_pb::GetPipelineDefinitionResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    // Per-tenant fair admission (lighter Read budget) so reads can't starve the pool.
    let _admit = svc.admit_read(&req.tenant_id).await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
    let definition_id = parse_uuid("definition_id", &req.definition_id)?;
    let context = native_service_context(&metadata, &req.tenant_id, "");
    let rows = svc
        .require_runtime()?
        .native_entity_read_for_service(
            "asset",
            &context,
            pipeline_definition_read(&tenant_id.to_string(), &definition_id.to_string()),
        )
        .await?;
    let definition = rows.first().map(pipeline_definition_from_json);
    if definition.is_none() {
        return Err(asset_schema_not_found_status(
            "get_pipeline_definition",
            "pipeline_definition_not_found",
            "pipeline definition not found",
        ));
    }
    Ok(Response::new(asset_pb::GetPipelineDefinitionResponse {
        definition,
        error: None,
    }))
}

pub(crate) async fn register_asset(
    svc: &AssetServiceImpl,
    request: Request<asset_pb::RegisterAssetRequest>,
) -> Result<Response<asset_pb::RegisterAssetResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
    // Per-tenant fair admission (held for the whole RPC).
    let _admit = svc.admit(&req.tenant_id, &req.project_id).await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
    if req.file_id.trim().is_empty() {
        return Err(asset_required_field(
            "file_id",
            "must be a non-empty storage file id",
            "file_id is required",
        ));
    }
    let pool = svc.require_pool()?;
    // Tenant-bind the referenced storage file: refuse to wrap a file that
    // isn't an active file owned by this tenant (prevents cross-tenant
    // file references via a forged file_id).
    let file_uuid = parse_uuid("file_id", &req.file_id)?;
    if svc
        .resolve_object_key(pool, file_uuid, tenant_id)
        .await
        .is_none()
    {
        return Err(active_storage_file_required_status());
    }
    let asset_id = Uuid::new_v4().to_string();
    let asset_metadata = svc.encrypt_native_json_state(&non_empty_json(&req.metadata))?;
    let context = native_service_context(&metadata, &req.tenant_id, req.project_id.trim());
    svc.require_runtime()?
        .native_entity_write_for_service(
            "asset",
            &context,
            ASSET_MSG,
            asset_record(
                &asset_id,
                &tenant_id.to_string(),
                req.project_id.trim(),
                &req,
                &asset_metadata,
            )?,
            ConflictStrategy::Error,
        )
        .await
        .map_err(|err| {
            crate::runtime::executor_utils::prefix_status("register asset failed", err)
        })?;
    emit_payload_event(
        pool,
        svc.outbox_relation.as_deref(),
        ASSET_REGISTERED_TOPIC,
        &asset_id,
        serde_json::json!({
            "asset_id": asset_id,
            "tenant_id": req.tenant_id,
            "project_id": req.project_id,
            "file_id": req.file_id.trim(),
            "name": req.name,
            "media_type": req.media_type,
        }),
        Some(&svc.metrics),
    )
    .await;
    Ok(Response::new(asset_pb::RegisterAssetResponse {
        asset_id,
        message: "asset registered".to_string(),
        error: None,
    }))
}

pub(crate) async fn start_pipeline(
    svc: &AssetServiceImpl,
    request: Request<asset_pb::StartPipelineRequest>,
) -> Result<Response<asset_pb::StartPipelineResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    // Per-tenant fair admission (held for the whole RPC) — starting a
    // pipeline schedules heavy step work, so it's gated per tenant.
    let _admit = svc.admit(&req.tenant_id, "").await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
    let definition_id = parse_uuid("definition_id", &req.definition_id)?;
    let asset_id = parse_uuid("asset_id", &req.asset_id)?;
    let pool = svc.require_pool()?;
    let inst = pipeline_instance_model();
    let inst_rel = inst.relation.clone();
    let def = pipeline_definition_model();
    let def_rel = def.relation.clone();
    let step = pipeline_step_model();
    let step_rel = step.relation.clone();

    let correlation_id = req.correlation_id.trim().to_string();

    // IDEMPOTENCY: if a correlation id is supplied and an instance already
    // exists for it, return that instance without re-triggering.
    if !correlation_id.is_empty() {
        if let Some(existing) = sqlx::query(&format!(
            "SELECT {instance_id}::TEXT AS instance_id FROM {inst_rel} \
             WHERE {tenant_id} = $1::UUID AND {correlation_id} = $2",
            instance_id = inst.q("instance_id"),
            tenant_id = inst.q("tenant_id"),
            correlation_id = inst.q("correlation_id"),
        ))
        .bind(tenant_id)
        .bind(&correlation_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            asset_internal_status(
                "start_pipeline",
                format!("start pipeline lookup failed: {err}"),
            )
        })? {
            let instance_id: String = existing.try_get("instance_id").map_err(|e| {
                asset_internal_status("start_pipeline", format!("decode instance id failed: {e}"))
            })?;
            return Ok(Response::new(asset_pb::StartPipelineResponse {
                instance_id,
                message: format!("pipeline already started [{PIPELINE_ALREADY_STARTED}]"),
                error: None,
                // Idempotent hit: only the existing instance id is in scope.
                // Steps are left empty to avoid an extra round-trip; callers
                // wanting them for an already-running instance call GetPipeline.
                steps: Vec::new(),
            }));
        }
    }

    // Load the definition's step list.
    let steps_json: Option<String> = sqlx::query_scalar(&format!(
        "SELECT {steps}::TEXT FROM {def_rel} \
         WHERE {definition_id} = $1::UUID AND {tenant_id} = $2::UUID",
        steps = def.q("steps"),
        definition_id = def.q("definition_id"),
        tenant_id = def.q("tenant_id"),
    ))
    .bind(definition_id)
    .bind(tenant_id)
    .fetch_optional(pool)
    .await
    .map_err(|err| {
        asset_internal_status(
            "start_pipeline",
            format!("load pipeline definition failed: {err}"),
        )
    })?;
    let steps_json = match steps_json {
        Some(s) => s,
        None => {
            return Err(asset_schema_not_found_status(
                "start_pipeline",
                "pipeline_definition_not_found",
                "pipeline definition not found",
            ));
        }
    };
    let parsed: serde_json::Value = serde_json::from_str(&steps_json).map_err(|e| {
        status_with_reason(
            asset_internal_status(
                "start_pipeline",
                format!("pipeline definition steps not JSON: {e}"),
            ),
            PIPELINE_DEFINITION_INVALID,
        )
    })?;
    let step_array: Vec<serde_json::Value> = match parsed {
        serde_json::Value::Array(a) => a,
        _ => Vec::new(),
    };
    let first_step_name = step_array
        .first()
        .and_then(|el| el.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let instance_id = Uuid::new_v4().to_string();
    let context = svc.encrypt_native_json_state(&non_empty_json(&req.context))?;
    let insert_result = sqlx::query(&format!(
        "INSERT INTO {inst_rel} \
         ({instance_id}, {definition_id}, {asset_id}, {tenant_id}, {status}, {current_step}, {context}, {correlation_id}, {started_at}) \
         VALUES ($1::UUID, $2::UUID, $3::UUID, $4::UUID, 'RUNNING', $5, $6::JSONB, NULLIF($7, ''), CURRENT_TIMESTAMP)",
        instance_id = inst.q("instance_id"),
        definition_id = inst.q("definition_id"),
        asset_id = inst.q("asset_id"),
        tenant_id = inst.q("tenant_id"),
        status = inst.q("status"),
        current_step = inst.q("current_step"),
        context = inst.q("context"),
        correlation_id = inst.q("correlation_id"),
        started_at = inst.q("started_at"),
    ))
    .bind(&instance_id)
    .bind(definition_id)
    .bind(asset_id)
    .bind(tenant_id)
    .bind(&first_step_name)
    .bind(&context)
    .bind(&correlation_id)
    .execute(pool)
    .await;

    if let Err(err) = insert_result {
        let is_unique = err
            .as_database_error()
            .map(|e| e.is_unique_violation())
            .unwrap_or(false);
        if is_unique && !correlation_id.is_empty() {
            // Concurrent start with the same correlation id won the race;
            // return the existing instance instead of failing.
            let existing = sqlx::query(&format!(
                "SELECT {instance_id}::TEXT AS instance_id FROM {inst_rel} \
                 WHERE {tenant_id} = $1::UUID AND {correlation_id} = $2",
                instance_id = inst.q("instance_id"),
                tenant_id = inst.q("tenant_id"),
                correlation_id = inst.q("correlation_id"),
            ))
            .bind(tenant_id)
            .bind(&correlation_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                asset_internal_status(
                    "start_pipeline",
                    format!("start pipeline re-lookup failed: {e}"),
                )
            })?;
            if let Some(row) = existing {
                let id: String = row.try_get("instance_id").map_err(|e| {
                    asset_internal_status(
                        "start_pipeline",
                        format!("decode instance id failed: {e}"),
                    )
                })?;
                return Ok(Response::new(asset_pb::StartPipelineResponse {
                    instance_id: id,
                    message: format!("pipeline already started [{PIPELINE_ALREADY_STARTED}]"),
                    error: None,
                    // Race branch: only the existing instance id is in scope.
                    // Reading its steps would cost an extra round-trip, so the
                    // step list is left empty here; callers wanting steps for an
                    // already-running instance call GetPipeline.
                    steps: Vec::new(),
                }));
            }
        }
        return Err(crate::runtime::executor_utils::sqlx_error_to_status(
            "start pipeline failed",
            &err,
        ));
    }

    // Pipeline started → emit the lifecycle event.
    emit_payload_event(
        pool,
        svc.outbox_relation.as_deref(),
        PIPELINE_STARTED_TOPIC,
        &instance_id,
        serde_json::json!({
            "instance_id": instance_id,
            "definition_id": req.definition_id,
            "asset_id": req.asset_id,
            "tenant_id": req.tenant_id,
        }),
        Some(&svc.metrics),
    )
    .await;

    // Load the asset's name + metadata once: these are the inputs available
    // to in-process steps without object bytes. Missing asset → empty inputs.
    let am = asset_model();
    let am_rel = am.relation.clone();
    let asset_inputs: Option<(String, String, String, String)> = sqlx::query(&format!(
        "SELECT {name}, {metadata}, {file_id}, {project_id} FROM {am_rel} \
         WHERE {asset_id} = $1::UUID AND {tenant_id} = $2::UUID",
        name = am.text_or_empty_as("name", "asset_name"),
        metadata = am.text_or_empty_as("metadata", "asset_metadata"),
        file_id = am.text_or_empty_as("file_id", "file_id"),
        project_id = am.text_or_empty_as("project_id", "project_id"),
        asset_id = am.q("asset_id"),
        tenant_id = am.q("tenant_id"),
    ))
    .bind(asset_id)
    .bind(tenant_id)
    .fetch_optional(pool)
    .await
    .map_err(|err| {
        asset_internal_status(
            "start_pipeline",
            format!("load asset for pipeline failed: {err}"),
        )
    })?
    .map(|row| {
        let name: String = row.try_get("asset_name").unwrap_or_default();
        let metadata: String = row.try_get("asset_metadata").unwrap_or_default();
        let file_id: String = row.try_get("file_id").unwrap_or_default();
        let project_id: String = row.try_get("project_id").unwrap_or_default();
        (name, metadata, file_id, project_id)
    });
    let (asset_name, asset_metadata, asset_file_id, asset_project_id) =
        asset_inputs.unwrap_or_default();
    let asset_metadata = svc.decrypt_native_json_state(&asset_metadata)?;

    // Accumulate the materialized steps so the response can return them
    // inline (mirrors GetPipelineResponse.steps) without a follow-up read.
    let mut response_steps: Vec<asset_entity_pb::PipelineStep> =
        Vec::with_capacity(step_array.len());

    // Materialize each step, RUN it in-process, and record the outcome.
    for el in &step_array {
        let step_name = el.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let step_type_str = el.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let step_type = step_type_to_db(step_type_str, "EMBED").map_err(|e| {
            // Same Status code; only add the stable reason for SDK branching.
            status_with_reason(e, STEP_TYPE_UNSUPPORTED)
        })?;
        let step_type_i32 = step_type_from_db(&step_type);
        let step_id = Uuid::new_v4().to_string();
        // Persist the step's transform params (RESIZE width/height, CONVERT
        // format) so the configuration is a first-class part of the step row
        // and is echoed back on read — not just consumed inline by the byte
        // step. A step with no `params` object stores `{}`.
        let step_params_json = el
            .get("params")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}))
            .to_string();

        // Pure-CPU metadata steps (EMBED/EXTRACT) run via the sync registry.
        // Byte-IO steps (THUMBNAIL/RESIZE/TRANSCODE) fetch the source object,
        // transform, and store a derived object — inherently async, so they
        // take a separate path (still no registry edit to add metadata step types).
        let outcome = if is_byte_step(step_type_i32) {
            svc.run_byte_step(
                pool,
                step_type_i32,
                &asset_file_id,
                tenant_id,
                &asset_project_id,
                &parse_byte_step_params(el),
            )
            .await
        } else {
            step_registry().run(
                step_type_i32,
                &StepContext {
                    asset_name: &asset_name,
                    metadata_json: &asset_metadata,
                },
            )
        };
        let outcome = if step_type == "EMBED" {
            match outcome {
                StepOutcome::Completed(mut value) => {
                    if let Some(target) = svc
                        .upsert_embedding(&asset_project_id, &req.asset_id, &value)
                        .await
                    {
                        if let Some(object) = value.as_object_mut() {
                            object.insert(
                                "vector_backend".to_string(),
                                serde_json::Value::String("qdrant".to_string()),
                            );
                            object.insert(
                                "vector_backend_instance".to_string(),
                                serde_json::Value::String(target.instance),
                            );
                            object.insert(
                                "vector_project_id".to_string(),
                                serde_json::Value::String(target.project_id),
                            );
                        }
                    }
                    StepOutcome::Completed(value)
                }
                other => other,
            }
        } else {
            outcome
        };
        let (status_token, result_json, error_text) = match &outcome {
            StepOutcome::Completed(v) => ("COMPLETED", v.to_string(), String::new()),
            StepOutcome::Failed(msg) => ("FAILED", "{}".to_string(), msg.clone()),
        };
        let result_json = svc.encrypt_native_json_state(&result_json)?;

        sqlx::query(&format!(
            "INSERT INTO {step_rel} \
             ({step_id}, {instance_id}, {tenant_id}, {step_name}, {step_type}, {status}, {result}, {error}, {params}, {completed_at}) \
             VALUES ($1::UUID, $2::UUID, $3::UUID, $4, $5, $6, $7::JSONB, NULLIF($8, ''), $9::JSONB, CURRENT_TIMESTAMP)",
            step_id = step.q("step_id"),
            instance_id = step.q("instance_id"),
            tenant_id = step.q("tenant_id"),
            step_name = step.q("step_name"),
            step_type = step.q("step_type"),
            status = step.q("status"),
            result = step.q("result"),
            error = step.q("error"),
            params = step.q("params"),
            completed_at = step.q("completed_at"),
        ))
        .bind(&step_id)
        .bind(&instance_id)
        .bind(tenant_id)
        .bind(step_name)
        .bind(&step_type)
        .bind(status_token)
        .bind(&result_json)
        .bind(&error_text)
        .bind(&step_params_json)
        .execute(pool)
        .await
        .map_err(|err| {
            crate::runtime::executor_utils::sqlx_error_to_status(
                "create pipeline step failed",
                &err,
            )
        })?;

        // Mirror the persisted row into the response. The plaintext result /
        // error come straight from `outcome` (the same values the row holds,
        // pre-encryption), matching what GetPipeline returns after decrypt.
        let (step_result_plain, step_error_plain) = match &outcome {
            StepOutcome::Completed(v) => (v.to_string(), String::new()),
            StepOutcome::Failed(msg) => ("{}".to_string(), msg.clone()),
        };
        response_steps.push(asset_entity_pb::PipelineStep {
            step_id: step_id.clone(),
            instance_id: instance_id.clone(),
            tenant_id: req.tenant_id.clone(),
            step_name: step_name.to_string(),
            step_type: step_type_i32,
            status: step_status_from_db(status_token),
            result: step_result_plain,
            error: step_error_plain,
            params: step_params_json.clone(),
            ..Default::default()
        });

        emit_payload_event(
            pool,
            svc.outbox_relation.as_deref(),
            PIPELINE_STEP_COMPLETED_TOPIC,
            &instance_id,
            serde_json::json!({
                "instance_id": instance_id,
                "tenant_id": req.tenant_id,
                "step_id": step_id,
                "step_name": step_name,
                "step_type": step_type,
                "status": status_token,
            }),
            Some(&svc.metrics),
        )
        .await;
    }

    // Advance the instance to a terminal state (emits completed/failed).
    let instance_uuid = parse_uuid("instance_id", &instance_id)?;
    advance_instance(svc, pool, instance_uuid, tenant_id).await?;

    Ok(Response::new(asset_pb::StartPipelineResponse {
        instance_id,
        message: "pipeline started".to_string(),
        error: None,
        steps: response_steps,
    }))
}

pub(crate) async fn get_pipeline(
    svc: &AssetServiceImpl,
    request: Request<asset_pb::GetPipelineRequest>,
) -> Result<Response<asset_pb::GetPipelineResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    // Per-tenant fair admission (lighter Read budget) so reads can't starve the pool.
    let _admit = svc.admit_read(&req.tenant_id).await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
    let instance_id = parse_uuid("instance_id", &req.instance_id)?;
    let pool = svc.require_pool()?;
    let inst = pipeline_instance_model();
    let inst_rel = inst.relation.clone();
    let inst_projection = pipeline_instance_select_projection(&inst);
    let row = sqlx::query(&format!(
        "SELECT {inst_projection} FROM {inst_rel} \
         WHERE {instance_id} = $1::UUID AND {tenant_id} = $2::UUID",
        instance_id = inst.q("instance_id"),
        tenant_id = inst.q("tenant_id"),
    ))
    .bind(instance_id)
    .bind(tenant_id)
    .fetch_optional(pool)
    .await
    .map_err(|err| asset_internal_status("get_pipeline", format!("get pipeline failed: {err}")))?;
    let instance = match row {
        Some(row) => {
            let mut instance = pipeline_instance_from_row(&row)?;
            instance.context = svc.decrypt_native_json_state(&instance.context)?;
            Some(instance)
        }
        None => {
            return Err(asset_schema_not_found_status(
                "get_pipeline",
                "pipeline_instance_not_found",
                "pipeline instance not found",
            ));
        }
    };

    let step = pipeline_step_model();
    let step_rel = step.relation.clone();
    let step_projection = pipeline_step_select_projection(&step);
    let step_rows = sqlx::query(&format!(
        "SELECT {step_projection} FROM {step_rel} \
         WHERE {instance_id} = $1::UUID AND {tenant_id} = $2::UUID ORDER BY {step_name}",
        instance_id = step.q("instance_id"),
        tenant_id = step.q("tenant_id"),
        step_name = step.q("step_name"),
    ))
    .bind(instance_id)
    .bind(tenant_id)
    .fetch_all(pool)
    .await
    .map_err(|err| {
        asset_internal_status("get_pipeline", format!("get pipeline steps failed: {err}"))
    })?;
    let mut steps = Vec::with_capacity(step_rows.len());
    for r in &step_rows {
        let mut step = pipeline_step_from_row(r)?;
        step.result = svc.decrypt_native_json_state(&step.result)?;
        steps.push(step);
    }

    Ok(Response::new(asset_pb::GetPipelineResponse {
        instance,
        steps,
        error: None,
    }))
}

pub(crate) async fn complete_step(
    svc: &AssetServiceImpl,
    request: Request<asset_pb::CompleteStepRequest>,
) -> Result<Response<asset_pb::CompleteStepResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    // Per-tenant fair admission (held for the whole RPC) — completing a step
    // can trigger the next step + vector upserts, so it's gated per tenant.
    let _admit = svc.admit(&req.tenant_id, "").await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
    let step_id = parse_uuid("step_id", &req.step_id)?;
    let pool = svc.require_pool()?;
    let step = pipeline_step_model();
    let step_rel = step.relation.clone();
    let status = step_status_to_db(&req.status, "COMPLETED")?;
    let result_json = if req.result.trim().is_empty() {
        String::new()
    } else {
        svc.encrypt_native_json_state(req.result.trim())?
    };

    let result = sqlx::query(&format!(
        "UPDATE {step_rel} SET \
           {status} = $3, \
           {result} = CASE WHEN $4 = '' THEN {result} ELSE $4::JSONB END, \
           {error} = NULLIF($5, ''), \
           {completed_at} = CURRENT_TIMESTAMP \
         WHERE {step_id} = $1::UUID AND {tenant_id} = $2::UUID",
        status = step.q("status"),
        result = step.q("result"),
        error = step.q("error"),
        completed_at = step.q("completed_at"),
        step_id = step.q("step_id"),
        tenant_id = step.q("tenant_id"),
    ))
    .bind(step_id)
    .bind(tenant_id)
    .bind(&status)
    .bind(&result_json)
    .bind(req.error_message.trim())
    .execute(pool)
    .await
    .map_err(|err| {
        asset_internal_status("complete_step", format!("complete step failed: {err}"))
    })?;
    if result.rows_affected() == 0 {
        return Err(asset_schema_not_found_status(
            "complete_step",
            "pipeline_step_not_found",
            "pipeline step not found",
        ));
    }

    // Resolve the owning instance for advance.
    let instance_id: Uuid = sqlx::query_scalar(&format!(
        "SELECT {instance_id} FROM {step_rel} \
         WHERE {step_id} = $1::UUID AND {tenant_id} = $2::UUID",
        instance_id = step.q("instance_id"),
        step_id = step.q("step_id"),
        tenant_id = step.q("tenant_id"),
    ))
    .bind(step_id)
    .bind(tenant_id)
    .fetch_one(pool)
    .await
    .map_err(|err| {
        asset_internal_status(
            "complete_step",
            format!("resolve step instance failed: {err}"),
        )
    })?;

    // Per-step completion event (externally-driven step).
    emit_payload_event(
        pool,
        svc.outbox_relation.as_deref(),
        PIPELINE_STEP_COMPLETED_TOPIC,
        &instance_id.to_string(),
        serde_json::json!({
            "instance_id": instance_id.to_string(),
            "tenant_id": req.tenant_id,
            "step_id": req.step_id,
            "status": status,
        }),
        Some(&svc.metrics),
    )
    .await;

    // Roll the instance to a terminal state when all steps are accounted for,
    // or to FAILED on any failed step (shared with start_pipeline).
    advance_instance(svc, pool, instance_id, tenant_id).await?;

    Ok(Response::new(asset_pb::CompleteStepResponse {
        message: "step completed".to_string(),
        error: None,
    }))
}

pub(crate) async fn list_assets(
    svc: &AssetServiceImpl,
    request: Request<asset_pb::ListAssetsRequest>,
) -> Result<Response<asset_pb::ListAssetsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    // Per-tenant fair admission (lighter Read budget) so list scans can't starve the pool.
    let _admit = svc.admit_read(&req.tenant_id).await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
    let m = asset_model();
    let rel = m.relation.clone();
    let media_filter = req.media_type.trim().to_string();
    let status_filter = asset_status_to_db(&req.status, "")?;
    let page_window = native_offset_page_window(req.page, req.page_size, &req.page_token, 50);
    let pool = svc.require_pool()?;
    let where_clause = format!(
        "WHERE {tenant_id} = $1::UUID AND {deleted} IS NULL \
         AND ($2 = '' OR {media_type} = $2) AND ($3 = '' OR {status} = $3)",
        tenant_id = m.q("tenant_id"),
        deleted = m.q("deleted_at"),
        media_type = m.q("media_type"),
        status = m.q("status"),
    );
    let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {rel} {where_clause}"))
        .bind(tenant_id)
        .bind(&media_filter)
        .bind(&status_filter)
        .fetch_one(pool)
        .await
        .map_err(|err| {
            asset_internal_status("list_assets", format!("count assets failed: {err}"))
        })?;
    let context = native_service_context(&metadata, &req.tenant_id, "");
    let rows = svc
        .require_runtime()?
        .native_entity_read_for_service(
            "asset",
            &context,
            asset_read(
                &tenant_id.to_string(),
                None,
                Some(&media_filter),
                Some(&status_filter),
                page_window.offset as u64,
                page_window.limit as u32,
            ),
        )
        .await?;
    let mut assets = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut asset = asset_from_json(row);
        asset.metadata = svc.decrypt_native_json_state(&asset.metadata)?;
        assets.push(asset);
    }
    Ok(Response::new(asset_pb::ListAssetsResponse {
        assets,
        total_count: total as i32,
        error: None,
        next_page_token: native_next_page_token_for_total(
            page_window.offset,
            page_window.limit,
            total,
        ),
    }))
}

pub(crate) async fn get_asset(
    svc: &AssetServiceImpl,
    request: Request<asset_pb::GetAssetRequest>,
) -> Result<Response<asset_pb::GetAssetResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    // Per-tenant fair admission (lighter Read budget) so reads can't starve the pool.
    let _admit = svc.admit_read(&req.tenant_id).await?;
    let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
    let asset_id = parse_uuid("asset_id", &req.asset_id)?;
    let context = native_service_context(&metadata, &req.tenant_id, "");
    let rows = svc
        .require_runtime()?
        .native_entity_read_for_service(
            "asset",
            &context,
            asset_read(
                &tenant_id.to_string(),
                Some(&asset_id.to_string()),
                None,
                None,
                0,
                1,
            ),
        )
        .await?;
    let asset = match rows.first() {
        Some(row) => {
            let mut asset = asset_from_json(row);
            asset.metadata = svc.decrypt_native_json_state(&asset.metadata)?;
            Some(asset)
        }
        None => {
            return Err(asset_schema_not_found_status(
                "get_asset",
                "asset_not_found",
                "asset not found",
            ));
        }
    };
    Ok(Response::new(asset_pb::GetAssetResponse {
        asset,
        error: None,
    }))
}
