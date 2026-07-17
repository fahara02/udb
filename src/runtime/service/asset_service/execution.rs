//! Pipeline execution + auto-trigger orchestration for the native `AssetService`:
//! the CDC/Kafka-trigger entry points (`handle_storage_finalized` /
//! `handle_trigger_event`), the shared file-resolve + asset-register/start tail,
//! the tenant-bound object-key resolver, the async byte-IO step runner
//! (`run_byte_step`), and the terminal-state advance. Extracted verbatim from the
//! former god file; the self-using orchestration stays inherent on
//! `AssetServiceImpl`, and `advance_instance` stays a free fn taking `svc`.

use sqlx::{PgPool, Row};
use tonic::{Request, Status};
use uuid::Uuid;

use crate::proto::udb::core::asset::entity::v1 as asset_entity_pb;
use crate::proto::udb::core::asset::services::v1 as asset_pb;
use crate::proto::udb::core::asset::services::v1::asset_service_server::AssetService;
use crate::runtime::native_catalog::native_model;
use crate::runtime::service::native_helpers::{
    emit_payload_event, parse_uuid, storage_object_defaults,
};

use super::AssetServiceImpl;
use super::config::{PIPELINE_COMPLETED_TOPIC, PIPELINE_FAILED_TOPIC};
use super::errors::asset_internal_status;
use super::model::{
    asset_model, pipeline_definition_model, pipeline_instance_model, pipeline_step_model,
};
#[cfg(feature = "asset-image")]
use super::steps::image::{
    apply_image_transform, check_image_pixels, check_input_bytes, resolve_output_format,
};
use super::steps::transcode::run_ffmpeg_transcode;
use super::steps::{ByteStepParams, StepOutcome, derived_object_key, register_derived_file};

/// Resolved metadata for a finalized storage file that an asset pipeline acts on.
/// Shared by the storage-finalized and Kafka-trigger handlers so both derive the
/// asset's name/project/media_type identically.
pub(crate) struct FinalizedFile {
    filename: String,
    project_id: String,
    media_type: String,
}

impl AssetServiceImpl {
    /// CDC trigger handler: on a finalized storage file
    /// (`udb.storage.file.finalized.v1`), auto-register the asset and start the
    /// tenant's active pipeline whose `media_type` matches the file's content type.
    /// Idempotent: the asset is reused per `file_id`, and the pipeline is deduped on
    /// `correlation_id = file_id`. Returns the started instance id, or `None` when
    /// the file is gone or no matching active pipeline definition exists (no-op).
    pub(crate) async fn handle_storage_finalized(
        &self,
        file_id: &str,
        tenant_id: &str,
    ) -> Result<Option<String>, Status> {
        let pool = self.require_pool()?;
        let tenant_uuid = parse_uuid("tenant_id", tenant_id)?;
        let file_uuid = parse_uuid("file_id", file_id)?;

        let Some(file) = self
            .resolve_finalized_file(pool, tenant_uuid, file_uuid)
            .await?
        else {
            return Ok(None);
        };

        // Match an active pipeline definition for this tenant + media type.
        let dm = pipeline_definition_model();
        let def_id: Option<String> = sqlx::query_scalar(&format!(
            "SELECT {did}::TEXT FROM {rel} \
             WHERE {tid} = $1::UUID AND {mt} = $2 AND {status} = 'ACTIVE' \
             ORDER BY {ver} DESC LIMIT 1",
            did = dm.q("definition_id"),
            rel = dm.relation,
            tid = dm.q("tenant_id"),
            mt = dm.q("media_type"),
            status = dm.q("status"),
            ver = dm.q("version"),
        ))
        .bind(tenant_uuid)
        .bind(&file.media_type)
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            asset_internal_status(
                "handle_storage_finalized",
                format!("match pipeline definition failed: {e}"),
            )
        })?;
        let Some(definition_id) = def_id else {
            return Ok(None);
        };

        let instance_id = self
            .start_pipeline_for_file(
                tenant_id,
                file_id,
                file_uuid,
                tenant_uuid,
                definition_id,
                &file,
            )
            .await?;
        Ok(Some(instance_id))
    }

    /// Kafka-trigger handler (master-plan 5.2): start the most recent active
    /// pipeline definition whose `trigger_topic` matches `topic` for the file's
    /// tenant. Mirrors [`Self::handle_storage_finalized`] but selects definitions
    /// by `trigger_topic` instead of media_type, sharing the same file-resolve and
    /// asset-register/start path. Idempotent on `correlation_id = file_id`, so the
    /// trigger consumer's at-least-once redelivery is safe. Returns the started
    /// instance id, or `None` when no file or no matching definition is found.
    // Reached from the trigger consumer, which serve() wires via
    // `spawn_trigger_manager` (master-plan 5.2); allow keeps the build clean until then.
    #[allow(dead_code)]
    pub(crate) async fn handle_trigger_event(
        &self,
        topic: &str,
        file_id: &str,
        tenant_id: &str,
    ) -> Result<Option<String>, Status> {
        let pool = self.require_pool()?;
        let tenant_uuid = parse_uuid("tenant_id", tenant_id)?;
        let file_uuid = parse_uuid("file_id", file_id)?;

        let Some(file) = self
            .resolve_finalized_file(pool, tenant_uuid, file_uuid)
            .await?
        else {
            return Ok(None);
        };

        // Match an active pipeline definition for this tenant + trigger_topic.
        let dm = pipeline_definition_model();
        let def_id: Option<String> = sqlx::query_scalar(&format!(
            "SELECT {did}::TEXT FROM {rel} \
             WHERE {tid} = $1::UUID AND {tt} = $2 AND {status} = 'ACTIVE' \
             ORDER BY {ver} DESC LIMIT 1",
            did = dm.q("definition_id"),
            rel = dm.relation,
            tid = dm.q("tenant_id"),
            tt = dm.q("trigger_topic"),
            status = dm.q("status"),
            ver = dm.q("version"),
        ))
        .bind(tenant_uuid)
        .bind(topic)
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            asset_internal_status(
                "handle_trigger_event",
                format!("match pipeline definition by trigger_topic failed: {e}"),
            )
        })?;
        let Some(definition_id) = def_id else {
            return Ok(None);
        };

        let instance_id = self
            .start_pipeline_for_file(
                tenant_id,
                file_id,
                file_uuid,
                tenant_uuid,
                definition_id,
                &file,
            )
            .await?;
        Ok(Some(instance_id))
    }

    /// Tenant-bound resolve of a finalized storage file's metadata (proto-driven).
    /// Only acts on a file owned by `tenant_uuid`; returns `None` when absent.
    /// Shared by [`Self::handle_storage_finalized`] and [`Self::handle_trigger_event`].
    async fn resolve_finalized_file(
        &self,
        pool: &PgPool,
        tenant_uuid: Uuid,
        file_uuid: Uuid,
    ) -> Result<Option<FinalizedFile>, Status> {
        let fm = native_model(
            "udb.core.storage.entity.v1.File",
            &["file_id", "content_type", "filename"],
        );
        let frow = sqlx::query(&format!(
            "SELECT {ct}, {fname}, {project_id} FROM {rel} \
             WHERE {fid} = $1::UUID AND {tid} = $2::UUID AND {del} IS NULL",
            ct = fm.text_or_empty_as("content_type", "content_type"),
            fname = fm.text_or_empty_as("filename", "filename"),
            project_id = fm.text_or_empty_as("project_id", "project_id"),
            rel = fm.relation,
            fid = fm.q("file_id"),
            tid = fm.q("tenant_id"),
            del = fm.q("deleted_at"),
        ))
        .bind(file_uuid)
        .bind(tenant_uuid)
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            asset_internal_status(
                "resolve_finalized_file",
                format!("resolve finalized file failed: {e}"),
            )
        })?;
        let Some(frow) = frow else {
            return Ok(None);
        };
        let content_type: String = frow.try_get("content_type").unwrap_or_default();
        let filename: String = frow.try_get("filename").unwrap_or_default();
        let project_id: String = frow.try_get("project_id").unwrap_or_default();
        // image/png → "image"; falls back to the whole string if no slash.
        let media_type = content_type
            .split('/')
            .next()
            .unwrap_or("")
            .trim()
            .to_string();
        Ok(Some(FinalizedFile {
            filename,
            project_id,
            media_type,
        }))
    }

    /// Reuse an existing asset for `file_id`, else register one, then start
    /// `definition_id`'s pipeline (idempotent on `correlation_id = file_id`).
    /// Returns the pipeline instance id. Shared trigger/finalize tail.
    async fn start_pipeline_for_file(
        &self,
        tenant_id: &str,
        file_id: &str,
        file_uuid: Uuid,
        tenant_uuid: Uuid,
        definition_id: String,
        file: &FinalizedFile,
    ) -> Result<String, Status> {
        let pool = self.require_pool()?;
        let am = asset_model();
        let existing: Option<String> = sqlx::query_scalar(&format!(
            "SELECT {aid}::TEXT FROM {rel} \
             WHERE {fid} = $1::UUID AND {tid} = $2::UUID AND {del} IS NULL LIMIT 1",
            aid = am.q("asset_id"),
            rel = am.relation,
            fid = am.q("file_id"),
            tid = am.q("tenant_id"),
            del = am.q("deleted_at"),
        ))
        .bind(file_uuid)
        .bind(tenant_uuid)
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            asset_internal_status(
                "start_pipeline_for_file",
                format!("lookup asset for file failed: {e}"),
            )
        })?;
        let asset_id = match existing {
            Some(a) => a,
            None => {
                self.register_asset(Request::new(asset_pb::RegisterAssetRequest {
                    tenant_id: tenant_id.to_string(),
                    project_id: file.project_id.clone(),
                    file_id: file_id.to_string(),
                    name: if file.filename.is_empty() {
                        file_id.to_string()
                    } else {
                        file.filename.clone()
                    },
                    media_type: file.media_type.clone(),
                    ..Default::default()
                }))
                .await?
                .into_inner()
                .asset_id
            }
        };

        // Start the pipeline, idempotent on correlation_id = file_id.
        let started = self
            .start_pipeline(Request::new(asset_pb::StartPipelineRequest {
                tenant_id: tenant_id.to_string(),
                definition_id,
                asset_id,
                correlation_id: file_id.to_string(),
                ..Default::default()
            }))
            .await?
            .into_inner();
        Ok(started.instance_id)
    }

    /// Resolve a storage file's `object_key` (UDB-owned `udb_storage.files`),
    /// **tenant-bound** so a byte step can only read a file owned by its tenant.
    /// Proto-driven via the embedded manifest — no hardcoded table/columns.
    pub(crate) async fn resolve_object_key(
        &self,
        pool: &PgPool,
        file_id: Uuid,
        tenant_id: Uuid,
    ) -> Option<String> {
        let m = native_model(
            "udb.core.storage.entity.v1.File",
            &["file_id", "object_key"],
        );
        let rel = m.relation.clone();
        sqlx::query_scalar::<_, String>(&format!(
            "SELECT {ok}::TEXT FROM {rel} \
             WHERE {fid} = $1::UUID AND {tid} = $2::UUID AND {del} IS NULL",
            ok = m.q("object_key"),
            fid = m.q("file_id"),
            tid = m.q("tenant_id"),
            del = m.q("deleted_at"),
        ))
        .bind(file_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
    }

    /// Run a byte-IO step: fetch the source object bytes, transform them per the
    /// step's [`ByteStepParams`], store the derived object under the `derived/`
    /// namespace, and register it as a `udb_storage.files` row. Image processing
    /// is behind the `asset-image` feature; TRANSCODE uses the ffmpeg executor.
    /// Without the required executor the step fails explicitly (no fake success).
    /// Source bytes/derived objects use the same object backend+bucket as the storage service
    /// (`UDB_STORAGE_OBJECT_BACKEND` / `UDB_STORAGE_BUCKET`).
    pub(crate) async fn run_byte_step(
        &self,
        pool: &PgPool,
        step_type_i32: i32,
        file_id_str: &str,
        tenant_id: Uuid,
        project_id: &str,
        params: &ByteStepParams,
    ) -> StepOutcome {
        let Some(runtime) = self.runtime.as_ref() else {
            return StepOutcome::Failed(
                "byte steps require a runtime object handle (none configured)".to_string(),
            );
        };
        let Ok(file_id) = Uuid::parse_str(file_id_str.trim()) else {
            return StepOutcome::Failed("asset has no valid file_id for a byte step".to_string());
        };
        let Some(object_key) = self.resolve_object_key(pool, file_id, tenant_id).await else {
            return StepOutcome::Failed("source file not found for tenant".to_string());
        };
        let (backend, bucket) = storage_object_defaults(
            std::env::var("UDB_STORAGE_OBJECT_BACKEND").ok(),
            std::env::var("UDB_STORAGE_BUCKET").ok(),
        );
        let step_type = asset_entity_pb::StepType::try_from(step_type_i32)
            .unwrap_or(asset_entity_pb::StepType::Unspecified);

        if matches!(step_type, asset_entity_pb::StepType::Transcode) {
            let get_req = crate::runtime::core::setup_data::object_request_json(
                "get",
                &bucket,
                &object_key,
                "",
            );
            let bytes = match runtime
                .get_object_backend_target_for_project(&backend, None, project_id, &get_req)
                .await
            {
                Ok(b) => b,
                Err(err) => {
                    return StepOutcome::Failed(format!("fetch source bytes failed: {err}"));
                }
            };
            let (out_bytes, content_type, ext) = match run_ffmpeg_transcode(&bytes, params).await {
                Ok(output) => output,
                Err(reason) => return StepOutcome::Failed(reason),
            };
            let out_len = out_bytes.len();
            let derived_key = derived_object_key(&object_key, step_type, ext);
            let put_req = crate::runtime::core::setup_data::object_request_json(
                "put",
                &bucket,
                &derived_key,
                content_type,
            );
            if let Err(err) = runtime
                .put_object_backend_target_for_project(
                    &backend, None, project_id, &put_req, out_bytes,
                )
                .await
            {
                return StepOutcome::Failed(format!("store derived object failed: {err}"));
            }
            if let Err(err) = register_derived_file(
                pool,
                tenant_id,
                &derived_key,
                &backend,
                &bucket,
                content_type,
                "VIDEO",
                out_len as i64,
            )
            .await
            {
                return StepOutcome::Failed(format!("register derived object failed: {err}"));
            }
            return StepOutcome::Completed(serde_json::json!({
                "derived_object_key": derived_key,
                "format": ext,
                "content_type": content_type,
                "bytes": out_len,
            }));
        }

        #[cfg(not(feature = "asset-image"))]
        {
            let _ = (
                runtime,
                step_type_i32,
                &object_key,
                &backend,
                &bucket,
                project_id,
                params,
            );
            StepOutcome::Failed(
                "THUMBNAIL/RESIZE require the `asset-image` feature build".to_string(),
            )
        }
        #[cfg(feature = "asset-image")]
        {
            use asset_entity_pb::StepType as T;
            let step_type = T::try_from(step_type_i32).unwrap_or(T::Unspecified);

            let get_req = crate::runtime::core::setup_data::object_request_json(
                "get",
                &bucket,
                &object_key,
                "",
            );
            let bytes = match runtime
                .get_object_backend_target_for_project(&backend, None, project_id, &get_req)
                .await
            {
                Ok(b) => b,
                Err(err) => {
                    return StepOutcome::Failed(format!("fetch source bytes failed: {err}"));
                }
            };

            // (1) byte cap BEFORE decode — bound memory before the decoder allocates.
            if let Err(reason) = check_input_bytes(bytes.len() as u64) {
                return StepOutcome::Failed(reason);
            }
            // (2) header pixel cap BEFORE full decode — probe dimensions only, so a
            //     pixel-flood decompression bomb is rejected pre-decode.
            let probe = match image::ImageReader::new(std::io::Cursor::new(bytes.as_slice()))
                .with_guessed_format()
            {
                Ok(reader) => reader.into_dimensions(),
                Err(err) => {
                    return StepOutcome::Failed(format!("probe image header failed: {err}"));
                }
            };
            let (src_w, src_h) = match probe {
                Ok(dims) => dims,
                Err(err) => {
                    return StepOutcome::Failed(format!("probe image dimensions failed: {err}"));
                }
            };
            if let Err(reason) = check_image_pixels(src_w, src_h) {
                return StepOutcome::Failed(reason);
            }

            // (3) full decode — now bounded by the two checks above.
            let img = match image::load_from_memory(&bytes) {
                Ok(i) => i,
                Err(err) => return StepOutcome::Failed(format!("decode image failed: {err}")),
            };

            // (4) transform per step type + requested params.
            let (out_format, content_type, ext) =
                match resolve_output_format(params.format.as_deref(), image::ImageFormat::Png) {
                    Ok(triple) => triple,
                    Err(reason) => return StepOutcome::Failed(reason),
                };
            let transformed = match apply_image_transform(img, step_type, params) {
                Ok(t) => t,
                Err(reason) => return StepOutcome::Failed(reason),
            };

            // (5) encode.
            let mut out = std::io::Cursor::new(Vec::new());
            if let Err(err) = transformed.write_to(&mut out, out_format) {
                return StepOutcome::Failed(format!("encode derived image failed: {err}"));
            }
            let out_bytes = out.into_inner();
            let out_len = out_bytes.len();
            let (out_w, out_h) = (transformed.width(), transformed.height());

            // (6) store under the `derived/` namespace (no source-key collision).
            let derived_key = derived_object_key(&object_key, step_type, ext);
            let put_req = crate::runtime::core::setup_data::object_request_json(
                "put",
                &bucket,
                &derived_key,
                content_type,
            );
            if let Err(err) = runtime
                .put_object_backend_target_for_project(
                    &backend, None, project_id, &put_req, out_bytes,
                )
                .await
            {
                return StepOutcome::Failed(format!("store derived object failed: {err}"));
            }

            // (7) register the derived object as a tracked `udb_storage.files` row.
            if let Err(err) = register_derived_file(
                pool,
                tenant_id,
                &derived_key,
                &backend,
                &bucket,
                content_type,
                "IMAGE",
                out_len as i64,
            )
            .await
            {
                return StepOutcome::Failed(format!("register derived object failed: {err}"));
            }

            StepOutcome::Completed(serde_json::json!({
                "derived_object_key": derived_key,
                "width": out_w,
                "height": out_h,
                "format": ext,
                "bytes": out_len,
            }))
        }
    }
}

/// Roll a pipeline instance to a terminal state when all steps are accounted for,
/// or to FAILED on any failed step. Shared by `start_pipeline` (inline execution)
/// and `complete_step` (externally-driven). Emits the terminal domain event.
/// Returns the terminal status token (`"COMPLETED"`/`"FAILED"`) if one was set.
pub(crate) async fn advance_instance(
    svc: &AssetServiceImpl,
    pool: &PgPool,
    instance_id: Uuid,
    tenant_id: Uuid,
) -> Result<Option<&'static str>, Status> {
    let step = pipeline_step_model();
    let step_rel = step.relation.clone();
    let counts = sqlx::query(&format!(
        "SELECT \
           COUNT(*) AS total, \
           COUNT(*) FILTER (WHERE {status} IN ('COMPLETED', 'SKIPPED')) AS done, \
           COUNT(*) FILTER (WHERE {status} = 'FAILED') AS failed \
         FROM {step_rel} WHERE {instance_id} = $1::UUID AND {tenant_id} = $2::UUID",
        status = step.q("status"),
        instance_id = step.q("instance_id"),
        tenant_id = step.q("tenant_id"),
    ))
    .bind(instance_id)
    .bind(tenant_id)
    .fetch_one(pool)
    .await
    .map_err(|err| {
        asset_internal_status(
            "advance_pipeline_instance",
            format!("aggregate step status failed: {err}"),
        )
    })?;
    let total: i64 = counts.try_get("total").map_err(|e| {
        asset_internal_status(
            "advance_pipeline_instance",
            format!("decode total failed: {e}"),
        )
    })?;
    let done: i64 = counts.try_get("done").map_err(|e| {
        asset_internal_status(
            "advance_pipeline_instance",
            format!("decode done failed: {e}"),
        )
    })?;
    let failed: i64 = counts.try_get("failed").map_err(|e| {
        asset_internal_status(
            "advance_pipeline_instance",
            format!("decode failed failed: {e}"),
        )
    })?;

    let new_instance_status = if failed > 0 {
        Some("FAILED")
    } else if total > 0 && done == total {
        Some("COMPLETED")
    } else {
        None
    };
    if let Some(terminal) = new_instance_status {
        let inst = pipeline_instance_model();
        let inst_rel = inst.relation.clone();
        sqlx::query(&format!(
            "UPDATE {inst_rel} SET {status} = $3, {completed_at} = CURRENT_TIMESTAMP \
             WHERE {instance_id} = $1::UUID AND {tenant_id} = $2::UUID",
            status = inst.q("status"),
            completed_at = inst.q("completed_at"),
            instance_id = inst.q("instance_id"),
            tenant_id = inst.q("tenant_id"),
        ))
        .bind(instance_id)
        .bind(tenant_id)
        .bind(terminal)
        .execute(pool)
        .await
        .map_err(|err| {
            asset_internal_status(
                "advance_pipeline_instance",
                format!("advance pipeline instance failed: {err}"),
            )
        })?;

        let topic = if terminal == "FAILED" {
            PIPELINE_FAILED_TOPIC
        } else {
            PIPELINE_COMPLETED_TOPIC
        };
        emit_payload_event(
            pool,
            svc.outbox_relation.as_deref(),
            topic,
            &instance_id.to_string(),
            serde_json::json!({
                "instance_id": instance_id.to_string(),
                "tenant_id": tenant_id.to_string(),
                "status": terminal,
            }),
            Some(&svc.metrics),
        )
        .await;

        // On failure, remove the asset's embedding so a failed run leaves no
        // orphan vector behind (best-effort).
        if terminal == "FAILED"
            && let Ok(Some(row)) = sqlx::query(&format!(
                "SELECT i.{asset_id}::TEXT AS asset_id, COALESCE(a.{project_id}::TEXT, '') AS project_id, \
                        COALESCE(s.{result}::TEXT, '{{}}') AS vector_result \
                 FROM {inst_rel} i \
                 LEFT JOIN {asset_rel} a ON a.{asset_pk} = i.{asset_id} AND a.{asset_tenant} = i.{tenant_id} \
                 LEFT JOIN LATERAL ( \
                    SELECT {step_result} \
                    FROM {step_rel} \
                    WHERE {step_instance_id} = i.{instance_id} \
                      AND {step_tenant_id} = i.{tenant_id} \
                      AND {step_type} = 'EMBED' \
                      AND {step_status} = 'COMPLETED' \
                    ORDER BY {step_completed_at} DESC NULLS LAST \
                    LIMIT 1 \
                 ) s ON TRUE \
                 WHERE i.{instance_id} = $1::UUID AND i.{tenant_id} = $2::UUID",
                asset_id = inst.q("asset_id"),
                asset_rel = asset_model().relation,
                asset_pk = asset_model().q("asset_id"),
                asset_tenant = asset_model().q("tenant_id"),
                project_id = asset_model().q("project_id"),
                result = step.q("result"),
                step_result = step.q("result"),
                step_rel = step.relation,
                step_instance_id = step.q("instance_id"),
                step_tenant_id = step.q("tenant_id"),
                step_type = step.q("step_type"),
                step_status = step.q("status"),
                step_completed_at = step.q("completed_at"),
                instance_id = inst.q("instance_id"),
                tenant_id = inst.q("tenant_id"),
            ))
            .bind(instance_id)
            .bind(tenant_id)
            .fetch_optional(pool)
            .await
        {
            if let Ok(asset_id) = row.try_get::<String, _>("asset_id") {
                let fallback_project = row.try_get::<String, _>("project_id").unwrap_or_default();
                let vector_result = row
                    .try_get::<String, _>("vector_result")
                    .unwrap_or_else(|_| "{}".to_string());
                let decoded = svc
                    .decrypt_native_json_state(&vector_result)
                    .unwrap_or(vector_result);
                let vector_target = serde_json::from_str::<serde_json::Value>(&decoded).ok();
                let vector_project = vector_target
                    .as_ref()
                    .and_then(|value| value.get("vector_project_id"))
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or(&fallback_project);
                let vector_instance = vector_target
                    .as_ref()
                    .and_then(|value| value.get("vector_backend_instance"))
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.trim().is_empty());
                svc.delete_embedding(vector_project, vector_instance, &asset_id)
                    .await;
            }
        }
    }
    Ok(new_instance_status)
}
