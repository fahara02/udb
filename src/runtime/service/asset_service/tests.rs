//! Unit tests for the native `AssetService`, extracted verbatim from the former
//! god file. Each nested module imports exactly the symbols it exercises (no
//! `use super::*`) and, where it drives an RPC, the generated server trait
//! `asset_service_server::AssetService`.

#[cfg(test)]
mod step_executor_tests {
    use super::super::steps::{StepContext, StepOutcome, StepRegistry, is_byte_step};
    use crate::proto::udb::core::asset::entity::v1::StepType as T;

    #[test]
    fn registry_dispatches_embed_extract_and_fails_unknown_metadata_step() {
        let registry = StepRegistry::default_registry();
        let ctx = StepContext {
            asset_name: "report",
            metadata_json: "{\"k\":\"v\"}",
        };

        match registry.run(T::Embed as i32, &ctx) {
            StepOutcome::Completed(v) => {
                assert!(
                    v.get("embedding").and_then(|e| e.as_array()).is_some(),
                    "EMBED must produce an `embedding` array, got {v}"
                );
            }
            StepOutcome::Failed(msg) => panic!("EMBED should complete, failed with: {msg}"),
        }

        assert!(
            matches!(
                registry.run(T::Extract as i32, &ctx),
                StepOutcome::Completed(_)
            ),
            "EXTRACT should complete"
        );

        match registry.run(T::Caption as i32, &ctx) {
            StepOutcome::Failed(msg) => {
                assert!(
                    msg.contains("not yet implemented"),
                    "CAPTION failure message should explain it is unimplemented, got: {msg}"
                );
            }
            StepOutcome::Completed(_) => panic!("CAPTION must fail (no capability lie)"),
        }
    }

    #[test]
    fn transcode_is_async_byte_step_not_registry_step() {
        assert!(is_byte_step(T::Transcode as i32));
        assert!(!is_byte_step(T::Caption as i32));
    }
}

#[cfg(test)]
mod byte_step_param_tests {
    use super::super::steps::parse_byte_step_params;
    use super::super::steps::transcode::{
        MAX_TRANSCODE_INPUT_BYTES, MAX_TRANSCODE_OUTPUT_BYTES, check_transcode_input_bytes,
        check_transcode_output_bytes, ffmpeg_exe_name, ffmpeg_platform_dir,
        resolve_transcode_output_format, vendored_ffmpeg_candidates, vendored_ffmpeg_path,
    };
    use std::path::Path;

    #[test]
    fn parses_params_subobject_and_flattened_keys() {
        let nested = serde_json::json!({
            "type": "RESIZE",
            "params": { "width": 800, "height": 600, "format": "JPEG" }
        });
        let p = parse_byte_step_params(&nested);
        assert_eq!(p.width, Some(800));
        assert_eq!(p.height, Some(600));
        assert_eq!(p.format.as_deref(), Some("jpeg"), "format is lower-cased");

        // Flattened top-level keys (and string-encoded numbers) also resolve.
        let flat = serde_json::json!({ "type": "RESIZE", "width": "1024", "format": "png" });
        let p = parse_byte_step_params(&flat);
        assert_eq!(p.width, Some(1024));
        assert_eq!(p.height, None);
        assert_eq!(p.format.as_deref(), Some("png"));

        // Absent/zero/blank values fall back to None (per-step defaults apply).
        let empty = serde_json::json!({ "type": "THUMBNAIL", "width": 0, "format": "  " });
        let p = parse_byte_step_params(&empty);
        assert_eq!(p.width, None);
        assert_eq!(p.format, None);
    }

    #[test]
    fn transcode_format_is_allowlisted() {
        let (_, content_type, ext) =
            resolve_transcode_output_format(None).expect("mp4 is the default");
        assert_eq!(content_type, "video/mp4");
        assert_eq!(ext, "mp4");
        assert!(resolve_transcode_output_format(Some("mp4")).is_ok());
        assert!(resolve_transcode_output_format(Some("webm")).is_err());
    }

    #[test]
    fn transcode_byte_limits_are_bounded() {
        assert!(check_transcode_input_bytes(MAX_TRANSCODE_INPUT_BYTES).is_ok());
        assert!(check_transcode_input_bytes(MAX_TRANSCODE_INPUT_BYTES + 1).is_err());
        assert!(check_transcode_output_bytes(MAX_TRANSCODE_OUTPUT_BYTES).is_ok());
        assert!(check_transcode_output_bytes(MAX_TRANSCODE_OUTPUT_BYTES + 1).is_err());
    }

    #[test]
    fn vendored_ffmpeg_search_contract_uses_platform_layout() {
        let expected_suffix = Path::new("third_party")
            .join("ffmpeg")
            .join("bin")
            .join(ffmpeg_platform_dir())
            .join(ffmpeg_exe_name());
        let candidates = vendored_ffmpeg_candidates();
        assert!(
            !candidates.is_empty(),
            "vendored ffmpeg search must always have at least one candidate"
        );
        assert!(
            candidates
                .iter()
                .any(|path| path.ends_with(&expected_suffix)),
            "search must include the documented third_party/ffmpeg/bin/<platform> layout: {candidates:?}"
        );
        assert!(vendored_ffmpeg_path().ends_with(expected_suffix));
    }
}

/// Pure decode-limit guard tests — exercise the predicates without decoding any
/// real image, proving the byte/pixel caps reject before the full decode.
#[cfg(all(test, feature = "asset-image"))]
mod image_limit_tests {
    use super::super::steps::image::{
        MAX_IMAGE_INPUT_BYTES, MAX_IMAGE_PIXELS, apply_image_transform, check_image_pixels,
        check_input_bytes, resolve_output_format,
    };
    use super::super::steps::{ByteStepParams, DERIVED_OBJECT_PREFIX, derived_object_key};
    use crate::proto::udb::core::asset::entity::v1 as asset_entity_pb;

    #[test]
    fn input_bytes_over_limit_rejected() {
        assert!(check_input_bytes(MAX_IMAGE_INPUT_BYTES + 1).is_err());
    }

    #[test]
    fn input_bytes_at_or_under_limit_ok() {
        assert!(check_input_bytes(MAX_IMAGE_INPUT_BYTES).is_ok());
        assert!(check_input_bytes(0).is_ok());
        assert!(check_input_bytes(1024).is_ok());
    }

    #[test]
    fn pixels_over_limit_rejected() {
        // A 1×(MAX+1) header probe (a classic pixel-flood bomb shape) is rejected.
        assert!(check_image_pixels(1, (MAX_IMAGE_PIXELS + 1) as u32).is_err());
        // 9000×9000 = 81 MP > 64 MP cap.
        assert!(check_image_pixels(9000, 9000).is_err());
    }

    #[test]
    fn pixels_at_or_under_limit_ok() {
        assert!(check_image_pixels(8000, 8000).is_ok()); // 64 MP, exactly the cap
        assert!(check_image_pixels(1920, 1080).is_ok());
        assert!(check_image_pixels(0, 0).is_ok());
    }

    #[test]
    fn output_format_resolves_and_rejects_unknown() {
        assert_eq!(
            resolve_output_format(Some("jpg"), image::ImageFormat::Png)
                .unwrap()
                .2,
            "jpg"
        );
        assert_eq!(
            resolve_output_format(None, image::ImageFormat::Png)
                .unwrap()
                .2,
            "png"
        );
        assert!(resolve_output_format(Some("gif"), image::ImageFormat::Png).is_err());
    }

    #[test]
    fn derived_key_is_namespaced_and_collision_free() {
        let key = derived_object_key(
            "tenant/file/photo.png",
            asset_entity_pb::StepType::Thumbnail,
            "png",
        );
        assert_eq!(key, "derived/tenant/file/photo.png.thumbnail.png");
        assert!(key.starts_with(DERIVED_OBJECT_PREFIX));
        assert_ne!(
            key, "tenant/file/photo.png",
            "must not collide with the source key"
        );
    }

    fn solid_image(w: u32, h: u32) -> image::DynamicImage {
        image::DynamicImage::ImageRgba8(image::RgbaImage::new(w, h))
    }

    #[test]
    fn resize_param_produces_requested_dimensions() {
        // A square source resized into a 4x4 box yields exactly 4x4 (aspect 1:1).
        let out = apply_image_transform(
            solid_image(10, 10),
            asset_entity_pb::StepType::Resize,
            &ByteStepParams {
                width: Some(4),
                height: Some(4),
                format: None,
            },
        )
        .expect("RESIZE with dimensions must succeed");
        assert_eq!((out.width(), out.height()), (4, 4));
    }

    #[test]
    fn convert_only_resize_preserves_dimensions() {
        // RESIZE with a format but no dimensions == CONVERT: keep the source size.
        let out = apply_image_transform(
            solid_image(7, 5),
            asset_entity_pb::StepType::Resize,
            &ByteStepParams {
                width: None,
                height: None,
                format: Some("jpeg".to_string()),
            },
        )
        .expect("format-only RESIZE (CONVERT) must succeed");
        assert_eq!((out.width(), out.height()), (7, 5));
    }

    #[test]
    fn resize_without_dimensions_or_format_fails_explicitly() {
        let err = apply_image_transform(
            solid_image(7, 5),
            asset_entity_pb::StepType::Resize,
            &ByteStepParams::default(),
        )
        .expect_err("RESIZE with neither dimensions nor format must fail explicitly");
        assert!(
            err.contains("RESIZE requires"),
            "explicit reason, got: {err}"
        );
    }

    #[test]
    fn thumbnail_honors_param_edge_within_box() {
        // 100x50 thumbnailed into a 16-box fits within 16x16 (aspect-preserving).
        let out = apply_image_transform(
            solid_image(100, 50),
            asset_entity_pb::StepType::Thumbnail,
            &ByteStepParams {
                width: Some(16),
                height: Some(16),
                format: None,
            },
        )
        .expect("THUMBNAIL must succeed");
        assert!(
            out.width() <= 16 && out.height() <= 16,
            "got {}x{}",
            out.width(),
            out.height()
        );
    }

    #[test]
    fn non_image_byte_step_fails_explicitly() {
        let err = apply_image_transform(
            solid_image(2, 2),
            asset_entity_pb::StepType::Transcode,
            &ByteStepParams::default(),
        )
        .expect_err("a non-image step type must not be silently transformed");
        assert!(
            err.contains("not an image transform"),
            "explicit reason, got: {err}"
        );
    }

    #[test]
    fn unimplemented_convert_format_fails_explicitly() {
        // An unsupported output format (this build links only png/jpeg) fails CLOSED
        // rather than silently re-encoding as the default — no capability lie.
        assert!(resolve_output_format(Some("webp"), image::ImageFormat::Png).is_err());
        assert!(resolve_output_format(Some("tiff"), image::ImageFormat::Png).is_err());
    }
}

#[cfg(test)]
mod tenant_scope_tests {
    use super::super::AssetServiceImpl;
    use super::super::errors::{
        active_storage_file_required_status, asset_capability_status, asset_internal_status,
        asset_schema_not_found_status, native_state_decryption_failed_status,
        native_state_encryption_failed_status,
    };
    use super::super::model::{asset_status_to_db, step_status_to_db, step_type_to_db};
    use super::super::store::logical_json_text;
    use crate::proto::udb::core::asset::services::v1 as asset_pb;
    use crate::proto::udb::core::asset::services::v1::asset_service_server::AssetService;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use tonic::metadata::MetadataValue;
    use tonic::{Request, Status};
    use uuid::Uuid;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_single_field_violation(status: &Status, field: &str, description: &str) {
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_schema_not_found_detail(
        status: &Status,
        operation: &str,
        schema_code: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "asset");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, schema_code);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "asset");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn asset_internal_status_carries_typed_detail() {
        assert_internal_detail(
            &asset_internal_status(
                "start_pipeline",
                "load pipeline definition failed: unavailable",
            ),
            "start_pipeline",
            "load pipeline definition failed: unavailable",
        );
    }

    /// A caller scoped to tenant-a must not read another tenant's asset by putting
    /// a foreign tenant_id in the request BODY; the scope guard rejects this before
    /// any pool/DB access (no Postgres needed).
    #[tokio::test]
    async fn get_asset_rejects_cross_tenant_body() {
        let svc = AssetServiceImpl::new(); // no pool, no channels (admit no-op)
        let mut request = Request::new(asset_pb::GetAssetRequest {
            tenant_id: "tenant-b".to_string(),
            asset_id: "00000000-0000-0000-0000-000000000001".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .get_asset(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn create_pipeline_definition_missing_name_carries_field_violation() {
        let svc = AssetServiceImpl::new(); // no runtime; validation runs first
        let tenant_id = Uuid::new_v4().to_string();
        let mut request = Request::new(asset_pb::CreatePipelineDefinitionRequest {
            tenant_id: tenant_id.clone(),
            name: " ".to_string(),
            steps: "[]".to_string(),
            ..Default::default()
        });
        request.metadata_mut().insert(
            "x-tenant-id",
            MetadataValue::try_from(tenant_id.as_str()).unwrap(),
        );

        let err = svc
            .create_pipeline_definition(request)
            .await
            .expect_err("missing pipeline name must fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "name is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "name");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty pipeline definition name"
        );
    }

    #[tokio::test]
    async fn create_pipeline_definition_invalid_steps_carries_field_violation() {
        let svc = AssetServiceImpl::new(); // no runtime; validation runs first
        let tenant_id = Uuid::new_v4().to_string();
        let mut request = Request::new(asset_pb::CreatePipelineDefinitionRequest {
            tenant_id: tenant_id.clone(),
            name: "resize-images".to_string(),
            steps: "{not-json".to_string(),
            ..Default::default()
        });
        request.metadata_mut().insert(
            "x-tenant-id",
            MetadataValue::try_from(tenant_id.as_str()).unwrap(),
        );

        let err = svc
            .create_pipeline_definition(request)
            .await
            .expect_err("invalid steps JSON must fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().starts_with("steps must be valid JSON:"));
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "steps");
        assert_eq!(detail.field_violations[0].description, "must be valid JSON");
    }

    #[tokio::test]
    async fn register_asset_missing_file_id_carries_field_violation() {
        let svc = AssetServiceImpl::new(); // no pool; validation runs first
        let tenant_id = Uuid::new_v4().to_string();
        let mut request = Request::new(asset_pb::RegisterAssetRequest {
            tenant_id: tenant_id.clone(),
            file_id: " ".to_string(),
            ..Default::default()
        });
        request.metadata_mut().insert(
            "x-tenant-id",
            MetadataValue::try_from(tenant_id.as_str()).unwrap(),
        );

        let err = svc
            .register_asset(request)
            .await
            .expect_err("missing file id must fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "file_id is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "file_id");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty storage file id"
        );
    }

    #[test]
    fn asset_helper_validation_carries_field_violations() {
        let json = logical_json_text("{not-json").expect_err("invalid JSON must fail");
        assert_eq!(json.code(), tonic::Code::InvalidArgument);
        assert!(json.message().starts_with("native JSON field is invalid:"));
        assert_single_field_violation(&json, "json", "must be valid native JSON");

        let asset_status =
            asset_status_to_db("archived", "PENDING").expect_err("unknown asset status");
        assert_eq!(asset_status.code(), tonic::Code::InvalidArgument);
        assert_eq!(asset_status.message(), "unknown asset status: ARCHIVED");
        assert_single_field_violation(
            &asset_status,
            "status",
            "must be a supported AssetStatus enum value",
        );

        let step_status = step_status_to_db("paused", "PENDING").expect_err("unknown step status");
        assert_eq!(step_status.code(), tonic::Code::InvalidArgument);
        assert_eq!(step_status.message(), "unknown step status: PAUSED");
        assert_single_field_violation(
            &step_status,
            "status",
            "must be a supported StepStatus enum value",
        );

        let step_type = step_type_to_db("watermark", "EMBED").expect_err("unknown step type");
        assert_eq!(step_type.code(), tonic::Code::InvalidArgument);
        assert_eq!(step_type.message(), "unknown step type: WATERMARK");
        assert_single_field_violation(
            &step_type,
            "step_type",
            "must be a supported StepType enum value",
        );
    }

    #[test]
    fn register_asset_inactive_file_status_carries_field_violation() {
        let err = active_storage_file_required_status();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "file_id does not reference an active storage file owned by this tenant"
        );
        assert_single_field_violation(
            &err,
            "file_id",
            "must reference an active storage file owned by this tenant",
        );
    }

    #[test]
    fn asset_missing_runtime_capability_carries_typed_detail() {
        let err = asset_capability_status(
            "native_entity_dispatch",
            "runtime_native_entity_dispatch",
            "asset service requires runtime native entity dispatch",
        );
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "asset service requires runtime native entity dispatch"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "asset");
        assert_eq!(detail.operation, "native_entity_dispatch");
        assert_eq!(detail.capability_required, "runtime_native_entity_dispatch");
        assert!(!detail.retryable);
    }

    #[test]
    fn native_state_crypto_failures_carry_capability_detail() {
        for (err, message, operation) in [
            (
                native_state_encryption_failed_status("key unavailable"),
                "native-state encryption failed: key unavailable",
                "native_state_encrypt",
            ),
            (
                native_state_decryption_failed_status("ciphertext invalid"),
                "native-state decryption failed: ciphertext invalid",
                "native_state_decrypt",
            ),
        ] {
            assert_eq!(err.code(), tonic::Code::FailedPrecondition);
            assert_eq!(err.message(), message);
            let detail = decode_detail(&err);
            assert_eq!(detail.kind, ErrorKind::Capability as i32);
            assert_eq!(detail.backend, "asset");
            assert_eq!(detail.operation, operation);
            assert_eq!(detail.capability_required, "native_state_encryption");
            assert!(!detail.retryable);
        }
    }

    #[test]
    fn asset_not_found_statuses_carry_schema_detail() {
        for (operation, schema_code, message) in [
            (
                "get_pipeline_definition",
                "pipeline_definition_not_found",
                "pipeline definition not found",
            ),
            (
                "start_pipeline",
                "pipeline_definition_not_found",
                "pipeline definition not found",
            ),
            (
                "get_pipeline",
                "pipeline_instance_not_found",
                "pipeline instance not found",
            ),
            (
                "complete_step",
                "pipeline_step_not_found",
                "pipeline step not found",
            ),
            ("get_asset", "asset_not_found", "asset not found"),
        ] {
            assert_schema_not_found_detail(
                &asset_schema_not_found_status(operation, schema_code, message),
                operation,
                schema_code,
                message,
            );
        }
    }
}

#[cfg(test)]
mod trigger_reconcile_tests {
    use super::super::consumer::reconcile_trigger_topics;
    use std::collections::BTreeSet;

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn reconcile_starts_new_and_stops_removed_topics() {
        let running = set(&["a", "b", "c"]);
        let desired = set(&["b", "c", "d"]);
        let delta = reconcile_trigger_topics(&running, &desired);
        assert_eq!(delta.to_start, vec!["d".to_string()]);
        assert_eq!(delta.to_stop, vec!["a".to_string()]);
    }

    #[test]
    fn reconcile_first_start_from_empty() {
        let running = BTreeSet::new();
        let desired = set(&["t1", "t2"]);
        let delta = reconcile_trigger_topics(&running, &desired);
        assert_eq!(delta.to_start, vec!["t1".to_string(), "t2".to_string()]);
        assert!(delta.to_stop.is_empty());
    }

    #[test]
    fn reconcile_noop_when_sets_match() {
        let s = set(&["x", "y"]);
        let delta = reconcile_trigger_topics(&s, &s);
        assert!(delta.to_start.is_empty());
        assert!(delta.to_stop.is_empty());
    }

    #[test]
    fn reconcile_full_teardown_when_desired_empty() {
        let running = set(&["a", "b"]);
        let desired = BTreeSet::new();
        let delta = reconcile_trigger_topics(&running, &desired);
        assert!(delta.to_start.is_empty());
        assert_eq!(delta.to_stop, vec!["a".to_string(), "b".to_string()]);
    }
}

#[cfg(all(test, feature = "kafka"))]
mod storage_finalized_consumer_tests {
    use super::super::consumer::{
        STORAGE_FINALIZED_TOPIC, should_commit_storage_finalized_offset,
        storage_finalized_commit_offsets, storage_finalized_consumer_config,
        storage_finalized_payload_ids,
    };
    use super::super::errors::asset_internal_status;

    #[test]
    fn consumer_config_replays_backlog_and_disables_auto_commit() {
        let config = storage_finalized_consumer_config("broker-a:9092");

        assert_eq!(config.get("bootstrap.servers"), Some("broker-a:9092"));
        assert_eq!(
            config.get("group.id"),
            Some("udb-asset-storage-finalized-trigger")
        );
        assert_eq!(config.get("auto.offset.reset"), Some("earliest"));
        assert_eq!(config.get("enable.auto.commit"), Some("false"));
    }

    #[test]
    fn commit_offset_advances_only_the_processed_message() {
        let offsets = storage_finalized_commit_offsets(STORAGE_FINALIZED_TOPIC, 2, 41).unwrap();
        let elem = offsets
            .find_partition(STORAGE_FINALIZED_TOPIC, 2)
            .expect("topic partition offset should be present");

        assert_eq!(elem.offset(), rdkafka::Offset::Offset(42));
    }

    #[test]
    fn commit_decision_follows_handler_success() {
        assert!(should_commit_storage_finalized_offset(&Ok(Some(
            "instance-1".to_string()
        ))));
        assert!(should_commit_storage_finalized_offset(&Ok(None)));
        assert!(!should_commit_storage_finalized_offset(&Err(
            asset_internal_status("handle_storage_finalized", "handler failed")
        )));
    }

    #[test]
    fn finalized_payload_extracts_payload_file_id_then_document_id() {
        let direct = br#"{
            "tenant_id": "tenant-a",
            "document_id": "fallback",
            "payload": { "file_id": "file-a" }
        }"#;
        assert_eq!(
            storage_finalized_payload_ids(direct),
            Some(("tenant-a".to_string(), "file-a".to_string()))
        );

        let fallback = br#"{
            "tenant_id": "tenant-a",
            "document_id": "file-b",
            "payload": {}
        }"#;
        assert_eq!(
            storage_finalized_payload_ids(fallback),
            Some(("tenant-a".to_string(), "file-b".to_string()))
        );

        assert_eq!(
            storage_finalized_payload_ids(br#"{"tenant_id":"tenant-a"}"#),
            None
        );
    }
}
