//! service.rs split — admin RPC handlers (Phase G).
use super::*;

impl DataBrokerService {
    pub(crate) async fn list_dlq_events_inner(
        &self,
        request: Request<DlqListRequest>,
    ) -> Result<Response<DlqListResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "ListDlqEvents");
        let req = request.into_inner();
        let limit = bounded_list_limit(req.limit);
        let offset = page_offset(&req.page_token);
        let result = self
            .runtime_snapshot()
            .list_dlq_events(
                &req.topic,
                &req.status_filter,
                limit as i64,
                &offset.to_string(),
                &security.tenant_id,
                &security.project_id,
            )
            .await;
        match result {
            Ok(events) => {
                // total_count is the overall match count (COUNT(*) OVER()),
                // carried on each row; fall back to the page size when empty.
                let total_count = events
                    .first()
                    .and_then(|e| e["total_count"].as_i64())
                    .unwrap_or(events.len() as i64) as i32;
                let proto_events: Vec<DlqEventRecord> =
                    events.into_iter().map(dlq_event_record_from_json).collect();
                self.record_grpc(
                    "ListDlqEvents",
                    started,
                    Ok(Response::new(DlqListResponse {
                        next_page_token: next_page_token(offset, limit, proto_events.len() as i32),
                        total_count,
                        events: proto_events,
                    })),
                )
            }
            Err(err) => self.record_grpc("ListDlqEvents", started, Err(err)),
        }
    }

    pub(crate) async fn get_dlq_event_inner(
        &self,
        request: Request<DlqEventRequest>,
    ) -> Result<Response<DlqEventResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "GetDlqEvent");
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .get_dlq_event(&req.dlq_id, &security.tenant_id, &security.project_id)
            .await;
        match result {
            Ok(event) => self.record_grpc(
                "GetDlqEvent",
                started,
                Ok(Response::new(DlqEventResponse {
                    event: Some(dlq_event_record_from_json(event)),
                })),
            ),
            Err(err) => self.record_grpc("GetDlqEvent", started, Err(err)),
        }
    }

    pub(crate) async fn replay_dlq_event_inner(
        &self,
        request: Request<DlqActionRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "ReplayDlqEvent");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("ReplayDlqEvent", started, Err(err));
        }
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .replay_dlq_event(
                &req.dlq_id,
                req.preserve_event_id,
                &security.tenant_id,
                &security.project_id,
            )
            .await;
        match result {
            Ok(replayed_event_id) => {
                if let Err(err) = self
                    .runtime_snapshot()
                    .write_audit_log(
                        &security.service_identity,
                        "ReplayDlqEvent",
                        &req.dlq_id,
                        &serde_json::json!({
                            "dlq_id": req.dlq_id,
                            "replayed_event_id": replayed_event_id,
                            "preserve_event_id": req.preserve_event_id
                        }),
                        "ok",
                        &security.tenant_id,
                        "",
                        &security.correlation_id,
                    )
                    .await
                {
                    tracing::warn!(error = %err, operation = "ReplayDlqEvent", "admin audit log write failed");
                }
                self.record_grpc(
                    "ReplayDlqEvent",
                    started,
                    Ok(Response::new(MutationResponse {
                        mutation_id: replayed_event_id,
                        resource_uri: req.dlq_id,
                        affected_rows: 1,
                        ..Default::default()
                    })),
                )
            }
            Err(err) => self.record_grpc("ReplayDlqEvent", started, Err(err)),
        }
    }

    pub(crate) async fn dismiss_dlq_event_inner(
        &self,
        request: Request<DlqActionRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "DismissDlqEvent");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("DismissDlqEvent", started, Err(err));
        }
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .update_dlq_status(
                &req.dlq_id,
                "DISMISSED",
                &security.tenant_id,
                &security.project_id,
            )
            .await;
        match result {
            Ok(_) => {
                if let Err(err) = self
                    .runtime_snapshot()
                    .write_audit_log(
                        &security.service_identity,
                        "DismissDlqEvent",
                        &req.dlq_id,
                        &serde_json::json!({"dlq_id": req.dlq_id}),
                        "ok",
                        &security.tenant_id,
                        "",
                        &security.correlation_id,
                    )
                    .await
                {
                    tracing::warn!(error = %err, operation = "DismissDlqEvent", "admin audit log write failed");
                }
                self.record_grpc(
                    "DismissDlqEvent",
                    started,
                    Ok(Response::new(MutationResponse {
                        mutation_id: req.dlq_id.clone(),
                        resource_uri: req.dlq_id,
                        affected_rows: 1,
                        ..Default::default()
                    })),
                )
            }
            Err(err) => self.record_grpc("DismissDlqEvent", started, Err(err)),
        }
    }

    pub(crate) async fn quarantine_dlq_event_inner(
        &self,
        request: Request<DlqActionRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "QuarantineDlqEvent");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("QuarantineDlqEvent", started, Err(err));
        }
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .update_dlq_status(
                &req.dlq_id,
                "QUARANTINED",
                &security.tenant_id,
                &security.project_id,
            )
            .await;
        match result {
            Ok(_) => {
                if let Err(err) = self
                    .runtime_snapshot()
                    .write_audit_log(
                        &security.service_identity,
                        "QuarantineDlqEvent",
                        &req.dlq_id,
                        &serde_json::json!({"dlq_id": req.dlq_id}),
                        "ok",
                        &security.tenant_id,
                        "",
                        &security.correlation_id,
                    )
                    .await
                {
                    tracing::warn!(error = %err, operation = "QuarantineDlqEvent", "admin audit log write failed");
                }
                self.record_grpc(
                    "QuarantineDlqEvent",
                    started,
                    Ok(Response::new(MutationResponse {
                        mutation_id: req.dlq_id.clone(),
                        resource_uri: req.dlq_id,
                        affected_rows: 1,
                        ..Default::default()
                    })),
                )
            }
            Err(err) => self.record_grpc("QuarantineDlqEvent", started, Err(err)),
        }
    }

    pub(crate) async fn get_cdc_status_inner(
        &self,
        request: Request<CdcControlRequest>,
    ) -> Result<Response<CdcStatusResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "GetCdcStatus");
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .get_cdc_status(&req.slot_name, &security.tenant_id, &security.project_id)
            .await;
        match result {
            Ok(v) => self.record_grpc(
                "GetCdcStatus",
                started,
                Ok(Response::new(CdcStatusResponse {
                    slot_name: v["slot_name"].as_str().unwrap_or_default().into(),
                    paused: v["paused"].as_bool().unwrap_or(false),
                    pause_reason: v["pause_reason"].as_str().unwrap_or_default().into(),
                    outbox_depth: v["outbox_depth"].as_i64().unwrap_or(0),
                    ..Default::default()
                })),
            ),
            Err(err) => self.record_grpc("GetCdcStatus", started, Err(err)),
        }
    }

    pub(crate) async fn pause_cdc_inner(
        &self,
        request: Request<CdcControlRequest>,
    ) -> Result<Response<CdcStatusResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "PauseCdc");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("PauseCdc", started, Err(err));
        }
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .pause_cdc(
                &req.slot_name,
                &req.reason,
                &security.tenant_id,
                &security.project_id,
            )
            .await;
        match result {
            Ok(()) => {
                if let Err(err) = self
                    .runtime_snapshot()
                    .write_audit_log(
                        &security.service_identity,
                        "PauseCdc",
                        &req.slot_name,
                        &serde_json::json!({"slot_name": req.slot_name, "reason": req.reason}),
                        "ok",
                        &security.tenant_id,
                        "",
                        &security.correlation_id,
                    )
                    .await
                {
                    tracing::warn!(error = %err, operation = "PauseCdc", "admin audit log write failed");
                }
                self.record_grpc(
                    "PauseCdc",
                    started,
                    Ok(Response::new(CdcStatusResponse {
                        slot_name: req.slot_name,
                        paused: true,
                        pause_reason: req.reason,
                        ..Default::default()
                    })),
                )
            }
            Err(err) => self.record_grpc("PauseCdc", started, Err(err)),
        }
    }

    pub(crate) async fn resume_cdc_inner(
        &self,
        request: Request<CdcControlRequest>,
    ) -> Result<Response<CdcStatusResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "ResumeCdc");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("ResumeCdc", started, Err(err));
        }
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .resume_cdc(&req.slot_name, &security.tenant_id, &security.project_id)
            .await;
        match result {
            Ok(()) => {
                if let Err(err) = self
                    .runtime_snapshot()
                    .write_audit_log(
                        &security.service_identity,
                        "ResumeCdc",
                        &req.slot_name,
                        &serde_json::json!({"slot_name": req.slot_name}),
                        "ok",
                        &security.tenant_id,
                        "",
                        &security.correlation_id,
                    )
                    .await
                {
                    tracing::warn!(error = %err, operation = "ResumeCdc", "admin audit log write failed");
                }
                self.record_grpc(
                    "ResumeCdc",
                    started,
                    Ok(Response::new(CdcStatusResponse {
                        slot_name: req.slot_name,
                        paused: false,
                        ..Default::default()
                    })),
                )
            }
            Err(err) => self.record_grpc("ResumeCdc", started, Err(err)),
        }
    }

    pub(crate) async fn step_down_cdc_leader_inner(
        &self,
        request: Request<CdcControlRequest>,
    ) -> Result<Response<CdcStatusResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "StepDownCdcLeader");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("StepDownCdcLeader", started, Err(err));
        }
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .stepdown_cdc_leader(&req.slot_name, &security.tenant_id, &security.project_id)
            .await;
        match result {
            Ok(()) => {
                if let Err(err) = self
                    .runtime_snapshot()
                    .write_audit_log(
                        &security.service_identity,
                        "StepDownCdcLeader",
                        &req.slot_name,
                        &serde_json::json!({"slot_name": req.slot_name}),
                        "ok",
                        &security.tenant_id,
                        "",
                        &security.correlation_id,
                    )
                    .await
                {
                    tracing::warn!(error = %err, operation = "StepDownCdcLeader", "admin audit log write failed");
                }
                self.record_grpc(
                    "StepDownCdcLeader",
                    started,
                    Ok(Response::new(CdcStatusResponse {
                        slot_name: req.slot_name,
                        ..Default::default()
                    })),
                )
            }
            Err(err) => self.record_grpc("StepDownCdcLeader", started, Err(err)),
        }
    }

    pub(crate) async fn preview_cdc_redaction_inner(
        &self,
        request: Request<CdcRedactionPreviewRequest>,
    ) -> Result<Response<CdcRedactionPreviewResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "PreviewCdcRedaction");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("PreviewCdcRedaction", started, Err(err));
        }
        let req = request.into_inner();
        let payload: serde_json::Value = if req.payload_json.is_empty() {
            serde_json::Value::Object(serde_json::Map::new())
        } else {
            serde_json::from_slice(&req.payload_json).map_err(|err| {
                Status::invalid_argument(format!("payload_json must be JSON: {err}"))
            })?
        };
        let runtime = self.runtime_snapshot();
        let cdc_config = &runtime.config().cdc;
        let mode = if req.redaction_mode.trim().is_empty() {
            cdc_config.redaction_mode
        } else {
            CdcRedactionMode::from_env_value(&req.redaction_mode)
        };
        let redaction_version = if req.redaction_version > 0 {
            req.redaction_version as u32
        } else {
            cdc_config.redaction_version
        };
        let preview = crate::runtime::cdc::preview_manifest_cdc_redaction(
            &self.manifest,
            &req.message_type,
            &req.topic,
            if req.schema_uri.trim().is_empty() {
                None
            } else {
                Some(req.schema_uri.as_str())
            },
            payload,
            mode,
            redaction_version,
        );
        let payload_json = serde_json::to_vec(&preview.payload).map_err(|err| {
            Status::internal(format!("failed to serialize redaction preview: {err}"))
        })?;
        let audit_redacted_fields = preview.redacted_fields.clone();
        if let Err(err) = runtime
            .write_audit_log(
                &security.service_identity,
                "PreviewCdcRedaction",
                &format!("cdc/redaction/{}", req.topic),
                &serde_json::json!({
                    "message_type": req.message_type,
                    "topic": req.topic,
                    "schema_uri": req.schema_uri,
                    "requested_redaction_mode": req.redaction_mode,
                    "effective_redaction_mode": preview.redaction_mode.as_str(),
                    "effective_redaction_version": preview.redaction_version,
                    "redacted_fields": audit_redacted_fields,
                    "payload_bytes": req.payload_json.len()
                }),
                "previewed",
                &security.tenant_id,
                &security.project_id,
                &security.correlation_id,
            )
            .await
        {
            tracing::warn!(error = %err, operation = "PreviewCdcRedaction", "admin audit log write failed");
        }
        self.record_grpc("PreviewCdcRedaction", started, {
            let would_redact = !preview.redacted_fields.is_empty();
            Ok(Response::new(CdcRedactionPreviewResponse {
                payload_json,
                redacted_fields: preview.redacted_fields,
                redaction_mode: preview.redaction_mode.as_str().to_string(),
                redaction_version: preview.redaction_version as i32,
                would_redact,
            }))
        })
    }

    pub(crate) async fn scan_projection_drift_inner(
        &self,
        request: Request<ProjectionDriftScanRequest>,
    ) -> Result<Response<ProjectionDriftScanResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "ScanProjectionDrift");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("ScanProjectionDrift", started, Err(err));
        }
        let req = request.into_inner();
        let project_id = if req.project_id.trim().is_empty() {
            security.project_id.clone()
        } else {
            req.project_id.trim().to_string()
        };
        if project_id.trim().is_empty() {
            return self.record_grpc(
                "ScanProjectionDrift",
                started,
                Err(Status::invalid_argument(
                    "project_id is required in request or metadata",
                )),
            );
        }
        let message_type = req.message_type.trim().to_string();
        if message_type.is_empty() {
            return self.record_grpc(
                "ScanProjectionDrift",
                started,
                Err(Status::invalid_argument("message_type is required")),
            );
        }
        let Some(engine) = self.projection_engine.as_ref() else {
            return self.record_grpc(
                "ScanProjectionDrift",
                started,
                Err(Status::failed_precondition(
                    "projection engine is not available; configure Postgres canonical source and system store",
                )),
            );
        };

        let rows_per_target = if req.rows_per_target > 0 {
            req.rows_per_target as usize
        } else {
            100
        };
        let (mode, source_limit, mode_label) =
            projection_drift_scan_mode(&req.scan_mode, rows_per_target, req.limit)?;
        let active = self.catalog.active_for(&project_id);
        let plan = crate::runtime::projection::ProjectionPlan::from_manifest(&active.manifest)
            .into_iter()
            .find(|plan| plan.message_type == message_type)
            .ok_or_else(|| {
                Status::invalid_argument(format!(
                    "message_type '{}' has no projection plan in project '{}'",
                    message_type, project_id
                ))
            })?;
        let samples = engine
            .load_source_samples(&active.manifest, &message_type, source_limit)
            .await
            .map_err(|err| Status::internal(format!("projection source scan failed: {err}")))?;
        let runtime = self.runtime_snapshot();
        let worker =
            crate::runtime::drift_reconciliation::DriftScannerWorker::new(runtime.clone(), mode);
        let scan_results = worker.scan_plan(&plan, &samples).await;
        let mut reports = Vec::new();
        let mut summary_reports = Vec::new();
        let mut warnings = Vec::new();
        for result in scan_results {
            let mut target_warnings = result.warnings;
            warnings.extend(target_warnings.iter().cloned());
            let repair_tasks_enqueued = if req.repair && result.report.divergent_rows.is_empty() {
                0
            } else if req.repair {
                match crate::runtime::drift_reconciliation::repair_drift(
                    engine,
                    &active.manifest,
                    &project_id,
                    &message_type,
                    &result.report,
                )
                .await
                {
                    Ok(count) => count as i64,
                    Err(err) => {
                        target_warnings.push(format!("repair failed: {err}"));
                        warnings.push(format!(
                            "repair failed for {}:{}: {err}",
                            result.report.target_backend, result.report.target_resource
                        ));
                        0
                    }
                }
            } else {
                0
            };
            let divergent_rows = result
                .report
                .divergent_rows
                .iter()
                .map(|row| {
                    serde_json::to_vec(&row.row_key)
                        .map(|row_key_json| ProjectionDriftDivergentRow {
                            row_key_json,
                            source_checksum: row.source_checksum.clone(),
                            target_checksum: row.target_checksum.clone().unwrap_or_default(),
                            kind: match row.kind {
                                crate::runtime::drift_reconciliation::DivergenceKind::MissingOnTarget => "missing_on_target",
                                crate::runtime::drift_reconciliation::DivergenceKind::ChecksumMismatch => "checksum_mismatch",
                            }
                            .to_string(),
                        })
                        .map_err(|err| {
                            Status::internal(format!(
                                "failed to encode drift row key as JSON: {err}"
                            ))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            reports.push(ProjectionDriftTargetReport {
                target_backend: result.report.target_backend.clone(),
                target_instance: result.report.target_instance.clone(),
                target_resource: result.report.target_resource.clone(),
                source_rows_scanned: result.report.source_rows_scanned as i32,
                divergent_rows,
                rows_to_repair: result.report.estimated_repair_cost.rows_to_repair as i32,
                estimated_cost_units: result.report.estimated_repair_cost.total_cost_units,
                repair_tasks_enqueued,
                warnings: target_warnings,
            });
            summary_reports.push(result.report);
        }
        let summary =
            crate::runtime::drift_reconciliation::DriftScanner::summarise(&summary_reports);
        let summary_json = serde_json::to_vec(&summary)
            .map_err(|err| Status::internal(format!("failed to serialize drift summary: {err}")))?;
        if let Err(err) = runtime
            .write_audit_log(
                &security.service_identity,
                "ScanProjectionDrift",
                &format!("projection/drift/{project_id}/{message_type}"),
                &serde_json::json!({
                    "project_id": project_id.clone(),
                    "message_type": message_type.clone(),
                    "scan_mode": mode_label.clone(),
                    "source_rows_loaded": samples.len(),
                    "targets": reports.len(),
                    "repair": req.repair,
                }),
                "ok",
                &security.tenant_id,
                &project_id,
                &security.correlation_id,
            )
            .await
        {
            tracing::warn!(error = %err, operation = "ScanProjectionDrift", "admin audit log write failed");
        }
        self.record_grpc(
            "ScanProjectionDrift",
            started,
            Ok(Response::new(ProjectionDriftScanResponse {
                project_id,
                message_type,
                scan_mode: mode_label,
                source_rows_loaded: samples.len() as i32,
                reports,
                summary_json,
                warnings,
            })),
        )
    }

    pub(crate) async fn list_sagas_inner(
        &self,
        request: Request<SagaListRequest>,
    ) -> Result<Response<SagaListResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "ListSagas");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("ListSagas", started, Err(err));
        }
        let req = request.into_inner();
        let limit = bounded_list_limit(req.limit);
        let offset = page_offset(&req.page_token);
        let result = self
            .runtime_snapshot()
            .list_sagas_admin(
                &req.tenant_id_filter,
                &req.status_filter,
                &req.tx_id_filter,
                &req.correlation_id_filter,
                limit as i64,
                offset as i64,
            )
            .await;
        match result {
            Ok(records) => {
                let sagas: Vec<SagaRecord> =
                    records.into_iter().map(saga_record_to_proto).collect();
                self.record_grpc(
                    "ListSagas",
                    started,
                    Ok(Response::new(SagaListResponse {
                        next_page_token: next_page_token(offset, limit, sagas.len() as i32),
                        total_count: sagas.len() as i32,
                        sagas,
                    })),
                )
            }
            Err(err) => self.record_grpc("ListSagas", started, Err(err)),
        }
    }

    pub(crate) async fn get_saga_inner(
        &self,
        request: Request<SagaRequest>,
    ) -> Result<Response<SagaResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "GetSaga");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("GetSaga", started, Err(err));
        }
        let req = request.into_inner();
        let result = self.runtime_snapshot().get_saga_admin(&req.saga_id).await;
        match result {
            Ok(record) => self.record_grpc(
                "GetSaga",
                started,
                Ok(Response::new(SagaResponse {
                    saga: Some(saga_record_to_proto(record)),
                    ..Default::default()
                })),
            ),
            Err(err) => self.record_grpc("GetSaga", started, Err(err)),
        }
    }

    pub(crate) async fn retry_saga_compensation_inner(
        &self,
        request: Request<SagaRequest>,
    ) -> Result<Response<SagaResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "RetrySagaCompensation");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("RetrySagaCompensation", started, Err(err));
        }
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .retry_saga_compensation_admin(&req.saga_id)
            .await;
        match result {
            Ok(()) => {
                if let Err(err) = self
                    .runtime_snapshot()
                    .write_audit_log(
                        &security.service_identity,
                        "RetrySagaCompensation",
                        &req.saga_id,
                        &serde_json::json!({"saga_id": req.saga_id}),
                        "ok",
                        &security.tenant_id,
                        "",
                        &security.correlation_id,
                    )
                    .await
                {
                    tracing::warn!(error = %err, operation = "RetrySagaCompensation", "admin audit log write failed");
                }
                self.record_grpc(
                    "RetrySagaCompensation",
                    started,
                    Ok(Response::new(SagaResponse::default())),
                )
            }
            Err(err) => self.record_grpc("RetrySagaCompensation", started, Err(err)),
        }
    }

    pub(crate) async fn mark_saga_reviewed_inner(
        &self,
        request: Request<SagaRequest>,
    ) -> Result<Response<SagaResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "MarkSagaReviewed");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("MarkSagaReviewed", started, Err(err));
        }
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .mark_saga_reviewed_admin(&req.saga_id)
            .await;
        match result {
            Ok(()) => {
                if let Err(err) = self
                    .runtime_snapshot()
                    .write_audit_log(
                        &security.service_identity,
                        "MarkSagaReviewed",
                        &req.saga_id,
                        &serde_json::json!({"saga_id": req.saga_id}),
                        "ok",
                        &security.tenant_id,
                        "",
                        &security.correlation_id,
                    )
                    .await
                {
                    tracing::warn!(error = %err, operation = "MarkSagaReviewed", "admin audit log write failed");
                }
                self.record_grpc(
                    "MarkSagaReviewed",
                    started,
                    Ok(Response::new(SagaResponse::default())),
                )
            }
            Err(err) => self.record_grpc("MarkSagaReviewed", started, Err(err)),
        }
    }
}

fn projection_drift_scan_mode(
    requested: &str,
    rows_per_target: usize,
    limit: i32,
) -> Result<(crate::runtime::drift_reconciliation::ScanMode, i64, String), Status> {
    let requested = requested.trim().to_ascii_lowercase();
    match requested.as_str() {
        "" | "sample" => {
            let sample_rows = rows_per_target.max(1);
            let source_limit = if limit > 0 {
                i64::from(limit)
            } else {
                sample_rows as i64
            };
            Ok((
                crate::runtime::drift_reconciliation::ScanMode::Sample {
                    rows_per_target: sample_rows,
                },
                source_limit.max(1),
                "sample".to_string(),
            ))
        }
        "full" => {
            if limit <= 0 {
                return Err(Status::invalid_argument(
                    "full projection drift scans require limit > 0 to bound the canonical source read",
                ));
            }
            Ok((
                crate::runtime::drift_reconciliation::ScanMode::Full,
                i64::from(limit),
                "full".to_string(),
            ))
        }
        other => Err(Status::invalid_argument(format!(
            "unsupported projection drift scan_mode '{other}'; use sample or full"
        ))),
    }
}

fn dlq_event_record_from_json(mut event: serde_json::Value) -> DlqEventRecord {
    let mut obj = event.as_object_mut();
    DlqEventRecord {
        dlq_id: take_json_string(&mut obj, "dlq_id"),
        event_id: take_json_string(&mut obj, "event_id"),
        topic: take_json_string(&mut obj, "topic"),
        payload_json: take_json_string(&mut obj, "payload_json").into_bytes(),
        error_type: take_json_string(&mut obj, "error_type"),
        error_message: take_json_string(&mut obj, "error_message"),
        status: take_json_string(&mut obj, "status"),
        created_at_unix: take_json_i64(&mut obj, "created_at_unix"),
        updated_at_unix: take_json_i64(&mut obj, "updated_at_unix"),
    }
}

fn take_json_string(
    obj: &mut Option<&mut serde_json::Map<String, serde_json::Value>>,
    key: &str,
) -> String {
    obj.as_mut()
        .and_then(|obj| obj.remove(key))
        .and_then(|value| match value {
            serde_json::Value::String(value) => Some(value),
            serde_json::Value::Number(value) => Some(value.to_string()),
            serde_json::Value::Bool(value) => Some(value.to_string()),
            _ => None,
        })
        .unwrap_or_default()
}

fn take_json_i64(
    obj: &mut Option<&mut serde_json::Map<String, serde_json::Value>>,
    key: &str,
) -> i64 {
    obj.as_mut()
        .and_then(|obj| obj.remove(key))
        .and_then(|value| match value {
            serde_json::Value::Number(value) => value.as_i64(),
            serde_json::Value::String(value) => value.parse().ok(),
            _ => None,
        })
        .unwrap_or_default()
}
