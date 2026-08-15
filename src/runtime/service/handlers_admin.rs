//! service.rs split — admin RPC handlers (Phase G).
use super::*;

fn admin_invalid_fields<I, F, D>(message: impl Into<String>, fields: I) -> Status
where
    I: IntoIterator<Item = (F, D)>,
    F: Into<String>,
    D: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, fields)
}

fn admin_required_field(
    message: &'static str,
    field: &'static str,
    description: &'static str,
) -> Status {
    admin_invalid_fields(message, [(field, description)])
}

fn admin_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "admin",
        operation,
        capability_required,
        message,
    )
}

fn admin_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("admin", operation, message)
}

fn invalid_redaction_payload_json(err: serde_json::Error) -> Status {
    admin_invalid_fields(
        format!("payload_json must be JSON: {err}"),
        [("payload_json", "must be valid JSON bytes")],
    )
}

fn validate_projection_drift_request(project_id: &str, message_type: &str) -> Result<(), Status> {
    if project_id.trim().is_empty() {
        return Err(admin_required_field(
            "project_id is required in request or metadata",
            "project_id",
            "must be supplied in request or metadata",
        ));
    }
    if message_type.trim().is_empty() {
        return Err(admin_required_field(
            "message_type is required",
            "message_type",
            "must be a non-empty projected message type",
        ));
    }
    Ok(())
}

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
            serde_json::from_slice(&req.payload_json).map_err(invalid_redaction_payload_json)?
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
            admin_internal_status(
                "PreviewCdcRedaction",
                format!("failed to serialize redaction preview: {err}"),
            )
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
        let project_id = match super::handlers_catalog::resolve_catalog_mutation_project(
            &security,
            &req.project_id,
            "ScanProjectionDrift",
        ) {
            Ok(project_id) => project_id,
            Err(err) => return self.record_grpc("ScanProjectionDrift", started, Err(err)),
        };
        let message_type = req.message_type.trim().to_string();
        if let Err(err) = validate_projection_drift_request(&project_id, &message_type) {
            return self.record_grpc("ScanProjectionDrift", started, Err(err));
        }
        let Some(engine) = self.projection_engine.as_ref() else {
            return self.record_grpc(
                "ScanProjectionDrift",
                started,
                Err(admin_capability_status(
                    "projection_drift",
                    "projection_engine",
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
        let active = match self.catalog.active_exact_for(&project_id) {
            Some(active) => active,
            None => {
                return self.record_grpc(
                    "ScanProjectionDrift",
                    started,
                    Err(crate::runtime::executor_utils::schema_status(
                        tonic::Code::FailedPrecondition,
                        "catalog",
                        "ScanProjectionDrift",
                        "catalog_project_not_active",
                        "projection drift scanning requires an exact ACTIVE project catalog",
                    )),
                );
            }
        };
        let plan = crate::runtime::projection::ProjectionPlan::from_manifest(&active.manifest)
            .into_iter()
            .find(|plan| {
                crate::runtime::projection::message_type_matches(&plan.message_type, &message_type)
            })
            .ok_or_else(|| {
                admin_invalid_fields(
                    format!(
                        "message_type '{}' has no projection plan in project '{}'",
                        message_type, project_id
                    ),
                    [(
                        "message_type",
                        "must match a configured projection plan for the project",
                    )],
                )
            })?;
        let runtime = self.runtime_snapshot();
        let samples = match engine
            .load_source_samples(
                runtime.as_ref(),
                &project_id,
                &active.manifest,
                &message_type,
                source_limit,
            )
            .await
        {
            Ok(samples) => samples,
            Err(err) => return self.record_grpc("ScanProjectionDrift", started, Err(err)),
        };
        let worker = crate::runtime::drift_reconciliation::DriftScannerWorker::new(
            runtime.clone(),
            project_id.clone(),
            mode,
        );
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
                    &samples,
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
                            row_key_json: row_key_json.into(),
                            source_checksum: row.source_checksum.clone(),
                            target_checksum: row.target_checksum.clone().unwrap_or_default(),
                            kind: match row.kind {
                                crate::runtime::drift_reconciliation::DivergenceKind::MissingOnTarget => "missing_on_target",
                                crate::runtime::drift_reconciliation::DivergenceKind::ChecksumMismatch => "checksum_mismatch",
                            }
                            .to_string(),
                        })
                        .map_err(|err| {
                            admin_internal_status(
                                "ScanProjectionDrift",
                                format!("failed to encode drift row key as JSON: {err}"),
                            )
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
        let summary_json = serde_json::to_vec(&summary).map_err(|err| {
            admin_internal_status(
                "ScanProjectionDrift",
                format!("failed to serialize drift summary: {err}"),
            )
        })?;
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

    /// Idempotently seed a baseline `manual_review` saga row and a retryable
    /// DLQ row for the VERIFIED principal's tenant/project. This is
    /// PRIVILEGE-CREATING admin tooling, so it is fail-closed: it requires the
    /// `udb:admin` scope AND the `UDB_ENABLE_ADMIN_SEED` env switch, and it
    /// derives tenant/project ONLY from `security` (never from the request
    /// body). Inserts mirror `durable_dlq_insert_sql` (ON CONFLICT (event_id))
    /// and the saga DDL (ON CONFLICT (saga_id)).
    pub(crate) async fn ensure_baseline_inner(
        &self,
        request: Request<EnsureBaselineRequest>,
    ) -> Result<Response<EnsureBaselineResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "EnsureBaseline");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("EnsureBaseline", started, Err(err));
        }
        // Fail-closed: privilege-creating seed is disabled unless explicitly
        // enabled by the operator. Default (unset) => failed_precondition. The env
        // gate is read in service/mod.rs (allowlisted startup/config boundary), not
        // here, so this handler stays free of direct env access.
        let enabled = super::admin_seed_enabled();
        if !enabled {
            return self.record_grpc(
                "EnsureBaseline",
                started,
                Err(admin_capability_status(
                    "ensure_baseline",
                    "admin_seed_enabled",
                    "admin baseline seeding is disabled; set UDB_ENABLE_ADMIN_SEED=1 to enable",
                )),
            );
        }
        // Tenant/project come ONLY from the verified principal, never the body.
        let tenant_id = security.tenant_id.clone();
        let project_id = security.project_id.clone();

        let runtime = self.runtime_snapshot();
        let pool = match runtime.pg_pool() {
            Ok(pool) => pool,
            Err(err) => return self.record_grpc("EnsureBaseline", started, Err(err)),
        };
        let config = crate::runtime::system::SystemCatalogConfig::default();
        let dlq_rel = config.dlq_relation();

        let dlq_event_id = Uuid::new_v4();

        let result: Result<(Uuid, Uuid), Status> = async {
            let store = runtime.default_system_stores().ok_or_else(|| {
                admin_capability_status(
                    "ensure_baseline",
                    "canonical_system_store",
                    "admin baseline seeding requires a canonical system store",
                )
            })?;
            let saga_id = crate::runtime::canonical_store::system_store::SagaStore::record_saga(
                store.as_ref(),
                &crate::runtime::canonical_store::system_store::SagaInsert {
                    tx_id: Uuid::new_v4().to_string(),
                    tenant_id: tenant_id.clone(),
                    correlation_id: security.correlation_id.clone(),
                    backend_instance: "admin".to_string(),
                    operation: "ensure_baseline".to_string(),
                    status: crate::runtime::canonical_store::system_store::SagaStatus::ManualReview,
                    steps: serde_json::json!([]),
                    compensations: serde_json::json!([]),
                },
            )
            .await
            .map_err(|err| {
                admin_internal_status(
                    "EnsureBaseline",
                    format!("EnsureBaseline saga insert failed: {err}"),
                )
            })?;

            // Retryable DLQ row mirroring `durable_dlq_insert_sql`: status
            // RETRYING, ON CONFLICT (event_id) DO NOTHING (the conflict target
            // is `event_id`, NOT `dlq_id`).
            let dlq_id = sqlx::query_scalar::<_, Uuid>(&format!(
                "INSERT INTO {dlq_rel} \
                 (event_id, topic, tenant_id, project_id, error_type, error_message, payload, \
                  status, next_retry_at, created_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7::JSONB, 'RETRYING', \
                         NOW() + ($8::BIGINT * INTERVAL '1 second'), NOW()) \
                 ON CONFLICT (event_id) DO NOTHING \
                 RETURNING dlq_id"
            ))
            .bind(dlq_event_id)
            .bind("udb.admin.baseline.seed")
            .bind(&tenant_id)
            .bind(&project_id)
            .bind("baseline_seed")
            .bind("baseline seed")
            .bind(serde_json::json!({}))
            .bind(60_i64)
            .fetch_one(pool)
            .await
            .map_err(|err| {
                admin_internal_status(
                    "EnsureBaseline",
                    format!("EnsureBaseline dlq insert failed: {err}"),
                )
            })?;
            Ok((saga_id, dlq_id))
        }
        .await;

        match result {
            Ok((saga_id, dlq_id)) => {
                if let Err(err) = runtime
                    .write_audit_log(
                        &security.service_identity,
                        "EnsureBaseline",
                        &saga_id.to_string(),
                        &serde_json::json!({
                            "saga_id": saga_id.to_string(),
                            "dlq_id": dlq_id.to_string(),
                            "dlq_event_id": dlq_event_id.to_string(),
                        }),
                        "ok",
                        &tenant_id,
                        "",
                        &security.correlation_id,
                    )
                    .await
                {
                    tracing::warn!(error = %err, operation = "EnsureBaseline", "admin audit log write failed");
                }
                self.record_grpc(
                    "EnsureBaseline",
                    started,
                    Ok(Response::new(EnsureBaselineResponse {
                        saga_ids: vec![saga_id.to_string()],
                        dlq_ids: vec![dlq_id.to_string()],
                        device_id: String::new(),
                    })),
                )
            }
            Err(err) => self.record_grpc("EnsureBaseline", started, Err(err)),
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
                return Err(admin_required_field(
                    "full projection drift scans require limit > 0 to bound the canonical source read",
                    "limit",
                    "must be greater than 0 when scan_mode is full",
                ));
            }
            Ok((
                crate::runtime::drift_reconciliation::ScanMode::Full,
                i64::from(limit),
                "full".to_string(),
            ))
        }
        other => Err(admin_invalid_fields(
            format!("unsupported projection drift scan_mode '{other}'; use sample or full"),
            [("scan_mode", "must be sample or full")],
        )),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use tonic::{Code, Status};

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_validation_field(status: &Status, field: &str, description: &str) {
        assert_eq!(status.code(), Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_capability_detail(
        status: &Status,
        operation: &str,
        capability_required: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "admin");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability_required);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "admin");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn admin_internal_status_carries_typed_detail() {
        assert_internal_detail(
            &admin_internal_status(
                "ScanProjectionDrift",
                "projection source scan failed: missing source",
            ),
            "ScanProjectionDrift",
            "projection source scan failed: missing source",
        );
    }

    #[test]
    fn admin_setup_capabilities_carry_typed_detail() {
        for (operation, capability_required, message) in [
            (
                "projection_drift",
                "projection_engine",
                "projection engine is not available; configure Postgres canonical source and system store",
            ),
            (
                "ensure_baseline",
                "admin_seed_enabled",
                "admin baseline seeding is disabled; set UDB_ENABLE_ADMIN_SEED=1 to enable",
            ),
        ] {
            let status = admin_capability_status(operation, capability_required, message);
            assert_capability_detail(&status, operation, capability_required, message);
        }
    }

    #[test]
    fn redaction_preview_invalid_payload_json_carries_field_violation() {
        let err = serde_json::from_slice::<serde_json::Value>(b"{")
            .map_err(invalid_redaction_payload_json)
            .expect_err("invalid payload_json must fail before redaction preview");

        assert!(err.message().starts_with("payload_json must be JSON:"));
        assert_validation_field(&err, "payload_json", "must be valid JSON bytes");
    }

    #[test]
    fn projection_drift_missing_project_id_carries_field_violation() {
        let err = validate_projection_drift_request(" ", "acme.Invoice")
            .expect_err("missing project_id must fail before projection lookup");

        assert_eq!(
            err.message(),
            "project_id is required in request or metadata"
        );
        assert_validation_field(
            &err,
            "project_id",
            "must be supplied in request or metadata",
        );
    }

    #[test]
    fn projection_drift_missing_message_type_carries_field_violation() {
        let err = validate_projection_drift_request("project-a", " ")
            .expect_err("missing message_type must fail before projection lookup");

        assert_eq!(err.message(), "message_type is required");
        assert_validation_field(
            &err,
            "message_type",
            "must be a non-empty projected message type",
        );
    }

    #[test]
    fn projection_drift_full_scan_without_limit_carries_field_violation() {
        let err = projection_drift_scan_mode("full", 100, 0)
            .expect_err("full scan without limit must fail before source read");

        assert_eq!(
            err.message(),
            "full projection drift scans require limit > 0 to bound the canonical source read"
        );
        assert_validation_field(
            &err,
            "limit",
            "must be greater than 0 when scan_mode is full",
        );
    }

    #[test]
    fn projection_drift_unknown_scan_mode_carries_field_violation() {
        let err = projection_drift_scan_mode("everything", 100, 10)
            .expect_err("unsupported scan_mode must fail before source read");

        assert_eq!(
            err.message(),
            "unsupported projection drift scan_mode 'everything'; use sample or full"
        );
        assert_validation_field(&err, "scan_mode", "must be sample or full");
    }

    #[test]
    fn projection_drift_missing_plan_status_carries_field_violation() {
        let err = admin_invalid_fields(
            "message_type 'acme.Invoice' has no projection plan in project 'project-a'",
            [(
                "message_type",
                "must match a configured projection plan for the project",
            )],
        );

        assert_eq!(
            err.message(),
            "message_type 'acme.Invoice' has no projection plan in project 'project-a'"
        );
        assert_validation_field(
            &err,
            "message_type",
            "must match a configured projection plan for the project",
        );
    }
}
