//! service.rs split — admin RPC handlers (Phase G).
use super::*;

impl DataBrokerService {
    pub(crate) async fn list_dlq_events_inner(
        &self,
        request: Request<DlqListRequest>,
    ) -> Result<Response<DlqListResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("ListDlqEvents", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "ListDlqEvents").await {
            return self.record_grpc("ListDlqEvents", started, Err(err));
        }
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
            )
            .await;
        match result {
            Ok(events) => {
                let proto_events: Vec<DlqEventRecord> = events
                    .iter()
                    .map(|e| DlqEventRecord {
                        dlq_id: e["dlq_id"].as_str().unwrap_or_default().into(),
                        event_id: e["event_id"].as_str().unwrap_or_default().into(),
                        topic: e["topic"].as_str().unwrap_or_default().into(),
                        payload_json: e["payload_json"]
                            .as_str()
                            .unwrap_or_default()
                            .as_bytes()
                            .to_vec(),
                        error_type: e["error_type"].as_str().unwrap_or_default().into(),
                        error_message: e["error_message"].as_str().unwrap_or_default().into(),
                        status: e["status"].as_str().unwrap_or_default().into(),
                        created_at_unix: e["created_at_unix"].as_i64().unwrap_or_default(),
                        updated_at_unix: e["updated_at_unix"].as_i64().unwrap_or_default(),
                    })
                    .collect();
                self.record_grpc(
                    "ListDlqEvents",
                    started,
                    Ok(Response::new(DlqListResponse {
                        next_page_token: next_page_token(offset, limit, proto_events.len() as i32),
                        total_count: proto_events.len() as i32,
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
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("GetDlqEvent", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "GetDlqEvent").await {
            return self.record_grpc("GetDlqEvent", started, Err(err));
        }
        let req = request.into_inner();
        let result = self.runtime_snapshot().get_dlq_event(&req.dlq_id).await;
        match result {
            Ok(event) => self.record_grpc(
                "GetDlqEvent",
                started,
                Ok(Response::new(DlqEventResponse {
                    event: Some(DlqEventRecord {
                        dlq_id: event["dlq_id"].as_str().unwrap_or_default().into(),
                        event_id: event["event_id"].as_str().unwrap_or_default().into(),
                        topic: event["topic"].as_str().unwrap_or_default().into(),
                        payload_json: event["payload_json"]
                            .as_str()
                            .unwrap_or_default()
                            .as_bytes()
                            .to_vec(),
                        error_type: event["error_type"].as_str().unwrap_or_default().into(),
                        error_message: event["error_message"].as_str().unwrap_or_default().into(),
                        status: event["status"].as_str().unwrap_or_default().into(),
                        created_at_unix: event["created_at_unix"].as_i64().unwrap_or_default(),
                        updated_at_unix: event["updated_at_unix"].as_i64().unwrap_or_default(),
                    }),
                })),
            ),
            Err(err) => self.record_grpc("GetDlqEvent", started, Err(err)),
        }
    }

    pub(crate) async fn replay_dlq_event_inner(
        &self,
        request: Request<DlqActionRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("ReplayDlqEvent", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "ReplayDlqEvent").await {
            return self.record_grpc("ReplayDlqEvent", started, Err(err));
        }
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .replay_dlq_event(&req.dlq_id, req.preserve_event_id)
            .await;
        match result {
            Ok(replayed_event_id) => {
                let _ = self
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
                    .await;
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
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("DismissDlqEvent", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "DismissDlqEvent").await {
            return self.record_grpc("DismissDlqEvent", started, Err(err));
        }
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .update_dlq_status(&req.dlq_id, "DISMISSED")
            .await;
        match result {
            Ok(_) => {
                let _ = self
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
                    .await;
                self.record_grpc(
                    "DismissDlqEvent",
                    started,
                    Ok(Response::new(MutationResponse {
                        mutation_id: uuid::Uuid::new_v4().to_string(),
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
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("QuarantineDlqEvent", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "QuarantineDlqEvent").await {
            return self.record_grpc("QuarantineDlqEvent", started, Err(err));
        }
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .update_dlq_status(&req.dlq_id, "QUARANTINED")
            .await;
        match result {
            Ok(_) => {
                let _ = self
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
                    .await;
                self.record_grpc(
                    "QuarantineDlqEvent",
                    started,
                    Ok(Response::new(MutationResponse {
                        mutation_id: uuid::Uuid::new_v4().to_string(),
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
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("GetCdcStatus", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "GetCdcStatus").await {
            return self.record_grpc("GetCdcStatus", started, Err(err));
        }
        let req = request.into_inner();
        let result = self.runtime_snapshot().get_cdc_status(&req.slot_name).await;
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
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("PauseCdc", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "PauseCdc").await {
            return self.record_grpc("PauseCdc", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "PauseCdc",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
        }
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .pause_cdc(&req.slot_name, &req.reason)
            .await;
        match result {
            Ok(()) => {
                let _ = self
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
                    .await;
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
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("ResumeCdc", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "ResumeCdc").await {
            return self.record_grpc("ResumeCdc", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "ResumeCdc",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
        }
        let req = request.into_inner();
        let result = self.runtime_snapshot().resume_cdc(&req.slot_name).await;
        match result {
            Ok(()) => {
                let _ = self
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
                    .await;
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
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("StepDownCdcLeader", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "StepDownCdcLeader").await {
            return self.record_grpc("StepDownCdcLeader", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "StepDownCdcLeader",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
        }
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .stepdown_cdc_leader(&req.slot_name)
            .await;
        match result {
            Ok(()) => {
                let _ = self
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
                    .await;
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

    pub(crate) async fn list_sagas_inner(
        &self,
        request: Request<SagaListRequest>,
    ) -> Result<Response<SagaListResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("ListSagas", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "ListSagas").await {
            return self.record_grpc("ListSagas", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "ListSagas",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
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
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("GetSaga", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "GetSaga").await {
            return self.record_grpc("GetSaga", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "GetSaga",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
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
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("RetrySagaCompensation", started, Err(e)),
        };
        if let Err(err) = self
            .authorize(&security, "*", "RetrySagaCompensation")
            .await
        {
            return self.record_grpc("RetrySagaCompensation", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "RetrySagaCompensation",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
        }
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .retry_saga_compensation_admin(&req.saga_id)
            .await;
        match result {
            Ok(()) => {
                let _ = self
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
                    .await;
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
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("MarkSagaReviewed", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "MarkSagaReviewed").await {
            return self.record_grpc("MarkSagaReviewed", started, Err(err));
        }
        if !security.scopes.iter().any(|s| s == "udb:admin" || s == "*") {
            return self.record_grpc(
                "MarkSagaReviewed",
                started,
                Err(Status::permission_denied("scope udb:admin is required")),
            );
        }
        let req = request.into_inner();
        let result = self
            .runtime_snapshot()
            .mark_saga_reviewed_admin(&req.saga_id)
            .await;
        match result {
            Ok(()) => {
                let _ = self
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
                    .await;
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
