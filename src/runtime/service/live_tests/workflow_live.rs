use super::support::*;
use crate::proto::udb::core::workflow::services::v1 as workflow_pb;
use crate::proto::udb::core::workflow::services::v1::workflow_service_server::WorkflowService;
use tonic::{Code, Request, Status};
use uuid::Uuid;

const OUTBOX_RELATION: &str = "udb_system.outbox_events";

fn workflow_project_request<T>(
    message: T,
    tenant_id: &str,
    project_id: Option<&str>,
) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "x-tenant-id",
        tenant_id.parse().expect("valid tenant metadata"),
    );
    if let Some(project_id) = project_id {
        request.metadata_mut().insert(
            "x-udb-project-id",
            project_id.parse().expect("valid project metadata"),
        );
    }
    request
}

fn assert_cross_project_not_found<T>(result: Result<T, Status>, operation: &str) {
    let status = result
        .err()
        .unwrap_or_else(|| panic!("{operation} must not expose a different project's workflow"));
    assert_eq!(status.code(), Code::NotFound, "{operation}: {status}");
}

async fn start_workflow(
    svc: &crate::runtime::service::workflow_service::WorkflowServiceImpl,
    tenant_id: &str,
    project_id: &str,
    body_project_id: &str,
    workflow_type: &str,
) -> String {
    svc.start_workflow(workflow_project_request(
        workflow_pb::StartWorkflowRequest {
            tenant_id: tenant_id.to_string(),
            project_id: body_project_id.to_string(),
            workflow_type: workflow_type.to_string(),
            total_steps: 2,
            payload: "{}".to_string(),
            compensations: "[]".to_string(),
            ..Default::default()
        },
        tenant_id,
        Some(project_id),
    ))
    .await
    .unwrap_or_else(|err| panic!("start {workflow_type}: {err}"))
    .into_inner()
    .workflow_id
}

async fn workflow_event(pool: &sqlx::PgPool, topic: &str, workflow_id: &str) -> serde_json::Value {
    sqlx::query_scalar(
        "SELECT payload FROM udb_system.outbox_events \
         WHERE topic = $1 AND payload->'payload'->>'workflow_id' = $2 \
         ORDER BY event_seq DESC LIMIT 1",
    )
    .bind(topic)
    .bind(workflow_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|err| panic!("load {topic} for {workflow_id}: {err}"))
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_workflow_project_ownership_isolation -- --ignored --nocapture"]
async fn live_postgres_workflow_project_ownership_isolation() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    reset_native_outbox(&pool).await;
    let svc = workflow_service(pool.clone())
        .await
        .with_outbox(Some(OUTBOX_RELATION.to_string()));
    let tenant_id = Uuid::new_v4().to_string();
    let project_a = Uuid::new_v4().to_string();
    let project_b = Uuid::new_v4().to_string();

    let workflow_a = start_workflow(&svc, &tenant_id, &project_a, "", "project-a-flow").await;
    let workflow_b =
        start_workflow(&svc, &tenant_id, &project_b, &project_b, "project-b-flow").await;

    let got_a = svc
        .get_workflow(workflow_project_request(
            workflow_pb::GetWorkflowRequest {
                tenant_id: tenant_id.clone(),
                workflow_id: workflow_a.clone(),
            },
            &tenant_id,
            Some(&project_a),
        ))
        .await
        .expect("get project A workflow")
        .into_inner()
        .workflow
        .expect("project A workflow");
    assert_eq!(
        got_a.project_id, project_a,
        "claim/header project must be persisted when the body omits it"
    );

    let listed_a = svc
        .list_workflows(workflow_project_request(
            workflow_pb::ListWorkflowsRequest {
                tenant_id: tenant_id.clone(),
                ..Default::default()
            },
            &tenant_id,
            Some(&project_a),
        ))
        .await
        .expect("list project A workflows")
        .into_inner();
    assert_eq!(listed_a.total_count, 1);
    assert_eq!(listed_a.workflows[0].workflow_id, workflow_a);

    assert_cross_project_not_found(
        svc.get_workflow(workflow_project_request(
            workflow_pb::GetWorkflowRequest {
                tenant_id: tenant_id.clone(),
                workflow_id: workflow_b.clone(),
            },
            &tenant_id,
            Some(&project_a),
        ))
        .await,
        "get_workflow",
    );
    assert_cross_project_not_found(
        svc.signal_workflow(workflow_project_request(
            workflow_pb::SignalWorkflowRequest {
                tenant_id: tenant_id.clone(),
                workflow_id: workflow_b.clone(),
                signal_name: "cross-project".to_string(),
                ..Default::default()
            },
            &tenant_id,
            Some(&project_a),
        ))
        .await,
        "signal_workflow",
    );
    assert_cross_project_not_found(
        svc.cancel_workflow(workflow_project_request(
            workflow_pb::CancelWorkflowRequest {
                tenant_id: tenant_id.clone(),
                workflow_id: workflow_b.clone(),
                reason: "cross-project".to_string(),
            },
            &tenant_id,
            Some(&project_a),
        ))
        .await,
        "cancel_workflow",
    );

    svc.signal_workflow(workflow_project_request(
        workflow_pb::SignalWorkflowRequest {
            tenant_id: tenant_id.clone(),
            workflow_id: workflow_b.clone(),
            signal_name: "tenant-admin-signal".to_string(),
            signal_payload: "{}".to_string(),
        },
        &tenant_id,
        None,
    ))
    .await
    .expect("tenant-wide operator signals project B workflow");
    let signaled = workflow_event(&pool, "udb.workflow.signaled.v1", &workflow_b).await;
    assert_eq!(signaled["project_id"], project_b);
    assert_eq!(signaled["payload"]["project_id"], project_b);

    svc.cancel_workflow(workflow_project_request(
        workflow_pb::CancelWorkflowRequest {
            tenant_id: tenant_id.clone(),
            workflow_id: workflow_b.clone(),
            reason: "tenant-admin-cancel".to_string(),
        },
        &tenant_id,
        None,
    ))
    .await
    .expect("tenant-wide operator cancels project B workflow");
    let cancelled = workflow_event(&pool, "udb.workflow.cancelled.v1", &workflow_b).await;
    assert_eq!(cancelled["project_id"], project_b);
    assert_eq!(cancelled["payload"]["project_id"], project_b);

    let tenant_wide = svc
        .list_workflows(workflow_project_request(
            workflow_pb::ListWorkflowsRequest {
                tenant_id: tenant_id.clone(),
                ..Default::default()
            },
            &tenant_id,
            None,
        ))
        .await
        .expect("tenant-wide credential may list both projects")
        .into_inner();
    assert_eq!(tenant_wide.total_count, 2);

    cleanup_native_service_db(&pool).await;
}
