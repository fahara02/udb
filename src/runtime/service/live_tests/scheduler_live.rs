use super::support::*;
use crate::proto::udb::core::scheduler::services::v1 as scheduler_pb;
use crate::proto::udb::core::scheduler::services::v1::scheduler_service_server::SchedulerService;
use tonic::{Code, Request, Status};
use uuid::Uuid;

const OUTBOX_RELATION: &str = "udb_system.outbox_events";

fn scheduler_project_request<T>(message: T, tenant_id: &str, project_id: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "x-tenant-id",
        tenant_id.parse().expect("valid tenant metadata"),
    );
    request.metadata_mut().insert(
        "x-udb-project-id",
        project_id.parse().expect("valid project metadata"),
    );
    request
}

fn assert_cross_project_not_found<T>(result: Result<T, Status>, operation: &str) {
    let status = result
        .err()
        .unwrap_or_else(|| panic!("{operation} must not expose a different project's job"));
    assert_eq!(status.code(), Code::NotFound, "{operation}: {status}");
}

async fn create_job(
    svc: &crate::runtime::service::scheduler_service::SchedulerServiceImpl,
    tenant_id: &str,
    project_id: &str,
    body_project_id: &str,
    name: &str,
) -> String {
    svc.create_job(scheduler_project_request(
        scheduler_pb::CreateJobRequest {
            tenant_id: tenant_id.to_string(),
            project_id: body_project_id.to_string(),
            name: name.to_string(),
            schedule_type: "CRON".to_string(),
            cron_expression: "@daily".to_string(),
            payload: "{}".to_string(),
            ..Default::default()
        },
        tenant_id,
        project_id,
    ))
    .await
    .unwrap_or_else(|err| panic!("create {name}: {err}"))
    .into_inner()
    .job_id
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_scheduler_project_ownership_isolation -- --ignored --nocapture"]
async fn live_postgres_scheduler_project_ownership_isolation() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    reset_native_outbox(&pool).await;
    let svc = scheduler_service(pool.clone())
        .await
        .with_outbox(Some(OUTBOX_RELATION.to_string()));
    let tenant_id = Uuid::new_v4().to_string();
    let project_a = Uuid::new_v4().to_string();
    let project_b = Uuid::new_v4().to_string();

    let job_a = create_job(&svc, &tenant_id, &project_a, "", "project-a-job").await;
    let job_b = create_job(&svc, &tenant_id, &project_b, &project_b, "project-b-job").await;

    let got_a = svc
        .get_job(scheduler_project_request(
            scheduler_pb::GetJobRequest {
                tenant_id: tenant_id.clone(),
                job_id: job_a.clone(),
            },
            &tenant_id,
            &project_a,
        ))
        .await
        .expect("get project A job")
        .into_inner()
        .job
        .expect("project A job");
    assert_eq!(
        got_a.project_id, project_a,
        "claim/header project must be persisted when the body omits it"
    );
    let listed_a = svc
        .list_jobs(scheduler_project_request(
            scheduler_pb::ListJobsRequest {
                tenant_id: tenant_id.clone(),
                ..Default::default()
            },
            &tenant_id,
            &project_a,
        ))
        .await
        .expect("list project A jobs")
        .into_inner();
    assert_eq!(listed_a.total_count, 1);
    assert_eq!(listed_a.jobs[0].job_id, job_a);

    assert_cross_project_not_found(
        svc.get_job(scheduler_project_request(
            scheduler_pb::GetJobRequest {
                tenant_id: tenant_id.clone(),
                job_id: job_b.clone(),
            },
            &tenant_id,
            &project_a,
        ))
        .await,
        "get_job",
    );
    assert_cross_project_not_found(
        svc.pause_job(scheduler_project_request(
            scheduler_pb::PauseJobRequest {
                tenant_id: tenant_id.clone(),
                job_id: job_b.clone(),
            },
            &tenant_id,
            &project_a,
        ))
        .await,
        "pause_job",
    );

    svc.pause_job(scheduler_project_request(
        scheduler_pb::PauseJobRequest {
            tenant_id: tenant_id.clone(),
            job_id: job_b.clone(),
        },
        &tenant_id,
        &project_b,
    ))
    .await
    .expect("pause project B job");
    let paused_envelope: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM udb_system.outbox_events \
         WHERE topic = $1 AND payload->'payload'->>'job_id' = $2 \
         ORDER BY event_seq DESC LIMIT 1",
    )
    .bind("udb.scheduler.job.paused.v1")
    .bind(&job_b)
    .fetch_one(&pool)
    .await
    .expect("project B pause event");
    assert_eq!(paused_envelope["project_id"], project_b);
    assert_eq!(paused_envelope["payload"]["project_id"], project_b);

    assert_cross_project_not_found(
        svc.resume_job(scheduler_project_request(
            scheduler_pb::ResumeJobRequest {
                tenant_id: tenant_id.clone(),
                job_id: job_b.clone(),
            },
            &tenant_id,
            &project_a,
        ))
        .await,
        "resume_job",
    );
    assert_cross_project_not_found(
        svc.delete_job(scheduler_project_request(
            scheduler_pb::DeleteJobRequest {
                tenant_id: tenant_id.clone(),
                job_id: job_b.clone(),
            },
            &tenant_id,
            &project_a,
        ))
        .await,
        "delete_job",
    );

    let tenant_wide = svc
        .list_jobs(Request::new(scheduler_pb::ListJobsRequest {
            tenant_id,
            ..Default::default()
        }))
        .await
        .expect("tenant-wide credential may list both projects")
        .into_inner();
    assert_eq!(tenant_wide.total_count, 2);

    cleanup_native_service_db(&pool).await;
}
