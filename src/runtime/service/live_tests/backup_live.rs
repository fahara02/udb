use super::support::*;
use crate::proto::udb::core::backup::services::v1 as backup_pb;
use crate::proto::udb::core::backup::services::v1::backup_service_server::BackupService;
use crate::runtime::native_catalog::native_model;
use tonic::{Code, Request};
use uuid::Uuid;

fn backup_request<T>(message: T, tenant_id: &str, project_id: &str) -> Request<T> {
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

async fn put_policy(
    svc: &crate::runtime::service::backup_service::BackupServiceImpl,
    tenant_id: &str,
    project_id: &str,
    bucket: &str,
) -> String {
    svc.put_backup_policy(backup_request(
        backup_pb::PutBackupPolicyRequest {
            tenant_id: tenant_id.to_string(),
            policy_name: "daily".to_string(),
            schedule_cron: "0 2 * * *".to_string(),
            retention_days: 30,
            max_retained_backups: 5,
            enabled: true,
            object_backend: "minio".to_string(),
            object_bucket: bucket.to_string(),
            metadata_json: "{}".to_string(),
        },
        tenant_id,
        project_id,
    ))
    .await
    .unwrap_or_else(|err| panic!("put {project_id} policy: {err}"))
    .into_inner()
    .policy_id
}

async fn seed_run(
    pool: &sqlx::PgPool,
    tenant_id: &str,
    project_id: &str,
    backup_id: &str,
    status: &str,
) {
    let model = native_model(
        "udb.core.backup.entity.v1.BackupRun",
        &["backup_id", "tenant_id", "project_id", "kind", "status"],
    );
    let sql = format!(
        "INSERT INTO {} ({}, {}, {}, {}, {}) VALUES ($1::uuid, $2, $3, 'BACKUP', $4)",
        model.relation,
        model.q("backup_id"),
        model.q("tenant_id"),
        model.q("project_id"),
        model.q("kind"),
        model.q("status"),
    );
    sqlx::query(&sql)
        .bind(backup_id)
        .bind(tenant_id)
        .bind(project_id)
        .bind(status)
        .execute(pool)
        .await
        .unwrap_or_else(|err| panic!("seed backup run {backup_id}: {err}"));
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_backup_same_tenant_project_isolation -- --ignored --nocapture"]
async fn live_postgres_backup_same_tenant_project_isolation() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    reset_native_outbox(&pool).await;

    let tenant_id = Uuid::new_v4().to_string();
    let project_a = Uuid::new_v4().to_string();
    let project_b = Uuid::new_v4().to_string();
    let svc = backup_service_for_projects(&[&project_a, &project_b]).await;

    let policy_a = put_policy(&svc, &tenant_id, &project_a, "project-a-backups").await;
    let policy_b = put_policy(&svc, &tenant_id, &project_b, "project-b-backups").await;
    assert_ne!(policy_a, policy_b, "same policy name must be project-local");

    let got_a = svc
        .get_backup_policy(backup_request(
            backup_pb::GetBackupPolicyRequest {
                tenant_id: tenant_id.clone(),
                policy_name: "daily".to_string(),
            },
            &tenant_id,
            &project_a,
        ))
        .await
        .expect("get project A policy")
        .into_inner()
        .policy
        .expect("project A policy present");
    assert_eq!(got_a.policy_id, policy_a);
    assert_eq!(got_a.project_id, project_a);
    assert_eq!(got_a.object_bucket, "project-a-backups");

    let listed_b = svc
        .list_backup_policies(backup_request(
            backup_pb::ListBackupPoliciesRequest {
                tenant_id: tenant_id.clone(),
                page_size: 100,
                page_token: String::new(),
            },
            &tenant_id,
            &project_b,
        ))
        .await
        .expect("list project B policies")
        .into_inner()
        .policies;
    assert_eq!(listed_b.len(), 1);
    assert_eq!(listed_b[0].policy_id, policy_b);
    assert_eq!(listed_b[0].project_id, project_b);

    svc.delete_backup_policy(backup_request(
        backup_pb::DeleteBackupPolicyRequest {
            tenant_id: tenant_id.clone(),
            policy_name: "daily".to_string(),
        },
        &tenant_id,
        &project_a,
    ))
    .await
    .expect("delete project A policy");
    let still_b = svc
        .get_backup_policy(backup_request(
            backup_pb::GetBackupPolicyRequest {
                tenant_id: tenant_id.clone(),
                policy_name: "daily".to_string(),
            },
            &tenant_id,
            &project_b,
        ))
        .await
        .expect("project A delete must leave project B policy")
        .into_inner()
        .policy
        .expect("project B policy remains");
    assert_eq!(still_b.policy_id, policy_b);

    let run_a = Uuid::new_v4().to_string();
    let run_b = Uuid::new_v4().to_string();
    let legacy_run = Uuid::new_v4().to_string();
    seed_run(&pool, &tenant_id, &project_a, &run_a, "RUNNING").await;
    seed_run(&pool, &tenant_id, &project_b, &run_b, "FAILED").await;
    seed_run(&pool, &tenant_id, "", &legacy_run, "FAILED").await;

    let listed_a = svc
        .list_backups(backup_request(
            backup_pb::ListBackupsRequest {
                tenant_id: tenant_id.clone(),
                kind: "BACKUP".to_string(),
                page_size: 100,
                page_token: String::new(),
            },
            &tenant_id,
            &project_a,
        ))
        .await
        .expect("list project A runs")
        .into_inner()
        .backups;
    assert_eq!(listed_a.len(), 1, "project B and legacy rows stay hidden");
    assert_eq!(listed_a[0].backup_id, run_a);
    assert_eq!(listed_a[0].project_id, project_a);

    let cross_project = svc
        .get_backup(backup_request(
            backup_pb::GetBackupRequest {
                tenant_id: tenant_id.clone(),
                backup_id: run_b,
            },
            &tenant_id,
            &project_a,
        ))
        .await
        .expect_err("project A must not read project B run");
    assert_eq!(cross_project.code(), Code::NotFound);

    let legacy = svc
        .get_backup(backup_request(
            backup_pb::GetBackupRequest {
                tenant_id: tenant_id.clone(),
                backup_id: legacy_run,
            },
            &tenant_id,
            &project_a,
        ))
        .await
        .expect_err("blank-project legacy row must remain quarantined");
    assert_eq!(legacy.code(), Code::NotFound);
}
