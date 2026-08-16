use super::support::*;
use crate::proto::udb::core::backup::services::v1 as backup_pb;
use crate::proto::udb::core::backup::services::v1::backup_service_server::BackupService;
use crate::runtime::native_catalog::native_model;
use crate::runtime::service::method_security::{scope_claim_context_for_test, test_claim_context};
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

#[tokio::test]
#[ignore = "requires live Postgres+MinIO; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_backup_restore_remaps_owned_bigserial_identity -- --ignored --nocapture"]
async fn live_postgres_backup_restore_remaps_owned_bigserial_identity() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    reset_native_outbox(&pool).await;

    let source_tenant = Uuid::new_v4().to_string();
    let target_tenant = Uuid::new_v4().to_string();
    let project_id = Uuid::new_v4().to_string();
    let tuple = native_model(
        "udb.core.authz.entity.v1.PolicyTuple",
        &[
            "policy_tuple_id",
            "tuple_kind",
            "subject",
            "domain",
            "object",
            "action",
            "effect",
            "tenant_id",
            "project_id",
        ],
    );
    let seed_sql = format!(
        "INSERT INTO {} ({}, {}, {}, {}, {}, {}, {}, {}) \
         VALUES ('relationship', $1, $2, 'document:seed', 'read', 'allow', $3, $4) \
         RETURNING {}",
        tuple.relation,
        tuple.q("tuple_kind"),
        tuple.q("subject"),
        tuple.q("domain"),
        tuple.q("object"),
        tuple.q("action"),
        tuple.q("effect"),
        tuple.q("tenant_id"),
        tuple.q("project_id"),
        tuple.q("policy_tuple_id"),
    );
    let source_tuple_id: i64 = sqlx::query_scalar(&seed_sql)
        .bind(format!("user:{source_tenant}"))
        .bind(format!("tenant:{source_tenant}"))
        .bind(&source_tenant)
        .bind(&project_id)
        .fetch_one(&pool)
        .await
        .expect("seed source BIGSERIAL policy tuple");

    let notification_log = native_model(
        "udb.core.notification.entity.v1.NotificationLog",
        &[
            "log_id",
            "event_type",
            "channel",
            "tenant_id",
            "project_id",
            "status",
            "retry_count",
            "created_at",
        ],
    );
    let source_log_id = Uuid::new_v4().to_string();
    let event_type = format!("backup.restore.partition-key.{source_log_id}");
    let log_seed_sql = format!(
        "INSERT INTO {} ({}, {}, {}, {}, {}, {}, {}) \
         VALUES ($1::uuid, $2, 'EMAIL', $3, $4, 'PENDING', 0) \
         RETURNING {}::text",
        notification_log.relation,
        notification_log.q("log_id"),
        notification_log.q("event_type"),
        notification_log.q("channel"),
        notification_log.q("tenant_id"),
        notification_log.q("project_id"),
        notification_log.q("status"),
        notification_log.q("retry_count"),
        notification_log.q("created_at"),
    );
    let source_created_at: String = sqlx::query_scalar(&log_seed_sql)
        .bind(&source_log_id)
        .bind(&event_type)
        .bind(&source_tenant)
        .bind(&project_id)
        .fetch_one(&pool)
        .await
        .expect("seed source partitioned notification log");

    let user = native_model(
        "udb.core.authn.entity.v1.User",
        &[
            "user_id",
            "username",
            "email",
            "password_hash",
            "account_kind",
            "status",
            "tenant_id",
            "full_name",
            "created_by",
            "project_id",
        ],
    );
    let expression_index = "udb_users_restore_expression_probe";
    sqlx::query(&format!(
        "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ((lower({})), {})",
        crate::runtime::executor_utils::qi_runtime(expression_index),
        user.relation,
        user.q("username"),
        user.q("user_id"),
    ))
    .execute(&pool)
    .await
    .expect("create live expression-key restore probe");
    let source_user_a = Uuid::new_v4().to_string();
    let source_user_b = Uuid::new_v4().to_string();
    let username_a = format!("u{}", Uuid::new_v4().simple());
    let username_b = format!("u{}", Uuid::new_v4().simple());
    let user_seed_sql = format!(
        "INSERT INTO {} ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}) \
         VALUES ($1::uuid, $2, '', 'test-only-not-a-real-hash', 'PERSON', 'ACTIVE', $3, $4, $5::uuid, $6)",
        user.relation,
        user.q("user_id"),
        user.q("username"),
        user.q("email"),
        user.q("password_hash"),
        user.q("account_kind"),
        user.q("status"),
        user.q("tenant_id"),
        user.q("full_name"),
        user.q("created_by"),
        user.q("project_id"),
    );
    sqlx::query(&user_seed_sql)
        .bind(&source_user_b)
        .bind(&username_b)
        .bind(&source_tenant)
        .bind("restore-self-user-b")
        .bind(Option::<String>::None)
        .bind(&project_id)
        .execute(&pool)
        .await
        .expect("seed first self-reference user");
    sqlx::query(&user_seed_sql)
        .bind(&source_user_a)
        .bind(&username_a)
        .bind(&source_tenant)
        .bind("restore-self-user-a")
        .bind(&source_user_b)
        .bind(&project_id)
        .execute(&pool)
        .await
        .expect("seed second self-reference user");
    sqlx::query(&format!(
        "UPDATE {} SET {} = $1::uuid WHERE {} = $2::uuid",
        user.relation,
        user.q("created_by"),
        user.q("user_id"),
    ))
    .bind(&source_user_a)
    .bind(&source_user_b)
    .execute(&pool)
    .await
    .expect("make the first exported user reference the later self row");

    let svc = backup_service_for_projects(&[&project_id]).await;
    let backup = svc
        .start_tenant_backup(backup_request(
            backup_pb::StartTenantBackupRequest {
                tenant_id: source_tenant.clone(),
                object_backend: "minio".to_string(),
                object_bucket: "udb-storage".to_string(),
                ..Default::default()
            },
            &source_tenant,
            &project_id,
        ))
        .await
        .expect("back up source tenant with BIGSERIAL row")
        .into_inner();
    assert!(
        backup.total_rows > 0,
        "backup must contain the source tuple"
    );

    let restore = backup_request(
        backup_pb::RestoreTenantRequest {
            source_tenant_id: source_tenant.clone(),
            target_tenant_id: target_tenant.clone(),
            backup_id: backup.backup_id,
            confirmation_token: "confirm-cross-tenant-restore".to_string(),
            allow_cross_tenant: true,
            metadata_json: "{}".to_string(),
        },
        &target_tenant,
        &project_id,
    );
    let restored = scope_claim_context_for_test(
        test_claim_context(
            "backup-platform-admin",
            "",
            "",
            &["udb:platform_admin"],
            &["platform_admin"],
        ),
        svc.restore_tenant(restore),
    )
    .await
    .expect("restore allocates a fresh BIGSERIAL identity")
    .into_inner();
    assert!(
        restored.restored_rows > 0,
        "restore must insert tenant rows"
    );

    let identity_sql = format!(
        "SELECT {} FROM {} WHERE {}::text = $1 ORDER BY {}",
        tuple.q("policy_tuple_id"),
        tuple.relation,
        tuple.q("tenant_id"),
        tuple.q("policy_tuple_id"),
    );
    let source_ids: Vec<i64> = sqlx::query_scalar(&identity_sql)
        .bind(&source_tenant)
        .fetch_all(&pool)
        .await
        .expect("read preserved source tuple identity");
    let target_ids: Vec<i64> = sqlx::query_scalar(&identity_sql)
        .bind(&target_tenant)
        .fetch_all(&pool)
        .await
        .expect("read restored target tuple identity");

    assert!(source_ids.contains(&source_tuple_id));
    assert_eq!(target_ids.len(), 1, "one source tuple must restore once");
    assert_ne!(
        target_ids[0], source_tuple_id,
        "RestoreTenant must allocate a fresh sequence-owned identity without overwriting the source"
    );

    let restored_log_sql = format!(
        "SELECT {}::text, {}::text FROM {} WHERE {}::text = $1 AND {} = $2",
        notification_log.q("log_id"),
        notification_log.q("created_at"),
        notification_log.relation,
        notification_log.q("tenant_id"),
        notification_log.q("event_type"),
    );
    let (target_log_id, target_created_at): (String, String) =
        sqlx::query_as(&restored_log_sql)
            .bind(&target_tenant)
            .bind(&event_type)
            .fetch_one(&pool)
            .await
            .expect("read restored partitioned notification log");
    assert_ne!(
        target_log_id, source_log_id,
        "the UUID identity member of the partition-aware key must be remapped"
    );
    assert_eq!(
        target_created_at, source_created_at,
        "the timestamp partition-key member must be preserved once log_id protects the composite key"
    );

    let restored_users_sql = format!(
        "SELECT {}::text, {}, {}, {}::text, {} FROM {} WHERE {}::text = $1 ORDER BY {}",
        user.q("user_id"),
        user.q("username"),
        user.q("email"),
        user.q("created_by"),
        user.q("full_name"),
        user.relation,
        user.q("tenant_id"),
        user.q("full_name"),
    );
    let restored_users: Vec<(String, String, String, Option<String>, String)> =
        sqlx::query_as(&restored_users_sql)
            .bind(&target_tenant)
            .fetch_all(&pool)
            .await
            .expect("read restored self-referencing users");
    assert_eq!(restored_users.len(), 2);
    let restored_a = restored_users
        .iter()
        .find(|row| row.4 == "restore-self-user-a")
        .expect("restored user A");
    let restored_b = restored_users
        .iter()
        .find(|row| row.4 == "restore-self-user-b")
        .expect("restored user B");
    assert_ne!(restored_a.0, source_user_a);
    assert_ne!(restored_b.0, source_user_b);
    assert_eq!(restored_a.3.as_deref(), Some(restored_b.0.as_str()));
    assert_eq!(restored_b.3.as_deref(), Some(restored_a.0.as_str()));
    for restored_user in [&restored_a, &restored_b] {
        assert_eq!(restored_user.2, "", "partial-index-excluded email stays empty");
        assert_eq!(restored_user.1.len(), 33);
        assert!(restored_user.1.starts_with('r'));
    }

    sqlx::query(&format!(
        "DROP INDEX IF EXISTS {}.{}",
        crate::runtime::executor_utils::qi_runtime("udb_authn"),
        crate::runtime::executor_utils::qi_runtime(expression_index),
    ))
    .execute(&pool)
    .await
    .expect("drop live expression-key restore probe");
}
