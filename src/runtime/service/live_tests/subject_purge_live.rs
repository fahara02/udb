//! Served erasure proof: exact counts, tenant/project boundaries, old
//! tombstones, and a receipt signed with the configured broker key.

use super::data_plane_live::{
    col, create_schema, dp_live_pg_dsn, dp_pool, dp_service_with, teardown, widget_manifest,
    with_ctx,
};
use crate::proto::PurgeSubjectRequest;
use crate::proto::data_broker_server::DataBroker;
use crate::runtime::security::verify_erasure_receipt;
use crate::runtime::system::ensure_system_catalog;
use tonic::Code;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live Postgres; CI runs every ignored lib test"]
async fn subject_purge_removes_only_the_subject_and_counts_rows_once() {
    let Some(dsn) = dp_live_pg_dsn() else { return };
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool).await.expect("system catalog");
    let schema = format!("udb_purge_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    let other_tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".subjects (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL,
         project_id TEXT NOT NULL, owner_id TEXT NOT NULL, status TEXT, deleted_at TIMESTAMPTZ)"
    ))
    .execute(&pool)
    .await
    .expect("subject table");
    for (id, row_tenant, project, owner, deleted) in [
        ("active", tenant.as_str(), "p1", "subject-a", false),
        ("tombstone", tenant.as_str(), "p1", "subject-a", true),
        ("other-subject", tenant.as_str(), "p1", "subject-b", false),
        (
            "other-tenant",
            other_tenant.as_str(),
            "p1",
            "subject-a",
            true,
        ),
        ("other-project", tenant.as_str(), "p2", "subject-a", true),
    ] {
        sqlx::query(&format!(
            "INSERT INTO \"{schema}\".subjects (id, tenant_id, project_id, owner_id, status, deleted_at)
             VALUES ($1, $2, $3, $4, 'LIVE', CASE WHEN $5 THEN NOW() ELSE NULL END)"
        ))
        .bind(id).bind(row_tenant).bind(project).bind(owner).bind(deleted)
        .execute(&pool).await.expect("seed subject rows");
    }
    let mut manifest = widget_manifest(&schema, "subjects", "SubjectRecord", true);
    let mut owner = col("owner_id", "TEXT", false);
    owner.security.owner_field = true;
    manifest.tables[0]
        .columns
        .extend([owner, col("project_id", "TEXT", false)]);
    manifest.tables[0].table_security.project_column = "project_id".into();
    let service = dp_service_with(&dsn, manifest, |config| {
        config.security.jwt_private_key =
            Some(include_str!("../../testdata/jwt_rs256_private.pem").into());
    })
    .await;
    let active = service.catalog.active_for("default").manifest.clone();
    let checksum = service
        .catalog
        .stage_catalog(
            (*active).clone(),
            "p1".into(),
            "1.0.0".into(),
            "exact".into(),
        )
        .await
        .expect("stage subject catalog");
    service
        .catalog
        .activate_catalog_for("p1", &checksum)
        .await
        .expect("activate subject project");
    let request = |confirm: &str, dry_run: bool| {
        let mut request = with_ctx(
            PurgeSubjectRequest {
                subject_id: "subject-a".into(),
                confirm_subject_id: confirm.into(),
                dry_run,
                ..Default::default()
            },
            &tenant,
        );
        request
            .metadata_mut()
            .insert("x-udb-project-id", "p1".parse().unwrap());
        request
    };
    assert_eq!(
        service
            .purge_subject(request("wrong", false))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let dry = service
        .purge_subject(request("subject-a", true))
        .await
        .expect("dry run")
        .into_inner();
    assert!(dry.dry_run);
    let before: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM \"{schema}\".subjects"))
        .fetch_one(&pool)
        .await
        .expect("count after dry run");
    assert_eq!(before, 5, "dry run and refused confirmation change nothing");
    let erased = service
        .purge_subject(request("subject-a", false))
        .await
        .expect("purge subject")
        .into_inner();
    assert_eq!(
        erased.total_deleted, 2,
        "active row and old tombstone each count once"
    );
    assert_eq!(erased.tables[0].deleted, 2);
    let remaining: Vec<String> =
        sqlx::query_scalar(&format!("SELECT id FROM \"{schema}\".subjects ORDER BY id"))
            .fetch_all(&pool)
            .await
            .expect("read back survivors");
    assert_eq!(
        remaining,
        ["other-project", "other-subject", "other-tenant"]
    );
    let receipt = verify_erasure_receipt(
        &erased.receipt_jws,
        include_str!("../../testdata/jwt_rs256_public.pem"),
    )
    .expect("signed erasure receipt verifies");
    assert_eq!(receipt["total_deleted"], 2);
    assert_eq!(receipt["project_id"], "p1");
    assert!(
        !erased.receipt_json.contains("subject-a"),
        "receipt excludes the erased identifier"
    );
    teardown(&pool, &schema, &tenant).await;
}
