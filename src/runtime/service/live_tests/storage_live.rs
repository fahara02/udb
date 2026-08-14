use super::support::*;
use crate::proto::udb::core::storage::services::v1 as storage_pb;
use crate::proto::udb::core::storage::services::v1::storage_service_server::StorageService;
use tonic::{Code, Request, Status};
use uuid::Uuid;

fn storage_project_request<T>(message: T, tenant_id: &str, project_id: &str) -> Request<T> {
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
        .unwrap_or_else(|| panic!("{operation} must not expose a different project's file"));
    assert_eq!(status.code(), Code::NotFound, "{operation}: {status}");
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_storage_native_schema_from_proto -- --ignored --nocapture"]
async fn live_postgres_storage_native_schema_from_proto() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;

    assert_native_table_columns(
        &pool,
        "udb.core.storage.entity.v1.File",
        &[
            "file_id",
            "tenant_id",
            "filename",
            "object_key",
            "file_type",
            "status",
            "is_public",
            "audit_info",
            "deleted_at",
        ],
    )
    .await;

    cleanup_native_service_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_storage_crud_roundtrip -- --ignored --nocapture"]
async fn live_postgres_storage_crud_roundtrip() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let svc = storage_service(pool.clone()).await;
    let tenant_id = Uuid::new_v4().to_string();

    // register → PENDING record + allocated object_key
    let reg = svc
        .register_upload(Request::new(storage_pb::RegisterUploadRequest {
            tenant_id: tenant_id.clone(),
            filename: "invoice.pdf".to_string(),
            content_type: "application/pdf".to_string(),
            file_type: "PDF".to_string(),
            size_bytes: 13,
            // finalize supplies is_public: true, and it is IMMUTABLE after
            // register, so establish it here (the test asserts a public file).
            is_public: Some(true),
            ..Default::default()
        }))
        .await
        .expect("register_upload")
        .into_inner();
    assert!(!reg.file_id.is_empty());
    assert!(reg.object_key.contains(&reg.file_id));
    put_storage_object(&reg.object_key, "application/pdf", b"%PDF-udb-live").await;

    // finalize → ACTIVE, returns the row
    let fin = svc
        .finalize_upload(Request::new(storage_pb::FinalizeUploadRequest {
            tenant_id: tenant_id.clone(),
            file_id: reg.file_id.clone(),
            content_type: "application/pdf".to_string(),
            is_public: Some(true),
            size_bytes: 13,
            ..Default::default()
        }))
        .await
        .expect("finalize_upload")
        .into_inner();
    let file = fin.file.expect("finalized file");
    assert_eq!(file.file_id, reg.file_id);
    assert!(file.is_public);
    assert_eq!(file.size_bytes, 13, "finalize must persist the actual size");

    // list → finds it
    let listed = svc
        .list_files(Request::new(storage_pb::ListFilesRequest {
            tenant_id: tenant_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("list_files")
        .into_inner();
    assert_eq!(listed.total_count, 1);

    // update → new filename
    svc.update_file(Request::new(storage_pb::UpdateFileRequest {
        tenant_id: tenant_id.clone(),
        file_id: reg.file_id.clone(),
        filename: "invoice-final.pdf".to_string(),
        ..Default::default()
    }))
    .await
    .expect("update_file");

    // delete (soft) → gone from get
    let del = svc
        .delete_file(Request::new(storage_pb::DeleteFileRequest {
            tenant_id: tenant_id.clone(),
            file_id: reg.file_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("delete_file")
        .into_inner();
    assert!(del.success);

    let missing = svc
        .get_file(Request::new(storage_pb::GetFileRequest {
            tenant_id,
            file_id: reg.file_id,
        }))
        .await;
    assert!(missing.is_err(), "soft-deleted file must not be readable");

    cleanup_native_service_db(&pool).await;
}

/// §1 read-after-write (13.7.1.2): the id `RegisterUpload`→`FinalizeUpload` returns
/// is IMMEDIATELY gettable by `GetFile` on the SAME served path with the SAME tenant
/// metadata. Reverting the storage finalize/get guarantee fails this assertion.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_storage_read_after_write -- --ignored --nocapture"]
async fn live_postgres_storage_read_after_write() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let svc = storage_service(pool.clone()).await;
    let tenant_id = Uuid::new_v4().to_string();

    let reg = svc
        .register_upload(Request::new(storage_pb::RegisterUploadRequest {
            tenant_id: tenant_id.clone(),
            filename: "ryw.pdf".to_string(),
            content_type: "application/pdf".to_string(),
            file_type: "PDF".to_string(),
            size_bytes: 14,
            ..Default::default()
        }))
        .await
        .expect("register_upload")
        .into_inner();
    put_storage_object(&reg.object_key, "application/pdf", b"%PDF-udb-ryw01").await;
    svc.finalize_upload(Request::new(storage_pb::FinalizeUploadRequest {
        tenant_id: tenant_id.clone(),
        file_id: reg.file_id.clone(),
        content_type: "application/pdf".to_string(),
        size_bytes: 14,
        ..Default::default()
    }))
    .await
    .expect("finalize_upload");

    assert_create_then_get("RegisterUpload→GetFile", &reg.file_id, |id| {
        let svc = &svc;
        let tenant_id = tenant_id.clone();
        async move {
            let file = svc
                .get_file(Request::new(storage_pb::GetFileRequest {
                    tenant_id,
                    file_id: id.clone(),
                }))
                .await?
                .into_inner()
                .file;
            Ok(file.is_some_and(|f| f.file_id == id))
        }
    })
    .await;

    cleanup_native_service_db(&pool).await;
}

/// A project-scoped credential must never use a same-tenant file id to cross
/// the Storage ownership boundary. The metadata remains physically tenant
/// placed; only an intentionally tenant-wide request may see both projects.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_storage_project_ownership_isolation -- --ignored --nocapture"]
async fn live_postgres_storage_project_ownership_isolation() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let svc = storage_service(pool.clone()).await;
    let tenant_id = Uuid::new_v4().to_string();
    let project_a = Uuid::new_v4().to_string();
    let project_b = Uuid::new_v4().to_string();

    let file_a = svc
        .register_upload(storage_project_request(
            storage_pb::RegisterUploadRequest {
                tenant_id: tenant_id.clone(),
                filename: "project-a.txt".to_string(),
                content_type: "text/plain".to_string(),
                ..Default::default()
            },
            &tenant_id,
            &project_a,
        ))
        .await
        .expect("register project A file")
        .into_inner();
    let file_b = svc
        .register_upload(storage_project_request(
            storage_pb::RegisterUploadRequest {
                tenant_id: tenant_id.clone(),
                project_id: project_b.clone(),
                filename: "project-b.txt".to_string(),
                content_type: "text/plain".to_string(),
                ..Default::default()
            },
            &tenant_id,
            &project_b,
        ))
        .await
        .expect("register project B file")
        .into_inner();

    let listed_a = svc
        .list_files(storage_project_request(
            storage_pb::ListFilesRequest {
                tenant_id: tenant_id.clone(),
                ..Default::default()
            },
            &tenant_id,
            &project_a,
        ))
        .await
        .expect("list project A")
        .into_inner();
    assert_eq!(listed_a.total_count, 1);
    assert_eq!(listed_a.files.len(), 1);
    assert_eq!(listed_a.files[0].file_id, file_a.file_id);
    assert_eq!(
        listed_a.files[0].project_id, project_a,
        "claim/header project must be persisted when the body omits it"
    );

    assert_cross_project_not_found(
        svc.get_file(storage_project_request(
            storage_pb::GetFileRequest {
                tenant_id: tenant_id.clone(),
                file_id: file_b.file_id.clone(),
            },
            &tenant_id,
            &project_a,
        ))
        .await,
        "get_file",
    );
    assert_cross_project_not_found(
        svc.get_download_url(storage_project_request(
            storage_pb::GetDownloadUrlRequest {
                tenant_id: tenant_id.clone(),
                file_id: file_b.file_id.clone(),
                ..Default::default()
            },
            &tenant_id,
            &project_a,
        ))
        .await,
        "get_download_url",
    );
    assert_cross_project_not_found(
        svc.reissue_upload_url(storage_project_request(
            storage_pb::ReissueUploadUrlRequest {
                tenant_id: tenant_id.clone(),
                file_id: file_b.file_id.clone(),
                ..Default::default()
            },
            &tenant_id,
            &project_a,
        ))
        .await,
        "reissue_upload_url",
    );
    assert_cross_project_not_found(
        svc.download_file(storage_project_request(
            storage_pb::DownloadFileRequest {
                tenant_id: tenant_id.clone(),
                file_id: file_b.file_id.clone(),
                ..Default::default()
            },
            &tenant_id,
            &project_a,
        ))
        .await,
        "download_file",
    );
    assert_cross_project_not_found(
        svc.finalize_upload(storage_project_request(
            storage_pb::FinalizeUploadRequest {
                tenant_id: tenant_id.clone(),
                file_id: file_b.file_id.clone(),
                ..Default::default()
            },
            &tenant_id,
            &project_a,
        ))
        .await,
        "finalize_upload",
    );
    assert_cross_project_not_found(
        svc.update_file(storage_project_request(
            storage_pb::UpdateFileRequest {
                tenant_id: tenant_id.clone(),
                file_id: file_b.file_id.clone(),
                filename: "forbidden.txt".to_string(),
                ..Default::default()
            },
            &tenant_id,
            &project_a,
        ))
        .await,
        "update_file",
    );
    assert_cross_project_not_found(
        svc.delete_file(storage_project_request(
            storage_pb::DeleteFileRequest {
                tenant_id: tenant_id.clone(),
                file_id: file_b.file_id.clone(),
                ..Default::default()
            },
            &tenant_id,
            &project_a,
        ))
        .await,
        "delete_file_soft",
    );
    assert_cross_project_not_found(
        svc.delete_file(storage_project_request(
            storage_pb::DeleteFileRequest {
                tenant_id: tenant_id.clone(),
                file_id: file_b.file_id.clone(),
                mode: storage_pb::DeleteMode::Hard as i32,
                idempotency_key: Uuid::new_v4().to_string(),
                ..Default::default()
            },
            &tenant_id,
            &project_a,
        ))
        .await,
        "delete_file_hard",
    );

    let tenant_wide_b = svc
        .get_file(Request::new(storage_pb::GetFileRequest {
            tenant_id: tenant_id.clone(),
            file_id: file_b.file_id.clone(),
        }))
        .await
        .expect("tenant-wide credential may read project B")
        .into_inner()
        .file
        .expect("project B file");
    assert_eq!(tenant_wide_b.project_id, project_b);
    let tenant_wide_list = svc
        .list_files(Request::new(storage_pb::ListFilesRequest {
            tenant_id,
            ..Default::default()
        }))
        .await
        .expect("tenant-wide credential may list both projects")
        .into_inner();
    assert_eq!(tenant_wide_list.total_count, 2);

    cleanup_native_service_db(&pool).await;
}
