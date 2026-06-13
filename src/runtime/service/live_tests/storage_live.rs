use super::support::*;
use crate::proto::udb::core::storage::services::v1 as storage_pb;
use crate::proto::udb::core::storage::services::v1::storage_service_server::StorageService;
use tonic::Request;
use uuid::Uuid;

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
            size_bytes: 2048,
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
            size_bytes: 2048,
            ..Default::default()
        }))
        .await
        .expect("finalize_upload")
        .into_inner();
    let file = fin.file.expect("finalized file");
    assert_eq!(file.file_id, reg.file_id);
    assert!(file.is_public);
    assert_eq!(
        file.size_bytes, 2048,
        "finalize must persist the actual size"
    );

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
