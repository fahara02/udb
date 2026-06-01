use super::support::*;
use crate::proto::udb::core::apikey::entity::v1 as apikey_entity_pb;
use crate::proto::udb::core::apikey::services::v1 as apikey_pb;
use crate::proto::udb::core::apikey::services::v1::api_key_service_server::ApiKeyService;
use crate::proto::udb::core::common::v1 as common_pb;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_apikey_roundtrip -- --ignored --nocapture"]
async fn live_postgres_apikey_roundtrip() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = api_key_service(pool.clone());
    let principal_id = Uuid::new_v4().to_string();

    let created = svc
        .create_api_key(Request::new(apikey_pb::CreateApiKeyRequest {
            name: "live-key".to_string(),
            owner_id: principal_id.clone(),
            scopes: vec!["data:read".to_string()],
            context: Some(common_pb::RequestContext {
                principal_id: principal_id.clone(),
                tenant: Some(common_pb::TenantContext {
                    tenant_id: "acme".to_string(),
                    project_id: "billing".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .expect("create API key through Postgres store")
        .into_inner();
    let key = created.key.expect("created API key");
    assert!(created.plain_key.starts_with("udbk_"));

    let valid = svc
        .validate_api_key(Request::new(apikey_pb::ValidateApiKeyRequest {
            plain_key: created.plain_key.clone(),
            required_scope: "data:read".to_string(),
            ..Default::default()
        }))
        .await
        .expect("validate API key through Postgres store")
        .into_inner();
    assert!(valid.valid);
    assert_eq!(valid.owner_id, principal_id);

    svc.revoke_api_key(Request::new(apikey_pb::RevokeApiKeyRequest {
        key_id: key.key_id,
        revoke_reason: "live_test".to_string(),
        ..Default::default()
    }))
    .await
    .expect("revoke API key through Postgres store");

    let invalid = svc
        .validate_api_key(Request::new(apikey_pb::ValidateApiKeyRequest {
            plain_key: created.plain_key,
            ..Default::default()
        }))
        .await
        .expect("validate revoked API key")
        .into_inner();
    assert!(!invalid.valid);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_apikey_admin_lifecycle -- --ignored --nocapture"]
async fn live_postgres_apikey_admin_lifecycle() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = api_key_service(pool.clone());
    let owner_id = Uuid::new_v4().to_string();

    let created = svc
        .create_api_key(Request::new(apikey_pb::CreateApiKeyRequest {
            name: "admin-live-key".to_string(),
            owner_type: apikey_entity_pb::ApiKeyOwnerType::ServiceAccount as i32,
            owner_id: owner_id.clone(),
            scopes: vec!["data:read".to_string()],
            context: Some(common_pb::RequestContext {
                principal_id: owner_id.clone(),
                tenant: Some(common_pb::TenantContext {
                    tenant_id: "acme".to_string(),
                    project_id: "billing".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .expect("create admin API key")
        .into_inner();
    let key_id = created.key.as_ref().expect("created key").key_id.clone();

    let missing_scope = svc
        .validate_api_key(Request::new(apikey_pb::ValidateApiKeyRequest {
            plain_key: created.plain_key.clone(),
            required_scope: "data:write".to_string(),
            ..Default::default()
        }))
        .await
        .expect("validate missing scope")
        .into_inner();
    assert!(!missing_scope.valid);

    let listed = svc
        .list_api_keys(Request::new(apikey_pb::ListApiKeysRequest {
            owner_id: owner_id.clone(),
            status: apikey_entity_pb::ApiKeyStatus::Active as i32,
            ..Default::default()
        }))
        .await
        .expect("list active API keys")
        .into_inner();
    assert_eq!(listed.keys.len(), 1);
    assert_eq!(listed.keys[0].key_id, key_id);

    let updated = svc
        .update_api_key(Request::new(apikey_pb::UpdateApiKeyRequest {
            key_id: key_id.clone(),
            scopes: vec!["data:read".to_string(), "data:write".to_string()],
            ..Default::default()
        }))
        .await
        .expect("update API key scopes")
        .into_inner()
        .key
        .expect("updated key");
    assert!(updated.scopes_json.contains("data:write"));

    let write_ok = svc
        .validate_api_key(Request::new(apikey_pb::ValidateApiKeyRequest {
            plain_key: created.plain_key,
            required_scope: "data:write".to_string(),
            ..Default::default()
        }))
        .await
        .expect("validate updated scope")
        .into_inner();
    assert!(write_ok.valid);
    assert_eq!(write_ok.owner_id, owner_id);

    let got = svc
        .get_api_key(Request::new(apikey_pb::GetApiKeyRequest { key_id }))
        .await
        .expect("get API key")
        .into_inner()
        .key
        .expect("got key");
    assert_eq!(got.owner_id, owner_id);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_apikey_usage_stats_contract -- --ignored --nocapture"]
async fn live_postgres_apikey_usage_stats_contract() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = api_key_service(pool.clone());

    let stats = svc
        .get_api_key_usage_stats(Request::new(apikey_pb::GetApiKeyUsageStatsRequest {
            key_id: Uuid::new_v4().to_string(),
            ..Default::default()
        }))
        .await
        .expect("usage stats endpoint reads the live Postgres usage table")
        .into_inner();
    assert_eq!(stats.total_requests, 0);
    assert!(stats.stats.is_empty());

    cleanup_native_auth_db(&pool).await;
}
