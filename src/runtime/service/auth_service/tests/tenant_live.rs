use super::support::*;
use crate::proto::udb::core::tenant::entity::v1 as tenant_entity_pb;
use crate::proto::udb::core::tenant::services::v1 as tenant_pb;
use crate::proto::udb::core::tenant::services::v1::tenant_service_server::TenantService;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_tenant_native_schema_from_proto -- --ignored --nocapture"]
async fn live_postgres_tenant_native_schema_from_proto() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    assert_native_table_columns(
        &pool,
        "udb.core.tenant.entity.v1.Tenant",
        &[
            "tenant_id",
            "code",
            "name",
            "type",
            "status",
            "config",
            "branding",
            "audit_info",
            "deleted_at",
            "deleted_by",
        ],
    )
    .await;
    assert_native_table_columns(
        &pool,
        "udb.core.tenant.entity.v1.TenantConfig",
        &[
            "id",
            "tenant_id",
            "config_key",
            "config_value",
            "type",
            "description",
            "audit_info",
        ],
    )
    .await;

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_tenant_service_crud_roundtrip -- --ignored --nocapture"]
async fn live_postgres_tenant_service_crud_roundtrip() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = tenant_service(pool.clone()).await;

    // Default tenant seed (create_tenant under the hood).
    let tenant_id = seed_default_tenant(&pool).await;

    let got = svc
        .get_tenant(Request::new(tenant_pb::GetTenantRequest {
            tenant_id: tenant_id.clone(),
        }))
        .await
        .expect("get seeded tenant")
        .into_inner()
        .tenant
        .expect("tenant");
    assert_eq!(got.tenant_id, tenant_id);
    assert_eq!(
        got.r#type,
        tenant_entity_pb::TenantType::Organization as i32
    );
    assert_eq!(got.status, tenant_entity_pb::TenantStatus::Active as i32);
    assert!(got.config.contains("enterprise"));

    // A second tenant, then list with the ACTIVE filter.
    svc.create_tenant(Request::new(tenant_pb::CreateTenantRequest {
        code: format!("beta_{}", Uuid::new_v4().simple()),
        name: "Beta Workspace".to_string(),
        r#type: "WORKSPACE".to_string(),
        ..Default::default()
    }))
    .await
    .expect("create second tenant");
    let listed = svc
        .list_tenants(Request::new(tenant_pb::ListTenantsRequest {
            status: "ACTIVE".to_string(),
            page: 1,
            page_size: 50,
            ..Default::default()
        }))
        .await
        .expect("list tenants")
        .into_inner();
    assert!(listed.total_count >= 2, "expected >= 2 active tenants");
    assert!(listed.tenants.iter().any(|t| t.tenant_id == tenant_id));

    // Partial update: rename + suspend + new config.
    svc.update_tenant(Request::new(tenant_pb::UpdateTenantRequest {
        tenant_id: tenant_id.clone(),
        name: "Acme Renamed".to_string(),
        status: "SUSPENDED".to_string(),
        config: r#"{"plan":"pro"}"#.to_string(),
        ..Default::default()
    }))
    .await
    .expect("update tenant");
    let updated = svc
        .get_tenant(Request::new(tenant_pb::GetTenantRequest {
            tenant_id: tenant_id.clone(),
        }))
        .await
        .expect("get updated tenant")
        .into_inner()
        .tenant
        .expect("tenant");
    assert_eq!(updated.name, "Acme Renamed");
    assert_eq!(
        updated.status,
        tenant_entity_pb::TenantStatus::Suspended as i32
    );
    assert!(updated.config.contains("pro"));

    // Tenant config upsert is idempotent on (tenant_id, config_key).
    for value in ["100", "200"] {
        svc.update_tenant_config(Request::new(tenant_pb::UpdateTenantConfigRequest {
            tenant_id: tenant_id.clone(),
            config_key: "max_users".to_string(),
            config_value: value.to_string(),
            r#type: "NUMBER".to_string(),
        }))
        .await
        .expect("upsert tenant config");
    }
    let configs = svc
        .get_tenant_config(Request::new(tenant_pb::GetTenantConfigRequest {
            tenant_id: tenant_id.clone(),
        }))
        .await
        .expect("get tenant config")
        .into_inner()
        .configs;
    assert_eq!(configs.len(), 1, "upsert must not duplicate the config key");
    assert_eq!(configs[0].config_value, "200");

    cleanup_native_auth_db(&pool).await;
}
