use super::support::*;
use crate::proto::udb::core::authz::services::v1 as authz_pb;
use crate::proto::udb::core::authz::services::v1::authz_service_server::AuthzService;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authz_relationship_tuple_roundtrip -- --ignored --nocapture"]
async fn live_postgres_authz_relationship_tuple_roundtrip() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let authz = authz_service(pool.clone()).await;
    let user = create_verified_user(&authn, "rebac", "CorrectHorse1!").await;
    let object = format!("invoice:{}", Uuid::new_v4().simple());
    let policy_id = Uuid::new_v4().to_string();

    authz
        .put_relationship(Request::new(authz_pb::PutRelationshipRequest {
            tuple: Some(authz_pb::RelationshipTuple {
                subject: user.user_id.clone(),
                relation: "owner".to_string(),
                object: object.clone(),
                tenant: "acme".to_string(),
                project: "billing".to_string(),
                version: 1,
                expires_at_unix: now_unix() as i64 + 3600,
                source: "live_test".to_string(),
            }),
        }))
        .await
        .expect("put relationship tuple");

    authz
        .put_authz_policy(Request::new(authz_pb::PutAuthzPolicyRequest {
            policy: Some(authz_pb::AuthzPolicyRecord {
                id: policy_id.clone(),
                enabled: true,
                effect: "allow".to_string(),
                tenant: "acme".to_string(),
                project: "billing".to_string(),
                action: "data.select".to_string(),
                resource: object.clone(),
                relationship: "owner".to_string(),
                ..Default::default()
            }),
        }))
        .await
        .expect("put ReBAC policy");

    let allowed = authz
        .check_access(Request::new(authz_pb::CheckAccessRequest {
            user_id: user.user_id.clone(),
            domain: "acme".to_string(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            object: object.clone(),
            action: "data.select".to_string(),
            ..Default::default()
        }))
        .await
        .expect("check relationship-owned object")
        .into_inner();
    assert!(allowed.allowed);
    assert_eq!(allowed.matched_rule, policy_id);

    let other_object = authz
        .check_access(Request::new(authz_pb::CheckAccessRequest {
            user_id: user.user_id.clone(),
            domain: "acme".to_string(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            object: "invoice:not-owned".to_string(),
            action: "data.select".to_string(),
            ..Default::default()
        }))
        .await
        .expect("check non-owned object")
        .into_inner();
    assert!(!other_object.allowed);

    let listed = authz
        .list_policy_rules(Request::new(authz_pb::ListPolicyRulesRequest {
            domain: "acme".to_string(),
            object,
            active_only: true,
            ..Default::default()
        }))
        .await
        .expect("list ReBAC policy")
        .into_inner();
    assert_eq!(listed.policies.len(), 1);
    assert_eq!(listed.policies[0].policy_id, policy_id);

    cleanup_native_auth_db(&pool).await;
}
