//! Live Postgres acceptance for typed service-account grants and certificate
//! bindings. These tests intentionally exercise the RPC/store boundaries used
//! by password login, API keys, refresh/JWT validation, and mTLS resolution.

use super::support::*;
use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::proto::data_broker_client::DataBrokerClient;
use crate::proto::data_broker_server::DataBrokerServer;
use crate::proto::udb::core::apikey::services::v1 as apikey_pb;
use crate::proto::udb::core::apikey::services::v1::api_key_service_server::ApiKeyService;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_client::AuthnServiceClient;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnServiceServer;
use crate::proto::udb::core::common::v1 as common_pb;
use crate::runtime::service::method_security::{
    MethodSecurityLayer, scope_claim_context_for_test, test_claim_context,
};
use std::sync::{Arc, RwLock};
use tonic::Request;

fn login_request(username: &str, password: &str) -> authn_pb::LoginRequest {
    authn_pb::LoginRequest {
        username: username.to_string(),
        password: password.to_string(),
        device_name: "typed-grant-live-test".to_string(),
        tenant_hint: "acme".to_string(),
        project_hint: "billing".to_string(),
        ..Default::default()
    }
}

async fn validate_access_token(
    svc: &super::super::AuthnServiceImpl,
    token: String,
) -> authn_pb::ValidateTokenResponse {
    svc.validate_token(Request::new(authn_pb::ValidateTokenRequest {
        token,
        token_type: authn_entity_pb::TokenType::JwtAccess as i32,
    }))
    .await
    .expect("validate service access token")
    .into_inner()
}

fn certificate_der(spiffe_uri: &str, dns_name: &str) -> Vec<u8> {
    let mut params =
        rcgen::CertificateParams::new(vec![dns_name.to_string()]).expect("valid test DNS SAN");
    params.subject_alt_names.insert(
        0,
        rcgen::SanType::URI(spiffe_uri.try_into().expect("valid test SPIFFE URI")),
    );
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, dns_name);
    let key_pair = rcgen::KeyPair::generate().expect("generate test certificate key");
    params
        .self_signed(&key_pair)
        .expect("self-sign test certificate")
        .der()
        .to_vec()
}

struct MtlsTestMaterial {
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
    client_der: Vec<u8>,
    unknown_cert_pem: String,
    unknown_key_pem: String,
}

fn mtls_test_material(registered_spiffe: &str) -> MtlsTestMaterial {
    let ca_key = rcgen::KeyPair::generate().expect("generate test CA key");
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new())
        .expect("test CA certificate parameters");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "UDB fix-plan test CA");
    let ca_cert = ca_params.self_signed(&ca_key).expect("self-sign test CA");

    let server_key = rcgen::KeyPair::generate().expect("generate test server key");
    let mut server_params =
        rcgen::CertificateParams::new(vec!["localhost".to_string()]).expect("server parameters");
    server_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "localhost");
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .expect("sign test server certificate");

    let client_key = rcgen::KeyPair::generate().expect("generate registered client key");
    let mut client_params = rcgen::CertificateParams::new(vec!["registered.test.udb".to_string()])
        .expect("registered client parameters");
    client_params.subject_alt_names.insert(
        0,
        rcgen::SanType::URI(registered_spiffe.try_into().expect("registered SPIFFE URI")),
    );
    client_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "registered.test.udb");
    let client_cert = client_params
        .signed_by(&client_key, &ca_cert, &ca_key)
        .expect("sign registered client certificate");

    let unknown_key = rcgen::KeyPair::generate().expect("generate unknown client key");
    let mut unknown_params = rcgen::CertificateParams::new(vec!["unknown.test.udb".to_string()])
        .expect("unknown client parameters");
    unknown_params.subject_alt_names.insert(
        0,
        rcgen::SanType::URI(
            "spiffe://test.udb/unknown"
                .try_into()
                .expect("unknown SPIFFE URI"),
        ),
    );
    let unknown_cert = unknown_params
        .signed_by(&unknown_key, &ca_cert, &ca_key)
        .expect("sign unknown client certificate");

    MtlsTestMaterial {
        ca_pem: ca_cert.pem(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
        client_cert_pem: client_cert.pem(),
        client_key_pem: client_key.serialize_pem(),
        client_der: client_cert.der().to_vec(),
        unknown_cert_pem: unknown_cert.pem(),
        unknown_key_pem: unknown_key.serialize_pem(),
    }
}

fn validate_token_request(token: &str) -> Request<authn_pb::ValidateTokenRequest> {
    let mut request = Request::new(authn_pb::ValidateTokenRequest {
        token: token.to_string(),
        token_type: authn_entity_pb::TokenType::JwtAccess as i32,
    });
    request.metadata_mut().insert(
        "x-correlation-id",
        "mtls-served-acceptance".parse().unwrap(),
    );
    request
}

async fn connect_mtls_authn(
    address: std::net::SocketAddr,
    ca_pem: &str,
    client_cert_pem: &str,
    client_key_pem: &str,
) -> AuthnServiceClient<tonic::transport::Channel> {
    let tls = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(ca_pem))
        .identity(tonic::transport::Identity::from_pem(
            client_cert_pem,
            client_key_pem,
        ))
        .domain_name("localhost");
    let channel = tonic::transport::Endpoint::from_shared(format!("https://{address}"))
        .expect("mTLS test endpoint")
        .tls_config(tls)
        .expect("mTLS client config")
        .connect()
        .await
        .expect("connect mTLS Authn client");
    AuthnServiceClient::new(channel)
}

fn api_key_data_request<T>(message: T, plain_key: &str) -> Request<T> {
    let mut request = Request::new(message);
    let metadata = request.metadata_mut();
    metadata.insert("x-api-key", plain_key.parse().expect("API key metadata"));
    metadata.insert("x-tenant-id", "acme".parse().unwrap());
    metadata.insert("x-udb-project-id", "billing".parse().unwrap());
    metadata.insert("x-purpose", "typed-grant-acceptance".parse().unwrap());
    metadata.insert(
        "x-correlation-id",
        "api-key-data-only-listener".parse().unwrap(),
    );
    request
}

fn bearer_data_request<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    let metadata = request.metadata_mut();
    metadata.insert(
        "authorization",
        format!("Bearer {token}")
            .parse()
            .expect("bearer authorization metadata"),
    );
    metadata.insert("x-tenant-id", "acme".parse().unwrap());
    metadata.insert("x-udb-project-id", "billing".parse().unwrap());
    metadata.insert("x-purpose", "typed-grant-acceptance".parse().unwrap());
    metadata.insert(
        "x-correlation-id",
        "bearer-data-only-listener".parse().unwrap(),
    );
    request
}

fn bearer_native_request<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}")
            .parse()
            .expect("bearer authorization metadata"),
    );
    request.metadata_mut().insert(
        "x-correlation-id",
        "grant-management-served-acceptance".parse().unwrap(),
    );
    request
}

fn api_key_crud_manifest() -> CatalogManifest {
    let mut tables = Vec::new();
    for (package, schema) in [
        ("ambulife.authn.entity.v1", "fix_ambulife_authn"),
        ("udb.core.authn.entity.v1", "fix_udb_authn"),
    ] {
        for (message, physical) in [("OTP", "otps"), ("User", "users"), ("Session", "sessions")] {
            tables.push(ManifestTable {
                message_name: message.to_string(),
                proto_package: package.to_string(),
                schema: schema.to_string(),
                table: physical.to_string(),
                primary_key: vec!["id".to_string()],
                columns: vec![
                    ManifestColumn {
                        field_name: "id".to_string(),
                        column_name: "id".to_string(),
                        proto_type: "string".to_string(),
                        sql_type: "TEXT".to_string(),
                        is_primary: true,
                        not_null: true,
                        ..ManifestColumn::default()
                    },
                    ManifestColumn {
                        field_name: "status".to_string(),
                        column_name: "status".to_string(),
                        proto_type: "string".to_string(),
                        sql_type: "TEXT".to_string(),
                        not_null: true,
                        ..ManifestColumn::default()
                    },
                ],
                ..ManifestTable::default()
            });
        }
    }
    CatalogManifest {
        checksum_sha256: "api-key-data-only-listener".to_string(),
        tables,
        ..CatalogManifest::default()
    }
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_typed_grant_login_and_boundaries -- --ignored --nocapture"]
async fn live_postgres_typed_grant_login_and_boundaries() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service_with_jwt(pool.clone());
    let password = "CorrectHorse1!";
    let (user, grant) = create_service_account_with_grant(
        &svc,
        "typed_grant",
        password,
        &["udb:read", "udb:write"],
    )
    .await;

    let login = svc
        .login(Request::new(login_request(&user.email, password)))
        .await
        .expect("typed-grant service login")
        .into_inner();
    let validated = validate_access_token(&svc, login.access_token).await;
    assert!(validated.valid);
    assert_eq!(validated.user_id, user.user_id);
    assert_eq!(validated.tenant_id, "acme");
    assert_eq!(validated.project_id, "billing");
    assert_eq!(validated.scopes, vec!["udb:read", "udb:write"]);
    assert_eq!(
        validated
            .principal
            .as_ref()
            .expect("service principal")
            .service_identity,
        grant.service_identity
    );

    let mut attenuated_request = Request::new(login_request(&user.email, password));
    attenuated_request
        .metadata_mut()
        .insert("x-scopes", "udb:read".parse().unwrap());
    let attenuated_login = svc
        .login(attenuated_request)
        .await
        .expect("grant subset login")
        .into_inner();
    let attenuated = validate_access_token(&svc, attenuated_login.access_token.clone()).await;
    assert!(attenuated.valid);
    assert_eq!(attenuated.scopes, vec!["udb:read"]);
    let refreshed = svc
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            session_id: attenuated_login.session_id,
            ..Default::default()
        }))
        .await
        .expect("refresh attenuated service session")
        .into_inner();
    let refreshed = validate_access_token(&svc, refreshed.access_token).await;
    assert!(refreshed.valid);
    assert_eq!(
        refreshed.scopes,
        vec!["udb:read"],
        "refresh must preserve the service session's attenuation"
    );

    let mut widened_request = Request::new(login_request(&user.email, password));
    widened_request
        .metadata_mut()
        .insert("x-scopes", "udb:delete".parse().unwrap());
    let widened = svc
        .login(widened_request)
        .await
        .expect_err("service login must not widen beyond the typed grant");
    assert_eq!(widened.code(), tonic::Code::PermissionDenied);

    let wildcard = svc
        .create_service_account_grant(Request::new(authn_pb::CreateServiceAccountGrantRequest {
            tenant_id: "acme".to_string(),
            user_id: user.user_id.clone(),
            service_identity: "spiffe://test.udb/forbidden".to_string(),
            project_id: "billing".to_string(),
            approved_scopes: vec!["udb:*".to_string()],
            reason: "must fail".to_string(),
        }))
        .await
        .expect_err("wildcard service grants must be rejected before persistence");
    assert_eq!(wildcard.code(), tonic::Code::PermissionDenied);

    macro_rules! assert_cross_tenant_denied {
        ($message:literal, $future:expr) => {{
            let error = scope_claim_context_for_test(
                test_claim_context(
                    "tenant-b-admin",
                    "tenant-b",
                    "billing",
                    &["udb:authn:read-grants", "udb:authn:manage-grants"],
                    &[],
                ),
                $future,
            )
            .await
            .expect_err($message);
            assert_eq!(error.code(), tonic::Code::PermissionDenied, $message);
        }};
    }

    assert_cross_tenant_denied!(
        "tenant-B claim must not create a tenant-A grant",
        svc.create_service_account_grant(Request::new(
            authn_pb::CreateServiceAccountGrantRequest {
                tenant_id: "acme".to_string(),
                user_id: user.user_id.clone(),
                service_identity: "spiffe://test.udb/cross-tenant".to_string(),
                project_id: "billing".to_string(),
                approved_scopes: vec!["udb:read".to_string()],
                reason: "must fail".to_string(),
            },
        ))
    );
    assert_cross_tenant_denied!(
        "tenant-B claim must not read a tenant-A grant",
        svc.get_service_account_grant(Request::new(authn_pb::GetServiceAccountGrantRequest {
            tenant_id: "acme".to_string(),
            user_id: user.user_id.clone(),
        }))
    );
    assert_cross_tenant_denied!(
        "tenant-B claim must not list tenant-A grants",
        svc.list_service_account_grants(Request::new(authn_pb::ListServiceAccountGrantsRequest {
            tenant_id: "acme".to_string(),
            page_size: 50,
            page_token: String::new(),
        },))
    );
    assert_cross_tenant_denied!(
        "tenant-B claim must not replace a tenant-A grant",
        svc.replace_service_account_grant(Request::new(
            authn_pb::ReplaceServiceAccountGrantRequest {
                tenant_id: "acme".to_string(),
                user_id: user.user_id.clone(),
                approved_scopes: vec!["udb:read".to_string()],
                project_id: "billing".to_string(),
                reason: "must fail".to_string(),
                expected_revision: grant.revision,
            },
        ))
    );
    assert_cross_tenant_denied!(
        "tenant-B claim must not rotate a tenant-A service identity",
        svc.rotate_service_account_identity(Request::new(
            authn_pb::RotateServiceAccountIdentityRequest {
                tenant_id: "acme".to_string(),
                user_id: user.user_id.clone(),
                new_service_identity: "spiffe://test.udb/cross-tenant".to_string(),
                expected_revision: grant.revision,
                reason: "must fail".to_string(),
            },
        ))
    );
    assert_cross_tenant_denied!(
        "tenant-B claim must not revoke a tenant-A grant",
        svc.revoke_service_account_grant(Request::new(
            authn_pb::RevokeServiceAccountGrantRequest {
                tenant_id: "acme".to_string(),
                user_id: user.user_id.clone(),
                reason: "must fail".to_string(),
            },
        ))
    );
    assert_cross_tenant_denied!(
        "tenant-B claim must not create a tenant-A certificate binding",
        svc.create_certificate_binding(Request::new(authn_pb::CreateCertificateBindingRequest {
            tenant_id: "acme".to_string(),
            user_id: user.user_id.clone(),
            selector_kind: "DNS_SAN".to_string(),
            selector_value: "cross-tenant.test.udb".to_string(),
            scope_subset: vec!["udb:read".to_string()],
            reason: "must fail".to_string(),
            not_before: None,
            not_after: None,
        },))
    );
    assert_cross_tenant_denied!(
        "tenant-B claim must not list tenant-A certificate bindings",
        svc.list_certificate_bindings(Request::new(authn_pb::ListCertificateBindingsRequest {
            tenant_id: "acme".to_string(),
            page_size: 50,
            page_token: String::new(),
        },))
    );
    assert_cross_tenant_denied!(
        "tenant-B claim must not revoke a tenant-A certificate binding",
        svc.revoke_certificate_binding(Request::new(authn_pb::RevokeCertificateBindingRequest {
            tenant_id: "acme".to_string(),
            binding_id: "00000000-0000-0000-0000-000000000000".to_string(),
            reason: "must fail".to_string(),
        },))
    );

    let cross_project = scope_claim_context_for_test(
        test_claim_context(
            "project-b-admin",
            "acme",
            "project-b",
            &["udb:authn:manage-grants"],
            &[],
        ),
        svc.replace_service_account_grant(Request::new(
            authn_pb::ReplaceServiceAccountGrantRequest {
                tenant_id: "acme".to_string(),
                user_id: user.user_id.clone(),
                approved_scopes: vec!["udb:read".to_string()],
                project_id: "billing".to_string(),
                reason: "must not cross projects".to_string(),
                expected_revision: grant.revision,
            },
        )),
    )
    .await
    .expect_err("project-B claim must not mutate a project-billing grant");
    assert_eq!(cross_project.code(), tonic::Code::PermissionDenied);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_served_grant_management_binds_every_rpc_to_claim_tenant -- --ignored --nocapture"]
async fn live_postgres_served_grant_management_binds_every_rpc_to_claim_tenant() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let (owner, grant) = create_service_account_with_grant(
        &authn,
        "served_grant_admin",
        "CorrectHorse1!",
        &["udb:read"],
    )
    .await;

    let security = crate::runtime::security::SecurityConfig {
        jwt_private_key: Some(include_str!("../../../testdata/jwt_rs256_private.pem").to_string()),
        jwt_public_key: Some(include_str!("../../../testdata/jwt_rs256_public.pem").to_string()),
        ..crate::runtime::security::SecurityConfig::default()
    };
    let scopes = vec![
        "udb:authn:read-grants".to_string(),
        "udb:authn:manage-grants".to_string(),
    ];
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_secs();
    let tenant_a_token = crate::runtime::security::sign_access_token(
        &security,
        "tenant-a-grant-admin",
        "acme",
        "billing",
        &scopes,
        &[],
        "",
        "served-grant-admin-a",
        "password",
        now,
    )
    .expect("sign tenant-A grant admin token")
    .expect("tenant-A signing key configured")
    .0;
    let tenant_b_token = crate::runtime::security::sign_access_token(
        &security,
        "tenant-b-grant-admin",
        "tenant-b",
        "billing",
        &scopes,
        &[],
        "",
        "served-grant-admin-b",
        "password",
        now,
    )
    .expect("sign tenant-B grant admin token")
    .expect("tenant-B signing key configured")
    .0;
    crate::runtime::security::SecurityConfig::install_global(security);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind served grant-management listener");
    let address = listener
        .local_addr()
        .expect("served grant-management listener address");
    let incoming = futures::stream::unfold(listener, |listener| async move {
        let connection = listener.accept().await.map(|(stream, _)| stream);
        Some((connection, listener))
    });
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(crate::runtime::credential_layer::CredentialResolveLayer::new())
            .add_service(MethodSecurityLayer::new().wrap(AuthnServiceServer::new(authn)))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve grant-management Authn listener");
    });
    let mut client = AuthnServiceClient::connect(format!("http://{address}"))
        .await
        .expect("connect grant-management client");

    let positive = client
        .get_service_account_grant(bearer_native_request(
            authn_pb::GetServiceAccountGrantRequest {
                tenant_id: "acme".to_string(),
                user_id: owner.user_id.clone(),
            },
            &tenant_a_token,
        ))
        .await
        .expect("tenant-A bearer reads tenant-A grant")
        .into_inner()
        .grant
        .expect("served grant response");
    assert_eq!(positive.grant_id, grant.grant_id);

    macro_rules! assert_served_cross_tenant_denied {
        ($message:literal, $call:expr) => {{
            let error = $call.await.expect_err($message);
            assert_eq!(error.code(), tonic::Code::PermissionDenied, $message);
        }};
    }
    assert_served_cross_tenant_denied!(
        "served create grant must reject a foreign body tenant",
        client.create_service_account_grant(bearer_native_request(
            authn_pb::CreateServiceAccountGrantRequest {
                tenant_id: "acme".to_string(),
                user_id: owner.user_id.clone(),
                service_identity: "spiffe://test.udb/served-cross-tenant".to_string(),
                project_id: "billing".to_string(),
                approved_scopes: vec!["udb:read".to_string()],
                reason: "must fail".to_string(),
            },
            &tenant_b_token,
        ))
    );
    assert_served_cross_tenant_denied!(
        "served get grant must reject a foreign body tenant",
        client.get_service_account_grant(bearer_native_request(
            authn_pb::GetServiceAccountGrantRequest {
                tenant_id: "acme".to_string(),
                user_id: owner.user_id.clone(),
            },
            &tenant_b_token,
        ))
    );
    assert_served_cross_tenant_denied!(
        "served list grants must reject a foreign body tenant",
        client.list_service_account_grants(bearer_native_request(
            authn_pb::ListServiceAccountGrantsRequest {
                tenant_id: "acme".to_string(),
                page_size: 50,
                page_token: String::new(),
            },
            &tenant_b_token,
        ))
    );
    assert_served_cross_tenant_denied!(
        "served replace grant must reject a foreign body tenant",
        client.replace_service_account_grant(bearer_native_request(
            authn_pb::ReplaceServiceAccountGrantRequest {
                tenant_id: "acme".to_string(),
                user_id: owner.user_id.clone(),
                approved_scopes: vec!["udb:read".to_string()],
                project_id: "billing".to_string(),
                reason: "must fail".to_string(),
                expected_revision: grant.revision,
            },
            &tenant_b_token,
        ))
    );
    assert_served_cross_tenant_denied!(
        "served identity rotation must reject a foreign body tenant",
        client.rotate_service_account_identity(bearer_native_request(
            authn_pb::RotateServiceAccountIdentityRequest {
                tenant_id: "acme".to_string(),
                user_id: owner.user_id.clone(),
                new_service_identity: "spiffe://test.udb/served-cross-tenant".to_string(),
                expected_revision: grant.revision,
                reason: "must fail".to_string(),
            },
            &tenant_b_token,
        ))
    );
    assert_served_cross_tenant_denied!(
        "served revoke grant must reject a foreign body tenant",
        client.revoke_service_account_grant(bearer_native_request(
            authn_pb::RevokeServiceAccountGrantRequest {
                tenant_id: "acme".to_string(),
                user_id: owner.user_id.clone(),
                reason: "must fail".to_string(),
            },
            &tenant_b_token,
        ))
    );
    assert_served_cross_tenant_denied!(
        "served create binding must reject a foreign body tenant",
        client.create_certificate_binding(bearer_native_request(
            authn_pb::CreateCertificateBindingRequest {
                tenant_id: "acme".to_string(),
                user_id: owner.user_id.clone(),
                selector_kind: "DNS_SAN".to_string(),
                selector_value: "served-cross-tenant.test.udb".to_string(),
                scope_subset: vec!["udb:read".to_string()],
                reason: "must fail".to_string(),
                not_before: None,
                not_after: None,
            },
            &tenant_b_token,
        ))
    );
    assert_served_cross_tenant_denied!(
        "served list bindings must reject a foreign body tenant",
        client.list_certificate_bindings(bearer_native_request(
            authn_pb::ListCertificateBindingsRequest {
                tenant_id: "acme".to_string(),
                page_size: 50,
                page_token: String::new(),
            },
            &tenant_b_token,
        ))
    );
    assert_served_cross_tenant_denied!(
        "served revoke binding must reject a foreign body tenant",
        client.revoke_certificate_binding(bearer_native_request(
            authn_pb::RevokeCertificateBindingRequest {
                tenant_id: "acme".to_string(),
                binding_id: "00000000-0000-0000-0000-000000000000".to_string(),
                reason: "must fail".to_string(),
            },
            &tenant_b_token,
        ))
    );

    let _ = shutdown_tx.send(());
    server.await.expect("join grant-management listener");
    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_api_key_crud_on_data_only_listener -- --ignored --nocapture"]
async fn live_postgres_api_key_crud_on_data_only_listener() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    sqlx::query("DROP SCHEMA IF EXISTS fix_ambulife_authn CASCADE")
        .execute(&pool)
        .await
        .expect("drop prior AmbuLife acceptance schema");
    sqlx::query("DROP SCHEMA IF EXISTS fix_udb_authn CASCADE")
        .execute(&pool)
        .await
        .expect("drop prior UDB acceptance schema");
    sqlx::raw_sql(
        "CREATE SCHEMA fix_ambulife_authn; CREATE SCHEMA fix_udb_authn; \
         CREATE TABLE fix_ambulife_authn.otps (id TEXT PRIMARY KEY, status TEXT NOT NULL); \
         CREATE TABLE fix_udb_authn.otps (id TEXT PRIMARY KEY, status TEXT NOT NULL)",
    )
    .execute(&pool)
    .await
    .expect("create API-key CRUD table");

    let authn = authn_service(pool.clone());
    let apikey = api_key_service(pool.clone());
    let (owner, grant) = create_service_account_with_grant(
        &authn,
        "data_only_key",
        "CorrectHorse1!",
        &["udb:read", "udb:write"],
    )
    .await;
    let created = apikey
        .create_api_key(Request::new(apikey_pb::CreateApiKeyRequest {
            name: "data-only-listener".to_string(),
            owner_id: owner.user_id.clone(),
            scopes: vec!["udb:read".to_string(), "udb:write".to_string()],
            context: Some(common_pb::RequestContext {
                principal_id: owner.user_id.clone(),
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
        .expect("create grant-backed data-plane API key")
        .into_inner();

    let authn_config = crate::runtime::authn::AuthnConfig {
        session_enabled: true,
        session_hash_secret: "live-auth-test-secret".to_string(),
        ..crate::runtime::authn::AuthnConfig::default()
    };
    super::super::install_data_plane_credential_resolvers(pool.clone(), &authn_config);
    let security = crate::runtime::security::SecurityConfig {
        allow_header_scopes: false,
        service_identity_required: true,
        jwt_private_key: Some(include_str!("../../../testdata/jwt_rs256_private.pem").to_string()),
        jwt_public_key: Some(include_str!("../../../testdata/jwt_rs256_public.pem").to_string()),
        ..crate::runtime::security::SecurityConfig::default()
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_secs();
    let sign_bearer = |tenant: &str, project: &str, scopes: &[String], jti: &str| {
        crate::runtime::security::sign_access_token(
            &security,
            &owner.user_id,
            tenant,
            project,
            scopes,
            &[],
            &grant.service_identity,
            jti,
            "service_account",
            now,
        )
        .expect("sign data-only listener bearer")
        .expect("data-only listener signing key configured")
        .0
    };
    let full_bearer = sign_bearer(
        "acme",
        "billing",
        &["udb:read".to_string(), "udb:write".to_string()],
        "data-only-full",
    );
    let read_bearer = sign_bearer(
        "acme",
        "billing",
        &["udb:read".to_string()],
        "data-only-read",
    );
    let missing_tenant_bearer = sign_bearer(
        "",
        "billing",
        &["udb:read".to_string()],
        "data-only-no-tenant",
    );
    let missing_project_bearer = sign_bearer(
        "acme",
        "",
        &["udb:read".to_string()],
        "data-only-no-project",
    );
    crate::runtime::security::SecurityConfig::install_global(security);

    let runtime =
        crate::runtime::DataBrokerRuntime::from_config(crate::runtime::config::UdbConfig {
            primary: crate::runtime::config::DbConfig {
                direct_dsn: live_pg_dsn(),
                ..crate::runtime::config::DbConfig::default()
            },
            ..crate::runtime::config::UdbConfig::default()
        })
        .await;
    let broker = crate::runtime::service::DataBrokerService::with_runtime_and_state(
        api_key_crud_manifest(),
        runtime,
        Arc::new(RwLock::new(crate::FsmState::Completed)),
        Arc::new(crate::runtime::metrics::PrometheusMetrics::new().expect("metrics")),
        None,
        true,
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind data-only listener");
    let address = listener.local_addr().expect("data-only listener address");
    let incoming = futures::stream::unfold(listener, |listener| async move {
        let connection = listener.accept().await.map(|(stream, _)| stream);
        Some((connection, listener))
    });
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(crate::runtime::credential_layer::CredentialResolveLayer::new())
            // Intentionally mount DataBroker only: native Authn/ApiKey services
            // are disabled on this listener and cannot provide resolver wiring.
            .add_service(DataBrokerServer::new(broker))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve data-only DataBroker");
    });
    let mut client = DataBrokerClient::connect(format!("http://{address}"))
        .await
        .expect("connect DataBroker client");
    let context = crate::proto::RequestContext {
        tenant_id: "acme".to_string(),
        project_id: "billing".to_string(),
        purpose: "typed-grant-acceptance".to_string(),
        correlation_id: "api-key-data-only-listener".to_string(),
        service_identity: grant.service_identity.clone(),
        scopes: vec!["udb:read".to_string(), "udb:write".to_string()],
        ..crate::proto::RequestContext::default()
    };

    let select_request = || crate::proto::SelectRequest {
        context: Some(context.clone()),
        message_type: "ambulife.authn.entity.v1.OTP".to_string(),
        limit: 10,
        ..crate::proto::SelectRequest::default()
    };

    let mut subject_mismatch = bearer_data_request(select_request(), &full_bearer);
    subject_mismatch
        .metadata_mut()
        .insert("x-user-id", "different-user".parse().unwrap());
    let denied = client
        .select(subject_mismatch)
        .await
        .expect_err("served JWT subject/header mismatch must fail closed");
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);

    let mut tenant_mismatch = bearer_data_request(select_request(), &full_bearer);
    tenant_mismatch
        .metadata_mut()
        .insert("x-tenant-id", "other-tenant".parse().unwrap());
    let denied = client
        .select(tenant_mismatch)
        .await
        .expect_err("served JWT tenant/header mismatch must fail closed");
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);

    let mut project_mismatch = bearer_data_request(select_request(), &full_bearer);
    project_mismatch
        .metadata_mut()
        .insert("x-udb-project-id", "other-project".parse().unwrap());
    let denied = client
        .select(project_mismatch)
        .await
        .expect_err("served JWT project/header mismatch must fail closed");
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);

    for (token, label) in [
        (&missing_tenant_bearer, "tenant"),
        (&missing_project_bearer, "project"),
    ] {
        let denied = match client
            .select(bearer_data_request(select_request(), token))
            .await
        {
            Ok(_) => panic!("served JWT missing authoritative {label} must fail"),
            Err(status) => status,
        };
        assert_eq!(denied.code(), tonic::Code::Unauthenticated);
    }

    let mut scope_injection = bearer_data_request(
        crate::proto::UpsertRequest {
            context: Some(context.clone()),
            message_type: "ambulife.authn.entity.v1.OTP".to_string(),
            record_json: br#"{"id":"scope-injection","status":"denied"}"#.to_vec(),
            ..crate::proto::UpsertRequest::default()
        },
        &read_bearer,
    );
    scope_injection
        .metadata_mut()
        .insert("x-scopes", "udb:write,udb:admin".parse().unwrap());
    let denied = client
        .upsert(scope_injection)
        .await
        .expect_err("caller scope headers must not widen a served bearer");
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);

    for (message_type, row_id, status) in [
        ("ambulife.authn.entity.v1.OTP", "ambulife-otp", "pending"),
        ("udb.core.authn.entity.v1.OTP", "udb-otp", "verified"),
    ] {
        let upsert = client
            .upsert(api_key_data_request(
                crate::proto::UpsertRequest {
                    context: Some(context.clone()),
                    message_type: message_type.to_string(),
                    record_json: serde_json::to_vec(&serde_json::json!({
                        "id": row_id,
                        "status": status,
                    }))
                    .expect("serialize acceptance row"),
                    return_record: true,
                    ..crate::proto::UpsertRequest::default()
                },
                &created.plain_key,
            ))
            .await
            .expect("exact-FQN API key Upsert over data-only listener")
            .into_inner();
        assert_eq!(upsert.affected_rows, 1);

        let selected = client
            .select(api_key_data_request(
                crate::proto::SelectRequest {
                    context: Some(context.clone()),
                    message_type: message_type.to_string(),
                    limit: 10,
                    ..crate::proto::SelectRequest::default()
                },
                &created.plain_key,
            ))
            .await
            .expect("exact-FQN API key Select over data-only listener")
            .into_inner();
        assert_eq!(selected.records_json.len(), 1);
        let row: serde_json::Value =
            serde_json::from_slice(&selected.records_json[0]).expect("selected row JSON");
        assert_eq!(row["id"], row_id);
        assert_eq!(row["status"], status);
    }

    let ambiguous = client
        .select(api_key_data_request(
            crate::proto::SelectRequest {
                context: Some(context),
                message_type: "OTP".to_string(),
                limit: 10,
                ..crate::proto::SelectRequest::default()
            },
            &created.plain_key,
        ))
        .await
        .expect_err("colliding short name must fail before backend dispatch");
    assert_eq!(ambiguous.code(), tonic::Code::InvalidArgument);
    assert!(ambiguous.message().contains("ambiguous message type 'OTP'"));
    assert!(ambiguous.message().contains("ambulife.authn.entity.v1.OTP"));
    assert!(ambiguous.message().contains("udb.core.authn.entity.v1.OTP"));

    let _ = shutdown_tx.send(());
    server.await.expect("join data-only listener");
    sqlx::query("DROP SCHEMA IF EXISTS fix_ambulife_authn CASCADE")
        .execute(&pool)
        .await
        .expect("drop AmbuLife acceptance schema");
    sqlx::query("DROP SCHEMA IF EXISTS fix_udb_authn CASCADE")
        .execute(&pool)
        .await
        .expect("drop UDB acceptance schema");
    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_mtls_native_listener_is_binding_backed_and_revocable -- --ignored --nocapture"]
async fn live_postgres_mtls_native_listener_is_binding_backed_and_revocable() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service_with_jwt(pool.clone());
    let password = "CorrectHorse1!";
    let registered_spiffe = "spiffe://test.udb/served-mtls";
    let (owner, _) = create_service_account_with_grant(
        &authn,
        "served_mtls",
        password,
        &["udb:authn:validate-token"],
    )
    .await;
    let (other_owner, _) = create_service_account_with_grant(
        &authn,
        "served_mtls_other",
        password,
        &["udb:authn:validate-token"],
    )
    .await;
    let material = mtls_test_material(registered_spiffe);
    let binding = authn
        .create_certificate_binding(Request::new(authn_pb::CreateCertificateBindingRequest {
            tenant_id: "acme".to_string(),
            user_id: owner.user_id.clone(),
            selector_kind: "SPIFFE_URI".to_string(),
            selector_value: registered_spiffe.to_string(),
            scope_subset: vec!["udb:authn:validate-token".to_string()],
            reason: "served mTLS acceptance".to_string(),
            not_before: None,
            not_after: None,
        }))
        .await
        .expect("create registered mTLS binding")
        .into_inner()
        .binding
        .expect("registered mTLS binding");
    assert!(
        super::super::grants::resolve_certificate_grant(&pool, &material.client_der)
            .await
            .expect("resolve registered TLS certificate")
            .is_some()
    );
    let login = authn
        .login(Request::new(login_request(&owner.email, password)))
        .await
        .expect("mint token used as ValidateToken payload")
        .into_inner();
    let other_login = authn
        .login(Request::new(login_request(&other_owner.email, password)))
        .await
        .expect("mint independently valid mismatched service bearer")
        .into_inner();
    let revoker = authn_service(pool.clone());

    let security = crate::runtime::security::SecurityConfig {
        jwt_public_key: Some(include_str!("../../../testdata/jwt_rs256_public.pem").to_string()),
        ..crate::runtime::security::SecurityConfig::default()
    };
    crate::runtime::security::SecurityConfig::install_global(security);
    super::super::install_data_plane_credential_resolvers(
        pool.clone(),
        &crate::runtime::authn::AuthnConfig {
            session_enabled: true,
            session_hash_secret: "live-auth-test-secret".to_string(),
            ..crate::runtime::authn::AuthnConfig::default()
        },
    );
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mTLS native listener");
    let address = listener.local_addr().expect("mTLS listener address");
    let incoming = futures::stream::unfold(listener, |listener| async move {
        let connection = listener.accept().await.map(|(stream, _)| stream);
        Some((connection, listener))
    });
    let server_tls = tonic::transport::ServerTlsConfig::new()
        .identity(tonic::transport::Identity::from_pem(
            material.server_cert_pem.clone(),
            material.server_key_pem.clone(),
        ))
        .client_ca_root(tonic::transport::Certificate::from_pem(
            material.ca_pem.clone(),
        ));
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let served_authn = authn;
    let server = tokio::spawn(async move {
        let mut builder = tonic::transport::Server::builder()
            .layer(crate::runtime::credential_layer::CredentialResolveLayer::new());
        builder = builder.tls_config(server_tls).expect("mTLS server config");
        builder
            .add_service(MethodSecurityLayer::new().wrap(AuthnServiceServer::new(served_authn)))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve mTLS Authn listener");
    });

    let mut registered_client = connect_mtls_authn(
        address,
        &material.ca_pem,
        &material.client_cert_pem,
        &material.client_key_pem,
    )
    .await;
    let validated = registered_client
        .validate_token(validate_token_request(&login.access_token))
        .await
        .expect("registered mTLS client reaches descriptor-approved RPC")
        .into_inner();
    assert!(validated.valid);
    assert_eq!(validated.user_id, owner.user_id);

    let mut mismatched = validate_token_request(&login.access_token);
    mismatched.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", other_login.access_token)
            .parse()
            .expect("mismatched bearer metadata"),
    );
    let mismatch = registered_client
        .validate_token(mismatched)
        .await
        .expect_err("a valid certificate and valid bearer for different services must not compose");
    assert_eq!(mismatch.code(), tonic::Code::PermissionDenied);

    let mut unknown_client = connect_mtls_authn(
        address,
        &material.ca_pem,
        &material.unknown_cert_pem,
        &material.unknown_key_pem,
    )
    .await;
    let unknown = unknown_client
        .validate_token(validate_token_request(&login.access_token))
        .await
        .expect_err("unknown CA-trusted certificate must still lack a UDB binding");
    assert_eq!(unknown.code(), tonic::Code::Unauthenticated);

    revoker
        .revoke_certificate_binding(Request::new(authn_pb::RevokeCertificateBindingRequest {
            tenant_id: "acme".to_string(),
            binding_id: binding.binding_id,
            reason: "served revocation acceptance".to_string(),
        }))
        .await
        .expect("revoke served certificate binding");
    let revoked = registered_client
        .validate_token(validate_token_request(&login.access_token))
        .await
        .expect_err("revoked certificate must stop authenticating immediately");
    assert_eq!(revoked.code(), tonic::Code::Unauthenticated);

    let _ = shutdown_tx.send(());
    server.await.expect("join mTLS native listener");
    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_service_credentials_follow_grant_and_account_state -- --ignored --nocapture"]
async fn live_postgres_service_credentials_follow_grant_and_account_state() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service_with_jwt(pool.clone());
    let apikey = api_key_service(pool.clone());
    let password = "CorrectHorse1!";
    let (user, grant) = create_service_account_with_grant(
        &authn,
        "credential_state",
        password,
        &["udb:read", "udb:write"],
    )
    .await;

    let login = authn
        .login(Request::new(login_request(&user.email, password)))
        .await
        .expect("service login before grant replacement")
        .into_inner();
    let api_key = apikey
        .create_api_key(Request::new(apikey_pb::CreateApiKeyRequest {
            name: "grant-revision-key".to_string(),
            owner_id: user.user_id.clone(),
            scopes: vec!["udb:read".to_string()],
            context: Some(common_pb::RequestContext {
                principal_id: user.user_id.clone(),
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
        .expect("create grant-backed API key")
        .into_inner();
    assert!(
        apikey
            .validate_api_key(Request::new(apikey_pb::ValidateApiKeyRequest {
                plain_key: api_key.plain_key.clone(),
                required_scope: "udb:read".to_string(),
                ..Default::default()
            }))
            .await
            .expect("validate grant-backed API key")
            .into_inner()
            .valid
    );

    let spiffe_uri = "spiffe://test.udb/credential-state";
    let cert_der = certificate_der(spiffe_uri, "credential-state.test.udb");
    let binding = authn
        .create_certificate_binding(Request::new(authn_pb::CreateCertificateBindingRequest {
            tenant_id: "acme".to_string(),
            user_id: user.user_id.clone(),
            selector_kind: "SPIFFE_URI".to_string(),
            selector_value: spiffe_uri.to_string(),
            scope_subset: vec!["udb:read".to_string()],
            reason: "live binding".to_string(),
            not_before: None,
            not_after: None,
        }))
        .await
        .expect("create certificate binding")
        .into_inner()
        .binding
        .expect("created binding");
    let resolved = super::super::grants::resolve_certificate_grant(&pool, &cert_der)
        .await
        .expect("resolve certificate binding")
        .expect("active binding principal");
    assert_eq!(resolved.subject, user.user_id);
    assert_eq!(resolved.service_identity, grant.service_identity);
    assert_eq!(resolved.scopes, vec!["udb:read"]);

    let replaced = authn
        .replace_service_account_grant(Request::new(authn_pb::ReplaceServiceAccountGrantRequest {
            tenant_id: "acme".to_string(),
            user_id: user.user_id.clone(),
            approved_scopes: vec!["udb:read".to_string()],
            project_id: "billing".to_string(),
            reason: "narrow grant".to_string(),
            expected_revision: grant.revision,
        }))
        .await
        .expect("replace typed grant")
        .into_inner()
        .grant
        .expect("replaced grant");
    assert_eq!(replaced.revision, grant.revision + 1);
    assert!(
        !validate_access_token(&authn, login.access_token)
            .await
            .valid,
        "an already-issued widened service JWT must die when the grant narrows"
    );
    assert!(
        !apikey
            .validate_api_key(Request::new(apikey_pb::ValidateApiKeyRequest {
                plain_key: api_key.plain_key.clone(),
                required_scope: "udb:read".to_string(),
                ..Default::default()
            }))
            .await
            .expect("validate stale-revision API key")
            .into_inner()
            .valid,
        "API key reviewed against an old grant revision must fail closed"
    );
    assert!(
        super::super::grants::resolve_certificate_grant(&pool, &cert_der)
            .await
            .expect("resolve stale certificate binding")
            .is_none(),
        "certificate binding reviewed against an old grant revision must fail closed"
    );

    authn
        .revoke_certificate_binding(Request::new(authn_pb::RevokeCertificateBindingRequest {
            tenant_id: "acme".to_string(),
            binding_id: binding.binding_id.clone(),
            reason: "re-review".to_string(),
        }))
        .await
        .expect("revoke stale binding");
    let reviewed = authn
        .create_certificate_binding(Request::new(authn_pb::CreateCertificateBindingRequest {
            tenant_id: "acme".to_string(),
            user_id: user.user_id.clone(),
            selector_kind: "SPIFFE_URI".to_string(),
            selector_value: spiffe_uri.to_string(),
            scope_subset: vec!["udb:read".to_string()],
            reason: "review against current grant".to_string(),
            not_before: None,
            not_after: None,
        }))
        .await
        .expect("supersede revoked binding")
        .into_inner()
        .binding
        .expect("reviewed binding");
    assert_eq!(reviewed.binding_id, binding.binding_id);
    assert_eq!(reviewed.grant_revision, replaced.revision);
    assert!(
        super::super::grants::resolve_certificate_grant(&pool, &cert_der)
            .await
            .expect("resolve reviewed binding")
            .is_some()
    );

    let expired_spiffe = "spiffe://test.udb/expired";
    let expired_der = certificate_der(expired_spiffe, "expired.test.udb");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_secs() as i64;
    authn
        .create_certificate_binding(Request::new(authn_pb::CreateCertificateBindingRequest {
            tenant_id: "acme".to_string(),
            user_id: user.user_id.clone(),
            selector_kind: "SPIFFE_URI".to_string(),
            selector_value: expired_spiffe.to_string(),
            scope_subset: vec!["udb:read".to_string()],
            reason: "expired validity-window acceptance".to_string(),
            not_before: Some(prost_types::Timestamp {
                seconds: now - 3_600,
                nanos: 0,
            }),
            not_after: Some(prost_types::Timestamp {
                seconds: now - 60,
                nanos: 0,
            }),
        }))
        .await
        .expect("persist an already-expired binding for resolution testing");
    assert!(
        super::super::grants::resolve_certificate_grant(&pool, &expired_der)
            .await
            .expect("resolve expired certificate binding")
            .is_none(),
        "an expired binding must never authenticate"
    );

    let dns_binding = authn
        .create_certificate_binding(Request::new(authn_pb::CreateCertificateBindingRequest {
            tenant_id: "acme".to_string(),
            user_id: user.user_id.clone(),
            selector_kind: "DNS_SAN".to_string(),
            selector_value: "credential-state.test.udb".to_string(),
            scope_subset: vec!["udb:read".to_string()],
            reason: "weaker fallback selector".to_string(),
            not_before: None,
            not_after: None,
        }))
        .await
        .expect("create current DNS binding")
        .into_inner()
        .binding
        .expect("DNS binding");
    assert_eq!(dns_binding.grant_revision, replaced.revision);
    authn
        .revoke_certificate_binding(Request::new(authn_pb::RevokeCertificateBindingRequest {
            tenant_id: "acme".to_string(),
            binding_id: reviewed.binding_id.clone(),
            reason: "prove strong-selector fail closed".to_string(),
        }))
        .await
        .expect("revoke stronger SPIFFE binding");
    assert!(
        super::super::grants::resolve_certificate_grant(&pool, &cert_der)
            .await
            .expect("resolve revoked strong selector")
            .is_none(),
        "a revoked SPIFFE binding must not fall through to an active DNS binding"
    );
    authn
        .create_certificate_binding(Request::new(authn_pb::CreateCertificateBindingRequest {
            tenant_id: "acme".to_string(),
            user_id: user.user_id.clone(),
            selector_kind: "SPIFFE_URI".to_string(),
            selector_value: spiffe_uri.to_string(),
            scope_subset: vec!["udb:read".to_string()],
            reason: "restore strong binding before rotation".to_string(),
            not_before: None,
            not_after: None,
        }))
        .await
        .expect("restore SPIFFE binding");

    let pre_rotation_login = authn
        .login(Request::new(login_request(&user.email, password)))
        .await
        .expect("service login before identity rotation")
        .into_inner();
    let pre_rotation_key = apikey
        .create_api_key(Request::new(apikey_pb::CreateApiKeyRequest {
            name: "identity-rotation-key".to_string(),
            owner_id: user.user_id.clone(),
            scopes: vec!["udb:read".to_string()],
            context: Some(common_pb::RequestContext {
                principal_id: user.user_id.clone(),
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
        .expect("create key before identity rotation")
        .into_inner();
    let rotated = authn
        .rotate_service_account_identity(Request::new(
            authn_pb::RotateServiceAccountIdentityRequest {
                tenant_id: "acme".to_string(),
                user_id: user.user_id.clone(),
                new_service_identity: "spiffe://test.udb/credential-state-v2".to_string(),
                expected_revision: replaced.revision,
                reason: "rotate compromised service identity".to_string(),
            },
        ))
        .await
        .expect("rotate immutable service identity")
        .into_inner();
    let rotated_grant = rotated.grant.expect("rotated grant");
    assert_eq!(rotated.previous_service_identity, grant.service_identity);
    assert_eq!(rotated_grant.revision, replaced.revision + 1);
    assert_eq!(
        rotated_grant.service_identity,
        "spiffe://test.udb/credential-state-v2"
    );
    assert!(
        !validate_access_token(&authn, pre_rotation_login.access_token)
            .await
            .valid,
        "identity rotation must invalidate JWTs carrying the previous identity"
    );
    assert!(
        !apikey
            .validate_api_key(Request::new(apikey_pb::ValidateApiKeyRequest {
                plain_key: pre_rotation_key.plain_key,
                required_scope: "udb:read".to_string(),
                ..Default::default()
            }))
            .await
            .expect("validate key after identity rotation")
            .into_inner()
            .valid,
        "identity rotation must invalidate keys reviewed against the prior revision"
    );
    assert!(
        super::super::grants::resolve_certificate_grant(&pool, &cert_der)
            .await
            .expect("resolve binding after identity rotation")
            .is_none(),
        "identity rotation must invalidate bindings reviewed against the prior revision"
    );

    authn
        .change_user_status(Request::new(authn_pb::ChangeUserStatusRequest {
            user_id: user.user_id.clone(),
            new_status: authn_entity_pb::UserStatus::Suspended as i32,
            reason: "credential shutdown".to_string(),
            ..Default::default()
        }))
        .await
        .expect("suspend service account");
    assert!(
        super::super::grants::resolve_certificate_grant(&pool, &cert_der)
            .await
            .expect("resolve certificate for suspended owner")
            .is_none()
    );
    let login_error = authn
        .login(Request::new(login_request(&user.email, password)))
        .await
        .expect_err("suspended service account must not log in");
    assert_eq!(login_error.code(), tonic::Code::Unauthenticated);

    let unknown_cert = certificate_der(
        "spiffe://test.udb/not-registered",
        "not-registered.test.udb",
    );
    assert!(
        super::super::grants::resolve_certificate_grant(&pool, &unknown_cert)
            .await
            .expect("unknown certificate lookup")
            .is_none()
    );

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_grant_and_binding_audit_failure_is_atomic -- --ignored --nocapture"]
async fn live_postgres_grant_and_binding_audit_failure_is_atomic() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let healthy = authn_service(pool.clone());
    let (user, grant) = create_service_account_with_grant(
        &healthy,
        "grant_atomic",
        "CorrectHorse1!",
        &["udb:read"],
    )
    .await;
    let failing = authn_service_with_failing_sink(pool.clone());

    let replace = failing
        .replace_service_account_grant(Request::new(authn_pb::ReplaceServiceAccountGrantRequest {
            tenant_id: "acme".to_string(),
            user_id: user.user_id.clone(),
            approved_scopes: vec!["udb:write".to_string()],
            project_id: "billing".to_string(),
            reason: "forced rollback".to_string(),
            expected_revision: grant.revision,
        }))
        .await;
    assert!(
        replace.is_err(),
        "grant replace must propagate audit failure"
    );
    let after = healthy
        .get_service_account_grant(Request::new(authn_pb::GetServiceAccountGrantRequest {
            tenant_id: "acme".to_string(),
            user_id: user.user_id.clone(),
        }))
        .await
        .expect("re-read grant after rolled-back replace")
        .into_inner()
        .grant
        .expect("grant remains");
    assert_eq!(after.revision, grant.revision);
    assert_eq!(after.approved_scopes_json, grant.approved_scopes_json);

    let rotate = failing
        .rotate_service_account_identity(Request::new(
            authn_pb::RotateServiceAccountIdentityRequest {
                tenant_id: "acme".to_string(),
                user_id: user.user_id.clone(),
                new_service_identity: "spiffe://test.udb/rolled-back".to_string(),
                expected_revision: grant.revision,
                reason: "forced rollback".to_string(),
            },
        ))
        .await;
    assert!(
        rotate.is_err(),
        "identity rotation must propagate audit failure"
    );
    let after_rotation = healthy
        .get_service_account_grant(Request::new(authn_pb::GetServiceAccountGrantRequest {
            tenant_id: "acme".to_string(),
            user_id: user.user_id.clone(),
        }))
        .await
        .expect("re-read grant after rolled-back identity rotation")
        .into_inner()
        .grant
        .expect("grant remains after rotation rollback");
    assert_eq!(after_rotation.revision, grant.revision);
    assert_eq!(after_rotation.service_identity, grant.service_identity);

    let binding = failing
        .create_certificate_binding(Request::new(authn_pb::CreateCertificateBindingRequest {
            tenant_id: "acme".to_string(),
            user_id: user.user_id,
            selector_kind: "DNS_SAN".to_string(),
            selector_value: "atomic.test.udb".to_string(),
            scope_subset: vec!["udb:read".to_string()],
            reason: "forced rollback".to_string(),
            not_before: None,
            not_after: None,
        }))
        .await;
    assert!(
        binding.is_err(),
        "binding create must propagate audit failure"
    );
    let listed = healthy
        .list_certificate_bindings(Request::new(authn_pb::ListCertificateBindingsRequest {
            tenant_id: "acme".to_string(),
            page_size: 50,
            page_token: String::new(),
        }))
        .await
        .expect("list bindings after rolled-back create")
        .into_inner();
    assert!(listed.bindings.is_empty());

    cleanup_native_auth_db(&pool).await;
}
