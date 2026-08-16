//! Native auth CLI commands.
//!
//! These commands call the generated UDB authn/authz/apikey clients. The CLI is
//! a control-plane convenience layer only; CRUD still flows through the native
//! service protos and their server implementations.

use super::*;
use serde_json::json;
use tonic::Request;
use tonic::transport::Channel;
use udb::proto::udb::core::apikey::entity::v1 as apikey_entity_pb;
use udb::proto::udb::core::apikey::services::v1 as apikey_pb;
use udb::proto::udb::core::apikey::services::v1::api_key_service_client::ApiKeyServiceClient;
use udb::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use udb::proto::udb::core::authn::services::v1 as authn_pb;
use udb::proto::udb::core::authn::services::v1::authn_service_client::AuthnServiceClient;
use udb::proto::udb::core::authz::services::v1 as authz_pb;
use udb::proto::udb::core::authz::services::v1::authz_service_client::AuthzServiceClient;
use udb::proto::udb::core::common::v1 as common_pb;

const SECRET_PREFIX_REVEAL_LEN: usize = 6;
const METADATA_HEADERS: &[(&str, &str)] = &[
    ("x-tenant-id", "UDB_TENANT_ID"),
    ("x-udb-project-id", "UDB_PROJECT_ID"),
    ("x-user-id", "UDB_USER_ID"),
    ("x-service-identity", "UDB_SERVICE_IDENTITY"),
    ("x-scopes", "UDB_SCOPES"),
    ("x-purpose", "UDB_PURPOSE"),
];

fn grant_json(grant: authn_entity_pb::ServiceAccountGrant) -> serde_json::Value {
    json!({
        "grant_id": grant.grant_id,
        "user_id": grant.user_id,
        "service_identity": grant.service_identity,
        "tenant_id": grant.tenant_id,
        "project_id": grant.project_id,
        "approved_scopes": serde_json::from_str::<serde_json::Value>(&grant.approved_scopes_json)
            .unwrap_or_else(|_| json!([])),
        "status": grant.status,
        "revision": grant.revision,
        "updated_by": grant.updated_by,
        "reason": grant.reason,
    })
}

fn timestamp_json(timestamp: Option<prost_types::Timestamp>) -> serde_json::Value {
    timestamp
        .map(|value| json!({ "seconds": value.seconds, "nanos": value.nanos }))
        .unwrap_or(serde_json::Value::Null)
}

fn binding_json(binding: authn_entity_pb::CertificateBinding) -> serde_json::Value {
    json!({
        "binding_id": binding.binding_id,
        "selector_kind": binding.selector_kind,
        "selector_value": binding.selector_value,
        "user_id": binding.user_id,
        "tenant_id": binding.tenant_id,
        "grant_revision": binding.grant_revision,
        "scope_subset": serde_json::from_str::<serde_json::Value>(&binding.scope_subset_json)
            .unwrap_or_else(|_| json!([])),
        "status": binding.status,
        "not_before": timestamp_json(binding.not_before),
        "not_after": timestamp_json(binding.not_after),
        "updated_by": binding.updated_by,
        "reason": binding.reason,
    })
}

fn cli_timestamp(unix: u64) -> Option<prost_types::Timestamp> {
    (unix != 0).then_some(prost_types::Timestamp {
        seconds: unix as i64,
        nanos: 0,
    })
}

pub(crate) fn run_auth_command(command: AuthCommand) -> i32 {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("auth: failed to create tokio runtime: {err}");
            return 1;
        }
    };
    match runtime.block_on(run_auth_command_async(command)) {
        Ok(value) => {
            output_json(&value, "auth command result");
            0
        }
        Err(err) => {
            eprintln!("auth: {err}");
            1
        }
    }
}

async fn run_auth_command_async(command: AuthCommand) -> Result<serde_json::Value, String> {
    match command {
        AuthCommand::Bootstrap {
            username,
            email,
            password,
            tenant,
            project,
            platform_admin,
        } => {
            // urgent_fix #20: OFFLINE — connect straight to Postgres and create the
            // first verified admin user via the in-process authn service (no running
            // broker / no pre-existing credential needed). This is the root bootstrap
            // that breaks the otherwise-circular credential dependency on the
            // PEP-fronted listener.
            if password.trim().len() < 8 {
                return Err("--password is required (>= 8 chars)".to_string());
            }
            let dsn = require_pg_dsn()?;
            // 06.4.1.1 wiring: when the operator opts into the SERVED single-use
            // bootstrap path (`UDB_ALLOW_SERVED_BOOTSTRAP=1`), route the same CLI
            // through `served_bootstrap_admin`, which re-checks that env gate AND
            // enforces a durable single-use marker (`udb_system.bootstrap_state`)
            // so a privilege-creating bootstrap can never be replayed. Without the
            // opt-in this stays the plain offline utility (`bootstrap_admin_user`):
            // the gate is checked inside the served wrapper and is NOT weakened here.
            let served = std::env::var("UDB_ALLOW_SERVED_BOOTSTRAP")
                .ok()
                .map(|v| {
                    let v = v.trim().to_ascii_lowercase();
                    v == "1" || v == "true"
                })
                .unwrap_or(false);
            if served && platform_admin {
                return Err(
                    "--platform-admin is offline-only and cannot be combined with served bootstrap"
                        .to_string(),
                );
            }
            let admin = if served {
                udb::runtime::service::served_bootstrap_admin(
                    &dsn, &username, &email, &password, &tenant, &project,
                )
                .await?
            } else if platform_admin {
                udb::runtime::service::bootstrap_platform_admin_user(
                    &dsn, &username, &email, &password, &tenant, &project,
                )
                .await?
            } else {
                udb::runtime::service::bootstrap_admin_user(
                    &dsn, &username, &email, &password, &tenant, &project,
                )
                .await?
            };
            Ok(json!({
                "bootstrapped": true,
                "served": served,
                "platform_admin": platform_admin,
                "user_id": admin.user_id,
                "username": username,
                // `tenant` is the human code/input; `tenant_id` is the CANONICAL
                // UUID the principal was bound to (and that the Login JWT claim
                // carries). Clients/tests MUST send this UUID in request bodies.
                "tenant": tenant,
                "tenant_id": admin.tenant_id,
                "project": project,
                "note": if platform_admin {
                    "user is ACTIVE with offline-provisioned system/global platform authority"
                } else {
                    "user is ACTIVE; clients can now Authenticate with these credentials"
                },
            }))
        }
        AuthCommand::MigrateGrants { dry_run } => {
            // fix_plan §2.3: offline against the database, mirroring bootstrap.
            let dsn = require_pg_dsn()?;
            udb::runtime::service::migrate_service_account_grants(&dsn, dry_run).await
        }
        // ── `udb auth grant …` / `udb auth cert-binding …` ────────────────────
        // Typed-grant + certificate-binding management goes through the native
        // Authn API, so descriptor scopes and claim tenant binding apply.
        AuthCommand::GrantCreate {
            tenant_id,
            user_id,
            service_identity,
            project_id,
            scopes,
            reason,
        } => {
            require("tenant", &tenant_id)?;
            require("user", &user_id)?;
            require("identity", &service_identity)?;
            if scopes.is_empty() {
                return Err("--scope is required (repeat for multiple scopes)".to_string());
            }
            let mut client = authn_client().await?;
            let response = client
                .create_service_account_grant(with_metadata(
                    authn_pb::CreateServiceAccountGrantRequest {
                        tenant_id,
                        user_id,
                        service_identity,
                        project_id,
                        approved_scopes: scopes,
                        reason,
                    },
                ))
                .await
                .map_err(|error| format!("grant create failed: {error}"))?
                .into_inner();
            response
                .grant
                .map(grant_json)
                .ok_or_else(|| "grant create returned no grant".to_string())
        }
        AuthCommand::GrantGet { tenant_id, user_id } => {
            require("tenant", &tenant_id)?;
            require("user", &user_id)?;
            let mut client = authn_client().await?;
            let response = client
                .get_service_account_grant(with_metadata(authn_pb::GetServiceAccountGrantRequest {
                    tenant_id,
                    user_id,
                }))
                .await
                .map_err(|error| format!("grant get failed: {error}"))?
                .into_inner();
            response
                .grant
                .map(grant_json)
                .ok_or_else(|| "grant get returned no grant".to_string())
        }
        AuthCommand::GrantList { tenant_id } => {
            require("tenant", &tenant_id)?;
            let mut client = authn_client().await?;
            let response = client
                .list_service_account_grants(with_metadata(
                    authn_pb::ListServiceAccountGrantsRequest {
                        tenant_id,
                        page_size: 200,
                        page_token: String::new(),
                    },
                ))
                .await
                .map_err(|error| format!("grant list failed: {error}"))?
                .into_inner();
            Ok(json!({
                "grants": response.grants.into_iter().map(grant_json).collect::<Vec<_>>(),
                "next_page_token": response.next_page_token,
            }))
        }
        AuthCommand::GrantReplace {
            tenant_id,
            user_id,
            scopes,
            project_id,
            reason,
            expected_revision,
        } => {
            require("tenant", &tenant_id)?;
            require("user", &user_id)?;
            if scopes.is_empty() {
                return Err("--scope is required (repeat for multiple scopes)".to_string());
            }
            if expected_revision <= 0 {
                return Err(
                    "--expected-revision is required (the grant's current revision, from \
                     `auth grant get`)"
                        .to_string(),
                );
            }
            let mut client = authn_client().await?;
            let response = client
                .replace_service_account_grant(with_metadata(
                    authn_pb::ReplaceServiceAccountGrantRequest {
                        tenant_id,
                        user_id,
                        approved_scopes: scopes,
                        project_id,
                        reason,
                        expected_revision,
                    },
                ))
                .await
                .map_err(|error| format!("grant replace failed: {error}"))?
                .into_inner();
            response
                .grant
                .map(grant_json)
                .ok_or_else(|| "grant replace returned no grant".to_string())
        }
        AuthCommand::GrantRevoke {
            tenant_id,
            user_id,
            reason,
        } => {
            require("tenant", &tenant_id)?;
            require("user", &user_id)?;
            let mut client = authn_client().await?;
            let response = client
                .revoke_service_account_grant(with_metadata(
                    authn_pb::RevokeServiceAccountGrantRequest {
                        tenant_id,
                        user_id,
                        reason,
                    },
                ))
                .await
                .map_err(|error| format!("grant revoke failed: {error}"))?
                .into_inner();
            Ok(json!({ "revoked": response.revoked, "message": response.message }))
        }
        AuthCommand::GrantRotateIdentity {
            tenant_id,
            user_id,
            new_service_identity,
            reason,
            expected_revision,
        } => {
            require("tenant", &tenant_id)?;
            require("user", &user_id)?;
            require("identity", &new_service_identity)?;
            if expected_revision <= 0 {
                return Err(
                    "--expected-revision is required (the grant's current revision, from \
                     `auth grant get`)"
                        .to_string(),
                );
            }
            let mut client = authn_client().await?;
            let response = client
                .rotate_service_account_identity(with_metadata(
                    authn_pb::RotateServiceAccountIdentityRequest {
                        tenant_id,
                        user_id,
                        new_service_identity,
                        expected_revision,
                        reason,
                    },
                ))
                .await
                .map_err(|error| format!("grant identity rotation failed: {error}"))?
                .into_inner();
            let grant = response
                .grant
                .map(grant_json)
                .ok_or_else(|| "grant identity rotation returned no grant".to_string())?;
            Ok(json!({
                "grant": grant,
                "previous_service_identity": response.previous_service_identity,
                "message": response.message,
            }))
        }
        AuthCommand::CertBindingCreate {
            tenant_id,
            user_id,
            selector_kind,
            selector_value,
            scope_subset,
            not_before_unix,
            not_after_unix,
            reason,
        } => {
            require("tenant", &tenant_id)?;
            require("user", &user_id)?;
            require("selector-kind", &selector_kind)?;
            require("selector-value", &selector_value)?;
            let mut client = authn_client().await?;
            let response = client
                .create_certificate_binding(with_metadata(
                    authn_pb::CreateCertificateBindingRequest {
                        tenant_id,
                        user_id,
                        selector_kind,
                        selector_value,
                        scope_subset,
                        reason,
                        not_before: cli_timestamp(not_before_unix),
                        not_after: cli_timestamp(not_after_unix),
                    },
                ))
                .await
                .map_err(|error| format!("certificate binding create failed: {error}"))?
                .into_inner();
            response
                .binding
                .map(binding_json)
                .ok_or_else(|| "certificate binding create returned no binding".to_string())
        }
        AuthCommand::CertBindingList { tenant_id } => {
            require("tenant", &tenant_id)?;
            let mut client = authn_client().await?;
            let response = client
                .list_certificate_bindings(with_metadata(
                    authn_pb::ListCertificateBindingsRequest {
                        tenant_id,
                        page_size: 200,
                        page_token: String::new(),
                    },
                ))
                .await
                .map_err(|error| format!("certificate binding list failed: {error}"))?
                .into_inner();
            Ok(json!({
                "bindings": response.bindings.into_iter().map(binding_json).collect::<Vec<_>>(),
                "next_page_token": response.next_page_token,
            }))
        }
        AuthCommand::CertBindingRevoke {
            tenant_id,
            binding_id,
            reason,
        } => {
            require("tenant", &tenant_id)?;
            require("binding", &binding_id)?;
            let mut client = authn_client().await?;
            let response = client
                .revoke_certificate_binding(with_metadata(
                    authn_pb::RevokeCertificateBindingRequest {
                        tenant_id,
                        binding_id,
                        reason,
                    },
                ))
                .await
                .map_err(|error| format!("certificate binding revoke failed: {error}"))?
                .into_inner();
            Ok(json!({ "revoked": response.revoked, "message": response.message }))
        }
        AuthCommand::PrincipalList { tenant_id } => {
            let mut client = authn_client().await?;
            let response = client
                .list_users(with_metadata(authn_pb::ListUsersRequest {
                    tenant_id,
                    ..Default::default()
                }))
                .await
                .map_err(|err| format!("principal list failed: {err}"))?
                .into_inner();
            Ok(json!({
                "principals": response.users.into_iter().map(|user| json!({
                    "user_id": user.user_id,
                    "username": user.username,
                    "email": user.email,
                    "tenant_id": user.tenant_id,
                    "project_id": user.project_id,
                    "account_kind": user.account_kind,
                    "status": user.status,
                    "external_provider_id": user.external_provider_id,
                    "external_subject": user.external_subject,
                })).collect::<Vec<_>>(),
                "page": response.page.map(|page| json!({
                    "total_items": page.total_items,
                    "total_pages": page.total_pages,
                    "page": page.page,
                    "page_size": page.page_size,
                    "next_page_token": page.next_page_token,
                })),
            }))
        }
        AuthCommand::IdentityLink {
            user_id,
            provider_id,
            subject,
        } => {
            require("user", &user_id)?;
            require("provider", &provider_id)?;
            require("subject", &subject)?;
            let mut client = authn_client().await?;
            let response = client
                .update_user(with_metadata(authn_pb::UpdateUserRequest {
                    user_id,
                    external_provider_id: provider_id,
                    external_subject: subject,
                    ..Default::default()
                }))
                .await
                .map_err(|err| format!("identity link failed: {err}"))?
                .into_inner();
            let user = response
                .user
                .ok_or_else(|| "identity link returned no user".to_string())?;
            Ok(json!({
                "linked": true,
                "user_id": user.user_id,
                "external_provider_id": user.external_provider_id,
                "external_subject": user.external_subject,
            }))
        }
        AuthCommand::SessionRevoke {
            session_id,
            principal_id,
            all_for_principal,
        } => {
            if all_for_principal {
                require("principal", &principal_id)?;
            } else {
                require("session", &session_id)?;
            }
            let mut client = authn_client().await?;
            let response = client
                .revoke_session(with_metadata(authn_pb::RevokeSessionRequest {
                    session_id,
                    principal_id,
                    all_for_principal,
                    ..Default::default()
                }))
                .await
                .map_err(|err| format!("session revoke failed: {err}"))?
                .into_inner();
            Ok(json!({
                "session_id": redact_secret(&response.session_id),
                "revoked_count": response.revoked_count,
                "operation_id": response.operation_id,
            }))
        }
        // UDB-AUTH-009: offline api-key rotation/reconciliation (direct DB, same
        // operator trust model as `auth bootstrap user`).
        AuthCommand::ApiKeyRevoke { key_prefix } => {
            require("key", &key_prefix)?;
            let dsn = require_pg_dsn()?;
            udb::runtime::service::cli_api_key_revoke(&dsn, &key_prefix).await
        }
        AuthCommand::ApiKeyList { owner_id } => {
            require("owner", &owner_id)?;
            let dsn = require_pg_dsn()?;
            udb::runtime::service::cli_api_key_list(&dsn, &owner_id).await
        }
        AuthCommand::ApiKeyCreate {
            owner_id,
            name,
            scopes,
        } => {
            require("owner", &owner_id)?;
            let mut client = apikey_client().await?;
            let response = client
                .create_api_key(with_metadata(apikey_pb::CreateApiKeyRequest {
                    name,
                    owner_type: apikey_entity_pb::ApiKeyOwnerType::ServiceAccount as i32,
                    owner_id,
                    scopes,
                    context: Some(request_context()),
                    ..Default::default()
                }))
                .await
                .map_err(|err| format!("api-key create failed: {err}"))?
                .into_inner();
            let key = response
                .key
                .ok_or_else(|| "api-key create returned no key".to_string())?;
            // The plain key is shown ONCE at creation. Redact it by default so it
            // does not leak into shell history / logs; set UDB_SHOW_SECRET=1 to
            // reveal it for the single moment the operator needs to store it.
            let reveal = env::var("UDB_SHOW_SECRET")
                .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
                .unwrap_or(false);
            Ok(json!({
                "key_id": key.key_id,
                "key_prefix": key.key_prefix,
                "owner_id": key.owner_id,
                "status": key.status,
                "plain_key": if reveal { response.plain_key } else { redact_secret(&response.plain_key) },
                "secret_revealed": reveal,
                "note": if reveal {
                    "store this key now — it is not retrievable again"
                } else {
                    "key redacted; re-run with UDB_SHOW_SECRET=1 to reveal the one-time key"
                },
            }))
        }
        AuthCommand::RoleBind {
            user_id,
            role_id,
            domain,
            assigned_by,
        } => {
            require("user", &user_id)?;
            require("role", &role_id)?;
            let mut client = authz_client().await?;
            let response = client
                .assign_role(with_metadata(authz_pb::AssignRoleRequest {
                    user_id,
                    role_id,
                    domain,
                    assigned_by,
                    tenant_id: env::var("UDB_TENANT_ID").unwrap_or_default(),
                    project_id: env::var("UDB_PROJECT_ID").unwrap_or_default(),
                    ..Default::default()
                }))
                .await
                .map_err(|err| format!("role bind failed: {err}"))?
                .into_inner();
            let user_role = response
                .user_role
                .ok_or_else(|| "role bind returned no assignment".to_string())?;
            Ok(json!({
                "bound": true,
                "user_role_id": user_role.user_role_id,
                "user_id": user_role.user_id,
                "role_id": user_role.role_id,
                "domain": user_role.domain,
                "tenant_id": user_role.tenant_id,
            }))
        }
        AuthCommand::RelationPut {
            subject,
            relation,
            object,
            tenant,
            project,
        } => {
            require("subject", &subject)?;
            require("relation", &relation)?;
            require("object", &object)?;
            let mut client = authz_client().await?;
            let response = client
                .put_relationship(with_metadata(authz_pb::PutRelationshipRequest {
                    tuple: Some(authz_pb::RelationshipTuple {
                        subject,
                        relation,
                        object,
                        tenant,
                        project,
                        source: "udb-cli".to_string(),
                        ..Default::default()
                    }),
                }))
                .await
                .map_err(|err| format!("relation put failed: {err}"))?
                .into_inner();
            Ok(json!({
                "ok": response.ok,
                "message": response.message,
            }))
        }
        AuthCommand::PolicyPut {
            subject,
            role,
            action,
            resource,
            effect,
            tenant,
            project,
        } => {
            require("action", &action)?;
            require("resource", &resource)?;
            let mut client = authz_client().await?;
            let response = client
                .put_authz_policy(with_metadata(authz_pb::PutAuthzPolicyRequest {
                    policy: Some(authz_pb::AuthzPolicyRecord {
                        subject,
                        role,
                        action,
                        resource,
                        effect: effect.to_ascii_uppercase(),
                        tenant,
                        project,
                        enabled: true,
                        ..Default::default()
                    }),
                }))
                .await
                .map_err(|err| format!("policy put failed: {err}"))?
                .into_inner();
            Ok(json!({
                "ok": response.ok,
                "message": response.message,
            }))
        }
        AuthCommand::PolicyLint => {
            let mut client = authz_client().await?;
            let response = client
                .lint_authz_policies(with_metadata(authz_pb::LintAuthzPoliciesRequest {}))
                .await
                .map_err(|err| format!("policy lint failed: {err}"))?
                .into_inner();
            Ok(json!({
                "passed": response.findings.is_empty(),
                "findings": response.findings,
            }))
        }
    }
}

async fn authn_client() -> Result<AuthnServiceClient<Channel>, String> {
    AuthnServiceClient::connect(auth_target())
        .await
        .map_err(|err| format!("failed to connect to authn service: {err}"))
}

async fn authz_client() -> Result<AuthzServiceClient<Channel>, String> {
    AuthzServiceClient::connect(auth_target())
        .await
        .map_err(|err| format!("failed to connect to authz service: {err}"))
}

async fn apikey_client() -> Result<ApiKeyServiceClient<Channel>, String> {
    ApiKeyServiceClient::connect(auth_target())
        .await
        .map_err(|err| format!("failed to connect to apikey service: {err}"))
}

fn auth_target() -> String {
    let raw = env::var("UDB_AUTH_TARGET")
        .or_else(|_| env::var("UDB_GRPC_TARGET"))
        .or_else(|_| env::var("UDB_GRPC_ADDR"))
        .map(|addr| client_target_addr(&addr))
        .unwrap_or_else(|_| DEFAULT_GRPC_TARGET_ADDR.to_string());
    if raw.starts_with("http://") || raw.starts_with("https://") {
        raw
    } else {
        format!("http://{raw}")
    }
}

fn request_context() -> common_pb::RequestContext {
    common_pb::RequestContext {
        tenant: Some(common_pb::TenantContext {
            tenant_id: env::var("UDB_TENANT_ID").unwrap_or_default(),
            project_id: env::var("UDB_PROJECT_ID").unwrap_or_default(),
            ..Default::default()
        }),
        user_id: env::var("UDB_USER_ID").unwrap_or_default(),
        principal_id: env::var("UDB_PRINCIPAL_ID")
            .or_else(|_| env::var("UDB_USER_ID"))
            .unwrap_or_default(),
        service_identity: env::var("UDB_SERVICE_IDENTITY").unwrap_or_default(),
        scopes: env::var("UDB_SCOPES")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|scope| !scope.is_empty())
            .map(ToString::to_string)
            .collect(),
        ..Default::default()
    }
}

fn with_metadata<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "x-correlation-id",
        format!("udb-cli-{}", uuid::Uuid::new_v4())
            .parse()
            .expect("generated correlation id is valid gRPC metadata"),
    );
    if let Ok(token) = env::var("UDB_AUTH_TOKEN").or_else(|_| env::var("UDB_BEARER_TOKEN")) {
        let token = token.trim();
        if !token.is_empty() {
            if token.bytes().any(|b| matches!(b, b'\r' | b'\n')) {
                eprintln!(
                    "warning: UDB_AUTH_TOKEN/UDB_BEARER_TOKEN contains a newline and was not sent"
                );
            } else {
                match format!("Bearer {token}").parse() {
                    Ok(value) => {
                        request.metadata_mut().insert("authorization", value);
                    }
                    Err(_) => {
                        eprintln!(
                            "warning: UDB_AUTH_TOKEN/UDB_BEARER_TOKEN is not a valid gRPC header value \
                         (likely a stray newline or non-ASCII char); the request will be sent \
                         UNAUTHENTICATED"
                        );
                    }
                }
            }
        }
    }
    for (key, env_name) in METADATA_HEADERS {
        if let Ok(raw) = env::var(env_name) {
            if !raw.trim().is_empty() {
                match raw.parse() {
                    Ok(value) => {
                        request.metadata_mut().insert(*key, value);
                    }
                    Err(err) => {
                        eprintln!(
                            "warning: {env_name} is not a valid gRPC metadata value for header \
                             '{key}' (likely a stray newline or non-ASCII char); it is being \
                             dropped from the request: {err}"
                        );
                    }
                }
            }
        }
    }
    request
}

fn require(name: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("--{name} is required"))
    } else {
        Ok(())
    }
}

/// Resolve the offline-command Postgres DSN (UDB_PG_DSN, falling back to
/// DATABASE_URL) — shared by every direct-DB `auth` verb (bootstrap,
/// migrate-grants, grant, cert-binding).
fn require_pg_dsn() -> Result<String, String> {
    std::env::var("UDB_PG_DSN")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .map_err(|_| "set UDB_PG_DSN (or DATABASE_URL) to the target Postgres".to_string())
}

/// Mask a secret/identifier in CLI output (item 105): keep a short prefix for
/// correlation, hide the remainder. Tokens, API keys, and session ids are never
/// printed in full from these commands.
fn redact_secret(value: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        return String::new();
    }
    if value.len() <= SECRET_PREFIX_REVEAL_LEN {
        return "******".to_string();
    }
    format!("{}…(redacted)", &value[..SECRET_PREFIX_REVEAL_LEN])
}

fn client_target_addr(addr: &str) -> String {
    let addr = addr.trim();
    if let Some(port) = addr.strip_prefix(&format!("{DEFAULT_GRPC_BIND_HOST}:")) {
        format!("{DEFAULT_GRPC_TARGET_HOST}:{port}")
    } else {
        addr.to_string()
    }
}
