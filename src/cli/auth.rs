//! Native auth CLI commands.
//!
//! These commands call the generated UDB authn/authz/apikey clients. The CLI is
//! a control-plane convenience layer only; CRUD still flows through the native
//! service protos and their server implementations.

use super::*;
use serde_json::json;
use std::path::{Path, PathBuf};
use tonic::Request;
use tonic::transport::Channel;
use udb::proto::udb::core::apikey::entity::v1 as apikey_entity_pb;
use udb::proto::udb::core::apikey::services::v1 as apikey_pb;
use udb::proto::udb::core::apikey::services::v1::api_key_service_client::ApiKeyServiceClient;
use udb::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use udb::proto::udb::core::authn::services::v1 as authn_pb;
use udb::proto::udb::core::authn::services::v1::authn_service_client::AuthnServiceClient;
use udb::proto::udb::core::authz::entity::v1 as authz_entity_pb;
use udb::proto::udb::core::authz::services::v1 as authz_pb;
use udb::proto::udb::core::authz::services::v1::authz_service_client::AuthzServiceClient;
use udb::proto::udb::core::common::v1 as common_pb;
use udb::proto::udb::core::tenant::services::v1 as tenant_pb;
use udb::proto::udb::core::tenant::services::v1::tenant_service_client::TenantServiceClient;

const SECRET_PREFIX_REVEAL_LEN: usize = 6;

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
    if let AuthCommand::Login {
        username,
        password_file,
        tenant,
        project,
        out,
        addr,
    } = command
    {
        let login = LoginArgs {
            username,
            password_file,
            tenant,
            project,
            out,
            addr,
        };
        return match runtime.block_on(run_login(&login)) {
            // Without --out the token alone goes to stdout, so
            // `UDB_AUTH_TOKEN=$(udb auth login …)` works.
            Ok(LoginOutcome::Printed(token)) => {
                println!("{token}");
                0
            }
            Ok(LoginOutcome::Written(summary)) => {
                output_json(&summary, "auth login result");
                0
            }
            Err(err) => {
                eprintln!("auth login: {err}");
                1
            }
        };
    }
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
            // An unknown --tenant or --username is created rather than refused,
            // so a typo silently mints a new tenant or user; say so loudly.
            if admin.created_tenant {
                eprintln!(
                    "warning: no tenant matched --tenant '{tenant}', so a new tenant {} was created",
                    admin.tenant_id
                );
            }
            if admin.created_user {
                eprintln!("warning: no user named '{username}' existed, so a new user was created");
            } else {
                eprintln!(
                    "note: reused the existing user '{username}'; its password and email were not changed"
                );
            }
            Ok(json!({
                "bootstrapped": true,
                "served": served,
                "platform_admin": platform_admin,
                "user_id": admin.user_id,
                "username": username,
                "created_user": admin.created_user,
                "created_tenant": admin.created_tenant,
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
        AuthCommand::ServiceAccountCreate {
            tenant_id,
            username,
            email,
            password_env,
            password_file,
            project_id,
            reason,
        } => {
            require("tenant", &tenant_id)?;
            require("username", &username)?;
            require("email", &email)?;
            super::identity_ops::service_account_create(
                &tenant_id,
                &username,
                &email,
                &password_env,
                &password_file,
                &project_id,
                &reason,
            )
            .await
        }
        AuthCommand::ServiceAccountActivate {
            tenant_id,
            user_id,
            reason,
        } => {
            require("tenant", &tenant_id)?;
            require("user", &user_id)?;
            super::identity_ops::service_account_activate(&tenant_id, &user_id, &reason).await
        }
        AuthCommand::ServiceAccountSetPassword {
            tenant_id,
            user_id,
            password_env,
            password_file,
            reason,
        } => {
            require("tenant", &tenant_id)?;
            require("user", &user_id)?;
            require("reason", &reason)?;
            super::identity_ops::service_account_set_password(
                &tenant_id,
                &user_id,
                &password_env,
                &password_file,
                &reason,
            )
            .await
        }
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
                .map_err(|error| {
                    if error.message().contains("already exists") {
                        "grant create: this account already holds a grant, which was left \
                         unchanged. To change its scopes, run `udb auth grant replace`"
                            .to_string()
                    } else {
                        format!("grant create failed: {error}")
                    }
                })?
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
        AuthCommand::GrantTransfer {
            tenant_id,
            from_user_id,
            to_user_id,
            expected_revision,
            reason,
        } => {
            require("tenant", &tenant_id)?;
            require("from", &from_user_id)?;
            require("to", &to_user_id)?;
            if expected_revision <= 0 {
                return Err(
                    "--expected-revision is required (the grant's current revision, from \
                     `auth grant get`)"
                        .to_string(),
                );
            }
            let mut client = authn_client().await?;
            let response = client
                .transfer_service_account_grant(with_metadata(
                    authn_pb::TransferServiceAccountGrantRequest {
                        tenant_id,
                        from_user_id,
                        to_user_id,
                        expected_revision,
                        reason,
                    },
                ))
                .await
                .map_err(|error| format!("grant transfer failed: {error}"))?
                .into_inner();
            let previous = response.previous_user_id.clone();
            let mut out = response
                .grant
                .map(grant_json)
                .ok_or_else(|| "grant transfer returned no grant".to_string())?;
            out["previous_user_id"] = serde_json::Value::String(previous);
            Ok(out)
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
        AuthCommand::MembershipCreate {
            target_tenant_id,
            all_children,
            user_id,
            home_tenant_id,
            project_id,
            scopes,
            reason,
            expires_in_days,
            replace,
        } => {
            require("user", &user_id)?;
            require("home-tenant", &home_tenant_id)?;
            require("reason", &reason)?;
            if scopes.is_empty() {
                return Err("--scope is required (repeat for multiple scopes)".to_string());
            }
            if !(1..=365).contains(&expires_in_days) {
                return Err("--expires-in-days must be between 1 and 365".to_string());
            }
            let targets = membership_targets(&target_tenant_id, all_children).await?;
            let expires_at = membership_expiry(expires_in_days)?;
            let mut client = authn_client().await?;
            let mut approved = Vec::new();
            let mut failed = Vec::new();
            for target in targets {
                let request = authn_pb::CreateServiceAccountTenantMembershipRequest {
                    target_tenant_id: target.clone(),
                    user_id: user_id.clone(),
                    home_tenant_id: home_tenant_id.clone(),
                    target_project_id: project_id.clone(),
                    approved_scopes: scopes.clone(),
                    reason: reason.clone(),
                    expires_at: Some(expires_at),
                    replace_active: replace,
                };
                match client
                    .create_service_account_tenant_membership(with_metadata(request))
                    .await
                {
                    Ok(response) => approved.push(json!({
                        "target_tenant_id": target,
                        "membership_id": response
                            .into_inner()
                            .membership
                            .map(|membership| membership.membership_id)
                            .unwrap_or_default(),
                    })),
                    Err(error) => failed.push(json!({
                        "target_tenant_id": target,
                        "error": error.message(),
                    })),
                }
            }
            let summary = json!({ "approved": approved, "failed": failed });
            if failed.is_empty() {
                Ok(summary)
            } else {
                Err(format!(
                    "membership create failed for {} of {} tenant(s): {summary}",
                    failed.len(),
                    failed.len() + approved.len()
                ))
            }
        }
        AuthCommand::MembershipList { target_tenant_id } => {
            require("target-tenant", &target_tenant_id)?;
            let mut client = authn_client().await?;
            let mut memberships = Vec::new();
            let mut page_token = String::new();
            loop {
                let response = client
                    .list_service_account_tenant_memberships(with_metadata(
                        authn_pb::ListServiceAccountTenantMembershipsRequest {
                            target_tenant_id: target_tenant_id.clone(),
                            page_size: 200,
                            page_token: page_token.clone(),
                        },
                    ))
                    .await
                    .map_err(|error| format!("membership list failed: {error}"))?
                    .into_inner();
                memberships.extend(response.memberships.into_iter().map(|membership| {
                    json!({
                        "membership_id": membership.membership_id,
                        "user_id": membership.user_id,
                        "home_tenant_id": membership.home_tenant_id,
                        "target_tenant_id": membership.target_tenant_id,
                        "target_project_id": membership.target_project_id,
                        "approved_scopes": serde_json::from_str::<serde_json::Value>(
                            &membership.approved_scopes_json,
                        )
                        .unwrap_or(serde_json::Value::Null),
                        "status": membership.status,
                    })
                }));
                if response.next_page_token.is_empty() {
                    break;
                }
                page_token = response.next_page_token;
            }
            Ok(json!({ "memberships": memberships }))
        }
        AuthCommand::MembershipRevoke {
            target_tenant_id,
            membership_id,
            reason,
        } => {
            require("target-tenant", &target_tenant_id)?;
            require("membership", &membership_id)?;
            require("reason", &reason)?;
            let mut client = authn_client().await?;
            let response = client
                .revoke_service_account_tenant_membership(with_metadata(
                    authn_pb::RevokeServiceAccountTenantMembershipRequest {
                        target_tenant_id,
                        membership_id,
                        reason,
                    },
                ))
                .await
                .map_err(|error| format!("membership revoke failed: {error}"))?
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
        AuthCommand::RoleBindingPut {
            principal,
            role,
            tenant,
            project,
            expires_at_unix,
        } => {
            require_role_binding(&principal, &role, &tenant)?;
            put_role_binding(principal, role, tenant, project, expires_at_unix, "bind").await
        }
        AuthCommand::RoleUnbind {
            principal,
            role,
            tenant,
            project,
        } => {
            require_role_binding(&principal, &role, &tenant)?;
            // The binding row is keyed on (subject, tenant, role); re-putting it
            // already expired is what removes it from the enforcer's snapshot.
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_secs() as i64)
                .unwrap_or(1)
                .max(1);
            put_role_binding(principal, role, tenant, project, now, "unbind").await
        }
        AuthCommand::RoleCreate {
            code,
            name,
            description,
            tenant,
            project,
        } => {
            require("code", &code)?;
            let scope_type = if !project.trim().is_empty() {
                authz_entity_pb::RoleScopeType::Project
            } else if !tenant.trim().is_empty() {
                authz_entity_pb::RoleScopeType::Tenant
            } else {
                authz_entity_pb::RoleScopeType::Unspecified
            };
            let mut client = authz_client().await?;
            let response = client
                .create_role(with_metadata(authz_pb::CreateRoleRequest {
                    name: if name.trim().is_empty() {
                        code.clone()
                    } else {
                        name
                    },
                    description,
                    role_code: code,
                    tenant_id: tenant,
                    project_id: project,
                    scope_type: scope_type as i32,
                    ..Default::default()
                }))
                .await
                .map_err(|err| format!("role create failed: {err}"))?
                .into_inner();
            let role = response
                .role
                .ok_or_else(|| "role create returned no role".to_string())?;
            Ok(json!({ "created": true, "role": role_json(&role) }))
        }
        AuthCommand::RoleList {
            domain,
            include_inactive,
        } => {
            let mut client = authz_client().await?;
            let response = client
                .list_roles(with_metadata(authz_pb::ListRolesRequest {
                    domain,
                    active_only: !include_inactive,
                    ..Default::default()
                }))
                .await
                .map_err(|err| format!("role list failed: {err}"))?
                .into_inner();
            Ok(json!({
                "roles": response.roles.iter().map(role_json).collect::<Vec<_>>(),
            }))
        }
        AuthCommand::RoleAssignments { user_id, domain } => {
            require("user", &user_id)?;
            let mut client = authz_client().await?;
            let response = client
                .list_user_roles(with_metadata(authz_pb::ListUserRolesRequest {
                    user_id,
                    domain,
                    active_only: true,
                    ..Default::default()
                }))
                .await
                .map_err(|err| format!("role assignments failed: {err}"))?
                .into_inner();
            Ok(json!({
                "user_roles": response
                    .user_roles
                    .iter()
                    .map(|assignment| json!({
                        "user_role_id": assignment.user_role_id,
                        "user_id": assignment.user_id,
                        "role_id": assignment.role_id,
                        "domain": assignment.domain,
                        "tenant_id": assignment.tenant_id,
                        "expires_at": timestamp_json(assignment.expires_at.clone()),
                    }))
                    .collect::<Vec<_>>(),
                "note": "lists AssignRole (`user_roles`) assignments; bindings made with \
                         `role bind --principal` are policy tuples and are not listed here",
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
            // PutAuthzPolicy requires an id: derive the same stable id
            // `udb authz policy put` does, so re-running one rule writes one row.
            let mut policy = authz_pb::AuthzPolicyRecord {
                subject,
                role,
                action,
                resource,
                effect: effect.to_ascii_uppercase(),
                tenant,
                project,
                enabled: true,
                ..Default::default()
            };
            policy.id = super::authz_cli::derived_policy_id(&policy);
            let mut client = authz_client().await?;
            let response = client
                .put_authz_policy(with_metadata(authz_pb::PutAuthzPolicyRequest {
                    policy: Some(policy),
                }))
                .await
                .map_err(|err| format!("policy put failed: {err}"))?
                .into_inner();
            Ok(json!({
                "ok": response.ok,
                "message": response.message,
            }))
        }
        AuthCommand::Login { .. } => {
            Err("auth login is handled before the JSON command dispatcher".to_string())
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

/// `udb auth login` inputs. The password is never an argument: it is read from
/// `password_file` and lives only in the request.
pub(crate) struct LoginArgs {
    pub(crate) username: String,
    pub(crate) password_file: String,
    pub(crate) tenant: String,
    pub(crate) project: String,
    pub(crate) out: String,
    pub(crate) addr: String,
}

pub(crate) enum LoginOutcome {
    /// No `--out`: the access token, for stdout.
    Printed(String),
    /// `--out`: where the token was written, without the token.
    Written(serde_json::Value),
}

/// Longest password file `udb auth login` reads.
const LOGIN_PASSWORD_FILE_LIMIT: u64 = 4096;

/// Sign in through AuthnService.Login. No bearer from the environment rides along:
/// Login carries its credential in the request.
async fn run_login(args: &LoginArgs) -> Result<LoginOutcome, String> {
    require("username", &args.username)?;
    require("password-file", &args.password_file)?;
    let password = read_login_password(Path::new(args.password_file.trim()))?;
    let target = if args.addr.trim().is_empty() {
        auth_target()
    } else {
        let addr = client_target_addr(&args.addr);
        if addr.starts_with("http://") || addr.starts_with("https://") {
            addr
        } else {
            format!("http://{addr}")
        }
    };
    let mut client = AuthnServiceClient::connect(target.clone())
        .await
        .map_err(|err| format!("failed to connect to authn service at {target}: {err}"))?;
    let tenant = args.tenant.trim();
    let mut request = Request::new(authn_pb::LoginRequest {
        username: args.username.trim().to_string(),
        password,
        tenant_hint: tenant.to_string(),
        project_hint: args.project.trim().to_string(),
        device_name: "udb-cli".to_string(),
        ..Default::default()
    });
    super::authz_cli::attach_correlation_id(&mut request);
    if !tenant.is_empty() {
        let value = tenant
            .parse()
            .map_err(|_| "--tenant is not a valid gRPC metadata value".to_string())?;
        request.metadata_mut().insert("x-tenant-id", value);
    }
    let response = client
        .login(request)
        .await
        .map_err(|status| format!("login refused: {} ({:?})", status.message(), status.code()))?
        .into_inner();
    if response.access_token.is_empty() {
        return Err(if response.mfa_required {
            "the account requires a second factor; `udb auth login` does not complete MFA"
                .to_string()
        } else {
            "the broker returned no access token".to_string()
        });
    }
    if args.out.trim().is_empty() {
        return Ok(LoginOutcome::Printed(response.access_token));
    }
    let out = Path::new(args.out.trim());
    write_owner_only(out, response.access_token.as_bytes())?;
    Ok(LoginOutcome::Written(json!({
        "written": out.to_string_lossy(),
        "user_id": response.user_id,
        "expires_in_seconds": response.access_token_expires_in,
    })))
}

/// Read the password: one non-empty line from a regular file (not a symlink) of
/// bounded size, which on Unix only its owner can read. Errors never include the
/// file's contents.
fn read_login_password(path: &Path) -> Result<String, String> {
    let meta = std::fs::symlink_metadata(path)
        .map_err(|_| format!("--password-file {} is unavailable", path.display()))?;
    if !meta.file_type().is_file() {
        return Err(
            "--password-file must be a regular file, not a symlink or directory".to_string(),
        );
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o077 != 0 {
            return Err(format!(
                "--password-file {} is readable by others; chmod 600 it",
                path.display()
            ));
        }
    }
    if meta.len() > LOGIN_PASSWORD_FILE_LIMIT {
        return Err("--password-file is larger than a password".to_string());
    }
    let raw = std::fs::read_to_string(path)
        .map_err(|_| "--password-file could not be read as UTF-8 text".to_string())?;
    let password = raw.trim_end_matches(['\r', '\n']);
    if password.is_empty() || password.chars().any(char::is_control) {
        return Err("--password-file must contain the password on one non-empty line".to_string());
    }
    Ok(password.to_string())
}

/// Write `bytes` to `path` readable only by its owner (0600 on Unix). The file is
/// written beside `path` and renamed over it, so a reader never sees a partial
/// token and an existing token file is replaced whole.
fn write_owner_only(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write as _;
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let name = path
        .file_name()
        .ok_or_else(|| "--out must name a file".to_string())?;
    let mut tmp_name = std::ffi::OsString::from(".");
    tmp_name.push(name);
    tmp_name.push(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let tmp = parent.join(tmp_name);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = (|| {
        let mut file = options
            .open(&tmp)
            .map_err(|err| format!("cannot create {}: {err}", tmp.display()))?;
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|err| format!("cannot write {}: {err}", tmp.display()))?;
        drop(file);
        std::fs::rename(&tmp, path).map_err(|err| format!("cannot write {}: {err}", path.display()))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

async fn authn_client() -> Result<AuthnServiceClient<Channel>, String> {
    AuthnServiceClient::connect(auth_target())
        .await
        .map_err(|err| format!("failed to connect to authn service: {err}"))
}

async fn tenant_client() -> Result<TenantServiceClient<Channel>, String> {
    TenantServiceClient::connect(auth_target())
        .await
        .map_err(|err| format!("failed to connect to tenant service: {err}"))
}

/// The tenants a `membership create` approves into: the one named by
/// `--target-tenant`, or with `--all-children` every DIRECT child of the
/// caller's tenant (`UDB_TENANT_ID`, the tenant the operator token is for).
async fn membership_targets(target: &str, all_children: bool) -> Result<Vec<String>, String> {
    match (target.trim(), all_children) {
        ("", false) => Err("--target-tenant or --all-children is required".to_string()),
        (_, true) if !target.trim().is_empty() => {
            Err("--target-tenant and --all-children are exclusive".to_string())
        }
        (target, false) => Ok(vec![target.to_string()]),
        (_, true) => {
            let parent = env::var("UDB_TENANT_ID").unwrap_or_default();
            let parent = parent.trim();
            if parent.is_empty() {
                return Err(
                    "--all-children needs UDB_TENANT_ID: the tenant whose direct children                      are approved (the operator token's tenant)"
                        .to_string(),
                );
            }
            let mut client = tenant_client().await?;
            let mut children = Vec::new();
            let mut page_token = String::new();
            loop {
                let response = client
                    .list_tenants(with_metadata(tenant_pb::ListTenantsRequest {
                        page_size: 200,
                        page_token: page_token.clone(),
                        ..Default::default()
                    }))
                    .await
                    .map_err(|error| format!("listing child tenants failed: {error}"))?
                    .into_inner();
                children.extend(direct_children(&response.tenants, parent));
                if response.next_page_token.is_empty() {
                    break;
                }
                page_token = response.next_page_token;
            }
            if children.is_empty() {
                return Err(format!(
                    "tenant {parent} has no child tenants to approve into"
                ));
            }
            Ok(children)
        }
    }
}

/// The ids of `tenants` whose direct parent is `parent`.
fn direct_children(
    tenants: &[udb::proto::udb::core::tenant::entity::v1::Tenant],
    parent: &str,
) -> Vec<String> {
    tenants
        .iter()
        .filter(|tenant| tenant.parent_tenant_id.trim().eq_ignore_ascii_case(parent))
        .filter(|tenant| !tenant.tenant_id.trim().eq_ignore_ascii_case(parent))
        .map(|tenant| tenant.tenant_id.trim().to_string())
        .collect()
}

/// `expires_at` for a membership `days` from now.
fn membership_expiry(days: u64) -> Result<prost_types::Timestamp, String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|err| format!("system clock before the epoch: {err}"))?
        .as_secs();
    Ok(prost_types::Timestamp {
        seconds: i64::try_from(now + days * 86_400).unwrap_or(i64::MAX),
        nanos: 0,
    })
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
    super::authz_cli::with_metadata(message)
}

fn require_role_binding(principal: &str, role: &str, tenant: &str) -> Result<(), String> {
    require("principal", principal)?;
    require("role", role)?;
    require("tenant", tenant)?;
    // The enforcer matches the bare code: a role-gated policy for `app_rw`
    // never matches a `role:app_rw` binding.
    if role.trim().starts_with("role:") {
        return Err(format!(
            "--role takes the bare role code (`{}`), not `{role}`",
            role.trim().trim_start_matches("role:")
        ));
    }
    Ok(())
}

/// PutRoleBinding: the grouping tuple the data-plane enforcer reads.
async fn put_role_binding(
    principal: String,
    role: String,
    tenant: String,
    project: String,
    expires_at_unix: i64,
    verb: &str,
) -> Result<serde_json::Value, String> {
    let mut client = authz_client().await?;
    let response = client
        .put_role_binding(with_metadata(authz_pb::PutRoleBindingRequest {
            binding: Some(authz_pb::RoleBinding {
                subject: principal.trim().to_string(),
                role: role.trim().to_string(),
                tenant: tenant.trim().to_string(),
                project: project.trim().to_string(),
                expires_at_unix,
                source: "udb-cli".to_string(),
            }),
        }))
        .await
        .map_err(|err| format!("role {verb} failed: {err}"))?
        .into_inner();
    Ok(json!({
        "ok": response.ok,
        "message": response.message,
        "principal": principal.trim(),
        "role": role.trim(),
        "tenant": tenant.trim(),
        "project": if project.trim().is_empty() { "*".to_string() } else { project.trim().to_string() },
        "expires_at_unix": expires_at_unix,
        "unbound": verb == "unbind",
    }))
}

fn role_json(role: &authz_entity_pb::Role) -> serde_json::Value {
    json!({
        "role_id": role.role_id,
        "role_code": role.role_code,
        "name": role.name,
        "description": role.description,
        "tenant_id": role.tenant_id,
        "project_id": role.project_id,
        "domain": role.domain,
        "is_active": role.is_active,
        "is_system": role.is_system,
    })
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

#[cfg(test)]
mod login_tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "udb-auth-login-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir.join(name)
    }

    #[cfg(unix)]
    fn set_mode(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
    }

    #[test]
    fn token_file_is_owner_only_and_replaced_whole() {
        let out = scratch("token");
        write_owner_only(&out, b"first-token").expect("first write");
        write_owner_only(&out, b"second").expect("replace");
        assert_eq!(std::fs::read(&out).expect("read"), b"second");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&out).expect("meta").permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "token file mode {mode:o}");
        }
        let leftovers: Vec<_> = std::fs::read_dir(out.parent().unwrap())
            .expect("dir")
            .flatten()
            .filter(|entry| entry.file_name() != "token")
            .collect();
        assert!(leftovers.is_empty(), "temporary files left behind");
        let _ = std::fs::remove_dir_all(out.parent().unwrap());
    }

    #[test]
    fn password_file_yields_one_line_and_never_echoes_it() {
        let file = scratch("password");
        std::fs::write(&file, "s3cret-Pass\n").expect("write");
        #[cfg(unix)]
        set_mode(&file, 0o600);
        assert_eq!(read_login_password(&file).expect("read"), "s3cret-Pass");

        for bad in ["", "\n", "two\nlines\n"] {
            std::fs::write(&file, bad).expect("write");
            let err = read_login_password(&file).unwrap_err();
            assert!(err.contains("one non-empty line"), "{err}");
            assert!(!err.contains("two"), "the error echoed the file: {err}");
        }
        let missing = read_login_password(&file.with_file_name("absent")).unwrap_err();
        assert!(missing.contains("unavailable"), "{missing}");
        let _ = std::fs::remove_dir_all(file.parent().unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn password_file_readable_by_others_is_refused() {
        let file = scratch("password");
        std::fs::write(&file, "s3cret-Pass").expect("write");
        set_mode(&file, 0o644);
        let err = read_login_password(&file).unwrap_err();
        assert!(err.contains("chmod 600"), "{err}");
        assert!(
            !err.contains("s3cret"),
            "the error echoed the password: {err}"
        );
        let _ = std::fs::remove_dir_all(file.parent().unwrap());
    }

    #[test]
    fn login_requires_username_and_password_file_before_any_connection() {
        let args = LoginArgs {
            username: String::new(),
            password_file: "pw".into(),
            tenant: String::new(),
            project: String::new(),
            out: String::new(),
            addr: "127.0.0.1:1".into(),
        };
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let err = runtime.block_on(run_login(&args)).err().expect("refused");
        assert_eq!(err, "--username is required");
        let args = LoginArgs {
            username: "svc".into(),
            password_file: String::new(),
            ..args
        };
        let err = runtime.block_on(run_login(&args)).err().expect("refused");
        assert_eq!(err, "--password-file is required");
    }
}

#[cfg(test)]
mod membership_cli_tests {
    use super::*;
    use udb::proto::udb::core::tenant::entity::v1::Tenant;

    fn tenant(id: &str, parent: &str) -> Tenant {
        Tenant {
            tenant_id: id.to_string(),
            parent_tenant_id: parent.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn all_children_means_direct_children_of_the_callers_tenant() {
        let listed = vec![
            tenant("platform", ""),
            tenant("doctor-a", "platform"),
            tenant("doctor-b", "PLATFORM"),
            tenant("clinic-of-a", "doctor-a"),
        ];
        assert_eq!(
            direct_children(&listed, "platform"),
            vec!["doctor-a".to_string(), "doctor-b".to_string()],
            "the caller's own row and grandchildren are not approved into"
        );
        assert!(direct_children(&listed, "nobody").is_empty());
    }

    #[test]
    fn a_membership_expiry_is_days_from_now() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs() as i64;
        let expiry = membership_expiry(30).expect("expiry");
        assert!((expiry.seconds - now - 30 * 86_400).abs() <= 2);
    }
}
