//! Delegated job tokens: a durable async job acts for the user who started it.
//!
//! A service (a service-account bearer holding `udb:token:delegate`) presents
//! a user's current access token and exchanges it (`ExchangeTokenForJob`, in
//! the style of RFC 8693 token exchange) for a token that:
//!
//! - names the USER as `sub`, in the user's tenant and project;
//! - names the SERVICE in the RFC 8693 `act` claim (`act.sub` = the grant's
//!   service identity, `act.account_id` = the service account);
//! - carries the requested `purpose` (signed, so `purpose_verified` holds when
//!   it is used), the `job_id`, and `delegated: true`;
//! - carries only the scopes BOTH the user's token and the service's grant
//!   allow (never `udb:token:delegate` or the tenant-exchange scope);
//! - lives at most `UDB_DELEGATED_TOKEN_MAX_TTL` (24 hours by default), has no
//!   refresh token, and is never exchanged again.
//!
//! Every use re-checks, through the shared JWT persisted-state check (and its
//! short verdict cache), that the user is still active and not revoked, that
//! the jti is not revoked, and that the acting service still holds an ACTIVE
//! grant with `udb:token:delegate` on an ACTIVE service account
//! ([`validate_delegated_actor`]). Every mint and every use is audited with
//! the actor, the subject and the jti.

use sqlx::PgPool;
use tonic::{Code, Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::runtime::security::{MEMBERSHIP_TOKEN_AUTH_METHOD, SecurityClaims};

use super::authn::{AuthnServiceImpl, JwtPersistedState};
use super::events::{AuthEvent, ComplianceEnvelope};
use super::grants::{
    STATUS_ACTIVE, event_auth_method, get_grant_by_user, grants_invalid_fields,
    owner_is_active_service_account,
};
use super::memberships::{EXCHANGE_TENANT_TOKEN_SCOPE, MEMBERSHIP_TOKEN_JTI_PREFIX};
use super::now_unix;

/// The scope a service's typed grant (and its bearer) must carry, literally,
/// to mint delegated job tokens. Never carried by a delegated token.
pub(crate) const DELEGATE_TOKEN_SCOPE: &str = "udb:token:delegate";

/// `jti` prefix of every delegated job token (followed by a new UUID).
pub(crate) const DELEGATED_TOKEN_JTI_PREFIX: &str = "dlg_";

/// A delegated job token was minted.
pub(crate) const TOPIC_TOKEN_DELEGATED: &str = "udb.authn.token.delegated.v1";
/// A delegated job token was accepted (published when its live state is
/// checked, so at most once per verdict-cache window per token).
pub(crate) const TOPIC_DELEGATED_TOKEN_USED: &str = "udb.authn.token.delegated.used.v1";

const MAX_JOB_ID_BYTES: usize = 200;
const MAX_PURPOSE_BYTES: usize = 64;

// ── Statuses ─────────────────────────────────────────────────────────────────

fn delegating_service_required_status() -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        Code::PermissionDenied,
        "exchange_token_for_job",
        "delegated_token_requires_service",
        "minting a delegated job token requires a service account's own bearer whose ACTIVE grant holds udb:token:delegate",
    )
}

fn subject_token_refused_status(decision: &str, message: &str) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        Code::PermissionDenied,
        "exchange_token_for_job",
        decision,
        message,
    )
}

// ── Pure validation ──────────────────────────────────────────────────────────

/// `job_id`: required, at most 200 bytes, no control characters.
pub(crate) fn validated_job_id(raw: &str) -> Result<String, Status> {
    let value = raw.trim();
    if value.is_empty() || value.len() > MAX_JOB_ID_BYTES || value.chars().any(char::is_control) {
        return Err(grants_invalid_fields(
            "job_id is required, at most 200 bytes, without control characters",
            [(
                "job_id",
                "must be a non-empty job id of at most 200 bytes without control characters",
            )],
        ));
    }
    Ok(value.to_string())
}

/// `purpose`: required, at most 64 bytes, no whitespace or control
/// characters (for example `export`).
pub(crate) fn validated_purpose(raw: &str) -> Result<String, Status> {
    let value = raw.trim();
    if value.is_empty()
        || value.len() > MAX_PURPOSE_BYTES
        || value.chars().any(|c| c.is_control() || c.is_whitespace())
    {
        return Err(grants_invalid_fields(
            "purpose is required, at most 64 bytes, without spaces or control characters",
            [(
                "purpose",
                "must be a non-empty purpose such as \"export\", at most 64 bytes",
            )],
        ));
    }
    Ok(value.to_string())
}

/// The token's lifetime: the requested `ttl_seconds` (0 = the maximum),
/// never more than `max_ttl_secs`. A negative request is refused.
pub(crate) fn delegated_token_ttl(requested: i64, max_ttl_secs: u64) -> Result<u64, Status> {
    let max = max_ttl_secs.max(1);
    match u64::try_from(requested) {
        Ok(0) => Ok(max),
        Ok(secs) => Ok(secs.min(max)),
        Err(_) => Err(grants_invalid_fields(
            "ttl_seconds must not be negative",
            [(
                "ttl_seconds",
                "must be 0 (the configured maximum) or a positive number of seconds",
            )],
        )),
    }
}

/// The subject token as sent, with or without a `Bearer ` prefix.
fn subject_token_text(raw: &str) -> Result<&str, Status> {
    let token = raw.trim();
    let token = token
        .strip_prefix("Bearer ")
        .or_else(|| token.strip_prefix("bearer "))
        .unwrap_or(token)
        .trim();
    if token.is_empty() {
        return Err(grants_invalid_fields(
            "subject_token is required",
            [("subject_token", "must be the user's current access token")],
        ));
    }
    Ok(token)
}

/// Whether the user's token scopes cover `scope`: the scope itself, a full
/// wildcard (`*`, `udb:*`), or a prefix wildcard (`udb:vector:*`).
fn user_scopes_cover(user_scopes: &[String], scope: &str) -> bool {
    user_scopes.iter().any(|held| {
        let held = held.trim();
        held.eq_ignore_ascii_case(scope)
            || held == "*"
            || held == "udb:*"
            || held.strip_suffix('*').is_some_and(|prefix| {
                prefix.ends_with(':')
                    && scope
                        .to_ascii_lowercase()
                        .starts_with(&prefix.to_ascii_lowercase())
            })
    })
}

/// The scopes of a delegated token: every scope of the service's grant that
/// the user's token also allows, in grant order, minus the scopes that mint
/// further tokens (`udb:token:delegate`, `udb:authn:exchange-tenant-token`).
/// Grant scopes are never wildcards or admin scopes, so neither is the result.
pub(crate) fn delegated_scopes(user_scopes: &[String], grant_scopes: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for scope in grant_scopes {
        let scope = scope.trim();
        if scope.is_empty()
            || scope.eq_ignore_ascii_case(DELEGATE_TOKEN_SCOPE)
            || scope.eq_ignore_ascii_case(EXCHANGE_TENANT_TOKEN_SCOPE)
            || !user_scopes_cover(user_scopes, scope)
            || out.iter().any(|kept| kept.eq_ignore_ascii_case(scope))
        {
            continue;
        }
        out.push(scope.to_string());
    }
    out
}

/// The delegation claims of a verified token, checked for shape: every
/// delegated token UDB mints has all of them. `None` when one is missing or
/// malformed (the token is then refused outright).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DelegatedTokenClaims {
    pub subject: String,
    pub tenant_id: String,
    pub actor_service_identity: String,
    pub actor_account_id: Uuid,
    pub job_id: String,
    pub purpose: String,
    pub jti: String,
}

impl DelegatedTokenClaims {
    pub(crate) fn from_claims(claims: &SecurityClaims) -> Option<Self> {
        let text = |value: Option<&str>| {
            value
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };
        if claims.delegated != Some(true) {
            return None;
        }
        let act = claims.act.as_ref()?;
        let subject = text(claims.sub.as_deref())?;
        Uuid::parse_str(&subject).ok()?;
        let jti = text(claims.jti.as_deref())?;
        if !jti.starts_with(DELEGATED_TOKEN_JTI_PREFIX) {
            return None;
        }
        Some(Self {
            subject,
            tenant_id: text(claims.tenant_id.as_deref())?,
            actor_service_identity: text(act.sub.as_deref())?,
            actor_account_id: Uuid::parse_str(text(act.account_id.as_deref())?.as_str()).ok()?,
            job_id: text(claims.job_id.as_deref())?,
            purpose: text(claims.purpose.as_deref())?,
            jti,
        })
    }
}

/// Is the acting service of a delegated token still allowed to act? Its
/// grant in the token's tenant must be ACTIVE, for the same service identity,
/// still holding `udb:token:delegate`, on an ACTIVE service account. `Err` is
/// a store failure (the caller fails closed on it).
pub(crate) async fn validate_delegated_actor(
    pool: &PgPool,
    delegation: &DelegatedTokenClaims,
) -> Result<bool, String> {
    let grant = get_grant_by_user(
        pool,
        &delegation.tenant_id,
        &delegation.actor_account_id.to_string(),
    )
    .await
    .map_err(|status| {
        crate::runtime::executor_utils::store_string_from_status(
            "delegated token actor grant lookup failed",
            &status,
        )
    })?;
    let Some(grant) = grant.filter(|grant| {
        grant.status == STATUS_ACTIVE
            && grant.service_identity.trim() == delegation.actor_service_identity
            && grant
                .scopes
                .iter()
                .any(|scope| scope.trim().eq_ignore_ascii_case(DELEGATE_TOKEN_SCOPE))
    }) else {
        return Ok(false);
    };
    owner_is_active_service_account(pool, &grant.user_id, &grant.tenant_id).await
}

// ── Events ───────────────────────────────────────────────────────────────────

/// One delegated-token event (minted or used): actor = the service account,
/// target = the user, credential = the delegated token's jti.
pub(crate) fn build_delegation_event(
    topic: &'static str,
    operation: &str,
    delegation: &DelegatedTokenClaims,
    project_id: &str,
    scopes: &[String],
    expires_at_unix: i64,
) -> AuthEvent {
    AuthEvent::new(
        topic,
        delegation.jti.clone(),
        delegation.tenant_id.clone(),
        serde_json::json!({
            "jti": delegation.jti,
            "subject": delegation.subject,
            "actor": delegation.actor_service_identity,
            "actor_account_id": delegation.actor_account_id.to_string(),
            "tenant_id": delegation.tenant_id,
            "project_id": project_id,
            "job_id": delegation.job_id,
            "purpose": delegation.purpose,
            "scopes": scopes,
            "expires_at_unix": expires_at_unix,
        }),
    )
    .with_correlation(format!("delegated_token:{}", delegation.jti))
    .with_compliance(ComplianceEnvelope {
        actor: delegation.actor_account_id.to_string(),
        target_resource: format!("user:{}", delegation.subject),
        target_tenant: delegation.tenant_id.clone(),
        target_project: project_id.to_string(),
        operation: operation.to_string(),
        outcome: "success".to_string(),
        reason_code: operation.to_string(),
        auth_method: event_auth_method(),
        credential_id: delegation.jti.clone(),
        service_identity: delegation.actor_service_identity.clone(),
        ..ComplianceEnvelope::default()
    })
}

// ── Handler ──────────────────────────────────────────────────────────────────

/// `ExchangeTokenForJob`: mint a delegated job token for the user behind
/// `subject_token`, acting as the calling service.
pub(crate) async fn exchange_token_for_job(
    svc: &AuthnServiceImpl,
    pool: &PgPool,
    request: Request<authn_pb::ExchangeTokenForJobRequest>,
) -> Result<Response<authn_pb::ExchangeTokenForJobResponse>, Status> {
    let req = request.into_inner();
    // There is no acting service without a verified claim.
    if !crate::runtime::service::method_security::claim_context_present() {
        return Err(delegating_service_required_status());
    }
    let ctx = crate::runtime::service::method_security::current_claim_context();
    // A service account's own bearer, holding the delegate scope literally.
    // Never a tenant-membership token or a delegated token.
    if !ctx.is_service_account_bearer()
        || !ctx.has_exact_scope(DELEGATE_TOKEN_SCOPE)
        || ctx.credential_id.starts_with(MEMBERSHIP_TOKEN_JTI_PREFIX)
        || ctx.credential_id.starts_with(DELEGATED_TOKEN_JTI_PREFIX)
        || ctx
            .auth_method
            .eq_ignore_ascii_case(MEMBERSHIP_TOKEN_AUTH_METHOD)
        || ctx
            .auth_method
            .eq_ignore_ascii_case(crate::runtime::security::DELEGATED_TOKEN_AUTH_METHOD)
    {
        return Err(delegating_service_required_status());
    }
    let Ok(actor_account) = Uuid::parse_str(ctx.subject.trim()) else {
        return Err(delegating_service_required_status());
    };
    let tenant = ctx.tenant_id.trim().to_string();
    if tenant.is_empty() {
        return Err(delegating_service_required_status());
    }
    let purpose = validated_purpose(&req.purpose)?;
    let job_id = validated_job_id(&req.job_id)?;
    let ttl = delegated_token_ttl(req.ttl_seconds, svc.delegated_token_max_ttl_secs())?;
    let subject_token = subject_token_text(&req.subject_token)?;

    // The service's grant must still stand, for this exact identity, with the
    // delegate scope, on an ACTIVE service account.
    let grant = get_grant_by_user(pool, &tenant, &actor_account.to_string()).await?;
    let Some(grant) = grant.filter(|grant| {
        grant.status == STATUS_ACTIVE
            && grant.service_identity.trim() == ctx.service_identity.trim()
            && grant
                .scopes
                .iter()
                .any(|scope| scope.trim().eq_ignore_ascii_case(DELEGATE_TOKEN_SCOPE))
    }) else {
        return Err(delegating_service_required_status());
    };
    if !owner_is_active_service_account(pool, &grant.user_id, &grant.tenant_id)
        .await
        .map_err(|error| {
            crate::runtime::executor_utils::internal_status(
                "authn",
                "exchange_token_for_job_account",
                error,
            )
        })?
    {
        return Err(delegating_service_required_status());
    }

    // The subject token: a valid, unexpired USER access token of the same
    // tenant, still backed by live state, and not itself delegated.
    let now = now_unix();
    let claims = svc.verify_delegation_subject_token(subject_token)?;
    if claims.is_delegated() {
        return Err(subject_token_refused_status(
            "delegated_token_not_exchangeable",
            "subject_token is itself a delegated job token; a delegated token is never exchanged again",
        ));
    }
    let is_service_token = claims
        .service_identity
        .as_deref()
        .is_some_and(|identity| !identity.trim().is_empty())
        || claims.account_kind == Some(authn_entity_pb::AccountKind::ServiceAccount as i32)
        || claims.mbr.is_some()
        || claims.home_tenant_id.is_some();
    let subject = claims.sub.as_deref().unwrap_or_default().trim().to_string();
    if is_service_token || Uuid::parse_str(&subject).is_err() {
        return Err(subject_token_refused_status(
            "delegated_token_requires_user_token",
            "subject_token must be a user's own access token, not a service or exchanged token",
        ));
    }
    if claims.tenant_id.as_deref().unwrap_or_default().trim() != tenant {
        return Err(subject_token_refused_status(
            "delegated_token_tenant_mismatch",
            "subject_token belongs to another tenant than the calling service",
        ));
    }
    if let Some(own_purpose) = claims
        .purpose
        .as_deref()
        .map(str::trim)
        .filter(|own| !own.is_empty())
        && own_purpose != purpose
    {
        return Err(grants_invalid_fields(
            "purpose differs from the purpose the subject token already states",
            [(
                "purpose",
                "must equal the subject token's own purpose when it states one",
            )],
        ));
    }
    if matches!(
        svc.jwt_persisted_state_user(&claims, now).await?,
        JwtPersistedState::Rejected
    ) {
        return Err(subject_token_refused_status(
            "delegated_token_subject_revoked",
            "subject_token is revoked, or its user is no longer active",
        ));
    }

    let scopes = delegated_scopes(&claims.resolved_scopes(), &grant.scopes);
    if scopes.is_empty() {
        return Err(subject_token_refused_status(
            "delegated_token_no_common_scope",
            "the user's token and the service's grant have no scope in common to delegate",
        ));
    }
    let project_id = claims
        .project_id
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_string();
    let delegation = DelegatedTokenClaims {
        subject,
        tenant_id: tenant,
        actor_service_identity: grant.service_identity.trim().to_string(),
        actor_account_id: actor_account,
        job_id,
        purpose,
        jti: format!("{DELEGATED_TOKEN_JTI_PREFIX}{}", Uuid::new_v4()),
    };
    let (access_token, expires_at_unix) = svc.issue_delegated_access_token(
        &delegation,
        &project_id,
        &scopes,
        claims.account_kind.unwrap_or(0),
        now,
        ttl,
    )?;
    tracing::info!(
        target: "udb.audit.authn",
        subject = %delegation.subject,
        tenant_id = %delegation.tenant_id,
        actor = %delegation.actor_service_identity,
        actor_account_id = %delegation.actor_account_id,
        jti = %delegation.jti,
        job_id = %delegation.job_id,
        purpose = %delegation.purpose,
        expires_at_unix,
        "service minted a delegated job token for a user"
    );
    svc.emit_event(build_delegation_event(
        TOPIC_TOKEN_DELEGATED,
        "exchange_token_for_job",
        &delegation,
        &project_id,
        &scopes,
        expires_at_unix,
    ))
    .await;
    Ok(Response::new(authn_pb::ExchangeTokenForJobResponse {
        access_token,
        jti: delegation.jti,
        expires_at_unix,
        subject: delegation.subject,
        actor: delegation.actor_service_identity,
        purpose: delegation.purpose,
        job_id: delegation.job_id,
        scopes,
        tenant_id: delegation.tenant_id,
        project_id,
        message: "delegated job token issued".to_string(),
        error: None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| item.to_string()).collect()
    }

    #[test]
    fn job_id_and_purpose_are_required_and_bounded() {
        assert_eq!(validated_job_id(" export-42 ").unwrap(), "export-42");
        assert!(validated_job_id("").is_err());
        assert!(validated_job_id(&"j".repeat(201)).is_err());
        assert!(validated_job_id(&"j".repeat(200)).is_ok());
        assert!(validated_job_id("a\nb").is_err());
        assert_eq!(validated_purpose("export").unwrap(), "export");
        assert!(validated_purpose("").is_err());
        assert!(validated_purpose("two words").is_err());
        assert!(validated_purpose(&"p".repeat(65)).is_err());
    }

    #[test]
    fn ttl_is_capped_by_the_setting_and_never_negative() {
        assert_eq!(delegated_token_ttl(0, 86_400).unwrap(), 86_400);
        assert_eq!(delegated_token_ttl(600, 86_400).unwrap(), 600);
        assert_eq!(delegated_token_ttl(999_999, 86_400).unwrap(), 86_400);
        assert_eq!(delegated_token_ttl(10, 0).unwrap(), 1);
        let err = delegated_token_ttl(-1, 86_400).unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[test]
    fn scopes_are_what_both_the_user_and_the_grant_allow_minus_minting_scopes() {
        let grant = strings(&[
            "udb:read",
            "udb:write",
            "udb:vector:query",
            DELEGATE_TOKEN_SCOPE,
            EXCHANGE_TENANT_TOKEN_SCOPE,
        ]);
        // An owner's wildcard covers every grant scope, never the minting ones.
        assert_eq!(
            delegated_scopes(&strings(&["udb:admin", "udb:*"]), &grant),
            strings(&["udb:read", "udb:write", "udb:vector:query"])
        );
        // An exact user scope set narrows it; a prefix wildcard covers its family.
        assert_eq!(
            delegated_scopes(&strings(&["udb:read", "udb:vector:*"]), &grant),
            strings(&["udb:read", "udb:vector:query"])
        );
        // A user with the delegate scope still never passes it on.
        assert_eq!(
            delegated_scopes(&strings(&[DELEGATE_TOKEN_SCOPE]), &grant),
            Vec::<String>::new()
        );
        assert!(delegated_scopes(&strings(&["udb:stream"]), &grant).is_empty());
    }

    #[test]
    fn subject_token_accepts_a_bearer_prefix_and_refuses_blank() {
        assert_eq!(subject_token_text("Bearer abc.def").unwrap(), "abc.def");
        assert_eq!(subject_token_text(" abc.def ").unwrap(), "abc.def");
        assert_eq!(
            subject_token_text("  ").unwrap_err().code(),
            Code::InvalidArgument
        );
    }

    fn delegated_claims() -> SecurityClaims {
        SecurityClaims {
            sub: Some(Uuid::new_v4().to_string()),
            tenant_id: Some("acme".to_string()),
            project_id: Some("billing".to_string()),
            jti: Some(format!("{DELEGATED_TOKEN_JTI_PREFIX}{}", Uuid::new_v4())),
            purpose: Some("export".to_string()),
            job_id: Some("export-42".to_string()),
            delegated: Some(true),
            act: Some(crate::runtime::security::ActorClaim {
                sub: Some("spiffe://acme/exporter".to_string()),
                account_id: Some(Uuid::new_v4().to_string()),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn delegation_claims_must_all_be_present() {
        let full = delegated_claims();
        let parsed = DelegatedTokenClaims::from_claims(&full).expect("well-formed");
        assert_eq!(parsed.job_id, "export-42");
        assert_eq!(parsed.actor_service_identity, "spiffe://acme/exporter");
        let broken = [
            SecurityClaims {
                delegated: None,
                ..full.clone()
            },
            SecurityClaims {
                act: None,
                ..full.clone()
            },
            SecurityClaims {
                job_id: None,
                ..full.clone()
            },
            SecurityClaims {
                purpose: Some(" ".to_string()),
                ..full.clone()
            },
            SecurityClaims {
                jti: Some("sess_abc".to_string()),
                ..full.clone()
            },
            SecurityClaims {
                sub: Some("not-a-uuid".to_string()),
                ..full.clone()
            },
            SecurityClaims {
                act: Some(crate::runtime::security::ActorClaim {
                    sub: Some("spiffe://acme/exporter".to_string()),
                    account_id: Some("nope".to_string()),
                }),
                ..full.clone()
            },
        ];
        for claims in broken {
            assert!(
                DelegatedTokenClaims::from_claims(&claims).is_none(),
                "{claims:?}"
            );
            // Any one marker still makes it a delegated token (never a plain
            // user token).
            assert!(claims.is_delegated());
        }
    }

    #[tokio::test]
    async fn persisted_state_rejects_delegated_claims_out_of_shape() {
        let svc = AuthnServiceImpl::new(
            crate::runtime::authn::AuthnConfig::default(),
            crate::runtime::security::SecurityConfig::default(),
        );
        let now = 1_800_000_000;
        let full = delegated_claims();
        // Out of shape: refused outright, no store needed.
        for claims in [
            SecurityClaims {
                job_id: None,
                ..full.clone()
            },
            // A delegated token never carries a service identity.
            SecurityClaims {
                service_identity: Some("spiffe://acme/exporter".to_string()),
                ..full.clone()
            },
        ] {
            assert!(matches!(
                svc.jwt_persisted_state_user(&claims, now)
                    .await
                    .expect("shape rejection needs no store"),
                JwtPersistedState::Rejected
            ));
        }
    }

    #[tokio::test]
    async fn only_a_service_bearer_with_the_delegate_scope_may_mint() {
        use crate::runtime::service::method_security::{
            VerifiedClaimContext, scope_claim_context_for_test,
        };
        let svc = AuthnServiceImpl::new(
            crate::runtime::authn::AuthnConfig::default(),
            crate::runtime::security::SecurityConfig::default(),
        );
        let service = VerifiedClaimContext {
            subject: Uuid::new_v4().to_string(),
            service_identity: "spiffe://acme/exporter".to_string(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            scopes: strings(&[DELEGATE_TOKEN_SCOPE]),
            roles: Vec::new(),
            // CREDENTIAL_TYPE_SERVICE_ACCOUNT
            credential_type: 4,
            credential_id: "sess_x".to_string(),
            auth_method: "pwd".to_string(),
            authenticated: true,
        };
        let refused = [
            // A human bearer.
            VerifiedClaimContext {
                service_identity: String::new(),
                credential_type: 1,
                ..service.clone()
            },
            // No delegate scope (admin scopes do not imply it).
            VerifiedClaimContext {
                scopes: strings(&["udb:admin", "udb:*"]),
                ..service.clone()
            },
            // A tenant-membership token.
            VerifiedClaimContext {
                credential_id: format!("{MEMBERSHIP_TOKEN_JTI_PREFIX}abc"),
                ..service.clone()
            },
            // A delegated token presented as the caller.
            VerifiedClaimContext {
                credential_id: format!("{DELEGATED_TOKEN_JTI_PREFIX}abc"),
                ..service.clone()
            },
        ];
        // Refused before any store access: the pool handed in is never touched.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@127.0.0.1:1/unused")
            .expect("lazy pool");
        let request = || {
            Request::new(authn_pb::ExchangeTokenForJobRequest {
                subject_token: "a.b.c".to_string(),
                purpose: "export".to_string(),
                job_id: "export-42".to_string(),
                ttl_seconds: 600,
            })
        };
        for ctx in refused {
            let err =
                scope_claim_context_for_test(ctx, exchange_token_for_job(&svc, &pool, request()))
                    .await
                    .expect_err("only a service bearer with udb:token:delegate may mint");
            assert_eq!(err.code(), Code::PermissionDenied);
        }
        // Without any verified claim there is no acting service.
        let err = exchange_token_for_job(&svc, &pool, request())
            .await
            .expect_err("no claim, no delegated token");
        assert_eq!(err.code(), Code::PermissionDenied);
        // A request that fails validation is refused before the store, too.
        let err = scope_claim_context_for_test(
            service,
            exchange_token_for_job(
                &svc,
                &pool,
                Request::new(authn_pb::ExchangeTokenForJobRequest {
                    subject_token: "a.b.c".to_string(),
                    purpose: "export".to_string(),
                    job_id: String::new(),
                    ttl_seconds: 600,
                }),
            ),
        )
        .await
        .expect_err("job_id is required");
        assert_eq!(err.code(), Code::InvalidArgument);
    }
}
