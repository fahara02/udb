//! Delegated `CheckAccess` / `BatchCheckPermissions` ("check access for").
//!
//! A claim-bound decision call normally evaluates the CALLER: a non-admin may
//! only ask the PDP about itself. A backend service that gates its own
//! endpoints on UDB policy needs to ask "may end user U do A on R?" for the
//! human it is serving. This module admits exactly that, and nothing wider:
//!
//! - the caller is a service account (a verified service-account credential
//!   carrying its grant's service identity) — a human or a raw API key holding
//!   the scope is refused;
//! - the caller carries `udb:authz:check-access-for` LITERALLY — broad admin
//!   scopes and `udb:*` do not imply it;
//! - the subject is an ACTIVE `PERSON` in the caller's own claim tenant; a
//!   missing subject, a subject in another tenant and a non-person all get the
//!   SAME refusal, so the call cannot probe which ids exist elsewhere;
//! - the decision is evaluated in the token's tenant and project only.
//!
//! The evaluated principal is built fresh from the subject id: no caller
//! identity, role, scope or service identity is copied onto it (the decision
//! engine OR-matches every identity of a principal, so a copied caller id would
//! let the caller's own policies answer for the subject). Roles come from the
//! subject's bindings in the authz snapshot. Because the subject carries no
//! scopes, a policy that declares `required_scopes` never allows a delegated
//! check — the subject's token scopes are not known to the PDP.
//!
//! Every delegated decision, allow and deny, is written to the access-decision
//! audit with the caller in `evaluated_by` / `evaluated_by_service_identity`,
//! and published as `udb.authz.check.access.delegated.v1`.

use tonic::Status;
use uuid::Uuid;

use super::super::events::{AuthEvent, ComplianceEnvelope, topics};
use super::{AuthzServiceImpl, authz_capability_status};
use crate::runtime::authz::{Decision, Principal, ResourceRef};
use crate::runtime::native_catalog::native_model;
use crate::runtime::service::method_security::VerifiedClaimContext;

/// The scope a service account must carry, literally, to evaluate a decision
/// for another user of its tenant.
pub(super) const CHECK_ACCESS_FOR_SCOPE: &str = "udb:authz:check-access-for";

/// The service caller on whose behalf a delegated decision was evaluated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DelegatedEvaluation {
    /// The caller's verified subject (the service account's user id).
    pub(super) evaluated_by: String,
    /// The caller's grant service identity.
    pub(super) evaluated_by_service_identity: String,
}

impl DelegatedEvaluation {
    pub(super) fn from_claim(ctx: &VerifiedClaimContext) -> Self {
        Self {
            evaluated_by: ctx.subject.trim().to_string(),
            evaluated_by_service_identity: ctx.service_identity.trim().to_string(),
        }
    }
}

fn delegated_check_requires_service_account_status() -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        "check_access_delegated",
        "delegated_check_requires_service_account",
        "checking access for another user requires a service-account credential",
    )
}

/// The ONE refusal for a subject that is missing, in another tenant, not a
/// person or not active — identical so the call reveals nothing about ids
/// outside the caller's tenant.
pub(super) fn delegated_subject_not_in_tenant_status() -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        "check_access_delegated",
        "delegated_subject_not_in_tenant",
        "the requested user is not an active person in the caller's tenant",
    )
}

fn delegated_subject_invalid_status() -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        "user_id must be a UUID to check access for another user",
        [("user_id", "must be a UUID user id")],
    )
}

/// Decide whom a claim-bound (non-cross-tenant-admin) decision evaluates.
///
/// - `Ok(None)`: the caller itself — the request names no other user, or the
///   caller does not hold [`CHECK_ACCESS_FOR_SCOPE`] (the pre-existing
///   rebinding: a body `user_id` never selects another subject).
/// - `Ok(Some(uuid))`: a delegated evaluation of that user, still subject to
///   [`AuthzServiceImpl::resolve_delegated_subject`].
/// - `Err`: the caller holds the scope but is not a service account
///   (`PermissionDenied`), or the requested id is not a UUID
///   (`InvalidArgument`).
pub(super) fn delegated_check_subject(
    ctx: &VerifiedClaimContext,
    requested_user_id: &str,
) -> Result<Option<String>, Status> {
    let requested = requested_user_id.trim();
    if requested.is_empty() || requested == ctx.subject.trim() {
        return Ok(None);
    }
    if !ctx.has_exact_scope(CHECK_ACCESS_FOR_SCOPE) {
        return Ok(None);
    }
    // Authorization before validation: a caller that may not delegate learns
    // nothing about how the id would have been parsed.
    if !ctx.is_service_account_bearer() {
        return Err(delegated_check_requires_service_account_status());
    }
    let subject = Uuid::parse_str(requested).map_err(|_| delegated_subject_invalid_status())?;
    let subject = subject.to_string();
    if subject == ctx.subject.trim().to_ascii_lowercase() {
        return Ok(None);
    }
    Ok(Some(subject))
}

/// The principal a delegated decision evaluates: built FRESH from the subject
/// id, scoped to the caller's claim tenant and project. Nothing of the caller
/// is copied — no identity, no roles (the snapshot supplies the subject's), no
/// scopes, no service identity.
pub(super) fn delegated_principal(subject: &str, ctx: &VerifiedClaimContext) -> Principal {
    Principal {
        principal_id: subject.to_string(),
        subject: subject.to_string(),
        user_id: subject.to_string(),
        service_identity: String::new(),
        tenant_id: ctx.tenant_id.trim().to_string(),
        project_id: ctx.project_id.trim().to_string(),
        scopes: Vec::new(),
        roles: Vec::new(),
        provider_id: String::new(),
        auth_method: String::new(),
    }
}

/// Is `subject` an ACTIVE `PERSON` whose tenant is exactly `claim_tenant`?
/// `Err` is reserved for a store failure (the caller fails closed on it).
async fn person_is_active_in_tenant(
    pool: &sqlx::PgPool,
    subject: Uuid,
    claim_tenant: &str,
) -> Result<bool, Status> {
    let um = native_model(
        "udb.core.authn.entity.v1.User",
        &[
            "user_id",
            "tenant_id",
            "account_kind",
            "status",
            "deleted_at",
        ],
    );
    let sql = format!(
        "SELECT 1 FROM {rel} WHERE {uid} = $1 AND {tenant} = $2 \
           AND {kind} = 'PERSON' AND {status} = 'ACTIVE' AND {deleted} IS NULL",
        rel = um.relation,
        uid = um.q("user_id"),
        tenant = um.q("tenant_id"),
        kind = um.q("account_kind"),
        status = um.q("status"),
        deleted = um.q("deleted_at"),
    );
    let row = sqlx::query(&sql)
        .bind(subject)
        .bind(claim_tenant)
        .fetch_optional(pool)
        .await
        .map_err(|error| {
            crate::runtime::executor_utils::sqlx_error_to_status(
                "check_access_delegated_subject",
                &error,
            )
        })?;
    Ok(row.is_some())
}

/// Test seam: unit tests install the user directory a delegated lookup sees
/// (no Postgres), matched with the same predicate as the SQL above.
#[cfg(test)]
#[derive(Debug, Clone)]
pub(super) struct TestDelegatedUser {
    pub(super) user_id: &'static str,
    pub(super) tenant_id: &'static str,
    pub(super) account_kind: &'static str,
    pub(super) status: &'static str,
}

#[cfg(test)]
tokio::task_local! {
    pub(super) static TEST_DELEGATED_USERS: Vec<TestDelegatedUser>;
}

impl AuthzServiceImpl {
    /// Admit `subject` for a delegated decision in `claim_tenant`, or refuse
    /// with [`delegated_subject_not_in_tenant_status`]. No Postgres store ⇒
    /// capability error; a store failure keeps its classified status (fail
    /// closed, never an evaluation of an unverified subject).
    pub(super) async fn resolve_delegated_subject(
        &self,
        claim_tenant: &str,
        subject: &str,
    ) -> Result<(), Status> {
        let claim_tenant = claim_tenant.trim();
        let Ok(subject) = Uuid::parse_str(subject.trim()) else {
            return Err(delegated_subject_not_in_tenant_status());
        };
        if claim_tenant.is_empty() {
            return Err(delegated_subject_not_in_tenant_status());
        }
        #[cfg(test)]
        if let Ok(found) = TEST_DELEGATED_USERS.try_with(|users| {
            users.iter().any(|user| {
                Uuid::parse_str(user.user_id).ok() == Some(subject)
                    && user.tenant_id == claim_tenant
                    && user.account_kind == "PERSON"
                    && user.status == "ACTIVE"
            })
        }) {
            return if found {
                Ok(())
            } else {
                Err(delegated_subject_not_in_tenant_status())
            };
        }
        let pool = self.pg_pool.as_ref().ok_or_else(|| {
            authz_capability_status(
                "check_access_delegated",
                "postgres_auth_store",
                "checking access for another user requires the Postgres-backed user store",
            )
        })?;
        match person_is_active_in_tenant(pool, subject, claim_tenant).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(delegated_subject_not_in_tenant_status()),
            // The store refusal keeps its classified status (never a subject
            // verdict): the delegated check fails closed.
            Err(status) => Err(status),
        }
    }

    /// Publish one delegated decision (allow or deny) as
    /// `udb.authz.check.access.delegated.v1`: actor = the service caller,
    /// target = the evaluated subject. The typed audit row is written by
    /// `write_decision_audit`; this is the compliance-plane event.
    pub(super) async fn emit_delegated_decision_event(
        &self,
        delegation: &DelegatedEvaluation,
        subject: &Principal,
        resource: &ResourceRef,
        action: &str,
        decision: &Decision,
        correlation_id: &str,
    ) {
        let outcome = if decision.allowed { "allow" } else { "deny" };
        let correlation = if correlation_id.trim().is_empty() {
            format!(
                "check_access_delegated:{}:{}",
                delegation.evaluated_by, subject.subject
            )
        } else {
            correlation_id.to_string()
        };
        self.emit_event(
            AuthEvent::new(
                topics::CHECK_ACCESS_DELEGATED,
                delegation.evaluated_by.clone(),
                subject.tenant_id.clone(),
                serde_json::json!({
                    "evaluated_subject": subject.subject.clone(),
                    "evaluated_by": delegation.evaluated_by.clone(),
                    "evaluated_by_service_identity":
                        delegation.evaluated_by_service_identity.clone(),
                    "tenant_id": subject.tenant_id.clone(),
                    "project_id": subject.project_id.clone(),
                    "resource": resource.resource_name.clone(),
                    "action": action,
                    "allowed": decision.allowed,
                    "decision_id": decision.decision_id.clone(),
                    "matched_policy_ids": decision.matched_policy_ids.clone(),
                }),
            )
            .with_correlation(correlation)
            .with_compliance(ComplianceEnvelope {
                actor: delegation.evaluated_by.clone(),
                actor_project: subject.project_id.clone(),
                target_resource: subject.subject.clone(),
                target_tenant: subject.tenant_id.clone(),
                target_project: subject.project_id.clone(),
                operation: "check_access_delegated".to_string(),
                outcome: outcome.to_string(),
                reason_code: if decision.allowed {
                    "delegated_allow".to_string()
                } else {
                    "delegated_deny".to_string()
                },
                service_identity: delegation.evaluated_by_service_identity.clone(),
                decision_id: decision.decision_id.clone(),
                policy_version: decision.policy_version.clone(),
                relationship_version: decision.relationship_version.clone(),
                ..ComplianceEnvelope::default()
            }),
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::udb::core::authz::services::v1 as authz_pb;
    use crate::proto::udb::core::authz::services::v1::authz_service_server::AuthzService;
    use crate::runtime::authz::{AuthzPolicy, AuthzSnapshot, Effect};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use crate::runtime::service::method_security::scope_claim_context_for_test;
    use tonic::{Code, Request};

    const CALLER: &str = "5b0c4e3e-0d1a-4f43-9c43-6f7d1d5a0001";
    const PATIENT: &str = "5b0c4e3e-0d1a-4f43-9c43-6f7d1d5a0002";
    const OTHER_TENANT_PATIENT: &str = "5b0c4e3e-0d1a-4f43-9c43-6f7d1d5a0003";
    const SERVICE_USER: &str = "5b0c4e3e-0d1a-4f43-9c43-6f7d1d5a0004";
    const UNKNOWN: &str = "5b0c4e3e-0d1a-4f43-9c43-6f7d1d5a0005";

    fn directory() -> Vec<TestDelegatedUser> {
        vec![
            TestDelegatedUser {
                user_id: PATIENT,
                tenant_id: "tenant-a",
                account_kind: "PERSON",
                status: "ACTIVE",
            },
            TestDelegatedUser {
                user_id: OTHER_TENANT_PATIENT,
                tenant_id: "tenant-b",
                account_kind: "PERSON",
                status: "ACTIVE",
            },
            TestDelegatedUser {
                user_id: SERVICE_USER,
                tenant_id: "tenant-a",
                account_kind: "SERVICE_ACCOUNT",
                status: "ACTIVE",
            },
        ]
    }

    fn service_ctx(scopes: &[&str]) -> VerifiedClaimContext {
        VerifiedClaimContext {
            subject: CALLER.to_string(),
            service_identity: "spiffe://tenant-a/meddesk".to_string(),
            tenant_id: "tenant-a".to_string(),
            project_id: "clinic".to_string(),
            scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
            roles: Vec::new(),
            // CREDENTIAL_TYPE_SERVICE_ACCOUNT
            credential_type: 4,
            credential_id: "mbx_test".to_string(),
            auth_method: "api_key".to_string(),
            authenticated: true,
        }
    }

    fn human_ctx(scopes: &[&str]) -> VerifiedClaimContext {
        VerifiedClaimContext {
            service_identity: String::new(),
            // CREDENTIAL_TYPE_BEARER_JWT
            credential_type: 1,
            ..service_ctx(scopes)
        }
    }

    /// The patient may read charts; the CALLER (the service account) may delete
    /// them.
    fn service() -> AuthzServiceImpl {
        let mut snap = AuthzSnapshot::default();
        snap.version = "delegation-v1".to_string();
        snap.policies.push(AuthzPolicy {
            id: "patient-read".to_string(),
            effect: Effect::Allow,
            tenant: "tenant-a".to_string(),
            subject: PATIENT.to_string(),
            action: "read".to_string(),
            resource: "chart".to_string(),
            ..Default::default()
        });
        snap.policies.push(AuthzPolicy {
            id: "caller-delete".to_string(),
            effect: Effect::Allow,
            tenant: "tenant-a".to_string(),
            subject: CALLER.to_string(),
            action: "delete".to_string(),
            resource: "chart".to_string(),
            ..Default::default()
        });
        AuthzServiceImpl::new(snap)
    }

    fn check(user_id: &str, action: &str) -> Request<authz_pb::CheckAccessRequest> {
        Request::new(authz_pb::CheckAccessRequest {
            user_id: user_id.to_string(),
            object: "chart".to_string(),
            action: action.to_string(),
            ..Default::default()
        })
    }

    async fn check_as(
        ctx: VerifiedClaimContext,
        user_id: &str,
        action: &str,
    ) -> Result<authz_pb::CheckAccessResponse, Status> {
        let svc = service();
        TEST_DELEGATED_USERS
            .scope(
                directory(),
                scope_claim_context_for_test(ctx, svc.check_access(check(user_id, action))),
            )
            .await
            .map(|response| response.into_inner())
    }

    fn policy_decision_id(status: &Status) -> String {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw).policy_decision_id
    }

    #[test]
    fn exact_scope_never_falls_back_to_admin_or_wildcard() {
        assert!(service_ctx(&[CHECK_ACCESS_FOR_SCOPE]).has_exact_scope(CHECK_ACCESS_FOR_SCOPE));
        assert!(
            service_ctx(&[" UDB:AUTHZ:CHECK-ACCESS-FOR "]).has_exact_scope(CHECK_ACCESS_FOR_SCOPE)
        );
        for scopes in [
            &["udb:admin"][..],
            &["udb:*"][..],
            &["udb:authz:*"][..],
            &["udb:authz:check-access"][..],
        ] {
            assert!(
                !service_ctx(scopes).has_exact_scope(CHECK_ACCESS_FOR_SCOPE),
                "{scopes:?} must not imply {CHECK_ACCESS_FOR_SCOPE}"
            );
        }
    }

    #[test]
    fn delegated_principal_copies_nothing_from_the_caller() {
        let ctx = VerifiedClaimContext {
            roles: vec!["clinic_staff".to_string()],
            ..service_ctx(&["udb:authz:check-access", CHECK_ACCESS_FOR_SCOPE])
        };
        let principal = delegated_principal(PATIENT, &ctx);
        assert_eq!(principal.principal_id, PATIENT);
        assert_eq!(principal.subject, PATIENT);
        assert_eq!(principal.user_id, PATIENT);
        assert!(principal.service_identity.is_empty());
        assert!(principal.roles.is_empty());
        assert!(principal.scopes.is_empty());
        assert_eq!(principal.tenant_id, "tenant-a");
        assert_eq!(principal.project_id, "clinic");
    }

    #[test]
    fn delegated_check_subject_matrix() {
        let scoped = service_ctx(&[CHECK_ACCESS_FOR_SCOPE]);
        assert_eq!(delegated_check_subject(&scoped, "").unwrap(), None);
        assert_eq!(delegated_check_subject(&scoped, CALLER).unwrap(), None);
        assert_eq!(
            delegated_check_subject(&scoped, &PATIENT.to_ascii_uppercase()).unwrap(),
            Some(PATIENT.to_string())
        );
        assert_eq!(
            delegated_check_subject(&service_ctx(&[]), PATIENT).unwrap(),
            None
        );
        let raw_key = VerifiedClaimContext {
            // CREDENTIAL_TYPE_API_KEY
            credential_type: 3,
            ..service_ctx(&[CHECK_ACCESS_FOR_SCOPE])
        };
        assert!(delegated_check_subject(&raw_key, PATIENT).is_err());
    }

    #[tokio::test]
    async fn scoped_service_account_evaluates_a_same_tenant_person() {
        let response = check_as(
            service_ctx(&["udb:authz:check-access", CHECK_ACCESS_FOR_SCOPE]),
            PATIENT,
            "read",
        )
        .await
        .expect("delegated check evaluates the subject");
        assert!(response.allowed, "the patient's own policy allows read");
        assert_eq!(response.evaluated_subject, PATIENT);
        assert_eq!(response.matched_rule, "patient-read");
    }

    #[tokio::test]
    async fn the_callers_own_policy_never_answers_for_the_subject() {
        // The caller may delete charts; the patient may not. A delegated check
        // answers for the patient only.
        let response = check_as(
            service_ctx(&["udb:authz:check-access", CHECK_ACCESS_FOR_SCOPE]),
            PATIENT,
            "delete",
        )
        .await
        .expect("a delegated deny is a decision, not an error");
        assert!(!response.allowed);
        assert_eq!(response.evaluated_subject, PATIENT);
    }

    #[tokio::test]
    async fn without_the_scope_the_check_rebinds_to_the_caller() {
        for scopes in [&["udb:authz:check-access"][..], &["udb:admin"][..]] {
            let response = check_as(service_ctx(scopes), PATIENT, "delete")
                .await
                .expect("a rebinding check answers for the caller");
            assert_eq!(
                response.evaluated_subject, CALLER,
                "{scopes:?}: a body user_id never selects another subject"
            );
            // The caller's own policy answered.
            assert!(response.allowed);
        }
    }

    #[tokio::test]
    async fn a_human_holding_the_scope_is_refused() {
        let err = check_as(
            human_ctx(&["udb:authz:check-access", CHECK_ACCESS_FOR_SCOPE]),
            PATIENT,
            "read",
        )
        .await
        .expect_err("only service accounts may delegate");
        assert_eq!(err.code(), Code::PermissionDenied);
        assert_eq!(
            policy_decision_id(&err),
            "delegated_check_requires_service_account"
        );
    }

    #[tokio::test]
    async fn a_non_uuid_subject_is_invalid() {
        let err = check_as(
            service_ctx(&["udb:authz:check-access", CHECK_ACCESS_FOR_SCOPE]),
            "patient-7",
            "read",
        )
        .await
        .expect_err("delegated subjects are UUID user ids");
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn missing_cross_tenant_and_non_person_subjects_get_one_identical_refusal() {
        let ctx = service_ctx(&["udb:authz:check-access", CHECK_ACCESS_FOR_SCOPE]);
        let mut refusals = Vec::new();
        for subject in [OTHER_TENANT_PATIENT, UNKNOWN, SERVICE_USER] {
            let err = check_as(ctx.clone(), subject, "read")
                .await
                .expect_err("a subject that is not a person of the caller's tenant is refused");
            assert_eq!(err.code(), Code::PermissionDenied, "{subject}");
            assert_eq!(policy_decision_id(&err), "delegated_subject_not_in_tenant");
            refusals.push(err.message().to_string());
        }
        assert!(
            refusals.windows(2).all(|pair| pair[0] == pair[1]),
            "the refusal must not reveal which case applied: {refusals:?}"
        );
        assert!(!refusals[0].contains(OTHER_TENANT_PATIENT));
    }

    #[tokio::test]
    async fn batch_check_delegates_like_check_access() {
        let request = Request::new(authz_pb::BatchCheckPermissionsRequest {
            user_id: PATIENT.to_string(),
            domain: "tenant-a".to_string(),
            checks: vec![
                authz_pb::PermissionCheck {
                    object: "chart".to_string(),
                    action: "read".to_string(),
                },
                authz_pb::PermissionCheck {
                    object: "chart".to_string(),
                    action: "delete".to_string(),
                },
            ],
            context: None,
        });
        let svc = service();
        let results = TEST_DELEGATED_USERS
            .scope(
                directory(),
                scope_claim_context_for_test(
                    service_ctx(&["udb:authz:batch-check", CHECK_ACCESS_FOR_SCOPE]),
                    svc.batch_check_permissions(request),
                ),
            )
            .await
            .expect("a delegated batch evaluates the subject")
            .into_inner()
            .results;
        assert_eq!(results.get("chart:read"), Some(&true));
        // The caller's delete policy does not answer for the patient.
        assert_eq!(results.get("chart:delete"), Some(&false));
    }
}
