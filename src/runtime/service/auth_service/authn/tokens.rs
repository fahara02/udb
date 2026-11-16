//! JWT issuance, token introspection, and the JWK Set endpoint.

use super::*;

/// Named role→scope projection (auth_fix.md Block 1, Decisions D/E): the coarse
/// control-plane scopes a role confers in the issued token. The role binding is
/// the single source of truth; the scope is a derived claim. Extend this ONE map
/// (e.g. add `tenant_admin`) — never scatter role→scope knowledge elsewhere.
const ROLE_SCOPE_PROJECTIONS: &[(&str, &[&str])] =
    // `organization_owner` is the tenant superuser, so it projects to BOTH the
    // coarse control-plane admin scope (`udb:admin`, matched literally by
    // method-security ADMIN_SCOPES) AND the broad `udb:*` wildcard, which
    // `has_scope` expands to satisfy every data-plane scope gate (udb:write,
    // udb:read, udb:stream, udb:dispatch, udb:vector:*, udb:object:*, …). Without
    // `udb:*` the admin passes the admin gates but fails the per-op data scopes.
    &[
        ("organization_owner", &["udb:admin", "udb:*"]),
        // This role can enter the snapshot only from an active, system-owned,
        // global role row; Authz hydration rejects tenant-created lookalikes.
        // Project the control-plane scope required by the transport gate and
        // the explicit platform marker consumed by cross-tenant handlers.
        (
            "platform_admin",
            &["udb:admin", "udb:*", "udb:platform_admin"],
        ),
    ];

/// Tenant/project domain match mirroring the authz snapshot's private
/// `domain_match` (empty or `*` is a wildcard, else exact) without reaching into
/// the authz module. Role bindings loaded from `user_roles` carry a concrete
/// tenant and (usually) an empty project, so this stays exact-or-wildcard.
fn domain_allows(pattern: &str, value: &str) -> bool {
    let pattern = pattern.trim();
    pattern.is_empty() || pattern == "*" || pattern == value
}

/// A role code a service-account bearer may NEVER carry, even when an operator
/// bound it to the account: a workload credential must not hold platform,
/// owner, admin or break-glass authority through a claim role. Mirrors the
/// segment rules of the service-scope validator (wildcard / admin / owner /
/// superuser / root segments, owner aliases) and adds every role code that
/// projects scopes into a token ([`ROLE_SCOPE_PROJECTIONS`]) plus the
/// break-glass role. Fail-closed: a blank code is forbidden.
pub(super) fn is_service_forbidden_role(role: &str) -> bool {
    let role = role.trim();
    if role.is_empty() {
        return true;
    }
    if crate::runtime::service::method_security::is_platform_authority_role(role) {
        return true;
    }
    if ROLE_SCOPE_PROJECTIONS
        .iter()
        .any(|(code, _)| role.eq_ignore_ascii_case(code))
    {
        return true;
    }
    let normalized = role.to_ascii_lowercase().replace(['-', '.', ':'], "_");
    if normalized.contains("break_glass") || normalized.contains("breakglass") {
        return true;
    }
    normalized.split('_').any(|segment| {
        matches!(
            segment,
            "*" | "admin" | "administrator" | "owner" | "superuser" | "superadmin" | "root"
        )
    }) || matches!(
        normalized.as_str(),
        "organization_owner"
            | "organisation_owner"
            | "tenant_owner"
            | "account_owner"
            | "org_owner"
    )
}

/// Role bindings resolved for a service-account bearer, split into the codes
/// the bearer carries and the codes withheld by [`is_service_forbidden_role`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct ServiceAccountRoles {
    pub(super) kept: Vec<String>,
    pub(super) withheld: Vec<String>,
}

impl AuthnServiceImpl {
    /// Mint a tenant-membership access token (`ExchangeServiceTokenForTenant`)
    /// for service account `subject` in the target `tenant_id`/`project_id`.
    /// It carries the account's role bindings in the TARGET tenant (filtered
    /// like every service-account bearer), the home grant's service identity,
    /// and the membership pin; `ttl_secs` is already bounded by the caller.
    /// Returns `(token, exp)`; a missing signing key is a capability error
    /// (there is no session fallback for an exchanged token).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn issue_membership_access_token(
        &self,
        subject: &str,
        tenant_id: &str,
        project_id: &str,
        scopes: &[String],
        service_identity: &str,
        jti: &str,
        pin: crate::runtime::security::MembershipPin<'_>,
        now: u64,
        ttl_secs: u64,
    ) -> Result<(String, i64), Status> {
        let roles = self
            .service_account_bearer_roles(subject, tenant_id, project_id, "tenant_token_exchange")
            .await;
        match crate::runtime::security::sign_membership_access_token(
            &self.security,
            subject,
            tenant_id,
            project_id,
            scopes,
            &roles,
            service_identity,
            jti,
            authn_entity_pb::AccountKind::ServiceAccount as i32,
            now,
            ttl_secs,
            pin,
        ) {
            Ok(Some(signed)) => Ok(signed),
            Ok(None) => Err(authn_capability_status(
                "exchange_service_token_for_tenant",
                "jwt_signing_key",
                "exchanging for a tenant token requires a configured JWT signing key",
            )),
            Err(err) => Err(authn_internal_status(
                "exchange_service_token_for_tenant_sign",
                err,
            )),
        }
    }

    /// Test hook: put one token's jti on the durable revocation deny list, as
    /// a single-token revoke would.
    #[cfg(test)]
    pub(crate) async fn revoke_jti_for_test(
        &self,
        jti: &str,
        tenant_id: &str,
        expires_at_unix: u64,
    ) -> Result<(), Status> {
        self.revoke_token_jti(jti, "jwt_access", tenant_id, expires_at_unix, "", "test")
            .await?;
        // A served revoke publishes an auth event, which drops remembered
        // verdicts on this node; do the same.
        verdict_cache::invalidate();
        Ok(())
    }

    /// Longest lifetime of a delegated job token (`UDB_DELEGATED_TOKEN_MAX_TTL`).
    pub(crate) fn delegated_token_max_ttl_secs(&self) -> u64 {
        self.security.delegated_token_max_ttl_secs.max(1)
    }

    /// Verify the user's token presented to `ExchangeTokenForJob`: signature,
    /// `exp`, `nbf`, issuer, audience, as for any bearer. Live state (user,
    /// revocation) is checked by the caller. Every refusal is
    /// PERMISSION_DENIED: the caller (the service) is authenticated, it is the
    /// token it presents that cannot be used.
    pub(crate) fn verify_delegation_subject_token(
        &self,
        token: &str,
    ) -> Result<crate::runtime::security::SecurityClaims, Status> {
        validate_bearer_token(&self.security, token).map_err(|error| {
            let expired =
                crate::runtime::security::bearer_refusal_reason(&error).is_some_and(|reason| {
                    reason.code == crate::runtime::error_reasons::TOKEN_EXPIRED.code
                });
            let (decision, message) = if expired {
                (
                    "delegated_token_subject_expired",
                    "subject_token has expired; the user must sign in or refresh first",
                )
            } else {
                (
                    "delegated_token_subject_invalid",
                    "subject_token is not a valid access token",
                )
            };
            crate::runtime::executor_utils::policy_status_with_code(
                tonic::Code::PermissionDenied,
                "exchange_token_for_job",
                decision,
                message,
            )
        })
    }

    /// Mint a delegated job token (`ExchangeTokenForJob`) for
    /// `delegation.subject`, acting as `delegation.actor_*`. `ttl_secs` is
    /// already bounded by the caller. A missing signing key is a capability
    /// error (there is no session fallback for a delegated token).
    pub(crate) fn issue_delegated_access_token(
        &self,
        delegation: &super::super::delegated_tokens::DelegatedTokenClaims,
        project_id: &str,
        scopes: &[String],
        account_kind: i32,
        now: u64,
        ttl_secs: u64,
    ) -> Result<(String, i64), Status> {
        let actor_account_id = delegation.actor_account_id.to_string();
        match crate::runtime::security::sign_delegated_access_token(
            &self.security,
            &delegation.subject,
            &delegation.tenant_id,
            project_id,
            scopes,
            &delegation.jti,
            account_kind,
            now,
            ttl_secs,
            crate::runtime::security::DelegationClaims {
                actor_service_identity: &delegation.actor_service_identity,
                actor_account_id: &actor_account_id,
                job_id: &delegation.job_id,
                purpose: &delegation.purpose,
            },
        ) {
            Ok(Some(signed)) => Ok(signed),
            Ok(None) => Err(authn_capability_status(
                "exchange_token_for_job",
                "jwt_signing_key",
                "minting a delegated job token requires a configured JWT signing key",
            )),
            Err(err) => Err(authn_internal_status("exchange_token_for_job_sign", err)),
        }
    }

    pub(super) fn issue_access_token(
        &self,
        subject: &str,
        tenant_id: &str,
        project_id: &str,
        scopes: &[String],
        roles: &[String],
        service_identity: &str,
        jti: &str,
        auth_method: &str,
        account_kind: i32,
        now: u64,
    ) -> (String, i64) {
        // Rotation-aware signing: when the DB-backed signing-key registry has an
        // ACTIVE key (published into the process-global snapshot by
        // `refresh_signing_key_cache` at startup seed / on rotation), sign with
        // THAT key's decrypted PEM and stamp its `kid`, so a rotated key actually
        // signs (and JWKS — which publishes the same kid — verifies it). When the
        // registry is empty/unseeded we fall back to the env key + `UDB_RS256_KID`,
        // keeping single-key deployments unchanged.
        let signed =
            if let Some((kid, private_pem)) = crate::runtime::security::active_signing_key() {
                crate::runtime::security::sign_access_token_with_key(
                    &self.security,
                    subject,
                    tenant_id,
                    project_id,
                    scopes,
                    roles,
                    service_identity,
                    jti,
                    auth_method,
                    account_kind,
                    now,
                    &private_pem,
                    &kid,
                )
                .map(Some)
            } else {
                crate::runtime::security::sign_access_token(
                    &self.security,
                    subject,
                    tenant_id,
                    project_id,
                    scopes,
                    roles,
                    service_identity,
                    jti,
                    auth_method,
                    account_kind,
                    now,
                )
            };
        match signed {
            Ok(Some((token, exp))) => (token, exp),
            Ok(None) => (String::new(), 0),
            Err(err) => {
                tracing::warn!(error = %err, "access-token signing failed; falling back to session-only");
                (String::new(), 0)
            }
        }
    }

    /// Resolve a user's effective control-plane grants from the WARM shared
    /// authz snapshot (auth_fix.md Block 1, Decision E): the role codes bound to
    /// the user — read straight off the pub `role_bindings` the central plane
    /// loads (the same source `list_user_permissions` uses), NOT the private
    /// `effective_roles`, and with NO parallel `user_roles` query — and the
    /// scopes those roles project to via [`ROLE_SCOPE_PROJECTIONS`], unioned
    /// with the `token_scopes` each bound role configures
    /// ([`Self::bound_role_token_scopes`]). PERSON bearers only: service
    /// accounts resolve through [`Self::resolve_service_account_roles`].
    ///
    /// Returns `(scopes, roles)`. Fail-closed: no snapshot wired ⇒ no grants
    /// (never empty-as-admin). `empty ⇒ empty`, so revoking a user's last role
    /// drops the projected admin scope at the next issue/refresh. Scopes are
    /// projected at issue/refresh time only: after a role's scopes change, a
    /// bound user carries the new set once they log in again or refresh.
    pub(super) fn resolve_effective_grants(
        &self,
        user_id: &str,
        tenant_id: &str,
        project_id: &str,
    ) -> (Vec<String>, Vec<String>) {
        let roles = self.bound_role_codes(user_id, tenant_id, project_id);
        let mut scopes: Vec<String> = Vec::new();
        for role in &roles {
            for &(code, projected) in ROLE_SCOPE_PROJECTIONS {
                if code == role.as_str() {
                    for &scope in projected {
                        let scope = scope.to_string();
                        if !scopes.contains(&scope) {
                            scopes.push(scope);
                        }
                    }
                }
            }
        }
        for scope in self.bound_role_token_scopes(user_id, tenant_id, project_id) {
            if !scopes.contains(&scope) {
                scopes.push(scope);
            }
        }
        (scopes, roles)
    }

    /// Reload the shared snapshot before a sign-in that would otherwise issue a
    /// bearer with no roles, when the durable authz revision has moved past it.
    /// A binding written outside this process (an offline `udb auth bootstrap
    /// user`, another node) reaches this node only at the next interval reload,
    /// and a token issued before then keeps its missing scopes until it expires.
    /// A user with roles skips the revision read, so ordinary logins cost nothing.
    pub(super) async fn reload_snapshot_if_unbound_and_stale(
        &self,
        user_id: &str,
        tenant_id: &str,
        project_id: &str,
    ) {
        if !self
            .bound_role_codes(user_id, tenant_id, project_id)
            .is_empty()
        {
            return;
        }
        let (Some(cell), Some(pool)) = (self.authz_snapshot.as_ref(), self.pg_pool.as_ref()) else {
            return;
        };
        if !crate::runtime::authz::snapshot_behind_durable_revision().await {
            return;
        }
        super::super::AuthzServiceImpl::shared(cell.clone())
            .with_postgres(Some(pool.clone()))
            .warm_shared_snapshot()
            .await;
    }

    /// The configured `token_scopes` of every role bound to `user_id` in the
    /// tenant/project domain: a binding matched exactly as in
    /// [`Self::bound_role_codes`] contributes the scopes of the role row with
    /// the SAME role key, tenant and project, so a role's scopes never leak
    /// into another tenant or project. No snapshot wired ⇒ no scopes.
    fn bound_role_token_scopes(
        &self,
        user_id: &str,
        tenant_id: &str,
        project_id: &str,
    ) -> Vec<String> {
        let Some(cell) = self.authz_snapshot.as_ref() else {
            return Vec::new();
        };
        let snapshot = cell.load();
        let mut scopes: Vec<String> = Vec::new();
        for binding in &snapshot.role_bindings {
            if binding.subject.as_str() != user_id
                || !domain_allows(&binding.tenant, tenant_id)
                || !domain_allows(&binding.project, project_id)
            {
                continue;
            }
            for configured in &snapshot.role_token_scopes {
                // A role defined without a project applies to its bindings in
                // any project of the tenant; a project role needs that project.
                if configured.role == binding.role
                    && configured.tenant == binding.tenant
                    && (configured.project.is_empty() || configured.project == binding.project)
                {
                    for scope in &configured.scopes {
                        if !scopes.contains(scope) {
                            scopes.push(scope.clone());
                        }
                    }
                }
            }
        }
        scopes
    }

    /// The role codes bound to `user_id` in the tenant/project domain, read off
    /// the warm authz snapshot's `role_bindings` (the roles half of
    /// [`Self::resolve_effective_grants`]). No snapshot wired ⇒ no roles.
    fn bound_role_codes(&self, user_id: &str, tenant_id: &str, project_id: &str) -> Vec<String> {
        let Some(cell) = self.authz_snapshot.as_ref() else {
            return Vec::new();
        };
        let snapshot = cell.load();
        let mut roles: Vec<String> = Vec::new();
        for binding in &snapshot.role_bindings {
            if binding.subject.as_str() == user_id
                && domain_allows(&binding.tenant, tenant_id)
                && domain_allows(&binding.project, project_id)
                && !roles.contains(&binding.role)
            {
                roles.push(binding.role.clone());
            }
        }
        roles
    }

    /// The roles a service-account bearer carries: the codes bound to the
    /// account in its grant's tenant/project, minus every
    /// [`is_service_forbidden_role`] code. Roles ONLY — a service bearer's
    /// scopes come exclusively from its typed grant, never from
    /// [`ROLE_SCOPE_PROJECTIONS`] or a bound role's configured token scopes.
    pub(super) fn resolve_service_account_roles(
        &self,
        user_id: &str,
        tenant_id: &str,
        project_id: &str,
    ) -> ServiceAccountRoles {
        let (withheld, kept): (Vec<String>, Vec<String>) = self
            .bound_role_codes(user_id, tenant_id, project_id)
            .into_iter()
            .partition(|role| is_service_forbidden_role(role));
        ServiceAccountRoles { kept, withheld }
    }

    /// [`Self::resolve_service_account_roles`] for a bearer being minted on
    /// `surface` (password login, API-key exchange, refresh). A withheld role
    /// is reported — an audit-target warning plus an auth event — but never
    /// fails the issue: the account still authenticates, just without it.
    pub(super) async fn service_account_bearer_roles(
        &self,
        user_id: &str,
        tenant_id: &str,
        project_id: &str,
        surface: &'static str,
    ) -> Vec<String> {
        let resolved = self.resolve_service_account_roles(user_id, tenant_id, project_id);
        if !resolved.withheld.is_empty() {
            tracing::warn!(
                target: "udb.audit.authn",
                user_id = %user_id,
                tenant_id = %tenant_id,
                project_id = %project_id,
                surface,
                withheld_roles = ?resolved.withheld,
                "service-account bearer issued without role bindings a workload credential may never carry"
            );
            self.emit_event(
                AuthEvent::new(
                    topics::SERVICE_ACCOUNT_ROLE_WITHHELD,
                    user_id.to_string(),
                    tenant_id.to_string(),
                    serde_json::json!({
                        "user_id": user_id,
                        "tenant_id": tenant_id,
                        "project_id": project_id,
                        "surface": surface,
                        "withheld_roles": resolved.withheld.clone(),
                    }),
                )
                .with_correlation(format!("service_role_withheld:{user_id}"))
                .with_compliance(ComplianceEnvelope {
                    actor: user_id.to_string(),
                    actor_project: project_id.to_string(),
                    target_resource: user_id.to_string(),
                    operation: "service_account_token_issue".to_string(),
                    outcome: "roles_withheld".to_string(),
                    reason_code: "service_forbidden_role".to_string(),
                    auth_method: surface.to_string(),
                    ..ComplianceEnvelope::default()
                }),
            )
            .await;
        }
        resolved.kept
    }

    /// Build the JWK Set for verifying UDB-issued JWTs. Order of preference:
    ///   1. The DB-backed signing-key registry (ACTIVE + VERIFYING public keys),
    ///      so a just-rotated key is still verifiable during the overlap window.
    ///   2. A configured external JWKS URL (operator delegated hosting).
    ///   3. The single env public key (legacy single-key deployments).
    async fn jwks_document(&self) -> Result<String, Status> {
        const EMPTY: &str = "{\"keys\":[]}";
        // 1. Registry keys (Phase 3 / I2.1): publish every ACTIVE/VERIFYING key.
        //
        // Phase 5 fail-closed: a *store error* (not an empty registry) must not be
        // papered over with the legacy env public key in production — that would
        // FAIL OPEN, publishing a stale/wrong JWKS. In fail_closed_mode we surface
        // the error; in dev we log and fall through to the env key as before.
        let registry = match self.jwks_registry_keys().await {
            Ok(registry) => registry,
            Err(err) => {
                if crate::runtime::security::fail_closed_mode() {
                    return Err(err);
                }
                tracing::warn!(error = %err, "signing-key registry read failed; JWKS falls back to env key (dev fail-open)");
                Vec::new()
            }
        };
        if !registry.is_empty() {
            // Publish each registry key's OWN registered algorithm (not a hardcoded
            // RS256), so a key seeded/rotated under a different RSA alg advertises
            // the correct `alg` in JWKS.
            let keys: Vec<serde_json::Value> = registry
                .iter()
                .filter_map(|k| rsa_jwk_value_from_pem(&k.public_material, &k.key_id, &k.algorithm))
                .collect();
            if !keys.is_empty() {
                return Ok(serde_json::json!({ "keys": keys }).to_string());
            }
        }
        #[cfg(feature = "http-client")]
        if let Some(url) = self
            .security
            .jwt_jwks_url
            .as_ref()
            .filter(|u| !u.trim().is_empty())
        {
            // The operator delegated JWKS hosting — proxy it as the source of truth.
            return Ok(fetch_delegated_jwks(url.trim())
                .await
                .unwrap_or_else(|| EMPTY.to_string()));
        }
        if let Some(pem) = self.security.jwt_public_pem() {
            if let Some(jwk) = rsa_jwk_from_pem(&pem) {
                return Ok(jwk);
            }
        }
        Ok(EMPTY.to_string())
    }

    pub(super) async fn introspect_token_impl(
        &self,
        request: Request<authn_pb::IntrospectTokenRequest>,
    ) -> Result<Response<authn_pb::IntrospectTokenResponse>, Status> {
        let req = request.into_inner();
        // The kid is read from the JWT header before signature validation so we
        // can surface it even for an otherwise-rejected token.
        let key_id = jsonwebtoken::decode_header(&req.token)
            .ok()
            .and_then(|h| h.kid)
            .unwrap_or_default();
        let claims = match validate_bearer_token(&self.security, &req.token) {
            Ok(claims) => claims,
            Err(_) => {
                return Ok(Response::new(authn_pb::IntrospectTokenResponse {
                    active: false,
                    key_id,
                    ..Default::default()
                }));
            }
        };
        let now = now_unix();
        let jti = claims.jti.clone().unwrap_or_default();
        let session_id = if jti.starts_with("sess_") {
            jti.clone()
        } else {
            String::new()
        };
        let token_type = if jti.starts_with("sess_") {
            "session"
        } else {
            "jwt_access"
        };
        // Real introspection: a signature-valid, unexpired token is still only
        // `active` if it is not revoked and its persisted session/user state holds.
        let (revoked, revocation_reason) = self
            .is_token_revoked(
                &jti,
                claims.tenant_id.as_deref().unwrap_or_default(),
                claims.sub.as_deref().unwrap_or_default(),
                claims.iat.unwrap_or(0).max(0) as u64,
            )
            .await;
        // An unanswered revocation store is an outage, not an inactive token.
        if crate::runtime::authn::revocation::revocation_verdict(revoked, &revocation_reason)
            == crate::runtime::authn::revocation::RevocationVerdict::StoreUnavailable
        {
            return Err(crate::runtime::executor_utils::retryable_status(
                "authn",
                "revocation_lookup",
                crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                "the revocation store did not answer; introspection is refused (fail-closed) \
                 and can be retried",
            ));
        }
        let persisted_ok = self.jwt_persisted_state_valid(&claims, now).await?;
        let active = !revoked && persisted_ok;
        Ok(Response::new(authn_pb::IntrospectTokenResponse {
            active,
            subject: claims.sub.clone().unwrap_or_default(),
            tenant_id: claims.tenant_id.clone().unwrap_or_default(),
            service_identity: claims.service_identity.clone().unwrap_or_default(),
            scopes: claims.resolved_scopes(),
            // Real expiry surfaced from the validated claims when present.
            expires_at_unix: token_exp_unix(&req.token),
            key_id,
            token_type: token_type.to_string(),
            session_id,
            revocation_reason: if active {
                String::new()
            } else {
                revocation_reason
            },
        }))
    }

    pub(super) async fn get_jwks_impl(
        &self,
        _request: Request<authn_pb::GetJwksRequest>,
    ) -> Result<Response<authn_pb::GetJwksResponse>, Status> {
        Ok(Response::new(authn_pb::GetJwksResponse {
            jwks_json: self.jwks_document().await?,
        }))
    }
}

/// Derive an RFC 7517 JWK Set (single RS256 key) from an RSA public key in PEM
/// form (SPKI `BEGIN PUBLIC KEY` or PKCS#1 `BEGIN RSA PUBLIC KEY`). Returns
/// `None` for non-RSA / unparseable keys.
/// Best-effort extraction of the `exp` claim (unix seconds) from a JWT without
/// re-validating the signature (already validated by the caller). Returns 0 when
/// absent/unparseable.
fn token_exp_unix(token: &str) -> i64 {
    use base64::Engine;
    let Some(payload_b64) = token.split('.').nth(1) else {
        return 0;
    };
    let Ok(bytes) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload_b64) else {
        return 0;
    };
    serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|v| v.get("exp").and_then(|e| e.as_i64()))
        .unwrap_or(0)
}

fn rsa_jwk_from_pem(pem: &str) -> Option<String> {
    let jwk = rsa_jwk_value_from_pem(
        pem,
        crate::runtime::security::UDB_RS256_KID,
        crate::runtime::authn::signing_keys::DEFAULT_SIGNING_ALGORITHM,
    )?;
    Some(serde_json::json!({ "keys": [jwk] }).to_string())
}

/// Derive a single RFC 7517 JWK object from an RSA public PEM, stamping the given
/// `kid` and signing `alg`. Shared by the single-key env path (RS256) and the
/// registry path (which assigns one JWK per ACTIVE/VERIFYING key and publishes
/// that key's *own* registered algorithm). `None` for non-RSA / unparseable keys.
pub(super) fn rsa_jwk_value_from_pem(pem: &str, kid: &str, alg: &str) -> Option<serde_json::Value> {
    use base64::Engine;
    use rsa::RsaPublicKey;
    use rsa::pkcs1::DecodeRsaPublicKey;
    use rsa::pkcs8::DecodePublicKey;
    use rsa::traits::PublicKeyParts;

    let key = RsaPublicKey::from_public_key_pem(pem)
        .or_else(|_| RsaPublicKey::from_pkcs1_pem(pem))
        .ok()?;
    // An empty/unknown registered algorithm falls back to the default RSA alg so
    // legacy rows (and the env key) keep publishing a valid `alg`.
    let alg = if alg.trim().is_empty() {
        crate::runtime::authn::signing_keys::DEFAULT_SIGNING_ALGORITHM
    } else {
        alg.trim()
    };
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let n = b64.encode(key.n().to_bytes_be());
    let e = b64.encode(key.e().to_bytes_be());
    Some(serde_json::json!({
        "kty": "RSA",
        "use": "sig",
        "alg": alg,
        "kid": kid,
        "n": n,
        "e": e,
    }))
}

/// Fetch the operator's delegated JWKS document for republication.
///
/// The document decides which keys verify tokens, so it must not be readable
/// or rewritable on the way: a plaintext http:// URL to a non-loopback host is
/// refused when UDB_OUTBOUND_TLS_REQUIRED is on. The fetch has bounded connect
/// and request timeouts (a hung host used to hold the JWKS RPC open
/// indefinitely), follows no redirects, and republishes only a 2xx answer
/// whose body is a JWKS document. Anything else returns `None`, which the
/// caller turns into an empty key set (nothing verifies) rather than
/// republishing an error page.
#[cfg(feature = "http-client")]
async fn fetch_delegated_jwks(url: &str) -> Option<String> {
    if let Err(reason) = crate::runtime::transport_policy::check_secret_url(url, "jwt_jwks_url") {
        tracing::warn!(%reason, "delegated JWKS not fetched");
        return None;
    }
    let client = crate::runtime::security::outbound_secret_http_client_with_timeout(
        url,
        std::time::Duration::from_secs(10),
    )
    .map_err(|error| tracing::warn!(%error, "delegated JWKS not fetched"))
    .ok()?;
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|error| tracing::warn!(%error, "delegated JWKS fetch failed"))
        .ok()?;
    if !response.status().is_success() {
        tracing::warn!(status = %response.status(), "delegated JWKS host refused the fetch");
        return None;
    }
    let body = response.text().await.ok()?;
    is_jwks_document(&body).then_some(body)
}

/// Whether `body` is a JWKS document: a JSON object with a `keys` array.
#[cfg(feature = "http-client")]
fn is_jwks_document(body: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("keys").map(serde_json::Value::is_array))
        .unwrap_or(false)
}

#[cfg(all(test, feature = "http-client"))]
mod delegated_jwks_tests {
    use super::{fetch_delegated_jwks, is_jwks_document};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn one_shot_host(response: String) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback host");
        let address = listener.local_addr().expect("host address");
        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buffer = vec![0u8; 16 * 1024];
                let _ = socket.read(&mut buffer).await;
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        format!("http://{address}/jwks.json")
    }

    fn http_response(status: &str, extra_headers: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\n{extra_headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    #[test]
    fn only_a_keys_array_is_a_jwks_document() {
        assert!(is_jwks_document(r#"{"keys":[]}"#));
        assert!(!is_jwks_document(r#"{"error":"not found"}"#));
        assert!(!is_jwks_document("<html>gateway error</html>"));
    }

    #[tokio::test]
    async fn delegated_jwks_republishes_only_a_successful_jwks_answer() {
        let ok = one_shot_host(http_response("200 OK", "", r#"{"keys":[]}"#)).await;
        assert_eq!(
            fetch_delegated_jwks(&ok).await.as_deref(),
            Some(r#"{"keys":[]}"#)
        );

        let error_page =
            one_shot_host(http_response("500 Internal Server Error", "", "oops")).await;
        assert_eq!(fetch_delegated_jwks(&error_page).await, None);

        // A redirect is not followed, so its target is never republished.
        let redirect = one_shot_host(http_response(
            "302 Found",
            "Location: http://192.0.2.1/attacker-jwks.json\r\n",
            "",
        ))
        .await;
        assert_eq!(fetch_delegated_jwks(&redirect).await, None);
    }
}

#[cfg(test)]
mod jwk_tests {
    use super::{ROLE_SCOPE_PROJECTIONS, rsa_jwk_from_pem};

    const RS256_PUBLIC_PEM: &str = include_str!("../../../testdata/jwt_rs256_public.pem");

    #[test]
    fn rsa_jwk_from_pem_emits_valid_rs256_jwk() {
        let jwk = rsa_jwk_from_pem(RS256_PUBLIC_PEM).expect("RSA PEM should yield a JWK");
        let parsed: serde_json::Value = serde_json::from_str(&jwk).expect("JWK is valid JSON");
        let key = &parsed["keys"][0];
        assert_eq!(key["kty"], "RSA");
        assert_eq!(key["alg"], "RS256");
        assert_eq!(key["use"], "sig");
        // The JWKS kid must match the kid the JWT header carries.
        assert_eq!(key["kid"], crate::runtime::security::UDB_RS256_KID);
        // Base64url modulus/exponent must be present and non-trivial.
        assert!(key["n"].as_str().unwrap().len() > 64);
        assert!(!key["e"].as_str().unwrap().is_empty());
        // Base64url alphabet only (no '+', '/', or '=' padding).
        let n = key["n"].as_str().unwrap();
        assert!(!n.contains('+') && !n.contains('/') && !n.contains('='));
    }

    #[test]
    fn rsa_jwk_from_pem_rejects_garbage() {
        assert!(rsa_jwk_from_pem("not a pem").is_none());
    }

    #[test]
    fn trusted_platform_role_projects_explicit_platform_authority() {
        let scopes = ROLE_SCOPE_PROJECTIONS
            .iter()
            .find_map(|(role, scopes)| (*role == "platform_admin").then_some(*scopes))
            .expect("platform role projection exists");
        assert!(scopes.contains(&"udb:admin"));
        assert!(scopes.contains(&"udb:platform_admin"));
    }
}
