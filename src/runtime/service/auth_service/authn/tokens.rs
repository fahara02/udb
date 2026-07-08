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
    &[("organization_owner", &["udb:admin", "udb:*"])];

/// Tenant/project domain match mirroring the authz snapshot's private
/// `domain_match` (empty or `*` is a wildcard, else exact) without reaching into
/// the authz module. Role bindings loaded from `user_roles` carry a concrete
/// tenant and (usually) an empty project, so this stays exact-or-wildcard.
fn domain_allows(pattern: &str, value: &str) -> bool {
    let pattern = pattern.trim();
    pattern.is_empty() || pattern == "*" || pattern == value
}

impl AuthnServiceImpl {
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
    /// scopes those roles project to via [`ROLE_SCOPE_PROJECTIONS`].
    ///
    /// Returns `(scopes, roles)`. Fail-closed: no snapshot wired ⇒ no grants
    /// (never empty-as-admin). `empty ⇒ empty`, so revoking a user's last role
    /// drops the projected admin scope at the next issue/refresh.
    pub(super) fn resolve_effective_grants(
        &self,
        user_id: &str,
        tenant_id: &str,
        project_id: &str,
    ) -> (Vec<String>, Vec<String>) {
        let Some(cell) = self.authz_snapshot.as_ref() else {
            return (Vec::new(), Vec::new());
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
        (scopes, roles)
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
            if let Ok(resp) = reqwest::Client::new().get(url.trim()).send().await {
                if let Ok(body) = resp.text().await {
                    return Ok(body);
                }
            }
            return Ok(EMPTY.to_string());
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

#[cfg(test)]
mod jwk_tests {
    use super::rsa_jwk_from_pem;

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
}
