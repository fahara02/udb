//! JWT issuance, token introspection, and the JWK Set endpoint.

use super::*;

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
        now: u64,
    ) -> (String, i64) {
        match crate::runtime::security::sign_access_token(
            &self.security,
            subject,
            tenant_id,
            project_id,
            scopes,
            roles,
            service_identity,
            jti,
            now,
        ) {
            Ok(Some((token, exp))) => (token, exp),
            Ok(None) => (String::new(), 0),
            Err(err) => {
                tracing::warn!(error = %err, "access-token signing failed; falling back to session-only");
                (String::new(), 0)
            }
        }
    }

    /// Build the JWK Set for verifying UDB-issued JWTs: proxy a configured
    /// external JWKS if present, otherwise derive an RS256 JWK from the
    /// configured public key (PEM or file path). Empty set when neither applies.
    async fn jwks_document(&self) -> String {
        const EMPTY: &str = "{\"keys\":[]}";
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
                    return body;
                }
            }
            return EMPTY.to_string();
        }
        if let Some(key) = self
            .security
            .jwt_public_key
            .as_ref()
            .filter(|k| !k.trim().is_empty())
        {
            let pem = if key.contains("-----BEGIN") {
                key.clone()
            } else {
                match std::fs::read_to_string(key) {
                    Ok(s) => s,
                    Err(_) => return EMPTY.to_string(),
                }
            };
            if let Some(jwk) = rsa_jwk_from_pem(&pem) {
                return jwk;
            }
        }
        EMPTY.to_string()
    }

    pub(super) async fn introspect_token_impl(
        &self,
        request: Request<authn_pb::IntrospectTokenRequest>,
    ) -> Result<Response<authn_pb::IntrospectTokenResponse>, Status> {
        let req = request.into_inner();
        match validate_bearer_token(&self.security, &req.token) {
            Ok(claims) => Ok(Response::new(authn_pb::IntrospectTokenResponse {
                active: true,
                subject: claims.sub.clone().unwrap_or_default(),
                tenant_id: claims.tenant_id.clone().unwrap_or_default(),
                service_identity: claims.service_identity.clone().unwrap_or_default(),
                scopes: claims.resolved_scopes(),
                // `exp` is validated inside `validate_bearer_token`; `active`
                // already reflects expiry. (Not separately surfaced here.)
                expires_at_unix: 0,
            })),
            Err(_) => Ok(Response::new(authn_pb::IntrospectTokenResponse {
                active: false,
                ..Default::default()
            })),
        }
    }

    pub(super) async fn get_jwks_impl(
        &self,
        _request: Request<authn_pb::GetJwksRequest>,
    ) -> Result<Response<authn_pb::GetJwksResponse>, Status> {
        Ok(Response::new(authn_pb::GetJwksResponse {
            jwks_json: self.jwks_document().await,
        }))
    }
}

/// Derive an RFC 7517 JWK Set (single RS256 key) from an RSA public key in PEM
/// form (SPKI `BEGIN PUBLIC KEY` or PKCS#1 `BEGIN RSA PUBLIC KEY`). Returns
/// `None` for non-RSA / unparseable keys.
fn rsa_jwk_from_pem(pem: &str) -> Option<String> {
    use base64::Engine;
    use rsa::RsaPublicKey;
    use rsa::pkcs1::DecodeRsaPublicKey;
    use rsa::pkcs8::DecodePublicKey;
    use rsa::traits::PublicKeyParts;

    let key = RsaPublicKey::from_public_key_pem(pem)
        .or_else(|_| RsaPublicKey::from_pkcs1_pem(pem))
        .ok()?;
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let n = b64.encode(key.n().to_bytes_be());
    let e = b64.encode(key.e().to_bytes_be());
    Some(
        serde_json::json!({
            "keys": [{
                "kty": "RSA",
                "use": "sig",
                "alg": "RS256",
                "kid": "udb-rs256-1",
                "n": n,
                "e": e,
            }]
        })
        .to_string(),
    )
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
