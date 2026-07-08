//! Stage 2: signed policy bundles for local SDK authorization caches.
//!
//! `GetPolicyBundle` serializes the live [`AuthzSnapshot`] (policies, role
//! bindings, relationship tuples) into a canonical JSON payload and signs it
//! with a keyed HMAC so an SDK can cache it and answer `can()` locally without
//! a round-trip, while still being able to (a) verify the bundle was issued by
//! UDB and (b) reject it once expired. UDB remains the policy authority; the
//! bundle is a signed, time-boxed projection of the authority's state.

use base64::Engine as _;

use crate::runtime::authz::{AuthzPolicy, AuthzSnapshot, Effect};
use crate::runtime::security::hmac_sha256;

/// Encode an HMAC tag as **Base64 (standard alphabet)**. This is the single,
/// canonical bundle-signature encoding: the proto
/// (`SignedPolicyBundle.signature`) documents Base64, so server and every SDK
/// verifier agree on Base64 — Phase K4 normalization away from the prior
/// lowercase-hex output, which silently disagreed with the proto contract.
pub fn encode_bundle_signature(tag: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(tag)
}

/// Inverse of [`encode_bundle_signature`] — decode a Base64 signature so a
/// verifier can constant-time-compare it against a locally recomputed HMAC.
pub fn decode_bundle_signature(sig: &str) -> Result<Vec<u8>, base64::DecodeError> {
    base64::engine::general_purpose::STANDARD.decode(sig.trim())
}

/// Operator configuration for policy-bundle signing, read from the environment.
#[derive(Clone, PartialEq, Eq)]
pub struct PolicyBundleConfig {
    /// `UDB_POLICY_BUNDLE_SECRET` — HMAC signing secret. Falls back to
    /// `UDB_SESSION_HASH_SECRET` (the Stage-1 keyed-hash secret) when unset.
    /// Empty disables bundle issuance (the handler returns `failed_precondition`).
    pub secret: String,
    /// `UDB_POLICY_BUNDLE_KEY_ID` — key identifier callers use to select the
    /// verification key and detect rotation (default `udb-policy-v1`).
    pub key_id: String,
    /// `UDB_POLICY_BUNDLE_TTL_SECS` — bundle validity window (default 300).
    pub ttl_seconds: u64,
}

// 4.6 secrets posture: redact the HMAC signing `secret` in Debug so a `{:?}` in a
// log can never leak it (key_id/ttl are non-secret and print normally).
impl std::fmt::Debug for PolicyBundleConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PolicyBundleConfig")
            .field("secret", &"[redacted]")
            .field("key_id", &self.key_id)
            .field("ttl_seconds", &self.ttl_seconds)
            .finish()
    }
}

impl Default for PolicyBundleConfig {
    fn default() -> Self {
        Self {
            secret: String::new(),
            key_id: "udb-policy-v1".to_string(),
            ttl_seconds: 300,
        }
    }
}

impl PolicyBundleConfig {
    pub fn from_env() -> Self {
        let default = Self::default();
        let secret = std::env::var("UDB_POLICY_BUNDLE_SECRET")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .or_else(|| {
                std::env::var("UDB_SESSION_HASH_SECRET")
                    .ok()
                    .filter(|v| !v.trim().is_empty())
            })
            .unwrap_or_default();
        Self {
            secret,
            key_id: std::env::var("UDB_POLICY_BUNDLE_KEY_ID")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or(default.key_id),
            ttl_seconds: std::env::var("UDB_POLICY_BUNDLE_TTL_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(default.ttl_seconds),
        }
    }

    pub fn enabled(&self) -> bool {
        !self.secret.trim().is_empty()
    }

    /// Serialize + sign the snapshot for the given tenant/project/domain scope.
    /// Returns `None` when signing is disabled (no secret configured).
    pub fn sign(
        &self,
        snapshot: &AuthzSnapshot,
        tenant_id: &str,
        project_id: &str,
        now_unix: i64,
    ) -> Option<SignedBundle> {
        if !self.enabled() {
            return None;
        }
        let payload = snapshot_to_json(snapshot, tenant_id, project_id, now_unix, self.ttl_seconds);
        let bundle = serde_json::to_vec(&payload).ok()?;
        // Base64 (not hex) so the wire signature matches the proto contract and
        // every SDK verifier (Phase K4).
        let signature = encode_bundle_signature(&hmac_sha256(self.secret.as_bytes(), &bundle));
        Some(SignedBundle {
            bundle,
            signature,
            key_id: self.key_id.clone(),
            algorithm: "HMAC-SHA256".to_string(),
            policy_version: snapshot.version.clone(),
            relationship_version: snapshot.relationship_version.clone(),
            issued_at_unix: now_unix,
            expires_at_unix: now_unix.saturating_add(self.ttl_seconds as i64),
            ttl_seconds: self.ttl_seconds,
        })
    }
}

/// A signed, time-boxed projection of the authorization snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedBundle {
    pub bundle: Vec<u8>,
    pub signature: String,
    pub key_id: String,
    pub algorithm: String,
    pub policy_version: String,
    pub relationship_version: String,
    pub issued_at_unix: i64,
    pub expires_at_unix: i64,
    pub ttl_seconds: u64,
}

/// Canonical JSON projection of the snapshot the SDK evaluates locally. The
/// shape is intentionally flat and stable so multiple SDK languages can decode
/// it without sharing Rust types.
fn snapshot_to_json(
    snapshot: &AuthzSnapshot,
    tenant_id: &str,
    project_id: &str,
    issued_at_unix: i64,
    ttl_seconds: u64,
) -> serde_json::Value {
    let policies: Vec<serde_json::Value> = snapshot
        .policies
        .iter()
        .filter(|p| domain_matches(p, tenant_id, project_id))
        .map(policy_to_json)
        .collect();
    let role_bindings: Vec<serde_json::Value> = snapshot
        .role_bindings
        .iter()
        .filter(|b| {
            domain_str_matches(&b.tenant, tenant_id) && domain_str_matches(&b.project, project_id)
        })
        .map(|b| {
            serde_json::json!({
                "subject": b.subject,
                "role": b.role,
                "tenant": b.tenant,
                "project": b.project,
            })
        })
        .collect();
    let tuples: Vec<serde_json::Value> = snapshot
        .tuples
        .iter()
        .filter(|t| {
            domain_str_matches(&t.tenant, tenant_id) && domain_str_matches(&t.project, project_id)
        })
        .map(|t| {
            serde_json::json!({
                "subject": t.subject,
                "relation": t.relation,
                "object": t.object,
                "tenant": t.tenant,
                "project": t.project,
            })
        })
        .collect();

    serde_json::json!({
        "schema": "udb.policy-bundle.v1",
        "tenant_id": tenant_id,
        "project_id": project_id,
        "policy_version": snapshot.version,
        "relationship_version": snapshot.relationship_version,
        "default_allow": snapshot.default_allow,
        "issued_at_unix": issued_at_unix,
        "ttl_seconds": ttl_seconds,
        "policies": policies,
        "role_bindings": role_bindings,
        "relationship_tuples": tuples,
    })
}

fn policy_to_json(p: &AuthzPolicy) -> serde_json::Value {
    serde_json::json!({
        "id": p.id,
        "priority": p.priority,
        "enabled": p.enabled,
        "effect": match p.effect { Effect::Allow => "allow", Effect::Deny => "deny" },
        "tenant": p.tenant,
        "project": p.project,
        "subject": p.subject,
        "role": p.role,
        "action": p.action,
        "resource": p.resource,
        "purpose": p.purpose,
        "relationship": p.relationship,
        "conditions": p.conditions,
        "required_scopes": p.required_scopes,
    })
}

/// A policy belongs in a scoped bundle when its tenant/project selectors are
/// wildcards or match the requested scope. Wildcard policies (empty/`*`) always
/// apply so the SDK sees the same fall-through behavior the server uses.
fn domain_matches(p: &AuthzPolicy, tenant_id: &str, project_id: &str) -> bool {
    domain_str_matches(&p.tenant, tenant_id) && domain_str_matches(&p.project, project_id)
}

fn domain_str_matches(selector: &str, requested: &str) -> bool {
    // An empty `requested` must NOT match every tenant — that leaked all tenants'
    // policies into one bundle. Only wildcard selectors (empty/`*`) fall through;
    // a concrete selector matches only an exactly-equal request.
    let s = selector.trim();
    s.is_empty() || s == "*" || s == requested.trim()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::authz::{AuthzPolicy, AuthzSnapshot, Effect};

    fn snapshot() -> AuthzSnapshot {
        AuthzSnapshot {
            version: "v1".into(),
            relationship_version: "r1".into(),
            policies: vec![
                AuthzPolicy {
                    id: "p-acme".into(),
                    tenant: "acme".into(),
                    action: "data.select".into(),
                    effect: Effect::Allow,
                    ..AuthzPolicy::default()
                },
                AuthzPolicy {
                    id: "p-other".into(),
                    tenant: "other".into(),
                    action: "data.select".into(),
                    ..AuthzPolicy::default()
                },
                AuthzPolicy {
                    id: "p-wild".into(),
                    tenant: String::new(),
                    action: "*".into(),
                    ..AuthzPolicy::default()
                },
            ],
            role_bindings: Vec::new(),
            tuples: Vec::new(),
            default_allow: false,
        }
    }

    #[test]
    fn disabled_without_secret() {
        let cfg = PolicyBundleConfig::default();
        assert!(!cfg.enabled());
        assert!(cfg.sign(&snapshot(), "acme", "", 0).is_none());
    }

    #[test]
    fn signs_and_scopes_to_tenant() {
        let cfg = PolicyBundleConfig {
            secret: "topsecret".into(),
            ..PolicyBundleConfig::default()
        };
        let signed = cfg.sign(&snapshot(), "acme", "", 1000).expect("signed");
        assert_eq!(signed.algorithm, "HMAC-SHA256");
        assert_eq!(signed.expires_at_unix, 1000 + 300);
        // Signature is deterministic, Base64-encoded (proto contract), and
        // verifiable with the same key.
        let expect = encode_bundle_signature(&hmac_sha256(b"topsecret", &signed.bundle));
        assert_eq!(signed.signature, expect);
        // Round-trips through the documented Base64 decoder.
        assert_eq!(
            decode_bundle_signature(&signed.signature).unwrap(),
            hmac_sha256(b"topsecret", &signed.bundle).to_vec()
        );

        let payload: serde_json::Value = serde_json::from_slice(&signed.bundle).unwrap();
        let ids: Vec<&str> = payload["policies"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["id"].as_str().unwrap())
            .collect();
        // acme + wildcard included; other-tenant excluded.
        assert!(ids.contains(&"p-acme"));
        assert!(ids.contains(&"p-wild"));
        assert!(!ids.contains(&"p-other"));
    }

    #[test]
    fn tampering_breaks_signature() {
        let cfg = PolicyBundleConfig {
            secret: "topsecret".into(),
            ..PolicyBundleConfig::default()
        };
        let signed = cfg.sign(&snapshot(), "acme", "", 1000).expect("signed");
        let mut tampered = signed.bundle.clone();
        tampered[0] ^= 0xff;
        let recomputed = encode_bundle_signature(&hmac_sha256(b"topsecret", &tampered));
        assert_ne!(signed.signature, recomputed);
    }
}
