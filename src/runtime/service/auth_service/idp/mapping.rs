//! Pure mapping logic for the IdP plane: claim mapping, group→role mapping,
//! JIT provisioning policy, and assurance-level normalization.
//!
//! Everything here is deterministic and DB-free so it is unit-testable without a
//! live Postgres (Phase J3). The handlers ([`super::mod`]) call these helpers
//! after resolving the provider's stored mapping JSON.

use serde_json::Value;

use crate::proto::udb::core::idp::entity::v1 as idp_entity_pb;

/// Normalized result of applying a provider's `claim_mapping_json` to a set of
/// raw IdP claims (an OIDC token payload or SAML attribute set as a JSON object).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MappedClaims {
    pub subject: String,
    pub email: String,
    pub email_verified: bool,
    pub display_name: String,
    pub groups: Vec<String>,
    pub assurance: idp_entity_pb::AssuranceLevel,
}

/// Look up a (possibly dotted) path in a JSON object. `a.b.c` walks nested
/// objects; a bare key is a direct lookup. Returns `None` when any segment is
/// missing.
fn json_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = root;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// Coerce a JSON value to a string claim (string as-is; number/bool stringified;
/// arrays/objects rejected as `None`).
fn as_claim_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Coerce a JSON value to a boolean (`true`/`false`, `"true"`/`"1"`, `1`).
fn as_claim_bool(value: &Value) -> bool {
    match value {
        Value::Bool(b) => *b,
        Value::String(s) => matches!(
            s.trim().to_ascii_lowercase().as_str(),
            "true" | "1" | "yes" | "on"
        ),
        Value::Number(n) => n.as_i64().map(|v| v != 0).unwrap_or(false),
        _ => false,
    }
}

/// Extract a list of group strings from a claim value (a JSON array of strings,
/// or a single string, or a comma/space-separated string).
fn as_claim_groups(value: &Value) -> Vec<String> {
    match value {
        Value::Array(items) => items.iter().filter_map(as_claim_string).collect(),
        Value::String(s) => s
            .split([',', ' '])
            .map(str::trim)
            .filter(|g| !g.is_empty())
            .map(ToString::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

/// The claim name a mapping points a logical field at. The mapping JSON is an
/// object like `{"subject":"sub","email":"email","groups":"groups"}`. When a
/// logical field is absent from the mapping the conventional default name is
/// used so a bare/empty mapping still works for standard OIDC tokens.
fn mapped_claim_name<'a>(mapping: &'a Value, logical: &str, default: &'a str) -> &'a str {
    mapping
        .get(logical)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(default)
}

/// Apply a provider claim mapping to raw claims. `mapping_json` is the provider's
/// `claim_mapping_json`; `claims_json` is the verified token/attribute payload.
/// Both are parsed leniently — an empty/invalid mapping falls back to standard
/// OIDC claim names. The assurance level is derived from `amr`/`acr` (J2.5).
pub fn apply_claim_mapping(mapping_json: &str, claims: &Value) -> MappedClaims {
    let mapping: Value = serde_json::from_str(mapping_json.trim())
        .ok()
        .filter(Value::is_object)
        .unwrap_or_else(|| Value::Object(Default::default()));

    let subject = json_path(claims, mapped_claim_name(&mapping, "subject", "sub"))
        .and_then(as_claim_string)
        .unwrap_or_default();
    let email = json_path(claims, mapped_claim_name(&mapping, "email", "email"))
        .and_then(as_claim_string)
        .unwrap_or_default();
    let email_verified = json_path(
        claims,
        mapped_claim_name(&mapping, "email_verified", "email_verified"),
    )
    .map(as_claim_bool)
    .unwrap_or(false);
    let display_name = json_path(claims, mapped_claim_name(&mapping, "display_name", "name"))
        .and_then(as_claim_string)
        .unwrap_or_default();
    let groups = json_path(claims, mapped_claim_name(&mapping, "groups", "groups"))
        .map(as_claim_groups)
        .unwrap_or_default();

    let assurance = derive_assurance(claims);

    MappedClaims {
        subject,
        email,
        email_verified,
        display_name,
        groups,
        assurance,
    }
}

/// Map IdP authentication-context claims (`amr`, `acr`) onto a UDB
/// [`AssuranceLevel`]. Recognizes the common OIDC `amr` tokens for MFA and
/// phishing-resistant/hardware factors. Unknown/absent context degrades to
/// single-factor when a subject authenticated, else NONE.
pub fn derive_assurance(claims: &Value) -> idp_entity_pb::AssuranceLevel {
    use idp_entity_pb::AssuranceLevel as A;

    let amr: Vec<String> = claims
        .get("amr")
        .map(as_claim_groups)
        .unwrap_or_default()
        .into_iter()
        .map(|s| s.to_ascii_lowercase())
        .collect();
    let acr = claims
        .get("acr")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();

    // Hardware / phishing-resistant factors.
    const HARDWARE: &[&str] = &["hwk", "swk", "fido", "webauthn", "phr", "phrh", "hardware"];
    // Multi-factor markers.
    const MFA: &[&str] = &["mfa", "otp", "sms", "totp", "hotp", "tel"];

    if amr.iter().any(|m| HARDWARE.contains(&m.as_str()))
        || acr.contains("phr")
        || acr.contains("aal3")
    {
        return A::Hardware;
    }
    if amr.iter().any(|m| MFA.contains(&m.as_str()))
        || (amr.len() >= 2 && amr.iter().any(|m| m == "pwd" || m == "mca"))
        || acr.contains("mfa")
        || acr.contains("aal2")
    {
        return A::MultiFactor;
    }
    // A present subject with a single recognized factor.
    let has_subject = claims
        .get("sub")
        .and_then(Value::as_str)
        .map(|s| !s.is_empty())
        .unwrap_or(false);
    if !amr.is_empty() || has_subject {
        A::SingleFactor
    } else {
        A::None
    }
}

/// Map a set of IdP groups to UDB roles using ONLY the explicit
/// `group_mapping_json` entries (J2.3 / J3: "group mapping never grants
/// unconfigured roles"). The mapping JSON is `{"<group>": "<role>" | ["r1","r2"]}`.
/// Returns `(granted_roles_sorted_unique, unmapped_groups)`.
pub fn map_groups_to_roles(
    group_mapping_json: &str,
    groups: &[String],
) -> (Vec<String>, Vec<String>) {
    let mapping: Value = serde_json::from_str(group_mapping_json.trim())
        .ok()
        .filter(Value::is_object)
        .unwrap_or_else(|| Value::Object(Default::default()));
    let obj = mapping.as_object();

    let mut roles: Vec<String> = Vec::new();
    let mut unmapped: Vec<String> = Vec::new();
    for group in groups {
        let entry = obj.and_then(|m| m.get(group));
        match entry {
            Some(Value::String(role)) if !role.trim().is_empty() => {
                roles.push(role.trim().to_string());
            }
            Some(Value::Array(items)) => {
                let mut any = false;
                for item in items {
                    if let Some(role) = item.as_str().filter(|s| !s.trim().is_empty()) {
                        roles.push(role.trim().to_string());
                        any = true;
                    }
                }
                if !any {
                    unmapped.push(group.clone());
                }
            }
            _ => unmapped.push(group.clone()),
        }
    }
    roles.sort();
    roles.dedup();
    (roles, unmapped)
}

/// JIT provisioning policy parsed from a provider's `jit_policy_json`.
#[derive(Debug, Clone, Default)]
pub struct JitPolicy {
    /// Email domains (lowercase, no leading `@`) allowed to auto-provision.
    /// Empty = any domain allowed.
    pub allowed_domains: Vec<String>,
    /// Require a verified email before provisioning (default true when absent).
    pub require_verified_email: bool,
    /// JIT provisioning is enabled at all (default true when absent).
    pub enabled: bool,
    pub default_project: String,
    /// Roles assigned to every JIT-provisioned user (in addition to group roles).
    pub default_roles: Vec<String>,
}

impl JitPolicy {
    pub fn from_json(jit_policy_json: &str) -> Self {
        let v: Value = serde_json::from_str(jit_policy_json.trim())
            .ok()
            .filter(Value::is_object)
            .unwrap_or_else(|| Value::Object(Default::default()));
        let allowed_domains = v
            .get("allowed_domains")
            .map(as_claim_groups)
            .unwrap_or_default()
            .into_iter()
            .map(|d| d.trim().trim_start_matches('@').to_ascii_lowercase())
            .filter(|d| !d.is_empty())
            .collect();
        let require_verified_email = v
            .get("require_verified_email")
            .map(as_claim_bool)
            .unwrap_or(true);
        let enabled = v.get("enabled").map(as_claim_bool).unwrap_or(true);
        let default_project = v
            .get("default_project")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let default_roles = v
            .get("default_roles")
            .map(as_claim_groups)
            .unwrap_or_default();
        Self {
            allowed_domains,
            require_verified_email,
            enabled,
            default_project,
            default_roles,
        }
    }

    /// Whether `email` is permitted to auto-provision under this policy. An empty
    /// allowlist permits any domain; otherwise the email's domain must match
    /// (case-insensitive) an allowed entry.
    pub fn domain_allowed(&self, email: &str) -> bool {
        if self.allowed_domains.is_empty() {
            return true;
        }
        let domain = email
            .rsplit_once('@')
            .map(|(_, d)| d.trim().to_ascii_lowercase())
            .unwrap_or_default();
        !domain.is_empty() && self.allowed_domains.iter().any(|d| d == &domain)
    }
}

/// Outcome of evaluating whether a verified external identity may JIT-provision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JitDecision {
    /// Allowed to provision a new user.
    Provision,
    /// Rejected; carries a human-readable reason.
    Reject(String),
}

/// Decide whether claims satisfy the JIT policy (J2.4 / J3: rejects unverified or
/// disallowed-domain emails). A missing subject always rejects.
pub fn evaluate_jit(policy: &JitPolicy, claims: &MappedClaims) -> JitDecision {
    if claims.subject.trim().is_empty() {
        return JitDecision::Reject("missing subject claim".to_string());
    }
    if !policy.enabled {
        return JitDecision::Reject("JIT provisioning is disabled for this provider".to_string());
    }
    if policy.require_verified_email && !claims.email_verified {
        return JitDecision::Reject("email is not verified".to_string());
    }
    if claims.email.trim().is_empty() && !policy.allowed_domains.is_empty() {
        return JitDecision::Reject(
            "email is required to check the allowed-domain policy".to_string(),
        );
    }
    if !policy.domain_allowed(&claims.email) {
        return JitDecision::Reject(format!(
            "email domain is not in the provider allowed-domains list: {}",
            claims.email
        ));
    }
    JitDecision::Provision
}

/// Decision for resolving an email collision between a new external identity
/// and an existing local user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountLinkDecision {
    /// Refuse the login/link attempt because this provider never links by email.
    Deny,
    /// Link the external identity to the existing local user.
    LinkExisting,
    /// Require an explicit operator/user link action before login can continue.
    RequireExplicit,
}

/// Evaluate the provider account-linking policy for an email collision.
///
/// The default is deliberately explicit: unknown/empty policy values never
/// silently link an IdP subject to an existing account. `auto_verified` only
/// links when the IdP asserted a verified email.
pub fn evaluate_account_linking(policy: &str, email_verified: bool) -> AccountLinkDecision {
    match policy.trim().to_ascii_lowercase().as_str() {
        "deny" => AccountLinkDecision::Deny,
        "auto_verified" if email_verified => AccountLinkDecision::LinkExisting,
        "auto_verified" | "explicit" | "" => AccountLinkDecision::RequireExplicit,
        _ => AccountLinkDecision::RequireExplicit,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use idp_entity_pb::AssuranceLevel as A;
    use serde_json::json;

    #[test]
    fn claim_mapping_defaults_to_standard_oidc_names() {
        let claims = json!({
            "sub": "abc-123",
            "email": "a@b.com",
            "email_verified": true,
            "name": "Ada",
            "groups": ["eng", "admins"],
        });
        let m = apply_claim_mapping("{}", &claims);
        assert_eq!(m.subject, "abc-123");
        assert_eq!(m.email, "a@b.com");
        assert!(m.email_verified);
        assert_eq!(m.display_name, "Ada");
        assert_eq!(m.groups, vec!["eng", "admins"]);
    }

    #[test]
    fn claim_mapping_honors_custom_and_nested_paths() {
        let claims = json!({
            "oid": "xyz",
            "upn": "u@corp.com",
            "profile": { "verified": "true" },
            "roles": "r1,r2 r3",
        });
        let mapping = json!({
            "subject": "oid",
            "email": "upn",
            "email_verified": "profile.verified",
            "groups": "roles"
        })
        .to_string();
        let m = apply_claim_mapping(&mapping, &claims);
        assert_eq!(m.subject, "xyz");
        assert_eq!(m.email, "u@corp.com");
        assert!(m.email_verified);
        assert_eq!(m.groups, vec!["r1", "r2", "r3"]);
    }

    #[test]
    fn assurance_from_amr_and_acr() {
        assert_eq!(
            derive_assurance(&json!({"sub":"s","amr":["pwd"]})),
            A::SingleFactor
        );
        assert_eq!(
            derive_assurance(&json!({"sub":"s","amr":["pwd","otp"]})),
            A::MultiFactor
        );
        assert_eq!(
            derive_assurance(&json!({"sub":"s","amr":["mfa"]})),
            A::MultiFactor
        );
        assert_eq!(
            derive_assurance(&json!({"sub":"s","amr":["fido"]})),
            A::Hardware
        );
        assert_eq!(
            derive_assurance(&json!({"sub":"s","acr":"phr"})),
            A::Hardware
        );
        assert_eq!(
            derive_assurance(&json!({"acr":"urn:mace:aal2"})),
            A::MultiFactor
        );
        assert_eq!(derive_assurance(&json!({})), A::None);
        assert_eq!(derive_assurance(&json!({"sub":"s"})), A::SingleFactor);
    }

    #[test]
    fn group_mapping_never_grants_unconfigured_roles() {
        let mapping = json!({
            "eng": "role:developer",
            "admins": ["role:admin", "role:auditor"]
        })
        .to_string();
        let groups = vec![
            "eng".to_string(),
            "admins".to_string(),
            "interns".to_string(), // not in the mapping → grants nothing
        ];
        let (roles, unmapped) = map_groups_to_roles(&mapping, &groups);
        assert_eq!(roles, vec!["role:admin", "role:auditor", "role:developer"]);
        assert_eq!(unmapped, vec!["interns"]);
        // A wholly-unmapped group set grants no roles.
        let (none, _) = map_groups_to_roles(&mapping, &["nobody".to_string()]);
        assert!(none.is_empty());
        // An empty mapping grants nothing for any group.
        let (empty, un) = map_groups_to_roles("{}", &["eng".to_string()]);
        assert!(empty.is_empty());
        assert_eq!(un, vec!["eng"]);
    }

    #[test]
    fn jit_rejects_unverified_and_disallowed_domains() {
        let policy = JitPolicy::from_json(
            &json!({
                "allowed_domains": ["corp.com"],
                "require_verified_email": true,
                "default_roles": ["role:member"]
            })
            .to_string(),
        );
        // Unverified email → reject.
        let unverified = MappedClaims {
            subject: "s".into(),
            email: "u@corp.com".into(),
            email_verified: false,
            ..Default::default()
        };
        assert!(matches!(
            evaluate_jit(&policy, &unverified),
            JitDecision::Reject(_)
        ));
        // Disallowed domain → reject.
        let wrong_domain = MappedClaims {
            subject: "s".into(),
            email: "u@evil.com".into(),
            email_verified: true,
            ..Default::default()
        };
        assert!(matches!(
            evaluate_jit(&policy, &wrong_domain),
            JitDecision::Reject(_)
        ));
        // Verified + allowed domain → provision.
        let ok = MappedClaims {
            subject: "s".into(),
            email: "u@corp.com".into(),
            email_verified: true,
            ..Default::default()
        };
        assert_eq!(evaluate_jit(&policy, &ok), JitDecision::Provision);
        // Missing subject → always reject.
        let nosub = MappedClaims {
            email: "u@corp.com".into(),
            email_verified: true,
            ..Default::default()
        };
        assert!(matches!(
            evaluate_jit(&policy, &nosub),
            JitDecision::Reject(_)
        ));
    }

    #[test]
    fn jit_empty_allowlist_permits_any_domain() {
        let policy = JitPolicy::from_json(&json!({"require_verified_email": false}).to_string());
        let claims = MappedClaims {
            subject: "s".into(),
            email: "x@anywhere.io".into(),
            ..Default::default()
        };
        assert_eq!(evaluate_jit(&policy, &claims), JitDecision::Provision);
    }

    #[test]
    fn account_linking_never_silently_links_without_verified_auto_policy() {
        assert_eq!(
            evaluate_account_linking("auto_verified", true),
            AccountLinkDecision::LinkExisting
        );
        assert_eq!(
            evaluate_account_linking("auto_verified", false),
            AccountLinkDecision::RequireExplicit
        );
        assert_eq!(
            evaluate_account_linking("explicit", true),
            AccountLinkDecision::RequireExplicit
        );
        assert_eq!(
            evaluate_account_linking("", true),
            AccountLinkDecision::RequireExplicit
        );
        assert_eq!(
            evaluate_account_linking("unknown-new-mode", true),
            AccountLinkDecision::RequireExplicit
        );
        assert_eq!(
            evaluate_account_linking("deny", true),
            AccountLinkDecision::Deny
        );
    }
}
