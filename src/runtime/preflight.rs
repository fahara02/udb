//! Enterprise startup preflight (UDB_FRICTION §2).
//!
//! In a hardened/enterprise deployment several hard prerequisites previously
//! surfaced only ONE AT A TIME as runtime failures on a fresh start — encryption
//! key, native-password hash secret, session secret, auth control-plane
//! exposure, rate-limit Redis, and authz default-deny — each behind a ~2-minute
//! restart/re-bootstrap cycle ("death by a thousand restarts"). This module
//! evaluates ALL of them once, up front, against the already-loaded config +
//! process env, so a single consolidated report lists every missing/risky
//! prerequisite instead of failing on them serially.
//!
//! The same check set powers `udb doctor --enterprise`. Findings are advisory at
//! startup (the per-capability guards still enforce when each capability is
//! actually used) — the value is surfacing the WHOLE list at once.

use std::net::SocketAddr;

use crate::runtime::config::UdbConfig;

/// How badly an unmet prerequisite bites.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreflightSeverity {
    /// The named capability WILL fail (e.g. login, native-user creation) until
    /// this is set. Not necessarily fatal to the whole broker.
    Fail,
    /// Likely-misconfigured / degraded, but the broker can still serve.
    Warn,
}

impl PreflightSeverity {
    pub fn label(self) -> &'static str {
        match self {
            Self::Fail => "FAIL",
            Self::Warn => "WARN",
        }
    }
}

/// A single unmet (or risky) enterprise prerequisite.
#[derive(Debug, Clone)]
pub struct PreflightFinding {
    /// Stable short key, e.g. `"encryption-key"`.
    pub name: &'static str,
    pub severity: PreflightSeverity,
    /// What goes wrong if left unaddressed.
    pub detail: String,
    /// The concrete env/config change that fixes it.
    pub fix: &'static str,
}

fn env_present(key: &str) -> bool {
    std::env::var(key)
        .ok()
        .is_some_and(|value| !value.trim().is_empty())
}

fn env_truthy(key: &str) -> bool {
    std::env::var(key)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Evaluate every enterprise prerequisite against the loaded config + env.
///
/// Returns ONLY the unmet/risky findings (empty slice = clean). `public_addr` is
/// the public DataBroker bind address — used to judge whether the loopback-only
/// auth control plane is reachable by remote clients.
pub fn evaluate(config: &UdbConfig, public_addr: SocketAddr) -> Vec<PreflightFinding> {
    let mut out = Vec::new();

    // (a) Object/native-state encryption key.
    if config.encryption.object_native_state_required && !config.encryption.has_key_source() {
        out.push(PreflightFinding {
            name: "encryption-key",
            severity: PreflightSeverity::Fail,
            detail: "object/native-state encryption is required but no key source is configured"
                .to_string(),
            fix: "set UDB_ENCRYPTION_KEY (32 bytes; base64/hex/raw) or a Vault key source",
        });
    }

    // (b) Native-password hash secret (admin bootstrap / create_user).
    if !env_present("UDB_PASSWORD_HASH_SECRET") && !env_present("UDB_SESSION_HASH_SECRET") {
        out.push(PreflightFinding {
            name: "password-hash-secret",
            severity: PreflightSeverity::Fail,
            detail: "native user password create/verify will fail (no hash secret)".to_string(),
            fix: "set UDB_PASSWORD_HASH_SECRET (or UDB_SESSION_HASH_SECRET)",
        });
    }

    // (b2) API-key hash secret. API keys are stored as keyed hashes; the key
    // falls back to UDB_SESSION_HASH_SECRET, so a deployment that regenerates
    // the session secret on each start (or rotates it) invalidates every API
    // key at once ("x-api-key is invalid, revoked, or expired").
    if !env_present("UDB_API_KEY_HASH_SECRET") {
        if env_present("UDB_SESSION_HASH_SECRET") {
            out.push(PreflightFinding {
                name: "api-key-hash-secret",
                severity: PreflightSeverity::Warn,
                detail: "API keys are hashed with UDB_SESSION_HASH_SECRET; changing that secret \
                         (or generating it per start) invalidates every API key"
                    .to_string(),
                fix: "set a stable, dedicated UDB_API_KEY_HASH_SECRET",
            });
        } else {
            out.push(PreflightFinding {
                name: "api-key-hash-secret",
                severity: PreflightSeverity::Fail,
                detail: "API key create/validate will fail (no hash secret)".to_string(),
                fix: "set a stable UDB_API_KEY_HASH_SECRET (or UDB_SESSION_HASH_SECRET)",
            });
        }
    }

    // (b3) Access-token signing key. Without it `Authenticate` (password AND
    // `api_key` exchange) returns an empty access token: SDKs that exchange a
    // service key for a bearer fail with "returned no access token".
    if !env_present("UDB_JWT_PRIVATE_KEY") {
        out.push(PreflightFinding {
            name: "jwt-signing-key",
            severity: PreflightSeverity::Fail,
            detail: "Authenticate (login and API-key exchange) issues no access token: no UDB-issued JWT signing key is configured"
                .to_string(),
            fix: "set UDB_JWT_PRIVATE_KEY (RS256 PEM, inline or a file path) and UDB_JWT_PUBLIC_KEY so the broker can sign and verify its own bearers",
        });
    }

    // (c) Server-side sessions (login / Authenticate).
    if !env_truthy("UDB_SESSION_ENABLED") || !env_present("UDB_SESSION_HASH_SECRET") {
        out.push(PreflightFinding {
            name: "sessions",
            severity: PreflightSeverity::Fail,
            detail: "login (Authenticate) returns FAILED_PRECONDITION 'sessions disabled'"
                .to_string(),
            fix: "set UDB_SESSION_ENABLED=true and UDB_SESSION_HASH_SECRET",
        });
    }

    // (d) Auth control-plane reachability. An empty control_plane_addr defaults
    // to loopback:(public_port+10); a loopback auth plane behind a public data
    // plane is unreachable by remote clients (login → UNIMPLEMENTED on :50051).
    let cp = config.native_services.control_plane_addr.trim();
    let cp_loopback = if cp.is_empty() {
        true
    } else {
        cp.parse::<SocketAddr>()
            .map(|addr| addr.ip().is_loopback())
            .unwrap_or(false)
    };
    if cp_loopback && !public_addr.ip().is_loopback() {
        out.push(PreflightFinding {
            name: "auth-plane-exposure",
            severity: PreflightSeverity::Warn,
            detail: "the Authn/Authz control plane binds the loopback-only internal listener; \
                     remote clients calling login get UNIMPLEMENTED on the public port"
                .to_string(),
            fix: "set UDB_AUTH_GRPC_ADDR=0.0.0.0:<public_port+10> to expose it on a trusted interface",
        });
    }

    // (d2) WebRTC peer listener. Same loopback default (public_port+20), so a
    // containerised broker serves no remote peer signalling unless exposed.
    let webrtc = config.native_services.webrtc_peer_addr.trim();
    let webrtc_loopback = webrtc.is_empty()
        || webrtc
            .parse::<SocketAddr>()
            .map(|addr| addr.ip().is_loopback())
            .unwrap_or(false);
    if webrtc_loopback && !public_addr.ip().is_loopback() {
        out.push(PreflightFinding {
            name: "webrtc-plane-exposure",
            severity: PreflightSeverity::Warn,
            detail:
                "the WebRTC peer listener binds loopback by default; remote peers cannot reach it"
                    .to_string(),
            fix: "set UDB_WEBRTC_GRPC_ADDR=0.0.0.0:<public_port+20> when WebRTC is used",
        });
    }

    // (e) Rate-limiter Redis.
    if !config.has_redis() {
        out.push(PreflightFinding {
            name: "redis",
            severity: PreflightSeverity::Warn,
            detail: "no Redis configured: the distributed rate limiter is disabled (no-op)"
                .to_string(),
            fix: "set REDIS_URL (or UDB_REDIS_DSN) to a reachable Redis for rate limiting",
        });
    }

    // (f) Authz default posture: default-deny with no seeded policies, or the
    // dev default-allow hatch (refused in production).
    if let Some(finding) = authz_default_posture_finding(
        config.service.abac_default_allow,
        crate::runtime::security::udb_env_is_production(),
    ) {
        out.push(finding);
    }

    out
}

/// Pure authz default-posture finding. The live Casbin engine reads the
/// PG-warmed `udb_authz.policy_rules` table (written by the AuthzService and
/// `udb authz seed`).
fn authz_default_posture_finding(
    default_allow: bool,
    production: bool,
) -> Option<PreflightFinding> {
    match (default_allow, production) {
        (false, _) => Some(PreflightFinding {
            name: "authz-default-deny",
            severity: PreflightSeverity::Warn,
            detail: "authz default-deny is active: data RPCs return PERMISSION_DENIED until \
                     policy rules are seeded in udb_authz.policy_rules (the table the live \
                     Casbin engine reads) and principals are bound to roles"
                .to_string(),
            fix: "seed policy with `udb authz seed --tenant <tenant-uuid> --role app_rw` (or AuthzService CreatePolicyRule/PutAuthzPolicy) and bind principals; UDB_ABAC_DEFAULT_ALLOW=true is a dev-only bootstrap hatch",
        }),
        (true, true) => Some(PreflightFinding {
            name: "authz-default-allow-production",
            severity: PreflightSeverity::Fail,
            detail: "UDB_ABAC_DEFAULT_ALLOW is set in production: every request would be allowed \
                     while zero policy rows exist; production startup refuses this"
                .to_string(),
            fix: "unset UDB_ABAC_DEFAULT_ALLOW and seed policy with `udb authz seed --tenant <tenant-uuid> --role app_rw`",
        }),
        (true, false) => None,
    }
}

/// Emit the findings as a single consolidated, human-readable startup report —
/// one `tracing::warn!` line per finding plus a header — instead of letting them
/// surface one-at-a-time over multiple restarts.
pub fn log_findings(findings: &[PreflightFinding]) {
    if findings.is_empty() {
        return;
    }
    let fails = findings
        .iter()
        .filter(|f| f.severity == PreflightSeverity::Fail)
        .count();
    tracing::warn!(
        total = findings.len(),
        will_fail = fails,
        "enterprise preflight: {} prerequisite(s) unmet — listing ALL now so you don't \
         discover them one-restart-at-a-time (UDB_FRICTION §2)",
        findings.len()
    );
    for finding in findings {
        tracing::warn!(
            check = finding.name,
            severity = finding.severity.label(),
            fix = finding.fix,
            "preflight[{}] {}: {} → {}",
            finding.severity.label(),
            finding.name,
            finding.detail,
            finding.fix
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These assert only the CONFIG-driven findings (Redis, authz default-deny,
    // auth-plane exposure), which are deterministic from `UdbConfig::default()`
    // and the bind address. The env-driven findings (secrets/sessions) depend on
    // ambient process env and are intentionally not asserted here to avoid racy
    // env mutation under parallel tests.

    #[test]
    fn public_default_config_flags_config_driven_prereqs() {
        let config = UdbConfig::default();
        let public: SocketAddr = "0.0.0.0:50051".parse().unwrap();
        let names: Vec<&str> = evaluate(&config, public).iter().map(|f| f.name).collect();
        // Default config has no Redis and default-deny authz.
        assert!(names.contains(&"redis"));
        assert!(names.contains(&"authz-default-deny"));
        // Default (empty) control plane is loopback while the bind is public.
        assert!(names.contains(&"auth-plane-exposure"));
    }

    #[test]
    fn authz_default_posture_names_the_live_policy_table_and_refuses_prod_allow() {
        let deny = authz_default_posture_finding(false, false).expect("default-deny is flagged");
        assert_eq!(deny.name, "authz-default-deny");
        assert!(
            deny.detail.contains("udb_authz.policy_rules") && !deny.detail.contains("NOT the"),
            "the finding must say the live engine reads policy_rules: {}",
            deny.detail
        );
        assert!(authz_default_posture_finding(true, false).is_none());
        let prod =
            authz_default_posture_finding(true, true).expect("prod default-allow is flagged");
        assert_eq!(prod.severity, PreflightSeverity::Fail);
    }

    #[test]
    fn loopback_bind_does_not_flag_auth_plane_exposure() {
        let config = UdbConfig::default();
        let local: SocketAddr = "127.0.0.1:50051".parse().unwrap();
        let findings = evaluate(&config, local);
        assert!(!findings.iter().any(|f| f.name == "auth-plane-exposure"));
    }
}
