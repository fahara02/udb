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

    // (f) Authz default-deny with no seeded policies.
    if !config.service.abac_default_allow {
        out.push(PreflightFinding {
            name: "authz-default-deny",
            severity: PreflightSeverity::Warn,
            detail: "ABAC default-deny is active: data RPCs return PERMISSION_DENIED until ABAC \
                     policies are seeded (note: the live engine reads ABAC policies, NOT the \
                     udb_authz.policy_rules governance table)"
                .to_string(),
            fix: "configure policies via the AuthzService (policy_rules), or set UDB_ABAC_DEFAULT_ALLOW=true for dev/bootstrap",
        });
    }

    out
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
    fn loopback_bind_does_not_flag_auth_plane_exposure() {
        let config = UdbConfig::default();
        let local: SocketAddr = "127.0.0.1:50051".parse().unwrap();
        let findings = evaluate(&config, local);
        assert!(!findings.iter().any(|f| f.name == "auth-plane-exposure"));
    }
}
