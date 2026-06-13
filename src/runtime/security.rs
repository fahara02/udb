#![allow(clippy::result_large_err)]

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Mutex, OnceLock, RwLock};
use tonic::{Request, Status};
use x509_parser::prelude::parse_x509_certificate;

use crate::broker::table_for_message;
use crate::generation::{CatalogManifest, ManifestColumn};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SecurityContext {
    pub tenant_id: String,
    pub purpose: String,
    pub correlation_id: String,
    pub user_id: String,
    pub scopes: Vec<String>,
    pub service_identity: String,
    pub trace_id: String,
    /// Optional project identifier extracted from `x-udb-project-id` header.
    /// Empty string means single-project mode (default).
    pub project_id: String,
    pub consistency: String,
    pub max_replica_lag_ms: u64,
    pub client_catalog_version: String,
    pub target_backend: String,
    pub target_instance: String,
    pub routing_policy: String,
    pub primary_read: bool,
    pub eventual_consistency_allowed: bool,
    pub read_fence_json: String,
}

/// Phase 9: Security configuration
static INSTALLED_SECURITY_CONFIG: OnceLock<Mutex<SecurityConfig>> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SecurityConfig {
    /// Require TLS for production gRPC
    pub tls_required: bool,
    /// Require TLS for PostgreSQL connections
    pub pg_tls_required: bool,
    /// Require TLS for Redis connections
    pub redis_tls_required: bool,
    /// Require TLS for Kafka connections
    pub kafka_tls_required: bool,
    /// Require service identity on all non-health RPCs
    pub service_identity_required: bool,
    /// JWT public key path or inline PEM
    pub jwt_public_key: Option<String>,
    /// mTLS required (vs optional header fallback)
    pub mtls_required: bool,
    /// Allow header-based scopes (dev only)
    pub allow_header_scopes: bool,
    /// Field-level encryption key rotation enabled
    pub encryption_key_rotation_enabled: bool,
    /// Current encryption key ID
    pub current_encryption_key_id: String,
    /// Audit sink URL (HTTP endpoint for audit logs)
    pub audit_sink_url: String,
    /// PII-safe logging enabled
    pub pii_safe_logging: bool,
    /// Metric endpoint allowlist (comma-separated IPs or CIDRs)
    pub metric_endpoint_allowlist: Vec<String>,
    /// Expected JWT issuer (`UDB_JWT_ISSUER`). `None` disables issuer validation.
    pub jwt_issuer: Option<String>,
    /// Expected JWT audience (`UDB_JWT_AUDIENCE`). `None` disables audience
    /// validation (tokens are accepted regardless of any `aud` claim).
    pub jwt_audience: Option<String>,
    /// Allowed JWT signing algorithms (`UDB_JWT_ALLOWED_ALGS`, comma-separated,
    /// e.g. `RS256,EdDSA`). Empty = the default asymmetric set.
    pub jwt_allowed_algs: Vec<String>,
    /// Clock-skew leeway in seconds applied to `exp`/`nbf`
    /// (`UDB_JWT_CLOCK_SKEW_SECONDS`). Defaults to 60.
    pub jwt_clock_skew_secs: u64,
    /// JWKS endpoint for asymmetric JWT verification (`UDB_JWT_JWKS_URL`).
    pub jwt_jwks_url: Option<String>,
    /// JWKS cache TTL (`UDB_JWT_JWKS_CACHE_TTL_SECONDS`). Defaults to 300.
    pub jwt_jwks_cache_ttl_secs: u64,
    /// RSA private key (inline PEM or path) UDB uses to *sign* the access tokens
    /// it issues (`UDB_JWT_PRIVATE_KEY`). `None` disables UDB-issued JWTs — login
    /// then falls back to server-side session tokens only.
    pub jwt_private_key: Option<String>,
    /// Lifetime of UDB-issued access tokens in seconds
    /// (`UDB_JWT_ACCESS_TTL_SECONDS`). Defaults to 900 (15 minutes).
    pub jwt_access_ttl_secs: u64,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            tls_required: true,
            pg_tls_required: true,
            redis_tls_required: true,
            kafka_tls_required: true,
            service_identity_required: true,
            jwt_public_key: None,
            mtls_required: true,
            allow_header_scopes: false,
            encryption_key_rotation_enabled: false,
            current_encryption_key_id: "default".to_string(),
            audit_sink_url: String::new(),
            pii_safe_logging: true,
            metric_endpoint_allowlist: Vec::new(),
            jwt_issuer: None,
            jwt_audience: None,
            jwt_allowed_algs: Vec::new(),
            jwt_clock_skew_secs: 60,
            jwt_jwks_url: None,
            jwt_jwks_cache_ttl_secs: 300,
            jwt_private_key: None,
            jwt_access_ttl_secs: 900,
        }
    }
}

impl SecurityConfig {
    pub fn install_global(config: Self) {
        let cell = INSTALLED_SECURITY_CONFIG.get_or_init(|| Mutex::new(Self::from_env()));
        if let Ok(mut guard) = cell.lock() {
            *guard = config;
        }
    }

    /// Central resolver for a PEM value that may be supplied either inline
    /// (starts with `-----BEGIN`) or as a filesystem path. Returns `None` for an
    /// empty/whitespace value; surfaces a read error for a non-empty path that
    /// cannot be read. Single source of truth so signing, JWKS publication and
    /// the registry seed all interpret `UDB_JWT_*` keys identically.
    pub fn resolve_pem(key_src: &str) -> Result<Option<String>, String> {
        let trimmed = key_src.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        if trimmed.contains("-----BEGIN") {
            return Ok(Some(key_src.to_string()));
        }
        std::fs::read_to_string(trimmed)
            .map(Some)
            .map_err(|e| format!("failed to read PEM from path '{trimmed}': {e}"))
    }

    /// Private signing PEM (`UDB_JWT_PRIVATE_KEY`), resolved inline-or-path.
    pub fn jwt_private_pem(&self) -> Option<String> {
        self.jwt_private_key
            .as_deref()
            .and_then(|src| Self::resolve_pem(src).ok().flatten())
    }

    /// Public verification PEM (`UDB_JWT_PUBLIC_KEY`), resolved inline-or-path.
    pub fn jwt_public_pem(&self) -> Option<String> {
        self.jwt_public_key
            .as_deref()
            .and_then(|src| Self::resolve_pem(src).ok().flatten())
    }

    pub fn current() -> Self {
        INSTALLED_SECURITY_CONFIG
            .get()
            .and_then(|cell| cell.lock().ok().map(|guard| guard.clone()))
            .unwrap_or_else(Self::from_env)
    }

    pub fn from_env() -> Self {
        let defaults = Self::default();

        // Detect production mode
        let is_production = std::env::var("UDB_ENV")
            .map(|v| v.to_lowercase() == "production" || v.to_lowercase() == "prod")
            .unwrap_or(false);

        Self {
            tls_required: std::env::var("UDB_TLS_REQUIRED")
                .map(|v| !matches!(v.as_str(), "0" | "false" | "no"))
                .unwrap_or(is_production),
            pg_tls_required: std::env::var("UDB_PG_TLS_REQUIRED")
                .map(|v| !matches!(v.as_str(), "0" | "false" | "no"))
                .unwrap_or(is_production),
            redis_tls_required: std::env::var("UDB_REDIS_TLS_REQUIRED")
                .map(|v| !matches!(v.as_str(), "0" | "false" | "no"))
                .unwrap_or(is_production),
            kafka_tls_required: std::env::var("UDB_KAFKA_TLS_REQUIRED")
                .map(|v| !matches!(v.as_str(), "0" | "false" | "no"))
                .unwrap_or(is_production),
            service_identity_required: std::env::var("UDB_SERVICE_IDENTITY_REQUIRED")
                .map(|v| !matches!(v.as_str(), "0" | "false" | "no"))
                .unwrap_or(is_production),
            jwt_public_key: std::env::var("UDB_JWT_PUBLIC_KEY").ok(),
            mtls_required: std::env::var("UDB_MTLS_REQUIRED")
                .map(|v| !matches!(v.as_str(), "0" | "false" | "no"))
                .unwrap_or(true),
            // Parse as a boolean, not mere presence — `UDB_ALLOW_HEADER_SCOPES=0`
            // / `=false` must DISABLE the dev header-scope fallback, not enable it.
            allow_header_scopes: std::env::var("UDB_ALLOW_HEADER_SCOPES")
                .map(|v| {
                    matches!(
                        v.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes" | "on"
                    )
                })
                .unwrap_or(false),
            encryption_key_rotation_enabled: std::env::var("UDB_ENCRYPTION_KEY_ROTATION")
                .map(|v| !matches!(v.as_str(), "0" | "false" | "no"))
                .unwrap_or(false),
            current_encryption_key_id: std::env::var("UDB_ENCRYPTION_KEY_ID")
                .unwrap_or(defaults.current_encryption_key_id),
            audit_sink_url: std::env::var("UDB_AUDIT_SINK_URL").unwrap_or_default(),
            pii_safe_logging: std::env::var("UDB_PII_SAFE_LOGGING")
                .map(|v| !matches!(v.as_str(), "0" | "false" | "no"))
                .unwrap_or(defaults.pii_safe_logging),
            metric_endpoint_allowlist: std::env::var("UDB_METRIC_ENDPOINT_ALLOWLIST")
                .ok()
                .map(|raw| {
                    raw.split(',')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(ToString::to_string)
                        .collect()
                })
                .unwrap_or_default(),
            jwt_issuer: non_empty_env("UDB_JWT_ISSUER"),
            jwt_audience: non_empty_env("UDB_JWT_AUDIENCE"),
            jwt_allowed_algs: csv_env("UDB_JWT_ALLOWED_ALGS"),
            jwt_clock_skew_secs: std::env::var("UDB_JWT_CLOCK_SKEW_SECONDS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(60),
            jwt_jwks_url: non_empty_env("UDB_JWT_JWKS_URL"),
            jwt_jwks_cache_ttl_secs: std::env::var("UDB_JWT_JWKS_CACHE_TTL_SECONDS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(300),
            jwt_private_key: non_empty_env("UDB_JWT_PRIVATE_KEY"),
            jwt_access_ttl_secs: std::env::var("UDB_JWT_ACCESS_TTL_SECONDS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(900),
        }
    }

    pub fn merge_env(&mut self) {
        let is_production = std::env::var("UDB_ENV")
            .map(|v| v.to_lowercase() == "production" || v.to_lowercase() == "prod")
            .unwrap_or(false);
        if let Ok(v) = std::env::var("UDB_TLS_REQUIRED") {
            self.tls_required = !matches!(v.as_str(), "0" | "false" | "no");
        } else if is_production {
            self.tls_required = true;
        }
        if let Ok(v) = std::env::var("UDB_PG_TLS_REQUIRED") {
            self.pg_tls_required = !matches!(v.as_str(), "0" | "false" | "no");
        } else if is_production {
            self.pg_tls_required = true;
        }
        if let Ok(v) = std::env::var("UDB_REDIS_TLS_REQUIRED") {
            self.redis_tls_required = !matches!(v.as_str(), "0" | "false" | "no");
        } else if is_production {
            self.redis_tls_required = true;
        }
        if let Ok(v) = std::env::var("UDB_KAFKA_TLS_REQUIRED") {
            self.kafka_tls_required = !matches!(v.as_str(), "0" | "false" | "no");
        } else if is_production {
            self.kafka_tls_required = true;
        }
        if let Ok(v) = std::env::var("UDB_SERVICE_IDENTITY_REQUIRED") {
            self.service_identity_required = !matches!(v.as_str(), "0" | "false" | "no");
        } else if is_production {
            self.service_identity_required = true;
        }
        if let Ok(v) = std::env::var("UDB_JWT_PUBLIC_KEY") {
            self.jwt_public_key = Some(v);
        }
        if let Ok(v) = std::env::var("UDB_MTLS_REQUIRED") {
            self.mtls_required = !matches!(v.as_str(), "0" | "false" | "no");
        }
        if let Ok(v) = std::env::var("UDB_ALLOW_HEADER_SCOPES") {
            self.allow_header_scopes = matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            );
        }
        if let Ok(v) = std::env::var("UDB_ENCRYPTION_KEY_ROTATION") {
            self.encryption_key_rotation_enabled = !matches!(v.as_str(), "0" | "false" | "no");
        }
        if let Ok(v) = std::env::var("UDB_ENCRYPTION_KEY_ID") {
            self.current_encryption_key_id = v;
        }
        if let Ok(v) = std::env::var("UDB_AUDIT_SINK_URL") {
            self.audit_sink_url = v;
        }
        if let Ok(v) = std::env::var("UDB_PII_SAFE_LOGGING") {
            self.pii_safe_logging = !matches!(v.as_str(), "0" | "false" | "no");
        }
        if let Ok(raw) = std::env::var("UDB_METRIC_ENDPOINT_ALLOWLIST") {
            self.metric_endpoint_allowlist = raw
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string)
                .collect();
        }
        if let Some(v) = non_empty_env("UDB_JWT_ISSUER") {
            self.jwt_issuer = Some(v);
        }
        if let Some(v) = non_empty_env("UDB_JWT_AUDIENCE") {
            self.jwt_audience = Some(v);
        }
        if std::env::var("UDB_JWT_ALLOWED_ALGS").is_ok() {
            self.jwt_allowed_algs = csv_env("UDB_JWT_ALLOWED_ALGS");
        }
        if let Ok(v) = std::env::var("UDB_JWT_CLOCK_SKEW_SECONDS")
            && let Ok(secs) = v.parse::<u64>()
        {
            self.jwt_clock_skew_secs = secs;
        }
        if let Some(v) = non_empty_env("UDB_JWT_JWKS_URL") {
            self.jwt_jwks_url = Some(v);
        }
        if let Ok(v) = std::env::var("UDB_JWT_JWKS_CACHE_TTL_SECONDS")
            && let Ok(secs) = v.parse::<u64>()
        {
            self.jwt_jwks_cache_ttl_secs = secs;
        }
        // Without these in the merged-config path, UDB-issued JWT signing
        // (`sign_access_token`) silently no-ops even when the key is configured.
        if let Some(v) = non_empty_env("UDB_JWT_PRIVATE_KEY") {
            self.jwt_private_key = Some(v);
        }
        if let Ok(v) = std::env::var("UDB_JWT_ACCESS_TTL_SECONDS")
            && let Ok(secs) = v.parse::<u64>()
            && secs > 0
        {
            self.jwt_access_ttl_secs = secs;
        }
    }

    /// Returns true if the system is in production mode
    pub fn is_production(&self) -> bool {
        self.tls_required && self.service_identity_required
    }

    /// Validate security configuration for production readiness
    pub fn validate_production(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if !self.tls_required {
            errors.push("TLS must be required in production (UDB_TLS_REQUIRED=true)".to_string());
        }
        if !self.service_identity_required {
            errors.push("Service identity must be required in production (UDB_SERVICE_IDENTITY_REQUIRED=true)".to_string());
        }
        if self.allow_header_scopes {
            errors.push("Header-based scopes must be disabled in production (remove UDB_ALLOW_HEADER_SCOPES)".to_string());
        }
        if self.jwt_public_key.is_none() && self.jwt_jwks_url.is_none() && !self.mtls_required {
            errors.push("Either JWT or mTLS must be configured for authentication".to_string());
        }
        if self.audit_sink_url.is_empty() {
            errors.push(
                "Audit sink URL must be configured in production (UDB_AUDIT_SINK_URL)".to_string(),
            );
        }
        if !self.pii_safe_logging {
            errors.push(
                "PII-safe logging must be enabled in production (UDB_PII_SAFE_LOGGING=true)"
                    .to_string(),
            );
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn validate_compliance_profile(
        &self,
        profile: ComplianceProfile,
        facts: &ComplianceProfileFacts,
    ) -> Result<(), Vec<String>> {
        let mut errors = self.validate_production().err().unwrap_or_default();
        match profile {
            ComplianceProfile::Soc2Type2 => {}
            ComplianceProfile::Iso27001And27017 => {
                if !facts.encryption_key_source_configured {
                    errors.push(
                        "ISO 27001/27017 profile requires encryption-at-rest key source"
                            .to_string(),
                    );
                }
                if !self.encryption_key_rotation_enabled {
                    errors.push(
                        "ISO 27001/27017 profile requires encryption key rotation".to_string(),
                    );
                }
            }
            ComplianceProfile::PciHipaa => {
                if !facts.encryption_key_source_configured {
                    errors.push(
                        "PCI/HIPAA profile requires encryption-at-rest key source".to_string(),
                    );
                }
                if !self.encryption_key_rotation_enabled {
                    errors.push("PCI/HIPAA profile requires encryption key rotation".to_string());
                }
                if !self.mtls_required {
                    errors.push("PCI/HIPAA profile requires mTLS".to_string());
                }
                if !facts.fail_closed_enabled {
                    errors.push("PCI/HIPAA profile requires fail-closed mode".to_string());
                }
                if !facts.durable_audit_sink_configured {
                    errors.push("PCI/HIPAA profile requires a durable audit sink".to_string());
                }
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Derive the runtime [`ComplianceProfileFacts`] from this config so the
    /// startup compliance gate (`serve()`) can validate the selected profile
    /// against actual deployment state rather than test fixtures.
    pub fn compliance_profile_facts(&self) -> ComplianceProfileFacts {
        ComplianceProfileFacts {
            encryption_key_source_configured: !self.current_encryption_key_id.trim().is_empty(),
            fail_closed_enabled: fail_closed_mode(),
            durable_audit_sink_configured: !self.audit_sink_url.trim().is_empty(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComplianceProfile {
    Soc2Type2,
    Iso27001And27017,
    PciHipaa,
}

impl ComplianceProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Soc2Type2 => "soc2_type2",
            Self::Iso27001And27017 => "iso27001_27017",
            Self::PciHipaa => "pci_hipaa",
        }
    }
}

/// The compliance profile the operator selected via `UDB_COMPLIANCE_PROFILE`
/// (`soc2` / `iso27001` / `pci_hipaa`), or `None` when unset/`none`. The startup
/// path validates the selected profile and refuses to serve on violation, so a
/// declared profile is an ENFORCED runtime posture, not just documentation.
pub fn selected_compliance_profile() -> Option<ComplianceProfile> {
    let raw = std::env::var("UDB_COMPLIANCE_PROFILE").ok()?;
    match raw
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '/', '-'], "_")
        .as_str()
    {
        "" | "none" => None,
        "soc2" | "soc2_type2" | "soc2type2" => Some(ComplianceProfile::Soc2Type2),
        "iso27001" | "iso_27001" | "iso27001_27017" | "iso27017" | "iso" => {
            Some(ComplianceProfile::Iso27001And27017)
        }
        "pci" | "hipaa" | "pci_hipaa" | "pcihipaa" | "pci_dss" => Some(ComplianceProfile::PciHipaa),
        // Unknown value: caller logs a warning. Returning None keeps an obvious
        // typo from silently passing as a stricter profile.
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ComplianceProfileFacts {
    pub encryption_key_source_configured: bool,
    pub fail_closed_enabled: bool,
    pub durable_audit_sink_configured: bool,
}

/// Single source of truth for whether security-sensitive paths must FAIL CLOSED
/// (deny on dependency/store error) rather than fail open (degrade to allow).
///
/// Phase 5 ("Security Depth"): in a hardened/enterprise deployment a revocation
/// lookup error, signing-key/JWKS store error, or rate-limit store error must
/// DENY, never silently allow. This is true when the process is in a production
/// security posture (`is_production()`) OR enterprise audit mode is on
/// (`UDB_ENTERPRISE_AUDIT`) OR it is explicitly requested (`UDB_FAIL_CLOSED`).
/// Dev/test default is fail-open so a missing local dependency does not block work.
pub fn fail_closed_mode() -> bool {
    if SecurityConfig::current().is_production() {
        return true;
    }
    for key in ["UDB_FAIL_CLOSED", "UDB_ENTERPRISE_AUDIT"] {
        if let Ok(value) = std::env::var(key) {
            if matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            ) {
                return true;
            }
        }
    }
    false
}

/// Startup posture gate for Phase 5 ("Enterprise mode refuses insecure transport
/// unless explicitly configured for development").
///
/// Returns the combined list of production/secure-transport violations that must
/// ABORT startup when the process is hardened — i.e. in a production security
/// posture ([`SecurityConfig::is_production`]) or [`fail_closed_mode`] is on.
/// In a dev posture (not production, not fail-closed) it returns an empty list so
/// a plaintext, no-TLS local deployment is still permitted; the caller logs those
/// same advisory findings instead of aborting.
///
/// `transport_violations` is supplied by the caller (the service layer owns the
/// `validate_secure_transport` check over `ServiceSettings`/`TlsSettings`); this
/// keeps `security.rs` free of a dependency on the service config types while
/// still centralizing the hardened-vs-dev decision.
pub fn hardened_startup_violations(transport_violations: &[String]) -> Vec<String> {
    if !(SecurityConfig::current().is_production() || fail_closed_mode()) {
        return Vec::new();
    }
    let mut violations = SecurityConfig::current()
        .validate_production()
        .err()
        .unwrap_or_default();
    violations.extend(transport_violations.iter().cloned());
    violations
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbacPolicy {
    pub effect: PolicyEffect,
    pub service_identity: String,
    pub tenant_id: String,
    pub purpose: String,
    pub message_type: String,
    pub operation: String,
    pub required_scope: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum PolicyEffect {
    #[default]
    Allow,
    Deny,
}

impl SecurityContext {
    pub fn request_context(&self) -> crate::RequestContext {
        crate::RequestContext {
            tenant_id: self.tenant_id.clone(),
            purpose: self.purpose.clone(),
            correlation_id: self.correlation_id.clone(),
            user_id: self.user_id.clone(),
            scopes: self.scopes.clone(),
            project_id: self.project_id.clone(),
            consistency: self.consistency.clone(),
            max_replica_lag_ms: self.max_replica_lag_ms,
            client_catalog_version: self.client_catalog_version.clone(),
            target_backend: self.target_backend.clone(),
            target_instance: self.target_instance.clone(),
            routing_policy: self.routing_policy.clone(),
            primary_read: self.primary_read,
            eventual_consistency_allowed: self.eventual_consistency_allowed,
            read_fence_json: self.read_fence_json.clone(),
            service_identity: self.service_identity.clone(),
            decision_id: String::new(),
        }
    }

    /// Same as [`request_context`](Self::request_context) but stamps the
    /// authorization `decision_id` so the backend context can emit
    /// `app.current_decision_id` for row-level audit correlation (Stage 2).
    pub fn request_context_with_decision(&self, decision_id: &str) -> crate::RequestContext {
        let mut ctx = self.request_context();
        ctx.decision_id = decision_id.to_string();
        ctx
    }

    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes
            .iter()
            .any(|candidate| candidate == scope || candidate == "udb:*" || candidate == "*")
    }

    pub fn log_safe(&self) -> LogSafeSecurityContext {
        LogSafeSecurityContext {
            tenant_id: self.tenant_id.clone(),
            purpose: self.purpose.clone(),
            correlation_id: self.correlation_id.clone(),
            service_identity: self.service_identity.clone(),
            user_id: if self.user_id.is_empty() {
                String::new()
            } else {
                "***MASKED***".to_string()
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LogSafeSecurityContext {
    pub tenant_id: String,
    pub purpose: String,
    pub correlation_id: String,
    pub service_identity: String,
    pub user_id: String,
}

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};

#[derive(Debug, Clone, Deserialize)]
pub struct SecurityClaims {
    pub tenant_id: Option<String>,
    pub purpose: Option<String>,
    pub scopes: Option<Vec<String>>,
    /// OAuth2 singular `scope` claim — a space/comma-separated string. Used when
    /// the array `scopes` claim is absent.
    #[serde(default)]
    pub scope: Option<String>,
    pub service_identity: Option<String>,
    pub project_id: Option<String>,
    /// Expiry (unix seconds). `jsonwebtoken` validates `exp` during decode; we ALSO
    /// map it (a numeric claim — safe, unlike `aud`) so the data-plane validation
    /// cache can refuse to serve a cached hit past the token's own expiry.
    #[serde(default)]
    pub exp: Option<i64>,
    // Registered/UDB claims parsed for mapping. `nbf`, `aud`, and `iss` are
    // validated by `jsonwebtoken`'s `Validation` (not mapped here) — adding
    // `aud` here as a String would break tokens whose `aud` is an array.
    #[serde(default)]
    pub sub: Option<String>,
    #[serde(default)]
    pub iss: Option<String>,
    #[serde(default)]
    pub jti: Option<String>,
    /// RBAC roles — parsed now; wired into `SecurityContext`/`Decision` in a
    /// later phase (policy model upgrade).
    #[serde(default)]
    pub roles: Option<Vec<String>>,
    /// ReBAC snapshot version for relationship-cache invalidation (later phase).
    #[serde(default)]
    pub relationships_version: Option<String>,
    /// Authentication method that minted this token ("pwd", "mfa", "oidc",
    /// "refresh", "service", …). Lets resource servers make step-up / assurance
    /// decisions from the token alone (zero-trust).
    #[serde(default)]
    pub auth_method: Option<String>,
    /// Authentication Context Class Reference — the assurance level ("aal1" for
    /// single-factor, "aal2" for MFA/WebAuthn), derived from `auth_method`.
    #[serde(default)]
    pub acr: Option<String>,
}

impl SecurityClaims {
    /// Resolve scopes from either the array `scopes` claim or the OAuth2
    /// singular `scope` string (space/comma-separated). The array form wins when
    /// both are present.
    pub fn resolved_scopes(&self) -> Vec<String> {
        if let Some(scopes) = &self.scopes {
            return scopes.clone();
        }
        if let Some(scope) = &self.scope {
            return scope
                .split([' ', ','])
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string)
                .collect();
        }
        Vec::new()
    }
}

#[derive(Clone)]
struct JwksCacheEntry {
    url: String,
    fetched_at_unix: u64,
    jwks: JwkSet,
}

static JWT_JWKS_CACHE: OnceLock<Mutex<Option<JwksCacheEntry>>> = OnceLock::new();

/// Test-only JWKS override: when set, `cached_jwks` returns it directly for both
/// the fresh and refresh paths, so JWKS verification (kid lookup + rotation) can
/// be exercised offline without a network fetch.
#[cfg(test)]
static TEST_JWKS_OVERRIDE: OnceLock<Mutex<Option<JwkSet>>> = OnceLock::new();

#[cfg(test)]
pub(crate) fn set_test_jwks(jwks: Option<JwkSet>) {
    let cell = TEST_JWKS_OVERRIDE.get_or_init(|| Mutex::new(None));
    if let Ok(mut guard) = cell.lock() {
        *guard = jwks;
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(feature = "http-client")]
fn fetch_jwks(url: &str) -> Result<JwkSet, String> {
    // Refuse plaintext JWKS URLs: an http(s)-downgraded or MITM'd JWKS lets an
    // attacker inject their own signing keys and forge tokens. Verification keys
    // must be fetched over TLS.
    if !url.trim().to_ascii_lowercase().starts_with("https://") {
        return Err(format!(
            "JWKS URL must use https:// (refusing non-TLS endpoint to prevent key-injection MITM): {url}"
        ));
    }
    reqwest::blocking::get(url)
        .map_err(|err| format!("JWKS fetch failed: {err}"))?
        .error_for_status()
        .map_err(|err| format!("JWKS fetch returned an error status: {err}"))?
        .json::<JwkSet>()
        .map_err(|err| format!("JWKS JSON parse failed: {err}"))
}

#[cfg(not(feature = "http-client"))]
fn fetch_jwks(_url: &str) -> Result<JwkSet, String> {
    Err("JWKS validation requires the http-client feature".to_string())
}

fn cached_jwks(config: &SecurityConfig, refresh: bool) -> Result<JwkSet, String> {
    #[cfg(test)]
    if let Some(cell) = TEST_JWKS_OVERRIDE.get()
        && let Ok(guard) = cell.lock()
        && let Some(jwks) = guard.as_ref()
    {
        return Ok(jwks.clone());
    }
    let url = config
        .jwt_jwks_url
        .as_deref()
        .filter(|url| !url.trim().is_empty())
        .ok_or_else(|| "UDB_JWT_JWKS_URL is not configured".to_string())?;
    let now = unix_now();
    let cache = JWT_JWKS_CACHE.get_or_init(|| Mutex::new(None));
    if !refresh
        && let Ok(guard) = cache.lock()
        && let Some(entry) = guard.as_ref()
        && entry.url == url
        && now.saturating_sub(entry.fetched_at_unix) <= config.jwt_jwks_cache_ttl_secs
    {
        return Ok(entry.jwks.clone());
    }
    let jwks = fetch_jwks(url)?;
    if let Ok(mut guard) = cache.lock() {
        *guard = Some(JwksCacheEntry {
            url: url.to_string(),
            fetched_at_unix: now,
            jwks: jwks.clone(),
        });
    }
    Ok(jwks)
}

fn jwks_decoding_key(
    config: &SecurityConfig,
    header: &jsonwebtoken::Header,
) -> Result<DecodingKey, String> {
    let kid = header
        .kid
        .as_deref()
        .filter(|kid| !kid.trim().is_empty())
        .ok_or_else(|| "JWT header missing kid for JWKS verification".to_string())?;
    for refresh in [false, true] {
        let jwks = cached_jwks(config, refresh)?;
        if let Some(jwk) = jwks
            .keys
            .iter()
            .find(|jwk| jwk.common.key_id.as_deref() == Some(kid))
        {
            return DecodingKey::from_jwk(jwk)
                .map_err(|err| format!("invalid JWKS key for kid '{kid}': {err}"));
        }
    }
    Err(format!("JWT kid '{kid}' not found in JWKS"))
}

/// Sign a UDB-issued access token (RS256) from the configured private key
/// (`UDB_JWT_PRIVATE_KEY`, inline PEM or path). The emitted claims mirror
/// [`SecurityClaims`] so the token validates through the same
/// [`validate_bearer_token`] path. Returns `Ok(None)` when no signing key is
/// configured (the caller then falls back to server-side sessions only); `Err`
/// Key id (`kid`) for the default UDB-issued RS256 signing key. Stamped into
/// both the JWT header (`sign_access_token`) and the published JWK
/// (`rsa_jwk_from_pem`) so JWKS consumers can select the verification key by
/// `kid`. One named const keeps header and JWKS provably consistent. When the
/// DB-backed signing-key registry has an ACTIVE key, that key's `kid` supersedes
/// this static id for newly-issued tokens; this remains the env/dev fallback kid.
pub const UDB_RS256_KID: &str = "udb-rs256-1";

/// Process-global snapshot of the DB-backed JWT signing-key registry, kept in
/// sync by the async auth control plane (`signing_keys::refresh_signing_key_cache`).
///
/// The signing path ([`sign_access_token`]) and the validation path
/// ([`validate_bearer_token`]) are synchronous and have no DB/runtime handle, so
/// the async registry reads + at-rest decryption happen out of band (at startup
/// seed and on rotation) and the resolved material is published here:
///   - `active` — the ACTIVE key's decrypted private PEM + its `kid`, used to
///     SIGN new tokens (so a rotated key actually signs, not just publishes).
///   - `public_by_kid` — ACTIVE + VERIFYING public PEMs keyed by `kid`, used to
///     VERIFY tokens during the rotation overlap window without a JWKS URL.
///
/// Empty by default (no registry / dev-unseeded): callers fall back to the env
/// `jwt_private_key` + [`UDB_RS256_KID`] for signing and `jwt_public_key` for
/// validation, so single-key deployments keep working unchanged.
#[derive(Default, Clone)]
pub struct SigningKeyRegistrySnapshot {
    /// ACTIVE key: (kid, decrypted private PEM). `None` when the registry is
    /// empty/unseeded — signing then uses the env key.
    pub active: Option<(String, String)>,
    /// ACTIVE + VERIFYING public PEMs keyed by `kid` for verification.
    pub public_by_kid: HashMap<String, String>,
}

static SIGNING_KEY_REGISTRY_CACHE: OnceLock<RwLock<SigningKeyRegistrySnapshot>> = OnceLock::new();

fn signing_key_registry_cache() -> &'static RwLock<SigningKeyRegistrySnapshot> {
    SIGNING_KEY_REGISTRY_CACHE.get_or_init(|| RwLock::new(SigningKeyRegistrySnapshot::default()))
}

/// Publish a fresh registry snapshot (called by the async auth control plane at
/// startup seed and on key rotation). Overwrites the previous snapshot wholesale.
pub fn install_signing_key_registry_snapshot(snapshot: SigningKeyRegistrySnapshot) {
    if let Ok(mut guard) = signing_key_registry_cache().write() {
        *guard = snapshot;
    }
}

/// The ACTIVE signing key (decrypted private PEM, kid) when the registry has one.
/// `None` falls the signing path back to the env key + [`UDB_RS256_KID`].
pub fn active_signing_key() -> Option<(String, String)> {
    signing_key_registry_cache()
        .read()
        .ok()
        .and_then(|g| g.active.clone())
}

/// The registry public PEM for `kid` (ACTIVE or VERIFYING), if published. Used by
/// [`validate_bearer_token`] to resolve a decoding key by the token header `kid`
/// across multiple live keys during a rotation overlap window.
fn registry_public_pem_for_kid(kid: &str) -> Option<String> {
    signing_key_registry_cache()
        .read()
        .ok()
        .and_then(|g| g.public_by_kid.get(kid).cloned())
}

/// Sign a UDB-issued access token (RS256) from the configured env private key
/// (`UDB_JWT_PRIVATE_KEY`, inline PEM or path), stamping [`UDB_RS256_KID`]. This
/// is the single-key fallback used when the signing-key registry has no ACTIVE
/// key; the registry path calls [`sign_access_token_with_key`] directly. Returns
/// `Ok(None)` when no env key is configured (caller falls back to server-side
/// sessions only); `Err` only on a real key/signing failure. `iss`/`aud` are
/// stamped when configured.
#[allow(clippy::too_many_arguments)]
pub fn sign_access_token(
    config: &SecurityConfig,
    subject: &str,
    tenant_id: &str,
    project_id: &str,
    scopes: &[String],
    roles: &[String],
    service_identity: &str,
    jti: &str,
    auth_method: &str,
    now_unix: u64,
) -> Result<Option<(String, i64)>, String> {
    // Default/fallback signer: env private key (inline PEM or path) stamped with
    // the static [`UDB_RS256_KID`]. The registry-key path delegates here only when
    // the registry has no ACTIVE key (dev/unseeded), so single-key deployments are
    // unchanged. `Ok(None)` when no env key is configured (sessions-only).
    let Some(key_src) = config.jwt_private_key.clone() else {
        return Ok(None);
    };
    let Some(private_pem) = SecurityConfig::resolve_pem(&key_src)? else {
        return Ok(None);
    };
    sign_access_token_with_key(
        config,
        subject,
        tenant_id,
        project_id,
        scopes,
        roles,
        service_identity,
        jti,
        auth_method,
        now_unix,
        &private_pem,
        UDB_RS256_KID,
    )
    .map(Some)
}

/// Sign a UDB-issued RS256 access token with an *explicit* private PEM + `kid`.
///
/// This is the registry-aware signer: the auth control plane resolves the ACTIVE
/// signing key's decrypted PEM + `kid` (via the signing-key registry) and calls
/// this so the token is signed by — and its header `kid` names — the key that is
/// actually live. [`sign_access_token`] delegates here with the env key +
/// [`UDB_RS256_KID`] as the single-key fallback. Returns `(token, exp_unix)`;
/// `Err` only on a real key-parse/sign failure.
#[allow(clippy::too_many_arguments)]
pub fn sign_access_token_with_key(
    config: &SecurityConfig,
    subject: &str,
    tenant_id: &str,
    project_id: &str,
    scopes: &[String],
    roles: &[String],
    service_identity: &str,
    jti: &str,
    auth_method: &str,
    now_unix: u64,
    private_pem: &str,
    kid: &str,
) -> Result<(String, i64), String> {
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use serde_json::json;

    let encoding_key = EncodingKey::from_rsa_pem(private_pem.as_bytes())
        .map_err(|e| format!("invalid JWT private key: {e}"))?;
    let ttl = config.jwt_access_ttl_secs.max(1);
    let exp = now_unix.saturating_add(ttl) as i64;
    let mut claims = serde_json::Map::new();
    claims.insert("sub".into(), json!(subject));
    claims.insert("iat".into(), json!(now_unix as i64));
    claims.insert("exp".into(), json!(exp));
    claims.insert("jti".into(), json!(jti));
    if !tenant_id.is_empty() {
        claims.insert("tenant_id".into(), json!(tenant_id));
    }
    if !project_id.is_empty() {
        claims.insert("project_id".into(), json!(project_id));
    }
    if !scopes.is_empty() {
        claims.insert("scopes".into(), json!(scopes));
    }
    if !roles.is_empty() {
        claims.insert("roles".into(), json!(roles));
    }
    if !service_identity.is_empty() {
        claims.insert("service_identity".into(), json!(service_identity));
    }
    if !auth_method.is_empty() {
        claims.insert("auth_method".into(), json!(auth_method));
        // Assurance level (ACR): multi-factor / phishing-resistant methods are
        // AAL2; everything else is AAL1. Resource servers gate step-up on this.
        let acr = if matches!(auth_method, "mfa" | "webauthn" | "passkey" | "totp") {
            "aal2"
        } else {
            "aal1"
        };
        claims.insert("acr".into(), json!(acr));
    }
    if let Some(iss) = &config.jwt_issuer {
        claims.insert("iss".into(), json!(iss));
    }
    if let Some(aud) = &config.jwt_audience {
        claims.insert("aud".into(), json!(aud));
    }
    let mut header = Header::new(Algorithm::RS256);
    // Stamp the signing key's `kid` so JWKS consumers can select the verification
    // key by `kid`. For the env fallback this is `UDB_RS256_KID`; for a registry
    // ACTIVE key it is that key's id (matching the JWK published in JWKS).
    header.kid = Some(kid.to_string());
    let token = encode(&header, &serde_json::Value::Object(claims), &encoding_key)
        .map_err(|e| format!("failed to sign access token: {e}"))?;
    Ok((token, exp))
}

/// Decode and validate a raw bearer JWT against the configured public key and
/// validation rules, returning the mapped claims. This shares one validation
/// path with the per-request broker flow (`security_from_request`) so the auth
/// service's `Authenticate` RPC enforces the same `exp`/`nbf`/`iss`/`aud`/alg
/// rules. `Err` carries a caller-safe message (no key material).
pub fn validate_bearer_token(
    config: &SecurityConfig,
    token: &str,
) -> Result<SecurityClaims, String> {
    // Pick the decoding-key constructor by the token's declared algorithm
    // family. A blind `from_rsa_pem().or_else(from_ec_pem)…` chain can build a
    // wrong-family key from a PEM that several constructors accept by tag, which
    // then surfaces as a confusing `InvalidAlgorithm` at verification time.
    let header =
        jsonwebtoken::decode_header(token).map_err(|e| format!("invalid JWT header: {e}"))?;
    // Registry-by-kid resolution (rotation overlap window): when the DB-backed
    // signing-key registry has published an ACTIVE/VERIFYING public key matching
    // the token's `kid`, verify against THAT key. This lets tokens signed by a
    // just-rotated ACTIVE key (or an about-to-retire VERIFYING key) validate even
    // without a JWKS URL — covering the whole overlap window, not just the env
    // key's single kid. Registry keys are RS256; non-RSA tokens skip this and use
    // the env path. Falls through to the env public key when the kid is unknown.
    let registry_key = header
        .kid
        .as_deref()
        .filter(|kid| !kid.trim().is_empty())
        .and_then(registry_public_pem_for_kid)
        .filter(|_| {
            matches!(
                header.alg,
                Algorithm::RS256
                    | Algorithm::RS384
                    | Algorithm::RS512
                    | Algorithm::PS256
                    | Algorithm::PS384
                    | Algorithm::PS512
            )
        });
    let decoding_key = if config.jwt_jwks_url.is_some() {
        jwks_decoding_key(config, &header)?
    } else if let Some(pem) = registry_key {
        DecodingKey::from_rsa_pem(pem.as_bytes())
            .map_err(|e| format!("invalid registry JWT public key format: {e}"))?
    } else {
        let Some(jwt_key_env) = config.jwt_public_key.clone() else {
            return Err(
                "JWT validation is not configured (set UDB_JWT_PUBLIC_KEY or UDB_JWT_JWKS_URL)"
                    .to_string(),
            );
        };
        let key_bytes = SecurityConfig::resolve_pem(&jwt_key_env)?
            .ok_or_else(|| {
                "JWT validation is not configured (set UDB_JWT_PUBLIC_KEY or UDB_JWT_JWKS_URL)"
                    .to_string()
            })?
            .into_bytes();
        match header.alg {
            Algorithm::RS256
            | Algorithm::RS384
            | Algorithm::RS512
            | Algorithm::PS256
            | Algorithm::PS384
            | Algorithm::PS512 => DecodingKey::from_rsa_pem(&key_bytes),
            Algorithm::ES256 | Algorithm::ES384 => DecodingKey::from_ec_pem(&key_bytes),
            Algorithm::EdDSA => DecodingKey::from_ed_pem(&key_bytes),
            Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => {
                return Err(
                    "symmetric (HMAC) JWTs are not supported; configure an asymmetric key"
                        .to_string(),
                );
            }
        }
        .map_err(|e| format!("invalid JWT public key format: {e}"))?
    };
    let mut validation = build_jwt_validation(config);
    // jsonwebtoken's verifier rejects the token if ANY algorithm in
    // `validation.algorithms` belongs to a different key family than the decoding
    // key (it returns `InvalidAlgorithm` before even checking the signature). The
    // default allow-list spans RSA, EC, and EdDSA families, so an RSA key paired
    // with the (multi-family) default set would otherwise fail every token. Narrow
    // the allowed set to the presented token's family; an empty result means the
    // token's algorithm family is not permitted by the configured allow-list.
    let presented_family = jwt_algorithm_family(header.alg);
    validation
        .algorithms
        .retain(|alg| jwt_algorithm_family(*alg) == presented_family);
    if validation.algorithms.is_empty() {
        return Err(format!(
            "JWT algorithm {:?} is not permitted by the configured allowed algorithms",
            header.alg
        ));
    }
    decode::<SecurityClaims>(token, &decoding_key, &validation)
        .map(|data| data.claims)
        .map_err(|e| format!("invalid JWT token: {e}"))
}

// ── Data-plane token-validation cache ─────────────────────────────────────────
// The data plane re-validates the SAME bearer on every request (e.g. a PHP-FPM
// app reusing one admin/user token), so the broker was doing PEM-parse + RSA
// signature verification — asymmetric crypto — on EVERY call. This short-TTL cache
// turns the repeated case into a map lookup, which is the latency a high-volume
// client (PHP especially) actually feels.
//
// SAFETY (this is the auth hot path, so the invariants are explicit):
//   * Keyed by the FULL token string — never a hash — so no collision can swap one
//     token's claims for another's.
//   * A hit is served ONLY while BOTH the TTL window is open AND the token's own
//     `exp` has not passed; a cached entry can never outlive the token.
//   * The TTL (default 5s) bounds how long a *revoked* token could still be served
//     under the optional hardened-revocation posture; `UDB_TOKEN_VALIDATION_CACHE_TTL_SECS=0`
//     disables the cache entirely.
//   * Fail-safe: a poisoned lock or any miss falls through to full validation.
//   * `validate_bearer_token` itself stays UNCACHED — the AuthnService RPCs
//     (Authenticate / ValidateToken / refresh) always perform a fresh verify.
const TOKEN_CACHE_CAPACITY: usize = 4096;

fn token_validation_cache_ttl() -> std::time::Duration {
    static TTL: OnceLock<std::time::Duration> = OnceLock::new();
    *TTL.get_or_init(|| {
        let secs = std::env::var("UDB_TOKEN_VALIDATION_CACHE_TTL_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(5);
        std::time::Duration::from_secs(secs)
    })
}

#[allow(clippy::type_complexity)]
fn token_validation_cache()
-> &'static Mutex<HashMap<String, (SecurityClaims, Option<i64>, std::time::Instant)>> {
    static CACHE: OnceLock<
        Mutex<HashMap<String, (SecurityClaims, Option<i64>, std::time::Instant)>>,
    > = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Data-plane bearer validation with a short-TTL result cache (see the notes
/// above). Semantically identical to [`validate_bearer_token`] on a miss; used by
/// [`security_from_request`] so the high-volume data plane does not pay RSA
/// verification on every repeat of the same token.
pub fn validate_bearer_token_cached(
    config: &SecurityConfig,
    token: &str,
) -> Result<SecurityClaims, String> {
    let ttl = token_validation_cache_ttl();
    if ttl.is_zero() {
        return validate_bearer_token(config, token);
    }
    let now = std::time::Instant::now();
    let now_unix = unix_now() as i64;
    if let Ok(cache) = token_validation_cache().lock() {
        if let Some((claims, exp, cached_at)) = cache.get(token) {
            let fresh = now.duration_since(*cached_at) < ttl;
            let unexpired = exp.map(|e| now_unix < e).unwrap_or(true);
            if fresh && unexpired {
                return Ok(claims.clone());
            }
        }
    }
    let claims = validate_bearer_token(config, token)?;
    let exp = claims.exp;
    if let Ok(mut cache) = token_validation_cache().lock() {
        // Bounded memory: drop stale entries first, then hard-clear if still full.
        if cache.len() >= TOKEN_CACHE_CAPACITY {
            cache.retain(|_, (_, _, at)| now.duration_since(*at) < ttl);
            if cache.len() >= TOKEN_CACHE_CAPACITY {
                cache.clear();
            }
        }
        cache.insert(token.to_string(), (claims.clone(), exp, now));
    }
    Ok(claims)
}

pub fn security_from_request<T>(request: &Request<T>) -> Result<SecurityContext, Status> {
    let config = SecurityConfig::current();
    let metadata = request.metadata();
    let header = |name: &str| {
        metadata
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string()
    };

    let trace_id = non_empty(header("traceparent"))
        .and_then(|traceparent| traceparent.split('-').nth(1).map(ToString::to_string))
        .or_else(|| non_empty(header("x-trace-id")))
        .unwrap_or_default();

    let correlation_id = header("x-correlation-id");
    let user_id = header("x-user-id");
    let consistency = header("x-udb-consistency");
    let client_catalog_version = header("x-udb-client-catalog-version");
    let target_backend = header("x-udb-target-backend");
    let target_instance = header("x-udb-target-instance");
    let routing_policy = header("x-udb-routing-policy");
    let read_fence_json = header("x-udb-read-fence");
    let project_id_header = non_empty(header("x-udb-project-id"))
        .or_else(|| non_empty(header("x-project-id")))
        .unwrap_or_default();
    let primary_read = header_bool("x-udb-primary-read", &header);
    let eventual_consistency_allowed = header_bool("x-udb-eventual-consistency-allowed", &header)
        || matches!(
            consistency.to_ascii_lowercase().replace('-', "_").as_str(),
            "eventual" | "eventual_consistency"
        );
    let max_replica_lag_ms = header("x-udb-max-replica-lag-ms")
        .parse::<u64>()
        .unwrap_or_default();

    if config.jwt_public_key.is_some() || config.jwt_jwks_url.is_some() {
        // A JWT verifier is configured via either a static PEM or a JWKS URL.
        // Both must enter this branch: gating on `jwt_public_key` alone let a
        // JWKS-only deployment silently skip JWT validation and fall through to
        // the mTLS/header path (fail-open). `validate_bearer_token` resolves the
        // key from JWKS when no static PEM is present.
        let auth_header = header("authorization");
        let token = auth_header.strip_prefix("Bearer ").ok_or_else(|| {
            Status::unauthenticated("missing or invalid authorization header (JWT required)")
        })?;

        // Single JWT validation path: reuse the CACHED validator (the data plane
        // re-checks the same token on every request; the AuthnService RPCs still
        // call the uncached `validate_bearer_token`). Semantically identical to a
        // fresh verify on a cache miss.
        let claims =
            validate_bearer_token_cached(&config, token).map_err(Status::unauthenticated)?;

        // `sub` is the canonical principal id; fall back to it when the
        // x-user-id header is absent.
        let user_id = if user_id.trim().is_empty() {
            claims.sub.clone().unwrap_or_default()
        } else {
            user_id
        };
        // Compute before the struct literal: the literal partially moves `claims`.
        let resolved_scopes = claims.resolved_scopes();

        return Ok(SecurityContext {
            tenant_id: claims.tenant_id.unwrap_or_else(|| header("x-tenant-id")),
            purpose: claims.purpose.unwrap_or_else(|| header("x-purpose")),
            correlation_id,
            user_id,
            scopes: resolved_scopes,
            service_identity: claims
                .service_identity
                .unwrap_or_else(|| "unknown".to_string()),
            trace_id,
            project_id: claims
                .project_id
                .unwrap_or_else(|| project_id_header.clone()),
            consistency,
            max_replica_lag_ms,
            client_catalog_version: client_catalog_version.clone(),
            target_backend,
            target_instance,
            routing_policy,
            primary_read,
            eventual_consistency_allowed,
            read_fence_json,
        });
    }

    let mtls_required = config.mtls_required;

    let service_identity = if mtls_required {
        request
            .peer_certs()
            .as_deref()
            .and_then(|certs| certs.first())
            .and_then(|cert| service_identity_from_der(cert.as_ref()))
            .ok_or_else(|| {
                Status::unauthenticated(
                    "mTLS client certificate required — header fallback disabled",
                )
            })?
    } else {
        request
            .peer_certs()
            .as_deref()
            .and_then(|certs| certs.first())
            .and_then(|cert| service_identity_from_der(cert.as_ref()))
            .or_else(|| non_empty(header("x-client-cert-cn")))
            .or_else(|| non_empty(header("x-service-identity")))
            .unwrap_or_else(|| "unknown".to_string())
    };

    let allow_header_scopes = config.allow_header_scopes;
    let scopes = if allow_header_scopes {
        header("x-scopes")
            .split(',')
            .map(str::trim)
            .filter(|scope| !scope.is_empty())
            .map(ToString::to_string)
            .collect()
    } else {
        vec![]
    };

    Ok(SecurityContext {
        tenant_id: header("x-tenant-id"),
        purpose: header("x-purpose"),
        correlation_id,
        user_id,
        scopes,
        service_identity,
        trace_id,
        project_id: project_id_header,
        consistency,
        max_replica_lag_ms,
        client_catalog_version: client_catalog_version.clone(),
        target_backend,
        target_instance,
        routing_policy,
        primary_read,
        eventual_consistency_allowed,
        read_fence_json,
    })
}

fn header_bool(name: &str, header: &impl Fn(&str) -> String) -> bool {
    matches!(
        header(name).to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Derive the service identity from a client certificate.
///
/// Modern service identity lives in the Subject Alternative Name, so we prefer
/// it over the legacy CN: a URI SAN first (e.g. a SPIFFE id
/// `spiffe://trust-domain/ns/sa`), then a DNS SAN, and only then fall back to
/// the subject Common Name. The resolved value flows into
/// `SecurityContext.service_identity` and therefore into the authz `Principal`.
pub fn service_identity_from_der(der: &[u8]) -> Option<String> {
    use x509_parser::extensions::GeneralName;
    let (_, cert) = parse_x509_certificate(der).ok()?;
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        let names = &san.value.general_names;
        if let Some(uri) = names.iter().find_map(|gn| match gn {
            GeneralName::URI(uri) if !uri.is_empty() => Some(uri.to_string()),
            _ => None,
        }) {
            return Some(uri);
        }
        if let Some(dns) = names.iter().find_map(|gn| match gn {
            GeneralName::DNSName(dns) if !dns.is_empty() => Some(dns.to_string()),
            _ => None,
        }) {
            return Some(dns);
        }
    }
    cert.subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(ToString::to_string)
}

pub fn enforce_select_export_controls(
    manifest: &CatalogManifest,
    context: &SecurityContext,
    message_type: &str,
    selected_fields: &[String],
) -> Result<(), Status> {
    if context.has_scope("udb:pii:read") {
        return Ok(());
    }
    if matches!(
        context.purpose.as_str(),
        "export" | "verification" | "audit"
    ) {
        return Ok(());
    }
    let message_types = message_type
        .split([',', '+'])
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if message_types.len() > 1 {
        for nested in message_types {
            let nested_fields = selected_fields
                .iter()
                .filter_map(|field| {
                    let (prefix, column) = field.split_once('.')?;
                    if prefix.eq_ignore_ascii_case(nested) {
                        Some(column.to_string())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            enforce_select_export_controls(manifest, context, nested, &nested_fields)?;
        }
        return Ok(());
    }
    let Some(table) = table_for_message(manifest, message_type) else {
        return Ok(());
    };
    let selected = if selected_fields.is_empty() {
        table
            .columns
            .iter()
            .map(|column| column.column_name.clone())
            .collect::<Vec<_>>()
    } else {
        selected_fields
            .iter()
            .map(|field| field.to_ascii_lowercase())
            .collect::<Vec<_>>()
    };
    let blocked = table
        .columns
        .iter()
        .filter(|column| selected.iter().any(|field| field == &column.column_name))
        .any(is_aead_or_pii_column);
    if blocked {
        Err(Status::permission_denied(
            "PII/encrypted fields require purpose export, verification, or audit, or scope udb:pii:read",
        ))
    } else {
        Ok(())
    }
}

fn is_aead_or_pii_column(column: &ManifestColumn) -> bool {
    column.encrypted || column.security.is_encrypted || column.security.is_pii
}

fn non_empty(value: String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

/// Read an env var, returning `None` when unset or blank.
fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().and_then(non_empty)
}

/// Read a comma-separated env var into a trimmed, non-empty `Vec<String>`.
fn csv_env(key: &str) -> Vec<String> {
    std::env::var(key)
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Map JWT algorithm names (`"RS256"`, `"EdDSA"`, …) to `Algorithm`. Unknown
/// names are skipped. Symmetric algorithms (HS*) are intentionally not mapped —
/// UDB's JWT path is public-key only.
fn parse_jwt_algorithms(spec: &[String]) -> Vec<Algorithm> {
    spec.iter()
        .filter_map(|name| match name.trim().to_ascii_uppercase().as_str() {
            "RS256" => Some(Algorithm::RS256),
            "RS384" => Some(Algorithm::RS384),
            "RS512" => Some(Algorithm::RS512),
            "ES256" => Some(Algorithm::ES256),
            "ES384" => Some(Algorithm::ES384),
            "PS256" => Some(Algorithm::PS256),
            "PS384" => Some(Algorithm::PS384),
            "PS512" => Some(Algorithm::PS512),
            "EDDSA" => Some(Algorithm::EdDSA),
            _ => None,
        })
        .collect()
}

/// The default asymmetric algorithm set when `UDB_JWT_ALLOWED_ALGS` is unset.
fn default_jwt_algorithms() -> Vec<Algorithm> {
    vec![
        Algorithm::RS256,
        Algorithm::RS384,
        Algorithm::RS512,
        Algorithm::ES256,
        Algorithm::ES384,
        Algorithm::EdDSA,
    ]
}

/// Key-family discriminant for a JWT algorithm. `jsonwebtoken::Algorithm::family`
/// is crate-private, so we mirror its grouping here: a decoding key and the
/// algorithms it is validated against must belong to the same family. Distinct
/// values per family are all that callers compare.
fn jwt_algorithm_family(alg: Algorithm) -> u8 {
    match alg {
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => 0,
        Algorithm::RS256
        | Algorithm::RS384
        | Algorithm::RS512
        | Algorithm::PS256
        | Algorithm::PS384
        | Algorithm::PS512 => 1,
        Algorithm::ES256 | Algorithm::ES384 => 2,
        Algorithm::EdDSA => 3,
    }
}

/// Build the `jsonwebtoken::Validation` from the security config. Pure (no env,
/// no key material) so it can be unit-tested directly.
///
/// Hardening over the previous inline logic:
/// - `exp` AND `nbf` are validated (was `exp` only).
/// - `leeway` is operator-tunable clock-skew tolerance (was the crate default).
/// - issuer is validated when `UDB_JWT_ISSUER` is set.
/// - audience is validated when `UDB_JWT_AUDIENCE` is set, and audience
///   validation is explicitly **disabled** otherwise (the crate default leaves
///   it on, which can reject tokens that merely carry an `aud` claim).
pub(crate) fn build_jwt_validation(config: &SecurityConfig) -> Validation {
    let mut validation = Validation::new(Algorithm::RS256);
    validation.algorithms = if config.jwt_allowed_algs.is_empty() {
        default_jwt_algorithms()
    } else {
        let parsed = parse_jwt_algorithms(&config.jwt_allowed_algs);
        if parsed.is_empty() {
            default_jwt_algorithms()
        } else {
            parsed
        }
    };
    validation.validate_exp = true;
    validation.validate_nbf = true;
    validation.leeway = config.jwt_clock_skew_secs;
    if let Some(iss) = config.jwt_issuer.as_deref().filter(|s| !s.is_empty()) {
        validation.set_issuer(&[iss]);
    }
    match config.jwt_audience.as_deref().filter(|s| !s.is_empty()) {
        Some(aud) => validation.set_audience(&[aud]),
        None => validation.validate_aud = false,
    }
    validation
}

// ── Phase 9: Audit Logging ────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogEntry {
    pub audit_id: String,
    pub actor: String,
    pub operation: String,
    pub target: String,
    pub request_json: serde_json::Value,
    pub result: String,
    pub tenant_id: String,
    pub project_id: String,
    pub correlation_id: String,
    pub service_identity: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub duration_ms: u64,
    pub error_message: Option<String>,
}

impl AuditLogEntry {
    pub fn new(
        context: &SecurityContext,
        operation: &str,
        target: &str,
        request_json: serde_json::Value,
    ) -> Self {
        Self {
            audit_id: uuid::Uuid::new_v4().to_string(),
            actor: context.user_id.clone(),
            operation: operation.to_string(),
            target: target.to_string(),
            request_json,
            result: "ok".to_string(),
            tenant_id: context.tenant_id.clone(),
            project_id: context.project_id.clone(),
            correlation_id: context.correlation_id.clone(),
            service_identity: context.service_identity.clone(),
            timestamp: chrono::Utc::now(),
            duration_ms: 0,
            error_message: None,
        }
    }

    pub fn with_error(mut self, error: &str) -> Self {
        self.result = "error".to_string();
        self.error_message = Some(error.to_string());
        self
    }

    pub fn with_duration(mut self, duration_ms: u64) -> Self {
        self.duration_ms = duration_ms;
        self
    }

    /// Mask PII fields in the audit log entry
    pub fn mask_pii(mut self) -> Self {
        if let Some(obj) = self.request_json.as_object_mut() {
            for (key, value) in obj.iter_mut() {
                if is_pii_field_name(key) {
                    *value = serde_json::Value::String("***MASKED***".to_string());
                }
            }
        }
        self
    }
}

/// Send audit log entry to configured audit sink
pub async fn send_audit_log(config: &SecurityConfig, entry: AuditLogEntry) -> Result<(), String> {
    if config.audit_sink_url.is_empty() {
        // No audit sink configured, log locally
        tracing::info!(
            audit_id = %entry.audit_id,
            actor = %entry.actor,
            operation = %entry.operation,
            target = %entry.target,
            result = %entry.result,
            tenant_id = %entry.tenant_id,
            correlation_id = %entry.correlation_id,
            "audit_log"
        );
        return Ok(());
    }

    // U34: HTTP audit sink requires the `http-client` feature. Without it
    // the broker falls back to local logging (same as the empty-URL branch)
    // so the slim postgres-only build can still be deployed without `reqwest`.
    #[cfg(feature = "http-client")]
    {
        let client = reqwest::Client::new();
        let masked_entry = if config.pii_safe_logging {
            entry.mask_pii()
        } else {
            entry
        };

        client
            .post(&config.audit_sink_url)
            .json(&masked_entry)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
            .map_err(|e| format!("failed to send audit log: {}", e))?;

        return Ok(());
    }
    #[cfg(not(feature = "http-client"))]
    {
        let _ = entry; // suppress unused warning
        tracing::warn!(
            audit_sink_url = %config.audit_sink_url,
            "audit_sink_url configured but `http-client` feature is disabled; \
             falling back to local audit_log only. Enable the `http-client` \
             feature (or any HTTP-using backend) to deliver to the sink."
        );
        Ok(())
    }
}

fn is_pii_field_name(field: &str) -> bool {
    let lower = field.to_lowercase();
    lower.contains("email")
        || lower.contains("phone")
        || lower.contains("ssn")
        || lower.contains("passport")
        || lower.contains("address")
        || lower.contains("name")
        || lower.contains("dob")
        || lower.contains("birth")
        || lower.contains("account")
        || lower.contains("nid")
        || lower.contains("tin")
        || lower.contains("mobile")
        || lower.contains("national_id")
        // Credential/secret-bearing field names must never be logged in clear.
        || lower.contains("password")
        || lower.contains("passwd")
        || lower.contains("secret")
        || lower.contains("token")
        || lower.contains("api_key")
        || lower.contains("apikey")
        || lower.contains("authorization")
        || lower.contains("key_hash")
        || lower.contains("private_key")
        || lower.contains("session_id")
        || lower.contains("plain_key")
}

// ── Phase 9: PII-Safe Logging ─────────────────────────────────────────────────

/// Mask PII fields in a JSON value for safe logging
pub fn mask_pii_in_json(mut value: serde_json::Value) -> serde_json::Value {
    if let Some(obj) = value.as_object_mut() {
        for (key, val) in obj.iter_mut() {
            if is_pii_field_name(key) {
                *val = serde_json::Value::String("***MASKED***".to_string());
            } else if val.is_object() || val.is_array() {
                *val = mask_pii_in_json(val.clone());
            }
        }
    } else if let Some(arr) = value.as_array_mut() {
        for item in arr.iter_mut() {
            *item = mask_pii_in_json(item.clone());
        }
    }
    value
}

/// Check if a request should be allowed based on IP allowlist
pub fn check_ip_allowlist(peer_addr: Option<&str>, allowlist: &[String]) -> Result<(), Status> {
    if allowlist.is_empty() {
        return Ok(());
    }

    let Some(addr) = peer_addr else {
        return Err(Status::permission_denied("peer address not available"));
    };

    let ip = peer_ip_from_addr(addr).ok_or_else(|| {
        Status::permission_denied(format!("peer address '{addr}' is not a valid IP address"))
    })?;

    for allowed in allowlist {
        if ip_matches_allow_entry(ip, allowed) {
            return Ok(());
        }
    }

    Err(Status::permission_denied(format!(
        "IP {} not in allowlist",
        ip
    )))
}

fn peer_ip_from_addr(addr: &str) -> Option<IpAddr> {
    addr.parse::<SocketAddr>()
        .map(|addr| addr.ip())
        .or_else(|_| addr.parse::<IpAddr>())
        .ok()
}

/// Match an IP against one allowlist entry.
///
/// Supports `*`, exact IPv4/IPv6 addresses, and IPv4/IPv6 CIDR blocks.
pub fn ip_matches_allow_entry(ip: IpAddr, entry: &str) -> bool {
    let entry = entry.trim();
    if entry == "*" {
        return true;
    }
    if let Ok(exact) = entry.parse::<IpAddr>() {
        return ip == exact;
    }
    parse_cidr(entry)
        .map(|(net, prefix_len)| ip_in_cidr(ip, net, prefix_len))
        .unwrap_or(false)
}

/// Parse a CIDR string like `10.0.0.0/8` or `2001:db8::/32`.
pub fn parse_cidr(cidr: &str) -> Option<(IpAddr, u8)> {
    let (addr_str, prefix_str) = cidr.trim().split_once('/')?;
    let addr: IpAddr = addr_str.parse().ok()?;
    let prefix: u8 = prefix_str.parse().ok()?;
    let max = match addr {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    };
    if prefix > max {
        return None;
    }
    Some((addr, prefix))
}

/// Returns true if `ip` falls within the network defined by `net/prefix_len`.
pub fn ip_in_cidr(ip: IpAddr, net: IpAddr, prefix_len: u8) -> bool {
    match (ip, net) {
        (IpAddr::V4(ip), IpAddr::V4(net)) => {
            let ip_u32 = u32::from(ip);
            let net_u32 = u32::from(net);
            let mask = if prefix_len == 0 {
                0u32
            } else {
                !0u32 << (32 - prefix_len)
            };
            (ip_u32 & mask) == (net_u32 & mask)
        }
        (IpAddr::V6(ip), IpAddr::V6(net)) => {
            let ip_u128 = u128::from(ip);
            let net_u128 = u128::from(net);
            let mask = if prefix_len == 0 {
                0u128
            } else {
                !0u128 << (128 - prefix_len)
            };
            (ip_u128 & mask) == (net_u128 & mask)
        }
        _ => false,
    }
}

#[cfg(feature = "http-client")]
use reqwest;
use uuid;

// ── RFC 2104 HMAC ─────────────────────────────────────────────────────────────

/// Generic RFC 2104 HMAC over a `sha1`/`sha2` `Digest` whose block size is 64
/// bytes (true for both SHA-1 and SHA-256). Returns the raw MAC bytes.
///
/// This is the single source of truth for the keyed-HMAC construction used by
/// session/API-key hashing, policy-bundle signing, plan-approval signatures, and
/// RFC 6238 TOTP — all of which previously hand-rolled the identical loop. The
/// output is byte-for-byte identical to those implementations (block size 64,
/// ipad `0x36`, opad `0x5c`, key shortened via the same digest when longer than
/// the block, zero-padded otherwise).
fn hmac<H>(key: &[u8], msg: &[u8]) -> sha2::digest::Output<H>
where
    H: sha2::digest::Digest,
{
    const BLOCK: usize = 64;
    let mut k = if key.len() > BLOCK {
        H::digest(key).to_vec()
    } else {
        key.to_vec()
    };
    k.resize(BLOCK, 0);
    let mut ipad = vec![0x36u8; BLOCK];
    let mut opad = vec![0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let mut inner = H::new();
    inner.update(&ipad);
    inner.update(msg);
    let inner_hash = inner.finalize();
    let mut outer = H::new();
    outer.update(&opad);
    outer.update(inner_hash);
    outer.finalize()
}

/// HMAC-SHA256 over `msg` keyed by `key`, returning the raw 32-byte digest.
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    hmac::<sha2::Sha256>(key, msg).into()
}

/// HMAC-SHA1 over `msg` keyed by `key`, returning the raw 20-byte digest.
/// SHA-1 here is only the RFC 6238 TOTP HMAC (authenticator-app interop), not a
/// security primitive for new constructions.
pub fn hmac_sha1(key: &[u8], msg: &[u8]) -> [u8; 20] {
    hmac::<sha1::Sha1>(key, msg).into()
}

/// Mint a coturn long-term-secret REST credential (RFC 5766-bis / coturn
/// `use-auth-secret`): `username = "<expiry-unix>:<principal>"`,
/// `credential = base64(HMAC-SHA1(secret, username))`. Shared by the native
/// WebRTC `TurnService` and the ws:// signalling bridge so one TURN secret and
/// one formula drive both.
pub fn turn_rest_credential(secret: &[u8], principal: &str, expiry_unix: i64) -> (String, String) {
    use base64::Engine as _;
    let username = format!("{expiry_unix}:{principal}");
    let mac = hmac_sha1(secret, username.as_bytes());
    let credential = base64::engine::general_purpose::STANDARD.encode(mac);
    (username, credential)
}

/// Resolve the shared coturn secret used to mint TURN REST credentials, in order:
/// `UDB_TURN_SECRET`, then `UDB_ENCRYPTION_KEY`. **Fails closed in production**
/// (`UDB_ENV=prod`/`production`): a missing secret returns `None`, so callers
/// advertise STUN only / reject credential minting rather than leaking a
/// well-known dev secret. Outside production a fixed dev secret is used (with a
/// warning) so local flows work without configuration. Shared by the native
/// WebRTC `TurnService` and the ws:// signalling bridge.
pub fn resolve_turn_secret() -> Option<Vec<u8>> {
    if let Some(s) = std::env::var("UDB_TURN_SECRET")
        .ok()
        .filter(|s| !s.trim().is_empty())
    {
        return Some(s.into_bytes());
    }
    if let Some(s) = std::env::var("UDB_ENCRYPTION_KEY")
        .ok()
        .filter(|s| !s.trim().is_empty())
    {
        return Some(s.into_bytes());
    }
    let is_production = std::env::var("UDB_ENV")
        .map(|v| {
            let v = v.to_ascii_lowercase();
            v == "production" || v == "prod"
        })
        .unwrap_or(false);
    if is_production {
        tracing::warn!(
            "no TURN secret configured (set UDB_TURN_SECRET); TURN credential minting \
             is disabled in production (failing closed)"
        );
        None
    } else {
        tracing::warn!(
            "no TURN secret configured; using a non-production dev fallback secret \
             (set UDB_TURN_SECRET, or UDB_ENV=production to fail closed)"
        );
        Some(b"udb-dev-turn-secret".to_vec())
    }
}

// ── Policy linting ────────────────────────────────────────────────────────────

/// A single finding from `lint_policies`.
#[derive(Debug, Clone, Serialize)]
pub struct PolicyLintFinding {
    /// Severity: "warning" or "error"
    pub severity: String,
    /// Category: "deny_by_default", "shadowed_policy", "broad_wildcard", etc.
    pub category: String,
    /// Human-readable description.
    pub message: String,
    /// Zero-based index of the policy that triggered this finding (None = global).
    pub policy_index: Option<usize>,
}

/// Lint a set of ABAC policies and return a list of findings.
///
/// Checks performed:
/// - `deny_by_default`: warn if no Allow policy covers `*`/`*` (broad default allow missing).
/// - `shadowed_policy`: warn when an earlier Allow policy makes a later Allow policy
///   for the same (identity, tenant, purpose, message_type, operation) unreachable.
/// - `broad_wildcard`: warn when a Deny policy uses `*` for tenant_id or purpose,
///   which could silently block unintended callers.
pub fn lint_policies(policies: &[AbacPolicy]) -> Vec<PolicyLintFinding> {
    let mut findings = Vec::new();

    // 1. deny_by_default: if there are no policies at all, all RPCs are gated
    //    by the default_allow flag, which defaults to false (deny).
    if policies.is_empty() {
        findings.push(PolicyLintFinding {
            severity: "warning".to_string(),
            category: "deny_by_default".to_string(),
            message: "No ABAC policies are loaded. All RPCs will be denied by default \
                      unless UDB_ABAC_DEFAULT_ALLOW=true is set."
                .to_string(),
            policy_index: None,
        });
        return findings;
    }

    // 2. shadowed_policy: detect duplicate Allow policies (same key tuple).
    let mut seen_allow_keys: Vec<(usize, String)> = Vec::new();
    for (idx, policy) in policies.iter().enumerate() {
        if policy.effect != PolicyEffect::Allow {
            continue;
        }
        let key = format!(
            "{}|{}|{}|{}|{}",
            policy.service_identity,
            policy.tenant_id,
            policy.purpose,
            policy.message_type,
            policy.operation
        );
        if let Some((prior_idx, _)) = seen_allow_keys.iter().find(|(_, k)| k == &key) {
            findings.push(PolicyLintFinding {
                severity: "warning".to_string(),
                category: "shadowed_policy".to_string(),
                message: format!(
                    "Policy #{idx} (Allow {}/{}) is shadowed by the identical Allow at \
                     policy #{prior_idx}. Remove the duplicate.",
                    policy.message_type, policy.operation
                ),
                policy_index: Some(idx),
            });
        } else {
            seen_allow_keys.push((idx, key));
        }
    }

    // 3. broad_wildcard on Deny policies.
    for (idx, policy) in policies.iter().enumerate() {
        if policy.effect != PolicyEffect::Deny {
            continue;
        }
        if policy.tenant_id == "*" {
            findings.push(PolicyLintFinding {
                severity: "warning".to_string(),
                category: "broad_wildcard".to_string(),
                message: format!(
                    "Policy #{idx} (Deny {}/{}) uses wildcard tenant_id='*'. \
                     This will deny access for ALL tenants. Narrow the scope.",
                    policy.message_type, policy.operation
                ),
                policy_index: Some(idx),
            });
        }
        if policy.purpose == "*" {
            findings.push(PolicyLintFinding {
                severity: "warning".to_string(),
                category: "broad_wildcard".to_string(),
                message: format!(
                    "Policy #{idx} (Deny {}/{}) uses wildcard purpose='*'. \
                     This will deny access regardless of declared purpose. Narrow the scope.",
                    policy.message_type, policy.operation
                ),
                policy_index: Some(idx),
            });
        }
        if policy.message_type == "*" && policy.operation == "*" {
            findings.push(PolicyLintFinding {
                severity: "warning".to_string(),
                category: "broad_wildcard".to_string(),
                message: format!(
                    "Policy #{idx} is a blanket Deny (message_type='*', operation='*'). \
                     This will silently deny all RPCs for matched identities."
                ),
                policy_index: Some(idx),
            });
        }
    }

    findings
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_access_token_stamps_kid_header() {
        let config = SecurityConfig {
            jwt_private_key: Some(include_str!("testdata/jwt_rs256_private.pem").to_string()),
            ..SecurityConfig::default()
        };
        let (token, _exp) = sign_access_token(
            &config,
            "user-1",
            "acme",
            "billing",
            &[],
            &[],
            "",
            "jti-1",
            "pwd",
            1_700_000_000,
        )
        .expect("signing should not error")
        .expect("a private key is configured, so a token is issued");
        let header = jsonwebtoken::decode_header(&token).expect("valid JWT header");
        // The header kid must equal the published JWKS kid so consumers can
        // select the verification key.
        assert_eq!(header.kid.as_deref(), Some(UDB_RS256_KID));
        assert_eq!(header.alg, jsonwebtoken::Algorithm::RS256);
    }

    fn make_allow_policy(service: &str, purpose: &str, operation: &str, scope: &str) -> AbacPolicy {
        AbacPolicy {
            effect: PolicyEffect::Allow,
            service_identity: service.to_string(),
            tenant_id: "*".to_string(),
            purpose: purpose.to_string(),
            message_type: "*".to_string(),
            operation: operation.to_string(),
            required_scope: scope.to_string(),
        }
    }

    fn make_context(
        tenant: &str,
        purpose: &str,
        identity: &str,
        scopes: &[&str],
    ) -> SecurityContext {
        SecurityContext {
            tenant_id: tenant.to_string(),
            purpose: purpose.to_string(),
            service_identity: identity.to_string(),
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
            ..SecurityContext::default()
        }
    }

    #[test]
    fn ip_allowlist_supports_ipv4_cidr_and_socket_addr() {
        let allowlist = vec!["10.20.0.0/16".to_string()];
        assert!(check_ip_allowlist(Some("10.20.30.40:443"), &allowlist).is_ok());
        assert!(check_ip_allowlist(Some("10.21.30.40:443"), &allowlist).is_err());
    }

    #[test]
    fn ip_allowlist_supports_ipv6_cidr() {
        let allowlist = vec!["2001:db8::/32".to_string()];
        assert!(check_ip_allowlist(Some("[2001:db8::1]:443"), &allowlist).is_ok());
        assert!(check_ip_allowlist(Some("2001:db9::1"), &allowlist).is_err());
    }

    #[test]
    fn lint_policies_empty_gives_deny_by_default_warning() {
        let findings = lint_policies(&[]);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].category, "deny_by_default");
        assert_eq!(findings[0].severity, "warning");
    }

    #[test]
    fn lint_policies_detects_shadowed_allow() {
        let p = AbacPolicy {
            effect: PolicyEffect::Allow,
            service_identity: "s".to_string(),
            tenant_id: "t".to_string(),
            purpose: "p".to_string(),
            message_type: "M".to_string(),
            operation: "Select".to_string(),
            required_scope: String::new(),
        };
        let findings = lint_policies(&[p.clone(), p]);
        assert!(findings.iter().any(|f| f.category == "shadowed_policy"));
    }

    #[test]
    fn lint_policies_detects_broad_wildcard_on_deny() {
        let p = AbacPolicy {
            effect: PolicyEffect::Deny,
            service_identity: "s".to_string(),
            tenant_id: "*".to_string(),
            purpose: "p".to_string(),
            message_type: "M".to_string(),
            operation: "Select".to_string(),
            required_scope: String::new(),
        };
        let findings = lint_policies(&[p]);
        assert!(findings.iter().any(|f| f.category == "broad_wildcard"));
    }

    // ── Phase 12: ABAC service policy checks ─────────────────────────────────

    #[test]
    fn abac_deny_policy_overrides_allow() {
        let _ctx = make_context("tenant-1", "ocr", "example.ocr.python", &["udb:write"]);
        let policies = vec![
            make_allow_policy("example.ocr.python", "ocr", "Upsert", "udb:write"),
            AbacPolicy {
                effect: PolicyEffect::Deny,
                service_identity: "example.ocr.python".to_string(),
                tenant_id: "tenant-1".to_string(),
                purpose: "*".to_string(),
                message_type: "*".to_string(),
                operation: "Upsert".to_string(),
                required_scope: String::new(),
            },
        ];
        // Deny rule targets exact tenant — should be caught by lint_policies broad_wildcard check
        let findings = lint_policies(&policies);
        assert!(findings.iter().any(|f| f.category == "broad_wildcard"));
    }

    // ── Phase 12: PII masking ─────────────────────────────────────────────────

    #[test]
    fn pii_mask_replaces_known_sensitive_fields() {
        let value = serde_json::json!({
            "full_name": "John Doe",
            "email": "john@example.com",
            "phone_number": "01700000000",
            "account_number": "0123456789",
            "balance": 5000
        });
        let masked = mask_pii_in_json(value);
        assert_eq!(masked["email"], "***MASKED***", "email should be masked");
        assert_eq!(
            masked["phone_number"], "***MASKED***",
            "phone_number should be masked"
        );
        assert_eq!(
            masked["account_number"], "***MASKED***",
            "account_number should be masked"
        );
        assert_eq!(
            masked["full_name"], "***MASKED***",
            "full_name should be masked"
        );
        // Non-PII numeric field must be untouched
        assert_eq!(masked["balance"], 5000);
    }

    #[test]
    fn pii_mask_handles_nested_objects() {
        let value = serde_json::json!({
            "customer": {
                "email": "secret@example.com",
                "age": 30
            }
        });
        let masked = mask_pii_in_json(value);
        assert_eq!(masked["customer"]["email"], "***MASKED***");
        assert_eq!(masked["customer"]["age"], 30);
    }

    #[test]
    fn pii_mask_handles_arrays() {
        let value = serde_json::json!([
            { "email": "a@b.com", "id": 1 },
            { "email": "c@d.com", "id": 2 }
        ]);
        let masked = mask_pii_in_json(value);
        assert_eq!(masked[0]["email"], "***MASKED***");
        assert_eq!(masked[1]["email"], "***MASKED***");
        assert_eq!(masked[0]["id"], 1);
    }

    // ── JWT validation hardening (Phase 2) ─────────────────────────────────

    #[test]
    fn jwt_algorithm_parsing_maps_known_and_skips_unknown() {
        let algs = parse_jwt_algorithms(&[
            "RS256".into(),
            "eddsa".into(), // case-insensitive
            "ES384".into(),
            "HS256".into(), // symmetric — intentionally skipped
            "garbage".into(),
        ]);
        assert_eq!(
            algs,
            vec![Algorithm::RS256, Algorithm::EdDSA, Algorithm::ES384]
        );
    }

    #[test]
    fn jwt_validation_defaults_disable_aud_and_validate_exp_nbf() {
        let cfg = SecurityConfig::default();
        let v = build_jwt_validation(&cfg);
        assert!(v.validate_exp, "exp must be validated");
        assert!(v.validate_nbf, "nbf must be validated");
        // No audience configured → audience validation must be OFF so a token
        // that merely carries an `aud` claim is not rejected.
        assert!(!v.validate_aud);
        assert!(v.aud.is_none());
        assert!(v.iss.is_none(), "no issuer configured → no issuer check");
        assert_eq!(v.leeway, 60, "default clock-skew leeway");
        assert_eq!(v.algorithms, default_jwt_algorithms());
    }

    #[test]
    fn jwt_validation_enables_issuer_and_audience_when_configured() {
        let cfg = SecurityConfig {
            jwt_issuer: Some("https://issuer.example".into()),
            jwt_audience: Some("udb".into()),
            jwt_clock_skew_secs: 5,
            ..SecurityConfig::default()
        };
        let v = build_jwt_validation(&cfg);
        assert_eq!(v.leeway, 5);
        assert!(v.validate_aud);
        assert!(
            v.aud.as_ref().is_some_and(|set| set.contains("udb")),
            "audience set must contain the configured aud"
        );
        assert!(
            v.iss
                .as_ref()
                .is_some_and(|set| set.contains("https://issuer.example")),
            "issuer set must contain the configured iss"
        );
    }

    #[test]
    fn jwt_validation_explicit_algs_override_default() {
        let cfg = SecurityConfig {
            jwt_allowed_algs: vec!["EdDSA".into()],
            ..SecurityConfig::default()
        };
        let v = build_jwt_validation(&cfg);
        assert_eq!(v.algorithms, vec![Algorithm::EdDSA]);
    }

    #[test]
    fn jwt_validation_all_unknown_algs_fall_back_to_default() {
        let cfg = SecurityConfig {
            jwt_allowed_algs: vec!["HS256".into(), "nonsense".into()],
            ..SecurityConfig::default()
        };
        let v = build_jwt_validation(&cfg);
        // All entries unmappable → fall back to the safe default set rather than
        // an empty algorithm list (which would reject every token).
        assert_eq!(v.algorithms, default_jwt_algorithms());
    }

    #[test]
    fn security_claims_parse_registered_and_udb_claims() {
        let json = serde_json::json!({
            "sub": "user_123",
            "iss": "https://issuer.example",
            "jti": "tok-1",
            "tenant_id": "acme",
            "roles": ["admin", "billing"],
            "relationships_version": "v42"
        });
        let claims: SecurityClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.sub.as_deref(), Some("user_123"));
        assert_eq!(claims.tenant_id.as_deref(), Some("acme"));
        assert_eq!(claims.roles.unwrap(), vec!["admin", "billing"]);
        assert_eq!(claims.relationships_version.as_deref(), Some("v42"));
    }

    #[test]
    fn security_claims_tolerate_array_audience() {
        // Regression guard: an `aud` array must not break claim deserialization
        // (we intentionally do not model `aud` as a String in SecurityClaims).
        let json = serde_json::json!({
            "sub": "u1",
            "aud": ["a", "b"],
            "exp": 9999999999i64
        });
        let claims: SecurityClaims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.sub.as_deref(), Some("u1"));
    }

    #[test]
    fn production_validation_rejects_dev_header_scopes() {
        // Header-based scopes are a dev-only fallback; production must reject them.
        let cfg = SecurityConfig {
            allow_header_scopes: true,
            ..SecurityConfig::default()
        };
        let errors = cfg.validate_production().unwrap_err();
        assert!(
            errors.iter().any(|e| e.contains("Header-based scopes")),
            "production validation must reject UDB_ALLOW_HEADER_SCOPES, got: {errors:?}"
        );
    }

    /// Phase 5 acceptance: a hardened compliance posture must PASS
    /// `validate_production`, and an insecure/dev posture must FAIL it — proving
    /// the posture is enforced in code, not merely documented in a profile.
    #[test]
    fn compliance_hardened_posture_passes_dev_posture_fails() {
        // Mirrors the keys set by `configs/compliance-hardened.yaml`: TLS + service
        // identity + mTLS required, header scopes off, audit sink configured,
        // PII-safe logging on.
        let hardened = SecurityConfig {
            tls_required: true,
            service_identity_required: true,
            mtls_required: true,
            allow_header_scopes: false,
            audit_sink_url: "https://audit.internal.example/api/events".to_string(),
            pii_safe_logging: true,
            ..SecurityConfig::default()
        };
        assert!(
            hardened.validate_production().is_ok(),
            "hardened compliance posture must pass validate_production, got: {:?}",
            hardened.validate_production()
        );
        assert!(
            hardened.is_production(),
            "hardened posture must report is_production() so the startup gate is fatal"
        );

        // The insecure/dev posture mirrored from `configs/services.yaml`
        // (plaintext, header scopes on, no audit sink) must be rejected.
        let dev = SecurityConfig {
            tls_required: false,
            service_identity_required: false,
            mtls_required: false,
            allow_header_scopes: true,
            audit_sink_url: String::new(),
            pii_safe_logging: true,
            ..SecurityConfig::default()
        };
        let errors = dev
            .validate_production()
            .expect_err("dev posture must fail validate_production");
        assert!(
            errors.iter().any(|e| e.contains("TLS must be required")),
            "dev posture must be rejected for missing TLS, got: {errors:?}"
        );
        assert!(
            !dev.is_production(),
            "dev posture must NOT report is_production() so plaintext stays allowed"
        );
    }

    fn hardened_compliance_config() -> SecurityConfig {
        SecurityConfig {
            tls_required: true,
            service_identity_required: true,
            mtls_required: true,
            allow_header_scopes: false,
            audit_sink_url: "https://audit.internal.example/api/events".to_string(),
            pii_safe_logging: true,
            encryption_key_rotation_enabled: true,
            ..SecurityConfig::default()
        }
    }

    #[test]
    fn soc2_profile_accepts_hardened_production_posture() {
        let cfg = hardened_compliance_config();
        assert_eq!(
            cfg.validate_compliance_profile(
                ComplianceProfile::Soc2Type2,
                &ComplianceProfileFacts::default()
            ),
            Ok(())
        );
    }

    #[test]
    fn iso_profile_requires_encryption_key_source_and_rotation() {
        let mut cfg = hardened_compliance_config();
        cfg.encryption_key_rotation_enabled = false;
        let errors = cfg
            .validate_compliance_profile(
                ComplianceProfile::Iso27001And27017,
                &ComplianceProfileFacts::default(),
            )
            .expect_err("ISO profile must require encryption source and rotation");
        assert!(errors.iter().any(|e| e.contains("key source")));
        assert!(errors.iter().any(|e| e.contains("key rotation")));

        cfg.encryption_key_rotation_enabled = true;
        assert_eq!(
            cfg.validate_compliance_profile(
                ComplianceProfile::Iso27001And27017,
                &ComplianceProfileFacts {
                    encryption_key_source_configured: true,
                    ..ComplianceProfileFacts::default()
                },
            ),
            Ok(())
        );
    }

    #[test]
    fn pci_hipaa_profile_requires_fail_closed_mtls_audit_and_encryption() {
        let mut cfg = hardened_compliance_config();
        cfg.mtls_required = false;
        cfg.encryption_key_rotation_enabled = false;
        let errors = cfg
            .validate_compliance_profile(
                ComplianceProfile::PciHipaa,
                &ComplianceProfileFacts::default(),
            )
            .expect_err("PCI/HIPAA profile must reject missing hardening facts");
        assert!(errors.iter().any(|e| e.contains("key source")));
        assert!(errors.iter().any(|e| e.contains("key rotation")));
        assert!(errors.iter().any(|e| e.contains("mTLS")));
        assert!(errors.iter().any(|e| e.contains("fail-closed")));
        assert!(errors.iter().any(|e| e.contains("durable audit sink")));

        cfg.mtls_required = true;
        cfg.encryption_key_rotation_enabled = true;
        assert_eq!(
            cfg.validate_compliance_profile(
                ComplianceProfile::PciHipaa,
                &ComplianceProfileFacts {
                    encryption_key_source_configured: true,
                    fail_closed_enabled: true,
                    durable_audit_sink_configured: true,
                },
            ),
            Ok(())
        );
    }

    #[test]
    fn security_claims_resolve_scope_string_or_array() {
        let from = |v: serde_json::Value| -> SecurityClaims { serde_json::from_value(v).unwrap() };
        // Array `scopes`.
        assert_eq!(
            from(serde_json::json!({"scopes": ["a", "b"]})).resolved_scopes(),
            vec!["a", "b"]
        );
        // OAuth2 singular space-separated `scope`.
        assert_eq!(
            from(serde_json::json!({"scope": "a b c"})).resolved_scopes(),
            vec!["a", "b", "c"]
        );
        // Comma-separated tolerated.
        assert_eq!(
            from(serde_json::json!({"scope": "a, b"})).resolved_scopes(),
            vec!["a", "b"]
        );
        // Neither → empty.
        assert!(from(serde_json::json!({})).resolved_scopes().is_empty());
        // Array wins when both present.
        assert_eq!(
            from(serde_json::json!({"scopes": ["x"], "scope": "y z"})).resolved_scopes(),
            vec!["x"]
        );
    }

    // ── Signed JWT end-to-end coverage (sign → validate_bearer_token) ───────────
    //
    // These exercise the full `validate_bearer_token` path with a real RS256
    // keypair (generated under `src/runtime/testdata/`), proving signature,
    // `exp`, `nbf`, `iss`, and `aud` enforcement rather than only config wiring.

    /// RS256 keypair used solely for tests. NOT a production secret.
    const TEST_JWT_PRIVATE_PEM: &str = include_str!("testdata/jwt_rs256_private.pem");
    const TEST_JWT_PUBLIC_PEM: &str = include_str!("testdata/jwt_rs256_public.pem");

    fn unix_now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_secs() as i64
    }

    /// Sign `claims` into an RS256 JWT using the test private key.
    fn sign_rs256(claims: &serde_json::Value) -> String {
        use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
        let key = EncodingKey::from_rsa_pem(TEST_JWT_PRIVATE_PEM.as_bytes())
            .expect("load test RSA private key");
        encode(&Header::new(Algorithm::RS256), claims, &key).expect("sign test JWT")
    }

    /// `SecurityConfig` that trusts the test public key, plus any overrides.
    fn jwt_verify_config() -> SecurityConfig {
        SecurityConfig {
            jwt_public_key: Some(TEST_JWT_PUBLIC_PEM.to_string()),
            ..SecurityConfig::default()
        }
    }

    #[test]
    fn jwt_signed_valid_token_round_trips() {
        let now = unix_now();
        let token = sign_rs256(&serde_json::json!({
            "sub": "user_42",
            "tenant_id": "acme",
            "scopes": ["udb:read", "udb:write"],
            "iat": now,
            "nbf": now - 5,
            "exp": now + 3600,
        }));
        let claims = validate_bearer_token(&jwt_verify_config(), &token)
            .expect("a correctly-signed, unexpired token must validate");
        assert_eq!(claims.sub.as_deref(), Some("user_42"));
        assert_eq!(claims.tenant_id.as_deref(), Some("acme"));
        assert_eq!(claims.resolved_scopes(), vec!["udb:read", "udb:write"]);
    }

    #[test]
    fn jwt_signed_bad_signature_is_rejected() {
        let now = unix_now();
        let token = sign_rs256(&serde_json::json!({
            "sub": "user_42",
            "exp": now + 3600,
        }));
        // Tamper the signature segment (3rd dot-delimited part) so the body is
        // intact but the RSA signature no longer verifies.
        let mut parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3, "JWT must have header.payload.signature");
        let sig = parts[2];
        // Flip the first signature char to a different base64url char.
        let first = sig.chars().next().unwrap();
        let replacement = if first == 'A' { 'B' } else { 'A' };
        let tampered_sig = format!("{replacement}{}", &sig[1..]);
        parts[2] = &tampered_sig;
        let tampered = parts.join(".");
        let err = validate_bearer_token(&jwt_verify_config(), &tampered)
            .expect_err("a token with a tampered signature must be rejected");
        assert!(
            err.contains("invalid JWT token"),
            "unexpected error for bad signature: {err}"
        );
    }

    #[test]
    fn jwt_signed_expired_token_is_rejected() {
        let now = unix_now();
        // exp well past the default 60s leeway.
        let token = sign_rs256(&serde_json::json!({
            "sub": "user_42",
            "iat": now - 7200,
            "exp": now - 3600,
        }));
        let err = validate_bearer_token(&jwt_verify_config(), &token)
            .expect_err("an expired token must be rejected");
        assert!(
            err.contains("invalid JWT token"),
            "unexpected error for expired token: {err}"
        );
    }

    #[test]
    fn jwt_signed_not_before_in_future_is_rejected() {
        let now = unix_now();
        // nbf well beyond the default 60s leeway; exp still in the future so only
        // the not-before constraint can fail.
        let token = sign_rs256(&serde_json::json!({
            "sub": "user_42",
            "nbf": now + 3600,
            "exp": now + 7200,
        }));
        let err = validate_bearer_token(&jwt_verify_config(), &token)
            .expect_err("a token whose nbf is in the future must be rejected");
        assert!(
            err.contains("invalid JWT token"),
            "unexpected error for not-yet-valid token: {err}"
        );
    }

    #[test]
    fn jwt_signed_wrong_issuer_is_rejected() {
        let now = unix_now();
        let cfg = SecurityConfig {
            jwt_issuer: Some("https://issuer.example".into()),
            ..jwt_verify_config()
        };
        // Valid signature & timing, but the issuer claim does not match.
        let token = sign_rs256(&serde_json::json!({
            "sub": "user_42",
            "iss": "https://evil.example",
            "exp": now + 3600,
        }));
        let err = validate_bearer_token(&cfg, &token)
            .expect_err("a token with the wrong issuer must be rejected");
        assert!(
            err.contains("invalid JWT token"),
            "unexpected error for wrong issuer: {err}"
        );
        // Sanity: the matching issuer is accepted under the same config.
        let good = sign_rs256(&serde_json::json!({
            "sub": "user_42",
            "iss": "https://issuer.example",
            "exp": now + 3600,
        }));
        assert!(
            validate_bearer_token(&cfg, &good).is_ok(),
            "the configured issuer must be accepted"
        );
    }

    #[test]
    fn jwt_signed_wrong_audience_is_rejected() {
        let now = unix_now();
        let cfg = SecurityConfig {
            jwt_audience: Some("udb".into()),
            ..jwt_verify_config()
        };
        // Valid signature & timing, but the audience claim does not match.
        let token = sign_rs256(&serde_json::json!({
            "sub": "user_42",
            "aud": "some-other-service",
            "exp": now + 3600,
        }));
        let err = validate_bearer_token(&cfg, &token)
            .expect_err("a token with the wrong audience must be rejected");
        assert!(
            err.contains("invalid JWT token"),
            "unexpected error for wrong audience: {err}"
        );
        // Sanity: the matching audience is accepted under the same config.
        let good = sign_rs256(&serde_json::json!({
            "sub": "user_42",
            "aud": "udb",
            "exp": now + 3600,
        }));
        assert!(
            validate_bearer_token(&cfg, &good).is_ok(),
            "the configured audience must be accepted"
        );
    }

    /// Base64url modulus (`n`) of the test RSA public key (`jwt_rs256_public.pem`),
    /// extracted with `openssl rsa -pubin -modulus`. Paired with the standard
    /// exponent `AQAB` (65537) it forms a JWK that verifies tokens signed by
    /// `jwt_rs256_private.pem`.
    const TEST_JWK_N: &str = "pbvbMYOK95BdLQvc4SaGOwUp1MTQzVtSHsy-9Jr8wuMTQCJVVOfuCOjdgCwAf1DrOkO9zqSxUnvIilarUI_EPHiqWZVdESGGNY0TReKg0rp3iDh6SJXBjgXzJEY_wr79IRoG-sE7PN4o2C2L9zdOJ9GQLJBxUo2UFa8Yyy9WGtcBX1p_tYKb42bItRpiT-Nwt3HFGTMxNhNXhWUSwY9mwR4rPGCeT6NxwQg7UAmTQAzJPz4LWECAmsKcdecsH_dEU3Jl8TN35OTbx6KGySW-gIrMfzqK12pkgEBn3Zmw5UdOA9sMgjSfjVN-MTfnIl-8VmuaAsnIEi13Embmw8kpeQ";

    /// Build a single-key JWKS advertising the test key under `kid`.
    fn jwks_with_kid(kid: &str) -> jsonwebtoken::jwk::JwkSet {
        let json = format!(
            r#"{{"keys":[{{"kty":"RSA","use":"sig","alg":"RS256","kid":"{kid}","n":"{TEST_JWK_N}","e":"AQAB"}}]}}"#
        );
        serde_json::from_str(&json).expect("parse test JWKS")
    }

    /// Sign a test JWT carrying an explicit `kid` header (JWKS key selection).
    fn sign_rs256_kid(claims: &serde_json::Value, kid: &str) -> String {
        use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
        let key = EncodingKey::from_rsa_pem(TEST_JWT_PRIVATE_PEM.as_bytes())
            .expect("load test RSA private key");
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_string());
        encode(&header, claims, &key).expect("sign test JWT")
    }

    /// `SecurityConfig` that resolves keys via a (test-overridden) JWKS endpoint
    /// rather than a static public key, so the `kid`-lookup path is exercised.
    fn jwt_jwks_config() -> SecurityConfig {
        SecurityConfig {
            jwt_jwks_url: Some("https://test.local/.well-known/jwks.json".to_string()),
            ..SecurityConfig::default()
        }
    }

    #[test]
    fn jwt_jwks_kid_rotation_and_lookup() {
        let now = unix_now();
        let claims = serde_json::json!({ "sub": "user_42", "exp": now + 3600 });
        let cfg = jwt_jwks_config();

        // The published key set advertises kid "k1": a token signed under k1
        // resolves the right JWK and validates.
        set_test_jwks(Some(jwks_with_kid("k1")));
        let t1 = sign_rs256_kid(&claims, "k1");
        assert!(
            validate_bearer_token(&cfg, &t1).is_ok(),
            "a token whose kid is published in the JWKS must validate"
        );

        // A token whose kid is absent from the JWKS is rejected (no silent
        // fall-through to an arbitrary key).
        let unknown = sign_rs256_kid(&claims, "not-published");
        assert!(
            validate_bearer_token(&cfg, &unknown).is_err(),
            "a token with an unpublished kid must be rejected"
        );

        // Rotate the JWKS to kid "k2": tokens under the new kid validate, and
        // the retired kid is no longer accepted (key rotation is picked up).
        set_test_jwks(Some(jwks_with_kid("k2")));
        let t2 = sign_rs256_kid(&claims, "k2");
        assert!(
            validate_bearer_token(&cfg, &t2).is_ok(),
            "a token signed under the rotated kid must validate"
        );
        assert!(
            validate_bearer_token(&cfg, &t1).is_err(),
            "after rotation, a token under the retired kid must be rejected"
        );

        set_test_jwks(None);
    }
}
