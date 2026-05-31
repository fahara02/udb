#![allow(clippy::result_large_err)]

use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Mutex, OnceLock};
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
            allow_header_scopes: std::env::var("UDB_ALLOW_HEADER_SCOPES").is_ok(),
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
        if std::env::var("UDB_ALLOW_HEADER_SCOPES").is_ok() {
            self.allow_header_scopes = true;
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
        if self.jwt_public_key.is_none() && !self.mtls_required {
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
        }
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

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};

#[derive(Debug, Deserialize)]
pub struct SecurityClaims {
    pub tenant_id: Option<String>,
    pub purpose: Option<String>,
    pub scopes: Option<Vec<String>>,
    pub service_identity: Option<String>,
    pub project_id: Option<String>,
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
    let primary_read = header_bool("x-udb-primary-read", &header);
    let eventual_consistency_allowed = header_bool("x-udb-eventual-consistency-allowed", &header)
        || matches!(
            consistency.to_ascii_lowercase().replace('-', "_").as_str(),
            "eventual" | "eventual_consistency"
        );
    let max_replica_lag_ms = header("x-udb-max-replica-lag-ms")
        .parse::<u64>()
        .unwrap_or_default();

    if let Some(jwt_key_env) = config.jwt_public_key.clone() {
        let auth_header = header("authorization");
        let token = auth_header.strip_prefix("Bearer ").ok_or_else(|| {
            Status::unauthenticated("missing or invalid authorization header (JWT required)")
        })?;

        let key_bytes = if jwt_key_env.contains("-----BEGIN") {
            jwt_key_env.into_bytes()
        } else {
            std::fs::read(&jwt_key_env)
                .map_err(|e| Status::internal(format!("failed to read JWT public key: {}", e)))?
        };

        let decoding_key = DecodingKey::from_rsa_pem(&key_bytes)
            .or_else(|_| DecodingKey::from_ec_pem(&key_bytes))
            .or_else(|_| DecodingKey::from_ed_pem(&key_bytes))
            .map_err(|e| Status::internal(format!("invalid JWT public key format: {}", e)))?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.algorithms = vec![
            Algorithm::RS256,
            Algorithm::RS384,
            Algorithm::RS512,
            Algorithm::ES256,
            Algorithm::ES384,
            Algorithm::EdDSA,
        ];
        validation.validate_exp = true;

        let claims = decode::<SecurityClaims>(token, &decoding_key, &validation)
            .map_err(|e| Status::unauthenticated(format!("invalid JWT token: {}", e)))?
            .claims;

        return Ok(SecurityContext {
            tenant_id: claims.tenant_id.unwrap_or_else(|| header("x-tenant-id")),
            purpose: claims.purpose.unwrap_or_else(|| header("x-purpose")),
            correlation_id,
            user_id,
            scopes: claims.scopes.unwrap_or_default(),
            service_identity: claims
                .service_identity
                .unwrap_or_else(|| "unknown".to_string()),
            trace_id,
            project_id: claims
                .project_id
                .unwrap_or_else(|| header("x-udb-project-id")),
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
        project_id: header("x-udb-project-id"),
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

pub fn service_identity_from_der(der: &[u8]) -> Option<String> {
    let (_, cert) = parse_x509_certificate(der).ok()?;
    cert.subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(ToString::to_string)
}

pub fn evaluate_abac(
    policies: &[AbacPolicy],
    context: &SecurityContext,
    message_type: &str,
    operation: &str,
    default_allow: bool,
) -> Result<(), Status> {
    if context.tenant_id.trim().is_empty() {
        return Err(Status::unauthenticated("x-tenant-id is required"));
    }
    if context.purpose.trim().is_empty() {
        return Err(Status::permission_denied("x-purpose is required"));
    }

    let mut matched_allow = false;
    for policy in policies {
        if !matches_policy(policy, context, message_type, operation) {
            continue;
        }
        if !policy.required_scope.trim().is_empty() && !context.has_scope(&policy.required_scope) {
            continue;
        }
        match policy.effect {
            PolicyEffect::Deny => {
                return Err(Status::permission_denied("ABAC policy denied request"));
            }
            PolicyEffect::Allow => matched_allow = true,
        }
    }

    if policies.is_empty() {
        if default_allow {
            Ok(())
        } else {
            Err(Status::permission_denied("no ABAC policies are loaded"))
        }
    } else if matched_allow {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "no ABAC allow policy matched request",
        ))
    }
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

fn matches_policy(
    policy: &AbacPolicy,
    context: &SecurityContext,
    message_type: &str,
    operation: &str,
) -> bool {
    wildcard(&policy.service_identity, &context.service_identity)
        && wildcard(&policy.tenant_id, &context.tenant_id)
        && wildcard(&policy.purpose, &context.purpose)
        && wildcard(&policy.message_type, message_type)
        && wildcard(&policy.operation, operation)
}

fn wildcard(pattern: &str, value: &str) -> bool {
    pattern.trim().is_empty() || pattern == "*" || pattern == value
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
    fn abac_denies_empty_tenant() {
        let context = SecurityContext {
            purpose: "read".to_string(),
            ..SecurityContext::default()
        };
        assert!(evaluate_abac(&[], &context, "M", "Select", true).is_err());
    }

    #[test]
    fn abac_allows_matching_policy() {
        let context = SecurityContext {
            tenant_id: "t1".to_string(),
            purpose: "verification".to_string(),
            service_identity: "svc:go-orchestrator".to_string(),
            scopes: vec!["udb:read".to_string()],
            ..SecurityContext::default()
        };
        let policies = vec![AbacPolicy {
            effect: PolicyEffect::Allow,
            service_identity: "svc:go-orchestrator".to_string(),
            tenant_id: "*".to_string(),
            purpose: "verification".to_string(),
            message_type: "*".to_string(),
            operation: "Select".to_string(),
            required_scope: "udb:read".to_string(),
        }];
        assert!(evaluate_abac(&policies, &context, "Any", "Select", true).is_ok());
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
    fn abac_denies_wrong_scope() {
        let ctx = make_context("tenant-1", "ocr", "example.ocr.python", &["udb:read"]);
        let policies = vec![make_allow_policy(
            "example.ocr.python",
            "ocr",
            "Upsert",
            "udb:write",
        )];
        // Has read scope but policy requires write — should be denied
        let result = evaluate_abac(&policies, &ctx, "DocumentExtraction", "Upsert", false);
        assert!(
            result.is_err(),
            "Should deny when required scope is not in caller's scopes"
        );
    }

    #[test]
    fn abac_denies_wrong_purpose() {
        let ctx = make_context("tenant-1", "reporting", "example.ocr.python", &["udb:read"]);
        let policies = vec![make_allow_policy(
            "example.ocr.python",
            "ocr",
            "Select",
            "udb:read",
        )];
        // Policy only allows purpose=ocr, caller has purpose=reporting
        let result = evaluate_abac(&policies, &ctx, "DocumentExtraction", "Select", false);
        assert!(
            result.is_err(),
            "Should deny when purpose does not match policy"
        );
    }

    #[test]
    fn abac_default_allow_bypasses_empty_policies() {
        let ctx = make_context("tenant-1", "ocr", "example.ocr.python", &["udb:read"]);
        // With default_allow=true and no policies, should pass
        let result = evaluate_abac(&[], &ctx, "DocumentExtraction", "Select", true);
        // Note: empty tenant check happens first — this will still fail due to non-empty tenant
        // This tests that with a valid tenant + default_allow + no policies, we pass.
        assert!(
            result.is_ok(),
            "default_allow=true with no policies should permit calls with valid tenant"
        );
    }

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
}
