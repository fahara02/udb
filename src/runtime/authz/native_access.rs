//! Stage 2: native Postgres restricted-role / DSN contract.
//!
//! Once the authorization engine has produced an *allowed* [`Decision`] for a
//! principal, `GetNativeAccess` projects that decision into a short-lived
//! connection contract so the caller can talk to the backend directly on the
//! native fast path instead of proxying every row through the broker:
//!
//! - a **restricted role** scoped to the caller's tenant and access mode
//!   (read vs. write), derived from the requested action,
//! - a **scoped DSN** rendered from an operator-configured base template, and
//! - the exact set of **`app.current_*` session variables** the SDK must
//!   `SET LOCAL` per transaction so the broker-generated RLS policies see the
//!   same request context the broker enforced when it made the decision.
//!
//! UDB stays the source of the authorization decision; this module only carries
//! an *already-allowed* decision forward. It deliberately does **not** create
//! Postgres roles or mint real passwords — provisioning the restricted roles is
//! the operator's responsibility (Stage 2 role/DSN contract). The DSN here is a
//! rendered connection target for those operator-provisioned roles.

use std::collections::BTreeMap;

use crate::runtime::authz::{Decision, Principal, ResourceRef};
use crate::runtime::backend_context::AppliedContext;

/// Operator configuration for native-access minting, read from the environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeAccessConfig {
    /// `UDB_NATIVE_BASE_DSN` — connection-string template the scoped DSN is
    /// rendered from, e.g.
    /// `postgresql://{role}@db.internal:5432/{database}?sslmode=require`.
    /// `{role}` and `{database}` placeholders are substituted; if `{role}` is
    /// absent the role is injected as the userinfo component. Empty disables
    /// native access entirely (the handler returns the decision with no grant).
    pub base_dsn: String,
    /// `UDB_NATIVE_ROLE_PREFIX` — restricted-role name prefix (default `udb`).
    pub role_prefix: String,
    /// `UDB_NATIVE_DATABASE` — database name substituted for `{database}`.
    pub database: String,
    /// `UDB_NATIVE_SCHEMA` — default schema reported in the grant.
    pub schema: String,
    /// `UDB_NATIVE_BACKEND` — backend label (default `postgres`).
    pub backend: String,
    /// `UDB_NATIVE_ACCESS_TTL_SECS` — grant lifetime in seconds (default 900).
    /// Fifteen minutes keeps direct-DB credentials useful for pooled native
    /// clients while forcing regular re-authorization against UDB policy.
    pub ttl_seconds: u64,
}

impl Default for NativeAccessConfig {
    fn default() -> Self {
        Self {
            base_dsn: String::new(),
            role_prefix: "udb".to_string(),
            database: String::new(),
            schema: "public".to_string(),
            backend: "postgres".to_string(),
            ttl_seconds: 900,
        }
    }
}

impl NativeAccessConfig {
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            base_dsn: std::env::var("UDB_NATIVE_BASE_DSN").unwrap_or(default.base_dsn),
            role_prefix: non_empty_env("UDB_NATIVE_ROLE_PREFIX").unwrap_or(default.role_prefix),
            database: std::env::var("UDB_NATIVE_DATABASE").unwrap_or(default.database),
            schema: non_empty_env("UDB_NATIVE_SCHEMA").unwrap_or(default.schema),
            backend: non_empty_env("UDB_NATIVE_BACKEND").unwrap_or(default.backend),
            ttl_seconds: std::env::var("UDB_NATIVE_ACCESS_TTL_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(default.ttl_seconds),
        }
    }

    /// Native access is only mintable when an operator has configured a base
    /// DSN template; otherwise the contract has nowhere to point.
    pub fn enabled(&self) -> bool {
        !self.base_dsn.trim().is_empty()
    }
}

/// Read vs. write access mode, derived from the requested action. Determines
/// which restricted role the caller is routed to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessMode {
    Read,
    Write,
}

impl AccessMode {
    /// Map a canonical authorization action (see `authz::rpc_action`) to an
    /// access mode. Anything that is not a pure read is treated as write so the
    /// restricted role is never under-privileged for the operation.
    pub fn from_action(action: &str) -> Self {
        match action {
            "data.select" | "vector.search" | "object.read" | "object.presign" => AccessMode::Read,
            _ => AccessMode::Write,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            AccessMode::Read => "ro",
            AccessMode::Write => "rw",
        }
    }
}

/// The minted native-access contract. Only produced for an *allowed* decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeGrant {
    /// Scoped connection string. Secret — must be redacted in logs.
    pub dsn: String,
    /// Restricted role the DSN authenticates as.
    pub role: String,
    pub backend: String,
    pub database: String,
    pub schema: String,
    /// `app.current_*` variables the caller must `SET LOCAL` per transaction.
    pub session_variables: BTreeMap<String, String>,
    pub expires_at_unix: i64,
    pub ttl_seconds: u64,
}

impl NativeAccessConfig {
    /// Derive the restricted role name for a tenant + access mode. Tenant ids
    /// are sanitized to a Postgres-identifier-safe lowercase token so the role
    /// name is stable and injection-free: `<prefix>_<tenant>_<mode>`.
    pub fn role_name(&self, tenant_id: &str, mode: AccessMode) -> String {
        let tenant = sanitize_identifier(tenant_id);
        let tenant = if tenant.is_empty() {
            "default".to_string()
        } else {
            tenant
        };
        format!("{}_{}_{}", self.role_prefix, tenant, mode.as_str())
    }

    /// Render the scoped DSN for a role by substituting the template
    /// placeholders. When the template has no `{role}` placeholder the role is
    /// injected as the connection's userinfo so the DSN still authenticates as
    /// the restricted role.
    pub fn render_dsn(&self, role: &str) -> String {
        let mut dsn = self
            .base_dsn
            .replace("{role}", role)
            .replace("{database}", &self.database);
        if !self.base_dsn.contains("{role}") {
            dsn = inject_userinfo(&dsn, role);
        }
        dsn
    }

    /// Mint a native-access grant from an allowed decision. Returns `None` when
    /// native access is disabled (no base DSN) or the decision did not allow —
    /// UDB never hands out a connection for a denied request.
    pub fn mint(
        &self,
        principal: &Principal,
        resource: &ResourceRef,
        action: &str,
        purpose: &str,
        decision: &Decision,
        now_unix: i64,
    ) -> Option<NativeGrant> {
        if !self.enabled() || !decision.allowed {
            return None;
        }
        let mode = AccessMode::from_action(action);
        let role = self.role_name(&principal.tenant_id, mode);
        let dsn = self.render_dsn(&role);

        // The session variables are the *same* canonical set the broker emits
        // on its own connections (item 135/136), stamped with this decision so
        // RLS + audit (including purpose-limitation policies) see identical
        // context on the native fast path.
        let applied = AppliedContext {
            tenant_id: principal.tenant_id.clone(),
            project_id: principal.project_id.clone(),
            purpose: purpose.to_string(),
            scopes: principal.scopes.join(","),
            correlation_id: String::new(),
            user_id: principal.user_id.clone(),
            service_identity: principal.service_identity.clone(),
            decision_id: decision.decision_id.clone(),
            attributes: BTreeMap::new(),
        };
        let session_variables = applied
            .session_context_pairs()
            .into_iter()
            .filter(|(_, value)| !value.is_empty())
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();

        let schema = if resource.schema.trim().is_empty() {
            self.schema.clone()
        } else {
            resource.schema.clone()
        };

        Some(NativeGrant {
            dsn,
            role,
            backend: self.backend.clone(),
            database: self.database.clone(),
            schema,
            session_variables,
            expires_at_unix: now_unix.saturating_add(self.ttl_seconds as i64),
            ttl_seconds: self.ttl_seconds,
        })
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.trim().is_empty())
}

/// Lowercase, keep `[a-z0-9_]`, collapse everything else to `_`. Produces a
/// safe Postgres identifier fragment from an arbitrary tenant id.
fn sanitize_identifier(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    out.trim_matches('_').to_string()
}

/// Inject `role@` userinfo into a `scheme://host/...` DSN that has no userinfo.
/// If the DSN already carries userinfo, it is left unchanged.
fn inject_userinfo(dsn: &str, role: &str) -> String {
    if let Some((scheme, rest)) = dsn.split_once("://") {
        if rest.contains('@') {
            return dsn.to_string();
        }
        return format!("{scheme}://{role}@{rest}");
    }
    dsn.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::authz::Decision;

    fn allowed(id: &str) -> Decision {
        Decision {
            decision_id: id.to_string(),
            allowed: true,
            ..Decision::default()
        }
    }

    #[test]
    fn disabled_without_base_dsn() {
        let cfg = NativeAccessConfig::default();
        assert!(!cfg.enabled());
        let p = Principal {
            tenant_id: "acme".into(),
            ..Principal::default()
        };
        assert!(
            cfg.mint(
                &p,
                &ResourceRef::message("X"),
                "data.select",
                "billing",
                &allowed("d1"),
                0
            )
            .is_none()
        );
    }

    #[test]
    fn role_name_sanitizes_tenant_and_modes() {
        let cfg = NativeAccessConfig {
            role_prefix: "udb".into(),
            ..NativeAccessConfig::default()
        };
        assert_eq!(
            cfg.role_name("Acme Corp!", AccessMode::Read),
            "udb_acme_corp_ro"
        );
        assert_eq!(cfg.role_name("", AccessMode::Write), "udb_default_rw");
    }

    #[test]
    fn action_to_mode() {
        assert_eq!(AccessMode::from_action("data.select"), AccessMode::Read);
        assert_eq!(AccessMode::from_action("data.upsert"), AccessMode::Write);
        assert_eq!(AccessMode::from_action("admin.manage"), AccessMode::Write);
    }

    #[test]
    fn render_dsn_substitutes_or_injects_role() {
        let templ = NativeAccessConfig {
            base_dsn: "postgresql://host:5432/{database}".into(),
            database: "app".into(),
            ..NativeAccessConfig::default()
        };
        assert_eq!(
            templ.render_dsn("udb_acme_ro"),
            "postgresql://udb_acme_ro@host:5432/app"
        );

        let placeholder = NativeAccessConfig {
            base_dsn: "postgresql://{role}@host/{database}".into(),
            database: "app".into(),
            ..NativeAccessConfig::default()
        };
        assert_eq!(
            placeholder.render_dsn("udb_acme_rw"),
            "postgresql://udb_acme_rw@host/app"
        );
    }

    #[test]
    fn mint_carries_decision_context_into_session_vars() {
        let cfg = NativeAccessConfig {
            base_dsn: "postgresql://host/{database}".into(),
            database: "app".into(),
            ttl_seconds: 600,
            ..NativeAccessConfig::default()
        };
        let p = Principal {
            tenant_id: "acme".into(),
            user_id: "u1".into(),
            service_identity: "svc".into(),
            scopes: vec!["data.read".into()],
            ..Principal::default()
        };
        let grant = cfg
            .mint(
                &p,
                &ResourceRef::message("Invoice"),
                "data.select",
                "billing",
                &allowed("dec-123"),
                1000,
            )
            .expect("grant");
        assert_eq!(grant.role, "udb_acme_ro");
        assert_eq!(grant.expires_at_unix, 1600);
        assert_eq!(
            grant.session_variables.get("app.current_purpose"),
            Some(&"billing".to_string())
        );
        assert_eq!(
            grant.session_variables.get("app.current_decision_id"),
            Some(&"dec-123".to_string())
        );
        assert_eq!(
            grant.session_variables.get("app.current_service_identity"),
            Some(&"svc".to_string())
        );
        assert_eq!(
            grant.session_variables.get("app.current_tenant_id"),
            Some(&"acme".to_string())
        );
    }
}
