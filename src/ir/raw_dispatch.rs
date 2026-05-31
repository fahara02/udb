//! Raw backend-shaped dispatch escape hatch (U2 step 8).
//!
//! Pre-U2 the gRPC `GenericDispatch` accepted backend-shaped JSON
//! directly (`{"sql":"SELECT …"}` to Postgres, `{"collection":…,
//! "filter":{…}}` to Mongo). The neutral IR path is the new default, but
//! we keep the raw path available under a policy flag because:
//!
//! - **Migration headroom.** Existing clients need a deprecation window
//!   before they all switch to the IR surface.
//! - **Capability gaps.** Compilers may not support every operator yet
//!   (Qdrant text-match, ClickHouse window functions, Mongo aggregation
//!   pipelines); operators with the right scope can still send raw bodies.
//!
//! `RawDispatchPolicy` is what the runtime checks before honouring a raw
//! request — `Forbid` for security-conscious deployments, `AllowWithAudit`
//! for production with deprecation logging, `AllowAll` for dev sandboxes.

use serde::{Deserialize, Serialize};

/// Whether to honour backend-shaped JSON requests on the generic-dispatch
/// path, and how strictly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawDispatchPolicy {
    /// Reject raw requests with `tonic::Code::PermissionDenied`. This is
    /// the safest default — the IR is the contract.
    #[default]
    Forbid,
    /// Allow raw requests but emit a structured audit event each time so
    /// operators can track the deprecation surface.
    AllowWithAudit,
    /// Allow raw requests with no extra logging. Use for dev sandboxes
    /// and tests only.
    AllowAll,
}

impl RawDispatchPolicy {
    /// Token surfaced in config and audit events. Pin these — config files
    /// reference them by name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Forbid => "forbid",
            Self::AllowWithAudit => "allow_with_audit",
            Self::AllowAll => "allow_all",
        }
    }

    /// Read the policy from `UDB_RAW_DISPATCH_POLICY` env var. Falls back
    /// to `Forbid`. Recognised values match `as_str()` tokens.
    pub fn from_env(var: &str) -> Self {
        std::env::var(var)
            .ok()
            .and_then(|v| match v.trim().to_ascii_lowercase().as_str() {
                "forbid" | "deny" | "reject" => Some(Self::Forbid),
                "allow_with_audit" | "audit" => Some(Self::AllowWithAudit),
                "allow_all" | "allow" => Some(Self::AllowAll),
                _ => None,
            })
            .unwrap_or_default()
    }

    /// True when this policy permits raw dispatch at all.
    pub fn permits(self) -> bool {
        !matches!(self, Self::Forbid)
    }

    /// True when the runtime should emit an audit event for each
    /// raw-dispatch RPC.
    pub fn requires_audit(self) -> bool {
        matches!(self, Self::AllowWithAudit)
    }
}

/// Audit event a `runtime/security` consumer should record when
/// `requires_audit()` is true. Kept in `crate::ir` because the IR
/// module is the only place that knows when we're hitting the raw path.
#[derive(Debug, Clone, Serialize)]
pub struct RawDispatchAudit {
    pub tenant_id: Option<String>,
    pub project_id: Option<String>,
    pub backend: String,
    pub instance: Option<String>,
    pub message_type: String,
    /// First 200 chars of the raw body — enough to identify the shape
    /// without dumping payloads. The audit sink can hash for stronger
    /// fingerprinting.
    pub body_preview: String,
    pub policy: RawDispatchPolicy,
}

impl RawDispatchAudit {
    /// Build an audit record, truncating `body` to the safe preview size.
    pub fn record(
        tenant_id: Option<&str>,
        project_id: Option<&str>,
        backend: impl Into<String>,
        instance: Option<&str>,
        message_type: impl Into<String>,
        body: &str,
        policy: RawDispatchPolicy,
    ) -> Self {
        const PREVIEW: usize = 200;
        let mut preview = body.chars().take(PREVIEW).collect::<String>();
        if body.len() > preview.len() {
            preview.push_str(" …");
        }
        Self {
            tenant_id: tenant_id.map(str::to_string),
            project_id: project_id.map(str::to_string),
            backend: backend.into(),
            instance: instance.map(str::to_string),
            message_type: message_type.into(),
            body_preview: preview,
            policy,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_forbid() {
        assert_eq!(RawDispatchPolicy::default(), RawDispatchPolicy::Forbid);
        assert!(!RawDispatchPolicy::Forbid.permits());
        assert!(!RawDispatchPolicy::Forbid.requires_audit());
    }

    #[test]
    fn allow_with_audit_demands_audit_records() {
        assert!(RawDispatchPolicy::AllowWithAudit.permits());
        assert!(RawDispatchPolicy::AllowWithAudit.requires_audit());
    }

    #[test]
    fn allow_all_skips_audit() {
        assert!(RawDispatchPolicy::AllowAll.permits());
        assert!(!RawDispatchPolicy::AllowAll.requires_audit());
    }

    #[test]
    fn from_env_recognises_aliases() {
        // Use a uniquely-named var that won't be set in the env.
        let var = "UDB_TEST_RAW_DISPATCH_UNSET";
        assert_eq!(RawDispatchPolicy::from_env(var), RawDispatchPolicy::Forbid);
    }

    #[test]
    fn audit_record_truncates_body_preview() {
        let huge = "x".repeat(500);
        let audit = RawDispatchAudit::record(
            Some("t"),
            None,
            "postgres",
            Some("primary"),
            "Customer",
            &huge,
            RawDispatchPolicy::AllowWithAudit,
        );
        assert!(audit.body_preview.len() <= 210); // 200 + "…" + margin
        assert!(audit.body_preview.ends_with(" …"));
        assert_eq!(audit.policy, RawDispatchPolicy::AllowWithAudit);
    }
}
