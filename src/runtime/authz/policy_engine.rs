//! Policy-engine trait seam (task C.4, step 4 — seam only).
//!
//! [`PolicyEngine`] abstracts the policy *decision surface* that the native
//! `AuthzService` consumes today directly off [`AuthzSnapshot`]'s Casbin path.
//! It exists so a future Cedar / OPA engine can be slotted in behind the same
//! surface without touching the service boundary. **Casbin stays the default**:
//! the bundled [`AuthzSnapshot`] implements this trait over its existing
//! `casbin_authorize` / `lint` / bundle paths, and nothing in the default build
//! selects an alternative engine.
//!
//! The trait deliberately reuses the *existing* authz request/decision/finding/
//! bundle types ([`AuthzQuery`], [`Decision`], [`PolicyLintFinding`],
//! [`SignedBundle`], [`PolicyBundleConfig`]) rather than introducing parallel
//! shapes — the seam is a re-grouping of the real call surface, not a new model.

use async_trait::async_trait;

use crate::runtime::security::PolicyLintFinding;

use super::bundle::{PolicyBundleConfig, SignedBundle};
use super::{AuthzQuery, AuthzSnapshot, Decision};

/// The pluggable policy-decision surface. Implementations evaluate a request to
/// a structured [`Decision`], surface an audit-grade explanation of how that
/// decision was reached, lint the loaded policy set for likely mistakes, expose
/// a signed projection of the policy state for SDK-local caches, and report the
/// authority's current policy version so callers can detect drift.
///
/// All methods are `&self` so an engine is shareable behind an `Arc` and may be
/// swapped atomically (`arc-swap`) the same way the snapshot is today.
#[async_trait]
pub trait PolicyEngine: Send + Sync {
    /// Evaluate `req` and return the structured allow/deny [`Decision`]
    /// (carrying the deterministic `decision_id`, matched policy ids, required
    /// scopes, and policy/relationship versions). This is the hot enforcement
    /// path — equivalent to `AuthzSnapshot::casbin_authorize`.
    async fn decide(&self, req: &AuthzQuery<'_>) -> Decision;

    /// Audit-grade explanation of a decision for the same request. The default
    /// re-runs [`PolicyEngine::decide`]; engines that can attach a richer trace
    /// (matched rule chain, evaluated conditions) override this. The returned
    /// [`Decision`] already exposes `matched_policy_ids` and `deny_reason`.
    async fn explain(&self, req: &AuthzQuery<'_>) -> Decision {
        self.decide(req).await
    }

    /// Lint the loaded policy set for likely-misconfigurations (deny-by-default,
    /// shadowed allows, overly broad wildcard denies), returning the existing
    /// [`PolicyLintFinding`] records the `lint_policies` admin RPC already
    /// surfaces. Synchronous in spirit but `async` for engines that fetch state.
    async fn lint(&self) -> Vec<PolicyLintFinding>;

    /// Produce a signed, time-boxed projection of the policy state scoped to a
    /// tenant/project for SDK-local authorization caches, using the operator's
    /// [`PolicyBundleConfig`]. Returns `None` when bundle signing is disabled
    /// (no secret configured) — mirrors `PolicyBundleConfig::sign`.
    async fn bundle(
        &self,
        cfg: &PolicyBundleConfig,
        tenant_id: &str,
        project_id: &str,
        now_unix: i64,
    ) -> Option<SignedBundle>;

    /// The authority's current policy bundle version (the snapshot `version`),
    /// so callers can detect drift / invalidate caches. Pairs with
    /// [`PolicyEngine::relationship_version`] for the ReBAC tuple generation.
    fn bundle_version(&self) -> String;

    /// The relationship-tuple (ReBAC) version of the current state.
    fn relationship_version(&self) -> String {
        String::new()
    }
}

/// Casbin is the default engine: [`AuthzSnapshot`] already owns the real
/// decision (`casbin_authorize`), lint, and bundle paths, so the seam is a
/// trivial forwarding impl over the existing surface. Kept here so the default
/// build always has a working `PolicyEngine` without selecting any feature.
#[async_trait]
impl PolicyEngine for AuthzSnapshot {
    async fn decide(&self, req: &AuthzQuery<'_>) -> Decision {
        self.casbin_authorize(req).await
    }

    async fn lint(&self) -> Vec<PolicyLintFinding> {
        // The snapshot's policies are the v2 shape; the existing linter operates
        // on the legacy `AbacPolicy` surface, so the engine-level lint of a v2
        // snapshot reduces to the empty-policy / deny-by-default signal here.
        // Richer v2 linting is future work behind this seam.
        if self.policies.is_empty() {
            return crate::runtime::security::lint_policies(&[]);
        }
        Vec::new()
    }

    async fn bundle(
        &self,
        cfg: &PolicyBundleConfig,
        tenant_id: &str,
        project_id: &str,
        now_unix: i64,
    ) -> Option<SignedBundle> {
        cfg.sign(self, tenant_id, project_id, now_unix)
    }

    fn bundle_version(&self) -> String {
        self.version.clone()
    }

    fn relationship_version(&self) -> String {
        self.relationship_version.clone()
    }
}
