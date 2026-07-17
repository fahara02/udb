//! Real Casbin-driven authorization.
//!
//! The [`AuthzSnapshot`] is enforced through an actual [`casbin::Enforcer`]
//! running an advanced **PERM** model (request / policy / role / effect /
//! matchers): RBAC with tenant domains and glob resource/action matching via
//! `keyMatch2`. Deny override is enforced before Casbin because Rust Casbin's
//! built-in effector does not reliably apply the combined deny/allow expression
//! when deny lines are loaded into the enforcer (verified: doing so lets a broad
//! allow win over an explicit deny). Only Allow lines enter the enforcer; an
//! explicit matched Deny short-circuits in Rust beforehand.
//!
//! ABAC gates that the Casbin RBAC matcher does not express — required scopes,
//! attribute conditions, ReBAC relationship tuples, and purpose — pre-filter
//! which policies enter the enforcer. Casbin then drives subject/role/resource/
//! action/domain matching and the allow/deny effect.
//!
//! ## Operator-configurable model
//! The Casbin model is NOT hardcoded: an operator may supply their own
//! `model.conf` via `UDB_AUTHZ_CASBIN_MODEL_PATH` (file) or `UDB_AUTHZ_CASBIN_MODEL`
//! (inline text); absent both, the embedded [`CASBIN_MODEL`] default is used.
//! Any Casbin matcher / effect / role-definition and any built-in function
//! (`keyMatch`, `keyMatch2/3/4`, `regexMatch`, `globMatch`, `ipMatch`) are
//! honored. The **request/policy token contract** the loader maps UDB rows onto
//! is fixed: requests are `r = sub, dom, obj, act`; DB-derived policy lines are
//! `p = sub, dom, obj, act, eft`; the role grouping is `g = _, _`
//! (subject → role). Custom models must keep this token shape; everything else
//! (matchers, effect, functions) is free. [`validate_casbin_model`] parse-checks
//! the configured model at startup so a malformed override fails fast.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use casbin::{CoreApi, DefaultModel, Enforcer, MemoryAdapter, MgmtApi};
use sha2::{Digest, Sha256};

use super::{AuthzPolicy, AuthzQuery, AuthzSnapshot, Decision, Effect, conditions_match, wildcard};

/// The Casbin PERM model. RBAC-with-domains request + policy, a `g` role
/// grouping, allow effect, and `keyMatch2` glob matching for resources and
/// actions. Explicit deny is checked before enforcement. `p.sub == "*"`
/// (wildcard subject) and empty/`*` domains are honored in the matcher so
/// legacy broad policies keep working.
pub(crate) const CASBIN_MODEL: &str = r#"[request_definition]
r = sub, dom, obj, act

[policy_definition]
p = sub, dom, obj, act, eft

[role_definition]
g = _, _

[policy_effect]
e = some(where (p_eft == allow)) && !some(where (p_eft == deny))

[matchers]
m = (p.sub == "*" || g(r.sub, p.sub)) && (p.dom == "*" || p.dom == "" || r.dom == p.dom) && (p.obj == "*" || keyMatch2(r.obj, p.obj)) && (p.act == "*" || r.act == p.act || keyMatch2(r.act, p.act))
"#;

/// Empty selector → Casbin wildcard token.
fn slot(value: &str) -> String {
    if value.trim().is_empty() {
        "*".to_string()
    } else {
        value.to_string()
    }
}

/// Resolve precedence once: an operator-supplied file
/// (`UDB_AUTHZ_CASBIN_MODEL_PATH`) wins, then inline text
/// (`UDB_AUTHZ_CASBIN_MODEL`), else the embedded [`CASBIN_MODEL`] default. An
/// unreadable path is logged and falls through to the next source.
fn resolve_casbin_model_text_once() -> Arc<str> {
    if let Ok(path) = std::env::var("UDB_AUTHZ_CASBIN_MODEL_PATH") {
        let path = path.trim();
        if !path.is_empty() {
            match std::fs::read_to_string(path) {
                Ok(text) => return Arc::from(text.as_str()),
                Err(err) => tracing::error!(
                    %err,
                    path,
                    "UDB_AUTHZ_CASBIN_MODEL_PATH unreadable; falling back to inline/default authz model"
                ),
            }
        }
    }
    match std::env::var("UDB_AUTHZ_CASBIN_MODEL") {
        Ok(text) if !text.trim().is_empty() => Arc::from(text.as_str()),
        _ => Arc::from(CASBIN_MODEL),
    }
}

/// Process-wide model-text cache (item 17): the env vars are consulted once and
/// the selected model file, if any, is read once. The hot decision path only
/// clones the cached `Arc<str>`; it performs no env or filesystem access.
fn cached_casbin_model_text() -> Arc<str> {
    static CACHE: OnceLock<Arc<str>> = OnceLock::new();
    CACHE.get_or_init(resolve_casbin_model_text_once).clone()
}

/// Parsed-model cache keyed by model-text content hash. Parsing a `DefaultModel`
/// is not free; the configured model rarely changes, so cache it rather than
/// reparse per decision. `DefaultModel` is `Clone`.
fn model_cache() -> &'static tokio::sync::Mutex<HashMap<String, DefaultModel>> {
    static CACHE: OnceLock<tokio::sync::Mutex<HashMap<String, DefaultModel>>> = OnceLock::new();
    CACHE.get_or_init(|| tokio::sync::Mutex::new(HashMap::new()))
}

fn model_text_hash(model_text: &str) -> String {
    format!("{:x}", Sha256::digest(model_text.as_bytes()))
}

async fn load_model(model_text: &str) -> Result<DefaultModel, String> {
    let key = model_text_hash(model_text);
    if let Some(model) = model_cache().lock().await.get(&key).cloned() {
        return Ok(model);
    }
    let model = DefaultModel::from_str(model_text)
        .await
        .map_err(|err| format!("model parse: {err}"))?;
    let mut cache = model_cache().lock().await;
    Ok(cache.entry(key).or_insert(model).clone())
}

/// Parse-check the operator-configured Casbin model so a malformed custom model
/// fails at startup instead of denying every request at runtime. Call this from
/// the service startup lifecycle.
pub(crate) async fn validate_casbin_model() -> Result<(), String> {
    let text = cached_casbin_model_text();
    DefaultModel::from_str(&text)
        .await
        .map(|_| ())
        .map_err(|err| format!("invalid UDB authz Casbin model: {err}"))
}

impl AuthzSnapshot {
    /// Authorize `req` through a real Casbin enforcer built from this snapshot.
    /// This is the production decision path for the native `AuthzService`.
    pub(crate) async fn casbin_authorize(&self, req: &AuthzQuery<'_>) -> Decision {
        use tracing::Instrument as _;
        // The Casbin model is operator-configurable but env/file resolution is
        // NOT per-decision (item 17): the text comes from a process-wide cache
        // (env read once; file read once), and parsed models are cached by
        // content hash.
        let model_text = cached_casbin_model_text();
        // Phase 10: a light span at the authz decision boundary. Joins the inbound
        // trace (extracted by `TraceExtractLayer`); the `trace_id` field carries
        // the current trace so the decision is greppable in the trace backend.
        let trace_id = crate::runtime::otel::current_trace_context().trace_id;
        let span = tracing::info_span!("authz.casbin_authorize", trace_id = %trace_id);
        self.casbin_authorize_with_model(&model_text, req)
            .instrument(span)
            .await
    }

    /// Decision core shared by the configurable [`casbin_authorize`] entry point
    /// and by tests that pin an explicit model.
    pub(crate) async fn casbin_authorize_with_model(
        &self,
        model_text: &str,
        req: &AuthzQuery<'_>,
    ) -> Decision {
        let decision_id = self.decision_id(req);

        // Dev-only default-allow applies only when there is genuinely no policy.
        if self.policies.is_empty() {
            let allowed = self.default_allow;
            return Decision {
                decision_id,
                allowed,
                effect: if allowed { Effect::Allow } else { Effect::Deny },
                deny_reason: if allowed {
                    String::new()
                } else {
                    // Actionable hint: the live decision engine reads the PG-warmed
                    // Casbin snapshot sourced from the `udb_authz.policy_rules`
                    // governance table, so configure authorization through the
                    // AuthzService (CreatePolicyRule / PutAuthzPolicy) — or flip the
                    // dev escape hatch for local bootstrap.
                    "no authz policy (default deny); configure authorization via the \
                     AuthzService (policy_rules) or set UDB_ABAC_DEFAULT_ALLOW=true for dev"
                        .to_string()
                },
                policy_version: self.version.clone(),
                relationship_version: self.relationship_version.clone(),
                audit_required: !allowed,
                ..Default::default()
            };
        }

        let principal = req.principal;
        let roles = self.effective_roles(principal);

        // ABAC pre-filter: keep only policies whose purpose / attribute conditions
        // / ReBAC relationship / required-scope gates the request satisfies. Scopes
        // refine an Allow only (a Deny still applies — never fail-open).
        let applicable: Vec<&AuthzPolicy> = self
            .policies
            .iter()
            .filter(|p| {
                p.enabled
                    && wildcard(&p.purpose, req.purpose)
                    && conditions_match(&p.conditions, req.attributes)
                    && (p.relationship.is_empty()
                        || self.has_tuple(principal, &p.relationship, &req.resource.resource_name))
                    && match p.effect {
                        Effect::Allow => p.required_scopes.iter().all(|s| principal.has_scope(s)),
                        Effect::Deny => true,
                    }
            })
            .collect();

        let deny_matches: Vec<&AuthzPolicy> = applicable
            .iter()
            .copied()
            .filter(|p| p.effect == Effect::Deny && self.policy_matches(p, &roles, req))
            .collect();
        if let Some(policy) = deny_matches.first() {
            return Decision {
                decision_id,
                allowed: false,
                effect: Effect::Deny,
                deny_reason: format!("denied by policy {}", policy.id),
                matched_policy_ids: deny_matches.iter().map(|p| p.id.clone()).collect(),
                required_scopes: policy.required_scopes.clone(),
                policy_version: self.version.clone(),
                relationship_version: self.relationship_version.clone(),
                cache_ttl_seconds: 0,
                audit_required: true,
                via_role: false,
            };
        }

        let model = match load_model(model_text).await {
            Ok(model) => model,
            Err(err) => return self.casbin_error(decision_id, &err),
        };
        let enforcer = match cached_enforcer(model, model_text, &applicable).await {
            Ok(enforcer) => enforcer,
            Err(err) => return self.casbin_error(decision_id, &err),
        };

        // The remainder is fully synchronous once the enforcer is in hand; it is
        // shared with any other caller that has already resolved an `Enforcer`
        // (DRY: one enforce/Decision implementation, never duplicated).
        self.enforce_decision(decision_id, &enforcer, &applicable, &roles, req)
    }

    /// Synchronous Casbin enforce + `Decision` construction over a *pre-built*
    /// enforcer. This is the single, shared enforce tail: subject/identity/role
    /// candidate expansion, per-selector enforce, granting-policy resolution and
    /// `Decision` assembly. [`casbin_authorize_with_model`] is its only caller in
    /// the default path; factoring it out keeps the enforce logic in ONE place so
    /// no second authorization path can drift from (or fail open relative to) it.
    ///
    /// Fail-closed: `enforce()` errors collapse to `false` (deny) per candidate;
    /// a request that matches no Allow line denies with the Casbin PERM reason.
    fn enforce_decision(
        &self,
        decision_id: String,
        enforcer: &Enforcer,
        applicable: &[&AuthzPolicy],
        roles: &[String],
        req: &AuthzQuery<'_>,
    ) -> Decision {
        let principal = req.principal;
        let subject = if principal.subject.trim().is_empty() {
            principal.principal_id.clone()
        } else {
            principal.subject.clone()
        };
        // Candidate request subjects (item 16): the cached enforcer is shared
        // across principals, so the per-principal `subject → role` /
        // `subject → identity` grouping links are no longer baked into it.
        // Enforcing each candidate token as `r.sub` is equivalent under the
        // fixed `g = _, _` token contract: the prior loader only ever added
        // one-level links from the subject, and Casbin's role manager resolves
        // `g(a, b)` to `a == b` when no links are loaded.
        let identities = principal.identities();
        let mut request_subjects: Vec<String> = vec![subject.clone()];
        for id in &identities {
            let id = id.trim();
            if !id.is_empty() && !request_subjects.iter().any(|s| s == id) {
                request_subjects.push(id.to_string());
            }
        }
        for role in roles {
            let role = role.trim();
            if !role.is_empty() && !request_subjects.iter().any(|s| s == role) {
                request_subjects.push(role.to_string());
            }
        }

        let dom = slot(&principal.tenant_id);
        // Match AuthzSnapshot::resource_match, which tests the policy resource
        // pattern against EVERY selector (resource_name, message_type, table,
        // resource_type) and matches if any does. Enforce once per non-empty
        // selector and allow if any enforce() succeeds, so the two engines agree
        // (the prior single-`obj` check could deny a request the snapshot engine
        // would allow via a different selector).
        let mut selectors: Vec<String> = [
            req.resource.resource_name.trim(),
            req.resource.message_type.trim(),
            req.resource.table.trim(),
            req.resource.resource_type.trim(),
        ]
        .into_iter()
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
        if selectors.is_empty() {
            selectors.push("*".to_string());
        }
        let allowed = request_subjects.iter().any(|sub| {
            selectors.iter().any(|obj| {
                enforcer
                    .enforce((
                        sub.clone(),
                        dom.clone(),
                        obj.clone(),
                        req.action.to_string(),
                    ))
                    .unwrap_or(false)
            })
        });

        // Resolve the actual granting policy once: the highest-priority
        // applicable Allow whose full selector set matches the request. Both
        // required_scopes and the ROLE_POLICY audit classification derive from
        // it (not an arbitrary `applicable.first()`).
        let granting: Option<&AuthzPolicy> = if allowed {
            let mut grantors: Vec<&AuthzPolicy> = applicable
                .iter()
                .copied()
                .filter(|p| p.effect == Effect::Allow && self.policy_matches(p, roles, req))
                .collect();
            grantors.sort_by_key(|p| std::cmp::Reverse(p.priority));
            grantors.first().copied()
        } else {
            None
        };

        Decision {
            decision_id,
            allowed,
            effect: if allowed { Effect::Allow } else { Effect::Deny },
            deny_reason: if allowed {
                String::new()
            } else {
                "denied by Casbin PERM model".to_string()
            },
            // Candidate policy set Casbin evaluated (post ABAC pre-filter).
            matched_policy_ids: applicable.iter().map(|p| p.id.clone()).collect(),
            required_scopes: granting
                .map(|p| p.required_scopes.clone())
                .unwrap_or_default(),
            policy_version: self.version.clone(),
            relationship_version: self.relationship_version.clone(),
            cache_ttl_seconds: 0,
            audit_required: !allowed,
            via_role: granting.map(|p| !p.role.trim().is_empty()).unwrap_or(false),
        }
    }

    /// Fail closed on a Casbin engine error.
    fn casbin_error(&self, decision_id: String, reason: &str) -> Decision {
        Decision {
            decision_id,
            allowed: false,
            effect: Effect::Deny,
            deny_reason: format!("casbin engine error: {reason}"),
            policy_version: self.version.clone(),
            audit_required: true,
            ..Default::default()
        }
    }
}

/// Upper bound on cached enforcers (item 16). Eviction past the cap removes
/// ONLY the least-recently-used entry — never the whole cache — so a burst of
/// distinct policy sets cannot evict every hot enforcer at once.
const ENFORCER_CACHE_CAP: usize = 256;

/// Minimal bounded LRU. `tick` is a monotonic use counter: lookups and inserts
/// stamp the entry, and a capacity-exceeding insert evicts the smallest stamp.
/// Eviction is O(cap) but cap is small (256) and inserts are cache misses only.
/// Generic over the value so the eviction logic is unit-testable without
/// constructing real `Enforcer`s.
struct LruCache<V> {
    map: HashMap<String, (V, u64)>,
    tick: u64,
    cap: usize,
}

impl<V: Clone> LruCache<V> {
    fn new(cap: usize) -> Self {
        Self {
            map: HashMap::new(),
            tick: 0,
            cap: cap.max(1),
        }
    }

    fn get(&mut self, key: &str) -> Option<V> {
        self.tick += 1;
        let tick = self.tick;
        self.map.get_mut(key).map(|entry| {
            entry.1 = tick;
            entry.0.clone()
        })
    }

    fn insert(&mut self, key: String, value: V) {
        self.tick += 1;
        let tick = self.tick;
        if !self.map.contains_key(&key) && self.map.len() >= self.cap {
            if let Some(oldest) = self
                .map
                .iter()
                .min_by_key(|(_, (_, used))| *used)
                .map(|(k, _)| k.clone())
            {
                self.map.remove(&oldest);
            }
        }
        let entry = self.map.entry(key).or_insert((value.clone(), tick));
        *entry = (value, tick);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.len()
    }
}

fn enforcer_cache() -> &'static tokio::sync::Mutex<LruCache<Arc<Enforcer>>> {
    static CACHE: OnceLock<tokio::sync::Mutex<LruCache<Arc<Enforcer>>>> = OnceLock::new();
    CACHE.get_or_init(|| tokio::sync::Mutex::new(LruCache::new(ENFORCER_CACHE_CAP)))
}

/// Test-only visibility into cache effectiveness: monotonic hit counter, so a
/// test can assert a delta without being perturbed by concurrent tests (they
/// only ever increase it).
#[cfg(test)]
static ENFORCER_CACHE_HITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

async fn cached_enforcer(
    model: DefaultModel,
    model_text: &str,
    applicable: &[&AuthzPolicy],
) -> Result<Arc<Enforcer>, String> {
    // Key = (model hash, Allow-policy set) ONLY (item 16): no principal
    // subject/role/identity input, so one enforcer serves every principal
    // whose ABAC pre-filter yields the same applicable set. The per-principal
    // grouping is supplied at enforce time as candidate `r.sub` tokens. The
    // model hash keeps a model swap from reusing a stale enforcer.
    let key = casbin_policy_set_hash(model_text, applicable);
    if let Some(enforcer) = enforcer_cache().lock().await.get(&key) {
        #[cfg(test)]
        ENFORCER_CACHE_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return Ok(enforcer);
    }

    let mut enforcer = Enforcer::new(model, MemoryAdapter::default())
        .await
        .map_err(|err| format!("enforcer init: {err}"))?;
    for p in applicable.iter().filter(|p| p.effect == Effect::Allow) {
        let sub = if !p.role.trim().is_empty() {
            p.role.clone()
        } else {
            slot(&p.subject)
        };
        let rule = vec![
            sub,
            slot(&p.tenant),
            slot(&p.resource),
            slot(&p.action),
            p.effect.as_str().to_string(),
        ];
        enforcer
            .add_policy(rule)
            .await
            .map_err(|err| format!("policy load: {err}"))?;
    }

    let enforcer = Arc::new(enforcer);
    let mut cache = enforcer_cache().lock().await;
    // A racing builder may have inserted while we built: serve theirs so all
    // callers share one instance.
    if let Some(existing) = cache.get(&key) {
        return Ok(existing);
    }
    cache.insert(key, enforcer.clone());
    Ok(enforcer)
}

fn casbin_policy_set_hash(model_text: &str, applicable: &[&AuthzPolicy]) -> String {
    let mut parts = vec![format!("m|{}", model_text_hash(model_text))];
    for p in applicable.iter().filter(|p| p.effect == Effect::Allow) {
        parts.push(format!(
            "p|{}|{}|{}|{}|{}|{}",
            p.id,
            p.priority,
            if p.role.trim().is_empty() {
                slot(&p.subject)
            } else {
                p.role.clone()
            },
            slot(&p.tenant),
            slot(&p.resource),
            slot(&p.action)
        ));
    }
    parts.sort();

    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(b"\n");
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::authz::{Principal, ResourceRef, RoleBinding};
    use std::collections::BTreeMap;

    fn query<'a>(
        principal: &'a Principal,
        resource: &'a ResourceRef,
        action: &'a str,
        attrs: &'a BTreeMap<String, String>,
    ) -> AuthzQuery<'a> {
        AuthzQuery {
            principal,
            resource,
            action,
            purpose: "",
            attributes: attrs,
        }
    }

    #[tokio::test]
    async fn casbin_rbac_role_grants_via_binding_and_default_denies() {
        let mut snap = AuthzSnapshot::default();
        snap.version = "v1".to_string();
        // A reader role may select invoices in tenant acme.
        snap.policies.push(AuthzPolicy {
            id: "p1".to_string(),
            effect: Effect::Allow,
            tenant: "acme".to_string(),
            role: "reader".to_string(),
            action: "data.select".to_string(),
            resource: "invoice".to_string(),
            ..Default::default()
        });
        snap.role_bindings.push(RoleBinding {
            subject: "alice".to_string(),
            role: "reader".to_string(),
            tenant: "acme".to_string(),
            project: String::new(),
        });
        let attrs = BTreeMap::new();
        let resource = ResourceRef::message("invoice");

        let alice = Principal {
            subject: "alice".to_string(),
            tenant_id: "acme".to_string(),
            ..Default::default()
        };
        let allow = snap
            .casbin_authorize(&query(&alice, &resource, "data.select", &attrs))
            .await;
        assert!(
            allow.allowed,
            "role-bound principal must be allowed by Casbin"
        );

        // A different action is denied (default deny / deny-override).
        let deny = snap
            .casbin_authorize(&query(&alice, &resource, "data.delete", &attrs))
            .await;
        assert!(!deny.allowed);

        // An unbound principal is denied.
        let bob = Principal {
            subject: "bob".to_string(),
            tenant_id: "acme".to_string(),
            ..Default::default()
        };
        let bob_deny = snap
            .casbin_authorize(&query(&bob, &resource, "data.select", &attrs))
            .await;
        assert!(!bob_deny.allowed);
    }

    #[tokio::test]
    async fn casbin_explicit_deny_overrides_allow() {
        let mut snap = AuthzSnapshot::default();
        snap.version = "v1".to_string();
        snap.policies.push(AuthzPolicy {
            id: "allow".to_string(),
            effect: Effect::Allow,
            subject: "*".to_string(),
            action: "*".to_string(),
            resource: "*".to_string(),
            ..Default::default()
        });
        snap.policies.push(AuthzPolicy {
            id: "deny".to_string(),
            effect: Effect::Deny,
            subject: "carol".to_string(),
            action: "data.delete".to_string(),
            resource: "invoice".to_string(),
            ..Default::default()
        });
        let attrs = BTreeMap::new();
        let resource = ResourceRef::message("invoice");
        let carol = Principal {
            subject: "carol".to_string(),
            ..Default::default()
        };
        let decision = snap
            .casbin_authorize(&query(&carol, &resource, "data.delete", &attrs))
            .await;
        assert!(
            !decision.allowed,
            "explicit deny must override the broad allow"
        );
    }

    #[tokio::test]
    async fn default_casbin_model_validates() {
        validate_casbin_model()
            .await
            .expect("embedded default model must parse");
    }

    #[tokio::test]
    async fn operator_supplied_model_overrides_default_matcher() {
        let mut snap = AuthzSnapshot::default();
        snap.version = "v1".to_string();
        // An Allow policy scoped to a specific subject; the default model denies
        // any other subject (no role link, subject mismatch).
        snap.policies.push(AuthzPolicy {
            id: "p1".to_string(),
            effect: Effect::Allow,
            subject: "specific-user".to_string(),
            tenant: "acme".to_string(),
            action: "data.select".to_string(),
            resource: "invoice".to_string(),
            ..Default::default()
        });
        let attrs = BTreeMap::new();
        let resource = ResourceRef::message("invoice");
        let stranger = Principal {
            subject: "stranger".to_string(),
            tenant_id: "acme".to_string(),
            ..Default::default()
        };

        // Baseline: the embedded default model denies the subject mismatch.
        let default_decision = snap
            .casbin_authorize(&query(&stranger, &resource, "data.select", &attrs))
            .await;
        assert!(
            !default_decision.allowed,
            "default PERM model must deny a subject/role mismatch"
        );

        // An operator-supplied model whose matcher ignores subject/obj/act must
        // change the outcome — proving the model is honored, not hardcoded.
        let permissive = r#"[request_definition]
r = sub, dom, obj, act

[policy_definition]
p = sub, dom, obj, act, eft

[role_definition]
g = _, _

[policy_effect]
e = some(where (p_eft == allow)) && !some(where (p_eft == deny))

[matchers]
m = (p.dom == "*" || r.dom == p.dom)
"#;
        let custom_decision = snap
            .casbin_authorize_with_model(
                permissive,
                &query(&stranger, &resource, "data.select", &attrs),
            )
            .await;
        assert!(
            custom_decision.allowed,
            "operator-supplied model must override the default matcher"
        );
    }

    /// Item 16: the enforcer cache key excludes the principal, so >256 distinct
    /// principals under one policy set share ONE cached enforcer (cache hits),
    /// instead of 256+ per-principal entries triggering a clear-all.
    #[tokio::test]
    async fn enforcer_cache_shared_across_many_principals() {
        let mut snap = AuthzSnapshot::default();
        snap.version = "v1".to_string();
        snap.policies.push(AuthzPolicy {
            id: "shared".to_string(),
            effect: Effect::Allow,
            tenant: "acme".to_string(),
            role: "reader".to_string(),
            action: "data.select".to_string(),
            resource: "invoice".to_string(),
            ..Default::default()
        });
        let attrs = BTreeMap::new();
        let resource = ResourceRef::message("invoice");

        // The cache key is principal-free: identical for any two principals
        // with the same applicable policy set.
        let key = casbin_policy_set_hash(CASBIN_MODEL, &[&snap.policies[0]]);
        assert_eq!(
            key,
            casbin_policy_set_hash(CASBIN_MODEL, &[&snap.policies[0]]),
            "policy-set hash must be deterministic and principal-free"
        );

        let hits_before = ENFORCER_CACHE_HITS.load(std::sync::atomic::Ordering::Relaxed);
        for i in 0..300 {
            let principal = Principal {
                subject: format!("user-{i}"),
                tenant_id: "acme".to_string(),
                roles: vec!["reader".to_string()],
                ..Default::default()
            };
            let decision = snap
                .casbin_authorize(&query(&principal, &resource, "data.select", &attrs))
                .await;
            assert!(decision.allowed, "principal user-{i} must be allowed");
        }
        let hits_after = ENFORCER_CACHE_HITS.load(std::sync::atomic::Ordering::Relaxed);
        // 300 distinct principals share one enforcer: at most the first call
        // builds it, every later call is a hit. (The counter is monotonic, so
        // concurrent tests can only increase the delta.)
        assert!(
            hits_after - hits_before >= 299,
            "expected >=299 enforcer cache hits for 300 principals, got {}",
            hits_after - hits_before
        );
    }

    /// Item 16: capacity overflow evicts ONLY the least-recently-used entry —
    /// recently used keys survive; no code path clears the whole cache.
    #[test]
    fn lru_eviction_is_bounded_and_oldest_only() {
        let mut lru: LruCache<u32> = LruCache::new(4);
        for i in 0..4u32 {
            lru.insert(format!("k{i}"), i);
        }
        // Touch k0 so k1 becomes the least-recently-used entry.
        assert_eq!(lru.get("k0"), Some(0));
        lru.insert("k4".to_string(), 4);
        assert_eq!(lru.len(), 4, "insert past cap must stay bounded");
        assert!(lru.get("k1").is_none(), "only the LRU entry is evicted");
        for key in ["k0", "k2", "k3", "k4"] {
            assert!(lru.get(key).is_some(), "{key} must survive eviction");
        }

        // Many distinct keys never clear the cache wholesale: the most recent
        // `cap` keys are always present.
        let mut lru: LruCache<u32> = LruCache::new(256);
        for i in 0..300u32 {
            lru.insert(format!("p{i}"), i);
        }
        assert_eq!(lru.len(), 256);
        for i in 44..300u32 {
            assert!(
                lru.get(&format!("p{i}")).is_some(),
                "recent key p{i} must not be dropped by older inserts"
            );
        }
    }

    /// Item 17: the steady-state decision path serves the SAME cached model
    /// text allocation — no per-decision env read or file re-read.
    #[tokio::test]
    async fn model_text_resolved_once_per_process() {
        let first = cached_casbin_model_text();
        let second = cached_casbin_model_text();
        assert!(
            Arc::ptr_eq(&first, &second),
            "cached model text must be the same allocation across decisions"
        );
    }
}
