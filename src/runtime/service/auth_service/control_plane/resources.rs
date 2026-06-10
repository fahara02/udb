//! Pure resource model + dependency-ordering + content-hash versioning for the
//! Phase 9 control-plane distribution core. No I/O — every fn here is
//! deterministic and unit-tested.

use crate::proto::udb::core::control::entity::v1 as control_entity_pb;
use control_entity_pb::ResourceType;

/// All distributable resource types, in the aggregated push order.
///
/// "Definition" resources (backend targets) and the standalone control resources
/// come BEFORE the policies that reference them, so a referencing policy is
/// never sent — and therefore never applied — before its target definition
/// exists on the node (make-before-break). Concretely:
///   1. BACKEND_TARGET_DEFINITION   (targets the routing/RLS policies reference)
///   2. NATIVE_SERVICE_ENABLEMENT   (which services exist before policies bind)
///   3. METHOD_SECURITY_POLICY      (per-RPC security; self-contained)
///   4. ROUTING_POLICY              (references backend targets)
///   5. RLS_TENANT_POLICY           (references backend targets + tenants)
///
/// Acceptance: "Referencing policies are never applied before their target
/// definitions" — enforced by sending definitions earlier in this slice and by
/// the stream pushing per-type in exactly this order.
pub const ORDERED_RESOURCE_TYPES: &[ResourceType] = &[
    ResourceType::BackendTargetDefinition,
    ResourceType::NativeServiceEnablement,
    ResourceType::MethodSecurityPolicy,
    ResourceType::RoutingPolicy,
    ResourceType::RlsTenantPolicy,
];

/// The aggregated push order (definitions before referencing policies).
pub fn ordered_resource_types() -> &'static [ResourceType] {
    ORDERED_RESOURCE_TYPES
}

/// Canonical DB string for a resource type (stored verbatim in the
/// `resource_type` column, mirroring the prost enum value name).
pub fn resource_type_to_db(rt: ResourceType) -> &'static str {
    match rt {
        ResourceType::Unspecified => "RESOURCE_TYPE_UNSPECIFIED",
        ResourceType::RoutingPolicy => "RESOURCE_TYPE_ROUTING_POLICY",
        ResourceType::MethodSecurityPolicy => "RESOURCE_TYPE_METHOD_SECURITY_POLICY",
        ResourceType::RlsTenantPolicy => "RESOURCE_TYPE_RLS_TENANT_POLICY",
        ResourceType::NativeServiceEnablement => "RESOURCE_TYPE_NATIVE_SERVICE_ENABLEMENT",
        ResourceType::BackendTargetDefinition => "RESOURCE_TYPE_BACKEND_TARGET_DEFINITION",
    }
}

/// Parse a DB string (or proto enum name) back into a [`ResourceType`].
pub fn resource_type_from_db(value: &str) -> ResourceType {
    match value {
        "RESOURCE_TYPE_ROUTING_POLICY" | "ROUTING_POLICY" => ResourceType::RoutingPolicy,
        "RESOURCE_TYPE_METHOD_SECURITY_POLICY" | "METHOD_SECURITY_POLICY" => {
            ResourceType::MethodSecurityPolicy
        }
        "RESOURCE_TYPE_RLS_TENANT_POLICY" | "RLS_TENANT_POLICY" => ResourceType::RlsTenantPolicy,
        "RESOURCE_TYPE_NATIVE_SERVICE_ENABLEMENT" | "NATIVE_SERVICE_ENABLEMENT" => {
            ResourceType::NativeServiceEnablement
        }
        "RESOURCE_TYPE_BACKEND_TARGET_DEFINITION" | "BACKEND_TARGET_DEFINITION" => {
            ResourceType::BackendTargetDefinition
        }
        _ => ResourceType::Unspecified,
    }
}

/// Resolve a proto enum i32 into a [`ResourceType`], defaulting to UNSPECIFIED.
pub fn resource_type_from_i32(value: i32) -> ResourceType {
    ResourceType::try_from(value).unwrap_or(ResourceType::Unspecified)
}

/// One control-plane resource in its in-memory form (a row projection).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceModel {
    pub resource_id: String,
    pub resource_type: String,
    pub name: String,
    /// Empty string == fleet-wide (NULL tenant in the DB).
    pub tenant_id: String,
    pub project_id: String,
    pub version: String,
    pub content_hash: String,
    pub payload_json: String,
    pub updated_by: String,
    pub updated_at_unix: i64,
}

impl ResourceModel {
    pub fn resource_type_enum(&self) -> ResourceType {
        resource_type_from_db(&self.resource_type)
    }
}

/// Content-addressed version for a single resource payload.
///
/// Deterministic and order-independent at the JSON-object level: the payload is
/// re-serialized through a canonical form (object keys sorted recursively) so
/// two payloads that differ only in key order hash identically, while any value
/// change flips the hash. Mirrors the determinism contract of
/// `authz::mod::policy_content_version` (length-prefixed, stable hasher).
pub fn content_version(payload_json: &str) -> String {
    use std::hash::{Hash, Hasher};
    let canonical = canonical_json(payload_json);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    canonical.len().hash(&mut hasher);
    canonical.hash(&mut hasher);
    format!("cp-{:016x}", hasher.finish())
}

/// Aggregate "version-of-the-world" for an ordered set of resources of one type.
///
/// Order-independent: each resource contributes `name\u{1f}content_hash`, the
/// contributions are sorted, then length-prefixed and hashed. Re-ordering the
/// input does not change the result; adding/removing/changing any resource does.
pub fn aggregate_version(resources: &[ResourceModel]) -> String {
    use std::hash::{Hash, Hasher};
    if resources.is_empty() {
        return "cp-empty".to_string();
    }
    let mut entries: Vec<String> = resources
        .iter()
        .map(|r| {
            let hash = if r.content_hash.is_empty() {
                content_version(&r.payload_json)
            } else {
                r.content_hash.clone()
            };
            format!("{}\u{1f}{}", r.name, hash)
        })
        .collect();
    entries.sort();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    entries.len().hash(&mut hasher);
    for entry in &entries {
        entry.hash(&mut hasher);
    }
    format!("cp-world-{}-{:016x}", entries.len(), hasher.finish())
}

/// Canonicalize a JSON document so semantically-equal payloads serialize
/// identically (recursively sorted object keys). Invalid JSON falls back to the
/// trimmed raw string so non-JSON payloads still hash deterministically.
fn canonical_json(raw: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(raw.trim()) {
        Ok(value) => canonicalize(&value).to_string(),
        Err(_) => raw.trim().to_string(),
    }
}

fn canonicalize(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut sorted = serde_json::Map::new();
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for key in keys {
                sorted.insert(key.clone(), canonicalize(&map[key]));
            }
            serde_json::Value::Object(sorted)
        }
        serde_json::Value::Array(items) => {
            // Array ORDER is significant for a resource payload, so it is
            // preserved; only object keys are normalized.
            serde_json::Value::Array(items.iter().map(canonicalize).collect())
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model(name: &str, rt: ResourceType, payload: &str) -> ResourceModel {
        ResourceModel {
            name: name.to_string(),
            resource_type: resource_type_to_db(rt).to_string(),
            content_hash: content_version(payload),
            payload_json: payload.to_string(),
            ..Default::default()
        }
    }

    // Acceptance: definitions are ordered before the policies that reference them.
    #[test]
    fn definitions_precede_referencing_policies() {
        let order = ordered_resource_types();
        let idx = |rt: ResourceType| order.iter().position(|t| *t == rt).unwrap();
        // Backend targets are the targets routing + RLS reference.
        assert!(idx(ResourceType::BackendTargetDefinition) < idx(ResourceType::RoutingPolicy));
        assert!(idx(ResourceType::BackendTargetDefinition) < idx(ResourceType::RlsTenantPolicy));
        // Service enablement precedes the policies that bind to those services.
        assert!(idx(ResourceType::NativeServiceEnablement) < idx(ResourceType::RoutingPolicy));
        assert!(idx(ResourceType::NativeServiceEnablement) < idx(ResourceType::RlsTenantPolicy));
    }

    #[test]
    fn ordered_types_are_complete_and_unique() {
        let order = ordered_resource_types();
        assert_eq!(
            order.len(),
            5,
            "all five real resource types are distributed"
        );
        let mut seen = std::collections::BTreeSet::new();
        for rt in order {
            assert!(
                seen.insert(*rt),
                "no duplicate resource type in the push order"
            );
            assert_ne!(
                *rt,
                ResourceType::Unspecified,
                "UNSPECIFIED is not distributed"
            );
        }
    }

    // Content-hash is stable under key reorder and changes on an edit.
    #[test]
    fn content_version_stable_under_reorder_and_changes_on_edit() {
        let a = content_version(r#"{"cluster":"pg-primary","weight":10}"#);
        let b = content_version(r#"{"weight":10,"cluster":"pg-primary"}"#);
        assert_eq!(a, b, "key order must not change the content version");
        // Idempotent.
        assert_eq!(
            a,
            content_version(r#"{"cluster":"pg-primary","weight":10}"#)
        );
        // Any value change flips it.
        let c = content_version(r#"{"cluster":"pg-primary","weight":11}"#);
        assert_ne!(a, c, "a value edit must change the content version");
    }

    #[test]
    fn content_version_handles_nested_objects() {
        let a = content_version(r#"{"a":{"x":1,"y":2},"b":[1,2,3]}"#);
        let b = content_version(r#"{"b":[1,2,3],"a":{"y":2,"x":1}}"#);
        assert_eq!(a, b, "nested key reorder must hash identically");
        // Array element order IS significant.
        let c = content_version(r#"{"a":{"x":1,"y":2},"b":[3,2,1]}"#);
        assert_ne!(a, c, "array order is significant");
    }

    #[test]
    fn aggregate_version_is_order_independent_and_change_sensitive() {
        let r1 = model(
            "pg-primary",
            ResourceType::BackendTargetDefinition,
            r#"{"host":"a"}"#,
        );
        let r2 = model(
            "pg-replica",
            ResourceType::BackendTargetDefinition,
            r#"{"host":"b"}"#,
        );
        let forward = aggregate_version(&[r1.clone(), r2.clone()]);
        let reversed = aggregate_version(&[r2.clone(), r1.clone()]);
        assert_eq!(
            forward, reversed,
            "world version must not depend on row order"
        );
        // Editing one resource changes the world version.
        let r2b = model(
            "pg-replica",
            ResourceType::BackendTargetDefinition,
            r#"{"host":"c"}"#,
        );
        let changed = aggregate_version(&[r1, r2b]);
        assert_ne!(
            forward, changed,
            "an edited resource must bump the world version"
        );
        // Empty set has a stable sentinel.
        assert_eq!(aggregate_version(&[]), "cp-empty");
    }

    #[test]
    fn db_roundtrip_for_every_type() {
        for rt in [
            ResourceType::RoutingPolicy,
            ResourceType::MethodSecurityPolicy,
            ResourceType::RlsTenantPolicy,
            ResourceType::NativeServiceEnablement,
            ResourceType::BackendTargetDefinition,
        ] {
            assert_eq!(resource_type_from_db(resource_type_to_db(rt)), rt);
        }
        assert_eq!(
            resource_type_from_db("RESOURCE_TYPE_UNSPECIFIED"),
            ResourceType::Unspecified
        );
        // Bare (un-prefixed) names also parse.
        assert_eq!(
            resource_type_from_db("ROUTING_POLICY"),
            ResourceType::RoutingPolicy
        );
    }
}
