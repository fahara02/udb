//! Proto-derived native-service → persistence-store binding (extend_udb.md).
//!
//! The persistence backend a native control-plane service uses is NOT hand-picked in
//! Rust and NOT pinned by an env var by default — it is DERIVED from the service's
//! `(udb.native_service)` proto annotation (`requires_postgres` &c., see
//! `proto/udb/core/common/v1/security.proto`), read off the embedded descriptor
//! exactly like native-service ids, method security, and DDL generation are. Proto is
//! the single source of truth: annotating a native service wires its store
//! automatically, with no Rust id/backend list to maintain (directive #1).
//!
//! P0 resolves to `postgres` — the canonical system store every native contract
//! currently declares via `requires_postgres`. When P2 introduces non-postgres
//! canonical stores, the proto annotation carries the authority and this derivation
//! follows it with no wiring change. `UDB_NATIVE_STORE` remains an explicit operator
//! OVERRIDE of this proto-derived default (see `core::native_store`).

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::runtime::descriptor_manifest::descriptor_contract_manifest_static;
use crate::runtime::service::native_registry::canonical_service_id;

/// Canonical store-backend label native control-plane services persist their system
/// tables to, derived ONCE from the descriptor's `native_service` contracts. Returns
/// `"postgres"` while any native contract declares `requires_postgres` (today: all of
/// them); returns `""` only if no native contract declares a canonical SQL store, in
/// which case the caller falls back to its own default. Never a hardcoded backend
/// choice — the value is read from proto.
pub(crate) fn proto_native_store_backend() -> &'static str {
    static BACKEND: OnceLock<&'static str> = OnceLock::new();
    *BACKEND.get_or_init(|| {
        let manifest = descriptor_contract_manifest_static();
        let requires_postgres = manifest
            .services
            .iter()
            .filter_map(|service| service.native_service.as_ref())
            .any(|native| native.requires_postgres);
        if requires_postgres { "postgres" } else { "" }
    })
}

/// Per-service proto-derived store-backend map: canonical service id → backend label,
/// built ONCE from each `native_service` contract (`requires_postgres`). This is the
/// authority for "which backend does service X persist to" — read from the descriptor,
/// never a hand-written id/backend list (directive #1). P1 binding seam: when a service
/// is annotated for a non-postgres canonical store, its entry follows automatically.
fn native_store_backend_map() -> &'static HashMap<String, &'static str> {
    static MAP: OnceLock<HashMap<String, &'static str>> = OnceLock::new();
    MAP.get_or_init(|| {
        let manifest = descriptor_contract_manifest_static();
        let mut map = HashMap::new();
        for service in &manifest.services {
            if let Some(native) = service.native_service.as_ref() {
                if native.requires_postgres {
                    map.insert(canonical_service_id(&native.service_id), "postgres");
                }
            }
        }
        map
    })
}

/// The proto-declared persistence backend for a specific native service (service id in
/// any `.`/`-`/`_` spelling), or `None` if the service declares no canonical SQL store.
pub(crate) fn native_service_store_backend(service_id: &str) -> Option<&'static str> {
    native_store_backend_map()
        .get(&canonical_service_id(service_id))
        .copied()
}

#[cfg(test)]
mod tests {
    use super::{native_service_store_backend, proto_native_store_backend};

    /// The native-store backend must be derivable from the embedded descriptor (the
    /// served path), not a Rust constant: every native control-plane contract
    /// declares `requires_postgres`, so the proto-derived binding is `postgres`. If
    /// the contracts ever stopped requiring postgres, this would surface it.
    #[test]
    fn native_store_backend_is_derived_from_proto_contracts() {
        assert_eq!(proto_native_store_backend(), "postgres");
    }

    /// Per-service binding is descriptor-derived for every native control-plane
    /// service (all declare `requires_postgres`), and `None` for an unknown id.
    #[test]
    fn per_service_store_backend_is_derived_from_proto() {
        for id in [
            "storage",
            "notification",
            "tenant",
            "analytics",
            "asset",
            "authn",
            "authz",
            "control",
            "idp",
            "webrtc.room",
        ] {
            assert_eq!(
                native_service_store_backend(id),
                Some("postgres"),
                "service {id} must derive its store from proto"
            );
        }
        assert_eq!(native_service_store_backend("does-not-exist"), None);
    }
}
