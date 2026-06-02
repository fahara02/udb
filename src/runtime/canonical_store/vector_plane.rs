//! Common canonical-system-state plane for vector/search backends.
//!
//! This module is the single place where vector backends are evaluated for the
//! primitives needed by the `SystemStores` contract. Backend-specific adapters
//! must satisfy this plane before they can register in
//! `CANONICAL_SYSTEM_STORE_BACKENDS`.

use crate::backend::BackendKind;

/// Primitive requirements every vector canonical adapter must satisfy.
pub const VECTOR_PLANE_REQUIRED_PRIMITIVES: &[&str] = &[
    "atomic lease/task claim with one observable winner",
    "durable monotonic outbox sequence across writers",
    "tenant/project-scoped system-state namespace",
    "read fence token that later reads can wait on",
    "bounded scan/listing for projection, saga, audit, and migration recovery",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorPlaneBackend {
    Qdrant,
    Pinecone,
    Weaviate,
    Elasticsearch,
}

impl VectorPlaneBackend {
    pub fn from_kind(kind: &BackendKind) -> Option<Self> {
        match kind {
            BackendKind::Qdrant => Some(Self::Qdrant),
            BackendKind::Pinecone => Some(Self::Pinecone),
            BackendKind::Weaviate => Some(Self::Weaviate),
            BackendKind::Elasticsearch => Some(Self::Elasticsearch),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Qdrant => "qdrant",
            Self::Pinecone => "pinecone",
            Self::Weaviate => "weaviate",
            Self::Elasticsearch => "elasticsearch",
        }
    }
}

/// Native primitive matrix for one vector backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorPlaneProfile {
    pub backend: VectorPlaneBackend,
    pub record_namespace_strategy: &'static str,
    pub insert_only_or_create_only: bool,
    pub filtered_update: bool,
    pub strong_write_ordering: bool,
    pub single_record_optimistic_concurrency: bool,
    pub atomic_claim_winner: bool,
    pub monotonic_outbox_sequence: bool,
    pub read_fence_token: bool,
    pub bounded_recovery_scan: bool,
    pub blocking_gaps: &'static [&'static str],
}

impl VectorPlaneProfile {
    /// A vector backend may register a canonical `SystemStores` implementation
    /// only when every required primitive is proven.
    pub fn can_register_system_stores(&self) -> bool {
        self.atomic_claim_winner
            && self.monotonic_outbox_sequence
            && self.read_fence_token
            && self.bounded_recovery_scan
            && self.blocking_gaps.is_empty()
    }
}

pub fn profile_for(kind: &BackendKind) -> Option<VectorPlaneProfile> {
    let backend = VectorPlaneBackend::from_kind(kind)?;
    let profile = match backend {
        VectorPlaneBackend::Qdrant => VectorPlaneProfile {
            backend,
            record_namespace_strategy: "dedicated UDB system collections with deterministic point IDs and tenant/project payload fields",
            insert_only_or_create_only: true,
            filtered_update: true,
            strong_write_ordering: true,
            single_record_optimistic_concurrency: cfg!(feature = "qdrant"),
            atomic_claim_winner: cfg!(feature = "qdrant"),
            monotonic_outbox_sequence: cfg!(feature = "qdrant"),
            read_fence_token: cfg!(feature = "qdrant"),
            bounded_recovery_scan: true,
            blocking_gaps: if cfg!(feature = "qdrant") {
                &[]
            } else {
                &[
                    "qdrant feature not compiled, so the native SystemStores adapter is absent",
                    "no live conformance proof can run without the compiled adapter",
                ]
            },
        },
        VectorPlaneBackend::Pinecone => VectorPlaneProfile {
            backend,
            record_namespace_strategy: "dedicated namespace plus metadata fields for tenant/project/system record type",
            insert_only_or_create_only: false,
            filtered_update: true,
            strong_write_ordering: false,
            single_record_optimistic_concurrency: cfg!(feature = "pinecone"),
            atomic_claim_winner: cfg!(feature = "pinecone"),
            monotonic_outbox_sequence: cfg!(feature = "pinecone"),
            read_fence_token: cfg!(feature = "pinecone"),
            bounded_recovery_scan: true,
            blocking_gaps: if cfg!(feature = "pinecone") {
                &[]
            } else {
                &[
                    "pinecone feature not compiled, so the shared vector SystemStores adapter is absent",
                ]
            },
        },
        VectorPlaneBackend::Weaviate => VectorPlaneProfile {
            backend,
            record_namespace_strategy: "dedicated class/collection with multi-tenancy and tenant/project properties",
            insert_only_or_create_only: false,
            filtered_update: true,
            strong_write_ordering: false,
            single_record_optimistic_concurrency: cfg!(feature = "weaviate"),
            atomic_claim_winner: cfg!(feature = "weaviate"),
            monotonic_outbox_sequence: cfg!(feature = "weaviate"),
            read_fence_token: cfg!(feature = "weaviate"),
            bounded_recovery_scan: true,
            blocking_gaps: if cfg!(feature = "weaviate") {
                &[]
            } else {
                &[
                    "weaviate feature not compiled, so the shared vector SystemStores adapter is absent",
                ]
            },
        },
        VectorPlaneBackend::Elasticsearch => VectorPlaneProfile {
            backend,
            record_namespace_strategy: "dedicated system index/alias with tenant/project fields",
            insert_only_or_create_only: true,
            filtered_update: true,
            strong_write_ordering: false,
            single_record_optimistic_concurrency: true,
            atomic_claim_winner: cfg!(feature = "elasticsearch"),
            monotonic_outbox_sequence: cfg!(feature = "elasticsearch"),
            read_fence_token: cfg!(feature = "elasticsearch"),
            bounded_recovery_scan: true,
            blocking_gaps: if cfg!(feature = "elasticsearch") {
                &[]
            } else {
                &[
                    "elasticsearch feature not compiled, so the shared vector SystemStores adapter is absent",
                ]
            },
        },
    };
    Some(profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_plane_profiles_cover_every_vector_backend() {
        for kind in [
            BackendKind::Qdrant,
            BackendKind::Pinecone,
            BackendKind::Weaviate,
            BackendKind::Elasticsearch,
        ] {
            let profile = profile_for(&kind).expect("vector backend profile");
            assert_eq!(profile.backend.as_str(), kind.as_str());
            assert!(!profile.record_namespace_strategy.is_empty());
            if profile.can_register_system_stores() {
                assert!(profile.blocking_gaps.is_empty());
            } else {
                assert!(!profile.blocking_gaps.is_empty());
            }
        }
    }

    #[test]
    fn qdrant_profile_tracks_compiled_adapter() {
        let profile = profile_for(&BackendKind::Qdrant).unwrap();
        assert!(profile.insert_only_or_create_only);
        assert!(profile.strong_write_ordering);
        if cfg!(feature = "qdrant") {
            assert!(profile.atomic_claim_winner);
            assert!(profile.can_register_system_stores());
        } else {
            assert!(!profile.atomic_claim_winner);
            assert!(!profile.can_register_system_stores());
        }
    }
}
