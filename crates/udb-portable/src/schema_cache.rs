//! U13 step 2 — SDK-side schema cache.
//!
//! The server side of U13 added `LookupMessageSchema` /
//! `ListMessageSchemas` RPCs plus `x-udb-catalog-version` and
//! `x-udb-manifest-checksum` headers on every response. The pending
//! follow-up was *"generated SDKs should consume the schema metadata
//! and expose descriptor caches."* This module is that consumer: it
//! belongs in [`udb_portable`](crate) so any SDK (Rust, JS via WASM,
//! Go via FFI, etc.) can re-use the same negotiation logic.
//!
//! ## What an SDK does with this cache
//!
//! 1. On startup, the SDK has no idea what catalog version the server
//!    is running. On the first response it sees
//!    `x-udb-catalog-version` and `x-udb-manifest-checksum`; it calls
//!    [`SchemaCache::observe`] to record them.
//! 2. On every subsequent response, it calls [`SchemaCache::observe`]
//!    again. If the values changed, `observe` returns
//!    [`Negotiation::CatalogRolled`] and the SDK invalidates its
//!    descriptor cache + re-fetches via `LookupMessageSchema`.
//! 3. When building a request that depends on a specific descriptor,
//!    the SDK calls [`SchemaCache::descriptor`]. A miss returns
//!    `None` so the SDK fetches via `LookupMessageSchema` and stores
//!    the result with [`SchemaCache::insert_descriptor`].
//! 4. When the SDK's local manifest checksum differs from the
//!    server's, [`SchemaCache::compatibility`] returns
//!    [`CatalogCompatibility::BreakingMaybe`] so the SDK can apply
//!    its own policy (warn, retry, fail-closed).
//!
//! ## Why in udb-portable
//!
//! The cache is pure logic + the AST types — no IO, no async, no
//! network. Putting it in the WASM-portable crate means a browser SDK
//! (compiled to wasm32-unknown-unknown) gets the same behaviour as a
//! server-side Rust SDK without dragging in tokio/sqlx/reqwest.

use std::collections::HashMap;

use crate::ast::ProtoSchema;
use crate::schema_checksum;

// ── Negotiation outcomes ──────────────────────────────────────────────────────

/// What `observe` reports after the SDK records a fresh server
/// header pair. The SDK matches on this and acts accordingly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Negotiation {
    /// First observation in this cache's lifetime. SDK records and
    /// continues normally.
    Initial {
        catalog_version: String,
        manifest_checksum: String,
    },
    /// Server is on the same catalog the SDK last saw — descriptors
    /// in the cache stay valid.
    Stable {
        catalog_version: String,
        manifest_checksum: String,
    },
    /// Catalog rolled. The version and/or checksum changed since the
    /// last observation; the SDK invalidates its descriptor cache.
    CatalogRolled {
        previous_version: String,
        previous_checksum: String,
        new_version: String,
        new_checksum: String,
    },
}

/// SDK-side compatibility verdict comparing a cached manifest
/// checksum against an observed one. The SDK supplies its build-time
/// checksum if it shipped with a generated client; otherwise it
/// passes the most recent observed checksum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogCompatibility {
    /// Checksums match — request is safe.
    Same,
    /// Checksums differ — the request might be wire-incompatible.
    /// The SDK's policy (warn / retry / fail-closed) decides what to
    /// do.
    BreakingMaybe {
        sdk_checksum: String,
        server_checksum: String,
    },
}

// ── Protocol support (A.2) ────────────────────────────────────────────────────

/// SDK-side mirror of the server's `udb.entity.v1.ProtocolSupport`
/// negotiation block (returned in `CapabilitiesResponse.protocol_support`).
///
/// `udb-portable` is the WASM/edge-safe subset and deliberately does **not**
/// depend on the generated `prost`/`tonic` admin types (see crate docs), so
/// this is a plain serde struct an SDK fills in after a `GetCapabilities`
/// call. The field set mirrors the proto message one-for-one so an SDK can
/// cache the negotiated protocol and decide, for example, whether to request
/// the V1 `record_set_v1` encoding or a later `record_batch_v2`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProtocolSupport {
    /// Lowest UDB wire protocol version the server still accepts (semver).
    pub min_protocol_version: String,
    /// Highest UDB wire protocol version the server speaks (semver).
    pub max_protocol_version: String,
    /// Supported row/wire encodings, e.g. `"record_set_v1"`.
    pub encodings: Vec<String>,
    /// Supported transport compression algorithms (may be empty).
    pub compression: Vec<String>,
    /// Whether the server can stream query results.
    pub supports_streaming_reads: bool,
    /// Whether the server can stream object payloads.
    pub supports_object_streaming: bool,
    /// Max inbound message size in bytes (0 == transport default).
    pub max_recv_message_bytes: i64,
    /// Max outbound message size in bytes (0 == transport default).
    pub max_send_message_bytes: i64,
    /// RPC method names supported by the server.
    pub supported_rpcs: Vec<String>,
}

impl ProtocolSupport {
    /// True when the server advertises the given wire encoding. SDKs use this
    /// to fall back from `record_batch_v2` to `record_set_v1` when the server
    /// has not yet shipped the typed columnar encoding.
    pub fn supports_encoding(&self, encoding: &str) -> bool {
        self.encodings.iter().any(|e| e == encoding)
    }
}

// ── SchemaCache ───────────────────────────────────────────────────────────────

/// In-memory cache of `(catalog_version, manifest_checksum)` plus
/// per-message descriptors. Single-threaded — the SDK wraps it in
/// whatever synchronization makes sense (a `Mutex` for Rust, a
/// `class` field for JS).
#[derive(Debug, Clone, Default)]
pub struct SchemaCache {
    last_seen: Option<ObservedHeaders>,
    descriptors: HashMap<String, ProtoSchema>,
    /// Negotiated protocol support, recorded after the SDK's first
    /// `GetCapabilities` call. `None` until the SDK calls
    /// [`SchemaCache::set_protocol_support`].
    protocol_support: Option<ProtocolSupport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedHeaders {
    catalog_version: String,
    manifest_checksum: String,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the server's response headers. Returns the typed
    /// negotiation outcome the SDK reacts to.
    pub fn observe(
        &mut self,
        catalog_version: impl Into<String>,
        manifest_checksum: impl Into<String>,
    ) -> Negotiation {
        let new = ObservedHeaders {
            catalog_version: catalog_version.into(),
            manifest_checksum: manifest_checksum.into(),
        };
        match self.last_seen.replace(new.clone()) {
            None => Negotiation::Initial {
                catalog_version: new.catalog_version,
                manifest_checksum: new.manifest_checksum,
            },
            Some(prev) if prev == new => Negotiation::Stable {
                catalog_version: new.catalog_version,
                manifest_checksum: new.manifest_checksum,
            },
            Some(prev) => {
                // Catalog rolled — descriptor cache must be flushed
                // because field numbers / column shape may differ.
                self.descriptors.clear();
                Negotiation::CatalogRolled {
                    previous_version: prev.catalog_version,
                    previous_checksum: prev.manifest_checksum,
                    new_version: new.catalog_version,
                    new_checksum: new.manifest_checksum,
                }
            }
        }
    }

    /// Most recent observation, or `None` if `observe` has never
    /// been called.
    pub fn last_observation(&self) -> Option<(&str, &str)> {
        self.last_seen
            .as_ref()
            .map(|o| (o.catalog_version.as_str(), o.manifest_checksum.as_str()))
    }

    /// Look up a cached descriptor by fully-qualified message name
    /// (e.g. `"udb.test.v1.User"`).
    pub fn descriptor(&self, qualified_name: &str) -> Option<&ProtoSchema> {
        self.descriptors.get(qualified_name)
    }

    /// Store a freshly-fetched descriptor.
    pub fn insert_descriptor(&mut self, qualified_name: impl Into<String>, schema: ProtoSchema) {
        self.descriptors.insert(qualified_name.into(), schema);
    }

    /// How many descriptors are currently cached.
    pub fn descriptor_count(&self) -> usize {
        self.descriptors.len()
    }

    /// Record the protocol support block the server returned from
    /// `GetCapabilities`. The SDK calls this once after negotiation so later
    /// requests can consult [`SchemaCache::protocol_support`] to pick a wire
    /// encoding / streaming path without re-fetching capabilities.
    pub fn set_protocol_support(&mut self, support: ProtocolSupport) {
        self.protocol_support = Some(support);
    }

    /// The negotiated protocol support, or `None` if the SDK has not yet
    /// called [`SchemaCache::set_protocol_support`].
    pub fn protocol_support(&self) -> Option<&ProtocolSupport> {
        self.protocol_support.as_ref()
    }

    /// Compare an SDK-side checksum against the most recent observed
    /// server checksum. `None` if `observe` has never been called —
    /// the SDK should treat that as "fetch first, decide later."
    pub fn compatibility(&self, sdk_checksum: &str) -> Option<CatalogCompatibility> {
        let observed = self.last_seen.as_ref()?;
        if observed.manifest_checksum == sdk_checksum {
            Some(CatalogCompatibility::Same)
        } else {
            Some(CatalogCompatibility::BreakingMaybe {
                sdk_checksum: sdk_checksum.to_string(),
                server_checksum: observed.manifest_checksum.clone(),
            })
        }
    }

    /// Convenience: compute the SDK's own manifest checksum from a
    /// schema slice and feed it into [`compatibility`]. Used by SDKs
    /// that ship a baked-in manifest and want a one-call freshness
    /// check.
    pub fn compatibility_against_local(
        &self,
        local_schemas: &[ProtoSchema],
    ) -> Result<Option<CatalogCompatibility>, serde_json::Error> {
        let checksum = schema_checksum(local_schemas)?;
        Ok(self.compatibility(&checksum))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{ProtoColumn, ProtoSchema};

    fn sample_schema() -> ProtoSchema {
        ProtoSchema {
            message_name: "User".to_string(),
            schema_name: "public".to_string(),
            table_name: "users".to_string(),
            columns: vec![ProtoColumn {
                field_name: "id".to_string(),
                column_name: "id".to_string(),
                proto_type: "string".to_string(),
                sql_type: "uuid".to_string(),
                field_number: 1,
                is_primary: true,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Pin: first observation produces `Initial`. The SDK uses this
    /// to bootstrap its descriptor cache.
    #[test]
    fn first_observation_is_initial() {
        let mut cache = SchemaCache::new();
        let outcome = cache.observe("v1", "abc");
        assert_eq!(
            outcome,
            Negotiation::Initial {
                catalog_version: "v1".to_string(),
                manifest_checksum: "abc".to_string(),
            }
        );
        assert_eq!(cache.last_observation(), Some(("v1", "abc")));
    }

    /// Pin: repeated observation with the same headers is `Stable`.
    /// Descriptors stay valid across a stream of stable observations.
    #[test]
    fn same_headers_observed_again_is_stable() {
        let mut cache = SchemaCache::new();
        cache.observe("v1", "abc");
        cache.insert_descriptor("udb.test.User", sample_schema());
        let outcome = cache.observe("v1", "abc");
        assert!(matches!(outcome, Negotiation::Stable { .. }));
        assert_eq!(
            cache.descriptor_count(),
            1,
            "descriptors must survive stable observations"
        );
    }

    /// Pin: catalog roll flushes the descriptor cache. The SDK must
    /// re-fetch because field numbers may have shifted.
    #[test]
    fn catalog_roll_flushes_descriptors() {
        let mut cache = SchemaCache::new();
        cache.observe("v1", "abc");
        cache.insert_descriptor("udb.test.User", sample_schema());
        assert_eq!(cache.descriptor_count(), 1);
        let outcome = cache.observe("v2", "def");
        match outcome {
            Negotiation::CatalogRolled {
                previous_version,
                previous_checksum,
                new_version,
                new_checksum,
            } => {
                assert_eq!(previous_version, "v1");
                assert_eq!(previous_checksum, "abc");
                assert_eq!(new_version, "v2");
                assert_eq!(new_checksum, "def");
            }
            other => panic!("expected CatalogRolled, got {:?}", other),
        }
        assert_eq!(
            cache.descriptor_count(),
            0,
            "descriptors must be flushed when the catalog rolls"
        );
    }

    /// Pin: a catalog-version change alone (checksum unchanged) is
    /// still a roll. The server may have shipped a no-op version
    /// bump, but the SDK can't tell from outside — flush to be safe.
    #[test]
    fn version_bump_with_same_checksum_still_flushes() {
        let mut cache = SchemaCache::new();
        cache.observe("v1", "abc");
        cache.insert_descriptor("udb.test.User", sample_schema());
        let outcome = cache.observe("v2", "abc");
        assert!(matches!(outcome, Negotiation::CatalogRolled { .. }));
        assert_eq!(cache.descriptor_count(), 0);
    }

    /// Pin: `compatibility` returns `Same` when the SDK's baked-in
    /// checksum matches the server's. The SDK skips a freshness fetch.
    #[test]
    fn compatibility_same_when_checksums_match() {
        let mut cache = SchemaCache::new();
        cache.observe("v1", "abc");
        assert_eq!(cache.compatibility("abc"), Some(CatalogCompatibility::Same));
    }

    /// Pin: `compatibility` flags a mismatch as `BreakingMaybe` with
    /// both checksums named, so the SDK can log a useful message.
    #[test]
    fn compatibility_breaking_maybe_when_checksums_differ() {
        let mut cache = SchemaCache::new();
        cache.observe("v1", "server-abc");
        match cache.compatibility("sdk-xyz") {
            Some(CatalogCompatibility::BreakingMaybe {
                sdk_checksum,
                server_checksum,
            }) => {
                assert_eq!(sdk_checksum, "sdk-xyz");
                assert_eq!(server_checksum, "server-abc");
            }
            other => panic!("expected BreakingMaybe, got {:?}", other),
        }
    }

    /// Pin: compatibility before any observation returns `None`. The
    /// SDK uses this as "fetch first, decide later."
    #[test]
    fn compatibility_returns_none_before_first_observation() {
        let cache = SchemaCache::new();
        assert!(cache.compatibility("anything").is_none());
    }

    /// Pin: protocol support starts unset and round-trips through the
    /// cache. A.2 — SDKs cache the negotiated `ProtocolSupport` so they can
    /// pick a wire encoding without re-fetching capabilities.
    #[test]
    fn protocol_support_round_trips_through_cache() {
        let mut cache = SchemaCache::new();
        assert!(
            cache.protocol_support().is_none(),
            "protocol support is unset until negotiated"
        );
        let support = ProtocolSupport {
            min_protocol_version: "1.0.0".to_string(),
            max_protocol_version: "1.0.0".to_string(),
            encodings: vec!["record_set_v1".to_string()],
            supports_streaming_reads: true,
            supports_object_streaming: true,
            supported_rpcs: vec!["Select".to_string()],
            ..Default::default()
        };
        cache.set_protocol_support(support.clone());
        let cached = cache.protocol_support().expect("set above");
        assert_eq!(cached, &support);
        assert!(cached.supports_encoding("record_set_v1"));
        assert!(
            !cached.supports_encoding("record_batch_v2"),
            "V2 columnar encoding is not advertised yet"
        );
    }

    /// Pin: `compatibility_against_local` computes the SDK's checksum
    /// from a schema slice. End-to-end smoke for the "I have a baked
    /// manifest, am I out of date?" workflow.
    #[test]
    fn compatibility_against_local_round_trips() {
        let mut cache = SchemaCache::new();
        let schema = sample_schema();
        let local_checksum = schema_checksum(&[schema.clone()]).unwrap();
        cache.observe("v1", local_checksum.clone());
        match cache
            .compatibility_against_local(&[schema])
            .expect("checksum compute")
            .expect("post-observation")
        {
            CatalogCompatibility::Same => {}
            other => panic!("expected Same, got {:?}", other),
        }
    }
}
