//! Portable message-type → table lookup over a `CatalogManifest`.
//!
//! Extracted out of `planning/broker/mod.rs` (a native module) so the
//! WASM/edge-safe `udb-portable` crate can resolve `crate::broker::table_for_message`
//! — the one broker function the IR→SQL compilers call (via
//! `ir::compile::sql_dialect::SqlCompiler::resolve_table`). The implementation
//! is pure: `std::sync::{OnceLock,Mutex}` + `std::collections::HashMap` (all
//! wasm32-unknown-unknown-clean, single-threaded) over manifest fields only.
//! `planning::broker` re-exports this so server call-sites are unchanged.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::generation::manifest::{CatalogManifest, ManifestTable};

/// Resolve the `ManifestTable` for a fully-qualified or leaf message type,
/// case-insensitively, against either the message name or the physical table
/// name. Maintains a bounded process-global index keyed by manifest checksum so
/// repeated lookups are O(1); falls back to a linear scan when uncached.
pub fn table_for_message<'a>(
    manifest: &'a CatalogManifest,
    message_type: &str,
) -> Option<&'a ManifestTable> {
    static INDEX: OnceLock<Mutex<HashMap<String, HashMap<String, usize>>>> = OnceLock::new();
    let leaf = message_type
        .rsplit('.')
        .next()
        .unwrap_or(message_type)
        .to_ascii_lowercase();
    let exact = message_type.to_ascii_lowercase();
    let cache_key = if manifest.checksum_sha256.is_empty() {
        format!("ptr:{:p}:{}", manifest, manifest.tables.len())
    } else {
        manifest.checksum_sha256.clone()
    };
    let map = INDEX.get_or_init(|| Mutex::new(HashMap::new()));
    if let Ok(mut guard) = map.lock() {
        // Bound the process-global cache: manifest checksums change on every
        // schema reload, so without a cap this map grows forever. When a new
        // manifest would exceed the cap, drop the whole cache and rebuild lazily
        // (rebuilding one table's index is cheap) — #136.
        const INDEX_CACHE_CAP: usize = 64;
        if guard.len() >= INDEX_CACHE_CAP && !guard.contains_key(&cache_key) {
            guard.clear();
        }
        let index = guard.entry(cache_key).or_insert_with(|| {
            let mut built = HashMap::new();
            for (idx, table) in manifest.tables.iter().enumerate() {
                if !table.message_name.trim().is_empty() {
                    // Register the FULLY-QUALIFIED name so an exact FQN query
                    // resolves deterministically even when two packages reuse a
                    // short name (e.g. `ambulife.authn.entity.v1.User` vs embedded
                    // `udb.core.authn.entity.v1.User`). The bare short name below is
                    // a convenience fallback only; under a short-name collision only
                    // the FQN query is unambiguous.
                    built
                        .entry(table.message_fqn().to_ascii_lowercase())
                        .or_insert(idx);
                    built.insert(table.message_name.to_ascii_lowercase(), idx);
                    if let Some(short) = table.message_name.rsplit('.').next() {
                        built.entry(short.to_ascii_lowercase()).or_insert(idx);
                    }
                }
                if !table.table.trim().is_empty() {
                    built.insert(table.table.to_ascii_lowercase(), idx);
                }
            }
            built
        });
        if let Some(idx) = index.get(&exact).or_else(|| index.get(&leaf)) {
            return manifest.tables.get(*idx);
        }
    }
    manifest.tables.iter().find(|table| {
        table.message_name.eq_ignore_ascii_case(message_type)
            || table.message_name.eq_ignore_ascii_case(&leaf)
            || table.table.eq_ignore_ascii_case(message_type)
            || table.table.eq_ignore_ascii_case(&leaf)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(proto_package: &str, message: &str, schema: &str, name: &str) -> ManifestTable {
        ManifestTable {
            message_name: message.to_string(),
            proto_package: proto_package.to_string(),
            schema: schema.to_string(),
            table: name.to_string(),
            ..ManifestTable::default()
        }
    }

    /// Two messages that share a short name but belong to different packages must
    /// each resolve to their OWN table when queried by the fully-qualified name.
    /// (UDB-SRV-003: embedded `udb.core.authn.entity.v1.User` vs consumer
    /// `ambulife.authn.entity.v1.User`.)
    #[test]
    fn exact_fqn_disambiguates_shared_short_name() {
        let manifest = CatalogManifest {
            // Non-empty checksum so the cached index path (not just the linear
            // fallback) is exercised.
            checksum_sha256: "srv003-fqn-index".to_string(),
            tables: vec![
                table("ambulife.authn.entity.v1", "User", "authn", "users"),
                table("udb.core.authn.entity.v1", "User", "udb_authn", "users"),
            ],
            ..CatalogManifest::default()
        };

        let consumer = table_for_message(&manifest, "ambulife.authn.entity.v1.User")
            .expect("consumer FQN must resolve");
        assert_eq!(consumer.schema, "authn");

        let embedded = table_for_message(&manifest, "udb.core.authn.entity.v1.User")
            .expect("embedded FQN must resolve");
        assert_eq!(embedded.schema, "udb_authn");
    }
}
