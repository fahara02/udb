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
use std::fmt;
use std::sync::{Mutex, OnceLock};

use crate::generation::manifest::{CatalogManifest, ManifestTable};

const AMBIGUOUS_INDEX: usize = usize::MAX;

#[derive(Default)]
struct ManifestLookupIndex {
    exact_fqn: HashMap<String, usize>,
    exact_physical: HashMap<String, usize>,
    short_message: HashMap<String, usize>,
    bare_table: HashMap<String, usize>,
}

fn insert_unique(index: &mut HashMap<String, usize>, key: String, table_index: usize) {
    match index.get(&key).copied() {
        Some(existing) if existing == table_index || existing == AMBIGUOUS_INDEX => {}
        Some(_) => {
            index.insert(key, AMBIGUOUS_INDEX);
        }
        None => {
            index.insert(key, table_index);
        }
    }
}

fn combined_index_hit(first: Option<&usize>, second: Option<&usize>) -> Option<usize> {
    match (first.copied(), second.copied()) {
        (Some(AMBIGUOUS_INDEX), _) | (_, Some(AMBIGUOUS_INDEX)) => None,
        (Some(left), Some(right)) if left != right => None,
        (Some(index), _) | (_, Some(index)) => Some(index),
        (None, None) => None,
    }
}

/// Resolve the `ManifestTable` for a fully-qualified or leaf message type,
/// case-insensitively, against either the message name or the physical table
/// name. Maintains a bounded process-global index keyed by manifest checksum so
/// repeated lookups are O(1); falls back to a linear scan when uncached.
pub fn table_for_message<'a>(
    manifest: &'a CatalogManifest,
    message_type: &str,
) -> Option<&'a ManifestTable> {
    static INDEX: OnceLock<Mutex<HashMap<String, ManifestLookupIndex>>> = OnceLock::new();
    let leaf = message_type
        .rsplit('.')
        .next()
        .unwrap_or(message_type)
        .to_ascii_lowercase();
    let exact = message_type.to_ascii_lowercase();
    // A checksum is the only stable identity for a catalog. Never cache by a
    // borrowed manifest's address: after that value is dropped the allocator
    // may reuse the address for a different same-sized manifest, which would
    // apply stale table indices and can misroute a request. Checksum-less test
    // or embedded manifests use the exact same ambiguity-refusing linear path
    // below.
    if !manifest.checksum_sha256.is_empty()
        && let Ok(mut guard) = INDEX.get_or_init(|| Mutex::new(HashMap::new())).lock()
    {
        let cache_key = manifest.checksum_sha256.clone();
        // Bound the process-global cache: manifest checksums change on every
        // schema reload, so without a cap this map grows forever. When a new
        // manifest would exceed the cap, drop the whole cache and rebuild lazily
        // (rebuilding one table's index is cheap) — #136.
        const INDEX_CACHE_CAP: usize = 64;
        if guard.len() >= INDEX_CACHE_CAP && !guard.contains_key(&cache_key) {
            guard.clear();
        }
        let index = guard.entry(cache_key).or_insert_with(|| {
            // UDB-SRV-003/CAT-001: an AMBIGUITY-REFUSING index. Exact keys (the
            // protobuf FQN and the schema-qualified physical identity) always
            // resolve deterministically. Convenience keys (bare message name,
            // short leaf, bare physical table name) are valid ONLY while unique
            // in the composed catalog: when two tables claim the same
            // convenience key (consumer `acme.authn.entity.v1.User` vs
            // embedded `udb.core.authn.entity.v1.User`, or `authn.users` vs
            // `udb_authn.users`) the key is poisoned with `AMBIGUOUS` and the
            // lookup refuses — a silent first-wins misroute to an arbitrary
            // tenant table is strictly worse than a fail-closed miss. Callers
            // that need the collided entity must qualify with the FQN (the data
            // plane already sends FQNs) or `schema.table`.
            let mut built = ManifestLookupIndex::default();
            for (idx, table) in manifest.tables.iter().enumerate() {
                if !table.message_name.trim().is_empty() {
                    insert_unique(
                        &mut built.exact_fqn,
                        table.message_fqn().to_ascii_lowercase(),
                        idx,
                    );
                    if let Some(short) = table.message_name.rsplit('.').next() {
                        insert_unique(&mut built.short_message, short.to_ascii_lowercase(), idx);
                    }
                }
                if !table.table.trim().is_empty() {
                    if !table.schema.trim().is_empty() {
                        insert_unique(
                            &mut built.exact_physical,
                            format!("{}.{}", table.schema, table.table).to_ascii_lowercase(),
                            idx,
                        );
                    }
                    insert_unique(&mut built.bare_table, table.table.to_ascii_lowercase(), idx);
                }
            }
            built
        });
        // HIGH-5: a DOTTED input names an exact identity (protobuf FQN or
        // `schema.table`) — it must NEVER degrade to its leaf. Otherwise
        // `wrong.package.Invoice` silently routes to whatever single `Invoice`
        // the catalog happens to hold. Leaf convenience applies to UNDOTTED
        // inputs only.
        let dotted = message_type.contains('.');
        let hit = if dotted {
            combined_index_hit(
                index.exact_fqn.get(&exact),
                index.exact_physical.get(&exact),
            )
        } else {
            combined_index_hit(index.short_message.get(&leaf), index.bare_table.get(&leaf))
        };
        if let Some(idx) = hit {
            return manifest.tables.get(idx);
        }
        return None;
    }
    // Uncached linear fallback: same ambiguity-refusing contract. Exact
    // identities (FQN, schema-qualified table) win immediately; convenience
    // identities resolve only when exactly ONE table matches.
    let dotted = message_type.contains('.');
    let mut matched_index: Option<usize> = None;
    for (index, table) in manifest.tables.iter().enumerate() {
        let hit = if dotted {
            table.message_fqn().eq_ignore_ascii_case(message_type)
                || (!table.schema.trim().is_empty()
                    && format!("{}.{}", table.schema, table.table)
                        .eq_ignore_ascii_case(message_type))
        } else {
            table
                .message_name
                .rsplit('.')
                .next()
                .is_some_and(|short| short.eq_ignore_ascii_case(&leaf))
                || table.table.eq_ignore_ascii_case(&leaf)
        };
        if hit {
            if matched_index.is_some_and(|matched| matched != index) {
                return None;
            }
            matched_index = Some(index);
        }
    }
    matched_index.and_then(|index| manifest.tables.get(index))
}

/// The typed lookup contract (fix_plan §4.1): distinguishes a genuinely
/// unknown identity from a CONVENIENCE identity (bare message name, short
/// leaf, bare table name) that matches MORE THAN ONE table in the composed
/// catalog — the latter carries every candidate FQN and physical table so the
/// caller can emit an actionable disambiguation error instead of a bare
/// "unknown".
pub enum TableLookup<'a> {
    Found(&'a ManifestTable),
    Missing,
    Ambiguous { candidates: Vec<String> },
}

/// Owned lookup failure used by runtime consumers. Keeping the requested
/// identity and candidate list in the error prevents callers from collapsing
/// an ambiguity into a generic missing-table branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableLookupError {
    Missing {
        message_type: String,
        /// How many entities this broker image actually knows, and the catalog
        /// checksum that produced them.
        ///
        /// A broker only serves entities baked into its image, so adding one to
        /// the project proto and deploying the services that use it fails at the
        /// FIRST WRITE — which for a new feature can be hours later, in whatever
        /// code path happens to run first. "unknown message_type X" alone reads
        /// like a typo; the count and checksum say "this image predates your
        /// entity" and point at a rebuild.
        known_entities: usize,
        catalog_checksum: String,
    },
    Ambiguous {
        message_type: String,
        candidates: Vec<String>,
    },
}

impl fmt::Display for TableLookupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing {
                message_type,
                known_entities,
                catalog_checksum,
            } => {
                let checksum = catalog_checksum.get(..12).unwrap_or(catalog_checksum);
                write!(
                    formatter,
                    "unknown message_type {message_type}: this broker serves {known_entities}                      entities from catalog {checksum}. If you just added this entity, the running                      broker image predates it — rebuild and redeploy the broker, then retry"
                )
            }
            Self::Ambiguous {
                message_type,
                candidates,
            } => write!(
                formatter,
                "ambiguous message type '{message_type}': it matches multiple catalog entities \
                 [{}]; qualify with the fully-qualified protobuf name or schema.table",
                candidates.join(", ")
            ),
        }
    }
}

impl std::error::Error for TableLookupError {}

/// Resolve with ambiguity made explicit. `Found` follows exactly the
/// [`table_for_message`] rules (exact FQN / `schema.table` first, unique
/// convenience identities second); a refused ambiguous convenience identity
/// comes back as `Ambiguous` with the candidate FQNs and physical tables.
pub fn table_lookup<'a>(manifest: &'a CatalogManifest, message_type: &str) -> TableLookup<'a> {
    if let Some(table) = table_for_message(manifest, message_type) {
        return TableLookup::Found(table);
    }
    let dotted = message_type.contains('.');
    let leaf = message_type
        .rsplit('.')
        .next()
        .unwrap_or(message_type)
        .to_ascii_lowercase();
    let candidates: Vec<String> = manifest
        .tables
        .iter()
        .filter(|table| {
            if dotted {
                table.message_fqn().eq_ignore_ascii_case(message_type)
                    || (!table.schema.trim().is_empty()
                        && format!("{}.{}", table.schema, table.table)
                            .eq_ignore_ascii_case(message_type))
            } else {
                table
                    .message_name
                    .rsplit('.')
                    .next()
                    .is_some_and(|short| short.eq_ignore_ascii_case(&leaf))
                    || table.table.eq_ignore_ascii_case(&leaf)
            }
        })
        .map(|table| {
            let physical = if table.schema.trim().is_empty() {
                table.table.clone()
            } else {
                format!("{}.{}", table.schema, table.table)
            };
            format!("{} ({physical})", table.message_fqn())
        })
        .collect();
    if candidates.len() > 1 {
        TableLookup::Ambiguous { candidates }
    } else {
        TableLookup::Missing
    }
}

/// Canonical runtime lookup contract. New production consumers should use this
/// instead of the legacy `Option` wrapper so ambiguity remains typed and its
/// candidate list survives every error boundary.
pub fn resolve_table_for_message<'a>(
    manifest: &'a CatalogManifest,
    message_type: &str,
) -> Result<&'a ManifestTable, TableLookupError> {
    match table_lookup(manifest, message_type) {
        TableLookup::Found(table) => Ok(table),
        TableLookup::Missing => Err(TableLookupError::Missing {
            message_type: message_type.to_string(),
            known_entities: manifest.tables.len(),
            catalog_checksum: manifest.checksum_sha256.clone(),
        }),
        TableLookup::Ambiguous { candidates } => Err(TableLookupError::Ambiguous {
            message_type: message_type.to_string(),
            candidates,
        }),
    }
}

/// The actionable error text for a failed lookup: names every candidate on an
/// ambiguous convenience identity so the caller knows exactly how to qualify.
pub fn describe_table_lookup_miss(manifest: &CatalogManifest, message_type: &str) -> String {
    match resolve_table_for_message(manifest, message_type) {
        Err(error) => error.to_string(),
        Ok(_) => format!("message_type {message_type} resolved successfully"),
    }
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
    /// `acme.authn.entity.v1.User`.)
    #[test]
    fn exact_fqn_disambiguates_shared_short_name() {
        let manifest = CatalogManifest {
            // Non-empty checksum so the cached index path (not just the linear
            // fallback) is exercised.
            checksum_sha256: "srv003-fqn-index".to_string(),
            tables: vec![
                table("acme.authn.entity.v1", "User", "authn", "users"),
                table("udb.core.authn.entity.v1", "User", "udb_authn", "users"),
            ],
            ..CatalogManifest::default()
        };

        let consumer = table_for_message(&manifest, "acme.authn.entity.v1.User")
            .expect("consumer FQN must resolve");
        assert_eq!(consumer.schema, "authn");

        let embedded = table_for_message(&manifest, "udb.core.authn.entity.v1.User")
            .expect("embedded FQN must resolve");
        assert_eq!(embedded.schema, "udb_authn");
    }

    /// UDB-CAT-001 / SRV-003 acceptance at the resolver level: a BARE short name
    /// shared by two packages must refuse to resolve (never first-wins into an
    /// arbitrary table), while each FQN and each schema-qualified physical
    /// identity still resolves to its own table, and a UNIQUE short name keeps
    /// working as a convenience.
    #[test]
    fn ambiguous_short_name_refuses_instead_of_first_wins() {
        let manifest = CatalogManifest {
            checksum_sha256: "cat001-ambiguity-index".to_string(),
            tables: vec![
                table("acme.authn.entity.v1", "OTP", "authn", "otps"),
                table("udb.core.authn.entity.v1", "OTP", "udb_authn", "otps"),
                table("acme.fleet.entity.v1", "Vehicle", "fleet", "vehicles"),
            ],
            ..CatalogManifest::default()
        };

        // Ambiguous convenience identities refuse (bare message name AND bare
        // physical table name are both shared).
        assert!(table_for_message(&manifest, "OTP").is_none());
        assert!(table_for_message(&manifest, "otps").is_none());

        // Exact identities still resolve deterministically.
        assert_eq!(
            table_for_message(&manifest, "acme.authn.entity.v1.OTP")
                .expect("consumer FQN resolves")
                .schema,
            "authn"
        );
        assert_eq!(
            table_for_message(&manifest, "udb.core.authn.entity.v1.OTP")
                .expect("embedded FQN resolves")
                .schema,
            "udb_authn"
        );
        assert_eq!(
            table_for_message(&manifest, "udb_authn.otps")
                .expect("schema-qualified physical identity resolves")
                .schema,
            "udb_authn"
        );

        // A globally-unique short name remains a valid convenience key.
        assert_eq!(
            table_for_message(&manifest, "Vehicle")
                .expect("unique short name resolves")
                .schema,
            "fleet"
        );

        // HIGH-5: a DOTTED identity is exact-only — an unknown package must
        // NEVER degrade to its leaf and route to some same-named table.
        assert!(
            table_for_message(&manifest, "wrong.package.Vehicle").is_none(),
            "unknown dotted package must not resolve by leaf"
        );
        assert!(matches!(
            table_lookup(&manifest, "wrong.package.OTP"),
            TableLookup::Missing
        ));

        // Exact protobuf and physical identities occupy separate namespaces,
        // but a request that collides across them is still ambiguous. It must
        // not silently prefer one namespace.
        let cross_namespace = CatalogManifest {
            checksum_sha256: "cat001-cross-namespace".to_string(),
            tables: vec![
                table("acme.billing", "Invoice", "billing", "invoices"),
                table("other.package", "Ledger", "acme", "billing.Invoice"),
            ],
            ..CatalogManifest::default()
        };
        let TableLookup::Ambiguous { candidates } =
            table_lookup(&cross_namespace, "acme.billing.Invoice")
        else {
            panic!("cross-namespace exact collision must be ambiguous");
        };
        assert_eq!(candidates.len(), 2);
        assert!(candidates.iter().any(|candidate| {
            candidate.contains("acme.billing.Invoice") && candidate.contains("billing.invoices")
        }));
        assert!(candidates.iter().any(|candidate| {
            candidate.contains("other.package.Ledger") && candidate.contains("acme.billing.Invoice")
        }));
        let error = resolve_table_for_message(&cross_namespace, "acme.billing.Invoice")
            .expect_err("runtime lookup must retain the cross-namespace ambiguity");
        assert!(matches!(error, TableLookupError::Ambiguous { .. }));
        assert!(error.to_string().contains("other.package.Ledger"));

        // Uncached path: an empty checksum deliberately uses the pure linear
        // fallback because a borrowed manifest address is not a stable cache key.
        let uncached = CatalogManifest {
            checksum_sha256: String::new(),
            tables: manifest.tables.clone(),
            ..CatalogManifest::default()
        };
        assert!(table_for_message(&uncached, "OTP").is_none());
        assert_eq!(
            table_for_message(&uncached, "acme.authn.entity.v1.OTP")
                .expect("consumer FQN resolves uncached")
                .schema,
            "authn"
        );
    }
}
