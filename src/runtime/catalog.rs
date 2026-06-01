//! Multi-project catalog manager (U4).
//!
//! Until U4, this module held a **single** active catalog and a single
//! staging slot — fine for one project but not for a broker serving
//! multiple proto packages from the same process. The data-path handlers
//! all call `catalog.active().manifest`, which silently treated every
//! request as belonging to one project.
//!
//! U4 reshapes the manager so it owns:
//!
//! - `active_catalogs: ArcSwap<HashMap<project_id, Arc<CatalogState>>>`
//!   — lock-free reads on the hot path; one ArcSwap of a small map.
//! - `staged_catalogs: RwLock<HashMap<project_id, CatalogState>>`
//!   — protected by a write lock because stage / activate / rollback
//!   transitions a single project's slot at a time.
//!
//! The pre-existing single-project API (`active()`, `staged()`,
//! `activate_catalog`, `rollback_catalog`) is preserved as a back-compat
//! shim that operates on the `"default"` project, so callers that haven't
//! yet been migrated to pass a project_id keep working.

use crate::CatalogManifest;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// The bookkeeping that distinguishes one project's catalog from another.
#[derive(Debug, Clone)]
pub struct CatalogMetadata {
    pub version: String,
    pub checksum: String,
    pub project_id: String,
    pub compatibility_level: String,
    pub applied_at_unix: i64,
}

/// One project's active catalog: the proto-derived manifest + metadata.
#[derive(Debug, Clone)]
pub struct CatalogState {
    pub manifest: Arc<CatalogManifest>,
    pub metadata: CatalogMetadata,
}

/// The single project ID the back-compat wrappers act on. Anywhere else
/// in the codebase that needs the same constant should import this,
/// not re-spell `"default"`.
pub const DEFAULT_PROJECT_ID: &str = "default";

#[derive(Debug)]
pub struct CatalogManager {
    /// Live, per-project active catalogs. ArcSwap over the whole map so
    /// the data-path read (`active_for(&project_id).manifest`) doesn't
    /// take any lock.
    active_catalogs: ArcSwap<HashMap<String, Arc<CatalogState>>>,
    /// In-progress stagings keyed by project. Stage → activate is a
    /// project-local transition, so an RwLock around the map is fine
    /// (writes are infrequent; reads use the active_catalogs path).
    /// Staged catalogs keyed by project, each tagged with its staged-at instant
    /// so old un-activated stagings can be evicted (#211).
    staged_catalogs: RwLock<HashMap<String, (CatalogState, std::time::Instant)>>,
}

impl CatalogManager {
    /// Seed with one initial manifest, registered under the "default"
    /// project. Pre-U4 callers (single-tenant deployments) keep this
    /// constructor; multi-project deployments push additional manifests
    /// via `stage_catalog(project_id, …)` + `activate_catalog_for`.
    pub fn new(initial_manifest: CatalogManifest) -> Self {
        let checksum = if !initial_manifest.checksum_sha256.is_empty() {
            initial_manifest.checksum_sha256.clone()
        } else {
            "initial".to_string()
        };

        let initial_state = CatalogState {
            manifest: Arc::new(initial_manifest),
            metadata: CatalogMetadata {
                version: "1.0.0".to_string(),
                checksum,
                project_id: DEFAULT_PROJECT_ID.to_string(),
                compatibility_level: "exact".to_string(),
                applied_at_unix: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64,
            },
        };

        let mut initial_map = HashMap::with_capacity(1);
        initial_map.insert(DEFAULT_PROJECT_ID.to_string(), Arc::new(initial_state));

        Self {
            active_catalogs: ArcSwap::from_pointee(initial_map),
            staged_catalogs: RwLock::new(HashMap::new()),
        }
    }

    /// Back-compat: active catalog for the **default** project. New code
    /// should prefer [`active_for`] with the request's `project_id`.
    pub fn active(&self) -> Arc<CatalogState> {
        self.active_for(DEFAULT_PROJECT_ID)
    }

    /// Active catalog for `project_id`. Falls back to the default
    /// project's catalog when `project_id` is empty or has no active
    /// catalog yet — this is what makes legacy single-project clients
    /// keep working even after the broker grows multi-project support.
    pub fn active_for(&self, project_id: &str) -> Arc<CatalogState> {
        let key = canonical_project_key(project_id);
        let map = self.active_catalogs.load();
        if let Some(state) = map.get(&*key) {
            return state.clone();
        }
        if let Some(state) = map.get(DEFAULT_PROJECT_ID) {
            return state.clone();
        }
        // Should never trigger — `new` always inserts the default — but
        // produce something rather than panicking, so a misconfigured
        // broker doesn't take down requests at the catalog edge.
        Arc::new(CatalogState {
            manifest: Arc::new(CatalogManifest::default()),
            metadata: CatalogMetadata {
                version: "1.0.0".into(),
                checksum: "empty".into(),
                project_id: key.into_owned(),
                compatibility_level: "exact".into(),
                applied_at_unix: 0,
            },
        })
    }

    /// All project IDs with an active catalog. Used by `GetAdminSummary`
    /// and the portal to enumerate live projects.
    pub fn active_project_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.active_catalogs.load().keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Back-compat: metadata for the default project's active catalog.
    pub fn active_metadata(&self) -> CatalogMetadata {
        self.active().metadata.clone()
    }

    /// Metadata for `project_id` (falls back to default per [`active_for`]).
    pub fn active_metadata_for(&self, project_id: &str) -> CatalogMetadata {
        self.active_for(project_id).metadata.clone()
    }

    /// Back-compat: staged catalog for the default project.
    pub async fn staged(&self) -> Option<CatalogState> {
        self.staged_for(DEFAULT_PROJECT_ID).await
    }

    /// Staged (not yet active) catalog for `project_id`.
    pub async fn staged_for(&self, project_id: &str) -> Option<CatalogState> {
        let key = canonical_project_key(project_id);
        self.staged_catalogs
            .read()
            .await
            .get(&*key)
            .map(|(state, _)| state.clone())
    }

    /// Stage a new catalog **for the project carried in the metadata**.
    /// The metadata's `project_id` is taken verbatim; if it's blank the
    /// catalog is staged for the default project — same back-compat
    /// promise as the rest of the API.
    pub async fn stage_catalog(
        &self,
        manifest: CatalogManifest,
        project_id: String,
        version: String,
        compatibility_level: String,
    ) -> Result<String, String> {
        let key = canonical_project_key(&project_id).into_owned();
        let version = if version.trim().is_empty() {
            catalog_version_from_manifest(&manifest)
        } else {
            version.trim().to_string()
        };
        let compatibility_level = normalize_compatibility_level(&compatibility_level);
        let checksum = if !manifest.checksum_sha256.is_empty() {
            manifest.checksum_sha256.clone()
        } else {
            Uuid::new_v4().to_string()
        };

        let new_state = CatalogState {
            manifest: Arc::new(manifest),
            metadata: CatalogMetadata {
                version,
                checksum: checksum.clone(),
                project_id: key.clone(),
                compatibility_level: compatibility_level.to_string(),
                applied_at_unix: 0,
            },
        };

        let mut staged = self.staged_catalogs.write().await;
        // #211: bound the staged map — evict stagings idle past the TTL, then
        // the oldest until under the cap, before inserting this project's entry.
        // Re-staging a project replaces its own entry, so this only grows with
        // distinct projects that stage without ever activating/rolling back.
        const STAGED_CATALOG_MAX: usize = 1000;
        const STAGED_CATALOG_TTL: std::time::Duration = std::time::Duration::from_secs(3600);
        let now = std::time::Instant::now();
        if staged.len() >= STAGED_CATALOG_MAX && !staged.contains_key(&key) {
            staged.retain(|_, (_, at)| now.saturating_duration_since(*at) < STAGED_CATALOG_TTL);
            if staged.len() >= STAGED_CATALOG_MAX {
                let mut ages: Vec<(String, std::time::Instant)> =
                    staged.iter().map(|(k, (_, at))| (k.clone(), *at)).collect();
                ages.sort_by_key(|(_, t)| *t);
                let to_remove = staged.len() + 1 - STAGED_CATALOG_MAX;
                for (k, _) in ages.into_iter().take(to_remove) {
                    staged.remove(&k);
                }
            }
        }
        staged.insert(key, (new_state, now));
        Ok(checksum)
    }

    /// Back-compat: activate the default project's staged catalog.
    pub async fn activate_catalog(&self, expected_selector: &str) -> Result<(), String> {
        self.activate_catalog_for(DEFAULT_PROJECT_ID, expected_selector)
            .await
    }

    /// Activate the catalog staged for `project_id`. The selector must
    /// match the staged version or checksum (or be empty). On mismatch
    /// the staged entry is returned to the map untouched so the caller
    /// can re-stage or retry with the correct selector.
    pub async fn activate_catalog_for(
        &self,
        project_id: &str,
        expected_selector: &str,
    ) -> Result<(), String> {
        let key = canonical_project_key(project_id).into_owned();
        let mut staged = self.staged_catalogs.write().await;
        let Some((state, staged_at)) = staged.remove(&*key) else {
            return Err(format!("no catalog staged for project '{key}'"));
        };
        let selector = expected_selector.trim();
        let matched = selector.is_empty()
            || state.metadata.checksum == selector
            || state.metadata.version == selector;
        if !matched {
            let staged_version = state.metadata.version.clone();
            let staged_checksum = state.metadata.checksum.clone();
            staged.insert(key.clone(), (state, staged_at));
            return Err(format!(
                "catalog selector mismatch for project '{key}': expected {expected_selector}, \
                 staged version is {staged_version}, staged checksum is {staged_checksum}"
            ));
        }
        let mut activated = state;
        activated.metadata.applied_at_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        // Publish into the active map via `rcu` (compare-and-swap retry) so the
        // read-clone-mutate-store is atomic against any future writer, not just
        // the `staged_catalogs` write lock currently held here. Readers continue
        // to see one consistent pointer or the other, never a torn write.
        // (#207 — defensive: today the held staged lock already serializes
        // activations, so the CAS never actually retries.)
        let activated = Arc::new(activated);
        self.active_catalogs.rcu(|current| {
            let mut next: HashMap<String, Arc<CatalogState>> = (**current).clone();
            next.insert(key.clone(), Arc::clone(&activated));
            Arc::new(next)
        });
        Ok(())
    }

    /// Back-compat: roll back the default project's staged catalog.
    pub async fn rollback_catalog(&self) -> Result<(), String> {
        self.rollback_catalog_for(DEFAULT_PROJECT_ID).await
    }

    /// Discard the staged catalog for `project_id` without activating.
    pub async fn rollback_catalog_for(&self, project_id: &str) -> Result<(), String> {
        let key = canonical_project_key(project_id);
        let mut staged = self.staged_catalogs.write().await;
        if staged.remove(&*key).is_some() {
            Ok(())
        } else {
            Err(format!("no catalog staged for project '{key}' to rollback"))
        }
    }

    /// True when `client_version` is acceptable against the **default**
    /// project's active catalog. Back-compat; new callers should use
    /// `is_compatible_for(project_id, client_version)`.
    pub fn is_compatible(&self, client_version: &str, project_id: &str) -> bool {
        self.compatibility_error(client_version, project_id)
            .is_none()
    }

    pub fn compatibility_error(&self, client_version: &str, project_id: &str) -> Option<String> {
        // Skip validation if both inputs blank (lenient mode preserved
        // from pre-U4).
        if client_version.is_empty() && project_id.is_empty() {
            return None;
        }
        let active = self.active_for(project_id);
        let proj_match = active.metadata.project_id == project_id
            || active.metadata.project_id == DEFAULT_PROJECT_ID
            || project_id.is_empty();
        if !proj_match {
            return Some(format!(
                "client project '{}' is not bound to active catalog project '{}'",
                project_id, active.metadata.project_id
            ));
        }
        if client_version.trim().is_empty() {
            return None;
        }
        match normalize_compatibility_level(&active.metadata.compatibility_level) {
            "none" | "any" => None,
            "exact" => (active.metadata.version == client_version)
                .then_some(())
                .map_or_else(
                    || {
                        Some(format!(
                            "client catalog version '{}' must exactly match active version '{}'",
                            client_version, active.metadata.version
                        ))
                    },
                    |_| None,
                ),
            _ => {
                if active.metadata.version == client_version
                    || backward_compatible(&active.metadata.version, client_version)
                {
                    None
                } else {
                    Some(format!(
                        "client catalog version '{}' is not backward-compatible with active version '{}'",
                        client_version, active.metadata.version
                    ))
                }
            }
        }
    }
}

/// Normalise an incoming `project_id` to the lookup key. Empty / blank
/// values collapse to `DEFAULT_PROJECT_ID` so a missing header doesn't
/// silently fall off the routing map.
///
/// Returns `Cow` because the common case (already correct) avoids an
/// allocation; only blank → "default" or trim-needed paths copy.
fn canonical_project_key(raw: &str) -> std::borrow::Cow<'_, str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        std::borrow::Cow::Borrowed(DEFAULT_PROJECT_ID)
    } else if trimmed.len() == raw.len() {
        std::borrow::Cow::Borrowed(trimmed)
    } else {
        std::borrow::Cow::Owned(trimmed.to_string())
    }
}

fn catalog_version_from_manifest(manifest: &CatalogManifest) -> String {
    if !manifest.generator_version.trim().is_empty() {
        format!("generator-{}", manifest.generator_version.trim())
    } else if !manifest.checksum_sha256.trim().is_empty() {
        manifest.checksum_sha256.chars().take(12).collect()
    } else {
        "unversioned".to_string()
    }
}

fn normalize_compatibility_level(level: &str) -> &'static str {
    match level.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "exact" => "exact",
        "none" | "any" | "disabled" => "none",
        _ => "backward",
    }
}

fn backward_compatible(active_version: &str, client_version: &str) -> bool {
    let Some(active) = parse_semver(active_version) else {
        return false;
    };
    let Some(client) = parse_semver(client_version) else {
        return false;
    };
    active.0 == client.0 && (active.1, active.2) >= (client.1, client.2)
}

fn parse_semver(version: &str) -> Option<(u64, u64, u64)> {
    let cleaned = version
        .trim()
        .strip_prefix('v')
        .unwrap_or_else(|| version.trim());
    let mut parts = cleaned.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next().unwrap_or("0").parse().ok()?;
    let patch = parts
        .next()
        .unwrap_or("0")
        .split(|c: char| !c.is_ascii_digit())
        .next()
        .unwrap_or("0")
        .parse()
        .ok()?;
    Some((major, minor, patch))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(checksum: &str) -> CatalogManifest {
        CatalogManifest {
            checksum_sha256: checksum.to_string(),
            generator_version: "3".to_string(),
            ..CatalogManifest::default()
        }
    }

    #[tokio::test]
    async fn stages_and_activates_by_version_or_checksum() {
        let manager = CatalogManager::new(manifest("initial"));
        let checksum = manager
            .stage_catalog(
                manifest("abc123"),
                "prime".to_string(),
                "2.1.0".to_string(),
                "backward".to_string(),
            )
            .await
            .unwrap();
        assert_eq!(checksum, "abc123");
        assert_eq!(
            manager.staged_for("prime").await.unwrap().metadata.version,
            "2.1.0"
        );

        manager
            .activate_catalog_for("prime", "2.1.0")
            .await
            .unwrap();
        let active = manager.active_metadata_for("prime");
        assert_eq!(active.project_id, "prime");
        assert_eq!(active.checksum, "abc123");
        assert!(active.applied_at_unix > 0);
    }

    #[tokio::test]
    async fn projects_are_isolated() {
        // Two projects, two distinct manifests, the data-path lookup
        // must hand back the right one for each project_id.
        let mut billing_manifest = manifest("billing-checksum");
        billing_manifest.generator_version = "billing-1".into();
        let mut analytics_manifest = manifest("analytics-checksum");
        analytics_manifest.generator_version = "analytics-1".into();

        let manager = CatalogManager::new(manifest("default-initial"));
        manager
            .stage_catalog(
                billing_manifest,
                "billing".into(),
                "1.0.0".into(),
                "backward".into(),
            )
            .await
            .unwrap();
        manager
            .activate_catalog_for("billing", "1.0.0")
            .await
            .unwrap();
        manager
            .stage_catalog(
                analytics_manifest,
                "analytics".into(),
                "1.0.0".into(),
                "backward".into(),
            )
            .await
            .unwrap();
        manager
            .activate_catalog_for("analytics", "1.0.0")
            .await
            .unwrap();

        let billing_state = manager.active_for("billing");
        let analytics_state = manager.active_for("analytics");
        let default_state = manager.active_for(DEFAULT_PROJECT_ID);
        let unknown_state = manager.active_for("unknown-project");

        assert_eq!(billing_state.metadata.checksum, "billing-checksum");
        assert_eq!(analytics_state.metadata.checksum, "analytics-checksum");
        assert_eq!(default_state.metadata.checksum, "default-initial");

        // Unknown project_id falls back to the default catalog — not to
        // some other project's data. This is what stops a missing
        // header from leaking project A's manifest to project B.
        assert_eq!(
            unknown_state.metadata.checksum, "default-initial",
            "unknown project must fall back to default, never another project"
        );

        let ids = manager.active_project_ids();
        assert_eq!(
            ids,
            vec![
                "analytics".to_string(),
                "billing".to_string(),
                DEFAULT_PROJECT_ID.to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn blank_project_id_collapses_to_default() {
        let manager = CatalogManager::new(manifest("default-initial"));
        let state_blank = manager.active_for("");
        let state_default = manager.active_for(DEFAULT_PROJECT_ID);
        let state_whitespace = manager.active_for("   ");
        assert_eq!(state_blank.metadata.checksum, "default-initial");
        assert_eq!(state_default.metadata.checksum, "default-initial");
        assert_eq!(state_whitespace.metadata.checksum, "default-initial");
    }

    #[tokio::test]
    async fn back_compat_active_and_activate_target_default_project() {
        let manager = CatalogManager::new(manifest("initial"));
        manager
            .stage_catalog(
                manifest("v2"),
                String::new(), // blank → default
                "2.0.0".into(),
                "backward".into(),
            )
            .await
            .unwrap();
        // Pre-U4 single-arg API still targets the default project.
        manager.activate_catalog("v2").await.unwrap();
        assert_eq!(manager.active_metadata().checksum, "v2");
        assert_eq!(manager.active_metadata().project_id, DEFAULT_PROJECT_ID);
    }

    #[tokio::test]
    async fn rollback_drops_only_the_named_projects_staging() {
        let manager = CatalogManager::new(manifest("initial"));
        manager
            .stage_catalog(
                manifest("billing"),
                "billing".into(),
                "1.0.0".into(),
                "backward".into(),
            )
            .await
            .unwrap();
        manager
            .stage_catalog(
                manifest("analytics"),
                "analytics".into(),
                "1.0.0".into(),
                "backward".into(),
            )
            .await
            .unwrap();
        manager.rollback_catalog_for("billing").await.unwrap();
        assert!(manager.staged_for("billing").await.is_none());
        assert!(manager.staged_for("analytics").await.is_some());
    }

    #[tokio::test]
    async fn exact_and_backward_compatibility_are_enforced() {
        let manager = CatalogManager::new(manifest("initial"));
        manager
            .stage_catalog(
                manifest("def456"),
                "prime".to_string(),
                "2.3.1".to_string(),
                "backward".to_string(),
            )
            .await
            .unwrap();
        manager
            .activate_catalog_for("prime", "def456")
            .await
            .unwrap();

        assert!(manager.is_compatible("2.2.0", "prime"));
        assert!(!manager.is_compatible("3.0.0", "prime"));
        // Cross-project mismatch: "other" project has no active catalog,
        // so the compat check falls back to checking against default's
        // version, which won't match "2.2.0".
        assert!(!manager.is_compatible("2.2.0", "other"));

        manager
            .stage_catalog(
                manifest("ghi789"),
                "prime".to_string(),
                "2.3.1".to_string(),
                "exact".to_string(),
            )
            .await
            .unwrap();
        manager
            .activate_catalog_for("prime", "ghi789")
            .await
            .unwrap();
        assert!(manager.is_compatible("2.3.1", "prime"));
        assert!(!manager.is_compatible("2.3.0", "prime"));
    }
}
