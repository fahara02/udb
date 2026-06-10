use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};

use crate::runtime::config::UdbConfig;
use crate::runtime::descriptor_manifest::{
    NativeServiceContract, descriptor_contract_manifest_static,
};

/// Canonical native-service ids, **derived from the embedded descriptor's
/// `native_service` annotations** (proto is the single source of truth). Adding a
/// new annotated native service makes it appear in the registry, health, SDK
/// manifest, CLI scaffolds, docs, and drift output with NO edit to a Rust id list
/// (final_task.md Phase 3 acceptance check). Deterministic (sorted, deduped).
pub fn native_service_ids() -> Vec<String> {
    static IDS: OnceLock<Vec<String>> = OnceLock::new();
    IDS.get_or_init(|| {
        let manifest = descriptor_contract_manifest_static();
        let mut ids: Vec<String> = manifest
            .services
            .iter()
            .filter_map(|service| service.native_service.as_ref())
            .map(|native| canonical_service_id(&native.service_id))
            .collect();
        ids.sort();
        ids.dedup();
        ids
    })
    .clone()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeServiceSurface {
    NativeControlPlane,
    WebRtcPeerPlane,
    BackgroundWorker,
}

impl NativeServiceSurface {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NativeControlPlane => "native_control_plane",
            Self::WebRtcPeerPlane => "webrtc_peer_plane",
            Self::BackgroundWorker => "background_worker",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeListenerKind {
    ControlPlane,
    WebRtcPeer,
    None,
}

impl NativeListenerKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ControlPlane => "control_plane",
            Self::WebRtcPeer => "webrtc_peer",
            Self::None => "none",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeServiceRegistryEntry {
    pub service_id: String,
    pub display_name: String,
    pub proto_services: Vec<String>,
    pub rpc_names: Vec<String>,
    pub surface: NativeServiceSurface,
    pub listener_kind: NativeListenerKind,
    pub owned_proto_packages: Vec<String>,
    pub db_schema_prefixes: Vec<String>,
    pub required_backends: Vec<String>,
    pub optional_backends: Vec<String>,
    pub dependencies: Vec<String>,
    pub migration_owner: bool,
    pub background_workers: Vec<String>,
    /// Whether the proto contract declares this service owns background workers
    /// (`native_service.owns_background_workers`). Explicit contract bit, distinct
    /// from the descriptive `background_workers` name list. Kept in sync by the
    /// `owns_background_workers_matches_background_worker_list` test.
    pub owns_background_workers: bool,
    pub default_enabled: bool,
    pub config_path: String,
    pub env_aliases: Vec<String>,
    pub sdk_facade_name: String,
    pub cli_scaffold_group: String,
    pub security_policy_expectations: Vec<String>,
    pub capability_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeServiceStatus {
    pub service_id: String,
    pub display_name: String,
    pub proto_services: Vec<String>,
    pub enabled: bool,
    pub configured: bool,
    pub mounted: bool,
    pub healthy: bool,
    pub degraded: bool,
    pub surface: String,
    pub listener_kind: String,
    pub supported_rpcs: Vec<String>,
    pub selected_capabilities: Vec<String>,
    pub required_backends: Vec<String>,
    pub missing_dependencies: Vec<String>,
    pub disabled_reason: String,
    pub migration_enabled: bool,
    pub migration_status: String,
    pub owns_background_workers: bool,
    pub background_worker_enabled: bool,
    pub background_workers: Vec<String>,
    pub descriptor_version: String,
    pub enablement_key: String,
    pub service_enablement_key: String,
}

static INSTALLED_NATIVE_SERVICE_CONFIG: OnceLock<Mutex<UdbConfig>> = OnceLock::new();

pub(crate) fn install_native_service_runtime_config(config: &UdbConfig) {
    let cell = INSTALLED_NATIVE_SERVICE_CONFIG.get_or_init(|| Mutex::new(UdbConfig::default()));
    if let Ok(mut guard) = cell.lock() {
        *guard = config.clone();
    }
}

fn current_native_service_runtime_config() -> UdbConfig {
    INSTALLED_NATIVE_SERVICE_CONFIG
        .get()
        .and_then(|cell| cell.lock().ok().map(|guard| guard.clone()))
        .unwrap_or_else(|| UdbConfig {
            native_services: crate::runtime::native_catalog::current_native_services_settings(),
            ..UdbConfig::default()
        })
}

fn installed_native_service_runtime_config() -> Option<UdbConfig> {
    INSTALLED_NATIVE_SERVICE_CONFIG
        .get()
        .and_then(|cell| cell.lock().ok().map(|guard| guard.clone()))
}

pub fn native_service_registry() -> Vec<NativeServiceRegistryEntry> {
    native_service_registry_static().to_vec()
}

/// Process-wide cached registry (mirrors `method_security_registry`): the
/// entries derive solely from the embedded descriptor, so the grouping/build —
/// and the full `FdSet` decode behind it — runs once per process instead of on
/// every gRPC request (`native_service_for_grpc_path` / `native_service_enabled`
/// sit on the per-RPC enforce path). Live-config status overlays stay per-call
/// in [`resolved_native_service_statuses`].
fn native_service_registry_static() -> &'static [NativeServiceRegistryEntry] {
    static REGISTRY: OnceLock<Vec<NativeServiceRegistryEntry>> = OnceLock::new();
    REGISTRY.get_or_init(build_native_service_registry)
}

fn build_native_service_registry() -> Vec<NativeServiceRegistryEntry> {
    let manifest = descriptor_contract_manifest_static();
    let mut grouped: BTreeMap<String, Vec<_>> = BTreeMap::new();
    for service in manifest
        .services
        .iter()
        .filter(|service| service.native_service.is_some())
    {
        let native = service.native_service.as_ref().expect("filtered");
        grouped
            .entry(canonical_service_id(&native.service_id))
            .or_default()
            .push(service);
    }

    // Iterate the descriptor-grouped services directly (BTreeMap → deterministic
    // canonical-id order). No hardcoded id list: membership and order both derive
    // from the descriptor's native_service annotations.
    let mut out = Vec::new();
    for (_service_id, services) in grouped {
        if services.is_empty() {
            continue;
        }
        let native = services[0]
            .native_service
            .as_ref()
            .expect("native metadata");
        let mut proto_services = Vec::new();
        let mut rpc_names = Vec::new();
        let mut packages = BTreeSet::new();
        for service in services {
            proto_services.push(service.full_name());
            packages.insert(service.package.clone());
            for method in &service.methods {
                rpc_names.push(method.method.clone());
            }
        }
        proto_services.sort();
        proto_services.dedup();
        rpc_names.sort();
        rpc_names.dedup();
        out.push(entry_from_contract(
            native,
            proto_services,
            rpc_names,
            packages,
        ));
    }
    out
}

pub fn resolved_native_service_statuses(config: &UdbConfig) -> Vec<NativeServiceStatus> {
    let descriptor_version = crate::runtime::native_catalog::protocol_version().to_string();
    native_service_registry_static()
        .iter()
        .map(|entry| resolve_entry(entry.clone(), config, &descriptor_version))
        .collect()
}

pub fn native_service_for_grpc_path(path: &str) -> Option<String> {
    let path = path.trim_start_matches('/');
    let (service, _) = path.split_once('/')?;
    native_service_registry_static()
        .iter()
        .find(|entry| entry.proto_services.iter().any(|name| name == service))
        .map(|entry| entry.service_id.clone())
}

/// Whether a native service may accept RPC traffic on the current node. This is
/// stricter than enablement: dependency-missing or listener-disabled services
/// are not routable even when their config says `enabled=true`.
pub fn native_service_enabled(service_id: &str) -> bool {
    let Some(config) = installed_native_service_runtime_config() else {
        let fallback = current_native_service_runtime_config();
        return resolved_native_service_statuses(&fallback)
            .into_iter()
            .find(|status| status.service_id == service_id)
            .map(|status| status.enabled && status.disabled_reason != "listener disabled")
            .unwrap_or(false);
    };
    resolved_native_service_statuses(&config)
        .into_iter()
        .find(|status| status.service_id == service_id)
        .map(|status| status.mounted)
        .unwrap_or(false)
}

pub fn any_control_plane_enabled(config: &UdbConfig) -> bool {
    config.native_services.enabled
        && config.native_services.control_plane_enabled
        && resolved_native_service_statuses(config)
            .into_iter()
            .any(|status| {
                status.enabled && status.listener_kind == NativeListenerKind::ControlPlane.as_str()
            })
}

pub fn any_webrtc_peer_enabled(config: &UdbConfig) -> bool {
    config.native_services.enabled
        && config.native_services.webrtc_peer_enabled
        && resolved_native_service_statuses(config)
            .into_iter()
            .any(|status| {
                status.enabled && status.listener_kind == NativeListenerKind::WebRtcPeer.as_str()
            })
}

pub fn migration_enabled_service_ids(config: &UdbConfig) -> BTreeSet<String> {
    resolved_native_service_statuses(config)
        .into_iter()
        .filter(|status| status.migration_enabled)
        .map(|status| status.service_id)
        .collect()
}

fn entry_from_contract(
    native: &NativeServiceContract,
    proto_services: Vec<String>,
    rpc_names: Vec<String>,
    packages: BTreeSet<String>,
) -> NativeServiceRegistryEntry {
    let service_id = canonical_service_id(&native.service_id);
    let required_backends = required_backends(native);
    let surface = surface(native);
    let listener_kind = listener_kind(native);
    let schema = schema_prefix(&service_id);
    NativeServiceRegistryEntry {
        service_id: service_id.clone(),
        display_name: if native.display_name.is_empty() {
            service_id.clone()
        } else {
            native.display_name.clone()
        },
        proto_services,
        rpc_names,
        surface,
        listener_kind,
        owned_proto_packages: packages.into_iter().collect(),
        db_schema_prefixes: vec![schema],
        required_backends,
        optional_backends: Vec::new(),
        dependencies: Vec::new(),
        migration_owner: true,
        background_workers: background_workers(&service_id),
        owns_background_workers: native.owns_background_workers,
        default_enabled: native.default_enabled,
        config_path: format!("native_services.services.{service_id}"),
        env_aliases: env_aliases(&service_id),
        sdk_facade_name: native.sdk_facade_name.clone(),
        cli_scaffold_group: native.cli_scaffold_group.clone(),
        security_policy_expectations: vec!["endpoint_security".to_string()],
        capability_ref: native.capability_ref.clone(),
    }
}

fn resolve_entry(
    entry: NativeServiceRegistryEntry,
    config: &UdbConfig,
    descriptor_version: &str,
) -> NativeServiceStatus {
    let override_cfg = config.native_services.services.get(&entry.service_id);
    let configured = override_cfg.is_some() || config.native_services.default_enabled;
    let enabled = config.native_services.enabled
        && override_cfg
            .and_then(|cfg| cfg.enabled)
            .unwrap_or(config.native_services.default_enabled && entry.default_enabled);
    let migration_enabled = config.native_services.migrate_enabled
        && override_cfg
            .and_then(|cfg| cfg.migrate)
            .unwrap_or(enabled && entry.migration_owner);
    let required_backends = override_cfg
        .filter(|cfg| !cfg.required_backends.is_empty())
        .map(|cfg| cfg.required_backends.clone())
        .unwrap_or_else(|| entry.required_backends.clone());
    let missing_dependencies = missing_dependencies(config, &required_backends);
    let listener_enabled = match entry.listener_kind {
        NativeListenerKind::ControlPlane => config.native_services.control_plane_enabled,
        NativeListenerKind::WebRtcPeer => config.native_services.webrtc_peer_enabled,
        NativeListenerKind::None => true,
    };
    let mounted = enabled && listener_enabled && missing_dependencies.is_empty();
    let degraded = enabled && !missing_dependencies.is_empty();
    let disabled_reason = disabled_reason(config, enabled, listener_enabled, &missing_dependencies);
    NativeServiceStatus {
        service_id: entry.service_id.clone(),
        display_name: entry.display_name,
        proto_services: entry.proto_services,
        enabled,
        configured,
        mounted,
        healthy: mounted && !degraded,
        degraded,
        surface: entry.surface.as_str().to_string(),
        listener_kind: entry.listener_kind.as_str().to_string(),
        supported_rpcs: entry.rpc_names,
        selected_capabilities: if entry.capability_ref.is_empty() {
            Vec::new()
        } else {
            vec![entry.capability_ref]
        },
        required_backends,
        missing_dependencies,
        disabled_reason,
        migration_enabled,
        migration_status: if migration_enabled {
            "enabled".to_string()
        } else {
            "disabled".to_string()
        },
        owns_background_workers: entry.owns_background_workers,
        background_worker_enabled: mounted && entry.owns_background_workers,
        background_workers: entry.background_workers,
        descriptor_version: descriptor_version.to_string(),
        enablement_key: "UDB_NATIVE_SERVICES_ENABLED".to_string(),
        service_enablement_key: format!(
            "UDB_NATIVE_{}_ENABLED",
            entry.service_id.to_ascii_uppercase()
        ),
    }
}

fn required_backends(native: &NativeServiceContract) -> Vec<String> {
    let mut out = Vec::new();
    if native.requires_postgres {
        out.push("postgres".to_string());
    }
    if native.requires_redis {
        out.push("redis".to_string());
    }
    if native.requires_object_store {
        out.push("object_storage".to_string());
    }
    if native.requires_kafka {
        out.push("kafka".to_string());
    }
    out
}

fn missing_dependencies(config: &UdbConfig, required_backends: &[String]) -> Vec<String> {
    required_backends
        .iter()
        .filter(|backend| match backend.as_str() {
            "postgres" => !config.primary.is_configured() && config.primary.direct_dsn.is_empty(),
            "redis" => !config.has_redis(),
            "object_storage" | "s3" | "minio" => !config.has_minio(),
            "kafka" => config
                .kafka_brokers
                .as_deref()
                .unwrap_or_default()
                .trim()
                .is_empty(),
            _ => false,
        })
        .cloned()
        .collect()
}

fn disabled_reason(
    config: &UdbConfig,
    enabled: bool,
    listener_enabled: bool,
    missing_dependencies: &[String],
) -> String {
    if !config.native_services.enabled {
        return "native_services.enabled=false".to_string();
    }
    if !enabled {
        return "service disabled".to_string();
    }
    if !listener_enabled {
        return "listener disabled".to_string();
    }
    if !missing_dependencies.is_empty() {
        return format!("missing dependencies: {}", missing_dependencies.join(","));
    }
    String::new()
}

fn surface(native: &NativeServiceContract) -> NativeServiceSurface {
    if native.peer_listener_allowed {
        NativeServiceSurface::WebRtcPeerPlane
    } else {
        NativeServiceSurface::NativeControlPlane
    }
}

fn listener_kind(native: &NativeServiceContract) -> NativeListenerKind {
    if native.peer_listener_allowed {
        NativeListenerKind::WebRtcPeer
    } else if native.control_plane_listener_allowed {
        NativeListenerKind::ControlPlane
    } else {
        NativeListenerKind::None
    }
}

fn schema_prefix(service_id: &str) -> String {
    match service_id {
        "apikey" => "udb_authn".to_string(),
        "webrtc_room" | "webrtc_peer" | "webrtc_track" | "webrtc_turn" | "webrtc_signaling" => {
            "udb_webrtc".to_string()
        }
        other => format!("udb_{other}"),
    }
}

fn background_workers(service_id: &str) -> Vec<String> {
    match service_id {
        "storage" => vec!["storage_orphan_reaper".to_string()],
        "webrtc_turn" => vec!["turn_credential_rotation".to_string()],
        "webrtc_signaling" => vec!["sfu_signaling_helpers".to_string()],
        _ => Vec::new(),
    }
}

fn env_aliases(service_id: &str) -> Vec<String> {
    let upper = service_id.to_ascii_uppercase();
    vec![
        "UDB_NATIVE_SERVICES_ENABLED".to_string(),
        "UDB_NATIVE_SERVICES".to_string(),
        format!("UDB_NATIVE_{upper}_ENABLED"),
        format!("UDB_NATIVE_{upper}_MIGRATE_ENABLED"),
    ]
}

/// Canonical native-service id: dotted proto ids (`webrtc.room`) and CLI/registry
/// underscore ids (`webrtc_room`) both normalize to the underscore form, so
/// descriptor lookups and env-key generation agree regardless of source.
pub fn canonical_service_id(service_id: &str) -> String {
    service_id.replace(['.', '-'], "_")
}

pub fn status_to_proto(status: NativeServiceStatus) -> crate::proto::NativeServiceStatus {
    crate::proto::NativeServiceStatus {
        service_id: status.service_id,
        proto_service_names: status.proto_services,
        enabled: status.enabled,
        configured: status.configured,
        mounted: status.mounted,
        healthy: status.healthy,
        degraded: status.degraded,
        surface: status.surface,
        listener_kind: status.listener_kind,
        supported_rpcs: status.supported_rpcs,
        capabilities: status.selected_capabilities,
        required_backends: status.required_backends,
        missing_dependencies: status.missing_dependencies,
        disabled_reason: status.disabled_reason,
        migration_status: status.migration_status,
        descriptor_version: status.descriptor_version,
        owns_background_workers: status.owns_background_workers,
        background_worker_enabled: status.background_worker_enabled,
        background_workers: status.background_workers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::config::{NativeServiceConfig, NativeServicesSettings};

    #[test]
    fn registry_is_complete_against_native_descriptor_services() {
        // The registry and the derived id list must be mutually consistent: every
        // descriptor native_service id appears as a registry entry, and vice versa.
        let registry_ids: BTreeSet<String> = native_service_registry()
            .into_iter()
            .map(|entry| entry.service_id)
            .collect();
        let derived_ids: BTreeSet<String> = native_service_ids().into_iter().collect();
        assert_eq!(
            registry_ids, derived_ids,
            "registry ids and descriptor-derived native_service_ids() must match exactly"
        );
        // Sanity floor: the core native services must be present (guards against an
        // empty/degenerate descriptor silently emptying the registry).
        for expected in ["authn", "authz", "storage", "asset", "webrtc_room"] {
            assert!(
                derived_ids.contains(expected),
                "missing core native service id {expected}"
            );
        }
    }

    #[test]
    fn global_disable_disables_serving_and_migration_status() {
        let config = UdbConfig {
            native_services: NativeServicesSettings {
                enabled: false,
                ..NativeServicesSettings::default()
            },
            ..UdbConfig::default()
        };
        let statuses = resolved_native_service_statuses(&config);
        assert!(statuses.iter().all(|status| !status.enabled));
        assert!(statuses.iter().all(|status| !status.mounted));
    }

    // final_task.md §8 acceptance "Health/capabilities match actual mounted
    // routes": the health report must be internally coherent — a service reported
    // as mounted must carry the surface metadata that says WHERE it is mounted, a
    // migration-enabled service must report a migration status, and a degraded
    // service must be honest about WHY (reason or missing dependency). This keeps
    // capability reporting from claiming a mounted route without describing it.
    #[test]
    fn health_statuses_are_coherent_with_mounted_surface() {
        let statuses = resolved_native_service_statuses(&UdbConfig::default());
        assert!(
            !statuses.is_empty(),
            "expected the descriptor to yield native service statuses"
        );
        for status in &statuses {
            if status.mounted {
                assert!(
                    !status.surface.is_empty() && !status.listener_kind.is_empty(),
                    "mounted service '{}' must report a surface + listener_kind",
                    status.service_id
                );
            }
            if status.migration_enabled {
                assert!(
                    !status.migration_status.is_empty(),
                    "migration-enabled service '{}' must report a migration_status",
                    status.service_id
                );
            }
            if status.degraded {
                assert!(
                    !status.disabled_reason.is_empty() || !status.missing_dependencies.is_empty(),
                    "degraded service '{}' must report a reason or missing dependency",
                    status.service_id
                );
            }
            if status.owns_background_workers {
                assert!(
                    !status.background_workers.is_empty(),
                    "worker-owning service '{}' must report worker names",
                    status.service_id
                );
            }
            assert_eq!(
                status.background_worker_enabled,
                status.mounted && status.owns_background_workers,
                "service '{}' worker enablement must follow mounted worker-owner status",
                status.service_id
            );
        }
    }

    // final_task.md §8 acceptance "Health/capabilities match actual mounted
    // routes": the set of gRPC service descriptors the registry claims to mount
    // (the union of every entry's `proto_services`) must equal the set of native
    // gRPC services the descriptor actually carries (`native_service` annotated).
    // Both derive from the embedded descriptor, so this proves the registry neither
    // drops a native gRPC service it should mount nor invents a route for a service
    // the descriptor does not declare — without standing up a server. The runtime
    // `add_service(...)` wiring in mod.rs mounts exactly these proto services.
    #[test]
    fn registry_mounted_proto_services_match_native_descriptor_surface() {
        // Registry side: every gRPC service name the native registry owns.
        let registry_proto_services: BTreeSet<String> = native_service_registry()
            .into_iter()
            .flat_map(|entry| entry.proto_services)
            .collect();

        // Descriptor side: every gRPC service that carries a `native_service`
        // annotation (i.e. is a native, mountable service).
        let manifest = descriptor_contract_manifest_static();
        let descriptor_native_services: BTreeSet<String> = manifest
            .services
            .iter()
            .filter(|service| service.native_service.is_some())
            .map(|service| service.full_name())
            .collect();

        assert!(
            !registry_proto_services.is_empty(),
            "registry must claim to mount at least one native gRPC service"
        );
        assert_eq!(
            registry_proto_services, descriptor_native_services,
            "the registry's mounted gRPC service set must equal the descriptor's \
             native_service-annotated gRPC services (no dropped or invented routes)"
        );
    }

    // final_task.md §8 acceptance "[~] background worker enabled": the explicit
    // contract bit `owns_background_workers` (from the proto `native_service`
    // option) must agree with the descriptive `background_workers` name list, so a
    // service cannot claim worker ownership in the contract while exposing no worker
    // (or run a worker it never declared). Keeps worker enablement an explicit,
    // self-consistent part of the surface rather than implicit runtime behavior.
    #[test]
    fn owns_background_workers_matches_background_worker_list() {
        for entry in native_service_registry() {
            assert_eq!(
                entry.owns_background_workers,
                !entry.background_workers.is_empty(),
                "service '{}' declares owns_background_workers={} but its background_workers \
                 list is {:?}; the contract bit and the worker list must agree",
                entry.service_id,
                entry.owns_background_workers,
                entry.background_workers
            );
        }
    }

    #[test]
    fn per_service_disable_and_migration_only_are_distinct() {
        let config = UdbConfig {
            native_services: NativeServicesSettings {
                default_enabled: true,
                services: [(
                    "storage".to_string(),
                    NativeServiceConfig {
                        enabled: Some(false),
                        migrate: Some(true),
                        ..NativeServiceConfig::default()
                    },
                )]
                .into_iter()
                .collect(),
                ..NativeServicesSettings::default()
            },
            ..UdbConfig::default()
        };
        let storage = resolved_native_service_statuses(&config)
            .into_iter()
            .find(|status| status.service_id == "storage")
            .expect("storage status");
        assert!(!storage.enabled);
        assert!(!storage.mounted);
        assert!(storage.migration_enabled);
        assert_eq!(storage.migration_status, "enabled");
    }

    #[test]
    fn serving_only_reports_degraded_when_dependency_is_missing() {
        let config = UdbConfig {
            native_services: NativeServicesSettings {
                services: [(
                    "storage".to_string(),
                    NativeServiceConfig {
                        enabled: Some(true),
                        migrate: Some(false),
                        ..NativeServiceConfig::default()
                    },
                )]
                .into_iter()
                .collect(),
                ..NativeServicesSettings::default()
            },
            ..UdbConfig::default()
        };
        let storage = resolved_native_service_statuses(&config)
            .into_iter()
            .find(|status| status.service_id == "storage")
            .expect("storage status");
        assert!(storage.enabled);
        assert!(!storage.mounted);
        assert!(!storage.migration_enabled);
        assert!(storage.degraded);
        assert!(
            storage
                .missing_dependencies
                .iter()
                .any(|dep| dep == "postgres")
        );
    }
}
