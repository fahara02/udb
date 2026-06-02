//! SDK-generation RPC manifest.
//!
//! `udb sdk generate` renders per-language client layers from templates, and the
//! list of RPCs it iterates is derived here from the *same* embedded
//! `FileDescriptorSet` the gRPC reflection service serves
//! ([`crate::runtime::native_catalog::embedded_file_descriptor_set`]). Proto stays
//! the single source of truth: there is no second, hand-maintained RPC list that
//! could drift from the wire contract.
//!
//! Only UDB services (package starting with `udb`) are surfaced; the vendored
//! `google.*` descriptors carry no services and are skipped regardless.

use prost::Message;
use prost_types::FileDescriptorSet;

/// One RPC method, flattened with its owning service for easy per-RPC template
/// iteration. All names are the raw proto identifiers; mapping them to a
/// language's generated type names (e.g. Python `types_pb2.SelectRequest`, Go
/// `entityv1.SelectRequest`) is the template's job, since that mapping is
/// codegen-specific.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcDescriptor {
    /// Service short name, e.g. `DataBroker`.
    pub service_name: String,
    /// Service package, e.g. `udb.services.v1`.
    pub service_pkg: String,
    /// Method name, e.g. `Select`.
    pub method: String,
    /// `method` in snake_case, e.g. `select`.
    pub method_snake: String,
    /// Request message short name, e.g. `SelectRequest`.
    pub input_short: String,
    /// Request message package, e.g. `udb.entity.v1`.
    pub input_pkg: String,
    /// Response message short name.
    pub output_short: String,
    /// Response message package.
    pub output_pkg: String,
    pub client_streaming: bool,
    pub server_streaming: bool,
}

impl RpcDescriptor {
    /// Fully-qualified service name, e.g. `udb.services.v1.DataBroker`.
    pub fn service_full(&self) -> String {
        format!("{}.{}", self.service_pkg, self.service_name)
    }

    /// gRPC method path, e.g. `/udb.services.v1.DataBroker/Select`.
    pub fn grpc_path(&self) -> String {
        format!("/{}/{}", self.service_full(), self.method)
    }

    /// Streaming kind token used by template block filters.
    pub fn kind(&self) -> &'static str {
        match (self.client_streaming, self.server_streaming) {
            (false, false) => "unary",
            (false, true) => "server_streaming",
            (true, false) => "client_streaming",
            (true, true) => "bidi",
        }
    }
}

/// All UDB RPCs across every embedded service, sorted by
/// `(service_full, method)` for deterministic codegen output.
pub fn rpc_manifest() -> Vec<RpcDescriptor> {
    let bytes = crate::runtime::native_catalog::embedded_file_descriptor_set();
    let set = match FileDescriptorSet::decode(bytes) {
        Ok(set) => set,
        Err(err) => {
            tracing::error!(error = %err, "failed to decode embedded descriptor set");
            return Vec::new();
        }
    };

    let mut out = Vec::new();
    for file in &set.file {
        let pkg = file.package();
        if !pkg.starts_with("udb") {
            continue; // skip vendored google.* descriptors
        }
        for service in &file.service {
            let service_name = service.name().to_string();
            for method in &service.method {
                let (input_pkg, input_short) = split_type(method.input_type());
                let (output_pkg, output_short) = split_type(method.output_type());
                out.push(RpcDescriptor {
                    service_name: service_name.clone(),
                    service_pkg: pkg.to_string(),
                    method: method.name().to_string(),
                    method_snake: to_snake(method.name()),
                    input_short,
                    input_pkg,
                    output_short,
                    output_pkg,
                    client_streaming: method.client_streaming(),
                    server_streaming: method.server_streaming(),
                });
            }
        }
    }
    out.sort_by(|a, b| {
        a.service_full()
            .cmp(&b.service_full())
            .then_with(|| a.method.cmp(&b.method))
    });
    out
}

/// Split a fully-qualified proto type reference (`.udb.entity.v1.SelectRequest`)
/// into `(package, short_name)` = `("udb.entity.v1", "SelectRequest")`. A bare
/// name with no package yields `("", name)`.
fn split_type(fq: &str) -> (String, String) {
    let trimmed = fq.strip_prefix('.').unwrap_or(fq);
    match trimmed.rsplit_once('.') {
        Some((pkg, short)) => (pkg.to_string(), short.to_string()),
        None => (String::new(), trimmed.to_string()),
    }
}

/// PascalCase/camelCase → snake_case (`GetHealthReport` → `get_health_report`,
/// `SelectV2` → `select_v2`).
fn to_snake(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for (i, ch) in name.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if i != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_is_derived_from_embedded_descriptors() {
        let manifest = rpc_manifest();
        assert!(
            !manifest.is_empty(),
            "embedded descriptor set yielded no RPCs"
        );
        // Every entry must be a UDB service with a non-empty method + types.
        for rpc in &manifest {
            assert!(rpc.service_pkg.starts_with("udb"), "non-udb service leaked");
            assert!(!rpc.method.is_empty());
            assert!(!rpc.input_short.is_empty());
            assert!(!rpc.output_short.is_empty());
        }
    }

    #[test]
    fn data_broker_select_surface_is_present() {
        let manifest = rpc_manifest();
        let select = manifest
            .iter()
            .find(|r| r.service_name == "DataBroker" && r.method == "Select")
            .expect("DataBroker/Select must be in the manifest");
        assert_eq!(select.service_pkg, "udb.services.v1");
        assert_eq!(select.method_snake, "select");
        assert_eq!(select.input_short, "SelectRequest");
        assert_eq!(select.kind(), "unary");
        assert_eq!(select.grpc_path(), "/udb.services.v1.DataBroker/Select");
    }

    #[test]
    fn native_control_plane_services_are_present() {
        let manifest = rpc_manifest();
        let services: std::collections::BTreeSet<String> =
            manifest.iter().map(|r| r.service_full()).collect();
        // The six native control-plane services + the public broker.
        for expected in [
            "udb.services.v1.DataBroker",
            "udb.core.authn.services.v1.AuthnService",
            "udb.core.authz.services.v1.AuthzService",
            "udb.core.apikey.services.v1.ApiKeyService",
            "udb.core.tenant.services.v1.TenantService",
            "udb.core.notification.services.v1.NotificationService",
            "udb.core.analytics.services.v1.AnalyticsService",
        ] {
            assert!(
                services.iter().any(|s| s == expected),
                "expected service {expected} not found in manifest; present: {services:?}"
            );
        }
    }

    #[test]
    fn server_streaming_select_v2_is_flagged() {
        let manifest = rpc_manifest();
        if let Some(v2) = manifest
            .iter()
            .find(|r| r.service_name == "DataBroker" && r.method == "SelectV2")
        {
            assert!(v2.server_streaming, "SelectV2 should be server-streaming");
            assert_eq!(v2.kind(), "server_streaming");
        }
    }
}
