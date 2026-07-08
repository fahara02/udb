//! Native authz governance CLI commands.
//!
//! Surfaces the policy-governance UX over the generated `AuthzService` client.
//! `udb authz simulate` answers "what would change if this policy bundle
//! shipped?" by calling the EXISTING server-side simulation model
//! (`AuthzService.SimulatePolicy`) over the control-plane gRPC target. The CLI
//! never re-implements the policy engine and never re-validates a bundle
//! signature client-side: bundle signing is enforced server-side in enterprise
//! mode, so any rejection is rendered VERBATIM from the server's gRPC status
//! (a second client-side check over the Base64 signature would only diverge).

use super::*;
use serde::Deserialize;
use std::collections::HashMap;
use tonic::Request;
use tonic::transport::Channel;
use udb::proto::udb::core::authz::services::v1 as authz_pb;
use udb::proto::udb::core::authz::services::v1::authz_service_client::AuthzServiceClient;

/// Control-plane metadata headers forwarded from the environment, mirroring the
/// `auth` CLI so `udb authz` dials the same target with the same identity hints.
const METADATA_HEADERS: &[(&str, &str)] = &[
    ("x-tenant-id", "UDB_TENANT_ID"),
    ("x-udb-project-id", "UDB_PROJECT_ID"),
    ("x-user-id", "UDB_USER_ID"),
    ("x-service-identity", "UDB_SERVICE_IDENTITY"),
    ("x-scopes", "UDB_SCOPES"),
    ("x-purpose", "UDB_PURPOSE"),
];

pub(crate) fn run_authz_command(command: AuthzCommand) -> i32 {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("authz: failed to create tokio runtime: {err}");
            return 1;
        }
    };
    match runtime.block_on(run_authz_command_async(command)) {
        Ok(value) => {
            output_json(&value, "authz command result");
            0
        }
        Err(err) => {
            // Server-side rejections (including unsigned/invalid policy bundles in
            // enterprise mode) are surfaced VERBATIM here — the CLI never decides
            // a bundle is invalid on its own.
            eprintln!("authz: {err}");
            1
        }
    }
}

async fn run_authz_command_async(command: AuthzCommand) -> Result<serde_json::Value, String> {
    match command {
        AuthzCommand::Simulate {
            bundle,
            draft_id,
            tenant,
            project,
            persist,
            policy_version_id,
        } => {
            if bundle.trim().is_empty() && draft_id.trim().is_empty() {
                return Err(
                    "provide --bundle <path> (candidate policy document + cases) and/or \
                     --draft <id> (a stored draft to evaluate)"
                        .to_string(),
                );
            }

            // Load the candidate bundle (policies/bindings/tuples + simulation
            // cases). The cases drive the diff; the candidate document is what
            // "would ship". An empty --bundle is allowed only when --draft names a
            // stored draft AND the operator passes cases another way — but in
            // practice the cases live in the bundle, so require it when present.
            let input = if bundle.trim().is_empty() {
                BundleInput::default()
            } else {
                let raw = std::fs::read_to_string(bundle.trim())
                    .map_err(|err| format!("failed to read bundle '{}': {err}", bundle.trim()))?;
                parse_bundle(&raw)?
            };
            if input.cases.is_empty() {
                return Err(
                    "bundle declares no simulation `cases`; add at least one case \
                     (principal + resource + action) to evaluate the policy change"
                        .to_string(),
                );
            }

            let tenant = first_non_empty(&tenant, "UDB_TENANT_ID");
            let project = first_non_empty(&project, "UDB_PROJECT_ID");

            // Identity hints only — the AUTHORITATIVE caller identity comes from
            // the verified bearer claim attached in `with_metadata`. These fields
            // satisfy the in-process/standing-scope fallback when no claim is
            // present, exactly like the other governance callers.
            let actor = governance_actor(&tenant, &project);

            // `draft_id` (a stored draft) takes precedence server-side; otherwise
            // the in-memory `candidate` document is evaluated. Either way the
            // server runs its own PolicyEngine and never mutates durable state.
            let request = authz_pb::SimulatePolicyRequest {
                actor: Some(actor),
                tenant_id: tenant,
                project_id: project,
                draft_id: draft_id.trim().to_string(),
                candidate: if draft_id.trim().is_empty() {
                    Some(input.candidate_document())
                } else {
                    None
                },
                cases: input.cases.iter().map(BundleCase::to_proto).collect(),
                persist,
                policy_version_id: policy_version_id.trim().to_string(),
            };

            let mut client = authz_client().await?;
            let response = client
                .simulate_policy(with_metadata(request))
                .await
                .map_err(|err| format!("policy simulate failed: {err}"))?
                .into_inner();

            Ok(render_simulation(&response))
        }
    }
}

/// Render a [`SimulatePolicyResponse`] into the CLI diff view: a per-case
/// allow/deny delta plus an aggregate "what would change" summary. Pure so it
/// can be unit-tested against a canned response with no live gRPC.
fn render_simulation(response: &authz_pb::SimulatePolicyResponse) -> serde_json::Value {
    let cases: Vec<serde_json::Value> = response
        .results
        .iter()
        .map(|result| {
            let active = result.active_decision.as_ref();
            let draft = result.draft_decision.as_ref();
            serde_json::json!({
                "label": result.label,
                "changed": result.changed,
                "active_allowed": active.map(|d| d.allowed),
                "draft_allowed": draft.map(|d| d.allowed),
                "active_deny_reason": active.map(|d| d.deny_reason.clone()).unwrap_or_default(),
                "draft_deny_reason": draft.map(|d| d.deny_reason.clone()).unwrap_or_default(),
            })
        })
        .collect();
    let changed: Vec<&authz_pb::SimulationResult> =
        response.results.iter().filter(|r| r.changed).collect();
    // "added" = newly allowed by the candidate; "removed" = newly denied.
    let newly_allowed = changed
        .iter()
        .filter(|r| {
            r.draft_decision
                .as_ref()
                .map(|d| d.allowed)
                .unwrap_or(false)
        })
        .count();
    let newly_denied = changed.len() - newly_allowed;
    serde_json::json!({
        "total_cases": response.results.len(),
        "changed_count": changed.len(),
        "newly_allowed": newly_allowed,
        "newly_denied": newly_denied,
        "cases": cases,
        "diff_json": response.diff_json,
    })
}

/// Parse a policy-simulation bundle JSON document into its candidate policy
/// document + simulation cases. Pure (no IO) so it is unit-tested directly.
fn parse_bundle(raw: &str) -> Result<BundleInput, String> {
    serde_json::from_str::<BundleInput>(raw)
        .map_err(|err| format!("failed to parse simulation bundle JSON: {err}"))
}

fn first_non_empty(flag: &str, env_name: &str) -> String {
    if !flag.trim().is_empty() {
        return flag.trim().to_string();
    }
    env::var(env_name).unwrap_or_default()
}

/// Identity-hint actor mirroring `auth`'s `request_context()`: subject/scopes
/// from the environment. NOT authoritative — the verified bearer claim wins.
fn governance_actor(tenant: &str, project: &str) -> authz_pb::GovernanceActor {
    authz_pb::GovernanceActor {
        subject: env::var("UDB_PRINCIPAL_ID")
            .or_else(|_| env::var("UDB_USER_ID"))
            .unwrap_or_default(),
        tenant_id: tenant.to_string(),
        project_id: project.to_string(),
        scopes: env::var("UDB_SCOPES")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|scope| !scope.is_empty())
            .map(ToString::to_string)
            .collect(),
        ..Default::default()
    }
}

async fn authz_client() -> Result<AuthzServiceClient<Channel>, String> {
    AuthzServiceClient::connect(auth_target())
        .await
        .map_err(|err| format!("failed to connect to authz service: {err}"))
}

/// Resolve the control-plane gRPC target exactly like the `auth` CLI: honor
/// `UDB_AUTH_TARGET` / `UDB_GRPC_TARGET` / `UDB_GRPC_ADDR`, rewrite a bind host
/// to the loopback target host, and default to the standard target address.
fn auth_target() -> String {
    let raw = env::var("UDB_AUTH_TARGET")
        .or_else(|_| env::var("UDB_GRPC_TARGET"))
        .or_else(|_| env::var("UDB_GRPC_ADDR"))
        .map(|addr| client_target_addr(&addr))
        .unwrap_or_else(|_| DEFAULT_GRPC_TARGET_ADDR.to_string());
    if raw.starts_with("http://") || raw.starts_with("https://") {
        raw
    } else {
        format!("http://{raw}")
    }
}

fn client_target_addr(addr: &str) -> String {
    let addr = addr.trim();
    if let Some(port) = addr.strip_prefix(&format!("{DEFAULT_GRPC_BIND_HOST}:")) {
        format!("{DEFAULT_GRPC_TARGET_HOST}:{port}")
    } else {
        addr.to_string()
    }
}

/// Attach the bearer token + control-plane identity headers from the
/// environment, mirroring the `auth` CLI. The bearer claim is the authoritative
/// caller identity the server authorizes against.
fn with_metadata<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    if let Ok(token) = env::var("UDB_AUTH_TOKEN").or_else(|_| env::var("UDB_BEARER_TOKEN")) {
        let token = token.trim();
        if !token.is_empty() {
            if token.bytes().any(|b| matches!(b, b'\r' | b'\n')) {
                eprintln!(
                    "warning: UDB_AUTH_TOKEN/UDB_BEARER_TOKEN contains a newline and was not sent"
                );
            } else if let Ok(value) = format!("Bearer {token}").parse() {
                request.metadata_mut().insert("authorization", value);
            } else {
                eprintln!(
                    "warning: UDB_AUTH_TOKEN/UDB_BEARER_TOKEN is not a valid gRPC header value; \
                     the request will be sent UNAUTHENTICATED"
                );
            }
        }
    }
    for (key, env_name) in METADATA_HEADERS {
        if let Ok(raw) = env::var(env_name) {
            if !raw.trim().is_empty() {
                match raw.parse() {
                    Ok(value) => {
                        request.metadata_mut().insert(*key, value);
                    }
                    Err(err) => {
                        eprintln!(
                            "warning: {env_name} is not a valid gRPC metadata value for header \
                             '{key}'; dropping it: {err}"
                        );
                    }
                }
            }
        }
    }
    request
}

// ── Bundle input model (serde) ──────────────────────────────────────────────
// The proto types are prost-generated and carry no serde derives, so the bundle
// file is parsed into these mirror structs and mapped into the request. Every
// field defaults, so a minimal bundle (just `cases`) is valid.

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundleInput {
    #[serde(default)]
    policies: Vec<BundlePolicy>,
    #[serde(default)]
    role_bindings: Vec<BundleRoleBinding>,
    #[serde(default)]
    relationship_tuples: Vec<BundleTuple>,
    #[serde(default)]
    cases: Vec<BundleCase>,
}

impl BundleInput {
    fn candidate_document(&self) -> authz_pb::PolicyDocument {
        authz_pb::PolicyDocument {
            policies: self.policies.iter().map(BundlePolicy::to_proto).collect(),
            role_bindings: self
                .role_bindings
                .iter()
                .map(BundleRoleBinding::to_proto)
                .collect(),
            relationship_tuples: self
                .relationship_tuples
                .iter()
                .map(BundleTuple::to_proto)
                .collect(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundlePolicy {
    #[serde(default)]
    id: String,
    #[serde(default)]
    priority: i32,
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default)]
    effect: String,
    #[serde(default)]
    tenant: String,
    #[serde(default)]
    project: String,
    #[serde(default)]
    subject: String,
    #[serde(default)]
    role: String,
    #[serde(default)]
    action: String,
    #[serde(default)]
    resource: String,
    #[serde(default)]
    purpose: String,
    #[serde(default)]
    relationship: String,
    #[serde(default)]
    conditions: HashMap<String, String>,
    #[serde(default)]
    required_scopes: Vec<String>,
}

impl BundlePolicy {
    fn to_proto(&self) -> authz_pb::AuthzPolicyRecord {
        authz_pb::AuthzPolicyRecord {
            id: self.id.clone(),
            priority: self.priority,
            enabled: self.enabled,
            effect: self.effect.clone(),
            tenant: self.tenant.clone(),
            project: self.project.clone(),
            subject: self.subject.clone(),
            role: self.role.clone(),
            action: self.action.clone(),
            resource: self.resource.clone(),
            purpose: self.purpose.clone(),
            relationship: self.relationship.clone(),
            conditions: self.conditions.clone(),
            required_scopes: self.required_scopes.clone(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundleRoleBinding {
    #[serde(default)]
    subject: String,
    #[serde(default)]
    role: String,
    #[serde(default)]
    tenant: String,
    #[serde(default)]
    project: String,
    #[serde(default)]
    expires_at_unix: i64,
    #[serde(default)]
    source: String,
}

impl BundleRoleBinding {
    fn to_proto(&self) -> authz_pb::RoleBinding {
        authz_pb::RoleBinding {
            subject: self.subject.clone(),
            role: self.role.clone(),
            tenant: self.tenant.clone(),
            project: self.project.clone(),
            expires_at_unix: self.expires_at_unix,
            source: self.source.clone(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundleTuple {
    #[serde(default)]
    subject: String,
    #[serde(default)]
    relation: String,
    #[serde(default)]
    object: String,
    #[serde(default)]
    tenant: String,
    #[serde(default)]
    project: String,
    #[serde(default)]
    version: i64,
    #[serde(default)]
    expires_at_unix: i64,
    #[serde(default)]
    source: String,
}

impl BundleTuple {
    fn to_proto(&self) -> authz_pb::RelationshipTuple {
        authz_pb::RelationshipTuple {
            subject: self.subject.clone(),
            relation: self.relation.clone(),
            object: self.object.clone(),
            tenant: self.tenant.clone(),
            project: self.project.clone(),
            version: self.version,
            expires_at_unix: self.expires_at_unix,
            source: self.source.clone(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundleCase {
    #[serde(default)]
    label: String,
    #[serde(default)]
    action: String,
    #[serde(default)]
    purpose: String,
    #[serde(default)]
    principal: Option<BundlePrincipal>,
    #[serde(default)]
    resource: Option<BundleResource>,
    #[serde(default)]
    attributes: HashMap<String, String>,
}

impl BundleCase {
    fn to_proto(&self) -> authz_pb::SimulationCase {
        authz_pb::SimulationCase {
            principal: self.principal.as_ref().map(BundlePrincipal::to_proto),
            resource: self.resource.as_ref().map(BundleResource::to_proto),
            action: self.action.clone(),
            purpose: self.purpose.clone(),
            attributes: self.attributes.clone(),
            label: self.label.clone(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundlePrincipal {
    #[serde(default)]
    principal_id: String,
    #[serde(default)]
    subject: String,
    #[serde(default)]
    user_id: String,
    #[serde(default)]
    service_identity: String,
    #[serde(default)]
    tenant_id: String,
    #[serde(default)]
    project_id: String,
    #[serde(default)]
    scopes: Vec<String>,
    #[serde(default)]
    roles: Vec<String>,
    #[serde(default)]
    domain: String,
    #[serde(default)]
    attributes: HashMap<String, String>,
}

impl BundlePrincipal {
    fn to_proto(&self) -> authz_pb::Principal {
        authz_pb::Principal {
            principal_id: self.principal_id.clone(),
            subject: self.subject.clone(),
            user_id: self.user_id.clone(),
            service_identity: self.service_identity.clone(),
            tenant_id: self.tenant_id.clone(),
            project_id: self.project_id.clone(),
            scopes: self.scopes.clone(),
            roles: self.roles.clone(),
            domain: self.domain.clone(),
            attributes: self.attributes.clone(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundleResource {
    #[serde(default)]
    resource_type: String,
    #[serde(default)]
    resource_name: String,
    #[serde(default)]
    message_type: String,
    #[serde(default)]
    schema: String,
    #[serde(default)]
    table: String,
    #[serde(default)]
    backend: String,
    #[serde(default)]
    resource_id: String,
    #[serde(default)]
    tenant_id: String,
    #[serde(default)]
    project_id: String,
    #[serde(default)]
    attributes: HashMap<String, String>,
}

impl BundleResource {
    fn to_proto(&self) -> authz_pb::ResourceRef {
        authz_pb::ResourceRef {
            resource_type: self.resource_type.clone(),
            resource_name: self.resource_name.clone(),
            message_type: self.message_type.clone(),
            schema: self.schema.clone(),
            table: self.table.clone(),
            backend: self.backend.clone(),
            resource_id: self.resource_id.clone(),
            tenant_id: self.tenant_id.clone(),
            project_id: self.project_id.clone(),
            attributes: self.attributes.clone(),
            ..Default::default()
        }
    }
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bundle_maps_candidate_and_cases() {
        let raw = r#"{
            "policies": [
                {"id": "p1", "effect": "ALLOW", "subject": "alice", "action": "read", "resource": "Invoice"}
            ],
            "cases": [
                {"label": "alice-reads-invoice", "action": "read",
                 "principal": {"subject": "alice", "scopes": ["invoice:read"]},
                 "resource": {"message_type": "Invoice"}}
            ]
        }"#;
        let input = parse_bundle(raw).expect("bundle parses");
        assert_eq!(input.policies.len(), 1);
        assert_eq!(input.cases.len(), 1);

        let doc = input.candidate_document();
        assert_eq!(doc.policies.len(), 1);
        assert_eq!(doc.policies[0].id, "p1");
        // `enabled` defaults to true when omitted from the bundle.
        assert!(doc.policies[0].enabled);

        let case = input.cases[0].to_proto();
        assert_eq!(case.label, "alice-reads-invoice");
        assert_eq!(case.action, "read");
        assert_eq!(case.principal.expect("principal").subject, "alice");
        assert_eq!(case.resource.expect("resource").message_type, "Invoice");
    }

    #[test]
    fn parse_bundle_rejects_unknown_fields() {
        let raw = r#"{"cazes": []}"#;
        assert!(parse_bundle(raw).is_err());
    }

    #[test]
    fn render_simulation_counts_added_and_removed_decisions() {
        let response = authz_pb::SimulatePolicyResponse {
            results: vec![
                // Unchanged.
                authz_pb::SimulationResult {
                    label: "stable".to_string(),
                    active_decision: Some(authz_pb::Decision {
                        allowed: true,
                        ..Default::default()
                    }),
                    draft_decision: Some(authz_pb::Decision {
                        allowed: true,
                        ..Default::default()
                    }),
                    changed: false,
                    diff_json: String::new(),
                },
                // Newly allowed by the candidate (deny -> allow).
                authz_pb::SimulationResult {
                    label: "grant".to_string(),
                    active_decision: Some(authz_pb::Decision {
                        allowed: false,
                        deny_reason: "no policy".to_string(),
                        ..Default::default()
                    }),
                    draft_decision: Some(authz_pb::Decision {
                        allowed: true,
                        ..Default::default()
                    }),
                    changed: true,
                    diff_json: String::new(),
                },
                // Newly denied by the candidate (allow -> deny).
                authz_pb::SimulationResult {
                    label: "revoke".to_string(),
                    active_decision: Some(authz_pb::Decision {
                        allowed: true,
                        ..Default::default()
                    }),
                    draft_decision: Some(authz_pb::Decision {
                        allowed: false,
                        deny_reason: "removed".to_string(),
                        ..Default::default()
                    }),
                    changed: true,
                    diff_json: String::new(),
                },
            ],
            diff_json: "{\"cases\":[]}".to_string(),
        };

        let rendered = render_simulation(&response);
        assert_eq!(rendered["total_cases"], 3);
        assert_eq!(rendered["changed_count"], 2);
        assert_eq!(rendered["newly_allowed"], 1);
        assert_eq!(rendered["newly_denied"], 1);
        assert_eq!(rendered["cases"][1]["label"], "grant");
        assert_eq!(rendered["cases"][1]["draft_allowed"], true);
        assert_eq!(rendered["cases"][2]["draft_deny_reason"], "removed");
    }
}
