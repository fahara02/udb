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
use udb::proto::udb::core::authz::entity::v1 as authz_entity_pb;
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
        AuthzCommand::Check {
            user,
            object,
            action,
            tenant,
            purpose,
            credential,
            role,
            project,
            brief,
        } => {
            if user.trim().is_empty() || object.trim().is_empty() || action.trim().is_empty() {
                return Err(
                    "provide <subject> <action> <resource> (or --user <id> --action <RPC name, \
                     e.g. Select> --object <message type or native RPC>)"
                        .to_string(),
                );
            }
            let tenant = first_non_empty(&tenant, "UDB_TENANT_ID");
            // A project-bound rule only matches a question asked in its
            // project; without one the broker evaluates "no project".
            let project = first_non_empty(&project, "UDB_PROJECT_ID");
            // Credential-type admission is decided before any policy, from the
            // RPC's descriptor; report it offline so a credential-type refusal
            // is never chased as a missing grant.
            let credential_rules =
                explain_credential_rules(object.trim(), action.trim(), &credential);
            let request = authz_pb::CheckAccessRequest {
                user_id: user.trim().to_string(),
                domain: tenant.clone(),
                object: object.trim().to_string(),
                action: action.trim().to_string(),
                purpose: purpose.trim().to_string(),
                tenant_id: tenant.clone(),
                project_id: project.clone(),
                ..Default::default()
            };
            let mut client = authz_client()
                .await
                .map_err(|err| explain_offline_error(&err, &credential_rules))?;
            let response = client
                .check_access(with_metadata(request))
                .await
                .map_err(|err| {
                    explain_offline_error(&format!("authz check failed: {err}"), &credential_rules)
                })?
                .into_inner();
            Ok(render_check(
                &response,
                ExplainView {
                    target: ExplainTarget {
                        user: user.trim(),
                        role: role.trim(),
                        tenant: &tenant,
                        action: action.trim(),
                        object: object.trim(),
                    },
                    project: &project,
                    credential_rules: &credential_rules,
                    brief,
                },
            ))
        }
        AuthzCommand::PolicyPut(mut rule) => {
            rule.tenant = first_non_empty(&rule.tenant, "UDB_TENANT_ID");
            let record = policy_record_from_rule(&rule)?;
            let mut client = authz_client().await?;
            let response = client
                .put_authz_policy(with_tenant_metadata(
                    authz_pb::PutAuthzPolicyRequest {
                        policy: Some(record.clone()),
                    },
                    &record.tenant,
                ))
                .await
                .map_err(|err| format!("policy put failed: {err}"))?
                .into_inner();
            Ok(serde_json::json!({
                "ok": response.ok,
                "message": response.message,
                "policy": policy_record_json(&record),
            }))
        }
        AuthzCommand::PolicyList {
            tenant,
            subject,
            object,
            include_inactive,
        } => {
            let tenant = first_non_empty(&tenant, "UDB_TENANT_ID");
            if tenant.is_empty() {
                return Err(
                    "pass --tenant <tenant uuid> (or '*' for the platform-wide rules)".to_string(),
                );
            }
            let mut client = authz_client().await?;
            let response = client
                .list_policy_rules(with_tenant_metadata(
                    authz_pb::ListPolicyRulesRequest {
                        domain: tenant.clone(),
                        subject: subject.trim().to_string(),
                        object: object.trim().to_string(),
                        active_only: !include_inactive,
                        page: None,
                    },
                    &tenant,
                ))
                .await
                .map_err(|err| format!("policy list failed: {err}"))?
                .into_inner();
            let policies: Vec<serde_json::Value> =
                response.policies.iter().map(policy_rule_json).collect();
            Ok(serde_json::json!({
                "tenant": tenant,
                "count": policies.len(),
                "policies": policies,
            }))
        }
        AuthzCommand::PolicyDelete { id, tenant } => {
            if id.trim().is_empty() {
                return Err(
                    "provide the policy id: udb authz policy delete <policy id>".to_string()
                );
            }
            let tenant = first_non_empty(&tenant, "UDB_TENANT_ID");
            let mut client = authz_client().await?;
            let response = client
                .delete_policy_rule(with_tenant_metadata(
                    authz_pb::DeletePolicyRuleRequest {
                        policy_id: id.trim().to_string(),
                        deleted_by: String::new(),
                    },
                    &tenant,
                ))
                .await
                .map_err(|err| format!("policy delete failed: {err}"))?
                .into_inner();
            Ok(serde_json::json!({
                "policy_id": id.trim(),
                "deleted": response.deleted,
                "message": if response.deleted {
                    "the rule is soft-deleted; every broker drops it at its next snapshot reload"
                } else {
                    "no active rule with that id in the caller's tenant (already deleted, or another tenant's)"
                },
            }))
        }
    }
}

/// How many matched policy ids `udb authz explain` prints before it
/// summarises the rest: a broad tenant evaluates thousands of candidates.
const EXPLAIN_POLICY_IDS_SHOWN: usize = 20;

/// The first [`EXPLAIN_POLICY_IDS_SHOWN`] ids, then `… N more`.
fn capped_policy_ids(ids: &[String]) -> Vec<String> {
    let mut shown: Vec<String> = ids.iter().take(EXPLAIN_POLICY_IDS_SHOWN).cloned().collect();
    if ids.len() > EXPLAIN_POLICY_IDS_SHOWN {
        shown.push(format!("… {} more", ids.len() - EXPLAIN_POLICY_IDS_SHOWN));
    }
    shown
}

/// A non-admin CheckAccess is answered for the CALLER, whatever subject the
/// request names (only a service account holding
/// `udb:authz:check-access-for` may ask about a person of its tenant). When the
/// broker evaluated someone else, the decision says nothing about the subject
/// that was asked about, so say so instead of presenting it as theirs.
fn evaluated_other_subject(requested: &str, evaluated: &str) -> Option<String> {
    let (requested, evaluated) = (requested.trim(), evaluated.trim());
    if evaluated.is_empty() || evaluated.eq_ignore_ascii_case(requested) {
        return None;
    }
    Some(format!(
        "the broker evaluated '{evaluated}' (the caller of UDB_AUTH_TOKEN), not '{requested}': a \
         caller that is not a cross-tenant admin is always answered for itself, so this decision \
         says nothing about '{requested}'. Run it again with UDB_AUTH_TOKEN set to a platform-admin \
         token, or to a token of '{requested}' itself"
    ))
}

#[derive(Clone, Copy)]
struct ExplainView<'a> {
    target: ExplainTarget<'a>,
    project: &'a str,
    credential_rules: &'a [udb::runtime::service::RpcAdmission],
    brief: bool,
}

/// The `udb authz explain|check` output. Pure, so it is tested on canned
/// responses.
fn render_check(
    response: &authz_pb::CheckAccessResponse,
    view: ExplainView<'_>,
) -> serde_json::Value {
    let decision = response.decision.clone().unwrap_or_default();
    let other = evaluated_other_subject(view.target.user, &response.evaluated_subject);
    let next_step = match &other {
        Some(note) => note.clone(),
        None => explain_next_step(response.allowed, view.credential_rules, view.target),
    };
    let evaluated = if response.evaluated_subject.trim().is_empty() {
        view.target.user.to_string()
    } else {
        response.evaluated_subject.clone()
    };
    if view.brief {
        return serde_json::json!({
            "allowed": response.allowed,
            "user": view.target.user,
            "evaluated_subject": evaluated,
            "matched_rule": response.matched_rule,
            "reason": response.reason,
            "next_step": next_step,
        });
    }
    serde_json::json!({
        "allowed": response.allowed,
        "user": view.target.user,
        "evaluated_subject": evaluated,
        "tenant": view.target.tenant,
        "project": view.project,
        "object": view.target.object,
        "action": view.target.action,
        "matched_rule": response.matched_rule,
        "reason": response.reason,
        "deny_reason": decision.deny_reason,
        "matched_policy_count": decision.matched_policy_ids.len(),
        "matched_policy_ids": capped_policy_ids(&decision.matched_policy_ids),
        "required_scopes": decision.required_scopes,
        "credential_rules": view.credential_rules,
        "next_step": next_step,
    })
}

/// Build the PutAuthzPolicy record for `udb authz policy put`. Every value is
/// checked here and refused when malformed: nothing is guessed or defaulted
/// into a different rule. Without `--id` the id is derived from the rule, so
/// putting the same rule twice writes one row.
fn policy_record_from_rule(rule: &AuthzPolicyRule) -> Result<authz_pb::AuthzPolicyRecord, String> {
    let tenant = rule.tenant.trim();
    if tenant.is_empty() {
        return Err(
            "pass --tenant <tenant uuid> (or '*' for a rule in every tenant, which needs a \
             cross-tenant admin token)"
                .to_string(),
        );
    }
    if rule.action.trim().is_empty() || rule.resource.trim().is_empty() {
        return Err(
            "pass --action <RPC name, e.g. Select> and --resource <message type or *>".to_string(),
        );
    }
    let effect = rule.effect.trim().to_ascii_uppercase();
    if effect != "ALLOW" && effect != "DENY" {
        return Err(format!(
            "--effect must be ALLOW or DENY, got '{}'",
            rule.effect.trim()
        ));
    }
    let priority = if rule.priority.trim().is_empty() {
        0
    } else {
        rule.priority.trim().parse::<i32>().map_err(|_| {
            format!(
                "--priority must be an integer, got '{}'",
                rule.priority.trim()
            )
        })?
    };
    let mut conditions = std::collections::BTreeMap::new();
    for raw in &rule.conditions {
        let Some((key, value)) = raw.split_once('=') else {
            return Err(format!("--condition must be key=value, got '{raw}'"));
        };
        if key.trim().is_empty() {
            return Err(format!("--condition needs a key, got '{raw}'"));
        }
        conditions.insert(key.trim().to_string(), value.trim().to_string());
    }
    let mut required_scopes: Vec<String> = rule
        .required_scopes
        .iter()
        .map(|scope| scope.trim().to_string())
        .filter(|scope| !scope.is_empty())
        .collect();
    required_scopes.sort_unstable();
    required_scopes.dedup();
    let mut record = authz_pb::AuthzPolicyRecord {
        id: rule.id.trim().to_string(),
        priority,
        enabled: rule.enabled,
        effect,
        tenant: tenant.to_string(),
        project: rule.project.trim().to_string(),
        subject: rule.subject.trim().to_string(),
        role: rule.role.trim().to_string(),
        action: rule.action.trim().to_string(),
        resource: rule.resource.trim().to_string(),
        purpose: rule.purpose.trim().to_string(),
        relationship: rule.relationship.trim().to_string(),
        conditions: conditions.into_iter().collect(),
        required_scopes,
    };
    if record.id.is_empty() {
        record.id = derived_policy_id(&record);
    }
    Ok(record)
}

/// A stable UUID for a rule: the same rule always gets the same id.
pub(super) fn derived_policy_id(record: &authz_pb::AuthzPolicyRecord) -> String {
    use sha2::{Digest, Sha256};
    let mut conditions: Vec<String> = record
        .conditions
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect();
    conditions.sort_unstable();
    let priority = record.priority.to_string();
    let conditions = conditions.join("&");
    let scopes = record.required_scopes.join(",");
    let key = [
        record.tenant.as_str(),
        record.project.as_str(),
        record.subject.as_str(),
        record.role.as_str(),
        record.action.as_str(),
        record.resource.as_str(),
        record.effect.as_str(),
        record.purpose.as_str(),
        record.relationship.as_str(),
        priority.as_str(),
        conditions.as_str(),
        scopes.as_str(),
    ]
    .join("|");
    let digest = Sha256::digest(format!("udb-authz-policy-put|{key}").as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Uuid::from_bytes(bytes).to_string()
}

/// A written record in the policy-file vocabulary (`udb policy apply`).
fn policy_record_json(record: &authz_pb::AuthzPolicyRecord) -> serde_json::Value {
    let conditions: std::collections::BTreeMap<&String, &String> =
        record.conditions.iter().collect();
    serde_json::json!({
        "id": record.id,
        "tenant": record.tenant,
        "project": record.project,
        "subject": record.subject,
        "role": record.role,
        "action": record.action,
        "resource": record.resource,
        "effect": record.effect,
        "purpose": record.purpose,
        "relationship": record.relationship,
        "priority": record.priority,
        "conditions": conditions,
        "required_scopes": record.required_scopes,
        "enabled": record.enabled,
    })
}

/// A stored `PolicyRule` row in the policy-file vocabulary: the rule metadata
/// kept in `attributes_json` (role, purpose, scopes, priority, relationship)
/// is lifted out, the rest are the rule's attribute conditions.
fn policy_rule_json(rule: &authz_entity_pb::PolicyRule) -> serde_json::Value {
    let mut attributes: serde_json::Map<String, serde_json::Value> =
        match serde_json::from_str::<serde_json::Value>(&rule.attributes_json) {
            Ok(serde_json::Value::Object(map)) => map,
            _ => serde_json::Map::new(),
        };
    let mut take = |key: &str| -> String {
        match attributes.remove(key) {
            Some(serde_json::Value::String(value)) => value,
            Some(serde_json::Value::Null) | None => String::new(),
            Some(other) => other.to_string(),
        }
    };
    let role = take("role");
    let purpose = take("purpose");
    let relationship = take("relationship");
    let priority = take("priority");
    let required_scopes: Vec<String> = take("required_scopes")
        .split([',', ' '])
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
        .map(ToString::to_string)
        .collect();
    serde_json::json!({
        "id": rule.policy_id,
        "tenant": rule.tenant_id,
        "project": rule.project_id,
        "subject": rule.subject,
        "role": role,
        "action": rule.action,
        "resource": rule.object,
        "effect": if rule.effect == authz_entity_pb::PolicyEffect::Deny as i32 { "DENY" } else { "ALLOW" },
        "purpose": purpose,
        "relationship": relationship,
        // A stored priority that is not a number is shown as stored.
        "priority": match priority.trim() {
            "" => serde_json::Value::from(0),
            raw => raw
                .parse::<i64>()
                .map(serde_json::Value::from)
                .unwrap_or_else(|_| serde_json::Value::String(raw.to_string())),
        },
        "conditions": attributes,
        "required_scopes": required_scopes,
        "enabled": rule.is_active,
    })
}

/// [`with_metadata`], with `x-tenant-id` naming `tenant` when it is a concrete
/// tenant (the platform domain `*` is not a tenant id and is not sent).
fn with_tenant_metadata<T>(message: T, tenant: &str) -> Request<T> {
    let mut request = with_metadata(message);
    let tenant = tenant.trim();
    if !tenant.is_empty() && tenant != "*" {
        match tenant.parse() {
            Ok(value) => {
                request.metadata_mut().insert("x-tenant-id", value);
            }
            Err(err) => eprintln!(
                "warning: --tenant is not a valid gRPC metadata value; x-tenant-id not sent: {err}"
            ),
        }
    }
    request
}

/// The native RPC(s) the explained resource (or action) names, with their
/// credential-type admission. A data-plane message type matches nothing.
fn explain_credential_rules(
    object: &str,
    action: &str,
    credential: &str,
) -> Vec<udb::runtime::service::RpcAdmission> {
    let object = object.trim_start_matches("native.rpc:");
    let by_object = udb::runtime::service::describe_rpc_admission(object, credential);
    if !by_object.is_empty() {
        return by_object;
    }
    // A bare action such as `Select` would match unrelated native methods;
    // only an explicit `Service/Method` action is looked up.
    if action.contains('/') {
        return udb::runtime::service::describe_rpc_admission(action, credential);
    }
    Vec::new()
}

/// When the broker cannot be asked, still print the offline credential rule.
fn explain_offline_error(
    err: &str,
    credential_rules: &[udb::runtime::service::RpcAdmission],
) -> String {
    let rules: Vec<&str> = credential_rules
        .iter()
        .map(|rule| rule.explanation.as_str())
        .collect();
    if rules.is_empty() {
        err.to_string()
    } else {
        format!("{err}. Credential rule (offline): {}", rules.join(" | "))
    }
}

#[derive(Clone, Copy)]
struct ExplainTarget<'a> {
    user: &'a str,
    role: &'a str,
    tenant: &'a str,
    action: &'a str,
    object: &'a str,
}

/// The single next step for an explained decision: the credential rule when
/// it refuses the caller's credential type, else the grant (and the binding
/// when the grant names a role).
fn explain_next_step(
    allowed: bool,
    credential_rules: &[udb::runtime::service::RpcAdmission],
    target: ExplainTarget<'_>,
) -> String {
    if let Some(refused) = credential_rules
        .iter()
        .find(|rule| rule.credential_admitted == Some(false))
    {
        return refused.explanation.clone();
    }
    if allowed {
        return "allowed: nothing to do".to_string();
    }
    let object = target.object.trim_start_matches("native.rpc:");
    let (grantee, binding) = if target.role.is_empty() {
        (
            udb::runtime::authz::Grantee::Subject(target.user),
            String::new(),
        )
    } else {
        (
            udb::runtime::authz::Grantee::Role(target.role),
            format!(
                "; then bind the subject to the role: `udb auth role bind --principal {} --role {} \
                 --tenant {}`",
                target.user,
                target.role,
                if target.tenant.is_empty() {
                    "<tenant-uuid>"
                } else {
                    target.tenant
                }
            ),
        )
    };
    format!(
        "{}{binding}",
        udb::runtime::authz::grant_command_hint(target.tenant, grantee, target.action, object)
    )
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
pub(super) fn auth_target() -> String {
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

pub(super) fn client_target_addr(addr: &str) -> String {
    let addr = addr.trim();
    if let Some(port) = addr.strip_prefix(&format!("{DEFAULT_GRPC_BIND_HOST}:")) {
        format!("{DEFAULT_GRPC_TARGET_HOST}:{port}")
    } else {
        addr.to_string()
    }
}

/// Give one CLI request its own correlation id. Most native RPCs declare
/// `request_context_required`, and the broker refuses them without
/// `x-correlation-id`, `x-request-id` or `traceparent`.
pub(super) fn attach_correlation_id<T>(request: &mut Request<T>) {
    request.metadata_mut().insert(
        "x-correlation-id",
        format!("udb-cli-{}", uuid::Uuid::new_v4())
            .parse()
            .expect("generated correlation id is valid gRPC metadata"),
    );
}

/// The one request builder for CLI calls to the broker: a correlation id, the
/// bearer token and the control-plane identity headers from the environment. The
/// bearer claim is the authoritative caller identity the server authorizes
/// against.
pub(super) fn with_metadata<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    attach_correlation_id(&mut request);
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
    fn explain_takes_subject_action_resource_positionally() {
        let args: Vec<String> = [
            "authz",
            "explain",
            "svc:lessons",
            "PutFlag",
            "ConfigService/PutFlag",
            "--credential",
            "SERVICE_ACCOUNT",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let (command, _, _, _) = parse_args(&args);
        match command {
            Command::Authz(AuthzCommand::Check {
                user,
                action,
                object,
                credential,
                ..
            }) => {
                assert_eq!(user, "svc:lessons");
                assert_eq!(action, "PutFlag");
                assert_eq!(object, "ConfigService/PutFlag");
                assert_eq!(credential, "SERVICE_ACCOUNT");
            }
            _ => panic!("explain parses as Check"),
        }
    }

    #[test]
    fn explain_names_the_credential_rule_before_any_grant() {
        let rules = explain_credential_rules(
            "native.rpc:AuthzService/CreatePolicyRule",
            "CreatePolicyRule",
            "SERVICE_ACCOUNT",
        );
        assert_eq!(rules.len(), 1, "{rules:?}");
        let step = explain_next_step(
            false,
            &rules,
            ExplainTarget {
                user: "svc:ops",
                role: "",
                tenant: "t-1",
                action: "CreatePolicyRule",
                object: "AuthzService/CreatePolicyRule",
            },
        );
        assert!(step.contains("SERVICE_ACCOUNT is NOT admitted"), "{step}");

        // A data-plane miss names the grant and, with --role, the binding.
        let step = explain_next_step(
            false,
            &[],
            ExplainTarget {
                user: "svc:lessons",
                role: "teacher",
                tenant: "t-1",
                action: "Upsert",
                object: "acme.v1.Lesson",
            },
        );
        assert!(
            step.contains("{role: \"teacher\", action: \"Upsert\", resource: \"acme.v1.Lesson\""),
            "{step}"
        );
        assert!(
            step.contains("udb policy apply -f <policies.yaml> --tenant t-1"),
            "{step}"
        );
        assert!(
            step.contains("udb auth role bind --principal svc:lessons --role teacher --tenant t-1"),
            "{step}"
        );
        // Message types never match native RPC descriptors.
        assert!(explain_credential_rules("acme.v1.Lesson", "Upsert", "").is_empty());
    }

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

    fn strings(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn explain_parses_project_and_brief() {
        let args = strings(&[
            "authz",
            "explain",
            "b6e0d1c2-7a3b-4c5d-9e8f-001122334455",
            "seed-persona",
            "list:roots",
            "--tenant",
            "t-1",
            "--project",
            "meddesk",
            "--purpose",
            "meddesk.persona",
            "--brief",
        ]);
        match parse_args(&args).0 {
            Command::Authz(AuthzCommand::Check {
                user,
                project,
                brief,
                purpose,
                ..
            }) => {
                assert_eq!(user, "b6e0d1c2-7a3b-4c5d-9e8f-001122334455");
                assert_eq!(project, "meddesk");
                assert_eq!(purpose, "meddesk.persona");
                assert!(brief);
            }
            _ => panic!("explain parses as Check"),
        }
    }

    fn explain_view<'a>(
        user: &'a str,
        rules: &'a [udb::runtime::service::RpcAdmission],
        brief: bool,
    ) -> ExplainView<'a> {
        ExplainView {
            target: ExplainTarget {
                user,
                role: "",
                tenant: "t-1",
                action: "seed-persona",
                object: "list:roots",
            },
            project: "meddesk",
            credential_rules: rules,
            brief,
        }
    }

    /// Thousands of evaluated candidates print as the first 20 and a count.
    #[test]
    fn explain_caps_matched_policy_ids() {
        let ids: Vec<String> = (0..2500).map(|i| format!("p-{i}")).collect();
        let shown = capped_policy_ids(&ids);
        assert_eq!(shown.len(), EXPLAIN_POLICY_IDS_SHOWN + 1);
        assert_eq!(shown[0], "p-0");
        assert_eq!(shown.last().unwrap(), "… 2480 more");
        assert_eq!(capped_policy_ids(&ids[..3]), ids[..3].to_vec());

        let response = authz_pb::CheckAccessResponse {
            allowed: false,
            evaluated_subject: "sa".to_string(),
            decision: Some(authz_pb::Decision {
                matched_policy_ids: ids,
                ..Default::default()
            }),
            ..Default::default()
        };
        let full = render_check(&response, explain_view("sa", &[], false));
        assert_eq!(full["matched_policy_count"], 2500);
        assert_eq!(
            full["matched_policy_ids"].as_array().unwrap().len(),
            EXPLAIN_POLICY_IDS_SHOWN + 1
        );
        assert_eq!(full["project"], "meddesk");
        let brief = render_check(&response, explain_view("sa", &[], true));
        let keys: Vec<&String> = brief.as_object().unwrap().keys().collect();
        assert!(
            !keys.iter().any(|key| key.as_str() == "matched_policy_ids"),
            "{keys:?}"
        );
        assert!(brief.get("next_step").is_some());
    }

    /// A non-admin token is answered for its own caller: explain must not
    /// present that decision as the asked-about subject's.
    #[test]
    fn explain_says_when_the_broker_evaluated_the_caller_instead() {
        let response = authz_pb::CheckAccessResponse {
            allowed: false,
            evaluated_subject: "the-admin-human".to_string(),
            reason: "no allow rule grants action".to_string(),
            ..Default::default()
        };
        let rendered = render_check(&response, explain_view("the-service-account", &[], false));
        assert_eq!(rendered["evaluated_subject"], "the-admin-human");
        let step = rendered["next_step"].as_str().unwrap();
        assert!(
            step.contains("evaluated 'the-admin-human'")
                && step.contains("says nothing about 'the-service-account'"),
            "{step}"
        );
        assert!(evaluated_other_subject("SA", "sa").is_none());
        assert!(evaluated_other_subject("sa", "").is_none());
    }

    fn put_rule() -> AuthzPolicyRule {
        AuthzPolicyRule {
            tenant: "t-1".to_string(),
            project: "meddesk".to_string(),
            subject: "b6e0d1c2-7a3b-4c5d-9e8f-001122334455".to_string(),
            action: "seed-persona".to_string(),
            resource: "list:roots".to_string(),
            purpose: "meddesk.persona".to_string(),
            effect: "allow".to_string(),
            enabled: true,
            ..Default::default()
        }
    }

    #[test]
    fn policy_put_builds_a_checked_record_with_a_stable_id() {
        let record = policy_record_from_rule(&put_rule()).expect("valid rule");
        assert_eq!(record.effect, "ALLOW");
        assert_eq!(record.priority, 0);
        assert!(uuid::Uuid::parse_str(&record.id).is_ok(), "{}", record.id);
        assert_eq!(
            policy_record_from_rule(&put_rule()).unwrap().id,
            record.id,
            "the same rule gets the same id"
        );
        let mut other = put_rule();
        other.action = "Select".to_string();
        assert_ne!(policy_record_from_rule(&other).unwrap().id, record.id);

        let mut explicit = put_rule();
        explicit.id = "c41d9e7a-0000-4000-8000-00000000beef".to_string();
        explicit.priority = "10".to_string();
        explicit.conditions = strings(&["department = cardiology"]);
        explicit.required_scopes = strings(&["b", " a ", "a"]);
        let record = policy_record_from_rule(&explicit).unwrap();
        assert_eq!(record.id, "c41d9e7a-0000-4000-8000-00000000beef");
        assert_eq!(record.priority, 10);
        assert_eq!(
            record.conditions.get("department").map(String::as_str),
            Some("cardiology")
        );
        assert_eq!(record.required_scopes, strings(&["a", "b"]));

        let mut wildcard = put_rule();
        wildcard.tenant = "*".to_string();
        assert_eq!(policy_record_from_rule(&wildcard).unwrap().tenant, "*");

        for (field, broken) in [
            (
                "tenant",
                AuthzPolicyRule {
                    tenant: " ".into(),
                    ..put_rule()
                },
            ),
            (
                "action",
                AuthzPolicyRule {
                    action: String::new(),
                    ..put_rule()
                },
            ),
            (
                "effect",
                AuthzPolicyRule {
                    effect: "permit".into(),
                    ..put_rule()
                },
            ),
            (
                "priority",
                AuthzPolicyRule {
                    priority: "high".into(),
                    ..put_rule()
                },
            ),
            (
                "condition",
                AuthzPolicyRule {
                    conditions: strings(&["no-equals"]),
                    ..put_rule()
                },
            ),
        ] {
            assert!(
                policy_record_from_rule(&broken).is_err(),
                "a malformed {field} must be refused, not defaulted"
            );
        }
    }

    #[test]
    fn policy_subcommands_parse() {
        let args = strings(&[
            "authz",
            "policy",
            "put",
            "--tenant",
            "*",
            "--action",
            "Select",
            "--resource",
            "*",
            "--role",
            "auditor",
            "--scope",
            "udb:read,udb:audit",
            "--condition",
            "a=b",
            "--condition",
            "c=d",
            "--disabled",
        ]);
        match parse_args(&args).0 {
            Command::Authz(AuthzCommand::PolicyPut(rule)) => {
                assert_eq!(rule.tenant, "*");
                assert_eq!(rule.role, "auditor");
                assert_eq!(rule.effect, "ALLOW");
                assert_eq!(rule.required_scopes, strings(&["udb:read", "udb:audit"]));
                assert_eq!(rule.conditions, strings(&["a=b", "c=d"]));
                assert!(!rule.enabled);
            }
            _ => panic!("authz policy put"),
        }
        match parse_args(&strings(&[
            "authz", "policy", "list", "--tenant", "t-1", "--all",
        ]))
        .0
        {
            Command::Authz(AuthzCommand::PolicyList {
                tenant,
                include_inactive,
                ..
            }) => {
                assert_eq!(tenant, "t-1");
                assert!(include_inactive);
            }
            _ => panic!("authz policy list"),
        }
        match parse_args(&strings(&["authz", "policy", "delete", "p-1"])).0 {
            Command::Authz(AuthzCommand::PolicyDelete { id, .. }) => assert_eq!(id, "p-1"),
            _ => panic!("authz policy delete"),
        }
    }

    #[test]
    fn listed_rules_use_the_policy_file_vocabulary() {
        let rule = authz_entity_pb::PolicyRule {
            policy_id: "p-1".to_string(),
            subject: "sa".to_string(),
            object: "list:roots".to_string(),
            action: "seed-persona".to_string(),
            effect: authz_entity_pb::PolicyEffect::Allow as i32,
            is_active: true,
            tenant_id: "t-1".to_string(),
            project_id: "meddesk".to_string(),
            attributes_json: r#"{"role":"","purpose":"meddesk.persona","required_scopes":"","priority":"5","ward":"3"}"#.to_string(),
            ..Default::default()
        };
        let json = policy_rule_json(&rule);
        assert_eq!(json["resource"], "list:roots");
        assert_eq!(json["purpose"], "meddesk.persona");
        assert_eq!(json["role"], "");
        assert_eq!(json["priority"], 5);
        assert_eq!(json["effect"], "ALLOW");
        assert_eq!(json["required_scopes"], serde_json::json!([]));
        assert_eq!(json["conditions"], serde_json::json!({"ward": "3"}));
    }
}
