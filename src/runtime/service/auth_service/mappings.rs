//! proto ↔ runtime conversions and Postgres row helpers shared by the
//! `authn` and `authz` handler modules (Milestone 2).

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};
use sqlx::Row;
use tonic::Status;

use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authz::entity::v1 as authz_entity_pb;
use crate::proto::udb::core::authz::services::v1 as authz_pb;
use crate::proto::udb::core::common::v1 as common_pb;

use super::super::native_helpers::{native_page_response, native_page_window};
use crate::runtime::authn::{self, SessionRecord};
use crate::runtime::authz::{AuthzPolicy, Decision, Effect, Principal, ResourceRef};
pub(super) fn authn_principal_to_pb(p: &Principal, expires_at_unix: i64) -> authn_pb::Principal {
    authn_pb::Principal {
        principal_id: p.principal_id.clone(),
        subject: p.subject.clone(),
        user_id: p.user_id.clone(),
        service_identity: p.service_identity.clone(),
        tenant_id: p.tenant_id.clone(),
        project_id: p.project_id.clone(),
        scopes: p.scopes.clone(),
        roles: p.roles.clone(),
        provider_id: p.provider_id.clone(),
        auth_method: p.auth_method.clone(),
        expires_at_unix,
        // AccountKind unspecified (tag 0): the runtime Principal does not classify
        // human/service/external accounts yet.
        account_kind: 0,
        // authz domain string — the runtime Principal scopes via tenant/project,
        // so no separate domain is carried.
        domain: String::new(),
        // No free-form principal attributes are tracked at runtime yet.
        attributes: std::collections::HashMap::new(),
    }
}

pub(super) fn authz_principal_to_runtime(p: &authz_pb::Principal) -> Principal {
    Principal {
        principal_id: p.principal_id.clone(),
        subject: p.subject.clone(),
        user_id: p.user_id.clone(),
        service_identity: p.service_identity.clone(),
        tenant_id: p.tenant_id.clone(),
        project_id: p.project_id.clone(),
        scopes: p.scopes.clone(),
        roles: p.roles.clone(),
        provider_id: p.provider_id.clone(),
        auth_method: p.auth_method.clone(),
    }
}

pub(super) fn resource_to_runtime(r: &authz_pb::ResourceRef) -> ResourceRef {
    ResourceRef {
        resource_type: r.resource_type.clone(),
        resource_name: r.resource_name.clone(),
        message_type: r.message_type.clone(),
        schema: r.schema.clone(),
        table: r.table.clone(),
        backend: r.backend.clone(),
        instance: r.instance.clone(),
    }
}

pub(super) fn effect_to_entity(effect: Effect) -> i32 {
    match effect {
        Effect::Allow => authz_entity_pb::PolicyEffect::Allow as i32,
        Effect::Deny => authz_entity_pb::PolicyEffect::Deny as i32,
    }
}

pub(super) fn entity_effect_to_runtime(effect: i32) -> Result<Effect, Status> {
    match authz_entity_pb::PolicyEffect::try_from(effect).unwrap_or_default() {
        authz_entity_pb::PolicyEffect::Allow => Ok(Effect::Allow),
        authz_entity_pb::PolicyEffect::Deny => Ok(Effect::Deny),
        authz_entity_pb::PolicyEffect::Unspecified => {
            Err(crate::runtime::executor_utils::invalid_argument_fields(
                "policy effect is required",
                [("effect", "must be either ALLOW or DENY")],
            ))
        }
    }
}

pub(super) fn decision_to_pb(d: &Decision) -> authz_pb::Decision {
    authz_pb::Decision {
        decision_id: d.decision_id.clone(),
        allowed: d.allowed,
        effect: d.effect.as_str().to_string(),
        deny_reason: d.deny_reason.clone(),
        matched_policy_ids: d.matched_policy_ids.clone(),
        required_scopes: d.required_scopes.clone(),
        policy_version: d.policy_version.clone(),
        relationship_version: d.relationship_version.clone(),
        cache_ttl_seconds: d.cache_ttl_seconds,
        audit_required: d.audit_required,
    }
}

pub(super) fn policy_to_rule_pb(p: &AuthzPolicy) -> authz_entity_pb::PolicyRule {
    authz_entity_pb::PolicyRule {
        policy_id: p.id.clone(),
        subject: p.subject.clone(),
        domain: if p.tenant.trim().is_empty() {
            "*".to_string()
        } else {
            p.tenant.clone()
        },
        object: p.resource.clone(),
        action: p.action.clone(),
        effect: effect_to_entity(p.effect),
        condition: String::new(),
        description: String::new(),
        is_active: p.enabled,
        created_by: String::new(),
        created_at: None,
        updated_at: None,
        deleted_at: None,
        tenant_id: p.tenant.clone(),
        deleted_by: String::new(),
        project_id: p.project.clone(),
        resource_type: String::new(),
        attributes_json: serde_json::to_string(&p.conditions).unwrap_or_else(|_| "{}".to_string()),
    }
}

pub(super) fn page_response(
    total_items: usize,
    page: Option<&common_pb::PageRequest>,
) -> common_pb::PageResponse {
    native_page_response(page, total_items as i64, total_items.max(1) as i32)
}

pub(super) fn bounded_page_window(page: Option<&common_pb::PageRequest>) -> (usize, usize, i32) {
    let window = native_page_window(page, 100);
    (window.limit, window.offset, window.page_size)
}

pub(super) fn bounded_page_response(
    total_items: usize,
    page: Option<&common_pb::PageRequest>,
) -> common_pb::PageResponse {
    native_page_response(page, total_items as i64, 100)
}

pub(super) fn timestamp_from_unix(seconds: u64) -> Option<prost_types::Timestamp> {
    if seconds == 0 {
        None
    } else {
        Some(prost_types::Timestamp {
            seconds: seconds as i64,
            nanos: 0,
        })
    }
}

pub(super) fn public_session_handle_from_hash(session_id_hash: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"udb.public-session-handle.v1:");
    hasher.update(session_id_hash.as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity("sesspub_".len() + 32);
    out.push_str("sesspub_");
    for b in digest.iter().take(16) {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

pub(super) fn session_record_to_pb(rec: &SessionRecord, now_unix: u64) -> authn_entity_pb::Session {
    let mut dto = authn_entity_pb::Session {
        session_id: public_session_handle_from_hash(&rec.session_id_hash),
        user_id: rec.user_id.clone(),
        session_type: authn_entity_pb::SessionType::ServerSide as i32,
        // Phase 0 (seal sensitive surfaces): the keyed session-token digests are
        // credential-equivalent (a leaked digest + the global hash secret enables
        // session hijack), so they are never surfaced through Get/ListSessions.
        // `session_id` above is a deterministic public handle, not the raw
        // session id and not the stored lookup hash.
        session_token_lookup: String::new(),
        session_token_hash: String::new(),
        csrf_token_hash: String::new(),
        access_token_jti: String::new(),
        refresh_token_jti: String::new(),
        device_type: authn_entity_pb::DeviceType::Api as i32,
        device_name: rec.client_fingerprint.clone(),
        ip_address: String::new(),
        user_agent: String::new(),
        is_active: rec.is_active(now_unix),
        expires_at: timestamp_from_unix(rec.expires_at_unix),
        last_active_at: timestamp_from_unix(rec.updated_at_unix),
        revoked_by: String::new(),
        revoke_reason: String::new(),
        created_at: timestamp_from_unix(rec.created_at_unix),
        tenant_id: rec.tenant_id.clone(),
        project_id: rec.project_id.clone(),
        principal_id: rec.principal_id.clone(),
        provider_id: String::new(),
        auth_method: authn::AuthnMethod::Session.as_str().to_string(),
        scopes_json: serde_json::to_string(&rec.scopes).unwrap_or_else(|_| "[]".to_string()),
        metadata_json: serde_json::json!({
            "roles": rec.roles.clone(),
            "relationship_version": rec.relationship_version.clone(),
            "client_fingerprint": rec.client_fingerprint.clone(),
        })
        .to_string(),
    };
    // §7: structurally blank OUTPUT_VIEW_STORAGE_ONLY fields (session token
    // hash/lookup + CSRF hash) via descriptor-driven codegen.
    crate::proto_redaction::RedactStorageOnly::redact_storage_only(&mut dto);
    dto
}

pub(super) fn scopes_to_db(scopes: &[String]) -> String {
    scopes
        .iter()
        .filter(|s| !s.trim().is_empty())
        .cloned()
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn scopes_from_db(raw: &str) -> Vec<String> {
    raw.split([',', ' ', '\n', '\t'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

pub(super) fn conditions_from_json(value: serde_json::Value) -> BTreeMap<String, String> {
    value
        .as_object()
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| {
                    v.as_str()
                        .map(|s| (k.clone(), s.to_string()))
                        .or_else(|| Some((k.clone(), v.to_string())))
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn policy_from_pg_row(row: &sqlx::postgres::PgRow) -> Result<AuthzPolicy, sqlx::Error> {
    let effect: String = row.try_get("effect")?;
    let conditions: serde_json::Value = row.try_get("conditions")?;
    let required_scopes: String = row.try_get("required_scopes")?;
    let mut conditions = conditions_from_json(conditions);
    for metadata_key in [
        "priority",
        "role",
        "purpose",
        "relationship",
        "required_scopes",
    ] {
        conditions.remove(metadata_key);
    }
    Ok(AuthzPolicy {
        id: row.try_get("id")?,
        priority: row.try_get("priority")?,
        enabled: row.try_get("enabled")?,
        effect: if effect.eq_ignore_ascii_case("deny") {
            Effect::Deny
        } else {
            Effect::Allow
        },
        tenant: row.try_get("tenant")?,
        project: row.try_get("project")?,
        subject: row.try_get("subject")?,
        role: row.try_get("role")?,
        action: row.try_get("action")?,
        resource: row.try_get("resource")?,
        purpose: row.try_get("purpose")?,
        relationship: row.try_get("relationship")?,
        conditions,
        required_scopes: scopes_from_db(&required_scopes),
    })
}

pub(super) fn principal_from_session(rec: &SessionRecord) -> Principal {
    let subject = if rec.user_id.trim().is_empty() {
        rec.principal_id.clone()
    } else {
        rec.user_id.clone()
    };
    Principal {
        principal_id: rec.principal_id.clone(),
        subject,
        user_id: rec.user_id.clone(),
        service_identity: rec.service_identity.clone(),
        tenant_id: rec.tenant_id.clone(),
        project_id: rec.project_id.clone(),
        scopes: rec.scopes.clone(),
        roles: rec.roles.clone(),
        provider_id: String::new(),
        auth_method: authn::AuthnMethod::Session.as_str().to_string(),
    }
}

pub(super) fn principal_from_api_key(rec: &authn::ApiKeyRecord) -> Principal {
    let subject = if rec.service_identity.trim().is_empty() {
        rec.principal_id.clone()
    } else {
        rec.service_identity.clone()
    };
    Principal {
        principal_id: rec.principal_id.clone(),
        subject,
        user_id: String::new(),
        service_identity: rec.service_identity.clone(),
        tenant_id: rec.tenant_id.clone(),
        project_id: rec.project_id.clone(),
        scopes: rec.scopes.clone(),
        roles: Vec::new(),
        provider_id: String::new(),
        auth_method: authn::AuthnMethod::ApiKey.as_str().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn storage_only_field_names(message_full_name: &str) -> BTreeSet<String> {
        const OUTPUT_VIEW_STORAGE_ONLY: i32 = 1;
        let manifest = crate::runtime::descriptor_manifest::descriptor_contract_manifest_static();
        let message = manifest
            .messages
            .iter()
            .find(|message| message.full_name == message_full_name)
            .unwrap_or_else(|| panic!("descriptor message {message_full_name} must exist"));

        message
            .fields
            .iter()
            .filter(|field| {
                field
                    .db_column_security
                    .as_ref()
                    .is_some_and(|security| security.output_view == OUTPUT_VIEW_STORAGE_ONLY)
            })
            .map(|field| field.name.clone())
            .collect()
    }

    fn session_pb_string_field<'a>(pb: &'a authn_entity_pb::Session, field: &str) -> &'a str {
        match field {
            "csrf_token_hash" => &pb.csrf_token_hash,
            "session_token_hash" => &pb.session_token_hash,
            "session_token_lookup" => &pb.session_token_lookup,
            other => panic!("Session mapper has no storage-only assertion for {other}"),
        }
    }

    fn session_record() -> SessionRecord {
        SessionRecord {
            session_id_hash: "hmac-sha256:0123456789abcdef".to_string(),
            principal_id: "user-1".to_string(),
            user_id: "user-1".to_string(),
            service_identity: String::new(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            scopes: vec!["data:read".to_string()],
            roles: vec!["reader".to_string()],
            relationship_version: "rv1".to_string(),
            created_at_unix: 10,
            updated_at_unix: 20,
            expires_at_unix: 30,
            revoked_at_unix: 0,
            client_fingerprint: "device".to_string(),
        }
    }

    #[test]
    fn session_mapper_uses_public_handle_and_blanks_hash_material() {
        let rec = session_record();
        let pb = session_record_to_pb(&rec, 20);

        assert!(pb.session_id.starts_with("sesspub_"));
        assert_ne!(pb.session_id, rec.session_id_hash);
        assert!(pb.session_token_lookup.is_empty());
        assert!(pb.session_token_hash.is_empty());
        assert!(pb.csrf_token_hash.is_empty());
        assert!(pb.access_token_jti.is_empty());
        assert!(pb.refresh_token_jti.is_empty());
    }

    #[test]
    fn session_mapper_blanks_descriptor_storage_only_fields() {
        let storage_only = storage_only_field_names("udb.core.authn.entity.v1.Session");
        assert_eq!(
            storage_only,
            [
                "csrf_token_hash",
                "session_token_hash",
                "session_token_lookup"
            ]
            .into_iter()
            .map(str::to_string)
            .collect()
        );

        let pb = session_record_to_pb(&session_record(), 20);
        for field in storage_only {
            assert!(
                session_pb_string_field(&pb, &field).is_empty(),
                "descriptor storage-only Session field {field} must be blanked by the mapper"
            );
        }
    }

    #[test]
    fn public_session_handle_is_stable_and_not_a_lookup_hash() {
        let hash = "hmac-sha256:abcdef";
        let one = public_session_handle_from_hash(hash);
        let two = public_session_handle_from_hash(hash);

        assert_eq!(one, two);
        assert!(one.starts_with("sesspub_"));
        assert!(!one.contains("hmac-sha256"));
        assert_ne!(one, hash);
    }
}
