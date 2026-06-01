//! proto ↔ runtime conversions and Postgres row helpers shared by the
//! `authn` and `authz` handler modules (Milestone 2).

use std::collections::BTreeMap;

use sqlx::Row;
use tonic::Status;

use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authz::entity::v1 as authz_entity_pb;
use crate::proto::udb::core::authz::services::v1 as authz_pb;
use crate::proto::udb::core::common::v1 as common_pb;

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
            Err(Status::invalid_argument("policy effect is required"))
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
    let page_number = page.map(|p| p.page).filter(|p| *p > 0).unwrap_or(1);
    let page_size = page
        .map(|p| p.page_size)
        .filter(|s| *s > 0)
        .unwrap_or(total_items.max(1) as i32);
    let total_pages = if total_items == 0 {
        0
    } else {
        ((total_items as i32) + page_size - 1) / page_size
    };
    common_pb::PageResponse {
        page: page_number,
        page_size,
        total_items: total_items as i64,
        total_pages,
        next_page_token: String::new(),
        total_count: total_items as i64,
        has_next: page_number < total_pages,
        has_previous: page_number > 1 && total_pages > 0,
    }
}

pub(super) const DEFAULT_LIST_PAGE_SIZE: i32 = 100;
pub(super) const MAX_LIST_PAGE_SIZE: i32 = 500;

pub(super) fn bounded_page_window(page: Option<&common_pb::PageRequest>) -> (usize, usize, i32) {
    let page_number = page.map(|p| p.page).filter(|p| *p > 0).unwrap_or(1);
    let page_size = page
        .map(|p| p.page_size)
        .filter(|s| *s > 0)
        .unwrap_or(DEFAULT_LIST_PAGE_SIZE)
        .min(MAX_LIST_PAGE_SIZE);
    let limit = page_size as usize;
    let offset = (page_number as usize)
        .saturating_sub(1)
        .saturating_mul(limit);
    (limit, offset, page_size)
}

pub(super) fn bounded_page_response(
    total_items: usize,
    page: Option<&common_pb::PageRequest>,
) -> common_pb::PageResponse {
    let (_, _, page_size) = bounded_page_window(page);
    let page_number = page.map(|p| p.page).filter(|p| *p > 0).unwrap_or(1);
    let total_pages = if total_items == 0 {
        0
    } else {
        ((total_items as i32) + page_size - 1) / page_size
    };
    common_pb::PageResponse {
        page: page_number,
        page_size,
        total_items: total_items as i64,
        total_pages,
        next_page_token: String::new(),
        total_count: total_items as i64,
        has_next: page_number < total_pages,
        has_previous: page_number > 1 && total_pages > 0,
    }
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

pub(super) fn session_record_to_pb(rec: &SessionRecord, now_unix: u64) -> authn_entity_pb::Session {
    authn_entity_pb::Session {
        session_id: rec.session_id_hash.clone(),
        user_id: rec.user_id.clone(),
        session_type: authn_entity_pb::SessionType::ServerSide as i32,
        session_token_lookup: rec.session_id_hash.clone(),
        session_token_hash: rec.session_id_hash.clone(),
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
    }
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
