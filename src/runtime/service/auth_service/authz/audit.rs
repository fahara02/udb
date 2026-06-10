//! Access-decision audit reads.

use super::*;

fn decision_source_from_db(value: &str) -> i32 {
    match value {
        "ROLE_POLICY" | "DECISION_SOURCE_ROLE_POLICY" => {
            authz_entity_pb::DecisionSource::RolePolicy as i32
        }
        "DIRECT_POLICY" | "DECISION_SOURCE_DIRECT_POLICY" => {
            authz_entity_pb::DecisionSource::DirectPolicy as i32
        }
        "NO_MATCH" | "DECISION_SOURCE_NO_MATCH" => authz_entity_pb::DecisionSource::NoMatch as i32,
        _ => authz_entity_pb::DecisionSource::Unspecified as i32,
    }
}

fn redact_audit_text(value: String) -> String {
    let lower = value.to_ascii_lowercase();
    let sensitive = lower.contains("hmac-sha256:")
        || lower.contains("argon2id$")
        || lower.contains("password")
        || lower.contains("session_token")
        || lower.contains("csrf_token")
        || lower.contains("api_key")
        || lower.contains("key_hash")
        || lower.contains("totp_secret")
        || lower.contains("recovery_code")
        || lower.contains("bearer ")
        || lower.starts_with("sess_")
        || (lower.starts_with("udbk_") && lower.contains('.'));
    if sensitive {
        "[REDACTED]".to_string()
    } else {
        value
    }
}

fn audit_select_projection(model: &NativeModel) -> String {
    [
        model.text("decision_audit_id"),
        model.text("user_id"),
        model.text_or_empty("domain"),
        model.text_or_empty("object"),
        model.text_or_empty("action"),
        model.select("effect"),
        model.select("decision_source"),
        model.text_or_empty("matched_rule"),
        model.text_or_empty("reason"),
        model.text_or_empty("ip_address"),
        model.text_or_empty("correlation_id"),
        model.text_or_empty("tenant_id"),
        // Phase L3 task4 expanded compliance columns.
        model.text_or_empty("decision_id"),
        model.text_or_empty("policy_version"),
        model.text_or_empty("relationship_version"),
        model.text_or_empty("purpose"),
        model.text_or_empty("scopes"),
        model.text_or_empty("project_id"),
        model.text_or_empty("actor_kind"),
        model.text_or_empty("resource_type"),
        model.text_or_empty("trace_id"),
        model.text_or_empty("span_id"),
        model.text_or_empty("user_agent_hash"),
        // matched_policy_ids is a TEXT[] — read it as an array below; the JSONB
        // decision_input is rendered to text.
        format!(
            "COALESCE({}, ARRAY[]::TEXT[]) AS matched_policy_ids",
            model.q("matched_policy_ids")
        ),
        model.json_text_as("decision_input", "decision_input"),
    ]
    .join(", ")
}

/// Map an `access_decision_audits` row to the `AccessDecisionAudit` entity.
fn audit_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<authz_entity_pb::AccessDecisionAudit, Status> {
    let map = |e: sqlx::Error| Status::internal(format!("decode access audit failed: {e}"));
    Ok(authz_entity_pb::AccessDecisionAudit {
        decision_audit_id: row.try_get("decision_audit_id").map_err(map)?,
        user_id: row.try_get("user_id").map_err(map)?,
        domain: row.try_get("domain").map_err(map)?,
        object: row.try_get("object").map_err(map)?,
        action: row.try_get("action").map_err(map)?,
        effect: effect_from_db(&row.try_get::<String, _>("effect").map_err(map)?),
        decision_source: decision_source_from_db(
            &row.try_get::<String, _>("decision_source").map_err(map)?,
        ),
        matched_rule: redact_audit_text(row.try_get("matched_rule").map_err(map)?),
        reason: redact_audit_text(row.try_get("reason").map_err(map)?),
        ip_address: row.try_get("ip_address").map_err(map)?,
        correlation_id: redact_audit_text(row.try_get("correlation_id").map_err(map)?),
        decided_at: None,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        // Phase L3 task4 expanded compliance columns. These were written through
        // the redacting decision-input path, so they carry no credential material;
        // text columns still pass through `redact_audit_text` as defense in depth.
        decision_id: row.try_get("decision_id").unwrap_or_default(),
        policy_version: row.try_get("policy_version").unwrap_or_default(),
        relationship_version: row.try_get("relationship_version").unwrap_or_default(),
        purpose: row.try_get("purpose").unwrap_or_default(),
        scopes: row.try_get("scopes").unwrap_or_default(),
        matched_policy_ids: row
            .try_get::<Vec<String>, _>("matched_policy_ids")
            .unwrap_or_default(),
        project_id: row.try_get("project_id").unwrap_or_default(),
        actor_kind: row.try_get("actor_kind").unwrap_or_default(),
        resource_type: row.try_get("resource_type").unwrap_or_default(),
        trace_id: row.try_get("trace_id").unwrap_or_default(),
        span_id: row.try_get("span_id").unwrap_or_default(),
        user_agent_hash: row.try_get("user_agent_hash").unwrap_or_default(),
        decision_input: redact_audit_text(row.try_get("decision_input").unwrap_or_default()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_text_redacts_credential_shaped_values() {
        assert_eq!(
            redact_audit_text("hmac-sha256:abc".to_string()),
            "[REDACTED]"
        );
        assert_eq!(redact_audit_text("sess_raw".to_string()), "[REDACTED]");
        assert_eq!(
            redact_audit_text("policy denied".to_string()),
            "policy denied"
        );
    }

    #[test]
    fn audit_context_masks_ip_hashes_ua_and_redacts_input() {
        use std::collections::BTreeMap;
        let mut attrs = BTreeMap::new();
        attrs.insert("ip_address".to_string(), "203.0.113.9".to_string());
        attrs.insert("user_agent".to_string(), "Mozilla/5.0 secret".to_string());
        attrs.insert("correlation_id".to_string(), "corr-7".to_string());
        attrs.insert("trace_id".to_string(), "trace-7".to_string());
        // A credential-shaped attribute MUST be scrubbed from decision_input.
        attrs.insert("api_key".to_string(), "udbk_live.abc".to_string());
        attrs.insert("region".to_string(), "us-east".to_string());

        let ctx = super::super::AuditContext::from_attributes(&attrs, "billing");
        // IP masked to /24, never verbatim.
        assert_eq!(ctx.source_ip, "203.0.113.0/24");
        assert!(!ctx.source_ip.contains(".9"));
        // UA hashed.
        assert!(ctx.user_agent.starts_with("ua_"));
        assert!(!ctx.user_agent.contains("Mozilla"));
        assert_eq!(ctx.correlation_id, "corr-7");
        assert_eq!(ctx.trace_id, "trace-7");
        assert_eq!(ctx.purpose, "billing");
        // decision_input keeps non-sensitive attrs and redacts credentials, and
        // never carries the raw IP / UA / correlation id.
        assert!(ctx.decision_input.contains("us-east"));
        assert!(ctx.decision_input.contains("[REDACTED]"));
        assert!(!ctx.decision_input.contains("udbk_live.abc"));
        assert!(!ctx.decision_input.contains("203.0.113.9"));
    }
}

impl AuthzServiceImpl {
    pub(super) async fn list_access_decision_audits_impl(
        &self,
        request: Request<authz_pb::ListAccessDecisionAuditsRequest>,
    ) -> Result<Response<authz_pb::ListAccessDecisionAuditsResponse>, Status> {
        let req = request.into_inner();
        let pool = self.require_pool()?;
        let audit_model = self.audits_model();
        let rel = audit_model.relation.clone();
        let projection = audit_select_projection(&audit_model);
        let user_filter = if req.user_id.trim().is_empty() {
            None
        } else {
            Some(stable_uuid_from_subject(&req.user_id))
        };
        // Honor the caller's page request with a hard cap so a single call can
        // neither ignore pagination nor pull an unbounded result set.
        const MAX_PAGE_SIZE: i32 = 500;
        let page_size = req
            .page
            .as_ref()
            .map(|p| p.page_size)
            .filter(|&n| n > 0)
            .unwrap_or(100)
            .min(MAX_PAGE_SIZE) as i64;
        let page_num = req
            .page
            .as_ref()
            .map(|p| p.page)
            .filter(|&n| n > 0)
            .unwrap_or(1) as i64;
        let offset = (page_num - 1).max(0) * page_size;
        let rows = sqlx::query(&format!(
            "SELECT {projection} \
             FROM {rel} \
             WHERE ($1::UUID IS NULL OR {user_id} = $1) \
               AND ($2 = '' OR {domain_col} = $2) \
               AND ($3 = '' OR {correlation_id} = $3) \
             ORDER BY {decided_at} DESC \
             LIMIT $4 OFFSET $5",
            user_id = audit_model.q("user_id"),
            domain_col = audit_model.q("domain"),
            correlation_id = audit_model.q("correlation_id"),
            decided_at = audit_model.q("decided_at"),
        ))
        .bind(user_filter)
        .bind(&req.domain)
        .bind(&req.correlation_id)
        .bind(page_size)
        .bind(offset)
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("list access audits failed: {err}")))?;
        let mut all = Vec::with_capacity(rows.len());
        for row in &rows {
            all.push(audit_from_row(row)?);
        }
        // The SQL already applied LIMIT/OFFSET, so `all` IS this page — do NOT
        // re-paginate in memory (that returned empty for page >= 2). Total comes
        // from a COUNT over the same filters so has_next is correct.
        let total: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {rel} \
             WHERE ($1::UUID IS NULL OR {user_id} = $1) \
               AND ($2 = '' OR {domain_col} = $2) \
               AND ($3 = '' OR {correlation_id} = $3)",
            user_id = audit_model.q("user_id"),
            domain_col = audit_model.q("domain"),
            correlation_id = audit_model.q("correlation_id"),
        ))
        .bind(user_filter)
        .bind(&req.domain)
        .bind(&req.correlation_id)
        .fetch_one(pool)
        .await
        .map_err(|err| Status::internal(format!("count access audits failed: {err}")))?;
        Ok(Response::new(authz_pb::ListAccessDecisionAuditsResponse {
            page: Some(page_response(total as usize, req.page.as_ref())),
            audits: all,
        }))
    }
}
