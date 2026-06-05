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
        matched_rule: row.try_get("matched_rule").map_err(map)?,
        reason: row.try_get("reason").map_err(map)?,
        ip_address: row.try_get("ip_address").map_err(map)?,
        correlation_id: row.try_get("correlation_id").map_err(map)?,
        decided_at: None,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
    })
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
