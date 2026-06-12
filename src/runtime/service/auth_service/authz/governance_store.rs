//! Durable load/persist helpers for the Phase K governance entities
//! (`policy_drafts`, `policy_versions`, `policy_approvals`, `policy_sets`).
//!
//! A `PolicyDraft` row stores policies in `proposed_policies_json` and the
//! relationship tuples + role bindings together in `proposed_tuples_json` under
//! `{ "relationship_tuples": [...], "role_bindings": [...] }`, so the full
//! candidate document (policies + bindings + tuples) round-trips even though the
//! draft table predates role bindings.

use super::governance_logic::{self as glogic, PolicyDocument};
use super::*;
use crate::proto::udb::core::authz::entity::v1 as authz_entity_pb;

impl AuthzServiceImpl {
    /// Persist the full candidate document onto a draft row: policies go in
    /// `proposed_policies_json`, tuples + role bindings in `proposed_tuples_json`.
    pub(super) async fn persist_draft_document(
        &self,
        draft_id: &str,
        document: &PolicyDocument,
    ) -> Result<(), Status> {
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            Status::failed_precondition("native authz requires runtime-backed draft persistence")
        })?;
        // P6.10 Wave 4: typed UPDATE of the two JSONB document columns (was raw
        // UPDATE). Values are built as JSON directly and bound as JSONB.
        let policies_value = serde_json::Value::Array(
            document
                .policies
                .iter()
                .map(glogic::policy_to_json)
                .collect::<Vec<_>>(),
        );
        let full = document.to_json();
        let tuples_value = serde_json::json!({
            "relationship_tuples": full["relationship_tuples"],
            "role_bindings": full["role_bindings"],
        });
        let mut assignments = std::collections::BTreeMap::new();
        assignments.insert(
            "proposed_policies_json".to_string(),
            LogicalAssignment::Set {
                value: LogicalValue::Json(policies_value),
            },
        );
        assignments.insert(
            "proposed_tuples_json".to_string(),
            LogicalAssignment::Set {
                value: LogicalValue::Json(tuples_value),
            },
        );
        runtime
            .native_entity_update_for_service(
                "authz",
                &crate::RequestContext::default(),
                LogicalUpdate {
                    message_type: "udb.core.authz.entity.v1.PolicyDraft".to_string(),
                    filter: LogicalFilter::Comparison {
                        field: "draft_id".to_string(),
                        op: ComparisonOp::Eq,
                        value: LogicalValue::String(draft_id.to_string()),
                    },
                    assignments,
                    return_fields: Vec::new(),
                    require_affected: false,
                },
            )
            .await
            .map_err(|err| Status::internal(format!("persist draft document failed: {err}")))?;
        Ok(())
    }

    /// Load a `PolicyDraft` row as the proto entity.
    pub(super) async fn load_draft(
        &self,
        draft_id: &str,
    ) -> Result<authz_entity_pb::PolicyDraft, Status> {
        let pool = self.require_pool()?;
        let m = self.policy_drafts_model();
        let rel = m.relation.clone();
        let row = sqlx::query(&format!(
            "SELECT {draft_id}::TEXT AS draft_id, {tenant_id}::TEXT AS tenant_id, \
                    COALESCE({project_id}, '') AS project_id, {title}, COALESCE({description}, '') AS description, \
                    {proposed_policies_json}::TEXT AS proposed_policies_json, COALESCE({proposed_tuples_json}::TEXT, '') AS proposed_tuples_json, \
                    COALESCE({base_version_id}::TEXT, '') AS base_version_id, {status}, {author}, {high_risk}, \
                    COALESCE(EXTRACT(EPOCH FROM {updated_at})::BIGINT, 0) AS updated_at_unix \
             FROM {rel} WHERE {draft_id} = $1::UUID",
            draft_id = m.q("draft_id"),
            tenant_id = m.q("tenant_id"),
            project_id = m.q("project_id"),
            title = m.q("title"),
            description = m.q("description"),
            proposed_policies_json = m.q("proposed_policies_json"),
            proposed_tuples_json = m.q("proposed_tuples_json"),
            base_version_id = m.q("base_version_id"),
            status = m.q("status"),
            author = m.q("author"),
            high_risk = m.q("high_risk"),
            updated_at = m.q("updated_at"),
        ))
        .bind(draft_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("load draft failed: {err}")))?
        .ok_or_else(|| Status::not_found("policy draft not found"))?;
        let updated_unix: i64 = row.try_get("updated_at_unix").unwrap_or(0);
        Ok(authz_entity_pb::PolicyDraft {
            draft_id: row.try_get("draft_id").map_err(decode_err)?,
            tenant_id: row.try_get("tenant_id").map_err(decode_err)?,
            project_id: row.try_get("project_id").map_err(decode_err)?,
            title: row.try_get("title").map_err(decode_err)?,
            description: row.try_get("description").map_err(decode_err)?,
            proposed_policies_json: row.try_get("proposed_policies_json").map_err(decode_err)?,
            proposed_tuples_json: row.try_get("proposed_tuples_json").map_err(decode_err)?,
            base_version_id: row.try_get("base_version_id").map_err(decode_err)?,
            status: row.try_get("status").map_err(decode_err)?,
            author: row.try_get("author").map_err(decode_err)?,
            high_risk: row.try_get("high_risk").map_err(decode_err)?,
            created_at: None,
            updated_at: crate::runtime::service::auth_service::mappings::timestamp_from_unix(
                updated_unix.max(0) as u64,
            ),
        })
    }

    /// Reconstruct the candidate document from a draft row.
    pub(super) async fn load_draft_document(
        &self,
        draft_id: &str,
    ) -> Result<PolicyDocument, Status> {
        let draft = self.load_draft(draft_id).await?;
        Ok(draft_to_document(&draft))
    }

    /// Load a `PolicySet` row as the proto entity.
    pub(super) async fn load_policy_set(
        &self,
        policy_set_id: &str,
    ) -> Result<Option<authz_entity_pb::PolicySet>, Status> {
        let pool = self.require_pool()?;
        let m = self.policy_sets_model();
        let rel = m.relation.clone();
        let row = sqlx::query(&format!(
            "SELECT {policy_set_id}::TEXT AS policy_set_id, {tenant_id}::TEXT AS tenant_id, \
                    COALESCE({project_id}, '') AS project_id, {name}, \
                    COALESCE({active_version_id}::TEXT, '') AS active_version_id, \
                    COALESCE({rollback_version_id}::TEXT, '') AS rollback_version_id, \
                    COALESCE({description}, '') AS description, COALESCE({created_by}, '') AS created_by \
             FROM {rel} WHERE {policy_set_id} = $1::UUID AND {deleted_at} IS NULL",
            policy_set_id = m.q("policy_set_id"),
            tenant_id = m.q("tenant_id"),
            project_id = m.q("project_id"),
            name = m.q("name"),
            active_version_id = m.q("active_version_id"),
            rollback_version_id = m.q("rollback_version_id"),
            description = m.q("description"),
            created_by = m.q("created_by"),
            deleted_at = m.q("deleted_at"),
        ))
        .bind(policy_set_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("load policy set failed: {err}")))?;
        Ok(row.map(|row| authz_entity_pb::PolicySet {
            policy_set_id: row.try_get("policy_set_id").unwrap_or_default(),
            tenant_id: row.try_get("tenant_id").unwrap_or_default(),
            project_id: row.try_get("project_id").unwrap_or_default(),
            name: row.try_get("name").unwrap_or_default(),
            active_version_id: row.try_get("active_version_id").unwrap_or_default(),
            rollback_version_id: row.try_get("rollback_version_id").unwrap_or_default(),
            description: row.try_get("description").unwrap_or_default(),
            created_by: row.try_get("created_by").unwrap_or_default(),
            created_at: None,
            updated_at: None,
            deleted_at: None,
        }))
    }

    /// Load a `PolicyVersion` row as the proto entity.
    pub(super) async fn load_version(
        &self,
        version_id: &str,
    ) -> Result<authz_entity_pb::PolicyVersion, Status> {
        let pool = self.require_pool()?;
        let m = self.policy_versions_model();
        let rel = m.relation.clone();
        let row = sqlx::query(&format!(
            "SELECT {policy_version_id}::TEXT AS policy_version_id, {policy_set_id}::TEXT AS policy_set_id, \
                    {version_number}, {state}, COALESCE({snapshot_hash}, '') AS snapshot_hash, COALESCE({created_by}, '') AS created_by, \
                    COALESCE({activated_by}, '') AS activated_by, COALESCE({rollback_of}::TEXT, '') AS rollback_of, \
                    COALESCE({change_reason}, '') AS change_reason, {revision}, COALESCE({content_hash}, '') AS content_hash, \
                    {tenant_id}::TEXT AS tenant_id, COALESCE({project_id}, '') AS project_id, {payload_json}::TEXT AS payload_json, \
                    {high_risk}, COALESCE({submitted_by}, '') AS submitted_by, COALESCE({source_draft_id}::TEXT, '') AS source_draft_id \
             FROM {rel} WHERE {policy_version_id} = $1::UUID",
            policy_version_id = m.q("policy_version_id"),
            policy_set_id = m.q("policy_set_id"),
            version_number = m.q("version_number"),
            state = m.q("state"),
            snapshot_hash = m.q("snapshot_hash"),
            created_by = m.q("created_by"),
            activated_by = m.q("activated_by"),
            rollback_of = m.q("rollback_of"),
            change_reason = m.q("change_reason"),
            revision = m.q("revision"),
            content_hash = m.q("content_hash"),
            tenant_id = m.q("tenant_id"),
            project_id = m.q("project_id"),
            payload_json = m.q("payload_json"),
            high_risk = m.q("high_risk"),
            submitted_by = m.q("submitted_by"),
            source_draft_id = m.q("source_draft_id"),
        ))
        .bind(version_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("load version failed: {err}")))?
        .ok_or_else(|| Status::not_found("policy version not found"))?;
        Ok(version_from_row(&row))
    }

    pub(super) async fn load_version_document(
        &self,
        version_id: &str,
    ) -> Result<PolicyDocument, Status> {
        let v = self.load_version(version_id).await?;
        let value: serde_json::Value =
            serde_json::from_str(&v.payload_json).unwrap_or_else(|_| serde_json::json!({}));
        Ok(PolicyDocument::from_json(&value))
    }

    pub(super) async fn load_approval(
        &self,
        approval_id: &str,
    ) -> Result<Option<authz_entity_pb::PolicyApproval>, Status> {
        let pool = self.require_pool()?;
        let m = self.policy_approvals_model();
        let rel = m.relation.clone();
        let row = sqlx::query(&format!(
            "SELECT {approval_id}::TEXT AS approval_id, {draft_id}::TEXT AS draft_id, {tenant_id}::TEXT AS tenant_id, \
                    {actor}, {role}, {decision}, {reason} \
             FROM {rel} WHERE {approval_id} = $1::UUID",
            approval_id = m.q("approval_id"),
            draft_id = m.q("draft_id"),
            tenant_id = m.q("tenant_id"),
            actor = m.q("actor"),
            role = m.q("role"),
            decision = m.q("decision"),
            reason = m.q("reason"),
        ))
        .bind(approval_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("load approval failed: {err}")))?;
        Ok(row.map(|row| authz_entity_pb::PolicyApproval {
            approval_id: row.try_get("approval_id").unwrap_or_default(),
            draft_id: row.try_get("draft_id").unwrap_or_default(),
            tenant_id: row.try_get("tenant_id").unwrap_or_default(),
            actor: row.try_get("actor").unwrap_or_default(),
            role: row.try_get("role").unwrap_or_default(),
            decision: row.try_get("decision").unwrap_or_default(),
            reason: row.try_get("reason").unwrap_or_default(),
            created_at: None,
        }))
    }

    /// Promote an approved draft into an immutable APPROVED `PolicyVersion`,
    /// numbering it monotonically within its policy set and freezing the
    /// document into `payload_json`. Returns the created version.
    pub(super) async fn promote_draft_to_version(
        &self,
        draft: &authz_entity_pb::PolicyDraft,
        actor: &str,
    ) -> Result<authz_entity_pb::PolicyVersion, Status> {
        let document = draft_to_document(draft);
        let content_hash = document.content_hash();
        let policy_set_id = self
            .ensure_policy_set(&draft.tenant_id, &draft.project_id, "default", actor)
            .await?;
        let pool = self.require_pool()?;
        let m = self.policy_versions_model();
        let rel = m.relation.clone();
        let version_id = Uuid::new_v4().to_string();
        let payload_json = document.to_json().to_string();
        let row = sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({policy_version_id}, {policy_set_id}, {version_number}, {state}, {created_by}, {submitted_by}, \
              {change_reason}, {revision}, {content_hash}, {tenant_id}, {project_id}, {payload_json}, {high_risk}, {source_draft_id}) \
             VALUES ($1::UUID, $2::UUID, \
               (SELECT COALESCE(MAX({version_number}), 0) + 1 FROM {rel} WHERE {policy_set_id} = $2::UUID), \
               '{approved}', $3, $3, $4, 1, $5, $6, $7, $8::JSONB, $9, $10::UUID) \
             RETURNING {version_number}",
            policy_version_id = m.q("policy_version_id"),
            policy_set_id = m.q("policy_set_id"),
            version_number = m.q("version_number"),
            state = m.q("state"),
            created_by = m.q("created_by"),
            submitted_by = m.q("submitted_by"),
            change_reason = m.q("change_reason"),
            revision = m.q("revision"),
            content_hash = m.q("content_hash"),
            tenant_id = m.q("tenant_id"),
            project_id = m.q("project_id"),
            payload_json = m.q("payload_json"),
            high_risk = m.q("high_risk"),
            source_draft_id = m.q("source_draft_id"),
            approved = "POLICY_VERSION_STATE_APPROVED",
        ))
        .bind(&version_id)
        .bind(&policy_set_id)
        .bind(actor)
        .bind(&draft.description)
        .bind(&content_hash)
        .bind(&draft.tenant_id)
        .bind(&draft.project_id)
        .bind(&payload_json)
        .bind(draft.high_risk)
        .bind(&draft.draft_id)
        .fetch_one(pool)
        .await
        .map_err(|err| Status::internal(format!("promote draft to version failed: {err}")))?;
        let version_number: i64 = row.try_get("version_number").unwrap_or(1);
        Ok(authz_entity_pb::PolicyVersion {
            policy_version_id: version_id,
            policy_set_id,
            version_number,
            state: authz_entity_pb::PolicyVersionState::Approved as i32,
            snapshot_hash: String::new(),
            created_by: actor.to_string(),
            created_at: None,
            activated_by: String::new(),
            activated_at: None,
            rollback_of: String::new(),
            change_reason: draft.description.clone(),
            revision: 1,
            content_hash,
            tenant_id: draft.tenant_id.clone(),
            project_id: draft.project_id.clone(),
            payload_json,
            high_risk: draft.high_risk,
            submitted_by: actor.to_string(),
            source_draft_id: draft.draft_id.clone(),
        })
    }
}

fn decode_err(e: sqlx::Error) -> Status {
    Status::internal(format!("decode governance row failed: {e}"))
}

/// Reconstruct the candidate document from a draft entity.
pub(super) fn draft_to_document(draft: &authz_entity_pb::PolicyDraft) -> PolicyDocument {
    let policies_value: serde_json::Value = serde_json::from_str(&draft.proposed_policies_json)
        .unwrap_or_else(|_| serde_json::json!([]));
    let tuples_value: serde_json::Value =
        serde_json::from_str(&draft.proposed_tuples_json).unwrap_or_else(|_| serde_json::json!({}));
    // proposed_tuples_json carries { relationship_tuples, role_bindings }.
    let combined = serde_json::json!({
        "policies": policies_value,
        "relationship_tuples": tuples_value.get("relationship_tuples").cloned().unwrap_or(serde_json::json!([])),
        "role_bindings": tuples_value.get("role_bindings").cloned().unwrap_or(serde_json::json!([])),
    });
    PolicyDocument::from_json(&combined)
}

/// Map a `policy_versions` row to the proto entity.
pub(super) fn version_from_row(row: &sqlx::postgres::PgRow) -> authz_entity_pb::PolicyVersion {
    authz_entity_pb::PolicyVersion {
        policy_version_id: row.try_get("policy_version_id").unwrap_or_default(),
        policy_set_id: row.try_get("policy_set_id").unwrap_or_default(),
        version_number: row.try_get("version_number").unwrap_or(0),
        state: version_state_from_db(&row.try_get::<String, _>("state").unwrap_or_default()),
        snapshot_hash: row.try_get("snapshot_hash").unwrap_or_default(),
        created_by: row.try_get("created_by").unwrap_or_default(),
        created_at: None,
        activated_by: row.try_get("activated_by").unwrap_or_default(),
        activated_at: None,
        rollback_of: row.try_get("rollback_of").unwrap_or_default(),
        change_reason: row.try_get("change_reason").unwrap_or_default(),
        revision: row.try_get("revision").unwrap_or(1),
        content_hash: row.try_get("content_hash").unwrap_or_default(),
        tenant_id: row.try_get("tenant_id").unwrap_or_default(),
        project_id: row.try_get("project_id").unwrap_or_default(),
        payload_json: row.try_get("payload_json").unwrap_or_default(),
        high_risk: row.try_get("high_risk").unwrap_or(false),
        submitted_by: row.try_get("submitted_by").unwrap_or_default(),
        source_draft_id: row.try_get("source_draft_id").unwrap_or_default(),
    }
}

pub(super) fn version_state_from_db(value: &str) -> i32 {
    use authz_entity_pb::PolicyVersionState as S;
    let v = match value {
        "POLICY_VERSION_STATE_DRAFT" | "DRAFT" => S::Draft,
        "POLICY_VERSION_STATE_PENDING_REVIEW" | "PENDING_REVIEW" => S::PendingReview,
        "POLICY_VERSION_STATE_APPROVED" | "APPROVED" => S::Approved,
        "POLICY_VERSION_STATE_ACTIVE" | "ACTIVE" => S::Active,
        "POLICY_VERSION_STATE_SUPERSEDED" | "SUPERSEDED" => S::Superseded,
        "POLICY_VERSION_STATE_REJECTED" | "REJECTED" => S::Rejected,
        "POLICY_VERSION_STATE_ROLLED_BACK" | "ROLLED_BACK" => S::RolledBack,
        _ => S::Unspecified,
    };
    v as i32
}

pub(super) fn version_state_to_db(state: authz_entity_pb::PolicyVersionState) -> &'static str {
    use authz_entity_pb::PolicyVersionState as S;
    match state {
        S::Unspecified => "POLICY_VERSION_STATE_UNSPECIFIED",
        S::Draft => "POLICY_VERSION_STATE_DRAFT",
        S::PendingReview => "POLICY_VERSION_STATE_PENDING_REVIEW",
        S::Approved => "POLICY_VERSION_STATE_APPROVED",
        S::Active => "POLICY_VERSION_STATE_ACTIVE",
        S::Superseded => "POLICY_VERSION_STATE_SUPERSEDED",
        S::Rejected => "POLICY_VERSION_STATE_REJECTED",
        S::RolledBack => "POLICY_VERSION_STATE_ROLLED_BACK",
    }
}
