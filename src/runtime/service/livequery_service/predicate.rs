//! Source-entity resolution, IR filter construction, the single-row IR predicate
//! evaluator (`ir::eval` scoped to 9.7), and CDC change-payload parsing.

use tonic::Status;

use crate::ir::{ComparisonOp, LogicalFilter, LogicalValue};
use crate::proto::udb::core::livequery::services::v1 as lq_pb;

use super::errors::livequery_required_field;

/// Resolved source-entity facts a live query needs: the proto logical field name
/// of the tenant-isolation column (injected into the snapshot filter) and the
/// entity's CDC topic (the exact tenant-scoped topic the delta feed watches).
pub(crate) struct SourceBinding {
    pub(crate) tenant_field: String,
    pub(crate) cdc_topic: String,
}

/// Resolve the source entity under the SHARED resolver
/// (`postgres_helpers::tenant_column_ref`, the same family search the data plane
/// and search service use) plus its manifest `cdc_topic`. Fails CLOSED when the
/// source message type is unknown or declares no tenant column — a live query we
/// cannot tenant-scope must never be served.
pub(crate) fn resolve_source(message_type: &str) -> Result<SourceBinding, Status> {
    let manifest = crate::runtime::native_catalog::native_manifest();
    let table =
        crate::broker::resolve_table_for_message(manifest, message_type).map_err(|_error| {
            crate::runtime::executor_utils::invalid_argument_fields(
                format!("live query source '{message_type}' is not a known UDB entity"),
                [(
                    "message_type",
                    "must name exactly one known tenant-scoped UDB entity",
                )],
            )
        })?;
    let column = crate::runtime::postgres_helpers::tenant_column_ref(table).ok_or_else(|| {
        crate::runtime::executor_utils::invalid_argument_fields(
            format!(
                "live query source '{message_type}' has no tenant-isolation column; \
                 cannot be tenant-scoped"
            ),
            [("message_type", "must name a tenant-scoped UDB entity")],
        )
    })?;
    Ok(SourceBinding {
        tenant_field: column.field_name.clone(),
        cdc_topic: table.cdc_topic.trim().to_string(),
    })
}

/// Map a proto comparison to the neutral IR operator. `UNSPECIFIED` is rejected.
pub(crate) fn map_comparison(op: lq_pb::LiveQueryComparison) -> Result<ComparisonOp, Status> {
    use lq_pb::LiveQueryComparison as P;
    match op {
        P::Eq => Ok(ComparisonOp::Eq),
        P::Ne => Ok(ComparisonOp::Ne),
        P::Lt => Ok(ComparisonOp::Lt),
        P::Le => Ok(ComparisonOp::Le),
        P::Gt => Ok(ComparisonOp::Gt),
        P::Ge => Ok(ComparisonOp::Ge),
        P::Unspecified => Err(livequery_required_field(
            "filters.op",
            "must specify a live query predicate comparison operator",
            "live query predicate comparison op is unspecified",
        )),
    }
}

/// Type a predicate value: numeric when it parses cleanly, else a string. Keeps
/// the mediated snapshot read binding the operand with the right backend type.
fn typed_value(raw: &str) -> LogicalValue {
    if let Ok(int_value) = raw.parse::<i64>() {
        return LogicalValue::Int(int_value);
    }
    if let Ok(float_value) = raw.parse::<f64>() {
        return LogicalValue::Float(float_value);
    }
    LogicalValue::String(raw.to_string())
}

/// Build the user-supplied IR filter (AND of comparisons). Returns `None` when
/// no predicates were supplied. An empty field or unspecified op is rejected.
pub(crate) fn build_user_filter(
    predicates: &[lq_pb::LiveQueryPredicate],
) -> Result<Option<LogicalFilter>, Status> {
    if predicates.is_empty() {
        return Ok(None);
    }
    let mut comparisons = Vec::with_capacity(predicates.len());
    for predicate in predicates {
        let field = predicate.field.trim();
        if field.is_empty() {
            return Err(livequery_required_field(
                "filters.field",
                "must be a non-empty live query predicate field",
                "live query predicate field must not be empty",
            ));
        }
        let op = map_comparison(predicate.op());
        comparisons.push(LogicalFilter::Comparison {
            field: field.to_string(),
            op: op?,
            value: typed_value(predicate.value.trim()),
        });
    }
    Ok(Some(LogicalFilter::And(comparisons)))
}

/// Compose the snapshot filter: the server-side tenant equality (and project
/// equality when scoped) injected on top of the caller's predicates. The tenant
/// value is the VERIFIED claim tenant, never raw body — the cross-tenant guard
/// has already proven they are equal.
pub(crate) fn snapshot_filter(
    tenant_field: &str,
    tenant_id: &str,
    user_filter: Option<LogicalFilter>,
) -> LogicalFilter {
    let mut branches = vec![LogicalFilter::Comparison {
        field: tenant_field.to_string(),
        op: ComparisonOp::Eq,
        value: LogicalValue::String(tenant_id.to_string()),
    }];
    if let Some(filter) = user_filter {
        match filter {
            LogicalFilter::And(inner) => branches.extend(inner),
            other => branches.push(other),
        }
    }
    LogicalFilter::And(branches)
}

/// Unwrap the canonical native-read row JSON. The mediated read may nest the row
/// under `"n"`; return the inner object when present, else the value itself.
pub(crate) fn row_object(row: &serde_json::Value) -> serde_json::Value {
    row.get("n").cloned().unwrap_or_else(|| row.clone())
}

/// The CDC payload key that may carry the row image, in precedence order. A
/// native outbox payload is usually the row itself; richer envelopes nest it.
const ROW_KEYS: [&str; 5] = ["after", "row", "new", "record", "data"];

/// Extract the changed row image from a CDC payload: the first object-valued
/// `ROW_KEYS` member, else the payload object itself.
pub(crate) fn change_row(payload: &serde_json::Value) -> serde_json::Value {
    for key in ROW_KEYS {
        if let Some(value) = payload.get(key) {
            if value.is_object() {
                return value.clone();
            }
        }
    }
    payload.clone()
}

/// Classify the delta operation from the payload's `op`/`operation` field, else
/// from the topic verb suffix; defaults to UPDATE.
fn change_op(topic: &str, payload: &serde_json::Value) -> lq_pb::LiveQueryChangeOp {
    use lq_pb::LiveQueryChangeOp as Op;
    let explicit = payload
        .get("op")
        .or_else(|| payload.get("operation"))
        .and_then(|value| value.as_str())
        .map(str::to_ascii_lowercase);
    if let Some(op) = explicit {
        if op.starts_with("ins") || op == "c" || op == "create" {
            return Op::Insert;
        }
        if op.starts_with("del") || op == "d" || op == "remove" {
            return Op::Delete;
        }
        if op.starts_with("upd") || op == "u" || op == "modify" {
            return Op::Update;
        }
    }
    let topic = topic.to_ascii_lowercase();
    if topic.contains("created") || topic.contains("inserted") {
        Op::Insert
    } else if topic.contains("deleted") || topic.contains("removed") {
        Op::Delete
    } else {
        Op::Update
    }
}

/// Whether a CDC `topic` is the subscription's source-entity topic. The match is
/// EXACT against the manifest `cdc_topic`. Tenant isolation does NOT depend on
/// this — it only narrows the feed to the right entity; the fail-closed tenant
/// re-check below is the security boundary.
pub(crate) fn topic_matches_source(topic: &str, cdc_topic: &str) -> bool {
    !cdc_topic.is_empty() && topic.trim() == cdc_topic
}

/// SECURITY CRUX of 9.7: re-check, fail closed, that a CDC event belongs to the
/// subscriber's tenant before it is ever streamed. Mirrors the engine-tail
/// per-event tenant-scoped predicate for a non-privileged, tenant-scoped
/// subscriber (a live-query subscriber is never an unscoped admin stream):
///
/// - non-`udb.` (non-tenant-scoped) topics are not consumed at all — dropped;
/// - a payload with a missing / empty `tenant_id` is dropped (tenant-less);
/// - a payload whose `tenant_id` differs from the verified scope is dropped;
/// - when a project scope is set, a mismatched / missing `project_id` is dropped.
///
/// Only an event whose tenant (and project, if scoped) matches the verified
/// claim survives. Reuses the public [`crate::runtime::cdc::tenant_scoped_topic`].
pub(crate) fn event_matches_tenant_scope(
    topic: &str,
    payload: &serde_json::Value,
    tenant_scope: &str,
    project_scope: &str,
) -> bool {
    if !crate::runtime::cdc::tenant_scoped_topic(topic) {
        return false;
    }
    if tenant_scope.trim().is_empty() {
        // An unscoped live-query subscriber cannot prove ownership of any
        // tenant-stamped event: fail closed.
        return false;
    }
    let event_tenant = payload
        .get("tenant_id")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .trim();
    if event_tenant.is_empty() || event_tenant != tenant_scope.trim() {
        return false;
    }
    if !project_scope.trim().is_empty() {
        let event_project = payload
            .get("project_id")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .trim();
        if event_project.is_empty() || event_project != project_scope.trim() {
            return false;
        }
    }
    true
}

/// Resolve a possibly-dotted field path within a JSON object.
fn path_value<'a>(row: &'a serde_json::Value, field: &str) -> Option<&'a serde_json::Value> {
    let mut current = row;
    for segment in field.split('.') {
        current = current.as_object()?.get(segment)?;
    }
    Some(current)
}

/// Numeric + string projection of a JSON value for comparison.
fn json_scalar(value: &serde_json::Value) -> (Option<f64>, String) {
    match value {
        serde_json::Value::Number(number) => (number.as_f64(), number.to_string()),
        serde_json::Value::String(text) => (text.parse::<f64>().ok(), text.clone()),
        serde_json::Value::Bool(flag) => (None, flag.to_string()),
        serde_json::Value::Null => (None, String::new()),
        other => (None, other.to_string()),
    }
}

/// Numeric + string projection of an IR value for comparison.
fn logical_scalar(value: &LogicalValue) -> (Option<f64>, String) {
    match value {
        LogicalValue::Int(int_value) => (Some(*int_value as f64), int_value.to_string()),
        LogicalValue::Float(float_value) => (Some(*float_value), float_value.to_string()),
        LogicalValue::String(text) => (text.parse::<f64>().ok(), text.clone()),
        LogicalValue::Bool(flag) => (None, flag.to_string()),
        LogicalValue::Null => (None, String::new()),
        other => (None, format!("{other:?}")),
    }
}

/// Compare a JSON field value against an IR operand under `op`. Numeric when both
/// sides are numeric, else lexicographic on the string projection.
fn compare_scalar(field: &serde_json::Value, op: ComparisonOp, operand: &LogicalValue) -> bool {
    let (field_num, field_str) = json_scalar(field);
    let (operand_num, operand_str) = logical_scalar(operand);
    match (field_num, operand_num) {
        (Some(left), Some(right)) => match op {
            ComparisonOp::Eq => left == right,
            ComparisonOp::Ne => left != right,
            ComparisonOp::Lt => left < right,
            ComparisonOp::Le => left <= right,
            ComparisonOp::Gt => left > right,
            ComparisonOp::Ge => left >= right,
            _ => false,
        },
        _ => match op {
            ComparisonOp::Eq => field_str == operand_str,
            ComparisonOp::Ne => field_str != operand_str,
            ComparisonOp::Lt => field_str < operand_str,
            ComparisonOp::Le => field_str <= operand_str,
            ComparisonOp::Gt => field_str > operand_str,
            ComparisonOp::Ge => field_str >= operand_str,
            _ => false,
        },
    }
}

/// Single-row IR predicate evaluator (the `ir::eval` 9.7 needs, scoped to this
/// service and reusing the neutral [`LogicalFilter`] types). Evaluates a filter
/// tree against one JSON row so a delta is yielded only when it still matches the
/// subscription. A missing field makes a comparison false (the row does not
/// match the predicate).
pub(crate) fn filter_matches_row(filter: &LogicalFilter, row: &serde_json::Value) -> bool {
    match filter {
        LogicalFilter::And(branches) => branches
            .iter()
            .all(|branch| filter_matches_row(branch, row)),
        LogicalFilter::Or(branches) => branches
            .iter()
            .any(|branch| filter_matches_row(branch, row)),
        LogicalFilter::Not(inner) => !filter_matches_row(inner, row),
        LogicalFilter::Comparison { field, op, value } => match path_value(row, field) {
            Some(found) => compare_scalar(found, *op, value),
            None => false,
        },
        LogicalFilter::IsNull(field) => path_value(row, field)
            .map(serde_json::Value::is_null)
            .unwrap_or(true),
        LogicalFilter::InList { field, values } => match path_value(row, field) {
            Some(found) => values
                .iter()
                .any(|candidate| compare_scalar(found, ComparisonOp::Eq, candidate)),
            None => false,
        },
    }
}

/// Build a `Change` frame from a CDC envelope and its parsed payload.
pub(crate) fn change_frame(
    envelope: &crate::cdc::CdcEnvelope,
    payload: &serde_json::Value,
) -> lq_pb::SubscribeResponse {
    let row = change_row(payload);
    let change = lq_pb::LiveQueryChange {
        op: change_op(&envelope.topic, payload) as i32,
        row_json: serde_json::to_string(&row).unwrap_or_else(|_| "{}".to_string()),
        event_id: envelope.event_id.clone(),
    };
    lq_pb::SubscribeResponse {
        payload: Some(lq_pb::subscribe_response::Payload::Change(change)),
        error: None,
    }
}
