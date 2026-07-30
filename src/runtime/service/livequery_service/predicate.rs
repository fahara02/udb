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

/// Parse the optional durable-resume cursor from the `x-udb-livequery-resume`
/// metadata header value: the client's last-delivered `LiveQueryChange.event_id`.
/// Trims surrounding whitespace (a CRLF-tainted proxy header) and rejects blank;
/// returns `None` when absent/blank — a fresh, non-resuming subscription. Kept
/// pure over the raw header value so it is unit-testable without a tonic
/// `MetadataMap`.
pub(crate) fn parse_resume_cursor(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
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

/// Type a predicate value: coerce to a numeric IR value ONLY when the raw string
/// is already in CANONICAL numeric form — i.e. the parsed number renders back to
/// the exact same string. This keeps a numeric-LOOKING business string that is
/// not canonical — a leading-zero identifier like an account number `"0123"` or a
/// zip code `"007"`, or a signed/padded form like `"+5"` — as a `String`, so the
/// mediated snapshot read binds it against a text column as text (matching the
/// stored `"0123"`) instead of silently binding the integer `123` (which would
/// mismatch, or force a text↔int cast). Canonical forms (`"123"`, `"0"`, `"-5"`,
/// `"1.5"`) still coerce so numeric predicates bind with the right backend type.
fn typed_value(raw: &str) -> LogicalValue {
    if let Ok(int_value) = raw.parse::<i64>() {
        // Round-trip guard: "0123" parses to 123 but renders "123" != "0123", so
        // it is NOT canonical and stays a string. "123"/"0"/"-5" round-trip.
        if int_value.to_string() == raw {
            return LogicalValue::Int(int_value);
        }
    }
    if let Ok(float_value) = raw.parse::<f64>() {
        // Same round-trip rule for floats: "1.5"/"0.5" render identically and
        // coerce; padded/exponent forms ("1.50", "1e3") are not canonical and
        // stay strings (and still compare numerically against a numeric column
        // via the scalar-projection parse-back in `compare_scalar`).
        if float_value.to_string() == raw {
            return LogicalValue::Float(float_value);
        }
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
/// `payload` is UDB's own outbox envelope key — the generic data-plane emit
/// (`core/setup_data.rs`) and the native-service compliance envelope both nest
/// the row image under `payload`, with tenant/operation stamped at the top
/// level. It is checked AFTER the external CDC conventions (Debezium-style
/// `after`, etc.) so a foreign envelope's more-specific row key still wins, and
/// BEFORE the whole-object fallthrough so a UDB delta unwraps to the real row
/// (otherwise filters and change frames operate on the envelope, dropping every
/// matching delta and shipping the wrong shape).
const ROW_KEYS: [&str; 6] = ["after", "row", "new", "record", "data", "payload"];

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
    let topic_lower = topic.to_ascii_lowercase();
    // Whether the topic verb suffix names a row CREATION event; shared by the
    // explicit-`upsert` classification below and the topic-only fallthrough.
    let topic_indicates_create =
        topic_lower.contains("created") || topic_lower.contains("inserted");
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
        // An explicit "upsert" is a create-OR-update: it does not start with
        // "upd", so it would otherwise fall through to the UPDATE default even
        // when the topic clearly marks a creation. Classify it by the topic verb
        // suffix (`*.created`/`*.inserted` => INSERT), else UPDATE. Checked
        // before the `upd*` arm since "upsert" does not match that prefix.
        if op == "upsert" {
            return if topic_indicates_create {
                Op::Insert
            } else {
                Op::Update
            };
        }
        if op.starts_with("upd") || op == "u" || op == "modify" {
            return Op::Update;
        }
    }
    if topic_indicates_create {
        Op::Insert
    } else if topic_lower.contains("deleted") || topic_lower.contains("removed") {
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
///
/// Canonical filter reuse: the tenant/project comparison — the actual tenant-leak
/// boundary — is delegated to the ONE canonical implementation
/// ([`crate::cdc::CdcEngine::event_matches_stream_scope`], which wraps the
/// engine-tail `payload_value_matches_stream_scope`) so the two copies can never
/// drift. A live-query subscriber is a non-privileged, tenant-scoped subscriber
/// and must NEVER receive a non-`udb.` (non-tenant-scoped) topic, so those are
/// hard-dropped up front here (the canonical predicate, being general, would pass
/// a tenant-less non-`udb.` event to a privileged unscoped admin stream — a mode
/// that does not exist for this service); everything reachable past that guard
/// goes through the canonical tenant/project check unchanged.
pub(crate) fn event_matches_tenant_scope(
    topic: &str,
    payload: &serde_json::Value,
    tenant_scope: &str,
    project_scope: &str,
) -> bool {
    // Fail closed: a non-`udb.` topic is never a tenant-scoped live-query source.
    if !crate::runtime::cdc::tenant_scoped_topic(topic) {
        return false;
    }
    // Delegate the tenant/project comparison to the single canonical boundary.
    // `privileged=false, policy_scoped=false`: a live-query subscriber is neither
    // a privileged unscoped admin stream nor a policy-scoped topic subscriber, so
    // an empty tenant scope and any tenant mismatch both fail closed.
    crate::cdc::CdcEngine::event_matches_stream_scope(
        topic,
        payload,
        tenant_scope.trim(),
        project_scope.trim(),
        false,
        false,
    )
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

/// Build a keepalive/heartbeat frame: an empty `Change` with the UNSPECIFIED op,
/// an empty row, and an empty event id. It carries NO data — its only purpose is
/// to put a byte on an otherwise-idle stream so an L4/L7 load balancer's idle
/// timeout does not reap a healthy long-lived subscription. It needs no proto
/// change: a client distinguishes it by the UNSPECIFIED op (and empty
/// `event_id`) and ignores it — it is never a real delta and never advances a
/// resume cursor. Emitted only when `livequery_keepalive_interval()` is enabled.
pub(crate) fn keepalive_frame() -> lq_pb::SubscribeResponse {
    lq_pb::SubscribeResponse {
        payload: Some(lq_pb::subscribe_response::Payload::Change(
            lq_pb::LiveQueryChange {
                op: lq_pb::LiveQueryChangeOp::Unspecified as i32,
                row_json: String::new(),
                event_id: String::new(),
            },
        )),
        error: None,
    }
}

#[cfg(test)]
mod predicate_tests {
    use serde_json::json;

    use super::lq_pb;
    use super::{
        change_op, event_matches_tenant_scope, keepalive_frame, parse_resume_cursor, typed_value,
    };
    use crate::ir::LogicalValue;

    /// The durable-resume cursor parse: a present, non-blank header yields the
    /// trimmed event_id; absent or whitespace-only yields `None` (fresh, non-
    /// resuming subscription). Whitespace is trimmed so a CRLF-tainted proxy
    /// header still resolves.
    #[test]
    fn parse_resume_cursor_trims_and_rejects_blank() {
        assert_eq!(
            parse_resume_cursor(Some("11111111-1111-1111-1111-111111111111")),
            Some("11111111-1111-1111-1111-111111111111".to_string())
        );
        assert_eq!(
            parse_resume_cursor(Some("  evt-42\r\n")),
            Some("evt-42".to_string())
        );
        assert_eq!(parse_resume_cursor(Some("   ")), None);
        assert_eq!(parse_resume_cursor(Some("")), None);
        assert_eq!(parse_resume_cursor(None), None);
    }

    /// Tenant-isolation is preserved after delegating to the canonical scope
    /// predicate: an in-tenant `udb.` event passes, but a FOREIGN-tenant event and
    /// a NON-`udb.` topic are both still dropped (fail closed), as is a tenant-less
    /// or empty-scope subscriber.
    #[test]
    fn event_matches_tenant_scope_drops_foreign_and_non_udb() {
        let topic = "udb.item.item.changed.v1";
        // Same tenant, no project scope: passes.
        assert!(event_matches_tenant_scope(
            topic,
            &json!({"tenant_id": "acme"}),
            "acme",
            "",
        ));
        // Foreign tenant on a udb. topic: dropped.
        assert!(!event_matches_tenant_scope(
            topic,
            &json!({"tenant_id": "evil"}),
            "acme",
            "",
        ));
        // Tenant-less payload: dropped (cannot prove ownership).
        assert!(!event_matches_tenant_scope(topic, &json!({}), "acme", ""));
        // Non-`udb.` topic is never a tenant-scoped live-query source: dropped even
        // when the tenant matches (the subscriber-appropriate hard-drop).
        assert!(!event_matches_tenant_scope(
            "external.orders.created",
            &json!({"tenant_id": "acme"}),
            "acme",
            "",
        ));
        // Empty subscriber scope fails closed against a tenant-stamped event.
        assert!(!event_matches_tenant_scope(
            topic,
            &json!({"tenant_id": "acme"}),
            "",
            "",
        ));
        // Project scope enforced when set: mismatched project dropped.
        assert!(!event_matches_tenant_scope(
            topic,
            &json!({"tenant_id": "acme", "project_id": "p1"}),
            "acme",
            "p2",
        ));
        assert!(event_matches_tenant_scope(
            topic,
            &json!({"tenant_id": "acme", "project_id": "p1"}),
            "acme",
            "p1",
        ));
    }

    /// A keepalive frame is a proto-free heartbeat: an UNSPECIFIED-op `Change`
    /// with empty row/event id that a client ignores. It must NOT look like a
    /// real delta (no op, no event id to advance a resume cursor).
    #[test]
    fn keepalive_frame_is_an_ignorable_empty_change() {
        let frame = keepalive_frame();
        assert!(frame.error.is_none());
        match frame.payload {
            Some(lq_pb::subscribe_response::Payload::Change(change)) => {
                assert_eq!(change.op, lq_pb::LiveQueryChangeOp::Unspecified as i32);
                assert!(change.row_json.is_empty());
                assert!(change.event_id.is_empty());
            }
            other => panic!("keepalive must be a Change frame, got {other:?}"),
        }
    }

    /// F7 regression: a numeric-looking business string with a leading zero (or
    /// other non-canonical form) must NOT be coerced to a number, or the mediated
    /// snapshot read would bind it as an integer and mismatch the text column.
    #[test]
    fn typed_value_keeps_non_canonical_numeric_strings_as_string() {
        // Leading-zero identifiers stay strings ("0123" is not the integer 123).
        assert_eq!(
            typed_value("0123"),
            LogicalValue::String("0123".to_string())
        );
        assert_eq!(typed_value("007"), LogicalValue::String("007".to_string()));
        // A signed-with-plus form is not canonical either.
        assert_eq!(typed_value("+5"), LogicalValue::String("+5".to_string()));
        // Canonical integers/floats still coerce so numeric binds keep working.
        assert_eq!(typed_value("123"), LogicalValue::Int(123));
        assert_eq!(typed_value("0"), LogicalValue::Int(0));
        assert_eq!(typed_value("-5"), LogicalValue::Int(-5));
        assert_eq!(typed_value("1.5"), LogicalValue::Float(1.5));
        // A plainly non-numeric string is unchanged.
        assert_eq!(
            typed_value("HELD"),
            LogicalValue::String("HELD".to_string())
        );
    }

    /// F8 regression: an explicit `"upsert"` op is create-or-update, classified by
    /// the topic verb suffix rather than silently defaulting to UPDATE.
    #[test]
    fn change_op_classifies_upsert_by_topic_verb() {
        use lq_pb::LiveQueryChangeOp as Op;
        // A creation topic => INSERT.
        assert_eq!(
            change_op("udb.lock.lock.created.v1", &json!({"op": "upsert"})),
            Op::Insert
        );
        // An "inserted" verb (and the `operation` alias key) also => INSERT.
        assert_eq!(
            change_op("udb.item.item.inserted.v1", &json!({"operation": "upsert"})),
            Op::Insert
        );
        // A non-creation topic => UPDATE (the create-or-update fallback).
        assert_eq!(
            change_op("udb.lock.lock.changed.v1", &json!({"op": "upsert"})),
            Op::Update
        );
        // Explicit non-upsert ops are unaffected by the new arm.
        assert_eq!(
            change_op("udb.lock.lock.changed.v1", &json!({"op": "insert"})),
            Op::Insert
        );
        assert_eq!(
            change_op("udb.lock.lock.changed.v1", &json!({"op": "update"})),
            Op::Update
        );
    }
}
