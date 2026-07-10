//! Native `LiveQueryService` (master-plan 9.7) — query results that update
//! themselves. A client subscribes to a tenant-scoped query over a source proto
//! entity and receives an initial `Snapshot` (the rows currently matching an
//! IR-expressible filter) followed by an open server stream of `Change` deltas
//! (insert / update / delete) as the underlying data mutates.
//!
//! Tenant isolation is the entire point of 9.7, so it is enforced twice, both
//! fail closed:
//!
//! 1. **Snapshot** — produced ONLY through the mediated IR read path
//!    ([`DataBrokerRuntime::native_entity_read_for_service`]) with the tenant
//!    predicate injected server-side from the VERIFIED claim. No raw query is
//!    ever built, so there is no authz-bypass surface. The read-fence carried on
//!    the `RequestContext` is honoured by that mediated path.
//! 2. **Delta stream** — every CDC event is re-checked, fail closed, against the
//!    subscriber's tenant scope ([`event_matches_tenant_scope`], which reuses the
//!    public [`crate::runtime::cdc::tenant_scoped_topic`] classifier and mirrors
//!    the engine-tail per-event predicate). A missing or foreign `tenant_id`
//!    payload is DROPPED, never streamed — this is the cross-tenant-leak guard.
//!    The subscription's IR predicate is then evaluated against the row by a
//!    single-row evaluator ([`filter_matches_row`]) before the delta is yielded.
//!
//! Backpressure: deltas are forwarded over a BOUNDED `tokio::sync::mpsc` channel.
//! If the broadcast feed lags (CDC outruns this subscription) or the subscriber
//! is too slow to drain the channel, the stream is CLOSED with
//! `resource_exhausted` rather than buffering without bound.

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::cdc::CdcEngine;
use crate::ir::{ComparisonOp, LogicalFilter, LogicalPagination, LogicalRead, LogicalValue};
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::livequery::services::v1 as lq_pb;
use crate::proto::udb::core::livequery::services::v1::live_query_service_server::LiveQueryService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::{ChannelManager, OperationChannel};

pub use crate::proto::udb::core::livequery::services::v1::live_query_service_server::LiveQueryServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    admit_on as native_admit_on, native_service_context, validate_request_tenant,
};

/// Service id this handler dispatches its mediated snapshot reads under. The
/// dispatch target only selects the backend/instance; the table is resolved from
/// the request `message_type` via the embedded manifest, so a snapshot reads the
/// source entity's table on the resolved (postgres) store.
const SERVICE_ID: &str = "livequery";

/// Default bound on the initial snapshot row count when the caller omits one.
const DEFAULT_SNAPSHOT_LIMIT: u32 = 1_000;
/// Hard cap on the initial snapshot so one subscription cannot scan unbounded.
const MAX_SNAPSHOT_LIMIT: u32 = 10_000;

/// Default bounded per-subscription delta buffer. Overridable via
/// `LIVEQUERY_BUFFER_EVENTS`; on overflow the stream is closed (never grown).
const DEFAULT_BUFFER_EVENTS: usize = 1_024;

/// Resolve the bounded delta-buffer depth from `LIVEQUERY_BUFFER_EVENTS`,
/// falling back to [`DEFAULT_BUFFER_EVENTS`]. A non-positive / unparsable value
/// uses the default so the channel is always bounded. Resolved exactly once
/// (no per-subscribe env reads): the buffer bound is process-static config.
fn buffer_events() -> usize {
    static BUF: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *BUF.get_or_init(|| {
        std::env::var("LIVEQUERY_BUFFER_EVENTS")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_BUFFER_EVENTS)
    })
}

/// The streamed frame type: an initial snapshot then a stream of change deltas.
type LiveQueryStream =
    Pin<Box<dyn Stream<Item = Result<lq_pb::SubscribeResponse, Status>> + Send + 'static>>;

/// Postgres-backed `LiveQueryService` handler.
pub struct LiveQueryServiceImpl {
    /// Runtime handle for the mediated snapshot read path (IR query plan +
    /// read-fence + server-side tenant predicate). `None` fails closed.
    runtime: Option<Arc<DataBrokerRuntime>>,
    /// CDC engine the delta stream subscribes to. `None` means no live feed is
    /// available (slim deployments) — the snapshot is served and the stream ends.
    cdc_engine: Option<Arc<CdcEngine>>,
    /// Shared per-tenant fair-admission manager (rate-bounds new subscriptions).
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
}

fn livequery_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "livequery",
        operation,
        capability_required,
        message,
    )
}

impl LiveQueryServiceImpl {
    pub fn new() -> Self {
        Self {
            runtime: None,
            cdc_engine: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

    pub(crate) fn with_cdc_engine(mut self, cdc_engine: Option<Arc<CdcEngine>>) -> Self {
        self.cdc_engine = cdc_engine;
        self
    }

    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Snapshot reads are durable-only: fail closed when no runtime dispatch is
    /// configured. Returns a cloned `Arc` so the read can outlive the borrow.
    fn require_runtime(&self) -> Result<Arc<DataBrokerRuntime>, Status> {
        self.runtime.clone().ok_or_else(|| {
            livequery_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "live query service requires runtime native-entity dispatch (no runtime configured)",
            )
        })
    }
}

impl Default for LiveQueryServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

/// Resolved source-entity facts a live query needs: the proto logical field name
/// of the tenant-isolation column (injected into the snapshot filter) and the
/// entity's CDC topic (the exact tenant-scoped topic the delta feed watches).
struct SourceBinding {
    tenant_field: String,
    cdc_topic: String,
}

/// Resolve the source entity under the SHARED resolver
/// (`postgres_helpers::tenant_column_ref`, the same family search the data plane
/// and search service use) plus its manifest `cdc_topic`. Fails CLOSED when the
/// source message type is unknown or declares no tenant column — a live query we
/// cannot tenant-scope must never be served.
fn resolve_source(message_type: &str) -> Result<SourceBinding, Status> {
    let manifest = crate::runtime::native_catalog::native_manifest();
    let table = crate::broker::table_for_message(manifest, message_type).ok_or_else(|| {
        crate::runtime::executor_utils::invalid_argument_fields(
            format!("live query source '{message_type}' is not a known UDB entity"),
            [("message_type", "must name a known tenant-scoped UDB entity")],
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

fn livequery_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

/// Map a proto comparison to the neutral IR operator. `UNSPECIFIED` is rejected.
fn map_comparison(op: lq_pb::LiveQueryComparison) -> Result<ComparisonOp, Status> {
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
fn build_user_filter(
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
fn snapshot_filter(
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
fn row_object(row: &serde_json::Value) -> serde_json::Value {
    row.get("n").cloned().unwrap_or_else(|| row.clone())
}

/// The CDC payload key that may carry the row image, in precedence order. A
/// native outbox payload is usually the row itself; richer envelopes nest it.
const ROW_KEYS: [&str; 5] = ["after", "row", "new", "record", "data"];

/// Extract the changed row image from a CDC payload: the first object-valued
/// `ROW_KEYS` member, else the payload object itself.
fn change_row(payload: &serde_json::Value) -> serde_json::Value {
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
fn topic_matches_source(topic: &str, cdc_topic: &str) -> bool {
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
fn event_matches_tenant_scope(
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
fn filter_matches_row(filter: &LogicalFilter, row: &serde_json::Value) -> bool {
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
fn change_frame(
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

/// Drive the bounded delta forwarder: subscribe to the CDC broadcast feed, drop
/// every event that fails the fail-closed tenant re-check or the IR predicate,
/// and forward survivors as `Change` frames. Closes the stream with
/// `resource_exhausted` on broadcast lag or a saturated subscriber channel.
fn livequery_backpressure_status(operation: &'static str, message: &'static str) -> Status {
    crate::runtime::executor_utils::quota_status("livequery", operation, 0, message)
}

async fn run_delta_forward(
    mut rx: broadcast::Receiver<crate::cdc::CdcEnvelope>,
    tx: mpsc::Sender<Result<lq_pb::SubscribeResponse, Status>>,
    tenant_id: String,
    project_id: String,
    cdc_topic: String,
    user_filter: Option<LogicalFilter>,
) {
    loop {
        match rx.recv().await {
            Ok(envelope) => {
                if !topic_matches_source(&envelope.topic, &cdc_topic) {
                    continue;
                }
                // Fail closed if the payload cannot be inspected: an opaque event
                // cannot be proven to belong to this tenant.
                let payload =
                    match serde_json::from_str::<serde_json::Value>(&envelope.payload_json) {
                        Ok(value) => value,
                        Err(_) => continue,
                    };
                // SECURITY: per-event tenant-scope re-check — a tenant-less or
                // foreign event is dropped here, never streamed.
                if !event_matches_tenant_scope(&envelope.topic, &payload, &tenant_id, &project_id) {
                    continue;
                }
                let row = change_row(&payload);
                if let Some(filter) = user_filter.as_ref() {
                    if !filter_matches_row(filter, &row) {
                        continue;
                    }
                }
                match tx.try_send(Ok(change_frame(&envelope, &payload))) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        let _ = tx
                            .send(Err(livequery_backpressure_status(
                                "subscriber_channel",
                                "live query subscriber too slow; stream closed",
                            )))
                            .await;
                        break;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                let _ = tx
                    .send(Err(livequery_backpressure_status(
                        "delta feed lag",
                        "live query delta feed lagged; stream closed",
                    )))
                    .await;
                break;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

#[tonic::async_trait]
impl LiveQueryService for LiveQueryServiceImpl {
    type SubscribeStream = LiveQueryStream;

    async fn subscribe(
        &self,
        request: Request<lq_pb::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        // Cross-tenant guard FIRST: the body tenant_id must match the verified
        // claim/header. After this passes, the body value IS the verified tenant.
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let message_type = req.message_type.trim().to_string();
        let project_id = req.project_id.trim().to_string();
        if message_type.is_empty() {
            return Err(livequery_required_field(
                "message_type",
                "must be a non-empty source message type",
                "message_type is required",
            ));
        }
        // Fail closed BEFORE any read/stream if the source has no tenant column.
        let source = resolve_source(&message_type)?;
        let user_filter = build_user_filter(&req.filters)?;

        // Rate-bound new subscriptions per tenant at entry; the permit is dropped
        // immediately (not held across the long-lived stream) so it cannot block.
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            SERVICE_ID,
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;

        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, &project_id);

        // Subscribe to the delta feed BEFORE the snapshot so events committed
        // during the snapshot are buffered on the broadcast receiver (not lost in
        // the subscribe→read gap); any overlap with snapshot rows is de-duplicated
        // client-side by event_id.
        let delta_rx = self.cdc_engine.as_ref().map(|cdc| cdc.subscribe());

        let limit = match req.snapshot_limit {
            value if value <= 0 => DEFAULT_SNAPSHOT_LIMIT,
            value => (value as u32).min(MAX_SNAPSHOT_LIMIT),
        };
        let read = LogicalRead {
            message_type: message_type.clone(),
            filter: Some(snapshot_filter(
                &source.tenant_field,
                &tenant_id,
                user_filter.clone(),
            )),
            projection: None,
            sort: Vec::new(),
            include: Vec::new(),
            pagination: Some(LogicalPagination::limit(limit)),
        };
        // MEDIATED snapshot: IR query plan + read-fence (carried on the context),
        // tenant predicate injected server-side. Never a raw query.
        let rows = runtime
            .native_entity_read_for_service(SERVICE_ID, &context, read)
            .await?;
        let rows_json: Vec<String> = rows
            .iter()
            .map(|row| serde_json::to_string(&row_object(row)).unwrap_or_else(|_| "{}".to_string()))
            .collect();
        let snapshot = lq_pb::SubscribeResponse {
            payload: Some(lq_pb::subscribe_response::Payload::Snapshot(
                lq_pb::LiveQuerySnapshot {
                    row_count: rows_json.len() as i64,
                    rows_json,
                },
            )),
            error: None,
        };

        let (tx, mut rx) =
            mpsc::channel::<Result<lq_pb::SubscribeResponse, Status>>(buffer_events());
        // The snapshot is the FIRST frame (the channel is empty, so this fits).
        let _ = tx.try_send(Ok(snapshot));

        // Spawn the bounded delta forwarder when a live CDC feed exists; otherwise
        // drop the sender so the stream ends cleanly after the snapshot.
        if let Some(rx_delta) = delta_rx {
            tokio::spawn(run_delta_forward(
                rx_delta,
                tx,
                tenant_id,
                project_id,
                source.cdc_topic,
                user_filter,
            ));
        } else {
            drop(tx);
        }

        let stream = async_stream::try_stream! {
            while let Some(item) = rx.recv().await {
                let frame = item?;
                yield frame;
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }
}

#[cfg(test)]
mod livequery_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use serde_json::json;
    use tonic::metadata::MetadataValue;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    /// A caller scoped to tenant-a must not subscribe to tenant-b by putting a
    /// foreign tenant_id in the request BODY; the scope guard rejects this before
    /// any read/stream access (no runtime/CDC needed).
    #[tokio::test]
    async fn subscribe_rejects_cross_tenant_body() {
        let svc = LiveQueryServiceImpl::new();
        let mut request = Request::new(lq_pb::SubscribeRequest {
            tenant_id: "tenant-b".to_string(),
            message_type: "udb.core.lock.entity.v1.Lock".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        // `.err()` first: the Ok variant (a boxed Subscribe stream) is not `Debug`,
        // so `expect_err` can't format it — only the `Status` error is needed here.
        let err = svc
            .subscribe(request)
            .await
            .err()
            .expect("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn subscribe_missing_message_type_carries_field_violation() {
        let svc = LiveQueryServiceImpl::new(); // no runtime/CDC; validation runs first
        let mut request = Request::new(lq_pb::SubscribeRequest {
            tenant_id: "tenant-a".to_string(),
            message_type: " ".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

        let err = svc
            .subscribe(request)
            .await
            .err()
            .expect("missing message_type must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "message_type is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "message_type");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty source message type"
        );
    }

    #[tokio::test]
    async fn subscribe_empty_predicate_field_carries_field_violation() {
        let svc = LiveQueryServiceImpl::new(); // no runtime/CDC; filter validation runs first
        let mut request = Request::new(lq_pb::SubscribeRequest {
            tenant_id: "tenant-a".to_string(),
            message_type: "udb.core.lock.entity.v1.Lock".to_string(),
            filters: vec![lq_pb::LiveQueryPredicate {
                field: " ".to_string(),
                op: lq_pb::LiveQueryComparison::Eq as i32,
                value: "HELD".to_string(),
            }],
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

        let err = svc
            .subscribe(request)
            .await
            .err()
            .expect("empty predicate field must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "live query predicate field must not be empty"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "filters.field");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty live query predicate field"
        );
    }

    #[tokio::test]
    async fn subscribe_unspecified_predicate_op_carries_field_violation() {
        let svc = LiveQueryServiceImpl::new(); // no runtime/CDC; filter validation runs first
        let mut request = Request::new(lq_pb::SubscribeRequest {
            tenant_id: "tenant-a".to_string(),
            message_type: "udb.core.lock.entity.v1.Lock".to_string(),
            filters: vec![lq_pb::LiveQueryPredicate {
                field: "status".to_string(),
                op: lq_pb::LiveQueryComparison::Unspecified as i32,
                value: "HELD".to_string(),
            }],
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

        let err = svc
            .subscribe(request)
            .await
            .err()
            .expect("unspecified predicate op must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "live query predicate comparison op is unspecified"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "filters.op");
        assert_eq!(
            detail.field_violations[0].description,
            "must specify a live query predicate comparison operator"
        );
    }

    /// The per-event re-check is the cross-tenant-leak guard: a tenant-less event
    /// and a foreign-tenant event are DROPPED; only a matching event is KEPT.
    #[test]
    fn per_event_scope_drops_foreign_and_tenantless_keeps_match() {
        let topic = "udb.lock.lock.acquired.v1";
        // Matching tenant (and project) is kept.
        assert!(event_matches_tenant_scope(
            topic,
            &json!({"tenant_id": "tenant-a", "project_id": "project-a"}),
            "tenant-a",
            "project-a",
        ));
        // Foreign tenant is dropped.
        assert!(!event_matches_tenant_scope(
            topic,
            &json!({"tenant_id": "tenant-b"}),
            "tenant-a",
            "",
        ));
        // Tenant-less payload is dropped (fail closed).
        assert!(!event_matches_tenant_scope(
            topic,
            &json!({"event_id": "e1"}),
            "tenant-a",
            "",
        ));
        // A non-`udb.` (non-tenant-scoped) topic is never consumed.
        assert!(!event_matches_tenant_scope(
            "app.customer.changed",
            &json!({"tenant_id": "tenant-a"}),
            "tenant-a",
            "",
        ));
        // An unscoped subscriber cannot receive a tenant-stamped event.
        assert!(!event_matches_tenant_scope(
            topic,
            &json!({"tenant_id": "tenant-a"}),
            "",
            "",
        ));
        // Project mismatch is dropped even when the tenant matches.
        assert!(!event_matches_tenant_scope(
            topic,
            &json!({"tenant_id": "tenant-a", "project_id": "project-b"}),
            "tenant-a",
            "project-a",
        ));
    }

    /// The source must resolve a tenant column or the subscription fails closed.
    #[test]
    fn unknown_source_fails_closed() {
        // `.err()` first: the Ok variant `SourceBinding` is not `Debug`.
        let err = resolve_source("not.a.real.Entity")
            .err()
            .expect("unknown source must fail closed");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "live query source 'not.a.real.Entity' is not a known UDB entity"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "message_type");
        assert_eq!(
            detail.field_violations[0].description,
            "must name a known tenant-scoped UDB entity"
        );
    }

    #[test]
    fn backpressure_status_carries_typed_quota_detail() {
        let status = livequery_backpressure_status(
            "subscriber_channel",
            "live query subscriber too slow; stream closed",
        );
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Quota as i32);
        assert!(detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert_eq!(detail.backend, "livequery");
        assert_eq!(detail.operation, "subscriber_channel");
    }

    #[test]
    fn livequery_missing_runtime_capability_carries_typed_detail() {
        let err = livequery_capability_status(
            "native_entity_dispatch",
            "runtime_native_entity_dispatch",
            "live query service requires runtime native-entity dispatch (no runtime configured)",
        );
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "live query service requires runtime native-entity dispatch (no runtime configured)"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "livequery");
        assert_eq!(detail.operation, "native_entity_dispatch");
        assert_eq!(detail.capability_required, "runtime_native_entity_dispatch");
        assert!(!detail.retryable);
    }

    /// The single-row IR evaluator gates deltas: only rows still matching the
    /// subscription predicate are yielded.
    #[test]
    fn single_row_evaluator_matches_predicate() {
        let filter = LogicalFilter::And(vec![
            LogicalFilter::Comparison {
                field: "status".to_string(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("HELD".to_string()),
            },
            LogicalFilter::Comparison {
                field: "fencing_token".to_string(),
                op: ComparisonOp::Ge,
                value: LogicalValue::Int(5),
            },
        ]);
        assert!(filter_matches_row(
            &filter,
            &json!({"status": "HELD", "fencing_token": 7})
        ));
        // Numeric predicate fails (token below threshold).
        assert!(!filter_matches_row(
            &filter,
            &json!({"status": "HELD", "fencing_token": 3})
        ));
        // String predicate fails (status differs).
        assert!(!filter_matches_row(
            &filter,
            &json!({"status": "RELEASED", "fencing_token": 9})
        ));
        // Missing field => comparison false => row excluded.
        assert!(!filter_matches_row(&filter, &json!({"status": "HELD"})));
    }
}

impl DataBrokerService {
    /// Build the native `LiveQueryService`, wired to the runtime mediated-read
    /// dispatch (snapshots), the CDC engine broadcast feed (deltas), and the
    /// shared fair-admission channels.
    pub(crate) fn build_livequery_service(&self) -> LiveQueryServiceImpl {
        let runtime = self.runtime.load_full();
        let channels = Some(runtime.channels().clone());
        LiveQueryServiceImpl::new()
            .with_runtime(Some(runtime))
            .with_cdc_engine(self.cdc_engine.clone())
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
