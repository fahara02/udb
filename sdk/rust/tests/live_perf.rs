//! Full-surface live benchmark: every generated RPC, timed through tonic.
//!
//! Gated on `UDB_LIVE_SDK_TESTS=1` and `UDB_LIVE_PERF=1`; without them the test
//! passes without touching the network, so plain `cargo test` stays offline.
//!
//! Requests come from the shared benchmark plan (`UDB_BENCH_PLAN`), which
//! `sdk/python/tests/export_bench_plan.py` writes after running the common seed:
//! each RPC's documented body, resolved against real disposable entities, in
//! measurement order. The plan carries no credentials. This harness logs in
//! itself, substitutes its own session material for the plan's sentinels, and
//! times only the call: encode, transport, server handling and decode.
//!
//! Timing follows the other SDK harnesses so the reports compare row for row:
//! read-only unary RPCs run ten times after a warm-up, mutations once, streaming
//! RPCs to their first response, and `PublishCDC` to the first delivered event.
//! The report is `sdk/rust/perf_report_rust.md`, the format
//! `scripts/collect_sdk_bench_results.py` reads.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::{Duration, Instant};

use base64::Engine as _;
use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, SerializeOptions};
use serde_json::{json, Value};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};
use udb_client::Metadata;

const AUTHN: &str = "/udb.core.authn.services.v1.AuthnService";
const DATA_BROKER: &str = "udb.services.v1.DataBroker";
const CALL_TIMEOUT: Duration = Duration::from_secs(20);
const STREAM_TIMEOUT: Duration = Duration::from_secs(12);
const ACCESS_SENTINEL: &str = "__UDB_BENCH_ACCESS_TOKEN__";
const REFRESH_SENTINEL: &str = "__UDB_BENCH_REFRESH_TOKEN__";
const SESSION_SENTINEL: &str = "__UDB_BENCH_SESSION_ID__";
const CSRF_SENTINEL: &str = "__UDB_BENCH_CSRF_TOKEN__";

// ── Dynamic codec ──────────────────────────────────────────────────────────

/// A tonic codec over `prost_reflect::DynamicMessage`, so any RPC in the
/// descriptor set can be called without a generated stub for it.
#[derive(Clone)]
struct DynamicCodec {
    output: MessageDescriptor,
}

struct DynamicEncoder;
struct DynamicDecoder(MessageDescriptor);

impl Codec for DynamicCodec {
    type Encode = DynamicMessage;
    type Decode = DynamicMessage;
    type Encoder = DynamicEncoder;
    type Decoder = DynamicDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        DynamicEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        DynamicDecoder(self.output.clone())
    }
}

impl Encoder for DynamicEncoder {
    type Item = DynamicMessage;
    type Error = Status;

    fn encode(&mut self, item: DynamicMessage, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        item.encode(dst)
            .map_err(|err| Status::internal(format!("encode request: {err}")))
    }
}

impl Decoder for DynamicDecoder {
    type Item = DynamicMessage;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<DynamicMessage>, Status> {
        DynamicMessage::decode(self.0.clone(), src)
            .map(Some)
            .map_err(|err| Status::internal(format!("decode response: {err}")))
    }
}

// ── Plan ───────────────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct Plan {
    schema_version: u32,
    tenant_id: String,
    project_id: String,
    seeded_fixture_keys: Vec<String>,
    cdc: CdcPlan,
    rpcs: Vec<RpcPlan>,
}

#[derive(serde::Deserialize)]
struct CdcPlan {
    message_type: String,
    record_id: String,
    conflict_fields: Vec<String>,
    context: Value,
}

#[derive(serde::Deserialize)]
struct RpcPlan {
    path: String,
    service: String,
    rpc: String,
    api_alias: String,
    operation_id: String,
    input_type: String,
    output_type: String,
    client_streaming: bool,
    server_streaming: bool,
    kind: String,
    iters: usize,
    warm_up: bool,
    platform: bool,
    reauthenticate_before: bool,
    capability_optional: bool,
    #[serde(default)]
    request: Option<Value>,
    #[serde(default)]
    reset_after_ok: Option<Value>,
    #[serde(default)]
    missing_body: Option<String>,
}

// ── Calls ──────────────────────────────────────────────────────────────────

struct Caller {
    pool: DescriptorPool,
    broker: Channel,
    native: Channel,
    calls: u64,
}

fn code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

/// The outcome of one timed call: a gRPC status name (`OK` on success) and the
/// server's detail.
type Outcome = (String, String);

fn failed(status: &Status) -> Outcome {
    (
        code_name(status.code()).to_string(),
        status.message().to_string(),
    )
}

fn client_error(detail: impl std::fmt::Display) -> Outcome {
    ("CLIENT_ERROR".to_string(), detail.to_string())
}

impl Caller {
    fn message(&self, type_name: &str, body: &Value) -> Result<DynamicMessage, String> {
        let descriptor = self
            .pool
            .get_message_by_name(type_name)
            .ok_or_else(|| format!("{type_name} is not in the SDK descriptor set"))?;
        DynamicMessage::deserialize(descriptor, body.clone())
            .map_err(|err| format!("{type_name}: {err}"))
    }

    fn output(&self, type_name: &str) -> Result<MessageDescriptor, String> {
        self.pool
            .get_message_by_name(type_name)
            .ok_or_else(|| format!("{type_name} is not in the SDK descriptor set"))
    }

    /// Request and response descriptors of `/package.Service/Method`.
    fn method(&self, path: &str) -> Result<(MessageDescriptor, MessageDescriptor), String> {
        let (service, method) = path
            .trim_start_matches('/')
            .split_once('/')
            .ok_or_else(|| format!("malformed rpc path {path}"))?;
        let method = self
            .pool
            .get_service_by_name(service)
            .and_then(|service| {
                service
                    .methods()
                    .find(|candidate| candidate.name() == method)
            })
            .ok_or_else(|| format!("{path} is not in the SDK descriptor set"))?;
        Ok((method.input(), method.output()))
    }

    fn channel(&self, path: &str) -> Channel {
        if path.starts_with(&format!("/{DATA_BROKER}/")) {
            self.broker.clone()
        } else {
            self.native.clone()
        }
    }

    fn request<T>(&mut self, message: T, meta: &Metadata) -> Result<Request<T>, Status> {
        self.calls += 1;
        let meta = meta.clone().with_audit(
            meta.purpose.clone(),
            format!("rust-live-perf-{}", self.calls),
        );
        let mut request = Request::new(message);
        meta.apply(&mut request)?;
        request.set_timeout(CALL_TIMEOUT);
        Ok(request)
    }

    async fn unary(
        &mut self,
        path: &str,
        message: DynamicMessage,
        output: MessageDescriptor,
        meta: &Metadata,
    ) -> Result<DynamicMessage, Status> {
        let request = self.request(message, meta)?;
        let mut grpc = tonic::client::Grpc::new(self.channel(path));
        grpc.ready()
            .await
            .map_err(|err| Status::unavailable(format!("channel not ready: {err}")))?;
        let path = PathAndQuery::try_from(path.to_string())
            .map_err(|err| Status::internal(format!("rpc path: {err}")))?;
        grpc.unary(request, path, DynamicCodec { output })
            .await
            .map(tonic::Response::into_inner)
    }

    /// A unary call from JSON in and to JSON (proto field names) out.
    async fn call_json(
        &mut self,
        path: &str,
        input: &str,
        output: &str,
        body: Value,
        meta: &Metadata,
    ) -> Result<Value, String> {
        let message = self.message(input, &body)?;
        let output = self.output(output)?;
        let response = self
            .unary(path, message, output, meta)
            .await
            .map_err(|status| {
                format!("{path}: {}: {}", code_name(status.code()), status.message())
            })?;
        response
            .serialize_with_options(
                serde_json::value::Serializer,
                &SerializeOptions::new().use_proto_field_name(true),
            )
            .map_err(|err| format!("{path} response: {err}"))
    }

    /// Open a streaming RPC with one request and wait for its first response.
    async fn first_response(
        &mut self,
        rpc: &RpcPlan,
        message: DynamicMessage,
        meta: &Metadata,
    ) -> Outcome {
        let output = match self.output(&rpc.output_type) {
            Ok(output) => output,
            Err(err) => return client_error(err),
        };
        let channel = self.channel(&rpc.path);
        let path = match PathAndQuery::try_from(rpc.path.clone()) {
            Ok(path) => path,
            Err(err) => return client_error(err),
        };
        let codec = DynamicCodec { output };
        let mut grpc = tonic::client::Grpc::new(channel);
        if let Err(err) = grpc.ready().await {
            return client_error(format!("channel not ready: {err}"));
        }
        let call = async {
            if rpc.client_streaming {
                let request = self.request(tokio_stream::iter(vec![message]), meta)?;
                if rpc.server_streaming {
                    let mut stream = grpc.streaming(request, path, codec).await?.into_inner();
                    stream.message().await?;
                } else {
                    grpc.client_streaming(request, path, codec).await?;
                }
            } else {
                let request = self.request(message, meta)?;
                let mut stream = grpc
                    .server_streaming(request, path, codec)
                    .await?
                    .into_inner();
                stream.message().await?;
            }
            Ok::<(), Status>(())
        };
        match tokio::time::timeout(STREAM_TIMEOUT, call).await {
            Ok(Ok(())) => ("OK".to_string(), String::new()),
            Ok(Err(status)) => failed(&status),
            Err(_) => (
                "DEADLINE_EXCEEDED".to_string(),
                "no first response".to_string(),
            ),
        }
    }
}

// ── Session ────────────────────────────────────────────────────────────────

fn required_env(name: &str) -> String {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => panic!("{name} is required when UDB_LIVE_SDK_TESTS=1 and UDB_LIVE_PERF=1"),
    }
}

fn endpoint(target: &str) -> String {
    if target.contains("://") {
        target.to_string()
    } else {
        format!("http://{target}")
    }
}

fn text(value: &Value, field: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

struct Credentials {
    access_token: String,
    refresh_token: String,
    session_id: String,
    csrf_token: String,
    expires_in: u64,
}

async fn login(
    caller: &mut Caller,
    meta: &Metadata,
    username: &str,
    password: &str,
    device: &str,
) -> Result<Credentials, String> {
    let response = caller
        .call_json(
            &format!("{AUTHN}/Login"),
            "udb.core.authn.services.v1.LoginRequest",
            "udb.core.authn.services.v1.LoginResponse",
            json!({
                "username": username,
                "password": password,
                "tenant_hint": meta.tenant_id,
                "project_hint": meta.project_id,
                "device_name": device,
            }),
            meta,
        )
        .await?;
    let access_token = text(&response, "access_token");
    if access_token.is_empty() {
        return Err(format!("Login for {device} returned no access token"));
    }
    Ok(Credentials {
        access_token,
        refresh_token: text(&response, "refresh_token"),
        session_id: text(&response, "session_id"),
        csrf_token: text(&response, "csrf_token"),
        expires_in: response
            .get("access_token_expires_in")
            .and_then(Value::as_u64)
            .unwrap_or(240),
    })
}

/// The verified principal behind a bearer token.
async fn principal(caller: &mut Caller, meta: &Metadata, token: &str) -> Result<Value, String> {
    let response = caller
        .call_json(
            &format!("{AUTHN}/Authenticate"),
            "udb.core.authn.services.v1.AuthnRequest",
            "udb.core.authn.services.v1.AuthnResponse",
            json!({
                "bearer_token": token,
                "tenant_hint": meta.tenant_id,
                "project_hint": meta.project_id,
            }),
            meta,
        )
        .await?;
    response
        .get("principal")
        .cloned()
        .ok_or_else(|| "Authenticate returned no principal".to_string())
}

fn has_role(principal: &Value, role: &str) -> bool {
    principal
        .get("roles")
        .and_then(Value::as_array)
        .is_some_and(|roles| {
            roles
                .iter()
                .filter_map(Value::as_str)
                .any(|value| value.trim().eq_ignore_ascii_case(role))
        })
}

// ── Report ─────────────────────────────────────────────────────────────────

struct Sample {
    service: String,
    rpc: String,
    api_alias: String,
    operation_id: String,
    kind: String,
    iters: usize,
    err: String,
    err_detail: String,
    p50: f64,
    p99: f64,
    mean: f64,
}

fn cell(text: &str) -> String {
    text.replace('\n', " ").replace('|', "\\|")
}

fn write_report(samples: &[Sample], fixture_keys: &[String], tenant: &str, error: Option<&str>) {
    let mut out = String::new();
    let _ = writeln!(out, "# UDB SDK Live Perf — Rust (localhost)\n");
    let _ = writeln!(out, "RPCs measured: {}   tenant={tenant}\n", samples.len());
    out.push_str(
        "Every RPC is driven down its SUCCESS path: the shared seed first creates real, disposable \
         entities (a user, role + assignment + policies, an API key, a notification, a stored file, \
         an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and each request's \
         reference/ID fields are resolved to those real identifiers. The Rust client sends and times \
         every request itself. The TARGET is zero failures; any residual non-OK RPC is listed under \
         Failures for the maintainer to finish.\n\n",
    );
    out.push_str(
        "Unary = full request/response round-trip. Non-CDC streaming RPCs (kind=stream_first_recv) \
         report time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (kind=cdc_first_event, \
         PublishCDC) reports time-to-FIRST-EVENT: the harness subscribes, fires a real Upsert that \
         flows outbox->CDC, and times the first delivered event.\n\n",
    );
    if let Some(error) = error {
        let _ = writeln!(
            out,
            "## Harness error\n\n`HarnessError`: {}\n\nThis is a partial report written before the benchmark process failed.\n",
            cell(error)
        );
    }
    let _ = writeln!(
        out,
        "## Seeded fixtures\n\nCaptured semantic field -> seeded value keys used to resolve request fields: {}\n",
        fixture_keys.join(", ")
    );
    out.push_str("## Per-service mean latency\n\n| Service | RPCs | mean ms |\n|---|--:|--:|\n");
    let mut services: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
    for sample in samples {
        services
            .entry(&sample.service)
            .or_default()
            .push(sample.mean);
    }
    let mut ranked: Vec<(&str, usize, f64)> = services
        .iter()
        .map(|(name, means)| {
            (
                *name,
                means.len(),
                means.iter().sum::<f64>() / means.len() as f64,
            )
        })
        .collect();
    ranked.sort_by(|a, b| b.2.total_cmp(&a.2));
    for (name, count, mean) in ranked {
        let _ = writeln!(out, "| {name} | {count} | {mean:.2} |");
    }
    let mut by_name: Vec<&Sample> = samples.iter().collect();
    by_name.sort_by(|a, b| (&a.service, &a.rpc).cmp(&(&b.service, &b.rpc)));
    let failures: Vec<&&Sample> = by_name
        .iter()
        .filter(|s| s.err != "OK" && s.err != "CAPABILITY_SKIPPED")
        .collect();
    let skipped: Vec<&&Sample> = by_name
        .iter()
        .filter(|s| s.err == "CAPABILITY_SKIPPED")
        .collect();
    let _ = writeln!(out, "\n## Failures ({})\n", failures.len());
    if failures.is_empty() {
        out.push_str("No RPC returned a non-OK gRPC status.\n");
    } else {
        out.push_str(
            "These RPCs returned a non-OK gRPC status and are FAILURES, not latency samples.\n\n",
        );
        out.push_str("| RPC | api_alias | operation_id | kind | err | detail | p99 ms | mean ms | iters |\n|---|---|---|---|---|---|--:|--:|--:|\n");
        for s in failures {
            let _ = writeln!(
                out,
                "| {}/{} | {} | {} | {} | {} | {} | {:.2} | {:.2} | {} |",
                s.service,
                s.rpc,
                s.api_alias,
                s.operation_id,
                s.kind,
                s.err,
                cell(&s.err_detail),
                s.p99,
                s.mean,
                s.iters
            );
        }
    }
    let _ = writeln!(out, "\n## Capability Skips ({})\n", skipped.len());
    if skipped.is_empty() {
        out.push_str("No optional service capability was skipped.\n");
    } else {
        out.push_str("These RPCs reached the served path but require an optional backend capability disabled in this local profile.\n\n");
        out.push_str("| RPC | api_alias | operation_id | kind | detail |\n|---|---|---|---|---|\n");
        for s in skipped {
            let _ = writeln!(
                out,
                "| {}/{} | {} | {} | {} | {} |",
                s.service,
                s.rpc,
                s.api_alias,
                s.operation_id,
                s.kind,
                cell(&s.err_detail)
            );
        }
    }
    out.push_str("\n## Slowest 20 by p99\n\n| RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms |\n|---|---|---|---|---|--:|--:|--:|\n");
    let mut slowest: Vec<&Sample> = samples.iter().collect();
    slowest.sort_by(|a, b| b.p99.total_cmp(&a.p99));
    for s in slowest.into_iter().take(20) {
        let _ = writeln!(
            out,
            "| {}/{} | {} | {} | {} | {} | {:.2} | {:.2} | {:.2} |",
            s.service, s.rpc, s.api_alias, s.operation_id, s.kind, s.err, s.p50, s.p99, s.mean
        );
    }
    out.push_str("\n## Full per-RPC table (sorted by service, then RPC)\n\n| Service | RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms | iters |\n|---|---|---|---|---|---|--:|--:|--:|--:|\n");
    for s in by_name {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {:.2} | {:.2} | {:.2} | {} |",
            s.service,
            s.rpc,
            s.api_alias,
            s.operation_id,
            s.kind,
            s.err,
            s.p50,
            s.p99,
            s.mean,
            s.iters
        );
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("perf_report_rust.md");
    std::fs::write(&path, out).expect("write perf_report_rust.md");
    println!(
        "Rust perf: {} RPCs measured -> {}",
        samples.len(),
        path.display()
    );
}

// ── Harness ────────────────────────────────────────────────────────────────

struct Session {
    user: Metadata,
    platform: Metadata,
    sentinels: BTreeMap<&'static str, String>,
    auto_refresh_token: String,
    auto_session_id: String,
    refresh_at: Instant,
}

impl Session {
    fn bind(&mut self, credentials: &Credentials) {
        self.user.bearer_token = credentials.access_token.clone();
        self.sentinels
            .insert(ACCESS_SENTINEL, credentials.access_token.clone());
        self.sentinels
            .insert(CSRF_SENTINEL, credentials.csrf_token.clone());
        self.auto_refresh_token = credentials.refresh_token.clone();
        self.auto_session_id = credentials.session_id.clone();
        self.refresh_at = Instant::now() + Duration::from_secs(180);
    }
}

/// Keep the measuring bearer fresh across a long run, as the Python harness does.
async fn ensure_fresh(caller: &mut Caller, session: &mut Session) -> Result<(), String> {
    if Instant::now() < session.refresh_at {
        return Ok(());
    }
    let response = caller
        .call_json(
            &format!("{AUTHN}/RefreshToken"),
            "udb.core.authn.services.v1.RefreshTokenRequest",
            "udb.core.authn.services.v1.RefreshTokenResponse",
            json!({
                "refresh_token": session.auto_refresh_token,
                "session_id": session.auto_session_id,
            }),
            &session.user,
        )
        .await?;
    let access = text(&response, "access_token");
    if !access.is_empty() {
        session.user.bearer_token = access.clone();
        session.sentinels.insert(ACCESS_SENTINEL, access);
    }
    let rotated = text(&response, "refresh_token");
    if !rotated.is_empty() {
        session.auto_refresh_token = rotated;
    }
    let expires = response
        .get("access_token_expires_in")
        .and_then(Value::as_u64)
        .unwrap_or(240) as f64;
    session.refresh_at =
        Instant::now() + Duration::from_secs_f64((expires * 0.6).clamp(60.0, 240.0));
    Ok(())
}

fn substitute(body: &Value, sentinels: &BTreeMap<&'static str, String>) -> Value {
    match body {
        Value::String(text) => {
            let mut text = text.clone();
            for (sentinel, value) in sentinels {
                if text.contains(sentinel) {
                    text = text.replace(sentinel, value);
                }
            }
            Value::String(text)
        }
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| substitute(item, sentinels))
                .collect(),
        ),
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), substitute(value, sentinels)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn is_capability_skip(rpc: &RpcPlan, outcome: &Outcome) -> bool {
    if outcome.0 != "FAILED_PRECONDITION" || !rpc.capability_optional {
        return false;
    }
    let detail = outcome.1.to_lowercase();
    detail.is_empty()
        || detail.contains("webrtc egress")
        || detail.contains("egress is not enabled")
}

async fn time_cdc_first_event(caller: &mut Caller, plan: &CdcPlan, meta: &Metadata) -> Outcome {
    const PUBLISH_CDC: &str = "/udb.services.v1.DataBroker/PublishCDC";
    const UPSERT: &str = "/udb.services.v1.DataBroker/Upsert";
    let (subscription_type, output) = match caller.method(PUBLISH_CDC) {
        Ok(types) => types,
        Err(err) => return client_error(err),
    };
    let (upsert_type, upsert_output) = match caller.method(UPSERT) {
        Ok(types) => types,
        Err(err) => return client_error(err),
    };
    let subscription = json!({ "context": plan.context, "topic_pattern": "*" });
    let message = match caller.message(subscription_type.full_name(), &subscription) {
        Ok(message) => message,
        Err(err) => return client_error(err),
    };
    let record = json!({
        "record_id": plan.record_id,
        "tenant_id": meta.tenant_id,
        "project_id": meta.project_id,
        "lookup_key": "rs-perf-cdc",
        "payload": "rs-perf-cdc",
        "revision": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or_default(),
    });
    let upsert = json!({
        "context": plan.context,
        "message_type": plan.message_type,
        "record_json": base64::engine::general_purpose::STANDARD.encode(record.to_string()),
        "conflict_fields": plan.conflict_fields,
    });
    let upsert = match caller.message(upsert_type.full_name(), &upsert) {
        Ok(message) => message,
        Err(err) => return client_error(err),
    };
    let channel = caller.broker.clone();
    let mut grpc = tonic::client::Grpc::new(channel);
    if let Err(err) = grpc.ready().await {
        return client_error(format!("channel not ready: {err}"));
    }
    let request = match caller.request(message, meta) {
        Ok(request) => request,
        Err(status) => return failed(&status),
    };
    let flow = async {
        let path = PathAndQuery::from_static(PUBLISH_CDC);
        let mut stream = grpc
            .server_streaming(request, path, DynamicCodec { output })
            .await?
            .into_inner();
        // A failed write is not this RPC's failure: the stream still has to
        // deliver an event for the row the seed created.
        let _ = caller.unary(UPSERT, upsert, upsert_output, meta).await;
        stream.message().await?;
        Ok::<(), Status>(())
    };
    match tokio::time::timeout(STREAM_TIMEOUT, flow).await {
        Ok(Ok(())) => ("OK".to_string(), String::new()),
        Ok(Err(status)) => failed(&status),
        Err(_) => (
            "DEADLINE_EXCEEDED".to_string(),
            "no CDC event delivered".to_string(),
        ),
    }
}

async fn time_one(
    caller: &mut Caller,
    session: &mut Session,
    plan: &Plan,
    rpc: &RpcPlan,
) -> Result<(f64, Outcome), String> {
    ensure_fresh(caller, session).await?;
    let meta = if rpc.platform {
        session.platform.clone()
    } else {
        session.user.clone()
    };
    let start = Instant::now();
    if rpc.kind == "cdc_first_event" {
        let outcome = time_cdc_first_event(caller, &plan.cdc, &meta).await;
        return Ok((start.elapsed().as_secs_f64() * 1000.0, outcome));
    }
    let Some(body) = &rpc.request else {
        let detail = rpc
            .missing_body
            .clone()
            .unwrap_or_else(|| "no seeded body".into());
        return Ok((
            start.elapsed().as_secs_f64() * 1000.0,
            ("SKIP_NO_BODY".into(), detail),
        ));
    };
    let body = substitute(body, &session.sentinels);
    let message = match caller.message(&rpc.input_type, &body) {
        Ok(message) => message,
        Err(err) => return Ok((start.elapsed().as_secs_f64() * 1000.0, client_error(err))),
    };
    let start = Instant::now();
    let mut outcome = if rpc.client_streaming || rpc.server_streaming {
        caller.first_response(rpc, message, &meta).await
    } else {
        match caller.output(&rpc.output_type) {
            Ok(output) => match caller.unary(&rpc.path, message, output, &meta).await {
                Ok(_) => ("OK".to_string(), String::new()),
                Err(status) => failed(&status),
            },
            Err(err) => client_error(err),
        }
    };
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    if is_capability_skip(rpc, &outcome) {
        outcome.0 = "CAPABILITY_SKIPPED".to_string();
    }
    if outcome.0 == "OK" {
        if let Some(reset) = &rpc.reset_after_ok {
            let reset = substitute(reset, &session.sentinels);
            if let (Ok(message), Ok(output)) = (
                caller.message(&rpc.input_type, &reset),
                caller.output(&rpc.output_type),
            ) {
                let user = session.user.clone();
                let _ = caller.unary(&rpc.path, message, output, &user).await;
            }
        }
    }
    Ok((elapsed, outcome))
}

fn percentile(sorted: &[f64], p: usize) -> f64 {
    sorted[(p * (sorted.len() - 1) / 100).min(sorted.len() - 1)]
}

async fn measure(
    caller: &mut Caller,
    session: &mut Session,
    plan: &Plan,
    samples: &mut Vec<Sample>,
) -> Result<(), String> {
    let username = required_env("UDB_LIVE_USERNAME");
    let password = required_env("UDB_LIVE_PASSWORD");
    for rpc in &plan.rpcs {
        if rpc.reauthenticate_before {
            // Tenant-wide revocation ran earlier; the terminal PurgeTenant needs
            // a bearer minted after that cutoff.
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut last = String::new();
            let mut fresh = None;
            while Instant::now() < deadline {
                let base = session.user.clone().with_bearer_token("");
                match login(
                    caller,
                    &base,
                    &username,
                    &password,
                    "rust-sdk-perf-final-purge",
                )
                .await
                {
                    Ok(credentials) => {
                        match principal(caller, &base, &credentials.access_token).await {
                            Ok(_) => {
                                fresh = Some(credentials);
                                break;
                            }
                            Err(err) => last = err,
                        }
                    }
                    Err(err) => last = err,
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let credentials = fresh.ok_or_else(|| {
                format!("fresh login remained revoked after tenant cutoff: {last}")
            })?;
            session.bind(&credentials);
        }
        println!(
            "[PERF-RPC] {} kind={} iters={}",
            rpc.path, rpc.kind, rpc.iters
        );
        let mut runs: Vec<(f64, Outcome)> = Vec::new();
        if rpc.warm_up {
            let warm = time_one(caller, session, plan, rpc).await?;
            if warm.1 .0 != "OK" {
                runs.push(warm);
            }
        }
        while runs.len() < rpc.iters.max(1) {
            let run = time_one(caller, session, plan, rpc).await?;
            let stop = run.1 .0 != "OK";
            runs.push(run);
            if stop {
                break;
            }
        }
        let ok = runs.iter().any(|(_, outcome)| outcome.0 == "OK");
        let (mut err, err_detail) = runs
            .iter()
            .find(|(_, outcome)| outcome.0 != "OK")
            .map(|(_, outcome)| outcome.clone())
            .unwrap_or_else(|| ("OK".to_string(), String::new()));
        if ok && err == "CAPABILITY_SKIPPED" {
            err = "FAILED_PRECONDITION".to_string();
        }
        let mut durations: Vec<f64> = runs.iter().map(|(ms, _)| *ms).collect();
        durations.sort_by(f64::total_cmp);
        let mean = durations.iter().sum::<f64>() / durations.len() as f64;
        if err != "OK" && err != "CAPABILITY_SKIPPED" {
            println!("[PERF-FAIL] {} => {err}: {err_detail}", rpc.path);
        } else {
            println!("[PERF-OK] {}", rpc.path);
        }
        samples.push(Sample {
            service: rpc.service.clone(),
            rpc: rpc.rpc.clone(),
            api_alias: rpc.api_alias.clone(),
            operation_id: rpc.operation_id.clone(),
            kind: rpc.kind.clone(),
            iters: runs.len(),
            err,
            err_detail,
            p50: percentile(&durations, 50),
            p99: percentile(&durations, 99),
            mean,
        });
    }
    Ok(())
}

async fn prepare(plan: &Plan) -> Result<(Caller, Session), String> {
    if plan.schema_version != 1 {
        return Err(format!(
            "unsupported benchmark plan schema {}",
            plan.schema_version
        ));
    }
    let target = required_env("UDB_GRPC_TARGET");
    let auth_target = std::env::var("UDB_AUTH_GRPC_TARGET").unwrap_or_else(|_| target.clone());
    let connect = |address: String| async move {
        Endpoint::from_shared(endpoint(&address))
            .map_err(|err| format!("{address}: {err}"))?
            .connect()
            .await
            .map_err(|err| format!("connect {address}: {err}"))
    };
    let pool = DescriptorPool::decode(udb_client::FILE_DESCRIPTOR_SET)
        .map_err(|err| format!("SDK descriptor set: {err}"))?;
    let mut caller = Caller {
        pool,
        broker: connect(target.clone()).await?,
        native: connect(auth_target).await?,
        calls: 0,
    };
    let mut base =
        Metadata::new(std::env::var("UDB_LIVE_TENANT").unwrap_or_else(|_| "sdk-live".into()))
            .with_project(std::env::var("UDB_LIVE_PROJECT").unwrap_or_else(|_| "default".into()));
    base.purpose = "rust.live.perf".into();
    base.service_identity = "rust.sdk.perf".into();
    let username = required_env("UDB_LIVE_USERNAME");
    let password = required_env("UDB_LIVE_PASSWORD");
    let first = login(&mut caller, &base, &username, &password, "rust-sdk-perf").await?;
    let who = principal(&mut caller, &base, &first.access_token).await?;
    let tenant = text(&who, "tenant_id");
    if tenant != plan.tenant_id || plan.project_id != base.project_id {
        return Err("the benchmark plan was exported for another tenant or project".into());
    }
    base.tenant_id = tenant;
    let platform_login = login(
        &mut caller,
        &base,
        &required_env("UDB_LIVE_PLATFORM_USERNAME"),
        &required_env("UDB_LIVE_PLATFORM_PASSWORD"),
        "rust-sdk-perf-platform",
    )
    .await?;
    let platform_who = principal(&mut caller, &base, &platform_login.access_token).await?;
    if !has_role(&platform_who, "platform_admin") {
        return Err("the platform fixture did not issue the reserved platform_admin role".into());
    }
    let mut platform = base.clone().with_bearer_token(platform_login.access_token);
    platform.tenant_id = text(&platform_who, "tenant_id");

    // Independent sessions, as in the other harnesses, so the session-mutating
    // RPCs measured early (RefreshToken, RefreshSession) cannot invalidate the
    // credentials later rows depend on.
    let token = login(
        &mut caller,
        &base,
        &username,
        &password,
        "rust-sdk-perf-token",
    )
    .await?;
    let refresh = login(
        &mut caller,
        &base,
        &username,
        &password,
        "rust-sdk-perf-refresh",
    )
    .await?;
    let session_login = login(
        &mut caller,
        &base,
        &username,
        &password,
        "rust-sdk-perf-session",
    )
    .await?;
    let auto = login(
        &mut caller,
        &base,
        &username,
        &password,
        "rust-sdk-perf-auto-refresh",
    )
    .await?;
    let mut sentinels = BTreeMap::new();
    sentinels.insert(ACCESS_SENTINEL, token.access_token.clone());
    sentinels.insert(CSRF_SENTINEL, token.csrf_token.clone());
    sentinels.insert(REFRESH_SENTINEL, refresh.refresh_token.clone());
    sentinels.insert(SESSION_SENTINEL, session_login.session_id.clone());
    let session = Session {
        user: base.clone().with_bearer_token(first.access_token),
        platform,
        sentinels,
        auto_refresh_token: auto.refresh_token,
        auto_session_id: auto.session_id,
        refresh_at: Instant::now()
            + Duration::from_secs_f64((auto.expires_in as f64 * 0.6).clamp(60.0, 180.0)),
    };
    Ok((caller, session))
}

#[tokio::test(flavor = "multi_thread")]
async fn live_perf_measures_every_rpc() {
    if std::env::var("UDB_LIVE_SDK_TESTS").as_deref() != Ok("1")
        || std::env::var("UDB_LIVE_PERF").as_deref() != Ok("1")
    {
        eprintln!("skipped: the live benchmark needs UDB_LIVE_SDK_TESTS=1 and UDB_LIVE_PERF=1");
        return;
    }
    let plan_path = required_env("UDB_BENCH_PLAN");
    let plan: Plan = serde_json::from_str(
        &std::fs::read_to_string(&plan_path)
            .unwrap_or_else(|err| panic!("read {plan_path}: {err}")),
    )
    .unwrap_or_else(|err| panic!("decode {plan_path}: {err}"));
    let mut samples = Vec::new();
    let result = async {
        let (mut caller, mut session) = prepare(&plan).await?;
        measure(&mut caller, &mut session, &plan, &mut samples).await
    }
    .await;
    match result {
        Ok(()) => write_report(&samples, &plan.seeded_fixture_keys, &plan.tenant_id, None),
        Err(err) => {
            write_report(
                &samples,
                &plan.seeded_fixture_keys,
                &plan.tenant_id,
                Some(&err),
            );
            panic!("live benchmark failed: {err}");
        }
    }
}

#[test]
fn the_sdk_descriptor_set_covers_every_generated_rpc() {
    let pool = DescriptorPool::decode(udb_client::FILE_DESCRIPTOR_SET).expect("descriptor set");
    let mut missing = Vec::new();
    for spec in udb_client::generated_rpcs::RPCS {
        let service = pool.get_service_by_name(spec.service_full);
        if !service
            .is_some_and(|service| service.methods().any(|method| method.name() == spec.method))
        {
            missing.push(spec.path);
        }
    }
    assert!(
        missing.is_empty(),
        "RPCs without a typed client in the SDK: {missing:?}"
    );
}
