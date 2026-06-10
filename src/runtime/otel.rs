//! Phase 10: OpenTelemetry trace propagation for the native control plane.
//!
//! This module wires W3C trace-context through the gRPC boundary in two layers,
//! deliberately split so the slim, no-`otel` build keeps compiling:
//!
//!   * **Always compiled (pure plumbing, no otel crates).** A [`tokio::task_local`]
//!     holds the current [`TraceContext`]; [`current_trace_context`] reads it (an
//!     empty default outside any scope) and [`scope`] runs a future inside a given
//!     context. [`TraceExtractLayer`] is a tonic-compatible tower layer that reads
//!     the inbound `traceparent` / `tracestate` / `x-correlation-id` request
//!     metadata, builds a `TraceContext`, and runs the inner service inside the
//!     task-local scope. It mirrors `MethodSecurityLayer` (a `wrap`-based tower
//!     `Service`) so the parent mounts it the same way.
//!
//!   * **Feature-gated OTLP export (`#[cfg(feature = "otel")]`).** [`init_otel`]
//!     installs a `tracing-opentelemetry` OTLP layer onto the subscriber when
//!     `UDB_OTEL_ENABLED` is truthy or `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
//!     Without the feature it is a no-op that logs once. The default (no-`otel`)
//!     `cargo check` must stay green, so every otel-crate reference is gated.
//!
//! The trace context this module scopes is what the compliance-envelope builders
//! and the CDC→Kafka publish path read via [`current_trace_context`], so an
//! inbound `traceparent` flows all the way to the audit row and the egress Kafka
//! header (continuing the distributed trace across the broker).

use tonic::body::BoxBody;
use tonic::codegen::http;
use tonic::codegen::{Context, Future, Pin, Poll, Service};

pub use crate::runtime::observability::TraceContext;

tokio::task_local! {
    /// The trace context for the currently-executing request. Set by
    /// [`TraceExtractLayer`] (or [`scope`]) for the duration of the inner future;
    /// outside any scope, [`current_trace_context`] returns an empty default.
    static CURRENT_TRACE: TraceContext;
}

/// The trace context for the current task, or an empty default when none is in
/// scope. Cheap to call from any depth inside a request (envelope builders, the
/// authz boundary, the CDC publish path) — it clones the scoped value.
pub fn current_trace_context() -> TraceContext {
    CURRENT_TRACE.try_with(|tc| tc.clone()).unwrap_or_default()
}

/// Run `fut` with `ctx` installed as the current trace context. Any
/// [`current_trace_context`] call inside `fut` (transitively) observes `ctx`.
pub async fn scope<F>(ctx: TraceContext, fut: F) -> F::Output
where
    F: Future,
{
    CURRENT_TRACE.scope(ctx, fut).await
}

/// The authenticated principal + authorization decision for the current request,
/// established at the method-security gate: the validated `subject`, the credential
/// type it authenticated with (`auth_method`), the `decision_id` of the gate
/// authorization, and the `policy_revision` (contract version) whose
/// endpoint-security rules the gate enforced. All four are scoped together and
/// auto-filled onto native compliance events so a gate-authorized native event is
/// traceable to the authorization that permitted it (Phase 10 audit completeness).
#[derive(Clone, Default)]
pub struct RequestPrincipal {
    pub subject: String,
    pub auth_method: String,
    pub decision_id: String,
    pub policy_revision: String,
}

tokio::task_local! {
    /// The authenticated principal for the current request, set by the
    /// method-security layer AFTER the credential is validated. Empty for public
    /// RPCs or outside any scope. Read centrally by the native outbox path so every
    /// native compliance event records the real `actor`/`auth_method` without each
    /// handler threading it (Phase 10).
    static CURRENT_PRINCIPAL: RequestPrincipal;
}

/// The authenticated subject for the current request, or empty when none is in
/// scope (public RPC, or called outside a request). Phase 10 audit `actor` source.
pub fn current_actor() -> String {
    CURRENT_PRINCIPAL
        .try_with(|p| p.subject.clone())
        .unwrap_or_default()
}

/// The credential type the current principal authenticated with (e.g.
/// `bearer_jwt`), or empty outside an authenticated request. Phase 10 audit
/// `auth_method` source.
pub fn current_auth_method() -> String {
    CURRENT_PRINCIPAL
        .try_with(|p| p.auth_method.clone())
        .unwrap_or_default()
}

/// The id of the method-security gate authorization decision for the current
/// request, or empty for public/unauthenticated calls. Phase 10 audit
/// `decision_id` source for gate-authorized native events.
pub fn current_decision_id() -> String {
    CURRENT_PRINCIPAL
        .try_with(|p| p.decision_id.clone())
        .unwrap_or_default()
}

/// The policy (contract) revision whose endpoint-security rules authorized the
/// current request at the method-security gate, or empty for public calls. Phase
/// 10 audit `policy_revision` source for gate-authorized native events.
pub fn current_policy_revision() -> String {
    CURRENT_PRINCIPAL
        .try_with(|p| p.policy_revision.clone())
        .unwrap_or_default()
}

/// Run `fut` with `principal` installed as the current authenticated principal.
/// The method-security layer wraps each authenticated handler in this scope.
pub async fn scope_principal<F>(principal: RequestPrincipal, fut: F) -> F::Output
where
    F: Future,
{
    CURRENT_PRINCIPAL.scope(principal, fut).await
}

/// Build a [`TraceContext`] from inbound gRPC request headers. Reads the W3C
/// `traceparent` (falling back to an empty context if absent/malformed) and
/// enriches it with `x-correlation-id` so logs and the compliance envelope share
/// one correlation id even when no traceparent is present.
fn context_from_headers(headers: &http::HeaderMap) -> TraceContext {
    let mut ctx = headers
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
        .and_then(TraceContext::from_traceparent)
        .unwrap_or_default();

    if let Some(corr) = headers
        .get("x-correlation-id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        ctx.correlation_id = corr.to_string();
    } else if let Some(req_id) = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        // No correlation id supplied — fall back to the request id so the audit
        // row still has a stable correlation token.
        ctx.correlation_id = req_id.to_string();
    }

    ctx
}

/// Tower layer that extracts inbound trace context and runs the wrapped tonic
/// service inside the [`current_trace_context`] task-local scope. Stateless and
/// `Clone`; the parent mounts it like [`crate::runtime::service::method_security::MethodSecurityLayer`]
/// via `wrap`.
#[derive(Clone, Default)]
pub struct TraceExtractLayer;

impl TraceExtractLayer {
    pub fn new() -> Self {
        Self
    }

    /// Wrap a tonic service so each request runs inside the trace-context scope
    /// derived from its inbound `traceparent`. Mirrors `MethodSecurityLayer::wrap`
    /// (an inherent method, not the `tower::Layer` trait) so the wiring reads
    /// `trace.wrap(XServer::new(svc))` and composes with the security layer.
    pub fn wrap<S>(&self, inner: S) -> TraceExtractService<S> {
        TraceExtractService { inner }
    }
}

// Also expose the standard `tower::Layer` trait so the parent can mount trace
// extraction ONCE on the shared `ServiceBuilder` (`make_layer`) that every gRPC
// listener already uses, rather than wrapping each service individually.
impl<S> tower::Layer<S> for TraceExtractLayer {
    type Service = TraceExtractService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        self.wrap(inner)
    }
}

#[derive(Clone)]
pub struct TraceExtractService<S> {
    inner: S,
}

// Preserve the wrapped service's gRPC name so tonic's router still dispatches it.
impl<S> tonic::server::NamedService for TraceExtractService<S>
where
    S: tonic::server::NamedService,
{
    const NAME: &'static str = S::NAME;
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for TraceExtractService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = http::Response<BoxBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<http::Response<BoxBody>, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let ctx = context_from_headers(req.headers());
        let path = req.uri().path().to_string();
        // Standard tower clone-and-swap: `self.inner` is the copy `poll_ready`
        // readied, so drive it to completion and leave a fresh clone behind for
        // the next request (mirrors how tonic-generated services dispatch).
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        // One server span per gRPC request, named by the method path. This
        // instruments EVERY handler on EVERY listener in one place (Phase 10
        // "spans through gRPC handlers"); deeper `#[tracing::instrument]` spans
        // (executor calls, CDC publish, saga compensation, authz decisions) nest
        // under it via the tracing span tree.
        let span = tracing::info_span!(
            "grpc.request",
            "otel.name" = %path,
            "otel.kind" = "server",
            correlation_id = %ctx.correlation_id,
            trace_id = %ctx.trace_id,
        );
        // Link the broker span to the inbound W3C trace so a distributed trace
        // started at the SDK continues through the broker (otel build only).
        #[cfg(feature = "otel")]
        set_span_parent_from_inbound(&span, &ctx);
        let fut = inner.call(req);
        let scoped = CURRENT_TRACE.scope(ctx, fut);
        Box::pin(tracing::Instrument::instrument(scoped, span))
    }
}

/// Parent a broker request span to the inbound W3C trace context so the
/// distributed trace from the SDK continues across the broker. Only meaningful
/// when the `otel` feature is compiled; a no-op otherwise.
#[cfg(feature = "otel")]
fn set_span_parent_from_inbound(span: &tracing::Span, ctx: &TraceContext) {
    use opentelemetry::Context as OtelContext;
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId, TraceState,
    };
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    if ctx.trace_id.is_empty() || ctx.parent_span_id.is_empty() {
        return;
    }
    let (Ok(trace_id), Ok(parent_span)) = (
        TraceId::from_hex(&ctx.trace_id),
        SpanId::from_hex(&ctx.parent_span_id),
    ) else {
        return;
    };
    let remote = SpanContext::new(
        trace_id,
        parent_span,
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    );
    span.set_parent(OtelContext::new().with_remote_span_context(remote));
}

/// Initialize OpenTelemetry OTLP trace export. Only active when the `otel`
/// feature is compiled AND export is requested at runtime (`UDB_OTEL_ENABLED`
/// truthy, or `OTEL_EXPORTER_OTLP_ENDPOINT` set). The trace-context extraction /
/// propagation plumbing (the task-local + [`TraceExtractLayer`]) works regardless
/// of this function — it is metadata→task-local plumbing with no otel crate.
///
/// Returns `true` when an OTLP exporter was installed, `false` otherwise.
///
/// ORDERING: call this at startup before ordinary request handling. When
/// `runtime-logging` is compiled, this installs one subscriber containing both
/// the normal fmt logger and the OTLP tracing layer so enabling OTEL does not
/// suppress human or JSON logs.
#[cfg(feature = "otel")]
pub fn init_otel() -> bool {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig as _;

    if !otel_export_requested() {
        tracing::info!(
            "otel: OTLP export not requested (set UDB_OTEL_ENABLED or OTEL_EXPORTER_OTLP_ENDPOINT)"
        );
        return false;
    }

    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .build()
    {
        Ok(exporter) => exporter,
        Err(e) => {
            tracing::warn!("otel: failed to build OTLP span exporter ({endpoint}): {e}");
            return false;
        }
    };

    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .build();
    let tracer = provider.tracer("udb");
    let _ = opentelemetry::global::set_tracer_provider(provider);

    if init_otel_subscriber(tracer).is_err() {
        tracing::info!(
            "otel: OTLP exporter installed (endpoint={endpoint}); a subscriber was already set"
        );
    } else {
        tracing::info!("otel: OTLP trace export enabled (endpoint={endpoint})");
    }
    true
}

#[cfg(all(feature = "otel", feature = "runtime-logging"))]
fn init_otel_subscriber(
    tracer: opentelemetry_sdk::trace::Tracer,
) -> Result<(), tracing_subscriber::util::TryInitError> {
    use crate::runtime::pretty_log::PrettyLog;
    use tracing_subscriber::{EnvFilter, layer::SubscriberExt as _, util::SubscriberInitExt as _};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_opentelemetry::layer().with_tracer(tracer));

    if log_json_enabled() {
        subscriber
            .with(tracing_subscriber::fmt::layer().json())
            .try_init()
    } else {
        subscriber
            .with(tracing_subscriber::fmt::layer().event_format(PrettyLog {
                colors: log_colors_enabled(),
            }))
            .try_init()
    }
}

#[cfg(all(feature = "otel", not(feature = "runtime-logging")))]
fn init_otel_subscriber(
    tracer: opentelemetry_sdk::trace::Tracer,
) -> Result<(), tracing_subscriber::util::TryInitError> {
    use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()
}

#[cfg(all(feature = "otel", feature = "runtime-logging"))]
fn log_json_enabled() -> bool {
    std::env::var("UDB_LOG_JSON")
        .map(|v| match v.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            other => {
                eprintln!(
                    "unrecognized UDB_LOG_JSON value '{other}'; expected 1/0, true/false, yes/no, or on/off"
                );
                false
            }
        })
        .unwrap_or(false)
}

#[cfg(all(feature = "otel", feature = "runtime-logging"))]
fn log_colors_enabled() -> bool {
    std::env::var("NO_COLOR").is_err()
        && std::env::var("UDB_NO_COLOR")
            .map(|v| match v.to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" | "on" => false,
                "0" | "false" | "no" | "off" => true,
                other => {
                    eprintln!(
                        "unrecognized UDB_NO_COLOR value '{other}'; expected 1/0, true/false, yes/no, or on/off"
                    );
                    true
                }
            })
            .unwrap_or(true)
}

/// No-op when the `otel` feature is not compiled. Logs once so an operator who set
/// `UDB_OTEL_ENABLED` on a slim build understands why no traces are exported.
#[cfg(not(feature = "otel"))]
pub fn init_otel() -> bool {
    if otel_export_requested() {
        tracing::info!(
            "otel: export requested but the `otel` feature is not compiled; rebuild with --features otel"
        );
    }
    false
}

/// True when OTLP export is requested via env: `UDB_OTEL_ENABLED` truthy, or the
/// standard `OTEL_EXPORTER_OTLP_ENDPOINT` set.
fn otel_export_requested() -> bool {
    let enabled = std::env::var("UDB_OTEL_ENABLED")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    enabled
        || std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn current_trace_context_is_empty_outside_scope() {
        let tc = current_trace_context();
        assert!(!tc.is_present(), "no scope ⇒ empty trace context");
    }

    #[tokio::test]
    async fn scope_exposes_the_scoped_context() {
        let ctx = TraceContext {
            trace_id: "0af7651916cd43dd8448eb211c80319c".to_string(),
            span_id: "b7ad6b7169203331".to_string(),
            parent_span_id: String::new(),
            correlation_id: "corr-123".to_string(),
        };
        let observed = scope(ctx.clone(), async { current_trace_context() }).await;
        assert_eq!(observed.trace_id, ctx.trace_id);
        assert_eq!(observed.span_id, ctx.span_id);
        assert_eq!(observed.correlation_id, "corr-123");
        // Leaving the scope restores the empty default.
        assert!(!current_trace_context().is_present());
    }

    #[test]
    fn header_extraction_reads_traceparent_and_correlation() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
                .parse()
                .unwrap(),
        );
        headers.insert("x-correlation-id", "corr-abc".parse().unwrap());
        let ctx = context_from_headers(&headers);
        assert_eq!(ctx.trace_id, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(ctx.parent_span_id, "b7ad6b7169203331");
        assert_eq!(ctx.correlation_id, "corr-abc");
    }

    #[test]
    fn header_extraction_tolerates_missing_traceparent() {
        let headers = http::HeaderMap::new();
        let ctx = context_from_headers(&headers);
        assert!(!ctx.is_present());
    }
}
