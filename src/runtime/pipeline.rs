//! Unified request pipeline (U7).
//!
//! Pre-U7 every gRPC handler walked through these phases inline:
//! `security_from_request` → `authorize` → `self.catalog.active().manifest`
//! → broker plan build → channel admission → executor call → audit/metric.
//! That's correct but invisible — there's no shared name for "phase 4 of
//! the read path," so traces, dashboards, and tests can't assert against
//! a stable structure.
//!
//! `RequestPipeline` is the type-safe **shape** of those phases. It does
//! NOT replace the handlers' inline logic (that would be a much bigger
//! refactor); it owns the:
//!
//! - **Phase enum** so observability code groups spans/metrics by
//!   `phase = "execute"` instead of by handler name.
//! - **Tracing-span helpers** that fan a request's correlation_id /
//!   tenant_id / project_id into every span as fields, so one trace can
//!   answer "where did this request spend its 142 ms?".
//! - **Acceptance contract** so the U7 gate ("one trace shows catalog
//!   resolution, routing decision, pool wait, backend call, and response
//!   shaping") can be asserted by name in tests.
//!
//! Handlers opt in incrementally — they create a `RequestPipeline` at
//! the top of `*_inner`, then call `pipeline.enter(Phase::X)` to nest
//! the right span around each phase.

use std::time::Instant;

use serde::Serialize;
use tracing::Span;

use crate::broker::RequestContext;

/// The eight named phases of a UDB request. The `as_str` tokens are the
/// canonical labels for `phase=` span attributes, OTEL resource attributes,
/// and metric labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// `security_from_request` — extract bearer token, mTLS identity,
    /// `x-udb-*` headers.
    SecurityExtract,
    /// `catalog.active_for(project_id)` — resolve the project catalog so
    /// downstream phases see the right manifest.
    CatalogResolve,
    /// `self.authorize(...)` — RBAC scope check + ABAC policy match.
    Authorize,
    /// Build the logical operation (typed select/upsert/etc.) from the
    /// gRPC request body + manifest. Post-U2 this is `crate::ir::compile`.
    BuildLogical,
    /// Compile the logical operation to a backend-specific plan.
    /// `runtime::broker::build_*_plan` for typed paths,
    /// `crate::ir::compile::*::compile_*` for the IR path.
    CompilePlan,
    /// Channel admission, circuit-breaker check, pool lease acquisition.
    AcquireLease,
    /// The actual backend call (sqlx, HTTP, AWS SDK).
    Execute,
    /// `record_grpc` — emit the duration metric, write the audit row,
    /// shape the response.
    EmitAudit,
}

impl Phase {
    /// Stable label for span attributes / metrics. **Pinned** — changing
    /// these breaks downstream dashboards.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SecurityExtract => "security_extract",
            Self::CatalogResolve => "catalog_resolve",
            Self::Authorize => "authorize",
            Self::BuildLogical => "build_logical",
            Self::CompilePlan => "compile_plan",
            Self::AcquireLease => "acquire_lease",
            Self::Execute => "execute",
            Self::EmitAudit => "emit_audit",
        }
    }

    /// Ordered list — useful for "for every phase, …" iteration.
    pub fn all() -> &'static [Phase] {
        &[
            Phase::SecurityExtract,
            Phase::CatalogResolve,
            Phase::Authorize,
            Phase::BuildLogical,
            Phase::CompilePlan,
            Phase::AcquireLease,
            Phase::Execute,
            Phase::EmitAudit,
        ]
    }
}

/// Holds the per-request observability context. One `RequestPipeline` is
/// constructed at the top of a handler; its `enter` returns a `tracing::Span`
/// guarded with the right `phase`, `rpc`, `tenant_id`, `project_id`,
/// and `correlation_id` attributes.
pub struct RequestPipeline<'a> {
    rpc: &'static str,
    context: &'a RequestContext,
    started: Instant,
}

impl<'a> RequestPipeline<'a> {
    /// Create a pipeline for the given RPC + request. Doesn't enter any
    /// span yet — call [`enter`](Self::enter) per phase.
    pub fn new(rpc: &'static str, context: &'a RequestContext) -> Self {
        Self {
            rpc,
            context,
            started: Instant::now(),
        }
    }

    /// Enter a per-phase span with the canonical attribute set. Spans are
    /// `INFO` level so they're visible in default sampling configs; cap
    /// the cardinality with one extra attribute per phase, not per call.
    pub fn enter(&self, phase: Phase) -> Span {
        // `target` matches the canonical OTEL DB span convention so
        // exporters group UDB requests next to other DB-tier spans.
        tracing::info_span!(
            target: "udb.request",
            "udb.request",
            rpc = self.rpc,
            phase = phase.as_str(),
            tenant_id = %self.context.tenant_id,
            project_id = %self.context.project_id,
            correlation_id = %self.context.correlation_id,
            target_backend = %self.context.target_backend,
            target_instance = %self.context.target_instance,
        )
    }

    /// Elapsed since the pipeline was constructed. Use in `EmitAudit` to
    /// record the end-to-end RPC duration.
    pub fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    /// Mark the request as a debug / dry-run. Handlers can read this to
    /// short-circuit `Execute` and return a structured plan response
    /// instead of running the SQL. Not yet wired — the gRPC contract
    /// needs a `dry_run` field first.
    pub fn is_dry_run(_context: &RequestContext) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_context() -> RequestContext {
        RequestContext {
            tenant_id: "acme".into(),
            project_id: "billing".into(),
            correlation_id: "corr-1".into(),
            purpose: "test".into(),
            ..Default::default()
        }
    }

    #[test]
    fn phase_tokens_are_pinned() {
        // SDKs / dashboards reference these by string; pin them.
        assert_eq!(Phase::SecurityExtract.as_str(), "security_extract");
        assert_eq!(Phase::CatalogResolve.as_str(), "catalog_resolve");
        assert_eq!(Phase::Authorize.as_str(), "authorize");
        assert_eq!(Phase::BuildLogical.as_str(), "build_logical");
        assert_eq!(Phase::CompilePlan.as_str(), "compile_plan");
        assert_eq!(Phase::AcquireLease.as_str(), "acquire_lease");
        assert_eq!(Phase::Execute.as_str(), "execute");
        assert_eq!(Phase::EmitAudit.as_str(), "emit_audit");
    }

    #[test]
    fn all_phases_returned_in_order() {
        let names: Vec<&str> = Phase::all().iter().map(|p| p.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "security_extract",
                "catalog_resolve",
                "authorize",
                "build_logical",
                "compile_plan",
                "acquire_lease",
                "execute",
                "emit_audit",
            ]
        );
    }

    #[test]
    fn pipeline_enter_does_not_panic_and_records_elapsed() {
        let ctx = fake_context();
        let pipeline = RequestPipeline::new("Select", &ctx);
        // Just exercise the span constructor — the contents are validated
        // via integration tests with a real tracing subscriber.
        let _guard = pipeline.enter(Phase::Execute).entered();
        let elapsed = pipeline.elapsed_ms();
        assert!(
            elapsed < 1_000,
            "test pipeline elapsed unreasonably: {elapsed}"
        );
    }
}
