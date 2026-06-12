//! Neutral logical IR for UDB — the source-of-truth shape every gRPC RPC
//! lowers to before any backend-specific compilation happens.
//!
//! This module is **purely a data model** (U2 step 1). No I/O, no async,
//! no manifest lookup — those land in `crate::ir::compile` (step 2+) and
//! the runtime dispatcher. Splitting the IR from its compilers keeps the
//! types reusable from the SDKs (via gRPC), from the planner, and from
//! tests.
//!
//! ## Why a neutral IR?
//!
//! Today's generic-dispatch handler accepts a backend-shaped JSON blob
//! (`{"sql":"SELECT …"}` for Postgres, `{"collection":…, "filter":{…}}`
//! for Mongo, …) and lets each executor re-parse it. That's:
//!
//! - **Not portable.** Callers must know which backend a logical message
//!   lives on, which contradicts the manifest-driven routing promise.
//! - **Not safe.** Raw SQL/JSON is hard to audit, hard to authorise, and
//!   hard to cache. The IR makes every operation introspectable.
//! - **Not federated.** A read that needs Postgres rows + Qdrant vectors
//!   has no shared shape today; the multi-leg planner in U2 step 6 needs a
//!   common input to scatter across backends.
//!
//! Adding the IR doesn't deprecate the raw path immediately — that's the
//! `raw_backend_request` escape hatch (U2 step 5). The neutral path
//! becomes the default; the raw path stays available behind a policy flag.
//!
//! ## Layout
//!
//! - [`value`]   — `LogicalValue` (the scalar/array universe)
//! - [`filter`]  — `LogicalFilter` + `ComparisonOp` (predicates)
//! - [`projection`] — sort, pagination, field selection
//! - [`operations`] — `LogicalRead`/`Write`/`Delete`/`Search`/`ResourceOp`/`Aggregate`

pub mod cache_key;
pub mod compile;
pub mod filter;
pub mod operations;
pub mod plan;
pub mod projection;
pub mod raw_dispatch;
pub mod value;

#[cfg(test)]
mod cross_backend_tests;

pub use filter::{ComparisonOp, LogicalFilter};
pub use operations::{
    AggregateExpr, AggregateFunc, ConflictStrategy, LogicalAggregate, LogicalAssignment,
    LogicalDelete, LogicalRead, LogicalRecord, LogicalResourceOp, LogicalSearch, LogicalUpdate,
    LogicalWrite, ResourceKind, ResourceOpKind,
};
pub use projection::{LogicalPagination, LogicalProjection, LogicalSort, NullOrder, SortDirection};
pub use value::LogicalValue;
