//! Static configuration for the native `AssetService`: the entity message names,
//! the stable machine-readable error-reason codes, the default EMBED vector
//! collection, and the versioned outbox topics. Extracted verbatim from the
//! former god file; every value is byte-stable for downstream audit/CDC consumers.

pub(crate) const ASSET_MSG: &str = "udb.core.asset.entity.v1.Asset";
pub(crate) const PIPELINE_DEFINITION_MSG: &str = "udb.core.asset.entity.v1.PipelineDefinition";
pub(crate) const PIPELINE_INSTANCE_MSG: &str = "udb.core.asset.entity.v1.PipelineInstance";
pub(crate) const PIPELINE_STEP_MSG: &str = "udb.core.asset.entity.v1.PipelineStep";

// ── stable machine-readable error reasons ─────────────────────────────────────
// Attached to the matching pipeline failures so SDK callers can branch on a
// stable code instead of parsing human text. The gRPC Status *code* is left
// unchanged at each site. The repo has no `google.rpc.ErrorInfo` status-detail
// infrastructure, so non-OK statuses carry the reason on the `error-reason`
// metadata trailer (uniform with the storage/webrtc/notification services); the
// OK "already started" return carries it in its response `message` body field.
/// The pipeline definition is structurally invalid (e.g. its persisted step
/// list is not valid JSON).
pub(crate) const PIPELINE_DEFINITION_INVALID: &str = "PIPELINE_DEFINITION_INVALID";
/// A definition step declares a `type` the runtime does not support.
pub(crate) const STEP_TYPE_UNSUPPORTED: &str = "STEP_TYPE_UNSUPPORTED";
/// Reserved: the source asset/file required by a step is missing. No hard
/// failure site exists today (a missing asset yields empty step inputs), so this
/// is held for the future byte-step "source object not found" path.
#[allow(dead_code)]
pub(crate) const ASSET_FILE_MISSING: &str = "ASSET_FILE_MISSING";
/// A concurrent start with the same correlation id won the race; the existing
/// instance is returned instead of starting a new pipeline.
pub(crate) const PIPELINE_ALREADY_STARTED: &str = "PIPELINE_ALREADY_STARTED";

/// Vector collection EMBED-step vectors are upserted into, when not overridden by
/// `UDB_ASSET_VECTOR_COLLECTION`.
pub(crate) const DEFAULT_VECTOR_COLLECTION: &str = "udb_asset_embeddings";

// ── outbox topics (dot-only per Kafka topic policy) ───────────────────────────
pub(crate) const ASSET_REGISTERED_TOPIC: &str = "udb.asset.asset.registered.v1";
pub(crate) const PIPELINE_STARTED_TOPIC: &str = "udb.asset.pipeline.started.v1";
pub(crate) const PIPELINE_STEP_COMPLETED_TOPIC: &str = "udb.asset.pipeline.step_completed.v1";
pub(crate) const PIPELINE_COMPLETED_TOPIC: &str = "udb.asset.pipeline.completed.v1";
pub(crate) const PIPELINE_FAILED_TOPIC: &str = "udb.asset.pipeline.failed.v1";
