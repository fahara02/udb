//! Pure-Rust step execution for the native `AssetService`: the step outcome type,
//! the byte-step classifier + parameter parser, the derived-object key/registration
//! helpers, and the sync metadata-step registry (EMBED/EXTRACT). The byte-IO step
//! transforms live in the feature-gated [`image`] / [`transcode`] submodules.
//! Extracted verbatim from the former god file.

use sqlx::PgPool;
use uuid::Uuid;

use crate::proto::udb::core::asset::entity::v1 as asset_entity_pb;
use crate::runtime::native_catalog::native_model;

pub(crate) mod image;
pub(crate) mod transcode;

pub(crate) enum StepOutcome {
    Completed(serde_json::Value),
    Failed(String),
}

/// THUMBNAIL/RESIZE/TRANSCODE are byte-IO steps run async
/// (fetch→transform→store) outside the sync metadata-step registry.
pub(crate) fn is_byte_step(step_type: i32) -> bool {
    use asset_entity_pb::StepType as T;
    matches!(
        T::try_from(step_type),
        Ok(T::Thumbnail) | Ok(T::Resize) | Ok(T::Transcode)
    )
}

/// Optional transform parameters carried by a pipeline-definition step element
/// (the step JSON object). Read from a `params` sub-object, falling back to the
/// step's top-level keys, so a definition can declare either:
///   `{"type":"RESIZE","params":{"width":800,"height":600,"format":"jpeg"}}`
/// or the flattened `{"type":"RESIZE","width":800,"format":"jpeg"}`.
/// All fields are optional; absent/zero/invalid values fall back to per-step
/// defaults (THUMBNAIL → 256², RESIZE → required, format → PNG).
// Fields are consumed by the image RESIZE/CONVERT path, which is `asset-image`-gated;
// without that feature the parsed params are intentionally unused (not dead code).
#[cfg_attr(not(feature = "asset-image"), allow(dead_code))]
#[derive(Debug, Clone, Default)]
pub(crate) struct ByteStepParams {
    pub(crate) width: Option<u32>,
    pub(crate) height: Option<u32>,
    pub(crate) format: Option<String>,
}

/// Parse the optional [`ByteStepParams`] from a pipeline-definition step element.
/// Pure JSON reading — cheap and side-effect-free even without `asset-image`, so
/// it is NOT feature-gated (the call site is shared by both builds).
pub(crate) fn parse_byte_step_params(el: &serde_json::Value) -> ByteStepParams {
    let params = el.get("params");
    let read_u32 = |key: &str| -> Option<u32> {
        let src = params.and_then(|p| p.get(key)).or_else(|| el.get(key))?;
        src.as_u64()
            .or_else(|| src.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
            .filter(|v| *v > 0)
            .map(|v| v.min(u32::MAX as u64) as u32)
    };
    let format = params
        .and_then(|p| p.get("format"))
        .or_else(|| el.get("format"))
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty());
    ByteStepParams {
        width: read_u32("width"),
        height: read_u32("height"),
        format,
    }
}

/// Object-key prefix for every derived media object so derived blobs live in their own
/// namespace and can NEVER collide with (or shadow) a source `object_key`.
pub(crate) const DERIVED_OBJECT_PREFIX: &str = "derived/";

/// Build the derived object's key under the [`DERIVED_OBJECT_PREFIX`] namespace,
/// tagged by step type + output extension so multiple derivations of one source
/// stay distinct AND unique (the File table's `object_key` is UNIQUE). Fixes the
/// old `{object_key}.thumb.png` collision that shared the source key's prefix.
pub(crate) fn derived_object_key(
    source_key: &str,
    step: asset_entity_pb::StepType,
    ext: &str,
) -> String {
    let tag = step
        .as_str_name()
        .trim_start_matches("STEP_TYPE_")
        .to_ascii_lowercase();
    format!("{DERIVED_OBJECT_PREFIX}{source_key}.{tag}.{ext}")
}

/// Register derived media as a first-class `udb_storage.files` row so it is a
/// tracked object (quota/GC/lifecycle), not an orphan blob. Mirrors
/// [`AssetServiceImpl::resolve_object_key`]'s manifest-driven, tenant-bound raw
/// SQL on the same pool; idempotent via `ON CONFLICT (object_key) DO NOTHING`.
pub(crate) async fn register_derived_file(
    pool: &PgPool,
    tenant_id: Uuid,
    derived_key: &str,
    backend: &str,
    bucket: &str,
    content_type: &str,
    file_type: &str,
    size_bytes: i64,
) -> Result<(), String> {
    let m = native_model(
        "udb.core.storage.entity.v1.File",
        &["file_id", "object_key"],
    );
    let rel = m.relation.clone();
    let filename = derived_key.rsplit('/').next().unwrap_or(derived_key);
    sqlx::query(&format!(
        "INSERT INTO {rel} \
           ({tid}, {fname}, {okey}, {be}, {bk}, {ct}, {sz}, {st}, {ft}) \
         VALUES ($1::UUID, $2, $3, $4, $5, $6, $7, 'ACTIVE', $8) \
         ON CONFLICT ({okey}) DO NOTHING",
        tid = m.q("tenant_id"),
        fname = m.q("filename"),
        okey = m.q("object_key"),
        be = m.q("backend"),
        bk = m.q("bucket"),
        ct = m.q("content_type"),
        sz = m.q("size_bytes"),
        st = m.q("status"),
        ft = m.q("file_type"),
    ))
    .bind(tenant_id)
    .bind(filename)
    .bind(derived_key)
    .bind(backend)
    .bind(bucket)
    .bind(content_type)
    .bind(size_bytes)
    .bind(file_type)
    .execute(pool)
    .await
    .map_err(|e| format!("insert derived file row failed: {e}"))?;
    Ok(())
}

/// Transform parameters parsed from a pipeline step; the byte-step executor
/// fetches object bytes separately before applying these settings.
pub(crate) struct StepContext<'a> {
    pub(crate) asset_name: &'a str,
    pub(crate) metadata_json: &'a str,
}

/// Executes one asset-pipeline step. Implementations are pure/in-process for v1.
trait AssetStepExecutor: Send + Sync {
    /// The proto StepType enum value this executor handles.
    fn step_type(&self) -> i32;
    fn execute(&self, ctx: &StepContext) -> StepOutcome;
}

/// Signed feature-hashing embedding (the "hashing trick") — a real, deterministic,
/// dependency-free text embedding. Not a neural model, but a legitimate scheme.
fn embed_text(text: &str, dim: usize) -> Vec<f32> {
    let mut v = vec![0f32; dim];
    for token in text.split_whitespace() {
        let mut h: u64 = 0xcbf29ce484222325;
        for b in token.to_ascii_lowercase().bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        let idx = (h % dim as u64) as usize;
        v[idx] += if (h >> 1) & 1 == 0 { 1.0 } else { -1.0 };
    }
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

/// EMBED: signed feature-hashing embedding over `asset_name + metadata`.
struct EmbedStepExecutor;
impl AssetStepExecutor for EmbedStepExecutor {
    fn step_type(&self) -> i32 {
        asset_entity_pb::StepType::Embed as i32
    }
    fn execute(&self, ctx: &StepContext) -> StepOutcome {
        let text = format!("{} {}", ctx.asset_name, ctx.metadata_json);
        let emb = embed_text(&text, 64);
        StepOutcome::Completed(serde_json::json!({ "embedding": emb, "dim": 64 }))
    }
}

/// EXTRACT: trivial text extraction from the available (non-byte) inputs.
struct ExtractStepExecutor;
impl AssetStepExecutor for ExtractStepExecutor {
    fn step_type(&self) -> i32 {
        asset_entity_pb::StepType::Extract as i32
    }
    fn execute(&self, ctx: &StepContext) -> StepOutcome {
        let text = format!("{} {}", ctx.asset_name, ctx.metadata_json);
        StepOutcome::Completed(serde_json::json!({
            "text": text.trim(),
            "chars": text.trim().chars().count(),
        }))
    }
}

/// Registry of step executors keyed by proto `StepType` enum value. Adding a new
/// step type = register an executor in [`StepRegistry::default_registry`]; the
/// pipeline orchestration (`start_pipeline`) never changes.
pub(crate) struct StepRegistry {
    by_type: std::collections::HashMap<i32, Box<dyn AssetStepExecutor>>,
}

impl StepRegistry {
    pub(crate) fn default_registry() -> Self {
        let mut by_type: std::collections::HashMap<i32, Box<dyn AssetStepExecutor>> =
            std::collections::HashMap::new();
        for executor in [
            Box::new(EmbedStepExecutor) as Box<dyn AssetStepExecutor>,
            Box::new(ExtractStepExecutor) as Box<dyn AssetStepExecutor>,
        ] {
            by_type.insert(executor.step_type(), executor);
        }
        Self { by_type }
    }

    /// Dispatch by `step_type`. Unregistered types (incl. media steps) fail
    /// EXPLICITLY with a clear "not yet implemented" message — keeping the
    /// no-capability-lies contract (no faked success).
    pub(crate) fn run(&self, step_type: i32, ctx: &StepContext) -> StepOutcome {
        match self.by_type.get(&step_type) {
            Some(executor) => executor.execute(ctx),
            None => {
                use asset_entity_pb::StepType as T;
                let name = T::try_from(step_type)
                    .map(|t| t.as_str_name())
                    .unwrap_or("STEP_TYPE_UNSPECIFIED");
                StepOutcome::Failed(format!(
                    "step type {name} not yet implemented \
                     (needs object-store integration + asset-media)"
                ))
            }
        }
    }
}

/// Process-wide default registry, built once. Adding a step type means editing
/// [`StepRegistry::default_registry`] only — not this accessor or `start_pipeline`.
pub(crate) fn step_registry() -> &'static StepRegistry {
    static REGISTRY: std::sync::OnceLock<StepRegistry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(StepRegistry::default_registry)
}
