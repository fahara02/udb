//! Projection drift reconciliation (U15).
//!
//! `ReconciliationWorker` (in `runtime::projection`) already handles
//! **task-level** drift — DEAD_LETTER tasks get re-enqueued, stale
//! IN_PROGRESS tasks reset to PENDING, and the source table is replayed.
//! That covers "the projection worker failed and gave up" but **not**
//! "the target backend was directly mutated and the projection now
//! disagrees with the source row."
//!
//! U15's `DriftScanner` covers that case. It:
//!
//! - Samples or fully scans the canonical Postgres source.
//! - Computes a per-row projection checksum from the source payload +
//!   projection target spec.
//! - Compares against the target's stored checksum (or absence of a row).
//! - Reports divergent rows in `DriftReport.divergent_rows` together with a
//!   repair-cost estimate.
//!
//! ## Detect + repair
//!
//! [`DriftScanner`] *detects* drift (pure logic, no engine handle, testable
//! without live backends). [`repair_drift`] is the *repair* half: it takes a
//! [`DriftReport`] and re-projects every divergent row through
//! [`ProjectionEngine::replay_by_primary_key`], which enqueues an idempotent
//! `upsert` projection task the standard worker then applies to the drifted
//! target. The scanner stays pure; `repair_drift` lives at the runtime layer
//! where the `ProjectionEngine` handle is available.
//!
//! This module is pure logic — actual target reads (Mongo find,
//! Qdrant retrieve, ClickHouse SELECT) are abstracted behind the
//! `TargetChecksumProbe` trait so the scanner is testable without
//! live backends.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::runtime::DataBrokerRuntime;
use crate::runtime::projection::{ProjectionPlan, ProjectionTarget};

/// How aggressively to scan the source for drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanMode {
    /// Random sample of `n` source rows per projection target. Cheap;
    /// the default for hourly background scans. Misses isolated
    /// corruption with probability `(N-n)/N`.
    Sample { rows_per_target: usize },
    /// Walk every source row. Expensive; the operator should set a
    /// concurrency limit and a deadline. Use for compliance audits or
    /// post-incident verification.
    Full,
}

impl Default for ScanMode {
    fn default() -> Self {
        Self::Sample {
            rows_per_target: 100,
        }
    }
}

/// One source row's view as the scanner observed it. Carries the
/// primary-key projection (used to look up the target) and the
/// source-side checksum the target's stored value should match.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SourceSample {
    pub row_key: serde_json::Value,
    pub source_payload: serde_json::Value,
    /// SHA-256 of the canonicalised source payload. Stable across
    /// reruns so the scanner's "did this row drift?" is deterministic.
    pub source_checksum: String,
}

impl SourceSample {
    pub fn new(row_key: serde_json::Value, source_payload: serde_json::Value) -> Self {
        let checksum = checksum_of_payload(&source_payload);
        Self {
            row_key,
            source_payload,
            source_checksum: checksum,
        }
    }
}

/// What the scanner thinks the target should look like for a sample.
/// `TargetChecksumProbe` impls produce this — Mongo reads the document
/// by `_id`, Qdrant retrieves by point id, ClickHouse SELECTs by PK.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TargetObservation {
    pub row_key: serde_json::Value,
    /// `None` when the target has no row for this key (= missing).
    /// `Some(checksum)` otherwise. The scanner compares against the
    /// source's `source_checksum`.
    pub target_checksum: Option<String>,
}

/// One row of drift the scanner detected.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DivergentRow {
    pub row_key: serde_json::Value,
    pub source_checksum: String,
    pub target_checksum: Option<String>,
    pub kind: DivergenceKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DivergenceKind {
    /// Source row exists; target has no row for the key.
    MissingOnTarget,
    /// Source and target both have rows but checksums differ.
    ChecksumMismatch,
}

/// One projection target's drift report.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DriftReport {
    pub target_backend: String,
    pub target_instance: String,
    pub target_resource: String,
    pub source_rows_scanned: usize,
    pub divergent_rows: Vec<DivergentRow>,
    /// Estimated cost of repairing all divergent rows: one enqueue +
    /// one backend mutation per row. The portal renders this as
    /// `n rows × ~$x.xx` based on per-backend price hints.
    pub estimated_repair_cost: RepairCostEstimate,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RepairCostEstimate {
    pub rows_to_repair: usize,
    /// Sum of `cost_units * rows_to_repair`. Operators tune `cost_units`
    /// via labels on the projection target (e.g. expensive vector
    /// rebuild for Qdrant vs cheap upsert for Mongo).
    pub total_cost_units: f64,
}

/// Per-backend hint for repair cost. Defaults reflect the relative
/// expense of each backend's mutation cycle as observed in
/// `metrics::observe_projection_lag_seconds` distributions.
pub fn default_cost_units(backend: &str) -> f64 {
    match backend.to_ascii_lowercase().as_str() {
        "postgres" => 0.5,
        "redis" | "memcached" => 0.2,
        "mongodb" | "neo4j" => 1.0,
        "qdrant" | "weaviate" | "pinecone" => 3.0, // vector rebuild expensive
        "clickhouse" => 2.0,
        "s3" | "minio" | "azureblob" | "gcs" => 1.5,
        _ => 1.0,
    }
}

/// Abstraction over the target backend's "read by key for drift check"
/// operation. The runtime wires this to Mongo `findOne`, Qdrant
/// `points/retrieve`, ClickHouse `SELECT … WHERE pk = ?`, etc. Tests
/// plug a deterministic stub.
#[async_trait::async_trait]
pub trait TargetChecksumProbe: Send + Sync {
    async fn observe(
        &self,
        target: &ProjectionTarget,
        row_key: &serde_json::Value,
    ) -> Result<TargetObservation, String>;
}

/// Runtime-backed drift probe. It uses the configured backend dispatchers
/// instead of direct client calls, so production drift scans exercise the same
/// backend instances/operators configured for the broker.
pub struct RuntimeTargetChecksumProbe {
    runtime: Arc<DataBrokerRuntime>,
}

impl RuntimeTargetChecksumProbe {
    pub fn new(runtime: Arc<DataBrokerRuntime>) -> Self {
        Self { runtime }
    }

    pub fn support_warning(target: &ProjectionTarget) -> Option<String> {
        let backend = target.backend.to_ascii_lowercase();
        match backend.as_str() {
            "postgres" | "mysql" | "sqlite" | "mssql" | "mongodb" | "clickhouse"
            | "elasticsearch" => None,
            _ => Some(format!(
                "projection drift probe is not implemented for backend '{}' resource '{}'",
                target.backend, target.resource_name
            )),
        }
    }

    fn request_for_target(
        target: &ProjectionTarget,
        row_key: &serde_json::Value,
    ) -> Result<String, String> {
        let backend = target.backend.to_ascii_lowercase();
        let request = match backend.as_str() {
            "postgres" => sql_probe_request(target, row_key, SqlProbeDialect::Postgres)?,
            "mysql" => sql_probe_request(target, row_key, SqlProbeDialect::Mysql)?,
            "sqlite" => sql_probe_request(target, row_key, SqlProbeDialect::Sqlite)?,
            "mssql" => sql_probe_request(target, row_key, SqlProbeDialect::Mssql)?,
            "mongodb" => serde_json::json!({
                "collection": target.resource_name,
                "filter": row_key,
                "limit": 1
            }),
            "clickhouse" => serde_json::json!({
                "table": target.resource_name,
                "filter": row_key,
                "limit": 1
            }),
            "elasticsearch" => serde_json::json!({
                "method": "POST",
                "path": format!("/{}/_search", target.resource_name),
                "body": {
                    "query": {"bool": {"filter": row_key_to_elastic_terms(row_key)?}},
                    "size": 1
                }
            }),
            _ => {
                return Err(format!(
                    "projection drift probe is not implemented for backend '{}'",
                    target.backend
                ));
            }
        };
        serde_json::to_string(&request)
            .map_err(|err| format!("failed to encode drift probe request: {err}"))
    }
}

#[async_trait::async_trait]
impl TargetChecksumProbe for RuntimeTargetChecksumProbe {
    async fn observe(
        &self,
        target: &ProjectionTarget,
        row_key: &serde_json::Value,
    ) -> Result<TargetObservation, String> {
        let request = Self::request_for_target(target, row_key)?;
        let response = self
            .runtime
            .query_backend_target(&target.backend, Some(&target.instance), &request)
            .await
            .map_err(|err| format!("drift probe query failed for {}: {err}", target.backend))?;
        let response_json: serde_json::Value = serde_json::from_str(&response)
            .map_err(|err| format!("drift probe response was not JSON: {err}"))?;
        let target_checksum =
            first_payload(&response_json).map(|payload| checksum_of_payload(&payload));
        Ok(TargetObservation {
            row_key: row_key.clone(),
            target_checksum,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DriftScanTargetResult {
    pub report: DriftReport,
    pub warnings: Vec<String>,
}

pub struct DriftScannerWorker {
    scanner: DriftScanner,
    probe: RuntimeTargetChecksumProbe,
}

impl DriftScannerWorker {
    pub fn new(runtime: Arc<DataBrokerRuntime>, mode: ScanMode) -> Self {
        Self {
            scanner: DriftScanner::new(mode),
            probe: RuntimeTargetChecksumProbe::new(runtime),
        }
    }

    pub async fn scan_plan(
        &self,
        plan: &ProjectionPlan,
        samples: &[SourceSample],
    ) -> Vec<DriftScanTargetResult> {
        let mut results = Vec::new();
        for target in &plan.targets {
            if let Some(warning) = RuntimeTargetChecksumProbe::support_warning(target) {
                results.push(DriftScanTargetResult {
                    report: empty_report(target),
                    warnings: vec![warning],
                });
                continue;
            }
            match self.scanner.scan(target, samples, &self.probe).await {
                Ok(report) => results.push(DriftScanTargetResult {
                    report,
                    warnings: Vec::new(),
                }),
                Err(err) => results.push(DriftScanTargetResult {
                    report: empty_report(target),
                    warnings: vec![err],
                }),
            }
        }
        results
    }
}

/// Repair every divergent row in a [`DriftReport`] by re-projecting the
/// authoritative source row through the projection engine.
///
/// Closes the detect→repair loop (the scanner only *detects*; this is the
/// repair half). For each `DivergentRow` it calls
/// [`ProjectionEngine::replay_by_primary_key`], which loads the canonical
/// source row by its PK and enqueues an idempotent `upsert` projection task —
/// the standard projection worker then re-applies it to the drifted target.
///
/// `message_type` / `project_id` identify the source the report was scanned
/// against (the scanner is intentionally pure and does not carry them).
/// Returns the number of projection tasks enqueued.
pub async fn repair_drift(
    engine: &crate::runtime::projection::ProjectionEngine,
    manifest: &crate::generation::CatalogManifest,
    project_id: &str,
    message_type: &str,
    report: &DriftReport,
) -> Result<u64, String> {
    let row_keys: Vec<serde_json::Value> = report
        .divergent_rows
        .iter()
        .map(|row| row.row_key.clone())
        .collect();
    let (enqueued, _checkpoint) = engine
        .replay_batch_rows(
            manifest,
            project_id,
            message_type,
            &row_keys,
            DRIFT_REPAIR_BATCH_SIZE,
            None,
        )
        .await?;
    Ok(enqueued)
}

const DRIFT_REPAIR_BATCH_SIZE: usize = 100;

#[derive(Debug, Clone, Copy)]
enum SqlProbeDialect {
    Postgres,
    Mysql,
    Sqlite,
    Mssql,
}

fn sql_probe_request(
    target: &ProjectionTarget,
    row_key: &serde_json::Value,
    dialect: SqlProbeDialect,
) -> Result<serde_json::Value, String> {
    let key = row_key
        .as_object()
        .ok_or_else(|| "projection row key must be a JSON object".to_string())?;
    if key.is_empty() {
        return Err("projection row key must not be empty".to_string());
    }
    let table = quote_qualified_identifier(&target.resource_name, dialect)?;
    let mut params = Vec::new();
    let mut predicates = Vec::new();
    for (idx, (column, value)) in key.iter().enumerate() {
        params.push(value.clone());
        predicates.push(format!(
            "{} = {}",
            quote_identifier(column, dialect)?,
            placeholder(idx + 1, dialect)
        ));
    }
    let sql = match dialect {
        SqlProbeDialect::Mssql => format!(
            "SELECT TOP (1) * FROM {table} WHERE {}",
            predicates.join(" AND ")
        ),
        _ => format!(
            "SELECT * FROM {table} WHERE {} LIMIT 1",
            predicates.join(" AND ")
        ),
    };
    Ok(serde_json::json!({ "sql": sql, "params": params }))
}

fn row_key_to_elastic_terms(row_key: &serde_json::Value) -> Result<Vec<serde_json::Value>, String> {
    let key = row_key
        .as_object()
        .ok_or_else(|| "projection row key must be a JSON object".to_string())?;
    if key.is_empty() {
        return Err("projection row key must not be empty".to_string());
    }
    Ok(key
        .iter()
        .map(|(field, value)| {
            let mut term = serde_json::Map::new();
            term.insert(field.clone(), value.clone());
            serde_json::json!({ "term": term })
        })
        .collect())
}

fn placeholder(index: usize, dialect: SqlProbeDialect) -> String {
    match dialect {
        SqlProbeDialect::Postgres => format!("${index}"),
        SqlProbeDialect::Mysql | SqlProbeDialect::Sqlite => "?".to_string(),
        SqlProbeDialect::Mssql => format!("@P{index}"),
    }
}

fn quote_qualified_identifier(value: &str, dialect: SqlProbeDialect) -> Result<String, String> {
    value
        .split('.')
        .map(|part| quote_identifier(part, dialect))
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join("."))
}

fn quote_identifier(value: &str, dialect: SqlProbeDialect) -> Result<String, String> {
    if value.is_empty()
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        || value.chars().next().is_some_and(|ch| ch.is_ascii_digit())
    {
        return Err(format!("unsafe projection identifier '{value}'"));
    }
    Ok(match dialect {
        SqlProbeDialect::Mysql => format!("`{value}`"),
        SqlProbeDialect::Mssql => format!("[{value}]"),
        SqlProbeDialect::Postgres | SqlProbeDialect::Sqlite => format!("\"{value}\""),
    })
}

fn first_payload(response: &serde_json::Value) -> Option<serde_json::Value> {
    match response {
        serde_json::Value::Array(rows) => rows.first().cloned(),
        serde_json::Value::Object(map) => {
            for key in ["rows", "documents", "results", "items", "hits", "data"] {
                if let Some(value) = map.get(key).and_then(serde_json::Value::as_array)
                    && let Some(first) = value.first()
                {
                    return Some(unwrap_elastic_hit(first));
                }
            }
            if let Some(hits) = map
                .get("hits")
                .and_then(|hits| hits.get("hits"))
                .and_then(serde_json::Value::as_array)
                && let Some(first) = hits.first()
            {
                return Some(unwrap_elastic_hit(first));
            }
            if map.get("found").and_then(serde_json::Value::as_bool) == Some(false) {
                return None;
            }
            Some(response.clone())
        }
        _ => None,
    }
}

fn unwrap_elastic_hit(value: &serde_json::Value) -> serde_json::Value {
    value
        .get("_source")
        .cloned()
        .unwrap_or_else(|| value.clone())
}

fn empty_report(target: &ProjectionTarget) -> DriftReport {
    DriftReport {
        target_backend: target.backend.clone(),
        target_instance: target.instance.clone(),
        target_resource: target.resource_name.clone(),
        source_rows_scanned: 0,
        divergent_rows: Vec::new(),
        estimated_repair_cost: RepairCostEstimate {
            rows_to_repair: 0,
            total_cost_units: 0.0,
        },
    }
}

/// Pure-logic scanner. Takes source samples + a probe, produces a
/// drift report. The runtime feeds it source rows via
/// `ProjectionEngine::load_source_rows`; tests feed it canned vectors.
pub struct DriftScanner {
    pub mode: ScanMode,
}

impl Default for DriftScanner {
    fn default() -> Self {
        Self {
            mode: ScanMode::default(),
        }
    }
}

impl DriftScanner {
    pub fn new(mode: ScanMode) -> Self {
        Self { mode }
    }

    /// Compare a sequence of source samples against the target via
    /// `probe`. Returns one `DriftReport` per target. Pure async logic
    /// — no DB connection assumed beyond what `probe` provides.
    pub async fn scan(
        &self,
        target: &ProjectionTarget,
        samples: &[SourceSample],
        probe: &dyn TargetChecksumProbe,
    ) -> Result<DriftReport, String> {
        let limited = self.limit_samples(samples);
        let mut divergent = Vec::new();
        // Dedup by (kind, row_key): the same key can be sampled more than once,
        // and without this the report (and the rows_to_repair cost estimate) is
        // inflated by duplicates even though repair dedups via idempotency_key.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for sample in &limited {
            let observation = probe.observe(target, &sample.row_key).await?;
            let (target_checksum, kind) = match &observation.target_checksum {
                None => (None, DivergenceKind::MissingOnTarget),
                Some(target_sum) if target_sum != &sample.source_checksum => {
                    (Some(target_sum.clone()), DivergenceKind::ChecksumMismatch)
                }
                Some(_) => continue,
            };
            if !seen.insert(format!("{kind:?}|{:?}", sample.row_key)) {
                continue;
            }
            divergent.push(DivergentRow {
                row_key: sample.row_key.clone(),
                source_checksum: sample.source_checksum.clone(),
                target_checksum,
                kind,
            });
        }
        let cost_units = default_cost_units(&target.backend);
        let rows_to_repair = divergent.len();
        Ok(DriftReport {
            target_backend: target.backend.clone(),
            target_instance: target.instance.clone(),
            target_resource: target.resource_name.clone(),
            source_rows_scanned: limited.len(),
            divergent_rows: divergent,
            estimated_repair_cost: RepairCostEstimate {
                rows_to_repair,
                total_cost_units: rows_to_repair as f64 * cost_units,
            },
        })
    }

    /// Aggregate per-target reports into a single summary the portal
    /// renders as "scanned N rows, found M divergent, estimated repair
    /// cost X units across {backend, instance} pairs".
    pub fn summarise(reports: &[DriftReport]) -> DriftSummary {
        let mut by_target: BTreeMap<(String, String), TargetSummary> = BTreeMap::new();
        for r in reports {
            let entry = by_target
                .entry((r.target_backend.clone(), r.target_instance.clone()))
                .or_default();
            entry.scanned += r.source_rows_scanned;
            entry.divergent += r.divergent_rows.len();
            entry.estimated_cost += r.estimated_repair_cost.total_cost_units;
        }
        DriftSummary {
            per_target: by_target
                .into_iter()
                .map(|((backend, instance), s)| TargetSummaryEntry {
                    backend,
                    instance,
                    scanned: s.scanned,
                    divergent: s.divergent,
                    estimated_cost: s.estimated_cost,
                })
                .collect(),
        }
    }

    fn limit_samples<'a>(&self, all: &'a [SourceSample]) -> Vec<&'a SourceSample> {
        match self.mode {
            ScanMode::Full => all.iter().collect(),
            ScanMode::Sample { rows_per_target } => {
                let take = rows_per_target.min(all.len());
                // A first-N prefix (`&all[..take]`) never detects corruption
                // beyond row N. Instead order rows by their `source_checksum`
                // (a SHA-256 — uniformly distributed over the keyspace) and take
                // the first N. This yields a sample spread across ALL rows, yet
                // stays deterministic across reruns (no RNG dependency) so the
                // scan is reproducible.
                let mut idx: Vec<usize> = (0..all.len()).collect();
                idx.sort_by(|&a, &b| all[a].source_checksum.cmp(&all[b].source_checksum));
                idx.truncate(take);
                idx.into_iter().map(|i| &all[i]).collect()
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
struct TargetSummary {
    scanned: usize,
    divergent: usize,
    estimated_cost: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DriftSummary {
    pub per_target: Vec<TargetSummaryEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TargetSummaryEntry {
    pub backend: String,
    pub instance: String,
    pub scanned: usize,
    pub divergent: usize,
    pub estimated_cost: f64,
}

/// SHA-256 of a JSON payload after canonicalisation (sorted keys, no
/// whitespace). The scanner uses this on both sides — for source rows
/// (here) and inside `TargetChecksumProbe` impls — so identical values
/// hash identically regardless of map ordering.
pub fn checksum_of_payload(payload: &serde_json::Value) -> String {
    let canonical = canonical_json(payload);
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn canonical_json(value: &serde_json::Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &serde_json::Value, out: &mut String) {
    use serde_json::Value::*;
    match value {
        Null => out.push_str("null"),
        Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Number(n) => out.push_str(&n.to_string()),
        String(s) => {
            out.push('"');
            for ch in s.chars() {
                match ch {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    c => out.push(c),
                }
            }
            out.push('"');
        }
        Array(arr) => {
            out.push('[');
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        Object(map) => {
            out.push('{');
            let mut keys: Vec<&std::string::String> = map.keys().collect();
            keys.sort();
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push('"');
                out.push_str(k);
                out.push_str("\":");
                write_canonical(map.get(*k).expect("canonical key exists"), out);
            }
            out.push('}');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn target(backend: &str) -> ProjectionTarget {
        ProjectionTarget {
            projection_kind: "document".into(),
            backend: backend.into(),
            instance: "default".into(),
            resource_name: "customers".into(),
            write_policy: "primary".into(),
            fanout_policy: "outbox".into(),
            options: vec![],
        }
    }

    /// Stub probe: returns canned target observations keyed by row id.
    struct StubProbe {
        observations: BTreeMap<String, Option<String>>,
    }

    #[async_trait::async_trait]
    impl TargetChecksumProbe for StubProbe {
        async fn observe(
            &self,
            _target: &ProjectionTarget,
            row_key: &serde_json::Value,
        ) -> Result<TargetObservation, String> {
            let id = row_key
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            Ok(TargetObservation {
                row_key: row_key.clone(),
                target_checksum: self.observations.get(&id).cloned().flatten(),
            })
        }
    }

    fn sample(id: &str, payload: serde_json::Value) -> SourceSample {
        SourceSample::new(json!({ "id": id }), payload)
    }

    /// **U15 acceptance gate**: "Corrupting a Mongo/Qdrant/ClickHouse
    /// projection row is detected and repaired." This test pins the
    /// detect path — a target whose checksum disagrees with the source
    /// shows up in `divergent_rows`.
    #[tokio::test]
    async fn checksum_mismatch_is_detected_as_divergent() {
        let source = vec![sample("cust-1", json!({ "name": "Alice" }))];
        let mut observations = BTreeMap::new();
        // Target reports a stale checksum — drift!
        observations.insert("cust-1".to_string(), Some("stale-checksum".to_string()));
        let probe = StubProbe { observations };

        let scanner = DriftScanner::new(ScanMode::Full);
        let report = scanner
            .scan(&target("mongodb"), &source, &probe)
            .await
            .unwrap();
        assert_eq!(report.divergent_rows.len(), 1);
        assert_eq!(
            report.divergent_rows[0].kind,
            DivergenceKind::ChecksumMismatch
        );
        assert_eq!(report.estimated_repair_cost.rows_to_repair, 1);
    }

    #[tokio::test]
    async fn missing_target_row_is_detected_as_divergent() {
        let source = vec![sample("cust-2", json!({ "name": "Bob" }))];
        let mut observations = BTreeMap::new();
        // Target has no row for cust-2.
        observations.insert("cust-2".to_string(), None);
        let probe = StubProbe { observations };

        let scanner = DriftScanner::new(ScanMode::Full);
        let report = scanner
            .scan(&target("qdrant"), &source, &probe)
            .await
            .unwrap();
        assert_eq!(report.divergent_rows.len(), 1);
        assert_eq!(
            report.divergent_rows[0].kind,
            DivergenceKind::MissingOnTarget
        );
    }

    #[tokio::test]
    async fn matching_checksums_produce_no_drift() {
        let payload = json!({ "name": "Carol" });
        let source = vec![sample("cust-3", payload.clone())];
        let expected = checksum_of_payload(&payload);
        let mut observations = BTreeMap::new();
        observations.insert("cust-3".to_string(), Some(expected));
        let probe = StubProbe { observations };

        let scanner = DriftScanner::new(ScanMode::Full);
        let report = scanner
            .scan(&target("clickhouse"), &source, &probe)
            .await
            .unwrap();
        assert!(report.divergent_rows.is_empty());
        assert_eq!(report.estimated_repair_cost.rows_to_repair, 0);
    }

    /// Sample mode caps the scan at `rows_per_target` — important for
    /// large source tables where a full scan would be cost-prohibitive.
    #[tokio::test]
    async fn sample_mode_limits_rows_scanned() {
        let source: Vec<SourceSample> = (0..100)
            .map(|i| sample(&format!("cust-{i}"), json!({ "n": i })))
            .collect();
        let mut observations = BTreeMap::new();
        for i in 0..100 {
            observations.insert(format!("cust-{i}"), None); // all missing
        }
        let probe = StubProbe { observations };

        let scanner = DriftScanner::new(ScanMode::Sample {
            rows_per_target: 10,
        });
        let report = scanner
            .scan(&target("mongodb"), &source, &probe)
            .await
            .unwrap();
        assert_eq!(report.source_rows_scanned, 10);
        assert_eq!(report.divergent_rows.len(), 10);
    }

    /// Different backends have different repair costs; the estimator
    /// must reflect that so the portal can show "Qdrant repair will
    /// be expensive" vs "Redis repair is cheap."
    #[test]
    fn repair_cost_varies_by_backend() {
        assert!(default_cost_units("qdrant") > default_cost_units("mongodb"));
        assert!(default_cost_units("redis") < default_cost_units("mongodb"));
        assert!(default_cost_units("clickhouse") > default_cost_units("postgres"));
    }

    /// The payload checksum is canonical — different field ordering or
    /// whitespace mustn't yield different hashes, or every benign
    /// re-serialise would look like drift.
    #[test]
    fn checksum_is_canonical_over_key_order() {
        let a = json!({ "name": "Alice", "age": 30 });
        let b = json!({ "age": 30, "name": "Alice" });
        assert_eq!(checksum_of_payload(&a), checksum_of_payload(&b));
        let c = json!({ "name": "Bob", "age": 30 });
        assert_ne!(checksum_of_payload(&a), checksum_of_payload(&c));
    }

    #[tokio::test]
    async fn summary_aggregates_per_target() {
        let reports = vec![
            DriftReport {
                target_backend: "mongodb".into(),
                target_instance: "primary".into(),
                target_resource: "a".into(),
                source_rows_scanned: 10,
                divergent_rows: vec![],
                estimated_repair_cost: RepairCostEstimate {
                    rows_to_repair: 0,
                    total_cost_units: 0.0,
                },
            },
            DriftReport {
                target_backend: "mongodb".into(),
                target_instance: "primary".into(),
                target_resource: "b".into(),
                source_rows_scanned: 20,
                divergent_rows: vec![DivergentRow {
                    row_key: json!({"id":"x"}),
                    source_checksum: "s".into(),
                    target_checksum: None,
                    kind: DivergenceKind::MissingOnTarget,
                }],
                estimated_repair_cost: RepairCostEstimate {
                    rows_to_repair: 1,
                    total_cost_units: 1.0,
                },
            },
        ];
        let summary = DriftScanner::summarise(&reports);
        assert_eq!(
            summary.per_target.len(),
            1,
            "same (backend, instance) folds"
        );
        assert_eq!(summary.per_target[0].scanned, 30);
        assert_eq!(summary.per_target[0].divergent, 1);
        assert!((summary.per_target[0].estimated_cost - 1.0).abs() < 1e-9);
    }
}
