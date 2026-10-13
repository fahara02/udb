//! Pipeline units (P6): a declared `topic → mapping → entity` step that the
//! broker runs itself, so a service does not have to host a consumer just to
//! copy event fields into a table. Each unit reads its topic from the durable
//! CDC journal in order, maps the domain event's fields onto the target
//! entity, and upserts the row through the normal mutation path (tenant/project
//! checks, projections and events follow). These are operator-configured system
//! workers, not requests submitted by an application principal. A failing event is
//! retried on later ticks up to `max_attempts`, then dead-lettered to the CDC
//! DLQ and skipped; a mapping error (a missing field) is dead-lettered at once.
//! One replica runs each unit at a time (a Postgres advisory lock), and the
//! position and retry budget are durable in the system pipeline cursor table.
//!
//! Units are declared in a YAML/JSON file named by `UDB_PIPELINES_FILE` (the
//! `pipelines:` section of `udb.yaml` works as-is).

use super::*;
use std::collections::{BTreeMap, BTreeSet};

/// One declared pipeline unit.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineSpec {
    pub name: String,
    pub source_topic: String,
    pub target_message_type: String,
    /// target field → dotted path in the domain event (`order.id`).
    pub fields: BTreeMap<String, String>,
    #[serde(default)]
    pub conflict_fields: Vec<String>,
    #[serde(default)]
    pub project: String,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
}

fn default_max_attempts() -> u32 {
    5
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum PipelineFile {
    List(Vec<PipelineSpec>),
    Document {
        #[serde(default)]
        pipelines: Vec<PipelineSpec>,
    },
}

/// Parse a pipelines file (YAML or JSON; a list, or an object whose
/// `pipelines` key holds it). Other udb.yaml keys are ignored.
pub fn parse_pipeline_specs(text: &str) -> Result<Vec<PipelineSpec>, String> {
    let value: serde_yaml::Value = serde_yaml::from_str(text)
        .map_err(|err| format!("pipelines file is not YAML/JSON: {err}"))?;
    let (default_project, value) = match value {
        serde_yaml::Value::Mapping(map) => {
            let project = map
                .get(serde_yaml::Value::String("project".into()))
                .and_then(serde_yaml::Value::as_str)
                .unwrap_or_default()
                .to_string();
            let pipelines = map
                .get(serde_yaml::Value::String("pipelines".into()))
                .cloned()
                .unwrap_or(serde_yaml::Value::Sequence(Vec::new()));
            (
                project,
                serde_yaml::Value::Sequence(match pipelines {
                    serde_yaml::Value::Sequence(items) => items,
                    _ => return Err("`pipelines` must be a list".to_string()),
                }),
            )
        }
        other => (String::new(), other),
    };
    let mut specs = match serde_yaml::from_value::<PipelineFile>(value)
        .map_err(|err| format!("invalid pipeline unit: {err}"))?
    {
        PipelineFile::List(specs) | PipelineFile::Document { pipelines: specs } => specs,
    };
    let mut identities = BTreeSet::new();
    for spec in &mut specs {
        if spec.project.trim().is_empty() {
            spec.project = default_project.clone();
        }
    }
    for spec in &specs {
        if spec.name.trim().is_empty()
            || spec.source_topic.trim().is_empty()
            || spec.target_message_type.trim().is_empty()
            || spec.fields.is_empty()
            || spec.max_attempts == 0
        {
            return Err(format!(
                "pipeline '{}' needs name, source_topic, target_message_type and fields",
                spec.name
            ));
        }
        if !identities.insert((&spec.project, &spec.name)) {
            return Err(format!(
                "duplicate pipeline '{}' in project '{}'",
                spec.name, spec.project
            ));
        }
    }
    Ok(specs)
}

/// The domain event inside a journal payload (the outbox envelope nests it
/// under `payload`, and a journal row may wrap the envelope once more).
pub(crate) fn domain_event(payload: &serde_json::Value) -> serde_json::Value {
    let mut envelope = payload.clone();
    if let Some(inner) = envelope.get("payload").cloned()
        && inner.get("event_type").is_some()
        && inner.get("payload").is_some()
    {
        envelope = inner;
    }
    envelope.get("payload").cloned().unwrap_or(envelope)
}

fn path_value<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    path.split('.')
        .try_fold(value, |current, segment| current.get(segment))
        .filter(|found| !found.is_null())
}

/// Map one domain event onto the target record. A missing source path is a
/// permanent error (the event will never map).
pub fn map_pipeline_event(
    spec: &PipelineSpec,
    event: &serde_json::Value,
) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    let mut record = serde_json::Map::new();
    for (target, source) in &spec.fields {
        let value = path_value(event, source.trim())
            .ok_or_else(|| format!("field '{target}': path '{source}' is missing in the event"))?;
        record.insert(target.clone(), value.clone());
    }
    Ok(record)
}

impl DataBrokerRuntime {
    /// Run one tick of a pipeline unit: process up to `batch` events after its
    /// cursor. Returns how many events were consumed (applied or dead-lettered).
    pub async fn run_pipeline_tick(
        &self,
        spec: &PipelineSpec,
        manifest: &CatalogManifest,
        batch: i64,
    ) -> Result<usize, String> {
        let pool = self
            .pg_pool_clone()
            .ok_or("pipelines need the Postgres system store")?;
        let config = crate::runtime::system::SystemCatalogConfig::current();
        let cursors = config.pipeline_cursors_relation();
        let journal = config.cdc_journal_relation();
        let consumer = format!("pipeline:{}", spec.name);
        let mut tx = pool.begin().await.map_err(|err| format!("begin: {err}"))?;
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtext($1))")
            .bind(format!("{}:{consumer}:{}", spec.project, spec.source_topic))
            .fetch_one(&mut *tx)
            .await
            .map_err(|err| format!("pipeline lock: {err}"))?;
        if !locked {
            return Ok(0);
        }
        sqlx::query(&format!(
            "INSERT INTO {cursors} (project_id, pipeline_name, source_topic) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"
        )).bind(&spec.project).bind(&spec.name).bind(&spec.source_topic)
            .execute(&mut *tx).await.map_err(|err| format!("initialize cursor: {err}"))?;
        let (mut after_ts, mut after_id, mut pending, mut retry_count): (chrono::DateTime<chrono::Utc>, String, Option<String>, i32) = sqlx::query_as(&format!(
            "SELECT last_published_at, last_event_id::TEXT, pending_event_id::TEXT, retry_count FROM {cursors} \
             WHERE project_id = $1 AND pipeline_name = $2 AND source_topic = $3"
        )).bind(&spec.project).bind(&spec.name).bind(&spec.source_topic)
            .fetch_one(&mut *tx).await.map_err(|err| format!("read cursor: {err}"))?;
        let rows: Vec<(String, chrono::DateTime<chrono::Utc>, serde_json::Value)> =
            sqlx::query_as(&format!(
                "SELECT event_id::TEXT, published_at, payload FROM {journal} \
             WHERE topic = $1 AND (published_at, event_id::TEXT) > ($2, $3) \
             AND COALESCE(payload->>'project_id', payload->'payload'->>'project_id', '') = $5 \
             ORDER BY published_at ASC, event_id ASC LIMIT $4"
            ))
            .bind(&spec.source_topic)
            .bind(after_ts)
            .bind(&after_id)
            .bind(batch)
            .bind(&spec.project)
            .fetch_all(&mut *tx)
            .await
            .map_err(|err| format!("read journal: {err}"))?;

        let mut consumed = 0usize;
        for (event_id, published_at, payload) in rows {
            let tenant = payload
                .get("tenant_id")
                .or_else(|| payload.get("payload").and_then(|p| p.get("tenant_id")))
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let outcome = match map_pipeline_event(spec, &domain_event(&payload)) {
                Err(err) => Err((err, true)),
                Ok(record) => {
                    let request = crate::proto::UpsertRequest {
                        message_type: spec.target_message_type.clone(),
                        record_json: serde_json::to_vec(&record).unwrap_or_default(),
                        conflict_fields: spec.conflict_fields.clone(),
                        idempotency_key: format!("{}:{consumer}:{event_id}", spec.project),
                        ..Default::default()
                    };
                    let context = crate::RequestContext {
                        tenant_id: tenant.clone(),
                        project_id: spec.project.clone(),
                        purpose: format!("pipeline:{}", spec.name),
                        service_identity: format!("udb.pipeline:{}", spec.name),
                        ..crate::RequestContext::default()
                    };
                    self.upsert(manifest, request, context)
                        .await
                        .map(|_| ())
                        .map_err(|status| (status.message().to_string(), false))
                }
            };
            match outcome {
                Ok(()) => {}
                Err((error, permanent)) => {
                    retry_count = if pending.as_deref() == Some(event_id.as_str()) {
                        retry_count.saturating_add(1)
                    } else {
                        1
                    };
                    pending = Some(event_id.clone());
                    if !permanent && (retry_count as u32) < spec.max_attempts {
                        let tries = retry_count;
                        tracing::warn!(pipeline = %spec.name, event_id = %event_id, attempt = tries, error = %error, "pipeline event failed; retrying on the next tick");
                        break;
                    }
                    sqlx::query(&format!(
                        "INSERT INTO {} (event_id, topic, tenant_id, project_id, error_type, error_message, payload, retry_count, status, next_retry_at, created_at) \
                         VALUES ($1::UUID, $2, $3, $4, $5, $6, $7::JSONB, $8, 'OPEN', NULL, NOW())",
                        config.dlq_relation()
                    ))
                    .bind(&event_id)
                    .bind(&spec.source_topic)
                    .bind(&tenant)
                    .bind(&spec.project)
                    .bind(if permanent { "pipeline_mapping" } else { "pipeline_apply" })
                    .bind(format!("pipeline '{}': {error}", spec.name))
                    .bind(payload.to_string())
                    .bind(retry_count)
                    .execute(&mut *tx)
                    .await
                    .map_err(|err| format!("dead-letter {event_id}: {err}"))?;
                    tracing::error!(pipeline = %spec.name, event_id = %event_id, error = %error, "pipeline event dead-lettered");
                }
            }
            consumed += 1;
            after_ts = published_at;
            after_id = event_id;
            pending = None;
            retry_count = 0;
        }
        sqlx::query(&format!(
            "UPDATE {cursors} SET last_published_at = $4, last_event_id = $5::UUID, pending_event_id = $6::UUID, retry_count = $7, updated_at = NOW() \
             WHERE project_id = $1 AND pipeline_name = $2 AND source_topic = $3"
        )).bind(&spec.project).bind(&spec.name).bind(&spec.source_topic)
            .bind(after_ts).bind(&after_id).bind(pending).bind(retry_count)
            .execute(&mut *tx).await.map_err(|err| format!("advance cursor: {err}"))?;
        tx.commit().await.map_err(|err| format!("commit: {err}"))?;
        Ok(consumed)
    }
}

#[cfg(test)]
mod pipeline_tests {
    use super::{domain_event, map_pipeline_event, parse_pipeline_specs};

    const SPEC: &str = r#"
tenant: t1
project: shop
pipelines:
  - name: order-lines
    source_topic: shop.order.placed.v1
    target_message_type: shop.v1.OrderSummary
    conflict_fields: [order_id]
    fields:
      order_id: order.id
      total: order.total
"#;

    #[test]
    fn parses_units_from_udb_yaml() {
        let specs = parse_pipeline_specs(SPEC).unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].max_attempts, 5);
        assert_eq!(specs[0].project, "shop");
        assert!(parse_pipeline_specs("pipelines: [{name: x}]").is_err());
        assert!(parse_pipeline_specs("tenant: t").unwrap().is_empty());
    }

    #[test]
    fn maps_the_domain_event_from_either_envelope_shape() {
        let spec = &parse_pipeline_specs(SPEC).unwrap()[0];
        let event = serde_json::json!({"order": {"id": "o1", "total": 12.5}});
        let flat = serde_json::json!({"event_type": "x", "payload": event});
        let nested = serde_json::json!({"payload": {"event_type": "x", "payload": event}});
        for envelope in [flat, nested] {
            let record = map_pipeline_event(spec, &domain_event(&envelope)).unwrap();
            assert_eq!(record["order_id"], "o1");
            assert_eq!(record["total"], 12.5);
        }
        let missing = serde_json::json!({"payload": {"order": {"id": "o1"}}});
        assert!(
            map_pipeline_event(spec, &domain_event(&missing))
                .unwrap_err()
                .contains("order.total")
        );
    }
}
