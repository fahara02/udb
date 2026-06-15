//! cdc.rs split — engine_tail (Phase I).
use super::*;
use crate::runtime::system::SystemCatalogConfig;

fn cdc_stream_privileged(scopes: &[String]) -> bool {
    scopes
        .iter()
        .any(|scope| matches!(scope.trim(), "udb:admin" | "udb:cdc:admin" | "udb:*" | "*"))
}

fn topic_pattern_may_match_tenant_scoped(pattern: &str) -> bool {
    let pattern = pattern.trim().to_ascii_lowercase();
    pattern == "*" || pattern.starts_with("udb.") || pattern.starts_with("udb*")
}

fn scope_text(value: Option<String>) -> String {
    value.unwrap_or_default().trim().to_string()
}

fn source_cdc_event_id(label: &str, evt: &super::source::CdcEvent) -> Uuid {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(b"udb.source-cdc-event.v1");
    hasher.update(label.as_bytes());
    hasher.update([0]);
    hasher.update(evt.source.as_bytes());
    hasher.update([0]);
    hasher.update(evt.source_offset.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    // UUIDv8-style deterministic application id with RFC 4122 variant bits.
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn json_string_field<'a>(value: &'a serde_json::Value, field: &str) -> &'a str {
    value
        .get(field)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
}

fn payload_value_matches_stream_scope(
    topic: &str,
    payload: &serde_json::Value,
    tenant_scope: &str,
    project_scope: &str,
    privileged: bool,
    policy_scoped: bool,
) -> bool {
    let event_tenant = json_string_field(payload, "tenant_id").trim();
    if !crate::runtime::cdc::tenant_scoped_topic(topic) && !policy_scoped {
        // #8 fail closed: a non-`udb.` topic prefix is NOT a tenant bypass.
        // An event stamped with a tenant_id must only reach a subscriber
        // whose verified tenant scope matches it (or a privileged unscoped
        // admin stream). Only genuinely tenant-less payloads pass freely.
        if event_tenant.is_empty() {
            return true;
        }
        if privileged && tenant_scope.is_empty() && project_scope.is_empty() {
            return true;
        }
        if tenant_scope.is_empty() || event_tenant != tenant_scope {
            return false;
        }
        if !project_scope.is_empty() {
            let event_project = json_string_field(payload, "project_id").trim();
            if !event_project.is_empty() && event_project != project_scope {
                return false;
            }
        }
        return true;
    }
    if privileged && tenant_scope.is_empty() && project_scope.is_empty() {
        return true;
    }

    if event_tenant.is_empty() {
        return false;
    }
    if !tenant_scope.is_empty() && event_tenant != tenant_scope {
        return false;
    }
    if !privileged && tenant_scope.is_empty() {
        return false;
    }

    if !project_scope.is_empty() {
        let event_project = json_string_field(payload, "project_id").trim();
        if event_project.is_empty() || event_project != project_scope {
            return false;
        }
    }

    true
}

fn payload_string_matches_stream_scope(
    topic: &str,
    payload_json: &str,
    tenant_scope: &str,
    project_scope: &str,
    privileged: bool,
    policy_scoped: bool,
) -> bool {
    match serde_json::from_str::<serde_json::Value>(payload_json) {
        Ok(payload) => payload_value_matches_stream_scope(
            topic,
            &payload,
            tenant_scope,
            project_scope,
            privileged,
            policy_scoped,
        ),
        // Fail closed when the payload cannot be inspected. Non-`udb.`
        // topics are not a tenant-filter bypass: if an event payload is
        // malformed, we cannot prove it belongs to the requesting stream.
        Err(_) => false,
    }
}

/// #1: the error a `tail_source` publish failure aborts the tail with. The
/// abort (instead of falling through to the next stream event) is what keeps
/// the persisted source offset from moving past the failed event — the
/// supervisor restarts the tail from the last persisted offset and the source
/// re-delivers the failed event.
fn tail_source_abort_reason(label: &str, source_offset: &str, err: &str) -> String {
    format!(
        "cdc tail_source[{label}]: kafka publish failed at source offset {source_offset}: {err}; \
         aborting tail so the supervisor restarts from the last persisted offset"
    )
}

/// #1: the per-event control decision `tail_source`'s loop makes from a
/// publish result. `Publish` carries the broker-confirmed coordinates the
/// journal row needs; `Abort` makes the loop return BEFORE the journal
/// insert and the source-offset upsert (both are only reachable through the
/// `Publish` arm), so a failed event can never be skipped by a later
/// offset persist.
#[derive(Debug, PartialEq, Eq)]
enum TailSourceStep {
    /// Broker-confirmed delivery — journal the event, then advance the
    /// persisted source offset.
    Publish { partition: i32, offset: i64 },
    /// Abort the tail with this reason; the supervisor restarts from the
    /// last persisted offset and the source re-delivers the failed event.
    Abort(String),
}

/// #1: map one publish result to the loop's next step. This is the actual
/// decision point `tail_source` executes per event — kept free of `self` so
/// the abort-before-offset-persist contract is unit-testable.
fn tail_source_publish_decision(
    label: &str,
    source_offset: &str,
    publish_result: Result<(i32, i64), String>,
) -> TailSourceStep {
    match publish_result {
        Ok((partition, offset)) => TailSourceStep::Publish { partition, offset },
        Err(err) => TailSourceStep::Abort(tail_source_abort_reason(label, source_offset, &err)),
    }
}

/// #9: which exactly-once modes need the stale-'publishing' sweep. Every
/// mode that tracks `delivery_state` can strand rows in 'publishing' when an
/// in-flight publish is dropped (crash or delivery timeout) — StateMachine
/// exactly like KafkaTransactional. AtLeastOnce rows stay 'pending' until the
/// ack deletes them, so there is nothing to sweep.
fn indoubt_sweep_applies(mode: CdcExactlyOnceMode) -> bool {
    mode != CdcExactlyOnceMode::AtLeastOnce
}

/// #19: replay query anchored at a known journal row. Binds: `$1` = the
/// anchor row's `published_at`, `$2` = the anchor `event_id` as text. The
/// compound comparison keeps events journalled in the same microsecond as
/// the anchor from being silently skipped.
fn replay_sql_from_anchor(journal_relation: &str, limit: i64) -> String {
    format!(
        "SELECT event_id, topic, partition_key, payload, published_at \
         FROM {journal_relation} \
         WHERE (published_at, event_id::TEXT) > ($1, $2) \
         ORDER BY published_at ASC, event_id ASC \
         LIMIT {limit}"
    )
}

/// Bounded per-stream de-dup window for `stream_cdc`'s hybrid delivery: returns
/// `true` if `id` is newly admitted (first time seen), `false` if it was already
/// delivered (via the other source). Evicts the oldest id past `window`. This is
/// request-scoped loop state for one subscription, NOT a shared store.
fn cdc_dedup_admit(
    seen: &mut std::collections::HashSet<String>,
    order: &mut std::collections::VecDeque<String>,
    id: &str,
    window: usize,
) -> bool {
    if !seen.insert(id.to_string()) {
        return false;
    }
    order.push_back(id.to_string());
    if order.len() > window {
        if let Some(old) = order.pop_front() {
            seen.remove(&old);
        }
    }
    true
}

/// Poll the durable `cdc_journal` for rows after `(cursor_ts, cursor_id)`, applying
/// the same topic-pattern + tenant/project scope filters as the live path and the
/// shared de-dup window. Advances the cursor past every scanned row (matched or
/// not) and returns the envelopes to yield. This is the cross-replica, race-free
/// backstop behind the in-process broadcast fast-path.
#[allow(clippy::too_many_arguments)]
async fn cdc_journal_poll(
    pool: &PgPool,
    sql: &str,
    cursor_ts: &mut DateTime<Utc>,
    cursor_id: &mut String,
    matcher: &WildMatch,
    tenant_scope: &str,
    project_scope: &str,
    privileged: bool,
    policy_topics: &[String],
    seen: &mut std::collections::HashSet<String>,
    order: &mut std::collections::VecDeque<String>,
    window: usize,
) -> Vec<CdcEnvelope> {
    let mut out = Vec::new();
    // Bind owned/copied cursor values so the live stream can mutate the cursor.
    let mut rows = sqlx::query(sql)
        .bind(*cursor_ts)
        .bind(cursor_id.clone())
        .fetch(pool);
    while let Some(row) = tokio_stream::StreamExt::next(&mut rows).await {
        let record = match row {
            Ok(record) => record,
            Err(err) => {
                warn!("[cdc] journal tail row decode failed: {err}");
                continue;
            }
        };
        let event_id: Uuid = match record.try_get("event_id") {
            Ok(value) => value,
            Err(_) => continue,
        };
        let published_at: DateTime<Utc> = match record.try_get("published_at") {
            Ok(value) => value,
            Err(_) => continue,
        };
        // Advance the cursor past EVERY scanned row so the next poll never rescans it.
        *cursor_ts = published_at;
        *cursor_id = event_id.to_string();
        let topic: String = match record.try_get("topic") {
            Ok(value) => value,
            Err(_) => continue,
        };
        if !matcher.matches(&topic) {
            continue;
        }
        let payload: serde_json::Value = match record.try_get("payload") {
            Ok(value) => value,
            Err(_) => continue,
        };
        if !payload_value_matches_stream_scope(
            &topic,
            &payload,
            tenant_scope,
            project_scope,
            privileged,
            policy_topics.iter().any(|policy| policy == &topic),
        ) {
            continue;
        }
        let partition_key: String = match record.try_get("partition_key") {
            Ok(value) => value,
            Err(_) => continue,
        };
        let id = event_id.to_string();
        if !cdc_dedup_admit(seen, order, &id, window) {
            continue;
        }
        out.push(CdcEnvelope {
            event_id: id,
            topic,
            partition_key,
            payload_json: payload.to_string(),
            published_at,
        });
    }
    out
}

/// #26: retention sweep over the CDC journal. Bind `$1` = TTL in seconds
/// (the engine passes `idempotency_ttl_secs`, so journal retention and the
/// redis dedup-claim TTL age out together). Deletes only old acked rows:
/// unacked `published`/`dlq` rows are durable operator/replay evidence and
/// survive retention.
fn journal_retention_sweep_sql(journal_relation: &str) -> String {
    format!(
        "DELETE FROM {journal_relation} j \
         WHERE j.published_at < NOW() - make_interval(secs => $1::double precision) \
           AND j.delivery_state = 'acked'"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tenant_scoped_stream_filter_rejects_missing_tenant() {
        assert!(!payload_value_matches_stream_scope(
            "udb.storage.file.created.v1",
            &json!({"event_id": "e1"}),
            "tenant-a",
            "",
            false,
            false,
        ));
    }

    #[test]
    fn tenant_scoped_stream_filter_rejects_other_tenant() {
        assert!(!payload_value_matches_stream_scope(
            "udb.storage.file.created.v1",
            &json!({"tenant_id": "tenant-b", "project_id": "project-a"}),
            "tenant-a",
            "project-a",
            false,
            false,
        ));
    }

    #[test]
    fn tenant_scoped_stream_filter_accepts_matching_tenant_project() {
        assert!(payload_value_matches_stream_scope(
            "udb.storage.file.created.v1",
            &json!({"tenant_id": "tenant-a", "project_id": "project-a"}),
            "tenant-a",
            "project-a",
            false,
            false,
        ));
    }

    #[test]
    fn non_tenant_topic_stream_filter_does_not_require_udb_scope_fields() {
        assert!(payload_value_matches_stream_scope(
            "app.customer.changed",
            &json!({}),
            "",
            "",
            false,
            false,
        ));
    }

    /// #8: a tenant-A event on a non-`udb.` topic must never reach a
    /// tenant-B subscriber — the topic prefix is not a tenant bypass.
    #[test]
    fn non_udb_topic_rejects_cross_tenant_event() {
        assert!(!payload_value_matches_stream_scope(
            "app.customer.changed",
            &json!({"tenant_id": "tenant-a"}),
            "tenant-b",
            "",
            false,
            false,
        ));
        // Matching tenant still passes.
        assert!(payload_value_matches_stream_scope(
            "app.customer.changed",
            &json!({"tenant_id": "tenant-a"}),
            "tenant-a",
            "",
            false,
            false,
        ));
        // An unscoped non-privileged subscriber must not see tenant-stamped
        // events either (fail closed).
        assert!(!payload_value_matches_stream_scope(
            "app.customer.changed",
            &json!({"tenant_id": "tenant-a"}),
            "",
            "",
            false,
            false,
        ));
        // A privileged unscoped admin stream still receives everything.
        assert!(payload_value_matches_stream_scope(
            "app.customer.changed",
            &json!({"tenant_id": "tenant-a"}),
            "",
            "",
            true,
            false,
        ));
    }

    /// #8: a topic owned by an active topic policy is tenant-scoped even
    /// without the `udb.` prefix — missing tenant payloads are rejected.
    #[test]
    fn policy_owned_topic_is_tenant_scoped() {
        assert!(!payload_value_matches_stream_scope(
            "app.invoice.created",
            &json!({"event_id": "e1"}),
            "tenant-a",
            "",
            false,
            true,
        ));
        assert!(payload_value_matches_stream_scope(
            "app.invoice.created",
            &json!({"tenant_id": "tenant-a"}),
            "tenant-a",
            "",
            false,
            true,
        ));
        // Unparseable payloads on policy-owned topics fail closed too.
        assert!(!payload_string_matches_stream_scope(
            "app.invoice.created",
            "not-json",
            "tenant-a",
            "",
            false,
            true,
        ));
    }

    /// #8: malformed payloads on non-`udb.` topics fail closed too. The
    /// stream filter cannot prove tenant ownership, so it must not pass the
    /// event through just because the topic lacks the native `udb.` prefix.
    #[test]
    fn non_udb_topic_rejects_malformed_payload() {
        assert!(!payload_string_matches_stream_scope(
            "app.customer.changed",
            "not-json",
            "tenant-a",
            "",
            false,
            false,
        ));
        assert!(!payload_string_matches_stream_scope(
            "app.customer.changed",
            "not-json",
            "",
            "",
            false,
            false,
        ));
        assert!(!payload_string_matches_stream_scope(
            "app.customer.changed",
            "not-json",
            "",
            "",
            true,
            false,
        ));
    }

    /// #1: a publish failure must abort the tail instead of falling through
    /// to the next event — otherwise a later success would persist a LATER
    /// offset and the failed event would be lost forever. This drives
    /// `tail_source_publish_decision`, the function `tail_source`'s loop
    /// actually executes per event: an `Err` yields `Abort`, and in the real
    /// loop the `Abort` arm `return`s before the journal insert and the
    /// source-offset upsert (both are only reachable through `Publish`), so
    /// no offset persistence can ever follow a failed publish.
    #[test]
    fn tail_source_publish_failure_aborts_before_later_offsets_persist() {
        match tail_source_publish_decision("mysql-main", "offset-2", Err("broker down".to_string()))
        {
            TailSourceStep::Abort(reason) => {
                assert!(reason.contains("mysql-main"), "got: {reason}");
                assert!(reason.contains("offset-2"), "got: {reason}");
                assert!(reason.contains("broker down"), "got: {reason}");
                assert!(reason.contains("last persisted offset"), "got: {reason}");
            }
            TailSourceStep::Publish { partition, offset } => {
                panic!("Err publish result must abort, got Publish({partition}, {offset})")
            }
        }

        // A confirmed publish carries the broker coordinates the journal
        // row (and only then the offset upsert) needs.
        assert_eq!(
            tail_source_publish_decision("mysql-main", "offset-1", Ok((3, 42))),
            TailSourceStep::Publish {
                partition: 3,
                offset: 42
            }
        );
    }

    /// #9: the in-doubt sweep applies to every mode that tracks
    /// `delivery_state` — StateMachine rows can get stuck in 'publishing'
    /// exactly like KafkaTransactional ones. Only AtLeastOnce (rows stay
    /// 'pending' until deleted) has nothing to sweep.
    #[test]
    fn indoubt_sweep_applies_to_all_state_tracked_modes() {
        assert!(indoubt_sweep_applies(CdcExactlyOnceMode::StateMachine));
        assert!(indoubt_sweep_applies(
            CdcExactlyOnceMode::KafkaTransactional
        ));
        assert!(!indoubt_sweep_applies(CdcExactlyOnceMode::AtLeastOnce));
    }

    /// #19: replay reads from the durable cdc_journal (outbox rows are
    /// deleted on ack, so the outbox cannot serve a reconnect gap), and an
    /// absent anchor falls back to a time-cursor over the retained window
    /// instead of returning nothing.
    #[test]
    fn replay_sql_reads_cdc_journal() {
        let anchored = replay_sql_from_anchor("udb_system.udb_cdc_journal", 10_000);
        assert!(anchored.contains("FROM udb_system.udb_cdc_journal"));
        assert!(anchored.contains("(published_at, event_id::TEXT) > ($1, $2)"));
        assert!(anchored.contains("ORDER BY published_at ASC, event_id ASC"));
        assert!(!anchored.contains("outbox"));
    }

    /// #26: the retention sweep removes only acked journal rows older than
    /// the TTL. Unacked `published`/`dlq` rows survive because they are
    /// durable publish/replay evidence. This pins the SQL shape; the actual
    /// behavior is exercised by the env-gated live test
    /// `cdc::live_tests::live_journal_retention_sweep_removes_only_old_acked_rows`,
    /// which runs the real `run_journal_retention_sweep` against Postgres.
    #[test]
    fn journal_retention_sweep_targets_only_acked_rows() {
        let sql = journal_retention_sweep_sql("udb_system.udb_cdc_journal");
        assert!(sql.starts_with("DELETE FROM udb_system.udb_cdc_journal"));
        assert!(sql.contains("delivery_state = 'acked'"));
        assert!(sql.contains("make_interval(secs => $1::double precision)"));
        // Unacked states are never named by the sweep.
        assert!(!sql.contains("'published'"));
        assert!(!sql.contains("'dlq'"));
        assert!(!sql.contains("'publishing'"));
        assert!(!sql.contains("'pending'"));
        assert!(!sql.contains("outbox"));
    }

    /// #73: DLQ rows for rejected source events use a deterministic event_id
    /// derived from (label, source, source_offset), so re-streaming the same
    /// rejected event hits `ON CONFLICT (event_id) DO NOTHING` instead of
    /// inserting a duplicate DLQ row after every supervisor restart.
    #[test]
    fn source_cdc_event_id_is_deterministic_for_dlq_dedup() {
        let evt = super::super::source::CdcEvent::insert(
            "appdb.customers",
            json!({"id": "c-1"}),
            "binlog.000001:4242",
        );
        let first = source_cdc_event_id("mysql-main", &evt);
        let second = source_cdc_event_id("mysql-main", &evt);
        assert_eq!(first, second);

        let other_offset = super::super::source::CdcEvent::insert(
            "appdb.customers",
            json!({"id": "c-1"}),
            "binlog.000001:4243",
        );
        assert_ne!(first, source_cdc_event_id("mysql-main", &other_offset));
        assert_ne!(first, source_cdc_event_id("mysql-replica", &evt));
    }
}

/// Data carried from `prepare_outbox_event` to the produce step (#81).
#[cfg(feature = "kafka")]
struct PreparedOutbox {
    event_id: Uuid,
    topic: String,
    partition_key: String,
    payload_string: String,
    created_at: DateTime<Utc>,
    lsn: i64,
    idempotency_key: String,
}

/// A prepared event whose at-least-once produce is already in flight; awaiting
/// `future` yields the delivery result so the row can be acked or retried (#81).
#[cfg(feature = "kafka")]
struct PendingDelivery {
    prepared: PreparedOutbox,
    future: rdkafka::producer::DeliveryFuture,
}

impl CdcEngine {
    // The CDC tailer publishes to Kafka, so construction requires the `kafka`
    // feature. Rust can't `cfg` a single parameter, so the `redis` idempotency
    // client is taken only when the `redis` feature is on (two variants).
    #[cfg(all(feature = "kafka", feature = "redis"))]
    pub fn new(
        pool: PgPool,
        redis: Option<redis::Client>,
        kafka_brokers: &str,
        dsn: String,
        metrics: std::sync::Arc<dyn MetricsRecorder>,
        config: CdcConfig,
    ) -> Result<Self, String> {
        let kafka_producer = Self::build_kafka_producer(kafka_brokers, &config)?;
        let (broadcast_tx, _) = broadcast::channel(config.broadcast_capacity);
        Ok(Self {
            pool,
            redis,
            kafka_producer,
            broadcast_tx,
            #[cfg(feature = "kafka")]
            dsn,
            metrics,
            config,
            topic_policies: Vec::new(),
        })
    }

    #[cfg(all(feature = "kafka", not(feature = "redis")))]
    pub fn new(
        pool: PgPool,
        kafka_brokers: &str,
        dsn: String,
        metrics: std::sync::Arc<dyn MetricsRecorder>,
        config: CdcConfig,
    ) -> Result<Self, String> {
        let kafka_producer = Self::build_kafka_producer(kafka_brokers, &config)?;
        let (broadcast_tx, _) = broadcast::channel(config.broadcast_capacity);
        Ok(Self {
            pool,
            kafka_producer,
            broadcast_tx,
            #[cfg(feature = "kafka")]
            dsn,
            metrics,
            config,
            topic_policies: Vec::new(),
        })
    }

    #[cfg(feature = "kafka")]
    fn build_kafka_producer(
        kafka_brokers: &str,
        config: &CdcConfig,
    ) -> Result<FutureProducer, String> {
        // U21: KafkaTransactional mode is no longer fail-closed. The
        // `kafka_tx` module builds the producer with `transactional.id`
        // set and calls `init_transactions` so the broker can fence the
        // previous producer epoch and abort any in-doubt transactions
        // from the prior process. The per-event publish path wraps each
        // send in begin/commit_transaction (see kafka_tx::run_in_transaction).
        if config.exactly_once_mode == CdcExactlyOnceMode::KafkaTransactional {
            let tx_cfg = super::kafka_tx::KafkaTxConfig::from_cdc_config(kafka_brokers, config);
            return super::kafka_tx::build_transactional_producer(&tx_cfg);
        }
        ClientConfig::new()
            .set("bootstrap.servers", kafka_brokers)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("compression.type", "lz4")
            .set("message.timeout.ms", "30000")
            .set("retries", "10")
            .set("retry.backoff.ms", "100")
            .set("linger.ms", config.producer_linger_ms.to_string())
            .set(
                "batch.num.messages",
                config.producer_batch_messages.to_string(),
            )
            .create()
            .map_err(|e| e.to_string())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<CdcEnvelope> {
        self.broadcast_tx.subscribe()
    }

    /// U21 step 2: sweep in-doubt `publishing` rows from prior epochs.
    ///
    /// The supervisor MUST call this once after `new()` returns and
    /// before starting the publish loop. The construction path's
    /// `build_kafka_producer` already called `init_transactions`, so by
    /// the time we get here the broker has aborted any in-flight
    /// transactions from the previous epoch. Resetting `publishing`
    /// rows to `pending` lets the publish loop re-run them inside a
    /// fresh transaction.
    ///
    /// No-op when the mode is `AtLeastOnce` (no state-machine tracking).
    /// #9: `StateMachine` mode is swept too — its rows transition through
    /// the same `publishing` state and get stuck the same way when an
    /// in-flight publish is dropped, just without a Kafka transaction to
    /// fence.
    pub async fn run_indoubt_recovery_on_startup(&self) -> Result<u64, String> {
        self.run_indoubt_sweep().await
    }

    /// #9: the in-doubt sweep itself — reset rows stuck in `publishing`
    /// past the grace window (or from a prior producer epoch) back to
    /// `pending`. Called once at startup via
    /// [`run_indoubt_recovery_on_startup`] and periodically from
    /// `run_tailer`'s maintenance tick, so a stranded row recovers within
    /// one sweep interval without a process restart.
    pub(crate) async fn run_indoubt_sweep(&self) -> Result<u64, String> {
        if !indoubt_sweep_applies(self.config.exactly_once_mode) {
            return Ok(0);
        }
        let outbox_relation = self.config.outbox_relation();
        // 5-minute grace by default — well past Kafka's 60s default
        // transaction.timeout.ms, configurable via env.
        let grace_secs = std::env::var("UDB_CDC_INDOUBT_GRACE_SECS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(300);
        let reset = super::indoubt_recovery::reset_indoubt_publishing_rows(
            &self.pool,
            &outbox_relation,
            self.config.producer_epoch,
            grace_secs,
        )
        .await?;
        if reset > 0 {
            info!(
                "[cdc] in-doubt recovery: reset {} 'publishing' rows from prior epoch back to 'pending' \
                 (current producer_epoch={}, grace_secs={})",
                reset, self.config.producer_epoch, grace_secs
            );
        }
        Ok(reset)
    }

    /// FIX-9C: an in-flight publish/ack future for `event_id` timed out and
    /// was dropped, so nothing will ever finalize the row it moved to
    /// `publishing` — without this it stays stuck until the 300s in-doubt
    /// grace sweep. Reset it immediately with the same semantics as
    /// [`run_indoubt_sweep`]: a recorded `kafka_offset` is commit proof
    /// (finalize `acked`); otherwise the publish is unproven and the row
    /// returns to `pending` for the next poll. Scoped to rows still in
    /// `publishing` for THIS producer epoch. Best-effort: on a DB error the
    /// periodic in-doubt sweep remains the backstop.
    #[cfg(feature = "kafka")]
    async fn reset_timed_out_publishing_row(&self, event_id: Uuid) {
        if !indoubt_sweep_applies(self.config.exactly_once_mode) {
            // AtLeastOnce rows never leave 'pending'; nothing to reset.
            return;
        }
        match super::indoubt_recovery::reset_indoubt_publishing_row(
            &self.pool,
            &self.config.outbox_relation(),
            event_id,
            self.config.producer_epoch,
        )
        .await
        {
            Ok(outcome) => {
                info!(
                    "[cdc] delivery-timeout recovery for event {}: {:?}",
                    event_id, outcome
                );
            }
            Err(err) => {
                warn!(
                    "[cdc] delivery-timeout recovery failed for event {} \
                     (periodic in-doubt sweep will retry): {err}",
                    event_id
                );
            }
        }
    }

    /// #26: prune acked CDC journal rows older than the idempotency TTL
    /// (`idempotency_ttl_secs`, default [`DEFAULT_CDC_IDEMPOTENCY_TTL_SECS`])
    /// so journal retention and the redis dedup-claim TTL age out together.
    /// Unacked rows are kept as durable replay/operator evidence. Best-effort:
    /// failures are logged and retried on the next maintenance tick.
    #[cfg(feature = "kafka")]
    pub(crate) async fn run_journal_retention_sweep(&self) {
        let journal_relation = SystemCatalogConfig::default().cdc_journal_relation();
        let sweep_sql = journal_retention_sweep_sql(&journal_relation);
        let ttl_secs = self.config.idempotency_ttl_secs.max(1) as f64;
        match sqlx::query(&sweep_sql)
            .bind(ttl_secs)
            .execute(&self.pool)
            .await
        {
            Ok(res) if res.rows_affected() > 0 => {
                info!(
                    "[cdc] journal retention sweep removed {} acked rows older than {}s",
                    res.rows_affected(),
                    ttl_secs
                );
            }
            Ok(_) => {}
            Err(err) => {
                warn!("[cdc] journal retention sweep failed: {err}");
            }
        }
    }

    pub async fn stream_cdc(
        &self,
        scopes: Vec<String>,
        topic_pattern: String,
        since_event_id: Option<String>,
        tenant_id: Option<String>,
        project_id: Option<String>,
    ) -> Result<
        Pin<
            Box<
                dyn tokio_stream::Stream<Item = Result<CdcEnvelope, tonic::Status>>
                    + Send
                    + 'static,
            >,
        >,
        tonic::Status,
    > {
        let allowed = scopes.iter().any(|scope| {
            scope == "udb:cdc:subscribe"
                || scope == "udb:cdc:read"
                || scope == "udb:*"
                || scope == "*"
        });
        if !allowed {
            return Err(tonic::Status::permission_denied(
                "Missing udb:cdc:read scope",
            ));
        }
        let tenant_scope = scope_text(tenant_id);
        let project_scope = scope_text(project_id);
        let privileged = cdc_stream_privileged(&scopes);
        let matcher = WildMatch::new(&topic_pattern);
        // #8: topics owned by an active topic policy are tenant-scoped even
        // without the `udb.` prefix — consult the policies at subscription
        // time, both for the tenant-required guard and per-event filtering.
        let policy_topics: Vec<String> = self
            .topic_policies
            .iter()
            .filter(|policy| policy.enabled)
            .map(|policy| policy.topic.clone())
            .collect();
        if !privileged
            && tenant_scope.is_empty()
            && (topic_pattern_may_match_tenant_scoped(&topic_pattern)
                || policy_topics.iter().any(|topic| matcher.matches(topic)))
        {
            return Err(tonic::Status::permission_denied(
                "tenant_id is required to stream tenant-scoped CDC topics",
            ));
        }

        use async_stream::try_stream;

        let rx = self.broadcast_tx.subscribe();
        let pool = self.pool.clone();
        // #19: replay reads the durable cdc_journal — outbox rows are
        // deleted on ack, so the outbox cannot serve a reconnect gap.
        let journal_relation = SystemCatalogConfig::default().cdc_journal_relation();

        Ok(Box::pin(try_stream! {
            // Hybrid delivery (bug_report.md §R "kafka is not used"): an in-process
            // broadcast FAST-PATH for low latency, plus a durable cdc_journal
            // BACKSTOP that is cross-replica (shared Postgres) and race-free
            // (subscribe-time cursor), de-duplicated by event_id. The journal row is
            // written only AFTER Kafka acks the produce, so it is the durable record
            // of exactly what reached Kafka — and it backfills anything the bounded
            // broadcast dropped (lag) or never saw (events from another replica).
            const TAIL_BATCH: i64 = 1_000;
            const TAIL_POLL: std::time::Duration = std::time::Duration::from_millis(500);
            const DEDUP_WINDOW: usize = 16_384;
            let tail_sql = replay_sql_from_anchor(&journal_relation, TAIL_BATCH);
            let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
            let mut order: std::collections::VecDeque<String> = std::collections::VecDeque::new();

            // Journal cursor (published_at, event_id). With `since_event_id` we
            // anchor at that event (or the retained-window start if it was pruned)
            // to replay history; a fresh subscription starts at "now" so only future
            // events stream and the subscribe→publish window cannot drop an event.
            let (mut cursor_ts, mut cursor_id): (DateTime<Utc>, String) = match since_event_id
                .as_deref()
                .and_then(|id| Uuid::parse_str(id).ok())
            {
                Some(since_uuid) => {
                    let anchor_sql = format!(
                        "SELECT published_at FROM {journal_relation} WHERE event_id = $1"
                    );
                    match sqlx::query_as::<_, (DateTime<Utc>,)>(&anchor_sql)
                        .bind(since_uuid)
                        .fetch_optional(&pool)
                        .await
                    {
                        Ok(Some((ts,))) => (ts, since_uuid.to_string()),
                        Ok(None) | Err(_) => (
                            DateTime::<Utc>::from_timestamp(0, 0).unwrap_or_else(Utc::now),
                            String::new(),
                        ),
                    }
                }
                // Fresh subscription: anchor at the journal's CURRENT newest row
                // (DB clock + DB ordering), NOT the application `Utc::now()` — an
                // app-vs-DB clock skew could otherwise place the cursor ahead of a
                // just-committed event's `published_at` and skip it. Every event
                // journaled after subscribe is strictly greater and is picked up.
                None => {
                    let max_sql = format!(
                        "SELECT published_at, event_id FROM {journal_relation} \
                         ORDER BY published_at DESC, event_id DESC LIMIT 1"
                    );
                    match sqlx::query_as::<_, (DateTime<Utc>, Uuid)>(&max_sql)
                        .fetch_optional(&pool)
                        .await
                    {
                        Ok(Some((ts, id))) => (ts, id.to_string()),
                        // Empty journal (or read error): start from the epoch so the
                        // first future event is admitted; the broadcast fast-path
                        // covers the interim with no app-clock dependency.
                        Ok(None) | Err(_) => (
                            DateTime::<Utc>::from_timestamp(0, 0).unwrap_or_else(Utc::now),
                            String::new(),
                        ),
                    }
                }
            };

            // 1. Replay: drain the journal from the cursor (bounded per batch).
            loop {
                let batch = cdc_journal_poll(
                    &pool, &tail_sql, &mut cursor_ts, &mut cursor_id, &matcher,
                    &tenant_scope, &project_scope, privileged, &policy_topics,
                    &mut seen, &mut order, DEDUP_WINDOW,
                )
                .await;
                let caught_up = batch.len() < TAIL_BATCH as usize;
                for envelope in batch {
                    yield envelope;
                }
                if caught_up {
                    break;
                }
            }

            // 2. Live: broadcast fast-path + journal backstop, de-duped by event_id.
            let mut rx = Some(rx);
            loop {
                let fast_path = async {
                    match rx.as_mut() {
                        Some(receiver) => receiver.recv().await,
                        // Fast-path closed: park forever so the journal poll is the
                        // sole live source (never busy-loop on a closed channel).
                        None => std::future::pending::<
                            Result<CdcEnvelope, broadcast::error::RecvError>,
                        >()
                        .await,
                    }
                };
                tokio::select! {
                    received = fast_path => match received {
                        Ok(envelope) => {
                            if matcher.matches(&envelope.topic)
                                && payload_string_matches_stream_scope(
                                    &envelope.topic,
                                    &envelope.payload_json,
                                    &tenant_scope,
                                    &project_scope,
                                    privileged,
                                    policy_topics.iter().any(|policy| policy == &envelope.topic),
                                )
                                && cdc_dedup_admit(&mut seen, &mut order, &envelope.event_id, DEDUP_WINDOW)
                            {
                                yield envelope;
                            }
                        }
                        // Dropped on lag — the journal backstop backfills them, so
                        // do NOT break the stream.
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("[cdc] PublishCDC fast-path lagged; {n} events dropped (journal backstop backfills)");
                        }
                        // Fast-path gone — continue with the journal as the sole source.
                        Err(broadcast::error::RecvError::Closed) => {
                            rx = None;
                        }
                    },
                    _ = tokio::time::sleep(TAIL_POLL) => {
                        let batch = cdc_journal_poll(
                            &pool, &tail_sql, &mut cursor_ts, &mut cursor_id, &matcher,
                            &tenant_scope, &project_scope, privileged, &policy_topics,
                            &mut seen, &mut order, DEDUP_WINDOW,
                        )
                        .await;
                        for envelope in batch {
                            yield envelope;
                        }
                    }
                }
            }
        }))
    }

    /// GAP 9: Replace crash-unsafe pg_try_advisory_lock with a row-level upsert lock.
    ///
    /// Advisory locks are silently released when the database connection drops (e.g.
    /// on process crash + immediate restart), allowing two CDC consumers to race.
    /// A row in `udb_cdc_lock_log` has an `acquired_at` timestamp that is heartbeated
    /// every 10 s; any row older than 30 s is considered stale and can be stolen.
    ///
    /// The upsert returns 1 affected row only when the lock was acquired (either
    /// freshly inserted or stolen from a stale holder); 0 rows means the lock is
    /// held by a live peer.
    #[cfg(feature = "kafka")]
    pub async fn run_advisory_lock_loop(&self) {
        let mut check_interval = interval(Duration::from_secs(30));
        let lock_rel = self.config.lock_log_relation();
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string());

        loop {
            check_interval.tick().await;

            match PgConnection::connect(&self.dsn).await {
                Ok(mut conn) => {
                    // GAP 9a: Upsert-based row-level lock.
                    // The WHERE clause steals the row only when the current holder has
                    // not heartbeated in the last 30 s (i.e. it crashed or was killed).
                    let upsert_sql = format!(
                        "INSERT INTO {lock_rel} (lock_key, holder_host, acquired_at)
                         VALUES ($1, $2, NOW())
                         ON CONFLICT (lock_key) DO UPDATE
                             SET holder_host = EXCLUDED.holder_host,
                                 acquired_at  = NOW()
                             WHERE {lock_rel}.acquired_at < NOW() - INTERVAL '30 seconds'"
                    );
                    let result = sqlx::query(&upsert_sql)
                        .bind(self.config.advisory_lock_key)
                        .bind(&hostname)
                        .execute(&mut conn)
                        .await;

                    match result {
                        Ok(r) if r.rows_affected() == 1 => {
                            info!("[cdc] row-level lock acquired — CDC tailer starting");
                            self.metrics.set_cdc_is_leader(&hostname, true);
                            self.run_tailer(conn).await;
                            self.metrics.set_cdc_is_leader(&hostname, false);
                        }
                        Ok(_) => {
                            warn!("[cdc] row-level lock held by live peer — CDC tailer inactive");
                        }
                        Err(e) => {
                            error!("[cdc] failed to acquire row-level lock: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("[cdc] failed to connect for lock loop: {}", e);
                }
            }
        }
    }

    #[cfg(feature = "kafka")]
    pub(crate) async fn run_tailer(&self, mut lock_conn: PgConnection) {
        info!("[cdc] logical replication tailer started");

        let mut heartbeat = interval(Duration::from_secs(10));
        let mut metrics_poll = interval(Duration::from_secs(5));
        let mut tail_loop = Box::pin(self.tail_outbox());
        let pool = self.pool.clone();
        let metrics = self.metrics.clone();
        let lock_rel = self.config.lock_log_relation();
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string());
        // GAP 9c: Exponential backoff for replication tailer restarts.
        let mut backoff_secs: u64 = 1;
        const MAX_BACKOFF_SECS: u64 = 60;
        // #9/#26: maintenance sweeps ride the metrics tick, throttled to
        // every 12th tick (~60s at the 5s metrics interval) — well inside
        // the 300s in-doubt grace window without hammering the DB each tick.
        const MAINTENANCE_EVERY_TICKS: u32 = 12;
        let mut maintenance_tick: u32 = 0;

        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    // GAP 9b: Heartbeat the row-level lock instead of SELECT 1.
                    // If 0 rows are updated, another instance stole the lock → step down.
                    let update_sql = format!(
                        "UPDATE {lock_rel}
                         SET acquired_at = NOW()
                         WHERE lock_key = $1 AND holder_host = $2"
                    );
                    let hb = sqlx::query(&update_sql)
                        .bind(self.config.advisory_lock_key)
                        .bind(&hostname)
                        .execute(&mut lock_conn)
                        .await;
                    match hb {
                        Ok(r) if r.rows_affected() == 0 => {
                            error!("[cdc] row-level lock stolen by another instance; stepping down");
                            break;
                        }
                        Err(e) => {
                            error!("[cdc] lost connection during lock heartbeat: {}", e);
                            break;
                        }
                        _ => {}
                    }

                    // Paused-state check: query udb_cdc_control for this slot
                    {
                        use crate::runtime::system::SystemCatalogConfig;
                        let control_rel = SystemCatalogConfig::default().cdc_control_relation();
                        let paused: Option<(bool,)> = match sqlx::query_as(&format!(
                            "SELECT paused FROM {control_rel} WHERE slot_name = $1 LIMIT 1"
                        ))
                        .bind(&self.config.slot_name)
                        .fetch_optional(&pool)
                        .await
                        {
                            Ok(row) => row,
                            Err(err) => {
                                // Transient DB error — treat as "not paused" (safe-fail)
                                // so the tailer continues rather than silently stopping.
                                warn!(
                                    "[cdc] paused-state check failed (treating as not-paused): {}",
                                    err
                                );
                                None
                            }
                        };

                        if matches!(paused, Some((true,))) {
                            info!(
                                "[cdc] slot '{}' is paused via cdc_control; tailer stepping down",
                                self.config.slot_name
                            );
                            break;
                        }
                    }
                }
                _ = metrics_poll.tick() => {
                    // Poll outbox depth and lag
                    let metrics_sql = format!(
                        "SELECT COUNT(*) as depth, EXTRACT(EPOCH FROM (NOW() - MIN(created_at))) as lag_sec FROM {}",
                        self.config.outbox_relation()
                    );
                    let row: Result<(Option<i64>, Option<f64>), _> =
                        sqlx::query_as(&metrics_sql).fetch_one(&pool).await;

                    if let Ok((depth_opt, lag_opt)) = row {
                        if let Some(depth) = depth_opt {
                            metrics.set_cdc_outbox_depth(depth);
                        }
                        if let Some(lag) = lag_opt {
                            metrics.set_cdc_lag_seconds(lag);
                        } else {
                            metrics.set_cdc_lag_seconds(0.0);
                        }
                    }

                    maintenance_tick += 1;
                    if maintenance_tick >= MAINTENANCE_EVERY_TICKS {
                        maintenance_tick = 0;
                        // #9: recover rows stuck in 'publishing' (dropped
                        // in-flight publish, no ack) without a restart — the
                        // sweep returns them to 'pending' for the next poll.
                        if let Err(err) = self.run_indoubt_sweep().await {
                            warn!("[cdc] periodic in-doubt sweep failed: {err}");
                        }
                        // #26: prune terminal journal rows past the
                        // idempotency TTL so the journal doesn't grow forever.
                        self.run_journal_retention_sweep().await;
                    }
                }
                res = &mut tail_loop => {
                    if let Err(e) = res {
                        // GAP 9c: Exponential backoff — doubles each restart, caps at 60 s.
                        error!(
                            "[cdc] replication tailer error: {}; restarting in {}s",
                            e, backoff_secs
                        );
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                        tail_loop = Box::pin(self.tail_outbox());
                    } else {
                        // Clean exit — reset backoff
                        backoff_secs = 1;
                    }
                }
            }
        }

        // Row-level lock expires naturally via the 30-second staleness window.
        // Explicitly clear acquired_at so the next instance can acquire immediately.
        if let Err(err) = sqlx::query(&format!(
            "UPDATE {} SET acquired_at = '1970-01-01' WHERE lock_key = $1 AND holder_host = $2",
            self.config.lock_log_relation()
        ))
        .bind(self.config.advisory_lock_key)
        .bind(&hostname)
        .execute(&mut lock_conn)
        .await
        {
            warn!("[cdc] failed to clear tailer lock for {hostname}: {err}");
        }
    }

    /// PostgreSQL logical replication tailing.
    ///
    /// The published `tokio-postgres` crate does not expose the copy-both logical
    /// replication APIs required for this path. Until those APIs are available
    /// from crates.io, use the generic `CdcSource` path or an external sidecar to
    /// feed Kafka CDC events.
    /// Reserved for a future WAL logical-replication tailer (needs copy-both
    /// APIs the published `tokio-postgres` does not expose). The production relay
    /// is [`tail_outbox`] (polling); this is kept as the documented WAL seam.
    #[cfg(feature = "kafka")]
    #[allow(dead_code)]
    pub(crate) async fn tail_replication_slot(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Err("Postgres logical replication tailing requires copy-both APIs that are not exposed by the published tokio-postgres crate; use tail_source with a CDC sidecar/source adapter instead".into())
    }

    /// Polling outbox relay (the production tailer).
    ///
    /// Rather than depend on WAL logical replication (which needs a publication +
    /// slot + copy-both APIs the published `tokio-postgres` does not expose), this
    /// polls the transactional outbox directly: oldest-first batches of rows are
    /// handed to [`process_outbox_event`], which publishes to Kafka and — on a
    /// successful publish — acks (deletes) the row in `finish_published_event`, so
    /// the next poll skips it. A failed publish leaves the row in place for retry
    /// (at-least-once). Runs under the advisory leader lock (single poller), so a
    /// plain `ORDER BY event_seq` scan is safe. Returns `Err` only on a DB error,
    /// which the supervising `run_tailer` loop restarts with backoff.
    #[cfg(feature = "kafka")]
    pub(crate) async fn tail_outbox(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let outbox = self.config.outbox_relation();
        let poll_batch = self.config.poll_batch;
        // Only pick up rows that have not yet been published. Without this
        // filter, exactly-once rows that transitioned to publishing/published/
        // acked/dlq (but whose DELETE was lost or which are state-tracked, not
        // deleted) would be re-selected and re-published as duplicates. In
        // at-least-once mode rows stay 'pending' until ack deletes them, so the
        // filter is a no-op there.
        let select_sql = format!(
            "SELECT event_id, topic, partition_key, payload, created_at, event_seq \
             FROM {outbox} WHERE delivery_state = 'pending' \
             ORDER BY event_seq ASC LIMIT {poll_batch}"
        );
        let mut tick = interval(Duration::from_millis(self.config.poll_interval_ms));
        let poll_timeout = Duration::from_secs(self.config.kafka_tx_timeout_secs.max(30));
        // #215: hold one multiplexed redis connection across polls instead of
        // re-acquiring it every `poll_interval_ms`. Reconnect lazily, skipping
        // an exponentially-growing number of polls after each failure so a
        // down redis doesn't trigger a connect attempt (and a warn) every tick.
        let mut redis_conn: Option<redis::aio::MultiplexedConnection> = None;
        let mut redis_reconnect_skip: u32 = 0;
        let mut redis_fail_streak: u32 = 0;
        if self.redis.is_none() {
            warn!("[cdc] redis idempotency guard disabled; relying on Kafka idempotence");
        }
        loop {
            tick.tick().await;
            let rows: Vec<(Uuid, String, String, serde_json::Value, DateTime<Utc>, i64)> =
                match tokio::time::timeout(
                    poll_timeout,
                    sqlx::query_as(&select_sql).fetch_all(&self.pool),
                )
                .await
                {
                    Ok(rows) => rows?,
                    Err(_) => {
                        warn!(
                            "[cdc] outbox poll timed out after {}s",
                            poll_timeout.as_secs()
                        );
                        self.metrics.inc_cdc_errors_total("transient");
                        continue;
                    }
                };
            if let Some(redis) = &self.redis
                && redis_conn.is_none()
            {
                if redis_reconnect_skip > 0 {
                    redis_reconnect_skip -= 1;
                } else {
                    match redis.get_multiplexed_async_connection().await {
                        Ok(conn) => {
                            redis_conn = Some(conn);
                            redis_fail_streak = 0;
                        }
                        Err(e) => {
                            redis_fail_streak = redis_fail_streak.saturating_add(1);
                            redis_reconnect_skip = (1u32 << redis_fail_streak.min(6)).min(64);
                            warn!(
                                "[cdc] failed to connect to redis for idempotency, continuing without guard (retry in {} polls): {}",
                                redis_reconnect_skip, e
                            );
                        }
                    }
                }
            }
            // Phase 1: prepare each row in event_seq order. Exactly-once mode
            // publishes inline (one Kafka transaction per event); at-least-once
            // enqueues the produce (kept in flight by librdkafka) and collects the
            // delivery future, so a batch of produces pipelines instead of waiting
            // one delivery at a time. The idempotent producer preserves
            // per-partition order since `send()` is issued in event_seq order (#81).
            let mut pending = Vec::new();
            for (event_id, topic, partition_key, payload, created_at, event_seq) in rows {
                if let Some(prepared) = match tokio::time::timeout(
                    poll_timeout,
                    self.prepare_outbox_event(
                        event_id,
                        topic,
                        partition_key,
                        payload,
                        created_at,
                        event_seq,
                        redis_conn.as_mut(),
                    ),
                )
                .await
                {
                    Ok(prepared) => prepared,
                    Err(_) => {
                        warn!(
                            "[cdc] outbox event {} preparation timed out after {}s",
                            event_id,
                            poll_timeout.as_secs()
                        );
                        self.metrics.inc_cdc_errors_total("transient");
                        None
                    }
                } {
                    if self.config.exactly_once_mode == CdcExactlyOnceMode::KafkaTransactional {
                        if tokio::time::timeout(
                            poll_timeout,
                            self.produce_and_ack(prepared, redis_conn.as_mut()),
                        )
                        .await
                        .is_err()
                        {
                            warn!(
                                "[cdc] outbox publish/ack timed out after {}s",
                                poll_timeout.as_secs()
                            );
                            self.metrics.inc_cdc_errors_total("transient");
                            // FIX-9C: the dropped future already marked this
                            // row 'publishing' and nothing will finalize it —
                            // reset it now instead of waiting for the 300s
                            // in-doubt grace sweep.
                            self.reset_timed_out_publishing_row(event_id).await;
                        }
                    } else {
                        match self.enqueue_outbox_produce(prepared) {
                            Ok(delivery) => pending.push(delivery),
                            Err((prepared, reason)) => {
                                self.fail_pending(&prepared, &reason, redis_conn.as_mut())
                                    .await;
                            }
                        }
                    }
                }
            }
            // Phase 2: await the in-flight at-least-once deliveries and ack each.
            for delivery in pending {
                let event_id = delivery.prepared.event_id;
                if tokio::time::timeout(
                    poll_timeout,
                    self.await_and_ack_delivery(delivery, redis_conn.as_mut()),
                )
                .await
                .is_err()
                {
                    warn!(
                        "[cdc] outbox delivery ack timed out after {}s",
                        poll_timeout.as_secs()
                    );
                    self.metrics.inc_cdc_errors_total("transient");
                    // FIX-9C: same recovery as the transactional arm — the
                    // dropped delivery future leaves the row 'publishing'
                    // (StateMachine mode) with no finalizer; reset it now.
                    self.reset_timed_out_publishing_row(event_id).await;
                }
            }
        }
    }

    #[cfg(feature = "kafka")]
    pub(crate) async fn mark_cdc_delivery_state(
        &self,
        event_id: Uuid,
        state: &str,
        kafka_partition: Option<i32>,
        kafka_offset: Option<i64>,
        error_message: Option<&str>,
    ) {
        if let Err(err) = self
            .try_mark_cdc_delivery_state(
                event_id,
                state,
                kafka_partition,
                kafka_offset,
                error_message,
            )
            .await
        {
            warn!(
                "[cdc] failed to mark event {} as {} in delivery ledgers: {}",
                event_id, state, err
            );
        }
    }

    async fn try_mark_cdc_delivery_state(
        &self,
        event_id: Uuid,
        state: &str,
        kafka_partition: Option<i32>,
        kafka_offset: Option<i64>,
        error_message: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        if self.config.exactly_once_mode == CdcExactlyOnceMode::AtLeastOnce {
            return Ok(());
        }
        let state_column = match state {
            "publishing" => "publishing_started_at",
            "published" => "published_at",
            "acked" => "acked_at",
            "dlq" => "dlq_at",
            _ => "publishing_started_at",
        };
        let outbox_sql = format!(
            "UPDATE {} SET delivery_state = $2, {state_column} = NOW(), \
             producer_epoch = $3, transactional_id = $4, \
             kafka_partition = COALESCE($5, kafka_partition), \
             kafka_offset = COALESCE($6, kafka_offset), \
             last_error = COALESCE($7, last_error) \
             WHERE event_id = $1",
            self.config.outbox_relation()
        );
        sqlx::query(&outbox_sql)
            .bind(event_id)
            .bind(state)
            .bind(self.config.producer_epoch)
            .bind(self.config.transactional_id())
            .bind(kafka_partition)
            .bind(kafka_offset)
            .bind(error_message)
            .execute(&self.pool)
            .await?;

        if matches!(state, "published" | "acked" | "dlq") {
            use crate::runtime::system::SystemCatalogConfig;
            let sys = SystemCatalogConfig::default();
            let journal_sql = format!(
                "UPDATE {} SET delivery_state = $2, \
                 producer_epoch = $3, transactional_id = $4, \
                 acked_at = CASE WHEN $2 = 'acked' THEN NOW() ELSE acked_at END, \
                 kafka_partition = COALESCE($5, kafka_partition), \
                 kafka_offset = COALESCE($6, kafka_offset), \
                 last_error = COALESCE($7, last_error) \
                 WHERE event_id = $1",
                sys.cdc_journal_relation()
            );
            if let Err(err) = sqlx::query(&journal_sql)
                .bind(event_id)
                .bind(state)
                .bind(self.config.producer_epoch)
                .bind(self.config.transactional_id())
                .bind(kafka_partition)
                .bind(kafka_offset)
                .bind(error_message)
                .execute(&self.pool)
                .await
            {
                self.metrics.inc_cdc_journal_failures_total();
                return Err(err);
            }
        }
        Ok(())
    }

    /// Process a single outbox row end-to-end (idempotency → validate → produce →
    /// ack), synchronously. The auth/event tests drive this directly; the polling
    /// `tail_outbox` loop instead pipelines via `prepare_outbox_event` +
    /// `enqueue_outbox_produce` + `await_and_ack_delivery` so multiple deliveries
    /// are in flight at once (the idempotent producer keeps them per-partition
    /// ordered) — #81.
    #[cfg(feature = "kafka")]
    pub async fn process_outbox_event(
        &self,
        event_id: Uuid,
        topic: String,
        partition_key: String,
        payload_json: serde_json::Value,
        created_at: DateTime<Utc>,
        lsn: i64,
        mut redis_conn: Option<&mut redis::aio::MultiplexedConnection>,
    ) {
        if let Some(prepared) = self
            .prepare_outbox_event(
                event_id,
                topic,
                partition_key,
                payload_json,
                created_at,
                lsn,
                redis_conn.as_deref_mut(),
            )
            .await
        {
            self.produce_and_ack(prepared, redis_conn).await;
        }
    }

    /// Idempotency check, envelope/topic/schema validation, DLQ routing, and the
    /// `'publishing'` state transition. Returns the data needed to publish, or
    /// `None` when the event was a duplicate / invalid / DLQ-routed (all fully
    /// handled here). Pulled out so `tail_outbox` can run this in `event_seq`
    /// order and then pipeline the produces (#81).
    #[cfg(feature = "kafka")]
    async fn prepare_outbox_event(
        &self,
        event_id: Uuid,
        topic: String,
        partition_key: String,
        payload_json: serde_json::Value,
        created_at: DateTime<Utc>,
        lsn: i64,
        mut redis_conn: Option<&mut redis::aio::MultiplexedConnection>,
    ) -> Option<PreparedOutbox> {
        // 1. Idempotency Check
        let idempotency_key = format!("{}:{}", self.config.idempotency_key_prefix, event_id);

        if let Some(conn) = redis_conn.as_deref_mut() {
            let set_nx: Result<bool, redis::RedisError> = redis::cmd("SET")
                .arg(&idempotency_key)
                .arg("1")
                .arg("NX")
                .arg("EX")
                .arg(self.config.idempotency_ttl_secs)
                .query_async(conn)
                .await;

            if let Ok(false) = set_nx {
                // #114: the redis key is only a *claim* written BEFORE the Kafka
                // publish — it is NOT proof the event was durably published. A
                // crash between this `SET NX` and the publish leaves the key set
                // with the row still unpublished; acking on the key alone would
                // silently delete an unpublished outbox row (data loss). Redis is
                // a fast-path dedup hint, never the ack/delete authority: consult
                // the durable publish evidence (the CDC journal) first.
                if self.was_durably_published(event_id).await {
                    info!(
                        "[cdc] skipping duplicate event {} (durably published)",
                        event_id
                    );
                    self.metrics.inc_cdc_duplicate_skipped_total();
                    if self.ack_event(event_id, lsn).await {
                        self.mark_cdc_delivery_state(event_id, "acked", None, None, None)
                            .await;
                    } else {
                        warn!(
                            "[cdc] duplicate event {} was not acked; retaining idempotency key for retry",
                            event_id
                        );
                    }
                } else {
                    // Stale claim from a crashed pre-publish attempt: there is no
                    // durable evidence the event was published. Drop the key so the
                    // next poll re-selects and re-publishes this still-pending row
                    // (at-least-once; downstream consumers dedupe on event_id). Do
                    // NOT ack — the event was never published.
                    warn!(
                        "[cdc] idempotency key for {} present but no durable publish evidence; \
                         dropping stale claim and re-publishing on next poll",
                        event_id
                    );
                    if let Some(conn) = redis_conn.as_deref_mut() {
                        let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                    }
                }
                return None;
            }
        }

        // 2. Validate Event
        let envelope: Result<EventEnvelope, _> = serde_json::from_value(payload_json.clone());
        match envelope {
            Ok(mut env) => {
                if env.event_id != event_id.to_string() {
                    let error_message = format!(
                        "payload event_id {} does not match outbox event_id {}",
                        env.event_id, event_id
                    );
                    error!(
                        "[cdc] validation failed for event {}: {}",
                        event_id, error_message
                    );
                    self.metrics.inc_cdc_errors_total("validation");
                    self.metrics.inc_cdc_errors_total("dlq_routed");
                    if self
                        .route_to_dlq(
                            event_id,
                            payload_json,
                            "EnvelopeEventIdMismatch",
                            &error_message,
                        )
                        .await
                    {
                        self.ack_event(event_id, lsn).await;
                    } else if let Some(conn) = redis_conn.as_deref_mut() {
                        let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                    }
                    return None;
                }
                if env.schema_uri.is_none() {
                    env.schema_uri = self.config.schema_uri_for(&env.event_type);
                }
                if let Err(error_message) =
                    crate::runtime::cdc::validate_topic_tenant_scope(&topic, &env)
                {
                    error!(
                        "[cdc] tenant-scope validation rejected event {}: {}",
                        event_id, error_message
                    );
                    self.metrics.inc_cdc_errors_total("tenant_scope_missing");
                    self.metrics.inc_cdc_errors_total("dlq_routed");
                    if self
                        .route_to_dlq(event_id, payload_json, "TenantScopeMissing", &error_message)
                        .await
                    {
                        self.ack_event(event_id, lsn).await;
                    } else if let Some(conn) = redis_conn.as_deref_mut() {
                        let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                    }
                    return None;
                }

                // 2a. Phase 7: Topic policy enforcement — reject topics not in the allowlist.
                if !self.topic_policies.is_empty() {
                    // Resolve the policy once. Missing → reject (allowlist).
                    // Matched → apply its policy-specific behavior: the policy's
                    // declared `schema_uri` is authoritative over the event's own,
                    // so schema validation below enforces the policy contract (#131).
                    let policy_schema = match self.topic_policy_for(&topic) {
                        Some(policy) => {
                            if policy.tenant_id.trim() != "*"
                                && policy.tenant_id.trim() != env.tenant_id.trim()
                            {
                                let error_message = format!(
                                    "topic '{}' policy requires tenant '{}' but event has '{}'",
                                    topic,
                                    policy.tenant_id.trim(),
                                    env.tenant_id.trim()
                                );
                                error!(
                                    "[cdc] topic policy rejected event {}: {}",
                                    event_id, error_message
                                );
                                self.metrics.inc_cdc_errors_total("topic_policy_rejected");
                                self.metrics.inc_cdc_errors_total("dlq_routed");
                                if self
                                    .route_to_dlq(
                                        event_id,
                                        payload_json,
                                        "TopicPolicyTenantRejected",
                                        &error_message,
                                    )
                                    .await
                                {
                                    self.ack_event(event_id, lsn).await;
                                } else if let Some(conn) = redis_conn.as_deref_mut() {
                                    let _: () =
                                        conn.del(&idempotency_key).await.unwrap_or_default();
                                }
                                return None;
                            }
                            if !policy.owning_project.trim().is_empty()
                                && policy.owning_project.trim() != env.project_id.trim()
                            {
                                let error_message = format!(
                                    "topic '{}' policy requires project '{}' but event has '{}'",
                                    topic,
                                    policy.owning_project.trim(),
                                    env.project_id.trim()
                                );
                                error!(
                                    "[cdc] topic policy rejected event {}: {}",
                                    event_id, error_message
                                );
                                self.metrics.inc_cdc_errors_total("topic_policy_rejected");
                                self.metrics.inc_cdc_errors_total("dlq_routed");
                                if self
                                    .route_to_dlq(
                                        event_id,
                                        payload_json,
                                        "TopicPolicyProjectRejected",
                                        &error_message,
                                    )
                                    .await
                                {
                                    self.ack_event(event_id, lsn).await;
                                } else if let Some(conn) = redis_conn.as_deref_mut() {
                                    let _: () =
                                        conn.del(&idempotency_key).await.unwrap_or_default();
                                }
                                return None;
                            }
                            policy.schema_uri.trim().to_string()
                        }
                        None => {
                            let error_message = format!(
                                "topic '{}' is not in the active topic policy allowlist",
                                topic
                            );
                            error!(
                                "[cdc] topic policy rejected event {}: {}",
                                event_id, error_message
                            );
                            self.metrics.inc_cdc_errors_total("topic_policy_rejected");
                            self.metrics.inc_cdc_errors_total("dlq_routed");
                            if self
                                .route_to_dlq(
                                    event_id,
                                    payload_json,
                                    "TopicPolicyRejected",
                                    &error_message,
                                )
                                .await
                            {
                                self.ack_event(event_id, lsn).await;
                            } else if let Some(conn) = redis_conn.as_deref_mut() {
                                let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                            }
                            return None;
                        }
                    };
                    if !policy_schema.is_empty() {
                        env.schema_uri = Some(policy_schema);
                    }
                }

                // 2b. Phase 7: Schema registry validation (soft-fail — network errors allowed).
                let schema_uri_ref = env.schema_uri.as_deref().unwrap_or("");
                if let Err(reason) = self.validate_event_schema(schema_uri_ref).await {
                    let error_message =
                        format!("schema registry rejected '{}': {}", schema_uri_ref, reason);
                    error!(
                        "[cdc] schema validation rejected event {}: {}",
                        event_id, error_message
                    );
                    self.metrics
                        .inc_cdc_errors_total("schema_registry_rejected");
                    self.metrics.inc_cdc_errors_total("dlq_routed");
                    if self
                        .route_to_dlq(
                            event_id,
                            payload_json,
                            "SchemaRegistryRejected",
                            &error_message,
                        )
                        .await
                    {
                        self.ack_event(event_id, lsn).await;
                    } else if let Some(conn) = redis_conn.as_deref_mut() {
                        let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                    }
                    return None;
                }

                // 3. Prepared — hand the data back so the caller publishes it
                // (synchronously via `produce_and_ack`, or pipelined by
                // `tail_outbox`). (#81)
                let payload_string = serde_json::to_string(&env).unwrap_or_default();
                self.mark_cdc_delivery_state(event_id, "publishing", None, None, None)
                    .await;
                Some(PreparedOutbox {
                    event_id,
                    topic,
                    partition_key,
                    payload_string,
                    created_at,
                    lsn,
                    idempotency_key,
                })
            }
            Err(e) => {
                error!("[cdc] validation failed for event {}: {}", event_id, e);
                self.metrics.inc_cdc_errors_total("validation");
                self.metrics.inc_cdc_errors_total("dlq_routed");
                // DLQ Routing
                if self
                    .route_to_dlq(
                        event_id,
                        payload_json,
                        "SchemaValidationError",
                        &e.to_string(),
                    )
                    .await
                {
                    self.ack_event(event_id, lsn).await;
                } else if let Some(conn) = redis_conn.as_deref_mut() {
                    let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                }
                None
            }
        }
    }

    /// Publish a prepared event and ack it — the synchronous single-event path
    /// (exactly-once Kafka transaction, or at-least-once produce + await). The
    /// pipelined `tail_outbox` loop instead uses `enqueue_outbox_produce` +
    /// `await_and_ack_delivery` so a batch of produces overlaps in flight (#81).
    #[cfg(feature = "kafka")]
    async fn produce_and_ack(
        &self,
        prepared: PreparedOutbox,
        mut redis_conn: Option<&mut redis::aio::MultiplexedConnection>,
    ) {
        if self.config.exactly_once_mode == CdcExactlyOnceMode::KafkaTransactional {
            let timeout = Duration::from_secs(self.config.kafka_tx_timeout_secs.max(1));
            match super::kafka_tx::run_in_transaction(
                &self.kafka_producer,
                timeout,
                &prepared.topic,
                &prepared.partition_key,
                &prepared.payload_string,
            )
            .await
            {
                Ok(super::kafka_tx::KafkaTxPublishOutcome::Committed { partition, offset }) => {
                    self.finish_published_event(
                        prepared.event_id,
                        &prepared.topic,
                        &prepared.partition_key,
                        &prepared.payload_string,
                        prepared.created_at,
                        prepared.lsn,
                        partition,
                        offset,
                    )
                    .await;
                }
                Ok(super::kafka_tx::KafkaTxPublishOutcome::Aborted { reason }) => {
                    error!("[cdc] transactional kafka publish aborted: {}", reason);
                    self.metrics.inc_cdc_errors_total("transient");
                    self.mark_cdc_delivery_state(
                        prepared.event_id,
                        "pending",
                        None,
                        None,
                        Some(&reason),
                    )
                    .await;
                    if let Some(conn) = redis_conn.as_deref_mut() {
                        let _: () = conn
                            .del(&prepared.idempotency_key)
                            .await
                            .unwrap_or_default();
                    }
                }
                Err(e) => {
                    error!("[cdc] transactional kafka publish failed: {:?}", e);
                    self.metrics.inc_cdc_errors_total("transient");
                    self.mark_cdc_delivery_state(
                        prepared.event_id,
                        "pending",
                        None,
                        None,
                        Some(&e.to_string()),
                    )
                    .await;
                    if let Some(conn) = redis_conn.as_deref_mut() {
                        let _: () = conn
                            .del(&prepared.idempotency_key)
                            .await
                            .unwrap_or_default();
                    }
                }
            }
        } else {
            match self.enqueue_outbox_produce(prepared) {
                Ok(pending) => self.await_and_ack_delivery(pending, redis_conn).await,
                Err((prepared, reason)) => self.fail_pending(&prepared, &reason, redis_conn).await,
            }
        }
    }

    /// Enqueue a prepared event to the at-least-once Kafka producer WITHOUT
    /// awaiting delivery. `send_result` copies the payload into librdkafka and
    /// returns a `'static` `DeliveryFuture` that no longer borrows `prepared`, so
    /// issuing many in `event_seq` order keeps them in flight and pipelines the
    /// batch (the idempotent producer preserves per-partition order). On a
    /// full-queue error the produce never happened — `prepared` is handed back so
    /// the caller can leave the row `pending` for the next poll (#81).
    #[cfg(feature = "kafka")]
    fn enqueue_outbox_produce(
        &self,
        prepared: PreparedOutbox,
    ) -> Result<PendingDelivery, (PreparedOutbox, String)> {
        // Phase 10: continue the distributed trace by attaching the current
        // `traceparent` as a Kafka header. Additive — absent any active trace the
        // header is omitted and the record is byte-for-byte as before.
        let traceparent = super::current_egress_traceparent();
        let _span = tracing::info_span!(
            "cdc.publish",
            topic = %prepared.topic,
            traceparent = traceparent.as_deref().unwrap_or("")
        )
        .entered();
        let mut record = FutureRecord::to(&prepared.topic)
            .key(&prepared.partition_key)
            .payload(&prepared.payload_string);
        if let Some(tp) = traceparent.as_deref() {
            record = record.headers(rdkafka::message::OwnedHeaders::new().insert(
                rdkafka::message::Header {
                    key: "traceparent",
                    value: Some(tp),
                },
            ));
        }
        match self.kafka_producer.send_result(record) {
            Ok(future) => Ok(PendingDelivery { prepared, future }),
            Err((e, _)) => Err((prepared, e.to_string())),
        }
    }

    /// Await a pipelined delivery and finalize it: ack (delete + advance offset)
    /// on success, or return the row to `pending` so the next poll retries it.
    /// `DeliveryFuture` resolves to `Result<delivery, Canceled>`; a cancel is
    /// treated as a transient failure (#81).
    #[cfg(feature = "kafka")]
    async fn await_and_ack_delivery(
        &self,
        pending: PendingDelivery,
        redis_conn: Option<&mut redis::aio::MultiplexedConnection>,
    ) {
        let PendingDelivery { prepared, future } = pending;
        match future.await {
            Ok(Ok((partition, offset))) => {
                self.finish_published_event(
                    prepared.event_id,
                    &prepared.topic,
                    &prepared.partition_key,
                    &prepared.payload_string,
                    prepared.created_at,
                    prepared.lsn,
                    partition,
                    offset,
                )
                .await;
            }
            Ok(Err((e, _))) => {
                self.fail_pending(&prepared, &e.to_string(), redis_conn)
                    .await
            }
            Err(_canceled) => {
                self.fail_pending(&prepared, "kafka delivery future canceled", redis_conn)
                    .await
            }
        }
    }

    /// Return a prepared event to `pending` (so the next poll retries it) and drop
    /// its redis idempotency key. Shared by the at-least-once enqueue-failure and
    /// delivery-failure paths (#81).
    #[cfg(feature = "kafka")]
    async fn fail_pending(
        &self,
        prepared: &PreparedOutbox,
        reason: &str,
        mut redis_conn: Option<&mut redis::aio::MultiplexedConnection>,
    ) {
        error!("[cdc] failed to publish to kafka: {}", reason);
        self.metrics.inc_cdc_errors_total("transient");
        self.mark_cdc_delivery_state(prepared.event_id, "pending", None, None, Some(reason))
            .await;
        if let Some(conn) = redis_conn.as_deref_mut() {
            let _: () = conn
                .del(&prepared.idempotency_key)
                .await
                .unwrap_or_default();
        }
    }

    /// Durable evidence that an event was actually published to Kafka, used to
    /// gate ack/delete on redis duplicate-detection (#114). The CDC journal row
    /// is written by [`finish_published_event`] only AFTER the broker
    /// acknowledged the produce, so a `published`/`acked` journal state is proof
    /// the event left the outbox. On any query error this returns `false`
    /// (fail-safe: prefer re-publishing over silently dropping the event).
    #[cfg(feature = "kafka")]
    async fn was_durably_published(&self, event_id: Uuid) -> bool {
        use crate::runtime::system::SystemCatalogConfig;
        let journal = SystemCatalogConfig::default().cdc_journal_relation();
        let state: Result<Option<(String,)>, _> = sqlx::query_as(&format!(
            "SELECT delivery_state FROM {journal} WHERE event_id = $1"
        ))
        .bind(event_id)
        .fetch_optional(&self.pool)
        .await;
        matches!(state, Ok(Some((s,))) if s == "published" || s == "acked")
    }

    #[cfg(feature = "kafka")]
    async fn finish_published_event(
        &self,
        event_id: Uuid,
        topic: &str,
        partition_key: &str,
        payload_string: &str,
        created_at: DateTime<Utc>,
        lsn: i64,
        partition: i32,
        offset: i64,
    ) {
        info!(
            "[cdc] published {} to partition {} offset {}",
            event_id, partition, offset
        );

        // Live-subscription fan-out is best-effort, but a send error means zero
        // receivers / full buffer — surface it (the subscriber logs Lagged; the
        // publisher was silent). Durability is unaffected (Kafka already has it).
        if self
            .broadcast_tx
            .send(CdcEnvelope {
                event_id: event_id.to_string(),
                topic: topic.to_string(),
                partition_key: partition_key.to_string(),
                payload_json: payload_string.to_string(),
                published_at: Utc::now(),
            })
            .is_err()
        {
            tracing::debug!(
                event_id = %event_id,
                topic = %topic,
                "[cdc] live broadcast dropped (no active subscribers / buffer full)"
            );
        }

        let publish_duration = (Utc::now() - created_at).num_milliseconds() as f64 / 1000.0;
        self.metrics
            .observe_cdc_publish_duration_seconds(publish_duration);
        self.metrics.inc_cdc_events_published_total(topic);

        {
            use crate::runtime::system::SystemCatalogConfig;
            let sys = SystemCatalogConfig::default();
            let journal = sys.cdc_journal_relation();
            if let Err(err) = sqlx::query(&format!(
                "INSERT INTO {journal} \
                 (event_id, topic, partition_key, payload, published_at, kafka_partition, kafka_offset, delivery_state, producer_epoch, transactional_id) \
                 VALUES ($1, $2, $3, $4::JSONB, NOW(), $5, $6, 'published', $7, $8) \
                 ON CONFLICT (event_id) DO UPDATE SET \
                   delivery_state = 'published', \
                   kafka_partition = EXCLUDED.kafka_partition, \
                   kafka_offset = EXCLUDED.kafka_offset, \
                   producer_epoch = EXCLUDED.producer_epoch, \
                   transactional_id = EXCLUDED.transactional_id"
            ))
            .bind(event_id)
            .bind(topic)
            .bind(partition_key)
            .bind(payload_string)
            .bind(partition)
            .bind(offset)
            .bind(self.config.producer_epoch)
            .bind(self.config.transactional_id())
            .execute(&self.pool)
            .await
            {
                self.metrics.inc_cdc_journal_failures_total();
                error!("[cdc] journal insert failed for event {}: {}", event_id, err);
                return;
            }
        }

        if let Err(err) = self
            .try_mark_cdc_delivery_state(event_id, "published", Some(partition), Some(offset), None)
            .await
        {
            error!(
                "[cdc] publish state update failed for event {} after broker ack: {}",
                event_id, err
            );
            return;
        }

        if let Err(err) = self
            .try_mark_cdc_delivery_state(event_id, "acked", Some(partition), Some(offset), None)
            .await
        {
            error!(
                "[cdc] ack state update failed for event {} after broker ack: {}",
                event_id, err
            );
            return;
        }
        self.ack_event(event_id, lsn).await;
    }

    // ── D (2026-05-30): generic CdcSource tail ────────────────────────────
    //
    // The PG-specific `tail_replication_slot` keeps the WAL/pgoutput
    // machinery (replication mode connection, RELATION cache,
    // KeepAlive replies). For Mongo / MySQL / future sources, we
    // consume the abstract `CdcSource` trait — `source.open()`
    // returns a stream of `CdcEvent`s, the engine publishes each to
    // Kafka, and the resume offset is persisted in the existing
    // `udb_cdc_offsets` table keyed by the source's stable label.
    //
    // Resume: `source.open(last_offset)` lets each source's adapter
    // restart exactly where it left off (Mongo resume token; MySQL
    // binlog file:position). When `last_offset` is empty the source
    // chooses its own "tail from now" default.
    //
    // This does NOT replace `tail_replication_slot` — PG-specific
    // logical replication has its own dedicated path for backwards
    // compatibility + the schema-registry projection wiring. The
    // generic path is what Mongo / MySQL CDC use.

    /// D: generic CDC source tail. Consumes events from the given
    /// `CdcSource`, publishes them to Kafka using the configured
    /// topic mapping, and persists `source_offset` after each
    /// successful publish so a restart resumes from the last
    /// successfully-acked event.
    ///
    /// Returns `Ok(())` on graceful stream end (`open()` future
    /// completes); errors from the underlying source surface as
    /// `Err`. The caller is expected to run this in a supervisor
    /// that retries on transient failure.
    #[cfg(feature = "kafka")]
    pub async fn tail_source(
        &self,
        source: std::sync::Arc<dyn super::source::CdcSource>,
    ) -> Result<(), String> {
        use futures::StreamExt;

        let label = source.backend_label().to_string();
        // Use the source's backend label as the offset key. Multiple
        // sources from the same backend (e.g. two Mongo databases)
        // should expose distinct labels via the constructor.
        let slot_key = format!("cdc_source:{label}");
        let offsets_relation = self.config.offsets_relation();

        // 1. Load last persisted offset (empty if first run).
        let offset_sql = format!("SELECT last_offset FROM {offsets_relation} WHERE slot_name = $1");
        let from_offset: String = sqlx::query_scalar(&offset_sql)
            .bind(&slot_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| format!("cdc tail_source: offset load failed: {e}"))?
            .unwrap_or_default();

        // 2. Open the stream.
        let mut stream = source.open(&from_offset).await?;

        info!(
            "[cdc] generic source tail started: label={} from_offset={}",
            label,
            if from_offset.is_empty() {
                "<begin>"
            } else {
                &from_offset
            }
        );

        // 3. Drain. Publish each event to Kafka with the source's
        //    label as the topic; advance the persisted offset on
        //    successful ack. Kafka publish errors are fatal to this
        //    tail invocation: aborting prevents a later event from
        //    persisting a later source offset past the failed event.
        while let Some(evt_res) = stream.next().await {
            let mut evt = match evt_res {
                Ok(e) => e,
                Err(err) => {
                    error!("[cdc] tail_source stream error from {label}: {err}");
                    self.metrics.inc_cdc_errors_total("source_stream_error");
                    return Err(err);
                }
            };
            if let Err(err) = evt.validate_identity() {
                error!("[cdc] tail_source rejected invalid event from {label}: {err}");
                self.metrics.inc_cdc_errors_total("source_identity_missing");
                // CDC pipeline parity (final_task.md §9): route the malformed
                // source event to the DLQ — the same as the Postgres outbox path —
                // and CONTINUE tailing. A single invalid event must not tear down
                // the whole source stream (previously this returned Err and the
                // supervisor restarted the tail, replaying from the last offset).
                self.metrics.inc_cdc_errors_total("dlq_routed");
                let payload = serde_json::to_value(&evt).unwrap_or(serde_json::Value::Null);
                let source_event_id = source_cdc_event_id(&label, &evt);
                self.route_to_dlq(
                    source_event_id,
                    payload,
                    "SourceIdentityMissing",
                    &err.to_string(),
                )
                .await;
                continue;
            }
            let topic = format!("udb.cdc.{}.{}", label, evt.source);
            // CDC pipeline parity (final_task.md §9): gate the source topic on
            // topic policy before publish — the same check the Postgres outbox
            // path applies. A denied topic is DLQ-routed, never published.
            if !self.config.topic_allowed(&topic) {
                warn!("[cdc] tail_source topic '{topic}' denied by policy; routing to DLQ");
                self.metrics
                    .inc_cdc_errors_total("source_topic_policy_denied");
                self.metrics.inc_cdc_errors_total("dlq_routed");
                let payload = serde_json::to_value(&evt).unwrap_or(serde_json::Value::Null);
                let source_event_id = source_cdc_event_id(&label, &evt);
                self.route_to_dlq(
                    source_event_id,
                    payload,
                    "TopicPolicyDenied",
                    &format!("topic {topic} not permitted by policy"),
                )
                .await;
                continue;
            }
            let partition_key = evt
                .after
                .as_ref()
                .or(evt.before.as_ref())
                .and_then(|v| v.get("id").or_else(|| v.get("_id")))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{}:{}", evt.tenant_id, evt.source_offset));
            // CDC pipeline parity (final_task.md §9): redact operator-declared
            // sensitive fields from external-source row data before publish. The
            // outbox path derives these from the UDB manifest; external sources
            // have none, so the field list is config-driven
            // (`UDB_CDC_SOURCE_SENSITIVE_FIELDS`). No-op when unset.
            if !self.config.source_sensitive_fields.is_empty() {
                let fields = &self.config.source_sensitive_fields;
                let mode = self.config.redaction_mode;
                if let Some(after) = evt.after.take() {
                    evt.after = Some(super::redact_cdc_payload_fields(after, fields, mode));
                }
                if let Some(before) = evt.before.take() {
                    evt.before = Some(super::redact_cdc_payload_fields(before, fields, mode));
                }
            }
            let payload_string = serde_json::to_string(&evt).unwrap_or_else(|_| "{}".to_string());
            let source_event_id = source_cdc_event_id(&label, &evt);

            let publish_result = if self.config.exactly_once_mode
                == CdcExactlyOnceMode::KafkaTransactional
            {
                let timeout = Duration::from_secs(self.config.kafka_tx_timeout_secs.max(1));
                match super::kafka_tx::run_in_transaction(
                    &self.kafka_producer,
                    timeout,
                    &topic,
                    &partition_key,
                    &payload_string,
                )
                .await
                {
                    Ok(super::kafka_tx::KafkaTxPublishOutcome::Committed { partition, offset }) => {
                        Ok((partition, offset))
                    }
                    Ok(super::kafka_tx::KafkaTxPublishOutcome::Aborted { reason }) => {
                        Err(format!("transaction aborted: {reason}"))
                    }
                    Err(err) => Err(format!("transactional publish failed: {err:?}")),
                }
            } else {
                // Phase 10: propagate the current trace onto the direct-tail publish.
                let traceparent = super::current_egress_traceparent();
                let mut record = FutureRecord::to(&topic)
                    .key(&partition_key)
                    .payload(&payload_string);
                if let Some(tp) = traceparent.as_deref() {
                    record = record.headers(rdkafka::message::OwnedHeaders::new().insert(
                        rdkafka::message::Header {
                            key: "traceparent",
                            value: Some(tp),
                        },
                    ));
                }
                self.kafka_producer
                    .send(record, Duration::from_secs(30))
                    .await
                    .map_err(|(err, _)| format!("{err:?}"))
            };

            // #1: the per-event decision — an `Abort` returns BEFORE the
            // journal insert / offset upsert below, so the persisted source
            // offset can never move past a failed event. Continuing instead
            // could publish a later event and persist its later
            // source_offset, which would skip this failed event forever on
            // restart.
            let (partition, kafka_offset) =
                match tail_source_publish_decision(&label, &evt.source_offset, publish_result) {
                    TailSourceStep::Publish { partition, offset } => (partition, offset),
                    TailSourceStep::Abort(reason) => {
                        error!("[cdc] tail_source kafka publish failed: {reason}");
                        self.metrics.inc_cdc_errors_total("transient");
                        return Err(reason);
                    }
                };
            info!(
                "[cdc] tail_source published {} → topic={} partition={} offset={}",
                label, topic, partition, kafka_offset
            );
            self.metrics.inc_cdc_wal_messages_received_total();
            // Same delivery-proof shape as the outbox path: Kafka ack
            // first, then durable CDC journal, then source offset advance.
            // If the journal write fails, do NOT advance the offset; the
            // source may replay and downstream consumers can dedupe by the
            // deterministic source_event_id.
            let sys = SystemCatalogConfig::default();
            let journal = sys.cdc_journal_relation();
            let journal_sql = format!(
                "INSERT INTO {journal} \
                 (event_id, topic, partition_key, payload, published_at, kafka_partition, kafka_offset, delivery_state, producer_epoch, transactional_id) \
                 VALUES ($1, $2, $3, $4::JSONB, NOW(), $5, $6, 'published', $7, $8) \
                 ON CONFLICT (event_id) DO UPDATE SET \
                   delivery_state = 'published', \
                   published_at = NOW(), \
                   kafka_partition = EXCLUDED.kafka_partition, \
                   kafka_offset = EXCLUDED.kafka_offset, \
                   producer_epoch = EXCLUDED.producer_epoch, \
                   transactional_id = EXCLUDED.transactional_id"
            );
            if let Err(err) = sqlx::query(&journal_sql)
                .bind(source_event_id)
                .bind(&topic)
                .bind(&partition_key)
                .bind(&payload_string)
                .bind(partition)
                .bind(kafka_offset)
                .bind(self.config.producer_epoch)
                .bind(self.config.transactional_id())
                .execute(&self.pool)
                .await
            {
                warn!(
                    "[cdc] tail_source journal persist failed for {} at offset {}: {err}; \
                     event published but source offset will not advance",
                    label, evt.source_offset
                );
                self.metrics.inc_cdc_journal_failures_total();
                continue;
            }

            // Persist the source offset so restart resumes only after
            // delivery has durable evidence in `cdc_journal`.
            let upsert_sql = format!(
                "INSERT INTO {offsets_relation} (slot_name, last_offset, updated_at) \
                 VALUES ($1, $2, NOW()) \
                 ON CONFLICT (slot_name) DO UPDATE \
                   SET last_offset = EXCLUDED.last_offset, updated_at = NOW()"
            );
            if let Err(err) = sqlx::query(&upsert_sql)
                .bind(&slot_key)
                .bind(&evt.source_offset)
                .execute(&self.pool)
                .await
            {
                warn!(
                    "[cdc] tail_source offset persist failed for {}: {err}; \
                     event published but resume may replay",
                    slot_key
                );
            }
        }

        info!("[cdc] generic source tail ended: label={label}");
        Ok(())
    }
}
