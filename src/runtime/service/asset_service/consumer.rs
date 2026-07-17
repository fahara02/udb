//! Kafka consumers for the native `AssetService`: the per-node storage→asset
//! auto-trigger on `udb.storage.file.finalized.v1`, and the leader-elected,
//! reconciling trigger-topic manager (master-plan 5.2) that runs exactly one
//! consumer per distinct active `trigger_topic` cluster-wide. Extracted verbatim
//! from the former god file; every `#[cfg(...)]` / `#[allow(dead_code)]` gate and
//! the commit-after-success offset posture are byte-for-byte identical. Spawned by
//! `serve()` via the `pub(crate)` `spawn_*` methods on `AssetServiceImpl`.

#[cfg(feature = "kafka")]
use tonic::Status;

#[cfg(feature = "kafka")]
use super::AssetServiceImpl;
#[cfg(feature = "kafka")]
use super::errors::asset_internal_status;
#[cfg(feature = "kafka")]
use super::model::pipeline_definition_model;

/// Topic the storage service emits on finalize; the auto-trigger consumes it.
#[cfg(feature = "kafka")]
pub(crate) const STORAGE_FINALIZED_TOPIC: &str = "udb.storage.file.finalized.v1";

/// Backoff after a consumer recv error so an unreachable broker (no Kafka in the
/// deployment) can't spin this loop. Matches the topic-not-visible sleep below.
#[cfg(feature = "kafka")]
const STORAGE_FINALIZED_RECV_ERROR_BACKOFF_SECS: u64 = 2;

/// Cooldown for the recv-error WARN log, so a persistently unreachable broker
/// collapses to one line per window (with a suppressed count) instead of a flood.
#[cfg(feature = "kafka")]
const STORAGE_FINALIZED_RECV_ERROR_LOG_COOLDOWN_SECS: u64 = 30;

#[cfg(feature = "kafka")]
pub(crate) fn storage_finalized_consumer_config(brokers: &str) -> rdkafka::ClientConfig {
    let mut config = rdkafka::ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", "udb-asset-storage-finalized-trigger")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest");
    config
}

#[cfg(feature = "kafka")]
async fn ensure_storage_finalized_topic(brokers: &str) -> Result<(), String> {
    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .map_err(|err| format!("create Kafka admin client failed: {err}"))?;
    match admin
        .create_topics(
            &[NewTopic::new(
                STORAGE_FINALIZED_TOPIC,
                1,
                TopicReplication::Fixed(1),
            )],
            &AdminOptions::new(),
        )
        .await
    {
        Ok(results) => {
            for result in results {
                if let Err((name, code)) = result
                    && !format!("{code:?}").contains("TopicAlreadyExists")
                {
                    return Err(format!("create Kafka topic {name} failed: {code:?}"));
                }
            }
        }
        Err(err) => return Err(format!("create Kafka topic request failed: {err}")),
    }
    admin
        .inner()
        .fetch_metadata(
            Some(STORAGE_FINALIZED_TOPIC),
            std::time::Duration::from_secs(10),
        )
        .map_err(|err| {
            format!("Kafka topic {STORAGE_FINALIZED_TOPIC} metadata was not visible: {err}")
        })?;
    Ok(())
}

#[cfg(feature = "kafka")]
pub(crate) fn storage_finalized_payload_ids(bytes: &[u8]) -> Option<(String, String)> {
    let env = serde_json::from_slice::<serde_json::Value>(bytes).ok()?;
    // Canonical envelope: tenant_id at top level; file_id in the payload
    // (document_id is the partition key = file_id too).
    let tenant_id = env.get("tenant_id").and_then(|v| v.as_str())?.trim();
    let file_id = env
        .get("payload")
        .and_then(|p| p.get("file_id"))
        .and_then(|v| v.as_str())
        .or_else(|| env.get("document_id").and_then(|v| v.as_str()))?
        .trim();
    if tenant_id.is_empty() || file_id.is_empty() {
        return None;
    }
    Some((tenant_id.to_string(), file_id.to_string()))
}

#[cfg(feature = "kafka")]
pub(crate) fn storage_finalized_commit_offsets(
    topic: &str,
    partition: i32,
    message_offset: i64,
) -> rdkafka::error::KafkaResult<rdkafka::TopicPartitionList> {
    let mut offsets = rdkafka::TopicPartitionList::new();
    offsets.add_partition_offset(
        topic,
        partition,
        rdkafka::Offset::Offset(message_offset.saturating_add(1)),
    )?;
    Ok(offsets)
}

#[cfg(feature = "kafka")]
pub(crate) fn should_commit_storage_finalized_offset(
    result: &Result<Option<String>, Status>,
) -> bool {
    result.is_ok()
}

#[cfg(feature = "kafka")]
fn is_storage_finalized_topic_missing_error(err: &rdkafka::error::KafkaError) -> bool {
    let text = err.to_string();
    text.contains("UnknownTopicOrPartition")
        || text.contains("Broker: Unknown topic or partition")
        || text.contains("unknown topic or partition")
}

#[cfg(feature = "kafka")]
impl AssetServiceImpl {
    /// Spawn the storage→asset auto-trigger: a background Kafka consumer on
    /// `udb.storage.file.finalized.v1` that, per finalized file, registers the
    /// asset and starts the matching pipeline via [`handle_storage_finalized`]
    /// (idempotent). Offsets are committed only after successful handling, so
    /// backlog is replayed at-least-once across restarts. Best-effort — a
    /// consumer error logs; the broker keeps running.
    ///
    /// Lifecycle (P6.4 decision): this runs **per node**, intentionally — every
    /// replica joins the **shared** Kafka consumer group
    /// `udb-asset-storage-finalized-trigger`, so the group coordinator
    /// distributes partitions across replicas and each message is delivered to
    /// exactly one consumer. This is NOT the leader-elected `NativeWorkerHost`
    /// pattern and must NOT be converted to it: a singleton lease would collapse
    /// every partition onto one node and forfeit horizontal consume throughput.
    /// At-least-once redelivery on rebalance is made safe by
    /// [`handle_storage_finalized`]'s idempotency, not by single-ownership.
    pub(crate) fn spawn_storage_finalized_consumer(self: std::sync::Arc<Self>, brokers: String) {
        tokio::spawn(async move {
            use rdkafka::Message;
            use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
            if let Err(err) = ensure_storage_finalized_topic(&brokers).await {
                tracing::warn!(
                    error = %err,
                    topic = STORAGE_FINALIZED_TOPIC,
                    "asset storage-finalized consumer: topic preflight failed; consumer will retry metadata"
                );
            }
            let config = storage_finalized_consumer_config(&brokers);
            let consumer: StreamConsumer = match config.create() {
                Ok(c) => c,
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        "asset storage-finalized consumer: create failed"
                    );
                    return;
                }
            };
            if let Err(err) = consumer.subscribe(&[STORAGE_FINALIZED_TOPIC]) {
                tracing::error!(error = %err, "asset storage-finalized consumer: subscribe failed");
                return;
            }
            tracing::info!(
                topic = STORAGE_FINALIZED_TOPIC,
                "storage→asset auto-trigger consumer started"
            );
            // Collapse the recv-error WARN so an unreachable broker can't flood
            // the log; the per-error backoff below prevents a tight spin.
            let recv_error_gate = crate::runtime::executor_utils::LogRateGate::new(
                std::time::Duration::from_secs(STORAGE_FINALIZED_RECV_ERROR_LOG_COOLDOWN_SECS),
            );
            loop {
                match consumer.recv().await {
                    Ok(msg) => {
                        let Some(bytes) = msg.payload() else {
                            tracing::warn!(
                                "asset storage-finalized consumer: message missing payload"
                            );
                            continue;
                        };
                        let Some((tenant_id, file_id)) = storage_finalized_payload_ids(bytes)
                        else {
                            tracing::warn!(
                                topic = msg.topic(),
                                partition = msg.partition(),
                                offset = msg.offset(),
                                "asset storage-finalized consumer: invalid envelope"
                            );
                            continue;
                        };
                        let topic = msg.topic().to_string();
                        let partition = msg.partition();
                        let offset = msg.offset();
                        let result = self.handle_storage_finalized(&file_id, &tenant_id).await;
                        if should_commit_storage_finalized_offset(&result) {
                            match storage_finalized_commit_offsets(&topic, partition, offset) {
                                Ok(offsets) => {
                                    if let Err(err) = consumer.commit(&offsets, CommitMode::Async) {
                                        tracing::warn!(
                                            error = %err,
                                            file_id = %file_id,
                                            topic = %topic,
                                            partition,
                                            offset,
                                            "asset storage-finalized consumer commit failed"
                                        );
                                    }
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        error = %err,
                                        file_id = %file_id,
                                        topic = %topic,
                                        partition,
                                        offset,
                                        "asset storage-finalized consumer commit offset build failed"
                                    );
                                }
                            }
                        }
                        if let Err(err) = result {
                            tracing::warn!(
                                error = %err,
                                file_id = %file_id,
                                "storage→asset trigger failed"
                            );
                        }
                    }
                    Err(err) => {
                        if is_storage_finalized_topic_missing_error(&err) {
                            tracing::debug!(
                                error = %err,
                                topic = STORAGE_FINALIZED_TOPIC,
                                "asset storage-finalized consumer: topic not visible yet"
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                            continue;
                        }
                        if let Some(suppressed) = recv_error_gate.check() {
                            if suppressed > 0 {
                                tracing::warn!(
                                    error = %err,
                                    suppressed,
                                    "asset storage-finalized consumer recv error ({suppressed} more \
                                     suppressed since last log; is Kafka reachable?)"
                                );
                            } else {
                                tracing::warn!(error = %err, "asset storage-finalized consumer recv error");
                            }
                        }
                        // Back off so a persistently unreachable broker can't spin
                        // this loop at full speed.
                        tokio::time::sleep(std::time::Duration::from_secs(
                            STORAGE_FINALIZED_RECV_ERROR_BACKOFF_SECS,
                        ))
                        .await;
                    }
                }
            }
        });
    }
}

// ── Kafka-triggered pipelines (master-plan 5.2) ──────────────────────────────
//
// The static storage-finalized consumer above is hardcoded to one topic and runs
// per-node. Operator-defined `trigger_topic`s are dynamic, so a leader-elected
// manager reconciles the distinct active trigger topics and runs exactly one
// consumer per topic cluster-wide (start on add, stop on remove). The per-topic
// consumer GENERALIZES `spawn_storage_finalized_consumer`: same
// `enable.auto.commit=false` + commit-after-success gate, keyed by topic and
// routed through `handle_trigger_event` instead of the hardcoded
// `STORAGE_FINALIZED_TOPIC` / `handle_storage_finalized`.

/// Start/stop delta for the trigger-topic consumer set.
#[cfg(any(feature = "kafka", test))]
#[allow(dead_code)] // alive via tests + the (serve-wired) manager loop
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct TriggerReconcile {
    pub(crate) to_start: Vec<String>,
    pub(crate) to_stop: Vec<String>,
}

/// Pure reconcile of the trigger-topic consumer set: given the topics that
/// currently have a running consumer and the desired set (distinct active
/// `trigger_topic`s), return which topics to START (desired ∖ running) and which
/// to STOP (running ∖ desired). Topic-keyed, so the manager owns at most one
/// consumer per topic. Side-effect free + deterministic (BTreeSet ordering) so it
/// is unit-tested without Kafka.
#[cfg(any(feature = "kafka", test))]
#[allow(dead_code)] // alive via tests + the (serve-wired) manager loop
pub(crate) fn reconcile_trigger_topics(
    running: &std::collections::BTreeSet<String>,
    desired: &std::collections::BTreeSet<String>,
) -> TriggerReconcile {
    TriggerReconcile {
        to_start: desired.difference(running).cloned().collect(),
        to_stop: running.difference(desired).cloned().collect(),
    }
}

/// Shared consumer-group prefix for trigger-topic consumers; the topic is appended
/// so each topic gets its own group (offsets tracked per topic). Dot-delimited per
/// the Kafka topic-naming policy.
#[cfg(feature = "kafka")]
#[allow(dead_code)] // alive once serve() wires spawn_trigger_manager (master-plan 5.2)
fn trigger_consumer_group_id(topic: &str) -> String {
    format!("udb-asset-trigger.{topic}")
}

/// Per-topic trigger consumer config. Mirrors `storage_finalized_consumer_config`:
/// `enable.auto.commit=false` (offsets committed only after the handler succeeds)
/// and `auto.offset.reset=earliest` (replay backlog), keyed by `topic`.
#[cfg(feature = "kafka")]
#[allow(dead_code)] // alive once serve() wires spawn_trigger_manager (master-plan 5.2)
fn trigger_consumer_config(brokers: &str, topic: &str) -> rdkafka::ClientConfig {
    let mut config = rdkafka::ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", trigger_consumer_group_id(topic))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest");
    config
}

/// Fail-closed existence check for a `trigger_topic`: returns `true` only when the
/// topic already exists in the cluster. The broker NEVER auto-creates a trigger
/// topic (unlike `ensure_storage_finalized_topic`). Full-cluster metadata is
/// fetched (`topic = None`) precisely so a per-topic metadata probe can't trigger
/// broker-side `auto.create.topics.enable`; a topic we don't see is treated as
/// absent (fail closed).
#[cfg(feature = "kafka")]
#[allow(dead_code)] // alive once serve() wires spawn_trigger_manager (master-plan 5.2)
async fn trigger_topic_exists(brokers: &str, topic: &str) -> Result<bool, String> {
    use rdkafka::ClientConfig;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", trigger_consumer_group_id(topic))
        .create()
        .map_err(|err| format!("create Kafka client for topic check failed: {err}"))?;
    let metadata = consumer
        .fetch_metadata(None, std::time::Duration::from_secs(10))
        .map_err(|err| format!("fetch Kafka metadata failed: {err}"))?;
    Ok(metadata
        .topics()
        .iter()
        .any(|t| t.name() == topic && !t.partitions().is_empty()))
}

/// Owns the per-topic consumer tasks the manager has spawned. Aborting on `Drop`
/// is load-bearing: when the singleton lease is lost the manager-loop future is
/// dropped, and a tokio `JoinHandle` detaches (does NOT abort) on its own drop —
/// so without this guard the consumers would outlive the lease and a new leader
/// would double-spawn them. The guard guarantees consumers stop whenever leadership
/// ends, preserving "exactly one consumer per topic cluster-wide".
#[cfg(feature = "kafka")]
#[allow(dead_code)] // alive once serve() wires spawn_trigger_manager (master-plan 5.2)
struct TriggerConsumers {
    handles: std::collections::BTreeMap<String, tokio::task::JoinHandle<()>>,
}

#[cfg(feature = "kafka")]
impl Drop for TriggerConsumers {
    fn drop(&mut self) {
        for handle in self.handles.values() {
            handle.abort();
        }
    }
}

#[cfg(feature = "kafka")]
// Methods are alive once serve() wires `spawn_trigger_manager` (master-plan 5.2);
// the allow keeps the default build warning-clean until that one-line wiring lands.
#[allow(dead_code)]
impl AssetServiceImpl {
    /// Spawn the leader-elected, reconciling Kafka-trigger manager (master-plan
    /// 5.2). Under the singleton lease [`crate::runtime::singleton::WORKER_ASSET_TRIGGER_MANAGER`]
    /// it periodically reads the distinct active `trigger_topic`s and runs exactly
    /// one consumer per topic cluster-wide, starting consumers for newly-referenced
    /// topics and stopping those no longer referenced. Non-leaders idle. Fails
    /// CLOSED on a missing topic (never auto-creates).
    ///
    /// NOTE for `serve()` — wire this exactly like the other leader-elected workers,
    /// next to the `spawn_storage_finalized_consumer` block:
    ///
    /// ```ignore
    /// #[cfg(feature = "kafka")]
    /// if crate::runtime::cdc::cdc_delivery_enabled() {
    ///     if let Some(brokers) = runtime_config.kafka_brokers.clone() {
    ///         let trigger_runtime = service.runtime.load_full();
    ///         if let Ok(trigger_pool) =
    ///             trigger_runtime.native_store_pool_for_service("asset", true, "")
    ///         {
    ///             let singleton_relation = trigger_runtime.config().cdc.lock_log_relation();
    ///             std::sync::Arc::new(service.build_asset_service())
    ///                 .spawn_trigger_manager(brokers, trigger_pool, singleton_relation);
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// The `run_while_leader(WORKER_ASSET_TRIGGER_MANAGER, ...)` call lives inside
    /// this fn (below), so `serve()` only needs the spawn above.
    pub(crate) fn spawn_trigger_manager(
        self: std::sync::Arc<Self>,
        brokers: String,
        singleton_pool: sqlx::PgPool,
        singleton_relation: String,
    ) {
        tokio::spawn(async move {
            loop {
                let manager = self.clone();
                let brokers = brokers.clone();
                // Leader-elected: only the lease holder runs the manager loop, so
                // the per-topic consumers exist once cluster-wide (no per-node dup).
                match crate::runtime::singleton::run_while_leader(
                    &singleton_pool,
                    &singleton_relation,
                    crate::runtime::singleton::WORKER_ASSET_TRIGGER_MANAGER,
                    crate::runtime::singleton::WORKER_SINGLETON_LEASE_TTL,
                    || async move { manager.run_trigger_manager_loop(brokers).await },
                )
                .await
                {
                    Ok(Some(())) => {}
                    Ok(None) => {
                        tracing::debug!("asset trigger manager idle: singleton lease held by peer")
                    }
                    Err(err) => {
                        // Lease lost (or acquisition failed): the loop future was
                        // dropped, so its TriggerConsumers guard already aborted every
                        // consumer. Back off, then re-contend for leadership.
                        tracing::warn!("asset trigger manager lease ended: {err}")
                    }
                }
                tokio::time::sleep(crate::runtime::singleton::WORKER_SINGLETON_RETRY_SLEEP).await;
            }
        });
    }

    /// The manager loop, run only while this node holds the lease. Reconciles the
    /// running consumer set against the desired trigger-topic set every interval.
    async fn run_trigger_manager_loop(self: std::sync::Arc<Self>, brokers: String) {
        let reconcile_interval = std::time::Duration::from_secs(
            std::env::var("UDB_ASSET_TRIGGER_RECONCILE_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(30),
        );
        // Drop-guard: aborts all consumers if this future is cancelled (lease lost).
        let mut consumers = TriggerConsumers {
            handles: std::collections::BTreeMap::new(),
        };
        let mut ticker = tokio::time::interval(reconcile_interval);
        loop {
            ticker.tick().await;
            // Forget consumers whose task already exited so they can be restarted.
            consumers
                .handles
                .retain(|_topic, handle| !handle.is_finished());
            let desired = match self.distinct_trigger_topics().await {
                Ok(set) => set,
                Err(err) => {
                    tracing::warn!(error = %err, "asset trigger manager: load trigger topics failed");
                    continue;
                }
            };
            let running: std::collections::BTreeSet<String> =
                consumers.handles.keys().cloned().collect();
            let delta = reconcile_trigger_topics(&running, &desired);
            for topic in delta.to_stop {
                if let Some(handle) = consumers.handles.remove(&topic) {
                    handle.abort();
                    tracing::info!(topic = %topic, "asset trigger consumer stopped (definition removed)");
                }
            }
            for topic in delta.to_start {
                // Fail CLOSED: never auto-create a trigger topic. Skip until it
                // exists; the next reconcile retries once an operator creates it.
                match trigger_topic_exists(&brokers, &topic).await {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::warn!(
                            topic = %topic,
                            "asset trigger consumer not started: trigger_topic does not exist (no auto-create); will retry"
                        );
                        continue;
                    }
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            topic = %topic,
                            "asset trigger consumer not started: topic existence check failed; will retry"
                        );
                        continue;
                    }
                }
                let handle = tokio::spawn(
                    self.clone()
                        .run_trigger_consumer(brokers.clone(), topic.clone()),
                );
                consumers.handles.insert(topic.clone(), handle);
                tracing::info!(topic = %topic, "asset trigger consumer started (definition added)");
            }
        }
    }

    /// Distinct active `trigger_topic`s across all pipeline definitions (the
    /// desired consumer set). Runs cluster-wide on the asset native pool, mirroring
    /// the scheduler/webhook leader workers' cross-tenant reads.
    async fn distinct_trigger_topics(&self) -> Result<std::collections::BTreeSet<String>, Status> {
        let pool = self.require_pool()?;
        let dm = pipeline_definition_model();
        let rows: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT DISTINCT {tt} FROM {rel} \
             WHERE {status} = 'ACTIVE' AND {tt} IS NOT NULL AND {tt} <> ''",
            tt = dm.q("trigger_topic"),
            rel = dm.relation,
            status = dm.q("status"),
        ))
        .fetch_all(pool)
        .await
        .map_err(|e| {
            asset_internal_status(
                "load_trigger_topics",
                format!("load trigger topics failed: {e}"),
            )
        })?;
        Ok(rows
            .into_iter()
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect())
    }

    /// Run one trigger-topic consumer until the task is aborted (the manager owns
    /// the abort handle). GENERALIZES [`Self::spawn_storage_finalized_consumer`]:
    /// identical `enable.auto.commit=false` + commit-after-success posture and the
    /// same [`storage_finalized_payload_ids`] envelope, but keyed by `topic` and
    /// routed through [`Self::handle_trigger_event`].
    async fn run_trigger_consumer(self: std::sync::Arc<Self>, brokers: String, topic: String) {
        use rdkafka::Message;
        use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
        let config = trigger_consumer_config(&brokers, &topic);
        let consumer: StreamConsumer = match config.create() {
            Ok(c) => c,
            Err(err) => {
                tracing::error!(error = %err, topic = %topic, "asset trigger consumer: create failed");
                return;
            }
        };
        if let Err(err) = consumer.subscribe(&[topic.as_str()]) {
            tracing::error!(error = %err, topic = %topic, "asset trigger consumer: subscribe failed");
            return;
        }
        tracing::info!(topic = %topic, "asset trigger pipeline consumer started");
        let recv_error_gate = crate::runtime::executor_utils::LogRateGate::new(
            std::time::Duration::from_secs(STORAGE_FINALIZED_RECV_ERROR_LOG_COOLDOWN_SECS),
        );
        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    let Some(bytes) = msg.payload() else {
                        tracing::warn!(topic = %topic, "asset trigger consumer: message missing payload");
                        continue;
                    };
                    let Some((tenant_id, file_id)) = storage_finalized_payload_ids(bytes) else {
                        tracing::warn!(
                            topic = msg.topic(),
                            partition = msg.partition(),
                            offset = msg.offset(),
                            "asset trigger consumer: invalid envelope"
                        );
                        continue;
                    };
                    let msg_topic = msg.topic().to_string();
                    let partition = msg.partition();
                    let offset = msg.offset();
                    let result = self
                        .handle_trigger_event(&topic, &file_id, &tenant_id)
                        .await;
                    // Same should_commit gate as the storage-finalized consumer:
                    // commit only after the handler succeeds (at-least-once replay).
                    if should_commit_storage_finalized_offset(&result) {
                        match storage_finalized_commit_offsets(&msg_topic, partition, offset) {
                            Ok(offsets) => {
                                if let Err(err) = consumer.commit(&offsets, CommitMode::Async) {
                                    tracing::warn!(
                                        error = %err,
                                        topic = %msg_topic,
                                        partition,
                                        offset,
                                        "asset trigger consumer commit failed"
                                    );
                                }
                            }
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    topic = %msg_topic,
                                    partition,
                                    offset,
                                    "asset trigger consumer commit offset build failed"
                                );
                            }
                        }
                    }
                    if let Err(err) = result {
                        tracing::warn!(
                            error = %err,
                            file_id = %file_id,
                            topic = %topic,
                            "asset trigger pipeline failed"
                        );
                    }
                }
                Err(err) => {
                    if is_storage_finalized_topic_missing_error(&err) {
                        tracing::debug!(error = %err, topic = %topic, "asset trigger consumer: topic not visible yet");
                        tokio::time::sleep(std::time::Duration::from_secs(
                            STORAGE_FINALIZED_RECV_ERROR_BACKOFF_SECS,
                        ))
                        .await;
                        continue;
                    }
                    if let Some(suppressed) = recv_error_gate.check() {
                        if suppressed > 0 {
                            tracing::warn!(
                                error = %err,
                                suppressed,
                                topic = %topic,
                                "asset trigger consumer recv error ({suppressed} more suppressed since last log; is Kafka reachable?)"
                            );
                        } else {
                            tracing::warn!(error = %err, topic = %topic, "asset trigger consumer recv error");
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(
                        STORAGE_FINALIZED_RECV_ERROR_BACKOFF_SECS,
                    ))
                    .await;
                }
            }
        }
    }
}
