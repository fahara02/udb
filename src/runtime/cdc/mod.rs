use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
#[cfg(feature = "kafka")]
use rdkafka::ClientConfig;
#[cfg(feature = "kafka")]
use rdkafka::producer::{FutureProducer, FutureRecord};
#[cfg(feature = "redis")]
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
#[cfg(feature = "kafka")]
use sqlx::{Connection, PgConnection};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
#[cfg(feature = "kafka")]
use std::time::Duration;
use tokio::sync::broadcast;

use crate::runtime::executor_utils::{env_identifier, qi_runtime as qi};
#[cfg(feature = "kafka")]
use tokio::time::interval;
#[cfg(feature = "kafka")]
use tracing::error;
use tracing::{info, warn};
use uuid::Uuid;
use wildmatch::WildMatch;

use crate::generation::CatalogManifest;
use crate::metrics::MetricsRecorder;

/// THE one canonical outbox-row insert, shared by EVERY writer of the
/// `(event_id, topic, partition_key, payload, created_at)` outbox table: the auth
/// lane (`auth_service::events::write_outbox_row`), the native data-plane lane
/// (`service::native_helpers`), the generic `EnqueueOutboxEvent` path
/// (`core::probe_dispatch`), and DLQ replay (`core::catalog_admin`). Centralizing the
/// INSERT here means the SQL and — critically — the bind TYPES can never diverge across
/// writers. A prior `Uuid`-vs-`String` drift on `$1` between two lanes caused a sqlx
/// prepared-statement collision ("incorrect binary data format in bind parameter 1")
/// that poisoned the pooled connection and cascaded into bogus "invalid byte sequence
/// for encoding UTF8" failures on the next clean write (§4). Taking `event_id: Uuid`
/// makes that drift unrepresentable at the type level.
pub(crate) async fn insert_outbox_row<'c, E>(
    executor: E,
    relation: &str,
    event_id: Uuid,
    topic: &str,
    partition_key: &str,
    envelope: &serde_json::Value,
) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let topic = strip_nul_text(topic);
    let partition_key = strip_nul_text(partition_key);
    let envelope = strip_nul_json(envelope);
    let sql = format!(
        "INSERT INTO {relation} (event_id, topic, partition_key, payload, created_at) \
         VALUES ($1::UUID, $2, $3, $4::JSONB, NOW())"
    );
    sqlx::query(&sql)
        .bind(event_id)
        .bind(topic)
        .bind(partition_key)
        .bind(envelope.to_string())
        .execute(executor)
        .await
        .map(|_| ())
}

fn strip_nul_text(value: &str) -> String {
    if value.contains('\u{0}') {
        value.replace('\u{0}', "")
    } else {
        value.to_string()
    }
}

fn strip_nul_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(s) if s.contains('\u{0}') => {
            serde_json::Value::String(strip_nul_text(s))
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(strip_nul_json).collect())
        }
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(key, value)| (strip_nul_text(key), strip_nul_json(value)))
                .collect(),
        ),
        other => other.clone(),
    }
}

pub mod source; // C2 + C3: per-backend CDC source trait + Postgres / MongoDB / MySQL impls
pub use source::{CdcEvent, CdcSource};

static INSTALLED_CDC_CONFIG: OnceLock<Mutex<CdcConfig>> = OnceLock::new();

pub(crate) const DEFAULT_CDC_IDEMPOTENCY_TTL_SECS: u64 = 604_800;
pub(crate) const DEFAULT_CDC_BROADCAST_CAPACITY: usize = 1024;
pub(crate) const DEFAULT_CDC_POLL_INTERVAL_MS: u64 = 250;
/// Maximum delay before topic-policy changes reach publishers and subscribers.
/// Resolved once with the rest of [`CdcConfig`]; never read on an event hot path.
pub(crate) const DEFAULT_CDC_TOPIC_POLICY_RELOAD_INTERVAL_MS: u64 = 1_000;
pub(crate) const DEFAULT_CDC_POLL_BATCH: i64 = 200;
pub(crate) const DEFAULT_CDC_PRODUCER_LINGER_MS: u64 = 20;
pub(crate) const DEFAULT_CDC_PRODUCER_BATCH_MESSAGES: u64 = 10_000;
/// Cooldown for the "failed to publish to kafka" ERROR log. When Kafka is
/// unreachable the tailer hits this on every event of every 250ms poll; collapse
/// it to one line per this window (carrying the suppressed count) so the log
/// stays usable. Override with `UDB_CDC_PUBLISH_FAIL_LOG_COOLDOWN_SECS`.
#[cfg(feature = "kafka")]
pub(crate) const DEFAULT_CDC_PUBLISH_FAIL_LOG_COOLDOWN_SECS: u64 = 30;
pub(crate) const DEFAULT_CDC_IDEMPOTENCY_KEY_PREFIX: &str = "idempotency:udb";

/// CDC scale-out (master-plan 6.5) — producer-epoch fold layout.
///
/// The durable fence is a single `producer_epoch BIGINT` column. To give each
/// tailer shard epoch-fenced ownership of its slice WITHOUT a schema change, the
/// shard ordinal is folded into the high bits of that i64 and the operator's
/// monotonic epoch keeps the low [`CDC_EPOCH_FOLD_BITS`]. The two never overlap,
/// so within a shard the folded value is still strictly monotonic in the epoch
/// (the fence is preserved) and across shards the value ranges are disjoint (one
/// writer per slice on takeover). With `shard_count == 1` the fold is skipped
/// entirely, so the column carries the raw operator epoch exactly as before.
pub(crate) const CDC_EPOCH_FOLD_BITS: u32 = 48;
/// Low-bit mask carrying the operator epoch: `2^48 - 1` (~2.8e14 bumps).
pub(crate) const CDC_EPOCH_FOLD_MASK: i64 = (1i64 << CDC_EPOCH_FOLD_BITS) - 1;
/// Shard ordinals occupy bits 48..63 (15 bits), leaving the sign bit clear so a
/// folded epoch is always non-negative. Caps the shard count to keep the fold
/// total within the positive i64 range.
pub(crate) const MAX_CDC_SHARDS: u32 = (1u32 << (63 - CDC_EPOCH_FOLD_BITS)) - 1;

/// Fold a shard ordinal into the operator's `producer_epoch` to produce the
/// durable per-shard fence written to `producer_epoch` columns and matched in the
/// in-doubt recovery WHERE clauses.
///
/// CRITICAL invariant (master-plan 6.5): when `shard_count <= 1` (the unsharded
/// default) this returns `producer_epoch` UNCHANGED — bit-identical to the
/// pre-shard behavior, a true no-op. Only when `shard_count > 1` is the ordinal
/// folded into the high bits.
///
/// Properties for `shard_count > 1`:
/// - **Monotonic per shard:** for a fixed shard, the result increases strictly
///   with `producer_epoch` (while the epoch fits in [`CDC_EPOCH_FOLD_BITS`]), so
///   `producer_epoch < current` still detects a prior epoch of the SAME shard.
/// - **Disjoint across shards:** distinct ordinals land in disjoint high-bit
///   bands ([`CdcConfig::shard_epoch_band`]), so no two shards' fences collide.
pub(crate) fn fence_producer_epoch(producer_epoch: i64, shard_id: u32, shard_count: u32) -> i64 {
    if shard_count <= 1 {
        // Unsharded: no fold. The fence is the raw operator epoch, exactly as the
        // pre-6.5 code wrote it.
        return producer_epoch;
    }
    // Defensive mask to the 15 ordinal bits (the ordinal is already clamped
    // `< shard_count <= MAX_CDC_SHARDS` by `normalize`).
    let shard = (shard_id as i64) & (MAX_CDC_SHARDS as i64);
    (shard << CDC_EPOCH_FOLD_BITS) | (producer_epoch & CDC_EPOCH_FOLD_MASK)
}

/// Stable FNV-1a 64-bit hash used for CDC shard partition assignment. Chosen over
/// `std::hash::DefaultHasher` (whose output is intentionally unstable across
/// builds) so every shard process computes the SAME owner for a given key with no
/// coordination.
pub(crate) fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Whether CDC change-event delivery is enabled. This gates BOTH the
/// transactional outbox WRITE (in each mutation's tx) AND the Kafka tailer /
/// storage-finalized consumer that drain it — so `UDB_CDC_ENABLED=false` is a
/// true full-stop: a deployment with no Kafka neither writes undrained outbox
/// rows (no unbounded `outbox_events` growth) nor opens any broker connection
/// (no resolve-failure log flood). Default = enabled. Resolved once: the env is
/// fixed for the process lifetime, and this is read on the mutation hot path.
pub(crate) fn cdc_delivery_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("UDB_CDC_ENABLED")
            .map(|value| {
                let value = value.trim();
                !(value == "0"
                    || value.eq_ignore_ascii_case("false")
                    || value.eq_ignore_ascii_case("no"))
            })
            .unwrap_or(true)
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CdcConfig {
    pub system_schema: String,
    pub outbox_table: String,
    pub offsets_table: String,
    pub lock_log_table: String,
    pub publication_name: String,
    pub slot_name: String,
    pub dlq_topic: String,
    pub schema_uri_template: String,
    pub valid_topics: Vec<String>,
    pub advisory_lock_key: i64,
    // Phase 7: Event schema registry
    pub schema_registry_url: String,
    pub schema_registry_auth: String,
    pub schema_registry_mode: SchemaRegistryMode,
    // Phase 7: Retry strategy
    pub retry_topic_prefix: String,
    pub max_retry_attempts: u32,
    pub retry_delay_secs: Vec<u64>,
    // Logical replication controls.
    pub replication_tls_required: bool,
    pub max_wal_lag_bytes: i64,
    pub exactly_once_mode: CdcExactlyOnceMode,
    pub transactional_id_prefix: String,
    pub producer_epoch: i64,
    /// CDC scale-out (master-plan 6.5): number of tailer shards configured for
    /// this source. `1` (the default) is the single, unsharded tailer — exactly
    /// today's behavior. `> 1` partitions outbox ownership across N processes,
    /// each claiming a deterministic, disjoint slice (see [`Self::owns_partition`])
    /// and fencing its slice with a shard-folded producer epoch (see
    /// [`fence_producer_epoch`]). Resolved ONCE from `UDB_CDC_SHARD_COUNT`; never
    /// read per event.
    pub shard_count: u32,
    /// This process's shard ordinal in `[0, shard_count)`. Default `0`. With
    /// `shard_count == 1` the ordinal is forced to `0` and the epoch fold is a
    /// no-op, so the single-shard path is bit-identical to the pre-shard code.
    /// Resolved ONCE from `UDB_CDC_SHARD_ID`.
    pub shard_id: u32,
    pub kafka_tx_timeout_secs: u64,
    pub redaction_mode: CdcRedactionMode,
    pub redaction_version: u32,
    /// Field names redacted from **external CDC source** events before publish.
    /// The outbox path derives sensitive fields from the UDB manifest, but
    /// external-source tables have no manifest, so the operator declares them via
    /// `UDB_CDC_SOURCE_SENSITIVE_FIELDS` (comma-separated). Empty = no source redaction.
    pub source_sensitive_fields: Vec<String>,
    pub idempotency_ttl_secs: u64,
    pub broadcast_capacity: usize,
    pub poll_interval_ms: u64,
    pub topic_policy_reload_interval_ms: u64,
    pub poll_batch: i64,
    pub producer_linger_ms: u64,
    pub producer_batch_messages: u64,
    pub idempotency_key_prefix: String,
    #[serde(skip)]
    pub encryption_key_resolver: Option<encryption::StaticKeyResolver>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaRegistryMode {
    Off,
    Warn,
    FailClosed,
}

impl Default for SchemaRegistryMode {
    fn default() -> Self {
        Self::Warn
    }
}

impl SchemaRegistryMode {
    pub fn from_env_value(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "off" | "disabled" | "none" => Self::Off,
            "fail_closed" | "fail-closed" | "strict" | "required" => Self::FailClosed,
            _ => Self::Warn,
        }
    }

    pub fn fail_closed(self) -> bool {
        matches!(self, Self::FailClosed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CdcExactlyOnceMode {
    AtLeastOnce,
    StateMachine,
    KafkaTransactional,
}

impl Default for CdcExactlyOnceMode {
    fn default() -> Self {
        Self::AtLeastOnce
    }
}

impl CdcExactlyOnceMode {
    pub fn from_env_value(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "state_machine" | "state-machine" | "ledger" => Self::StateMachine,
            "kafka_transactional"
            | "kafka-transactional"
            | "transactional"
            | "exactly_once"
            | "exactly-once" => Self::KafkaTransactional,
            _ => Self::AtLeastOnce,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CdcRedactionMode {
    Mask,
    Drop,
    Hash,
    /// Reversible AES-256-GCM-SIV field encryption (U22). Sensitive fields are
    /// replaced with an `EncryptedField` envelope an authorised consumer can
    /// later decrypt, instead of being irreversibly masked/dropped/hashed.
    Encrypt,
}

impl Default for CdcRedactionMode {
    fn default() -> Self {
        Self::Mask
    }
}

impl CdcRedactionMode {
    pub fn from_env_value(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "drop" | "remove" => Self::Drop,
            "hash" | "sha256" => Self::Hash,
            "encrypt" | "reversible" => Self::Encrypt,
            _ => Self::Mask,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mask => "mask",
            Self::Drop => "drop",
            Self::Hash => "hash",
            Self::Encrypt => "encrypt",
        }
    }
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            system_schema: "udb_system".to_string(),
            outbox_table: "outbox_events".to_string(),
            offsets_table: "udb_cdc_offsets".to_string(),
            lock_log_table: "udb_cdc_lock_log".to_string(),
            publication_name: "udb_outbox_pub".to_string(),
            slot_name: "udb_outbox_slot".to_string(),
            dlq_topic: "workflow.dead_letter.v1".to_string(),
            schema_uri_template: "udb.{domain}.events.v1.Message".to_string(),
            valid_topics: Vec::new(),
            advisory_lock_key: 0x0075_6462_5f63_6463,
            schema_registry_url: String::new(),
            schema_registry_auth: String::new(),
            schema_registry_mode: SchemaRegistryMode::default(),
            retry_topic_prefix: "workflow.retry".to_string(),
            max_retry_attempts: 3,
            retry_delay_secs: vec![10, 60, 300], // 10s, 1m, 5m
            replication_tls_required: true,
            max_wal_lag_bytes: 1_073_741_824,
            exactly_once_mode: CdcExactlyOnceMode::default(),
            transactional_id_prefix: "udb-cdc".to_string(),
            producer_epoch: 0,
            shard_count: 1,
            shard_id: 0,
            kafka_tx_timeout_secs: 30,
            redaction_mode: CdcRedactionMode::default(),
            redaction_version: 1,
            source_sensitive_fields: Vec::new(),
            idempotency_ttl_secs: DEFAULT_CDC_IDEMPOTENCY_TTL_SECS,
            broadcast_capacity: DEFAULT_CDC_BROADCAST_CAPACITY,
            poll_interval_ms: DEFAULT_CDC_POLL_INTERVAL_MS,
            topic_policy_reload_interval_ms: DEFAULT_CDC_TOPIC_POLICY_RELOAD_INTERVAL_MS,
            poll_batch: DEFAULT_CDC_POLL_BATCH,
            producer_linger_ms: DEFAULT_CDC_PRODUCER_LINGER_MS,
            producer_batch_messages: DEFAULT_CDC_PRODUCER_BATCH_MESSAGES,
            idempotency_key_prefix: DEFAULT_CDC_IDEMPOTENCY_KEY_PREFIX.to_string(),
            encryption_key_resolver: None,
        }
    }
}

impl CdcConfig {
    pub fn install_global(config: Self) {
        let cell = INSTALLED_CDC_CONFIG.get_or_init(|| Mutex::new(Self::from_env_uninstalled()));
        if let Ok(mut guard) = cell.lock() {
            *guard = config;
        }
    }

    pub fn current() -> Self {
        INSTALLED_CDC_CONFIG
            .get()
            .and_then(|cell| cell.lock().ok().map(|guard| guard.clone()))
            .unwrap_or_else(Self::from_env_uninstalled)
    }

    pub fn from_env() -> Self {
        Self::current()
    }

    pub fn from_env_uninstalled() -> Self {
        let defaults = Self::default();
        let mut config = Self {
            system_schema: env_identifier("UDB_CDC_SYSTEM_SCHEMA", &defaults.system_schema),
            outbox_table: env_identifier("UDB_CDC_OUTBOX_TABLE", &defaults.outbox_table),
            offsets_table: env_identifier("UDB_CDC_OFFSETS_TABLE", &defaults.offsets_table),
            lock_log_table: env_identifier("UDB_CDC_LOCK_LOG_TABLE", &defaults.lock_log_table),
            publication_name: env_identifier("UDB_CDC_PUBLICATION", &defaults.publication_name),
            slot_name: env_identifier("UDB_CDC_SLOT", &defaults.slot_name),
            dlq_topic: std::env::var("UDB_CDC_DLQ_TOPIC").unwrap_or(defaults.dlq_topic),
            schema_uri_template: std::env::var("UDB_CDC_SCHEMA_URI_TEMPLATE")
                .unwrap_or(defaults.schema_uri_template),
            valid_topics: std::env::var("UDB_CDC_VALID_TOPICS")
                .ok()
                .map(|raw| {
                    raw.split(',')
                        .map(str::trim)
                        .filter(|topic| !topic.is_empty())
                        .map(ToString::to_string)
                        .collect()
                })
                .unwrap_or_default(),
            advisory_lock_key: std::env::var("UDB_CDC_ADVISORY_LOCK_KEY")
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(defaults.advisory_lock_key),
            // Phase 7: Schema registry
            schema_registry_url: std::env::var("UDB_SCHEMA_REGISTRY_URL").unwrap_or_default(),
            schema_registry_auth: std::env::var("UDB_SCHEMA_REGISTRY_AUTH").unwrap_or_default(),
            schema_registry_mode: std::env::var("UDB_SCHEMA_REGISTRY_MODE")
                .or_else(|_| std::env::var("UDB_CDC_SCHEMA_REGISTRY_MODE"))
                .map(|value| SchemaRegistryMode::from_env_value(&value))
                .unwrap_or(defaults.schema_registry_mode),
            // Phase 7: Retry strategy
            retry_topic_prefix: std::env::var("UDB_CDC_RETRY_TOPIC_PREFIX")
                .unwrap_or(defaults.retry_topic_prefix),
            max_retry_attempts: std::env::var("UDB_CDC_MAX_RETRY_ATTEMPTS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(defaults.max_retry_attempts),
            retry_delay_secs: std::env::var("UDB_CDC_RETRY_DELAY_SECS")
                .ok()
                .and_then(|raw| {
                    raw.split(',')
                        .map(|s| s.trim().parse::<u64>().ok())
                        .collect::<Option<Vec<_>>>()
                })
                .unwrap_or(defaults.retry_delay_secs),
            replication_tls_required: std::env::var("UDB_CDC_TLS")
                .map(|value| !matches!(value.as_str(), "0" | "false" | "no"))
                .unwrap_or(defaults.replication_tls_required),
            max_wal_lag_bytes: std::env::var("UDB_CDC_MAX_WAL_LAG_BYTES")
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(defaults.max_wal_lag_bytes),
            exactly_once_mode: std::env::var("UDB_CDC_EXACTLY_ONCE_MODE")
                .map(|value| CdcExactlyOnceMode::from_env_value(&value))
                .unwrap_or(defaults.exactly_once_mode),
            transactional_id_prefix: std::env::var("UDB_CDC_TRANSACTIONAL_ID_PREFIX")
                .unwrap_or(defaults.transactional_id_prefix),
            producer_epoch: std::env::var("UDB_CDC_PRODUCER_EPOCH")
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(defaults.producer_epoch),
            shard_count: std::env::var("UDB_CDC_SHARD_COUNT")
                .ok()
                .and_then(|value| value.trim().parse::<u32>().ok())
                .unwrap_or(defaults.shard_count),
            shard_id: std::env::var("UDB_CDC_SHARD_ID")
                .ok()
                .and_then(|value| value.trim().parse::<u32>().ok())
                .unwrap_or(defaults.shard_id),
            kafka_tx_timeout_secs: std::env::var("UDB_CDC_KAFKA_TX_TIMEOUT_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(defaults.kafka_tx_timeout_secs),
            redaction_mode: std::env::var("UDB_CDC_REDACTION_MODE")
                .map(|value| CdcRedactionMode::from_env_value(&value))
                .unwrap_or(defaults.redaction_mode),
            redaction_version: std::env::var("UDB_CDC_REDACTION_VERSION")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(defaults.redaction_version),
            source_sensitive_fields: std::env::var("UDB_CDC_SOURCE_SENSITIVE_FIELDS")
                .ok()
                .map(|value| {
                    value
                        .split(',')
                        .map(|field| field.trim().to_string())
                        .filter(|field| !field.is_empty())
                        .collect::<Vec<_>>()
                })
                .unwrap_or(defaults.source_sensitive_fields),
            idempotency_ttl_secs: std::env::var("UDB_CDC_IDEMPOTENCY_TTL_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(defaults.idempotency_ttl_secs),
            broadcast_capacity: std::env::var("UDB_CDC_BROADCAST_CAPACITY")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(defaults.broadcast_capacity),
            poll_interval_ms: std::env::var("UDB_CDC_POLL_INTERVAL_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(defaults.poll_interval_ms),
            topic_policy_reload_interval_ms: std::env::var(
                "UDB_CDC_TOPIC_POLICY_RELOAD_INTERVAL_MS",
            )
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(defaults.topic_policy_reload_interval_ms),
            poll_batch: std::env::var("UDB_CDC_POLL_BATCH")
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(defaults.poll_batch),
            producer_linger_ms: std::env::var("UDB_CDC_PRODUCER_LINGER_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(defaults.producer_linger_ms),
            producer_batch_messages: std::env::var("UDB_CDC_PRODUCER_BATCH_MESSAGES")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(defaults.producer_batch_messages),
            idempotency_key_prefix: std::env::var("UDB_CDC_IDEMPOTENCY_KEY_PREFIX")
                .unwrap_or(defaults.idempotency_key_prefix),
            encryption_key_resolver: crate::runtime::cdc::encryption::StaticKeyResolver::from_env(),
        };
        config.normalize();
        config
    }

    pub fn merge_env(&mut self) {
        self.system_schema = env_identifier("UDB_CDC_SYSTEM_SCHEMA", &self.system_schema);
        self.outbox_table = env_identifier("UDB_CDC_OUTBOX_TABLE", &self.outbox_table);
        self.offsets_table = env_identifier("UDB_CDC_OFFSETS_TABLE", &self.offsets_table);
        self.lock_log_table = env_identifier("UDB_CDC_LOCK_LOG_TABLE", &self.lock_log_table);
        self.publication_name = env_identifier("UDB_CDC_PUBLICATION", &self.publication_name);
        self.slot_name = env_identifier("UDB_CDC_SLOT", &self.slot_name);
        if let Ok(value) = std::env::var("UDB_CDC_DLQ_TOPIC") {
            self.dlq_topic = value;
        }
        if let Ok(value) = std::env::var("UDB_CDC_SCHEMA_URI_TEMPLATE") {
            self.schema_uri_template = value;
        }
        if let Ok(raw) = std::env::var("UDB_CDC_VALID_TOPICS") {
            self.valid_topics = raw
                .split(',')
                .map(str::trim)
                .filter(|topic| !topic.is_empty())
                .map(ToString::to_string)
                .collect();
        }
        if let Ok(value) = std::env::var("UDB_CDC_ADVISORY_LOCK_KEY")
            && let Ok(parsed) = value.parse::<i64>()
        {
            self.advisory_lock_key = parsed;
        }
        if let Ok(value) = std::env::var("UDB_SCHEMA_REGISTRY_URL") {
            self.schema_registry_url = value;
        }
        if let Ok(value) = std::env::var("UDB_SCHEMA_REGISTRY_AUTH") {
            self.schema_registry_auth = value;
        }
        if let Ok(value) = std::env::var("UDB_SCHEMA_REGISTRY_MODE")
            .or_else(|_| std::env::var("UDB_CDC_SCHEMA_REGISTRY_MODE"))
        {
            self.schema_registry_mode = SchemaRegistryMode::from_env_value(&value);
        }
        if let Ok(value) = std::env::var("UDB_CDC_RETRY_TOPIC_PREFIX") {
            self.retry_topic_prefix = value;
        }
        if let Ok(value) = std::env::var("UDB_CDC_MAX_RETRY_ATTEMPTS")
            && let Ok(parsed) = value.parse::<u32>()
        {
            self.max_retry_attempts = parsed;
        }
        if let Ok(raw) = std::env::var("UDB_CDC_RETRY_DELAY_SECS")
            && let Some(delays) = raw
                .split(',')
                .map(|s| s.trim().parse::<u64>().ok())
                .collect::<Option<Vec<_>>>()
        {
            self.retry_delay_secs = delays;
        }
        if let Ok(value) = std::env::var("UDB_CDC_TLS") {
            self.replication_tls_required = !matches!(value.as_str(), "0" | "false" | "no");
        }
        if let Ok(value) = std::env::var("UDB_CDC_MAX_WAL_LAG_BYTES")
            && let Ok(parsed) = value.parse::<i64>()
        {
            self.max_wal_lag_bytes = parsed;
        }
        if let Ok(value) = std::env::var("UDB_CDC_EXACTLY_ONCE_MODE") {
            self.exactly_once_mode = CdcExactlyOnceMode::from_env_value(&value);
        }
        if let Ok(value) = std::env::var("UDB_CDC_TRANSACTIONAL_ID_PREFIX") {
            self.transactional_id_prefix = value;
        }
        if let Ok(value) = std::env::var("UDB_CDC_PRODUCER_EPOCH")
            && let Ok(parsed) = value.parse::<i64>()
        {
            self.producer_epoch = parsed;
        }
        if let Ok(value) = std::env::var("UDB_CDC_SHARD_COUNT")
            && let Ok(parsed) = value.trim().parse::<u32>()
        {
            self.shard_count = parsed;
        }
        if let Ok(value) = std::env::var("UDB_CDC_SHARD_ID")
            && let Ok(parsed) = value.trim().parse::<u32>()
        {
            self.shard_id = parsed;
        }
        if let Ok(value) = std::env::var("UDB_CDC_KAFKA_TX_TIMEOUT_SECS")
            && let Ok(parsed) = value.parse::<u64>()
        {
            self.kafka_tx_timeout_secs = parsed;
        }
        if let Ok(value) = std::env::var("UDB_CDC_REDACTION_MODE") {
            self.redaction_mode = CdcRedactionMode::from_env_value(&value);
        }
        if let Ok(value) = std::env::var("UDB_CDC_REDACTION_VERSION")
            && let Ok(parsed) = value.parse::<u32>()
        {
            self.redaction_version = parsed;
        }
        if let Ok(value) = std::env::var("UDB_CDC_IDEMPOTENCY_TTL_SECS")
            && let Ok(parsed) = value.parse::<u64>()
        {
            self.idempotency_ttl_secs = parsed;
        }
        if let Ok(value) = std::env::var("UDB_CDC_BROADCAST_CAPACITY")
            && let Ok(parsed) = value.parse::<usize>()
        {
            self.broadcast_capacity = parsed;
        }
        if let Ok(value) = std::env::var("UDB_CDC_POLL_INTERVAL_MS")
            && let Ok(parsed) = value.parse::<u64>()
        {
            self.poll_interval_ms = parsed;
        }
        if let Ok(value) = std::env::var("UDB_CDC_TOPIC_POLICY_RELOAD_INTERVAL_MS")
            && let Ok(parsed) = value.parse::<u64>()
        {
            self.topic_policy_reload_interval_ms = parsed;
        }
        if let Ok(value) = std::env::var("UDB_CDC_POLL_BATCH")
            && let Ok(parsed) = value.parse::<i64>()
        {
            self.poll_batch = parsed;
        }
        if let Ok(value) = std::env::var("UDB_CDC_PRODUCER_LINGER_MS")
            && let Ok(parsed) = value.parse::<u64>()
        {
            self.producer_linger_ms = parsed;
        }
        if let Ok(value) = std::env::var("UDB_CDC_PRODUCER_BATCH_MESSAGES")
            && let Ok(parsed) = value.parse::<u64>()
        {
            self.producer_batch_messages = parsed;
        }
        if let Ok(value) = std::env::var("UDB_CDC_IDEMPOTENCY_KEY_PREFIX") {
            self.idempotency_key_prefix = value;
        }
        self.normalize();
    }

    fn normalize(&mut self) {
        if self.retry_delay_secs.is_empty() {
            self.retry_delay_secs = Self::default().retry_delay_secs;
        }
        if self.max_wal_lag_bytes <= 0 {
            self.max_wal_lag_bytes = Self::default().max_wal_lag_bytes;
        }
        if self.transactional_id_prefix.trim().is_empty() {
            self.transactional_id_prefix = Self::default().transactional_id_prefix;
        }
        if self.redaction_version == 0 {
            self.redaction_version = 1;
        }
        if self.kafka_tx_timeout_secs == 0 {
            self.kafka_tx_timeout_secs = Self::default().kafka_tx_timeout_secs;
        }
        // CDC scale-out (6.5): a shard count of 0 is meaningless — clamp to the
        // single-shard default. The ordinal must be a valid index into the shard
        // set; an out-of-range ordinal (incl. any non-zero value while unsharded)
        // is clamped to 0 so the single-shard path stays bit-identical and a
        // mis-set ordinal can never silently disown every partition.
        if self.shard_count == 0 {
            self.shard_count = 1;
        }
        if self.shard_count > MAX_CDC_SHARDS {
            self.shard_count = MAX_CDC_SHARDS;
        }
        if self.shard_id >= self.shard_count {
            warn!(
                "[cdc] UDB_CDC_SHARD_ID={} is out of range for UDB_CDC_SHARD_COUNT={}; \
                 clamping ordinal to 0",
                self.shard_id, self.shard_count
            );
            self.shard_id = 0;
        }
        if self.idempotency_ttl_secs == 0 {
            self.idempotency_ttl_secs = Self::default().idempotency_ttl_secs;
        }
        if self.broadcast_capacity == 0 {
            self.broadcast_capacity = Self::default().broadcast_capacity;
        }
        if self.poll_interval_ms == 0 {
            self.poll_interval_ms = Self::default().poll_interval_ms;
        }
        if self.topic_policy_reload_interval_ms == 0 {
            self.topic_policy_reload_interval_ms = Self::default().topic_policy_reload_interval_ms;
        }
        if self.poll_batch <= 0 {
            self.poll_batch = Self::default().poll_batch;
        }
        if self.producer_batch_messages == 0 {
            self.producer_batch_messages = Self::default().producer_batch_messages;
        }
        if self.idempotency_key_prefix.trim().is_empty() {
            self.idempotency_key_prefix = Self::default().idempotency_key_prefix;
        }
        // Auth security/audit events must ALWAYS be relayed: when an operator
        // tightens the CDC topic allowlist (`UDB_CDC_VALID_TOPICS`) to a custom
        // set, a restrictive allowlist would otherwise silently DROP the native
        // auth/authz/apikey/idp/ops outbox rows that carry login/revocation/
        // policy/break-glass audit events. The shared `AUTH_TOPIC_PATTERNS`
        // wildcard set exists precisely to guarantee those topics survive any
        // operator allowlist. An empty `valid_topics` is the "open" posture
        // (everything relayed), so we only force-insert when an allowlist is set.
        if !self.valid_topics.is_empty() {
            for pattern in crate::runtime::service::AUTH_TOPIC_PATTERNS {
                let pattern = (*pattern).to_string();
                if !self.valid_topics.iter().any(|t| t == &pattern) {
                    self.valid_topics.push(pattern);
                }
            }
        }
    }

    pub fn outbox_relation(&self) -> String {
        format!("{}.{}", qi(&self.system_schema), qi(&self.outbox_table))
    }

    /// #115: dialect-specific outbox identifiers. The Postgres
    /// [`outbox_relation`](Self::outbox_relation) double-quotes a
    /// `schema.table` pair, which MySQL (backtick identifiers, database-as-schema)
    /// and SQLite (no schemas; identifier validator rejects dots/quotes) cannot
    /// accept. These return engine-correct identifiers for the connected database.
    ///
    /// Bare, unquoted outbox table name (SQLite operates on a single attached
    /// database and its canonical store validates `[A-Za-z0-9_]+`).
    pub fn outbox_table_bare(&self) -> String {
        self.outbox_table.clone()
    }

    /// Backtick-quoted outbox table for MySQL, resolved within the database the
    /// pool is connected to (MySQL's "schema" is the connected database).
    pub fn outbox_relation_mysql(&self) -> String {
        format!("`{}`", self.outbox_table.replace('`', "``"))
    }

    /// Bracket-quoted outbox table for SQL Server (B.8). MSSQL uses `[ident]`
    /// quoting; the table lives in the connected database's default schema.
    pub fn outbox_relation_mssql(&self) -> String {
        format!("[{}]", self.outbox_table.replace(']', "]]"))
    }

    pub fn offsets_relation(&self) -> String {
        format!("{}.{}", qi(&self.system_schema), qi(&self.offsets_table))
    }

    pub fn lock_log_relation(&self) -> String {
        format!("{}.{}", qi(&self.system_schema), qi(&self.lock_log_table))
    }

    pub fn transactional_id(&self) -> String {
        format!("{}-{}", self.transactional_id_prefix, self.slot_name)
    }

    /// CDC scale-out (6.5): the durable producer-epoch fence this process writes
    /// to `producer_epoch` columns and matches in in-doubt recovery. Folds this
    /// process's shard ordinal into [`Self::producer_epoch`]; for the unsharded
    /// default (`shard_count == 1`) this is the raw epoch — BIT-IDENTICAL to the
    /// pre-shard fence — so every UPSERT/recovery bind is unchanged when N=1.
    pub fn fenced_producer_epoch(&self) -> i64 {
        fence_producer_epoch(self.producer_epoch, self.shard_id, self.shard_count)
    }

    /// Whether more than one tailer shard is configured. When `false`, the entire
    /// sharding apparatus (epoch fold, ownership filter, recovery banding) is a
    /// no-op and behavior is exactly the pre-6.5 single tailer.
    pub fn is_sharded(&self) -> bool {
        self.shard_count > 1
    }

    /// Inclusive `[lo, hi]` band of folded `producer_epoch` values owned by THIS
    /// shard, or `None` when unsharded. In-doubt recovery uses it to scope its
    /// sweep to this shard's own rows so two shards never reset each other's
    /// in-flight publishes (disjoint ownership under takeover). `None` keeps the
    /// recovery SQL bit-identical to the pre-shard path.
    pub fn shard_epoch_band(&self) -> Option<(i64, i64)> {
        if !self.is_sharded() {
            return None;
        }
        let base = (self.shard_id as i64 & MAX_CDC_SHARDS as i64) << CDC_EPOCH_FOLD_BITS;
        Some((base, base | CDC_EPOCH_FOLD_MASK))
    }

    /// Deterministic shard ordinal that OWNS a given partition key. The key space
    /// is partitioned by an FNV-1a hash modulo `shard_count`, so every key maps to
    /// exactly one shard and the shards' slices are disjoint by construction. The
    /// hash is content-only (no process/version state), so all shards agree on the
    /// owner without coordination.
    pub fn partition_owner(&self, partition_key: &str) -> u32 {
        if self.shard_count <= 1 {
            return 0;
        }
        (fnv1a_64(partition_key.as_bytes()) % self.shard_count as u64) as u32
    }

    /// Whether THIS shard owns `partition_key`. Always `true` when unsharded, so
    /// the single tailer claims every row exactly as before; when sharded, each
    /// row is claimed by exactly one shard ([`Self::partition_owner`]).
    pub fn owns_partition(&self, partition_key: &str) -> bool {
        self.shard_count <= 1 || self.partition_owner(partition_key) == self.shard_id
    }

    #[cfg(feature = "kafka")]
    fn schema_uri_for(&self, event_type: &str) -> Option<String> {
        // Native event types are `udb.<domain>.<entity>.<verb>.vN`; the
        // meaningful domain is the segment AFTER the `udb` namespace prefix
        // (e.g. `udb.authn.user.registered.v1` → `authn`), not the literal
        // `udb`. Fall back to the first segment for non-namespaced types.
        let mut parts = event_type.split('.').filter(|segment| !segment.is_empty());
        let first = parts.next()?;
        let domain = if first == "udb" {
            parts.next().unwrap_or(first)
        } else {
            first
        };
        Some(
            self.schema_uri_template
                .replace("{domain}", domain)
                .replace("{event_type}", event_type),
        )
    }

    #[cfg(any(feature = "kafka", test))]
    fn topic_allowed(&self, topic: &str) -> bool {
        self.valid_topics.is_empty()
            || self
                .valid_topics
                .iter()
                .any(|valid| WildMatch::new(valid).matches(topic))
    }
}

fn utc_now() -> DateTime<Utc> {
    Utc::now()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventEnvelope {
    pub event_id: String,
    pub event_type: String,
    #[serde(default = "utc_now")]
    pub timestamp: DateTime<Utc>,
    pub correlation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_number: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_agent: Option<String>,
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub project_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_uri: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub redaction_version: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub redacted_fields: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub redaction_mode: String,
    // Phase 10: compliance/audit lineage, converged from the auth + native
    // `ComplianceEnvelope`. CDC deserializes the outbox payload into this envelope;
    // before, it DROPPED these fields as unknown keys, so a CDC-delivered event lost
    // the actor/operation/decision/trace it was emitted with. Declaring them here
    // (all default-empty, so raw non-UDB application events deserialize unchanged)
    // makes CDC carry the same audit field set end-to-end — closing the "CDC
    // delivery on the unified compliance envelope" item. Field names match the keys
    // written by `build_native_compliance_envelope` / the auth event sink.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub actor: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub operation: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub outcome: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub decision_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub policy_version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub auth_method: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub trace_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub span_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub target_resource: String,
}

#[derive(Debug, Clone)]
pub struct CdcRedactionPreview {
    pub payload: serde_json::Value,
    pub redacted_fields: Vec<String>,
    pub redaction_mode: CdcRedactionMode,
    pub redaction_version: u32,
}

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

/// W3C `traceparent` for the current request, if any, to stamp on the egress
/// Kafka record so the distributed trace continues into downstream consumers
/// (Phase 10). Derived from the ambient trace context populated by the inbound
/// `TraceExtractLayer`; `None` when no trace is in scope (then no header is
/// attached and behaviour is unchanged). Pure string helper — no kafka dep — so
/// it compiles on the slim build.
pub(crate) fn current_egress_traceparent() -> Option<String> {
    crate::runtime::otel::current_trace_context().to_traceparent()
}

/// Conservative Phase-2 tenant-scope classifier for CDC topics. UDB-native
/// domain topics are tenant-scoped by contract; non-UDB application topics can
/// opt into tenant/project policy via `udb_topic_policy`.
pub fn tenant_scoped_topic(topic: &str) -> bool {
    let topic = topic.trim().to_ascii_lowercase();
    topic.starts_with("udb.") && !topic.starts_with("udb.ops.")
}

pub fn validate_topic_tenant_scope(topic: &str, envelope: &EventEnvelope) -> Result<(), String> {
    if tenant_scoped_topic(topic) && envelope.tenant_id.trim().is_empty() {
        return Err(format!(
            "tenant-scoped topic '{}' is missing envelope tenant_id",
            topic
        ));
    }
    Ok(())
}

pub fn cdc_sensitive_fields_for_manifest(
    manifest: &CatalogManifest,
    message_or_topic: &str,
) -> Vec<String> {
    let needle = message_or_topic.trim();
    if needle.is_empty() {
        return Vec::new();
    }
    let normalized = normalize_manifest_match_key(needle);
    let mut fields = Vec::new();
    for table in &manifest.tables {
        let candidates = [
            table.message_name.as_str(),
            table.table.as_str(),
            table.cdc_topic.as_str(),
        ];
        let matches_table = candidates.iter().any(|candidate| {
            let candidate = candidate.trim();
            !candidate.is_empty()
                && (normalize_manifest_match_key(candidate) == normalized
                    || normalized
                        .ends_with(&format!(".{}", normalize_manifest_match_key(candidate)))
                    || normalize_manifest_match_key(candidate)
                        .ends_with(&format!(".{}", normalized)))
        });
        if !matches_table {
            continue;
        }
        for column in &table.columns {
            if column.encrypted
                || column.security.is_pii
                || column.security.is_encrypted
                || column.security.mask_in_logs
            {
                if !column.field_name.trim().is_empty() {
                    fields.push(column.field_name.clone());
                }
                if !column.column_name.trim().is_empty() {
                    fields.push(column.column_name.clone());
                }
            }
        }
    }
    fields.sort();
    fields.dedup();
    fields
}

pub fn apply_manifest_cdc_redaction(
    manifest: &CatalogManifest,
    message_type: &str,
    topic: &str,
    schema_uri: Option<&str>,
    payload: serde_json::Value,
    mode: CdcRedactionMode,
    redaction_version: u32,
) -> serde_json::Value {
    preview_manifest_cdc_redaction(
        manifest,
        message_type,
        topic,
        schema_uri,
        payload,
        mode,
        redaction_version,
    )
    .payload
}

/// Recursively decrypt `EncryptedField` envelopes in CDC/admin JSON payloads.
///
/// Used by DLQ replay (`DecryptScope::Replay`) and by privileged admin audit
/// reads (`DecryptScope::Audit`). Failures leave the original envelope in place
/// so a bad key never corrupts the stored audit/DLQ data.
pub fn decrypt_encrypted_json_fields(
    value: serde_json::Value,
    resolver: &dyn encryption::CdcKeyResolver,
    scope: encryption::DecryptScope,
) -> serde_json::Value {
    use encryption::{EncryptedField, decrypt_field_value};
    match value {
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (key, nested) in map {
                if EncryptedField::is_envelope(&nested) {
                    let restored = serde_json::from_value::<EncryptedField>(nested.clone())
                        .ok()
                        .and_then(|env| decrypt_field_value(&env, resolver, scope).ok());
                    out.insert(key, restored.unwrap_or(nested));
                } else {
                    out.insert(key, decrypt_encrypted_json_fields(nested, resolver, scope));
                }
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .into_iter()
                .map(|item| decrypt_encrypted_json_fields(item, resolver, scope))
                .collect(),
        ),
        other => other,
    }
}

pub fn preview_manifest_cdc_redaction(
    manifest: &CatalogManifest,
    message_type: &str,
    topic: &str,
    schema_uri: Option<&str>,
    payload: serde_json::Value,
    mode: CdcRedactionMode,
    redaction_version: u32,
) -> CdcRedactionPreview {
    let candidates = [
        message_type,
        topic,
        schema_uri.unwrap_or_default(),
        payload
            .get("event_type")
            .and_then(|value| value.as_str())
            .unwrap_or_default(),
    ];
    let sensitive_fields = sensitive_fields_for_candidates(manifest, &candidates);
    if sensitive_fields.is_empty() {
        return CdcRedactionPreview {
            payload,
            redacted_fields: Vec::new(),
            redaction_mode: mode,
            redaction_version: redaction_version.max(1),
        };
    }

    let redacted = if mode == CdcRedactionMode::Encrypt {
        // U22: reversible field encryption. Build the key resolver from the
        // operator environment; if no key is configured (or encryption fails)
        // fall back to irreversible Mask so plaintext is NEVER emitted.
        match crate::runtime::cdc::encryption::StaticKeyResolver::from_env() {
            Some(resolver) => {
                let seed = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                let nonce = crate::runtime::cdc::encryption::CounterNonceProvider::seeded(seed);
                let mut encrypted = payload;
                match crate::runtime::cdc::encryption::encrypt_cdc_payload_fields_in_place(
                    &mut encrypted,
                    &sensitive_fields,
                    &resolver,
                    &nonce,
                ) {
                    Ok(()) => encrypted,
                    Err(err) => {
                        tracing::warn!(
                            "CDC field encryption failed ({err}); masking instead to avoid \
                             leaking plaintext"
                        );
                        redact_cdc_payload_fields(
                            encrypted,
                            &sensitive_fields,
                            CdcRedactionMode::Mask,
                        )
                    }
                }
            }
            None => {
                tracing::warn!(
                    "CDC redaction_mode=encrypt but no encryption key is configured \
                     (set UDB_CDC_ENCRYPTION_KEY_B64); masking sensitive fields instead"
                );
                redact_cdc_payload_fields(payload, &sensitive_fields, CdcRedactionMode::Mask)
            }
        }
    } else {
        redact_cdc_payload_fields(payload, &sensitive_fields, mode)
    };
    let payload = annotate_redaction(redacted, &sensitive_fields, mode, redaction_version);
    CdcRedactionPreview {
        payload,
        redacted_fields: sensitive_fields,
        redaction_mode: mode,
        redaction_version: redaction_version.max(1),
    }
}

fn sensitive_fields_for_candidates(manifest: &CatalogManifest, candidates: &[&str]) -> Vec<String> {
    let normalized_candidates = candidates
        .iter()
        .filter(|candidate| !candidate.trim().is_empty())
        .map(|candidate| normalize_manifest_match_key(candidate.trim()))
        .collect::<Vec<_>>();
    let mut sensitive_fields = Vec::new();
    for table in &manifest.tables {
        let table_candidates = [
            table.message_name.as_str(),
            table.table.as_str(),
            table.cdc_topic.as_str(),
        ];
        let matches_table = table_candidates.iter().any(|candidate| {
            let candidate = candidate.trim();
            if candidate.is_empty() {
                return false;
            }
            let normalized_table = normalize_manifest_match_key(candidate);
            normalized_candidates.iter().any(|needle| {
                &normalized_table == needle
                    || needle.ends_with(&format!(".{normalized_table}"))
                    || normalized_table.ends_with(&format!(".{needle}"))
            })
        });
        if !matches_table {
            continue;
        }
        for column in &table.columns {
            if column.encrypted
                || column.security.is_pii
                || column.security.is_encrypted
                || column.security.mask_in_logs
            {
                if !column.field_name.trim().is_empty() {
                    sensitive_fields.push(column.field_name.clone());
                }
                if !column.column_name.trim().is_empty() {
                    sensitive_fields.push(column.column_name.clone());
                }
            }
        }
    }
    sensitive_fields.sort();
    sensitive_fields.dedup();
    sensitive_fields
}

pub fn redact_cdc_payload_fields(
    mut value: serde_json::Value,
    sensitive_fields: &[String],
    mode: CdcRedactionMode,
) -> serde_json::Value {
    let keys = sensitive_fields
        .iter()
        .map(|field| field.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    redact_value(&mut value, &keys, mode);
    value
}

fn redact_value(
    value: &mut serde_json::Value,
    sensitive_fields: &std::collections::HashSet<String>,
    mode: CdcRedactionMode,
) {
    match value {
        serde_json::Value::Object(obj) => {
            let keys = obj.keys().cloned().collect::<Vec<_>>();
            for key in keys {
                if sensitive_fields.contains(&key.to_ascii_lowercase()) {
                    match mode {
                        CdcRedactionMode::Drop => {
                            obj.remove(&key);
                        }
                        CdcRedactionMode::Mask => {
                            obj.insert(key, serde_json::Value::String("***MASKED***".to_string()));
                        }
                        CdcRedactionMode::Hash => {
                            let hash = obj
                                .get(&key)
                                .map(hash_json_value)
                                .unwrap_or_else(|| "sha256:".to_string());
                            obj.insert(key, serde_json::Value::String(hash));
                        }
                        // Encrypt is dispatched in apply_manifest_cdc_redaction
                        // (it needs a key resolver this pure fn lacks). If it
                        // ever reaches here, mask — never leak plaintext.
                        CdcRedactionMode::Encrypt => {
                            obj.insert(key, serde_json::Value::String("***MASKED***".to_string()));
                        }
                    }
                } else if let Some(child) = obj.get_mut(&key) {
                    redact_value(child, sensitive_fields, mode);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                redact_value(item, sensitive_fields, mode);
            }
        }
        _ => {}
    }
}

fn annotate_redaction(
    mut value: serde_json::Value,
    sensitive_fields: &[String],
    mode: CdcRedactionMode,
    redaction_version: u32,
) -> serde_json::Value {
    if let Some(obj) = value.as_object_mut() {
        obj.insert(
            "redaction_version".to_string(),
            serde_json::Value::Number(serde_json::Number::from(redaction_version.max(1))),
        );
        obj.insert(
            "redaction_mode".to_string(),
            serde_json::Value::String(mode.as_str().to_string()),
        );
        obj.insert(
            "redacted_fields".to_string(),
            serde_json::Value::Array(
                sensitive_fields
                    .iter()
                    .cloned()
                    .map(serde_json::Value::String)
                    .collect(),
            ),
        );
    }
    value
}

fn hash_json_value(value: &serde_json::Value) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(value.to_string().as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

fn normalize_manifest_match_key(value: &str) -> String {
    value
        .trim()
        .trim_matches('"')
        .replace("::", ".")
        .to_ascii_lowercase()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DlqMeta {
    pub service: String,
    pub error_type: String,
    pub error_message: String,
    pub failed_at: DateTime<Utc>,
    pub retry_count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DlqEnvelope {
    pub failed_event: serde_json::Value,
    pub failure_metadata: DlqMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEvent {
    pub dlq_id: Uuid,
    pub event_id: Uuid,
    pub topic: String,
    pub payload: serde_json::Value,
    pub error_type: String,
    pub error_message: String,
    pub retry_count: i32,
    pub last_retry_at: Option<DateTime<Utc>>,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcMetrics {
    pub outbox_depth: i64,
    pub outbox_lag_seconds: f64,
    pub wal_lag_bytes: i64,
    pub dlq_open: i64,
    pub dlq_replayed: i64,
    pub dlq_dismissed: i64,
    pub dlq_quarantined: i64,
    pub events_by_topic: HashMap<String, i64>,
}

/// Phase 7: Topic policy loaded from `udb_system.udb_topic_policy`.
/// Controls per-topic allowlist, owning service/project, schema URI, and retry config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicPolicy {
    pub policy_id: i64,
    pub topic: String,
    pub tenant_id: String,
    pub owning_project: String,
    pub owning_service: String,
    pub schema_uri: String,
    pub redaction_mode: String,
    pub redaction_version: i32,
    pub retention_class: String,
    pub max_retry_attempts: i32,
    pub retry_delay_secs: Vec<i32>,
    pub dlq_enabled: bool,
    pub enabled: bool,
}

impl TopicPolicy {
    pub(crate) fn matches_topic(&self, topic: &str) -> bool {
        topic_policy_pattern_matches(&self.topic, topic)
    }

    pub(crate) fn allows_tenant(&self, tenant_id: &str) -> bool {
        topic_policy_scope_allows(&self.tenant_id, tenant_id)
    }

    pub(crate) fn allows_project(&self, project_id: &str) -> bool {
        topic_policy_scope_allows(&self.owning_project, project_id)
    }
}

pub(crate) fn topic_policy_pattern_matches(pattern: &str, topic: &str) -> bool {
    WildMatch::new(pattern.trim()).matches(topic.trim())
}

pub(crate) fn topic_policy_scope_allows(required: &str, actual: &str) -> bool {
    let required = required.trim();
    required.is_empty() || required == "*" || required == actual.trim()
}

/// One immutable topic-policy world shared by CDC publication, replay, live
/// subscription, and DLQ retry. `available=false` is a fail-closed sentinel used
/// after a reload failure; callers must not reinterpret it as an empty allowlist.
#[derive(Debug, Clone)]
pub(crate) struct TopicPolicySnapshot {
    pub(crate) policies: Vec<TopicPolicy>,
    pub(crate) generation: u64,
    pub(crate) loaded_at_unix: i64,
    pub(crate) available: bool,
}

impl TopicPolicySnapshot {
    fn unavailable() -> Self {
        Self {
            policies: Vec::new(),
            generation: 0,
            loaded_at_unix: 0,
            available: false,
        }
    }

    pub(crate) fn configured(&self) -> bool {
        !self.policies.is_empty()
    }

    pub(crate) fn active_policy_for(
        &self,
        topic: &str,
        tenant_id: &str,
        project_id: &str,
        privileged: bool,
    ) -> Option<&TopicPolicy> {
        self.policies.iter().find(|policy| {
            policy.enabled
                && policy.matches_topic(topic)
                && (privileged
                    || (policy.allows_tenant(tenant_id) && policy.allows_project(project_id)))
        })
    }

    pub(crate) fn active_topic_is_policy_owned(&self, topic: &str) -> bool {
        self.policies
            .iter()
            .any(|policy| policy.enabled && policy.matches_topic(topic))
    }
}

#[derive(Debug, Clone)]
pub struct CdcEnvelope {
    pub event_id: String,
    pub topic: String,
    pub partition_key: String,
    pub payload_json: String,
    pub published_at: DateTime<Utc>,
}

pub struct CdcEngine {
    pool: PgPool,
    #[cfg(feature = "redis")]
    redis: Option<redis::Client>,
    #[cfg(feature = "kafka")]
    kafka_producer: FutureProducer,
    broadcast_tx: broadcast::Sender<CdcEnvelope>,
    #[cfg(feature = "kafka")]
    dsn: String,
    metrics: std::sync::Arc<dyn MetricsRecorder>,
    config: CdcConfig,
    /// Live topic-policy world. Readers take one immutable snapshot per logical
    /// operation, while the reloader atomically publishes complete generations.
    topic_policies: Arc<ArcSwap<TopicPolicySnapshot>>,
}

impl fmt::Debug for CdcEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CdcEngine").finish_non_exhaustive()
    }
}

// Phase I: impl CdcEngine split into continuation impl modules.
mod engine_dlq;
mod engine_tail;
// U21: Kafka transactional exactly-once publish.
#[cfg(feature = "kafka")]
pub mod kafka_tx;
// U21 step 2: in-doubt recovery sweep run after init_transactions.
pub mod indoubt_recovery;
// U22: reversible CDC field encryption (AES-GCM-SIV + scope-gated decrypt).
pub mod encryption;
// Env-gated live-Postgres tests for the CDC delivery state machine
// (in-doubt reset, DLQ failure path, journal retention sweep).
#[cfg(test)]
mod live_tests;

// `env_identifier`, `is_identifier`, and `qi` are imported from
// `runtime::executor_utils` (single-sourced).

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn topic_policy(pattern: &str, tenant: &str, project: &str, enabled: bool) -> TopicPolicy {
        TopicPolicy {
            policy_id: 1,
            topic: pattern.to_string(),
            tenant_id: tenant.to_string(),
            owning_project: project.to_string(),
            owning_service: "billing".to_string(),
            schema_uri: String::new(),
            redaction_mode: "mask".to_string(),
            redaction_version: 1,
            retention_class: "standard".to_string(),
            max_retry_attempts: 3,
            retry_delay_secs: vec![10, 60, 300],
            dlq_enabled: true,
            enabled,
        }
    }

    #[test]
    fn topic_policy_uses_one_wildcard_and_scope_contract() {
        let policy = topic_policy("billing.*.v1", "tenant-a", "*", true);
        assert!(policy.matches_topic("billing.invoice.v1"));
        assert!(!policy.matches_topic("shipping.invoice.v1"));
        assert!(policy.allows_tenant("tenant-a"));
        assert!(!policy.allows_tenant("tenant-b"));
        assert!(policy.allows_project("any-project"));
    }

    #[test]
    fn disabled_final_policy_keeps_allowlist_configured_and_denies_match() {
        let snapshot = TopicPolicySnapshot {
            policies: vec![topic_policy("billing.*", "*", "*", false)],
            generation: 2,
            loaded_at_unix: 1,
            available: true,
        };
        assert!(snapshot.configured());
        assert!(
            snapshot
                .active_policy_for("billing.invoice", "tenant-a", "project-a", false)
                .is_none()
        );
    }

    #[test]
    fn outbox_payload_sanitizer_strips_nul_before_jsonb_insert() {
        let value = json!({
            "event_id": "evt-1",
            "payload": {
                "text": "payload\u{0}with-nul",
                "nested": ["a\u{0}b"],
                "bad\u{0}key": "value"
            }
        });
        let sanitized = strip_nul_json(&value);

        assert_eq!(sanitized["payload"]["text"], "payloadwith-nul");
        assert_eq!(sanitized["payload"]["nested"][0], "ab");
        assert_eq!(sanitized["payload"]["badkey"], "value");
        assert!(
            !sanitized.to_string().contains("\\u0000"),
            "Postgres JSONB rejects NUL escapes"
        );
    }

    #[test]
    fn event_envelope_is_project_neutral() {
        let event_id = Uuid::new_v4().to_string();
        let envelope: EventEnvelope = serde_json::from_value(json!({
            "event_id": event_id,
            "event_type": "billing.invoice.created",
            "correlation_id": "corr-1",
            "payload": {"invoice_id": "inv-1"}
        }))
        .expect("neutral envelope should deserialize");

        assert_eq!(envelope.event_type, "billing.invoice.created");
        assert!(envelope.document_id.is_none());
        assert!(envelope.page_number.is_none());
        assert!(envelope.source_agent.is_none());
    }

    #[test]
    fn event_envelope_decodes_enriched_compliance_envelope() {
        // Phase 10 telemetry coherence: the native/auth lanes attach the full
        // compliance envelope (CloudEvents + actor/operation/trace). CDC now
        // RETAINS the audit lineage (actor/operation/outcome/decision/trace) on the
        // `EventEnvelope` instead of dropping it on deserialize, so a CDC-delivered
        // event carries the same compliance field set it was emitted with. Purely
        // CloudEvents-transport fields (specversion/source/...) remain ignored.
        let event_id = Uuid::new_v4().to_string();
        let envelope: EventEnvelope = serde_json::from_value(json!({
            "event_id": event_id,
            "event_type": "udb.storage.upload.registered.v1",
            "correlation_id": "corr-9",
            "document_id": "object-1",
            "payload": {"object_key": "k1"},
            "tenant_id": "acme",
            "project_id": "proj-1",
            "redaction_mode": "none",
            "redaction_version": 1,
            "redacted_fields": [],
            // Additive Phase-10 compliance fields the CDC envelope ignores:
            "specversion": "1.0",
            "source": "udb.native/acme",
            "subject": "object/abc",
            "datacontenttype": "application/json",
            "time": "2026-06-09T00:00:00Z",
            "actor": "svc-storage",
            "operation": "register_upload",
            "outcome": "success",
            "trace_id": "0af7651916cd43dd8448eb211c80319c",
            "span_id": "b7ad6b7169203331",
            "decision_id": "dec-1",
            "redaction_profile": "v1"
        }))
        .expect("enriched envelope must still decode");
        assert_eq!(envelope.event_type, "udb.storage.upload.registered.v1");
        assert_eq!(envelope.tenant_id, "acme");
        assert_eq!(envelope.project_id, "proj-1");
        assert_eq!(envelope.redaction_mode, "none");
        assert_eq!(envelope.document_id.as_deref(), Some("object-1"));
        assert_eq!(envelope.payload["object_key"], "k1");
        // Phase 10 convergence: the compliance lineage is now RETAINED, not dropped.
        assert_eq!(envelope.actor, "svc-storage");
        assert_eq!(envelope.operation, "register_upload");
        assert_eq!(envelope.outcome, "success");
        assert_eq!(envelope.decision_id, "dec-1");
        assert_eq!(envelope.trace_id, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(envelope.span_id, "b7ad6b7169203331");
    }

    #[test]
    fn tenant_scoped_cdc_topic_rejects_missing_tenant() {
        let event_id = Uuid::new_v4().to_string();
        let envelope: EventEnvelope = serde_json::from_value(json!({
            "event_id": event_id,
            "event_type": "udb.storage.upload.registered.v1",
            "correlation_id": "corr-tenant-missing",
            "payload": {"object_key": "k1"},
            "redaction_mode": "none",
            "redaction_version": 1
        }))
        .expect("envelope should decode before scope validation");

        let err = validate_topic_tenant_scope("udb.storage.upload.registered.v1", &envelope)
            .expect_err("tenant-scoped UDB topics must fail closed without tenant_id");
        assert!(err.contains("missing envelope tenant_id"));
    }

    #[test]
    fn non_udb_cdc_topic_can_remain_unscoped() {
        let event_id = Uuid::new_v4().to_string();
        let envelope: EventEnvelope = serde_json::from_value(json!({
            "event_id": event_id,
            "event_type": "billing.invoice.created",
            "correlation_id": "corr-neutral",
            "payload": {"invoice_id": "inv-1"}
        }))
        .expect("neutral envelope should decode");

        validate_topic_tenant_scope("billing.invoice.created", &envelope)
            .expect("non-UDB topics use explicit topic policy, not implicit tenant scope");
    }

    #[test]
    fn udb_ops_topics_are_platform_scoped() {
        let event_id = Uuid::new_v4().to_string();
        let envelope: EventEnvelope = serde_json::from_value(json!({
            "event_id": event_id,
            "event_type": "udb.ops.readiness.failure.v1",
            "correlation_id": "corr-ops",
            "payload": {"check": "audit_sink"}
        }))
        .expect("ops envelope should decode");

        validate_topic_tenant_scope("udb.ops.readiness.failure.v1", &envelope)
            .expect("platform ops topics are not tenant-scoped CDC topics");
        assert!(!tenant_scoped_topic("udb.ops.readiness.failure.v1"));
    }

    #[test]
    fn topic_registry_accepts_wildcards() {
        let config = CdcConfig {
            valid_topics: vec!["billing.*.v1".to_string()],
            ..CdcConfig::default()
        };

        assert!(config.topic_allowed("billing.invoice.v1"));
        assert!(!config.topic_allowed("shipping.invoice.v1"));
    }

    #[test]
    fn default_advisory_key_is_udb_scoped() {
        assert_eq!(
            CdcConfig::default().advisory_lock_key,
            0x0075_6462_5f63_6463
        );
    }

    #[test]
    fn cdc_redaction_uses_manifest_security_metadata() {
        let manifest = CatalogManifest {
            tables: vec![crate::generation::ManifestTable {
                message_name: "Patient".to_string(),
                cdc_topic: "patient.updated.v1".to_string(),
                columns: vec![
                    crate::generation::ManifestColumn {
                        field_name: "email".to_string(),
                        column_name: "email_address".to_string(),
                        security: crate::generation::manifest::ManifestColumnSecurity {
                            is_pii: true,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    crate::generation::ManifestColumn {
                        field_name: "age".to_string(),
                        column_name: "age".to_string(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };

        let redacted = apply_manifest_cdc_redaction(
            &manifest,
            "Patient",
            "patient.updated.v1",
            None,
            json!({
                "event_id": "11111111-1111-4111-8111-111111111111",
                "event_type": "patient.updated.v1",
                "correlation_id": "corr-1",
                "document_id": "patient-1",
                "payload": {"email": "a@example.com", "age": 42}
            }),
            CdcRedactionMode::Mask,
            3,
        );

        assert_eq!(redacted["payload"]["email"], "***MASKED***");
        assert_eq!(redacted["payload"]["age"], 42);
        assert_eq!(redacted["redaction_version"], 3);
        assert_eq!(redacted["redaction_mode"], "mask");
    }

    #[test]
    fn cdc_encrypt_mode_masks_when_no_key_is_configured() {
        let prior_key = std::env::var("UDB_CDC_ENCRYPTION_KEY_B64").ok();
        let prior_key_id = std::env::var("UDB_CDC_ENCRYPTION_KEY_ID").ok();
        let prior_key_version = std::env::var("UDB_CDC_ENCRYPTION_KEY_VERSION").ok();

        // Tests mutate process-wide env before constructing the config under test.
        unsafe {
            std::env::remove_var("UDB_CDC_ENCRYPTION_KEY_B64");
            std::env::remove_var("UDB_CDC_ENCRYPTION_KEY_ID");
            std::env::remove_var("UDB_CDC_ENCRYPTION_KEY_VERSION");
        }

        let manifest = CatalogManifest {
            tables: vec![crate::generation::ManifestTable {
                message_name: "Patient".to_string(),
                cdc_topic: "patient.updated.v1".to_string(),
                columns: vec![crate::generation::ManifestColumn {
                    field_name: "email".to_string(),
                    column_name: "email".to_string(),
                    security: crate::generation::manifest::ManifestColumnSecurity {
                        is_pii: true,
                        is_encrypted: true,
                        ..Default::default()
                    },
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let redacted = apply_manifest_cdc_redaction(
            &manifest,
            "Patient",
            "patient.updated.v1",
            None,
            json!({"payload": {"email": "a@example.com"}}),
            CdcRedactionMode::Encrypt,
            1,
        );

        assert_eq!(redacted["payload"]["email"], "***MASKED***");
        assert_eq!(redacted["redaction_mode"], "encrypt");
        assert_eq!(redacted["redacted_fields"], json!(["email"]));

        restore_env("UDB_CDC_ENCRYPTION_KEY_B64", prior_key);
        restore_env("UDB_CDC_ENCRYPTION_KEY_ID", prior_key_id);
        restore_env("UDB_CDC_ENCRYPTION_KEY_VERSION", prior_key_version);
    }

    #[test]
    fn cdc_redaction_can_hash_or_drop_fields() {
        let hashed = redact_cdc_payload_fields(
            json!({"payload": {"email": "a@example.com", "age": 42}}),
            &["email".to_string()],
            CdcRedactionMode::Hash,
        );
        assert!(
            hashed["payload"]["email"]
                .as_str()
                .unwrap_or_default()
                .starts_with("sha256:")
        );

        let dropped = redact_cdc_payload_fields(
            json!({"payload": {"email": "a@example.com", "age": 42}}),
            &["email".to_string()],
            CdcRedactionMode::Drop,
        );
        assert!(dropped["payload"].get("email").is_none());
        assert_eq!(dropped["payload"]["age"], 42);
    }

    #[test]
    fn cdc_shard_fence_n1_is_bit_identical_to_unsharded_epoch() {
        // CRITICAL (master-plan 6.5): with a single shard the fold is a NO-OP —
        // the fence equals the operator's raw producer_epoch, byte-for-byte what
        // the pre-shard code wrote. Holds for every epoch; and because
        // shard_count==1 short-circuits, even a stray shard_id is ignored.
        for epoch in [
            0i64,
            1,
            7,
            100,
            4096,
            1 << 20,
            (1i64 << 47) - 1,
            i64::from(u32::MAX),
        ] {
            assert_eq!(fence_producer_epoch(epoch, 0, 1), epoch);
            assert_eq!(fence_producer_epoch(epoch, 9, 1), epoch);
            let cfg = CdcConfig {
                producer_epoch: epoch,
                shard_count: 1,
                shard_id: 0,
                ..CdcConfig::default()
            };
            assert_eq!(cfg.fenced_producer_epoch(), epoch);
            assert_eq!(cfg.shard_epoch_band(), None);
            assert!(!cfg.is_sharded());
        }
    }

    #[test]
    fn cdc_shard_fence_partitions_epoch_space_disjointly() {
        // N>1: distinct shards land in disjoint high-bit bands so two shards'
        // fences never collide; the low bits still carry the operator epoch.
        let shard_count = 4u32;
        let epoch = 42i64;
        let mut seen = std::collections::HashSet::new();
        for shard_id in 0..shard_count {
            let fenced = fence_producer_epoch(epoch, shard_id, shard_count);
            assert!(fenced >= 0, "fold must stay non-negative");
            assert_eq!(
                fenced & CDC_EPOCH_FOLD_MASK,
                epoch,
                "low bits carry the epoch"
            );
            assert_eq!(
                fenced >> CDC_EPOCH_FOLD_BITS,
                shard_id as i64,
                "high bits encode the shard ordinal"
            );
            assert!(seen.insert(fenced), "shard fences must be distinct");
            let cfg = CdcConfig {
                producer_epoch: epoch,
                shard_count,
                shard_id,
                ..CdcConfig::default()
            };
            let (lo, hi) = cfg.shard_epoch_band().expect("sharded band");
            assert!(
                lo <= fenced && fenced <= hi,
                "fence must lie in its own band"
            );
        }
    }

    #[test]
    fn cdc_shard_fence_is_monotonic_per_shard() {
        // The takeover fence stays monotonic within a shard: a higher operator
        // epoch yields a strictly higher fenced value for the SAME shard, so the
        // in-doubt sweep's `producer_epoch < current` still detects a prior epoch.
        let shard_count = 8u32;
        for shard_id in [0u32, 3, 7] {
            let mut prev = i64::MIN;
            for epoch in [0i64, 1, 2, 50, 1000, 1 << 30, 1i64 << 47] {
                let fenced = fence_producer_epoch(epoch, shard_id, shard_count);
                assert!(
                    fenced > prev,
                    "epoch {epoch} on shard {shard_id} broke monotonicity"
                );
                prev = fenced;
            }
        }
    }

    #[test]
    fn cdc_shard_partition_assignment_is_disjoint_and_total() {
        // Each partition key is owned by EXACTLY one shard (disjoint + total),
        // so two shards never both claim the same outbox row.
        let shard_count = 3u32;
        let shards: Vec<CdcConfig> = (0..shard_count)
            .map(|shard_id| CdcConfig {
                shard_count,
                shard_id,
                ..CdcConfig::default()
            })
            .collect();
        for key in [
            "tenant-a:1",
            "tenant-b:2",
            "order-42",
            "",
            "x",
            "a-very-long-partition-key-0xfeed",
        ] {
            let owners: Vec<u32> = shards
                .iter()
                .filter(|c| c.owns_partition(key))
                .map(|c| c.shard_id)
                .collect();
            assert_eq!(
                owners.len(),
                1,
                "key {key:?} must be owned by exactly one shard, got {owners:?}"
            );
            assert_eq!(owners[0], shards[0].partition_owner(key));
        }
    }

    #[test]
    fn cdc_single_shard_owns_every_partition() {
        // N=1: the lone tailer claims every row exactly as the pre-shard code did.
        let cfg = CdcConfig::default();
        assert_eq!(cfg.shard_count, 1);
        for key in ["a", "b", "anything", ""] {
            assert!(cfg.owns_partition(key));
            assert_eq!(cfg.partition_owner(key), 0);
        }
    }

    #[test]
    fn cdc_shard_config_normalizes_out_of_range_ordinal() {
        // A zero shard count collapses to the single-shard default; an ordinal
        // outside [0, count) is clamped to 0 so a misconfig never disowns every
        // partition (and the single-shard path stays bit-identical).
        let mut zero = CdcConfig {
            shard_count: 0,
            shard_id: 5,
            ..CdcConfig::default()
        };
        zero.normalize();
        assert_eq!(zero.shard_count, 1);
        assert_eq!(zero.shard_id, 0);
        assert!(!zero.is_sharded());

        let mut oob = CdcConfig {
            shard_count: 3,
            shard_id: 9,
            ..CdcConfig::default()
        };
        oob.normalize();
        assert_eq!(oob.shard_id, 0);

        let mut ok = CdcConfig {
            shard_count: 4,
            shard_id: 2,
            ..CdcConfig::default()
        };
        ok.normalize();
        assert_eq!((ok.shard_count, ok.shard_id), (4, 2));
    }

    fn restore_env(key: &str, value: Option<String>) {
        // Tests restore process-wide env to the captured pre-test values.
        unsafe {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}
