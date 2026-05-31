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
use std::sync::{Mutex, OnceLock};
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

pub mod source; // C2 + C3: per-backend CDC source trait + Postgres / MongoDB / MySQL impls
pub use source::{CdcEvent, CdcSource};

static INSTALLED_CDC_CONFIG: OnceLock<Mutex<CdcConfig>> = OnceLock::new();

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
    pub kafka_tx_timeout_secs: u64,
    pub redaction_mode: CdcRedactionMode,
    pub redaction_version: u32,
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
            _ => Self::Mask,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mask => "mask",
            Self::Drop => "drop",
            Self::Hash => "hash",
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
            kafka_tx_timeout_secs: 30,
            redaction_mode: CdcRedactionMode::default(),
            redaction_version: 1,
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
    }

    pub fn outbox_relation(&self) -> String {
        format!("{}.{}", qi(&self.system_schema), qi(&self.outbox_table))
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

    #[cfg(feature = "kafka")]
    fn schema_uri_for(&self, event_type: &str) -> Option<String> {
        let domain = event_type.split('.').next().unwrap_or_default();
        if domain.is_empty() {
            return None;
        }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_uri: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub redaction_version: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub redacted_fields: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub redaction_mode: String,
}

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
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
    let mut sensitive_fields = Vec::new();
    for candidate in [
        message_type,
        topic,
        schema_uri.unwrap_or_default(),
        payload
            .get("event_type")
            .and_then(|value| value.as_str())
            .unwrap_or_default(),
    ] {
        sensitive_fields.extend(cdc_sensitive_fields_for_manifest(manifest, candidate));
    }
    sensitive_fields.sort();
    sensitive_fields.dedup();
    if sensitive_fields.is_empty() {
        return payload;
    }

    let redacted = redact_cdc_payload_fields(payload, &sensitive_fields, mode);
    annotate_redaction(redacted, &sensitive_fields, mode, redaction_version)
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Phase 7: topic policies loaded from DB at engine construction.
    /// Empty means all topics allowed (backward-compatible default).
    topic_policies: Vec<TopicPolicy>,
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

// `env_identifier`, `is_identifier`, and `qi` are imported from
// `runtime::executor_utils` (single-sourced).

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
}
