//! Shared canonical `SystemStores` adapter for vector/search REST backends.
//!
//! Pinecone, Weaviate, and Elasticsearch already have native HTTP clients in
//! the runtime. This module gives them a real canonical system-state plane
//! instead of keeping them as projection-only islands. The adapter stores UDB
//! system records in a dedicated backend namespace/index/class per instance,
//! using deterministic IDs derived from logical record keys.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[cfg(feature = "elasticsearch")]
use crate::runtime::executors::elasticsearch::ElasticsearchHttpClient;
#[cfg(feature = "pinecone")]
use crate::runtime::executors::pinecone::PineconeHttpClient;
#[cfg(feature = "weaviate")]
use crate::runtime::executors::weaviate::WeaviateHttpClient;

use super::system_store::{
    AdminAuditChainReport, AdminAuditInsert, AdminAuditListFilter, AdminAuditRow, AdvisoryLeaseRow,
    CompensationStatus, DeadLetterGroup, JsonProjectionTaskAdapter, JsonSystemRecordAdapter,
    MigrationAuditStore, MigrationOpInsert, MigrationOpRow, MigrationRunInsert, MigrationRunRow,
    MigrationRunState, MigrationRunsFilter, PendingTaskMetric, ProjectionClaimFilter,
    ProjectionTaskFailurePolicy, ProjectionTaskInsert, ProjectionTaskRow, ProjectionTaskStatus,
    ProjectionTaskStore, ProjectionTaskSummary, SagaInsert, SagaListFilter, SagaRow, SagaStatus,
    SagaStore, SagaSummary, SystemStoreError, SystemStoreResult, advisory_lease_can_acquire,
    advisory_lease_is_owned_by, append_json_admin_audit, claim_json_projection_tasks,
    claim_json_recoverable_sagas, enqueue_json_projection_task, get_json_migration_run,
    get_json_saga, increment_json_saga_recovery_attempts, latest_json_admin_audit_hash,
    list_json_admin_audit, list_json_migration_ops, list_json_migration_runs, list_json_sagas,
    mark_json_projection_task_completed, mark_json_projection_task_failed,
    mark_json_stale_sagas_indeterminate, new_advisory_lease_row,
    pending_json_projection_task_count, pending_projection_task_metrics,
    projection_dead_letter_groups, record_json_saga, requeue_json_dead_letter_by_source,
    requeue_json_dead_letter_tasks, reset_stale_json_in_progress_tasks, start_json_migration_run,
    summarize_json_sagas, summarize_projection_tasks, update_json_saga_status,
    verify_json_admin_audit_chain,
};
use super::{CanonicalStore, DurabilityToken};

const VECTOR_SYSTEM_DURABILITY_POLL_MS: u64 = 10;
const KV_MEMBERSHIP_MAX_ITEMS: usize = 10_000;

#[derive(Debug, Clone)]
enum VectorSystemClient {
    #[cfg(feature = "pinecone")]
    Pinecone(PineconeHttpClient),
    #[cfg(feature = "weaviate")]
    Weaviate(WeaviateHttpClient),
    #[cfg(feature = "elasticsearch")]
    Elasticsearch(ElasticsearchHttpClient),
}

impl VectorSystemClient {
    fn backend_label(&self) -> &'static str {
        match self {
            #[cfg(feature = "pinecone")]
            Self::Pinecone(_) => "pinecone",
            #[cfg(feature = "weaviate")]
            Self::Weaviate(_) => "weaviate",
            #[cfg(feature = "elasticsearch")]
            Self::Elasticsearch(_) => "elasticsearch",
        }
    }

    async fn request_json(
        &self,
        method: reqwest::Method,
        path: &str,
        body: &JsonValue,
    ) -> Result<JsonValue, tonic::Status> {
        match self {
            #[cfg(feature = "pinecone")]
            Self::Pinecone(client) => client.request_json(method, path, body).await,
            #[cfg(feature = "weaviate")]
            Self::Weaviate(client) => client.request_json(method, path, body).await,
            #[cfg(feature = "elasticsearch")]
            Self::Elasticsearch(client) => client.request_json(method, path, body).await,
        }
    }
}

pub struct VectorSystemCanonicalStore {
    client: VectorSystemClient,
    instance_name: String,
    resource_name: String,
    op_lock: tokio::sync::Mutex<()>,
}

impl VectorSystemCanonicalStore {
    #[cfg(feature = "pinecone")]
    pub(crate) fn new_pinecone(
        client: PineconeHttpClient,
        instance_name: impl Into<String>,
    ) -> Self {
        let instance_name = instance_name.into();
        Self {
            client: VectorSystemClient::Pinecone(client),
            resource_name: format!("udb_system_{}", sanitize_component(&instance_name)),
            instance_name,
            op_lock: tokio::sync::Mutex::new(()),
        }
    }

    #[cfg(feature = "weaviate")]
    pub(crate) fn new_weaviate(
        client: WeaviateHttpClient,
        instance_name: impl Into<String>,
    ) -> Self {
        let instance_name = instance_name.into();
        Self {
            client: VectorSystemClient::Weaviate(client),
            resource_name: format!("UdbSystem{}", pascal_component(&instance_name)),
            instance_name,
            op_lock: tokio::sync::Mutex::new(()),
        }
    }

    #[cfg(feature = "elasticsearch")]
    pub(crate) fn new_elasticsearch(
        client: ElasticsearchHttpClient,
        instance_name: impl Into<String>,
    ) -> Self {
        let instance_name = instance_name.into();
        Self {
            client: VectorSystemClient::Elasticsearch(client),
            resource_name: format!("udb-system-{}", sanitize_component(&instance_name)),
            instance_name,
            op_lock: tokio::sync::Mutex::new(()),
        }
    }

    fn backend_label_static(&self) -> &'static str {
        self.client.backend_label()
    }

    fn record_id(key: &str) -> String {
        let digest = Sha256::digest(key.as_bytes());
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        bytes[6] = (bytes[6] & 0x0f) | 0x50;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes).to_string()
    }

    fn kind_for_key(key: &str) -> &str {
        key.split(':').next().unwrap_or("record")
    }

    fn point_key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.instance_name)
    }

    fn projection_task_key(&self, id: Uuid) -> String {
        self.point_key(&format!("projection_task:{id}"))
    }

    fn saga_key(&self, id: Uuid) -> String {
        self.point_key(&format!("saga:{id}"))
    }

    fn audit_key(&self, id: Uuid) -> String {
        self.point_key(&format!("admin_audit:{id}"))
    }

    fn migration_run_key(&self, id: Uuid) -> String {
        self.point_key(&format!("migration_run:{id}"))
    }

    fn migration_op_key(&self, id: i64) -> String {
        self.point_key(&format!("migration_op:{id}"))
    }

    async fn ensure_resource(&self) -> Result<(), String> {
        match &self.client {
            #[cfg(feature = "pinecone")]
            VectorSystemClient::Pinecone(client) => client.ping().await,
            #[cfg(feature = "weaviate")]
            VectorSystemClient::Weaviate(_) => {
                let path = format!("/v1/schema/{}", self.resource_name);
                match self
                    .client
                    .request_json(reqwest::Method::GET, &path, &JsonValue::Null)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(status) if status.code() == tonic::Code::NotFound => self
                        .client
                        .request_json(
                            reqwest::Method::POST,
                            "/v1/schema",
                            &json!({
                                "class": self.resource_name,
                                "vectorizer": "none",
                                "properties": [
                                    {"name": "record_key", "dataType": ["text"]},
                                    {"name": "record_kind", "dataType": ["text"]},
                                    {"name": "value_json", "dataType": ["text"]},
                                    {"name": "updated_at_ms", "dataType": ["int"]}
                                ]
                            }),
                        )
                        .await
                        .map(|_| ())
                        .map_err(|err| err.to_string()),
                    Err(status) => Err(status.to_string()),
                }
            }
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                let path = format!("/{}", self.resource_name);
                match self
                    .client
                    .request_json(reqwest::Method::GET, &path, &JsonValue::Null)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(status) if status.code() == tonic::Code::NotFound => self
                        .client
                        .request_json(
                            reqwest::Method::PUT,
                            &path,
                            &json!({
                                "mappings": {
                                    "dynamic": true,
                                    "properties": {
                                        "record_key": {"type": "keyword"},
                                        "record_kind": {"type": "keyword"},
                                        "updated_at_ms": {"type": "long"},
                                        "value": {"type": "object", "enabled": false}
                                    }
                                }
                            }),
                        )
                        .await
                        .map(|_| ())
                        .map_err(|err| err.to_string()),
                    Err(status) => Err(status.to_string()),
                }
            }
        }
    }

    async fn get_json<T>(&self, key: &str) -> SystemStoreResult<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let value = match &self.client {
            #[cfg(feature = "pinecone")]
            VectorSystemClient::Pinecone(_) => self.get_pinecone_value(key).await?,
            #[cfg(feature = "weaviate")]
            VectorSystemClient::Weaviate(_) => self.get_weaviate_value(key).await?,
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => self.get_elasticsearch_value(key).await?,
        };
        value
            .map(|value| {
                serde_json::from_value(value).map_err(|err| {
                    SystemStoreError::InvalidInput(format!(
                        "decode {} system JSON {key}: {err}",
                        self.backend_label_static()
                    ))
                })
            })
            .transpose()
    }

    #[cfg(feature = "pinecone")]
    async fn get_pinecone_value(&self, key: &str) -> SystemStoreResult<Option<JsonValue>> {
        let path = format!(
            "/vectors/fetch?ids={}&namespace={}",
            Self::record_id(key),
            self.resource_name
        );
        let response = self
            .client
            .request_json(reqwest::Method::GET, &path, &JsonValue::Null)
            .await
            .map_err(|err| SystemStoreError::query("pinecone", "fetch vector", err))?;
        let Some(raw) = response
            .get("vectors")
            .and_then(|vectors| vectors.get(Self::record_id(key)))
            .and_then(|point| point.get("metadata"))
            .and_then(|metadata| metadata.get("value_json"))
            .and_then(JsonValue::as_str)
        else {
            return Ok(None);
        };
        serde_json::from_str(raw)
            .map(Some)
            .map_err(|err| SystemStoreError::InvalidInput(format!("decode pinecone JSON: {err}")))
    }

    #[cfg(feature = "weaviate")]
    async fn get_weaviate_value(&self, key: &str) -> SystemStoreResult<Option<JsonValue>> {
        let path = format!(
            "/v1/objects/{}/{}",
            self.resource_name,
            Self::record_id(key)
        );
        let response = match self
            .client
            .request_json(reqwest::Method::GET, &path, &JsonValue::Null)
            .await
        {
            Ok(response) => response,
            Err(status) if status.code() == tonic::Code::NotFound => return Ok(None),
            Err(status) => {
                return Err(SystemStoreError::query("weaviate", "get object", status));
            }
        };
        let Some(raw) = response
            .get("properties")
            .and_then(|properties| properties.get("value_json"))
            .and_then(JsonValue::as_str)
        else {
            return Ok(None);
        };
        serde_json::from_str(raw)
            .map(Some)
            .map_err(|err| SystemStoreError::InvalidInput(format!("decode weaviate JSON: {err}")))
    }

    #[cfg(feature = "elasticsearch")]
    async fn get_elasticsearch_value(&self, key: &str) -> SystemStoreResult<Option<JsonValue>> {
        let path = format!("/{}/_doc/{}", self.resource_name, Self::record_id(key));
        let response = match self
            .client
            .request_json(reqwest::Method::GET, &path, &JsonValue::Null)
            .await
        {
            Ok(response) => response,
            Err(status) if status.code() == tonic::Code::NotFound => return Ok(None),
            Err(status) => {
                return Err(SystemStoreError::query(
                    "elasticsearch",
                    "get document",
                    status,
                ));
            }
        };
        Ok(response
            .get("_source")
            .and_then(|source| source.get("value"))
            .cloned())
    }

    async fn set_json<T>(&self, key: &str, value: &T) -> SystemStoreResult<()>
    where
        T: Serialize,
    {
        let value = serde_json::to_value(value).map_err(|err| {
            SystemStoreError::InvalidInput(format!(
                "encode {} system JSON {key}: {err}",
                self.backend_label_static()
            ))
        })?;
        match &self.client {
            #[cfg(feature = "pinecone")]
            VectorSystemClient::Pinecone(_) => self.set_pinecone_value(key, value).await,
            #[cfg(feature = "weaviate")]
            VectorSystemClient::Weaviate(_) => self.set_weaviate_value(key, value).await,
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => self.set_elasticsearch_value(key, value).await,
        }
    }

    #[cfg(feature = "pinecone")]
    async fn set_pinecone_value(&self, key: &str, value: JsonValue) -> SystemStoreResult<()> {
        let value_json = serde_json::to_string(&value).map_err(|err| {
            SystemStoreError::InvalidInput(format!("encode pinecone metadata JSON: {err}"))
        })?;
        self.client
            .request_json(
                reqwest::Method::POST,
                "/vectors/upsert",
                &json!({
                    "namespace": self.resource_name,
                    "vectors": [{
                        "id": Self::record_id(key),
                        "values": [0.0],
                        "metadata": {
                            "record_key": key,
                            "record_kind": Self::kind_for_key(key),
                            "value_json": value_json,
                            "updated_at_ms": Utc::now().timestamp_millis()
                        }
                    }]
                }),
            )
            .await
            .map(|_| ())
            .map_err(|err| SystemStoreError::query("pinecone", "upsert vector", err))
    }

    #[cfg(feature = "weaviate")]
    async fn set_weaviate_value(&self, key: &str, value: JsonValue) -> SystemStoreResult<()> {
        let value_json = serde_json::to_string(&value).map_err(|err| {
            SystemStoreError::InvalidInput(format!("encode weaviate property JSON: {err}"))
        })?;
        let path = format!(
            "/v1/objects/{}/{}",
            self.resource_name,
            Self::record_id(key)
        );
        self.client
            .request_json(
                reqwest::Method::PUT,
                &path,
                &json!({
                    "class": self.resource_name,
                    "id": Self::record_id(key),
                    "properties": {
                        "record_key": key,
                        "record_kind": Self::kind_for_key(key),
                        "value_json": value_json,
                        "updated_at_ms": Utc::now().timestamp_millis()
                    },
                    "vector": [0.0]
                }),
            )
            .await
            .map(|_| ())
            .map_err(|err| SystemStoreError::query("weaviate", "upsert object", err))
    }

    #[cfg(feature = "elasticsearch")]
    async fn set_elasticsearch_value(&self, key: &str, value: JsonValue) -> SystemStoreResult<()> {
        let path = format!(
            "/{}/_doc/{}?refresh=wait_for",
            self.resource_name,
            Self::record_id(key)
        );
        self.client
            .request_json(
                reqwest::Method::PUT,
                &path,
                &json!({
                    "record_key": key,
                    "record_kind": Self::kind_for_key(key),
                    "value": value,
                    "updated_at_ms": Utc::now().timestamp_millis()
                }),
            )
            .await
            .map(|_| ())
            .map_err(|err| SystemStoreError::query("elasticsearch", "index document", err))
    }

    async fn delete_key(&self, key: &str) -> SystemStoreResult<()> {
        match &self.client {
            #[cfg(feature = "pinecone")]
            VectorSystemClient::Pinecone(_) => self.delete_pinecone_key(key).await,
            #[cfg(feature = "weaviate")]
            VectorSystemClient::Weaviate(_) => self.delete_weaviate_key(key).await,
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => self.delete_elasticsearch_key(key).await,
        }
    }

    #[cfg(feature = "pinecone")]
    async fn delete_pinecone_key(&self, key: &str) -> SystemStoreResult<()> {
        self.client
            .request_json(
                reqwest::Method::POST,
                "/vectors/delete",
                &json!({"namespace": self.resource_name, "ids": [Self::record_id(key)]}),
            )
            .await
            .map(|_| ())
            .map_err(|err| SystemStoreError::query("pinecone", "delete vector", err))
    }

    #[cfg(feature = "weaviate")]
    async fn delete_weaviate_key(&self, key: &str) -> SystemStoreResult<()> {
        let path = format!(
            "/v1/objects/{}/{}",
            self.resource_name,
            Self::record_id(key)
        );
        match self
            .client
            .request_json(reqwest::Method::DELETE, &path, &JsonValue::Null)
            .await
        {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
            Err(status) => Err(SystemStoreError::query("weaviate", "delete object", status)),
        }
    }

    #[cfg(feature = "elasticsearch")]
    async fn delete_elasticsearch_key(&self, key: &str) -> SystemStoreResult<()> {
        let path = format!(
            "/{}/_doc/{}?refresh=wait_for",
            self.resource_name,
            Self::record_id(key)
        );
        match self
            .client
            .request_json(reqwest::Method::DELETE, &path, &JsonValue::Null)
            .await
        {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
            Err(status) => Err(SystemStoreError::query(
                "elasticsearch",
                "delete document",
                status,
            )),
        }
    }

    async fn list_set(&self, set_key: &str) -> SystemStoreResult<Vec<String>> {
        Ok(self.get_json(set_key).await?.unwrap_or_default())
    }

    async fn add_to_set(&self, set_key: &str, item: String) -> SystemStoreResult<()> {
        let mut items = self.list_set(set_key).await?;
        if !items.iter().any(|existing| existing == &item) {
            items.push(item);
            self.set_json(set_key, &items).await?;
        }
        Ok(())
    }

    async fn add_to_capped_set(
        &self,
        set_key: &str,
        item: String,
        max_items: usize,
    ) -> SystemStoreResult<Vec<String>> {
        let mut items = self.list_set(set_key).await?;
        if items.iter().any(|existing| existing == &item) {
            return Ok(Vec::new());
        }
        items.push(item);
        let mut trimmed = Vec::new();
        if items.len() > max_items {
            let trim_count = items.len() - max_items;
            trimmed.extend(items.drain(0..trim_count));
            tracing::warn!(
                backend = self.backend_label_static(),
                set_key = %set_key,
                trimmed = trimmed.len(),
                max_items,
                "trimmed canonical KV membership set"
            );
        }
        self.set_json(set_key, &items).await?;
        Ok(trimmed)
    }

    async fn remove_from_set(&self, set_key: &str, item: &str) -> SystemStoreResult<bool> {
        let mut items = self.list_set(set_key).await?;
        let before = items.len();
        items.retain(|existing| existing != item);
        if items.len() != before {
            self.set_json(set_key, &items).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn load_all<T>(&self, set_key: &str) -> SystemStoreResult<Vec<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let keys = self.list_set(set_key).await?;
        let mut rows = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(row) = self.get_json(&key).await? {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    async fn current_seq_value(&self) -> Result<i64, String> {
        self.get_json(&self.point_key("outbox_seq"))
            .await
            .map_err(|err| err.to_string())
            .map(|seq| seq.unwrap_or(0))
    }
}

fn sanitize_component(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out.trim_matches(['_', '-'])
        .chars()
        .collect::<String>()
        .if_empty("default")
}

fn pascal_component(value: &str) -> String {
    let mut out = String::new();
    let mut uppercase_next = true;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            if uppercase_next {
                out.push(ch.to_ascii_uppercase());
                uppercase_next = false;
            } else {
                out.push(ch);
            }
        } else {
            uppercase_next = true;
        }
    }
    if out.is_empty() {
        "Default".to_string()
    } else {
        out
    }
}

trait EmptyStringExt {
    fn if_empty(self, fallback: &str) -> String;
}

impl EmptyStringExt for String {
    fn if_empty(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_string()
        } else {
            self
        }
    }
}

#[async_trait]
impl CanonicalStore for VectorSystemCanonicalStore {
    fn backend_label(&self) -> &'static str {
        self.backend_label_static()
    }

    fn instance_name(&self) -> &str {
        &self.instance_name
    }

    async fn current_durability_token(&self) -> Result<DurabilityToken, String> {
        Ok(DurabilityToken::new(
            self.backend_label_static(),
            self.current_seq_value().await?.to_string(),
        ))
    }

    async fn wait_for_token(
        &self,
        token: &DurabilityToken,
        timeout: Duration,
    ) -> Result<bool, String> {
        if !token.is_for(self.backend_label_static()) {
            return Err(format!(
                "{} VectorSystemCanonicalStore cannot wait on a '{}' token",
                self.backend_label_static(),
                token.backend_label
            ));
        }
        let target: i64 = token.value.parse().map_err(|err| {
            format!(
                "invalid {} durability token '{}': {err}",
                self.backend_label_static(),
                token.value
            )
        })?;
        let started = Instant::now();
        let poll = super::durability_poll_interval(timeout, VECTOR_SYSTEM_DURABILITY_POLL_MS);
        loop {
            if self.current_seq_value().await? >= target {
                return Ok(true);
            }
            if started.elapsed() >= timeout {
                return Ok(false);
            }
            tokio::time::sleep(poll).await;
        }
    }

    async fn enqueue_outbox_event(
        &self,
        event_id: &str,
        topic: &str,
        partition_key: &str,
        payload: &serde_json::Value,
    ) -> Result<i64, String> {
        let _guard = self.op_lock.lock().await;
        let seq = self.current_seq_value().await?.saturating_add(1);
        self.set_json(&self.point_key("outbox_seq"), &seq)
            .await
            .map_err(|err| err.to_string())?;
        let event = super::OutboxEvent {
            event_seq: seq,
            event_id: event_id.to_string(),
            topic: topic.to_string(),
            partition_key: partition_key.to_string(),
            payload: payload.clone(),
            created_at_unix_ms: Utc::now().timestamp_millis(),
        };
        let key = self.point_key(&format!("outbox_event:{seq}"));
        self.set_json(&key, &event)
            .await
            .map_err(|err| err.to_string())?;
        let trimmed = self
            .add_to_capped_set(
                &self.point_key("outbox_events"),
                key,
                KV_MEMBERSHIP_MAX_ITEMS,
            )
            .await
            .map_err(|err| err.to_string())?;
        for old_key in trimmed {
            self.delete_key(&old_key)
                .await
                .map_err(|err| err.to_string())?;
        }
        Ok(seq)
    }

    async fn outbox_max_seq(&self) -> Result<i64, String> {
        self.current_seq_value().await
    }

    async fn ensure_system_tables(&self) -> Result<(), String> {
        self.ensure_resource().await
    }

    async fn ensure_advisory_lease_table(&self) -> Result<(), String> {
        self.ensure_system_tables().await
    }

    async fn try_acquire_advisory_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        ttl: Duration,
    ) -> Result<bool, String> {
        let _guard = self.op_lock.lock().await;
        let key = self.point_key(&format!("lease:{lease_name}"));
        let now = Utc::now().timestamp_millis();
        let current: Option<AdvisoryLeaseRow> =
            self.get_json(&key).await.map_err(|err| err.to_string())?;
        if !advisory_lease_can_acquire(current.as_ref(), owner_id, now) {
            return Ok(false);
        }
        self.set_json(&key, &new_advisory_lease_row(owner_id, now, ttl))
            .await
            .map_err(|err| err.to_string())?;
        Ok(true)
    }

    async fn release_advisory_lease(&self, lease_name: &str, owner_id: &str) -> Result<(), String> {
        let _guard = self.op_lock.lock().await;
        let key = self.point_key(&format!("lease:{lease_name}"));
        let current: Option<AdvisoryLeaseRow> =
            self.get_json(&key).await.map_err(|err| err.to_string())?;
        if advisory_lease_is_owned_by(current.as_ref(), owner_id) {
            self.delete_key(&key).await.map_err(|err| err.to_string())?;
        }
        Ok(())
    }
}

#[async_trait]
impl JsonProjectionTaskAdapter for VectorSystemCanonicalStore {
    fn projection_backend_label(&self) -> &'static str {
        self.backend_label_static()
    }

    fn projection_idem_key(&self, idempotency_key: &str) -> String {
        self.point_key(&format!("projection_idem:{idempotency_key}"))
    }

    fn projection_all_key(&self) -> String {
        self.point_key("projection_all")
    }

    fn projection_task_key(&self, task_id: Uuid) -> String {
        VectorSystemCanonicalStore::projection_task_key(self, task_id)
    }

    async fn get_projection_idem(&self, key: &str) -> SystemStoreResult<Option<String>> {
        self.get_json(key).await
    }

    async fn set_projection_idem(&self, key: &str, value: &String) -> SystemStoreResult<()> {
        self.set_json(key, value).await
    }

    async fn get_projection_row(&self, key: &str) -> SystemStoreResult<Option<ProjectionTaskRow>> {
        self.get_json(key).await
    }

    async fn set_projection_row(
        &self,
        key: &str,
        row: &ProjectionTaskRow,
    ) -> SystemStoreResult<()> {
        self.set_json(key, row).await
    }

    async fn load_projection_rows(
        &self,
        set_key: &str,
    ) -> SystemStoreResult<Vec<ProjectionTaskRow>> {
        self.load_all(set_key).await
    }

    async fn add_projection_row_key(
        &self,
        set_key: &str,
        row_key: String,
        max_items: usize,
    ) -> SystemStoreResult<Vec<String>> {
        self.add_to_capped_set(set_key, row_key, max_items).await
    }

    async fn remove_projection_row_key(
        &self,
        set_key: &str,
        row_key: &str,
    ) -> SystemStoreResult<bool> {
        self.remove_from_set(set_key, row_key).await
    }
}

/// FIX-79: raw typed KV primitives backing the shared saga / admin-audit /
/// migration-audit logic in `system_store.rs`.
#[async_trait]
impl JsonSystemRecordAdapter for VectorSystemCanonicalStore {
    fn record_backend_label(&self) -> &'static str {
        self.backend_label_static()
    }

    fn record_point_key(&self, suffix: &str) -> String {
        self.point_key(suffix)
    }

    fn saga_record_key(&self, saga_id: Uuid) -> String {
        self.saga_key(saga_id)
    }

    fn admin_audit_record_key(&self, audit_id: Uuid) -> String {
        self.audit_key(audit_id)
    }

    fn migration_run_record_key(&self, run_id: Uuid) -> String {
        self.migration_run_key(run_id)
    }

    async fn get_saga_row(&self, key: &str) -> SystemStoreResult<Option<SagaRow>> {
        self.get_json(key).await
    }

    async fn set_saga_row(&self, key: &str, row: &SagaRow) -> SystemStoreResult<()> {
        self.set_json(key, row).await
    }

    async fn load_saga_rows(&self, set_key: &str) -> SystemStoreResult<Vec<SagaRow>> {
        self.load_all(set_key).await
    }

    async fn get_admin_audit_row(&self, key: &str) -> SystemStoreResult<Option<AdminAuditRow>> {
        self.get_json(key).await
    }

    async fn set_admin_audit_row(&self, key: &str, row: &AdminAuditRow) -> SystemStoreResult<()> {
        self.set_json(key, row).await
    }

    async fn get_string_record(&self, key: &str) -> SystemStoreResult<Option<String>> {
        self.get_json(key).await
    }

    async fn set_string_record(&self, key: &str, value: &String) -> SystemStoreResult<()> {
        self.set_json(key, value).await
    }

    async fn get_migration_run_row(
        &self,
        key: &str,
    ) -> SystemStoreResult<Option<MigrationRunRow>> {
        self.get_json(key).await
    }

    async fn set_migration_run_row(
        &self,
        key: &str,
        row: &MigrationRunRow,
    ) -> SystemStoreResult<()> {
        self.set_json(key, row).await
    }

    async fn load_migration_run_rows(
        &self,
        set_key: &str,
    ) -> SystemStoreResult<Vec<MigrationRunRow>> {
        self.load_all(set_key).await
    }

    async fn load_migration_op_rows(
        &self,
        set_key: &str,
    ) -> SystemStoreResult<Vec<MigrationOpRow>> {
        self.load_all(set_key).await
    }

    async fn list_record_set(&self, set_key: &str) -> SystemStoreResult<Vec<String>> {
        self.list_set(set_key).await
    }

    async fn add_record_to_set(&self, set_key: &str, item: String) -> SystemStoreResult<()> {
        self.add_to_set(set_key, item).await
    }
}

#[async_trait]
impl ProjectionTaskStore for VectorSystemCanonicalStore {
    fn backend_label(&self) -> &'static str {
        self.backend_label_static()
    }

    async fn ensure_projection_tables(&self) -> SystemStoreResult<()> {
        self.ensure_system_tables()
            .await
            .map_err(|err| SystemStoreError::io(self.backend_label_static(), err))
    }

    async fn enqueue_projection_task(
        &self,
        task: &ProjectionTaskInsert,
    ) -> SystemStoreResult<Uuid> {
        let _guard = self.op_lock.lock().await;
        enqueue_json_projection_task(self, task, KV_MEMBERSHIP_MAX_ITEMS).await
    }

    async fn claim_projection_tasks(
        &self,
        filter: &ProjectionClaimFilter,
    ) -> SystemStoreResult<Vec<ProjectionTaskRow>> {
        let _guard = self.op_lock.lock().await;
        claim_json_projection_tasks(self, filter).await
    }

    async fn mark_projection_task_completed(&self, task_id: Uuid) -> SystemStoreResult<()> {
        let _guard = self.op_lock.lock().await;
        mark_json_projection_task_completed(self, task_id).await
    }

    async fn mark_projection_task_failed(
        &self,
        task_id: Uuid,
        new_retry_count: i32,
        new_status: ProjectionTaskStatus,
        error: &str,
    ) -> SystemStoreResult<()> {
        let _guard = self.op_lock.lock().await;
        mark_json_projection_task_failed(
            self,
            task_id,
            new_retry_count,
            new_status,
            error,
            ProjectionTaskFailurePolicy::LegacyAllowAnyStatusRemoveCompleted,
        )
        .await
    }

    async fn requeue_dead_letter_tasks(
        &self,
        target_backend: Option<&str>,
    ) -> SystemStoreResult<i64> {
        let _guard = self.op_lock.lock().await;
        requeue_json_dead_letter_tasks(self, target_backend).await
    }

    async fn reset_stale_in_progress_tasks(&self, stale_after: Duration) -> SystemStoreResult<i64> {
        let _guard = self.op_lock.lock().await;
        reset_stale_json_in_progress_tasks(self, stale_after).await
    }

    async fn projection_task_summary(&self) -> SystemStoreResult<ProjectionTaskSummary> {
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.point_key("projection_all")).await?;
        Ok(summarize_projection_tasks(rows))
    }

    async fn pending_task_metrics(&self, limit: i64) -> SystemStoreResult<Vec<PendingTaskMetric>> {
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.point_key("projection_all")).await?;
        Ok(pending_projection_task_metrics(rows, limit, Utc::now()))
    }

    async fn dead_letter_groups(&self, limit: i64) -> SystemStoreResult<Vec<DeadLetterGroup>> {
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.point_key("projection_all")).await?;
        Ok(projection_dead_letter_groups(rows, limit))
    }

    async fn requeue_dead_letter_by_source(
        &self,
        source_table: &str,
        target_backend: &str,
        target_instance: &str,
    ) -> SystemStoreResult<i64> {
        let _guard = self.op_lock.lock().await;
        requeue_json_dead_letter_by_source(self, source_table, target_backend, target_instance)
            .await
    }

    async fn pending_projection_task_count(
        &self,
        idempotency_keys: &[String],
    ) -> SystemStoreResult<i64> {
        pending_json_projection_task_count(self, idempotency_keys).await
    }
}

#[async_trait]
impl SagaStore for VectorSystemCanonicalStore {
    fn backend_label(&self) -> &'static str {
        self.backend_label_static()
    }

    async fn ensure_saga_tables(&self) -> SystemStoreResult<()> {
        self.ensure_system_tables()
            .await
            .map_err(|err| SystemStoreError::io(self.backend_label_static(), err))
    }

    async fn record_saga(&self, saga: &SagaInsert) -> SystemStoreResult<Uuid> {
        let _guard = self.op_lock.lock().await;
        record_json_saga(self, saga).await
    }

    async fn get_saga(&self, saga_id: Uuid) -> SystemStoreResult<Option<SagaRow>> {
        get_json_saga(self, saga_id).await
    }

    async fn list_sagas(&self, filter: &SagaListFilter) -> SystemStoreResult<Vec<SagaRow>> {
        list_json_sagas(self, filter).await
    }

    async fn update_saga_status(
        &self,
        saga_id: Uuid,
        status: SagaStatus,
        compensation_status: CompensationStatus,
    ) -> SystemStoreResult<()> {
        let _guard = self.op_lock.lock().await;
        update_json_saga_status(self, saga_id, status, compensation_status).await
    }

    async fn mark_saga_manual_review(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        self.update_saga_status(
            saga_id,
            SagaStatus::ManualReview,
            CompensationStatus::ManualReview,
        )
        .await
    }

    async fn request_saga_recompensation(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        let _guard = self.op_lock.lock().await;
        let key = self.saga_key(saga_id);
        let Some(mut row) = self.get_json::<SagaRow>(&key).await? else {
            return Ok(());
        };
        if !matches!(
            row.status,
            SagaStatus::FailedCompensation | SagaStatus::ManualReview
        ) {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} is not eligible for recompensation"
            )));
        }
        row.compensation_status = CompensationStatus::RetryRequested;
        row.updated_at = Utc::now();
        self.set_json(&key, &row).await
    }

    async fn increment_recovery_attempts(
        &self,
        saga_id: Uuid,
        error: &str,
    ) -> SystemStoreResult<i64> {
        let _guard = self.op_lock.lock().await;
        increment_json_saga_recovery_attempts(self, saga_id, error).await
    }

    async fn claim_recoverable_sagas(
        &self,
        stale_after: Duration,
        limit: i64,
    ) -> SystemStoreResult<Vec<SagaRow>> {
        claim_json_recoverable_sagas(self, stale_after, limit).await
    }

    async fn mark_stale_in_progress_indeterminate(
        &self,
        stale_after: Duration,
    ) -> SystemStoreResult<i64> {
        let _guard = self.op_lock.lock().await;
        mark_json_stale_sagas_indeterminate(self, stale_after).await
    }

    async fn saga_summary(&self) -> SystemStoreResult<SagaSummary> {
        summarize_json_sagas(self).await
    }
}

#[async_trait]
impl super::system_store::AdminAuditStore for VectorSystemCanonicalStore {
    fn backend_label(&self) -> &'static str {
        self.backend_label_static()
    }

    async fn ensure_admin_audit_tables(&self) -> SystemStoreResult<()> {
        self.ensure_system_tables()
            .await
            .map_err(|err| SystemStoreError::io(self.backend_label_static(), err))
    }

    async fn latest_admin_audit_hash(&self) -> SystemStoreResult<String> {
        latest_json_admin_audit_hash(self).await
    }

    async fn append_admin_audit(&self, entry: &AdminAuditInsert) -> SystemStoreResult<Uuid> {
        append_json_admin_audit(self, entry).await
    }

    async fn list_admin_audit(
        &self,
        filter: &AdminAuditListFilter,
    ) -> SystemStoreResult<Vec<AdminAuditRow>> {
        list_json_admin_audit(self, filter).await
    }

    async fn verify_admin_audit_chain(
        &self,
        limit: Option<i64>,
    ) -> SystemStoreResult<AdminAuditChainReport> {
        verify_json_admin_audit_chain(self, limit).await
    }
}

#[async_trait]
impl MigrationAuditStore for VectorSystemCanonicalStore {
    fn backend_label(&self) -> &'static str {
        self.backend_label_static()
    }

    async fn ensure_migration_audit_tables(&self) -> SystemStoreResult<()> {
        self.ensure_system_tables()
            .await
            .map_err(|err| SystemStoreError::io(self.backend_label_static(), err))
    }

    async fn start_migration_run(&self, run: &MigrationRunInsert) -> SystemStoreResult<Uuid> {
        let _guard = self.op_lock.lock().await;
        start_json_migration_run(self, run).await
    }

    async fn record_migration_op(&self, op: &MigrationOpInsert) -> SystemStoreResult<i64> {
        let _guard = self.op_lock.lock().await;
        let seq_key = self.point_key("migration_op_seq");
        let id: i64 = self.get_json(&seq_key).await?.unwrap_or(0_i64) + 1;
        self.set_json(&seq_key, &id).await?;
        let row = MigrationOpRow {
            id,
            run_id: op.run_id,
            operation_index: op.operation_index,
            backend: op.backend.clone(),
            resource_uri: op.resource_uri.clone(),
            operation_kind: op.operation_kind.clone(),
            status: op.status,
            payload_json: op.payload_json.clone(),
            error: op.error.clone(),
            applied_at: Some(Utc::now()),
        };
        let key = self.migration_op_key(id);
        self.set_json(&key, &row).await?;
        self.add_to_set(
            &self.point_key(&format!("migration_ops:{}", op.run_id)),
            key.clone(),
        )
        .await?;
        self.add_to_set(&self.point_key("migration_ops_all"), key)
            .await?;
        Ok(id)
    }

    async fn finish_migration_run(
        &self,
        run_id: Uuid,
        new_state: MigrationRunState,
        error: &str,
    ) -> SystemStoreResult<()> {
        let _guard = self.op_lock.lock().await;
        let key = self.migration_run_key(run_id);
        let Some(mut row) = self.get_json::<MigrationRunRow>(&key).await? else {
            return Ok(());
        };
        row.state = new_state;
        row.error = error.to_string();
        if new_state.is_terminal() {
            row.finished_at = Some(Utc::now());
        }
        self.set_json(&key, &row).await
    }

    async fn get_migration_run(&self, run_id: Uuid) -> SystemStoreResult<Option<MigrationRunRow>> {
        get_json_migration_run(self, run_id).await
    }

    async fn list_migration_ops(&self, run_id: Uuid) -> SystemStoreResult<Vec<MigrationOpRow>> {
        list_json_migration_ops(self, run_id).await
    }

    async fn list_migration_runs(
        &self,
        filter: &MigrationRunsFilter,
    ) -> SystemStoreResult<Vec<MigrationRunRow>> {
        list_json_migration_runs(self, filter).await
    }
}
