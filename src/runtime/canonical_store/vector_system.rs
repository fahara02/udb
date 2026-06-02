//! Shared canonical `SystemStores` adapter for vector/search REST backends.
//!
//! Pinecone, Weaviate, and Elasticsearch already have native HTTP clients in
//! the runtime. This module gives them a real canonical system-state plane
//! instead of keeping them as projection-only islands. The adapter stores UDB
//! system records in a dedicated backend namespace/index/class per instance,
//! using deterministic IDs derived from logical record keys.

use std::collections::BTreeMap;
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
    AdminAuditChainReport, AdminAuditInsert, AdminAuditListFilter, AdminAuditRow,
    CompensationStatus, DeadLetterGroup, MigrationAuditStore, MigrationOpInsert, MigrationOpRow,
    MigrationRunInsert, MigrationRunRow, MigrationRunState, MigrationRunsFilter, PendingTaskMetric,
    ProjectionClaimFilter, ProjectionTaskInsert, ProjectionTaskRow, ProjectionTaskStatus,
    ProjectionTaskStore, ProjectionTaskSummary, SagaInsert, SagaListFilter, SagaRow, SagaStatus,
    SagaStore, SagaSummary, SystemStoreError, SystemStoreResult, compute_admin_audit_hash,
    verify_admin_audit_chain_step,
};
use super::{CanonicalStore, DurabilityToken};

const VECTOR_SYSTEM_DURABILITY_POLL_MS: u64 = 10;
const AUDIT_LOCK: &str = "admin-audit-chain";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseDoc {
    owner_id: String,
    expires_at_ms: i64,
}

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

    fn apply_saga_filter(row: &SagaRow, filter: &SagaListFilter) -> bool {
        filter
            .tenant_id
            .as_ref()
            .is_none_or(|value| row.tenant_id == *value)
            && filter.status.is_none_or(|value| row.status == value)
            && filter
                .tx_id
                .as_ref()
                .is_none_or(|value| row.tx_id == *value)
            && filter
                .correlation_id
                .as_ref()
                .is_none_or(|value| row.correlation_id == *value)
    }

    fn apply_audit_filter(row: &AdminAuditRow, filter: &AdminAuditListFilter) -> bool {
        filter
            .operation
            .as_ref()
            .is_none_or(|value| row.operation == *value)
            && filter
                .actor
                .as_ref()
                .is_none_or(|value| row.actor == *value)
            && filter
                .tenant_id
                .as_ref()
                .is_none_or(|value| row.tenant_id == *value)
            && filter
                .project_id
                .as_ref()
                .is_none_or(|value| row.project_id == *value)
    }

    fn apply_migration_filter(row: &MigrationRunRow, filter: &MigrationRunsFilter) -> bool {
        filter
            .project_id
            .as_ref()
            .is_none_or(|value| row.project_id == *value)
            && filter.state.is_none_or(|value| row.state == value)
            && filter
                .catalog_version
                .as_ref()
                .is_none_or(|value| row.catalog_version == *value)
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
        self.add_to_set(&self.point_key("outbox_events"), key)
            .await
            .map_err(|err| err.to_string())?;
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
        let current: Option<LeaseDoc> = self.get_json(&key).await.map_err(|err| err.to_string())?;
        let can_acquire = current
            .as_ref()
            .is_none_or(|lease| lease.expires_at_ms <= now || lease.owner_id == owner_id);
        if !can_acquire {
            return Ok(false);
        }
        let ttl_ms = ttl.as_millis().max(1) as i64;
        self.set_json(
            &key,
            &LeaseDoc {
                owner_id: owner_id.to_string(),
                expires_at_ms: now.saturating_add(ttl_ms),
            },
        )
        .await
        .map_err(|err| err.to_string())?;
        Ok(true)
    }

    async fn release_advisory_lease(&self, lease_name: &str, owner_id: &str) -> Result<(), String> {
        let _guard = self.op_lock.lock().await;
        let key = self.point_key(&format!("lease:{lease_name}"));
        let current: Option<LeaseDoc> = self.get_json(&key).await.map_err(|err| err.to_string())?;
        if current
            .as_ref()
            .is_some_and(|lease| lease.owner_id == owner_id)
        {
            self.delete_key(&key).await.map_err(|err| err.to_string())?;
        }
        Ok(())
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
        let idem_key = self.point_key(&format!("projection_idem:{}", task.idempotency_key));
        if let Some(existing) = self.get_json::<String>(&idem_key).await? {
            return Uuid::parse_str(&existing).map_err(|err| {
                SystemStoreError::InvalidInput(format!(
                    "bad {} task id: {err}",
                    self.backend_label_static()
                ))
            });
        }

        let now = Utc::now();
        let task_id = Uuid::new_v4();
        let row = ProjectionTaskRow {
            task_id,
            idempotency_key: task.idempotency_key.clone(),
            project_id: task.project_id.clone(),
            target_backend: task.target_backend.clone(),
            target_instance: task.target_instance.clone(),
            projection_kind: task.projection_kind.clone(),
            resource_name: task.resource_name.clone(),
            operation: task.operation,
            source_row_key: task.source_row_key.clone(),
            target_options: task.target_options.clone(),
            source_payload: task.source_payload.clone(),
            source_checksum: task.source_checksum.clone(),
            status: ProjectionTaskStatus::Pending,
            retry_count: 0,
            last_error: String::new(),
            created_at: now,
            updated_at: now,
            next_retry_at: None,
            completed_at: None,
        };
        let row_key = self.projection_task_key(task_id);
        self.set_json(&idem_key, &task_id.to_string()).await?;
        self.set_json(&row_key, &row).await?;
        self.add_to_set(&self.point_key("projection_all"), row_key)
            .await?;
        Ok(task_id)
    }

    async fn claim_projection_tasks(
        &self,
        filter: &ProjectionClaimFilter,
    ) -> SystemStoreResult<Vec<ProjectionTaskRow>> {
        let _guard = self.op_lock.lock().await;
        let mut rows: Vec<ProjectionTaskRow> =
            self.load_all(&self.point_key("projection_all")).await?;
        rows.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        let mut claimed = Vec::new();
        for mut row in rows {
            if claimed.len() >= filter.batch_size.max(1) as usize {
                break;
            }
            if !row.status.is_claimable()
                || row.retry_count >= filter.max_retries
                || row
                    .next_retry_at
                    .is_some_and(|next_retry_at| next_retry_at > Utc::now())
                || filter
                    .target_backend
                    .as_ref()
                    .is_some_and(|value| row.target_backend != *value)
                || filter
                    .target_instance
                    .as_ref()
                    .is_some_and(|value| row.target_instance != *value)
                || filter
                    .project_id
                    .as_ref()
                    .is_some_and(|value| row.project_id != *value)
            {
                continue;
            }
            row.status = ProjectionTaskStatus::InProgress;
            row.updated_at = Utc::now();
            row.next_retry_at = None;
            self.set_json(&self.projection_task_key(row.task_id), &row)
                .await?;
            claimed.push(row);
        }
        Ok(claimed)
    }

    async fn mark_projection_task_completed(&self, task_id: Uuid) -> SystemStoreResult<()> {
        let _guard = self.op_lock.lock().await;
        let row_key = self.projection_task_key(task_id);
        let Some(mut row) = self.get_json::<ProjectionTaskRow>(&row_key).await? else {
            return Ok(());
        };
        row.status = ProjectionTaskStatus::Completed;
        row.updated_at = Utc::now();
        row.completed_at = Some(row.updated_at);
        row.next_retry_at = None;
        self.set_json(&row_key, &row).await
    }

    async fn mark_projection_task_failed(
        &self,
        task_id: Uuid,
        new_retry_count: i32,
        new_status: ProjectionTaskStatus,
        error: &str,
    ) -> SystemStoreResult<()> {
        let _guard = self.op_lock.lock().await;
        let row_key = self.projection_task_key(task_id);
        let Some(mut row) = self.get_json::<ProjectionTaskRow>(&row_key).await? else {
            return Ok(());
        };
        row.status = new_status;
        row.retry_count = new_retry_count;
        row.last_error = error.to_string();
        row.updated_at = Utc::now();
        row.next_retry_at = (new_status == ProjectionTaskStatus::Failed).then_some(row.updated_at);
        self.set_json(&row_key, &row).await
    }

    async fn requeue_dead_letter_tasks(
        &self,
        target_backend: Option<&str>,
    ) -> SystemStoreResult<i64> {
        let _guard = self.op_lock.lock().await;
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.point_key("projection_all")).await?;
        let mut count = 0;
        for mut row in rows {
            if row.status != ProjectionTaskStatus::DeadLetter
                || target_backend.is_some_and(|backend| row.target_backend != backend)
            {
                continue;
            }
            row.status = ProjectionTaskStatus::Pending;
            row.retry_count = 0;
            row.last_error = "operator requeue".to_string();
            row.updated_at = Utc::now();
            row.next_retry_at = None;
            self.set_json(&self.projection_task_key(row.task_id), &row)
                .await?;
            count += 1;
        }
        Ok(count)
    }

    async fn reset_stale_in_progress_tasks(&self, stale_after: Duration) -> SystemStoreResult<i64> {
        let _guard = self.op_lock.lock().await;
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.point_key("projection_all")).await?;
        let cutoff = Utc::now() - chrono::Duration::from_std(stale_after).unwrap_or_default();
        let mut count = 0;
        for mut row in rows {
            if row.status != ProjectionTaskStatus::InProgress || row.updated_at > cutoff {
                continue;
            }
            row.status = ProjectionTaskStatus::Pending;
            row.last_error = "stale in-progress reconciliation".to_string();
            row.updated_at = Utc::now();
            row.next_retry_at = None;
            self.set_json(&self.projection_task_key(row.task_id), &row)
                .await?;
            count += 1;
        }
        Ok(count)
    }

    async fn projection_task_summary(&self) -> SystemStoreResult<ProjectionTaskSummary> {
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.point_key("projection_all")).await?;
        let mut summary = ProjectionTaskSummary::default();
        for row in rows {
            match row.status {
                ProjectionTaskStatus::Pending => summary.pending += 1,
                ProjectionTaskStatus::InProgress => summary.in_progress += 1,
                ProjectionTaskStatus::Completed => summary.completed += 1,
                ProjectionTaskStatus::Failed => summary.failed += 1,
                ProjectionTaskStatus::DeadLetter => summary.dead_letter += 1,
            }
        }
        Ok(summary)
    }

    async fn pending_task_metrics(&self, limit: i64) -> SystemStoreResult<Vec<PendingTaskMetric>> {
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.point_key("projection_all")).await?;
        let now = Utc::now();
        let mut groups: BTreeMap<(String, String, String, String), (i64, f64)> = BTreeMap::new();
        for row in rows.into_iter().filter(|row| {
            matches!(
                row.status,
                ProjectionTaskStatus::Pending | ProjectionTaskStatus::Failed
            )
        }) {
            let key = (
                row.project_id,
                row.target_backend,
                row.target_instance,
                row.projection_kind,
            );
            let age = (now - row.created_at).num_milliseconds().max(0) as f64 / 1000.0;
            groups
                .entry(key)
                .and_modify(|entry| {
                    entry.0 += 1;
                    entry.1 = entry.1.max(age);
                })
                .or_insert((1, age));
        }
        Ok(groups
            .into_iter()
            .take(limit.max(0) as usize)
            .map(
                |(
                    (project_id, target_backend, target_instance, projection_kind),
                    (pending, oldest_age_seconds),
                )| PendingTaskMetric {
                    project_id,
                    target_backend,
                    target_instance,
                    projection_kind,
                    pending,
                    oldest_age_seconds,
                },
            )
            .collect())
    }

    async fn dead_letter_groups(&self, limit: i64) -> SystemStoreResult<Vec<DeadLetterGroup>> {
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.point_key("projection_all")).await?;
        let mut groups: BTreeMap<(String, String, String), i64> = BTreeMap::new();
        for row in rows
            .into_iter()
            .filter(|row| row.status == ProjectionTaskStatus::DeadLetter)
        {
            *groups
                .entry((row.resource_name, row.target_backend, row.target_instance))
                .or_default() += 1;
        }
        Ok(groups
            .into_iter()
            .take(limit.max(0) as usize)
            .map(
                |((source_table, target_backend, target_instance), dead_count)| DeadLetterGroup {
                    source_table,
                    target_backend,
                    target_instance,
                    dead_count,
                },
            )
            .collect())
    }

    async fn requeue_dead_letter_by_source(
        &self,
        source_table: &str,
        target_backend: &str,
        target_instance: &str,
    ) -> SystemStoreResult<i64> {
        let _guard = self.op_lock.lock().await;
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.point_key("projection_all")).await?;
        let mut count = 0;
        for mut row in rows {
            if row.status != ProjectionTaskStatus::DeadLetter
                || row.resource_name != source_table
                || row.target_backend != target_backend
                || row.target_instance != target_instance
            {
                continue;
            }
            row.status = ProjectionTaskStatus::Pending;
            row.retry_count = 0;
            row.last_error = "reconciliation repair".to_string();
            row.updated_at = Utc::now();
            row.next_retry_at = None;
            self.set_json(&self.projection_task_key(row.task_id), &row)
                .await?;
            count += 1;
        }
        Ok(count)
    }

    async fn pending_projection_task_count(
        &self,
        idempotency_keys: &[String],
    ) -> SystemStoreResult<i64> {
        let mut count = 0;
        for idem in idempotency_keys {
            let idem_key = self.point_key(&format!("projection_idem:{idem}"));
            let Some(task_id) = self
                .get_json::<String>(&idem_key)
                .await?
                .and_then(|id| Uuid::parse_str(&id).ok())
            else {
                continue;
            };
            let Some(row) = self
                .get_json::<ProjectionTaskRow>(&self.projection_task_key(task_id))
                .await?
            else {
                continue;
            };
            if !matches!(
                row.status,
                ProjectionTaskStatus::Completed
                    | ProjectionTaskStatus::DeadLetter
                    | ProjectionTaskStatus::Failed
            ) {
                count += 1;
            }
        }
        Ok(count)
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
        let now = Utc::now();
        let saga_id = Uuid::new_v4();
        let row = SagaRow {
            saga_id,
            tx_id: saga.tx_id.clone(),
            tenant_id: saga.tenant_id.clone(),
            correlation_id: saga.correlation_id.clone(),
            status: saga.status,
            backend_instance: saga.backend_instance.clone(),
            operation: saga.operation.clone(),
            current_step: 0,
            retry_count: 0,
            recovery_attempts: 0,
            compensation_status: CompensationStatus::None,
            steps: saga.steps.clone(),
            compensations: saga.compensations.clone(),
            last_error: String::new(),
            created_at: now,
            updated_at: now,
        };
        let key = self.saga_key(saga_id);
        self.set_json(&key, &row).await?;
        self.add_to_set(&self.point_key("saga_all"), key).await?;
        Ok(saga_id)
    }

    async fn get_saga(&self, saga_id: Uuid) -> SystemStoreResult<Option<SagaRow>> {
        self.get_json(&self.saga_key(saga_id)).await
    }

    async fn list_sagas(&self, filter: &SagaListFilter) -> SystemStoreResult<Vec<SagaRow>> {
        let mut rows: Vec<SagaRow> = self.load_all(&self.point_key("saga_all")).await?;
        rows.retain(|row| Self::apply_saga_filter(row, filter));
        rows.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        Ok(rows
            .into_iter()
            .skip(filter.offset.max(0) as usize)
            .take(filter.limit.max(0) as usize)
            .collect())
    }

    async fn update_saga_status(
        &self,
        saga_id: Uuid,
        status: SagaStatus,
        compensation_status: CompensationStatus,
    ) -> SystemStoreResult<()> {
        let _guard = self.op_lock.lock().await;
        let key = self.saga_key(saga_id);
        let Some(mut row) = self.get_json::<SagaRow>(&key).await? else {
            return Ok(());
        };
        row.status = status;
        row.compensation_status = compensation_status;
        row.updated_at = Utc::now();
        self.set_json(&key, &row).await
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
        let key = self.saga_key(saga_id);
        let Some(mut row) = self.get_json::<SagaRow>(&key).await? else {
            return Ok(0);
        };
        row.recovery_attempts += 1;
        row.last_error = error.to_string();
        row.updated_at = Utc::now();
        let attempts = i64::from(row.recovery_attempts);
        self.set_json(&key, &row).await?;
        Ok(attempts)
    }

    async fn claim_recoverable_sagas(
        &self,
        stale_after: Duration,
        limit: i64,
    ) -> SystemStoreResult<Vec<SagaRow>> {
        let cutoff = Utc::now() - chrono::Duration::from_std(stale_after).unwrap_or_default();
        let mut rows: Vec<SagaRow> = self.load_all(&self.point_key("saga_all")).await?;
        rows.retain(|row| {
            matches!(
                row.status,
                SagaStatus::Indeterminate | SagaStatus::InDoubt | SagaStatus::FailedCompensation
            ) || (row.status == SagaStatus::InProgress && row.updated_at <= cutoff)
        });
        rows.sort_by(|a, b| a.updated_at.cmp(&b.updated_at));
        Ok(rows.into_iter().take(limit.max(0) as usize).collect())
    }

    async fn mark_stale_in_progress_indeterminate(
        &self,
        stale_after: Duration,
    ) -> SystemStoreResult<i64> {
        let _guard = self.op_lock.lock().await;
        let cutoff = Utc::now() - chrono::Duration::from_std(stale_after).unwrap_or_default();
        let rows: Vec<SagaRow> = self.load_all(&self.point_key("saga_all")).await?;
        let mut count = 0;
        for mut row in rows {
            if row.status == SagaStatus::InProgress && row.updated_at <= cutoff {
                row.status = SagaStatus::Indeterminate;
                row.updated_at = Utc::now();
                self.set_json(&self.saga_key(row.saga_id), &row).await?;
                count += 1;
            }
        }
        Ok(count)
    }

    async fn saga_summary(&self) -> SystemStoreResult<SagaSummary> {
        let rows: Vec<SagaRow> = self.load_all(&self.point_key("saga_all")).await?;
        let mut summary = SagaSummary::default();
        for row in rows {
            match row.status {
                SagaStatus::Indeterminate => summary.indeterminate += 1,
                SagaStatus::InProgress => summary.in_progress += 1,
                SagaStatus::Pending => summary.pending += 1,
                SagaStatus::Committed => summary.committed += 1,
                SagaStatus::Compensated => summary.compensated += 1,
                SagaStatus::Failed => summary.failed += 1,
                SagaStatus::InDoubt => summary.in_doubt += 1,
                SagaStatus::FailedCompensation => summary.failed_compensation += 1,
                SagaStatus::ManualReview => summary.manual_review += 1,
            }
        }
        Ok(summary)
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
        Ok(self
            .get_json(&self.point_key("admin_audit_latest_hash"))
            .await?
            .unwrap_or_default())
    }

    async fn append_admin_audit(&self, entry: &AdminAuditInsert) -> SystemStoreResult<Uuid> {
        let owner = Uuid::new_v4().to_string();
        let started = Instant::now();
        while !self
            .try_acquire_advisory_lease(AUDIT_LOCK, &owner, Duration::from_secs(10))
            .await
            .map_err(|err| SystemStoreError::io(self.backend_label_static(), err))?
        {
            if started.elapsed() > Duration::from_secs(10) {
                return Err(SystemStoreError::io(
                    self.backend_label_static(),
                    "admin audit lock timeout",
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let audit_id = Uuid::new_v4();
        let previous_hash = self.latest_admin_audit_hash().await?;
        let current_hash = compute_admin_audit_hash(
            &previous_hash,
            &entry.actor,
            &entry.operation,
            &entry.target,
            &entry.request_json,
            &entry.result,
            &entry.tenant_id,
            &entry.project_id,
            &entry.correlation_id,
            &entry.signer_key_id,
            &entry.external_anchor,
        );
        let row = AdminAuditRow {
            audit_id,
            actor: entry.actor.clone(),
            operation: entry.operation.clone(),
            target: entry.target.clone(),
            request_json: entry.request_json.clone(),
            result: entry.result.clone(),
            tenant_id: entry.tenant_id.clone(),
            project_id: entry.project_id.clone(),
            correlation_id: entry.correlation_id.clone(),
            previous_hash,
            current_hash: current_hash.clone(),
            signer_key_id: entry.signer_key_id.clone(),
            external_anchor: entry.external_anchor.clone(),
            created_at: Utc::now(),
        };
        let key = self.audit_key(audit_id);
        let result = async {
            self.set_json(&key, &row).await?;
            self.add_to_set(&self.point_key("admin_audit_order"), key)
                .await?;
            self.set_json(&self.point_key("admin_audit_latest_hash"), &current_hash)
                .await
        }
        .await;
        let _ = self.release_advisory_lease(AUDIT_LOCK, &owner).await;
        result?;
        Ok(audit_id)
    }

    async fn list_admin_audit(
        &self,
        filter: &AdminAuditListFilter,
    ) -> SystemStoreResult<Vec<AdminAuditRow>> {
        let keys = self.list_set(&self.point_key("admin_audit_order")).await?;
        let mut rows = Vec::new();
        for key in keys {
            if let Some(mut row) = self.get_json::<AdminAuditRow>(&key).await?
                && Self::apply_audit_filter(&row, filter)
            {
                if filter.redact_request_json {
                    row.request_json = serde_json::json!({"redacted": true});
                }
                rows.push(row);
            }
        }
        rows.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(rows
            .into_iter()
            .skip(filter.offset.max(0) as usize)
            .take(filter.limit.max(0) as usize)
            .collect())
    }

    async fn verify_admin_audit_chain(
        &self,
        limit: Option<i64>,
    ) -> SystemStoreResult<AdminAuditChainReport> {
        let mut keys = self.list_set(&self.point_key("admin_audit_order")).await?;
        if let Some(limit) = limit
            && limit >= 0
        {
            keys.truncate(limit as usize);
        }
        let mut checked = 0_i64;
        let mut previous = String::new();
        for key in keys {
            let Some(row) = self.get_json::<AdminAuditRow>(&key).await? else {
                continue;
            };
            match verify_admin_audit_chain_step(&row, &previous, checked) {
                Ok(next) => {
                    previous = next;
                    checked += 1;
                }
                Err(report) => return Ok(report),
            }
        }
        Ok(AdminAuditChainReport::Passed {
            checked_count: checked,
            last_hash: previous,
        })
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
        let run_id = Uuid::new_v4();
        let row = MigrationRunRow {
            run_id,
            project_id: run.project_id.clone(),
            catalog_version: run.catalog_version.clone(),
            state: run.state,
            operations_hash: run.operations_hash.clone(),
            approval_token: run.approval_token.clone(),
            started_at: Utc::now(),
            finished_at: None,
            error: String::new(),
        };
        let key = self.migration_run_key(run_id);
        self.set_json(&key, &row).await?;
        self.add_to_set(&self.point_key("migration_runs"), key)
            .await?;
        Ok(run_id)
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
            rollback_json: op.rollback_json.clone(),
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
        self.get_json(&self.migration_run_key(run_id)).await
    }

    async fn list_migration_ops(&self, run_id: Uuid) -> SystemStoreResult<Vec<MigrationOpRow>> {
        let mut rows: Vec<MigrationOpRow> = self
            .load_all(&self.point_key(&format!("migration_ops:{run_id}")))
            .await?;
        rows.sort_by_key(|row| row.operation_index);
        Ok(rows)
    }

    async fn list_migration_runs(
        &self,
        filter: &MigrationRunsFilter,
    ) -> SystemStoreResult<Vec<MigrationRunRow>> {
        let mut rows: Vec<MigrationRunRow> =
            self.load_all(&self.point_key("migration_runs")).await?;
        rows.retain(|row| Self::apply_migration_filter(row, filter));
        rows.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        Ok(rows
            .into_iter()
            .skip(filter.offset.max(0) as usize)
            .take(filter.limit.max(0) as usize)
            .collect())
    }
}
