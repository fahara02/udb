//! Shared gated canonical `SystemStores` adapter for vector/search REST backends.
//!
//! Pinecone, Weaviate, and Elasticsearch already have native HTTP clients in
//! the runtime. This module stores UDB system records in a dedicated backend
//! namespace/index/class per instance, using deterministic IDs derived from
//! logical record keys. Only Elasticsearch currently has a backend-native
//! conditional-write path (`_seq_no`/`_primary_term`) for the HA-sensitive
//! SystemStores operations; Pinecone and Weaviate fail closed from SystemStores
//! registration until their own conditional-write CAS path exists.

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
    CompensationStatus, DeadLetterGroup, JSON_ADMIN_AUDIT_LOCK, JsonProjectionTaskAdapter,
    JsonSystemRecordAdapter, MigrationAuditStore, MigrationOpInsert, MigrationOpRow,
    MigrationRunInsert, MigrationRunRow, MigrationRunState, MigrationRunsFilter, OpLedgerStatus,
    PendingTaskMetric, ProjectionClaimFilter, ProjectionTaskFailurePolicy, ProjectionTaskInsert,
    ProjectionTaskRow, ProjectionTaskStatus, ProjectionTaskStore, ProjectionTaskSummary,
    SagaInsert, SagaListFilter, SagaRow, SagaStatus, SagaStore, SagaSummary, SystemStoreError,
    SystemStoreResult, advisory_lease_can_acquire, advisory_lease_is_owned_by,
    append_json_admin_audit, claim_json_projection_tasks, claim_json_recoverable_sagas,
    compute_admin_audit_hash, enqueue_json_projection_task, get_json_migration_run, get_json_saga,
    increment_json_saga_recovery_attempts, latest_json_admin_audit_hash, list_json_admin_audit,
    list_json_migration_ops, list_json_migration_runs, list_json_sagas,
    mark_json_projection_task_completed, mark_json_projection_task_failed,
    mark_json_stale_sagas_indeterminate, mark_projection_task_claimed, new_advisory_lease_row,
    pending_json_projection_task_count, pending_projection_task_metrics,
    projection_dead_letter_groups, projection_task_matches_claim, record_json_saga,
    request_json_saga_recompensation, requeue_json_dead_letter_by_source,
    requeue_json_dead_letter_tasks, reset_stale_json_in_progress_tasks, start_json_migration_run,
    summarize_json_sagas, summarize_projection_tasks, update_json_saga_status,
    verify_admin_audit_chain_step, verify_json_admin_audit_chain,
};
use super::{CanonicalStore, DurabilityToken};

const VECTOR_SYSTEM_DURABILITY_POLL_MS: u64 = 10;
const KV_MEMBERSHIP_MAX_ITEMS: usize = 10_000;
const VECTOR_CAS_MAX_RETRIES: usize = 8;

#[derive(Debug, Clone)]
enum VectorSystemClient {
    #[cfg(feature = "pinecone")]
    Pinecone(PineconeHttpClient),
    #[cfg(feature = "weaviate")]
    Weaviate(WeaviateHttpClient),
    #[cfg(feature = "elasticsearch")]
    Elasticsearch(ElasticsearchHttpClient),
}

#[cfg(feature = "elasticsearch")]
#[derive(Debug, Clone)]
struct ElasticsearchSystemDoc {
    value: Option<JsonValue>,
    seq_no: i64,
    primary_term: i64,
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

    fn cas_unsupported(&self, op: &str) -> String {
        format!(
            "{} canonical SystemStores require real multi-process CAS for {op}; \
             this backend path is not HA-canonical until a backend-native conditional write is wired",
            self.backend_label_static()
        )
    }

    async fn ensure_cas_capable(&self) -> Result<(), String> {
        self.ensure_resource().await?;
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => Ok(()),
            #[allow(unreachable_patterns)]
            _ => Err(self.cas_unsupported("system state")),
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
        Ok(self
            .get_elasticsearch_doc(key)
            .await?
            .and_then(|doc| doc.value))
    }

    #[cfg(feature = "elasticsearch")]
    async fn get_elasticsearch_doc(
        &self,
        key: &str,
    ) -> SystemStoreResult<Option<ElasticsearchSystemDoc>> {
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
        Ok(Some(ElasticsearchSystemDoc {
            value: response
                .get("_source")
                .and_then(|source| source.get("value"))
                .cloned(),
            seq_no: response
                .get("_seq_no")
                .and_then(JsonValue::as_i64)
                .ok_or_else(|| {
                    SystemStoreError::InvalidInput(format!(
                        "elasticsearch system document {key} missing _seq_no for CAS"
                    ))
                })?,
            primary_term: response
                .get("_primary_term")
                .and_then(JsonValue::as_i64)
                .ok_or_else(|| {
                    SystemStoreError::InvalidInput(format!(
                        "elasticsearch system document {key} missing _primary_term for CAS"
                    ))
                })?,
        }))
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
        self.put_elasticsearch_value(key, value, None)
            .await
            .map(|_| ())
    }

    #[cfg(feature = "elasticsearch")]
    fn elasticsearch_system_doc_body(&self, key: &str, value: JsonValue) -> JsonValue {
        json!({
            "record_key": key,
            "record_kind": Self::kind_for_key(key),
            "value": value,
            "updated_at_ms": Utc::now().timestamp_millis()
        })
    }

    #[cfg(feature = "elasticsearch")]
    async fn put_elasticsearch_value(
        &self,
        key: &str,
        value: JsonValue,
        cas: Option<(i64, i64)>,
    ) -> SystemStoreResult<bool> {
        let record_id = Self::record_id(key);
        let path = if let Some((seq_no, primary_term)) = cas {
            format!(
                "/{}/_doc/{}?if_seq_no={seq_no}&if_primary_term={primary_term}&refresh=wait_for",
                self.resource_name, record_id
            )
        } else {
            format!(
                "/{}/_doc/{}?refresh=wait_for",
                self.resource_name, record_id
            )
        };
        match self
            .client
            .request_json(
                reqwest::Method::PUT,
                &path,
                &self.elasticsearch_system_doc_body(key, value),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(status) if status.code() == tonic::Code::AlreadyExists => Ok(false),
            Err(status) => Err(SystemStoreError::query(
                "elasticsearch",
                "index document with CAS",
                status,
            )),
        }
    }

    #[cfg(feature = "elasticsearch")]
    async fn create_elasticsearch_value(
        &self,
        key: &str,
        value: JsonValue,
    ) -> SystemStoreResult<bool> {
        let path = format!(
            "/{}/_create/{}?refresh=wait_for",
            self.resource_name,
            Self::record_id(key)
        );
        match self
            .client
            .request_json(
                reqwest::Method::PUT,
                &path,
                &self.elasticsearch_system_doc_body(key, value),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(status) if status.code() == tonic::Code::AlreadyExists => Ok(false),
            Err(status) => Err(SystemStoreError::query(
                "elasticsearch",
                "create document",
                status,
            )),
        }
    }

    #[cfg(feature = "elasticsearch")]
    async fn compare_and_set_elasticsearch_value(
        &self,
        key: &str,
        expected: Option<(i64, i64)>,
        value: JsonValue,
    ) -> SystemStoreResult<bool> {
        match expected {
            Some(cas) => self.put_elasticsearch_value(key, value, Some(cas)).await,
            None => self.create_elasticsearch_value(key, value).await,
        }
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
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                return self.add_to_elasticsearch_set(set_key, item).await;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

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
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                return self
                    .add_to_elasticsearch_capped_set(set_key, item, max_items)
                    .await;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

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
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                return self.remove_from_elasticsearch_set(set_key, item).await;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

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

    #[cfg(feature = "elasticsearch")]
    fn decode_elasticsearch_set(
        &self,
        set_key: &str,
        doc: Option<&ElasticsearchSystemDoc>,
    ) -> SystemStoreResult<Vec<String>> {
        doc.and_then(|doc| doc.value.as_ref())
            .cloned()
            .map(serde_json::from_value::<Vec<String>>)
            .transpose()
            .map_err(|err| {
                SystemStoreError::InvalidInput(format!(
                    "decode elasticsearch membership set {set_key}: {err}"
                ))
            })
            .map(|items| items.unwrap_or_default())
    }

    #[cfg(feature = "elasticsearch")]
    async fn compare_and_set_elasticsearch_set(
        &self,
        set_key: &str,
        current: Option<&ElasticsearchSystemDoc>,
        items: Vec<String>,
    ) -> SystemStoreResult<bool> {
        let expected = current.map(|doc| (doc.seq_no, doc.primary_term));
        let value = serde_json::to_value(items).map_err(|err| {
            SystemStoreError::InvalidInput(format!(
                "encode elasticsearch membership set {set_key}: {err}"
            ))
        })?;
        self.compare_and_set_elasticsearch_value(set_key, expected, value)
            .await
    }

    #[cfg(feature = "elasticsearch")]
    fn elasticsearch_membership_cas_lost(&self, set_key: &str) -> SystemStoreError {
        SystemStoreError::io(
            "elasticsearch",
            format!("membership set {set_key} CAS lost after {VECTOR_CAS_MAX_RETRIES} retries"),
        )
    }

    #[cfg(feature = "elasticsearch")]
    async fn add_to_elasticsearch_set(&self, set_key: &str, item: String) -> SystemStoreResult<()> {
        for _ in 0..VECTOR_CAS_MAX_RETRIES {
            let current = self.get_elasticsearch_doc(set_key).await?;
            let mut items = self.decode_elasticsearch_set(set_key, current.as_ref())?;
            if items.iter().any(|existing| existing == &item) {
                return Ok(());
            }
            items.push(item.clone());
            if self
                .compare_and_set_elasticsearch_set(set_key, current.as_ref(), items)
                .await?
            {
                return Ok(());
            }
        }
        Err(self.elasticsearch_membership_cas_lost(set_key))
    }

    #[cfg(feature = "elasticsearch")]
    async fn add_to_elasticsearch_capped_set(
        &self,
        set_key: &str,
        item: String,
        max_items: usize,
    ) -> SystemStoreResult<Vec<String>> {
        for _ in 0..VECTOR_CAS_MAX_RETRIES {
            let current = self.get_elasticsearch_doc(set_key).await?;
            let mut items = self.decode_elasticsearch_set(set_key, current.as_ref())?;
            if items.iter().any(|existing| existing == &item) {
                return Ok(Vec::new());
            }
            items.push(item.clone());
            let mut trimmed = Vec::new();
            if items.len() > max_items {
                let trim_count = items.len() - max_items;
                trimmed.extend(items.drain(0..trim_count));
            }
            if self
                .compare_and_set_elasticsearch_set(set_key, current.as_ref(), items)
                .await?
            {
                if !trimmed.is_empty() {
                    tracing::warn!(
                        backend = self.backend_label_static(),
                        set_key = %set_key,
                        trimmed = trimmed.len(),
                        max_items,
                        "trimmed canonical KV membership set"
                    );
                }
                return Ok(trimmed);
            }
        }
        Err(self.elasticsearch_membership_cas_lost(set_key))
    }

    #[cfg(feature = "elasticsearch")]
    async fn remove_from_elasticsearch_set(
        &self,
        set_key: &str,
        item: &str,
    ) -> SystemStoreResult<bool> {
        for _ in 0..VECTOR_CAS_MAX_RETRIES {
            let current = self.get_elasticsearch_doc(set_key).await?;
            let mut items = self.decode_elasticsearch_set(set_key, current.as_ref())?;
            let before = items.len();
            items.retain(|existing| existing != item);
            if items.len() == before {
                return Ok(false);
            }
            if self
                .compare_and_set_elasticsearch_set(set_key, current.as_ref(), items)
                .await?
            {
                return Ok(true);
            }
        }
        Err(self.elasticsearch_membership_cas_lost(set_key))
    }

    #[cfg(feature = "elasticsearch")]
    async fn get_elasticsearch_typed_record<T>(
        &self,
        key: &str,
        label: &str,
    ) -> SystemStoreResult<Option<(T, ElasticsearchSystemDoc)>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let Some(doc) = self.get_elasticsearch_doc(key).await? else {
            return Ok(None);
        };
        let Some(value) = doc.value.clone() else {
            return Err(SystemStoreError::InvalidInput(format!(
                "elasticsearch {label} record {key} missing value"
            )));
        };
        let row = serde_json::from_value(value).map_err(|err| {
            SystemStoreError::InvalidInput(format!(
                "decode elasticsearch {label} record {key}: {err}"
            ))
        })?;
        Ok(Some((row, doc)))
    }

    #[cfg(feature = "elasticsearch")]
    async fn compare_and_set_elasticsearch_record<T>(
        &self,
        key: &str,
        current: &ElasticsearchSystemDoc,
        row: &T,
        label: &str,
    ) -> SystemStoreResult<bool>
    where
        T: Serialize,
    {
        let value = serde_json::to_value(row).map_err(|err| {
            SystemStoreError::InvalidInput(format!(
                "encode elasticsearch {label} record {key}: {err}"
            ))
        })?;
        self.compare_and_set_elasticsearch_value(
            key,
            Some((current.seq_no, current.primary_term)),
            value,
        )
        .await
    }

    #[cfg(feature = "elasticsearch")]
    fn elasticsearch_record_cas_lost(&self, label: &str, key: &str) -> SystemStoreError {
        SystemStoreError::io(
            "elasticsearch",
            format!("{label} record {key} CAS lost after {VECTOR_CAS_MAX_RETRIES} retries"),
        )
    }

    #[cfg(feature = "elasticsearch")]
    async fn update_elasticsearch_saga_row<R, F>(
        &self,
        saga_id: Uuid,
        label: &str,
        mut mutate: F,
    ) -> SystemStoreResult<Option<R>>
    where
        F: FnMut(&mut SagaRow) -> SystemStoreResult<R>,
    {
        let key = self.saga_key(saga_id);
        for _ in 0..VECTOR_CAS_MAX_RETRIES {
            let Some((mut row, current)) = self
                .get_elasticsearch_typed_record::<SagaRow>(&key, label)
                .await?
            else {
                return Ok(None);
            };
            let outcome = mutate(&mut row)?;
            if self
                .compare_and_set_elasticsearch_record(&key, &current, &row, label)
                .await?
            {
                return Ok(Some(outcome));
            }
        }
        Err(self.elasticsearch_record_cas_lost(label, &key))
    }

    #[cfg(feature = "elasticsearch")]
    async fn update_elasticsearch_saga_status(
        &self,
        saga_id: Uuid,
        status: SagaStatus,
        compensation_status: CompensationStatus,
    ) -> SystemStoreResult<()> {
        let _ = self
            .update_elasticsearch_saga_row(saga_id, "saga", |row| {
                row.status = status;
                row.compensation_status = compensation_status;
                row.updated_at = Utc::now();
                Ok(())
            })
            .await?;
        Ok(())
    }

    #[cfg(feature = "elasticsearch")]
    async fn request_elasticsearch_saga_recompensation(
        &self,
        saga_id: Uuid,
    ) -> SystemStoreResult<()> {
        let _ = self
            .update_elasticsearch_saga_row(saga_id, "saga recompensation", |row| {
                if !matches!(
                    row.status,
                    SagaStatus::FailedCompensation | SagaStatus::ManualReview
                ) {
                    return Err(SystemStoreError::InvalidInput(format!(
                        "saga {saga_id} is not in a retryable state (must be failed_compensation or manual_review)"
                    )));
                }
                row.status = SagaStatus::Indeterminate;
                row.last_error = String::new();
                row.compensation_status = CompensationStatus::RetryRequested;
                row.updated_at = Utc::now();
                Ok(())
            })
            .await?;
        Ok(())
    }

    #[cfg(feature = "elasticsearch")]
    async fn increment_elasticsearch_saga_recovery_attempts(
        &self,
        saga_id: Uuid,
        error: &str,
    ) -> SystemStoreResult<i64> {
        Ok(self
            .update_elasticsearch_saga_row(saga_id, "saga recovery attempts", |row| {
                row.recovery_attempts += 1;
                row.last_error = error.to_string();
                row.updated_at = Utc::now();
                Ok(i64::from(row.recovery_attempts))
            })
            .await?
            .unwrap_or(0))
    }

    #[cfg(feature = "elasticsearch")]
    async fn mark_stale_elasticsearch_sagas_indeterminate(
        &self,
        stale_after: Duration,
    ) -> SystemStoreResult<i64> {
        let cutoff = Utc::now() - chrono::Duration::from_std(stale_after).unwrap_or_default();
        let rows: Vec<SagaRow> = self.load_all(&self.point_key("saga_all")).await?;
        let mut count = 0_i64;
        for row in rows {
            if row.status != SagaStatus::InProgress || row.updated_at > cutoff {
                continue;
            }
            count += self
                .update_elasticsearch_saga_row(row.saga_id, "stale saga", |current| {
                    if current.status == SagaStatus::InProgress && current.updated_at <= cutoff {
                        current.status = SagaStatus::Indeterminate;
                        current.updated_at = Utc::now();
                        Ok(1_i64)
                    } else {
                        Ok(0_i64)
                    }
                })
                .await?
                .unwrap_or(0);
        }
        Ok(count)
    }

    #[cfg(feature = "elasticsearch")]
    async fn update_elasticsearch_projection_row<R, F>(
        &self,
        task_id: Uuid,
        label: &str,
        mut mutate: F,
    ) -> SystemStoreResult<Option<R>>
    where
        F: FnMut(&mut ProjectionTaskRow) -> SystemStoreResult<R>,
    {
        let key = self.projection_task_key(task_id);
        for _ in 0..VECTOR_CAS_MAX_RETRIES {
            let Some((mut row, current)) = self
                .get_elasticsearch_typed_record::<ProjectionTaskRow>(&key, label)
                .await?
            else {
                return Ok(None);
            };
            let outcome = mutate(&mut row)?;
            if self
                .compare_and_set_elasticsearch_record(&key, &current, &row, label)
                .await?
            {
                return Ok(Some(outcome));
            }
        }
        Err(self.elasticsearch_record_cas_lost(label, &key))
    }

    #[cfg(feature = "elasticsearch")]
    async fn claim_elasticsearch_projection_tasks(
        &self,
        filter: &ProjectionClaimFilter,
    ) -> SystemStoreResult<Vec<ProjectionTaskRow>> {
        let mut rows: Vec<ProjectionTaskRow> =
            self.load_all(&self.point_key("projection_all")).await?;
        rows.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        let mut claimed = Vec::new();
        for row in rows {
            if claimed.len() >= filter.batch_size.max(1) as usize {
                break;
            }
            if !projection_task_matches_claim(&row, filter, Utc::now()) {
                continue;
            }
            if let Some(Some(claimed_row)) = self
                .update_elasticsearch_projection_row(row.task_id, "projection claim", |current| {
                    if !projection_task_matches_claim(current, filter, Utc::now()) {
                        return Ok(None);
                    }
                    mark_projection_task_claimed(current, Utc::now());
                    Ok(Some(current.clone()))
                })
                .await?
            {
                claimed.push(claimed_row);
            }
        }
        Ok(claimed)
    }

    #[cfg(feature = "elasticsearch")]
    async fn mark_elasticsearch_projection_task_completed(
        &self,
        task_id: Uuid,
    ) -> SystemStoreResult<()> {
        // Unlike the SQL/Mongo stores — whose `projection_task_summary` aggregates
        // the full backing table/collection — the vector store's summary counts
        // over the `projection_all` membership set (the ES `value` payload is a
        // non-indexed object, so status can't be server-side aggregated). The
        // completed row must therefore STAY in `projection_all` so the summary can
        // count it; the claim path already skips terminal rows via
        // `projection_task_matches_claim`, and the set is capped/trimmed. Removing
        // it here (as the shared JSON path does for query-backed stores) would keep
        // `summary.completed` stuck at 0 on this backend.
        self.update_elasticsearch_projection_row(task_id, "projection complete", |row| {
            row.status = ProjectionTaskStatus::Completed;
            row.updated_at = Utc::now();
            row.completed_at = Some(row.updated_at);
            row.next_retry_at = None;
            Ok(())
        })
        .await?;
        Ok(())
    }

    #[cfg(feature = "elasticsearch")]
    async fn mark_elasticsearch_projection_task_failed(
        &self,
        task_id: Uuid,
        new_retry_count: i32,
        new_status: ProjectionTaskStatus,
        error: &str,
    ) -> SystemStoreResult<()> {
        // Same strict target-status guard the shared JSON path enforces: a
        // failure transition may only land in FAILED or DEAD_LETTER (never back
        // to Pending/InProgress/Completed), so an invalid caller status is
        // rejected before any write.
        if !matches!(
            new_status,
            ProjectionTaskStatus::Failed | ProjectionTaskStatus::DeadLetter
        ) {
            return Err(SystemStoreError::InvalidInput(format!(
                "mark_projection_task_failed: status must be FAILED or DEAD_LETTER, got {new_status:?}"
            )));
        }
        let updated = self
            .update_elasticsearch_projection_row(task_id, "projection failure", |row| {
                row.status = new_status;
                row.retry_count = new_retry_count;
                row.last_error = error.to_string();
                row.updated_at = Utc::now();
                row.next_retry_at =
                    (new_status == ProjectionTaskStatus::Failed).then_some(row.updated_at);
                Ok(())
            })
            .await?
            .is_some();
        if updated && new_status == ProjectionTaskStatus::Completed {
            let row_key = self.projection_task_key(task_id);
            let _ = self
                .remove_from_elasticsearch_set(&self.projection_all_key(), &row_key)
                .await?;
        }
        Ok(())
    }

    #[cfg(feature = "elasticsearch")]
    async fn requeue_elasticsearch_dead_letter_tasks(
        &self,
        target_backend: Option<&str>,
    ) -> SystemStoreResult<i64> {
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.point_key("projection_all")).await?;
        let mut count = 0_i64;
        for row in rows {
            if row.status != ProjectionTaskStatus::DeadLetter
                || target_backend.is_some_and(|backend| row.target_backend != backend)
            {
                continue;
            }
            count += self
                .update_elasticsearch_projection_row(row.task_id, "projection requeue", |current| {
                    if current.status != ProjectionTaskStatus::DeadLetter
                        || target_backend.is_some_and(|backend| current.target_backend != backend)
                    {
                        return Ok(0_i64);
                    }
                    current.status = ProjectionTaskStatus::Pending;
                    current.retry_count = 0;
                    current.last_error = "operator requeue".to_string();
                    current.updated_at = Utc::now();
                    current.next_retry_at = None;
                    Ok(1_i64)
                })
                .await?
                .unwrap_or(0);
        }
        Ok(count)
    }

    #[cfg(feature = "elasticsearch")]
    async fn reset_stale_elasticsearch_projection_tasks(
        &self,
        stale_after: Duration,
    ) -> SystemStoreResult<i64> {
        let cutoff = Utc::now() - chrono::Duration::from_std(stale_after).unwrap_or_default();
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.point_key("projection_all")).await?;
        let mut count = 0_i64;
        for row in rows {
            if row.status != ProjectionTaskStatus::InProgress || row.updated_at > cutoff {
                continue;
            }
            count += self
                .update_elasticsearch_projection_row(
                    row.task_id,
                    "projection stale reset",
                    |current| {
                        if current.status != ProjectionTaskStatus::InProgress
                            || current.updated_at > cutoff
                        {
                            return Ok(0_i64);
                        }
                        current.status = ProjectionTaskStatus::Pending;
                        current.last_error = "stale in-progress reconciliation".to_string();
                        current.updated_at = Utc::now();
                        current.next_retry_at = None;
                        Ok(1_i64)
                    },
                )
                .await?
                .unwrap_or(0);
        }
        Ok(count)
    }

    #[cfg(feature = "elasticsearch")]
    async fn requeue_elasticsearch_dead_letter_by_source(
        &self,
        source_table: &str,
        target_backend: &str,
        target_instance: &str,
    ) -> SystemStoreResult<i64> {
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.point_key("projection_all")).await?;
        let mut count = 0_i64;
        for row in rows {
            if row.status != ProjectionTaskStatus::DeadLetter
                || row.resource_name != source_table
                || row.target_backend != target_backend
                || row.target_instance != target_instance
            {
                continue;
            }
            count += self
                .update_elasticsearch_projection_row(
                    row.task_id,
                    "projection source requeue",
                    |current| {
                        if current.status != ProjectionTaskStatus::DeadLetter
                            || current.resource_name != source_table
                            || current.target_backend != target_backend
                            || current.target_instance != target_instance
                        {
                            return Ok(0_i64);
                        }
                        current.status = ProjectionTaskStatus::Pending;
                        current.retry_count = 0;
                        current.last_error = "reconciliation repair".to_string();
                        current.updated_at = Utc::now();
                        current.next_retry_at = None;
                        Ok(1_i64)
                    },
                )
                .await?
                .unwrap_or(0);
        }
        Ok(count)
    }

    #[cfg(feature = "elasticsearch")]
    async fn update_elasticsearch_migration_run_row<R, F>(
        &self,
        run_id: Uuid,
        label: &str,
        mut mutate: F,
    ) -> SystemStoreResult<Option<R>>
    where
        F: FnMut(&mut MigrationRunRow) -> SystemStoreResult<R>,
    {
        let key = self.migration_run_key(run_id);
        for _ in 0..VECTOR_CAS_MAX_RETRIES {
            let Some((mut row, current)) = self
                .get_elasticsearch_typed_record::<MigrationRunRow>(&key, label)
                .await?
            else {
                return Ok(None);
            };
            let outcome = mutate(&mut row)?;
            if self
                .compare_and_set_elasticsearch_record(&key, &current, &row, label)
                .await?
            {
                return Ok(Some(outcome));
            }
        }
        Err(self.elasticsearch_record_cas_lost(label, &key))
    }

    #[cfg(feature = "elasticsearch")]
    async fn finish_elasticsearch_migration_run(
        &self,
        run_id: Uuid,
        new_state: MigrationRunState,
        error: &str,
    ) -> SystemStoreResult<()> {
        let error = error.to_string();
        // Finishing a non-existent run is an error (mirrors PG's rows_affected==0
        // and the Qdrant store) — `update_elasticsearch_migration_run_row` returns
        // None when the run key is absent, which must NOT be silently swallowed.
        let updated = self
            .update_elasticsearch_migration_run_row(run_id, "migration run finish", |row| {
                row.state = new_state;
                row.error = error.clone();
                if new_state.is_terminal() {
                    row.finished_at = Some(Utc::now());
                }
                Ok(())
            })
            .await?;
        if updated.is_none() {
            return Err(SystemStoreError::InvalidInput(format!(
                "migration run {run_id} not found"
            )));
        }
        Ok(())
    }

    #[cfg(feature = "elasticsearch")]
    async fn latest_elasticsearch_admin_audit_hash_from_order(&self) -> SystemStoreResult<String> {
        let keys = self.list_set(&self.point_key("admin_audit_order")).await?;
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
                Err(report) => {
                    return Err(SystemStoreError::InvalidInput(format!(
                        "admin audit chain is broken before append: {report:?}"
                    )));
                }
            }
        }
        Ok(previous)
    }

    #[cfg(feature = "elasticsearch")]
    async fn append_elasticsearch_admin_audit(
        &self,
        entry: &AdminAuditInsert,
    ) -> SystemStoreResult<Uuid> {
        let backend = self.backend_label_static();
        let owner = Uuid::new_v4().to_string();
        let started = Instant::now();
        while !self
            .try_acquire_advisory_lease(JSON_ADMIN_AUDIT_LOCK, &owner, Duration::from_secs(10))
            .await
            .map_err(|err| SystemStoreError::io(backend, err))?
        {
            if started.elapsed() > Duration::from_secs(10) {
                return Err(SystemStoreError::io(backend, "admin audit lock timeout"));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let result = async {
            let audit_id = Uuid::new_v4();
            let previous_hash = self
                .latest_elasticsearch_admin_audit_hash_from_order()
                .await?;
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
            self.set_json(&key, &row).await?;
            self.add_to_set(&self.point_key("admin_audit_order"), key)
                .await?;
            self.set_json(&self.point_key("admin_audit_latest_hash"), &current_hash)
                .await?;
            Ok(audit_id)
        }
        .await;
        let _ = self
            .release_advisory_lease(JSON_ADMIN_AUDIT_LOCK, &owner)
            .await;
        result
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

    async fn next_seq_value(&self) -> Result<i64, String> {
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => self.next_elasticsearch_seq_value().await,
            #[allow(unreachable_patterns)]
            _ => Err(self.cas_unsupported("outbox sequence allocation")),
        }
    }

    #[cfg(feature = "elasticsearch")]
    async fn next_elasticsearch_seq_value(&self) -> Result<i64, String> {
        self.next_elasticsearch_cas_sequence_value(&self.point_key("outbox_seq"), "outbox sequence")
            .await
    }

    #[cfg(feature = "elasticsearch")]
    async fn next_elasticsearch_cas_sequence_value(
        &self,
        key: &str,
        label: &str,
    ) -> Result<i64, String> {
        for _ in 0..VECTOR_CAS_MAX_RETRIES {
            let current = self
                .get_elasticsearch_doc(key)
                .await
                .map_err(|err| err.to_string())?;
            let current_value = current
                .as_ref()
                .and_then(|doc| doc.value.as_ref())
                .cloned()
                .map(serde_json::from_value::<i64>)
                .transpose()
                .map_err(|err| format!("decode elasticsearch {label}: {err}"))?
                .unwrap_or(0);
            let next = current_value.saturating_add(1);
            let expected = current.as_ref().map(|doc| (doc.seq_no, doc.primary_term));
            if self
                .compare_and_set_elasticsearch_value(key, expected, json!(next))
                .await
                .map_err(|err| err.to_string())?
            {
                return Ok(next);
            }
        }
        Err(format!(
            "elasticsearch {label} CAS lost after {VECTOR_CAS_MAX_RETRIES} retries"
        ))
    }

    async fn next_migration_op_id(&self) -> Result<i64, String> {
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                self.next_elasticsearch_cas_sequence_value(
                    &self.point_key("migration_op_seq"),
                    "migration audit sequence",
                )
                .await
            }
            #[allow(unreachable_patterns)]
            _ => Err(self.cas_unsupported("migration audit sequence allocation")),
        }
    }

    async fn try_acquire_cas_advisory_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        ttl: Duration,
    ) -> Result<bool, String> {
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                self.try_acquire_elasticsearch_advisory_lease(lease_name, owner_id, ttl)
                    .await
            }
            #[allow(unreachable_patterns)]
            _ => Err(self.cas_unsupported("advisory leases")),
        }
    }

    #[cfg(feature = "elasticsearch")]
    async fn try_acquire_elasticsearch_advisory_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        ttl: Duration,
    ) -> Result<bool, String> {
        let key = self.point_key(&format!("lease:{lease_name}"));
        for _ in 0..VECTOR_CAS_MAX_RETRIES {
            let now = Utc::now().timestamp_millis();
            let current = self
                .get_elasticsearch_doc(&key)
                .await
                .map_err(|err| err.to_string())?;
            let current_lease: Option<AdvisoryLeaseRow> = current
                .as_ref()
                .and_then(|doc| doc.value.as_ref())
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(|err| format!("decode elasticsearch advisory lease: {err}"))?;
            if !advisory_lease_can_acquire(current_lease.as_ref(), owner_id, now) {
                return Ok(false);
            }
            let expected = current.as_ref().map(|doc| (doc.seq_no, doc.primary_term));
            let next = serde_json::to_value(new_advisory_lease_row(owner_id, now, ttl))
                .map_err(|err| format!("encode elasticsearch advisory lease: {err}"))?;
            if self
                .compare_and_set_elasticsearch_value(&key, expected, next)
                .await
                .map_err(|err| err.to_string())?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn release_cas_advisory_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
    ) -> Result<(), String> {
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                self.release_elasticsearch_advisory_lease(lease_name, owner_id)
                    .await
            }
            #[allow(unreachable_patterns)]
            _ => Err(self.cas_unsupported("advisory lease release")),
        }
    }

    #[cfg(feature = "elasticsearch")]
    async fn release_elasticsearch_advisory_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
    ) -> Result<(), String> {
        let key = self.point_key(&format!("lease:{lease_name}"));
        for _ in 0..VECTOR_CAS_MAX_RETRIES {
            let current = self
                .get_elasticsearch_doc(&key)
                .await
                .map_err(|err| err.to_string())?;
            let Some(doc) = current.as_ref() else {
                return Ok(());
            };
            let current_lease: Option<AdvisoryLeaseRow> = doc
                .value
                .as_ref()
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(|err| format!("decode elasticsearch advisory lease: {err}"))?;
            if !advisory_lease_is_owned_by(current_lease.as_ref(), owner_id) {
                return Ok(());
            }
            let tombstone = serde_json::to_value(AdvisoryLeaseRow {
                owner_id: String::new(),
                expires_at_ms: 0,
            })
            .map_err(|err| format!("encode elasticsearch lease tombstone: {err}"))?;
            if self
                .compare_and_set_elasticsearch_value(
                    &key,
                    Some((doc.seq_no, doc.primary_term)),
                    tombstone,
                )
                .await
                .map_err(|err| err.to_string())?
            {
                return Ok(());
            }
        }
        Err(format!(
            "elasticsearch advisory lease release CAS lost after {VECTOR_CAS_MAX_RETRIES} retries"
        ))
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
        let seq = self.next_seq_value().await?;
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
        self.ensure_cas_capable().await
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
        self.try_acquire_cas_advisory_lease(lease_name, owner_id, ttl)
            .await
    }

    async fn release_advisory_lease(&self, lease_name: &str, owner_id: &str) -> Result<(), String> {
        self.release_cas_advisory_lease(lease_name, owner_id).await
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

    async fn get_migration_run_row(&self, key: &str) -> SystemStoreResult<Option<MigrationRunRow>> {
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
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                return self.claim_elasticsearch_projection_tasks(filter).await;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

        let _guard = self.op_lock.lock().await;
        claim_json_projection_tasks(self, filter).await
    }

    async fn mark_projection_task_completed(&self, task_id: Uuid) -> SystemStoreResult<()> {
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                return self
                    .mark_elasticsearch_projection_task_completed(task_id)
                    .await;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

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
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                return self
                    .mark_elasticsearch_projection_task_failed(
                        task_id,
                        new_retry_count,
                        new_status,
                        error,
                    )
                    .await;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

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
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                return self
                    .requeue_elasticsearch_dead_letter_tasks(target_backend)
                    .await;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

        let _guard = self.op_lock.lock().await;
        requeue_json_dead_letter_tasks(self, target_backend).await
    }

    async fn reset_stale_in_progress_tasks(&self, stale_after: Duration) -> SystemStoreResult<i64> {
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                return self
                    .reset_stale_elasticsearch_projection_tasks(stale_after)
                    .await;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

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
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                return self
                    .requeue_elasticsearch_dead_letter_by_source(
                        source_table,
                        target_backend,
                        target_instance,
                    )
                    .await;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

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
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                return self
                    .update_elasticsearch_saga_status(saga_id, status, compensation_status)
                    .await;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

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
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                return self
                    .request_elasticsearch_saga_recompensation(saga_id)
                    .await;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

        let _guard = self.op_lock.lock().await;
        request_json_saga_recompensation(self, saga_id).await
    }

    async fn increment_recovery_attempts(
        &self,
        saga_id: Uuid,
        error: &str,
    ) -> SystemStoreResult<i64> {
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                return self
                    .increment_elasticsearch_saga_recovery_attempts(saga_id, error)
                    .await;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

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
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                return self
                    .mark_stale_elasticsearch_sagas_indeterminate(stale_after)
                    .await;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

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
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                return self
                    .latest_elasticsearch_admin_audit_hash_from_order()
                    .await;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

        latest_json_admin_audit_hash(self).await
    }

    async fn append_admin_audit(&self, entry: &AdminAuditInsert) -> SystemStoreResult<Uuid> {
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                return self.append_elasticsearch_admin_audit(entry).await;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

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
        let id = self
            .next_migration_op_id()
            .await
            .map_err(|err| SystemStoreError::io(self.backend_label_static(), err))?;
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
            // Only an APPLIED op records an applied_at timestamp (mirrors PG +
            // the Qdrant store); Skipped/Failed/Pending/etc. leave it None.
            applied_at: (op.status == OpLedgerStatus::Applied).then(Utc::now),
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
        match &self.client {
            #[cfg(feature = "elasticsearch")]
            VectorSystemClient::Elasticsearch(_) => {
                return self
                    .finish_elasticsearch_migration_run(run_id, new_state, error)
                    .await;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

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

#[cfg(test)]
mod recompensation_tests {
    //! Proves the recompensation semantics the vector-system store now
    //! delegates to. `VectorSystemCanonicalStore` itself wraps a live
    //! Pinecone/Weaviate/Elasticsearch HTTP client (no in-memory backend
    //! exists), so we drive the exact shared helper
    //! (`request_json_saga_recompensation`) that
    //! `SagaStore::request_saga_recompensation` calls, through a small
    //! map-backed [`JsonSystemRecordAdapter`]. Before this fix the
    //! vector-system path only set `compensation_status` and left `status`
    //! untouched, so a second recompensation was silently accepted; this
    //! test pins both halves of the fix.

    use std::collections::HashMap;
    use std::sync::Mutex;

    // `super::*` re-exports every name this test needs: the row/insert types,
    // SystemStoreError, the SagaStatus/CompensationStatus enums, the
    // JsonSystemRecordAdapter trait + its row types, record_json_saga, and the
    // request_json_saga_recompensation helper under test.
    use super::*;

    #[derive(Default)]
    struct MapAdapter {
        sagas: Mutex<HashMap<String, SagaRow>>,
        sets: Mutex<HashMap<String, Vec<String>>>,
    }

    #[async_trait]
    impl JsonSystemRecordAdapter for MapAdapter {
        fn record_backend_label(&self) -> &'static str {
            "test"
        }
        fn record_point_key(&self, suffix: &str) -> String {
            format!("test:{suffix}")
        }
        fn saga_record_key(&self, saga_id: Uuid) -> String {
            format!("test:saga:{saga_id}")
        }
        fn admin_audit_record_key(&self, audit_id: Uuid) -> String {
            format!("test:admin_audit:{audit_id}")
        }
        fn migration_run_record_key(&self, run_id: Uuid) -> String {
            format!("test:migration_run:{run_id}")
        }

        async fn get_saga_row(&self, key: &str) -> SystemStoreResult<Option<SagaRow>> {
            Ok(self.sagas.lock().unwrap().get(key).cloned())
        }
        async fn set_saga_row(&self, key: &str, row: &SagaRow) -> SystemStoreResult<()> {
            self.sagas
                .lock()
                .unwrap()
                .insert(key.to_string(), row.clone());
            Ok(())
        }
        async fn load_saga_rows(&self, _set_key: &str) -> SystemStoreResult<Vec<SagaRow>> {
            Ok(self.sagas.lock().unwrap().values().cloned().collect())
        }

        async fn get_admin_audit_row(
            &self,
            _key: &str,
        ) -> SystemStoreResult<Option<AdminAuditRow>> {
            Ok(None)
        }
        async fn set_admin_audit_row(
            &self,
            _key: &str,
            _row: &AdminAuditRow,
        ) -> SystemStoreResult<()> {
            Ok(())
        }
        async fn get_string_record(&self, _key: &str) -> SystemStoreResult<Option<String>> {
            Ok(None)
        }
        async fn set_string_record(&self, _key: &str, _value: &String) -> SystemStoreResult<()> {
            Ok(())
        }

        async fn get_migration_run_row(
            &self,
            _key: &str,
        ) -> SystemStoreResult<Option<MigrationRunRow>> {
            Ok(None)
        }
        async fn set_migration_run_row(
            &self,
            _key: &str,
            _row: &MigrationRunRow,
        ) -> SystemStoreResult<()> {
            Ok(())
        }
        async fn load_migration_run_rows(
            &self,
            _set_key: &str,
        ) -> SystemStoreResult<Vec<MigrationRunRow>> {
            Ok(Vec::new())
        }
        async fn load_migration_op_rows(
            &self,
            _set_key: &str,
        ) -> SystemStoreResult<Vec<MigrationOpRow>> {
            Ok(Vec::new())
        }

        async fn list_record_set(&self, set_key: &str) -> SystemStoreResult<Vec<String>> {
            Ok(self
                .sets
                .lock()
                .unwrap()
                .get(set_key)
                .cloned()
                .unwrap_or_default())
        }
        async fn add_record_to_set(&self, set_key: &str, item: String) -> SystemStoreResult<()> {
            self.sets
                .lock()
                .unwrap()
                .entry(set_key.to_string())
                .or_default()
                .push(item);
            Ok(())
        }
    }

    fn failed_compensation_saga() -> SagaInsert {
        SagaInsert {
            tx_id: "tx-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            correlation_id: "corr-1".to_string(),
            backend_instance: "inst-1".to_string(),
            operation: "op".to_string(),
            status: SagaStatus::FailedCompensation,
            steps: json!([]),
            compensations: json!([]),
        }
    }

    #[tokio::test]
    async fn recompensation_sets_indeterminate_then_refuses_second() {
        let adapter = MapAdapter::default();

        // Seed a saga in the only-eligible failed_compensation state, with a
        // non-empty last_error to prove it gets cleared.
        let saga_id = record_json_saga(&adapter, &failed_compensation_saga())
            .await
            .expect("record saga");
        {
            let key = adapter.saga_record_key(saga_id);
            let mut sagas = adapter.sagas.lock().unwrap();
            sagas.get_mut(&key).unwrap().last_error = "boom".to_string();
        }

        // First recompensation: must move to indeterminate, clear last_error,
        // and flag compensation_status = retry_requested.
        request_json_saga_recompensation(&adapter, saga_id)
            .await
            .expect("first recompensation succeeds");
        let row = adapter
            .get_saga_row(&adapter.saga_record_key(saga_id))
            .await
            .unwrap()
            .expect("saga present");
        assert_eq!(row.status, SagaStatus::Indeterminate);
        assert_eq!(row.compensation_status, CompensationStatus::RetryRequested);
        assert_eq!(row.last_error, "");

        // Second recompensation: the saga is now indeterminate (not an
        // eligible source state), so it MUST be refused. This is the bug the
        // old vector-system impl silently allowed.
        let err = request_json_saga_recompensation(&adapter, saga_id)
            .await
            .expect_err("second recompensation must be refused");
        assert!(matches!(err, SystemStoreError::InvalidInput(_)));

        // State unchanged after the refused second attempt.
        let row = adapter
            .get_saga_row(&adapter.saga_record_key(saga_id))
            .await
            .unwrap()
            .expect("saga present");
        assert_eq!(row.status, SagaStatus::Indeterminate);
    }
}

#[cfg(any(feature = "pinecone", feature = "weaviate"))]
#[cfg(test)]
mod vector_native_cas_fail_closed_tests {
    use std::time::Duration;

    use super::*;

    async fn assert_advisory_lease_fails_closed(store: VectorSystemCanonicalStore) {
        let backend = store.backend_label_static();
        let err = CanonicalStore::try_acquire_advisory_lease(
            &store,
            "worker",
            "owner",
            Duration::from_secs(1),
        )
        .await
        .expect_err("non-ES vector canonical leases must fail closed without native CAS");
        assert!(
            err.contains(backend),
            "error should name backend {backend}; got: {err}"
        );
        assert!(err.contains("real multi-process CAS"));
        assert!(err.contains("backend-native conditional write"));
    }

    #[cfg(feature = "pinecone")]
    #[tokio::test]
    async fn pinecone_advisory_lease_fails_closed_until_native_cas_exists() {
        let store = VectorSystemCanonicalStore::new_pinecone(
            crate::runtime::executors::pinecone::PineconeHttpClient::new(
                "https://example.invalid",
                "test-key",
            ),
            "unit",
        );
        assert_advisory_lease_fails_closed(store).await;
    }

    #[cfg(feature = "weaviate")]
    #[tokio::test]
    async fn weaviate_advisory_lease_fails_closed_until_native_cas_exists() {
        let store = VectorSystemCanonicalStore::new_weaviate(
            crate::runtime::executors::weaviate::WeaviateHttpClient::new(
                "http://127.0.0.1:8080",
                None,
            ),
            "unit",
        );
        assert_advisory_lease_fails_closed(store).await;
    }
}
