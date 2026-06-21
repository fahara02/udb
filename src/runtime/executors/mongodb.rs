//! MongoDB document-store executor.
//!
//! Uses the MongoDB Atlas Data API (HTTP/JSON) so that no native driver
//! dependency is required. Plain `mongodb://` connection strings are only used
//! to derive the database name; callers must provide `UDB_NOSQL_API_URL` (or an
//! instance `api_url` label) for the HTTP API endpoint.
//!
//! Environment variables
//! ─────────────────────
//! | Variable               | Purpose                                             |
//! |------------------------|-----------------------------------------------------|
//! | `UDB_NOSQL_DSN`        | `mongodb://[user:pass@]host:port/dbname`             |
//! | `UDB_NOSQL_API_URL`    | Override Atlas Data API base (default: parsed from DSN) |
//! | `UDB_NOSQL_API_KEY`    | API key for Atlas Data API                          |
//! | `UDB_NOSQL_DATABASE`   | Database name (defaults to path segment of DSN)     |
//!
//! Operations implemented
//! ──────────────────────
//! - `ping()`               → GET /action/find (limit 0)
//! - `ensure_resource()`    → createCollection command via runCommand
//! - `drop_resource()`      → dropCollection command via runCommand
//! - `list_resources()`     → listCollections via runCommand
//! - `insert_document()`    → insertOne
//! - `find_documents()`     → find with filter + projection
//! - `update_document()`    → updateOne with $set
//! - `delete_document()`    → deleteOne
//! - `count_documents()`    → count

use std::env;

#[cfg(feature = "mongodb-native")]
use futures::TryStreamExt;
#[cfg(feature = "mongodb-native")]
use mongodb_driver::bson::{Bson, Document};
use reqwest::Client;
use serde::Serialize;
use serde_json::{Value as Json, json};

use crate::backend::BackendKind;
use crate::runtime::executor_utils::build_probe;
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

// ── Configuration ─────────────────────────────────────────────────────────────

/// Connection configuration parsed from env-vars.
#[derive(Debug, Clone)]
pub struct MongoDbConfig {
    /// Full HTTP(S) base URL to the Atlas Data API (or self-hosted equivalent).
    /// e.g. `https://data.mongodb-api.com/app/<APP_ID>/endpoint/data/v1`
    pub api_base: String,
    /// API key / bearer token.
    pub api_key: Option<String>,
    /// Default database name.
    pub database: String,
    /// Whether this endpoint should enforce HTTPS-only requests.
    pub is_cloud: bool,
    /// Allow plaintext self-hosted HTTP without warning.
    pub dev_mode: bool,
    /// Request timeout in seconds.
    pub timeout_secs: u64,
}

/// Native MongoDB wire-protocol connection configuration.
#[cfg(feature = "mongodb-native")]
#[derive(Debug, Clone)]
pub struct MongoDbNativeConfig {
    /// Full `mongodb://` or `mongodb+srv://` connection string.
    pub dsn: String,
    /// Default database name.
    pub database: String,
    /// Request timeout in seconds.
    pub timeout_secs: u64,
    /// Optional driver application name.
    pub app_name: Option<String>,
    /// Optional maximum driver pool size.
    pub max_pool_size: Option<u32>,
    /// Whether to force direct connection to a single seed.
    pub direct_connection: Option<bool>,
    /// Override retryable writes.
    pub retry_writes: Option<bool>,
}

impl MongoDbConfig {
    /// Compatibility-only env constructor.
    ///
    /// New runtime paths should use `runtime::config::UdbConfig` and pass the
    /// resolved backend settings into executors instead of reading process env.
    pub fn from_env() -> Option<Self> {
        // Require the HTTP Data API endpoint explicitly. A mongodb:// DSN is a
        // native wire-protocol endpoint, not an HTTP API base.
        let dsn = env::var("UDB_NOSQL_DSN").ok();
        let api_base = env::var("UDB_NOSQL_API_URL").ok()?;

        let database = env::var("UDB_NOSQL_DATABASE")
            .ok()
            .or_else(|| dsn.as_deref().and_then(Self::db_from_dsn))
            .unwrap_or_else(|| "udb".to_string());

        let api_key = env::var("UDB_NOSQL_API_KEY")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        let is_cloud = super::http::is_cloud("UDB_MONGO_DEPLOY_MODE", &api_base, ".mongodb.net");
        let dev_mode = std::env::var("UDB_DEV_MODE")
            .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);
        let timeout_secs = super::http::env_timeout("UDB_NOSQL_TIMEOUT_SECS", 30).as_secs();

        Some(Self {
            api_base,
            api_key,
            database,
            is_cloud,
            dev_mode,
            timeout_secs,
        })
    }

    #[cfg(test)]
    pub(crate) fn host_from_dsn(dsn: &str) -> Option<String> {
        // mongodb://[user:pass@]host:port[/db]
        let rest = dsn.strip_prefix("mongodb://")?;
        let rest = if rest.contains('@') {
            rest.split_once('@').map(|(_, r)| r)?
        } else {
            rest
        };
        let host = rest.split('/').next()?;
        Some(host.to_string())
    }

    pub(crate) fn db_from_dsn(dsn: &str) -> Option<String> {
        // last path segment after the host
        let after_host = dsn.strip_prefix("mongodb://").unwrap_or(dsn);
        let after_host = if after_host.contains('@') {
            after_host
                .split_once('@')
                .map(|(_, r)| r)
                .unwrap_or(after_host)
        } else {
            after_host
        };
        let db = after_host.split('/').nth(1)?;
        let db = db.split('?').next().unwrap_or(db);
        if db.is_empty() {
            None
        } else {
            Some(db.to_string())
        }
    }
}

// ── Executor ──────────────────────────────────────────────────────────────────

/// MongoDB document-store executor.
///
/// The default transport is Atlas/Data API HTTP. When compiled with
/// `mongodb-native`, the same executor can also own a native MongoDB driver
/// client for `mongodb://` and `mongodb+srv://` DSNs.
#[derive(Debug, Clone)]
pub struct MongoDbExecutor {
    transport: MongoDbTransport,
}

impl crate::runtime::backend_context::BackendContextEnforcer for MongoDbExecutor {
    fn backend_label(&self) -> &str {
        "mongodb"
    }

    fn enforce(
        &self,
        ctx: &crate::runtime::backend_context::AppliedContext,
    ) -> crate::runtime::backend_context::ContextEffect {
        // HONEST POSTURE: this generic executor accepts caller-provided command
        // JSON and filters. It can record tenant context for audit/operator
        // policy, but it does not prove that arbitrary command/filter dispatch
        // was rewritten with `_tenant_id` / `_project_id` predicates. Treat the
        // generic path as advisory unless a higher-level compiler path owns and
        // verifies predicate injection.
        if ctx.is_empty() {
            crate::runtime::backend_context::ContextEffect::Advisory {
                recorded_in: "no_context_to_apply".into(),
            }
        } else {
            crate::runtime::backend_context::ContextEffect::Advisory {
                recorded_in: "MongoDB context recorded for generic command/filter dispatch; \
                              _tenant_id/_project_id predicate injection not verified here"
                    .into(),
            }
        }
    }
}

#[derive(Debug, Clone)]
enum MongoDbTransport {
    DataApi {
        config: MongoDbConfig,
        http: Client,
    },
    #[cfg(feature = "mongodb-native")]
    Native(MongoDbNativeExecutor),
}

#[cfg(feature = "mongodb-native")]
#[derive(Debug, Clone)]
struct MongoDbNativeExecutor {
    config: MongoDbNativeConfig,
    client: mongodb_driver::Client,
}

/// Wrap an update document in `$set` ONLY when it does not already use MongoDB
/// update operators (top-level `$`-prefixed keys like `$set`/`$inc`/`$push`/
/// `$unset`). Unconditionally wrapping an operator doc produces
/// `{"$set":{"$inc":...}}`, which MongoDB rejects.
fn mongo_wrap_update(update: Json) -> Json {
    let has_operator = update
        .as_object()
        .map(|m| m.keys().any(|k| k.starts_with('$')))
        .unwrap_or(false);
    if has_operator {
        update
    } else {
        json!({ "$set": update })
    }
}

#[cfg(feature = "mongodb-native")]
fn mongo_wrap_update_bson(
    update: mongodb_driver::bson::Document,
) -> mongodb_driver::bson::Document {
    if update.keys().any(|k| k.starts_with('$')) {
        update
    } else {
        mongodb_driver::bson::doc! { "$set": update }
    }
}

impl MongoDbExecutor {
    /// Construct from config.
    ///
    /// GAP 28 fix: the HTTP client is built with a configurable request timeout
    /// (UDB_NOSQL_TIMEOUT_SECS, default 30 s) to prevent Atlas Data API calls
    /// from blocking indefinitely. A warning is emitted when plain HTTP is used
    /// since the api-key header would cross the wire unencrypted.
    ///
    /// GAP 39: When `UDB_MONGO_DEPLOY_MODE=cloud` (or the DSN contains
    /// `.mongodb.net`), the URL must use `https://` and the client is built
    /// with `.https_only(true)` to prevent api-key leakage.
    pub fn new(config: MongoDbConfig) -> Self {
        if config.is_cloud && config.api_base.starts_with("http://") {
            // Log an error instead of panicking — the reqwest client is already
            // built with .https_only(true) below, so every request will fail at
            // runtime with a clear TLS error rather than crashing the process.
            // Panicking here would bring down the entire service if MongoDB is
            // misconfigured, which is worse than a per-request error.
            tracing::error!(
                api_base = %config.api_base,
                "MongoDB is configured as cloud but the API base uses http:// — \
                 all requests will fail. Change to https:// or set UDB_MONGO_DEPLOY_MODE=self_hosted"
            );
        }
        if !config.dev_mode && !config.is_cloud && config.api_base.starts_with("http://") {
            tracing::warn!(
                api_base = %config.api_base,
                "MongoDB API base uses plain HTTP — api-key header will be sent unencrypted"
            );
        }
        let timeout = std::time::Duration::from_secs(config.timeout_secs.max(1));
        let http = super::http::HttpClientSpec::with_timeout(timeout)
            .https_only(config.is_cloud)
            .build();
        Self {
            transport: MongoDbTransport::DataApi { config, http },
        }
    }

    /// Construct a native wire-protocol MongoDB executor.
    #[cfg(feature = "mongodb-native")]
    pub async fn new_native(config: MongoDbNativeConfig) -> Result<Self, String> {
        let mut options = mongodb_driver::options::ClientOptions::parse(&config.dsn)
            .await
            .map_err(|err| format!("MongoDB native DSN parse failed: {err}"))?;
        let timeout = std::time::Duration::from_secs(config.timeout_secs.max(1));
        options.connect_timeout = Some(timeout);
        options.server_selection_timeout = Some(timeout);
        options.app_name = Some(config.app_name.clone().unwrap_or_else(|| "udb".to_string()));
        options.default_database = Some(config.database.clone());
        // Only override DSN-parsed values when the config explicitly sets them;
        // a `None` config field must NOT clobber a value the DSN already carries
        // (e.g. `?directConnection=true` / `?retryWrites=…`).
        if config.max_pool_size.is_some() {
            options.max_pool_size = config.max_pool_size;
        }
        if let Some(direct) = config.direct_connection {
            options.direct_connection = Some(direct);
        }
        if let Some(retry) = config.retry_writes {
            options.retry_writes = Some(retry);
        }
        let client = mongodb_driver::Client::with_options(options)
            .map_err(|err| format!("MongoDB native client creation failed: {err}"))?;
        Ok(Self {
            transport: MongoDbTransport::Native(MongoDbNativeExecutor { config, client }),
        })
    }

    pub fn kind(&self) -> BackendKind {
        BackendKind::Mongodb
    }

    pub fn name(&self) -> &str {
        "MongoDB"
    }

    /// Construct from environment variables.  Returns `None` when
    /// `UDB_NOSQL_DSN` / `UDB_NOSQL_API_URL` are absent.
    pub fn from_env() -> Option<Self> {
        MongoDbConfig::from_env().map(Self::new)
    }

    /// Returns the transport mechanism used by this executor.
    ///
    /// Callers (e.g. `GetCapabilities`, `GetAdminSummary`) should surface this
    /// value so operators know which transport is active.
    pub fn transport_kind(&self) -> &'static str {
        match &self.transport {
            MongoDbTransport::DataApi { .. } => "atlas_data_api",
            #[cfg(feature = "mongodb-native")]
            MongoDbTransport::Native(_) => "native",
        }
    }

    // ── Low-level helpers ─────────────────────────────────────────────────────

    fn action_url(&self, action: &str) -> String {
        match &self.transport {
            MongoDbTransport::DataApi { config, .. } => {
                format!("{}/action/{action}", config.api_base)
            }
            #[cfg(feature = "mongodb-native")]
            MongoDbTransport::Native(_) => String::new(),
        }
    }

    #[cfg_attr(not(feature = "mongodb-native"), allow(irrefutable_let_patterns))]
    fn base_body(&self, collection: &str) -> Json {
        let MongoDbTransport::DataApi { config, .. } = &self.transport else {
            return json!({});
        };
        json!({
            "dataSource": "Cluster0",
            "database": config.database,
            "collection": collection,
        })
    }

    fn auth_header(&self) -> Option<(&'static str, String)> {
        match &self.transport {
            MongoDbTransport::DataApi { config, .. } => {
                config.api_key.as_ref().map(|k| ("api-key", k.clone()))
            }
            #[cfg(feature = "mongodb-native")]
            MongoDbTransport::Native(_) => None,
        }
    }

    #[cfg_attr(not(feature = "mongodb-native"), allow(irrefutable_let_patterns))]
    async fn post_action<B: Serialize>(&self, action: &str, body: B) -> Result<Json, String> {
        let MongoDbTransport::DataApi { http, .. } = &self.transport else {
            return Err("MongoDB Data API action called on native transport".to_string());
        };
        let mut req = http.post(self.action_url(action)).json(&body);
        if let Some((header, value)) = self.auth_header() {
            req = req.header(header, value);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| format!("MongoDB HTTP error: {e}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("MongoDB {action} failed [{status}]: {text}"));
        }
        serde_json::from_str::<Json>(&text)
            .map_err(|e| format!("MongoDB {action} JSON decode failed: {e}"))
    }

    #[cfg(feature = "mongodb-native")]
    fn native(&self) -> Result<&MongoDbNativeExecutor, String> {
        match &self.transport {
            MongoDbTransport::Native(native) => Ok(native),
            MongoDbTransport::DataApi { .. } => {
                Err("MongoDB native action called on Data API transport".to_string())
            }
        }
    }

    /// B.9: the native default `Database` handle (the database resolved from the
    /// native config). `Some` only for the native transport. The canonical store
    /// builds its outbox / lease collections on this handle.
    #[cfg(feature = "mongodb-native")]
    pub(crate) fn native_database(&self) -> Option<mongodb_driver::Database> {
        match &self.transport {
            MongoDbTransport::Native(native) => Some(native.database()),
            MongoDbTransport::DataApi { .. } => None,
        }
    }

    #[cfg(feature = "mongodb-native")]
    fn json_to_document(value: &Json, context: &str) -> Result<Document, String> {
        match mongodb_driver::bson::to_bson(value)
            .map_err(|err| format!("MongoDB {context} BSON encode failed: {err}"))?
        {
            Bson::Document(document) => Ok(document),
            Bson::Null => Ok(Document::new()),
            other => Err(format!(
                "MongoDB {context} must be a JSON object, got BSON {other:?}"
            )),
        }
    }

    #[cfg(feature = "mongodb-native")]
    fn document_to_json(document: Document) -> Json {
        serde_json::to_value(Bson::Document(document)).unwrap_or(Json::Null)
    }

    // ── Public document API ───────────────────────────────────────────────────

    /// Insert a single document into `collection`.
    pub async fn insert_document(
        &self,
        collection: &str,
        document: Json,
    ) -> Result<String, String> {
        #[cfg(feature = "mongodb-native")]
        if let MongoDbTransport::Native(native) = &self.transport {
            return native.insert_document(collection, document).await;
        }

        let mut body = self.base_body(collection);
        body["document"] = document;
        let resp = self.post_action("insertOne", body).await?;
        let inserted_id = resp
            .get("insertedId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Ok(inserted_id)
    }

    /// Find documents matching `filter`.
    /// `projection` selects which fields to return (empty JSON object = all fields).
    /// `limit` 0 means use the server default (20 for Atlas Data API).
    pub async fn find_documents(
        &self,
        collection: &str,
        filter: Json,
        projection: Json,
        limit: i64,
    ) -> Result<Vec<Json>, String> {
        #[cfg(feature = "mongodb-native")]
        if let MongoDbTransport::Native(native) = &self.transport {
            return native
                .find_documents(collection, filter, projection, limit)
                .await;
        }

        let mut body = self.base_body(collection);
        body["filter"] = filter;
        if !projection.is_null() && projection != json!({}) {
            body["projection"] = projection;
        }
        if limit > 0 {
            body["limit"] = json!(limit);
        }
        let resp = self.post_action("find", body).await?;
        let docs = resp
            .get("documents")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(docs)
    }

    /// Update the first document matching `filter` using MongoDB `$set` semantics.
    pub async fn update_document(
        &self,
        collection: &str,
        filter: Json,
        update: Json,
    ) -> Result<i64, String> {
        #[cfg(feature = "mongodb-native")]
        if let MongoDbTransport::Native(native) = &self.transport {
            return native
                .update_document(collection, filter, update, false)
                .await;
        }

        let mut body = self.base_body(collection);
        body["filter"] = filter;
        body["update"] = mongo_wrap_update(update);
        let resp = self.post_action("updateOne", body).await?;
        let modified = resp
            .get("modifiedCount")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        Ok(modified)
    }

    /// Upsert a document matching `filter` using MongoDB `$set` semantics.
    pub async fn upsert_document(
        &self,
        collection: &str,
        filter: Json,
        update: Json,
    ) -> Result<i64, String> {
        #[cfg(feature = "mongodb-native")]
        if let MongoDbTransport::Native(native) = &self.transport {
            return native
                .update_document(collection, filter, update, true)
                .await;
        }

        let mut body = self.base_body(collection);
        body["filter"] = filter;
        body["update"] = mongo_wrap_update(update);
        body["upsert"] = json!(true);
        let resp = self.post_action("updateOne", body).await?;
        let modified = resp
            .get("modifiedCount")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let upserted = resp.get("upsertedId").map(|_| 1).unwrap_or(0);
        Ok(modified.max(upserted))
    }

    /// Delete the first document matching `filter`.
    pub async fn delete_document(&self, collection: &str, filter: Json) -> Result<i64, String> {
        #[cfg(feature = "mongodb-native")]
        if let MongoDbTransport::Native(native) = &self.transport {
            return native.delete_document(collection, filter).await;
        }

        let mut body = self.base_body(collection);
        body["filter"] = filter;
        let resp = self.post_action("deleteOne", body).await?;
        let deleted = resp
            .get("deletedCount")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        Ok(deleted)
    }

    /// Count documents matching `filter`.
    pub async fn count_documents(&self, collection: &str, filter: Json) -> Result<i64, String> {
        #[cfg(feature = "mongodb-native")]
        if let MongoDbTransport::Native(native) = &self.transport {
            return native.count_documents(collection, filter).await;
        }

        // Atlas Data API /action/aggregate with $count stage
        let mut body = self.base_body(collection);
        body["pipeline"] = json!([
            { "$match": filter },
            { "$count": "n" }
        ]);
        let resp = self.post_action("aggregate", body).await?;
        let count = resp
            .get("documents")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|obj| obj.get("n"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        Ok(count)
    }

    pub async fn insert_many(&self, collection: &str, documents: &[Json]) -> Result<i64, String> {
        #[cfg(feature = "mongodb-native")]
        if let MongoDbTransport::Native(native) = &self.transport {
            return native.insert_many(collection, documents).await;
        }

        let mut body = self.base_body(collection);
        body["documents"] = Json::Array(documents.to_vec());
        let resp = self.post_action("insertMany", body).await?;
        Ok(resp
            .get("insertedIds")
            .and_then(Json::as_array)
            .map(|ids| ids.len() as i64)
            .unwrap_or(documents.len() as i64))
    }

    pub async fn update_many(
        &self,
        collection: &str,
        filter: Json,
        update: Json,
    ) -> Result<i64, String> {
        #[cfg(feature = "mongodb-native")]
        if let MongoDbTransport::Native(native) = &self.transport {
            return native.update_many(collection, filter, update).await;
        }

        let mut body = self.base_body(collection);
        body["filter"] = filter;
        body["update"] = mongo_wrap_update(update);
        let resp = self.post_action("updateMany", body).await?;
        Ok(resp
            .get("modifiedCount")
            .and_then(Json::as_i64)
            .unwrap_or_default())
    }

    pub async fn delete_many(&self, collection: &str, filter: Json) -> Result<i64, String> {
        #[cfg(feature = "mongodb-native")]
        if let MongoDbTransport::Native(native) = &self.transport {
            return native.delete_many(collection, filter).await;
        }

        let mut body = self.base_body(collection);
        body["filter"] = filter;
        let resp = self.post_action("deleteMany", body).await?;
        Ok(resp
            .get("deletedCount")
            .and_then(Json::as_i64)
            .unwrap_or_default())
    }

    async fn ensure_indexes(&self, collection: &str, indexes: &[Json]) -> Result<(), String> {
        if indexes.is_empty() {
            return Ok(());
        }
        let index_specs = indexes
            .iter()
            .map(|index| {
                let key = index
                    .get("key")
                    .cloned()
                    .unwrap_or_else(|| json!({ "id": 1 }));
                let name = index
                    .get("name")
                    .and_then(Json::as_str)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| {
                        key.as_object()
                            .map(|map| {
                                map.keys()
                                    .map(|field| format!("{field}_idx"))
                                    .collect::<Vec<_>>()
                                    .join("_")
                            })
                            .filter(|name| !name.is_empty())
                            .unwrap_or_else(|| "udb_idx".to_string())
                    });
                let mut spec = json!({ "key": key, "name": name });
                if let Some(unique) = index.get("unique") {
                    spec["unique"] = unique.clone();
                }
                if let Some(expire) = index.get("expire_after_seconds") {
                    spec["expireAfterSeconds"] = expire.clone();
                }
                spec
            })
            .collect::<Vec<_>>();
        self.run_command(json!({
            "createIndexes": collection,
            "indexes": index_specs,
        }))
        .await
        .map(|_| ())
    }

    #[cfg(feature = "mongodb-native")]
    async fn watch_changes(&self, collection: &str, pipeline: Vec<Json>) -> Result<Json, String> {
        let native = self.native()?;
        native.watch_changes(collection, pipeline).await
    }

    /// Run a raw MongoDB command (database-level command JSON).
    #[cfg_attr(not(feature = "mongodb-native"), allow(irrefutable_let_patterns))]
    async fn run_command(&self, command: Json) -> Result<Json, String> {
        #[cfg(feature = "mongodb-native")]
        if matches!(self.transport, MongoDbTransport::Native(_)) {
            let native = self.native()?;
            let command = Self::json_to_document(&command, "command")?;
            let document = native
                .database()
                .run_command(command)
                .await
                .map_err(|err| format!("MongoDB native runCommand failed: {err}"))?;
            return Ok(Self::document_to_json(document));
        }

        let MongoDbTransport::DataApi { config, http } = &self.transport else {
            return Err("MongoDB runCommand called on unsupported transport".to_string());
        };
        let url = format!("{}/action/runCommand", config.api_base);
        let body = json!({
            "dataSource": "Cluster0",
            "database": config.database,
            "command": command,
        });
        let mut req = http.post(url).json(&body);
        if let Some((h, v)) = self.auth_header() {
            req = req.header(h, v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| format!("MongoDB runCommand error: {e}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("MongoDB runCommand failed [{status}]: {text}"));
        }
        serde_json::from_str::<Json>(&text)
            .map_err(|e| format!("MongoDB runCommand decode failed: {e}"))
    }
}

#[cfg(feature = "mongodb-native")]
impl MongoDbNativeExecutor {
    /// Classify a MongoDB driver error into a tagged error string (bug_report.md
    /// B5). Not-primary / topology errors (codes 10107 NotWritablePrimary, 13435
    /// NotPrimaryNoSecondaryOk, 13436 NotPrimaryOrSecondary, or the
    /// `NotWritablePrimary`/`NotPrimaryOrSecondary` server labels) are transient:
    /// they tag a retryable `Unavailable` with a clean message (no raw BSON blob).
    /// Every other error keeps the descriptive `{context}: {err}` (untagged →
    /// decodes to `Internal` at the boundary).
    fn classify_err(context: &str, err: &mongodb_driver::error::Error) -> String {
        // The driver's typed `code()`/`is_*` predicates are crate-private in
        // mongodb 3.x, so classify off the public error text + labels. Not-primary
        // / topology errors surface their SQLSTATE-equivalent code + code-name in
        // the message (e.g. "Error code 10107 (NotWritablePrimary)").
        let text = err.to_string();
        let is_not_primary = text.contains("NotWritablePrimary")
            || text.contains("NotPrimaryOrSecondary")
            || text.contains("NotPrimaryNoSecondaryOk")
            || text.contains("not primary")
            || text.contains("10107")
            || text.contains("13435")
            || text.contains("13436");
        if is_not_primary {
            return crate::runtime::executor_utils::tagged_status_string(
                tonic::Code::Unavailable,
                "backend temporarily unavailable (not primary)",
            );
        }
        format!("{context}: {err}")
    }

    fn database(&self) -> mongodb_driver::Database {
        self.client.database(&self.config.database)
    }

    fn collection(&self, collection: &str) -> mongodb_driver::Collection<Document> {
        self.database().collection::<Document>(collection)
    }

    async fn insert_document(&self, collection: &str, document: Json) -> Result<String, String> {
        let document = MongoDbExecutor::json_to_document(&document, "insert document")?;
        let result = self
            .collection(collection)
            .insert_one(document)
            .await
            .map_err(|err| Self::classify_err("MongoDB native insertOne failed", &err))?;
        Ok(result.inserted_id.to_string())
    }

    async fn find_documents(
        &self,
        collection: &str,
        filter: Json,
        projection: Json,
        limit: i64,
    ) -> Result<Vec<Json>, String> {
        let filter = MongoDbExecutor::json_to_document(&filter, "find filter")?;
        let collection = self.collection(collection);
        let mut find = collection.find(filter);
        if !projection.is_null() && projection != json!({}) {
            find = find.projection(MongoDbExecutor::json_to_document(
                &projection,
                "find projection",
            )?);
        }
        if limit > 0 {
            find = find.limit(limit);
        }
        let cursor = find
            .await
            .map_err(|err| format!("MongoDB native find failed: {err}"))?;
        let docs: Vec<Document> = cursor
            .try_collect()
            .await
            .map_err(|err| format!("MongoDB native cursor read failed: {err}"))?;
        Ok(docs
            .into_iter()
            .map(MongoDbExecutor::document_to_json)
            .collect())
    }

    async fn update_document(
        &self,
        collection: &str,
        filter: Json,
        update: Json,
        upsert: bool,
    ) -> Result<i64, String> {
        let filter = MongoDbExecutor::json_to_document(&filter, "update filter")?;
        let update = MongoDbExecutor::json_to_document(&update, "update document")?;
        let update = mongo_wrap_update_bson(update);
        let result = self
            .collection(collection)
            .update_one(filter, update)
            .upsert(upsert)
            .await
            .map_err(|err| Self::classify_err("MongoDB native updateOne failed", &err))?;
        let upserted = i64::from(result.upserted_id.is_some());
        Ok((result.modified_count as i64).max(upserted))
    }

    async fn delete_document(&self, collection: &str, filter: Json) -> Result<i64, String> {
        let filter = MongoDbExecutor::json_to_document(&filter, "delete filter")?;
        let result = self
            .collection(collection)
            .delete_one(filter)
            .await
            .map_err(|err| Self::classify_err("MongoDB native deleteOne failed", &err))?;
        Ok(result.deleted_count as i64)
    }

    async fn count_documents(&self, collection: &str, filter: Json) -> Result<i64, String> {
        let filter = MongoDbExecutor::json_to_document(&filter, "count filter")?;
        let count = self
            .collection(collection)
            .count_documents(filter)
            .await
            .map_err(|err| format!("MongoDB native countDocuments failed: {err}"))?;
        Ok(count as i64)
    }

    async fn insert_many(&self, collection: &str, documents: &[Json]) -> Result<i64, String> {
        let docs = documents
            .iter()
            .map(|document| MongoDbExecutor::json_to_document(document, "insertMany document"))
            .collect::<Result<Vec<_>, _>>()?;
        if docs.is_empty() {
            return Ok(0);
        }
        let result = self
            .collection(collection)
            .insert_many(docs)
            .await
            .map_err(|err| Self::classify_err("MongoDB native insertMany failed", &err))?;
        Ok(result.inserted_ids.len() as i64)
    }

    async fn update_many(
        &self,
        collection: &str,
        filter: Json,
        update: Json,
    ) -> Result<i64, String> {
        let filter = MongoDbExecutor::json_to_document(&filter, "updateMany filter")?;
        let update = MongoDbExecutor::json_to_document(&update, "updateMany document")?;
        let update = mongo_wrap_update_bson(update);
        let result = self
            .collection(collection)
            .update_many(filter, update)
            .await
            .map_err(|err| Self::classify_err("MongoDB native updateMany failed", &err))?;
        Ok(result.modified_count as i64)
    }

    async fn delete_many(&self, collection: &str, filter: Json) -> Result<i64, String> {
        let filter = MongoDbExecutor::json_to_document(&filter, "deleteMany filter")?;
        let result = self
            .collection(collection)
            .delete_many(filter)
            .await
            .map_err(|err| Self::classify_err("MongoDB native deleteMany failed", &err))?;
        Ok(result.deleted_count as i64)
    }

    async fn watch_changes(&self, collection: &str, pipeline: Vec<Json>) -> Result<Json, String> {
        let pipeline = pipeline
            .iter()
            .map(|stage| MongoDbExecutor::json_to_document(stage, "change stream pipeline stage"))
            .collect::<Result<Vec<_>, _>>()?;
        let mut stream = self
            .collection(collection)
            .watch()
            .pipeline(pipeline)
            .await
            .map_err(|err| format!("MongoDB native change stream open failed: {err}"))?;
        let event = stream
            .next_if_any()
            .await
            .map_err(|err| format!("MongoDB native change stream poll failed: {err}"))?;
        Ok(json!({
            "alive": stream.is_alive(),
            "resume_token": stream.resume_token().map(|token| serde_json::to_value(token).unwrap_or(Json::Null)),
            "event": event.and_then(|event| serde_json::to_value(event).ok()),
        }))
    }

    async fn execute_transaction(&self, operations: &[Json]) -> Result<i64, String> {
        let mut session = self
            .client
            .start_session()
            .await
            .map_err(|err| format!("MongoDB native startSession failed: {err}"))?;
        session
            .start_transaction()
            .await
            .map_err(|err| format!("MongoDB native startTransaction failed: {err}"))?;
        let mut affected = 0_i64;
        for operation in operations {
            match self
                .execute_mutation_in_session(operation, &mut session)
                .await
            {
                Ok(count) => affected += count,
                Err(err) => {
                    let _ = session.abort_transaction().await;
                    return Err(err);
                }
            }
        }
        session
            .commit_transaction()
            .await
            .map_err(|err| format!("MongoDB native commitTransaction failed: {err}"))?;
        Ok(affected)
    }

    async fn execute_mutation_in_session(
        &self,
        spec: &Json,
        session: &mut mongodb_driver::ClientSession,
    ) -> Result<i64, String> {
        let collection = spec
            .get("collection")
            .and_then(Json::as_str)
            .ok_or_else(|| "transaction operation missing collection".to_string())?;
        let operation = spec
            .get("operation")
            .and_then(Json::as_str)
            .ok_or_else(|| "transaction operation missing operation".to_string())?;
        let coll = self.collection(collection);
        match operation {
            "insert" | "insert_one" => {
                let document = spec
                    .get("document")
                    .ok_or_else(|| "transaction insert requires document".to_string())?;
                let document = MongoDbExecutor::json_to_document(document, "transaction insert")?;
                coll.insert_one(document)
                    .session(&mut *session)
                    .await
                    .map_err(|err| format!("MongoDB native transaction insert failed: {err}"))?;
                Ok(1)
            }
            "update" | "update_one" | "upsert" | "upsert_one" => {
                let filter = spec
                    .get("filter")
                    .ok_or_else(|| "transaction update requires filter".to_string())?;
                let update = spec
                    .get("update")
                    .or_else(|| spec.get("document"))
                    .ok_or_else(|| "transaction update requires update/document".to_string())?;
                let filter =
                    MongoDbExecutor::json_to_document(filter, "transaction update filter")?;
                let update = MongoDbExecutor::json_to_document(update, "transaction update")?;
                let result = coll
                    .update_one(filter, mongo_wrap_update_bson(update))
                    .upsert(operation.contains("upsert"))
                    .session(&mut *session)
                    .await
                    .map_err(|err| format!("MongoDB native transaction update failed: {err}"))?;
                Ok((result.modified_count as i64).max(i64::from(result.upserted_id.is_some())))
            }
            "delete" | "delete_one" => {
                let filter = spec
                    .get("filter")
                    .ok_or_else(|| "transaction delete requires filter".to_string())?;
                let filter =
                    MongoDbExecutor::json_to_document(filter, "transaction delete filter")?;
                let result = coll
                    .delete_one(filter)
                    .session(&mut *session)
                    .await
                    .map_err(|err| format!("MongoDB native transaction delete failed: {err}"))?;
                Ok(result.deleted_count as i64)
            }
            other => Err(format!(
                "unsupported MongoDB transaction operation '{other}'"
            )),
        }
    }

    async fn ensure_resource(&self, resource_name: &str) -> Result<(), String> {
        match self.database().create_collection(resource_name).await {
            Ok(_) => Ok(()),
            Err(err) if err.to_string().contains("NamespaceExists") => Ok(()),
            Err(err) if err.to_string().contains("already exists") => Ok(()),
            Err(err) => Err(Self::classify_err(
                &format!("MongoDB native createCollection '{resource_name}' failed"),
                &err,
            )),
        }
    }

    async fn drop_resource(&self, resource_name: &str) -> Result<(), String> {
        self.collection(resource_name).drop().await.map_err(|err| {
            Self::classify_err(
                &format!("MongoDB native drop collection '{resource_name}' failed"),
                &err,
            )
        })
    }

    async fn list_resources(&self) -> Result<Vec<String>, String> {
        self.database()
            .list_collection_names()
            .await
            .map_err(|err| Self::classify_err("MongoDB native listCollections failed", &err))
    }
}

// ── BackendExecutor (runtime trait) ─────────────────────────────────────────────
// Phase D: MongoDbExecutor as stateless leaf I/O. Request shapes mirror the former
// inline arms in core.rs (`query_backend_target` / `mutate_backend_target`).

impl BackendHealth for MongoDbExecutor {
    async fn ping(&self) -> Result<(), String> {
        // Run { ping: 1 } command as a connectivity check.
        let result = self.run_command(json!({ "ping": 1 })).await?;
        // Atlas Data API returns {"document": {"ok": 1}} on success.
        let ok = result
            .get("document")
            .and_then(|d| d.get("ok"))
            .or_else(|| result.get("ok"))
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        if ok == 1.0 {
            Ok(())
        } else {
            Err(format!("MongoDB ping returned ok={ok}"))
        }
    }
}

impl QueryExecutor for MongoDbExecutor {
    /// `{"collection":"c","filter":{...},"projection":{...},"limit":N}`.
    async fn query(&self, request_json: &str) -> Result<String, tonic::Status> {
        let spec: Json = serde_json::from_str(request_json)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid request json: {e}")))?;
        let collection = spec
            .get("collection")
            .and_then(Json::as_str)
            .ok_or_else(|| {
                tonic::Status::invalid_argument("missing required field 'collection'")
            })?;
        if matches!(
            spec.get("operation").and_then(Json::as_str),
            Some("watch" | "watch_changes" | "change_stream")
        ) {
            #[cfg(feature = "mongodb-native")]
            {
                let pipeline = spec
                    .get("pipeline")
                    .and_then(Json::as_array)
                    .cloned()
                    .unwrap_or_default();
                let result = self
                    .watch_changes(collection, pipeline)
                    .await
                    .map_err(tonic::Status::internal)?;
                return Ok(result.to_string());
            }
            #[cfg(not(feature = "mongodb-native"))]
            return Err(tonic::Status::failed_precondition(
                "MongoDB change streams require the mongodb-native feature",
            ));
        }
        let filter = spec.get("filter").cloned().unwrap_or_else(|| json!({}));
        let projection = spec.get("projection").cloned().unwrap_or_else(|| json!({}));
        let limit = spec.get("limit").and_then(Json::as_i64).unwrap_or(100);
        let rows = self
            .find_documents(collection, filter, projection, limit)
            .await
            .map_err(tonic::Status::internal)?;
        serde_json::to_string(&rows).map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl MutationExecutor for MongoDbExecutor {
    /// `{"collection":"c","operation":"insert|upsert|update|delete", ...}`.
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status> {
        let spec: Json = serde_json::from_str(request_json)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid request json: {e}")))?;
        let collection = spec
            .get("collection")
            .and_then(Json::as_str)
            .ok_or_else(|| {
                tonic::Status::invalid_argument("missing required field 'collection'")
            })?;
        let operation = spec
            .get("operation")
            .and_then(Json::as_str)
            .ok_or_else(|| tonic::Status::invalid_argument("missing required field 'operation'"))?;
        match operation {
            "insert" | "insert_one" => {
                let document = spec
                    .get("document")
                    .cloned()
                    .ok_or_else(|| tonic::Status::invalid_argument("document is required"))?;
                let id = self
                    .insert_document(collection, document)
                    .await
                    .map_err(crate::runtime::executor_utils::status_from_store_string)?;
                Ok(json!({ "inserted_id": id }).to_string())
            }
            "insert_many" | "bulk_insert" => {
                let documents = spec
                    .get("documents")
                    .or_else(|| spec.get("docs"))
                    .and_then(Json::as_array)
                    .ok_or_else(|| tonic::Status::invalid_argument("documents must be an array"))?;
                let count = self
                    .insert_many(collection, documents)
                    .await
                    .map_err(crate::runtime::executor_utils::status_from_store_string)?;
                Ok(json!({ "affected_rows": count }).to_string())
            }
            "update" | "update_one" => {
                let filter = spec
                    .get("filter")
                    .cloned()
                    .ok_or_else(|| tonic::Status::invalid_argument("filter is required"))?;
                let update = spec
                    .get("update")
                    .or_else(|| spec.get("document"))
                    .cloned()
                    .ok_or_else(|| tonic::Status::invalid_argument("update is required"))?;
                let count = self
                    .update_document(collection, filter, update)
                    .await
                    .map_err(crate::runtime::executor_utils::status_from_store_string)?;
                Ok(json!({ "affected_rows": count }).to_string())
            }
            "update_many" | "bulk_update" => {
                let filter = spec
                    .get("filter")
                    .cloned()
                    .ok_or_else(|| tonic::Status::invalid_argument("filter is required"))?;
                let update = spec
                    .get("update")
                    .or_else(|| spec.get("document"))
                    .cloned()
                    .ok_or_else(|| tonic::Status::invalid_argument("update is required"))?;
                let count = self
                    .update_many(collection, filter, update)
                    .await
                    .map_err(crate::runtime::executor_utils::status_from_store_string)?;
                Ok(json!({ "affected_rows": count }).to_string())
            }
            "upsert" | "upsert_one" => {
                let filter = spec
                    .get("filter")
                    .cloned()
                    .ok_or_else(|| tonic::Status::invalid_argument("filter is required"))?;
                let update = spec
                    .get("update")
                    .or_else(|| spec.get("document"))
                    .cloned()
                    .ok_or_else(|| tonic::Status::invalid_argument("document is required"))?;
                let count = self
                    .upsert_document(collection, filter, update)
                    .await
                    .map_err(crate::runtime::executor_utils::status_from_store_string)?;
                Ok(json!({ "affected_rows": count }).to_string())
            }
            "delete" | "delete_one" => {
                let filter = spec
                    .get("filter")
                    .cloned()
                    .ok_or_else(|| tonic::Status::invalid_argument("filter is required"))?;
                let count = self
                    .delete_document(collection, filter)
                    .await
                    .map_err(crate::runtime::executor_utils::status_from_store_string)?;
                Ok(json!({ "affected_rows": count }).to_string())
            }
            "delete_many" | "bulk_delete" => {
                let filter = spec
                    .get("filter")
                    .cloned()
                    .ok_or_else(|| tonic::Status::invalid_argument("filter is required"))?;
                let count = self
                    .delete_many(collection, filter)
                    .await
                    .map_err(crate::runtime::executor_utils::status_from_store_string)?;
                Ok(json!({ "affected_rows": count }).to_string())
            }
            "create_indexes" | "ensure_indexes" => {
                let indexes = spec
                    .get("indexes")
                    .and_then(Json::as_array)
                    .ok_or_else(|| tonic::Status::invalid_argument("indexes must be an array"))?;
                self.ensure_indexes(collection, indexes)
                    .await
                    .map_err(tonic::Status::internal)?;
                Ok(json!({ "affected_rows": indexes.len() }).to_string())
            }
            other => Err(tonic::Status::invalid_argument(format!(
                "unsupported MongoDB mutation operation '{other}'"
            ))),
        }
    }
}

impl SearchExecutor for MongoDbExecutor {
    async fn search(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "mongodb does not support generic vector search dispatch",
        ))
    }
}

impl ObjectExecutor for MongoDbExecutor {
    async fn get_object(&self, _request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "mongodb is not an object store",
        ))
    }
    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "mongodb is not an object store",
        ))
    }
}

impl ResourceAdminExecutor for MongoDbExecutor {
    async fn ensure_resource(
        &self,
        resource_name: &str,
        spec_json: &str,
    ) -> Result<(), tonic::Status> {
        let spec: Json = serde_json::from_str(spec_json).unwrap_or_else(|_| json!({}));
        #[cfg(feature = "mongodb-native")]
        if let MongoDbTransport::Native(native) = &self.transport {
            native
                .ensure_resource(resource_name)
                .await
                .map_err(crate::runtime::executor_utils::status_from_store_string)?;
            let indexes = spec
                .get("indexes")
                .and_then(Json::as_array)
                .cloned()
                .unwrap_or_default();
            return self
                .ensure_indexes(resource_name, &indexes)
                .await
                .map_err(crate::runtime::executor_utils::status_from_store_string);
        }

        // createCollection is idempotent when the collection already exists
        // (returns {ok:1} even on NamespaceExists).
        self.run_command(json!({ "create": resource_name }))
            .await
            .map(|_| ())
            .map_err(tonic::Status::internal)?;
        let indexes = spec
            .get("indexes")
            .and_then(Json::as_array)
            .cloned()
            .unwrap_or_default();
        self.ensure_indexes(resource_name, &indexes)
            .await
            .map_err(tonic::Status::internal)
    }
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        #[cfg(feature = "mongodb-native")]
        if let MongoDbTransport::Native(native) = &self.transport {
            return native
                .drop_resource(resource_name)
                .await
                .map_err(crate::runtime::executor_utils::status_from_store_string);
        }

        self.run_command(json!({ "drop": resource_name }))
            .await
            .map(|_| ())
            .map_err(tonic::Status::internal)
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        #[cfg(feature = "mongodb-native")]
        if let MongoDbTransport::Native(native) = &self.transport {
            return native
                .list_resources()
                .await
                .map_err(crate::runtime::executor_utils::status_from_store_string);
        }

        let resp = self
            .run_command(json!({ "listCollections": 1, "nameOnly": true }))
            .await
            .map_err(tonic::Status::internal)?;
        let names = resp
            .get("document")
            .and_then(|d| d.get("cursor"))
            .and_then(|c| c.get("firstBatch"))
            .and_then(|b| b.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|item| item.get("name").and_then(|n| n.as_str()))
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default();
        Ok(names)
    }
}

impl BackendExecutor for MongoDbExecutor {
    async fn transaction(&self, _request_json: &str) -> Result<String, tonic::Status> {
        #[cfg(feature = "mongodb-native")]
        if let MongoDbTransport::Native(native) = &self.transport {
            let spec: Json = serde_json::from_str(_request_json).map_err(|err| {
                tonic::Status::invalid_argument(format!("invalid request json: {err}"))
            })?;
            let operations = spec
                .get("operations")
                .and_then(Json::as_array)
                .ok_or_else(|| tonic::Status::invalid_argument("operations must be an array"))?;
            let affected = native
                .execute_transaction(operations)
                .await
                .map_err(tonic::Status::internal)?;
            return Ok(
                json!({ "affected_rows": affected, "transaction": "committed" }).to_string(),
            );
        }

        Err(tonic::Status::failed_precondition(
            "MongoDB generic transactions require native transport via the mongodb-native feature",
        ))
    }
    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(build_probe(
            "mongodb",
            <Self as BackendHealth>::ping(self).await,
        ))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::backend_context::{AppliedContext, BackendContextEnforcer, ContextEffect};

    #[test]
    fn mongodb_executor_kind_and_name() {
        let cfg = MongoDbConfig {
            api_base: "http://localhost:27017".to_string(),
            api_key: None,
            database: "testdb".to_string(),
            is_cloud: false,
            dev_mode: true,
            timeout_secs: 30,
        };
        let exec = MongoDbExecutor::new(cfg);
        assert_eq!(exec.kind(), BackendKind::Mongodb);
        assert_eq!(exec.name(), "MongoDB");
    }

    #[test]
    fn mongodb_generic_dispatch_context_is_advisory() {
        let exec = MongoDbExecutor::new(MongoDbConfig {
            api_base: "http://localhost:27017".to_string(),
            api_key: None,
            database: "testdb".to_string(),
            is_cloud: false,
            dev_mode: true,
            timeout_secs: 30,
        });
        let ctx = AppliedContext {
            tenant_id: "tenant-a".into(),
            project_id: "project-a".into(),
            ..Default::default()
        };

        match BackendContextEnforcer::enforce(&exec, &ctx) {
            ContextEffect::Advisory { recorded_in } => {
                assert!(recorded_in.contains("generic command/filter dispatch"));
                assert!(recorded_in.contains("not verified"));
            }
            other => panic!("expected Advisory, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn mongodb_backend_executor_rejects_unsupported_and_malformed() {
        let exec = MongoDbExecutor::new(MongoDbConfig {
            api_base: "http://localhost:27017".to_string(),
            api_key: None,
            database: "testdb".to_string(),
            is_cloud: false,
            dev_mode: true,
            timeout_secs: 30,
        });
        assert!(SearchExecutor::search(&exec, "{}").await.is_err());
        assert!(ObjectExecutor::get_object(&exec, "{}").await.is_err());
        assert!(BackendExecutor::transaction(&exec, "{}").await.is_err());
        assert!(QueryExecutor::query(&exec, "not json").await.is_err());
        assert!(QueryExecutor::query(&exec, "{}").await.is_err()); // missing collection
        // missing operation
        assert!(
            MutationExecutor::mutate(&exec, r#"{"collection":"c"}"#)
                .await
                .is_err()
        );
    }

    #[test]
    fn mongodb_config_parses_dsn_host() {
        let host = MongoDbConfig::host_from_dsn("mongodb://user:pass@mongo.example.com:27017/mydb");
        assert_eq!(host.as_deref(), Some("mongo.example.com:27017"));
    }

    #[test]
    fn mongodb_config_parses_db_from_dsn() {
        let db = MongoDbConfig::db_from_dsn("mongodb://localhost:27017/inventory");
        assert_eq!(db.as_deref(), Some("inventory"));
    }

    #[test]
    fn mongodb_config_from_env_returns_none_without_vars() {
        // No env-vars set in test environment → returns None
        unsafe {
            env::remove_var("UDB_NOSQL_DSN");
            env::remove_var("UDB_NOSQL_API_URL");
        }
        assert!(MongoDbConfig::from_env().is_none());
    }

    #[test]
    fn mongodb_action_url_is_correct() {
        let cfg = MongoDbConfig {
            api_base: "https://data.mongodb-api.com/app/APP/endpoint/data/v1".to_string(),
            api_key: Some("key123".to_string()),
            database: "prod".to_string(),
            is_cloud: true,
            dev_mode: false,
            timeout_secs: 30,
        };
        let exec = MongoDbExecutor::new(cfg);
        assert_eq!(
            exec.action_url("insertOne"),
            "https://data.mongodb-api.com/app/APP/endpoint/data/v1/action/insertOne"
        );
    }

    #[cfg(feature = "mongodb-native")]
    #[tokio::test]
    async fn mongodb_native_executor_uses_native_transport() {
        let exec = MongoDbExecutor::new_native(MongoDbNativeConfig {
            dsn: "mongodb://localhost:27017/udb".to_string(),
            database: "udb".to_string(),
            timeout_secs: 1,
            app_name: None,
            max_pool_size: Some(4),
            direct_connection: Some(true),
            retry_writes: Some(true),
        })
        .await
        .expect("native mongodb executor should construct from a standard DSN");
        assert_eq!(exec.transport_kind(), "native");
    }
}
