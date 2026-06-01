//! `ApiKeyService` handler over the UDB-owned API-key primitives.

use std::sync::Arc;

use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::apikey::entity::v1 as apikey_entity_pb;
use crate::proto::udb::core::apikey::services::v1 as apikey_pb;
use apikey_pb::api_key_service_server::ApiKeyService;

use crate::runtime::authn::{self, ApiKeyRecord, ApiKeyStore, AuthnConfig, UnavailableApiKeyStore};

use super::events::{self, AuthEvent, AuthEventSink, topics};
use super::mappings::{bounded_page_response, bounded_page_window, timestamp_from_unix};
use super::now_unix;

pub struct ApiKeyServiceImpl {
    api_keys: Arc<dyn ApiKeyStore>,
    config: AuthnConfig,
    event_sink: Arc<dyn AuthEventSink>,
    /// Direct pool for read-only usage aggregation over `api_key_usages`
    /// (the trait-based store does not expose analytic queries).
    pg_pool: Option<sqlx::PgPool>,
}

impl ApiKeyServiceImpl {
    pub fn new(config: AuthnConfig) -> Self {
        Self {
            api_keys: Arc::new(UnavailableApiKeyStore),
            config,
            event_sink: events::noop_sink(),
            pg_pool: None,
        }
    }

    pub fn with_store(config: AuthnConfig, api_keys: Arc<dyn ApiKeyStore>) -> Self {
        Self {
            api_keys,
            config,
            event_sink: events::noop_sink(),
            pg_pool: None,
        }
    }

    /// Attach the Postgres pool used for usage-stat aggregation.
    pub(crate) fn with_postgres(mut self, pool: Option<sqlx::PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    /// Attach the domain-event sink (outbox → Kafka). Defaults to a no-op.
    pub(crate) fn with_event_sink(mut self, sink: Arc<dyn AuthEventSink>) -> Self {
        self.event_sink = sink;
        self
    }

    fn hash_key(&self) -> Vec<u8> {
        self.config.api_key_hash_secret().as_bytes().to_vec()
    }

    async fn emit_event(&self, event: AuthEvent) {
        let topic = event.topic;
        if let Err(err) = self.event_sink.emit(event).await {
            tracing::warn!(topic, error = %err, "failed to publish apikey event");
        }
    }
}

fn expires_at_unix(ts: Option<prost_types::Timestamp>) -> u64 {
    ts.map(|ts| ts.seconds.max(0) as u64).unwrap_or(0)
}

fn status_for(rec: &ApiKeyRecord, now_unix: u64) -> apikey_entity_pb::ApiKeyStatus {
    if rec.is_revoked() {
        apikey_entity_pb::ApiKeyStatus::Revoked
    } else if rec.is_expired(now_unix) {
        apikey_entity_pb::ApiKeyStatus::Expired
    } else {
        apikey_entity_pb::ApiKeyStatus::Active
    }
}

fn api_key_to_pb(rec: &ApiKeyRecord, now_unix: u64) -> apikey_entity_pb::ApiKey {
    apikey_entity_pb::ApiKey {
        key_id: rec.key_prefix.clone(),
        key_prefix: rec.key_prefix.clone(),
        // Never return the stored key hash over the read API — the digest should
        // not leave the storage layer (defense-in-depth if the hash secret leaks).
        key_hash: String::new(),
        name: rec.key_prefix.clone(),
        description: String::new(),
        owner_type: apikey_entity_pb::ApiKeyOwnerType::ServiceAccount as i32,
        owner_id: rec.principal_id.clone(),
        scopes_json: serde_json::to_string(&rec.scopes).unwrap_or_else(|_| "[]".to_string()),
        status: status_for(rec, now_unix) as i32,
        ip_allowlist_json: "[]".to_string(),
        rate_limit_per_minute: 60,
        rate_limit_per_day: 10_000,
        created_by: String::new(),
        revoked_by: String::new(),
        revoke_reason: String::new(),
        expires_at: timestamp_from_unix(rec.expires_at_unix),
        last_used_at: timestamp_from_unix(rec.last_used_at_unix),
        created_at: timestamp_from_unix(rec.created_at_unix),
        updated_at: timestamp_from_unix(rec.created_at_unix),
        deleted_at: timestamp_from_unix(rec.revoked_at_unix),
        deleted_by: String::new(),
        tenant_id: rec.tenant_id.clone(),
        project_id: rec.project_id.clone(),
        allowed_resources_json: "[]".to_string(),
        metadata_json: serde_json::json!({
            "service_identity": rec.service_identity.clone(),
            "native_model": "udb.core.apikey.entity.v1.ApiKey"
        })
        .to_string(),
    }
}

#[tonic::async_trait]
impl ApiKeyService for ApiKeyServiceImpl {
    async fn create_api_key(
        &self,
        request: Request<apikey_pb::CreateApiKeyRequest>,
    ) -> Result<Response<apikey_pb::CreateApiKeyResponse>, Status> {
        if self.hash_key().is_empty() {
            return Err(Status::failed_precondition(
                "API key hashing requires UDB_SESSION_HASH_SECRET",
            ));
        }
        let req = request.into_inner();
        if req.owner_id.trim().is_empty() {
            return Err(Status::invalid_argument("owner_id is required"));
        }
        let prefix = format!(
            "udbk_{}",
            Uuid::new_v4()
                .simple()
                .to_string()
                .chars()
                .take(12)
                .collect::<String>()
        );
        let plain_key = format!("{}.{}", prefix, Uuid::new_v4().simple());
        let now = now_unix();
        let tenant_id = req
            .context
            .as_ref()
            .and_then(|ctx| ctx.tenant.as_ref())
            .map(|tenant| tenant.tenant_id.clone())
            .unwrap_or_default();
        let project_id = req
            .context
            .as_ref()
            .and_then(|ctx| ctx.tenant.as_ref())
            .map(|tenant| tenant.project_id.clone())
            .unwrap_or_default();
        let rec = ApiKeyRecord {
            key_prefix: authn::api_key_prefix(&plain_key),
            key_hash: authn::hash_secret(&plain_key, &self.hash_key()),
            principal_id: req.owner_id,
            service_identity: String::new(),
            tenant_id,
            project_id,
            scopes: req.scopes,
            created_at_unix: now,
            last_used_at_unix: 0,
            expires_at_unix: expires_at_unix(req.expires_at),
            revoked_at_unix: 0,
        };
        self.api_keys
            .put(rec.clone())
            .await
            .map_err(Status::internal)?;
        self.emit_event(AuthEvent::new(
            topics::API_KEY_CREATED,
            rec.key_prefix.clone(),
            rec.tenant_id.clone(),
            serde_json::json!({
                "key_id": rec.key_prefix.clone(),
                "key_prefix": rec.key_prefix.clone(),
                "owner_id": rec.principal_id.clone(),
                "scopes": rec.scopes.clone(),
                "tenant_id": rec.tenant_id.clone(),
                "project_id": rec.project_id.clone(),
            }),
        ))
        .await;
        Ok(Response::new(apikey_pb::CreateApiKeyResponse {
            key: Some(api_key_to_pb(&rec, now)),
            plain_key,
        }))
    }

    async fn get_api_key(
        &self,
        request: Request<apikey_pb::GetApiKeyRequest>,
    ) -> Result<Response<apikey_pb::GetApiKeyResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let key = self
            .api_keys
            .get_by_prefix(&req.key_id)
            .await
            .map_err(Status::internal)?
            .map(|rec| api_key_to_pb(&rec, now));
        Ok(Response::new(apikey_pb::GetApiKeyResponse { key }))
    }

    async fn list_api_keys(
        &self,
        request: Request<apikey_pb::ListApiKeysRequest>,
    ) -> Result<Response<apikey_pb::ListApiKeysResponse>, Status> {
        let req = request.into_inner();
        if req.owner_id.trim().is_empty() {
            return Err(Status::invalid_argument("owner_id is required"));
        }
        let now = now_unix();
        let status = apikey_entity_pb::ApiKeyStatus::try_from(req.status).unwrap_or_default();
        let page = req.page.as_ref();
        let (limit, offset, _) = bounded_page_window(page);
        let (records, total) = self
            .api_keys
            .list_for_principal_status_page(&req.owner_id, status, now, limit, offset)
            .await
            .map_err(Status::internal)?;
        let keys = records.iter().map(|rec| api_key_to_pb(rec, now)).collect();
        Ok(Response::new(apikey_pb::ListApiKeysResponse {
            keys,
            page: Some(bounded_page_response(total, page)),
        }))
    }

    async fn update_api_key(
        &self,
        request: Request<apikey_pb::UpdateApiKeyRequest>,
    ) -> Result<Response<apikey_pb::UpdateApiKeyResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let mut rec = self
            .api_keys
            .get_by_prefix(&req.key_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("api key not found"))?;
        if !req.scopes.is_empty() {
            rec.scopes = req.scopes;
        }
        if req.expires_at.is_some() {
            rec.expires_at_unix = expires_at_unix(req.expires_at);
        }
        self.api_keys
            .put(rec.clone())
            .await
            .map_err(Status::internal)?;
        self.emit_event(AuthEvent::new(
            topics::API_KEY_UPDATED,
            rec.key_prefix.clone(),
            String::new(),
            serde_json::json!({
                "key_id": req.key_id.clone(),
                "key_prefix": rec.key_prefix.clone(),
                "scopes": rec.scopes.clone(),
                "expires_at_unix": rec.expires_at_unix,
            }),
        ))
        .await;
        Ok(Response::new(apikey_pb::UpdateApiKeyResponse {
            key: Some(api_key_to_pb(&rec, now)),
        }))
    }

    async fn revoke_api_key(
        &self,
        request: Request<apikey_pb::RevokeApiKeyRequest>,
    ) -> Result<Response<apikey_pb::RevokeApiKeyResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let ok = self
            .api_keys
            .revoke(&req.key_id, now)
            .await
            .map_err(Status::internal)?;
        if !ok {
            return Err(Status::not_found("api key not found"));
        }
        self.emit_event(AuthEvent::new(
            topics::API_KEY_REVOKED,
            req.key_id.clone(),
            String::new(),
            serde_json::json!({
                "key_id": req.key_id.clone(),
                "key_prefix": req.key_id.clone(),
            }),
        ))
        .await;
        Ok(Response::new(apikey_pb::RevokeApiKeyResponse {
            key_id: req.key_id,
            revoked_at: timestamp_from_unix(now),
            operation_id: Uuid::new_v4().to_string(),
        }))
    }

    async fn validate_api_key(
        &self,
        request: Request<apikey_pb::ValidateApiKeyRequest>,
    ) -> Result<Response<apikey_pb::ValidateApiKeyResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let Some(rec) = authn::validate_api_key(
            self.api_keys.as_ref(),
            &req.plain_key,
            &self.hash_key(),
            now,
        )
        .await
        .map_err(Status::internal)?
        else {
            return Ok(Response::new(apikey_pb::ValidateApiKeyResponse {
                valid: false,
                ..Default::default()
            }));
        };
        // Best-effort last_used_at stamp on a successful match. Fire-and-forget
        // so it never adds latency to (or fails) the auth hot path.
        let _ = self.api_keys.touch_last_used(&rec.key_prefix, now).await;
        // Wildcard handling must match `SecurityContext::has_scope` (`*` and
        // `udb:*`) so an API key's scope semantics are uniform with the JWT/ABAC
        // path rather than only honoring a bare `*`.
        let scope_ok = req.required_scope.trim().is_empty()
            || rec
                .scopes
                .iter()
                .any(|scope| scope == "*" || scope == "udb:*" || scope == &req.required_scope);
        Ok(Response::new(apikey_pb::ValidateApiKeyResponse {
            valid: scope_ok,
            key_id: rec.key_prefix,
            owner_id: rec.principal_id,
            owner_type: apikey_entity_pb::ApiKeyOwnerType::ServiceAccount as i32,
            scopes: rec.scopes,
            rate_limited: false,
        }))
    }

    async fn get_api_key_usage_stats(
        &self,
        request: Request<apikey_pb::GetApiKeyUsageStatsRequest>,
    ) -> Result<Response<apikey_pb::GetApiKeyUsageStatsResponse>, Status> {
        use std::collections::BTreeMap;

        use crate::runtime::native_catalog::native_model;
        use sqlx::Row;

        let req = request.into_inner();
        let key_ref = req.key_id.trim();
        if key_ref.is_empty() {
            return Err(Status::invalid_argument("key_id is required"));
        }
        let Some(pool) = self.pg_pool.as_ref() else {
            return Err(Status::failed_precondition(
                "api-key usage stats require a Postgres backend",
            ));
        };

        // Resolve every column + the relation through the proto manifest so the
        // aggregation never hardcodes table/column names (proto is the source of
        // truth, per the native-service rule).
        let m = native_model(
            "udb.core.apikey.entity.v1.ApiKeyUsage",
            &[
                "key_id",
                "http_status",
                "latency_ms",
                "rate_limited",
                "requested_at",
            ],
        );
        // The API-key surface exposes the public `key_prefix` as the caller-facing
        // id (see `api_key_to_pb`), so `key_id` here is normally a prefix, not the
        // internal UUID FK on `api_key_usages`. Accept either: a UUID is matched
        // directly; otherwise resolve the prefix → UUID via the `api_keys` table.
        let ak = native_model(
            "udb.core.apikey.entity.v1.ApiKey",
            &["key_id", "key_prefix"],
        );
        let key_pred = if Uuid::parse_str(key_ref).is_ok() {
            format!("{key_id} = $1::UUID", key_id = m.q("key_id"))
        } else {
            format!(
                "{key_id} = (SELECT {ak_id} FROM {ak_rel} WHERE {ak_prefix} = $1 LIMIT 1)",
                key_id = m.q("key_id"),
                ak_id = ak.q("key_id"),
                ak_rel = ak.relation,
                ak_prefix = ak.q("key_prefix"),
            )
        };
        let from_secs = req.from.as_ref().map(|t| t.seconds);
        let to_secs = req.to.as_ref().map(|t| t.seconds);
        let sql = format!(
            "SELECT to_char(date_trunc('day', {req_at}), 'YYYY-MM-DD') AS day, \
                    COALESCE({status}, 0)::int AS status, \
                    COUNT(*)::bigint AS cnt, \
                    COUNT(*) FILTER (WHERE {rate_limited})::bigint AS rl, \
                    COALESCE(SUM({latency}), 0)::bigint AS lat_sum \
             FROM {rel} \
             WHERE {key_pred} \
               AND ($2::bigint IS NULL OR {req_at} >= to_timestamp($2)) \
               AND ($3::bigint IS NULL OR {req_at} <= to_timestamp($3)) \
             GROUP BY day, status \
             ORDER BY day",
            req_at = m.q("requested_at"),
            status = m.q("http_status"),
            rate_limited = m.q("rate_limited"),
            latency = m.q("latency_ms"),
            rel = m.relation,
            key_pred = key_pred,
        );
        let rows = sqlx::query(&sql)
            .bind(req.key_id.trim())
            .bind(from_secs)
            .bind(to_secs)
            .fetch_all(pool)
            .await
            .map_err(|err| Status::internal(format!("usage stats query failed: {err}")))?;

        // Fold (day, status) rows into one ApiKeyDailyStat per day.
        struct DayAgg {
            total: i64,
            rate_limited: i64,
            latency_sum: i64,
            status_counts: BTreeMap<String, i64>,
        }
        let mut by_day: BTreeMap<String, DayAgg> = BTreeMap::new();
        let mut overall_total: i64 = 0;
        for row in rows {
            let day: String = row.try_get("day").unwrap_or_default();
            let status: i32 = row.try_get("status").unwrap_or_default();
            let cnt: i64 = row.try_get("cnt").unwrap_or_default();
            let rl: i64 = row.try_get("rl").unwrap_or_default();
            let lat_sum: i64 = row.try_get("lat_sum").unwrap_or_default();
            overall_total += cnt;
            let agg = by_day.entry(day).or_insert(DayAgg {
                total: 0,
                rate_limited: 0,
                latency_sum: 0,
                status_counts: BTreeMap::new(),
            });
            agg.total += cnt;
            agg.rate_limited += rl;
            agg.latency_sum += lat_sum;
            if status != 0 {
                *agg.status_counts.entry(status.to_string()).or_insert(0) += cnt;
            }
        }
        let stats = by_day
            .into_iter()
            .map(|(date, agg)| apikey_pb::ApiKeyDailyStat {
                date,
                total_requests: agg.total,
                rate_limited_count: agg.rate_limited,
                avg_latency_ms: if agg.total > 0 {
                    agg.latency_sum as f64 / agg.total as f64
                } else {
                    0.0
                },
                status_counts: agg.status_counts.into_iter().collect(),
            })
            .collect();
        Ok(Response::new(apikey_pb::GetApiKeyUsageStatsResponse {
            stats,
            total_requests: overall_total,
        }))
    }
}
