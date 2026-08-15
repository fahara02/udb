//! Redis-backed canonical `SystemStores`.
//!
//! This implementation uses Redis as a real system-store backend, not as a
//! projection cache. It requires AOF persistence to be enabled before startup
//! registration succeeds. State is stored as JSON documents under stable UDB
//! keys, with Redis-native atomic primitives for sequences, leases, idempotent
//! enqueue, and claim ownership.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::dialect::projection_retry_delay_secs;
use super::system_store::{
    AdminAuditChainReport, AdminAuditInsert, AdminAuditListFilter, AdminAuditRow,
    CompensationStatus, DeadLetterGroup, MigrationAuditStore, MigrationOpInsert, MigrationOpRow,
    MigrationRunInsert, MigrationRunRow, MigrationRunState, MigrationRunsFilter, OpLedgerStatus,
    PendingTaskMetric, ProjectionClaimFilter, ProjectionTaskInsert, ProjectionTaskRow,
    ProjectionTaskStatus, ProjectionTaskStore, ProjectionTaskSummary, SagaInsert, SagaListFilter,
    SagaRow, SagaStatus, SagaStore, SagaSummary, SystemStoreError, SystemStoreResult,
    compute_admin_audit_hash, verify_admin_audit_chain_step,
};
use super::{CanonicalStore, DurabilityToken};

const REDIS_DURABILITY_POLL_MS: u64 = 10;
const REDIS_OUTBOX_EVENT_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const AUDIT_LOCK: &str = "admin-audit-chain";

pub struct RedisCanonicalStore {
    client: redis::Client,
    instance_name: String,
    prefix: String,
    conn: tokio::sync::OnceCell<redis::aio::MultiplexedConnection>,
}

impl RedisCanonicalStore {
    pub fn new(client: redis::Client, instance_name: impl Into<String>) -> Self {
        let instance_name = instance_name.into();
        let prefix = format!("udb:system:{instance_name}");
        Self {
            client,
            instance_name,
            prefix,
            conn: tokio::sync::OnceCell::new(),
        }
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, String> {
        let conn = self
            .conn
            .get_or_try_init(|| self.client.get_multiplexed_async_connection())
            .await
            .map_err(|err| format!("redis canonical connection failed: {err}"))?;
        Ok(conn.clone())
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.prefix)
    }

    fn projection_task_key(&self, id: Uuid) -> String {
        self.key(&format!("projection:task:{id}"))
    }

    fn saga_key(&self, id: Uuid) -> String {
        self.key(&format!("saga:{id}"))
    }

    fn audit_key(&self, id: Uuid) -> String {
        self.key(&format!("admin_audit:{id}"))
    }

    fn migration_run_key(&self, id: Uuid) -> String {
        self.key(&format!("migration:run:{id}"))
    }

    fn migration_op_key(&self, id: i64) -> String {
        self.key(&format!("migration:op:{id}"))
    }

    async fn require_durable_profile(&self) -> Result<(), String> {
        let mut conn = self.connection().await?;
        let info: String = redis::cmd("INFO")
            .arg("persistence")
            .query_async(&mut conn)
            .await
            .map_err(|err| format!("redis INFO persistence failed: {err}"))?;
        if info
            .lines()
            .any(|line| line.trim().eq_ignore_ascii_case("aof_enabled:1"))
        {
            Ok(())
        } else {
            Err(
                "redis canonical store requires AOF persistence (INFO persistence aof_enabled:1)"
                    .to_string(),
            )
        }
    }

    async fn get_json<T>(&self, key: &str) -> SystemStoreResult<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let mut conn = self
            .connection()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))?;
        let raw: Option<String> = conn
            .get(key)
            .await
            .map_err(|err| SystemStoreError::query("redis", format!("GET {key}"), err))?;
        raw.map(|value| {
            serde_json::from_str(&value).map_err(|err| {
                SystemStoreError::InvalidInput(format!("decode redis system JSON {key}: {err}"))
            })
        })
        .transpose()
    }

    async fn set_json<T>(&self, key: &str, value: &T) -> SystemStoreResult<()>
    where
        T: Serialize,
    {
        let raw = serde_json::to_string(value).map_err(|err| {
            SystemStoreError::InvalidInput(format!("encode redis system JSON {key}: {err}"))
        })?;
        let mut conn = self
            .connection()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))?;
        conn.set::<_, _, ()>(key, raw)
            .await
            .map_err(|err| SystemStoreError::query("redis", format!("SET {key}"), err))
    }

    async fn load_all<T>(&self, set_key: &str) -> SystemStoreResult<Vec<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let mut conn = self
            .connection()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))?;
        let keys: Vec<String> = conn
            .smembers(set_key)
            .await
            .map_err(|err| SystemStoreError::query("redis", format!("SMEMBERS {set_key}"), err))?;
        let mut rows = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(row) = self.get_json(&key).await? {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    async fn remove_claimable(&self, task_id: Uuid) -> SystemStoreResult<bool> {
        let mut conn = self
            .connection()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))?;
        let removed: i64 = conn
            .zrem(self.key("projection:claimable"), task_id.to_string())
            .await
            .map_err(|err| SystemStoreError::query("redis", "ZREM projection:claimable", err))?;
        Ok(removed > 0)
    }

    async fn add_claimable(&self, task_id: Uuid, when: DateTime<Utc>) -> SystemStoreResult<()> {
        let mut conn = self
            .connection()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))?;
        conn.zadd::<_, _, _, ()>(
            self.key("projection:claimable"),
            task_id.to_string(),
            when.timestamp_millis(),
        )
        .await
        .map_err(|err| SystemStoreError::query("redis", "ZADD projection:claimable", err))
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

#[async_trait]
impl CanonicalStore for RedisCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "redis"
    }

    fn instance_name(&self) -> &str {
        &self.instance_name
    }

    async fn current_durability_token(&self) -> Result<DurabilityToken, String> {
        let mut conn = self.connection().await?;
        let seq: i64 = conn.get(self.key("outbox:seq")).await.unwrap_or(0);
        Ok(DurabilityToken::new("redis", seq.to_string()))
    }

    async fn wait_for_token(
        &self,
        token: &DurabilityToken,
        timeout: Duration,
    ) -> Result<bool, String> {
        if !token.is_for("redis") {
            return Err(format!(
                "RedisCanonicalStore cannot wait on a '{}' token",
                token.backend_label
            ));
        }
        let target: i64 = token
            .value
            .parse()
            .map_err(|err| format!("invalid redis durability token '{}': {err}", token.value))?;
        let started = Instant::now();
        let poll = super::durability_poll_interval(timeout, REDIS_DURABILITY_POLL_MS);
        loop {
            let mut conn = self.connection().await?;
            let seq: i64 = conn.get(self.key("outbox:seq")).await.unwrap_or(0);
            if seq >= target {
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
        let mut conn = self.connection().await?;
        let seq: i64 = conn
            .incr(self.key("outbox:seq"), 1_i64)
            .await
            .map_err(|err| format!("redis outbox INCR failed: {err}"))?;
        let event = super::OutboxEvent {
            event_seq: seq,
            event_id: event_id.to_string(),
            topic: topic.to_string(),
            partition_key: partition_key.to_string(),
            payload: payload.clone(),
            created_at_unix_ms: Utc::now().timestamp_millis(),
        };
        let raw = serde_json::to_string(&event)
            .map_err(|err| format!("redis outbox encode failed: {err}"))?;
        redis::cmd("SET")
            .arg(self.key(&format!("outbox:event:{seq}")))
            .arg(raw)
            .arg("PX")
            .arg(REDIS_OUTBOX_EVENT_TTL.as_millis().max(1) as u64)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|err| format!("redis outbox SET failed: {err}"))?;
        Ok(seq)
    }

    async fn outbox_max_seq(&self) -> Result<i64, String> {
        let mut conn = self.connection().await?;
        Ok(conn.get(self.key("outbox:seq")).await.unwrap_or(0))
    }

    async fn ensure_system_tables(&self) -> Result<(), String> {
        self.require_durable_profile().await?;
        let mut conn = self.connection().await?;
        redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map(|_| ())
            .map_err(|err| format!("redis canonical PING failed: {err}"))
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
        let key = self.key(&format!("lease:{lease_name}"));
        let ttl_ms = ttl.as_millis() as u64;
        let mut conn = self.connection().await?;
        redis::Script::new(
            "local current = redis.call('GET', KEYS[1]); \
             if not current then \
               if tonumber(ARGV[2]) <= 0 then \
                 redis.call('DEL', KEYS[1]); \
               else \
                 redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2]); \
               end; \
               return 1; \
             end; \
             if current == ARGV[1] then \
               if tonumber(ARGV[2]) <= 0 then \
                 redis.call('DEL', KEYS[1]); \
               else \
                 redis.call('PEXPIRE', KEYS[1], ARGV[2]); \
               end; \
               return 1; \
             end; \
             return 0",
        )
        .key(key)
        .arg(owner_id)
        .arg(ttl_ms)
        .invoke_async::<i64>(&mut conn)
        .await
        .map(|value| value == 1)
        .map_err(|err| format!("redis lease acquire/refresh failed: {err}"))
    }

    async fn release_advisory_lease(&self, lease_name: &str, owner_id: &str) -> Result<(), String> {
        let key = self.key(&format!("lease:{lease_name}"));
        let mut conn = self.connection().await?;
        redis::Script::new(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then \
             return redis.call('DEL', KEYS[1]) else return 0 end",
        )
        .key(key)
        .arg(owner_id)
        .invoke_async::<i64>(&mut conn)
        .await
        .map(|_| ())
        .map_err(|err| format!("redis lease release failed: {err}"))
    }
}

#[async_trait]
impl ProjectionTaskStore for RedisCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "redis"
    }

    async fn ensure_projection_tables(&self) -> SystemStoreResult<()> {
        self.ensure_system_tables()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))
    }

    async fn enqueue_projection_task(
        &self,
        task: &ProjectionTaskInsert,
    ) -> SystemStoreResult<Uuid> {
        let idem_key = self.key(&format!("projection:idem:{}", task.idempotency_key));
        let mut conn = self
            .connection()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))?;
        let existing: Option<String> = conn
            .get(&idem_key)
            .await
            .map_err(|err| SystemStoreError::query("redis", format!("GET {idem_key}"), err))?;
        if let Some(existing) = existing {
            return Uuid::parse_str(&existing).map_err(|err| {
                SystemStoreError::InvalidInput(format!("bad redis task id: {err}"))
            });
        }
        let now = Utc::now();
        let task_id = Uuid::new_v4();
        let row = ProjectionTaskRow {
            task_id,
            idempotency_key: task.idempotency_key.clone(),
            project_id: task.project_id.clone(),
            manifest_checksum: task.manifest_checksum.clone(),
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
        let raw = serde_json::to_string(&row).map_err(|err| {
            SystemStoreError::InvalidInput(format!("encode redis projection task: {err}"))
        })?;
        let inserted: bool = redis::cmd("SET")
            .arg(&idem_key)
            .arg(task_id.to_string())
            .arg("NX")
            .query_async(&mut conn)
            .await
            .map_err(|err| SystemStoreError::query("redis", format!("SET NX {idem_key}"), err))?;
        if !inserted {
            let existing: String = conn
                .get(&idem_key)
                .await
                .map_err(|err| SystemStoreError::query("redis", format!("GET {idem_key}"), err))?;
            return Uuid::parse_str(&existing).map_err(|err| {
                SystemStoreError::InvalidInput(format!("bad redis task id: {err}"))
            });
        }
        redis::pipe()
            .atomic()
            .cmd("SET")
            .arg(&row_key)
            .arg(raw)
            .ignore()
            .cmd("SADD")
            .arg(self.key("projection:all"))
            .arg(&row_key)
            .ignore()
            .cmd("ZADD")
            .arg(self.key("projection:claimable"))
            .arg(now.timestamp_millis())
            .arg(task_id.to_string())
            .ignore()
            .query_async::<()>(&mut conn)
            .await
            .map_err(|err| SystemStoreError::query("redis", "projection enqueue pipeline", err))?;
        Ok(task_id)
    }

    async fn claim_projection_tasks(
        &self,
        filter: &ProjectionClaimFilter,
    ) -> SystemStoreResult<Vec<ProjectionTaskRow>> {
        let now_ms = Utc::now().timestamp_millis();
        let mut conn = self
            .connection()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))?;
        let scan_limit = (filter.batch_size.max(1) * 20).min(500);
        let ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(self.key("projection:claimable"))
            .arg("-inf")
            .arg(now_ms)
            .arg("LIMIT")
            .arg(0)
            .arg(scan_limit)
            .query_async(&mut conn)
            .await
            .map_err(|err| SystemStoreError::query("redis", "ZRANGEBYSCORE projection", err))?;
        let mut claimed = Vec::new();
        for id in ids {
            if claimed.len() >= filter.batch_size.max(1) as usize {
                break;
            }
            let Ok(task_id) = Uuid::parse_str(&id) else {
                continue;
            };
            let row_key = self.projection_task_key(task_id);
            let Some(mut row) = self.get_json::<ProjectionTaskRow>(&row_key).await? else {
                let _ = self.remove_claimable(task_id).await?;
                continue;
            };
            if !row.status.is_claimable()
                || row.retry_count >= filter.max_retries
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
            if !self.remove_claimable(task_id).await? {
                continue;
            }
            row.status = ProjectionTaskStatus::InProgress;
            row.updated_at = Utc::now();
            row.next_retry_at = None;
            self.set_json(&row_key, &row).await?;
            claimed.push(row);
        }
        Ok(claimed)
    }

    async fn mark_projection_task_completed(&self, task_id: Uuid) -> SystemStoreResult<()> {
        let row_key = self.projection_task_key(task_id);
        let Some(mut row) = self.get_json::<ProjectionTaskRow>(&row_key).await? else {
            return Ok(());
        };
        row.status = ProjectionTaskStatus::Completed;
        row.updated_at = Utc::now();
        row.completed_at = Some(row.updated_at);
        row.next_retry_at = None;
        self.set_json(&row_key, &row).await?;
        let _ = self.remove_claimable(task_id).await?;
        Ok(())
    }

    async fn mark_projection_task_failed(
        &self,
        task_id: Uuid,
        new_retry_count: i32,
        new_status: ProjectionTaskStatus,
        error: &str,
    ) -> SystemStoreResult<()> {
        if !matches!(
            new_status,
            ProjectionTaskStatus::Failed | ProjectionTaskStatus::DeadLetter
        ) {
            return Err(SystemStoreError::InvalidInput(format!(
                "mark_projection_task_failed only accepts FAILED or DEAD_LETTER, got {}",
                new_status.as_str()
            )));
        }
        let row_key = self.projection_task_key(task_id);
        let Some(mut row) = self.get_json::<ProjectionTaskRow>(&row_key).await? else {
            return Ok(());
        };
        row.status = new_status;
        row.retry_count = new_retry_count;
        row.last_error = error.to_string();
        row.updated_at = Utc::now();
        row.next_retry_at = (new_status == ProjectionTaskStatus::Failed).then(|| {
            row.updated_at + chrono::Duration::seconds(projection_retry_delay_secs(new_retry_count))
        });
        self.set_json(&row_key, &row).await?;
        if new_status == ProjectionTaskStatus::Failed {
            self.add_claimable(task_id, row.next_retry_at.unwrap_or(row.updated_at))
                .await?;
        } else {
            let _ = self.remove_claimable(task_id).await?;
        }
        Ok(())
    }

    async fn requeue_dead_letter_tasks(
        &self,
        target_backend: Option<&str>,
    ) -> SystemStoreResult<i64> {
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.key("projection:all")).await?;
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
            self.add_claimable(row.task_id, row.updated_at).await?;
            count += 1;
        }
        Ok(count)
    }

    async fn reset_stale_in_progress_tasks(&self, stale_after: Duration) -> SystemStoreResult<i64> {
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.key("projection:all")).await?;
        let cutoff = Utc::now() - chrono::Duration::from_std(stale_after).unwrap_or_default();
        let mut count = 0;
        for mut row in rows {
            if row.status != ProjectionTaskStatus::InProgress || row.updated_at > cutoff {
                continue;
            }
            row.status = ProjectionTaskStatus::Pending;
            row.last_error = "stale in-progress reconciliation".to_string();
            row.updated_at = Utc::now();
            self.set_json(&self.projection_task_key(row.task_id), &row)
                .await?;
            self.add_claimable(row.task_id, row.updated_at).await?;
            count += 1;
        }
        Ok(count)
    }

    async fn projection_task_summary(&self) -> SystemStoreResult<ProjectionTaskSummary> {
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.key("projection:all")).await?;
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
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.key("projection:all")).await?;
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
                )| {
                    PendingTaskMetric {
                        project_id,
                        target_backend,
                        target_instance,
                        projection_kind,
                        pending,
                        oldest_age_seconds,
                    }
                },
            )
            .collect())
    }

    async fn dead_letter_groups(&self, limit: i64) -> SystemStoreResult<Vec<DeadLetterGroup>> {
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.key("projection:all")).await?;
        let mut groups: BTreeMap<(String, String, String, String), i64> = BTreeMap::new();
        for row in rows.into_iter().filter(|row| {
            row.status == ProjectionTaskStatus::DeadLetter
                && !row
                    .last_error
                    .starts_with(super::system_store::PROJECTION_AUTHORITY_FAILURE_PREFIX)
        }) {
            *groups
                .entry((
                    row.project_id,
                    row.resource_name,
                    row.target_backend,
                    row.target_instance,
                ))
                .or_default() += 1;
        }
        Ok(groups
            .into_iter()
            .take(limit.max(0) as usize)
            .map(
                |((project_id, source_table, target_backend, target_instance), dead_count)| {
                    DeadLetterGroup {
                        project_id,
                        source_table,
                        target_backend,
                        target_instance,
                        dead_count,
                    }
                },
            )
            .collect())
    }

    async fn requeue_dead_letter_by_source(
        &self,
        project_id: &str,
        source_table: &str,
        target_backend: &str,
        target_instance: &str,
    ) -> SystemStoreResult<i64> {
        let rows: Vec<ProjectionTaskRow> = self.load_all(&self.key("projection:all")).await?;
        let mut count = 0;
        for mut row in rows {
            if row.status != ProjectionTaskStatus::DeadLetter
                || row
                    .last_error
                    .starts_with(super::system_store::PROJECTION_AUTHORITY_FAILURE_PREFIX)
                || row.project_id != project_id
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
            self.set_json(&self.projection_task_key(row.task_id), &row)
                .await?;
            self.add_claimable(row.task_id, row.updated_at).await?;
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
            let mut conn = self
                .connection()
                .await
                .map_err(|err| SystemStoreError::io("redis", err))?;
            let task_id: Option<String> = conn
                .get(self.key(&format!("projection:idem:{idem}")))
                .await
                .map_err(|err| SystemStoreError::query("redis", "GET projection idem", err))?;
            let Some(task_id) = task_id.and_then(|id| Uuid::parse_str(&id).ok()) else {
                continue;
            };
            let Some(row) = self
                .get_json::<ProjectionTaskRow>(&self.projection_task_key(task_id))
                .await?
            else {
                continue;
            };
            // P2-1 NF-1/NF-2: only COMPLETED clears the fence; FAILED/DEAD_LETTER
            // are not projected yet so they still count as pending.
            if !matches!(row.status, ProjectionTaskStatus::Completed) {
                count += 1;
            }
        }
        Ok(count)
    }
}

#[async_trait]
impl SagaStore for RedisCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "redis"
    }

    async fn ensure_saga_tables(&self) -> SystemStoreResult<()> {
        self.ensure_system_tables()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))
    }

    async fn record_saga(&self, saga: &SagaInsert) -> SystemStoreResult<Uuid> {
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
        let mut conn = self
            .connection()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))?;
        conn.sadd::<_, _, ()>(self.key("saga:all"), key)
            .await
            .map_err(|err| SystemStoreError::query("redis", "SADD saga:all", err))?;
        Ok(saga_id)
    }

    async fn get_saga(&self, saga_id: Uuid) -> SystemStoreResult<Option<SagaRow>> {
        self.get_json(&self.saga_key(saga_id)).await
    }

    async fn list_sagas(&self, filter: &SagaListFilter) -> SystemStoreResult<Vec<SagaRow>> {
        let mut rows: Vec<SagaRow> = self.load_all(&self.key("saga:all")).await?;
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
        let key = self.saga_key(saga_id);
        let Some(mut row) = self.get_json::<SagaRow>(&key).await? else {
            return Ok(());
        };
        if !matches!(
            row.status,
            SagaStatus::FailedCompensation | SagaStatus::ManualReview
        ) {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} is not in a retryable state (must be failed_compensation or manual_review)"
            )));
        }
        row.status = SagaStatus::Indeterminate;
        row.last_error.clear();
        row.retry_count += 1;
        row.compensation_status = CompensationStatus::RetryRequested;
        row.updated_at = Utc::now();
        self.set_json(&key, &row).await
    }

    async fn increment_recovery_attempts(
        &self,
        saga_id: Uuid,
        error: &str,
    ) -> SystemStoreResult<i64> {
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
        let mut rows: Vec<SagaRow> = self.load_all(&self.key("saga:all")).await?;
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
        let cutoff = Utc::now() - chrono::Duration::from_std(stale_after).unwrap_or_default();
        let rows: Vec<SagaRow> = self.load_all(&self.key("saga:all")).await?;
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
        let rows: Vec<SagaRow> = self.load_all(&self.key("saga:all")).await?;
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
impl super::system_store::AdminAuditStore for RedisCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "redis"
    }

    async fn ensure_admin_audit_tables(&self) -> SystemStoreResult<()> {
        self.ensure_system_tables()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))
    }

    async fn latest_admin_audit_hash(&self) -> SystemStoreResult<String> {
        let mut conn = self
            .connection()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))?;
        conn.get(self.key("admin_audit:latest_hash"))
            .await
            .map_err(|err| SystemStoreError::query("redis", "GET admin_audit:latest_hash", err))
            .map(|value: Option<String>| value.unwrap_or_default())
    }

    async fn append_admin_audit(&self, entry: &AdminAuditInsert) -> SystemStoreResult<Uuid> {
        let owner = Uuid::new_v4().to_string();
        let started = Instant::now();
        while !self
            .try_acquire_advisory_lease(AUDIT_LOCK, &owner, Duration::from_secs(10))
            .await
            .map_err(|err| SystemStoreError::io("redis", err))?
        {
            if started.elapsed() > Duration::from_secs(10) {
                return Err(SystemStoreError::io("redis", "admin audit lock timeout"));
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
            let mut conn = self
                .connection()
                .await
                .map_err(|err| SystemStoreError::io("redis", err))?;
            redis::pipe()
                .atomic()
                .cmd("RPUSH")
                .arg(self.key("admin_audit:order"))
                .arg(&key)
                .ignore()
                .cmd("SET")
                .arg(self.key("admin_audit:latest_hash"))
                .arg(current_hash)
                .ignore()
                .query_async::<()>(&mut conn)
                .await
                .map_err(|err| SystemStoreError::query("redis", "admin audit append", err))?;
            Ok::<_, SystemStoreError>(())
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
        let mut conn = self
            .connection()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))?;
        let keys: Vec<String> = conn
            .lrange(self.key("admin_audit:order"), 0, -1)
            .await
            .map_err(|err| SystemStoreError::query("redis", "LRANGE admin_audit:order", err))?;
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
        let mut conn = self
            .connection()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))?;
        let stop = limit.unwrap_or(-1);
        let end: isize = if stop > 0 {
            (stop - 1).try_into().unwrap_or(isize::MAX)
        } else {
            -1
        };
        let keys: Vec<String> = conn
            .lrange(self.key("admin_audit:order"), 0, end)
            .await
            .map_err(|err| SystemStoreError::query("redis", "LRANGE admin_audit:order", err))?;
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
impl MigrationAuditStore for RedisCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "redis"
    }

    async fn ensure_migration_audit_tables(&self) -> SystemStoreResult<()> {
        self.ensure_system_tables()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))
    }

    async fn start_migration_run(&self, run: &MigrationRunInsert) -> SystemStoreResult<Uuid> {
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
        let mut conn = self
            .connection()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))?;
        conn.sadd::<_, _, ()>(self.key("migration:runs"), key)
            .await
            .map_err(|err| SystemStoreError::query("redis", "SADD migration:runs", err))?;
        Ok(run_id)
    }

    async fn record_migration_op(&self, op: &MigrationOpInsert) -> SystemStoreResult<i64> {
        let mut conn = self
            .connection()
            .await
            .map_err(|err| SystemStoreError::io("redis", err))?;
        let id: i64 = conn
            .incr(self.key("migration:op_seq"), 1_i64)
            .await
            .map_err(|err| SystemStoreError::query("redis", "INCR migration:op_seq", err))?;
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
            applied_at: matches!(op.status, OpLedgerStatus::Applied).then(Utc::now),
        };
        let key = self.migration_op_key(id);
        self.set_json(&key, &row).await?;
        redis::pipe()
            .atomic()
            .cmd("SADD")
            .arg(self.key(&format!("migration:ops:{}", op.run_id)))
            .arg(&key)
            .ignore()
            .cmd("SADD")
            .arg(self.key("migration:ops:all"))
            .arg(&key)
            .ignore()
            .query_async::<()>(&mut conn)
            .await
            .map_err(|err| SystemStoreError::query("redis", "migration op indexes", err))?;
        Ok(id)
    }

    async fn finish_migration_run(
        &self,
        run_id: Uuid,
        new_state: MigrationRunState,
        error: &str,
    ) -> SystemStoreResult<()> {
        let key = self.migration_run_key(run_id);
        let Some(mut row) = self.get_json::<MigrationRunRow>(&key).await? else {
            return Err(SystemStoreError::InvalidInput(format!(
                "migration run {run_id} not found for finish_migration_run"
            )));
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
        let rows: Vec<MigrationOpRow> = self
            .load_all(&self.key(&format!("migration:ops:{run_id}")))
            .await?;
        let mut rows = rows;
        rows.sort_by_key(|row| row.operation_index);
        Ok(rows)
    }

    async fn list_migration_runs(
        &self,
        filter: &MigrationRunsFilter,
    ) -> SystemStoreResult<Vec<MigrationRunRow>> {
        let mut rows: Vec<MigrationRunRow> = self.load_all(&self.key("migration:runs")).await?;
        rows.retain(|row| Self::apply_migration_filter(row, filter));
        rows.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        Ok(rows
            .into_iter()
            .skip(filter.offset.max(0) as usize)
            .take(filter.limit.max(0) as usize)
            .collect())
    }
}
