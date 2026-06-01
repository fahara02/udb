//! cdc.rs split — engine_dlq (Phase I).
use super::*;

fn cdc_retry_delay_secs(delays: &[u64], retry_count: i32) -> i64 {
    let idx = retry_count.max(0) as usize;
    delays
        .get(idx)
        .copied()
        .or_else(|| delays.last().copied())
        .unwrap_or(60)
        .min(86_400) as i64
}

impl CdcEngine {
    #[cfg(feature = "kafka")]
    pub(crate) async fn ack_event(&self, event_id: Uuid, lsn: i64) -> bool {
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(e) => {
                error!("[cdc] failed to start tx for ack: {}", e);
                return false;
            }
        };

        let delete_sql = format!(
            "DELETE FROM {} WHERE event_id = $1",
            self.config.outbox_relation()
        );
        if let Err(e) = sqlx::query(&delete_sql)
            .bind(event_id)
            .execute(&mut *tx)
            .await
        {
            error!(
                "[cdc] failed to delete event {} from outbox (event may be replayed on restart): {}",
                event_id, e
            );
            // Abort the transaction; the offset will not advance, but at least
            // we don't silently commit a partially-applied ack.
            let _ = tx.rollback().await;
            return false;
        }

        let offsets_sql = format!(
            "INSERT INTO {} (slot_name, last_lsn, last_event_id, updated_at)
             VALUES ($1, $2, $3, NOW())
             ON CONFLICT (slot_name) DO UPDATE SET last_lsn = EXCLUDED.last_lsn, last_event_id = EXCLUDED.last_event_id, updated_at = NOW()",
            self.config.offsets_relation()
        );
        if let Err(e) = sqlx::query(&offsets_sql)
            .bind(&self.config.slot_name)
            .bind(lsn)
            .bind(event_id)
            .execute(&mut *tx)
            .await
        {
            error!(
                "[cdc] failed to update CDC offset for LSN {} (replay may restart from prior position): {}",
                lsn, e
            );
            let _ = tx.rollback().await;
            return false;
        }

        if self.config.exactly_once_mode != CdcExactlyOnceMode::AtLeastOnce {
            use crate::runtime::system::SystemCatalogConfig;
            let sys = SystemCatalogConfig::default();
            let journal_sql = format!(
                "UPDATE {} SET delivery_state = 'acked', acked_at = NOW(), \
                 producer_epoch = $2, transactional_id = $3 WHERE event_id = $1",
                sys.cdc_journal_relation()
            );
            if let Err(e) = sqlx::query(&journal_sql)
                .bind(event_id)
                .bind(self.config.producer_epoch)
                .bind(self.config.transactional_id())
                .execute(&mut *tx)
                .await
            {
                error!(
                    "[cdc] failed to mark event {} acked in journal (ack aborted): {}",
                    event_id, e
                );
                let _ = tx.rollback().await;
                return false;
            }
        }

        if let Err(e) = tx.commit().await {
            error!(
                "[cdc] failed to commit ack transaction for event {}: {}",
                event_id, e
            );
            return false;
        }
        true
    }

    #[cfg(feature = "kafka")]
    pub(crate) async fn route_to_dlq(
        &self,
        event_id: Uuid,
        payload: serde_json::Value,
        error_type: &str,
        error_message: &str,
    ) -> bool {
        let topic = payload
            .get("event_type")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string();
        let tenant_id = payload
            .get("tenant_id")
            .or_else(|| payload.get("tenant"))
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string();
        let project_id = payload
            .get("project_id")
            .or_else(|| payload.get("project"))
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string();
        let dlq_env = DlqEnvelope {
            failed_event: payload,
            failure_metadata: DlqMeta {
                service: "udb-cdc".to_string(),
                error_type: error_type.to_string(),
                error_message: error_message.to_string(),
                failed_at: Utc::now(),
                retry_count: 0,
            },
        };

        let payload_string = serde_json::to_string(&dlq_env).unwrap_or_default();
        let event_key = event_id.to_string();
        let record = FutureRecord::to(&self.config.dlq_topic)
            .key(&event_key)
            .payload(&payload_string);

        if let Err((e, _)) = self
            .kafka_producer
            .send(record, Duration::from_secs(30))
            .await
        {
            error!("[cdc] critical: failed to route to DLQ: {:?}", e);
            return false;
        }
        self.mark_cdc_delivery_state(event_id, "dlq", None, None, Some(error_message))
            .await;

        // Persist to Postgres DLQ table. This is part of the CDC contract:
        // operators need a durable queryable DLQ record in addition to the Kafka
        // DLQ topic. If the DB write fails, keep the outbox row unacked so the
        // event can be retried after the catalog/schema issue is fixed.
        {
            use crate::runtime::system::SystemCatalogConfig;
            let sys = SystemCatalogConfig::default();
            let dlq_rel = sys.dlq_relation();
            let retry_delay = cdc_retry_delay_secs(&self.config.retry_delay_secs, 0);
            if let Err(e) = sqlx::query(&format!(
                "INSERT INTO {dlq_rel} \
                 (event_id, topic, tenant_id, project_id, error_type, error_message, payload, status, next_retry_at, created_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7::JSONB, 'RETRYING', NOW() + ($8::TEXT || ' seconds')::INTERVAL, NOW()) \
                 ON CONFLICT (event_id) DO NOTHING"
            ))
            .bind(event_id)
            .bind(&topic)
            .bind(&tenant_id)
            .bind(&project_id)
            .bind(error_type)
            .bind(error_message)
            .bind(payload_string)
            .bind(retry_delay)
            .execute(&self.pool)
            .await
            {
                error!(
                    "[cdc] dlq postgres insert failed for event {}: {}",
                    event_id, e
                );
                return false;
            }
        }

        true
    }

    /// Phase 7: Load topic policies from `udb_system.udb_topic_policy` and cache them
    /// in this engine instance. Call once after construction; reload on SIGHUP if needed.
    pub async fn load_topic_policies(&mut self) -> Result<(), String> {
        use crate::runtime::system::SystemCatalogConfig;
        let sys = SystemCatalogConfig::default();
        let rel = sys.topic_policy_relation();

        let rows = sqlx::query(&format!(
            "SELECT policy_id, topic, tenant_id, owning_project, owning_service, schema_uri, \
             redaction_mode, redaction_version, retention_class, max_retry_attempts, retry_delay_secs, dlq_enabled, enabled \
             FROM {rel} WHERE enabled = TRUE ORDER BY policy_id ASC"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| format!("failed to load topic policies: {}", e))?;

        self.topic_policies = rows
            .iter()
            .map(|row| TopicPolicy {
                policy_id: row.try_get("policy_id").unwrap_or(0),
                topic: row.try_get("topic").unwrap_or_default(),
                tenant_id: row.try_get("tenant_id").unwrap_or_else(|_| "*".to_string()),
                owning_project: row.try_get("owning_project").unwrap_or_default(),
                owning_service: row.try_get("owning_service").unwrap_or_default(),
                schema_uri: row.try_get("schema_uri").unwrap_or_default(),
                redaction_mode: row
                    .try_get("redaction_mode")
                    .unwrap_or_else(|_| "mask".to_string()),
                redaction_version: row.try_get("redaction_version").unwrap_or(1),
                retention_class: row.try_get("retention_class").unwrap_or_default(),
                max_retry_attempts: row.try_get("max_retry_attempts").unwrap_or(3),
                retry_delay_secs: row.try_get("retry_delay_secs").unwrap_or_default(),
                dlq_enabled: row.try_get("dlq_enabled").unwrap_or(true),
                enabled: row.try_get("enabled").unwrap_or(true),
            })
            .collect();

        info!(
            "[cdc] loaded {} topic policies from {}",
            self.topic_policies.len(),
            rel
        );
        Ok(())
    }

    /// Phase 7: Look up the active policy for a given topic (exact match only).
    /// Returns `None` when the topic_policies list is empty (open allowlist).
    #[cfg(feature = "kafka")]
    pub(crate) fn topic_policy_for(&self, topic: &str) -> Option<&TopicPolicy> {
        if self.topic_policies.is_empty() {
            return None;
        }
        self.topic_policies
            .iter()
            .find(|p| p.topic == topic && p.enabled)
    }

    /// Phase 7: Validate an event's schema_uri against the configured schema registry.
    /// Returns `Ok(())` if the registry is not configured or the check passes.
    /// Returns `Err(reason)` when the schema is rejected, or when the registry
    /// is unavailable and `schema_registry_mode=fail_closed`.
    #[cfg(feature = "kafka")]
    pub(crate) async fn validate_event_schema(&self, schema_uri: &str) -> Result<(), String> {
        let registry_url = self.config.schema_registry_url.trim();
        if self.config.schema_registry_mode == SchemaRegistryMode::Off
            || registry_url.is_empty()
            || schema_uri.is_empty()
        {
            return Ok(());
        }

        let lookup_url = format!(
            "{}/subjects/{}/versions/latest",
            registry_url.trim_end_matches('/'),
            urlencoding::encode(schema_uri)
        );

        let mut request = reqwest::Client::new().get(&lookup_url);
        if !self.config.schema_registry_auth.is_empty() {
            request = request.header("Authorization", &self.config.schema_registry_auth);
        }

        match request.send().await {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(resp) if resp.status().as_u16() == 404 => {
                Err(format!("schema_uri '{}' not found in registry", schema_uri))
            }
            Ok(resp) => {
                let status = resp.status();
                let message = format!("schema registry returned {status} for '{schema_uri}'");
                if self.config.schema_registry_mode.fail_closed() {
                    Err(message)
                } else {
                    warn!("[cdc] {}; allowing event", message);
                    Ok(())
                }
            }
            Err(e) => {
                let message = format!("schema registry unreachable ({e})");
                if self.config.schema_registry_mode.fail_closed() {
                    Err(message)
                } else {
                    warn!("[cdc] {}; allowing event without validation", message);
                    Ok(())
                }
            }
        }
    }

    /// Phase 7: List DLQ events with filtering
    pub async fn list_dlq_events(
        &self,
        topic_filter: Option<String>,
        status_filter: Option<String>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<DlqEvent>, String> {
        use crate::runtime::system::SystemCatalogConfig;
        let sys = SystemCatalogConfig::default();
        let dlq_rel = sys.dlq_relation();

        // Parameterized filters (no string interpolation of caller-supplied
        // topic/status — this is an admin RPC). Placeholders are numbered as the
        // optional filters are appended.
        let mut query = format!(
            "SELECT dlq_id, event_id, topic, payload, error_type, error_message, \
             retry_count, last_retry_at, next_retry_at, status, created_at, updated_at \
             FROM {dlq_rel} WHERE 1=1"
        );
        let mut text_binds: Vec<String> = Vec::new();
        if let Some(topic) = &topic_filter {
            text_binds.push(topic.clone());
            query.push_str(&format!(" AND topic = ${}", text_binds.len()));
        }
        if let Some(status) = &status_filter {
            text_binds.push(status.clone());
            query.push_str(&format!(" AND status = ${}", text_binds.len()));
        }
        query.push_str(&format!(
            " ORDER BY created_at DESC LIMIT ${} OFFSET ${}",
            text_binds.len() + 1,
            text_binds.len() + 2,
        ));

        let mut q = sqlx::query(&query);
        for b in &text_binds {
            q = q.bind(b);
        }
        let rows = q
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| format!("failed to list DLQ events: {}", e))?;

        let mut events = Vec::new();
        for row in rows {
            events.push(DlqEvent {
                dlq_id: row.try_get("dlq_id").unwrap_or_default(),
                event_id: row.try_get("event_id").unwrap_or_default(),
                topic: row.try_get("topic").unwrap_or_default(),
                payload: row.try_get("payload").unwrap_or_default(),
                error_type: row.try_get("error_type").unwrap_or_default(),
                error_message: row.try_get("error_message").unwrap_or_default(),
                retry_count: row.try_get("retry_count").unwrap_or(0),
                last_retry_at: row.try_get("last_retry_at").ok(),
                next_retry_at: row.try_get("next_retry_at").ok(),
                status: row.try_get("status").unwrap_or_default(),
                created_at: row.try_get("created_at").unwrap_or_else(|_| Utc::now()),
                updated_at: row.try_get("updated_at").unwrap_or_else(|_| Utc::now()),
            });
        }

        Ok(events)
    }

    /// Phase 7: Replay a single DLQ event
    #[cfg(feature = "kafka")]
    pub async fn replay_dlq_event(&self, dlq_id: Uuid) -> Result<String, String> {
        use crate::runtime::system::SystemCatalogConfig;
        let sys = SystemCatalogConfig::default();
        let dlq_rel = sys.dlq_relation();

        // Fetch the DLQ event
        let row = sqlx::query(&format!(
            "SELECT event_id, topic, payload, retry_count FROM {dlq_rel} WHERE dlq_id = $1"
        ))
        .bind(dlq_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("failed to fetch DLQ event: {}", e))?
        .ok_or_else(|| "DLQ event not found".to_string())?;

        let event_id: Uuid = row.try_get("event_id").map_err(|e| e.to_string())?;
        let topic: String = row.try_get("topic").map_err(|e| e.to_string())?;
        let payload: serde_json::Value = row.try_get("payload").map_err(|e| e.to_string())?;
        let retry_count: i32 = match row.try_get("retry_count") {
            Ok(value) => value,
            Err(err) => {
                sqlx::query(&format!(
                    "UPDATE {dlq_rel}
                     SET status = 'QUARANTINED',
                         error_message = $1,
                         updated_at = NOW()
                     WHERE dlq_id = $2"
                ))
                .bind(format!("corrupt retry_count: {err}"))
                .bind(dlq_id)
                .execute(&self.pool)
                .await
                .map_err(|e| format!("failed to quarantine corrupt DLQ event: {e}"))?;
                return Err(format!("DLQ event {dlq_id} has corrupt retry_count: {err}"));
            }
        };

        // U22: decrypt any reversibly-encrypted fields before re-publishing so a
        // replayed event carries the original plaintext ("reversible for replay").
        // No-op when no key is configured (resolver None) or no fields are
        // encrypted; decrypt failures leave the envelope as-is (best-effort).
        let payload = match crate::runtime::cdc::encryption::StaticKeyResolver::from_env() {
            Some(resolver) => crate::runtime::cdc::decrypt_encrypted_json_fields(
                payload,
                &resolver,
                crate::runtime::cdc::encryption::DecryptScope::Replay,
            ),
            None => payload,
        };

        // Re-publish to Kafka
        let payload_string = payload.to_string();
        let event_key = event_id.to_string();
        let record = FutureRecord::to(&topic)
            .key(&event_key)
            .payload(&payload_string);

        self.kafka_producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(e, _)| format!("failed to republish event: {:?}", e))?;

        // Update DLQ status
        sqlx::query(&format!(
            "UPDATE {dlq_rel} SET status = 'REPLAYED', retry_count = $1, \
             last_retry_at = NOW(), updated_at = NOW() WHERE dlq_id = $2"
        ))
        .bind(retry_count + 1)
        .bind(dlq_id)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("failed to update DLQ status: {}", e))?;

        self.metrics.inc_cdc_dlq_replayed_total();
        Ok(format!("Event {} replayed successfully", event_id))
    }

    /// Phase 7: Replay DLQ events by topic and date range
    #[cfg(feature = "kafka")]
    pub async fn replay_dlq_by_topic(
        &self,
        topic: String,
        from_date: Option<DateTime<Utc>>,
        to_date: Option<DateTime<Utc>>,
        max_events: i64,
    ) -> Result<String, String> {
        use crate::runtime::system::SystemCatalogConfig;
        let sys = SystemCatalogConfig::default();
        let dlq_rel = sys.dlq_relation();

        let mut query = format!(
            "SELECT dlq_id FROM {dlq_rel} WHERE topic = $1 AND status IN ('OPEN','RETRYING') AND (next_retry_at IS NULL OR next_retry_at <= NOW())"
        );

        if from_date.is_some() {
            query.push_str(" AND created_at >= $2");
        }
        if to_date.is_some() {
            let param_num = if from_date.is_some() { 3 } else { 2 };
            query.push_str(&format!(" AND created_at <= ${}", param_num));
        }

        query.push_str(&format!(" ORDER BY created_at ASC LIMIT {}", max_events));

        let mut q = sqlx::query(&query).bind(&topic);
        if let Some(from) = from_date {
            q = q.bind(from);
        }
        if let Some(to) = to_date {
            q = q.bind(to);
        }

        let rows = q
            .fetch_all(&self.pool)
            .await
            .map_err(|e| format!("failed to fetch DLQ events: {}", e))?;

        let mut replayed = 0;
        let mut failed = 0;

        for row in rows {
            let dlq_id: Uuid = row.try_get("dlq_id").unwrap_or_default();
            match self.replay_dlq_event(dlq_id).await {
                Ok(_) => replayed += 1,
                Err(e) => {
                    error!("[cdc] failed to replay DLQ event {}: {}", dlq_id, e);
                    let _ = self.schedule_dlq_retry(dlq_id, &e).await;
                    failed += 1;
                }
            }
        }

        Ok(format!(
            "Replayed {} events, {} failed for topic {}",
            replayed, failed, topic
        ))
    }

    async fn schedule_dlq_retry(&self, dlq_id: Uuid, error: &str) -> Result<(), String> {
        use crate::runtime::system::SystemCatalogConfig;
        let sys = SystemCatalogConfig::default();
        let dlq_rel = sys.dlq_relation();
        let row = sqlx::query(&format!(
            "SELECT topic, retry_count FROM {dlq_rel} WHERE dlq_id = $1"
        ))
        .bind(dlq_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("failed to load DLQ retry state: {e}"))?;
        let (topic, retry_count) = match row {
            Some(row) => (
                row.try_get::<String, _>("topic").unwrap_or_default(),
                row.try_get::<i32, _>("retry_count").unwrap_or(0),
            ),
            None => return Err(format!("DLQ event {dlq_id} not found")),
        };
        let next_retry_count = retry_count + 1;
        let policy = self
            .topic_policies
            .iter()
            .find(|policy| policy.topic == topic && policy.enabled);
        let max_retry_attempts = policy
            .map(|policy| policy.max_retry_attempts.max(1))
            .unwrap_or_else(|| self.config.max_retry_attempts.max(1) as i32)
            .min(self.config.max_retry_attempts.max(1) as i32);
        if next_retry_count >= max_retry_attempts {
            sqlx::query(&format!(
                "UPDATE {dlq_rel}
                 SET status = 'QUARANTINED',
                     retry_count = $1,
                     error_message = $2,
                     last_retry_at = NOW(),
                     next_retry_at = NULL,
                     updated_at = NOW()
                 WHERE dlq_id = $3"
            ))
            .bind(next_retry_count)
            .bind(error)
            .bind(dlq_id)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("failed to quarantine DLQ event: {e}"))?;
            return Ok(());
        }
        let policy_delays = policy
            .map(|policy| {
                policy
                    .retry_delay_secs
                    .iter()
                    .filter_map(|delay| u64::try_from(*delay).ok())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let retry_delay = if policy_delays.is_empty() {
            cdc_retry_delay_secs(&self.config.retry_delay_secs, next_retry_count)
        } else {
            cdc_retry_delay_secs(&policy_delays, next_retry_count)
        };
        sqlx::query(&format!(
            "UPDATE {dlq_rel}
             SET status = 'RETRYING',
                 retry_count = $1,
                 error_message = $2,
                 last_retry_at = NOW(),
                 next_retry_at = NOW() + ($3::TEXT || ' seconds')::INTERVAL,
                 updated_at = NOW()
             WHERE dlq_id = $4"
        ))
        .bind(next_retry_count)
        .bind(error)
        .bind(retry_delay)
        .bind(dlq_id)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("failed to schedule DLQ retry: {e}"))?;
        Ok(())
    }

    /// Phase 7: Dismiss a DLQ event (mark as resolved without replay)
    pub async fn dismiss_dlq_event(&self, dlq_id: Uuid, reason: String) -> Result<String, String> {
        use crate::runtime::system::SystemCatalogConfig;
        let sys = SystemCatalogConfig::default();
        let dlq_rel = sys.dlq_relation();

        sqlx::query(&format!(
            "UPDATE {dlq_rel} SET status = 'DISMISSED', error_message = error_message || ' | Dismissed: ' || $1, \
             updated_at = NOW() WHERE dlq_id = $2"
        ))
        .bind(&reason)
        .bind(dlq_id)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("failed to dismiss DLQ event: {}", e))?;

        Ok(format!("DLQ event {} dismissed: {}", dlq_id, reason))
    }

    /// Phase 7: Mark a DLQ event as IGNORED — permanently suppressed, never replayed.
    /// Use when the event is known-bad and should not clutter the open queue or be retried.
    pub async fn mark_dlq_ignored(&self, dlq_id: Uuid, reason: String) -> Result<String, String> {
        use crate::runtime::system::SystemCatalogConfig;
        let sys = SystemCatalogConfig::default();
        let dlq_rel = sys.dlq_relation();

        let affected = sqlx::query(&format!(
            "UPDATE {dlq_rel} SET status = 'IGNORED', \
             error_message = error_message || ' | Ignored: ' || $1, \
             updated_at = NOW() \
             WHERE dlq_id = $2 AND status NOT IN ('REPLAYED')"
        ))
        .bind(&reason)
        .bind(dlq_id)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("failed to mark DLQ event as ignored: {}", e))?
        .rows_affected();

        if affected == 0 {
            return Err(format!(
                "DLQ event {} not found or already replayed",
                dlq_id
            ));
        }

        Ok(format!(
            "DLQ event {} marked as ignored: {}",
            dlq_id, reason
        ))
    }

    /// Phase 7: Get CDC monitoring metrics
    pub async fn get_cdc_metrics(&self) -> Result<CdcMetrics, String> {
        use crate::runtime::system::SystemCatalogConfig;
        let sys = SystemCatalogConfig::default();

        // Outbox depth and lag
        let outbox_sql = format!(
            "SELECT COUNT(*) as depth, \
             COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at))), 0) as lag_sec \
             FROM {}",
            self.config.outbox_relation()
        );
        let outbox_row: (i64, f64) = sqlx::query_as(&outbox_sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| format!("failed to fetch outbox metrics: {}", e))?;

        // DLQ depth by status
        let dlq_sql = format!(
            "SELECT status, COUNT(*) as count FROM {} GROUP BY status",
            sys.dlq_relation()
        );
        let dlq_rows = sqlx::query(&dlq_sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| format!("failed to fetch DLQ metrics: {}", e))?;

        let mut dlq_open = 0;
        let mut dlq_replayed = 0;
        let mut dlq_dismissed = 0;
        let mut dlq_quarantined = 0;

        for row in dlq_rows {
            let status: String = row.try_get("status").unwrap_or_default();
            let count: i64 = row.try_get("count").unwrap_or(0);
            match status.as_str() {
                "OPEN" => dlq_open = count,
                "REPLAYED" => dlq_replayed = count,
                "DISMISSED" => dlq_dismissed = count,
                "QUARANTINED" => dlq_quarantined = count,
                _ => {}
            }
        }
        // Push the current open depth to Prometheus so alerting rules can fire on it.
        self.metrics.set_cdc_dlq_depth(dlq_open);

        // WAL lag
        let wal_lag_sql = "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0) as lag_bytes \
             FROM pg_replication_slots WHERE slot_name = $1";
        let wal_lag: i64 = sqlx::query_scalar(wal_lag_sql)
            .bind(&self.config.slot_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| format!("failed to fetch WAL lag: {}", e))?
            .unwrap_or(0);

        // Events published by topic (last hour)
        let events_sql = format!(
            "SELECT topic, COUNT(*) as count FROM {} \
             WHERE published_at > NOW() - INTERVAL '1 hour' \
             GROUP BY topic ORDER BY count DESC LIMIT 20",
            sys.cdc_journal_relation()
        );
        let event_rows = sqlx::query(&events_sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| format!("failed to fetch event metrics: {}", e))?;

        let mut events_by_topic = HashMap::new();
        for row in event_rows {
            let topic: String = row.try_get("topic").unwrap_or_default();
            let count: i64 = row.try_get("count").unwrap_or(0);
            events_by_topic.insert(topic, count);
        }

        Ok(CdcMetrics {
            outbox_depth: outbox_row.0,
            outbox_lag_seconds: outbox_row.1,
            wal_lag_bytes: wal_lag,
            dlq_open,
            dlq_replayed,
            dlq_dismissed,
            dlq_quarantined,
            events_by_topic,
        })
    }
}
