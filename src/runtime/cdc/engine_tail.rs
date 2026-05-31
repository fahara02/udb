//! cdc.rs split — engine_tail (Phase I).
use super::*;

impl CdcEngine {
    // The CDC tailer publishes to Kafka, so construction requires the `kafka`
    // feature. Rust can't `cfg` a single parameter, so the `redis` idempotency
    // client is taken only when the `redis` feature is on (two variants).
    #[cfg(all(feature = "kafka", feature = "redis"))]
    pub fn new(
        pool: PgPool,
        redis: Option<redis::Client>,
        kafka_brokers: &str,
        dsn: String,
        metrics: std::sync::Arc<dyn MetricsRecorder>,
        config: CdcConfig,
    ) -> Result<Self, String> {
        let kafka_producer = Self::build_kafka_producer(kafka_brokers, &config)?;
        let (broadcast_tx, _) = broadcast::channel(1024);
        Ok(Self {
            pool,
            redis,
            kafka_producer,
            broadcast_tx,
            #[cfg(feature = "kafka")]
            dsn,
            metrics,
            config,
            topic_policies: Vec::new(),
        })
    }

    #[cfg(all(feature = "kafka", not(feature = "redis")))]
    pub fn new(
        pool: PgPool,
        kafka_brokers: &str,
        dsn: String,
        metrics: std::sync::Arc<dyn MetricsRecorder>,
        config: CdcConfig,
    ) -> Result<Self, String> {
        let kafka_producer = Self::build_kafka_producer(kafka_brokers, &config)?;
        let (broadcast_tx, _) = broadcast::channel(1024);
        Ok(Self {
            pool,
            kafka_producer,
            broadcast_tx,
            #[cfg(feature = "kafka")]
            dsn,
            metrics,
            config,
            topic_policies: Vec::new(),
        })
    }

    #[cfg(feature = "kafka")]
    fn build_kafka_producer(
        kafka_brokers: &str,
        config: &CdcConfig,
    ) -> Result<FutureProducer, String> {
        // U21: KafkaTransactional mode is no longer fail-closed. The
        // `kafka_tx` module builds the producer with `transactional.id`
        // set and calls `init_transactions` so the broker can fence the
        // previous producer epoch and abort any in-doubt transactions
        // from the prior process. The per-event publish path wraps each
        // send in begin/commit_transaction (see kafka_tx::run_in_transaction).
        if config.exactly_once_mode == CdcExactlyOnceMode::KafkaTransactional {
            let tx_cfg = super::kafka_tx::KafkaTxConfig::from_cdc_config(kafka_brokers, config);
            return super::kafka_tx::build_transactional_producer(&tx_cfg);
        }
        ClientConfig::new()
            .set("bootstrap.servers", kafka_brokers)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("compression.type", "lz4")
            .set("message.timeout.ms", "30000")
            .set("retries", "10")
            .set("retry.backoff.ms", "100")
            .create()
            .map_err(|e| e.to_string())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<CdcEnvelope> {
        self.broadcast_tx.subscribe()
    }

    /// U21 step 2: sweep in-doubt `publishing` rows from prior epochs.
    ///
    /// The supervisor MUST call this once after `new()` returns and
    /// before starting the publish loop. The construction path's
    /// `build_kafka_producer` already called `init_transactions`, so by
    /// the time we get here the broker has aborted any in-flight
    /// transactions from the previous epoch. Resetting `publishing`
    /// rows to `pending` lets the publish loop re-run them inside a
    /// fresh transaction.
    ///
    /// No-op when the mode is `AtLeastOnce` (no state-machine tracking)
    /// or `StateMachine` (no Kafka transaction; the state-machine
    /// already gates against duplicates). Only `KafkaTransactional`
    /// needs the broker-side abort + local sweep handshake.
    pub async fn run_indoubt_recovery_on_startup(&self) -> Result<u64, String> {
        if self.config.exactly_once_mode != CdcExactlyOnceMode::KafkaTransactional {
            return Ok(0);
        }
        let outbox_relation = self.config.outbox_relation();
        // 5-minute grace by default — well past Kafka's 60s default
        // transaction.timeout.ms, configurable via env.
        let grace_secs = std::env::var("UDB_CDC_INDOUBT_GRACE_SECS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(300);
        let reset = super::indoubt_recovery::reset_indoubt_publishing_rows(
            &self.pool,
            &outbox_relation,
            self.config.producer_epoch,
            grace_secs,
        )
        .await?;
        if reset > 0 {
            info!(
                "[cdc] in-doubt recovery: reset {} 'publishing' rows from prior epoch back to 'pending' \
                 (current producer_epoch={}, grace_secs={})",
                reset, self.config.producer_epoch, grace_secs
            );
        }
        Ok(reset)
    }

    pub async fn stream_cdc(
        &self,
        scopes: Vec<String>,
        topic_pattern: String,
        since_event_id: Option<String>,
    ) -> Result<
        Pin<
            Box<
                dyn tokio_stream::Stream<Item = Result<CdcEnvelope, tonic::Status>>
                    + Send
                    + 'static,
            >,
        >,
        tonic::Status,
    > {
        let allowed = scopes.iter().any(|scope| {
            scope == "udb:cdc:subscribe"
                || scope == "udb:cdc:read"
                || scope == "udb:*"
                || scope == "*"
        });
        if !allowed {
            return Err(tonic::Status::permission_denied(
                "Missing udb:cdc:read scope",
            ));
        }

        use async_stream::try_stream;

        let mut rx = self.broadcast_tx.subscribe();
        let matcher = WildMatch::new(&topic_pattern);
        let pool = self.pool.clone();
        let outbox_relation = self.config.outbox_relation();

        Ok(Box::pin(try_stream! {
            // 1. Replay historical events if since_event_id is provided
            // GAP 19: Cap replay at MAX_REPLAY_EVENTS to prevent full table scans on
            // first reconnect after a long outage. Caller should page via event_id cursor.
            const MAX_REPLAY_EVENTS: i64 = 10_000;
            if let Some(since_id) = since_event_id
                && let Ok(since_uuid) = Uuid::parse_str(&since_id)
            {
                    // Use a compound (created_at, event_id) comparison so that
                    // concurrent events inserted at the exact same microsecond as the
                    // anchor are not silently skipped by a strict `created_at >` filter.
                    let replay_sql = format!(
                        "SELECT event_id, topic, partition_key, payload, created_at
                         FROM {outbox_relation}
                         WHERE (created_at, event_id::TEXT) > (
                             SELECT created_at, event_id::TEXT
                             FROM {outbox_relation} WHERE event_id = $1
                         )
                         ORDER BY created_at ASC, event_id ASC
                         LIMIT {MAX_REPLAY_EVENTS}"
                    );
                    let mut rows = sqlx::query(&replay_sql)
                    .bind(since_uuid)
                    .fetch(&pool);

                    while let Some(row) = tokio_stream::StreamExt::next(&mut rows).await {
                        if let Ok(record) = row {
                            let topic: String = record.try_get("topic").unwrap_or_default();
                            if matcher.matches(&topic) {
                                let event_id: Uuid = record.try_get("event_id").unwrap_or_default();
                                let partition_key: String =
                                    record.try_get("partition_key").unwrap_or_default();
                                let payload: serde_json::Value =
                                    record.try_get("payload").unwrap_or_default();
                                let published_at: DateTime<Utc> =
                                    record.try_get("created_at").unwrap_or_else(|_| Utc::now());
                                yield CdcEnvelope {
                                    event_id: event_id.to_string(),
                                    topic,
                                    partition_key,
                                    payload_json: payload.to_string(),
                                    published_at,
                                };
                            }
                        }
                    }
            }

            // 2. Stream live events
            loop {
                match rx.recv().await {
                    Ok(envelope) => {
                        if matcher.matches(&envelope.topic) {
                            yield envelope;
                        }
                    }
                    // GAP 19: Emit a metric and warn when the broadcast channel drops
                    // events because this subscriber fell too far behind.
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("[cdc] PublishCDC subscriber lagged; {} events dropped", n);
                        // Use the existing CDC error counter — reason "broadcast_lagged".
                        // No arc clone needed: metrics is already Arc inside MetricsRecorder.
                        // We cannot call self.metrics here (inside a try_stream! closure),
                        // but we can call the pool-level metric via a channel-compatible path.
                        // For now, record via the tracing subscriber so Prometheus can scrape
                        // the warn log via a log-to-metric exporter (e.g. promtail + loki rule).
                        // TODO: pass a metrics handle into stream_cdc if a direct counter is needed.
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        }))
    }

    /// GAP 9: Replace crash-unsafe pg_try_advisory_lock with a row-level upsert lock.
    ///
    /// Advisory locks are silently released when the database connection drops (e.g.
    /// on process crash + immediate restart), allowing two CDC consumers to race.
    /// A row in `udb_cdc_lock_log` has an `acquired_at` timestamp that is heartbeated
    /// every 10 s; any row older than 30 s is considered stale and can be stolen.
    ///
    /// The upsert returns 1 affected row only when the lock was acquired (either
    /// freshly inserted or stolen from a stale holder); 0 rows means the lock is
    /// held by a live peer.
    #[cfg(feature = "kafka")]
    pub async fn run_advisory_lock_loop(&self) {
        let mut check_interval = interval(Duration::from_secs(30));
        let lock_rel = self.config.lock_log_relation();
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string());

        loop {
            check_interval.tick().await;

            match PgConnection::connect(&self.dsn).await {
                Ok(mut conn) => {
                    // GAP 9a: Upsert-based row-level lock.
                    // The WHERE clause steals the row only when the current holder has
                    // not heartbeated in the last 30 s (i.e. it crashed or was killed).
                    let upsert_sql = format!(
                        "INSERT INTO {lock_rel} (lock_key, holder_host, acquired_at)
                         VALUES ($1, $2, NOW())
                         ON CONFLICT (lock_key) DO UPDATE
                             SET holder_host = EXCLUDED.holder_host,
                                 acquired_at  = NOW()
                             WHERE {lock_rel}.acquired_at < NOW() - INTERVAL '30 seconds'"
                    );
                    let result = sqlx::query(&upsert_sql)
                        .bind(self.config.advisory_lock_key)
                        .bind(&hostname)
                        .execute(&mut conn)
                        .await;

                    match result {
                        Ok(r) if r.rows_affected() == 1 => {
                            info!("[cdc] row-level lock acquired — CDC tailer starting");
                            self.metrics.set_cdc_is_leader(&hostname, true);
                            self.run_tailer(conn).await;
                            self.metrics.set_cdc_is_leader(&hostname, false);
                        }
                        Ok(_) => {
                            warn!("[cdc] row-level lock held by live peer — CDC tailer inactive");
                        }
                        Err(e) => {
                            error!("[cdc] failed to acquire row-level lock: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("[cdc] failed to connect for lock loop: {}", e);
                }
            }
        }
    }

    #[cfg(feature = "kafka")]
    pub(crate) async fn run_tailer(&self, mut lock_conn: PgConnection) {
        info!("[cdc] logical replication tailer started");

        let mut heartbeat = interval(Duration::from_secs(10));
        let mut metrics_poll = interval(Duration::from_secs(5));
        let mut tail_loop = Box::pin(self.tail_replication_slot());
        let pool = self.pool.clone();
        let metrics = self.metrics.clone();
        let lock_rel = self.config.lock_log_relation();
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string());
        // GAP 9c: Exponential backoff for replication tailer restarts.
        let mut backoff_secs: u64 = 1;
        const MAX_BACKOFF_SECS: u64 = 60;

        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    // GAP 9b: Heartbeat the row-level lock instead of SELECT 1.
                    // If 0 rows are updated, another instance stole the lock → step down.
                    let update_sql = format!(
                        "UPDATE {lock_rel}
                         SET acquired_at = NOW()
                         WHERE lock_key = $1 AND holder_host = $2"
                    );
                    let hb = sqlx::query(&update_sql)
                        .bind(self.config.advisory_lock_key)
                        .bind(&hostname)
                        .execute(&mut lock_conn)
                        .await;
                    match hb {
                        Ok(r) if r.rows_affected() == 0 => {
                            error!("[cdc] row-level lock stolen by another instance; stepping down");
                            break;
                        }
                        Err(e) => {
                            error!("[cdc] lost connection during lock heartbeat: {}", e);
                            break;
                        }
                        _ => {}
                    }

                    // Paused-state check: query udb_cdc_control for this slot
                    {
                        use crate::runtime::system::SystemCatalogConfig;
                        let control_rel = SystemCatalogConfig::default().cdc_control_relation();
                        let paused: Option<(bool,)> = match sqlx::query_as(&format!(
                            "SELECT paused FROM {control_rel} WHERE slot_name = $1 LIMIT 1"
                        ))
                        .bind(&self.config.slot_name)
                        .fetch_optional(&pool)
                        .await
                        {
                            Ok(row) => row,
                            Err(err) => {
                                // Transient DB error — treat as "not paused" (safe-fail)
                                // so the tailer continues rather than silently stopping.
                                warn!(
                                    "[cdc] paused-state check failed (treating as not-paused): {}",
                                    err
                                );
                                None
                            }
                        };

                        if matches!(paused, Some((true,))) {
                            info!(
                                "[cdc] slot '{}' is paused via cdc_control; tailer stepping down",
                                self.config.slot_name
                            );
                            break;
                        }
                    }
                }
                _ = metrics_poll.tick() => {
                    // Poll outbox depth and lag
                    let metrics_sql = format!(
                        "SELECT COUNT(*) as depth, EXTRACT(EPOCH FROM (NOW() - MIN(created_at))) as lag_sec FROM {}",
                        self.config.outbox_relation()
                    );
                    let row: Result<(Option<i64>, Option<f64>), _> =
                        sqlx::query_as(&metrics_sql).fetch_one(&pool).await;

                    if let Ok((depth_opt, lag_opt)) = row {
                        if let Some(depth) = depth_opt {
                            metrics.set_cdc_outbox_depth(depth);
                        }
                        if let Some(lag) = lag_opt {
                            metrics.set_cdc_lag_seconds(lag);
                        } else {
                            metrics.set_cdc_lag_seconds(0.0);
                        }
                    }
                }
                res = &mut tail_loop => {
                    if let Err(e) = res {
                        // GAP 9c: Exponential backoff — doubles each restart, caps at 60 s.
                        error!(
                            "[cdc] replication tailer error: {}; restarting in {}s",
                            e, backoff_secs
                        );
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                        tail_loop = Box::pin(self.tail_replication_slot());
                    } else {
                        // Clean exit — reset backoff
                        backoff_secs = 1;
                    }
                }
            }
        }

        // Row-level lock expires naturally via the 30-second staleness window.
        // Explicitly clear acquired_at so the next instance can acquire immediately.
        let _ = sqlx::query(&format!(
            "UPDATE {} SET acquired_at = '1970-01-01' WHERE lock_key = $1 AND holder_host = $2",
            self.config.lock_log_relation()
        ))
        .bind(self.config.advisory_lock_key)
        .bind(&hostname)
        .execute(&mut lock_conn)
        .await;
    }

    /// PostgreSQL logical replication tailing.
    ///
    /// The published `tokio-postgres` crate does not expose the copy-both logical
    /// replication APIs required for this path. Until those APIs are available
    /// from crates.io, use the generic `CdcSource` path or an external sidecar to
    /// feed Kafka CDC events.
    #[cfg(feature = "kafka")]
    pub(crate) async fn tail_replication_slot(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Err("Postgres logical replication tailing requires copy-both APIs that are not exposed by the published tokio-postgres crate; use tail_source with a CDC sidecar/source adapter instead".into())
    }

    #[cfg(feature = "kafka")]
    pub(crate) async fn mark_cdc_delivery_state(
        &self,
        event_id: Uuid,
        state: &str,
        kafka_partition: Option<i32>,
        kafka_offset: Option<i64>,
        error_message: Option<&str>,
    ) {
        if self.config.exactly_once_mode == CdcExactlyOnceMode::AtLeastOnce {
            return;
        }
        let state_column = match state {
            "publishing" => "publishing_started_at",
            "published" => "published_at",
            "acked" => "acked_at",
            "dlq" => "dlq_at",
            _ => "publishing_started_at",
        };
        let outbox_sql = format!(
            "UPDATE {} SET delivery_state = $2, {state_column} = NOW(), \
             producer_epoch = $3, transactional_id = $4, \
             kafka_partition = COALESCE($5, kafka_partition), \
             kafka_offset = COALESCE($6, kafka_offset), \
             last_error = COALESCE($7, last_error) \
             WHERE event_id = $1",
            self.config.outbox_relation()
        );
        if let Err(err) = sqlx::query(&outbox_sql)
            .bind(event_id)
            .bind(state)
            .bind(self.config.producer_epoch)
            .bind(self.config.transactional_id())
            .bind(kafka_partition)
            .bind(kafka_offset)
            .bind(error_message)
            .execute(&self.pool)
            .await
        {
            warn!(
                "[cdc] failed to mark event {} as {} in outbox ledger: {}",
                event_id, state, err
            );
        }

        if matches!(state, "published" | "acked" | "dlq") {
            use crate::runtime::system::SystemCatalogConfig;
            let sys = SystemCatalogConfig::default();
            let journal_sql = format!(
                "UPDATE {} SET delivery_state = $2, \
                 producer_epoch = $3, transactional_id = $4, \
                 acked_at = CASE WHEN $2 = 'acked' THEN NOW() ELSE acked_at END, \
                 kafka_partition = COALESCE($5, kafka_partition), \
                 kafka_offset = COALESCE($6, kafka_offset), \
                 last_error = COALESCE($7, last_error) \
                 WHERE event_id = $1",
                sys.cdc_journal_relation()
            );
            if let Err(err) = sqlx::query(&journal_sql)
                .bind(event_id)
                .bind(state)
                .bind(self.config.producer_epoch)
                .bind(self.config.transactional_id())
                .bind(kafka_partition)
                .bind(kafka_offset)
                .bind(error_message)
                .execute(&self.pool)
                .await
            {
                warn!(
                    "[cdc] failed to mark event {} as {} in journal ledger: {}",
                    event_id, state, err
                );
            }
        }
    }

    /// Process a single outbox row, applying idempotency, validation, and Kafka produce
    #[cfg(feature = "kafka")]
    pub async fn process_outbox_event(
        &self,
        event_id: Uuid,
        topic: String,
        partition_key: String,
        payload_json: serde_json::Value,
        created_at: DateTime<Utc>,
        lsn: i64,
    ) {
        // 1. Idempotency Check
        let idempotency_key = format!("idempotency:udb:{}", event_id);
        let mut redis_conn = match &self.redis {
            Some(redis) => match redis.get_multiplexed_async_connection().await {
                Ok(conn) => Some(conn),
                Err(e) => {
                    warn!(
                        "[cdc] failed to connect to redis for idempotency, continuing without guard: {}",
                        e
                    );
                    None
                }
            },
            None => {
                warn!("[cdc] redis idempotency guard disabled; relying on Kafka idempotence");
                None
            }
        };

        if let Some(conn) = redis_conn.as_mut() {
            let set_nx: Result<bool, redis::RedisError> = redis::cmd("SET")
                .arg(&idempotency_key)
                .arg("1")
                .arg("NX")
                .arg("EX")
                .arg(604800) // 7 days
                .query_async(conn)
                .await;

            if let Ok(false) = set_nx {
                info!("[cdc] skipping duplicate event {}", event_id);
                self.metrics.inc_cdc_duplicate_skipped_total();
                self.mark_cdc_delivery_state(event_id, "acked", None, None, None)
                    .await;
                self.ack_event(event_id, lsn).await;
                return;
            }
        }

        // 2. Validate Event
        let envelope: Result<EventEnvelope, _> = serde_json::from_value(payload_json.clone());
        match envelope {
            Ok(mut env) => {
                if env.event_id != event_id.to_string() {
                    let error_message = format!(
                        "payload event_id {} does not match outbox event_id {}",
                        env.event_id, event_id
                    );
                    error!(
                        "[cdc] validation failed for event {}: {}",
                        event_id, error_message
                    );
                    self.metrics.inc_cdc_errors_total("validation");
                    self.metrics.inc_cdc_errors_total("dlq_routed");
                    if self
                        .route_to_dlq(
                            event_id,
                            payload_json,
                            "EnvelopeEventIdMismatch",
                            &error_message,
                        )
                        .await
                    {
                        self.ack_event(event_id, lsn).await;
                    } else if let Some(conn) = redis_conn.as_mut() {
                        let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                    }
                    return;
                }
                if env.schema_uri.is_none() {
                    env.schema_uri = self.config.schema_uri_for(&env.event_type);
                }

                // 2a. Phase 7: Topic policy enforcement — reject topics not in the allowlist.
                if !self.topic_policies.is_empty() && self.topic_policy_for(&topic).is_none() {
                    let error_message = format!(
                        "topic '{}' is not in the active topic policy allowlist",
                        topic
                    );
                    error!(
                        "[cdc] topic policy rejected event {}: {}",
                        event_id, error_message
                    );
                    self.metrics.inc_cdc_errors_total("topic_policy_rejected");
                    self.metrics.inc_cdc_errors_total("dlq_routed");
                    if self
                        .route_to_dlq(
                            event_id,
                            payload_json,
                            "TopicPolicyRejected",
                            &error_message,
                        )
                        .await
                    {
                        self.ack_event(event_id, lsn).await;
                    } else if let Some(conn) = redis_conn.as_mut() {
                        let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                    }
                    return;
                }

                // 2b. Phase 7: Schema registry validation (soft-fail — network errors allowed).
                let schema_uri_ref = env.schema_uri.as_deref().unwrap_or("");
                if let Err(reason) = self.validate_event_schema(schema_uri_ref).await {
                    let error_message =
                        format!("schema registry rejected '{}': {}", schema_uri_ref, reason);
                    error!(
                        "[cdc] schema validation rejected event {}: {}",
                        event_id, error_message
                    );
                    self.metrics
                        .inc_cdc_errors_total("schema_registry_rejected");
                    self.metrics.inc_cdc_errors_total("dlq_routed");
                    if self
                        .route_to_dlq(
                            event_id,
                            payload_json,
                            "SchemaRegistryRejected",
                            &error_message,
                        )
                        .await
                    {
                        self.ack_event(event_id, lsn).await;
                    } else if let Some(conn) = redis_conn.as_mut() {
                        let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                    }
                    return;
                }

                // 3. Produce to Kafka
                let payload_string = serde_json::to_string(&env).unwrap_or_default();
                self.mark_cdc_delivery_state(event_id, "publishing", None, None, None)
                    .await;

                if self.config.exactly_once_mode == CdcExactlyOnceMode::KafkaTransactional {
                    let timeout = Duration::from_secs(self.config.kafka_tx_timeout_secs.max(1));
                    match super::kafka_tx::run_in_transaction(
                        &self.kafka_producer,
                        timeout,
                        &topic,
                        &partition_key,
                        &payload_string,
                    )
                    .await
                    {
                        Ok(super::kafka_tx::KafkaTxPublishOutcome::Committed {
                            partition,
                            offset,
                        }) => {
                            self.finish_published_event(
                                event_id,
                                &topic,
                                &partition_key,
                                &payload_string,
                                created_at,
                                lsn,
                                partition,
                                offset,
                            )
                            .await;
                        }
                        Ok(super::kafka_tx::KafkaTxPublishOutcome::Aborted { reason }) => {
                            error!("[cdc] transactional kafka publish aborted: {}", reason);
                            self.metrics.inc_cdc_errors_total("transient");
                            self.mark_cdc_delivery_state(
                                event_id,
                                "pending",
                                None,
                                None,
                                Some(&reason),
                            )
                            .await;
                            if let Some(conn) = redis_conn.as_mut() {
                                let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                            }
                        }
                        Err(e) => {
                            error!("[cdc] transactional kafka publish failed: {:?}", e);
                            self.metrics.inc_cdc_errors_total("transient");
                            self.mark_cdc_delivery_state(
                                event_id,
                                "pending",
                                None,
                                None,
                                Some(&e.to_string()),
                            )
                            .await;
                            if let Some(conn) = redis_conn.as_mut() {
                                let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                            }
                        }
                    }
                } else {
                    let record = FutureRecord::to(&topic)
                        .key(&partition_key)
                        .payload(&payload_string);
                    match self
                        .kafka_producer
                        .send(record, Duration::from_secs(30))
                        .await
                    {
                        Ok((partition, offset)) => {
                            self.finish_published_event(
                                event_id,
                                &topic,
                                &partition_key,
                                &payload_string,
                                created_at,
                                lsn,
                                partition,
                                offset,
                            )
                            .await;
                        }
                        Err((e, _)) => {
                            error!("[cdc] failed to publish to kafka: {:?}", e);
                            self.metrics.inc_cdc_errors_total("transient");
                            self.mark_cdc_delivery_state(
                                event_id,
                                "pending",
                                None,
                                None,
                                Some(&e.to_string()),
                            )
                            .await;
                            // Transient errors: let it be retried by the tailer next time
                            // Remove from redis so it can be retried
                            if let Some(conn) = redis_conn.as_mut() {
                                let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!("[cdc] validation failed for event {}: {}", event_id, e);
                self.metrics.inc_cdc_errors_total("validation");
                self.metrics.inc_cdc_errors_total("dlq_routed");
                // DLQ Routing
                if self
                    .route_to_dlq(
                        event_id,
                        payload_json,
                        "SchemaValidationError",
                        &e.to_string(),
                    )
                    .await
                {
                    self.ack_event(event_id, lsn).await;
                } else if let Some(conn) = redis_conn.as_mut() {
                    let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                }
            }
        }
    }

    #[cfg(feature = "kafka")]
    async fn finish_published_event(
        &self,
        event_id: Uuid,
        topic: &str,
        partition_key: &str,
        payload_string: &str,
        created_at: DateTime<Utc>,
        lsn: i64,
        partition: i32,
        offset: i64,
    ) {
        info!(
            "[cdc] published {} to partition {} offset {}",
            event_id, partition, offset
        );

        let _ = self.broadcast_tx.send(CdcEnvelope {
            event_id: event_id.to_string(),
            topic: topic.to_string(),
            partition_key: partition_key.to_string(),
            payload_json: payload_string.to_string(),
            published_at: Utc::now(),
        });

        let publish_duration = (Utc::now() - created_at).num_milliseconds() as f64 / 1000.0;
        self.metrics
            .observe_cdc_publish_duration_seconds(publish_duration);
        self.metrics.inc_cdc_events_published_total(topic);
        self.mark_cdc_delivery_state(event_id, "published", Some(partition), Some(offset), None)
            .await;

        {
            use crate::runtime::system::SystemCatalogConfig;
            let sys = SystemCatalogConfig::default();
            let journal = sys.cdc_journal_relation();
            let _ = sqlx::query(&format!(
                "INSERT INTO {journal} \
                 (event_id, topic, partition_key, payload, published_at, kafka_partition, kafka_offset, delivery_state, producer_epoch, transactional_id) \
                 VALUES ($1, $2, $3, $4::JSONB, NOW(), $5, $6, 'published', $7, $8) \
                 ON CONFLICT (event_id) DO UPDATE SET \
                   delivery_state = 'published', \
                   kafka_partition = EXCLUDED.kafka_partition, \
                   kafka_offset = EXCLUDED.kafka_offset, \
                   producer_epoch = EXCLUDED.producer_epoch, \
                   transactional_id = EXCLUDED.transactional_id"
            ))
            .bind(event_id)
            .bind(topic)
            .bind(partition_key)
            .bind(payload_string)
            .bind(partition)
            .bind(offset)
            .bind(self.config.producer_epoch)
            .bind(self.config.transactional_id())
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!("[cdc] journal insert failed for event {}: {}", event_id, e);
            });
        }

        self.mark_cdc_delivery_state(event_id, "acked", Some(partition), Some(offset), None)
            .await;
        self.ack_event(event_id, lsn).await;
    }

    // ── D (2026-05-30): generic CdcSource tail ────────────────────────────
    //
    // The PG-specific `tail_replication_slot` keeps the WAL/pgoutput
    // machinery (replication mode connection, RELATION cache,
    // KeepAlive replies). For Mongo / MySQL / future sources, we
    // consume the abstract `CdcSource` trait — `source.open()`
    // returns a stream of `CdcEvent`s, the engine publishes each to
    // Kafka, and the resume offset is persisted in the existing
    // `udb_cdc_offsets` table keyed by the source's stable label.
    //
    // Resume: `source.open(last_offset)` lets each source's adapter
    // restart exactly where it left off (Mongo resume token; MySQL
    // binlog file:position). When `last_offset` is empty the source
    // chooses its own "tail from now" default.
    //
    // This does NOT replace `tail_replication_slot` — PG-specific
    // logical replication has its own dedicated path for backwards
    // compatibility + the schema-registry projection wiring. The
    // generic path is what Mongo / MySQL CDC use.

    /// D: generic CDC source tail. Consumes events from the given
    /// `CdcSource`, publishes them to Kafka using the configured
    /// topic mapping, and persists `source_offset` after each
    /// successful publish so a restart resumes from the last
    /// successfully-acked event.
    ///
    /// Returns `Ok(())` on graceful stream end (`open()` future
    /// completes); errors from the underlying source surface as
    /// `Err`. The caller is expected to run this in a supervisor
    /// that retries on transient failure.
    #[cfg(feature = "kafka")]
    pub async fn tail_source(
        &self,
        source: std::sync::Arc<dyn super::source::CdcSource>,
    ) -> Result<(), String> {
        use futures::StreamExt;

        let label = source.backend_label().to_string();
        // Use the source's backend label as the offset key. Multiple
        // sources from the same backend (e.g. two Mongo databases)
        // should expose distinct labels via the constructor.
        let slot_key = format!("cdc_source:{label}");
        let offsets_relation = self.config.offsets_relation();

        // 1. Load last persisted offset (empty if first run).
        let offset_sql = format!("SELECT last_offset FROM {offsets_relation} WHERE slot_name = $1");
        let from_offset: String = sqlx::query_scalar(&offset_sql)
            .bind(&slot_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| format!("cdc tail_source: offset load failed: {e}"))?
            .unwrap_or_default();

        // 2. Open the stream.
        let mut stream = source.open(&from_offset).await?;

        info!(
            "[cdc] generic source tail started: label={} from_offset={}",
            label,
            if from_offset.is_empty() {
                "<begin>"
            } else {
                &from_offset
            }
        );

        // 3. Drain. Publish each event to Kafka with the source's
        //    label as the topic; advance the persisted offset on
        //    successful ack. Kafka publish errors are NOT
        //    fatal — the loop continues; the supervisor retries.
        while let Some(evt_res) = stream.next().await {
            let evt = match evt_res {
                Ok(e) => e,
                Err(err) => {
                    error!("[cdc] tail_source stream error from {label}: {err}");
                    self.metrics.inc_cdc_errors_total("source_stream_error");
                    return Err(err);
                }
            };
            let topic = format!("udb.cdc.{}.{}", label, evt.source);
            let partition_key = evt
                .after
                .as_ref()
                .or(evt.before.as_ref())
                .and_then(|v| v.get("id").or_else(|| v.get("_id")))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{}:{}", evt.tenant_id, evt.source_offset));
            let payload_string = serde_json::to_string(&evt).unwrap_or_else(|_| "{}".to_string());

            let record = FutureRecord::to(&topic)
                .key(&partition_key)
                .payload(&payload_string);
            match self
                .kafka_producer
                .send(record, Duration::from_secs(30))
                .await
            {
                Ok((partition, kafka_offset)) => {
                    info!(
                        "[cdc] tail_source published {} → topic={} partition={} offset={}",
                        label, topic, partition, kafka_offset
                    );
                    self.metrics.inc_cdc_wal_messages_received_total();
                    // Persist the source offset so restart resumes.
                    let upsert_sql = format!(
                        "INSERT INTO {offsets_relation} (slot_name, last_offset, updated_at) \
                         VALUES ($1, $2, NOW()) \
                         ON CONFLICT (slot_name) DO UPDATE \
                           SET last_offset = EXCLUDED.last_offset, updated_at = NOW()"
                    );
                    if let Err(err) = sqlx::query(&upsert_sql)
                        .bind(&slot_key)
                        .bind(&evt.source_offset)
                        .execute(&self.pool)
                        .await
                    {
                        warn!(
                            "[cdc] tail_source offset persist failed for {}: {err}; \
                             event published but resume may replay",
                            slot_key
                        );
                    }
                }
                Err((kerr, _)) => {
                    error!("[cdc] tail_source kafka publish failed: {kerr:?}");
                    self.metrics.inc_cdc_errors_total("transient");
                    // Don't advance the offset — the next iteration
                    // (or the supervisor's restart) re-publishes.
                }
            }
        }

        info!("[cdc] generic source tail ended: label={label}");
        Ok(())
    }
}
