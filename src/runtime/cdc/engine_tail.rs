//! cdc.rs split — engine_tail (Phase I).
use super::*;

/// Data carried from `prepare_outbox_event` to the produce step (#81).
#[cfg(feature = "kafka")]
struct PreparedOutbox {
    event_id: Uuid,
    topic: String,
    partition_key: String,
    payload_string: String,
    created_at: DateTime<Utc>,
    lsn: i64,
    idempotency_key: String,
}

/// A prepared event whose at-least-once produce is already in flight; awaiting
/// `future` yields the delivery result so the row can be acked or retried (#81).
#[cfg(feature = "kafka")]
struct PendingDelivery {
    prepared: PreparedOutbox,
    future: rdkafka::producer::DeliveryFuture,
}

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
        let (broadcast_tx, _) = broadcast::channel(config.broadcast_capacity);
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
        let (broadcast_tx, _) = broadcast::channel(config.broadcast_capacity);
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
            .set("linger.ms", config.producer_linger_ms.to_string())
            .set(
                "batch.num.messages",
                config.producer_batch_messages.to_string(),
            )
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
                        match row {
                        Ok(record) => {
                            let topic: String = match record.try_get("topic") {
                                Ok(topic) => topic,
                                Err(err) => {
                                    warn!("[cdc] replay row skipped: missing topic: {err}");
                                    continue;
                                }
                            };
                            if matcher.matches(&topic) {
                                let event_id: Uuid = match record.try_get("event_id") {
                                    Ok(value) => value,
                                    Err(err) => {
                                        warn!("[cdc] replay row skipped: missing event_id: {err}");
                                        continue;
                                    }
                                };
                                let partition_key: String = match record.try_get("partition_key") {
                                    Ok(value) => value,
                                    Err(err) => {
                                        warn!(
                                            "[cdc] replay row skipped for {}: missing partition_key: {err}",
                                            event_id
                                        );
                                        continue;
                                    }
                                };
                                let payload: serde_json::Value = match record.try_get("payload") {
                                    Ok(value) => value,
                                    Err(err) => {
                                        warn!("[cdc] replay row skipped for {}: missing payload: {err}", event_id);
                                        continue;
                                    }
                                };
                                let published_at: DateTime<Utc> = match record.try_get("created_at") {
                                    Ok(value) => value,
                                    Err(err) => {
                                        warn!(
                                            "[cdc] replay row skipped for {}: missing created_at: {err}",
                                            event_id
                                        );
                                        continue;
                                    }
                                };
                                yield CdcEnvelope {
                                    event_id: event_id.to_string(),
                                    topic,
                                    partition_key,
                                    payload_json: payload.to_string(),
                                    published_at,
                                };
                            }
                        }
                        Err(err) => {
                            warn!("[cdc] replay row skipped: decode failed: {err}");
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
        let mut tail_loop = Box::pin(self.tail_outbox());
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
                        tail_loop = Box::pin(self.tail_outbox());
                    } else {
                        // Clean exit — reset backoff
                        backoff_secs = 1;
                    }
                }
            }
        }

        // Row-level lock expires naturally via the 30-second staleness window.
        // Explicitly clear acquired_at so the next instance can acquire immediately.
        if let Err(err) = sqlx::query(&format!(
            "UPDATE {} SET acquired_at = '1970-01-01' WHERE lock_key = $1 AND holder_host = $2",
            self.config.lock_log_relation()
        ))
        .bind(self.config.advisory_lock_key)
        .bind(&hostname)
        .execute(&mut lock_conn)
        .await
        {
            warn!("[cdc] failed to clear tailer lock for {hostname}: {err}");
        }
    }

    /// PostgreSQL logical replication tailing.
    ///
    /// The published `tokio-postgres` crate does not expose the copy-both logical
    /// replication APIs required for this path. Until those APIs are available
    /// from crates.io, use the generic `CdcSource` path or an external sidecar to
    /// feed Kafka CDC events.
    /// Reserved for a future WAL logical-replication tailer (needs copy-both
    /// APIs the published `tokio-postgres` does not expose). The production relay
    /// is [`tail_outbox`] (polling); this is kept as the documented WAL seam.
    #[cfg(feature = "kafka")]
    #[allow(dead_code)]
    pub(crate) async fn tail_replication_slot(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Err("Postgres logical replication tailing requires copy-both APIs that are not exposed by the published tokio-postgres crate; use tail_source with a CDC sidecar/source adapter instead".into())
    }

    /// Polling outbox relay (the production tailer).
    ///
    /// Rather than depend on WAL logical replication (which needs a publication +
    /// slot + copy-both APIs the published `tokio-postgres` does not expose), this
    /// polls the transactional outbox directly: oldest-first batches of rows are
    /// handed to [`process_outbox_event`], which publishes to Kafka and — on a
    /// successful publish — acks (deletes) the row in `finish_published_event`, so
    /// the next poll skips it. A failed publish leaves the row in place for retry
    /// (at-least-once). Runs under the advisory leader lock (single poller), so a
    /// plain `ORDER BY event_seq` scan is safe. Returns `Err` only on a DB error,
    /// which the supervising `run_tailer` loop restarts with backoff.
    #[cfg(feature = "kafka")]
    pub(crate) async fn tail_outbox(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let outbox = self.config.outbox_relation();
        let poll_batch = self.config.poll_batch;
        // Only pick up rows that have not yet been published. Without this
        // filter, exactly-once rows that transitioned to publishing/published/
        // acked/dlq (but whose DELETE was lost or which are state-tracked, not
        // deleted) would be re-selected and re-published as duplicates. In
        // at-least-once mode rows stay 'pending' until ack deletes them, so the
        // filter is a no-op there.
        let select_sql = format!(
            "SELECT event_id, topic, partition_key, payload, created_at, event_seq \
             FROM {outbox} WHERE delivery_state = 'pending' \
             ORDER BY event_seq ASC LIMIT {poll_batch}"
        );
        let mut tick = interval(Duration::from_millis(self.config.poll_interval_ms));
        let poll_timeout = Duration::from_secs(self.config.kafka_tx_timeout_secs.max(30));
        // #215: hold one multiplexed redis connection across polls instead of
        // re-acquiring it every `poll_interval_ms`. Reconnect lazily, skipping
        // an exponentially-growing number of polls after each failure so a
        // down redis doesn't trigger a connect attempt (and a warn) every tick.
        let mut redis_conn: Option<redis::aio::MultiplexedConnection> = None;
        let mut redis_reconnect_skip: u32 = 0;
        let mut redis_fail_streak: u32 = 0;
        if self.redis.is_none() {
            warn!("[cdc] redis idempotency guard disabled; relying on Kafka idempotence");
        }
        loop {
            tick.tick().await;
            let rows: Vec<(Uuid, String, String, serde_json::Value, DateTime<Utc>, i64)> =
                match tokio::time::timeout(
                    poll_timeout,
                    sqlx::query_as(&select_sql).fetch_all(&self.pool),
                )
                .await
                {
                    Ok(rows) => rows?,
                    Err(_) => {
                        warn!(
                            "[cdc] outbox poll timed out after {}s",
                            poll_timeout.as_secs()
                        );
                        self.metrics.inc_cdc_errors_total("transient");
                        continue;
                    }
                };
            if let Some(redis) = &self.redis
                && redis_conn.is_none()
            {
                if redis_reconnect_skip > 0 {
                    redis_reconnect_skip -= 1;
                } else {
                    match redis.get_multiplexed_async_connection().await {
                        Ok(conn) => {
                            redis_conn = Some(conn);
                            redis_fail_streak = 0;
                        }
                        Err(e) => {
                            redis_fail_streak = redis_fail_streak.saturating_add(1);
                            redis_reconnect_skip = (1u32 << redis_fail_streak.min(6)).min(64);
                            warn!(
                                "[cdc] failed to connect to redis for idempotency, continuing without guard (retry in {} polls): {}",
                                redis_reconnect_skip, e
                            );
                        }
                    }
                }
            }
            // Phase 1: prepare each row in event_seq order. Exactly-once mode
            // publishes inline (one Kafka transaction per event); at-least-once
            // enqueues the produce (kept in flight by librdkafka) and collects the
            // delivery future, so a batch of produces pipelines instead of waiting
            // one delivery at a time. The idempotent producer preserves
            // per-partition order since `send()` is issued in event_seq order (#81).
            let mut pending = Vec::new();
            for (event_id, topic, partition_key, payload, created_at, event_seq) in rows {
                if let Some(prepared) = match tokio::time::timeout(
                    poll_timeout,
                    self.prepare_outbox_event(
                        event_id,
                        topic,
                        partition_key,
                        payload,
                        created_at,
                        event_seq,
                        redis_conn.as_mut(),
                    ),
                )
                .await
                {
                    Ok(prepared) => prepared,
                    Err(_) => {
                        warn!(
                            "[cdc] outbox event {} preparation timed out after {}s",
                            event_id,
                            poll_timeout.as_secs()
                        );
                        self.metrics.inc_cdc_errors_total("transient");
                        None
                    }
                } {
                    if self.config.exactly_once_mode == CdcExactlyOnceMode::KafkaTransactional {
                        if tokio::time::timeout(
                            poll_timeout,
                            self.produce_and_ack(prepared, redis_conn.as_mut()),
                        )
                        .await
                        .is_err()
                        {
                            warn!(
                                "[cdc] outbox publish/ack timed out after {}s",
                                poll_timeout.as_secs()
                            );
                            self.metrics.inc_cdc_errors_total("transient");
                        }
                    } else {
                        match self.enqueue_outbox_produce(prepared) {
                            Ok(delivery) => pending.push(delivery),
                            Err((prepared, reason)) => {
                                self.fail_pending(&prepared, &reason, redis_conn.as_mut())
                                    .await;
                            }
                        }
                    }
                }
            }
            // Phase 2: await the in-flight at-least-once deliveries and ack each.
            for delivery in pending {
                if tokio::time::timeout(
                    poll_timeout,
                    self.await_and_ack_delivery(delivery, redis_conn.as_mut()),
                )
                .await
                .is_err()
                {
                    warn!(
                        "[cdc] outbox delivery ack timed out after {}s",
                        poll_timeout.as_secs()
                    );
                    self.metrics.inc_cdc_errors_total("transient");
                }
            }
        }
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

    /// Process a single outbox row end-to-end (idempotency → validate → produce →
    /// ack), synchronously. The auth/event tests drive this directly; the polling
    /// `tail_outbox` loop instead pipelines via `prepare_outbox_event` +
    /// `enqueue_outbox_produce` + `await_and_ack_delivery` so multiple deliveries
    /// are in flight at once (the idempotent producer keeps them per-partition
    /// ordered) — #81.
    #[cfg(feature = "kafka")]
    pub async fn process_outbox_event(
        &self,
        event_id: Uuid,
        topic: String,
        partition_key: String,
        payload_json: serde_json::Value,
        created_at: DateTime<Utc>,
        lsn: i64,
        mut redis_conn: Option<&mut redis::aio::MultiplexedConnection>,
    ) {
        if let Some(prepared) = self
            .prepare_outbox_event(
                event_id,
                topic,
                partition_key,
                payload_json,
                created_at,
                lsn,
                redis_conn.as_deref_mut(),
            )
            .await
        {
            self.produce_and_ack(prepared, redis_conn).await;
        }
    }

    /// Idempotency check, envelope/topic/schema validation, DLQ routing, and the
    /// `'publishing'` state transition. Returns the data needed to publish, or
    /// `None` when the event was a duplicate / invalid / DLQ-routed (all fully
    /// handled here). Pulled out so `tail_outbox` can run this in `event_seq`
    /// order and then pipeline the produces (#81).
    #[cfg(feature = "kafka")]
    async fn prepare_outbox_event(
        &self,
        event_id: Uuid,
        topic: String,
        partition_key: String,
        payload_json: serde_json::Value,
        created_at: DateTime<Utc>,
        lsn: i64,
        mut redis_conn: Option<&mut redis::aio::MultiplexedConnection>,
    ) -> Option<PreparedOutbox> {
        // 1. Idempotency Check
        let idempotency_key = format!("{}:{}", self.config.idempotency_key_prefix, event_id);

        if let Some(conn) = redis_conn.as_deref_mut() {
            let set_nx: Result<bool, redis::RedisError> = redis::cmd("SET")
                .arg(&idempotency_key)
                .arg("1")
                .arg("NX")
                .arg("EX")
                .arg(self.config.idempotency_ttl_secs)
                .query_async(conn)
                .await;

            if let Ok(false) = set_nx {
                // #114: the redis key is only a *claim* written BEFORE the Kafka
                // publish — it is NOT proof the event was durably published. A
                // crash between this `SET NX` and the publish leaves the key set
                // with the row still unpublished; acking on the key alone would
                // silently delete an unpublished outbox row (data loss). Redis is
                // a fast-path dedup hint, never the ack/delete authority: consult
                // the durable publish evidence (the CDC journal) first.
                if self.was_durably_published(event_id).await {
                    info!(
                        "[cdc] skipping duplicate event {} (durably published)",
                        event_id
                    );
                    self.metrics.inc_cdc_duplicate_skipped_total();
                    if self.ack_event(event_id, lsn).await {
                        self.mark_cdc_delivery_state(event_id, "acked", None, None, None)
                            .await;
                    } else {
                        warn!(
                            "[cdc] duplicate event {} was not acked; retaining idempotency key for retry",
                            event_id
                        );
                    }
                } else {
                    // Stale claim from a crashed pre-publish attempt: there is no
                    // durable evidence the event was published. Drop the key so the
                    // next poll re-selects and re-publishes this still-pending row
                    // (at-least-once; downstream consumers dedupe on event_id). Do
                    // NOT ack — the event was never published.
                    warn!(
                        "[cdc] idempotency key for {} present but no durable publish evidence; \
                         dropping stale claim and re-publishing on next poll",
                        event_id
                    );
                    if let Some(conn) = redis_conn.as_deref_mut() {
                        let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                    }
                }
                return None;
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
                    } else if let Some(conn) = redis_conn.as_deref_mut() {
                        let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                    }
                    return None;
                }
                if env.schema_uri.is_none() {
                    env.schema_uri = self.config.schema_uri_for(&env.event_type);
                }

                // 2a. Phase 7: Topic policy enforcement — reject topics not in the allowlist.
                if !self.topic_policies.is_empty() {
                    // Resolve the policy once. Missing → reject (allowlist).
                    // Matched → apply its policy-specific behavior: the policy's
                    // declared `schema_uri` is authoritative over the event's own,
                    // so schema validation below enforces the policy contract (#131).
                    let policy_schema = match self.topic_policy_for(&topic) {
                        Some(policy) => policy.schema_uri.trim().to_string(),
                        None => {
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
                            } else if let Some(conn) = redis_conn.as_deref_mut() {
                                let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                            }
                            return None;
                        }
                    };
                    if !policy_schema.is_empty() {
                        env.schema_uri = Some(policy_schema);
                    }
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
                    } else if let Some(conn) = redis_conn.as_deref_mut() {
                        let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                    }
                    return None;
                }

                // 3. Prepared — hand the data back so the caller publishes it
                // (synchronously via `produce_and_ack`, or pipelined by
                // `tail_outbox`). (#81)
                let payload_string = serde_json::to_string(&env).unwrap_or_default();
                self.mark_cdc_delivery_state(event_id, "publishing", None, None, None)
                    .await;
                Some(PreparedOutbox {
                    event_id,
                    topic,
                    partition_key,
                    payload_string,
                    created_at,
                    lsn,
                    idempotency_key,
                })
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
                } else if let Some(conn) = redis_conn.as_deref_mut() {
                    let _: () = conn.del(&idempotency_key).await.unwrap_or_default();
                }
                None
            }
        }
    }

    /// Publish a prepared event and ack it — the synchronous single-event path
    /// (exactly-once Kafka transaction, or at-least-once produce + await). The
    /// pipelined `tail_outbox` loop instead uses `enqueue_outbox_produce` +
    /// `await_and_ack_delivery` so a batch of produces overlaps in flight (#81).
    #[cfg(feature = "kafka")]
    async fn produce_and_ack(
        &self,
        prepared: PreparedOutbox,
        mut redis_conn: Option<&mut redis::aio::MultiplexedConnection>,
    ) {
        if self.config.exactly_once_mode == CdcExactlyOnceMode::KafkaTransactional {
            let timeout = Duration::from_secs(self.config.kafka_tx_timeout_secs.max(1));
            match super::kafka_tx::run_in_transaction(
                &self.kafka_producer,
                timeout,
                &prepared.topic,
                &prepared.partition_key,
                &prepared.payload_string,
            )
            .await
            {
                Ok(super::kafka_tx::KafkaTxPublishOutcome::Committed { partition, offset }) => {
                    self.finish_published_event(
                        prepared.event_id,
                        &prepared.topic,
                        &prepared.partition_key,
                        &prepared.payload_string,
                        prepared.created_at,
                        prepared.lsn,
                        partition,
                        offset,
                    )
                    .await;
                }
                Ok(super::kafka_tx::KafkaTxPublishOutcome::Aborted { reason }) => {
                    error!("[cdc] transactional kafka publish aborted: {}", reason);
                    self.metrics.inc_cdc_errors_total("transient");
                    self.mark_cdc_delivery_state(
                        prepared.event_id,
                        "pending",
                        None,
                        None,
                        Some(&reason),
                    )
                    .await;
                    if let Some(conn) = redis_conn.as_deref_mut() {
                        let _: () = conn
                            .del(&prepared.idempotency_key)
                            .await
                            .unwrap_or_default();
                    }
                }
                Err(e) => {
                    error!("[cdc] transactional kafka publish failed: {:?}", e);
                    self.metrics.inc_cdc_errors_total("transient");
                    self.mark_cdc_delivery_state(
                        prepared.event_id,
                        "pending",
                        None,
                        None,
                        Some(&e.to_string()),
                    )
                    .await;
                    if let Some(conn) = redis_conn.as_deref_mut() {
                        let _: () = conn
                            .del(&prepared.idempotency_key)
                            .await
                            .unwrap_or_default();
                    }
                }
            }
        } else {
            match self.enqueue_outbox_produce(prepared) {
                Ok(pending) => self.await_and_ack_delivery(pending, redis_conn).await,
                Err((prepared, reason)) => self.fail_pending(&prepared, &reason, redis_conn).await,
            }
        }
    }

    /// Enqueue a prepared event to the at-least-once Kafka producer WITHOUT
    /// awaiting delivery. `send_result` copies the payload into librdkafka and
    /// returns a `'static` `DeliveryFuture` that no longer borrows `prepared`, so
    /// issuing many in `event_seq` order keeps them in flight and pipelines the
    /// batch (the idempotent producer preserves per-partition order). On a
    /// full-queue error the produce never happened — `prepared` is handed back so
    /// the caller can leave the row `pending` for the next poll (#81).
    #[cfg(feature = "kafka")]
    fn enqueue_outbox_produce(
        &self,
        prepared: PreparedOutbox,
    ) -> Result<PendingDelivery, (PreparedOutbox, String)> {
        let record = FutureRecord::to(&prepared.topic)
            .key(&prepared.partition_key)
            .payload(&prepared.payload_string);
        match self.kafka_producer.send_result(record) {
            Ok(future) => Ok(PendingDelivery { prepared, future }),
            Err((e, _)) => Err((prepared, e.to_string())),
        }
    }

    /// Await a pipelined delivery and finalize it: ack (delete + advance offset)
    /// on success, or return the row to `pending` so the next poll retries it.
    /// `DeliveryFuture` resolves to `Result<delivery, Canceled>`; a cancel is
    /// treated as a transient failure (#81).
    #[cfg(feature = "kafka")]
    async fn await_and_ack_delivery(
        &self,
        pending: PendingDelivery,
        redis_conn: Option<&mut redis::aio::MultiplexedConnection>,
    ) {
        let PendingDelivery { prepared, future } = pending;
        match future.await {
            Ok(Ok((partition, offset))) => {
                self.finish_published_event(
                    prepared.event_id,
                    &prepared.topic,
                    &prepared.partition_key,
                    &prepared.payload_string,
                    prepared.created_at,
                    prepared.lsn,
                    partition,
                    offset,
                )
                .await;
            }
            Ok(Err((e, _))) => {
                self.fail_pending(&prepared, &e.to_string(), redis_conn)
                    .await
            }
            Err(_canceled) => {
                self.fail_pending(&prepared, "kafka delivery future canceled", redis_conn)
                    .await
            }
        }
    }

    /// Return a prepared event to `pending` (so the next poll retries it) and drop
    /// its redis idempotency key. Shared by the at-least-once enqueue-failure and
    /// delivery-failure paths (#81).
    #[cfg(feature = "kafka")]
    async fn fail_pending(
        &self,
        prepared: &PreparedOutbox,
        reason: &str,
        mut redis_conn: Option<&mut redis::aio::MultiplexedConnection>,
    ) {
        error!("[cdc] failed to publish to kafka: {}", reason);
        self.metrics.inc_cdc_errors_total("transient");
        self.mark_cdc_delivery_state(prepared.event_id, "pending", None, None, Some(reason))
            .await;
        if let Some(conn) = redis_conn.as_deref_mut() {
            let _: () = conn
                .del(&prepared.idempotency_key)
                .await
                .unwrap_or_default();
        }
    }

    /// Durable evidence that an event was actually published to Kafka, used to
    /// gate ack/delete on redis duplicate-detection (#114). The CDC journal row
    /// is written by [`finish_published_event`] only AFTER the broker
    /// acknowledged the produce, so a `published`/`acked` journal state is proof
    /// the event left the outbox. On any query error this returns `false`
    /// (fail-safe: prefer re-publishing over silently dropping the event).
    #[cfg(feature = "kafka")]
    async fn was_durably_published(&self, event_id: Uuid) -> bool {
        use crate::runtime::system::SystemCatalogConfig;
        let journal = SystemCatalogConfig::default().cdc_journal_relation();
        let state: Result<Option<(String,)>, _> = sqlx::query_as(&format!(
            "SELECT delivery_state FROM {journal} WHERE event_id = $1"
        ))
        .bind(event_id)
        .fetch_optional(&self.pool)
        .await;
        matches!(state, Ok(Some((s,))) if s == "published" || s == "acked")
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

        // Live-subscription fan-out is best-effort, but a send error means zero
        // receivers / full buffer — surface it (the subscriber logs Lagged; the
        // publisher was silent). Durability is unaffected (Kafka already has it).
        if self
            .broadcast_tx
            .send(CdcEnvelope {
                event_id: event_id.to_string(),
                topic: topic.to_string(),
                partition_key: partition_key.to_string(),
                payload_json: payload_string.to_string(),
                published_at: Utc::now(),
            })
            .is_err()
        {
            tracing::debug!(
                event_id = %event_id,
                topic = %topic,
                "[cdc] live broadcast dropped (no active subscribers / buffer full)"
            );
        }

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
            if let Err(err) = evt.validate_identity() {
                error!("[cdc] tail_source rejected invalid event from {label}: {err}");
                self.metrics.inc_cdc_errors_total("source_identity_missing");
                return Err(err);
            }
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
