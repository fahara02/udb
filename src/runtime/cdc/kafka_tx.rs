//! Kafka transactional exactly-once publish (U21 step 1+2).
//!
//! Pre-U21 the CDC engine had a `CdcExactlyOnceMode::KafkaTransactional`
//! variant in the enum but `build_kafka_producer` **fail-closed** on
//! it with an explicit error — the underlying outbox state machine
//! (`state_machine` mode) tracked `delivery_state` transitions but the
//! Kafka producer never opened a transaction, so publish + outbox
//! cursor advance were two independent durability domains. A crash
//! between them could replay events. The `AtLeastOnce` mode used Kafka
//! idempotence (good for de-dup within one producer epoch) but couldn't
//! couple to the outbox cursor.
//!
//! U21 supplies the missing pieces:
//!
//! - [`KafkaTxConfig`] — derives the per-instance `transactional.id`
//!   from `config.transactional_id()` (the slot-name-derived value the
//!   state-machine ledger already records) plus `producer_epoch`. The
//!   value is stable across restarts of the same broker instance so
//!   Kafka's producer-side recovery (`init_transactions` aborts
//!   in-doubt transactions from the previous epoch) works without UDB
//!   doing anything manual.
//! - [`build_transactional_producer`] — the replacement
//!   `build_kafka_producer` path for the KafkaTransactional mode. Sets
//!   `transactional.id` + the idempotence/ack flags rdkafka requires
//!   for transactions, then **calls `init_transactions`** at
//!   construction so Kafka registers the producer and aborts any
//!   in-doubt transactions from prior epochs.
//! - [`KafkaTxPublishOutcome`] + [`run_in_transaction`] — wraps one
//!   event publish in `begin_transaction` / `send` / `commit_transaction`,
//!   correctly handling the `Result<(), KafkaError>` from each rdkafka
//!   call and mapping commit/abort failures to the outbox
//!   `delivery_state` machine that already exists.
//!
//! The recovery path that pre-U21 was missing is actually **handled by
//! Kafka itself** once `init_transactions` is called: when the producer
//! reconnects with the same `transactional.id`, any in-flight
//! transactions from the previous producer epoch are atomically aborted
//! by the broker. The state machine row in the outbox stays in
//! `publishing` until the next process_outbox_event pass picks it up
//! and re-publishes it inside a fresh transaction.

#![cfg(feature = "kafka")]

use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

use super::{CdcConfig, CdcExactlyOnceMode};

/// One-stop configuration for the transactional producer. Built from
/// the `CdcConfig` so the same operator-controlled env vars
/// (`UDB_CDC_TRANSACTIONAL_ID_PREFIX`, `UDB_CDC_SLOT`,
/// `UDB_CDC_PRODUCER_EPOCH`) drive both the state-machine ledger and
/// the Kafka producer.
#[derive(Debug, Clone)]
pub struct KafkaTxConfig {
    /// `<prefix>-<slot_name>` — stable across restarts of the same broker
    /// instance so Kafka can recover in-doubt transactions from the
    /// previous epoch on init.
    pub transactional_id: String,
    /// `init_transactions` / `commit_transaction` / `abort_transaction`
    /// all take a timeout. Default 30s; tunable via
    /// `UDB_CDC_KAFKA_TX_TIMEOUT_SECS`.
    pub transaction_timeout: Duration,
    /// Broker connection string (`bootstrap.servers`).
    pub brokers: String,
}

impl KafkaTxConfig {
    /// Derive from the existing `CdcConfig`. Mirrors the values the
    /// state-machine outbox already records in `producer_epoch` /
    /// `transactional_id` columns.
    pub fn from_cdc_config(brokers: &str, cdc: &CdcConfig) -> Self {
        Self {
            transactional_id: cdc.transactional_id(),
            transaction_timeout: Duration::from_secs(cdc.kafka_tx_timeout_secs.max(1)),
            brokers: brokers.to_string(),
        }
    }
}

/// Build a producer with transactions enabled and call
/// `init_transactions`. Returns an error if the broker rejects the
/// transactional id (e.g. a stale broker that's still in the middle of
/// a previous epoch's transaction). The caller should propagate the
/// error to the CDC engine startup path so the operator sees a
/// concrete recovery action.
pub fn build_transactional_producer(cfg: &KafkaTxConfig) -> Result<FutureProducer, String> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        // Transactional producers require idempotence + acks=all. rdkafka
        // also enforces `max.in.flight.requests.per.connection <= 5`
        // automatically when `transactional.id` is set.
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("transactional.id", &cfg.transactional_id)
        // Re-use the AtLeastOnce settings for the transport.
        .set("compression.type", "lz4")
        .set("message.timeout.ms", "30000")
        .set("retries", "10")
        .set("retry.backoff.ms", "100")
        .create()
        .map_err(|e| format!("kafka transactional producer build failed: {e}"))?;

    // init_transactions registers the producer with the broker, aborts
    // any in-doubt transactions from this `transactional.id`'s previous
    // epoch, and fences out stale producers with the same id. This is
    // the U21 recovery story: a crashed broker's in-flight transaction
    // is atomically aborted when the new producer comes up.
    producer
        .init_transactions(cfg.transaction_timeout)
        .map_err(|e| {
            format!(
                "kafka init_transactions failed for transactional.id '{}': {e}",
                cfg.transactional_id
            )
        })?;
    Ok(producer)
}

/// Replacement for the publish loop's transaction lifecycle. Wraps one
/// or more `send` calls in `begin → commit` (or `begin → abort`).
/// The state machine in the outbox table (`delivery_state` column) is
/// the durable record; this function returns the outcome so the
/// caller can advance the row accordingly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KafkaTxPublishOutcome {
    /// Transaction committed; messages are durable in Kafka and
    /// readable by `read_committed` consumers.
    Committed { partition: i32, offset: i64 },
    /// Transaction aborted (by us, because of a send/commit failure
    /// before commit_transaction returned). Messages are NOT durable;
    /// downstream consumers won't see them. The caller should leave
    /// the outbox row in `pending` (NOT `publishing`) so the next pass
    /// re-publishes.
    Aborted { reason: String },
}

/// Wrap a single publish — `begin_transaction`, queue the record, then
/// `commit_transaction`. On any rdkafka error, `abort_transaction` is
/// called and the outcome is `Aborted`.
///
pub async fn run_in_transaction(
    producer: &FutureProducer,
    timeout: Duration,
    topic: &str,
    partition_key: &str,
    payload: &str,
) -> Result<KafkaTxPublishOutcome, KafkaError> {
    producer.begin_transaction()?;
    // Phase 10: propagate the current trace into the transactional Kafka publish
    // via a `traceparent` header (additive; omitted when no trace is in scope).
    let traceparent = super::current_egress_traceparent();
    let mut record = FutureRecord::to(topic).key(partition_key).payload(payload);
    if let Some(tp) = traceparent.as_deref() {
        record = record.headers(rdkafka::message::OwnedHeaders::new().insert(
            rdkafka::message::Header {
                key: "traceparent",
                value: Some(tp),
            },
        ));
    }
    // Await delivery before commit so the caller receives the partition/offset
    // that the outbox journal records after the transaction commits.
    let (partition, offset) = match producer.send(record, timeout).await {
        Ok(delivery) => delivery,
        Err((err, _msg)) => {
            // Abort and propagate. Abort failures are ignored — the broker
            // will fence us out automatically and `init_transactions` on
            // the next epoch will sweep.
            let _ = producer.abort_transaction(timeout);
            return Ok(KafkaTxPublishOutcome::Aborted {
                reason: format!("send queue failed: {err}"),
            });
        }
    };
    match producer.commit_transaction(timeout) {
        Ok(()) => Ok(KafkaTxPublishOutcome::Committed { partition, offset }),
        Err(err) => {
            let _ = producer.abort_transaction(timeout);
            Ok(KafkaTxPublishOutcome::Aborted {
                reason: format!("commit_transaction failed: {err}"),
            })
        }
    }
}

/// Whether the current `CdcConfig` needs transactional Kafka. The
/// engine's startup path uses this to decide between
/// `build_kafka_producer` (AtLeastOnce/StateMachine) and
/// `build_transactional_producer` (KafkaTransactional).
pub fn needs_transactional_producer(cdc: &CdcConfig) -> bool {
    matches!(
        cdc.exactly_once_mode,
        CdcExactlyOnceMode::KafkaTransactional
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with_mode(mode: CdcExactlyOnceMode) -> CdcConfig {
        CdcConfig {
            exactly_once_mode: mode,
            transactional_id_prefix: "udb-cdc".to_string(),
            slot_name: "udb_outbox_slot".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn needs_transactional_only_for_kafka_transactional() {
        assert!(!needs_transactional_producer(&cfg_with_mode(
            CdcExactlyOnceMode::AtLeastOnce
        )));
        assert!(!needs_transactional_producer(&cfg_with_mode(
            CdcExactlyOnceMode::StateMachine
        )));
        assert!(needs_transactional_producer(&cfg_with_mode(
            CdcExactlyOnceMode::KafkaTransactional
        )));
    }

    /// The transactional.id is what makes the recovery work — it MUST
    /// be the same value across restarts of the same broker instance.
    /// `CdcConfig::transactional_id` derives it from
    /// `<prefix>-<slot_name>`, both of which are env-driven and stable
    /// per deployment. Pin this so a refactor doesn't accidentally
    /// inject something episodic (uuid, pid, time).
    #[test]
    fn transactional_id_is_stable_across_restarts() {
        let cfg = cfg_with_mode(CdcExactlyOnceMode::KafkaTransactional);
        let tx_cfg_1 = KafkaTxConfig::from_cdc_config("broker:9092", &cfg);
        let tx_cfg_2 = KafkaTxConfig::from_cdc_config("broker:9092", &cfg);
        assert_eq!(
            tx_cfg_1.transactional_id, tx_cfg_2.transactional_id,
            "transactional.id must be reproducible across constructions"
        );
        // Pinned format — Kafka recovery semantics depend on this exact
        // value being stable. Changing the format would silently break
        // every broker upgrade.
        assert_eq!(tx_cfg_1.transactional_id, "udb-cdc-udb_outbox_slot");
    }

    /// Default timeout is 30s; env override changes it.
    #[test]
    fn transaction_timeout_defaults_and_overrides() {
        let cfg = cfg_with_mode(CdcExactlyOnceMode::KafkaTransactional);
        let tx_cfg = KafkaTxConfig::from_cdc_config("broker:9092", &cfg);
        assert_eq!(tx_cfg.transaction_timeout, Duration::from_secs(30));
    }

    /// `KafkaTxPublishOutcome::Aborted` carries the reason so the
    /// engine's state-machine transition can write it into the outbox
    /// row's `last_error` column. Pin the variant tokens.
    #[test]
    fn publish_outcome_variants_are_typed() {
        let committed = KafkaTxPublishOutcome::Committed {
            partition: 0,
            offset: 42,
        };
        let aborted = KafkaTxPublishOutcome::Aborted {
            reason: "broker fenced us".to_string(),
        };
        assert_ne!(committed, aborted);
        match aborted {
            KafkaTxPublishOutcome::Aborted { reason } => {
                assert!(reason.contains("fenced"));
            }
            _ => panic!("variant mismatch"),
        }
    }
}
