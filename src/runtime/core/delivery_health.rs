//! Delivery health for `udb verify --live`: is the outbox draining, are the
//! durable consumers keeping up with the CDC journal, is the projection queue
//! moving. A deploy can pass every schema check and still deliver nothing; one
//! consumer loop that stopped retrying left a service's consumers idle for
//! eight hours with no error anywhere. These are the numbers that show it.

use super::*;
use crate::runtime::system::SystemCatalogConfig;

/// Thresholds above which a delivery check fails.
#[derive(Debug, Clone, Copy)]
pub struct DeliveryThresholds {
    /// Oldest undelivered outbox row, in seconds.
    pub outbox_max_age_secs: i64,
    /// A consumer whose last ack is this far behind the journal head.
    pub consumer_max_lag_secs: i64,
    /// Oldest pending or failed projection task, in seconds.
    pub projection_max_age_secs: i64,
}

impl Default for DeliveryThresholds {
    fn default() -> Self {
        Self {
            outbox_max_age_secs: 300,
            consumer_max_lag_secs: 900,
            projection_max_age_secs: 600,
        }
    }
}

/// One failed or passed delivery check.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DeliveryCheck {
    pub check: String,
    pub passed: bool,
    pub detail: String,
}

impl DataBrokerRuntime {
    /// Read-only delivery health over the system tables. A table that does
    /// not exist yet (a broker that never started) is reported, not failed.
    pub async fn delivery_health(
        &self,
        thresholds: DeliveryThresholds,
    ) -> Result<Vec<DeliveryCheck>, tonic::Status> {
        let pool = self.pg_pool()?;
        let config = SystemCatalogConfig::current();
        let mut checks = Vec::new();

        let outbox = sqlx::query_as::<_, (i64, Option<i64>, i64)>(&format!(
            "SELECT COUNT(*) FILTER (WHERE delivery_state IN ('pending','publishing')), \
                    EXTRACT(EPOCH FROM NOW() - MIN(created_at) FILTER (WHERE delivery_state IN ('pending','publishing')))::BIGINT, \
                    COUNT(*) FILTER (WHERE delivery_state = 'dlq') \
             FROM {}",
            config.cdc.outbox_relation()
        ))
        .fetch_one(pool)
        .await;
        checks.push(match outbox {
            Ok((pending, oldest, dlq)) => {
                let oldest = oldest.unwrap_or(0);
                DeliveryCheck {
                    check: "outbox".to_string(),
                    passed: oldest <= thresholds.outbox_max_age_secs,
                    detail: format!(
                        "{pending} undelivered (oldest {oldest}s, limit {}s), {dlq} dead-lettered",
                        thresholds.outbox_max_age_secs
                    ),
                }
            }
            Err(err) => unreadable("outbox", err),
        });

        // Each durable consumer's last ack against the newest journal row on
        // the topics it reads. Patterns are matched like PublishCDC does for
        // the common forms: exact topic or a trailing `*`.
        let consumers = sqlx::query_as::<_, (String, String, String, Option<i64>)>(&format!(
            "SELECT c.tenant_id, c.consumer_name, c.topic_pattern, \
                    EXTRACT(EPOCH FROM ( \
                        SELECT MAX(j.published_at) FROM {journal} j \
                        WHERE j.topic = c.topic_pattern \
                           OR (RIGHT(c.topic_pattern, 1) = '*' AND j.topic LIKE RTRIM(c.topic_pattern, '*') || '%') \
                    ) - acked.published_at)::BIGINT \
             FROM {cursors} c \
             LEFT JOIN LATERAL (SELECT published_at FROM {journal} WHERE event_id = c.last_event_id) acked ON TRUE",
            journal = config.cdc_journal_relation(),
            cursors = config.cdc_consumer_cursors_relation(),
        ))
        .fetch_all(pool)
        .await;
        match consumers {
            Ok(rows) if rows.is_empty() => checks.push(DeliveryCheck {
                check: "consumers".to_string(),
                passed: true,
                detail: "no durable consumers registered".to_string(),
            }),
            Ok(rows) => {
                for (tenant, name, pattern, lag) in rows {
                    let lag = lag.unwrap_or(0).max(0);
                    checks.push(DeliveryCheck {
                        check: format!("consumer {name} ({pattern}, tenant {tenant})"),
                        passed: lag <= thresholds.consumer_max_lag_secs,
                        detail: format!(
                            "{lag}s behind the journal head (limit {}s)",
                            thresholds.consumer_max_lag_secs
                        ),
                    });
                }
            }
            Err(err) => checks.push(unreadable("consumers", err)),
        }

        let projections = sqlx::query_as::<_, (i64, i64, Option<i64>)>(&format!(
            "SELECT COUNT(*) FILTER (WHERE status IN ('PENDING','IN_PROGRESS','FAILED')), \
                    COUNT(*) FILTER (WHERE status = 'DEAD_LETTER'), \
                    EXTRACT(EPOCH FROM NOW() - MIN(created_at) FILTER (WHERE status IN ('PENDING','FAILED')))::BIGINT \
             FROM {}",
            config.projection_tasks_relation()
        ))
        .fetch_one(pool)
        .await;
        checks.push(match projections {
            Ok((open, dead, oldest)) => {
                let oldest = oldest.unwrap_or(0);
                DeliveryCheck {
                    check: "projections".to_string(),
                    passed: oldest <= thresholds.projection_max_age_secs && dead == 0,
                    detail: format!(
                        "{open} open task(s) (oldest {oldest}s, limit {}s), {dead} dead-lettered",
                        thresholds.projection_max_age_secs
                    ),
                }
            }
            Err(err) => unreadable("projections", err),
        });
        Ok(checks)
    }
}

/// A missing table means the broker has not created it yet: report it as a
/// passed check with the reason, since there is nothing undelivered in it.
fn unreadable(check: &str, err: sqlx::Error) -> DeliveryCheck {
    let missing = err
        .as_database_error()
        .and_then(|db| db.code())
        .is_some_and(|code| code == "42P01");
    DeliveryCheck {
        check: check.to_string(),
        passed: missing,
        detail: if missing {
            "table not created yet (the broker has not started against this database)".to_string()
        } else {
            format!("could not read: {err}")
        },
    }
}
