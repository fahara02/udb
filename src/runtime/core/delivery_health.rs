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

/// A durable consumer's scoped position, shared by status and verification.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CdcConsumerLag {
    pub consumer_name: String,
    pub topic_pattern: String,
    pub last_event_id: String,
    pub lag_seconds: f64,
    pub acked_at_unix: i64,
    pub last_published_at_unix: i64,
    pub tenant_id: String,
    pub project_id: String,
    pub owner_identity: String,
    pub owner_migration_required: bool,
}

impl DataBrokerRuntime {
    /// Read cursor lag only within the supplied tenant and project.
    pub(crate) async fn cdc_consumer_lag(
        &self,
        tenant: &str,
        project: &str,
    ) -> Result<Vec<CdcConsumerLag>, tonic::Status> {
        consumer_lags(
            self.pg_pool()?,
            &SystemCatalogConfig::current(),
            Some(tenant),
            Some(project),
        )
        .await
        .map_err(|err| {
            crate::runtime::executor_utils::sqlx_error_to_status(
                "CDC consumer lag read failed",
                &err,
            )
        })
    }

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

        // Stored watermarks survive retention. Compare with this consumer's
        // scoped head; another tenant/project must not create false lag.
        let consumers = consumer_lags(pool, &config, None, None).await;
        match consumers {
            Ok(rows) if rows.is_empty() => checks.push(DeliveryCheck {
                check: "consumers".to_string(),
                passed: true,
                detail: "no durable consumers registered".to_string(),
            }),
            Ok(rows) => {
                for consumer in rows {
                    let check = format!(
                        "consumer {} ({}, tenant {})",
                        consumer.consumer_name, consumer.topic_pattern, consumer.tenant_id
                    );
                    if consumer.owner_migration_required {
                        checks.push(DeliveryCheck {
                            check,
                            passed: false,
                            detail: "legacy cursor requires a verified owner migration".into(),
                        });
                        continue;
                    }
                    let lag = consumer.lag_seconds;
                    checks.push(DeliveryCheck {
                        check,
                        passed: lag <= thresholds.consumer_max_lag_secs as f64,
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

async fn consumer_lags(
    pool: &sqlx::PgPool,
    config: &SystemCatalogConfig,
    tenant: Option<&str>,
    project: Option<&str>,
) -> Result<Vec<CdcConsumerLag>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, String, String, String, uuid::Uuid, chrono::DateTime<chrono::Utc>, String, chrono::DateTime<chrono::Utc>)>(&format!(
        "SELECT tenant_id, project_id, consumer_name, topic_pattern, last_event_id, last_published_at, owner_identity, acked_at \
         FROM {} WHERE ($1::TEXT IS NULL OR tenant_id=$1) AND ($2::TEXT IS NULL OR project_id=$2) \
         ORDER BY tenant_id, project_id, consumer_name, topic_pattern",
        config.cdc_consumer_cursors_relation(),
    ))
    .bind(tenant)
    .bind(project)
    .fetch_all(pool)
    .await?;
    let mut consumers = Vec::with_capacity(rows.len());
    for (
        tenant_id,
        project_id,
        consumer_name,
        topic_pattern,
        last_event_id,
        watermark,
        owner_identity,
        acked_at,
    ) in rows
    {
        let head: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(&format!(
            "SELECT MAX(published_at) FROM {} WHERE topic ~ $3 \
             AND payload->>'tenant_id'=$1 AND ($2='' OR payload->>'project_id'=$2) \
             AND delivery_state IN ('published','acked')",
            config.cdc_journal_relation(),
        ))
        .bind(&tenant_id)
        .bind(&project_id)
        .bind(topic_pattern_regex(&topic_pattern))
        .fetch_one(pool)
        .await?;
        consumers.push(CdcConsumerLag {
            consumer_name,
            topic_pattern,
            last_event_id: last_event_id.to_string(),
            lag_seconds: head
                .map_or(0.0, |head| {
                    (head - watermark).num_milliseconds() as f64 / 1000.0
                })
                .max(0.0),
            acked_at_unix: acked_at.timestamp(),
            last_published_at_unix: watermark.timestamp(),
            tenant_id,
            project_id,
            owner_migration_required: owner_identity.is_empty(),
            owner_identity,
        });
    }
    Ok(consumers)
}

// WildMatch's two metacharacters, converted to an anchored PostgreSQL regex.
// Quote all regex syntax so literal topic punctuation cannot broaden a check.
fn topic_pattern_regex(pattern: &str) -> String {
    let mut regex = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            '.' | '^' | '$' | '(' | ')' | '[' | ']' | '{' | '}' | '+' | '|' | '\\' => {
                regex.push('\\');
                regex.push(ch);
            }
            _ => regex.push(ch),
        }
    }
    regex.push('$');
    regex
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
