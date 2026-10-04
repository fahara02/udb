// tests/integration_tests.rs — UDB Docker-backed integration test matrix
//
// These tests run against a live stack brought up by docker-compose.integration.yml.
// They are disabled unless the env var UDB_INTEGRATION_TESTS=1 is set so that the
// standard `cargo test` run stays fast.
//
// To run:
//   docker compose -f docker-compose.integration.yml up -d --wait
//   UDB_INTEGRATION_TESTS=1 cargo test --test integration_tests -- --nocapture
//
// Each test starts from a known-clean state by using unique schema/topic prefixes
// derived from `std::thread::current().id()` so tests can run in parallel.

use std::env;
use std::time::Duration;
use uuid::Uuid;

// ── Guard ─────────────────────────────────────────────────────────────────────

macro_rules! integration_test {
    ($name:ident, $body:expr) => {
        // Honest gate: `#[ignore]` makes a default `cargo test` report these as
        // IGNORED (not passed). They run only under `-- --ignored` against a live
        // stack (docker-compose.integration.yml + UDB_INTEGRATION_* env); with no
        // stack the body connects to the default DSNs and fails loudly. This
        // replaced an env-gated early `return` that PASSED green without ever
        // touching a database (false coverage).
        #[tokio::test]
        #[ignore = "live stack required: run with `-- --ignored` and UDB_INTEGRATION_* env (docker-compose.integration.yml)"]
        async fn $name() {
            $body.await;
        }
    };
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn pg_dsn() -> String {
    env::var("UDB_INTEGRATION_PG_DSN")
        .unwrap_or_else(|_| "postgres://udb:udb@localhost:55432/udb".to_string())
}

fn kafka_brokers() -> String {
    env::var("UDB_INTEGRATION_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:59192".to_string())
}

fn redis_url() -> String {
    env::var("UDB_INTEGRATION_REDIS_URL").unwrap_or_else(|_| "redis://localhost:56379".to_string())
}

#[cfg(feature = "qdrant")]
fn qdrant_url() -> String {
    env::var("UDB_INTEGRATION_QDRANT_URL").unwrap_or_else(|_| "http://localhost:56333".to_string())
}

fn minio_endpoint() -> String {
    env::var("UDB_INTEGRATION_MINIO_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:59000".to_string())
}

/// Create a `PgPool` from the integration DSN.
async fn pg_pool() -> sqlx::PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(3)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&pg_dsn())
        .await
        .expect("connect to integration postgres")
}

// ── Test 1: PostgreSQL system catalog bootstrap with renamed schema ────────────

integration_test!(system_catalog_bootstrap_with_custom_schema, async {
    let pool = pg_pool().await;
    let schema = format!("udb_inttest_{}", Uuid::new_v4().simple());

    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .execute(&pool)
        .await
        .expect("create test schema");

    // Bootstrap the UDB system tables using the udb library's DDL generator.
    let sys = udb::runtime::system::SystemCatalogConfig::with_schema(&schema);
    let ddl = sys.system_catalog_ddl();
    assert!(!ddl.is_empty(), "DDL must not be empty");

    for stmt in ddl.iter() {
        sqlx::query(stmt)
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("DDL failed: {e}\nStatement: {stmt}"));
    }

    // Verify all expected tables were created.
    let tables: Vec<String> = sqlx::query_scalar(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name",
        )
        .bind(&schema)
        .fetch_all(&pool)
        .await
        .expect("fetch tables");

    for expected in &[
        "udb_abac_policies",
        "udb_admin_audit_log",
        "udb_cdc_dlq_events",
        "udb_cdc_event_journal",
        "udb_cdc_lock_log",
        "udb_cdc_offsets",
        "outbox_events",
        "udb_saga_coordinator",
    ] {
        assert!(
            tables.iter().any(|t| t.as_str() == *expected),
            "missing table {expected} in schema {schema}; found: {tables:?}"
        );
    }

    // Cleanup.
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("drop test schema");
});

// ── Test 2: Kafka broker reachability ─────────────────────────────────────────
//
// Formerly `cdc_outbox_to_kafka_delivery`: it wrote a row into a scratch outbox
// table that nothing tails and then asserted only that Kafka answered a
// metadata request — no UDB code ran, yet the name claimed CDC delivery. The
// real outbox → Kafka → journal path is exercised by the lib live tests that
// drive `CdcEngine::process_outbox_event` (auth/notification event live tests)
// and the CDC journal tests in `runtime::cdc::live_tests`. This test now claims
// only what it checks.

integration_test!(kafka_broker_metadata_reachable, async {
    use rdkafka::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};

    let topic = "document.uploaded.v1";
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", kafka_brokers())
        .set(
            "group.id",
            format!("udb-integration-{}", Uuid::new_v4().simple()),
        )
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("create kafka consumer");

    let brokers_reachable = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(5))
        .is_ok();
    assert!(
        brokers_reachable,
        "Kafka brokers should be reachable at {}",
        kafka_brokers()
    );
});

integration_test!(kafka_period_topic_publish_consume_roundtrip, async {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::{ClientConfig, Message};

    let brokers = kafka_brokers();
    let topic = format!("udb.integration.{}.v1", Uuid::new_v4().simple());
    assert!(
        !topic.contains('_'),
        "integration Kafka topics must use periods, not underscores"
    );
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("create Kafka admin client");
    admin
        .create_topics(
            &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create Kafka topic request");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("create Kafka producer");
    let event_id = Uuid::new_v4().to_string();
    let payload = serde_json::json!({
        "event_id": event_id,
        "event_type": topic,
        "document_id": event_id,
        "payload": {
            "backend": "kafka",
            "mode": "live"
        }
    })
    .to_string();
    producer
        .send(
            FutureRecord::to(&topic).key(&event_id).payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .expect("publish Kafka record");

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set(
            "group.id",
            format!("udb-integration-{}", Uuid::new_v4().simple()),
        )
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("create Kafka consumer");
    consumer.subscribe(&[&topic]).expect("subscribe to topic");

    let mut found = false;
    for _ in 0..20 {
        if let Some(result) = consumer.poll(Duration::from_millis(500)) {
            let msg = result.expect("Kafka message");
            if let Some(bytes) = msg.payload() {
                let value: serde_json::Value =
                    serde_json::from_slice(bytes).expect("Kafka JSON payload");
                if value["event_id"] == event_id {
                    assert_eq!(value["event_type"], topic);
                    found = true;
                    break;
                }
            }
        }
    }
    assert!(found, "published event must be consumable from {topic}");
});

// ── Tests 3–5 removed ─────────────────────────────────────────────────────────
//
// `cdc_dlq_routing_for_unknown_topic`, `cdc_journal_replay_after_outbox_delete`
// and `saga_stale_in_progress_detection` created scratch tables, ran hand-written
// SQL against them and asserted the SQL's own result — no UDB code executed, so
// they proved nothing about the DLQ, the journal or saga recovery while reading
// as coverage of all three. The real paths are covered by the lib live tests:
// `runtime::cdc::live_tests` (`route_to_dlq`, journal retention, the journal
// tail) against the production system-catalog DDL.

// ── Test 6: Qdrant health probe ───────────────────────────────────────────────

#[cfg(feature = "qdrant")]
integration_test!(qdrant_health_probe, async {
    let client = reqwest::Client::new();
    let url = format!("{}/collections", qdrant_url());
    let resp = client
        .get(&url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap_or_else(|e| panic!("Qdrant health check failed: {e}"));
    assert!(
        resp.status().is_success(),
        "Qdrant /collections returned {}",
        resp.status()
    );
});

#[cfg(feature = "qdrant")]
integration_test!(qdrant_collection_vector_roundtrip, async {
    let client = reqwest::Client::new();
    let collection = format!("udb_integration_{}", Uuid::new_v4().simple());
    let base = qdrant_url();
    let create = client
        .put(format!("{base}/collections/{collection}"))
        .json(&serde_json::json!({
            "vectors": {
                "size": 3,
                "distance": "Cosine"
            }
        }))
        .send()
        .await
        .expect("create qdrant collection");
    assert!(
        create.status().is_success(),
        "Qdrant create collection returned {}: {}",
        create.status(),
        create.text().await.unwrap_or_default()
    );

    let point_id = 1;
    let upsert = client
        .put(format!("{base}/collections/{collection}/points?wait=true"))
        .json(&serde_json::json!({
            "points": [{
                "id": point_id,
                "vector": [0.1, 0.2, 0.3],
                "payload": {
                    "backend": "qdrant",
                    "mode": "live"
                }
            }]
        }))
        .send()
        .await
        .expect("upsert qdrant point");
    assert!(
        upsert.status().is_success(),
        "Qdrant upsert returned {}: {}",
        upsert.status(),
        upsert.text().await.unwrap_or_default()
    );

    let retrieve = client
        .post(format!("{base}/collections/{collection}/points"))
        .json(&serde_json::json!({
            "ids": [point_id],
            "with_payload": true,
            "with_vector": true
        }))
        .send()
        .await
        .expect("retrieve qdrant point");
    assert!(
        retrieve.status().is_success(),
        "Qdrant retrieve returned {}",
        retrieve.status()
    );
    let body: serde_json::Value = retrieve.json().await.expect("qdrant retrieve JSON");
    assert_eq!(body["result"][0]["payload"]["backend"], "qdrant");

    let _ = client
        .delete(format!("{base}/collections/{collection}"))
        .send()
        .await;
});

// ── Test 7: Redis connectivity ────────────────────────────────────────────────

integration_test!(redis_connectivity, async {
    let client = redis::Client::open(redis_url()).expect("create redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect to redis");
    let pong: String = redis::cmd("PING")
        .query_async(&mut conn)
        .await
        .expect("redis PING");
    assert_eq!(pong, "PONG");
});

integration_test!(redis_live_read_write_delete_roundtrip, async {
    let client = redis::Client::open(redis_url()).expect("create redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect to redis");
    let key = format!("udb:integration:{}", Uuid::new_v4().simple());
    let value = serde_json::json!({
        "backend": "redis",
        "mode": "live",
        "id": Uuid::new_v4().to_string(),
    })
    .to_string();
    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg(&value)
        .query_async(&mut conn)
        .await
        .expect("redis SET");
    let got: String = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut conn)
        .await
        .expect("redis GET");
    assert_eq!(got, value);
    let deleted: i64 = redis::cmd("DEL")
        .arg(&key)
        .query_async(&mut conn)
        .await
        .expect("redis DEL");
    assert_eq!(deleted, 1);
});

// ── Test 8: PostgreSQL logical replication slot creation ──────────────────────

integration_test!(postgres_logical_replication_slot, async {
    let pool = pg_pool().await;
    let slot_name = format!("udb_inttest_{}", Uuid::new_v4().simple());

    // Create a logical replication slot.
    let result = sqlx::query(&format!(
        "SELECT pg_create_logical_replication_slot('{slot_name}', 'pgoutput')"
    ))
    .execute(&pool)
    .await;

    match result {
        Ok(_) => {
            // Verify it exists.
            let (exists,): (bool,) = sqlx::query_as(
                "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            )
            .bind(&slot_name)
            .fetch_one(&pool)
            .await
            .expect("query slot existence");
            assert!(exists, "replication slot should exist after creation");

            // Clean up.
            sqlx::query(&format!("SELECT pg_drop_replication_slot('{slot_name}')"))
                .execute(&pool)
                .await
                .expect("drop replication slot");
        }
        Err(e) => {
            // May fail in CI if the user lacks REPLICATION privilege.
            eprintln!(
                "[integration] WARNING: logical replication slot creation failed ({}). \
                     Ensure the test user has REPLICATION privilege and wal_level=logical.",
                e
            );
        }
    }
});

// ── Test 9: MinIO bucket probe ────────────────────────────────────────────────

integration_test!(minio_bucket_probe, async {
    use aws_config::BehaviorVersion;
    use aws_sdk_s3::config::{Credentials, Region};

    let creds = Credentials::new(
        env::var("UDB_INTEGRATION_MINIO_ACCESS_KEY").unwrap_or_else(|_| "minio".into()),
        env::var("UDB_INTEGRATION_MINIO_SECRET_KEY").unwrap_or_else(|_| "minio123".into()),
        None,
        None,
        "integration-test",
    );
    let s3_conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .endpoint_url(minio_endpoint())
        .force_path_style(true)
        .build();
    let s3 = aws_sdk_s3::Client::from_conf(s3_conf);

    // List buckets — just checks connectivity and credentials.
    let result = s3.list_buckets().send().await;
    assert!(
        result.is_ok(),
        "MinIO list_buckets failed: {:?}",
        result.err()
    );
});

integration_test!(minio_object_put_get_delete_roundtrip, async {
    use aws_config::BehaviorVersion;
    use aws_sdk_s3::config::{Credentials, Region};
    use aws_sdk_s3::primitives::ByteStream;

    let creds = Credentials::new(
        env::var("UDB_INTEGRATION_MINIO_ACCESS_KEY").unwrap_or_else(|_| "minio".into()),
        env::var("UDB_INTEGRATION_MINIO_SECRET_KEY").unwrap_or_else(|_| "minio123".into()),
        None,
        None,
        "integration-test",
    );
    let s3_conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .endpoint_url(minio_endpoint())
        .force_path_style(true)
        .build();
    let s3 = aws_sdk_s3::Client::from_conf(s3_conf);
    let bucket = format!("udb-integration-{}", Uuid::new_v4().simple());
    let key = "live/object.json";
    let payload = serde_json::json!({
        "backend": "minio",
        "mode": "live",
        "id": Uuid::new_v4().to_string(),
    })
    .to_string();

    s3.create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("create MinIO bucket");
    s3.put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(payload.clone().into_bytes()))
        .send()
        .await
        .expect("put MinIO object");
    let got = s3
        .get_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("get MinIO object")
        .body
        .collect()
        .await
        .expect("read MinIO body")
        .into_bytes();
    assert_eq!(String::from_utf8(got.to_vec()).expect("utf8 body"), payload);
    s3.delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("delete MinIO object");
    s3.delete_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("delete MinIO bucket");
});

// ── Test 10: Catalog version tables round-trip ─────────────────────────────────

integration_test!(catalog_version_tables_round_trip, async {
    let pool = pg_pool().await;
    let schema = format!("udb_cat_{}", Uuid::new_v4().simple());

    sqlx::raw_sql(&format!(
        "CREATE SCHEMA IF NOT EXISTS {schema};
             CREATE TABLE {schema}.udb_catalog_versions (
               catalog_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               project_id TEXT NOT NULL DEFAULT 'default',
               version TEXT NOT NULL,
               checksum_sha256 TEXT NOT NULL,
               manifest_json JSONB NOT NULL DEFAULT '{{}}',
               status VARCHAR(20) NOT NULL DEFAULT 'STAGED',
               created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
               activated_at TIMESTAMPTZ
             );
             CREATE TABLE {schema}.udb_catalog_activation_log (
               id BIGSERIAL PRIMARY KEY,
               project_id TEXT NOT NULL,
               from_version TEXT,
               to_version TEXT NOT NULL,
               actor TEXT NOT NULL DEFAULT 'test',
               reason TEXT,
               created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
             );"
    ))
    .execute(&pool)
    .await
    .expect("create catalog tables");

    // Stage a version.
    let catalog_id: Uuid = sqlx::query_scalar(&format!(
        "INSERT INTO {schema}.udb_catalog_versions
             (version, checksum_sha256, status)
             VALUES ('1.0.0', 'abc123', 'STAGED')
             RETURNING catalog_id"
    ))
    .fetch_one(&pool)
    .await
    .expect("stage catalog");

    // Activate it.
    sqlx::query(&format!(
        "UPDATE {schema}.udb_catalog_versions
             SET status = 'ACTIVE', activated_at = NOW()
             WHERE catalog_id = $1"
    ))
    .bind(catalog_id)
    .execute(&pool)
    .await
    .expect("activate catalog");

    let (status,): (String,) = sqlx::query_as(&format!(
        "SELECT status FROM {schema}.udb_catalog_versions WHERE catalog_id = $1"
    ))
    .bind(catalog_id)
    .fetch_one(&pool)
    .await
    .expect("fetch catalog status");
    assert_eq!(status, "ACTIVE");

    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("drop test schema");
});
