// Live runtime backend matrix for Docker-backed services.
//
// These are deliberately opt-in because they require real Kafka, Redis, Qdrant,
// and MinIO containers. They complement `integration_tests.rs` without making
// that file larger.

use std::env;
use std::time::Duration;
use uuid::Uuid;

fn integration_enabled() -> bool {
    env::var("UDB_INTEGRATION_TESTS")
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

macro_rules! live_backend_test {
    ($name:ident, $body:expr) => {
        #[tokio::test]
        async fn $name() {
            if !integration_enabled() {
                eprintln!(
                    "[integration] skipped: set UDB_INTEGRATION_TESTS=1 to run against live backends"
                );
                return;
            }
            $body.await;
        }
    };
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

live_backend_test!(kafka_period_topics_preserve_key_order, async {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::{ClientConfig, Message};

    let brokers = kafka_brokers();
    let topic = format!("udb.runtime.kafka.{}.v1", Uuid::new_v4().simple());
    assert!(!topic.contains('_'), "Kafka topic must use periods only");

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
        .expect("create Kafka topic");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("create Kafka producer");
    let key = Uuid::new_v4().to_string();
    for ordinal in 1..=2 {
        let payload = serde_json::json!({
            "event_id": Uuid::new_v4().to_string(),
            "event_type": topic,
            "key": key,
            "ordinal": ordinal,
        })
        .to_string();
        producer
            .send(
                FutureRecord::to(&topic).key(&key).payload(&payload),
                Duration::from_secs(10),
            )
            .await
            .expect("publish Kafka event");
    }

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set(
            "group.id",
            format!("udb-runtime-kafka-{}", Uuid::new_v4().simple()),
        )
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("create Kafka consumer");
    consumer.subscribe(&[&topic]).expect("subscribe to topic");

    let mut ordinals = Vec::new();
    for _ in 0..40 {
        if let Some(result) = consumer.poll(Duration::from_millis(500)) {
            let msg = result.expect("Kafka message");
            let Some(bytes) = msg.payload() else {
                continue;
            };
            let value: serde_json::Value =
                serde_json::from_slice(bytes).expect("Kafka JSON payload");
            if value["key"] == key {
                ordinals.push(value["ordinal"].as_i64().unwrap_or_default());
                if ordinals.len() == 2 {
                    break;
                }
            }
        }
    }
    assert_eq!(ordinals, vec![1, 2]);
});

live_backend_test!(redis_ttl_json_and_hash_roundtrip, async {
    let client = redis::Client::open(redis_url()).expect("create Redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect Redis");
    let key = format!("udb:runtime:{}:json", Uuid::new_v4().simple());
    let hash = format!("udb:runtime:{}:hash", Uuid::new_v4().simple());
    let payload = serde_json::json!({
        "backend": "redis",
        "mode": "ttl-json",
        "id": Uuid::new_v4().to_string(),
    })
    .to_string();

    let _: () = redis::cmd("SETEX")
        .arg(&key)
        .arg(30)
        .arg(&payload)
        .query_async(&mut conn)
        .await
        .expect("Redis SETEX");
    let ttl: i64 = redis::cmd("TTL")
        .arg(&key)
        .query_async(&mut conn)
        .await
        .expect("Redis TTL");
    assert!(ttl > 0 && ttl <= 30, "Redis key should have a live TTL");
    let got: String = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut conn)
        .await
        .expect("Redis GET");
    assert_eq!(got, payload);

    let _: i64 = redis::cmd("HSET")
        .arg(&hash)
        .arg("status")
        .arg("active")
        .arg("tenant")
        .arg("acme")
        .query_async(&mut conn)
        .await
        .expect("Redis HSET");
    let status: String = redis::cmd("HGET")
        .arg(&hash)
        .arg("status")
        .query_async(&mut conn)
        .await
        .expect("Redis HGET");
    assert_eq!(status, "active");

    let deleted: i64 = redis::cmd("DEL")
        .arg(&key)
        .arg(&hash)
        .query_async(&mut conn)
        .await
        .expect("Redis cleanup");
    assert_eq!(deleted, 2);
});

#[cfg(feature = "qdrant")]
live_backend_test!(qdrant_payload_filter_scroll_roundtrip, async {
    let client = reqwest::Client::new();
    let collection = format!("udb_runtime_{}", Uuid::new_v4().simple());
    let base = qdrant_url();
    let create = client
        .put(format!("{base}/collections/{collection}"))
        .json(&serde_json::json!({
            "vectors": {"size": 3, "distance": "Cosine"}
        }))
        .send()
        .await
        .expect("create Qdrant collection");
    assert!(
        create.status().is_success(),
        "Qdrant create returned {}",
        create.status()
    );

    let tenant_id = format!("tenant-{}", Uuid::new_v4().simple());
    let upsert = client
        .put(format!("{base}/collections/{collection}/points?wait=true"))
        .json(&serde_json::json!({
            "points": [
                {"id": 1, "vector": [0.1, 0.2, 0.3], "payload": {"tenant_id": tenant_id, "kind": "hit"}},
                {"id": 2, "vector": [0.3, 0.2, 0.1], "payload": {"tenant_id": "other", "kind": "miss"}}
            ]
        }))
        .send()
        .await
        .expect("upsert Qdrant points");
    assert!(
        upsert.status().is_success(),
        "Qdrant upsert returned {}",
        upsert.status()
    );

    let scroll = client
        .post(format!("{base}/collections/{collection}/points/scroll"))
        .json(&serde_json::json!({
            "filter": {
                "must": [{
                    "key": "tenant_id",
                    "match": {"value": tenant_id}
                }]
            },
            "with_payload": true,
            "limit": 10
        }))
        .send()
        .await
        .expect("scroll Qdrant collection");
    assert!(
        scroll.status().is_success(),
        "Qdrant scroll returned {}",
        scroll.status()
    );
    let body: serde_json::Value = scroll.json().await.expect("Qdrant scroll JSON");
    let points = body["result"]["points"]
        .as_array()
        .expect("Qdrant scroll points");
    assert_eq!(points.len(), 1);
    assert_eq!(points[0]["payload"]["kind"], "hit");

    let _ = client
        .delete(format!("{base}/collections/{collection}"))
        .send()
        .await;
});

live_backend_test!(minio_prefix_listing_and_object_body_roundtrip, async {
    use aws_config::BehaviorVersion;
    use aws_sdk_s3::config::{Credentials, Region};
    use aws_sdk_s3::primitives::ByteStream;

    let creds = Credentials::new(
        env::var("UDB_INTEGRATION_MINIO_ACCESS_KEY").unwrap_or_else(|_| "minio".into()),
        env::var("UDB_INTEGRATION_MINIO_SECRET_KEY").unwrap_or_else(|_| "minio123".into()),
        None,
        None,
        "runtime-live-test",
    );
    let s3_conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .endpoint_url(minio_endpoint())
        .force_path_style(true)
        .build();
    let s3 = aws_sdk_s3::Client::from_conf(s3_conf);
    let bucket = format!("udb-runtime-{}", Uuid::new_v4().simple());
    let prefix = "runtime/live/";
    let key_a = format!("{prefix}a.json");
    let key_b = format!("{prefix}b.json");
    let payload = serde_json::json!({
        "backend": "minio",
        "mode": "prefix-list",
        "id": Uuid::new_v4().to_string(),
    })
    .to_string();

    s3.create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("create MinIO bucket");
    for key in [&key_a, &key_b] {
        s3.put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(payload.clone().into_bytes()))
            .send()
            .await
            .expect("put MinIO object");
    }

    let listed = s3
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(prefix)
        .send()
        .await
        .expect("list MinIO objects");
    let keys = listed
        .contents()
        .iter()
        .filter_map(|obj| obj.key())
        .collect::<Vec<_>>();
    assert!(keys.contains(&key_a.as_str()));
    assert!(keys.contains(&key_b.as_str()));

    let body = s3
        .get_object()
        .bucket(&bucket)
        .key(&key_a)
        .send()
        .await
        .expect("get MinIO object")
        .body
        .collect()
        .await
        .expect("read MinIO object body")
        .into_bytes();
    assert_eq!(
        String::from_utf8(body.to_vec()).expect("UTF-8 body"),
        payload
    );

    for key in [&key_a, &key_b] {
        s3.delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .expect("delete MinIO object");
    }
    s3.delete_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("delete MinIO bucket");
});
