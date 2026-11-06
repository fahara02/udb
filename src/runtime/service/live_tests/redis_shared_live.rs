//! A busy broker shares one Redis connection rather than opening one per operation.

use uuid::Uuid;

use crate::runtime::authn::revocation::{DenylistDecision, JtiDenylist};
use crate::runtime::redis_shared;
use crate::runtime::service::cache_service::redis_engine;

fn redis_dsn() -> Option<String> {
    super::support::require_live_dsn_any(&[
        "UDB_REDIS_DSN",
        "UDB_INTEGRATION_REDIS_URL",
        "UDB_LIVE_REDIS_DSN",
    ])
}

async fn connections_received(client: &redis::Client) -> u64 {
    let mut admin = client
        .get_multiplexed_async_connection()
        .await
        .expect("an admin connection");
    let stats: String = redis::cmd("INFO")
        .arg("stats")
        .query_async(&mut admin)
        .await
        .expect("INFO stats");
    stats
        .lines()
        .find_map(|line| line.strip_prefix("total_connections_received:"))
        .and_then(|value| value.trim().parse().ok())
        .expect("total_connections_received")
}

#[tokio::test]
#[ignore = "requires live Redis (UDB_INTEGRATION_REDIS_URL); runs in the CI --ignored live step"]
async fn thousands_of_operations_share_one_connection_live() {
    let Some(dsn) = redis_dsn() else {
        return;
    };
    let client = redis::Client::open(dsn).expect("a Redis client");
    let tenant = Uuid::new_v4().to_string();
    let namespace = format!("shared-{}", Uuid::new_v4().simple());
    let denylist = JtiDenylist::new(client.clone(), 60);
    redis_shared::connection(&client).await.expect("connect");

    let before = connections_received(&client).await;
    for i in 0..2_000 {
        let outcome = redis_engine::increment(&client, &tenant, &namespace, "key", 1, 60, 0)
            .await
            .expect("an increment");
        assert_eq!(outcome.value, i + 1);
        assert!(matches!(
            denylist.check("never-revoked").await,
            DenylistDecision::NotFound
        ));
    }
    let opened = connections_received(&client).await - before;
    // The two INFO reads each open one; 4,000 operations must open none.
    assert!(opened <= 2, "4,000 operations opened {opened} connections");

    // Concurrent callers share it too.
    let before = connections_received(&client).await;
    let mut tasks = Vec::new();
    for _ in 0..64 {
        let client = client.clone();
        let tenant = tenant.clone();
        let namespace = namespace.clone();
        tasks.push(tokio::spawn(async move {
            redis_engine::increment(&client, &tenant, &namespace, "together", 1, 60, 0)
                .await
                .expect("a concurrent increment")
                .value
        }));
    }
    let mut values = Vec::new();
    for task in tasks {
        values.push(task.await.expect("the task"));
    }
    values.sort_unstable();
    assert_eq!(values, (1..=64).collect::<Vec<i64>>());
    assert!(connections_received(&client).await - before <= 2);

    redis_engine::flush_namespace(&client, &tenant, &namespace)
        .await
        .expect("a flush");
}

#[tokio::test]
#[ignore = "requires live Redis (UDB_INTEGRATION_REDIS_URL); runs in the CI --ignored live step"]
async fn the_shared_connection_recovers_after_redis_drops_it_live() {
    let Some(dsn) = redis_dsn() else {
        return;
    };
    let client = redis::Client::open(dsn).expect("a Redis client");
    let tenant = Uuid::new_v4().to_string();
    let namespace = format!("recover-{}", Uuid::new_v4().simple());
    redis_engine::increment(&client, &tenant, &namespace, "key", 1, 60, 0)
        .await
        .expect("before the drop");

    let mut admin = client
        .get_multiplexed_async_connection()
        .await
        .expect("an admin connection");
    let id: i64 = redis::cmd("CLIENT")
        .arg("ID")
        .query_async(&mut admin)
        .await
        .expect("CLIENT ID");
    let _: i64 = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("TYPE")
        .arg("normal")
        .arg("SKIPME")
        .arg("yes")
        .query_async(&mut admin)
        .await
        .unwrap_or_else(|err| panic!("CLIENT KILL from {id}: {err}"));

    // The call in flight may see the drop, as a retryable refusal. The next ones work.
    let mut recovered = false;
    for _ in 0..50 {
        match redis_engine::increment(&client, &tenant, &namespace, "key", 1, 60, 0).await {
            Ok(_) => {
                recovered = true;
                break;
            }
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::Unavailable, "{status}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
    assert!(recovered, "the shared connection never recovered");
    redis_engine::flush_namespace(&client, &tenant, &namespace)
        .await
        .expect("a flush");
}
