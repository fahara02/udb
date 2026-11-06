//! One shared Redis connection per endpoint.
//!
//! Opening a connection per operation leaves each closed one in TIME_WAIT for about a minute,
//! holding a local port. A busy broker runs out of them and every Redis call fails with
//! `Unavailable` while Redis itself is idle. A `ConnectionManager` is multiplexed, so concurrent
//! callers share one socket, and it reconnects after a failure. It must not carry `WATCH`,
//! `SUBSCRIBE` or a blocking command, which hold a connection's state; nothing here sends them.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use tokio::sync::OnceCell;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_RETRIES: usize = 2;

#[derive(Clone)]
struct Entry {
    connection: Arc<OnceCell<ConnectionManager>>,
    /// Cleared when the runtime that drives the connection shuts down, since the connection's
    /// task dies with it. A process serving from one runtime never sees this.
    runtime_alive: Arc<AtomicBool>,
}

impl Entry {
    fn new() -> Self {
        Self {
            connection: Arc::new(OnceCell::new()),
            runtime_alive: Arc::new(AtomicBool::new(true)),
        }
    }
}

struct ClearOnDrop(Arc<AtomicBool>);

impl Drop for ClearOnDrop {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn entries() -> &'static Mutex<HashMap<String, Entry>> {
    static ENTRIES: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();
    ENTRIES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The shared connection to `client`'s endpoint, opened on first use. Cloning the result is cheap.
pub(crate) async fn connection(client: &redis::Client) -> redis::RedisResult<ConnectionManager> {
    // The key holds the credentials only in memory, and is never logged.
    let key = format!("{:?}", client.get_connection_info());
    let entry = {
        let mut entries = entries()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = entries.entry(key).or_insert_with(Entry::new);
        if !entry.runtime_alive.load(Ordering::Acquire) {
            *entry = Entry::new();
        }
        entry.clone()
    };
    let shared = entry
        .connection
        .get_or_try_init(|| async {
            let config = ConnectionManagerConfig::new()
                .set_connection_timeout(CONNECT_TIMEOUT)
                .set_response_timeout(RESPONSE_TIMEOUT)
                .set_number_of_retries(CONNECT_RETRIES);
            let manager = client.get_connection_manager_with_config(config).await?;
            let guard = ClearOnDrop(entry.runtime_alive.clone());
            tokio::spawn(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            });
            Ok::<_, redis::RedisError>(manager)
        })
        .await?;
    Ok(shared.clone())
}
