//! Password work must not occupy the asynchronous request workers. One shared
//! admission budget bounds hashes and verifies before they enter Tokio's
//! blocking pool; cancellation cannot free a permit while its KDF still runs.

use std::sync::{Arc, OnceLock};
use tokio::sync::Semaphore;

fn concurrency_budget(available: usize, configured: Option<&str>) -> Result<usize, String> {
    let detected = available.saturating_sub(1).max(1);
    let Some(configured) = configured else {
        return Ok(detected);
    };
    let cap = configured
        .trim()
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| "UDB_PASSWORD_KDF_MAX_CONCURRENCY must be a positive integer".to_string())?;
    Ok(detected.min(cap))
}

fn admission() -> Result<Arc<Semaphore>, String> {
    static ADMISSION: OnceLock<Result<Arc<Semaphore>, String>> = OnceLock::new();
    ADMISSION
        .get_or_init(|| {
            let available = std::thread::available_parallelism()
                .map(|count| count.get())
                .unwrap_or(1);
            let configured = std::env::var("UDB_PASSWORD_KDF_MAX_CONCURRENCY").ok();
            concurrency_budget(available, configured.as_deref())
                .map(|permits| Arc::new(Semaphore::new(permits)))
        })
        .clone()
}

async fn run_on<R, F>(admission: Arc<Semaphore>, work: F) -> Result<R, String>
where
    R: Send + 'static,
    F: FnOnce() -> R + Send + 'static,
{
    // A queued caller can cancel this acquisition without starting CPU work.
    let permit = admission
        .acquire_owned()
        .await
        .map_err(|_| "password computation admission unavailable".to_string())?;
    tokio::task::spawn_blocking(move || {
        // Started blocking tasks outlive a cancelled JoinHandle. Keep admission
        // until the actual computation returns, including unwinding on panic.
        let _permit = permit;
        work()
    })
    .await
    .map_err(|_| "password computation worker failed".to_string())
}

pub(crate) async fn hash(password: &str, hash_key: &[u8]) -> Result<String, String> {
    let password = password.to_string();
    let hash_key = hash_key.to_vec();
    run_on(admission()?, move || {
        super::hash_password(&password, &hash_key)
    })
    .await
}

pub(crate) async fn verify(
    password: &str,
    hash_key: &[u8],
    stored_hash: &str,
) -> Result<bool, String> {
    let password = password.to_string();
    let hash_key = hash_key.to_vec();
    let stored_hash = stored_hash.to_string();
    run_on(admission()?, move || {
        super::verify_password(&password, &hash_key, &stored_hash)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_budget_reserves_cpu_and_only_accepts_positive_lower_caps() {
        assert_eq!(concurrency_budget(1, None), Ok(1));
        assert_eq!(concurrency_budget(4, None), Ok(3));
        assert_eq!(concurrency_budget(4, Some("2")), Ok(2));
        assert_eq!(concurrency_budget(4, Some("99")), Ok(3));
        for invalid in ["0", "", "invalid", "-1"] {
            assert!(concurrency_budget(4, Some(invalid)).is_err());
        }
    }

    #[tokio::test]
    async fn password_offload_preserves_argon_parameters_pepper_and_legacy_verification() {
        let stored = hash("CorrectHorse1!", b"password-cpu-test-key")
            .await
            .expect("hash through shared admission");
        assert!(stored.starts_with("$argon2id$v=19$m=19456,t=2,p=1$"));
        assert!(
            verify("CorrectHorse1!", b"password-cpu-test-key", &stored)
                .await
                .unwrap()
        );
        assert!(
            !verify("wrong password", b"password-cpu-test-key", &stored)
                .await
                .unwrap()
        );
        assert!(
            !verify("CorrectHorse1!", b"different-pepper", &stored)
                .await
                .unwrap()
        );
        let legacy = super::super::hash_secret("password:CorrectHorse1!", b"password-cpu-test-key");
        assert!(
            verify("CorrectHorse1!", b"password-cpu-test-key", &legacy)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn password_cancelled_work_retains_admission_and_cancelled_queue_never_starts() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let admission = Arc::new(Semaphore::new(1));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let running = tokio::spawn(run_on(admission.clone(), move || {
            started_tx.send(()).unwrap();
            finish_rx.recv().unwrap();
        }));
        started_rx.await.unwrap();
        running.abort();
        let _ = running.await;
        assert_eq!(
            admission.available_permits(),
            0,
            "cancelling a caller must not free admission while its CPU job is running"
        );

        let queued_started = Arc::new(AtomicBool::new(false));
        let queued_flag = queued_started.clone();
        let queued = tokio::spawn(run_on(admission.clone(), move || {
            queued_flag.store(true, Ordering::SeqCst);
        }));
        // The only permit is owned by the started blocking job, so this request
        // can only queue. Cancelling it must never submit a second job.
        tokio::task::yield_now().await;
        queued.abort();
        let _ = queued.await;
        finish_tx.send(()).unwrap();
        let restored = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            admission.clone().acquire_owned(),
        )
        .await
        .expect("completed CPU work must release admission")
        .unwrap();
        assert!(!queued_started.load(Ordering::SeqCst));
        drop(restored);
        assert_eq!(admission.available_permits(), 1);
    }
}
