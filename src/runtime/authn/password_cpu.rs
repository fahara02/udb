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

/// Memory one Argon2id hash or verify holds (`m=19456` KiB).
const ARGON2_MEMORY_BYTES: u64 = 19_456 * 1024;
/// Share of the process memory limit password work may use by default.
const KDF_MEMORY_SHARE_DIVISOR: u64 = 4;
/// Below this limit a startup warning names the knob even without a cut.
const SMALL_MEMORY_LIMIT_BYTES: u64 = 512 * 1024 * 1024;

/// The cgroup memory limit of this process (v2 `memory.max`, else v1
/// `memory.limit_in_bytes`); `None` when unlimited or not in a cgroup.
fn cgroup_memory_limit() -> Option<u64> {
    let read = |path: &str| std::fs::read_to_string(path).ok();
    let parse = |raw: String| {
        raw.trim()
            .parse::<u64>()
            .ok()
            // cgroup v1 reports "unlimited" as a page-rounded i64::MAX.
            .filter(|bytes| *bytes > 0 && *bytes < (1 << 60))
    };
    read("/sys/fs/cgroup/memory.max")
        .and_then(parse)
        .or_else(|| read("/sys/fs/cgroup/memory/memory.limit_in_bytes").and_then(parse))
}

/// The KDF admission budget and, when the memory limit shaped it (or is small),
/// the startup warning that names the knob. Without an operator cap the
/// CPU-derived budget is lowered so concurrent hashes fit in a quarter of the
/// memory limit; an explicit `UDB_PASSWORD_KDF_MAX_CONCURRENCY` is honored and
/// only warned about when it exceeds that share.
fn budget_with_memory(
    available: usize,
    configured: Option<&str>,
    memory_limit: Option<u64>,
) -> Result<(usize, Option<String>), String> {
    let cpu_budget = concurrency_budget(available, configured)?;
    let Some(limit) = memory_limit else {
        return Ok((cpu_budget, None));
    };
    let memory_budget =
        usize::try_from((limit / KDF_MEMORY_SHARE_DIVISOR / ARGON2_MEMORY_BYTES).max(1))
            .unwrap_or(usize::MAX);
    let limit_mib = limit / (1024 * 1024);
    let per_hash_mib = ARGON2_MEMORY_BYTES / (1024 * 1024);
    if configured.is_some() {
        let warning = (cpu_budget > memory_budget).then(|| {
            format!(
                "UDB_PASSWORD_KDF_MAX_CONCURRENCY allows {cpu_budget} concurrent password hashes \
                 (~{per_hash_mib} MiB each) but the memory limit is {limit_mib} MiB; logins under \
                 load can exhaust memory. Lower it to {memory_budget} or raise the container \
                 memory limit"
            )
        });
        return Ok((cpu_budget, warning));
    }
    let budget = cpu_budget.min(memory_budget);
    let warning = if budget < cpu_budget {
        Some(format!(
            "password KDF concurrency set to {budget} (not {cpu_budget} from the CPU count) \
             because the memory limit is {limit_mib} MiB and each Argon2id hash holds \
             ~{per_hash_mib} MiB; set UDB_PASSWORD_KDF_MAX_CONCURRENCY to choose it, or raise \
             the container memory limit"
        ))
    } else if limit < SMALL_MEMORY_LIMIT_BYTES {
        Some(format!(
            "memory limit is {limit_mib} MiB; password KDF concurrency is {budget} \
             (~{per_hash_mib} MiB per hash). Set UDB_PASSWORD_KDF_MAX_CONCURRENCY lower if \
             logins run out of memory"
        ))
    } else {
        None
    };
    Ok((budget, warning))
}

fn admission() -> Result<Arc<Semaphore>, String> {
    static ADMISSION: OnceLock<Result<Arc<Semaphore>, String>> = OnceLock::new();
    ADMISSION
        .get_or_init(|| {
            let available = std::thread::available_parallelism()
                .map(|count| count.get())
                .unwrap_or(1);
            let configured = std::env::var("UDB_PASSWORD_KDF_MAX_CONCURRENCY").ok();
            budget_with_memory(available, configured.as_deref(), cgroup_memory_limit()).map(
                |(permits, warning)| {
                    if let Some(warning) = warning {
                        tracing::warn!(permits, "{warning}");
                    }
                    Arc::new(Semaphore::new(permits))
                },
            )
        })
        .clone()
}

/// Size the password admission at startup so its memory warning appears in
/// the boot log rather than on the first login.
pub(crate) fn init_admission() {
    if let Err(err) = admission() {
        tracing::warn!(error = %err, "password KDF admission is not configured correctly");
    }
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
    fn kdf_budget_fits_the_memory_limit_and_names_the_knob() {
        const MIB: u64 = 1024 * 1024;
        // No limit: CPU-derived, no warning.
        assert_eq!(budget_with_memory(16, None, None), Ok((15, None)));
        // 256 MiB limit: a quarter (64 MiB) fits 3 hashes of ~19 MiB.
        let (permits, warning) = budget_with_memory(16, None, Some(256 * MIB)).unwrap();
        assert_eq!(permits, 3);
        let warning = warning.expect("a memory-shaped budget is announced");
        assert!(
            warning.contains("UDB_PASSWORD_KDF_MAX_CONCURRENCY"),
            "{warning}"
        );
        assert!(warning.contains("256 MiB"), "{warning}");
        // Tiny limit still admits one hash.
        assert_eq!(budget_with_memory(16, None, Some(32 * MIB)).unwrap().0, 1);
        // Plenty of memory: no cut, no warning.
        assert_eq!(budget_with_memory(4, None, Some(8192 * MIB)), Ok((3, None)));
        // An operator cap is honored, with a warning when it exceeds the share.
        let (permits, warning) = budget_with_memory(16, Some("8"), Some(256 * MIB)).unwrap();
        assert_eq!(permits, 8);
        assert!(warning.unwrap().contains("Lower it to 3"));
        assert!(budget_with_memory(16, Some("0"), Some(256 * MIB)).is_err());
    }

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
