//! `udb dev up --embedded`: a local broker with no Docker. Downloads a
//! PostgreSQL build for this platform once (checked against its published
//! sha256, cached under `~/.udb/postgresql/<version>/<target>`), runs a private
//! cluster under `.udb/dev/` in the project, starts `udb serve` against it, and
//! on first start bootstraps a tenant and admin whose credentials are printed
//! once. The same SQL as production; without Kafka, CDC delivers through the
//! durable PostgreSQL journal and the same authenticated subscriber streams.

use super::*;
use std::io::Read;
use std::path::{Path, PathBuf};

fn private_write(path: &Path, contents: &[u8]) -> Result<(), String> {
    use std::io::Write;
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|err| format!("create {}: {err}", path.display()))?;
    file.write_all(contents)
        .map_err(|err| format!("write {}: {err}", path.display()))
}

fn persistent_secret(dir: &Path, name: &str, environment_key: &str) -> Result<String, String> {
    if let Ok(secret) = env::var(environment_key)
        && !secret.trim().is_empty()
    {
        return Ok(secret);
    }
    let path = dir.join(name);
    if path.exists() {
        let secret =
            fs::read_to_string(&path).map_err(|err| format!("read {}: {err}", path.display()))?;
        if secret.trim().len() < 32 {
            return Err(format!("{} contains an invalid dev secret", path.display()));
        }
        return Ok(secret.trim().to_string());
    }
    let secret = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    private_write(&path, secret.as_bytes())?;
    Ok(secret)
}

fn signing_keys(dir: &Path) -> Result<(String, String), String> {
    use rsa::pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey, LineEnding};
    match (env::var("UDB_JWT_PRIVATE_KEY"), env::var("UDB_JWT_PUBLIC_KEY")) {
        (Ok(private), Ok(public)) if !private.trim().is_empty() && !public.trim().is_empty() => return Ok((private, public)),
        (Ok(_), _) | (_, Ok(_)) => return Err("set both UDB_JWT_PRIVATE_KEY and UDB_JWT_PUBLIC_KEY, or neither for generated dev keys".into()),
        _ => {}
    }
    let private_path = dir.join("jwt-private.pem");
    let public_path = dir.join("jwt-public.pem");
    let key = if private_path.exists() {
        let pem = fs::read_to_string(&private_path)
            .map_err(|err| format!("read dev signing key: {err}"))?;
        rsa::RsaPrivateKey::from_pkcs8_pem(&pem)
            .map_err(|err| format!("decode dev signing key: {err}"))?
    } else {
        let key = rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048)
            .map_err(|err| format!("generate dev signing key: {err}"))?;
        let pem = key
            .to_pkcs8_pem(LineEnding::LF)
            .map_err(|err| format!("encode dev signing key: {err}"))?;
        private_write(&private_path, pem.as_bytes())?;
        key
    };
    let public = rsa::RsaPublicKey::from(&key)
        .to_public_key_pem(LineEnding::LF)
        .map_err(|err| format!("encode dev verification key: {err}"))?;
    if public_path.exists() {
        if fs::read_to_string(&public_path)
            .map_err(|err| format!("read dev verification key: {err}"))?
            != public
        {
            return Err("dev JWT public key does not match the persisted private key".into());
        }
    } else {
        private_write(&public_path, public.as_bytes())?;
    }
    Ok((
        private_path.display().to_string(),
        public_path.display().to_string(),
    ))
}

fn dev_environment(dsn: &str, secret: &str) -> std::collections::BTreeMap<String, String> {
    let defaults = [
        ("UDB_ENV", "development"),
        ("UDB_TLS_REQUIRED", "false"),
        ("UDB_MTLS_REQUIRED", "false"),
        ("UDB_SERVICE_IDENTITY_REQUIRED", "false"),
        ("UDB_SESSION_ENABLED", "true"),
        ("UDB_NATIVE_SERVICES_ENABLED", "true"),
        ("UDB_NATIVE_SERVICES_MIGRATE_ENABLED", "true"),
        ("UDB_NATIVE_CONTROL_PLANE_ENABLED", "true"),
        ("UDB_STARTUP_DRY_RUN", "false"),
        ("UDB_ALLOW_DEGRADED_BACKENDS", "true"),
        ("UDB_GRPC_ADDR", "127.0.0.1:50051"),
        ("UDB_AUTH_GRPC_ADDR", "127.0.0.1:50061"),
        ("UDB_WEBRTC_GRPC_ADDR", "127.0.0.1:50071"),
        ("UDB_HTTP_ADDR", "127.0.0.1:8080"),
        ("UDB_METRICS_ADDR", "127.0.0.1:9090"),
    ];
    let mut values: std::collections::BTreeMap<String, String> = defaults
        .into_iter()
        .map(|(key, default)| {
            (
                key.to_string(),
                env::var(key).unwrap_or_else(|_| default.to_string()),
            )
        })
        .collect();
    values.insert("UDB_PG_DSN".into(), dsn.into());
    values.insert("DATABASE_URL".into(), dsn.into());
    values.insert("UDB_SESSION_HASH_SECRET".into(), secret.into());
    if env::var("UDB_KAFKA_BROKERS")
        .map(|v| v.trim().is_empty())
        .unwrap_or(true)
    {
        values.insert("UDB_CDC_ENABLED".into(), "true".into());
        values.insert("UDB_CDC_JOURNAL_ONLY".into(), "true".into());
    }
    values
}

/// PostgreSQL release used when `--pg-version` is not given.
const DEFAULT_PG_VERSION: &str = "16.4.0";
/// Where the theseus-rs PostgreSQL builds are published (per-target tar.gz +
/// .sha256). `UDB_POSTGRES_BINARIES_URL` points at a mirror.
const DEFAULT_PG_BINARIES_URL: &str =
    "https://github.com/theseus-rs/postgresql-binaries/releases/download";
const DEFAULT_PG_PORT: u16 = 54329;
const DEV_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);
const PG_COMMAND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Register before downloading or starting anything. Keep this monitor alive
/// through bootstrap; PostgreSQL commands finish before their owner is dropped.
#[cfg(feature = "postgres")]
struct DevInterrupt {
    requested: tokio::sync::watch::Receiver<bool>,
    monitor: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "postgres")]
impl DevInterrupt {
    fn new(marker: PathBuf) -> Result<Self, String> {
        #[cfg(unix)]
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .map_err(|err| format!("interrupt handler: {err}"))?;
        #[cfg(unix)]
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|err| format!("termination handler: {err}"))?;
        #[cfg(windows)]
        let mut interrupt =
            tokio::signal::windows::ctrl_c().map_err(|err| format!("interrupt handler: {err}"))?;
        #[cfg(windows)]
        let mut terminate =
            tokio::signal::windows::ctrl_break().map_err(|err| format!("break handler: {err}"))?;
        let (request, requested) = tokio::sync::watch::channel(false);
        let monitor = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = interrupt.recv() => break,
                    _ = terminate.recv() => break,
                    _ = tokio::time::sleep(DEV_POLL_INTERVAL) => {
                        if marker.exists() { break; }
                    }
                }
            }
            let _ = request.send(true);
        });
        Ok(Self { requested, monitor })
    }

    fn is_requested(&self) -> bool {
        *self.requested.borrow()
    }

    fn check(&self) -> Result<(), String> {
        if self.is_requested() {
            Err("embedded development was interrupted".into())
        } else {
            Ok(())
        }
    }

    async fn wait(&self) {
        let mut requested = self.requested.clone();
        let _ = requested.wait_for(|value| *value).await;
    }
}

#[cfg(feature = "postgres")]
impl Drop for DevInterrupt {
    fn drop(&mut self) {
        self.monitor.abort();
    }
}

pub(crate) struct EmbeddedOptions {
    pub(crate) pg_version: String,
    pub(crate) pg_port: u16,
}

impl EmbeddedOptions {
    pub(crate) fn from_args(args: &[String]) -> Self {
        let flag_value = |flag: &str| -> Option<String> {
            args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
        };
        Self {
            pg_version: flag_value("--pg-version")
                .unwrap_or_else(|| DEFAULT_PG_VERSION.to_string()),
            pg_port: flag_value("--pg-port")
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_PG_PORT),
        }
    }
}

/// The Rust target triple the binaries are published under.
pub(crate) fn pg_target() -> Result<&'static str, String> {
    Ok(match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => "x86_64-unknown-linux-gnu",
        ("linux", "aarch64") => "aarch64-unknown-linux-gnu",
        ("macos", "x86_64") => "x86_64-apple-darwin",
        ("macos", "aarch64") => "aarch64-apple-darwin",
        ("windows", "x86_64") => "x86_64-pc-windows-msvc",
        (os, arch) => return Err(format!("no embedded PostgreSQL build for {os}/{arch}")),
    })
}

fn udb_home() -> PathBuf {
    if let Ok(home) = env::var("UDB_HOME")
        && !home.trim().is_empty()
    {
        return PathBuf::from(home);
    }
    let base = env::var("HOME")
        .or_else(|_| env::var("USERPROFILE"))
        .unwrap_or_else(|_| ".".to_string());
    PathBuf::from(base).join(".udb")
}

fn exe(name: &str) -> String {
    if cfg!(windows) {
        format!("{name}.exe")
    } else {
        name.to_string()
    }
}

/// Read one tar archive (already gunzipped) into `dest`. Handles ustar names
/// with prefixes, GNU long names (`L`) and pax `path` records; regular files,
/// directories and (on Unix) symlinks. Paths that escape `dest` are refused.
pub(crate) fn untar(mut reader: impl Read, dest: &Path) -> Result<usize, String> {
    let mut header = [0u8; 512];
    let mut long_name: Option<String> = None;
    let mut files = 0usize;
    loop {
        reader
            .read_exact(&mut header)
            .map_err(|err| format!("truncated tar header: {err}"))?;
        if header.iter().all(|byte| *byte == 0) {
            return Ok(files);
        }
        let field = |range: std::ops::Range<usize>| -> String {
            let raw = &header[range];
            let end = raw.iter().position(|b| *b == 0).unwrap_or(raw.len());
            String::from_utf8_lossy(&raw[..end]).trim().to_string()
        };
        let size = u64::from_str_radix(field(124..136).trim_matches(char::from(0)), 8)
            .map_err(|err| format!("corrupt tar header size: {err}"))?;
        let mode = u32::from_str_radix(field(100..108).trim(), 8).unwrap_or(0o644);
        let kind = header[156];
        let mut data = vec![0u8; size as usize];
        reader
            .read_exact(&mut data)
            .map_err(|err| format!("truncated tar entry: {err}"))?;
        let padding = (512 - (size % 512)) % 512;
        if padding > 0 {
            let mut skip = vec![0u8; padding as usize];
            reader
                .read_exact(&mut skip)
                .map_err(|err| format!("truncated tar padding: {err}"))?;
        }
        match kind {
            b'L' => {
                long_name = Some(
                    String::from_utf8_lossy(&data)
                        .trim_end_matches('\0')
                        .to_string(),
                );
                continue;
            }
            b'x' => {
                // pax records: "<len> key=value\n"
                for record in String::from_utf8_lossy(&data).lines() {
                    if let Some((_, kv)) = record.split_once(' ')
                        && let Some(path) = kv.strip_prefix("path=")
                    {
                        long_name = Some(path.to_string());
                    }
                }
                continue;
            }
            b'g' => continue,
            _ => {}
        }
        let name = match long_name.take() {
            Some(name) => name,
            None => {
                let prefix = field(345..500);
                let base = field(0..100);
                if prefix.is_empty() {
                    base
                } else {
                    format!("{prefix}/{base}")
                }
            }
        };
        let relative = Path::new(&name);
        if relative.is_absolute()
            || relative.components().any(|part| {
                !matches!(
                    part,
                    std::path::Component::Normal(_) | std::path::Component::CurDir
                )
            })
        {
            return Err(format!("tar entry escapes the destination: {name}"));
        }
        let target = dest.join(relative);
        // Refuse writes through an earlier archive symlink, including the leaf.
        let mut ancestor = target.as_path();
        while ancestor != dest {
            if fs::symlink_metadata(ancestor).is_ok_and(|m| m.file_type().is_symlink()) {
                return Err(format!("tar entry traverses a symlink: {name}"));
            }
            ancestor = ancestor
                .parent()
                .ok_or_else(|| format!("invalid tar path: {name}"))?;
        }
        match kind {
            b'5' => {
                fs::create_dir_all(&target).map_err(|err| format!("mkdir {name}: {err}"))?;
            }
            b'2' => {
                #[cfg(unix)]
                {
                    let link = field(157..257);
                    if Path::new(&link).is_absolute() {
                        return Err(format!("absolute tar symlink: {name}"));
                    }
                    let mut depth = relative.parent().map_or(0, |p| {
                        p.components()
                            .filter(|c| matches!(c, std::path::Component::Normal(_)))
                            .count()
                    });
                    for component in Path::new(&link).components() {
                        match component {
                            std::path::Component::ParentDir => {
                                depth = depth.checked_sub(1).ok_or_else(|| {
                                    format!("tar symlink escapes destination: {name}")
                                })?;
                            }
                            std::path::Component::Normal(_) => depth += 1,
                            std::path::Component::CurDir => {}
                            _ => return Err(format!("invalid tar symlink: {name}")),
                        }
                    }
                    if let Some(parent) = target.parent() {
                        fs::create_dir_all(parent).map_err(|err| format!("mkdir: {err}"))?;
                    }
                    let _ = fs::remove_file(&target);
                    std::os::unix::fs::symlink(&link, &target)
                        .map_err(|err| format!("symlink {name}: {err}"))?;
                }
            }
            b'0' | 0 | b'7' => {
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent).map_err(|err| format!("mkdir: {err}"))?;
                }
                fs::write(&target, &data).map_err(|err| format!("write {name}: {err}"))?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = fs::set_permissions(&target, fs::Permissions::from_mode(mode & 0o777));
                }
                #[cfg(not(unix))]
                let _ = mode;
                files += 1;
            }
            _ => {}
        }
    }
}

const PG_CACHE_RECEIPT: &str = ".udb-postgres-cache.json";

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "kind", deny_unknown_fields)]
enum PgCacheEntry {
    Directory,
    File { sha256: String, executable: bool },
    Symlink { target: String },
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct PgCacheReceipt {
    format: u32,
    version: String,
    target: String,
    archive_sha256: String,
    installation: String,
    entries: std::collections::BTreeMap<String, PgCacheEntry>,
}

fn cache_relative(path: &Path, root: &Path) -> Result<String, String> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| "PostgreSQL cache path escaped its root".to_string())?;
    let name = relative
        .to_str()
        .ok_or_else(|| "PostgreSQL cache has a non-UTF8 path".to_string())?;
    if cfg!(unix) && name.contains('\\') {
        return Err("PostgreSQL cache has an ambiguous path separator".into());
    }
    Ok(name.replace('\\', "/"))
}

fn postgres_installation(root: &Path) -> Result<PathBuf, String> {
    let mut candidates = vec![root.to_path_buf()];
    for entry in fs::read_dir(root).map_err(|err| format!("read PostgreSQL cache: {err}"))? {
        let entry = entry.map_err(|err| format!("read PostgreSQL cache entry: {err}"))?;
        if entry
            .file_type()
            .map_err(|err| format!("read PostgreSQL cache type: {err}"))?
            .is_dir()
        {
            candidates.push(entry.path());
        }
    }
    let root = fs::canonicalize(root).map_err(|err| format!("resolve PostgreSQL cache: {err}"))?;
    let complete = |candidate: &Path| -> bool {
        ["initdb", "pg_ctl", "postgres"].iter().all(|name| {
            let binary = candidate.join("bin").join(exe(name));
            let Ok(metadata) = fs::metadata(&binary) else {
                return false;
            };
            if !metadata.is_file()
                || !fs::canonicalize(&binary).is_ok_and(|path| path.starts_with(&root))
            {
                return false;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                metadata.permissions().mode() & 0o111 != 0
            }
            #[cfg(not(unix))]
            true
        })
    };
    let mut found = candidates.into_iter().filter(|path| complete(path));
    let first = found.next().ok_or_else(|| {
        "PostgreSQL cache must contain initdb, pg_ctl and postgres executables".to_string()
    })?;
    if found.next().is_some() {
        return Err("PostgreSQL cache contains ambiguous installations".into());
    }
    Ok(first)
}

fn postgres_cache_entries(
    root: &Path,
) -> Result<std::collections::BTreeMap<String, PgCacheEntry>, String> {
    use sha2::{Digest, Sha256};
    let canonical =
        fs::canonicalize(root).map_err(|err| format!("resolve PostgreSQL cache: {err}"))?;
    let mut entries = std::collections::BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in
            fs::read_dir(directory).map_err(|err| format!("read PostgreSQL cache: {err}"))?
        {
            let path = entry
                .map_err(|err| format!("read PostgreSQL cache entry: {err}"))?
                .path();
            if path == root.join(PG_CACHE_RECEIPT) {
                continue;
            }
            let metadata = fs::symlink_metadata(&path)
                .map_err(|err| format!("read PostgreSQL cache metadata: {err}"))?;
            let value = if metadata.file_type().is_symlink() {
                let resolved = fs::canonicalize(&path)
                    .map_err(|err| format!("resolve PostgreSQL cache link: {err}"))?;
                if !resolved.starts_with(&canonical) {
                    return Err("PostgreSQL cache link escapes its root".into());
                }
                let target = fs::read_link(&path)
                    .map_err(|err| format!("read PostgreSQL cache link: {err}"))?
                    .to_str()
                    .ok_or_else(|| "PostgreSQL cache link has a non-UTF8 target".to_string())?
                    .to_string();
                PgCacheEntry::Symlink { target }
            } else if metadata.is_dir() {
                pending.push(path.clone());
                PgCacheEntry::Directory
            } else if metadata.is_file() {
                let mut file = fs::File::open(&path)
                    .map_err(|err| format!("read PostgreSQL cache file: {err}"))?;
                let mut hasher = Sha256::new();
                let mut buffer = [0u8; 64 * 1024];
                loop {
                    let read = file
                        .read(&mut buffer)
                        .map_err(|err| format!("hash PostgreSQL cache file: {err}"))?;
                    if read == 0 {
                        break;
                    }
                    hasher.update(&buffer[..read]);
                }
                #[cfg(unix)]
                let executable = {
                    use std::os::unix::fs::PermissionsExt;
                    metadata.permissions().mode() & 0o111 != 0
                };
                #[cfg(not(unix))]
                let executable = false;
                PgCacheEntry::File {
                    sha256: format!("{:x}", hasher.finalize()),
                    executable,
                }
            } else {
                return Err("PostgreSQL cache contains an unsupported file type".into());
            };
            entries.insert(cache_relative(&path, root)?, value);
        }
    }
    Ok(entries)
}

fn cached_postgres(
    root: &Path,
    version: &str,
    target: &str,
) -> Result<Option<(PathBuf, PgCacheReceipt)>, String> {
    let metadata = match fs::symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("read PostgreSQL cache root: {error}")),
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err("PostgreSQL cache root must be a real directory".into());
    }
    let receipt_path = root.join(PG_CACHE_RECEIPT);
    let receipt_metadata = fs::symlink_metadata(&receipt_path)
        .map_err(|err| format!("PostgreSQL cache lacks its completion receipt: {err}"))?;
    if !receipt_metadata.is_file() || receipt_metadata.file_type().is_symlink() {
        return Err("PostgreSQL cache completion receipt must be a regular file".into());
    }
    let receipt: PgCacheReceipt = serde_json::from_slice(
        &fs::read(&receipt_path).map_err(|err| format!("read PostgreSQL cache receipt: {err}"))?,
    )
    .map_err(|err| format!("decode PostgreSQL cache receipt: {err}"))?;
    if receipt.format != 1
        || receipt.version != version
        || receipt.target != target
        || receipt.archive_sha256.len() != 64
        || !receipt
            .archive_sha256
            .bytes()
            .all(|c| c.is_ascii_hexdigit())
    {
        return Err("PostgreSQL cache receipt has mismatched version, target or format".into());
    }
    let installation = postgres_installation(root)?;
    if receipt.installation != cache_relative(&installation, root)?
        || receipt.entries != postgres_cache_entries(root)?
    {
        return Err("PostgreSQL cache integrity check failed".into());
    }
    Ok(Some((installation, receipt)))
}

/// Removes only this invocation's checked UUID staging path, never a published cache.
struct PgCacheStaging {
    path: PathBuf,
    parent: PathBuf,
}

impl PgCacheStaging {
    fn create(root: &Path) -> Result<Self, String> {
        let parent = root.parent().ok_or("PostgreSQL cache has no parent")?;
        fs::create_dir_all(parent)
            .map_err(|err| format!("create PostgreSQL cache parent: {err}"))?;
        let parent = fs::canonicalize(parent)
            .map_err(|err| format!("resolve PostgreSQL cache parent: {err}"))?;
        let name = root
            .file_name()
            .ok_or("PostgreSQL cache has no name")?
            .to_string_lossy();
        let path = parent.join(format!(".{name}-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir(&path).map_err(|err| format!("create PostgreSQL staging: {err}"))?;
        Ok(Self { path, parent })
    }

    fn cleanup(&self) -> Result<(), String> {
        let metadata = match fs::symlink_metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(format!("read owned PostgreSQL staging: {error}")),
        };
        if self.path.parent() != Some(self.parent.as_path())
            || !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || fs::canonicalize(&self.path).map_err(|err| format!("resolve staging: {err}"))?
                != self.path
        {
            return Err("refuse cleanup of a replaced PostgreSQL staging path".into());
        }
        fs::remove_dir_all(&self.path)
            .map_err(|err| format!("remove owned PostgreSQL staging: {err}"))
    }
}

impl Drop for PgCacheStaging {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            eprintln!("dev: {error}");
        }
    }
}

/// Serializes publication, not download. Stale locks fail closed; no PID-based stealing.
struct PgCachePublishLock {
    path: PathBuf,
    owner_path: PathBuf,
    owner: Vec<u8>,
    file: Option<fs::File>,
    linked: bool,
}

impl PgCachePublishLock {
    async fn acquire(root: &Path) -> Result<Self, String> {
        use std::io::Write;
        let name = root
            .file_name()
            .ok_or("PostgreSQL cache has no name")?
            .to_string_lossy();
        let token = uuid::Uuid::new_v4().simple().to_string();
        let owner_path = root.with_file_name(format!(".{name}.publish-owner-{token}"));
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&owner_path)
            .map_err(|err| format!("create owned PostgreSQL publication token: {err}"))?;
        let mut lock = Self {
            path: root.with_file_name(format!(".{name}.publish-lock")),
            owner_path,
            owner: token.into_bytes(),
            file: Some(file),
            linked: false,
        };
        let file = lock.file.as_mut().expect("owned publication token");
        file.write_all(&lock.owner)
            .and_then(|_| file.sync_all())
            .map_err(|err| format!("initialize PostgreSQL publication token: {err}"))?;
        // The shared lock name is installed only after its complete owner token
        // is durable. hard_link never replaces another publisher's lock.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            match fs::hard_link(&lock.owner_path, &lock.path) {
                Ok(()) => {
                    lock.linked = true;
                    return Ok(lock);
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err("PostgreSQL publication lock timed out; an abandoned lock requires manual inspection".into());
                    }
                    tokio::time::sleep(DEV_POLL_INTERVAL).await;
                }
                Err(error) => return Err(format!("lock PostgreSQL cache publication: {error}")),
            }
        }
    }
}

impl Drop for PgCachePublishLock {
    fn drop(&mut self) {
        drop(self.file.take());
        // Only the successful linker owns the shared name; a changed owner is
        // never removed. The UUID private token path belongs to this invocation.
        if self.linked
            && fs::symlink_metadata(&self.path)
                .is_ok_and(|metadata| metadata.is_file() && !metadata.file_type().is_symlink())
            && fs::read(&self.path).is_ok_and(|bytes| self.owner == bytes)
        {
            let _ = fs::remove_file(&self.path);
        }
        if fs::symlink_metadata(&self.owner_path)
            .is_ok_and(|metadata| metadata.is_file() && !metadata.file_type().is_symlink())
        {
            let _ = fs::remove_file(&self.owner_path);
        }
    }
}

async fn publish_postgres_cache(
    staging: PgCacheStaging,
    root: &Path,
    receipt: &PgCacheReceipt,
) -> Result<PathBuf, String> {
    let _lock = PgCachePublishLock::acquire(root).await?;
    if let Some((installation, winner)) = cached_postgres(root, &receipt.version, &receipt.target)?
    {
        if winner.archive_sha256 != receipt.archive_sha256 {
            return Err("another PostgreSQL cache publisher installed a different archive".into());
        }
        staging.cleanup()?;
        return Ok(installation);
    }
    fs::rename(&staging.path, root).map_err(|err| format!("publish PostgreSQL cache: {err}"))?;
    cached_postgres(root, &receipt.version, &receipt.target)?
        .map(|(installation, _)| installation)
        .ok_or_else(|| "published PostgreSQL cache is missing".to_string())
}

async fn unpack_postgres_cache(
    root: &Path,
    version: &str,
    target: &str,
    archive: &[u8],
    archive_sha256: &str,
) -> Result<PathBuf, String> {
    let staging = PgCacheStaging::create(root)?;
    let files = untar(flate2::read::GzDecoder::new(archive), &staging.path)?;
    let installation = postgres_installation(&staging.path)?;
    let receipt = PgCacheReceipt {
        format: 1,
        version: version.to_string(),
        target: target.to_string(),
        archive_sha256: archive_sha256.to_string(),
        installation: cache_relative(&installation, &staging.path)?,
        entries: postgres_cache_entries(&staging.path)?,
    };
    let bytes = serde_json::to_vec(&receipt)
        .map_err(|err| format!("encode PostgreSQL cache receipt: {err}"))?;
    private_write(&staging.path.join(PG_CACHE_RECEIPT), &bytes)?;
    fs::OpenOptions::new()
        .write(true)
        .open(staging.path.join(PG_CACHE_RECEIPT))
        .and_then(|file| file.sync_all())
        .map_err(|err| format!("flush PostgreSQL cache receipt: {err}"))?;
    let installation = publish_postgres_cache(staging, root, &receipt).await?;
    eprintln!("dev: validated {files} archive files in {}", root.display());
    Ok(installation)
}

/// The PostgreSQL install directory (holding `bin/`), downloading it if needed.
// The archive/cache identity is an exact numeric release, never a range. Validate
// it before filesystem or download work and reuse the same major at cluster boot.
fn postgres_release_major(version: &str) -> Result<u64, String> {
    let mut components = version.split('.');
    let mut parse = || {
        let value = components.next()?;
        if value.is_empty()
            || !value.bytes().all(|byte| byte.is_ascii_digit())
            || (value.len() > 1 && value.starts_with('0'))
        {
            return None;
        }
        value.parse::<u64>().ok()
    };
    let release = (parse(), parse(), parse());
    let (Some(major), Some(_), Some(_)) = release else {
        return Err("embedded PostgreSQL requires one pinned release version".into());
    };
    if components.next().is_some() {
        return Err("embedded PostgreSQL requires one pinned release version".into());
    }
    if major < 10 {
        return Err("embedded PostgreSQL requires major version 10 or later".into());
    }
    Ok(major)
}

async fn ensure_postgres(version: &str, allow_download: bool) -> Result<PathBuf, String> {
    postgres_release_major(version)?;
    let target = pg_target()?;
    let root = udb_home().join("postgresql").join(version).join(target);
    if let Some((installation, _)) = cached_postgres(&root, version, target)? {
        return Ok(installation);
    }
    if !allow_download {
        return Err(format!(
            "no cached PostgreSQL installation at {}",
            root.display()
        ));
    }
    let archive = if let Ok(path) = env::var("UDB_EMBEDDED_PG_ARCHIVE") {
        let bytes =
            fs::read(&path).map_err(|err| format!("read offline PostgreSQL archive: {err}"))?;
        let checksum = fs::read(format!("{path}.sha256"))
            .map_err(|err| format!("read offline archive checksum: {err}"))?;
        (bytes, checksum)
    } else {
        #[cfg(feature = "http-client")]
        {
            let base = env::var("UDB_POSTGRES_BINARIES_URL")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_PG_BINARIES_URL.to_string());
            let file = format!("postgresql-{version}-{target}.tar.gz");
            let url = format!("{}/{version}/{file}", base.trim_end_matches('/'));
            eprintln!("dev: downloading PostgreSQL {version} for {target} (once) ...");
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(600))
                .build()
                .map_err(|err| format!("http client: {err}"))?;
            let fetch = async |url: &str| -> Result<Vec<u8>, String> {
                let response = client
                    .get(url)
                    .send()
                    .await
                    .map_err(|err| format!("GET {url}: {err}"))?;
                if !response.status().is_success() {
                    return Err(format!("GET {url} returned {}", response.status()));
                }
                response
                    .bytes()
                    .await
                    .map(|b| b.to_vec())
                    .map_err(|err| format!("GET {url}: {err}"))
            };
            (fetch(&url).await?, fetch(&format!("{url}.sha256")).await?)
        }
        #[cfg(not(feature = "http-client"))]
        {
            return Err("this build cannot download PostgreSQL; set UDB_EMBEDDED_PG_ARCHIVE and its .sha256 sidecar".into());
        }
    };
    let expected = String::from_utf8_lossy(&archive.1)
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let actual = {
        use sha2::{Digest, Sha256};
        format!("{:x}", Sha256::digest(&archive.0))
    };
    if expected.len() != 64
        || !expected.bytes().all(|c| c.is_ascii_hexdigit())
        || expected != actual
    {
        return Err(format!(
            "PostgreSQL archive: sha256 {actual} does not match the published {expected}"
        ));
    }
    unpack_postgres_cache(&root, version, target, &archive.0, &actual).await
}

fn dev_dir() -> PathBuf {
    PathBuf::from(".udb").join("dev")
}

#[cfg(feature = "postgres")]
const DEV_INTERRUPTED: &str = "embedded development was interrupted";
#[cfg(feature = "postgres")]
const OWNED_REAP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Only this child's handle may be killed. Tokio's kill-on-drop is a nonblocking
/// panic fallback; normal and cancellation paths explicitly await termination.
#[cfg(feature = "postgres")]
struct EmbeddedChild {
    child: tokio::process::Child,
    phase: &'static str,
}

#[cfg(feature = "postgres")]
impl EmbeddedChild {
    fn spawn(mut command: tokio::process::Command, phase: &'static str) -> Result<Self, String> {
        command.kill_on_drop(true);
        #[cfg(windows)]
        command.creation_flags(0x08000000); // CREATE_NO_WINDOW for background helpers.
        let child = command.spawn().map_err(|err| format!("{phase}: {err}"))?;
        Ok(Self { child, phase })
    }

    async fn terminate(&mut self) -> Result<(), String> {
        if self
            .child
            .try_wait()
            .map_err(|err| format!("{} status: {err}", self.phase))?
            .is_some()
        {
            return Ok(());
        }
        self.child
            .start_kill()
            .map_err(|err| format!("{} terminate: {err}", self.phase))?;
        tokio::time::timeout(OWNED_REAP_TIMEOUT, self.child.wait())
            .await
            .map_err(|_| {
                format!(
                    "{} did not exit after termination; cleanup is incomplete",
                    self.phase
                )
            })?
            .map_err(|err| format!("{} reap: {err}", self.phase))?;
        Ok(())
    }

    async fn wait(
        &mut self,
        budget: std::time::Duration,
        interrupt: Option<&DevInterrupt>,
    ) -> Result<std::process::ExitStatus, String> {
        let cancelled = async {
            match interrupt {
                Some(interrupt) => interrupt.wait().await,
                None => std::future::pending::<()>().await,
            }
        };
        let refusal = tokio::select! {
            result = self.child.wait() => return result.map_err(|err| format!("{} wait: {err}", self.phase)),
            _ = cancelled => DEV_INTERRUPTED.to_string(),
            _ = tokio::time::sleep(budget) => format!("{} exceeded its process deadline", self.phase),
        };
        // A timeout alone does not stop a child. Explicitly terminate and reap
        // this handle; any failure remains a concrete cleanup refusal.
        self.terminate().await?;
        Err(refusal)
    }

    async fn output(
        &mut self,
        budget: std::time::Duration,
        interrupt: &DevInterrupt,
    ) -> Result<(std::process::ExitStatus, Vec<u8>), String> {
        use tokio::io::AsyncReadExt;
        const LIMIT: u64 = 1024 * 1024;
        let stdout = self
            .child
            .stdout
            .take()
            .ok_or_else(|| format!("{} has no owned stdout", self.phase))?;
        let mut readers = tokio::task::JoinSet::new();
        readers.spawn(async move {
            let mut bytes = Vec::new();
            stdout
                .take(LIMIT + 1)
                .read_to_end(&mut bytes)
                .await
                .map(|_| bytes)
        });
        let status = self.wait(budget, Some(interrupt)).await;
        let read = tokio::time::timeout(OWNED_REAP_TIMEOUT, readers.join_next()).await;
        readers.abort_all();
        while readers.join_next().await.is_some() {}
        let status = status?;
        let bytes = read
            .map_err(|_| {
                format!(
                    "{} stdout did not close; output cleanup is incomplete",
                    self.phase
                )
            })?
            .ok_or_else(|| format!("{} stdout reader was missing", self.phase))?
            .map_err(|_| format!("{} stdout reader failed", self.phase))?
            .map_err(|_| format!("{} stdout read failed", self.phase))?;
        if bytes.len() as u64 > LIMIT {
            return Err(format!(
                "{} output exceeded its private response bound",
                self.phase
            ));
        }
        Ok((status, bytes))
    }
}

#[cfg(feature = "postgres")]
async fn embedded_command_status(
    command: tokio::process::Command,
    phase: &'static str,
    budget: std::time::Duration,
    interrupt: Option<&DevInterrupt>,
) -> Result<std::process::ExitStatus, String> {
    EmbeddedChild::spawn(command, phase)?
        .wait(budget, interrupt)
        .await
}

// Settings contain only inputs used by our existing checksum-verified cache and
// bounded process owner. No second downloader or upstream blocking Drop is used.
#[cfg(feature = "postgres")]
struct EmbeddedClusterSettings {
    version: String,
    installation_dir: PathBuf,
    password_file: PathBuf,
    data_dir: PathBuf,
    port: u16,
    password: String,
}

#[cfg(feature = "postgres")]
impl EmbeddedClusterSettings {
    fn binary_dir(&self) -> PathBuf {
        self.installation_dir.join("bin")
    }

    fn url(&self, database: &str) -> String {
        format!(
            "postgresql://postgres:{}@127.0.0.1:{}/{}",
            urlencoding::encode(&self.password),
            self.port,
            urlencoding::encode(database),
        )
    }
}

/// The cluster owns a foreground postmaster handle; pg_ctl is used only for a
/// verified graceful stop. We never signal an arbitrary process from a PID file.
#[cfg(feature = "postgres")]
struct EmbeddedCluster {
    settings: EmbeddedClusterSettings,
    requested_major: u64,
    postmaster: Option<EmbeddedChild>,
}

#[cfg(feature = "postgres")]
impl EmbeddedCluster {
    fn new(settings: EmbeddedClusterSettings) -> Result<Self, String> {
        let requested_major = postgres_release_major(&settings.version)?;
        Ok(Self {
            settings,
            requested_major,
            postmaster: None,
        })
    }

    fn command(&self, binary: &str) -> tokio::process::Command {
        let mut command =
            tokio::process::Command::new(self.settings.binary_dir().join(exe(binary)));
        command.stdin(std::process::Stdio::null());
        command.stdout(std::process::Stdio::null());
        command.stderr(std::process::Stdio::null());
        command
    }

    fn initialized_cluster(&self) -> Result<bool, String> {
        let data = &self.settings.data_dir;
        if !data.join("postgresql.conf").is_file() || !data.join("PG_VERSION").is_file() {
            return Ok(false);
        }
        let contents = fs::read_to_string(data.join("PG_VERSION"))
            .map_err(|err| format!("read PostgreSQL PG_VERSION: {err}"))?;
        let major = contents
            .strip_suffix("\r\n")
            .or_else(|| contents.strip_suffix('\n'))
            .unwrap_or(&contents);
        if major != self.requested_major.to_string() {
            return Err(format!(
                "PostgreSQL PG_VERSION must name requested major {}; the existing data directory was preserved",
                self.requested_major
            ));
        }
        Ok(true)
    }

    fn postmaster_command(&self) -> tokio::process::Command {
        let mut command = self.command("postgres");
        command
            .arg("-D")
            .arg(&self.settings.data_dir)
            .args(["-h", "127.0.0.1", "-p"])
            .arg(self.settings.port.to_string());
        command
    }

    async fn initialize(&self, interrupt: &DevInterrupt) -> Result<(), String> {
        let data = &self.settings.data_dir;
        if self.initialized_cluster()? {
            return Ok(());
        }
        if data.exists()
            && fs::read_dir(data)
                .map_err(|err| format!("read PostgreSQL data directory: {err}"))?
                .next()
                .is_some()
        {
            return Err("embedded PostgreSQL has an incomplete initialization; preserve or move that data directory before retrying".into());
        }
        let mut command = self.command("initdb");
        command
            .arg("--pgdata")
            .arg(data)
            .args([
                "--username",
                "postgres",
                "--auth",
                "password",
                "--encoding",
                "UTF8",
            ])
            .arg("--pwfile")
            .arg(&self.settings.password_file);
        let status = embedded_command_status(
            command,
            "initialize PostgreSQL",
            PG_COMMAND_TIMEOUT,
            Some(interrupt),
        )
        .await?;
        if !status.success() {
            return Err(format!(
                "initialize PostgreSQL exited with {status}; the data directory was preserved"
            ));
        }
        if !self.initialized_cluster()? {
            return Err("initialize PostgreSQL did not produce a complete cluster".into());
        }
        Ok(())
    }

    fn owns_postmaster_pid(&self) -> bool {
        let Some(expected) = self
            .postmaster
            .as_ref()
            .and_then(|process| process.child.id())
        else {
            return false;
        };
        fs::read_to_string(self.settings.data_dir.join("postmaster.pid"))
            .ok()
            .and_then(|contents| contents.lines().next()?.parse::<u32>().ok())
            == Some(expected)
    }

    async fn start(&mut self, interrupt: &DevInterrupt) -> Result<(), String> {
        let mut status = self.command("pg_ctl");
        status.arg("-D").arg(&self.settings.data_dir).arg("status");
        let status = embedded_command_status(
            status,
            "check PostgreSQL",
            OWNED_REAP_TIMEOUT,
            Some(interrupt),
        )
        .await?;
        if status.success() {
            return Err("this embedded cluster is already running; use its owning dev process or stop it before starting another".into());
        }
        if status.code() != Some(3) {
            return Err(format!("check PostgreSQL exited with {status}"));
        }
        let command = self.postmaster_command();
        self.postmaster = Some(EmbeddedChild::spawn(command, "embedded PostgreSQL")?);
        let deadline = std::time::Instant::now() + PG_COMMAND_TIMEOUT;
        loop {
            interrupt.check()?;
            if let Some(status) = self
                .postmaster
                .as_mut()
                .unwrap()
                .child
                .try_wait()
                .map_err(|err| format!("PostgreSQL startup status: {err}"))?
            {
                return Err(format!("PostgreSQL exited during startup ({status})"));
            }
            if self.owns_postmaster_pid() {
                let dsn = self.settings.url("postgres");
                let attempt = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(1)
                    .acquire_timeout(std::time::Duration::from_secs(1))
                    .connect(&dsn);
                if let Ok(Ok(pool)) =
                    tokio::time::timeout(std::time::Duration::from_secs(1), attempt).await
                {
                    let observed = tokio::time::timeout(
                        std::time::Duration::from_secs(1),
                        sqlx::query_scalar::<_, String>("SELECT current_setting('data_directory')")
                            .fetch_one(&pool),
                    )
                    .await;
                    let expected = fs::canonicalize(&self.settings.data_dir)
                        .map_err(|err| format!("cluster path: {err}"))?;
                    let matches = matches!(observed, Ok(Ok(ref path)) if fs::canonicalize(path).is_ok_and(|path| path == expected));
                    tokio::time::timeout(OWNED_REAP_TIMEOUT, pool.close())
                        .await
                        .map_err(|_| "PostgreSQL readiness pool did not close".to_string())?;
                    if !matches {
                        return Err(
                            "the PostgreSQL endpoint is not the owned data directory".into()
                        );
                    }
                    if self.owns_postmaster_pid() {
                        return Ok(());
                    }
                }
            }
            if std::time::Instant::now() >= deadline {
                return Err("owned PostgreSQL did not become ready before its deadline".into());
            }
            tokio::select! {
                _ = interrupt.wait() => return Err(DEV_INTERRUPTED.into()),
                _ = tokio::time::sleep(DEV_POLL_INTERVAL) => {}
            }
        }
    }

    async fn create_database(&self, interrupt: &DevInterrupt) -> Result<(), String> {
        let dsn = self.settings.url("postgres");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(PG_COMMAND_TIMEOUT)
            .connect(&dsn);
        let pool = tokio::select! {
            result = tokio::time::timeout(PG_COMMAND_TIMEOUT, pool) => result
                .map_err(|_| "connect PostgreSQL exceeded its deadline".to_string())?
                .map_err(|_| "connect owned PostgreSQL failed".to_string())?,
            _ = interrupt.wait() => return Err(DEV_INTERRUPTED.into()),
        };
        let work = async {
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)")
                    .bind("udb")
                    .fetch_one(&pool)
                    .await
                    .map_err(|_| "check dev database failed".to_string())?;
            if !exists {
                sqlx::query("CREATE DATABASE \"udb\"")
                    .execute(&pool)
                    .await
                    .map_err(|_| "create dev database failed".to_string())?;
            }
            Ok::<_, String>(())
        };
        let result = tokio::select! {
            result = tokio::time::timeout(PG_COMMAND_TIMEOUT, work) => result
                .unwrap_or_else(|_| Err("prepare dev database exceeded its deadline".into())),
            _ = interrupt.wait() => Err(DEV_INTERRUPTED.into()),
        };
        tokio::time::timeout(OWNED_REAP_TIMEOUT, pool.close())
            .await
            .map_err(|_| "dev database pool did not close".to_string())?;
        result
    }

    async fn stop(&mut self) -> Result<(), String> {
        let Some(process) = self.postmaster.as_mut() else {
            return Ok(());
        };
        if process
            .child
            .try_wait()
            .map_err(|err| format!("PostgreSQL stop status: {err}"))?
            .is_some()
        {
            return Ok(());
        }
        // Only call pg_ctl for the PID bound to this foreground child. If
        // startup never published our pidfile, kill our child handle directly.
        if !self.owns_postmaster_pid() {
            return self.postmaster.as_mut().unwrap().terminate().await;
        }
        let mut command = self.command("pg_ctl");
        command
            .arg("-D")
            .arg(&self.settings.data_dir)
            .args(["-m", "fast", "-w", "-t", "30", "stop"]);
        let graceful =
            embedded_command_status(command, "stop PostgreSQL", PG_COMMAND_TIMEOUT, None).await;
        match graceful {
            Ok(status) if status.success() => {
                self.postmaster
                    .as_mut()
                    .unwrap()
                    .wait(OWNED_REAP_TIMEOUT, None)
                    .await?;
                Ok(())
            }
            _ => {
                self.postmaster.as_mut().unwrap().terminate().await?;
                // The parent handle exited, but a failed graceful stop is not
                // proof that every Windows backend descendant was reaped.
                Err("PostgreSQL graceful shutdown failed; owned parent was terminated, descendant cleanup is unverified".into())
            }
        }
    }
}

/// `udb dev up --embedded`.
pub(crate) fn dev_up_embedded(options: &EmbeddedOptions) -> i32 {
    #[cfg(not(feature = "postgres"))]
    {
        let _ = options;
        eprintln!("dev: embedded mode requires a build with the postgres feature");
        return 1;
    }
    #[cfg(feature = "postgres")]
    let result = (|| {
        let runtime =
            tokio::runtime::Runtime::new().map_err(|err| format!("tokio runtime: {err}"))?;
        runtime.block_on(async {
            fs::create_dir_all(dev_dir()).map_err(|err| format!("create .udb/dev: {err}"))?;
            let marker = dev_dir().join("stop");
            if marker.exists() {
                fs::remove_file(&marker).map_err(|err| format!("reset stop marker: {err}"))?;
            }
            let interrupt = DevInterrupt::new(marker)?;
            let result = dev_up_embedded_inner(options, &interrupt).await;
            match result {
                Err(err) if interrupt.is_requested() && err == DEV_INTERRUPTED => Ok(0),
                result => result,
            }
        })
    })();
    #[cfg(feature = "postgres")]
    match result {
        Ok(code) => code,
        Err(err) => {
            eprintln!("dev: {err}");
            1
        }
    }
}

#[cfg(feature = "postgres")]
async fn dev_up_embedded_inner(
    options: &EmbeddedOptions,
    interrupt: &DevInterrupt,
) -> Result<i32, String> {
    let install = tokio::select! {
        result = ensure_postgres(&options.pg_version, true) => result?,
        _ = interrupt.wait() => return Ok(0),
    };
    interrupt.check()?;
    let directory = env::current_dir()
        .map_err(|err| format!("project directory: {err}"))?
        .join(dev_dir());
    if env::var_os("UDB_PROTO_ROOT").is_none() && env::var_os("UDB_PROTO_DIR").is_none() {
        fs::create_dir_all("proto")
            .map_err(|err| format!("create project proto directory: {err}"))?;
    }
    let data = directory.join("pgdata");
    let secret = persistent_secret(&dev_dir(), "session-secret", "UDB_SESSION_HASH_SECRET")?;
    let encryption_key = persistent_secret(&dev_dir(), "encryption-key", "UDB_ENCRYPTION_KEY")?;
    let (private_key, public_key) = signing_keys(&dev_dir())?;
    let pg_password =
        persistent_secret(&directory, "postgres-password", "UDB_EMBEDDED_PG_PASSWORD")?;
    // Keep the persisted development password contract; the DSN also escapes it.
    if !pg_password
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
    {
        return Err("UDB_EMBEDDED_PG_PASSWORD must contain only letters, digits, _ or -".into());
    }
    let password_file = directory.join("postgres-password");
    if password_file.exists() {
        if fs::read_to_string(&password_file)
            .map_err(|err| format!("read PostgreSQL password: {err}"))?
            .trim()
            != pg_password
        {
            return Err(
                "UDB_EMBEDDED_PG_PASSWORD differs from the persisted cluster password".into(),
            );
        }
    } else {
        private_write(&password_file, pg_password.as_bytes())?;
    }
    let settings = EmbeddedClusterSettings {
        version: options.pg_version.clone(),
        installation_dir: fs::canonicalize(&install)
            .map_err(|err| format!("PostgreSQL cache: {err}"))?,
        password_file,
        data_dir: data.clone(),
        port: options.pg_port,
        password: pg_password,
    };
    let mut postgres = EmbeddedCluster::new(settings)?;
    let mut child: Option<EmbeddedChild> = None;
    let result = async {
    postgres.initialize(interrupt).await?;
    interrupt.check()?;
    postgres.start(interrupt).await?;
    postgres.create_database(interrupt).await?;
    interrupt.check()?;
    let port = options.pg_port.to_string();
    let dsn = postgres.settings.url("udb");
    eprintln!(
        "dev: PostgreSQL {} on 127.0.0.1:{port} (data in {})",
        options.pg_version,
        data.display()
    );

    let current = env::current_exe().map_err(|err| format!("locate udb: {err}"))?;
    let mut child_env = dev_environment(&dsn, &secret);
    child_env.insert("UDB_JWT_PRIVATE_KEY".into(), private_key);
    child_env.insert("UDB_JWT_PUBLIC_KEY".into(), public_key);
    child_env.insert("UDB_ENCRYPTION_KEY".into(), encryption_key);
    let mut broker = tokio::process::Command::new(&current);
    broker.arg("serve").envs(&child_env);
    child = Some(EmbeddedChild::spawn(broker, "embedded broker")?);

    // First start only: bootstrap a tenant + admin once the broker is serving
    // (the schema exists by then) and print the credentials once.
    let marker = dev_dir().join("bootstrap.json");
    if !marker.exists() {
        let grpc = env::var("UDB_GRPC_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".to_string());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
        while !matches!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                tokio::net::TcpStream::connect(client_target_addr_for_dev(&grpc)),
            )
            .await,
            Ok(Ok(_))
        ) {
            interrupt.check()?;
            if std::time::Instant::now() > deadline {
                return Err("the broker did not start listening within 5 minutes".to_string());
            }
            if let Ok(Some(status)) = child.as_mut().unwrap().child.try_wait() {
                return Err(format!("the broker exited during startup ({status})"));
            }
            tokio::select! {
                _ = interrupt.wait() => return Ok(0),
                _ = tokio::time::sleep(DEV_POLL_INTERVAL) => {}
            }
        }
        interrupt.check()?;
        let password = format!(
            "Dev-{}!",
            persistent_secret(&directory, "admin-secret", "UDB_DEV_ADMIN_SECRET")?
        );
        let mut bootstrap_command = tokio::process::Command::new(&current);
        bootstrap_command.envs(&child_env).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::null()).args([
            "auth",
            "bootstrap",
            "user",
            "--username",
            "admin",
            "--email",
            "admin@localhost",
            "--password",
            &password,
            "--tenant",
            "dev",
            "--project",
            "default",
        ]);
        let mut bootstrap_child = EmbeddedChild::spawn(bootstrap_command, "bootstrap admin")?;
        let (status, output) = bootstrap_child.output(PG_COMMAND_TIMEOUT, interrupt).await?;
        if !status.success() {
            return Err(format!("bootstrap admin failed with {status}"));
        }
        let bootstrap: serde_json::Value = serde_json::from_slice(&output)
            .map_err(|err| format!("bootstrap result: {err}"))?;
        let record = serde_json::json!({
            "tenant_id": bootstrap["tenant_id"],
            "user_id": bootstrap["user_id"],
            "username": "admin",
            "password": password,
            "dsn": dsn,
        });
        private_write(
            &marker,
            serde_json::to_string_pretty(&record)
                .map_err(|err| format!("bootstrap credentials: {err}"))?
                .as_bytes(),
        )
        .map_err(|err| format!("write {}: {err}", marker.display()))?;
        eprintln!(
            "dev: bootstrapped tenant {} with user admin / {password} (saved in {}; shown once)",
            bootstrap["tenant_id"],
            marker.display()
        );
    }
    let exit_code = loop {
        if let Some(status) = child.as_mut().unwrap().child.try_wait().map_err(|err| format!("broker: {err}"))? {
            break status.code().unwrap_or(1);
        }
        tokio::select! {
            _ = interrupt.wait() => break 0,
            _ = tokio::time::sleep(DEV_POLL_INTERVAL) => {}
        }
    };
    Ok(exit_code)
    }.await;
    let broker_cleanup = match child.as_mut() {
        Some(child) => child.terminate().await,
        None => Ok(()),
    };
    let postgres_cleanup = postgres.stop().await;
    match (result, broker_cleanup, postgres_cleanup) {
        (result, Ok(()), Ok(())) => result,
        (result, broker, postgres) => {
            let primary = result
                .err()
                .unwrap_or_else(|| "embedded dev cleanup failed".into());
            Err(format!(
                "{primary}; broker cleanup: {}; PostgreSQL cleanup: {}",
                broker.err().unwrap_or_else(|| "complete".into()),
                postgres.err().unwrap_or_else(|| "complete".into())
            ))
        }
    }
}

/// `udb dev down --embedded`: stop the embedded PostgreSQL.
pub(crate) fn dev_down_embedded(options: &EmbeddedOptions) -> i32 {
    #[cfg(not(feature = "postgres"))]
    {
        let _ = options;
        eprintln!("dev: embedded mode requires a build with the postgres feature");
        return 1;
    }
    #[cfg(feature = "postgres")]
    let result = (|| {
        fs::create_dir_all(dev_dir()).map_err(|err| format!("create .udb/dev: {err}"))?;
        fs::write(dev_dir().join("stop"), b"stop").map_err(|err| format!("stop broker: {err}"))?;
        if !dev_dir().join("pgdata").exists() {
            return Ok(());
        }
        let runtime =
            tokio::runtime::Runtime::new().map_err(|err| format!("tokio runtime: {err}"))?;
        runtime.block_on(async {
            let install = ensure_postgres(&options.pg_version, false).await?;
            let data = dev_dir().join("pgdata");
            let pg_ctl = install.join("bin").join(exe("pg_ctl"));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                let mut status = tokio::process::Command::new(&pg_ctl);
                status
                    .arg("-D")
                    .arg(&data)
                    .arg("status")
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null());
                let status = embedded_command_status(
                    status,
                    "check embedded cluster",
                    OWNED_REAP_TIMEOUT,
                    None,
                )
                .await?;
                if status.code() == Some(3) {
                    return Ok(());
                }
                if !status.success() {
                    return Err(format!("check embedded cluster exited with {status}"));
                }
                if std::time::Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(DEV_POLL_INTERVAL).await;
            }
            let mut stop = tokio::process::Command::new(&pg_ctl);
            stop.arg("-D")
                .arg(&data)
                .args(["-m", "fast", "-w", "-t", "30", "stop"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            let status =
                embedded_command_status(stop, "stop embedded cluster", PG_COMMAND_TIMEOUT, None)
                    .await?;
            if !status.success() {
                return Err(format!("stop embedded cluster exited with {status}"));
            }
            Ok(())
        })
    })();
    #[cfg(feature = "postgres")]
    match result {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("dev: {err}");
            1
        }
    }
}

fn client_target_addr_for_dev(addr: &str) -> String {
    let addr = addr.trim().trim_start_matches("http://");
    match addr.strip_prefix("0.0.0.0:") {
        Some(port) => format!("127.0.0.1:{port}"),
        None => addr.to_string(),
    }
}

#[cfg(test)]
mod dev_embedded_tests {
    use super::*;

    fn header(name: &str, size: usize, kind: u8) -> [u8; 512] {
        let mut h = [0u8; 512];
        h[..name.len()].copy_from_slice(name.as_bytes());
        h[100..107].copy_from_slice(b"0000755");
        let size = format!("{size:011o}");
        h[124..135].copy_from_slice(size.as_bytes());
        h[156] = kind;
        h[257..262].copy_from_slice(b"ustar");
        h
    }

    #[test]
    fn untar_extracts_files_and_refuses_escapes() {
        let mut archive = Vec::new();
        archive.extend_from_slice(&header("pg/bin/", 0, b'5'));
        archive.extend_from_slice(&header("pg/bin/initdb", 5, b'0'));
        let mut body = b"hello".to_vec();
        body.resize(512, 0);
        archive.extend_from_slice(&body);
        archive.extend_from_slice(&[0u8; 1024]);
        let dir = std::env::temp_dir().join(format!("udb-untar-{}", uuid::Uuid::new_v4()));
        assert_eq!(untar(archive.as_slice(), &dir).unwrap(), 1);
        assert_eq!(std::fs::read(dir.join("pg/bin/initdb")).unwrap(), b"hello");
        let _ = std::fs::remove_dir_all(&dir);

        let mut evil = Vec::new();
        evil.extend_from_slice(&header("../escape", 0, b'0'));
        evil.extend_from_slice(&[0u8; 1024]);
        assert!(untar(evil.as_slice(), &std::env::temp_dir()).is_err());
        assert!(
            untar(&[0u8; 100][..], &std::env::temp_dir())
                .unwrap_err()
                .contains("truncated tar header")
        );
    }

    #[cfg(unix)]
    #[test]
    fn untar_refuses_symlinks_outside_destination() {
        let mut h = header("pg/lib/evil", 0, b'2');
        h[157..174].copy_from_slice(b"../../../outside/");
        let mut archive = h.to_vec();
        archive.extend_from_slice(&[0u8; 1024]);
        let dir = std::env::temp_dir().join(format!("udb-untar-{}", uuid::Uuid::new_v4()));
        assert!(
            untar(archive.as_slice(), &dir)
                .unwrap_err()
                .contains("escapes destination")
        );
    }
    fn cache_parent() -> PgCacheStaging {
        PgCacheStaging::create(&std::env::temp_dir().join("udb-pg-cache-proof")).unwrap()
    }

    #[cfg(feature = "postgres")]
    fn cluster_settings(root: &Path, version: &str) -> EmbeddedClusterSettings {
        EmbeddedClusterSettings {
            version: version.into(),
            installation_dir: root.join("missing-installation"),
            password_file: root.join("password"),
            data_dir: root.join("data"),
            port: DEFAULT_PG_PORT,
            password: "filesystem-preflight-only".into(),
        }
    }

    #[cfg(feature = "postgres")]
    fn initialize_existing_cluster(cluster: &EmbeddedCluster, root: &Path) -> Result<(), String> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let interrupt = DevInterrupt::new(root.join("stop")).unwrap();
            cluster.initialize(&interrupt).await
        })
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn cluster_constructor_refuses_ranges_and_incomplete_release_requirements() {
        let parent = cache_parent();
        for requirement in [
            "*",
            "^16",
            ">=16",
            "16.*",
            "=16",
            "=16.4",
            "^16.4.0",
            ">=16.4.0",
            ">=16.4.0,<17.0.0",
            "16.4",
            "16.4.0.1",
            "016.4.0",
            "16.04.0",
            "16.4.00",
            "16.4.0-beta",
            "16.4.0/other",
            "16.4.0\n",
            "18446744073709551616.4.0",
        ] {
            let mut settings = cluster_settings(&parent.path, "16.4.0");
            settings.version = requirement.into();
            let refusal = match EmbeddedCluster::new(settings) {
                Err(refusal) => refusal,
                Ok(_) => panic!("{requirement} does not identify one pinned release"),
            };
            assert!(refusal.contains("one pinned release version"), "{refusal}");
        }
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn owned_cluster_connection_preserves_escaped_credentials_and_database() {
        let parent = cache_parent();
        let mut settings = cluster_settings(&parent.path, "16.4.0");
        settings.password = "synthetic:@/?#% credential".into();
        let connection = settings
            .url("synthetic/database")
            .parse::<sqlx::postgres::PgConnectOptions>()
            .unwrap();
        assert_eq!(connection.get_host(), "127.0.0.1");
        assert_eq!(connection.get_port(), DEFAULT_PG_PORT);
        assert_eq!(connection.get_username(), "postgres");
        assert_eq!(connection.get_database(), Some("synthetic/database"));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn existing_cluster_reuses_only_the_requested_major_without_initdb() {
        let parent = cache_parent();
        let data = parent.path.join("data");
        fs::create_dir(&data).unwrap();
        let config = b"# existing persisted cluster\n";
        let payload = b"preserved user data";
        fs::write(data.join("postgresql.conf"), config).unwrap();
        fs::write(data.join("owned-data"), payload).unwrap();
        for version in ["16.4.0", "16.10.0"] {
            let cluster = EmbeddedCluster::new(cluster_settings(&parent.path, version)).unwrap();
            assert!(!cluster.settings.binary_dir().join(exe("initdb")).exists());
            for marker in [b"16".as_slice(), b"16\n", b"16\r\n"] {
                fs::write(data.join("PG_VERSION"), marker).unwrap();
                initialize_existing_cluster(&cluster, &parent.path).unwrap();
                assert_eq!(fs::read(data.join("PG_VERSION")).unwrap(), marker);
                assert_eq!(fs::read(data.join("postgresql.conf")).unwrap(), config);
                assert_eq!(fs::read(data.join("owned-data")).unwrap(), payload);
            }
        }
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn existing_cluster_refuses_wrong_or_malformed_major_without_changing_data() {
        let parent = cache_parent();
        let cluster = EmbeddedCluster::new(cluster_settings(&parent.path, "16.4.0")).unwrap();
        let data = &cluster.settings.data_dir;
        fs::create_dir(data).unwrap();
        let config = b"# persisted PostgreSQL configuration\n";
        let payload = b"existing owned data";
        fs::write(data.join("postgresql.conf"), config).unwrap();
        fs::write(data.join("owned-data"), payload).unwrap();
        for marker in [
            b"15\n".as_slice(),
            b"17\n",
            b"",
            b"16.4\n",
            b"9.6\n",
            b"016\n",
            b" 16\n",
            b"16 \n",
            b"16\n\n",
            b"not-a-version\n",
            &[0xff],
        ] {
            fs::write(data.join("PG_VERSION"), marker).unwrap();
            let refusal = initialize_existing_cluster(&cluster, &parent.path).unwrap_err();
            assert!(refusal.contains("PG_VERSION"), "{refusal}");
            assert_eq!(fs::read(data.join("PG_VERSION")).unwrap(), marker);
            assert_eq!(fs::read(data.join("postgresql.conf")).unwrap(), config);
            assert_eq!(fs::read(data.join("owned-data")).unwrap(), payload);
        }
        fs::remove_file(data.join("PG_VERSION")).unwrap();
        let refusal = initialize_existing_cluster(&cluster, &parent.path).unwrap_err();
        assert!(refusal.contains("incomplete initialization"), "{refusal}");
        assert!(!data.join("PG_VERSION").exists());
        assert_eq!(fs::read(data.join("postgresql.conf")).unwrap(), config);
        assert_eq!(fs::read(data.join("owned-data")).unwrap(), payload);
        let refusal = match EmbeddedCluster::new(cluster_settings(&parent.path, "9.6.24")) {
            Err(refusal) => refusal,
            Ok(_) => panic!("pre-10 cluster versions require a separate supported layout"),
        };
        assert!(refusal.contains("major version 10 or later"));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn foreground_postmaster_command_keeps_default_fsync_enabled() {
        let parent = cache_parent();
        let cluster = EmbeddedCluster::new(cluster_settings(&parent.path, "16.4.0")).unwrap();
        let command = cluster.postmaster_command();
        let command = command.as_std();
        assert_eq!(
            command.get_program(),
            cluster
                .settings
                .binary_dir()
                .join(exe("postgres"))
                .as_os_str()
        );
        let args: Vec<_> = command.get_args().collect();
        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "-D" && pair[1] == cluster.settings.data_dir.as_os_str())
        );
        assert!(
            !args
                .iter()
                .any(|arg| *arg == "-F" || *arg == "fsync=off" || *arg == "--fsync=off")
        );
    }

    fn cache_fixture(root: &Path) -> (PgCacheStaging, PgCacheReceipt) {
        let staging = PgCacheStaging::create(root).unwrap();
        fs::create_dir(staging.path.join("bin")).unwrap();
        for name in ["initdb", "pg_ctl", "postgres"] {
            let path = staging.path.join("bin").join(exe(name));
            fs::write(&path, format!("cache filesystem fixture {name}")).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
            }
        }
        // This exercises the real cache filesystem implementation, not a PG
        // process/startup proof. Actual archive and served checks stay in CI.
        let receipt = PgCacheReceipt {
            format: 1,
            version: DEFAULT_PG_VERSION.to_string(),
            target: pg_target().unwrap().to_string(),
            archive_sha256: "a".repeat(64),
            installation: cache_relative(
                &postgres_installation(&staging.path).unwrap(),
                &staging.path,
            )
            .unwrap(),
            entries: postgres_cache_entries(&staging.path).unwrap(),
        };
        private_write(
            &staging.path.join(PG_CACHE_RECEIPT),
            &serde_json::to_vec(&receipt).unwrap(),
        )
        .unwrap();
        (staging, receipt)
    }

    #[test]
    fn postgres_cache_concurrent_publishers_reuse_winner_and_remove_owned_staging() {
        let parent = cache_parent();
        let root = parent.path.join("target");
        let (first, first_receipt) = cache_fixture(&root);
        let (second, second_receipt) = cache_fixture(&root);
        let first_path = first.path.clone();
        let second_path = second.path.clone();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
            let first_barrier = barrier.clone();
            let first_root = root.clone();
            let first_task = tokio::spawn(async move {
                first_barrier.wait().await;
                publish_postgres_cache(first, &first_root, &first_receipt).await
            });
            let second_barrier = barrier.clone();
            let second_root = root.clone();
            let second_task = tokio::spawn(async move {
                second_barrier.wait().await;
                publish_postgres_cache(second, &second_root, &second_receipt).await
            });
            barrier.wait().await;
            assert_eq!(first_task.await.unwrap().unwrap(), root);
            assert_eq!(second_task.await.unwrap().unwrap(), root);
        });
        assert!(!first_path.exists());
        assert!(!second_path.exists());
        assert!(
            cached_postgres(&root, DEFAULT_PG_VERSION, pg_target().unwrap())
                .unwrap()
                .is_some()
        );
        assert_eq!(fs::read_dir(&parent.path).unwrap().count(), 1);
    }

    #[test]
    fn postgres_cache_integrity_refuses_corruption_without_overwriting_existing_install() {
        let parent = cache_parent();
        let root = parent.path.join("target");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (first, receipt) = cache_fixture(&root);
        runtime
            .block_on(publish_postgres_cache(first, &root, &receipt))
            .unwrap();
        assert!(cached_postgres(&root, "different-version", pg_target().unwrap()).is_err());
        assert!(cached_postgres(&root, DEFAULT_PG_VERSION, "different-target").is_err());
        let executable = root.join("bin").join(exe("postgres"));
        fs::write(&executable, b"changed cached executable").unwrap();
        assert!(cached_postgres(&root, DEFAULT_PG_VERSION, pg_target().unwrap()).is_err());
        let (replacement, replacement_receipt) = cache_fixture(&root);
        let replacement_path = replacement.path.clone();
        assert!(
            runtime
                .block_on(publish_postgres_cache(
                    replacement,
                    &root,
                    &replacement_receipt
                ))
                .is_err()
        );
        assert_eq!(fs::read(executable).unwrap(), b"changed cached executable");
        assert!(!replacement_path.exists());
        assert_eq!(fs::read_dir(&parent.path).unwrap().count(), 1);
    }

    #[test]
    fn postgres_cache_incomplete_existing_install_is_not_overwritten() {
        let parent = cache_parent();
        let root = parent.path.join("target");
        fs::create_dir_all(root.join("bin")).unwrap();
        let initial = root.join("bin").join(exe("initdb"));
        fs::write(&initial, b"incomplete prior installation").unwrap();
        let (replacement, receipt) = cache_fixture(&root);
        let staging = replacement.path.clone();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        assert!(
            runtime
                .block_on(publish_postgres_cache(replacement, &root, &receipt))
                .is_err()
        );
        assert_eq!(fs::read(initial).unwrap(), b"incomplete prior installation");
        assert!(!staging.exists());
        assert!(!root.join(PG_CACHE_RECEIPT).exists());
        assert_eq!(fs::read_dir(&parent.path).unwrap().count(), 1);
    }

    #[test]
    fn postgres_cache_failed_extraction_removes_only_its_owned_staging() {
        let parent = cache_parent();
        let root = parent.path.join("target");
        let sentinel = parent.path.join("unrelated-owner");
        fs::write(&sentinel, b"preserved").unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        assert!(
            runtime
                .block_on(unpack_postgres_cache(
                    &root,
                    DEFAULT_PG_VERSION,
                    pg_target().unwrap(),
                    b"not a gzip archive",
                    &"a".repeat(64)
                ))
                .is_err()
        );
        assert!(!root.exists());
        assert_eq!(fs::read(sentinel).unwrap(), b"preserved");
        assert_eq!(fs::read_dir(&parent.path).unwrap().count(), 1);
    }

    #[test]
    fn postgres_cache_cancelled_publisher_cleans_staging_and_preserves_other_lock_owner() {
        let parent = cache_parent();
        let root = parent.path.join("target");
        let (staging, receipt) = cache_fixture(&root);
        let staging_path = staging.path.clone();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let lock = PgCachePublishLock::acquire(&root).await.unwrap();
            let lock_path = lock.path.clone();
            let (entered, observed) = tokio::sync::oneshot::channel();
            let task_root = root.clone();
            let publisher = tokio::spawn(async move {
                entered.send(()).unwrap();
                publish_postgres_cache(staging, &task_root, &receipt).await
            });
            observed.await.unwrap();
            // Observe the real acquisition path's complete owner file. Merely
            // observing that the task entered would allow cancellation before
            // it actually tried to contend for the held publication lock.
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let owners = fs::read_dir(&parent.path)
                        .unwrap()
                        .filter_map(Result::ok)
                        .filter(|entry| {
                            entry
                                .file_name()
                                .to_string_lossy()
                                .starts_with(".target.publish-owner-")
                        })
                        .count();
                    if owners == 2 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("publisher actually contends for owned cache lock");
            publisher.abort();
            assert!(publisher.await.unwrap_err().is_cancelled());
            assert!(!staging_path.exists());
            assert!(!root.exists());
            assert_eq!(fs::read(&lock_path).unwrap(), lock.owner);
            drop(lock);
            assert!(!lock_path.exists());
        });
        assert_eq!(fs::read_dir(&parent.path).unwrap().count(), 0);
    }
}
