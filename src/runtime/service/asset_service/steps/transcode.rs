//! The TRANSCODE byte-step primitives for the native `AssetService`: the input/
//! output byte caps, the vendored-ffmpeg binary resolution (env override →
//! platform-layout search), the output-format allowlist, and the ffmpeg driver.
//! Always compiled (transcode uses a vendored ffmpeg BINARY at runtime, not a
//! Cargo feature). Extracted verbatim from the former god file.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use uuid::Uuid;

use super::ByteStepParams;

/// Largest source object (in bytes) a transcode step will send to ffmpeg.
/// 512 MiB keeps broker temp-disk/memory bounded until streaming transcode jobs
/// are added.
pub(crate) const MAX_TRANSCODE_INPUT_BYTES: u64 = 512 * 1024 * 1024;

/// Largest transcode output (in bytes) accepted back from ffmpeg.
pub(crate) const MAX_TRANSCODE_OUTPUT_BYTES: u64 = 512 * 1024 * 1024;

const UDB_FFMPEG_BIN_ENV: &str = "UDB_FFMPEG_BIN";
const UDB_FFMPEG_ROOT_ENV: &str = "UDB_FFMPEG_ROOT";
const DEFAULT_FFMPEG_TIMEOUT_SECS: u64 = 120;
const MAX_FFMPEG_TIMEOUT_SECS: u64 = 600;

pub(crate) fn check_transcode_input_bytes(len: u64) -> Result<(), String> {
    if len > MAX_TRANSCODE_INPUT_BYTES {
        return Err(format!(
            "source media is {len} bytes, exceeds the {MAX_TRANSCODE_INPUT_BYTES}-byte transcode input limit"
        ));
    }
    Ok(())
}

pub(crate) fn check_transcode_output_bytes(len: u64) -> Result<(), String> {
    if len > MAX_TRANSCODE_OUTPUT_BYTES {
        return Err(format!(
            "transcoded media is {len} bytes, exceeds the {MAX_TRANSCODE_OUTPUT_BYTES}-byte transcode output limit"
        ));
    }
    Ok(())
}

fn ffmpeg_timeout() -> Duration {
    static TIMEOUT: OnceLock<Duration> = OnceLock::new();
    *TIMEOUT.get_or_init(|| {
        let secs = std::env::var("UDB_ASSET_FFMPEG_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|secs| *secs > 0)
            .unwrap_or(DEFAULT_FFMPEG_TIMEOUT_SECS)
            .min(MAX_FFMPEG_TIMEOUT_SECS);
        Duration::from_secs(secs)
    })
}

pub(crate) fn ffmpeg_exe_name() -> &'static str {
    if cfg!(windows) {
        "ffmpeg.exe"
    } else {
        "ffmpeg"
    }
}

pub(crate) fn ffmpeg_platform_dir() -> &'static str {
    if cfg!(windows) {
        "windows"
    } else if cfg!(target_os = "macos") {
        "macos"
    } else {
        "linux"
    }
}

fn ffmpeg_under_root(root: impl AsRef<Path>) -> PathBuf {
    root.as_ref()
        .join("bin")
        .join(ffmpeg_platform_dir())
        .join(ffmpeg_exe_name())
}

fn push_ffmpeg_root(candidates: &mut Vec<PathBuf>, root: PathBuf) {
    let path = ffmpeg_under_root(root);
    if !candidates.iter().any(|candidate| candidate == &path) {
        candidates.push(path);
    }
}

pub(crate) fn vendored_ffmpeg_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(root) = std::env::var(UDB_FFMPEG_ROOT_ENV) {
        let root = root.trim();
        if !root.is_empty() {
            push_ffmpeg_root(&mut candidates, PathBuf::from(root));
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        push_ffmpeg_root(&mut candidates, cwd.join("third_party").join("ffmpeg"));
    }
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        // Raw release bundles can place `third_party/ffmpeg` beside the binary.
        // The release Docker image is covered by the current-working-directory
        // candidate because it starts in `/app` and copies `third_party` there.
        push_ffmpeg_root(&mut candidates, dir.join("third_party").join("ffmpeg"));
        if let Some(parent) = dir.parent() {
            push_ffmpeg_root(&mut candidates, parent.join("third_party").join("ffmpeg"));
        }
    }
    push_ffmpeg_root(
        &mut candidates,
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("third_party")
            .join("ffmpeg"),
    );
    candidates
}

#[cfg(test)]
pub(crate) fn vendored_ffmpeg_path() -> PathBuf {
    vendored_ffmpeg_candidates()
        .into_iter()
        .next()
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("third_party")
                .join("ffmpeg")
                .join("bin")
                .join(ffmpeg_platform_dir())
                .join(ffmpeg_exe_name())
        })
}

fn resolve_ffmpeg_binary() -> Result<PathBuf, String> {
    static FFMPEG_BINARY: OnceLock<Result<PathBuf, String>> = OnceLock::new();
    FFMPEG_BINARY
        .get_or_init(resolve_ffmpeg_binary_uncached)
        .clone()
}

fn resolve_ffmpeg_binary_uncached() -> Result<PathBuf, String> {
    if let Ok(path) = std::env::var(UDB_FFMPEG_BIN_ENV) {
        let path = PathBuf::from(path.trim());
        if path.is_file() {
            return Ok(path);
        }
        return Err(format!(
            "{UDB_FFMPEG_BIN_ENV} points to missing ffmpeg binary: {}",
            path.display()
        ));
    }
    let candidates = vendored_ffmpeg_candidates();
    for candidate in &candidates {
        if candidate.is_file() {
            return Ok(candidate.clone());
        }
    }
    let searched = candidates
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Err(format!(
        "ffmpeg transcode executor unavailable: missing vendored binary; searched: {searched} (or set {UDB_FFMPEG_BIN_ENV} / {UDB_FFMPEG_ROOT_ENV})"
    ))
}

pub(crate) fn resolve_transcode_output_format(
    requested: Option<&str>,
) -> Result<(&'static str, &'static str, &'static str), String> {
    match requested.unwrap_or("mp4") {
        "mp4" => Ok(("mp4", "video/mp4", "mp4")),
        other => Err(format!(
            "unsupported transcode output format '{other}' (this build supports: mp4)"
        )),
    }
}

pub(crate) async fn run_ffmpeg_transcode(
    source: &[u8],
    params: &ByteStepParams,
) -> Result<(Vec<u8>, &'static str, &'static str), String> {
    check_transcode_input_bytes(source.len() as u64)?;
    let (_container, content_type, ext) =
        resolve_transcode_output_format(params.format.as_deref())?;
    let ffmpeg = resolve_ffmpeg_binary()?;
    let job_id = Uuid::new_v4().to_string();
    let work_dir = std::env::temp_dir().join("udb-asset-ffmpeg").join(job_id);
    tokio::fs::create_dir_all(&work_dir)
        .await
        .map_err(|err| format!("create ffmpeg work dir failed: {err}"))?;
    let input_path = work_dir.join("input.bin");
    let output_path = work_dir.join(format!("output.{ext}"));
    let cleanup_dir = work_dir.clone();

    let result = async {
        tokio::fs::write(&input_path, source)
            .await
            .map_err(|err| format!("write ffmpeg input failed: {err}"))?;
        let mut command = tokio::process::Command::new(&ffmpeg);
        command
            .arg("-nostdin")
            .arg("-hide_banner")
            .arg("-loglevel")
            .arg("error")
            .arg("-y")
            .arg("-i")
            .arg(&input_path)
            .arg("-map")
            .arg("0:v:0?")
            .arg("-map")
            .arg("0:a:0?")
            .arg("-c:v")
            .arg("libx264")
            .arg("-preset")
            .arg("veryfast")
            .arg("-movflags")
            .arg("+faststart")
            .arg("-c:a")
            .arg("aac")
            .arg("-f")
            .arg("mp4")
            .arg(&output_path)
            .kill_on_drop(true);
        let output = tokio::time::timeout(ffmpeg_timeout(), command.output())
            .await
            .map_err(|_| "ffmpeg transcode timed out".to_string())?
            .map_err(|err| format!("spawn ffmpeg failed: {err}"))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let message = stderr.trim();
            return Err(if message.is_empty() {
                format!("ffmpeg transcode failed with status {}", output.status)
            } else {
                format!("ffmpeg transcode failed: {message}")
            });
        }
        let out = tokio::fs::read(&output_path)
            .await
            .map_err(|err| format!("read ffmpeg output failed: {err}"))?;
        check_transcode_output_bytes(out.len() as u64)?;
        Ok((out, content_type, ext))
    }
    .await;

    let _ = tokio::fs::remove_dir_all(cleanup_dir).await;
    result
}
