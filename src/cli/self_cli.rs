//! `udb self verify|install`: fetch a release's `manifest.json`, check every
//! byte against its published sha256, and (install) write the verified binary.
//! Consumers kept several hundred lines of shell to do this by hand.

use super::*;

/// Where release assets are published; `UDB_RELEASE_BASE_URL` points at a
/// mirror (the URL that holds the `v<version>/` directories).
const DEFAULT_RELEASE_BASE: &str = "https://github.com/fahara02/udb/releases/download";

/// One asset row of `manifest.json` (scripts/gen-release-manifest.mjs).
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
pub(crate) struct ManifestAsset {
    pub(crate) name: String,
    pub(crate) os: String,
    pub(crate) arch: String,
    pub(crate) tier: String,
    pub(crate) sha256: String,
    pub(crate) size: u64,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct ReleaseManifest {
    pub(crate) version: String,
    pub(crate) assets: Vec<ManifestAsset>,
}

/// This machine's `<os>`/`<arch>` in the release naming scheme.
pub(crate) fn host_platform() -> (&'static str, &'static str) {
    let os = match std::env::consts::OS {
        "macos" => "darwin",
        other => other,
    };
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    };
    (os, arch)
}

/// Pick the asset for a platform and tier ("portable" when empty).
pub(crate) fn select_asset<'a>(
    manifest: &'a ReleaseManifest,
    os: &str,
    arch: &str,
    tier: &str,
) -> Result<&'a ManifestAsset, String> {
    let tier = if tier.trim().is_empty() {
        "portable"
    } else {
        tier.trim()
    };
    manifest
        .assets
        .iter()
        .find(|asset| asset.os == os && asset.arch == arch && asset.tier == tier)
        .ok_or_else(|| {
            let available: Vec<String> = manifest
                .assets
                .iter()
                .map(|asset| format!("{}/{}/{}", asset.os, asset.arch, asset.tier))
                .collect();
            format!(
                "release {} has no {os}/{arch}/{tier} binary (available: {})",
                manifest.version,
                available.join(", ")
            )
        })
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}

/// Compare bytes against a manifest row: size and sha256 must both match.
pub(crate) fn check_asset_bytes(asset: &ManifestAsset, bytes: &[u8]) -> Result<(), String> {
    if bytes.len() as u64 != asset.size {
        return Err(format!(
            "{}: size {} does not match the manifest's {}",
            asset.name,
            bytes.len(),
            asset.size
        ));
    }
    let actual = sha256_hex(bytes);
    if !actual.eq_ignore_ascii_case(asset.sha256.trim()) {
        return Err(format!(
            "{}: sha256 {actual} does not match the manifest's {}",
            asset.name, asset.sha256
        ));
    }
    Ok(())
}

fn release_base() -> String {
    env::var("UDB_RELEASE_BASE_URL")
        .ok()
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_RELEASE_BASE.to_string())
}

#[cfg(feature = "http-client")]
fn fetch(url: &str) -> Result<Vec<u8>, String> {
    let response = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .map_err(|err| format!("http client: {err}"))?
        .get(url)
        .send()
        .map_err(|err| format!("GET {url} failed: {err}"))?;
    if !response.status().is_success() {
        return Err(format!("GET {url} returned {}", response.status()));
    }
    response
        .bytes()
        .map(|body| body.to_vec())
        .map_err(|err| format!("GET {url} body failed: {err}"))
}

#[cfg(not(feature = "http-client"))]
fn fetch(url: &str) -> Result<Vec<u8>, String> {
    Err(format!(
        "this udb build has no HTTP client; download {url} yourself and run `udb self verify --file <path> --manifest <manifest.json>`"
    ))
}

/// Load the manifest for `version`, either from `--manifest <path>` or from the
/// release, checking the downloaded manifest against `manifest.json.sha256`.
fn load_manifest(version: &str, manifest_path: &str) -> Result<ReleaseManifest, String> {
    let raw = if manifest_path.trim().is_empty() {
        let base = format!("{}/v{version}", release_base());
        let raw = fetch(&format!("{base}/manifest.json"))?;
        let sidecar = fetch(&format!("{base}/manifest.json.sha256"))?;
        let expected = String::from_utf8_lossy(&sidecar)
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_string();
        if !sha256_hex(&raw).eq_ignore_ascii_case(&expected) {
            return Err(format!(
                "manifest.json for v{version} does not match manifest.json.sha256; refusing it"
            ));
        }
        raw
    } else {
        fs::read(manifest_path).map_err(|err| format!("read {manifest_path} failed: {err}"))?
    };
    let manifest: ReleaseManifest =
        serde_json::from_slice(&raw).map_err(|err| format!("manifest.json is invalid: {err}"))?;
    if manifest.version.trim_start_matches('v') != version.trim_start_matches('v') {
        return Err(format!(
            "manifest is for {} but {version} was asked for",
            manifest.version
        ));
    }
    Ok(manifest)
}

pub(crate) fn run_self_command(command: SelfCommand) -> i32 {
    match run_self(command) {
        Ok(value) => {
            output_json(&value, "self command result");
            0
        }
        Err(err) => {
            eprintln!("udb self: {err}");
            1
        }
    }
}

fn run_self(command: SelfCommand) -> Result<serde_json::Value, String> {
    let SelfCommand {
        install,
        version,
        tier,
        file,
        manifest,
        to,
    } = command;
    let version = if version.trim().is_empty() {
        env!("CARGO_PKG_VERSION").to_string()
    } else {
        version.trim().trim_start_matches('v').to_string()
    };
    let manifest = load_manifest(&version, &manifest)?;
    let (os, arch) = host_platform();
    if !install {
        // Verify a local file (default: this executable) against the manifest
        // row with the same name, or the host's row for the tier.
        let path = if file.trim().is_empty() {
            std::env::current_exe().map_err(|err| format!("locate this executable: {err}"))?
        } else {
            std::path::PathBuf::from(file.trim())
        };
        let bytes =
            fs::read(&path).map_err(|err| format!("read {} failed: {err}", path.display()))?;
        let file_name = path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_default();
        let asset = match manifest.assets.iter().find(|asset| asset.name == file_name) {
            Some(asset) => asset,
            None => select_asset(&manifest, os, arch, &tier)?,
        };
        check_asset_bytes(asset, &bytes)?;
        return Ok(serde_json::json!({
            "verified": true,
            "version": manifest.version,
            "file": path.display().to_string(),
            "asset": asset,
        }));
    }
    let asset = select_asset(&manifest, os, arch, &tier)?.clone();
    let url = format!("{}/v{version}/{}", release_base(), asset.name);
    let bytes = fetch(&url)?;
    check_asset_bytes(&asset, &bytes)?;
    let target = if to.trim().is_empty() {
        std::path::PathBuf::from(if os == "windows" { "udb.exe" } else { "udb" })
    } else {
        std::path::PathBuf::from(to.trim())
    };
    // Write beside the target and rename, so a failed write never leaves a
    // half-written binary where the old one was.
    let staging = target.with_extension("udb-download");
    fs::write(&staging, &bytes)
        .map_err(|err| format!("write {} failed: {err}", staging.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&staging, fs::Permissions::from_mode(0o755))
            .map_err(|err| format!("chmod {} failed: {err}", staging.display()))?;
    }
    fs::rename(&staging, &target).map_err(|err| {
        format!(
            "move {} to {} failed: {err} (on Windows a running udb.exe cannot be replaced; pass --to <new path>)",
            staging.display(),
            target.display()
        )
    })?;
    Ok(serde_json::json!({
        "installed": target.display().to_string(),
        "version": manifest.version,
        "asset": asset,
    }))
}

#[cfg(test)]
mod self_cli_tests {
    use super::{ManifestAsset, ReleaseManifest, check_asset_bytes, select_asset, sha256_hex};

    fn manifest() -> ReleaseManifest {
        let body = b"udb-binary";
        ReleaseManifest {
            version: "0.5.29".into(),
            assets: vec![
                ManifestAsset {
                    name: "udb-linux-amd64".into(),
                    os: "linux".into(),
                    arch: "amd64".into(),
                    tier: "portable".into(),
                    sha256: sha256_hex(body),
                    size: body.len() as u64,
                },
                ManifestAsset {
                    name: "udb-linux-amd64-full".into(),
                    os: "linux".into(),
                    arch: "amd64".into(),
                    tier: "full".into(),
                    sha256: sha256_hex(b"full"),
                    size: 4,
                },
            ],
        }
    }

    #[test]
    fn selects_by_platform_and_tier() {
        let m = manifest();
        assert_eq!(
            select_asset(&m, "linux", "amd64", "").unwrap().name,
            "udb-linux-amd64"
        );
        assert_eq!(
            select_asset(&m, "linux", "amd64", "full").unwrap().name,
            "udb-linux-amd64-full"
        );
        let err = select_asset(&m, "darwin", "arm64", "").unwrap_err();
        assert!(err.contains("linux/amd64/full"), "{err}");
    }

    /// A tampered or truncated binary is refused by size and by hash.
    #[test]
    fn checks_size_and_hash() {
        let m = manifest();
        let asset = &m.assets[0];
        assert!(check_asset_bytes(asset, b"udb-binary").is_ok());
        assert!(
            check_asset_bytes(asset, b"udb-binarX")
                .unwrap_err()
                .contains("sha256")
        );
        assert!(
            check_asset_bytes(asset, b"udb")
                .unwrap_err()
                .contains("size")
        );
    }
}
