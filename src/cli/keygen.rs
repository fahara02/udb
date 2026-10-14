//! Secret and key generation shared by `udb env`, `udb env fill` and
//! `udb dev up --embedded`, so every command that writes credentials produces
//! values the broker accepts and stores them the same way.

use std::fs;
use std::path::{Path, PathBuf};

/// Create `path` with `contents`, readable by the owner only. Refuses to replace
/// an existing file: generated credentials are never overwritten silently.
pub(crate) fn private_write(path: &Path, contents: &[u8]) -> Result<(), String> {
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

/// A 48-character alphanumeric secret from the OS CSPRNG: safe to paste into a
/// `.env` without quoting. For HMAC-style secrets (session/password/API-key
/// hashing, approval signing), not for `UDB_ENCRYPTION_KEY`.
pub(crate) fn generated_secret() -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut raw = [0u8; 48];
    getrandom::getrandom(&mut raw).expect("OS CSPRNG unavailable");
    raw.iter()
        .map(|byte| ALPHABET[*byte as usize % ALPHABET.len()] as char)
        .collect()
}

/// A `UDB_ENCRYPTION_KEY`: 32 random bytes, base64-encoded. The broker accepts a
/// key only if it decodes to exactly 32 bytes.
pub(crate) fn generated_encryption_key() -> String {
    use base64::Engine as _;
    let mut raw = [0u8; 32];
    getrandom::getrandom(&mut raw).expect("OS CSPRNG unavailable");
    base64::engine::general_purpose::STANDARD.encode(raw)
}

/// The RS256 keypair the broker signs and verifies its own bearer tokens with,
/// kept as `jwt-private.pem` / `jwt-public.pem` in `dir`. Reuses an existing
/// private key (checking that a stored public key still matches it) and creates
/// one otherwise. Returns the two file paths, which is what `UDB_JWT_PRIVATE_KEY`
/// and `UDB_JWT_PUBLIC_KEY` accept.
pub(crate) fn ensure_rs256_keypair(dir: &Path) -> Result<(PathBuf, PathBuf), String> {
    use rsa::pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey, LineEnding};
    fs::create_dir_all(dir).map_err(|err| format!("create {}: {err}", dir.display()))?;
    #[cfg(unix)]
    {
        // The directory holds the signing key: owner-only, like the key files.
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
            .map_err(|err| format!("restrict {}: {err}", dir.display()))?;
    }
    let private_path = dir.join("jwt-private.pem");
    let public_path = dir.join("jwt-public.pem");
    let key = if private_path.exists() {
        let pem = fs::read_to_string(&private_path)
            .map_err(|err| format!("read {}: {err}", private_path.display()))?;
        rsa::RsaPrivateKey::from_pkcs8_pem(&pem)
            .map_err(|err| format!("decode {}: {err}", private_path.display()))?
    } else {
        let key = rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048)
            .map_err(|err| format!("generate JWT signing key: {err}"))?;
        let pem = key
            .to_pkcs8_pem(LineEnding::LF)
            .map_err(|err| format!("encode JWT signing key: {err}"))?;
        private_write(&private_path, pem.as_bytes())?;
        key
    };
    let public = rsa::RsaPublicKey::from(&key)
        .to_public_key_pem(LineEnding::LF)
        .map_err(|err| format!("encode JWT verification key: {err}"))?;
    if public_path.exists() {
        let stored = fs::read_to_string(&public_path)
            .map_err(|err| format!("read {}: {err}", public_path.display()))?;
        if stored != public {
            return Err(format!(
                "{} does not match {}",
                public_path.display(),
                private_path.display()
            ));
        }
    } else {
        private_write(&public_path, public.as_bytes())?;
    }
    Ok((private_path, public_path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_values_are_fresh_and_the_broker_accepts_the_encryption_key() {
        assert_ne!(generated_secret(), generated_secret());
        assert_eq!(generated_secret().len(), 48);
        let key = generated_encryption_key();
        udb::runtime::validate_encryption_key(&key)
            .unwrap_or_else(|err| panic!("generated key {key} refused: {err}"));
        assert_ne!(key, generated_encryption_key());
    }

    #[test]
    fn the_keypair_is_created_once_and_reused() {
        let dir = std::env::temp_dir().join(format!("udb-keygen-{}", uuid::Uuid::new_v4()));
        let (private, public) = ensure_rs256_keypair(&dir).expect("create");
        let first = fs::read_to_string(&private).unwrap();
        let (again, _) = ensure_rs256_keypair(&dir).expect("reuse");
        assert_eq!(
            first,
            fs::read_to_string(&again).unwrap(),
            "an existing key is kept"
        );
        assert!(fs::read_to_string(&public).unwrap().contains("PUBLIC KEY"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "the key directory is owner-only");
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
