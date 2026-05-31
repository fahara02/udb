use aead::{Aead, KeyInit};
use aes_gcm_siv::{Aes256GcmSiv, Nonce};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::runtime::config::EncryptionSettings;

#[derive(Debug, Clone)]
pub(super) struct EncryptionRuntime {
    keys: Vec<EncryptionKey>,
    active_version: u8,
}

#[derive(Debug, Clone)]
struct EncryptionKey {
    version: u8,
    key: [u8; 32],
}

impl EncryptionRuntime {
    pub(super) async fn from_settings(
        settings: &EncryptionSettings,
    ) -> Result<Option<Self>, String> {
        if let Some(runtime) = Self::from_key_materials(settings)? {
            return Ok(Some(runtime));
        }
        Self::from_vault_export(settings).await
    }

    fn from_key_materials(settings: &EncryptionSettings) -> Result<Option<Self>, String> {
        let mut keys = settings
            .keys
            .iter()
            .map(|(version, value)| {
                Ok(EncryptionKey {
                    version: *version,
                    key: decode_encryption_key(value)?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        if keys.is_empty() {
            return Ok(None);
        }
        keys.sort_by_key(|key| key.version);
        let active_version = settings
            .active_version
            .unwrap_or_else(|| keys.last().map(|key| key.version).unwrap_or(1));
        if !keys.iter().any(|key| key.version == active_version) {
            return Err(format!(
                "active encryption version {active_version} has no matching key"
            ));
        }
        Ok(Some(Self {
            keys,
            active_version,
        }))
    }

    async fn from_vault_export(settings: &EncryptionSettings) -> Result<Option<Self>, String> {
        let (Some(addr), Some(token)) = (&settings.vault_addr, &settings.vault_token) else {
            return Ok(None);
        };

        // U34: Vault key export requires the `http-client` feature. Without
        // it the slim build fails closed — refusing to silently fall back to
        // static keys because that would change which keys protect data.
        #[cfg(not(feature = "http-client"))]
        {
            let _ = (addr, token);
            return Err(
                "Vault key export requires the `http-client` Cargo feature; \
                 rebuild with `--features http-client` or supply static keys \
                 via UDB_ENCRYPTION_KEYS / UDB_ENCRYPTION_ACTIVE_VERSION."
                    .to_string(),
            );
        }

        #[cfg(feature = "http-client")]
        // GAP 17a: Reject plain-HTTP Vault addresses in non-dev mode.
        // The X-Vault-Token would be sent in cleartext, enabling token theft.
        if addr.starts_with("http://") && !settings.dev_mode {
            return Err(
                "UDB_VAULT_ADDR uses plain HTTP — X-Vault-Token would be sent unencrypted. \
                 Set https:// or set UDB_DEV_MODE=true to override."
                    .to_string(),
            );
        }

        #[cfg(feature = "http-client")]
        let url = format!(
            "{}/v1/{}/export/encryption-key/{}",
            addr.trim_end_matches('/'),
            settings.vault_transit_mount.trim_matches('/'),
            settings.vault_transit_key_name.trim_matches('/')
        );

        // GAP 17b: Add HTTP timeout so UDB does not hang at startup if Vault is unreachable.
        #[cfg(feature = "http-client")]
        let vault_timeout = std::time::Duration::from_secs(settings.vault_timeout_secs.max(1));
        #[cfg(feature = "http-client")]
        {
            let client = reqwest::Client::builder()
                .timeout(vault_timeout)
                .build()
                .map_err(|e| format!("Vault HTTP client build failed: {e}"))?;

            let payload: JsonValue = client
                .get(url)
                .header("X-Vault-Token", token)
                .send()
                .await
                .map_err(|err| format!("Vault Transit key export request failed: {err}"))?
                .error_for_status()
                .map_err(|err| format!("Vault Transit key export failed: {err}"))?
                .json()
                .await
                .map_err(|err| format!("Vault Transit key export JSON decode failed: {err}"))?;

            let key_map = payload
                .pointer("/data/keys")
                .and_then(JsonValue::as_object)
                .ok_or_else(|| "Vault Transit export response missing data.keys".to_string())?;
            let mut keys = Vec::new();
            for (version, value) in key_map {
                let version = version
                    .parse::<u8>()
                    .map_err(|err| format!("invalid Vault key version {version}: {err}"))?;
                let encoded = value
                    .as_str()
                    .ok_or_else(|| format!("Vault key version {version} is not a string"))?;
                keys.push(EncryptionKey {
                    version,
                    key: decode_encryption_key(encoded)?,
                });
            }
            if keys.is_empty() {
                return Ok(None);
            }
            keys.sort_by_key(|key| key.version);
            // GAP 17c: Respect UDB_ENCRYPTION_ACTIVE_VERSION. Using keys.last() alone
            // is wrong during rotation (e.g. v3 is active while v4 is being introduced).
            let active_version = settings
                .active_version
                .unwrap_or_else(|| keys.last().map(|k| k.version).unwrap_or(1));
            if !keys.iter().any(|k| k.version == active_version) {
                return Err(format!(
                    "UDB_ENCRYPTION_ACTIVE_VERSION={active_version} has no matching key in Vault export"
                ));
            }
            Ok(Some(Self {
                keys,
                active_version,
            }))
        }
    }

    pub(super) fn encrypt_json_value(&self, value: &JsonValue) -> Result<String, String> {
        let key = self
            .key(self.active_version)
            .ok_or_else(|| "active encryption key is missing".to_string())?;
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes.copy_from_slice(&Uuid::new_v4().as_bytes()[..12]);
        let cipher = Aes256GcmSiv::new_from_slice(&key.key)
            .map_err(|err| format!("invalid AEAD key: {err}"))?;
        let plaintext = serde_json::to_vec(value)
            .map_err(|err| format!("JSON plaintext serialization failed: {err}"))?;
        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&nonce_bytes), plaintext.as_ref())
            .map_err(|err| format!("AEAD encryption failed: {err}"))?;
        let mut envelope = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
        envelope.extend_from_slice(&nonce_bytes);
        envelope.extend_from_slice(&ciphertext);
        Ok(format!(
            "udb-aead:v{}:{}",
            key.version,
            BASE64_STANDARD.encode(envelope)
        ))
    }

    pub(super) fn decrypt_json_value(&self, value: &str) -> Result<JsonValue, String> {
        let Some((version, encoded)) = parse_ciphertext(value) else {
            return Ok(JsonValue::String(value.to_string()));
        };
        let envelope = BASE64_STANDARD
            .decode(encoded)
            .map_err(|err| format!("ciphertext base64 decode failed: {err}"))?;
        if envelope.len() <= 12 {
            return Err("ciphertext envelope is too short".to_string());
        }
        let (nonce_bytes, ciphertext) = envelope.split_at(12);
        let mut last_error = None;
        for key in self.keys_for_decrypt(version) {
            let cipher = Aes256GcmSiv::new_from_slice(&key.key)
                .map_err(|err| format!("invalid AEAD key: {err}"))?;
            match cipher.decrypt(Nonce::from_slice(nonce_bytes), ciphertext) {
                Ok(plaintext) => {
                    return serde_json::from_slice(&plaintext)
                        .map_err(|err| format!("JSON plaintext decode failed: {err}"));
                }
                Err(err) => last_error = Some(format!("AEAD decryption failed: {err}")),
            }
        }
        Err(last_error.unwrap_or_else(|| format!("no key for ciphertext version {version}")))
    }

    fn key(&self, version: u8) -> Option<&EncryptionKey> {
        self.keys.iter().find(|key| key.version == version)
    }

    fn keys_for_decrypt(&self, version: u8) -> Vec<&EncryptionKey> {
        let mut keys = self
            .keys
            .iter()
            .filter(|key| key.version == version)
            .collect::<Vec<_>>();
        keys.extend(self.keys.iter().filter(|key| key.version != version));
        keys
    }
}

fn parse_ciphertext(value: &str) -> Option<(u8, &str)> {
    let rest = value.strip_prefix("udb-aead:v")?;
    let (version, encoded) = rest.split_once(':')?;
    Some((version.parse().ok()?, encoded))
}

fn decode_encryption_key(value: &str) -> Result<[u8; 32], String> {
    let trimmed = value.trim();
    let decoded = BASE64_STANDARD
        .decode(trimmed)
        .or_else(|_| decode_hex(trimmed))
        .unwrap_or_else(|_| trimmed.as_bytes().to_vec());
    decoded
        .try_into()
        .map_err(|bytes: Vec<u8>| format!("encryption key must be 32 bytes, got {}", bytes.len()))
}

fn decode_hex(value: &str) -> Result<Vec<u8>, base64::DecodeError> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if !value.len().is_multiple_of(2) {
        return Err(base64::DecodeError::InvalidLength(value.len()));
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for chunk in value.as_bytes().chunks(2) {
        let pair = std::str::from_utf8(chunk)
            .map_err(|_| base64::DecodeError::InvalidByte(0, chunk[0]))?;
        let byte = u8::from_str_radix(pair, 16)
            .map_err(|_| base64::DecodeError::InvalidByte(0, chunk[0]))?;
        bytes.push(byte);
    }
    Ok(bytes)
}

// `env_first` is imported from `runtime::executor_utils` (single-sourced).

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn encryption_runtime_round_trips_json_with_active_key_version() {
        let runtime = EncryptionRuntime {
            active_version: 2,
            keys: vec![
                EncryptionKey {
                    version: 1,
                    key: [1; 32],
                },
                EncryptionKey {
                    version: 2,
                    key: [2; 32],
                },
            ],
        };

        let value = json!({"nid": "1234567890", "score": 7});
        let ciphertext = runtime.encrypt_json_value(&value).unwrap();

        assert!(ciphertext.starts_with("udb-aead:v2:"));
        assert_eq!(runtime.decrypt_json_value(&ciphertext).unwrap(), value);
    }

    #[test]
    fn encryption_runtime_decrypts_old_key_versions() {
        let old_runtime = EncryptionRuntime {
            active_version: 1,
            keys: vec![EncryptionKey {
                version: 1,
                key: [1; 32],
            }],
        };
        let rotated_runtime = EncryptionRuntime {
            active_version: 2,
            keys: vec![
                EncryptionKey {
                    version: 1,
                    key: [1; 32],
                },
                EncryptionKey {
                    version: 2,
                    key: [2; 32],
                },
            ],
        };

        let value = JsonValue::String("legacy secret".to_string());
        let ciphertext = old_runtime.encrypt_json_value(&value).unwrap();

        assert_eq!(
            rotated_runtime.decrypt_json_value(&ciphertext).unwrap(),
            value
        );
    }
}
