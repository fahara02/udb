//! Signing-key source abstraction (master-plan 3.4).
//!
//! The signing-key registry seed path resolves its RS256 *private* material
//! through a [`KeyProvider`] instead of reading the env/config key inline, so the
//! source can be the env/config key today and a KMS/HSM tomorrow without touching
//! the seal/seed logic in `signing_keys.rs`.
//!
//! Selection is by `UDB_SIGNING_KEY_PROVIDER`, resolved exactly once through a
//! process [`OnceLock`] — the SINGLE env-read site for signing-key sourcing:
//!   - unset / `env`   → [`EnvKeyProvider`] (today's behavior, byte-for-byte).
//!   - `aws-kms`/`kms` → [`UnavailableKeyProvider`] with a clear typed error
//!     (never a silent fallback to the env key); the concrete AWS KMS provider is
//!     a documented extension point (see the KMS note below) that lands with its
//!     `aws-sdk-kms` dependency on a networked build.
//!
//! `auth_service/` is allowlisted by the runtime env-confinement test
//! (`connection_manager.rs`), so the `OnceLock` env read below is in-bounds; it is
//! deliberately the only place signing-key sourcing consults the environment,
//! which keeps env-key reads pinned to this module (the seed path no longer reads
//! the env key directly).

use crate::runtime::security::SecurityConfig;
use std::sync::OnceLock;

/// Source of the RS256 signing *private* material the registry seed consumes.
///
/// `signing_key_pem` returns the private PEM (or a typed error). The default env
/// provider is infallible; fallible backends (a KMS/HSM fetch) surface `Err`. (A
/// future KMS provider may add a `key_id()` method here to advertise the sourced
/// key id; the env path stamps the static `UDB_RS256_KID` in the seed path.)
pub(super) trait KeyProvider: Send + Sync {
    /// Resolve the signing private key PEM, or a typed error string.
    fn signing_key_pem(&self) -> Result<String, String>;
}

/// Default provider: the env/config key path that exists today. Returns
/// [`SecurityConfig::jwt_private_pem`] collapsed with `unwrap_or_default()` —
/// byte-identical to the value the seed path read before this abstraction
/// (`Some(pem) → pem`, `None`/empty → `""`). Never errors (env path infallible).
pub(super) struct EnvKeyProvider<'a> {
    security: &'a SecurityConfig,
}

impl<'a> EnvKeyProvider<'a> {
    pub(super) fn new(security: &'a SecurityConfig) -> Self {
        Self { security }
    }
}

impl KeyProvider for EnvKeyProvider<'_> {
    fn signing_key_pem(&self) -> Result<String, String> {
        // Behavior-preserving: identical to the prior inline
        // `self.security.jwt_private_pem().unwrap_or_default()` read — resolves
        // inline-or-path, `""` when absent/empty/unresolvable. Always `Ok`.
        Ok(self.security.jwt_private_pem().unwrap_or_default())
    }
}

/// A provider that cannot supply a key: surfaces a clear, typed error each time
/// it is consulted. Selected when `UDB_SIGNING_KEY_PROVIDER` names a backend that
/// is unavailable in this build/config (`aws-kms` without `--features kms`, a KMS
/// provider missing its key id, or an unknown value) — so a misconfiguration
/// fails loud at the seed site instead of silently falling back to the env key.
pub(super) struct UnavailableKeyProvider {
    reason: String,
}

impl UnavailableKeyProvider {
    pub(super) fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl KeyProvider for UnavailableKeyProvider {
    fn signing_key_pem(&self) -> Result<String, String> {
        Err(self.reason.clone())
    }
}

// ── AWS KMS / CloudHSM extension point (master-plan 3.4 follow-up) ───────────
//
// Selecting `UDB_SIGNING_KEY_PROVIDER=aws-kms` resolves to `UnavailableKeyProvider`
// (a clear typed error, never a silent fallback to the env key). To activate a
// real KMS-backed signer on a networked build, add `aws-sdk-kms` as an optional
// dependency behind a `kms` feature and implement `AwsKmsProvider: KeyProvider`
// here. KMS/HSM keys are *non-exportable*, so `signing_key_pem` must return a typed
// "non-exportable" error and the real integration is a `GetPublicKey` round-trip
// (publish the public key to JWKS) plus the KMS `Sign` API (remote-sign), NOT
// seeding a private PEM. The trait, the once-resolved selector, and the `aws-kms`
// selection arm below are already wired — only the concrete provider and its
// dependency need to land. (The dependency is omitted here because it cannot be
// vendored offline; do not re-add it without committing an updated `Cargo.lock`.)

/// Provider-selection settings, read from the environment exactly once.
struct ProviderSettings {
    kind: String,
}

/// The SINGLE signing-key env-read site: resolves `UDB_SIGNING_KEY_PROVIDER` once
/// into a process `OnceLock`, so the provider choice is stable for the lifetime of
/// the process and the environment is consulted in exactly one place.
fn provider_settings() -> &'static ProviderSettings {
    static SETTINGS: OnceLock<ProviderSettings> = OnceLock::new();
    SETTINGS.get_or_init(|| ProviderSettings {
        kind: std::env::var("UDB_SIGNING_KEY_PROVIDER")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "env".to_string()),
    })
}

/// Select the active [`KeyProvider`] from the once-resolved settings.
///
/// Unset or `env` yields the behavior-preserving [`EnvKeyProvider`] (the default
/// path is byte-for-byte unchanged); `aws-kms`/`kms` yields a clear typed error
/// via [`UnavailableKeyProvider`] (the concrete AWS KMS provider is the deferred
/// extension point above — never a silent fallback to the env key); any other
/// value is a clear error. The returned box borrows `security` for the env path.
pub(super) fn active_key_provider(security: &SecurityConfig) -> Box<dyn KeyProvider + '_> {
    let settings = provider_settings();
    match settings.kind.as_str() {
        "env" => Box::new(EnvKeyProvider::new(security)),
        "aws-kms" | "kms" => Box::new(UnavailableKeyProvider::new(
            "UDB_SIGNING_KEY_PROVIDER=aws-kms is not built into this binary: the AWS \
             KMS signing provider (aws-sdk-kms dependency + AwsKmsProvider impl) is a \
             deferred extension point — see the KMS note in key_provider.rs. The \
             signing-key seed never silently falls back to the env key.",
        )),
        other => Box::new(UnavailableKeyProvider::new(format!(
            "unknown UDB_SIGNING_KEY_PROVIDER '{other}' (expected 'env' or 'aws-kms')"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn security_with_private(key: Option<String>) -> SecurityConfig {
        SecurityConfig {
            jwt_private_key: key,
            ..Default::default()
        }
    }

    /// A configured inline PEM is returned verbatim — identical to the value the
    /// seed path read directly via `jwt_private_pem().unwrap_or_default()` before
    /// the `KeyProvider` abstraction. (The PEM marker makes `resolve_pem` treat it
    /// as inline, not a filesystem path, matching `UDB_JWT_PRIVATE_KEY`.)
    #[test]
    fn env_provider_returns_configured_key() {
        let pem = "-----BEGIN PRIVATE KEY-----\nMIICONTENT\n-----END PRIVATE KEY-----";
        let security = security_with_private(Some(pem.to_string()));
        let provider = EnvKeyProvider::new(&security);

        let provided = provider
            .signing_key_pem()
            .expect("env provider is infallible");
        assert_eq!(provided, pem);
        // Byte-identical to the prior `jwt_private_pem().unwrap_or_default()`.
        assert_eq!(provided, security.jwt_private_pem().unwrap_or_default());
    }

    /// No key configured → `""`, byte-identical to the prior
    /// `jwt_private_pem().unwrap_or_default()`.
    #[test]
    fn env_provider_absent_key_matches_prior_default() {
        let security = security_with_private(None);
        let provider = EnvKeyProvider::new(&security);

        let provided = provider.signing_key_pem().expect("infallible");
        assert_eq!(provided, String::new());
        assert_eq!(provided, security.jwt_private_pem().unwrap_or_default());
    }

    /// An empty/whitespace value resolves to `""` (via `resolve_pem`), same as the
    /// absent case and same as before the abstraction.
    #[test]
    fn env_provider_empty_key_matches_prior_default() {
        let security = security_with_private(Some("   ".to_string()));
        let provider = EnvKeyProvider::new(&security);

        let provided = provider.signing_key_pem().expect("infallible");
        assert_eq!(provided, String::new());
        assert_eq!(provided, security.jwt_private_pem().unwrap_or_default());
    }

    /// An unavailable provider surfaces its reason as a typed error (the seed path
    /// logs it and skips rather than writing an empty key).
    #[test]
    fn unavailable_provider_surfaces_reason() {
        let provider = UnavailableKeyProvider::new("boom");
        assert_eq!(provider.signing_key_pem(), Err("boom".to_string()));
    }

    /// The provider selection is resolved exactly once (`OnceLock`): repeated calls
    /// return the same cached settings, so the provider choice is stable for the
    /// process lifetime regardless of any later environment mutation.
    #[test]
    fn selector_is_oncelock_stable() {
        let first = provider_settings();
        let second = provider_settings();
        assert!(std::ptr::eq(first, second));
    }

    /// On the default/unset env path the selector yields the behavior-preserving
    /// env provider whose PEM equals the env/config value. This is the only
    /// assertion that depends on the ambient environment, so it is gated on the
    /// default kind (skips if a non-default `UDB_SIGNING_KEY_PROVIDER` is set).
    #[test]
    fn active_provider_defaults_to_env() {
        if provider_settings().kind != "env" {
            return;
        }
        let pem = "-----BEGIN PRIVATE KEY-----\nMIICONTENT\n-----END PRIVATE KEY-----";
        let security = security_with_private(Some(pem.to_string()));
        let provider = active_key_provider(&security);
        assert_eq!(provider.signing_key_pem().expect("env infallible"), pem);
    }
}
