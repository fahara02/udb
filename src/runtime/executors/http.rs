//! Shared reqwest client construction for the HTTP-driven backends
//! (clickhouse, mongodb, neo4j, qdrant).
//!
//! Phase D add-on (§8.2): each of these executors previously hand-rolled
//! `reqwest::Client::builder()` with its own TLS/timeout/cloud-detection logic.
//! They now compose the helpers here so the policy (which knobs apply, how
//! "cloud" is detected, the `build().unwrap_or_default()` fallback) lives in one
//! place. `reqwest` is a non-optional dependency and these backends are always
//! compiled, so this module needs no feature gate.

use std::time::Duration;

/// Read a `*_SECS` timeout environment variable, falling back to `default_secs`
/// when the variable is unset or unparseable.
pub(crate) fn env_timeout(var: &str, default_secs: u64) -> Duration {
    Duration::from_secs(
        std::env::var(var)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(default_secs),
    )
}

/// Detect a "cloud" deployment for an HTTP backend.
///
/// True when either the explicit deploy-mode env var equals `cloud`
/// (case-insensitive), or the base URL contains `cloud_host_marker`
/// (e.g. `.clickhouse.cloud`). Pass an empty `cloud_host_marker` to skip the
/// URL-host heuristic.
pub(crate) fn is_cloud(deploy_mode_var: &str, base_url: &str, cloud_host_marker: &str) -> bool {
    let explicit = std::env::var(deploy_mode_var)
        .map(|value| value.eq_ignore_ascii_case("cloud"))
        .unwrap_or(false);
    let host_match = !cloud_host_marker.is_empty() && base_url.contains(cloud_host_marker);
    explicit || host_match
}

/// Declarative description of an HTTP backend client. Build it with
/// [`HttpClientSpec::build`] to get a `reqwest::Client`.
///
/// Both timeout knobs are optional because the backends differ: ClickHouse sets
/// a `connect_timeout` (per-request timeouts are applied at call sites), while
/// MongoDB/Neo4j/Qdrant set a whole-request `timeout`.
#[derive(Debug, Clone, Default)]
pub(crate) struct HttpClientSpec {
    /// Whole-request timeout (`reqwest` `timeout`).
    pub timeout: Option<Duration>,
    /// Connection-establishment timeout (`reqwest` `connect_timeout`).
    pub connect_timeout: Option<Duration>,
    /// Reject plaintext `http://` URLs (used for cloud deployments so credentials
    /// are never sent unencrypted).
    pub https_only: bool,
}

impl HttpClientSpec {
    /// A whole-request timeout client (MongoDB/Neo4j/Qdrant style).
    pub(crate) fn with_timeout(timeout: Duration) -> Self {
        Self {
            timeout: Some(timeout),
            ..Self::default()
        }
    }

    /// A connection-timeout client (ClickHouse style).
    pub(crate) fn with_connect_timeout(connect_timeout: Duration) -> Self {
        Self {
            connect_timeout: Some(connect_timeout),
            ..Self::default()
        }
    }

    /// Set `https_only` (typically gated on cloud detection) and return self.
    pub(crate) fn https_only(mut self, enabled: bool) -> Self {
        self.https_only = enabled;
        self
    }

    /// Build the `reqwest::Client`. Falls back to `Client::default()` if the
    /// builder fails (mirrors the previous per-executor `.unwrap_or_default()`).
    pub(crate) fn build(&self) -> reqwest::Client {
        let mut builder = reqwest::Client::builder();
        if let Some(timeout) = self.timeout {
            builder = builder.timeout(timeout);
        }
        if let Some(connect_timeout) = self.connect_timeout {
            builder = builder.connect_timeout(connect_timeout);
        }
        if self.https_only {
            builder = builder.https_only(true);
        }
        builder.build().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests use uniquely-named (UDB_TEST_*) env vars that are never set in
    // the environment, so they exercise the "unset" path without mutating env
    // state (env mutation is `unsafe` on edition 2024 and racy across threads).

    #[test]
    fn is_cloud_detects_host_marker_when_mode_unset() {
        let var = "UDB_TEST_HTTP_DEPLOY_MODE_HOST_UNSET";
        assert!(is_cloud(
            var,
            "https://abc.clickhouse.cloud",
            ".clickhouse.cloud"
        ));
        assert!(!is_cloud(
            var,
            "https://abc.example.com",
            ".clickhouse.cloud"
        ));
        // No host marker + unset mode → not cloud.
        assert!(!is_cloud(var, "http://localhost", ""));
    }

    #[test]
    fn env_timeout_falls_back_to_default() {
        let var = "UDB_TEST_HTTP_TIMEOUT_UNSET";
        assert_eq!(env_timeout(var, 30), Duration::from_secs(30));
    }

    #[test]
    fn spec_builds_clients() {
        // Just assert these don't panic and produce a usable client.
        let _ = HttpClientSpec::with_timeout(Duration::from_secs(5)).build();
        let _ = HttpClientSpec::with_connect_timeout(Duration::from_secs(5))
            .https_only(true)
            .build();
    }
}
