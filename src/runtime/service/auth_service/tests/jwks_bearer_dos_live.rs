//! JWKS bearer-verify DoS guard (HIGH — the deep-audit find in
//! `crate::runtime::security`).
//!
//! Customer symptom: a broker configured with `UDB_JWT_JWKS_URL` verifies bearer
//! tokens by fetching the IdP JWKS. A flood of tokens carrying DISTINCT unknown
//! `kid`s forces the unknown-kid refresh path, and a slow/hung IdP endpoint (or a
//! deliberately garbage-`kid` replay) could park the tokio worker on a blocking
//! `reqwest::blocking` fetch that had NO timeout — a trivial denial of service.
//!
//! The fix bounds every JWKS fetch with an explicit `reqwest` timeout
//! (`JWKS_FETCH_TIMEOUT_SECS`). This test drives the SERVED verification entry
//! point (`security::validate_bearer_token` in JWKS mode) against a controllable
//! endpoint that ACCEPTS the TCP connection but never responds, so the TLS
//! handshake hangs exactly like a hung IdP, and asserts every verify RETURNS
//! bounded by the fetch timeout rather than blocking indefinitely.
//!
//! Revert-proof: reverting the timeout back to `reqwest::blocking::get(url)`
//! (which has no timeout) makes the stalled handshake block far beyond the test's
//! 25s guard — the outer `tokio::time::timeout` trips and the `.expect(...)`
//! below fails. The `elapsed < 15s` bound fails too.
//!
//! ── FLAGGED, not covered here: the fetch-rate-limit (cooldown) half of the fix
//! (`JWKS_MIN_REFRESH_INTERVAL_SECS` — serve the cached set for forced refreshes
//! within the cooldown so an unknown-`kid` flood is ≤1 fetch/interval instead of
//! one-per-request) cannot be asserted from a pure test file. Proving it requires
//! a SUCCESSFUL priming JWKS fetch to populate the process JWKS cache, but
//! `security::fetch_jwks` builds a `reqwest` client bound to the compiled-in
//! webpki root store (`rustls-tls`), which will not trust a locally-generated
//! test-server certificate and ignores `SSL_CERT_FILE`; a failed fetch never
//! primes the cache, so the cooldown branch is never reached. The private
//! `cached_jwks`/`JWT_JWKS_CACHE` have no `#[cfg(test)]` cache-prime or
//! fetch-counter seam. Asserting the cooldown would need a one-line test seam in
//! `security.rs` (e.g. `#[cfg(test)] fn prime_jwks_cache(url, jwks)` plus a fetch
//! counter) or switching `reqwest` to native roots — both production edits, which
//! are out of scope for this test-only change. See the report for the exact seam.

use crate::runtime::security::{self, SecurityConfig, validate_bearer_token};
use std::time::{Duration, Instant};

/// A syntactically valid RS256 JWT whose header carries `kid`. The signature is
/// real (signed with the RS256 test key) but irrelevant: `validate_bearer_token`
/// decodes the header and, in JWKS mode, attempts the JWKS fetch BEFORE any
/// signature verification, so an unknown `kid` is enough to reach the fetch path.
fn unknown_kid_token(kid: &str) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    let key = EncodingKey::from_rsa_pem(include_bytes!("../../../testdata/jwt_rs256_private.pem"))
        .expect("load RS256 test signing key");
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(kid.to_string());
    let claims = serde_json::json!({ "sub": "dos-probe", "exp": 9_999_999_999u64 });
    encode(&header, &claims, &key).expect("encode unknown-kid probe token")
}

/// Run one served bearer verification on a blocking worker, guarded by an outer
/// async timeout so a REVERT (no fetch timeout) surfaces as a failed `.expect`
/// instead of hanging the whole test. Returns `(result, wall-clock elapsed)`.
async fn verify_bounded(
    config: &SecurityConfig,
    token: String,
) -> (Result<security::SecurityClaims, String>, Duration) {
    let cfg = config.clone();
    let start = Instant::now();
    let joined = tokio::time::timeout(
        Duration::from_secs(25),
        tokio::task::spawn_blocking(move || validate_bearer_token(&cfg, &token)),
    )
    .await;
    let elapsed = start.elapsed();
    let result = joined
        .expect(
            "validate_bearer_token must RETURN bounded by the JWKS fetch timeout — a revert of the \
             reqwest timeout makes the stalled JWKS handshake block indefinitely",
        )
        .expect("spawn_blocking join");
    (result, elapsed)
}

#[cfg(feature = "http-client")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "opens sockets and waits on the ~5s JWKS fetch timeout; run with: cargo test --lib live_jwks_bearer_verify_slow_endpoint_is_timeout_bounded -- --ignored --nocapture"]
async fn live_jwks_bearer_verify_slow_endpoint_is_timeout_bounded() {
    // Ensure no offline JWKS override from another test short-circuits the real
    // fetch path we are exercising.
    security::set_test_jwks(None);

    // A "hung IdP": accept the TCP connection, then hold it open forever without
    // responding, so reqwest's TLS handshake stalls (a plain reset would instead
    // produce a fast connection error, which would not exercise the timeout).
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stalling JWKS listener");
    let port = listener
        .local_addr()
        .expect("stalling listener addr")
        .port();
    let server = tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            match listener.accept().await {
                Ok((stream, _)) => held.push(stream), // hold open → stall the handshake
                Err(_) => break,
            }
        }
    });

    let config = SecurityConfig {
        jwt_jwks_url: Some(format!("https://127.0.0.1:{port}/.well-known/jwks.json")),
        ..SecurityConfig::default()
    };

    // A single unknown-kid verify is bounded by the fetch timeout.
    let (result, elapsed) = verify_bounded(&config, unknown_kid_token("dos-unknown-kid-0")).await;
    assert!(
        result.is_err(),
        "a stalled JWKS endpoint can never yield a validated token"
    );
    assert!(
        elapsed < Duration::from_secs(15),
        "JWKS fetch must be timeout-bounded (a hung IdP must not park the worker): took {elapsed:?}"
    );
    assert!(
        elapsed >= Duration::from_secs(4),
        "the ~5s fetch timeout should have elapsed — a sub-second return means the socket errored \
         instantly rather than exercising the timeout: {elapsed:?}"
    );

    // A flood of DISTINCT unknown-kid tokens: each verify stays INDIVIDUALLY
    // bounded, so a hung IdP under a garbage-kid replay flood never parks the
    // worker pool indefinitely.
    for i in 0..3 {
        let (result, elapsed) =
            verify_bounded(&config, unknown_kid_token(&format!("dos-flood-{i}"))).await;
        assert!(result.is_err(), "flood verify #{i} must fail closed");
        assert!(
            elapsed < Duration::from_secs(15),
            "flood verify #{i} must stay timeout-bounded: {elapsed:?}"
        );
    }

    server.abort();
}
