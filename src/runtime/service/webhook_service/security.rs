//! The security core (fail closed everywhere) for the native `WebhookService`,
//! extracted verbatim: the SSRF guard (`ip_is_blocked` / `parse_webhook_target` /
//! `validate_webhook_target_url` / `resolve_and_validate_target`), the vendored
//! sha2-based HMAC signing (`sign_webhook_body` / `generate_signing_secret`), and
//! the tenant-bound subscription matching (`topic_matches_pattern` /
//! `webhook_event_matches_endpoint_scope`).

use std::net::{IpAddr, SocketAddr};

use base64::Engine as _;
use tonic::Status;

use super::errors::{
    webhook_host_blocked_address_status, webhook_host_no_addresses_status,
    webhook_host_unresolved_status, webhook_url_violation,
};

// ── SSRF guard (the security core) ────────────────────────────────────────────

/// Reject an IP that a webhook must never be allowed to reach. Covers the full
/// private/loopback/link-local/CGNAT/unspecified set in BOTH families, so neither
/// a raw-IP literal at write time nor a rebound DNS answer at delivery time can
/// point the broker at an internal address (cloud metadata, service mesh, etc.).
///
/// IPv4: `0.0.0.0/8`, `10.0.0.0/8`, `127.0.0.0/8`, `169.254.0.0/16`,
/// `172.16.0.0/12`, `192.168.0.0/16`, `100.64.0.0/10` (CGNAT), plus multicast and
/// reserved (`>= 224.0.0.0`). IPv6: `::` (unspecified), `::1` (loopback),
/// `fc00::/7` (ULA), `fe80::/10` (link-local), and any IPv4-mapped address that is
/// itself blocked.
pub(crate) fn ip_is_blocked(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            v4.is_loopback()            // 127.0.0.0/8
                || v4.is_private()      // 10/8, 172.16/12, 192.168/16
                || v4.is_link_local()   // 169.254.0.0/16
                || v4.is_unspecified()  // 0.0.0.0
                || v4.is_broadcast()    // 255.255.255.255
                || o[0] == 0            // 0.0.0.0/8 "this network"
                || (o[0] == 100 && (o[1] & 0xc0) == 0x40) // 100.64.0.0/10 CGNAT
                || o[0] >= 224 // 224/4 multicast + 240/4 reserved
        }
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return ip_is_blocked(IpAddr::V4(mapped));
            }
            let seg0 = v6.segments()[0];
            v6.is_loopback()                 // ::1
                || v6.is_unspecified()       // ::
                || (seg0 & 0xfe00) == 0xfc00 // fc00::/7 unique-local
                || (seg0 & 0xffc0) == 0xfe80 // fe80::/10 link-local
        }
    }
}

/// Dependency-free parse of a webhook target into `(host, port)`. The host is
/// returned with IPv6 brackets stripped. Enforces the `https://` scheme (the only
/// allowed one) and extracts the authority's host the way a URL parser does — host
/// is whatever follows the LAST `@` (userinfo is dropped), up to the path/query/
/// fragment, with `\` treated as a path separator (WHATWG special-scheme rule) so
/// a `https://evil.com\@127.0.0.1` style trick cannot smuggle an internal host
/// past the check. No `url`/`reqwest` crate is used, so this stays compiled (and
/// unit-tested) on every feature set.
fn parse_webhook_target(url: &str) -> Result<(String, u16), Status> {
    let raw = url.trim();
    // `get(..8)` is None when shorter than 8 bytes OR when byte 8 is not a UTF-8
    // char boundary, so a malformed multi-byte input is rejected, never panics.
    let scheme_ok = raw
        .get(..8)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"));
    if !scheme_ok {
        return Err(webhook_url_violation(
            "must use https scheme",
            "webhook url must use https (cleartext http and non-http schemes are rejected)",
        ));
    }
    let rest = &raw[8..];
    let authority_end = rest.find(['/', '?', '#', '\\']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    // Host is everything after the LAST '@' (userinfo, if any, is discarded).
    let hostport = authority.rsplit('@').next().unwrap_or(authority);
    let (host, port) = if let Some(after_bracket) = hostport.strip_prefix('[') {
        // IPv6 literal: `[addr]` optionally followed by `:port`.
        let close = after_bracket.find(']').ok_or_else(|| {
            webhook_url_violation(
                "must contain a well-formed bracketed IPv6 host",
                "webhook url has a malformed IPv6 host",
            )
        })?;
        let addr = &after_bracket[..close];
        let port = after_bracket[close + 1..]
            .strip_prefix(':')
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(443);
        (addr.to_string(), port)
    } else if let Some(idx) = hostport.find(':') {
        let port = hostport[idx + 1..].parse::<u16>().unwrap_or(443);
        (hostport[..idx].to_string(), port)
    } else {
        (hostport.to_string(), 443)
    };
    if host.trim().is_empty() || host.chars().any(char::is_whitespace) {
        return Err(webhook_url_violation(
            "must include a valid external host",
            "webhook url must include a valid host",
        ));
    }
    Ok((host, port))
}

/// Pure SSRF validator enforced at endpoint WRITE time (Create/Update): the URL
/// must be `https://`, carry a host, and — when the host is a raw IP literal — not
/// fall in a blocked range. A hostname literal cannot be resolved here (that is
/// the delivery-time job in [`resolve_and_validate_target`]), but obvious local
/// names (`localhost`) are rejected up front. Maps every failure to
/// `invalid_argument` so a caller learns exactly why the endpoint was refused.
pub(crate) fn validate_webhook_target_url(url: &str) -> Result<(), Status> {
    let (host, _port) = parse_webhook_target(url)?;
    if let Ok(ip) = host.parse::<IpAddr>() {
        if ip_is_blocked(ip) {
            return Err(webhook_url_violation(
                "must not target private, loopback, link-local, CGNAT, unspecified, multicast, or reserved IP ranges",
                format!(
                    "webhook url host {host} resolves to a private/loopback/link-local address (SSRF blocked)"
                ),
            ));
        }
    } else {
        let lower = host.to_ascii_lowercase();
        if lower == "localhost" || lower.ends_with(".localhost") {
            return Err(webhook_url_violation(
                "must not target localhost hostnames",
                "webhook url host localhost is not an allowed external target (SSRF blocked)",
            ));
        }
    }
    Ok(())
}

/// A delivery target whose resolved addresses have ALL passed the SSRF range
/// check, carried together with the exact addresses to connect to. The caller
/// PINS these addresses for the actual TCP connection (never handing the hostname
/// back to the HTTP client to re-resolve), which closes the check→connect TOCTOU
/// a DNS-rebinding attacker would otherwise use to swing a validated public name
/// onto an internal address after the check but before the connect.
#[allow(dead_code)]
pub(crate) struct ValidatedTarget {
    pub host: String,
    pub port: u16,
    /// Every entry is confirmed non-blocked; the delivery client is pinned to
    /// these and only these addresses.
    pub addrs: Vec<SocketAddr>,
}

/// Delivery-time SSRF guard that also PINS the connect address (defeats DNS
/// rebinding at the connection layer, not just the validation layer): re-run the
/// write-time check, then — for a hostname target — resolve it, reject if ANY
/// resolved address is blocked, and return the validated addresses so the caller
/// connects to exactly those (no independent re-resolution). Fail closed: a
/// resolution error is a rejection, never a proceed. A literal-IP target (already
/// range-checked by [`validate_webhook_target_url`]) is pinned directly, so no
/// name resolution — hence no rebind window — exists for it.
#[allow(dead_code)]
pub(crate) async fn resolve_and_pin_target(url: &str) -> Result<ValidatedTarget, Status> {
    validate_webhook_target_url(url)?;
    let (host, port) = parse_webhook_target(url)?;
    if let Ok(ip) = host.parse::<IpAddr>() {
        // A literal IP was already range-checked by `validate_webhook_target_url`;
        // pin it directly so no resolver (and no rebind) is ever involved.
        return Ok(ValidatedTarget {
            host,
            port,
            addrs: vec![SocketAddr::new(ip, port)],
        });
    }
    let resolved = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|err| {
            // Fail closed: if we cannot prove where the host points, do not deliver.
            webhook_host_unresolved_status(&host, err)
        })?;
    let mut addrs = Vec::new();
    for addr in resolved {
        if ip_is_blocked(addr.ip()) {
            return Err(webhook_host_blocked_address_status(&host, addr.ip()));
        }
        addrs.push(addr);
    }
    if addrs.is_empty() {
        return Err(webhook_host_no_addresses_status(&host));
    }
    Ok(ValidatedTarget { host, port, addrs })
}

/// Back-compat delivery-time SSRF check that only reports pass/fail (used by the
/// notification service's shared guard, which does not need the pinned address).
/// Delegates to [`resolve_and_pin_target`] so the validation logic lives once.
#[allow(dead_code)]
pub(crate) async fn resolve_and_validate_target(url: &str) -> Result<(), Status> {
    resolve_and_pin_target(url).await.map(|_| ())
}

// ── HMAC signing (reuses the vendored sha2-based HMAC; no new crypto crate) ────

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// The `X-Udb-Signature` value for a body: `sha256=<hex(HMAC-SHA256(secret,body))>`.
/// Computed with [`crate::runtime::security::hmac_sha256`] (the manual sha2 HMAC
/// the Vault/TOTP lanes share) so no `hmac` crate is added.
pub(crate) fn sign_webhook_body(secret: &str, body: &[u8]) -> String {
    let mac = crate::runtime::security::hmac_sha256(secret.as_bytes(), body);
    format!("sha256={}", hex_lower(&mac))
}

/// The replay-resistant `X-Udb-Signature` value: the HMAC covers
/// `"<timestamp>.<body>"` (unix seconds `.` raw body, no separator escaping — the
/// timestamp is digits-only so the `.` boundary is unambiguous) rather than the
/// body alone. The `timestamp` is transmitted in the `X-Udb-Timestamp` header, so
/// a receiver recomputes the same material and rejects any delivery whose
/// timestamp falls outside its freshness window — a captured `(signature, body)`
/// pair can no longer be replayed indefinitely. Scheme:
/// `sha256=<hex(HMAC-SHA256(secret, "<timestamp>.<body>"))>`.
pub(crate) fn sign_webhook_body_with_timestamp(
    secret: &str,
    timestamp: i64,
    body: &[u8],
) -> String {
    let ts = timestamp.to_string();
    let mut signed = Vec::with_capacity(ts.len() + 1 + body.len());
    signed.extend_from_slice(ts.as_bytes());
    signed.push(b'.');
    signed.extend_from_slice(body);
    let mac = crate::runtime::security::hmac_sha256(secret.as_bytes(), &signed);
    format!("sha256={}", hex_lower(&mac))
}

/// Mint a fresh per-endpoint signing secret from two UUIDv4s (256 bits), matching
/// the no-extra-RNG-dependency convention used for TOTP secrets.
pub(crate) fn generate_signing_secret() -> String {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    bytes.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

// ── tenant-bound subscription matching (fail closed) ──────────────────────────

/// Glob match for a topic subscription pattern. `*` matches everything; a trailing
/// `*` (e.g. `udb.*`) is a prefix match; anything else is an exact match. An empty
/// pattern matches NOTHING (fail closed) so a misconfigured endpoint never
/// silently subscribes to the whole bus.
pub(crate) fn topic_matches_pattern(pattern: &str, topic: &str) -> bool {
    let pattern = pattern.trim();
    let topic = topic.trim();
    if pattern.is_empty() || topic.is_empty() {
        return false;
    }
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return topic.starts_with(prefix);
    }
    pattern == topic
}

/// The fail-closed delivery gate: an event is delivered to an endpoint ONLY when
/// (1) the event payload's `tenant_id` is non-empty and equals the endpoint's
/// verified `endpoint_tenant`, and (2) the topic matches the endpoint's
/// subscription pattern. The subscription is BOUND to the endpoint tenant — it is
/// never pattern-only — so a tenant-less event or a cross-tenant event is dropped,
/// mirroring `engine_tail::payload_value_matches_stream_scope` (#8).
pub(crate) fn webhook_event_matches_endpoint_scope(
    endpoint_tenant: &str,
    endpoint_pattern: &str,
    topic: &str,
    payload: &serde_json::Value,
) -> bool {
    let endpoint_tenant = endpoint_tenant.trim();
    if endpoint_tenant.is_empty() {
        return false;
    }
    let event_tenant = payload
        .get("tenant_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim();
    // Tenant BOUND to the endpoint: a tenant-less or foreign-tenant event is
    // never delivered (no pattern-only fan-out — the fail-open trap).
    if event_tenant.is_empty() || event_tenant != endpoint_tenant {
        return false;
    }
    topic_matches_pattern(endpoint_pattern, topic)
}
