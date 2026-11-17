//! Outbound transport policy.
//!
//! UDB opens connections to many other systems: PostgreSQL (primary, backup,
//! replicas, per-instance pools, the CDC source), Redis, the vector, search,
//! graph, document and column stores, object storage, the OTLP collector, and
//! a set of HTTP webhooks that carry secrets (OTP codes, audit events, API
//! keys). This module decides, from each DSN or URL, whether that connection is
//! encrypted and whether the server certificate is verified, and refuses to
//! start the broker when a connection that must be protected is not.
//!
//! Three flags drive the decision. Each defaults to ON under
//! `UDB_ENV=production` and OFF otherwise; an explicit value always wins:
//!
//! * `UDB_PG_TLS_REQUIRED`       — every PostgreSQL connection must use
//!   `sslmode=verify-ca` or `sslmode=verify-full`.
//! * `UDB_REDIS_TLS_REQUIRED`    — every Redis connection must use `rediss://`
//!   without `#insecure`.
//! * `UDB_OUTBOUND_TLS_REQUIRED` — every other outbound connection (HTTP
//!   backends, MySQL, SQL Server, MongoDB, Neo4j, Cassandra, Memcached, Kafka,
//!   object storage, OTLP, webhooks) must be encrypted and verified.
//!
//! A backend marked as a cloud deployment (`UDB_<BACKEND>_DEPLOY_MODE=cloud`,
//! a well-known cloud hostname, or `deploy.tls_override = true`) must be at
//! least encrypted even when its flag is off.
//!
//! Loopback hosts (`localhost`, `127.0.0.0/8`, `::1`) and unix sockets are
//! always allowed: that traffic never leaves the machine. A TLS tunnel or
//! sidecar listening on loopback is the supported way to reach a backend this
//! build cannot speak TLS to (Cassandra, Memcached).
//!
//! Kafka has no DSN: its brokers come from `UDB_KAFKA_BROKERS` and its
//! security from `UDB_KAFKA_SECURITY_PROTOCOL` and `UDB_KAFKA_SSL_*`
//! ([`crate::runtime::config::KafkaTlsConfig`]). The gate judges the two
//! together.
//!
//! The flags are read once, at startup, by [`TransportPolicy::from_env`]; the
//! per-request helpers ([`https_only_for`], [`check_secret_url`]) consult the
//! cached copy and never read the environment.

use std::net::IpAddr;
use std::sync::OnceLock;

use crate::runtime::config::{BackendDeployConfig, DeployMode, UdbConfig};

/// The env flag for PostgreSQL connections.
pub const PG_TLS_FLAG: &str = "UDB_PG_TLS_REQUIRED";
/// The env flag for Redis connections.
pub const REDIS_TLS_FLAG: &str = "UDB_REDIS_TLS_REQUIRED";
/// The env flag for every other outbound connection.
pub const OUTBOUND_TLS_FLAG: &str = "UDB_OUTBOUND_TLS_REQUIRED";

/// The resolved outbound transport flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransportPolicy {
    /// `UDB_ENV=production` (or `prod`).
    pub production: bool,
    /// PostgreSQL must be encrypted and verified.
    pub pg_tls_required: bool,
    /// Redis must be encrypted and verified.
    pub redis_tls_required: bool,
    /// Every other outbound connection must be encrypted and verified.
    pub outbound_tls_required: bool,
}

static INSTALLED: OnceLock<TransportPolicy> = OnceLock::new();

fn flag_value(raw: Option<String>, default: bool) -> bool {
    match raw {
        Some(value) if !value.trim().is_empty() => !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        ),
        _ => default,
    }
}

impl TransportPolicy {
    /// Read the flags from the environment. Startup-only.
    pub fn from_env() -> Self {
        let production = crate::runtime::security::udb_env_is_production();
        Self {
            production,
            pg_tls_required: flag_value(std::env::var(PG_TLS_FLAG).ok(), production),
            redis_tls_required: flag_value(std::env::var(REDIS_TLS_FLAG).ok(), production),
            outbound_tls_required: flag_value(std::env::var(OUTBOUND_TLS_FLAG).ok(), production),
        }
    }

    /// The flag that governs connections of `kind`, with its env name.
    fn flag_for(&self, kind: EndpointKind) -> (bool, &'static str) {
        match kind {
            EndpointKind::Postgres => (self.pg_tls_required, PG_TLS_FLAG),
            EndpointKind::Redis => (self.redis_tls_required, REDIS_TLS_FLAG),
            _ => (self.outbound_tls_required, OUTBOUND_TLS_FLAG),
        }
    }
}

/// The policy in force: the one installed at startup, or (before startup ran,
/// e.g. in a CLI command or a unit test) the environment read once.
pub fn current() -> TransportPolicy {
    *INSTALLED.get_or_init(TransportPolicy::from_env)
}

/// Install the policy resolved at startup. The first install wins.
pub fn install(policy: TransportPolicy) {
    let _ = INSTALLED.set(policy);
}

// ── Classification ────────────────────────────────────────────────────────────

/// The wire protocol family of an outbound endpoint; decides how its DSN is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointKind {
    Postgres,
    Redis,
    /// HTTP(S) or WebSocket URL (REST backends, object storage, webhooks, OTLP).
    Http,
    Mysql,
    /// SQL Server ADO connection string.
    Mssql,
    /// `host:port[,host:port]` contact points; this build has no Cassandra TLS.
    Cassandra,
    /// `mongodb://`, `mongodb+srv://`, or an HTTP Data API URL.
    Mongo,
    /// `neo4j[+s|+ssc]://`, `bolt[+s|+ssc]://`, or an HTTP URL.
    Neo4j,
    /// `memcache://`; this build has no Memcached TLS.
    Memcached,
    /// Kafka brokers with their client security, written by
    /// [`kafka_endpoint`] as `kafka://host:port,…?security.protocol=…`.
    Kafka,
}

/// How an endpoint protects its traffic, judged from its DSN or URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportSecurity {
    /// Loopback host or unix socket: the traffic never leaves the machine.
    Local,
    /// Encrypted, and the server certificate is verified.
    Verified,
    /// Encrypted, but the server certificate is not verified.
    EncryptedUnverified,
    /// Plaintext, or a mode that silently falls back to plaintext.
    Plaintext,
    /// The DSN could not be read; the gate does not judge it.
    Unknown,
}

/// The protection a connection must have.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Requirement {
    None,
    Encrypted,
    Verified,
}

#[derive(Debug, Default)]
struct ParsedUrl {
    scheme: String,
    hosts: Vec<String>,
    /// Query pairs with lower-cased keys and percent-decoded values.
    query: Vec<(String, String)>,
    fragment: String,
}

impl ParsedUrl {
    fn param(&self, keys: &[&str]) -> Option<&str> {
        self.query
            .iter()
            .find(|(key, _)| keys.contains(&key.as_str()))
            .map(|(_, value)| value.as_str())
    }
}

fn decode(value: &str) -> String {
    urlencoding::decode(value)
        .map(|value| value.into_owned())
        .unwrap_or_else(|_| value.to_string())
}

fn host_of(host_port: &str) -> String {
    let host_port = host_port.trim();
    if let Some(rest) = host_port.strip_prefix('[') {
        return rest.split(']').next().unwrap_or("").to_ascii_lowercase();
    }
    // A bare IPv6 address (several colons) has no port to strip.
    if host_port.matches(':').count() > 1 {
        return host_port.to_ascii_lowercase();
    }
    host_port
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(host_port)
        .to_ascii_lowercase()
}

fn parse_pairs(raw: &str, separator: char) -> Vec<(String, String)> {
    raw.split(separator)
        .filter_map(|pair| {
            let pair = pair.trim();
            if pair.is_empty() {
                return None;
            }
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            Some((key.trim().to_ascii_lowercase(), decode(value.trim())))
        })
        .collect()
}

fn parse_url(raw: &str) -> Option<ParsedUrl> {
    let raw = raw.trim();
    let (scheme, rest) = raw.split_once("://")?;
    let (rest, fragment) = match rest.split_once('#') {
        Some((rest, fragment)) => (rest, fragment.to_string()),
        None => (rest, String::new()),
    };
    let (rest, query) = rest.split_once('?').unwrap_or((rest, ""));
    // Userinfo ends at the last `@`; the authority ends at the next `/`.
    let after_userinfo = rest.rsplit_once('@').map(|(_, host)| host).unwrap_or(rest);
    let authority = after_userinfo.split('/').next().unwrap_or("");
    let hosts = authority
        .split(',')
        .map(|host| decode(&host_of(host)))
        .filter(|host| !host.is_empty())
        .collect();
    Some(ParsedUrl {
        scheme: scheme.trim().to_ascii_lowercase(),
        hosts,
        query: parse_pairs(query, '&'),
        fragment,
    })
}

/// Whether `host` is a loopback name or address, or a unix-socket path.
pub fn is_local_host(host: &str) -> bool {
    let host = host
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    if host.starts_with('/') || host.starts_with("%2f") {
        return true;
    }
    if host == "localhost" || host.ends_with(".localhost") {
        return true;
    }
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(v4)) => v4.is_loopback(),
        Ok(IpAddr::V6(v6)) => {
            v6.is_loopback() || v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
        }
        Err(_) => false,
    }
}

fn all_local(hosts: &[String]) -> bool {
    !hosts.is_empty() && hosts.iter().all(|host| is_local_host(host))
}

/// Whether a URL's host (every host, for a multi-host DSN) is loopback or a
/// unix socket. An unparseable URL is not local.
pub fn is_local_url(url: &str) -> bool {
    parse_url(url).is_some_and(|parsed| all_local(&parsed.hosts))
}

fn is_true(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn classify_http(parsed: &ParsedUrl) -> TransportSecurity {
    if parsed.hosts.is_empty() {
        return TransportSecurity::Unknown;
    }
    match parsed.scheme.as_str() {
        "https" | "wss" | "grpcs" => TransportSecurity::Verified,
        _ if all_local(&parsed.hosts) => TransportSecurity::Local,
        "http" | "ws" | "grpc" => TransportSecurity::Plaintext,
        _ => TransportSecurity::Unknown,
    }
}

fn classify_postgres(dsn: &str, default_sslmode: Option<&str>) -> TransportSecurity {
    let dsn = dsn.trim();
    let (hosts, sslmode, host_param) = if let Some(parsed) = parse_url(dsn) {
        let sslmode = parsed.param(&["sslmode", "ssl-mode"]).map(str::to_string);
        let host_param = parsed.param(&["host", "hostaddr"]).map(str::to_string);
        (parsed.hosts, sslmode, host_param)
    } else {
        // libpq key/value form: `host=… port=… sslmode=…`.
        let pairs = parse_pairs(dsn, ' ');
        let get = |key: &str| {
            pairs
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.trim_matches('\'').to_string())
        };
        let hosts = get("host").into_iter().collect::<Vec<_>>();
        (hosts, get("sslmode"), get("hostaddr"))
    };
    let hosts = match host_param {
        Some(host) if !host.trim().is_empty() => vec![host],
        _ => hosts,
    };
    // No host means the client's default: a local unix socket or localhost.
    if hosts.is_empty() || all_local(&hosts) {
        return TransportSecurity::Local;
    }
    let sslmode = sslmode
        .or_else(|| default_sslmode.map(str::to_string))
        .unwrap_or_else(|| "prefer".to_string())
        .to_ascii_lowercase();
    match sslmode.as_str() {
        "verify-full" | "verify-ca" => TransportSecurity::Verified,
        "require" => TransportSecurity::EncryptedUnverified,
        // disable, allow and prefer can all end up in plaintext without telling anyone.
        _ => TransportSecurity::Plaintext,
    }
}

fn classify_redis(parsed: &ParsedUrl) -> TransportSecurity {
    match parsed.scheme.as_str() {
        "redis+unix" | "unix" => TransportSecurity::Local,
        "rediss" if parsed.fragment.to_ascii_lowercase().contains("insecure") => {
            TransportSecurity::EncryptedUnverified
        }
        "rediss" => TransportSecurity::Verified,
        _ if all_local(&parsed.hosts) => TransportSecurity::Local,
        "redis" => TransportSecurity::Plaintext,
        _ => TransportSecurity::Unknown,
    }
}

fn classify_mysql(parsed: &ParsedUrl) -> TransportSecurity {
    if parsed.param(&["socket"]).is_some() || parsed.hosts.is_empty() || all_local(&parsed.hosts) {
        return TransportSecurity::Local;
    }
    let mode = parsed
        .param(&["ssl-mode", "sslmode", "ssl_mode"])
        .unwrap_or("preferred")
        .to_ascii_lowercase()
        .replace('-', "_");
    match mode.as_str() {
        "verify_identity" | "verify_ca" => TransportSecurity::Verified,
        "required" => TransportSecurity::EncryptedUnverified,
        _ => TransportSecurity::Plaintext,
    }
}

fn classify_mssql(ado: &str) -> TransportSecurity {
    let pairs: Vec<(String, String)> = ado
        .split(';')
        .filter_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            Some((
                key.trim().to_ascii_lowercase().replace(' ', ""),
                value.trim().to_string(),
            ))
        })
        .collect();
    let get = |keys: &[&str]| {
        pairs
            .iter()
            .find(|(key, _)| keys.contains(&key.as_str()))
            .map(|(_, value)| value.clone())
    };
    let Some(server) = get(&["server", "datasource", "address", "addr", "networkaddress"]) else {
        return TransportSecurity::Unknown;
    };
    let server = server.trim().to_ascii_lowercase();
    let server = server.strip_prefix("tcp:").unwrap_or(&server);
    let host = server
        .split([',', '\\'])
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    if host.is_empty() || host == "." || host == "(local)" || is_local_host(&host) {
        return TransportSecurity::Local;
    }
    let encrypted = get(&["encrypt"]).is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "true" | "yes" | "strict" | "mandatory"
        )
    });
    if !encrypted {
        return TransportSecurity::Plaintext;
    }
    if get(&["trustservercertificate"]).is_some_and(|value| is_true(&value)) {
        TransportSecurity::EncryptedUnverified
    } else {
        TransportSecurity::Verified
    }
}

fn classify_cassandra(dsn: &str) -> TransportSecurity {
    let nodes = dsn.rsplit_once('@').map(|(_, rest)| rest).unwrap_or(dsn);
    let hosts: Vec<String> = nodes
        .split(',')
        .map(host_of)
        .filter(|host| !host.is_empty())
        .collect();
    if hosts.is_empty() {
        TransportSecurity::Unknown
    } else if all_local(&hosts) {
        TransportSecurity::Local
    } else {
        // This build carries no Cassandra TLS support.
        TransportSecurity::Plaintext
    }
}

fn classify_mongo(parsed: &ParsedUrl) -> TransportSecurity {
    match parsed.scheme.as_str() {
        "http" | "https" => return classify_http(parsed),
        "mongodb" | "mongodb+srv" => {}
        _ => return TransportSecurity::Unknown,
    }
    let insecure = [
        "tlsinsecure",
        "tlsallowinvalidcertificates",
        "tlsallowinvalidhostnames",
    ]
    .iter()
    .any(|key| parsed.param(&[*key]).is_some_and(is_true));
    let explicit_tls = parsed.param(&["tls", "ssl"]);
    let tls = match explicit_tls {
        Some(value) => is_true(value),
        None => parsed.scheme == "mongodb+srv",
    };
    if !tls {
        if all_local(&parsed.hosts) {
            TransportSecurity::Local
        } else {
            TransportSecurity::Plaintext
        }
    } else if insecure {
        TransportSecurity::EncryptedUnverified
    } else {
        TransportSecurity::Verified
    }
}

fn classify_neo4j(parsed: &ParsedUrl) -> TransportSecurity {
    match parsed.scheme.as_str() {
        "neo4j+s" | "bolt+s" => TransportSecurity::Verified,
        "neo4j+ssc" | "bolt+ssc" => TransportSecurity::EncryptedUnverified,
        _ if all_local(&parsed.hosts) => TransportSecurity::Local,
        "neo4j" | "bolt" => TransportSecurity::Plaintext,
        _ => classify_http(parsed),
    }
}

fn classify_memcached(parsed: &ParsedUrl) -> TransportSecurity {
    if parsed.scheme == "memcache+unix" || all_local(&parsed.hosts) {
        TransportSecurity::Local
    } else {
        // This build carries no Memcached TLS support, so even `memcache+tls`
        // is refused by the client; judge it as plaintext here.
        TransportSecurity::Plaintext
    }
}

fn classify_kafka(parsed: &ParsedUrl) -> TransportSecurity {
    if parsed.hosts.is_empty() {
        return TransportSecurity::Unknown;
    }
    if all_local(&parsed.hosts) {
        return TransportSecurity::Local;
    }
    let protocol = parsed
        .param(&["security.protocol"])
        .unwrap_or("plaintext")
        .trim()
        .to_ascii_lowercase();
    if !matches!(protocol.as_str(), "ssl" | "sasl_ssl") {
        return TransportSecurity::Plaintext;
    }
    let verifies_certificate = parsed
        .param(&["enable.ssl.certificate.verification"])
        .is_none_or(is_true);
    let checks_hostname = parsed
        .param(&["ssl.endpoint.identification.algorithm"])
        .unwrap_or("https")
        .trim()
        .eq_ignore_ascii_case("https");
    if verifies_certificate && checks_hostname {
        TransportSecurity::Verified
    } else {
        TransportSecurity::EncryptedUnverified
    }
}

/// Judge one DSN or URL. `pg_default_sslmode` is `PGSSLMODE`, which the
/// PostgreSQL driver uses when the DSN carries no `sslmode`.
pub fn classify(
    kind: EndpointKind,
    dsn: &str,
    pg_default_sslmode: Option<&str>,
) -> TransportSecurity {
    match kind {
        EndpointKind::Postgres => classify_postgres(dsn, pg_default_sslmode),
        EndpointKind::Mssql => classify_mssql(dsn),
        EndpointKind::Cassandra => classify_cassandra(dsn),
        _ => {
            let Some(parsed) = parse_url(dsn) else {
                return TransportSecurity::Unknown;
            };
            match kind {
                EndpointKind::Redis => classify_redis(&parsed),
                EndpointKind::Http => classify_http(&parsed),
                EndpointKind::Mysql => classify_mysql(&parsed),
                EndpointKind::Mongo => classify_mongo(&parsed),
                EndpointKind::Neo4j => classify_neo4j(&parsed),
                EndpointKind::Memcached => classify_memcached(&parsed),
                EndpointKind::Kafka => classify_kafka(&parsed),
                EndpointKind::Postgres | EndpointKind::Mssql | EndpointKind::Cassandra => {
                    TransportSecurity::Unknown
                }
            }
        }
    }
}

/// What to change in the DSN to make a connection of `kind` encrypted and verified.
fn remedy(kind: EndpointKind) -> &'static str {
    match kind {
        EndpointKind::Postgres => {
            "set sslmode=verify-full in the DSN (add sslrootcert=/path/to/ca.pem when the server \
             certificate is signed by a private CA)"
        }
        EndpointKind::Redis => "use a rediss:// URL and drop any #insecure fragment",
        EndpointKind::Http => "use an https:// URL",
        EndpointKind::Mysql => {
            "set ssl-mode=VERIFY_IDENTITY in the DSN (add ssl-ca=/path/to/ca.pem for a private CA)"
        }
        EndpointKind::Mssql => {
            "set encrypt=true in the connection string and remove TrustServerCertificate=true"
        }
        EndpointKind::Cassandra => {
            "this build has no Cassandra TLS; reach Cassandra through a TLS tunnel listening on a \
             loopback address"
        }
        EndpointKind::Mongo => {
            "use mongodb+srv:// or add tls=true, and remove tlsInsecure / \
             tlsAllowInvalidCertificates / tlsAllowInvalidHostnames (an HTTP Data API URL must \
             be https://)"
        }
        EndpointKind::Neo4j => "use neo4j+s:// or bolt+s:// (or an https:// UDB_GRAPH_HTTP_URL)",
        EndpointKind::Memcached => {
            "this build has no Memcached TLS; reach Memcached through a TLS tunnel listening on a \
             loopback address"
        }
        EndpointKind::Kafka => {
            "set UDB_KAFKA_SECURITY_PROTOCOL=ssl (add UDB_KAFKA_SSL_CA_LOCATION=/path/to/ca.pem \
             for a private CA) and leave UDB_KAFKA_SSL_CERTIFICATE_VERIFICATION and \
             UDB_KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM unset, or set them to true and https"
        }
    }
}

/// One outbound connection the broker is configured to open.
#[derive(Debug, Clone)]
pub struct OutboundEndpoint {
    /// Human name, e.g. `PostgreSQL primary` or `OTP delivery webhook`.
    pub label: String,
    /// Where the DSN came from (env var or config key), named in the refusal.
    pub setting: String,
    pub kind: EndpointKind,
    /// The DSN or URL. Never printed: it may embed credentials.
    pub dsn: String,
    /// The backend is a cloud deployment or has `tls_override = true`: it must be
    /// at least encrypted whatever the flags say.
    pub deploy_requires_tls: bool,
}

impl OutboundEndpoint {
    fn new(
        label: impl Into<String>,
        setting: impl Into<String>,
        kind: EndpointKind,
        dsn: &str,
    ) -> Self {
        Self {
            label: label.into(),
            setting: setting.into(),
            kind,
            dsn: dsn.trim().to_string(),
            deploy_requires_tls: false,
        }
    }

    fn deploy(mut self, requires_tls: bool) -> Self {
        self.deploy_requires_tls = requires_tls;
        self
    }
}

fn describe(state: TransportSecurity) -> &'static str {
    match state {
        TransportSecurity::Plaintext => "is plaintext (or may silently fall back to plaintext)",
        TransportSecurity::EncryptedUnverified => {
            "is encrypted but does not verify the server certificate"
        }
        _ => "is not protected",
    }
}

/// Judge every endpoint against the policy. Returns one refusal message per
/// endpoint that does not meet its requirement; an empty list means the
/// configuration passes. Messages never contain the DSN.
pub fn evaluate(
    policy: &TransportPolicy,
    endpoints: &[OutboundEndpoint],
    pg_default_sslmode: Option<&str>,
) -> Vec<String> {
    let mut refusals = Vec::new();
    for endpoint in endpoints {
        if endpoint.dsn.is_empty() {
            continue;
        }
        let state = classify(endpoint.kind, &endpoint.dsn, pg_default_sslmode);
        let (flag_on, flag) = policy.flag_for(endpoint.kind);
        let requirement = if flag_on {
            Requirement::Verified
        } else if endpoint.deploy_requires_tls {
            Requirement::Encrypted
        } else {
            Requirement::None
        };
        let met = match state {
            TransportSecurity::Local | TransportSecurity::Verified | TransportSecurity::Unknown => {
                true
            }
            TransportSecurity::EncryptedUnverified => requirement <= Requirement::Encrypted,
            TransportSecurity::Plaintext => requirement == Requirement::None,
        };
        if met {
            continue;
        }
        let reason = if flag_on {
            format!("{flag} is on")
        } else {
            "the backend is configured as a cloud deployment (deploy mode or tls_override)"
                .to_string()
        };
        let host = host_summary(endpoint.kind, &endpoint.dsn);
        refusals.push(format!(
            "{label} ({setting}{host}) {state} and {reason}: {remedy}, or set {flag}=false to allow it",
            label = endpoint.label,
            setting = endpoint.setting,
            state = describe(state),
            remedy = remedy(endpoint.kind),
        ));
    }
    refusals
}

/// `, host <h>` for the refusal message; never the credentials.
fn host_summary(kind: EndpointKind, dsn: &str) -> String {
    let hosts = match kind {
        EndpointKind::Mssql | EndpointKind::Cassandra => return String::new(),
        _ => parse_url(dsn)
            .map(|parsed| parsed.hosts)
            .unwrap_or_default(),
    };
    if hosts.is_empty() {
        String::new()
    } else {
        format!(", host {}", hosts.join(","))
    }
}

/// The librdkafka properties the gate reads; file paths and the key password
/// never enter the endpoint string.
const KAFKA_JUDGED_PROPERTIES: [&str; 3] = [
    "security.protocol",
    "ssl.endpoint.identification.algorithm",
    "enable.ssl.certificate.verification",
];

/// The Kafka brokers as an endpoint the gate can judge: the broker hosts plus
/// the client's security properties (from
/// [`crate::runtime::config::KafkaTlsConfig::client_properties`]) as query
/// parameters. A `PROTOCOL://` prefix on a broker is dropped; librdkafka takes
/// the protocol from `security.protocol`, not from the broker address.
pub fn kafka_endpoint(brokers: &str, properties: &[(&'static str, String)]) -> OutboundEndpoint {
    let hosts = brokers
        .split(',')
        .map(|broker| {
            let broker = broker.trim();
            broker
                .split_once("://")
                .map(|(_, address)| address)
                .unwrap_or(broker)
        })
        .filter(|broker| !broker.is_empty())
        .collect::<Vec<_>>()
        .join(",");
    let query = properties
        .iter()
        .filter(|(key, _)| KAFKA_JUDGED_PROPERTIES.contains(key))
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    OutboundEndpoint::new(
        "Kafka",
        "UDB_KAFKA_BROKERS / UDB_KAFKA_SECURITY_PROTOCOL",
        EndpointKind::Kafka,
        &format!("kafka://{hosts}?{query}"),
    )
}

/// The configured Kafka broker list, when there is one.
#[cfg(feature = "kafka")]
fn configured_kafka_brokers(config: &UdbConfig) -> Option<&str> {
    config
        .kafka_brokers
        .as_deref()
        .map(str::trim)
        .filter(|brokers| !brokers.is_empty())
}

/// A refusal for Kafka security settings that cannot be applied at all (a
/// typo, a half-configured client certificate, TLS files on a plaintext
/// connection). Judged only when Kafka brokers are configured.
#[cfg(feature = "kafka")]
fn kafka_settings_refusal(config: &UdbConfig) -> Option<String> {
    configured_kafka_brokers(config)?;
    config
        .kafka_tls
        .client_properties()
        .err()
        .map(|err| format!("Kafka (UDB_KAFKA_BROKERS) client settings are invalid: {err}"))
}

// ── Startup gate ──────────────────────────────────────────────────────────────

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn instance_label_value(
    instance: &crate::runtime::config::BackendInstance,
    key: &str,
) -> Option<String> {
    instance
        .labels
        .get(key)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            instance
                .labels
                .get(format!("{key}_env").as_str())
                .and_then(|env_key| non_empty_env(env_key))
        })
}

/// The env var(s) an instance derived from the legacy single-DSN settings came
/// from, so a refusal names the setting the operator actually wrote.
fn legacy_dsn_setting(backend: &str) -> Option<&'static str> {
    Some(match backend {
        "postgres" => "UDB_PG_DSN / DATABASE_URL",
        "redis" => "UDB_REDIS_DSN",
        "qdrant" => "UDB_QDRANT_URL",
        "minio" => "UDB_MINIO_ENDPOINT",
        "mongodb" => "UDB_NOSQL_DSN / UDB_NOSQL_API_URL",
        "neo4j" => "UDB_GRAPH_DSN / UDB_GRAPH_HTTP_URL",
        "clickhouse" => "UDB_COLUMN_DSN / UDB_COLUMN_HTTP_URL",
        "mssql" => "UDB_MSSQL_DSN",
        "cassandra" => "UDB_CASSANDRA_DSN",
        _ => return None,
    })
}

/// Cloud mode or `tls_override = true` (the deploy profile's own rule).
fn deploy_tls(deploy: &BackendDeployConfig) -> bool {
    deploy.tls_required()
}

/// Collect every outbound endpoint the resolved config and the startup
/// environment name. Startup-only: reads env for settings that are not part of
/// [`UdbConfig`].
pub fn endpoints_from_config(config: &UdbConfig) -> Vec<OutboundEndpoint> {
    let mut out = Vec::new();

    // PostgreSQL roles.
    let pg_roles = [
        (
            "PostgreSQL primary",
            "UDB_PG_DSN / DATABASE_URL",
            Some(&config.primary),
        ),
        ("PostgreSQL backup", "backup DSN", config.backup.as_ref()),
        ("PostgreSQL signal", "signal DSN", config.signal.as_ref()),
    ];
    for (label, setting, db) in pg_roles {
        if let Some(db) = db
            && let Some(dsn) = crate::runtime::core::postgres_dsn_from_config(db)
        {
            out.push(
                OutboundEndpoint::new(label, setting, EndpointKind::Postgres, &dsn)
                    .deploy(deploy_tls(&db.deploy)),
            );
        }
    }
    for (index, dsn) in config.pg_replica_dsns.iter().enumerate() {
        out.push(
            OutboundEndpoint::new(
                format!("PostgreSQL read replica {index}"),
                "UDB_PG_REPLICA_DSNS",
                EndpointKind::Postgres,
                dsn,
            )
            .deploy(deploy_tls(&config.primary.deploy)),
        );
    }
    if let Some(dsn) = non_empty_env("UDB_CDC_POSTGRES_SOURCE_DSN") {
        out.push(OutboundEndpoint::new(
            "PostgreSQL CDC source",
            "UDB_CDC_POSTGRES_SOURCE_DSN",
            EndpointKind::Postgres,
            &dsn,
        ));
    }

    // Redis.
    if let Some(redis) = &config.redis
        && redis.is_configured()
    {
        let dsn = redis
            .dsn
            .clone()
            .filter(|dsn| !dsn.trim().is_empty())
            .unwrap_or_else(|| {
                let scheme = if redis.effective_tls() {
                    "rediss"
                } else {
                    "redis"
                };
                format!("{scheme}://{}:{}", redis.host, redis.effective_port())
            });
        out.push(
            OutboundEndpoint::new(
                "Redis",
                "UDB_REDIS_DSN / REDIS_URL",
                EndpointKind::Redis,
                &dsn,
            )
            .deploy(deploy_tls(&redis.deploy)),
        );
    }

    // Qdrant.
    if let Some(qdrant) = &config.qdrant
        && qdrant.is_configured()
    {
        let url = qdrant
            .url
            .clone()
            .filter(|url| !url.trim().is_empty())
            .unwrap_or_else(|| {
                let scheme = if qdrant.effective_tls() {
                    "https"
                } else {
                    "http"
                };
                format!(
                    "{scheme}://{}:{}",
                    qdrant.host,
                    qdrant.effective_http_port()
                )
            });
        out.push(
            OutboundEndpoint::new("Qdrant", "UDB_QDRANT_URL", EndpointKind::Http, &url)
                .deploy(deploy_tls(&qdrant.deploy)),
        );
    }

    // MinIO / S3.
    if let Some(minio) = &config.minio
        && minio.is_configured()
    {
        let endpoint = minio.endpoint.trim();
        let url = if endpoint.contains("://") {
            endpoint.to_string()
        } else if minio.effective_ssl() {
            format!("https://{endpoint}")
        } else {
            format!("http://{endpoint}")
        };
        out.push(
            OutboundEndpoint::new(
                "Object storage",
                "UDB_MINIO_ENDPOINT",
                EndpointKind::Http,
                &url,
            )
            .deploy(deploy_tls(&minio.deploy)),
        );
    }

    // Generic backend instances (per-instance pools and stores).
    for instance in config.backend_instances.active() {
        let Some(dsn) = instance.resolve_dsn() else {
            continue;
        };
        let backend = instance.backend.trim().to_ascii_lowercase();
        let label_cloud = instance_label_value(instance, "deploy_mode")
            .is_some_and(|mode| matches!(DeployMode::from_str(&mode), DeployMode::Cloud));
        let profile = &config.deploy;
        let (kind, url, profile_tls) = match backend.as_str() {
            "postgres" | "postgresql" | "pg" => {
                (EndpointKind::Postgres, dsn, deploy_tls(&profile.postgres))
            }
            "redis" | "valkey" => (EndpointKind::Redis, dsn, deploy_tls(&profile.redis)),
            "qdrant" => (EndpointKind::Http, dsn, deploy_tls(&profile.qdrant)),
            "minio" | "s3" => (EndpointKind::Http, dsn, deploy_tls(&profile.minio)),
            "weaviate" | "pinecone" | "elasticsearch" | "azureblob" | "gcs" => {
                (EndpointKind::Http, dsn, false)
            }
            "mongodb" | "mongo" => match instance_label_value(instance, "api_url")
                .or_else(|| instance_label_value(instance, "api_base"))
            {
                Some(api) => (EndpointKind::Http, api, deploy_tls(&profile.mongodb)),
                None => (EndpointKind::Mongo, dsn, deploy_tls(&profile.mongodb)),
            },
            "neo4j" => match instance_label_value(instance, "http_url") {
                Some(http) => (EndpointKind::Http, http, deploy_tls(&profile.neo4j)),
                None => (EndpointKind::Neo4j, dsn, deploy_tls(&profile.neo4j)),
            },
            "clickhouse" => {
                let url = instance_label_value(instance, "http_url").unwrap_or(dsn);
                // `clickhouse://` is always sent as plain http:// by the executor.
                let url = match url.strip_prefix("clickhouse://") {
                    Some(rest) => format!("http://{rest}"),
                    None => url,
                };
                (EndpointKind::Http, url, deploy_tls(&profile.clickhouse))
            }
            "mysql" | "mariadb" => (EndpointKind::Mysql, dsn, false),
            "mssql" | "sqlserver" => (EndpointKind::Mssql, dsn, false),
            "cassandra" | "scylla" => (EndpointKind::Cassandra, dsn, false),
            "memcached" | "memcache" => (EndpointKind::Memcached, dsn, false),
            _ => continue,
        };
        out.push(
            OutboundEndpoint::new(
                format!("{backend} instance '{}'", instance.name),
                instance.dsn_env.clone().unwrap_or_else(|| {
                    legacy_dsn_setting(&backend)
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("backend_instances.{}.dsn", instance.name))
                }),
                kind,
                &url,
            )
            .deploy(label_cloud || profile_tls),
        );
    }

    // Backends registered straight from their own env DSN.
    let env_backends = [
        ("MySQL", "UDB_MYSQL_DSN", EndpointKind::Mysql),
        ("Elasticsearch", "UDB_ELASTIC_DSN", EndpointKind::Http),
        ("Memcached", "UDB_MEMCACHED_DSN", EndpointKind::Memcached),
        ("Weaviate", "UDB_WEAVIATE_DSN", EndpointKind::Http),
    ];
    for (label, key, kind) in env_backends {
        if let Some(dsn) = non_empty_env(key) {
            // `apikey://<key>@host` is rebuilt as https:// by the broker.
            let dsn = match dsn.strip_prefix("apikey://") {
                Some(rest) => format!("https://{rest}"),
                None => dsn,
            };
            out.push(OutboundEndpoint::new(label, key, kind, &dsn));
        }
    }

    // OTLP trace export.
    if let Some(endpoint) = non_empty_env("OTEL_EXPORTER_OTLP_ENDPOINT") {
        out.push(OutboundEndpoint::new(
            "OTLP trace exporter",
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            EndpointKind::Http,
            &endpoint,
        ));
    }

    // HTTP endpoints that carry secrets: codes, tokens, audit events, keys.
    let mut secret_urls: Vec<(&str, &str, Option<String>)> = vec![
        (
            "OTP delivery webhook",
            "UDB_OTP_DELIVERY_WEBHOOK_URL",
            non_empty_env("UDB_OTP_DELIVERY_WEBHOOK_URL"),
        ),
        (
            "Audit export webhook",
            "UDB_AUDIT_EXPORT_WEBHOOK_URL",
            non_empty_env("UDB_AUDIT_EXPORT_WEBHOOK_URL"),
        ),
        (
            "Schema registry",
            "UDB_SCHEMA_REGISTRY_URL",
            non_empty_env("UDB_SCHEMA_REGISTRY_URL"),
        ),
        (
            "Embedding rerank sidecar",
            "UDB_EMBEDDING_RERANK_URL",
            non_empty_env("UDB_EMBEDDING_RERANK_URL"),
        ),
        (
            "OIDC issuer",
            "UDB_OIDC_ISSUER",
            non_empty_env("UDB_OIDC_ISSUER"),
        ),
        (
            "LiveKit",
            "UDB_LIVEKIT_URL",
            non_empty_env("UDB_LIVEKIT_URL"),
        ),
    ];
    let audit_sink = Some(config.security.audit_sink_url.trim().to_string())
        .filter(|url| !url.is_empty())
        .or_else(|| non_empty_env("UDB_AUDIT_SINK_URL"));
    secret_urls.push(("Audit sink", "UDB_AUDIT_SINK_URL", audit_sink));
    let jwks = config
        .security
        .jwt_jwks_url
        .clone()
        .filter(|url| !url.trim().is_empty())
        .or_else(|| non_empty_env("UDB_JWT_JWKS_URL"));
    secret_urls.push(("JWKS endpoint", "UDB_JWT_JWKS_URL", jwks));
    secret_urls.push((
        "Vault",
        "UDB_VAULT_ADDR",
        config
            .encryption
            .vault_addr
            .clone()
            .filter(|url| !url.trim().is_empty()),
    ));
    secret_urls.push((
        "Migration notification webhook",
        "migration.notification_url",
        Some(config.migration.notification_url.trim().to_string()).filter(|url| !url.is_empty()),
    ));
    for (label, setting, url) in secret_urls {
        if let Some(url) = url {
            out.push(OutboundEndpoint::new(
                label,
                setting,
                EndpointKind::Http,
                &url,
            ));
        }
    }

    // Kafka: every producer and consumer uses the one broker list and the one
    // set of security settings. Settings that cannot be applied are refused
    // separately by `kafka_settings_refusal`.
    #[cfg(feature = "kafka")]
    if let Some(brokers) = configured_kafka_brokers(config)
        && let Ok(properties) = config.kafka_tls.client_properties()
    {
        out.push(kafka_endpoint(brokers, &properties));
    }

    // The same connection is often named twice (the primary and its legacy
    // instance); judge each (kind, dsn) once.
    let mut seen = std::collections::HashSet::new();
    out.retain(|endpoint| seen.insert((endpoint.kind as u8, endpoint.dsn.clone())));
    out
}

/// The startup gate. Resolves the policy, installs it for the per-request
/// helpers, and refuses to start when any configured outbound connection does
/// not meet it. Call once, before the broker opens its pools.
pub fn enforce_at_startup(config: &UdbConfig) -> Result<(), String> {
    let policy = TransportPolicy::from_env();
    install(policy);
    crate::runtime::config::install_kafka_tls(config.kafka_tls.clone());
    let pg_default_sslmode = non_empty_env("PGSSLMODE");
    let endpoints = endpoints_from_config(config);
    let refusals = evaluate(&policy, &endpoints, pg_default_sslmode.as_deref());
    #[cfg(feature = "kafka")]
    let refusals = {
        let mut refusals = refusals;
        refusals.extend(kafka_settings_refusal(config));
        refusals
    };
    if refusals.is_empty() {
        tracing::info!(
            production = policy.production,
            pg_tls_required = policy.pg_tls_required,
            redis_tls_required = policy.redis_tls_required,
            outbound_tls_required = policy.outbound_tls_required,
            endpoints = endpoints.len(),
            "outbound transport policy passed"
        );
        return Ok(());
    }
    Err(format!(
        "outbound transport startup gate failed ({} connection(s) not protected): {}",
        refusals.len(),
        refusals.join("; ")
    ))
}

// ── Helpers for clients that send secrets ─────────────────────────────────────

fn scheme_is(url: &str, schemes: &[&str]) -> bool {
    url.trim()
        .split_once("://")
        .is_some_and(|(scheme, _)| schemes.iter().any(|s| scheme.eq_ignore_ascii_case(s)))
}

/// Whether a reqwest client for `base_url` must be built `https_only`.
///
/// True when the base URL is already https (so a redirect cannot downgrade it),
/// or when the client sends credentials, the host is not loopback, and
/// `UDB_OUTBOUND_TLS_REQUIRED` is on. A loopback http URL never gets
/// `https_only`, which would refuse it outright.
pub fn https_only_for(base_url: &str, sends_secrets: bool) -> bool {
    if scheme_is(base_url, &["https"]) {
        return true;
    }
    sends_secrets && current().outbound_tls_required && !is_local_url(base_url)
}

/// Refuse to send a secret to `url` over plaintext.
///
/// Returns an error naming `setting` when `url` is `http://` or `ws://` to a
/// non-loopback host and `UDB_OUTBOUND_TLS_REQUIRED` is on.
pub fn check_secret_url(url: &str, setting: &str) -> Result<(), String> {
    check_secret_url_with(url, setting, current().outbound_tls_required)
}

fn check_secret_url_with(url: &str, setting: &str, required: bool) -> Result<(), String> {
    if !required || !scheme_is(url, &["http", "ws"]) || is_local_url(url) {
        return Ok(());
    }
    Err(format!(
        "{setting} uses plaintext http:// to a non-loopback host, so the secret it carries would \
         cross the network unencrypted; use https://, or set {OUTBOUND_TLS_FLAG}=false to allow it"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(pg: bool, redis: bool, outbound: bool) -> TransportPolicy {
        TransportPolicy {
            production: pg && redis && outbound,
            pg_tls_required: pg,
            redis_tls_required: redis,
            outbound_tls_required: outbound,
        }
    }

    #[test]
    fn flag_value_defaults_and_overrides() {
        assert!(flag_value(None, true));
        assert!(!flag_value(None, false));
        assert!(!flag_value(Some("false".into()), true));
        assert!(!flag_value(Some(" OFF ".into()), true));
        assert!(flag_value(Some("1".into()), false));
        assert!(flag_value(Some("  ".into()), true));
    }

    #[test]
    fn loopback_and_unix_hosts_are_local() {
        for host in [
            "localhost",
            "127.0.0.1",
            "127.4.5.6",
            "::1",
            "[::1]",
            "/var/run/postgresql",
            "%2Fvar%2Frun",
            "::ffff:127.0.0.1",
        ] {
            assert!(is_local_host(host), "{host} should be local");
        }
        for host in ["db.internal", "10.0.0.5", "localhost.evil.com", "0.0.0.0"] {
            assert!(!is_local_host(host), "{host} should not be local");
        }
    }

    #[test]
    fn postgres_sslmodes() {
        let c = |dsn: &str| classify(EndpointKind::Postgres, dsn, None);
        assert_eq!(
            c("postgres://u:p@db.example.com/app"),
            TransportSecurity::Plaintext
        );
        for mode in ["disable", "allow", "prefer"] {
            assert_eq!(
                c(&format!("postgres://u:p@db.example.com/app?sslmode={mode}")),
                TransportSecurity::Plaintext
            );
        }
        assert_eq!(
            c("postgresql://u:p@db.example.com:5432/app?sslmode=require"),
            TransportSecurity::EncryptedUnverified
        );
        assert_eq!(
            c("postgresql://u:p@db.example.com/app?sslmode=verify-ca&sslrootcert=/ca.pem"),
            TransportSecurity::Verified
        );
        assert_eq!(
            c("postgres://db.example.com/app?application_name=x&sslmode=verify-full"),
            TransportSecurity::Verified
        );
        assert_eq!(
            c("postgres://u:p@127.0.0.1/app?sslmode=disable"),
            TransportSecurity::Local
        );
        assert_eq!(
            c("postgres:///app?host=/var/run/postgresql"),
            TransportSecurity::Local
        );
        assert_eq!(
            c("postgres://u:p@%2Fvar%2Frun%2Fpostgresql/app"),
            TransportSecurity::Local
        );
        assert_eq!(
            c("host=db.example.com user=u sslmode=verify-full"),
            TransportSecurity::Verified
        );
        assert_eq!(
            c("host=db.example.com user=u"),
            TransportSecurity::Plaintext
        );
        // A password with an `@` does not confuse the host.
        assert_eq!(
            c("postgres://u:p@ss@db.example.com/app?sslmode=verify-full"),
            TransportSecurity::Verified
        );
        // PGSSLMODE fills a DSN without sslmode.
        assert_eq!(
            classify(
                EndpointKind::Postgres,
                "postgres://db.example.com/app",
                Some("verify-full")
            ),
            TransportSecurity::Verified
        );
    }

    #[test]
    fn redis_schemes() {
        let c = |dsn: &str| classify(EndpointKind::Redis, dsn, None);
        assert_eq!(
            c("redis://:pw@cache.example.com:6379/0"),
            TransportSecurity::Plaintext
        );
        assert_eq!(
            c("rediss://:pw@cache.example.com:6380/0"),
            TransportSecurity::Verified
        );
        assert_eq!(
            c("rediss://cache.example.com:6380/#insecure"),
            TransportSecurity::EncryptedUnverified
        );
        assert_eq!(c("redis://localhost:6379"), TransportSecurity::Local);
        assert_eq!(c("redis+unix:///tmp/redis.sock"), TransportSecurity::Local);
    }

    #[test]
    fn http_mysql_mssql_mongo_neo4j_cassandra_memcached() {
        let c = |kind, dsn: &str| classify(kind, dsn, None);
        assert_eq!(
            c(EndpointKind::Http, "https://q.example.com:6333"),
            TransportSecurity::Verified
        );
        assert_eq!(
            c(EndpointKind::Http, "http://q.example.com:6333"),
            TransportSecurity::Plaintext
        );
        assert_eq!(
            c(EndpointKind::Http, "http://localhost:6333"),
            TransportSecurity::Local
        );
        assert_eq!(
            c(EndpointKind::Http, "wss://lk.example.com"),
            TransportSecurity::Verified
        );

        assert_eq!(
            c(EndpointKind::Mysql, "mysql://u:p@my.example.com/db"),
            TransportSecurity::Plaintext
        );
        assert_eq!(
            c(
                EndpointKind::Mysql,
                "mysql://u:p@my.example.com/db?ssl-mode=REQUIRED"
            ),
            TransportSecurity::EncryptedUnverified
        );
        assert_eq!(
            c(
                EndpointKind::Mysql,
                "mysql://u:p@my.example.com/db?ssl-mode=VERIFY_IDENTITY"
            ),
            TransportSecurity::Verified
        );

        assert_eq!(
            c(
                EndpointKind::Mssql,
                "server=tcp:sql.example.com,1433;user=sa;password=x"
            ),
            TransportSecurity::Plaintext
        );
        assert_eq!(
            c(
                EndpointKind::Mssql,
                "Server=tcp:sql.example.com,1433;Encrypt=true;TrustServerCertificate=true"
            ),
            TransportSecurity::EncryptedUnverified
        );
        assert_eq!(
            c(EndpointKind::Mssql, "Server=sql.example.com;Encrypt=true"),
            TransportSecurity::Verified
        );
        assert_eq!(
            c(EndpointKind::Mssql, "server=tcp:localhost,1433"),
            TransportSecurity::Local
        );

        assert_eq!(
            c(EndpointKind::Mongo, "mongodb+srv://u:p@c.mongodb.net/db"),
            TransportSecurity::Verified
        );
        assert_eq!(
            c(EndpointKind::Mongo, "mongodb://u:p@m1:27017,m2:27017/db"),
            TransportSecurity::Plaintext
        );
        assert_eq!(
            c(
                EndpointKind::Mongo,
                "mongodb://m1:27017/db?tls=true&tlsInsecure=true"
            ),
            TransportSecurity::EncryptedUnverified
        );
        assert_eq!(
            c(EndpointKind::Mongo, "mongodb://localhost:27017"),
            TransportSecurity::Local
        );

        assert_eq!(
            c(EndpointKind::Neo4j, "neo4j+s://x.databases.neo4j.io"),
            TransportSecurity::Verified
        );
        assert_eq!(
            c(EndpointKind::Neo4j, "bolt+ssc://g.example.com:7687"),
            TransportSecurity::EncryptedUnverified
        );
        assert_eq!(
            c(EndpointKind::Neo4j, "bolt://g.example.com:7687"),
            TransportSecurity::Plaintext
        );

        assert_eq!(
            c(EndpointKind::Cassandra, "u:p@c1:9042,c2:9042"),
            TransportSecurity::Plaintext
        );
        assert_eq!(
            c(EndpointKind::Cassandra, "127.0.0.1:9042"),
            TransportSecurity::Local
        );

        assert_eq!(
            c(
                EndpointKind::Memcached,
                "memcache+tls://mc.example.com:11211"
            ),
            TransportSecurity::Plaintext
        );
        assert_eq!(
            c(EndpointKind::Memcached, "memcache://127.0.0.1:11211"),
            TransportSecurity::Local
        );
    }

    #[test]
    fn evaluate_refuses_names_flag_and_parameter_without_leaking_dsn() {
        let endpoints = vec![
            OutboundEndpoint::new(
                "PostgreSQL primary",
                "UDB_PG_DSN",
                EndpointKind::Postgres,
                "postgres://udb:s3cr3t-canary@db.example.com/app?sslmode=require",
            ),
            OutboundEndpoint::new(
                "Redis",
                "UDB_REDIS_DSN",
                EndpointKind::Redis,
                "redis://:s3cr3t-canary@cache.example.com",
            ),
            OutboundEndpoint::new(
                "OTP delivery webhook",
                "UDB_OTP_DELIVERY_WEBHOOK_URL",
                EndpointKind::Http,
                "http://relay.example.com/otp",
            ),
        ];
        let refusals = evaluate(&policy(true, true, true), &endpoints, None);
        assert_eq!(refusals.len(), 3, "{refusals:?}");
        assert!(refusals[0].contains("UDB_PG_TLS_REQUIRED=false"));
        assert!(refusals[0].contains("sslmode=verify-full"));
        assert!(refusals[0].contains("db.example.com"));
        assert!(refusals[1].contains("UDB_REDIS_TLS_REQUIRED=false"));
        assert!(refusals[1].contains("rediss://"));
        assert!(refusals[2].contains("UDB_OUTBOUND_TLS_REQUIRED=false"));
        assert!(refusals.iter().all(|r| !r.contains("s3cr3t-canary")));
    }

    #[test]
    fn evaluate_passes_dev_posture_and_local_endpoints() {
        let endpoints = vec![
            OutboundEndpoint::new(
                "PostgreSQL primary",
                "UDB_PG_DSN",
                EndpointKind::Postgres,
                "postgres://u:p@db.example.com/app",
            ),
            OutboundEndpoint::new(
                "Redis",
                "UDB_REDIS_DSN",
                EndpointKind::Redis,
                "redis://cache.example.com",
            ),
        ];
        assert!(evaluate(&policy(false, false, false), &endpoints, None).is_empty());

        let local = vec![
            OutboundEndpoint::new(
                "PostgreSQL primary",
                "UDB_PG_DSN",
                EndpointKind::Postgres,
                "postgres://u:p@localhost/app?sslmode=disable",
            ),
            OutboundEndpoint::new(
                "Redis",
                "UDB_REDIS_DSN",
                EndpointKind::Redis,
                "redis://127.0.0.1:6379",
            ),
            OutboundEndpoint::new(
                "OTLP",
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                EndpointKind::Http,
                "http://localhost:4317",
            ),
        ];
        assert!(evaluate(&policy(true, true, true), &local, None).is_empty());
    }

    #[test]
    fn cloud_deploy_requires_encryption_even_with_flags_off() {
        let plaintext = vec![
            OutboundEndpoint::new(
                "PostgreSQL primary",
                "UDB_PG_DSN",
                EndpointKind::Postgres,
                "postgres://u:p@x.neon.tech/app?sslmode=prefer",
            )
            .deploy(true),
        ];
        let refusals = evaluate(&policy(false, false, false), &plaintext, None);
        assert_eq!(refusals.len(), 1);
        assert!(refusals[0].contains("cloud deployment"));

        let encrypted = vec![
            OutboundEndpoint::new(
                "PostgreSQL primary",
                "UDB_PG_DSN",
                EndpointKind::Postgres,
                "postgres://u:p@x.neon.tech/app?sslmode=require",
            )
            .deploy(true),
        ];
        assert!(evaluate(&policy(false, false, false), &encrypted, None).is_empty());
        // With the flag on, `require` is not enough: it does not verify.
        assert_eq!(
            evaluate(&policy(true, false, false), &encrypted, None).len(),
            1
        );
    }

    #[test]
    fn secret_url_check_respects_policy_and_loopback() {
        assert!(check_secret_url_with("http://relay.example.com", "X_URL", true).is_err());
        assert!(check_secret_url_with("http://relay.example.com", "X_URL", false).is_ok());
        assert!(check_secret_url_with("https://relay.example.com", "X_URL", true).is_ok());
        assert!(check_secret_url_with("http://127.0.0.1:8080", "X_URL", true).is_ok());
        let err =
            check_secret_url_with("ws://lk.example.com", "UDB_LIVEKIT_URL", true).unwrap_err();
        assert!(err.contains("UDB_LIVEKIT_URL") && err.contains("UDB_OUTBOUND_TLS_REQUIRED=false"));
    }

    use crate::runtime::config::KafkaTlsConfig;

    fn kafka(brokers: &str, tls: KafkaTlsConfig) -> Vec<OutboundEndpoint> {
        let properties = tls.client_properties().expect("valid Kafka settings");
        vec![kafka_endpoint(brokers, &properties)]
    }

    fn kafka_ssl() -> KafkaTlsConfig {
        KafkaTlsConfig {
            security_protocol: Some("ssl".into()),
            ..Default::default()
        }
    }

    #[test]
    fn kafka_plaintext_to_a_remote_broker_is_refused() {
        let endpoints = kafka(
            "kafka-1.example.com:9092,kafka-2.example.com:9092",
            KafkaTlsConfig::default(),
        );
        let refusals = evaluate(&policy(false, false, true), &endpoints, None);
        assert_eq!(refusals.len(), 1, "{refusals:?}");
        let refusal = &refusals[0];
        assert!(
            refusal.starts_with(
                "Kafka (UDB_KAFKA_BROKERS / UDB_KAFKA_SECURITY_PROTOCOL, host \
                 kafka-1.example.com,kafka-2.example.com) is plaintext"
            ),
            "{refusal}"
        );
        assert!(
            refusal.contains("UDB_OUTBOUND_TLS_REQUIRED is on"),
            "{refusal}"
        );
        assert!(
            refusal.contains("UDB_KAFKA_SECURITY_PROTOCOL=ssl"),
            "{refusal}"
        );
        assert!(
            refusal.contains("UDB_OUTBOUND_TLS_REQUIRED=false"),
            "{refusal}"
        );
    }

    #[test]
    fn kafka_tls_with_verification_passes() {
        let strict = policy(true, true, true);
        assert!(evaluate(&strict, &kafka("kafka.example.com:9093", kafka_ssl()), None).is_empty());
        let with_ca_and_client_cert = KafkaTlsConfig {
            ssl_ca_location: Some("/etc/udb/kafka-ca.pem".into()),
            ssl_certificate_location: Some("/etc/udb/kafka.crt".into()),
            ssl_key_location: Some("/etc/udb/kafka.key".into()),
            ssl_certificate_verification: Some("true".into()),
            ssl_endpoint_identification_algorithm: Some("https".into()),
            ..kafka_ssl()
        };
        assert!(
            evaluate(
                &strict,
                &kafka("SSL://kafka.example.com:9093", with_ca_and_client_cert),
                None
            )
            .is_empty()
        );
    }

    #[test]
    fn kafka_tls_without_verification_is_refused() {
        let strict = policy(true, true, true);
        let no_certificate_check = KafkaTlsConfig {
            ssl_certificate_verification: Some("false".into()),
            ..kafka_ssl()
        };
        let refusals = evaluate(
            &strict,
            &kafka("kafka.example.com:9093", no_certificate_check),
            None,
        );
        assert_eq!(refusals.len(), 1, "{refusals:?}");
        assert!(refusals[0].contains("does not verify the server certificate"));
        assert!(refusals[0].contains("UDB_OUTBOUND_TLS_REQUIRED=false"));

        let no_hostname_check = KafkaTlsConfig {
            ssl_endpoint_identification_algorithm: Some("none".into()),
            ..kafka_ssl()
        };
        assert_eq!(
            evaluate(
                &strict,
                &kafka("kafka.example.com:9093", no_hostname_check),
                None
            )
            .len(),
            1
        );
    }

    #[test]
    fn kafka_plaintext_to_loopback_is_allowed() {
        let strict = policy(true, true, true);
        for brokers in [
            "localhost:9092",
            "127.0.0.1:9092,127.0.0.2:9093",
            "[::1]:9092",
        ] {
            assert!(
                evaluate(&strict, &kafka(brokers, KafkaTlsConfig::default()), None).is_empty(),
                "{brokers}"
            );
        }
        // One remote broker in the list is enough to need TLS.
        assert_eq!(
            evaluate(
                &strict,
                &kafka(
                    "localhost:9092,kafka.example.com:9092",
                    KafkaTlsConfig::default()
                ),
                None
            )
            .len(),
            1
        );
    }

    #[test]
    fn kafka_plaintext_passes_with_the_outbound_flag_off() {
        let endpoints = kafka("kafka.example.com:9092", KafkaTlsConfig::default());
        assert!(evaluate(&policy(true, true, false), &endpoints, None).is_empty());
        assert!(evaluate(&policy(false, false, false), &endpoints, None).is_empty());
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_settings_that_cannot_be_applied_are_refused() {
        let config = UdbConfig {
            kafka_brokers: Some("kafka.example.com:9093".into()),
            kafka_tls: KafkaTlsConfig {
                ssl_ca_location: Some("/etc/udb/kafka-ca.pem".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let refusal = kafka_settings_refusal(&config).expect("refused");
        assert!(refusal.contains("UDB_KAFKA_SSL_CA_LOCATION"), "{refusal}");
        assert!(
            refusal.contains("UDB_KAFKA_SECURITY_PROTOCOL=ssl"),
            "{refusal}"
        );

        let no_brokers = UdbConfig {
            kafka_brokers: None,
            ..config
        };
        assert!(kafka_settings_refusal(&no_brokers).is_none());
    }

    #[test]
    fn https_base_is_always_https_only() {
        assert!(https_only_for("https://q.example.com", false));
        assert!(https_only_for("HTTPS://q.example.com", true));
        assert!(!https_only_for("http://localhost:6333", true));
    }
}
