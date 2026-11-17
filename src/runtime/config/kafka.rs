//! Kafka client connection security.
//!
//! Every Kafka producer, consumer and admin client UDB builds (the CDC and
//! outbox relay, the transactional CDC producer, the asset-service consumers
//! and the `udb doctor` probe) takes its connection security from here:
//! plaintext (the default) or TLS, a CA file, an optional client certificate
//! and key for mutual TLS, certificate verification and hostname
//! verification.
//!
//! The settings are read once from the environment by
//! [`UdbConfig::merge_env`](super::UdbConfig::merge_env), checked by the
//! outbound transport startup gate, and installed there. Client builders use
//! the installed copy through [`kafka_client_config`] and never read the
//! environment themselves.
//!
//! A bad combination is refused, never guessed: a CA or certificate set while
//! the protocol is still plaintext is an error, not a quiet upgrade or a quiet
//! plaintext connection.

use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

/// `plaintext` (default) or `ssl`.
pub const KAFKA_SECURITY_PROTOCOL_ENV: &str = "UDB_KAFKA_SECURITY_PROTOCOL";
/// CA file used to verify the Kafka brokers' certificates.
pub const KAFKA_SSL_CA_LOCATION_ENV: &str = "UDB_KAFKA_SSL_CA_LOCATION";
/// Client certificate for mutual TLS.
pub const KAFKA_SSL_CERTIFICATE_LOCATION_ENV: &str = "UDB_KAFKA_SSL_CERTIFICATE_LOCATION";
/// Client private key for mutual TLS.
pub const KAFKA_SSL_KEY_LOCATION_ENV: &str = "UDB_KAFKA_SSL_KEY_LOCATION";
/// Password of an encrypted client private key.
pub const KAFKA_SSL_KEY_PASSWORD_ENV: &str = "UDB_KAFKA_SSL_KEY_PASSWORD";
/// `https` (default: check the broker hostname) or `none`.
pub const KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_ENV: &str =
    "UDB_KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM";
/// `true` (default) or `false`.
pub const KAFKA_SSL_CERTIFICATE_VERIFICATION_ENV: &str = "UDB_KAFKA_SSL_CERTIFICATE_VERIFICATION";

/// Kafka connection security, as configured. Values are kept as written and
/// checked by [`KafkaTlsConfig::client_properties`], so a typo is refused
/// with the setting's name instead of being read as a default.
#[derive(Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct KafkaTlsConfig {
    /// `plaintext` or `ssl` (`UDB_KAFKA_SECURITY_PROTOCOL`). Unset means plaintext.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_protocol: Option<String>,
    /// CA file path (`UDB_KAFKA_SSL_CA_LOCATION`). Unset means the system roots.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssl_ca_location: Option<String>,
    /// Client certificate path (`UDB_KAFKA_SSL_CERTIFICATE_LOCATION`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssl_certificate_location: Option<String>,
    /// Client private key path (`UDB_KAFKA_SSL_KEY_LOCATION`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssl_key_location: Option<String>,
    /// Password of the client private key (`UDB_KAFKA_SSL_KEY_PASSWORD`).
    /// A secret: never serialized, redacted from `Debug`.
    #[serde(skip_serializing)]
    pub ssl_key_password: Option<String>,
    /// `https` or `none` (`UDB_KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM`).
    /// Unset means `https`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssl_endpoint_identification_algorithm: Option<String>,
    /// `true` or `false` (`UDB_KAFKA_SSL_CERTIFICATE_VERIFICATION`). Unset
    /// means `true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssl_certificate_verification: Option<String>,
}

impl std::fmt::Debug for KafkaTlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaTlsConfig")
            .field("security_protocol", &self.security_protocol)
            .field("ssl_ca_location", &self.ssl_ca_location)
            .field("ssl_certificate_location", &self.ssl_certificate_location)
            .field("ssl_key_location", &self.ssl_key_location)
            .field(
                "ssl_key_password",
                &self.ssl_key_password.as_ref().map(|_| "[redacted]"),
            )
            .field(
                "ssl_endpoint_identification_algorithm",
                &self.ssl_endpoint_identification_algorithm,
            )
            .field(
                "ssl_certificate_verification",
                &self.ssl_certificate_verification,
            )
            .finish()
    }
}

fn non_empty(value: &Option<String>) -> Option<&str> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

impl KafkaTlsConfig {
    /// Overlay the `UDB_KAFKA_*` security settings onto this config. Only a
    /// non-empty variable replaces a file value.
    pub fn merge_env(&mut self) {
        let read = |key: &str| crate::runtime::executor_utils::env_first(&[key]);
        if let Some(v) = read(KAFKA_SECURITY_PROTOCOL_ENV) {
            self.security_protocol = Some(v);
        }
        if let Some(v) = read(KAFKA_SSL_CA_LOCATION_ENV) {
            self.ssl_ca_location = Some(v);
        }
        if let Some(v) = read(KAFKA_SSL_CERTIFICATE_LOCATION_ENV) {
            self.ssl_certificate_location = Some(v);
        }
        if let Some(v) = read(KAFKA_SSL_KEY_LOCATION_ENV) {
            self.ssl_key_location = Some(v);
        }
        if let Some(v) = read(KAFKA_SSL_KEY_PASSWORD_ENV) {
            self.ssl_key_password = Some(v);
        }
        if let Some(v) = read(KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_ENV) {
            self.ssl_endpoint_identification_algorithm = Some(v);
        }
        if let Some(v) = read(KAFKA_SSL_CERTIFICATE_VERIFICATION_ENV) {
            self.ssl_certificate_verification = Some(v);
        }
    }

    /// The environment alone, for callers that run before startup installed
    /// the resolved copy (CLI commands, unit tests).
    pub fn from_env() -> Self {
        let mut config = Self::default();
        config.merge_env();
        config
    }

    /// `source` wins field by field; `base` fills the gaps.
    pub fn merged(base: Self, source: Self) -> Self {
        Self {
            security_protocol: source.security_protocol.or(base.security_protocol),
            ssl_ca_location: source.ssl_ca_location.or(base.ssl_ca_location),
            ssl_certificate_location: source
                .ssl_certificate_location
                .or(base.ssl_certificate_location),
            ssl_key_location: source.ssl_key_location.or(base.ssl_key_location),
            ssl_key_password: source.ssl_key_password.or(base.ssl_key_password),
            ssl_endpoint_identification_algorithm: source
                .ssl_endpoint_identification_algorithm
                .or(base.ssl_endpoint_identification_algorithm),
            ssl_certificate_verification: source
                .ssl_certificate_verification
                .or(base.ssl_certificate_verification),
        }
    }

    /// True when nothing is set (keeps the serialized config unchanged).
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// The librdkafka properties these settings stand for, checked. An empty
    /// list means plaintext, librdkafka's own default. Returns an error naming
    /// the setting for any value UDB will not guess at.
    pub fn client_properties(&self) -> Result<Vec<(&'static str, String)>, String> {
        let protocol = non_empty(&self.security_protocol)
            .unwrap_or("plaintext")
            .to_ascii_lowercase();
        let ssl_settings = [
            (KAFKA_SSL_CA_LOCATION_ENV, &self.ssl_ca_location),
            (
                KAFKA_SSL_CERTIFICATE_LOCATION_ENV,
                &self.ssl_certificate_location,
            ),
            (KAFKA_SSL_KEY_LOCATION_ENV, &self.ssl_key_location),
            (KAFKA_SSL_KEY_PASSWORD_ENV, &self.ssl_key_password),
            (
                KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_ENV,
                &self.ssl_endpoint_identification_algorithm,
            ),
            (
                KAFKA_SSL_CERTIFICATE_VERIFICATION_ENV,
                &self.ssl_certificate_verification,
            ),
        ];
        match protocol.as_str() {
            "plaintext" => {
                let set: Vec<&str> = ssl_settings
                    .iter()
                    .filter(|(_, value)| non_empty(value).is_some())
                    .map(|(name, _)| *name)
                    .collect();
                if set.is_empty() {
                    return Ok(Vec::new());
                }
                Err(format!(
                    "{} is set but {KAFKA_SECURITY_PROTOCOL_ENV} is plaintext, so the Kafka \
                     connection would not use TLS; set {KAFKA_SECURITY_PROTOCOL_ENV}=ssl, or \
                     remove the TLS settings",
                    set.join(", ")
                ))
            }
            "ssl" => self.ssl_properties(),
            "sasl_plaintext" | "sasl_ssl" => Err(format!(
                "{KAFKA_SECURITY_PROTOCOL_ENV}={protocol} is not supported: UDB does not \
                 configure Kafka SASL credentials; use ssl, with \
                 {KAFKA_SSL_CERTIFICATE_LOCATION_ENV} and {KAFKA_SSL_KEY_LOCATION_ENV} for \
                 mutual TLS"
            )),
            other => Err(format!(
                "{KAFKA_SECURITY_PROTOCOL_ENV}={other} is not valid; use plaintext or ssl"
            )),
        }
    }

    fn ssl_properties(&self) -> Result<Vec<(&'static str, String)>, String> {
        let mut props: Vec<(&'static str, String)> = vec![("security.protocol", "ssl".into())];
        if let Some(ca) = non_empty(&self.ssl_ca_location) {
            props.push(("ssl.ca.location", ca.to_string()));
        }
        let certificate = non_empty(&self.ssl_certificate_location);
        let key = non_empty(&self.ssl_key_location);
        match (certificate, key) {
            (Some(certificate), Some(key)) => {
                props.push(("ssl.certificate.location", certificate.to_string()));
                props.push(("ssl.key.location", key.to_string()));
            }
            (None, None) => {}
            _ => {
                return Err(format!(
                    "{KAFKA_SSL_CERTIFICATE_LOCATION_ENV} and {KAFKA_SSL_KEY_LOCATION_ENV} must be \
                     set together for mutual TLS; set both, or neither"
                ));
            }
        }
        if let Some(password) = non_empty(&self.ssl_key_password) {
            if key.is_none() {
                return Err(format!(
                    "{KAFKA_SSL_KEY_PASSWORD_ENV} is set without {KAFKA_SSL_KEY_LOCATION_ENV}; \
                     set the key file, or remove the password"
                ));
            }
            props.push(("ssl.key.password", password.to_string()));
        }
        let algorithm = non_empty(&self.ssl_endpoint_identification_algorithm)
            .unwrap_or("https")
            .to_ascii_lowercase();
        if !matches!(algorithm.as_str(), "https" | "none") {
            return Err(format!(
                "{KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_ENV}={algorithm} is not valid; use \
                 https (check the broker hostname) or none"
            ));
        }
        props.push(("ssl.endpoint.identification.algorithm", algorithm));
        let verification = match non_empty(&self.ssl_certificate_verification)
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            None | Some("1" | "true" | "yes" | "on") => "true",
            Some("0" | "false" | "no" | "off") => "false",
            Some(other) => {
                return Err(format!(
                    "{KAFKA_SSL_CERTIFICATE_VERIFICATION_ENV}={other} is not valid; use true or \
                     false"
                ));
            }
        };
        props.push(("enable.ssl.certificate.verification", verification.into()));
        Ok(props)
    }
}

static INSTALLED: OnceLock<KafkaTlsConfig> = OnceLock::new();

/// Install the settings resolved at startup. The first install wins.
pub fn install_kafka_tls(config: KafkaTlsConfig) {
    let _ = INSTALLED.set(config);
}

/// The settings in force: the copy installed at startup, or (before startup
/// ran, e.g. in a CLI command or a unit test) the environment read once.
pub fn current_kafka_tls() -> &'static KafkaTlsConfig {
    INSTALLED.get_or_init(KafkaTlsConfig::from_env)
}

/// A librdkafka client config for `brokers` carrying the installed connection
/// security. Every Kafka client UDB builds starts here. Fails, naming the
/// setting, when the security settings are invalid; it never falls back to
/// plaintext.
#[cfg(feature = "kafka")]
pub fn kafka_client_config(brokers: &str) -> Result<rdkafka::ClientConfig, String> {
    let properties = current_kafka_tls()
        .client_properties()
        .map_err(|err| format!("Kafka client settings are invalid: {err}"))?;
    let mut config = rdkafka::ClientConfig::new();
    config.set("bootstrap.servers", brokers);
    for (key, value) in properties {
        config.set(key, value);
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ssl() -> KafkaTlsConfig {
        KafkaTlsConfig {
            security_protocol: Some("ssl".into()),
            ..Default::default()
        }
    }

    fn get<'a>(props: &'a [(&'static str, String)], key: &str) -> Option<&'a str> {
        props
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.as_str())
    }

    #[test]
    fn unset_is_plaintext_with_no_properties() {
        assert!(
            KafkaTlsConfig::default()
                .client_properties()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn ssl_defaults_to_verified_hostname_check() {
        let props = ssl().client_properties().unwrap();
        assert_eq!(get(&props, "security.protocol"), Some("ssl"));
        assert_eq!(
            get(&props, "ssl.endpoint.identification.algorithm"),
            Some("https")
        );
        assert_eq!(
            get(&props, "enable.ssl.certificate.verification"),
            Some("true")
        );
        assert_eq!(get(&props, "ssl.ca.location"), None);
    }

    #[test]
    fn ssl_with_ca_and_client_certificate() {
        let config = KafkaTlsConfig {
            security_protocol: Some("SSL".into()),
            ssl_ca_location: Some("/etc/udb/kafka-ca.pem".into()),
            ssl_certificate_location: Some("/etc/udb/kafka.crt".into()),
            ssl_key_location: Some("/etc/udb/kafka.key".into()),
            ssl_key_password: Some("s3cr3t-canary".into()),
            ..Default::default()
        };
        let props = config.client_properties().unwrap();
        assert_eq!(
            get(&props, "ssl.ca.location"),
            Some("/etc/udb/kafka-ca.pem")
        );
        assert_eq!(
            get(&props, "ssl.certificate.location"),
            Some("/etc/udb/kafka.crt")
        );
        assert_eq!(get(&props, "ssl.key.location"), Some("/etc/udb/kafka.key"));
        assert_eq!(get(&props, "ssl.key.password"), Some("s3cr3t-canary"));
        assert!(!format!("{config:?}").contains("s3cr3t-canary"));
    }

    #[test]
    fn bad_combinations_are_refused_by_name() {
        let tls_setting_on_plaintext = KafkaTlsConfig {
            ssl_ca_location: Some("/etc/udb/kafka-ca.pem".into()),
            ..Default::default()
        };
        let err = tls_setting_on_plaintext.client_properties().unwrap_err();
        assert!(err.contains(KAFKA_SSL_CA_LOCATION_ENV) && err.contains("=ssl"));

        let half_mtls = KafkaTlsConfig {
            ssl_certificate_location: Some("/etc/udb/kafka.crt".into()),
            ..ssl()
        };
        assert!(
            half_mtls
                .client_properties()
                .unwrap_err()
                .contains(KAFKA_SSL_KEY_LOCATION_ENV)
        );

        let sasl = KafkaTlsConfig {
            security_protocol: Some("sasl_ssl".into()),
            ..Default::default()
        };
        assert!(sasl.client_properties().unwrap_err().contains("SASL"));

        let typo = KafkaTlsConfig {
            ssl_certificate_verification: Some("ture".into()),
            ..ssl()
        };
        assert!(
            typo.client_properties()
                .unwrap_err()
                .contains(KAFKA_SSL_CERTIFICATE_VERIFICATION_ENV)
        );

        let algorithm = KafkaTlsConfig {
            ssl_endpoint_identification_algorithm: Some("http".into()),
            ..ssl()
        };
        assert!(
            algorithm
                .client_properties()
                .unwrap_err()
                .contains(KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_ENV)
        );
    }

    #[test]
    fn verification_can_be_turned_off_explicitly() {
        let config = KafkaTlsConfig {
            ssl_endpoint_identification_algorithm: Some("none".into()),
            ssl_certificate_verification: Some("false".into()),
            ..ssl()
        };
        let props = config.client_properties().unwrap();
        assert_eq!(
            get(&props, "ssl.endpoint.identification.algorithm"),
            Some("none")
        );
        assert_eq!(
            get(&props, "enable.ssl.certificate.verification"),
            Some("false")
        );
    }

    #[test]
    fn merged_prefers_source() {
        let base = KafkaTlsConfig {
            security_protocol: Some("plaintext".into()),
            ssl_ca_location: Some("/base-ca.pem".into()),
            ..Default::default()
        };
        let merged = KafkaTlsConfig::merged(base, ssl());
        assert_eq!(merged.security_protocol.as_deref(), Some("ssl"));
        assert_eq!(merged.ssl_ca_location.as_deref(), Some("/base-ca.pem"));
    }
}
