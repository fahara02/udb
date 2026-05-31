use serde::{Deserialize, Serialize};
#[cfg(feature = "runtime-logging")]
use tracing_subscriber::EnvFilter;

#[cfg(feature = "runtime-logging")]
use super::pretty_log::PrettyLog;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TraceContext {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub correlation_id: String,
}

#[cfg(feature = "runtime-logging")]
pub fn init_observability() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // JSON mode: opt-in via UDB_LOG_JSON=1 (e.g. in production / log aggregation).
    // Default: human-readable pretty output.
    let json = std::env::var("UDB_LOG_JSON")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes"))
        .unwrap_or(false);

    if json {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .try_init();
    } else {
        let colors = std::env::var("NO_COLOR").is_err()
            && std::env::var("UDB_NO_COLOR")
                .map(|v| !matches!(v.as_str(), "1" | "true"))
                .unwrap_or(true);
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .event_format(PrettyLog { colors })
            .try_init();
    }
}

#[cfg(not(feature = "runtime-logging"))]
pub fn init_observability() {}

impl TraceContext {
    pub fn from_correlation(correlation_id: impl Into<String>) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            ..Self::default()
        }
    }

    pub fn is_present(&self) -> bool {
        !self.trace_id.is_empty() || !self.correlation_id.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MetricLabels {
    pub tier: String,
    pub backend: String,
    pub resource_kind: String,
    pub operation: String,
}
