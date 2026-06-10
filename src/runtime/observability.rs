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
        .map(|v| match v.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            other => {
                eprintln!(
                    "unrecognized UDB_LOG_JSON value '{other}'; expected 1/0, true/false, yes/no, or on/off"
                );
                false
            }
        })
        .unwrap_or(false);

    if json {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .try_init();
    } else {
        let colors = std::env::var("NO_COLOR").is_err()
            && std::env::var("UDB_NO_COLOR")
                .map(|v| match v.to_ascii_lowercase().as_str() {
                    "1" | "true" | "yes" | "on" => false,
                    "0" | "false" | "no" | "off" => true,
                    other => {
                        eprintln!(
                            "unrecognized UDB_NO_COLOR value '{other}'; expected 1/0, true/false, yes/no, or on/off"
                        );
                        true
                    }
                })
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

    /// Parse a W3C `traceparent` header (`00-<32hex trace_id>-<16hex span_id>-<2hex flags>`)
    /// into a [`TraceContext`]. The inbound `span_id` becomes our `parent_span_id`
    /// (it is the parent of any span we create); the `span_id` field is left empty
    /// until a local span id is assigned.
    ///
    /// Returns `None` for any structurally-invalid header (wrong arity, non-hex,
    /// wrong field widths, or the all-zero trace-id / span-id the spec forbids). The
    /// parse is pure (no otel crate) so it compiles on the slim, no-`otel` build.
    pub fn from_traceparent(header: &str) -> Option<Self> {
        let header = header.trim();
        let mut parts = header.split('-');
        let version = parts.next()?;
        let trace_id = parts.next()?;
        let parent_span_id = parts.next()?;
        let flags = parts.next()?;
        // Exactly four dash-separated fields.
        if parts.next().is_some() {
            return None;
        }
        // Only version `00` is defined; reject the reserved `ff`. (Forward-compat
        // versions would still start `00-...` so this strict check is safe.)
        if version.len() != 2 || !is_lower_hex(version) || version == "ff" {
            return None;
        }
        if trace_id.len() != 32 || !is_lower_hex(trace_id) || trace_id.bytes().all(|b| b == b'0') {
            return None;
        }
        if parent_span_id.len() != 16
            || !is_lower_hex(parent_span_id)
            || parent_span_id.bytes().all(|b| b == b'0')
        {
            return None;
        }
        if flags.len() != 2 || !is_lower_hex(flags) {
            return None;
        }
        Some(Self {
            trace_id: trace_id.to_string(),
            span_id: String::new(),
            parent_span_id: parent_span_id.to_string(),
            correlation_id: String::new(),
        })
    }

    /// Format this context as a W3C `traceparent` header for egress propagation.
    /// Uses the local `span_id` when present, otherwise the inbound
    /// `parent_span_id`, so the downstream parent links to whichever span we
    /// actually own. Returns `None` when no trace/span id is available (nothing to
    /// propagate).
    pub fn to_traceparent(&self) -> Option<String> {
        if self.trace_id.is_empty() {
            return None;
        }
        let span = if !self.span_id.is_empty() {
            self.span_id.as_str()
        } else if !self.parent_span_id.is_empty() {
            self.parent_span_id.as_str()
        } else {
            return None;
        };
        // Sampled flag (`01`) — UDB records compliance traces unconditionally.
        Some(format!("00-{}-{}-01", self.trace_id, span))
    }

    pub fn is_present(&self) -> bool {
        !self.trace_id.is_empty()
            || !self.span_id.is_empty()
            || !self.parent_span_id.is_empty()
            || !self.correlation_id.is_empty()
    }

    pub fn is_complete(&self) -> bool {
        !self.trace_id.is_empty() && !self.span_id.is_empty()
    }
}

/// True when every byte is a lowercase hex digit (`0-9` / `a-f`). W3C trace-context
/// mandates lowercase hex, so the strict check doubles as a malformed-header guard.
fn is_lower_hex(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MetricLabels {
    pub tier: String,
    pub backend: String,
    pub resource_kind: String,
    pub operation: String,
}

#[cfg(test)]
mod traceparent_tests {
    use super::TraceContext;

    const TRACE: &str = "0af7651916cd43dd8448eb211c80319c";
    const SPAN: &str = "b7ad6b7169203331";

    #[test]
    fn parses_valid_traceparent() {
        let tc = TraceContext::from_traceparent(&format!("00-{TRACE}-{SPAN}-01"))
            .expect("valid traceparent must parse");
        assert_eq!(tc.trace_id, TRACE);
        // The inbound span becomes our parent.
        assert_eq!(tc.parent_span_id, SPAN);
        assert!(tc.span_id.is_empty());
    }

    #[test]
    fn round_trips_parse_then_format() {
        let tc = TraceContext::from_traceparent(&format!("00-{TRACE}-{SPAN}-01")).unwrap();
        // With no local span id, the formatter falls back to the parent span.
        assert_eq!(
            tc.to_traceparent().unwrap(),
            format!("00-{TRACE}-{SPAN}-01")
        );
    }

    #[test]
    fn format_prefers_local_span_id() {
        let mut tc = TraceContext::from_traceparent(&format!("00-{TRACE}-{SPAN}-01")).unwrap();
        let local = "00f067aa0ba902b7";
        tc.span_id = local.to_string();
        assert_eq!(
            tc.to_traceparent().unwrap(),
            format!("00-{TRACE}-{local}-01")
        );
    }

    #[test]
    fn rejects_malformed_headers() {
        for bad in [
            "",                                                // empty
            "00-tooShort-b7ad6b7169203331-01",                 // bad trace width / non-hex
            &format!("00-{TRACE}-{SPAN}"),                     // missing flags
            &format!("00-{TRACE}-{SPAN}-01-xx"),               // extra field
            &format!("00-{TRACE}-{SPAN}-zz"),                  // non-hex flags
            &format!("ff-{TRACE}-{SPAN}-01"),                  // reserved version
            &format!("00-{}-{SPAN}-01", TRACE.to_uppercase()), // uppercase rejected
        ] {
            assert!(
                TraceContext::from_traceparent(bad).is_none(),
                "expected malformed header to be rejected: {bad}"
            );
        }
    }

    #[test]
    fn rejects_all_zero_ids() {
        let zero_trace = "0".repeat(32);
        let zero_span = "0".repeat(16);
        assert!(
            TraceContext::from_traceparent(&format!("00-{zero_trace}-{SPAN}-01")).is_none(),
            "all-zero trace id is invalid"
        );
        assert!(
            TraceContext::from_traceparent(&format!("00-{TRACE}-{zero_span}-01")).is_none(),
            "all-zero span id is invalid"
        );
    }

    #[test]
    fn empty_context_has_no_traceparent() {
        assert!(TraceContext::default().to_traceparent().is_none());
    }
}
