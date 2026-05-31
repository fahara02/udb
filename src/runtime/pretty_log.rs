//! Human-readable, coloured terminal log formatter for UDB.
//!
//! Output format (one line per event):
//!
//! ```text
//! 13:06 INFO  Starting force-sync lifecycle [udb::control::lifecycle]
//! 13:06 WARN  Qdrant backend not configured [udb]
//! 13:06 ERROR Failed to apply artifact: constraint already exists [udb::runtime]
//! ```
//!
//! Activated when `UDB_LOG_JSON` is **not** set to `1`/`true`/`yes`.
//! Disable colours with the standard `NO_COLOR` env var or `UDB_NO_COLOR=1`.

use std::fmt;

use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

// ── ANSI escape helpers ──────────────────────────────────────────────────────

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";

// level colours
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RED: &str = "\x1b[91m"; // bright red
const CYAN: &str = "\x1b[36m";
const MAGENTA: &str = "\x1b[35m";

// ── Formatter ────────────────────────────────────────────────────────────────

/// A compact, single-line log formatter with optional ANSI colours.
pub struct PrettyLog {
    pub colors: bool,
}

impl<S, N> FormatEvent<S, N> for PrettyLog
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let level = *event.metadata().level();
        let target = event.metadata().target();

        // ── timestamp  HH:MM ────────────────────────────────────────────────
        let now = chrono::Local::now();
        let time_str = now.format("%H:%M").to_string();

        if self.colors {
            write!(writer, "{DIM}{time_str}{RESET} ")?;
        } else {
            write!(writer, "{time_str} ")?;
        }

        // ── level badge (5 chars wide for alignment) ─────────────────────
        let (color, badge) = match level {
            Level::ERROR => (RED, "ERROR"),
            Level::WARN => (YELLOW, "WARN "),
            Level::INFO => (GREEN, "INFO "),
            Level::DEBUG => (CYAN, "DEBUG"),
            Level::TRACE => (MAGENTA, "TRACE"),
        };

        if self.colors {
            write!(writer, "{BOLD}{color}{badge}{RESET} ")?;
        } else {
            write!(writer, "{badge} ")?;
        }

        // ── message + structured fields ──────────────────────────────────
        ctx.format_fields(writer.by_ref(), event)?;

        // ── target in brackets (dim) ─────────────────────────────────────
        // Skip overly noisy targets that add no value at the terminal.
        let skip_target = target.is_empty()
            || target == "udb"
            || target.starts_with("tokio::")
            || target.starts_with("hyper::")
            || target.starts_with("h2::")
            || target.starts_with("tower::");

        if !skip_target {
            if self.colors {
                write!(writer, "  {DIM}[{target}]{RESET}")?;
            } else {
                write!(writer, "  [{target}]")?;
            }
        }

        // ── span fields (key=val pairs from parent spans, dim) ───────────
        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                let ext = span.extensions();
                if let Some(fields) = ext.get::<tracing_subscriber::fmt::FormattedFields<N>>()
                    && !fields.is_empty()
                {
                    if self.colors {
                        write!(writer, "  {DIM}{fields}{RESET}")?
                    } else {
                        write!(writer, "  {fields}")?
                    }
                }
            }
        }

        writeln!(writer)
    }
}
