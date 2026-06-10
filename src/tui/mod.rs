//! Feature-gated TUI scaffolding for the future `udb init` wizard.
//!
//! This module is intentionally presentation-only. Callers provide an
//! [`InitTuiDriver`] that owns domain state and receives abstract [`InitEvent`]
//! values. The TUI renders [`InitSnapshot`] values and never mutates files.

#![allow(unexpected_cfgs)]

pub mod init_app;
pub mod terminal;

pub use init_app::{
    InitAction, InitEvent, InitInput, InitSnapshot, InitStep, InitStepStatus, InitTuiApp,
    InitTuiDriver,
};
pub use terminal::TerminalSession;

/// Result type used by the TUI scaffolding.
pub type TuiResult<T> = Result<T, TuiError>;

/// Error type shared by feature-enabled and stub TUI entry points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TuiError {
    /// The requested presentation feature was not compiled into this binary.
    FeatureDisabled(&'static str),
    /// Terminal setup, teardown, or IO failed.
    Terminal(String),
    /// Rendering failed.
    Render(String),
    /// Input polling or event conversion failed.
    Event(String),
    /// The user cancelled the wizard.
    Cancelled,
    /// The caller-provided driver rejected an event.
    Driver(String),
}

impl std::fmt::Display for TuiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FeatureDisabled(feature) => {
                write!(f, "the `{feature}` feature is not enabled in this build")
            }
            Self::Terminal(message) => write!(f, "terminal error: {message}"),
            Self::Render(message) => write!(f, "render error: {message}"),
            Self::Event(message) => write!(f, "event error: {message}"),
            Self::Cancelled => f.write_str("init wizard cancelled"),
            Self::Driver(message) => write!(f, "init driver error: {message}"),
        }
    }
}

impl std::error::Error for TuiError {}

/// Run the future full-screen init wizard.
#[cfg(feature = "tui")]
pub fn run_init_tui<D>(driver: D) -> TuiResult<D::Output>
where
    D: InitTuiDriver,
{
    InitTuiApp::new(driver).run()
}

/// Stub entry point used when the `tui` feature is absent.
#[cfg(not(feature = "tui"))]
pub fn run_init_tui<D>(_driver: D) -> TuiResult<D::Output>
where
    D: InitTuiDriver,
{
    Err(TuiError::FeatureDisabled("tui"))
}
