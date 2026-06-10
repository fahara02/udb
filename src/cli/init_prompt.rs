//! Line-mode prompt fallback for `udb init`.
//!
//! This module is deliberately narrow: it collects choices and returns them to
//! the init core. Planning, validation, and disk writes stay outside prompts.

#![allow(unexpected_cfgs)]

/// Result type used by init prompt scaffolding.
pub type InitPromptResult<T> = Result<T, InitPromptError>;

/// Prompt fallback errors, including the non-feature stub case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitPromptError {
    FeatureDisabled(&'static str),
    /// A live prompt interaction failed (cancelled, I/O error). Only reachable
    /// when compiled with the `init-prompts` feature, which is the only code
    /// path that drives an interactive prompt.
    #[cfg(feature = "init-prompts")]
    Prompt(String),
}

impl std::fmt::Display for InitPromptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FeatureDisabled(feature) => {
                write!(f, "the `{feature}` feature is not enabled in this build")
            }
            #[cfg(feature = "init-prompts")]
            Self::Prompt(message) => write!(f, "init prompt error: {message}"),
        }
    }
}

impl std::error::Error for InitPromptError {}

/// Option catalog and defaults supplied by the init planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitPromptCatalog {
    pub profiles: Vec<String>,
    pub frameworks: Vec<String>,
    pub backends: Vec<String>,
    pub native_services: Vec<String>,
    pub features: Vec<String>,
    pub default_profile: Option<String>,
    pub default_framework: Option<String>,
    pub default_backends: Vec<String>,
    pub default_native_services: Vec<String>,
    pub default_features: Vec<String>,
}

impl Default for InitPromptCatalog {
    fn default() -> Self {
        Self {
            profiles: vec![
                "local".to_string(),
                "dev".to_string(),
                "ci".to_string(),
                "prod".to_string(),
            ],
            frameworks: Vec::new(),
            backends: vec!["postgres".to_string(), "redis".to_string()],
            native_services: Vec::new(),
            features: Vec::new(),
            default_profile: Some("local".to_string()),
            default_framework: None,
            default_backends: vec!["postgres".to_string()],
            default_native_services: Vec::new(),
            default_features: Vec::new(),
        }
    }
}

/// User choices collected by prompt mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitPromptSelection {
    pub profile: String,
    pub framework: Option<String>,
    pub backends: Vec<String>,
    pub native_services: Vec<String>,
    pub features: Vec<String>,
    pub confirm_apply: bool,
}

/// Run the `inquire` prompt fallback when compiled with `init-prompts`.
#[cfg(feature = "init-prompts")]
pub fn run_init_prompts(catalog: InitPromptCatalog) -> InitPromptResult<InitPromptSelection> {
    use inquire::{Confirm, MultiSelect, Select};

    let profile = select_one(
        "Profile",
        catalog.profiles,
        catalog.default_profile.as_deref(),
    )?;

    let framework = if catalog.frameworks.is_empty() {
        None
    } else {
        Some(select_one(
            "Framework",
            catalog.frameworks,
            catalog.default_framework.as_deref(),
        )?)
    };

    let backends = select_many("Backends", catalog.backends, &catalog.default_backends)?;
    let native_services = select_many(
        "Native services",
        catalog.native_services,
        &catalog.default_native_services,
    )?;
    let features = select_many("Features", catalog.features, &catalog.default_features)?;
    let confirm_apply = Confirm::new("Apply the generated init plan?")
        .with_default(false)
        .prompt()
        .map_err(|error| InitPromptError::Prompt(error.to_string()))?;

    Ok(InitPromptSelection {
        profile,
        framework,
        backends,
        native_services,
        features,
        confirm_apply,
    })
}

/// Return a clear error when prompt fallback support is absent.
#[cfg(not(feature = "init-prompts"))]
pub fn run_init_prompts(_catalog: InitPromptCatalog) -> InitPromptResult<InitPromptSelection> {
    Err(InitPromptError::FeatureDisabled("init-prompts"))
}

#[cfg(feature = "init-prompts")]
fn select_one(
    message: &str,
    options: Vec<String>,
    default: Option<&str>,
) -> InitPromptResult<String> {
    if options.is_empty() {
        return Err(InitPromptError::Prompt(format!(
            "no options available for {message}"
        )));
    }

    let default_index = default
        .and_then(|value| options.iter().position(|option| option == value))
        .unwrap_or(0);

    Select::new(message, options)
        .with_starting_cursor(default_index)
        .prompt()
        .map_err(|error| InitPromptError::Prompt(error.to_string()))
}

#[cfg(feature = "init-prompts")]
fn select_many(
    message: &str,
    options: Vec<String>,
    defaults: &[String],
) -> InitPromptResult<Vec<String>> {
    if options.is_empty() {
        return Ok(Vec::new());
    }

    let default_indexes = options
        .iter()
        .enumerate()
        .filter_map(|(index, option)| defaults.contains(option).then_some(index))
        .collect::<Vec<_>>();

    MultiSelect::new(message, options)
        .with_default(&default_indexes)
        .prompt()
        .map_err(|error| InitPromptError::Prompt(error.to_string()))
}
