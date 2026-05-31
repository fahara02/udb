// src/hooks.rs — Pre/post migration SQL hook configuration.
//
// Gap 13 (legacy_sql): execute optional SQL files immediately before and after
// all migrations in a run.  Pre-hook failure aborts the run; post-hook failure
// is non-fatal (logged as a warning).
//
// The Rust library exposes the hook config types so the CLI tool can validate
// the configuration and so the Go service can load hook file paths from YAML.
//
// Aligned with:
//   - legacy_sql  legacycore/db/migrate_hooks.go  (HookConfig, RunPreMigrateHook,
//                                              RunPostMigrateHook)

use serde::{Deserialize, Serialize};

// ── Config ────────────────────────────────────────────────────────────────────

/// Pre/post migration SQL hook configuration.
///
/// SQL files at the configured paths are executed directly against the primary
/// database using the same timeout as regular migration files.
///
/// * `pre_migrate_sql`  — executed **before** any migration files; failure halts the run.
/// * `post_migrate_sql` — executed **after** all migrations; failure is non-fatal.
///
/// Mirrors `HookConfig` in legacy_sql `migrate_hooks.go`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HookConfig {
    /// Path to the pre-migration SQL file.  Empty string disables the pre-hook.
    pub pre_migrate_sql: String,
    /// Path to the post-migration SQL file.  Empty string disables the post-hook.
    pub post_migrate_sql: String,
}

impl HookConfig {
    /// Build a `HookConfig` from raw path strings.
    pub fn new(pre: impl Into<String>, post: impl Into<String>) -> Self {
        Self {
            pre_migrate_sql: pre.into(),
            post_migrate_sql: post.into(),
        }
    }

    /// Returns `true` when the pre-migrate SQL hook is configured.
    pub fn has_pre_hook(&self) -> bool {
        !self.pre_migrate_sql.trim().is_empty()
    }

    /// Returns `true` when the post-migrate SQL hook is configured.
    pub fn has_post_hook(&self) -> bool {
        !self.post_migrate_sql.trim().is_empty()
    }

    /// Returns `true` when either hook is configured.
    pub fn is_active(&self) -> bool {
        self.has_pre_hook() || self.has_post_hook()
    }
}

// ── Hook execution result ─────────────────────────────────────────────────────

/// The outcome of executing a SQL hook file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HookOutcome {
    /// The hook was not configured (path was empty).
    NotConfigured,
    /// The hook file was found and executed successfully.
    Ok { path: String, duration_ms: u64 },
    /// The hook file could not be read or failed to execute.
    Failed { path: String, error: String },
}

impl HookOutcome {
    /// Returns `true` when the hook completed without error (including when not configured).
    pub fn is_ok(&self) -> bool {
        matches!(self, HookOutcome::NotConfigured | HookOutcome::Ok { .. })
    }

    /// Returns `true` when the hook failed.
    pub fn is_failed(&self) -> bool {
        matches!(self, HookOutcome::Failed { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_config_defaults_no_hooks() {
        let cfg = HookConfig::default();
        assert!(!cfg.has_pre_hook());
        assert!(!cfg.has_post_hook());
        assert!(!cfg.is_active());
    }

    #[test]
    fn hook_config_with_pre_hook() {
        let cfg = HookConfig::new("db/hooks/pre_migrate.sql", "");
        assert!(cfg.has_pre_hook());
        assert!(!cfg.has_post_hook());
        assert!(cfg.is_active());
    }

    #[test]
    fn hook_outcome_not_configured_is_ok() {
        let outcome = HookOutcome::NotConfigured;
        assert!(outcome.is_ok());
        assert!(!outcome.is_failed());
    }

    #[test]
    fn hook_outcome_failed_is_not_ok() {
        let outcome = HookOutcome::Failed {
            path: "pre.sql".to_string(),
            error: "syntax error".to_string(),
        };
        assert!(!outcome.is_ok());
        assert!(outcome.is_failed());
    }
}
