//! U4 step 2 — project-scoped backend routing.
//!
//! U4 step 1 (already shipped) added `CatalogManager::active_for(project_id)`
//! and made data/vector/object/transaction handlers resolve the active
//! manifest per request. The remaining gap, called out as
//! *"backend instance mapping and routing rules remain mostly
//! runtime-wide"* in the upgrade doc, is **routing**: which physical
//! backend instance is a given `(project, backend, instance)` request
//! allowed to land on?
//!
//! Pre-U4-step-2 the answer was *"any unlabeled instance matches any
//! project"* (see `runtime/core/accessors.rs::instance_matches_project`).
//! That's permissive for backward compatibility but unsafe in a
//! strict multi-tenant deployment where Project A and Project B share
//! a broker but must not share Qdrant collections.
//!
//! This module supplies:
//!
//! - [`InstanceProjectLabel`] — typed view of how a runtime instance
//!   declares its project membership. Wraps the existing label-map
//!   conventions (`project_id`, `project`, `projects` list) so the
//!   parsing logic isn't string-matched in three places.
//! - [`ProjectRoutingMode`] — operator-controlled strictness:
//!   `Permissive` (current behaviour: unlabeled = any project),
//!   `Strict` (unlabeled = only the default project), and
//!   `StrictWithDefault{name}` (unlabeled = exactly the named project,
//!   no wildcard).
//! - [`ProjectAccessDecision`] — typed outcome (`Allowed`,
//!   `NotProvisioned{reason}`) that handlers map to the right gRPC
//!   code without string-parsing an `Err(String)`.
//! - [`evaluate_instance_for_project`] — the pure decision function
//!   used by `accessors.rs` (and now also by tests) to gate every
//!   resolution path consistently.
//!
//! The module is pure logic; tests drive every decision branch
//! without a real runtime.

use std::collections::HashMap;

// ── InstanceProjectLabel ──────────────────────────────────────────────────────

/// Typed view of an instance's project-label declaration. Built once
/// from the instance's `labels` map and re-used by the router.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstanceProjectLabel {
    /// No `project_id` / `project` / `projects` label. Treated per
    /// [`ProjectRoutingMode`].
    Unlabeled,
    /// Single project label. `*` is a wildcard — any project may use
    /// the instance.
    Single(String),
    /// Comma-separated `projects` list. Wildcards and individual
    /// matches both work.
    Multiple(Vec<String>),
}

impl InstanceProjectLabel {
    /// Parse from the instance's label map. Mirrors the order
    /// `accessors.rs` checks: `project_id` first, then `project`, then
    /// `projects`. Empty values are dropped to avoid promoting a
    /// blank to a wildcard.
    pub fn from_labels(labels: &HashMap<String, String>) -> Self {
        if let Some(value) = labels
            .get("project_id")
            .or_else(|| labels.get("project"))
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            return Self::Single(value.to_string());
        }
        if let Some(values) = labels
            .get("projects")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            let list: Vec<String> = values
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
            if !list.is_empty() {
                return Self::Multiple(list);
            }
        }
        Self::Unlabeled
    }

    /// Whether this label declaration directly permits a given
    /// project. `Unlabeled` always returns `false` — the caller mixes
    /// in [`ProjectRoutingMode`] to decide what unlabeled means.
    pub fn directly_permits(&self, project_id: &str) -> bool {
        match self {
            Self::Unlabeled => false,
            Self::Single(label) => label == "*" || label == project_id,
            Self::Multiple(list) => list.iter().any(|l| l == "*" || l == project_id),
        }
    }
}

// ── ProjectRoutingMode ────────────────────────────────────────────────────────

/// Operator-controlled strictness for how unlabeled instances are
/// routed. Set per deployment via `UDB_PROJECT_ROUTING_MODE` (env value
/// `permissive` / `strict` / `strict_with_default:<name>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectRoutingMode {
    /// Current default. Unlabeled instances match every project.
    /// Matches the legacy pre-U4 behaviour.
    Permissive,
    /// Strict isolation. An unlabeled instance is only available to
    /// requests with project_id `""` or `"default"`.
    Strict,
    /// Strict isolation with a configurable default. Unlabeled
    /// instances are bound to exactly `name` — requests with any
    /// other `project_id` are denied unless the instance carries an
    /// explicit label.
    StrictWithDefault { name: String },
}

impl Default for ProjectRoutingMode {
    fn default() -> Self {
        Self::Permissive
    }
}

impl ProjectRoutingMode {
    /// Parse and validate the env-driven config value.
    ///
    /// Empty and the documented permissive/strict aliases retain their legacy
    /// meaning. Unknown tokens and a blank `strict_with_default:` name are
    /// configuration errors because treating an operator typo as permissive
    /// would silently widen every unlabeled backend instance to every project.
    pub fn try_parse(value: &str) -> Result<Self, String> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Ok(Self::Permissive);
        }
        let lowered = trimmed.to_ascii_lowercase();
        if let Some(rest) = lowered.strip_prefix("strict_with_default:") {
            let name = rest.trim();
            if name.is_empty() {
                return Err(
                    "project_routing_mode strict_with_default requires a non-empty project name"
                        .to_string(),
                );
            }
            return Ok(Self::StrictWithDefault {
                name: name.to_string(),
            });
        }
        match lowered.as_str() {
            "permissive" | "compat" | "lax" => Ok(Self::Permissive),
            "strict" | "isolated" | "tenant" => Ok(Self::Strict),
            _ => Err(format!(
                "project_routing_mode '{trimmed}' is invalid; expected permissive, compat, lax, strict, isolated, tenant, or strict_with_default:<project>"
            )),
        }
    }

    /// Runtime routing must stay fail closed even if a caller bypasses startup
    /// validation and constructs `UdbConfig` directly. Startup reports the
    /// configuration error; this fallback prevents the invalid value from ever
    /// becoming permissive authority in the meantime.
    pub fn parse(value: &str) -> Self {
        Self::try_parse(value).unwrap_or(Self::Strict)
    }
}

// ── ProjectAccessDecision ─────────────────────────────────────────────────────

/// Typed outcome of evaluating one instance against one project.
/// Handlers map the variants to gRPC codes (`Allowed → continue`,
/// `NotProvisioned → NotFound`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectAccessDecision {
    /// The instance is provisioned for this project.
    Allowed,
    /// The instance is NOT provisioned for this project. `reason` is
    /// the operator-facing explanation — used as the gRPC error
    /// message verbatim.
    NotProvisioned { reason: String },
}

impl ProjectAccessDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }
}

// ── Evaluator ─────────────────────────────────────────────────────────────────

/// Decide whether the project may use this instance.
///
/// `project_id` is the inbound request's project (may be empty —
/// treated as the default project).
/// `instance_labels` is the runtime instance's label map.
/// `mode` is the operator's strictness setting.
pub fn evaluate_instance_for_project(
    project_id: &str,
    instance_labels: &HashMap<String, String>,
    mode: &ProjectRoutingMode,
) -> ProjectAccessDecision {
    let label = InstanceProjectLabel::from_labels(instance_labels);
    let normalized = normalize_project_id(project_id);

    // Explicit label always wins — strictness only affects unlabeled.
    if !matches!(label, InstanceProjectLabel::Unlabeled) {
        return if label.directly_permits(normalized) {
            ProjectAccessDecision::Allowed
        } else {
            ProjectAccessDecision::NotProvisioned {
                reason: format!(
                    "instance is labeled for a different project; project '{normalized}' is not in the allow-list"
                ),
            }
        };
    }

    // Unlabeled instance — mode decides.
    match mode {
        ProjectRoutingMode::Permissive => ProjectAccessDecision::Allowed,
        ProjectRoutingMode::Strict => {
            if normalized.is_empty() || normalized == "default" {
                ProjectAccessDecision::Allowed
            } else {
                ProjectAccessDecision::NotProvisioned {
                    reason: format!(
                        "unlabeled instance is restricted to the default project under strict routing; project '{normalized}' is not provisioned"
                    ),
                }
            }
        }
        ProjectRoutingMode::StrictWithDefault { name } => {
            if normalized == name.as_str() {
                ProjectAccessDecision::Allowed
            } else {
                ProjectAccessDecision::NotProvisioned {
                    reason: format!(
                        "unlabeled instance is bound to default project '{name}' under strict routing; project '{normalized}' is not provisioned"
                    ),
                }
            }
        }
    }
}

fn normalize_project_id(project_id: &str) -> &str {
    let p = project_id.trim();
    if p.is_empty() { "default" } else { p }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Pin: label parser picks up the three legacy conventions and
    /// prefers `project_id` > `project` > `projects` in that order.
    #[test]
    fn label_parser_prefers_project_id_then_project_then_list() {
        let one = labels(&[("project_id", "a"), ("project", "b"), ("projects", "c,d")]);
        assert_eq!(
            InstanceProjectLabel::from_labels(&one),
            InstanceProjectLabel::Single("a".to_string())
        );
        let two = labels(&[("project", "b"), ("projects", "c,d")]);
        assert_eq!(
            InstanceProjectLabel::from_labels(&two),
            InstanceProjectLabel::Single("b".to_string())
        );
        let three = labels(&[("projects", "c, d , e")]);
        assert_eq!(
            InstanceProjectLabel::from_labels(&three),
            InstanceProjectLabel::Multiple(
                vec!["c".to_string(), "d".to_string(), "e".to_string(),]
            )
        );
        let empty = labels(&[("project_id", "  ")]);
        assert_eq!(
            InstanceProjectLabel::from_labels(&empty),
            InstanceProjectLabel::Unlabeled,
            "blank label must not promote to wildcard"
        );
        let none = labels(&[]);
        assert_eq!(
            InstanceProjectLabel::from_labels(&none),
            InstanceProjectLabel::Unlabeled
        );
    }

    /// Pin: explicit labels gate access regardless of strictness mode.
    /// An instance labeled for project A is unreachable from project B
    /// in every mode.
    #[test]
    fn explicit_label_blocks_other_projects_in_every_mode() {
        let l = labels(&[("project_id", "a")]);
        for mode in [
            ProjectRoutingMode::Permissive,
            ProjectRoutingMode::Strict,
            ProjectRoutingMode::StrictWithDefault {
                name: "default".to_string(),
            },
        ] {
            assert!(
                evaluate_instance_for_project("a", &l, &mode).is_allowed(),
                "project A must reach its labeled instance under {:?}",
                mode
            );
            let decision = evaluate_instance_for_project("b", &l, &mode);
            assert!(!decision.is_allowed(), "project B blocked under {:?}", mode);
            match decision {
                ProjectAccessDecision::NotProvisioned { reason } => {
                    assert!(
                        reason.contains("'b' is not in the allow-list"),
                        "got: {reason}"
                    );
                }
                _ => unreachable!(),
            }
        }
    }

    /// Pin: wildcard `*` permits every project even with a single-
    /// label declaration. Operator escape hatch.
    #[test]
    fn wildcard_label_permits_every_project() {
        let l = labels(&[("project_id", "*")]);
        for project in ["a", "b", "default", "anything-goes"] {
            assert!(
                evaluate_instance_for_project(project, &l, &ProjectRoutingMode::Strict)
                    .is_allowed(),
                "wildcard must let {project} in even under Strict"
            );
        }
    }

    /// Pin: under Permissive, an unlabeled instance is reachable
    /// from any project. Matches legacy behaviour.
    #[test]
    fn unlabeled_instance_under_permissive_matches_every_project() {
        let l = labels(&[]);
        for project in ["", "default", "a", "b"] {
            assert!(
                evaluate_instance_for_project(project, &l, &ProjectRoutingMode::Permissive)
                    .is_allowed()
            );
        }
    }

    /// Pin: under Strict, an unlabeled instance is reachable only by
    /// the default project. Project A is blocked with a typed reason.
    #[test]
    fn unlabeled_instance_under_strict_only_serves_default() {
        let l = labels(&[]);
        assert!(evaluate_instance_for_project("", &l, &ProjectRoutingMode::Strict).is_allowed());
        assert!(
            evaluate_instance_for_project("default", &l, &ProjectRoutingMode::Strict).is_allowed()
        );
        let decision = evaluate_instance_for_project("project-a", &l, &ProjectRoutingMode::Strict);
        assert!(!decision.is_allowed());
        match decision {
            ProjectAccessDecision::NotProvisioned { reason } => {
                assert!(
                    reason.contains("restricted to the default project"),
                    "got: {reason}"
                );
            }
            _ => unreachable!(),
        }
    }

    /// Pin: StrictWithDefault binds unlabeled instances to one
    /// named project. Other projects are blocked even if their
    /// id is "default".
    #[test]
    fn strict_with_default_binds_unlabeled_to_named_project() {
        let l = labels(&[]);
        let mode = ProjectRoutingMode::StrictWithDefault {
            name: "alpha".to_string(),
        };
        assert!(evaluate_instance_for_project("alpha", &l, &mode).is_allowed());
        // "default" is no longer special when a named default is set.
        assert!(!evaluate_instance_for_project("default", &l, &mode).is_allowed());
        assert!(!evaluate_instance_for_project("beta", &l, &mode).is_allowed());
    }

    /// Pin: env parser. The wire contract for
    /// `UDB_PROJECT_ROUTING_MODE` — operators rely on the canonical
    /// tokens.
    #[test]
    fn env_parser_recognises_canonical_tokens() {
        assert_eq!(
            ProjectRoutingMode::parse("permissive"),
            ProjectRoutingMode::Permissive
        );
        assert_eq!(
            ProjectRoutingMode::parse("Strict"),
            ProjectRoutingMode::Strict
        );
        assert_eq!(
            ProjectRoutingMode::parse("strict_with_default:alpha"),
            ProjectRoutingMode::StrictWithDefault {
                name: "alpha".to_string()
            }
        );
        // Empty retains the documented legacy permissive behavior.
        assert_eq!(
            ProjectRoutingMode::parse(""),
            ProjectRoutingMode::Permissive
        );
        for alias in ["compat", "lax"] {
            assert_eq!(
                ProjectRoutingMode::try_parse(alias),
                Ok(ProjectRoutingMode::Permissive)
            );
        }
        for alias in ["isolated", "tenant"] {
            assert_eq!(
                ProjectRoutingMode::try_parse(alias),
                Ok(ProjectRoutingMode::Strict)
            );
        }
    }

    #[test]
    fn invalid_routing_modes_fail_closed_and_report_configuration_errors() {
        assert!(ProjectRoutingMode::try_parse("garbage").is_err());
        assert_eq!(
            ProjectRoutingMode::parse("garbage"),
            ProjectRoutingMode::Strict
        );
        assert!(ProjectRoutingMode::try_parse("strict_with_default:").is_err());
        assert_eq!(
            ProjectRoutingMode::parse("strict_with_default:"),
            ProjectRoutingMode::Strict
        );
    }

    /// Pin: `projects` list with wildcards mixed in still works.
    /// Some operators stage a migration by listing the new project
    /// plus `*` for the cutover window.
    #[test]
    fn projects_list_handles_wildcard_mixed_with_names() {
        let l = labels(&[("projects", "alpha, *, beta")]);
        for project in ["alpha", "beta", "gamma", "anything"] {
            assert!(
                evaluate_instance_for_project(project, &l, &ProjectRoutingMode::Strict)
                    .is_allowed()
            );
        }
    }

    /// Pin: a `projects` list without wildcards rejects everything
    /// outside the list.
    #[test]
    fn projects_list_without_wildcard_rejects_outside_projects() {
        let l = labels(&[("projects", "alpha,beta")]);
        assert!(
            evaluate_instance_for_project("alpha", &l, &ProjectRoutingMode::Permissive)
                .is_allowed()
        );
        assert!(
            !evaluate_instance_for_project("gamma", &l, &ProjectRoutingMode::Permissive)
                .is_allowed()
        );
    }
}
