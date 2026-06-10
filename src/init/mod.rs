//! Pure `udb init` planning core.
//!
//! This module is intentionally presentation-free: CLI, prompt, and TUI layers
//! can all call [`build_init_plan`] and serialize the returned plan for preview
//! or dry-run output before any executor writes to disk.

pub mod catalog;
pub mod executor;
pub mod fsm;
pub mod mutations;
pub mod plan;
pub mod workspace;

pub use catalog::{
    BackendCatalogEntry, BackendRole, FrameworkCatalogEntry, NativeServiceCatalogEntry,
    backend_catalog, default_backend_ids_for_features, framework_catalog, native_service_catalog,
};
pub use executor::{ApplyReceipt, ApplyWrite, RevertReceipt, apply_init_plan, revert_last_init};
pub use fsm::{
    HsmEvent, InitEvent, InitMachine, InitSnapshot, InitState, StatigInitMachine,
    statig_smoke_trace,
};
pub use mutations::{MutationAction, MutationKind, PlanMutation, WarningLevel};
pub use plan::{
    DbOpsPolicy, FindingSeverity, InitOptions, InitPlan, InitPlanError, InitPlanResult,
    InitProfile, InitSelections, ProtoStrategy, ValidationFinding, build_init_plan,
};
pub use workspace::{
    DetectedFramework, DetectedLanguage, PackageManager, RootKind, WorkspaceScan, scan_workspace,
};
