// `backend` now lives at the crate root (`crate::backend`) as the single source
// of truth for backend identity. Re-exported here for path compatibility.
pub use crate::backend;
pub mod broker;
pub mod provisioning;
// `planning::registry` was folded into `crate::backend` in U2 step 6. The
// `BackendKind` source of truth + `Backend` plugin trait + `support_state_for_kind`
// helper replace the parallel `BackendRegistry`/`BackendStatus`/`BackendRegistration`
// types. External callers used only the support-state check (`lint.rs`); they now
// hit the canonical seam.
