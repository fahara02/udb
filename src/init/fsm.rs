use crate::init::plan::{InitPlan, ValidationFinding};
use crate::init::workspace::WorkspaceScan;
use serde::{Deserialize, Serialize};
use statig::prelude::*;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InitState {
    Boot,
    TerminalProbe,
    WorkspaceScan,
    ProjectClassify,
    ModeSelect,
    LanguageFrameworkSelect,
    ProtoStrategySelect,
    BackendSelect,
    NativeServiceSelect,
    FeatureSelect,
    DbOpsSelect,
    ValidateSelection,
    BuildPlan,
    PlanPreview,
    Confirm,
    ApplyMutations,
    RunGenerators,
    Verify,
    Complete,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InitEvent {
    Start,
    TerminalDetected,
    WorkspaceScanned,
    ChoiceChanged,
    Next,
    Back,
    Validate,
    PlanBuilt,
    ConfirmApply,
    MutationApplied,
    GeneratorFinished,
    VerifyFinished,
    Cancel,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitSnapshot {
    pub cwd: PathBuf,
    pub state: InitState,
    pub workspace_scan: Option<WorkspaceScan>,
    pub findings: Vec<ValidationFinding>,
    pub plan: Option<InitPlan>,
    pub receipts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitMachine {
    snapshot: InitSnapshot,
    history: Vec<InitState>,
}

impl InitMachine {
    pub fn new(cwd: impl Into<PathBuf>) -> Self {
        Self {
            snapshot: InitSnapshot {
                cwd: cwd.into(),
                state: InitState::Boot,
                workspace_scan: None,
                findings: Vec::new(),
                plan: None,
                receipts: Vec::new(),
            },
            history: Vec::new(),
        }
    }

    pub fn snapshot(&self) -> &InitSnapshot {
        &self.snapshot
    }

    pub fn set_workspace_scan(&mut self, scan: WorkspaceScan) {
        self.snapshot.workspace_scan = Some(scan);
    }

    pub fn set_findings(&mut self, findings: Vec<ValidationFinding>) {
        self.snapshot.findings = findings;
    }

    pub fn set_plan(&mut self, plan: InitPlan) {
        self.snapshot.plan = Some(plan);
    }

    pub fn push_receipt(&mut self, receipt: impl Into<String>) {
        self.snapshot.receipts.push(receipt.into());
    }

    pub fn dispatch(&mut self, event: InitEvent) -> InitState {
        let is_back = event == InitEvent::Back;
        let next = match (&self.snapshot.state, event) {
            (_, InitEvent::Cancel) => InitState::Cancelled,
            (_, InitEvent::Error) => InitState::Failed,
            (InitState::Boot, InitEvent::Start) => InitState::TerminalProbe,
            (InitState::TerminalProbe, InitEvent::TerminalDetected) => InitState::WorkspaceScan,
            (InitState::WorkspaceScan, InitEvent::WorkspaceScanned) => InitState::ProjectClassify,
            (InitState::ProjectClassify, InitEvent::Next) => InitState::ModeSelect,
            (InitState::ModeSelect, InitEvent::Next) => InitState::LanguageFrameworkSelect,
            (InitState::LanguageFrameworkSelect, InitEvent::Next) => InitState::ProtoStrategySelect,
            (InitState::ProtoStrategySelect, InitEvent::Next) => InitState::BackendSelect,
            (InitState::BackendSelect, InitEvent::Next) => InitState::NativeServiceSelect,
            (InitState::NativeServiceSelect, InitEvent::Next) => InitState::FeatureSelect,
            (InitState::FeatureSelect, InitEvent::Next) => InitState::DbOpsSelect,
            (InitState::DbOpsSelect, InitEvent::Validate | InitEvent::Next) => {
                InitState::ValidateSelection
            }
            (InitState::ValidateSelection, InitEvent::Next) => InitState::BuildPlan,
            (InitState::BuildPlan, InitEvent::PlanBuilt) => InitState::PlanPreview,
            (InitState::PlanPreview, InitEvent::Next) => InitState::Confirm,
            (InitState::Confirm, InitEvent::ConfirmApply) => InitState::ApplyMutations,
            (InitState::ApplyMutations, InitEvent::MutationApplied) => InitState::RunGenerators,
            (InitState::RunGenerators, InitEvent::GeneratorFinished) => InitState::Verify,
            (InitState::Verify, InitEvent::VerifyFinished) => InitState::Complete,
            (_, InitEvent::ChoiceChanged) => self.snapshot.state.clone(),
            (_, InitEvent::Back) => self
                .history
                .pop()
                .unwrap_or_else(|| self.snapshot.state.clone()),
            _ => self.snapshot.state.clone(),
        };

        if next != self.snapshot.state && !is_back {
            self.history.push(self.snapshot.state.clone());
            self.snapshot.state = next;
        } else if is_back {
            self.snapshot.state = next;
        }
        self.snapshot.state.clone()
    }
}

#[derive(Default)]
pub struct StatigInitMachine {
    trace: Vec<InitState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HsmEvent {
    Start,
    TerminalDetected,
    WorkspaceScanned,
    Next,
    PlanBuilt,
    ConfirmApply,
    MutationApplied,
    GeneratorFinished,
    VerifyFinished,
    Cancel,
    Error,
}

#[state_machine(
    initial = "State::boot()",
    state(derive(Debug, Clone, PartialEq, Eq)),
    superstate(derive(Debug, Clone, PartialEq, Eq))
)]
impl StatigInitMachine {
    #[state(entry_action = "enter_boot", superstate = "active")]
    fn boot(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::Start => Transition(State::terminal_probe()),
            HsmEvent::Cancel => Transition(State::cancelled()),
            HsmEvent::Error => Transition(State::failed()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_terminal_probe", superstate = "active")]
    fn terminal_probe(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::TerminalDetected => Transition(State::workspace_scan()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_workspace_scan", superstate = "active")]
    fn workspace_scan(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::WorkspaceScanned => Transition(State::project_classify()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_project_classify", superstate = "active")]
    fn project_classify(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::Next => Transition(State::mode_select()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_mode_select", superstate = "active")]
    fn mode_select(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::Next => Transition(State::language_framework_select()),
            _ => Super,
        }
    }

    #[state(
        entry_action = "enter_language_framework_select",
        superstate = "active"
    )]
    fn language_framework_select(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::Next => Transition(State::proto_strategy_select()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_proto_strategy_select", superstate = "active")]
    fn proto_strategy_select(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::Next => Transition(State::backend_select()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_backend_select", superstate = "active")]
    fn backend_select(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::Next => Transition(State::native_service_select()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_native_service_select", superstate = "active")]
    fn native_service_select(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::Next => Transition(State::feature_select()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_feature_select", superstate = "active")]
    fn feature_select(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::Next => Transition(State::db_ops_select()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_db_ops_select", superstate = "active")]
    fn db_ops_select(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::Next => Transition(State::validate_selection()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_validate_selection", superstate = "active")]
    fn validate_selection(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::Next => Transition(State::build_plan()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_build_plan", superstate = "active")]
    fn build_plan(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::PlanBuilt => Transition(State::plan_preview()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_plan_preview", superstate = "active")]
    fn plan_preview(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::Next => Transition(State::confirm()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_confirm", superstate = "active")]
    fn confirm(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::ConfirmApply => Transition(State::apply_mutations()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_apply_mutations", superstate = "active")]
    fn apply_mutations(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::MutationApplied => Transition(State::run_generators()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_run_generators", superstate = "active")]
    fn run_generators(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::GeneratorFinished => Transition(State::verify()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_verify", superstate = "active")]
    fn verify(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::VerifyFinished => Transition(State::complete()),
            _ => Super,
        }
    }

    #[state(entry_action = "enter_complete")]
    fn complete() -> Outcome<State> {
        Handled
    }

    #[state(entry_action = "enter_cancelled")]
    fn cancelled() -> Outcome<State> {
        Handled
    }

    #[state(entry_action = "enter_failed")]
    fn failed() -> Outcome<State> {
        Handled
    }

    #[superstate]
    fn active(event: &HsmEvent) -> Outcome<State> {
        match event {
            HsmEvent::Cancel => Transition(State::cancelled()),
            HsmEvent::Error => Transition(State::failed()),
            _ => Super,
        }
    }

    #[action]
    fn enter_boot(&mut self) {
        self.trace.push(InitState::Boot);
    }
    #[action]
    fn enter_terminal_probe(&mut self) {
        self.trace.push(InitState::TerminalProbe);
    }
    #[action]
    fn enter_workspace_scan(&mut self) {
        self.trace.push(InitState::WorkspaceScan);
    }
    #[action]
    fn enter_project_classify(&mut self) {
        self.trace.push(InitState::ProjectClassify);
    }
    #[action]
    fn enter_mode_select(&mut self) {
        self.trace.push(InitState::ModeSelect);
    }
    #[action]
    fn enter_language_framework_select(&mut self) {
        self.trace.push(InitState::LanguageFrameworkSelect);
    }
    #[action]
    fn enter_proto_strategy_select(&mut self) {
        self.trace.push(InitState::ProtoStrategySelect);
    }
    #[action]
    fn enter_backend_select(&mut self) {
        self.trace.push(InitState::BackendSelect);
    }
    #[action]
    fn enter_native_service_select(&mut self) {
        self.trace.push(InitState::NativeServiceSelect);
    }
    #[action]
    fn enter_feature_select(&mut self) {
        self.trace.push(InitState::FeatureSelect);
    }
    #[action]
    fn enter_db_ops_select(&mut self) {
        self.trace.push(InitState::DbOpsSelect);
    }
    #[action]
    fn enter_validate_selection(&mut self) {
        self.trace.push(InitState::ValidateSelection);
    }
    #[action]
    fn enter_build_plan(&mut self) {
        self.trace.push(InitState::BuildPlan);
    }
    #[action]
    fn enter_plan_preview(&mut self) {
        self.trace.push(InitState::PlanPreview);
    }
    #[action]
    fn enter_confirm(&mut self) {
        self.trace.push(InitState::Confirm);
    }
    #[action]
    fn enter_apply_mutations(&mut self) {
        self.trace.push(InitState::ApplyMutations);
    }
    #[action]
    fn enter_run_generators(&mut self) {
        self.trace.push(InitState::RunGenerators);
    }
    #[action]
    fn enter_verify(&mut self) {
        self.trace.push(InitState::Verify);
    }
    #[action]
    fn enter_complete(&mut self) {
        self.trace.push(InitState::Complete);
    }
    #[action]
    fn enter_cancelled(&mut self) {
        self.trace.push(InitState::Cancelled);
    }
    #[action]
    fn enter_failed(&mut self) {
        self.trace.push(InitState::Failed);
    }
}

pub fn statig_smoke_trace() -> Vec<InitState> {
    let mut machine = StatigInitMachine::default().state_machine();
    for event in [
        HsmEvent::Start,
        HsmEvent::TerminalDetected,
        HsmEvent::WorkspaceScanned,
        HsmEvent::Next,
        HsmEvent::Next,
        HsmEvent::Next,
        HsmEvent::Next,
        HsmEvent::Next,
        HsmEvent::Next,
        HsmEvent::Next,
        HsmEvent::Next,
        HsmEvent::Next,
        HsmEvent::PlanBuilt,
        HsmEvent::Next,
        HsmEvent::ConfirmApply,
        HsmEvent::MutationApplied,
        HsmEvent::GeneratorFinished,
        HsmEvent::VerifyFinished,
    ] {
        machine.handle(&event);
    }
    machine.inner().trace.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walks_discovery_states() {
        let mut machine = InitMachine::new(".");
        assert_eq!(machine.dispatch(InitEvent::Start), InitState::TerminalProbe);
        assert_eq!(
            machine.dispatch(InitEvent::TerminalDetected),
            InitState::WorkspaceScan
        );
        assert_eq!(
            machine.dispatch(InitEvent::WorkspaceScanned),
            InitState::ProjectClassify
        );
    }

    #[test]
    fn cancel_wins_from_any_state() {
        let mut machine = InitMachine::new(".");
        machine.dispatch(InitEvent::Start);
        assert_eq!(machine.dispatch(InitEvent::Cancel), InitState::Cancelled);
    }

    #[test]
    fn back_returns_to_previous_state() {
        let mut machine = InitMachine::new(".");
        machine.dispatch(InitEvent::Start);
        machine.dispatch(InitEvent::TerminalDetected);
        assert_eq!(machine.dispatch(InitEvent::Back), InitState::TerminalProbe);
    }

    #[test]
    fn statig_hsm_reaches_complete() {
        let trace = statig_smoke_trace();
        assert_eq!(trace.last(), Some(&InitState::Complete));
        assert!(trace.contains(&InitState::PlanPreview));
    }
}
