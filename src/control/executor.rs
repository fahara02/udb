use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use crate::provisioning::{ACTION_ENSURE, ProvisioningAction, ProvisioningPlan};

pub trait ProvisioningExecutor {
    fn apply(&mut self, action: &ProvisioningAction) -> Result<(), String>;
    fn verify(&self, action: &ProvisioningAction) -> Result<(), String>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ExecutionReport {
    pub applied: usize,
    pub verified: usize,
    pub errors: Vec<String>,
}

impl ExecutionReport {
    pub fn passed(&self) -> bool {
        self.errors.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FakeProvisioningExecutor {
    pub resources: BTreeSet<String>,
}

impl FakeProvisioningExecutor {
    pub fn apply_plan(&mut self, plan: &ProvisioningPlan) -> ExecutionReport {
        let mut report = ExecutionReport::default();
        for action in &plan.actions {
            match self.apply(action) {
                Ok(()) => report.applied += 1,
                Err(err) => report.errors.push(err),
            }
        }
        report
    }

    pub fn verify_plan(&self, plan: &ProvisioningPlan) -> ExecutionReport {
        let mut report = ExecutionReport::default();
        for action in &plan.actions {
            match self.verify(action) {
                Ok(()) => report.verified += 1,
                Err(err) => report.errors.push(err),
            }
        }
        report
    }

    pub fn repair_plan(&mut self, plan: &ProvisioningPlan) -> ExecutionReport {
        let mut report = ExecutionReport::default();
        for action in &plan.actions {
            if self.verify(action).is_err() {
                match self.apply(action) {
                    Ok(()) => report.applied += 1,
                    Err(err) => report.errors.push(err),
                }
            } else {
                report.verified += 1;
            }
        }
        report
    }
}

impl ProvisioningExecutor for FakeProvisioningExecutor {
    fn apply(&mut self, action: &ProvisioningAction) -> Result<(), String> {
        if action.action != ACTION_ENSURE {
            return Err(format!("unsupported action '{}'", action.action));
        }
        self.resources.insert(action.resource_uri.clone());
        Ok(())
    }

    fn verify(&self, action: &ProvisioningAction) -> Result<(), String> {
        if self.resources.contains(&action.resource_uri) {
            Ok(())
        } else {
            Err(format!("missing resource {}", action.resource_uri))
        }
    }
}
