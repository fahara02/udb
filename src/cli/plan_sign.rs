//! `udb plan sign` and `udb plan seal`: the supported way to produce the signed
//! `ApprovedPlan` that `serve` requires when an approval signing key is configured.
//!
//! Each approver signs alone with their own key, read from the environment and never
//! from a flag. `seal` then checks every signature under the same policy `serve` applies.

use serde_json::{Value, json};
use udb::control::plan_approval::{
    ApprovalConfig, ApprovalSignature, ApprovedPlan, ExportedPlan, compute_signature,
};

use super::catalog_transition::{read_json, write_new_json};

fn read_plan(path: &str) -> Result<ExportedPlan, String> {
    serde_json::from_value(read_json(path)?)
        .map_err(|err| format!("{path} is not a plan from `udb plan --emit-approval-plan`: {err}"))
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

fn sign(
    plan: &ExportedPlan,
    config: &ApprovalConfig,
    approver: &str,
    role: &str,
    reason: &str,
    now_unix_ms: i64,
) -> Result<ApprovalSignature, String> {
    if approver.trim().is_empty() {
        return Err("--approver <id> is required".to_string());
    }
    if reason.trim().is_empty() {
        return Err("--reason <text> is required".to_string());
    }
    let key = config
        .key_for(approver)
        .filter(|key| !key.is_empty())
        .ok_or_else(|| {
            format!(
                "no signing key for approver '{approver}': set UDB_APPROVAL_SIGNING_KEY, or list \
                 the approver in UDB_APPROVAL_APPROVER_KEYS"
            )
        })?;
    Ok(ApprovalSignature {
        approver_id: approver.to_string(),
        approver_role: role.to_string(),
        approved_at_unix_ms: now_unix_ms,
        reason: reason.to_string(),
        signature: compute_signature(&plan.operations_hash, approver, reason, key),
    })
}

fn seal(
    plan: ExportedPlan,
    signatures: Vec<ApprovalSignature>,
    config: &ApprovalConfig,
    now_unix_ms: i64,
) -> Result<ApprovedPlan, String> {
    ApprovedPlan::create(plan, signatures, config, now_unix_ms)
        .map_err(|err| format!("the signatures do not satisfy the approval policy: {err:?}"))
}

fn to_value<T: serde::Serialize>(value: &T) -> Result<Value, String> {
    serde_json::to_value(value).map_err(|err| format!("could not serialize: {err}"))
}

/// `udb plan sign`: write one approver's signature over the plan.
pub(crate) fn run_sign(
    plan_path: &str,
    approver: &str,
    role: &str,
    reason: &str,
    out: &str,
) -> Result<(), String> {
    let plan = read_plan(plan_path)?;
    let signature = sign(
        &plan,
        &ApprovalConfig::from_env(),
        approver,
        role,
        reason,
        now_unix_ms(),
    )?;
    write_new_json(out, &to_value(&signature)?)?;
    eprintln!("plan sign: {approver} signed {plan_path}; wrote {out}");
    Ok(())
}

/// `udb plan seal`: check the signatures and write the plan `serve` accepts.
pub(crate) fn run_seal(
    plan_path: &str,
    signature_paths: &[String],
    out: &str,
) -> Result<(), String> {
    let plan = read_plan(plan_path)?;
    let mut signatures = Vec::with_capacity(signature_paths.len());
    for path in signature_paths {
        signatures.push(
            serde_json::from_value::<ApprovalSignature>(read_json(path)?)
                .map_err(|err| format!("{path} is not a signature from `udb plan sign`: {err}"))?,
        );
    }
    let approved = seal(plan, signatures, &ApprovalConfig::from_env(), now_unix_ms())?;
    write_new_json(out, &to_value(&approved)?)?;
    eprintln!(
        "{}",
        json!({
            "sealed": out,
            "signatures": approved.signatures.len(),
            "expires_at_unix_ms": approved.expires_at_unix_ms,
        })
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(quorum: u32, keys: &[(&str, &str)]) -> ApprovalConfig {
        ApprovalConfig {
            quorum_size: quorum,
            allowed_roles: Vec::new(),
            expiry: std::time::Duration::from_secs(3600),
            signing_key: if keys.is_empty() {
                b"shared".to_vec()
            } else {
                Vec::new()
            },
            approver_keys: keys
                .iter()
                .map(|(id, key)| (id.to_string(), key.as_bytes().to_vec()))
                .collect(),
        }
    }

    fn plan() -> ExportedPlan {
        ExportedPlan {
            operations_hash: "sha256:abc".to_string(),
            ..ExportedPlan::default()
        }
    }

    #[test]
    fn one_approver_signs_and_the_seal_verifies() {
        let policy = config(1, &[]);
        let signature = sign(&plan(), &policy, "dba", "dba", "reviewed", 1).unwrap();
        let sealed = seal(plan(), vec![signature], &policy, 1).unwrap();
        assert_eq!(sealed.signatures.len(), 1);
        assert!(!sealed.seal.is_empty());
    }

    #[test]
    fn a_quorum_needs_each_approver_key_and_enough_signatures() {
        let policy = config(2, &[("a", "key-a"), ("b", "key-b")]);
        let a = sign(&plan(), &policy, "a", "", "ok", 1).unwrap();
        assert!(seal(plan(), vec![a.clone()], &policy, 1).is_err());
        let b = sign(&plan(), &policy, "b", "", "ok", 1).unwrap();
        assert!(seal(plan(), vec![a, b], &policy, 1).is_ok());
        assert!(sign(&plan(), &policy, "stranger", "", "ok", 1).is_err());
    }

    #[test]
    fn a_signature_for_another_plan_or_key_does_not_seal() {
        let policy = config(1, &[]);
        let signature = sign(&plan(), &policy, "dba", "", "ok", 1).unwrap();
        let other = ExportedPlan {
            operations_hash: "sha256:other".to_string(),
            ..ExportedPlan::default()
        };
        assert!(seal(other, vec![signature.clone()], &policy, 1).is_err());
        let mut other_key = config(1, &[]);
        other_key.signing_key = b"different".to_vec();
        assert!(seal(plan(), vec![signature], &other_key, 1).is_err());
    }

    #[test]
    fn an_empty_key_signs_nothing() {
        let mut policy = config(1, &[]);
        policy.signing_key.clear();
        assert!(sign(&plan(), &policy, "dba", "", "ok", 1).is_err());
    }

    #[test]
    fn an_approver_and_a_reason_are_required() {
        let policy = config(1, &[]);
        assert!(sign(&plan(), &policy, " ", "", "ok", 1).is_err());
        assert!(sign(&plan(), &policy, "dba", "", " ", 1).is_err());
    }
}
