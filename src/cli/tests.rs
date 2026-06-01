use super::*;

#[test]
fn parse_args_recognizes_doctor_without_proto_root() {
    let args = vec!["doctor".to_string()];
    let (command, proto_root, namespace, serve_addr) = parse_args(&args);

    assert!(matches!(command, Command::Doctor { .. }));
    assert_eq!(proto_root, "proto");
    assert_eq!(namespace, "");
    assert_eq!(serve_addr, "0.0.0.0:50051");
}

#[test]
fn parse_args_recognizes_health_check() {
    let args = vec!["health-check".to_string()];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(command, Command::HealthCheck));
}

#[test]
fn parse_args_recognizes_doctor_human() {
    let args = vec!["doctor".to_string(), "--human".to_string()];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::Doctor {
            output_mode: DoctorOutputMode::Human,
            ..
        }
    ));
}

#[test]
fn parse_args_recognizes_doctor_probe() {
    let args = vec!["doctor".to_string(), "--probe".to_string()];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::Doctor {
            with_probes: true,
            ..
        }
    ));
}

#[test]
fn parse_args_recognizes_init_project() {
    let args = vec!["init-project".to_string()];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(command, Command::InitProject));
}

#[test]
fn parse_args_recognizes_dev_defaults() {
    let args = vec!["dev".to_string()];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::Dev {
            action: DevAction::Up,
            service: None,
            confirmed: false,
        }
    ));
}

#[test]
fn parse_args_recognizes_dev_logs_service() {
    let args = vec![
        "dev".to_string(),
        "logs".to_string(),
        "postgres".to_string(),
    ];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::Dev {
            action: DevAction::Logs,
            service: Some(service),
            ..
        } if service == "postgres"
    ));
}

#[test]
fn parse_args_recognizes_dev_reset_confirmation() {
    let args = vec!["dev".to_string(), "reset".to_string(), "--yes".to_string()];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::Dev {
            action: DevAction::Reset,
            confirmed: true,
            ..
        }
    ));
}

#[test]
fn parse_args_recognizes_admin_verify_audit_limit() {
    let args = vec![
        "admin".to_string(),
        "verify-audit".to_string(),
        "--limit".to_string(),
        "250".to_string(),
    ];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(command, Command::AdminVerifyAudit { limit: 250 }));
}

#[test]
fn parse_args_recognizes_manifest_export() {
    let args = vec!["manifest-export".to_string()];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(command, Command::ManifestExport));
}

#[test]
fn parse_args_recognizes_policy_lint() {
    let args = vec!["policy-lint".to_string()];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(command, Command::PolicyLint));
}

#[test]
fn parse_args_recognizes_auth_commands() {
    let args = vec![
        "auth".to_string(),
        "api-key".to_string(),
        "create".to_string(),
        "--owner".to_string(),
        "svc.search".to_string(),
        "--scope".to_string(),
        "catalog:read".to_string(),
        "--scope".to_string(),
        "catalog:write".to_string(),
    ];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::Auth(AuthCommand::ApiKeyCreate {
            owner_id,
            scopes,
            ..
        }) if owner_id == "svc.search" && scopes.len() == 2
    ));
}

#[test]
fn parse_args_recognizes_auth_policy_lint() {
    let args = vec!["auth".to_string(), "policy".to_string(), "lint".to_string()];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(command, Command::Auth(AuthCommand::PolicyLint)));
}

#[test]
fn parse_args_recognizes_field_mask_preview() {
    let args = vec!["field-mask-preview".to_string()];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(command, Command::FieldMaskPreview));
}

#[test]
fn lint_policies_empty_set_returns_deny_by_default_warning() {
    use udb::lint_policies;
    let findings = lint_policies(&[]);
    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0].category, "deny_by_default");
}

#[test]
fn lint_policies_detects_broad_wildcard_deny() {
    use udb::{AbacPolicy, PolicyEffect, lint_policies};
    let policies = vec![AbacPolicy {
        effect: PolicyEffect::Deny,
        service_identity: "svc-a".to_string(),
        tenant_id: "*".to_string(),
        purpose: "read".to_string(),
        message_type: "User".to_string(),
        operation: "Select".to_string(),
        required_scope: "udb:read".to_string(),
    }];
    let findings = lint_policies(&policies);
    assert!(findings.iter().any(|f| f.category == "broad_wildcard"));
}

#[test]
fn lint_policies_detects_shadowed_allow() {
    use udb::{AbacPolicy, PolicyEffect, lint_policies};
    let p = AbacPolicy {
        effect: PolicyEffect::Allow,
        service_identity: "svc-a".to_string(),
        tenant_id: "t1".to_string(),
        purpose: "read".to_string(),
        message_type: "User".to_string(),
        operation: "Select".to_string(),
        required_scope: "udb:read".to_string(),
    };
    let policies = vec![p.clone(), p];
    let findings = lint_policies(&policies);
    assert!(findings.iter().any(|f| f.category == "shadowed_policy"));
}
