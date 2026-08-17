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
fn crypto_provider_is_installed_for_tls_serving() {
    // Regression for bug_report.md: rustls 0.23 panics ("Could not automatically
    // determine the process-level CryptoProvider") when building any ServerConfig
    // while both aws-lc-rs and ring are linked and no provider is installed —
    // making mandatory-TLS production mode unbootable. install_default_crypto_provider()
    // must select one up front.
    install_default_crypto_provider();
    assert!(
        rustls::crypto::CryptoProvider::get_default().is_some(),
        "a default CryptoProvider must be installed before any TLS config is built"
    );
    // Building a ServerConfig must not panic now that a provider is selected.
    let _ = rustls::ServerConfig::builder().with_no_client_auth();
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
fn parse_args_recognizes_doctor_enterprise() {
    let args = vec!["doctor".to_string(), "--enterprise".to_string()];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::Doctor {
            enterprise: true,
            ..
        }
    ));
}

#[test]
fn parse_args_recognizes_doctor_fix() {
    let args = vec!["doctor".to_string(), "--fix".to_string()];
    let (command, proto_root, _, _) = parse_args(&args);
    assert!(matches!(command, Command::Doctor { fix: true, .. }));
    // `--fix` must be consumed as a flag, never leak into the positional proto root.
    assert_ne!(proto_root, "--fix");
}

#[test]
fn parse_args_recognizes_init_project() {
    let args = vec!["init-project".to_string()];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(command, Command::InitProject));
}

#[test]
fn parse_args_recognizes_init_plan_flags() {
    let args = vec![
        "init".to_string(),
        "--json-plan".to_string(),
        "--profile".to_string(),
        "dev".to_string(),
        "--framework".to_string(),
        "laravel".to_string(),
        "--backend".to_string(),
        "postgres,redis".to_string(),
        "--native-service".to_string(),
        "authn".to_string(),
        "--feature".to_string(),
        "vector".to_string(),
    ];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::Init(InitArgs {
            json_plan: true,
            ref profile,
            ref framework,
            ref backends,
            ref native_services,
            ref features,
            ..
        }) if profile.as_deref() == Some("dev")
            && framework.as_deref() == Some("laravel")
            && backends == &["postgres".to_string(), "redis".to_string()]
            && native_services == &["authn".to_string()]
            && features == &["vector".to_string()]
    ));
}

#[test]
fn parse_args_recognizes_init_revert_and_force() {
    let args = vec![
        "init".to_string(),
        "--revert".to_string(),
        "--force".to_string(),
    ];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::Init(InitArgs {
            revert: true,
            force: true,
            ..
        })
    ));
}

#[test]
fn parse_args_recognizes_sdk_init_language() {
    let args = vec![
        "sdk".to_string(),
        "init".to_string(),
        "--lang".to_string(),
        "php".to_string(),
    ];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::Sdk {
            action: SdkAction::Init,
            ref lang,
            ..
        } if lang == "php"
    ));
}

#[test]
fn parse_args_orm_scaffold_defaults_lang_all_no_entity() {
    // master-plan 10.5: `udb orm scaffold` — lang defaults to `all`, entity optional.
    let args = vec!["orm".to_string(), "scaffold".to_string()];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::Orm {
            action: OrmAction::Scaffold,
            ref lang,
            entity: None,
            ..
        } if lang == "all"
    ));
}

#[test]
fn parse_args_orm_scaffold_lang_required_value_and_optional_entity() {
    let args = vec![
        "orm".to_string(),
        "scaffold".to_string(),
        "--lang".to_string(),
        "typescript".to_string(),
        "--entity".to_string(),
        "myapp.v1.User".to_string(),
    ];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::Orm {
            action: OrmAction::Scaffold,
            ref lang,
            entity: Some(ref entity),
            ..
        } if lang == "typescript" && entity == "myapp.v1.User"
    ));
}

#[test]
fn parse_args_orm_unknown_action_is_invalid_usage() {
    let args = vec!["orm".to_string(), "frobnicate".to_string()];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(command, Command::InvalidUsage { .. }));
}

#[test]
fn parse_args_recognizes_dbops_sync_alias() {
    let args = vec![
        "dbops".to_string(),
        "sync".to_string(),
        "--force-bootstrap".to_string(),
        "--backend".to_string(),
        "all".to_string(),
    ];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::SyncMigrations {
            force_bootstrap: true,
            backend: Some(ref backend),
        } if backend == "all"
    ));
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
fn parse_args_unknown_top_level_command_is_invalid_usage() {
    let args = vec!["sevre".to_string()];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::InvalidUsage { ref message }
            if message.contains("unknown command 'sevre'")
                && message.contains("catalog")
                && message.contains("serve")
    ));
}

#[test]
fn parse_args_unknown_dev_action_is_invalid_usage() {
    let args = vec!["dev".to_string(), "rest".to_string()];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::InvalidUsage { ref message }
            if message.contains("unknown dev action 'rest'")
                && message.contains("up/start")
                && message.contains("smoke/test")
    ));
}

#[test]
fn parse_args_unknown_sdk_and_native_actions_are_invalid_usage() {
    let (command, _, _, _) = parse_args(&["sdk".to_string(), "wat".to_string()]);
    assert!(matches!(
        command,
        Command::InvalidUsage { ref message }
            if message.contains("unknown sdk action 'wat'")
                && message.contains("generate")
                && message.contains("manifest")
    ));

    let (command, _, _, _) = parse_args(&["native".to_string(), "potato".to_string()]);
    assert!(matches!(
        command,
        Command::InvalidUsage { ref message }
            if message.contains("unknown native action 'potato'")
                && message.contains("manifest")
                && message.contains("generate/gen")
    ));
}

#[test]
fn parse_args_unknown_command_suggests_nearest_match() {
    // A near-miss top-level command appends a "did you mean …?" hint while still
    // listing the known commands (so discoverability is preserved).
    let (command, _, _, _) = parse_args(&["sevre".to_string()]);
    assert!(matches!(
        command,
        Command::InvalidUsage { ref message }
            if message.contains("unknown command 'sevre'")
                && message.contains("did you mean 'serve'?")
                && message.contains("known:")
    ));

    // A near-miss dev action suggests the closest action keyword.
    let (command, _, _, _) = parse_args(&["dev".to_string(), "rest".to_string()]);
    assert!(matches!(
        command,
        Command::InvalidUsage { ref message }
            if message.contains("unknown dev action 'rest'")
                && message.contains("did you mean 'reset'?")
    ));
}

#[test]
fn parse_args_unknown_command_with_no_close_match_omits_suggestion() {
    // A token with no near match must NOT fabricate a misleading suggestion.
    let (command, _, _, _) = parse_args(&["zzzzzzzz".to_string()]);
    assert!(matches!(
        command,
        Command::InvalidUsage { ref message }
            if message.contains("unknown command 'zzzzzzzz'")
                && !message.contains("did you mean")
    ));
}

#[test]
fn project_file_lookup_walks_current_dir_parents() {
    let root = std::env::temp_dir().join(format!("udb-cli-project-file-{}", uuid::Uuid::new_v4()));
    let nested = root.join("apps").join("demo");
    std::fs::create_dir_all(&nested).expect("create nested temp dir");
    let compose = root.join("docker-compose.playground.yml");
    std::fs::write(&compose, "services: {}\n").expect("write compose marker");

    let found = find_existing_relative_path_from(
        nested,
        &[std::path::PathBuf::from("docker-compose.playground.yml")],
    )
    .expect("project file should be found in parent");

    assert_eq!(found, compose);
    let _ = std::fs::remove_dir_all(root);
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
fn parse_args_native_generate_lang_is_optional() {
    let (command, _, _, _) = parse_args(&["native".to_string(), "generate".to_string()]);
    assert!(matches!(
        command,
        Command::Native {
            action: NativeAction::Generate,
            lang: None,
            ..
        }
    ));

    let (command, _, _, _) = parse_args(&[
        "native".to_string(),
        "generate".to_string(),
        "--lang".to_string(),
        "typescript".to_string(),
    ]);
    assert!(matches!(
        command,
        Command::Native {
            action: NativeAction::Generate,
            lang: Some(ref lang),
            ..
        } if lang == "typescript"
    ));
}

#[test]
fn parse_args_recognizes_native_commands() {
    let (command, _, _, _) = parse_args(&["native".to_string()]);
    assert!(matches!(
        command,
        Command::Native {
            action: NativeAction::Manifest,
            ..
        }
    ));

    let (command, _, _, _) = parse_args(&["native".to_string(), "list".to_string()]);
    assert!(matches!(
        command,
        Command::Native {
            action: NativeAction::List,
            ..
        }
    ));

    let (command, _, _, _) = parse_args(&["native".to_string(), "lint".to_string()]);
    assert!(matches!(
        command,
        Command::Native {
            action: NativeAction::Lint,
            ..
        }
    ));
}

#[test]
fn parse_args_recognizes_native_lifecycle_and_app_init() {
    let (command, _, _, _) = parse_args(&[
        "native".to_string(),
        "add".to_string(),
        "auth".to_string(),
        "storage".to_string(),
        "--out".to_string(),
        "build".to_string(),
    ]);
    assert!(matches!(
        command,
        Command::Native {
            action: NativeAction::Add,
            ref services,
            ref out_dir,
            ..
        } if services == &["auth".to_string(), "storage".to_string()] && out_dir == "build"
    ));

    let (command, _, _, _) = parse_args(&[
        "app".to_string(),
        "init".to_string(),
        "--lang".to_string(),
        "python".to_string(),
        "--services".to_string(),
        "authn,authz".to_string(),
        "--out".to_string(),
        "app".to_string(),
    ]);
    assert!(matches!(
        command,
        Command::AppInit {
            ref lang,
            ref services,
            ref out_dir,
            ..
        } if lang == "python"
            && services == &["authn".to_string(), "authz".to_string()]
            && out_dir == "app"
    ));
}

#[test]
fn parse_args_recognizes_proto_export_fmt() {
    let args = vec![
        "proto".to_string(),
        "export".to_string(),
        "--out".to_string(),
        "vendor/proto".to_string(),
        "--fmt".to_string(),
    ];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::ProtoExport {
            out_dir,
            manage_buf_yaml: true,
            format_proto: true,
        } if out_dir == "vendor/proto"
    ));
}

#[test]
fn parse_args_recognizes_proto_fmt_check() {
    let args = vec![
        "proto".to_string(),
        "fmt".to_string(),
        "vendor/proto".to_string(),
        "--check".to_string(),
    ];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::ProtoFmt {
            root,
            check: true,
        } if root == "vendor/proto"
    ));
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
fn parse_args_recognizes_offline_platform_admin_bootstrap() {
    let args = vec![
        "auth".to_string(),
        "bootstrap".to_string(),
        "user".to_string(),
        "--username".to_string(),
        "platform-ci".to_string(),
        "--platform-admin".to_string(),
    ];
    let (command, _, _, _) = parse_args(&args);
    assert!(matches!(
        command,
        Command::Auth(AuthCommand::Bootstrap {
            username,
            platform_admin: true,
            ..
        }) if username == "platform-ci"
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
fn lint_policy_entity_coverage_flags_an_entity_no_rule_reaches() {
    use udb::{AuthzEffect, AuthzPolicy, lint_policy_entity_coverage};
    let covering = AuthzPolicy {
        effect: AuthzEffect::Allow,
        enabled: true,
        resource: "acme.crm.entity.v1.Contact".to_string(),
        ..AuthzPolicy::default()
    };
    let entities = vec![
        (
            "acme.crm.entity.v1.Contact".to_string(),
            "crm.contacts".to_string(),
        ),
        (
            "acme.crm.entity.v1.TripRoutePlan".to_string(),
            "location.trip_route_plans".to_string(),
        ),
    ];
    let findings = lint_policy_entity_coverage(std::slice::from_ref(&covering), &entities);
    assert_eq!(findings.len(), 1, "only the uncovered entity is reported");
    assert_eq!(findings[0].category, "entity_without_policy");
    assert!(findings[0].message.contains("TripRoutePlan"));

    // A wildcard resource covers everything — no findings.
    let wildcard = AuthzPolicy {
        effect: AuthzEffect::Allow,
        enabled: true,
        resource: "*".to_string(),
        ..AuthzPolicy::default()
    };
    assert!(lint_policy_entity_coverage(&[wildcard], &entities).is_empty());

    // A disabled policy covers nothing, but an empty enabled set defers to the
    // deny_by_default finding rather than reporting every entity.
    let disabled = AuthzPolicy {
        effect: AuthzEffect::Allow,
        enabled: false,
        resource: "acme.crm.entity.v1.Contact".to_string(),
        ..AuthzPolicy::default()
    };
    assert!(lint_policy_entity_coverage(&[disabled], &entities).is_empty());
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
    use udb::{AuthzEffect, AuthzPolicy, lint_policies};
    let policies = vec![AuthzPolicy {
        effect: AuthzEffect::Deny,
        subject: "svc-a".to_string(),
        tenant: "*".to_string(),
        purpose: "read".to_string(),
        resource: "User".to_string(),
        action: "Select".to_string(),
        required_scopes: vec!["udb:read".to_string()],
        ..Default::default()
    }];
    let findings = lint_policies(&policies);
    assert!(findings.iter().any(|f| f.category == "broad_wildcard"));
}

#[test]
fn lint_policies_detects_shadowed_allow() {
    use udb::{AuthzEffect, AuthzPolicy, lint_policies};
    let p = AuthzPolicy {
        effect: AuthzEffect::Allow,
        subject: "svc-a".to_string(),
        tenant: "t1".to_string(),
        purpose: "read".to_string(),
        resource: "User".to_string(),
        action: "Select".to_string(),
        required_scopes: vec!["udb:read".to_string()],
        ..Default::default()
    };
    let policies = vec![p.clone(), p];
    let findings = lint_policies(&policies);
    assert!(findings.iter().any(|f| f.category == "shadowed_policy"));
}

#[test]
fn policy_lint_loader_rejects_malformed_policy_file() {
    let root = std::env::temp_dir().join(format!("udb-cli-policy-lint-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).expect("create temp dir");
    let path = root.join("policy.json");
    std::fs::write(&path, "[{").expect("write malformed policy");

    let error = load_authz_policies_from_file(path.to_str().expect("utf-8 temp path"))
        .expect_err("malformed policy file should fail");
    assert!(error.contains("failed to parse authorization policy file"));

    let (result, exit_code) = build_policy_lint_cli_result(Err(error));
    assert_eq!(exit_code, 1);
    assert!(!result.passed);
    assert_eq!(result.policy_count, 0);
    assert!(result.error.is_some());

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn baseline_native_service_protos_have_endpoint_security_and_authz_metadata() {
    let mut checked = 0usize;
    let mut failures = Vec::new();
    for file in native_service_proto_files() {
        let text = std::fs::read_to_string(&file).expect("service proto should be readable");
        for (method, block) in rpc_blocks(&text) {
            checked += 1;
            let Some(security) = option_block(&block, "endpoint_security") else {
                failures.push(format!(
                    "{}:{method} missing endpoint_security",
                    file.display()
                ));
                continue;
            };
            let is_public = security.contains("mode: AUTH_MODE_PUBLIC");
            let has_authz = security.contains("scopes:")
                || security.contains("roles:")
                || security.contains("policy_ref:");
            if !is_public && !has_authz {
                failures.push(format!(
                    "{}:{method} non-public endpoint_security has no scopes/roles/policy_ref",
                    file.display()
                ));
            }
            if is_public && security.contains("tenant_required: true") {
                failures.push(format!(
                    "{}:{method} public RPC must not require tenant without an explicit allowlist",
                    file.display()
                ));
            }
        }
    }
    assert!(
        checked >= 119,
        "expected the native auth/control-plane service inventory to cover at least 119 RPCs, checked {checked}"
    );
    assert!(
        failures.is_empty(),
        "native endpoint_security baseline failures:\n{}",
        failures.join("\n")
    );
}

#[test]
fn baseline_generated_authn_authz_inventory_docs_are_present() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let rpc = std::fs::read_to_string(root.join("docs/generated/authn-authz-rpc-inventory.md"))
        .expect("RPC inventory doc must be generated for Phase E");
    // Counts pin the CURRENT generator output (`node
    // scripts/generate-authn-authz-inventory.mjs`); regenerate the docs and
    // update these together when the RPC/field surface grows.
    assert!(rpc.contains("- Native RPCs inventoried: 298"));
    assert!(rpc.contains("- RPCs without endpoint_security: 0"));
    assert!(rpc.contains("- WebRTC SignalingService.Signal endpoint security: present"));

    let fields =
        std::fs::read_to_string(root.join("docs/generated/authn-authz-sensitive-fields.md"))
            .expect("sensitive field inventory doc must be generated for Phase E");
    assert!(fields.contains("- Sensitive-looking or annotated fields inventoried: 239"));
    assert!(fields.contains("password_hash"));
    assert!(fields.contains("session_token_hash"));
    assert!(fields.contains("plain_key"));
    assert!(
        !fields.contains("MISSING explicit redaction annotation"),
        "every sensitive field row must be classified and tied to a baseline redaction rule"
    );
}

#[test]
fn baseline_cli_redacts_auth_secret_output_keys() {
    let mut value = serde_json::json!({
        "access_token": "access",
        "refresh_token": "refresh",
        "session_token": "session",
        "csrf_token": "csrf",
        "key_hash": "hash",
        "password_hash": "password",
        "encrypted_private_material": "private-key",
        "client_secret": "client-secret",
        "totp_secret": "totp",
        "totp_qr_uri": "otpauth://secret",
        "otp_id": "otp",
        "mfa_otp_id": "mfa",
        "operation_id": "operation",
        "nested": {
            "api_key": "nested-api-key",
            "plain_key": "nested-plain-key"
        },
        "reveal": {
            "secret_revealed": true,
            "plain_key": "one-time-secret",
            "api_key": "one-time-api-key",
            "key_hash": "still-secret"
        }
    });
    redact_sensitive_json(&mut value);

    for key in [
        "access_token",
        "refresh_token",
        "session_token",
        "csrf_token",
        "key_hash",
        "password_hash",
        "encrypted_private_material",
        "client_secret",
        "totp_secret",
        "totp_qr_uri",
        "otp_id",
        "mfa_otp_id",
        "operation_id",
    ] {
        assert_eq!(value[key], "[REDACTED]", "{key} should be redacted");
    }
    assert_eq!(value["nested"]["api_key"], "[REDACTED]");
    assert_eq!(value["nested"]["plain_key"], "[REDACTED]");
    assert_eq!(value["reveal"]["plain_key"], "one-time-secret");
    assert_eq!(value["reveal"]["api_key"], "one-time-api-key");
    assert_eq!(value["reveal"]["key_hash"], "[REDACTED]");
}

#[test]
fn cli_redaction_covers_descriptor_sensitive_output_fields() {
    let manifest = udb::runtime::descriptor_manifest::descriptor_contract_manifest_static();
    let mut map = serde_json::Map::new();
    for field in manifest.messages.iter().flat_map(|message| &message.fields) {
        let scalar = &field.scalar_security;
        let column = field.db_column_security.as_ref();
        if scalar.sensitive
            || scalar.encrypted_security
            || scalar.log_redacted
            || column.is_some_and(|security| {
                matches!(security.output_view, 1 | 6)
                    || matches!(security.redaction_strategy, 3 | 4)
            })
        {
            map.insert(
                field.name.clone(),
                serde_json::Value::String("secret".into()),
            );
        }
    }
    assert!(
        !map.is_empty(),
        "descriptor must expose sensitive output fields for redaction coverage"
    );

    let mut value = serde_json::Value::Object(map);
    redact_sensitive_json(&mut value);
    let object = value.as_object().expect("redacted object");
    for (key, value) in object {
        assert_eq!(
            value, "[REDACTED]",
            "descriptor-sensitive CLI field {key} should be redacted"
        );
    }
}

#[test]
fn generated_native_contract_json_matches_embedded_descriptor() {
    let manifest = udb::runtime::descriptor_manifest::descriptor_contract_manifest_static();
    let rendered = native_manifest_json(manifest);
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let committed: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(root.join("docs/generated/udb-native-contract.json"))
            .expect("generated native contract JSON should be readable"),
    )
    .expect("generated native contract JSON should parse");

    assert_eq!(
        committed, rendered,
        "docs/generated/udb-native-contract.json is stale; run `udb native manifest` (the JSON it prints is this file's content) and commit the descriptor-derived output"
    );

    for rpc in rendered["services"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|service| service["rpcs"].as_array().into_iter().flatten())
    {
        assert!(
            rpc.get("operation_kind").is_some(),
            "contract JSON RPC node missing operation_kind: {rpc:#}"
        );
        assert!(
            rpc.get("read_only").is_some(),
            "contract JSON RPC node missing read_only: {rpc:#}"
        );
        assert!(
            rpc.get("rest_operation_id").is_some(),
            "contract JSON RPC node missing rest_operation_id: {rpc:#}"
        );
    }
}

#[test]
fn native_contract_lint_does_not_require_data_broker_endpoint_security() {
    let manifest = udb::runtime::descriptor_manifest::descriptor_contract_manifest_static();
    let findings = native_contract_findings(manifest);
    let data_broker_endpoint_errors: Vec<&serde_json::Value> = findings
        .iter()
        .filter(|finding| {
            finding.get("severity").and_then(|value| value.as_str()) == Some("error")
                && finding.get("kind").and_then(|value| value.as_str())
                    == Some("endpoint_security_missing")
                && finding
                    .get("rpc")
                    .and_then(|value| value.as_str())
                    .is_some_and(|rpc| rpc.starts_with("/udb.services.v1.DataBroker/"))
        })
        .collect();

    assert!(
        data_broker_endpoint_errors.is_empty(),
        "native lint must not require endpoint_security on the public DataBroker facade: {data_broker_endpoint_errors:#?}"
    );
}

#[test]
fn generated_native_docs_match_embedded_descriptor() {
    let manifest = udb::runtime::descriptor_manifest::descriptor_contract_manifest_static();
    let rendered = native_docs_markdown(manifest);
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let committed = std::fs::read_to_string(root.join("docs/generated/native-services.md"))
        .expect("generated native-service docs should be readable")
        .replace("\r\n", "\n");

    assert_eq!(
        committed, rendered,
        "docs/generated/native-services.md is stale; run `udb native docs` and commit the descriptor-derived output"
    );
}

/// Render the README "Current Surface" generated block straight from the
/// embedded descriptor. This is the single source of truth for the
/// hand-prone "N services, N RPCs" surface counts: the data-plane DataBroker
/// RPC total, the native-control-plane service + RPC totals, and the
/// per-service RPC breakdown. Kept byte-identical to the block fenced by the
/// `<!-- BEGIN/END GENERATED:services -->` markers in `README.md` so the table
/// can never silently drift from the descriptor.
fn readme_services_block(
    manifest: &udb::runtime::descriptor_manifest::DescriptorContractManifest,
) -> String {
    // Data plane: every RPC on the public DataBroker facade.
    let data_plane_rpcs = manifest
        .services
        .iter()
        .find(|service| service.full_name() == "udb.services.v1.DataBroker")
        .map(|service| service.methods.len())
        .expect("descriptor must expose the udb.services.v1.DataBroker facade");

    // Native control plane: services carrying native_service options, keyed by
    // canonical service id (the same id used by the generated docs/registry).
    let mut native: Vec<(String, usize)> = manifest
        .services
        .iter()
        .filter_map(|service| {
            service.native_service.as_ref().map(|n| {
                (
                    udb::runtime::service::native_registry::canonical_service_id(&n.service_id),
                    service.methods.len(),
                )
            })
        })
        .collect();
    native.sort_by(|a, b| a.0.cmp(&b.0));
    let native_service_count = native.len();
    let native_rpc_count: usize = native.iter().map(|(_, count)| count).sum();

    let mut out = String::new();
    out.push_str("| Area | Surface |\n");
    out.push_str("|---|---|\n");
    out.push_str(&format!(
        "| Data plane | {data_plane_rpcs} `DataBroker` RPCs |\n"
    ));
    out.push_str(&format!(
        "| Native control plane | {native_service_count} services, {native_rpc_count} RPCs |\n"
    ));
    out.push('\n');
    out.push_str("Per-service RPC counts (native control plane):\n");
    out.push('\n');
    out.push_str("| Service | RPCs |\n");
    out.push_str("|---|---|\n");
    for (id, count) in &native {
        out.push_str(&format!("| `{id}` | {count} |\n"));
    }
    out
}

#[test]
fn readme_services_block_matches_embedded_descriptor() {
    let manifest = udb::runtime::descriptor_manifest::descriptor_contract_manifest_static();
    let rendered = readme_services_block(manifest);

    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let readme = std::fs::read_to_string(root.join("README.md"))
        .expect("README.md should be readable")
        .replace("\r\n", "\n");

    const BEGIN: &str = "<!-- BEGIN GENERATED:services -->";
    const END: &str = "<!-- END GENERATED:services -->";
    let begin = readme
        .find(BEGIN)
        .expect("README.md must contain the <!-- BEGIN GENERATED:services --> marker");
    let end = readme
        .find(END)
        .expect("README.md must contain the <!-- END GENERATED:services --> marker");
    assert!(
        begin < end,
        "README.md BEGIN GENERATED:services marker must precede END"
    );
    // Block is the text strictly between the marker lines: drop the newline that
    // follows BEGIN and the newline that precedes END so the comparison is on
    // the rendered rows alone.
    let block = readme[begin + BEGIN.len()..end]
        .trim_matches('\n')
        .to_string();

    assert_eq!(
        block,
        rendered.trim_end_matches('\n'),
        "README.md `Current Surface` generated block is stale; run `udb native docs` and replace the text between the <!-- BEGIN/END GENERATED:services --> markers with the descriptor-derived output"
    );
}

#[test]
fn descriptor_native_services_reach_registry_health_sdk_and_docs() {
    let manifest = udb::runtime::descriptor_manifest::descriptor_contract_manifest_static();
    let descriptor_ids: std::collections::BTreeSet<String> = manifest
        .services
        .iter()
        .filter_map(|service| service.native_service.as_ref())
        .map(|native| {
            udb::runtime::service::native_registry::canonical_service_id(&native.service_id)
        })
        .collect();
    assert!(
        !descriptor_ids.is_empty(),
        "descriptor must expose native services for propagation checks"
    );

    let registry_ids: std::collections::BTreeSet<String> =
        udb::runtime::service::native_registry::native_service_registry()
            .into_iter()
            .map(|entry| entry.service_id)
            .collect();
    let health_ids: std::collections::BTreeSet<String> =
        udb::runtime::service::native_registry::resolved_native_service_statuses(
            &udb::runtime::config::UdbConfig::default(),
        )
        .into_iter()
        .map(|status| status.service_id)
        .collect();
    let sdk_ids: std::collections::BTreeSet<String> = udb::runtime::sdk_manifest::rpc_manifest()
        .into_iter()
        .filter_map(|rpc| {
            (!rpc.native_service_id.trim().is_empty()).then_some(rpc.native_service_id)
        })
        .collect();

    let docs = native_docs_markdown(&manifest);
    let generated_json = native_manifest_json(&manifest);
    let json_ids: std::collections::BTreeSet<String> = generated_json["services"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|service| service.get("native_service"))
        .filter_map(|native| native.get("service_id").and_then(|id| id.as_str()))
        .map(udb::runtime::service::native_registry::canonical_service_id)
        .collect();

    assert_eq!(
        registry_ids, descriptor_ids,
        "registry drifted from descriptor"
    );
    assert_eq!(
        health_ids, descriptor_ids,
        "health/status drifted from descriptor"
    );
    assert_eq!(
        json_ids, descriptor_ids,
        "contract JSON drifted from descriptor"
    );
    assert!(
        descriptor_ids.is_subset(&sdk_ids),
        "SDK manifest must include every descriptor-native service id; missing: {:?}",
        descriptor_ids.difference(&sdk_ids).collect::<Vec<_>>()
    );
    for service_id in descriptor_ids {
        assert!(
            docs.contains(&format!("| `{service_id}` |")),
            "generated native docs must include descriptor-native service `{service_id}`"
        );
    }
}

#[test]
fn public_capability_wire_contract_does_not_advertise_backend_rls() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let admin_proto = std::fs::read_to_string(root.join("proto/udb/entity/v1/admin.proto"))
        .expect("admin proto should be readable");
    let start = admin_proto
        .find("message BackendCapabilityDescriptor")
        .expect("BackendCapabilityDescriptor must exist");
    let block = &admin_proto[start
        ..admin_proto[start..]
            .find("\n}")
            .map(|idx| start + idx)
            .expect("BackendCapabilityDescriptor block must close")];

    assert!(
        !block.to_ascii_lowercase().contains("rls"),
        "public capability/SDK wire descriptor must not expose a broad supports_rls claim; native table RLS is descriptor/table security, not a backend-wide promise"
    );

    let public_matrix = serde_json::to_string(&udb::backend::capability_matrix())
        .expect("backend capability matrix should serialize");
    assert!(
        !public_matrix.to_ascii_lowercase().contains("supports_rls"),
        "doctor/health capability matrix must not serialize a broad supports_rls claim"
    );
}

#[test]
fn compat_matrix_entries_round_trip_through_parser_option_kind() {
    let matrix = build_compat_matrix();
    assert!(
        !matrix.is_empty(),
        "compat-matrix should document parser-backed options"
    );

    for entry in matrix {
        assert!(
            !entry.option_name.starts_with("db."),
            "compat-matrix must not advertise unrecognized db.* option `{}`",
            entry.option_name
        );
        let parsed = udb::parser::parser_option_kind_name(entry.option_name, "");
        assert_eq!(
            parsed,
            Some(entry.option_kind),
            "compat-matrix option `{}` should resolve through parser option_kind",
            entry.option_name
        );
        assert!(
            !entry.accepted_keys.is_empty(),
            "compat-matrix option `{}` should document accepted parser keys",
            entry.option_name
        );
    }
}

#[test]
fn baseline_authn_proto_hash_algorithm_comments_match_runtime_contract() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let user = std::fs::read_to_string(root.join("proto/udb/core/authn/entity/v1/user.proto"))
        .expect("user proto should be readable");
    let session =
        std::fs::read_to_string(root.join("proto/udb/core/authn/entity/v1/session.proto"))
            .expect("session proto should be readable");

    assert!(user.contains("Argon2id PHC"));
    assert!(user.contains("hashing_algorithm: \"argon2id\""));
    assert!(
        !user.to_ascii_lowercase().contains("bcrypt"),
        "authn proto docs must not drift back to bcrypt terminology"
    );
    assert!(session.contains("Keyed HMAC"));
    assert!(session.contains("hashing_algorithm: \"hmac-sha256\""));
}

fn native_service_proto_files() -> Vec<std::path::PathBuf> {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto/udb/core");
    let mut out = Vec::new();
    collect_service_proto_files(&root, &mut out);
    out.sort();
    out
}

fn collect_service_proto_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("proto directory should be readable") {
        let entry = entry.expect("proto directory entry should be readable");
        let path = entry.path();
        if path.is_dir() {
            collect_service_proto_files(&path, out);
        } else if path.file_name().and_then(|name| name.to_str()) == Some("_service.proto")
            || path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with("_service.proto"))
        {
            out.push(path);
        }
    }
}

fn rpc_blocks(text: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut search_at = 0usize;
    while let Some(relative) = text[search_at..].find("rpc ") {
        let rpc_at = search_at + relative;
        if !is_token_boundary(text, rpc_at) {
            search_at = rpc_at + 4;
            continue;
        }
        let name_start = rpc_at + 4;
        let name_end = text[name_start..]
            .find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
            .map(|idx| name_start + idx)
            .unwrap_or(text.len());
        let Some(open_rel) = text[name_end..].find('{') else {
            break;
        };
        let open = name_end + open_rel;
        let Some(close) = matching_brace(text, open) else {
            break;
        };
        out.push((
            text[name_start..name_end].to_string(),
            text[open + 1..close].to_string(),
        ));
        search_at = close + 1;
    }
    out
}

fn option_block(block: &str, option_name: &str) -> Option<String> {
    let needle = format!("option (udb.core.common.v1.{option_name})");
    let start = block.find(&needle)?;
    let open = block[start..].find('{').map(|idx| start + idx)?;
    let close = matching_brace(block, open)?;
    Some(block[open + 1..close].to_string())
}

fn matching_brace(text: &str, open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (idx, ch) in text.char_indices().skip_while(|(idx, _)| *idx < open) {
        if ch == '{' {
            depth += 1;
        } else if ch == '}' {
            depth = depth.checked_sub(1)?;
            if depth == 0 {
                return Some(idx);
            }
        }
    }
    None
}

fn is_token_boundary(text: &str, at: usize) -> bool {
    text[..at]
        .chars()
        .last()
        .is_none_or(|ch| !(ch.is_ascii_alphanumeric() || ch == '_'))
}

#[test]
fn broken_pipe_panic_messages_map_to_clean_exit() {
    // std's `println!` / `print!` panic with these messages when the downstream
    // reader closed the pipe early (`udb … | head`). Both platform spellings
    // must be recognized so `install_broken_pipe_panic_hook` exits cleanly (0)
    // instead of dumping a backtrace. Reverting the detector to miss either
    // spelling fails this test.
    assert!(super::is_broken_pipe_panic_message(
        "failed printing to stdout: Broken pipe (os error 32)" // Unix EPIPE
    ));
    assert!(super::is_broken_pipe_panic_message(
        "failed printing to stdout: The pipe is being closed. (os error 109)" // Windows
    ));
    assert!(super::is_broken_pipe_panic_message("Broken pipe"));
    // Unrelated panics must NOT be swallowed as a clean exit — they still get
    // the normal (previous) panic hook and a non-zero exit.
    assert!(!super::is_broken_pipe_panic_message(
        "index out of bounds: the len is 3 but the index is 5"
    ));
    assert!(!super::is_broken_pipe_panic_message(
        "called `Option::unwrap()` on a `None` value"
    ));
}
