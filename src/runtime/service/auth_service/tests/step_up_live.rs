//! 0.6.6 STEPUP live proofs: localized auth templates (`en` and `bn` coexist,
//! codes render in the user's locale with exact → language → `en` fallback)
//! and the OTP step-up flow end to end (IssueMfaChallenge with an action →
//! VerifyMfaChallenge mints a step-up token → CheckStepUp enforces the
//! tenant rule).

use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::udb::core::notification::entity::v1 as notif_entity_pb;
use crate::proto::udb::core::notification::services::v1 as notif_pb;
use crate::proto::udb::core::notification::services::v1::notification_service_server::NotificationService;
use crate::runtime::service::method_security::{
    VerifiedClaimContext, scope_claim_context_for_test, test_claim_context,
};
use tonic::Request;

/// Set the user's notification locale the way an operator does today (the
/// `users.locale` column).
async fn set_user_locale(pool: &sqlx::PgPool, user_id: &str, locale: &str) {
    let m = crate::runtime::native_catalog::native_model(
        "udb.core.authn.entity.v1.User",
        &["user_id", "locale"],
    );
    sqlx::query(&format!(
        "UPDATE {rel} SET {locale} = $1 WHERE {id} = $2::UUID",
        rel = m.relation,
        locale = m.q("locale"),
        id = m.q("user_id"),
    ))
    .bind(locale)
    .bind(user_id)
    .execute(pool)
    .await
    .expect("set user locale");
}

/// The queued (not yet delivered) body of the `event_type` notification to
/// `user_id` that carries `code`.
async fn queued_body_with_code(
    pool: &sqlx::PgPool,
    user_id: &str,
    event_type: &str,
    code: &str,
) -> String {
    let m = crate::runtime::native_catalog::native_model(
        "udb.core.notification.entity.v1.NotificationLog",
        &["recipient_id", "event_type", "rendered_body"],
    );
    sqlx::query_scalar(&format!(
        "SELECT {body} FROM {rel} WHERE {recipient}::TEXT = $1 AND {event}::TEXT = $2 \
         AND {body} LIKE $3",
        body = m.q("rendered_body"),
        rel = m.relation,
        recipient = m.q("recipient_id"),
        event = m.q("event_type"),
    ))
    .bind(user_id)
    .bind(event_type)
    .bind(format!("%{code}%"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|err| panic!("queued {event_type} body carrying the code: {err}"))
}

fn tenant_context(
    tenant_id: &str,
    project_id: &str,
) -> Option<crate::proto::udb::core::common::v1::RequestContext> {
    Some(crate::proto::udb::core::common::v1::RequestContext {
        tenant: Some(crate::proto::udb::core::common::v1::TenantContext {
            tenant_id: tenant_id.to_string(),
            project_id: project_id.to_string(),
            ..Default::default()
        }),
        ..Default::default()
    })
}

#[tokio::test]
#[ignore = "requires live Postgres; CI runs every ignored lib test"]
async fn en_and_bn_templates_coexist_and_codes_render_in_the_users_locale() {
    use crate::runtime::service::live_tests::ops_seams_live::EnvRestore;
    let _guard = live_auth_db_lock().lock().await;
    // A configured webhook would take the codes instead of NotificationService.
    let _no_webhook = EnvRestore::set("UDB_OTP_DELIVERY_WEBHOOK_URL", "");
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let notifier = std::sync::Arc::new(notification_service(pool.clone()).await);
    let authn = authn_service(pool.clone()).with_system_notifier(notifier.clone());
    let tenant = uuid::Uuid::new_v4().to_string();
    let user =
        create_verified_user_in(&authn, "locale", "CorrectHorse1!", &tenant, "default").await;

    // The same event + channel in two locales: two rows, neither overwrites
    // the other; re-upserting `en` updates the `en` row in place.
    for (locale, marker) in [("en", "EN-OLD"), ("bn", "BN"), ("en", "EN")] {
        let upserted = notifier
            .upsert_template(Request::new(notif_pb::UpsertTemplateRequest {
                event_type: "authn.otp".into(),
                channel: notif_entity_pb::NotificationChannel::Email as i32,
                subject_template: format!("{marker} subject {{{{code}}}}"),
                body_template: format!("{marker} body {{{{code}}}} {{{{user_name}}}}"),
                locale: locale.into(),
                context: tenant_context(&user.tenant_id, &user.project_id),
                is_active: true,
                ..Default::default()
            }))
            .await
            .unwrap_or_else(|err| panic!("upsert {locale} template: {err}"))
            .into_inner()
            .template
            .expect("upserted template");
        assert_eq!(upserted.locale, locale);
    }
    let template_model = crate::runtime::native_catalog::native_model(
        "udb.core.notification.entity.v1.NotificationTemplate",
        &["tenant_id", "event_type", "locale", "body_template"],
    );
    let rows: Vec<(String, String)> = sqlx::query_as(&format!(
        "SELECT {locale}::TEXT, {body}::TEXT FROM {rel} \
         WHERE {tenant}::TEXT = $1 AND {event}::TEXT = 'authn.otp' ORDER BY {locale}",
        locale = template_model.q("locale"),
        body = template_model.q("body_template"),
        rel = template_model.relation,
        tenant = template_model.q("tenant_id"),
        event = template_model.q("event_type"),
    ))
    .bind(&user.tenant_id)
    .fetch_all(&pool)
    .await
    .expect("stored templates");
    assert_eq!(rows.len(), 2, "en and bn coexist: {rows:?}");
    assert_eq!(rows[0].0, "bn");
    assert!(rows[0].1.starts_with("BN body"));
    assert_eq!(rows[1].0, "en");
    assert!(
        rows[1].1.starts_with("EN body"),
        "re-upserting en updated the en row in place"
    );

    let (authn_ref, user_id) = (&authn, user.user_id.as_str());
    let send_code = move || async move {
        let sent = authn_ref
            .send_otp(Request::new(authn_pb::SendOtpRequest {
                user_id: user_id.to_string(),
                otp_type: authn_entity_pb::OtpType::SensitiveOperation as i32,
                ..Default::default()
            }))
            .await
            .expect("send OTP")
            .into_inner();
        issued_test_otp_code(&sent.otp_id)
    };

    // bn-BD has no exact template: its language `bn` wins over `en`.
    set_user_locale(&pool, &user.user_id, "bn-BD").await;
    let code = send_code().await;
    let body = queued_body_with_code(&pool, &user.user_id, "authn.otp", &code).await;
    assert!(
        body.starts_with("BN body"),
        "bn-BD renders the bn template: {body}"
    );

    // A language with no template falls back to `en`.
    set_user_locale(&pool, &user.user_id, "fr").await;
    let code = send_code().await;
    let body = queued_body_with_code(&pool, &user.user_id, "authn.otp", &code).await;
    assert!(body.starts_with("EN body"), "fr falls back to en: {body}");

    // No stored password-reset template: a bn user gets the shipped Bangla one.
    set_user_locale(&pool, &user.user_id, "bn").await;
    let reset = authn
        .forgot_password(Request::new(authn_pb::ForgotPasswordRequest {
            identifier: user.email.clone(),
            ..Default::default()
        }))
        .await
        .expect("forgot password")
        .into_inner();
    let code = issued_test_otp_code(&reset.otp_id);
    let body = queued_body_with_code(&pool, &user.user_id, "authn.password_reset", &code).await;
    assert!(
        body.contains("পাসওয়ার্ড") && !body.contains("{{"),
        "shipped Bangla reset template, fully rendered: {body}"
    );
    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; CI runs every ignored lib test"]
async fn otp_step_up_mints_a_token_check_step_up_enforces_the_tenant_rule() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service_with_jwt(pool.clone());
    let tenant = uuid::Uuid::new_v4().to_string();
    let user = create_verified_user_in(&svc, "stepup", "CorrectHorse1!", &tenant, "default").await;
    svc.put_tenant_auth_policy(Request::new(authn_pb::PutTenantAuthPolicyRequest {
        tenant_id: tenant.clone(),
        step_up_rules_json: r#"[
            {"action":"payments.approve","max_age_seconds":120,"factors":["EMAIL_OTP","TOTP"]},
            {"action":"users.delete","max_age_seconds":60,"factors":["WEBAUTHN"]}
        ]"#
        .to_string(),
        ..Default::default()
    }))
    .await
    .expect("tenant step-up rules");

    // A password login without MFA: aal1, amr ["pwd"], auth_time stamped.
    let login = svc
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            device_name: "step-up".to_string(),
            ..Default::default()
        }))
        .await
        .expect("login")
        .into_inner();
    let security = crate::runtime::security::SecurityConfig {
        jwt_private_key: Some(include_str!("../../../testdata/jwt_rs256_private.pem").to_string()),
        jwt_public_key: Some(include_str!("../../../testdata/jwt_rs256_public.pem").to_string()),
        ..crate::runtime::security::SecurityConfig::default()
    };
    let (login_claims, login_assurance) =
        crate::runtime::security::validate_bearer_token_with_assurance(
            &security,
            &login.access_token,
        )
        .expect("login access token");
    assert_eq!(login_assurance.acr, "aal1");
    assert_eq!(login_assurance.amr, ["pwd"]);
    assert!(login_assurance.auth_time > 0);
    let validated_login = svc
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: login.access_token.clone(),
            token_type: authn_entity_pb::TokenType::JwtAccess as i32,
        }))
        .await
        .expect("validate login token")
        .into_inner();
    assert!(validated_login.valid);
    assert_eq!(validated_login.amr, ["pwd"]);
    assert_eq!(validated_login.auth_time, login_assurance.auth_time);
    assert!(validated_login.step_up_action.is_empty());

    // The caller is the user, on the session the login opened.
    let session_jti = login_claims.jti.clone().expect("session-bound jti");
    let as_user = || VerifiedClaimContext {
        credential_id: session_jti.clone(),
        ..test_claim_context(&user.user_id, &tenant, "default", &[], &[])
    };

    // The rule for users.delete accepts only WEBAUTHN: an email code is
    // refused before anything is sent.
    let refused = scope_claim_context_for_test(
        as_user(),
        svc.issue_mfa_challenge(Request::new(authn_pb::IssueMfaChallengeRequest {
            user_id: user.user_id.clone(),
            factor_kind: authn_entity_pb::AuthFactorKind::EmailOtp as i32,
            action: "users.delete".to_string(),
            ..Default::default()
        })),
    )
    .await
    .expect_err("factor outside the rule");
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);

    let issued = scope_claim_context_for_test(
        as_user(),
        svc.issue_mfa_challenge(Request::new(authn_pb::IssueMfaChallengeRequest {
            user_id: user.user_id.clone(),
            factor_kind: authn_entity_pb::AuthFactorKind::EmailOtp as i32,
            action: "payments.approve".to_string(),
            ..Default::default()
        })),
    )
    .await
    .expect("issue EMAIL_OTP step-up challenge")
    .into_inner();
    assert!(issued.otp_destination_masked.contains("***@"));
    let challenge_model = crate::runtime::native_catalog::native_model(
        "udb.core.authn.entity.v1.MfaChallenge",
        &["challenge_id", "otp_id", "action"],
    );
    let (otp_id, stored_action): (String, String) = sqlx::query_as(&format!(
        "SELECT {otp}::TEXT, {action}::TEXT FROM {rel} WHERE {id} = $1::UUID",
        otp = challenge_model.q("otp_id"),
        action = challenge_model.q("action"),
        rel = challenge_model.relation,
        id = challenge_model.q("challenge_id"),
    ))
    .bind(&issued.challenge_id)
    .fetch_one(&pool)
    .await
    .expect("stored challenge");
    assert_eq!(stored_action, "payments.approve");
    let code = issued_test_otp_code(&otp_id);

    let verified = scope_claim_context_for_test(
        as_user(),
        svc.verify_mfa_challenge(Request::new(authn_pb::VerifyMfaChallengeRequest {
            challenge_id: issued.challenge_id.clone(),
            code: code.clone(),
            ..Default::default()
        })),
    )
    .await
    .expect("verify step-up challenge")
    .into_inner();
    assert!(verified.verified);
    assert_eq!(verified.user_id, user.user_id);
    assert!(!verified.step_up_token.is_empty(), "step-up token minted");
    assert_eq!(verified.acr, "aal2");
    assert_eq!(verified.amr, ["otp"]);
    let now = now_unix() as i64;
    assert!(
        (now + 110..=now + 125).contains(&verified.step_up_expires_at_unix),
        "token lifetime is the rule's max_age_seconds"
    );
    let step_up_token = verified.step_up_token.clone();

    // A step-up token is never an access token.
    assert!(crate::runtime::security::validate_bearer_token(&security, &step_up_token).is_err());

    // The challenge is single use.
    let replay = scope_claim_context_for_test(
        as_user(),
        svc.verify_mfa_challenge(Request::new(authn_pb::VerifyMfaChallengeRequest {
            challenge_id: issued.challenge_id.clone(),
            code,
            ..Default::default()
        })),
    )
    .await
    .expect("replayed verify answers")
    .into_inner();
    assert!(!replay.verified && replay.step_up_token.is_empty());

    let svc_ref = &svc;
    let check = move |action: &'static str, token: String, caller: VerifiedClaimContext| async move {
        scope_claim_context_for_test(
            caller,
            svc_ref.check_step_up(Request::new(authn_pb::CheckStepUpRequest {
                action: action.to_string(),
                step_up_token: token,
                ..Default::default()
            })),
        )
        .await
        .expect("check step-up answers")
        .into_inner()
    };

    // Within max_age, same action, allowed factor, same user: satisfied.
    let ok = check("payments.approve", step_up_token.clone(), as_user()).await;
    assert!(ok.satisfied, "{ok:?}");
    assert_eq!(ok.reason, "satisfied");
    assert_eq!(ok.user_id, user.user_id);
    assert_eq!(ok.max_age_seconds, 120);
    assert_eq!(
        ok.factor_kind,
        authn_entity_pb::AuthFactorKind::EmailOtp as i32
    );
    assert_eq!(ok.amr, ["otp"]);

    // Another action: refused.
    let other_action = check("users.delete", step_up_token.clone(), as_user()).await;
    assert!(!other_action.satisfied);
    assert_eq!(other_action.reason, "action_mismatch");

    // Another person presenting the token: refused.
    let stranger = check(
        "payments.approve",
        step_up_token.clone(),
        test_claim_context(
            &uuid::Uuid::new_v4().to_string(),
            &tenant,
            "default",
            &[],
            &[],
        ),
    )
    .await;
    assert_eq!(stranger.reason, "subject_mismatch");

    // Proven longer ago than the rule's max_age: refused.
    let (stale_token, _) = crate::runtime::security::sign_step_up_token(
        &security,
        crate::runtime::security::StepUpGrant {
            subject: &user.user_id,
            tenant_id: &tenant,
            project_id: "default",
            jti: &session_jti,
            action: "payments.approve",
            factor: "EMAIL_OTP",
        },
        now_unix() - 200,
        1_000,
    )
    .expect("sign stale step-up token")
    .expect("key configured");
    let stale = check("payments.approve", stale_token, as_user()).await;
    assert_eq!(stale.reason, "too_old");

    // Garbage is an answer, not an error.
    let garbage = check("payments.approve", "not-a-token".to_string(), as_user()).await;
    assert_eq!(garbage.reason, "invalid_token");

    // ValidateToken reports the step-up claims.
    let validated = svc
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: step_up_token.clone(),
            token_type: authn_entity_pb::TokenType::JwtAccess as i32,
        }))
        .await
        .expect("validate step-up token")
        .into_inner();
    assert!(validated.valid);
    assert_eq!(validated.step_up_action, "payments.approve");
    assert_eq!(validated.acr, "aal2");
    assert!(validated.scopes.is_empty());

    // Logging out ends the session the step-up token is bound to.
    svc.logout(Request::new(authn_pb::LogoutRequest {
        session_id: login.session_id.clone(),
        ..Default::default()
    }))
    .await
    .expect("logout");
    let after_logout = check("payments.approve", step_up_token, as_user()).await;
    assert_eq!(after_logout.reason, "invalid_token");
    cleanup_native_auth_db(&pool).await;
}
