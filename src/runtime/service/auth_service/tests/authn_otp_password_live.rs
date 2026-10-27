use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
use sqlx::Connection;
use tonic::Request;
use uuid::Uuid;

fn decode_detail(status: &tonic::Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("error-detail trailer present")
        .to_bytes()
        .expect("trailer decodes to bytes");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authn_otp_password_lifecycle -- --ignored --nocapture"]
async fn live_postgres_authn_otp_password_lifecycle() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service(pool.clone());
    let user = create_verified_user(&svc, "password", "CorrectHorse1!").await;

    let sent = svc
        .send_otp(Request::new(authn_pb::SendOtpRequest {
            user_id: user.user_id.clone(),
            otp_type: authn_entity_pb::OtpType::SensitiveOperation as i32,
            correlation_id: "password-change".to_string(),
            ..Default::default()
        }))
        .await
        .expect("send sensitive-operation OTP")
        .into_inner();
    let wrong = svc
        .verify_otp(Request::new(authn_pb::VerifyOtpRequest {
            otp_id: sent.otp_id.clone(),
            code: "000000".to_string(),
        }))
        .await
        .expect("wrong OTP check")
        .into_inner();
    assert!(!wrong.verified);
    let verified = verify_issued_otp(&svc, &sent.otp_id).await;
    assert!(verified.verified);

    let changed = svc
        .change_password(Request::new(authn_pb::ChangePasswordRequest {
            user_id: user.user_id.clone(),
            current_password: "CorrectHorse1!".to_string(),
            new_password: "NewCorrectHorse1!".to_string(),
            otp_id: sent.otp_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("change password with verified OTP")
        .into_inner();
    assert_eq!(changed.user_id, user.user_id);

    let reused = svc
        .verify_otp(Request::new(authn_pb::VerifyOtpRequest {
            otp_id: sent.otp_id.clone(),
            code: issued_test_otp_code(&sent.otp_id),
        }))
        .await
        .expect("used OTP check")
        .into_inner();
    assert!(!reused.verified);

    let old_login = svc
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("old password must fail");
    assert_eq!(old_login.code(), tonic::Code::Unauthenticated);

    let new_login = svc
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email,
            password: "NewCorrectHorse1!".to_string(),
            device_name: "password-lifecycle".to_string(),
            ..Default::default()
        }))
        .await
        .expect("new password login")
        .into_inner();
    assert_eq!(new_login.user_id, user.user_id);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authn_otp_cooldown -- --ignored --nocapture"]
async fn live_postgres_authn_otp_cooldown() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service_with_cooldown(pool.clone(), 300);
    let user = create_verified_user(&svc, "cooldown", "CorrectHorse1!").await;

    // First sensitive-operation OTP succeeds.
    let first = svc
        .send_otp(Request::new(authn_pb::SendOtpRequest {
            user_id: user.user_id.clone(),
            otp_type: authn_entity_pb::OtpType::SensitiveOperation as i32,
            correlation_id: "cooldown-1".to_string(),
            ..Default::default()
        }))
        .await
        .expect("first OTP")
        .into_inner();
    assert!(!first.otp_id.is_empty());

    // An immediate second OTP of the same type is throttled by the cooldown.
    let throttled = svc
        .send_otp(Request::new(authn_pb::SendOtpRequest {
            user_id: user.user_id.clone(),
            otp_type: authn_entity_pb::OtpType::SensitiveOperation as i32,
            correlation_id: "cooldown-2".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("second OTP within cooldown must be rejected");
    assert_eq!(throttled.code(), tonic::Code::ResourceExhausted);
    let detail = decode_detail(&throttled);
    assert_eq!(detail.kind, ErrorKind::RateLimited as i32);
    assert_eq!(detail.reason, "UDB_RATE_LIMITED");
    assert!(detail.retryable);
    assert_eq!(detail.backend, "authn");
    assert_eq!(detail.operation, "otp_cooldown");
    assert!(detail.retry_after_ms > 0);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_phone_verification -- --ignored --nocapture"]
async fn live_postgres_phone_verification() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "phone", "CorrectHorse1!").await;

    let sent = authn
        .send_phone_verification(Request::new(authn_pb::SendPhoneVerificationRequest {
            user_id: user.user_id.clone(),
            phone: "+15551234567".to_string(),
            ..Default::default()
        }))
        .await
        .expect("send phone verification")
        .into_inner();
    assert!(!sent.otp_id.is_empty());

    let wrong = authn
        .verify_otp(Request::new(authn_pb::VerifyOtpRequest {
            otp_id: sent.otp_id.clone(),
            code: "000000".to_string(),
        }))
        .await
        .expect("wrong phone code")
        .into_inner();
    assert!(!wrong.verified);

    let verified = verify_issued_otp(&authn, &sent.otp_id).await;
    assert!(verified.verified);
    assert_eq!(
        verified.otp_type,
        authn_entity_pb::OtpType::PhoneVerification as i32
    );

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_forgot_and_reset_password -- --ignored --nocapture"]
async fn live_postgres_forgot_and_reset_password() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "forgot", "CorrectHorse1!").await;

    let forgot = authn
        .forgot_password(Request::new(authn_pb::ForgotPasswordRequest {
            identifier: user.email.clone(),
            ..Default::default()
        }))
        .await
        .expect("forgot password")
        .into_inner();
    assert!(!forgot.otp_id.is_empty());

    let reset = authn
        .reset_password(Request::new(authn_pb::ResetPasswordRequest {
            otp_id: forgot.otp_id.clone(),
            code: issued_test_otp_code(&forgot.otp_id),
            new_password: "NewCorrectHorse1!".to_string(),
            ..Default::default()
        }))
        .await
        .expect("reset password")
        .into_inner();
    assert_eq!(reset.user_id, user.user_id);

    // The old password no longer works; the new one does.
    let old = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("old password must fail after reset");
    assert_eq!(old.code(), tonic::Code::Unauthenticated);
    let new = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "NewCorrectHorse1!".to_string(),
            device_name: "reset".to_string(),
            ..Default::default()
        }))
        .await
        .expect("new password login")
        .into_inner();
    assert_eq!(new.user_id, user.user_id);

    // Enumeration safety: an unknown identifier returns a response with the SAME
    // shape as a known one — a non-empty otp_id (a throwaway handle that maps to
    // no real OTP). Returning an *empty* otp_id for unknown accounts while known
    // accounts get a non-empty one would itself be an enumeration oracle, so the
    // handler emits a uniform non-empty otp_id either way.
    let unknown = authn
        .forgot_password(Request::new(authn_pb::ForgotPasswordRequest {
            identifier: "does-not-exist@example.com".to_string(),
            ..Default::default()
        }))
        .await
        .expect("forgot password for unknown account")
        .into_inner();
    assert!(
        !unknown.otp_id.is_empty(),
        "unknown-account forgot_password must return a uniform non-empty otp_id (no enumeration)"
    );

    cleanup_native_auth_db(&pool).await;
}

/// §8A acceptance: production builds do not expose bypass material. Pins that the
/// single dev-echo chokepoint (`mfa::otp_dev_echo_enabled`) is prod-closed for ALL
/// three OTP issuance sites and, when the dev gate is on, every site DOES surface
/// the proof so a headless conformance harness can complete verification.
///
/// The prod-closed property is asserted deterministically through the pure
/// `otp_dev_echo_resolved(env_opt_in, is_production)` decision (the process-wide
/// `OnceLock` posture cannot be toggled mid-run). The dev-on, served-path half runs
/// only when the gate actually resolved enabled in this process
/// (`UDB_OTP_DEV_ECHO=1` + non-production posture), exactly mirroring how a live
/// conformance harness is launched.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_OTP_DEV_ECHO=1 UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_otp_dev_echo_prod_closed -- --ignored --nocapture"]
async fn live_postgres_otp_dev_echo_prod_closed() {
    use crate::runtime::service::auth_service::authn::{
        otp_dev_echo_enabled, otp_dev_echo_resolved,
    };

    // Production posture closes the echo for every site regardless of the env
    // opt-in; dev posture honours it. Reverting 13.1.1.1 (dropping the
    // `!is_production` AND) flips the first assertion and fails the test.
    assert!(
        !otp_dev_echo_resolved(true, true),
        "production posture must close the OTP dev-echo even with UDB_OTP_DEV_ECHO=1 (proof-material leak)"
    );
    assert!(
        otp_dev_echo_resolved(true, false),
        "non-production posture with the env opt-in must permit the dev-echo"
    );
    assert!(
        !otp_dev_echo_resolved(false, false),
        "without the env opt-in the dev-echo stays closed"
    );

    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());

    if !otp_dev_echo_enabled() {
        // The process resolved the gate OFF (no env opt-in or production posture):
        // every served site must return an EMPTY echo — proving prod/no-opt-in
        // builds never leak proof material on the served path.
        let user = create_verified_user(&authn, "echo_off", "CorrectHorse1!").await;
        let sent = authn
            .send_otp(Request::new(authn_pb::SendOtpRequest {
                user_id: user.user_id.clone(),
                otp_type: authn_entity_pb::OtpType::SensitiveOperation as i32,
                correlation_id: "echo-off".to_string(),
                ..Default::default()
            }))
            .await
            .expect("send OTP")
            .into_inner();
        assert!(
            sent.dev_otp_code.is_empty(),
            "SendOTP must NOT echo when the dev gate is closed"
        );
        let forgot = authn
            .forgot_password(Request::new(authn_pb::ForgotPasswordRequest {
                identifier: user.email.clone(),
                ..Default::default()
            }))
            .await
            .expect("forgot password")
            .into_inner();
        assert!(
            forgot.dev_otp_code.is_empty(),
            "ForgotPassword must NOT echo when the dev gate is closed"
        );
        let phone = authn
            .send_phone_verification(Request::new(authn_pb::SendPhoneVerificationRequest {
                user_id: user.user_id.clone(),
                phone: "+15557654321".to_string(),
                ..Default::default()
            }))
            .await
            .expect("send phone verification")
            .into_inner();
        assert!(
            phone.dev_otp_code.is_empty(),
            "SendPhoneVerification must NOT echo when the dev gate is closed"
        );
        cleanup_native_auth_db(&pool).await;
        return;
    }

    // Dev gate ON: every site must surface the real plaintext code AND that code
    // must verify, proving the echo is genuine conformance proof, not noise.
    let user = create_verified_user(&authn, "echo_on", "CorrectHorse1!").await;

    let sent = authn
        .send_otp(Request::new(authn_pb::SendOtpRequest {
            user_id: user.user_id.clone(),
            otp_type: authn_entity_pb::OtpType::SensitiveOperation as i32,
            correlation_id: "echo-on".to_string(),
            ..Default::default()
        }))
        .await
        .expect("send OTP")
        .into_inner();
    assert!(
        !sent.dev_otp_code.is_empty(),
        "SendOTP must echo the code under the dev gate"
    );
    assert_eq!(sent.dev_otp_code, issued_test_otp_code(&sent.otp_id));

    let forgot = authn
        .forgot_password(Request::new(authn_pb::ForgotPasswordRequest {
            identifier: user.email.clone(),
            ..Default::default()
        }))
        .await
        .expect("forgot password")
        .into_inner();
    assert!(
        !forgot.dev_otp_code.is_empty(),
        "ForgotPassword must echo the reset code under the dev gate"
    );
    assert_eq!(forgot.dev_otp_code, issued_test_otp_code(&forgot.otp_id));

    let phone = authn
        .send_phone_verification(Request::new(authn_pb::SendPhoneVerificationRequest {
            user_id: user.user_id.clone(),
            phone: "+15557654321".to_string(),
            ..Default::default()
        }))
        .await
        .expect("send phone verification")
        .into_inner();
    assert!(
        !phone.dev_otp_code.is_empty(),
        "SendPhoneVerification must echo the code under the dev gate"
    );
    assert_eq!(phone.dev_otp_code, issued_test_otp_code(&phone.otp_id));

    // The echoed phone code actually verifies the PHONE_VERIFICATION row.
    let verified = authn
        .verify_otp(Request::new(authn_pb::VerifyOtpRequest {
            otp_id: phone.otp_id.clone(),
            code: phone.dev_otp_code.clone(),
        }))
        .await
        .expect("verify echoed phone code")
        .into_inner();
    assert!(verified.verified);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authn_resend_otp_and_admin_reset -- --ignored --nocapture"]
async fn live_postgres_authn_resend_otp_and_admin_reset() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let suffix = Uuid::new_v4().simple().to_string();

    let created = authn
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: format!("resend_{suffix}"),
            email: format!("resend_{suffix}@example.com"),
            password: "CorrectHorse1!".to_string(),
            tenant_id: "acme".to_string(),
            full_name: "Resend Live".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect("create user")
        .into_inner();
    let user = created.user.expect("created user");

    let resent = authn
        .resend_otp(Request::new(authn_pb::ResendOtpRequest {
            original_otp_id: created.otp_id.clone(),
            reason: "not_received".to_string(),
        }))
        .await
        .expect("resend OTP")
        .into_inner();
    assert!(!resent.otp_id.is_empty());
    assert_ne!(resent.otp_id, created.otp_id);

    let verified = verify_issued_otp(&authn, &resent.otp_id).await;
    assert!(verified.verified);

    let reset = authn
        .admin_reset_password(Request::new(authn_pb::AdminResetPasswordRequest {
            user_id: user.user_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("admin reset password")
        .into_inner();
    assert!(!reset.otp_id.is_empty());

    cleanup_native_auth_db(&pool).await;
}

/// Least-privilege admin scope and reset cooldown share the persisted OTP state.
#[tokio::test]
#[ignore = "requires live Postgres; native CI runs every ignored auth test"]
async fn live_postgres_admin_and_forgot_reset_share_cooldown() {
    use crate::runtime::authn::{PostgresUserStore, UserStore};
    use crate::runtime::service::method_security::{
        scope_claim_context_for_test, test_claim_context,
    };
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service_with_cooldown(pool.clone(), 300);
    let user = create_verified_user(&svc, "reset-cooldown", "CorrectHorse1!").await;
    let caller = || {
        test_claim_context(
            &user.user_id,
            &user.tenant_id,
            &user.project_id,
            &["udb:authn:admin-reset-password"],
            &[],
        )
    };
    let first = scope_claim_context_for_test(
        caller(),
        svc.admin_reset_password(Request::new(authn_pb::AdminResetPasswordRequest {
            user_id: user.user_id.clone(),
            ..Default::default()
        })),
    )
    .await
    .expect("descriptor's least-privilege admin-reset scope passes handler")
    .into_inner();
    let store = PostgresUserStore::new(pool.clone(), "");
    assert!(store.get_otp(&first.otp_id).await.unwrap().is_some());
    let repeated = scope_claim_context_for_test(
        caller(),
        svc.admin_reset_password(Request::new(authn_pb::AdminResetPasswordRequest {
            user_id: user.user_id.clone(),
            ..Default::default()
        })),
    )
    .await
    .expect_err("admin reset honors the persisted cooldown");
    assert_eq!(repeated.code(), tonic::Code::ResourceExhausted);
    assert!(decode_detail(&repeated).retry_after_ms > 0);
    let public_repeat = svc
        .forgot_password(Request::new(authn_pb::ForgotPasswordRequest {
            identifier: user.email.clone(),
            ..Default::default()
        }))
        .await
        .expect("public cooldown keeps the non-enumerating response shape")
        .into_inner();
    assert!(!public_repeat.otp_id.is_empty());
    assert!(
        store
            .get_otp(&public_repeat.otp_id)
            .await
            .unwrap()
            .is_none(),
        "cooldown issues no second durable code"
    );
    assert!(
        store.get_otp(&first.otp_id).await.unwrap().is_some(),
        "the original code remains valid"
    );
    cleanup_native_auth_db(&pool).await;
}

/// Capture only status codes and registered reasons; OTP responses contain
/// secret proof material and must never be included in barrier diagnostics.
async fn observe_otp_issuer<F, T>(
    issuer: F,
    completed: &std::sync::Mutex<[Option<String>; 2]>,
    index: usize,
) -> Result<tonic::Response<T>, tonic::Status>
where
    F: std::future::Future<Output = Result<tonic::Response<T>, tonic::Status>>,
{
    let result = issuer.await;
    let summary = match &result {
        Ok(_) => "completed successfully before the barrier".to_string(),
        Err(status) => format!(
            "{:?}/{}",
            status.code(),
            crate::runtime::error_reasons::reason_of(status).unwrap_or_default(),
        ),
    };
    completed.lock().expect("issuer completion diagnostics")[index] = Some(summary);
    result
}

/// Hold the real owner row until both issuers have reached the atomic store
/// boundary. A check-then-insert implementation cannot satisfy this barrier.
async fn release_owner_after_concurrent_otp_waiters(
    pool: &sqlx::PgPool,
    observer: &mut sqlx::PgConnection,
    gate: sqlx::Transaction<'_, sqlx::Postgres>,
    phase: &str,
    completed: &std::sync::Mutex<[Option<String>; 2]>,
) {
    let mut observed_waiters = 0;
    let barrier = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let waiters: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE datname = current_database() AND wait_event_type = 'Lock' \
                 AND query LIKE '/* udb_otp_cooldown_lock */%'",
            )
            .fetch_one(&mut *observer)
            .await
            .expect("observe actual PostgreSQL issuance locks");
            observed_waiters = waiters;
            if waiters == 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    if barrier.is_err() {
        // The observer has its own connection, so a saturated issuer pool
        // cannot hide the blocker. Classify SQL on the server and return only
        // operation labels, session state and PIDs, never queries or OTP values.
        let states = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            sqlx::query_as::<
                _,
                (
                    i32,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                    String,
                    Vec<i32>,
                ),
            >(
                "SELECT pid, state, wait_event_type, wait_event, \
                 CASE WHEN query LIKE '/* udb_otp_cooldown_lock */%' THEN 'otp_owner_lock' \
                      WHEN query ILIKE '%MAX(%' THEN 'cooldown_lookup' \
                      WHEN query ILIKE '%FROM%users%' THEN 'user_lookup' \
                      WHEN query ILIKE 'BEGIN%' THEN 'transaction_begin' \
                      ELSE 'other' END, pg_blocking_pids(pid) FROM pg_stat_activity \
                 WHERE datname = current_database() \
                 AND pid <> pg_backend_pid() \
                 ORDER BY (wait_event_type = 'Lock') DESC NULLS LAST, pid LIMIT 16",
            )
            .fetch_all(&mut *observer),
        )
        .await;
        panic!(
            "both issuers must serialize at the durable owner lock: \
             phase={phase}; waiters={observed_waiters}; completed={:?}; \
             pool_size={}; pool_idle={}; session_states={states:?}",
            completed.lock().expect("issuer completion diagnostics"),
            pool.size(),
            pool.num_idle(),
        );
    }
    gate.commit().await.expect("release issuance barrier");
}

#[tokio::test]
#[ignore = "requires live Postgres; native CI runs every ignored auth test"]
async fn live_postgres_concurrent_otp_and_reset_issue_one_code_per_cooldown() {
    use crate::runtime::authn::{PostgresUserStore, UserStore};
    use crate::runtime::service::method_security::{
        scope_claim_context_for_test, test_claim_context,
    };
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service_with_cooldown(pool.clone(), 300);
    let user = create_verified_user(&svc, "concurrent-cooldown", "CorrectHorse1!").await;
    let store = PostgresUserStore::new(pool.clone(), "");
    assert!(
        store
            .latest_otp_created_at(
                &user.user_id,
                authn_entity_pb::OtpType::SensitiveOperation as i32,
            )
            .await
            .expect("read sensitive-operation fixture cooldown")
            .is_none(),
        "both sensitive-operation issuers start without an earlier code",
    );
    let count = || async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM udb_authn.otps WHERE user_id = $1::UUID")
            .bind(&user.user_id)
            .fetch_one(&pool)
            .await
            .expect("count durable codes without exposing proof material")
    };
    let before = count().await;
    // Diagnostics must not compete with the owner gate and concurrent issuers
    // for the four-connection service pool. The observer performs no mutation.
    let mut observer = sqlx::PgConnection::connect(&live_pg_dsn())
        .await
        .expect("connect independent OTP lock observer");
    let mut gate = pool.begin().await.expect("issuance barrier transaction");
    sqlx::query("SELECT user_id FROM udb_authn.users WHERE user_id = $1::UUID FOR UPDATE")
        .bind(&user.user_id)
        .fetch_one(&mut *gate)
        .await
        .expect("hold OTP owner");
    let request = || {
        Request::new(authn_pb::SendOtpRequest {
            user_id: user.user_id.clone(),
            otp_type: authn_entity_pb::OtpType::SensitiveOperation as i32,
            ..Default::default()
        })
    };
    let completed = std::sync::Mutex::new([None, None]);
    let (left, right, ()) = tokio::join!(
        observe_otp_issuer(svc.send_otp(request()), &completed, 0),
        observe_otp_issuer(svc.send_otp(request()), &completed, 1),
        release_owner_after_concurrent_otp_waiters(
            &pool,
            &mut observer,
            gate,
            "SendOtp",
            &completed,
        ),
    );
    let mut winners = 0;
    for result in [left, right] {
        match result {
            Ok(response) => {
                winners += 1;
                assert!(
                    store
                        .get_otp(&response.into_inner().otp_id)
                        .await
                        .unwrap()
                        .is_some()
                );
            }
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::ResourceExhausted);
                assert_eq!(decode_detail(&status).reason, "UDB_RATE_LIMITED");
                assert!(decode_detail(&status).retry_after_ms > 0);
            }
        }
    }
    assert_eq!(winners, 1);
    assert_eq!(count().await, before + 1);

    // AdminResetPassword and public ForgotPassword share the same durable
    // PasswordReset cooldown. A public loser must retain its opaque success.
    assert!(
        store
            .latest_otp_created_at(
                &user.user_id,
                authn_entity_pb::OtpType::PasswordReset as i32,
            )
            .await
            .expect("read password-reset fixture cooldown")
            .is_none(),
        "both password-reset issuers start without an earlier code",
    );
    let before = count().await;
    let mut gate = pool.begin().await.expect("reset barrier transaction");
    sqlx::query("SELECT user_id FROM udb_authn.users WHERE user_id = $1::UUID FOR UPDATE")
        .bind(&user.user_id)
        .fetch_one(&mut *gate)
        .await
        .expect("hold reset owner");
    let admin = scope_claim_context_for_test(
        test_claim_context(
            &user.user_id,
            &user.tenant_id,
            &user.project_id,
            &["udb:authn:admin-reset-password"],
            &[],
        ),
        svc.admin_reset_password(Request::new(authn_pb::AdminResetPasswordRequest {
            user_id: user.user_id.clone(),
            ..Default::default()
        })),
    );
    let public = svc.forgot_password(Request::new(authn_pb::ForgotPasswordRequest {
        identifier: user.email.clone(),
        ..Default::default()
    }));
    let completed = std::sync::Mutex::new([None, None]);
    let (admin, public, ()) = tokio::join!(
        observe_otp_issuer(admin, &completed, 0),
        observe_otp_issuer(public, &completed, 1),
        release_owner_after_concurrent_otp_waiters(
            &pool,
            &mut observer,
            gate,
            "PasswordReset",
            &completed,
        ),
    );
    let public = public
        .expect("public reset keeps its non-enumerating shape")
        .into_inner();
    let public_issued = store.get_otp(&public.otp_id).await.unwrap().is_some();
    match admin {
        Ok(response) => {
            assert!(!public_issued, "public loser cannot persist a second code");
            assert!(
                store
                    .get_otp(&response.into_inner().otp_id)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert!(public.dev_otp_code.is_empty());
        }
        Err(status) => {
            assert!(public_issued, "exactly one competing reset must issue");
            assert_eq!(status.code(), tonic::Code::ResourceExhausted);
            assert_eq!(decode_detail(&status).reason, "UDB_RATE_LIMITED");
        }
    }
    assert_eq!(count().await, before + 1);
    observer
        .close()
        .await
        .expect("close independent OTP observer");
    cleanup_native_auth_db(&pool).await;
}

/// The stored phone and whether it is verified, read straight from the user
/// row (the User DTO is built from the record, which does not carry them).
async fn stored_phone(pool: &sqlx::PgPool, user_id: &str) -> (String, bool) {
    let users = crate::runtime::native_catalog::native_model(
        "udb.core.authn.entity.v1.User",
        &["user_id", "phone", "phone_verified_at"],
    );
    let row: (Option<String>, bool) = sqlx::query_as(&format!(
        "SELECT {phone}, {verified} IS NOT NULL FROM {rel} WHERE {uid} = $1::UUID",
        phone = users.q("phone"),
        verified = users.q("phone_verified_at"),
        rel = users.relation,
        uid = users.q("user_id"),
    ))
    .bind(user_id)
    .fetch_one(pool)
    .await
    .expect("read stored phone");
    (row.0.unwrap_or_default(), row.1)
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_phone_only_create_user -- --ignored --nocapture"]
async fn live_postgres_phone_only_create_user() {
    use crate::runtime::service::live_tests::ops_seams_live::EnvRestore;

    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service(pool.clone());

    // No notifier is mounted and no webhook is configured: a phone-only account
    // could never receive its code, so it is refused before anything is written.
    {
        let _no_webhook = EnvRestore::set("UDB_OTP_DELIVERY_WEBHOOK_URL", "");
        let name = format!("phone_refused_{}", Uuid::new_v4().simple());
        let err = svc
            .create_user(Request::new(authn_pb::CreateUserRequest {
                username: name.clone(),
                phone: "+8801712345678".to_string(),
                password: "CorrectHorse1!".to_string(),
                tenant_id: "acme".to_string(),
                project_id: "billing".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("phone-only CreateUser without SMS delivery is refused");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            decode_detail(&err).capability_required,
            "sms_delivery_unavailable"
        );
        let lookup = svc
            .get_user(Request::new(authn_pb::GetUserRequest {
                username: name,
                ..Default::default()
            }))
            .await;
        assert!(
            lookup.is_err() || lookup.unwrap().into_inner().user.is_none(),
            "a refused phone-only CreateUser must write no user"
        );
    }

    // The operator webhook is the SMS channel. Delivery itself is best-effort
    // (nothing listens on the discard port); the issued code is captured by
    // the test build.
    let _webhook = EnvRestore::set("UDB_OTP_DELIVERY_WEBHOOK_URL", "http://127.0.0.1:9/otp");

    // Self-registration: a phone-verification code by SMS; verifying it
    // activates the email-less account and marks the phone verified.
    let created = svc
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: format!("phone_only_{}", Uuid::new_v4().simple()),
            phone: "+8801712345678".to_string(),
            password: "CorrectHorse1!".to_string(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect("phone-only CreateUser")
        .into_inner();
    let user = created.user.expect("created phone-only user");
    assert!(user.email.is_empty());
    assert_eq!(user.phone, "+8801712345678");
    assert_eq!(
        user.status,
        authn_entity_pb::UserStatus::PendingVerification as i32
    );
    assert_eq!(
        stored_phone(&pool, &user.user_id).await,
        ("+8801712345678".to_string(), false)
    );
    let verified = verify_issued_otp(&svc, &created.otp_id).await;
    assert!(verified.verified);
    assert_eq!(
        verified.otp_type,
        authn_entity_pb::OtpType::PhoneVerification as i32
    );
    let activated = svc
        .get_user(Request::new(authn_pb::GetUserRequest {
            user_id: user.user_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("re-read phone-only user")
        .into_inner()
        .user
        .expect("phone-only user present");
    assert_eq!(activated.status, authn_entity_pb::UserStatus::Active as i32);
    assert!(stored_phone(&pool, &user.user_id).await.1);

    // Invite: the password-reset code goes by SMS; setting the first password
    // activates the account and proves the phone (not an email it lacks).
    let invited = svc
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: format!("phone_invite_{}", Uuid::new_v4().simple()),
            phone: "+14155550123".to_string(),
            password_setup_required: true,
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect("phone-only invite")
        .into_inner();
    let invitee = invited.user.expect("invited user");
    assert_eq!(
        invitee.status,
        authn_entity_pb::UserStatus::PasswordSetupRequired as i32
    );
    svc.reset_password(Request::new(authn_pb::ResetPasswordRequest {
        otp_id: invited.otp_id.clone(),
        code: issued_test_otp_code(&invited.otp_id),
        new_password: "CorrectHorse2!".to_string(),
        ..Default::default()
    }))
    .await
    .expect("set the first password from the SMS code");
    let invitee = svc
        .get_user(Request::new(authn_pb::GetUserRequest {
            user_id: invitee.user_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("re-read invitee")
        .into_inner()
        .user
        .expect("invitee present");
    assert_eq!(invitee.status, authn_entity_pb::UserStatus::Active as i32);
    assert!(invitee.email_verified_at.is_none());
    assert!(stored_phone(&pool, &invitee.user_id).await.1);

    // Neither email nor phone: both fields are named.
    let err = svc
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: format!("no_contact_{}", Uuid::new_v4().simple()),
            password: "CorrectHorse1!".to_string(),
            tenant_id: "acme".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("a contact is required");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    let fields: Vec<String> = decode_detail(&err)
        .field_violations
        .into_iter()
        .map(|violation| violation.field)
        .collect();
    assert_eq!(fields, vec!["email", "phone"]);

    cleanup_native_auth_db(&pool).await;
}
