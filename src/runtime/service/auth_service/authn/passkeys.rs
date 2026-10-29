//! 0.6.6 passkeys: the flows built on the WebAuthn ceremonies in `authn/mod.rs`.
//!
//! * Passkey sign-in (`FinishWebAuthnAuthentication`) issues exactly Login's
//!   credentials through `complete_sign_in` ("webauthn" → acr aal2): session,
//!   access token, rotating refresh family, CSRF token.
//! * Usernameless sign-in: `StartWebAuthnAuthentication` with an empty
//!   `user_id` issues discoverable-credential options for a tenant; Finish
//!   resolves the user from the passkey's user handle.
//! * Passkey as a second factor: password Login (`webauthn_challenge_id` +
//!   `webauthn_assertion_json`) and step-up verify an assertion bound to one
//!   user through [`AuthnServiceImpl::verify_webauthn_assertion_for_user`].
//! * Invitation enrollment: [`AuthnServiceImpl::register_passkey_for_new_user_start`]
//!   / [`AuthnServiceImpl::register_passkey_for_new_user_finish`] register a
//!   passkey for the user an invitation is creating.
//!
//! Everything compiles without the `webauthn` feature; the entry points then
//! answer with the same typed capability refusal as the WebAuthn RPCs.

use super::*;

/// `user_id` stored on a usernameless challenge. The challenge names no user
/// until the passkey's user handle does, and the column is a NOT NULL UUID, so
/// the nil UUID marks "any user of the challenge's tenant". It is never a real
/// user id (user ids are random v4 UUIDs).
#[cfg(feature = "webauthn")]
pub(super) const DISCOVERABLE_CHALLENGE_USER_ID: &str = "00000000-0000-0000-0000-000000000000";

/// The one refusal a passkey assertion gets when it does not prove the user:
/// unknown user handle, wrong tenant, no stored passkey, bad signature. One
/// message so a usernameless probe cannot tell those cases apart.
#[cfg(feature = "webauthn")]
fn passkey_invalid_credential_status() -> Status {
    crate::runtime::executor_utils::unauthenticated_status(
        "invalid_webauthn_credential",
        "invalid credential",
    )
}

/// The passkey proved the user, but the account may not sign in (suspended,
/// deactivated, locked, or still awaiting password setup).
#[cfg(feature = "webauthn")]
fn passkey_login_user_active_status() -> Status {
    authn_permission_policy_status("passkey_login", "user_not_active", "user is not active")
}

/// Whether a failed assertion check means "this factor did not verify" (the
/// caller answers with its own invalid-second-factor refusal) rather than an
/// infrastructure or configuration failure that must surface as-is.
#[cfg(any(feature = "webauthn", test))]
fn passkey_assertion_rejected(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unauthenticated | tonic::Code::PermissionDenied | tonic::Code::InvalidArgument
    )
}

/// Test-mode credential for usernameless sign-in: `__UDB_WEBAUTHN_TEST__:<user_id>`
/// names which user's deterministic soft authenticator answers (a usernameless
/// challenge carries no user, so the plain sentinel cannot pick one). Returns
/// the user id, or `None` for anything else.
#[cfg(any(feature = "webauthn", test))]
fn parse_soft_authenticator_user(public_key_credential_json: &str) -> Option<&str> {
    public_key_credential_json
        .trim()
        .strip_prefix("__UDB_WEBAUTHN_TEST__")?
        .strip_prefix(':')
        .map(str::trim)
        .filter(|user_id| !user_id.is_empty())
}

/// Ask the browser for a resident (discoverable) key on the registration
/// options when the tenant policy prefers or requires one, so the passkey can
/// later sign in without a username. `Discouraged` (the historical default)
/// leaves the options untouched. Only the client-facing options change; the
/// server-side registration state does not depend on this. Options that are not
/// the expected JSON shape are returned unchanged.
#[cfg(feature = "webauthn")]
pub(super) fn request_resident_key(creation_json: &str, policy: &WebAuthnPolicy) -> String {
    let resident_key = match policy.resident_key {
        WebAuthnRequirement::Required => "required",
        WebAuthnRequirement::Preferred => "preferred",
        WebAuthnRequirement::Discouraged => return creation_json.to_string(),
    };
    let Ok(mut options) = serde_json::from_str::<serde_json::Value>(creation_json) else {
        return creation_json.to_string();
    };
    let Some(public_key) = options
        .get_mut("publicKey")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return creation_json.to_string();
    };
    let selection = public_key
        .entry("authenticatorSelection")
        .or_insert_with(|| serde_json::json!({}));
    let Some(selection) = selection.as_object_mut() else {
        return creation_json.to_string();
    };
    selection.insert("residentKey".to_string(), serde_json::json!(resident_key));
    selection.insert(
        "requireResidentKey".to_string(),
        serde_json::json!(resident_key == "required"),
    );
    options.to_string()
}

impl AuthnServiceImpl {
    /// Usernameless sign-in, step 1: discoverable-credential request options for
    /// a tenant (`tenant_id`, else the deployment's default tenant). Nothing is
    /// looked up, so the answer is the same whoever asks.
    #[cfg(feature = "webauthn")]
    pub(super) async fn start_discoverable_authentication_impl(
        &self,
        req: authn_pb::StartWebAuthnAuthenticationRequest,
    ) -> Result<authn_pb::StartWebAuthnAuthenticationResponse, Status> {
        let tenant_id = match req.tenant_id.trim() {
            "" => crate::runtime::service::auth_service::default_tenant_id(),
            tenant => tenant.to_string(),
        };
        let (request_options, state) = self
            .webauthn()?
            .start_discoverable_authentication()
            .map_err(|err| {
                authn_internal_status(
                    "start_discoverable_authentication_begin",
                    format!("start usernameless WebAuthn authentication failed: {err:?}"),
                )
            })?;
        let request_json = serde_json::to_string(&request_options).map_err(|err| {
            authn_internal_status(
                "start_discoverable_authentication_challenge_serialize",
                format!("serialize usernameless WebAuthn challenge failed: {err}"),
            )
        })?;
        let challenge_b64 =
            webauthn_softauth::challenge_from_options(&request_json).unwrap_or_default();
        let state_json = serde_json::to_string(&WebAuthnStateEnvelope {
            state,
            label: String::new(),
            challenge: challenge_b64,
        })
        .map_err(|err| {
            authn_internal_status(
                "start_discoverable_authentication_state_serialize",
                format!("serialize usernameless WebAuthn state failed: {err}"),
            )
        })?;
        let cfg = self.webauthn_config()?;
        let expires_at_unix = now_unix().saturating_add(cfg.challenge_ttl_secs);
        // The challenge store keys a challenge by its user and tenant; a
        // usernameless one carries only the tenant (and optional project scope).
        let scope = UserRecord {
            user_id: DISCOVERABLE_CHALLENGE_USER_ID.to_string(),
            tenant_id,
            project_id: req.project_id.trim().to_string(),
            ..UserRecord::default()
        };
        let challenge_id = self
            .store_webauthn_challenge(&scope, "authentication", state_json, expires_at_unix)
            .await?;
        Ok(authn_pb::StartWebAuthnAuthenticationResponse {
            challenge_id,
            public_key_credential_request_options_json: request_json,
            expires_at_unix: expires_at_unix as i64,
        })
    }

    /// Usernameless sign-in, step 2 (called by the shared assertion verifier):
    /// resolve the user from the assertion's user handle, require them to belong
    /// to the challenge's tenant (and project, when the challenge named one) and
    /// verify the signature against that user's stored passkeys. Every miss is
    /// the same `invalid credential` refusal.
    #[cfg(feature = "webauthn")]
    pub(super) async fn finish_discoverable_assertion(
        &self,
        challenge: &WebAuthnChallengeRecord,
        public_key_credential_json: &str,
    ) -> Result<(UserRecord, webauthn_rs::prelude::AuthenticationResult), Status> {
        use webauthn_rs::prelude::{
            DiscoverableAuthentication, DiscoverableKey, Passkey, PublicKeyCredential, Url,
        };

        let envelope: WebAuthnStateEnvelope<DiscoverableAuthentication> =
            serde_json::from_str(&challenge.state_json).map_err(|err| {
                authn_internal_status(
                    "finish_discoverable_authentication_state_decode",
                    format!("decode usernameless WebAuthn state failed: {err}"),
                )
            })?;
        let soft_user = if webauthn_softauth::test_mode_enabled() {
            parse_soft_authenticator_user(public_key_credential_json)
        } else {
            None
        };
        let credential: PublicKeyCredential = match soft_user {
            // Dev soft authenticator (UDB_WEBAUTHN_TEST_MODE only): a genuine
            // ES256 assertion from the named user's deterministic keypair, with
            // that user's handle; the real verifier below checks it.
            Some(user_id) => {
                let cfg = self.webauthn_config()?;
                let origin = Url::parse(&cfg.rp_origin)
                    .map_err(|err| {
                        authn_internal_status(
                            "finish_discoverable_authentication_origin_parse",
                            format!("invalid WebAuthn origin: {err}"),
                        )
                    })?
                    .origin()
                    .ascii_serialization();
                let json = webauthn_softauth::make_assertion(
                    user_id,
                    &cfg.rp_id,
                    &origin,
                    &envelope.challenge,
                )
                .map_err(|err| {
                    authn_internal_status(
                        "finish_discoverable_authentication_dev_make",
                        format!("dev WebAuthn assertion failed: {err}"),
                    )
                })?;
                serde_json::from_str(&json).map_err(|err| {
                    authn_internal_status(
                        "finish_discoverable_authentication_dev_decode",
                        format!("decode dev WebAuthn assertion failed: {err}"),
                    )
                })?
            }
            None => serde_json::from_str(public_key_credential_json)
                .map_err(Self::invalid_webauthn_authentication_credential_json_status)?,
        };
        let webauthn = self.webauthn()?;
        let (user_handle, _credential_id) = webauthn
            .identify_discoverable_authentication(&credential)
            .map_err(|_| passkey_invalid_credential_status())?;
        let user = self
            .users
            .get_user_by_id(&user_handle.to_string())
            .await
            .map_err(|err| {
                authn_internal_status(
                    "finish_discoverable_authentication_user_load",
                    err.to_string(),
                )
            })?
            .ok_or_else(passkey_invalid_credential_status)?;
        if user.tenant_id != challenge.tenant_id
            || (!challenge.project_id.is_empty() && user.project_id != challenge.project_id)
        {
            return Err(passkey_invalid_credential_status());
        }
        let keys = self
            .load_webauthn_passkeys(&user.user_id)
            .await?
            .iter()
            .map(|rec| {
                serde_json::from_str::<Passkey>(&rec.passkey_json)
                    .map(|passkey| DiscoverableKey::from(&passkey))
                    .map_err(|err| {
                        authn_internal_status(
                            "finish_discoverable_authentication_passkey_decode",
                            format!("decode WebAuthn passkey failed: {err}"),
                        )
                    })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        if keys.is_empty() {
            return Err(passkey_invalid_credential_status());
        }
        let result = webauthn
            .finish_discoverable_authentication(&credential, envelope.state, &keys)
            .map_err(|_| passkey_invalid_credential_status())?;
        Ok((user, result))
    }

    /// Passkey sign-in, last step: the assertion has proven `user`; apply Login's
    /// account gate, then issue Login's credentials via `complete_sign_in`
    /// ("webauthn" → acr aal2) and map them into the Finish response.
    #[cfg(feature = "webauthn")]
    pub(super) async fn passkey_sign_in(
        &self,
        user: UserRecord,
        credential_id: String,
        req: &authn_pb::FinishWebAuthnAuthenticationRequest,
    ) -> Result<authn_pb::FinishWebAuthnAuthenticationResponse, Status> {
        let now = now_unix();
        let may_sign_in = matches!(
            user.status,
            AccountStatus::Active | AccountStatus::PendingVerification
        ) && user.locked_until_unix <= now;
        if !may_sign_in {
            return Err(passkey_login_user_active_status());
        }
        let prior = user.clone();
        let mut user = user;
        user.last_login_at_unix = now;
        user.updated_at_unix = now;
        self.users.put_user(user.clone()).await.map_err(|err| {
            authn_internal_status("passkey_login_success_update", err.to_string())
        })?;
        let signed_in = self
            .complete_sign_in(
                user.clone(),
                super::login::SignIn {
                    device_name: &req.device_name,
                    ip_address: &req.ip_address,
                    user_agent: &req.user_agent,
                    device_id: &req.device_id,
                    requested_scopes: &[],
                    token_auth_method: "webauthn",
                    audit_auth_method: "passkey",
                    audit_reason_code: "passkey_verified",
                    restore_on_failure: Some(prior),
                },
                now,
            )
            .await?;
        let expires_at_unix = (now as i64).saturating_add(signed_in.access_token_expires_in as i64);
        let principal = principal_from_user_record(&user, authn::AuthnMethod::WebAuthn);
        Ok(authn_pb::FinishWebAuthnAuthenticationResponse {
            // The passkey path HAS the user record, so project its profile
            // attributes rather than an empty map.
            principal: Some(authn_principal_to_pb_with_attributes(
                &principal,
                expires_at_unix,
                account_kind_to_proto(user.account_kind),
                &user.profile_attributes_json,
            )),
            session_id: signed_in.session_id,
            access_token: signed_in.access_token,
            expires_at_unix,
            credential_id,
            refresh_token: signed_in.refresh_token,
            access_token_expires_in: signed_in.access_token_expires_in,
            refresh_token_expires_in: signed_in.refresh_token_expires_in,
            session_token: signed_in.session_token,
            csrf_token: signed_in.csrf_token,
            session_public_id: signed_in.session_public_id,
            user_id: signed_in.user_id,
        })
    }

    /// Second factor / step-up, step 1: assertion request options for `user_id`
    /// (who must hold at least one passkey), stored as a single-use
    /// `authentication` challenge bound to that user. `tenant_id`, when
    /// non-empty, must be the user's tenant. Returns `(challenge_id,
    /// options_json)`; hand `options_json` to `navigator.credentials.get()`.
    pub(super) async fn start_webauthn_assertion_for_user(
        &self,
        user_id: &str,
        tenant_id: &str,
    ) -> Result<(String, String), Status> {
        #[cfg(feature = "webauthn")]
        {
            if user_id.trim().is_empty() {
                return Err(Self::webauthn_user_id_required_status());
            }
            let started = self
                .start_webauthn_authentication_impl(authn_pb::StartWebAuthnAuthenticationRequest {
                    user_id: user_id.to_string(),
                    tenant_id: tenant_id.to_string(),
                    ..Default::default()
                })
                .await?;
            Ok((
                started.challenge_id,
                started.public_key_credential_request_options_json,
            ))
        }
        #[cfg(not(feature = "webauthn"))]
        {
            let _ = (user_id, tenant_id);
            Err(webauthn_feature_required_status())
        }
    }

    /// Second factor / step-up, step 2: verify `assertion_json` against
    /// `challenge_id`, which must have been issued for `user_id` (a usernameless
    /// challenge or another user's is refused). On success the challenge is
    /// consumed, the passkey's sign counter advances and `WEBAUTHN_AUTHENTICATED`
    /// is audited. `Ok(false)` = the factor did not verify (bad or foreign
    /// assertion, unknown/expired/used challenge, malformed JSON); `Err` = an
    /// infrastructure, configuration or tenant-policy failure.
    pub(super) async fn verify_webauthn_assertion_for_user(
        &self,
        user_id: &str,
        challenge_id: &str,
        assertion_json: &str,
    ) -> Result<bool, Status> {
        #[cfg(feature = "webauthn")]
        {
            match self
                .verify_webauthn_authentication_assertion(
                    challenge_id,
                    assertion_json,
                    Some(user_id),
                )
                .await
            {
                Ok((user, _credential_id)) => Ok(user.user_id == user_id),
                Err(status) if passkey_assertion_rejected(&status) => Ok(false),
                Err(status) => Err(status),
            }
        }
        #[cfg(not(feature = "webauthn"))]
        {
            let _ = (user_id, challenge_id, assertion_json);
            Err(webauthn_feature_required_status())
        }
    }

    /// Invitation passkey enrollment, step 1, for a user the invitation flow is
    /// about to create: `user` carries the RESERVED `user_id` (a v4 UUID — it
    /// becomes the passkey's user handle, so create the user with exactly this
    /// id), `username`, `full_name`, `tenant_id` and `project_id`; it need not be
    /// stored yet. Asks for a resident key per the tenant WebAuthn policy.
    /// Returns `(challenge_id, creation_options_json, expires_at_unix)`; hand the
    /// options to `navigator.credentials.create()`.
    pub(super) async fn register_passkey_for_new_user_start(
        &self,
        user: &authn::UserRecord,
        label: &str,
    ) -> Result<(String, String, i64), Status> {
        #[cfg(feature = "webauthn")]
        {
            if Uuid::parse_str(user.user_id.trim()).is_err() {
                return Err(Self::webauthn_user_id_uuid_status());
            }
            let started = self
                .start_passkey_registration_for(user, label.to_string())
                .await?;
            Ok((
                started.challenge_id,
                started.public_key_credential_creation_options_json,
                started.expires_at_unix,
            ))
        }
        #[cfg(not(feature = "webauthn"))]
        {
            let _ = (user, label);
            Err(webauthn_feature_required_status())
        }
    }

    /// Invitation passkey enrollment, step 2: verify `attestation_json` (the
    /// `navigator.credentials.create()` result) against the challenge from step 1
    /// for the SAME user/tenant/project, enforce the tenant WebAuthn policy,
    /// store the passkey, consume the challenge and audit `WEBAUTHN_REGISTERED`.
    /// Returns the credential id. The passkey row references `users`, so the user
    /// must already be stored (in the acceptance's failure scope: on `Err` the
    /// acceptance must not activate the account). The user record is not
    /// modified; set `mfa_enabled` like StartWebAuthnRegistration does if the
    /// account should then require a second factor on password Login.
    pub(super) async fn register_passkey_for_new_user_finish(
        &self,
        user: &authn::UserRecord,
        challenge_id: &str,
        attestation_json: &str,
        label: &str,
    ) -> Result<String, Status> {
        #[cfg(feature = "webauthn")]
        {
            Self::validate_webauthn_finish_fields(challenge_id, attestation_json)?;
            let challenge = self
                .load_webauthn_challenge(challenge_id, "registration")
                .await?;
            self.finish_passkey_registration_for(
                user,
                &challenge,
                challenge_id,
                attestation_json,
                label,
            )
            .await
        }
        #[cfg(not(feature = "webauthn"))]
        {
            let _ = (user, challenge_id, attestation_json, label);
            Err(webauthn_feature_required_status())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn svc() -> AuthnServiceImpl {
        AuthnServiceImpl::new(
            authn::AuthnConfig::default(),
            crate::runtime::security::SecurityConfig::default(),
        )
    }

    #[test]
    fn soft_authenticator_user_is_parsed_only_from_the_named_sentinel() {
        assert_eq!(
            parse_soft_authenticator_user("__UDB_WEBAUTHN_TEST__:5f0c2c4e-1"),
            Some("5f0c2c4e-1")
        );
        assert_eq!(
            parse_soft_authenticator_user("  __UDB_WEBAUTHN_TEST__: abc  "),
            Some("abc")
        );
        assert_eq!(parse_soft_authenticator_user("__UDB_WEBAUTHN_TEST__"), None);
        assert_eq!(
            parse_soft_authenticator_user("__UDB_WEBAUTHN_TEST__:"),
            None
        );
        assert_eq!(parse_soft_authenticator_user("{\"id\":\"x\"}"), None);
    }

    #[test]
    fn only_verification_refusals_count_as_a_failed_factor() {
        let rejected = [
            crate::runtime::executor_utils::unauthenticated_status("bad_assertion", "x"),
            authn_permission_policy_status("webauthn", "invalid_webauthn_ceremony", "x"),
            crate::runtime::executor_utils::invalid_argument_fields(
                "x",
                [("public_key_credential_json", "must decode")],
            ),
        ];
        for status in &rejected {
            assert!(passkey_assertion_rejected(status), "{status:?}");
        }
        // Infrastructure / configuration / tenant-policy failures surface as-is.
        let surfaced = [
            authn_internal_status("load_webauthn_challenge_query", "x"),
            authn_capability_status("webauthn_relying_party_config", "webauthn_rp_config", "x"),
            authn_policy_status("webauthn_assertion_policy", "uv_required", "x"),
        ];
        for status in &surfaced {
            assert!(!passkey_assertion_rejected(status), "{status:?}");
        }
    }

    #[cfg(feature = "webauthn")]
    #[test]
    fn soft_authenticator_sentinel_prefix_matches_the_dev_authenticator() {
        assert!("__UDB_WEBAUTHN_TEST__:u".starts_with(webauthn_softauth::TEST_CREDENTIAL_SENTINEL));
    }

    #[cfg(feature = "webauthn")]
    fn creation_options() -> String {
        serde_json::json!({
            "publicKey": {
                "challenge": "abc",
                "authenticatorSelection": {
                    "requireResidentKey": false,
                    "residentKey": "discouraged",
                    "userVerification": "required"
                }
            }
        })
        .to_string()
    }

    #[cfg(feature = "webauthn")]
    #[test]
    fn resident_key_follows_the_tenant_policy() {
        let options = creation_options();
        let discouraged = WebAuthnPolicy::from_parts(None, Some("discouraged".into()), None);
        assert_eq!(request_resident_key(&options, &discouraged), options);

        for (policy, expected, required) in [
            ("preferred", "preferred", false),
            ("required", "required", true),
        ] {
            let policy = WebAuthnPolicy::from_parts(None, Some(policy.into()), None);
            let adjusted: serde_json::Value =
                serde_json::from_str(&request_resident_key(&options, &policy)).unwrap();
            let selection = &adjusted["publicKey"]["authenticatorSelection"];
            assert_eq!(selection["residentKey"], expected);
            assert_eq!(selection["requireResidentKey"], required);
            assert_eq!(selection["userVerification"], "required");
            assert_eq!(adjusted["publicKey"]["challenge"], "abc");
        }
    }

    #[cfg(feature = "webauthn")]
    #[test]
    fn resident_key_leaves_unexpected_options_untouched() {
        let policy = WebAuthnPolicy::from_parts(None, Some("required".into()), None);
        assert_eq!(request_resident_key("not json", &policy), "not json");
        assert_eq!(request_resident_key("{\"x\":1}", &policy), "{\"x\":1}");
        let adjusted: serde_json::Value =
            serde_json::from_str(&request_resident_key("{\"publicKey\":{}}", &policy)).unwrap();
        assert_eq!(
            adjusted["publicKey"]["authenticatorSelection"]["residentKey"],
            "required"
        );
    }

    #[cfg(feature = "webauthn")]
    #[test]
    fn discoverable_marker_is_the_nil_uuid() {
        assert_eq!(
            Uuid::parse_str(DISCOVERABLE_CHALLENGE_USER_ID).unwrap(),
            Uuid::nil()
        );
    }

    #[cfg(feature = "webauthn")]
    #[tokio::test]
    async fn malformed_second_factor_input_is_a_failed_factor_not_an_error() {
        let svc = svc();
        // Empty challenge id / assertion: refused at the boundary, reported as
        // "did not verify" so Login answers its uniform invalid-second-factor.
        assert!(
            !svc.verify_webauthn_assertion_for_user("00000000-0000-4000-8000-000000000001", "", "")
                .await
                .unwrap()
        );
        // A non-UUID challenge id never reaches the store.
        assert!(
            !svc.verify_webauthn_assertion_for_user(
                "00000000-0000-4000-8000-000000000001",
                "not-a-uuid",
                "{}"
            )
            .await
            .unwrap()
        );
    }

    #[cfg(feature = "webauthn")]
    #[tokio::test]
    async fn new_user_passkey_enrollment_requires_a_uuid_user_id() {
        let user = UserRecord {
            user_id: "not-a-uuid".to_string(),
            tenant_id: "acme".to_string(),
            ..UserRecord::default()
        };
        let err = svc()
            .register_passkey_for_new_user_start(&user, "laptop")
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        let err = svc()
            .register_passkey_for_new_user_finish(&user, "", "", "laptop")
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[cfg(feature = "webauthn")]
    #[tokio::test]
    async fn second_factor_start_requires_a_user() {
        let err = svc()
            .start_webauthn_assertion_for_user(" ", "acme")
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[cfg(not(feature = "webauthn"))]
    #[tokio::test]
    async fn passkey_helpers_refuse_with_the_webauthn_capability_without_the_feature() {
        let svc = svc();
        let user = UserRecord::default();
        let statuses = [
            svc.start_webauthn_assertion_for_user("u", "t")
                .await
                .unwrap_err(),
            svc.verify_webauthn_assertion_for_user("u", "c", "{}")
                .await
                .unwrap_err(),
            svc.register_passkey_for_new_user_start(&user, "l")
                .await
                .unwrap_err(),
            svc.register_passkey_for_new_user_finish(&user, "c", "{}", "l")
                .await
                .unwrap_err(),
        ];
        for status in statuses {
            assert_eq!(status.code(), webauthn_feature_required_status().code());
            assert_eq!(
                status.message(),
                webauthn_feature_required_status().message()
            );
        }
    }
}
