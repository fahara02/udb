#!/usr/bin/env python3
"""Source guard for Chapter 13 gap-closure posture.

This pins the source evidence for the broker authn conformance gate (13.1) and
the read-after-write live-test family (13.7). It is intentionally source-only:
no cargo, live Postgres, buf, or SDK generation.
"""

from __future__ import annotations

import argparse
import tempfile
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class SourceCheck:
    label: str
    path: str
    required: tuple[str, ...]


CHECKS: tuple[SourceCheck, ...] = (
    SourceCheck(
        "OTP dev-echo fail-closed gate",
        "src/runtime/service/auth_service/authn/mfa.rs",
        (
            "pub(crate) fn otp_dev_echo_resolved(env_opt_in: bool, is_production: bool) -> bool",
            "env_opt_in && !is_production",
            "crate::runtime::security::SecurityConfig::current().is_production()",
            "This is the SINGLE",
            "dev_otp_code",
            "send_phone_verification_impl",
        ),
    ),
    SourceCheck(
        "ForgotPassword dev OTP echo",
        "src/runtime/service/auth_service/authn/login.rs",
        (
            "forgot_password_impl",
            "let (otp_id, code)",
            "authn_entity_pb::OtpType::PasswordReset",
            "super::mfa::otp_dev_echo_enabled()",
            "authn_pb::ForgotPasswordResponse",
            "dev_otp_code",
            "Uniform (non-enumerating) miss branch",
        ),
    ),
    SourceCheck(
        "Authn proto dev OTP fields",
        "proto/udb/core/authn/services/v1/core.proto",
        (
            "message ForgotPasswordResponse",
            "string dev_otp_code = 2;",
            "message SendPhoneVerificationResponse",
            "Dev-only echo of the plaintext PASSWORD_RESET OTP code",
            "Dev-only echo of the plaintext PHONE_VERIFICATION OTP code",
        ),
    ),
    SourceCheck(
        "OTP served-path conformance test",
        "src/runtime/service/auth_service/tests/authn_otp_password_live.rs",
        (
            "live_postgres_otp_dev_echo_prod_closed",
            "otp_dev_echo_resolved(true, true)",
            "SendOTP must NOT echo when the dev gate is closed",
            "ForgotPassword must echo the reset code under the dev gate",
            "SendPhoneVerification must echo the code under the dev gate",
            "verify echoed phone code",
        ),
    ),
    SourceCheck(
        "media read-after-write helper",
        "src/runtime/service/live_tests/support.rs",
        (
            "pub(super) async fn assert_create_then_get",
            "created_id",
            "get(created_id.to_string())",
        ),
    ),
    SourceCheck(
        "auth read-after-write helper",
        "src/runtime/service/auth_service/tests/support.rs",
        (
            "pub(super) async fn assert_create_then_get",
            "created_id",
            "get(created_id.to_string())",
        ),
    ),
    SourceCheck(
        "authz policy read-after-write tests",
        "src/runtime/service/auth_service/tests/authz_admin_live.rs",
        (
            "live_postgres_authz_create_policy_rule_read_after_write",
            "live_postgres_authz_governance_activate_policy_read_after_write",
            "CreatePolicyRule",
            "GetPolicyRule",
            "CreatePolicyRule→GetPolicyRule must resolve the returned id",
            "CreatePolicyDraft",
            "SubmitPolicyDraft",
            "ApprovePolicyDraft",
            "ActivatePolicyVersion",
            "ActivatePolicyVersion→GetPolicyRule must resolve the original document id",
            "activated governance policy must be readable by its original id",
            "break_glass_reason",
        ),
    ),
    SourceCheck(
        "storage read-after-write test",
        "src/runtime/service/live_tests/storage_live.rs",
        (
            "RegisterUpload→GetFile",
            "assert_create_then_get",
            "get_file",
        ),
    ),
    SourceCheck(
        "asset read-after-write test",
        "src/runtime/service/live_tests/asset_live.rs",
        (
            "RegisterAsset→GetAsset",
            "StartPipeline→GetPipeline",
            "StartPipeline must return inline steps",
            "assert_create_then_get",
        ),
    ),
    SourceCheck(
        "webrtc read-after-write test",
        "src/runtime/service/live_tests/webrtc_live.rs",
        (
            "CreateRoom→GetRoom",
            "JoinRoom→GetPeer",
            "assert_create_then_get",
        ),
    ),
    SourceCheck(
        "authn user read-after-write test",
        "src/runtime/service/auth_service/tests/authn_user_live.rs",
        (
            "CreateUser→GetUser",
            "CreateUser→ListUsers",
            "assert_create_then_get",
        ),
    ),
    SourceCheck(
        "tenant read-after-write test",
        "src/runtime/service/auth_service/tests/tenant_live.rs",
        (
            "CreateTenant→GetTenant",
            "assert_create_then_get",
        ),
    ),
    SourceCheck(
        "apikey read-after-write test",
        "src/runtime/service/auth_service/tests/apikey_live.rs",
        (
            "CreateApiKey→GetApiKey",
            "assert_create_then_get",
            "GetApiKey",
        ),
    ),
    SourceCheck(
        "typed consistency proto contract",
        "proto/udb/entity/v1/consistency.proto",
        (
            "enum ConsistencyMode",
            "CONSISTENCY_MODE_READ_YOUR_WRITES = 2;",
            "message WriteReceipt",
            "string source_lsn = 1;",
            "uint64 outbox_seq = 2;",
            "message ReadFence",
            "string min_outbox_lsn = 1;",
            "uint64 max_wait_ms = 3;",
        ),
    ),
    SourceCheck(
        "MutationResponse typed receipt field keeps JSON compatibility",
        "proto/udb/entity/v1/mutation.proto",
        (
            'import "udb/entity/v1/consistency.proto";',
            "string write_receipt_json = 7;",
            "WriteReceipt write_receipt = 11;",
            "Kept in lockstep with write_receipt_json",
        ),
    ),
    SourceCheck(
        "RequestContext typed fence fields keep JSON compatibility",
        "proto/udb/entity/v1/context.proto",
        (
            'import "udb/entity/v1/consistency.proto";',
            "string read_fence_json = 14;",
            "ReadFence read_fence = 21;",
            "ConsistencyMode consistency_mode = 22;",
            "Metadata/header values still win",
        ),
    ),
    SourceCheck(
        "Rust consistency proto converters",
        "src/runtime/consistency.rs",
        (
            "pub(crate) fn from_proto_i32(mode: i32) -> Option<Self>",
            "pub(crate) fn to_proto_i32(self) -> i32",
            "pub(crate) fn to_proto(&self) -> crate::proto::WriteReceipt",
            "pub(crate) fn from_proto(proto: &crate::proto::WriteReceipt) -> Self",
            "pub(crate) fn to_proto(&self) -> crate::proto::ReadFence",
            "pub(crate) fn from_proto(proto: &crate::proto::ReadFence) -> Self",
            "write_receipt_proto_round_trip_preserves_serde_shape",
            "read_fence_proto_round_trip_preserves_serde_shape",
            "consistency_mode_proto_numbers_match_pinned_wire_tokens",
        ),
    ),
    SourceCheck(
        "RequestContext merge consumes typed fence fields with metadata precedence",
        "src/runtime/executor_utils.rs",
        (
            "ConsistencyMode::from_proto_i32",
            "proto.consistency_mode",
            "proto.read_fence",
            "ReadFence::from_proto",
            "consistency: first_non_empty(&metadata_context.consistency, &proto_consistency)",
            "read_fence_json: first_non_empty(&metadata_context.read_fence_json, &proto_read_fence_json)",
            "typed_body_read_fence_used_only_when_metadata_absent",
            "metadata_read_fence_wins_over_typed_body_read_fence",
            "legacy_body_read_fence_json_wins_over_typed_body_read_fence",
            "typed_body_consistency_used_only_when_metadata_absent",
            "legacy_body_consistency_string_wins_over_typed_body_consistency",
            "metadata_consistency_wins_over_body_consistency_fields",
        ),
    ),
    SourceCheck(
        "MutationResponse stamper emits typed receipt plus JSON",
        "src/runtime/service/mod.rs",
        (
            "mutation.write_receipt.as_ref()",
            "WriteReceipt::from_proto",
            "mutation.write_receipt_json = receipt_json.clone();",
            "mutation.write_receipt = Some(receipt.to_proto());",
            "x-udb-write-receipt",
        ),
    ),
    SourceCheck(
        "Data-plane idempotency replay restores typed receipt from JSON",
        "src/runtime/core/setup_data.rs",
        (
            "write_receipt: Some(receipt.to_proto())",
            "mutation_response_from_idempotency_json",
            "serde_json::from_str::<crate::runtime::consistency::WriteReceipt>(raw)",
            "replay should restore typed write_receipt from stored JSON",
        ),
    ),
)


def check_source(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    for check in CHECKS:
        path = root / check.path
        if not path.is_file():
            failures.append(f"{check.label}: missing file {check.path}")
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for token in check.required:
            if token not in text:
                failures.append(f"{check.label}: missing token {token!r} in {check.path}")
    return failures


def run_selftest() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for check in CHECKS:
            path = root / check.path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("\n".join(check.required) + "\n", encoding="utf-8")

        failures = check_source(root)
        if failures:
            raise AssertionError(f"expected clean fixture, got {failures}")

        broken = root / "src/runtime/service/auth_service/authn/mfa.rs"
        broken.write_text(
            broken.read_text(encoding="utf-8").replace("env_opt_in && !is_production", "env_opt_in"),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("OTP dev-echo fail-closed gate" in failure for failure in failures):
            raise AssertionError(f"expected missing prod-closed gate failure, got {failures}")

        fixed = root / "src/runtime/service/auth_service/authn/mfa.rs"
        fixed.write_text("\n".join(CHECKS[0].required) + "\n", encoding="utf-8")
        authz = root / "src/runtime/service/auth_service/tests/authz_admin_live.rs"
        authz.write_text(
            authz.read_text(encoding="utf-8").replace(
                "live_postgres_authz_governance_activate_policy_read_after_write", ""
            ),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("authz policy read-after-write tests" in failure for failure in failures):
            raise AssertionError(f"expected missing governance lifecycle test failure, got {failures}")

    print("gap-closure posture selftest passed")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selftest", action="store_true", help="run no-repo fixture assertions")
    args = parser.parse_args(argv)
    if args.selftest:
        return run_selftest()

    failures = check_source()
    if failures:
        for failure in failures:
            print(f"::error::{failure}")
        return 1
    print("gap-closure posture guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
