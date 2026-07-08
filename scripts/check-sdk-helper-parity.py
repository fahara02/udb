#!/usr/bin/env python3
"""Fail CI if Chapter 13 SDK helper parity drifts across Go, TS, and Python."""

from __future__ import annotations

import argparse
import sys
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
        "go auth helpers",
        "sdk/go/udbclient/auth_native.go",
        (
            "func (c *AuthClient) ConformanceProof(",
            "ConformanceOTP",
            "SendOTP",
            "ForgotPassword",
            "SendPhoneVerification",
            "totpNow",
            "func (c *AuthClient) Passkeys()",
            "StartWebAuthnRegistration",
            "FinishWebAuthnRegistration",
            "StartWebAuthnAuthentication",
            "FinishWebAuthnAuthentication",
        ),
    ),
    SourceCheck(
        "go event and notification helpers",
        "sdk/go/udbclient/services.go",
        (
            "func (f *NotificationFacade) SendTemplate(",
            "func (f *NotificationFacade) RetryFailed(",
            "func (f *NotificationFacade) WaitForDelivery(",
            "GetNotification",
            "type EventsFacade struct",
            "func (f *EventsFacade) Subscribe(",
            "func (s *Subscription) Ready()",
            "func (f *EventsFacade) PublishAndWait(",
            "PublishCDC",
            "EnqueueOutboxEvent",
        ),
    ),
    SourceCheck(
        "go media helpers",
        "sdk/go/udbclient/media.go",
        (
            "func (f *AssetFacade) DefinePipeline(",
            "func (f *AssetFacade) RegisterFromStorageFile(",
            "func (f *AssetFacade) StartAndWait(",
            "start.GetSteps()",
            "func (f *WebRTCFacade) JoinSession(",
            "JoinSessionRequest",
            "Signal(streamCtx)",
        ),
    ),
    SourceCheck(
        "go project wiring",
        "sdk/go/udbclient/project.go",
        (
            "Events  *EventsFacade",
            "u.Events = newEventsFacade(",
        ),
    ),
    SourceCheck(
        "go helper tests",
        "sdk/go/udbclient/auth_helpers_test.go",
        (
            "TestConformanceProofOTP",
            "TestConformanceProofEmptyErrors",
            "TestPasskeysRegisterTwoRPCs",
            "TestPasskeysAuthenticateTwoRPCs",
        ),
    ),
    SourceCheck(
        "go event tests",
        "sdk/go/udbclient/events_test.go",
        (
            "TestEventsSubscribeReadyAndPublishAndWait",
            "Ready()",
            "PublishAndWait",
        ),
    ),
    SourceCheck(
        "go media tests",
        "sdk/go/udbclient/media_helpers_test.go",
        (
            "TestJoinSessionAtomicAndLeave",
            "TestAssetStartAndWaitUsesInlineSteps",
            "TestNotificationSendTemplateAndWait",
            "JoinSession must be atomic",
            "GetPipeline",
        ),
    ),
    SourceCheck(
        "typescript auth helpers",
        "sdk/typescript/auth.ts",
        (
            "async conformanceProof(",
            "SendOTP",
            "ForgotPassword",
            "SendPhoneVerification",
            "totpNow",
            "readonly passkeys =",
            "StartWebAuthnRegistration",
            "FinishWebAuthnRegistration",
            "StartWebAuthnAuthentication",
            "FinishWebAuthnAuthentication",
        ),
    ),
    SourceCheck(
        "typescript project helpers",
        "sdk/typescript/project.ts",
        (
            "export class NotificationFacade",
            "sendTemplate(",
            "retryFailed(",
            "waitForDelivery(",
            "export class AssetFacade",
            "definePipeline(",
            "registerFromStorageFile(",
            "startAndWait(",
            "export class EventsFacade",
            "publishAndWait(",
            "readonly events: EventsFacade",
            "async joinSession(",
            "JoinSession",
            "EnqueueOutboxEvent",
            "PublishCDC",
        ),
    ),
    SourceCheck(
        "typescript live scenario hooks",
        "sdk/typescript/live-auth.test.ts",
        (
            "events.publishAndWait",
            "project.events.publishAndWait",
            "webrtc.joinSession",
            "project.webrtc.joinSession",
        ),
    ),
    SourceCheck(
        "python auth helpers",
        "sdk/python/udb_client/auth.py",
        (
            "def totp_now(",
            "def conformance_proof(",
            "SendOTP",
            "ForgotPassword",
            "SendPhoneVerification",
            "dev_otp_code",
            "def passkeys(",
            "StartWebAuthnRegistration",
            "FinishWebAuthnRegistration",
            "StartWebAuthnAuthentication",
            "FinishWebAuthnAuthentication",
        ),
    ),
    SourceCheck(
        "python project helpers",
        "sdk/python/udb_client/project.py",
        (
            "def send_template(",
            "def retry_failed(",
            "def wait_for_delivery(",
            "def define_pipeline(",
            "def register_from_storage_file(",
            "def start_and_wait(",
            "class _EventsFacade",
            "def publish_and_wait(",
            "self.events = _EventsFacade(",
            "def join_session(",
            "JoinSession",
            "EnqueueOutboxEvent",
            "PublishCDC",
        ),
    ),
    SourceCheck(
        "python helper tests",
        "sdk/python/tests/test_simple_client.py",
        (
            "test_events_ready_and_publish_and_wait",
            "test_join_session_one_rpc_and_leave",
            "test_send_template_and_wait_for_delivery",
            "test_start_and_wait_reads_inline_steps",
            "test_conformance_proof_otp",
            "test_passkeys_register_two_rpcs",
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
        text = path.read_text(encoding="utf-8")
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

        broken = root / "sdk/typescript/project.ts"
        broken.write_text(broken.read_text(encoding="utf-8").replace("publishAndWait(\n", ""), encoding="utf-8")
        failures = check_source(root)
        if not any("typescript project helpers" in failure and "publishAndWait(" in failure for failure in failures):
            raise AssertionError(f"expected missing TS publishAndWait failure, got {failures}")

    print("SDK helper parity selftest passed")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selftest", action="store_true", help="run no-repo assertions")
    args = parser.parse_args(argv)
    if args.selftest:
        return run_selftest()

    failures = check_source()
    if failures:
        for failure in failures:
            print(f"::error::{failure}", file=sys.stderr)
        return 1
    print("SDK helper parity guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
