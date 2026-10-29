#!/usr/bin/env python3
"""Prepare disposable, served benchmark inputs after each isolated CI DB reset.

Completion and acknowledgement consume state. Give them independent fixtures
instead of timing a nonexistent upload or a workflow changed by SignalWorkflow.
The JSON contains reference IDs, ETags and one service-account username, never
passwords or signed URLs (that account's password is the environment's
UDB_LIVE_PASSWORD). The identity-flow fixtures add single-use proofs for
disposable targets (one-time codes, invitation accept tokens, one step-up
token); none of them is a password, an access token or a refresh token.
"""

from __future__ import annotations

import base64
import datetime
import json
import os
import shutil
import subprocess
import time
import urllib.request
import uuid
from dataclasses import replace
from pathlib import Path

import grpc
from google.protobuf import timestamp_pb2

from bootstrap_benchmark_project_catalog import required_env
from udb.core.authn.entity.v1 import enums_pb2 as authn_enums
from udb.core.authn.services.v1 import authn_service_pb2 as authn_service_pb
from udb.core.authn.services.v1 import authn_service_pb2_grpc as authn_grpc
from udb.core.authn.services.v1 import core_pb2 as authn_pb
from udb.core.common.v1 import types_pb2 as common_pb
from udb.core.idp.entity.v1 import enums_pb2 as idp_enums
from udb.core.idp.services.v1 import core_pb2 as idp_pb
from udb.core.idp.services.v1 import identity_provider_service_pb2_grpc as idp_grpc
from udb.core.messaging.services.v1 import messaging_service_pb2 as messaging_pb
from udb.core.messaging.services.v1 import messaging_service_pb2_grpc as messaging_grpc
from udb.core.workflow.services.v1 import workflow_service_pb2 as workflow_pb
from udb.core.workflow.services.v1 import workflow_service_pb2_grpc as workflow_grpc
from udb.entity.v1 import admin_pb2, blob_pb2, cdc_pb2, outbox_pb2
from udb.services.v1 import data_broker_pb2_grpc
from udb_client.auth import UdbAuthClient
from udb_client.metadata import Metadata


#: The backups the GetMessagingBackup row reads and the DeleteMessagingBackup
#: row erases. The broker checks only the udb-mls envelope framing (magic +
#: minimum length) of an opaque backup.
MESSAGING_BACKUP_IDS = ("sdk-perf-backup", "sdk-perf-backup-delete")
MESSAGING_BACKUP_ENVELOPE = b"UDBMLSB1" + bytes(68)


def mint_messaging_fixture(tenant_id: str, subject: str) -> dict | None:
    """Run `udb messaging bench-fixture` (the udb-mls client crate) for the
    benchmark principal. None when the benchmark binary predates the command;
    the MLS rows then report their refusal instead of a success."""
    binary = os.getenv("UDB_BENCH_BIN", "").strip() or shutil.which("udb") or ""
    if not binary:
        print("messaging fixture skipped: no udb binary (set UDB_BENCH_BIN)", flush=True)
        return None
    result = subprocess.run(
        [binary, "messaging", "bench-fixture", "--tenant", tenant_id, "--subject", subject],
        capture_output=True, text=True, timeout=120, check=False)
    if result.returncode != 0:
        print(f"messaging fixture skipped: {binary} messaging bench-fixture exited "
              f"{result.returncode}: {result.stderr.strip()[-400:]}", flush=True)
        return None
    document = json.loads(result.stdout)
    if (document.get("schema") != "udb.messaging.bench-fixture.v1"
            or document.get("tenant_id") != tenant_id or document.get("subject_id") != subject):
        raise RuntimeError("messaging bench fixture names another tenant or subject")
    return document


def seed_messaging(channel, headers, tenant_id: str, subject: str) -> dict[str, str]:
    """Send the fixture's setup requests (the measured rows then each consume a
    target of their own) and store the backup the GetMessagingBackup row
    reads. Returns the `mls_*` bench-body fixtures."""
    messaging = messaging_grpc.MessagingServiceStub(channel)
    try:
        for backup_id in MESSAGING_BACKUP_IDS:
            messaging.PutMessagingBackup(messaging_pb.PutMessagingBackupRequest(
                tenant_id=tenant_id, subject_id=subject, backup_id=backup_id,
                ciphertext=MESSAGING_BACKUP_ENVELOPE), metadata=headers, timeout=20.0)
    except grpc.RpcError as exc:
        # A broker older than 0.6.0 serves no MessagingService at all.
        if exc.code() != grpc.StatusCode.UNIMPLEMENTED:
            raise
        print("messaging fixture skipped: the broker serves no MessagingService", flush=True)
        return {}
    document = mint_messaging_fixture(tenant_id, subject)
    if document is None:
        return {}
    raw = base64.b64decode
    setup = document["setup"]
    for device in setup["devices"]:
        messaging.RegisterMessagingDevice(messaging_pb.RegisterMessagingDeviceRequest(
            tenant_id=tenant_id, subject_id=subject, device_id=device["device_id"],
            signature_public_key=raw(device["signature_public_key"]),
            credential=raw(device["credential"])), metadata=headers, timeout=20.0)
    for stock in setup["key_packages"]:
        messaging.PublishKeyPackages(messaging_pb.PublishKeyPackagesRequest(
            tenant_id=tenant_id, device_id=stock["device_id"],
            key_packages=[raw(kp) for kp in stock["key_packages"]],
            last_resort_key_package=raw(stock["last_resort_key_package"])),
            metadata=headers, timeout=20.0)
    for group in setup["groups"]:
        messaging.CreateGroup(messaging_pb.CreateGroupRequest(
            tenant_id=tenant_id, group_id=group["group_id"],
            creator_device_id=group["creator_device_id"], group_info=raw(group["group_info"]),
            ratchet_tree=raw(group["ratchet_tree"])), metadata=headers, timeout=20.0)
    for commit in setup["commits"]:
        accepted = messaging.SubmitCommit(messaging_pb.SubmitCommitRequest(
            tenant_id=tenant_id, group_id=commit["group_id"],
            sender_device_id=commit["sender_device_id"], epoch=int(commit["epoch"]),
            commit=raw(commit["commit"]), welcome=raw(commit["welcome"]),
            client_message_id=commit["client_message_id"]), metadata=headers, timeout=20.0)
        if accepted.epoch != int(commit["epoch"]) + 1:
            raise RuntimeError("messaging setup commit was not applied")
    fixtures = document["fixtures"]
    if not all(isinstance(value, str) and value for value in fixtures.values()):
        raise RuntimeError("messaging bench fixture has an empty value")
    return dict(fixtures)


#: The home-tenant service accounts' grant: `udb:read` (what the bench tenant
#: approves) and the exchange scope ExchangeServiceTokenForTenant requires.
MEMBERSHIP_GRANT_SCOPES = ("udb:read", "udb:authn:exchange-tenant-token")
MEMBERSHIP_APPROVED_SCOPES = ("udb:read",)


def seed_memberships(channel, bench_headers, bench_tenant: str, bench_project: str) -> dict[str, str]:
    """Service accounts homed in ANOTHER tenant, for the four tenant-membership
    rows. The bench tenant is the membership TARGET, so its admin (the
    benchmark principal) approves; the home tenant's admin is a separate
    offline-bootstrapped principal (`UDB_LIVE_MEMBERSHIP_HOME_USERNAME`, password
    `UDB_LIVE_PASSWORD`). Three accounts, so no measured row consumes another's
    target:

    - `membership_user_id`: grant only; the measured Create approves it;
    - `membership_id`: a membership of a second account, the measured Revoke's target;
    - `membership_exchange_username`: a third account with an approved
      membership; the harnesses log in AS it (password `UDB_LIVE_PASSWORD`, never
      written here) to measure ExchangeServiceTokenForTenant with its home bearer.

    Empty when no home admin is configured or the broker predates 0.6.4."""
    home_username = os.getenv("UDB_LIVE_MEMBERSHIP_HOME_USERNAME", "").strip()
    home_code = os.getenv("UDB_LIVE_MEMBERSHIP_HOME_TENANT", "").strip()
    if not home_username or not home_code:
        print("membership fixture skipped: no UDB_LIVE_MEMBERSHIP_HOME_USERNAME/TENANT", flush=True)
        return {}
    password = required_env("UDB_LIVE_PASSWORD")
    home_meta = Metadata(tenant_id=home_code, project_id=bench_project,
                         purpose="ci.benchmark.fixtures.membership-home",
                         correlation_id=str(uuid.uuid4()), client_catalog_version="")
    with UdbAuthClient(required_env("UDB_AUTH_GRPC_TARGET"), home_meta, timeout=15.0) as auth:
        login = auth.login(home_username, password, device_name="ci-benchmark-membership-home")
        home = auth.authenticate_bearer(login.access_token).principal
    if not home.tenant_id or home.tenant_id == bench_tenant:
        raise RuntimeError("the membership home admin is not bound to a tenant of its own")
    home_project = home.project_id or bench_project
    home_headers = replace(home_meta, tenant_id=home.tenant_id, project_id=home_project,
                           bearer_token=login.access_token).to_grpc_metadata()
    authn = authn_grpc.AuthnServiceStub(channel)
    suffix = uuid.uuid4().hex[:12]

    def service_account(label: str) -> tuple[str, str]:
        username = f"sdk-perf-membership-{label}-{suffix}"
        user_id = authn.CreateUser(authn_pb.CreateUserRequest(
            username=username, email=f"{username}@example.invalid", password=password,
            tenant_id=home.tenant_id, project_id=home_project, full_name=f"SDK perf membership {label}",
            account_kind=authn_enums.ACCOUNT_KIND_SERVICE_ACCOUNT),
            metadata=home_headers, timeout=20.0).user.user_id
        authn.ChangeUserStatus(authn_pb.ChangeUserStatusRequest(
            user_id=user_id, new_status=authn_enums.USER_STATUS_ACTIVE,
            reason="benchmark membership fixture"), metadata=home_headers, timeout=20.0)
        authn.CreateServiceAccountGrant(authn_service_pb.CreateServiceAccountGrantRequest(
            tenant_id=home.tenant_id, user_id=user_id,
            service_identity=f"spiffe://bench/membership-{label}-{suffix}", project_id=home_project,
            approved_scopes=list(MEMBERSHIP_GRANT_SCOPES), reason="benchmark membership fixture"),
            metadata=home_headers, timeout=20.0)
        return username, user_id

    expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=30)

    def approve(user_id: str) -> str:
        membership = authn.CreateServiceAccountTenantMembership(
            authn_service_pb.CreateServiceAccountTenantMembershipRequest(
                target_tenant_id=bench_tenant, user_id=user_id, home_tenant_id=home.tenant_id,
                target_project_id=bench_project, approved_scopes=list(MEMBERSHIP_APPROVED_SCOPES),
                reason="benchmark membership fixture",
                expires_at=timestamp_pb2.Timestamp(seconds=int(expires.timestamp()))),
            metadata=bench_headers, timeout=20.0).membership
        if membership.status != "ACTIVE" or not membership.membership_id:
            raise RuntimeError("benchmark membership fixture is not ACTIVE")
        return membership.membership_id

    try:
        _create_username, create_user_id = service_account("create")
        _revoke_username, revoke_user_id = service_account("revoke")
        exchange_username, exchange_user_id = service_account("exchange")
        revoke_membership_id = approve(revoke_user_id)
        approve(exchange_user_id)
    except grpc.RpcError as exc:
        if exc.code() != grpc.StatusCode.UNIMPLEMENTED:
            raise
        print("membership fixture skipped: the broker serves no tenant memberships", flush=True)
        return {}
    return {
        "membership_user_id": create_user_id,
        "membership_home_tenant_id": home.tenant_id,
        "membership_expires_at": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "membership_id": revoke_membership_id,
        "membership_exchange_username": exchange_username,
        "membership_exchange_project_id": home_project,
    }


#: The bench tenant's sign-in policy. The measured PutTenantAuthPolicy row
#: (docs/bench-bodies/authn.md) writes exactly this policy again, so it can run
#: in any order relative to the rows that depend on it. `password_required` is
#: left unset (true): the correctness suites share this fixture and still expect
#: CreateUser to demand a password.
AUTH_POLICY_CHANNELS = ("SMS", "EMAIL")
AUTH_POLICY_MAGIC_LINK_BASE_URL = "https://app.udb-test.invalid/auth"
#: The step-up action CheckStepUp measures, with a rule whose max age (the
#: step-up token's lifetime) outlasts a full benchmark run.
STEP_UP_ACTION = "sdk.bench.step-up"
AUTH_POLICY_STEP_UP_RULES = json.dumps(
    [{"action": STEP_UP_ACTION, "max_age_seconds": 3600, "factors": ["RECOVERY_CODE", "OTP"]}],
    separators=(",", ":"))
#: The dev in-process OpenID provider the broker serves under
#: UDB_OIDC_TEST_MODE (never in production), its client and redirect URI.
OIDC_TEST_ISSUER = "https://oidc.udb-test.invalid"
OIDC_TEST_CLIENT_ID = "sdk-bench-oidc-client"
OIDC_TEST_REDIRECT_URI = "https://app.udb-test.invalid/oidc/callback"


def seed_identity_flows(channel, headers, tenant: str, project: str, subject: str) -> dict[str, str]:
    """Inputs for the 0.6.6 identity RPCs (tenant auth policy, passwordless
    sign-in, invitations, step-up, OIDC sign-in). Every single-use input gets a
    disposable target of its own so no measured row consumes another's:

    - the tenant auth policy (passwordless SMS+EMAIL, an https magic-link page,
      the `STEP_UP_ACTION` rule) is written first;
    - `login_otp_send_phone`: a person account SendLoginOtp sends a real code to;
    - `login_otp_id` / `login_otp_code`: a code already sent to a SECOND account
      (dev echo, `UDB_OTP_DEV_ECHO`), redeemed once by LoginWithOtp;
    - `invitation_{get,start,accept,revoke,resend}_*`: one EMAIL invitation per
      row; the accept one has a started acceptance and its code. Resend needs
      the last send to be a minute old: the reset script backdates
      `invitation_resend_id` (the broker's 60 s resend interval);
    - `step_up_token`: VerifyMfaChallenge's step-up token for the benchmark
      principal itself (a recovery-code challenge for `STEP_UP_ACTION`);
    - `oidc_provider_id` / `oidc_state`: a provider on the dev OpenID issuer and
      a started sign-in for FinishOidcLogin (needs UDB_OIDC_TEST_MODE).

    The OTP codes, accept tokens and the step-up token are single-use proofs for
    disposable fixtures (the step-up token is not an access token), never a
    password. Everything is skipped when the broker predates 0.6.6
    (UNIMPLEMENTED). A group that cannot be prepared (for OIDC: the dev provider
    is off) is reported and skipped, so only its own rows fail, naming the
    missing seed, instead of the whole SDK run."""
    authn = authn_grpc.AuthnServiceStub(channel)
    suffix = uuid.uuid4().hex[:12]
    scope = common_pb.RequestContext(tenant=common_pb.TenantContext(tenant_id=tenant, project_id=project))
    fixtures: dict[str, str] = {}
    try:
        authn.PutTenantAuthPolicy(authn_pb.PutTenantAuthPolicyRequest(
            tenant_id=tenant, passwordless_channels=list(AUTH_POLICY_CHANNELS),
            magic_link_base_url=AUTH_POLICY_MAGIC_LINK_BASE_URL,
            step_up_rules_json=AUTH_POLICY_STEP_UP_RULES), metadata=headers, timeout=20.0)
    except grpc.RpcError as exc:
        if exc.code() != grpc.StatusCode.UNIMPLEMENTED:
            raise
        print("identity fixture skipped: the broker serves no tenant auth policy", flush=True)
        return {}

    def person(label: str) -> tuple[str, str]:
        """An ACTIVE person account with an email and an (unverified) phone."""
        username = f"sdk-perf-{label}-{suffix}"
        phone = f"+1555{uuid.uuid4().int % 10 ** 7:07d}"
        user_id = authn.CreateUser(authn_pb.CreateUserRequest(
            username=username, email=f"{username}@example.invalid",
            password=required_env("UDB_LIVE_PASSWORD"), tenant_id=tenant, project_id=project,
            full_name=f"SDK perf {label}", phone=phone,
            account_kind=authn_enums.ACCOUNT_KIND_PERSON), metadata=headers, timeout=20.0).user.user_id
        authn.ChangeUserStatus(authn_pb.ChangeUserStatusRequest(
            user_id=user_id, new_status=authn_enums.USER_STATUS_ACTIVE,
            reason="benchmark identity fixture", context=scope), metadata=headers, timeout=20.0)
        return user_id, phone

    def prepare(group: str, seed) -> None:
        try:
            seed()
        except (grpc.RpcError, RuntimeError) as exc:
            detail = f"{exc.code().name}: {exc.details()}" if isinstance(exc, grpc.RpcError) else str(exc)
            print(f"identity fixture group {group} skipped: {detail}", flush=True)

    def passwordless() -> None:
        # SendLoginOtp measures one account; LoginWithOtp redeems a code
        # already sent to a second one, so the measured send cannot supersede it.
        _send_user, send_phone = person("login-otp-send")
        fixtures["login_otp_send_phone"] = send_phone
        _login_user, login_phone = person("login-otp")
        sent = authn.SendLoginOtp(authn_pb.SendLoginOtpRequest(
            tenant_hint=tenant, channel="SMS", address=login_phone, project_hint=project),
            metadata=headers, timeout=20.0)
        if not sent.dev_otp_code:
            raise RuntimeError("SendLoginOtp echoed no dev code (UDB_OTP_DEV_ECHO off?)")
        fixtures["login_otp_id"] = sent.otp_id
        fixtures["login_otp_code"] = sent.dev_otp_code

    # Invitations: one disposable EMAIL invitation per measured row.
    def invite(label: str) -> tuple[str, str]:
        created = authn.CreateInvitation(authn_pb.CreateInvitationRequest(
            tenant_id=tenant, project_id=project, channel="EMAIL",
            address=f"sdk-perf-invite-{label}-{suffix}@example.invalid",
            full_name=f"SDK perf invitee {label}", expires_in_seconds=86400),
            metadata=headers, timeout=20.0)
        if not created.invitation.invitation_id or not created.accept_token:
            raise RuntimeError(f"invitation fixture {label} returned no id or token")
        return created.invitation.invitation_id, created.accept_token

    def invitations() -> None:
        for label in ("get", "start"):
            invitation_id, token = invite(label)
            fixtures[f"invitation_{label}_id"] = invitation_id
            fixtures[f"invitation_{label}_token"] = token
        fixtures["invitation_revoke_id"] = invite("revoke")[0]
        fixtures["invitation_resend_id"] = invite("resend")[0]
        accept_id, accept_token = invite("accept")
        started = authn.StartInvitationAcceptance(authn_pb.StartInvitationAcceptanceRequest(
            invitation_id=accept_id, token=accept_token), metadata=headers, timeout=20.0)
        if not started.otp_id or not started.dev_otp_code:
            raise RuntimeError("StartInvitationAcceptance echoed no dev code (UDB_OTP_DEV_ECHO off?)")
        fixtures.update({
            "invitation_accept_id": accept_id, "invitation_accept_token": accept_token,
            "invitation_accept_otp_id": started.otp_id,
            "invitation_accept_otp_code": started.dev_otp_code,
        })

    def step_up() -> None:
        # The benchmark principal proves a recovery code for STEP_UP_ACTION on
        # its own session, so CheckStepUp (called by that same user) is satisfied.
        codes = authn.GenerateRecoveryCodes(authn_pb.GenerateRecoveryCodesRequest(
            user_id=subject, count=4), metadata=headers, timeout=20.0).codes
        challenge = authn.IssueMfaChallenge(authn_pb.IssueMfaChallengeRequest(
            user_id=subject, factor_kind=authn_enums.AUTH_FACTOR_KIND_RECOVERY_CODE,
            purpose=authn_enums.MFA_CHALLENGE_PURPOSE_SENSITIVE_OPERATION, action=STEP_UP_ACTION,
            context=scope), metadata=headers, timeout=20.0)
        verified = authn.VerifyMfaChallenge(authn_pb.VerifyMfaChallengeRequest(
            challenge_id=challenge.challenge_id, code=codes[0], context=scope),
            metadata=headers, timeout=20.0)
        if not verified.verified or not verified.step_up_token:
            raise RuntimeError("VerifyMfaChallenge returned no step-up token")
        fixtures["step_up_token"] = verified.step_up_token

    def oidc() -> None:
        # A provider on the broker's dev OpenID issuer. StartOidcLogin answers
        # UNAVAILABLE when the dev provider is off (no UDB_OIDC_TEST_MODE).
        provider_id = idp_grpc.IdentityProviderServiceStub(channel).CreateProvider(
            idp_pb.CreateProviderRequest(
                tenant_id=tenant, kind=idp_enums.IDP_KIND_OIDC,
                display_name=f"SDK perf dev OIDC {suffix}", issuer=OIDC_TEST_ISSUER,
                client_ids=[OIDC_TEST_CLIENT_ID], redirect_uris=[OIDC_TEST_REDIRECT_URI],
                claim_mapping_json="{}",
                jit_policy_json=json.dumps({"require_verified_email": True, "default_project": project}),
                account_linking_policy="explicit", enabled=True, created_by=subject, context=scope),
            metadata=headers, timeout=20.0).provider.provider_id
        started = authn.StartOidcLogin(authn_pb.StartOidcLoginRequest(
            tenant_hint=tenant, provider_id=provider_id, redirect_uri=OIDC_TEST_REDIRECT_URI),
            metadata=headers, timeout=20.0)
        fixtures["oidc_provider_id"] = provider_id
        fixtures["oidc_state"] = started.state

    prepare("passwordless", passwordless)
    prepare("invitations", invitations)
    prepare("step-up", step_up)
    prepare("oidc", oidc)
    return fixtures


def main() -> int:
    output = Path(required_env("UDB_BENCH_FIXTURES"))
    metadata = Metadata(
        tenant_id=required_env("UDB_LIVE_TENANT"),
        project_id=required_env("UDB_LIVE_PROJECT"),
        purpose="ci.benchmark.fixtures",
        correlation_id=str(uuid.uuid4()),
        client_catalog_version="",
    )
    with UdbAuthClient(required_env("UDB_AUTH_GRPC_TARGET"), metadata, timeout=15.0) as auth:
        login = auth.login(required_env("UDB_LIVE_USERNAME"), required_env("UDB_LIVE_PASSWORD"),
                           device_name="ci-benchmark-fixtures")
        principal = auth.authenticate_bearer(login.access_token).principal
        if not principal.tenant_id or principal.project_id != metadata.project_id:
            raise RuntimeError("benchmark fixture principal is not bound to the target project")
    metadata = replace(metadata, tenant_id=principal.tenant_id, bearer_token=login.access_token)
    headers = metadata.to_grpc_metadata()
    context = metadata.to_request_context()
    with grpc.insecure_channel(required_env("UDB_GRPC_TARGET")) as data_channel, \
            grpc.insecure_channel(required_env("UDB_AUTH_GRPC_TARGET")) as native_channel:
        broker = data_broker_pb2_grpc.DataBrokerStub(data_channel)
        workflow = workflow_grpc.WorkflowServiceStub(native_channel)
        # ACK measures a real retained event in this exact verified scope.
        # Open the stream first and wait for its response headers so the event
        # cannot race a fresh subscription's journal anchor.
        ack_topic = "udb.sdk.benchmark.ack"
        ack_document = str(uuid.uuid4())
        events = broker.PublishCDC(cdc_pb2.CDCSubscriptionRequest(
            context=context, topic_pattern=ack_topic), metadata=headers, timeout=30.0)
        try:
            events.initial_metadata()
            event = broker.EnqueueOutboxEvent(outbox_pb2.EnqueueOutboxEventRequest(
                context=context, topic=ack_topic, partition_key=ack_document,
                payload={
                    "event_id": str(uuid.uuid4()), "event_type": ack_topic,
                    "correlation_id": str(uuid.uuid4()), "document_id": ack_document,
                    "tenant_id": principal.tenant_id, "project_id": principal.project_id,
                    "envelope_version": 2,
                }), metadata=headers, timeout=15.0)
            if not event.enqueued or not event.event_id:
                raise RuntimeError("CDC acknowledgement fixture was not enqueued")
            for envelope in events:
                if envelope.event_id != event.event_id:
                    continue
                payload = json.loads(envelope.payload_json)
                if (envelope.topic != ack_topic or payload.get("tenant_id") != principal.tenant_id
                        or payload.get("project_id") != principal.project_id):
                    raise RuntimeError("CDC acknowledgement event has a foreign scope")
                break
            else:
                raise RuntimeError("CDC acknowledgement event was not delivered")
            ack_event_id = event.event_id
        finally:
            events.cancel()
        bucket = os.getenv("UDB_LIVE_S3_BUCKET", "udb-live-sdk")
        broker.EnsureResource(admin_pb2.ResourceAdminRequest(
            context=context, backend="minio", resource_name=bucket, spec_json="{}"),
            metadata=headers, timeout=20.0)
        object_key = f"benchmark-multipart/{uuid.uuid4()}.txt"
        upload = broker.InitiateMultipartUpload(blob_pb2.MultipartUploadRequest(
            context=context, bucket=bucket, object_key=object_key,
            content_type="text/plain", part_count=1, ttl_seconds=3600),
            metadata=headers, timeout=20.0)
        if not upload.upload_id or len(upload.part_urls) != 1:
            raise RuntimeError("multipart fixture initiation returned no upload or part URL")
        data = b"UDB benchmark multipart fixture\n"
        try:
            request = urllib.request.Request(upload.part_urls[0], data=data, method="PUT",
                                             headers={"Content-Type": "text/plain"})
            with urllib.request.urlopen(request, timeout=20.0) as response:
                etag = response.headers.get("ETag", "").strip().strip('"')
                if response.status != 200 or not etag:
                    raise RuntimeError("multipart fixture PUT returned no ETag")
        except Exception:
            broker.AbortMultipartUpload(blob_pb2.AbortMultipartUploadRequest(
                context=context, bucket=bucket, object_key=object_key, upload_id=upload.upload_id),
                metadata=headers, timeout=20.0)
            raise RuntimeError("multipart fixture part upload failed") from None
        instance = workflow.StartWorkflow(workflow_pb.StartWorkflowRequest(
            tenant_id=principal.tenant_id, project_id=principal.project_id,
            workflow_type="sdk.perf.ack", total_steps=1, payload="{}", compensations="[]",
            correlation_id=str(uuid.uuid4())), metadata=headers, timeout=20.0)
        if not instance.workflow_id:
            raise RuntimeError("acknowledgement fixture returned no workflow ID")
        deadline = time.monotonic() + 30.0
        while True:
            current = workflow.GetWorkflow(workflow_pb.GetWorkflowRequest(
                tenant_id=principal.tenant_id, workflow_id=instance.workflow_id),
                metadata=headers, timeout=10.0).workflow
            if json.loads(current.payload or "{}").get("awaiting_ack_step") == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("acknowledgement fixture was not dispatched within 30 seconds")
            time.sleep(0.1)
        messaging_fixtures = seed_messaging(
            native_channel, headers, principal.tenant_id, principal.subject)
        membership_fixtures = seed_memberships(
            native_channel, headers, principal.tenant_id, principal.project_id)
        identity_fixtures = seed_identity_flows(
            native_channel, headers, principal.tenant_id, principal.project_id, principal.subject)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps({
            "schema_version": 1, "tenant_id": principal.tenant_id, "project_id": principal.project_id,
            "fixtures": {
                "multipart_bucket": bucket, "multipart_object_key": object_key,
                "multipart_upload_id": upload.upload_id, "multipart_etag": etag,
                "ack_workflow_id": instance.workflow_id,
                "cdc_ack_event_id": ack_event_id, "cdc_ack_topic": ack_topic,
                **messaging_fixtures,
                **membership_fixtures,
                **identity_fixtures,
            },
        }, sort_keys=True) + "\n", encoding="utf-8")
    print("Prepared real CDC event, multipart, dispatched workflow, messaging, tenant-membership"
          f" and identity-flow benchmark fixtures ({len(messaging_fixtures)} MLS values, "
          f"{len(membership_fixtures)} membership values, {len(identity_fixtures)} identity values)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
