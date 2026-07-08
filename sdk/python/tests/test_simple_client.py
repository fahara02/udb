"""Chapter-10 + chapter-13 (Python) simple-client helper tests.

Covers the unified facade invoker (retry/typed errors), `storage.upload_file`
(register→PUT→finalize, no proof Get/List), bound entity/table, typed
WriteReceipt/ReadFence + golden-fixture parity, error-detail decode, optional
`is_public`, `login_and_adopt_tenant`, and the chapter-13 events/webrtc/
notification/asset/conformance/passkey wrappers — all on capture fakes (no live
broker).
"""

from __future__ import annotations

import json
from pathlib import Path

import grpc
import pytest

from udb_client import (
    Metadata,
    ReadFence,
    UdbConfig,
    UdbProject,
    WriteReceipt,
)
from udb_client.generated_client import UdbDetailedRpcError, _map_error

# Skip cleanly if the generated stubs are not present in this checkout.
_storage = pytest.importorskip("udb.core.storage.services.v1.storage_service_pb2")
_asset = pytest.importorskip("udb.core.asset.services.v1.asset_service_pb2")
_webrtc = pytest.importorskip("udb.core.webrtc.services.v1.webrtc_service_pb2")
_authn = pytest.importorskip("udb.core.authn.services.v1.core_pb2")
_apikey = pytest.importorskip("udb.core.apikey.services.v1.core_pb2")

# Repo-root golden fixture (lane 07-owned; READ it, never author a per-lang const).
_GOLDEN = Path(__file__).resolve().parents[3] / "docs" / "generated" / "consistency-golden.json"

# Cross-SDK workflow-sequence contract (single source of truth). Read it rather than
# carrying an inline per-test literal so a drift in the contract fails every language
# identically. Resolves the repo-root the same way bench_body_rows() does.
_WORKFLOW_SEQUENCES = (
    Path(__file__).resolve().parents[3] / "docs" / "bench-bodies" / "workflow-sequences.md"
)


def load_workflow_sequence(helper: str) -> list[str]:
    """Return the exact ordered RPC sequence for a workflow helper.

    Reads the `| helper | sequence |` table in docs/bench-bodies/workflow-sequences.md,
    keying on col1 (`Facade.method`) and splitting col2 on `,` (the literal token `PUT`
    denotes the presigned-URL byte transfer, not a gRPC call).
    """
    for line in _WORKFLOW_SEQUENCES.read_text(encoding="utf-8").splitlines():
        if not line.startswith("|"):
            continue
        parts = [part.strip() for part in line.strip().strip("|").split("|")]
        if len(parts) < 2 or parts[0] in ("helper", "---"):
            continue
        if parts[0] == helper:
            return [item.strip() for item in parts[1].split(",") if item.strip()]
    raise KeyError(f"no workflow-sequence row for helper {helper!r} in {_WORKFLOW_SEQUENCES}")


class _FakeUnary:
    """A captured-call gRPC unary method returning a canned response."""

    def __init__(self, response, *, raise_codes=()):
        self._response = response
        self.requests: list = []
        self.calls = 0
        self._raise_codes = list(raise_codes)

    def __call__(self, request, *, metadata=None, timeout=None):
        self.requests.append(request)
        self.calls += 1
        if self._raise_codes:
            code = self._raise_codes.pop(0)
            raise _FakeRpcError(code)
        return self._response


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code, detail_bytes=None):
        self._code = code
        self._detail = detail_bytes

    def code(self):
        return self._code

    def details(self):
        return "fake error"

    def trailing_metadata(self):
        if self._detail is not None:
            return (("udb-error-detail-bin", self._detail),)
        return ()


@pytest.fixture
def project() -> UdbProject:
    cfg = UdbConfig(
        target="unused:1",
        tenant_id="acme",
        project_id="billing",
        user_id="user-1",
        scopes=("udb:read", "udb:write"),
        correlation_id="corr-1",
    )
    proj = UdbProject(cfg)
    try:
        yield proj
    finally:
        proj.close()


# ── 10.1.1.x: facade routes through the generated retry/error layer ──────────
def test_storage_read_only_rpc_is_retried_then_typed(project: UdbProject) -> None:
    # GetFile is read_only → retried on UNAVAILABLE then succeeds.
    fake = _FakeUnary(
        _storage.GetFileResponse(), raise_codes=[grpc.StatusCode.UNAVAILABLE]
    )
    project.storage.stub.GetFile = fake
    # speed up backoff
    project.config.deadline = 5.0
    project.storage.get_file("f1")
    assert fake.calls == 2  # one retry


def test_storage_mutation_not_retried_and_typed_error(project: UdbProject) -> None:
    detail = _authn  # not used; just to keep import
    fake = _FakeUnary(
        _storage.RegisterUploadResponse(),
        raise_codes=[grpc.StatusCode.UNAVAILABLE],
    )
    project.storage.stub.RegisterUpload = fake
    with pytest.raises(UdbDetailedRpcError if False else Exception) as exc:
        project.storage.register_upload("f")
    # mutation → not retried (single call), mapped to a UdbRpcError subtype
    assert fake.calls == 1
    from udb_client import UdbRpcError

    assert isinstance(exc.value, UdbRpcError)


def test_send_notification_not_retried(project: UdbProject) -> None:
    # send_notification is mutation; route through _invoke (no raw stub call).
    pytest.importorskip("udb.core.notification.services.v1.core_pb2")
    fake = _FakeUnary(None, raise_codes=[grpc.StatusCode.UNAVAILABLE])
    project.notification.SendNotification = fake
    from udb_client import UdbRpcError

    with pytest.raises(UdbRpcError):
        project.send_notification("order.shipped", recipient_id="u1")
    assert fake.calls == 1


# ── 10.2.1.1 / 10.5.1.1: upload_file is exactly Register → PUT → Finalize ────
def test_upload_file_sequence_no_proof_reads(project: UdbProject) -> None:
    reg = _FakeUnary(
        _storage.RegisterUploadResponse(file_id="f9", upload_url="https://put.example/abc")
    )
    fin = _FakeUnary(_storage.FinalizeUploadResponse())
    getf = _FakeUnary(_storage.GetFileResponse())
    listf = _FakeUnary(_storage.ListFilesResponse())
    project.storage.stub.RegisterUpload = reg
    project.storage.stub.FinalizeUpload = fin
    project.storage.stub.GetFile = getf
    project.storage.stub.ListFiles = listf

    seq: list[str] = []

    def fake_put(url, data, content_type):
        seq.append("PUT")
        assert url == "https://put.example/abc"
        assert data == b"hello"

    # wrap stubs to record order
    orig_reg = reg.__call__
    orig_fin = fin.__call__

    def reg_rec(*a, **k):
        seq.append("RegisterUpload")
        return orig_reg(*a, **k)

    def fin_rec(*a, **k):
        seq.append("FinalizeUpload")
        return orig_fin(*a, **k)

    project.storage.stub.RegisterUpload = reg_rec
    project.storage.stub.FinalizeUpload = fin_rec

    project.storage.upload_file(
        "report.pdf", b"hello", {"content_type": "application/pdf"}, http_put=fake_put
    )

    assert seq == load_workflow_sequence("StorageFacade.uploadFile")
    assert getf.calls == 0 and listf.calls == 0
    # size_bytes wired from len(data)
    assert reg.requests[-1].size_bytes == 5


def test_upload_file_raises_when_no_upload_url(project: UdbProject) -> None:
    reg = _FakeUnary(_storage.RegisterUploadResponse(file_id="f9"))  # no upload_url
    project.storage.stub.RegisterUpload = reg
    from udb_client import UdbConfigurationError

    with pytest.raises(UdbConfigurationError):
        project.storage.upload_file("x", b"y", http_put=lambda *a: None)


# ── 10.2.2.1/.2: bound entity/table issues exactly one Upsert ───────────────
def test_entity_upsert_one_upsert_no_readback(project: UdbProject) -> None:
    from udb.entity.v1 import types_pb2

    captured: list = []
    seq: list[str] = []

    def fake_upsert(request, *, metadata=None, timeout=None):
        seq.append("Upsert")
        captured.append(request)
        return types_pb2.MutationResponse()

    project.data.stub.Upsert = fake_upsert
    ent = project.data.entity("acme.Invoice", key=["id"])
    ent.upsert({"id": "i1", "amount": 10})
    expected = load_workflow_sequence("Entity.upsert")
    assert seq == expected
    assert len(captured) == len(expected)
    req = captured[0]
    assert req.message_type == "acme.Invoice"
    assert list(req.conflict_fields) == ["id"]
    assert json.loads(req.record_json.decode()) == {"amount": 10, "id": "i1"}


def test_table_alias_select(project: UdbProject) -> None:
    from udb.entity.v1 import types_pb2

    calls: list = []

    def fake_select(request, *, metadata=None, timeout=None):
        calls.append(request)
        rs = types_pb2.RecordSet()
        rs.records_json.append(b'{"id":"i1"}')
        return rs

    def fake_list(*a, **k):
        raise AssertionError("no hidden List")

    project.data.stub.Select = fake_select
    rows = project.data.table("invoice").select(where={"id": "i1"})
    assert len(calls) == 1
    assert rows == [{"id": "i1"}]


# ── 10.3.1.1 / 10.3.1.5 golden: ReadFence.from_receipt matches the fixture ──
def test_readfence_from_receipt_matches_golden() -> None:
    golden = json.loads(_GOLDEN.read_text())
    wr = golden["write_receipt"]
    receipt = WriteReceipt(
        source_lsn=wr["source_lsn"],
        outbox_seq=wr["outbox_seq"],
        projection_task_ids=tuple(wr["projection_task_ids"]),
        manifest_checksum=wr["manifest_checksum"],
        written_at_unix_ms=wr["written_at_unix_ms"],
    )
    fence = ReadFence.from_receipt(receipt, golden["read_fence"]["max_wait_ms"])
    assert fence.to_dict() == golden["read_fence"]
    assert json.loads(fence.to_json()) == golden["read_fence"]
    # the load-bearing mapping
    assert fence.min_outbox_lsn == receipt.source_lsn


def test_readfence_skips_empty_fields() -> None:
    fence = ReadFence.from_receipt(WriteReceipt(), max_wait_ms=100)
    assert fence.to_dict() == {"max_wait_ms": 100}  # min_outbox_lsn/ids omitted


# ── 10.3.1.2/.3: after_write + receipt capture ──────────────────────────────
def test_after_write_maps_source_lsn() -> None:
    meta = Metadata(tenant_id="t", purpose="p", correlation_id="c")
    receipt = WriteReceipt(source_lsn="0/AA", projection_task_ids=("a",))
    fenced = meta.after_write(receipt)
    decoded = json.loads(fenced.read_fence_json)
    assert decoded["min_outbox_lsn"] == "0/AA"


def test_upsert_with_receipt_parses_field7(project: UdbProject) -> None:
    from udb.entity.v1 import types_pb2

    resp = types_pb2.MutationResponse(
        write_receipt_json=json.dumps({"source_lsn": "0/BB", "outbox_seq": 7})
    )

    def fake_upsert(request, *, metadata=None, timeout=None):
        return resp

    project.data.stub.Upsert = fake_upsert
    _r, receipt = project.data.upsert_with_receipt(
        message_type="m", record={"x": 1}
    )
    assert receipt.source_lsn == "0/BB"
    assert receipt.outbox_seq == 7


# ── 10.3.2.1: typed ErrorDetail decode + is_retryable()/kind() ──────────────
def test_error_detail_decoded_typed() -> None:
    from udb.entity.v1.error_pb2 import ErrorDetail, ErrorFieldViolation, ErrorKind

    ed = ErrorDetail(
        retryable=False,
        retry_after_ms=0,
        kind=ErrorKind.ERROR_KIND_VALIDATION,
        field_violations=[
            ErrorFieldViolation(field="email", description="must be a valid email")
        ],
    )
    err = _FakeRpcError(grpc.StatusCode.FAILED_PRECONDITION, ed.SerializeToString())
    wrapped = UdbDetailedRpcError("X", err, ed.SerializeToString())
    assert wrapped.is_retryable() is False
    assert wrapped.retry_after_ms == 0
    assert wrapped.kind() == "ERROR_KIND_VALIDATION"
    assert wrapped.field_violations == [
        {"field": "email", "description": "must be a valid email"}
    ]
    assert wrapped.detail == ed.SerializeToString()  # raw bytes preserved


def test_error_detail_quota_retry_after_decoded_typed() -> None:
    from udb.entity.v1.error_pb2 import ErrorDetail, ErrorKind

    ed = ErrorDetail(
        backend="admission",
        operation="tenant budget",
        retryable=True,
        retry_after_ms=250,
        kind=ErrorKind.ERROR_KIND_QUOTA,
    )
    err = _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, ed.SerializeToString())
    wrapped = UdbDetailedRpcError("X", err, ed.SerializeToString())
    assert wrapped.is_retryable() is True
    assert wrapped.retry_after_ms == 250
    assert wrapped.kind() == "ERROR_KIND_QUOTA"
    assert wrapped.field_violations == []
    assert wrapped.detail == ed.SerializeToString()


def test_transport_error_detail_synthesized_typed() -> None:
    err = _FakeRpcError(grpc.StatusCode.DEADLINE_EXCEEDED)
    wrapped = _map_error("X", err)
    assert isinstance(wrapped, UdbDetailedRpcError)
    assert wrapped.error_detail is not None
    assert wrapped.error_detail.backend == "transport"
    assert wrapped.error_detail.operation == "deadline_exceeded"
    assert wrapped.is_retryable() is True
    assert wrapped.retry_after_ms == 0
    assert wrapped.kind() == "ERROR_KIND_RETRYABLE"
    assert wrapped.field_violations == []


def test_cancelled_transport_error_detail_synthesized_not_retryable() -> None:
    err = _FakeRpcError(grpc.StatusCode.CANCELLED)
    wrapped = _map_error("X", err)
    assert isinstance(wrapped, UdbDetailedRpcError)
    assert wrapped.error_detail is not None
    assert wrapped.error_detail.backend == "transport"
    assert wrapped.error_detail.operation == "cancelled"
    assert wrapped.is_retryable() is False
    assert wrapped.retry_after_ms == 0
    assert wrapped.kind() == "ERROR_KIND_RETRYABLE"
    assert wrapped.field_violations == []


# ── 10.4.1.x: optional is_public stays unset when omitted ───────────────────
def test_register_upload_is_public_unset_by_default(project: UdbProject) -> None:
    fake = _FakeUnary(_storage.RegisterUploadResponse())
    project.storage.stub.RegisterUpload = fake
    project.storage.register_upload("f")
    assert fake.requests[-1].HasField("is_public") is False
    fake2 = _FakeUnary(_storage.RegisterUploadResponse())
    project.storage.stub.RegisterUpload = fake2
    project.storage.register_upload("f", is_public=True)
    assert fake2.requests[-1].HasField("is_public") is True
    assert fake2.requests[-1].is_public is True


def test_finalize_and_update_is_public_unset(project: UdbProject) -> None:
    fin = _FakeUnary(_storage.FinalizeUploadResponse())
    upd = _FakeUnary(_storage.UpdateFileResponse())
    project.storage.stub.FinalizeUpload = fin
    project.storage.stub.UpdateFile = upd
    project.storage.finalize_upload("id")
    project.storage.update_file("id")
    assert fin.requests[-1].HasField("is_public") is False
    assert upd.requests[-1].HasField("is_public") is False


# ── 10.2.3.1: login_and_adopt_tenant = [Login, Authenticate] then adopt ─────
def test_login_and_adopt_tenant_two_rpcs(project: UdbProject) -> None:
    seq: list[str] = []

    def fake_login(request, *, metadata=None, timeout=None):
        seq.append("Login")
        return _authn.LoginResponse(access_token="tok-123", user_id="user-1")

    def fake_auth(request, *, metadata=None, timeout=None):
        seq.append("Authenticate")
        resp = _authn.AuthnResponse()
        resp.principal.tenant_id = "tenant-uuid"
        resp.principal.project_id = "proj-x"
        return resp

    project.auth.authn.Login = fake_login
    project.auth.authn.Authenticate = fake_auth

    authn = project.login_and_adopt_tenant("alice", "pw")
    assert seq == ["Login", "Authenticate"]
    assert authn.principal.tenant_id == "tenant-uuid"
    # adopted across the shared metadata + bearer installed
    assert project.metadata.tenant_id == "tenant-uuid"
    assert project.metadata.project_id == "proj-x"
    assert project.metadata.bearer_token == "tok-123"


# ── 13.3.1.3: events ready() + publish_and_wait, no sleeps ──────────────────
def test_events_ready_and_publish_and_wait(project: UdbProject) -> None:
    from udb.entity.v1 import types_pb2
    from udb.events.v1 import udb_events_pb2

    env = udb_events_pb2.CDCEnvelope(event_id="evt-1", topic="t")

    class _Stream:
        def __init__(self):
            self._items = [env]

        def initial_metadata(self):
            return ()

        def __iter__(self):
            return iter(self._items)

    def fake_publishcdc(request, *, metadata=None, timeout=None):
        assert request.topic_pattern == "t"
        return _Stream()

    def fake_enqueue(request, *, metadata=None, timeout=None):
        return types_pb2.EnqueueOutboxEventResponse(event_id="evt-1", enqueued=True)

    project.data.stub.PublishCDC = fake_publishcdc
    project.data.stub.EnqueueOutboxEvent = fake_enqueue

    sub = project.events.subscribe("t")
    sub.ready()  # returns without a timer
    got = project.events.publish_and_wait("t", {"k": "v"}, subscription=sub)
    assert got.event_id == "evt-1"


# ── 13.4.1.3: join_session = one JoinSession; leave closes + LeaveRoom ──────
def test_join_session_one_rpc_and_leave(project: UdbProject) -> None:
    join_resp = _webrtc.JoinSessionResponse()
    join_resp.peer.peer_id = "p1"
    join_resp.peer.room_id = "r1"
    join = _FakeUnary(join_resp)
    leave = _FakeUnary(_webrtc.LeaveRoomResponse())
    join_room = _FakeUnary(_webrtc.JoinRoomResponse())
    issue = _FakeUnary(_webrtc.IssueCredentialsResponse())
    project.webrtc.peer.stub.JoinSession = join
    project.webrtc.peer.stub.LeaveRoom = leave
    project.webrtc.peer.stub.JoinRoom = join_room
    project.webrtc.turn.stub.IssueCredentials = issue

    session = project.webrtc.join_session("r1", display_name="Ada")
    assert join.calls == 1
    assert join_room.calls == 0 and issue.calls == 0
    assert session.peer_id == "p1"
    session.leave()
    assert leave.calls == 1


# ── 13.5.1.3: send_template one Send; wait_for_delivery reads only ──────────
def test_send_template_and_wait_for_delivery(project: UdbProject) -> None:
    _notif = pytest.importorskip("udb.core.notification.services.v1.core_pb2")
    _enums = pytest.importorskip("udb.core.notification.entity.v1.enums_pb2")

    send = _FakeUnary(_notif.SendNotificationResponse())
    project.notification.SendNotification = send
    project.send_template("order.shipped", "u1", {"n": "1"})
    assert send.calls == 1

    log = _notif.GetNotificationResponse()
    log.log.log_id = "lg1"
    log.log.status = _enums.NOTIFICATION_STATUS_DELIVERED
    getn = _FakeUnary(log)
    project.notification.GetNotification = getn
    final = project.wait_for_delivery("lg1", deadline_s=2.0)
    assert final.status == _enums.NOTIFICATION_STATUS_DELIVERED
    assert getn.calls == 1  # terminal on first read, no sleep loop


# ── 13.6.1.3: start_and_wait one StartPipeline + inline steps, no proof Get ─
def test_start_and_wait_reads_inline_steps(project: UdbProject) -> None:
    _enums = pytest.importorskip("udb.core.asset.entity.v1.enums_pb2")
    start_resp = _asset.StartPipelineResponse(instance_id="inst-1")
    start_resp.steps.add(step_id="s1")
    start = _FakeUnary(start_resp)
    get_resp = _asset.GetPipelineResponse()
    get_resp.instance.status = _enums.PIPELINE_STATUS_COMPLETED
    getp = _FakeUnary(get_resp)
    project.asset.stub.StartPipeline = start
    project.asset.stub.GetPipeline = getp

    s, last = project.asset.start_and_wait("def-1", "asset-1", deadline_s=2.0)
    assert start.calls == 1
    assert len(s.steps) == 1  # inline steps read from StartPipeline
    assert last.instance.status == _enums.PIPELINE_STATUS_COMPLETED
    assert getp.calls == 1


# ── 13.2.1.3: conformance_proof surfaces gated dev_otp_code / errors empty ──
def test_conformance_proof_otp(project: UdbProject) -> None:
    project.auth.authn.SendOTP = _FakeUnary(
        _authn.SendOTPResponse(otp_id="o1", dev_otp_code="123456")
    )
    assert project.auth.conformance_proof("otp") == "123456"


def test_conformance_proof_empty_raises(project: UdbProject) -> None:
    project.auth.authn.ForgotPassword = _FakeUnary(
        _authn.ForgotPasswordResponse(otp_id="o1")  # empty dev_otp_code
    )
    from udb_client import UdbConfigurationError

    with pytest.raises(UdbConfigurationError):
        project.auth.conformance_proof("password_reset")


# ── 13.2.2.3: passkeys register/authenticate = exactly 2 RPCs ───────────────
def test_passkeys_register_two_rpcs(project: UdbProject) -> None:
    seq: list[str] = []

    def start(request, *, metadata=None, timeout=None):
        seq.append("Start")
        return _authn.StartWebAuthnRegistrationResponse(challenge_id="ch1")

    def finish(request, *, metadata=None, timeout=None):
        seq.append("Finish")
        assert request.challenge_id == "ch1"
        return _authn.FinishWebAuthnRegistrationResponse(registered=True)

    project.auth.authn.StartWebAuthnRegistration = start
    project.auth.authn.FinishWebAuthnRegistration = finish
    res = project.auth.passkeys.register(user_id="u1")
    assert seq == ["Start", "Finish"]
    assert res.registered is True


# ── R2.2: canonical-name aliases emit EXACTLY their declared RPC ─────────────
def test_authz_allow_role_one_create_policy_rule(project: UdbProject) -> None:
    _authz = pytest.importorskip("udb.core.authz.services.v1.core_pb2")
    _authz_enums = pytest.importorskip("udb.core.authz.entity.v1.enums_pb2")
    create = _FakeUnary(_authz.CreatePolicyRuleResponse())
    listp = _FakeUnary(_authz.ListPolicyRulesResponse())
    getp = _FakeUnary(_authz.GetPolicyRuleResponse())
    project.auth.authz.CreatePolicyRule = create
    project.auth.authz.ListPolicyRules = listp
    project.auth.authz.GetPolicyRule = getp

    project.authz.allow_role("billing-admin", "acme.Invoice", "read")

    assert create.calls == 1
    assert listp.calls == 0 and getp.calls == 0  # no hidden get/list
    req = create.requests[-1]
    assert req.subject == "billing-admin"
    assert req.object == "acme.Invoice"
    assert req.action == "read"
    assert req.effect == _authz_enums.POLICY_EFFECT_ALLOW
    assert req.domain == "acme"  # tenant from bound metadata


def test_authz_bind_role_one_assign_role(project: UdbProject) -> None:
    _authz = pytest.importorskip("udb.core.authz.services.v1.core_pb2")
    assign = _FakeUnary(_authz.AssignRoleResponse())
    listr = _FakeUnary(_authz.ListUserRolesResponse())
    project.auth.authz.AssignRole = assign
    project.auth.authz.ListUserRoles = listr

    project.authz.bind_role("user-7", "billing-admin")

    assert assign.calls == 1
    assert listr.calls == 0  # no hidden get/list
    req = assign.requests[-1]
    assert req.user_id == "user-7"
    assert req.role_id == "billing-admin"
    assert req.domain == "acme"


def test_storage_download_file_one_get_download_url(project: UdbProject) -> None:
    geturl = _FakeUnary(
        _storage.GetDownloadUrlResponse(download_url="https://get.example/abc")
    )
    getf = _FakeUnary(_storage.GetFileResponse())
    listf = _FakeUnary(_storage.ListFilesResponse())
    project.storage.stub.GetDownloadUrl = geturl
    project.storage.stub.GetFile = getf
    project.storage.stub.ListFiles = listf

    resp = project.storage.download_file("f1")
    assert geturl.calls == 1
    assert getf.calls == 0 and listf.calls == 0  # no proof get/list
    assert resp.download_url == "https://get.example/abc"


def test_storage_download_file_fetch_uses_presigned_get_only(project: UdbProject) -> None:
    geturl = _FakeUnary(
        _storage.GetDownloadUrlResponse(download_url="https://get.example/abc")
    )
    project.storage.stub.GetDownloadUrl = geturl
    fetched: list[str] = []

    def fake_get(url: str) -> bytes:
        fetched.append(url)
        return b"file-bytes"

    data = project.storage.download_file("f1", fetch=True, http_get=fake_get)
    assert geturl.calls == 1  # still exactly one gRPC call
    assert fetched == ["https://get.example/abc"]
    assert data == b"file-bytes"


class _FakeServerStream:
    """A captured-call gRPC server-streaming method returning canned chunks."""

    def __init__(self, chunks):
        self._chunks = list(chunks)
        self.requests: list = []
        self.calls = 0

    def __call__(self, request, *, metadata=None, timeout=None):
        self.requests.append(request)
        self.calls += 1
        return iter(self._chunks)


def test_storage_download_file_stream_uses_downloadfile_and_reassembles(
    project: UdbProject,
) -> None:
    # stream=True bypasses the presigned URL and streams bytes THROUGH the broker
    # via the server-streaming DownloadFile RPC; chunk `data` is reassembled.
    chunks = [
        _storage.DownloadFileChunk(data=b"hello ", total_size=11, content_type="text/plain"),
        _storage.DownloadFileChunk(data=b"world"),
    ]
    dl = _FakeServerStream(chunks)
    geturl = _FakeUnary(
        _storage.GetDownloadUrlResponse(download_url="https://get.example/abc")
    )
    project.storage.stub.DownloadFile = dl
    project.storage.stub.GetDownloadUrl = geturl

    data = project.storage.download_file("f1", stream=True, chunk_size_bytes=4096)
    assert dl.calls == 1  # exactly one DownloadFile RPC
    assert geturl.calls == 0  # presigned path NOT used
    assert data == b"hello world"  # chunks reassembled in order
    req = dl.requests[0]
    assert req.file_id == "f1"
    assert req.chunk_size_bytes == 4096


def test_storage_download_stream_helper_reassembles(project: UdbProject) -> None:
    dl = _FakeServerStream(
        [_storage.DownloadFileChunk(data=b"ab"), _storage.DownloadFileChunk(data=b"c")]
    )
    project.storage.stub.DownloadFile = dl
    assert project.storage.download_stream("f9") == b"abc"
    assert dl.calls == 1


def test_storage_download_file_default_still_presigned(project: UdbProject) -> None:
    # Default (no fetch/stream) is unchanged: exactly one GetDownloadUrl, no DownloadFile.
    geturl = _FakeUnary(
        _storage.GetDownloadUrlResponse(download_url="https://get.example/abc")
    )
    dl = _FakeServerStream([])
    project.storage.stub.GetDownloadUrl = geturl
    project.storage.stub.DownloadFile = dl

    resp = project.storage.download_file("f1")
    assert geturl.calls == 1
    assert dl.calls == 0  # streaming RPC untouched on the presigned path
    assert resp.download_url == "https://get.example/abc"


def test_auth_login_session_no_rpc_then_wraps_tokensession(project: UdbProject) -> None:
    from udb_client.auth import TokenSession

    session = project.auth.login_session()
    assert isinstance(session, TokenSession)
    # The accessor itself issues no RPC; login is what calls AuthnService.Login.
    login = _FakeUnary(_authn.LoginResponse(access_token="tok-1"))
    project.auth.authn.Login = login
    session.login("alice", "pw")
    assert login.calls == 1


def test_project_login_session_delegates(project: UdbProject) -> None:
    from udb_client.auth import TokenSession

    assert isinstance(project.login_session(), TokenSession)


def test_read_fence_from_receipt_alias_matches_method() -> None:
    from udb_client import read_fence_from_receipt

    receipt = WriteReceipt(source_lsn="0/CC", projection_task_ids=("p1",))
    fn = read_fence_from_receipt(receipt, 1234)
    method = ReadFence.from_receipt(receipt, 1234)
    assert fn.to_dict() == method.to_dict()
    assert fn.min_outbox_lsn == "0/CC"


def test_project_connect_classmethod_alias() -> None:
    proj = UdbProject.connect(target="unused:1", tenant_id="acme")
    try:
        assert proj.metadata.tenant_id == "acme"
    finally:
        proj.close()
