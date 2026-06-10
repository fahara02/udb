"""Phase 7 M8 — storage / asset / webrtc facade wrappers (Python SDK).

Verifies that :class:`udb_client.UdbProject` exposes the new media sub-clients
and that each high-level wrapper builds the correct generated request message
with ``tenant_id`` / ``project_id`` populated from the shared :class:`Metadata`.

No live broker: a real (lazy) insecure channel is opened, then each facade's
stub method is swapped for a capture fake that records the outbound request and
returns a canned response, so the assertions run on the exact message the
wrapper would have put on the wire.
"""

from __future__ import annotations

import pytest

from udb_client import Metadata, UdbConfig, UdbProject

# Skip cleanly if the generated media stubs are not present in this checkout.
_storage = pytest.importorskip("udb.core.storage.services.v1.storage_service_pb2")
_asset = pytest.importorskip("udb.core.asset.services.v1.asset_service_pb2")
_webrtc = pytest.importorskip("udb.core.webrtc.services.v1.webrtc_service_pb2")


class _FakeUnary:
    """A captured-call gRPC unary method returning a canned response."""

    def __init__(self, response):
        self._response = response
        self.requests: list = []
        self.metadatas: list = []
        self.calls = 0

    def __call__(self, request, *, metadata=None, timeout=None):
        self.requests.append(request)
        self.metadatas.append(metadata)
        self.calls += 1
        return self._response


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


# ── facade exposure ─────────────────────────────────────────────────────────
def test_facade_exposes_storage_asset_webrtc(project: UdbProject) -> None:
    assert project.storage is not None
    assert project.asset is not None
    assert project.webrtc is not None
    # webrtc sub-clients
    for name in ("room", "peer", "track", "turn"):
        assert getattr(project.webrtc, name) is not None
    # raw stubs reachable as escape hatches
    assert project.storage.stub is not None
    assert project.asset.stub is not None
    assert project.webrtc.room.stub is not None
    assert project.webrtc.signaling_stub is not None
    # Wave-1 data-plane helpers stay reachable on .storage
    for helper in ("put_object", "get_object", "vector_upsert", "vector_search"):
        assert callable(getattr(project.storage, helper))


# ── storage wrappers ────────────────────────────────────────────────────────
def test_storage_register_upload_builds_request(project: UdbProject) -> None:
    fake = _FakeUnary(_storage.RegisterUploadResponse())
    project.storage.stub.RegisterUpload = fake

    project.storage.register_upload(
        "report.pdf", content_type="application/pdf", size_bytes=42
    )

    req = fake.requests[-1]
    assert isinstance(req, _storage.RegisterUploadRequest)
    assert req.tenant_id == "acme"
    assert req.project_id == "billing"
    assert req.filename == "report.pdf"
    assert req.content_type == "application/pdf"
    assert req.size_bytes == 42
    # shared metadata applied as gRPC call metadata
    assert fake.metadatas[-1] is not None


def test_storage_get_file_and_delete(project: UdbProject) -> None:
    get_fake = _FakeUnary(_storage.GetFileResponse())
    del_fake = _FakeUnary(_storage.DeleteFileResponse())
    project.storage.stub.GetFile = get_fake
    project.storage.stub.DeleteFile = del_fake

    project.storage.get_file("file-1")
    project.storage.delete_file("file-1")

    assert get_fake.requests[-1].file_id == "file-1"
    assert get_fake.requests[-1].tenant_id == "acme"
    assert del_fake.requests[-1].file_id == "file-1"


def test_storage_list_files(project: UdbProject) -> None:
    fake = _FakeUnary(_storage.ListFilesResponse())
    project.storage.stub.ListFiles = fake

    project.storage.list_files(reference_type="invoice", page=2, page_size=50)

    req = fake.requests[-1]
    assert req.tenant_id == "acme"
    assert req.reference_type == "invoice"
    assert req.page == 2
    assert req.page_size == 50


# ── asset wrappers ──────────────────────────────────────────────────────────
def test_asset_register_asset_builds_request(project: UdbProject) -> None:
    fake = _FakeUnary(_asset.RegisterAssetResponse())
    project.asset.stub.RegisterAsset = fake

    project.asset.register_asset(
        "file-9", name="hero", asset_metadata='{"k": "v"}'
    )

    req = fake.requests[-1]
    assert isinstance(req, _asset.RegisterAssetRequest)
    assert req.tenant_id == "acme"
    assert req.project_id == "billing"
    assert req.file_id == "file-9"
    assert req.name == "hero"
    assert req.metadata == '{"k": "v"}'


def test_asset_start_pipeline_fills_correlation_id(project: UdbProject) -> None:
    fake = _FakeUnary(_asset.StartPipelineResponse())
    project.asset.stub.StartPipeline = fake

    project.asset.start_pipeline("def-1", "asset-1")

    req = fake.requests[-1]
    assert req.definition_id == "def-1"
    assert req.asset_id == "asset-1"
    # falls back to shared metadata correlation id
    assert req.correlation_id == "corr-1"


# ── webrtc wrappers ─────────────────────────────────────────────────────────
def test_webrtc_room_create_builds_request(project: UdbProject) -> None:
    fake = _FakeUnary(_webrtc.CreateRoomResponse())
    project.webrtc.room.stub.CreateRoom = fake

    project.webrtc.room.create_room("standup", max_participants=8)

    req = fake.requests[-1]
    assert isinstance(req, _webrtc.CreateRoomRequest)
    assert req.tenant_id == "acme"
    assert req.name == "standup"
    assert req.max_participants == 8
    # created_by falls back to shared metadata user_id
    assert req.created_by == "user-1"


def test_webrtc_peer_join_room(project: UdbProject) -> None:
    fake = _FakeUnary(_webrtc.JoinRoomResponse())
    project.webrtc.peer.stub.JoinRoom = fake

    project.webrtc.peer.join_room(
        "room-1", display_name="Ada", peer_metadata='{"role": "host"}'
    )

    req = fake.requests[-1]
    assert req.tenant_id == "acme"
    assert req.room_id == "room-1"
    assert req.display_name == "Ada"
    assert req.metadata == '{"role": "host"}'


def test_webrtc_track_and_turn(project: UdbProject) -> None:
    pub = _FakeUnary(_webrtc.PublishTrackResponse())
    turn = _FakeUnary(_webrtc.IssueCredentialsResponse())
    project.webrtc.track.stub.PublishTrack = pub
    project.webrtc.turn.stub.IssueCredentials = turn

    project.webrtc.track.publish_track("room-1", "peer-1", label="cam")
    project.webrtc.turn.issue_credentials("room-1", "peer-1", ttl_seconds=600)

    pub_req = pub.requests[-1]
    assert pub_req.room_id == "room-1"
    assert pub_req.peer_id == "peer-1"
    assert pub_req.label == "cam"

    turn_req = turn.requests[-1]
    assert turn_req.tenant_id == "acme"
    assert turn_req.room_id == "room-1"
    assert turn_req.ttl_seconds == 600


# ── signaling stream helper ─────────────────────────────────────────────────
def test_webrtc_make_signal_and_stream(project: UdbProject) -> None:
    captured = {}

    def fake_signal(request_iterator, *, metadata=None, timeout=None):
        captured["requests"] = list(request_iterator)
        captured["metadata"] = metadata
        return iter(())  # empty response stream

    project.webrtc.signaling_stub.Signal = fake_signal

    msg = project.webrtc.make_signal(room_id="room-1", peer_id="peer-1", ping=True)
    assert isinstance(msg, _webrtc.SignalRequest)
    assert msg.tenant_id == "acme"
    assert msg.room_id == "room-1"
    assert msg.ping is True

    responses = list(project.webrtc.signal(iter([msg])))
    assert responses == []
    assert captured["requests"][0].room_id == "room-1"
    assert captured["metadata"] is not None
