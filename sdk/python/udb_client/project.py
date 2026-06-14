"""Unified UDB facade (Phase 7 SDK ergonomics).

:class:`UdbProject` ties the hand-written DataBroker client
(:class:`udb_client.client.UdbClient`), the auth ergonomics
(:class:`udb_client.auth.UdbAuthClient` + :class:`AuthzCache`), and the native
control-plane services (apikey / tenant / notification / analytics) behind one
shared :class:`Metadata` and one :class:`UdbConfig`.

Design notes
------------
* One config, many channels. The DataBroker, AuthN/AuthZ and the native
  control-plane services may live behind different listeners (the broker on
  ``target``; the auth + control plane on ``auth_target``). The facade opens at
  most two channels and shares them across the sub-clients.
* Every sub-client receives the SAME :class:`Metadata`, so tenant / identity /
  scope / purpose context is consistent across data, authz and control-plane
  calls. Rebinding via :meth:`UdbProject.bind_metadata` updates all of them.
* Raw clients stay reachable as escape hatches: ``.data``, ``.auth`` and the
  per-service stub accessors expose the underlying generated stubs.
* Sub-clients are only wired when their generated stubs exist in this checkout.
  The StorageService, AssetService and WebRTC (Room/Peer/Track/Turn/Signaling)
  stubs are now generated, so ``.storage`` exposes the StorageService file
  lifecycle (with the Wave-1 object/vector data-plane helpers kept reachable as
  escape hatches), ``.asset`` exposes the AssetService pipeline lifecycle, and
  ``.webrtc`` exposes the Room/Peer/Track/Turn sub-clients plus a thin
  ``Signal`` bidi-stream helper.

Async parity: :func:`create_udb_async` / :class:`UdbAsyncProject` mirror the
data-plane surface using :class:`udb_client.client.UdbAsyncClient`. The control
plane stubs are synchronous gRPC stubs, so the async facade exposes the data
helpers and shares config/metadata; auth + control-plane calls remain on the
sync clients (documented on the class).
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Sequence

import grpc

from ._channel import merge_channel_options
from .auth import AuthzCache, UdbAuthClient, as_resource
from .client import UdbAsyncClient, UdbClient
from .exceptions import UdbConfigurationError
from .metadata import Metadata

# Native control-plane stubs + request messages. Imported defensively so a
# checkout missing a particular generated service simply omits that sub-client
# rather than breaking the whole facade import.
try:  # pragma: no cover - presence depends on codegen having run
    from udb.core.apikey.services.v1 import apikey_service_pb2_grpc as _apikey_grpc
    from udb.core.apikey.services.v1 import core_pb2 as _apikey

    _HAS_APIKEY = True
except ImportError:  # pragma: no cover
    _HAS_APIKEY = False

try:  # pragma: no cover
    from udb.core.tenant.services.v1 import tenant_service_pb2 as _tenant
    from udb.core.tenant.services.v1 import tenant_service_pb2_grpc as _tenant_grpc

    _HAS_TENANT = True
except ImportError:  # pragma: no cover
    _HAS_TENANT = False

try:  # pragma: no cover
    from udb.core.notification.services.v1 import core_pb2 as _notif
    from udb.core.notification.services.v1 import notification_service_pb2_grpc as _notif_grpc

    _HAS_NOTIFICATION = True
except ImportError:  # pragma: no cover
    _HAS_NOTIFICATION = False

try:  # pragma: no cover
    from udb.core.analytics.services.v1 import analytics_service_pb2_grpc as _analytics_grpc

    _HAS_ANALYTICS = True
except ImportError:  # pragma: no cover
    _HAS_ANALYTICS = False

try:  # pragma: no cover
    from udb.core.storage.services.v1 import storage_service_pb2 as _storage
    from udb.core.storage.services.v1 import storage_service_pb2_grpc as _storage_grpc

    _HAS_STORAGE = True
except ImportError:  # pragma: no cover
    _HAS_STORAGE = False

try:  # pragma: no cover
    from udb.core.asset.services.v1 import asset_service_pb2 as _asset
    from udb.core.asset.services.v1 import asset_service_pb2_grpc as _asset_grpc

    _HAS_ASSET = True
except ImportError:  # pragma: no cover
    _HAS_ASSET = False

try:  # pragma: no cover
    from udb.core.webrtc.services.v1 import webrtc_service_pb2 as _webrtc
    from udb.core.webrtc.services.v1 import webrtc_service_pb2_grpc as _webrtc_grpc

    _HAS_WEBRTC = True
except ImportError:  # pragma: no cover
    _HAS_WEBRTC = False


@dataclass
class UdbConfig:
    """Shared connection + request configuration for :class:`UdbProject`.

    ``target`` is the DataBroker listener. ``auth_target`` is the AuthN/AuthZ +
    native control-plane listener; it defaults to ``target`` when unset (single
    process deployments).
    """

    target: str
    auth_target: str = ""
    # WebRTC signalling/peer endpoint. Defaults to ``auth_target`` (the
    # native control-plane listener that also serves Room/Peer/Track/Turn).
    # When set and distinct, the ``.webrtc`` facade dials a dedicated channel.
    webrtc_target: str = ""

    # Request context (becomes the shared Metadata).
    tenant_id: str = ""
    project_id: str = "default"
    user_id: str = ""
    service_identity: str = "python.app"
    purpose: str = "python.request"
    correlation_id: str = ""
    scopes: Sequence[str] = field(default_factory=tuple)
    bearer_token: str = ""
    api_key: str = ""

    # Transport.
    tls: bool = False
    root_certificates: bytes | None = None
    deadline: float | None = 30.0  # per-call timeout, seconds
    channel_options: Sequence[tuple[str, Any]] | None = None

    # HMAC secret for verifying signed policy bundles (M5.3). When set, the auth
    # client verifies every SignedPolicyBundle it fetches before returning it.
    policy_bundle_secret: str | bytes | None = None

    # Retry knobs are surfaced for parity with other SDK config objects; the
    # generated robustness layer (generated_client.RetryPolicy) owns the actual
    # retry behaviour, so this is advisory metadata for callers that build one.
    retry: Any = None

    def metadata(self) -> Metadata:
        """Build the shared :class:`Metadata` from this config."""
        return Metadata(
            tenant_id=self.tenant_id,
            purpose=self.purpose,
            correlation_id=self.correlation_id,
            scopes=tuple(self.scopes),
            service_identity=self.service_identity,
            user_id=self.user_id,
            project_id=self.project_id,
            bearer_token=self.bearer_token,
            api_key=self.api_key,
        )

    def effective_auth_target(self) -> str:
        return self.auth_target or self.target

    def effective_webrtc_target(self) -> str:
        return self.webrtc_target or self.effective_auth_target()


class _StorageFacade:
    """``project.storage`` — StorageService file lifecycle + data-plane helpers.

    Primary surface is the generated ``StorageService`` file lifecycle
    (register/finalize upload, download URL, get/update/delete/list files) when
    its stubs exist in this checkout. The Wave-1 DataBroker object + vector
    streaming helpers stay reachable on the same namespace as escape hatches
    (``put_object`` / ``get_object`` / ``generate_presigned_url`` /
    ``vector_*``) so existing callers keep working.

    ``tenant_id`` / ``project_id`` default from the shared :class:`Metadata`;
    the underlying stub is reachable as :attr:`stub` and per-call metadata is
    applied via the owning project's ``_ctl_metadata``.
    """

    def __init__(
        self,
        data: UdbClient | UdbAsyncClient,
        *,
        stub: Any = None,
        owner: "UdbProject | None" = None,
    ):
        self._data = data
        self.stub = stub
        self._owner = owner

    # ── data-plane escape hatches (Wave 1) ────────────────────────────────
    def put_object(self, *args: Any, **kwargs: Any) -> Any:
        return self._data.put_object(*args, **kwargs)

    def get_object(self, *args: Any, **kwargs: Any) -> Any:
        return self._data.get_object(*args, **kwargs)

    def generate_presigned_url(self, *args: Any, **kwargs: Any) -> Any:
        return self._data.generate_presigned_url(*args, **kwargs)

    def vector_upsert(self, *args: Any, **kwargs: Any) -> Any:
        return self._data.vector_upsert(*args, **kwargs)

    def vector_search(self, *args: Any, **kwargs: Any) -> Any:
        return self._data.vector_search(*args, **kwargs)

    def vector_hybrid_search(self, *args: Any, **kwargs: Any) -> Any:
        return self._data.vector_hybrid_search(*args, **kwargs)

    # ── StorageService file lifecycle ─────────────────────────────────────
    def _call(self, rpc: str, request: Any, metadata: Metadata | None) -> Any:
        if self.stub is None or self._owner is None:
            raise UdbConfigurationError("StorageService stubs are not generated")
        return getattr(self.stub, rpc)(
            request,
            metadata=self._owner._ctl_metadata(metadata),
            timeout=self._owner.config.deadline,
        )

    def _ids(self, metadata: Metadata | None) -> tuple[str, str]:
        meta = metadata or (self._owner._metadata if self._owner else None)
        if meta is None:
            return "", ""
        return meta.tenant_id, meta.project_id

    def register_upload(
        self,
        filename: str,
        *,
        content_type: str = "",
        file_type: str = "",
        reference_id: str = "",
        reference_type: str = "",
        is_public: bool = False,
        expires_in_minutes: int = 0,
        size_bytes: int = 0,
        metadata: Metadata | None = None,
    ) -> Any:
        """``StorageService.RegisterUpload`` — reserve a presigned PUT slot."""
        tenant_id, project_id = self._ids(metadata)
        request = _storage.RegisterUploadRequest(
            tenant_id=tenant_id,
            project_id=project_id,
            filename=filename,
            content_type=content_type,
            file_type=file_type,
            reference_id=reference_id,
            reference_type=reference_type,
            is_public=is_public,
            expires_in_minutes=expires_in_minutes,
            size_bytes=size_bytes,
        )
        return self._call("RegisterUpload", request, metadata)

    def finalize_upload(
        self,
        file_id: str,
        *,
        content_type: str = "",
        file_type: str = "",
        reference_id: str = "",
        reference_type: str = "",
        is_public: bool = False,
        size_bytes: int = 0,
        metadata: Metadata | None = None,
    ) -> Any:
        """``StorageService.FinalizeUpload`` — mark a reserved upload complete."""
        tenant_id, _ = self._ids(metadata)
        request = _storage.FinalizeUploadRequest(
            tenant_id=tenant_id,
            file_id=file_id,
            content_type=content_type,
            file_type=file_type,
            reference_id=reference_id,
            reference_type=reference_type,
            is_public=is_public,
            size_bytes=size_bytes,
        )
        return self._call("FinalizeUpload", request, metadata)

    def get_download_url(
        self,
        file_id: str,
        *,
        expires_in_minutes: int = 0,
        metadata: Metadata | None = None,
    ) -> Any:
        """``StorageService.GetDownloadUrl`` — presigned GET for a stored file."""
        tenant_id, _ = self._ids(metadata)
        request = _storage.GetDownloadUrlRequest(
            tenant_id=tenant_id,
            file_id=file_id,
            expires_in_minutes=expires_in_minutes,
        )
        return self._call("GetDownloadUrl", request, metadata)

    def get_file(self, file_id: str, *, metadata: Metadata | None = None) -> Any:
        """``StorageService.GetFile`` — file metadata record."""
        tenant_id, _ = self._ids(metadata)
        request = _storage.GetFileRequest(tenant_id=tenant_id, file_id=file_id)
        return self._call("GetFile", request, metadata)

    def update_file(
        self,
        file_id: str,
        *,
        filename: str = "",
        content_type: str = "",
        file_type: str = "",
        reference_id: str = "",
        reference_type: str = "",
        is_public: bool = False,
        metadata: Metadata | None = None,
    ) -> Any:
        """``StorageService.UpdateFile`` — mutate file metadata."""
        tenant_id, _ = self._ids(metadata)
        request = _storage.UpdateFileRequest(
            tenant_id=tenant_id,
            file_id=file_id,
            filename=filename,
            content_type=content_type,
            file_type=file_type,
            reference_id=reference_id,
            reference_type=reference_type,
            is_public=is_public,
        )
        return self._call("UpdateFile", request, metadata)

    def delete_file(self, file_id: str, *, metadata: Metadata | None = None) -> Any:
        """``StorageService.DeleteFile`` — delete file + backing object."""
        tenant_id, _ = self._ids(metadata)
        request = _storage.DeleteFileRequest(tenant_id=tenant_id, file_id=file_id)
        return self._call("DeleteFile", request, metadata)

    def list_files(
        self,
        *,
        file_type: str = "",
        reference_id: str = "",
        reference_type: str = "",
        uploaded_by: str = "",
        page: int = 0,
        page_size: int = 0,
        metadata: Metadata | None = None,
    ) -> Any:
        """``StorageService.ListFiles`` — paginated file listing."""
        tenant_id, _ = self._ids(metadata)
        request = _storage.ListFilesRequest(
            tenant_id=tenant_id,
            file_type=file_type,
            reference_id=reference_id,
            reference_type=reference_type,
            uploaded_by=uploaded_by,
            page=page,
            page_size=page_size,
        )
        return self._call("ListFiles", request, metadata)


class _AssetFacade:
    """``project.asset`` — AssetService pipeline + asset lifecycle.

    Wraps the generated ``AssetService`` stub. ``tenant_id`` / ``project_id``
    default from the shared :class:`Metadata`. The raw stub stays reachable as
    :attr:`stub`.
    """

    def __init__(self, stub: Any, owner: "UdbProject"):
        self.stub = stub
        self._owner = owner

    def _call(self, rpc: str, request: Any, metadata: Metadata | None) -> Any:
        if self.stub is None:
            raise UdbConfigurationError("AssetService stubs are not generated")
        return getattr(self.stub, rpc)(
            request,
            metadata=self._owner._ctl_metadata(metadata),
            timeout=self._owner.config.deadline,
        )

    def create_pipeline_definition(
        self,
        name: str,
        *,
        description: str = "",
        media_type: str = "",
        steps: str = "",
        version: int = 0,
        metadata: Metadata | None = None,
    ) -> Any:
        """``AssetService.CreatePipelineDefinition``.

        ``steps`` is the JSON-encoded pipeline step list (the proto ``steps``
        field is a string per the generated contract).
        """
        meta = metadata or self._owner._metadata
        request = _asset.CreatePipelineDefinitionRequest(
            tenant_id=meta.tenant_id,
            name=name,
            description=description,
            media_type=media_type,
            steps=steps,
            version=version,
        )
        return self._call("CreatePipelineDefinition", request, metadata)

    def get_pipeline_definition(
        self, definition_id: str, *, metadata: Metadata | None = None
    ) -> Any:
        """``AssetService.GetPipelineDefinition``."""
        meta = metadata or self._owner._metadata
        request = _asset.GetPipelineDefinitionRequest(
            tenant_id=meta.tenant_id, definition_id=definition_id
        )
        return self._call("GetPipelineDefinition", request, metadata)

    def register_asset(
        self,
        file_id: str,
        *,
        name: str = "",
        media_type: str = "",
        asset_metadata: str = "",
        metadata: Metadata | None = None,
    ) -> Any:
        """``AssetService.RegisterAsset`` — register a stored file as an asset.

        ``asset_metadata`` is the JSON-encoded asset metadata (the proto
        ``metadata`` field is a string per the generated contract); the kwarg is
        renamed to avoid colliding with the shared-:class:`Metadata` ``metadata``
        argument used for per-call request context.
        """
        meta = metadata or self._owner._metadata
        request = _asset.RegisterAssetRequest(
            tenant_id=meta.tenant_id,
            project_id=meta.project_id,
            file_id=file_id,
            name=name,
            media_type=media_type,
            metadata=asset_metadata,
        )
        return self._call("RegisterAsset", request, metadata)

    def start_pipeline(
        self,
        definition_id: str,
        asset_id: str,
        *,
        context: str = "",
        correlation_id: str = "",
        metadata: Metadata | None = None,
    ) -> Any:
        """``AssetService.StartPipeline`` — launch a pipeline over an asset."""
        meta = metadata or self._owner._metadata
        request = _asset.StartPipelineRequest(
            tenant_id=meta.tenant_id,
            definition_id=definition_id,
            asset_id=asset_id,
            context=context,
            correlation_id=correlation_id or meta.correlation_id,
        )
        return self._call("StartPipeline", request, metadata)

    def get_pipeline(self, instance_id: str, *, metadata: Metadata | None = None) -> Any:
        """``AssetService.GetPipeline`` — pipeline instance state."""
        meta = metadata or self._owner._metadata
        request = _asset.GetPipelineRequest(
            tenant_id=meta.tenant_id, instance_id=instance_id
        )
        return self._call("GetPipeline", request, metadata)

    def complete_step(
        self,
        step_id: str,
        *,
        status: str = "",
        result: str = "",
        error_message: str = "",
        metadata: Metadata | None = None,
    ) -> Any:
        """``AssetService.CompleteStep`` — report a pipeline step result."""
        meta = metadata or self._owner._metadata
        request = _asset.CompleteStepRequest(
            tenant_id=meta.tenant_id,
            step_id=step_id,
            status=status,
            result=result,
            error_message=error_message,
        )
        return self._call("CompleteStep", request, metadata)

    def list_assets(
        self,
        *,
        media_type: str = "",
        status: str = "",
        page: int = 0,
        page_size: int = 0,
        metadata: Metadata | None = None,
    ) -> Any:
        """``AssetService.ListAssets`` — paginated asset listing."""
        meta = metadata or self._owner._metadata
        request = _asset.ListAssetsRequest(
            tenant_id=meta.tenant_id,
            media_type=media_type,
            status=status,
            page=page,
            page_size=page_size,
        )
        return self._call("ListAssets", request, metadata)

    def get_asset(self, asset_id: str, *, metadata: Metadata | None = None) -> Any:
        """``AssetService.GetAsset``."""
        meta = metadata or self._owner._metadata
        request = _asset.GetAssetRequest(tenant_id=meta.tenant_id, asset_id=asset_id)
        return self._call("GetAsset", request, metadata)


class _WebRtcRoomFacade:
    """``project.webrtc.room`` — RoomService."""

    def __init__(self, stub: Any, owner: "UdbProject"):
        self.stub = stub
        self._owner = owner

    def _call(self, rpc: str, request: Any, metadata: Metadata | None) -> Any:
        if self.stub is None:
            raise UdbConfigurationError("RoomService stubs are not generated")
        return getattr(self.stub, rpc)(
            request,
            metadata=self._owner._ctl_metadata(metadata),
            timeout=self._owner.config.deadline,
        )

    def create_room(
        self,
        name: str,
        *,
        max_participants: int = 0,
        config: str = "",
        created_by: str = "",
        metadata: Metadata | None = None,
    ) -> Any:
        """``RoomService.CreateRoom``."""
        meta = metadata or self._owner._metadata
        request = _webrtc.CreateRoomRequest(
            tenant_id=meta.tenant_id,
            name=name,
            max_participants=max_participants,
            config=config,
            created_by=created_by or meta.user_id,
        )
        return self._call("CreateRoom", request, metadata)

    def get_room(self, room_id: str, *, metadata: Metadata | None = None) -> Any:
        """``RoomService.GetRoom``."""
        meta = metadata or self._owner._metadata
        request = _webrtc.GetRoomRequest(tenant_id=meta.tenant_id, room_id=room_id)
        return self._call("GetRoom", request, metadata)

    def update_room(
        self,
        room_id: str,
        *,
        name: str = "",
        state: str = "",
        config: str = "",
        metadata: Metadata | None = None,
    ) -> Any:
        """``RoomService.UpdateRoom``."""
        meta = metadata or self._owner._metadata
        request = _webrtc.UpdateRoomRequest(
            tenant_id=meta.tenant_id,
            room_id=room_id,
            name=name,
            state=state,
            config=config,
        )
        return self._call("UpdateRoom", request, metadata)

    def close_room(self, room_id: str, *, metadata: Metadata | None = None) -> Any:
        """``RoomService.CloseRoom``."""
        meta = metadata or self._owner._metadata
        request = _webrtc.CloseRoomRequest(tenant_id=meta.tenant_id, room_id=room_id)
        return self._call("CloseRoom", request, metadata)

    def list_rooms(
        self,
        *,
        state: str = "",
        page: int = 0,
        page_size: int = 0,
        metadata: Metadata | None = None,
    ) -> Any:
        """``RoomService.ListRooms``."""
        meta = metadata or self._owner._metadata
        request = _webrtc.ListRoomsRequest(
            tenant_id=meta.tenant_id, state=state, page=page, page_size=page_size
        )
        return self._call("ListRooms", request, metadata)


class _WebRtcPeerFacade:
    """``project.webrtc.peer`` — PeerService."""

    def __init__(self, stub: Any, owner: "UdbProject"):
        self.stub = stub
        self._owner = owner

    def _call(self, rpc: str, request: Any, metadata: Metadata | None) -> Any:
        if self.stub is None:
            raise UdbConfigurationError("PeerService stubs are not generated")
        return getattr(self.stub, rpc)(
            request,
            metadata=self._owner._ctl_metadata(metadata),
            timeout=self._owner.config.deadline,
        )

    def join_room(
        self,
        room_id: str,
        *,
        display_name: str = "",
        peer_metadata: str = "",
        user_agent: str = "",
        metadata: Metadata | None = None,
    ) -> Any:
        """``PeerService.JoinRoom``.

        ``peer_metadata`` is the JSON-encoded peer metadata (the proto
        ``metadata`` field is a string per the generated contract); renamed to
        avoid colliding with the per-call request-context ``metadata``.
        """
        meta = metadata or self._owner._metadata
        request = _webrtc.JoinRoomRequest(
            tenant_id=meta.tenant_id,
            room_id=room_id,
            display_name=display_name,
            metadata=peer_metadata,
            user_agent=user_agent,
        )
        return self._call("JoinRoom", request, metadata)

    def leave_room(
        self,
        room_id: str,
        peer_id: str,
        *,
        metadata: Metadata | None = None,
    ) -> Any:
        """``PeerService.LeaveRoom``."""
        meta = metadata or self._owner._metadata
        request = _webrtc.LeaveRoomRequest(
            tenant_id=meta.tenant_id, room_id=room_id, peer_id=peer_id
        )
        return self._call("LeaveRoom", request, metadata)

    def get_peer(self, peer_id: str, *, metadata: Metadata | None = None) -> Any:
        """``PeerService.GetPeer``."""
        meta = metadata or self._owner._metadata
        request = _webrtc.GetPeerRequest(tenant_id=meta.tenant_id, peer_id=peer_id)
        return self._call("GetPeer", request, metadata)

    def list_peers(
        self,
        room_id: str,
        *,
        state: str = "",
        metadata: Metadata | None = None,
    ) -> Any:
        """``PeerService.ListPeers``."""
        meta = metadata or self._owner._metadata
        request = _webrtc.ListPeersRequest(
            tenant_id=meta.tenant_id, room_id=room_id, state=state
        )
        return self._call("ListPeers", request, metadata)


class _WebRtcTrackFacade:
    """``project.webrtc.track`` — TrackService."""

    def __init__(self, stub: Any, owner: "UdbProject"):
        self.stub = stub
        self._owner = owner

    def _call(self, rpc: str, request: Any, metadata: Metadata | None) -> Any:
        if self.stub is None:
            raise UdbConfigurationError("TrackService stubs are not generated")
        return getattr(self.stub, rpc)(
            request,
            metadata=self._owner._ctl_metadata(metadata),
            timeout=self._owner.config.deadline,
        )

    def publish_track(
        self,
        room_id: str,
        peer_id: str,
        *,
        kind: str = "",
        label: str = "",
        settings: str = "",
        track_metadata: str = "",
        metadata: Metadata | None = None,
    ) -> Any:
        """``TrackService.PublishTrack``.

        ``kind`` / ``settings`` / ``track_metadata`` are strings per the
        generated contract (``settings`` / ``track_metadata`` are JSON);
        ``track_metadata`` is renamed to avoid colliding with the per-call
        request-context ``metadata``.
        """
        meta = metadata or self._owner._metadata
        request = _webrtc.PublishTrackRequest(
            tenant_id=meta.tenant_id,
            room_id=room_id,
            peer_id=peer_id,
            kind=kind,
            label=label,
            settings=settings,
            metadata=track_metadata,
        )
        return self._call("PublishTrack", request, metadata)

    def unpublish_track(self, track_id: str, *, metadata: Metadata | None = None) -> Any:
        """``TrackService.UnpublishTrack``."""
        meta = metadata or self._owner._metadata
        request = _webrtc.UnpublishTrackRequest(
            tenant_id=meta.tenant_id, track_id=track_id
        )
        return self._call("UnpublishTrack", request, metadata)

    def mute_track(
        self,
        track_id: str,
        *,
        muted: bool = True,
        metadata: Metadata | None = None,
    ) -> Any:
        """``TrackService.MuteTrack``."""
        meta = metadata or self._owner._metadata
        request = _webrtc.MuteTrackRequest(
            tenant_id=meta.tenant_id, track_id=track_id, muted=muted
        )
        return self._call("MuteTrack", request, metadata)

    def list_tracks(
        self,
        room_id: str,
        *,
        peer_id: str = "",
        kind: str = "",
        metadata: Metadata | None = None,
    ) -> Any:
        """``TrackService.ListTracks``."""
        meta = metadata or self._owner._metadata
        request = _webrtc.ListTracksRequest(
            tenant_id=meta.tenant_id, room_id=room_id, peer_id=peer_id, kind=kind
        )
        return self._call("ListTracks", request, metadata)


class _WebRtcTurnFacade:
    """``project.webrtc.turn`` — TurnService."""

    def __init__(self, stub: Any, owner: "UdbProject"):
        self.stub = stub
        self._owner = owner

    def issue_credentials(
        self,
        room_id: str,
        peer_id: str,
        *,
        ttl_seconds: int = 0,
        metadata: Metadata | None = None,
    ) -> Any:
        """``TurnService.IssueCredentials`` — short-lived TURN credentials."""
        if self.stub is None:
            raise UdbConfigurationError("TurnService stubs are not generated")
        meta = metadata or self._owner._metadata
        request = _webrtc.IssueCredentialsRequest(
            tenant_id=meta.tenant_id,
            room_id=room_id,
            peer_id=peer_id,
            ttl_seconds=ttl_seconds,
        )
        return self.stub.IssueCredentials(
            request,
            metadata=self._owner._ctl_metadata(metadata),
            timeout=self._owner.config.deadline,
        )


class _WebRtcFacade:
    """``project.webrtc`` — Room / Peer / Track / Turn sub-clients + signaling.

    Each sub-client (``.room`` / ``.peer`` / ``.track`` / ``.turn``) wraps the
    matching generated stub. ``project.webrtc.signal(...)`` opens the
    ``SignalingService.Signal`` bidi stream (see :meth:`signal`). Raw stubs are
    reachable via each sub-client's ``stub`` attribute and via
    :attr:`signaling_stub`.
    """

    def __init__(self, owner: "UdbProject"):
        self._owner = owner
        channel = owner._webrtc_channel if _HAS_WEBRTC else None
        self.room = _WebRtcRoomFacade(
            _webrtc_grpc.RoomServiceStub(channel) if _HAS_WEBRTC else None, owner
        )
        self.peer = _WebRtcPeerFacade(
            _webrtc_grpc.PeerServiceStub(channel) if _HAS_WEBRTC else None, owner
        )
        self.track = _WebRtcTrackFacade(
            _webrtc_grpc.TrackServiceStub(channel) if _HAS_WEBRTC else None, owner
        )
        self.turn = _WebRtcTurnFacade(
            _webrtc_grpc.TurnServiceStub(channel) if _HAS_WEBRTC else None, owner
        )
        self.signaling_stub = (
            _webrtc_grpc.SignalingServiceStub(channel) if _HAS_WEBRTC else None
        )

    def make_signal(
        self,
        *,
        room_id: str,
        peer_id: str,
        offer_sdp: str = "",
        answer_sdp: str = "",
        ice_candidate: str = "",
        ping: bool = False,
        metadata: Metadata | None = None,
    ) -> Any:
        """Build a ``SignalRequest`` with ``tenant_id`` filled from metadata.

        Helper for assembling the messages fed to :meth:`signal`.
        """
        meta = metadata or self._owner._metadata
        return _webrtc.SignalRequest(
            tenant_id=meta.tenant_id,
            room_id=room_id,
            peer_id=peer_id,
            offer_sdp=offer_sdp,
            answer_sdp=answer_sdp,
            ice_candidate=ice_candidate,
            ping=ping,
        )

    def signal(self, request_iterator: Any, *, metadata: Metadata | None = None) -> Any:
        """``SignalingService.Signal`` — bidirectional signaling stream.

        ``request_iterator`` is any iterable/generator yielding ``SignalRequest``
        messages (build them with :meth:`make_signal`). Returns the response
        iterator of ``SignalResponse`` messages. Shared :class:`Metadata` is
        applied as gRPC call metadata. This mirrors the streaming pattern used
        by :class:`UdbClient` (pass a request generator to the stub method and
        iterate the returned stream).
        """
        if self.signaling_stub is None:
            raise UdbConfigurationError("SignalingService stubs are not generated")
        return self.signaling_stub.Signal(
            request_iterator,
            metadata=self._owner._ctl_metadata(metadata),
            timeout=self._owner.config.deadline,
        )


class _AuthzFacade:
    """``project.authz`` — the cached authz decision surface.

    Delegates to :class:`UdbAuthClient` (which routes ``can``/``require``/
    ``explain`` through a shared :class:`AuthzCache`). Provided as its own
    namespace so authz lives apart from authentication on the facade.
    """

    def __init__(self, auth: UdbAuthClient):
        self._auth = auth

    @property
    def cache(self) -> AuthzCache:
        return self._auth.cache

    def can(self, resource: Any, action: str, **kwargs: Any) -> Any:
        return self._auth.can(resource, action, **kwargs)

    def require(self, resource: Any, action: str, **kwargs: Any) -> Any:
        return self._auth.require(resource, action, **kwargs)

    def explain(self, resource: Any, action: str, **kwargs: Any) -> Any:
        return self._auth.explain(resource, action, **kwargs)

    def batch_can(self, checks: Sequence[tuple[str, str]], **kwargs: Any) -> Any:
        return self._auth.batch_can(checks, **kwargs)

    def native_access(self, resource: Any, action: str, **kwargs: Any) -> Any:
        return self._auth.native_access(as_resource(resource), action, **kwargs)

    def get_policy_bundle(self, **kwargs: Any) -> Any:
        return self._auth.get_policy_bundle(**kwargs)


def _make_channel(
    target: str,
    *,
    tls: bool,
    root_certificates: bytes | None,
    options: Sequence[tuple[str, Any]] | None,
) -> grpc.Channel:
    opts = merge_channel_options(options)
    if tls:
        creds = grpc.ssl_channel_credentials(root_certificates)
        return grpc.secure_channel(target, creds, options=opts)
    return grpc.insecure_channel(target, options=opts)


class UdbProject:
    """One-stop synchronous facade over the UDB data plane + control plane.

    Sub-surfaces:

    * ``.data``      — raw :class:`UdbClient` (Select/Upsert/Delete/Batch/...).
    * ``.auth``      — :class:`UdbAuthClient` (authenticate_*/login/refresh).
    * ``.authz``     — cached can/require/batch_can/explain/native_access.
    * ``.storage``   — StorageService file lifecycle (register/finalize upload,
      download URL, get/update/delete/list) PLUS the Wave-1 object + vector
      data-plane helpers as escape hatches.
    * ``.asset``     — AssetService (pipeline definitions, asset registration,
      pipeline run/step lifecycle, listing).
    * ``.webrtc``    — Room / Peer / Track / Turn sub-clients + the
      SignalingService.Signal bidi stream helper.
    * ``.apikey``    — raw ApiKeyService stub (+ create/rotate/revoke helpers).
    * ``.tenant``    — raw TenantService stub (+ create/onboard helpers).
    * ``.notification`` — raw NotificationService stub (+ send helper).
    * ``.analytics`` — raw AnalyticsService stub.

    A sub-client/attribute is only present when its generated stubs exist; when
    a service's stubs are missing the wrapper raises
    :class:`UdbConfigurationError` on use.
    """

    def __init__(self, config: UdbConfig):
        if not config.target:
            raise UdbConfigurationError(
                "UdbConfig.target is required, e.g. '127.0.0.1:50051'"
            )
        self.config = config
        self._metadata = config.metadata()

        # Data plane (broker) channel.
        self._data_channel = _make_channel(
            config.target,
            tls=config.tls,
            root_certificates=config.root_certificates,
            options=config.channel_options,
        )
        # Control plane (auth + native services) channel; reuse the data
        # channel when the targets coincide so we don't double-dial.
        auth_target = config.effective_auth_target()
        if auth_target == config.target:
            self._ctl_channel = self._data_channel
            self._owns_ctl_channel = False
        else:
            self._ctl_channel = _make_channel(
                auth_target,
                tls=config.tls,
                root_certificates=config.root_certificates,
                options=config.channel_options,
            )
            self._owns_ctl_channel = True

        # WebRTC channel; reuse the control-plane channel unless a distinct
        # webrtc_target is configured.
        webrtc_target = config.effective_webrtc_target()
        if webrtc_target == auth_target:
            self._webrtc_channel = self._ctl_channel
            self._owns_webrtc_channel = False
        elif webrtc_target == config.target:
            self._webrtc_channel = self._data_channel
            self._owns_webrtc_channel = False
        else:
            self._webrtc_channel = _make_channel(
                webrtc_target,
                tls=config.tls,
                root_certificates=config.root_certificates,
                options=config.channel_options,
            )
            self._owns_webrtc_channel = True

        self.data = UdbClient(
            config.target,
            self._metadata,
            timeout=config.deadline,
            channel=self._data_channel,
        )
        self.auth = UdbAuthClient(
            auth_target,
            self._metadata,
            timeout=config.deadline,
            channel=self._ctl_channel,
            policy_bundle_secret=config.policy_bundle_secret,
        )
        self.authz = _AuthzFacade(self.auth)
        self.storage = _StorageFacade(
            self.data,
            stub=(
                _storage_grpc.StorageServiceStub(self._ctl_channel)
                if _HAS_STORAGE
                else None
            ),
            owner=self,
        )
        self.asset = _AssetFacade(
            _asset_grpc.AssetServiceStub(self._ctl_channel) if _HAS_ASSET else None,
            self,
        )
        self.webrtc = _WebRtcFacade(self)

        # Native control-plane stubs (only when generated).
        self.apikey = (
            _apikey_grpc.ApiKeyServiceStub(self._ctl_channel) if _HAS_APIKEY else None
        )
        self.tenant = (
            _tenant_grpc.TenantServiceStub(self._ctl_channel) if _HAS_TENANT else None
        )
        self.notification = (
            _notif_grpc.NotificationServiceStub(self._ctl_channel)
            if _HAS_NOTIFICATION
            else None
        )
        self.analytics = (
            _analytics_grpc.AnalyticsServiceStub(self._ctl_channel)
            if _HAS_ANALYTICS
            else None
        )

    # ── lifecycle ─────────────────────────────────────────────────────────
    @property
    def metadata(self) -> Metadata:
        return self._metadata

    def bind_metadata(self, metadata: Metadata) -> None:
        """Rebind the shared metadata across every sub-client."""
        metadata = self._with_config_credentials(metadata)
        self._metadata = metadata
        self.data.bind_metadata(metadata)
        self.auth.bind_metadata(metadata)

    def set_credentials(
        self, *, bearer_token: str = "", api_key: str = ""
    ) -> None:
        """Hot-swap the auth headers attached to future facade calls."""
        self.config.bearer_token = bearer_token
        self.config.api_key = api_key
        self.bind_metadata(
            replace(self._metadata, bearer_token=bearer_token, api_key=api_key)
        )

    def close(self) -> None:
        if self._owns_webrtc_channel:
            self._webrtc_channel.close()
        if self._owns_ctl_channel:
            self._ctl_channel.close()
        self._data_channel.close()

    def __enter__(self) -> "UdbProject":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _ctl_metadata(self, metadata: Metadata | None) -> tuple[tuple[str, str], ...]:
        return self._with_config_credentials(metadata or self._metadata).to_grpc_metadata()

    def _with_config_credentials(self, metadata: Metadata) -> Metadata:
        bearer_token = metadata.bearer_token or self.config.bearer_token
        api_key = metadata.api_key or self.config.api_key
        if bearer_token == metadata.bearer_token and api_key == metadata.api_key:
            return metadata
        return replace(metadata, bearer_token=bearer_token, api_key=api_key)

    # ── notification convenience ──────────────────────────────────────────
    def send_notification(
        self,
        event_type: str,
        *,
        recipient_id: str = "",
        recipient_address: str = "",
        channels: Sequence[Any] = (),
        variables: dict[str, str] | None = None,
        resource_type: str = "",
        resource_id: str = "",
        resource_name: str = "",
        locale: str = "",
        metadata: Metadata | None = None,
    ) -> Any:
        """Send a notification via ``NotificationService.SendNotification``.

        ``channels`` are ``NotificationChannel`` enum values. ``tenant_id`` /
        ``project_id`` / ``correlation_id`` / request context are filled from
        the shared metadata.
        """
        if self.notification is None:
            raise UdbConfigurationError("NotificationService stubs are not generated")
        meta = metadata or self._metadata
        request = _notif.SendNotificationRequest(
            event_type=event_type,
            recipient_id=recipient_id,
            recipient_address=recipient_address,
            tenant_id=meta.tenant_id,
            project_id=meta.project_id,
            resource_type=resource_type,
            resource_id=resource_id,
            resource_name=resource_name,
            correlation_id=meta.correlation_id,
            locale=locale,
            variables=dict(variables or {}),
            channels=list(channels),
            context=meta.to_request_context(),
        )
        return self.notification.SendNotification(
            request, metadata=self._ctl_metadata(metadata), timeout=self.config.deadline
        )

    # ── apikey convenience (only the RPCs the service actually exposes) ────
    def create_api_key(
        self,
        name: str,
        *,
        owner_type: Any = None,
        owner_id: str = "",
        scopes: Sequence[str] = (),
        description: str = "",
        ip_allowlist: Sequence[str] = (),
        rate_limit_per_minute: int = 0,
        rate_limit_per_day: int = 0,
        expires_at: Any = None,
        metadata: Metadata | None = None,
    ) -> Any:
        """Create an API key (``ApiKeyService.CreateApiKey``).

        Returns the ``CreateApiKeyResponse`` whose ``plain_key`` is the only
        time the secret is returned in clear text.
        """
        if self.apikey is None:
            raise UdbConfigurationError("ApiKeyService stubs are not generated")
        meta = metadata or self._metadata
        kwargs: dict[str, Any] = dict(
            name=name,
            description=description,
            owner_id=owner_id,
            scopes=list(scopes),
            ip_allowlist=list(ip_allowlist),
            rate_limit_per_minute=rate_limit_per_minute,
            rate_limit_per_day=rate_limit_per_day,
            context=meta.to_request_context(),
        )
        if owner_type is not None:
            kwargs["owner_type"] = owner_type
        if expires_at is not None:
            kwargs["expires_at"] = expires_at
        request = _apikey.CreateApiKeyRequest(**kwargs)
        return self.apikey.CreateApiKey(
            request, metadata=self._ctl_metadata(metadata), timeout=self.config.deadline
        )

    def revoke_api_key(
        self,
        key_id: str,
        *,
        reason: str = "",
        metadata: Metadata | None = None,
    ) -> Any:
        """Revoke an API key (``ApiKeyService.RevokeApiKey``)."""
        if self.apikey is None:
            raise UdbConfigurationError("ApiKeyService stubs are not generated")
        meta = metadata or self._metadata
        request = _apikey.RevokeApiKeyRequest(
            key_id=key_id,
            revoke_reason=reason,
            context=meta.to_request_context(),
        )
        return self.apikey.RevokeApiKey(
            request, metadata=self._ctl_metadata(metadata), timeout=self.config.deadline
        )

    def rotate_api_key(
        self,
        key_id: str,
        *,
        name: str,
        owner_type: Any = None,
        owner_id: str = "",
        scopes: Sequence[str] = (),
        reason: str = "rotated",
        metadata: Metadata | None = None,
    ) -> Any:
        """Rotate a key = create a fresh key, then revoke the old one.

        ApiKeyService has no atomic rotate RPC (only Create/Revoke), so rotation
        is implemented as create-new-then-revoke-old. The NEW key's
        ``CreateApiKeyResponse`` (with ``plain_key``) is returned; the old key is
        revoked with ``reason``. If creation fails the old key is left intact.
        """
        created = self.create_api_key(
            name,
            owner_type=owner_type,
            owner_id=owner_id,
            scopes=scopes,
            metadata=metadata,
        )
        self.revoke_api_key(key_id, reason=reason, metadata=metadata)
        return created

    # ── tenant convenience ────────────────────────────────────────────────
    def create_tenant(
        self,
        code: str,
        name: str,
        *,
        type: str = "",
        parent_tenant_id: str = "",
        config: str = "",
        branding: str = "",
        metadata: Metadata | None = None,
    ) -> Any:
        """Create / onboard a tenant (``TenantService.CreateTenant``).

        ``config`` and ``branding`` are JSON strings per the proto contract.
        """
        if self.tenant is None:
            raise UdbConfigurationError("TenantService stubs are not generated")
        request = _tenant.CreateTenantRequest(
            code=code,
            name=name,
            type=type,
            parent_tenant_id=parent_tenant_id,
            config=config,
            branding=branding,
        )
        return self.tenant.CreateTenant(
            request, metadata=self._ctl_metadata(metadata), timeout=self.config.deadline
        )

    # ``onboard_tenant`` reads as the intent; delegate to ``create_tenant``.
    onboard_tenant = create_tenant


def create_udb(config: UdbConfig | None = None, /, **kwargs: Any) -> UdbProject:
    """Construct a :class:`UdbProject`.

    Either pass a :class:`UdbConfig`, or pass keyword args matching its fields::

        udb = create_udb(target="127.0.0.1:50051", tenant_id="acme")
        udb = create_udb(UdbConfig(target="127.0.0.1:50051", tenant_id="acme"))
    """
    if config is None:
        config = UdbConfig(**kwargs)
    elif kwargs:
        raise UdbConfigurationError(
            "pass either a UdbConfig or keyword args, not both"
        )
    return UdbProject(config)


class UdbAsyncProject:
    """Async facade mirroring the data-plane surface of :class:`UdbProject`.

    The native control-plane stubs and the auth client are synchronous gRPC
    stubs, so this async facade exposes:

    * ``.data``    — :class:`UdbAsyncClient` (await-able Select/Upsert/...).
    * ``.storage`` — the async object + vector data-plane helpers proxied to the
      async data client.

    The StorageService / AssetService / WebRTC control-plane stubs are
    synchronous (like apikey/tenant/notification/analytics), so they are NOT
    surfaced here; the async ``.storage`` carries only the await-able data-plane
    helpers and raises :class:`UdbConfigurationError` for the sync file
    lifecycle. For StorageService file lifecycle, AssetService, or WebRTC from
    async code, build a sibling :class:`UdbProject` via :meth:`sync_project`
    (they share the same :class:`UdbConfig`) or call the sync stubs in a thread
    executor. This keeps async parity honest rather than pretending the sync
    stubs are awaitable.
    """

    def __init__(self, config: UdbConfig):
        if not config.target:
            raise UdbConfigurationError(
                "UdbConfig.target is required, e.g. '127.0.0.1:50051'"
            )
        self.config = config
        self._metadata = config.metadata()
        self.data = UdbAsyncClient(
            config.target,
            self._metadata,
            secure=config.tls,
            root_certificates=config.root_certificates,
            channel_options=config.channel_options,
            timeout=config.deadline,
        )
        self.storage = _StorageFacade(self.data)

    @property
    def metadata(self) -> Metadata:
        return self._metadata

    def bind_metadata(self, metadata: Metadata) -> None:
        metadata = self._with_config_credentials(metadata)
        self._metadata = metadata
        self.data.bind_metadata(metadata)

    def set_credentials(
        self, *, bearer_token: str = "", api_key: str = ""
    ) -> None:
        """Hot-swap the auth headers attached to future async data calls."""
        self.config.bearer_token = bearer_token
        self.config.api_key = api_key
        self.bind_metadata(
            replace(self._metadata, bearer_token=bearer_token, api_key=api_key)
        )

    def sync_project(self) -> UdbProject:
        """A sync :class:`UdbProject` sharing this config (auth/control plane)."""
        return UdbProject(self.config)

    def _with_config_credentials(self, metadata: Metadata) -> Metadata:
        bearer_token = metadata.bearer_token or self.config.bearer_token
        api_key = metadata.api_key or self.config.api_key
        if bearer_token == metadata.bearer_token and api_key == metadata.api_key:
            return metadata
        return replace(metadata, bearer_token=bearer_token, api_key=api_key)

    async def close(self) -> None:
        await self.data.close()

    async def __aenter__(self) -> "UdbAsyncProject":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


def create_udb_async(config: UdbConfig | None = None, /, **kwargs: Any) -> UdbAsyncProject:
    """Construct a :class:`UdbAsyncProject` (see :func:`create_udb`)."""
    if config is None:
        config = UdbConfig(**kwargs)
    elif kwargs:
        raise UdbConfigurationError(
            "pass either a UdbConfig or keyword args, not both"
        )
    return UdbAsyncProject(config)
