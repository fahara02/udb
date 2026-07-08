#!/usr/bin/env python3
"""Smoke-test the UDB LiveKit SFU bridge against a running compose profile.

Run after starting the local SFU profile, for example:

  docker compose -f docker-compose.integration.yml --profile sfu up -d --wait udb-livekit livekit coturn
  python scripts/livekit_sfu_smoke.py

The smoke intentionally exercises the served broker path for token minting and
lifecycle hooks, then calls LiveKit's RoomService with the same API key/secret so
miswired LiveKit credentials or container reachability fail visibly.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "sdk" / "python" / "gen"))
sys.path.insert(0, str(ROOT / "sdk" / "python"))

MAX_LIVEKIT_URL_BYTES = 2048
MAX_LIVEKIT_RESPONSE_BYTES = 1_048_576
BROKER_TARGET_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+:\d{1,5}$")


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def b64url_json(segment: str) -> dict[str, Any]:
    padded = segment + ("=" * (-len(segment) % 4))
    return json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))


def sign_hs256(claims: dict[str, Any], secret: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    head = b64url(json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    body = b64url(json.dumps(claims, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signing_input = f"{head}.{body}".encode("ascii")
    sig = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{head}.{body}.{b64url(sig)}"


def verify_hs256(token: str, secret: str) -> dict[str, Any]:
    head, body, sig = token.split(".")
    expected = hmac.new(
        secret.encode("utf-8"),
        f"{head}.{body}".encode("ascii"),
        hashlib.sha256,
    ).digest()
    actual = base64.urlsafe_b64decode((sig + "=" * (-len(sig) % 4)).encode("ascii"))
    if not hmac.compare_digest(expected, actual):
        raise RuntimeError("LiveKit join token signature did not verify with the configured secret")
    return b64url_json(body)


def canonical_network_token(name: str, value: str, *, max_bytes: int = MAX_LIVEKIT_URL_BYTES) -> str:
    if value != value.strip():
        raise RuntimeError(f"{name} must not include surrounding whitespace")
    if not value:
        raise RuntimeError(f"{name} must not be empty")
    if len(value.encode("utf-8")) > max_bytes:
        raise RuntimeError(f"{name} must be <= {max_bytes} bytes")
    if any(ch.isspace() or ord(ch) < 32 or ord(ch) == 127 for ch in value):
        raise RuntimeError(f"{name} must not include whitespace or control characters")
    return value


def validate_base_url(name: str, value: str, *, schemes: set[str]) -> str:
    value = canonical_network_token(name, value)
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme not in schemes:
        expected = ", ".join(sorted(schemes))
        raise RuntimeError(f"{name} must use one of: {expected}")
    if not parsed.hostname:
        raise RuntimeError(f"{name} must include a hostname")
    if parsed.username or parsed.password:
        raise RuntimeError(f"{name} must not include credentials")
    try:
        port = parsed.port
    except ValueError as exc:
        raise RuntimeError(f"{name} must include a valid port when a port is present") from exc
    if port is not None and not 1 <= port <= 65535:
        raise RuntimeError(f"{name} port must be between 1 and 65535")
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise RuntimeError(f"{name} must be a base URL without path, query, or fragment")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def validate_broker_target(value: str) -> str:
    value = canonical_network_token("--broker", value)
    if "://" in value or "/" in value or "?" in value or "#" in value:
        raise RuntimeError("--broker must be a host:port gRPC target without scheme, path, query, or fragment")
    if value.startswith("["):
        host, sep, port_text = value.rpartition(":")
        if not sep or not host.endswith("]") or len(host) <= 2:
            raise RuntimeError("--broker must be a host:port gRPC target")
    else:
        if not BROKER_TARGET_PATTERN.fullmatch(value):
            raise RuntimeError("--broker must be a host:port gRPC target")
        _host, _sep, port_text = value.rpartition(":")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise RuntimeError("--broker must include a valid numeric port") from exc
    if not 1 <= port <= 65535:
        raise RuntimeError("--broker port must be between 1 and 65535")
    return value


def decode_limited_json(source: Any) -> dict[str, Any]:
    raw = source.read(MAX_LIVEKIT_RESPONSE_BYTES + 1)
    if len(raw) > MAX_LIVEKIT_RESPONSE_BYTES:
        raise RuntimeError("LiveKit response exceeded 1048576 bytes")
    return json.loads(raw.decode("utf-8") or "{}")


def request_json(method: str, url: str, body: dict[str, Any], token: str) -> tuple[int, dict[str, Any]]:
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            payload = decode_limited_json(resp)
            return resp.status, payload
    except urllib.error.HTTPError as exc:
        payload = decode_limited_json(exc)
        return exc.code, payload


def metadata_for(tenant_id: str, project_id: str) -> list[tuple[str, str]]:
    request_id = f"sfu-smoke-{uuid.uuid4().hex}"
    return [
        ("x-tenant-id", tenant_id),
        ("x-project-id", project_id),
        ("x-purpose", "livekit-sfu-smoke"),
        ("x-request-id", request_id),
        ("x-correlation-id", request_id),
        ("x-udb-scopes", "udb:webrtc:room:create-room udb:webrtc:peer:join-room udb:webrtc:peer:leave-room udb:webrtc:room:close-room udb:webrtc:turn:issue-credentials"),
    ]


def response_metadata(call: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    for source in (call.initial_metadata(), call.trailing_metadata()):
        if source is None:
            continue
        for key, value in source:
            out[key.lower()] = value
    return out


def assert_join_metadata(
    meta: dict[str, str],
    *,
    tenant_id: str,
    room_id: str,
    peer_id: str,
    livekit_url: str,
    api_key: str,
    api_secret: str,
) -> None:
    token = meta.get("x-udb-sfu-join-token", "")
    url = meta.get("x-udb-sfu-url", "")
    expires = meta.get("x-udb-sfu-expires-at", "")
    if not token or not url or not expires:
        raise RuntimeError(f"missing SFU metadata headers: {meta}")
    if url.rstrip("/") != livekit_url.rstrip("/"):
        raise RuntimeError(f"SFU URL header mismatch: got {url!r}, expected {livekit_url!r}")
    claims = verify_hs256(token, api_secret)
    expected_sub = f"udb:{tenant_id}:{room_id}:{peer_id}"
    if claims.get("iss") != api_key or claims.get("sub") != expected_sub:
        raise RuntimeError(f"unexpected LiveKit token identity claims: {claims}")
    video = claims.get("video") or {}
    if video.get("roomJoin") is not True or video.get("room") != room_id:
        raise RuntimeError(f"join token lacks room-bound join grant: {claims}")
    metadata = json.loads(str(claims.get("metadata") or "{}"))
    expected_metadata = {"tenant_id": tenant_id, "room_id": room_id, "peer_id": peer_id}
    if metadata != expected_metadata:
        raise RuntimeError(f"join token metadata mismatch: {metadata}")


def livekit_room_service_ok(base_url: str, api_key: str, api_secret: str) -> None:
    now = int(time.time())
    token = sign_hs256(
        {
            "iss": api_key,
            "sub": "udb:sfu-smoke",
            "nbf": now - 5,
            "exp": now + 300,
            # LiveKit RoomService.ListRooms authorizes on the `roomList` grant;
            # `roomAdmin` alone is per-room admin and is rejected with 401
            # "permissions denied". Grant both so the reachability probe both
            # lists and could administer.
            "video": {"roomList": True, "roomAdmin": True},
        },
        api_secret,
    )
    status, payload = request_json(
        "POST",
        f"{base_url.rstrip('/')}/twirp/livekit.RoomService/ListRooms",
        {},
        token,
    )
    if status != 200:
        raise RuntimeError(f"LiveKit RoomService auth/reachability failed: status={status} payload={payload}")


def expect_runtime_error(label: str, fn: Any) -> None:
    try:
        fn()
    except RuntimeError:
        return
    raise AssertionError(f"selftest did not reject {label}")


def run_selftest() -> None:
    tenant_id = "tenant-selftest"
    room_id = "room-selftest"
    peer_id = "peer-selftest"
    api_key = "devkey"
    api_secret = "secret"
    livekit_url = "ws://livekit:7880"
    now = int(time.time())
    claims = {
        "iss": api_key,
        "sub": f"udb:{tenant_id}:{room_id}:{peer_id}",
        "nbf": now - 5,
        "exp": now + 300,
        "video": {"roomJoin": True, "room": room_id},
        "metadata": json.dumps(
            {"tenant_id": tenant_id, "room_id": room_id, "peer_id": peer_id},
            separators=(",", ":"),
            sort_keys=True,
        ),
    }
    token = sign_hs256(claims, api_secret)
    meta = {
        "x-udb-sfu-join-token": token,
        "x-udb-sfu-url": livekit_url,
        "x-udb-sfu-expires-at": str(now + 300),
    }
    assert_join_metadata(
        meta,
        tenant_id=tenant_id,
        room_id=room_id,
        peer_id=peer_id,
        livekit_url=livekit_url,
        api_key=api_key,
        api_secret=api_secret,
    )

    bad_sig = dict(meta)
    bad_sig["x-udb-sfu-join-token"] = sign_hs256(claims, "wrong-secret")
    expect_runtime_error(
        "bad LiveKit token signature",
        lambda: assert_join_metadata(
            bad_sig,
            tenant_id=tenant_id,
            room_id=room_id,
            peer_id=peer_id,
            livekit_url=livekit_url,
            api_key=api_key,
            api_secret=api_secret,
        ),
    )

    bad_grant = dict(meta)
    bad_claims = dict(claims)
    bad_claims["video"] = {"roomJoin": False, "room": room_id}
    bad_grant["x-udb-sfu-join-token"] = sign_hs256(bad_claims, api_secret)
    expect_runtime_error(
        "missing room-bound join grant",
        lambda: assert_join_metadata(
            bad_grant,
            tenant_id=tenant_id,
            room_id=room_id,
            peer_id=peer_id,
            livekit_url=livekit_url,
            api_key=api_key,
            api_secret=api_secret,
        ),
    )

    bad_url = dict(meta)
    bad_url["x-udb-sfu-url"] = "ws://other-livekit:7880"
    expect_runtime_error(
        "SFU URL mismatch",
        lambda: assert_join_metadata(
            bad_url,
            tenant_id=tenant_id,
            room_id=room_id,
            peer_id=peer_id,
            livekit_url=livekit_url,
            api_key=api_key,
            api_secret=api_secret,
        ),
    )

    scope_header = dict(metadata_for(tenant_id, "default")).get("x-udb-scopes", "")
    required_scopes = {
        "udb:webrtc:room:create-room",
        "udb:webrtc:peer:join-room",
        "udb:webrtc:peer:leave-room",
        "udb:webrtc:room:close-room",
        "udb:webrtc:turn:issue-credentials",
    }
    missing = required_scopes.difference(scope_header.split())
    if missing:
        raise AssertionError(f"selftest metadata is missing scopes: {sorted(missing)}")

    valid_http = validate_base_url("--livekit-http", "http://127.0.0.1:57880", schemes={"http", "https"})
    if valid_http != "http://127.0.0.1:57880":
        raise AssertionError(f"canonical LiveKit HTTP URL changed unexpectedly: {valid_http}")
    valid_ws = validate_base_url("--livekit-url", "ws://livekit:7880", schemes={"ws", "wss"})
    if valid_ws != "ws://livekit:7880":
        raise AssertionError(f"canonical LiveKit URL changed unexpectedly: {valid_ws}")
    if validate_broker_target("127.0.0.1:50081") != "127.0.0.1:50081":
        raise AssertionError("canonical broker target was rejected")
    expect_runtime_error(
        "padded LiveKit HTTP URL",
        lambda: validate_base_url("--livekit-http", " http://127.0.0.1:57880", schemes={"http", "https"}),
    )
    expect_runtime_error(
        "credentialed LiveKit HTTP URL",
        lambda: validate_base_url("--livekit-http", "http://devkey:secret@127.0.0.1:57880", schemes={"http", "https"}),
    )
    expect_runtime_error(
        "path-bearing LiveKit HTTP URL",
        lambda: validate_base_url("--livekit-http", "http://127.0.0.1:57880/twirp", schemes={"http", "https"}),
    )
    expect_runtime_error(
        "unsupported LiveKit URL scheme",
        lambda: validate_base_url("--livekit-url", "file://livekit:7880", schemes={"ws", "wss"}),
    )
    expect_runtime_error(
        "out-of-range broker port",
        lambda: validate_broker_target("127.0.0.1:99999"),
    )
    expect_runtime_error(
        "nonnumeric broker port",
        lambda: validate_broker_target("[::1]:grpc"),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test the UDB LiveKit SFU bridge")
    parser.add_argument("--selftest", action="store_true", help="run no-network token/metadata checks")
    parser.add_argument("--broker", default="127.0.0.1:50081", help="udb-livekit gRPC target")
    parser.add_argument("--livekit-http", default="http://127.0.0.1:57880", help="LiveKit HTTP base URL")
    parser.add_argument("--livekit-url", default="ws://livekit:7880", help="LiveKit URL expected in broker SFU headers")
    parser.add_argument("--api-key", default="devkey")
    parser.add_argument("--api-secret", default="secret")
    parser.add_argument("--tenant-id", default="")
    parser.add_argument("--project-id", default="default")
    # The WebRTC Room/Peer/Turn RPCs live on the native control-plane listener,
    # which requires a real bearer (header scopes alone do not bypass it). Supply
    # login credentials so the smoke authenticates like a real operator; the
    # canonical tenant UUID is then taken from the authenticated principal when
    # --tenant-id is not given.
    parser.add_argument("--username", default="", help="operator login for the native control-plane bearer")
    parser.add_argument("--password", default="", help="operator password")
    parser.add_argument("--auth-broker", default="", help="auth gRPC target (defaults to --broker)")
    args = parser.parse_args()

    if args.selftest:
        run_selftest()
        print("LiveKit SFU smoke selftest passed")
        return 0

    broker = validate_broker_target(args.broker)
    livekit_http = validate_base_url("--livekit-http", args.livekit_http, schemes={"http", "https"})
    livekit_url = validate_base_url("--livekit-url", args.livekit_url, schemes={"ws", "wss"})

    try:
        import grpc
        from udb.core.webrtc.services.v1 import webrtc_service_pb2 as pb
        from udb.core.webrtc.services.v1 import webrtc_service_pb2_grpc as pb_grpc
    except Exception as exc:  # noqa: BLE001 - operator diagnostics
        raise RuntimeError(
            "Python gRPC stubs are unavailable; run from the repo root with SDK Python deps installed"
        ) from exc

    livekit_room_service_ok(livekit_http, args.api_key, args.api_secret)

    tenant_id = args.tenant_id or f"tenant-{uuid.uuid4().hex[:12]}"
    bearer = ""
    if args.username:
        from udb_client.auth import UdbAuthClient
        from udb_client.metadata import Metadata

        auth_target = validate_broker_target(args.auth_broker) if args.auth_broker else broker
        auth_meta = Metadata(tenant_id=tenant_id if args.tenant_id else "sdk-live", project_id=args.project_id,
                             purpose="livekit-sfu-smoke", correlation_id="livekit-sfu-smoke",
                             service_identity="livekit-sfu-smoke")
        auth = UdbAuthClient(auth_target, auth_meta, timeout=15.0)
        login = auth.login(args.username, args.password, device_name="livekit-sfu-smoke")
        bearer = login.access_token
        # Prefer the canonical tenant UUID bound to the authenticated principal.
        principal = auth.authenticate_bearer(bearer)
        if getattr(principal.principal, "tenant_id", ""):
            tenant_id = principal.principal.tenant_id

    meta = metadata_for(tenant_id, args.project_id)
    if bearer:
        meta = meta + [("authorization", f"Bearer {bearer}")]
    channel = grpc.insecure_channel(broker)
    grpc.channel_ready_future(channel).result(timeout=15)
    room_stub = pb_grpc.RoomServiceStub(channel)
    peer_stub = pb_grpc.PeerServiceStub(channel)
    turn_stub = pb_grpc.TurnServiceStub(channel)

    room = room_stub.CreateRoom(
        pb.CreateRoomRequest(
            tenant_id=tenant_id,
            name="livekit-sfu-smoke",
            max_participants=4,
            # created_by is a UUID column (wv_uuid_or_null) — a UUID or empty for
            # NULL; the literal "smoke" is rejected as a non-UUID.
            created_by=str(uuid.uuid4()),
        ),
        metadata=meta,
        timeout=10,
    )
    if not room.room_id:
        raise RuntimeError(f"CreateRoom returned no room_id: {room}")

    join, join_call = peer_stub.JoinSession.with_call(
        pb.JoinSessionRequest(
            tenant_id=tenant_id,
            room_id=room.room_id,
            display_name="Ada",
            metadata='{"smoke":true}',
            user_agent="udb-livekit-sfu-smoke",
            ttl_seconds=300,
        ),
        metadata=meta,
        timeout=10,
    )
    if join.peer is None or not join.peer.peer_id:
        raise RuntimeError(f"JoinSession returned no peer: {join}")
    assert_join_metadata(
        response_metadata(join_call),
        tenant_id=tenant_id,
        room_id=room.room_id,
        peer_id=join.peer.peer_id,
        livekit_url=livekit_url,
        api_key=args.api_key,
        api_secret=args.api_secret,
    )

    creds, creds_call = turn_stub.IssueCredentials.with_call(
        pb.IssueCredentialsRequest(
            tenant_id=tenant_id,
            room_id=room.room_id,
            peer_id=join.peer.peer_id,
            ttl_seconds=300,
        ),
        metadata=meta,
        timeout=10,
    )
    if not creds.ice_servers or not creds.username or not creds.credential:
        raise RuntimeError(f"IssueCredentials returned incomplete TURN credentials: {creds}")
    assert_join_metadata(
        response_metadata(creds_call),
        tenant_id=tenant_id,
        room_id=room.room_id,
        peer_id=join.peer.peer_id,
        livekit_url=livekit_url,
        api_key=args.api_key,
        api_secret=args.api_secret,
    )

    left = peer_stub.LeaveRoom(
        pb.LeaveRoomRequest(tenant_id=tenant_id, room_id=room.room_id, peer_id=join.peer.peer_id),
        metadata=meta,
        timeout=10,
    )
    if not left.success:
        raise RuntimeError(f"LeaveRoom did not report success: {left}")
    room_stub.CloseRoom(
        pb.CloseRoomRequest(tenant_id=tenant_id, room_id=room.room_id),
        metadata=meta,
        timeout=10,
    )

    print(
        json.dumps(
            {
                "ok": True,
                "broker": broker,
                "livekit_http": livekit_http,
                "tenant_id": tenant_id,
                "room_id": room.room_id,
                "peer_id": join.peer.peer_id,
            },
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
