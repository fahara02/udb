#!/usr/bin/env python3
"""Export the shared full-surface benchmark plan for SDKs without their own seed.

The Rust SDK measures every generated RPC exactly as the Python harness does,
but it does not re-implement the 1,700-line seed phase: this script runs the
Python seed against the live broker (setup only, never timed), resolves every
RPC's documented request body against the seeded fixtures, and writes them in
measurement order as proto3 JSON. The Rust harness then sends each request
through its own client and times only that.

Credentials are never written. Request fields that carry the caller's own
session material hold a sentinel instead (`__UDB_BENCH_*__`), which the
measuring SDK replaces with credentials it minted itself.

Usage (same environment as `UDB_LIVE_PERF=1 pytest`):

    python sdk/python/tests/export_bench_plan.py <plan.json>
"""

from __future__ import annotations

import json
import sys
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from google.protobuf.json_format import MessageToDict  # noqa: E402

import test_live_conformance as live  # noqa: E402

SCHEMA_VERSION = 1
SENTINELS = {
    "token": "__UDB_BENCH_ACCESS_TOKEN__",
    "access_token": "__UDB_BENCH_ACCESS_TOKEN__",
    "refresh_token": "__UDB_BENCH_REFRESH_TOKEN__",
    "session_id": "__UDB_BENCH_SESSION_ID__",
    "csrf_token": "__UDB_BENCH_CSRF_TOKEN__",
}


def as_json(message) -> dict:
    return MessageToDict(message, preserving_proto_field_name=True)


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    output = Path(argv[1])
    target = live.required_env("UDB_GRPC_TARGET")
    auth_target = live.os.getenv("UDB_AUTH_GRPC_TARGET", target)
    auth = live.UdbAuthClient(auth_target, live.metadata(), timeout=10.0)
    login = auth.login(live.required_env("UDB_LIVE_USERNAME"), live.required_env("UDB_LIVE_PASSWORD"),
                       device_name="bench-plan-export")
    principal = auth.authenticate_bearer(login.access_token).principal
    authed_meta = replace(live.metadata(bearer_token=login.access_token), tenant_id=principal.tenant_id,
                          client_catalog_version="")
    platform_login = auth.login(live.required_env("UDB_LIVE_PLATFORM_USERNAME"),
                                live.required_env("UDB_LIVE_PLATFORM_PASSWORD"),
                                device_name="bench-plan-export-platform")
    platform_principal = auth.authenticate_bearer(platform_login.access_token).principal
    if not any(role.strip().lower() == "platform_admin" for role in platform_principal.roles):
        raise AssertionError("offline platform fixture did not issue platform_admin role")
    platform_meta = replace(live.metadata(bearer_token=platform_login.access_token),
                            tenant_id=platform_principal.tenant_id, client_catalog_version="")
    clients, platform_clients = {}, {}
    try:
        for client_cls in live.SERVICE_CLIENTS:
            endpoint = target if client_cls is live.DataBrokerClient else auth_target
            clients[client_cls] = client_cls(endpoint, authed_meta, timeout=20.0)
        for client_cls in (live.DataBrokerClient, live.AuthzServiceClient, live.AnalyticsServiceClient,
                           live.BackupServiceClient, live.TenantServiceClient):
            endpoint = target if client_cls is live.DataBrokerClient else auth_target
            platform_clients[client_cls] = client_cls(endpoint, platform_meta, timeout=20.0)
        # The seed's disposable entities stay in place: the measuring SDK uses
        # them. CI resets the database before each SDK's run.
        fixtures, seed_record_id, _cleanup = live.perf_seed(
            clients, platform_clients, authed_meta, platform_meta, principal.subject,
            platform_principal.subject,
        )
    finally:
        for client in list(clients.values()) + list(platform_clients.values()):
            client.close()
    for key, sentinel in SENTINELS.items():
        fixtures.set(key, sentinel)

    rpcs = []
    for client_cls, method in sorted(live.ALL_RPCS, key=live.perf_rpc_order_key):
        path = live.rpc_path(method)
        streaming = method.client_streaming or method.server_streaming
        cdc = client_cls is live.DataBrokerClient and method.name == "PublishCDC"
        if cdc:
            kind, iters = "cdc_first_event", 1
        elif streaming:
            kind = "stream_first_recv"
            iters = 3 if live.RPC_OPERATION_KIND.get(path) == "read_only" else 1
        else:
            kind = live.RPC_OPERATION_KIND.get(path, "read_only")
            iters = 10 if kind == "read_only" else 1
            if method.name in live.SINGLE_CALL_PERF_METHODS:
                iters = 1
        entry = {
            "path": path,
            "service": client_cls._SERVICE_FULL.split(".")[-1],
            "rpc": method.name,
            "api_alias": live.RPC_API_ALIAS.get(path, ""),
            "operation_id": live.RPC_OPERATION_ID.get(path, ""),
            "input_type": method.input_type.full_name,
            "output_type": method.output_type.full_name,
            "client_streaming": method.client_streaming,
            "server_streaming": method.server_streaming,
            "kind": kind,
            "iters": iters,
            "warm_up": kind == "read_only" and not streaming and method.name not in live.SINGLE_CALL_PERF_METHODS,
            "platform": live.requires_platform_perf_identity(client_cls, method.name),
            "reauthenticate_before": client_cls is live.TenantServiceClient and method.name == "PurgeTenant",
            "capability_optional": client_cls is live.RoomServiceClient
            and method.name in live.WEBRTC_EGRESS_OPTIONAL_METHODS,
        }
        if not cdc:
            try:
                request = live.perf_real_body(client_cls, method, authed_meta, fixtures)
            except live.MissingExplicitPerfBody as exc:
                entry["missing_body"] = str(exc)
            else:
                entry["request"] = as_json(request)
                if client_cls is live.AuthnServiceClient and method.name == "PutMfaPolicy":
                    # Python restores the tenant MFA policy after a successful
                    # call so later logins are not challenged; so must Rust.
                    request.require_mfa = False
                    entry["reset_after_ok"] = as_json(request)
        rpcs.append(entry)

    plan = {
        "schema_version": SCHEMA_VERSION,
        "tenant_id": authed_meta.tenant_id,
        "project_id": authed_meta.project_id,
        "seeded_fixture_keys": sorted(fixtures.m),
        "sentinels": sorted(set(SENTINELS.values())),
        "cdc": {
            "message_type": live.LIVE_MESSAGE_TYPE,
            "record_id": seed_record_id,
            "conflict_fields": ["record_id"],
            "context": as_json(authed_meta.with_purpose("rust.live.perf.cdc").to_request_context()),
        },
        "rpcs": rpcs,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(plan, indent=1) + "\n", encoding="utf-8")
    missing = sum(1 for entry in rpcs if "missing_body" in entry)
    print(f"benchmark plan: {len(rpcs)} RPCs ({missing} without a seeded body) -> {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
