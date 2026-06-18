"""Scenario perf bench — Python (R6.2 scenario-vs-surface split).

This is the SCENARIO counterpart to ``test_live_perf`` (the full 265-RPC coverage
sweep, which stays in ``test_live_conformance.py`` and writes ``perf_report_python.md``).
Where the full sweep answers "is every RPC reachable and how fast", this answers
"how fast is the user-facing facade path the simple-client docs tell people to call":

    - loginAndAdoptTenant   (Login + AuthenticateBearer + atomic adopt)
    - entity.upsert         (one Upsert RPC, no proof read)
    - entity.select         (one Select RPC)
    - entity.delete         (one Delete RPC, NON-existent row -> success path, no destroy)
    - uploadFile            (RegisterUpload -> presigned PUT -> FinalizeUpload)
    - downloadFile          (GetDownloadUrl — presigned, bytes never transit the broker)
    - events.subscribeReady (Subscribe + Ready — readiness boundary)
    - events.publishAndWait (EnqueueOutboxEvent + first matching CDC envelope)
    - webrtc.joinSession    (JoinSession + open signalling stream, then Leave)

It is gated SEPARATELY from both the conformance suite and the full-surface perf
sweep: ``UDB_LIVE_SDK_TESTS=1`` + ``UDB_SCENARIO_PERF=1``. The result is published to
its OWN file (``scenario_perf_python.md``) so it runs and reports independently of
``perf_report_python.md``. As with the full sweep, a slow scenario is reported, not
failed — only a scenario that NEVER completes is surfaced as a failure.

The latency includes the FULL facade cost a caller pays: client encode + every RPC in
the helper's documented sequence (see docs/bench-bodies/workflow-sequences.md) +
transport + server handle + decode + any presigned byte transfer. This mirrors the Go
reference bench (sdk/go/udbclient/live_scenario_perf_test.go) and the TS scenario bench
(sdk/typescript/live-auth.test.ts "live scenario perf").
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pytest

from udb_client import UdbConfig, UdbProject


# Gated SEPARATELY from the conformance module-level gate AND the full-surface perf
# sweep: this whole module is skipped unless BOTH live-SDK + scenario-perf flags are
# set, so it compiles + collects cleanly offline (backends/registry down) and only
# executes against a real broker.
pytestmark = pytest.mark.skipif(
    os.getenv("UDB_LIVE_SDK_TESTS") != "1" or os.getenv("UDB_SCENARIO_PERF") != "1",
    reason="scenario perf run requires UDB_LIVE_SDK_TESTS=1 and UDB_SCENARIO_PERF=1",
)

ENTITY = "udb.sdk.live.v1.SdkLiveRecord"


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise AssertionError(f"{name} is required when UDB_SCENARIO_PERF=1")
    return value


def _code_name(exc: BaseException) -> str:
    """gRPC status code NAME for an error, else the exception class name."""
    code = getattr(exc, "code", None)
    if callable(code):
        try:
            return exc.code().name  # type: ignore[union-attr]
        except Exception:
            return "UNKNOWN"
    return type(exc).__name__


@dataclass
class _Scenario:
    name: str
    seq: str
    iters: int
    fn: Callable[[], Any]
    # Warm-up is only safe for idempotent helpers (login rotates a session,
    # publishAndWait awaits a fresh event), so skip it where it would skew/consume.
    warmup: bool = True


@dataclass
class _Sample:
    name: str
    seq: str
    err: str
    p50: float
    p99: float
    mean: float
    minimum: float
    maximum: float
    iters: int


def _pct(sorted_ms: list[float], p: int) -> float:
    if not sorted_ms:
        return 0.0
    idx = min(len(sorted_ms) - 1, (p * (len(sorted_ms) - 1)) // 100)
    return sorted_ms[idx]


def _write_report(samples: list[_Sample], tenant: str) -> None:
    lines: list[str] = []
    lines.append("# UDB SDK Scenario Perf — Python (localhost)")
    lines.append("")
    lines.append(f"Scenarios measured: {len(samples)}   tenant={tenant}")
    lines.append("")
    lines.append(
        "This is the SCENARIO bench: it times the user-facing WORKFLOW HELPERS the "
        "simple-client docs prescribe (uploadFile, downloadFile, bound entity "
        "upsert/select/delete, loginAndAdoptTenant, events subscribe-ready/publish-and-wait, "
        "webrtc joinSession) — measured as end-to-end facade calls, NOT the raw 265-RPC "
        "surface (that stays in perf_report_python.md, test_live_perf). Each `seq` is the "
        "documented helper RPC sequence (docs/bench-bodies/workflow-sequences.md)."
    )
    lines.append("")
    lines.append("| Scenario | seq | err | p50 ms | p99 ms | mean ms | min ms | max ms | iters |")
    lines.append("|---|---|---|--:|--:|--:|--:|--:|--:|")
    for s in sorted(samples, key=lambda s: s.name):
        lines.append(
            f"| {s.name} | {s.seq} | {s.err} | {s.p50:.2f} | {s.p99:.2f} | "
            f"{s.mean:.2f} | {s.minimum:.2f} | {s.maximum:.2f} | {s.iters} |"
        )
    failed = [s for s in samples if s.err != "OK"]
    lines.append("")
    if not failed:
        lines.append("Every scenario ran its success path (no non-OK gRPC status).")
    else:
        lines.append(
            f"{len(failed)} scenario(s) returned a non-OK gRPC status "
            "(see [SCENARIO-FAIL] log lines)."
        )
    report_path = Path(__file__).resolve().parents[1] / "scenario_perf_python.md"
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(
        f"\nPython scenario perf: {len(samples)} workflow helpers measured, "
        f"{len(failed)} FAILED → sdk/python/scenario_perf_python.md"
    )


def test_scenario_perf():
    target = _required_env("UDB_GRPC_TARGET")
    auth_target = os.getenv("UDB_AUTH_GRPC_TARGET", "").strip() or target
    username = _required_env("UDB_LIVE_USERNAME")
    password = _required_env("UDB_LIVE_PASSWORD")
    tenant_hint = os.getenv("UDB_LIVE_TENANT", "sdk-live")
    project_id = os.getenv("UDB_LIVE_PROJECT", "default")

    # Build the unified facade exactly as a real caller would, then login-and-adopt so
    # every facade carries the canonical tenant from the VERIFIED principal — the same
    # path the simple-client docs prescribe.
    project = UdbProject(
        UdbConfig(
            target=target,
            auth_target=auth_target,
            tenant_id=tenant_hint,
            project_id=project_id,
            purpose="python.live.scenario.perf",
            service_identity="python.sdk.scenario.perf",
            correlation_id="python-live-scenario-perf",
            deadline=20.0,
        )
    )

    suffix = str(int(time.time() * 1e9))
    samples: list[_Sample] = []
    try:
        # Bootstrap adoption first so the data/native scenarios carry the canonical
        # tenant (the loginAndAdoptTenant scenario below still re-measures the helper).
        authn = project.login_and_adopt_tenant(username, password)
        tenant = authn.principal.tenant_id or tenant_hint

        entity = project.data.entity(ENTITY, key=["record_id"])

        def scenario_record(i: int) -> dict[str, Any]:
            return {
                "record_id": f"py-scn-{suffix}-{i}",
                "tenant_id": tenant,
                "project_id": project_id,
                "lookup_key": "py-scn-lk",
                "payload": "py-scenario",
                "revision": 1,
            }

        scenarios: list[_Scenario] = [
            _Scenario(
                name="loginAndAdoptTenant",
                seq="Login, AuthenticateBearer",
                iters=5,
                warmup=False,
                fn=lambda: project.login_and_adopt_tenant(username, password),
            ),
            _Scenario(
                name="entity.upsert",
                seq="Upsert",
                iters=10,
                fn=lambda: entity.upsert(scenario_record(0)),
            ),
            _Scenario(
                name="entity.select",
                seq="Select",
                iters=25,
                fn=lambda: entity.select(where={"tenant_id": tenant, "project_id": project_id}),
            ),
            _Scenario(
                name="entity.delete",
                seq="Delete",
                # Delete a NON-EXISTENT row so the helper runs its full path without
                # destroying a row a later iteration/scenario reads.
                iters=5,
                fn=lambda: entity.delete(
                    where={"record_id": "py-scn-delete-noop", "tenant_id": tenant, "project_id": project_id}
                ),
            ),
            _Scenario(
                name="uploadFile",
                seq="RegisterUpload, PUT, FinalizeUpload",
                iters=5,
                fn=lambda: project.storage.upload_file(
                    "py-scenario.txt",
                    b"py-scenario-upload",
                    {"content_type": "text/plain", "file_type": "DOCUMENT"},
                ),
            ),
            _Scenario(
                name="events.subscribeReady",
                seq="PublishCDC(open), Header",
                iters=5,
                fn=lambda: _subscribe_ready(project, f"{project_id}.*"),
            ),
            _Scenario(
                name="events.publishAndWait",
                seq="EnqueueOutboxEvent, PublishCDC first-event",
                iters=3,
                warmup=False,
                fn=lambda: project.events.publish_and_wait(
                    f"sdk.scenario.{suffix}",
                    {"event": "py-scenario", "n": suffix},
                    lambda _env: True,
                ),
            ),
        ]

        # downloadFile / webrtc.joinSession need a pre-existing file / room. Seed one of
        # each up front (their cost is NOT measured) so the measured scenario is the pure
        # helper path. A failed seed downgrades the scenario to a recorded skip.
        try:
            up = project.storage.upload_file(
                "py-scenario-dl.txt",
                b"py-scenario-download",
                {"content_type": "text/plain", "file_type": "DOCUMENT"},
            )
            file_id = getattr(getattr(up, "file", None), "file_id", "") or getattr(up, "file_id", "")
            if file_id:
                scenarios.append(
                    _Scenario(
                        name="downloadFile",
                        seq="GetDownloadUrl",
                        iters=25,
                        fn=lambda fid=file_id: project.storage.download_file(fid, expires_in_minutes=5),
                    )
                )
        except Exception as exc:  # noqa: BLE001 - seed failure downgrades to a skip
            print(f"scenario seed: download file upload failed, downloadFile scenario skipped: {_code_name(exc)}")

        try:
            room = project.webrtc.room.create_room(name=f"py-scenario-room-{suffix}", max_participants=8, config="{}")
            room_id = getattr(room, "room_id", "") or getattr(getattr(room, "room", None), "room_id", "")
            if room_id:
                scenarios.append(
                    _Scenario(
                        name="webrtc.joinSession",
                        seq="JoinSession, Signal(open)",
                        iters=5,
                        warmup=False,
                        fn=lambda rid=room_id: _join_and_leave(project, rid),
                    )
                )
        except Exception as exc:  # noqa: BLE001 - seed failure downgrades to a skip
            print(f"scenario seed: webrtc room create failed, joinSession scenario skipped: {_code_name(exc)}")

        # ── measurement loop ────────────────────────────────────────────────────
        for sc in scenarios:
            if sc.warmup:
                try:
                    sc.fn()
                except Exception:  # noqa: BLE001 - warm-up errors are ignored
                    pass
            ok_ms: list[float] = []
            all_ms: list[float] = []
            first_err = "OK"
            first_detail = ""
            for i in range(sc.iters):
                start = time.perf_counter()
                err = "OK"
                try:
                    sc.fn()
                except Exception as exc:  # noqa: BLE001 - record the status, never raise
                    err = _code_name(exc)
                    if i == 0:
                        first_detail = (getattr(exc, "details", lambda: "")() if callable(getattr(exc, "details", None)) else str(exc))[:200]
                ms = (time.perf_counter() - start) * 1000.0
                if i == 0:
                    first_err = err
                if err == "OK":
                    ok_ms.append(ms)
                all_ms.append(ms)
            measured = sorted(ok_ms or all_ms)
            err_code = "OK" if ok_ms else first_err
            if err_code != "OK":
                print(f"[SCENARIO-FAIL] {sc.name} => {err_code}: {first_detail}")
            samples.append(
                _Sample(
                    name=sc.name,
                    seq=sc.seq,
                    err=err_code,
                    p50=_pct(measured, 50),
                    p99=_pct(measured, 99),
                    mean=sum(measured) / len(measured),
                    minimum=measured[0],
                    maximum=measured[-1],
                    iters=sc.iters,
                )
            )
    finally:
        _write_report(samples, getattr(getattr(locals().get("authn", None), "principal", None), "tenant_id", "") or tenant_hint)
        project.close()


def _subscribe_ready(project: UdbProject, topic: str) -> None:
    """events.subscribeReady scenario body: open a CDC stream and resolve readiness."""
    sub = project.events.subscribe(topic)
    try:
        sub.ready()
    finally:
        sub.cancel()


def _join_and_leave(project: UdbProject, room_id: str) -> None:
    """webrtc.joinSession scenario body: JoinSession then Leave (Leave is not timed
    separately here — the helper cost is the join + signalling open)."""
    session = project.webrtc.join_session(room_id, display_name="py-scenario-peer", ttl_seconds=60)
    try:
        session.leave()
    except Exception:  # noqa: BLE001 - leave is best-effort cleanup
        pass
