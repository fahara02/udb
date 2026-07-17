#!/usr/bin/env python3
"""Fail CI if WorkflowService tick posture drifts."""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(root: Path, path: str) -> str:
    p = root / path
    if p.is_dir():
        # A path may name a modularized service DIRECTORY; read all its `.rs`
        # files concatenated so tokens split across submodules (tick/events/…)
        # are still found after a god-file → module-tree refactor.
        return "\n".join(f.read_text(encoding="utf-8") for f in sorted(p.rglob("*.rs")))
    return p.read_text(encoding="utf-8")


def _require(text: str, needle: str, label: str, failures: list[str]) -> None:
    if needle not in text:
        failures.append(f"missing {label}: {needle}")


def _reject(text: str, needle: str, label: str, failures: list[str]) -> None:
    if needle in text:
        failures.append(f"forbidden {label}: {needle}")


def _window(text: str, needle: str, radius: int = 1600) -> str:
    pos = text.rfind(needle)
    if pos < 0:
        return ""
    start = max(0, pos - radius)
    end = min(len(text), pos + len(needle) + radius)
    return text[start:end]


def check_source(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    singleton = _read(root, "src/runtime/singleton.rs")
    service_mod = _read(root, "src/runtime/service/mod.rs")
    workflow = _read(root, "src/runtime/service/workflow_service")
    saga = _read(root, "src/runtime/saga.rs")
    ci = _read(root, ".github/workflows/ci.yml")

    _require(
        singleton,
        'pub const WORKER_WORKFLOW_TICK: &str = "udb:workflow:tick";',
        "workflow singleton worker id",
        failures,
    )
    _require(
        saga,
        "pub enum SagaKind",
        "saga kind discriminator",
        failures,
    )
    _require(
        saga,
        "Workflow",
        "workflow saga kind variant",
        failures,
    )

    worker_window = _window(service_mod, "WORKER_WORKFLOW_TICK")
    if not worker_window:
        failures.append("service/mod.rs: missing WORKER_WORKFLOW_TICK serve wiring")
    else:
        _require(
            worker_window,
            "NativeWorkerHost::spawn_while_leader",
            "leader-elected workflow worker spawn",
            failures,
        )
        _require(
            worker_window,
            "run_workflow_tick_once",
            "workflow tick callback",
            failures,
        )
        _require(
            worker_window,
            "WORKFLOW_TICK_BATCH",
            "bounded workflow tick batch",
            failures,
        )
        _require(
            worker_window,
            "outbox_relation",
            "transactional outbox relation capture",
            failures,
        )
        _require(
            worker_window,
            "default_system_stores",
            "saga store handoff",
            failures,
        )
        _reject(
            worker_window,
            "tokio::spawn",
            "bare workflow tick spawn inside serve wiring",
            failures,
        )

    _require(
        workflow,
        "pub(crate) async fn run_workflow_tick_once",
        "workflow tick entrypoint",
        failures,
    )
    _require(
        workflow,
        "FOR UPDATE SKIP LOCKED",
        "skip-locked workflow claim",
        failures,
    )
    _require(
        workflow,
        "insert_tick_outbox(",
        "tick outbox enqueue",
        failures,
    )
    _require(
        workflow,
        "crate::runtime::cdc::insert_outbox_row",
        "shared CDC outbox insert",
        failures,
    )
    _require(
        workflow,
        "build_native_compliance_envelope",
        "shared compliance envelope",
        failures,
    )
    _require(
        workflow,
        "SagaKind::Workflow.tag_operation",
        "workflow saga tagging",
        failures,
    )
    _require(
        workflow,
        "SagaStore::update_saga_status",
        "completed workflow saga settle",
        failures,
    )
    _require(
        workflow,
        "SagaStatus::Committed",
        "completed workflow terminal saga status",
        failures,
    )
    _require(
        workflow,
        "CompensationStatus::None",
        "completed workflow compensation status",
        failures,
    )
    _reject(
        workflow,
        "tokio::spawn(async move { run_workflow_tick_once",
        "workflow-local background tick loop",
        failures,
    )

    _require(
        ci,
        "python3 scripts/check-workflow-service-posture.py",
        "CI quick-gate workflow service posture guard",
        failures,
    )
    return failures


def run_selftest() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for path in (
            "src/runtime",
            "src/runtime/service",
            "src/runtime/service/workflow_service",
            ".github/workflows",
        ):
            (root / path).mkdir(parents=True, exist_ok=True)
        (root / "src/runtime/singleton.rs").write_text(
            'pub const WORKER_WORKFLOW_TICK: &str = "udb:workflow:tick";\n',
            encoding="utf-8",
        )
        (root / "src/runtime/saga.rs").write_text(
            "pub enum SagaKind { Default, Workflow }\n",
            encoding="utf-8",
        )
        service_mod = """
crate::runtime::service::native_runtime::NativeWorkerHost::spawn_while_leader(
    crate::runtime::singleton::WORKER_WORKFLOW_TICK,
    "workflow tick advanced due instances",
    lease_pool,
    singleton_relation,
    tick_interval,
    move || {
        let outbox = outbox_relation.clone();
        let stores = workflow_runtime.default_system_stores();
        async move {
            crate::runtime::service::workflow_service::run_workflow_tick_once(
                &pool,
                Some(&outbox),
                stores,
                crate::runtime::service::workflow_service::WORKFLOW_TICK_BATCH,
            ).await
        }
    },
);
"""
        (root / "src/runtime/service/mod.rs").write_text(service_mod, encoding="utf-8")
        workflow = """
pub(crate) async fn run_workflow_tick_once() {
    let _ = "FOR UPDATE SKIP LOCKED";
    insert_tick_outbox();
    crate::runtime::cdc::insert_outbox_row();
    build_native_compliance_envelope();
    SagaKind::Workflow.tag_operation("demo");
    SagaStore::update_saga_status();
    SagaStatus::Committed;
    CompensationStatus::None;
}
"""
        (root / "src/runtime/service/workflow_service/mod.rs").write_text(
            workflow,
            encoding="utf-8",
        )
        (root / ".github/workflows/ci.yml").write_text(
            "run: python3 scripts/check-workflow-service-posture.py\n",
            encoding="utf-8",
        )
        failures = check_source(root)
        if failures:
            raise AssertionError(f"expected clean fixture, got {failures}")

        (root / "src/runtime/service/mod.rs").write_text(
            service_mod.replace(
                "NativeWorkerHost::spawn_while_leader",
                "tokio::spawn",
            ),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("leader-elected workflow worker spawn" in f for f in failures):
            raise AssertionError(f"expected leader-spawn failure, got {failures}")

    print("workflow service posture selftest passed")
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
    print("workflow service posture guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
