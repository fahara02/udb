#!/usr/bin/env python3
"""Bind a Pages refresh to a successful released-binary benchmark and its harness."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
from pathlib import Path


WORKFLOW_NAME = "Benchmark · SDKs"
WORKFLOW_PATH = ".github/workflows/benchmark-sdks.yml"
WIRE_FIELDS = (
    "service", "rpc", "wire_rpc", "api_alias", "operation_id", "op_kind", "request_msg",
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def wire_contract(rows: list[dict]) -> dict[tuple[str, str], tuple[str, ...]]:
    require(isinstance(rows, list) and bool(rows), "canonical RPC manifest is empty")
    contract = {}
    for row in rows:
        require(isinstance(row, dict), "invalid canonical RPC row")
        values = tuple(row.get(field) for field in WIRE_FIELDS)
        require(all(isinstance(value, str) and value for value in values), "missing canonical wire field")
        key = (row["service"], row["rpc"])
        require(key not in contract, "duplicate canonical wire identity")
        contract[key] = values
    return contract


def validate(
    bench: dict, run: dict, release_rows: list[dict], harness_rows: list[dict], *,
    repository: str, run_id: str, release_commit: str, harness_commit: str,
    latest_tag: str, recovery: bool,
) -> dict:
    require(re.fullmatch(r"[1-9][0-9]*", run_id) is not None, "invalid benchmark run id")
    require(re.fullmatch(r"[0-9a-f]{40}", release_commit) is not None, "invalid release commit")
    require(re.fullmatch(r"[0-9a-f]{40}", harness_commit) is not None, "invalid harness commit")
    require(run.get("id") == int(run_id), "selected benchmark run id mismatch")
    require(run.get("name") == WORKFLOW_NAME, "selected run is not the release benchmark workflow")
    require(run.get("path") == WORKFLOW_PATH, "selected benchmark workflow path mismatch")
    require((run.get("repository") or {}).get("full_name") == repository, "selected benchmark repository mismatch")
    require(run.get("status") == "completed" and run.get("conclusion") == "success", "selected benchmark did not finish successfully")
    allowed_events = {"workflow_run", "workflow_dispatch"} if recovery else {"workflow_run"}
    require(run.get("event") in allowed_events, "selected benchmark event is not eligible")
    environment = bench.get("environment") or {}
    require(str(environment.get("run_id") or "") == run_id, "benchmark artifact run_id does not match triggering run")
    require(environment.get("workflow") == WORKFLOW_NAME, "benchmark artifact workflow identity mismatch")
    require(bench.get("source") == "github-actions", "benchmark artifact source is not github-actions")
    require((bench.get("git") or {}).get("commit") == harness_commit, "benchmark artifact harness commit mismatch")
    release = bench.get("release") or {}
    require(re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", latest_tag) is not None, "invalid latest release tag")
    require(release.get("tag") == latest_tag, "benchmark does not measure the latest released tag")
    require(re.fullmatch(r"[0-9a-f]{64}", str(release.get("sha256") or "")) is not None, "benchmark has no released binary digest")
    if not recovery:
        require(harness_commit == release_commit, "automatic publication harness must match the release")
    require(wire_contract(release_rows) == wire_contract(harness_rows), "reviewed harness changes the released canonical RPC surface")
    return {
        "repository": repository, "run_id": run_id, "release_tag": latest_tag,
        "release_commit": release_commit, "harness_commit": harness_commit,
        "binary_sha256": release["sha256"], "recovery": recovery,
    }


def selftest() -> int:
    release_commit, harness_commit = "a" * 40, "b" * 40
    rows = [{"service": "AService", "rpc": "Read", "wire_rpc": "AService/Read",
             "api_alias": "read_a", "operation_id": "readA", "op_kind": "READ",
             "request_msg": "ReadRequest", "body": "old fixture"}]
    harness = copy.deepcopy(rows)
    harness[0]["body"] = "corrected fixture"
    run = {"id": 123, "name": WORKFLOW_NAME, "path": WORKFLOW_PATH,
           "repository": {"full_name": "owner/repo"}, "status": "completed",
           "conclusion": "success", "event": "workflow_dispatch"}
    bench = {"source": "github-actions", "environment": {"run_id": "123", "workflow": WORKFLOW_NAME},
             "git": {"commit": harness_commit}, "release": {"tag": "v0.5.29", "sha256": "c" * 64}}
    params = dict(repository="owner/repo", run_id="123", release_commit=release_commit,
                  harness_commit=harness_commit, latest_tag="v0.5.29", recovery=True)
    assert validate(bench, run, rows, harness, **params)["harness_commit"] == harness_commit
    automatic_run, automatic_bench = copy.deepcopy(run), copy.deepcopy(bench)
    automatic_run["event"] = "workflow_run"
    automatic_bench["git"]["commit"] = release_commit
    assert validate(automatic_bench, automatic_run, rows, rows,
                    **{**params, "harness_commit": release_commit, "recovery": False})["recovery"] is False
    cases = []
    for field, value in [("id", 124), ("name", "Benchmark · candidate"), ("path", ".github/workflows/benchmark-candidate.yml"),
                         ("repository", {"full_name": "other/repo"}), ("status", "in_progress"),
                         ("conclusion", "failure"), ("event", "push")]:
        changed = copy.deepcopy(run); changed[field] = value
        cases.append((bench, changed, rows, harness, params))
    for part, field, value in [("environment", "run_id", "124"), ("environment", "workflow", "Benchmark · candidate"),
                               ("git", "commit", "d" * 40), ("release", "tag", "v0.4.28"),
                               ("release", "sha256", "")]:
        changed = copy.deepcopy(bench); changed[part][field] = value
        cases.append((changed, run, rows, harness, params))
    for field in WIRE_FIELDS:
        changed = copy.deepcopy(harness); changed[0][field] += "Changed"
        cases.append((bench, run, rows, changed, params))
    cases.append((bench, run, rows, harness + harness, params))
    cases.append((bench, run, rows, [], params))
    cases.append((bench, run, rows, harness, {**params, "recovery": False}))
    for item in cases:
        try:
            validate(*item[:4], **item[4])
        except ValueError:
            continue
        raise AssertionError("ineligible benchmark publication accepted")
    print(f"benchmark publication selftest passed ({len(cases)} rejection cases)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selftest", action="store_true")
    parser.add_argument("--benchmark", type=Path)
    parser.add_argument("--run", type=Path)
    parser.add_argument("--release-manifest", type=Path)
    parser.add_argument("--harness-manifest", type=Path)
    parser.add_argument("--repository")
    parser.add_argument("--run-id")
    parser.add_argument("--release-commit")
    parser.add_argument("--harness-commit")
    parser.add_argument("--latest-tag")
    parser.add_argument("--recovery", action="store_true")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    if args.selftest:
        return selftest()
    for name in ["benchmark", "run", "release_manifest", "harness_manifest", "repository", "run_id",
                 "release_commit", "harness_commit", "latest_tag", "out"]:
        if not getattr(args, name):
            parser.error(f"--{name.replace('_', '-')} is required")
    try:
        result = validate(
            json.loads(args.benchmark.read_bytes()), json.loads(args.run.read_bytes()),
            json.loads(args.release_manifest.read_bytes()), json.loads(args.harness_manifest.read_bytes()),
            repository=args.repository, run_id=args.run_id, release_commit=args.release_commit,
            harness_commit=args.harness_commit, latest_tag=args.latest_tag, recovery=args.recovery,
        )
    except (ValueError, OSError) as error:
        parser.exit(1, f"benchmark publication refused: {error}\n")
    for name, path in [("benchmark_sha256", args.benchmark), ("release_manifest_sha256", args.release_manifest),
                       ("harness_manifest_sha256", args.harness_manifest)]:
        result[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print("released benchmark run and harness provenance verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
