#!/usr/bin/env python3
"""Gate-D smoke for the full embedding sidecar callback loop.

This harness intentionally avoids generated embedding Python stubs because Gate C
SDK regeneration may not have produced them yet. Live mode consumes one durable
`udb.embedding.work.v1` payload from the transactional outbox or CDC journal,
posts it to the embedding sidecar, then calls the broker's internal
EmbeddingService.ReportEmbedding callback through grpcurl in checked-in proto
mode. Reflection is opt-in because the native listener does not have to expose it.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
TOPIC_WORK = "udb.embedding.work.v1"
REPORT_METHOD = "udb.core.embedding.services.v1.EmbeddingService/ReportEmbedding"
CALLBACK_PROTO = "proto/udb/core/embedding/services/v1/embedding_service.proto"
DEFAULT_PROTO_IMPORT_PATHS = ("proto", "third_party/googleapis")
RELATION_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")
FORBIDDEN_WORK_KEYS = {
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "credentials",
    "key",
    "password",
    "secret",
    "token",
}


class SmokeError(RuntimeError):
    pass


def check_no_credentials(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).strip().lower().replace("-", "_")
            if normalized in FORBIDDEN_WORK_KEYS:
                raise SmokeError(f"work payload contains forbidden credential key at {path}.{key}")
            check_no_credentials(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            check_no_credentials(child, f"{path}[{index}]")


def required_str(mapping: dict[str, Any], key: str) -> str:
    value = str(mapping.get(key, "")).strip()
    if not value:
        raise SmokeError(f"field {key!r} is required")
    return value


def normalize_work(value: dict[str, Any]) -> dict[str, Any]:
    check_no_credentials(value)
    candidate = value
    domain_keys = ("row_pk", "text", "model_id")
    while not all(candidate.get(key) for key in domain_keys) and isinstance(
        candidate.get("payload"), dict
    ):
        candidate = candidate["payload"]
    normalized = dict(candidate)
    for key in ("tenant_id", "project_id", "source", "row_pk", "text", "model_id"):
        normalized[key] = required_str(candidate, key)
    normalized["target_collection"] = str(candidate.get("target_collection", "")).strip()
    return normalized


def request_json(method: str, url: str, body: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
    data = None if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"} if body is not None else {},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8") or "{}")


def sidecar_embed(sidecar_url: str, work: dict[str, Any]) -> dict[str, Any]:
    status, payload = request_json("POST", f"{sidecar_url.rstrip('/')}/embed", work)
    if status != 200:
        raise SmokeError(f"embedding sidecar failed: status={status} payload={payload}")
    report = payload.get("report_embedding_request")
    if not isinstance(report, dict):
        raise SmokeError(f"embedding sidecar did not return report_embedding_request: {payload}")
    return validate_report(work, report)


def validate_report(work: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    expected = {
        "tenant_id": work["tenant_id"],
        "source_name": work["source"],
        "row_pk": work["row_pk"],
        "model": work["model_id"],
    }
    for key, value in expected.items():
        if report.get(key) != value:
            raise SmokeError(f"report field {key!r} mismatch: expected {value!r}, got {report.get(key)!r}")
    vector = report.get("vector")
    if not isinstance(vector, list) or not vector:
        raise SmokeError(f"report vector is missing/empty: {report}")
    if any(not isinstance(value, (int, float)) for value in vector):
        raise SmokeError(f"report vector contains non-numeric values: {vector}")
    dims = int(report.get("dims") or len(vector))
    if dims != len(vector):
        raise SmokeError(f"report dims={dims} does not match vector length={len(vector)}")
    normalized = {
        "tenant_id": report["tenant_id"],
        "source_name": report["source_name"],
        "row_pk": report["row_pk"],
        "vector": [float(value) for value in vector],
        "model": report["model"],
        "dims": dims,
    }
    for key in ("work_item_id", "chunk_hash", "token_count", "vector_name"):
        if key in work and report.get(key) != work[key]:
            raise SmokeError(f"report field {key!r} did not echo durable work identity")
        if key in report:
            normalized[key] = report[key]
    return normalized


def assert_relation(value: str) -> str:
    if not RELATION_RE.fullmatch(value):
        raise SmokeError(f"unsafe SQL relation name: {value!r}")
    return value


def psql_json(dsn: str, sql: str) -> dict[str, Any]:
    psql = shutil.which("psql")
    if not psql:
        raise SmokeError("psql is required when --pg-dsn is used")
    proc = subprocess.run(
        [psql, dsn, "-AtX", "-v", "ON_ERROR_STOP=1", "-c", sql],
        cwd=str(ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode != 0:
        raise SmokeError(f"psql query failed: {proc.stderr.strip()}")
    raw = proc.stdout.strip()
    if not raw:
        raise SmokeError(f"no {TOPIC_WORK} payload found")
    return json.loads(raw)


def load_work_from_postgres(args: argparse.Namespace) -> dict[str, Any]:
    outbox_relation = assert_relation(args.outbox_relation)
    journal_relation = assert_relation(args.journal_relation)
    if args.work_source == "outbox":
        sql = (
            f"SELECT payload::text FROM {outbox_relation} "
            f"WHERE topic = '{TOPIC_WORK}' ORDER BY created_at DESC, event_id DESC LIMIT 1"
        )
    elif args.work_source == "journal":
        sql = (
            f"SELECT payload::text FROM {journal_relation} "
            f"WHERE topic = '{TOPIC_WORK}' ORDER BY published_at DESC, event_id DESC LIMIT 1"
        )
    else:
        sql = (
            "WITH candidates AS ("
            f"SELECT payload, created_at AS observed_at, event_id::text AS event_id FROM {outbox_relation} "
            f"WHERE topic = '{TOPIC_WORK}' "
            "UNION ALL "
            f"SELECT payload, published_at AS observed_at, event_id::text AS event_id FROM {journal_relation} "
            f"WHERE topic = '{TOPIC_WORK}'"
            ") SELECT payload::text FROM candidates ORDER BY observed_at DESC, event_id DESC LIMIT 1"
        )
    return normalize_work(psql_json(args.pg_dsn, sql))


def load_work(args: argparse.Namespace) -> dict[str, Any]:
    if args.work_json:
        return normalize_work(json.loads(args.work_json))
    if args.work_json_file:
        return normalize_work(json.loads(Path(args.work_json_file).read_text(encoding="utf-8")))
    if args.pg_dsn:
        return load_work_from_postgres(args)
    raise SmokeError("provide --work-json, --work-json-file, or --pg-dsn")


def grpcurl_command(args: argparse.Namespace, tenant_id: str, project_id: str) -> list[str]:
    cmd = [args.grpcurl]
    if args.plaintext:
        cmd.append("-plaintext")
    if not args.use_reflection:
        for import_path in args.proto_import_path or DEFAULT_PROTO_IMPORT_PATHS:
            cmd.extend(["-import-path", str((ROOT / import_path).resolve())])
        cmd.extend(["-proto", str((ROOT / args.proto).resolve())])
    for header in [
        f"x-tenant-id: {tenant_id}",
        f"x-project-id: {project_id}",
        "x-purpose: embedding-sidecar-roundtrip-smoke",
        "x-request-id: embedding-sidecar-roundtrip-smoke",
        "x-correlation-id: embedding-sidecar-roundtrip-smoke",
        "x-udb-scopes: udb:embedding:report-embedding",
    ]:
        cmd.extend(["-H", header])
    if args.bearer_token:
        cmd.extend(["-H", "authorization: Bearer REDACTED"])
    cmd.extend(["-d", "@", args.broker, REPORT_METHOD])
    return cmd


def call_report_embedding(args: argparse.Namespace, report: dict[str, Any], tenant_id: str, project_id: str) -> dict[str, Any]:
    if not shutil.which(args.grpcurl):
        raise SmokeError(f"{args.grpcurl!r} is required for the ReportEmbedding callback")
    cmd = grpcurl_command(args, tenant_id, project_id)
    actual_cmd = list(cmd)
    if args.bearer_token:
        redacted = actual_cmd.index("authorization: Bearer REDACTED")
        actual_cmd[redacted] = f"authorization: Bearer {args.bearer_token}"
    proc = subprocess.run(
        actual_cmd,
        input=json.dumps(report, separators=(",", ":")),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode != 0:
        raise SmokeError(f"ReportEmbedding callback failed: {proc.stderr.strip()}")
    payload = json.loads(proc.stdout or "{}")
    if payload.get("upserted") is not True:
        raise SmokeError(f"ReportEmbedding did not upsert: {payload}")
    return payload


def selftest() -> int:
    work = normalize_work(
        {
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "source": "contacts",
            "row_pk": "contact-1",
            "text": "Ada Lovelace",
            "model_id": "deterministic-v1",
            "target_collection": "contacts_vec",
        }
    )
    wrapped = normalize_work(
        {
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "source": "udb.native/tenant-a",
            "payload": work,
        }
    )
    if wrapped["source"] != "contacts" or wrapped["project_id"] != "project-a":
        raise SmokeError("compliance envelope was not unwrapped to canonical embedding work")
    report = validate_report(
        work,
        {
            "tenant_id": "tenant-a",
            "source_name": "contacts",
            "row_pk": "contact-1",
            "vector": [0.1, 0.2],
            "model": "deterministic-v1",
            "dims": 2,
        },
    )
    parser = build_parser()
    args = parser.parse_args(["--work-json", json.dumps(work), "--dry-run"])
    cmd = grpcurl_command(args, "tenant-a", "project-a")
    joined = " ".join(cmd)
    if REPORT_METHOD not in joined or "x-tenant-id: tenant-a" not in joined:
        raise SmokeError(f"grpcurl command missing required callback metadata: {joined}")
    if "-proto" not in cmd or "embedding_service.proto" not in joined or "third_party" not in joined:
        raise SmokeError(f"grpcurl command must use checked-in proto mode by default: {joined}")
    reflection_args = parser.parse_args(
        ["--work-json", json.dumps(work), "--dry-run", "--use-reflection"]
    )
    if "-proto" in grpcurl_command(reflection_args, "tenant-a", "project-a"):
        raise SmokeError("grpcurl --use-reflection must not add proto import flags")
    try:
        normalize_work({"tenant_id": "tenant-a", "api_key": "nope"})
    except SmokeError:
        pass
    else:
        raise SmokeError("credential-shaped work key was not rejected")
    if report["dims"] != 2:
        raise SmokeError("report normalization failed")
    print(json.dumps({"ok": True, "selftest": "embedding-sidecar-roundtrip"}, separators=(",", ":")))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Smoke-test embedding work -> sidecar -> ReportEmbedding")
    parser.add_argument("--selftest", action="store_true", help="run offline parser/command selftest")
    parser.add_argument("--sidecar-url", default="http://127.0.0.1:58090")
    parser.add_argument("--broker", default="127.0.0.1:50061", help="internal/control-plane gRPC target")
    parser.add_argument(
        "--project-id",
        default="",
        help="optional assertion that the work envelope carries this project id",
    )
    parser.add_argument("--bearer-token", default=os.environ.get("UDB_BEARER_TOKEN", ""))
    parser.add_argument("--grpcurl", default=os.environ.get("GRPCURL", "grpcurl"))
    parser.add_argument("--plaintext", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--use-reflection",
        action="store_true",
        help="use server reflection instead of checked-in proto descriptors",
    )
    parser.add_argument("--proto", default=CALLBACK_PROTO, help="ReportEmbedding callback proto path")
    parser.add_argument(
        "--proto-import-path",
        action="append",
        help="grpcurl proto import path; repeatable (defaults to proto and third_party/googleapis)",
    )
    parser.add_argument("--dry-run", action="store_true", help="validate sidecar report but do not call broker")
    parser.add_argument("--work-json", help="inline udb.embedding.work.v1 JSON payload")
    parser.add_argument("--work-json-file", help="file containing one work payload JSON object")
    parser.add_argument("--pg-dsn", default=os.environ.get("UDB_INTEGRATION_PG_DSN", ""))
    parser.add_argument("--work-source", choices=["either", "outbox", "journal"], default="either")
    parser.add_argument("--outbox-relation", default="udb_system.outbox_events")
    parser.add_argument("--journal-relation", default="udb_system.udb_cdc_event_journal")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.selftest:
        return selftest()
    work = load_work(args)
    if args.project_id and args.project_id != work["project_id"]:
        raise SmokeError(
            f"work project_id={work['project_id']!r} does not match --project-id={args.project_id!r}"
        )
    report = sidecar_embed(args.sidecar_url, work)
    if args.dry_run:
        print(
            json.dumps(
                {
                    "ok": True,
                    "dry_run": True,
                    "work": {k: work[k] for k in ["tenant_id", "project_id", "source", "row_pk", "model_id"]},
                    "grpcurl": grpcurl_command(args, work["tenant_id"], work["project_id"]),
                },
                separators=(",", ":"),
            )
        )
        return 0
    response = call_report_embedding(args, report, work["tenant_id"], work["project_id"])
    print(
        json.dumps(
            {
                "ok": True,
                "tenant_id": work["tenant_id"],
                "source": work["source"],
                "row_pk": work["row_pk"],
                "report_embedding": response,
            },
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SmokeError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, separators=(",", ":")), file=sys.stderr)
        raise SystemExit(1)
