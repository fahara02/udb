#!/usr/bin/env python3
"""Gate-D smoke for notification sidecar -> ReportDelivery reconciliation.

Live mode loads one queued PENDING NotificationLog intent (or accepts explicit
JSON), calls the notification provider sidecar using the same generic POST body
the broker worker sends, then reports the provider outcome to the broker's
internal NotificationService.ReportDelivery RPC through grpcurl in checked-in
proto mode. Reflection is opt-in because the native listener does not have to
expose it.
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
REPORT_METHOD = "udb.core.notification.services.v1.NotificationService/ReportDelivery"
CALLBACK_PROTO = "proto/udb/core/notification/services/v1/notification_service.proto"
DEFAULT_PROTO_IMPORT_PATHS = ("proto", "third_party/googleapis")
RELATION_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")

CHANNEL_ENUM = {
    "EMAIL": "NOTIFICATION_CHANNEL_EMAIL",
    "SMS": "NOTIFICATION_CHANNEL_SMS",
    "PUSH": "NOTIFICATION_CHANNEL_PUSH",
    "IN_APP": "NOTIFICATION_CHANNEL_IN_APP",
    "WEBHOOK": "NOTIFICATION_CHANNEL_WEBHOOK",
}
STATUS_ENUM = {
    "SENT": "NOTIFICATION_STATUS_SENT",
    "DELIVERED": "NOTIFICATION_STATUS_DELIVERED",
    "FAILED": "NOTIFICATION_STATUS_FAILED",
}


class SmokeError(RuntimeError):
    pass


def required_str(mapping: dict[str, Any], key: str) -> str:
    value = str(mapping.get(key, "")).strip()
    if not value:
        raise SmokeError(f"field {key!r} is required")
    return value


def normalize_channel(value: Any) -> tuple[str, str]:
    raw = str(value or "").strip().upper()
    if raw.startswith("NOTIFICATION_CHANNEL_"):
        enum = raw
        key = raw.removeprefix("NOTIFICATION_CHANNEL_")
    elif raw in CHANNEL_ENUM:
        enum = CHANNEL_ENUM[raw]
        key = raw
    else:
        raise SmokeError(f"unsupported notification channel {value!r}")
    return key, enum


def normalize_intent(value: dict[str, Any]) -> dict[str, str]:
    channel_key, channel_enum = normalize_channel(value.get("channel", "EMAIL"))
    return {
        "tenant_id": required_str(value, "tenant_id"),
        "project_id": str(value.get("project_id", "")).strip(),
        "log_id": required_str(value, "log_id"),
        "channel": channel_key,
        "channel_enum": channel_enum,
        "recipient_address": required_str(value, "recipient_address"),
        "rendered_subject": str(value.get("rendered_subject", "")).strip(),
        "rendered_body": required_str(value, "rendered_body"),
    }


def request_json(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], dict[str, Any]]:
    data = None if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, dict(resp.headers.items()), json.loads(resp.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers.items()), json.loads(exc.read().decode("utf-8") or "{}")


def sidecar_send(sidecar_url: str, intent: dict[str, str], provider_credential: str) -> dict[str, str]:
    status, headers, payload = request_json(
        "POST",
        f"{sidecar_url.rstrip('/')}/send",
        {
            "to": intent["recipient_address"],
            "subject": intent["rendered_subject"],
            "body": intent["rendered_body"],
        },
        {
            "Authorization": f"Bearer {provider_credential}",
            "Content-Type": "application/json",
        },
    )
    provider_message_id = next(
        (value for key, value in headers.items() if key.lower() == "x-provider-message-id"),
        "",
    )
    if status != 200:
        return {
            "status": "FAILED",
            "provider_message_id": "",
            "error_message": str(payload.get("error") or f"sidecar returned HTTP {status}"),
        }
    if not provider_message_id or provider_message_id != payload.get("provider_message_id"):
        raise SmokeError(
            f"sidecar response missing/mismatched x-provider-message-id: "
            f"header={provider_message_id!r} payload={payload}"
        )
    return {"status": "SENT", "provider_message_id": provider_message_id, "error_message": ""}


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
        raise SmokeError("no queued PENDING notification intent found")
    return json.loads(raw)


def load_intent_from_postgres(args: argparse.Namespace) -> dict[str, str]:
    log_rel = assert_relation(args.notification_log_relation)
    attempt_rel = assert_relation(args.delivery_attempt_relation)
    sql = (
        "SELECT json_build_object("
        "'log_id', l.log_id::text, "
        "'tenant_id', l.tenant_id::text, "
        "'project_id', COALESCE(l.project_id::text, ''), "
        "'channel', l.channel::text, "
        "'recipient_address', COALESCE(l.recipient_address::text, ''), "
        "'rendered_subject', COALESCE(l.rendered_subject::text, ''), "
        "'rendered_body', COALESCE(l.rendered_body::text, '')"
        f")::text FROM {log_rel} l "
        "WHERE l.status = 'PENDING' "
        "AND COALESCE(l.tenant_id::text, '') <> '' "
        "AND COALESCE(l.recipient_address::text, '') <> '' "
        f"AND NOT EXISTS (SELECT 1 FROM {attempt_rel} a "
        "WHERE a.notification_id = l.log_id "
        "AND a.channel = l.channel "
        "AND a.status IN ('SENT','DELIVERED')) "
        "ORDER BY l.created_at ASC, l.log_id ASC LIMIT 1"
    )
    return normalize_intent(psql_json(args.pg_dsn, sql))


def load_intent(args: argparse.Namespace) -> dict[str, str]:
    if args.intent_json:
        return normalize_intent(json.loads(args.intent_json))
    if args.intent_json_file:
        return normalize_intent(json.loads(Path(args.intent_json_file).read_text(encoding="utf-8")))
    if args.pg_dsn:
        return load_intent_from_postgres(args)
    raise SmokeError("provide --intent-json, --intent-json-file, or --pg-dsn")


def build_report(intent: dict[str, str], provider: str, outcome: dict[str, str]) -> dict[str, Any]:
    status = outcome["status"].upper()
    if status not in STATUS_ENUM:
        raise SmokeError(f"unsupported delivery outcome status {status!r}")
    return {
        "tenantId": intent["tenant_id"],
        "logId": intent["log_id"],
        "channel": intent["channel_enum"],
        "provider": provider,
        "status": STATUS_ENUM[status],
        "providerMessageId": outcome["provider_message_id"],
        "errorMessage": outcome["error_message"],
    }


def grpcurl_command(args: argparse.Namespace, tenant_id: str, project_id: str) -> list[str]:
    cmd = [args.grpcurl]
    if args.plaintext:
        cmd.append("-plaintext")
    if not args.use_reflection:
        for import_path in args.proto_import_path or DEFAULT_PROTO_IMPORT_PATHS:
            cmd.extend(["-import-path", str((ROOT / import_path).resolve())])
        cmd.extend(["-proto", str((ROOT / args.proto).resolve())])
    headers = [
        f"x-tenant-id: {tenant_id}",
        "x-purpose: notify-sidecar-roundtrip-smoke",
        "x-request-id: notify-sidecar-roundtrip-smoke",
        "x-correlation-id: notify-sidecar-roundtrip-smoke",
        "x-udb-scopes: udb:notification:report-delivery",
    ]
    if project_id:
        headers.append(f"x-udb-project-id: {project_id}")
    for header in headers:
        cmd.extend(["-H", header])
    if args.bearer_token:
        cmd.extend(["-H", "authorization: Bearer REDACTED"])
    cmd.extend(["-d", "@", args.broker, REPORT_METHOD])
    return cmd


def call_report_delivery(args: argparse.Namespace, report: dict[str, Any], tenant_id: str, project_id: str) -> dict[str, Any]:
    if not shutil.which(args.grpcurl):
        raise SmokeError(f"{args.grpcurl!r} is required for the ReportDelivery callback")
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
        raise SmokeError(f"ReportDelivery callback failed: {proc.stderr.strip()}")
    payload = json.loads(proc.stdout or "{}")
    if "attempt" not in payload:
        raise SmokeError(f"ReportDelivery returned no attempt: {payload}")
    return payload


def selftest() -> int:
    intent = normalize_intent(
        {
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "log_id": "00000000-0000-0000-0000-000000000001",
            "channel": "EMAIL",
            "recipient_address": "ops@example.com",
            "rendered_subject": "UDB notify sidecar smoke",
            "rendered_body": "dry-run delivery",
        }
    )
    outcome = {
        "status": "SENT",
        "provider_message_id": "dryrun-abc",
        "error_message": "",
    }
    report = build_report(intent, "smtp", outcome)
    if report["channel"] != "NOTIFICATION_CHANNEL_EMAIL" or report["status"] != "NOTIFICATION_STATUS_SENT":
        raise SmokeError(f"bad report enum mapping: {report}")
    parser = build_parser()
    args = parser.parse_args(["--intent-json", json.dumps(intent), "--dry-run"])
    joined = " ".join(grpcurl_command(args, "tenant-a", "project-a"))
    if REPORT_METHOD not in joined or "x-udb-scopes: udb:notification:report-delivery" not in joined:
        raise SmokeError(f"grpcurl command missing required callback metadata: {joined}")
    if "-proto" not in joined or "notification_service.proto" not in joined or "third_party" not in joined:
        raise SmokeError(f"grpcurl command must use checked-in proto mode by default: {joined}")
    reflection_args = parser.parse_args(
        ["--intent-json", json.dumps(intent), "--dry-run", "--use-reflection"]
    )
    if "-proto" in grpcurl_command(reflection_args, "tenant-a", "project-a"):
        raise SmokeError("grpcurl --use-reflection must not add proto import flags")
    try:
        normalize_intent({"tenant_id": "tenant-a", "log_id": "id", "channel": "FAX"})
    except SmokeError:
        pass
    else:
        raise SmokeError("unsupported channel was not rejected")
    mismatch_args = parser.parse_args(
        ["--intent-json", json.dumps(intent), "--dry-run", "--project-id", "project-b"]
    )
    try:
        assert_project(mismatch_args, intent)
    except SmokeError:
        pass
    else:
        raise SmokeError("--project-id mismatch was not rejected")
    print(json.dumps({"ok": True, "selftest": "notify-sidecar-roundtrip"}, separators=(",", ":")))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Smoke-test notify sidecar -> ReportDelivery")
    parser.add_argument("--selftest", action="store_true", help="run offline parser/command selftest")
    parser.add_argument("--sidecar-url", default="http://127.0.0.1:58080")
    parser.add_argument("--provider", default=os.environ.get("UDB_NOTIFY_PROVIDER", "smtp"))
    parser.add_argument("--provider-credential", default=os.environ.get("UDB_NOTIFY_PROVIDER_CREDENTIAL", "smoke-credential"))
    parser.add_argument("--broker", default="127.0.0.1:50061", help="internal/control-plane gRPC target")
    parser.add_argument(
        "--project-id",
        default=os.environ.get("UDB_PROJECT_ID", ""),
        help="optional assertion; must equal the project stored on the queued intent",
    )
    parser.add_argument("--bearer-token", default=os.environ.get("UDB_BEARER_TOKEN", ""))
    parser.add_argument("--grpcurl", default=os.environ.get("GRPCURL", "grpcurl"))
    parser.add_argument("--plaintext", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--use-reflection",
        action="store_true",
        help="use server reflection instead of checked-in proto descriptors",
    )
    parser.add_argument("--proto", default=CALLBACK_PROTO, help="ReportDelivery callback proto path")
    parser.add_argument(
        "--proto-import-path",
        action="append",
        help="grpcurl proto import path; repeatable (defaults to proto and third_party/googleapis)",
    )
    parser.add_argument("--dry-run", action="store_true", help="validate sidecar/report body but do not call broker")
    parser.add_argument("--intent-json", help="inline queued notification intent JSON")
    parser.add_argument("--intent-json-file", help="file containing one queued intent JSON object")
    parser.add_argument("--pg-dsn", default=os.environ.get("UDB_INTEGRATION_PG_DSN", ""))
    parser.add_argument("--notification-log-relation", default="udb_notification.notification_logs")
    parser.add_argument("--delivery-attempt-relation", default="udb_notification.notification_delivery_attempts")
    return parser


def assert_project(args: argparse.Namespace, intent: dict[str, str]) -> str:
    stored = intent["project_id"]
    expected = args.project_id.strip()
    if expected and expected != stored:
        raise SmokeError(
            f"queued intent project {stored!r} does not match --project-id {expected!r}"
        )
    return stored


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.selftest:
        return selftest()
    intent = load_intent(args)
    project_id = assert_project(args, intent)
    outcome = sidecar_send(args.sidecar_url, intent, args.provider_credential)
    report = build_report(intent, args.provider, outcome)
    if args.dry_run:
        print(
            json.dumps(
                {
                    "ok": True,
                    "dry_run": True,
                    "intent": {
                        k: intent[k]
                        for k in ["tenant_id", "project_id", "log_id", "channel"]
                    },
                    "report_delivery_request": report,
                    "grpcurl": grpcurl_command(args, intent["tenant_id"], project_id),
                },
                separators=(",", ":"),
            )
        )
        return 0
    response = call_report_delivery(args, report, intent["tenant_id"], project_id)
    print(
        json.dumps(
            {
                "ok": True,
                "tenant_id": intent["tenant_id"],
                "project_id": project_id,
                "log_id": intent["log_id"],
                "provider": args.provider,
                "report_delivery": response,
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
