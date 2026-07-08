#!/usr/bin/env python3
"""UDB notification provider sidecar.

The broker's leader-elected notification worker owns durable queueing, tenant
checks, vault decrypt, SSRF validation, and delivery-attempt persistence. This
sidecar is a narrow provider adapter: it receives the broker's generic POST body
(`to`, `subject`, `body`) plus a one-call bearer credential, then calls exactly
one configured provider.
"""

from __future__ import annotations

import base64
import datetime as dt
import email.message
import hashlib
import hmac
import http.client
import http.server
import json
import os
import secrets
import smtplib
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


MAX_BODY_BYTES = 256 * 1024
DEFAULT_PORT = 8080


class DeliveryError(Exception):
    def __init__(self, message: str, status: int = 502) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class DeliveryRequest:
    to: str
    subject: str
    body: str


@dataclass(frozen=True)
class DeliveryResult:
    provider_message_id: str
    status: str = "sent"


def env_bool(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def provider_name() -> str:
    return os.environ.get("UDB_NOTIFY_PROVIDER", "").strip().lower()


def load_json(raw: str, context: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise DeliveryError(f"{context} must be JSON: {exc}", 400) from exc
    if not isinstance(value, dict):
        raise DeliveryError(f"{context} must be a JSON object", 400)
    return value


def require(mapping: dict[str, Any], key: str) -> str:
    value = str(mapping.get(key, "")).strip()
    if not value:
        raise DeliveryError(f"credential field {key!r} is required", 400)
    return value


def optional_bool(mapping: dict[str, Any], key: str, default: bool) -> bool:
    if key not in mapping:
        return default
    value = mapping[key]
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def credential_from_authorization(headers: http.client.HTTPMessage) -> str:
    header = headers.get("authorization", "")
    prefix = "Bearer "
    if not header.startswith(prefix):
        raise DeliveryError("Authorization: Bearer <provider credential> is required", 401)
    credential = header[len(prefix) :].strip()
    if not credential:
        raise DeliveryError("provider credential is empty", 401)
    return credential


def parse_delivery_request(raw: bytes) -> DeliveryRequest:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DeliveryError(f"request body must be JSON: {exc}", 400) from exc
    if not isinstance(value, dict):
        raise DeliveryError("request body must be a JSON object", 400)
    to = str(value.get("to", "")).strip()
    subject = str(value.get("subject", "")).strip()
    body = str(value.get("body", "")).strip()
    if not to:
        raise DeliveryError("field 'to' is required", 400)
    if not body:
        raise DeliveryError("field 'body' is required", 400)
    return DeliveryRequest(to=to, subject=subject, body=body)


def dry_run(provider: str, request: DeliveryRequest) -> DeliveryResult:
    digest = hashlib.sha256(
        f"{provider}\0{request.to}\0{request.subject}\0{request.body}".encode("utf-8")
    ).hexdigest()[:32]
    return DeliveryResult(provider_message_id=f"dryrun-{digest}")


def send_smtp(credential: str, request: DeliveryRequest) -> DeliveryResult:
    cfg = load_json(credential, "SMTP credential")
    host = require(cfg, "host")
    from_addr = require(cfg, "from")
    port = int(cfg.get("port", 587))
    timeout = float(cfg.get("timeout_seconds", 10))
    username = str(cfg.get("username", "")).strip()
    password = str(cfg.get("password", "")).strip()
    use_starttls = optional_bool(cfg, "starttls", True)

    msg = email.message.EmailMessage()
    msg["From"] = from_addr
    msg["To"] = request.to
    msg["Subject"] = request.subject
    msg.set_content(request.body)

    with smtplib.SMTP(host, port, timeout=timeout) as smtp:
        smtp.ehlo()
        if use_starttls:
            smtp.starttls(context=ssl.create_default_context())
            smtp.ehlo()
        if username:
            smtp.login(username, password)
        refused = smtp.send_message(msg)
    if refused:
        raise DeliveryError(f"SMTP refused recipients: {sorted(refused)}")
    message_id = msg.get("Message-ID") or f"smtp-{secrets.token_hex(16)}"
    return DeliveryResult(provider_message_id=message_id)


def http_json(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout: float,
) -> tuple[int, dict[str, str], str]:
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers.items()), resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", errors="replace")
        raise DeliveryError(f"provider returned HTTP {exc.code}: {text}", 502) from exc
    except urllib.error.URLError as exc:
        raise DeliveryError(f"provider request failed: {exc.reason}", 502) from exc


def send_twilio(credential: str, request: DeliveryRequest) -> DeliveryResult:
    cfg = load_json(credential, "Twilio credential")
    account_sid = require(cfg, "account_sid")
    auth_token = require(cfg, "auth_token")
    from_number = require(cfg, "from")
    timeout = float(cfg.get("timeout_seconds", 10))
    endpoint = str(
        cfg.get(
            "endpoint",
            f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
        )
    )
    form = urllib.parse.urlencode(
        {"To": request.to, "From": from_number, "Body": request.body}
    ).encode("utf-8")
    basic = base64.b64encode(f"{account_sid}:{auth_token}".encode("utf-8")).decode("ascii")
    req = urllib.request.Request(
        endpoint,
        data=form,
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", errors="replace")
        raise DeliveryError(f"Twilio returned HTTP {exc.code}: {text}", 502) from exc
    data = json.loads(text or "{}")
    sid = str(data.get("sid", "")).strip()
    if not sid:
        raise DeliveryError("Twilio response did not include sid")
    return DeliveryResult(provider_message_id=sid)


def aws_sigv4_key(secret_key: str, date: str, region: str, service: str) -> bytes:
    k_date = hmac.new(f"AWS4{secret_key}".encode(), date.encode(), hashlib.sha256).digest()
    k_region = hmac.new(k_date, region.encode(), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode(), hashlib.sha256).digest()
    return hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()


def send_ses(credential: str, request: DeliveryRequest) -> DeliveryResult:
    cfg = load_json(credential, "SES credential")
    region = require(cfg, "region")
    access_key = require(cfg, "access_key_id")
    secret_key = require(cfg, "secret_access_key")
    from_addr = require(cfg, "from")
    timeout = float(cfg.get("timeout_seconds", 10))
    host = str(cfg.get("host", f"email.{region}.amazonaws.com")).strip()
    endpoint = str(cfg.get("endpoint", f"https://{host}/v2/email/outbound-emails")).strip()
    now = dt.datetime.now(dt.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date = now.strftime("%Y%m%d")
    payload = {
        "FromEmailAddress": from_addr,
        "Destination": {"ToAddresses": [request.to]},
        "Content": {
            "Simple": {
                "Subject": {"Data": request.subject or "(no subject)", "Charset": "UTF-8"},
                "Body": {"Text": {"Data": request.body, "Charset": "UTF-8"}},
            }
        },
    }
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    body_hash = hashlib.sha256(body).hexdigest()
    canonical_headers = (
        f"content-type:application/json\nhost:{host}\nx-amz-date:{amz_date}\n"
    )
    signed_headers = "content-type;host;x-amz-date"
    canonical_request = "\n".join(
        ["POST", "/v2/email/outbound-emails", "", canonical_headers, signed_headers, body_hash]
    )
    scope = f"{date}/{region}/ses/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )
    signature = hmac.new(
        aws_sigv4_key(secret_key, date, region, "ses"),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    auth = (
        "AWS4-HMAC-SHA256 "
        f"Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    )
    status, _headers, text = http_json(
        endpoint,
        payload,
        {
            "Authorization": auth,
            "Content-Type": "application/json",
            "Host": host,
            "X-Amz-Date": amz_date,
        },
        timeout,
    )
    if status not in {200, 201}:
        raise DeliveryError(f"SES returned HTTP {status}: {text}")
    data = json.loads(text or "{}")
    message_id = str(data.get("MessageId", "")).strip()
    if not message_id:
        raise DeliveryError("SES response did not include MessageId")
    return DeliveryResult(provider_message_id=message_id)


def send_fcm(credential: str, request: DeliveryRequest) -> DeliveryResult:
    cfg = load_json(credential, "FCM credential")
    project_id = require(cfg, "project_id")
    access_token = require(cfg, "access_token")
    timeout = float(cfg.get("timeout_seconds", 10))
    endpoint = str(
        cfg.get(
            "endpoint",
            f"https://fcm.googleapis.com/v1/projects/{project_id}/messages:send",
        )
    )
    payload = {
        "message": {
            "token": request.to,
            "notification": {
                "title": request.subject or "Notification",
                "body": request.body,
            },
        }
    }
    _status, _headers, text = http_json(
        endpoint,
        payload,
        {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        timeout,
    )
    data = json.loads(text or "{}")
    name = str(data.get("name", "")).strip()
    if not name:
        raise DeliveryError("FCM response did not include name")
    return DeliveryResult(provider_message_id=name)


def deliver(provider: str, credential: str, request: DeliveryRequest) -> DeliveryResult:
    if env_bool("UDB_NOTIFY_DRY_RUN"):
        return dry_run(provider, request)
    if provider == "smtp":
        return send_smtp(credential, request)
    if provider == "ses":
        return send_ses(credential, request)
    if provider == "twilio":
        return send_twilio(credential, request)
    if provider == "fcm":
        return send_fcm(credential, request)
    raise DeliveryError("UDB_NOTIFY_PROVIDER must be one of smtp, ses, twilio, fcm", 500)


class Handler(http.server.BaseHTTPRequestHandler):
    server_version = "udb-notify-sidecar/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write(
            "%s - - [%s] %s\n"
            % (self.client_address[0], self.log_date_time_string(), fmt % args)
        )

    def write_json(
        self,
        status: int,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/healthz":
            configured = provider_name() in {"smtp", "ses", "twilio", "fcm"}
            self.write_json(
                200 if configured else 503,
                {"ok": configured, "provider": provider_name() or None},
            )
            return
        self.write_json(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path not in {"/send", "/v1/send"}:
            self.write_json(404, {"error": "not found"})
            return
        try:
            length = int(self.headers.get("content-length", "0"))
        except ValueError:
            self.write_json(400, {"error": "invalid Content-Length"})
            return
        if length <= 0 or length > MAX_BODY_BYTES:
            self.write_json(413, {"error": "request body size is invalid"})
            return
        try:
            credential = credential_from_authorization(self.headers)
            request = parse_delivery_request(self.rfile.read(length))
            provider = provider_name()
            result = deliver(provider, credential, request)
            self.write_json(
                200,
                {
                    "status": result.status,
                    "provider": provider,
                    "provider_message_id": result.provider_message_id,
                },
                {"x-provider-message-id": result.provider_message_id},
            )
        except DeliveryError as exc:
            self.write_json(exc.status, {"error": str(exc)})
        except Exception as exc:  # Provider libraries often raise broad exceptions.
            self.write_json(502, {"error": f"delivery failed: {exc}"})


def main() -> None:
    port = int(os.environ.get("PORT", str(DEFAULT_PORT)))
    bind = os.environ.get("HOST", "0.0.0.0")
    httpd = http.server.ThreadingHTTPServer((bind, port), Handler)
    print(
        json.dumps(
            {
                "event": "udb_notify_sidecar_started",
                "provider": provider_name() or None,
                "port": port,
                "dry_run": env_bool("UDB_NOTIFY_DRY_RUN"),
            },
            separators=(",", ":"),
        ),
        flush=True,
    )
    httpd.serve_forever()


if __name__ == "__main__":
    main()
