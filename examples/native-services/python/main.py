"""Python tour of UDB's native control plane.

Logs in as the bootstrapped admin on the real enterprise path, then makes at
least one real call to each of the 28 native services, printing one numbered
line per service. Same steps, same requests and same output lines as
../go/main.go (the reference) and ../typescript/src/main.ts.

Run (after examples/ts_enterprise/scripts/bootstrap.sh + serve.sh):

    set -a; source ../../ts_enterprise/secrets/enterprise.env; set +a
    pip install -r requirements.txt
    python main.py
"""

from __future__ import annotations

import base64
import json
import os
import re
import secrets
import sys
import threading
import uuid
from typing import Any, Callable

import grpc
from google.protobuf import descriptor_pool, json_format, message_factory

from udb_client import create_udb
from udb_client.generated_client import RPC_API_ALIAS

TOTAL_SERVICES = 28


def env(key: str, default: str) -> str:
    return os.environ.get(key) or default


def pkg(svc: str) -> str:
    return f"udb.core.{svc}.services.v1"


S = {
    "authn": f"{pkg('authn')}.AuthnService",
    "tenant": f"{pkg('tenant')}.TenantService",
    "authz": f"{pkg('authz')}.AuthzService",
    "apikey": f"{pkg('apikey')}.ApiKeyService",
    "idp": f"{pkg('idp')}.IdentityProviderService",
    "config": f"{pkg('config')}.ConfigService",
    "lock": f"{pkg('lock')}.LockService",
    "livequery": f"{pkg('livequery')}.LiveQueryService",
    "cache": f"{pkg('cache')}.CacheService",
    "scheduler": f"{pkg('scheduler')}.SchedulerService",
    "workflow": f"{pkg('workflow')}.WorkflowService",
    "webhook": f"{pkg('webhook')}.WebhookService",
    "vault": f"{pkg('vault')}.VaultService",
    "notification": f"{pkg('notification')}.NotificationService",
    "metering": f"{pkg('metering')}.MeteringService",
    "analytics": f"{pkg('analytics')}.AnalyticsService",
    "backup": f"{pkg('backup')}.BackupService",
    "control": f"{pkg('control')}.ControlPlaneService",
    "storage": f"{pkg('storage')}.StorageService",
    "asset": f"{pkg('asset')}.AssetService",
    "search": f"{pkg('search')}.SearchService",
    "embedding": f"{pkg('embedding')}.EmbeddingService",
    "messaging": f"{pkg('messaging')}.MessagingService",
    "room": f"{pkg('webrtc')}.RoomService",
    "peer": f"{pkg('webrtc')}.PeerService",
    "track": f"{pkg('webrtc')}.TrackService",
    "turn": f"{pkg('webrtc')}.TurnService",
    "signaling": f"{pkg('webrtc')}.SignalingService",
}


def b(value: Any) -> str:
    """Render a bool the way the Go and TypeScript tours do."""
    return "true" if value else "false"


# ── errors ──────────────────────────────────────────────────────────────────


class BodyError(Exception):
    """A refusal the service returned inside an OK response (ApiError)."""

    def __init__(self, rpc: str, code: str, message: str):
        self.rpc, self.code, self.detail_message = rpc, code, message
        super().__init__(f"{rpc} returned {code}: {message}")


class DepError(Exception):
    """A step that could not run because a step it depends on was skipped or failed."""

    def __init__(self, message: str, skipped: bool):
        self.skipped = skipped
        super().__init__(message)


def api_err(rpc: str, e: Any) -> None:
    if e and e.get("code"):
        raise BodyError(rpc, e["code"], e.get("message", ""))


# Domain reasons a service uses when its backend or secret is not set up on this
# broker (UPLOAD_URL_UNAVAILABLE, TURN_NOT_CONFIGURED, ...).
NOT_CONFIGURED = re.compile(r"(NOT_CONFIGURED|NOT_AVAILABLE|UNAVAILABLE)$")
ERROR_KIND_CAPABILITY = 1


def inspect(err: BaseException) -> dict[str, Any]:
    """Code, message and typed reasons of a broker refusal (rpc=False otherwise)."""
    raw = getattr(err, "raw", None) or (err if isinstance(err, grpc.RpcError) else None)
    if raw is None or not callable(getattr(raw, "code", None)):
        return {"rpc": False, "message": str(err)}
    domain = ""
    try:
        for key, value in raw.trailing_metadata() or ():
            if key == "error-reason":
                domain = value if isinstance(value, str) else value.decode()
    except Exception:  # noqa: BLE001 - trailers are best-effort context
        domain = ""
    detail = getattr(err, "error_detail", None)
    return {
        "rpc": True,
        "code": raw.code(),
        "message": raw.details() or "",
        "reason": getattr(detail, "reason", "") or "",
        "domain": domain,
        "capability": getattr(detail, "capability_required", "") or "",
        "kind": int(getattr(detail, "kind", 0) or 0),
    }


def skip_reason(err: BaseException) -> str | None:
    """The reason line when err is the broker saying "this service's backend is not
    configured here"; None for any other failure."""
    if isinstance(err, BodyError):
        if NOT_CONFIGURED.search(err.code):
            return f"{err.code}: {err.detail_message}"
        return None
    info = inspect(err)
    if not info["rpc"] or info["code"] != grpc.StatusCode.FAILED_PRECONDITION:
        return None
    capability = (
        info["kind"] == ERROR_KIND_CAPABILITY or info["reason"] == "UDB_CAPABILITY_UNSUPPORTED"
    )
    if not capability and not NOT_CONFIGURED.search(info["domain"]):
        return None
    reason = info["domain"] or info["reason"] or "UDB_CAPABILITY_UNSUPPORTED"
    if info["capability"]:
        reason += f" ({info['capability']})"
    return f"{reason}: {info['message']}"


def describe(err: BaseException) -> str:
    if isinstance(err, BodyError):
        return str(err)
    info = inspect(err)
    if not info["rpc"]:
        return info["message"]
    code = info["code"].name
    reason = info["domain"] or info["reason"]
    return f"{code} {reason}: {info['message']}" if reason else f"{code}: {info['message']}"


def snake(method: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", method).lower()


# ── tour ────────────────────────────────────────────────────────────────────


class Tour:
    def __init__(self, udb: Any, tenant: str, project: str, actor: str):
        self.udb = udb
        self.gen = udb.generated  # one generated client per service, on the control-plane channel
        self.tenant, self.project, self.actor = tenant, project, actor
        self.suffix = secrets.token_hex(4)
        self.n = self.called = self.skipped = 0
        self.failures: list[str] = []
        self.status: dict[str, str] = {}
        self.room_id = ""
        self.peer_id = ""

    def _client_and_request(self, service: str, method: str, body: dict[str, Any]) -> tuple[Any, Any]:
        client = getattr(self.gen, service.rsplit(".", 1)[1])
        full = f"{service.rsplit('.', 1)[0]}.{method}Request"
        cls = message_factory.GetMessageClass(descriptor_pool.Default().FindMessageTypeByName(full))
        return client, json_format.ParseDict(body, cls())

    @staticmethod
    def _to_dict(msg: Any) -> dict[str, Any]:
        return json_format.MessageToDict(
            msg, preserving_proto_field_name=True, always_print_fields_with_no_presence=True
        )

    def call(self, service: str, method: str, body: dict[str, Any]) -> dict[str, Any]:
        """Unary call by service + method name, with a JSON-shaped request/response
        (the same snake_case bodies the TypeScript tour sends)."""
        client, request = self._client_and_request(service, method, body)
        alias = RPC_API_ALIAS.get(f"/{service}/{method}") or snake(method)
        return self._to_dict(getattr(client, alias)(request, timeout=15))

    def step(self, service: str, optional: bool, fn: Callable[[], str]) -> None:
        self.n += 1
        prefix = f"{self.n:02d} {service}:"
        try:
            msg = fn()
        except DepError as err:
            if err.skipped:
                self.skipped += 1
                self.status[service] = "skipped"
                print(prefix, "skipped —", err)
                return
            self._fail(prefix, service, err)
            return
        except Exception as err:  # noqa: BLE001 - every failure is reported below
            reason = skip_reason(err) if optional else None
            if reason is not None:
                self.skipped += 1
                self.status[service] = "skipped"
                print(prefix, "skipped —", reason)
                return
            self._fail(prefix, service, err)
            return
        self.called += 1
        self.status[service] = "ok"
        print(prefix, msg)

    def _fail(self, prefix: str, service: str, err: BaseException) -> None:
        self.status[service] = "failed"
        line = describe(err)
        self.failures.append(f"{self.n:02d} {service}: {line}")
        print(prefix, "FAILED —", line)

    def needs(self, service: str) -> None:
        st = self.status.get(service)
        if st == "ok":
            return
        if st == "skipped":
            raise DepError(f"needs {service}, which was skipped above", True)
        raise DepError(f"needs {service}, which failed above", False)

    # ── steps (one per native service) ───────────────────────────────────────

    def authn(self) -> str:
        got = self.call(S["authn"], "GetUser", {"user_id": self.actor})
        user = got.get("user", {})
        return f"GetUser -> {json.dumps(user.get('username', ''))} ({user.get('status')})"

    def tenant_svc(self) -> str:
        self.call(S["tenant"], "UpdateTenantConfig", {
            "tenant_id": self.tenant, "config_key": "native_tour_max_users",
            "config_value": "200", "type": "NUMBER",
        })
        cfg = self.call(S["tenant"], "GetTenantConfig", {"tenant_id": self.tenant})
        api_err("GetTenantConfig", cfg.get("error"))
        for kv in cfg.get("configs", []):
            if kv.get("config_key") == "native_tour_max_users":
                return f"UpdateTenantConfig + GetTenantConfig -> native_tour_max_users={kv.get('config_value')}"
        raise RuntimeError("GetTenantConfig did not return native_tour_max_users")

    def authz(self) -> str:
        code = f"native_tour_{self.suffix}"
        created = self.call(S["authz"], "CreateRole", {
            "name": f"Native tour {self.suffix}", "description": "native-services tour role",
            "created_by": self.actor, "role_code": code, "domain": self.tenant,
            "tenant_id": self.tenant, "project_id": self.project,
        })
        role_id = created.get("role", {}).get("role_id", "")
        failure: BaseException | None = None
        check: dict[str, Any] = {}
        try:
            got = self.call(S["authz"], "GetRole", {"role_id": role_id})
            if got.get("role", {}).get("role_code") != code:
                raise RuntimeError(
                    f"GetRole returned role_code {json.dumps(got.get('role', {}).get('role_code'))}, want {json.dumps(code)}"
                )
            check = self.call(S["authz"], "CheckAccess", {
                "user_id": self.actor, "domain": self.tenant, "tenant_id": self.tenant,
                "project_id": self.project, "object": "billing.v1.Invoice", "action": "Select",
            })
        except Exception as err:  # noqa: BLE001
            failure = err
        try:
            self.call(S["authz"], "DeleteRole", {"role_id": role_id, "deleted_by": self.actor})
        except Exception as err:  # noqa: BLE001
            failure = failure or err
        if failure:
            raise failure
        return (
            "CreateRole + GetRole + DeleteRole ok; CheckAccess(admin, Select billing.v1.Invoice) "
            f"-> allowed={b(check.get('allowed'))}"
        )

    def api_key(self) -> str:
        tenant_ctx = {"tenant": {"tenant_id": self.tenant, "project_id": self.project}}
        # An API key belongs to a service account, and a service account needs an
        # active grant (its approved scopes) before it can hold a key.
        name = f"native-tour-svc-{self.suffix}"
        sa = self.call(S["authn"], "CreateUser", {
            "username": name, "email": f"{name}@example.com", "password": "CorrectHorse1!",
            "tenant_id": self.tenant, "project_id": self.project,
            "full_name": "Native tour service account", "account_kind": "ACCOUNT_KIND_SERVICE_ACCOUNT",
        })
        owner = sa.get("user", {}).get("user_id", "")
        failure: BaseException | None = None
        valid: dict[str, Any] = {}
        try:
            self.call(S["authn"], "ChangeUserStatus", {
                "user_id": owner, "new_status": "USER_STATUS_ACTIVE",
                "reason": "native tour service account", "context": tenant_ctx,
            })
            self.call(S["authn"], "CreateServiceAccountGrant", {
                "tenant_id": self.tenant, "project_id": self.project, "user_id": owner,
                "service_identity": name, "approved_scopes": ["data:read"], "reason": "native tour API key",
            })
            key_ctx = {"user_id": owner, "tenant": tenant_ctx["tenant"]}
            created = self.call(S["apikey"], "CreateApiKey", {
                "name": f"native-tour-key-{self.suffix}", "owner_id": owner,
                "scopes": ["data:read"], "context": key_ctx,
            })
            try:
                valid = self.call(S["apikey"], "ValidateApiKey", {
                    "plain_key": created.get("plain_key", ""), "required_scope": "data:read",
                })
                if not valid.get("valid") or valid.get("owner_id") != owner:
                    raise RuntimeError(
                        f"ValidateApiKey valid={b(valid.get('valid'))} owner={json.dumps(valid.get('owner_id'))}, "
                        f"want true/{json.dumps(owner)}"
                    )
            except Exception as err:  # noqa: BLE001
                failure = err
            try:
                self.call(S["apikey"], "RevokeApiKey", {
                    "key_id": created.get("key", {}).get("key_id", ""),
                    "revoke_reason": "native tour cleanup", "context": key_ctx,
                })
            except Exception as err:  # noqa: BLE001
                failure = failure or err
        except Exception as err:  # noqa: BLE001
            failure = failure or err
        try:
            self.call(S["authn"], "RevokeServiceAccountGrant", {
                "tenant_id": self.tenant, "user_id": owner, "reason": "native tour cleanup",
            })
        except Exception as err:  # noqa: BLE001
            failure = failure or err
        try:
            self.call(S["authn"], "ChangeUserStatus", {
                "user_id": owner, "new_status": "USER_STATUS_DEACTIVATED",
                "reason": "native tour cleanup", "context": tenant_ctx,
            })
        except Exception as err:  # noqa: BLE001
            failure = failure or err
        if failure:
            raise failure
        return (
            "CreateApiKey(service account) + ValidateApiKey(data:read) "
            f"-> valid={b(valid.get('valid'))}; RevokeApiKey ok"
        )

    def idp(self) -> str:
        out = self.call(S["idp"], "ListProviders", {
            "tenant_id": self.tenant, "kind": "IDP_KIND_UNSPECIFIED", "page": {"page": 1, "page_size": 20},
        })
        return f"ListProviders -> {len(out.get('providers', []))} provider(s)"

    def config(self) -> str:
        key = f"native_tour.{self.suffix}"
        where = {"tenant_id": self.tenant, "project_id": self.project, "environment": "prod", "flag_key": key}
        put = self.call(S["config"], "PutFlag", {
            **where, "value": {"bool_value": True}, "enabled": True, "rollout_percentage": 100,
            "rollout_context_key": "user_id", "metadata_json": "{}",
        })
        api_err("PutFlag", put.get("error"))
        failure: BaseException | None = None
        got: dict[str, Any] = {}
        try:
            got = self.call(S["config"], "GetFlag", where)
            api_err("GetFlag", got.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = err
        try:
            api_err("DeleteFlag", self.call(S["config"], "DeleteFlag", where).get("error"))
        except Exception as err:  # noqa: BLE001
            failure = failure or err
        if failure:
            raise failure
        value = got.get("flag", {}).get("value", {})
        if not got.get("found") or value.get("bool_value") is not True:
            raise RuntimeError(
                f"GetFlag found={b(got.get('found'))} value={json.dumps(value)}, want the flag just written"
            )
        return "PutFlag + GetFlag -> bool_value=true; DeleteFlag ok"

    def lock(self) -> str:
        name = f"native-tour-lock-{self.suffix}"
        acq = self.call(S["lock"], "AcquireLock", {
            "tenant_id": self.tenant, "lock_name": name, "owner_id": self.actor,
            "lease_ttl_seconds": 60, "metadata_json": "{}",
        })
        api_err("AcquireLock", acq.get("error"))
        if not acq.get("acquired"):
            raise RuntimeError(f"AcquireLock acquired=false: {acq.get('message', '')}")
        failure: BaseException | None = None
        got: dict[str, Any] = {}
        rel: dict[str, Any] = {}
        try:
            got = self.call(S["lock"], "GetLock", {"tenant_id": self.tenant, "lock_name": name})
            api_err("GetLock", got.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = err
        try:
            rel = self.call(S["lock"], "ReleaseLock", {
                "tenant_id": self.tenant, "lock_name": name, "owner_id": self.actor,
                "fencing_token": acq.get("fencing_token", "0"),
            })
            api_err("ReleaseLock", rel.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = failure or err
        if failure:
            raise failure
        if not got.get("found"):
            raise RuntimeError("GetLock did not find the lock just acquired")
        return (
            f"AcquireLock -> fencing token issued; GetLock -> found={b(got.get('found'))}; "
            f"ReleaseLock -> released={b(rel.get('released'))}"
        )

    def live_query(self) -> str:
        # Hold a lock so the subscription's snapshot has a row to show.
        name = f"native-tour-live-{self.suffix}"
        acq = self.call(S["lock"], "AcquireLock", {
            "tenant_id": self.tenant, "lock_name": name, "owner_id": self.actor,
            "lease_ttl_seconds": 60, "metadata_json": "{}",
        })
        api_err("AcquireLock", acq.get("error"))
        failure: BaseException | None = None
        rows = -1
        try:
            client, request = self._client_and_request(S["livequery"], "Subscribe", {
                "tenant_id": self.tenant, "message_type": "udb.core.lock.entity.v1.Lock",
                "snapshot_limit": 10,
                "filters": [{"field": "lock_name", "op": "LIVE_QUERY_COMPARISON_EQ", "value": name}],
            })
            # The stream is bounded by the 15 s timeout; we stop reading at the snapshot.
            for msg in client.subscribe(request, timeout=15):
                frame = self._to_dict(msg)
                api_err("Subscribe", frame.get("error"))
                if "snapshot" in frame:
                    rows = int(frame["snapshot"].get("row_count", 0))
                    break
            if rows < 0:
                raise RuntimeError("Subscribe ended before sending a snapshot")
        except Exception as err:  # noqa: BLE001
            failure = err
        try:
            rel = self.call(S["lock"], "ReleaseLock", {
                "tenant_id": self.tenant, "lock_name": name, "owner_id": self.actor,
                "fencing_token": acq.get("fencing_token", "0"),
            })
            api_err("ReleaseLock", rel.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = failure or err
        if failure:
            raise failure
        return f"Subscribe(udb.core.lock.entity.v1.Lock, lock_name=...) -> snapshot with {rows} row(s)"

    def cache(self) -> str:
        ns = f"native-tour-{self.suffix}"
        created = self.call(S["cache"], "CreateNamespace", {
            "tenant_id": self.tenant, "namespace": ns, "max_bytes": 1 << 20, "default_ttl_seconds": 300,
        })
        api_err("CreateNamespace", created.get("error"))
        failure: BaseException | None = None
        got: dict[str, Any] = {}
        try:
            stored = self.call(S["cache"], "Set", {
                "tenant_id": self.tenant, "namespace": ns, "key": "greeting",
                "value": base64.b64encode(b"hello").decode(), "ttl_seconds": 300,
            })
            api_err("Set", stored.get("error"))
            got = self.call(S["cache"], "Get", {"tenant_id": self.tenant, "namespace": ns, "key": "greeting"})
            api_err("Get", got.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = err
        try:
            deleted = self.call(S["cache"], "DeleteNamespace", {
                "tenant_id": self.tenant, "namespace": ns, "confirmation_token": ns,
            })
            api_err("DeleteNamespace", deleted.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = failure or err
        if failure:
            raise failure
        value = base64.b64decode(got.get("value", "")).decode()
        if not got.get("found") or value != "hello":
            raise RuntimeError(f'Get found={b(got.get("found"))} value={json.dumps(value)}, want "hello"')
        return 'CreateNamespace + Set + Get -> "hello"; DeleteNamespace ok'

    def scheduler(self) -> str:
        created = self.call(S["scheduler"], "CreateJob", {
            "tenant_id": self.tenant, "project_id": self.project, "name": f"native-tour-{self.suffix}",
            "schedule_type": "CRON", "cron_expression": "*/5 * * * *", "payload": "{}",
            "target_topic": "native_tour.scheduler", "max_attempts": 3, "backoff_seconds": 30,
        })
        api_err("CreateJob", created.get("error"))
        failure: BaseException | None = None
        got: dict[str, Any] = {}
        try:
            got = self.call(S["scheduler"], "GetJob", {"tenant_id": self.tenant, "job_id": created.get("job_id", "")})
            api_err("GetJob", got.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = err
        try:
            deleted = self.call(S["scheduler"], "DeleteJob", {"tenant_id": self.tenant, "job_id": created.get("job_id", "")})
            api_err("DeleteJob", deleted.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = failure or err
        if failure:
            raise failure
        return f"CreateJob(*/5 * * * *) + GetJob -> {got.get('job', {}).get('status')}; DeleteJob ok"

    def workflow(self) -> str:
        started = self.call(S["workflow"], "StartWorkflow", {
            "tenant_id": self.tenant, "project_id": self.project, "workflow_type": "native_tour.demo",
            "total_steps": 1, "payload": "{}", "compensations": "[]",
            "correlation_id": f"native-tour-{self.suffix}",
        })
        api_err("StartWorkflow", started.get("error"))
        wf_id = started.get("workflow_id", "")
        failure: BaseException | None = None
        got: dict[str, Any] = {}
        try:
            got = self.call(S["workflow"], "GetWorkflow", {"tenant_id": self.tenant, "workflow_id": wf_id})
            api_err("GetWorkflow", got.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = err
        try:
            cancelled = self.call(S["workflow"], "CancelWorkflow", {
                "tenant_id": self.tenant, "workflow_id": wf_id, "reason": "native tour cleanup",
            })
            api_err("CancelWorkflow", cancelled.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = failure or err
        if failure:
            raise failure
        return f"StartWorkflow + GetWorkflow -> {got.get('workflow', {}).get('status')}; CancelWorkflow ok"

    def webhook(self) -> str:
        created = self.call(S["webhook"], "CreateEndpoint", {
            "tenant_id": self.tenant, "url": "https://example.com/udb-native-tour",
            "topic_pattern": "native_tour.*", "description": "native-services tour",
            "max_attempts": 3, "metadata_json": "{}",
        })
        api_err("CreateEndpoint", created.get("error"))
        endpoint_id = created.get("endpoint_id", "")
        failure: BaseException | None = None
        got: dict[str, Any] = {}
        try:
            got = self.call(S["webhook"], "GetEndpoint", {"tenant_id": self.tenant, "endpoint_id": endpoint_id})
            api_err("GetEndpoint", got.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = err
        try:
            deleted = self.call(S["webhook"], "DeleteEndpoint", {"tenant_id": self.tenant, "endpoint_id": endpoint_id})
            api_err("DeleteEndpoint", deleted.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = failure or err
        if failure:
            raise failure
        return f"CreateEndpoint + GetEndpoint -> {got.get('endpoint', {}).get('url')}; DeleteEndpoint ok"

    def vault(self) -> str:
        path = f"native-tour/{self.suffix}/db-password"
        put = self.call(S["vault"], "PutSecret", {
            "tenant_id": self.tenant, "secret_path": path, "secret_value": "s3cr3t",
            "expected_version": 0, "metadata_json": "{}",
        })
        api_err("PutSecret", put.get("error"))
        failure: BaseException | None = None
        got: dict[str, Any] = {}
        try:
            got = self.call(S["vault"], "GetSecret", {"tenant_id": self.tenant, "secret_path": path})
            api_err("GetSecret", got.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = err
        try:
            deleted = self.call(S["vault"], "DeleteSecret", {"tenant_id": self.tenant, "secret_path": path})
            api_err("DeleteSecret", deleted.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = failure or err
        if failure:
            raise failure
        if got.get("secret_value") != "s3cr3t":
            raise RuntimeError("GetSecret did not return the value just written")
        return f"PutSecret + GetSecret -> version {got.get('version')} round-trips; DeleteSecret ok"

    def notification(self) -> str:
        event = f"native_tour.{self.suffix}"
        self.call(S["notification"], "UpsertTemplate", {
            "event_type": event, "channel": "NOTIFICATION_CHANNEL_EMAIL", "locale": "en",
            "subject_template": "Native tour", "body_template": "Hello {{name}}", "is_active": True,
        })
        sent = self.call(S["notification"], "SendNotification", {
            "event_type": event, "recipient_id": self.actor, "recipient_address": "native-tour@example.com",
            "tenant_id": self.tenant, "project_id": self.project, "locale": "en",
            "variables": {"name": "UDB"}, "channels": ["NOTIFICATION_CHANNEL_EMAIL"],
        })
        logs = sent.get("logs", [])
        if not logs:
            raise RuntimeError("SendNotification returned no delivery log")
        got = self.call(S["notification"], "GetNotification", {"log_id": logs[0].get("log_id", "")})
        return f"UpsertTemplate + SendNotification(EMAIL) + GetNotification -> {got.get('log', {}).get('status')}"

    def metering(self) -> str:
        metric = f"native_tour.{self.suffix}"
        rec = self.call(S["metering"], "RecordUsage", {
            "tenant_id": self.tenant, "principal_id": self.actor, "method": metric,
            "unit": "request", "quantity": 3, "metadata_json": "{}",
        })
        api_err("RecordUsage", rec.get("error"))
        used = self.call(S["metering"], "QueryUsage", {
            "tenant_id": self.tenant, "metric": metric, "window_seconds": 3600,
        })
        api_err("QueryUsage", used.get("error"))
        if int(used.get("used", 0)) != 3:
            raise RuntimeError(f"QueryUsage used={used.get('used')}, want 3")
        return f"RecordUsage(3) + QueryUsage(1h) -> used={used.get('used')}"

    def analytics(self) -> str:
        acc = self.call(S["analytics"], "RecordPipelineMetric", {
            "stage_name": f"native_tour_{self.suffix}", "tenant_id": self.tenant,
            "latency_ms": 12.5, "is_success": True,
        })
        if not acc.get("accepted"):
            raise RuntimeError("RecordPipelineMetric accepted=false")
        through = self.call(S["analytics"], "GetThroughput", {"tenant_id": self.tenant})
        if int(through.get("total_requests", 0)) < 1:
            raise RuntimeError(f"GetThroughput total_requests={through.get('total_requests')}, want >= 1")
        return "RecordPipelineMetric + GetThroughput -> total_requests >= 1"

    def backup(self) -> str:
        name = f"native-tour-{self.suffix}"
        put = self.call(S["backup"], "PutBackupPolicy", {
            "tenant_id": self.tenant, "policy_name": name, "schedule_cron": "0 3 * * *",
            "retention_days": 7, "max_retained_backups": 3, "enabled": True, "metadata_json": "{}",
        })
        api_err("PutBackupPolicy", put.get("error"))
        failure: BaseException | None = None
        got: dict[str, Any] = {}
        try:
            got = self.call(S["backup"], "GetBackupPolicy", {"tenant_id": self.tenant, "policy_name": name})
            api_err("GetBackupPolicy", got.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = err
        try:
            deleted = self.call(S["backup"], "DeleteBackupPolicy", {"tenant_id": self.tenant, "policy_name": name})
            api_err("DeleteBackupPolicy", deleted.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = failure or err
        if failure:
            raise failure
        policy = got.get("policy", {})
        return (
            f"PutBackupPolicy + GetBackupPolicy -> {json.dumps(policy.get('schedule_cron', ''))} "
            f"retention {policy.get('retention_days')} days; DeleteBackupPolicy ok"
        )

    def control_plane(self) -> str:
        out = self.call(S["control"], "ListNodeStates", {
            "resource_type": "RESOURCE_TYPE_UNSPECIFIED", "page": {"page": 1, "page_size": 50},
            "context": {"tenant": {"tenant_id": self.tenant}},
        })
        return f"ListNodeStates -> {len(out.get('node_states', []))} node state(s)"

    def storage(self) -> str:
        reg = self.call(S["storage"], "RegisterUpload", {
            "tenant_id": self.tenant, "filename": f"native-tour-{self.suffix}.txt",
            "content_type": "text/plain", "file_type": "DOCUMENT", "reference_id": str(uuid.uuid4()),
            "reference_type": "native_tour", "size_bytes": 5, "expires_in_minutes": 10,
        })
        file_id = reg.get("file_id", "")
        # Without an object store the file row is still registered, but no upload URL
        # can be minted: the response carries UPLOAD_URL_UNAVAILABLE. Remove the row
        # either way, then report.
        failure: BaseException | None = None
        got: dict[str, Any] = {}
        try:
            api_err("RegisterUpload", reg.get("error"))
            if file_id:
                got = self.call(S["storage"], "GetFile", {"tenant_id": self.tenant, "file_id": file_id})
                api_err("GetFile", got.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = err
        if file_id:
            try:
                deleted = self.call(S["storage"], "DeleteFile", {
                    "tenant_id": self.tenant, "file_id": file_id, "reason": "native tour cleanup",
                })
                api_err("DeleteFile", deleted.get("error"))
            except Exception as err:  # noqa: BLE001
                failure = failure or err
        if failure:
            raise failure
        return (
            "RegisterUpload -> presigned upload URL issued; "
            f"GetFile -> {got.get('file', {}).get('status')}; DeleteFile ok"
        )

    def asset(self) -> str:
        definition = self.call(S["asset"], "CreatePipelineDefinition", {
            "tenant_id": self.tenant, "name": f"native-tour-{self.suffix}",
            "description": "native-services tour pipeline", "media_type": "application/json",
            "steps": '[{"name":"extract","type":"EXTRACT"}]', "version": 1,
        })
        api_err("CreatePipelineDefinition", definition.get("error"))
        got = self.call(S["asset"], "GetPipelineDefinition", {
            "tenant_id": self.tenant, "definition_id": definition.get("definition_id", ""),
        })
        api_err("GetPipelineDefinition", got.get("error"))
        assets = self.call(S["asset"], "ListAssets", {"tenant_id": self.tenant, "page": 1, "page_size": 20})
        api_err("ListAssets", assets.get("error"))
        return (
            "CreatePipelineDefinition + GetPipelineDefinition -> "
            f"version {got.get('definition', {}).get('version')}; ListAssets -> {len(assets.get('assets', []))} asset(s)"
        )

    def search(self) -> str:
        out = self.call(S["search"], "ListIndexes", {"tenant_id": self.tenant, "page_size": 50})
        api_err("ListIndexes", out.get("error"))
        return f"ListIndexes -> {len(out.get('indexes', []))} index(es)"

    def embedding(self) -> str:
        out = self.call(S["embedding"], "ListModels", {"tenant_id": self.tenant, "page_size": 50})
        api_err("ListModels", out.get("error"))
        return f"ListModels -> {len(out.get('models', []))} model(s)"

    def messaging(self) -> str:
        out = self.call(S["messaging"], "ListMessagingDevices", {
            "tenant_id": self.tenant, "subject_id": self.actor, "page_size": 20,
        })
        api_err("ListMessagingDevices", out.get("error"))
        return f"ListMessagingDevices(admin) -> {len(out.get('devices', []))} device(s)"

    # The five WebRTC services share one room: RoomService creates it, PeerService
    # joins it, TrackService publishes into it, TurnService issues TURN credentials
    # for the peer, and SignalingService pings over the signalling stream and then
    # closes the room.

    def room(self) -> str:
        created = self.call(S["room"], "CreateRoom", {
            "tenant_id": self.tenant, "name": f"native-tour-{self.suffix}", "max_participants": 8,
            "config": "{}", "created_by": self.actor,
        })
        api_err("CreateRoom", created.get("error"))
        room_id = created.get("room_id", "")
        try:
            got = self.call(S["room"], "GetRoom", {"tenant_id": self.tenant, "room_id": room_id})
            api_err("GetRoom", got.get("error"))
        except Exception:
            try:
                self.call(S["room"], "CloseRoom", {"tenant_id": self.tenant, "room_id": room_id})
            except Exception:  # noqa: BLE001 - the GetRoom failure is what gets reported
                pass
            raise
        self.room_id = room_id
        return f"CreateRoom + GetRoom -> {got.get('room', {}).get('state')}"

    def peer(self) -> str:
        self.needs("RoomService")
        joined = self.call(S["peer"], "JoinRoom", {
            "tenant_id": self.tenant, "room_id": self.room_id, "display_name": "tour-peer",
            "metadata": "{}", "user_agent": "native-tour",
        })
        api_err("JoinRoom", joined.get("error"))
        self.peer_id = joined.get("peer", {}).get("peer_id", "")
        out = self.call(S["peer"], "ListPeers", {"tenant_id": self.tenant, "room_id": self.room_id, "page_size": 20})
        api_err("ListPeers", out.get("error"))
        return f"JoinRoom + ListPeers -> {len(out.get('peers', []))} peer(s) in the room"

    def track(self) -> str:
        self.needs("PeerService")
        pub = self.call(S["track"], "PublishTrack", {
            "tenant_id": self.tenant, "room_id": self.room_id, "peer_id": self.peer_id,
            "kind": "audio", "label": "mic", "settings": "{}", "metadata": "{}",
        })
        api_err("PublishTrack", pub.get("error"))
        failure: BaseException | None = None
        out: dict[str, Any] = {}
        try:
            out = self.call(S["track"], "ListTracks", {"tenant_id": self.tenant, "room_id": self.room_id, "page_size": 20})
            api_err("ListTracks", out.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = err
        try:
            unpub = self.call(S["track"], "UnpublishTrack", {"tenant_id": self.tenant, "track_id": pub.get("track_id", "")})
            api_err("UnpublishTrack", unpub.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = failure or err
        if failure:
            raise failure
        return f"PublishTrack(audio) + ListTracks -> {len(out.get('tracks', []))} track(s); UnpublishTrack ok"

    def turn(self) -> str:
        self.needs("PeerService")
        creds = self.call(S["turn"], "IssueCredentials", {
            "tenant_id": self.tenant, "room_id": self.room_id, "peer_id": self.peer_id, "ttl_seconds": 3600,
        })
        api_err("IssueCredentials", creds.get("error"))
        return f"IssueCredentials -> {len(creds.get('ice_servers', []))} ICE server(s), ttl {creds.get('ttl_seconds')}s"

    def signaling(self) -> str:
        self.needs("RoomService")
        failure: BaseException | None = None
        try:
            # Closing a signalling stream disconnects its peer, so ping with a
            # second, disposable peer.
            joined = self.call(S["peer"], "JoinRoom", {
                "tenant_id": self.tenant, "room_id": self.room_id, "display_name": "tour-signal",
                "metadata": "{}", "user_agent": "native-tour",
            })
            api_err("JoinRoom", joined.get("error"))
            client, ping = self._client_and_request(S["signaling"], "Signal", {
                "tenant_id": self.tenant, "room_id": self.room_id,
                "peer_id": joined.get("peer", {}).get("peer_id", ""), "ping": True,
            })
            got_pong = threading.Event()

            def requests():
                yield ping
                got_pong.wait(15)  # keep the stream open until the pong arrives

            pong = False
            try:
                for msg in client.signal(requests(), timeout=15):
                    if self._to_dict(msg).get("pong"):
                        pong = True
                        break
            finally:
                got_pong.set()
            if not pong:
                raise RuntimeError("Signal stream ended before a pong")
        except Exception as err:  # noqa: BLE001
            failure = err
        # Leave + close the shared room (this step runs last).
        if self.peer_id:
            try:
                left = self.call(S["peer"], "LeaveRoom", {
                    "tenant_id": self.tenant, "room_id": self.room_id, "peer_id": self.peer_id,
                })
                api_err("LeaveRoom", left.get("error"))
            except Exception as err:  # noqa: BLE001
                failure = failure or err
        try:
            closed = self.call(S["room"], "CloseRoom", {"tenant_id": self.tenant, "room_id": self.room_id})
            api_err("CloseRoom", closed.get("error"))
        except Exception as err:  # noqa: BLE001
            failure = failure or err
        if failure:
            raise failure
        return "Signal(ping) -> pong; LeaveRoom + CloseRoom ok"


def main() -> int:
    password = os.environ.get("UDB_PASS")
    if not password:
        print("set UDB_PASS to the admin password bootstrap.sh wrote to secrets/enterprise.env", file=sys.stderr)
        return 2
    tenant_code = env("UDB_TENANT", "acme")
    username = env("UDB_USER", "admin")

    udb = create_udb(
        target=env("UDB_TARGET", "127.0.0.1:50051"),
        auth_target=env("UDB_AUTH_TARGET", "127.0.0.1:50061"),
        tenant_id=tenant_code,  # pre-login hint; replaced by the canonical UUID
        project_id=env("UDB_PROJECT", "default"),
        purpose="native-services-tour",
    )
    try:
        try:
            authn = udb.login_and_adopt_tenant(username, password)
        except Exception as err:  # noqa: BLE001
            print(f"login failed: {err}", file=sys.stderr)
            return 1
        principal = authn.principal
        t = Tour(udb, udb.config.tenant_id, principal.project_id, principal.user_id)
        print(f"logged in as {username} (tenant code {json.dumps(tenant_code)} -> {t.tenant})")

        t.step("AuthnService", False, t.authn)
        t.step("TenantService", False, t.tenant_svc)
        t.step("AuthzService", False, t.authz)
        t.step("ApiKeyService", False, t.api_key)
        t.step("IdentityProviderService", False, t.idp)
        t.step("ConfigService", False, t.config)
        t.step("LockService", False, t.lock)
        t.step("LiveQueryService", False, t.live_query)
        t.step("CacheService", True, t.cache)
        t.step("SchedulerService", False, t.scheduler)
        t.step("WorkflowService", False, t.workflow)
        t.step("WebhookService", False, t.webhook)
        t.step("VaultService", True, t.vault)
        t.step("NotificationService", True, t.notification)
        t.step("MeteringService", True, t.metering)
        t.step("AnalyticsService", False, t.analytics)
        t.step("BackupService", False, t.backup)
        t.step("ControlPlaneService", False, t.control_plane)
        t.step("StorageService", True, t.storage)
        t.step("AssetService", True, t.asset)
        t.step("SearchService", True, t.search)
        t.step("EmbeddingService", True, t.embedding)
        t.step("MessagingService", True, t.messaging)
        t.step("RoomService", True, t.room)
        t.step("PeerService", True, t.peer)
        t.step("TrackService", True, t.track)
        t.step("TurnService", True, t.turn)
        t.step("SignalingService", True, t.signaling)

        print()
        if t.failures:
            print(f"FAILED {len(t.failures)}/{TOTAL_SERVICES} services ({t.called} called, {t.skipped} skipped):")
            for line in t.failures:
                print(f"  {line}")
            return 1
        print(f"OK {t.called}/{TOTAL_SERVICES} services called, {t.skipped} skipped (backend not configured)")
        return 0
    finally:
        # Stops the background bearer refresher and closes the channels.
        udb.close()


if __name__ == "__main__":
    sys.exit(main())
