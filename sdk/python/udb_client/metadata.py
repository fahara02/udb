from __future__ import annotations

from dataclasses import dataclass, field, replace
import json
import os
import uuid
from typing import Any, Iterable, Sequence

from udb.entity.v1 import types_pb2

UDB_PROTOCOL_VERSION = "1.0.0"


@dataclass(frozen=True)
class WriteReceipt:
    """The durability receipt a mutation returns (``MutationResponse.write_receipt_json``).

    Mirrors the Rust ``WriteReceipt`` serde shape: all five fields are always
    emitted. ``source_lsn`` is the load-bearing field mapped into a
    :class:`ReadFence`'s ``min_outbox_lsn`` by :meth:`ReadFence.from_receipt`.
    """

    source_lsn: str = ""
    outbox_seq: int = 0
    projection_task_ids: Sequence[str] = field(default_factory=tuple)
    manifest_checksum: str = ""
    written_at_unix_ms: int = 0

    @classmethod
    def from_json(cls, raw: str | bytes | dict[str, Any] | None) -> "WriteReceipt":
        """Parse a ``write_receipt_json`` string/bytes/dict into a typed receipt."""
        if raw is None or raw == "" or raw == b"":
            return cls()
        if isinstance(raw, (str, bytes)):
            data = json.loads(raw)
        else:
            data = dict(raw)
        return cls(
            source_lsn=str(data.get("source_lsn", "") or ""),
            outbox_seq=int(data.get("outbox_seq", 0) or 0),
            projection_task_ids=tuple(data.get("projection_task_ids", []) or []),
            manifest_checksum=str(data.get("manifest_checksum", "") or ""),
            written_at_unix_ms=int(data.get("written_at_unix_ms", 0) or 0),
        )

    def to_dict(self) -> dict[str, Any]:
        """All five fields, always present (matches the Rust serde shape)."""
        return {
            "source_lsn": self.source_lsn,
            "outbox_seq": self.outbox_seq,
            "projection_task_ids": list(self.projection_task_ids),
            "manifest_checksum": self.manifest_checksum,
            "written_at_unix_ms": self.written_at_unix_ms,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))


# Default fence wait when a caller does not specify one (matches the golden
# fixture's max_wait_ms and the other SDKs).
DEFAULT_READ_FENCE_MAX_WAIT_MS = 2500


def read_fence_from_receipt(
    receipt: "WriteReceipt",
    max_wait_ms: int = DEFAULT_READ_FENCE_MAX_WAIT_MS,
) -> "ReadFence":
    """Build a :class:`ReadFence` from a :class:`WriteReceipt` (canonical name).

    Naming-contract free-function alias of :meth:`ReadFence.from_receipt`
    (the spec's ``readFenceFromReceipt``). ``ReadFence.from_receipt`` remains as
    the existing alias. Maps the receipt's ``source_lsn`` into the fence's
    ``min_outbox_lsn`` and copies the projection task ids through.
    """
    return ReadFence.from_receipt(receipt, max_wait_ms)


@dataclass(frozen=True)
class ReadFence:
    """The read-your-writes fence a subsequent read carries (``x-udb-read-fence``).

    Mirrors the Rust ``ReadFence`` serde shape: ``max_wait_ms`` is ALWAYS
    serialized; ``min_outbox_lsn`` and ``projection_task_ids`` are skipped when
    empty (serde ``skip_serializing_if``).
    """

    min_outbox_lsn: str = ""
    projection_task_ids: Sequence[str] = field(default_factory=tuple)
    max_wait_ms: int = DEFAULT_READ_FENCE_MAX_WAIT_MS

    @classmethod
    def from_receipt(
        cls, receipt: WriteReceipt, max_wait_ms: int = DEFAULT_READ_FENCE_MAX_WAIT_MS
    ) -> "ReadFence":
        """Build a fence from a receipt, mapping ``source_lsn → min_outbox_lsn``.

        This is the load-bearing cross-type mapping (not a field rename): the
        receipt's ``source_lsn`` becomes the fence's ``min_outbox_lsn`` and the
        projection task ids are copied through.
        """
        return cls(
            min_outbox_lsn=receipt.source_lsn,
            projection_task_ids=tuple(receipt.projection_task_ids),
            max_wait_ms=max_wait_ms,
        )

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"max_wait_ms": self.max_wait_ms}
        if self.min_outbox_lsn:
            out["min_outbox_lsn"] = self.min_outbox_lsn
        if self.projection_task_ids:
            out["projection_task_ids"] = list(self.projection_task_ids)
        return out

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class Metadata:
    """Request metadata required by the UDB broker."""

    tenant_id: str
    purpose: str
    correlation_id: str
    scopes: Sequence[str] = field(default_factory=tuple)
    service_identity: str = "python.app"
    user_id: str = ""
    project_id: str = "default"
    client_catalog_version: str = UDB_PROTOCOL_VERSION
    consistency: str = ""
    target_backend: str = ""
    target_instance: str = ""
    routing_policy: str = ""
    primary_read: bool = False
    max_replica_lag_ms: int = 0
    eventual_consistency_allowed: bool = False
    read_fence_json: str = ""
    trace_id: str = ""
    bearer_token: str = ""
    api_key: str = ""

    @classmethod
    def from_env(
        cls,
        *,
        tenant_id: str | None = None,
        purpose: str | None = None,
        correlation_id: str | None = None,
        scopes: Sequence[str] | None = None,
        prefix: str = "UDB_",
    ) -> "Metadata":
        """Build metadata from environment variables with explicit overrides."""

        def env(name: str, default: str = "") -> str:
            return os.getenv(f"{prefix}{name}", default)

        raw_scopes = env("SCOPES")
        parsed_scopes = tuple(
            scope.strip() for scope in raw_scopes.split(",") if scope.strip()
        )
        return cls(
            tenant_id=tenant_id or env("TENANT_ID"),
            user_id=env("USER_ID"),
            purpose=purpose or env("PURPOSE", "python.request"),
            correlation_id=correlation_id or env("CORRELATION_ID"),
            scopes=tuple(scopes) if scopes is not None else parsed_scopes,
            service_identity=env("SERVICE_IDENTITY", "python.app"),
            project_id=env("PROJECT_ID", "default"),
            client_catalog_version=env(
                "CLIENT_CATALOG_VERSION", UDB_PROTOCOL_VERSION
            ),
            consistency=env("CONSISTENCY"),
            target_backend=env("TARGET_BACKEND"),
            target_instance=env("TARGET_INSTANCE"),
            routing_policy=env("ROUTING_POLICY"),
            primary_read=env("PRIMARY_READ").lower() == "true",
            max_replica_lag_ms=int(env("MAX_REPLICA_LAG_MS", "0") or "0"),
            eventual_consistency_allowed=(
                env("EVENTUAL_CONSISTENCY_ALLOWED").lower() == "true"
            ),
            read_fence_json=env("READ_FENCE_JSON"),
            trace_id=env("TRACE_ID"),
            bearer_token=env("BEARER_TOKEN"),
            api_key=env("API_KEY"),
        )

    def to_grpc_metadata(self) -> tuple[tuple[str, str], ...]:
        # §13: native RPCs require a request context (x-request-id /
        # x-correlation-id / traceparent) and fail closed without one. Always
        # send a request id, and fall back to it for the correlation id when the
        # caller set none.
        request_id = uuid.uuid4().hex
        correlation_id = self.correlation_id or request_id
        headers: list[tuple[str, str]] = [
            ("x-tenant-id", self.tenant_id),
            ("x-user-id", self.user_id),
            ("x-purpose", self.purpose),
            ("x-correlation-id", correlation_id),
            ("x-request-id", request_id),
            ("x-scopes", ",".join(self.scopes)),
            ("x-service-identity", self.service_identity),
            ("x-udb-project-id", self.project_id),
            ("x-udb-client-catalog-version", self.client_catalog_version),
        ]
        if self.bearer_token:
            bearer = self.bearer_token.strip()
            if not bearer.lower().startswith("bearer "):
                bearer = f"Bearer {bearer}"
            headers.append(("authorization", bearer))
        if self.api_key:
            headers.append(("x-api-key", self.api_key))
        optional = {
            "x-udb-consistency": self.consistency,
            "x-udb-target-backend": self.target_backend,
            "x-udb-target-instance": self.target_instance,
            "x-udb-routing-policy": self.routing_policy,
            "x-udb-read-fence": self.read_fence_json,
            "x-udb-max-replica-lag-ms": (
                str(self.max_replica_lag_ms) if self.max_replica_lag_ms else ""
            ),
            "x-udb-primary-read": "true" if self.primary_read else "",
            "x-udb-eventual-consistency-allowed": (
                "true" if self.eventual_consistency_allowed else ""
            ),
        }
        headers.extend((key, value) for key, value in optional.items() if value)
        return tuple(headers)

    def grpc_metadata(self) -> Iterable[tuple[str, str]]:
        """Backward-compatible alias for older examples."""

        return self.to_grpc_metadata()

    def to_request_context(self) -> types_pb2.RequestContext:
        return types_pb2.RequestContext(
            tenant_id=self.tenant_id,
            user_id=self.user_id,
            purpose=self.purpose,
            correlation_id=self.correlation_id,
            scopes=list(self.scopes),
            service_identity=self.service_identity,
            trace_id=self.trace_id,
            target_backend=self.target_backend,
            target_instance=self.target_instance,
            routing_policy=self.routing_policy,
            primary_read=self.primary_read,
            max_replica_lag_ms=self.max_replica_lag_ms,
            eventual_consistency_allowed=self.eventual_consistency_allowed,
            read_fence_json=self.read_fence_json,
            project_id=self.project_id,
            client_catalog_version=self.client_catalog_version,
        )

    def with_purpose(self, purpose: str) -> "Metadata":
        return replace(self, purpose=purpose)

    def with_scopes(self, scopes: Sequence[str]) -> "Metadata":
        return replace(self, scopes=tuple(scopes))

    def with_project_id(self, project_id: str) -> "Metadata":
        return replace(self, project_id=project_id)

    def with_read_fence(self, read_fence_json: str) -> "Metadata":
        return replace(self, read_fence_json=read_fence_json)

    def after_write(
        self, receipt: WriteReceipt, max_wait_ms: int = DEFAULT_READ_FENCE_MAX_WAIT_MS
    ) -> "Metadata":
        """Return metadata carrying a read fence derived from a write receipt.

        Read-your-writes helper: ``udb.after_write(receipt)`` produces metadata
        whose ``read_fence_json`` maps the receipt's ``source_lsn`` into the
        fence's ``min_outbox_lsn`` (see :meth:`ReadFence.from_receipt`).
        """
        fence = ReadFence.from_receipt(receipt, max_wait_ms)
        return replace(self, read_fence_json=fence.to_json())
