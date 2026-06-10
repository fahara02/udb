from __future__ import annotations

from dataclasses import dataclass, field, replace
import os
from typing import Iterable, Sequence

from udb.entity.v1 import types_pb2

UDB_PROTOCOL_VERSION = "1.0.0"


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
        headers: list[tuple[str, str]] = [
            ("x-tenant-id", self.tenant_id),
            ("x-user-id", self.user_id),
            ("x-purpose", self.purpose),
            ("x-correlation-id", self.correlation_id),
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
        )

    def with_purpose(self, purpose: str) -> "Metadata":
        return replace(self, purpose=purpose)

    def with_scopes(self, scopes: Sequence[str]) -> "Metadata":
        return replace(self, scopes=tuple(scopes))

    def with_project_id(self, project_id: str) -> "Metadata":
        return replace(self, project_id=project_id)

    def with_read_fence(self, read_fence_json: str) -> "Metadata":
        return replace(self, read_fence_json=read_fence_json)
