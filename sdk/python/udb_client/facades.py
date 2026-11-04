"""Small facades over native services that every service otherwise wrapped by hand."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Sequence

from udb.entity.v1 import types_pb2

from .exceptions import UdbError
from .metadata import Metadata

_CONFIG_SERVICE = "udb.core.config.services.v1.ConfigService"


@dataclass(frozen=True)
class UserSession:
    """A validated user session (see ``UdbAuthClient.validate_session``)."""

    #: The stable public session id (``sesspub_...``), the same for the login token
    #: and every token refreshed from it. Use it in logs and revocation lists.
    public_id: str = ""
    user_id: str = ""
    tenant_id: str = ""
    project_id: str = ""
    scopes: tuple[str, ...] = field(default_factory=tuple)
    roles: tuple[str, ...] = field(default_factory=tuple)
    expires_at: datetime | None = None
    principal: Any = None


def _evaluate_request(
    config_pb: Any, meta: Metadata, keys: Sequence[str], attrs: Mapping[str, str] | None
) -> Any:
    return config_pb.EvaluateFlagsRequest(
        tenant_id=meta.tenant_id,
        keys=list(keys),
        context=config_pb.EvaluateContext(project_id=meta.project_id, attributes=dict(attrs or {})),
    )


def _is_enabled(values: Mapping[str, Any], key: str) -> bool:
    value = values.get(key)
    return bool(value.bool_value) if value is not None else False


class FlagsFacade:
    """``project.flags``: feature flags, served on the control-plane target.

    ``stub`` is the generated ConfigService stub.
    """

    def __init__(self, stub: Any, owner: Any):
        self.stub = stub
        self._owner = owner

    def evaluate(
        self,
        *keys: str,
        attrs: Mapping[str, str] | None = None,
        metadata: Metadata | None = None,
    ) -> dict[str, Any]:
        """Resolve ``keys`` for the caller's tenant and project with ``attrs``, for
        example ``{"user_id": "..."}``. Unknown keys are absent from the result."""
        from udb.core.config.services.v1 import config_service_pb2 as config_pb

        meta = metadata or self._owner.metadata
        response = self._owner._invoke(
            _CONFIG_SERVICE,
            "EvaluateFlags",
            self.stub,
            _evaluate_request(config_pb, meta, keys, attrs),
            metadata,
        )
        return dict(response.values)

    def enabled(
        self,
        key: str,
        attrs: Mapping[str, str] | None = None,
        *,
        metadata: Metadata | None = None,
    ) -> bool:
        """Whether boolean flag ``key`` is on for ``attrs``. A flag that does not exist is off."""
        return _is_enabled(self.evaluate(key, attrs=attrs, metadata=metadata), key)


class AsyncFlagsFacade:
    """``await project.flags.evaluate(...)`` on :class:`UdbAsyncProject`."""

    def __init__(self, owner: Any):
        self._owner = owner

    async def evaluate(
        self,
        *keys: str,
        attrs: Mapping[str, str] | None = None,
        metadata: Metadata | None = None,
    ) -> dict[str, Any]:
        """See :meth:`FlagsFacade.evaluate`."""
        import grpc

        from udb.core.config.services.v1 import config_service_pb2 as config_pb
        from udb.core.config.services.v1 import config_service_pb2_grpc as config_grpc

        from .generated_client import _map_error

        meta = metadata or self._owner.metadata
        stub = config_grpc.ConfigServiceStub(self._owner._async_control_channel())
        try:
            response = await stub.EvaluateFlags(
                _evaluate_request(config_pb, meta, keys, attrs),
                metadata=meta.to_grpc_metadata(),
                timeout=self._owner.config.deadline,
            )
        except grpc.RpcError as error:
            raise _map_error("EvaluateFlags", error) from error
        return dict(response.values)

    async def enabled(
        self,
        key: str,
        attrs: Mapping[str, str] | None = None,
        *,
        metadata: Metadata | None = None,
    ) -> bool:
        """See :meth:`FlagsFacade.enabled`."""
        return _is_enabled(await self.evaluate(key, attrs=attrs, metadata=metadata), key)


def _plan_request(project_id: str) -> Any:
    return types_pb2.MigrationPlanRequest(project_id=project_id)


def _approve_request(run_id: str, project_id: str) -> Any:
    return types_pb2.MigrationRunRequest(run_id=run_id, project_id=project_id)


def _apply_request(run_id: str, project_id: str, token: str) -> Any:
    return types_pb2.MigrationApplyRequest(run_id=run_id, project_id=project_id, approval_token=token)


def _run_id(plan: Any) -> str:
    if not plan.run_id:
        raise UdbError("udb: PlanMigration returned no run id")
    return plan.run_id


class AdminFacade:
    """``project.admin``: the migration lifecycle over the DataBroker client."""

    def __init__(self, client: Any):
        self._client = client

    def apply_current(self, project_id: str, *, metadata: Metadata | None = None) -> Any:
        """Run plan, approve and apply for a project, and return the final
        ``MigrationStatusResponse``. The approval token comes from the approve response."""
        plan = self._client.call("PlanMigration", _plan_request(project_id), metadata=metadata)
        run_id = _run_id(plan)
        approved = self._client.call(
            "ApproveMigrationPlan", _approve_request(run_id, project_id), metadata=metadata
        )
        return self._client.call(
            "ApplyMigration",
            _apply_request(run_id, project_id, approved.approval_token),
            metadata=metadata,
        )


class AsyncAdminFacade:
    """``await project.admin.apply_current(...)`` on :class:`UdbAsyncProject`."""

    def __init__(self, client: Any):
        self._client = client

    async def apply_current(self, project_id: str, *, metadata: Metadata | None = None) -> Any:
        """See :meth:`AdminFacade.apply_current`."""
        plan = await self._client.call("PlanMigration", _plan_request(project_id), metadata=metadata)
        run_id = _run_id(plan)
        approved = await self._client.call(
            "ApproveMigrationPlan", _approve_request(run_id, project_id), metadata=metadata
        )
        return await self._client.call(
            "ApplyMigration",
            _apply_request(run_id, project_id, approved.approval_token),
            metadata=metadata,
        )

