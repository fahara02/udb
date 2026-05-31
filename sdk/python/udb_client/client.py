from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Sequence

import grpc

from udb.entity.v1 import types_pb2
from udb.services.v1 import data_broker_pb2_grpc

UDB_PROTOCOL_VERSION = "1.0.0"


@dataclass(frozen=True)
class Metadata:
    tenant_id: str
    purpose: str
    correlation_id: str
    scopes: Sequence[str] = field(default_factory=tuple)
    service_identity: str = "example.service"
    user_id: str = ""
    project_id: str = "default"
    client_catalog_version: str = UDB_PROTOCOL_VERSION

    def grpc_metadata(self) -> Iterable[tuple[str, str]]:
        return (
            ("x-tenant-id", self.tenant_id),
            ("x-user-id", self.user_id),
            ("x-purpose", self.purpose),
            ("x-correlation-id", self.correlation_id),
            ("x-scopes", ",".join(self.scopes)),
            ("x-service-identity", self.service_identity),
            ("x-udb-project-id", self.project_id),
            ("x-udb-client-catalog-version", self.client_catalog_version),
        )


class UdbAsyncClient:
    def __init__(self, target: str, metadata: Metadata):
        self._channel = grpc.aio.insecure_channel(target)
        self._stub = data_broker_pb2_grpc.DataBrokerStub(self._channel)
        self._metadata = metadata

    async def close(self) -> None:
        await self._channel.close()

    async def select(self, request: types_pb2.SelectRequest):
        return await self._stub.Select(request, metadata=self._metadata.grpc_metadata())

    async def upsert(self, request: types_pb2.UpsertRequest):
        return await self._stub.Upsert(request, metadata=self._metadata.grpc_metadata())

    async def enqueue_outbox_event(self, request: types_pb2.EnqueueOutboxEventRequest):
        return await self._stub.EnqueueOutboxEvent(
            request, metadata=self._metadata.grpc_metadata()
        )
