from __future__ import annotations

from typing import Any, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field

from udb.entity.v1 import types_pb2

from .client import to_record_json, to_struct
from .metadata import Metadata, UDB_PROTOCOL_VERSION


class UdbModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MetadataModel(UdbModel):
    tenant_id: str = Field(min_length=1)
    purpose: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)
    scopes: tuple[str, ...] = ()
    service_identity: str = "python.app"
    user_id: str = ""
    project_id: str = "default"
    client_catalog_version: str = UDB_PROTOCOL_VERSION
    consistency: str = ""
    target_backend: str = ""
    target_instance: str = ""
    routing_policy: str = ""
    primary_read: bool = False
    max_replica_lag_ms: int = Field(default=0, ge=0)
    eventual_consistency_allowed: bool = False
    read_fence_json: str = ""
    trace_id: str = ""

    def to_metadata(self) -> Metadata:
        return Metadata(
            tenant_id=self.tenant_id,
            user_id=self.user_id,
            purpose=self.purpose,
            correlation_id=self.correlation_id,
            scopes=self.scopes,
            service_identity=self.service_identity,
            project_id=self.project_id,
            client_catalog_version=self.client_catalog_version,
            consistency=self.consistency,
            target_backend=self.target_backend,
            target_instance=self.target_instance,
            routing_policy=self.routing_policy,
            primary_read=self.primary_read,
            max_replica_lag_ms=self.max_replica_lag_ms,
            eventual_consistency_allowed=self.eventual_consistency_allowed,
            read_fence_json=self.read_fence_json,
            trace_id=self.trace_id,
        )


class SortModel(UdbModel):
    field: str = Field(min_length=1)
    descending: bool = False

    def to_proto(self) -> types_pb2.Sort:
        return types_pb2.Sort(field=self.field, descending=self.descending)


class SelectQuery(UdbModel):
    message_type: str = Field(min_length=1)
    filter: Mapping[str, Any] = Field(default_factory=dict)
    fields: tuple[str, ...] = ()
    limit: int = Field(default=0, ge=0)
    page_token: str = ""
    sort: tuple[SortModel, ...] = ()

    def to_proto(self, metadata: Metadata | None = None) -> types_pb2.SelectRequest:
        request = types_pb2.SelectRequest(
            message_type=self.message_type,
            filter=to_struct(self.filter),
            fields=list(self.fields),
            limit=self.limit,
            page_token=self.page_token,
            sort=[item.to_proto() for item in self.sort],
        )
        if metadata is not None:
            request.context.CopyFrom(metadata.to_request_context())
        return request


class UpsertCommand(UdbModel):
    message_type: str = Field(min_length=1)
    record: Mapping[str, Any] | bytes | str
    payload: Mapping[str, Any] = Field(default_factory=dict)
    conflict_fields: tuple[str, ...] = ()
    return_record: bool = False
    idempotency_key: str = ""

    def to_proto(self, metadata: Metadata | None = None) -> types_pb2.UpsertRequest:
        request = types_pb2.UpsertRequest(
            message_type=self.message_type,
            record_json=to_record_json(self.record),
            payload=to_struct(self.payload),
            conflict_fields=list(self.conflict_fields),
            return_record=self.return_record,
            idempotency_key=self.idempotency_key,
        )
        if metadata is not None:
            request.context.CopyFrom(metadata.to_request_context())
        return request


class DeleteCommand(UdbModel):
    message_type: str = Field(min_length=1)
    filter: Mapping[str, Any] = Field(default_factory=dict)
    idempotency_key: str = ""

    def to_proto(self, metadata: Metadata | None = None) -> types_pb2.DeleteRequest:
        request = types_pb2.DeleteRequest(
            message_type=self.message_type,
            filter=to_struct(self.filter),
            idempotency_key=self.idempotency_key,
        )
        if metadata is not None:
            request.context.CopyFrom(metadata.to_request_context())
        return request


class VectorPointModel(UdbModel):
    id: str = Field(min_length=1)
    vector: Sequence[float]
    payload: Mapping[str, Any] = Field(default_factory=dict)

    def to_proto(self) -> types_pb2.VectorPointMutation:
        return types_pb2.VectorPointMutation(
            id=self.id,
            vector=list(self.vector),
            payload=to_struct(self.payload),
        )
