from google.api import annotations_pb2 as _annotations_pb2
from google.protobuf import field_mask_pb2 as _field_mask_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from udb.core.webhook.entity.v1 import webhook_endpoint_pb2 as _webhook_endpoint_pb2
from udb.core.webhook.entity.v1 import webhook_delivery_pb2 as _webhook_delivery_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateEndpointRequest(_message.Message):
    __slots__ = ("tenant_id", "url", "topic_pattern", "description", "max_attempts", "signing_secret", "metadata_json")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    URL_FIELD_NUMBER: _ClassVar[int]
    TOPIC_PATTERN_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    SIGNING_SECRET_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    url: str
    topic_pattern: str
    description: str
    max_attempts: int
    signing_secret: str
    metadata_json: str
    def __init__(self, tenant_id: _Optional[str] = ..., url: _Optional[str] = ..., topic_pattern: _Optional[str] = ..., description: _Optional[str] = ..., max_attempts: _Optional[int] = ..., signing_secret: _Optional[str] = ..., metadata_json: _Optional[str] = ...) -> None: ...

class CreateEndpointResponse(_message.Message):
    __slots__ = ("endpoint_id", "signing_secret", "message", "error")
    ENDPOINT_ID_FIELD_NUMBER: _ClassVar[int]
    SIGNING_SECRET_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    endpoint_id: str
    signing_secret: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, endpoint_id: _Optional[str] = ..., signing_secret: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetEndpointRequest(_message.Message):
    __slots__ = ("tenant_id", "endpoint_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    endpoint_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., endpoint_id: _Optional[str] = ...) -> None: ...

class GetEndpointResponse(_message.Message):
    __slots__ = ("endpoint", "error")
    ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    endpoint: _webhook_endpoint_pb2.WebhookEndpoint
    error: _dto_pb2.ApiError
    def __init__(self, endpoint: _Optional[_Union[_webhook_endpoint_pb2.WebhookEndpoint, _Mapping]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListEndpointsRequest(_message.Message):
    __slots__ = ("tenant_id", "page", "page_size", "active_only", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_ONLY_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    page: int
    page_size: int
    active_only: bool
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., page: _Optional[int] = ..., page_size: _Optional[int] = ..., active_only: bool = ..., page_token: _Optional[str] = ...) -> None: ...

class ListEndpointsResponse(_message.Message):
    __slots__ = ("endpoints", "total_count", "error", "next_page_token")
    ENDPOINTS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    endpoints: _containers.RepeatedCompositeFieldContainer[_webhook_endpoint_pb2.WebhookEndpoint]
    total_count: int
    error: _dto_pb2.ApiError
    next_page_token: str
    def __init__(self, endpoints: _Optional[_Iterable[_Union[_webhook_endpoint_pb2.WebhookEndpoint, _Mapping]]] = ..., total_count: _Optional[int] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class UpdateEndpointRequest(_message.Message):
    __slots__ = ("tenant_id", "endpoint_id", "url", "topic_pattern", "description", "active", "max_attempts", "update_mask")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_ID_FIELD_NUMBER: _ClassVar[int]
    URL_FIELD_NUMBER: _ClassVar[int]
    TOPIC_PATTERN_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    UPDATE_MASK_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    endpoint_id: str
    url: str
    topic_pattern: str
    description: str
    active: bool
    max_attempts: int
    update_mask: _field_mask_pb2.FieldMask
    def __init__(self, tenant_id: _Optional[str] = ..., endpoint_id: _Optional[str] = ..., url: _Optional[str] = ..., topic_pattern: _Optional[str] = ..., description: _Optional[str] = ..., active: bool = ..., max_attempts: _Optional[int] = ..., update_mask: _Optional[_Union[_field_mask_pb2.FieldMask, _Mapping]] = ...) -> None: ...

class UpdateEndpointResponse(_message.Message):
    __slots__ = ("message", "error")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class DeleteEndpointRequest(_message.Message):
    __slots__ = ("tenant_id", "endpoint_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    endpoint_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., endpoint_id: _Optional[str] = ...) -> None: ...

class DeleteEndpointResponse(_message.Message):
    __slots__ = ("message", "error")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListDeliveriesRequest(_message.Message):
    __slots__ = ("tenant_id", "endpoint_id", "status", "page", "page_size", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    endpoint_id: str
    status: str
    page: int
    page_size: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., endpoint_id: _Optional[str] = ..., status: _Optional[str] = ..., page: _Optional[int] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListDeliveriesResponse(_message.Message):
    __slots__ = ("deliveries", "total_count", "error", "next_page_token")
    DELIVERIES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    deliveries: _containers.RepeatedCompositeFieldContainer[_webhook_delivery_pb2.WebhookDelivery]
    total_count: int
    error: _dto_pb2.ApiError
    next_page_token: str
    def __init__(self, deliveries: _Optional[_Iterable[_Union[_webhook_delivery_pb2.WebhookDelivery, _Mapping]]] = ..., total_count: _Optional[int] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., next_page_token: _Optional[str] = ...) -> None: ...
