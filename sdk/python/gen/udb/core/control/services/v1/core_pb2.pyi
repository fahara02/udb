import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.control.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import types_pb2 as _types_pb2
from udb.core.common.v1 import domain_types_pb2 as _domain_types_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Resource(_message.Message):
    __slots__ = ("name", "version", "payload_json", "resource_type")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    name: str
    version: str
    payload_json: str
    resource_type: _enums_pb2.ResourceType
    def __init__(self, name: _Optional[str] = ..., version: _Optional[str] = ..., payload_json: _Optional[str] = ..., resource_type: _Optional[_Union[_enums_pb2.ResourceType, str]] = ...) -> None: ...

class ErrorDetail(_message.Message):
    __slots__ = ("code", "message")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    code: int
    message: str
    def __init__(self, code: _Optional[int] = ..., message: _Optional[str] = ...) -> None: ...

class DiscoveryRequest(_message.Message):
    __slots__ = ("node_id", "resource_type", "version_info", "response_nonce", "resource_names", "error_detail", "context")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    VERSION_INFO_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_NONCE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAMES_FIELD_NUMBER: _ClassVar[int]
    ERROR_DETAIL_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    resource_type: _enums_pb2.ResourceType
    version_info: str
    response_nonce: str
    resource_names: _containers.RepeatedScalarFieldContainer[str]
    error_detail: ErrorDetail
    context: _types_pb2.RequestContext
    def __init__(self, node_id: _Optional[str] = ..., resource_type: _Optional[_Union[_enums_pb2.ResourceType, str]] = ..., version_info: _Optional[str] = ..., response_nonce: _Optional[str] = ..., resource_names: _Optional[_Iterable[str]] = ..., error_detail: _Optional[_Union[ErrorDetail, _Mapping]] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class DiscoveryResponse(_message.Message):
    __slots__ = ("resource_type", "version_info", "nonce", "resources", "removed_resources")
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    VERSION_INFO_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    REMOVED_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    resource_type: _enums_pb2.ResourceType
    version_info: str
    nonce: str
    resources: _containers.RepeatedCompositeFieldContainer[Resource]
    removed_resources: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, resource_type: _Optional[_Union[_enums_pb2.ResourceType, str]] = ..., version_info: _Optional[str] = ..., nonce: _Optional[str] = ..., resources: _Optional[_Iterable[_Union[Resource, _Mapping]]] = ..., removed_resources: _Optional[_Iterable[str]] = ...) -> None: ...

class DeltaDiscoveryRequest(_message.Message):
    __slots__ = ("node_id", "resource_type", "response_nonce", "resource_names_subscribe", "resource_names_unsubscribe", "initial_resource_versions", "error_detail", "context")
    class InitialResourceVersionsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_NONCE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAMES_SUBSCRIBE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAMES_UNSUBSCRIBE_FIELD_NUMBER: _ClassVar[int]
    INITIAL_RESOURCE_VERSIONS_FIELD_NUMBER: _ClassVar[int]
    ERROR_DETAIL_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    resource_type: _enums_pb2.ResourceType
    response_nonce: str
    resource_names_subscribe: _containers.RepeatedScalarFieldContainer[str]
    resource_names_unsubscribe: _containers.RepeatedScalarFieldContainer[str]
    initial_resource_versions: _containers.ScalarMap[str, str]
    error_detail: ErrorDetail
    context: _types_pb2.RequestContext
    def __init__(self, node_id: _Optional[str] = ..., resource_type: _Optional[_Union[_enums_pb2.ResourceType, str]] = ..., response_nonce: _Optional[str] = ..., resource_names_subscribe: _Optional[_Iterable[str]] = ..., resource_names_unsubscribe: _Optional[_Iterable[str]] = ..., initial_resource_versions: _Optional[_Mapping[str, str]] = ..., error_detail: _Optional[_Union[ErrorDetail, _Mapping]] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class DeltaDiscoveryResponse(_message.Message):
    __slots__ = ("resource_type", "nonce", "resources", "removed_resources", "system_version_info")
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    REMOVED_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    SYSTEM_VERSION_INFO_FIELD_NUMBER: _ClassVar[int]
    resource_type: _enums_pb2.ResourceType
    nonce: str
    resources: _containers.RepeatedCompositeFieldContainer[Resource]
    removed_resources: _containers.RepeatedScalarFieldContainer[str]
    system_version_info: str
    def __init__(self, resource_type: _Optional[_Union[_enums_pb2.ResourceType, str]] = ..., nonce: _Optional[str] = ..., resources: _Optional[_Iterable[_Union[Resource, _Mapping]]] = ..., removed_resources: _Optional[_Iterable[str]] = ..., system_version_info: _Optional[str] = ...) -> None: ...

class GetResourcesRequest(_message.Message):
    __slots__ = ("resource_type", "tenant_id", "resource_names", "page", "context")
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAMES_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    resource_type: _enums_pb2.ResourceType
    tenant_id: str
    resource_names: _containers.RepeatedScalarFieldContainer[str]
    page: _dto_pb2.PageRequest
    context: _types_pb2.RequestContext
    def __init__(self, resource_type: _Optional[_Union[_enums_pb2.ResourceType, str]] = ..., tenant_id: _Optional[str] = ..., resource_names: _Optional[_Iterable[str]] = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class GetResourcesResponse(_message.Message):
    __slots__ = ("resources", "version_info", "page")
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    VERSION_INFO_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    resources: _containers.RepeatedCompositeFieldContainer[Resource]
    version_info: str
    page: _dto_pb2.PageResponse
    def __init__(self, resources: _Optional[_Iterable[_Union[Resource, _Mapping]]] = ..., version_info: _Optional[str] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class ListNodeStatesRequest(_message.Message):
    __slots__ = ("node_id", "resource_type", "page", "context")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    resource_type: _enums_pb2.ResourceType
    page: _dto_pb2.PageRequest
    context: _types_pb2.RequestContext
    def __init__(self, node_id: _Optional[str] = ..., resource_type: _Optional[_Union[_enums_pb2.ResourceType, str]] = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class NodeAckState(_message.Message):
    __slots__ = ("node_id", "resource_type", "subscribed_names", "accepted_version", "last_good_version", "last_response_nonce", "nack_error_detail", "in_sync", "updated_at")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    SUBSCRIBED_NAMES_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    LAST_GOOD_VERSION_FIELD_NUMBER: _ClassVar[int]
    LAST_RESPONSE_NONCE_FIELD_NUMBER: _ClassVar[int]
    NACK_ERROR_DETAIL_FIELD_NUMBER: _ClassVar[int]
    IN_SYNC_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    resource_type: _enums_pb2.ResourceType
    subscribed_names: _containers.RepeatedScalarFieldContainer[str]
    accepted_version: str
    last_good_version: str
    last_response_nonce: str
    nack_error_detail: str
    in_sync: bool
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, node_id: _Optional[str] = ..., resource_type: _Optional[_Union[_enums_pb2.ResourceType, str]] = ..., subscribed_names: _Optional[_Iterable[str]] = ..., accepted_version: _Optional[str] = ..., last_good_version: _Optional[str] = ..., last_response_nonce: _Optional[str] = ..., nack_error_detail: _Optional[str] = ..., in_sync: bool = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ListNodeStatesResponse(_message.Message):
    __slots__ = ("node_states", "page")
    NODE_STATES_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    node_states: _containers.RepeatedCompositeFieldContainer[NodeAckState]
    page: _dto_pb2.PageResponse
    def __init__(self, node_states: _Optional[_Iterable[_Union[NodeAckState, _Mapping]]] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class AckStatusRequest(_message.Message):
    __slots__ = ("node_id", "resource_type", "context")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    resource_type: _enums_pb2.ResourceType
    context: _types_pb2.RequestContext
    def __init__(self, node_id: _Optional[str] = ..., resource_type: _Optional[_Union[_enums_pb2.ResourceType, str]] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class AckStatusResponse(_message.Message):
    __slots__ = ("node_state", "current_version", "acknowledged", "nacked")
    NODE_STATE_FIELD_NUMBER: _ClassVar[int]
    CURRENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_FIELD_NUMBER: _ClassVar[int]
    NACKED_FIELD_NUMBER: _ClassVar[int]
    node_state: NodeAckState
    current_version: str
    acknowledged: bool
    nacked: bool
    def __init__(self, node_state: _Optional[_Union[NodeAckState, _Mapping]] = ..., current_version: _Optional[str] = ..., acknowledged: bool = ..., nacked: bool = ...) -> None: ...
