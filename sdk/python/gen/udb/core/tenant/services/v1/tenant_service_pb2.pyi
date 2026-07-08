from google.api import annotations_pb2 as _annotations_pb2
from google.protobuf import field_mask_pb2 as _field_mask_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from udb.core.tenant.entity.v1 import tenant_pb2 as _tenant_pb2
from udb.core.tenant.entity.v1 import tenant_config_pb2 as _tenant_config_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateTenantRequest(_message.Message):
    __slots__ = ("code", "name", "type", "parent_tenant_id", "config", "branding")
    CODE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    PARENT_TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    BRANDING_FIELD_NUMBER: _ClassVar[int]
    code: str
    name: str
    type: str
    parent_tenant_id: str
    config: str
    branding: str
    def __init__(self, code: _Optional[str] = ..., name: _Optional[str] = ..., type: _Optional[str] = ..., parent_tenant_id: _Optional[str] = ..., config: _Optional[str] = ..., branding: _Optional[str] = ...) -> None: ...

class CreateTenantResponse(_message.Message):
    __slots__ = ("tenant_id", "message", "error")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, tenant_id: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetTenantRequest(_message.Message):
    __slots__ = ("tenant_id",)
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    def __init__(self, tenant_id: _Optional[str] = ...) -> None: ...

class GetTenantResponse(_message.Message):
    __slots__ = ("tenant", "error")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    tenant: _tenant_pb2.Tenant
    error: _dto_pb2.ApiError
    def __init__(self, tenant: _Optional[_Union[_tenant_pb2.Tenant, _Mapping]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListTenantsRequest(_message.Message):
    __slots__ = ("type", "status", "page", "page_size", "page_token")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    type: str
    status: str
    page: int
    page_size: int
    page_token: str
    def __init__(self, type: _Optional[str] = ..., status: _Optional[str] = ..., page: _Optional[int] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListTenantsResponse(_message.Message):
    __slots__ = ("tenants", "total_count", "error", "next_page_token")
    TENANTS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenants: _containers.RepeatedCompositeFieldContainer[_tenant_pb2.Tenant]
    total_count: int
    error: _dto_pb2.ApiError
    next_page_token: str
    def __init__(self, tenants: _Optional[_Iterable[_Union[_tenant_pb2.Tenant, _Mapping]]] = ..., total_count: _Optional[int] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class UpdateTenantRequest(_message.Message):
    __slots__ = ("tenant_id", "name", "status", "config", "branding", "update_mask")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    BRANDING_FIELD_NUMBER: _ClassVar[int]
    UPDATE_MASK_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    name: str
    status: str
    config: str
    branding: str
    update_mask: _field_mask_pb2.FieldMask
    def __init__(self, tenant_id: _Optional[str] = ..., name: _Optional[str] = ..., status: _Optional[str] = ..., config: _Optional[str] = ..., branding: _Optional[str] = ..., update_mask: _Optional[_Union[_field_mask_pb2.FieldMask, _Mapping]] = ...) -> None: ...

class UpdateTenantResponse(_message.Message):
    __slots__ = ("message", "error")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetTenantConfigRequest(_message.Message):
    __slots__ = ("tenant_id",)
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    def __init__(self, tenant_id: _Optional[str] = ...) -> None: ...

class GetTenantConfigResponse(_message.Message):
    __slots__ = ("configs", "error")
    CONFIGS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    configs: _containers.RepeatedCompositeFieldContainer[_tenant_config_pb2.TenantConfig]
    error: _dto_pb2.ApiError
    def __init__(self, configs: _Optional[_Iterable[_Union[_tenant_config_pb2.TenantConfig, _Mapping]]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class UpdateTenantConfigRequest(_message.Message):
    __slots__ = ("tenant_id", "config_key", "config_value", "type")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIG_KEY_FIELD_NUMBER: _ClassVar[int]
    CONFIG_VALUE_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    config_key: str
    config_value: str
    type: str
    def __init__(self, tenant_id: _Optional[str] = ..., config_key: _Optional[str] = ..., config_value: _Optional[str] = ..., type: _Optional[str] = ...) -> None: ...

class UpdateTenantConfigResponse(_message.Message):
    __slots__ = ("message", "error")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class PurgeTenantRequest(_message.Message):
    __slots__ = ("tenant_id", "confirmation_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIRMATION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    confirmation_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., confirmation_token: _Optional[str] = ...) -> None: ...

class PurgedTableCount(_message.Message):
    __slots__ = ("schema", "table", "tenant_column", "deleted")
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    TENANT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    DELETED_FIELD_NUMBER: _ClassVar[int]
    schema: str
    table: str
    tenant_column: str
    deleted: int
    def __init__(self, schema: _Optional[str] = ..., table: _Optional[str] = ..., tenant_column: _Optional[str] = ..., deleted: _Optional[int] = ...) -> None: ...

class PurgeExcludedTable(_message.Message):
    __slots__ = ("schema", "table", "reason")
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    schema: str
    table: str
    reason: str
    def __init__(self, schema: _Optional[str] = ..., table: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class PurgeTenantResponse(_message.Message):
    __slots__ = ("tenant_id", "purged", "excluded", "total_deleted", "tenant_denylisted", "principals_denylisted", "message", "error")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PURGED_FIELD_NUMBER: _ClassVar[int]
    EXCLUDED_FIELD_NUMBER: _ClassVar[int]
    TOTAL_DELETED_FIELD_NUMBER: _ClassVar[int]
    TENANT_DENYLISTED_FIELD_NUMBER: _ClassVar[int]
    PRINCIPALS_DENYLISTED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    purged: _containers.RepeatedCompositeFieldContainer[PurgedTableCount]
    excluded: _containers.RepeatedCompositeFieldContainer[PurgeExcludedTable]
    total_deleted: int
    tenant_denylisted: bool
    principals_denylisted: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, tenant_id: _Optional[str] = ..., purged: _Optional[_Iterable[_Union[PurgedTableCount, _Mapping]]] = ..., excluded: _Optional[_Iterable[_Union[PurgeExcludedTable, _Mapping]]] = ..., total_deleted: _Optional[int] = ..., tenant_denylisted: bool = ..., principals_denylisted: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...
