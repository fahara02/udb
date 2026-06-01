from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor

class ConfigType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONFIG_TYPE_UNSPECIFIED: _ClassVar[ConfigType]
    CONFIG_TYPE_STRING: _ClassVar[ConfigType]
    CONFIG_TYPE_NUMBER: _ClassVar[ConfigType]
    CONFIG_TYPE_BOOLEAN: _ClassVar[ConfigType]
    CONFIG_TYPE_JSON: _ClassVar[ConfigType]

class TenantType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TENANT_TYPE_UNSPECIFIED: _ClassVar[TenantType]
    TENANT_TYPE_PLATFORM: _ClassVar[TenantType]
    TENANT_TYPE_PARTNER: _ClassVar[TenantType]
    TENANT_TYPE_ORGANIZATION: _ClassVar[TenantType]
    TENANT_TYPE_WORKSPACE: _ClassVar[TenantType]
    TENANT_TYPE_CUSTOMER_ACCOUNT: _ClassVar[TenantType]
    TENANT_TYPE_DEPARTMENT: _ClassVar[TenantType]
    TENANT_TYPE_SANDBOX: _ClassVar[TenantType]

class TenantStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TENANT_STATUS_UNSPECIFIED: _ClassVar[TenantStatus]
    TENANT_STATUS_ACTIVE: _ClassVar[TenantStatus]
    TENANT_STATUS_SUSPENDED: _ClassVar[TenantStatus]
    TENANT_STATUS_INACTIVE: _ClassVar[TenantStatus]
CONFIG_TYPE_UNSPECIFIED: ConfigType
CONFIG_TYPE_STRING: ConfigType
CONFIG_TYPE_NUMBER: ConfigType
CONFIG_TYPE_BOOLEAN: ConfigType
CONFIG_TYPE_JSON: ConfigType
TENANT_TYPE_UNSPECIFIED: TenantType
TENANT_TYPE_PLATFORM: TenantType
TENANT_TYPE_PARTNER: TenantType
TENANT_TYPE_ORGANIZATION: TenantType
TENANT_TYPE_WORKSPACE: TenantType
TENANT_TYPE_CUSTOMER_ACCOUNT: TenantType
TENANT_TYPE_DEPARTMENT: TenantType
TENANT_TYPE_SANDBOX: TenantType
TENANT_STATUS_UNSPECIFIED: TenantStatus
TENANT_STATUS_ACTIVE: TenantStatus
TENANT_STATUS_SUSPENDED: TenantStatus
TENANT_STATUS_INACTIVE: TenantStatus
