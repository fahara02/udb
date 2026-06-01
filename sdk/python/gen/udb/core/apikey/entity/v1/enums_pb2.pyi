from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor

class ApiKeyStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    API_KEY_STATUS_UNSPECIFIED: _ClassVar[ApiKeyStatus]
    API_KEY_STATUS_ACTIVE: _ClassVar[ApiKeyStatus]
    API_KEY_STATUS_REVOKED: _ClassVar[ApiKeyStatus]
    API_KEY_STATUS_EXPIRED: _ClassVar[ApiKeyStatus]

class ApiKeyOwnerType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    API_KEY_OWNER_TYPE_UNSPECIFIED: _ClassVar[ApiKeyOwnerType]
    API_KEY_OWNER_TYPE_INTEGRATION: _ClassVar[ApiKeyOwnerType]
    API_KEY_OWNER_TYPE_CICD: _ClassVar[ApiKeyOwnerType]
    API_KEY_OWNER_TYPE_ANALYTICS: _ClassVar[ApiKeyOwnerType]
    API_KEY_OWNER_TYPE_TENANT: _ClassVar[ApiKeyOwnerType]
    API_KEY_OWNER_TYPE_PROJECT: _ClassVar[ApiKeyOwnerType]
    API_KEY_OWNER_TYPE_SERVICE_ACCOUNT: _ClassVar[ApiKeyOwnerType]
    API_KEY_OWNER_TYPE_WORKLOAD: _ClassVar[ApiKeyOwnerType]
API_KEY_STATUS_UNSPECIFIED: ApiKeyStatus
API_KEY_STATUS_ACTIVE: ApiKeyStatus
API_KEY_STATUS_REVOKED: ApiKeyStatus
API_KEY_STATUS_EXPIRED: ApiKeyStatus
API_KEY_OWNER_TYPE_UNSPECIFIED: ApiKeyOwnerType
API_KEY_OWNER_TYPE_INTEGRATION: ApiKeyOwnerType
API_KEY_OWNER_TYPE_CICD: ApiKeyOwnerType
API_KEY_OWNER_TYPE_ANALYTICS: ApiKeyOwnerType
API_KEY_OWNER_TYPE_TENANT: ApiKeyOwnerType
API_KEY_OWNER_TYPE_PROJECT: ApiKeyOwnerType
API_KEY_OWNER_TYPE_SERVICE_ACCOUNT: ApiKeyOwnerType
API_KEY_OWNER_TYPE_WORKLOAD: ApiKeyOwnerType
