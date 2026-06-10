from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor

class ResourceType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RESOURCE_TYPE_UNSPECIFIED: _ClassVar[ResourceType]
    RESOURCE_TYPE_ROUTING_POLICY: _ClassVar[ResourceType]
    RESOURCE_TYPE_METHOD_SECURITY_POLICY: _ClassVar[ResourceType]
    RESOURCE_TYPE_RLS_TENANT_POLICY: _ClassVar[ResourceType]
    RESOURCE_TYPE_NATIVE_SERVICE_ENABLEMENT: _ClassVar[ResourceType]
    RESOURCE_TYPE_BACKEND_TARGET_DEFINITION: _ClassVar[ResourceType]
RESOURCE_TYPE_UNSPECIFIED: ResourceType
RESOURCE_TYPE_ROUTING_POLICY: ResourceType
RESOURCE_TYPE_METHOD_SECURITY_POLICY: ResourceType
RESOURCE_TYPE_RLS_TENANT_POLICY: ResourceType
RESOURCE_TYPE_NATIVE_SERVICE_ENABLEMENT: ResourceType
RESOURCE_TYPE_BACKEND_TARGET_DEFINITION: ResourceType
