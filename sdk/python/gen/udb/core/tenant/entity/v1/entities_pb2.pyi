from udb.core.tenant.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.tenant.entity.v1 import tenant_pb2 as _tenant_pb2
from udb.core.tenant.entity.v1 import tenant_config_pb2 as _tenant_config_pb2
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar
from udb.core.tenant.entity.v1.enums_pb2 import ConfigType as ConfigType
from udb.core.tenant.entity.v1.enums_pb2 import TenantType as TenantType
from udb.core.tenant.entity.v1.enums_pb2 import TenantStatus as TenantStatus
from udb.core.tenant.entity.v1.tenant_pb2 import Tenant as Tenant
from udb.core.tenant.entity.v1.tenant_config_pb2 import TenantConfig as TenantConfig

DESCRIPTOR: _descriptor.FileDescriptor
CONFIG_TYPE_UNSPECIFIED: _enums_pb2.ConfigType
CONFIG_TYPE_STRING: _enums_pb2.ConfigType
CONFIG_TYPE_NUMBER: _enums_pb2.ConfigType
CONFIG_TYPE_BOOLEAN: _enums_pb2.ConfigType
CONFIG_TYPE_JSON: _enums_pb2.ConfigType
TENANT_TYPE_UNSPECIFIED: _enums_pb2.TenantType
TENANT_TYPE_PLATFORM: _enums_pb2.TenantType
TENANT_TYPE_PARTNER: _enums_pb2.TenantType
TENANT_TYPE_ORGANIZATION: _enums_pb2.TenantType
TENANT_TYPE_WORKSPACE: _enums_pb2.TenantType
TENANT_TYPE_CUSTOMER_ACCOUNT: _enums_pb2.TenantType
TENANT_TYPE_DEPARTMENT: _enums_pb2.TenantType
TENANT_TYPE_SANDBOX: _enums_pb2.TenantType
TENANT_STATUS_UNSPECIFIED: _enums_pb2.TenantStatus
TENANT_STATUS_ACTIVE: _enums_pb2.TenantStatus
TENANT_STATUS_SUSPENDED: _enums_pb2.TenantStatus
TENANT_STATUS_INACTIVE: _enums_pb2.TenantStatus
