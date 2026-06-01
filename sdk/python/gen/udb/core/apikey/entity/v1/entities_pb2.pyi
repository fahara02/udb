from udb.core.apikey.entity.v1 import api_key_pb2 as _api_key_pb2
from udb.core.apikey.entity.v1 import api_key_usage_pb2 as _api_key_usage_pb2
from udb.core.apikey.entity.v1 import enums_pb2 as _enums_pb2
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar
from udb.core.apikey.entity.v1.api_key_pb2 import ApiKey as ApiKey
from udb.core.apikey.entity.v1.api_key_usage_pb2 import ApiKeyUsage as ApiKeyUsage
from udb.core.apikey.entity.v1.enums_pb2 import ApiKeyStatus as ApiKeyStatus
from udb.core.apikey.entity.v1.enums_pb2 import ApiKeyOwnerType as ApiKeyOwnerType

DESCRIPTOR: _descriptor.FileDescriptor
API_KEY_STATUS_UNSPECIFIED: _enums_pb2.ApiKeyStatus
API_KEY_STATUS_ACTIVE: _enums_pb2.ApiKeyStatus
API_KEY_STATUS_REVOKED: _enums_pb2.ApiKeyStatus
API_KEY_STATUS_EXPIRED: _enums_pb2.ApiKeyStatus
API_KEY_OWNER_TYPE_UNSPECIFIED: _enums_pb2.ApiKeyOwnerType
API_KEY_OWNER_TYPE_INTEGRATION: _enums_pb2.ApiKeyOwnerType
API_KEY_OWNER_TYPE_CICD: _enums_pb2.ApiKeyOwnerType
API_KEY_OWNER_TYPE_ANALYTICS: _enums_pb2.ApiKeyOwnerType
API_KEY_OWNER_TYPE_TENANT: _enums_pb2.ApiKeyOwnerType
API_KEY_OWNER_TYPE_PROJECT: _enums_pb2.ApiKeyOwnerType
API_KEY_OWNER_TYPE_SERVICE_ACCOUNT: _enums_pb2.ApiKeyOwnerType
API_KEY_OWNER_TYPE_WORKLOAD: _enums_pb2.ApiKeyOwnerType
