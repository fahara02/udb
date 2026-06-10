import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.idp.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class IdentityProvider(_message.Message):
    __slots__ = ("provider_id", "tenant_id", "kind", "display_name", "issuer", "entity_id", "jwks_url", "saml_metadata_url", "client_ids_json", "audiences_json", "claim_mapping_json", "group_mapping_json", "jit_policy_json", "account_linking_policy", "enabled", "client_secret", "saml_signing_key_pem", "saml_idp_certs_json", "saml_sso_url", "health", "last_jwks_refresh_at", "last_jwks_refresh_status", "created_by", "updated_by", "created_at", "updated_at", "deleted_at")
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    ISSUER_FIELD_NUMBER: _ClassVar[int]
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    JWKS_URL_FIELD_NUMBER: _ClassVar[int]
    SAML_METADATA_URL_FIELD_NUMBER: _ClassVar[int]
    CLIENT_IDS_JSON_FIELD_NUMBER: _ClassVar[int]
    AUDIENCES_JSON_FIELD_NUMBER: _ClassVar[int]
    CLAIM_MAPPING_JSON_FIELD_NUMBER: _ClassVar[int]
    GROUP_MAPPING_JSON_FIELD_NUMBER: _ClassVar[int]
    JIT_POLICY_JSON_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_LINKING_POLICY_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    CLIENT_SECRET_FIELD_NUMBER: _ClassVar[int]
    SAML_SIGNING_KEY_PEM_FIELD_NUMBER: _ClassVar[int]
    SAML_IDP_CERTS_JSON_FIELD_NUMBER: _ClassVar[int]
    SAML_SSO_URL_FIELD_NUMBER: _ClassVar[int]
    HEALTH_FIELD_NUMBER: _ClassVar[int]
    LAST_JWKS_REFRESH_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_JWKS_REFRESH_STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    UPDATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    DELETED_AT_FIELD_NUMBER: _ClassVar[int]
    provider_id: str
    tenant_id: str
    kind: _enums_pb2.IdpKind
    display_name: str
    issuer: str
    entity_id: str
    jwks_url: str
    saml_metadata_url: str
    client_ids_json: str
    audiences_json: str
    claim_mapping_json: str
    group_mapping_json: str
    jit_policy_json: str
    account_linking_policy: str
    enabled: bool
    client_secret: str
    saml_signing_key_pem: str
    saml_idp_certs_json: str
    saml_sso_url: str
    health: _enums_pb2.ProviderHealth
    last_jwks_refresh_at: _timestamp_pb2.Timestamp
    last_jwks_refresh_status: str
    created_by: str
    updated_by: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    deleted_at: _timestamp_pb2.Timestamp
    def __init__(self, provider_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., kind: _Optional[_Union[_enums_pb2.IdpKind, str]] = ..., display_name: _Optional[str] = ..., issuer: _Optional[str] = ..., entity_id: _Optional[str] = ..., jwks_url: _Optional[str] = ..., saml_metadata_url: _Optional[str] = ..., client_ids_json: _Optional[str] = ..., audiences_json: _Optional[str] = ..., claim_mapping_json: _Optional[str] = ..., group_mapping_json: _Optional[str] = ..., jit_policy_json: _Optional[str] = ..., account_linking_policy: _Optional[str] = ..., enabled: bool = ..., client_secret: _Optional[str] = ..., saml_signing_key_pem: _Optional[str] = ..., saml_idp_certs_json: _Optional[str] = ..., saml_sso_url: _Optional[str] = ..., health: _Optional[_Union[_enums_pb2.ProviderHealth, str]] = ..., last_jwks_refresh_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_jwks_refresh_status: _Optional[str] = ..., created_by: _Optional[str] = ..., updated_by: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., deleted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
