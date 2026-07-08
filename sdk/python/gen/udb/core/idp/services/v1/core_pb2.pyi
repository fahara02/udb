import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import field_mask_pb2 as _field_mask_pb2
from udb.core.idp.entity.v1 import identity_provider_pb2 as _identity_provider_pb2
from udb.core.idp.entity.v1 import external_identity_pb2 as _external_identity_pb2
from udb.core.idp.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import types_pb2 as _types_pb2
from udb.core.common.v1 import domain_types_pb2 as _domain_types_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateProviderRequest(_message.Message):
    __slots__ = ("tenant_id", "kind", "display_name", "issuer", "entity_id", "jwks_url", "saml_metadata_url", "client_ids", "audiences", "claim_mapping_json", "group_mapping_json", "jit_policy_json", "account_linking_policy", "enabled", "client_secret", "saml_signing_key_pem", "created_by", "context")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    ISSUER_FIELD_NUMBER: _ClassVar[int]
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    JWKS_URL_FIELD_NUMBER: _ClassVar[int]
    SAML_METADATA_URL_FIELD_NUMBER: _ClassVar[int]
    CLIENT_IDS_FIELD_NUMBER: _ClassVar[int]
    AUDIENCES_FIELD_NUMBER: _ClassVar[int]
    CLAIM_MAPPING_JSON_FIELD_NUMBER: _ClassVar[int]
    GROUP_MAPPING_JSON_FIELD_NUMBER: _ClassVar[int]
    JIT_POLICY_JSON_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_LINKING_POLICY_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    CLIENT_SECRET_FIELD_NUMBER: _ClassVar[int]
    SAML_SIGNING_KEY_PEM_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    kind: _enums_pb2.IdpKind
    display_name: str
    issuer: str
    entity_id: str
    jwks_url: str
    saml_metadata_url: str
    client_ids: _containers.RepeatedScalarFieldContainer[str]
    audiences: _containers.RepeatedScalarFieldContainer[str]
    claim_mapping_json: str
    group_mapping_json: str
    jit_policy_json: str
    account_linking_policy: str
    enabled: bool
    client_secret: str
    saml_signing_key_pem: str
    created_by: str
    context: _types_pb2.RequestContext
    def __init__(self, tenant_id: _Optional[str] = ..., kind: _Optional[_Union[_enums_pb2.IdpKind, str]] = ..., display_name: _Optional[str] = ..., issuer: _Optional[str] = ..., entity_id: _Optional[str] = ..., jwks_url: _Optional[str] = ..., saml_metadata_url: _Optional[str] = ..., client_ids: _Optional[_Iterable[str]] = ..., audiences: _Optional[_Iterable[str]] = ..., claim_mapping_json: _Optional[str] = ..., group_mapping_json: _Optional[str] = ..., jit_policy_json: _Optional[str] = ..., account_linking_policy: _Optional[str] = ..., enabled: bool = ..., client_secret: _Optional[str] = ..., saml_signing_key_pem: _Optional[str] = ..., created_by: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class CreateProviderResponse(_message.Message):
    __slots__ = ("provider",)
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    provider: _identity_provider_pb2.IdentityProvider
    def __init__(self, provider: _Optional[_Union[_identity_provider_pb2.IdentityProvider, _Mapping]] = ...) -> None: ...

class UpdateProviderRequest(_message.Message):
    __slots__ = ("provider_id", "tenant_id", "display_name", "issuer", "entity_id", "jwks_url", "saml_metadata_url", "client_ids", "audiences", "claim_mapping_json", "group_mapping_json", "jit_policy_json", "account_linking_policy", "client_secret", "saml_signing_key_pem", "updated_by", "context", "update_mask")
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    ISSUER_FIELD_NUMBER: _ClassVar[int]
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    JWKS_URL_FIELD_NUMBER: _ClassVar[int]
    SAML_METADATA_URL_FIELD_NUMBER: _ClassVar[int]
    CLIENT_IDS_FIELD_NUMBER: _ClassVar[int]
    AUDIENCES_FIELD_NUMBER: _ClassVar[int]
    CLAIM_MAPPING_JSON_FIELD_NUMBER: _ClassVar[int]
    GROUP_MAPPING_JSON_FIELD_NUMBER: _ClassVar[int]
    JIT_POLICY_JSON_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_LINKING_POLICY_FIELD_NUMBER: _ClassVar[int]
    CLIENT_SECRET_FIELD_NUMBER: _ClassVar[int]
    SAML_SIGNING_KEY_PEM_FIELD_NUMBER: _ClassVar[int]
    UPDATED_BY_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    UPDATE_MASK_FIELD_NUMBER: _ClassVar[int]
    provider_id: str
    tenant_id: str
    display_name: str
    issuer: str
    entity_id: str
    jwks_url: str
    saml_metadata_url: str
    client_ids: _containers.RepeatedScalarFieldContainer[str]
    audiences: _containers.RepeatedScalarFieldContainer[str]
    claim_mapping_json: str
    group_mapping_json: str
    jit_policy_json: str
    account_linking_policy: str
    client_secret: str
    saml_signing_key_pem: str
    updated_by: str
    context: _types_pb2.RequestContext
    update_mask: _field_mask_pb2.FieldMask
    def __init__(self, provider_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., display_name: _Optional[str] = ..., issuer: _Optional[str] = ..., entity_id: _Optional[str] = ..., jwks_url: _Optional[str] = ..., saml_metadata_url: _Optional[str] = ..., client_ids: _Optional[_Iterable[str]] = ..., audiences: _Optional[_Iterable[str]] = ..., claim_mapping_json: _Optional[str] = ..., group_mapping_json: _Optional[str] = ..., jit_policy_json: _Optional[str] = ..., account_linking_policy: _Optional[str] = ..., client_secret: _Optional[str] = ..., saml_signing_key_pem: _Optional[str] = ..., updated_by: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ..., update_mask: _Optional[_Union[_field_mask_pb2.FieldMask, _Mapping]] = ...) -> None: ...

class UpdateProviderResponse(_message.Message):
    __slots__ = ("provider",)
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    provider: _identity_provider_pb2.IdentityProvider
    def __init__(self, provider: _Optional[_Union[_identity_provider_pb2.IdentityProvider, _Mapping]] = ...) -> None: ...

class DisableProviderRequest(_message.Message):
    __slots__ = ("provider_id", "tenant_id", "updated_by", "context")
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    UPDATED_BY_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    provider_id: str
    tenant_id: str
    updated_by: str
    context: _types_pb2.RequestContext
    def __init__(self, provider_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., updated_by: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class DisableProviderResponse(_message.Message):
    __slots__ = ("provider",)
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    provider: _identity_provider_pb2.IdentityProvider
    def __init__(self, provider: _Optional[_Union[_identity_provider_pb2.IdentityProvider, _Mapping]] = ...) -> None: ...

class GetProviderRequest(_message.Message):
    __slots__ = ("provider_id", "tenant_id")
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    provider_id: str
    tenant_id: str
    def __init__(self, provider_id: _Optional[str] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class GetProviderResponse(_message.Message):
    __slots__ = ("provider",)
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    provider: _identity_provider_pb2.IdentityProvider
    def __init__(self, provider: _Optional[_Union[_identity_provider_pb2.IdentityProvider, _Mapping]] = ...) -> None: ...

class ListProvidersRequest(_message.Message):
    __slots__ = ("tenant_id", "kind", "enabled_only", "page")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    ENABLED_ONLY_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    kind: _enums_pb2.IdpKind
    enabled_only: bool
    page: _dto_pb2.PageRequest
    def __init__(self, tenant_id: _Optional[str] = ..., kind: _Optional[_Union[_enums_pb2.IdpKind, str]] = ..., enabled_only: bool = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListProvidersResponse(_message.Message):
    __slots__ = ("providers", "page")
    PROVIDERS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    providers: _containers.RepeatedCompositeFieldContainer[_identity_provider_pb2.IdentityProvider]
    page: _dto_pb2.PageResponse
    def __init__(self, providers: _Optional[_Iterable[_Union[_identity_provider_pb2.IdentityProvider, _Mapping]]] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class TestProviderDiscoveryRequest(_message.Message):
    __slots__ = ("provider_id", "tenant_id")
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    provider_id: str
    tenant_id: str
    def __init__(self, provider_id: _Optional[str] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class TestProviderDiscoveryResponse(_message.Message):
    __slots__ = ("reachable", "health", "resolved_issuer", "resolved_jwks_url", "key_count", "key_ids", "detail")
    REACHABLE_FIELD_NUMBER: _ClassVar[int]
    HEALTH_FIELD_NUMBER: _ClassVar[int]
    RESOLVED_ISSUER_FIELD_NUMBER: _ClassVar[int]
    RESOLVED_JWKS_URL_FIELD_NUMBER: _ClassVar[int]
    KEY_COUNT_FIELD_NUMBER: _ClassVar[int]
    KEY_IDS_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    reachable: bool
    health: _enums_pb2.ProviderHealth
    resolved_issuer: str
    resolved_jwks_url: str
    key_count: int
    key_ids: _containers.RepeatedScalarFieldContainer[str]
    detail: str
    def __init__(self, reachable: bool = ..., health: _Optional[_Union[_enums_pb2.ProviderHealth, str]] = ..., resolved_issuer: _Optional[str] = ..., resolved_jwks_url: _Optional[str] = ..., key_count: _Optional[int] = ..., key_ids: _Optional[_Iterable[str]] = ..., detail: _Optional[str] = ...) -> None: ...

class ForceJwksRefreshRequest(_message.Message):
    __slots__ = ("provider_id", "tenant_id")
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    provider_id: str
    tenant_id: str
    def __init__(self, provider_id: _Optional[str] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class ForceJwksRefreshResponse(_message.Message):
    __slots__ = ("ok", "key_count", "key_ids", "refreshed_at", "status")
    OK_FIELD_NUMBER: _ClassVar[int]
    KEY_COUNT_FIELD_NUMBER: _ClassVar[int]
    KEY_IDS_FIELD_NUMBER: _ClassVar[int]
    REFRESHED_AT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    key_count: int
    key_ids: _containers.RepeatedScalarFieldContainer[str]
    refreshed_at: _timestamp_pb2.Timestamp
    status: str
    def __init__(self, ok: bool = ..., key_count: _Optional[int] = ..., key_ids: _Optional[_Iterable[str]] = ..., refreshed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., status: _Optional[str] = ...) -> None: ...

class PreviewClaimMappingRequest(_message.Message):
    __slots__ = ("provider_id", "tenant_id", "claims_json", "claim_mapping_json")
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CLAIMS_JSON_FIELD_NUMBER: _ClassVar[int]
    CLAIM_MAPPING_JSON_FIELD_NUMBER: _ClassVar[int]
    provider_id: str
    tenant_id: str
    claims_json: str
    claim_mapping_json: str
    def __init__(self, provider_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., claims_json: _Optional[str] = ..., claim_mapping_json: _Optional[str] = ...) -> None: ...

class PreviewClaimMappingResponse(_message.Message):
    __slots__ = ("subject", "email", "email_verified", "display_name", "groups", "assurance", "mapped_principal_json")
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    EMAIL_VERIFIED_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    GROUPS_FIELD_NUMBER: _ClassVar[int]
    ASSURANCE_FIELD_NUMBER: _ClassVar[int]
    MAPPED_PRINCIPAL_JSON_FIELD_NUMBER: _ClassVar[int]
    subject: str
    email: str
    email_verified: bool
    display_name: str
    groups: _containers.RepeatedScalarFieldContainer[str]
    assurance: _enums_pb2.AssuranceLevel
    mapped_principal_json: str
    def __init__(self, subject: _Optional[str] = ..., email: _Optional[str] = ..., email_verified: bool = ..., display_name: _Optional[str] = ..., groups: _Optional[_Iterable[str]] = ..., assurance: _Optional[_Union[_enums_pb2.AssuranceLevel, str]] = ..., mapped_principal_json: _Optional[str] = ...) -> None: ...

class PreviewGroupMappingRequest(_message.Message):
    __slots__ = ("provider_id", "tenant_id", "groups", "group_mapping_json")
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    GROUPS_FIELD_NUMBER: _ClassVar[int]
    GROUP_MAPPING_JSON_FIELD_NUMBER: _ClassVar[int]
    provider_id: str
    tenant_id: str
    groups: _containers.RepeatedScalarFieldContainer[str]
    group_mapping_json: str
    def __init__(self, provider_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., groups: _Optional[_Iterable[str]] = ..., group_mapping_json: _Optional[str] = ...) -> None: ...

class PreviewGroupMappingResponse(_message.Message):
    __slots__ = ("roles", "unmapped_groups")
    ROLES_FIELD_NUMBER: _ClassVar[int]
    UNMAPPED_GROUPS_FIELD_NUMBER: _ClassVar[int]
    roles: _containers.RepeatedScalarFieldContainer[str]
    unmapped_groups: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, roles: _Optional[_Iterable[str]] = ..., unmapped_groups: _Optional[_Iterable[str]] = ...) -> None: ...

class ListExternalIdentitiesRequest(_message.Message):
    __slots__ = ("tenant_id", "provider_id", "user_id", "page")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    provider_id: str
    user_id: str
    page: _dto_pb2.PageRequest
    def __init__(self, tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., user_id: _Optional[str] = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListExternalIdentitiesResponse(_message.Message):
    __slots__ = ("identities", "page")
    IDENTITIES_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    identities: _containers.RepeatedCompositeFieldContainer[_external_identity_pb2.ExternalIdentity]
    page: _dto_pb2.PageResponse
    def __init__(self, identities: _Optional[_Iterable[_Union[_external_identity_pb2.ExternalIdentity, _Mapping]]] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class LinkIdentityRequest(_message.Message):
    __slots__ = ("tenant_id", "provider_id", "subject", "user_id", "email", "email_verified", "context")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    EMAIL_VERIFIED_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    provider_id: str
    subject: str
    user_id: str
    email: str
    email_verified: bool
    context: _types_pb2.RequestContext
    def __init__(self, tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., subject: _Optional[str] = ..., user_id: _Optional[str] = ..., email: _Optional[str] = ..., email_verified: bool = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class LinkIdentityResponse(_message.Message):
    __slots__ = ("identity",)
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    identity: _external_identity_pb2.ExternalIdentity
    def __init__(self, identity: _Optional[_Union[_external_identity_pb2.ExternalIdentity, _Mapping]] = ...) -> None: ...

class UnlinkIdentityRequest(_message.Message):
    __slots__ = ("tenant_id", "external_identity_id", "context")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_IDENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    external_identity_id: str
    context: _types_pb2.RequestContext
    def __init__(self, tenant_id: _Optional[str] = ..., external_identity_id: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class UnlinkIdentityResponse(_message.Message):
    __slots__ = ("unlinked",)
    UNLINKED_FIELD_NUMBER: _ClassVar[int]
    unlinked: bool
    def __init__(self, unlinked: bool = ...) -> None: ...

class ImportSamlMetadataRequest(_message.Message):
    __slots__ = ("provider_id", "tenant_id", "metadata_xml", "updated_by", "context")
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    METADATA_XML_FIELD_NUMBER: _ClassVar[int]
    UPDATED_BY_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    provider_id: str
    tenant_id: str
    metadata_xml: str
    updated_by: str
    context: _types_pb2.RequestContext
    def __init__(self, provider_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., metadata_xml: _Optional[str] = ..., updated_by: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class ImportSamlMetadataResponse(_message.Message):
    __slots__ = ("entity_id", "sso_url", "cert_count", "provider")
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    SSO_URL_FIELD_NUMBER: _ClassVar[int]
    CERT_COUNT_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    entity_id: str
    sso_url: str
    cert_count: int
    provider: _identity_provider_pb2.IdentityProvider
    def __init__(self, entity_id: _Optional[str] = ..., sso_url: _Optional[str] = ..., cert_count: _Optional[int] = ..., provider: _Optional[_Union[_identity_provider_pb2.IdentityProvider, _Mapping]] = ...) -> None: ...

class StartSamlLoginRequest(_message.Message):
    __slots__ = ("provider_id", "tenant_id", "relay_state")
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    RELAY_STATE_FIELD_NUMBER: _ClassVar[int]
    provider_id: str
    tenant_id: str
    relay_state: str
    def __init__(self, provider_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., relay_state: _Optional[str] = ...) -> None: ...

class StartSamlLoginResponse(_message.Message):
    __slots__ = ("redirect_url", "saml_request", "request_id", "signed")
    REDIRECT_URL_FIELD_NUMBER: _ClassVar[int]
    SAML_REQUEST_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    SIGNED_FIELD_NUMBER: _ClassVar[int]
    redirect_url: str
    saml_request: str
    request_id: str
    signed: bool
    def __init__(self, redirect_url: _Optional[str] = ..., saml_request: _Optional[str] = ..., request_id: _Optional[str] = ..., signed: bool = ...) -> None: ...

class SamlAcsRequest(_message.Message):
    __slots__ = ("provider_id", "tenant_id", "saml_response", "relay_state", "context")
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SAML_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    RELAY_STATE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    provider_id: str
    tenant_id: str
    saml_response: str
    relay_state: str
    context: _types_pb2.RequestContext
    def __init__(self, provider_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., saml_response: _Optional[str] = ..., relay_state: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class SamlAcsResponse(_message.Message):
    __slots__ = ("authenticated", "subject", "user_id", "email", "email_verified", "groups", "roles", "assurance", "signature_verified", "detail", "attributes_json")
    AUTHENTICATED_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    EMAIL_VERIFIED_FIELD_NUMBER: _ClassVar[int]
    GROUPS_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    ASSURANCE_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_VERIFIED_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_JSON_FIELD_NUMBER: _ClassVar[int]
    authenticated: bool
    subject: str
    user_id: str
    email: str
    email_verified: bool
    groups: _containers.RepeatedScalarFieldContainer[str]
    roles: _containers.RepeatedScalarFieldContainer[str]
    assurance: _enums_pb2.AssuranceLevel
    signature_verified: bool
    detail: str
    attributes_json: str
    def __init__(self, authenticated: bool = ..., subject: _Optional[str] = ..., user_id: _Optional[str] = ..., email: _Optional[str] = ..., email_verified: bool = ..., groups: _Optional[_Iterable[str]] = ..., roles: _Optional[_Iterable[str]] = ..., assurance: _Optional[_Union[_enums_pb2.AssuranceLevel, str]] = ..., signature_verified: bool = ..., detail: _Optional[str] = ..., attributes_json: _Optional[str] = ...) -> None: ...

class ResolveExternalIdentityRequest(_message.Message):
    __slots__ = ("provider_id", "tenant_id", "claims_json")
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CLAIMS_JSON_FIELD_NUMBER: _ClassVar[int]
    provider_id: str
    tenant_id: str
    claims_json: str
    def __init__(self, provider_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., claims_json: _Optional[str] = ...) -> None: ...

class ResolveExternalIdentityResponse(_message.Message):
    __slots__ = ("user_id", "subject", "email", "provisioned", "linked", "roles", "assurance", "detail")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    PROVISIONED_FIELD_NUMBER: _ClassVar[int]
    LINKED_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    ASSURANCE_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    subject: str
    email: str
    provisioned: bool
    linked: bool
    roles: _containers.RepeatedScalarFieldContainer[str]
    assurance: _enums_pb2.AssuranceLevel
    detail: str
    def __init__(self, user_id: _Optional[str] = ..., subject: _Optional[str] = ..., email: _Optional[str] = ..., provisioned: bool = ..., linked: bool = ..., roles: _Optional[_Iterable[str]] = ..., assurance: _Optional[_Union[_enums_pb2.AssuranceLevel, str]] = ..., detail: _Optional[str] = ...) -> None: ...

class ScimUser(_message.Message):
    __slots__ = ("id", "user_name", "display_name", "email", "active", "groups", "raw_json")
    ID_FIELD_NUMBER: _ClassVar[int]
    USER_NAME_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    GROUPS_FIELD_NUMBER: _ClassVar[int]
    RAW_JSON_FIELD_NUMBER: _ClassVar[int]
    id: str
    user_name: str
    display_name: str
    email: str
    active: bool
    groups: _containers.RepeatedScalarFieldContainer[str]
    raw_json: str
    def __init__(self, id: _Optional[str] = ..., user_name: _Optional[str] = ..., display_name: _Optional[str] = ..., email: _Optional[str] = ..., active: bool = ..., groups: _Optional[_Iterable[str]] = ..., raw_json: _Optional[str] = ...) -> None: ...

class ScimGroup(_message.Message):
    __slots__ = ("id", "display_name", "members", "raw_json")
    ID_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    RAW_JSON_FIELD_NUMBER: _ClassVar[int]
    id: str
    display_name: str
    members: _containers.RepeatedScalarFieldContainer[str]
    raw_json: str
    def __init__(self, id: _Optional[str] = ..., display_name: _Optional[str] = ..., members: _Optional[_Iterable[str]] = ..., raw_json: _Optional[str] = ...) -> None: ...

class ScimCreateUserRequest(_message.Message):
    __slots__ = ("tenant_id", "provider_id", "scim_user_json", "context")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    SCIM_USER_JSON_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    provider_id: str
    scim_user_json: str
    context: _types_pb2.RequestContext
    def __init__(self, tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., scim_user_json: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class ScimCreateUserResponse(_message.Message):
    __slots__ = ("user",)
    USER_FIELD_NUMBER: _ClassVar[int]
    user: ScimUser
    def __init__(self, user: _Optional[_Union[ScimUser, _Mapping]] = ...) -> None: ...

class ScimGetUserRequest(_message.Message):
    __slots__ = ("tenant_id", "provider_id", "scim_user_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    SCIM_USER_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    provider_id: str
    scim_user_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., scim_user_id: _Optional[str] = ...) -> None: ...

class ScimGetUserResponse(_message.Message):
    __slots__ = ("user",)
    USER_FIELD_NUMBER: _ClassVar[int]
    user: ScimUser
    def __init__(self, user: _Optional[_Union[ScimUser, _Mapping]] = ...) -> None: ...

class ScimListUsersRequest(_message.Message):
    __slots__ = ("tenant_id", "provider_id", "filter", "page")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    provider_id: str
    filter: str
    page: _dto_pb2.PageRequest
    def __init__(self, tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., filter: _Optional[str] = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ScimListUsersResponse(_message.Message):
    __slots__ = ("users", "page")
    USERS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    users: _containers.RepeatedCompositeFieldContainer[ScimUser]
    page: _dto_pb2.PageResponse
    def __init__(self, users: _Optional[_Iterable[_Union[ScimUser, _Mapping]]] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class ScimReplaceUserRequest(_message.Message):
    __slots__ = ("tenant_id", "provider_id", "scim_user_id", "scim_user_json", "context")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    SCIM_USER_ID_FIELD_NUMBER: _ClassVar[int]
    SCIM_USER_JSON_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    provider_id: str
    scim_user_id: str
    scim_user_json: str
    context: _types_pb2.RequestContext
    def __init__(self, tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., scim_user_id: _Optional[str] = ..., scim_user_json: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class ScimReplaceUserResponse(_message.Message):
    __slots__ = ("user",)
    USER_FIELD_NUMBER: _ClassVar[int]
    user: ScimUser
    def __init__(self, user: _Optional[_Union[ScimUser, _Mapping]] = ...) -> None: ...

class ScimPatchOp(_message.Message):
    __slots__ = ("op", "path", "value_json")
    OP_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    VALUE_JSON_FIELD_NUMBER: _ClassVar[int]
    op: str
    path: str
    value_json: str
    def __init__(self, op: _Optional[str] = ..., path: _Optional[str] = ..., value_json: _Optional[str] = ...) -> None: ...

class ScimPatchUserRequest(_message.Message):
    __slots__ = ("tenant_id", "provider_id", "scim_user_id", "operations", "context")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    SCIM_USER_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATIONS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    provider_id: str
    scim_user_id: str
    operations: _containers.RepeatedCompositeFieldContainer[ScimPatchOp]
    context: _types_pb2.RequestContext
    def __init__(self, tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., scim_user_id: _Optional[str] = ..., operations: _Optional[_Iterable[_Union[ScimPatchOp, _Mapping]]] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class ScimPatchUserResponse(_message.Message):
    __slots__ = ("user",)
    USER_FIELD_NUMBER: _ClassVar[int]
    user: ScimUser
    def __init__(self, user: _Optional[_Union[ScimUser, _Mapping]] = ...) -> None: ...

class ScimDeleteUserRequest(_message.Message):
    __slots__ = ("tenant_id", "provider_id", "scim_user_id", "context")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    SCIM_USER_ID_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    provider_id: str
    scim_user_id: str
    context: _types_pb2.RequestContext
    def __init__(self, tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., scim_user_id: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class ScimDeleteUserResponse(_message.Message):
    __slots__ = ("deactivated",)
    DEACTIVATED_FIELD_NUMBER: _ClassVar[int]
    deactivated: bool
    def __init__(self, deactivated: bool = ...) -> None: ...

class ScimCreateGroupRequest(_message.Message):
    __slots__ = ("tenant_id", "provider_id", "scim_group_json", "context")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    SCIM_GROUP_JSON_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    provider_id: str
    scim_group_json: str
    context: _types_pb2.RequestContext
    def __init__(self, tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., scim_group_json: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class ScimCreateGroupResponse(_message.Message):
    __slots__ = ("group",)
    GROUP_FIELD_NUMBER: _ClassVar[int]
    group: ScimGroup
    def __init__(self, group: _Optional[_Union[ScimGroup, _Mapping]] = ...) -> None: ...

class ScimGetGroupRequest(_message.Message):
    __slots__ = ("tenant_id", "provider_id", "scim_group_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    SCIM_GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    provider_id: str
    scim_group_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., scim_group_id: _Optional[str] = ...) -> None: ...

class ScimGetGroupResponse(_message.Message):
    __slots__ = ("group",)
    GROUP_FIELD_NUMBER: _ClassVar[int]
    group: ScimGroup
    def __init__(self, group: _Optional[_Union[ScimGroup, _Mapping]] = ...) -> None: ...

class ScimListGroupsRequest(_message.Message):
    __slots__ = ("tenant_id", "provider_id", "filter", "page")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    provider_id: str
    filter: str
    page: _dto_pb2.PageRequest
    def __init__(self, tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., filter: _Optional[str] = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ScimListGroupsResponse(_message.Message):
    __slots__ = ("groups", "page")
    GROUPS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    groups: _containers.RepeatedCompositeFieldContainer[ScimGroup]
    page: _dto_pb2.PageResponse
    def __init__(self, groups: _Optional[_Iterable[_Union[ScimGroup, _Mapping]]] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class ScimPatchGroupRequest(_message.Message):
    __slots__ = ("tenant_id", "provider_id", "scim_group_id", "operations", "context")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    SCIM_GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATIONS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    provider_id: str
    scim_group_id: str
    operations: _containers.RepeatedCompositeFieldContainer[ScimPatchOp]
    context: _types_pb2.RequestContext
    def __init__(self, tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., scim_group_id: _Optional[str] = ..., operations: _Optional[_Iterable[_Union[ScimPatchOp, _Mapping]]] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class ScimPatchGroupResponse(_message.Message):
    __slots__ = ("group", "granted_roles")
    GROUP_FIELD_NUMBER: _ClassVar[int]
    GRANTED_ROLES_FIELD_NUMBER: _ClassVar[int]
    group: ScimGroup
    granted_roles: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, group: _Optional[_Union[ScimGroup, _Mapping]] = ..., granted_roles: _Optional[_Iterable[str]] = ...) -> None: ...

class ScimDeleteGroupRequest(_message.Message):
    __slots__ = ("tenant_id", "provider_id", "scim_group_id", "context")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    SCIM_GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    provider_id: str
    scim_group_id: str
    context: _types_pb2.RequestContext
    def __init__(self, tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., scim_group_id: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class ScimDeleteGroupResponse(_message.Message):
    __slots__ = ("deleted",)
    DELETED_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    def __init__(self, deleted: bool = ...) -> None: ...
