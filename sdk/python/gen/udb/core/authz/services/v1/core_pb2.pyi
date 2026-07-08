import datetime

from google.api import field_behavior_pb2 as _field_behavior_pb2
from google.protobuf import field_mask_pb2 as _field_mask_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.authz.entity.v1 import access_decision_audit_pb2 as _access_decision_audit_pb2
from udb.core.authz.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.authz.entity.v1 import policy_rule_pb2 as _policy_rule_pb2
from udb.core.authz.entity.v1 import role_pb2 as _role_pb2
from udb.core.authz.entity.v1 import user_role_pb2 as _user_role_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Principal(_message.Message):
    __slots__ = ("principal_id", "subject", "user_id", "service_identity", "tenant_id", "project_id", "scopes", "roles", "provider_id", "auth_method", "expires_at_unix", "account_kind", "domain", "attributes")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    SERVICE_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    AUTH_METHOD_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_KIND_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    principal_id: str
    subject: str
    user_id: str
    service_identity: str
    tenant_id: str
    project_id: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    roles: _containers.RepeatedScalarFieldContainer[str]
    provider_id: str
    auth_method: str
    expires_at_unix: int
    account_kind: str
    domain: str
    attributes: _containers.ScalarMap[str, str]
    def __init__(self, principal_id: _Optional[str] = ..., subject: _Optional[str] = ..., user_id: _Optional[str] = ..., service_identity: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ..., roles: _Optional[_Iterable[str]] = ..., provider_id: _Optional[str] = ..., auth_method: _Optional[str] = ..., expires_at_unix: _Optional[int] = ..., account_kind: _Optional[str] = ..., domain: _Optional[str] = ..., attributes: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ResourceRef(_message.Message):
    __slots__ = ("resource_type", "resource_name", "message_type", "schema", "table", "backend", "instance", "resource_id", "collection", "bucket", "path", "service", "api", "tenant_id", "project_id", "attributes")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    API_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    resource_type: str
    resource_name: str
    message_type: str
    schema: str
    table: str
    backend: str
    instance: str
    resource_id: str
    collection: str
    bucket: str
    path: str
    service: str
    api: str
    tenant_id: str
    project_id: str
    attributes: _containers.ScalarMap[str, str]
    def __init__(self, resource_type: _Optional[str] = ..., resource_name: _Optional[str] = ..., message_type: _Optional[str] = ..., schema: _Optional[str] = ..., table: _Optional[str] = ..., backend: _Optional[str] = ..., instance: _Optional[str] = ..., resource_id: _Optional[str] = ..., collection: _Optional[str] = ..., bucket: _Optional[str] = ..., path: _Optional[str] = ..., service: _Optional[str] = ..., api: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., attributes: _Optional[_Mapping[str, str]] = ...) -> None: ...

class Decision(_message.Message):
    __slots__ = ("decision_id", "allowed", "effect", "deny_reason", "matched_policy_ids", "required_scopes", "policy_version", "relationship_version", "cache_ttl_seconds", "audit_required")
    DECISION_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    DENY_REASON_FIELD_NUMBER: _ClassVar[int]
    MATCHED_POLICY_IDS_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_SCOPES_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    RELATIONSHIP_VERSION_FIELD_NUMBER: _ClassVar[int]
    CACHE_TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    AUDIT_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    decision_id: str
    allowed: bool
    effect: str
    deny_reason: str
    matched_policy_ids: _containers.RepeatedScalarFieldContainer[str]
    required_scopes: _containers.RepeatedScalarFieldContainer[str]
    policy_version: str
    relationship_version: str
    cache_ttl_seconds: int
    audit_required: bool
    def __init__(self, decision_id: _Optional[str] = ..., allowed: bool = ..., effect: _Optional[str] = ..., deny_reason: _Optional[str] = ..., matched_policy_ids: _Optional[_Iterable[str]] = ..., required_scopes: _Optional[_Iterable[str]] = ..., policy_version: _Optional[str] = ..., relationship_version: _Optional[str] = ..., cache_ttl_seconds: _Optional[int] = ..., audit_required: bool = ...) -> None: ...

class AccessContext(_message.Message):
    __slots__ = ("ip_address", "user_agent", "device_id", "token_id", "session_id", "attributes")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    USER_AGENT_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    TOKEN_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    ip_address: str
    user_agent: str
    device_id: str
    token_id: str
    session_id: str
    attributes: _containers.ScalarMap[str, str]
    def __init__(self, ip_address: _Optional[str] = ..., user_agent: _Optional[str] = ..., device_id: _Optional[str] = ..., token_id: _Optional[str] = ..., session_id: _Optional[str] = ..., attributes: _Optional[_Mapping[str, str]] = ...) -> None: ...

class CheckAccessRequest(_message.Message):
    __slots__ = ("user_id", "domain", "object", "action", "context", "principal", "resource", "purpose", "tenant_id", "project_id", "attributes")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    PURPOSE_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    domain: str
    object: str
    action: str
    context: AccessContext
    principal: Principal
    resource: ResourceRef
    purpose: str
    tenant_id: str
    project_id: str
    attributes: _containers.ScalarMap[str, str]
    def __init__(self, user_id: _Optional[str] = ..., domain: _Optional[str] = ..., object: _Optional[str] = ..., action: _Optional[str] = ..., context: _Optional[_Union[AccessContext, _Mapping]] = ..., principal: _Optional[_Union[Principal, _Mapping]] = ..., resource: _Optional[_Union[ResourceRef, _Mapping]] = ..., purpose: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., attributes: _Optional[_Mapping[str, str]] = ...) -> None: ...

class CheckAccessResponse(_message.Message):
    __slots__ = ("allowed", "effect", "matched_rule", "reason", "decision")
    ALLOWED_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    MATCHED_RULE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    DECISION_FIELD_NUMBER: _ClassVar[int]
    allowed: bool
    effect: _enums_pb2.PolicyEffect
    matched_rule: str
    reason: str
    decision: Decision
    def __init__(self, allowed: bool = ..., effect: _Optional[_Union[_enums_pb2.PolicyEffect, str]] = ..., matched_rule: _Optional[str] = ..., reason: _Optional[str] = ..., decision: _Optional[_Union[Decision, _Mapping]] = ...) -> None: ...

class AuthzRequest(_message.Message):
    __slots__ = ("principal", "session_id", "tenant_id", "project_id", "resource", "action", "purpose", "attributes", "context", "requested_scopes", "domain")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    PRINCIPAL_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    PURPOSE_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_SCOPES_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    principal: Principal
    session_id: str
    tenant_id: str
    project_id: str
    resource: ResourceRef
    action: str
    purpose: str
    attributes: _containers.ScalarMap[str, str]
    context: AccessContext
    requested_scopes: _containers.RepeatedScalarFieldContainer[str]
    domain: str
    def __init__(self, principal: _Optional[_Union[Principal, _Mapping]] = ..., session_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., resource: _Optional[_Union[ResourceRef, _Mapping]] = ..., action: _Optional[str] = ..., purpose: _Optional[str] = ..., attributes: _Optional[_Mapping[str, str]] = ..., context: _Optional[_Union[AccessContext, _Mapping]] = ..., requested_scopes: _Optional[_Iterable[str]] = ..., domain: _Optional[str] = ...) -> None: ...

class AuthzResponse(_message.Message):
    __slots__ = ("decision",)
    DECISION_FIELD_NUMBER: _ClassVar[int]
    decision: Decision
    def __init__(self, decision: _Optional[_Union[Decision, _Mapping]] = ...) -> None: ...

class CreateRoleRequest(_message.Message):
    __slots__ = ("name", "description", "created_by", "role_code", "domain", "tenant_id", "project_id", "scope_type", "access_surface", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    ROLE_CODE_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPE_TYPE_FIELD_NUMBER: _ClassVar[int]
    ACCESS_SURFACE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    created_by: str
    role_code: str
    domain: str
    tenant_id: str
    project_id: str
    scope_type: _enums_pb2.RoleScopeType
    access_surface: str
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., created_by: _Optional[str] = ..., role_code: _Optional[str] = ..., domain: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., scope_type: _Optional[_Union[_enums_pb2.RoleScopeType, str]] = ..., access_surface: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class CreateRoleResponse(_message.Message):
    __slots__ = ("role",)
    ROLE_FIELD_NUMBER: _ClassVar[int]
    role: _role_pb2.Role
    def __init__(self, role: _Optional[_Union[_role_pb2.Role, _Mapping]] = ...) -> None: ...

class AssignRoleRequest(_message.Message):
    __slots__ = ("user_id", "role_id", "domain", "assigned_by", "expires_at", "principal_id", "principal_kind", "tenant_id", "project_id")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    ASSIGNED_BY_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_KIND_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    role_id: str
    domain: str
    assigned_by: str
    expires_at: _timestamp_pb2.Timestamp
    principal_id: str
    principal_kind: _enums_pb2.PrincipalKind
    tenant_id: str
    project_id: str
    def __init__(self, user_id: _Optional[str] = ..., role_id: _Optional[str] = ..., domain: _Optional[str] = ..., assigned_by: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., principal_id: _Optional[str] = ..., principal_kind: _Optional[_Union[_enums_pb2.PrincipalKind, str]] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ...) -> None: ...

class AssignRoleResponse(_message.Message):
    __slots__ = ("user_role",)
    USER_ROLE_FIELD_NUMBER: _ClassVar[int]
    user_role: _user_role_pb2.UserRole
    def __init__(self, user_role: _Optional[_Union[_user_role_pb2.UserRole, _Mapping]] = ...) -> None: ...

class CreatePolicyRuleRequest(_message.Message):
    __slots__ = ("subject", "domain", "object", "action", "effect", "condition", "description", "created_by", "tenant_id", "project_id", "resource_type", "attributes")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    subject: str
    domain: str
    object: str
    action: str
    effect: _enums_pb2.PolicyEffect
    condition: str
    description: str
    created_by: str
    tenant_id: str
    project_id: str
    resource_type: str
    attributes: _containers.ScalarMap[str, str]
    def __init__(self, subject: _Optional[str] = ..., domain: _Optional[str] = ..., object: _Optional[str] = ..., action: _Optional[str] = ..., effect: _Optional[_Union[_enums_pb2.PolicyEffect, str]] = ..., condition: _Optional[str] = ..., description: _Optional[str] = ..., created_by: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., resource_type: _Optional[str] = ..., attributes: _Optional[_Mapping[str, str]] = ...) -> None: ...

class CreatePolicyRuleResponse(_message.Message):
    __slots__ = ("policy",)
    POLICY_FIELD_NUMBER: _ClassVar[int]
    policy: _policy_rule_pb2.PolicyRule
    def __init__(self, policy: _Optional[_Union[_policy_rule_pb2.PolicyRule, _Mapping]] = ...) -> None: ...

class ListUserPermissionsRequest(_message.Message):
    __slots__ = ("user_id", "domain", "page_size", "page_token")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    domain: str
    page_size: int
    page_token: str
    def __init__(self, user_id: _Optional[str] = ..., domain: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class EffectivePermission(_message.Message):
    __slots__ = ("object", "action", "via_role", "resource_type", "domain")
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    VIA_ROLE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    object: str
    action: str
    via_role: str
    resource_type: str
    domain: str
    def __init__(self, object: _Optional[str] = ..., action: _Optional[str] = ..., via_role: _Optional[str] = ..., resource_type: _Optional[str] = ..., domain: _Optional[str] = ...) -> None: ...

class ListUserPermissionsResponse(_message.Message):
    __slots__ = ("permissions", "next_page_token")
    PERMISSIONS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    permissions: _containers.RepeatedCompositeFieldContainer[EffectivePermission]
    next_page_token: str
    def __init__(self, permissions: _Optional[_Iterable[_Union[EffectivePermission, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class ListAccessDecisionAuditsRequest(_message.Message):
    __slots__ = ("user_id", "domain", "correlation_id", "page")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    domain: str
    correlation_id: str
    page: _dto_pb2.PageRequest
    def __init__(self, user_id: _Optional[str] = ..., domain: _Optional[str] = ..., correlation_id: _Optional[str] = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListAccessDecisionAuditsResponse(_message.Message):
    __slots__ = ("audits", "page")
    AUDITS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    audits: _containers.RepeatedCompositeFieldContainer[_access_decision_audit_pb2.AccessDecisionAudit]
    page: _dto_pb2.PageResponse
    def __init__(self, audits: _Optional[_Iterable[_Union[_access_decision_audit_pb2.AccessDecisionAudit, _Mapping]]] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class RevokeRoleRequest(_message.Message):
    __slots__ = ("user_id", "user_role_id", "reason", "revoked_by")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    REVOKED_BY_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    user_role_id: str
    reason: str
    revoked_by: str
    def __init__(self, user_id: _Optional[str] = ..., user_role_id: _Optional[str] = ..., reason: _Optional[str] = ..., revoked_by: _Optional[str] = ...) -> None: ...

class RevokeRoleResponse(_message.Message):
    __slots__ = ("revoked",)
    REVOKED_FIELD_NUMBER: _ClassVar[int]
    revoked: bool
    def __init__(self, revoked: bool = ...) -> None: ...

class ListUserRolesRequest(_message.Message):
    __slots__ = ("user_id", "domain", "active_only", "page_size", "page_token")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_ONLY_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    domain: str
    active_only: bool
    page_size: int
    page_token: str
    def __init__(self, user_id: _Optional[str] = ..., domain: _Optional[str] = ..., active_only: bool = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListUserRolesResponse(_message.Message):
    __slots__ = ("user_roles", "next_page_token")
    USER_ROLES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    user_roles: _containers.RepeatedCompositeFieldContainer[_user_role_pb2.UserRole]
    next_page_token: str
    def __init__(self, user_roles: _Optional[_Iterable[_Union[_user_role_pb2.UserRole, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class GetRoleRequest(_message.Message):
    __slots__ = ("role_id", "role_code", "domain")
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_CODE_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    role_id: str
    role_code: str
    domain: str
    def __init__(self, role_id: _Optional[str] = ..., role_code: _Optional[str] = ..., domain: _Optional[str] = ...) -> None: ...

class GetRoleResponse(_message.Message):
    __slots__ = ("role",)
    ROLE_FIELD_NUMBER: _ClassVar[int]
    role: _role_pb2.Role
    def __init__(self, role: _Optional[_Union[_role_pb2.Role, _Mapping]] = ...) -> None: ...

class ListRolesRequest(_message.Message):
    __slots__ = ("domain", "active_only", "page")
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_ONLY_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    domain: str
    active_only: bool
    page: _dto_pb2.PageRequest
    def __init__(self, domain: _Optional[str] = ..., active_only: bool = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListRolesResponse(_message.Message):
    __slots__ = ("roles", "page")
    ROLES_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    roles: _containers.RepeatedCompositeFieldContainer[_role_pb2.Role]
    page: _dto_pb2.PageResponse
    def __init__(self, roles: _Optional[_Iterable[_Union[_role_pb2.Role, _Mapping]]] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class PermissionCheck(_message.Message):
    __slots__ = ("object", "action")
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    object: str
    action: str
    def __init__(self, object: _Optional[str] = ..., action: _Optional[str] = ...) -> None: ...

class BatchCheckPermissionsRequest(_message.Message):
    __slots__ = ("user_id", "domain", "checks", "context")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    CHECKS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    domain: str
    checks: _containers.RepeatedCompositeFieldContainer[PermissionCheck]
    context: AccessContext
    def __init__(self, user_id: _Optional[str] = ..., domain: _Optional[str] = ..., checks: _Optional[_Iterable[_Union[PermissionCheck, _Mapping]]] = ..., context: _Optional[_Union[AccessContext, _Mapping]] = ...) -> None: ...

class BatchCheckPermissionsResponse(_message.Message):
    __slots__ = ("results",)
    class ResultsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bool
        def __init__(self, key: _Optional[str] = ..., value: bool = ...) -> None: ...
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.ScalarMap[str, bool]
    def __init__(self, results: _Optional[_Mapping[str, bool]] = ...) -> None: ...

class UpdateRoleRequest(_message.Message):
    __slots__ = ("role_id", "name", "description", "is_active", "updated_by", "update_mask")
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    IS_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    UPDATED_BY_FIELD_NUMBER: _ClassVar[int]
    UPDATE_MASK_FIELD_NUMBER: _ClassVar[int]
    role_id: str
    name: str
    description: str
    is_active: bool
    updated_by: str
    update_mask: _field_mask_pb2.FieldMask
    def __init__(self, role_id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., is_active: bool = ..., updated_by: _Optional[str] = ..., update_mask: _Optional[_Union[_field_mask_pb2.FieldMask, _Mapping]] = ...) -> None: ...

class UpdateRoleResponse(_message.Message):
    __slots__ = ("role",)
    ROLE_FIELD_NUMBER: _ClassVar[int]
    role: _role_pb2.Role
    def __init__(self, role: _Optional[_Union[_role_pb2.Role, _Mapping]] = ...) -> None: ...

class DeleteRoleRequest(_message.Message):
    __slots__ = ("role_id", "deleted_by")
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    DELETED_BY_FIELD_NUMBER: _ClassVar[int]
    role_id: str
    deleted_by: str
    def __init__(self, role_id: _Optional[str] = ..., deleted_by: _Optional[str] = ...) -> None: ...

class DeleteRoleResponse(_message.Message):
    __slots__ = ("deleted",)
    DELETED_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    def __init__(self, deleted: bool = ...) -> None: ...

class GetPolicyRuleRequest(_message.Message):
    __slots__ = ("policy_id",)
    POLICY_ID_FIELD_NUMBER: _ClassVar[int]
    policy_id: str
    def __init__(self, policy_id: _Optional[str] = ...) -> None: ...

class GetPolicyRuleResponse(_message.Message):
    __slots__ = ("policy",)
    POLICY_FIELD_NUMBER: _ClassVar[int]
    policy: _policy_rule_pb2.PolicyRule
    def __init__(self, policy: _Optional[_Union[_policy_rule_pb2.PolicyRule, _Mapping]] = ...) -> None: ...

class ListPolicyRulesRequest(_message.Message):
    __slots__ = ("domain", "subject", "object", "active_only", "page")
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_ONLY_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    domain: str
    subject: str
    object: str
    active_only: bool
    page: _dto_pb2.PageRequest
    def __init__(self, domain: _Optional[str] = ..., subject: _Optional[str] = ..., object: _Optional[str] = ..., active_only: bool = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListPolicyRulesResponse(_message.Message):
    __slots__ = ("policies", "page")
    POLICIES_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    policies: _containers.RepeatedCompositeFieldContainer[_policy_rule_pb2.PolicyRule]
    page: _dto_pb2.PageResponse
    def __init__(self, policies: _Optional[_Iterable[_Union[_policy_rule_pb2.PolicyRule, _Mapping]]] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class DeletePolicyRuleRequest(_message.Message):
    __slots__ = ("policy_id", "deleted_by")
    POLICY_ID_FIELD_NUMBER: _ClassVar[int]
    DELETED_BY_FIELD_NUMBER: _ClassVar[int]
    policy_id: str
    deleted_by: str
    def __init__(self, policy_id: _Optional[str] = ..., deleted_by: _Optional[str] = ...) -> None: ...

class DeletePolicyRuleResponse(_message.Message):
    __slots__ = ("deleted",)
    DELETED_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    def __init__(self, deleted: bool = ...) -> None: ...

class AuthMutationResponse(_message.Message):
    __slots__ = ("ok", "message")
    OK_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    message: str
    def __init__(self, ok: bool = ..., message: _Optional[str] = ...) -> None: ...

class RoleBinding(_message.Message):
    __slots__ = ("subject", "role", "tenant", "project", "expires_at_unix", "source")
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    subject: str
    role: str
    tenant: str
    project: str
    expires_at_unix: int
    source: str
    def __init__(self, subject: _Optional[str] = ..., role: _Optional[str] = ..., tenant: _Optional[str] = ..., project: _Optional[str] = ..., expires_at_unix: _Optional[int] = ..., source: _Optional[str] = ...) -> None: ...

class RelationshipTuple(_message.Message):
    __slots__ = ("subject", "relation", "object", "tenant", "project", "version", "expires_at_unix", "source")
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    RELATION_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    subject: str
    relation: str
    object: str
    tenant: str
    project: str
    version: int
    expires_at_unix: int
    source: str
    def __init__(self, subject: _Optional[str] = ..., relation: _Optional[str] = ..., object: _Optional[str] = ..., tenant: _Optional[str] = ..., project: _Optional[str] = ..., version: _Optional[int] = ..., expires_at_unix: _Optional[int] = ..., source: _Optional[str] = ...) -> None: ...

class AuthzPolicyRecord(_message.Message):
    __slots__ = ("id", "priority", "enabled", "effect", "tenant", "project", "subject", "role", "action", "resource", "purpose", "relationship", "conditions", "required_scopes")
    class ConditionsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    PURPOSE_FIELD_NUMBER: _ClassVar[int]
    RELATIONSHIP_FIELD_NUMBER: _ClassVar[int]
    CONDITIONS_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_SCOPES_FIELD_NUMBER: _ClassVar[int]
    id: str
    priority: int
    enabled: bool
    effect: str
    tenant: str
    project: str
    subject: str
    role: str
    action: str
    resource: str
    purpose: str
    relationship: str
    conditions: _containers.ScalarMap[str, str]
    required_scopes: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, id: _Optional[str] = ..., priority: _Optional[int] = ..., enabled: bool = ..., effect: _Optional[str] = ..., tenant: _Optional[str] = ..., project: _Optional[str] = ..., subject: _Optional[str] = ..., role: _Optional[str] = ..., action: _Optional[str] = ..., resource: _Optional[str] = ..., purpose: _Optional[str] = ..., relationship: _Optional[str] = ..., conditions: _Optional[_Mapping[str, str]] = ..., required_scopes: _Optional[_Iterable[str]] = ...) -> None: ...

class PutRoleBindingRequest(_message.Message):
    __slots__ = ("binding",)
    BINDING_FIELD_NUMBER: _ClassVar[int]
    binding: RoleBinding
    def __init__(self, binding: _Optional[_Union[RoleBinding, _Mapping]] = ...) -> None: ...

class PutRelationshipRequest(_message.Message):
    __slots__ = ("tuple",)
    TUPLE_FIELD_NUMBER: _ClassVar[int]
    tuple: RelationshipTuple
    def __init__(self, tuple: _Optional[_Union[RelationshipTuple, _Mapping]] = ...) -> None: ...

class PutAuthzPolicyRequest(_message.Message):
    __slots__ = ("policy",)
    POLICY_FIELD_NUMBER: _ClassVar[int]
    policy: AuthzPolicyRecord
    def __init__(self, policy: _Optional[_Union[AuthzPolicyRecord, _Mapping]] = ...) -> None: ...

class LintAuthzPoliciesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class LintAuthzPoliciesResponse(_message.Message):
    __slots__ = ("findings",)
    FINDINGS_FIELD_NUMBER: _ClassVar[int]
    findings: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, findings: _Optional[_Iterable[str]] = ...) -> None: ...

class NativeAccessRequest(_message.Message):
    __slots__ = ("principal", "session_id", "tenant_id", "project_id", "resource", "action", "purpose", "requested_scopes", "context", "backend", "attributes")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    PRINCIPAL_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    PURPOSE_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_SCOPES_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    principal: Principal
    session_id: str
    tenant_id: str
    project_id: str
    resource: ResourceRef
    action: str
    purpose: str
    requested_scopes: _containers.RepeatedScalarFieldContainer[str]
    context: AccessContext
    backend: str
    attributes: _containers.ScalarMap[str, str]
    def __init__(self, principal: _Optional[_Union[Principal, _Mapping]] = ..., session_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., resource: _Optional[_Union[ResourceRef, _Mapping]] = ..., action: _Optional[str] = ..., purpose: _Optional[str] = ..., requested_scopes: _Optional[_Iterable[str]] = ..., context: _Optional[_Union[AccessContext, _Mapping]] = ..., backend: _Optional[str] = ..., attributes: _Optional[_Mapping[str, str]] = ...) -> None: ...

class NativeAccessGrant(_message.Message):
    __slots__ = ("dsn", "role", "backend", "database", "schema", "session_variables", "expires_at_unix", "ttl_seconds")
    class SessionVariablesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    DSN_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    SESSION_VARIABLES_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    dsn: str
    role: str
    backend: str
    database: str
    schema: str
    session_variables: _containers.ScalarMap[str, str]
    expires_at_unix: int
    ttl_seconds: int
    def __init__(self, dsn: _Optional[str] = ..., role: _Optional[str] = ..., backend: _Optional[str] = ..., database: _Optional[str] = ..., schema: _Optional[str] = ..., session_variables: _Optional[_Mapping[str, str]] = ..., expires_at_unix: _Optional[int] = ..., ttl_seconds: _Optional[int] = ...) -> None: ...

class NativeAccessResponse(_message.Message):
    __slots__ = ("decision", "grant")
    DECISION_FIELD_NUMBER: _ClassVar[int]
    GRANT_FIELD_NUMBER: _ClassVar[int]
    decision: Decision
    grant: NativeAccessGrant
    def __init__(self, decision: _Optional[_Union[Decision, _Mapping]] = ..., grant: _Optional[_Union[NativeAccessGrant, _Mapping]] = ...) -> None: ...

class PolicyBundleRequest(_message.Message):
    __slots__ = ("tenant_id", "project_id", "domain")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    project_id: str
    domain: str
    def __init__(self, tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., domain: _Optional[str] = ...) -> None: ...

class SignedPolicyBundle(_message.Message):
    __slots__ = ("bundle", "signature", "key_id", "algorithm", "policy_version", "relationship_version", "issued_at_unix", "expires_at_unix", "ttl_seconds")
    BUNDLE_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    RELATIONSHIP_VERSION_FIELD_NUMBER: _ClassVar[int]
    ISSUED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    bundle: bytes
    signature: str
    key_id: str
    algorithm: str
    policy_version: str
    relationship_version: str
    issued_at_unix: int
    expires_at_unix: int
    ttl_seconds: int
    def __init__(self, bundle: _Optional[bytes] = ..., signature: _Optional[str] = ..., key_id: _Optional[str] = ..., algorithm: _Optional[str] = ..., policy_version: _Optional[str] = ..., relationship_version: _Optional[str] = ..., issued_at_unix: _Optional[int] = ..., expires_at_unix: _Optional[int] = ..., ttl_seconds: _Optional[int] = ...) -> None: ...

class PolicyBundleResponse(_message.Message):
    __slots__ = ("bundle",)
    BUNDLE_FIELD_NUMBER: _ClassVar[int]
    bundle: SignedPolicyBundle
    def __init__(self, bundle: _Optional[_Union[SignedPolicyBundle, _Mapping]] = ...) -> None: ...
