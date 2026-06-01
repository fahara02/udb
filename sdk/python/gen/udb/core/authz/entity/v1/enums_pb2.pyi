from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor

class RoleScopeType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ROLE_SCOPE_TYPE_UNSPECIFIED: _ClassVar[RoleScopeType]
    ROLE_SCOPE_TYPE_GLOBAL: _ClassVar[RoleScopeType]
    ROLE_SCOPE_TYPE_TENANT: _ClassVar[RoleScopeType]
    ROLE_SCOPE_TYPE_PROJECT: _ClassVar[RoleScopeType]
    ROLE_SCOPE_TYPE_RESOURCE: _ClassVar[RoleScopeType]
    ROLE_SCOPE_TYPE_EXTERNAL: _ClassVar[RoleScopeType]

class PrincipalKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PRINCIPAL_KIND_UNSPECIFIED: _ClassVar[PrincipalKind]
    PRINCIPAL_KIND_USER: _ClassVar[PrincipalKind]
    PRINCIPAL_KIND_SERVICE_ACCOUNT: _ClassVar[PrincipalKind]
    PRINCIPAL_KIND_WORKLOAD: _ClassVar[PrincipalKind]
    PRINCIPAL_KIND_GROUP: _ClassVar[PrincipalKind]
    PRINCIPAL_KIND_ROLE: _ClassVar[PrincipalKind]
    PRINCIPAL_KIND_EXTERNAL_SUBJECT: _ClassVar[PrincipalKind]

class PolicyEffect(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    POLICY_EFFECT_UNSPECIFIED: _ClassVar[PolicyEffect]
    POLICY_EFFECT_ALLOW: _ClassVar[PolicyEffect]
    POLICY_EFFECT_DENY: _ClassVar[PolicyEffect]

class DecisionSource(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DECISION_SOURCE_UNSPECIFIED: _ClassVar[DecisionSource]
    DECISION_SOURCE_ROLE_POLICY: _ClassVar[DecisionSource]
    DECISION_SOURCE_DIRECT_POLICY: _ClassVar[DecisionSource]
    DECISION_SOURCE_NO_MATCH: _ClassVar[DecisionSource]
ROLE_SCOPE_TYPE_UNSPECIFIED: RoleScopeType
ROLE_SCOPE_TYPE_GLOBAL: RoleScopeType
ROLE_SCOPE_TYPE_TENANT: RoleScopeType
ROLE_SCOPE_TYPE_PROJECT: RoleScopeType
ROLE_SCOPE_TYPE_RESOURCE: RoleScopeType
ROLE_SCOPE_TYPE_EXTERNAL: RoleScopeType
PRINCIPAL_KIND_UNSPECIFIED: PrincipalKind
PRINCIPAL_KIND_USER: PrincipalKind
PRINCIPAL_KIND_SERVICE_ACCOUNT: PrincipalKind
PRINCIPAL_KIND_WORKLOAD: PrincipalKind
PRINCIPAL_KIND_GROUP: PrincipalKind
PRINCIPAL_KIND_ROLE: PrincipalKind
PRINCIPAL_KIND_EXTERNAL_SUBJECT: PrincipalKind
POLICY_EFFECT_UNSPECIFIED: PolicyEffect
POLICY_EFFECT_ALLOW: PolicyEffect
POLICY_EFFECT_DENY: PolicyEffect
DECISION_SOURCE_UNSPECIFIED: DecisionSource
DECISION_SOURCE_ROLE_POLICY: DecisionSource
DECISION_SOURCE_DIRECT_POLICY: DecisionSource
DECISION_SOURCE_NO_MATCH: DecisionSource
