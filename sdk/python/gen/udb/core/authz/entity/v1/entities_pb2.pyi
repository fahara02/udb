from udb.core.authz.entity.v1 import access_decision_audit_pb2 as _access_decision_audit_pb2
from udb.core.authz.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.authz.entity.v1 import policy_rule_pb2 as _policy_rule_pb2
from udb.core.authz.entity.v1 import policy_tuple_pb2 as _policy_tuple_pb2
from udb.core.authz.entity.v1 import role_pb2 as _role_pb2
from udb.core.authz.entity.v1 import role_permission_pb2 as _role_permission_pb2
from udb.core.authz.entity.v1 import user_role_pb2 as _user_role_pb2
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar
from udb.core.authz.entity.v1.access_decision_audit_pb2 import AccessDecisionAudit as AccessDecisionAudit
from udb.core.authz.entity.v1.enums_pb2 import RoleScopeType as RoleScopeType
from udb.core.authz.entity.v1.enums_pb2 import PrincipalKind as PrincipalKind
from udb.core.authz.entity.v1.enums_pb2 import PolicyEffect as PolicyEffect
from udb.core.authz.entity.v1.enums_pb2 import DecisionSource as DecisionSource
from udb.core.authz.entity.v1.policy_rule_pb2 import PolicyRule as PolicyRule
from udb.core.authz.entity.v1.policy_tuple_pb2 import PolicyTuple as PolicyTuple
from udb.core.authz.entity.v1.role_pb2 import Role as Role
from udb.core.authz.entity.v1.role_permission_pb2 import RolePermission as RolePermission
from udb.core.authz.entity.v1.user_role_pb2 import UserRole as UserRole

DESCRIPTOR: _descriptor.FileDescriptor
ROLE_SCOPE_TYPE_UNSPECIFIED: _enums_pb2.RoleScopeType
ROLE_SCOPE_TYPE_GLOBAL: _enums_pb2.RoleScopeType
ROLE_SCOPE_TYPE_TENANT: _enums_pb2.RoleScopeType
ROLE_SCOPE_TYPE_PROJECT: _enums_pb2.RoleScopeType
ROLE_SCOPE_TYPE_RESOURCE: _enums_pb2.RoleScopeType
ROLE_SCOPE_TYPE_EXTERNAL: _enums_pb2.RoleScopeType
PRINCIPAL_KIND_UNSPECIFIED: _enums_pb2.PrincipalKind
PRINCIPAL_KIND_USER: _enums_pb2.PrincipalKind
PRINCIPAL_KIND_SERVICE_ACCOUNT: _enums_pb2.PrincipalKind
PRINCIPAL_KIND_WORKLOAD: _enums_pb2.PrincipalKind
PRINCIPAL_KIND_GROUP: _enums_pb2.PrincipalKind
PRINCIPAL_KIND_ROLE: _enums_pb2.PrincipalKind
PRINCIPAL_KIND_EXTERNAL_SUBJECT: _enums_pb2.PrincipalKind
POLICY_EFFECT_UNSPECIFIED: _enums_pb2.PolicyEffect
POLICY_EFFECT_ALLOW: _enums_pb2.PolicyEffect
POLICY_EFFECT_DENY: _enums_pb2.PolicyEffect
DECISION_SOURCE_UNSPECIFIED: _enums_pb2.DecisionSource
DECISION_SOURCE_ROLE_POLICY: _enums_pb2.DecisionSource
DECISION_SOURCE_DIRECT_POLICY: _enums_pb2.DecisionSource
DECISION_SOURCE_NO_MATCH: _enums_pb2.DecisionSource
