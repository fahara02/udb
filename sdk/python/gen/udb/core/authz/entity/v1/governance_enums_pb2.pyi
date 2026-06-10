from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor

class PolicyVersionState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    POLICY_VERSION_STATE_UNSPECIFIED: _ClassVar[PolicyVersionState]
    POLICY_VERSION_STATE_DRAFT: _ClassVar[PolicyVersionState]
    POLICY_VERSION_STATE_PENDING_REVIEW: _ClassVar[PolicyVersionState]
    POLICY_VERSION_STATE_APPROVED: _ClassVar[PolicyVersionState]
    POLICY_VERSION_STATE_ACTIVE: _ClassVar[PolicyVersionState]
    POLICY_VERSION_STATE_SUPERSEDED: _ClassVar[PolicyVersionState]
    POLICY_VERSION_STATE_REJECTED: _ClassVar[PolicyVersionState]
    POLICY_VERSION_STATE_ROLLED_BACK: _ClassVar[PolicyVersionState]

class PolicyApprovalDecision(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    POLICY_APPROVAL_DECISION_UNSPECIFIED: _ClassVar[PolicyApprovalDecision]
    POLICY_APPROVAL_DECISION_APPROVED: _ClassVar[PolicyApprovalDecision]
    POLICY_APPROVAL_DECISION_REJECTED: _ClassVar[PolicyApprovalDecision]

class AuthzChangeType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    AUTHZ_CHANGE_TYPE_UNSPECIFIED: _ClassVar[AuthzChangeType]
    AUTHZ_CHANGE_TYPE_POLICY: _ClassVar[AuthzChangeType]
    AUTHZ_CHANGE_TYPE_ROLE: _ClassVar[AuthzChangeType]
    AUTHZ_CHANGE_TYPE_ROLE_ASSIGNMENT: _ClassVar[AuthzChangeType]
    AUTHZ_CHANGE_TYPE_RELATIONSHIP: _ClassVar[AuthzChangeType]
    AUTHZ_CHANGE_TYPE_ACTIVATION: _ClassVar[AuthzChangeType]
    AUTHZ_CHANGE_TYPE_ROLLBACK: _ClassVar[AuthzChangeType]
POLICY_VERSION_STATE_UNSPECIFIED: PolicyVersionState
POLICY_VERSION_STATE_DRAFT: PolicyVersionState
POLICY_VERSION_STATE_PENDING_REVIEW: PolicyVersionState
POLICY_VERSION_STATE_APPROVED: PolicyVersionState
POLICY_VERSION_STATE_ACTIVE: PolicyVersionState
POLICY_VERSION_STATE_SUPERSEDED: PolicyVersionState
POLICY_VERSION_STATE_REJECTED: PolicyVersionState
POLICY_VERSION_STATE_ROLLED_BACK: PolicyVersionState
POLICY_APPROVAL_DECISION_UNSPECIFIED: PolicyApprovalDecision
POLICY_APPROVAL_DECISION_APPROVED: PolicyApprovalDecision
POLICY_APPROVAL_DECISION_REJECTED: PolicyApprovalDecision
AUTHZ_CHANGE_TYPE_UNSPECIFIED: AuthzChangeType
AUTHZ_CHANGE_TYPE_POLICY: AuthzChangeType
AUTHZ_CHANGE_TYPE_ROLE: AuthzChangeType
AUTHZ_CHANGE_TYPE_ROLE_ASSIGNMENT: AuthzChangeType
AUTHZ_CHANGE_TYPE_RELATIONSHIP: AuthzChangeType
AUTHZ_CHANGE_TYPE_ACTIVATION: AuthzChangeType
AUTHZ_CHANGE_TYPE_ROLLBACK: AuthzChangeType
