import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.authz.entity.v1 import governance_enums_pb2 as _governance_enums_pb2
from udb.core.authz.entity.v1 import policy_approval_pb2 as _policy_approval_pb2
from udb.core.authz.entity.v1 import policy_canary_pb2 as _policy_canary_pb2
from udb.core.authz.entity.v1 import policy_draft_pb2 as _policy_draft_pb2
from udb.core.authz.entity.v1 import policy_set_pb2 as _policy_set_pb2
from udb.core.authz.entity.v1 import policy_version_pb2 as _policy_version_pb2
from udb.core.authz.services.v1 import core_pb2 as _core_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GovernanceActor(_message.Message):
    __slots__ = ("subject", "tenant_id", "project_id", "scopes", "roles", "break_glass", "break_glass_reason", "break_glass_expires_at_unix")
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    BREAK_GLASS_FIELD_NUMBER: _ClassVar[int]
    BREAK_GLASS_REASON_FIELD_NUMBER: _ClassVar[int]
    BREAK_GLASS_EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    subject: str
    tenant_id: str
    project_id: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    roles: _containers.RepeatedScalarFieldContainer[str]
    break_glass: bool
    break_glass_reason: str
    break_glass_expires_at_unix: int
    def __init__(self, subject: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ..., roles: _Optional[_Iterable[str]] = ..., break_glass: bool = ..., break_glass_reason: _Optional[str] = ..., break_glass_expires_at_unix: _Optional[int] = ...) -> None: ...

class PolicyDocument(_message.Message):
    __slots__ = ("policies", "role_bindings", "relationship_tuples")
    POLICIES_FIELD_NUMBER: _ClassVar[int]
    ROLE_BINDINGS_FIELD_NUMBER: _ClassVar[int]
    RELATIONSHIP_TUPLES_FIELD_NUMBER: _ClassVar[int]
    policies: _containers.RepeatedCompositeFieldContainer[_core_pb2.AuthzPolicyRecord]
    role_bindings: _containers.RepeatedCompositeFieldContainer[_core_pb2.RoleBinding]
    relationship_tuples: _containers.RepeatedCompositeFieldContainer[_core_pb2.RelationshipTuple]
    def __init__(self, policies: _Optional[_Iterable[_Union[_core_pb2.AuthzPolicyRecord, _Mapping]]] = ..., role_bindings: _Optional[_Iterable[_Union[_core_pb2.RoleBinding, _Mapping]]] = ..., relationship_tuples: _Optional[_Iterable[_Union[_core_pb2.RelationshipTuple, _Mapping]]] = ...) -> None: ...

class CreatePolicyDraftRequest(_message.Message):
    __slots__ = ("actor", "tenant_id", "project_id", "policy_set_name", "title", "change_reason", "high_risk", "document", "branch_from_active")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_SET_NAME_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    CHANGE_REASON_FIELD_NUMBER: _ClassVar[int]
    HIGH_RISK_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_FIELD_NUMBER: _ClassVar[int]
    BRANCH_FROM_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    tenant_id: str
    project_id: str
    policy_set_name: str
    title: str
    change_reason: str
    high_risk: bool
    document: PolicyDocument
    branch_from_active: bool
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., policy_set_name: _Optional[str] = ..., title: _Optional[str] = ..., change_reason: _Optional[str] = ..., high_risk: bool = ..., document: _Optional[_Union[PolicyDocument, _Mapping]] = ..., branch_from_active: bool = ...) -> None: ...

class PolicyDraftResponse(_message.Message):
    __slots__ = ("draft", "policy_set", "document")
    DRAFT_FIELD_NUMBER: _ClassVar[int]
    POLICY_SET_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_FIELD_NUMBER: _ClassVar[int]
    draft: _policy_draft_pb2.PolicyDraft
    policy_set: _policy_set_pb2.PolicySet
    document: PolicyDocument
    def __init__(self, draft: _Optional[_Union[_policy_draft_pb2.PolicyDraft, _Mapping]] = ..., policy_set: _Optional[_Union[_policy_set_pb2.PolicySet, _Mapping]] = ..., document: _Optional[_Union[PolicyDocument, _Mapping]] = ...) -> None: ...

class UpdatePolicyDraftRequest(_message.Message):
    __slots__ = ("actor", "draft_id", "document", "change_reason", "expected_updated_at_unix", "high_risk", "title")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    DRAFT_ID_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_FIELD_NUMBER: _ClassVar[int]
    CHANGE_REASON_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_UPDATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    HIGH_RISK_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    draft_id: str
    document: PolicyDocument
    change_reason: str
    expected_updated_at_unix: int
    high_risk: bool
    title: str
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., draft_id: _Optional[str] = ..., document: _Optional[_Union[PolicyDocument, _Mapping]] = ..., change_reason: _Optional[str] = ..., expected_updated_at_unix: _Optional[int] = ..., high_risk: bool = ..., title: _Optional[str] = ...) -> None: ...

class DiffPolicyDraftRequest(_message.Message):
    __slots__ = ("actor", "draft_id", "against_version_id")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    DRAFT_ID_FIELD_NUMBER: _ClassVar[int]
    AGAINST_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    draft_id: str
    against_version_id: str
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., draft_id: _Optional[str] = ..., against_version_id: _Optional[str] = ...) -> None: ...

class PolicyDiffEntry(_message.Message):
    __slots__ = ("change", "kind", "id", "before_json", "after_json")
    CHANGE_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    BEFORE_JSON_FIELD_NUMBER: _ClassVar[int]
    AFTER_JSON_FIELD_NUMBER: _ClassVar[int]
    change: str
    kind: str
    id: str
    before_json: str
    after_json: str
    def __init__(self, change: _Optional[str] = ..., kind: _Optional[str] = ..., id: _Optional[str] = ..., before_json: _Optional[str] = ..., after_json: _Optional[str] = ...) -> None: ...

class DiffPolicyDraftResponse(_message.Message):
    __slots__ = ("entries", "diff_json")
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    DIFF_JSON_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[PolicyDiffEntry]
    diff_json: str
    def __init__(self, entries: _Optional[_Iterable[_Union[PolicyDiffEntry, _Mapping]]] = ..., diff_json: _Optional[str] = ...) -> None: ...

class SubmitPolicyDraftRequest(_message.Message):
    __slots__ = ("actor", "draft_id", "expected_updated_at_unix")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    DRAFT_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_UPDATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    draft_id: str
    expected_updated_at_unix: int
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., draft_id: _Optional[str] = ..., expected_updated_at_unix: _Optional[int] = ...) -> None: ...

class ApprovePolicyDraftRequest(_message.Message):
    __slots__ = ("actor", "draft_id", "reviewer", "reason")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    DRAFT_ID_FIELD_NUMBER: _ClassVar[int]
    REVIEWER_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    draft_id: str
    reviewer: str
    reason: str
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., draft_id: _Optional[str] = ..., reviewer: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class RejectPolicyDraftRequest(_message.Message):
    __slots__ = ("actor", "draft_id", "reviewer", "reason")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    DRAFT_ID_FIELD_NUMBER: _ClassVar[int]
    REVIEWER_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    draft_id: str
    reviewer: str
    reason: str
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., draft_id: _Optional[str] = ..., reviewer: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class PolicyApprovalResponse(_message.Message):
    __slots__ = ("draft", "approval", "version")
    DRAFT_FIELD_NUMBER: _ClassVar[int]
    APPROVAL_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    draft: _policy_draft_pb2.PolicyDraft
    approval: _policy_approval_pb2.PolicyApproval
    version: _policy_version_pb2.PolicyVersion
    def __init__(self, draft: _Optional[_Union[_policy_draft_pb2.PolicyDraft, _Mapping]] = ..., approval: _Optional[_Union[_policy_approval_pb2.PolicyApproval, _Mapping]] = ..., version: _Optional[_Union[_policy_version_pb2.PolicyVersion, _Mapping]] = ...) -> None: ...

class ActivatePolicyVersionRequest(_message.Message):
    __slots__ = ("actor", "policy_version_id", "expected_revision", "expected_policy_revision", "expected_relationship_revision")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_REVISION_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_POLICY_REVISION_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_RELATIONSHIP_REVISION_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    policy_version_id: str
    expected_revision: int
    expected_policy_revision: int
    expected_relationship_revision: int
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., policy_version_id: _Optional[str] = ..., expected_revision: _Optional[int] = ..., expected_policy_revision: _Optional[int] = ..., expected_relationship_revision: _Optional[int] = ...) -> None: ...

class RollbackPolicyVersionRequest(_message.Message):
    __slots__ = ("actor", "policy_set_id", "target_version_id", "change_reason")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    POLICY_SET_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    CHANGE_REASON_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    policy_set_id: str
    target_version_id: str
    change_reason: str
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., policy_set_id: _Optional[str] = ..., target_version_id: _Optional[str] = ..., change_reason: _Optional[str] = ...) -> None: ...

class ActivationResponse(_message.Message):
    __slots__ = ("version", "policy_set", "policy_revision", "relationship_revision", "content_hash")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    POLICY_SET_FIELD_NUMBER: _ClassVar[int]
    POLICY_REVISION_FIELD_NUMBER: _ClassVar[int]
    RELATIONSHIP_REVISION_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    version: _policy_version_pb2.PolicyVersion
    policy_set: _policy_set_pb2.PolicySet
    policy_revision: int
    relationship_revision: int
    content_hash: str
    def __init__(self, version: _Optional[_Union[_policy_version_pb2.PolicyVersion, _Mapping]] = ..., policy_set: _Optional[_Union[_policy_set_pb2.PolicySet, _Mapping]] = ..., policy_revision: _Optional[int] = ..., relationship_revision: _Optional[int] = ..., content_hash: _Optional[str] = ...) -> None: ...

class ActivateCanaryRequest(_message.Message):
    __slots__ = ("actor", "policy_version_id", "scope_kind", "scope_values", "success_window_secs", "metric_threshold", "min_samples", "expected_revision")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPE_KIND_FIELD_NUMBER: _ClassVar[int]
    SCOPE_VALUES_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_WINDOW_SECS_FIELD_NUMBER: _ClassVar[int]
    METRIC_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    MIN_SAMPLES_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_REVISION_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    policy_version_id: str
    scope_kind: _policy_canary_pb2.CanaryScopeKind
    scope_values: _containers.RepeatedScalarFieldContainer[str]
    success_window_secs: int
    metric_threshold: float
    min_samples: int
    expected_revision: int
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., policy_version_id: _Optional[str] = ..., scope_kind: _Optional[_Union[_policy_canary_pb2.CanaryScopeKind, str]] = ..., scope_values: _Optional[_Iterable[str]] = ..., success_window_secs: _Optional[int] = ..., metric_threshold: _Optional[float] = ..., min_samples: _Optional[int] = ..., expected_revision: _Optional[int] = ...) -> None: ...

class CanaryResponse(_message.Message):
    __slots__ = ("canary", "version", "policy_set")
    CANARY_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    POLICY_SET_FIELD_NUMBER: _ClassVar[int]
    canary: _policy_canary_pb2.PolicyCanary
    version: _policy_version_pb2.PolicyVersion
    policy_set: _policy_set_pb2.PolicySet
    def __init__(self, canary: _Optional[_Union[_policy_canary_pb2.PolicyCanary, _Mapping]] = ..., version: _Optional[_Union[_policy_version_pb2.PolicyVersion, _Mapping]] = ..., policy_set: _Optional[_Union[_policy_set_pb2.PolicySet, _Mapping]] = ...) -> None: ...

class PromoteCanaryRequest(_message.Message):
    __slots__ = ("actor", "canary_id", "expected_revision")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    CANARY_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_REVISION_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    canary_id: str
    expected_revision: int
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., canary_id: _Optional[str] = ..., expected_revision: _Optional[int] = ...) -> None: ...

class GetCanaryStatusRequest(_message.Message):
    __slots__ = ("actor", "canary_id")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    CANARY_ID_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    canary_id: str
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., canary_id: _Optional[str] = ...) -> None: ...

class GetCanaryStatusResponse(_message.Message):
    __slots__ = ("canary", "promote_eligible", "window_remaining_secs")
    CANARY_FIELD_NUMBER: _ClassVar[int]
    PROMOTE_ELIGIBLE_FIELD_NUMBER: _ClassVar[int]
    WINDOW_REMAINING_SECS_FIELD_NUMBER: _ClassVar[int]
    canary: _policy_canary_pb2.PolicyCanary
    promote_eligible: bool
    window_remaining_secs: int
    def __init__(self, canary: _Optional[_Union[_policy_canary_pb2.PolicyCanary, _Mapping]] = ..., promote_eligible: bool = ..., window_remaining_secs: _Optional[int] = ...) -> None: ...

class SimulationCase(_message.Message):
    __slots__ = ("principal", "resource", "action", "purpose", "attributes", "label")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    PRINCIPAL_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    PURPOSE_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    principal: _core_pb2.Principal
    resource: _core_pb2.ResourceRef
    action: str
    purpose: str
    attributes: _containers.ScalarMap[str, str]
    label: str
    def __init__(self, principal: _Optional[_Union[_core_pb2.Principal, _Mapping]] = ..., resource: _Optional[_Union[_core_pb2.ResourceRef, _Mapping]] = ..., action: _Optional[str] = ..., purpose: _Optional[str] = ..., attributes: _Optional[_Mapping[str, str]] = ..., label: _Optional[str] = ...) -> None: ...

class SimulatePolicyRequest(_message.Message):
    __slots__ = ("actor", "tenant_id", "project_id", "draft_id", "candidate", "cases", "persist", "policy_version_id")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    DRAFT_ID_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_FIELD_NUMBER: _ClassVar[int]
    CASES_FIELD_NUMBER: _ClassVar[int]
    PERSIST_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    tenant_id: str
    project_id: str
    draft_id: str
    candidate: PolicyDocument
    cases: _containers.RepeatedCompositeFieldContainer[SimulationCase]
    persist: bool
    policy_version_id: str
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., draft_id: _Optional[str] = ..., candidate: _Optional[_Union[PolicyDocument, _Mapping]] = ..., cases: _Optional[_Iterable[_Union[SimulationCase, _Mapping]]] = ..., persist: bool = ..., policy_version_id: _Optional[str] = ...) -> None: ...

class SimulationResult(_message.Message):
    __slots__ = ("label", "active_decision", "draft_decision", "changed", "diff_json")
    LABEL_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_DECISION_FIELD_NUMBER: _ClassVar[int]
    DRAFT_DECISION_FIELD_NUMBER: _ClassVar[int]
    CHANGED_FIELD_NUMBER: _ClassVar[int]
    DIFF_JSON_FIELD_NUMBER: _ClassVar[int]
    label: str
    active_decision: _core_pb2.Decision
    draft_decision: _core_pb2.Decision
    changed: bool
    diff_json: str
    def __init__(self, label: _Optional[str] = ..., active_decision: _Optional[_Union[_core_pb2.Decision, _Mapping]] = ..., draft_decision: _Optional[_Union[_core_pb2.Decision, _Mapping]] = ..., changed: bool = ..., diff_json: _Optional[str] = ...) -> None: ...

class SimulatePolicyResponse(_message.Message):
    __slots__ = ("results", "diff_json")
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    DIFF_JSON_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[SimulationResult]
    diff_json: str
    def __init__(self, results: _Optional[_Iterable[_Union[SimulationResult, _Mapping]]] = ..., diff_json: _Optional[str] = ...) -> None: ...

class ExplainPolicyRequest(_message.Message):
    __slots__ = ("actor", "tenant_id", "project_id", "draft_id", "candidate", "test_case")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    DRAFT_ID_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_FIELD_NUMBER: _ClassVar[int]
    TEST_CASE_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    tenant_id: str
    project_id: str
    draft_id: str
    candidate: PolicyDocument
    test_case: SimulationCase
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., draft_id: _Optional[str] = ..., candidate: _Optional[_Union[PolicyDocument, _Mapping]] = ..., test_case: _Optional[_Union[SimulationCase, _Mapping]] = ...) -> None: ...

class ExplainPolicyResponse(_message.Message):
    __slots__ = ("decision", "matched_policy_ids", "deny_reason", "explanation")
    DECISION_FIELD_NUMBER: _ClassVar[int]
    MATCHED_POLICY_IDS_FIELD_NUMBER: _ClassVar[int]
    DENY_REASON_FIELD_NUMBER: _ClassVar[int]
    EXPLANATION_FIELD_NUMBER: _ClassVar[int]
    decision: _core_pb2.Decision
    matched_policy_ids: _containers.RepeatedScalarFieldContainer[str]
    deny_reason: str
    explanation: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, decision: _Optional[_Union[_core_pb2.Decision, _Mapping]] = ..., matched_policy_ids: _Optional[_Iterable[str]] = ..., deny_reason: _Optional[str] = ..., explanation: _Optional[_Iterable[str]] = ...) -> None: ...

class ListPolicyVersionsRequest(_message.Message):
    __slots__ = ("actor", "tenant_id", "project_id", "policy_set_id", "state", "page")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_SET_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    tenant_id: str
    project_id: str
    policy_set_id: str
    state: _governance_enums_pb2.PolicyVersionState
    page: _dto_pb2.PageRequest
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., policy_set_id: _Optional[str] = ..., state: _Optional[_Union[_governance_enums_pb2.PolicyVersionState, str]] = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListPolicyVersionsResponse(_message.Message):
    __slots__ = ("versions", "page")
    VERSIONS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    versions: _containers.RepeatedCompositeFieldContainer[_policy_version_pb2.PolicyVersion]
    page: _dto_pb2.PageResponse
    def __init__(self, versions: _Optional[_Iterable[_Union[_policy_version_pb2.PolicyVersion, _Mapping]]] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class GetAuthzRevisionRequest(_message.Message):
    __slots__ = ("tenant_id", "project_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    project_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ...) -> None: ...

class GetAuthzRevisionResponse(_message.Message):
    __slots__ = ("policy_revision", "relationship_revision", "content_hash", "changed_at")
    POLICY_REVISION_FIELD_NUMBER: _ClassVar[int]
    RELATIONSHIP_REVISION_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    CHANGED_AT_FIELD_NUMBER: _ClassVar[int]
    policy_revision: int
    relationship_revision: int
    content_hash: str
    changed_at: _timestamp_pb2.Timestamp
    def __init__(self, policy_revision: _Optional[int] = ..., relationship_revision: _Optional[int] = ..., content_hash: _Optional[str] = ..., changed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class InvalidatePolicyBundlesRequest(_message.Message):
    __slots__ = ("actor", "tenant_id", "project_id", "reason")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    tenant_id: str
    project_id: str
    reason: str
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class InvalidatePolicyBundlesResponse(_message.Message):
    __slots__ = ("ok", "policy_revision", "relationship_revision")
    OK_FIELD_NUMBER: _ClassVar[int]
    POLICY_REVISION_FIELD_NUMBER: _ClassVar[int]
    RELATIONSHIP_REVISION_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    policy_revision: int
    relationship_revision: int
    def __init__(self, ok: bool = ..., policy_revision: _Optional[int] = ..., relationship_revision: _Optional[int] = ...) -> None: ...

class SeedBuiltinRolesRequest(_message.Message):
    __slots__ = ("actor", "tenant_id", "project_id")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    tenant_id: str
    project_id: str
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ...) -> None: ...

class SeedBuiltinRolesResponse(_message.Message):
    __slots__ = ("seeded_role_codes", "created", "existing")
    SEEDED_ROLE_CODES_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    EXISTING_FIELD_NUMBER: _ClassVar[int]
    seeded_role_codes: _containers.RepeatedScalarFieldContainer[str]
    created: int
    existing: int
    def __init__(self, seeded_role_codes: _Optional[_Iterable[str]] = ..., created: _Optional[int] = ..., existing: _Optional[int] = ...) -> None: ...

class MigrateLegacyPoliciesRequest(_message.Message):
    __slots__ = ("actor", "tenant_id", "project_id", "apply", "policy_set_name")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    APPLY_FIELD_NUMBER: _ClassVar[int]
    POLICY_SET_NAME_FIELD_NUMBER: _ClassVar[int]
    actor: GovernanceActor
    tenant_id: str
    project_id: str
    apply: bool
    policy_set_name: str
    def __init__(self, actor: _Optional[_Union[GovernanceActor, _Mapping]] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., apply: bool = ..., policy_set_name: _Optional[str] = ...) -> None: ...

class MigrateLegacyPoliciesResponse(_message.Message):
    __slots__ = ("draft", "diff", "simulation", "report_json")
    DRAFT_FIELD_NUMBER: _ClassVar[int]
    DIFF_FIELD_NUMBER: _ClassVar[int]
    SIMULATION_FIELD_NUMBER: _ClassVar[int]
    REPORT_JSON_FIELD_NUMBER: _ClassVar[int]
    draft: _policy_draft_pb2.PolicyDraft
    diff: _containers.RepeatedCompositeFieldContainer[PolicyDiffEntry]
    simulation: _containers.RepeatedCompositeFieldContainer[SimulationResult]
    report_json: str
    def __init__(self, draft: _Optional[_Union[_policy_draft_pb2.PolicyDraft, _Mapping]] = ..., diff: _Optional[_Iterable[_Union[PolicyDiffEntry, _Mapping]]] = ..., simulation: _Optional[_Iterable[_Union[SimulationResult, _Mapping]]] = ..., report_json: _Optional[str] = ...) -> None: ...
