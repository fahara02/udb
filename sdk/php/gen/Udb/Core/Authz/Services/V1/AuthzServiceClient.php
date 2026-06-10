<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Authz\Services\V1;

/**
 * UDB-owned authorization service for RBAC, ABAC, ReBAC, tenant/project
 * domains, and audit-ready access decisions.
 */
class AuthzServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\AuthzRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\AuthzResponse>
     */
    public function Authorize(\Udb\Core\Authz\Services\V1\AuthzRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/Authorize',
        $argument,
        ['\Udb\Core\Authz\Services\V1\AuthzResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\CheckAccessRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\CheckAccessResponse>
     */
    public function CheckAccess(\Udb\Core\Authz\Services\V1\CheckAccessRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/CheckAccess',
        $argument,
        ['\Udb\Core\Authz\Services\V1\CheckAccessResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\CreateRoleRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\CreateRoleResponse>
     */
    public function CreateRole(\Udb\Core\Authz\Services\V1\CreateRoleRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/CreateRole',
        $argument,
        ['\Udb\Core\Authz\Services\V1\CreateRoleResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\AssignRoleRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\AssignRoleResponse>
     */
    public function AssignRole(\Udb\Core\Authz\Services\V1\AssignRoleRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/AssignRole',
        $argument,
        ['\Udb\Core\Authz\Services\V1\AssignRoleResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\CreatePolicyRuleRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\CreatePolicyRuleResponse>
     */
    public function CreatePolicyRule(\Udb\Core\Authz\Services\V1\CreatePolicyRuleRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/CreatePolicyRule',
        $argument,
        ['\Udb\Core\Authz\Services\V1\CreatePolicyRuleResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\ListUserPermissionsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\ListUserPermissionsResponse>
     */
    public function ListUserPermissions(\Udb\Core\Authz\Services\V1\ListUserPermissionsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/ListUserPermissions',
        $argument,
        ['\Udb\Core\Authz\Services\V1\ListUserPermissionsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\ListAccessDecisionAuditsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\ListAccessDecisionAuditsResponse>
     */
    public function ListAccessDecisionAudits(\Udb\Core\Authz\Services\V1\ListAccessDecisionAuditsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/ListAccessDecisionAudits',
        $argument,
        ['\Udb\Core\Authz\Services\V1\ListAccessDecisionAuditsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Revoke a role from a user.
     * @param \Udb\Core\Authz\Services\V1\RevokeRoleRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\RevokeRoleResponse>
     */
    public function RevokeRole(\Udb\Core\Authz\Services\V1\RevokeRoleRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/RevokeRole',
        $argument,
        ['\Udb\Core\Authz\Services\V1\RevokeRoleResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List all role assignments for a user.
     * @param \Udb\Core\Authz\Services\V1\ListUserRolesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\ListUserRolesResponse>
     */
    public function ListUserRoles(\Udb\Core\Authz\Services\V1\ListUserRolesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/ListUserRoles',
        $argument,
        ['\Udb\Core\Authz\Services\V1\ListUserRolesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get a role by ID.
     * @param \Udb\Core\Authz\Services\V1\GetRoleRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\GetRoleResponse>
     */
    public function GetRole(\Udb\Core\Authz\Services\V1\GetRoleRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/GetRole',
        $argument,
        ['\Udb\Core\Authz\Services\V1\GetRoleResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List all roles for a domain/tenant.
     * @param \Udb\Core\Authz\Services\V1\ListRolesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\ListRolesResponse>
     */
    public function ListRoles(\Udb\Core\Authz\Services\V1\ListRolesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/ListRoles',
        $argument,
        ['\Udb\Core\Authz\Services\V1\ListRolesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Batch check multiple permissions at once.
     * @param \Udb\Core\Authz\Services\V1\BatchCheckPermissionsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\BatchCheckPermissionsResponse>
     */
    public function BatchCheckPermissions(\Udb\Core\Authz\Services\V1\BatchCheckPermissionsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/BatchCheckPermissions',
        $argument,
        ['\Udb\Core\Authz\Services\V1\BatchCheckPermissionsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Update a role's name, description, or active status.
     * @param \Udb\Core\Authz\Services\V1\UpdateRoleRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\UpdateRoleResponse>
     */
    public function UpdateRole(\Udb\Core\Authz\Services\V1\UpdateRoleRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/UpdateRole',
        $argument,
        ['\Udb\Core\Authz\Services\V1\UpdateRoleResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Delete a role (soft-delete; existing assignments are revoked).
     * @param \Udb\Core\Authz\Services\V1\DeleteRoleRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\DeleteRoleResponse>
     */
    public function DeleteRole(\Udb\Core\Authz\Services\V1\DeleteRoleRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/DeleteRole',
        $argument,
        ['\Udb\Core\Authz\Services\V1\DeleteRoleResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get a single policy rule by ID.
     * @param \Udb\Core\Authz\Services\V1\GetPolicyRuleRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\GetPolicyRuleResponse>
     */
    public function GetPolicyRule(\Udb\Core\Authz\Services\V1\GetPolicyRuleRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/GetPolicyRule',
        $argument,
        ['\Udb\Core\Authz\Services\V1\GetPolicyRuleResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List policy rules with optional domain/subject/object filters.
     * @param \Udb\Core\Authz\Services\V1\ListPolicyRulesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\ListPolicyRulesResponse>
     */
    public function ListPolicyRules(\Udb\Core\Authz\Services\V1\ListPolicyRulesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/ListPolicyRules',
        $argument,
        ['\Udb\Core\Authz\Services\V1\ListPolicyRulesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Delete a policy rule.
     * @param \Udb\Core\Authz\Services\V1\DeletePolicyRuleRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\DeletePolicyRuleResponse>
     */
    public function DeletePolicyRule(\Udb\Core\Authz\Services\V1\DeletePolicyRuleRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/DeletePolicyRule',
        $argument,
        ['\Udb\Core\Authz\Services\V1\DeletePolicyRuleResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\PutRoleBindingRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\AuthMutationResponse>
     */
    public function PutRoleBinding(\Udb\Core\Authz\Services\V1\PutRoleBindingRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/PutRoleBinding',
        $argument,
        ['\Udb\Core\Authz\Services\V1\AuthMutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\PutRelationshipRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\AuthMutationResponse>
     */
    public function PutRelationship(\Udb\Core\Authz\Services\V1\PutRelationshipRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/PutRelationship',
        $argument,
        ['\Udb\Core\Authz\Services\V1\AuthMutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\PutAuthzPolicyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\AuthMutationResponse>
     */
    public function PutAuthzPolicy(\Udb\Core\Authz\Services\V1\PutAuthzPolicyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/PutAuthzPolicy',
        $argument,
        ['\Udb\Core\Authz\Services\V1\AuthMutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\LintAuthzPoliciesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\LintAuthzPoliciesResponse>
     */
    public function LintAuthzPolicies(\Udb\Core\Authz\Services\V1\LintAuthzPoliciesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/LintAuthzPolicies',
        $argument,
        ['\Udb\Core\Authz\Services\V1\LintAuthzPoliciesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Stage 2: authorize and, when allowed, mint a short-lived native-access
     * contract (restricted role + scoped DSN + RLS session variables).
     * @param \Udb\Core\Authz\Services\V1\NativeAccessRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\NativeAccessResponse>
     */
    public function GetNativeAccess(\Udb\Core\Authz\Services\V1\NativeAccessRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/GetNativeAccess',
        $argument,
        ['\Udb\Core\Authz\Services\V1\NativeAccessResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Stage 2: return a signed policy bundle for local SDK authorization caches.
     * @param \Udb\Core\Authz\Services\V1\PolicyBundleRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\PolicyBundleResponse>
     */
    public function GetPolicyBundle(\Udb\Core\Authz\Services\V1\PolicyBundleRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/GetPolicyBundle',
        $argument,
        ['\Udb\Core\Authz\Services\V1\PolicyBundleResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Phase K: Authz Policy Governance ─────────────────────────────────────
     * Every governance mutation is itself authorized under
     * `native.authz.governance` and requires an explicit authz governance scope
     * (authz:admin / authz:policy:write / authz:role:write) or a break-glass
     * grant — never a bare bearer token. Drafts and simulations never touch the
     * live authorization snapshot; only ActivatePolicyVersion / RollbackPolicyVersion do.
     *
     * @param \Udb\Core\Authz\Services\V1\CreatePolicyDraftRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\PolicyDraftResponse>
     */
    public function CreatePolicyDraft(\Udb\Core\Authz\Services\V1\CreatePolicyDraftRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/CreatePolicyDraft',
        $argument,
        ['\Udb\Core\Authz\Services\V1\PolicyDraftResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\UpdatePolicyDraftRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\PolicyDraftResponse>
     */
    public function UpdatePolicyDraft(\Udb\Core\Authz\Services\V1\UpdatePolicyDraftRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/UpdatePolicyDraft',
        $argument,
        ['\Udb\Core\Authz\Services\V1\PolicyDraftResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\DiffPolicyDraftRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\DiffPolicyDraftResponse>
     */
    public function DiffPolicyDraft(\Udb\Core\Authz\Services\V1\DiffPolicyDraftRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/DiffPolicyDraft',
        $argument,
        ['\Udb\Core\Authz\Services\V1\DiffPolicyDraftResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\SubmitPolicyDraftRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\PolicyDraftResponse>
     */
    public function SubmitPolicyDraft(\Udb\Core\Authz\Services\V1\SubmitPolicyDraftRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/SubmitPolicyDraft',
        $argument,
        ['\Udb\Core\Authz\Services\V1\PolicyDraftResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\ApprovePolicyDraftRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\PolicyApprovalResponse>
     */
    public function ApprovePolicyDraft(\Udb\Core\Authz\Services\V1\ApprovePolicyDraftRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/ApprovePolicyDraft',
        $argument,
        ['\Udb\Core\Authz\Services\V1\PolicyApprovalResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\RejectPolicyDraftRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\PolicyApprovalResponse>
     */
    public function RejectPolicyDraft(\Udb\Core\Authz\Services\V1\RejectPolicyDraftRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/RejectPolicyDraft',
        $argument,
        ['\Udb\Core\Authz\Services\V1\PolicyApprovalResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\ActivatePolicyVersionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\ActivationResponse>
     */
    public function ActivatePolicyVersion(\Udb\Core\Authz\Services\V1\ActivatePolicyVersionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/ActivatePolicyVersion',
        $argument,
        ['\Udb\Core\Authz\Services\V1\ActivationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\RollbackPolicyVersionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\ActivationResponse>
     */
    public function RollbackPolicyVersion(\Udb\Core\Authz\Services\V1\RollbackPolicyVersionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/RollbackPolicyVersion',
        $argument,
        ['\Udb\Core\Authz\Services\V1\ActivationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Activate a policy version to a canary scope (subset of the fleet) before
     * fleet-wide. A metric-based evaluator then auto-rolls back on breach.
     * @param \Udb\Core\Authz\Services\V1\ActivateCanaryRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\CanaryResponse>
     */
    public function ActivateCanary(\Udb\Core\Authz\Services\V1\ActivateCanaryRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/ActivateCanary',
        $argument,
        ['\Udb\Core\Authz\Services\V1\CanaryResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Promote a baked, within-threshold canary to fleet-wide enforcement.
     * @param \Udb\Core\Authz\Services\V1\PromoteCanaryRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\CanaryResponse>
     */
    public function PromoteCanary(\Udb\Core\Authz\Services\V1\PromoteCanaryRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/PromoteCanary',
        $argument,
        ['\Udb\Core\Authz\Services\V1\CanaryResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Read a canary's current state + promote-eligibility.
     * @param \Udb\Core\Authz\Services\V1\GetCanaryStatusRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\GetCanaryStatusResponse>
     */
    public function GetCanaryStatus(\Udb\Core\Authz\Services\V1\GetCanaryStatusRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/GetCanaryStatus',
        $argument,
        ['\Udb\Core\Authz\Services\V1\GetCanaryStatusResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\ListPolicyVersionsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\ListPolicyVersionsResponse>
     */
    public function ListPolicyVersions(\Udb\Core\Authz\Services\V1\ListPolicyVersionsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/ListPolicyVersions',
        $argument,
        ['\Udb\Core\Authz\Services\V1\ListPolicyVersionsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\SimulatePolicyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\SimulatePolicyResponse>
     */
    public function SimulatePolicy(\Udb\Core\Authz\Services\V1\SimulatePolicyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/SimulatePolicy',
        $argument,
        ['\Udb\Core\Authz\Services\V1\SimulatePolicyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\ExplainPolicyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\ExplainPolicyResponse>
     */
    public function ExplainPolicy(\Udb\Core\Authz\Services\V1\ExplainPolicyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/ExplainPolicy',
        $argument,
        ['\Udb\Core\Authz\Services\V1\ExplainPolicyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\GetAuthzRevisionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\GetAuthzRevisionResponse>
     */
    public function GetAuthzRevision(\Udb\Core\Authz\Services\V1\GetAuthzRevisionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/GetAuthzRevision',
        $argument,
        ['\Udb\Core\Authz\Services\V1\GetAuthzRevisionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\InvalidatePolicyBundlesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\InvalidatePolicyBundlesResponse>
     */
    public function InvalidatePolicyBundles(\Udb\Core\Authz\Services\V1\InvalidatePolicyBundlesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/InvalidatePolicyBundles',
        $argument,
        ['\Udb\Core\Authz\Services\V1\InvalidatePolicyBundlesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\SeedBuiltinRolesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\SeedBuiltinRolesResponse>
     */
    public function SeedBuiltinRoles(\Udb\Core\Authz\Services\V1\SeedBuiltinRolesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/SeedBuiltinRoles',
        $argument,
        ['\Udb\Core\Authz\Services\V1\SeedBuiltinRolesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authz\Services\V1\MigrateLegacyPoliciesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authz\Services\V1\MigrateLegacyPoliciesResponse>
     */
    public function MigrateLegacyPolicies(\Udb\Core\Authz\Services\V1\MigrateLegacyPoliciesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authz.services.v1.AuthzService/MigrateLegacyPolicies',
        $argument,
        ['\Udb\Core\Authz\Services\V1\MigrateLegacyPoliciesResponse', 'decode'],
        $metadata, $options);
    }

}
