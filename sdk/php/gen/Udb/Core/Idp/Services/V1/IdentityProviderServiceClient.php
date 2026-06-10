<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Idp\Services\V1;

/**
 * ---------------------------------------------------------------------------
 * IdentityProviderService — Enterprise identity-provider lifecycle, SAML 2.0
 * web SSO (metadata import + ACS), SCIM 2.0 provisioning, JIT user
 * provisioning, and external-identity linking. All RPCs are tenant-scoped and
 * server-only (control-plane); they run on the isolated native auth listener.
 * ---------------------------------------------------------------------------
 */
class IdentityProviderServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * ── Provider administration (J2.6) ────────────────────────────────────────
     * @param \Udb\Core\Idp\Services\V1\CreateProviderRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\CreateProviderResponse>
     */
    public function CreateProvider(\Udb\Core\Idp\Services\V1\CreateProviderRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/CreateProvider',
        $argument,
        ['\Udb\Core\Idp\Services\V1\CreateProviderResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\UpdateProviderRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\UpdateProviderResponse>
     */
    public function UpdateProvider(\Udb\Core\Idp\Services\V1\UpdateProviderRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/UpdateProvider',
        $argument,
        ['\Udb\Core\Idp\Services\V1\UpdateProviderResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\DisableProviderRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\DisableProviderResponse>
     */
    public function DisableProvider(\Udb\Core\Idp\Services\V1\DisableProviderRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/DisableProvider',
        $argument,
        ['\Udb\Core\Idp\Services\V1\DisableProviderResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\GetProviderRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\GetProviderResponse>
     */
    public function GetProvider(\Udb\Core\Idp\Services\V1\GetProviderRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/GetProvider',
        $argument,
        ['\Udb\Core\Idp\Services\V1\GetProviderResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\ListProvidersRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\ListProvidersResponse>
     */
    public function ListProviders(\Udb\Core\Idp\Services\V1\ListProvidersRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/ListProviders',
        $argument,
        ['\Udb\Core\Idp\Services\V1\ListProvidersResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\TestProviderDiscoveryRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\TestProviderDiscoveryResponse>
     */
    public function TestProviderDiscovery(\Udb\Core\Idp\Services\V1\TestProviderDiscoveryRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/TestProviderDiscovery',
        $argument,
        ['\Udb\Core\Idp\Services\V1\TestProviderDiscoveryResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\ForceJwksRefreshRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\ForceJwksRefreshResponse>
     */
    public function ForceJwksRefresh(\Udb\Core\Idp\Services\V1\ForceJwksRefreshRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/ForceJwksRefresh',
        $argument,
        ['\Udb\Core\Idp\Services\V1\ForceJwksRefreshResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\PreviewClaimMappingRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\PreviewClaimMappingResponse>
     */
    public function PreviewClaimMapping(\Udb\Core\Idp\Services\V1\PreviewClaimMappingRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/PreviewClaimMapping',
        $argument,
        ['\Udb\Core\Idp\Services\V1\PreviewClaimMappingResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\PreviewGroupMappingRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\PreviewGroupMappingResponse>
     */
    public function PreviewGroupMapping(\Udb\Core\Idp\Services\V1\PreviewGroupMappingRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/PreviewGroupMapping',
        $argument,
        ['\Udb\Core\Idp\Services\V1\PreviewGroupMappingResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\ListExternalIdentitiesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\ListExternalIdentitiesResponse>
     */
    public function ListExternalIdentities(\Udb\Core\Idp\Services\V1\ListExternalIdentitiesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/ListExternalIdentities',
        $argument,
        ['\Udb\Core\Idp\Services\V1\ListExternalIdentitiesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\LinkIdentityRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\LinkIdentityResponse>
     */
    public function LinkIdentity(\Udb\Core\Idp\Services\V1\LinkIdentityRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/LinkIdentity',
        $argument,
        ['\Udb\Core\Idp\Services\V1\LinkIdentityResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\UnlinkIdentityRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\UnlinkIdentityResponse>
     */
    public function UnlinkIdentity(\Udb\Core\Idp\Services\V1\UnlinkIdentityRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/UnlinkIdentity',
        $argument,
        ['\Udb\Core\Idp\Services\V1\UnlinkIdentityResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── SAML 2.0 (J2.2) ───────────────────────────────────────────────────────
     * @param \Udb\Core\Idp\Services\V1\ImportSamlMetadataRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\ImportSamlMetadataResponse>
     */
    public function ImportSamlMetadata(\Udb\Core\Idp\Services\V1\ImportSamlMetadataRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/ImportSamlMetadata',
        $argument,
        ['\Udb\Core\Idp\Services\V1\ImportSamlMetadataResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\StartSamlLoginRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\StartSamlLoginResponse>
     */
    public function StartSamlLogin(\Udb\Core\Idp\Services\V1\StartSamlLoginRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/StartSamlLogin',
        $argument,
        ['\Udb\Core\Idp\Services\V1\StartSamlLoginResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\SamlAcsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\SamlAcsResponse>
     */
    public function SamlAcs(\Udb\Core\Idp\Services\V1\SamlAcsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/SamlAcs',
        $argument,
        ['\Udb\Core\Idp\Services\V1\SamlAcsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── JIT provisioning + assurance (J2.4 / J2.5) ────────────────────────────
     * @param \Udb\Core\Idp\Services\V1\ResolveExternalIdentityRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\ResolveExternalIdentityResponse>
     */
    public function ResolveExternalIdentity(\Udb\Core\Idp\Services\V1\ResolveExternalIdentityRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/ResolveExternalIdentity',
        $argument,
        ['\Udb\Core\Idp\Services\V1\ResolveExternalIdentityResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── SCIM 2.0 (J2.3) ───────────────────────────────────────────────────────
     * @param \Udb\Core\Idp\Services\V1\ScimCreateUserRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\ScimCreateUserResponse>
     */
    public function ScimCreateUser(\Udb\Core\Idp\Services\V1\ScimCreateUserRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/ScimCreateUser',
        $argument,
        ['\Udb\Core\Idp\Services\V1\ScimCreateUserResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\ScimGetUserRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\ScimGetUserResponse>
     */
    public function ScimGetUser(\Udb\Core\Idp\Services\V1\ScimGetUserRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/ScimGetUser',
        $argument,
        ['\Udb\Core\Idp\Services\V1\ScimGetUserResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\ScimListUsersRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\ScimListUsersResponse>
     */
    public function ScimListUsers(\Udb\Core\Idp\Services\V1\ScimListUsersRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/ScimListUsers',
        $argument,
        ['\Udb\Core\Idp\Services\V1\ScimListUsersResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\ScimReplaceUserRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\ScimReplaceUserResponse>
     */
    public function ScimReplaceUser(\Udb\Core\Idp\Services\V1\ScimReplaceUserRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/ScimReplaceUser',
        $argument,
        ['\Udb\Core\Idp\Services\V1\ScimReplaceUserResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\ScimPatchUserRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\ScimPatchUserResponse>
     */
    public function ScimPatchUser(\Udb\Core\Idp\Services\V1\ScimPatchUserRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/ScimPatchUser',
        $argument,
        ['\Udb\Core\Idp\Services\V1\ScimPatchUserResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\ScimDeleteUserRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\ScimDeleteUserResponse>
     */
    public function ScimDeleteUser(\Udb\Core\Idp\Services\V1\ScimDeleteUserRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/ScimDeleteUser',
        $argument,
        ['\Udb\Core\Idp\Services\V1\ScimDeleteUserResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\ScimCreateGroupRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\ScimCreateGroupResponse>
     */
    public function ScimCreateGroup(\Udb\Core\Idp\Services\V1\ScimCreateGroupRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/ScimCreateGroup',
        $argument,
        ['\Udb\Core\Idp\Services\V1\ScimCreateGroupResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\ScimGetGroupRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\ScimGetGroupResponse>
     */
    public function ScimGetGroup(\Udb\Core\Idp\Services\V1\ScimGetGroupRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/ScimGetGroup',
        $argument,
        ['\Udb\Core\Idp\Services\V1\ScimGetGroupResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\ScimListGroupsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\ScimListGroupsResponse>
     */
    public function ScimListGroups(\Udb\Core\Idp\Services\V1\ScimListGroupsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/ScimListGroups',
        $argument,
        ['\Udb\Core\Idp\Services\V1\ScimListGroupsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\ScimPatchGroupRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\ScimPatchGroupResponse>
     */
    public function ScimPatchGroup(\Udb\Core\Idp\Services\V1\ScimPatchGroupRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/ScimPatchGroup',
        $argument,
        ['\Udb\Core\Idp\Services\V1\ScimPatchGroupResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Idp\Services\V1\ScimDeleteGroupRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Idp\Services\V1\ScimDeleteGroupResponse>
     */
    public function ScimDeleteGroup(\Udb\Core\Idp\Services\V1\ScimDeleteGroupRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.idp.services.v1.IdentityProviderService/ScimDeleteGroup',
        $argument,
        ['\Udb\Core\Idp\Services\V1\ScimDeleteGroupResponse', 'decode'],
        $metadata, $options);
    }

}
