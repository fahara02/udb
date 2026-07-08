<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Tenant\Services\V1;

/**
 */
class TenantServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Create tenant
     * @param \Udb\Core\Tenant\Services\V1\CreateTenantRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Tenant\Services\V1\CreateTenantResponse>
     */
    public function CreateTenant(\Udb\Core\Tenant\Services\V1\CreateTenantRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.tenant.services.v1.TenantService/CreateTenant',
        $argument,
        ['\Udb\Core\Tenant\Services\V1\CreateTenantResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get tenant
     * @param \Udb\Core\Tenant\Services\V1\GetTenantRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Tenant\Services\V1\GetTenantResponse>
     */
    public function GetTenant(\Udb\Core\Tenant\Services\V1\GetTenantRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.tenant.services.v1.TenantService/GetTenant',
        $argument,
        ['\Udb\Core\Tenant\Services\V1\GetTenantResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List tenants
     * @param \Udb\Core\Tenant\Services\V1\ListTenantsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Tenant\Services\V1\ListTenantsResponse>
     */
    public function ListTenants(\Udb\Core\Tenant\Services\V1\ListTenantsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.tenant.services.v1.TenantService/ListTenants',
        $argument,
        ['\Udb\Core\Tenant\Services\V1\ListTenantsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Update tenant
     * @param \Udb\Core\Tenant\Services\V1\UpdateTenantRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Tenant\Services\V1\UpdateTenantResponse>
     */
    public function UpdateTenant(\Udb\Core\Tenant\Services\V1\UpdateTenantRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.tenant.services.v1.TenantService/UpdateTenant',
        $argument,
        ['\Udb\Core\Tenant\Services\V1\UpdateTenantResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get tenant config
     * @param \Udb\Core\Tenant\Services\V1\GetTenantConfigRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Tenant\Services\V1\GetTenantConfigResponse>
     */
    public function GetTenantConfig(\Udb\Core\Tenant\Services\V1\GetTenantConfigRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.tenant.services.v1.TenantService/GetTenantConfig',
        $argument,
        ['\Udb\Core\Tenant\Services\V1\GetTenantConfigResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Update tenant config
     * @param \Udb\Core\Tenant\Services\V1\UpdateTenantConfigRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Tenant\Services\V1\UpdateTenantConfigResponse>
     */
    public function UpdateTenantConfig(\Udb\Core\Tenant\Services\V1\UpdateTenantConfigRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.tenant.services.v1.TenantService/UpdateTenantConfig',
        $argument,
        ['\Udb\Core\Tenant\Services\V1\UpdateTenantConfigResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Purge tenant (GDPR right-to-be-forgotten). HARD-deletes every row the tenant
     * owns across all tenant-columned entity tables, then revokes the tenant's and
     * its principals' tokens. Irreversible — DESTRUCTIVE op-kind + a required
     * confirmation token gate it. Mirrors the destructive-RPC endpoint_security of
     * siblings like authn.ChangeUserStatus (AUTH_MODE_BEARER, tenant_required,
     * request_context_required).
     * @param \Udb\Core\Tenant\Services\V1\PurgeTenantRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Tenant\Services\V1\PurgeTenantResponse>
     */
    public function PurgeTenant(\Udb\Core\Tenant\Services\V1\PurgeTenantRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.tenant.services.v1.TenantService/PurgeTenant',
        $argument,
        ['\Udb\Core\Tenant\Services\V1\PurgeTenantResponse', 'decode'],
        $metadata, $options);
    }

}
