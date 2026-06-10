<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Services;

use Fahara02\UdbLaravel\UdbMetadata;
use Fahara02\UdbLaravel\UdbProject;
use Udb\Core\Tenant\Services\V1\CreateTenantRequest;
use Udb\Core\Tenant\Services\V1\CreateTenantResponse;
use Udb\Core\Tenant\Services\V1\TenantServiceClient;

/**
 * Convenience wrapper over the generated {@see TenantServiceClient}.
 * Get/List/Update + config RPCs remain available via {@see client()}.
 */
final class TenantService
{
    public function __construct(
        private readonly UdbProject $project,
        private readonly TenantServiceClient $stub,
    ) {
    }

    /** The raw generated stub (Get/List/Update/Config RPCs live here). */
    public function client(): TenantServiceClient
    {
        return $this->stub;
    }

    /**
     * Create a tenant. `$config` and `$branding` are JSON strings as the
     * broker expects (the proto carries them as serialized JSON fields).
     */
    public function create(
        string $code,
        string $name,
        string $type = '',
        string $parentTenantId = '',
        string $config = '',
        string $branding = '',
        ?UdbMetadata $metadata = null,
    ): CreateTenantResponse {
        $request = (new CreateTenantRequest())
            ->setCode($code)
            ->setName($name)
            ->setType($type)
            ->setParentTenantId($parentTenantId)
            ->setConfig($config)
            ->setBranding($branding);

        return $this->createRequest($request, $metadata);
    }

    /**
     * Onboard a tenant — alias for {@see create()} that reads as the
     * higher-level intent at call sites ("onboard a new customer").
     */
    public function onboard(
        string $code,
        string $name,
        string $type = '',
        string $config = '',
        ?UdbMetadata $metadata = null,
    ): CreateTenantResponse {
        return $this->create($code, $name, $type, '', $config, '', $metadata);
    }

    public function createRequest(CreateTenantRequest $request, ?UdbMetadata $metadata = null): CreateTenantResponse
    {
        return $this->project->invoke(
            'CreateTenant',
            fn (array $md, array $o) => $this->stub->CreateTenant($request, $md, $o),
            $metadata,
            CreateTenantResponse::class,
        );
    }
}
