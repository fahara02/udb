<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Services;

use Fahara02\UdbLaravel\UdbMetadata;
use Fahara02\UdbLaravel\UdbProject;
use Udb\Core\Asset\Services\V1\AssetServiceClient;
use Udb\Core\Asset\Services\V1\CompleteStepRequest;
use Udb\Core\Asset\Services\V1\CompleteStepResponse;
use Udb\Core\Asset\Services\V1\CreatePipelineDefinitionRequest;
use Udb\Core\Asset\Services\V1\CreatePipelineDefinitionResponse;
use Udb\Core\Asset\Services\V1\GetAssetRequest;
use Udb\Core\Asset\Services\V1\GetAssetResponse;
use Udb\Core\Asset\Services\V1\GetPipelineDefinitionRequest;
use Udb\Core\Asset\Services\V1\GetPipelineDefinitionResponse;
use Udb\Core\Asset\Services\V1\GetPipelineRequest;
use Udb\Core\Asset\Services\V1\GetPipelineResponse;
use Udb\Core\Asset\Services\V1\ListAssetsRequest;
use Udb\Core\Asset\Services\V1\ListAssetsResponse;
use Udb\Core\Asset\Services\V1\RegisterAssetRequest;
use Udb\Core\Asset\Services\V1\RegisterAssetResponse;
use Udb\Core\Asset\Services\V1\StartPipelineRequest;
use Udb\Core\Asset\Services\V1\StartPipelineResponse;

/**
 * Convenience wrapper over the generated {@see AssetServiceClient}.
 * Shares the project's metadata + deadline; the raw stub remains reachable
 * via {@see client()} for RPCs not wrapped here.
 *
 * The `$steps`, `$context`, `$metadata` (asset) and `$result` parameters carry
 * serialized JSON as the broker expects (the proto fields are JSON strings).
 */
final class AssetService
{
    public function __construct(
        private readonly UdbProject $project,
        private readonly AssetServiceClient $stub,
    ) {
    }

    /** The raw generated stub, for RPCs without a convenience wrapper. */
    public function client(): AssetServiceClient
    {
        return $this->stub;
    }

    /**
     * Create a reusable pipeline definition. `$steps` is a JSON-encoded array of
     * step descriptors; `$mediaType` scopes the definition (e.g. "image").
     */
    public function createPipelineDefinition(
        string $name,
        string $mediaType,
        string $steps,
        string $description = '',
        int $version = 0,
        ?UdbMetadata $metadata = null,
    ): CreatePipelineDefinitionResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new CreatePipelineDefinitionRequest())
            ->setTenantId($meta->tenantId)
            ->setName($name)
            ->setDescription($description)
            ->setMediaType($mediaType)
            ->setSteps($steps)
            ->setVersion($version);

        return $this->project->invoke(
            'CreatePipelineDefinition',
            fn (array $md, array $o) => $this->stub->CreatePipelineDefinition($request, $md, $o),
            $metadata,
            CreatePipelineDefinitionResponse::class,
        );
    }

    /** Get a pipeline definition. */
    public function getPipelineDefinition(
        string $definitionId,
        ?UdbMetadata $metadata = null,
    ): GetPipelineDefinitionResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new GetPipelineDefinitionRequest())
            ->setTenantId($meta->tenantId)
            ->setDefinitionId($definitionId);

        return $this->project->invoke(
            'GetPipelineDefinition',
            fn (array $md, array $o) => $this->stub->GetPipelineDefinition($request, $md, $o),
            $metadata,
            GetPipelineDefinitionResponse::class,
        );
    }

    /**
     * Register a managed asset wrapping a storage file. `$metadataJson` is the
     * asset's serialized JSON metadata (named to avoid clashing with the shared
     * `$metadata` request-metadata parameter).
     */
    public function registerAsset(
        string $fileId,
        string $name,
        string $mediaType,
        string $metadataJson = '',
        ?UdbMetadata $metadata = null,
    ): RegisterAssetResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new RegisterAssetRequest())
            ->setTenantId($meta->tenantId)
            ->setProjectId($meta->projectId)
            ->setFileId($fileId)
            ->setName($name)
            ->setMediaType($mediaType)
            ->setMetadata($metadataJson);

        return $this->project->invoke(
            'RegisterAsset',
            fn (array $md, array $o) => $this->stub->RegisterAsset($request, $md, $o),
            $metadata,
            RegisterAssetResponse::class,
        );
    }

    /**
     * Start a pipeline instance for an asset. `$context` is JSON-encoded run
     * context; `$correlationId` defaults to the bound metadata correlation id.
     */
    public function startPipeline(
        string $definitionId,
        string $assetId,
        string $context = '',
        string $correlationId = '',
        ?UdbMetadata $metadata = null,
    ): StartPipelineResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new StartPipelineRequest())
            ->setTenantId($meta->tenantId)
            ->setDefinitionId($definitionId)
            ->setAssetId($assetId)
            ->setContext($context)
            ->setCorrelationId($correlationId !== '' ? $correlationId : $meta->correlationId);

        return $this->project->invoke(
            'StartPipeline',
            fn (array $md, array $o) => $this->stub->StartPipeline($request, $md, $o),
            $metadata,
            StartPipelineResponse::class,
        );
    }

    /** Get a pipeline instance with its steps. */
    public function getPipeline(string $instanceId, ?UdbMetadata $metadata = null): GetPipelineResponse
    {
        $meta = $this->project->metadata($metadata);
        $request = (new GetPipelineRequest())
            ->setTenantId($meta->tenantId)
            ->setInstanceId($instanceId);

        return $this->project->invoke(
            'GetPipeline',
            fn (array $md, array $o) => $this->stub->GetPipeline($request, $md, $o),
            $metadata,
            GetPipelineResponse::class,
        );
    }

    /**
     * Complete (or skip/fail) a pipeline step. `$status` is the step-status
     * string the broker expects; `$result` is JSON-encoded step output.
     */
    public function completeStep(
        string $stepId,
        string $status,
        string $result = '',
        string $errorMessage = '',
        ?UdbMetadata $metadata = null,
    ): CompleteStepResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new CompleteStepRequest())
            ->setTenantId($meta->tenantId)
            ->setStepId($stepId)
            ->setStatus($status)
            ->setResult($result)
            ->setErrorMessage($errorMessage);

        return $this->project->invoke(
            'CompleteStep',
            fn (array $md, array $o) => $this->stub->CompleteStep($request, $md, $o),
            $metadata,
            CompleteStepResponse::class,
        );
    }

    /** List assets, optionally filtered by media type / status. */
    public function listAssets(
        string $mediaType = '',
        string $status = '',
        int $page = 0,
        int $pageSize = 0,
        ?UdbMetadata $metadata = null,
    ): ListAssetsResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new ListAssetsRequest())
            ->setTenantId($meta->tenantId)
            ->setMediaType($mediaType)
            ->setStatus($status)
            ->setPage($page)
            ->setPageSize($pageSize);

        return $this->project->invoke(
            'ListAssets',
            fn (array $md, array $o) => $this->stub->ListAssets($request, $md, $o),
            $metadata,
            ListAssetsResponse::class,
        );
    }

    /** Get an asset. */
    public function getAsset(string $assetId, ?UdbMetadata $metadata = null): GetAssetResponse
    {
        $meta = $this->project->metadata($metadata);
        $request = (new GetAssetRequest())
            ->setTenantId($meta->tenantId)
            ->setAssetId($assetId);

        return $this->project->invoke(
            'GetAsset',
            fn (array $md, array $o) => $this->stub->GetAsset($request, $md, $o),
            $metadata,
            GetAssetResponse::class,
        );
    }
}
