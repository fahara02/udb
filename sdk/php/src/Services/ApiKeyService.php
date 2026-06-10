<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Services;

use Fahara02\UdbLaravel\UdbMetadata;
use Fahara02\UdbLaravel\UdbProject;
use Udb\Core\Apikey\Services\V1\ApiKeyServiceClient;
use Udb\Core\Apikey\Services\V1\CreateApiKeyRequest;
use Udb\Core\Apikey\Services\V1\CreateApiKeyResponse;
use Udb\Core\Apikey\Services\V1\RevokeApiKeyRequest;
use Udb\Core\Apikey\Services\V1\RevokeApiKeyResponse;

/**
 * Convenience wrapper over the generated {@see ApiKeyServiceClient}.
 *
 * Note: the broker exposes Create / Revoke (and Get/List/Update/Validate via
 * {@see client()}). There is no first-class "rotate" RPC, so {@see rotate()}
 * is the canonical revoke-old-then-create-new sequence performed client-side.
 */
final class ApiKeyService
{
    public function __construct(
        private readonly UdbProject $project,
        private readonly ApiKeyServiceClient $stub,
    ) {
    }

    /** The raw generated stub (Get/List/Update/Validate/UsageStats live here). */
    public function client(): ApiKeyServiceClient
    {
        return $this->stub;
    }

    /**
     * Create an API key.
     *
     * @param  list<string>  $scopes
     * @param  list<string>  $ipAllowlist
     */
    public function create(
        string $name,
        int $ownerType,
        string $ownerId,
        array $scopes = [],
        string $description = '',
        array $ipAllowlist = [],
        ?UdbMetadata $metadata = null,
    ): CreateApiKeyResponse {
        $request = (new CreateApiKeyRequest())
            ->setName($name)
            ->setDescription($description)
            ->setOwnerType($ownerType)
            ->setOwnerId($ownerId)
            ->setScopes($scopes)
            ->setIpAllowlist($ipAllowlist);

        return $this->project->invoke(
            'CreateApiKey',
            fn (array $md, array $o) => $this->stub->CreateApiKey($request, $md, $o),
            $metadata,
            CreateApiKeyResponse::class,
        );
    }

    public function createRequest(CreateApiKeyRequest $request, ?UdbMetadata $metadata = null): CreateApiKeyResponse
    {
        return $this->project->invoke(
            'CreateApiKey',
            fn (array $md, array $o) => $this->stub->CreateApiKey($request, $md, $o),
            $metadata,
            CreateApiKeyResponse::class,
        );
    }

    public function revoke(string $keyId, string $reason = '', ?UdbMetadata $metadata = null): RevokeApiKeyResponse
    {
        $request = (new RevokeApiKeyRequest())
            ->setKeyId($keyId)
            ->setRevokeReason($reason);

        return $this->project->invoke(
            'RevokeApiKey',
            fn (array $md, array $o) => $this->stub->RevokeApiKey($request, $md, $o),
            $metadata,
            RevokeApiKeyResponse::class,
        );
    }

    /**
     * Rotate a key: revoke the old one, then create a fresh key carrying the
     * supplied attributes. The broker has no atomic rotate RPC; this is the
     * standard two-step performed client-side. Returns the new key's response
     * (read the plaintext via `->getPlainKey()`).
     *
     * @param  list<string>  $scopes
     * @param  list<string>  $ipAllowlist
     */
    public function rotate(
        string $oldKeyId,
        string $name,
        int $ownerType,
        string $ownerId,
        array $scopes = [],
        string $description = '',
        array $ipAllowlist = [],
        string $revokeReason = 'rotated',
        ?UdbMetadata $metadata = null,
    ): CreateApiKeyResponse {
        $this->revoke($oldKeyId, $revokeReason, $metadata);

        return $this->create($name, $ownerType, $ownerId, $scopes, $description, $ipAllowlist, $metadata);
    }
}
