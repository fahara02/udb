<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel;

use Fahara02\UdbLaravel\Support\ScopeJoiner;

/**
 * Immutable value object carrying the 8 broker-required metadata
 * headers. Constructed by `UdbClient` per-request from the bound
 * context + the static service identity from `config/udb.php`.
 *
 * Matches the canonical SDK contract:
 *  - x-tenant-id
 *  - x-user-id
 *  - x-purpose
 *  - x-correlation-id
 *  - x-scopes              (comma-joined)
 *  - x-service-identity
 *  - x-udb-project-id
 *  - x-udb-client-catalog-version
 *
 * Equivalent to Go's `udbclient.Metadata`, Python's `Metadata`,
 * TypeScript's `UdbMetadata`. Kept in parity so SDK behaviour is
 * predictable across languages.
 */
final class UdbMetadata
{
    /**
     * @param  list<string>  $scopes
     */
    public function __construct(
        public readonly string $tenantId,
        public readonly string $userId,
        public readonly string $purpose,
        public readonly string $correlationId,
        public readonly array $scopes,
        public readonly string $serviceIdentity,
        public readonly string $projectId,
        public readonly string $clientCatalogVersion,
        public readonly string $bearerToken = '',
        public readonly string $apiKey = '',
    ) {
    }

    /**
     * Build a default Metadata from the application config + a
     * supplied tenant/user/correlation. Used by `UdbClient` when no
     * explicit Metadata is passed to a call — keeps call sites
     * concise for the common case.
     *
     * @param  list<string>|null  $scopes
     */
    public static function fromContext(
        string $tenantId,
        string $userId,
        string $correlationId,
        ?string $purpose = null,
        ?array $scopes = null,
        ?string $projectId = null,
    ): self {
        /** @var array<string, mixed> $meta */
        $meta = (array) config('udb.metadata', []);

        return new self(
            tenantId: $tenantId,
            userId: $userId,
            purpose: $purpose ?? (string) ($meta['default_purpose'] ?? 'web.request'),
            correlationId: $correlationId,
            scopes: $scopes ?? (array) ($meta['default_scopes'] ?? []),
            serviceIdentity: (string) ($meta['service_identity'] ?? 'laravel.app'),
            projectId: $projectId ?? (string) ($meta['default_project_id'] ?? 'default'),
            clientCatalogVersion: (string) ($meta['client_catalog_version'] ?? '1.0.0'),
            bearerToken: (string) ($meta['bearer_token'] ?? ''),
            apiKey: (string) ($meta['api_key'] ?? ''),
        );
    }

    /**
     * Render as the `array<string, list<string>>` shape gRPC's PHP
     * extension expects for `_simpleRequest`'s `$metadata`
     * parameter. gRPC sends one wire header per element of the
     * inner list; UDB's broker reads only the first.
     *
     * @return array<string, list<string>>
     */
    public function toGrpcMetadata(): array
    {
        $metadata = [
            'x-tenant-id'                   => [$this->tenantId],
            'x-user-id'                     => [$this->userId],
            'x-purpose'                     => [$this->purpose],
            'x-correlation-id'              => [$this->correlationId],
            'x-scopes'                      => [ScopeJoiner::join($this->scopes)],
            'x-service-identity'            => [$this->serviceIdentity],
            'x-udb-project-id'              => [$this->projectId],
            'x-udb-client-catalog-version'  => [$this->clientCatalogVersion],
        ];
        if ($this->bearerToken !== '') {
            $metadata['authorization'] = ['Bearer '.$this->bearerToken];
        }
        if ($this->apiKey !== '') {
            $metadata['x-api-key'] = [$this->apiKey];
        }

        return $metadata;
    }

    /**
     * Return a new Metadata with the given purpose. Useful for
     * one-off overrides at a call site without redeclaring the
     * whole object — `$meta->withPurpose('billing.daily')`.
     */
    public function withPurpose(string $purpose): self
    {
        return new self(
            tenantId: $this->tenantId,
            userId: $this->userId,
            purpose: $purpose,
            correlationId: $this->correlationId,
            scopes: $this->scopes,
            serviceIdentity: $this->serviceIdentity,
            projectId: $this->projectId,
            clientCatalogVersion: $this->clientCatalogVersion,
            bearerToken: $this->bearerToken,
            apiKey: $this->apiKey,
        );
    }

    /**
     * @param  list<string>  $scopes
     */
    public function withScopes(array $scopes): self
    {
        return new self(
            tenantId: $this->tenantId,
            userId: $this->userId,
            purpose: $this->purpose,
            correlationId: $this->correlationId,
            scopes: $scopes,
            serviceIdentity: $this->serviceIdentity,
            projectId: $this->projectId,
            clientCatalogVersion: $this->clientCatalogVersion,
            bearerToken: $this->bearerToken,
            apiKey: $this->apiKey,
        );
    }

    public function withProjectId(string $projectId): self
    {
        return new self(
            tenantId: $this->tenantId,
            userId: $this->userId,
            purpose: $this->purpose,
            correlationId: $this->correlationId,
            scopes: $this->scopes,
            serviceIdentity: $this->serviceIdentity,
            projectId: $projectId,
            clientCatalogVersion: $this->clientCatalogVersion,
            bearerToken: $this->bearerToken,
            apiKey: $this->apiKey,
        );
    }

    public function withCredentials(?string $bearerToken = null, ?string $apiKey = null): self
    {
        return new self(
            tenantId: $this->tenantId,
            userId: $this->userId,
            purpose: $this->purpose,
            correlationId: $this->correlationId,
            scopes: $this->scopes,
            serviceIdentity: $this->serviceIdentity,
            projectId: $this->projectId,
            clientCatalogVersion: $this->clientCatalogVersion,
            bearerToken: $bearerToken ?? $this->bearerToken,
            apiKey: $apiKey ?? $this->apiKey,
        );
    }
}
