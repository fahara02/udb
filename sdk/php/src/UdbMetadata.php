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
        // Consistency / read-your-writes context (parity with Python's Metadata).
        // Emitted as the x-udb-* headers in toGrpcMetadata(), omit-when-empty.
        public readonly string $consistency = '',
        public readonly bool $primaryRead = false,
        public readonly int $maxReplicaLagMs = 0,
        public readonly bool $eventualConsistencyAllowed = false,
        public readonly string $readFenceJson = '',
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
        // Consistency / read-your-writes headers — omitted when empty so an
        // unset value stays off the wire (matches Python's optional emission).
        if ($this->consistency !== '') {
            $metadata['x-udb-consistency'] = [$this->consistency];
        }
        if ($this->readFenceJson !== '') {
            $metadata['x-udb-read-fence'] = [$this->readFenceJson];
        }
        if ($this->primaryRead) {
            $metadata['x-udb-primary-read'] = ['true'];
        }
        if ($this->maxReplicaLagMs > 0) {
            $metadata['x-udb-max-replica-lag-ms'] = [(string) $this->maxReplicaLagMs];
        }
        if ($this->eventualConsistencyAllowed) {
            $metadata['x-udb-eventual-consistency-allowed'] = ['true'];
        }

        return $metadata;
    }

    /**
     * Build a {@see \Udb\Entity\V1\RequestContext} from this metadata, for the
     * RPCs that carry the context in the request body rather than (only) the
     * header bag — e.g. CDC `PublishCDC` / `EnqueueOutboxEvent`. Mirrors
     * Python's `Metadata.to_request_context`. Empty fields stay default.
     */
    public function toRequestContext(): \Udb\Entity\V1\RequestContext
    {
        $ctx = (new \Udb\Entity\V1\RequestContext())
            ->setTenantId($this->tenantId)
            ->setUserId($this->userId)
            ->setPurpose($this->purpose)
            ->setCorrelationId($this->correlationId)
            ->setScopes($this->scopes)
            ->setServiceIdentity($this->serviceIdentity)
            ->setProjectId($this->projectId)
            ->setClientCatalogVersion($this->clientCatalogVersion);
        if ($this->consistency !== '') {
            $ctx->setConsistency($this->consistency);
        }
        if ($this->primaryRead) {
            $ctx->setPrimaryRead(true);
        }
        if ($this->maxReplicaLagMs > 0) {
            $ctx->setMaxReplicaLagMs($this->maxReplicaLagMs);
        }
        if ($this->eventualConsistencyAllowed) {
            $ctx->setEventualConsistencyAllowed(true);
        }
        if ($this->readFenceJson !== '') {
            $ctx->setReadFenceJson($this->readFenceJson);
        }

        return $ctx;
    }

    /**
     * Return a new Metadata with the given purpose. Useful for
     * one-off overrides at a call site without redeclaring the
     * whole object — `$meta->withPurpose('billing.daily')`.
     */
    public function withPurpose(string $purpose): self
    {
        return $this->copyWith(purpose: $purpose);
    }

    /**
     * @param  list<string>  $scopes
     */
    public function withScopes(array $scopes): self
    {
        return $this->copyWith(scopes: $scopes);
    }

    public function withProjectId(string $projectId): self
    {
        return $this->copyWith(projectId: $projectId);
    }

    public function withCredentials(?string $bearerToken = null, ?string $apiKey = null): self
    {
        return $this->copyWith(
            bearerToken: $bearerToken ?? $this->bearerToken,
            apiKey: $apiKey ?? $this->apiKey,
        );
    }

    /**
     * Return a copy carrying a read-your-writes fence (`x-udb-read-fence`).
     * `$readFenceJson` is the serialized {@see ReadFence} (see
     * {@see ReadFence::toJson()}); empty clears the fence.
     */
    public function withReadFence(string $readFenceJson): self
    {
        return $this->copyWith(readFenceJson: $readFenceJson);
    }

    /**
     * Return a copy whose read fence is derived from a write receipt — the PHP
     * read-your-writes analogue of Python's `Metadata.after_write`. Maps the
     * receipt's `source_lsn` into the fence's `min_outbox_lsn`.
     */
    public function afterWrite(WriteReceipt $receipt, ?int $maxWaitMs = null): self
    {
        $fence = ReadFence::fromReceipt(
            $receipt,
            $maxWaitMs ?? ReadFence::DEFAULT_MAX_WAIT_MS,
        );

        return $this->withReadFence($fence->toJson());
    }

    /** Single reconstruction point so every `with*` carries all fields. */
    private function copyWith(
        ?string $purpose = null,
        ?array $scopes = null,
        ?string $projectId = null,
        ?string $bearerToken = null,
        ?string $apiKey = null,
        ?string $readFenceJson = null,
    ): self {
        return new self(
            tenantId: $this->tenantId,
            userId: $this->userId,
            purpose: $purpose ?? $this->purpose,
            correlationId: $this->correlationId,
            scopes: $scopes ?? $this->scopes,
            serviceIdentity: $this->serviceIdentity,
            projectId: $projectId ?? $this->projectId,
            clientCatalogVersion: $this->clientCatalogVersion,
            bearerToken: $bearerToken ?? $this->bearerToken,
            apiKey: $apiKey ?? $this->apiKey,
            consistency: $this->consistency,
            primaryRead: $this->primaryRead,
            maxReplicaLagMs: $this->maxReplicaLagMs,
            eventualConsistencyAllowed: $this->eventualConsistencyAllowed,
            readFenceJson: $readFenceJson ?? $this->readFenceJson,
        );
    }
}
