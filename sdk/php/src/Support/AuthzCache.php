<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Support;

use Udb\Core\Authz\Services\V1\Decision;

/**
 * In-process TTL cache for authorization decisions, in parity with the Go
 * (`authzCache`), Python (`AuthzCache`), and TypeScript (`AuthzCache`) SDKs.
 *
 * The cache key is composed of every input that can change the decision —
 * principal, tenant/project, resource, action, purpose, and the requested
 * scopes — so two callers with different scopes never share an entry.
 *
 * TTL is server-controlled: when a {@see Decision} carries
 * `cache_ttl_seconds > 0` that value wins. A client-side fallback TTL is
 * opt-in via the constructor and is only used when the server TTL is zero. A
 * non-positive effective TTL means "do not cache" (the decision is returned
 * but never stored). Denies are cached the same as allows — the broker decides
 * cacheability via the TTL.
 *
 * Not safe for cross-process sharing (it lives in PHP request/worker memory);
 * that matches the other SDKs' in-memory caches.
 */
final class AuthzCache
{
    /** @var array<string, array{decision: Decision, expiresAt: float}> */
    private array $entries = [];

    public function __construct(
        private readonly int $defaultTtlSeconds = 0,
        private readonly int $maxEntries = 4096,
    ) {
    }

    /**
     * Build a stable cache key from the decision inputs.
     *
     * @param  list<string>  $scopes
     */
    public static function key(
        string $principal,
        string $tenantId,
        string $projectId,
        string $resource,
        string $action,
        string $purpose,
        array $scopes,
    ): string {
        $sortedScopes = $scopes;
        sort($sortedScopes);

        return implode('|', [
            $principal,
            $tenantId,
            $projectId,
            $resource,
            $action,
            $purpose,
            implode(',', $sortedScopes),
        ]);
    }

    /** Return a live (non-expired) decision for the key, or null. */
    public function get(string $key): ?Decision
    {
        $entry = $this->entries[$key] ?? null;
        if ($entry === null) {
            return null;
        }
        if ($entry['expiresAt'] <= microtime(true)) {
            unset($this->entries[$key]);
            return null;
        }
        return $entry['decision'];
    }

    /**
     * Store a decision under the key honouring the server-controlled TTL.
     * A non-positive effective TTL skips caching entirely.
     */
    public function put(string $key, Decision $decision): void
    {
        $ttl = (int) $decision->getCacheTtlSeconds();
        if ($ttl <= 0) {
            $ttl = $this->defaultTtlSeconds;
        }
        if ($ttl <= 0) {
            return; // explicitly non-cacheable
        }

        // Bound memory: drop the oldest insertion when full.
        if (\count($this->entries) >= $this->maxEntries && ! isset($this->entries[$key])) {
            array_shift($this->entries);
        }

        $this->entries[$key] = [
            'decision' => $decision,
            'expiresAt' => microtime(true) + $ttl,
        ];
    }

    /** Drop every cached decision (e.g. after a policy-version bump). */
    public function flush(): void
    {
        $this->entries = [];
    }

    public function size(): int
    {
        return \count($this->entries);
    }
}
