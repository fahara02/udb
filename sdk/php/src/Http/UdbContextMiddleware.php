<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Http;

use Closure;
use Fahara02\UdbLaravel\UdbClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Illuminate\Contracts\Container\Container;
use Illuminate\Http\Request;
use Symfony\Component\HttpFoundation\Response;

/**
 * Resolves the request's tenant / user / correlation id and
 * binds them to the singleton `UdbClient` so every RPC issued
 * during the request inherits them automatically.
 *
 * Registered globally on `web` and `api` groups when
 * `udb.middleware.auto_register=true` (the default). Operators
 * who want fine-grained control can opt out and mount it on
 * specific route groups manually.
 *
 * Resolution strategy (in order, first non-empty wins):
 *  1. The container binding named in `udb.middleware.tenant_resolver`
 *     (a closure: `fn (Request $request): ?string`). The default
 *     binding pulls from the authenticated user's `tenant_id`
 *     attribute / claim.
 *  2. The request header `X-Tenant-Id`.
 *  3. The config fallback `udb.metadata.default_tenant_id` (rare —
 *     usually a misconfiguration if reached).
 *
 * Correlation id follows the same pattern but defaults to a fresh
 * UUID v4 when nothing is provided, so distributed tracing always
 * has a join key.
 */
final class UdbContextMiddleware
{
    public function __construct(
        private readonly Container $container,
        private readonly UdbClient $client,
    ) {
    }

    public function handle(Request $request, Closure $next): Response
    {
        $tenantId = $this->resolve('udb.tenant_resolver', $request)
            ?? (string) $request->header('X-Tenant-Id', '');
        $userId = $this->resolve('udb.user_resolver', $request)
            ?? (string) $request->header('X-User-Id', '');
        $correlationId = $this->resolve('udb.correlation_resolver', $request)
            ?? (string) $request->header('X-Correlation-Id', '')
            ?: $this->generateCorrelationId();

        $metadata = UdbMetadata::fromContext(
            tenantId: $tenantId,
            userId: $userId,
            correlationId: $correlationId,
        )->withCredentials(
            bearerToken: $this->bearerToken((string) $request->header('Authorization', '')),
            apiKey: $this->nonBlank((string) $request->header('X-Api-Key', '')),
        );
        $this->client->bindContext($metadata);

        // Propagate correlation id to downstream observers via the
        // response header — gives operators a clickable trace id
        // in every HTTP response without app-level glue.
        $response = $next($request);
        if (! $response->headers->has('X-Correlation-Id')) {
            $response->headers->set('X-Correlation-Id', $correlationId);
        }
        return $response;
    }

    /**
     * Resolve one of the configured resolvers from the container.
     * Returns null when the binding is missing OR the closure
     * returns an empty string — both cases fall through to the
     * header fallback above.
     */
    private function resolve(string $bindingKey, Request $request): ?string
    {
        if (! $this->container->bound($bindingKey)) {
            return null;
        }
        $resolver = $this->container->make($bindingKey);
        if (! is_callable($resolver)) {
            return null;
        }
        $value = $resolver($request);
        return is_string($value) && $value !== '' ? $value : null;
    }

    private function generateCorrelationId(): string
    {
        // RFC 4122 v4 — same shape every other SDK emits so the
        // broker's audit ledger can index correlation ids
        // across-language without normalization.
        if (function_exists('random_bytes')) {
            $bytes = random_bytes(16);
            $bytes[6] = chr((ord($bytes[6]) & 0x0f) | 0x40);
            $bytes[8] = chr((ord($bytes[8]) & 0x3f) | 0x80);
            return vsprintf(
                '%s%s-%s-%s-%s-%s%s%s',
                str_split(bin2hex($bytes), 4),
            );
        }
        // PHP 8.1+ always has random_bytes; defensive fallback
        // for exotic ZTS builds.
        return sprintf('%08x-%04x-%04x-%04x-%012x', mt_rand(), mt_rand(0, 0xffff), mt_rand(0, 0x0fff) | 0x4000, mt_rand(0, 0x3fff) | 0x8000, mt_rand());
    }

    private function bearerToken(string $authorization): ?string
    {
        $authorization = trim($authorization);
        return strncasecmp($authorization, 'Bearer ', 7) === 0
            ? $this->nonBlank(substr($authorization, 7))
            : null;
    }

    private function nonBlank(string $value): ?string
    {
        $value = trim($value);
        return $value !== '' ? $value : null;
    }
}
