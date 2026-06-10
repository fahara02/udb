<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel;

use Fahara02\UdbLaravel\Exceptions\UdbAuthzDeniedException;
use Fahara02\UdbLaravel\Exceptions\UdbConfigurationException;
use Fahara02\UdbLaravel\Exceptions\UdbPolicyBundleSignatureException;
use Fahara02\UdbLaravel\Exceptions\UdbRpcException;
use Fahara02\UdbLaravel\Support\AuthzCache;
use Grpc\ChannelCredentials;
use Udb\Core\Authn\Services\V1\AuthnRequest;
use Udb\Core\Authn\Services\V1\AuthnResponse;
use Udb\Core\Authn\Services\V1\AuthnServiceClient;
use Udb\Core\Authz\Services\V1\AuthzRequest;
use Udb\Core\Authz\Services\V1\AuthzServiceClient;
use Udb\Core\Authz\Services\V1\BatchCheckPermissionsRequest;
use Udb\Core\Authz\Services\V1\BatchCheckPermissionsResponse;
use Udb\Core\Authz\Services\V1\Decision;
use Udb\Core\Authz\Services\V1\NativeAccessGrant;
use Udb\Core\Authz\Services\V1\NativeAccessRequest;
use Udb\Core\Authz\Services\V1\NativeAccessResponse;
use Udb\Core\Authz\Services\V1\PermissionCheck;
use Udb\Core\Authz\Services\V1\PolicyBundleRequest;
use Udb\Core\Authz\Services\V1\PolicyBundleResponse;
use Udb\Core\Authz\Services\V1\Principal;
use Udb\Core\Authz\Services\V1\ResourceRef;
use Udb\Core\Authz\Services\V1\SignedPolicyBundle;

/**
 * Hand-written auth ergonomics over the generated AuthnService / AuthzService
 * stubs, mirroring {@see UdbClient}'s metadata + unary-invoke convention
 * (item 109). Share a single instance per request; bind the request context
 * with {@see bindContext()} (the Laravel middleware does this automatically).
 */
class UdbAuthClient
{
    private ?UdbMetadata $boundContext = null;
    private ?AuthnServiceClient $authn = null;
    private ?AuthzServiceClient $authz = null;
    private readonly AuthzCache $cache;

    /** @param array{endpoint:string, tls?: array{enabled?:bool, root_certs?:?string}, deadline_ms?: int, authz_cache_ttl_ms?: int, policy_bundle_secret?: ?string} $config */
    public function __construct(private readonly array $config)
    {
        if (! \extension_loaded('grpc')) {
            throw new UdbConfigurationException('The "grpc" PHP extension is required for UDB auth.');
        }
        if (trim((string) ($config['endpoint'] ?? '')) === '') {
            throw new UdbConfigurationException('UDB endpoint is not configured (set UDB_ENDPOINT).');
        }
        $defaultTtl = (int) ceil(((int) ($config['authz_cache_ttl_ms'] ?? 0)) / 1000);
        $this->cache = new AuthzCache($defaultTtl > 0 ? $defaultTtl : 0);
    }

    /** Direct access to the in-process authorization decision cache. */
    public function authzCache(): AuthzCache
    {
        return $this->cache;
    }

    public function bindContext(UdbMetadata $metadata): void
    {
        $this->boundContext = $metadata;
    }

    private function context(?UdbMetadata $metadata): UdbMetadata
    {
        $meta = $metadata ?? $this->boundContext;
        if ($meta === null) {
            throw new UdbConfigurationException('No UDB request context is bound; pass metadata or call bindContext().');
        }
        return $meta;
    }

    // ── Authentication ──────────────────────────────────────────────────────
    public function authenticate(AuthnRequest $request, ?UdbMetadata $metadata = null): AuthnResponse
    {
        return $this->invoke('Authenticate', fn (array $md, array $o) => $this->authn()->Authenticate($request, $md, $o), $metadata, AuthnResponse::class);
    }

    public function authenticateBearer(string $token, ?UdbMetadata $metadata = null): AuthnResponse
    {
        $meta = $this->context($metadata);
        return $this->authenticate(
            (new AuthnRequest())->setBearerToken($token)->setTenantHint($meta->tenantId)->setProjectHint($meta->projectId ?? ''),
            $metadata,
        );
    }

    public function authenticateApiKey(string $apiKey, ?UdbMetadata $metadata = null): AuthnResponse
    {
        $meta = $this->context($metadata);
        return $this->authenticate(
            (new AuthnRequest())->setApiKey($apiKey)->setTenantHint($meta->tenantId)->setProjectHint($meta->projectId ?? ''),
            $metadata,
        );
    }

    /**
     * Forward an already-authenticated Laravel/app identity to UDB as an
     * external principal (JSON claims or a verified token). UDB maps the claims
     * to a principal; UDB policy — not the app's roles — decides authorization.
     */
    public function authenticateExternal(string $token, string $providerId = 'laravel', ?UdbMetadata $metadata = null): AuthnResponse
    {
        $meta = $this->context($metadata);
        return $this->authenticate(
            (new AuthnRequest())->setExternalProviderId($providerId)->setExternalToken($token)
                ->setTenantHint($meta->tenantId)->setProjectHint($meta->projectId ?? ''),
            $metadata,
        );
    }

    // ── Authorization ─────────────────────────────────────────────────────────
    public function authorize(AuthzRequest $request, ?UdbMetadata $metadata = null): Decision
    {
        $resp = $this->invoke('Authorize', fn (array $md, array $o) => $this->authz()->Authorize($request, $md, $o), $metadata, \Udb\Core\Authz\Services\V1\AuthzResponse::class);
        return $resp->getDecision() ?? new Decision();
    }

    /**
     * Returns [bool allowed, Decision] for the request principal on a resource.
     *
     * `requested_scopes` is populated on the AuthzRequest from the explicit
     * `$scopes` argument when given, otherwise from the bound metadata scopes —
     * achieving parity with the Go/Python/TypeScript `can()` implementations.
     * Decisions are served from (and stored in) the server-TTL'd AuthzCache.
     *
     * @param  list<string>|null  $scopes
     * @return array{0: bool, 1: Decision}
     */
    public function can(ResourceRef $resource, string $action, string $purpose = '', ?UdbMetadata $metadata = null, ?array $scopes = null): array
    {
        $decision = $this->decide($resource, $action, $purpose, $metadata, $scopes);
        return [$decision->getAllowed(), $decision];
    }

    /**
     * Core decision path shared by {@see can()}, {@see require()}, and
     * {@see explain()}: builds the AuthzRequest with requested_scopes, consults
     * the cache, falls back to the Authorize RPC, then caches the result.
     *
     * @param  list<string>|null  $scopes
     */
    private function decide(ResourceRef $resource, string $action, string $purpose, ?UdbMetadata $metadata, ?array $scopes): Decision
    {
        $meta = $this->context($metadata);
        $requestedScopes = $scopes ?? ($meta->scopes ?? []);
        $effectivePurpose = $purpose !== '' ? $purpose : $meta->purpose;

        $principalId = ($meta->userId ?? '') !== '' ? (string) $meta->userId : (string) ($meta->serviceIdentity ?? '');
        $cacheKey = AuthzCache::key(
            $principalId,
            $meta->tenantId,
            $meta->projectId ?? '',
            self::resourceKey($resource),
            $action,
            $effectivePurpose,
            $requestedScopes,
        );
        $cached = $this->cache->get($cacheKey);
        if ($cached !== null) {
            return $cached;
        }

        $principal = (new Principal())
            ->setUserId($meta->userId ?? '')
            ->setServiceIdentity($meta->serviceIdentity ?? '')
            ->setTenantId($meta->tenantId)
            ->setProjectId($meta->projectId ?? '')
            ->setScopes($meta->scopes ?? []);
        $request = (new AuthzRequest())
            ->setPrincipal($principal)
            ->setTenantId($meta->tenantId)
            ->setProjectId($meta->projectId ?? '')
            ->setResource($resource)
            ->setAction($action)
            ->setPurpose($effectivePurpose)
            ->setRequestedScopes($requestedScopes);
        $decision = $this->authorize($request, $metadata);
        $this->cache->put($cacheKey, $decision);
        return $decision;
    }

    /**
     * Authorize and throw {@see UdbAuthzDeniedException} on deny. On allow,
     * returns the {@see Decision} for callers that want the matched policy
     * ids / required scopes.
     *
     * @param  list<string>|null  $scopes
     */
    public function require(ResourceRef $resource, string $action, string $purpose = '', ?UdbMetadata $metadata = null, ?array $scopes = null): Decision
    {
        $decision = $this->decide($resource, $action, $purpose, $metadata, $scopes);
        if (! $decision->getAllowed()) {
            throw new UdbAuthzDeniedException(
                resource: self::resourceKey($resource),
                action: $action,
                decision: $decision,
                purpose: $purpose,
            );
        }
        return $decision;
    }

    /**
     * Return the {@see Decision} for a check without throwing — the
     * non-raising counterpart to {@see require()}, for UIs that gate features
     * on `->getAllowed()` and surface `->getDenyReason()`.
     *
     * @param  list<string>|null  $scopes
     */
    public function explain(ResourceRef $resource, string $action, string $purpose = '', ?UdbMetadata $metadata = null, ?array $scopes = null): Decision
    {
        return $this->decide($resource, $action, $purpose, $metadata, $scopes);
    }

    /**
     * Batch many object/action checks in a single BatchCheckPermissions RPC.
     * Each input is `['object' => string, 'action' => string]`; the returned
     * map is `"object:action" => bool` exactly as the broker keys its results.
     *
     * @param  list<array{object:string, action:string}>  $checks
     * @return array<string, bool>
     */
    public function batchCan(array $checks, ?UdbMetadata $metadata = null): array
    {
        $meta = $this->context($metadata);
        $userId = ($meta->userId ?? '') !== '' ? (string) $meta->userId : (string) ($meta->serviceIdentity ?? '');

        $permChecks = [];
        foreach ($checks as $check) {
            $permChecks[] = (new PermissionCheck())
                ->setObject((string) $check['object'])
                ->setAction((string) $check['action']);
        }
        $request = (new BatchCheckPermissionsRequest())
            ->setUserId($userId)
            ->setDomain($meta->tenantId)
            ->setChecks($permChecks);

        /** @var BatchCheckPermissionsResponse $resp */
        $resp = $this->invoke('BatchCheckPermissions', fn (array $md, array $o) => $this->authz()->BatchCheckPermissions($request, $md, $o), $metadata, BatchCheckPermissionsResponse::class);

        $out = [];
        foreach ($resp->getResults() as $key => $allowed) {
            $out[(string) $key] = (bool) $allowed;
        }
        return $out;
    }

    /** Stable string identity for a ResourceRef, used in the cache key. */
    private static function resourceKey(ResourceRef $r): string
    {
        return implode(':', [
            $r->getResourceType(),
            $r->getMessageType(),
            $r->getBackend(),
            $r->getInstance(),
            $r->getSchema(),
            $r->getTable(),
            $r->getCollection(),
            $r->getBucket(),
            $r->getPath(),
            $r->getResourceId(),
            $r->getResourceName(),
        ]);
    }

    // ── Stage 2: native database fast-path access (item 138) ──────────────────
    /**
     * Authorize and, when allowed, return the native-access grant (restricted
     * role + scoped DSN + RLS session variables). Returns null when access is
     * allowed but no grant was minted; throws on deny.
     */
    public function nativeAccess(ResourceRef $resource, string $action, string $purpose = '', ?UdbMetadata $metadata = null, ?array $scopes = null): ?NativeAccessGrant
    {
        $meta = $this->context($metadata);
        $requestedScopes = $scopes ?? ($meta->scopes ?? []);
        $principal = (new Principal())
            ->setUserId($meta->userId ?? '')
            ->setServiceIdentity($meta->serviceIdentity ?? '')
            ->setTenantId($meta->tenantId)
            ->setProjectId($meta->projectId ?? '')
            ->setScopes($meta->scopes ?? []);
        $request = (new NativeAccessRequest())
            ->setPrincipal($principal)
            ->setTenantId($meta->tenantId)
            ->setProjectId($meta->projectId ?? '')
            ->setResource($resource)
            ->setAction($action)
            ->setPurpose($purpose !== '' ? $purpose : $meta->purpose)
            ->setRequestedScopes($requestedScopes);
        /** @var NativeAccessResponse $resp */
        $resp = $this->invoke('GetNativeAccess', fn (array $md, array $o) => $this->authz()->GetNativeAccess($request, $md, $o), $metadata, NativeAccessResponse::class);
        $decision = $resp->getDecision();
        if ($decision !== null && ! $decision->getAllowed()) {
            throw new UdbRpcException(status: 7, details: 'udb: native access denied: '.$decision->getDenyReason(), raw: null, rpcName: 'GetNativeAccess');
        }
        return $resp->getGrant();
    }

    // ── Stage 2: signed policy bundle (item 140) ──────────────────────────────
    /**
     * Fetch the tenant/project-scoped {@see SignedPolicyBundle}. When a bundle
     * secret is configured (`policy_bundle_secret` in config), the bundle's
     * HMAC-SHA256 signature is verified before it is returned; a mismatch
     * raises {@see UdbPolicyBundleSignatureException} so an unverified bundle is
     * never handed back for local `can()` evaluation (fail-closed).
     */
    public function getPolicyBundle(?UdbMetadata $metadata = null): ?SignedPolicyBundle
    {
        $meta = $this->context($metadata);
        $request = (new PolicyBundleRequest())->setTenantId($meta->tenantId)->setProjectId($meta->projectId ?? '');
        /** @var PolicyBundleResponse $resp */
        $resp = $this->invoke('GetPolicyBundle', fn (array $md, array $o) => $this->authz()->GetPolicyBundle($request, $md, $o), $metadata, PolicyBundleResponse::class);
        $bundle = $resp->getBundle();

        $secret = (string) ($this->config['policy_bundle_secret'] ?? '');
        if ($bundle !== null && $secret !== '' && ! $this->verifyPolicyBundle($bundle, $secret)) {
            throw new UdbPolicyBundleSignatureException(
                keyId: (string) $bundle->getKeyId(),
                algorithm: (string) $bundle->getAlgorithm(),
            );
        }

        return $bundle;
    }

    /**
     * Verify a {@see SignedPolicyBundle}'s signature by recomputing the keyed
     * HMAC over the raw `bundle` payload and constant-time comparing it to the
     * signature the broker emitted.
     *
     * The broker signs with `lowercase-hex(HMAC-SHA256(secret, bundle))` (see
     * `runtime::authz::bundle`). Although the proto comment labels `signature`
     * as Base64, the server emits lowercase hex; we match the server.
     *
     * Uses {@see hash_equals()} for a timing-safe comparison so a partial-match
     * oracle cannot be built from response timing.
     */
    public function verifyPolicyBundle(SignedPolicyBundle $signed, string $secret): bool
    {
        if ($secret === '') {
            return false;
        }
        $expected = hash_hmac('sha256', (string) $signed->getBundle(), $secret);

        return hash_equals($expected, (string) $signed->getSignature());
    }

    /**
     * Run a transaction on a PDO connection opened with the grant DSN, applying
     * the grant's app.current_* session variables so RLS sees the broker's
     * request context. Commits on success, rolls back on error.
     *
     * @template T
     * @param  callable(\PDO):T  $fn
     * @return T
     */
    public static function withNativeTx(\PDO $pdo, NativeAccessGrant $grant, callable $fn): mixed
    {
        $pdo->beginTransaction();
        try {
            $stmt = $pdo->prepare('SELECT set_config(?, ?, true)');
            foreach ($grant->getSessionVariables() as $key => $value) {
                $stmt->execute([$key, $value]);
            }
            $result = $fn($pdo);
            $pdo->commit();
            return $result;
        } catch (\Throwable $e) {
            $pdo->rollBack();
            throw $e;
        }
    }

    // ── Internals ───────────────────────────────────────────────────────────
    private function authn(): AuthnServiceClient
    {
        return $this->authn ??= new AuthnServiceClient((string) $this->config['endpoint'], $this->channelOptions());
    }

    private function authz(): AuthzServiceClient
    {
        return $this->authz ??= new AuthzServiceClient((string) $this->config['endpoint'], $this->channelOptions());
    }

    /** @return array<string,mixed> */
    private function channelOptions(): array
    {
        $tls = (array) ($this->config['tls'] ?? []);
        if ((bool) ($tls['enabled'] ?? false)) {
            $root = $tls['root_certs'] ?? null;
            $pem = $root !== null ? (string) file_get_contents((string) $root) : null;
            return ['credentials' => ChannelCredentials::createSsl($pem)];
        }
        return ['credentials' => ChannelCredentials::createInsecure()];
    }

    /**
     * @template T of object
     * @param  callable(array<string,list<string>>, array<string,mixed>):mixed  $invoker
     * @param  class-string<T>  $responseClass
     * @return T
     */
    private function invoke(string $rpcName, callable $invoker, ?UdbMetadata $metadata, string $responseClass): object
    {
        $meta = $this->context($metadata);
        $deadlineMs = (int) ($this->config['deadline_ms'] ?? 30_000);
        $opts = $deadlineMs > 0 ? ['timeout' => $deadlineMs * 1000] : [];
        /** @var \Grpc\UnaryCall $call */
        $call = $invoker($meta->toGrpcMetadata(), $opts);
        [$response, $status] = $call->wait();
        $code = is_object($status) ? ($status->code ?? -1) : ($status['code'] ?? -1);
        if ($code !== 0) {
            throw UdbRpcException::fromGrpcStatus($status, $rpcName);
        }
        if (! $response instanceof $responseClass) {
            throw new UdbRpcException(status: 13, details: "UDB {$rpcName} returned an unexpected response type", raw: $status, rpcName: $rpcName);
        }
        return $response;
    }
}
