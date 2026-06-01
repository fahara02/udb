<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel;

use Fahara02\UdbLaravel\Exceptions\UdbConfigurationException;
use Fahara02\UdbLaravel\Exceptions\UdbRpcException;
use Grpc\ChannelCredentials;
use Udb\Core\Authn\Services\V1\AuthnRequest;
use Udb\Core\Authn\Services\V1\AuthnResponse;
use Udb\Core\Authn\Services\V1\AuthnServiceClient;
use Udb\Core\Authz\Services\V1\AuthzRequest;
use Udb\Core\Authz\Services\V1\AuthzServiceClient;
use Udb\Core\Authz\Services\V1\Decision;
use Udb\Core\Authz\Services\V1\NativeAccessGrant;
use Udb\Core\Authz\Services\V1\NativeAccessRequest;
use Udb\Core\Authz\Services\V1\NativeAccessResponse;
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

    /** @param array{endpoint:string, tls?: array{enabled?:bool, root_certs?:?string}, deadline_ms?: int} $config */
    public function __construct(private readonly array $config)
    {
        if (! \extension_loaded('grpc')) {
            throw new UdbConfigurationException('The "grpc" PHP extension is required for UDB auth.');
        }
        if (trim((string) ($config['endpoint'] ?? '')) === '') {
            throw new UdbConfigurationException('UDB endpoint is not configured (set UDB_ENDPOINT).');
        }
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

    /** Returns [bool allowed, Decision] for the request principal on a resource. */
    public function can(ResourceRef $resource, string $action, string $purpose = '', ?UdbMetadata $metadata = null): array
    {
        $meta = $this->context($metadata);
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
            ->setPurpose($purpose !== '' ? $purpose : $meta->purpose);
        $decision = $this->authorize($request, $metadata);
        return [$decision->getAllowed(), $decision];
    }

    // ── Stage 2: native database fast-path access (item 138) ──────────────────
    /**
     * Authorize and, when allowed, return the native-access grant (restricted
     * role + scoped DSN + RLS session variables). Returns null when access is
     * allowed but no grant was minted; throws on deny.
     */
    public function nativeAccess(ResourceRef $resource, string $action, string $purpose = '', ?UdbMetadata $metadata = null): ?NativeAccessGrant
    {
        $meta = $this->context($metadata);
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
            ->setPurpose($purpose !== '' ? $purpose : $meta->purpose);
        /** @var NativeAccessResponse $resp */
        $resp = $this->invoke('GetNativeAccess', fn (array $md, array $o) => $this->authz()->GetNativeAccess($request, $md, $o), $metadata, NativeAccessResponse::class);
        $decision = $resp->getDecision();
        if ($decision !== null && ! $decision->getAllowed()) {
            throw new UdbRpcException(status: 7, details: 'udb: native access denied: '.$decision->getDenyReason(), raw: null, rpcName: 'GetNativeAccess');
        }
        return $resp->getGrant();
    }

    // ── Stage 2: signed policy bundle (item 140) ──────────────────────────────
    public function getPolicyBundle(?UdbMetadata $metadata = null): ?SignedPolicyBundle
    {
        $meta = $this->context($metadata);
        $request = (new PolicyBundleRequest())->setTenantId($meta->tenantId)->setProjectId($meta->projectId ?? '');
        /** @var PolicyBundleResponse $resp */
        $resp = $this->invoke('GetPolicyBundle', fn (array $md, array $o) => $this->authz()->GetPolicyBundle($request, $md, $o), $metadata, PolicyBundleResponse::class);
        return $resp->getBundle();
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
