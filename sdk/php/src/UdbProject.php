<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel;

use Fahara02\UdbLaravel\Exceptions\UdbConfigurationException;
use Fahara02\UdbLaravel\Exceptions\UdbRpcException;
use Fahara02\UdbLaravel\Services\AnalyticsService;
use Fahara02\UdbLaravel\Services\ApiKeyService;
use Fahara02\UdbLaravel\Services\AssetService;
use Fahara02\UdbLaravel\Services\NotificationService;
use Fahara02\UdbLaravel\Services\StorageService;
use Fahara02\UdbLaravel\Services\TenantService;
use Fahara02\UdbLaravel\Services\WebRtcService;
use Fahara02\UdbLaravel\Support\ScopeJoiner;
use Grpc\ChannelCredentials;
use Udb\Core\Analytics\Services\V1\AnalyticsServiceClient;
use Udb\Core\Apikey\Services\V1\ApiKeyServiceClient;
use Udb\Core\Asset\Services\V1\AssetServiceClient;
use Udb\Core\Notification\Services\V1\NotificationServiceClient;
use Udb\Core\Storage\Services\V1\StorageServiceClient;
use Udb\Core\Tenant\Services\V1\TenantServiceClient;
use Udb\Core\Webrtc\Services\V1\PeerServiceClient;
use Udb\Core\Webrtc\Services\V1\RoomServiceClient;
use Udb\Core\Webrtc\Services\V1\SignalingServiceClient;
use Udb\Core\Webrtc\Services\V1\TrackServiceClient;
use Udb\Core\Webrtc\Services\V1\TurnServiceClient;

/**
 * Framework-agnostic unified facade over every UDB service whose generated
 * client ships in this SDK. One config bag, one shared {@see UdbMetadata}, one
 * gRPC channel per target. Distinct from the Laravel {@see Facades\Udb} facade
 * (which is a static proxy to the container-bound {@see UdbClient}); this class
 * is constructed directly — typically via {@see createUdb()} — and is the
 * portable parity surface for the Go/Python/TypeScript "client" objects.
 *
 *   $udb = createUdb([
 *       'target'    => '127.0.0.1:50051',
 *       'tenantId'  => 't_acme',
 *       'projectId' => 'default',
 *       'purpose'   => 'web.request',
 *       'scopes'    => ['read', 'write'],
 *   ]);
 *   $udb->authz()->require($resource, 'read');
 *   $udb->notification()->send('order.shipped', 'u_42');
 *
 * Only services with a generated client are exposed: data, auth, authz,
 * apiKey, tenant, notification, analytics, storage, asset, webRtc.
 *
 * @phpstan-type ProjectConfig array{
 *   target?: string,
 *   endpoint?: string,
 *   authTarget?: string,
 *   webrtcTarget?: string,
 *   tenantId?: string,
 *   userId?: string,
 *   serviceIdentity?: string,
 *   projectId?: string,
 *   purpose?: string,
 *   correlationId?: string,
 *   scopes?: list<string>,
 *   clientCatalogVersion?: string,
 *   credentials?: array{bearerToken?:string, apiKey?:string},
 *   bearerToken?: string,
 *   apiKey?: string,
 *   tls?: array{enabled?:bool, root_certs?:?string, target?:?string},
 *   retry?: array<string,mixed>,
 *   deadline?: int,
 *   deadline_ms?: int,
 *   authz_cache_ttl_ms?: int,
 *   policy_bundle_secret?: ?string,
 * }
 */
final class UdbProject
{
    /** @var ProjectConfig */
    private readonly array $config;
    private readonly string $dataTarget;
    private readonly string $authTarget;

    private ?UdbMetadata $boundContext = null;

    private ?UdbClient $data = null;
    private ?UdbAuthClient $auth = null;
    private ?NotificationService $notification = null;
    private ?ApiKeyService $apiKey = null;
    private ?TenantService $tenant = null;
    private ?AnalyticsService $analytics = null;
    private ?StorageService $storage = null;
    private ?AssetService $asset = null;
    private ?WebRtcService $webRtc = null;

    private ?NotificationServiceClient $notificationStub = null;
    private ?ApiKeyServiceClient $apiKeyStub = null;
    private ?TenantServiceClient $tenantStub = null;
    private ?AnalyticsServiceClient $analyticsStub = null;
    private ?StorageServiceClient $storageStub = null;
    private ?AssetServiceClient $assetStub = null;
    private ?RoomServiceClient $roomStub = null;
    private ?PeerServiceClient $peerStub = null;
    private ?TrackServiceClient $trackStub = null;
    private ?TurnServiceClient $turnStub = null;
    private ?SignalingServiceClient $signalingStub = null;

    /** @param ProjectConfig $config */
    public function __construct(array $config)
    {
        if (! \extension_loaded('grpc')) {
            throw new UdbConfigurationException('The "grpc" PHP extension is required for UDB.');
        }
        $dataTarget = trim((string) ($config['target'] ?? $config['endpoint'] ?? ''));
        if ($dataTarget === '') {
            throw new UdbConfigurationException('UDB target is not configured (set config["target"]).');
        }
        $this->config = $config;
        $this->dataTarget = $dataTarget;
        // Auth (authn/authz/apikey/tenant/notification/analytics) may live on a
        // separate control-plane listener; default it to the data target.
        $this->authTarget = trim((string) ($config['authTarget'] ?? $dataTarget)) ?: $dataTarget;
    }

    /**
     * Bind a request-scoped metadata; every service accessor inherits it.
     * When unset, accessors derive metadata from the config defaults.
     */
    public function bindContext(UdbMetadata $metadata): self
    {
        $this->boundContext = $metadata;
        $this->data?->bindContext($metadata);
        $this->auth?->bindContext($metadata);
        return $this;
    }

    /**
     * Resolve the metadata to use for a call: explicit override → bound
     * context → config-derived defaults.
     */
    public function metadata(?UdbMetadata $override = null): UdbMetadata
    {
        if ($override !== null) {
            return $override;
        }
        return $this->boundContext ??= $this->defaultMetadata();
    }

    /**
     * Hot-swap the bearer token / API key sent on every subsequent call across
     * all sub-clients. Call this after a login/refresh so the new token reaches
     * outbound metadata — the PHP analogue of the TypeScript
     * {@code core.setCredentials}. Passing null for an argument keeps the
     * current value. Re-binds the updated context so the cached data/auth
     * clients (and every accessor that funnels through {@see metadata()}) pick
     * up the new credentials on their next call.
     */
    public function setCredentials(?string $bearerToken = null, ?string $apiKey = null): UdbProject
    {
        $updated = $this->metadata()->withCredentials($bearerToken, $apiKey);

        return $this->bindContext($updated);
    }

    // ── Service accessors ────────────────────────────────────────────────────

    /** The CRUD data plane ({@see UdbClient}: select/upsert/delete/stub). */
    public function data(): UdbClient
    {
        if ($this->data === null) {
            $this->data = new UdbClient($this->dataClientConfig());
            $this->data->bindContext($this->metadata());
        }
        return $this->data;
    }

    /** Authentication + authorization ergonomics ({@see UdbAuthClient}). */
    public function auth(): UdbAuthClient
    {
        if ($this->auth === null) {
            $this->auth = new UdbAuthClient($this->authClientConfig());
            $this->auth->bindContext($this->metadata());
        }
        return $this->auth;
    }

    /** Alias for {@see auth()} — authorization-flavoured call sites. */
    public function authz(): UdbAuthClient
    {
        return $this->auth();
    }

    public function apiKey(): ApiKeyService
    {
        return $this->apiKey ??= new ApiKeyService(
            $this,
            $this->apiKeyStub ??= new ApiKeyServiceClient($this->authTarget, $this->channelOptions()),
        );
    }

    public function tenant(): TenantService
    {
        return $this->tenant ??= new TenantService(
            $this,
            $this->tenantStub ??= new TenantServiceClient($this->authTarget, $this->channelOptions()),
        );
    }

    public function notification(): NotificationService
    {
        return $this->notification ??= new NotificationService(
            $this,
            $this->notificationStub ??= new NotificationServiceClient($this->authTarget, $this->channelOptions()),
        );
    }

    public function analytics(): AnalyticsService
    {
        return $this->analytics ??= new AnalyticsService(
            $this,
            $this->analyticsStub ??= new AnalyticsServiceClient($this->authTarget, $this->channelOptions()),
        );
    }

    public function storage(): StorageService
    {
        return $this->storage ??= new StorageService(
            $this,
            $this->storageStub ??= new StorageServiceClient($this->authTarget, $this->channelOptions()),
        );
    }

    public function asset(): AssetService
    {
        return $this->asset ??= new AssetService(
            $this,
            $this->assetStub ??= new AssetServiceClient($this->authTarget, $this->channelOptions()),
        );
    }

    /**
     * Grouped WebRTC services (room/peer/track/turn/signaling). Honours an
     * optional `webrtcTarget` config override; otherwise uses the auth target.
     */
    public function webRtc(): WebRtcService
    {
        if ($this->webRtc !== null) {
            return $this->webRtc;
        }
        $target = trim((string) ($this->config['webrtcTarget'] ?? $this->authTarget)) ?: $this->authTarget;
        $opts = $this->channelOptions();

        return $this->webRtc = new WebRtcService(
            $this,
            $this->roomStub ??= new RoomServiceClient($target, $opts),
            $this->peerStub ??= new PeerServiceClient($target, $opts),
            $this->trackStub ??= new TrackServiceClient($target, $opts),
            $this->turnStub ??= new TurnServiceClient($target, $opts),
            $this->signalingStub ??= new SignalingServiceClient($target, $opts),
        );
    }

    // ── Shared invoke (used by the service wrappers) ──────────────────────────

    /**
     * Execute a unary RPC with shared metadata + deadline and map non-OK
     * status to {@see UdbRpcException}. Public so the per-service wrappers can
     * funnel through one consistent error path.
     *
     * @template T of object
     * @param  callable(array<string,list<string>>, array<string,mixed>):mixed  $invoker
     * @param  class-string<T>  $responseClass
     * @return T
     */
    public function invoke(string $rpcName, callable $invoker, ?UdbMetadata $metadata, string $responseClass): object
    {
        $meta = $this->metadata($metadata);
        $deadlineMs = $this->deadlineMs();
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

    // ── Internals ─────────────────────────────────────────────────────────────

    private function deadlineMs(): int
    {
        if (isset($this->config['deadline_ms'])) {
            return (int) $this->config['deadline_ms'];
        }
        if (isset($this->config['deadline'])) {
            // `deadline` is documented in milliseconds for cross-SDK parity.
            return (int) $this->config['deadline'];
        }
        return 30_000;
    }

    private function defaultMetadata(): UdbMetadata
    {
        /** @var list<string> $scopes */
        $scopes = array_values((array) ($this->config['scopes'] ?? []));

        /** @var array<string,mixed> $credentials */
        $credentials = (array) ($this->config['credentials'] ?? []);

        return new UdbMetadata(
            tenantId: (string) ($this->config['tenantId'] ?? ''),
            userId: (string) ($this->config['userId'] ?? ''),
            purpose: (string) ($this->config['purpose'] ?? 'web.request'),
            correlationId: (string) ($this->config['correlationId'] ?? ''),
            scopes: $scopes,
            serviceIdentity: (string) ($this->config['serviceIdentity'] ?? ''),
            projectId: (string) ($this->config['projectId'] ?? 'default'),
            clientCatalogVersion: (string) ($this->config['clientCatalogVersion'] ?? '1.0.0'),
            bearerToken: (string) ($credentials['bearerToken'] ?? $this->config['bearerToken'] ?? ''),
            apiKey: (string) ($credentials['apiKey'] ?? $this->config['apiKey'] ?? ''),
        );
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

    /** @return array{endpoint:string, tls?: array{enabled?:bool, root_certs?:?string, target?:?string}, deadline_ms:int} */
    private function dataClientConfig(): array
    {
        return [
            'endpoint' => $this->dataTarget,
            'tls' => (array) ($this->config['tls'] ?? []),
            'deadline_ms' => $this->deadlineMs(),
        ];
    }

    /** @return array{endpoint:string, tls?: array{enabled?:bool, root_certs?:?string}, deadline_ms:int, authz_cache_ttl_ms:int, policy_bundle_secret:?string} */
    private function authClientConfig(): array
    {
        return [
            'endpoint' => $this->authTarget,
            'tls' => (array) ($this->config['tls'] ?? []),
            'deadline_ms' => $this->deadlineMs(),
            'authz_cache_ttl_ms' => (int) ($this->config['authz_cache_ttl_ms'] ?? 30_000),
            'policy_bundle_secret' => isset($this->config['policy_bundle_secret'])
                ? (string) $this->config['policy_bundle_secret']
                : null,
        ];
    }

    /** Internal: comma-joined scopes for diagnostics/logging. */
    public function scopeHeader(): string
    {
        return ScopeJoiner::join(array_values((array) ($this->config['scopes'] ?? [])));
    }
}
