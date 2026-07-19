<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel;

use Fahara02\UdbLaravel\Exceptions\UdbConfigurationException;
use Fahara02\UdbLaravel\Exceptions\UdbRpcException;
use Fahara02\UdbLaravel\Services\AnalyticsService;
use Fahara02\UdbLaravel\Services\ApiKeyService;
use Fahara02\UdbLaravel\Services\AssetService;
use Fahara02\UdbLaravel\Services\EventsService;
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
    private ?EventsService $events = null;

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

    /**
     * Canonical static factory (naming-contract `UdbProject::connect($config)`,
     * parity with TS `UdbProject.connect`, Python `UdbProject.connect`, Go
     * `udbclient.Connect`). Equivalent to `new UdbProject($config)` /
     * {@see createUdb()}; the constructor + `createUdb()` stay as aliases.
     *
     * @param  ProjectConfig  $config
     */
    public static function connect(array $config): UdbProject
    {
        return new self($config);
    }

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
        $initial = $this->defaultMetadata();
        if ($initial->apiKey !== '' && $initial->bearerToken === '') {
            $this->bindContext($initial);
            $this->authenticateApiKeyAndAdopt($initial->apiKey);
        }
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
        $updated = $this->metadata()->withCredentials(
            $bearerToken,
            ($bearerToken !== null && trim($bearerToken) !== '') ? '' : $apiKey,
        );

        return $this->bindContext($updated);
    }

    public function authenticateApiKeyAndAdopt(string $apiKey = '', ?UdbMetadata $metadata = null): \Udb\Core\Authn\Services\V1\AuthnResponse
    {
        $key = trim($apiKey) !== '' ? $apiKey : $this->metadata()->apiKey;
        if (trim($key) === '') {
            throw new UdbConfigurationException('api key is required');
        }

        $authn = $this->auth()->authenticateApiKey($key, $metadata);
        $token = $authn->getAccessToken();
        if ($token === '') {
            throw new UdbConfigurationException('authenticateApiKey returned no access token');
        }
        $principal = $authn->getPrincipal();
        if ($principal === null) {
            throw new UdbConfigurationException('authenticateApiKey returned no principal');
        }

        $current = $this->metadata();
        $scopes = [];
        foreach ($principal->getScopes() as $scope) {
            $scopes[] = (string) $scope;
        }
        $adopted = new UdbMetadata(
            tenantId: $principal->getTenantId() !== '' ? $principal->getTenantId() : $current->tenantId,
            userId: $principal->getUserId() !== '' ? $principal->getUserId() : $current->userId,
            purpose: $current->purpose,
            correlationId: $current->correlationId,
            scopes: $scopes,
            serviceIdentity: $principal->getServiceIdentity() !== '' ? $principal->getServiceIdentity() : $current->serviceIdentity,
            projectId: $principal->getProjectId() !== '' ? $principal->getProjectId() : $current->projectId,
            clientCatalogVersion: $current->clientCatalogVersion,
            bearerToken: $token,
            apiKey: '',
            consistency: $current->consistency,
            primaryRead: $current->primaryRead,
            maxReplicaLagMs: $current->maxReplicaLagMs,
            eventualConsistencyAllowed: $current->eventualConsistencyAllowed,
            readFenceJson: $current->readFenceJson,
        );
        $this->bindContext($adopted);

        return $authn;
    }
    /**
     * Login, then adopt the canonical tenant/project from the VERIFIED principal.
     *
     * Canonical 2-RPC sequence (D11), ALWAYS two RPCs: `Login` →
     * `Authenticate(bearer)` → adopt `{tenant_id, project_id}` from the
     * authenticated principal, then one atomic context swap via
     * {@see setCredentials()} + a tenant/project rebind. Authenticate-bearer
     * always runs (no conditional skip). Returns the `AuthnResponse` (the
     * verified principal).
     */
    public function loginAndAdoptTenant(
        string $username,
        string $password,
        string $totpCode = '',
        string $mfaOtpId = '',
        ?UdbMetadata $metadata = null,
    ): \Udb\Core\Authn\Services\V1\AuthnResponse {
        $login = $this->auth()->login($username, $password, $totpCode, $mfaOtpId, $metadata);
        $token = $login->getAccessToken();
        $authn = $this->auth()->authenticateBearer($token, $metadata);
        $principal = $authn->getPrincipal();

        $current = $this->metadata();
        $tenantId = ($principal !== null && $principal->getTenantId() !== '')
            ? $principal->getTenantId()
            : $current->tenantId;
        $projectId = ($principal !== null && $principal->getProjectId() !== '')
            ? $principal->getProjectId()
            : $current->projectId;

        // One atomic context swap: install the bearer + adopted tenant/project
        // across every sub-client.
        $adopted = $current
            ->withCredentials($token, '')
            ->withProjectId($projectId);
        // tenantId is readonly; rebuild via withCredentials chain + a tenant set.
        $adopted = new UdbMetadata(
            tenantId: $tenantId,
            userId: $adopted->userId,
            purpose: $adopted->purpose,
            correlationId: $adopted->correlationId,
            scopes: $adopted->scopes,
            serviceIdentity: $adopted->serviceIdentity,
            projectId: $adopted->projectId,
            clientCatalogVersion: $adopted->clientCatalogVersion,
            bearerToken: $token,
            apiKey: '',
            consistency: $adopted->consistency,
            primaryRead: $adopted->primaryRead,
            maxReplicaLagMs: $adopted->maxReplicaLagMs,
            eventualConsistencyAllowed: $adopted->eventualConsistencyAllowed,
            readFenceJson: $adopted->readFenceJson,
        );
        $this->bindContext($adopted);

        return $authn;
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

    /**
     * CDC events facade ({@see EventsService}): `events()->subscribe($topic)->ready()`
     * + `events()->publishAndWait(...)` over the DataBroker `PublishCDC`
     * (server-stream) and `EnqueueOutboxEvent` RPCs. Parity with the TS/Python
     * `events` namespace. Reuses the data-plane channel via {@see data()}.
     */
    public function events(): EventsService
    {
        return $this->events ??= new EventsService($this, $this->data());
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
        $md = $meta->toGrpcMetadata();

        // Unify with GeneratedClient's robustness: read-only RPCs (from the
        // proto-derived operation_kind) are retried with exponential backoff on
        // transient codes; mutations are never retried. Non-OK status is mapped
        // to a typed UdbRpcException carrying the decoded `udb-error-detail-bin`.
        $readOnly = (\Fahara02\UdbLaravel\Generated\GeneratedClient::OPERATION_KIND[$rpcName] ?? 'mutation') === 'read_only';
        $retry = (array) ($this->config['retry'] ?? []);
        $maxAttempts = max(1, (int) ($retry['max_attempts'] ?? 4));
        $baseDelayMs = max(1, (int) ($retry['base_delay_ms'] ?? 50));
        $maxDelayMs = max($baseDelayMs, (int) ($retry['max_delay_ms'] ?? 2_000));

        $status = null;
        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            /** @var \Grpc\UnaryCall $call */
            $call = $invoker($md, $opts);
            [$response, $status] = $call->wait();
            $code = is_object($status) ? (int) ($status->code ?? -1) : (int) ($status['code'] ?? -1);
            if ($code === 0) {
                if (! $response instanceof $responseClass) {
                    throw new UdbRpcException(status: 13, details: "UDB {$rpcName} returned an unexpected response type", raw: $status, rpcName: $rpcName);
                }

                return $response;
            }
            if ($attempt < $maxAttempts && $this->isRetryableStatus($code, $readOnly)) {
                $this->sleepBackoff($attempt, $baseDelayMs, $maxDelayMs);

                continue;
            }
            break;
        }

        throw UdbRpcException::fromGrpcStatus($status, $rpcName);
    }

    /** Transient codes are retried only for read-only RPCs. */
    private function isRetryableStatus(int $code, bool $readOnly): bool
    {
        if (! $readOnly) {
            return false;
        }
        // 4=DEADLINE_EXCEEDED, 14=UNAVAILABLE, 8=RESOURCE_EXHAUSTED.
        return in_array($code, [4, 14, 8], true);
    }

    /** Exponential backoff with full jitter, capped at max_delay_ms. */
    private function sleepBackoff(int $attempt, int $baseDelayMs, int $maxDelayMs): void
    {
        $ceil = (int) min($maxDelayMs, $baseDelayMs * (2 ** ($attempt - 1)));
        $jittered = random_int(0, max(0, $ceil));
        if ($jittered > 0) {
            usleep($jittered * 1000);
        }
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

        $bearerToken = (string) ($credentials['bearerToken'] ?? $this->config['bearerToken'] ?? '');
        $apiKey = $bearerToken !== '' ? '' : (string) ($credentials['apiKey'] ?? $this->config['apiKey'] ?? '');

        return new UdbMetadata(
            tenantId: (string) ($this->config['tenantId'] ?? ''),
            userId: (string) ($this->config['userId'] ?? ''),
            purpose: (string) ($this->config['purpose'] ?? 'web.request'),
            correlationId: (string) ($this->config['correlationId'] ?? ''),
            scopes: $scopes,
            serviceIdentity: (string) ($this->config['serviceIdentity'] ?? ''),
            projectId: (string) ($this->config['projectId'] ?? 'default'),
            clientCatalogVersion: (string) ($this->config['clientCatalogVersion'] ?? '1.0.0'),
            bearerToken: $bearerToken,
            apiKey: $apiKey,
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
