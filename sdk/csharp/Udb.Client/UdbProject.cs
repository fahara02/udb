using Grpc.Net.Client;
using Udb.Entity.V1;
using Udb.Services.V1;
using AnalyticsV1 = udb.core.Analytics.Services.V1;
using ApikeyV1 = udb.core.Apikey.Services.V1;
using AssetV1 = udb.core.Asset.Services.V1;
using NotificationV1 = udb.core.Notification.Services.V1;
using StorageV1 = udb.core.Storage.Services.V1;
using TenantV1 = udb.core.Tenant.Services.V1;
using WebRtcV1 = udb.core.Webrtc.Services.V1;

namespace Udb.Client;

/// <summary>
/// One configuration object for a <see cref="UdbProject"/>. All fields have
/// sensible defaults; only <see cref="Target"/> is required. <see cref="AuthTarget"/>
/// defaults to <see cref="Target"/> when unset (a single broker fronts both the
/// data plane and the native auth services in the integration stack).
/// </summary>
public sealed record UdbProjectConfig
{
    /// <summary>Data-plane / default gRPC endpoint, e.g. <c>http://localhost:50051</c>.</summary>
    public required string Target { get; init; }

    /// <summary>Auth-plane endpoint. Defaults to <see cref="Target"/>.</summary>
    public string? AuthTarget { get; init; }

    /// <summary>
    /// WebRTC signalling/peer endpoint. Defaults to the (resolved)
    /// <see cref="AuthTarget"/> — the native control-plane listener that serves
    /// the WebRTC services. When set and distinct, the <see cref="UdbProject.WebRtc"/>
    /// facade dials a dedicated channel.
    /// </summary>
    public string? WebrtcTarget { get; init; }

    public string TenantId { get; init; } = "default";
    public string ProjectId { get; init; } = "default";
    public string Purpose { get; init; } = "";
    public string UserId { get; init; } = "";
    public string ServiceIdentity { get; init; } = "";
    public string CorrelationId { get; init; } = "";
    public string[] Scopes { get; init; } = Array.Empty<string>();
    public string ClientCatalogVersion { get; init; } = UdbClient.ProtocolVersion;
    public string BearerToken { get; init; } = "";
    public string ApiKey { get; init; } = "";

    /// <summary>Default TTL for the shared authz decision cache.</summary>
    public TimeSpan AuthzCacheTtl { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Shared secret for HMAC-SHA256 verification of signed policy bundles.
    /// When set, <see cref="UdbProject.Authz"/>.GetPolicyBundleAsync verifies
    /// every returned bundle and throws on mismatch. Unset disables verification.
    /// </summary>
    public string? PolicyBundleSecret { get; init; }
}

/// <summary>
/// Top-level ergonomic facade over every native UDB service whose generated
/// client ships in this package. Mirrors the Go/Python/TS <c>Project</c> facade:
/// one config, shared metadata + channels, and first-class accessors. Raw
/// generated clients stay reachable (<see cref="ApiKey"/>, <see cref="Tenant"/>,
/// …) for RPCs not yet wrapped.
/// The native media services — <see cref="Storage"/>, <see cref="Asset"/>, and
/// <see cref="WebRtc"/> — share the control-plane channel (the same target as
/// auth) and the shared metadata headers.
/// </summary>
public sealed class UdbProject : IAsyncDisposable, IDisposable
{
    private readonly GrpcChannel _dataChannel;
    private readonly GrpcChannel _authChannel;
    private readonly GrpcChannel _nativeServicesChannel;
    private readonly GrpcChannel _webrtcChannel;
    private readonly bool _separateAuthChannel;
    private readonly bool _separateWebrtcChannel;
    private readonly UdbMetadata _metadata;
    private readonly UdbCredentials _credentials;

    private UdbProject(UdbProjectConfig config)
    {
        _metadata = new UdbMetadata(
            TenantId: config.TenantId,
            Purpose: config.Purpose,
            CorrelationId: string.IsNullOrEmpty(config.CorrelationId) ? Guid.NewGuid().ToString("N") : config.CorrelationId,
            Scopes: config.Scopes,
            ServiceIdentity: config.ServiceIdentity,
            UserId: config.UserId,
            ProjectId: config.ProjectId,
            ClientCatalogVersion: config.ClientCatalogVersion,
            BearerToken: config.BearerToken,
            ApiKey: config.ApiKey);
        // Shared, mutable credentials sent on every outbound call. SetCredentials
        // mutates this holder so a refreshed token reaches all clients at once.
        _credentials = new UdbCredentials(config.BearerToken, config.ApiKey);

        var authTarget = string.IsNullOrEmpty(config.AuthTarget) ? config.Target : config.AuthTarget!;
        var webrtcTarget = string.IsNullOrEmpty(config.WebrtcTarget) ? authTarget : config.WebrtcTarget!;
        _dataChannel = GrpcChannel.ForAddress(config.Target);
        _separateAuthChannel = !string.Equals(authTarget, config.Target, StringComparison.Ordinal);
        _authChannel = _separateAuthChannel ? GrpcChannel.ForAddress(authTarget) : _dataChannel;
        _nativeServicesChannel = _authChannel;
        // WebRTC rides the control-plane channel unless a distinct webrtcTarget is set.
        if (string.Equals(webrtcTarget, authTarget, StringComparison.Ordinal))
        {
            _webrtcChannel = _authChannel;
            _separateWebrtcChannel = false;
        }
        else if (string.Equals(webrtcTarget, config.Target, StringComparison.Ordinal))
        {
            _webrtcChannel = _dataChannel;
            _separateWebrtcChannel = false;
        }
        else
        {
            _webrtcChannel = GrpcChannel.ForAddress(webrtcTarget);
            _separateWebrtcChannel = true;
        }

        var cache = new AuthzCache(config.AuthzCacheTtl);

        Data = new DataBroker.DataBrokerClient(_dataChannel);
        Auth = new UdbAuthClient(_authChannel, _metadata, cache, config.PolicyBundleSecret, _credentials);
        Authz = Auth; // same wrapper exposes the authz surface
        ApiKey = new ApikeyV1.ApiKeyService.ApiKeyServiceClient(_nativeServicesChannel);
        Tenant = new TenantV1.TenantService.TenantServiceClient(_nativeServicesChannel);
        Notification = new NotificationV1.NotificationService.NotificationServiceClient(_nativeServicesChannel);
        Analytics = new AnalyticsV1.AnalyticsService.AnalyticsServiceClient(_nativeServicesChannel);

        // Native media services live on the control-plane listener (same target as
        // auth) and reuse the shared metadata headers; WebRTC rides its own channel
        // when webrtcTarget is distinct.
        Storage = new UdbStorageClient(
            new StorageV1.StorageService.StorageServiceClient(_nativeServicesChannel), Headers);
        Asset = new UdbAssetClient(
            new AssetV1.AssetService.AssetServiceClient(_nativeServicesChannel), Headers);
        WebRtc = new UdbWebRtcClient(
            new WebRtcV1.RoomService.RoomServiceClient(_webrtcChannel),
            new WebRtcV1.PeerService.PeerServiceClient(_webrtcChannel),
            new WebRtcV1.TrackService.TrackServiceClient(_webrtcChannel),
            new WebRtcV1.TurnService.TurnServiceClient(_webrtcChannel),
            new WebRtcV1.SignalingService.SignalingServiceClient(_webrtcChannel),
            Headers);
    }

    /// <summary>Constructs a project facade. Async for symmetry with the other SDKs.</summary>
    public static Task<UdbProject> ProjectAsync(UdbProjectConfig config)
        => Task.FromResult(new UdbProject(config));

    /// <summary>Synchronous constructor for callers that don't need the async form.</summary>
    public static UdbProject Open(UdbProjectConfig config) => new(config);

    /// <summary>Shared metadata headers (eight wire/identity headers).</summary>
    public Grpc.Core.Metadata Headers()
    {
        var headers = new Grpc.Core.Metadata
        {
            { "x-tenant-id", _metadata.TenantId },
            { "x-user-id", _metadata.UserId },
            { "x-purpose", _metadata.Purpose },
            { "x-correlation-id", _metadata.CorrelationId },
            { "x-scopes", string.Join(",", _metadata.Scopes) },
            { "x-service-identity", _metadata.ServiceIdentity },
            { "x-udb-project-id", _metadata.ProjectId },
            { "x-udb-client-catalog-version", _metadata.ClientCatalogVersion },
        };
        var bearer = _credentials.BearerToken;
        if (!string.IsNullOrWhiteSpace(bearer))
        {
            headers.Add("authorization", $"Bearer {bearer}");
        }
        var apiKey = _credentials.ApiKey;
        if (!string.IsNullOrWhiteSpace(apiKey))
        {
            headers.Add("x-api-key", apiKey);
        }
        return headers;
    }

    /// <summary>The shared credentials holder; mutating it hot-swaps creds on every client.</summary>
    public UdbCredentials Credentials => _credentials;

    internal GrpcChannel DataChannelForTesting => _dataChannel;

    internal GrpcChannel AuthChannelForTesting => _authChannel;

    internal GrpcChannel NativeServicesChannelForTesting => _nativeServicesChannel;

    /// <summary>
    /// Hot-swap the bearer token and API key sent on every subsequent call across
    /// all sub-clients. Call this after a login/refresh so the new token reaches
    /// outbound metadata — the C# analogue of the TypeScript <c>core.setCredentials</c>.
    /// </summary>
    public void SetCredentials(string? bearerToken, string? apiKey = null)
        => _credentials.Set(bearerToken, apiKey ?? _credentials.ApiKey);

    // ── first-class accessors ───────────────────────────────────────────────
    /// <summary>Raw data-plane broker client (Select/Upsert/etc.).</summary>
    public DataBroker.DataBrokerClient Data { get; }

    /// <summary>Authentication ergonomics (authenticate / refresh).</summary>
    public UdbAuthClient Auth { get; }

    /// <summary>Authorization ergonomics (can / require / batch / explain / native-access).</summary>
    public UdbAuthClient Authz { get; }

    /// <summary>Raw generated API-key service client.</summary>
    public ApikeyV1.ApiKeyService.ApiKeyServiceClient ApiKey { get; }

    /// <summary>Raw generated tenant service client.</summary>
    public TenantV1.TenantService.TenantServiceClient Tenant { get; }

    /// <summary>Raw generated notification service client.</summary>
    public NotificationV1.NotificationService.NotificationServiceClient Notification { get; }

    /// <summary>Raw generated analytics service client.</summary>
    public AnalyticsV1.AnalyticsService.AnalyticsServiceClient Analytics { get; }

    /// <summary>Storage ergonomics (upload/download/file metadata).</summary>
    public UdbStorageClient Storage { get; }

    /// <summary>Asset ergonomics (pipeline definitions, assets, step completion).</summary>
    public UdbAssetClient Asset { get; }

    /// <summary>WebRTC ergonomics, grouped: <c>Room</c> / <c>Peer</c> / <c>Track</c> / <c>Turn</c> / <c>Signaling</c>.</summary>
    public UdbWebRtcClient WebRtc { get; }

    // ── convenience wrappers ────────────────────────────────────────────────
    /// <summary>Run a SELECT through the data broker with the shared metadata.</summary>
    public Task<RecordSet> SelectAsync(SelectRequest request, CancellationToken ct = default)
        => Data.SelectAsync(request, Headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>Run an UPSERT through the data broker with the shared metadata.</summary>
    public Task<MutationResponse> UpsertAsync(UpsertRequest request, CancellationToken ct = default)
        => Data.UpsertAsync(request, Headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>
    /// Send a notification. The tenant/project/correlation fields default from
    /// the facade config when left unset on the request.
    /// </summary>
    public Task<NotificationV1.SendNotificationResponse> SendNotificationAsync(
        NotificationV1.SendNotificationRequest request, CancellationToken ct = default)
    {
        if (string.IsNullOrEmpty(request.TenantId)) request.TenantId = _metadata.TenantId;
        if (string.IsNullOrEmpty(request.ProjectId)) request.ProjectId = _metadata.ProjectId;
        if (string.IsNullOrEmpty(request.CorrelationId)) request.CorrelationId = _metadata.CorrelationId;
        return Notification.SendNotificationAsync(request, Headers(), cancellationToken: ct).ResponseAsync;
    }

    /// <summary>Mint a new API key; returns the plaintext key alongside its metadata.</summary>
    public Task<ApikeyV1.CreateApiKeyResponse> CreateApiKeyAsync(
        ApikeyV1.CreateApiKeyRequest request, CancellationToken ct = default)
        => ApiKey.CreateApiKeyAsync(request, Headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>Revoke an API key by id, with an optional reason.</summary>
    public Task<ApikeyV1.RevokeApiKeyResponse> RevokeApiKeyAsync(
        string keyId, string reason = "", CancellationToken ct = default)
        => ApiKey.RevokeApiKeyAsync(
            new ApikeyV1.RevokeApiKeyRequest { KeyId = keyId, RevokeReason = reason },
            Headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>Create / onboard a tenant.</summary>
    public Task<TenantV1.CreateTenantResponse> CreateTenantAsync(
        TenantV1.CreateTenantRequest request, CancellationToken ct = default)
        => Tenant.CreateTenantAsync(request, Headers(), cancellationToken: ct).ResponseAsync;

    // ── lifecycle ───────────────────────────────────────────────────────────
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        _dataChannel.Dispose();
        if (_separateAuthChannel)
        {
            _authChannel.Dispose();
        }
        if (_separateWebrtcChannel)
        {
            _webrtcChannel.Dispose();
        }
    }
}
