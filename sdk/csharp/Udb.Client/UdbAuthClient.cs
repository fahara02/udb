using System.Security.Cryptography;
using System.Text;
using Grpc.Core;
using Grpc.Net.Client;
using AuthnV1 = udb.core.Authn.Services.V1;
using AuthzV1 = udb.core.Authz.Services.V1;

namespace Udb.Client;

/// <summary>
/// Hand-written auth ergonomics over the generated AuthnService / AuthzService
/// clients, mirroring <see cref="UdbClient"/>'s metadata convention (item 110).
/// </summary>
public sealed class UdbAuthClient : IAsyncDisposable
{
    private readonly GrpcChannel? _ownedChannel;
    private readonly UdbMetadata _metadata;
    private readonly UdbCredentials _credentials;
    private readonly AuthnV1.AuthnService.AuthnServiceClient _authn;
    private readonly AuthzV1.AuthzService.AuthzServiceClient _authz;
    private readonly AuthzCache _cache;
    private readonly string? _bundleSecret;

    /// <summary>Standalone constructor: owns its channel and a fresh decision cache.</summary>
    /// <param name="bundleSecret">
    /// When set, signed policy bundles returned by <see cref="GetPolicyBundleAsync"/>
    /// are HMAC-SHA256 verified against this shared secret.
    /// </param>
    public UdbAuthClient(string address, UdbMetadata metadata, AuthzCache? cache = null, string? bundleSecret = null, UdbCredentials? credentials = null)
    {
        _ownedChannel = GrpcChannel.ForAddress(address);
        _metadata = metadata;
        _credentials = credentials ?? new UdbCredentials(metadata.BearerToken, metadata.ApiKey);
        _authn = new AuthnV1.AuthnService.AuthnServiceClient(_ownedChannel);
        _authz = new AuthzV1.AuthzService.AuthzServiceClient(_ownedChannel);
        _cache = cache ?? new AuthzCache();
        _bundleSecret = bundleSecret;
    }

    /// <summary>
    /// Facade constructor: reuses a channel owned elsewhere (the channel is NOT
    /// disposed by this instance) and a shared decision cache.
    /// </summary>
    internal UdbAuthClient(GrpcChannel sharedChannel, UdbMetadata metadata, AuthzCache cache, string? bundleSecret = null, UdbCredentials? credentials = null)
    {
        _ownedChannel = null;
        _metadata = metadata;
        _credentials = credentials ?? new UdbCredentials(metadata.BearerToken, metadata.ApiKey);
        _authn = new AuthnV1.AuthnService.AuthnServiceClient(sharedChannel);
        _authz = new AuthzV1.AuthzService.AuthzServiceClient(sharedChannel);
        _cache = cache;
        _bundleSecret = bundleSecret;
    }

    /// <summary>
    /// Test constructor: injects pre-built generated clients (e.g. backed by a
    /// fake <see cref="Grpc.Core.CallInvoker"/>) so request/header construction
    /// can be exercised without a live server.
    /// </summary>
    internal UdbAuthClient(
        AuthnV1.AuthnService.AuthnServiceClient authn,
        AuthzV1.AuthzService.AuthzServiceClient authz,
        UdbMetadata metadata,
        AuthzCache? cache = null,
        string? bundleSecret = null,
        UdbCredentials? credentials = null)
    {
        _ownedChannel = null;
        _metadata = metadata;
        _credentials = credentials ?? new UdbCredentials(metadata.BearerToken, metadata.ApiKey);
        _authn = authn;
        _authz = authz;
        _cache = cache ?? new AuthzCache();
        _bundleSecret = bundleSecret;
    }

    /// <summary>The shared TTL decision cache backing <see cref="CanAsync"/>/<see cref="RequireAsync"/>.</summary>
    public AuthzCache Cache => _cache;

    /// <summary>Raw generated Authn client, for RPCs not yet wrapped.</summary>
    public AuthnV1.AuthnService.AuthnServiceClient Authn => _authn;

    /// <summary>Raw generated Authz client, for RPCs not yet wrapped.</summary>
    public AuthzV1.AuthzService.AuthzServiceClient Authz => _authz;

    private Metadata Headers()
    {
        var headers = new Metadata
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

    /// <summary>The shared credentials holder backing this client's outbound auth headers.</summary>
    public UdbCredentials Credentials => _credentials;

    // ── Authentication ──────────────────────────────────────────────────────
    public Task<AuthnV1.AuthnResponse> AuthenticateAsync(AuthnV1.AuthnRequest request, CancellationToken ct = default)
        => _authn.AuthenticateAsync(request, Headers(), cancellationToken: ct).ResponseAsync;

    public Task<AuthnV1.AuthnResponse> AuthenticateBearerAsync(string token, CancellationToken ct = default)
        => AuthenticateAsync(new AuthnV1.AuthnRequest { BearerToken = token, TenantHint = _metadata.TenantId, ProjectHint = _metadata.ProjectId }, ct);

    public Task<AuthnV1.AuthnResponse> AuthenticateApiKeyAsync(string apiKey, CancellationToken ct = default)
        => AuthenticateAsync(new AuthnV1.AuthnRequest { ApiKey = apiKey, TenantHint = _metadata.TenantId, ProjectHint = _metadata.ProjectId }, ct);

    public Task<AuthnV1.AuthnResponse> AuthenticateSessionAsync(string sessionId, CancellationToken ct = default)
        => AuthenticateAsync(new AuthnV1.AuthnRequest { SessionId = sessionId, TenantHint = _metadata.TenantId, ProjectHint = _metadata.ProjectId }, ct);

    /// <summary>
    /// Exchange a refresh token (and optional session id) for a fresh access
    /// token via the Authn RefreshToken RPC. Returns the new access token and
    /// its lifetime in seconds.
    /// </summary>
    public async Task<AuthnV1.RefreshTokenResponse> RefreshTokenAsync(
        string refreshToken, string sessionId = "", CancellationToken ct = default)
    {
        var request = new AuthnV1.RefreshTokenRequest { RefreshToken = refreshToken };
        if (!string.IsNullOrEmpty(sessionId))
        {
            request.SessionId = sessionId;
        }
        return await _authn.RefreshTokenAsync(request, Headers(), cancellationToken: ct).ResponseAsync;
    }

    // ── Authorization ─────────────────────────────────────────────────────────
    public async Task<AuthzV1.Decision> AuthorizeAsync(AuthzV1.AuthzRequest request, CancellationToken ct = default)
    {
        var resp = await _authz.AuthorizeAsync(request, Headers(), cancellationToken: ct).ResponseAsync;
        return resp.Decision;
    }

    /// <summary>
    /// Check whether the configured principal may perform <paramref name="action"/>
    /// on <paramref name="resource"/>. The decision is served from (and stored in)
    /// the TTL cache. <paramref name="scopes"/> augments the metadata scopes for
    /// both the request's <c>requested_scopes</c> and the cache key.
    /// </summary>
    public async Task<(bool Allowed, AuthzV1.Decision Decision)> CanAsync(
        AuthzV1.ResourceRef resource, string action, string purpose = "",
        IEnumerable<string>? scopes = null, CancellationToken ct = default)
    {
        var effectivePurpose = string.IsNullOrEmpty(purpose) ? _metadata.Purpose : purpose;
        var requestedScopes = MergeScopes(scopes);

        var cacheKey = AuthzCache.Key(PrincipalKey(), RenderResource(resource), action, effectivePurpose, requestedScopes);
        var cached = _cache.TryGet(cacheKey);
        if (cached is not null)
        {
            return (cached.Allowed, cached);
        }

        var request = BuildAuthzRequest(resource, action, effectivePurpose, requestedScopes);
        var decision = await AuthorizeAsync(request, ct);
        _cache.Set(cacheKey, decision);
        return (decision.Allowed, decision);
    }

    /// <summary>
    /// Like <see cref="CanAsync"/> but throws <see cref="UdbAuthzDeniedException"/>
    /// on deny. Returns the allowing decision on success.
    /// </summary>
    public async Task<AuthzV1.Decision> RequireAsync(
        AuthzV1.ResourceRef resource, string action, string purpose = "",
        IEnumerable<string>? scopes = null, CancellationToken ct = default)
    {
        var (allowed, decision) = await CanAsync(resource, action, purpose, scopes, ct);
        if (!allowed)
        {
            throw new UdbAuthzDeniedException(RenderResource(resource), action, decision);
        }
        return decision;
    }

    /// <summary>
    /// Returns the raw decision for a check without throwing — useful for UIs
    /// that surface the deny reason. Does not consult or populate the cache so
    /// the explanation is always fresh.
    /// </summary>
    public async Task<AuthzV1.Decision> ExplainAsync(
        AuthzV1.ResourceRef resource, string action, string purpose = "",
        IEnumerable<string>? scopes = null, CancellationToken ct = default)
    {
        var effectivePurpose = string.IsNullOrEmpty(purpose) ? _metadata.Purpose : purpose;
        var request = BuildAuthzRequest(resource, action, effectivePurpose, MergeScopes(scopes));
        return await AuthorizeAsync(request, ct);
    }

    /// <summary>
    /// Batch-check many (object, action) pairs for the configured user in one
    /// RPC (BatchCheckPermissions). Returns a map keyed by <c>"object:action"</c>
    /// to the boolean allow result, mirroring the server response map keys.
    /// </summary>
    public async Task<IReadOnlyDictionary<string, bool>> BatchCanAsync(
        IEnumerable<(string Object, string Action)> checks, string domain = "", CancellationToken ct = default)
    {
        var request = new AuthzV1.BatchCheckPermissionsRequest
        {
            UserId = _metadata.UserId,
            Domain = string.IsNullOrEmpty(domain) ? _metadata.TenantId : domain,
        };
        foreach (var (obj, action) in checks)
        {
            request.Checks.Add(new AuthzV1.PermissionCheck { Object = obj, Action = action });
        }
        var resp = await _authz.BatchCheckPermissionsAsync(request, Headers(), cancellationToken: ct).ResponseAsync;
        return new Dictionary<string, bool>(resp.Results);
    }

    // ── Stage 2: native database fast-path access (item 138) ──────────────────
    /// <summary>
    /// Authorize and, when allowed, return the native-access grant (restricted
    /// role + scoped DSN + RLS session variables). Returns <c>null</c> when
    /// access is allowed but no grant was minted; throws on deny.
    /// <paramref name="scopes"/> augments the metadata scopes for the request's
    /// <c>requested_scopes</c>.
    /// </summary>
    public async Task<AuthzV1.NativeAccessGrant?> NativeAccessAsync(
        AuthzV1.ResourceRef resource, string action, string purpose = "",
        IEnumerable<string>? scopes = null, CancellationToken ct = default)
    {
        var principal = BuildPrincipal();
        var request = new AuthzV1.NativeAccessRequest
        {
            Principal = principal,
            TenantId = _metadata.TenantId,
            ProjectId = _metadata.ProjectId,
            Resource = resource,
            Action = action,
            Purpose = string.IsNullOrEmpty(purpose) ? _metadata.Purpose : purpose,
        };
        request.RequestedScopes.AddRange(MergeScopes(scopes));
        var resp = await _authz.GetNativeAccessAsync(request, Headers(), cancellationToken: ct).ResponseAsync;
        if (resp.Decision is { Allowed: false })
        {
            throw new UdbAuthzDeniedException(RenderResource(resource), action, resp.Decision);
        }
        return resp.Grant;
    }

    // ── Stage 2: signed policy bundle (item 140) ──────────────────────────────
    public async Task<AuthzV1.SignedPolicyBundle> GetPolicyBundleAsync(CancellationToken ct = default)
    {
        var request = new AuthzV1.PolicyBundleRequest
        {
            TenantId = _metadata.TenantId,
            ProjectId = _metadata.ProjectId,
        };
        var resp = await _authz.GetPolicyBundleAsync(request, Headers(), cancellationToken: ct).ResponseAsync;
        if (!string.IsNullOrEmpty(_bundleSecret))
        {
            VerifyPolicyBundle(resp.Bundle, _bundleSecret!);
        }
        return resp.Bundle;
    }

    /// <summary>
    /// Verify the HMAC-SHA256 signature of a <see cref="AuthzV1.SignedPolicyBundle"/>.
    /// The server signs the <c>bundle</c> bytes and emits the signature as
    /// lowercase hex (the proto comment says Base64, but the server emits hex —
    /// we match the server). Comparison is constant-time. Throws
    /// <see cref="UdbPolicyBundleSignatureException"/> on mismatch.
    /// </summary>
    public static void VerifyPolicyBundle(AuthzV1.SignedPolicyBundle signed, string secret)
    {
        ArgumentNullException.ThrowIfNull(signed);
        if (string.IsNullOrEmpty(secret))
        {
            throw new ArgumentException("bundle secret must be non-empty", nameof(secret));
        }

        var expected = HmacSha256HexLower(secret, signed.Bundle.ToByteArray());
        var actual = signed.Signature ?? string.Empty;

        // Fixed-time compare over the raw bytes of the lowercase-hex strings.
        var expectedBytes = Encoding.ASCII.GetBytes(expected);
        var actualBytes = Encoding.ASCII.GetBytes(actual);
        if (!CryptographicOperations.FixedTimeEquals(expectedBytes, actualBytes))
        {
            throw new UdbPolicyBundleSignatureException(signed.KeyId, signed.Algorithm);
        }
    }

    /// <summary>Lowercase-hex HMAC-SHA256 of <paramref name="message"/> under <paramref name="secret"/>.</summary>
    private static string HmacSha256HexLower(string secret, byte[] message)
    {
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(secret));
        var mac = hmac.ComputeHash(message);
        return Convert.ToHexString(mac).ToLowerInvariant();
    }

    // ── helpers ───────────────────────────────────────────────────────────────
    private AuthzV1.Principal BuildPrincipal()
    {
        var principal = new AuthzV1.Principal
        {
            UserId = _metadata.UserId,
            ServiceIdentity = _metadata.ServiceIdentity,
            TenantId = _metadata.TenantId,
            ProjectId = _metadata.ProjectId,
        };
        principal.Scopes.AddRange(_metadata.Scopes);
        return principal;
    }

    private AuthzV1.AuthzRequest BuildAuthzRequest(
        AuthzV1.ResourceRef resource, string action, string purpose, IReadOnlyList<string> requestedScopes)
    {
        var request = new AuthzV1.AuthzRequest
        {
            Principal = BuildPrincipal(),
            TenantId = _metadata.TenantId,
            ProjectId = _metadata.ProjectId,
            Resource = resource,
            Action = action,
            Purpose = purpose,
        };
        request.RequestedScopes.AddRange(requestedScopes);
        return request;
    }

    /// <summary>Union of the metadata scopes and any per-call scopes, de-duplicated.</summary>
    private IReadOnlyList<string> MergeScopes(IEnumerable<string>? extra)
    {
        if (extra is null)
        {
            return _metadata.Scopes;
        }
        var merged = new List<string>(_metadata.Scopes);
        foreach (var s in extra)
        {
            if (!string.IsNullOrEmpty(s) && !merged.Contains(s))
            {
                merged.Add(s);
            }
        }
        return merged;
    }

    private string PrincipalKey()
        => $"{_metadata.TenantId}/{_metadata.ProjectId}/{_metadata.UserId}/{_metadata.ServiceIdentity}";

    /// <summary>Stable, human-readable rendering of a resource for cache keys / errors.</summary>
    private static string RenderResource(AuthzV1.ResourceRef r)
    {
        if (!string.IsNullOrEmpty(r.MessageType)) return r.MessageType;
        if (!string.IsNullOrEmpty(r.Table)) return string.IsNullOrEmpty(r.Schema) ? r.Table : $"{r.Schema}.{r.Table}";
        if (!string.IsNullOrEmpty(r.Collection)) return r.Collection;
        if (!string.IsNullOrEmpty(r.ResourceName)) return r.ResourceName;
        if (!string.IsNullOrEmpty(r.Path)) return r.Path;
        if (!string.IsNullOrEmpty(r.ResourceType)) return r.ResourceType;
        return r.ResourceId;
    }

    public ValueTask DisposeAsync()
    {
        _ownedChannel?.Dispose();
        return ValueTask.CompletedTask;
    }
}
