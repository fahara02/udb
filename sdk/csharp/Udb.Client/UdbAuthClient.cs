using System.Security.Cryptography;
using System.Text;
using Grpc.Core;
using Grpc.Net.Client;
using AuthnV1 = Udb.Core.Authn.Services.V1;
using AuthzV1 = Udb.Core.Authz.Services.V1;

namespace Udb.Client;

/// <summary>
/// Hand-written auth ergonomics over the generated AuthnService / AuthzService
/// clients, mirroring <see cref="UdbClient"/>'s metadata convention (item 110).
/// </summary>
public sealed class UdbAuthClient : IAsyncDisposable
{
    private readonly GrpcChannel? _ownedChannel;
    private UdbMetadata _metadata;
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
        _ownedChannel = UdbChannel.ForAddress(address);
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
        return global::Udb.Client.Generated.UdbRequestContext.Ensure(headers);
    }

    /// <summary>The shared credentials holder backing this client's outbound auth headers.</summary>
    public UdbCredentials Credentials => _credentials;

    /// <summary>The metadata identity currently bound to this auth client.</summary>
    public UdbMetadata Metadata => _metadata;

    internal void UpdateMetadata(UdbMetadata metadata)
    {
        _metadata = metadata;
    }

    // ── Authentication ──────────────────────────────────────────────────────
    public Task<AuthnV1.AuthnResponse> AuthenticateAsync(AuthnV1.AuthnRequest request, CancellationToken ct = default)
        => _authn.AuthenticateAsync(request, Headers(), cancellationToken: ct).ResponseAsync;

    public Task<AuthnV1.LoginResponse> LoginAsync(AuthnV1.LoginRequest request, CancellationToken ct = default)
        => _authn.LoginAsync(request, Headers(), cancellationToken: ct).ResponseAsync;

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

    // ── Delegated job tokens ──────────────────────────────────────────────────
    /// <summary>
    /// Exchange a user's current access token for a delegated job token
    /// (<c>AuthnService.ExchangeTokenForJob</c>), so a durable job (an export, a
    /// report, a nightly sync) acts for that user after the user's own short-lived
    /// token expired. Call it on a client carrying the SERVICE's own bearer (a
    /// service account whose grant holds <c>udb:token:delegate</c>, in the user's
    /// tenant); the broker refuses any other caller, and any subject token that is
    /// not a valid user access token of that tenant, with <c>PermissionDenied</c>.
    /// The token names the user as subject and the service as actor, carries
    /// <paramref name="purpose"/> and <paramref name="jobId"/>, and lives at most
    /// <paramref name="ttlSeconds"/> (0 = the broker's maximum,
    /// <c>UDB_DELEGATED_TOKEN_MAX_TTL</c>, 24 hours by default). It has no refresh
    /// token, cannot be exchanged again, and stops working as soon as the user, the
    /// token or the service's grant is revoked: see <see cref="IsTokenExpired"/>
    /// and <see cref="IsTokenRevoked"/>.
    /// </summary>
    /// <param name="subjectToken">The user's current access token (<c>Bearer </c> prefix optional).</param>
    /// <param name="purpose">Why the job acts for the user, e.g. <c>"export"</c>; signed into the token.</param>
    /// <param name="jobId">The job the token is for; at most 200 bytes.</param>
    /// <param name="ttlSeconds">The requested lifetime; 0 asks for the broker's maximum.</param>
    /// <exception cref="ArgumentException">A required argument is blank or <paramref name="ttlSeconds"/> is negative.</exception>
    /// <exception cref="RpcException">The broker refused the exchange.</exception>
    public async Task<DelegatedJobToken> ExchangeTokenForJobAsync(
        string subjectToken, string purpose, string jobId, long ttlSeconds = 0, CancellationToken ct = default)
    {
        var request = JobTokenRequest(subjectToken, purpose, jobId, ttlSeconds);
        var response = await _authn.ExchangeTokenForJobAsync(request, Headers(), cancellationToken: ct).ResponseAsync;
        return ToDelegatedJobToken(response, jobId);
    }

    internal static AuthnV1.ExchangeTokenForJobRequest JobTokenRequest(
        string subjectToken, string purpose, string jobId, long ttlSeconds)
    {
        if (string.IsNullOrWhiteSpace(subjectToken))
        {
            throw new ArgumentException("udb: ExchangeTokenForJob: subjectToken is required", nameof(subjectToken));
        }
        if (string.IsNullOrWhiteSpace(jobId))
        {
            throw new ArgumentException("udb: ExchangeTokenForJob: jobId is required", nameof(jobId));
        }
        if (string.IsNullOrWhiteSpace(purpose))
        {
            throw new ArgumentException("udb: ExchangeTokenForJob: purpose is required", nameof(purpose));
        }
        if (ttlSeconds < 0)
        {
            throw new ArgumentException("udb: ExchangeTokenForJob: ttlSeconds must not be negative", nameof(ttlSeconds));
        }
        return new AuthnV1.ExchangeTokenForJobRequest
        {
            SubjectToken = subjectToken,
            Purpose = purpose,
            JobId = jobId,
            TtlSeconds = ttlSeconds,
        };
    }

    internal static DelegatedJobToken ToDelegatedJobToken(AuthnV1.ExchangeTokenForJobResponse response, string jobId)
    {
        if (string.IsNullOrEmpty(response.AccessToken))
        {
            throw new InvalidOperationException(
                $"udb: exchange the user's token for job {jobId}: the broker returned no token");
        }
        return new DelegatedJobToken(
            response.AccessToken,
            response.Jti,
            response.ExpiresAtUnix,
            response.Subject,
            response.Actor,
            response.Purpose,
            response.JobId,
            response.Scopes.ToArray(),
            response.TenantId,
            response.ProjectId);
    }

    /// <summary>
    /// The broker's typed refusal reason (<c>ErrorDetail.reason</c>, e.g.
    /// <c>UDB_TOKEN_EXPIRED</c>) carried by a failed call, or <c>""</c> when the
    /// error carries none. Reads a raw <see cref="RpcException"/>'s
    /// <c>udb-error-detail-bin</c> trailer and a generated client's
    /// <see cref="Udb.Client.Generated.UdbRpcException"/>, including either one
    /// found as an inner exception. The message text is never matched.
    /// </summary>
    public static string ErrorReason(Exception? error)
    {
        for (var e = error; e is not null; e = e.InnerException)
        {
            if (e is global::Udb.Client.Generated.UdbRpcException udb)
            {
                return udb.DecodedErrorDetail?.Reason ?? "";
            }
            if (e is RpcException rpc)
            {
                foreach (var entry in rpc.Trailers)
                {
                    if (entry.IsBinary &&
                        string.Equals(entry.Key, "udb-error-detail-bin", StringComparison.OrdinalIgnoreCase))
                    {
                        try
                        {
                            return global::Udb.Entity.V1.ErrorDetail.Parser.ParseFrom(entry.ValueBytes).Reason;
                        }
                        catch (Google.Protobuf.InvalidProtocolBufferException)
                        {
                            return "";
                        }
                    }
                }
                return "";
            }
        }
        return "";
    }

    /// <summary>
    /// The broker refused the bearer because it expired (<c>UDB_TOKEN_EXPIRED</c>):
    /// refresh it or sign in again. A delegated job token cannot be refreshed: the
    /// job exchanges a fresh user token or stops.
    /// </summary>
    public static bool IsTokenExpired(Exception? error) => ErrorReason(error) == "UDB_TOKEN_EXPIRED";

    /// <summary>
    /// The broker refused the bearer because it was revoked or is no longer backed
    /// by live state (<c>UDB_TOKEN_REVOKED</c>): do not retry with it.
    /// </summary>
    public static bool IsTokenRevoked(Exception? error) => ErrorReason(error) == "UDB_TOKEN_REVOKED";

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
