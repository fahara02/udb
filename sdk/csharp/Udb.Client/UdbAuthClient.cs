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
    private readonly GrpcChannel _channel;
    private readonly UdbMetadata _metadata;
    private readonly AuthnV1.AuthnService.AuthnServiceClient _authn;
    private readonly AuthzV1.AuthzService.AuthzServiceClient _authz;

    public UdbAuthClient(string address, UdbMetadata metadata)
    {
        _channel = GrpcChannel.ForAddress(address);
        _metadata = metadata;
        _authn = new AuthnV1.AuthnService.AuthnServiceClient(_channel);
        _authz = new AuthzV1.AuthzService.AuthzServiceClient(_channel);
    }

    private Metadata Headers() => new()
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

    // ── Authentication ──────────────────────────────────────────────────────
    public Task<AuthnV1.AuthnResponse> AuthenticateAsync(AuthnV1.AuthnRequest request, CancellationToken ct = default)
        => _authn.AuthenticateAsync(request, Headers(), cancellationToken: ct).ResponseAsync;

    public Task<AuthnV1.AuthnResponse> AuthenticateBearerAsync(string token, CancellationToken ct = default)
        => AuthenticateAsync(new AuthnV1.AuthnRequest { BearerToken = token, TenantHint = _metadata.TenantId, ProjectHint = _metadata.ProjectId }, ct);

    public Task<AuthnV1.AuthnResponse> AuthenticateApiKeyAsync(string apiKey, CancellationToken ct = default)
        => AuthenticateAsync(new AuthnV1.AuthnRequest { ApiKey = apiKey, TenantHint = _metadata.TenantId, ProjectHint = _metadata.ProjectId }, ct);

    public Task<AuthnV1.AuthnResponse> AuthenticateSessionAsync(string sessionId, CancellationToken ct = default)
        => AuthenticateAsync(new AuthnV1.AuthnRequest { SessionId = sessionId, TenantHint = _metadata.TenantId, ProjectHint = _metadata.ProjectId }, ct);

    // ── Authorization ─────────────────────────────────────────────────────────
    public async Task<AuthzV1.Decision> AuthorizeAsync(AuthzV1.AuthzRequest request, CancellationToken ct = default)
    {
        var resp = await _authz.AuthorizeAsync(request, Headers(), cancellationToken: ct).ResponseAsync;
        return resp.Decision;
    }

    public async Task<(bool Allowed, AuthzV1.Decision Decision)> CanAsync(
        AuthzV1.ResourceRef resource, string action, string purpose = "", CancellationToken ct = default)
    {
        var principal = new AuthzV1.Principal
        {
            UserId = _metadata.UserId,
            ServiceIdentity = _metadata.ServiceIdentity,
            TenantId = _metadata.TenantId,
            ProjectId = _metadata.ProjectId,
        };
        principal.Scopes.AddRange(_metadata.Scopes);
        var request = new AuthzV1.AuthzRequest
        {
            Principal = principal,
            TenantId = _metadata.TenantId,
            ProjectId = _metadata.ProjectId,
            Resource = resource,
            Action = action,
            Purpose = string.IsNullOrEmpty(purpose) ? _metadata.Purpose : purpose,
        };
        var decision = await AuthorizeAsync(request, ct);
        return (decision.Allowed, decision);
    }

    // ── Stage 2: native database fast-path access (item 138) ──────────────────
    /// <summary>
    /// Authorize and, when allowed, return the native-access grant (restricted
    /// role + scoped DSN + RLS session variables). Returns <c>null</c> when
    /// access is allowed but no grant was minted; throws on deny.
    /// </summary>
    public async Task<AuthzV1.NativeAccessGrant?> NativeAccessAsync(
        AuthzV1.ResourceRef resource, string action, string purpose = "", CancellationToken ct = default)
    {
        var principal = new AuthzV1.Principal
        {
            UserId = _metadata.UserId,
            ServiceIdentity = _metadata.ServiceIdentity,
            TenantId = _metadata.TenantId,
            ProjectId = _metadata.ProjectId,
        };
        principal.Scopes.AddRange(_metadata.Scopes);
        var request = new AuthzV1.NativeAccessRequest
        {
            Principal = principal,
            TenantId = _metadata.TenantId,
            ProjectId = _metadata.ProjectId,
            Resource = resource,
            Action = action,
            Purpose = string.IsNullOrEmpty(purpose) ? _metadata.Purpose : purpose,
        };
        var resp = await _authz.GetNativeAccessAsync(request, Headers(), cancellationToken: ct).ResponseAsync;
        if (resp.Decision is { Allowed: false })
        {
            throw new InvalidOperationException($"udb: native access denied: {resp.Decision.DenyReason}");
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
        return resp.Bundle;
    }

    public ValueTask DisposeAsync()
    {
        _channel.Dispose();
        return ValueTask.CompletedTask;
    }
}
