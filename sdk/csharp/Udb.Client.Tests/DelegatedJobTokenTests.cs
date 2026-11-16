using Google.Protobuf;
using Grpc.Core;
using Xunit;
using AuthnV1 = Udb.Core.Authn.Services.V1;
using AuthzV1 = Udb.Core.Authz.Services.V1;
using EntityV1 = Udb.Entity.V1;

namespace Udb.Client.Tests;

/// <summary>
/// Delegated job tokens without a broker: the request
/// <see cref="UdbAuthClient.ExchangeTokenForJobAsync"/> sends, the token it
/// returns, the local refusals, and the token-refusal reason helpers.
/// </summary>
public sealed class DelegatedJobTokenTests
{
    private static UdbMetadata Meta() => new(
        TenantId: "tenant-1",
        Purpose: "test",
        CorrelationId: "c-1",
        Scopes: new[] { "udb:token:delegate" },
        ServiceIdentity: "spiffe://acme/exporter",
        UserId: "",
        ProjectId: "billing");

    private static RpcException Refusal(string reason)
    {
        var detail = new EntityV1.ErrorDetail { Reason = reason, Kind = EntityV1.ErrorKind.Policy };
        var trailers = new Metadata { { "udb-error-detail-bin", detail.ToByteArray() } };
        return new RpcException(new Status(StatusCode.Unauthenticated, "refused"), trailers);
    }

    [Fact]
    public async Task ExchangeTokenForJob_SendsTheRequest_AndReturnsTheToken()
    {
        var invoker = new CapturingCallInvoker(_ => new AuthnV1.ExchangeTokenForJobResponse
        {
            AccessToken = "delegated.jwt",
            Jti = "dlg_1",
            ExpiresAtUnix = 1_900_000_000,
            Subject = "user-1",
            Actor = "spiffe://acme/exporter",
            Purpose = "export",
            JobId = "export-42",
            Scopes = { "udb:read" },
            TenantId = "tenant-1",
            ProjectId = "billing",
        });
        var client = new UdbAuthClient(
            new AuthnV1.AuthnService.AuthnServiceClient(invoker),
            new AuthzV1.AuthzService.AuthzServiceClient(invoker),
            Meta());

        var token = await client.ExchangeTokenForJobAsync("user.jwt", "export", "export-42", 600);

        Assert.Equal("/udb.core.authn.services.v1.AuthnService/ExchangeTokenForJob", invoker.LastMethod);
        var sent = Assert.IsType<AuthnV1.ExchangeTokenForJobRequest>(invoker.LastRequest);
        Assert.Equal("user.jwt", sent.SubjectToken);
        Assert.Equal("export", sent.Purpose);
        Assert.Equal("export-42", sent.JobId);
        Assert.Equal(600, sent.TtlSeconds);
        Assert.Equal("tenant-1", invoker.LastHeaders!.GetValue("x-tenant-id"));

        Assert.Equal("delegated.jwt", token.AccessToken);
        Assert.Equal("dlg_1", token.Jti);
        Assert.Equal(1_900_000_000, token.ExpiresAtUnix);
        Assert.Equal("user-1", token.Subject);
        Assert.Equal("spiffe://acme/exporter", token.Actor);
        Assert.Equal("export", token.Purpose);
        Assert.Equal("export-42", token.JobId);
        Assert.Equal(new[] { "udb:read" }, token.Scopes);
        Assert.Equal("tenant-1", token.TenantId);
        Assert.Equal("billing", token.ProjectId);

        // Refused locally, before any call.
        await Assert.ThrowsAsync<ArgumentException>(() => client.ExchangeTokenForJobAsync(" ", "export", "j"));
        await Assert.ThrowsAsync<ArgumentException>(() => client.ExchangeTokenForJobAsync("t", "", "j"));
        await Assert.ThrowsAsync<ArgumentException>(() => client.ExchangeTokenForJobAsync("t", "export", ""));
        await Assert.ThrowsAsync<ArgumentException>(() => client.ExchangeTokenForJobAsync("t", "export", "j", -1));
        Assert.Single(invoker.RequestHistory);
    }

    [Fact]
    public async Task ExchangeTokenForJob_RefusesAResponseWithoutAToken()
    {
        var invoker = new CapturingCallInvoker(_ => new AuthnV1.ExchangeTokenForJobResponse());
        var client = new UdbAuthClient(
            new AuthnV1.AuthnService.AuthnServiceClient(invoker),
            new AuthzV1.AuthzService.AuthzServiceClient(invoker),
            Meta());
        await Assert.ThrowsAsync<InvalidOperationException>(() => client.ExchangeTokenForJobAsync("t", "export", "j"));
    }

    [Fact]
    public void ExpiredAndRevokedBearers_AreToldApart()
    {
        var expired = Refusal("UDB_TOKEN_EXPIRED");
        var revoked = Refusal("UDB_TOKEN_REVOKED");
        Assert.Equal("UDB_TOKEN_EXPIRED", UdbAuthClient.ErrorReason(expired));
        Assert.True(UdbAuthClient.IsTokenExpired(expired));
        Assert.False(UdbAuthClient.IsTokenRevoked(expired));
        Assert.True(UdbAuthClient.IsTokenRevoked(revoked));
        Assert.False(UdbAuthClient.IsTokenExpired(revoked));
        Assert.True(UdbAuthClient.IsTokenExpired(new Generated.UdbRpcException("/svc/Call", expired)));
        Assert.True(UdbAuthClient.IsTokenExpired(new InvalidOperationException("wrapped", expired)));
        Assert.False(UdbAuthClient.IsTokenExpired(Refusal("UDB_AUTHENTICATION_FAILED")));
        Assert.False(UdbAuthClient.IsTokenExpired(new InvalidOperationException("UDB_TOKEN_EXPIRED")));
        Assert.False(UdbAuthClient.IsTokenRevoked(null));
    }
}
