using Xunit;
using AuthnV1 = Udb.Core.Authn.Services.V1;
using AuthzV1 = Udb.Core.Authz.Services.V1;

namespace Udb.Client.Tests;

public sealed class UdbLoginAdoptTests
{
    [Fact]
    public async Task LoginAuthenticateSequence_ThenMetadataRebind_UsesVerifiedPrincipal()
    {
        var invoker = new CapturingCallInvoker(method => method switch
        {
            "Login" => new AuthnV1.LoginResponse { AccessToken = "tok-1", SessionId = "sess-1" },
            "Authenticate" => new AuthnV1.AuthnResponse
            {
                Principal = new AuthnV1.Principal
                {
                    TenantId = "canonical-tenant",
                    ProjectId = "canonical-project",
                    UserId = "user-9",
                },
            },
            _ => throw new InvalidOperationException(method),
        });
        var authn = new AuthnV1.AuthnService.AuthnServiceClient(invoker);
        var authz = new AuthzV1.AuthzService.AuthzServiceClient(invoker);
        var credentials = new UdbCredentials("", "api-1");
        var metadata = new UdbMetadata(
            TenantId: "hint-tenant",
            Purpose: "login",
            CorrelationId: "corr-1",
            Scopes: Array.Empty<string>(),
            ServiceIdentity: "",
            UserId: "",
            ProjectId: "hint-project");
        var client = new UdbAuthClient(authn, authz, metadata, credentials: credentials);

        var login = await client.LoginAsync(new AuthnV1.LoginRequest
        {
            Username = "alice",
            Password = "pw",
            TenantHint = "hint-tenant",
            ProjectHint = "hint-project",
        });
        var verified = await client.AuthenticateBearerAsync(login.AccessToken);

        Assert.Equal(new[] { "Login", "Authenticate" }, invoker.MethodHistory);
        var authenticate = Assert.IsType<AuthnV1.AuthnRequest>(invoker.RequestHistory[1]);
        Assert.Equal("tok-1", authenticate.BearerToken);
        Assert.Equal("hint-tenant", authenticate.TenantHint);

        var adopted = metadata with
        {
            TenantId = verified.Principal.TenantId,
            ProjectId = verified.Principal.ProjectId,
            UserId = verified.Principal.UserId,
            BearerToken = login.AccessToken,
            ApiKey = credentials.ApiKey,
        };
        client.UpdateMetadata(adopted);
        credentials.Set(login.AccessToken, credentials.ApiKey);

        await client.AuthenticateSessionAsync("sess-1");
        Assert.Equal("canonical-tenant", invoker.LastHeaders!.GetValue("x-tenant-id"));
        Assert.Equal("canonical-project", invoker.LastHeaders!.GetValue("x-udb-project-id"));
        Assert.Equal("user-9", invoker.LastHeaders!.GetValue("x-user-id"));
        Assert.Equal("Bearer tok-1", invoker.LastHeaders!.GetValue("authorization"));
    }
}
