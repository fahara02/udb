using Xunit;

namespace Udb.Client.Tests;

/// <summary>
/// Proves the shared <see cref="UdbCredentials"/> holder hot-swaps the outbound
/// <c>authorization</c> header: a token refresh (via <see cref="UdbProject.SetCredentials"/>)
/// changes the credentials emitted by <see cref="UdbProject.Headers"/> on the next
/// call, without rebuilding channels. No live broker is needed — the project opens
/// a channel but no RPC is dispatched.
/// </summary>
public sealed class UdbCredentialHotSwapTests
{
    private static UdbProject OpenOffline(string bearer = "", string apiKey = "")
        => UdbProject.Open(new UdbProjectConfig
        {
            Target = "http://localhost:1",
            TenantId = "tenant-a",
            BearerToken = bearer,
            ApiKey = apiKey,
        });

    private static string? AuthHeader(UdbProject project)
        => project.Headers().GetValue("authorization");

    private static string? ApiKeyHeader(UdbProject project)
        => project.Headers().GetValue("x-api-key");

    [Fact]
    public void SetCredentials_ChangesOutboundAuthorizationHeader()
    {
        using var project = OpenOffline(bearer: "token-1");
        Assert.Equal("Bearer token-1", AuthHeader(project));

        // Simulate a refresh.
        project.SetCredentials("token-2");
        Assert.Equal("Bearer token-2", AuthHeader(project));
    }

    [Fact]
    public void SetCredentials_KeepsApiKeyWhenOmitted()
    {
        using var project = OpenOffline(bearer: "token-1", apiKey: "key-1");
        Assert.Equal("key-1", ApiKeyHeader(project));

        project.SetCredentials("token-2");
        Assert.Equal("key-1", ApiKeyHeader(project));
        Assert.Equal("Bearer token-2", AuthHeader(project));
    }

    [Fact]
    public void Credentials_HolderIsSharedWithAuthClient()
    {
        using var project = OpenOffline(bearer: "token-1");
        project.SetCredentials("rotated", "key-9");
        Assert.Equal("rotated", project.Credentials.BearerToken);
        Assert.Equal("key-9", project.Credentials.ApiKey);
        Assert.Same(project.Credentials, project.Auth.Credentials);
    }

    [Fact]
    public void BlankCredentials_OmitAuthorizationHeader()
    {
        using var project = OpenOffline();
        Assert.Null(AuthHeader(project));
    }
}
