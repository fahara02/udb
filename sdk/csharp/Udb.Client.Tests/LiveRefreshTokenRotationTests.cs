// Live refresh-token rotation conformance.
//
// Gated on UDB_LIVE_SDK_TESTS=1; otherwise the test returns without touching the
// network. CI's "SDK refresh-token rotation (live)" job runs it against the
// broker it just built, on every push to main.
//
// The broker rotates the refresh token on every AuthnService.RefreshToken call
// and each one works once. A caller that kept the token it logged in with
// refreshed once and then failed with UNAUTHENTICATED "invalid credential" — a
// production outage no single-refresh test could catch. The C# SDK keeps no
// token store: refresh is UdbAuthClient.RefreshTokenAsync, and the caller
// contract is that each refresh presents the refresh token the previous one
// returned. This test chains it exactly that way.

using Grpc.Core;
using Xunit;
using AuthnEntityV1 = Udb.Core.Authn.Entity.V1;
using AuthnV1 = Udb.Core.Authn.Services.V1;

namespace Udb.Client.Tests;

public sealed class LiveRefreshTokenRotationTests
{
    private const int Refreshes = 3;

    private static string Env(string name, string? fallback = null)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (!string.IsNullOrWhiteSpace(value))
        {
            return value.Trim();
        }
        return fallback ?? throw new InvalidOperationException($"{name} is required when UDB_LIVE_SDK_TESTS=1");
    }

    private static string Url(string target) =>
        target.Contains("://", StringComparison.Ordinal) ? target : $"http://{target}";

    [Fact]
    public async Task LiveRefreshTokenRotationSurvivesRepeatedRefreshes()
    {
        if (Environment.GetEnvironmentVariable("UDB_LIVE_SDK_TESTS") != "1")
        {
            return;
        }
        var target = Env("UDB_GRPC_TARGET");
        var authTarget = Env("UDB_AUTH_GRPC_TARGET", target);
        var username = Env("UDB_LIVE_USERNAME");
        var password = Env("UDB_LIVE_PASSWORD");
        var tenant = Env("UDB_LIVE_TENANT", "sdk-live");
        var projectId = Env("UDB_LIVE_PROJECT", "default");

        await using var project = await UdbProject.ProjectAsync(new UdbProjectConfig
        {
            Target = Url(target),
            AuthTarget = Url(authTarget),
            TenantId = tenant,
            ProjectId = projectId,
            Purpose = "csharp.live.refresh-rotation",
            CorrelationId = "csharp-live-refresh-rotation",
            ServiceIdentity = "csharp.sdk.live",
        });
        AuthnV1.LoginRequest NewLogin() => new()
        {
            Username = username,
            Password = password,
            TenantHint = tenant,
            ProjectHint = projectId,
            DeviceName = "csharp-sdk-refresh-rotation",
        };

        // Adopt the canonical tenant the login JWT carries; authenticated calls
        // must assert it rather than the human tenant code.
        await project.LoginAndAdoptTenantAsync(NewLogin());
        // The refresh chain starts at a login's refresh token.
        var login = await project.Auth.LoginAsync(NewLogin());
        var first = login.RefreshToken;
        Assert.False(string.IsNullOrEmpty(login.AccessToken), "login returned no access token");
        Assert.False(string.IsNullOrEmpty(first), "login returned no refresh token");
        project.SetCredentials(login.AccessToken);

        var previous = first;
        for (var i = 1; i <= Refreshes; i++)
        {
            var refreshed = await project.Auth.RefreshTokenAsync(previous);
            Assert.False(string.IsNullOrEmpty(refreshed.AccessToken), $"refresh {i} returned no access token");
            Assert.False(string.IsNullOrEmpty(refreshed.RefreshToken), $"refresh {i} returned no refresh token");
            Assert.True(
                refreshed.RefreshToken != previous,
                $"refresh {i}: the refresh token did not change — the broker stopped rotating it");

            // An authenticated call carrying the refreshed bearer.
            project.SetCredentials(refreshed.AccessToken);
            var validated = await project.Auth.Authn.ValidateTokenAsync(
                new AuthnV1.ValidateTokenRequest
                {
                    Token = refreshed.AccessToken,
                    TokenType = AuthnEntityV1.TokenType.JwtAccess,
                },
                project.Headers()).ResponseAsync;
            Assert.True(validated.Valid, $"refresh {i}: the refreshed access token does not validate");
            previous = refreshed.RefreshToken;
        }

        // The first refresh token was spent three rotations ago: the broker must
        // refuse it. (Replaying the immediately-previous token instead would trip
        // reuse detection and revoke every session of this shared live user.)
        var refused = await Assert.ThrowsAsync<RpcException>(() => project.Auth.RefreshTokenAsync(first));
        Assert.Equal(StatusCode.Unauthenticated, refused.StatusCode);
    }
}
