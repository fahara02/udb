// Live broker-restart recovery: a C# client outlives a broker restart without
// being recreated. The same UdbProject reconnects and keeps working on the data
// plane and the native control plane. Same contract as the Go SDK's
// TestLiveBrokerRestartRecovery.
//
// Runs when UDB_LIVE_SDK_TESTS=1 and UDB_LIVE_BROKER_RESTART_CMD names a command
// (run with sh -c) that restarts the broker on the same addresses and KEEPS its
// database (sessions, users and tenant ids survive). While the broker is down a
// call may fail UNAVAILABLE; any other failure, or no recovery within 90s, fails
// the test. Otherwise the test returns without touching the network.

using System.Diagnostics;
using Grpc.Core;
using Xunit;
using AuthnEntityV1 = Udb.Core.Authn.Entity.V1;
using AuthnV1 = Udb.Core.Authn.Services.V1;
using EntityV1 = Udb.Entity.V1;

namespace Udb.Client.Tests;

public sealed class LiveBrokerRestartTests
{
    private static string Env(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrWhiteSpace(value)
            ? throw new InvalidOperationException($"{name} is required when UDB_LIVE_SDK_TESTS=1")
            : value.Trim();
    }

    private static string Url(string target) =>
        target.Contains("://", StringComparison.Ordinal) ? target : $"http://{target}";

    [Fact]
    public async Task LiveBrokerRestartRecovery()
    {
        var restart = Environment.GetEnvironmentVariable("UDB_LIVE_BROKER_RESTART_CMD")?.Trim();
        if (Environment.GetEnvironmentVariable("UDB_LIVE_SDK_TESTS") != "1" || string.IsNullOrEmpty(restart))
        {
            return;
        }
        var tenant = Env("UDB_LIVE_TENANT");
        var projectId = Env("UDB_LIVE_PROJECT");
        await using var project = UdbProject.Open(new UdbProjectConfig
        {
            Target = Url(Env("UDB_GRPC_TARGET")),
            AuthTarget = Url(Env("UDB_AUTH_GRPC_TARGET")),
            TenantId = tenant,
            ProjectId = projectId,
            Purpose = "csharp.live.broker-restart",
            CorrelationId = "csharp-live-broker-restart",
            ServiceIdentity = "csharp.sdk.live",
        });
        await project.LoginAndAdoptTenantAsync(new AuthnV1.LoginRequest
        {
            Username = Env("UDB_LIVE_USERNAME"),
            Password = Env("UDB_LIVE_PASSWORD"),
            TenantHint = tenant,
            ProjectHint = projectId,
            DeviceName = "csharp-sdk-broker-restart",
        });

        DateTime Deadline() => DateTime.UtcNow.AddSeconds(5);
        var probes = new (string Name, Func<Task> Call)[]
        {
            ("data GetCapabilities", async () =>
                await project.Data.GetCapabilitiesAsync(
                    new EntityV1.CapabilitiesRequest(), project.Headers(), Deadline())),
            ("native ValidateSession", async () =>
            {
                var validated = await project.Auth.Authn.ValidateTokenAsync(
                    new AuthnV1.ValidateTokenRequest
                    {
                        Token = project.Credentials.BearerToken,
                        TokenType = AuthnEntityV1.TokenType.JwtAccess,
                    },
                    project.Headers(), Deadline()).ResponseAsync;
                Assert.True(validated.Valid, "the session token is not valid");
            }),
        };
        foreach (var (name, call) in probes)
        {
            try
            {
                await call();
            }
            catch (RpcException err)
            {
                Assert.Fail($"before restart, {name}: code={err.StatusCode}");
            }
        }

        using (var cmd = Process.Start(new ProcessStartInfo("sh") { ArgumentList = { "-c", restart } })!)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
            await cmd.WaitForExitAsync(timeout.Token);
            Assert.True(cmd.ExitCode == 0, $"UDB_LIVE_BROKER_RESTART_CMD exited {cmd.ExitCode}");
        }

        foreach (var (name, call) in probes)
        {
            var deadline = DateTime.UtcNow.AddSeconds(90);
            while (true)
            {
                try
                {
                    await call();
                    break;
                }
                catch (RpcException err)
                {
                    if (err.StatusCode != StatusCode.Unavailable)
                    {
                        Assert.Fail($"after restart, {name} on the same project: code={err.StatusCode} err={err.Message}");
                    }
                    if (DateTime.UtcNow > deadline)
                    {
                        Assert.Fail($"after restart, {name} stayed UNAVAILABLE for 90s: {err.Message}");
                    }
                    await Task.Delay(250);
                }
            }
        }
    }
}
