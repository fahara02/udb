// Recovery after a broker restart, mirroring the Go SDK's reconnect tests.
//
// The C# SDK owns no renewable credential (it keeps no refresh token or
// password; an API key rides every call and the broker checks it each time), so
// there is no bearer for it to renew after a refusal. What it does own is the
// channel and the sign-in: the channel must reconnect promptly once the broker is
// back, and LoginAndAdoptTenantAsync must ride out a broker that is restarting.
//
// The in-process broker is a bare Kestrel HTTP/2 server speaking the gRPC wire
// format by hand, so the test needs no server-side generated code.

using System.Buffers.Binary;
using System.Net;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;
using AuthnV1 = Udb.Core.Authn.Services.V1;
using EntityV1 = Udb.Entity.V1;

namespace Udb.Client.Tests;

/// <summary>An in-process broker that can be stopped and started again on the same port.</summary>
internal sealed class RestartBroker : IAsyncDisposable
{
    private WebApplication? _app;
    private int _logins;
    private int _capabilities;

    public int Port { get; private set; }

    public int Logins => Volatile.Read(ref _logins);

    public int Capabilities => Volatile.Read(ref _capabilities);

    /// <summary>Login fails with <see cref="LoginFailure"/> for the first <see cref="FailLogins"/> attempts.</summary>
    public int FailLogins { get; set; }

    public StatusCode LoginFailure { get; set; } = StatusCode.Unavailable;

    public string Address => $"http://127.0.0.1:{Port}";

    public async Task StartAsync()
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (true)
        {
            var builder = WebApplication.CreateSlimBuilder();
            builder.Logging.ClearProviders();
            builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(1));
            var port = Port;
            builder.WebHost.ConfigureKestrel(k =>
                k.Listen(IPAddress.Loopback, port, o => o.Protocols = HttpProtocols.Http2));
            var app = builder.Build();
            app.MapPost("/udb.services.v1.DataBroker/GetCapabilities", ctx =>
            {
                Interlocked.Increment(ref _capabilities);
                return Reply(ctx, new EntityV1.CapabilitiesResponse { ProtocolVersion = "1.0.0" });
            });
            app.MapPost("/udb.core.authn.services.v1.AuthnService/Login", ctx =>
            {
                var n = Interlocked.Increment(ref _logins);
                if (n <= FailLogins)
                {
                    return Fail(ctx, LoginFailure, $"login attempt {n}");
                }
                return Reply(ctx, new AuthnV1.LoginResponse
                {
                    AccessToken = $"access-{n}",
                    RefreshToken = $"refresh-{n}",
                    AccessTokenExpiresIn = 3600,
                });
            });
            app.MapPost("/udb.core.authn.services.v1.AuthnService/Authenticate", ctx =>
                Reply(ctx, new AuthnV1.AuthnResponse
                {
                    Principal = new AuthnV1.Principal
                    {
                        TenantId = "tenant-canonical",
                        ProjectId = "project-canonical",
                        UserId = "gateway",
                    },
                }));
            try
            {
                await app.StartAsync();
            }
            catch (IOException) when (DateTime.UtcNow < deadline)
            {
                // The previous generation's port is not free yet.
                await app.DisposeAsync();
                await Task.Delay(50);
                continue;
            }
            var bound = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
            Port = new Uri(bound).Port;
            _app = app;
            return;
        }
    }

    /// <summary>Take the broker down: every connection is closed and nothing listens.</summary>
    public async Task StopAsync()
    {
        var app = _app;
        _app = null;
        if (app is not null)
        {
            await app.StopAsync();
            await app.DisposeAsync();
        }
    }

    public ValueTask DisposeAsync() => new(StopAsync());

    private static async Task Reply(HttpContext ctx, IMessage message)
    {
        ctx.Response.ContentType = "application/grpc";
        var payload = message.ToByteArray();
        var frame = new byte[5 + payload.Length];
        BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(1), (uint)payload.Length);
        payload.CopyTo(frame, 5);
        await ctx.Response.Body.WriteAsync(frame);
        ctx.Response.AppendTrailer("grpc-status", "0");
    }

    private static Task Fail(HttpContext ctx, StatusCode code, string message)
    {
        ctx.Response.ContentType = "application/grpc";
        ctx.Response.AppendTrailer("grpc-status", ((int)code).ToString());
        ctx.Response.AppendTrailer("grpc-message", message);
        return Task.CompletedTask;
    }
}

public sealed class UdbBrokerRestartTests
{
    private static UdbProject Open(RestartBroker broker) => UdbProject.Open(new UdbProjectConfig
    {
        Target = broker.Address,
        TenantId = "tenant-hint",
        ProjectId = "default",
        Purpose = "csharp.unit.broker-restart",
    });

    private static AuthnV1.LoginRequest Login() => new() { Username = "gateway", Password = "secret" };

    [Fact]
    public void DefaultChannelOptionsCapTheReconnectBackoff()
    {
        var options = UdbChannel.DefaultOptions();
        Assert.Equal(TimeSpan.FromSeconds(5), options.MaxReconnectBackoff);
        Assert.Equal(TimeSpan.FromMilliseconds(500), options.InitialReconnectBackoff);
        var tls = UdbChannel.DefaultOptions(null, null);
        Assert.Equal(TimeSpan.FromSeconds(5), tls.MaxReconnectBackoff);
    }

    [Fact]
    public async Task SameProjectRecoversAfterBrokerRestart()
    {
        await using var broker = new RestartBroker();
        await broker.StartAsync();
        await using var project = Open(broker);
        await project.LoginAndAdoptTenantAsync(Login());
        await project.Data.GetCapabilitiesAsync(new EntityV1.CapabilitiesRequest(), project.Headers());

        await broker.StopAsync();
        var down = await Assert.ThrowsAsync<RpcException>(async () =>
            await project.Data.GetCapabilitiesAsync(new EntityV1.CapabilitiesRequest(), project.Headers()));
        Assert.Equal(StatusCode.Unavailable, down.StatusCode);
        // Let the channel fail a few dials, growing its reconnect backoff.
        await Task.Delay(TimeSpan.FromMilliseconds(1500));
        await broker.StartAsync();

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (true)
        {
            try
            {
                await project.Data.GetCapabilitiesAsync(new EntityV1.CapabilitiesRequest(), project.Headers());
                break;
            }
            catch (RpcException err) when (err.StatusCode == StatusCode.Unavailable && DateTime.UtcNow < deadline)
            {
                // Only UNAVAILABLE is tolerated while the channel reconnects.
                await Task.Delay(50);
            }
        }
        Assert.Equal(2, broker.Capabilities);
        Assert.Equal("Bearer access-1", project.Headers().GetValue("authorization"));
    }

    [Fact]
    public async Task LoginRetriesATransientFailure()
    {
        await using var broker = new RestartBroker { FailLogins = 2 };
        await broker.StartAsync();
        await using var project = Open(broker);
        var verified = await project.LoginAndAdoptTenantAsync(Login());
        Assert.Equal("tenant-canonical", verified.Principal.TenantId);
        Assert.Equal(3, broker.Logins);
        Assert.Equal("Bearer access-3", project.Headers().GetValue("authorization"));
        Assert.Equal("tenant-canonical", project.Headers().GetValue("x-tenant-id"));
    }

    [Fact]
    public async Task LoginGivesUpAfterThreeTransientFailures()
    {
        await using var broker = new RestartBroker { FailLogins = int.MaxValue };
        await broker.StartAsync();
        await using var project = Open(broker);
        var err = await Assert.ThrowsAsync<RpcException>(() => project.LoginAndAdoptTenantAsync(Login()));
        Assert.Equal(StatusCode.Unavailable, err.StatusCode);
        Assert.Equal(UdbProject.LoginAttempts, broker.Logins);
    }

    // Negative: a refused credential is final; it is never repeated.
    [Fact]
    public async Task LoginRefusedCredentialIsNotRetried()
    {
        await using var broker = new RestartBroker { FailLogins = int.MaxValue, LoginFailure = StatusCode.Unauthenticated };
        await broker.StartAsync();
        await using var project = Open(broker);
        var err = await Assert.ThrowsAsync<RpcException>(() => project.LoginAndAdoptTenantAsync(Login()));
        Assert.Equal(StatusCode.Unauthenticated, err.StatusCode);
        Assert.Equal(1, broker.Logins);
        Assert.Null(project.Headers().GetValue("authorization"));
    }

    [Fact]
    public async Task LoginWaitsForARestartingBroker()
    {
        await using var broker = new RestartBroker();
        await broker.StartAsync();
        await broker.StopAsync(); // the port is known; nothing listens on it
        var restart = Task.Run(async () =>
        {
            await Task.Delay(200);
            await broker.StartAsync();
        });
        await using var project = Open(broker);
        await project.LoginAndAdoptTenantAsync(Login());
        await restart;
        Assert.Equal(1, broker.Logins);
    }
}
