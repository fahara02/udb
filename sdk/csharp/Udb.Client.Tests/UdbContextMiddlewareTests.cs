using Microsoft.AspNetCore.Http;
using Udb.Client.AspNetCore;
using Xunit;

namespace Udb.Client.Tests;

public sealed class UdbContextMiddlewareTests
{
    private static async Task<HttpContext> RunAsync(Action<HttpContext> setup)
    {
        var ctx = new DefaultHttpContext();
        setup(ctx);
        var called = false;
        var mw = new UdbContextMiddleware(_ => { called = true; return Task.CompletedTask; });
        await mw.InvokeAsync(ctx);
        Assert.True(called, "next middleware should be invoked");
        return ctx;
    }

    [Fact]
    public async Task Extracts_Canonical_Headers_Into_Metadata()
    {
        var ctx = await RunAsync(c =>
        {
            c.Request.Headers["x-tenant-id"] = "acme";
            c.Request.Headers["x-user-id"] = "user-1";
            c.Request.Headers["x-purpose"] = "reporting";
            c.Request.Headers["x-correlation-id"] = "corr-9";
            c.Request.Headers["x-request-id"] = "req-9";
            c.Request.Headers["x-scopes"] = "read, write";
            c.Request.Headers["x-service-identity"] = "svc";
            c.Request.Headers["x-udb-project-id"] = "proj-1";
            c.Request.Headers["x-udb-client-catalog-version"] = "9.9.9";
        });

        var meta = ctx.GetUdbMetadata();
        Assert.NotNull(meta);
        Assert.Equal("acme", meta!.TenantId);
        Assert.Equal("user-1", meta.UserId);
        Assert.Equal("reporting", meta.Purpose);
        Assert.Equal("corr-9", meta.CorrelationId);
        Assert.Equal("svc", meta.ServiceIdentity);
        Assert.Equal("proj-1", meta.ProjectId);
        Assert.Equal("9.9.9", meta.ClientCatalogVersion);
        Assert.Equal(new[] { "read", "write" }, meta.Scopes);
        Assert.Equal("req-9", ctx.GetUdbRequestId());
    }

    [Fact]
    public async Task Applies_Defaults_When_Headers_Absent()
    {
        var ctx = await RunAsync(_ => { });

        var meta = ctx.GetUdbMetadata();
        Assert.NotNull(meta);
        Assert.Equal("default", meta!.TenantId);
        Assert.Equal("default", meta.ProjectId);
        Assert.Empty(meta.Scopes);
        Assert.False(string.IsNullOrEmpty(meta.CorrelationId)); // generated
        Assert.Equal(UdbClient.ProtocolVersion, meta.ClientCatalogVersion);
        // request id falls back to the trace identifier (non-null on DefaultHttpContext).
        Assert.NotNull(ctx.GetUdbRequestId());
    }
}
