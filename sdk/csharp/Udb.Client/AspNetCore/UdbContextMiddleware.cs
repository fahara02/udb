using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace Udb.Client.AspNetCore;

/// <summary>
/// ASP.NET Core middleware that lifts UDB request context out of inbound HTTP
/// headers and exposes a request-scoped <see cref="UdbMetadata"/> (and, when a
/// <see cref="UdbProject"/> is registered in DI, the project facade) on
/// <see cref="HttpContext.Items"/>. Parity with the Go/Python/TS framework
/// adapters (M5.6).
///
/// Recognised inbound headers (case-insensitive), each with a sensible fallback:
/// <list type="bullet">
///   <item><c>x-tenant-id</c> → <see cref="UdbMetadata.TenantId"/> (default "default").</item>
///   <item><c>x-user-id</c> → <see cref="UdbMetadata.UserId"/>.</item>
///   <item><c>x-purpose</c> → <see cref="UdbMetadata.Purpose"/>.</item>
///   <item><c>x-correlation-id</c> → <see cref="UdbMetadata.CorrelationId"/>
///         (generated when absent).</item>
///   <item><c>x-request-id</c> → exposed under <see cref="RequestIdItemKey"/>
///         (falls back to <see cref="HttpContext.TraceIdentifier"/>).</item>
///   <item><c>x-scopes</c> → comma-separated → <see cref="UdbMetadata.Scopes"/>.</item>
///   <item><c>x-service-identity</c> → <see cref="UdbMetadata.ServiceIdentity"/>.</item>
///   <item><c>x-udb-project-id</c> → <see cref="UdbMetadata.ProjectId"/>
///         (default "default").</item>
///   <item><c>x-udb-client-catalog-version</c> →
///         <see cref="UdbMetadata.ClientCatalogVersion"/>.</item>
/// </list>
///
/// This adapter lives in the optional <c>Udb.Client.AspNetCore</c> namespace and
/// only compiles when the <c>Microsoft.AspNetCore.Http.Abstractions</c> reference
/// is present; the core SDK has no other dependency on it.
/// </summary>
public sealed class UdbContextMiddleware
{
    /// <summary><see cref="HttpContext.Items"/> key holding the request-scoped <see cref="UdbMetadata"/>.</summary>
    public const string MetadataItemKey = "udb.metadata";

    /// <summary><see cref="HttpContext.Items"/> key holding the resolved request id.</summary>
    public const string RequestIdItemKey = "udb.request-id";

    /// <summary><see cref="HttpContext.Items"/> key holding the request-scoped <see cref="UdbProject"/> (when registered).</summary>
    public const string ProjectItemKey = "udb.project";

    private readonly RequestDelegate _next;

    public UdbContextMiddleware(RequestDelegate next)
    {
        _next = next ?? throw new ArgumentNullException(nameof(next));
    }

    public Task InvokeAsync(HttpContext context)
    {
        var headers = context.Request.Headers;

        var correlationId = Header(headers, "x-correlation-id");
        if (string.IsNullOrEmpty(correlationId))
        {
            correlationId = Guid.NewGuid().ToString("N");
        }

        var requestId = Header(headers, "x-request-id");
        if (string.IsNullOrEmpty(requestId))
        {
            requestId = context.TraceIdentifier;
        }
        if (string.IsNullOrEmpty(requestId))
        {
            requestId = Guid.NewGuid().ToString("N");
        }

        var scopesHeader = Header(headers, "x-scopes");
        var scopes = string.IsNullOrEmpty(scopesHeader)
            ? Array.Empty<string>()
            : scopesHeader
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        var tenantId = Header(headers, "x-tenant-id");
        var projectId = Header(headers, "x-udb-project-id");
        var catalogVersion = Header(headers, "x-udb-client-catalog-version");
        var authorization = Header(headers, "authorization");
        var bearerToken = authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase)
            ? authorization["Bearer ".Length..].Trim()
            : string.Empty;

        var metadata = new UdbMetadata(
            TenantId: string.IsNullOrEmpty(tenantId) ? "default" : tenantId,
            Purpose: Header(headers, "x-purpose"),
            CorrelationId: correlationId,
            Scopes: scopes,
            ServiceIdentity: Header(headers, "x-service-identity"),
            UserId: Header(headers, "x-user-id"),
            ProjectId: string.IsNullOrEmpty(projectId) ? "default" : projectId,
            ClientCatalogVersion: string.IsNullOrEmpty(catalogVersion)
                ? UdbClient.ProtocolVersion
                : catalogVersion,
            BearerToken: bearerToken,
            ApiKey: Header(headers, "x-api-key"));

        context.Items[MetadataItemKey] = metadata;
        context.Items[RequestIdItemKey] = requestId;

        // Surface a DI-registered project facade for handlers that want a ready
        // client. Resolved best-effort: absence is not an error.
        var project = context.RequestServices?.GetService(typeof(UdbProject)) as UdbProject;
        if (project is not null)
        {
            context.Items[ProjectItemKey] = project;
        }

        return _next(context);
    }

    private static string Header(IHeaderDictionary headers, string name)
        => headers.TryGetValue(name, out var values) ? values.ToString() : string.Empty;
}

/// <summary>
/// <see cref="IApplicationBuilder"/> and <see cref="HttpContext"/> extensions for
/// the UDB request-context adapter.
/// </summary>
public static class UdbContextMiddlewareExtensions
{
    /// <summary>
    /// Adds <see cref="UdbContextMiddleware"/> to the request pipeline. Place it
    /// before any endpoint that reads UDB request context.
    /// </summary>
    public static IApplicationBuilder UseUdbContext(this IApplicationBuilder app)
    {
        ArgumentNullException.ThrowIfNull(app);
        return app.UseMiddleware<UdbContextMiddleware>();
    }

    /// <summary>The request-scoped <see cref="UdbMetadata"/> set by the middleware, or <c>null</c>.</summary>
    public static UdbMetadata? GetUdbMetadata(this HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return context.Items.TryGetValue(UdbContextMiddleware.MetadataItemKey, out var value)
            ? value as UdbMetadata
            : null;
    }

    /// <summary>The resolved request id set by the middleware, or <c>null</c>.</summary>
    public static string? GetUdbRequestId(this HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return context.Items.TryGetValue(UdbContextMiddleware.RequestIdItemKey, out var value)
            ? value as string
            : null;
    }

    /// <summary>The DI-registered <see cref="UdbProject"/> surfaced by the middleware, or <c>null</c>.</summary>
    public static UdbProject? GetUdbProject(this HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return context.Items.TryGetValue(UdbContextMiddleware.ProjectItemKey, out var value)
            ? value as UdbProject
            : null;
    }
}
