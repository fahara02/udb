using Grpc.Core;

namespace Udb.Client.Tests;

/// <summary>
/// A test <see cref="CallInvoker"/> that captures the outbound request message
/// and the request <see cref="Metadata"/> headers for the most recent unary
/// call, and returns a caller-supplied canned response. No network involved.
/// </summary>
internal sealed class CapturingCallInvoker : CallInvoker
{
    private readonly Func<string, object> _responder;

    public CapturingCallInvoker(Func<string, object> responder)
    {
        _responder = responder;
    }

    /// <summary>The request message of the most recent call.</summary>
    public object? LastRequest { get; private set; }

    /// <summary>The request headers of the most recent call (the canonical x-* metadata).</summary>
    public Metadata? LastHeaders { get; private set; }

    /// <summary>The full method name (e.g. "/udb.core.authz.services.v1.AuthzService/Authorize") of the most recent call.</summary>
    public string? LastMethod { get; private set; }

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
    {
        LastRequest = request;
        LastHeaders = options.Headers;
        LastMethod = method.FullName;

        var response = (TResponse)_responder(method.Name);
        return new AsyncUnaryCall<TResponse>(
            Task.FromResult(response),
            Task.FromResult(new Metadata()),
            () => Status.DefaultSuccess,
            () => new Metadata(),
            () => { });
    }

    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
    {
        LastRequest = request;
        LastHeaders = options.Headers;
        LastMethod = method.FullName;
        return (TResponse)_responder(method.Name);
    }

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options)
        => throw new NotSupportedException();

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options)
        => throw new NotSupportedException();

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        => throw new NotSupportedException();
}
