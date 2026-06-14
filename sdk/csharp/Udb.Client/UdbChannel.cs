using System.Net.Http;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;

namespace Udb.Client;

/// <summary>
/// Factory for the long-lived UDB <see cref="GrpcChannel"/>. Construct a channel
/// once and reuse it across every RPC: a fresh channel forces a TCP+TLS+HTTP/2
/// handshake on each call, which dominates per-RPC latency. The defaults here keep
/// an idle connection warm (HTTP/2 keepalive) and add a native gRPC service-config
/// retry on <c>UNAVAILABLE</c> (jittered exponential backoff, bounded by the call
/// deadline). <see cref="GrpcChannel"/> already auto-enables
/// <c>EnableMultipleHttp2Connections</c>, so it pools connections past the server's
/// max-concurrent-streams without us touching that server limit.
/// </summary>
public static class UdbChannel
{
    // Keepalive: ping an idle connection every 30s, give the ack 10s.
    private static readonly TimeSpan KeepAlivePingDelay = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan KeepAlivePingTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// The UDB default service-config retry policy: 4 attempts, 0.1s initial
    /// backoff, 2s ceiling, x2 multiplier, only on <c>UNAVAILABLE</c>. gRPC applies
    /// jitter internally and bounds the sequence by the call deadline.
    /// </summary>
    public static RetryPolicy DefaultRetryPolicy => new()
    {
        MaxAttempts = 4,
        InitialBackoff = TimeSpan.FromMilliseconds(100),
        MaxBackoff = TimeSpan.FromSeconds(2),
        BackoffMultiplier = 2.0,
        RetryableStatusCodes = { Grpc.Core.StatusCode.Unavailable },
    };

    /// <summary>Build the default <see cref="GrpcChannelOptions"/> (keepalive + retry).</summary>
    public static GrpcChannelOptions DefaultOptions() => new()
    {
        HttpHandler = new SocketsHttpHandler
        {
            // Keep the warm channel alive so an idle connection does not drop to
            // IDLE and re-handshake on the next RPC.
            KeepAlivePingDelay = KeepAlivePingDelay,
            KeepAlivePingTimeout = KeepAlivePingTimeout,
            EnableMultipleHttp2Connections = true,
            PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
        },
        ServiceConfig = new ServiceConfig
        {
            MethodConfigs =
            {
                new MethodConfig
                {
                    Names = { MethodName.Default }, // applies to every method on the channel
                    RetryPolicy = DefaultRetryPolicy,
                },
            },
        },
    };

    /// <summary>
    /// Create a long-lived channel to <paramref name="address"/> with UDB's default
    /// keepalive + retry options. Reuse the returned channel across all RPCs.
    /// </summary>
    public static GrpcChannel ForAddress(string address) =>
        GrpcChannel.ForAddress(address, DefaultOptions());
}
