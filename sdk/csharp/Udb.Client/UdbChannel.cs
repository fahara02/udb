using System.Net.Http;
using Grpc.Net.Client;

namespace Udb.Client;

/// <summary>
/// Factory for the long-lived UDB <see cref="GrpcChannel"/>. Construct a channel
/// once and reuse it across every RPC: a fresh channel forces a TCP+TLS+HTTP/2
/// handshake on each call, which dominates per-RPC latency. The defaults here keep
/// an idle connection warm (HTTP/2 keepalive). Retry policy belongs in the generated
/// wrapper, where proto-derived operation_kind is known. <see cref="GrpcChannel"/>
/// already auto-enables
/// <c>EnableMultipleHttp2Connections</c>, so it pools connections past the server's
/// max-concurrent-streams without us touching that server limit.
/// </summary>
public static class UdbChannel
{
    // Keepalive: ping an idle connection every 30s, give the ack 10s.
    private static readonly TimeSpan KeepAlivePingDelay = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan KeepAlivePingTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Build the default <see cref="GrpcChannelOptions"/> (keepalive only).</summary>
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
    };

    /// <summary>
    /// Create a long-lived channel to <paramref name="address"/> with UDB's default
    /// keepalive options. Reuse the returned channel across all RPCs.
    /// </summary>
    public static GrpcChannel ForAddress(string address) =>
        GrpcChannel.ForAddress(address, DefaultOptions());
}
