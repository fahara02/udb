using System.Net.Http;
using System.Net.Security;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
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

    /// <summary>
    /// Create a long-lived <c>https://</c> channel that trusts
    /// <paramref name="trustedRoots"/> instead of the system store (a private CA) and,
    /// when <paramref name="clientCertificate"/> is set, presents it to a mutual-TLS
    /// endpoint. Either may be null; both null is <see cref="ForAddress(string)"/>.
    /// </summary>
    public static GrpcChannel ForAddress(
        string address,
        X509Certificate2Collection? trustedRoots,
        X509Certificate2? clientCertificate) =>
        GrpcChannel.ForAddress(address, DefaultOptions(trustedRoots, clientCertificate));

    /// <summary>
    /// <see cref="DefaultOptions()"/> plus a custom trust store and an optional client
    /// certificate on the handler's TLS options.
    /// </summary>
    public static GrpcChannelOptions DefaultOptions(
        X509Certificate2Collection? trustedRoots,
        X509Certificate2? clientCertificate)
    {
        var options = DefaultOptions();
        var handler = (SocketsHttpHandler)options.HttpHandler!;
        if (clientCertificate is not null)
        {
            if (!clientCertificate.HasPrivateKey)
            {
                throw new ArgumentException(
                    "udb: the client certificate has no private key; mutual TLS needs both", nameof(clientCertificate));
            }
            handler.SslOptions.ClientCertificates = new X509CertificateCollection { clientCertificate };
        }
        if (trustedRoots is { Count: > 0 })
        {
            var policy = new X509ChainPolicy
            {
                TrustMode = X509ChainTrustMode.CustomRootTrust,
                // Same as the SslStream default: no revocation fetch for a private CA.
                RevocationMode = X509RevocationMode.NoCheck,
            };
            policy.CustomTrustStore.AddRange(trustedRoots);
            handler.SslOptions.CertificateChainPolicy = policy;
        }
        return options;
    }

    /// <summary>
    /// Load a PEM client certificate chain and its PEM private key (for example the
    /// files an nginx <c>ssl_verify_client</c> deployment issues) as a certificate
    /// usable for mutual TLS.
    /// </summary>
    public static X509Certificate2 LoadClientCertificate(string certificatePemPath, string privateKeyPemPath)
    {
        var certificate = X509Certificate2.CreateFromPemFile(certificatePemPath, privateKeyPemPath);
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return certificate;
        }
        // SChannel cannot use a PEM key held only in memory; round-trip through
        // PKCS#12 so the key is usable for the TLS handshake on Windows.
        using (certificate)
        {
            return new X509Certificate2(certificate.Export(X509ContentType.Pkcs12));
        }
    }

    /// <summary>Load every certificate in a PEM bundle, for <c>trustedRoots</c>.</summary>
    public static X509Certificate2Collection LoadTrustedRoots(string pemBundlePath)
    {
        var roots = new X509Certificate2Collection();
        roots.ImportFromPemFile(pemBundlePath);
        if (roots.Count == 0)
        {
            throw new ArgumentException($"udb: {pemBundlePath} holds no PEM certificate", nameof(pemBundlePath));
        }
        return roots;
    }
}
