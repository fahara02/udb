using System.Net.Http;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Xunit;

namespace Udb.Client.Tests;

/// <summary>
/// A private CA and a mutual-TLS client certificate reach the channel's TLS options.
/// No broker: the handler is inspected, and no call is dispatched.
/// </summary>
public sealed class UdbChannelTlsTests
{
    private static X509Certificate2 SelfSigned()
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=udb-test", key, HashAlgorithmName.SHA256);
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow.AddHours(1));
    }

    [Fact]
    public void TrustedRootsAndClientCertificateReachTheHandler()
    {
        using var cert = SelfSigned();
        var options = UdbChannel.DefaultOptions(new X509Certificate2Collection { cert }, cert);
        var handler = Assert.IsType<SocketsHttpHandler>(options.HttpHandler);
        var clientCertificates = handler.SslOptions.ClientCertificates!;
        Assert.Same(cert, Assert.Single(clientCertificates));
        var policy = Assert.IsType<X509ChainPolicy>(handler.SslOptions.CertificateChainPolicy);
        Assert.Equal(X509ChainTrustMode.CustomRootTrust, policy.TrustMode);
        Assert.Single(policy.CustomTrustStore);
    }

    [Fact]
    public void NoTlsSettingsLeaveTheDefaults()
    {
        var handler = Assert.IsType<SocketsHttpHandler>(UdbChannel.DefaultOptions(null, null).HttpHandler);
        Assert.Null(handler.SslOptions.ClientCertificates);
        Assert.Null(handler.SslOptions.CertificateChainPolicy);
    }

    [Fact]
    public void ClientCertificateWithoutKeyIsRefused()
    {
        using var cert = SelfSigned();
        using var publicOnly = new X509Certificate2(cert.Export(X509ContentType.Cert));
        Assert.Throws<ArgumentException>(() => UdbChannel.DefaultOptions(null, publicOnly));
    }

    [Fact]
    public void ProjectDialsWithTheTlsSettings()
    {
        using var cert = SelfSigned();
        using var project = UdbProject.Open(new UdbProjectConfig
        {
            Target = "https://localhost:1",
            TlsTrustedRoots = new X509Certificate2Collection { cert },
            TlsClientCertificate = cert,
        });
        Assert.NotNull(project.DataChannelForTesting);
    }
}
