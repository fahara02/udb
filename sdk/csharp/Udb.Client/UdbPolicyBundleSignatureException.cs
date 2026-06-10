namespace Udb.Client;

/// <summary>
/// Thrown by <see cref="UdbAuthClient.VerifyPolicyBundle"/> (and by
/// <see cref="UdbAuthClient.GetPolicyBundleAsync"/> when a bundle secret is
/// configured) when a signed policy bundle's HMAC-SHA256 signature does not
/// match the recomputed value — i.e. the bundle was tampered with or signed
/// under a different secret. Parity with the Go/Python/TS bundle-verification
/// error types (M5.3).
/// </summary>
public sealed class UdbPolicyBundleSignatureException : Exception
{
    public UdbPolicyBundleSignatureException(string keyId, string algorithm)
        : base($"udb: policy bundle signature verification failed (key_id='{keyId}', algorithm='{algorithm}')")
    {
        KeyId = keyId;
        Algorithm = algorithm;
    }

    /// <summary>The signing key id reported by the server, if any.</summary>
    public string KeyId { get; }

    /// <summary>The signature algorithm reported by the server (e.g. "HMAC-SHA256").</summary>
    public string Algorithm { get; }
}
