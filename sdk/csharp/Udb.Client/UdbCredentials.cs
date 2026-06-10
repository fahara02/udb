namespace Udb.Client;

/// <summary>
/// Mutable, thread-safe holder for the per-call auth credentials (bearer token
/// and API key). A single instance is shared across a <see cref="UdbProject"/>'s
/// clients (data, auth, control-plane, storage, asset, WebRTC), so a refreshed
/// token reaches every outbound call without rebuilding channels or clients —
/// the C# analogue of the TypeScript <c>core.setCredentials</c> hot-swap.
/// </summary>
public sealed class UdbCredentials
{
    private volatile string _bearerToken;
    private volatile string _apiKey;

    public UdbCredentials(string bearerToken = "", string apiKey = "")
    {
        _bearerToken = bearerToken ?? "";
        _apiKey = apiKey ?? "";
    }

    /// <summary>Current bearer token (empty when unset).</summary>
    public string BearerToken => _bearerToken;

    /// <summary>Current API key (empty when unset).</summary>
    public string ApiKey => _apiKey;

    /// <summary>Replace both bearer token and API key.</summary>
    public void Set(string? bearerToken, string? apiKey)
    {
        _bearerToken = bearerToken ?? "";
        _apiKey = apiKey ?? "";
    }

    /// <summary>Replace just the bearer token (keeps the API key).</summary>
    public void SetBearerToken(string? bearerToken) => _bearerToken = bearerToken ?? "";

    /// <summary>Replace just the API key (keeps the bearer token).</summary>
    public void SetApiKey(string? apiKey) => _apiKey = apiKey ?? "";
}
