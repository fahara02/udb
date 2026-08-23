using Grpc.Core;
using AssetV1 = Udb.Core.Asset.Services.V1;

namespace Udb.Client;

/// <summary>
/// Ergonomic async facade over the native <c>AssetService</c> (asset registration
/// and processing pipelines). Each wrapper applies the shared <see cref="UdbProject"/>
/// metadata headers and returns the unwrapped response task. The raw generated
/// client stays reachable via <see cref="Raw"/>.
/// </summary>
public sealed class UdbAssetClient
{
    private readonly AssetV1.AssetService.AssetServiceClient _client;
    private readonly Func<Metadata> _headers;

    internal UdbAssetClient(AssetV1.AssetService.AssetServiceClient client, Func<Metadata> headers)
    {
        _client = client;
        _headers = headers;
    }

    /// <summary>Raw generated asset service client.</summary>
    public AssetV1.AssetService.AssetServiceClient Raw => _client;

    /// <summary>Create a reusable pipeline definition.</summary>
    public Task<AssetV1.CreatePipelineDefinitionResponse> CreatePipelineDefinitionAsync(
        AssetV1.CreatePipelineDefinitionRequest request, CancellationToken ct = default)
        => _client.CreatePipelineDefinitionAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>Get a pipeline definition.</summary>
    public Task<AssetV1.GetPipelineDefinitionResponse> GetPipelineDefinitionAsync(
        AssetV1.GetPipelineDefinitionRequest request, CancellationToken ct = default)
        => _client.GetPipelineDefinitionAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>Register a managed asset wrapping a storage file.</summary>
    public Task<AssetV1.RegisterAssetResponse> RegisterAssetAsync(
        AssetV1.RegisterAssetRequest request, CancellationToken ct = default)
        => _client.RegisterAssetAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>Start a pipeline instance for an asset.</summary>
    public Task<AssetV1.StartPipelineResponse> StartPipelineAsync(
        AssetV1.StartPipelineRequest request, CancellationToken ct = default)
        => _client.StartPipelineAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>Get a pipeline instance with its steps.</summary>
    public Task<AssetV1.GetPipelineResponse> GetPipelineAsync(
        AssetV1.GetPipelineRequest request, CancellationToken ct = default)
        => _client.GetPipelineAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>Complete (or skip/fail) a pipeline step.</summary>
    public Task<AssetV1.CompleteStepResponse> CompleteStepAsync(
        AssetV1.CompleteStepRequest request, CancellationToken ct = default)
        => _client.CompleteStepAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>List assets.</summary>
    public Task<AssetV1.ListAssetsResponse> ListAssetsAsync(
        AssetV1.ListAssetsRequest request, CancellationToken ct = default)
        => _client.ListAssetsAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    /// <summary>Get an asset.</summary>
    public Task<AssetV1.GetAssetResponse> GetAssetAsync(
        AssetV1.GetAssetRequest request, CancellationToken ct = default)
        => _client.GetAssetAsync(request, _headers(), cancellationToken: ct).ResponseAsync;
}
