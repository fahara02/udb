using Grpc.Core;
using Grpc.Net.Client;
using Udb.Entity.V1;
using Udb.Services.V1;

namespace Udb.Client;

public sealed record UdbMetadata(
    string TenantId,
    string Purpose,
    string CorrelationId,
    string[] Scopes,
    string ServiceIdentity,
    string UserId = "",
    string ProjectId = "default",
    string ClientCatalogVersion = UdbClient.ProtocolVersion);

public sealed class UdbClient : IAsyncDisposable
{
    public const string ProtocolVersion = "1.0.0";

    private readonly GrpcChannel _channel;
    private readonly UdbMetadata _metadata;

    public UdbClient(string address, UdbMetadata metadata)
    {
        _channel = GrpcChannel.ForAddress(address);
        _metadata = metadata;
        Broker = new DataBroker.DataBrokerClient(_channel);
    }

    public DataBroker.DataBrokerClient Broker { get; }

    public Metadata Headers()
    {
        return new Metadata
        {
            { "x-tenant-id", _metadata.TenantId },
            { "x-user-id", _metadata.UserId },
            { "x-purpose", _metadata.Purpose },
            { "x-correlation-id", _metadata.CorrelationId },
            { "x-scopes", string.Join(",", _metadata.Scopes) },
            { "x-service-identity", _metadata.ServiceIdentity },
            { "x-udb-project-id", _metadata.ProjectId },
            { "x-udb-client-catalog-version", _metadata.ClientCatalogVersion }
        };
    }

    public Task<RecordSet> SelectAsync(SelectRequest request, CancellationToken cancellationToken = default)
    {
        return Broker.SelectAsync(request, Headers(), cancellationToken: cancellationToken).ResponseAsync;
    }

    public Task<MutationResponse> UpsertAsync(UpsertRequest request, CancellationToken cancellationToken = default)
    {
        return Broker.UpsertAsync(request, Headers(), cancellationToken: cancellationToken).ResponseAsync;
    }

    public ValueTask DisposeAsync()
    {
        _channel.Dispose();
        return ValueTask.CompletedTask;
    }
}
