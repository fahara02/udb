using Grpc.Core;
using Grpc.Net.Client;
using System.Text.Json;
using System.Text.Json.Serialization;
using Udb.Entity.V1;
using Udb.Services.V1;

namespace Udb.Client;

public sealed record WriteReceipt(
    [property: JsonPropertyName("source_lsn")] string SourceLsn = "",
    [property: JsonPropertyName("outbox_seq")] long OutboxSeq = 0,
    [property: JsonPropertyName("projection_task_ids")] string[]? ProjectionTaskIds = null,
    [property: JsonPropertyName("manifest_checksum")] string ManifestChecksum = "",
    [property: JsonPropertyName("written_at_unix_ms")] long WrittenAtUnixMs = 0)
{
    public static WriteReceipt FromJson(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return new WriteReceipt();
        }
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        if (root.ValueKind == JsonValueKind.Object &&
            root.TryGetProperty("write_receipt", out var wrapped))
        {
            root = wrapped;
        }
        return JsonSerializer.Deserialize<WriteReceipt>(root.GetRawText()) ?? new WriteReceipt();
    }

    public bool IsEmpty =>
        string.IsNullOrEmpty(SourceLsn) &&
        OutboxSeq == 0 &&
        (ProjectionTaskIds is null || ProjectionTaskIds.Length == 0) &&
        string.IsNullOrEmpty(ManifestChecksum) &&
        WrittenAtUnixMs == 0;

    public string ToJson() => JsonSerializer.Serialize(new SortedDictionary<string, object?>
    {
        ["manifest_checksum"] = ManifestChecksum,
        ["outbox_seq"] = OutboxSeq,
        ["projection_task_ids"] = ProjectionTaskIds ?? Array.Empty<string>(),
        ["source_lsn"] = SourceLsn,
        ["written_at_unix_ms"] = WrittenAtUnixMs,
    });
}

/// <summary>
/// Ergonomic view over a <see cref="MutationResponse"/> returned by an upsert/delete.
/// Surfaces the broker's <c>was_duplicate</c> flag — true when the write was collapsed
/// as a durable-idempotency replay rather than applied as a fresh mutation — alongside
/// the typed <see cref="WriteReceipt"/> and the raw response (kept available for
/// everything else: mutation_id, affected_rows, warnings, metadata, …).
/// The flag also stays reachable directly as <c>response.WasDuplicate</c>; this wrapper
/// makes the replay-vs-fresh distinction discoverable at the facade layer.
/// </summary>
public sealed record MutationOutcome(bool WasDuplicate, WriteReceipt WriteReceipt, MutationResponse Response)
{
    /// <summary>Derive an outcome from a raw upsert/delete <see cref="MutationResponse"/>.</summary>
    public static MutationOutcome From(MutationResponse response)
    {
        ArgumentNullException.ThrowIfNull(response);
        return new MutationOutcome(
            response.WasDuplicate,
            WriteReceipt.FromJson(response.WriteReceiptJson),
            response);
    }
}

public sealed record ReadFence(
    [property: JsonPropertyName("min_outbox_lsn")] string MinOutboxLsn = "",
    [property: JsonPropertyName("projection_task_ids")] string[]? ProjectionTaskIds = null,
    [property: JsonPropertyName("max_wait_ms")] long MaxWaitMs = ReadFence.DefaultMaxWaitMs)
{
    public const long DefaultMaxWaitMs = 2500;

    public static ReadFence FromReceipt(WriteReceipt receipt, long maxWaitMs = DefaultMaxWaitMs)
        => new(receipt.SourceLsn, receipt.ProjectionTaskIds?.ToArray() ?? Array.Empty<string>(), maxWaitMs);

    public bool IsEmpty =>
        string.IsNullOrEmpty(MinOutboxLsn) &&
        (ProjectionTaskIds is null || ProjectionTaskIds.Length == 0);

    public string ToJson()
    {
        var payload = new SortedDictionary<string, object?>
        {
            ["max_wait_ms"] = MaxWaitMs,
        };
        if (!string.IsNullOrEmpty(MinOutboxLsn))
        {
            payload["min_outbox_lsn"] = MinOutboxLsn;
        }
        if (ProjectionTaskIds is { Length: > 0 })
        {
            payload["projection_task_ids"] = ProjectionTaskIds;
        }
        return JsonSerializer.Serialize(payload);
    }
}

public sealed record UdbMetadata(
    string TenantId,
    string Purpose,
    string CorrelationId,
    string[] Scopes,
    string ServiceIdentity,
    string UserId = "",
    string ProjectId = "default",
    string ClientCatalogVersion = UdbClient.ProtocolVersion,
    string BearerToken = "",
    string ApiKey = "",
    string Consistency = "",
    bool PrimaryRead = false,
    long MaxReplicaLagMs = 0,
    bool EventualConsistencyAllowed = false,
    string ReadFenceJson = "")
{
    public UdbMetadata WithReadFence(string readFenceJson) => this with
    {
        ReadFenceJson = readFenceJson ?? string.Empty,
    };

    public UdbMetadata AfterWrite(WriteReceipt receipt, long maxWaitMs = ReadFence.DefaultMaxWaitMs)
        => WithReadFence(ReadFence.FromReceipt(receipt, maxWaitMs).ToJson());
}

public sealed class UdbClient : IAsyncDisposable
{
    public const string ProtocolVersion = "1.0.0";

    private readonly GrpcChannel _channel;
    private readonly UdbMetadata _metadata;

    public UdbClient(string address, UdbMetadata metadata)
    {
        _channel = UdbChannel.ForAddress(address);
        _metadata = metadata;
        Broker = new DataBroker.DataBrokerClient(_channel);
    }

    public DataBroker.DataBrokerClient Broker { get; }

    public Metadata Headers()
    {
        var headers = new Metadata
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
        if (!string.IsNullOrWhiteSpace(_metadata.BearerToken))
        {
            headers.Add("authorization", $"Bearer {_metadata.BearerToken}");
        }
        if (!string.IsNullOrWhiteSpace(_metadata.ApiKey))
        {
            headers.Add("x-api-key", _metadata.ApiKey);
        }
        if (!string.IsNullOrWhiteSpace(_metadata.Consistency))
        {
            headers.Add("x-udb-consistency", _metadata.Consistency);
        }
        if (_metadata.PrimaryRead)
        {
            headers.Add("x-udb-primary-read", "true");
        }
        if (_metadata.MaxReplicaLagMs > 0)
        {
            headers.Add("x-udb-max-replica-lag-ms", _metadata.MaxReplicaLagMs.ToString());
        }
        if (_metadata.EventualConsistencyAllowed)
        {
            headers.Add("x-udb-eventual-consistency-allowed", "true");
        }
        if (!string.IsNullOrWhiteSpace(_metadata.ReadFenceJson))
        {
            headers.Add("x-udb-read-fence", _metadata.ReadFenceJson);
        }
        return headers;
    }

    public Task<RecordSet> SelectAsync(SelectRequest request, CancellationToken cancellationToken = default)
    {
        return Broker.SelectAsync(request, Headers(), cancellationToken: cancellationToken).ResponseAsync;
    }

    public Task<MutationResponse> UpsertAsync(UpsertRequest request, CancellationToken cancellationToken = default)
    {
        return Broker.UpsertAsync(request, Headers(), cancellationToken: cancellationToken).ResponseAsync;
    }

    public Task<MutationResponse> DeleteAsync(DeleteRequest request, CancellationToken cancellationToken = default)
    {
        return Broker.DeleteAsync(request, Headers(), cancellationToken: cancellationToken).ResponseAsync;
    }

    public UdbEntityHandle Entity(string messageType, params string[] key)
        => UdbEntityHandle.For(messageType, key, () => _metadata, SelectAsync, UpsertAsync, DeleteAsync);

    public UdbEntityHandle Table(string name, params string[] key)
        => UdbEntityHandle.ForTable(name, key, () => _metadata, SelectAsync, UpsertAsync, DeleteAsync);

    public ValueTask DisposeAsync()
    {
        _channel.Dispose();
        return ValueTask.CompletedTask;
    }
}
