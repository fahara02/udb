using System.Collections;
using System.Globalization;
using System.Text.Json;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Udb.Client.Generated;
using Udb.Entity.V1;

namespace Udb.Client;

/// <summary>Bound relational entity/table facade over the canonical DataBroker RPCs.</summary>
public sealed class UdbEntityHandle
{
    private readonly Func<UdbMetadata> _metadata;
    private readonly Func<SelectRequest, CancellationToken, Task<RecordSet>> _select;
    private readonly Func<UpsertRequest, CancellationToken, Task<MutationResponse>> _upsert;
    private readonly Func<DeleteRequest, CancellationToken, Task<MutationResponse>> _delete;

    internal UdbEntityHandle(
        string messageType,
        IEnumerable<string> key,
        Func<UdbMetadata> metadata,
        Func<SelectRequest, CancellationToken, Task<RecordSet>> select,
        Func<UpsertRequest, CancellationToken, Task<MutationResponse>> upsert,
        Func<DeleteRequest, CancellationToken, Task<MutationResponse>> delete)
    {
        if (string.IsNullOrWhiteSpace(messageType))
        {
            throw new ArgumentException("udb: messageType is required", nameof(messageType));
        }
        MessageType = messageType;
        Key = key.ToArray();
        _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        _select = select ?? throw new ArgumentNullException(nameof(select));
        _upsert = upsert ?? throw new ArgumentNullException(nameof(upsert));
        _delete = delete ?? throw new ArgumentNullException(nameof(delete));
    }

    public string MessageType { get; }

    public IReadOnlyList<string> Key { get; }

    public async Task<IReadOnlyList<IReadOnlyDictionary<string, object?>>> SelectAsync(
        IReadOnlyDictionary<string, object?>? where = null,
        int limit = 0,
        CancellationToken ct = default)
    {
        var raw = await SelectRawAsync(where, limit, ct).ConfigureAwait(false);
        return DecodeRecords(raw);
    }

    public Task<RecordSet> SelectRawAsync(
        IReadOnlyDictionary<string, object?>? where = null,
        int limit = 0,
        CancellationToken ct = default)
    {
        var request = new SelectRequest
        {
            Context = Context(_metadata()),
            MessageType = MessageType,
            Filter = ToStruct(where),
            Limit = Math.Max(0, limit),
        };
        return _select(request, ct);
    }

    public Task<MutationResponse> UpsertAsync(
        IReadOnlyDictionary<string, object?> record,
        bool returnRecord = false,
        string idempotencyKey = "",
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(record);
        var request = new UpsertRequest
        {
            Context = Context(_metadata()),
            MessageType = MessageType,
            RecordJson = ByteString.CopyFromUtf8(RecordJson(record)),
            Payload = ToStruct(record),
            ReturnRecord = returnRecord,
            IdempotencyKey = idempotencyKey ?? string.Empty,
        };
        request.ConflictFields.AddRange(Key);
        return _upsert(request, ct);
    }

    public Task<MutationResponse> DeleteAsync(
        IReadOnlyDictionary<string, object?>? where = null,
        string idempotencyKey = "",
        CancellationToken ct = default)
    {
        var request = new DeleteRequest
        {
            Context = Context(_metadata()),
            MessageType = MessageType,
            Filter = ToStruct(where),
            IdempotencyKey = idempotencyKey ?? string.Empty,
        };
        return _delete(request, ct);
    }

    internal static UdbEntityHandle For(
        string messageType,
        IEnumerable<string>? key,
        Func<UdbMetadata> metadata,
        Func<SelectRequest, CancellationToken, Task<RecordSet>> select,
        Func<UpsertRequest, CancellationToken, Task<MutationResponse>> upsert,
        Func<DeleteRequest, CancellationToken, Task<MutationResponse>> delete)
    {
        var resolvedKey = key is null || !key.Any() ? ResolveEntityKey(messageType) : key.ToArray();
        return new UdbEntityHandle(messageType, resolvedKey, metadata, select, upsert, delete);
    }

    internal static UdbEntityHandle ForTable(
        string name,
        IEnumerable<string>? key,
        Func<UdbMetadata> metadata,
        Func<SelectRequest, CancellationToken, Task<RecordSet>> select,
        Func<UpsertRequest, CancellationToken, Task<MutationResponse>> upsert,
        Func<DeleteRequest, CancellationToken, Task<MutationResponse>> delete)
    {
        var binding = UdbIr.Entities.Values.FirstOrDefault(item => item.Table == name);
        var messageType = binding?.MessageType ?? name;
        var resolvedKey = key is null || !key.Any() ? binding?.PrimaryKeys ?? Array.Empty<string>() : key.ToArray();
        return new UdbEntityHandle(messageType, resolvedKey, metadata, select, upsert, delete);
    }

    private static IReadOnlyList<string> ResolveEntityKey(string messageType)
        => UdbIr.Entities.TryGetValue(messageType, out var binding)
            ? binding.PrimaryKeys
            : Array.Empty<string>();

    private static RequestContext Context(UdbMetadata metadata)
    {
        var context = new RequestContext
        {
            TenantId = metadata.TenantId,
            UserId = metadata.UserId,
            CorrelationId = metadata.CorrelationId,
            Purpose = metadata.Purpose,
            ServiceIdentity = metadata.ServiceIdentity,
            ProjectId = metadata.ProjectId,
            ClientCatalogVersion = metadata.ClientCatalogVersion,
            Consistency = metadata.Consistency,
            PrimaryRead = metadata.PrimaryRead,
            MaxReplicaLagMs = (ulong)Math.Max(0, metadata.MaxReplicaLagMs),
            EventualConsistencyAllowed = metadata.EventualConsistencyAllowed,
            ReadFenceJson = metadata.ReadFenceJson,
        };
        context.Scopes.AddRange(metadata.Scopes ?? Array.Empty<string>());
        return context;
    }

    private static Struct ToStruct(IReadOnlyDictionary<string, object?>? values)
    {
        var output = new Struct();
        if (values is null)
        {
            return output;
        }
        foreach (var (key, value) in values)
        {
            output.Fields.Add(key, ToValue(value));
        }
        return output;
    }

    private static Value ToValue(object? value)
    {
        if (value is null)
        {
            return new Value { NullValue = NullValue.NullValue };
        }
        if (value is JsonElement json)
        {
            return ToValue(FromJson(json));
        }
        if (value is bool boolean)
        {
            return new Value { BoolValue = boolean };
        }
        if (IsNumber(value))
        {
            var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            if (!double.IsFinite(number))
            {
                throw new ArgumentException("udb: protobuf Struct does not support non-finite numbers");
            }
            return new Value { NumberValue = number };
        }
        if (value is IReadOnlyDictionary<string, object?> map)
        {
            return new Value { StructValue = ToStruct(map) };
        }
        if (value is IDictionary dictionary)
        {
            var nested = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (DictionaryEntry entry in dictionary)
            {
                nested[Convert.ToString(entry.Key, CultureInfo.InvariantCulture) ?? string.Empty] = entry.Value;
            }
            return new Value { StructValue = ToStruct(nested) };
        }
        if (value is IEnumerable items and not string)
        {
            var list = new ListValue();
            foreach (var item in items)
            {
                list.Values.Add(ToValue(item));
            }
            return new Value { ListValue = list };
        }
        return new Value { StringValue = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty };
    }

    private static bool IsNumber(object value)
        => value is byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal;

    private static string RecordJson(IReadOnlyDictionary<string, object?> record)
    {
        var sorted = new SortedDictionary<string, object?>(StringComparer.Ordinal);
        foreach (var (key, value) in record)
        {
            sorted[key] = value;
        }
        return JsonSerializer.Serialize(sorted);
    }

    private static IReadOnlyList<IReadOnlyDictionary<string, object?>> DecodeRecords(RecordSet recordSet)
    {
        var rows = new List<IReadOnlyDictionary<string, object?>>(recordSet.RecordsJson.Count);
        foreach (var recordJson in recordSet.RecordsJson)
        {
            using var document = JsonDocument.Parse(recordJson.ToStringUtf8());
            var value = FromJson(document.RootElement);
            if (value is IReadOnlyDictionary<string, object?> map)
            {
                rows.Add(map);
            }
            else
            {
                throw new InvalidOperationException("udb: DataBroker returned non-object record_json");
            }
        }
        return rows;
    }

    private static object? FromJson(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Object => element.EnumerateObject()
                .ToDictionary(prop => prop.Name, prop => FromJson(prop.Value), StringComparer.Ordinal),
            JsonValueKind.Array => element.EnumerateArray().Select(FromJson).ToArray(),
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var integer) ? integer : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => null,
        };
    }
}
