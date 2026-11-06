// The data path a caller depends on, through this SDK: a value beyond 2^53 stays exact, a value the column cannot hold is refused, and a delete removes the row.
//
// Gated on UDB_LIVE_SDK_TESTS=1. CI's "SDK refresh-token rotation (live)" job runs it.

using System.Text;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Udb.Entity.V1;
using Xunit;
using AuthnV1 = Udb.Core.Authn.Services.V1;

namespace Udb.Client.Tests;

public sealed class LiveDataContractTests
{
    private const string File = "udb.core.storage.entity.v1.File";
    private const string BeyondDouble = "9007199254740993";

    private static string Env(string name, string? fallback = null)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (!string.IsNullOrWhiteSpace(value))
        {
            return value.Trim();
        }
        return fallback ?? throw new InvalidOperationException($"{name} is required when UDB_LIVE_SDK_TESTS=1");
    }

    private static string Url(string target) =>
        target.Contains("://", StringComparison.Ordinal) ? target : $"http://{target}";

    private static ByteString Row(string id, string size) => ByteString.CopyFrom(
        $"{{\"file_id\":\"{id}\",\"filename\":\"a.pdf\",\"object_key\":\"contract/{id}\",\"status\":\"PENDING\",\"size_bytes\":{size}}}",
        Encoding.UTF8);

    private static Struct Key(string id)
    {
        var filter = new Struct();
        filter.Fields["file_id"] = Value.ForString(id);
        return filter;
    }

    [Fact]
    public async Task LiveDataContractHolds()
    {
        if (Environment.GetEnvironmentVariable("UDB_LIVE_SDK_TESTS") != "1")
        {
            return;
        }
        var target = Env("UDB_GRPC_TARGET");
        var tenant = Env("UDB_LIVE_TENANT", "sdk-live");
        var projectId = Env("UDB_LIVE_PROJECT", "default");
        await using var project = await UdbProject.ProjectAsync(new UdbProjectConfig
        {
            Target = Url(target),
            AuthTarget = Url(Env("UDB_AUTH_GRPC_TARGET", target)),
            TenantId = tenant,
            ProjectId = projectId,
            Purpose = "csharp.live.data-contract",
            CorrelationId = "csharp-live-data-contract",
            ServiceIdentity = "csharp.sdk.live",
        });
        await project.LoginAndAdoptTenantAsync(new AuthnV1.LoginRequest
        {
            Username = Env("UDB_LIVE_USERNAME"),
            Password = Env("UDB_LIVE_PASSWORD"),
            TenantHint = tenant,
            ProjectHint = projectId,
            DeviceName = "csharp-sdk-data-contract",
        });

        var id = Guid.NewGuid().ToString();
        await project.UpsertAsync(new UpsertRequest { MessageType = File, RecordJson = Row(id, BeyondDouble) });
        var read = new SelectRequest { MessageType = File, Filter = Key(id), Limit = 1 };
        var found = await project.SelectAsync(read);
        Assert.Single(found.RecordsJson);
        var text = found.RecordsJson[0].ToStringUtf8();
        Assert.Contains(BeyondDouble, text);

        var refused = await Assert.ThrowsAnyAsync<Exception>(() =>
            project.UpsertAsync(new UpsertRequest { MessageType = File, RecordJson = Row(id, "1.5") }));
        Assert.Contains("InvalidArgument", refused.ToString());

        await project.DeleteAsync(new DeleteRequest { MessageType = File, Filter = Key(id) });
        Assert.Empty((await project.SelectAsync(read)).RecordsJson);
    }
}
