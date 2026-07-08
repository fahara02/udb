using Google.Protobuf;
using Udb.Entity.V1;
using Xunit;

namespace Udb.Client.Tests;

public sealed class UdbEntityHandleTests
{
    [Fact]
    public async Task BoundHandleShapesDataBrokerCrudRequests()
    {
        var metadata = new UdbMetadata(
            TenantId: "tenant-a",
            Purpose: "unit-test",
            CorrelationId: "corr-1",
            Scopes: new[] { "entities:read", "entities:write" },
            ServiceIdentity: "svc",
            UserId: "user-1",
            ProjectId: "project-a");

        SelectRequest? seenSelect = null;
        UpsertRequest? seenUpsert = null;
        DeleteRequest? seenDelete = null;

        var handle = new UdbEntityHandle(
            "example.invoice.v1.Invoice",
            new[] { "invoice_id" },
            () => metadata,
            (request, _) =>
            {
                seenSelect = request;
                var rows = new RecordSet();
                rows.RecordsJson.Add(ByteString.CopyFromUtf8("{\"invoice_id\":\"inv-1\",\"total_cents\":42}"));
                return Task.FromResult(rows);
            },
            (request, _) =>
            {
                seenUpsert = request;
                return Task.FromResult(new MutationResponse { AffectedRows = 1 });
            },
            (request, _) =>
            {
                seenDelete = request;
                return Task.FromResult(new MutationResponse { AffectedRows = 1 });
            });

        var rows = await handle.SelectAsync(new Dictionary<string, object?> { ["tenant_id"] = "tenant-a" });
        await handle.UpsertAsync(
            new Dictionary<string, object?> { ["invoice_id"] = "inv-1", ["total_cents"] = 42 },
            returnRecord: true,
            idempotencyKey: "idem-1");
        await handle.DeleteAsync(new Dictionary<string, object?> { ["invoice_id"] = "inv-1" }, "idem-2");

        Assert.NotNull(seenSelect);
        Assert.Equal("tenant-a", seenSelect!.Context.TenantId);
        Assert.Equal("project-a", seenSelect.Context.ProjectId);
        Assert.Equal("example.invoice.v1.Invoice", seenSelect.MessageType);
        Assert.Equal("tenant-a", seenSelect.Filter.Fields["tenant_id"].StringValue);
        Assert.Single(rows);
        Assert.Equal("inv-1", rows[0]["invoice_id"]);

        Assert.NotNull(seenUpsert);
        Assert.Equal("example.invoice.v1.Invoice", seenUpsert!.MessageType);
        Assert.Equal(new[] { "invoice_id" }, seenUpsert.ConflictFields);
        Assert.True(seenUpsert.ReturnRecord);
        Assert.Equal("idem-1", seenUpsert.IdempotencyKey);
        Assert.Contains("\"invoice_id\":\"inv-1\"", seenUpsert.RecordJson.ToStringUtf8());
        Assert.Equal(42, seenUpsert.Payload.Fields["total_cents"].NumberValue);

        Assert.NotNull(seenDelete);
        Assert.Equal("example.invoice.v1.Invoice", seenDelete!.MessageType);
        Assert.Equal("idem-2", seenDelete.IdempotencyKey);
        Assert.Equal("inv-1", seenDelete.Filter.Fields["invoice_id"].StringValue);
    }
}
