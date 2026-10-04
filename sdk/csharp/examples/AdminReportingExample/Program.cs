using Udb.Client;
using Udb.Entity.V1;

await using var client = new UdbClient(
    "http://localhost:50051",
    new UdbMetadata(
        TenantId: "tenant-1",
        Purpose: "admin-report",
        CorrelationId: "csharp-admin-example",
        Scopes: ["udb:read", "udb:admin"],
        ServiceIdentity: "example.service",
        ProjectId: "default",
        ClientCatalogVersion: "1.0.0"));

var response = await client.SelectAsync(new SelectRequest
{
    MessageType = "example.report.v1.ReportExecution",
    Limit = 25
});

// RecordsJson is the canonical representation; Rows is a compatibility field the
// broker sends EMPTY, one entry per record — counting it looks right, reading it
// finds nothing.
Console.WriteLine($"records={response.RecordsJson.Count}");
