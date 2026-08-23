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

Console.WriteLine($"rows={response.Rows.Count}");
