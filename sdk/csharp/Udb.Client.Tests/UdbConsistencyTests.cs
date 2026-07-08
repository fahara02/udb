using System.Runtime.CompilerServices;
using Xunit;

namespace Udb.Client.Tests;

public sealed class UdbConsistencyTests
{
    [Fact]
    public async Task AfterWrite_Installs_Golden_ReadFence_Header()
    {
        var receipt = WriteReceipt.FromJson(File.ReadAllText(RepoFile("docs/generated/consistency-golden.json")));
        var metadata = new UdbMetadata(
            TenantId: "tenant-a",
            Purpose: "read",
            CorrelationId: "corr-1",
            Scopes: Array.Empty<string>(),
            ServiceIdentity: "orders.service",
            ProjectId: "project-a").AfterWrite(receipt);

        await using var client = new UdbClient("http://127.0.0.1:1", metadata);
        var header = client.Headers().GetValue("x-udb-read-fence");

        Assert.Equal(
            "{\"max_wait_ms\":2500,\"min_outbox_lsn\":\"0/1A2B3C4D\",\"projection_task_ids\":[\"projection-task-a\",\"projection-task-b\"]}",
            header);
    }

    [Fact]
    public async Task Optional_Consistency_Headers_Are_Omitted_When_Unset()
    {
        var metadata = new UdbMetadata(
            TenantId: "tenant-a",
            Purpose: "read",
            CorrelationId: "corr-1",
            Scopes: Array.Empty<string>(),
            ServiceIdentity: "orders.service");

        await using var client = new UdbClient("http://127.0.0.1:1", metadata);
        Assert.Null(client.Headers().GetValue("x-udb-read-fence"));
        Assert.Null(client.Headers().GetValue("x-udb-consistency"));
    }

    private static string RepoFile(string relative, [CallerFilePath] string sourceFile = "")
    {
        var testDir = Path.GetDirectoryName(sourceFile)!;
        return Path.GetFullPath(Path.Combine(testDir, "..", "..", "..", relative));
    }
}
