package dev.udb.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.grpc.Metadata;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

final class UdbClientTest {
  @Test
  void headersIncludeRequiredUdbMetadata() {
    UdbMetadata metadata =
        new UdbMetadata(
            "tenant-a",
            "read",
            "corr-1",
            List.of("udb:read", "udb:portal:viewer"),
            "orders.service",
            "user-1",
            "project-a",
            "catalog-v1");

    Metadata headers = UdbClient.headers(metadata);

    assertEquals("tenant-a", header(headers, "x-tenant-id"));
    assertEquals("user-1", header(headers, "x-user-id"));
    assertEquals("read", header(headers, "x-purpose"));
    assertEquals("corr-1", header(headers, "x-correlation-id"));
    assertEquals("udb:read,udb:portal:viewer", header(headers, "x-scopes"));
    assertEquals("orders.service", header(headers, "x-service-identity"));
    assertEquals("project-a", header(headers, "x-udb-project-id"));
    assertEquals("catalog-v1", header(headers, "x-udb-client-catalog-version"));
  }

  @Test
  void afterWriteInstallsGoldenReadFenceHeader() throws Exception {
    String golden = Files.readString(Path.of("..", "..", "docs", "generated", "consistency-golden.json"));
    WriteReceipt receipt = WriteReceipt.fromJson(golden);

    UdbMetadata metadata =
        new UdbMetadata("tenant-a", "read", "corr-1", List.of(), "orders.service", "", "project-a", "")
            .afterWrite(receipt);

    assertEquals(
        "{\"max_wait_ms\":2500,\"min_outbox_lsn\":\"0/1A2B3C4D\",\"projection_task_ids\":[\"projection-task-a\",\"projection-task-b\"]}",
        header(UdbClient.headers(metadata), "x-udb-read-fence"));
  }

  private static String header(Metadata headers, String name) {
    return headers.get(Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER));
  }
}
