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
  void headersAlwaysCarryARequestContext() {
    UdbMetadata metadata =
        new UdbMetadata("tenant-a", "read", "", List.of(), "orders.service", "", "project-a", "");

    Metadata first = UdbClient.headers(metadata);
    Metadata second = UdbClient.headers(metadata);

    String requestId = header(first, "x-request-id");
    assertEquals(36, requestId.length());
    assertEquals(requestId, header(first, "x-correlation-id"));
    org.junit.jupiter.api.Assertions.assertNotEquals(requestId, header(second, "x-request-id"));

    Metadata callerSupplied = new Metadata();
    callerSupplied.put(
        Metadata.Key.of("x-request-id", Metadata.ASCII_STRING_MARSHALLER), "caller-req");
    Metadata merged = UdbClient.withoutRequestContextOverride(callerSupplied, UdbClient.headers(metadata));
    callerSupplied.merge(merged);
    List<String> ids = new java.util.ArrayList<>();
    callerSupplied
        .getAll(Metadata.Key.of("x-request-id", Metadata.ASCII_STRING_MARSHALLER))
        .forEach(ids::add);
    assertEquals(List.of("caller-req"), ids);
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
