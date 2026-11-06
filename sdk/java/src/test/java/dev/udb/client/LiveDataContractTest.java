package dev.udb.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.udb.core.authn.services.v1.LoginRequest;
import com.udb.entity.v1.DeleteRequest;
import com.udb.entity.v1.RecordSet;
import com.udb.entity.v1.SelectRequest;
import com.udb.entity.v1.UpsertRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** The data path a caller depends on, through this SDK: a value beyond 2^53 stays exact, a value the column cannot hold is refused, and a delete removes the row. */
class LiveDataContractTest {
  private static final String FILE = "udb.core.storage.entity.v1.File";
  private static final String BEYOND_DOUBLE = "9007199254740993";

  private static String env(String name, String fallback) {
    String value = System.getenv(name);
    if (value != null && !value.isBlank()) {
      return value.trim();
    }
    if (fallback != null) {
      return fallback;
    }
    throw new IllegalStateException(name + " is required when UDB_LIVE_SDK_TESTS=1");
  }

  private static ByteString row(String id, String size) {
    return ByteString.copyFrom(
        ("{\"file_id\":\"" + id + "\",\"filename\":\"a.pdf\",\"object_key\":\"contract/" + id
                + "\",\"status\":\"PENDING\",\"size_bytes\":" + size + "}")
            .getBytes(StandardCharsets.UTF_8));
  }

  private static Struct key(String id) {
    return Struct.newBuilder()
        .putFields("file_id", Value.newBuilder().setStringValue(id).build())
        .build();
  }

  @Test
  void liveDataContractHolds() {
    assumeTrue("1".equals(System.getenv("UDB_LIVE_SDK_TESTS")), "requires live UDB broker");
    String target = env("UDB_GRPC_TARGET", null);
    String tenant = env("UDB_LIVE_TENANT", "sdk-live");
    String projectId = env("UDB_LIVE_PROJECT", "default");
    UdbProjectConfig config =
        UdbProjectConfig.builder()
            .target(target)
            .authTarget(env("UDB_AUTH_GRPC_TARGET", target))
            .tenantId(tenant)
            .projectId(projectId)
            .purpose("java.live.data-contract")
            .correlationId("java-live-data-contract")
            .serviceIdentity("java.sdk.live")
            .build();
    try (UdbProject project = UdbProject.open(config)) {
      project.loginAndAdoptTenant(
          LoginRequest.newBuilder()
              .setUsername(env("UDB_LIVE_USERNAME", null))
              .setPassword(env("UDB_LIVE_PASSWORD", null))
              .setTenantHint(tenant)
              .setProjectHint(projectId)
              .setDeviceName("java-sdk-data-contract")
              .build());
      String id = UUID.randomUUID().toString();
      project
          .data()
          .upsert(
              UpsertRequest.newBuilder()
                  .setMessageType(FILE)
                  .setRecordJson(row(id, BEYOND_DOUBLE))
                  .build());
      SelectRequest read =
          SelectRequest.newBuilder().setMessageType(FILE).setFilter(key(id)).setLimit(1).build();
      RecordSet found = project.data().select(read);
      assertEquals(1, found.getRecordsJsonCount());
      String text = found.getRecordsJson(0).toStringUtf8();
      assertTrue(text.contains(BEYOND_DOUBLE), "the integer changed: " + text);

      StatusRuntimeException refused =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  project
                      .data()
                      .upsert(
                          UpsertRequest.newBuilder()
                              .setMessageType(FILE)
                              .setRecordJson(row(id, "1.5"))
                              .build()));
      assertEquals(Status.Code.INVALID_ARGUMENT, refused.getStatus().getCode());

      project
          .data()
          .delete(DeleteRequest.newBuilder().setMessageType(FILE).setFilter(key(id)).build());
      assertEquals(0, project.data().select(read).getRecordsJsonCount(), "the row is gone");
    }
  }
}
