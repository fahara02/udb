package dev.udb.examples.select;

import dev.udb.client.UdbClient;
import dev.udb.client.UdbMetadata;
import java.util.List;
import com.udb.entity.v1.Types.RecordSet;
import com.udb.entity.v1.Types.SelectRequest;

public final class SelectExample {
  private SelectExample() {}

  public static void main(String[] args) {
    String target = args.length > 0 ? args[0] : "localhost:50051";
    UdbMetadata metadata =
        new UdbMetadata(
            "tenant-a",
            "read",
            "java-example-1",
            List.of("udb:read"),
            "java.example",
            "user-1",
            "default",
            UdbClient.PROTOCOL_VERSION);

    try (UdbClient client = new UdbClient(target, metadata)) {
      SelectRequest request =
          SelectRequest.newBuilder()
              .setMessageType("Example")
              .setLimit(10)
              .build();
      RecordSet rows = client.select(request);
      System.out.printf("received %d rows%n", rows.getRowsCount());
    }
  }
}
