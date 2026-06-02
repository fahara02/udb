package dev.udb.client;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import com.udb.entity.v1.MutationResponse;
import com.udb.entity.v1.RecordSet;
import com.udb.entity.v1.SelectRequest;
import com.udb.entity.v1.UpsertRequest;
import com.udb.services.v1.DataBrokerGrpc;

public final class UdbClient implements AutoCloseable {
  public static final String PROTOCOL_VERSION = "1.0.0";

  private static final Metadata.Key<String> TENANT_ID =
      Metadata.Key.of("x-tenant-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> USER_ID =
      Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PURPOSE =
      Metadata.Key.of("x-purpose", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORRELATION_ID =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> SCOPES =
      Metadata.Key.of("x-scopes", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> SERVICE_IDENTITY =
      Metadata.Key.of("x-service-identity", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PROJECT_ID =
      Metadata.Key.of("x-udb-project-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CLIENT_CATALOG_VERSION =
      Metadata.Key.of("x-udb-client-catalog-version", Metadata.ASCII_STRING_MARSHALLER);

  private final ManagedChannel managedChannel;
  private final DataBrokerGrpc.DataBrokerBlockingStub broker;

  public UdbClient(String target, UdbMetadata metadata) {
    this(
        ManagedChannelBuilder.forTarget(target).usePlaintext().build(),
        metadata,
        true);
  }

  public UdbClient(Channel channel, UdbMetadata metadata) {
    this(channel, metadata, false);
  }

  private UdbClient(Channel channel, UdbMetadata metadata, boolean ownsChannel) {
    Objects.requireNonNull(channel, "channel");
    Objects.requireNonNull(metadata, "metadata");
    this.managedChannel = ownsChannel && channel instanceof ManagedChannel managed ? managed : null;
    this.broker =
        DataBrokerGrpc.newBlockingStub(channel)
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers(metadata)));
  }

  public DataBrokerGrpc.DataBrokerBlockingStub broker() {
    return broker;
  }

  public RecordSet select(SelectRequest request) {
    return broker.select(request);
  }

  public MutationResponse upsert(UpsertRequest request) {
    return broker.upsert(request);
  }

  public static Metadata headers(UdbMetadata meta) {
    Metadata headers = new Metadata();
    headers.put(TENANT_ID, meta.tenantId());
    headers.put(USER_ID, meta.userId());
    headers.put(PURPOSE, meta.purpose());
    headers.put(CORRELATION_ID, meta.correlationId());
    headers.put(SCOPES, String.join(",", meta.scopes()));
    headers.put(SERVICE_IDENTITY, meta.serviceIdentity());
    headers.put(PROJECT_ID, meta.projectId());
    headers.put(CLIENT_CATALOG_VERSION, meta.clientCatalogVersion());
    return headers;
  }

  @Override
  public void close() {
    if (managedChannel == null) {
      return;
    }
    managedChannel.shutdown();
    try {
      if (!managedChannel.awaitTermination(5, TimeUnit.SECONDS)) {
        managedChannel.shutdownNow();
      }
    } catch (InterruptedException err) {
      managedChannel.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
