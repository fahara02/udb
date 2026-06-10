package dev.udb.client;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
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
  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> API_KEY =
      Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER);

  private final ManagedChannel managedChannel;
  private final DataBrokerGrpc.DataBrokerBlockingStub broker;

  public UdbClient(String target, UdbMetadata metadata) {
    this(
        ManagedChannelBuilder.forTarget(target).usePlaintext().build(),
        metadata,
        UdbCredentials.fromMetadata(metadata),
        true);
  }

  public UdbClient(Channel channel, UdbMetadata metadata) {
    this(channel, metadata, UdbCredentials.fromMetadata(metadata), false);
  }

  /** Build over a shared, mutable credentials holder so a refreshed token reaches
   *  this stub without rebuilding the channel. */
  public UdbClient(Channel channel, UdbMetadata metadata, UdbCredentials credentials) {
    this(channel, metadata, credentials, false);
  }

  private UdbClient(
      Channel channel, UdbMetadata metadata, UdbCredentials credentials, boolean ownsChannel) {
    Objects.requireNonNull(channel, "channel");
    Objects.requireNonNull(metadata, "metadata");
    Objects.requireNonNull(credentials, "credentials");
    this.managedChannel = ownsChannel && channel instanceof ManagedChannel managed ? managed : null;
    this.broker =
        DataBrokerGrpc.newBlockingStub(channel)
            .withInterceptors(credentialInterceptor(metadata, credentials));
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
    return headers(meta, meta.bearerToken(), meta.apiKey());
  }

  /**
   * Build the per-call headers using the supplied (live) bearer token / API key
   * instead of the ones baked into {@code meta}. The non-credential identity
   * headers still come from {@code meta}. Used by the dynamic credential
   * interceptor so a refreshed token is attached on the next call.
   */
  public static Metadata headers(UdbMetadata meta, String bearerToken, String apiKey) {
    Metadata headers = new Metadata();
    headers.put(TENANT_ID, meta.tenantId());
    headers.put(USER_ID, meta.userId());
    headers.put(PURPOSE, meta.purpose());
    headers.put(CORRELATION_ID, meta.correlationId());
    headers.put(SCOPES, String.join(",", meta.scopes()));
    headers.put(SERVICE_IDENTITY, meta.serviceIdentity());
    headers.put(PROJECT_ID, meta.projectId());
    headers.put(CLIENT_CATALOG_VERSION, meta.clientCatalogVersion());
    if (bearerToken != null && !bearerToken.isBlank()) {
      headers.put(AUTHORIZATION, "Bearer " + bearerToken);
    }
    if (apiKey != null && !apiKey.isBlank()) {
      headers.put(API_KEY, apiKey);
    }
    return headers;
  }

  /**
   * A {@link ClientInterceptor} that attaches the caller identity headers plus
   * the <em>current</em> credentials from {@code credentials} on every call.
   * Because it reads the holder per-RPC, mutating the holder (e.g. after a token
   * refresh) immediately changes the credentials sent on subsequent calls.
   */
  public static ClientInterceptor credentialInterceptor(
      UdbMetadata metadata, UdbCredentials credentials) {
    return new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
          MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
            next.newCall(method, callOptions)) {
          @Override
          public void start(Listener<RespT> responseListener, Metadata headers) {
            headers.merge(
                headers(metadata, credentials.bearerToken(), credentials.apiKey()));
            super.start(responseListener, headers);
          }
        };
      }
    };
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
