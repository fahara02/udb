package dev.udb.client;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.List;
import com.udb.entity.v1.DeleteRequest;
import com.udb.entity.v1.MutationResponse;
import com.udb.entity.v1.RecordSet;
import com.udb.entity.v1.SelectRequest;
import com.udb.entity.v1.UpsertRequest;
import com.udb.services.v1.DataBrokerGrpc;
import dev.udb.client.generated.GeneratedUdbClient;

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
  private static final Metadata.Key<String> CONSISTENCY =
      Metadata.Key.of("x-udb-consistency", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PRIMARY_READ =
      Metadata.Key.of("x-udb-primary-read", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> MAX_REPLICA_LAG_MS =
      Metadata.Key.of("x-udb-max-replica-lag-ms", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> EVENTUAL_CONSISTENCY_ALLOWED =
      Metadata.Key.of("x-udb-eventual-consistency-allowed", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> READ_FENCE =
      Metadata.Key.of("x-udb-read-fence", Metadata.ASCII_STRING_MARSHALLER);

  private final ManagedChannel managedChannel;
  private final DataBrokerGrpc.DataBrokerBlockingStub broker;
  private final UdbMetadataRef metadata;

  public UdbClient(String target, UdbMetadata metadata) {
    this(
        UdbChannels.forTarget(target, false),
        new UdbMetadataRef(metadata),
        UdbCredentials.fromMetadata(metadata),
        true);
  }

  public UdbClient(Channel channel, UdbMetadata metadata) {
    this(channel, new UdbMetadataRef(metadata), UdbCredentials.fromMetadata(metadata), false);
  }

  /** Build over a shared, mutable credentials holder so a refreshed token reaches
   *  this stub without rebuilding the channel. */
  public UdbClient(Channel channel, UdbMetadata metadata, UdbCredentials credentials) {
    this(channel, new UdbMetadataRef(metadata), credentials, false);
  }

  UdbClient(Channel channel, UdbMetadataRef metadata, UdbCredentials credentials) {
    this(channel, metadata, credentials, false);
  }

  private UdbClient(
      Channel channel, UdbMetadataRef metadata, UdbCredentials credentials, boolean ownsChannel) {
    Objects.requireNonNull(channel, "channel");
    Objects.requireNonNull(metadata, "metadata");
    Objects.requireNonNull(credentials, "credentials");
    this.managedChannel = ownsChannel && channel instanceof ManagedChannel managed ? managed : null;
    this.metadata = metadata;
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

  public MutationResponse delete(DeleteRequest request) {
    return broker.delete(request);
  }

  public UdbEntityHandle entity(String messageType, String... key) {
    return new UdbEntityHandle(this, messageType, resolveEntityKey(messageType, key));
  }

  public UdbEntityHandle table(String name, String... key) {
    GeneratedUdbClient.EntityBinding binding = resolveTable(name);
    String messageType = binding == null ? name : binding.messageType();
    List<String> resolvedKey = key == null || key.length == 0 ? defaultKey(binding) : List.of(key);
    return new UdbEntityHandle(this, messageType, resolvedKey);
  }

  UdbMetadata metadata() {
    return metadata.current();
  }

  private static List<String> resolveEntityKey(String messageType, String... key) {
    if (key != null && key.length > 0) {
      return List.of(key);
    }
    return defaultKey(GeneratedUdbClient.entities().get(messageType));
  }

  private static GeneratedUdbClient.EntityBinding resolveTable(String name) {
    for (GeneratedUdbClient.EntityBinding binding : GeneratedUdbClient.entities().values()) {
      if (binding.table().equals(name)) {
        return binding;
      }
    }
    return null;
  }

  private static List<String> defaultKey(GeneratedUdbClient.EntityBinding binding) {
    return binding == null ? List.of() : binding.primaryKeys();
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
    if (meta.consistency() != null && !meta.consistency().isBlank()) {
      headers.put(CONSISTENCY, meta.consistency());
    }
    if (meta.primaryRead()) {
      headers.put(PRIMARY_READ, "true");
    }
    if (meta.maxReplicaLagMs() > 0) {
      headers.put(MAX_REPLICA_LAG_MS, Long.toString(meta.maxReplicaLagMs()));
    }
    if (meta.eventualConsistencyAllowed()) {
      headers.put(EVENTUAL_CONSISTENCY_ALLOWED, "true");
    }
    if (meta.readFenceJson() != null && !meta.readFenceJson().isBlank()) {
      headers.put(READ_FENCE, meta.readFenceJson());
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
    return credentialInterceptor(new UdbMetadataRef(metadata), credentials);
  }

  static ClientInterceptor credentialInterceptor(
      UdbMetadataRef metadata, UdbCredentials credentials) {
    return new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
          MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
            next.newCall(method, callOptions)) {
          @Override
          public void start(Listener<RespT> responseListener, Metadata headers) {
            headers.merge(
                headers(metadata.current(), credentials.bearerToken(), credentials.apiKey()));
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
