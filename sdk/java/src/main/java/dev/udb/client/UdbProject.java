package dev.udb.client;

import com.udb.core.analytics.services.v1.AnalyticsServiceGrpc;
import com.udb.core.apikey.services.v1.ApiKeyServiceGrpc;
import com.udb.core.apikey.services.v1.CreateApiKeyRequest;
import com.udb.core.apikey.services.v1.CreateApiKeyResponse;
import com.udb.core.apikey.services.v1.RevokeApiKeyRequest;
import com.udb.core.apikey.services.v1.RevokeApiKeyResponse;
import com.udb.core.authn.services.v1.AuthnResponse;
import com.udb.core.authn.services.v1.LoginRequest;
import com.udb.core.authn.services.v1.LoginResponse;
import com.udb.core.authn.services.v1.Principal;
import com.udb.core.notification.services.v1.NotificationServiceGrpc;
import com.udb.core.notification.services.v1.SendNotificationRequest;
import com.udb.core.notification.services.v1.SendNotificationResponse;
import com.udb.core.tenant.services.v1.CreateTenantRequest;
import com.udb.core.tenant.services.v1.CreateTenantResponse;
import com.udb.core.tenant.services.v1.TenantServiceGrpc;
import io.grpc.ManagedChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * One-stop project facade over the UDB control- and data-plane services. It
 * shares a single {@link UdbMetadata} identity across all sub-clients, attaches
 * the caller headers to every generated stub, and owns the channels it creates
 * (data-plane + auth-plane), closing them via {@link #close()}.
 *
 * <p>Exposed sub-clients: {@link #data()}, {@link #auth()}/{@link #authz()},
 * {@link #apiKey()}, {@link #tenant()}, {@link #notification()},
 * {@link #analytics()}, {@link #storage()}, {@link #asset()}, and
 * {@link #webRtc()}. The storage/asset/webrtc services live on the native
 * control-plane listener (same target as auth), so they reuse the existing
 * control-plane channel.
 */
public final class UdbProject implements AutoCloseable {
  private final UdbProjectConfig config;
  private final UdbMetadataRef metadata;

  private final ManagedChannel dataChannel;
  private final ManagedChannel authChannel; // may be the same as dataChannel
  private final ManagedChannel webrtcChannel; // may be the same as authChannel

  /** Shared, mutable credentials sent on every outbound call. Mutating it
   *  (via {@link #setCredentials}) hot-swaps the bearer/API key on all stubs. */
  private final UdbCredentials credentials;

  private final UdbClient data;
  private final UdbAuthClient auth;
  private final ApiKeyServiceGrpc.ApiKeyServiceBlockingStub apiKey;
  private final TenantServiceGrpc.TenantServiceBlockingStub tenant;
  private final NotificationServiceGrpc.NotificationServiceBlockingStub notification;
  private final AnalyticsServiceGrpc.AnalyticsServiceBlockingStub analytics;
  private final UdbStorageClient storage;
  private final UdbAssetClient asset;
  private final UdbWebRtcClient webRtc;

  private UdbProject(UdbProjectConfig config) {
    this.config = config;
    UdbMetadata initialMetadata = config.metadata();
    this.metadata = new UdbMetadataRef(initialMetadata);
    this.credentials = UdbCredentials.fromMetadata(initialMetadata);

    this.dataChannel = channel(config.target(), config.tls());
    this.authChannel =
        config.authTarget().equals(config.target())
            ? dataChannel
            : channel(config.authTarget(), config.tls());
    // WebRTC rides the control-plane channel unless a distinct webrtcTarget is
    // configured.
    if (config.webrtcTarget().equals(config.authTarget())) {
      this.webrtcChannel = authChannel;
    } else if (config.webrtcTarget().equals(config.target())) {
      this.webrtcChannel = dataChannel;
    } else {
      this.webrtcChannel = channel(config.webrtcTarget(), config.tls());
    }

    this.data = new UdbClient(dataChannel, metadata, credentials);
    this.auth = new UdbAuthClient(authChannel, metadata, credentials);

    // Native control-plane stubs hang off the auth/control-plane channel; the
    // dynamic credential interceptor attaches shared identity headers + live
    // credentials.
    io.grpc.ClientInterceptor creds = UdbClient.credentialInterceptor(metadata, credentials);
    this.apiKey = ApiKeyServiceGrpc.newBlockingStub(authChannel).withInterceptors(creds);
    this.tenant = TenantServiceGrpc.newBlockingStub(authChannel).withInterceptors(creds);
    this.notification =
        NotificationServiceGrpc.newBlockingStub(authChannel).withInterceptors(creds);
    this.analytics = AnalyticsServiceGrpc.newBlockingStub(authChannel).withInterceptors(creds);

    // Storage / asset are served on the native control-plane listener (same target
    // as auth); WebRTC rides its own channel when webrtcTarget is distinct.
    this.storage = new UdbStorageClient(authChannel, metadata, credentials);
    this.asset = new UdbAssetClient(authChannel, metadata, credentials);
    this.webRtc = new UdbWebRtcClient(webrtcChannel, metadata, credentials);
  }

  /** Open a project facade from config. See {@link Udb#project(UdbProjectConfig)}. */
  public static UdbProject open(UdbProjectConfig config) {
    return new UdbProject(config);
  }

  private static ManagedChannel channel(String target, boolean tls) {
    // Long-lived channel with UDB keepalive + UNAVAILABLE retry; reused across RPCs.
    return UdbChannels.forTarget(target, tls);
  }

  // ── Raw clients / stubs (reachable, never hidden) ──────────────────────────
  public UdbClient data() {
    return data;
  }

  /** Auth ergonomics (authn + authz helpers, cache-friendly). */
  public UdbAuthClient auth() {
    return auth;
  }

  /** Alias for {@link #auth()} — the authorization surface lives on the same client. */
  public UdbAuthClient authz() {
    return auth;
  }

  public ApiKeyServiceGrpc.ApiKeyServiceBlockingStub apiKey() {
    return apiKey;
  }

  public TenantServiceGrpc.TenantServiceBlockingStub tenant() {
    return tenant;
  }

  public NotificationServiceGrpc.NotificationServiceBlockingStub notification() {
    return notification;
  }

  public AnalyticsServiceGrpc.AnalyticsServiceBlockingStub analytics() {
    return analytics;
  }

  /** Native file storage (presigned upload/download, file metadata CRUD). */
  public UdbStorageClient storage() {
    return storage;
  }

  /** Native asset registration + processing pipelines. */
  public UdbAssetClient asset() {
    return asset;
  }

  /** Native WebRTC: room/peer/track/turn accessors + async signaling stream. */
  public UdbWebRtcClient webRtc() {
    return webRtc;
  }

  public UdbProjectConfig config() {
    return config;
  }

  public UdbMetadata metadata() {
    return metadata.current();
  }

  public UdbEntityHandle entity(String messageType, String... key) {
    return data.entity(messageType, key);
  }

  public UdbEntityHandle table(String name, String... key) {
    return data.table(name, key);
  }

  /** The shared credentials holder; mutating it hot-swaps creds on every stub. */
  public UdbCredentials credentials() {
    return credentials;
  }

  /**
   * Hot-swap the bearer token and API key sent on every subsequent call across
   * all sub-clients (data, auth, control-plane, storage, asset, WebRTC). Call
   * this after a login/refresh so the new token reaches outbound metadata — the
   * Java analogue of the TypeScript {@code core.setCredentials}.
   */
  public void setCredentials(String bearerToken, String apiKey) {
    this.credentials.set(bearerToken, apiKey);
  }

  /** Hot-swap just the bearer token (keeps the configured API key). */
  public void setBearerToken(String bearerToken) {
    this.credentials.setBearerToken(bearerToken);
  }

  // ── Convenience wrappers ───────────────────────────────────────────────────

  public AuthnResponse loginAndAdoptTenant(String username, String password) {
    return loginAndAdoptTenant(
        LoginRequest.newBuilder()
            .setUsername(username == null ? "" : username)
            .setPassword(password == null ? "" : password)
            .build());
  }

  /**
   * Login, ALWAYS authenticate the freshly minted bearer, then adopt the
   * canonical tenant/project from the verified principal across every facade.
   */
  public synchronized AuthnResponse loginAndAdoptTenant(LoginRequest request) {
    LoginResponse login = auth.login(request);
    String token = login.getAccessToken().isBlank() ? login.getSessionToken() : login.getAccessToken();
    if (token.isBlank()) {
      throw new IllegalStateException(
          "udb: Login returned no access token (MFA required: " + login.getMfaRequired() + ")");
    }

    AuthnResponse verified = auth.authenticateBearer(token);
    if (!verified.hasPrincipal()) {
      throw new IllegalStateException("udb: AuthenticateBearer returned no principal");
    }

    Principal principal = verified.getPrincipal();
    UdbMetadata current = metadata.current();
    UdbMetadata adopted =
        new UdbMetadata(
            principal.getTenantId().isBlank() ? current.tenantId() : principal.getTenantId(),
            current.purpose(),
            current.correlationId(),
            current.scopes(),
            current.serviceIdentity(),
            principal.getUserId().isBlank() ? current.userId() : principal.getUserId(),
            principal.getProjectId().isBlank() ? current.projectId() : principal.getProjectId(),
            current.clientCatalogVersion(),
            token,
            credentials.apiKey(),
            current.consistency(),
            current.primaryRead(),
            current.maxReplicaLagMs(),
            current.eventualConsistencyAllowed(),
            current.readFenceJson());
    metadata.set(adopted);
    credentials.setBearerToken(token);
    return verified;
  }

  /** Send a notification to a recipient over one or more channels. */
  public SendNotificationResponse sendNotification(SendNotificationRequest request) {
    return notification.sendNotification(request);
  }

  /** Mint a new API key for an owner, returning the response (plain key once). */
  public CreateApiKeyResponse createApiKey(String name, String ownerId, List<String> scopes) {
    return apiKey.createApiKey(CreateApiKeyRequest.newBuilder()
        .setName(name)
        .setOwnerId(ownerId)
        .addAllScopes(scopes == null ? List.of() : scopes)
        .build());
  }

  /**
   * Revoke an API key by id. The ApiKey service exposes create + revoke (no
   * dedicated rotate RPC); rotation is "revoke old, then create new".
   */
  public RevokeApiKeyResponse revokeApiKey(String keyId, String reason) {
    return apiKey.revokeApiKey(RevokeApiKeyRequest.newBuilder()
        .setKeyId(keyId)
        .setRevokeReason(reason == null ? "" : reason)
        .build());
  }

  /**
   * Revoke an existing key and mint a replacement with the same name + scopes —
   * the SDK-side "rotate" convenience over the create/revoke RPCs.
   */
  public CreateApiKeyResponse rotateApiKey(
      String oldKeyId, String name, String ownerId, List<String> scopes) {
    revokeApiKey(oldKeyId, "rotated");
    return createApiKey(name, ownerId, scopes);
  }

  /** Onboard / create a tenant by code + display name. */
  public CreateTenantResponse createTenant(String code, String name) {
    return tenant.createTenant(CreateTenantRequest.newBuilder()
        .setCode(code)
        .setName(name)
        .build());
  }

  @Override
  public void close() {
    shutdown(dataChannel);
    if (authChannel != dataChannel) {
      shutdown(authChannel);
    }
    if (webrtcChannel != dataChannel && webrtcChannel != authChannel) {
      shutdown(webrtcChannel);
    }
  }

  private static void shutdown(ManagedChannel channel) {
    channel.shutdown();
    try {
      if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
        channel.shutdownNow();
      }
    } catch (InterruptedException err) {
      channel.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
