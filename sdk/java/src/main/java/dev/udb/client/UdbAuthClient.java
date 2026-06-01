package dev.udb.client;

import com.udb.core.authn.services.v1.AuthnRequest;
import com.udb.core.authn.services.v1.AuthnResponse;
import com.udb.core.authn.services.v1.AuthnServiceGrpc;
import com.udb.core.authz.services.v1.AuthzRequest;
import com.udb.core.authz.services.v1.AuthzServiceGrpc;
import com.udb.core.authz.services.v1.Decision;
import com.udb.core.authz.services.v1.NativeAccessGrant;
import com.udb.core.authz.services.v1.NativeAccessRequest;
import com.udb.core.authz.services.v1.NativeAccessResponse;
import com.udb.core.authz.services.v1.PolicyBundleRequest;
import com.udb.core.authz.services.v1.Principal;
import com.udb.core.authz.services.v1.ResourceRef;
import com.udb.core.authz.services.v1.SignedPolicyBundle;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

/**
 * Hand-written auth ergonomics over the generated AuthnService / AuthzService
 * stubs, mirroring {@link UdbClient}'s metadata convention (item 111).
 */
public final class UdbAuthClient implements AutoCloseable {
  private final ManagedChannel channel;
  private final UdbMetadata metadata;
  private final AuthnServiceGrpc.AuthnServiceBlockingStub authn;
  private final AuthzServiceGrpc.AuthzServiceBlockingStub authz;

  public UdbAuthClient(String target, UdbMetadata metadata) {
    this(ManagedChannelBuilder.forTarget(target).usePlaintext().build(), metadata);
  }

  public UdbAuthClient(ManagedChannel channel, UdbMetadata metadata) {
    this.channel = channel;
    this.metadata = metadata;
    this.authn = AuthnServiceGrpc.newBlockingStub(channel);
    this.authz = AuthzServiceGrpc.newBlockingStub(channel);
  }

  private static void put(Metadata md, String key, String value) {
    md.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value == null ? "" : value);
  }

  private Metadata headers() {
    Metadata md = new Metadata();
    put(md, "x-tenant-id", metadata.tenantId());
    put(md, "x-user-id", metadata.userId());
    put(md, "x-purpose", metadata.purpose());
    put(md, "x-correlation-id", metadata.correlationId());
    put(md, "x-scopes", String.join(",", metadata.scopes()));
    put(md, "x-service-identity", metadata.serviceIdentity());
    put(md, "x-udb-project-id", metadata.projectId());
    put(md, "x-udb-client-catalog-version", metadata.clientCatalogVersion());
    return md;
  }

  // ── Authentication ────────────────────────────────────────────────────────
  public AuthnResponse authenticate(AuthnRequest request) {
    return authn.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers())).authenticate(request);
  }

  public AuthnResponse authenticateBearer(String token) {
    return authenticate(AuthnRequest.newBuilder()
        .setBearerToken(token)
        .setTenantHint(metadata.tenantId())
        .setProjectHint(metadata.projectId())
        .build());
  }

  public AuthnResponse authenticateApiKey(String apiKey) {
    return authenticate(AuthnRequest.newBuilder()
        .setApiKey(apiKey)
        .setTenantHint(metadata.tenantId())
        .setProjectHint(metadata.projectId())
        .build());
  }

  public AuthnResponse authenticateSession(String sessionId) {
    return authenticate(AuthnRequest.newBuilder()
        .setSessionId(sessionId)
        .setTenantHint(metadata.tenantId())
        .setProjectHint(metadata.projectId())
        .build());
  }

  // ── Authorization ───────────────────────────────────────────────────────────
  public Decision authorize(AuthzRequest request) {
    return authz.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers()))
        .authorize(request)
        .getDecision();
  }

  /** Authorize the bound principal acting on a resource. */
  public Decision can(ResourceRef resource, String action, String purpose) {
    Principal principal = Principal.newBuilder()
        .setUserId(metadata.userId())
        .setServiceIdentity(metadata.serviceIdentity())
        .setTenantId(metadata.tenantId())
        .setProjectId(metadata.projectId())
        .addAllScopes(metadata.scopes())
        .build();
    AuthzRequest request = AuthzRequest.newBuilder()
        .setPrincipal(principal)
        .setTenantId(metadata.tenantId())
        .setProjectId(metadata.projectId())
        .setResource(resource)
        .setAction(action)
        .setPurpose(purpose == null || purpose.isEmpty() ? metadata.purpose() : purpose)
        .build();
    return authorize(request);
  }

  // ── Stage 2: native database fast-path access (item 138) ────────────────────
  /**
   * Authorize and, when allowed, return the native-access grant (restricted role
   * + scoped DSN + RLS session variables). Returns {@code null} when access is
   * allowed but no grant was minted; throws when the decision denies access.
   */
  public NativeAccessGrant nativeAccess(ResourceRef resource, String action, String purpose) {
    Principal principal = Principal.newBuilder()
        .setUserId(metadata.userId())
        .setServiceIdentity(metadata.serviceIdentity())
        .setTenantId(metadata.tenantId())
        .setProjectId(metadata.projectId())
        .addAllScopes(metadata.scopes())
        .build();
    NativeAccessRequest request = NativeAccessRequest.newBuilder()
        .setPrincipal(principal)
        .setTenantId(metadata.tenantId())
        .setProjectId(metadata.projectId())
        .setResource(resource)
        .setAction(action)
        .setPurpose(purpose == null || purpose.isEmpty() ? metadata.purpose() : purpose)
        .build();
    NativeAccessResponse resp = authz.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers()))
        .getNativeAccess(request);
    if (resp.hasDecision() && !resp.getDecision().getAllowed()) {
      throw new IllegalStateException("udb: native access denied: " + resp.getDecision().getDenyReason());
    }
    return resp.hasGrant() ? resp.getGrant() : null;
  }

  // ── Stage 2: signed policy bundle (item 140) ────────────────────────────────
  public SignedPolicyBundle getPolicyBundle() {
    PolicyBundleRequest request = PolicyBundleRequest.newBuilder()
        .setTenantId(metadata.tenantId())
        .setProjectId(metadata.projectId())
        .build();
    return authz.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers()))
        .getPolicyBundle(request)
        .getBundle();
  }

  @Override
  public void close() {
    channel.shutdownNow();
  }
}
