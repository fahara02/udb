package dev.udb.client;

import com.udb.core.authn.services.v1.AuthnRequest;
import com.udb.core.authn.services.v1.AuthnResponse;
import com.udb.core.authn.services.v1.AuthnServiceGrpc;
import com.udb.core.authn.services.v1.ExchangeTokenForJobRequest;
import com.udb.core.authn.services.v1.ExchangeTokenForJobResponse;
import com.udb.core.authn.services.v1.LoginRequest;
import com.udb.core.authn.services.v1.LoginResponse;
import com.udb.core.authz.services.v1.AuthzRequest;
import com.udb.core.authz.services.v1.AuthzServiceGrpc;
import com.udb.core.authz.services.v1.BatchCheckPermissionsRequest;
import com.udb.core.authz.services.v1.BatchCheckPermissionsResponse;
import com.udb.core.authz.services.v1.CheckAccessRequest;
import com.udb.core.authz.services.v1.CheckAccessResponse;
import com.udb.core.authz.services.v1.Decision;
import com.udb.core.authz.services.v1.NativeAccessGrant;
import com.udb.core.authz.services.v1.NativeAccessRequest;
import com.udb.core.authz.services.v1.NativeAccessResponse;
import com.udb.core.authz.services.v1.PermissionCheck;
import com.udb.core.authz.services.v1.PolicyBundleRequest;
import com.udb.core.authz.services.v1.Principal;
import com.udb.core.authz.services.v1.ResourceRef;
import com.udb.core.authz.services.v1.SignedPolicyBundle;
import com.udb.entity.v1.ErrorDetail;
import dev.udb.client.generated.GeneratedClientSupport;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Hand-written auth ergonomics over the generated AuthnService / AuthzService
 * stubs, mirroring {@link UdbClient}'s metadata convention (item 111).
 */
public class UdbAuthClient implements AutoCloseable {
  private final ManagedChannel channel;
  private final UdbMetadataRef metadata;
  private final UdbCredentials credentials;
  private final AuthnServiceGrpc.AuthnServiceBlockingStub authn;
  private final AuthzServiceGrpc.AuthzServiceBlockingStub authz;

  /**
   * Optional shared secret used to verify the HMAC-SHA256 signature on policy
   * bundles fetched via {@link #getPolicyBundle()}. When non-null/non-empty, an
   * unverifiable bundle raises {@link UdbPolicyBundleSignatureException}.
   */
  private volatile String bundleSecret;

  public UdbAuthClient(String target, UdbMetadata metadata) {
    this(UdbChannels.forTarget(target, false), metadata);
  }

  public UdbAuthClient(ManagedChannel channel, UdbMetadata metadata) {
    this(channel, metadata, UdbCredentials.fromMetadata(metadata));
  }

  /** Build over a shared, mutable credentials holder so a refreshed token is sent
   *  on the next authn/authz call without rebuilding the channel. */
  public UdbAuthClient(ManagedChannel channel, UdbMetadata metadata, UdbCredentials credentials) {
    this(channel, new UdbMetadataRef(metadata), credentials);
  }

  UdbAuthClient(ManagedChannel channel, UdbMetadataRef metadata, UdbCredentials credentials) {
    this.channel = channel;
    this.metadata = metadata;
    this.credentials = credentials;
    this.authn = AuthnServiceGrpc.newBlockingStub(channel);
    this.authz = AuthzServiceGrpc.newBlockingStub(channel);
  }

  /**
   * Channel-free constructor for subclassed test doubles that override the RPC
   * surface ({@link #can}, {@link #authorize}, etc.) and never touch the wire.
   * No channel/stubs are created, so the network RPC methods must not be called
   * on instances built this way.
   */
  protected UdbAuthClient(UdbMetadata metadata) {
    this.channel = null;
    this.metadata = new UdbMetadataRef(metadata);
    this.credentials = UdbCredentials.fromMetadata(metadata);
    this.authn = null;
    this.authz = null;
  }

  private Metadata headers() {
    return UdbClient.headers(metadata.current(), credentials.bearerToken(), credentials.apiKey());
  }

  /** The shared credentials holder backing this client's outbound auth headers. */
  public UdbCredentials credentials() {
    return credentials;
  }

  // ── Authentication ────────────────────────────────────────────────────────
  public AuthnResponse authenticate(AuthnRequest request) {
    return authn.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers())).authenticate(request);
  }

  public LoginResponse login(LoginRequest request) {
    return authn.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers())).login(request);
  }

  public AuthnResponse authenticateBearer(String token) {
    UdbMetadata current = metadata.current();
    return authenticate(AuthnRequest.newBuilder()
        .setBearerToken(token)
        .setTenantHint(current.tenantId())
        .setProjectHint(current.projectId())
        .build());
  }

  public AuthnResponse authenticateApiKey(String apiKey) {
    UdbMetadata current = metadata.current();
    return authenticate(AuthnRequest.newBuilder()
        .setApiKey(apiKey)
        .setTenantHint(current.tenantId())
        .setProjectHint(current.projectId())
        .build());
  }

  public AuthnResponse authenticateSession(String sessionId) {
    UdbMetadata current = metadata.current();
    return authenticate(AuthnRequest.newBuilder()
        .setSessionId(sessionId)
        .setTenantHint(current.tenantId())
        .setProjectHint(current.projectId())
        .build());
  }

  // ── Delegated job tokens ────────────────────────────────────────────────────
  /**
   * Exchange a user's current access token for a delegated job token
   * ({@code AuthnService.ExchangeTokenForJob}), so a durable job (an export, a
   * report, a nightly sync) acts for that user after the user's own short-lived
   * token expired. Call it on a client carrying the SERVICE's own bearer (a
   * service account whose grant holds {@code udb:token:delegate}, in the user's
   * tenant); the broker refuses any other caller, and any subject token that is
   * not a valid user access token of that tenant, with {@code PERMISSION_DENIED}.
   *
   * <p>The token names the user as subject and the service as actor, carries
   * {@code purpose} and {@code jobId}, and lives at most {@code ttlSeconds} (0 =
   * the broker's maximum, {@code UDB_DELEGATED_TOKEN_MAX_TTL}, 24 hours by
   * default). It has no refresh token, cannot be exchanged again, and stops
   * working as soon as the user, the token or the service's grant is revoked:
   * see {@link #isTokenExpired} and {@link #isTokenRevoked}.
   *
   * @param subjectToken the user's current access token ({@code "Bearer "} prefix optional)
   * @param purpose why the job acts for the user, e.g. {@code "export"}; signed into the token
   * @param jobId the job the token is for; at most 200 bytes
   * @param ttlSeconds the requested lifetime; 0 asks for the broker's maximum
   * @throws IllegalArgumentException when a required argument is blank or ttlSeconds is negative
   * @throws StatusRuntimeException when the broker refuses the exchange
   */
  public DelegatedJobToken exchangeTokenForJob(
      String subjectToken, String purpose, String jobId, long ttlSeconds) {
    ExchangeTokenForJobRequest request = jobTokenRequest(subjectToken, purpose, jobId, ttlSeconds);
    return delegatedJobToken(authn().exchangeTokenForJob(request), jobId);
  }

  /** {@link #exchangeTokenForJob(String, String, String, long)} with the broker's maximum lifetime. */
  public DelegatedJobToken exchangeTokenForJob(String subjectToken, String purpose, String jobId) {
    return exchangeTokenForJob(subjectToken, purpose, jobId, 0L);
  }

  static ExchangeTokenForJobRequest jobTokenRequest(
      String subjectToken, String purpose, String jobId, long ttlSeconds) {
    if (subjectToken == null || subjectToken.isBlank()) {
      throw new IllegalArgumentException("udb: exchangeTokenForJob: subjectToken is required");
    }
    if (jobId == null || jobId.isBlank()) {
      throw new IllegalArgumentException("udb: exchangeTokenForJob: jobId is required");
    }
    if (purpose == null || purpose.isBlank()) {
      throw new IllegalArgumentException("udb: exchangeTokenForJob: purpose is required");
    }
    if (ttlSeconds < 0) {
      throw new IllegalArgumentException("udb: exchangeTokenForJob: ttlSeconds must not be negative");
    }
    return ExchangeTokenForJobRequest.newBuilder()
        .setSubjectToken(subjectToken)
        .setPurpose(purpose)
        .setJobId(jobId)
        .setTtlSeconds(ttlSeconds)
        .build();
  }

  static DelegatedJobToken delegatedJobToken(ExchangeTokenForJobResponse response, String jobId) {
    if (response.getAccessToken().isEmpty()) {
      throw new IllegalStateException(
          "udb: exchange the user's token for job " + jobId + ": the broker returned no token");
    }
    return new DelegatedJobToken(
        response.getAccessToken(),
        response.getJti(),
        response.getExpiresAtUnix(),
        response.getSubject(),
        response.getActor(),
        response.getPurpose(),
        response.getJobId(),
        response.getScopesList(),
        response.getTenantId(),
        response.getProjectId());
  }

  /**
   * The broker's typed refusal reason ({@code ErrorDetail.reason}, e.g.
   * {@code UDB_TOKEN_EXPIRED}) carried by a failed call, or {@code ""} when the
   * error carries none. Reads a raw {@link StatusRuntimeException}'s
   * {@code udb-error-detail-bin} trailer and a generated client's
   * {@link GeneratedClientSupport.UdbRpcException}, including either one found
   * as a cause. The message text is never matched.
   */
  public static String errorReason(Throwable error) {
    for (Throwable t = error; t != null; t = t.getCause()) {
      if (t instanceof GeneratedClientSupport.UdbRpcException rpc) {
        ErrorDetail detail = rpc.decodedErrorDetail();
        return detail == null ? "" : detail.getReason();
      }
      if (t instanceof StatusRuntimeException sre) {
        Metadata trailers = sre.getTrailers();
        byte[] raw = trailers == null ? null : trailers.get(GeneratedClientSupport.ERROR_DETAIL_KEY);
        if (raw == null || raw.length == 0) {
          return "";
        }
        try {
          return ErrorDetail.parseFrom(raw).getReason();
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
          return "";
        }
      }
      if (t.getCause() == t) {
        break;
      }
    }
    return "";
  }

  /**
   * The broker refused the bearer because it expired ({@code UDB_TOKEN_EXPIRED}):
   * refresh it or sign in again. A delegated job token cannot be refreshed: the
   * job exchanges a fresh user token or stops.
   */
  public static boolean isTokenExpired(Throwable error) {
    return "UDB_TOKEN_EXPIRED".equals(errorReason(error));
  }

  /**
   * The broker refused the bearer because it was revoked or is no longer backed
   * by live state ({@code UDB_TOKEN_REVOKED}): do not retry with it.
   */
  public static boolean isTokenRevoked(Throwable error) {
    return "UDB_TOKEN_REVOKED".equals(errorReason(error));
  }

  // ── Authorization ───────────────────────────────────────────────────────────
  public Decision authorize(AuthzRequest request) {
    return authz.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers()))
        .authorize(request)
        .getDecision();
  }

  /**
   * Authorize the bound principal acting on a resource. The caller's metadata
   * scopes are forwarded as {@code requested_scopes} so the authority can apply
   * scope-narrowing exactly as the Go/Python/TS SDKs do.
   */
  public Decision can(ResourceRef resource, String action, String purpose) {
    UdbMetadata current = metadata.current();
    Principal principal = boundPrincipal();
    AuthzRequest request = AuthzRequest.newBuilder()
        .setPrincipal(principal)
        .setTenantId(current.tenantId())
        .setProjectId(current.projectId())
        .setResource(resource)
        .setAction(action)
        .setPurpose(purpose == null || purpose.isEmpty() ? current.purpose() : purpose)
        .addAllRequestedScopes(current.scopes())
        .build();
    return authorize(request);
  }

  /**
   * Authorize and throw {@link UdbAuthzDeniedException} when the decision denies
   * access; returns the allow {@link Decision} otherwise (matched policies,
   * required scopes, server cache TTL).
   */
  public Decision require(ResourceRef resource, String action, String purpose) {
    Decision decision = can(resource, action, purpose);
    if (!decision.getAllowed()) {
      throw new UdbAuthzDeniedException(resource, action, decision);
    }
    return decision;
  }

  /**
   * Non-throwing companion to {@link #require}: returns the full {@link Decision}
   * (allowed flag + deny reason + matched policies) for inspection.
   */
  public Decision explain(ResourceRef resource, String action, String purpose) {
    return can(resource, action, purpose);
  }

  /**
   * Forward a Casbin-style (user, domain, object, action) request and report
   * whether access is allowed.
   */
  public CheckAccessResponse checkAccess(CheckAccessRequest request) {
    return authz.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers()))
        .checkAccess(request);
  }

  /**
   * Evaluate many (object, action) checks in one round-trip via the
   * BatchCheckPermissions RPC, returning the server's per-check
   * {@code object:action -> allowed} result map.
   */
  public Map<String, Boolean> batchCan(List<PermissionCheck> checks) {
    UdbMetadata current = metadata.current();
    BatchCheckPermissionsRequest request = BatchCheckPermissionsRequest.newBuilder()
        .setUserId(current.userId())
        .setDomain(current.tenantId())
        .addAllChecks(checks)
        .build();
    BatchCheckPermissionsResponse resp =
        authz.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers()))
            .batchCheckPermissions(request);
    return resp.getResultsMap();
  }

  /** Build a single {@link PermissionCheck} for use with {@link #batchCan}. */
  public static PermissionCheck check(String object, String action) {
    return PermissionCheck.newBuilder().setObject(object).setAction(action).build();
  }

  private Principal boundPrincipal() {
    UdbMetadata current = metadata.current();
    return Principal.newBuilder()
        .setUserId(current.userId())
        .setServiceIdentity(current.serviceIdentity())
        .setTenantId(current.tenantId())
        .setProjectId(current.projectId())
        .addAllScopes(current.scopes())
        .build();
  }

  /** The metadata identity bound to this client (used by {@link AuthzCache}). */
  public UdbMetadata metadata() {
    return metadata.current();
  }

  /** Raw AuthnService stub with caller headers attached. */
  public AuthnServiceGrpc.AuthnServiceBlockingStub authn() {
    return authn.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers()));
  }

  /** Raw AuthzService stub with caller headers attached. */
  public AuthzServiceGrpc.AuthzServiceBlockingStub authz() {
    return authz.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers()));
  }

  // ── Stage 2: native database fast-path access (item 138) ────────────────────
  /**
   * Authorize and, when allowed, return the native-access grant (restricted role
   * + scoped DSN + RLS session variables). Returns {@code null} when access is
   * allowed but no grant was minted; throws when the decision denies access.
   */
  public NativeAccessGrant nativeAccess(ResourceRef resource, String action, String purpose) {
    UdbMetadata current = metadata.current();
    Principal principal = boundPrincipal();
    NativeAccessRequest request = NativeAccessRequest.newBuilder()
        .setPrincipal(principal)
        .setTenantId(current.tenantId())
        .setProjectId(current.projectId())
        .setResource(resource)
        .setAction(action)
        .setPurpose(purpose == null || purpose.isEmpty() ? current.purpose() : purpose)
        .addAllRequestedScopes(current.scopes())
        .build();
    NativeAccessResponse resp = authz.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers()))
        .getNativeAccess(request);
    if (resp.hasDecision() && !resp.getDecision().getAllowed()) {
      throw new IllegalStateException("udb: native access denied: " + resp.getDecision().getDenyReason());
    }
    return resp.hasGrant() ? resp.getGrant() : null;
  }

  // ── Stage 2: signed policy bundle (item 140) ────────────────────────────────
  /**
   * Set (or clear with {@code null}/empty) the shared secret used to verify the
   * HMAC-SHA256 signature on bundles fetched by {@link #getPolicyBundle()}. With a
   * secret configured, {@link #getPolicyBundle()} throws {@link
   * UdbPolicyBundleSignatureException} on a signature mismatch. Returns {@code
   * this} for chaining.
   */
  public UdbAuthClient withBundleSecret(String secret) {
    this.bundleSecret = secret;
    return this;
  }

  /**
   * Fetch the signed policy bundle for the bound tenant/project. When a bundle
   * secret has been configured via {@link #withBundleSecret(String)}, the bundle's
   * HMAC-SHA256 signature is verified before it is returned; a mismatch raises
   * {@link UdbPolicyBundleSignatureException}.
   */
  public SignedPolicyBundle getPolicyBundle() {
    UdbMetadata current = metadata.current();
    PolicyBundleRequest request = PolicyBundleRequest.newBuilder()
        .setTenantId(current.tenantId())
        .setProjectId(current.projectId())
        .build();
    SignedPolicyBundle bundle =
        authz.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers()))
            .getPolicyBundle(request)
            .getBundle();
    String secret = this.bundleSecret;
    if (secret != null && !secret.isEmpty() && !verifyPolicyBundle(bundle, secret)) {
      throw new UdbPolicyBundleSignatureException(
          "udb: policy bundle signature verification failed"
              + (bundle.getKeyId().isEmpty() ? "" : " (key_id=" + bundle.getKeyId() + ")"),
          bundle);
    }
    return bundle;
  }

  /**
   * Recompute the HMAC-SHA256 over {@code signed.getBundle()} bytes using {@code
   * secret} and constant-time compare the lowercase-hex digest to {@code
   * signed.getSignature()}. The server emits a lowercase-hex HMAC (the proto
   * comment's "Base64" is stale), so this matches the wire format. Returns {@code
   * true} when the signature is valid.
   */
  public static boolean verifyPolicyBundle(SignedPolicyBundle signed, String secret) {
    if (signed == null || secret == null || secret.isEmpty()) {
      return false;
    }
    String expected = hmacSha256Hex(signed.getBundle().toByteArray(), secret);
    return constantTimeEquals(expected, signed.getSignature());
  }

  private static String hmacSha256Hex(byte[] data, String secret) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
      byte[] digest = mac.doFinal(data);
      StringBuilder sb = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        sb.append(Character.forDigit((b >> 4) & 0xF, 16));
        sb.append(Character.forDigit(b & 0xF, 16));
      }
      return sb.toString();
    } catch (java.security.GeneralSecurityException err) {
      throw new IllegalStateException("udb: HMAC-SHA256 unavailable", err);
    }
  }

  /** Length-independent constant-time string compare (via {@link MessageDigest}). */
  private static boolean constantTimeEquals(String a, String b) {
    byte[] x = a == null ? new byte[0] : a.getBytes(StandardCharsets.UTF_8);
    byte[] y = b == null ? new byte[0] : b.getBytes(StandardCharsets.UTF_8);
    return MessageDigest.isEqual(x, y);
  }

  @Override
  public void close() {
    if (channel != null) {
      channel.shutdownNow();
    }
  }
}
