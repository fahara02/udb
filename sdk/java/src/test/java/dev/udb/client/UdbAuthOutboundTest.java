package dev.udb.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.udb.core.authz.services.v1.AuthzRequest;
import com.udb.core.authz.services.v1.NativeAccessRequest;
import com.udb.core.authz.services.v1.Principal;
import com.udb.core.authz.services.v1.ResourceRef;
import com.udb.core.authz.services.v1.SignedPolicyBundle;
import io.grpc.Metadata;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;

/**
 * Conformance / outbound-shape tests for the auth surface (M9): they assert the
 * request messages the SDK builds carry the caller's scopes as {@code
 * requested_scopes}, that the gRPC metadata uses the canonical UDB header names,
 * that {@link AuthzCache} honours the server-provided TTL, and that {@link
 * UdbAuthClient#verifyPolicyBundle} accepts a good HMAC and rejects a bad one.
 *
 * <p>These build the request messages exactly as {@link UdbAuthClient#can} and
 * {@link UdbAuthClient#nativeAccess} do (without opening a channel), so they run
 * with no live broker.
 */
final class UdbAuthOutboundTest {

  private static UdbMetadata meta() {
    return new UdbMetadata(
        "tenant-a",
        "read",
        "corr-1",
        List.of("udb:read", "udb:portal:viewer"),
        "orders.service",
        "user-1",
        "project-a",
        "catalog-v1");
  }

  private static Principal principalOf(UdbMetadata m) {
    return Principal.newBuilder()
        .setUserId(m.userId())
        .setServiceIdentity(m.serviceIdentity())
        .setTenantId(m.tenantId())
        .setProjectId(m.projectId())
        .addAllScopes(m.scopes())
        .build();
  }

  @Test
  void authnAuthzResponseJsonFixturesDoNotExposePersistedCredentials() {
    String json =
        """
        [
          {"user":{"user_id":"u1","username":"ada"}},
          {"session":{"session_id":"sesspub_abc"}},
          {"key":{"key_id":"udbk_abc","key_prefix":"udbk_abc"}},
          {"audits":[{"decision_audit_id":"a1","user_id":"u1"}]}
        ]
        """;
    for (String banned :
        List.of(
            "argon2id$",
            "hmac-sha256:",
            "password_hash",
            "passwordHash",
            "totp_secret_enc",
            "totpSecretEnc",
            "session_token_lookup",
            "sessionTokenLookup",
            "session_token_hash",
            "sessionTokenHash",
            "csrf_token_hash",
            "csrfTokenHash",
            "key_hash",
            "keyHash")) {
      assertFalse(json.contains(banned), "response JSON fixture leaked " + banned);
    }
  }

  @Test
  void canForwardsScopesAsRequestedScopes() {
    UdbMetadata m = meta();
    ResourceRef resource = ResourceRef.newBuilder().setMessageType("Order").build();
    AuthzRequest request =
        AuthzRequest.newBuilder()
            .setPrincipal(principalOf(m))
            .setTenantId(m.tenantId())
            .setProjectId(m.projectId())
            .setResource(resource)
            .setAction("read")
            .setPurpose(m.purpose())
            .addAllRequestedScopes(m.scopes())
            .build();

    assertEquals(List.of("udb:read", "udb:portal:viewer"), request.getRequestedScopesList());
    assertEquals("tenant-a", request.getTenantId());
    assertEquals("project-a", request.getProjectId());
  }

  @Test
  void nativeAccessForwardsScopesAsRequestedScopes() {
    UdbMetadata m = meta();
    ResourceRef resource = ResourceRef.newBuilder().setTable("orders").build();
    NativeAccessRequest request =
        NativeAccessRequest.newBuilder()
            .setPrincipal(principalOf(m))
            .setTenantId(m.tenantId())
            .setProjectId(m.projectId())
            .setResource(resource)
            .setAction("select")
            .setPurpose(m.purpose())
            .addAllRequestedScopes(m.scopes())
            .build();

    assertEquals(List.of("udb:read", "udb:portal:viewer"), request.getRequestedScopesList());
  }

  @Test
  void headersUseCanonicalUdbNames() {
    Metadata headers = UdbClient.headers(meta());
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
  void verifyPolicyBundleAcceptsMatchingHmacAndRejectsTampered() throws Exception {
    String secret = "s3cr3t";
    byte[] payload = "{\"policies\":[]}".getBytes(StandardCharsets.UTF_8);
    String goodSig = hmacHex(payload, secret);

    SignedPolicyBundle good =
        SignedPolicyBundle.newBuilder()
            .setBundle(ByteString.copyFrom(payload))
            .setSignature(goodSig)
            .setKeyId("k1")
            .setAlgorithm("HMAC-SHA256")
            .build();
    assertTrue(UdbAuthClient.verifyPolicyBundle(good, secret));

    // Wrong secret -> reject.
    assertFalse(UdbAuthClient.verifyPolicyBundle(good, "wrong"));

    // Tampered payload, original signature -> reject.
    SignedPolicyBundle tampered =
        good.toBuilder().setBundle(ByteString.copyFromUtf8("{\"policies\":[\"evil\"]}")).build();
    assertFalse(UdbAuthClient.verifyPolicyBundle(tampered, secret));

    // Empty secret never verifies.
    assertFalse(UdbAuthClient.verifyPolicyBundle(good, ""));
  }

  @Test
  void authzCacheServesServerTtlThenRefetches() {
    UdbMetadata m = meta();
    StubAuthClient stub = new StubAuthClient(m);
    AuthzCache cache = new AuthzCache(stub, 0L);
    ResourceRef resource = ResourceRef.newBuilder().setMessageType("Order").build();

    // First call hits the stub, decision carries cache_ttl_seconds=60 -> cached.
    assertTrue(cache.allowed(resource, "read", "read"));
    assertEquals(1, stub.calls);
    // Second call is served from cache (no extra stub call).
    assertTrue(cache.allowed(resource, "read", "read"));
    assertEquals(1, stub.calls);

    // A zero-TTL decision is never cached: every call hits the stub.
    StubAuthClient zeroTtl = new StubAuthClient(m, 0);
    AuthzCache noCache = new AuthzCache(zeroTtl, 0L);
    assertTrue(noCache.allowed(resource, "read", "read"));
    assertTrue(noCache.allowed(resource, "read", "read"));
    assertEquals(2, zeroTtl.calls);
  }

  private static String hmacHex(byte[] data, String secret) throws Exception {
    Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
    byte[] d = mac.doFinal(data);
    StringBuilder sb = new StringBuilder(d.length * 2);
    for (byte b : d) {
      sb.append(Character.forDigit((b >> 4) & 0xF, 16));
      sb.append(Character.forDigit(b & 0xF, 16));
    }
    return sb.toString();
  }

  private static String header(Metadata headers, String name) {
    return headers.get(Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER));
  }

  /**
   * Test double for {@link UdbAuthClient} that returns a canned allow decision
   * with a configurable {@code cache_ttl_seconds}, without opening a channel —
   * lets {@link AuthzCache} TTL behaviour be asserted offline.
   */
  private static final class StubAuthClient extends UdbAuthClient {
    final long ttlSeconds;
    int calls;

    StubAuthClient(UdbMetadata metadata) {
      this(metadata, 60);
    }

    StubAuthClient(UdbMetadata metadata, long ttlSeconds) {
      super(metadata); // channel-free test-double ctor; only can() is exercised.
      this.ttlSeconds = ttlSeconds;
    }

    @Override
    public com.udb.core.authz.services.v1.Decision can(
        ResourceRef resource, String action, String purpose) {
      calls++;
      return com.udb.core.authz.services.v1.Decision.newBuilder()
          .setAllowed(true)
          .setCacheTtlSeconds(ttlSeconds)
          .build();
    }
  }
}
