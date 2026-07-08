package dev.udb.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Proves that the shared {@link UdbCredentials} holder hot-swaps the outbound
 * {@code authorization} header: a token refresh (mutating the holder) changes
 * the credentials attached on the <em>next</em> call, without rebuilding the
 * stub. Uses a capturing fake {@link Channel}, so no live broker is needed.
 */
final class UdbCredentialHotSwapTest {

  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> TENANT_ID =
      Metadata.Key.of("x-tenant-id", Metadata.ASCII_STRING_MARSHALLER);

  private static final MethodDescriptor.Marshaller<String> STRING_MARSHALLER =
      new MethodDescriptor.Marshaller<>() {
        @Override
        public InputStream stream(String value) {
          return new java.io.ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String parse(InputStream stream) {
          return "";
        }
      };

  private static final MethodDescriptor<String, String> METHOD =
      MethodDescriptor.<String, String>newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName("udb.test/Echo")
          .setRequestMarshaller(STRING_MARSHALLER)
          .setResponseMarshaller(STRING_MARSHALLER)
          .build();

  private static UdbMetadata metadata() {
    return new UdbMetadata(
        "tenant-a", "purpose", "corr", List.of("read"), "svc", "user-a", "project-a", "");
  }

  /** Capture the {@link Metadata} the interceptor attaches on {@code start}. */
  private static String authHeaderFor(ClientInterceptor interceptor) {
    return headerFor(interceptor, AUTHORIZATION);
  }

  private static String headerFor(ClientInterceptor interceptor, Metadata.Key<String> key) {
    CapturingChannel channel = new CapturingChannel();
    ClientCall<String, String> call = interceptor.interceptCall(METHOD, CallOptions.DEFAULT, channel);
    call.start(new ClientCall.Listener<>() {}, new Metadata());
    return channel.captured == null ? null : channel.captured.get(key);
  }

  @Test
  void mutatingCredentialsChangesOutboundAuthorization() {
    UdbCredentials credentials = new UdbCredentials("token-1", "");
    ClientInterceptor interceptor = UdbClient.credentialInterceptor(metadata(), credentials);

    assertEquals("Bearer token-1", authHeaderFor(interceptor), "initial bearer not attached");

    // Simulate a refresh: the holder is mutated, the interceptor is unchanged.
    credentials.setBearerToken("token-2");
    assertEquals(
        "Bearer token-2", authHeaderFor(interceptor), "refreshed bearer not reflected on next call");
  }

  @Test
  void blankCredentialsAttachNoAuthorizationHeader() {
    UdbCredentials credentials = new UdbCredentials("", "");
    ClientInterceptor interceptor = UdbClient.credentialInterceptor(metadata(), credentials);
    assertNull(authHeaderFor(interceptor), "no bearer should mean no authorization header");
  }

  @Test
  void mutatingMetadataReferenceChangesOutboundTenantHeader() {
    UdbMetadataRef metadata = new UdbMetadataRef(metadata());
    UdbCredentials credentials = new UdbCredentials("", "");
    ClientInterceptor interceptor = UdbClient.credentialInterceptor(metadata, credentials);

    assertEquals("tenant-a", headerFor(interceptor, TENANT_ID), "initial tenant not attached");

    metadata.set(new UdbMetadata(
        "canonical-tenant", "purpose", "corr", List.of("read"), "svc", "user-a", "project-a", ""));
    assertEquals(
        "canonical-tenant",
        headerFor(interceptor, TENANT_ID),
        "adopted tenant not reflected on next call");
  }

  @Test
  void projectSetCredentialsSharesHolderWithAuthClient() {
    try (UdbProject project =
        UdbProject.open(UdbProjectConfig.builder().target("localhost:1").build())) {
      project.setCredentials("rotated", "key-1");
      assertEquals("rotated", project.credentials().bearerToken());
      assertEquals("key-1", project.credentials().apiKey());
      // The auth client shares the same holder, so it sees the rotated token.
      assertEquals("rotated", project.auth().credentials().bearerToken());
    }
  }

  /** A {@link Channel} that records the headers handed to the next call's start. */
  private static final class CapturingChannel extends Channel {
    private Metadata captured;

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
        MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
      return new ClientCall<>() {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          captured = headers;
        }

        @Override
        public void request(int numMessages) {}

        @Override
        public void cancel(String message, Throwable cause) {}

        @Override
        public void halfClose() {}

        @Override
        public void sendMessage(ReqT message) {}
      };
    }

    @Override
    public String authority() {
      return "localhost";
    }
  }
}
