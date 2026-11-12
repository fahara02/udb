package dev.udb.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.udb.core.authn.services.v1.AuthnRequest;
import com.udb.core.authn.services.v1.AuthnResponse;
import com.udb.core.authn.services.v1.AuthnServiceGrpc;
import com.udb.core.authn.services.v1.LoginRequest;
import com.udb.core.authn.services.v1.LoginResponse;
import com.udb.core.authn.services.v1.Principal;
import com.udb.entity.v1.CapabilitiesRequest;
import com.udb.entity.v1.CapabilitiesResponse;
import com.udb.services.v1.DataBrokerGrpc;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Recovery after a broker restart, mirroring the Go SDK's reconnect tests.
 *
 * <p>The Java SDK owns no renewable credential (no refresh token or password is kept; an API key
 * is sent on every call and checked by the broker each time), so it has no bearer to renew after a
 * refusal. What it does own is the channel and the sign-in: the channel must reconnect promptly
 * once the broker is back, and {@link UdbProject#loginAndAdoptTenant} must ride out a broker that
 * is restarting.
 */
final class UdbBrokerRestartTest {

  /** An in-process broker that can be stopped and started again on the same port. */
  private static final class RestartBroker {
    final AtomicInteger logins = new AtomicInteger();
    final AtomicInteger capabilities = new AtomicInteger();
    /** Login fails with this status for the first {@code failLogins} attempts. */
    volatile Status loginFailure = Status.UNAVAILABLE;
    volatile int failLogins;

    private Server server;
    int port;

    void start() throws IOException, InterruptedException {
      IOException last = null;
      for (long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
          System.nanoTime() < deadline; ) {
        try {
          server =
              Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                  .addService(new Data())
                  .addService(new Authn())
                  .build()
                  .start();
          port = server.getPort();
          return;
        } catch (IOException err) {
          last = err; // the previous generation's port is not free yet
          Thread.sleep(50);
        }
      }
      throw last;
    }

    void stop() throws InterruptedException {
      if (server != null) {
        server.shutdownNow();
        server.awaitTermination(5, TimeUnit.SECONDS);
        server = null;
      }
    }

    String target() {
      return "localhost:" + port;
    }

    final class Data extends DataBrokerGrpc.DataBrokerImplBase {
      @Override
      public void getCapabilities(
          CapabilitiesRequest request, StreamObserver<CapabilitiesResponse> response) {
        capabilities.incrementAndGet();
        response.onNext(CapabilitiesResponse.newBuilder().setProtocolVersion("1.0.0").build());
        response.onCompleted();
      }
    }

    final class Authn extends AuthnServiceGrpc.AuthnServiceImplBase {
      @Override
      public void login(LoginRequest request, StreamObserver<LoginResponse> response) {
        int n = logins.incrementAndGet();
        if (n <= failLogins) {
          response.onError(loginFailure.withDescription("login attempt " + n).asRuntimeException());
          return;
        }
        response.onNext(
            LoginResponse.newBuilder()
                .setAccessToken("access-" + n)
                .setRefreshToken("refresh-" + n)
                .setAccessTokenExpiresIn(3600)
                .build());
        response.onCompleted();
      }

      @Override
      public void authenticate(AuthnRequest request, StreamObserver<AuthnResponse> response) {
        response.onNext(
            AuthnResponse.newBuilder()
                .setPrincipal(
                    Principal.newBuilder()
                        .setTenantId("tenant-canonical")
                        .setProjectId("project-canonical")
                        .setUserId("gateway"))
                .build());
        response.onCompleted();
      }
    }
  }

  private final RestartBroker broker = new RestartBroker();
  private UdbProject project;

  @AfterEach
  void tearDown() throws InterruptedException {
    if (project != null) {
      project.close();
    }
    broker.stop();
  }

  private UdbProject open() {
    project =
        UdbProject.open(
            UdbProjectConfig.builder()
                .target(broker.target())
                .tenantId("tenant-hint")
                .projectId("default")
                .purpose("java.unit.broker-restart")
                .build());
    return project;
  }

  private static LoginRequest login() {
    return LoginRequest.newBuilder().setUsername("gateway").setPassword("secret").build();
  }

  private static CapabilitiesRequest capabilities() {
    return CapabilitiesRequest.getDefaultInstance();
  }

  @Test
  void sameProjectRecoversAfterBrokerRestart() throws Exception {
    broker.start();
    UdbProject udb = open();
    udb.loginAndAdoptTenant(login());
    udb.data().broker().getCapabilities(capabilities());

    broker.stop();
    StatusRuntimeException down =
        assertThrows(
            StatusRuntimeException.class, () -> udb.data().broker().getCapabilities(capabilities()));
    assertEquals(Status.Code.UNAVAILABLE, down.getStatus().getCode());
    // Let the channel fail a few dials, growing its reconnect backoff.
    Thread.sleep(1500);
    broker.start();

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (true) {
      try {
        udb.data().broker().getCapabilities(capabilities());
        break;
      } catch (StatusRuntimeException err) {
        // Only UNAVAILABLE is tolerated while the channel reconnects.
        assertEquals(Status.Code.UNAVAILABLE, err.getStatus().getCode(), err.toString());
        if (System.nanoTime() > deadline) {
          fail("broker did not become reachable again: " + err);
        }
        Thread.sleep(50);
      }
    }
    assertEquals(2, broker.capabilities.get());
    assertEquals("access-1", udb.credentials().bearerToken());
  }

  @Test
  void loginRetriesATransientFailure() throws Exception {
    broker.failLogins = 2;
    broker.start();
    AuthnResponse verified = open().loginAndAdoptTenant(login());
    assertEquals("tenant-canonical", verified.getPrincipal().getTenantId());
    assertEquals(3, broker.logins.get());
    assertEquals("access-3", project.credentials().bearerToken());
    assertEquals("tenant-canonical", project.metadata().tenantId());
  }

  @Test
  void loginGivesUpAfterThreeTransientFailures() throws Exception {
    broker.failLogins = Integer.MAX_VALUE;
    broker.start();
    StatusRuntimeException err =
        assertThrows(StatusRuntimeException.class, () -> open().loginAndAdoptTenant(login()));
    assertEquals(Status.Code.UNAVAILABLE, err.getStatus().getCode());
    assertEquals(UdbProject.LOGIN_ATTEMPTS, broker.logins.get());
  }

  /** Negative: a refused credential is final; it is never repeated. */
  @Test
  void loginRefusedCredentialIsNotRetried() throws Exception {
    broker.failLogins = Integer.MAX_VALUE;
    broker.loginFailure = Status.UNAUTHENTICATED;
    broker.start();
    StatusRuntimeException err =
        assertThrows(StatusRuntimeException.class, () -> open().loginAndAdoptTenant(login()));
    assertEquals(Status.Code.UNAUTHENTICATED, err.getStatus().getCode());
    assertEquals(1, broker.logins.get());
    assertTrue(project.credentials().bearerToken().isEmpty(), "a refused login installed a bearer");
  }

  @Test
  void loginWaitsForARestartingBroker() throws Exception {
    broker.start();
    broker.stop(); // the port is known; nothing listens on it
    Thread restart =
        new Thread(
            () -> {
              try {
                Thread.sleep(200);
                broker.start();
              } catch (Exception err) {
                throw new IllegalStateException(err);
              }
            });
    restart.start();
    UdbProject udb = open();
    udb.loginAndAdoptTenant(login());
    restart.join();
    assertEquals(1, broker.logins.get());
  }

  @Test
  void reconnectKickResetsBackoffOnUnavailableAtMostOncePerSecond() throws Exception {
    CountingChannel channel = new CountingChannel();
    UdbChannels.ReconnectKick kick = new UdbChannels.ReconnectKick();
    kick.channel = channel;

    kick.onClose(Status.UNAVAILABLE);
    assertEquals(1, channel.resets.get());
    for (int i = 0; i < 20; i++) {
      kick.onClose(Status.UNAVAILABLE); // within the interval: not hammered
    }
    assertEquals(1, channel.resets.get());
    Thread.sleep(TimeUnit.NANOSECONDS.toMillis(UdbChannels.RECONNECT_KICK_INTERVAL_NANOS) + 100);
    // Negative: only UNAVAILABLE wakes the channel.
    kick.onClose(Status.UNAUTHENTICATED);
    kick.onClose(Status.DEADLINE_EXCEEDED);
    kick.onClose(Status.OK);
    assertEquals(1, channel.resets.get());
    kick.onClose(Status.UNAVAILABLE);
    assertEquals(2, channel.resets.get());
  }

  private static final class CountingChannel extends ManagedChannel {
    final AtomicInteger resets = new AtomicInteger();

    @Override
    public void resetConnectBackoff() {
      resets.incrementAndGet();
    }

    @Override
    public ManagedChannel shutdown() {
      return this;
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public ManagedChannel shutdownNow() {
      return this;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return true;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String authority() {
      return "localhost";
    }
  }
}
