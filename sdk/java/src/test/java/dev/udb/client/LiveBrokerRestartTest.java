package dev.udb.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.udb.core.authn.entity.v1.TokenType;
import com.udb.core.authn.services.v1.LoginRequest;
import com.udb.core.authn.services.v1.ValidateTokenRequest;
import com.udb.entity.v1.CapabilitiesRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Live broker-restart recovery: a Java client outlives a broker restart without being recreated.
 * The same {@link UdbProject} reconnects and keeps working on the data plane and the native control
 * plane. Same contract as the Go SDK's {@code TestLiveBrokerRestartRecovery}.
 *
 * <p>Runs when {@code UDB_LIVE_SDK_TESTS=1} and {@code UDB_LIVE_BROKER_RESTART_CMD} names a command
 * (run with {@code sh -c}) that restarts the broker on the same addresses and KEEPS its database
 * (sessions, users and tenant ids survive). While the broker is down a call may fail UNAVAILABLE;
 * any other failure, or no recovery within 90s, fails the test.
 */
class LiveBrokerRestartTest {

  private static String env(String name, String fallback) {
    String value = System.getenv(name);
    if (value != null && !value.isBlank()) {
      return value.trim();
    }
    if (fallback != null) {
      return fallback;
    }
    throw new IllegalStateException(name + " is required when UDB_LIVE_SDK_TESTS=1");
  }

  private interface Probe {
    void call();
  }

  @Test
  void liveBrokerRestartRecovery() throws Exception {
    String restart = System.getenv("UDB_LIVE_BROKER_RESTART_CMD");
    assumeTrue(
        "1".equals(System.getenv("UDB_LIVE_SDK_TESTS")) && restart != null && !restart.isBlank(),
        "requires a live broker and UDB_LIVE_BROKER_RESTART_CMD");
    String target = env("UDB_GRPC_TARGET", null);
    String authTarget = env("UDB_AUTH_GRPC_TARGET", target);
    String tenant = env("UDB_LIVE_TENANT", null);
    String projectId = env("UDB_LIVE_PROJECT", null);
    UdbProjectConfig config =
        UdbProjectConfig.builder()
            .target(target)
            .authTarget(authTarget)
            .tenantId(tenant)
            .projectId(projectId)
            .purpose("java.live.broker-restart")
            .correlationId("java-live-broker-restart")
            .serviceIdentity("java.sdk.live")
            .build();
    LoginRequest login =
        LoginRequest.newBuilder()
            .setUsername(env("UDB_LIVE_USERNAME", null))
            .setPassword(env("UDB_LIVE_PASSWORD", null))
            .setTenantHint(tenant)
            .setProjectHint(projectId)
            .setDeviceName("java-sdk-broker-restart")
            .build();

    try (UdbProject project = UdbProject.open(config)) {
      project.loginAndAdoptTenant(login);
      List<String> names = List.of("data GetCapabilities", "native ValidateSession");
      List<Probe> probes =
          List.of(
              () ->
                  project
                      .data()
                      .broker()
                      .withDeadlineAfter(5, TimeUnit.SECONDS)
                      .getCapabilities(CapabilitiesRequest.getDefaultInstance()),
              () ->
                  assertTrue(
                      project
                          .auth()
                          .authn()
                          .withDeadlineAfter(5, TimeUnit.SECONDS)
                          .validateToken(
                              ValidateTokenRequest.newBuilder()
                                  .setToken(project.credentials().bearerToken())
                                  .setTokenType(TokenType.TOKEN_TYPE_JWT_ACCESS)
                                  .build())
                          .getValid(),
                      "the session token is not valid"));
      for (int i = 0; i < probes.size(); i++) {
        try {
          probes.get(i).call();
        } catch (StatusRuntimeException err) {
          fail("before restart, " + names.get(i) + ": code=" + err.getStatus().getCode());
        }
      }

      Process cmd = new ProcessBuilder("sh", "-c", restart).inheritIO().start();
      if (!cmd.waitFor(3, TimeUnit.MINUTES)) {
        cmd.destroyForcibly();
        fail("UDB_LIVE_BROKER_RESTART_CMD did not finish within 3 minutes");
      }
      assertEquals(0, cmd.exitValue(), "UDB_LIVE_BROKER_RESTART_CMD failed");

      for (int i = 0; i < probes.size(); i++) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (true) {
          try {
            probes.get(i).call();
            break;
          } catch (StatusRuntimeException err) {
            Status.Code code = err.getStatus().getCode();
            if (code != Status.Code.UNAVAILABLE) {
              fail("after restart, " + names.get(i) + " on the same project: code=" + code + " err=" + err);
            }
            if (System.nanoTime() > deadline) {
              fail("after restart, " + names.get(i) + " stayed UNAVAILABLE for 90s: " + err);
            }
            Thread.sleep(250);
          }
        }
      }
    }
  }
}
