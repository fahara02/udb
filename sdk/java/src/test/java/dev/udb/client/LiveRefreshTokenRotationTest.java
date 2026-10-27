package dev.udb.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.udb.core.authn.entity.v1.TokenType;
import com.udb.core.authn.services.v1.AuthnServiceGrpc;
import com.udb.core.authn.services.v1.LoginRequest;
import com.udb.core.authn.services.v1.LoginResponse;
import com.udb.core.authn.services.v1.RefreshTokenRequest;
import com.udb.core.authn.services.v1.RefreshTokenResponse;
import com.udb.core.authn.services.v1.ValidateTokenRequest;
import com.udb.core.authn.services.v1.ValidateTokenResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Live refresh-token rotation conformance.
 *
 * <p>Gated on {@code UDB_LIVE_SDK_TESTS=1}. CI's "SDK refresh-token rotation (live)" job runs it
 * against the broker it just built, on every push to main.
 *
 * <p>The broker rotates the refresh token on every {@code AuthnService.RefreshToken} call and each
 * one works once. A caller that kept the token it logged in with refreshed once and then failed with
 * UNAUTHENTICATED "invalid credential" — a production outage no single-refresh test could catch. The
 * Java SDK keeps no token store: refresh is the AuthnService stub behind {@link UdbAuthClient#authn()},
 * and the caller contract is that each refresh presents the refresh token the previous one returned.
 * This test chains it exactly that way.
 */
class LiveRefreshTokenRotationTest {
  private static final int REFRESHES = 3;

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

  private static AuthnServiceGrpc.AuthnServiceBlockingStub authn(UdbProject project) {
    return project.auth().authn().withDeadlineAfter(10, TimeUnit.SECONDS);
  }

  @Test
  void liveRefreshTokenRotationSurvivesRepeatedRefreshes() {
    assumeTrue("1".equals(System.getenv("UDB_LIVE_SDK_TESTS")), "requires live UDB broker");
    String target = env("UDB_GRPC_TARGET", null);
    String authTarget = env("UDB_AUTH_GRPC_TARGET", target);
    String tenant = env("UDB_LIVE_TENANT", "sdk-live");
    String projectId = env("UDB_LIVE_PROJECT", "default");
    UdbProjectConfig config =
        UdbProjectConfig.builder()
            .target(target)
            .authTarget(authTarget)
            .tenantId(tenant)
            .projectId(projectId)
            .purpose("java.live.refresh-rotation")
            .correlationId("java-live-refresh-rotation")
            .serviceIdentity("java.sdk.live")
            .build();
    LoginRequest loginRequest =
        LoginRequest.newBuilder()
            .setUsername(env("UDB_LIVE_USERNAME", null))
            .setPassword(env("UDB_LIVE_PASSWORD", null))
            .setTenantHint(tenant)
            .setProjectHint(projectId)
            .setDeviceName("java-sdk-refresh-rotation")
            .build();

    try (UdbProject project = UdbProject.open(config)) {
      // Adopt the canonical tenant the login JWT carries; authenticated calls must
      // assert it rather than the human tenant code.
      project.loginAndAdoptTenant(loginRequest);
      // The refresh chain starts at a login's refresh token.
      LoginResponse login = project.auth().login(loginRequest);
      String first = login.getRefreshToken();
      assertFalse(login.getAccessToken().isEmpty(), "login returned no access token");
      assertFalse(first.isEmpty(), "login returned no refresh token");
      project.setBearerToken(login.getAccessToken());

      String previous = first;
      for (int i = 1; i <= REFRESHES; i++) {
        RefreshTokenResponse refreshed =
            authn(project)
                .refreshToken(RefreshTokenRequest.newBuilder().setRefreshToken(previous).build());
        assertFalse(refreshed.getAccessToken().isEmpty(), "refresh " + i + " returned no access token");
        assertFalse(refreshed.getRefreshToken().isEmpty(), "refresh " + i + " returned no refresh token");
        assertNotEquals(
            previous,
            refreshed.getRefreshToken(),
            "refresh " + i + ": the refresh token did not change — the broker stopped rotating it");

        // An authenticated call carrying the refreshed bearer.
        project.setBearerToken(refreshed.getAccessToken());
        ValidateTokenResponse validated =
            authn(project)
                .validateToken(
                    ValidateTokenRequest.newBuilder()
                        .setToken(refreshed.getAccessToken())
                        .setTokenType(TokenType.TOKEN_TYPE_JWT_ACCESS)
                        .build());
        assertTrue(validated.getValid(), "refresh " + i + ": the refreshed access token does not validate");
        previous = refreshed.getRefreshToken();
      }

      // The first refresh token was spent three rotations ago: the broker must
      // refuse it. (Replaying the immediately-previous token instead would trip
      // reuse detection and revoke every session of this shared live user.)
      StatusRuntimeException refused =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  authn(project)
                      .refreshToken(RefreshTokenRequest.newBuilder().setRefreshToken(first).build()),
              "a spent refresh token was accepted");
      assertEquals(
          Status.Code.UNAUTHENTICATED,
          refused.getStatus().getCode(),
          "a spent refresh token must be refused with UNAUTHENTICATED");
    }
  }
}
