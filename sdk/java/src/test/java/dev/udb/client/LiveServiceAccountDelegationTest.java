package dev.udb.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.udb.core.authn.entity.v1.AccountKind;
import com.udb.core.authn.entity.v1.OTPType;
import com.udb.core.authn.entity.v1.User;
import com.udb.core.authn.entity.v1.UserStatus;
import com.udb.core.authn.services.v1.AuthnRequest;
import com.udb.core.authn.services.v1.AuthnServiceGrpc;
import com.udb.core.authn.services.v1.CreateUserRequest;
import com.udb.core.authn.services.v1.CreateUserResponse;
import com.udb.core.authn.services.v1.ExchangeServiceTokenForTenantRequest;
import com.udb.core.authn.services.v1.ExchangeServiceTokenForTenantResponse;
import com.udb.core.authn.services.v1.GetUserRequest;
import com.udb.core.authn.services.v1.GetUserResponse;
import com.udb.core.authn.services.v1.LoginRequest;
import com.udb.core.authn.services.v1.LoginResponse;
import com.udb.core.authn.services.v1.Principal;
import com.udb.core.authn.services.v1.SendOTPRequest;
import com.udb.core.authn.services.v1.SendOTPResponse;
import com.udb.core.authn.services.v1.VerifyOTPRequest;
import com.udb.core.authn.services.v1.VerifyOTPResponse;
import com.udb.core.authz.services.v1.AuthzServiceGrpc;
import com.udb.core.authz.services.v1.CheckAccessRequest;
import com.udb.core.authz.services.v1.CheckAccessResponse;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import java.security.SecureRandom;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Live service-account conformance.
 *
 * <p>Gated on {@code UDB_LIVE_SDK_TESTS=1}. CI's "SDK service accounts (live)" job runs it against
 * the broker it just built, on every push to main, after {@code
 * scripts/provision-service-account-live.py} has created the identities it reads from the
 * environment.
 *
 * <p>It proves, through the Java SDK's generated stubs, channel and header convention against the
 * served broker:
 *
 * <ol>
 *   <li>Authenticate names the account ({@code Principal.username});
 *   <li>a service-account bearer carries its bound roles, never a withheld admin-segment role, and
 *       its typed-grant scopes;
 *   <li>a service account with {@code udb:authz:check-access-for} evaluates CheckAccess for a
 *       person of its tenant ({@code evaluated_subject}), and is refused for an id that is not one;
 *   <li>ExchangeServiceTokenForTenant mints a short-lived target-tenant token that authenticates
 *       and authorizes IN the target tenant, and is refused for a tenant with no approved
 *       membership;
 *   <li>a phone-only CreateUser is verified by a PHONE_VERIFICATION OTP and the account becomes
 *       ACTIVE.
 * </ol>
 */
class LiveServiceAccountDelegationTest {
  private static final String CHECKED_OBJECT = "sdk-live-chart";
  private static final String CHECKED_ACTION = "read";
  private static final String EXCHANGED_SCOPE = "udb:authz:check-access";
  private static final SecureRandom RANDOM = new SecureRandom();

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

  /**
   * The SDK's caller headers asserting {@code tenantId}/{@code projectId}, with a correlation id
   * (and the request id the SDK always adds: authenticated native calls require a request
   * context) and, when given, the bearer credential.
   */
  private static Metadata headers(String tenantId, String projectId, String role, String bearer) {
    UdbMetadata meta =
        new UdbMetadata(
            tenantId,
            "java.live.service-accounts",
            "java-live-service-accounts-" + role,
            List.of(),
            "java.sdk.live",
            "",
            projectId,
            UdbClient.PROTOCOL_VERSION,
            bearer == null ? "" : bearer,
            "");
    return UdbClient.headers(meta);
  }

  private static AuthnServiceGrpc.AuthnServiceBlockingStub authn(ManagedChannel channel, Metadata headers) {
    return AuthnServiceGrpc.newBlockingStub(channel)
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
        .withDeadlineAfter(20, TimeUnit.SECONDS);
  }

  private static AuthzServiceGrpc.AuthzServiceBlockingStub authz(ManagedChannel channel, Metadata headers) {
    return AuthzServiceGrpc.newBlockingStub(channel)
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
        .withDeadlineAfter(20, TimeUnit.SECONDS);
  }

  private static String firstNonEmpty(String... values) {
    for (String value : values) {
      if (value != null && !value.isEmpty()) {
        return value;
      }
    }
    return "";
  }

  private static String randomHex(int bytes) {
    byte[] buf = new byte[bytes];
    RANDOM.nextBytes(buf);
    StringBuilder sb = new StringBuilder(bytes * 2);
    for (byte b : buf) {
      sb.append(Character.forDigit((b >> 4) & 0xF, 16));
      sb.append(Character.forDigit(b & 0xF, 16));
    }
    return sb.toString();
  }

  private static String randomDigits(int count) {
    StringBuilder sb = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      sb.append((char) ('0' + RANDOM.nextInt(10)));
    }
    return sb.toString();
  }

  private static String describe(StatusRuntimeException err) {
    return "code=" + err.getStatus().getCode() + " " + err.getStatus().getDescription();
  }

  @Test
  void liveServiceAccountDelegationConformance() {
    assumeTrue("1".equals(System.getenv("UDB_LIVE_SDK_TESTS")), "requires live UDB broker");
    String target = env("UDB_GRPC_TARGET", null);
    String authTarget = env("UDB_AUTH_GRPC_TARGET", target);
    String tenant = env("UDB_LIVE_TENANT", null);
    String project = env("UDB_LIVE_PROJECT", null);
    String humanUsername = env("UDB_LIVE_USERNAME", null);
    String humanPassword = env("UDB_LIVE_PASSWORD", null);
    String serviceUsername = env("UDB_LIVE_SERVICE_USERNAME", null);
    String servicePassword = env("UDB_LIVE_SERVICE_PASSWORD", null);
    String serviceUserId = env("UDB_LIVE_SERVICE_USER_ID", null);
    String serviceRole = env("UDB_LIVE_SERVICE_ROLE", null);
    String withheldRole = env("UDB_LIVE_SERVICE_WITHHELD_ROLE", null);
    String delegatedUserId = env("UDB_LIVE_DELEGATED_USER_ID", null);
    String targetTenantId = env("UDB_LIVE_TARGET_TENANT_ID", null);
    String targetProject = env("UDB_LIVE_TARGET_PROJECT", null);
    String unapprovedTenantId = env("UDB_LIVE_UNAPPROVED_TENANT_ID", null);

    ManagedChannel channel = UdbChannels.forTarget(authTarget, false);
    try {
      // Login and Authenticate are public: they carry the human tenant code.
      Metadata publicHeaders = headers(tenant, project, "public", null);

      // ── 1. Principal.username ────────────────────────────────────────────
      LoginResponse humanLogin;
      try {
        humanLogin =
            authn(channel, publicHeaders)
                .login(
                    LoginRequest.newBuilder()
                        .setUsername(humanUsername)
                        .setPassword(humanPassword)
                        .setTenantHint(tenant)
                        .setProjectHint(project)
                        .setDeviceName("java-sdk-service-accounts-human")
                        .build());
      } catch (StatusRuntimeException err) {
        throw new AssertionError("step 1: human login failed: " + describe(err), err);
      }
      String humanBearer = humanLogin.getAccessToken();
      assertFalse(humanBearer.isEmpty(), "step 1: the human login returned no access token");
      Principal human;
      try {
        human =
            authn(channel, publicHeaders)
                .authenticate(
                    AuthnRequest.newBuilder()
                        .setBearerToken(humanBearer)
                        .setTenantHint(tenant)
                        .setProjectHint(project)
                        .build())
                .getPrincipal();
      } catch (StatusRuntimeException err) {
        throw new AssertionError("step 1: Authenticate(human bearer) failed: " + describe(err), err);
      }
      assertTrue(
          human.getUsername().equalsIgnoreCase(humanUsername),
          "step 1: principal.username = '" + human.getUsername() + "', want '" + humanUsername + "'");
      String homeTenantId = human.getTenantId();
      assertFalse(homeTenantId.isEmpty(), "step 1: the human principal has no canonical tenant_id");
      String humanProject = firstNonEmpty(human.getProjectId(), project);

      // ── 2. service bearer roles + username ───────────────────────────────
      LoginResponse serviceLogin;
      try {
        serviceLogin =
            authn(channel, publicHeaders)
                .login(
                    LoginRequest.newBuilder()
                        .setUsername(serviceUsername)
                        .setPassword(servicePassword)
                        .setTenantHint(tenant)
                        .setProjectHint(project)
                        .setDeviceName("java-sdk-service-accounts-service")
                        .build());
      } catch (StatusRuntimeException err) {
        throw new AssertionError("step 2: service-account login failed: " + describe(err), err);
      }
      String serviceBearer = serviceLogin.getAccessToken();
      assertFalse(serviceBearer.isEmpty(), "step 2: the service-account login returned no access token");
      Principal svc;
      try {
        svc =
            authn(channel, publicHeaders)
                .authenticate(
                    AuthnRequest.newBuilder()
                        .setBearerToken(serviceBearer)
                        .setTenantHint(tenant)
                        .setProjectHint(project)
                        .build())
                .getPrincipal();
      } catch (StatusRuntimeException err) {
        throw new AssertionError("step 2: Authenticate(service bearer) failed: " + describe(err), err);
      }
      assertTrue(
          svc.getUsername().equalsIgnoreCase(serviceUsername),
          "step 2: principal.username = '" + svc.getUsername() + "', want '" + serviceUsername + "'");
      assertEquals(
          serviceUserId,
          svc.getUserId(),
          "step 2: principal.user_id = '" + svc.getUserId() + "', want '" + serviceUserId + "'");
      assertEquals(
          AccountKind.ACCOUNT_KIND_SERVICE_ACCOUNT,
          svc.getAccountKind(),
          "step 2: principal.account_kind = " + svc.getAccountKind() + ", want ACCOUNT_KIND_SERVICE_ACCOUNT");
      assertEquals(
          homeTenantId,
          svc.getTenantId(),
          "step 2: principal.tenant_id = '" + svc.getTenantId() + "', want the home tenant '" + homeTenantId + "'");
      assertTrue(
          svc.getRolesList().contains(serviceRole),
          "step 2: principal.roles = " + svc.getRolesList() + ", missing the bound role '" + serviceRole + "'");
      for (String forbidden : List.of(withheldRole, "platform_admin", "organization_owner")) {
        assertFalse(
            svc.getRolesList().contains(forbidden),
            "step 2: principal.roles = " + svc.getRolesList() + " carries the withheld role '" + forbidden + "'");
      }
      assertTrue(
          svc.getScopesList().contains("udb:authz:check-access-for"),
          "step 2: principal.scopes = " + svc.getScopesList() + ", missing udb:authz:check-access-for");
      Metadata serviceHeaders =
          headers(homeTenantId, firstNonEmpty(svc.getProjectId(), project), "service", serviceBearer);

      // ── 3. delegated CheckAccess ─────────────────────────────────────────
      CheckAccessResponse delegated;
      try {
        delegated =
            authz(channel, serviceHeaders)
                .checkAccess(
                    CheckAccessRequest.newBuilder()
                        .setUserId(delegatedUserId)
                        .setObject(CHECKED_OBJECT)
                        .setAction(CHECKED_ACTION)
                        .build());
      } catch (StatusRuntimeException err) {
        throw new AssertionError(
            "step 3: delegated CheckAccess for " + delegatedUserId + " failed: " + describe(err), err);
      }
      assertEquals(
          delegatedUserId,
          delegated.getEvaluatedSubject(),
          "step 3: evaluated_subject = '" + delegated.getEvaluatedSubject()
              + "', want the delegated user '" + delegatedUserId + "'");
      String stranger = UUID.randomUUID().toString();
      StatusRuntimeException strangerRefused =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  authz(channel, serviceHeaders)
                      .checkAccess(
                          CheckAccessRequest.newBuilder()
                              .setUserId(stranger)
                              .setObject(CHECKED_OBJECT)
                              .setAction(CHECKED_ACTION)
                              .build()),
              "step 3: delegated CheckAccess for the unknown id " + stranger + " was accepted");
      assertEquals(
          Status.Code.PERMISSION_DENIED,
          strangerRefused.getStatus().getCode(),
          "step 3: delegated CheckAccess for the unknown id " + stranger
              + " must be PERMISSION_DENIED, got " + describe(strangerRefused));

      // ── 4. tenant token exchange ─────────────────────────────────────────
      ExchangeServiceTokenForTenantResponse exchanged;
      try {
        exchanged =
            authn(channel, serviceHeaders)
                .exchangeServiceTokenForTenant(
                    ExchangeServiceTokenForTenantRequest.newBuilder()
                        .setTargetTenantId(targetTenantId)
                        .setTargetProjectId(targetProject)
                        .addRequestedScopes(EXCHANGED_SCOPE)
                        .build());
      } catch (StatusRuntimeException err) {
        throw new AssertionError(
            "step 4: ExchangeServiceTokenForTenant(" + targetTenantId + ") failed: " + describe(err), err);
      }
      assertFalse(exchanged.getAccessToken().isEmpty(), "step 4: the exchange returned no access token");
      assertEquals(
          targetTenantId,
          exchanged.getTenantId(),
          "step 4: exchanged tenant_id = '" + exchanged.getTenantId() + "', want '" + targetTenantId + "'");
      assertEquals(
          targetProject,
          exchanged.getProjectId(),
          "step 4: exchanged project_id = '" + exchanged.getProjectId() + "', want '" + targetProject + "'");
      assertEquals(
          List.of(EXCHANGED_SCOPE),
          List.copyOf(exchanged.getScopesList()),
          "step 4: exchanged scopes = " + exchanged.getScopesList() + ", want [" + EXCHANGED_SCOPE + "]");
      assertFalse(exchanged.getMembershipId().isEmpty(), "step 4: the exchange returned no membership_id");
      long now = System.currentTimeMillis() / 1000L;
      long expiresAt = exchanged.getExpiresAtUnix();
      assertTrue(
          expiresAt > now && expiresAt <= now + 15 * 60 + 60,
          "step 4: expires_at_unix = " + expiresAt + ", want in (" + now + ", " + (now + 15 * 60 + 60) + "]");
      String exchangedBearer = exchanged.getAccessToken();
      Principal exchangedPrincipal;
      try {
        exchangedPrincipal =
            authn(channel, headers(targetTenantId, targetProject, "exchanged-authn", null))
                .authenticate(
                    AuthnRequest.newBuilder()
                        .setBearerToken(exchangedBearer)
                        .setTenantHint(targetTenantId)
                        .setProjectHint(targetProject)
                        .build())
                .getPrincipal();
      } catch (StatusRuntimeException err) {
        throw new AssertionError("step 4: Authenticate(exchanged bearer) failed: " + describe(err), err);
      }
      assertEquals(
          targetTenantId,
          exchangedPrincipal.getTenantId(),
          "step 4: exchanged principal.tenant_id = '" + exchangedPrincipal.getTenantId()
              + "', want '" + targetTenantId + "'");
      assertEquals(
          serviceUserId,
          exchangedPrincipal.getUserId(),
          "step 4: exchanged principal.user_id = '" + exchangedPrincipal.getUserId()
              + "', want '" + serviceUserId + "'");
      CheckAccessResponse inTarget;
      try {
        inTarget =
            authz(channel, headers(targetTenantId, targetProject, "exchanged", exchangedBearer))
                .checkAccess(
                    // CheckAccess requires a user_id; naming the caller itself is a self-check.
                    CheckAccessRequest.newBuilder()
                        .setUserId(serviceUserId)
                        .setObject(CHECKED_OBJECT)
                        .setAction(CHECKED_ACTION)
                        .build());
      } catch (StatusRuntimeException err) {
        throw new AssertionError(
            "step 4: CheckAccess in the target tenant with the exchanged bearer failed: " + describe(err), err);
      }
      assertEquals(
          serviceUserId,
          inTarget.getEvaluatedSubject(),
          "step 4: target-tenant evaluated_subject = '" + inTarget.getEvaluatedSubject()
              + "', want the service account '" + serviceUserId + "'");
      StatusRuntimeException unapproved =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  authn(channel, serviceHeaders)
                      .exchangeServiceTokenForTenant(
                          ExchangeServiceTokenForTenantRequest.newBuilder()
                              .setTargetTenantId(unapprovedTenantId)
                              .setTargetProjectId(targetProject)
                              .build()),
              "step 4: the exchange for the unapproved tenant " + unapprovedTenantId + " was accepted");
      assertEquals(
          Status.Code.PERMISSION_DENIED,
          unapproved.getStatus().getCode(),
          "step 4: the exchange for the unapproved tenant " + unapprovedTenantId
              + " must be PERMISSION_DENIED, got " + describe(unapproved));

      // ── 5. phone-only CreateUser ─────────────────────────────────────────
      Metadata humanHeaders = headers(homeTenantId, humanProject, "human", humanBearer);
      String phone = "+1555" + randomDigits(7);
      CreateUserResponse created;
      try {
        created =
            authn(channel, humanHeaders)
                .createUser(
                    CreateUserRequest.newBuilder()
                        .setUsername("sdk-live-phone-java-" + randomHex(6))
                        .setPhone(phone)
                        .setPassword("SdkLivePhone#2026")
                        .setTenantId(homeTenantId)
                        .setProjectId(humanProject)
                        .build());
      } catch (StatusRuntimeException err) {
        throw new AssertionError("step 5: phone-only CreateUser failed: " + describe(err), err);
      }
      User user = created.getUser();
      assertFalse(user.getUserId().isEmpty(), "step 5: CreateUser returned no user_id");
      assertTrue(user.getEmail().isEmpty(), "step 5: user.email = '" + user.getEmail() + "', want empty");
      assertEquals(phone, user.getPhone(), "step 5: user.phone = '" + user.getPhone() + "', want '" + phone + "'");
      assertEquals(
          UserStatus.USER_STATUS_PENDING_VERIFICATION,
          user.getStatus(),
          "step 5: user.status = " + user.getStatus() + ", want USER_STATUS_PENDING_VERIFICATION");
      assertFalse(created.getOtpId().isEmpty(), "step 5: CreateUser returned no otp_id");
      SendOTPResponse sent;
      try {
        sent =
            authn(channel, humanHeaders)
                .sendOTP(
                    SendOTPRequest.newBuilder()
                        .setUserId(user.getUserId())
                        .setOtpType(OTPType.OTP_TYPE_PHONE_VERIFICATION)
                        .build());
      } catch (StatusRuntimeException err) {
        throw new AssertionError("step 5: SendOTP(PHONE_VERIFICATION) failed: " + describe(err), err);
      }
      if (sent.getDevOtpCode().isEmpty()) {
        fail("step 5: SendOTP returned no dev_otp_code (is UDB_OTP_DEV_ECHO=1 set on the broker?)");
      }
      // The echoed code belongs to the OTP SendOTP just issued.
      String otpId = firstNonEmpty(sent.getOtpId(), created.getOtpId());
      VerifyOTPResponse verified;
      try {
        verified =
            authn(channel, humanHeaders)
                .verifyOTP(
                    VerifyOTPRequest.newBuilder().setOtpId(otpId).setCode(sent.getDevOtpCode()).build());
      } catch (StatusRuntimeException err) {
        throw new AssertionError("step 5: VerifyOTP(" + otpId + ") failed: " + describe(err), err);
      }
      assertTrue(verified.getVerified(), "step 5: VerifyOTP(" + otpId + ") verified = false");
      assertEquals(
          OTPType.OTP_TYPE_PHONE_VERIFICATION,
          verified.getOtpType(),
          "step 5: VerifyOTP otp_type = " + verified.getOtpType() + ", want OTP_TYPE_PHONE_VERIFICATION");
      GetUserResponse fetched;
      try {
        fetched =
            authn(channel, humanHeaders)
                .getUser(GetUserRequest.newBuilder().setUserId(user.getUserId()).build());
      } catch (StatusRuntimeException err) {
        throw new AssertionError("step 5: GetUser(" + user.getUserId() + ") failed: " + describe(err), err);
      }
      assertEquals(
          UserStatus.USER_STATUS_ACTIVE,
          fetched.getUser().getStatus(),
          "step 5: after VerifyOTP user.status = " + fetched.getUser().getStatus() + ", want USER_STATUS_ACTIVE");
      assertEquals(
          phone,
          fetched.getUser().getPhone(),
          "step 5: after VerifyOTP user.phone = '" + fetched.getUser().getPhone() + "', want '" + phone + "'");
    } finally {
      channel.shutdownNow();
      try {
        channel.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException err) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
