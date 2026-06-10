package dev.udb.examples;

import com.udb.core.apikey.services.v1.CreateApiKeyResponse;
import com.udb.core.authn.entity.v1.User;
import com.udb.core.authn.services.v1.AuthnResponse;
import com.udb.core.authn.services.v1.CreateUserRequest;
import com.udb.core.authn.services.v1.CreateUserResponse;
import com.udb.core.authz.entity.v1.Role;
import com.udb.core.authz.services.v1.AssignRoleRequest;
import com.udb.core.authz.services.v1.AuthzPolicyRecord;
import com.udb.core.authz.services.v1.CheckAccessRequest;
import com.udb.core.authz.services.v1.CheckAccessResponse;
import com.udb.core.authz.services.v1.CreateRoleRequest;
import com.udb.core.authz.services.v1.CreateRoleResponse;
import com.udb.core.authz.services.v1.NativeAccessGrant;
import com.udb.core.authz.services.v1.PutAuthzPolicyRequest;
import com.udb.core.authz.services.v1.ResourceRef;
import dev.udb.client.Udb;
import dev.udb.client.UdbAuthClient;
import dev.udb.client.UdbProject;
import dev.udb.client.UdbProjectConfig;
import java.util.List;

/**
 * Progressive, simplest→advanced tour of UDB's native control-plane services
 * from Java — symmetric with the Go example: register a user, define RBAC
 * (role → assignment → policy), verify the access check, mint an API key,
 * authenticate it, then request a Stage-2 native-access grant. All over the
 * hand-written {@link UdbProject} facade + {@link UdbAuthClient} ergonomics.
 *
 * <p>Prereqs — a running broker with native auth enabled. The repo's integration
 * stack is the easy path:
 *
 * <pre>
 *   docker compose -f docker-compose.integration.yml up -d --wait postgres kafka redis
 *   docker compose -f docker-compose.integration.yml --profile broker up -d --wait udb
 * </pre>
 *
 * Then: {@code UDB_TARGET=127.0.0.1:50051 mvn -q compile exec:java} (or compile
 * against the SDK and run {@code dev.udb.examples.Main}).
 */
public final class Main {
  private Main() {}

  public static void main(String[] args) {
    String target = System.getenv().getOrDefault("UDB_TARGET", "localhost:50051");
    String suffix = Long.toString(System.nanoTime());

    // The caller identity is attached to every request as gRPC metadata. Broad
    // scopes + a control-plane purpose so the admin RPCs authorize cleanly. The
    // facade shares this one identity across data + auth + apikey services.
    UdbProjectConfig config = UdbProjectConfig.builder()
        .target(target)
        .tenantId("acme")
        .projectId("billing")
        .purpose("control-plane")
        .serviceIdentity("examples.native-java")
        .correlationId("native-java-example")
        .scopes(List.of("udb:*"))
        .clientCatalogVersion("1.0.0")
        .build();

    try (UdbProject udb = Udb.project(config)) {
      UdbAuthClient auth = udb.auth();

      // ── Step 1 (simplest): register a native user ──────────────────────────
      CreateUserResponse userResp = auth.authn().createUser(CreateUserRequest.newBuilder()
          .setUsername("alice_" + suffix)
          .setEmail("alice_" + suffix + "@example.com")
          .setPassword("CorrectHorse1!")
          .setFullName("Alice Example")
          .setTenantId("acme")
          .setProjectId("billing")
          .build());
      User user = userResp.getUser();
      String userId = user.getUserId();
      System.out.printf("1) registered user %s (%s)%n", userId, user.getUsername());

      // ── Step 2: define authorization — RBAC role → assignment → allow policy ─
      CreateRoleResponse roleResp = udb.authz().authz().createRole(CreateRoleRequest.newBuilder()
          .setName("Reader " + suffix)
          .setRoleCode("reader_" + suffix)
          .setCreatedBy(userId)
          .setDomain("acme")
          .setTenantId("acme")
          .setProjectId("billing")
          .build());
      Role role = roleResp.getRole();

      auth.authz().assignRole(AssignRoleRequest.newBuilder()
          .setUserId(userId)
          .setRoleId(role.getRoleId())
          .setDomain("acme")
          .setAssignedBy(userId)
          .setTenantId("acme")
          .setProjectId("billing")
          .build());

      auth.authz().putAuthzPolicy(PutAuthzPolicyRequest.newBuilder()
          .setPolicy(AuthzPolicyRecord.newBuilder()
              .setId("policy-" + role.getRoleCode())
              .setEnabled(true)
              .setEffect("allow")
              .setTenant("acme")
              .setProject("billing")
              .setRole(role.getRoleCode())
              .setAction("data.select")
              .setResource("invoice")
              .build())
          .build());
      System.out.printf(
          "2) role %s assigned to user; allow policy on invoice/data.select added%n",
          role.getRoleCode());

      // ── Step 3: the everyday authorization call ────────────────────────────
      System.out.printf("3) check data.select on invoice → allowed=%b%n",
          checkAccess(auth, userId, "invoice", "data.select"));
      System.out.printf("   check data.delete on invoice → allowed=%b (no policy grants it)%n",
          checkAccess(auth, userId, "invoice", "data.delete"));

      // ── Step 4: machine credentials — mint an API key, then authenticate it ──
      CreateApiKeyResponse keyResp = udb.createApiKey(
          "native-java-example-key", userId, List.of("data:read"));
      String plainKey = keyResp.getPlainKey();
      try {
        AuthnResponse authd = auth.authenticateApiKey(plainKey);
        System.out.printf("4) api key authenticated → principal user_id=%s scopes=%s%n",
            authd.getPrincipal().getUserId(), authd.getPrincipal().getScopesList());
      } catch (RuntimeException err) {
        System.out.printf("4) authenticate api key: %s%n", err.getMessage());
      }
      // Print the minted key (dev only) so the other consumer examples can use it.
      System.out.printf("   minted dev API key → export UDB_API_KEY=%s%n", plainKey);

      // ── Step 5 (advanced): Stage-2 native DB fast-path grant ───────────────
      ResourceRef invoice = ResourceRef.newBuilder()
          .setResourceName("invoice")
          .setMessageType("invoice")
          .build();
      try {
        NativeAccessGrant grant = auth.nativeAccess(invoice, "data.select", "control-plane");
        if (grant == null) {
          System.out.println(
              "5) access allowed, but no native grant minted (server native-access not configured)");
        } else {
          System.out.printf("5) native grant: role=%s session_vars=%d (open a JDBC conn on grant.getDsn())%n",
              grant.getRole(), grant.getSessionVariablesCount());
        }
      } catch (RuntimeException err) {
        System.out.printf("5) native access denied or unavailable: %s%n", err.getMessage());
      }
    }
  }

  private static boolean checkAccess(UdbAuthClient auth, String userId, String object, String action) {
    CheckAccessResponse resp = auth.checkAccess(CheckAccessRequest.newBuilder()
        .setUserId(userId)
        .setDomain("acme")
        .setTenantId("acme")
        .setProjectId("billing")
        .setObject(object)
        .setAction(action)
        .build());
    return resp.getAllowed();
  }
}
