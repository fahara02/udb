// Live service-account conformance.
//
// Gated on UDB_LIVE_SDK_TESTS=1; otherwise the test returns without touching the
// network. CI's "SDK service accounts (live)" job runs it against the broker it
// just built, on every push to main, after scripts/provision-service-account-live.py
// has created the identities it reads from the environment. That job requires the
// TRX to record exactly one executed, passing test, so this class holds ONE test.
//
// It proves, through the C# SDK's channel, generated clients and header
// convention against the served broker:
//  1. Authenticate names the account (Principal.Username);
//  2. a service-account bearer carries its bound roles, never a withheld
//     admin-segment role, and its typed-grant scopes;
//  3. a service account with udb:authz:check-access-for evaluates CheckAccess for
//     a person of its tenant (EvaluatedSubject), and is refused for an id that is
//     not one;
//  4. ExchangeServiceTokenForTenant mints a short-lived target-tenant token that
//     authenticates and authorizes IN the target tenant, and is refused for a
//     tenant with no approved membership;
//  5. a phone-only CreateUser is verified by a PHONE_VERIFICATION OTP and the
//     account becomes ACTIVE.

using System.Security.Cryptography;
using Grpc.Core;
using Xunit;
using AuthnEntityV1 = Udb.Core.Authn.Entity.V1;
using AuthnV1 = Udb.Core.Authn.Services.V1;
using AuthzV1 = Udb.Core.Authz.Services.V1;

namespace Udb.Client.Tests;

public sealed class LiveServiceAccountDelegationTests
{
    private const string CheckedObject = "sdk-live-chart";
    private const string CheckedAction = "read";
    private const string ExchangedScope = "udb:authz:check-access";

    private static string Env(string name, string? fallback = null)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (!string.IsNullOrWhiteSpace(value))
        {
            return value.Trim();
        }
        return fallback ?? throw new InvalidOperationException($"{name} is required when UDB_LIVE_SDK_TESTS=1");
    }

    private static string Url(string target) =>
        target.Contains("://", StringComparison.Ordinal) ? target : $"http://{target}";

    // The SDK's caller headers asserting tenantId/projectId, with a correlation id
    // and a request id (authenticated native calls require a request context) and,
    // when given, the bearer credential.
    private static Metadata Headers(string tenantId, string projectId, string role, string? bearer = null)
    {
        var headers = new Metadata
        {
            { "x-tenant-id", tenantId },
            { "x-user-id", "" },
            { "x-purpose", "csharp.live.service-accounts" },
            { "x-correlation-id", $"csharp-live-service-accounts-{role}" },
            { "x-request-id", Guid.NewGuid().ToString() },
            { "x-scopes", "" },
            { "x-service-identity", "csharp.sdk.live" },
            { "x-udb-project-id", projectId },
            { "x-udb-client-catalog-version", UdbClient.ProtocolVersion },
        };
        if (!string.IsNullOrWhiteSpace(bearer))
        {
            headers.Add("authorization", $"Bearer {bearer}");
        }
        return global::Udb.Client.Generated.UdbRequestContext.Ensure(headers);
    }

    private static DateTime Deadline() => DateTime.UtcNow.AddSeconds(20);

    private static string FirstNonEmpty(params string?[] values) =>
        values.FirstOrDefault(v => !string.IsNullOrEmpty(v)) ?? string.Empty;

    private static string RandomHex(int bytes) =>
        Convert.ToHexString(RandomNumberGenerator.GetBytes(bytes)).ToLowerInvariant();

    private static string RandomDigits(int count) =>
        string.Concat(Enumerable.Range(0, count).Select(_ => (char)('0' + RandomNumberGenerator.GetInt32(10))));

    private static string Describe(RpcException err) => $"code={err.StatusCode} {err.Status.Detail}";

    private static async Task<T> Step<T>(string what, Func<Task<T>> call)
    {
        try
        {
            return await call();
        }
        catch (RpcException err)
        {
            throw new InvalidOperationException($"{what} failed: {Describe(err)}", err);
        }
    }

    [Fact]
    public async Task LiveServiceAccountDelegationConformance()
    {
        if (Environment.GetEnvironmentVariable("UDB_LIVE_SDK_TESTS") != "1")
        {
            return;
        }
        var target = Env("UDB_GRPC_TARGET");
        var authTarget = Env("UDB_AUTH_GRPC_TARGET", target);
        var tenant = Env("UDB_LIVE_TENANT");
        var project = Env("UDB_LIVE_PROJECT");
        var humanUsername = Env("UDB_LIVE_USERNAME");
        var humanPassword = Env("UDB_LIVE_PASSWORD");
        var serviceUsername = Env("UDB_LIVE_SERVICE_USERNAME");
        var servicePassword = Env("UDB_LIVE_SERVICE_PASSWORD");
        var serviceUserId = Env("UDB_LIVE_SERVICE_USER_ID");
        var serviceRole = Env("UDB_LIVE_SERVICE_ROLE");
        var withheldRole = Env("UDB_LIVE_SERVICE_WITHHELD_ROLE");
        var delegatedUserId = Env("UDB_LIVE_DELEGATED_USER_ID");
        var targetTenantId = Env("UDB_LIVE_TARGET_TENANT_ID");
        var targetProject = Env("UDB_LIVE_TARGET_PROJECT");
        var unapprovedTenantId = Env("UDB_LIVE_UNAPPROVED_TENANT_ID");

        using var channel = UdbChannel.ForAddress(Url(authTarget));
        var authn = new AuthnV1.AuthnService.AuthnServiceClient(channel);
        var authz = new AuthzV1.AuthzService.AuthzServiceClient(channel);

        // Login and Authenticate are public: they carry the human tenant code.

        // ── 1. Principal.Username ───────────────────────────────────────────
        var humanLogin = await Step("step 1: human login", () => authn.LoginAsync(
            new AuthnV1.LoginRequest
            {
                Username = humanUsername,
                Password = humanPassword,
                TenantHint = tenant,
                ProjectHint = project,
                DeviceName = "csharp-sdk-service-accounts-human",
            },
            Headers(tenant, project, "public"), Deadline()).ResponseAsync);
        var humanBearer = humanLogin.AccessToken;
        Assert.False(string.IsNullOrEmpty(humanBearer), "step 1: the human login returned no access token");
        var humanAuth = await Step("step 1: Authenticate(human bearer)", () => authn.AuthenticateAsync(
            new AuthnV1.AuthnRequest { BearerToken = humanBearer, TenantHint = tenant, ProjectHint = project },
            Headers(tenant, project, "public"), Deadline()).ResponseAsync);
        var human = humanAuth.Principal;
        Assert.True(human is not null, "step 1: Authenticate(human bearer) returned no principal");
        Assert.True(
            string.Equals(human!.Username, humanUsername, StringComparison.OrdinalIgnoreCase),
            $"step 1: principal.username = '{human.Username}', want '{humanUsername}'");
        var homeTenantId = human.TenantId;
        Assert.False(string.IsNullOrEmpty(homeTenantId), "step 1: the human principal has no canonical tenant_id");
        var humanProject = FirstNonEmpty(human.ProjectId, project);

        // ── 2. service bearer roles + username ──────────────────────────────
        var serviceLogin = await Step("step 2: service-account login", () => authn.LoginAsync(
            new AuthnV1.LoginRequest
            {
                Username = serviceUsername,
                Password = servicePassword,
                TenantHint = tenant,
                ProjectHint = project,
                DeviceName = "csharp-sdk-service-accounts-service",
            },
            Headers(tenant, project, "public"), Deadline()).ResponseAsync);
        var serviceBearer = serviceLogin.AccessToken;
        Assert.False(string.IsNullOrEmpty(serviceBearer), "step 2: the service-account login returned no access token");
        var serviceAuth = await Step("step 2: Authenticate(service bearer)", () => authn.AuthenticateAsync(
            new AuthnV1.AuthnRequest { BearerToken = serviceBearer, TenantHint = tenant, ProjectHint = project },
            Headers(tenant, project, "public"), Deadline()).ResponseAsync);
        var svc = serviceAuth.Principal;
        Assert.True(svc is not null, "step 2: Authenticate(service bearer) returned no principal");
        Assert.True(
            string.Equals(svc!.Username, serviceUsername, StringComparison.OrdinalIgnoreCase),
            $"step 2: principal.username = '{svc.Username}', want '{serviceUsername}'");
        Assert.True(
            svc.UserId == serviceUserId,
            $"step 2: principal.user_id = '{svc.UserId}', want '{serviceUserId}'");
        Assert.True(
            svc.AccountKind == AuthnEntityV1.AccountKind.ServiceAccount,
            $"step 2: principal.account_kind = {svc.AccountKind}, want ACCOUNT_KIND_SERVICE_ACCOUNT");
        Assert.True(
            svc.TenantId == homeTenantId,
            $"step 2: principal.tenant_id = '{svc.TenantId}', want the home tenant '{homeTenantId}'");
        var roles = string.Join(",", svc.Roles);
        Assert.True(
            svc.Roles.Contains(serviceRole),
            $"step 2: principal.roles = [{roles}], missing the bound role '{serviceRole}'");
        foreach (var forbidden in new[] { withheldRole, "platform_admin", "organization_owner" })
        {
            Assert.False(
                svc.Roles.Contains(forbidden),
                $"step 2: principal.roles = [{roles}] carries the withheld role '{forbidden}'");
        }
        Assert.True(
            svc.Scopes.Contains("udb:authz:check-access-for"),
            $"step 2: principal.scopes = [{string.Join(",", svc.Scopes)}], missing udb:authz:check-access-for");
        var serviceProject = FirstNonEmpty(svc.ProjectId, project);

        // ── 3. delegated CheckAccess ────────────────────────────────────────
        var delegated = await Step($"step 3: delegated CheckAccess for {delegatedUserId}", () => authz.CheckAccessAsync(
            new AuthzV1.CheckAccessRequest { UserId = delegatedUserId, Object = CheckedObject, Action = CheckedAction },
            Headers(homeTenantId, serviceProject, "service", serviceBearer), Deadline()).ResponseAsync);
        Assert.True(
            delegated.EvaluatedSubject == delegatedUserId,
            $"step 3: evaluated_subject = '{delegated.EvaluatedSubject}', want the delegated user '{delegatedUserId}'");
        var stranger = Guid.NewGuid().ToString();
        var strangerRefused = await Assert.ThrowsAsync<RpcException>(() => authz.CheckAccessAsync(
            new AuthzV1.CheckAccessRequest { UserId = stranger, Object = CheckedObject, Action = CheckedAction },
            Headers(homeTenantId, serviceProject, "service", serviceBearer), Deadline()).ResponseAsync);
        Assert.True(
            strangerRefused.StatusCode == StatusCode.PermissionDenied,
            $"step 3: delegated CheckAccess for the unknown id {stranger} must be PERMISSION_DENIED, got {Describe(strangerRefused)}");

        // ── 4. tenant token exchange ────────────────────────────────────────
        var exchangeRequest = new AuthnV1.ExchangeServiceTokenForTenantRequest
        {
            TargetTenantId = targetTenantId,
            TargetProjectId = targetProject,
        };
        exchangeRequest.RequestedScopes.Add(ExchangedScope);
        var exchanged = await Step($"step 4: ExchangeServiceTokenForTenant({targetTenantId})", () => authn.ExchangeServiceTokenForTenantAsync(
            exchangeRequest,
            Headers(homeTenantId, serviceProject, "service", serviceBearer), Deadline()).ResponseAsync);
        Assert.False(string.IsNullOrEmpty(exchanged.AccessToken), "step 4: the exchange returned no access token");
        Assert.True(
            exchanged.TenantId == targetTenantId,
            $"step 4: exchanged tenant_id = '{exchanged.TenantId}', want '{targetTenantId}'");
        Assert.True(
            exchanged.ProjectId == targetProject,
            $"step 4: exchanged project_id = '{exchanged.ProjectId}', want '{targetProject}'");
        Assert.True(
            exchanged.Scopes.Count == 1 && exchanged.Scopes[0] == ExchangedScope,
            $"step 4: exchanged scopes = [{string.Join(",", exchanged.Scopes)}], want [{ExchangedScope}]");
        Assert.False(string.IsNullOrEmpty(exchanged.MembershipId), "step 4: the exchange returned no membership_id");
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var expiresAt = exchanged.ExpiresAtUnix;
        Assert.True(
            expiresAt > now && expiresAt <= now + 15 * 60 + 60,
            $"step 4: expires_at_unix = {expiresAt}, want in ({now}, {now + 15 * 60 + 60}]");
        var exchangedBearer = exchanged.AccessToken;
        var exchangedAuth = await Step("step 4: Authenticate(exchanged bearer)", () => authn.AuthenticateAsync(
            new AuthnV1.AuthnRequest { BearerToken = exchangedBearer, TenantHint = targetTenantId, ProjectHint = targetProject },
            Headers(targetTenantId, targetProject, "exchanged-authn"), Deadline()).ResponseAsync);
        var exchangedPrincipal = exchangedAuth.Principal;
        Assert.True(exchangedPrincipal is not null, "step 4: Authenticate(exchanged bearer) returned no principal");
        Assert.True(
            exchangedPrincipal!.TenantId == targetTenantId,
            $"step 4: exchanged principal.tenant_id = '{exchangedPrincipal.TenantId}', want '{targetTenantId}'");
        Assert.True(
            exchangedPrincipal.UserId == serviceUserId,
            $"step 4: exchanged principal.user_id = '{exchangedPrincipal.UserId}', want '{serviceUserId}'");
        var inTarget = await Step("step 4: CheckAccess in the target tenant with the exchanged bearer", () => authz.CheckAccessAsync(
            // CheckAccess requires a user_id; naming the caller itself is a self-check.
            new AuthzV1.CheckAccessRequest { UserId = serviceUserId, Object = CheckedObject, Action = CheckedAction },
            Headers(targetTenantId, targetProject, "exchanged", exchangedBearer), Deadline()).ResponseAsync);
        Assert.True(
            inTarget.EvaluatedSubject == serviceUserId,
            $"step 4: target-tenant evaluated_subject = '{inTarget.EvaluatedSubject}', want the service account '{serviceUserId}'");
        var unapproved = await Assert.ThrowsAsync<RpcException>(() => authn.ExchangeServiceTokenForTenantAsync(
            new AuthnV1.ExchangeServiceTokenForTenantRequest
            {
                TargetTenantId = unapprovedTenantId,
                TargetProjectId = targetProject,
            },
            Headers(homeTenantId, serviceProject, "service", serviceBearer), Deadline()).ResponseAsync);
        Assert.True(
            unapproved.StatusCode == StatusCode.PermissionDenied,
            $"step 4: the exchange for the unapproved tenant {unapprovedTenantId} must be PERMISSION_DENIED, got {Describe(unapproved)}");

        // ── 5. phone-only CreateUser ────────────────────────────────────────
        var phone = "+1555" + RandomDigits(7);
        var created = await Step("step 5: phone-only CreateUser", () => authn.CreateUserAsync(
            new AuthnV1.CreateUserRequest
            {
                Username = "sdk-live-phone-csharp-" + RandomHex(6),
                Phone = phone,
                Password = "SdkLivePhone#2026",
                TenantId = homeTenantId,
                ProjectId = humanProject,
            },
            Headers(homeTenantId, humanProject, "human", humanBearer), Deadline()).ResponseAsync);
        var user = created.User;
        Assert.True(user is not null, "step 5: CreateUser returned no user");
        Assert.False(string.IsNullOrEmpty(user!.UserId), "step 5: CreateUser returned no user_id");
        Assert.True(string.IsNullOrEmpty(user.Email), $"step 5: user.email = '{user.Email}', want empty");
        Assert.True(user.Phone == phone, $"step 5: user.phone = '{user.Phone}', want '{phone}'");
        Assert.True(
            user.Status == AuthnEntityV1.UserStatus.PendingVerification,
            $"step 5: user.status = {user.Status}, want USER_STATUS_PENDING_VERIFICATION");
        Assert.False(string.IsNullOrEmpty(created.OtpId), "step 5: CreateUser returned no otp_id");
        var userId = user.UserId;
        var sent = await Step("step 5: SendOTP(PHONE_VERIFICATION)", () => authn.SendOTPAsync(
            new AuthnV1.SendOTPRequest { UserId = userId, OtpType = AuthnEntityV1.OTPType.PhoneVerification },
            Headers(homeTenantId, humanProject, "human", humanBearer), Deadline()).ResponseAsync);
        Assert.False(
            string.IsNullOrEmpty(sent.DevOtpCode),
            "step 5: SendOTP returned no dev_otp_code (is UDB_OTP_DEV_ECHO=1 set on the broker?)");
        // The echoed code belongs to the OTP SendOTP just issued.
        var otpId = FirstNonEmpty(sent.OtpId, created.OtpId);
        var verified = await Step($"step 5: VerifyOTP({otpId})", () => authn.VerifyOTPAsync(
            new AuthnV1.VerifyOTPRequest { OtpId = otpId, Code = sent.DevOtpCode },
            Headers(homeTenantId, humanProject, "human", humanBearer), Deadline()).ResponseAsync);
        Assert.True(verified.Verified, $"step 5: VerifyOTP({otpId}) verified = false");
        Assert.True(
            verified.OtpType == AuthnEntityV1.OTPType.PhoneVerification,
            $"step 5: VerifyOTP otp_type = {verified.OtpType}, want OTP_TYPE_PHONE_VERIFICATION");
        var fetched = await Step($"step 5: GetUser({userId})", () => authn.GetUserAsync(
            new AuthnV1.GetUserRequest { UserId = userId },
            Headers(homeTenantId, humanProject, "human", humanBearer), Deadline()).ResponseAsync);
        Assert.True(fetched.User is not null, $"step 5: GetUser({userId}) returned no user");
        Assert.True(
            fetched.User!.Status == AuthnEntityV1.UserStatus.Active,
            $"step 5: after VerifyOTP user.status = {fetched.User.Status}, want USER_STATUS_ACTIVE");
        Assert.True(
            fetched.User.Phone == phone,
            $"step 5: after VerifyOTP user.phone = '{fetched.User.Phone}', want '{phone}'");
    }
}
