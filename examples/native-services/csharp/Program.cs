// Native control-plane services — C# example (admin + consumer flows).
//
// Mirrors the Go/Python/PHP/TypeScript native-services examples:
//
//   admin    : register → role/assign/policy → check → mint API key → authenticate
//              → native-access. Drives the raw Authn/Authz/ApiKey stubs (provisioning
//              is an admin concern) and prints `export UDB_API_KEY=…` at the end.
//   consumer : authenticate an API key, then run the everyday authz surface
//              (can / require / batch / explain / native-access) via the
//              UdbProject facade + UdbAuthClient wrapper.
//
// Prerequisites — a running UDB broker with native auth enabled:
//
//   docker compose -f docker-compose.integration.yml up -d --wait postgres kafka redis
//   docker compose -f docker-compose.integration.yml --profile broker up -d --wait udb
//
// Run:
//   dotnet run -- admin
//   UDB_API_KEY=udbk_... dotnet run -- consumer
//
// The integration `udb` service sets UDB_ABAC_DEFAULT_ALLOW=true so the admin
// RPCs authorize without first bootstrapping policies for this example's identity.

using Udb.Client;
using AuthnV1 = udb.core.Authn.Services.V1;
using AuthzV1 = udb.core.Authz.Services.V1;
using ApikeyV1 = udb.core.Apikey.Services.V1;

const string brokerAddr = "http://localhost:50051";

var mode = args.Length > 0 ? args[0].ToLowerInvariant() : "admin";

var config = new UdbProjectConfig
{
    Target = brokerAddr,
    TenantId = "acme",
    ProjectId = "billing",
    Purpose = "control-plane",
    ServiceIdentity = "examples.native-csharp",
    CorrelationId = "native-csharp-example",
    Scopes = new[] { "udb:*" },
};

switch (mode)
{
    case "admin":
        await RunAdminAsync(config);
        break;
    case "consumer":
        await RunConsumerAsync(config);
        break;
    default:
        Console.Error.WriteLine($"unknown mode '{mode}'. Use 'admin' or 'consumer'.");
        Environment.Exit(2);
        break;
}

// ── Admin flow ──────────────────────────────────────────────────────────────
static async Task RunAdminAsync(UdbProjectConfig config)
{
    await using var project = await UdbProject.ProjectAsync(config);
    var suffix = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString();
    var headers = project.Headers();

    // ── Step 1 (simplest): register a native user ────────────────────────────
    var createUser = await project.Auth.Authn.CreateUserAsync(new AuthnV1.CreateUserRequest
    {
        Username = $"alice_{suffix}",
        Email = $"alice_{suffix}@example.com",
        Password = "CorrectHorse1!",
        FullName = "Alice Example",
        TenantId = config.TenantId,
        ProjectId = config.ProjectId,
    }, headers).ResponseAsync;
    var userId = createUser.User.UserId;
    Console.WriteLine($"1) registered user {userId} ({createUser.User.Username})");

    // ── Step 2: define authorization — RBAC role → assignment → allow policy ─
    var roleCode = $"reader_{suffix}";
    var createRole = await project.Authz.Authz.CreateRoleAsync(new AuthzV1.CreateRoleRequest
    {
        Name = $"Reader {suffix}",
        RoleCode = roleCode,
        CreatedBy = userId,
        Domain = config.TenantId,
        TenantId = config.TenantId,
        ProjectId = config.ProjectId,
    }, headers).ResponseAsync;
    var roleId = createRole.Role.RoleId;

    await project.Authz.Authz.AssignRoleAsync(new AuthzV1.AssignRoleRequest
    {
        UserId = userId,
        RoleId = roleId,
        Domain = config.TenantId,
        AssignedBy = userId,
        TenantId = config.TenantId,
        ProjectId = config.ProjectId,
    }, headers).ResponseAsync;

    await project.Authz.Authz.PutAuthzPolicyAsync(new AuthzV1.PutAuthzPolicyRequest
    {
        Policy = new AuthzV1.AuthzPolicyRecord
        {
            Id = $"policy-{roleCode}",
            Enabled = true,
            Effect = "allow",
            Tenant = config.TenantId,
            Project = config.ProjectId,
            Role = roleCode,
            Action = "data.select",
            Resource = "invoice",
        },
    }, headers).ResponseAsync;
    Console.WriteLine($"2) role \"{roleCode}\" assigned to user; allow policy on invoice/data.select added");

    // ── Step 3: the everyday authorization call ──────────────────────────────
    Console.WriteLine($"3) check data.select on invoice → allowed={await CheckAccessAsync(project, userId, config.TenantId, "invoice", "data.select", headers)}");
    Console.WriteLine($"   check data.delete on invoice → allowed={await CheckAccessAsync(project, userId, config.TenantId, "invoice", "data.delete", headers)} (no policy grants it)");

    // ── Step 4: machine credentials — mint an API key, then authenticate it ──
    var created = await project.CreateApiKeyAsync(new ApikeyV1.CreateApiKeyRequest
    {
        Name = "native-csharp-example-key",
        OwnerId = userId,
        Scopes = { "data:read" },
    });
    var plainKey = created.PlainKey;
    try
    {
        var authn = await project.Auth.AuthenticateApiKeyAsync(plainKey);
        Console.WriteLine($"4) api key authenticated → principal user_id={authn.Principal.UserId} scopes=[{string.Join(",", authn.Principal.Scopes)}]");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"4) authenticate api key: {ex.Message}");
    }
    Console.WriteLine($"   minted dev API key → export UDB_API_KEY={plainKey}");

    // ── Step 5 (advanced): Stage-2 native DB fast-path grant ─────────────────
    try
    {
        var grant = await project.Authz.NativeAccessAsync(
            new AuthzV1.ResourceRef { ResourceName = "invoice", MessageType = "invoice" },
            "data.select", "control-plane");
        if (grant is null)
        {
            Console.WriteLine("5) access allowed, but no native grant minted (server native-access not configured)");
        }
        else
        {
            Console.WriteLine($"5) native grant: role={grant.Role} session_vars={grant.SessionVariables.Count} (open a direct DB conn on grant.Dsn)");
        }
    }
    catch (UdbAuthzDeniedException ex)
    {
        Console.WriteLine($"5) native access denied: {ex.DenyReason}");
    }
}

static async Task<bool> CheckAccessAsync(UdbProject project, string userId, string tenantId, string obj, string action, Grpc.Core.Metadata headers)
{
    var resp = await project.Authz.Authz.CheckAccessAsync(new AuthzV1.CheckAccessRequest
    {
        UserId = userId,
        Domain = tenantId,
        Object = obj,
        Action = action,
    }, headers).ResponseAsync;
    return resp.Allowed;
}

// ── Consumer flow ───────────────────────────────────────────────────────────
static async Task RunConsumerAsync(UdbProjectConfig config)
{
    var apiKey = Environment.GetEnvironmentVariable("UDB_API_KEY");
    if (string.IsNullOrEmpty(apiKey))
    {
        Console.Error.WriteLine("set UDB_API_KEY=<the key the admin flow minted> first.");
        Environment.Exit(2);
        return;
    }

    await using var project = await UdbProject.ProjectAsync(config);

    // 1) Authenticate the API key — what an app does on every request.
    var authn = await project.Auth.AuthenticateApiKeyAsync(apiKey);
    Console.WriteLine($"1) authenticated → user_id={authn.Principal.UserId} scopes=[{string.Join(",", authn.Principal.Scopes)}]");

    var invoice = new AuthzV1.ResourceRef { ResourceName = "invoice", MessageType = "invoice" };

    // 2) can() — cached allow/deny (second call is served from the TTL cache).
    var (allowed, decision) = await project.Authz.CanAsync(invoice, "data.select");
    Console.WriteLine($"2) can data.select on invoice → {allowed} (cache_ttl={decision.CacheTtlSeconds}s, entries={project.Authz.Cache.Count})");

    // 3) explain() — same check, never throws, surfaces the deny reason.
    var explained = await project.Authz.ExplainAsync(invoice, "data.delete");
    Console.WriteLine($"3) explain data.delete on invoice → allowed={explained.Allowed} reason=\"{explained.DenyReason}\"");

    // 4) require() — throws UdbAuthzDeniedException on deny.
    try
    {
        await project.Authz.RequireAsync(invoice, "data.delete");
        Console.WriteLine("4) require data.delete → allowed");
    }
    catch (UdbAuthzDeniedException ex)
    {
        Console.WriteLine($"4) require data.delete → denied: {ex.DenyReason}");
    }

    // 5) batchCan() — many checks in one RPC.
    var batch = await project.Authz.BatchCanAsync(new[]
    {
        ("invoice", "data.select"),
        ("invoice", "data.delete"),
    });
    foreach (var kv in batch)
    {
        Console.WriteLine($"5) batch {kv.Key} → {kv.Value}");
    }

    // 6) native-access — Stage-2 fast-path grant.
    try
    {
        var grant = await project.Authz.NativeAccessAsync(invoice, "data.select");
        Console.WriteLine(grant is null
            ? "6) native access allowed, no grant minted"
            : $"6) native grant role={grant.Role} session_vars={grant.SessionVariables.Count}");
    }
    catch (UdbAuthzDeniedException ex)
    {
        Console.WriteLine($"6) native access denied: {ex.DenyReason}");
    }
}
