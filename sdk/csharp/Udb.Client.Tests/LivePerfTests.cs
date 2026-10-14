// Full-surface live benchmark: every generated RPC, timed through Grpc.Net.Client.
//
// Gated on UDB_LIVE_SDK_TESTS=1 and UDB_LIVE_PERF=1; otherwise the test returns
// without touching the network. Request bodies come from the shared benchmark
// plan (UDB_BENCH_PLAN) that sdk/python/tests/export_bench_plan.py writes after
// the common seed: each RPC's documented body resolved against real disposable
// entities, in measurement order. Requests are parsed into this SDK's own
// generated message types. The plan carries no credentials: the harness logs in
// itself and substitutes its own session material for the plan's sentinels. It
// times only the call and writes sdk/csharp/perf_report_csharp.md in the
// collector's format.

using System.Globalization;
using System.Reflection;
using System.Text;
using System.Text.Json.Nodes;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Grpc.Core;
using Grpc.Net.Client;
using Xunit;

namespace Udb.Client.Tests;

public sealed class LivePerfTests
{
    private const string Authn = "udb.core.authn.services.v1.AuthnService";
    private const string DataBroker = "udb.services.v1.DataBroker";
    private static readonly TimeSpan CallTimeout = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan StreamTimeout = TimeSpan.FromSeconds(12);
    private const string AccessSentinel = "__UDB_BENCH_ACCESS_TOKEN__";
    private const string RefreshSentinel = "__UDB_BENCH_REFRESH_TOKEN__";
    private const string SessionSentinel = "__UDB_BENCH_SESSION_ID__";
    private const string CsrfSentinel = "__UDB_BENCH_CSRF_TOKEN__";

    private readonly Dictionary<string, MessageDescriptor> _messages = new();
    private readonly Dictionary<string, ServiceDescriptor> _services = new();
    private JsonParser _parser = JsonParser.Default;
    private CallInvoker _broker = null!;
    private CallInvoker _native = null!;
    private long _calls;

    private sealed class Identity
    {
        public string Tenant = "";
        public string Project = "";
        public string Bearer = "";
        public Identity Copy() => new() { Tenant = Tenant, Project = Project, Bearer = Bearer };
    }

    private sealed record Outcome(string Code, string Detail)
    {
        public static readonly Outcome Ok = new("OK", "");
    }

    private sealed record Sample(
        string Service, string Rpc, string ApiAlias, string OperationId, string Kind, int Iters,
        string Err, string ErrDetail, double P50, double P99, double Mean);

    private sealed record Credentials(string Access, string Refresh, string Session, string Csrf, long ExpiresIn);

    private static string Env(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidOperationException($"{name} is required when UDB_LIVE_PERF=1");
        }
        return value.Trim();
    }

    /// <summary>gRPC status code names as the other harnesses spell them (FAILED_PRECONDITION).</summary>
    private static string CodeName(StatusCode code)
    {
        if (code == StatusCode.OK)
        {
            return "OK";
        }
        var name = code.ToString();
        var builder = new StringBuilder();
        for (var i = 0; i < name.Length; i++)
        {
            if (i > 0 && char.IsUpper(name[i]))
            {
                builder.Append('_');
            }
            builder.Append(char.ToUpperInvariant(name[i]));
        }
        return builder.ToString();
    }

    private static Outcome Failed(RpcException error) => new(CodeName(error.StatusCode), error.Status.Detail ?? "");

    // ── Descriptors and calls ────────────────────────────────────────────────

    private void LoadDescriptors()
    {
        var files = new List<FileDescriptor>();
        foreach (var type in typeof(UdbClient).Assembly.GetTypes())
        {
            if (!type.IsClass || !type.Name.EndsWith("Reflection", StringComparison.Ordinal))
            {
                continue;
            }
            var property = type.GetProperty("Descriptor", BindingFlags.Public | BindingFlags.Static);
            if (property?.GetValue(null) is FileDescriptor file)
            {
                files.Add(file);
            }
        }
        foreach (var file in files)
        {
            foreach (var message in file.MessageTypes)
            {
                Index(message);
            }
            foreach (var service in file.Services)
            {
                _services[service.FullName] = service;
            }
        }
        _parser = new JsonParser(JsonParser.Settings.Default.WithTypeRegistry(TypeRegistry.FromFiles(files.ToArray())));
    }

    private void Index(MessageDescriptor message)
    {
        _messages[message.FullName] = message;
        foreach (var nested in message.NestedTypes)
        {
            Index(nested);
        }
    }

    private MethodDescriptor Rpc(string path)
    {
        var trimmed = path.TrimStart('/');
        var slash = trimmed.IndexOf('/');
        if (slash < 0 || !_services.TryGetValue(trimmed[..slash], out var service))
        {
            throw new InvalidOperationException($"{path} is not in the SDK's generated services");
        }
        return service.FindMethodByName(trimmed[(slash + 1)..])
            ?? throw new InvalidOperationException($"{path} is not in the SDK's generated services");
    }

    private static Method<IMessage, IMessage> Method(MethodDescriptor rpc)
    {
        var type = rpc.IsClientStreaming
            ? (rpc.IsServerStreaming ? MethodType.DuplexStreaming : MethodType.ClientStreaming)
            : (rpc.IsServerStreaming ? MethodType.ServerStreaming : MethodType.Unary);
        var input = rpc.InputType.Parser;
        var output = rpc.OutputType.Parser;
        return new Method<IMessage, IMessage>(
            type,
            rpc.Service.FullName,
            rpc.Name,
            Marshallers.Create<IMessage>(message => message.ToByteArray(), bytes => input.ParseFrom(bytes)),
            Marshallers.Create<IMessage>(message => message.ToByteArray(), bytes => output.ParseFrom(bytes)));
    }

    private IMessage Message(string typeName, JsonNode body)
    {
        if (!_messages.TryGetValue(typeName, out var descriptor))
        {
            throw new InvalidOperationException($"{typeName} is not in the SDK's generated types");
        }
        return _parser.Parse(body.ToJsonString(), descriptor);
    }

    private CallOptions Options(Identity identity, TimeSpan timeout, CancellationToken token = default)
    {
        var calls = Interlocked.Increment(ref _calls);
        var headers = new Grpc.Core.Metadata
        {
            { "x-tenant-id", identity.Tenant },
            { "x-udb-project-id", identity.Project },
            { "x-purpose", "csharp.live.perf" },
            { "x-correlation-id", $"csharp-live-perf-{calls}" },
            { "x-request-id", Guid.NewGuid().ToString("N") },
            { "x-service-identity", "csharp.sdk.perf" },
        };
        if (identity.Bearer.Length > 0)
        {
            headers.Add("authorization", $"Bearer {identity.Bearer}");
        }
        return new CallOptions(headers, DateTime.UtcNow + timeout, token);
    }

    private CallInvoker Invoker(string path) =>
        path.StartsWith($"/{DataBroker}/", StringComparison.Ordinal) ? _broker : _native;

    private async Task<IMessage> Unary(string path, IMessage request, Identity identity)
    {
        var call = Invoker(path).AsyncUnaryCall(Method(Rpc(path)), null, Options(identity, CallTimeout), request);
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    private async Task<Outcome> FirstResponse(string path, IMessage request, Identity identity)
    {
        var rpc = Rpc(path);
        var method = Method(rpc);
        var invoker = Invoker(path);
        using var cancel = new CancellationTokenSource(StreamTimeout);
        var options = Options(identity, StreamTimeout, cancel.Token);
        try
        {
            switch (method.Type)
            {
                case MethodType.ServerStreaming:
                {
                    using var call = invoker.AsyncServerStreamingCall(method, null, options, request);
                    await call.ResponseStream.MoveNext(cancel.Token).ConfigureAwait(false);
                    break;
                }
                case MethodType.ClientStreaming:
                {
                    using var call = invoker.AsyncClientStreamingCall(method, null, options);
                    await call.RequestStream.WriteAsync(request).ConfigureAwait(false);
                    await call.RequestStream.CompleteAsync().ConfigureAwait(false);
                    await call.ResponseAsync.ConfigureAwait(false);
                    break;
                }
                default:
                {
                    using var call = invoker.AsyncDuplexStreamingCall(method, null, options);
                    await call.RequestStream.WriteAsync(request).ConfigureAwait(false);
                    await call.RequestStream.CompleteAsync().ConfigureAwait(false);
                    await call.ResponseStream.MoveNext(cancel.Token).ConfigureAwait(false);
                    break;
                }
            }
            return Outcome.Ok;
        }
        catch (RpcException error) when (cancel.IsCancellationRequested && error.StatusCode == StatusCode.Cancelled)
        {
            return new Outcome("DEADLINE_EXCEEDED", "no first response");
        }
        catch (RpcException error)
        {
            return Failed(error);
        }
        catch (OperationCanceledException)
        {
            return new Outcome("DEADLINE_EXCEEDED", "no first response");
        }
    }

    private static string Text(IMessage message, string field)
    {
        var descriptor = message.Descriptor.FindFieldByName(field);
        return descriptor?.Accessor.GetValue(message)?.ToString() ?? "";
    }

    // ── Session ──────────────────────────────────────────────────────────────

    private async Task<Credentials> Login(Identity identity, string user, string password, string device)
    {
        var body = new JsonObject
        {
            ["username"] = user,
            ["password"] = password,
            ["tenant_hint"] = identity.Tenant,
            ["project_hint"] = identity.Project,
            ["device_name"] = device,
        };
        var anonymous = identity.Copy();
        anonymous.Bearer = "";
        var response = await Unary($"/{Authn}/Login", Message("udb.core.authn.services.v1.LoginRequest", body), anonymous)
            .ConfigureAwait(false);
        var access = Text(response, "access_token");
        if (access.Length == 0)
        {
            throw new InvalidOperationException($"Login for {device} returned no access token");
        }
        long.TryParse(Text(response, "access_token_expires_in"), out var expires);
        return new Credentials(access, Text(response, "refresh_token"), Text(response, "session_id"),
            Text(response, "csrf_token"), expires > 0 ? expires : 240);
    }

    private async Task<IMessage> Principal(Identity identity, string token)
    {
        var body = new JsonObject
        {
            ["bearer_token"] = token,
            ["tenant_hint"] = identity.Tenant,
            ["project_hint"] = identity.Project,
        };
        var response = await Unary($"/{Authn}/Authenticate", Message("udb.core.authn.services.v1.AuthnRequest", body), identity)
            .ConfigureAwait(false);
        return (IMessage)(response.Descriptor.FindFieldByName("principal").Accessor.GetValue(response)
            ?? throw new InvalidOperationException("Authenticate returned no principal"));
    }

    private static bool HasRole(IMessage principal, string role)
    {
        var roles = principal.Descriptor.FindFieldByName("roles").Accessor.GetValue(principal) as System.Collections.IEnumerable;
        return roles?.Cast<object>().Any(value => string.Equals(value?.ToString()?.Trim(), role, StringComparison.OrdinalIgnoreCase)) == true;
    }

    private static JsonNode? Substitute(JsonNode? body, IReadOnlyDictionary<string, string> sentinels)
    {
        switch (body)
        {
            case JsonValue value when value.TryGetValue<string>(out var text):
                foreach (var (sentinel, replacement) in sentinels)
                {
                    text = text.Replace(sentinel, replacement, StringComparison.Ordinal);
                }
                return JsonValue.Create(text);
            case JsonArray array:
                return new JsonArray(array.Select(item => Substitute(item, sentinels)).ToArray());
            case JsonObject obj:
            {
                var copy = new JsonObject();
                foreach (var (key, value) in obj)
                {
                    copy[key] = Substitute(value, sentinels);
                }
                return copy;
            }
            default:
                return body?.DeepClone();
        }
    }

    // ── Measurement ──────────────────────────────────────────────────────────

    private readonly Identity _user = new();
    private readonly Identity _platform = new();
    private readonly Dictionary<string, string> _sentinels = new();
    private string _autoRefresh = "";
    private string _autoSession = "";
    private DateTime _refreshAt;

    private void Bind(Credentials credentials)
    {
        _user.Bearer = credentials.Access;
        _sentinels[AccessSentinel] = credentials.Access;
        _sentinels[CsrfSentinel] = credentials.Csrf;
        _autoRefresh = credentials.Refresh;
        _autoSession = credentials.Session;
        _refreshAt = DateTime.UtcNow.AddSeconds(180);
    }

    private async Task EnsureFresh()
    {
        if (DateTime.UtcNow < _refreshAt)
        {
            return;
        }
        var body = new JsonObject { ["refresh_token"] = _autoRefresh, ["session_id"] = _autoSession };
        var response = await Unary($"/{Authn}/RefreshToken",
            Message("udb.core.authn.services.v1.RefreshTokenRequest", body), _user).ConfigureAwait(false);
        var access = Text(response, "access_token");
        if (access.Length > 0)
        {
            _user.Bearer = access;
            _sentinels[AccessSentinel] = access;
        }
        var rotated = Text(response, "refresh_token");
        if (rotated.Length > 0)
        {
            _autoRefresh = rotated;
        }
        long.TryParse(Text(response, "access_token_expires_in"), out var expires);
        _refreshAt = DateTime.UtcNow.AddSeconds(Math.Clamp((expires > 0 ? expires : 240) * 0.6, 60, 240));
    }

    private async Task<Outcome> CdcFirstEvent(JsonObject cdc, Identity identity)
    {
        var publish = Rpc($"/{DataBroker}/PublishCDC");
        var upsertRpc = Rpc($"/{DataBroker}/Upsert");
        var subscription = Message(publish.InputType.FullName,
            new JsonObject { ["context"] = cdc["context"]!.DeepClone(), ["topic_pattern"] = "*" });
        var record = new JsonObject
        {
            ["record_id"] = cdc["record_id"]!.GetValue<string>(),
            ["tenant_id"] = identity.Tenant,
            ["project_id"] = identity.Project,
            ["lookup_key"] = "csharp-perf-cdc",
            ["payload"] = "csharp-perf-cdc",
            ["revision"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L,
        };
        var write = Message(upsertRpc.InputType.FullName, new JsonObject
        {
            ["context"] = cdc["context"]!.DeepClone(),
            ["message_type"] = cdc["message_type"]!.DeepClone(),
            ["record_json"] = Convert.ToBase64String(Encoding.UTF8.GetBytes(record.ToJsonString())),
            ["conflict_fields"] = cdc["conflict_fields"]!.DeepClone(),
        });
        using var cancel = new CancellationTokenSource(StreamTimeout);
        try
        {
            using var call = _broker.AsyncServerStreamingCall(Method(publish), null,
                Options(identity, StreamTimeout, cancel.Token), subscription);
            await call.ResponseHeadersAsync.ConfigureAwait(false);
            try
            {
                await Unary($"/{DataBroker}/Upsert", write, identity).ConfigureAwait(false);
            }
            catch (RpcException)
            {
                // The stream still has to deliver an event for the seeded row.
            }
            await call.ResponseStream.MoveNext(cancel.Token).ConfigureAwait(false);
            return Outcome.Ok;
        }
        catch (RpcException error) when (cancel.IsCancellationRequested)
        {
            _ = error;
            return new Outcome("DEADLINE_EXCEEDED", "no CDC event delivered");
        }
        catch (RpcException error)
        {
            return Failed(error);
        }
        catch (OperationCanceledException)
        {
            return new Outcome("DEADLINE_EXCEEDED", "no CDC event delivered");
        }
    }

    private async Task<(double Ms, Outcome Outcome)> TimeOne(JsonObject plan, JsonObject rpc)
    {
        await EnsureFresh().ConfigureAwait(false);
        var identity = rpc["platform"]!.GetValue<bool>() ? _platform : _user;
        var clock = System.Diagnostics.Stopwatch.StartNew();
        if (rpc["kind"]!.GetValue<string>() == "cdc_first_event")
        {
            var cdc = await CdcFirstEvent(plan["cdc"]!.AsObject(), identity).ConfigureAwait(false);
            return (clock.Elapsed.TotalMilliseconds, cdc);
        }
        if (rpc["request"] is not JsonNode body)
        {
            return (0, new Outcome("SKIP_NO_BODY", rpc["missing_body"]?.GetValue<string>() ?? "no seeded body"));
        }
        var path = rpc["path"]!.GetValue<string>();
        var inputType = rpc["input_type"]!.GetValue<string>();
        IMessage request;
        try
        {
            request = Message(inputType, Substitute(body, _sentinels)!);
        }
        catch (Exception error)
        {
            return (0, new Outcome("CLIENT_ERROR", error.Message));
        }
        var streaming = rpc["client_streaming"]!.GetValue<bool>() || rpc["server_streaming"]!.GetValue<bool>();
        clock.Restart();
        Outcome outcome;
        if (streaming)
        {
            outcome = await FirstResponse(path, request, identity).ConfigureAwait(false);
        }
        else
        {
            try
            {
                await Unary(path, request, identity).ConfigureAwait(false);
                outcome = Outcome.Ok;
            }
            catch (RpcException error)
            {
                outcome = Failed(error);
            }
        }
        var ms = clock.Elapsed.TotalMilliseconds;
        if (outcome.Code == "FAILED_PRECONDITION" && rpc["capability_optional"]!.GetValue<bool>())
        {
            var detail = outcome.Detail.ToLowerInvariant();
            if (detail.Length == 0 || detail.Contains("webrtc egress") || detail.Contains("egress is not enabled"))
            {
                outcome = outcome with { Code = "CAPABILITY_SKIPPED" };
            }
        }
        if (outcome == Outcome.Ok && rpc["reset_after_ok"] is JsonNode reset)
        {
            try
            {
                await Unary(path, Message(inputType, Substitute(reset, _sentinels)!), _user).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Best effort, as in the other harnesses.
            }
        }
        return (ms, outcome);
    }

    private static double Percentile(IReadOnlyList<double> sorted, int p) =>
        sorted[Math.Min(sorted.Count - 1, p * (sorted.Count - 1) / 100)];

    [Fact]
    public async Task LiveBenchmarkMeasuresEveryRpc()
    {
        if (Environment.GetEnvironmentVariable("UDB_LIVE_SDK_TESTS") != "1"
            || Environment.GetEnvironmentVariable("UDB_LIVE_PERF") != "1")
        {
            return;
        }
        var plan = JsonNode.Parse(File.ReadAllText(Env("UDB_BENCH_PLAN")))!.AsObject();
        var fixtureKeys = plan["seeded_fixture_keys"]!.AsArray().Select(key => key!.GetValue<string>()).ToList();
        var tenant = plan["tenant_id"]!.GetValue<string>();
        var samples = new List<Sample>();
        try
        {
            if (plan["schema_version"]!.GetValue<int>() != 1)
            {
                throw new InvalidOperationException("unsupported benchmark plan schema");
            }
            LoadDescriptors();
            static string Url(string target) => target.Contains("://", StringComparison.Ordinal) ? target : $"http://{target}";
            var target = Env("UDB_GRPC_TARGET");
            var authTarget = Environment.GetEnvironmentVariable("UDB_AUTH_GRPC_TARGET") ?? target;
            using var brokerChannel = GrpcChannel.ForAddress(Url(target));
            using var nativeChannel = GrpcChannel.ForAddress(Url(authTarget));
            _broker = brokerChannel.CreateCallInvoker();
            _native = nativeChannel.CreateCallInvoker();
            var username = Env("UDB_LIVE_USERNAME");
            var password = Env("UDB_LIVE_PASSWORD");
            _user.Tenant = Environment.GetEnvironmentVariable("UDB_LIVE_TENANT") ?? "sdk-live";
            _user.Project = Environment.GetEnvironmentVariable("UDB_LIVE_PROJECT") ?? "default";
            var first = await Login(_user, username, password, "csharp-sdk-perf");
            var canonical = Text(await Principal(_user, first.Access), "tenant_id");
            if (canonical != tenant || plan["project_id"]!.GetValue<string>() != _user.Project)
            {
                throw new InvalidOperationException("the benchmark plan was exported for another tenant or project");
            }
            _user.Tenant = canonical;
            _user.Bearer = first.Access;
            var platformLogin = await Login(_user, Env("UDB_LIVE_PLATFORM_USERNAME"), Env("UDB_LIVE_PLATFORM_PASSWORD"), "csharp-sdk-perf-platform");
            var platformWho = await Principal(_user, platformLogin.Access);
            if (!HasRole(platformWho, "platform_admin"))
            {
                throw new InvalidOperationException("the platform fixture did not issue platform_admin");
            }
            _platform.Tenant = Text(platformWho, "tenant_id");
            _platform.Project = _user.Project;
            _platform.Bearer = platformLogin.Access;
            var token = await Login(_user, username, password, "csharp-sdk-perf-token");
            var refresh = await Login(_user, username, password, "csharp-sdk-perf-refresh");
            var session = await Login(_user, username, password, "csharp-sdk-perf-session");
            var auto = await Login(_user, username, password, "csharp-sdk-perf-auto-refresh");
            _sentinels[AccessSentinel] = token.Access;
            _sentinels[CsrfSentinel] = token.Csrf;
            _sentinels[RefreshSentinel] = refresh.Refresh;
            _sentinels[SessionSentinel] = session.Session;
            _autoRefresh = auto.Refresh;
            _autoSession = auto.Session;
            _refreshAt = DateTime.UtcNow.AddSeconds(Math.Clamp(auto.ExpiresIn * 0.6, 60, 180));

            foreach (var node in plan["rpcs"]!.AsArray())
            {
                var rpc = node!.AsObject();
                if (rpc["reauthenticate_before"]!.GetValue<bool>())
                {
                    await Reauthenticate(username, password);
                }
                Console.WriteLine($"[PERF-RPC] {rpc["path"]} kind={rpc["kind"]}");
                var iters = Math.Max(1, rpc["iters"]!.GetValue<int>());
                var runs = new List<(double Ms, Outcome Outcome)>();
                if (rpc["warm_up"]!.GetValue<bool>())
                {
                    var warm = await TimeOne(plan, rpc);
                    if (warm.Outcome != Outcome.Ok)
                    {
                        runs.Add(warm);
                    }
                }
                while (runs.Count < iters)
                {
                    var run = await TimeOne(plan, rpc);
                    runs.Add(run);
                    if (run.Outcome != Outcome.Ok)
                    {
                        break;
                    }
                }
                var anyOk = runs.Any(run => run.Outcome == Outcome.Ok);
                var failure = runs.Select(run => run.Outcome).FirstOrDefault(o => o != Outcome.Ok) ?? Outcome.Ok;
                var err = anyOk && failure.Code == "CAPABILITY_SKIPPED" ? "FAILED_PRECONDITION" : failure.Code;
                var durations = runs.Select(run => run.Ms).OrderBy(ms => ms).ToList();
                samples.Add(new Sample(
                    rpc["service"]!.GetValue<string>(), rpc["rpc"]!.GetValue<string>(),
                    rpc["api_alias"]!.GetValue<string>(), rpc["operation_id"]!.GetValue<string>(),
                    rpc["kind"]!.GetValue<string>(), runs.Count, err, failure.Detail,
                    Percentile(durations, 50), Percentile(durations, 99), durations.Average()));
            }
            WriteReport(samples, fixtureKeys, tenant, null);
        }
        catch (Exception error)
        {
            WriteReport(samples, fixtureKeys, tenant, error.Message);
            throw;
        }
    }

    private async Task Reauthenticate(string username, string password)
    {
        var deadline = DateTime.UtcNow.AddSeconds(2);
        Exception? last = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var fresh = await Login(_user, username, password, "csharp-sdk-perf-final-purge");
                await Principal(_user, fresh.Access);
                Bind(fresh);
                return;
            }
            catch (Exception error)
            {
                last = error;
                await Task.Delay(50);
            }
        }
        throw new InvalidOperationException($"fresh login remained revoked after tenant cutoff: {last?.Message}");
    }

    // ── Report ───────────────────────────────────────────────────────────────

    private static string Cell(string text) => text.Replace("\n", " ").Replace("|", "\\|");

    private static string F(double value) => value.ToString("0.00", CultureInfo.InvariantCulture);

    private static void WriteReport(IReadOnlyList<Sample> samples, IReadOnlyList<string> fixtureKeys, string tenant, string? error)
    {
        var out_ = new StringBuilder();
        out_.Append("# UDB SDK Live Perf — C# (localhost)\n\n");
        out_.Append($"RPCs measured: {samples.Count}   tenant={tenant}\n\n");
        out_.Append("Every RPC is driven down its SUCCESS path: the shared seed first creates real, disposable entities "
            + "and each request's reference/ID fields are resolved to those real identifiers. The C# client "
            + "(Grpc.Net.Client, generated message types) sends and times every request itself. The TARGET is zero "
            + "failures; any residual non-OK RPC is listed under Failures for the maintainer to finish.\n\n");
        out_.Append("Unary = full request/response round-trip. Non-CDC streaming RPCs (kind=stream_first_recv) report "
            + "time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (kind=cdc_first_event, PublishCDC) reports "
            + "time-to-FIRST-EVENT.\n\n");
        if (error is not null)
        {
            out_.Append($"## Harness error\n\n`HarnessError`: {Cell(error)}\n\nThis is a partial report written before the benchmark process failed.\n\n");
        }
        out_.Append($"## Seeded fixtures\n\nCaptured semantic field -> seeded value keys used to resolve request fields: {string.Join(", ", fixtureKeys)}\n\n");
        out_.Append("## Per-service mean latency\n\n| Service | RPCs | mean ms |\n|---|--:|--:|\n");
        foreach (var group in samples.GroupBy(s => s.Service).OrderByDescending(g => g.Average(s => s.Mean)))
        {
            out_.Append($"| {group.Key} | {group.Count()} | {F(group.Average(s => s.Mean))} |\n");
        }
        var byName = samples.OrderBy(s => s.Service, StringComparer.Ordinal).ThenBy(s => s.Rpc, StringComparer.Ordinal).ToList();
        var failures = byName.Where(s => s.Err != "OK" && s.Err != "CAPABILITY_SKIPPED").ToList();
        var skipped = byName.Where(s => s.Err == "CAPABILITY_SKIPPED").ToList();
        out_.Append($"\n## Failures ({failures.Count})\n\n");
        if (failures.Count == 0)
        {
            out_.Append("No RPC returned a non-OK gRPC status.\n");
        }
        else
        {
            out_.Append("These RPCs returned a non-OK gRPC status and are FAILURES, not latency samples.\n\n");
            out_.Append("| RPC | api_alias | operation_id | kind | err | detail | p99 ms | mean ms | iters |\n|---|---|---|---|---|---|--:|--:|--:|\n");
            foreach (var s in failures)
            {
                out_.Append($"| {s.Service}/{s.Rpc} | {s.ApiAlias} | {s.OperationId} | {s.Kind} | {s.Err} | {Cell(s.ErrDetail)} | {F(s.P99)} | {F(s.Mean)} | {s.Iters} |\n");
            }
        }
        out_.Append($"\n## Capability Skips ({skipped.Count})\n\n");
        if (skipped.Count == 0)
        {
            out_.Append("No optional service capability was skipped.\n");
        }
        else
        {
            out_.Append("These RPCs reached the served path but require an optional backend capability disabled in this local profile.\n\n");
            out_.Append("| RPC | api_alias | operation_id | kind | detail |\n|---|---|---|---|---|\n");
            foreach (var s in skipped)
            {
                out_.Append($"| {s.Service}/{s.Rpc} | {s.ApiAlias} | {s.OperationId} | {s.Kind} | {Cell(s.ErrDetail)} |\n");
            }
        }
        out_.Append("\n## Slowest 20 by p99\n\n| RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms |\n|---|---|---|---|---|--:|--:|--:|\n");
        foreach (var s in samples.OrderByDescending(s => s.P99).Take(20))
        {
            out_.Append($"| {s.Service}/{s.Rpc} | {s.ApiAlias} | {s.OperationId} | {s.Kind} | {s.Err} | {F(s.P50)} | {F(s.P99)} | {F(s.Mean)} |\n");
        }
        out_.Append("\n## Full per-RPC table (sorted by service, then RPC)\n\n| Service | RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms | iters |\n|---|---|---|---|---|---|--:|--:|--:|--:|\n");
        foreach (var s in byName)
        {
            out_.Append($"| {s.Service} | {s.Rpc} | {s.ApiAlias} | {s.OperationId} | {s.Kind} | {s.Err} | {F(s.P50)} | {F(s.P99)} | {F(s.Mean)} | {s.Iters} |\n");
        }
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "Udb.Client.Tests.csproj")))
        {
            directory = directory.Parent;
        }
        var root = directory?.Parent?.FullName ?? Directory.GetCurrentDirectory();
        var report = Path.Combine(root, "perf_report_csharp.md");
        File.WriteAllText(report, out_.ToString(), new UTF8Encoding(false));
        Console.WriteLine($"C# perf: {samples.Count} RPCs measured -> {report}");
    }
}
