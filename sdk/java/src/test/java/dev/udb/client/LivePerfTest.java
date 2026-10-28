package dev.udb.client;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * Full-surface live benchmark: every generated RPC, timed through grpc-java.
 *
 * <p>Gated on {@code UDB_LIVE_SDK_TESTS=1} and {@code UDB_LIVE_PERF=1}. Request
 * bodies come from the shared benchmark plan ({@code UDB_BENCH_PLAN}) that
 * {@code sdk/python/tests/export_bench_plan.py} writes after the common seed:
 * each RPC's documented body resolved against real disposable entities, in
 * measurement order, plus the contract's descriptor set. The plan carries no
 * credentials; this harness logs in itself and substitutes its own session
 * material for the plan's sentinels. It times only the call and writes
 * {@code sdk/java/perf_report_java.md} in the collector's format.
 */
class LivePerfTest {
  private static final String AUTHN = "udb.core.authn.services.v1.AuthnService";
  private static final String DATA_BROKER = "udb.services.v1.DataBroker";
  private static final long CALL_TIMEOUT_MS = 20_000;
  private static final long STREAM_TIMEOUT_MS = 12_000;
  private static final String ACCESS = "__UDB_BENCH_ACCESS_TOKEN__";
  private static final String REFRESH = "__UDB_BENCH_REFRESH_TOKEN__";
  private static final String SESSION = "__UDB_BENCH_SESSION_ID__";
  private static final String CSRF = "__UDB_BENCH_CSRF_TOKEN__";

  private final Map<String, Descriptors.ServiceDescriptor> services = new HashMap<>();
  private final Map<String, Descriptor> messages = new HashMap<>();
  private JsonFormat.Parser parser;
  private ManagedChannel broker;
  private ManagedChannel nativeChannel;
  private long calls;

  /** Identity headers for one principal; a fresh correlation id per call. */
  private static final class Identity {
    String tenant;
    String project;
    String bearer = "";

    Identity copy() {
      Identity other = new Identity();
      other.tenant = tenant;
      other.project = project;
      other.bearer = bearer;
      return other;
    }
  }

  private record Outcome(String code, String detail) {
    static final Outcome OK = new Outcome("OK", "");

    static Outcome of(StatusRuntimeException error) {
      Status status = error.getStatus();
      return new Outcome(
          status.getCode().name(), status.getDescription() == null ? "" : status.getDescription());
    }
  }

  private record Sample(
      String service,
      String rpc,
      String apiAlias,
      String operationId,
      String kind,
      int iters,
      String err,
      String errDetail,
      double p50,
      double p99,
      double mean) {}

  private static String env(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(name + " is required when UDB_LIVE_PERF=1");
    }
    return value.trim();
  }

  private static String text(DynamicMessage message, String field) {
    Descriptors.FieldDescriptor descriptor = message.getDescriptorForType().findFieldByName(field);
    return descriptor == null ? "" : String.valueOf(message.getField(descriptor));
  }

  // ── Descriptors and calls ────────────────────────────────────────────────

  private void loadDescriptors(String encoded) throws Exception {
    FileDescriptorSet set = FileDescriptorSet.parseFrom(Base64.getDecoder().decode(encoded));
    Map<String, FileDescriptorProto> protos = new HashMap<>();
    for (FileDescriptorProto proto : set.getFileList()) {
      protos.put(proto.getName(), proto);
    }
    Map<String, FileDescriptor> built = new HashMap<>();
    for (String name : protos.keySet()) {
      build(name, protos, built);
    }
    // google.protobuf.Any fields resolve their packed type through this registry.
    com.google.protobuf.TypeRegistry.Builder registry = com.google.protobuf.TypeRegistry.newBuilder();
    for (FileDescriptor file : built.values()) {
      for (Descriptor message : file.getMessageTypes()) {
        index(message);
      }
      if (!file.getMessageTypes().isEmpty()) {
        registry.add(file.getMessageTypes().get(0));
      }
      for (Descriptors.ServiceDescriptor service : file.getServices()) {
        services.put(service.getFullName(), service);
      }
    }
    parser = JsonFormat.parser().usingTypeRegistry(registry.build());
  }

  private void index(Descriptor message) {
    messages.put(message.getFullName(), message);
    for (Descriptor nested : message.getNestedTypes()) {
      index(nested);
    }
  }

  private static FileDescriptor build(
      String name, Map<String, FileDescriptorProto> protos, Map<String, FileDescriptor> built)
      throws Descriptors.DescriptorValidationException {
    FileDescriptor existing = built.get(name);
    if (existing != null) {
      return existing;
    }
    FileDescriptorProto proto = protos.get(name);
    if (proto == null) {
      throw new IllegalStateException("descriptor set is missing " + name);
    }
    List<FileDescriptor> dependencies = new ArrayList<>();
    for (String dependency : proto.getDependencyList()) {
      dependencies.add(build(dependency, protos, built));
    }
    FileDescriptor file =
        FileDescriptor.buildFrom(proto, dependencies.toArray(new FileDescriptor[0]));
    built.put(name, file);
    return file;
  }

  private static final class Marshaller implements MethodDescriptor.Marshaller<DynamicMessage> {
    private final Descriptor descriptor;

    Marshaller(Descriptor descriptor) {
      this.descriptor = descriptor;
    }

    @Override
    public InputStream stream(DynamicMessage value) {
      return value.toByteString().newInput();
    }

    @Override
    public DynamicMessage parse(InputStream stream) {
      try {
        return DynamicMessage.parseFrom(descriptor, stream);
      } catch (IOException error) {
        throw Status.INTERNAL.withDescription("decode response: " + error).asRuntimeException();
      }
    }
  }

  private MethodDescriptor<DynamicMessage, DynamicMessage> method(String path) {
    String trimmed = path.startsWith("/") ? path.substring(1) : path;
    int slash = trimmed.indexOf('/');
    Descriptors.ServiceDescriptor service = services.get(trimmed.substring(0, slash));
    if (service == null) {
      throw new IllegalStateException(path + " is not in the descriptor set");
    }
    Descriptors.MethodDescriptor rpc = service.findMethodByName(trimmed.substring(slash + 1));
    if (rpc == null) {
      throw new IllegalStateException(path + " is not in the descriptor set");
    }
    MethodDescriptor.MethodType type =
        rpc.isClientStreaming()
            ? (rpc.isServerStreaming()
                ? MethodDescriptor.MethodType.BIDI_STREAMING
                : MethodDescriptor.MethodType.CLIENT_STREAMING)
            : (rpc.isServerStreaming()
                ? MethodDescriptor.MethodType.SERVER_STREAMING
                : MethodDescriptor.MethodType.UNARY);
    return MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
        .setType(type)
        .setFullMethodName(trimmed)
        .setRequestMarshaller(new Marshaller(rpc.getInputType()))
        .setResponseMarshaller(new Marshaller(rpc.getOutputType()))
        .build();
  }

  private DynamicMessage message(String typeName, JsonElement body) throws IOException {
    Descriptor descriptor = messages.get(typeName);
    if (descriptor == null) {
      throw new IllegalStateException(typeName + " is not in the descriptor set");
    }
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    parser.merge(body.toString(), builder);
    return builder.build();
  }

  private Channel channel(String path, Identity identity) {
    Metadata headers = new Metadata();
    calls++;
    headers.put(key("x-tenant-id"), identity.tenant);
    headers.put(key("x-udb-project-id"), identity.project);
    headers.put(key("x-purpose"), "java.live.perf");
    headers.put(key("x-correlation-id"), "java-live-perf-" + calls);
    headers.put(key("x-request-id"), UUID.randomUUID().toString());
    headers.put(key("x-service-identity"), "java.sdk.perf");
    if (!identity.bearer.isEmpty()) {
      headers.put(key("authorization"), "Bearer " + identity.bearer);
    }
    ManagedChannel base = path.startsWith("/" + DATA_BROKER + "/") ? broker : nativeChannel;
    return ClientInterceptors.intercept(base, MetadataUtils.newAttachHeadersInterceptor(headers));
  }

  private static Metadata.Key<String> key(String name) {
    return Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER);
  }

  private DynamicMessage unary(String path, DynamicMessage request, Identity identity) {
    return ClientCalls.blockingUnaryCall(
        channel(path, identity),
        method(path),
        CallOptions.DEFAULT.withDeadlineAfter(CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        request);
  }

  private Outcome firstResponse(String path, DynamicMessage request, Identity identity) {
    MethodDescriptor<DynamicMessage, DynamicMessage> rpc = method(path);
    Channel channel = channel(path, identity);
    CallOptions options =
        CallOptions.DEFAULT.withDeadlineAfter(STREAM_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    if (rpc.getType() == MethodDescriptor.MethodType.SERVER_STREAMING) {
      Context.CancellableContext context = Context.current().withCancellation();
      try {
        return context.call(
            () -> {
              Iterator<DynamicMessage> stream =
                  ClientCalls.blockingServerStreamingCall(channel, rpc, options, request);
              stream.hasNext();
              return Outcome.OK;
            });
      } catch (StatusRuntimeException error) {
        return Outcome.of(error);
      } catch (Exception error) {
        return new Outcome("CLIENT_ERROR", error.toString());
      } finally {
        context.cancel(null);
      }
    }
    CompletableFuture<Outcome> first = new CompletableFuture<>();
    StreamObserver<DynamicMessage> responses =
        new StreamObserver<>() {
          @Override
          public void onNext(DynamicMessage value) {
            first.complete(Outcome.OK);
          }

          @Override
          public void onError(Throwable error) {
            first.complete(
                error instanceof StatusRuntimeException status
                    ? Outcome.of(status)
                    : new Outcome("CLIENT_ERROR", error.toString()));
          }

          @Override
          public void onCompleted() {
            first.complete(Outcome.OK);
          }
        };
    io.grpc.ClientCall<DynamicMessage, DynamicMessage> call = channel.newCall(rpc, options);
    StreamObserver<DynamicMessage> requests =
        rpc.getType() == MethodDescriptor.MethodType.BIDI_STREAMING
            ? ClientCalls.asyncBidiStreamingCall(call, responses)
            : ClientCalls.asyncClientStreamingCall(call, responses);
    requests.onNext(request);
    requests.onCompleted();
    try {
      return first.get(STREAM_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (TimeoutException error) {
      return new Outcome("DEADLINE_EXCEEDED", "no first response");
    } catch (Exception error) {
      return new Outcome("CLIENT_ERROR", error.toString());
    } finally {
      call.cancel("first response measured", null);
    }
  }

  // ── Session ──────────────────────────────────────────────────────────────

  private record Credentials(
      String access, String refresh, String session, String csrf, long expiresIn) {}

  private Credentials login(Identity identity, String user, String password, String device)
      throws IOException {
    JsonObject body = new JsonObject();
    body.addProperty("username", user);
    body.addProperty("password", password);
    body.addProperty("tenant_hint", identity.tenant);
    body.addProperty("project_hint", identity.project);
    body.addProperty("device_name", device);
    Identity anonymous = identity.copy();
    anonymous.bearer = "";
    DynamicMessage response =
        unary(
            "/" + AUTHN + "/Login",
            message("udb.core.authn.services.v1.LoginRequest", body),
            anonymous);
    String access = text(response, "access_token");
    if (access.isEmpty()) {
      throw new IllegalStateException("Login for " + device + " returned no access token");
    }
    long expires = Long.parseLong(text(response, "access_token_expires_in"));
    return new Credentials(
        access,
        text(response, "refresh_token"),
        text(response, "session_id"),
        text(response, "csrf_token"),
        expires > 0 ? expires : 240);
  }

  private DynamicMessage principal(Identity identity, String token) throws IOException {
    JsonObject body = new JsonObject();
    body.addProperty("bearer_token", token);
    body.addProperty("tenant_hint", identity.tenant);
    body.addProperty("project_hint", identity.project);
    DynamicMessage response =
        unary(
            "/" + AUTHN + "/Authenticate",
            message("udb.core.authn.services.v1.AuthnRequest", body),
            identity);
    return (DynamicMessage)
        response.getField(response.getDescriptorForType().findFieldByName("principal"));
  }

  @SuppressWarnings("unchecked")
  private static boolean hasRole(DynamicMessage principal, String role) {
    Descriptors.FieldDescriptor roles = principal.getDescriptorForType().findFieldByName("roles");
    for (Object value : (List<Object>) principal.getField(roles)) {
      if (String.valueOf(value).trim().equalsIgnoreCase(role)) {
        return true;
      }
    }
    return false;
  }

  private static JsonElement substitute(JsonElement body, Map<String, String> sentinels) {
    if (body.isJsonPrimitive() && body.getAsJsonPrimitive().isString()) {
      String value = body.getAsString();
      for (Map.Entry<String, String> sentinel : sentinels.entrySet()) {
        value = value.replace(sentinel.getKey(), sentinel.getValue());
      }
      return new JsonPrimitive(value);
    }
    if (body.isJsonArray()) {
      JsonArray out = new JsonArray();
      for (JsonElement item : body.getAsJsonArray()) {
        out.add(substitute(item, sentinels));
      }
      return out;
    }
    if (body.isJsonObject()) {
      JsonObject out = new JsonObject();
      for (Map.Entry<String, JsonElement> field : body.getAsJsonObject().entrySet()) {
        out.add(field.getKey(), substitute(field.getValue(), sentinels));
      }
      return out;
    }
    return body;
  }

  // ── Measurement ──────────────────────────────────────────────────────────

  private final Identity user = new Identity();
  private final Identity platform = new Identity();
  private final Map<String, String> sentinels = new LinkedHashMap<>();
  private String autoRefresh = "";
  private String autoSession = "";
  private long refreshAtNanos;
  // The plan's service account (null when the plan names none), logged in
  // lazily right before its first row and re-minted after three minutes, so a
  // long run cannot hand that row an expired access token.
  private JsonObject serviceAccountCaller;
  private Identity serviceAccount;
  private long serviceAccountMintedNanos;
  private boolean serviceAccountFailed;

  private void bind(Credentials credentials) {
    user.bearer = credentials.access();
    sentinels.put(ACCESS, credentials.access());
    sentinels.put(CSRF, credentials.csrf());
    autoRefresh = credentials.refresh();
    autoSession = credentials.session();
    refreshAtNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(180);
  }

  private void ensureFresh() throws IOException {
    if (System.nanoTime() < refreshAtNanos) {
      return;
    }
    JsonObject body = new JsonObject();
    body.addProperty("refresh_token", autoRefresh);
    body.addProperty("session_id", autoSession);
    DynamicMessage response =
        unary(
            "/" + AUTHN + "/RefreshToken",
            message("udb.core.authn.services.v1.RefreshTokenRequest", body),
            user);
    String access = text(response, "access_token");
    if (!access.isEmpty()) {
      user.bearer = access;
      sentinels.put(ACCESS, access);
    }
    String rotated = text(response, "refresh_token");
    if (!rotated.isEmpty()) {
      autoRefresh = rotated;
    }
    long expires = Long.parseLong(text(response, "access_token_expires_in"));
    double seconds = Math.max(60, Math.min((expires > 0 ? expires : 240) * 0.6, 240));
    refreshAtNanos = System.nanoTime() + (long) (seconds * 1e9);
  }

  private void ensureServiceAccount() {
    if (serviceAccountCaller == null || serviceAccountFailed) {
      return;
    }
    if (serviceAccount != null
        && System.nanoTime() - serviceAccountMintedNanos < TimeUnit.SECONDS.toNanos(180)) {
      return;
    }
    String username = serviceAccountCaller.get("username").getAsString();
    try {
      Identity hints = user.copy();
      hints.tenant = serviceAccountCaller.get("tenant_id").getAsString();
      hints.project = serviceAccountCaller.get("project_id").getAsString();
      hints.bearer = "";
      Credentials credentials =
          login(hints, username, env("UDB_LIVE_PASSWORD"), "java-sdk-perf-service-account");
      DynamicMessage who = principal(hints, credentials.access());
      Identity minted = hints.copy();
      String whoTenant = text(who, "tenant_id");
      String whoProject = text(who, "project_id");
      if (!whoTenant.isEmpty()) {
        minted.tenant = whoTenant;
      }
      if (!whoProject.isEmpty()) {
        minted.project = whoProject;
      }
      minted.bearer = credentials.access();
      serviceAccount = minted;
      serviceAccountMintedNanos = System.nanoTime();
    } catch (Exception error) {
      System.out.println(
          "[PERF-SERVICE-ACCOUNT] login as "
              + username
              + " failed; its rows run under the default identity: "
              + error);
      serviceAccount = null;
      serviceAccountFailed = true;
    }
  }

  private Outcome cdcFirstEvent(JsonObject cdc, Identity identity) throws IOException {
    JsonObject subscription = new JsonObject();
    subscription.add("context", cdc.get("context"));
    subscription.addProperty("topic_pattern", "*");
    String publish = "/" + DATA_BROKER + "/PublishCDC";
    String upsertPath = "/" + DATA_BROKER + "/Upsert";
    DynamicMessage request =
        message(services.get(DATA_BROKER).findMethodByName("PublishCDC").getInputType().getFullName(), subscription);
    JsonObject record = new JsonObject();
    record.addProperty("record_id", cdc.get("record_id").getAsString());
    record.addProperty("tenant_id", identity.tenant);
    record.addProperty("project_id", identity.project);
    record.addProperty("lookup_key", "java-perf-cdc");
    record.addProperty("payload", "java-perf-cdc");
    record.addProperty("revision", System.currentTimeMillis() * 1_000_000L);
    JsonObject upsert = new JsonObject();
    upsert.add("context", cdc.get("context"));
    upsert.add("message_type", cdc.get("message_type"));
    upsert.addProperty(
        "record_json",
        Base64.getEncoder().encodeToString(record.toString().getBytes(StandardCharsets.UTF_8)));
    upsert.add("conflict_fields", cdc.get("conflict_fields"));
    DynamicMessage write =
        message(services.get(DATA_BROKER).findMethodByName("Upsert").getInputType().getFullName(), upsert);
    Channel channel = channel(publish, identity);
    MethodDescriptor<DynamicMessage, DynamicMessage> rpc = method(publish);
    Context.CancellableContext context = Context.current().withCancellation();
    try {
      return context.call(
          () -> {
            Iterator<DynamicMessage> stream =
                ClientCalls.blockingServerStreamingCall(
                    channel,
                    rpc,
                    CallOptions.DEFAULT.withDeadlineAfter(STREAM_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                    request);
            try {
              unary(upsertPath, write, identity);
            } catch (StatusRuntimeException ignored) {
              // The stream still has to deliver an event for the seeded row.
            }
            stream.hasNext();
            return Outcome.OK;
          });
    } catch (StatusRuntimeException error) {
      return Outcome.of(error);
    } catch (Exception error) {
      return new Outcome("CLIENT_ERROR", error.toString());
    } finally {
      context.cancel(null);
    }
  }

  private record Run(double ms, Outcome outcome) {}

  private Run timeOne(JsonObject plan, JsonObject rpc) throws IOException {
    ensureFresh();
    boolean asServiceAccount =
        rpc.has("service_account")
            && rpc.get("service_account").isJsonPrimitive()
            && rpc.get("service_account").getAsBoolean();
    if (asServiceAccount) {
      ensureServiceAccount();
    }
    Identity identity =
        asServiceAccount && serviceAccount != null
            ? serviceAccount
            : rpc.get("platform").getAsBoolean() ? platform : user;
    long start = System.nanoTime();
    if ("cdc_first_event".equals(rpc.get("kind").getAsString())) {
      Outcome outcome = cdcFirstEvent(plan.getAsJsonObject("cdc"), identity);
      return new Run((System.nanoTime() - start) / 1e6, outcome);
    }
    if (!rpc.has("request")) {
      String detail = rpc.has("missing_body") ? rpc.get("missing_body").getAsString() : "no seeded body";
      return new Run(0, new Outcome("SKIP_NO_BODY", detail));
    }
    String path = rpc.get("path").getAsString();
    DynamicMessage request;
    try {
      request = message(rpc.get("input_type").getAsString(), substitute(rpc.get("request"), sentinels));
    } catch (Exception error) {
      return new Run(0, new Outcome("CLIENT_ERROR", error.toString()));
    }
    boolean streaming =
        rpc.get("client_streaming").getAsBoolean() || rpc.get("server_streaming").getAsBoolean();
    start = System.nanoTime();
    Outcome outcome;
    if (streaming) {
      outcome = firstResponse(path, request, identity);
    } else {
      try {
        unary(path, request, identity);
        outcome = Outcome.OK;
      } catch (StatusRuntimeException error) {
        outcome = Outcome.of(error);
      }
    }
    double ms = (System.nanoTime() - start) / 1e6;
    if ("FAILED_PRECONDITION".equals(outcome.code())
        && rpc.get("capability_optional").getAsBoolean()) {
      String detail = outcome.detail().toLowerCase(Locale.ROOT);
      if (detail.isEmpty()
          || detail.contains("webrtc egress")
          || detail.contains("egress is not enabled")) {
        outcome = new Outcome("CAPABILITY_SKIPPED", outcome.detail());
      }
    }
    if (Outcome.OK.equals(outcome) && rpc.has("reset_after_ok")) {
      try {
        unary(
            path,
            message(rpc.get("input_type").getAsString(), substitute(rpc.get("reset_after_ok"), sentinels)),
            user);
      } catch (Exception ignored) {
        // Best effort, as in the other harnesses.
      }
    }
    return new Run(ms, outcome);
  }

  private static double percentile(List<Double> sorted, int p) {
    return sorted.get(Math.min(sorted.size() - 1, p * (sorted.size() - 1) / 100));
  }

  private void reauthenticate(String username, String password) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    Exception last = null;
    while (System.nanoTime() < deadline) {
      try {
        Credentials fresh = login(user, username, password, "java-sdk-perf-final-purge");
        principal(user, fresh.access());
        bind(fresh);
        return;
      } catch (Exception error) {
        last = error;
        Thread.sleep(50);
      }
    }
    throw new IllegalStateException("fresh login remained revoked after tenant cutoff: " + last);
  }

  @Test
  void liveBenchmarkMeasuresEveryRpc() throws Exception {
    assumeTrue(
        "1".equals(System.getenv("UDB_LIVE_SDK_TESTS")) && "1".equals(System.getenv("UDB_LIVE_PERF")),
        "the live benchmark needs UDB_LIVE_SDK_TESTS=1 and UDB_LIVE_PERF=1");
    JsonObject plan =
        new Gson().fromJson(Files.readString(Path.of(env("UDB_BENCH_PLAN"))), JsonObject.class);
    List<String> fixtureKeys = new ArrayList<>();
    for (JsonElement key : plan.getAsJsonArray("seeded_fixture_keys")) {
      fixtureKeys.add(key.getAsString());
    }
    List<Sample> samples = new ArrayList<>();
    String tenant = plan.get("tenant_id").getAsString();
    try {
      if (plan.get("schema_version").getAsInt() != 1) {
        throw new IllegalStateException("unsupported benchmark plan schema");
      }
      loadDescriptors(plan.get("descriptor_set_b64").getAsString());
      String target = env("UDB_GRPC_TARGET");
      String authTarget =
          System.getenv("UDB_AUTH_GRPC_TARGET") == null ? target : System.getenv("UDB_AUTH_GRPC_TARGET");
      broker = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
      nativeChannel = ManagedChannelBuilder.forTarget(authTarget).usePlaintext().build();
      String username = env("UDB_LIVE_USERNAME");
      String password = env("UDB_LIVE_PASSWORD");
      user.tenant = System.getenv().getOrDefault("UDB_LIVE_TENANT", "sdk-live");
      user.project = System.getenv().getOrDefault("UDB_LIVE_PROJECT", "default");
      Credentials first = login(user, username, password, "java-sdk-perf");
      String canonical = text(principal(user, first.access()), "tenant_id");
      if (!canonical.equals(tenant) || !plan.get("project_id").getAsString().equals(user.project)) {
        throw new IllegalStateException("the benchmark plan was exported for another tenant or project");
      }
      user.tenant = canonical;
      user.bearer = first.access();
      Credentials platformLogin =
          login(user, env("UDB_LIVE_PLATFORM_USERNAME"), env("UDB_LIVE_PLATFORM_PASSWORD"), "java-sdk-perf-platform");
      DynamicMessage platformWho = principal(user, platformLogin.access());
      if (!hasRole(platformWho, "platform_admin")) {
        throw new IllegalStateException("the platform fixture did not issue platform_admin");
      }
      platform.tenant = text(platformWho, "tenant_id");
      platform.project = user.project;
      platform.bearer = platformLogin.access();
      JsonElement serviceAccountPlan = plan.get("service_account_caller");
      serviceAccountCaller =
          serviceAccountPlan != null && serviceAccountPlan.isJsonObject()
              ? serviceAccountPlan.getAsJsonObject()
              : null;
      Credentials token = login(user, username, password, "java-sdk-perf-token");
      Credentials refresh = login(user, username, password, "java-sdk-perf-refresh");
      Credentials session = login(user, username, password, "java-sdk-perf-session");
      Credentials auto = login(user, username, password, "java-sdk-perf-auto-refresh");
      sentinels.put(ACCESS, token.access());
      sentinels.put(CSRF, token.csrf());
      sentinels.put(REFRESH, refresh.refresh());
      sentinels.put(SESSION, session.session());
      autoRefresh = auto.refresh();
      autoSession = auto.session();
      refreshAtNanos =
          System.nanoTime()
              + (long) (Math.max(60, Math.min(auto.expiresIn() * 0.6, 180)) * 1e9);

      for (JsonElement element : plan.getAsJsonArray("rpcs")) {
        JsonObject rpc = element.getAsJsonObject();
        if (rpc.get("reauthenticate_before").getAsBoolean()) {
          reauthenticate(username, password);
        }
        System.out.println("[PERF-RPC] " + rpc.get("path").getAsString() + " kind=" + rpc.get("kind").getAsString());
        int iters = Math.max(1, rpc.get("iters").getAsInt());
        List<Run> runs = new ArrayList<>();
        if (rpc.get("warm_up").getAsBoolean()) {
          Run warm = timeOne(plan, rpc);
          if (!Outcome.OK.equals(warm.outcome())) {
            runs.add(warm);
          }
        }
        while (runs.size() < iters) {
          Run run = timeOne(plan, rpc);
          runs.add(run);
          if (!Outcome.OK.equals(run.outcome())) {
            break;
          }
        }
        boolean anyOk = runs.stream().anyMatch(run -> Outcome.OK.equals(run.outcome()));
        Outcome failure =
            runs.stream().map(Run::outcome).filter(o -> !Outcome.OK.equals(o)).findFirst().orElse(Outcome.OK);
        String err = failure.code();
        if (anyOk && "CAPABILITY_SKIPPED".equals(err)) {
          err = "FAILED_PRECONDITION";
        }
        List<Double> durations = new ArrayList<>();
        for (Run run : runs) {
          durations.add(run.ms());
        }
        durations.sort(Comparator.naturalOrder());
        double mean = durations.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        samples.add(
            new Sample(
                rpc.get("service").getAsString(),
                rpc.get("rpc").getAsString(),
                rpc.get("api_alias").getAsString(),
                rpc.get("operation_id").getAsString(),
                rpc.get("kind").getAsString(),
                runs.size(),
                err,
                failure.detail(),
                percentile(durations, 50),
                percentile(durations, 99),
                mean));
      }
      writeReport(samples, fixtureKeys, tenant, null);
    } catch (Exception error) {
      writeReport(samples, fixtureKeys, tenant, error.toString());
      throw error;
    } finally {
      if (broker != null) {
        broker.shutdownNow();
      }
      if (nativeChannel != null) {
        nativeChannel.shutdownNow();
      }
    }
  }

  // ── Report ───────────────────────────────────────────────────────────────

  private static String cell(String text) {
    return text.replace("\n", " ").replace("|", "\\|");
  }

  private static String ms(double value) {
    return String.format(Locale.ROOT, "%.2f", value);
  }

  private static void writeReport(
      List<Sample> samples, List<String> fixtureKeys, String tenant, String error) throws IOException {
    StringBuilder out = new StringBuilder();
    out.append("# UDB SDK Live Perf — Java (localhost)\n\n");
    out.append("RPCs measured: ").append(samples.size()).append("   tenant=").append(tenant).append("\n\n");
    out.append(
        "Every RPC is driven down its SUCCESS path: the shared seed first creates real, disposable entities"
            + " and each request's reference/ID fields are resolved to those real identifiers. The Java client"
            + " (grpc-java) sends and times every request itself. The TARGET is zero failures; any residual"
            + " non-OK RPC is listed under Failures for the maintainer to finish.\n\n");
    out.append(
        "Unary = full request/response round-trip. Non-CDC streaming RPCs (kind=stream_first_recv) report"
            + " time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (kind=cdc_first_event, PublishCDC)"
            + " reports time-to-FIRST-EVENT.\n\n");
    if (error != null) {
      out.append("## Harness error\n\n`HarnessError`: ").append(cell(error))
          .append("\n\nThis is a partial report written before the benchmark process failed.\n\n");
    }
    out.append("## Seeded fixtures\n\nCaptured semantic field -> seeded value keys used to resolve request fields: ")
        .append(String.join(", ", fixtureKeys)).append("\n\n");
    out.append("## Per-service mean latency\n\n| Service | RPCs | mean ms |\n|---|--:|--:|\n");
    Map<String, List<Double>> byService = new TreeMap<>();
    for (Sample sample : samples) {
      byService.computeIfAbsent(sample.service(), key -> new ArrayList<>()).add(sample.mean());
    }
    byService.entrySet().stream()
        .sorted((a, b) -> Double.compare(avg(b.getValue()), avg(a.getValue())))
        .forEach(entry -> out.append("| ").append(entry.getKey()).append(" | ")
            .append(entry.getValue().size()).append(" | ").append(ms(avg(entry.getValue()))).append(" |\n"));
    List<Sample> byName = new ArrayList<>(samples);
    byName.sort(Comparator.comparing(Sample::service).thenComparing(Sample::rpc));
    List<Sample> failures = byName.stream()
        .filter(s -> !"OK".equals(s.err()) && !"CAPABILITY_SKIPPED".equals(s.err())).toList();
    List<Sample> skipped = byName.stream().filter(s -> "CAPABILITY_SKIPPED".equals(s.err())).toList();
    out.append("\n## Failures (").append(failures.size()).append(")\n\n");
    if (failures.isEmpty()) {
      out.append("No RPC returned a non-OK gRPC status.\n");
    } else {
      out.append("These RPCs returned a non-OK gRPC status and are FAILURES, not latency samples.\n\n");
      out.append("| RPC | api_alias | operation_id | kind | err | detail | p99 ms | mean ms | iters |\n|---|---|---|---|---|---|--:|--:|--:|\n");
      for (Sample s : failures) {
        out.append("| ").append(s.service()).append('/').append(s.rpc()).append(" | ").append(s.apiAlias())
            .append(" | ").append(s.operationId()).append(" | ").append(s.kind()).append(" | ").append(s.err())
            .append(" | ").append(cell(s.errDetail())).append(" | ").append(ms(s.p99())).append(" | ")
            .append(ms(s.mean())).append(" | ").append(s.iters()).append(" |\n");
      }
    }
    out.append("\n## Capability Skips (").append(skipped.size()).append(")\n\n");
    if (skipped.isEmpty()) {
      out.append("No optional service capability was skipped.\n");
    } else {
      out.append("These RPCs reached the served path but require an optional backend capability disabled in this local profile.\n\n");
      out.append("| RPC | api_alias | operation_id | kind | detail |\n|---|---|---|---|---|\n");
      for (Sample s : skipped) {
        out.append("| ").append(s.service()).append('/').append(s.rpc()).append(" | ").append(s.apiAlias())
            .append(" | ").append(s.operationId()).append(" | ").append(s.kind()).append(" | ")
            .append(cell(s.errDetail())).append(" |\n");
      }
    }
    out.append("\n## Slowest 20 by p99\n\n| RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms |\n|---|---|---|---|---|--:|--:|--:|\n");
    samples.stream().sorted((a, b) -> Double.compare(b.p99(), a.p99())).limit(20).forEach(s ->
        out.append("| ").append(s.service()).append('/').append(s.rpc()).append(" | ").append(s.apiAlias())
            .append(" | ").append(s.operationId()).append(" | ").append(s.kind()).append(" | ").append(s.err())
            .append(" | ").append(ms(s.p50())).append(" | ").append(ms(s.p99())).append(" | ")
            .append(ms(s.mean())).append(" |\n"));
    out.append("\n## Full per-RPC table (sorted by service, then RPC)\n\n| Service | RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms | iters |\n|---|---|---|---|---|---|--:|--:|--:|--:|\n");
    for (Sample s : byName) {
      out.append("| ").append(s.service()).append(" | ").append(s.rpc()).append(" | ").append(s.apiAlias())
          .append(" | ").append(s.operationId()).append(" | ").append(s.kind()).append(" | ").append(s.err())
          .append(" | ").append(ms(s.p50())).append(" | ").append(ms(s.p99())).append(" | ")
          .append(ms(s.mean())).append(" | ").append(s.iters()).append(" |\n");
    }
    Path report = Path.of(System.getProperty("user.dir"), "perf_report_java.md");
    Files.writeString(report, out.toString(), StandardCharsets.UTF_8);
    System.out.println("Java perf: " + samples.size() + " RPCs measured -> " + report);
  }

  private static double avg(List<Double> values) {
    return values.stream().mapToDouble(Double::doubleValue).average().orElse(0);
  }
}
