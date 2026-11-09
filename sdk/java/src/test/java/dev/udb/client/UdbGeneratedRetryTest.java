package dev.udb.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.udb.entity.v1.CacheSetRequest;
import com.udb.entity.v1.ErrorDetail;
import com.udb.entity.v1.ErrorKind;
import com.udb.entity.v1.SelectRequest;
import dev.udb.client.generated.GeneratedClientSupport;
import dev.udb.client.generated.GeneratedClientSupport.CallTuning;
import dev.udb.client.generated.GeneratedClientSupport.UdbRpcException;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.junit.jupiter.api.Test;

final class UdbGeneratedRetryTest {
  private static final MethodDescriptor<ErrorDetail, ErrorDetail> PROBE_READ =
      MethodDescriptor.<ErrorDetail, ErrorDetail>newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(MethodDescriptor.generateFullMethodName("udb.test.v1.Probe", "Read"))
          .setRequestMarshaller(ProtoUtils.marshaller(ErrorDetail.getDefaultInstance()))
          .setResponseMarshaller(ProtoUtils.marshaller(ErrorDetail.getDefaultInstance()))
          .build();

  @Test
  void serverRetryAfterIsHonouredBeforeTheNextAttempt() {
    // Repeated: one lucky jitter draw must not be what makes this pass.
    for (int run = 0; run < 3; run++) {
      List<Duration> waits = new ArrayList<>();
      ScriptedChannel channel = new ScriptedChannel(throttled(1200), null);
      CallTuning tuning = CallTuning.builder().sleeper(waits::add).build();

      ErrorDetail response = call(channel, tuning, null);

      assertEquals("ok", response.getBackend());
      assertEquals(2, channel.calls);
      assertEquals(1, waits.size());
      assertTrue(
          waits.get(0).toMillis() >= 1200,
          "run " + run + ": waited " + waits.get(0).toMillis() + "ms, broker asked for 1200ms");
    }
  }

  @Test
  void serverRetryAfterPastTheDeadlineThrowsWithoutSleeping() {
    List<Duration> waits = new ArrayList<>();
    ScriptedChannel channel = new ScriptedChannel(throttled(1200), null);
    CallTuning tuning = CallTuning.builder().sleeper(waits::add).build();

    UdbRpcException ex =
        assertThrows(
            UdbRpcException.class, () -> call(channel, tuning, Duration.ofMillis(500)));

    assertEquals(Status.Code.RESOURCE_EXHAUSTED, ex.code());
    assertEquals(1200L, ex.retryAfterMs());
    assertEquals(1, channel.calls);
    assertTrue(waits.isEmpty());
  }

  @Test
  void serverRetryAfterAboveTheCapThrowsWithoutSleeping() {
    // An explicit cap, then the default (10s) cap.
    long[][] cases = {{1500, 5000}, {-1, 60000}};
    for (long[] c : cases) {
      List<Duration> waits = new ArrayList<>();
      ScriptedChannel channel = new ScriptedChannel(throttled(c[1]), null);
      CallTuning.Builder builder =
          CallTuning.builder().initialBackoff(Duration.ofMillis(1)).sleeper(waits::add);
      if (c[0] > 0) {
        builder.maxRetryAfter(Duration.ofMillis(c[0]));
      }
      CallTuning tuning = builder.build();

      UdbRpcException ex =
          assertThrows(UdbRpcException.class, () -> call(channel, tuning, null));

      assertEquals(Status.Code.RESOURCE_EXHAUSTED, ex.code());
      assertEquals(c[1], ex.retryAfterMs());
      assertEquals(1, channel.calls);
      assertTrue(waits.isEmpty());
    }
  }

  @Test
  void maxRetryAfterDefaultsToTenSeconds() {
    assertEquals(Duration.ofSeconds(10), GeneratedClientSupport.DEFAULT.maxRetryAfter);
    assertEquals(Duration.ofSeconds(10), GeneratedClientSupport.DEFAULT_MAX_RETRY_AFTER);
  }

  private static ErrorDetail call(ScriptedChannel channel, CallTuning tuning, Duration deadline) {
    return GeneratedClientSupport.unary(
        channel,
        PROBE_READ,
        ErrorDetail.getDefaultInstance(),
        tuning,
        deadline,
        Metadata::new,
        true,
        false);
  }

  /** A RESOURCE_EXHAUSTED refusal whose ErrorDetail asks for {@code retryAfterMs}. */
  private static Long throttled(long retryAfterMs) {
    return retryAfterMs;
  }

  /**
   * Answers each unary call from a script: a null entry answers OK; a number
   * fails with RESOURCE_EXHAUSTED plus an ErrorDetail trailer carrying that
   * retry_after_ms.
   */
  private static final class ScriptedChannel extends Channel {
    private final Deque<Long> script = new ArrayDeque<>();
    int calls;

    ScriptedChannel(Long... outcomes) {
      for (Long outcome : outcomes) {
        // ArrayDeque refuses null, so OK is encoded as a negative delay.
        script.addLast(outcome == null ? -1L : outcome);
      }
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
        MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
      return new ClientCall<>() {
        private Listener<RespT> listener;

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          listener = responseListener;
        }

        @Override
        public void request(int numMessages) {}

        @Override
        public void cancel(String message, Throwable cause) {}

        @Override
        @SuppressWarnings("unchecked")
        public void halfClose() {
          calls++;
          long retryAfterMs = script.removeFirst();
          if (retryAfterMs < 0) {
            listener.onMessage((RespT) ErrorDetail.newBuilder().setBackend("ok").build());
            listener.onClose(Status.OK, new Metadata());
            return;
          }
          ErrorDetail detail =
              ErrorDetail.newBuilder()
                  .setBackend("admission")
                  .setOperation("rate limit")
                  .setRetryable(true)
                  .setRetryAfterMs(retryAfterMs)
                  .setKind(ErrorKind.ERROR_KIND_QUOTA)
                  .build();
          Metadata trailers = new Metadata();
          trailers.put(GeneratedClientSupport.ERROR_DETAIL_KEY, detail.toByteArray());
          listener.onClose(Status.RESOURCE_EXHAUSTED.withDescription("slow down"), trailers);
        }

        @Override
        public void sendMessage(ReqT message) {}
      };
    }

    @Override
    public String authority() {
      return "localhost";
    }
  }

  @Test
  void replaySafeMutationWithKeyRetries() throws Exception {
    assertTrue(isRetryable(Status.Code.UNAVAILABLE, false, true, true));
    assertTrue(isRetryable(Status.Code.RESOURCE_EXHAUSTED, false, true, true));
  }

  @Test
  void replaySafeMutationWithoutKeyDoesNotRetry() throws Exception {
    assertFalse(isRetryable(Status.Code.UNAVAILABLE, false, true, false));
  }

  @Test
  void nonReplaySafeMutationWithKeyDoesNotRetry() throws Exception {
    assertFalse(isRetryable(Status.Code.UNAVAILABLE, false, false, true));
  }

  @Test
  void deadlineExceededMutationDoesNotRetry() throws Exception {
    assertFalse(isRetryable(Status.Code.DEADLINE_EXCEEDED, false, true, true));
  }

  @Test
  void readOnlyRetryBehaviorIsUnchanged() throws Exception {
    assertTrue(isRetryable(Status.Code.DEADLINE_EXCEEDED, true, false, false));
    assertTrue(isRetryable(Status.Code.UNAVAILABLE, true, false, false));
  }

  @Test
  void detectsNonEmptyIdempotencyKeyOnly() throws Exception {
    assertTrue(
        hasIdempotencyKey(CacheSetRequest.newBuilder().setIdempotencyKey("idem-1").build()));
    assertFalse(
        hasIdempotencyKey(CacheSetRequest.newBuilder().setIdempotencyKey("   ").build()));
    assertFalse(hasIdempotencyKey(SelectRequest.getDefaultInstance()));
    assertFalse(hasIdempotencyKey(null));
  }

  @Test
  void blankIdempotencyKeyDoesNotSatisfyMutationRetry() throws Exception {
    boolean hasKey =
        hasIdempotencyKey(CacheSetRequest.newBuilder().setIdempotencyKey("   ").build());
    assertFalse(hasKey);
    assertFalse(isRetryable(Status.Code.UNAVAILABLE, false, true, hasKey));
  }

  private static boolean isRetryable(
      Status.Code code, boolean readOnly, boolean replaySafe, boolean hasIdempotencyKey)
      throws Exception {
    Method method =
        GeneratedClientSupport.class.getDeclaredMethod(
            "isRetryable", Status.Code.class, boolean.class, boolean.class, boolean.class);
    method.setAccessible(true);
    return (boolean) method.invoke(null, code, readOnly, replaySafe, hasIdempotencyKey);
  }

  private static boolean hasIdempotencyKey(Object request) throws Exception {
    Method method = GeneratedClientSupport.class.getDeclaredMethod("hasIdempotencyKey", Object.class);
    method.setAccessible(true);
    return (boolean) method.invoke(null, new Object[] {request});
  }
}
