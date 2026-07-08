package dev.udb.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.udb.entity.v1.CacheSetRequest;
import com.udb.entity.v1.SelectRequest;
import dev.udb.client.generated.GeneratedClientSupport;
import io.grpc.Status;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

final class UdbGeneratedRetryTest {
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
