package dev.udb.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.udb.entity.v1.ErrorDetail;
import com.udb.entity.v1.ErrorFieldViolation;
import com.udb.entity.v1.ErrorKind;
import dev.udb.client.generated.GeneratedClientSupport;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class UdbRpcExceptionTest {
  @Test
  void decodesErrorDetailTrailerFieldViolationsAndTypedAccessors() {
    ErrorDetail detail =
        ErrorDetail.newBuilder()
            .setRetryable(false)
            .setRetryAfterMs(0)
            .setKind(ErrorKind.ERROR_KIND_VALIDATION)
            .addFieldViolations(
                ErrorFieldViolation.newBuilder()
                    .setField("email")
                    .setDescription("must be a valid email")
                    .build())
            .build();
    Metadata trailers = new Metadata();
    trailers.put(GeneratedClientSupport.ERROR_DETAIL_KEY, detail.toByteArray());

    GeneratedClientSupport.UdbRpcException ex =
        GeneratedClientSupport.mapError(
            "/svc/DoThing",
            Status.INVALID_ARGUMENT.withDescription("validation failed").asRuntimeException(trailers));

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.code());
    assertArrayEquals(detail.toByteArray(), ex.errorDetail());
    assertNotNull(ex.decodedErrorDetail());
    assertFalse(ex.retryable());
    assertEquals(0, ex.retryAfterMs());
    assertEquals(ErrorKind.ERROR_KIND_VALIDATION, ex.kind());
    assertEquals(
        List.of(Map.of("field", "email", "description", "must be a valid email")),
        ex.fieldViolations());
  }

  @Test
  void decodesQuotaErrorDetailRetryBackoff() {
    ErrorDetail detail =
        ErrorDetail.newBuilder()
            .setBackend("admission")
            .setOperation("tenant budget")
            .setRetryable(true)
            .setRetryAfterMs(250)
            .setKind(ErrorKind.ERROR_KIND_QUOTA)
            .build();
    Metadata trailers = new Metadata();
    trailers.put(GeneratedClientSupport.ERROR_DETAIL_KEY, detail.toByteArray());

    GeneratedClientSupport.UdbRpcException ex =
        GeneratedClientSupport.mapError(
            "/svc/DoThing",
            Status.RESOURCE_EXHAUSTED.withDescription("quota").asRuntimeException(trailers));

    assertTrue(ex.retryable());
    assertEquals(250, ex.retryAfterMs());
    assertEquals(ErrorKind.ERROR_KIND_QUOTA, ex.kind());
    assertEquals(List.of(), ex.fieldViolations());
  }

  @Test
  void synthesizesTransportErrorDetailWhenTrailerIsAbsent() {
    GeneratedClientSupport.UdbRpcException ex =
        GeneratedClientSupport.mapError(
            "/svc/DoThing",
            Status.DEADLINE_EXCEEDED.withDescription("deadline").asRuntimeException());

    assertNull(ex.errorDetail());
    assertNotNull(ex.decodedErrorDetail());
    assertTrue(ex.retryable());
    assertEquals("transport", ex.decodedErrorDetail().getBackend());
    assertEquals("deadline_exceeded", ex.decodedErrorDetail().getOperation());
    assertEquals(0, ex.retryAfterMs());
    assertEquals(ErrorKind.ERROR_KIND_RETRYABLE, ex.kind());
    assertEquals(List.of(), ex.fieldViolations());
  }

  @Test
  void synthesizesCancelledTransportErrorDetailAsNotRetryable() {
    GeneratedClientSupport.UdbRpcException ex =
        GeneratedClientSupport.mapError(
            "/svc/DoThing", Status.CANCELLED.withDescription("cancelled").asRuntimeException());

    assertNull(ex.errorDetail());
    assertNotNull(ex.decodedErrorDetail());
    assertFalse(ex.retryable());
    assertEquals("transport", ex.decodedErrorDetail().getBackend());
    assertEquals("cancelled", ex.decodedErrorDetail().getOperation());
    assertEquals(0, ex.retryAfterMs());
    assertEquals(ErrorKind.ERROR_KIND_RETRYABLE, ex.kind());
    assertEquals(List.of(), ex.fieldViolations());
  }
}
