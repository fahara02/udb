package dev.udb.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.udb.core.authn.services.v1.ExchangeTokenForJobRequest;
import com.udb.core.authn.services.v1.ExchangeTokenForJobResponse;
import com.udb.entity.v1.ErrorDetail;
import com.udb.entity.v1.ErrorKind;
import dev.udb.client.generated.GeneratedClientSupport;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Delegated job tokens without a broker: the request {@link
 * UdbAuthClient#exchangeTokenForJob} sends, the token it returns, the local
 * refusals, and the token-refusal reason helpers.
 */
final class DelegatedJobTokenTest {

  private static StatusRuntimeException refusal(String reason) {
    Metadata trailers = new Metadata();
    trailers.put(
        GeneratedClientSupport.ERROR_DETAIL_KEY,
        ErrorDetail.newBuilder()
            .setReason(reason)
            .setKind(ErrorKind.ERROR_KIND_POLICY)
            .build()
            .toByteArray());
    return Status.UNAUTHENTICATED.withDescription("refused").asRuntimeException(trailers);
  }

  @Test
  void requestCarriesTheSubjectTokenPurposeJobAndTtl() {
    ExchangeTokenForJobRequest request =
        UdbAuthClient.jobTokenRequest("user.jwt", "export", "export-42", 600);
    assertEquals("user.jwt", request.getSubjectToken());
    assertEquals("export", request.getPurpose());
    assertEquals("export-42", request.getJobId());
    assertEquals(600, request.getTtlSeconds());
    assertEquals(0, UdbAuthClient.jobTokenRequest("t", "export", "j", 0).getTtlSeconds());
  }

  @Test
  void blankArgumentsAndNegativeTtlAreRefusedLocally() {
    assertThrows(IllegalArgumentException.class, () -> UdbAuthClient.jobTokenRequest(" ", "export", "j", 0));
    assertThrows(IllegalArgumentException.class, () -> UdbAuthClient.jobTokenRequest(null, "export", "j", 0));
    assertThrows(IllegalArgumentException.class, () -> UdbAuthClient.jobTokenRequest("t", "", "j", 0));
    assertThrows(IllegalArgumentException.class, () -> UdbAuthClient.jobTokenRequest("t", "export", "", 0));
    assertThrows(IllegalArgumentException.class, () -> UdbAuthClient.jobTokenRequest("t", "export", "j", -1));
  }

  @Test
  void responseMapsToTheDelegatedJobToken() {
    ExchangeTokenForJobResponse response =
        ExchangeTokenForJobResponse.newBuilder()
            .setAccessToken("delegated.jwt")
            .setJti("dlg_1")
            .setExpiresAtUnix(1_900_000_000L)
            .setSubject("user-1")
            .setActor("spiffe://acme/exporter")
            .setPurpose("export")
            .setJobId("export-42")
            .addScopes("udb:read")
            .setTenantId("tenant-1")
            .setProjectId("billing")
            .build();
    assertEquals(
        new DelegatedJobToken(
            "delegated.jwt",
            "dlg_1",
            1_900_000_000L,
            "user-1",
            "spiffe://acme/exporter",
            "export",
            "export-42",
            List.of("udb:read"),
            "tenant-1",
            "billing"),
        UdbAuthClient.delegatedJobToken(response, "export-42"));
    assertThrows(
        IllegalStateException.class,
        () -> UdbAuthClient.delegatedJobToken(ExchangeTokenForJobResponse.getDefaultInstance(), "export-42"));
  }

  @Test
  void expiredAndRevokedBearersAreToldApart() {
    StatusRuntimeException expired = refusal("UDB_TOKEN_EXPIRED");
    StatusRuntimeException revoked = refusal("UDB_TOKEN_REVOKED");
    assertEquals("UDB_TOKEN_EXPIRED", UdbAuthClient.errorReason(expired));
    assertTrue(UdbAuthClient.isTokenExpired(expired));
    assertFalse(UdbAuthClient.isTokenRevoked(expired));
    assertTrue(UdbAuthClient.isTokenRevoked(revoked));
    assertFalse(UdbAuthClient.isTokenExpired(revoked));
    // Found as a cause, too.
    assertTrue(UdbAuthClient.isTokenExpired(new RuntimeException("wrapped", expired)));
    assertFalse(UdbAuthClient.isTokenExpired(refusal("UDB_AUTHENTICATION_FAILED")));
    assertFalse(
        UdbAuthClient.isTokenExpired(new RuntimeException("UDB_TOKEN_EXPIRED")), "text is never matched");
    assertFalse(UdbAuthClient.isTokenRevoked(null));
    assertFalse(UdbAuthClient.isTokenExpired(Status.UNAUTHENTICATED.asRuntimeException()));
  }
}
