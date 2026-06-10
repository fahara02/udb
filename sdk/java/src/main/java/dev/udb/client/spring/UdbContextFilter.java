package dev.udb.client.spring;

import dev.udb.client.UdbMetadata;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

/**
 * Spring Boot servlet {@link Filter} that lifts UDB request context out of the
 * inbound HTTP request (tenant / user / correlation / request id / scopes) and
 * publishes it as a request-scoped {@link UdbMetadata} on the {@link
 * ServletRequest} attributes, so downstream controllers/services can build a
 * tenant-scoped {@code UdbProject} (or attach the same identity headers to a
 * {@code UdbAuthClient}) without re-parsing headers.
 *
 * <p>This adapter lives in the optional {@code dev.udb.client.spring} package and
 * depends on {@code jakarta.servlet} (and, when wired as a {@code @Component},
 * Spring) which are declared <b>provided/optional</b>: the UDB SDK core compiles
 * and runs without the servlet API on the classpath. Register it from a Spring
 * Boot app via a {@code FilterRegistrationBean<UdbContextFilter>} or by annotating
 * a subclass with {@code @Component}.
 *
 * <p>Header names mirror the gRPC metadata convention used by {@code UdbClient}:
 * {@code x-tenant-id}, {@code x-user-id}, {@code x-purpose}, {@code
 * x-correlation-id}, {@code x-scopes}, {@code x-service-identity}, {@code
 * x-udb-project-id}, {@code x-udb-client-catalog-version}, {@code authorization},
 * and {@code x-api-key}. A missing/blank {@code x-correlation-id} (or {@code
 * x-request-id}) is synthesized as a UUID and echoed back on the response so the
 * trace id survives the hop.
 */
public class UdbContextFilter implements Filter {

  /** Request attribute under which the resolved {@link UdbMetadata} is stored. */
  public static final String METADATA_ATTRIBUTE = "udb.metadata";

  /** Request attribute under which the resolved correlation/request id is stored. */
  public static final String REQUEST_ID_ATTRIBUTE = "udb.requestId";

  // gRPC-parity header names (see UdbClient).
  public static final String H_TENANT_ID = "x-tenant-id";
  public static final String H_USER_ID = "x-user-id";
  public static final String H_PURPOSE = "x-purpose";
  public static final String H_CORRELATION_ID = "x-correlation-id";
  public static final String H_REQUEST_ID = "x-request-id";
  public static final String H_SCOPES = "x-scopes";
  public static final String H_SERVICE_IDENTITY = "x-service-identity";
  public static final String H_PROJECT_ID = "x-udb-project-id";
  public static final String H_CLIENT_CATALOG_VERSION = "x-udb-client-catalog-version";
  public static final String H_AUTHORIZATION = "authorization";
  public static final String H_API_KEY = "x-api-key";

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    if (request instanceof HttpServletRequest http) {
      UdbMetadata metadata = extract(http::getHeader);
      String requestId = firstNonBlank(http.getHeader(H_CORRELATION_ID), http.getHeader(H_REQUEST_ID));
      if (requestId == null || requestId.isBlank()) {
        requestId = UUID.randomUUID().toString();
      }
      request.setAttribute(METADATA_ATTRIBUTE, metadata);
      request.setAttribute(REQUEST_ID_ATTRIBUTE, requestId);
      if (response instanceof HttpServletResponse httpResp) {
        httpResp.setHeader(H_CORRELATION_ID, requestId);
      }
    }
    chain.doFilter(request, response);
  }

  /**
   * Build a {@link UdbMetadata} from a header lookup function. Exposed (and pure)
   * so it can be unit-tested or reused outside the servlet container.
   */
  public static UdbMetadata extract(Function<String, String> header) {
    String correlationId =
        firstNonBlank(header.apply(H_CORRELATION_ID), header.apply(H_REQUEST_ID));
    if (correlationId == null || correlationId.isBlank()) {
      correlationId = UUID.randomUUID().toString();
    }
    return new UdbMetadata(
        nullToEmpty(header.apply(H_TENANT_ID)),
        nullToEmpty(header.apply(H_PURPOSE)),
        correlationId,
        parseScopes(header.apply(H_SCOPES)),
        nullToEmpty(header.apply(H_SERVICE_IDENTITY)),
        nullToEmpty(header.apply(H_USER_ID)),
        header.apply(H_PROJECT_ID), // UdbMetadata defaults blank -> "default"
        nullToEmpty(header.apply(H_CLIENT_CATALOG_VERSION)),
        bearerToken(header.apply(H_AUTHORIZATION)),
        nullToEmpty(header.apply(H_API_KEY)));
  }

  /** Read the request-scoped {@link UdbMetadata} an earlier filter pass published. */
  public static UdbMetadata metadataOf(ServletRequest request) {
    Object value = request.getAttribute(METADATA_ATTRIBUTE);
    return value instanceof UdbMetadata md ? md : null;
  }

  private static List<String> parseScopes(String raw) {
    if (raw == null || raw.isBlank()) {
      return List.of();
    }
    return Arrays.stream(raw.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toList();
  }

  private static String nullToEmpty(String s) {
    return s == null ? "" : s;
  }

  private static String bearerToken(String authorization) {
    if (authorization == null) {
      return "";
    }
    String trimmed = authorization.trim();
    return trimmed.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length())
        ? trimmed.substring("Bearer ".length()).trim()
        : "";
  }

  private static String firstNonBlank(String a, String b) {
    if (a != null && !a.isBlank()) {
      return a;
    }
    return b;
  }
}
