package dev.udb.client;

import java.util.List;

/**
 * A delegated job token ({@code AuthnService.ExchangeTokenForJob}): it acts for
 * {@code subject} (the user) as {@code actor} (the service's identity).
 *
 * @param accessToken the token the job sends as its bearer
 * @param jti the token's id; revoking it stops the token
 * @param expiresAtUnix when the token expires, in Unix seconds
 * @param subject the user the token acts for
 * @param actor the acting service's identity (the RFC 8693 {@code act} claim)
 * @param purpose why the job acts for the user, as signed into the token
 * @param jobId the job the token is for
 * @param scopes the scopes both the user's token and the service's grant allow
 * @param tenantId the tenant the token is for
 * @param projectId the project the token is for
 */
public record DelegatedJobToken(
    String accessToken,
    String jti,
    long expiresAtUnix,
    String subject,
    String actor,
    String purpose,
    String jobId,
    List<String> scopes,
    String tenantId,
    String projectId) {

  public DelegatedJobToken {
    scopes = scopes == null ? List.of() : List.copyOf(scopes);
  }
}
