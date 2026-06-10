package dev.udb.client;

import java.util.List;

public record UdbMetadata(
    String tenantId,
    String purpose,
    String correlationId,
    List<String> scopes,
    String serviceIdentity,
    String userId,
    String projectId,
    String clientCatalogVersion,
    String bearerToken,
    String apiKey) {
  public static final String DEFAULT_PROJECT_ID = "default";

  public UdbMetadata(
      String tenantId,
      String purpose,
      String correlationId,
      List<String> scopes,
      String serviceIdentity,
      String userId,
      String projectId,
      String clientCatalogVersion) {
    this(
        tenantId,
        purpose,
        correlationId,
        scopes,
        serviceIdentity,
        userId,
        projectId,
        clientCatalogVersion,
        "",
        "");
  }

  public UdbMetadata {
    scopes = scopes == null ? List.of() : List.copyOf(scopes);
    userId = userId == null ? "" : userId;
    projectId = projectId == null || projectId.isBlank() ? DEFAULT_PROJECT_ID : projectId;
    bearerToken = bearerToken == null ? "" : bearerToken;
    apiKey = apiKey == null ? "" : apiKey;
    clientCatalogVersion =
        clientCatalogVersion == null || clientCatalogVersion.isBlank()
            ? UdbClient.PROTOCOL_VERSION
            : clientCatalogVersion;
  }
}
