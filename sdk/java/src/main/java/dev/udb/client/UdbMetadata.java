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
    String clientCatalogVersion) {
  public static final String DEFAULT_PROJECT_ID = "default";

  public UdbMetadata {
    scopes = scopes == null ? List.of() : List.copyOf(scopes);
    userId = userId == null ? "" : userId;
    projectId = projectId == null || projectId.isBlank() ? DEFAULT_PROJECT_ID : projectId;
    clientCatalogVersion =
        clientCatalogVersion == null || clientCatalogVersion.isBlank()
            ? UdbClient.PROTOCOL_VERSION
            : clientCatalogVersion;
  }
}
