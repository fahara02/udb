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
    String apiKey,
    String consistency,
    boolean primaryRead,
    long maxReplicaLagMs,
    boolean eventualConsistencyAllowed,
    String readFenceJson) {
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
        "",
        "",
        false,
        0,
        false,
        "");
  }

  public UdbMetadata(
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
    this(
        tenantId,
        purpose,
        correlationId,
        scopes,
        serviceIdentity,
        userId,
        projectId,
        clientCatalogVersion,
        bearerToken,
        apiKey,
        "",
        false,
        0,
        false,
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
    consistency = consistency == null ? "" : consistency;
    maxReplicaLagMs = Math.max(0, maxReplicaLagMs);
    readFenceJson = readFenceJson == null ? "" : readFenceJson;
  }

  public UdbMetadata withReadFence(String readFenceJson) {
    return new UdbMetadata(
        tenantId,
        purpose,
        correlationId,
        scopes,
        serviceIdentity,
        userId,
        projectId,
        clientCatalogVersion,
        bearerToken,
        apiKey,
        consistency,
        primaryRead,
        maxReplicaLagMs,
        eventualConsistencyAllowed,
        readFenceJson);
  }

  public UdbMetadata afterWrite(WriteReceipt receipt) {
    return afterWrite(receipt, ReadFence.DEFAULT_MAX_WAIT_MS);
  }

  public UdbMetadata afterWrite(WriteReceipt receipt, long maxWaitMs) {
    ReadFence fence = ReadFence.fromReceipt(receipt, maxWaitMs);
    return withReadFence(fence.toJson());
  }
}
