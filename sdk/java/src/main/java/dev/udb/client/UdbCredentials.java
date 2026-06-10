package dev.udb.client;

/**
 * Mutable, thread-safe holder for the per-call auth credentials (bearer token
 * and API key). A single instance is shared across all of a {@link UdbProject}'s
 * stubs through the dynamic header interceptor built by
 * {@link UdbClient#credentialInterceptor(UdbMetadata, UdbCredentials)}, so a
 * refreshed token reaches every outbound call without rebuilding channels or
 * stubs (mirrors the TypeScript {@code core.setCredentials} hot-swap).
 */
public final class UdbCredentials {
  private volatile String bearerToken;
  private volatile String apiKey;

  public UdbCredentials(String bearerToken, String apiKey) {
    this.bearerToken = bearerToken == null ? "" : bearerToken;
    this.apiKey = apiKey == null ? "" : apiKey;
  }

  /** Seed a holder from a metadata record's static credentials. */
  static UdbCredentials fromMetadata(UdbMetadata metadata) {
    return new UdbCredentials(metadata.bearerToken(), metadata.apiKey());
  }

  public String bearerToken() {
    return bearerToken;
  }

  public String apiKey() {
    return apiKey;
  }

  /** Replace the bearer token, keeping the API key. */
  public void setBearerToken(String bearerToken) {
    this.bearerToken = bearerToken == null ? "" : bearerToken;
  }

  /** Replace the API key, keeping the bearer token. */
  public void setApiKey(String apiKey) {
    this.apiKey = apiKey == null ? "" : apiKey;
  }

  /** Replace both bearer token and API key. */
  public void set(String bearerToken, String apiKey) {
    setBearerToken(bearerToken);
    setApiKey(apiKey);
  }
}
