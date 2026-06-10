package dev.udb.client;

import java.util.List;

/**
 * Connection + identity configuration for {@link UdbProject}. Built via the
 * {@link #builder()} fluent builder. {@code target} is the data-plane broker;
 * {@code authTarget} defaults to it when left blank, and {@code webrtcTarget}
 * defaults to the (resolved) {@code authTarget} — the native control-plane
 * listener that serves the WebRTC services — so a single endpoint deployment
 * needs only {@code target}.
 */
public final class UdbProjectConfig {
  private final String target;
  private final String authTarget;
  private final String webrtcTarget;
  private final String tenantId;
  private final String projectId;
  private final String purpose;
  private final String correlationId;
  private final String serviceIdentity;
  private final String userId;
  private final List<String> scopes;
  private final String clientCatalogVersion;
  private final String bearerToken;
  private final String apiKey;
  private final boolean tls;

  private UdbProjectConfig(Builder b) {
    this.target = b.target;
    this.authTarget = b.authTarget == null || b.authTarget.isBlank() ? b.target : b.authTarget;
    this.webrtcTarget =
        b.webrtcTarget == null || b.webrtcTarget.isBlank() ? this.authTarget : b.webrtcTarget;
    this.tenantId = b.tenantId;
    this.projectId = b.projectId;
    this.purpose = b.purpose;
    this.correlationId = b.correlationId;
    this.serviceIdentity = b.serviceIdentity;
    this.userId = b.userId;
    this.scopes = b.scopes == null ? List.of() : List.copyOf(b.scopes);
    this.clientCatalogVersion = b.clientCatalogVersion;
    this.bearerToken = b.bearerToken;
    this.apiKey = b.apiKey;
    this.tls = b.tls;
  }

  public String target() {
    return target;
  }

  public String authTarget() {
    return authTarget;
  }

  public String webrtcTarget() {
    return webrtcTarget;
  }

  public boolean tls() {
    return tls;
  }

  /** Build the shared {@link UdbMetadata} identity context for all services. */
  public UdbMetadata metadata() {
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
        apiKey);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Fluent builder for {@link UdbProjectConfig}. */
  public static final class Builder {
    private String target = "localhost:50051";
    private String authTarget;
    private String webrtcTarget;
    private String tenantId = "";
    private String projectId = UdbMetadata.DEFAULT_PROJECT_ID;
    private String purpose = "";
    private String correlationId = "";
    private String serviceIdentity = "";
    private String userId = "";
    private List<String> scopes;
    private String clientCatalogVersion = "";
    private String bearerToken = "";
    private String apiKey = "";
    private boolean tls = false;

    public Builder target(String v) {
      this.target = v;
      return this;
    }

    public Builder authTarget(String v) {
      this.authTarget = v;
      return this;
    }

    public Builder webrtcTarget(String v) {
      this.webrtcTarget = v;
      return this;
    }

    public Builder tenantId(String v) {
      this.tenantId = v;
      return this;
    }

    public Builder projectId(String v) {
      this.projectId = v;
      return this;
    }

    public Builder purpose(String v) {
      this.purpose = v;
      return this;
    }

    public Builder correlationId(String v) {
      this.correlationId = v;
      return this;
    }

    public Builder serviceIdentity(String v) {
      this.serviceIdentity = v;
      return this;
    }

    public Builder userId(String v) {
      this.userId = v;
      return this;
    }

    public Builder scopes(List<String> v) {
      this.scopes = v;
      return this;
    }

    public Builder clientCatalogVersion(String v) {
      this.clientCatalogVersion = v;
      return this;
    }

    public Builder bearerToken(String v) {
      this.bearerToken = v;
      return this;
    }

    public Builder apiKey(String v) {
      this.apiKey = v;
      return this;
    }

    /** Use TLS for created channels (default plaintext). */
    public Builder tls(boolean v) {
      this.tls = v;
      return this;
    }

    public UdbProjectConfig build() {
      return new UdbProjectConfig(this);
    }
  }
}
