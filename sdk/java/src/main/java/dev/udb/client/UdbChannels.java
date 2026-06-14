package dev.udb.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Factory for the long-lived UDB gRPC {@link ManagedChannel}.
 *
 * <p>Construct a channel once and reuse it across every RPC: a fresh channel forces a
 * TCP+TLS+HTTP/2 handshake on each call, which dominates per-RPC latency. The defaults
 * here keep an otherwise idle connection warm (HTTP/2 keepalive) so it does not drop to
 * IDLE and re-handshake, and attach a native gRPC service-config retry on
 * {@code UNAVAILABLE} (jittered exponential backoff, bounded by the call deadline).
 */
public final class UdbChannels {

  private UdbChannels() {}

  // Keepalive: ping an idle connection every 30s, give the ack 10s, and permit pings
  // even with no active RPC so a fully idle channel stays warm.
  private static final long KEEPALIVE_TIME_SECONDS = 30L;
  private static final long KEEPALIVE_TIMEOUT_SECONDS = 10L;

  /**
   * UDB's default gRPC service-config retry policy: 4 attempts, 0.1s initial backoff,
   * 2s ceiling, x2 multiplier, only on {@code UNAVAILABLE}. gRPC applies jitter and
   * bounds the sequence by the call deadline.
   */
  public static Map<String, Object> defaultServiceConfig() {
    Map<String, Object> retryPolicy =
        Map.of(
            "maxAttempts", 4D,
            "initialBackoff", "0.1s",
            "maxBackoff", "2s",
            "backoffMultiplier", 2D,
            "retryableStatusCodes", List.of("UNAVAILABLE"));
    Map<String, Object> methodConfig =
        Map.of(
            // Empty "name" with an empty selector applies the policy to every method.
            "name", List.of(Map.of()),
            "retryPolicy", retryPolicy);
    return Map.of("methodConfig", List.of(methodConfig));
  }

  /** Apply UDB's keepalive + retry tuning to an existing channel builder. */
  public static ManagedChannelBuilder<?> tune(ManagedChannelBuilder<?> builder) {
    return builder
        .keepAliveTime(KEEPALIVE_TIME_SECONDS, TimeUnit.SECONDS)
        .keepAliveTimeout(KEEPALIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .keepAliveWithoutCalls(true)
        .enableRetry()
        .defaultServiceConfig(defaultServiceConfig());
  }

  /**
   * Build a long-lived channel to {@code target} with UDB's default keepalive + retry
   * options. Reuse the returned channel across all RPCs.
   *
   * @param tls when true use transport security; otherwise plaintext (loopback/dev).
   */
  public static ManagedChannel forTarget(String target, boolean tls) {
    ManagedChannelBuilder<?> b = tune(ManagedChannelBuilder.forTarget(target));
    if (tls) {
      b.useTransportSecurity();
    } else {
      b.usePlaintext();
    }
    return b.build();
  }
}
