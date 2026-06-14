package dev.udb.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;

/**
 * Factory for the long-lived UDB gRPC {@link ManagedChannel}.
 *
 * <p>Construct a channel once and reuse it across every RPC: a fresh channel forces a
 * TCP+TLS+HTTP/2 handshake on each call, which dominates per-RPC latency. The defaults
 * here keep an otherwise idle connection warm (HTTP/2 keepalive) so it does not drop to
 * IDLE and re-handshake. Retry policy belongs in the generated wrapper, where
 * proto-derived operation_kind is known.
 */
public final class UdbChannels {

  private UdbChannels() {}

  // Keepalive: ping an idle connection every 30s, give the ack 10s, and permit pings
  // even with no active RPC so a fully idle channel stays warm.
  private static final long KEEPALIVE_TIME_SECONDS = 30L;
  private static final long KEEPALIVE_TIMEOUT_SECONDS = 10L;

  /** Apply UDB's keepalive tuning to an existing channel builder. */
  public static ManagedChannelBuilder<?> tune(ManagedChannelBuilder<?> builder) {
    return builder
        .keepAliveTime(KEEPALIVE_TIME_SECONDS, TimeUnit.SECONDS)
        .keepAliveTimeout(KEEPALIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .keepAliveWithoutCalls(true);
  }

  /**
   * Build a long-lived channel to {@code target} with UDB's default keepalive options.
   * Reuse the returned channel across all RPCs.
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
