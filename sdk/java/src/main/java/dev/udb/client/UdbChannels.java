package dev.udb.client;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ChannelCredentials;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Factory for the long-lived UDB gRPC {@link ManagedChannel}.
 *
 * <p>Construct a channel once and reuse it across every RPC: a fresh channel forces a
 * TCP+TLS+HTTP/2 handshake on each call, which dominates per-RPC latency. The defaults
 * here keep an otherwise idle connection warm (HTTP/2 keepalive) so it does not drop to
 * IDLE and re-handshake. Retry policy belongs in the generated wrapper, where
 * proto-derived operation_kind is known.
 *
 * <p>Recovery after a broker restart: grpc-java's reconnect backoff grows to two minutes and its
 * builder offers no public option to cap it, so a channel that failed a few dials while the broker
 * was down could keep failing calls with UNAVAILABLE long after the broker was back. Every channel
 * built here therefore resets its connect backoff when a call fails with UNAVAILABLE (at most once
 * per {@link #RECONNECT_KICK_INTERVAL_NANOS}, so an outage is not hammered): the next call dials at
 * once instead of when the backoff ends.
 */
public final class UdbChannels {

  private UdbChannels() {}

  // Keepalive: ping an idle connection every 30s, give the ack 10s, and permit pings
  // even with no active RPC so a fully idle channel stays warm.
  private static final long KEEPALIVE_TIME_SECONDS = 30L;
  private static final long KEEPALIVE_TIMEOUT_SECONDS = 10L;

  /** Shortest interval between two reconnect kicks of one channel. */
  static final long RECONNECT_KICK_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

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
    return build(b);
  }

  /**
   * Build a long-lived channel to {@code target} over caller-supplied channel
   * credentials, with UDB's default keepalive options. Use it for a private CA or a
   * mutual-TLS endpoint, for example:
   *
   * <pre>{@code
   * ChannelCredentials creds = TlsChannelCredentials.newBuilder()
   *     .trustManager(new File("ca.pem"))                          // trust this CA
   *     .keyManager(new File("client.pem"), new File("client.key")) // client certificate
   *     .build();
   * ManagedChannel channel = UdbChannels.forTarget("udb.example.com:443", creds);
   * }</pre>
   */
  public static ManagedChannel forTarget(String target, ChannelCredentials credentials) {
    if (credentials == null) {
      throw new IllegalArgumentException("udb: channel credentials must not be null");
    }
    return build(tune(Grpc.newChannelBuilder(target, credentials)));
  }

  /**
   * Build {@code builder} into a channel that wakes itself to dial again when a call fails with
   * UNAVAILABLE, so it reconnects promptly after a broker restart. Use it in place of {@code
   * builder.build()} for a channel configured by hand (with {@link #tune}).
   */
  public static ManagedChannel build(ManagedChannelBuilder<?> builder) {
    ReconnectKick kick = new ReconnectKick();
    ManagedChannel channel = builder.intercept(kick).build();
    kick.channel = channel;
    return channel;
  }

  /**
   * Resets the channel's connect backoff when a call fails with UNAVAILABLE, at most once per
   * {@link #RECONNECT_KICK_INTERVAL_NANOS}.
   */
  static final class ReconnectKick implements ClientInterceptor {
    volatile ManagedChannel channel;
    private final AtomicLong kickedAt = new AtomicLong(System.nanoTime() - RECONNECT_KICK_INTERVAL_NANOS);

    void onClose(Status status) {
      ManagedChannel target = channel;
      if (target == null || status.getCode() != Status.Code.UNAVAILABLE) {
        return;
      }
      long now = System.nanoTime();
      long last = kickedAt.get();
      if (now - last < RECONNECT_KICK_INTERVAL_NANOS || !kickedAt.compareAndSet(last, now)) {
        return;
      }
      target.resetConnectBackoff();
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
          next.newCall(method, callOptions)) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          super.start(
              new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
                  responseListener) {
                @Override
                public void onClose(Status status, Metadata trailers) {
                  ReconnectKick.this.onClose(status);
                  super.onClose(status, trailers);
                }
              },
              headers);
        }
      };
    }
  }
}
