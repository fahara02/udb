package dev.udb.client;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.grpc.ChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import org.junit.jupiter.api.Test;

/** Caller-supplied channel credentials (private CA, mutual TLS) reach every channel. */
final class UdbChannelsTest {

  @Test
  void projectDialsWithCallerCredentials() {
    ChannelCredentials creds = TlsChannelCredentials.create();
    UdbProjectConfig config =
        UdbProjectConfig.builder()
            .target("localhost:1") // unreachable; no call is dispatched
            .authTarget("localhost:2")
            .tenantId("tenant-a")
            .channelCredentials(creds)
            .build();
    assertSame(creds, config.channelCredentials());
    try (UdbProject project = UdbProject.open(config)) {
      assertNotNull(project.data());
    }
  }

  @Test
  void forTargetBuildsAChannelAndRefusesNullCredentials() {
    ManagedChannel channel = UdbChannels.forTarget("localhost:1", TlsChannelCredentials.create());
    channel.shutdownNow();
    assertThrows(
        IllegalArgumentException.class,
        () -> UdbChannels.forTarget("localhost:1", (ChannelCredentials) null));
  }
}
