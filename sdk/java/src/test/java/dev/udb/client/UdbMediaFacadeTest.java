package dev.udb.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.udb.core.asset.services.v1.CreatePipelineDefinitionRequest;
import com.udb.core.storage.services.v1.RegisterUploadRequest;
import com.udb.core.webrtc.services.v1.CreateRoomRequest;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractStub;
import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;

/**
 * Facade-shape tests for the Phase 7 M8 media services (storage / asset /
 * WebRTC). They assert {@link UdbProject} exposes the new sub-clients, that the
 * WebRTC grouped accessors and raw stubs are reachable, and that the wrapper
 * request builders carry the expected fields. No live broker is required — the
 * project opens a plaintext channel but no RPC is dispatched.
 */
final class UdbMediaFacadeTest {

  private static UdbProject openOffline() {
    return UdbProject.open(
        UdbProjectConfig.builder()
            .target("localhost:1") // unreachable; we never dispatch a call
            .tenantId("tenant-a")
            .projectId("project-a")
            .build());
  }

  @Test
  void projectExposesStorageAssetAndWebRtcSubClients() {
    try (UdbProject project = openOffline()) {
      assertNotNull(project.storage(), "storage() facade missing");
      assertNotNull(project.asset(), "asset() facade missing");
      assertNotNull(project.webRtc(), "webRtc() facade missing");

      // Same instance returned each time (sub-clients are cached, not rebuilt).
      assertSame(project.storage(), project.storage());
      assertSame(project.asset(), project.asset());
      assertSame(project.webRtc(), project.webRtc());

      // Raw generated stubs stay reachable.
      assertNotNull(project.storage().stub());
      assertNotNull(project.asset().stub());
    }
  }

  @Test
  void webRtcExposesRoomPeerTrackAndTurnAccessors() {
    try (UdbProject project = openOffline()) {
      UdbWebRtcClient webrtc = project.webRtc();
      assertNotNull(webrtc.room(), "room() accessor missing");
      assertNotNull(webrtc.peer(), "peer() accessor missing");
      assertNotNull(webrtc.track(), "track() accessor missing");
      assertNotNull(webrtc.turn(), "turn() accessor missing");
      // Signaling is a bidi stream: exposed as the async stub, not a blocking call.
      assertNotNull(webrtc.signaling(), "signaling() async stub missing");

      // Raw generated stubs under each group stay reachable.
      assertNotNull(webrtc.room().stub());
      assertNotNull(webrtc.peer().stub());
      assertNotNull(webrtc.track().stub());
      assertNotNull(webrtc.turn().stub());
    }
  }

  @Test
  void storageRegisterUploadBuildsExpectedRequestFields() {
    RegisterUploadRequest request =
        RegisterUploadRequest.newBuilder()
            .setTenantId("tenant-a")
            .setFilename("avatar.png")
            .setContentType("image/png")
            .setSizeBytes(2048L)
            .build();

    assertEquals("tenant-a", request.getTenantId());
    assertEquals("avatar.png", request.getFilename());
    assertEquals("image/png", request.getContentType());
    assertEquals(2048L, request.getSizeBytes());
  }

  @Test
  void assetCreatePipelineDefinitionBuildsExpectedRequestFields() {
    CreatePipelineDefinitionRequest request =
        CreatePipelineDefinitionRequest.newBuilder().setTenantId("tenant-a").build();
    assertEquals("tenant-a", request.getTenantId());
  }

  @Test
  void webRtcCreateRoomBuildsExpectedRequestFields() {
    CreateRoomRequest request =
        CreateRoomRequest.newBuilder()
            .setTenantId("tenant-a")
            .setName("standup")
            .setMaxParticipants(10)
            .build();
    assertEquals("tenant-a", request.getTenantId());
    assertEquals("standup", request.getName());
    assertEquals(10, request.getMaxParticipants());
  }

  @Test
  void nativeServiceStubsUseAuthChannelWhenAuthTargetDiffers() throws Exception {
    try (UdbProject project =
        UdbProject.open(
            UdbProjectConfig.builder()
                .target("localhost:1")
                .authTarget("localhost:2")
                .tenantId("tenant-a")
                .build())) {
      ManagedChannel dataChannel = channelField(project, "dataChannel");
      ManagedChannel authChannel = channelField(project, "authChannel");
      assertNotSame(dataChannel, authChannel);

      assertSame(authChannel, interceptedDelegate(project.apiKey()));
      assertSame(authChannel, interceptedDelegate(project.tenant()));
      assertSame(authChannel, interceptedDelegate(project.notification()));
      assertSame(authChannel, interceptedDelegate(project.analytics()));
      assertSame(authChannel, interceptedDelegate(project.storage().stub()));
      assertSame(authChannel, interceptedDelegate(project.asset().stub()));
    }
  }

  private static ManagedChannel channelField(UdbProject project, String name) throws Exception {
    Field field = UdbProject.class.getDeclaredField(name);
    field.setAccessible(true);
    return (ManagedChannel) field.get(project);
  }

  private static Channel interceptedDelegate(AbstractStub<?> stub) throws Exception {
    Channel channel = stub.getChannel();
    for (Field field : channel.getClass().getDeclaredFields()) {
      if (Channel.class.isAssignableFrom(field.getType())) {
        field.setAccessible(true);
        return (Channel) field.get(channel);
      }
    }
    throw new AssertionError("could not find intercepted delegate channel on " + channel.getClass());
  }
}
