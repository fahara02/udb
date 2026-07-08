package dev.udb.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.udb.core.asset.services.v1.CreatePipelineDefinitionRequest;
import com.udb.core.storage.services.v1.FinalizeUploadRequest;
import com.udb.core.storage.services.v1.FinalizeUploadResponse;
import com.udb.core.storage.services.v1.RegisterUploadRequest;
import com.udb.core.storage.services.v1.RegisterUploadResponse;
import com.udb.core.webrtc.services.v1.CreateRoomRequest;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.stub.AbstractStub;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
  void storageUploadFileEmitsSharedWorkflowSequence() {
    List<String> observed = new ArrayList<>();
    CapturingStorageChannel channel = new CapturingStorageChannel(observed);
    UdbMetadata metadata =
        new UdbMetadata(
            "tenant-a",
            "java.upload.sequence.test",
            "corr-1",
            List.of("storage.write"),
            "java.test",
            "user-a",
            "project-a",
            "test");
    UdbStorageClient storage =
        new UdbStorageClient(
            channel,
            new UdbMetadataRef(metadata),
            UdbCredentials.fromMetadata(metadata),
            (url, data, contentType) -> {
              assertEquals("https://object.example/upload/file-java-seq", url);
              assertEquals("hello from java", new String(data, StandardCharsets.UTF_8));
              assertEquals("text/plain", contentType);
              observed.add("PUT");
            });

    storage.uploadFile(
        "hello.txt",
        "hello from java".getBytes(StandardCharsets.UTF_8),
        new UdbStorageClient.UploadFileOptions(
            "text/plain", "DOCUMENT", "ref-1", "case", true, 15, "sha256:java", "etag-java"));

    assertEquals(loadWorkflowSequence("StorageFacade.uploadFile"), observed);
    RegisterUploadRequest register = (RegisterUploadRequest) channel.requests.get(0);
    assertEquals("tenant-a", register.getTenantId());
    assertEquals("project-a", register.getProjectId());
    assertEquals("hello.txt", register.getFilename());
    assertEquals("text/plain", register.getContentType());
    assertEquals("DOCUMENT", register.getFileType());
    assertEquals("ref-1", register.getReferenceId());
    assertEquals("case", register.getReferenceType());
    assertTrue(register.getIsPublic());
    assertEquals(15, register.getExpiresInMinutes());
    assertEquals("hello from java".getBytes(StandardCharsets.UTF_8).length, register.getSizeBytes());

    FinalizeUploadRequest finalize = (FinalizeUploadRequest) channel.requests.get(1);
    assertEquals("tenant-a", finalize.getTenantId());
    assertEquals("file-java-seq", finalize.getFileId());
    assertEquals("text/plain", finalize.getContentType());
    assertEquals("DOCUMENT", finalize.getFileType());
    assertEquals("ref-1", finalize.getReferenceId());
    assertEquals("case", finalize.getReferenceType());
    assertTrue(finalize.getIsPublic());
    assertEquals("hello from java".getBytes(StandardCharsets.UTF_8).length, finalize.getSizeBytes());
    assertEquals("sha256:java", finalize.getChecksum());
    assertEquals("etag-java", finalize.getEtag());
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

  private static List<String> loadWorkflowSequence(String helper) {
    Path path = findWorkflowSequenceFixture();
    try {
      for (String rawLine : Files.readAllLines(path)) {
        String line = rawLine.trim();
        if (!line.startsWith("|") || line.contains("---")) {
          continue;
        }
        String[] cols = line.substring(1, line.length() - 1).split("\\|");
        if (cols.length < 2 || !cols[0].trim().equals(helper)) {
          continue;
        }
        List<String> out = new ArrayList<>();
        for (String part : cols[1].split(",")) {
          String trimmed = part.trim();
          if (!trimmed.isEmpty()) {
            out.add(trimmed);
          }
        }
        return out;
      }
    } catch (Exception err) {
      throw new AssertionError("failed to read workflow-sequences.md", err);
    }
    throw new AssertionError("workflow helper " + helper + " missing from workflow-sequences.md");
  }

  private static Path findWorkflowSequenceFixture() {
    Path dir = Path.of(System.getProperty("user.dir")).toAbsolutePath();
    while (dir != null) {
      Path candidate = dir.resolve("docs").resolve("bench-bodies").resolve("workflow-sequences.md");
      if (Files.isRegularFile(candidate)) {
        return candidate;
      }
      dir = dir.getParent();
    }
    throw new AssertionError("docs/bench-bodies/workflow-sequences.md was not found");
  }

  private static final class CapturingStorageChannel extends Channel {
    private final List<String> observed;
    private final List<Object> requests = new ArrayList<>();

    private CapturingStorageChannel(List<String> observed) {
      this.observed = observed;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
        MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
      return new ClientCall<>() {
        private Listener<RespT> listener;

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          listener = responseListener;
        }

        @Override
        public void request(int numMessages) {}

        @Override
        public void cancel(String message, Throwable cause) {}

        @Override
        public void halfClose() {
          listener.onMessage(response(methodDescriptor.getBareMethodName()));
          listener.onClose(Status.OK, new Metadata());
        }

        @Override
        public void sendMessage(ReqT message) {
          observed.add(methodDescriptor.getBareMethodName());
          requests.add(message);
        }

        @SuppressWarnings("unchecked")
        private RespT response(String method) {
          return (RespT)
              switch (method) {
                case "RegisterUpload" ->
                    RegisterUploadResponse.newBuilder()
                        .setFileId("file-java-seq")
                        .setUploadUrl("https://object.example/upload/file-java-seq")
                        .build();
                case "FinalizeUpload" -> FinalizeUploadResponse.newBuilder().build();
                default -> throw new AssertionError("unexpected storage RPC " + method);
              };
        }
      };
    }

    @Override
    public String authority() {
      return "localhost";
    }
  }
}
