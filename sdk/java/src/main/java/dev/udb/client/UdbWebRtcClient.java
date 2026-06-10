package dev.udb.client;

import com.udb.core.webrtc.services.v1.CloseRoomRequest;
import com.udb.core.webrtc.services.v1.CloseRoomResponse;
import com.udb.core.webrtc.services.v1.CreateRoomRequest;
import com.udb.core.webrtc.services.v1.CreateRoomResponse;
import com.udb.core.webrtc.services.v1.GetPeerRequest;
import com.udb.core.webrtc.services.v1.GetPeerResponse;
import com.udb.core.webrtc.services.v1.GetRoomRequest;
import com.udb.core.webrtc.services.v1.GetRoomResponse;
import com.udb.core.webrtc.services.v1.IssueCredentialsRequest;
import com.udb.core.webrtc.services.v1.IssueCredentialsResponse;
import com.udb.core.webrtc.services.v1.JoinRoomRequest;
import com.udb.core.webrtc.services.v1.JoinRoomResponse;
import com.udb.core.webrtc.services.v1.LeaveRoomRequest;
import com.udb.core.webrtc.services.v1.LeaveRoomResponse;
import com.udb.core.webrtc.services.v1.ListPeersRequest;
import com.udb.core.webrtc.services.v1.ListPeersResponse;
import com.udb.core.webrtc.services.v1.ListRoomsRequest;
import com.udb.core.webrtc.services.v1.ListRoomsResponse;
import com.udb.core.webrtc.services.v1.ListTracksRequest;
import com.udb.core.webrtc.services.v1.ListTracksResponse;
import com.udb.core.webrtc.services.v1.MuteTrackRequest;
import com.udb.core.webrtc.services.v1.MuteTrackResponse;
import com.udb.core.webrtc.services.v1.PeerServiceGrpc;
import com.udb.core.webrtc.services.v1.PublishTrackRequest;
import com.udb.core.webrtc.services.v1.PublishTrackResponse;
import com.udb.core.webrtc.services.v1.RoomServiceGrpc;
import com.udb.core.webrtc.services.v1.SignalRequest;
import com.udb.core.webrtc.services.v1.SignalResponse;
import com.udb.core.webrtc.services.v1.SignalingServiceGrpc;
import com.udb.core.webrtc.services.v1.TrackServiceGrpc;
import com.udb.core.webrtc.services.v1.TurnServiceGrpc;
import com.udb.core.webrtc.services.v1.UnpublishTrackRequest;
import com.udb.core.webrtc.services.v1.UnpublishTrackResponse;
import com.udb.core.webrtc.services.v1.UpdateRoomRequest;
import com.udb.core.webrtc.services.v1.UpdateRoomResponse;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

/**
 * Blocking facade over the native WebRTC services, grouped by resource:
 * {@link #room()}, {@link #peer()}, {@link #track()}, {@link #turn()}. Each rides
 * the shared control-plane channel and attaches the project {@link UdbMetadata}
 * headers to every call.
 *
 * <p>{@code SignalingService.Signal} is a bidirectional stream, so it is exposed
 * via {@link #signaling()} (the async stub) and the {@link #signal} convenience —
 * deliberately not wrapped as a blocking call, which would not make sense over a
 * bidi stream.
 */
public final class UdbWebRtcClient {
  private final Room room;
  private final Peer peer;
  private final Track track;
  private final Turn turn;
  private final SignalingServiceGrpc.SignalingServiceStub signaling;

  UdbWebRtcClient(Channel channel, UdbMetadata metadata) {
    this(channel, metadata, UdbCredentials.fromMetadata(metadata));
  }

  UdbWebRtcClient(Channel channel, UdbMetadata metadata, UdbCredentials credentials) {
    io.grpc.ClientInterceptor headers =
        UdbClient.credentialInterceptor(metadata, credentials);
    this.room =
        new Room(RoomServiceGrpc.newBlockingStub(channel).withInterceptors(headers));
    this.peer =
        new Peer(PeerServiceGrpc.newBlockingStub(channel).withInterceptors(headers));
    this.track =
        new Track(TrackServiceGrpc.newBlockingStub(channel).withInterceptors(headers));
    this.turn =
        new Turn(TurnServiceGrpc.newBlockingStub(channel).withInterceptors(headers));
    this.signaling =
        SignalingServiceGrpc.newStub(channel).withInterceptors(headers);
  }

  public Room room() {
    return room;
  }

  public Peer peer() {
    return peer;
  }

  public Track track() {
    return track;
  }

  public Turn turn() {
    return turn;
  }

  /**
   * The async signaling stub for the bidirectional {@code Signal} stream. Use
   * {@link #signal(StreamObserver)} as a shorthand to open a stream.
   */
  public SignalingServiceGrpc.SignalingServiceStub signaling() {
    return signaling;
  }

  /**
   * Open the bidirectional SDP/ICE signaling stream. Returns the request observer
   * to push {@link SignalRequest}s; {@code responses} receives the peer's
   * {@link SignalResponse}s. This is NOT a blocking call.
   */
  public StreamObserver<SignalRequest> signal(StreamObserver<SignalResponse> responses) {
    return signaling.signal(responses);
  }

  /** RoomService wrapper: lifecycle of rooms. */
  public static final class Room {
    private final RoomServiceGrpc.RoomServiceBlockingStub stub;

    Room(RoomServiceGrpc.RoomServiceBlockingStub stub) {
      this.stub = stub;
    }

    public RoomServiceGrpc.RoomServiceBlockingStub stub() {
      return stub;
    }

    public CreateRoomResponse createRoom(CreateRoomRequest request) {
      return stub.createRoom(request);
    }

    public GetRoomResponse getRoom(GetRoomRequest request) {
      return stub.getRoom(request);
    }

    public UpdateRoomResponse updateRoom(UpdateRoomRequest request) {
      return stub.updateRoom(request);
    }

    public CloseRoomResponse closeRoom(CloseRoomRequest request) {
      return stub.closeRoom(request);
    }

    public ListRoomsResponse listRooms(ListRoomsRequest request) {
      return stub.listRooms(request);
    }
  }

  /** PeerService wrapper: participants joining/leaving rooms. */
  public static final class Peer {
    private final PeerServiceGrpc.PeerServiceBlockingStub stub;

    Peer(PeerServiceGrpc.PeerServiceBlockingStub stub) {
      this.stub = stub;
    }

    public PeerServiceGrpc.PeerServiceBlockingStub stub() {
      return stub;
    }

    public JoinRoomResponse joinRoom(JoinRoomRequest request) {
      return stub.joinRoom(request);
    }

    public LeaveRoomResponse leaveRoom(LeaveRoomRequest request) {
      return stub.leaveRoom(request);
    }

    public GetPeerResponse getPeer(GetPeerRequest request) {
      return stub.getPeer(request);
    }

    public ListPeersResponse listPeers(ListPeersRequest request) {
      return stub.listPeers(request);
    }
  }

  /** TrackService wrapper: published media tracks. */
  public static final class Track {
    private final TrackServiceGrpc.TrackServiceBlockingStub stub;

    Track(TrackServiceGrpc.TrackServiceBlockingStub stub) {
      this.stub = stub;
    }

    public TrackServiceGrpc.TrackServiceBlockingStub stub() {
      return stub;
    }

    public PublishTrackResponse publishTrack(PublishTrackRequest request) {
      return stub.publishTrack(request);
    }

    public UnpublishTrackResponse unpublishTrack(UnpublishTrackRequest request) {
      return stub.unpublishTrack(request);
    }

    public MuteTrackResponse muteTrack(MuteTrackRequest request) {
      return stub.muteTrack(request);
    }

    public ListTracksResponse listTracks(ListTracksRequest request) {
      return stub.listTracks(request);
    }
  }

  /** TurnService wrapper: ephemeral TURN credentials. */
  public static final class Turn {
    private final TurnServiceGrpc.TurnServiceBlockingStub stub;

    Turn(TurnServiceGrpc.TurnServiceBlockingStub stub) {
      this.stub = stub;
    }

    public TurnServiceGrpc.TurnServiceBlockingStub stub() {
      return stub;
    }

    public IssueCredentialsResponse issueCredentials(IssueCredentialsRequest request) {
      return stub.issueCredentials(request);
    }
  }
}
