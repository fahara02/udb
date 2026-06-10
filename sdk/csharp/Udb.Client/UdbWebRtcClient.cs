using Grpc.Core;
using WebRtcV1 = udb.core.Webrtc.Services.V1;

namespace Udb.Client;

/// <summary>
/// Ergonomic async facade over the native WebRTC control plane. The proto splits
/// the surface across five gRPC services (RoomService, PeerService, TrackService,
/// TurnService, SignalingService); this facade groups them under the matching
/// accessors <see cref="Room"/>, <see cref="Peer"/>, <see cref="Track"/>,
/// <see cref="Turn"/>, plus a thin <see cref="Signaling"/> stream accessor.
/// Every wrapper applies the shared <see cref="UdbProject"/> metadata headers.
/// </summary>
public sealed class UdbWebRtcClient
{
    internal UdbWebRtcClient(
        WebRtcV1.RoomService.RoomServiceClient room,
        WebRtcV1.PeerService.PeerServiceClient peer,
        WebRtcV1.TrackService.TrackServiceClient track,
        WebRtcV1.TurnService.TurnServiceClient turn,
        WebRtcV1.SignalingService.SignalingServiceClient signaling,
        Func<Metadata> headers)
    {
        Room = new UdbWebRtcRoomClient(room, headers);
        Peer = new UdbWebRtcPeerClient(peer, headers);
        Track = new UdbWebRtcTrackClient(track, headers);
        Turn = new UdbWebRtcTurnClient(turn, headers);
        Signaling = new UdbWebRtcSignalingClient(signaling, headers);
    }

    /// <summary>Room lifecycle (create/get/update/close/list).</summary>
    public UdbWebRtcRoomClient Room { get; }

    /// <summary>Peer membership (join/leave/get/list).</summary>
    public UdbWebRtcPeerClient Peer { get; }

    /// <summary>Track publication (publish/unpublish/mute/list).</summary>
    public UdbWebRtcTrackClient Track { get; }

    /// <summary>TURN credential issuance.</summary>
    public UdbWebRtcTurnClient Turn { get; }

    /// <summary>Bidirectional signaling stream accessor (SDP/ICE exchange).</summary>
    public UdbWebRtcSignalingClient Signaling { get; }
}

/// <summary>Async facade over <c>RoomService</c>.</summary>
public sealed class UdbWebRtcRoomClient
{
    private readonly WebRtcV1.RoomService.RoomServiceClient _client;
    private readonly Func<Metadata> _headers;

    internal UdbWebRtcRoomClient(WebRtcV1.RoomService.RoomServiceClient client, Func<Metadata> headers)
    {
        _client = client;
        _headers = headers;
    }

    /// <summary>Raw generated room service client.</summary>
    public WebRtcV1.RoomService.RoomServiceClient Raw => _client;

    public Task<WebRtcV1.CreateRoomResponse> CreateRoomAsync(
        WebRtcV1.CreateRoomRequest request, CancellationToken ct = default)
        => _client.CreateRoomAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    public Task<WebRtcV1.GetRoomResponse> GetRoomAsync(
        WebRtcV1.GetRoomRequest request, CancellationToken ct = default)
        => _client.GetRoomAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    public Task<WebRtcV1.UpdateRoomResponse> UpdateRoomAsync(
        WebRtcV1.UpdateRoomRequest request, CancellationToken ct = default)
        => _client.UpdateRoomAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    public Task<WebRtcV1.CloseRoomResponse> CloseRoomAsync(
        WebRtcV1.CloseRoomRequest request, CancellationToken ct = default)
        => _client.CloseRoomAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    public Task<WebRtcV1.ListRoomsResponse> ListRoomsAsync(
        WebRtcV1.ListRoomsRequest request, CancellationToken ct = default)
        => _client.ListRoomsAsync(request, _headers(), cancellationToken: ct).ResponseAsync;
}

/// <summary>Async facade over <c>PeerService</c>.</summary>
public sealed class UdbWebRtcPeerClient
{
    private readonly WebRtcV1.PeerService.PeerServiceClient _client;
    private readonly Func<Metadata> _headers;

    internal UdbWebRtcPeerClient(WebRtcV1.PeerService.PeerServiceClient client, Func<Metadata> headers)
    {
        _client = client;
        _headers = headers;
    }

    /// <summary>Raw generated peer service client.</summary>
    public WebRtcV1.PeerService.PeerServiceClient Raw => _client;

    public Task<WebRtcV1.JoinRoomResponse> JoinRoomAsync(
        WebRtcV1.JoinRoomRequest request, CancellationToken ct = default)
        => _client.JoinRoomAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    public Task<WebRtcV1.LeaveRoomResponse> LeaveRoomAsync(
        WebRtcV1.LeaveRoomRequest request, CancellationToken ct = default)
        => _client.LeaveRoomAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    public Task<WebRtcV1.GetPeerResponse> GetPeerAsync(
        WebRtcV1.GetPeerRequest request, CancellationToken ct = default)
        => _client.GetPeerAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    public Task<WebRtcV1.ListPeersResponse> ListPeersAsync(
        WebRtcV1.ListPeersRequest request, CancellationToken ct = default)
        => _client.ListPeersAsync(request, _headers(), cancellationToken: ct).ResponseAsync;
}

/// <summary>Async facade over <c>TrackService</c>.</summary>
public sealed class UdbWebRtcTrackClient
{
    private readonly WebRtcV1.TrackService.TrackServiceClient _client;
    private readonly Func<Metadata> _headers;

    internal UdbWebRtcTrackClient(WebRtcV1.TrackService.TrackServiceClient client, Func<Metadata> headers)
    {
        _client = client;
        _headers = headers;
    }

    /// <summary>Raw generated track service client.</summary>
    public WebRtcV1.TrackService.TrackServiceClient Raw => _client;

    public Task<WebRtcV1.PublishTrackResponse> PublishTrackAsync(
        WebRtcV1.PublishTrackRequest request, CancellationToken ct = default)
        => _client.PublishTrackAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    public Task<WebRtcV1.UnpublishTrackResponse> UnpublishTrackAsync(
        WebRtcV1.UnpublishTrackRequest request, CancellationToken ct = default)
        => _client.UnpublishTrackAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    public Task<WebRtcV1.MuteTrackResponse> MuteTrackAsync(
        WebRtcV1.MuteTrackRequest request, CancellationToken ct = default)
        => _client.MuteTrackAsync(request, _headers(), cancellationToken: ct).ResponseAsync;

    public Task<WebRtcV1.ListTracksResponse> ListTracksAsync(
        WebRtcV1.ListTracksRequest request, CancellationToken ct = default)
        => _client.ListTracksAsync(request, _headers(), cancellationToken: ct).ResponseAsync;
}

/// <summary>Async facade over <c>TurnService</c>.</summary>
public sealed class UdbWebRtcTurnClient
{
    private readonly WebRtcV1.TurnService.TurnServiceClient _client;
    private readonly Func<Metadata> _headers;

    internal UdbWebRtcTurnClient(WebRtcV1.TurnService.TurnServiceClient client, Func<Metadata> headers)
    {
        _client = client;
        _headers = headers;
    }

    /// <summary>Raw generated turn service client.</summary>
    public WebRtcV1.TurnService.TurnServiceClient Raw => _client;

    /// <summary>Issue short-lived TURN credentials for a peer.</summary>
    public Task<WebRtcV1.IssueCredentialsResponse> IssueCredentialsAsync(
        WebRtcV1.IssueCredentialsRequest request, CancellationToken ct = default)
        => _client.IssueCredentialsAsync(request, _headers(), cancellationToken: ct).ResponseAsync;
}

/// <summary>
/// Thin accessor over <c>SignalingService.Signal</c> — a bidirectional
/// (duplex-streaming) RPC for SDP offer/answer and ICE exchange. Because it is a
/// stream (not a unary request/response), it is not wrapped as a single async
/// call; callers open the duplex stream and drive the
/// <see cref="AsyncDuplexStreamingCall{TRequest,TResponse}"/> request/response
/// pipes themselves. The shared metadata headers are applied on open.
/// </summary>
public sealed class UdbWebRtcSignalingClient
{
    private readonly WebRtcV1.SignalingService.SignalingServiceClient _client;
    private readonly Func<Metadata> _headers;

    internal UdbWebRtcSignalingClient(WebRtcV1.SignalingService.SignalingServiceClient client, Func<Metadata> headers)
    {
        _client = client;
        _headers = headers;
    }

    /// <summary>Raw generated signaling service client.</summary>
    public WebRtcV1.SignalingService.SignalingServiceClient Raw => _client;

    /// <summary>
    /// Open the bidirectional signaling stream. The returned call exposes
    /// <c>RequestStream</c> / <c>ResponseStream</c> for the caller to drive.
    /// Shared metadata headers are applied.
    /// </summary>
    public AsyncDuplexStreamingCall<WebRtcV1.SignalRequest, WebRtcV1.SignalResponse> Signal(
        CancellationToken ct = default)
        => _client.Signal(_headers(), cancellationToken: ct);
}
