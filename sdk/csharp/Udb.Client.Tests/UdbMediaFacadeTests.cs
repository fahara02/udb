using Xunit;
using AssetV1 = Udb.Core.Asset.Services.V1;
using StorageV1 = Udb.Core.Storage.Services.V1;
using WebRtcV1 = Udb.Core.Webrtc.Services.V1;

namespace Udb.Client.Tests;

/// <summary>
/// Unit tests for the Wave-8 media facades (Storage / Asset / WebRTC). They use
/// the <see cref="CapturingCallInvoker"/> to assert that each ergonomic wrapper
/// dispatches the right gRPC method with the request it was handed — no live
/// broker. Also pins the public <see cref="UdbProject"/> accessor surface.
/// </summary>
public sealed class UdbMediaFacadeTests
{
    private static readonly string[] WorkflowSequenceProbeRoots =
    {
        AppContext.BaseDirectory,
        Directory.GetCurrentDirectory(),
    };

    private static readonly string[] CanonicalHeaderNames =
    {
        "x-tenant-id",
        "x-user-id",
        "x-purpose",
        "x-correlation-id",
        "x-scopes",
        "x-service-identity",
        "x-udb-project-id",
        "x-udb-client-catalog-version",
    };

    private static Grpc.Core.Metadata Headers() => new()
    {
        { "x-tenant-id", "acme" },
        { "x-user-id", "user-1" },
        { "x-purpose", "media" },
        { "x-correlation-id", "corr-1" },
        { "x-scopes", "base" },
        { "x-service-identity", "svc" },
        { "x-udb-project-id", "proj-1" },
        { "x-udb-client-catalog-version", "1.0.0" },
    };

    // ── Storage ───────────────────────────────────────────────────────────────
    [Fact]
    public async Task Storage_RegisterUpload_Dispatches_RegisterUpload_With_Request_And_Headers()
    {
        var invoker = new CapturingCallInvoker(_ => new StorageV1.RegisterUploadResponse { FileId = "f-1" });
        var raw = new StorageV1.StorageService.StorageServiceClient(invoker);
        var storage = new UdbStorageClient(raw, Headers);

        var resp = await storage.RegisterUploadAsync(new StorageV1.RegisterUploadRequest
        {
            TenantId = "acme",
            Filename = "report.pdf",
        });

        Assert.Equal("f-1", resp.FileId);
        Assert.Equal("/udb.core.storage.services.v1.StorageService/RegisterUpload", invoker.LastMethod);
        var req = Assert.IsType<StorageV1.RegisterUploadRequest>(invoker.LastRequest);
        Assert.Equal("report.pdf", req.Filename);
        AssertCanonicalHeaders(invoker);
        Assert.Same(raw, storage.Raw);
    }

    [Fact]
    public async Task Storage_UploadFile_Does_RegisterPutFinalize_Only()
    {
        var putSeen = false;
        var observed = new List<string>();
        var invoker = new CapturingCallInvoker(method => method switch
        {
            "RegisterUpload" => Record(method, new StorageV1.RegisterUploadResponse
            {
                FileId = "file-1",
                UploadUrl = "https://put.example/file-1",
            }),
            "FinalizeUpload" => Record(method, new StorageV1.FinalizeUploadResponse()),
            _ => throw new InvalidOperationException(method),
        });
        var raw = new StorageV1.StorageService.StorageServiceClient(invoker);
        var storage = new UdbStorageClient(raw, Headers, (url, data, contentType, _) =>
        {
            Assert.Equal("https://put.example/file-1", url);
            Assert.Equal(new byte[] { 1, 2, 3 }, data);
            Assert.Equal("text/plain", contentType);
            putSeen = true;
            observed.Add("PUT");
            return Task.CompletedTask;
        });

        await storage.UploadFileAsync(
            "report.txt",
            new byte[] { 1, 2, 3 },
            new UdbStorageClient.UploadFileOptions(
                ContentType: "text/plain",
                FileType: "report",
                ReferenceId: "r-1",
                ReferenceType: "case",
                IsPublic: true,
                ExpiresInMinutes: 15,
                Checksum: "sha256:abc",
                Etag: "etag-1"));

        Assert.True(putSeen);
        Assert.Equal(LoadWorkflowSequence("StorageFacade.uploadFile"), observed);
        Assert.Equal(new[] { "RegisterUpload", "FinalizeUpload" }, invoker.MethodHistory);
        var register = Assert.IsType<StorageV1.RegisterUploadRequest>(invoker.RequestHistory[0]);
        Assert.Equal("acme", register.TenantId);
        Assert.Equal("proj-1", register.ProjectId);
        Assert.Equal("report.txt", register.Filename);
        Assert.Equal(3, register.SizeBytes);
        Assert.True(register.IsPublic);

        var finalize = Assert.IsType<StorageV1.FinalizeUploadRequest>(invoker.RequestHistory[1]);
        Assert.Equal("file-1", finalize.FileId);
        Assert.Equal(3, finalize.SizeBytes);
        Assert.Equal("sha256:abc", finalize.Checksum);
        Assert.Equal("etag-1", finalize.Etag);

        object Record(string method, object response)
        {
            observed.Add(method);
            return response;
        }
    }

    // ── Asset ─────────────────────────────────────────────────────────────────
    [Fact]
    public async Task Asset_RegisterAsset_Dispatches_RegisterAsset_With_Request_And_Headers()
    {
        var invoker = new CapturingCallInvoker(_ => new AssetV1.RegisterAssetResponse());
        var raw = new AssetV1.AssetService.AssetServiceClient(invoker);
        var asset = new UdbAssetClient(raw, Headers);

        await asset.RegisterAssetAsync(new AssetV1.RegisterAssetRequest());

        Assert.Equal("/udb.core.asset.services.v1.AssetService/RegisterAsset", invoker.LastMethod);
        Assert.IsType<AssetV1.RegisterAssetRequest>(invoker.LastRequest);
        AssertCanonicalHeaders(invoker);
        Assert.Same(raw, asset.Raw);
    }

    // ── WebRTC (grouped) ──────────────────────────────────────────────────────
    [Fact]
    public async Task WebRtc_Room_CreateRoom_Dispatches_CreateRoom_With_Headers()
    {
        var invoker = new CapturingCallInvoker(_ => new WebRtcV1.CreateRoomResponse());
        var room = new UdbWebRtcRoomClient(new WebRtcV1.RoomService.RoomServiceClient(invoker), Headers);

        await room.CreateRoomAsync(new WebRtcV1.CreateRoomRequest());

        Assert.Equal("/udb.core.webrtc.services.v1.RoomService/CreateRoom", invoker.LastMethod);
        Assert.IsType<WebRtcV1.CreateRoomRequest>(invoker.LastRequest);
        AssertCanonicalHeaders(invoker);
    }

    [Fact]
    public async Task WebRtc_Peer_JoinRoom_Dispatches_JoinRoom()
    {
        var invoker = new CapturingCallInvoker(_ => new WebRtcV1.JoinRoomResponse());
        var peer = new UdbWebRtcPeerClient(new WebRtcV1.PeerService.PeerServiceClient(invoker), Headers);

        await peer.JoinRoomAsync(new WebRtcV1.JoinRoomRequest());

        Assert.Equal("/udb.core.webrtc.services.v1.PeerService/JoinRoom", invoker.LastMethod);
        AssertCanonicalHeaders(invoker);
    }

    [Fact]
    public async Task WebRtc_Track_PublishTrack_Dispatches_PublishTrack()
    {
        var invoker = new CapturingCallInvoker(_ => new WebRtcV1.PublishTrackResponse());
        var track = new UdbWebRtcTrackClient(new WebRtcV1.TrackService.TrackServiceClient(invoker), Headers);

        await track.PublishTrackAsync(new WebRtcV1.PublishTrackRequest());

        Assert.Equal("/udb.core.webrtc.services.v1.TrackService/PublishTrack", invoker.LastMethod);
        AssertCanonicalHeaders(invoker);
    }

    [Fact]
    public async Task WebRtc_Turn_IssueCredentials_Dispatches_IssueCredentials()
    {
        var invoker = new CapturingCallInvoker(_ => new WebRtcV1.IssueCredentialsResponse());
        var turn = new UdbWebRtcTurnClient(new WebRtcV1.TurnService.TurnServiceClient(invoker), Headers);

        await turn.IssueCredentialsAsync(new WebRtcV1.IssueCredentialsRequest());

        Assert.Equal("/udb.core.webrtc.services.v1.TurnService/IssueCredentials", invoker.LastMethod);
        AssertCanonicalHeaders(invoker);
    }

    // ── public facade surface ─────────────────────────────────────────────────
    [Fact]
    public void UdbProject_Exposes_Media_Facade_Accessors()
    {
        var t = typeof(UdbProject);
        Assert.Equal(typeof(UdbStorageClient), t.GetProperty("Storage")!.PropertyType);
        Assert.Equal(typeof(UdbAssetClient), t.GetProperty("Asset")!.PropertyType);
        Assert.Equal(typeof(UdbWebRtcClient), t.GetProperty("WebRtc")!.PropertyType);

        var w = typeof(UdbWebRtcClient);
        Assert.Equal(typeof(UdbWebRtcRoomClient), w.GetProperty("Room")!.PropertyType);
        Assert.Equal(typeof(UdbWebRtcPeerClient), w.GetProperty("Peer")!.PropertyType);
        Assert.Equal(typeof(UdbWebRtcTrackClient), w.GetProperty("Track")!.PropertyType);
        Assert.Equal(typeof(UdbWebRtcTurnClient), w.GetProperty("Turn")!.PropertyType);
        Assert.Equal(typeof(UdbWebRtcSignalingClient), w.GetProperty("Signaling")!.PropertyType);
    }

    [Fact]
    public void UdbProject_Native_Service_Channel_Uses_Auth_Target()
    {
        using var project = UdbProject.Open(new UdbProjectConfig
        {
            Target = "http://localhost:1",
            AuthTarget = "http://localhost:2",
            TenantId = "acme",
        });

        Assert.NotSame(project.DataChannelForTesting, project.AuthChannelForTesting);
        Assert.Same(project.AuthChannelForTesting, project.NativeServicesChannelForTesting);
        Assert.Equal("localhost:2", project.NativeServicesChannelForTesting.Target);
    }

    private static void AssertCanonicalHeaders(CapturingCallInvoker invoker)
    {
        Assert.NotNull(invoker.LastHeaders);
        foreach (var name in CanonicalHeaderNames)
        {
            Assert.Contains(invoker.LastHeaders!, e => e.Key == name);
        }
    }

    private static string[] LoadWorkflowSequence(string helper)
    {
        var path = FindWorkflowSequenceFixture();
        foreach (var rawLine in File.ReadLines(path))
        {
            var line = rawLine.Trim();
            if (!line.StartsWith("|", StringComparison.Ordinal) || line.Contains("---", StringComparison.Ordinal))
            {
                continue;
            }

            var parts = line.Split('|', StringSplitOptions.TrimEntries);
            if (parts.Length < 4 || parts[1] != helper)
            {
                continue;
            }

            return parts[2].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        }

        throw new InvalidOperationException($"workflow-sequences.md has no row for helper {helper}");
    }

    private static string FindWorkflowSequenceFixture()
    {
        foreach (var root in WorkflowSequenceProbeRoots)
        {
            var dir = new DirectoryInfo(root);
            while (dir is not null)
            {
                var candidate = Path.Combine(dir.FullName, "docs", "bench-bodies", "workflow-sequences.md");
                if (File.Exists(candidate))
                {
                    return candidate;
                }

                dir = dir.Parent;
            }
        }

        throw new FileNotFoundException("docs/bench-bodies/workflow-sequences.md was not found");
    }
}
