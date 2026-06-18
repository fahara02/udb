package udbclient

import (
	"bytes"
	"context"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	assetv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/asset/services/v1"
	notificationv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/notification/services/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/services/v1"
	webrtcv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/webrtc/services/v1"
	"google.golang.org/grpc"
)

// The fakes embed the generated client interfaces so only the exercised methods
// need bodies; every other method is a nil panic the tests never reach. Each
// fake records the last request so the wrapper's field-mapping can be asserted.

type fakeStorageClient struct {
	storagev1.StorageServiceClient
	lastRegister *storagev1.RegisterUploadRequest
	lastList     *storagev1.ListFilesRequest
}

func (f *fakeStorageClient) RegisterUpload(_ context.Context, in *storagev1.RegisterUploadRequest, _ ...grpc.CallOption) (*storagev1.RegisterUploadResponse, error) {
	f.lastRegister = in
	return &storagev1.RegisterUploadResponse{FileId: "file-123"}, nil
}

func (f *fakeStorageClient) ListFiles(_ context.Context, in *storagev1.ListFilesRequest, _ ...grpc.CallOption) (*storagev1.ListFilesResponse, error) {
	f.lastList = in
	return &storagev1.ListFilesResponse{}, nil
}

type fakeAssetClient struct {
	assetv1.AssetServiceClient
	lastStart *assetv1.StartPipelineRequest
}

func (f *fakeAssetClient) StartPipeline(_ context.Context, in *assetv1.StartPipelineRequest, _ ...grpc.CallOption) (*assetv1.StartPipelineResponse, error) {
	f.lastStart = in
	return &assetv1.StartPipelineResponse{}, nil
}

type fakeRoomClient struct {
	webrtcv1.RoomServiceClient
	lastCreate *webrtcv1.CreateRoomRequest
}

func (f *fakeRoomClient) CreateRoom(_ context.Context, in *webrtcv1.CreateRoomRequest, _ ...grpc.CallOption) (*webrtcv1.CreateRoomResponse, error) {
	f.lastCreate = in
	return &webrtcv1.CreateRoomResponse{}, nil
}

type fakeTurnClient struct {
	webrtcv1.TurnServiceClient
	lastIssue *webrtcv1.IssueCredentialsRequest
}

func (f *fakeTurnClient) IssueCredentials(_ context.Context, in *webrtcv1.IssueCredentialsRequest, _ ...grpc.CallOption) (*webrtcv1.IssueCredentialsResponse, error) {
	f.lastIssue = in
	return &webrtcv1.IssueCredentialsResponse{}, nil
}

func TestStorageFacadeRegisterUploadBuildsRequest(t *testing.T) {
	fake := &fakeStorageClient{}
	f := &StorageFacade{Raw: fake, meta: Metadata{TenantID: "t-1", ProjectID: "p-1"}}

	resp, err := f.RegisterUpload(context.Background(), "a.png", "image/png", "image", 4096)
	if err != nil {
		t.Fatalf("RegisterUpload: %v", err)
	}
	if resp.FileId != "file-123" {
		t.Fatalf("response not threaded through: %q", resp.FileId)
	}
	got := fake.lastRegister
	if got.TenantId != "t-1" || got.ProjectId != "p-1" {
		t.Fatalf("tenant/project not defaulted from meta: %+v", got)
	}
	if got.Filename != "a.png" || got.ContentType != "image/png" || got.FileType != "image" || got.SizeBytes != 4096 {
		t.Fatalf("fields not mapped: %+v", got)
	}
}

func TestStorageFacadeListFilesDefaultsTenant(t *testing.T) {
	fake := &fakeStorageClient{}
	f := &StorageFacade{Raw: fake, meta: Metadata{TenantID: "t-9"}}
	if _, err := f.ListFiles(context.Background(), "doc", 2, 50); err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	got := fake.lastList
	if got.TenantId != "t-9" || got.FileType != "doc" || got.Page != 2 || got.PageSize != 50 {
		t.Fatalf("fields not mapped: %+v", got)
	}
}

func TestAssetFacadeStartPipelineDefaultsCorrelation(t *testing.T) {
	fake := &fakeAssetClient{}
	f := &AssetFacade{Raw: fake, meta: Metadata{TenantID: "t-2", CorrelationID: "corr-7"}}

	// Empty correlationID should fall back to the meta CorrelationID.
	if _, err := f.StartPipeline(context.Background(), "def-1", "asset-1", "{}", ""); err != nil {
		t.Fatalf("StartPipeline: %v", err)
	}
	got := fake.lastStart
	if got.TenantId != "t-2" || got.DefinitionId != "def-1" || got.AssetId != "asset-1" || got.Context != "{}" {
		t.Fatalf("fields not mapped: %+v", got)
	}
	if got.CorrelationId != "corr-7" {
		t.Fatalf("correlation not defaulted from meta: %q", got.CorrelationId)
	}

	// Explicit correlationID must win over the meta default.
	if _, err := f.StartPipeline(context.Background(), "def-1", "asset-1", "{}", "explicit"); err != nil {
		t.Fatalf("StartPipeline: %v", err)
	}
	if fake.lastStart.CorrelationId != "explicit" {
		t.Fatalf("explicit correlation overridden: %q", fake.lastStart.CorrelationId)
	}
}

func TestWebRTCRoomFacadeCreateDefaultsCreatedBy(t *testing.T) {
	fake := &fakeRoomClient{}
	f := &WebRTCRoomFacade{Raw: fake, meta: Metadata{TenantID: "t-3", UserID: "u-5"}}

	if _, err := f.CreateRoom(context.Background(), "lobby", 8, "{}", ""); err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	got := fake.lastCreate
	if got.TenantId != "t-3" || got.Name != "lobby" || got.MaxParticipants != 8 || got.Config != "{}" {
		t.Fatalf("fields not mapped: %+v", got)
	}
	if got.CreatedBy != "u-5" {
		t.Fatalf("createdBy not defaulted from meta UserID: %q", got.CreatedBy)
	}
}

func TestWebRTCTurnFacadeIssueCredentialsBuildsRequest(t *testing.T) {
	fake := &fakeTurnClient{}
	f := &WebRTCTurnFacade{Raw: fake, meta: Metadata{TenantID: "t-4"}}

	if _, err := f.IssueCredentials(context.Background(), "room-1", "peer-1", 300); err != nil {
		t.Fatalf("IssueCredentials: %v", err)
	}
	got := fake.lastIssue
	if got.TenantId != "t-4" || got.RoomId != "room-1" || got.PeerId != "peer-1" || got.TtlSeconds != 300 {
		t.Fatalf("fields not mapped: %+v", got)
	}
}

// TestMediaFacadesWiredOntoUdb asserts the facade fields exist with the grouped
// WebRTC sub-facades and the Signal stream method, exercising the type surface
// without dialing.
func TestMediaFacadesWiredOntoUdb(t *testing.T) {
	var u Udb
	// Field existence is compile-time; nil-check keeps the vars used and proves
	// the grouped sub-facade fields are reachable.
	if u.Storage != nil || u.Asset != nil || u.WebRTC != nil {
		t.Fatal("zero-value Udb media facades should be nil")
	}
	wf := &WebRTCFacade{}
	_ = wf.Room
	_ = wf.Peer
	_ = wf.Track
	_ = wf.Turn
	// Signal must accept a context and return the bidi stream client type.
	var _ func(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[webrtcv1.SignalRequest, webrtcv1.SignalResponse], error) = wf.Signal
}

type authOnlyNotificationServer struct {
	notificationv1.UnimplementedNotificationServiceServer
	called bool
}

func (s *authOnlyNotificationServer) SendNotification(_ context.Context, _ *notificationv1.SendNotificationRequest) (*notificationv1.SendNotificationResponse, error) {
	s.called = true
	return &notificationv1.SendNotificationResponse{}, nil
}

func TestNewUdbRoutesNotificationFacadeToAuthTarget(t *testing.T) {
	dataTarget := startTestGrpcServer(t, nil)
	authSrv := &authOnlyNotificationServer{}
	authTarget := startTestGrpcServer(t, func(s *grpc.Server) {
		notificationv1.RegisterNotificationServiceServer(s, authSrv)
	})

	u, err := NewUdb(context.Background(), Config{
		Target:     dataTarget,
		AuthTarget: authTarget,
		TenantID:   "tenant-a",
		ProjectID:  "project-a",
	})
	if err != nil {
		t.Fatalf("NewUdb: %v", err)
	}
	defer u.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := u.Notification.Send(ctx, "welcome", "user-1", nil); err != nil {
		t.Fatalf("Notification.Send should use auth target: %v", err)
	}
	if !authSrv.called {
		t.Fatal("auth-target NotificationService was not called")
	}
}

// fakeDownloadStream is a hand-rolled ServerStreamingClient[DownloadFileChunk]
// that replays a fixed slice of chunks then EOF. Only Recv is exercised; the
// other ClientStream methods satisfy the interface but are never called.
type fakeDownloadStream struct {
	grpc.ClientStream
	chunks []*storagev1.DownloadFileChunk
	i      int
}

func (s *fakeDownloadStream) Recv() (*storagev1.DownloadFileChunk, error) {
	if s.i >= len(s.chunks) {
		return nil, io.EOF
	}
	c := s.chunks[s.i]
	s.i++
	return c, nil
}

// fakeStreamingStorage records the DownloadFile streaming request and the call
// sequence so the streaming path can be proven to issue EXACTLY the DownloadFile
// RPC (and never the presigned GetDownloadUrl).
type fakeStreamingStorage struct {
	storagev1.StorageServiceClient
	seq         []string
	lastDownReq *storagev1.DownloadFileRequest
	chunks      []*storagev1.DownloadFileChunk
}

func (f *fakeStreamingStorage) DownloadFile(_ context.Context, in *storagev1.DownloadFileRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[storagev1.DownloadFileChunk], error) {
	f.seq = append(f.seq, "DownloadFile")
	f.lastDownReq = in
	return &fakeDownloadStream{chunks: f.chunks}, nil
}

func (f *fakeStreamingStorage) GetDownloadUrl(_ context.Context, _ *storagev1.GetDownloadUrlRequest, _ ...grpc.CallOption) (*storagev1.GetDownloadUrlResponse, error) {
	f.seq = append(f.seq, "GetDownloadUrl")
	return &storagev1.GetDownloadUrlResponse{}, nil
}

// TestStorageDownloadFileBytesStreams proves DownloadFileBytes issues EXACTLY the
// server-streaming DownloadFile RPC (no presigned GetDownloadUrl), threads the
// file id + advisory chunk size + tenant, and reassembles the bounded chunk
// stream into the full payload with first-chunk metadata.
func TestStorageDownloadFileBytesStreams(t *testing.T) {
	ct, et := "text/plain", "etag-1"
	var ts int64 = 11
	fs := &fakeStreamingStorage{
		chunks: []*storagev1.DownloadFileChunk{
			{Data: []byte("hello "), ContentType: &ct, TotalSize: &ts, Etag: &et},
			{Data: []byte("world")},
		},
	}
	sf := &StorageFacade{Raw: fs, meta: Metadata{TenantID: "tenant-7"}}

	res, err := sf.DownloadFileBytes(context.Background(), "file-9", WithDownloadChunkSize(65536))
	if err != nil {
		t.Fatalf("DownloadFileBytes: %v", err)
	}
	if !reflect.DeepEqual(fs.seq, []string{"DownloadFile"}) {
		t.Fatalf("streaming path must emit EXACTLY one DownloadFile RPC (no presigned GetDownloadUrl): %v", fs.seq)
	}
	if got := fs.lastDownReq; got.GetFileId() != "file-9" || got.GetTenantId() != "tenant-7" || got.GetChunkSizeBytes() != 65536 {
		t.Fatalf("request fields not threaded: %+v", got)
	}
	if !bytes.Equal(res.Data, []byte("hello world")) {
		t.Fatalf("chunks not reassembled: %q", res.Data)
	}
	if res.ContentType != "text/plain" || res.TotalSize != 11 || res.ETag != "etag-1" {
		t.Fatalf("first-chunk metadata not captured: %+v", res)
	}
}

// TestStorageDownloadFileBytesMaxBytes proves the MaxBytes cap fails closed once
// reassembly would exceed it.
func TestStorageDownloadFileBytesMaxBytes(t *testing.T) {
	fs := &fakeStreamingStorage{
		chunks: []*storagev1.DownloadFileChunk{
			{Data: []byte("0123456789")},
			{Data: []byte("0123456789")},
		},
	}
	sf := &StorageFacade{Raw: fs, meta: Metadata{TenantID: "t"}}
	if _, err := sf.DownloadFileBytes(context.Background(), "f", WithMaxDownloadBytes(15)); err == nil {
		t.Fatal("DownloadFileBytes should fail when reassembly exceeds MaxBytes")
	}
}

// TestStorageDownloadFilePresignedUnchanged proves the presigned DownloadFile
// accessor still emits exactly one GetDownloadUrl and never the streaming RPC.
func TestStorageDownloadFilePresignedUnchanged(t *testing.T) {
	fs := &fakeStreamingStorage{}
	sf := &StorageFacade{Raw: fs, meta: Metadata{TenantID: "t"}}
	if _, err := sf.DownloadFile(context.Background(), "file-1", 10); err != nil {
		t.Fatalf("DownloadFile: %v", err)
	}
	if !reflect.DeepEqual(fs.seq, []string{"GetDownloadUrl"}) {
		t.Fatalf("presigned path must emit EXACTLY one GetDownloadUrl (no streaming DownloadFile): %v", fs.seq)
	}
}

func startTestGrpcServer(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	if register != nil {
		register(server)
	}
	go func() {
		_ = server.Serve(lis)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})
	return lis.Addr().String()
}
