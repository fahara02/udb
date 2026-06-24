package udbclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	assetentityv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/asset/entity/v1"
	assetv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/asset/services/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/services/v1"
	webrtcv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/webrtc/services/v1"
	"google.golang.org/grpc"
)

// httpDoer is the minimal HTTP client interface UploadFile uses for the
// presigned PUT. It is a package var so the mock-transport test can intercept
// the byte transfer without a real network round trip; production uses
// http.DefaultClient.
type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

var uploadHTTPDoer httpDoer = http.DefaultClient

// ── Phase 7 M8: media-plane convenience facades ──────────────────────────────
//
// Storage, Asset, and WebRTC live on the same native control-plane listener as
// the auth/tenant/notification services, so these facades dial nothing of their
// own — NewUdb constructs them over the existing broker connection. Each facade
// wraps the raw generated client (reachable via .Raw) and adds thin helpers that
// seed TenantId/ProjectId from the caller Metadata. Only RPCs the generated
// stub actually exposes are wrapped.

// ── Storage ──────────────────────────────────────────────────────────────────

// StorageFacade wraps StorageServiceClient.
type StorageFacade struct {
	Raw  storagev1.StorageServiceClient
	meta Metadata

	// MaxUploadBytes caps the in-memory UploadFile byte path; zero = unlimited.
	// UploadFile returns a typed error BEFORE any RPC when the payload exceeds it.
	MaxUploadBytes int64
}

// UploadOptions carries the non-positional UploadFile inputs. The CANONICAL
// cross-language signature keeps filename + bytes positional and everything else
// here (matching the TS/Python uploadFile(filename, bytes, options) shape).
type UploadOptions struct {
	ContentType string
	FileType    string
	Checksum    string // optional integrity checksum persisted on finalize
	ETag        string // optional object etag asserted on finalize
}

// UploadOption is a functional option mutating UploadOptions.
type UploadOption func(*UploadOptions)

// WithContentType sets the upload Content-Type (used for the PUT header and the
// RegisterUpload request).
func WithContentType(ct string) UploadOption { return func(o *UploadOptions) { o.ContentType = ct } }

// WithFileType sets the logical file type bucket.
func WithFileType(ft string) UploadOption { return func(o *UploadOptions) { o.FileType = ft } }

// WithChecksum sets a content checksum persisted on finalize.
func WithChecksum(c string) UploadOption { return func(o *UploadOptions) { o.Checksum = c } }

// WithETag asserts an object etag on finalize.
func WithETag(e string) UploadOption { return func(o *UploadOptions) { o.ETag = e } }

// UploadFile is the combined register -> PUT -> finalize helper. It performs
// EXACTLY those three steps with NO hidden Get/List proof read (perf guardrail):
//
//  1. RegisterUpload(filename, len(data)) -> file id + presigned upload_url
//  2. HTTP PUT the bytes to upload_url (only when the broker returned one)
//  3. FinalizeUpload(file id, len(data)) with the optional checksum/etag
//
// filename and data are positional; all other inputs live in UploadOptions via
// functional options. The MaxUploadBytes cap is enforced before any RPC.
func (f *StorageFacade) UploadFile(ctx context.Context, filename string, data []byte, opts ...UploadOption) (*storagev1.FinalizeUploadResponse, error) {
	var o UploadOptions
	for _, opt := range opts {
		opt(&o)
	}
	if f.MaxUploadBytes > 0 && int64(len(data)) > f.MaxUploadBytes {
		return nil, fmt.Errorf("udb: upload %q is %d bytes, exceeds MaxUploadBytes %d", filename, len(data), f.MaxUploadBytes)
	}

	reg, err := f.RegisterUpload(ctx, filename, o.ContentType, o.FileType, int64(len(data)))
	if err != nil {
		return nil, err
	}
	if url := reg.GetUploadUrl(); url != "" {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("udb: build upload PUT: %w", err)
		}
		if o.ContentType != "" {
			req.Header.Set("Content-Type", o.ContentType)
		}
		resp, err := uploadHTTPDoer.Do(req)
		if err != nil {
			return nil, fmt.Errorf("udb: upload PUT: %w", err)
		}
		if resp.Body != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("udb: upload PUT returned HTTP %d", resp.StatusCode)
		}
	}
	fin := &storagev1.FinalizeUploadRequest{
		TenantId:  f.meta.TenantID,
		FileId:    reg.GetFileId(),
		SizeBytes: int64(len(data)),
	}
	if o.Checksum != "" {
		fin.Checksum = &o.Checksum
	}
	if o.ETag != "" {
		fin.Etag = &o.ETag
	}
	return f.Raw.FinalizeUpload(ctx, fin)
}

// RegisterUpload reserves a file id + presigned upload target and runs the
// pre-upload tenant quota check against sizeBytes. tenant/project default to the
// caller Metadata.
func (f *StorageFacade) RegisterUpload(ctx context.Context, filename, contentType, fileType string, sizeBytes int64) (*storagev1.RegisterUploadResponse, error) {
	req := &storagev1.RegisterUploadRequest{
		TenantId:    f.meta.TenantID,
		Filename:    filename,
		ContentType: contentType,
		FileType:    fileType,
		SizeBytes:   sizeBytes,
	}
	if isUUIDString(f.meta.ProjectID) {
		req.ProjectId = f.meta.ProjectID
	}
	return f.Raw.RegisterUpload(ctx, req)
}

func isUUIDString(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != 36 {
		return false
	}
	for i, r := range value {
		switch i {
		case 8, 13, 18, 23:
			if r != '-' {
				return false
			}
		default:
			if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
				return false
			}
		}
	}
	return true
}

// FinalizeUpload marks a registered file as uploaded, persisting its actual
// sizeBytes. tenant defaults to the caller Metadata.
func (f *StorageFacade) FinalizeUpload(ctx context.Context, fileID string, sizeBytes int64) (*storagev1.FinalizeUploadResponse, error) {
	return f.Raw.FinalizeUpload(ctx, &storagev1.FinalizeUploadRequest{
		TenantId:  f.meta.TenantID,
		FileId:    fileID,
		SizeBytes: sizeBytes,
	})
}

// GetDownloadUrl returns a presigned download URL valid for expiresInMinutes
// (zero lets the server choose its default). tenant defaults to the Metadata.
func (f *StorageFacade) GetDownloadUrl(ctx context.Context, fileID string, expiresInMinutes int32) (*storagev1.GetDownloadUrlResponse, error) {
	return f.Raw.GetDownloadUrl(ctx, &storagev1.GetDownloadUrlRequest{
		TenantId:         f.meta.TenantID,
		FileId:           fileID,
		ExpiresInMinutes: expiresInMinutes,
	})
}

// DownloadFile is the canonical naming-contract download accessor and the
// PREFERRED happy path: it mints a time-limited presigned download URL for
// fileID so the bytes never transit the broker. It emits EXACTLY one
// GetDownloadUrl RPC (no GetFile probe) — a fileId-first alias of GetDownloadUrl.
// expiresInMinutes of zero lets the server choose its default; tenant defaults
// to the caller Metadata. Callers that cannot use a presigned HTTP URL and need
// the bytes returned through the broker use DownloadFileBytes, which drives the
// server-streaming DownloadFile RPC instead.
func (f *StorageFacade) DownloadFile(ctx context.Context, fileID string, expiresInMinutes int32) (*storagev1.GetDownloadUrlResponse, error) {
	return f.GetDownloadUrl(ctx, fileID, expiresInMinutes)
}

// DownloadOptions carries the non-positional DownloadFileBytes inputs.
type DownloadOptions struct {
	// ChunkSizeBytes is the advisory server-side chunk size for the streaming
	// fallback; zero lets the server choose. It does not bound reassembly.
	ChunkSizeBytes int32
	// MaxBytes caps the reassembled payload; the stream is aborted with a typed
	// error once the accumulated bytes would exceed it. Zero = unlimited.
	MaxBytes int64
}

// DownloadOption is a functional option mutating DownloadOptions.
type DownloadOption func(*DownloadOptions)

// WithDownloadChunkSize sets the advisory streaming chunk size.
func WithDownloadChunkSize(n int32) DownloadOption {
	return func(o *DownloadOptions) { o.ChunkSizeBytes = n }
}

// WithMaxDownloadBytes caps the reassembled byte payload.
func WithMaxDownloadBytes(n int64) DownloadOption {
	return func(o *DownloadOptions) { o.MaxBytes = n }
}

// DownloadResult is the reassembled output of DownloadFileBytes: the full file
// bytes plus the content-type/total-size/etag metadata the first chunk carries.
type DownloadResult struct {
	Data        []byte
	ContentType string
	TotalSize   int64
	ETag        string
}

// DownloadFileBytes is the byte-fetch download path. The presigned URL flow
// (DownloadFile -> GetDownloadUrl) is the happy path and keeps bytes OUT of the
// broker; DownloadFileBytes is for callers that cannot resolve/use a presigned
// HTTP URL and need the bytes back through the broker. It calls the new
// server-streaming DownloadFile RPC and reassembles the bounded DownloadFileChunk
// stream into a single buffer. tenant defaults to the caller Metadata. The
// MaxBytes cap (option) fails closed with a typed error before the buffer can
// grow past it.
func (f *StorageFacade) DownloadFileBytes(ctx context.Context, fileID string, opts ...DownloadOption) (*DownloadResult, error) {
	var o DownloadOptions
	for _, opt := range opts {
		opt(&o)
	}
	req := &storagev1.DownloadFileRequest{
		TenantId: f.meta.TenantID,
		FileId:   fileID,
	}
	if o.ChunkSizeBytes != 0 {
		req.ChunkSizeBytes = &o.ChunkSizeBytes
	}
	stream, err := f.Raw.DownloadFile(ctx, req)
	if err != nil {
		return nil, err
	}
	res := &DownloadResult{}
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("udb: download stream: %w", err)
		}
		// The first chunk carries the file metadata; later chunks may repeat or
		// leave it empty — keep the first non-empty values.
		if res.ContentType == "" {
			res.ContentType = chunk.GetContentType()
		}
		if res.TotalSize == 0 {
			res.TotalSize = chunk.GetTotalSize()
		}
		if res.ETag == "" {
			res.ETag = chunk.GetEtag()
		}
		data := chunk.GetData()
		if o.MaxBytes > 0 && int64(len(res.Data))+int64(len(data)) > o.MaxBytes {
			return nil, fmt.Errorf("udb: download %q exceeds MaxBytes %d", fileID, o.MaxBytes)
		}
		res.Data = append(res.Data, data...)
	}
	return res, nil
}

// GetFile fetches file metadata by id. tenant defaults to the Metadata.
func (f *StorageFacade) GetFile(ctx context.Context, fileID string) (*storagev1.GetFileResponse, error) {
	return f.Raw.GetFile(ctx, &storagev1.GetFileRequest{
		TenantId: f.meta.TenantID,
		FileId:   fileID,
	})
}

// UpdateFile updates mutable file metadata. tenant defaults to the Metadata.
func (f *StorageFacade) UpdateFile(ctx context.Context, fileID, filename, contentType, fileType string, isPublic bool) (*storagev1.UpdateFileResponse, error) {
	return f.Raw.UpdateFile(ctx, &storagev1.UpdateFileRequest{
		TenantId:    f.meta.TenantID,
		FileId:      fileID,
		Filename:    filename,
		ContentType: contentType,
		FileType:    fileType,
		IsPublic:    &isPublic,
	})
}

// DeleteFile removes a file (and schedules its object for GC). tenant defaults
// to the Metadata.
func (f *StorageFacade) DeleteFile(ctx context.Context, fileID string) (*storagev1.DeleteFileResponse, error) {
	return f.Raw.DeleteFile(ctx, &storagev1.DeleteFileRequest{
		TenantId: f.meta.TenantID,
		FileId:   fileID,
	})
}

// ListFiles paginates files for the tenant, optionally filtered by fileType.
// page/pageSize of zero use the server defaults. tenant defaults to the Metadata.
func (f *StorageFacade) ListFiles(ctx context.Context, fileType string, page, pageSize int32) (*storagev1.ListFilesResponse, error) {
	return f.Raw.ListFiles(ctx, &storagev1.ListFilesRequest{
		TenantId: f.meta.TenantID,
		FileType: fileType,
		Page:     page,
		PageSize: pageSize,
	})
}

// ── Asset ────────────────────────────────────────────────────────────────────

// AssetFacade wraps AssetServiceClient.
type AssetFacade struct {
	Raw  assetv1.AssetServiceClient
	meta Metadata
}

// CreatePipelineDefinition registers a reusable processing pipeline. steps is a
// JSON array of step descriptors. tenant defaults to the caller Metadata.
func (f *AssetFacade) CreatePipelineDefinition(ctx context.Context, name, description, mediaType, stepsJSON string, version int32) (*assetv1.CreatePipelineDefinitionResponse, error) {
	return f.Raw.CreatePipelineDefinition(ctx, &assetv1.CreatePipelineDefinitionRequest{
		TenantId:    f.meta.TenantID,
		Name:        name,
		Description: description,
		MediaType:   mediaType,
		Steps:       stepsJSON,
		Version:     version,
	})
}

// GetPipelineDefinition fetches a pipeline definition by id. tenant defaults to
// the Metadata.
func (f *AssetFacade) GetPipelineDefinition(ctx context.Context, definitionID string) (*assetv1.GetPipelineDefinitionResponse, error) {
	return f.Raw.GetPipelineDefinition(ctx, &assetv1.GetPipelineDefinitionRequest{
		TenantId:     f.meta.TenantID,
		DefinitionId: definitionID,
	})
}

// RegisterAsset records an asset backed by a stored fileID. metadataJSON is an
// optional JSON blob. tenant/project default to the caller Metadata.
func (f *AssetFacade) RegisterAsset(ctx context.Context, fileID, name, mediaType, metadataJSON string) (*assetv1.RegisterAssetResponse, error) {
	req := &assetv1.RegisterAssetRequest{
		TenantId:  f.meta.TenantID,
		FileId:    fileID,
		Name:      name,
		MediaType: mediaType,
		Metadata:  metadataJSON,
	}
	// Same UUID-column contract as RegisterUpload: the server persists
	// project_id through logical_uuid_or_null, so a human project code (e.g.
	// "private") yields InvalidArgument. Only send it when it's a canonical UUID;
	// otherwise omit it (project is optional server-side).
	if isUUIDString(f.meta.ProjectID) {
		req.ProjectId = f.meta.ProjectID
	}
	return f.Raw.RegisterAsset(ctx, req)
}

// StartPipeline launches a pipeline definition against an asset. contextJSON is
// an optional JSON context blob; correlationID defaults to the caller Metadata
// CorrelationID when empty. tenant defaults to the Metadata.
func (f *AssetFacade) StartPipeline(ctx context.Context, definitionID, assetID, contextJSON, correlationID string) (*assetv1.StartPipelineResponse, error) {
	if correlationID == "" {
		correlationID = f.meta.CorrelationID
	}
	return f.Raw.StartPipeline(ctx, &assetv1.StartPipelineRequest{
		TenantId:      f.meta.TenantID,
		DefinitionId:  definitionID,
		AssetId:       assetID,
		Context:       contextJSON,
		CorrelationId: correlationID,
	})
}

// GetPipeline fetches a running/completed pipeline instance by id. tenant
// defaults to the Metadata.
func (f *AssetFacade) GetPipeline(ctx context.Context, instanceID string) (*assetv1.GetPipelineResponse, error) {
	return f.Raw.GetPipeline(ctx, &assetv1.GetPipelineRequest{
		TenantId:   f.meta.TenantID,
		InstanceId: instanceID,
	})
}

// CompleteStep reports the outcome of a pipeline step. status is one of
// COMPLETED | SKIPPED | FAILED; resultJSON and errorMessage are optional.
// tenant defaults to the Metadata.
func (f *AssetFacade) CompleteStep(ctx context.Context, stepID, status, resultJSON, errorMessage string) (*assetv1.CompleteStepResponse, error) {
	return f.Raw.CompleteStep(ctx, &assetv1.CompleteStepRequest{
		TenantId:     f.meta.TenantID,
		StepId:       stepID,
		Status:       status,
		Result:       resultJSON,
		ErrorMessage: errorMessage,
	})
}

// ListAssets paginates assets for the tenant, optionally filtered by mediaType
// and status. page/pageSize of zero use the server defaults. tenant defaults to
// the Metadata.
func (f *AssetFacade) ListAssets(ctx context.Context, mediaType, status string, page, pageSize int32) (*assetv1.ListAssetsResponse, error) {
	return f.Raw.ListAssets(ctx, &assetv1.ListAssetsRequest{
		TenantId:  f.meta.TenantID,
		MediaType: mediaType,
		Status:    status,
		Page:      page,
		PageSize:  pageSize,
	})
}

// GetAsset fetches an asset by id. tenant defaults to the Metadata.
func (f *AssetFacade) GetAsset(ctx context.Context, assetID string) (*assetv1.GetAssetResponse, error) {
	return f.Raw.GetAsset(ctx, &assetv1.GetAssetRequest{
		TenantId: f.meta.TenantID,
		AssetId:  assetID,
	})
}

// ── Asset workflow helpers (chapter 13.6) ────────────────────────────────────

// DefinePipeline is a thin wrapper over CreatePipelineDefinition taking the step
// list as a JSON array (the producer-side define helper). tenant defaults to the
// Metadata.
func (f *AssetFacade) DefinePipeline(ctx context.Context, name, description, mediaType, stepsJSON string, version int32) (*assetv1.CreatePipelineDefinitionResponse, error) {
	return f.CreatePipelineDefinition(ctx, name, description, mediaType, stepsJSON, version)
}

// RegisterFromStorageFile registers an asset bound to a storage file id (the
// producer-side register helper over RegisterAsset). tenant/project default to
// the Metadata.
func (f *AssetFacade) RegisterFromStorageFile(ctx context.Context, fileID, name, mediaType, metadataJSON string) (*assetv1.RegisterAssetResponse, error) {
	return f.RegisterAsset(ctx, fileID, name, mediaType, metadataJSON)
}

// StartAndWaitResult carries a StartAndWait outcome: the inline steps the
// StartPipeline response already returned (no GetPipeline proof read needed) and
// the terminal instance status reached by the bounded status poll.
type StartAndWaitResult struct {
	InstanceID string
	Steps      []*assetentityv1.PipelineStep
	Status     assetentityv1.PipelineStatus
}

func pipelineTerminal(s assetentityv1.PipelineStatus) bool {
	return s == assetentityv1.PipelineStatus_PIPELINE_STATUS_COMPLETED ||
		s == assetentityv1.PipelineStatus_PIPELINE_STATUS_FAILED
}

// StartAndWait starts a pipeline (consuming the inline steps from the
// StartPipeline response — NOT a follow-up GetPipeline proof read) and then polls
// instance status to a terminal state (COMPLETED/FAILED) or until the deadline.
// Status polling uses only GetPipeline reads, bounded and cancellation-aware —
// never a fixed sleep loop.
func (f *AssetFacade) StartAndWait(ctx context.Context, definitionID, assetID, contextJSON, correlationID string, deadline time.Duration) (*StartAndWaitResult, error) {
	start, err := f.StartPipeline(ctx, definitionID, assetID, contextJSON, correlationID)
	if err != nil {
		return nil, err
	}
	res := &StartAndWaitResult{
		InstanceID: start.GetInstanceId(),
		Steps:      start.GetSteps(), // inline steps from 04.2.1 — no GetPipeline here
	}
	if deadline > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, deadline)
		defer cancel()
	}
	const pollInterval = 150 * time.Millisecond
	for {
		got, err := f.GetPipeline(ctx, res.InstanceID)
		if err != nil {
			return res, err
		}
		res.Status = got.GetInstance().GetStatus()
		if pipelineTerminal(res.Status) {
			return res, nil
		}
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// ── WebRTC ───────────────────────────────────────────────────────────────────
//
// The WebRTC proto is split into five generated service clients (Room, Peer,
// Track, Turn, Signaling). WebRTCFacade groups the CRUD-shaped ones into named
// sub-facades and exposes the bidi Signal stream directly. Each sub-facade and
// the raw signaling client are reachable for any RPC not wrapped here.

// WebRTCFacade groups the WebRTC sub-facades.
type WebRTCFacade struct {
	Room  *WebRTCRoomFacade
	Peer  *WebRTCPeerFacade
	Track *WebRTCTrackFacade
	Turn  *WebRTCTurnFacade

	// RawSignaling is the bidi signaling client backing Signal.
	RawSignaling webrtcv1.SignalingServiceClient

	meta Metadata
}

// Signal opens the bidirectional SDP/ICE signaling stream. The caller drives it
// with stream.Send / stream.Recv and closes it via stream.CloseSend. This is the
// honest surface for a bidi stream — the facade does not buffer or fake frames.
func (f *WebRTCFacade) Signal(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[webrtcv1.SignalRequest, webrtcv1.SignalResponse], error) {
	return f.RawSignaling.Signal(ctx, opts...)
}

// ── joinSession wrapper (chapter 13.4) ───────────────────────────────────────

// Session is the handle JoinSession returns: the atomically-joined peer + ICE +
// existing peers from the single JoinSession RPC, plus the live signaling stream.
// Leave closes the stream and removes the peer from the room.
type Session struct {
	facade       *WebRTCFacade
	roomID       string
	Result       *webrtcv1.JoinSessionResponse
	Stream       grpc.BidiStreamingClient[webrtcv1.SignalRequest, webrtcv1.SignalResponse]
	cancelSignal context.CancelFunc
}

// PeerID returns the joined peer's id.
func (s *Session) PeerID() string {
	if s.Result == nil || s.Result.GetPeer() == nil {
		return ""
	}
	return s.Result.GetPeer().GetPeerId()
}

// Leave closes the signaling stream and issues LeaveRoom. Safe to call once.
func (s *Session) Leave(ctx context.Context) error {
	if s.Stream != nil {
		_ = s.Stream.CloseSend()
	}
	if s.cancelSignal != nil {
		s.cancelSignal()
	}
	if s.facade == nil || s.PeerID() == "" {
		return nil
	}
	_, err := s.facade.Peer.LeaveRoom(ctx, s.roomID, s.PeerID())
	return err
}

// JoinSession joins a room atomically via the JoinSession RPC (peer + ICE +
// existing peers in ONE call — no SDK-side JoinRoom+IssueCredentials fan-out),
// then opens the bidi signaling stream for the session. tenant defaults from the
// facade Metadata. The caller drives the returned Stream and ends the session
// with Leave. Reconnect is the caller's concern and is denied after a first
// observed response until the broker exposes a resume token (guardrail).
func (f *WebRTCFacade) JoinSession(ctx context.Context, roomID, displayName, metadataJSON, userAgent string, ttlSeconds int32) (*Session, error) {
	resp, err := f.Peer.Raw.JoinSession(ctx, &webrtcv1.JoinSessionRequest{
		TenantId:    f.meta.TenantID,
		RoomId:      roomID,
		DisplayName: displayName,
		Metadata:    metadataJSON,
		UserAgent:   userAgent,
		TtlSeconds:  ttlSeconds,
	})
	if err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := f.Signal(streamCtx)
	if err != nil {
		cancel()
		return nil, err
	}
	return &Session{
		facade:       f,
		roomID:       roomID,
		Result:       resp,
		Stream:       stream,
		cancelSignal: cancel,
	}, nil
}

// WebRTCRoomFacade wraps RoomServiceClient.
type WebRTCRoomFacade struct {
	Raw  webrtcv1.RoomServiceClient
	meta Metadata
}

// CreateRoom provisions a room. configJSON is an optional JSON config; createdBy
// defaults to the caller Metadata UserID when empty. tenant defaults to the
// Metadata.
func (f *WebRTCRoomFacade) CreateRoom(ctx context.Context, name string, maxParticipants int32, configJSON, createdBy string) (*webrtcv1.CreateRoomResponse, error) {
	if createdBy == "" {
		createdBy = f.meta.UserID
	}
	return f.Raw.CreateRoom(ctx, &webrtcv1.CreateRoomRequest{
		TenantId:        f.meta.TenantID,
		Name:            name,
		MaxParticipants: maxParticipants,
		Config:          configJSON,
		CreatedBy:       createdBy,
	})
}

// GetRoom fetches a room by id. tenant defaults to the Metadata.
func (f *WebRTCRoomFacade) GetRoom(ctx context.Context, roomID string) (*webrtcv1.GetRoomResponse, error) {
	return f.Raw.GetRoom(ctx, &webrtcv1.GetRoomRequest{
		TenantId: f.meta.TenantID,
		RoomId:   roomID,
	})
}

// UpdateRoom updates a room's name/state/config. tenant defaults to the Metadata.
func (f *WebRTCRoomFacade) UpdateRoom(ctx context.Context, roomID, name, state, configJSON string) (*webrtcv1.UpdateRoomResponse, error) {
	return f.Raw.UpdateRoom(ctx, &webrtcv1.UpdateRoomRequest{
		TenantId: f.meta.TenantID,
		RoomId:   roomID,
		Name:     name,
		State:    state,
		Config:   configJSON,
	})
}

// CloseRoom closes a room by id. tenant defaults to the Metadata.
func (f *WebRTCRoomFacade) CloseRoom(ctx context.Context, roomID string) (*webrtcv1.CloseRoomResponse, error) {
	return f.Raw.CloseRoom(ctx, &webrtcv1.CloseRoomRequest{
		TenantId: f.meta.TenantID,
		RoomId:   roomID,
	})
}

// ListRooms paginates rooms for the tenant, optionally filtered by state.
// page/pageSize of zero use the server defaults. tenant defaults to the Metadata.
func (f *WebRTCRoomFacade) ListRooms(ctx context.Context, state string, page, pageSize int32) (*webrtcv1.ListRoomsResponse, error) {
	return f.Raw.ListRooms(ctx, &webrtcv1.ListRoomsRequest{
		TenantId: f.meta.TenantID,
		State:    state,
		Page:     page,
		PageSize: pageSize,
	})
}

// WebRTCPeerFacade wraps PeerServiceClient.
type WebRTCPeerFacade struct {
	Raw  webrtcv1.PeerServiceClient
	meta Metadata
}

// JoinRoom adds a peer to a room. metadataJSON is an optional JSON blob. tenant
// defaults to the Metadata.
func (f *WebRTCPeerFacade) JoinRoom(ctx context.Context, roomID, displayName, metadataJSON, userAgent string) (*webrtcv1.JoinRoomResponse, error) {
	return f.Raw.JoinRoom(ctx, &webrtcv1.JoinRoomRequest{
		TenantId:    f.meta.TenantID,
		RoomId:      roomID,
		DisplayName: displayName,
		Metadata:    metadataJSON,
		UserAgent:   userAgent,
	})
}

// LeaveRoom removes a peer from a room. tenant defaults to the Metadata.
func (f *WebRTCPeerFacade) LeaveRoom(ctx context.Context, roomID, peerID string) (*webrtcv1.LeaveRoomResponse, error) {
	return f.Raw.LeaveRoom(ctx, &webrtcv1.LeaveRoomRequest{
		TenantId: f.meta.TenantID,
		RoomId:   roomID,
		PeerId:   peerID,
	})
}

// GetPeer fetches a peer by id. tenant defaults to the Metadata.
func (f *WebRTCPeerFacade) GetPeer(ctx context.Context, peerID string) (*webrtcv1.GetPeerResponse, error) {
	return f.Raw.GetPeer(ctx, &webrtcv1.GetPeerRequest{
		TenantId: f.meta.TenantID,
		PeerId:   peerID,
	})
}

// ListPeers lists peers in a room, optionally filtered by state. tenant defaults
// to the Metadata.
func (f *WebRTCPeerFacade) ListPeers(ctx context.Context, roomID, state string) (*webrtcv1.ListPeersResponse, error) {
	return f.Raw.ListPeers(ctx, &webrtcv1.ListPeersRequest{
		TenantId: f.meta.TenantID,
		RoomId:   roomID,
		State:    state,
	})
}

// WebRTCTrackFacade wraps TrackServiceClient.
type WebRTCTrackFacade struct {
	Raw  webrtcv1.TrackServiceClient
	meta Metadata
}

// PublishTrack publishes a media track for a peer. settingsJSON/metadataJSON are
// optional JSON blobs. tenant defaults to the Metadata.
func (f *WebRTCTrackFacade) PublishTrack(ctx context.Context, roomID, peerID, kind, label, settingsJSON, metadataJSON string) (*webrtcv1.PublishTrackResponse, error) {
	return f.Raw.PublishTrack(ctx, &webrtcv1.PublishTrackRequest{
		TenantId: f.meta.TenantID,
		RoomId:   roomID,
		PeerId:   peerID,
		Kind:     kind,
		Label:    label,
		Settings: settingsJSON,
		Metadata: metadataJSON,
	})
}

// UnpublishTrack removes a published track by id. tenant defaults to the Metadata.
func (f *WebRTCTrackFacade) UnpublishTrack(ctx context.Context, trackID string) (*webrtcv1.UnpublishTrackResponse, error) {
	return f.Raw.UnpublishTrack(ctx, &webrtcv1.UnpublishTrackRequest{
		TenantId: f.meta.TenantID,
		TrackId:  trackID,
	})
}

// MuteTrack sets the muted state of a track. tenant defaults to the Metadata.
func (f *WebRTCTrackFacade) MuteTrack(ctx context.Context, trackID string, muted bool) (*webrtcv1.MuteTrackResponse, error) {
	return f.Raw.MuteTrack(ctx, &webrtcv1.MuteTrackRequest{
		TenantId: f.meta.TenantID,
		TrackId:  trackID,
		Muted:    muted,
	})
}

// ListTracks lists tracks in a room, optionally filtered by peerID and kind.
// tenant defaults to the Metadata.
func (f *WebRTCTrackFacade) ListTracks(ctx context.Context, roomID, peerID, kind string) (*webrtcv1.ListTracksResponse, error) {
	return f.Raw.ListTracks(ctx, &webrtcv1.ListTracksRequest{
		TenantId: f.meta.TenantID,
		RoomId:   roomID,
		PeerId:   peerID,
		Kind:     kind,
	})
}

// WebRTCTurnFacade wraps TurnServiceClient.
type WebRTCTurnFacade struct {
	Raw  webrtcv1.TurnServiceClient
	meta Metadata
}

// IssueCredentials mints short-lived TURN credentials for a peer in a room.
// ttlSeconds of zero lets the server choose its default. tenant defaults to the
// Metadata.
func (f *WebRTCTurnFacade) IssueCredentials(ctx context.Context, roomID, peerID string, ttlSeconds int32) (*webrtcv1.IssueCredentialsResponse, error) {
	return f.Raw.IssueCredentials(ctx, &webrtcv1.IssueCredentialsRequest{
		TenantId:   f.meta.TenantID,
		RoomId:     roomID,
		PeerId:     peerID,
		TtlSeconds: ttlSeconds,
	})
}

// newWebRTCFacade builds the grouped WebRTC facade over a single broker conn.
func newWebRTCFacade(conn grpc.ClientConnInterface, meta Metadata) *WebRTCFacade {
	return &WebRTCFacade{
		Room:         &WebRTCRoomFacade{Raw: webrtcv1.NewRoomServiceClient(conn), meta: meta},
		Peer:         &WebRTCPeerFacade{Raw: webrtcv1.NewPeerServiceClient(conn), meta: meta},
		Track:        &WebRTCTrackFacade{Raw: webrtcv1.NewTrackServiceClient(conn), meta: meta},
		Turn:         &WebRTCTurnFacade{Raw: webrtcv1.NewTurnServiceClient(conn), meta: meta},
		RawSignaling: webrtcv1.NewSignalingServiceClient(conn),
		meta:         meta,
	}
}
