package udbclient

import (
	"context"

	assetv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/asset/services/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/services/v1"
	webrtcv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/webrtc/services/v1"
	"google.golang.org/grpc"
)

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
}

// RegisterUpload reserves a file id + presigned upload target and runs the
// pre-upload tenant quota check against sizeBytes. tenant/project default to the
// caller Metadata.
func (f *StorageFacade) RegisterUpload(ctx context.Context, filename, contentType, fileType string, sizeBytes int64) (*storagev1.RegisterUploadResponse, error) {
	return f.Raw.RegisterUpload(ctx, &storagev1.RegisterUploadRequest{
		TenantId:    f.meta.TenantID,
		ProjectId:   f.meta.ProjectID,
		Filename:    filename,
		ContentType: contentType,
		FileType:    fileType,
		SizeBytes:   sizeBytes,
	})
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
	return f.Raw.RegisterAsset(ctx, &assetv1.RegisterAssetRequest{
		TenantId:  f.meta.TenantID,
		ProjectId: f.meta.ProjectID,
		FileId:    fileID,
		Name:      name,
		MediaType: mediaType,
		Metadata:  metadataJSON,
	})
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
