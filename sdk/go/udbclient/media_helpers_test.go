package udbclient

import (
	"context"
	"reflect"
	"testing"

	assetentityv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/asset/entity/v1"
	assetv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/asset/services/v1"
	notificationentityv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/notification/entity/v1"
	notificationv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/notification/services/v1"
	webrtcentityv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/webrtc/entity/v1"
	webrtcv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/webrtc/services/v1"
	"google.golang.org/grpc"
)

// ── WebRTC JoinSession (13.4) ────────────────────────────────────────────────

type fakePeerClient struct {
	webrtcv1.PeerServiceClient
	seq       *[]string
	lastJoin  *webrtcv1.JoinSessionRequest
	lastLeave *webrtcv1.LeaveRoomRequest
}

func (c *fakePeerClient) JoinSession(_ context.Context, in *webrtcv1.JoinSessionRequest, _ ...grpc.CallOption) (*webrtcv1.JoinSessionResponse, error) {
	*c.seq = append(*c.seq, "JoinSession")
	c.lastJoin = in
	return &webrtcv1.JoinSessionResponse{Peer: &webrtcentityv1.Peer{PeerId: "peer-1"}}, nil
}

func (c *fakePeerClient) LeaveRoom(_ context.Context, in *webrtcv1.LeaveRoomRequest, _ ...grpc.CallOption) (*webrtcv1.LeaveRoomResponse, error) {
	*c.seq = append(*c.seq, "LeaveRoom")
	c.lastLeave = in
	return &webrtcv1.LeaveRoomResponse{}, nil
}

// fakeSignaling returns a no-op bidi stream.
type fakeSignaling struct {
	webrtcv1.SignalingServiceClient
	seq *[]string
}

type fakeSignalStream struct {
	grpc.BidiStreamingClient[webrtcv1.SignalRequest, webrtcv1.SignalResponse]
	ctx context.Context
}

func (s *fakeSignalStream) CloseSend() error         { return nil }
func (s *fakeSignalStream) Context() context.Context { return s.ctx }

func (c *fakeSignaling) Signal(ctx context.Context, _ ...grpc.CallOption) (grpc.BidiStreamingClient[webrtcv1.SignalRequest, webrtcv1.SignalResponse], error) {
	*c.seq = append(*c.seq, "Signal")
	return &fakeSignalStream{ctx: ctx}, nil
}

func TestJoinSessionAtomicAndLeave(t *testing.T) {
	var seq []string
	peer := &fakePeerClient{seq: &seq}
	sig := &fakeSignaling{seq: &seq}
	f := &WebRTCFacade{
		Peer:         &WebRTCPeerFacade{Raw: peer, meta: Metadata{TenantID: "t-1"}},
		RawSignaling: sig,
		meta:         Metadata{TenantID: "t-1"},
	}
	sess, err := f.JoinSession(context.Background(), "room-1", "alice", "{}", "ua", 300)
	if err != nil {
		t.Fatalf("JoinSession: %v", err)
	}
	// Exactly one JoinSession + the Signal open. No JoinRoom/IssueCredentials.
	if !reflect.DeepEqual(seq, []string{"JoinSession", "Signal"}) {
		t.Fatalf("JoinSession must be atomic (no JoinRoom/IssueCredentials): %v", seq)
	}
	if sess.PeerID() != "peer-1" {
		t.Fatalf("peer id wrong: %q", sess.PeerID())
	}
	if err := sess.Leave(context.Background()); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	if seq[len(seq)-1] != "LeaveRoom" {
		t.Fatalf("Leave must issue LeaveRoom: %v", seq)
	}
	if peer.lastLeave.PeerId != "peer-1" {
		t.Fatalf("LeaveRoom peer id wrong: %q", peer.lastLeave.PeerId)
	}
}

// ── Asset StartAndWait (13.6) ────────────────────────────────────────────────

type fakeAssetStartWait struct {
	assetv1.AssetServiceClient
	seq      *[]string
	getCalls int
}

func (c *fakeAssetStartWait) StartPipeline(_ context.Context, _ *assetv1.StartPipelineRequest, _ ...grpc.CallOption) (*assetv1.StartPipelineResponse, error) {
	*c.seq = append(*c.seq, "StartPipeline")
	return &assetv1.StartPipelineResponse{
		InstanceId: "inst-1",
		Steps:      []*assetentityv1.PipelineStep{{StepId: "step-a"}},
	}, nil
}

func (c *fakeAssetStartWait) GetPipeline(_ context.Context, _ *assetv1.GetPipelineRequest, _ ...grpc.CallOption) (*assetv1.GetPipelineResponse, error) {
	*c.seq = append(*c.seq, "GetPipeline")
	c.getCalls++
	st := assetentityv1.PipelineStatus_PIPELINE_STATUS_RUNNING
	if c.getCalls >= 2 {
		st = assetentityv1.PipelineStatus_PIPELINE_STATUS_COMPLETED
	}
	return &assetv1.GetPipelineResponse{Instance: &assetentityv1.PipelineInstance{InstanceId: "inst-1", Status: st}}, nil
}

func TestAssetStartAndWaitUsesInlineSteps(t *testing.T) {
	var seq []string
	fa := &fakeAssetStartWait{seq: &seq}
	f := &AssetFacade{Raw: fa, meta: Metadata{TenantID: "t-1"}}
	res, err := f.StartAndWait(context.Background(), "def-1", "asset-1", "{}", "", 0)
	if err != nil {
		t.Fatalf("StartAndWait: %v", err)
	}
	// First call must be StartPipeline; the inline steps come from it (NOT a proof
	// GetPipeline immediately after Start).
	if seq[0] != "StartPipeline" {
		t.Fatalf("first call must be StartPipeline: %v", seq)
	}
	if len(res.Steps) != 1 || res.Steps[0].GetStepId() != "step-a" {
		t.Fatalf("inline steps not consumed from StartPipeline response: %+v", res.Steps)
	}
	if res.Status != assetentityv1.PipelineStatus_PIPELINE_STATUS_COMPLETED {
		t.Fatalf("did not poll to terminal status: %v", res.Status)
	}
}

// ── Notification helpers (13.5) ──────────────────────────────────────────────

type fakeNotif struct {
	notificationv1.NotificationServiceClient
	seq      *[]string
	getCalls int
}

func (c *fakeNotif) SendNotification(_ context.Context, _ *notificationv1.SendNotificationRequest, _ ...grpc.CallOption) (*notificationv1.SendNotificationResponse, error) {
	*c.seq = append(*c.seq, "SendNotification")
	return &notificationv1.SendNotificationResponse{Logs: []*notificationentityv1.NotificationLog{{LogId: "log-1"}}}, nil
}

func (c *fakeNotif) GetNotification(_ context.Context, _ *notificationv1.GetNotificationRequest, _ ...grpc.CallOption) (*notificationv1.GetNotificationResponse, error) {
	*c.seq = append(*c.seq, "GetNotification")
	c.getCalls++
	st := notificationentityv1.NotificationStatus_NOTIFICATION_STATUS_PENDING
	if c.getCalls >= 2 {
		st = notificationentityv1.NotificationStatus_NOTIFICATION_STATUS_DELIVERED
	}
	return &notificationv1.GetNotificationResponse{Log: &notificationentityv1.NotificationLog{LogId: "log-1", Status: st}}, nil
}

func TestNotificationSendTemplateAndWait(t *testing.T) {
	var seq []string
	fn := &fakeNotif{seq: &seq}
	f := &NotificationFacade{Raw: fn, meta: Metadata{TenantID: "t-1"}}
	ids, err := f.SendTemplate(context.Background(), "welcome", "u-1", map[string]string{"n": "1"})
	if err != nil {
		t.Fatalf("SendTemplate: %v", err)
	}
	if !reflect.DeepEqual(ids, []string{"log-1"}) {
		t.Fatalf("log ids wrong: %v", ids)
	}
	if !reflect.DeepEqual(seq, []string{"SendNotification"}) {
		t.Fatalf("SendTemplate must issue exactly one SendNotification: %v", seq)
	}
	st, err := f.WaitForDelivery(context.Background(), "log-1", 0)
	if err != nil {
		t.Fatalf("WaitForDelivery: %v", err)
	}
	if st != notificationentityv1.NotificationStatus_NOTIFICATION_STATUS_DELIVERED {
		t.Fatalf("did not reach terminal status: %v", st)
	}
	// WaitForDelivery must use only Get reads (no sleep RPCs).
	for _, m := range seq[1:] {
		if m != "GetNotification" {
			t.Fatalf("WaitForDelivery issued a non-read RPC: %v", seq)
		}
	}
}
