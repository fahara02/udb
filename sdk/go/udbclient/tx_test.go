package udbclient

import (
	"context"
	"errors"
	"io"
	"testing"

	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type earlyRefusalTxStream struct {
	grpc.BidiStreamingClient[entityv1.Mutation, entityv1.TxStatus]
	trailer  metadata.MD
	received bool
}

func (*earlyRefusalTxStream) Send(*entityv1.Mutation) error { return io.EOF }
func (*earlyRefusalTxStream) CloseSend() error              { return nil }
func (s *earlyRefusalTxStream) Trailer() metadata.MD        { return s.trailer }
func (s *earlyRefusalTxStream) Recv() (*entityv1.TxStatus, error) {
	s.received = true
	return nil, status.Error(codes.FailedPrecondition, "opaque operator text")
}

type earlyRefusalTxBroker struct {
	servicesv1.DataBrokerClient
	stream grpc.BidiStreamingClient[entityv1.Mutation, entityv1.TxStatus]
}

func (b *earlyRefusalTxBroker) BeginTx(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[entityv1.Mutation, entityv1.TxStatus], error) {
	return b.stream, nil
}

func TestTxEarlySendEOFStillReadsTypedRefusal(t *testing.T) {
	stream := &earlyRefusalTxStream{trailer: metadata.Pairs("udb-error-detail-bin", string(marshalDetail(t,
		&entityv1.ErrorDetail{Reason: "UDB_CAS_CONFLICT"})))}
	u := &Udb{Data: &Client{Broker: &earlyRefusalTxBroker{stream: stream}}}
	err := u.Tx(context.Background(), func(tx *TxScope) error {
		return tx.Patch(&storagev1.File{}, RowKey{"file_id": "file"}, Record{"filename": "new"})
	})
	if !stream.received || !errors.Is(err, ErrConflict) {
		t.Fatalf("early EOF hid the server's refusal: received=%v error=%v", stream.received, err)
	}
	typed, ok := AsError(err)
	if !ok || typed.Code != codes.FailedPrecondition || typed.Reason() != "UDB_CAS_CONFLICT" {
		t.Fatalf("original code/detail lost: %v", err)
	}
}

type framedRefusalTxStream struct {
	grpc.BidiStreamingClient[entityv1.Mutation, entityv1.TxStatus]
	frame    *entityv1.TxStatus
	terminal error
	trailer  metadata.MD
	received int
}

func (*framedRefusalTxStream) Send(*entityv1.Mutation) error { return nil }
func (*framedRefusalTxStream) CloseSend() error              { return nil }
func (s *framedRefusalTxStream) Trailer() metadata.MD        { return s.trailer }
func (s *framedRefusalTxStream) Recv() (*entityv1.TxStatus, error) {
	s.received++
	if s.received == 1 {
		return s.frame, nil
	}
	return nil, s.terminal
}

// Exercise the public transaction method: helper-only tests do not catch
// returning on the frame before the terminal status and trailers arrive.
func TestTxPreservesFrameRefusalAndDrainsAuthoritativeTrailer(t *testing.T) {
	conflict := &entityv1.ErrorDetail{Reason: "UDB_CAS_CONFLICT", Column: "filename"}
	unique := &entityv1.ErrorDetail{Reason: "UDB_UNIQUE_VIOLATION", Constraint: "files_name_key"}
	for _, state := range []entityv1.TxStatus_State{
		entityv1.TxStatus_TX_STATE_ERROR, entityv1.TxStatus_TX_STATE_ROLLED_BACK,
	} {
		t.Run(state.String(), func(t *testing.T) {
			for _, test := range []struct {
				name        string
				terminal    error
				trailer     metadata.MD
				wantCode    codes.Code
				wantDetail  *entityv1.ErrorDetail
				wantMessage string
			}{
				{"frame followed by EOF", io.EOF, nil, codes.FailedPrecondition, conflict, "opaque frame text"},
				{"terminal conflict", status.Error(codes.FailedPrecondition, "opaque terminal text"),
					metadata.Pairs("udb-error-detail-bin", string(marshalDetail(t, conflict))),
					codes.FailedPrecondition, conflict, "opaque terminal text"},
				{"terminal overrides frame", status.Error(codes.AlreadyExists, "unique terminal text"),
					metadata.Pairs("udb-error-detail-bin", string(marshalDetail(t, unique))),
					codes.AlreadyExists, unique, "unique terminal text"},
			} {
				t.Run(test.name, func(t *testing.T) {
					stream := &framedRefusalTxStream{
						frame: &entityv1.TxStatus{State: state, Code: int32(codes.FailedPrecondition),
							Message: "opaque frame text", ErrorDetail: conflict},
						terminal: test.terminal, trailer: test.trailer,
					}
					u := &Udb{Data: &Client{Broker: &earlyRefusalTxBroker{stream: stream}}}
					err := u.Tx(context.Background(), func(tx *TxScope) error {
						return tx.Patch(&storagev1.File{}, RowKey{"file_id": "file"}, Record{"filename": "new"})
					})
					if stream.received != 2 {
						t.Fatalf("transaction did not drain its terminal refusal: Recv calls = %d", stream.received)
					}
					typed, ok := AsError(err)
					if !ok || typed.Code != test.wantCode || typed.Message != test.wantMessage {
						t.Fatalf("original transaction status lost: %v", err)
					}
					detail, ok := typed.Detail()
					if !ok || !proto.Equal(detail, test.wantDetail) {
						t.Fatalf("original transaction detail lost: %v", detail)
					}
					if errors.Is(err, ErrConflict) != (test.wantDetail.GetReason() == "UDB_CAS_CONFLICT") {
						t.Fatalf("transaction classified the wrong refusal: %v", err)
					}
				})
			}
		})
	}
}
