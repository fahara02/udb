package udbclient

import (
	"context"
	"net"
	"reflect"
	"testing"
	"time"

	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	livev1 "github.com/fahara02/udb/sdk/go/gen/udb/sdk/live/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestColumnPredicatesUseTheActualRecordCodec(t *testing.T) {
	var zero Column[string]
	if filter, err := zero.In(); err == nil || filter != nil {
		t.Fatal("a zero predicate column must not produce a filter")
	}
	if filter, err := zero.IsNull(); err == nil || filter != nil {
		t.Fatal("a zero null predicate must not produce a filter")
	}
	status := ColumnOf[storagev1.FileStatus]((*storagev1.File)(nil), "status")
	filter, err := status.In(storagev1.FileStatus_FILE_STATUS_PENDING, storagev1.FileStatus_FILE_STATUS_ACTIVE)
	if err != nil || !reflect.DeepEqual(filter, Filter{"status": map[string]any{"$in": []any{"PENDING", "ACTIVE"}}}) {
		t.Fatalf("enum predicate must use stored tokens: filter=%v err=%v", filter, err)
	}
	numeric := ColumnOf[int32]((*storagev1.File)(nil), "status")
	filter, err = numeric.Eq(int32(storagev1.FileStatus_FILE_STATUS_ACTIVE))
	if err != nil || !reflect.DeepEqual(filter, Filter{"status": map[string]any{"$eq": "ACTIVE"}}) {
		t.Fatal("numeric protobuf enum predicates must still use descriptor validation and stored tokens")
	}
	if filter, err := ColumnOf[int64]((*storagev1.File)(nil), "status").Eq(1 << 40); err == nil || filter != nil {
		t.Fatal("numeric enum predicates must refuse values outside protobuf int32")
	}
	if filter, err := ColumnOf[string]((*storagev1.File)(nil), "status").Eq("UNDECLARED_ENUM_NAME"); err == nil || filter != nil {
		t.Fatal("enum predicates must preserve descriptor codec refusal of unknown names")
	}
	when := timestamppb.New(time.Date(2026, 10, 9, 12, 13, 14, 0, time.UTC))
	expires := ColumnOf[*timestamppb.Timestamp]((*storagev1.File)(nil), "expires_at")
	filter, err = expires.Gt(when)
	if err != nil || !reflect.DeepEqual(filter, Filter{"expires_at": map[string]any{"$gt": "2026-10-09T12:13:14Z"}}) {
		t.Fatalf("timestamp predicate must use the protobuf field codec: filter=%v err=%v", filter, err)
	}
	optional := ColumnOf[string](codecPresenceMessage(t, "JSONB"), "note")
	if filter, err := optional.Eq("not json"); err == nil || filter != nil {
		t.Fatal("invalid JSON must return an error and no filter")
	}
	if filter, err := optional.Eq(`{"count":9007199254740993}`); err == nil || filter != nil {
		t.Fatal("JSON precision loss at the Struct protobuf boundary must be refused")
	}
	if filter, err := optional.Eq(`{"count":9007199254740993e0}`); err == nil || filter != nil {
		t.Fatal("scientific JSON integer notation must not bypass precision refusal")
	}
	if filter, err := optional.IsNull(); err != nil || !reflect.DeepEqual(filter, Filter{"note": map[string]any{"$is_null": true}}) {
		t.Fatalf("null predicate = %v, err=%v", filter, err)
	}
	if filter, err := ColumnOf[string]((*livev1.SdkLiveRecord)(nil), "not_a_column").NotNull(); err == nil || filter != nil {
		t.Fatal("invalid null predicate must refuse a broad query")
	}
}

type predicateWireServer struct {
	servicesv1.UnimplementedDataBrokerServer
	requests chan *entityv1.SelectRequest
}

func (s *predicateWireServer) Select(_ context.Context, request *entityv1.SelectRequest) (*entityv1.RecordSet, error) {
	s.requests <- request
	return &entityv1.RecordSet{}, nil
}

// Exercise Table.Select and the generated TCP client, not only a helper map:
// float64 conversion at the actual Struct protobuf boundary used to lose these
// integer values when a typed predicate supplied an ordinary int64.
func TestColumnPredicateRetainsExact64BitValuesOnTableWire(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	fixture := &predicateWireServer{requests: make(chan *entityv1.SelectRequest, 1)}
	servicesv1.RegisterDataBrokerServer(server, fixture)
	serveDone := make(chan struct{})
	go func() { defer close(serveDone); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-serveDone })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	u, err := Connect(ctx, Config{Target: listener.Addr().String(), TenantID: codecTenant, ProjectID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	where, err := ColumnOf[int64]((*livev1.SdkLiveRecord)(nil), "revision").Between(9007199254740993, 9007199254740995)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := TableOf[*livev1.SdkLiveRecord](u).Select(ctx, where, SelectOptions{Limit: 1}); err != nil {
		t.Fatalf("actual typed Select failed: %v", err)
	}
	select {
	case request := <-fixture.requests:
		want := map[string]any{"revision": map[string]any{"$between": []any{"9007199254740993", "9007199254740995"}}}
		if !reflect.DeepEqual(request.GetFilter().AsMap(), want) || request.GetMessageType() != liveMessageType {
			t.Fatal("actual Table filter wire rounded the integer range or changed its entity")
		}
	case <-ctx.Done():
		t.Fatal("actual generated TCP Select never reached the fixture")
	}
}
