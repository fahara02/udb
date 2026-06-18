package udbclient

import (
	"context"
	"reflect"
	"testing"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
)

// recordingBroker embeds the generated DataBrokerClient so only the exercised
// RPCs need bodies; each appends its method name to an ordered sequence so the
// single-RPC guardrail (08.8.1.1) is enforced.
type recordingBroker struct {
	servicesv1.DataBrokerClient
	seq        *[]string
	lastUpsert *entityv1.UpsertRequest
	lastSelect *entityv1.SelectRequest
	lastDelete *entityv1.DeleteRequest
}

func (b *recordingBroker) Upsert(_ context.Context, in *entityv1.UpsertRequest, _ ...grpc.CallOption) (*entityv1.MutationResponse, error) {
	*b.seq = append(*b.seq, "Upsert")
	b.lastUpsert = in
	return &entityv1.MutationResponse{RecordJson: []byte(`{"record_id":"r1","n":7}`)}, nil
}

func (b *recordingBroker) Select(_ context.Context, in *entityv1.SelectRequest, _ ...grpc.CallOption) (*entityv1.RecordSet, error) {
	*b.seq = append(*b.seq, "Select")
	b.lastSelect = in
	return &entityv1.RecordSet{RecordsJson: [][]byte{[]byte(`{"record_id":"r1"}`)}}, nil
}

func (b *recordingBroker) Delete(_ context.Context, in *entityv1.DeleteRequest, _ ...grpc.CallOption) (*entityv1.MutationResponse, error) {
	*b.seq = append(*b.seq, "Delete")
	b.lastDelete = in
	return &entityv1.MutationResponse{}, nil
}

func newRecordingEntity(seq *[]string) (*recordingBroker, *Entity) {
	b := &recordingBroker{seq: seq}
	c := &Client{Broker: b, Meta: Metadata{TenantID: "t-1", ProjectID: "p-1", Scopes: []string{"data:write"}}}
	return b, c.Entity("udb.sdk.live.v1.SdkLiveRecord", Key("record_id"))
}

func TestKeyConstructor(t *testing.T) {
	if got := Key("record_id", "tenant_id"); !reflect.DeepEqual([]string(got), []string{"record_id", "tenant_id"}) {
		t.Fatalf("Key drift: %v", got)
	}
}

func TestEntityUpsertSingleRPC(t *testing.T) {
	var seq []string
	b, e := newRecordingEntity(&seq)
	res, err := e.Upsert(context.Background(), map[string]any{"record_id": "r1", "n": 7})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if want := loadWorkflowSequence(t, "Entity.upsert"); !reflect.DeepEqual(seq, want) {
		t.Fatalf("Upsert sequence drift: got %v, want %v", seq, want)
	}
	if b.lastUpsert.MessageType != "udb.sdk.live.v1.SdkLiveRecord" {
		t.Fatalf("message type wrong: %q", b.lastUpsert.MessageType)
	}
	if !reflect.DeepEqual(b.lastUpsert.ConflictFields, []string{"record_id"}) {
		t.Fatalf("conflict fields wrong: %v", b.lastUpsert.ConflictFields)
	}
	if b.lastUpsert.Context.GetTenantId() != "t-1" || b.lastUpsert.Context.GetProjectId() != "p-1" {
		t.Fatalf("tenant/project not seeded: %+v", b.lastUpsert.Context)
	}
	if len(b.lastUpsert.Context.GetScopes()) != 0 {
		t.Fatalf("entity requestContext must NOT set body scopes: %v", b.lastUpsert.Context.GetScopes())
	}
	if b.lastUpsert.Context.GetPrimaryRead() {
		t.Fatal("entity requestContext must NOT force primary_read")
	}
	if res.Record != nil {
		t.Fatalf("ReturnRecord not requested but record decoded: %v", res.Record)
	}
}

func TestEntityUpsertReturnRecordNoSecondRPC(t *testing.T) {
	var seq []string
	_, e := newRecordingEntity(&seq)
	res, err := e.Upsert(context.Background(), map[string]any{"record_id": "r1"}, ReturnRecord())
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if want := loadWorkflowSequence(t, "Entity.upsert.returnRecord"); !reflect.DeepEqual(seq, want) {
		t.Fatalf("ReturnRecord sequence drift (no proof Get allowed): got %v, want %v", seq, want)
	}
	if res.Record["record_id"] != "r1" {
		t.Fatalf("record not decoded from same response: %v", res.Record)
	}
}

func TestEntitySelectSingleRPC(t *testing.T) {
	var seq []string
	b, e := newRecordingEntity(&seq)
	rows, err := e.Select(context.Background(), map[string]any{"record_id": "r1"})
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if want := loadWorkflowSequence(t, "Entity.select"); !reflect.DeepEqual(seq, want) {
		t.Fatalf("Select sequence drift: got %v, want %v", seq, want)
	}
	if b.lastSelect.Filter == nil || b.lastSelect.Filter.Fields["record_id"] == nil {
		t.Fatalf("filter struct not built: %+v", b.lastSelect.Filter)
	}
	if len(rows) != 1 || rows[0]["record_id"] != "r1" {
		t.Fatalf("rows not decoded: %v", rows)
	}
}

func TestEntityDeleteSingleRPC(t *testing.T) {
	var seq []string
	b, e := newRecordingEntity(&seq)
	if _, err := e.Delete(context.Background(), map[string]any{"record_id": "r1"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !reflect.DeepEqual(seq, []string{"Delete"}) {
		t.Fatalf("Delete must be exactly one RPC (no Select/Get): %v", seq)
	}
	if b.lastDelete.MessageType != "udb.sdk.live.v1.SdkLiveRecord" {
		t.Fatalf("message type wrong: %q", b.lastDelete.MessageType)
	}
}

func TestEntityRequestContextDistinct(t *testing.T) {
	_, e := newRecordingEntity(new([]string))
	a := e.requestContext()
	b := e.requestContext()
	if a == b {
		t.Fatal("requestContext must return a fresh instance per call")
	}
}
