package udbclient

import (
	"context"
	"encoding/json"
	"errors"
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

// #4: WithDeleteIdempotencyKey must populate DeleteRequest.idempotency_key (the
// wire field existed but the SDK never set it) in exactly one RPC.
func TestEntityDeleteIdempotencyKeyPassThrough(t *testing.T) {
	var seq []string
	b, e := newRecordingEntity(&seq)
	if _, err := e.Delete(context.Background(), map[string]any{"record_id": "r1"}, WithDeleteIdempotencyKey("idem-del-1")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !reflect.DeepEqual(seq, []string{"Delete"}) {
		t.Fatalf("Delete must be exactly one RPC: %v", seq)
	}
	if b.lastDelete.GetIdempotencyKey() != "idem-del-1" {
		t.Fatalf("DeleteRequest.IdempotencyKey not set: %q", b.lastDelete.GetIdempotencyKey())
	}
}

// #4: a whitespace-only key would silently disable replay-safety while looking
// set — it is rejected before any RPC.
func TestEntityDeleteWhitespaceIdempotencyKeyRejected(t *testing.T) {
	var seq []string
	_, e := newRecordingEntity(&seq)
	if _, err := e.Delete(context.Background(), map[string]any{"record_id": "r1"}, WithDeleteIdempotencyKey("   ")); err == nil {
		t.Fatal("whitespace-only idempotency key must be rejected")
	}
	if len(seq) != 0 {
		t.Fatalf("no RPC may be issued when the key is rejected: %v", seq)
	}
}

// #7: a guarded Delete (WithDeleteExpected) with a nil/empty precondition must
// be rejected with errEmptyCAS BEFORE any RPC — never degrade to a full-table
// delete of every matched row.
func TestEntityDeleteGuardedEmptyCASRejected(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expected map[string]any
	}{
		{"nil", nil},
		{"empty", map[string]any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seq []string
			_, e := newRecordingEntity(&seq)
			_, err := e.Delete(context.Background(), map[string]any{"record_id": "r1"}, WithDeleteExpected(tc.expected))
			if !errors.Is(err, errEmptyCAS) {
				t.Fatalf("guarded Delete with %s CAS must return errEmptyCAS, got %v", tc.name, err)
			}
			if len(seq) != 0 {
				t.Fatalf("no RPC may be issued for a rejected empty-CAS Delete: %v", seq)
			}
		})
	}
}

// #7: the same gate on Update/Increment (WithUpdateExpected).
func TestEntityUpdateGuardedEmptyCASRejected(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expected map[string]any
	}{
		{"nil", nil},
		{"empty", map[string]any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seq []string
			_, e := newRecordingEntity(&seq)
			_, err := e.Update(context.Background(), map[string]any{"record_id": "r1"}, map[string]any{"n": 2}, WithUpdateExpected(tc.expected))
			if !errors.Is(err, errEmptyCAS) {
				t.Fatalf("guarded Update with %s CAS must return errEmptyCAS, got %v", tc.name, err)
			}
			if len(seq) != 0 {
				t.Fatalf("no RPC may be issued for a rejected empty-CAS Update: %v", seq)
			}
		})
	}
}

// #7: the gate must NOT touch the unconditional path — a Delete with no
// WithDeleteExpected still issues exactly one RPC with a nil Expected.
func TestEntityUnconditionalDeleteUnaffected(t *testing.T) {
	var seq []string
	b, e := newRecordingEntity(&seq)
	if _, err := e.Delete(context.Background(), map[string]any{"record_id": "r1"}); err != nil {
		t.Fatalf("unconditional Delete: %v", err)
	}
	if !reflect.DeepEqual(seq, []string{"Delete"}) {
		t.Fatalf("unconditional Delete must still issue one RPC: %v", seq)
	}
	if b.lastDelete.GetExpected() != nil {
		t.Fatalf("unconditional Delete must send a nil Expected: %v", b.lastDelete.GetExpected())
	}
}

// #9: a 64-bit id beyond 2^53 must survive decode exactly. A plain
// json.Unmarshal into map[string]any rounds it through float64; UseNumber keeps
// the exact digits as a json.Number.
func TestDecodeRecordSetIntegerPrecision(t *testing.T) {
	const bigID = "9223372036854775807" // math.MaxInt64, unrepresentable as float64
	rs := &entityv1.RecordSet{RecordsJson: [][]byte{
		[]byte(`{"record_id":"r1","big_id":` + bigID + `}`),
	}}
	rows, err := decodeRecordSet(rs)
	if err != nil {
		t.Fatalf("decodeRecordSet: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	num, ok := rows[0]["big_id"].(json.Number)
	if !ok {
		t.Fatalf("big_id must decode to json.Number (precision-preserving), got %T", rows[0]["big_id"])
	}
	if num.String() != bigID {
		t.Fatalf("big_id digits corrupted: got %q want %q", num.String(), bigID)
	}
	if got, err := num.Int64(); err != nil || got != 9223372036854775807 {
		t.Fatalf("big_id.Int64() = %d, %v; want MaxInt64 exact", got, err)
	}
}
