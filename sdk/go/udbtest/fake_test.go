package udbtest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	apikeyeventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/apikey/events/v1"
	authzentityv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/entity/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	workflowentityv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/workflow/entity/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"github.com/fahara02/udb/sdk/go/udbtest"
	"github.com/fahara02/udb/sdk/go/udbtest/conformance"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// The fake meets the same typed-store contract the live broker does.
func TestFakeMeetsTheTableContract(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	conformance.RunTable(t, func(t testing.TB, tenant string) *udbclient.Udb {
		return fake.Client(t, tenant)
	})
}

// The fake answers a join-fusion Select the way the broker does.
func TestFakeMeetsTheJoinContract(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{}, &authzentityv1.PolicySet{}, &authzentityv1.PolicyVersion{})
	conformance.RunJoin(t, func(t testing.TB, tenant string) *udbclient.Udb {
		return fake.Client(t, tenant)
	})
}

// A string written to a JSON column is JSON text, as on the broker, so every kind of value reads
// back as what was written.
func TestFakeHoldsAJSONColumnAsTheBrokerDoes(t *testing.T) {
	const message = "udb.core.workflow.entity.v1.WorkflowInstance"
	fake := udbtest.New(t, &workflowentityv1.WorkflowInstance{})
	tenant := "0190f1b2-0000-4000-8000-0000000000aa"
	client := fake.Client(t, tenant)
	ctx := context.Background()
	for i, payload := range []string{`"My"`, `"5"`, `""`, `5`, `[1]`, `{"a":1}`} {
		id := fmt.Sprintf("0190f1b2-0000-4000-8000-%012d", i+1)
		row := &workflowentityv1.WorkflowInstance{WorkflowId: id, TenantId: tenant, WorkflowType: "json", Payload: payload}
		for attempt := 0; attempt < 3; attempt++ {
			rec, err := udbclient.EncodeRecord(row, udbclient.WriteInsert)
			if err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(rec)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.Data.Upsert(ctx, &entityv1.UpsertRequest{MessageType: message, RecordJson: body}); err != nil {
				t.Fatalf("upsert %s: %v", payload, err)
			}
			filter, err := structpb.NewStruct(map[string]any{"workflow_id": id})
			if err != nil {
				t.Fatal(err)
			}
			found, err := client.Data.Select(ctx, &entityv1.SelectRequest{MessageType: message, Filter: filter, Limit: 1})
			if err != nil || len(found.GetRecordsJson()) != 1 {
				t.Fatalf("read %s: %v", payload, err)
			}
			var stored udbclient.Record
			if err := json.Unmarshal(found.GetRecordsJson()[0], &stored); err != nil {
				t.Fatal(err)
			}
			var back workflowentityv1.WorkflowInstance
			if err := udbclient.DecodeRecord(stored, &back); err != nil {
				t.Fatal(err)
			}
			if back.GetPayload() != payload {
				t.Fatalf("%s read back as %q", payload, back.GetPayload())
			}
			row.Payload = back.GetPayload()
		}
	}
	for _, text := range []string{`My`, `null`} {
		body, _ := json.Marshal(map[string]any{"workflow_id": "0190f1b2-0000-4000-8000-000000000099", "workflow_type": "json", "payload": text})
		_, err := client.Data.Upsert(ctx, &entityv1.UpsertRequest{MessageType: message, RecordJson: body})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("the text %s must be refused: code=%s %v", text, status.Code(err), err)
		}
	}
}

// The broker compares `expected` on a JSON column as the document its text encodes.
// The fake compared the text with the stored document, so a row whose JSON was "[]"
// could not be the expectation of its own next write.
func TestFakeComparesExpectedJSONTextAsTheStoredDocument(t *testing.T) {
	const message = "udb.core.workflow.entity.v1.WorkflowInstance"
	fake := udbtest.New(t, &workflowentityv1.WorkflowInstance{})
	tenant := "0190f1b2-0000-4000-8000-0000000000aa"
	client := fake.Client(t, tenant)
	ctx := context.Background()
	for i, payload := range []string{`[]`, `{}`, `[1]`, `{"a":[]}`} {
		id := fmt.Sprintf("0190f1b2-0000-4000-8000-%012d", i+100)
		row := map[string]any{"workflow_id": id, "workflow_type": "json", "payload": payload}
		body, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Data.Upsert(ctx, &entityv1.UpsertRequest{MessageType: message, RecordJson: body}); err != nil {
			t.Fatalf("upsert %s: %v", payload, err)
		}
		expected, err := structpb.NewStruct(row)
		if err != nil {
			t.Fatal(err)
		}
		next, err := json.Marshal(map[string]any{"workflow_id": id, "workflow_type": "json-2", "payload": payload})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Data.Upsert(ctx, &entityv1.UpsertRequest{MessageType: message, RecordJson: next, Expected: expected}); err != nil {
			t.Fatalf("expected %s: %v", payload, err)
		}
	}
}

// The fake passes the cross-SDK cases the TypeScript runner shares.
func TestFakeMeetsTheSharedTableCases(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	conformance.RunTableCases(t, func(t testing.TB, tenant string) *udbclient.Udb {
		return fake.Client(t, tenant)
	}, "../../conformance/table-cases.json")
}

// SelectMany sends several reads across two entities in one BatchSelect; the
// fake answers each with Select's logic, in request order, and one bad read
// fails the whole call.
func TestFakeServesSelectManyInRequestOrder(t *testing.T) {
	const (
		file     = "udb.core.storage.entity.v1.File"
		workflow = "udb.core.workflow.entity.v1.WorkflowInstance"
		fileA    = "0190f1b2-0000-4000-8000-000000000301"
		fileB    = "0190f1b2-0000-4000-8000-000000000302"
		flow     = "0190f1b2-0000-4000-8000-000000000303"
	)
	fake := udbtest.New(t, &storagev1.File{}, &workflowentityv1.WorkflowInstance{})
	client := fake.Client(t, "0190f1b2-0000-4000-8000-0000000000aa")
	ctx := context.Background()
	upsert := func(message string, row map[string]any) {
		body, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Data.Upsert(ctx, &entityv1.UpsertRequest{MessageType: message, RecordJson: body}); err != nil {
			t.Fatalf("upsert %s: %v", message, err)
		}
	}
	upsert(file, map[string]any{"file_id": fileA, "filename": "a.pdf", "object_key": "many/a"})
	upsert(file, map[string]any{"file_id": fileB, "filename": "b.pdf", "object_key": "many/b"})
	upsert(workflow, map[string]any{"workflow_id": flow, "workflow_type": "many"})
	read := func(message string, where map[string]any, limit int32, sorts ...*entityv1.Sort) *entityv1.SelectRequest {
		filter, err := structpb.NewStruct(where)
		if err != nil {
			t.Fatal(err)
		}
		return &entityv1.SelectRequest{MessageType: message, Filter: filter, Limit: limit, Sort: sorts}
	}
	field := func(set *entityv1.RecordSet, name string) []any {
		var out []any
		for _, raw := range set.GetRecordsJson() {
			var row map[string]any
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			out = append(out, row[name])
		}
		return out
	}
	for attempt := 0; attempt < 3; attempt++ {
		sets, err := client.Data.SelectMany(ctx, []*entityv1.SelectRequest{
			read(file, map[string]any{"file_id": fileB}, 0),
			read(workflow, map[string]any{"workflow_id": flow}, 0),
			read(file, map[string]any{"file_id": fileA}, 0),
			read(file, map[string]any{"file_id": map[string]any{"$in": []any{fileA, fileB}}}, 1, &entityv1.Sort{Field: "filename", Descending: true}),
			read(workflow, map[string]any{"workflow_id": "0190f1b2-0000-4000-8000-000000000399"}, 0),
		})
		if err != nil {
			t.Fatalf("SelectMany: %v", err)
		}
		if len(sets) != 5 {
			t.Fatalf("results = %d, want 5", len(sets))
		}
		got := fmt.Sprint(field(sets[0], "filename"), field(sets[1], "workflow_type"), field(sets[2], "filename"), field(sets[3], "filename"), len(sets[4].GetRecordsJson()))
		if want := "[b.pdf] [many] [a.pdf] [b.pdf] 0"; got != want {
			t.Fatalf("results out of request order: got %s, want %s", got, want)
		}
		if !sets[3].GetHasMore() {
			t.Fatal("a capped read must say it has more")
		}
	}
	_, err := client.Data.SelectMany(ctx, []*entityv1.SelectRequest{
		read(file, map[string]any{}, 0),
		read("udb.nope.v1.Missing", map[string]any{}, 0),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("one bad read must fail the call: code=%s %v", status.Code(err), err)
	}
	if sets, err := client.Data.SelectMany(ctx, nil); err != nil || len(sets) != 0 {
		t.Fatalf("no reads, no call: %v %v", sets, err)
	}
}

// Emit builds the envelope from the event's own contract and commits it with
// the transaction's writes.
func TestEmitCommitsTheEventWithTheTransaction(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	tenant := "0190f1b2-0000-4000-8000-0000000000aa"
	u := fake.Client(t, tenant)
	err := u.Tx(context.Background(), func(tx *udbclient.TxScope) error {
		if err := tx.Upsert(&storagev1.File{FileId: "0190f1b2-0000-4000-8000-000000000001", Filename: "a.pdf", ObjectKey: "k"}); err != nil {
			return err
		}
		return tx.Emit(&apikeyeventsv1.ApiKeyCreatedEvent{KeyId: "key-1", TenantId: tenant, Name: "ci"})
	})
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	events := fake.Events()
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	e := events[0]
	if e.Topic != "udb.apikey.created.v1" || e.PartitionKey != tenant {
		t.Fatalf("topic/partition = %q/%q", e.Topic, e.PartitionKey)
	}
	if e.Envelope["event_type"] != "udb.core.apikey.events.v1.ApiKeyCreatedEvent" || e.Envelope["envelope_version"] != float64(udbclient.EventEnvelopeVersion) {
		t.Fatalf("envelope = %v", e.Envelope)
	}
	payload, _ := e.Envelope["payload"].(map[string]any)
	if payload["key_id"] != "key-1" {
		t.Fatalf("the event's fields must ride under payload, got %v", e.Envelope)
	}

	// An event with no partition-key value fails before anything is sent.
	err = u.Tx(context.Background(), func(tx *udbclient.TxScope) error {
		return tx.Emit(&apikeyeventsv1.ApiKeyCreatedEvent{KeyId: "key-2"})
	})
	if err == nil {
		t.Fatal("an event without its partition key must be refused")
	}
	if len(fake.Events()) != 1 {
		t.Fatal("a refused emit must not be committed")
	}
}

// Inspect turns a refusal into the closed error code with the broker's reason.
func TestInspectReadsTheBrokersReason(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	u := fake.Client(t, "0190f1b2-0000-4000-8000-0000000000bb")
	files := udbclient.TableOf[*storagev1.File](u)
	err := files.Delete(context.Background(), udbclient.RowKey{"file_id": "0190f1b2-0000-4000-8000-000000000009"})
	info := udbclient.Inspect(err)
	if info.Code != udbclient.CodeNotFound || info.Reason != "UDB_NO_ROWS_AFFECTED" || !udbclient.IsNotFound(err) {
		t.Fatalf("Inspect = %+v", info)
	}
	_, err = files.Select(context.Background(), udbclient.Filter{"filename": udbclient.Filter{"$regex": "x"}}, udbclient.SelectOptions{})
	if got := udbclient.Inspect(err); got.Code != udbclient.CodeInvalid || got.Reason != "UDB_UNSUPPORTED_FILTER_OPERATOR" {
		t.Fatalf("unknown operator = %+v", got)
	}
}

// The broker's array filters: `$contains` (@>), `$contained_by` (<@) and
// `$overlaps`/`$overlap` (&&) on an array (repeated) column.
func TestFakeArrayFilters(t *testing.T) {
	const tenant = "0190f1b2-0000-4000-8000-0000000000aa"
	fake := udbtest.New(t, &authzentityv1.AccessDecisionAudit{})
	audits := udbclient.TableOf[*authzentityv1.AccessDecisionAudit](fake.Client(t, tenant))
	ctx := context.Background()
	for id, matched := range map[string][]string{"101": {"p1", "p2"}, "102": {"p2", "p3"}, "103": {}} {
		row := &authzentityv1.AccessDecisionAudit{
			DecisionAuditId: "0190f1b2-0000-4000-8000-000000000" + id, UserId: tenant, Domain: "d", Object: "o",
			Action: "read", Effect: authzentityv1.PolicyEffect_POLICY_EFFECT_ALLOW,
			DecisionSource: authzentityv1.DecisionSource_DECISION_SOURCE_ROLE_POLICY, TenantId: tenant, MatchedPolicyIds: matched,
		}
		if err := audits.Upsert(ctx, row); err != nil {
			t.Fatalf("upsert %s: %v", id, err)
		}
	}
	ids := func(where udbclient.Filter) string {
		t.Helper()
		page, err := audits.Select(ctx, where, udbclient.SelectOptions{})
		if err != nil {
			t.Fatalf("select %v: %v", where, err)
		}
		var out []string
		for _, row := range page.Rows {
			out = append(out, row.GetDecisionAuditId()[len(row.GetDecisionAuditId())-3:])
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}
	op := func(name string, arg any) udbclient.Filter {
		return udbclient.Filter{"matched_policy_ids": udbclient.Filter{name: arg}}
	}
	for _, c := range []struct {
		where udbclient.Filter
		want  string
	}{
		{op("$contains", []any{"p2"}), "101,102"},
		{op("$contains", []any{"p1", "p2"}), "101"},
		{op("$contained_by", []any{"p1", "p2", "p9"}), "101,103"},
		{op("$overlaps", []any{"p3", "p9"}), "102"},
		{op("$overlap", []any{"p1", "p3"}), "101,102"},
		{op("$overlaps", []any{}), ""},
		{op("$not", map[string]any{"$contains": []any{"p1"}}), "102,103"},
	} {
		if got := ids(c.where); got != c.want {
			t.Errorf("%v = %q, want %q", c.where, got, c.want)
		}
	}
	for _, bad := range []udbclient.Filter{
		op("$contains", "p1"),
		{"domain": udbclient.Filter{"$contains": []any{"d"}}},
	} {
		if _, err := audits.Select(ctx, bad, udbclient.SelectOptions{}); udbclient.Inspect(err).Code != udbclient.CodeInvalid {
			t.Errorf("%v must be refused as invalid: %v", bad, err)
		}
	}
}

// On a JSON column `$contains`/`$contained_by` are jsonb containment, as on the
// broker; `$overlaps` has no jsonb form and is refused.
func TestFakeJSONContainment(t *testing.T) {
	const tenant = "0190f1b2-0000-4000-8000-0000000000aa"
	fake := udbtest.New(t, &workflowentityv1.WorkflowInstance{})
	flows := udbclient.TableOf[*workflowentityv1.WorkflowInstance](fake.Client(t, tenant))
	ctx := context.Background()
	for id, payload := range map[string]string{
		"201": `{"kind":"invoice","tags":["a","b"],"n":1}`,
		"202": `{"kind":"receipt","tags":["b"]}`,
		"203": `["x","y"]`,
	} {
		row := &workflowentityv1.WorkflowInstance{WorkflowId: "0190f1b2-0000-4000-8000-000000000" + id, TenantId: tenant, WorkflowType: "json", Payload: payload}
		if err := flows.Upsert(ctx, row); err != nil {
			t.Fatalf("upsert %s: %v", id, err)
		}
	}
	ids := func(where udbclient.Filter) string {
		t.Helper()
		page, err := flows.Select(ctx, where, udbclient.SelectOptions{})
		if err != nil {
			t.Fatalf("select %v: %v", where, err)
		}
		var out []string
		for _, row := range page.Rows {
			out = append(out, row.GetWorkflowId()[len(row.GetWorkflowId())-3:])
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}
	op := func(name string, arg any) udbclient.Filter {
		return udbclient.Filter{"payload": udbclient.Filter{name: arg}}
	}
	for _, c := range []struct {
		where udbclient.Filter
		want  string
	}{
		{op("$contains", map[string]any{"kind": "invoice"}), "201"},
		{op("$contains", map[string]any{"tags": []any{"b"}}), "201,202"},
		{op("$contains", map[string]any{"n": "1"}), ""},
		{op("$contains", "x"), "203"},
		{op("$contains", []any{"y"}), "203"},
		{op("$contained_by", map[string]any{"kind": "receipt", "tags": []any{"b", "c"}, "extra": true}), "202"},
	} {
		if got := ids(c.where); got != c.want {
			t.Errorf("%v = %q, want %q", c.where, got, c.want)
		}
	}
	if _, err := flows.Select(ctx, op("$overlaps", []any{"x"}), udbclient.SelectOptions{}); udbclient.Inspect(err).Code != udbclient.CodeInvalid {
		t.Errorf("$overlaps on a JSON column must be refused: %v", err)
	}
}
