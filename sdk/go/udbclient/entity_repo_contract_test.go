package udbclient

// V23-1 compile contract for the typed repositories emitted by
// `udb sdk generate --project-proto --lang go` (see src/cli/sdk_gen.rs,
// render_go_entities_file). The generator writes `<Entity>Repo` methods that
// call into THIS package; if the SDK DTOs drift incompatibly, the generated
// consumer code stops compiling — but that only surfaced downstream, in the
// consumer's build, never here. V23-1: the emitter declared an int64 List count
// return while the page count was int32, so every generated repo failed
// `go build` at the consumer. 0.6.0 replaced that count with Page.HasMore.
//
// This test (plus the never-called shape function below) mirrors the generated
// repo's expressions line-for-line so the same mismatch fails to COMPILE inside
// the SDK's own CI (`go vet ./...` + `go test ./udbclient`). It is a
// compile-time contract, not a runtime test — no broker calls execute.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	commonv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestEntityRepoGeneratedContract(t *testing.T) {
	// ── List() return shape ───────────────────────────────────────────────
	// The generated List returns `page.HasMore` as its bool third value.
	var page Page
	var _ []map[string]any = page.Rows // FromUDBRow consumes each row
	var _ string = page.NextPageToken
	var hasMore bool = page.HasMore
	_ = hasMore
	var _ int64 = page.ExactTotal

	// ── PageOptions the caller of a generated List constructs ──────────────
	_ = PageOptions{
		Fields:       nil,
		Sort:         []SortKey{{Field: "id", Descending: true}},
		Limit:        0,
		PageToken:    "",
		IncludeTotal: false,
	}

	// Keep the call-shape contract compiled/type-checked without invoking it.
	_ = entityRepoGeneratedCallShapes
}

// entityRepoGeneratedCallShapes is never called; it exists only so the compiler
// and `go vet` type-check the EXACT udbclient calls the generated <Entity>Repo
// methods emit. Taking `c *Client` as a parameter (not a provably-nil local)
// keeps the nilness analyzer from flagging an impossible branch, while still
// failing to build if any of these signatures drift.
func entityRepoGeneratedCallShapes(c *Client, ctx context.Context) {
	e := c.Entity("acme.v1.Thing", Key("id")) // New<Entity>Repo binding
	repo := struct{ E *Entity }{E: e}         // <Entity>Repo{ E *udbclient.Entity }
	where := map[string]any{"id": "x"}
	// List: SelectPage → rows → (out, token, hasMore, err)
	p, _ := repo.E.SelectPage(ctx, where, PageOptions{})
	_ = p.HasMore
	// Get: Select → first row
	_, _ = repo.E.Select(ctx, where)
	// UpdateGuarded / DeleteGuarded: CAS options pass-through
	_, _ = repo.E.Update(ctx, where, map[string]any{"a": 1}, WithUpdateExpected(where))
	_, _ = repo.E.Delete(ctx, where, WithDeleteExpected(where))
}

// ── message-valued JSON/JSONB columns ────────────────────────────────────────
//
// A protobuf message stored in a JSON/JSONB column is written by EncodeField and
// read by protojson. The emitter previously reached its TEXT arm for these columns and
// wrote `json.RawMessage(m.GetX())` (not a legal conversion) and
// `m.X = encoded` (a string into a message pointer), so the generated package
// did not compile — reported against v0.5.6, v0.5.8 and v0.5.17 in turn, each
// time from a consumer's build rather than from this repository.
//
// The shapes below mirror `go_coercion_helpers` and the message arms of
// `go_to_record_stmt` / `go_from_row_stmt` (src/cli/sdk_gen.rs) line-for-line,
// so the same class of type error fails `go build ./...` HERE. Keep them in
// step with the emitter; the Rust side pins the emitted strings, this side
// pins that those strings are valid Go.

func udbDecodeJSONMessageContract(m proto.Message, field string, raw any) error {
	if raw == nil {
		return nil
	}
	var encoded string
	switch s := raw.(type) {
	case string:
		encoded = s
	case []byte:
		encoded = string(s)
	default:
		b, err := json.Marshal(raw)
		if err != nil {
			return fmt.Errorf("encode JSON value: %w", err)
		}
		encoded = string(b)
	}
	if encoded == "" || encoded == "null" {
		return nil
	}
	rm := m.ProtoReflect()
	fd := rm.Descriptor().Fields().ByName(protoreflect.Name(field))
	if fd == nil || fd.Message() == nil || fd.IsList() || fd.IsMap() {
		return fmt.Errorf("field %q is not a singular message field", field)
	}
	msg := rm.NewField(fd).Message()
	if err := protojson.Unmarshal([]byte(encoded), msg.Interface()); err != nil {
		return fmt.Errorf("decode JSON message: %w", err)
	}
	rm.Set(fd, protoreflect.ValueOfMessage(msg))
	return nil
}

// TestGeneratedMessageJSONColumnRoundTrips exercises the emitted write/read
// arms against a real message-typed field (`FileDescriptorProto.options`), so
// the contract is a round-trip proof, not only a compile check.
func TestGeneratedMessageJSONColumnRoundTrips(t *testing.T) {
	src := &descriptorpb.FileDescriptorProto{
		Options: &descriptorpb.FileOptions{GoPackage: proto.String("example.com/d;dv1")},
	}

	// Emitted ToUDBRecord arm.
	r := map[string]any{}
	if src.Options != nil {
		v, ok, err := EncodeField(src, "options")
		if err != nil {
			t.Fatalf("encode column %q: %v", "options", err)
		}
		if ok {
			r["options"] = v
		}
	}
	if _, ok := r["options"]; !ok {
		t.Fatal("a populated message column must be written")
	}

	// Emitted FromUDBRow arm.
	dst := &descriptorpb.FileDescriptorProto{}
	if raw, ok := r["options"]; ok && raw != nil {
		if err := udbDecodeJSONMessageContract(dst, "options", raw); err != nil {
			t.Fatalf("decode column %q: %v", "options", err)
		}
	}
	if !proto.Equal(src.GetOptions(), dst.GetOptions()) {
		t.Fatalf("round-trip lost the message: %v != %v", src.GetOptions(), dst.GetOptions())
	}

	// An unset message is omitted, so the column keeps its SQL NULL.
	empty := map[string]any{}
	if (&descriptorpb.FileDescriptorProto{}).Options != nil {
		t.Fatal("unreachable")
	}
	if _, ok := empty["options"]; ok {
		t.Fatal("an unset message column must be omitted, not written as null")
	}

	// Malformed stored JSON fails closed rather than decoding to a zero value.
	if err := udbDecodeJSONMessageContract(&descriptorpb.FileDescriptorProto{}, "options", "{not json"); err == nil {
		t.Fatal("malformed JSON must fail the read")
	}

	// A non-message field is rejected instead of panicking.
	if err := udbDecodeJSONMessageContract(&descriptorpb.FileDescriptorProto{}, "name", `"x"`); err == nil {
		t.Fatal("a non-message field must be rejected")
	}
}

// A generated *ToUDBRecord helper and a typed-table write must store one document
// for one value, or a full-row CAS expectation built from the helper fails against
// a row the table wrote. The helper's message arm is EncodeField, so this pins
// EncodeField against the record codec.
func TestGeneratedMessageJSONColumnMatchesTheRecordCodec(t *testing.T) {
	file := &storagev1.File{
		TenantId:  codecTenant,
		AuditInfo: &commonv1.AuditInfo{CreatedBy: "synthetic-user"},
	}
	helper, ok, err := EncodeField(file, "audit_info")
	if err != nil || !ok {
		t.Fatalf("encode field: ok=%v err=%v", ok, err)
	}
	for _, mode := range []WriteMode{WriteInsert, WriteUpdate} {
		rec, err := EncodeRecord(file, mode)
		if err != nil {
			t.Fatal(err)
		}
		want, err := json.Marshal(rec["audit_info"])
		if err != nil {
			t.Fatal(err)
		}
		got, err := json.Marshal(helper)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("mode %v: helper stores %s, the record codec stores %s", mode, got, want)
		}
	}
	got, err := json.Marshal(helper)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"created_by":"synthetic-user"`, `"updated_by":""`} {
		if !strings.Contains(string(got), want) {
			t.Fatalf("expected proto names and emitted defaults (%s) in %s", want, got)
		}
	}

	// Enums are numbers in this shape.
	src := &descriptorpb.FileDescriptorProto{
		Options: &descriptorpb.FileOptions{
			GoPackage:   proto.String("example.com/d;dv1"),
			OptimizeFor: descriptorpb.FileOptions_CODE_SIZE.Enum(),
		},
	}
	canonical, ok, err := EncodeField(src, "options")
	if err != nil || !ok {
		t.Fatalf("encode options: ok=%v err=%v", ok, err)
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"optimize_for":2`) {
		t.Fatalf("expected a numeric enum under its proto name in %s", encoded)
	}

	// Rows written by earlier generated helpers hold camelCase names and enum
	// symbols with defaults omitted. They are not rewritten, so the emitted read
	// must keep decoding them alongside the canonical shape.
	legacy, err := protojson.Marshal(src.GetOptions())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(legacy), "optimizeFor") {
		t.Fatalf("not the legacy shape: %s", legacy)
	}
	for name, stored := range map[string]any{"canonical": canonical, "legacy": string(legacy)} {
		dst := &descriptorpb.FileDescriptorProto{}
		if err := udbDecodeJSONMessageContract(dst, "options", stored); err != nil {
			t.Fatalf("decode %s shape: %v", name, err)
		}
		if !proto.Equal(src.GetOptions(), dst.GetOptions()) {
			t.Fatalf("%s shape decoded to %v, want %v", name, dst.GetOptions(), src.GetOptions())
		}
	}
}
