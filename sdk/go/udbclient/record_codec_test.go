package udbclient

import (
	"encoding/json"
	"testing"
	"time"

	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const codecTenant = "0190f1b2-0000-7000-8000-0000000000aa"

// The column annotations drive every encode rule: an empty NOT NULL UUID is
// dropped (its default applies), an empty nullable column is NULL, an enum in a
// VARCHAR column is its short token, a timestamp is RFC 3339.
func TestEncodeRecordFollowsColumnAnnotations(t *testing.T) {
	file := &storagev1.File{
		TenantId:  codecTenant,
		Filename:  "notes.pdf",
		Status:    storagev1.FileStatus_FILE_STATUS_PENDING,
		FileType:  storagev1.FileType_FILE_TYPE_PDF,
		ExpiresAt: timestamppb.New(mustTime(t, "2026-10-07T09:30:00Z")),
	}
	rec, err := EncodeRecord(file, WriteInsert)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, present := rec["file_id"]; present {
		t.Errorf("an empty NOT NULL UUID key must be dropped so its default applies, got %v", rec["file_id"])
	}
	if v, present := rec["project_id"]; !present || v != nil {
		t.Errorf("an empty nullable column must be NULL, got %v (present=%v)", v, present)
	}
	if rec["status"] != "PENDING" || rec["file_type"] != "PDF" {
		t.Errorf("enums in VARCHAR columns must be short tokens, got status=%v file_type=%v", rec["status"], rec["file_type"])
	}
	if rec["expires_at"] != "2026-10-07T09:30:00Z" {
		t.Errorf("expires_at = %v", rec["expires_at"])
	}
	if rec["tenant_id"] != codecTenant {
		t.Errorf("tenant_id = %v", rec["tenant_id"])
	}

	update, err := EncodeRecord(file, WriteUpdate)
	if err != nil {
		t.Fatalf("encode update: %v", err)
	}
	for _, owned := range []string{"file_id", "tenant_id"} {
		if _, present := update[owned]; present {
			t.Errorf("an update must not carry %s", owned)
		}
	}
}

// Every shape the broker (or Postgres behind it) hands back decodes into the
// message: short or full enum names, enum numbers as text, timestamps.
func TestDecodeRecordAcceptsEveryStoredShape(t *testing.T) {
	var row Record
	if err := json.Unmarshal([]byte(`{
		"file_id": "0190f1b2-0000-7000-8000-000000000001",
		"tenant_id": "`+codecTenant+`",
		"filename": "notes.pdf",
		"status": "PENDING",
		"file_type": "FILE_TYPE_PDF",
		"scan_verdict": "1",
		"is_public": "true",
		"expires_at": "2026-10-07T09:30:00Z",
		"not_a_column": "ignored"
	}`), &row); err != nil {
		t.Fatal(err)
	}
	var file storagev1.File
	if err := DecodeRecord(row, &file); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if file.GetStatus() != storagev1.FileStatus_FILE_STATUS_PENDING {
		t.Errorf("short token status decoded to %v", file.GetStatus())
	}
	if file.GetFileType() != storagev1.FileType_FILE_TYPE_PDF {
		t.Errorf("full-name file_type decoded to %v", file.GetFileType())
	}
	if !file.GetIsPublic() {
		t.Error("boolean text decoded to false")
	}
	if file.GetExpiresAt().AsTime().Format("2006-01-02T15:04:05Z07:00") != "2026-10-07T09:30:00Z" {
		t.Errorf("expires_at = %v", file.GetExpiresAt())
	}
}

// Encode → decode is the identity for the fields a row stores.
func TestRecordCodecRoundTrips(t *testing.T) {
	in := &storagev1.File{
		FileId:   "0190f1b2-0000-7000-8000-000000000002",
		TenantId: codecTenant,
		Filename: "a.txt",
		Status:   storagev1.FileStatus_FILE_STATUS_PENDING,
		IsPublic: true,
	}
	rec, err := EncodeRecord(in, WriteInsert)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(rec)
	var back Record
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	var out storagev1.File
	if err := DecodeRecord(back, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.GetFileId() != in.GetFileId() || out.GetFilename() != in.GetFilename() ||
		out.GetStatus() != in.GetStatus() || out.GetIsPublic() != in.GetIsPublic() {
		t.Fatalf("round trip changed the record: %+v", &out)
	}
}

func TestSchemaAccessorsReadTheAnnotations(t *testing.T) {
	file := &storagev1.File{}
	if got := MessageType(file); got != "udb.core.storage.entity.v1.File" {
		t.Errorf("MessageType = %q", got)
	}
	if got := PrimaryKey(file); got != "file_id" {
		t.Errorf("PrimaryKey = %q", got)
	}
	if got := MaxLen(file, "filename"); got != 512 {
		t.Errorf("MaxLen(filename) = %d", got)
	}
}

func mustTime(t *testing.T, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

// Field-level encode/decode (what generated entity code calls for repeated,
// message and foreign-enum fields) follows the same column rules as the
// whole-record codec.
func TestEncodeAndDecodeFieldMatchTheRecordCodec(t *testing.T) {
	file := &storagev1.File{TenantId: codecTenant, Status: storagev1.FileStatus_FILE_STATUS_PENDING}
	v, ok, err := EncodeField(file, "status")
	if err != nil || !ok || v != "PENDING" {
		t.Fatalf("status = %v ok=%v err=%v", v, ok, err)
	}
	if v, ok, err := EncodeField(file, "project_id"); err != nil || !ok || v != nil {
		t.Fatalf("empty nullable column must encode as NULL, got %v ok=%v err=%v", v, ok, err)
	}
	if _, _, err := EncodeField(file, "nope"); err == nil {
		t.Fatal("an unknown field must be an error")
	}
	out := &storagev1.File{}
	if err := DecodeField(out, "status", "FILE_STATUS_ACTIVE"); err != nil {
		t.Fatal(err)
	}
	if err := DecodeField(out, "file_type", "PDF"); err != nil {
		t.Fatal(err)
	}
	if out.GetStatus() != storagev1.FileStatus_FILE_STATUS_ACTIVE || out.GetFileType() != storagev1.FileType_FILE_TYPE_PDF {
		t.Fatalf("decoded %v / %v", out.GetStatus(), out.GetFileType())
	}
	if err := DecodeField(out, "status", nil); err != nil || out.GetStatus() != storagev1.FileStatus_FILE_STATUS_ACTIVE {
		t.Fatalf("NULL must leave the field unchanged: %v %v", out.GetStatus(), err)
	}
}
