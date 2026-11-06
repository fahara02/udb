package udbclient

import (
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/entity/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
)

func TestFormatTimestampMatchesTheBrokerText(t *testing.T) {
	base := time.Date(2026, 10, 15, 8, 20, 0, 0, time.UTC)
	cases := map[time.Duration]string{
		0:                             "2026-10-15T08:20:00.000000Z",
		800 * time.Millisecond:        "2026-10-15T08:20:00.800000Z",
		807240 * time.Microsecond:     "2026-10-15T08:20:00.807240Z",
		807240*time.Microsecond + 999: "2026-10-15T08:20:00.807240Z",
	}
	for offset, want := range cases {
		if got := FormatTimestamp(base.Add(offset)); got != want {
			t.Errorf("FormatTimestamp(+%v) = %q, want %q", offset, got, want)
		}
	}
	local := base.In(time.FixedZone("UTC+6", 6*3600))
	if got := FormatTimestamp(local); got != "2026-10-15T08:20:00.000000Z" {
		t.Errorf("a non-UTC time renders in UTC: got %q", got)
	}
	parsed, err := ParseTimestamp(FormatTimestamp(base.Add(807240 * time.Microsecond)))
	if err != nil || !parsed.Equal(base.Add(807240*time.Microsecond)) {
		t.Errorf("round trip: %v %v", parsed, err)
	}
}

func TestMaskedDecodeLeavesTheFieldUnset(t *testing.T) {
	var file storagev1.File
	if err := DecodeRecord(Record{"file_id": "f", "size_bytes": "***MASKED***", "filename": "***MASKED***"}, &file); err != nil {
		t.Fatalf("a masked column fails the row: %v", err)
	}
	if file.GetSizeBytes() != 0 || file.GetFileId() != "f" {
		t.Fatalf("a masked number was kept: %v", &file)
	}
	// An unprotected text column may really hold the text.
	if file.GetFilename() != "***MASKED***" {
		t.Fatalf("a stored value was dropped: %v", &file)
	}
	var device authnv1.Device
	if err := DecodeRecord(Record{"last_ip_masked": "***MASKED***"}, &device); err != nil {
		t.Fatal(err)
	}
	if device.GetLastIpMasked() != "" {
		t.Fatalf("a protected column kept its placeholder: %v", &device)
	}
}
