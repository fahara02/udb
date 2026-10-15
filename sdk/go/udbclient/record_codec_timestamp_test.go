package udbclient

import (
	"testing"
	"time"
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
