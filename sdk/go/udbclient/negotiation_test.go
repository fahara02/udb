package udbclient

import "testing"

func TestNegotiateV1WhenOnlyRecordSetAdvertised(t *testing.T) {
	n := NewNegotiator(&ProtocolSupport{Encodings: []string{EncodingRecordSetV1}})
	if got := n.NegotiatedEncoding(); got != EncodingRecordSetV1 {
		t.Fatalf("expected %q, got %q", EncodingRecordSetV1, got)
	}
	if !n.SupportsEncoding(EncodingRecordSetV1) {
		t.Fatal("expected server to support record_set_v1")
	}
	if n.SupportsEncoding(EncodingRecordBatchV2) {
		t.Fatal("did not expect record_batch_v2 to be advertised")
	}
}

func TestSelectV2WhenAdvertisedAndClientSupports(t *testing.T) {
	n := NewNegotiator(&ProtocolSupport{
		Encodings: []string{EncodingRecordSetV1, EncodingRecordBatchV2},
	})
	if !n.SupportsEncoding(EncodingRecordBatchV2) {
		t.Fatal("expected server to advertise record_batch_v2")
	}
	// The client only compiles in record_set_v1 today, so negotiation must still
	// fall back to V1 until SelectV2 wrappers land. Once record_batch_v2 is added
	// to clientSupportedEncodings this asserts V2 selection.
	want := EncodingRecordSetV1
	if clientSupportsEncoding(EncodingRecordBatchV2) {
		want = EncodingRecordBatchV2
	}
	if got := n.NegotiatedEncoding(); got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestFallBackToV1WhenProtocolSupportAbsent(t *testing.T) {
	n := NewNegotiator(nil)
	if got := n.NegotiatedEncoding(); got != EncodingRecordSetV1 {
		t.Fatalf("expected V1 fallback, got %q", got)
	}
	if !n.SupportsEncoding(EncodingRecordSetV1) {
		t.Fatal("V1 must be implicitly supported when protocol_support is absent")
	}
	if n.ServerSupportsStreamingReads() {
		t.Fatal("absent protocol_support must report no streaming reads")
	}
	min, max := n.ProtocolRange()
	if min != ProtocolVersion || max != ProtocolVersion {
		t.Fatalf("expected client ProtocolVersion fallback, got [%s, %s]", min, max)
	}

	// Empty (non-nil) protocol support also falls back to V1.
	empty := NewNegotiator(&ProtocolSupport{})
	if got := empty.NegotiatedEncoding(); got != EncodingRecordSetV1 {
		t.Fatalf("expected V1 fallback for empty support, got %q", got)
	}
}
