package udbclient

// Protocol-negotiation helpers for the UDB wire protocol.
//
// The server advertises protocol support on CapabilitiesResponse.protocol_support
// (field 9). Generated stubs may not yet carry that field, so these helpers read it
// DEFENSIVELY through a small structural view rather than depending on a generated
// type. Anything absent/empty falls back to V1 (record_set_v1 / the Select+RecordSet
// path) and never hard-requires V2.

// Wire encoding identifiers.
const (
	// EncodingRecordSetV1 is the always-supported V1 row encoding (Select/RecordSet).
	EncodingRecordSetV1 = "record_set_v1"
	// EncodingRecordBatchV2 is the additive typed-column batch encoding (SelectV2),
	// used only when both server and client advertise it.
	EncodingRecordBatchV2 = "record_batch_v2"
)

// ProtocolSupport is a defensive, hand-written view of the server's
// CapabilitiesResponse.protocol_support. It mirrors the proto field names but does
// not depend on the generated stub, so it works before the SDK is regenerated.
type ProtocolSupport struct {
	MinProtocolVersion     string
	MaxProtocolVersion     string
	Encodings              []string
	Compression            []string
	SupportsStreamingReads bool
	SupportsObjectStream   bool
	MaxRecvMessageBytes    int64
	MaxSendMessageBytes    int64
	SupportedRpcs          []string
}

// clientSupportedEncodings lists the row encodings this SDK build can decode, most
// preferred first. record_set_v1 is always supported; record_batch_v2 will become
// fully usable once SelectV2 wrappers land (task A.4).
var clientSupportedEncodings = []string{EncodingRecordSetV1}

// Negotiator picks the best encoding shared by the client and a server's
// ProtocolSupport. A nil *ProtocolSupport (server advertised nothing, or an old
// stub with no field) safely negotiates V1.
type Negotiator struct {
	support *ProtocolSupport
}

// NewNegotiator builds a Negotiator from server-advertised protocol support.
// Passing nil is valid and yields V1-only behavior.
func NewNegotiator(support *ProtocolSupport) *Negotiator {
	return &Negotiator{support: support}
}

func contains(list []string, name string) bool {
	for _, item := range list {
		if item == name {
			return true
		}
	}
	return false
}

func clientSupportsEncoding(name string) bool {
	return contains(clientSupportedEncodings, name)
}

// SupportsEncoding reports whether the server advertises the named encoding.
func (n *Negotiator) SupportsEncoding(name string) bool {
	if n == nil || n.support == nil {
		// V1 is implicit even when the server advertised no protocol_support.
		return name == EncodingRecordSetV1
	}
	if len(n.support.Encodings) == 0 {
		return name == EncodingRecordSetV1
	}
	return contains(n.support.Encodings, name)
}

// NegotiatedEncoding returns "record_batch_v2" only if the server advertises it AND
// this client supports it; otherwise it falls back to "record_set_v1".
func (n *Negotiator) NegotiatedEncoding() string {
	if clientSupportsEncoding(EncodingRecordBatchV2) && n.SupportsEncoding(EncodingRecordBatchV2) {
		return EncodingRecordBatchV2
	}
	return EncodingRecordSetV1
}

// ProtocolRange returns the server's [min, max] protocol version. When unknown it
// falls back to the client's compiled-in ProtocolVersion for both bounds.
func (n *Negotiator) ProtocolRange() (min string, max string) {
	if n == nil || n.support == nil {
		return ProtocolVersion, ProtocolVersion
	}
	min = n.support.MinProtocolVersion
	if min == "" {
		min = ProtocolVersion
	}
	max = n.support.MaxProtocolVersion
	if max == "" {
		max = ProtocolVersion
	}
	return min, max
}

// ServerSupportsStreamingReads reports whether the server advertises streaming
// reads. Absent protocol support is treated as false (V1 unary behavior).
func (n *Negotiator) ServerSupportsStreamingReads() bool {
	if n == nil || n.support == nil {
		return false
	}
	return n.support.SupportsStreamingReads
}
