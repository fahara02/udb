"use strict";
// Protocol-negotiation helpers for the UDB wire protocol.
//
// The server advertises protocol support on CapabilitiesResponse.protocol_support
// (field 9). The bundled .proto / generated stubs may not yet carry that field, so
// these helpers read it DEFENSIVELY from the loosely-typed object returned by
// @grpc/proto-loader. A missing field, undefined, or an empty `encodings` array
// falls back to V1 (record_set_v1 / the Select+RecordSet path). V2 is never
// hard-required.
Object.defineProperty(exports, "__esModule", { value: true });
exports.Negotiator = exports.CLIENT_SUPPORTED_ENCODINGS = exports.ENCODING_RECORD_BATCH_V2 = exports.ENCODING_RECORD_SET_V1 = void 0;
const client_1 = require("./client");
/** The always-supported V1 row encoding (Select/RecordSet). */
exports.ENCODING_RECORD_SET_V1 = "record_set_v1";
/** The additive typed-column batch encoding (SelectV2), used only when both server
 * and client advertise it. */
exports.ENCODING_RECORD_BATCH_V2 = "record_batch_v2";
/** Row encodings this SDK build can decode, most preferred first. record_set_v1 is
 * always supported; record_batch_v2 becomes usable once SelectV2 wrappers land
 * (task A.4). */
exports.CLIENT_SUPPORTED_ENCODINGS = [exports.ENCODING_RECORD_SET_V1];
function extractSupport(source) {
    if (source == null)
        return undefined;
    const caps = source;
    if (caps.protocol_support != null)
        return caps.protocol_support;
    if (caps.protocolSupport != null)
        return caps.protocolSupport;
    // Already a protocol_support-shaped object?
    const ps = source;
    if (ps.encodings != null || ps.min_protocol_version != null || ps.minProtocolVersion != null) {
        return ps;
    }
    return undefined;
}
/** Picks the best encoding shared by this client and a server's protocol support.
 * Accepts a CapabilitiesResponse, a bare protocol_support object, or null/undefined
 * (server advertised nothing). null and empty inputs negotiate V1. */
class Negotiator {
    support;
    constructor(source) {
        this.support = extractSupport(source);
    }
    encodings() {
        const raw = this.support?.encodings;
        return Array.isArray(raw) ? raw.map(String) : [];
    }
    /** Whether the server advertises `name`. V1 is implicit when the server
     * advertised no encodings. */
    supportsEncoding(name) {
        const encodings = this.encodings();
        if (encodings.length === 0) {
            return name === exports.ENCODING_RECORD_SET_V1;
        }
        return encodings.includes(name);
    }
    /** Returns "record_batch_v2" only if the server advertises it AND this client
     * supports it; otherwise falls back to "record_set_v1". */
    negotiatedEncoding() {
        if (exports.CLIENT_SUPPORTED_ENCODINGS.includes(exports.ENCODING_RECORD_BATCH_V2) &&
            this.supportsEncoding(exports.ENCODING_RECORD_BATCH_V2)) {
            return exports.ENCODING_RECORD_BATCH_V2;
        }
        return exports.ENCODING_RECORD_SET_V1;
    }
    /** Server's [min, max] protocol version, falling back to the client's
     * compiled-in version when unknown. */
    protocolRange() {
        const min = this.support?.min_protocol_version ||
            this.support?.minProtocolVersion ||
            client_1.UDB_PROTOCOL_VERSION;
        const max = this.support?.max_protocol_version ||
            this.support?.maxProtocolVersion ||
            client_1.UDB_PROTOCOL_VERSION;
        return [min, max];
    }
    /** Whether the server advertises streaming reads. Absent support is false
     * (V1 unary behavior). */
    serverSupportsStreamingReads() {
        return Boolean(this.support?.supports_streaming_reads ?? this.support?.supportsStreamingReads ?? false);
    }
}
exports.Negotiator = Negotiator;
