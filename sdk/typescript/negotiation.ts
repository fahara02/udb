// Protocol-negotiation helpers for the UDB wire protocol.
//
// The server advertises protocol support on CapabilitiesResponse.protocol_support
// (field 9). The bundled .proto / generated stubs may not yet carry that field, so
// these helpers read it DEFENSIVELY from the loosely-typed object returned by
// @grpc/proto-loader. A missing field, undefined, or an empty `encodings` array
// falls back to V1 (record_set_v1 / the Select+RecordSet path). V2 is never
// hard-required.

import { UDB_PROTOCOL_VERSION } from "./client";

/** The always-supported V1 row encoding (Select/RecordSet). */
export const ENCODING_RECORD_SET_V1 = "record_set_v1";
/** The additive typed-column batch encoding (SelectV2), used only when both server
 * and client advertise it. */
export const ENCODING_RECORD_BATCH_V2 = "record_batch_v2";

/** Row encodings this SDK build can decode, most preferred first. record_set_v1 is
 * always supported; record_batch_v2 becomes usable once SelectV2 wrappers land
 * (task A.4). */
export const CLIENT_SUPPORTED_ENCODINGS: readonly string[] = [ENCODING_RECORD_SET_V1];

/** Defensive shape of the server's protocol_support. All fields optional because
 * the generated stub may predate field 9. proto-loader uses snake_case keys
 * (keepCase: true); camelCase aliases are also accepted just in case. */
export interface ProtocolSupport {
  min_protocol_version?: string;
  max_protocol_version?: string;
  encodings?: string[];
  compression?: string[];
  supports_streaming_reads?: boolean;
  supports_object_streaming?: boolean;
  max_recv_message_bytes?: number | string;
  max_send_message_bytes?: number | string;
  supported_rpcs?: string[];
  // camelCase tolerance
  minProtocolVersion?: string;
  maxProtocolVersion?: string;
  supportsStreamingReads?: boolean;
}

/** A CapabilitiesResponse may or may not carry protocol_support yet. */
export interface CapabilitiesLike {
  protocol_support?: ProtocolSupport;
  protocolSupport?: ProtocolSupport;
}

function extractSupport(
  source: CapabilitiesLike | ProtocolSupport | null | undefined,
): ProtocolSupport | undefined {
  if (source == null) return undefined;
  const caps = source as CapabilitiesLike;
  if (caps.protocol_support != null) return caps.protocol_support;
  if (caps.protocolSupport != null) return caps.protocolSupport;
  // Already a protocol_support-shaped object?
  const ps = source as ProtocolSupport;
  if (ps.encodings != null || ps.min_protocol_version != null || ps.minProtocolVersion != null) {
    return ps;
  }
  return undefined;
}

/** Picks the best encoding shared by this client and a server's protocol support.
 * Accepts a CapabilitiesResponse, a bare protocol_support object, or null/undefined
 * (server advertised nothing). null and empty inputs negotiate V1. */
export class Negotiator {
  private readonly support?: ProtocolSupport;

  constructor(source?: CapabilitiesLike | ProtocolSupport | null) {
    this.support = extractSupport(source);
  }

  private encodings(): string[] {
    const raw = this.support?.encodings;
    return Array.isArray(raw) ? raw.map(String) : [];
  }

  /** Whether the server advertises `name`. V1 is implicit when the server
   * advertised no encodings. */
  supportsEncoding(name: string): boolean {
    const encodings = this.encodings();
    if (encodings.length === 0) {
      return name === ENCODING_RECORD_SET_V1;
    }
    return encodings.includes(name);
  }

  /** Returns "record_batch_v2" only if the server advertises it AND this client
   * supports it; otherwise falls back to "record_set_v1". */
  negotiatedEncoding(): string {
    if (
      CLIENT_SUPPORTED_ENCODINGS.includes(ENCODING_RECORD_BATCH_V2) &&
      this.supportsEncoding(ENCODING_RECORD_BATCH_V2)
    ) {
      return ENCODING_RECORD_BATCH_V2;
    }
    return ENCODING_RECORD_SET_V1;
  }

  /** Server's [min, max] protocol version, falling back to the client's
   * compiled-in version when unknown. */
  protocolRange(): [string, string] {
    const min =
      this.support?.min_protocol_version ||
      this.support?.minProtocolVersion ||
      UDB_PROTOCOL_VERSION;
    const max =
      this.support?.max_protocol_version ||
      this.support?.maxProtocolVersion ||
      UDB_PROTOCOL_VERSION;
    return [min, max];
  }

  /** Whether the server advertises streaming reads. Absent support is false
   * (V1 unary behavior). */
  serverSupportsStreamingReads(): boolean {
    return Boolean(
      this.support?.supports_streaming_reads ?? this.support?.supportsStreamingReads ?? false,
    );
  }
}
