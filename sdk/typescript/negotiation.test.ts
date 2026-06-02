// Pure unit tests over a constructed/fake CapabilitiesResponse — no live server.
// Run with Node's built-in test runner over compiled JS, e.g.
//   npx tsc -p tsconfig.build.json && node --test dist
// or via a TypeScript loader. No extra test-framework dependency is required.

import { strict as assert } from "node:assert";
import { test } from "node:test";

import {
  CLIENT_SUPPORTED_ENCODINGS,
  ENCODING_RECORD_BATCH_V2,
  ENCODING_RECORD_SET_V1,
  Negotiator,
} from "./negotiation";
import { UDB_PROTOCOL_VERSION } from "./client";

test("negotiates V1 when only record_set_v1 is advertised", () => {
  const caps = { protocol_support: { encodings: [ENCODING_RECORD_SET_V1] } };
  const neg = new Negotiator(caps);
  assert.equal(neg.negotiatedEncoding(), ENCODING_RECORD_SET_V1);
  assert.equal(neg.supportsEncoding(ENCODING_RECORD_SET_V1), true);
  assert.equal(neg.supportsEncoding(ENCODING_RECORD_BATCH_V2), false);
});

test("selects V2 when advertised and client supports it", () => {
  const caps = {
    protocol_support: {
      encodings: [ENCODING_RECORD_SET_V1, ENCODING_RECORD_BATCH_V2],
    },
  };
  const neg = new Negotiator(caps);
  assert.equal(neg.supportsEncoding(ENCODING_RECORD_BATCH_V2), true);
  // Client only compiles in V1 today, so it falls back until SelectV2 wrappers land.
  // Once record_batch_v2 is added to CLIENT_SUPPORTED_ENCODINGS this asserts V2.
  const expected = CLIENT_SUPPORTED_ENCODINGS.includes(ENCODING_RECORD_BATCH_V2)
    ? ENCODING_RECORD_BATCH_V2
    : ENCODING_RECORD_SET_V1;
  assert.equal(neg.negotiatedEncoding(), expected);
});

test("falls back to V1 when protocol_support is absent or empty", () => {
  // null source (old stub / nothing advertised).
  const negNull = new Negotiator(null);
  assert.equal(negNull.negotiatedEncoding(), ENCODING_RECORD_SET_V1);
  assert.equal(negNull.supportsEncoding(ENCODING_RECORD_SET_V1), true);
  assert.equal(negNull.serverSupportsStreamingReads(), false);
  assert.deepEqual(negNull.protocolRange(), [UDB_PROTOCOL_VERSION, UDB_PROTOCOL_VERSION]);

  // CapabilitiesResponse with no protocol_support field.
  const negNoField = new Negotiator({});
  assert.equal(negNoField.negotiatedEncoding(), ENCODING_RECORD_SET_V1);

  // protocol_support present but with an empty encodings array.
  const negEmpty = new Negotiator({ protocol_support: { encodings: [] } });
  assert.equal(negEmpty.negotiatedEncoding(), ENCODING_RECORD_SET_V1);
});

test("reads streaming + protocol range from a populated protocol_support", () => {
  const neg = new Negotiator({
    protocol_support: {
      encodings: [ENCODING_RECORD_SET_V1],
      min_protocol_version: "1.0.0",
      max_protocol_version: "2.0.0",
      supports_streaming_reads: true,
    },
  });
  assert.equal(neg.serverSupportsStreamingReads(), true);
  assert.deepEqual(neg.protocolRange(), ["1.0.0", "2.0.0"]);
});
