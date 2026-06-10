"use strict";
// Pure unit tests over a constructed/fake CapabilitiesResponse — no live server.
// Run with Node's built-in test runner over compiled JS, e.g.
//   npx tsc -p tsconfig.build.json && node --test dist
// or via a TypeScript loader. No extra test-framework dependency is required.
Object.defineProperty(exports, "__esModule", { value: true });
const node_assert_1 = require("node:assert");
const node_test_1 = require("node:test");
const negotiation_1 = require("./negotiation");
const client_1 = require("./client");
(0, node_test_1.test)("negotiates V1 when only record_set_v1 is advertised", () => {
    const caps = { protocol_support: { encodings: [negotiation_1.ENCODING_RECORD_SET_V1] } };
    const neg = new negotiation_1.Negotiator(caps);
    node_assert_1.strict.equal(neg.negotiatedEncoding(), negotiation_1.ENCODING_RECORD_SET_V1);
    node_assert_1.strict.equal(neg.supportsEncoding(negotiation_1.ENCODING_RECORD_SET_V1), true);
    node_assert_1.strict.equal(neg.supportsEncoding(negotiation_1.ENCODING_RECORD_BATCH_V2), false);
});
(0, node_test_1.test)("selects V2 when advertised and client supports it", () => {
    const caps = {
        protocol_support: {
            encodings: [negotiation_1.ENCODING_RECORD_SET_V1, negotiation_1.ENCODING_RECORD_BATCH_V2],
        },
    };
    const neg = new negotiation_1.Negotiator(caps);
    node_assert_1.strict.equal(neg.supportsEncoding(negotiation_1.ENCODING_RECORD_BATCH_V2), true);
    // Client only compiles in V1 today, so it falls back until SelectV2 wrappers land.
    // Once record_batch_v2 is added to CLIENT_SUPPORTED_ENCODINGS this asserts V2.
    const expected = negotiation_1.CLIENT_SUPPORTED_ENCODINGS.includes(negotiation_1.ENCODING_RECORD_BATCH_V2)
        ? negotiation_1.ENCODING_RECORD_BATCH_V2
        : negotiation_1.ENCODING_RECORD_SET_V1;
    node_assert_1.strict.equal(neg.negotiatedEncoding(), expected);
});
(0, node_test_1.test)("falls back to V1 when protocol_support is absent or empty", () => {
    // null source (old stub / nothing advertised).
    const negNull = new negotiation_1.Negotiator(null);
    node_assert_1.strict.equal(negNull.negotiatedEncoding(), negotiation_1.ENCODING_RECORD_SET_V1);
    node_assert_1.strict.equal(negNull.supportsEncoding(negotiation_1.ENCODING_RECORD_SET_V1), true);
    node_assert_1.strict.equal(negNull.serverSupportsStreamingReads(), false);
    node_assert_1.strict.deepEqual(negNull.protocolRange(), [client_1.UDB_PROTOCOL_VERSION, client_1.UDB_PROTOCOL_VERSION]);
    // CapabilitiesResponse with no protocol_support field.
    const negNoField = new negotiation_1.Negotiator({});
    node_assert_1.strict.equal(negNoField.negotiatedEncoding(), negotiation_1.ENCODING_RECORD_SET_V1);
    // protocol_support present but with an empty encodings array.
    const negEmpty = new negotiation_1.Negotiator({ protocol_support: { encodings: [] } });
    node_assert_1.strict.equal(negEmpty.negotiatedEncoding(), negotiation_1.ENCODING_RECORD_SET_V1);
});
(0, node_test_1.test)("reads streaming + protocol range from a populated protocol_support", () => {
    const neg = new negotiation_1.Negotiator({
        protocol_support: {
            encodings: [negotiation_1.ENCODING_RECORD_SET_V1],
            min_protocol_version: "1.0.0",
            max_protocol_version: "2.0.0",
            supports_streaming_reads: true,
        },
    });
    node_assert_1.strict.equal(neg.serverSupportsStreamingReads(), true);
    node_assert_1.strict.deepEqual(neg.protocolRange(), ["1.0.0", "2.0.0"]);
});
