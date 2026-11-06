"use strict";
// Graph and vector query builders over GraphQuery's typed traversal and
// VectorHybridSearch, so callers stop assembling Cypher strings and request
// structures by hand:
//
//   const rows = await graphFrom(udb, "notes.v1.Note", noteId)
//     .out("RELATED").depth(1, 2).where("archived", "false").run();
//
//   const hits = await vectorsHybrid<Note>(udb, "notes")
//     .text("how to rotate keys").near(embedding).limit(10).run();
Object.defineProperty(exports, "__esModule", { value: true });
exports.VectorSearchBuilder = exports.GraphTraversalBuilder = void 0;
exports.graphFrom = graphFrom;
exports.vectorsHybrid = vectorsHybrid;
const schema_1 = require("./schema");
const wkt_1 = require("./wkt");
const DATABROKER = "udb.services.v1.DataBroker";
/** Builds a typed traversal from one node. */
class GraphTraversalBuilder {
    udb;
    messageType;
    traversal;
    constructor(udb, messageType, id, label) {
        this.udb = udb;
        this.messageType = messageType;
        this.traversal = {
            start_label: label,
            start_id: id,
            direction: "GRAPH_TRAVERSAL_DIRECTION_OUTGOING",
            relationship_types: [],
            min_depth: 0,
            max_depth: 0,
            node_labels: [],
            node_property_equals: {},
            relationship_property_equals: {},
            limit: 0,
            return_relationships: false,
        };
    }
    along(direction, relationships) {
        this.traversal.direction = `GRAPH_TRAVERSAL_DIRECTION_${direction}`;
        this.traversal.relationship_types.push(...relationships);
        return this;
    }
    /** Follow outgoing relationships of the given types (any type when none). */
    out(...relationships) {
        return this.along("OUTGOING", relationships);
    }
    /** Follow incoming relationships of the given types. */
    in(...relationships) {
        return this.along("INCOMING", relationships);
    }
    /** Follow relationships of the given types in either direction. */
    both(...relationships) {
        return this.along("BOTH", relationships);
    }
    /** Bound the number of hops (the broker clamps the maximum). */
    depth(min, max) {
        this.traversal.min_depth = min;
        this.traversal.max_depth = max;
        return this;
    }
    /** Keep only end nodes carrying one of the labels. */
    labels(...labels) {
        this.traversal.node_labels.push(...labels);
        return this;
    }
    /** Keep end nodes whose property equals `value`. */
    where(property, value) {
        this.traversal.node_property_equals[property] = value;
        return this;
    }
    /** Keep paths whose relationships carry property = `value`. */
    whereEdge(property, value) {
        this.traversal.relationship_property_equals[property] = value;
        return this;
    }
    /** Cap the returned rows. */
    limit(n) {
        this.traversal.limit = n;
        return this;
    }
    /** Return the relationships along each path as well as the nodes. */
    withEdges() {
        this.traversal.return_relationships = true;
        return this;
    }
    /** The GraphQuery request the builder sends. */
    request() {
        return {
            resource: { backend: "neo4j", message_type: this.messageType },
            traversal: this.traversal,
            read_only: true,
            limit: this.traversal.limit,
        };
    }
    /** Run the traversal; each row is one result record. */
    async run(call) {
        if (!this.traversal.start_id)
            throw new Error("udb: graph traversal needs a start node id");
        const response = await this.udb.core.unary(DATABROKER, "GraphQuery", this.request(), call);
        return (response?.records ?? []).map((record) => (0, wkt_1.structToObject)(record));
    }
}
exports.GraphTraversalBuilder = GraphTraversalBuilder;
/** Start a traversal at the node of entity `messageType` with `id`. The start label
 *  comes from the entity's annotations; pass `label` to name it yourself, and
 *  `protoRoot` for a message outside the SDK's bundled proto tree. */
function graphFrom(udb, messageType, id, options = {}) {
    return new GraphTraversalBuilder(udb, messageType, id, options.label ?? (0, schema_1.graphLabel)(messageType, options));
}
/** Builds a hybrid (dense + text) vector search. */
class VectorSearchBuilder {
    udb;
    decode;
    req;
    constructor(udb, collection, decode) {
        this.udb = udb;
        this.decode = decode;
        this.req = { collection, with_payload: true, limit: 10, text_query: "", vector: [], fusion_weights: [] };
    }
    /** Set the text leg (Postgres full-text search when the projection declares
     *  fts_columns, else the store's own text matching). */
    text(query) {
        this.req.text_query = query;
        return this;
    }
    /** Set the dense leg's query vector. */
    near(vector) {
        this.req.vector = vector;
        return this;
    }
    /** Set the fusion weights. */
    weights(dense, text) {
        this.req.fusion_weights = [dense, text];
        return this;
    }
    /** Add payload equality conditions (Qdrant match semantics). */
    filter(conditions) {
        this.req.filter = conditions;
        return this;
    }
    /** Cap the hits. */
    limit(n) {
        this.req.limit = n;
        return this;
    }
    /** The VectorHybridSearch request the builder sends. */
    request() {
        return this.req;
    }
    /** Run the search. */
    async run(call) {
        if (this.req.vector.length === 0 && !this.req.text_query) {
            throw new Error("udb: vector search needs near(vector) and/or text(query)");
        }
        const response = await this.udb.core.unary(DATABROKER, "VectorHybridSearch", this.req, call);
        return (response?.points ?? []).map((point) => {
            const payload = (0, wkt_1.structToObject)(point?.payload);
            let row;
            try {
                row = this.decode ? this.decode(payload) : payload;
            }
            catch (err) {
                throw new Error(`udb: vector hit ${point?.id}: ${err.message}`, { cause: err });
            }
            return { id: point?.id ?? "", score: Number(point?.score ?? 0), row, payload };
        });
    }
}
exports.VectorSearchBuilder = VectorSearchBuilder;
/** Start a hybrid search over `collection` (the vector projection's resource name).
 *  `decode` turns a hit's payload into `T`; by default the payload is the row. */
function vectorsHybrid(udb, collection, options = {}) {
    return new VectorSearchBuilder(udb, collection, options.decode);
}
