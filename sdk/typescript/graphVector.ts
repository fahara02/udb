// Graph and vector query builders over GraphQuery's typed traversal and
// VectorHybridSearch, so callers stop assembling Cypher strings and request
// structures by hand:
//
//   const rows = await graphFrom(udb, "notes.v1.Note", noteId)
//     .out("RELATED").depth(1, 2).where("archived", "false").run();
//
//   const hits = await vectorsHybrid<Note>(udb, "notes")
//     .text("how to rotate keys").near(embedding).limit(10).run();

import { CallOptions, UdbCore } from "./generatedClient";
import { graphLabel, SchemaOptions } from "./schema";
import { structToObject } from "./wkt";

const DATABROKER = "udb.services.v1.DataBroker";

/** What the builders need from a client. `UdbProject` satisfies it. */
export interface StoreQueryHost {
  core: UdbCore;
}

export type GraphTraversalDirection = "OUTGOING" | "INCOMING" | "BOTH";

/** Builds a typed traversal from one node. */
export class GraphTraversalBuilder {
  private readonly messageType: string;
  private readonly traversal: {
    start_label: string;
    start_id: string;
    direction: string;
    relationship_types: string[];
    min_depth: number;
    max_depth: number;
    node_labels: string[];
    node_property_equals: Record<string, string>;
    relationship_property_equals: Record<string, string>;
    limit: number;
    return_relationships: boolean;
  };

  constructor(
    private readonly udb: StoreQueryHost,
    messageType: string,
    id: string,
    label: string,
  ) {
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

  private along(direction: GraphTraversalDirection, relationships: string[]): this {
    this.traversal.direction = `GRAPH_TRAVERSAL_DIRECTION_${direction}`;
    this.traversal.relationship_types.push(...relationships);
    return this;
  }

  /** Follow outgoing relationships of the given types (any type when none). */
  out(...relationships: string[]): this {
    return this.along("OUTGOING", relationships);
  }

  /** Follow incoming relationships of the given types. */
  in(...relationships: string[]): this {
    return this.along("INCOMING", relationships);
  }

  /** Follow relationships of the given types in either direction. */
  both(...relationships: string[]): this {
    return this.along("BOTH", relationships);
  }

  /** Bound the number of hops (the broker clamps the maximum). */
  depth(min: number, max: number): this {
    this.traversal.min_depth = min;
    this.traversal.max_depth = max;
    return this;
  }

  /** Keep only end nodes carrying one of the labels. */
  labels(...labels: string[]): this {
    this.traversal.node_labels.push(...labels);
    return this;
  }

  /** Keep end nodes whose property equals `value`. */
  where(property: string, value: string): this {
    this.traversal.node_property_equals[property] = value;
    return this;
  }

  /** Keep paths whose relationships carry property = `value`. */
  whereEdge(property: string, value: string): this {
    this.traversal.relationship_property_equals[property] = value;
    return this;
  }

  /** Cap the returned rows. */
  limit(n: number): this {
    this.traversal.limit = n;
    return this;
  }

  /** Return the relationships along each path as well as the nodes. */
  withEdges(): this {
    this.traversal.return_relationships = true;
    return this;
  }

  /** The GraphQuery request the builder sends. */
  request(): Record<string, unknown> {
    return {
      resource: { backend: "neo4j", message_type: this.messageType },
      traversal: this.traversal,
      read_only: true,
      limit: this.traversal.limit,
    };
  }

  /** Run the traversal; each row is one result record. */
  async run(call?: CallOptions): Promise<Array<Record<string, unknown>>> {
    if (!this.traversal.start_id) throw new Error("udb: graph traversal needs a start node id");
    const response: any = await this.udb.core.unary(DATABROKER, "GraphQuery", this.request(), call);
    return (response?.records ?? []).map((record: unknown) => structToObject(record));
  }
}

/** Start a traversal at the node of entity `messageType` with `id`. The start label
 *  comes from the entity's annotations; pass `label` to name it yourself, and
 *  `protoRoot` for a message outside the SDK's bundled proto tree. */
export function graphFrom(
  udb: StoreQueryHost,
  messageType: string,
  id: string,
  options: SchemaOptions & { label?: string } = {},
): GraphTraversalBuilder {
  return new GraphTraversalBuilder(udb, messageType, id, options.label ?? graphLabel(messageType, options));
}

/** One search result: the point id, its score, and the payload decoded as `T`. */
export interface VectorHit<T> {
  id: string;
  score: number;
  row: T;
  payload: Record<string, unknown>;
}

/** Builds a hybrid (dense + text) vector search. */
export class VectorSearchBuilder<T = Record<string, unknown>> {
  private readonly req: {
    collection: string;
    with_payload: boolean;
    limit: number;
    text_query: string;
    vector: number[];
    fusion_weights: number[];
    filter?: Record<string, unknown>;
  };

  constructor(
    private readonly udb: StoreQueryHost,
    collection: string,
    private readonly decode?: (payload: Record<string, unknown>) => T,
  ) {
    this.req = { collection, with_payload: true, limit: 10, text_query: "", vector: [], fusion_weights: [] };
  }

  /** Set the text leg (Postgres full-text search when the projection declares
   *  fts_columns, else the store's own text matching). */
  text(query: string): this {
    this.req.text_query = query;
    return this;
  }

  /** Set the dense leg's query vector. */
  near(vector: number[]): this {
    this.req.vector = vector;
    return this;
  }

  /** Set the fusion weights. */
  weights(dense: number, text: number): this {
    this.req.fusion_weights = [dense, text];
    return this;
  }

  /** Add payload equality conditions (Qdrant match semantics). */
  filter(conditions: Record<string, unknown>): this {
    this.req.filter = conditions;
    return this;
  }

  /** Cap the hits. */
  limit(n: number): this {
    this.req.limit = n;
    return this;
  }

  /** The VectorHybridSearch request the builder sends. */
  request(): Record<string, unknown> {
    return this.req;
  }

  /** Run the search. */
  async run(call?: CallOptions): Promise<Array<VectorHit<T>>> {
    if (this.req.vector.length === 0 && !this.req.text_query) {
      throw new Error("udb: vector search needs near(vector) and/or text(query)");
    }
    const response: any = await this.udb.core.unary(DATABROKER, "VectorHybridSearch", this.req, call);
    return (response?.points ?? []).map((point: any) => {
      const payload = structToObject(point?.payload);
      let row: T;
      try {
        row = this.decode ? this.decode(payload) : (payload as T);
      } catch (err) {
        throw new Error(`udb: vector hit ${point?.id}: ${(err as Error).message}`, { cause: err });
      }
      return { id: point?.id ?? "", score: Number(point?.score ?? 0), row, payload };
    });
  }
}

/** Start a hybrid search over `collection` (the vector projection's resource name).
 *  `decode` turns a hit's payload into `T`; by default the payload is the row. */
export function vectorsHybrid<T = Record<string, unknown>>(
  udb: StoreQueryHost,
  collection: string,
  options: { decode?: (payload: Record<string, unknown>) => T } = {},
): VectorSearchBuilder<T> {
  return new VectorSearchBuilder<T>(udb, collection, options.decode);
}
