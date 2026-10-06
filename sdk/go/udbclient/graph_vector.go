package udbclient

import (
	"context"
	"fmt"

	udbcommonv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// Graph and vector query builders over GraphQuery's typed traversal and
// VectorHybridSearch, so callers stop assembling Cypher strings and request
// structs by hand:
//
//	rows, err := udbclient.GraphFrom[*notesv1.Note](u, noteID).
//		Out("RELATED").Depth(1, 2).Where("archived", "false").Run(ctx)
//
//	hits, err := udbclient.VectorsHybrid[*notesv1.Note](u, "notes").
//		Text("how to rotate keys").Near(embedding).Limit(10).Run(ctx)

// GraphLabel is the node label UDB projects entity m under: the graph
// data_store's node_label (or udb.neo4j_label), else its resource name, else
// the entity's table name.
func GraphLabel(m proto.Message) string {
	opts := m.ProtoReflect().Descriptor().Options()
	if store, ok := proto.GetExtension(opts, udbcommonv1.E_DataStore).(*udbcommonv1.GenericStoreOptions); ok && store != nil {
		if store.GetBackend() == "neo4j" || store.GetStoreKind() == "graph" {
			for _, key := range []string{"udb.neo4j_label", "node_label"} {
				if label := store.GetOptions()[key]; label != "" {
					return label
				}
			}
			if store.GetResourceName() != "" {
				return store.GetResourceName()
			}
		}
	}
	return tableOptions(m).GetTableName()
}

// GraphTraversalBuilder builds a typed traversal from one node.
type GraphTraversalBuilder struct {
	u         *Udb
	resource  *entityv1.StoreResource
	traversal *entityv1.GraphTraversal
}

// GraphFrom starts a traversal at the node of entity T with id.
func GraphFrom[T proto.Message](u *Udb, id string) *GraphTraversalBuilder {
	var zero T
	m := zero.ProtoReflect().Type().New().Interface()
	return &GraphTraversalBuilder{
		u:        u,
		resource: &entityv1.StoreResource{Backend: "neo4j", MessageType: MessageType(m)},
		traversal: &entityv1.GraphTraversal{
			StartLabel: GraphLabel(m),
			StartId:    id,
			Direction:  entityv1.GraphTraversalDirection_GRAPH_TRAVERSAL_DIRECTION_OUTGOING,
		},
	}
}

func (b *GraphTraversalBuilder) along(direction entityv1.GraphTraversalDirection, relationships []string) *GraphTraversalBuilder {
	b.traversal.Direction = direction
	b.traversal.RelationshipTypes = append(b.traversal.RelationshipTypes, relationships...)
	return b
}

// Out follows outgoing relationships of the given types (any type when none).
func (b *GraphTraversalBuilder) Out(relationships ...string) *GraphTraversalBuilder {
	return b.along(entityv1.GraphTraversalDirection_GRAPH_TRAVERSAL_DIRECTION_OUTGOING, relationships)
}

// In follows incoming relationships of the given types.
func (b *GraphTraversalBuilder) In(relationships ...string) *GraphTraversalBuilder {
	return b.along(entityv1.GraphTraversalDirection_GRAPH_TRAVERSAL_DIRECTION_INCOMING, relationships)
}

// Both follows relationships of the given types in either direction.
func (b *GraphTraversalBuilder) Both(relationships ...string) *GraphTraversalBuilder {
	return b.along(entityv1.GraphTraversalDirection_GRAPH_TRAVERSAL_DIRECTION_BOTH, relationships)
}

// Depth bounds the number of hops (the broker clamps the maximum).
func (b *GraphTraversalBuilder) Depth(min, max int32) *GraphTraversalBuilder {
	b.traversal.MinDepth, b.traversal.MaxDepth = min, max
	return b
}

// Labels keeps only end nodes carrying one of the labels.
func (b *GraphTraversalBuilder) Labels(labels ...string) *GraphTraversalBuilder {
	b.traversal.NodeLabels = append(b.traversal.NodeLabels, labels...)
	return b
}

// Where keeps end nodes whose property equals value.
func (b *GraphTraversalBuilder) Where(property, value string) *GraphTraversalBuilder {
	if b.traversal.NodePropertyEquals == nil {
		b.traversal.NodePropertyEquals = map[string]string{}
	}
	b.traversal.NodePropertyEquals[property] = value
	return b
}

// WhereEdge keeps paths whose relationships carry property = value.
func (b *GraphTraversalBuilder) WhereEdge(property, value string) *GraphTraversalBuilder {
	if b.traversal.RelationshipPropertyEquals == nil {
		b.traversal.RelationshipPropertyEquals = map[string]string{}
	}
	b.traversal.RelationshipPropertyEquals[property] = value
	return b
}

// Limit caps the returned rows.
func (b *GraphTraversalBuilder) Limit(n int32) *GraphTraversalBuilder {
	b.traversal.Limit = n
	return b
}

// WithEdges returns the relationships along each path as well as the nodes.
func (b *GraphTraversalBuilder) WithEdges() *GraphTraversalBuilder {
	b.traversal.ReturnRelationships = true
	return b
}

// Request is the GraphQuery request the builder sends.
func (b *GraphTraversalBuilder) Request() *entityv1.GraphQueryRequest {
	return &entityv1.GraphQueryRequest{
		Resource:  b.resource,
		Traversal: b.traversal,
		ReadOnly:  true,
		Limit:     b.traversal.GetLimit(),
	}
}

// Run executes the traversal; each row is one result record.
func (b *GraphTraversalBuilder) Run(ctx context.Context) ([]map[string]any, error) {
	if b.traversal.GetStartId() == "" {
		return nil, fmt.Errorf("udb: graph traversal needs a start node id")
	}
	res, err := b.u.Data.Broker.GraphQuery(ctx, b.Request())
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]any, 0, len(res.GetRecords()))
	for _, record := range res.GetRecords() {
		rows = append(rows, record.AsMap())
	}
	return rows, nil
}

// VectorSearchBuilder builds a hybrid (dense + text) vector search.
type VectorSearchBuilder[T proto.Message] struct {
	u   *Udb
	req *entityv1.VectorHybridSearchRequest
}

// VectorHit is one search result: the point id, its score and the payload
// decoded as T where the payload carries T's fields.
type VectorHit[T proto.Message] struct {
	ID      string
	Score   float32
	Row     T
	Payload map[string]any
}

// VectorsHybrid starts a hybrid search over collection (the vector
// projection's resource name).
func VectorsHybrid[T proto.Message](u *Udb, collection string) *VectorSearchBuilder[T] {
	return &VectorSearchBuilder[T]{u: u, req: &entityv1.VectorHybridSearchRequest{
		Collection:  collection,
		WithPayload: true,
		Limit:       10,
	}}
}

// Text sets the text leg (Postgres full-text search when the projection
// declares fts_columns, else the store's own text matching).
func (b *VectorSearchBuilder[T]) Text(query string) *VectorSearchBuilder[T] {
	b.req.TextQuery = query
	return b
}

// Near sets the dense leg's query vector.
func (b *VectorSearchBuilder[T]) Near(vector []float32) *VectorSearchBuilder[T] {
	b.req.Vector = vector
	return b
}

// Weights sets the fusion weights as [dense, text].
func (b *VectorSearchBuilder[T]) Weights(dense, text float32) *VectorSearchBuilder[T] {
	b.req.FusionWeights = []float32{dense, text}
	return b
}

// Filter adds payload equality conditions (Qdrant match semantics).
func (b *VectorSearchBuilder[T]) Filter(conditions map[string]any) *VectorSearchBuilder[T] {
	filter, err := structpb.NewStruct(conditions)
	if err == nil {
		b.req.Filter = filter
	}
	return b
}

// Limit caps the hits.
func (b *VectorSearchBuilder[T]) Limit(n int32) *VectorSearchBuilder[T] {
	b.req.Limit = n
	return b
}

// Request is the VectorHybridSearch request the builder sends.
func (b *VectorSearchBuilder[T]) Request() *entityv1.VectorHybridSearchRequest { return b.req }

// Run executes the search.
func (b *VectorSearchBuilder[T]) Run(ctx context.Context) ([]VectorHit[T], error) {
	if len(b.req.GetVector()) == 0 && b.req.GetTextQuery() == "" {
		return nil, fmt.Errorf("udb: vector search needs Near(vector) and/or Text(query)")
	}
	res, err := b.u.Data.Broker.VectorHybridSearch(ctx, b.req)
	if err != nil {
		return nil, err
	}
	hits := make([]VectorHit[T], 0, len(res.GetPoints()))
	for _, point := range res.GetPoints() {
		var zero T
		row := zero.ProtoReflect().Type().New().Interface().(T)
		payload := point.GetPayload().AsMap()
		if err := DecodeRecord(Record(payload), row); err != nil {
			return nil, fmt.Errorf("udb: vector hit %s: %w", point.GetId(), err)
		}
		hits = append(hits, VectorHit[T]{ID: point.GetId(), Score: point.GetScore(), Row: row, Payload: payload})
	}
	return hits, nil
}
