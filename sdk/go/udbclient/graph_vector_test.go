package udbclient

import (
	"context"
	"testing"

	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
)

// The traversal builder fills the typed GraphTraversal the broker compiles,
// starting at T's node label (its table name when no graph store names one).
func TestGraphFromBuildsTheTypedTraversal(t *testing.T) {
	req := GraphFrom[*storagev1.File](nil, "f-1").
		Out("RELATED", "CITES").Depth(1, 2).Labels("Note").
		Where("archived", "false").WhereEdge("weight", "1").Limit(25).WithEdges().
		Request()
	tr := req.GetTraversal()
	if tr.GetStartLabel() != tableOptions(&storagev1.File{}).GetTableName() || tr.GetStartLabel() == "" {
		t.Fatalf("start label = %q", tr.GetStartLabel())
	}
	if tr.GetStartId() != "f-1" || tr.GetDirection() != entityv1.GraphTraversalDirection_GRAPH_TRAVERSAL_DIRECTION_OUTGOING {
		t.Fatalf("start = %q direction = %v", tr.GetStartId(), tr.GetDirection())
	}
	if len(tr.GetRelationshipTypes()) != 2 || tr.GetMinDepth() != 1 || tr.GetMaxDepth() != 2 || tr.GetLimit() != 25 {
		t.Fatalf("traversal = %+v", tr)
	}
	if tr.GetNodePropertyEquals()["archived"] != "false" || tr.GetRelationshipPropertyEquals()["weight"] != "1" || !tr.GetReturnRelationships() {
		t.Fatalf("filters = %+v", tr)
	}
	if !req.GetReadOnly() || req.GetResource().GetBackend() != "neo4j" || req.GetResource().GetMessageType() != MessageType(&storagev1.File{}) {
		t.Fatalf("resource = %+v", req.GetResource())
	}
	if _, err := GraphFrom[*storagev1.File](nil, "").Run(context.Background()); err == nil {
		t.Fatal("a traversal without a start id must be refused before calling the broker")
	}
}

func TestVectorsHybridBuildsTheRequest(t *testing.T) {
	req := VectorsHybrid[*storagev1.File](nil, "notes").
		Text("rotate keys").Near([]float32{0.1, 0.2}).Weights(0.7, 0.3).
		Filter(map[string]any{"topic": "security"}).Limit(5).Request()
	if req.GetCollection() != "notes" || req.GetTextQuery() != "rotate keys" || len(req.GetVector()) != 2 || req.GetLimit() != 5 || !req.GetWithPayload() {
		t.Fatalf("request = %+v", req)
	}
	if w := req.GetFusionWeights(); len(w) != 2 || w[0] != 0.7 {
		t.Fatalf("weights = %v", w)
	}
	if req.GetFilter().AsMap()["topic"] != "security" {
		t.Fatalf("filter = %v", req.GetFilter())
	}
	if _, err := VectorsHybrid[*storagev1.File](nil, "notes").Run(context.Background()); err == nil {
		t.Fatal("a search with neither vector nor text must be refused")
	}
}
