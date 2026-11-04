"""Graph and vector query builders, so callers stop assembling Cypher strings and
request structs by hand::

    rows = project.graph_from(Note, note_id).out("RELATED").depth(1, 2).where("archived", "false").run()

    hits = project.vectors_hybrid("notes", Note).text("how to rotate keys").near(embedding).limit(10).run()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from google.protobuf.json_format import MessageToDict

from udb.entity.v1 import stores_pb2, vector_pb2
from udb.entity.v1 import operation_pb2

from ._rows import decode_row, message_class
from .client import to_struct
from .exceptions import UdbConfigurationError, UdbError
from .metadata import Metadata
from .schema_options import graph_label, message_type


class GraphTraversalBuilder:
    """A typed traversal from one node. Start one with ``project.graph_from``."""

    def __init__(self, client: Any, message: Any, start_id: str):
        self._client = client
        self._resource = operation_pb2.StoreResource(backend="neo4j", message_type=message_type(message))
        self._traversal = stores_pb2.GraphTraversal(
            start_label=graph_label(message),
            start_id=start_id,
            direction=stores_pb2.GRAPH_TRAVERSAL_DIRECTION_OUTGOING,
        )

    def _along(self, direction: Any, relationships: Sequence[str]) -> "GraphTraversalBuilder":
        self._traversal.direction = direction
        self._traversal.relationship_types.extend(relationships)
        return self

    def out(self, *relationships: str) -> "GraphTraversalBuilder":
        """Follow outgoing relationships of the given types (any type when none)."""
        return self._along(stores_pb2.GRAPH_TRAVERSAL_DIRECTION_OUTGOING, relationships)

    def in_(self, *relationships: str) -> "GraphTraversalBuilder":
        """Follow incoming relationships of the given types."""
        return self._along(stores_pb2.GRAPH_TRAVERSAL_DIRECTION_INCOMING, relationships)

    def both(self, *relationships: str) -> "GraphTraversalBuilder":
        """Follow relationships of the given types in either direction."""
        return self._along(stores_pb2.GRAPH_TRAVERSAL_DIRECTION_BOTH, relationships)

    def depth(self, minimum: int, maximum: int) -> "GraphTraversalBuilder":
        """Bound the number of hops (the broker clamps the maximum)."""
        self._traversal.min_depth = minimum
        self._traversal.max_depth = maximum
        return self

    def labels(self, *labels: str) -> "GraphTraversalBuilder":
        """Keep only end nodes carrying one of the labels."""
        self._traversal.node_labels.extend(labels)
        return self

    def where(self, property_name: str, value: str) -> "GraphTraversalBuilder":
        """Keep end nodes whose property equals ``value``."""
        self._traversal.node_property_equals[property_name] = value
        return self

    def where_edge(self, property_name: str, value: str) -> "GraphTraversalBuilder":
        """Keep paths whose relationships carry ``property_name = value``."""
        self._traversal.relationship_property_equals[property_name] = value
        return self

    def limit(self, count: int) -> "GraphTraversalBuilder":
        """Cap the returned rows."""
        self._traversal.limit = count
        return self

    def with_edges(self) -> "GraphTraversalBuilder":
        """Return the relationships along each path as well as the nodes."""
        self._traversal.return_relationships = True
        return self

    def request(self) -> Any:
        """The ``GraphQueryRequest`` the builder sends."""
        return stores_pb2.GraphQueryRequest(
            resource=self._resource,
            traversal=self._traversal,
            read_only=True,
            limit=self._traversal.limit,
        )

    def _checked_request(self) -> Any:
        if not self._traversal.start_id:
            raise UdbConfigurationError("udb: graph traversal needs a start node id")
        return self.request()

    def run(self, *, metadata: Metadata | None = None, timeout: float | None = None) -> list[dict[str, Any]]:
        """Execute the traversal; each row is one result record."""
        response = self._client.call(
            "GraphQuery", self._checked_request(), metadata=metadata, timeout=timeout
        )
        return _graph_rows(response)


class AsyncGraphTraversalBuilder(GraphTraversalBuilder):
    """:class:`GraphTraversalBuilder` whose :meth:`run` is awaited."""

    async def run(  # type: ignore[override]
        self, *, metadata: Metadata | None = None, timeout: float | None = None
    ) -> list[dict[str, Any]]:
        response = await self._client.call(
            "GraphQuery", self._checked_request(), metadata=metadata, timeout=timeout
        )
        return _graph_rows(response)


def _graph_rows(response: Any) -> list[dict[str, Any]]:
    return [MessageToDict(record, preserving_proto_field_name=True) for record in response.records]


@dataclass(frozen=True)
class VectorHit:
    """One search result: the point id, its score and its payload."""

    id: str = ""
    score: float = 0.0
    #: The payload decoded as the builder's message class; the payload dict without one.
    row: Any = None
    payload: dict[str, Any] = field(default_factory=dict)


class VectorSearchBuilder:
    """A hybrid (dense + text) search. Start one with ``project.vectors_hybrid``."""

    def __init__(self, client: Any, collection: str, message: Any = None):
        self._client = client
        self._message = message_class(message)
        self._request = vector_pb2.VectorHybridSearchRequest(
            collection=collection, with_payload=True, limit=10
        )

    def text(self, query: str) -> "VectorSearchBuilder":
        """Set the text leg (Postgres full-text search when the projection declares
        ``fts_columns``, else the store's own text matching)."""
        self._request.text_query = query
        return self

    def near(self, vector: Sequence[float]) -> "VectorSearchBuilder":
        """Set the dense leg's query vector."""
        del self._request.vector[:]
        self._request.vector.extend(vector)
        return self

    def weights(self, dense: float, text: float) -> "VectorSearchBuilder":
        """Set the fusion weights."""
        del self._request.fusion_weights[:]
        self._request.fusion_weights.extend((dense, text))
        return self

    def filter(self, conditions: Mapping[str, Any]) -> "VectorSearchBuilder":
        """Add payload equality conditions (Qdrant match semantics)."""
        self._request.filter.CopyFrom(to_struct(conditions))
        return self

    def limit(self, count: int) -> "VectorSearchBuilder":
        """Cap the hits (default 10)."""
        self._request.limit = count
        return self

    def request(self) -> Any:
        """The ``VectorHybridSearchRequest`` the builder sends."""
        return self._request

    def _checked_request(self) -> Any:
        if not self._request.vector and not self._request.text_query:
            raise UdbConfigurationError("udb: vector search needs near(vector) and/or text(query)")
        return self._request

    def run(self, *, metadata: Metadata | None = None, timeout: float | None = None) -> list[VectorHit]:
        """Execute the search."""
        response = self._client.call(
            "VectorHybridSearch", self._checked_request(), metadata=metadata, timeout=timeout
        )
        return _vector_hits(response, self._message)


class AsyncVectorSearchBuilder(VectorSearchBuilder):
    """:class:`VectorSearchBuilder` whose :meth:`run` is awaited."""

    async def run(  # type: ignore[override]
        self, *, metadata: Metadata | None = None, timeout: float | None = None
    ) -> list[VectorHit]:
        response = await self._client.call(
            "VectorHybridSearch", self._checked_request(), metadata=metadata, timeout=timeout
        )
        return _vector_hits(response, self._message)


def _vector_hits(response: Any, message: Any) -> list[VectorHit]:
    hits: list[VectorHit] = []
    for point in response.points:
        payload = MessageToDict(point.payload, preserving_proto_field_name=True)
        try:
            row = decode_row(payload, message)
        except UdbError as error:
            raise UdbError(f"udb: vector hit {point.id}: {error}") from error
        hits.append(VectorHit(id=point.id, score=point.score, row=row, payload=payload))
    return hits
