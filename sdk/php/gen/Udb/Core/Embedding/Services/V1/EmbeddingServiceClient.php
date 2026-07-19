<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Embedding\Services\V1;

/**
 * EmbeddingService (master-plan 9.11) — the AI data plane. Registers tenant-scoped
 * source entities to vector-index on change. INFERENCE RUNS IN SIDECARS ONLY: no
 * embedding model is ever linked into the broker. On a source row change (and on
 * Backfill) the broker emits a `udb.embedding.work.v1` event carrying ONLY the row
 * primary key + extracted text (NEVER credentials); a sidecar computes the vector
 * and returns it via the internal-only `ReportEmbedding` callback, which upserts
 * it through the shared asset vector-upsert seam tagged with the verified tenant.
 * `Retrieve` delegates to the SearchService (9.5) hybrid-search seam with a
 * server-side tenant filter — never a raw vector query.
 */
class EmbeddingServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Register a tenant-scoped source to vector-index on change. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * @param \Udb\Core\Embedding\Services\V1\RegisterSourceRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\RegisterSourceResponse>
     */
    public function RegisterSource(\Udb\Core\Embedding\Services\V1\RegisterSourceRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/RegisterSource',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\RegisterSourceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List the calling tenant's registered sources.
     * @param \Udb\Core\Embedding\Services\V1\ListSourcesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\ListSourcesResponse>
     */
    public function ListSources(\Udb\Core\Embedding\Services\V1\ListSourcesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/ListSources',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\ListSourcesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Delete a tenant-scoped source registration (destructive: stops indexing on
     * change; the engine collection teardown runs on the follow-up worker).
     * @param \Udb\Core\Embedding\Services\V1\DeleteSourceRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\DeleteSourceResponse>
     */
    public function DeleteSource(\Udb\Core\Embedding\Services\V1\DeleteSourceRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/DeleteSource',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\DeleteSourceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Enqueue embedding work for the source's EXISTING rows. The per-row work
     * enumeration runs in the leader-spawned work emitter, which calls the same
     * `udb.embedding.work.v1` emit path the CDC change handler uses.
     * @param \Udb\Core\Embedding\Services\V1\BackfillRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\BackfillResponse>
     */
    public function Backfill(\Udb\Core\Embedding\Services\V1\BackfillRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/Backfill',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\BackfillResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * SIDECAR CALLBACK (internal only). A sidecar that computed an embedding for a
     * source row returns the dense vector here; the broker upserts it through the
     * shared asset vector-upsert seam, tagged with the VERIFIED claim tenant (a
     * vector with no/foreign tenant is rejected — no fail-open). `internal_grpc_only`
     * restricts this to a loopback peer; it is never exposed in an SDK facade.
     * @param \Udb\Core\Embedding\Services\V1\ReportEmbeddingRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\ReportEmbeddingResponse>
     */
    public function ReportEmbedding(\Udb\Core\Embedding\Services\V1\ReportEmbeddingRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/ReportEmbedding',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\ReportEmbeddingResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Deadline-bounded semantic search over a source's vector collection. DELEGATES
     * to the SearchService (9.5) hybrid-search seam with a server-side tenant filter
     * injected from the verified claim. The broker never embeds the query (the
     * caller supplies an already-embedded `query_vector`); it never issues a raw
     * engine query. Returns `deadline_exceeded` if the gRPC deadline is past.
     * @param \Udb\Core\Embedding\Services\V1\RetrieveRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\RetrieveResponse>
     */
    public function Retrieve(\Udb\Core\Embedding\Services\V1\RetrieveRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/Retrieve',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\RetrieveResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Embedding\Services\V1\RegisterModelRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\RegisterModelResponse>
     */
    public function RegisterModel(\Udb\Core\Embedding\Services\V1\RegisterModelRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/RegisterModel',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\RegisterModelResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Embedding\Services\V1\ListModelsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\ListModelsResponse>
     */
    public function ListModels(\Udb\Core\Embedding\Services\V1\ListModelsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/ListModels',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\ListModelsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Embedding\Services\V1\DeleteModelRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\DeleteModelResponse>
     */
    public function DeleteModel(\Udb\Core\Embedding\Services\V1\DeleteModelRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/DeleteModel',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\DeleteModelResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Embedding\Services\V1\SetModelStatusRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\SetModelStatusResponse>
     */
    public function SetModelStatus(\Udb\Core\Embedding\Services\V1\SetModelStatusRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/SetModelStatus',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\SetModelStatusResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Embedding\Services\V1\CutoverModelAliasRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\CutoverModelAliasResponse>
     */
    public function CutoverModelAlias(\Udb\Core\Embedding\Services\V1\CutoverModelAliasRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/CutoverModelAlias',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\CutoverModelAliasResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Embedding\Services\V1\GetEmbeddingJobStatusRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\GetEmbeddingJobStatusResponse>
     */
    public function GetEmbeddingJobStatus(\Udb\Core\Embedding\Services\V1\GetEmbeddingJobStatusRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/GetEmbeddingJobStatus',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\GetEmbeddingJobStatusResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Embedding\Services\V1\ListEmbeddingWorkItemsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\ListEmbeddingWorkItemsResponse>
     */
    public function ListEmbeddingWorkItems(\Udb\Core\Embedding\Services\V1\ListEmbeddingWorkItemsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/ListEmbeddingWorkItems',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\ListEmbeddingWorkItemsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Embedding\Services\V1\ReportEmbeddingBatchRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\ReportEmbeddingBatchResponse>
     */
    public function ReportEmbeddingBatch(\Udb\Core\Embedding\Services\V1\ReportEmbeddingBatchRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/ReportEmbeddingBatch',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\ReportEmbeddingBatchResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Embedding\Services\V1\ReportEmbeddingFailureRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\ReportEmbeddingFailureResponse>
     */
    public function ReportEmbeddingFailure(\Udb\Core\Embedding\Services\V1\ReportEmbeddingFailureRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/ReportEmbeddingFailure',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\ReportEmbeddingFailureResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Embedding\Services\V1\IngestDocumentRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\IngestDocumentResponse>
     */
    public function IngestDocument(\Udb\Core\Embedding\Services\V1\IngestDocumentRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/IngestDocument',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\IngestDocumentResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Embedding\Services\V1\IngestDocumentBatchRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\IngestDocumentBatchResponse>
     */
    public function IngestDocumentBatch(\Udb\Core\Embedding\Services\V1\IngestDocumentBatchRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/IngestDocumentBatch',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\IngestDocumentBatchResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Embedding\Services\V1\ReportParsedDocumentRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\ReportParsedDocumentResponse>
     */
    public function ReportParsedDocument(\Udb\Core\Embedding\Services\V1\ReportParsedDocumentRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/ReportParsedDocument',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\ReportParsedDocumentResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Embedding\Services\V1\ReportRetrievalEvaluationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Embedding\Services\V1\ReportRetrievalEvaluationResponse>
     */
    public function ReportRetrievalEvaluation(\Udb\Core\Embedding\Services\V1\ReportRetrievalEvaluationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.embedding.services.v1.EmbeddingService/ReportRetrievalEvaluation',
        $argument,
        ['\Udb\Core\Embedding\Services\V1\ReportRetrievalEvaluationResponse', 'decode'],
        $metadata, $options);
    }

}
