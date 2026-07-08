<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Search\Services\V1;

/**
 * SearchService (master-plan 9.5) — one search box over everything. Registers
 * tenant-scoped full-text / vector / hybrid indexes over source entities and
 * serves queries. Every query runs through the mediated IR / vector dispatch so
 * a server-side tenant predicate is injected into the engine query (Elasticsearch
 * body term + Qdrant `must` clause); raw engine queries are never hand-built.
 */
class SearchServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Register a tenant-scoped index over a source entity. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * @param \Udb\Core\Search\Services\V1\CreateIndexRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Search\Services\V1\CreateIndexResponse>
     */
    public function CreateIndex(\Udb\Core\Search\Services\V1\CreateIndexRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.search.services.v1.SearchService/CreateIndex',
        $argument,
        ['\Udb\Core\Search\Services\V1\CreateIndexResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Delete a tenant-scoped index registration (destructive: drops the engine
     * index resource on the follow-up worker).
     * @param \Udb\Core\Search\Services\V1\DeleteIndexRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Search\Services\V1\DeleteIndexResponse>
     */
    public function DeleteIndex(\Udb\Core\Search\Services\V1\DeleteIndexRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.search.services.v1.SearchService/DeleteIndex',
        $argument,
        ['\Udb\Core\Search\Services\V1\DeleteIndexResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List the calling tenant's registered indexes.
     * @param \Udb\Core\Search\Services\V1\ListIndexesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Search\Services\V1\ListIndexesResponse>
     */
    public function ListIndexes(\Udb\Core\Search\Services\V1\ListIndexesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.search.services.v1.SearchService/ListIndexes',
        $argument,
        ['\Udb\Core\Search\Services\V1\ListIndexesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Run a full-text / vector / hybrid query. The tenant predicate is injected
     * server-side from the verified claim into every engine query.
     * @param \Udb\Core\Search\Services\V1\SearchRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Search\Services\V1\SearchResponse>
     */
    public function Search(\Udb\Core\Search\Services\V1\SearchRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.search.services.v1.SearchService/Search',
        $argument,
        ['\Udb\Core\Search\Services\V1\SearchResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Request a full rebuild of an index from the source entity. The backfill
     * reads source rows ONLY through the mediated IR path.
     * @param \Udb\Core\Search\Services\V1\ReindexRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Search\Services\V1\ReindexResponse>
     */
    public function Reindex(\Udb\Core\Search\Services\V1\ReindexRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.search.services.v1.SearchService/Reindex',
        $argument,
        ['\Udb\Core\Search\Services\V1\ReindexResponse', 'decode'],
        $metadata, $options);
    }

}
