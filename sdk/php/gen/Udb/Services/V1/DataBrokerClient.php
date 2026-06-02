<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Services\V1;

/**
 * DataBroker is the UDB-owned wire protocol. Project protos are parsed only as
 * schema input; they do not need to contain or import this service contract.
 */
class DataBrokerClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * ── Relational ─────────────────────────────────────────────────────────────
     * @param \Udb\Entity\V1\SelectRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\RecordSet>
     */
    public function Select(\Udb\Entity\V1\SelectRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/Select',
        $argument,
        ['\Udb\Entity\V1\RecordSet', 'decode'],
        $metadata, $options);
    }

    /**
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\BidiStreamingCall
     */
    public function BatchSelect($metadata = [], $options = []) {
        return $this->_bidiRequest('/udb.services.v1.DataBroker/BatchSelect',
        ['\Udb\Entity\V1\RecordSet','decode'],
        $metadata, $options);
    }

    /**
     * Additive typed columnar read. Reuses SelectRequest; streams RecordBatchV2.
     * Clients use this only when ProtocolSupport.encodings advertises
     * "record_batch_v2" and otherwise fall back to Select/RecordSet.
     * @param \Udb\Entity\V1\SelectRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function SelectV2(\Udb\Entity\V1\SelectRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/udb.services.v1.DataBroker/SelectV2',
        $argument,
        ['\Udb\Entity\V1\RecordBatchV2', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\UpsertRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function Upsert(\Udb\Entity\V1\UpsertRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/Upsert',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\BidiStreamingCall
     */
    public function BatchUpsert($metadata = [], $options = []) {
        return $this->_bidiRequest('/udb.services.v1.DataBroker/BatchUpsert',
        ['\Udb\Entity\V1\MutationResponse','decode'],
        $metadata, $options);
    }

    /**
     * Delete rows without raw SQL.
     * @param \Udb\Entity\V1\DeleteRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function Delete(\Udb\Entity\V1\DeleteRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/Delete',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Vector ──────────────────────────────────────────────────────────────────
     * @param \Udb\Entity\V1\VectorSearchRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\VectorSet>
     */
    public function VectorSearch(\Udb\Entity\V1\VectorSearchRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/VectorSearch',
        $argument,
        ['\Udb\Entity\V1\VectorSet', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\VectorHybridSearchRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\VectorSet>
     */
    public function VectorHybridSearch(\Udb\Entity\V1\VectorHybridSearchRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/VectorHybridSearch',
        $argument,
        ['\Udb\Entity\V1\VectorSet', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\VectorUpsertRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function VectorUpsert(\Udb\Entity\V1\VectorUpsertRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/VectorUpsert',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\BidiStreamingCall
     */
    public function VectorBatchUpsert($metadata = [], $options = []) {
        return $this->_bidiRequest('/udb.services.v1.DataBroker/VectorBatchUpsert',
        ['\Udb\Entity\V1\MutationResponse','decode'],
        $metadata, $options);
    }

    /**
     * ── Blob ───────────────────────────────────────────────────────────────────
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ClientStreamingCall
     */
    public function PutObject($metadata = [], $options = []) {
        return $this->_clientStreamRequest('/udb.services.v1.DataBroker/PutObject',
        ['\Udb\Entity\V1\MutationResponse','decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\ObjectRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function GetObject(\Udb\Entity\V1\ObjectRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/udb.services.v1.DataBroker/GetObject',
        $argument,
        ['\Udb\Entity\V1\Chunk', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\UrlRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\UrlResponse>
     */
    public function GeneratePresignedUrl(\Udb\Entity\V1\UrlRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/GeneratePresignedUrl',
        $argument,
        ['\Udb\Entity\V1\UrlResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\MultipartUploadRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MultipartUploadResponse>
     */
    public function InitiateMultipartUpload(\Udb\Entity\V1\MultipartUploadRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/InitiateMultipartUpload',
        $argument,
        ['\Udb\Entity\V1\MultipartUploadResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Cache / KV ─────────────────────────────────────────────────────────────
     * @param \Udb\Entity\V1\CacheGetRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\CacheGetResponse>
     */
    public function CacheGet(\Udb\Entity\V1\CacheGetRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/CacheGet',
        $argument,
        ['\Udb\Entity\V1\CacheGetResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\CacheSetRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function CacheSet(\Udb\Entity\V1\CacheSetRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/CacheSet',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\CacheDeleteRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function CacheDelete(\Udb\Entity\V1\CacheDeleteRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/CacheDelete',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\CacheScanRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\CacheScanResponse>
     */
    public function CacheScan(\Udb\Entity\V1\CacheScanRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/CacheScan',
        $argument,
        ['\Udb\Entity\V1\CacheScanResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Document / Graph / Time-Series / Analytical Stores ────────────────────
     * @param \Udb\Entity\V1\DocumentGetRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\DocumentSet>
     */
    public function DocumentGet(\Udb\Entity\V1\DocumentGetRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/DocumentGet',
        $argument,
        ['\Udb\Entity\V1\DocumentSet', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\DocumentFindRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\DocumentSet>
     */
    public function DocumentFind(\Udb\Entity\V1\DocumentFindRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/DocumentFind',
        $argument,
        ['\Udb\Entity\V1\DocumentSet', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\DocumentUpsertRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function DocumentUpsert(\Udb\Entity\V1\DocumentUpsertRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/DocumentUpsert',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\DocumentDeleteRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function DocumentDelete(\Udb\Entity\V1\DocumentDeleteRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/DocumentDelete',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\GraphQueryRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\GraphResultSet>
     */
    public function GraphQuery(\Udb\Entity\V1\GraphQueryRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/GraphQuery',
        $argument,
        ['\Udb\Entity\V1\GraphResultSet', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\GraphMutationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function GraphMutate(\Udb\Entity\V1\GraphMutationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/GraphMutate',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\TimeSeriesWriteRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function TimeSeriesWrite(\Udb\Entity\V1\TimeSeriesWriteRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/TimeSeriesWrite',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\TimeSeriesQueryRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\TimeSeriesQueryResponse>
     */
    public function TimeSeriesQuery(\Udb\Entity\V1\TimeSeriesQueryRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/TimeSeriesQuery',
        $argument,
        ['\Udb\Entity\V1\TimeSeriesQueryResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\AnalyticalQueryRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\AnalyticalQueryResponse>
     */
    public function AnalyticalQuery(\Udb\Entity\V1\AnalyticalQueryRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/AnalyticalQuery',
        $argument,
        ['\Udb\Entity\V1\AnalyticalQueryResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Tx / CDC ───────────────────────────────────────────────────────────────
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\BidiStreamingCall
     */
    public function BeginTx($metadata = [], $options = []) {
        return $this->_bidiRequest('/udb.services.v1.DataBroker/BeginTx',
        ['\Udb\Entity\V1\TxStatus','decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\CDCSubscriptionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function PublishCDC(\Udb\Entity\V1\CDCSubscriptionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/udb.services.v1.DataBroker/PublishCDC',
        $argument,
        ['\Udb\Events\V1\CDCEnvelope', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\ViewDefinition $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function CreateMaterializedView(\Udb\Entity\V1\ViewDefinition $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/CreateMaterializedView',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── First-Class Event API ─────────────────────────────────────────────────
     * @param \Udb\Entity\V1\EnqueueOutboxEventRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\EnqueueOutboxEventResponse>
     */
    public function EnqueueOutboxEvent(\Udb\Entity\V1\EnqueueOutboxEventRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/EnqueueOutboxEvent',
        $argument,
        ['\Udb\Entity\V1\EnqueueOutboxEventResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Generic resource administration.
     * Dispatch a lifecycle operation to any configured backend executor.
     * Requires scope: udb:dispatch
     * @param \Udb\Entity\V1\GenericDispatchRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\GenericDispatchResponse>
     */
    public function GenericDispatch(\Udb\Entity\V1\GenericDispatchRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/GenericDispatch',
        $argument,
        ['\Udb\Entity\V1\GenericDispatchResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Ensure a named resource exists on the target backend.
     * Requires scope: udb:admin
     * @param \Udb\Entity\V1\ResourceAdminRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function EnsureResource(\Udb\Entity\V1\ResourceAdminRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/EnsureResource',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Drop a named resource on the target backend.
     * Requires scope: udb:admin
     * @param \Udb\Entity\V1\ResourceAdminRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function DropResource(\Udb\Entity\V1\ResourceAdminRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/DropResource',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List all resources managed by the target backend.
     * Requires scope: udb:admin
     * @param \Udb\Entity\V1\ResourceAdminRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\ResourceListResponse>
     */
    public function ListResources(\Udb\Entity\V1\ResourceAdminRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/ListResources',
        $argument,
        ['\Udb\Entity\V1\ResourceListResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Catalog administration.
     * Stage a new catalog manifest version (validate + store as STAGED).
     * Requires scope: udb:admin
     * @param \Udb\Entity\V1\StageCatalogRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\CatalogVersionResponse>
     */
    public function StageCatalog(\Udb\Entity\V1\StageCatalogRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/StageCatalog',
        $argument,
        ['\Udb\Entity\V1\CatalogVersionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Activate a STAGED catalog version.
     * Requires scope: udb:admin
     * @param \Udb\Entity\V1\CatalogVersionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\CatalogVersionResponse>
     */
    public function ActivateCatalog(\Udb\Entity\V1\CatalogVersionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/ActivateCatalog',
        $argument,
        ['\Udb\Entity\V1\CatalogVersionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Roll back to the previous ACTIVE catalog version.
     * Requires scope: udb:admin
     * @param \Udb\Entity\V1\CatalogVersionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\CatalogVersionResponse>
     */
    public function RollbackCatalog(\Udb\Entity\V1\CatalogVersionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/RollbackCatalog',
        $argument,
        ['\Udb\Entity\V1\CatalogVersionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Validate a catalog manifest JSON without storing it.
     * Requires scope: udb:admin
     * @param \Udb\Entity\V1\StageCatalogRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\CatalogValidationResponse>
     */
    public function ValidateCatalog(\Udb\Entity\V1\StageCatalogRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/ValidateCatalog',
        $argument,
        ['\Udb\Entity\V1\CatalogValidationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Return the list of known catalog versions.
     * Requires scope: udb:admin
     * @param \Udb\Entity\V1\CatalogManifestRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\CatalogVersionListResponse>
     */
    public function GetCatalogVersions(\Udb\Entity\V1\CatalogManifestRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/GetCatalogVersions',
        $argument,
        ['\Udb\Entity\V1\CatalogVersionListResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Return one catalog version by catalog_id/version, or active version when empty.
     * Requires scope: udb:admin
     * @param \Udb\Entity\V1\CatalogVersionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\CatalogVersionResponse>
     */
    public function GetCatalogVersion(\Udb\Entity\V1\CatalogVersionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/GetCatalogVersion',
        $argument,
        ['\Udb\Entity\V1\CatalogVersionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Migration planning and apply.
     * Plan a migration against the active catalog without executing it.
     * Requires scope: udb:admin
     * @param \Udb\Entity\V1\MigrationPlanRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MigrationPlanResponse>
     */
    public function PlanMigration(\Udb\Entity\V1\MigrationPlanRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/PlanMigration',
        $argument,
        ['\Udb\Entity\V1\MigrationPlanResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Apply a previously planned (and optionally approved) migration.
     * Requires scope: udb:admin
     * @param \Udb\Entity\V1\MigrationApplyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MigrationStatusResponse>
     */
    public function ApplyMigration(\Udb\Entity\V1\MigrationApplyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/ApplyMigration',
        $argument,
        ['\Udb\Entity\V1\MigrationStatusResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Return the status of a migration run.
     * Requires scope: udb:admin
     * @param \Udb\Entity\V1\MigrationRunRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MigrationStatusResponse>
     */
    public function GetMigrationStatus(\Udb\Entity\V1\MigrationRunRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/GetMigrationStatus',
        $argument,
        ['\Udb\Entity\V1\MigrationStatusResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Return migration runs for an admin console page.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * @param \Udb\Entity\V1\MigrationRunListRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MigrationRunListResponse>
     */
    public function ListMigrationRuns(\Udb\Entity\V1\MigrationRunListRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/ListMigrationRuns',
        $argument,
        ['\Udb\Entity\V1\MigrationRunListResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Approve a migration plan that requires review.
     * Requires scope: udb:admin
     * @param \Udb\Entity\V1\MigrationRunRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MigrationStatusResponse>
     */
    public function ApproveMigrationPlan(\Udb\Entity\V1\MigrationRunRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/ApproveMigrationPlan',
        $argument,
        ['\Udb\Entity\V1\MigrationStatusResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * DLQ management.
     * @param \Udb\Entity\V1\DlqListRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\DlqListResponse>
     */
    public function ListDlqEvents(\Udb\Entity\V1\DlqListRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/ListDlqEvents',
        $argument,
        ['\Udb\Entity\V1\DlqListResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\DlqEventRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\DlqEventResponse>
     */
    public function GetDlqEvent(\Udb\Entity\V1\DlqEventRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/GetDlqEvent',
        $argument,
        ['\Udb\Entity\V1\DlqEventResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\DlqActionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function ReplayDlqEvent(\Udb\Entity\V1\DlqActionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/ReplayDlqEvent',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\DlqActionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function DismissDlqEvent(\Udb\Entity\V1\DlqActionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/DismissDlqEvent',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\DlqActionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function QuarantineDlqEvent(\Udb\Entity\V1\DlqActionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/QuarantineDlqEvent',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * CDC control plane.
     * @param \Udb\Entity\V1\CdcControlRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\CdcStatusResponse>
     */
    public function GetCdcStatus(\Udb\Entity\V1\CdcControlRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/GetCdcStatus',
        $argument,
        ['\Udb\Entity\V1\CdcStatusResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\CdcControlRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\CdcStatusResponse>
     */
    public function PauseCdc(\Udb\Entity\V1\CdcControlRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/PauseCdc',
        $argument,
        ['\Udb\Entity\V1\CdcStatusResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\CdcControlRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\CdcStatusResponse>
     */
    public function ResumeCdc(\Udb\Entity\V1\CdcControlRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/ResumeCdc',
        $argument,
        ['\Udb\Entity\V1\CdcStatusResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\CdcControlRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\CdcStatusResponse>
     */
    public function StepDownCdcLeader(\Udb\Entity\V1\CdcControlRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/StepDownCdcLeader',
        $argument,
        ['\Udb\Entity\V1\CdcStatusResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\CdcRedactionPreviewRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\CdcRedactionPreviewResponse>
     */
    public function PreviewCdcRedaction(\Udb\Entity\V1\CdcRedactionPreviewRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/PreviewCdcRedaction',
        $argument,
        ['\Udb\Entity\V1\CdcRedactionPreviewResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\ProjectionDriftScanRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\ProjectionDriftScanResponse>
     */
    public function ScanProjectionDrift(\Udb\Entity\V1\ProjectionDriftScanRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/ScanProjectionDrift',
        $argument,
        ['\Udb\Entity\V1\ProjectionDriftScanResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Saga administration.
     * @param \Udb\Entity\V1\SagaListRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\SagaListResponse>
     */
    public function ListSagas(\Udb\Entity\V1\SagaListRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/ListSagas',
        $argument,
        ['\Udb\Entity\V1\SagaListResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\SagaRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\SagaResponse>
     */
    public function GetSaga(\Udb\Entity\V1\SagaRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/GetSaga',
        $argument,
        ['\Udb\Entity\V1\SagaResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\SagaRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\SagaResponse>
     */
    public function RetrySagaCompensation(\Udb\Entity\V1\SagaRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/RetrySagaCompensation',
        $argument,
        ['\Udb\Entity\V1\SagaResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\SagaRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\SagaResponse>
     */
    public function MarkSagaReviewed(\Udb\Entity\V1\SagaRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/MarkSagaReviewed',
        $argument,
        ['\Udb\Entity\V1\SagaResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Policy administration.
     * @param \Udb\Entity\V1\PolicyListRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\PolicyListResponse>
     */
    public function ListPolicies(\Udb\Entity\V1\PolicyListRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/ListPolicies',
        $argument,
        ['\Udb\Entity\V1\PolicyListResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\PutPolicyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function PutPolicy(\Udb\Entity\V1\PutPolicyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/PutPolicy',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\PolicyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function DeletePolicy(\Udb\Entity\V1\PolicyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/DeletePolicy',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\CapabilitiesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function ReloadPolicies(\Udb\Entity\V1\CapabilitiesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/ReloadPolicies',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\CapabilitiesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\PolicyLintResponse>
     */
    public function LintPolicies(\Udb\Entity\V1\CapabilitiesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/LintPolicies',
        $argument,
        ['\Udb\Entity\V1\PolicyLintResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Admin API ─────────────────────────────────────────────────────────────
     * @param \Udb\Entity\V1\CapabilitiesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\CapabilitiesResponse>
     */
    public function GetCapabilities(\Udb\Entity\V1\CapabilitiesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/GetCapabilities',
        $argument,
        ['\Udb\Entity\V1\CapabilitiesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\CatalogManifestRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\CatalogManifestResponse>
     */
    public function GetCatalogManifest(\Udb\Entity\V1\CatalogManifestRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/GetCatalogManifest',
        $argument,
        ['\Udb\Entity\V1\CatalogManifestResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Runtime schema lookup for SDK/data operation compatibility negotiation.
     * @param \Udb\Entity\V1\MessageSchemaLookupRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MessageSchemaLookupResponse>
     */
    public function LookupMessageSchema(\Udb\Entity\V1\MessageSchemaLookupRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/LookupMessageSchema',
        $argument,
        ['\Udb\Entity\V1\MessageSchemaLookupResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\MessageSchemaListRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MessageSchemaListResponse>
     */
    public function ListMessageSchemas(\Udb\Entity\V1\MessageSchemaListRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/ListMessageSchemas',
        $argument,
        ['\Udb\Entity\V1\MessageSchemaListResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Entity\V1\HealthReportRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\HealthReportResponse>
     */
    public function GetHealthReport(\Udb\Entity\V1\HealthReportRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/GetHealthReport',
        $argument,
        ['\Udb\Entity\V1\HealthReportResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Multi-project registry.
     * Ensure a project namespace exists (idempotent).
     * Requires scope: udb:admin
     * @param \Udb\Entity\V1\EnsureProjectRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\MutationResponse>
     */
    public function EnsureProject(\Udb\Entity\V1\EnsureProjectRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/EnsureProject',
        $argument,
        ['\Udb\Entity\V1\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List all registered project namespaces.
     * Requires scope: udb:admin
     * @param \Udb\Entity\V1\ProjectListRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\ProjectListResponse>
     */
    public function ListProjects(\Udb\Entity\V1\ProjectListRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/ListProjects',
        $argument,
        ['\Udb\Entity\V1\ProjectListResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Unified Admin Surface ────────────────────────────────────────────────
     * Returns a single snapshot covering catalog, CDC, saga, backend, and policy
     * state for the admin console. Requires scope: udb:admin.
     * @param \Udb\Entity\V1\AdminSummaryRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\AdminSummaryResponse>
     */
    public function GetAdminSummary(\Udb\Entity\V1\AdminSummaryRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/GetAdminSummary',
        $argument,
        ['\Udb\Entity\V1\AdminSummaryResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Paginated admin audit log view for the admin console.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * @param \Udb\Entity\V1\AdminAuditLogRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\AdminAuditLogResponse>
     */
    public function ListAdminAuditLogs(\Udb\Entity\V1\AdminAuditLogRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/ListAdminAuditLogs',
        $argument,
        ['\Udb\Entity\V1\AdminAuditLogResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Verifies the admin audit log hash chain and reports the first broken link.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * @param \Udb\Entity\V1\AdminAuditVerifyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Entity\V1\AdminAuditVerifyResponse>
     */
    public function VerifyAdminAuditLog(\Udb\Entity\V1\AdminAuditVerifyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.services.v1.DataBroker/VerifyAdminAuditLog',
        $argument,
        ['\Udb\Entity\V1\AdminAuditVerifyResponse', 'decode'],
        $metadata, $options);
    }

}
