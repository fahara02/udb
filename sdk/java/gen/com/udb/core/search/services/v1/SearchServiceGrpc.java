package com.udb.core.search.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * SearchService (master-plan 9.5) — one search box over everything. Registers
 * tenant-scoped full-text / vector / hybrid indexes over source entities and
 * serves queries. Every query runs through the mediated IR / vector dispatch so
 * a server-side tenant predicate is injected into the engine query (Elasticsearch
 * body term + Qdrant `must` clause); raw engine queries are never hand-built.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class SearchServiceGrpc {

  private SearchServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.search.services.v1.SearchService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.search.services.v1.CreateIndexRequest,
      com.udb.core.search.services.v1.CreateIndexResponse> getCreateIndexMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateIndex",
      requestType = com.udb.core.search.services.v1.CreateIndexRequest.class,
      responseType = com.udb.core.search.services.v1.CreateIndexResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.search.services.v1.CreateIndexRequest,
      com.udb.core.search.services.v1.CreateIndexResponse> getCreateIndexMethod() {
    io.grpc.MethodDescriptor<com.udb.core.search.services.v1.CreateIndexRequest, com.udb.core.search.services.v1.CreateIndexResponse> getCreateIndexMethod;
    if ((getCreateIndexMethod = SearchServiceGrpc.getCreateIndexMethod) == null) {
      synchronized (SearchServiceGrpc.class) {
        if ((getCreateIndexMethod = SearchServiceGrpc.getCreateIndexMethod) == null) {
          SearchServiceGrpc.getCreateIndexMethod = getCreateIndexMethod =
              io.grpc.MethodDescriptor.<com.udb.core.search.services.v1.CreateIndexRequest, com.udb.core.search.services.v1.CreateIndexResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateIndex"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.search.services.v1.CreateIndexRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.search.services.v1.CreateIndexResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SearchServiceMethodDescriptorSupplier("CreateIndex"))
              .build();
        }
      }
    }
    return getCreateIndexMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.search.services.v1.DeleteIndexRequest,
      com.udb.core.search.services.v1.DeleteIndexResponse> getDeleteIndexMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteIndex",
      requestType = com.udb.core.search.services.v1.DeleteIndexRequest.class,
      responseType = com.udb.core.search.services.v1.DeleteIndexResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.search.services.v1.DeleteIndexRequest,
      com.udb.core.search.services.v1.DeleteIndexResponse> getDeleteIndexMethod() {
    io.grpc.MethodDescriptor<com.udb.core.search.services.v1.DeleteIndexRequest, com.udb.core.search.services.v1.DeleteIndexResponse> getDeleteIndexMethod;
    if ((getDeleteIndexMethod = SearchServiceGrpc.getDeleteIndexMethod) == null) {
      synchronized (SearchServiceGrpc.class) {
        if ((getDeleteIndexMethod = SearchServiceGrpc.getDeleteIndexMethod) == null) {
          SearchServiceGrpc.getDeleteIndexMethod = getDeleteIndexMethod =
              io.grpc.MethodDescriptor.<com.udb.core.search.services.v1.DeleteIndexRequest, com.udb.core.search.services.v1.DeleteIndexResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteIndex"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.search.services.v1.DeleteIndexRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.search.services.v1.DeleteIndexResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SearchServiceMethodDescriptorSupplier("DeleteIndex"))
              .build();
        }
      }
    }
    return getDeleteIndexMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.search.services.v1.ListIndexesRequest,
      com.udb.core.search.services.v1.ListIndexesResponse> getListIndexesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListIndexes",
      requestType = com.udb.core.search.services.v1.ListIndexesRequest.class,
      responseType = com.udb.core.search.services.v1.ListIndexesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.search.services.v1.ListIndexesRequest,
      com.udb.core.search.services.v1.ListIndexesResponse> getListIndexesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.search.services.v1.ListIndexesRequest, com.udb.core.search.services.v1.ListIndexesResponse> getListIndexesMethod;
    if ((getListIndexesMethod = SearchServiceGrpc.getListIndexesMethod) == null) {
      synchronized (SearchServiceGrpc.class) {
        if ((getListIndexesMethod = SearchServiceGrpc.getListIndexesMethod) == null) {
          SearchServiceGrpc.getListIndexesMethod = getListIndexesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.search.services.v1.ListIndexesRequest, com.udb.core.search.services.v1.ListIndexesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListIndexes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.search.services.v1.ListIndexesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.search.services.v1.ListIndexesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SearchServiceMethodDescriptorSupplier("ListIndexes"))
              .build();
        }
      }
    }
    return getListIndexesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.search.services.v1.SearchRequest,
      com.udb.core.search.services.v1.SearchResponse> getSearchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Search",
      requestType = com.udb.core.search.services.v1.SearchRequest.class,
      responseType = com.udb.core.search.services.v1.SearchResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.search.services.v1.SearchRequest,
      com.udb.core.search.services.v1.SearchResponse> getSearchMethod() {
    io.grpc.MethodDescriptor<com.udb.core.search.services.v1.SearchRequest, com.udb.core.search.services.v1.SearchResponse> getSearchMethod;
    if ((getSearchMethod = SearchServiceGrpc.getSearchMethod) == null) {
      synchronized (SearchServiceGrpc.class) {
        if ((getSearchMethod = SearchServiceGrpc.getSearchMethod) == null) {
          SearchServiceGrpc.getSearchMethod = getSearchMethod =
              io.grpc.MethodDescriptor.<com.udb.core.search.services.v1.SearchRequest, com.udb.core.search.services.v1.SearchResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Search"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.search.services.v1.SearchRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.search.services.v1.SearchResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SearchServiceMethodDescriptorSupplier("Search"))
              .build();
        }
      }
    }
    return getSearchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.search.services.v1.ReindexRequest,
      com.udb.core.search.services.v1.ReindexResponse> getReindexMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Reindex",
      requestType = com.udb.core.search.services.v1.ReindexRequest.class,
      responseType = com.udb.core.search.services.v1.ReindexResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.search.services.v1.ReindexRequest,
      com.udb.core.search.services.v1.ReindexResponse> getReindexMethod() {
    io.grpc.MethodDescriptor<com.udb.core.search.services.v1.ReindexRequest, com.udb.core.search.services.v1.ReindexResponse> getReindexMethod;
    if ((getReindexMethod = SearchServiceGrpc.getReindexMethod) == null) {
      synchronized (SearchServiceGrpc.class) {
        if ((getReindexMethod = SearchServiceGrpc.getReindexMethod) == null) {
          SearchServiceGrpc.getReindexMethod = getReindexMethod =
              io.grpc.MethodDescriptor.<com.udb.core.search.services.v1.ReindexRequest, com.udb.core.search.services.v1.ReindexResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Reindex"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.search.services.v1.ReindexRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.search.services.v1.ReindexResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SearchServiceMethodDescriptorSupplier("Reindex"))
              .build();
        }
      }
    }
    return getReindexMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static SearchServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SearchServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SearchServiceStub>() {
        @java.lang.Override
        public SearchServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SearchServiceStub(channel, callOptions);
        }
      };
    return SearchServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static SearchServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SearchServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SearchServiceBlockingV2Stub>() {
        @java.lang.Override
        public SearchServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SearchServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return SearchServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static SearchServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SearchServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SearchServiceBlockingStub>() {
        @java.lang.Override
        public SearchServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SearchServiceBlockingStub(channel, callOptions);
        }
      };
    return SearchServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static SearchServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SearchServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SearchServiceFutureStub>() {
        @java.lang.Override
        public SearchServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SearchServiceFutureStub(channel, callOptions);
        }
      };
    return SearchServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * SearchService (master-plan 9.5) — one search box over everything. Registers
   * tenant-scoped full-text / vector / hybrid indexes over source entities and
   * serves queries. Every query runs through the mediated IR / vector dispatch so
   * a server-side tenant predicate is injected into the engine query (Elasticsearch
   * body term + Qdrant `must` clause); raw engine queries are never hand-built.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Register a tenant-scoped index over a source entity. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * </pre>
     */
    default void createIndex(com.udb.core.search.services.v1.CreateIndexRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.search.services.v1.CreateIndexResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateIndexMethod(), responseObserver);
    }

    /**
     * <pre>
     * Delete a tenant-scoped index registration (destructive: drops the engine
     * index resource on the follow-up worker).
     * </pre>
     */
    default void deleteIndex(com.udb.core.search.services.v1.DeleteIndexRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.search.services.v1.DeleteIndexResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteIndexMethod(), responseObserver);
    }

    /**
     * <pre>
     * List the calling tenant's registered indexes.
     * </pre>
     */
    default void listIndexes(com.udb.core.search.services.v1.ListIndexesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.search.services.v1.ListIndexesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListIndexesMethod(), responseObserver);
    }

    /**
     * <pre>
     * Run a full-text / vector / hybrid query. The tenant predicate is injected
     * server-side from the verified claim into every engine query.
     * </pre>
     */
    default void search(com.udb.core.search.services.v1.SearchRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.search.services.v1.SearchResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSearchMethod(), responseObserver);
    }

    /**
     * <pre>
     * Request a full rebuild of an index from the source entity. The backfill
     * reads source rows ONLY through the mediated IR path.
     * </pre>
     */
    default void reindex(com.udb.core.search.services.v1.ReindexRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.search.services.v1.ReindexResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReindexMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service SearchService.
   * <pre>
   * SearchService (master-plan 9.5) — one search box over everything. Registers
   * tenant-scoped full-text / vector / hybrid indexes over source entities and
   * serves queries. Every query runs through the mediated IR / vector dispatch so
   * a server-side tenant predicate is injected into the engine query (Elasticsearch
   * body term + Qdrant `must` clause); raw engine queries are never hand-built.
   * </pre>
   */
  public static abstract class SearchServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return SearchServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service SearchService.
   * <pre>
   * SearchService (master-plan 9.5) — one search box over everything. Registers
   * tenant-scoped full-text / vector / hybrid indexes over source entities and
   * serves queries. Every query runs through the mediated IR / vector dispatch so
   * a server-side tenant predicate is injected into the engine query (Elasticsearch
   * body term + Qdrant `must` clause); raw engine queries are never hand-built.
   * </pre>
   */
  public static final class SearchServiceStub
      extends io.grpc.stub.AbstractAsyncStub<SearchServiceStub> {
    private SearchServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SearchServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SearchServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a tenant-scoped index over a source entity. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * </pre>
     */
    public void createIndex(com.udb.core.search.services.v1.CreateIndexRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.search.services.v1.CreateIndexResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateIndexMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Delete a tenant-scoped index registration (destructive: drops the engine
     * index resource on the follow-up worker).
     * </pre>
     */
    public void deleteIndex(com.udb.core.search.services.v1.DeleteIndexRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.search.services.v1.DeleteIndexResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteIndexMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List the calling tenant's registered indexes.
     * </pre>
     */
    public void listIndexes(com.udb.core.search.services.v1.ListIndexesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.search.services.v1.ListIndexesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListIndexesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Run a full-text / vector / hybrid query. The tenant predicate is injected
     * server-side from the verified claim into every engine query.
     * </pre>
     */
    public void search(com.udb.core.search.services.v1.SearchRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.search.services.v1.SearchResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSearchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Request a full rebuild of an index from the source entity. The backfill
     * reads source rows ONLY through the mediated IR path.
     * </pre>
     */
    public void reindex(com.udb.core.search.services.v1.ReindexRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.search.services.v1.ReindexResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReindexMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service SearchService.
   * <pre>
   * SearchService (master-plan 9.5) — one search box over everything. Registers
   * tenant-scoped full-text / vector / hybrid indexes over source entities and
   * serves queries. Every query runs through the mediated IR / vector dispatch so
   * a server-side tenant predicate is injected into the engine query (Elasticsearch
   * body term + Qdrant `must` clause); raw engine queries are never hand-built.
   * </pre>
   */
  public static final class SearchServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<SearchServiceBlockingV2Stub> {
    private SearchServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SearchServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SearchServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a tenant-scoped index over a source entity. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * </pre>
     */
    public com.udb.core.search.services.v1.CreateIndexResponse createIndex(com.udb.core.search.services.v1.CreateIndexRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreateIndexMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a tenant-scoped index registration (destructive: drops the engine
     * index resource on the follow-up worker).
     * </pre>
     */
    public com.udb.core.search.services.v1.DeleteIndexResponse deleteIndex(com.udb.core.search.services.v1.DeleteIndexRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeleteIndexMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List the calling tenant's registered indexes.
     * </pre>
     */
    public com.udb.core.search.services.v1.ListIndexesResponse listIndexes(com.udb.core.search.services.v1.ListIndexesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListIndexesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Run a full-text / vector / hybrid query. The tenant predicate is injected
     * server-side from the verified claim into every engine query.
     * </pre>
     */
    public com.udb.core.search.services.v1.SearchResponse search(com.udb.core.search.services.v1.SearchRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSearchMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Request a full rebuild of an index from the source entity. The backfill
     * reads source rows ONLY through the mediated IR path.
     * </pre>
     */
    public com.udb.core.search.services.v1.ReindexResponse reindex(com.udb.core.search.services.v1.ReindexRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getReindexMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service SearchService.
   * <pre>
   * SearchService (master-plan 9.5) — one search box over everything. Registers
   * tenant-scoped full-text / vector / hybrid indexes over source entities and
   * serves queries. Every query runs through the mediated IR / vector dispatch so
   * a server-side tenant predicate is injected into the engine query (Elasticsearch
   * body term + Qdrant `must` clause); raw engine queries are never hand-built.
   * </pre>
   */
  public static final class SearchServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<SearchServiceBlockingStub> {
    private SearchServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SearchServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SearchServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a tenant-scoped index over a source entity. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * </pre>
     */
    public com.udb.core.search.services.v1.CreateIndexResponse createIndex(com.udb.core.search.services.v1.CreateIndexRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateIndexMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a tenant-scoped index registration (destructive: drops the engine
     * index resource on the follow-up worker).
     * </pre>
     */
    public com.udb.core.search.services.v1.DeleteIndexResponse deleteIndex(com.udb.core.search.services.v1.DeleteIndexRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteIndexMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List the calling tenant's registered indexes.
     * </pre>
     */
    public com.udb.core.search.services.v1.ListIndexesResponse listIndexes(com.udb.core.search.services.v1.ListIndexesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListIndexesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Run a full-text / vector / hybrid query. The tenant predicate is injected
     * server-side from the verified claim into every engine query.
     * </pre>
     */
    public com.udb.core.search.services.v1.SearchResponse search(com.udb.core.search.services.v1.SearchRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSearchMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Request a full rebuild of an index from the source entity. The backfill
     * reads source rows ONLY through the mediated IR path.
     * </pre>
     */
    public com.udb.core.search.services.v1.ReindexResponse reindex(com.udb.core.search.services.v1.ReindexRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReindexMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service SearchService.
   * <pre>
   * SearchService (master-plan 9.5) — one search box over everything. Registers
   * tenant-scoped full-text / vector / hybrid indexes over source entities and
   * serves queries. Every query runs through the mediated IR / vector dispatch so
   * a server-side tenant predicate is injected into the engine query (Elasticsearch
   * body term + Qdrant `must` clause); raw engine queries are never hand-built.
   * </pre>
   */
  public static final class SearchServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<SearchServiceFutureStub> {
    private SearchServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SearchServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SearchServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a tenant-scoped index over a source entity. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.search.services.v1.CreateIndexResponse> createIndex(
        com.udb.core.search.services.v1.CreateIndexRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateIndexMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Delete a tenant-scoped index registration (destructive: drops the engine
     * index resource on the follow-up worker).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.search.services.v1.DeleteIndexResponse> deleteIndex(
        com.udb.core.search.services.v1.DeleteIndexRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteIndexMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List the calling tenant's registered indexes.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.search.services.v1.ListIndexesResponse> listIndexes(
        com.udb.core.search.services.v1.ListIndexesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListIndexesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Run a full-text / vector / hybrid query. The tenant predicate is injected
     * server-side from the verified claim into every engine query.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.search.services.v1.SearchResponse> search(
        com.udb.core.search.services.v1.SearchRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSearchMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Request a full rebuild of an index from the source entity. The backfill
     * reads source rows ONLY through the mediated IR path.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.search.services.v1.ReindexResponse> reindex(
        com.udb.core.search.services.v1.ReindexRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReindexMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_INDEX = 0;
  private static final int METHODID_DELETE_INDEX = 1;
  private static final int METHODID_LIST_INDEXES = 2;
  private static final int METHODID_SEARCH = 3;
  private static final int METHODID_REINDEX = 4;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CREATE_INDEX:
          serviceImpl.createIndex((com.udb.core.search.services.v1.CreateIndexRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.search.services.v1.CreateIndexResponse>) responseObserver);
          break;
        case METHODID_DELETE_INDEX:
          serviceImpl.deleteIndex((com.udb.core.search.services.v1.DeleteIndexRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.search.services.v1.DeleteIndexResponse>) responseObserver);
          break;
        case METHODID_LIST_INDEXES:
          serviceImpl.listIndexes((com.udb.core.search.services.v1.ListIndexesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.search.services.v1.ListIndexesResponse>) responseObserver);
          break;
        case METHODID_SEARCH:
          serviceImpl.search((com.udb.core.search.services.v1.SearchRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.search.services.v1.SearchResponse>) responseObserver);
          break;
        case METHODID_REINDEX:
          serviceImpl.reindex((com.udb.core.search.services.v1.ReindexRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.search.services.v1.ReindexResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getCreateIndexMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.search.services.v1.CreateIndexRequest,
              com.udb.core.search.services.v1.CreateIndexResponse>(
                service, METHODID_CREATE_INDEX)))
        .addMethod(
          getDeleteIndexMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.search.services.v1.DeleteIndexRequest,
              com.udb.core.search.services.v1.DeleteIndexResponse>(
                service, METHODID_DELETE_INDEX)))
        .addMethod(
          getListIndexesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.search.services.v1.ListIndexesRequest,
              com.udb.core.search.services.v1.ListIndexesResponse>(
                service, METHODID_LIST_INDEXES)))
        .addMethod(
          getSearchMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.search.services.v1.SearchRequest,
              com.udb.core.search.services.v1.SearchResponse>(
                service, METHODID_SEARCH)))
        .addMethod(
          getReindexMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.search.services.v1.ReindexRequest,
              com.udb.core.search.services.v1.ReindexResponse>(
                service, METHODID_REINDEX)))
        .build();
  }

  private static abstract class SearchServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    SearchServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.search.services.v1.SearchServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("SearchService");
    }
  }

  private static final class SearchServiceFileDescriptorSupplier
      extends SearchServiceBaseDescriptorSupplier {
    SearchServiceFileDescriptorSupplier() {}
  }

  private static final class SearchServiceMethodDescriptorSupplier
      extends SearchServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    SearchServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (SearchServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new SearchServiceFileDescriptorSupplier())
              .addMethod(getCreateIndexMethod())
              .addMethod(getDeleteIndexMethod())
              .addMethod(getListIndexesMethod())
              .addMethod(getSearchMethod())
              .addMethod(getReindexMethod())
              .build();
        }
      }
    }
    return result;
  }
}
