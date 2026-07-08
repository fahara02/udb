package com.udb.core.embedding.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * EmbeddingService (master-plan 9.11) — the AI data plane. Registers tenant-scoped
 * source entities to vector-index on change. INFERENCE RUNS IN SIDECARS ONLY: no
 * embedding model is ever linked into the broker. On a source row change (and on
 * Backfill) the broker emits a `udb.embedding.work.v1` event carrying ONLY the row
 * primary key + extracted text (NEVER credentials); a sidecar computes the vector
 * and returns it via the internal-only `ReportEmbedding` callback, which upserts
 * it through the shared asset vector-upsert seam tagged with the verified tenant.
 * `Retrieve` delegates to the SearchService (9.5) hybrid-search seam with a
 * server-side tenant filter — never a raw vector query.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class EmbeddingServiceGrpc {

  private EmbeddingServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.embedding.services.v1.EmbeddingService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.RegisterSourceRequest,
      com.udb.core.embedding.services.v1.RegisterSourceResponse> getRegisterSourceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RegisterSource",
      requestType = com.udb.core.embedding.services.v1.RegisterSourceRequest.class,
      responseType = com.udb.core.embedding.services.v1.RegisterSourceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.RegisterSourceRequest,
      com.udb.core.embedding.services.v1.RegisterSourceResponse> getRegisterSourceMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.RegisterSourceRequest, com.udb.core.embedding.services.v1.RegisterSourceResponse> getRegisterSourceMethod;
    if ((getRegisterSourceMethod = EmbeddingServiceGrpc.getRegisterSourceMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getRegisterSourceMethod = EmbeddingServiceGrpc.getRegisterSourceMethod) == null) {
          EmbeddingServiceGrpc.getRegisterSourceMethod = getRegisterSourceMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.RegisterSourceRequest, com.udb.core.embedding.services.v1.RegisterSourceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RegisterSource"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.RegisterSourceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.RegisterSourceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("RegisterSource"))
              .build();
        }
      }
    }
    return getRegisterSourceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ListSourcesRequest,
      com.udb.core.embedding.services.v1.ListSourcesResponse> getListSourcesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListSources",
      requestType = com.udb.core.embedding.services.v1.ListSourcesRequest.class,
      responseType = com.udb.core.embedding.services.v1.ListSourcesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ListSourcesRequest,
      com.udb.core.embedding.services.v1.ListSourcesResponse> getListSourcesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ListSourcesRequest, com.udb.core.embedding.services.v1.ListSourcesResponse> getListSourcesMethod;
    if ((getListSourcesMethod = EmbeddingServiceGrpc.getListSourcesMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getListSourcesMethod = EmbeddingServiceGrpc.getListSourcesMethod) == null) {
          EmbeddingServiceGrpc.getListSourcesMethod = getListSourcesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.ListSourcesRequest, com.udb.core.embedding.services.v1.ListSourcesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListSources"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ListSourcesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ListSourcesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("ListSources"))
              .build();
        }
      }
    }
    return getListSourcesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.DeleteSourceRequest,
      com.udb.core.embedding.services.v1.DeleteSourceResponse> getDeleteSourceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteSource",
      requestType = com.udb.core.embedding.services.v1.DeleteSourceRequest.class,
      responseType = com.udb.core.embedding.services.v1.DeleteSourceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.DeleteSourceRequest,
      com.udb.core.embedding.services.v1.DeleteSourceResponse> getDeleteSourceMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.DeleteSourceRequest, com.udb.core.embedding.services.v1.DeleteSourceResponse> getDeleteSourceMethod;
    if ((getDeleteSourceMethod = EmbeddingServiceGrpc.getDeleteSourceMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getDeleteSourceMethod = EmbeddingServiceGrpc.getDeleteSourceMethod) == null) {
          EmbeddingServiceGrpc.getDeleteSourceMethod = getDeleteSourceMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.DeleteSourceRequest, com.udb.core.embedding.services.v1.DeleteSourceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteSource"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.DeleteSourceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.DeleteSourceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("DeleteSource"))
              .build();
        }
      }
    }
    return getDeleteSourceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.BackfillRequest,
      com.udb.core.embedding.services.v1.BackfillResponse> getBackfillMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Backfill",
      requestType = com.udb.core.embedding.services.v1.BackfillRequest.class,
      responseType = com.udb.core.embedding.services.v1.BackfillResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.BackfillRequest,
      com.udb.core.embedding.services.v1.BackfillResponse> getBackfillMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.BackfillRequest, com.udb.core.embedding.services.v1.BackfillResponse> getBackfillMethod;
    if ((getBackfillMethod = EmbeddingServiceGrpc.getBackfillMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getBackfillMethod = EmbeddingServiceGrpc.getBackfillMethod) == null) {
          EmbeddingServiceGrpc.getBackfillMethod = getBackfillMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.BackfillRequest, com.udb.core.embedding.services.v1.BackfillResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Backfill"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.BackfillRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.BackfillResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("Backfill"))
              .build();
        }
      }
    }
    return getBackfillMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportEmbeddingRequest,
      com.udb.core.embedding.services.v1.ReportEmbeddingResponse> getReportEmbeddingMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReportEmbedding",
      requestType = com.udb.core.embedding.services.v1.ReportEmbeddingRequest.class,
      responseType = com.udb.core.embedding.services.v1.ReportEmbeddingResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportEmbeddingRequest,
      com.udb.core.embedding.services.v1.ReportEmbeddingResponse> getReportEmbeddingMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportEmbeddingRequest, com.udb.core.embedding.services.v1.ReportEmbeddingResponse> getReportEmbeddingMethod;
    if ((getReportEmbeddingMethod = EmbeddingServiceGrpc.getReportEmbeddingMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getReportEmbeddingMethod = EmbeddingServiceGrpc.getReportEmbeddingMethod) == null) {
          EmbeddingServiceGrpc.getReportEmbeddingMethod = getReportEmbeddingMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.ReportEmbeddingRequest, com.udb.core.embedding.services.v1.ReportEmbeddingResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReportEmbedding"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ReportEmbeddingRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ReportEmbeddingResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("ReportEmbedding"))
              .build();
        }
      }
    }
    return getReportEmbeddingMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.RetrieveRequest,
      com.udb.core.embedding.services.v1.RetrieveResponse> getRetrieveMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Retrieve",
      requestType = com.udb.core.embedding.services.v1.RetrieveRequest.class,
      responseType = com.udb.core.embedding.services.v1.RetrieveResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.RetrieveRequest,
      com.udb.core.embedding.services.v1.RetrieveResponse> getRetrieveMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.RetrieveRequest, com.udb.core.embedding.services.v1.RetrieveResponse> getRetrieveMethod;
    if ((getRetrieveMethod = EmbeddingServiceGrpc.getRetrieveMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getRetrieveMethod = EmbeddingServiceGrpc.getRetrieveMethod) == null) {
          EmbeddingServiceGrpc.getRetrieveMethod = getRetrieveMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.RetrieveRequest, com.udb.core.embedding.services.v1.RetrieveResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Retrieve"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.RetrieveRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.RetrieveResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("Retrieve"))
              .build();
        }
      }
    }
    return getRetrieveMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static EmbeddingServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceStub>() {
        @java.lang.Override
        public EmbeddingServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EmbeddingServiceStub(channel, callOptions);
        }
      };
    return EmbeddingServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static EmbeddingServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceBlockingV2Stub>() {
        @java.lang.Override
        public EmbeddingServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EmbeddingServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return EmbeddingServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static EmbeddingServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceBlockingStub>() {
        @java.lang.Override
        public EmbeddingServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EmbeddingServiceBlockingStub(channel, callOptions);
        }
      };
    return EmbeddingServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static EmbeddingServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceFutureStub>() {
        @java.lang.Override
        public EmbeddingServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EmbeddingServiceFutureStub(channel, callOptions);
        }
      };
    return EmbeddingServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * EmbeddingService (master-plan 9.11) — the AI data plane. Registers tenant-scoped
   * source entities to vector-index on change. INFERENCE RUNS IN SIDECARS ONLY: no
   * embedding model is ever linked into the broker. On a source row change (and on
   * Backfill) the broker emits a `udb.embedding.work.v1` event carrying ONLY the row
   * primary key + extracted text (NEVER credentials); a sidecar computes the vector
   * and returns it via the internal-only `ReportEmbedding` callback, which upserts
   * it through the shared asset vector-upsert seam tagged with the verified tenant.
   * `Retrieve` delegates to the SearchService (9.5) hybrid-search seam with a
   * server-side tenant filter — never a raw vector query.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Register a tenant-scoped source to vector-index on change. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * </pre>
     */
    default void registerSource(com.udb.core.embedding.services.v1.RegisterSourceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.RegisterSourceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRegisterSourceMethod(), responseObserver);
    }

    /**
     * <pre>
     * List the calling tenant's registered sources.
     * </pre>
     */
    default void listSources(com.udb.core.embedding.services.v1.ListSourcesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ListSourcesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListSourcesMethod(), responseObserver);
    }

    /**
     * <pre>
     * Delete a tenant-scoped source registration (destructive: stops indexing on
     * change; the engine collection teardown runs on the follow-up worker).
     * </pre>
     */
    default void deleteSource(com.udb.core.embedding.services.v1.DeleteSourceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.DeleteSourceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteSourceMethod(), responseObserver);
    }

    /**
     * <pre>
     * Enqueue embedding work for the source's EXISTING rows. The per-row work
     * enumeration runs in the leader-spawned work emitter, which calls the same
     * `udb.embedding.work.v1` emit path the CDC change handler uses.
     * </pre>
     */
    default void backfill(com.udb.core.embedding.services.v1.BackfillRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.BackfillResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getBackfillMethod(), responseObserver);
    }

    /**
     * <pre>
     * SIDECAR CALLBACK (internal only). A sidecar that computed an embedding for a
     * source row returns the dense vector here; the broker upserts it through the
     * shared asset vector-upsert seam, tagged with the VERIFIED claim tenant (a
     * vector with no/foreign tenant is rejected — no fail-open). `internal_grpc_only`
     * restricts this to a loopback peer; it is never exposed in an SDK facade.
     * </pre>
     */
    default void reportEmbedding(com.udb.core.embedding.services.v1.ReportEmbeddingRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportEmbeddingResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReportEmbeddingMethod(), responseObserver);
    }

    /**
     * <pre>
     * Deadline-bounded semantic search over a source's vector collection. DELEGATES
     * to the SearchService (9.5) hybrid-search seam with a server-side tenant filter
     * injected from the verified claim. The broker never embeds the query (the
     * caller supplies an already-embedded `query_vector`); it never issues a raw
     * engine query. Returns `deadline_exceeded` if the gRPC deadline is past.
     * </pre>
     */
    default void retrieve(com.udb.core.embedding.services.v1.RetrieveRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.RetrieveResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRetrieveMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service EmbeddingService.
   * <pre>
   * EmbeddingService (master-plan 9.11) — the AI data plane. Registers tenant-scoped
   * source entities to vector-index on change. INFERENCE RUNS IN SIDECARS ONLY: no
   * embedding model is ever linked into the broker. On a source row change (and on
   * Backfill) the broker emits a `udb.embedding.work.v1` event carrying ONLY the row
   * primary key + extracted text (NEVER credentials); a sidecar computes the vector
   * and returns it via the internal-only `ReportEmbedding` callback, which upserts
   * it through the shared asset vector-upsert seam tagged with the verified tenant.
   * `Retrieve` delegates to the SearchService (9.5) hybrid-search seam with a
   * server-side tenant filter — never a raw vector query.
   * </pre>
   */
  public static abstract class EmbeddingServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return EmbeddingServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service EmbeddingService.
   * <pre>
   * EmbeddingService (master-plan 9.11) — the AI data plane. Registers tenant-scoped
   * source entities to vector-index on change. INFERENCE RUNS IN SIDECARS ONLY: no
   * embedding model is ever linked into the broker. On a source row change (and on
   * Backfill) the broker emits a `udb.embedding.work.v1` event carrying ONLY the row
   * primary key + extracted text (NEVER credentials); a sidecar computes the vector
   * and returns it via the internal-only `ReportEmbedding` callback, which upserts
   * it through the shared asset vector-upsert seam tagged with the verified tenant.
   * `Retrieve` delegates to the SearchService (9.5) hybrid-search seam with a
   * server-side tenant filter — never a raw vector query.
   * </pre>
   */
  public static final class EmbeddingServiceStub
      extends io.grpc.stub.AbstractAsyncStub<EmbeddingServiceStub> {
    private EmbeddingServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EmbeddingServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EmbeddingServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a tenant-scoped source to vector-index on change. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * </pre>
     */
    public void registerSource(com.udb.core.embedding.services.v1.RegisterSourceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.RegisterSourceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRegisterSourceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List the calling tenant's registered sources.
     * </pre>
     */
    public void listSources(com.udb.core.embedding.services.v1.ListSourcesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ListSourcesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListSourcesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Delete a tenant-scoped source registration (destructive: stops indexing on
     * change; the engine collection teardown runs on the follow-up worker).
     * </pre>
     */
    public void deleteSource(com.udb.core.embedding.services.v1.DeleteSourceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.DeleteSourceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteSourceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Enqueue embedding work for the source's EXISTING rows. The per-row work
     * enumeration runs in the leader-spawned work emitter, which calls the same
     * `udb.embedding.work.v1` emit path the CDC change handler uses.
     * </pre>
     */
    public void backfill(com.udb.core.embedding.services.v1.BackfillRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.BackfillResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getBackfillMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * SIDECAR CALLBACK (internal only). A sidecar that computed an embedding for a
     * source row returns the dense vector here; the broker upserts it through the
     * shared asset vector-upsert seam, tagged with the VERIFIED claim tenant (a
     * vector with no/foreign tenant is rejected — no fail-open). `internal_grpc_only`
     * restricts this to a loopback peer; it is never exposed in an SDK facade.
     * </pre>
     */
    public void reportEmbedding(com.udb.core.embedding.services.v1.ReportEmbeddingRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportEmbeddingResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReportEmbeddingMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Deadline-bounded semantic search over a source's vector collection. DELEGATES
     * to the SearchService (9.5) hybrid-search seam with a server-side tenant filter
     * injected from the verified claim. The broker never embeds the query (the
     * caller supplies an already-embedded `query_vector`); it never issues a raw
     * engine query. Returns `deadline_exceeded` if the gRPC deadline is past.
     * </pre>
     */
    public void retrieve(com.udb.core.embedding.services.v1.RetrieveRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.RetrieveResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRetrieveMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service EmbeddingService.
   * <pre>
   * EmbeddingService (master-plan 9.11) — the AI data plane. Registers tenant-scoped
   * source entities to vector-index on change. INFERENCE RUNS IN SIDECARS ONLY: no
   * embedding model is ever linked into the broker. On a source row change (and on
   * Backfill) the broker emits a `udb.embedding.work.v1` event carrying ONLY the row
   * primary key + extracted text (NEVER credentials); a sidecar computes the vector
   * and returns it via the internal-only `ReportEmbedding` callback, which upserts
   * it through the shared asset vector-upsert seam tagged with the verified tenant.
   * `Retrieve` delegates to the SearchService (9.5) hybrid-search seam with a
   * server-side tenant filter — never a raw vector query.
   * </pre>
   */
  public static final class EmbeddingServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<EmbeddingServiceBlockingV2Stub> {
    private EmbeddingServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EmbeddingServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EmbeddingServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a tenant-scoped source to vector-index on change. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.RegisterSourceResponse registerSource(com.udb.core.embedding.services.v1.RegisterSourceRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRegisterSourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List the calling tenant's registered sources.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.ListSourcesResponse listSources(com.udb.core.embedding.services.v1.ListSourcesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListSourcesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a tenant-scoped source registration (destructive: stops indexing on
     * change; the engine collection teardown runs on the follow-up worker).
     * </pre>
     */
    public com.udb.core.embedding.services.v1.DeleteSourceResponse deleteSource(com.udb.core.embedding.services.v1.DeleteSourceRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeleteSourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Enqueue embedding work for the source's EXISTING rows. The per-row work
     * enumeration runs in the leader-spawned work emitter, which calls the same
     * `udb.embedding.work.v1` emit path the CDC change handler uses.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.BackfillResponse backfill(com.udb.core.embedding.services.v1.BackfillRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getBackfillMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * SIDECAR CALLBACK (internal only). A sidecar that computed an embedding for a
     * source row returns the dense vector here; the broker upserts it through the
     * shared asset vector-upsert seam, tagged with the VERIFIED claim tenant (a
     * vector with no/foreign tenant is rejected — no fail-open). `internal_grpc_only`
     * restricts this to a loopback peer; it is never exposed in an SDK facade.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.ReportEmbeddingResponse reportEmbedding(com.udb.core.embedding.services.v1.ReportEmbeddingRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getReportEmbeddingMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Deadline-bounded semantic search over a source's vector collection. DELEGATES
     * to the SearchService (9.5) hybrid-search seam with a server-side tenant filter
     * injected from the verified claim. The broker never embeds the query (the
     * caller supplies an already-embedded `query_vector`); it never issues a raw
     * engine query. Returns `deadline_exceeded` if the gRPC deadline is past.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.RetrieveResponse retrieve(com.udb.core.embedding.services.v1.RetrieveRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRetrieveMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service EmbeddingService.
   * <pre>
   * EmbeddingService (master-plan 9.11) — the AI data plane. Registers tenant-scoped
   * source entities to vector-index on change. INFERENCE RUNS IN SIDECARS ONLY: no
   * embedding model is ever linked into the broker. On a source row change (and on
   * Backfill) the broker emits a `udb.embedding.work.v1` event carrying ONLY the row
   * primary key + extracted text (NEVER credentials); a sidecar computes the vector
   * and returns it via the internal-only `ReportEmbedding` callback, which upserts
   * it through the shared asset vector-upsert seam tagged with the verified tenant.
   * `Retrieve` delegates to the SearchService (9.5) hybrid-search seam with a
   * server-side tenant filter — never a raw vector query.
   * </pre>
   */
  public static final class EmbeddingServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<EmbeddingServiceBlockingStub> {
    private EmbeddingServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EmbeddingServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EmbeddingServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a tenant-scoped source to vector-index on change. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.RegisterSourceResponse registerSource(com.udb.core.embedding.services.v1.RegisterSourceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRegisterSourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List the calling tenant's registered sources.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.ListSourcesResponse listSources(com.udb.core.embedding.services.v1.ListSourcesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListSourcesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a tenant-scoped source registration (destructive: stops indexing on
     * change; the engine collection teardown runs on the follow-up worker).
     * </pre>
     */
    public com.udb.core.embedding.services.v1.DeleteSourceResponse deleteSource(com.udb.core.embedding.services.v1.DeleteSourceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteSourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Enqueue embedding work for the source's EXISTING rows. The per-row work
     * enumeration runs in the leader-spawned work emitter, which calls the same
     * `udb.embedding.work.v1` emit path the CDC change handler uses.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.BackfillResponse backfill(com.udb.core.embedding.services.v1.BackfillRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getBackfillMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * SIDECAR CALLBACK (internal only). A sidecar that computed an embedding for a
     * source row returns the dense vector here; the broker upserts it through the
     * shared asset vector-upsert seam, tagged with the VERIFIED claim tenant (a
     * vector with no/foreign tenant is rejected — no fail-open). `internal_grpc_only`
     * restricts this to a loopback peer; it is never exposed in an SDK facade.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.ReportEmbeddingResponse reportEmbedding(com.udb.core.embedding.services.v1.ReportEmbeddingRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReportEmbeddingMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Deadline-bounded semantic search over a source's vector collection. DELEGATES
     * to the SearchService (9.5) hybrid-search seam with a server-side tenant filter
     * injected from the verified claim. The broker never embeds the query (the
     * caller supplies an already-embedded `query_vector`); it never issues a raw
     * engine query. Returns `deadline_exceeded` if the gRPC deadline is past.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.RetrieveResponse retrieve(com.udb.core.embedding.services.v1.RetrieveRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRetrieveMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service EmbeddingService.
   * <pre>
   * EmbeddingService (master-plan 9.11) — the AI data plane. Registers tenant-scoped
   * source entities to vector-index on change. INFERENCE RUNS IN SIDECARS ONLY: no
   * embedding model is ever linked into the broker. On a source row change (and on
   * Backfill) the broker emits a `udb.embedding.work.v1` event carrying ONLY the row
   * primary key + extracted text (NEVER credentials); a sidecar computes the vector
   * and returns it via the internal-only `ReportEmbedding` callback, which upserts
   * it through the shared asset vector-upsert seam tagged with the verified tenant.
   * `Retrieve` delegates to the SearchService (9.5) hybrid-search seam with a
   * server-side tenant filter — never a raw vector query.
   * </pre>
   */
  public static final class EmbeddingServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<EmbeddingServiceFutureStub> {
    private EmbeddingServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EmbeddingServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EmbeddingServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a tenant-scoped source to vector-index on change. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.RegisterSourceResponse> registerSource(
        com.udb.core.embedding.services.v1.RegisterSourceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRegisterSourceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List the calling tenant's registered sources.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.ListSourcesResponse> listSources(
        com.udb.core.embedding.services.v1.ListSourcesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListSourcesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Delete a tenant-scoped source registration (destructive: stops indexing on
     * change; the engine collection teardown runs on the follow-up worker).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.DeleteSourceResponse> deleteSource(
        com.udb.core.embedding.services.v1.DeleteSourceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteSourceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Enqueue embedding work for the source's EXISTING rows. The per-row work
     * enumeration runs in the leader-spawned work emitter, which calls the same
     * `udb.embedding.work.v1` emit path the CDC change handler uses.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.BackfillResponse> backfill(
        com.udb.core.embedding.services.v1.BackfillRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getBackfillMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * SIDECAR CALLBACK (internal only). A sidecar that computed an embedding for a
     * source row returns the dense vector here; the broker upserts it through the
     * shared asset vector-upsert seam, tagged with the VERIFIED claim tenant (a
     * vector with no/foreign tenant is rejected — no fail-open). `internal_grpc_only`
     * restricts this to a loopback peer; it is never exposed in an SDK facade.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.ReportEmbeddingResponse> reportEmbedding(
        com.udb.core.embedding.services.v1.ReportEmbeddingRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReportEmbeddingMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Deadline-bounded semantic search over a source's vector collection. DELEGATES
     * to the SearchService (9.5) hybrid-search seam with a server-side tenant filter
     * injected from the verified claim. The broker never embeds the query (the
     * caller supplies an already-embedded `query_vector`); it never issues a raw
     * engine query. Returns `deadline_exceeded` if the gRPC deadline is past.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.RetrieveResponse> retrieve(
        com.udb.core.embedding.services.v1.RetrieveRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRetrieveMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_REGISTER_SOURCE = 0;
  private static final int METHODID_LIST_SOURCES = 1;
  private static final int METHODID_DELETE_SOURCE = 2;
  private static final int METHODID_BACKFILL = 3;
  private static final int METHODID_REPORT_EMBEDDING = 4;
  private static final int METHODID_RETRIEVE = 5;

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
        case METHODID_REGISTER_SOURCE:
          serviceImpl.registerSource((com.udb.core.embedding.services.v1.RegisterSourceRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.RegisterSourceResponse>) responseObserver);
          break;
        case METHODID_LIST_SOURCES:
          serviceImpl.listSources((com.udb.core.embedding.services.v1.ListSourcesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ListSourcesResponse>) responseObserver);
          break;
        case METHODID_DELETE_SOURCE:
          serviceImpl.deleteSource((com.udb.core.embedding.services.v1.DeleteSourceRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.DeleteSourceResponse>) responseObserver);
          break;
        case METHODID_BACKFILL:
          serviceImpl.backfill((com.udb.core.embedding.services.v1.BackfillRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.BackfillResponse>) responseObserver);
          break;
        case METHODID_REPORT_EMBEDDING:
          serviceImpl.reportEmbedding((com.udb.core.embedding.services.v1.ReportEmbeddingRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportEmbeddingResponse>) responseObserver);
          break;
        case METHODID_RETRIEVE:
          serviceImpl.retrieve((com.udb.core.embedding.services.v1.RetrieveRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.RetrieveResponse>) responseObserver);
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
          getRegisterSourceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.RegisterSourceRequest,
              com.udb.core.embedding.services.v1.RegisterSourceResponse>(
                service, METHODID_REGISTER_SOURCE)))
        .addMethod(
          getListSourcesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.ListSourcesRequest,
              com.udb.core.embedding.services.v1.ListSourcesResponse>(
                service, METHODID_LIST_SOURCES)))
        .addMethod(
          getDeleteSourceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.DeleteSourceRequest,
              com.udb.core.embedding.services.v1.DeleteSourceResponse>(
                service, METHODID_DELETE_SOURCE)))
        .addMethod(
          getBackfillMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.BackfillRequest,
              com.udb.core.embedding.services.v1.BackfillResponse>(
                service, METHODID_BACKFILL)))
        .addMethod(
          getReportEmbeddingMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.ReportEmbeddingRequest,
              com.udb.core.embedding.services.v1.ReportEmbeddingResponse>(
                service, METHODID_REPORT_EMBEDDING)))
        .addMethod(
          getRetrieveMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.RetrieveRequest,
              com.udb.core.embedding.services.v1.RetrieveResponse>(
                service, METHODID_RETRIEVE)))
        .build();
  }

  private static abstract class EmbeddingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    EmbeddingServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.embedding.services.v1.EmbeddingServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("EmbeddingService");
    }
  }

  private static final class EmbeddingServiceFileDescriptorSupplier
      extends EmbeddingServiceBaseDescriptorSupplier {
    EmbeddingServiceFileDescriptorSupplier() {}
  }

  private static final class EmbeddingServiceMethodDescriptorSupplier
      extends EmbeddingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    EmbeddingServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (EmbeddingServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new EmbeddingServiceFileDescriptorSupplier())
              .addMethod(getRegisterSourceMethod())
              .addMethod(getListSourcesMethod())
              .addMethod(getDeleteSourceMethod())
              .addMethod(getBackfillMethod())
              .addMethod(getReportEmbeddingMethod())
              .addMethod(getRetrieveMethod())
              .build();
        }
      }
    }
    return result;
  }
}
