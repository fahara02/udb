package com.udb.core.metering.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * MeteringService (master-plan 9.9) — usage metering and quotas. Usage is an
 * append-only, durable stream of `UsageEvent` rows (written by a cheap admission
 * hook and by explicit RecordUsage ingest); quotas (`QuotaRule`) cap a metric
 * over a rolling window. CheckQuota is PURE aggregation — it sums the durable
 * rows in the window and compares against the limit (never an in-memory counter,
 * which would lie across restarts and replicas). Metering must NEVER fail the
 * metered request: the ingest hook log-and-swallows on error. Quota mutations are
 * durable, tenant-scoped by the verified claim, bump a monotone per-row revision,
 * and emit `udb.metering.quota.changed.v1`.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class MeteringServiceGrpc {

  private MeteringServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.metering.services.v1.MeteringService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.RecordUsageRequest,
      com.udb.core.metering.services.v1.RecordUsageResponse> getRecordUsageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RecordUsage",
      requestType = com.udb.core.metering.services.v1.RecordUsageRequest.class,
      responseType = com.udb.core.metering.services.v1.RecordUsageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.RecordUsageRequest,
      com.udb.core.metering.services.v1.RecordUsageResponse> getRecordUsageMethod() {
    io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.RecordUsageRequest, com.udb.core.metering.services.v1.RecordUsageResponse> getRecordUsageMethod;
    if ((getRecordUsageMethod = MeteringServiceGrpc.getRecordUsageMethod) == null) {
      synchronized (MeteringServiceGrpc.class) {
        if ((getRecordUsageMethod = MeteringServiceGrpc.getRecordUsageMethod) == null) {
          MeteringServiceGrpc.getRecordUsageMethod = getRecordUsageMethod =
              io.grpc.MethodDescriptor.<com.udb.core.metering.services.v1.RecordUsageRequest, com.udb.core.metering.services.v1.RecordUsageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RecordUsage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.metering.services.v1.RecordUsageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.metering.services.v1.RecordUsageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MeteringServiceMethodDescriptorSupplier("RecordUsage"))
              .build();
        }
      }
    }
    return getRecordUsageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.QueryUsageRequest,
      com.udb.core.metering.services.v1.QueryUsageResponse> getQueryUsageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "QueryUsage",
      requestType = com.udb.core.metering.services.v1.QueryUsageRequest.class,
      responseType = com.udb.core.metering.services.v1.QueryUsageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.QueryUsageRequest,
      com.udb.core.metering.services.v1.QueryUsageResponse> getQueryUsageMethod() {
    io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.QueryUsageRequest, com.udb.core.metering.services.v1.QueryUsageResponse> getQueryUsageMethod;
    if ((getQueryUsageMethod = MeteringServiceGrpc.getQueryUsageMethod) == null) {
      synchronized (MeteringServiceGrpc.class) {
        if ((getQueryUsageMethod = MeteringServiceGrpc.getQueryUsageMethod) == null) {
          MeteringServiceGrpc.getQueryUsageMethod = getQueryUsageMethod =
              io.grpc.MethodDescriptor.<com.udb.core.metering.services.v1.QueryUsageRequest, com.udb.core.metering.services.v1.QueryUsageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "QueryUsage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.metering.services.v1.QueryUsageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.metering.services.v1.QueryUsageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MeteringServiceMethodDescriptorSupplier("QueryUsage"))
              .build();
        }
      }
    }
    return getQueryUsageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.PutQuotaRequest,
      com.udb.core.metering.services.v1.PutQuotaResponse> getPutQuotaMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PutQuota",
      requestType = com.udb.core.metering.services.v1.PutQuotaRequest.class,
      responseType = com.udb.core.metering.services.v1.PutQuotaResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.PutQuotaRequest,
      com.udb.core.metering.services.v1.PutQuotaResponse> getPutQuotaMethod() {
    io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.PutQuotaRequest, com.udb.core.metering.services.v1.PutQuotaResponse> getPutQuotaMethod;
    if ((getPutQuotaMethod = MeteringServiceGrpc.getPutQuotaMethod) == null) {
      synchronized (MeteringServiceGrpc.class) {
        if ((getPutQuotaMethod = MeteringServiceGrpc.getPutQuotaMethod) == null) {
          MeteringServiceGrpc.getPutQuotaMethod = getPutQuotaMethod =
              io.grpc.MethodDescriptor.<com.udb.core.metering.services.v1.PutQuotaRequest, com.udb.core.metering.services.v1.PutQuotaResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PutQuota"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.metering.services.v1.PutQuotaRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.metering.services.v1.PutQuotaResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MeteringServiceMethodDescriptorSupplier("PutQuota"))
              .build();
        }
      }
    }
    return getPutQuotaMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.GetQuotaRequest,
      com.udb.core.metering.services.v1.GetQuotaResponse> getGetQuotaMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetQuota",
      requestType = com.udb.core.metering.services.v1.GetQuotaRequest.class,
      responseType = com.udb.core.metering.services.v1.GetQuotaResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.GetQuotaRequest,
      com.udb.core.metering.services.v1.GetQuotaResponse> getGetQuotaMethod() {
    io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.GetQuotaRequest, com.udb.core.metering.services.v1.GetQuotaResponse> getGetQuotaMethod;
    if ((getGetQuotaMethod = MeteringServiceGrpc.getGetQuotaMethod) == null) {
      synchronized (MeteringServiceGrpc.class) {
        if ((getGetQuotaMethod = MeteringServiceGrpc.getGetQuotaMethod) == null) {
          MeteringServiceGrpc.getGetQuotaMethod = getGetQuotaMethod =
              io.grpc.MethodDescriptor.<com.udb.core.metering.services.v1.GetQuotaRequest, com.udb.core.metering.services.v1.GetQuotaResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetQuota"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.metering.services.v1.GetQuotaRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.metering.services.v1.GetQuotaResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MeteringServiceMethodDescriptorSupplier("GetQuota"))
              .build();
        }
      }
    }
    return getGetQuotaMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.ListQuotasRequest,
      com.udb.core.metering.services.v1.ListQuotasResponse> getListQuotasMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListQuotas",
      requestType = com.udb.core.metering.services.v1.ListQuotasRequest.class,
      responseType = com.udb.core.metering.services.v1.ListQuotasResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.ListQuotasRequest,
      com.udb.core.metering.services.v1.ListQuotasResponse> getListQuotasMethod() {
    io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.ListQuotasRequest, com.udb.core.metering.services.v1.ListQuotasResponse> getListQuotasMethod;
    if ((getListQuotasMethod = MeteringServiceGrpc.getListQuotasMethod) == null) {
      synchronized (MeteringServiceGrpc.class) {
        if ((getListQuotasMethod = MeteringServiceGrpc.getListQuotasMethod) == null) {
          MeteringServiceGrpc.getListQuotasMethod = getListQuotasMethod =
              io.grpc.MethodDescriptor.<com.udb.core.metering.services.v1.ListQuotasRequest, com.udb.core.metering.services.v1.ListQuotasResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListQuotas"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.metering.services.v1.ListQuotasRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.metering.services.v1.ListQuotasResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MeteringServiceMethodDescriptorSupplier("ListQuotas"))
              .build();
        }
      }
    }
    return getListQuotasMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.CheckQuotaRequest,
      com.udb.core.metering.services.v1.CheckQuotaResponse> getCheckQuotaMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CheckQuota",
      requestType = com.udb.core.metering.services.v1.CheckQuotaRequest.class,
      responseType = com.udb.core.metering.services.v1.CheckQuotaResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.CheckQuotaRequest,
      com.udb.core.metering.services.v1.CheckQuotaResponse> getCheckQuotaMethod() {
    io.grpc.MethodDescriptor<com.udb.core.metering.services.v1.CheckQuotaRequest, com.udb.core.metering.services.v1.CheckQuotaResponse> getCheckQuotaMethod;
    if ((getCheckQuotaMethod = MeteringServiceGrpc.getCheckQuotaMethod) == null) {
      synchronized (MeteringServiceGrpc.class) {
        if ((getCheckQuotaMethod = MeteringServiceGrpc.getCheckQuotaMethod) == null) {
          MeteringServiceGrpc.getCheckQuotaMethod = getCheckQuotaMethod =
              io.grpc.MethodDescriptor.<com.udb.core.metering.services.v1.CheckQuotaRequest, com.udb.core.metering.services.v1.CheckQuotaResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CheckQuota"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.metering.services.v1.CheckQuotaRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.metering.services.v1.CheckQuotaResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MeteringServiceMethodDescriptorSupplier("CheckQuota"))
              .build();
        }
      }
    }
    return getCheckQuotaMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MeteringServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MeteringServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MeteringServiceStub>() {
        @java.lang.Override
        public MeteringServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MeteringServiceStub(channel, callOptions);
        }
      };
    return MeteringServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static MeteringServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MeteringServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MeteringServiceBlockingV2Stub>() {
        @java.lang.Override
        public MeteringServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MeteringServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return MeteringServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MeteringServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MeteringServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MeteringServiceBlockingStub>() {
        @java.lang.Override
        public MeteringServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MeteringServiceBlockingStub(channel, callOptions);
        }
      };
    return MeteringServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MeteringServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MeteringServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MeteringServiceFutureStub>() {
        @java.lang.Override
        public MeteringServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MeteringServiceFutureStub(channel, callOptions);
        }
      };
    return MeteringServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * MeteringService (master-plan 9.9) — usage metering and quotas. Usage is an
   * append-only, durable stream of `UsageEvent` rows (written by a cheap admission
   * hook and by explicit RecordUsage ingest); quotas (`QuotaRule`) cap a metric
   * over a rolling window. CheckQuota is PURE aggregation — it sums the durable
   * rows in the window and compares against the limit (never an in-memory counter,
   * which would lie across restarts and replicas). Metering must NEVER fail the
   * metered request: the ingest hook log-and-swallows on error. Quota mutations are
   * durable, tenant-scoped by the verified claim, bump a monotone per-row revision,
   * and emit `udb.metering.quota.changed.v1`.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Explicitly ingest a usage event. Durable append (single INSERT, no read);
     * attribution-only — it never blocks the caller's real operation.
     * </pre>
     */
    default void recordUsage(com.udb.core.metering.services.v1.RecordUsageRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.RecordUsageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRecordUsageMethod(), responseObserver);
    }

    /**
     * <pre>
     * Aggregate a tenant's usage for a metric over a rolling window (durable SUM).
     * </pre>
     */
    default void queryUsage(com.udb.core.metering.services.v1.QueryUsageRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.QueryUsageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryUsageMethod(), responseObserver);
    }

    /**
     * <pre>
     * Create or update a quota rule at a (tenant, project, metric) scope. Bumps the
     * rule's monotone revision and emits `udb.metering.quota.changed.v1`.
     * </pre>
     */
    default void putQuota(com.udb.core.metering.services.v1.PutQuotaRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.PutQuotaResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPutQuotaMethod(), responseObserver);
    }

    /**
     * <pre>
     * Fetch a single quota rule at an exact (tenant, project, metric) scope.
     * </pre>
     */
    default void getQuota(com.udb.core.metering.services.v1.GetQuotaRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.GetQuotaResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetQuotaMethod(), responseObserver);
    }

    /**
     * <pre>
     * List a tenant's quota rules, optionally narrowed to a project.
     * </pre>
     */
    default void listQuotas(com.udb.core.metering.services.v1.ListQuotasRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.ListQuotasResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListQuotasMethod(), responseObserver);
    }

    /**
     * <pre>
     * Check a quota: sum durable usage in the rule's window and compare against the
     * limit. Returns {allowed, used, limit, remaining}. The ingest hook remains
     * best-effort, but explicit quota checks fail closed when the durable aggregate
     * is unavailable, so an outage cannot silently bypass an enabled quota.
     * </pre>
     */
    default void checkQuota(com.udb.core.metering.services.v1.CheckQuotaRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.CheckQuotaResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCheckQuotaMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service MeteringService.
   * <pre>
   * MeteringService (master-plan 9.9) — usage metering and quotas. Usage is an
   * append-only, durable stream of `UsageEvent` rows (written by a cheap admission
   * hook and by explicit RecordUsage ingest); quotas (`QuotaRule`) cap a metric
   * over a rolling window. CheckQuota is PURE aggregation — it sums the durable
   * rows in the window and compares against the limit (never an in-memory counter,
   * which would lie across restarts and replicas). Metering must NEVER fail the
   * metered request: the ingest hook log-and-swallows on error. Quota mutations are
   * durable, tenant-scoped by the verified claim, bump a monotone per-row revision,
   * and emit `udb.metering.quota.changed.v1`.
   * </pre>
   */
  public static abstract class MeteringServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return MeteringServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service MeteringService.
   * <pre>
   * MeteringService (master-plan 9.9) — usage metering and quotas. Usage is an
   * append-only, durable stream of `UsageEvent` rows (written by a cheap admission
   * hook and by explicit RecordUsage ingest); quotas (`QuotaRule`) cap a metric
   * over a rolling window. CheckQuota is PURE aggregation — it sums the durable
   * rows in the window and compares against the limit (never an in-memory counter,
   * which would lie across restarts and replicas). Metering must NEVER fail the
   * metered request: the ingest hook log-and-swallows on error. Quota mutations are
   * durable, tenant-scoped by the verified claim, bump a monotone per-row revision,
   * and emit `udb.metering.quota.changed.v1`.
   * </pre>
   */
  public static final class MeteringServiceStub
      extends io.grpc.stub.AbstractAsyncStub<MeteringServiceStub> {
    private MeteringServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MeteringServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MeteringServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Explicitly ingest a usage event. Durable append (single INSERT, no read);
     * attribution-only — it never blocks the caller's real operation.
     * </pre>
     */
    public void recordUsage(com.udb.core.metering.services.v1.RecordUsageRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.RecordUsageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRecordUsageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Aggregate a tenant's usage for a metric over a rolling window (durable SUM).
     * </pre>
     */
    public void queryUsage(com.udb.core.metering.services.v1.QueryUsageRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.QueryUsageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryUsageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Create or update a quota rule at a (tenant, project, metric) scope. Bumps the
     * rule's monotone revision and emits `udb.metering.quota.changed.v1`.
     * </pre>
     */
    public void putQuota(com.udb.core.metering.services.v1.PutQuotaRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.PutQuotaResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPutQuotaMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Fetch a single quota rule at an exact (tenant, project, metric) scope.
     * </pre>
     */
    public void getQuota(com.udb.core.metering.services.v1.GetQuotaRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.GetQuotaResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetQuotaMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List a tenant's quota rules, optionally narrowed to a project.
     * </pre>
     */
    public void listQuotas(com.udb.core.metering.services.v1.ListQuotasRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.ListQuotasResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListQuotasMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Check a quota: sum durable usage in the rule's window and compare against the
     * limit. Returns {allowed, used, limit, remaining}. The ingest hook remains
     * best-effort, but explicit quota checks fail closed when the durable aggregate
     * is unavailable, so an outage cannot silently bypass an enabled quota.
     * </pre>
     */
    public void checkQuota(com.udb.core.metering.services.v1.CheckQuotaRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.CheckQuotaResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCheckQuotaMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service MeteringService.
   * <pre>
   * MeteringService (master-plan 9.9) — usage metering and quotas. Usage is an
   * append-only, durable stream of `UsageEvent` rows (written by a cheap admission
   * hook and by explicit RecordUsage ingest); quotas (`QuotaRule`) cap a metric
   * over a rolling window. CheckQuota is PURE aggregation — it sums the durable
   * rows in the window and compares against the limit (never an in-memory counter,
   * which would lie across restarts and replicas). Metering must NEVER fail the
   * metered request: the ingest hook log-and-swallows on error. Quota mutations are
   * durable, tenant-scoped by the verified claim, bump a monotone per-row revision,
   * and emit `udb.metering.quota.changed.v1`.
   * </pre>
   */
  public static final class MeteringServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<MeteringServiceBlockingV2Stub> {
    private MeteringServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MeteringServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MeteringServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Explicitly ingest a usage event. Durable append (single INSERT, no read);
     * attribution-only — it never blocks the caller's real operation.
     * </pre>
     */
    public com.udb.core.metering.services.v1.RecordUsageResponse recordUsage(com.udb.core.metering.services.v1.RecordUsageRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRecordUsageMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Aggregate a tenant's usage for a metric over a rolling window (durable SUM).
     * </pre>
     */
    public com.udb.core.metering.services.v1.QueryUsageResponse queryUsage(com.udb.core.metering.services.v1.QueryUsageRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getQueryUsageMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Create or update a quota rule at a (tenant, project, metric) scope. Bumps the
     * rule's monotone revision and emits `udb.metering.quota.changed.v1`.
     * </pre>
     */
    public com.udb.core.metering.services.v1.PutQuotaResponse putQuota(com.udb.core.metering.services.v1.PutQuotaRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPutQuotaMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch a single quota rule at an exact (tenant, project, metric) scope.
     * </pre>
     */
    public com.udb.core.metering.services.v1.GetQuotaResponse getQuota(com.udb.core.metering.services.v1.GetQuotaRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetQuotaMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List a tenant's quota rules, optionally narrowed to a project.
     * </pre>
     */
    public com.udb.core.metering.services.v1.ListQuotasResponse listQuotas(com.udb.core.metering.services.v1.ListQuotasRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListQuotasMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Check a quota: sum durable usage in the rule's window and compare against the
     * limit. Returns {allowed, used, limit, remaining}. The ingest hook remains
     * best-effort, but explicit quota checks fail closed when the durable aggregate
     * is unavailable, so an outage cannot silently bypass an enabled quota.
     * </pre>
     */
    public com.udb.core.metering.services.v1.CheckQuotaResponse checkQuota(com.udb.core.metering.services.v1.CheckQuotaRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCheckQuotaMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service MeteringService.
   * <pre>
   * MeteringService (master-plan 9.9) — usage metering and quotas. Usage is an
   * append-only, durable stream of `UsageEvent` rows (written by a cheap admission
   * hook and by explicit RecordUsage ingest); quotas (`QuotaRule`) cap a metric
   * over a rolling window. CheckQuota is PURE aggregation — it sums the durable
   * rows in the window and compares against the limit (never an in-memory counter,
   * which would lie across restarts and replicas). Metering must NEVER fail the
   * metered request: the ingest hook log-and-swallows on error. Quota mutations are
   * durable, tenant-scoped by the verified claim, bump a monotone per-row revision,
   * and emit `udb.metering.quota.changed.v1`.
   * </pre>
   */
  public static final class MeteringServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<MeteringServiceBlockingStub> {
    private MeteringServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MeteringServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MeteringServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Explicitly ingest a usage event. Durable append (single INSERT, no read);
     * attribution-only — it never blocks the caller's real operation.
     * </pre>
     */
    public com.udb.core.metering.services.v1.RecordUsageResponse recordUsage(com.udb.core.metering.services.v1.RecordUsageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRecordUsageMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Aggregate a tenant's usage for a metric over a rolling window (durable SUM).
     * </pre>
     */
    public com.udb.core.metering.services.v1.QueryUsageResponse queryUsage(com.udb.core.metering.services.v1.QueryUsageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryUsageMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Create or update a quota rule at a (tenant, project, metric) scope. Bumps the
     * rule's monotone revision and emits `udb.metering.quota.changed.v1`.
     * </pre>
     */
    public com.udb.core.metering.services.v1.PutQuotaResponse putQuota(com.udb.core.metering.services.v1.PutQuotaRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPutQuotaMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch a single quota rule at an exact (tenant, project, metric) scope.
     * </pre>
     */
    public com.udb.core.metering.services.v1.GetQuotaResponse getQuota(com.udb.core.metering.services.v1.GetQuotaRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetQuotaMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List a tenant's quota rules, optionally narrowed to a project.
     * </pre>
     */
    public com.udb.core.metering.services.v1.ListQuotasResponse listQuotas(com.udb.core.metering.services.v1.ListQuotasRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListQuotasMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Check a quota: sum durable usage in the rule's window and compare against the
     * limit. Returns {allowed, used, limit, remaining}. The ingest hook remains
     * best-effort, but explicit quota checks fail closed when the durable aggregate
     * is unavailable, so an outage cannot silently bypass an enabled quota.
     * </pre>
     */
    public com.udb.core.metering.services.v1.CheckQuotaResponse checkQuota(com.udb.core.metering.services.v1.CheckQuotaRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCheckQuotaMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service MeteringService.
   * <pre>
   * MeteringService (master-plan 9.9) — usage metering and quotas. Usage is an
   * append-only, durable stream of `UsageEvent` rows (written by a cheap admission
   * hook and by explicit RecordUsage ingest); quotas (`QuotaRule`) cap a metric
   * over a rolling window. CheckQuota is PURE aggregation — it sums the durable
   * rows in the window and compares against the limit (never an in-memory counter,
   * which would lie across restarts and replicas). Metering must NEVER fail the
   * metered request: the ingest hook log-and-swallows on error. Quota mutations are
   * durable, tenant-scoped by the verified claim, bump a monotone per-row revision,
   * and emit `udb.metering.quota.changed.v1`.
   * </pre>
   */
  public static final class MeteringServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<MeteringServiceFutureStub> {
    private MeteringServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MeteringServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MeteringServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Explicitly ingest a usage event. Durable append (single INSERT, no read);
     * attribution-only — it never blocks the caller's real operation.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.metering.services.v1.RecordUsageResponse> recordUsage(
        com.udb.core.metering.services.v1.RecordUsageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRecordUsageMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Aggregate a tenant's usage for a metric over a rolling window (durable SUM).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.metering.services.v1.QueryUsageResponse> queryUsage(
        com.udb.core.metering.services.v1.QueryUsageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryUsageMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Create or update a quota rule at a (tenant, project, metric) scope. Bumps the
     * rule's monotone revision and emits `udb.metering.quota.changed.v1`.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.metering.services.v1.PutQuotaResponse> putQuota(
        com.udb.core.metering.services.v1.PutQuotaRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPutQuotaMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Fetch a single quota rule at an exact (tenant, project, metric) scope.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.metering.services.v1.GetQuotaResponse> getQuota(
        com.udb.core.metering.services.v1.GetQuotaRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetQuotaMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List a tenant's quota rules, optionally narrowed to a project.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.metering.services.v1.ListQuotasResponse> listQuotas(
        com.udb.core.metering.services.v1.ListQuotasRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListQuotasMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Check a quota: sum durable usage in the rule's window and compare against the
     * limit. Returns {allowed, used, limit, remaining}. The ingest hook remains
     * best-effort, but explicit quota checks fail closed when the durable aggregate
     * is unavailable, so an outage cannot silently bypass an enabled quota.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.metering.services.v1.CheckQuotaResponse> checkQuota(
        com.udb.core.metering.services.v1.CheckQuotaRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCheckQuotaMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_RECORD_USAGE = 0;
  private static final int METHODID_QUERY_USAGE = 1;
  private static final int METHODID_PUT_QUOTA = 2;
  private static final int METHODID_GET_QUOTA = 3;
  private static final int METHODID_LIST_QUOTAS = 4;
  private static final int METHODID_CHECK_QUOTA = 5;

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
        case METHODID_RECORD_USAGE:
          serviceImpl.recordUsage((com.udb.core.metering.services.v1.RecordUsageRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.RecordUsageResponse>) responseObserver);
          break;
        case METHODID_QUERY_USAGE:
          serviceImpl.queryUsage((com.udb.core.metering.services.v1.QueryUsageRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.QueryUsageResponse>) responseObserver);
          break;
        case METHODID_PUT_QUOTA:
          serviceImpl.putQuota((com.udb.core.metering.services.v1.PutQuotaRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.PutQuotaResponse>) responseObserver);
          break;
        case METHODID_GET_QUOTA:
          serviceImpl.getQuota((com.udb.core.metering.services.v1.GetQuotaRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.GetQuotaResponse>) responseObserver);
          break;
        case METHODID_LIST_QUOTAS:
          serviceImpl.listQuotas((com.udb.core.metering.services.v1.ListQuotasRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.ListQuotasResponse>) responseObserver);
          break;
        case METHODID_CHECK_QUOTA:
          serviceImpl.checkQuota((com.udb.core.metering.services.v1.CheckQuotaRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.metering.services.v1.CheckQuotaResponse>) responseObserver);
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
          getRecordUsageMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.metering.services.v1.RecordUsageRequest,
              com.udb.core.metering.services.v1.RecordUsageResponse>(
                service, METHODID_RECORD_USAGE)))
        .addMethod(
          getQueryUsageMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.metering.services.v1.QueryUsageRequest,
              com.udb.core.metering.services.v1.QueryUsageResponse>(
                service, METHODID_QUERY_USAGE)))
        .addMethod(
          getPutQuotaMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.metering.services.v1.PutQuotaRequest,
              com.udb.core.metering.services.v1.PutQuotaResponse>(
                service, METHODID_PUT_QUOTA)))
        .addMethod(
          getGetQuotaMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.metering.services.v1.GetQuotaRequest,
              com.udb.core.metering.services.v1.GetQuotaResponse>(
                service, METHODID_GET_QUOTA)))
        .addMethod(
          getListQuotasMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.metering.services.v1.ListQuotasRequest,
              com.udb.core.metering.services.v1.ListQuotasResponse>(
                service, METHODID_LIST_QUOTAS)))
        .addMethod(
          getCheckQuotaMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.metering.services.v1.CheckQuotaRequest,
              com.udb.core.metering.services.v1.CheckQuotaResponse>(
                service, METHODID_CHECK_QUOTA)))
        .build();
  }

  private static abstract class MeteringServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    MeteringServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.metering.services.v1.MeteringServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("MeteringService");
    }
  }

  private static final class MeteringServiceFileDescriptorSupplier
      extends MeteringServiceBaseDescriptorSupplier {
    MeteringServiceFileDescriptorSupplier() {}
  }

  private static final class MeteringServiceMethodDescriptorSupplier
      extends MeteringServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    MeteringServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (MeteringServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new MeteringServiceFileDescriptorSupplier())
              .addMethod(getRecordUsageMethod())
              .addMethod(getQueryUsageMethod())
              .addMethod(getPutQuotaMethod())
              .addMethod(getGetQuotaMethod())
              .addMethod(getListQuotasMethod())
              .addMethod(getCheckQuotaMethod())
              .build();
        }
      }
    }
    return result;
  }
}
