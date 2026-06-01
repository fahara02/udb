package com.udb.core.analytics.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class AnalyticsServiceGrpc {

  private AnalyticsServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.analytics.services.v1.AnalyticsService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.RecordPipelineMetricRequest,
      com.udb.core.analytics.services.v1.RecordPipelineMetricResponse> getRecordPipelineMetricMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RecordPipelineMetric",
      requestType = com.udb.core.analytics.services.v1.RecordPipelineMetricRequest.class,
      responseType = com.udb.core.analytics.services.v1.RecordPipelineMetricResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.RecordPipelineMetricRequest,
      com.udb.core.analytics.services.v1.RecordPipelineMetricResponse> getRecordPipelineMetricMethod() {
    io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.RecordPipelineMetricRequest, com.udb.core.analytics.services.v1.RecordPipelineMetricResponse> getRecordPipelineMetricMethod;
    if ((getRecordPipelineMetricMethod = AnalyticsServiceGrpc.getRecordPipelineMetricMethod) == null) {
      synchronized (AnalyticsServiceGrpc.class) {
        if ((getRecordPipelineMetricMethod = AnalyticsServiceGrpc.getRecordPipelineMetricMethod) == null) {
          AnalyticsServiceGrpc.getRecordPipelineMetricMethod = getRecordPipelineMetricMethod =
              io.grpc.MethodDescriptor.<com.udb.core.analytics.services.v1.RecordPipelineMetricRequest, com.udb.core.analytics.services.v1.RecordPipelineMetricResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RecordPipelineMetric"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.analytics.services.v1.RecordPipelineMetricRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.analytics.services.v1.RecordPipelineMetricResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AnalyticsServiceMethodDescriptorSupplier("RecordPipelineMetric"))
              .build();
        }
      }
    }
    return getRecordPipelineMetricMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.GetPipelineSummaryRequest,
      com.udb.core.analytics.services.v1.GetPipelineSummaryResponse> getGetPipelineSummaryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPipelineSummary",
      requestType = com.udb.core.analytics.services.v1.GetPipelineSummaryRequest.class,
      responseType = com.udb.core.analytics.services.v1.GetPipelineSummaryResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.GetPipelineSummaryRequest,
      com.udb.core.analytics.services.v1.GetPipelineSummaryResponse> getGetPipelineSummaryMethod() {
    io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.GetPipelineSummaryRequest, com.udb.core.analytics.services.v1.GetPipelineSummaryResponse> getGetPipelineSummaryMethod;
    if ((getGetPipelineSummaryMethod = AnalyticsServiceGrpc.getGetPipelineSummaryMethod) == null) {
      synchronized (AnalyticsServiceGrpc.class) {
        if ((getGetPipelineSummaryMethod = AnalyticsServiceGrpc.getGetPipelineSummaryMethod) == null) {
          AnalyticsServiceGrpc.getGetPipelineSummaryMethod = getGetPipelineSummaryMethod =
              io.grpc.MethodDescriptor.<com.udb.core.analytics.services.v1.GetPipelineSummaryRequest, com.udb.core.analytics.services.v1.GetPipelineSummaryResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetPipelineSummary"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.analytics.services.v1.GetPipelineSummaryRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.analytics.services.v1.GetPipelineSummaryResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AnalyticsServiceMethodDescriptorSupplier("GetPipelineSummary"))
              .build();
        }
      }
    }
    return getGetPipelineSummaryMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.GetExecutorPerformanceRequest,
      com.udb.core.analytics.services.v1.GetExecutorPerformanceResponse> getGetExecutorPerformanceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetExecutorPerformance",
      requestType = com.udb.core.analytics.services.v1.GetExecutorPerformanceRequest.class,
      responseType = com.udb.core.analytics.services.v1.GetExecutorPerformanceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.GetExecutorPerformanceRequest,
      com.udb.core.analytics.services.v1.GetExecutorPerformanceResponse> getGetExecutorPerformanceMethod() {
    io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.GetExecutorPerformanceRequest, com.udb.core.analytics.services.v1.GetExecutorPerformanceResponse> getGetExecutorPerformanceMethod;
    if ((getGetExecutorPerformanceMethod = AnalyticsServiceGrpc.getGetExecutorPerformanceMethod) == null) {
      synchronized (AnalyticsServiceGrpc.class) {
        if ((getGetExecutorPerformanceMethod = AnalyticsServiceGrpc.getGetExecutorPerformanceMethod) == null) {
          AnalyticsServiceGrpc.getGetExecutorPerformanceMethod = getGetExecutorPerformanceMethod =
              io.grpc.MethodDescriptor.<com.udb.core.analytics.services.v1.GetExecutorPerformanceRequest, com.udb.core.analytics.services.v1.GetExecutorPerformanceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetExecutorPerformance"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.analytics.services.v1.GetExecutorPerformanceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.analytics.services.v1.GetExecutorPerformanceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AnalyticsServiceMethodDescriptorSupplier("GetExecutorPerformance"))
              .build();
        }
      }
    }
    return getGetExecutorPerformanceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.GetReconciliationAnalyticsRequest,
      com.udb.core.analytics.services.v1.GetReconciliationAnalyticsResponse> getGetReconciliationAnalyticsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetReconciliationAnalytics",
      requestType = com.udb.core.analytics.services.v1.GetReconciliationAnalyticsRequest.class,
      responseType = com.udb.core.analytics.services.v1.GetReconciliationAnalyticsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.GetReconciliationAnalyticsRequest,
      com.udb.core.analytics.services.v1.GetReconciliationAnalyticsResponse> getGetReconciliationAnalyticsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.GetReconciliationAnalyticsRequest, com.udb.core.analytics.services.v1.GetReconciliationAnalyticsResponse> getGetReconciliationAnalyticsMethod;
    if ((getGetReconciliationAnalyticsMethod = AnalyticsServiceGrpc.getGetReconciliationAnalyticsMethod) == null) {
      synchronized (AnalyticsServiceGrpc.class) {
        if ((getGetReconciliationAnalyticsMethod = AnalyticsServiceGrpc.getGetReconciliationAnalyticsMethod) == null) {
          AnalyticsServiceGrpc.getGetReconciliationAnalyticsMethod = getGetReconciliationAnalyticsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.analytics.services.v1.GetReconciliationAnalyticsRequest, com.udb.core.analytics.services.v1.GetReconciliationAnalyticsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetReconciliationAnalytics"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.analytics.services.v1.GetReconciliationAnalyticsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.analytics.services.v1.GetReconciliationAnalyticsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AnalyticsServiceMethodDescriptorSupplier("GetReconciliationAnalytics"))
              .build();
        }
      }
    }
    return getGetReconciliationAnalyticsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.GetThroughputRequest,
      com.udb.core.analytics.services.v1.GetThroughputResponse> getGetThroughputMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetThroughput",
      requestType = com.udb.core.analytics.services.v1.GetThroughputRequest.class,
      responseType = com.udb.core.analytics.services.v1.GetThroughputResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.GetThroughputRequest,
      com.udb.core.analytics.services.v1.GetThroughputResponse> getGetThroughputMethod() {
    io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.GetThroughputRequest, com.udb.core.analytics.services.v1.GetThroughputResponse> getGetThroughputMethod;
    if ((getGetThroughputMethod = AnalyticsServiceGrpc.getGetThroughputMethod) == null) {
      synchronized (AnalyticsServiceGrpc.class) {
        if ((getGetThroughputMethod = AnalyticsServiceGrpc.getGetThroughputMethod) == null) {
          AnalyticsServiceGrpc.getGetThroughputMethod = getGetThroughputMethod =
              io.grpc.MethodDescriptor.<com.udb.core.analytics.services.v1.GetThroughputRequest, com.udb.core.analytics.services.v1.GetThroughputResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetThroughput"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.analytics.services.v1.GetThroughputRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.analytics.services.v1.GetThroughputResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AnalyticsServiceMethodDescriptorSupplier("GetThroughput"))
              .build();
        }
      }
    }
    return getGetThroughputMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.GetSlaComplianceRequest,
      com.udb.core.analytics.services.v1.GetSlaComplianceResponse> getGetSlaComplianceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetSlaCompliance",
      requestType = com.udb.core.analytics.services.v1.GetSlaComplianceRequest.class,
      responseType = com.udb.core.analytics.services.v1.GetSlaComplianceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.GetSlaComplianceRequest,
      com.udb.core.analytics.services.v1.GetSlaComplianceResponse> getGetSlaComplianceMethod() {
    io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.GetSlaComplianceRequest, com.udb.core.analytics.services.v1.GetSlaComplianceResponse> getGetSlaComplianceMethod;
    if ((getGetSlaComplianceMethod = AnalyticsServiceGrpc.getGetSlaComplianceMethod) == null) {
      synchronized (AnalyticsServiceGrpc.class) {
        if ((getGetSlaComplianceMethod = AnalyticsServiceGrpc.getGetSlaComplianceMethod) == null) {
          AnalyticsServiceGrpc.getGetSlaComplianceMethod = getGetSlaComplianceMethod =
              io.grpc.MethodDescriptor.<com.udb.core.analytics.services.v1.GetSlaComplianceRequest, com.udb.core.analytics.services.v1.GetSlaComplianceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetSlaCompliance"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.analytics.services.v1.GetSlaComplianceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.analytics.services.v1.GetSlaComplianceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AnalyticsServiceMethodDescriptorSupplier("GetSlaCompliance"))
              .build();
        }
      }
    }
    return getGetSlaComplianceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.TriggerSnapshotRequest,
      com.udb.core.analytics.services.v1.TriggerSnapshotResponse> getTriggerSnapshotMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TriggerSnapshot",
      requestType = com.udb.core.analytics.services.v1.TriggerSnapshotRequest.class,
      responseType = com.udb.core.analytics.services.v1.TriggerSnapshotResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.TriggerSnapshotRequest,
      com.udb.core.analytics.services.v1.TriggerSnapshotResponse> getTriggerSnapshotMethod() {
    io.grpc.MethodDescriptor<com.udb.core.analytics.services.v1.TriggerSnapshotRequest, com.udb.core.analytics.services.v1.TriggerSnapshotResponse> getTriggerSnapshotMethod;
    if ((getTriggerSnapshotMethod = AnalyticsServiceGrpc.getTriggerSnapshotMethod) == null) {
      synchronized (AnalyticsServiceGrpc.class) {
        if ((getTriggerSnapshotMethod = AnalyticsServiceGrpc.getTriggerSnapshotMethod) == null) {
          AnalyticsServiceGrpc.getTriggerSnapshotMethod = getTriggerSnapshotMethod =
              io.grpc.MethodDescriptor.<com.udb.core.analytics.services.v1.TriggerSnapshotRequest, com.udb.core.analytics.services.v1.TriggerSnapshotResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TriggerSnapshot"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.analytics.services.v1.TriggerSnapshotRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.analytics.services.v1.TriggerSnapshotResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AnalyticsServiceMethodDescriptorSupplier("TriggerSnapshot"))
              .build();
        }
      }
    }
    return getTriggerSnapshotMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static AnalyticsServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AnalyticsServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AnalyticsServiceStub>() {
        @java.lang.Override
        public AnalyticsServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AnalyticsServiceStub(channel, callOptions);
        }
      };
    return AnalyticsServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static AnalyticsServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AnalyticsServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AnalyticsServiceBlockingV2Stub>() {
        @java.lang.Override
        public AnalyticsServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AnalyticsServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return AnalyticsServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static AnalyticsServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AnalyticsServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AnalyticsServiceBlockingStub>() {
        @java.lang.Override
        public AnalyticsServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AnalyticsServiceBlockingStub(channel, callOptions);
        }
      };
    return AnalyticsServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static AnalyticsServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AnalyticsServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AnalyticsServiceFutureStub>() {
        @java.lang.Override
        public AnalyticsServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AnalyticsServiceFutureStub(channel, callOptions);
        }
      };
    return AnalyticsServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     * <pre>
     * Record a single pipeline stage request observation (called per-request).
     * </pre>
     */
    default void recordPipelineMetric(com.udb.core.analytics.services.v1.RecordPipelineMetricRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.RecordPipelineMetricResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRecordPipelineMetricMethod(), responseObserver);
    }

    /**
     * <pre>
     * Query aggregated pipeline stage performance snapshots.
     * </pre>
     */
    default void getPipelineSummary(com.udb.core.analytics.services.v1.GetPipelineSummaryRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.GetPipelineSummaryResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetPipelineSummaryMethod(), responseObserver);
    }

    /**
     * <pre>
     * Query daily executor performance roll-ups.
     * </pre>
     */
    default void getExecutorPerformance(com.udb.core.analytics.services.v1.GetExecutorPerformanceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.GetExecutorPerformanceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetExecutorPerformanceMethod(), responseObserver);
    }

    /**
     * <pre>
     * Query daily reconciliation and conflict analytics.
     * </pre>
     */
    default void getReconciliationAnalytics(com.udb.core.analytics.services.v1.GetReconciliationAnalyticsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.GetReconciliationAnalyticsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetReconciliationAnalyticsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get throughput statistics over a time window.
     * </pre>
     */
    default void getThroughput(com.udb.core.analytics.services.v1.GetThroughputRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.GetThroughputResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetThroughputMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get SLA compliance report for a stage and time period.
     * </pre>
     */
    default void getSlaCompliance(com.udb.core.analytics.services.v1.GetSlaComplianceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.GetSlaComplianceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetSlaComplianceMethod(), responseObserver);
    }

    /**
     * <pre>
     * Manually trigger hourly snapshot aggregation (normally a cron job).
     * </pre>
     */
    default void triggerSnapshot(com.udb.core.analytics.services.v1.TriggerSnapshotRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.TriggerSnapshotResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTriggerSnapshotMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service AnalyticsService.
   */
  public static abstract class AnalyticsServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return AnalyticsServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service AnalyticsService.
   */
  public static final class AnalyticsServiceStub
      extends io.grpc.stub.AbstractAsyncStub<AnalyticsServiceStub> {
    private AnalyticsServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AnalyticsServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AnalyticsServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Record a single pipeline stage request observation (called per-request).
     * </pre>
     */
    public void recordPipelineMetric(com.udb.core.analytics.services.v1.RecordPipelineMetricRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.RecordPipelineMetricResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRecordPipelineMetricMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Query aggregated pipeline stage performance snapshots.
     * </pre>
     */
    public void getPipelineSummary(com.udb.core.analytics.services.v1.GetPipelineSummaryRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.GetPipelineSummaryResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetPipelineSummaryMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Query daily executor performance roll-ups.
     * </pre>
     */
    public void getExecutorPerformance(com.udb.core.analytics.services.v1.GetExecutorPerformanceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.GetExecutorPerformanceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetExecutorPerformanceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Query daily reconciliation and conflict analytics.
     * </pre>
     */
    public void getReconciliationAnalytics(com.udb.core.analytics.services.v1.GetReconciliationAnalyticsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.GetReconciliationAnalyticsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetReconciliationAnalyticsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get throughput statistics over a time window.
     * </pre>
     */
    public void getThroughput(com.udb.core.analytics.services.v1.GetThroughputRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.GetThroughputResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetThroughputMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get SLA compliance report for a stage and time period.
     * </pre>
     */
    public void getSlaCompliance(com.udb.core.analytics.services.v1.GetSlaComplianceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.GetSlaComplianceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetSlaComplianceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Manually trigger hourly snapshot aggregation (normally a cron job).
     * </pre>
     */
    public void triggerSnapshot(com.udb.core.analytics.services.v1.TriggerSnapshotRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.TriggerSnapshotResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTriggerSnapshotMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service AnalyticsService.
   */
  public static final class AnalyticsServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<AnalyticsServiceBlockingV2Stub> {
    private AnalyticsServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AnalyticsServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AnalyticsServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Record a single pipeline stage request observation (called per-request).
     * </pre>
     */
    public com.udb.core.analytics.services.v1.RecordPipelineMetricResponse recordPipelineMetric(com.udb.core.analytics.services.v1.RecordPipelineMetricRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRecordPipelineMetricMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Query aggregated pipeline stage performance snapshots.
     * </pre>
     */
    public com.udb.core.analytics.services.v1.GetPipelineSummaryResponse getPipelineSummary(com.udb.core.analytics.services.v1.GetPipelineSummaryRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetPipelineSummaryMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Query daily executor performance roll-ups.
     * </pre>
     */
    public com.udb.core.analytics.services.v1.GetExecutorPerformanceResponse getExecutorPerformance(com.udb.core.analytics.services.v1.GetExecutorPerformanceRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetExecutorPerformanceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Query daily reconciliation and conflict analytics.
     * </pre>
     */
    public com.udb.core.analytics.services.v1.GetReconciliationAnalyticsResponse getReconciliationAnalytics(com.udb.core.analytics.services.v1.GetReconciliationAnalyticsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetReconciliationAnalyticsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get throughput statistics over a time window.
     * </pre>
     */
    public com.udb.core.analytics.services.v1.GetThroughputResponse getThroughput(com.udb.core.analytics.services.v1.GetThroughputRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetThroughputMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get SLA compliance report for a stage and time period.
     * </pre>
     */
    public com.udb.core.analytics.services.v1.GetSlaComplianceResponse getSlaCompliance(com.udb.core.analytics.services.v1.GetSlaComplianceRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetSlaComplianceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Manually trigger hourly snapshot aggregation (normally a cron job).
     * </pre>
     */
    public com.udb.core.analytics.services.v1.TriggerSnapshotResponse triggerSnapshot(com.udb.core.analytics.services.v1.TriggerSnapshotRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getTriggerSnapshotMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service AnalyticsService.
   */
  public static final class AnalyticsServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<AnalyticsServiceBlockingStub> {
    private AnalyticsServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AnalyticsServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AnalyticsServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Record a single pipeline stage request observation (called per-request).
     * </pre>
     */
    public com.udb.core.analytics.services.v1.RecordPipelineMetricResponse recordPipelineMetric(com.udb.core.analytics.services.v1.RecordPipelineMetricRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRecordPipelineMetricMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Query aggregated pipeline stage performance snapshots.
     * </pre>
     */
    public com.udb.core.analytics.services.v1.GetPipelineSummaryResponse getPipelineSummary(com.udb.core.analytics.services.v1.GetPipelineSummaryRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetPipelineSummaryMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Query daily executor performance roll-ups.
     * </pre>
     */
    public com.udb.core.analytics.services.v1.GetExecutorPerformanceResponse getExecutorPerformance(com.udb.core.analytics.services.v1.GetExecutorPerformanceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetExecutorPerformanceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Query daily reconciliation and conflict analytics.
     * </pre>
     */
    public com.udb.core.analytics.services.v1.GetReconciliationAnalyticsResponse getReconciliationAnalytics(com.udb.core.analytics.services.v1.GetReconciliationAnalyticsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetReconciliationAnalyticsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get throughput statistics over a time window.
     * </pre>
     */
    public com.udb.core.analytics.services.v1.GetThroughputResponse getThroughput(com.udb.core.analytics.services.v1.GetThroughputRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetThroughputMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get SLA compliance report for a stage and time period.
     * </pre>
     */
    public com.udb.core.analytics.services.v1.GetSlaComplianceResponse getSlaCompliance(com.udb.core.analytics.services.v1.GetSlaComplianceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetSlaComplianceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Manually trigger hourly snapshot aggregation (normally a cron job).
     * </pre>
     */
    public com.udb.core.analytics.services.v1.TriggerSnapshotResponse triggerSnapshot(com.udb.core.analytics.services.v1.TriggerSnapshotRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTriggerSnapshotMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service AnalyticsService.
   */
  public static final class AnalyticsServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<AnalyticsServiceFutureStub> {
    private AnalyticsServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AnalyticsServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AnalyticsServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Record a single pipeline stage request observation (called per-request).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.analytics.services.v1.RecordPipelineMetricResponse> recordPipelineMetric(
        com.udb.core.analytics.services.v1.RecordPipelineMetricRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRecordPipelineMetricMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Query aggregated pipeline stage performance snapshots.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.analytics.services.v1.GetPipelineSummaryResponse> getPipelineSummary(
        com.udb.core.analytics.services.v1.GetPipelineSummaryRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetPipelineSummaryMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Query daily executor performance roll-ups.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.analytics.services.v1.GetExecutorPerformanceResponse> getExecutorPerformance(
        com.udb.core.analytics.services.v1.GetExecutorPerformanceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetExecutorPerformanceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Query daily reconciliation and conflict analytics.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.analytics.services.v1.GetReconciliationAnalyticsResponse> getReconciliationAnalytics(
        com.udb.core.analytics.services.v1.GetReconciliationAnalyticsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetReconciliationAnalyticsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get throughput statistics over a time window.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.analytics.services.v1.GetThroughputResponse> getThroughput(
        com.udb.core.analytics.services.v1.GetThroughputRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetThroughputMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get SLA compliance report for a stage and time period.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.analytics.services.v1.GetSlaComplianceResponse> getSlaCompliance(
        com.udb.core.analytics.services.v1.GetSlaComplianceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetSlaComplianceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Manually trigger hourly snapshot aggregation (normally a cron job).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.analytics.services.v1.TriggerSnapshotResponse> triggerSnapshot(
        com.udb.core.analytics.services.v1.TriggerSnapshotRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTriggerSnapshotMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_RECORD_PIPELINE_METRIC = 0;
  private static final int METHODID_GET_PIPELINE_SUMMARY = 1;
  private static final int METHODID_GET_EXECUTOR_PERFORMANCE = 2;
  private static final int METHODID_GET_RECONCILIATION_ANALYTICS = 3;
  private static final int METHODID_GET_THROUGHPUT = 4;
  private static final int METHODID_GET_SLA_COMPLIANCE = 5;
  private static final int METHODID_TRIGGER_SNAPSHOT = 6;

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
        case METHODID_RECORD_PIPELINE_METRIC:
          serviceImpl.recordPipelineMetric((com.udb.core.analytics.services.v1.RecordPipelineMetricRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.RecordPipelineMetricResponse>) responseObserver);
          break;
        case METHODID_GET_PIPELINE_SUMMARY:
          serviceImpl.getPipelineSummary((com.udb.core.analytics.services.v1.GetPipelineSummaryRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.GetPipelineSummaryResponse>) responseObserver);
          break;
        case METHODID_GET_EXECUTOR_PERFORMANCE:
          serviceImpl.getExecutorPerformance((com.udb.core.analytics.services.v1.GetExecutorPerformanceRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.GetExecutorPerformanceResponse>) responseObserver);
          break;
        case METHODID_GET_RECONCILIATION_ANALYTICS:
          serviceImpl.getReconciliationAnalytics((com.udb.core.analytics.services.v1.GetReconciliationAnalyticsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.GetReconciliationAnalyticsResponse>) responseObserver);
          break;
        case METHODID_GET_THROUGHPUT:
          serviceImpl.getThroughput((com.udb.core.analytics.services.v1.GetThroughputRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.GetThroughputResponse>) responseObserver);
          break;
        case METHODID_GET_SLA_COMPLIANCE:
          serviceImpl.getSlaCompliance((com.udb.core.analytics.services.v1.GetSlaComplianceRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.GetSlaComplianceResponse>) responseObserver);
          break;
        case METHODID_TRIGGER_SNAPSHOT:
          serviceImpl.triggerSnapshot((com.udb.core.analytics.services.v1.TriggerSnapshotRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.analytics.services.v1.TriggerSnapshotResponse>) responseObserver);
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
          getRecordPipelineMetricMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.analytics.services.v1.RecordPipelineMetricRequest,
              com.udb.core.analytics.services.v1.RecordPipelineMetricResponse>(
                service, METHODID_RECORD_PIPELINE_METRIC)))
        .addMethod(
          getGetPipelineSummaryMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.analytics.services.v1.GetPipelineSummaryRequest,
              com.udb.core.analytics.services.v1.GetPipelineSummaryResponse>(
                service, METHODID_GET_PIPELINE_SUMMARY)))
        .addMethod(
          getGetExecutorPerformanceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.analytics.services.v1.GetExecutorPerformanceRequest,
              com.udb.core.analytics.services.v1.GetExecutorPerformanceResponse>(
                service, METHODID_GET_EXECUTOR_PERFORMANCE)))
        .addMethod(
          getGetReconciliationAnalyticsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.analytics.services.v1.GetReconciliationAnalyticsRequest,
              com.udb.core.analytics.services.v1.GetReconciliationAnalyticsResponse>(
                service, METHODID_GET_RECONCILIATION_ANALYTICS)))
        .addMethod(
          getGetThroughputMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.analytics.services.v1.GetThroughputRequest,
              com.udb.core.analytics.services.v1.GetThroughputResponse>(
                service, METHODID_GET_THROUGHPUT)))
        .addMethod(
          getGetSlaComplianceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.analytics.services.v1.GetSlaComplianceRequest,
              com.udb.core.analytics.services.v1.GetSlaComplianceResponse>(
                service, METHODID_GET_SLA_COMPLIANCE)))
        .addMethod(
          getTriggerSnapshotMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.analytics.services.v1.TriggerSnapshotRequest,
              com.udb.core.analytics.services.v1.TriggerSnapshotResponse>(
                service, METHODID_TRIGGER_SNAPSHOT)))
        .build();
  }

  private static abstract class AnalyticsServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    AnalyticsServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.analytics.services.v1.AnalyticsServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("AnalyticsService");
    }
  }

  private static final class AnalyticsServiceFileDescriptorSupplier
      extends AnalyticsServiceBaseDescriptorSupplier {
    AnalyticsServiceFileDescriptorSupplier() {}
  }

  private static final class AnalyticsServiceMethodDescriptorSupplier
      extends AnalyticsServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    AnalyticsServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (AnalyticsServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new AnalyticsServiceFileDescriptorSupplier())
              .addMethod(getRecordPipelineMetricMethod())
              .addMethod(getGetPipelineSummaryMethod())
              .addMethod(getGetExecutorPerformanceMethod())
              .addMethod(getGetReconciliationAnalyticsMethod())
              .addMethod(getGetThroughputMethod())
              .addMethod(getGetSlaComplianceMethod())
              .addMethod(getTriggerSnapshotMethod())
              .build();
        }
      }
    }
    return result;
  }
}
