package com.udb.core.workflow.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * WorkflowService (master-plan 9.12) — durable multi-step operations with
 * compensation, exposed as a first-class native service. A workflow is a durable,
 * tenant-scoped instance handed to the EXISTING saga engine (`runtime::saga`):
 * forward progress is driven by the leader-elected workflow tick
 * (`FOR UPDATE SKIP LOCKED`, one advancer cluster-wide, fires transition events
 * only), and cancellation reuses the established saga compensation (reverse-order)
 * machinery rather than reimplementing it. Every state transition emits one
 * versioned `udb.workflow.&lt;state&gt;.v1` outbox event.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class WorkflowServiceGrpc {

  private WorkflowServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.workflow.services.v1.WorkflowService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.workflow.services.v1.StartWorkflowRequest,
      com.udb.core.workflow.services.v1.StartWorkflowResponse> getStartWorkflowMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StartWorkflow",
      requestType = com.udb.core.workflow.services.v1.StartWorkflowRequest.class,
      responseType = com.udb.core.workflow.services.v1.StartWorkflowResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.workflow.services.v1.StartWorkflowRequest,
      com.udb.core.workflow.services.v1.StartWorkflowResponse> getStartWorkflowMethod() {
    io.grpc.MethodDescriptor<com.udb.core.workflow.services.v1.StartWorkflowRequest, com.udb.core.workflow.services.v1.StartWorkflowResponse> getStartWorkflowMethod;
    if ((getStartWorkflowMethod = WorkflowServiceGrpc.getStartWorkflowMethod) == null) {
      synchronized (WorkflowServiceGrpc.class) {
        if ((getStartWorkflowMethod = WorkflowServiceGrpc.getStartWorkflowMethod) == null) {
          WorkflowServiceGrpc.getStartWorkflowMethod = getStartWorkflowMethod =
              io.grpc.MethodDescriptor.<com.udb.core.workflow.services.v1.StartWorkflowRequest, com.udb.core.workflow.services.v1.StartWorkflowResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StartWorkflow"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.workflow.services.v1.StartWorkflowRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.workflow.services.v1.StartWorkflowResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WorkflowServiceMethodDescriptorSupplier("StartWorkflow"))
              .build();
        }
      }
    }
    return getStartWorkflowMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.workflow.services.v1.GetWorkflowRequest,
      com.udb.core.workflow.services.v1.GetWorkflowResponse> getGetWorkflowMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetWorkflow",
      requestType = com.udb.core.workflow.services.v1.GetWorkflowRequest.class,
      responseType = com.udb.core.workflow.services.v1.GetWorkflowResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.workflow.services.v1.GetWorkflowRequest,
      com.udb.core.workflow.services.v1.GetWorkflowResponse> getGetWorkflowMethod() {
    io.grpc.MethodDescriptor<com.udb.core.workflow.services.v1.GetWorkflowRequest, com.udb.core.workflow.services.v1.GetWorkflowResponse> getGetWorkflowMethod;
    if ((getGetWorkflowMethod = WorkflowServiceGrpc.getGetWorkflowMethod) == null) {
      synchronized (WorkflowServiceGrpc.class) {
        if ((getGetWorkflowMethod = WorkflowServiceGrpc.getGetWorkflowMethod) == null) {
          WorkflowServiceGrpc.getGetWorkflowMethod = getGetWorkflowMethod =
              io.grpc.MethodDescriptor.<com.udb.core.workflow.services.v1.GetWorkflowRequest, com.udb.core.workflow.services.v1.GetWorkflowResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetWorkflow"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.workflow.services.v1.GetWorkflowRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.workflow.services.v1.GetWorkflowResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WorkflowServiceMethodDescriptorSupplier("GetWorkflow"))
              .build();
        }
      }
    }
    return getGetWorkflowMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.workflow.services.v1.ListWorkflowsRequest,
      com.udb.core.workflow.services.v1.ListWorkflowsResponse> getListWorkflowsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListWorkflows",
      requestType = com.udb.core.workflow.services.v1.ListWorkflowsRequest.class,
      responseType = com.udb.core.workflow.services.v1.ListWorkflowsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.workflow.services.v1.ListWorkflowsRequest,
      com.udb.core.workflow.services.v1.ListWorkflowsResponse> getListWorkflowsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.workflow.services.v1.ListWorkflowsRequest, com.udb.core.workflow.services.v1.ListWorkflowsResponse> getListWorkflowsMethod;
    if ((getListWorkflowsMethod = WorkflowServiceGrpc.getListWorkflowsMethod) == null) {
      synchronized (WorkflowServiceGrpc.class) {
        if ((getListWorkflowsMethod = WorkflowServiceGrpc.getListWorkflowsMethod) == null) {
          WorkflowServiceGrpc.getListWorkflowsMethod = getListWorkflowsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.workflow.services.v1.ListWorkflowsRequest, com.udb.core.workflow.services.v1.ListWorkflowsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListWorkflows"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.workflow.services.v1.ListWorkflowsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.workflow.services.v1.ListWorkflowsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WorkflowServiceMethodDescriptorSupplier("ListWorkflows"))
              .build();
        }
      }
    }
    return getListWorkflowsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.workflow.services.v1.CancelWorkflowRequest,
      com.udb.core.workflow.services.v1.CancelWorkflowResponse> getCancelWorkflowMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CancelWorkflow",
      requestType = com.udb.core.workflow.services.v1.CancelWorkflowRequest.class,
      responseType = com.udb.core.workflow.services.v1.CancelWorkflowResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.workflow.services.v1.CancelWorkflowRequest,
      com.udb.core.workflow.services.v1.CancelWorkflowResponse> getCancelWorkflowMethod() {
    io.grpc.MethodDescriptor<com.udb.core.workflow.services.v1.CancelWorkflowRequest, com.udb.core.workflow.services.v1.CancelWorkflowResponse> getCancelWorkflowMethod;
    if ((getCancelWorkflowMethod = WorkflowServiceGrpc.getCancelWorkflowMethod) == null) {
      synchronized (WorkflowServiceGrpc.class) {
        if ((getCancelWorkflowMethod = WorkflowServiceGrpc.getCancelWorkflowMethod) == null) {
          WorkflowServiceGrpc.getCancelWorkflowMethod = getCancelWorkflowMethod =
              io.grpc.MethodDescriptor.<com.udb.core.workflow.services.v1.CancelWorkflowRequest, com.udb.core.workflow.services.v1.CancelWorkflowResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CancelWorkflow"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.workflow.services.v1.CancelWorkflowRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.workflow.services.v1.CancelWorkflowResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WorkflowServiceMethodDescriptorSupplier("CancelWorkflow"))
              .build();
        }
      }
    }
    return getCancelWorkflowMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.workflow.services.v1.SignalWorkflowRequest,
      com.udb.core.workflow.services.v1.SignalWorkflowResponse> getSignalWorkflowMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SignalWorkflow",
      requestType = com.udb.core.workflow.services.v1.SignalWorkflowRequest.class,
      responseType = com.udb.core.workflow.services.v1.SignalWorkflowResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.workflow.services.v1.SignalWorkflowRequest,
      com.udb.core.workflow.services.v1.SignalWorkflowResponse> getSignalWorkflowMethod() {
    io.grpc.MethodDescriptor<com.udb.core.workflow.services.v1.SignalWorkflowRequest, com.udb.core.workflow.services.v1.SignalWorkflowResponse> getSignalWorkflowMethod;
    if ((getSignalWorkflowMethod = WorkflowServiceGrpc.getSignalWorkflowMethod) == null) {
      synchronized (WorkflowServiceGrpc.class) {
        if ((getSignalWorkflowMethod = WorkflowServiceGrpc.getSignalWorkflowMethod) == null) {
          WorkflowServiceGrpc.getSignalWorkflowMethod = getSignalWorkflowMethod =
              io.grpc.MethodDescriptor.<com.udb.core.workflow.services.v1.SignalWorkflowRequest, com.udb.core.workflow.services.v1.SignalWorkflowResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SignalWorkflow"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.workflow.services.v1.SignalWorkflowRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.workflow.services.v1.SignalWorkflowResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WorkflowServiceMethodDescriptorSupplier("SignalWorkflow"))
              .build();
        }
      }
    }
    return getSignalWorkflowMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static WorkflowServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WorkflowServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WorkflowServiceStub>() {
        @java.lang.Override
        public WorkflowServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WorkflowServiceStub(channel, callOptions);
        }
      };
    return WorkflowServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static WorkflowServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WorkflowServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WorkflowServiceBlockingV2Stub>() {
        @java.lang.Override
        public WorkflowServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WorkflowServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return WorkflowServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static WorkflowServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WorkflowServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WorkflowServiceBlockingStub>() {
        @java.lang.Override
        public WorkflowServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WorkflowServiceBlockingStub(channel, callOptions);
        }
      };
    return WorkflowServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static WorkflowServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WorkflowServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WorkflowServiceFutureStub>() {
        @java.lang.Override
        public WorkflowServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WorkflowServiceFutureStub(channel, callOptions);
        }
      };
    return WorkflowServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * WorkflowService (master-plan 9.12) — durable multi-step operations with
   * compensation, exposed as a first-class native service. A workflow is a durable,
   * tenant-scoped instance handed to the EXISTING saga engine (`runtime::saga`):
   * forward progress is driven by the leader-elected workflow tick
   * (`FOR UPDATE SKIP LOCKED`, one advancer cluster-wide, fires transition events
   * only), and cancellation reuses the established saga compensation (reverse-order)
   * machinery rather than reimplementing it. Every state transition emits one
   * versioned `udb.workflow.&lt;state&gt;.v1` outbox event.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Start a durable workflow instance and hand it to the saga engine. The instance
     * is persisted before any forward step runs, so it survives a restart.
     * </pre>
     */
    default void startWorkflow(com.udb.core.workflow.services.v1.StartWorkflowRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.workflow.services.v1.StartWorkflowResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStartWorkflowMethod(), responseObserver);
    }

    /**
     * <pre>
     * Fetch a single workflow instance by id (tenant-scoped).
     * </pre>
     */
    default void getWorkflow(com.udb.core.workflow.services.v1.GetWorkflowRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.workflow.services.v1.GetWorkflowResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetWorkflowMethod(), responseObserver);
    }

    /**
     * <pre>
     * List workflow instances for the verified tenant, optionally filtered by status.
     * </pre>
     */
    default void listWorkflows(com.udb.core.workflow.services.v1.ListWorkflowsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.workflow.services.v1.ListWorkflowsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListWorkflowsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Cancel a workflow and trigger the saga compensation path (reverse-order). The
     * instance moves to COMPENSATING and the EXISTING recovery worker undoes the
     * recorded side effects — this RPC never reimplements compensation.
     * </pre>
     */
    default void cancelWorkflow(com.udb.core.workflow.services.v1.CancelWorkflowRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.workflow.services.v1.CancelWorkflowResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCancelWorkflowMethod(), responseObserver);
    }

    /**
     * <pre>
     * Deliver an external signal to a waiting workflow step, resuming forward
     * progress (the durable equivalent of completing a blocked step).
     * </pre>
     */
    default void signalWorkflow(com.udb.core.workflow.services.v1.SignalWorkflowRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.workflow.services.v1.SignalWorkflowResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSignalWorkflowMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service WorkflowService.
   * <pre>
   * WorkflowService (master-plan 9.12) — durable multi-step operations with
   * compensation, exposed as a first-class native service. A workflow is a durable,
   * tenant-scoped instance handed to the EXISTING saga engine (`runtime::saga`):
   * forward progress is driven by the leader-elected workflow tick
   * (`FOR UPDATE SKIP LOCKED`, one advancer cluster-wide, fires transition events
   * only), and cancellation reuses the established saga compensation (reverse-order)
   * machinery rather than reimplementing it. Every state transition emits one
   * versioned `udb.workflow.&lt;state&gt;.v1` outbox event.
   * </pre>
   */
  public static abstract class WorkflowServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return WorkflowServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service WorkflowService.
   * <pre>
   * WorkflowService (master-plan 9.12) — durable multi-step operations with
   * compensation, exposed as a first-class native service. A workflow is a durable,
   * tenant-scoped instance handed to the EXISTING saga engine (`runtime::saga`):
   * forward progress is driven by the leader-elected workflow tick
   * (`FOR UPDATE SKIP LOCKED`, one advancer cluster-wide, fires transition events
   * only), and cancellation reuses the established saga compensation (reverse-order)
   * machinery rather than reimplementing it. Every state transition emits one
   * versioned `udb.workflow.&lt;state&gt;.v1` outbox event.
   * </pre>
   */
  public static final class WorkflowServiceStub
      extends io.grpc.stub.AbstractAsyncStub<WorkflowServiceStub> {
    private WorkflowServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WorkflowServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WorkflowServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Start a durable workflow instance and hand it to the saga engine. The instance
     * is persisted before any forward step runs, so it survives a restart.
     * </pre>
     */
    public void startWorkflow(com.udb.core.workflow.services.v1.StartWorkflowRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.workflow.services.v1.StartWorkflowResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStartWorkflowMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Fetch a single workflow instance by id (tenant-scoped).
     * </pre>
     */
    public void getWorkflow(com.udb.core.workflow.services.v1.GetWorkflowRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.workflow.services.v1.GetWorkflowResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetWorkflowMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List workflow instances for the verified tenant, optionally filtered by status.
     * </pre>
     */
    public void listWorkflows(com.udb.core.workflow.services.v1.ListWorkflowsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.workflow.services.v1.ListWorkflowsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListWorkflowsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Cancel a workflow and trigger the saga compensation path (reverse-order). The
     * instance moves to COMPENSATING and the EXISTING recovery worker undoes the
     * recorded side effects — this RPC never reimplements compensation.
     * </pre>
     */
    public void cancelWorkflow(com.udb.core.workflow.services.v1.CancelWorkflowRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.workflow.services.v1.CancelWorkflowResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCancelWorkflowMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Deliver an external signal to a waiting workflow step, resuming forward
     * progress (the durable equivalent of completing a blocked step).
     * </pre>
     */
    public void signalWorkflow(com.udb.core.workflow.services.v1.SignalWorkflowRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.workflow.services.v1.SignalWorkflowResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSignalWorkflowMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service WorkflowService.
   * <pre>
   * WorkflowService (master-plan 9.12) — durable multi-step operations with
   * compensation, exposed as a first-class native service. A workflow is a durable,
   * tenant-scoped instance handed to the EXISTING saga engine (`runtime::saga`):
   * forward progress is driven by the leader-elected workflow tick
   * (`FOR UPDATE SKIP LOCKED`, one advancer cluster-wide, fires transition events
   * only), and cancellation reuses the established saga compensation (reverse-order)
   * machinery rather than reimplementing it. Every state transition emits one
   * versioned `udb.workflow.&lt;state&gt;.v1` outbox event.
   * </pre>
   */
  public static final class WorkflowServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<WorkflowServiceBlockingV2Stub> {
    private WorkflowServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WorkflowServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WorkflowServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Start a durable workflow instance and hand it to the saga engine. The instance
     * is persisted before any forward step runs, so it survives a restart.
     * </pre>
     */
    public com.udb.core.workflow.services.v1.StartWorkflowResponse startWorkflow(com.udb.core.workflow.services.v1.StartWorkflowRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getStartWorkflowMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch a single workflow instance by id (tenant-scoped).
     * </pre>
     */
    public com.udb.core.workflow.services.v1.GetWorkflowResponse getWorkflow(com.udb.core.workflow.services.v1.GetWorkflowRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetWorkflowMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List workflow instances for the verified tenant, optionally filtered by status.
     * </pre>
     */
    public com.udb.core.workflow.services.v1.ListWorkflowsResponse listWorkflows(com.udb.core.workflow.services.v1.ListWorkflowsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListWorkflowsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Cancel a workflow and trigger the saga compensation path (reverse-order). The
     * instance moves to COMPENSATING and the EXISTING recovery worker undoes the
     * recorded side effects — this RPC never reimplements compensation.
     * </pre>
     */
    public com.udb.core.workflow.services.v1.CancelWorkflowResponse cancelWorkflow(com.udb.core.workflow.services.v1.CancelWorkflowRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCancelWorkflowMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Deliver an external signal to a waiting workflow step, resuming forward
     * progress (the durable equivalent of completing a blocked step).
     * </pre>
     */
    public com.udb.core.workflow.services.v1.SignalWorkflowResponse signalWorkflow(com.udb.core.workflow.services.v1.SignalWorkflowRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSignalWorkflowMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service WorkflowService.
   * <pre>
   * WorkflowService (master-plan 9.12) — durable multi-step operations with
   * compensation, exposed as a first-class native service. A workflow is a durable,
   * tenant-scoped instance handed to the EXISTING saga engine (`runtime::saga`):
   * forward progress is driven by the leader-elected workflow tick
   * (`FOR UPDATE SKIP LOCKED`, one advancer cluster-wide, fires transition events
   * only), and cancellation reuses the established saga compensation (reverse-order)
   * machinery rather than reimplementing it. Every state transition emits one
   * versioned `udb.workflow.&lt;state&gt;.v1` outbox event.
   * </pre>
   */
  public static final class WorkflowServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<WorkflowServiceBlockingStub> {
    private WorkflowServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WorkflowServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WorkflowServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Start a durable workflow instance and hand it to the saga engine. The instance
     * is persisted before any forward step runs, so it survives a restart.
     * </pre>
     */
    public com.udb.core.workflow.services.v1.StartWorkflowResponse startWorkflow(com.udb.core.workflow.services.v1.StartWorkflowRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStartWorkflowMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch a single workflow instance by id (tenant-scoped).
     * </pre>
     */
    public com.udb.core.workflow.services.v1.GetWorkflowResponse getWorkflow(com.udb.core.workflow.services.v1.GetWorkflowRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetWorkflowMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List workflow instances for the verified tenant, optionally filtered by status.
     * </pre>
     */
    public com.udb.core.workflow.services.v1.ListWorkflowsResponse listWorkflows(com.udb.core.workflow.services.v1.ListWorkflowsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListWorkflowsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Cancel a workflow and trigger the saga compensation path (reverse-order). The
     * instance moves to COMPENSATING and the EXISTING recovery worker undoes the
     * recorded side effects — this RPC never reimplements compensation.
     * </pre>
     */
    public com.udb.core.workflow.services.v1.CancelWorkflowResponse cancelWorkflow(com.udb.core.workflow.services.v1.CancelWorkflowRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCancelWorkflowMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Deliver an external signal to a waiting workflow step, resuming forward
     * progress (the durable equivalent of completing a blocked step).
     * </pre>
     */
    public com.udb.core.workflow.services.v1.SignalWorkflowResponse signalWorkflow(com.udb.core.workflow.services.v1.SignalWorkflowRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSignalWorkflowMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service WorkflowService.
   * <pre>
   * WorkflowService (master-plan 9.12) — durable multi-step operations with
   * compensation, exposed as a first-class native service. A workflow is a durable,
   * tenant-scoped instance handed to the EXISTING saga engine (`runtime::saga`):
   * forward progress is driven by the leader-elected workflow tick
   * (`FOR UPDATE SKIP LOCKED`, one advancer cluster-wide, fires transition events
   * only), and cancellation reuses the established saga compensation (reverse-order)
   * machinery rather than reimplementing it. Every state transition emits one
   * versioned `udb.workflow.&lt;state&gt;.v1` outbox event.
   * </pre>
   */
  public static final class WorkflowServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<WorkflowServiceFutureStub> {
    private WorkflowServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WorkflowServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WorkflowServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Start a durable workflow instance and hand it to the saga engine. The instance
     * is persisted before any forward step runs, so it survives a restart.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.workflow.services.v1.StartWorkflowResponse> startWorkflow(
        com.udb.core.workflow.services.v1.StartWorkflowRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStartWorkflowMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Fetch a single workflow instance by id (tenant-scoped).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.workflow.services.v1.GetWorkflowResponse> getWorkflow(
        com.udb.core.workflow.services.v1.GetWorkflowRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetWorkflowMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List workflow instances for the verified tenant, optionally filtered by status.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.workflow.services.v1.ListWorkflowsResponse> listWorkflows(
        com.udb.core.workflow.services.v1.ListWorkflowsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListWorkflowsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Cancel a workflow and trigger the saga compensation path (reverse-order). The
     * instance moves to COMPENSATING and the EXISTING recovery worker undoes the
     * recorded side effects — this RPC never reimplements compensation.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.workflow.services.v1.CancelWorkflowResponse> cancelWorkflow(
        com.udb.core.workflow.services.v1.CancelWorkflowRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCancelWorkflowMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Deliver an external signal to a waiting workflow step, resuming forward
     * progress (the durable equivalent of completing a blocked step).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.workflow.services.v1.SignalWorkflowResponse> signalWorkflow(
        com.udb.core.workflow.services.v1.SignalWorkflowRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSignalWorkflowMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_START_WORKFLOW = 0;
  private static final int METHODID_GET_WORKFLOW = 1;
  private static final int METHODID_LIST_WORKFLOWS = 2;
  private static final int METHODID_CANCEL_WORKFLOW = 3;
  private static final int METHODID_SIGNAL_WORKFLOW = 4;

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
        case METHODID_START_WORKFLOW:
          serviceImpl.startWorkflow((com.udb.core.workflow.services.v1.StartWorkflowRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.workflow.services.v1.StartWorkflowResponse>) responseObserver);
          break;
        case METHODID_GET_WORKFLOW:
          serviceImpl.getWorkflow((com.udb.core.workflow.services.v1.GetWorkflowRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.workflow.services.v1.GetWorkflowResponse>) responseObserver);
          break;
        case METHODID_LIST_WORKFLOWS:
          serviceImpl.listWorkflows((com.udb.core.workflow.services.v1.ListWorkflowsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.workflow.services.v1.ListWorkflowsResponse>) responseObserver);
          break;
        case METHODID_CANCEL_WORKFLOW:
          serviceImpl.cancelWorkflow((com.udb.core.workflow.services.v1.CancelWorkflowRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.workflow.services.v1.CancelWorkflowResponse>) responseObserver);
          break;
        case METHODID_SIGNAL_WORKFLOW:
          serviceImpl.signalWorkflow((com.udb.core.workflow.services.v1.SignalWorkflowRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.workflow.services.v1.SignalWorkflowResponse>) responseObserver);
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
          getStartWorkflowMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.workflow.services.v1.StartWorkflowRequest,
              com.udb.core.workflow.services.v1.StartWorkflowResponse>(
                service, METHODID_START_WORKFLOW)))
        .addMethod(
          getGetWorkflowMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.workflow.services.v1.GetWorkflowRequest,
              com.udb.core.workflow.services.v1.GetWorkflowResponse>(
                service, METHODID_GET_WORKFLOW)))
        .addMethod(
          getListWorkflowsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.workflow.services.v1.ListWorkflowsRequest,
              com.udb.core.workflow.services.v1.ListWorkflowsResponse>(
                service, METHODID_LIST_WORKFLOWS)))
        .addMethod(
          getCancelWorkflowMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.workflow.services.v1.CancelWorkflowRequest,
              com.udb.core.workflow.services.v1.CancelWorkflowResponse>(
                service, METHODID_CANCEL_WORKFLOW)))
        .addMethod(
          getSignalWorkflowMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.workflow.services.v1.SignalWorkflowRequest,
              com.udb.core.workflow.services.v1.SignalWorkflowResponse>(
                service, METHODID_SIGNAL_WORKFLOW)))
        .build();
  }

  private static abstract class WorkflowServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    WorkflowServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.workflow.services.v1.WorkflowServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("WorkflowService");
    }
  }

  private static final class WorkflowServiceFileDescriptorSupplier
      extends WorkflowServiceBaseDescriptorSupplier {
    WorkflowServiceFileDescriptorSupplier() {}
  }

  private static final class WorkflowServiceMethodDescriptorSupplier
      extends WorkflowServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    WorkflowServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (WorkflowServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new WorkflowServiceFileDescriptorSupplier())
              .addMethod(getStartWorkflowMethod())
              .addMethod(getGetWorkflowMethod())
              .addMethod(getListWorkflowsMethod())
              .addMethod(getCancelWorkflowMethod())
              .addMethod(getSignalWorkflowMethod())
              .build();
        }
      }
    }
    return result;
  }
}
