package com.udb.core.scheduler.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * SchedulerService — durable cron and one-shot jobs as a native service.
 * Mutations persist to the canonical store, tenant-scoped by the verified claim,
 * and emit one outbox event each. The leader-elected scheduler tick fires DUE
 * jobs as outbox events only (consumers do the work).
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class SchedulerServiceGrpc {

  private SchedulerServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.scheduler.services.v1.SchedulerService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.CreateJobRequest,
      com.udb.core.scheduler.services.v1.CreateJobResponse> getCreateJobMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateJob",
      requestType = com.udb.core.scheduler.services.v1.CreateJobRequest.class,
      responseType = com.udb.core.scheduler.services.v1.CreateJobResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.CreateJobRequest,
      com.udb.core.scheduler.services.v1.CreateJobResponse> getCreateJobMethod() {
    io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.CreateJobRequest, com.udb.core.scheduler.services.v1.CreateJobResponse> getCreateJobMethod;
    if ((getCreateJobMethod = SchedulerServiceGrpc.getCreateJobMethod) == null) {
      synchronized (SchedulerServiceGrpc.class) {
        if ((getCreateJobMethod = SchedulerServiceGrpc.getCreateJobMethod) == null) {
          SchedulerServiceGrpc.getCreateJobMethod = getCreateJobMethod =
              io.grpc.MethodDescriptor.<com.udb.core.scheduler.services.v1.CreateJobRequest, com.udb.core.scheduler.services.v1.CreateJobResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateJob"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.scheduler.services.v1.CreateJobRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.scheduler.services.v1.CreateJobResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SchedulerServiceMethodDescriptorSupplier("CreateJob"))
              .build();
        }
      }
    }
    return getCreateJobMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.GetJobRequest,
      com.udb.core.scheduler.services.v1.GetJobResponse> getGetJobMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetJob",
      requestType = com.udb.core.scheduler.services.v1.GetJobRequest.class,
      responseType = com.udb.core.scheduler.services.v1.GetJobResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.GetJobRequest,
      com.udb.core.scheduler.services.v1.GetJobResponse> getGetJobMethod() {
    io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.GetJobRequest, com.udb.core.scheduler.services.v1.GetJobResponse> getGetJobMethod;
    if ((getGetJobMethod = SchedulerServiceGrpc.getGetJobMethod) == null) {
      synchronized (SchedulerServiceGrpc.class) {
        if ((getGetJobMethod = SchedulerServiceGrpc.getGetJobMethod) == null) {
          SchedulerServiceGrpc.getGetJobMethod = getGetJobMethod =
              io.grpc.MethodDescriptor.<com.udb.core.scheduler.services.v1.GetJobRequest, com.udb.core.scheduler.services.v1.GetJobResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetJob"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.scheduler.services.v1.GetJobRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.scheduler.services.v1.GetJobResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SchedulerServiceMethodDescriptorSupplier("GetJob"))
              .build();
        }
      }
    }
    return getGetJobMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.ListJobsRequest,
      com.udb.core.scheduler.services.v1.ListJobsResponse> getListJobsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListJobs",
      requestType = com.udb.core.scheduler.services.v1.ListJobsRequest.class,
      responseType = com.udb.core.scheduler.services.v1.ListJobsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.ListJobsRequest,
      com.udb.core.scheduler.services.v1.ListJobsResponse> getListJobsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.ListJobsRequest, com.udb.core.scheduler.services.v1.ListJobsResponse> getListJobsMethod;
    if ((getListJobsMethod = SchedulerServiceGrpc.getListJobsMethod) == null) {
      synchronized (SchedulerServiceGrpc.class) {
        if ((getListJobsMethod = SchedulerServiceGrpc.getListJobsMethod) == null) {
          SchedulerServiceGrpc.getListJobsMethod = getListJobsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.scheduler.services.v1.ListJobsRequest, com.udb.core.scheduler.services.v1.ListJobsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListJobs"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.scheduler.services.v1.ListJobsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.scheduler.services.v1.ListJobsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SchedulerServiceMethodDescriptorSupplier("ListJobs"))
              .build();
        }
      }
    }
    return getListJobsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.DeleteJobRequest,
      com.udb.core.scheduler.services.v1.DeleteJobResponse> getDeleteJobMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteJob",
      requestType = com.udb.core.scheduler.services.v1.DeleteJobRequest.class,
      responseType = com.udb.core.scheduler.services.v1.DeleteJobResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.DeleteJobRequest,
      com.udb.core.scheduler.services.v1.DeleteJobResponse> getDeleteJobMethod() {
    io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.DeleteJobRequest, com.udb.core.scheduler.services.v1.DeleteJobResponse> getDeleteJobMethod;
    if ((getDeleteJobMethod = SchedulerServiceGrpc.getDeleteJobMethod) == null) {
      synchronized (SchedulerServiceGrpc.class) {
        if ((getDeleteJobMethod = SchedulerServiceGrpc.getDeleteJobMethod) == null) {
          SchedulerServiceGrpc.getDeleteJobMethod = getDeleteJobMethod =
              io.grpc.MethodDescriptor.<com.udb.core.scheduler.services.v1.DeleteJobRequest, com.udb.core.scheduler.services.v1.DeleteJobResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteJob"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.scheduler.services.v1.DeleteJobRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.scheduler.services.v1.DeleteJobResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SchedulerServiceMethodDescriptorSupplier("DeleteJob"))
              .build();
        }
      }
    }
    return getDeleteJobMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.PauseJobRequest,
      com.udb.core.scheduler.services.v1.PauseJobResponse> getPauseJobMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PauseJob",
      requestType = com.udb.core.scheduler.services.v1.PauseJobRequest.class,
      responseType = com.udb.core.scheduler.services.v1.PauseJobResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.PauseJobRequest,
      com.udb.core.scheduler.services.v1.PauseJobResponse> getPauseJobMethod() {
    io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.PauseJobRequest, com.udb.core.scheduler.services.v1.PauseJobResponse> getPauseJobMethod;
    if ((getPauseJobMethod = SchedulerServiceGrpc.getPauseJobMethod) == null) {
      synchronized (SchedulerServiceGrpc.class) {
        if ((getPauseJobMethod = SchedulerServiceGrpc.getPauseJobMethod) == null) {
          SchedulerServiceGrpc.getPauseJobMethod = getPauseJobMethod =
              io.grpc.MethodDescriptor.<com.udb.core.scheduler.services.v1.PauseJobRequest, com.udb.core.scheduler.services.v1.PauseJobResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PauseJob"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.scheduler.services.v1.PauseJobRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.scheduler.services.v1.PauseJobResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SchedulerServiceMethodDescriptorSupplier("PauseJob"))
              .build();
        }
      }
    }
    return getPauseJobMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.ResumeJobRequest,
      com.udb.core.scheduler.services.v1.ResumeJobResponse> getResumeJobMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ResumeJob",
      requestType = com.udb.core.scheduler.services.v1.ResumeJobRequest.class,
      responseType = com.udb.core.scheduler.services.v1.ResumeJobResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.ResumeJobRequest,
      com.udb.core.scheduler.services.v1.ResumeJobResponse> getResumeJobMethod() {
    io.grpc.MethodDescriptor<com.udb.core.scheduler.services.v1.ResumeJobRequest, com.udb.core.scheduler.services.v1.ResumeJobResponse> getResumeJobMethod;
    if ((getResumeJobMethod = SchedulerServiceGrpc.getResumeJobMethod) == null) {
      synchronized (SchedulerServiceGrpc.class) {
        if ((getResumeJobMethod = SchedulerServiceGrpc.getResumeJobMethod) == null) {
          SchedulerServiceGrpc.getResumeJobMethod = getResumeJobMethod =
              io.grpc.MethodDescriptor.<com.udb.core.scheduler.services.v1.ResumeJobRequest, com.udb.core.scheduler.services.v1.ResumeJobResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ResumeJob"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.scheduler.services.v1.ResumeJobRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.scheduler.services.v1.ResumeJobResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SchedulerServiceMethodDescriptorSupplier("ResumeJob"))
              .build();
        }
      }
    }
    return getResumeJobMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static SchedulerServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SchedulerServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SchedulerServiceStub>() {
        @java.lang.Override
        public SchedulerServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SchedulerServiceStub(channel, callOptions);
        }
      };
    return SchedulerServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static SchedulerServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SchedulerServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SchedulerServiceBlockingV2Stub>() {
        @java.lang.Override
        public SchedulerServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SchedulerServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return SchedulerServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static SchedulerServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SchedulerServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SchedulerServiceBlockingStub>() {
        @java.lang.Override
        public SchedulerServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SchedulerServiceBlockingStub(channel, callOptions);
        }
      };
    return SchedulerServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static SchedulerServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SchedulerServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SchedulerServiceFutureStub>() {
        @java.lang.Override
        public SchedulerServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SchedulerServiceFutureStub(channel, callOptions);
        }
      };
    return SchedulerServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * SchedulerService — durable cron and one-shot jobs as a native service.
   * Mutations persist to the canonical store, tenant-scoped by the verified claim,
   * and emit one outbox event each. The leader-elected scheduler tick fires DUE
   * jobs as outbox events only (consumers do the work).
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Create a cron or one-shot job.
     * </pre>
     */
    default void createJob(com.udb.core.scheduler.services.v1.CreateJobRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.CreateJobResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateJobMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get a job by id.
     * </pre>
     */
    default void getJob(com.udb.core.scheduler.services.v1.GetJobRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.GetJobResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetJobMethod(), responseObserver);
    }

    /**
     * <pre>
     * List jobs for the caller's tenant.
     * </pre>
     */
    default void listJobs(com.udb.core.scheduler.services.v1.ListJobsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.ListJobsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListJobsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Delete (soft-delete) a job.
     * </pre>
     */
    default void deleteJob(com.udb.core.scheduler.services.v1.DeleteJobRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.DeleteJobResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteJobMethod(), responseObserver);
    }

    /**
     * <pre>
     * Pause a job so the tick stops claiming it.
     * </pre>
     */
    default void pauseJob(com.udb.core.scheduler.services.v1.PauseJobRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.PauseJobResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPauseJobMethod(), responseObserver);
    }

    /**
     * <pre>
     * Resume a paused job.
     * </pre>
     */
    default void resumeJob(com.udb.core.scheduler.services.v1.ResumeJobRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.ResumeJobResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getResumeJobMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service SchedulerService.
   * <pre>
   * SchedulerService — durable cron and one-shot jobs as a native service.
   * Mutations persist to the canonical store, tenant-scoped by the verified claim,
   * and emit one outbox event each. The leader-elected scheduler tick fires DUE
   * jobs as outbox events only (consumers do the work).
   * </pre>
   */
  public static abstract class SchedulerServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return SchedulerServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service SchedulerService.
   * <pre>
   * SchedulerService — durable cron and one-shot jobs as a native service.
   * Mutations persist to the canonical store, tenant-scoped by the verified claim,
   * and emit one outbox event each. The leader-elected scheduler tick fires DUE
   * jobs as outbox events only (consumers do the work).
   * </pre>
   */
  public static final class SchedulerServiceStub
      extends io.grpc.stub.AbstractAsyncStub<SchedulerServiceStub> {
    private SchedulerServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SchedulerServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SchedulerServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Create a cron or one-shot job.
     * </pre>
     */
    public void createJob(com.udb.core.scheduler.services.v1.CreateJobRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.CreateJobResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateJobMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get a job by id.
     * </pre>
     */
    public void getJob(com.udb.core.scheduler.services.v1.GetJobRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.GetJobResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetJobMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List jobs for the caller's tenant.
     * </pre>
     */
    public void listJobs(com.udb.core.scheduler.services.v1.ListJobsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.ListJobsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListJobsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Delete (soft-delete) a job.
     * </pre>
     */
    public void deleteJob(com.udb.core.scheduler.services.v1.DeleteJobRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.DeleteJobResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteJobMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Pause a job so the tick stops claiming it.
     * </pre>
     */
    public void pauseJob(com.udb.core.scheduler.services.v1.PauseJobRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.PauseJobResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPauseJobMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Resume a paused job.
     * </pre>
     */
    public void resumeJob(com.udb.core.scheduler.services.v1.ResumeJobRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.ResumeJobResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getResumeJobMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service SchedulerService.
   * <pre>
   * SchedulerService — durable cron and one-shot jobs as a native service.
   * Mutations persist to the canonical store, tenant-scoped by the verified claim,
   * and emit one outbox event each. The leader-elected scheduler tick fires DUE
   * jobs as outbox events only (consumers do the work).
   * </pre>
   */
  public static final class SchedulerServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<SchedulerServiceBlockingV2Stub> {
    private SchedulerServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SchedulerServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SchedulerServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Create a cron or one-shot job.
     * </pre>
     */
    public com.udb.core.scheduler.services.v1.CreateJobResponse createJob(com.udb.core.scheduler.services.v1.CreateJobRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreateJobMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a job by id.
     * </pre>
     */
    public com.udb.core.scheduler.services.v1.GetJobResponse getJob(com.udb.core.scheduler.services.v1.GetJobRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetJobMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List jobs for the caller's tenant.
     * </pre>
     */
    public com.udb.core.scheduler.services.v1.ListJobsResponse listJobs(com.udb.core.scheduler.services.v1.ListJobsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListJobsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete (soft-delete) a job.
     * </pre>
     */
    public com.udb.core.scheduler.services.v1.DeleteJobResponse deleteJob(com.udb.core.scheduler.services.v1.DeleteJobRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeleteJobMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Pause a job so the tick stops claiming it.
     * </pre>
     */
    public com.udb.core.scheduler.services.v1.PauseJobResponse pauseJob(com.udb.core.scheduler.services.v1.PauseJobRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPauseJobMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Resume a paused job.
     * </pre>
     */
    public com.udb.core.scheduler.services.v1.ResumeJobResponse resumeJob(com.udb.core.scheduler.services.v1.ResumeJobRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getResumeJobMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service SchedulerService.
   * <pre>
   * SchedulerService — durable cron and one-shot jobs as a native service.
   * Mutations persist to the canonical store, tenant-scoped by the verified claim,
   * and emit one outbox event each. The leader-elected scheduler tick fires DUE
   * jobs as outbox events only (consumers do the work).
   * </pre>
   */
  public static final class SchedulerServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<SchedulerServiceBlockingStub> {
    private SchedulerServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SchedulerServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SchedulerServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Create a cron or one-shot job.
     * </pre>
     */
    public com.udb.core.scheduler.services.v1.CreateJobResponse createJob(com.udb.core.scheduler.services.v1.CreateJobRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateJobMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a job by id.
     * </pre>
     */
    public com.udb.core.scheduler.services.v1.GetJobResponse getJob(com.udb.core.scheduler.services.v1.GetJobRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetJobMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List jobs for the caller's tenant.
     * </pre>
     */
    public com.udb.core.scheduler.services.v1.ListJobsResponse listJobs(com.udb.core.scheduler.services.v1.ListJobsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListJobsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete (soft-delete) a job.
     * </pre>
     */
    public com.udb.core.scheduler.services.v1.DeleteJobResponse deleteJob(com.udb.core.scheduler.services.v1.DeleteJobRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteJobMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Pause a job so the tick stops claiming it.
     * </pre>
     */
    public com.udb.core.scheduler.services.v1.PauseJobResponse pauseJob(com.udb.core.scheduler.services.v1.PauseJobRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPauseJobMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Resume a paused job.
     * </pre>
     */
    public com.udb.core.scheduler.services.v1.ResumeJobResponse resumeJob(com.udb.core.scheduler.services.v1.ResumeJobRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getResumeJobMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service SchedulerService.
   * <pre>
   * SchedulerService — durable cron and one-shot jobs as a native service.
   * Mutations persist to the canonical store, tenant-scoped by the verified claim,
   * and emit one outbox event each. The leader-elected scheduler tick fires DUE
   * jobs as outbox events only (consumers do the work).
   * </pre>
   */
  public static final class SchedulerServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<SchedulerServiceFutureStub> {
    private SchedulerServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SchedulerServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SchedulerServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Create a cron or one-shot job.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.scheduler.services.v1.CreateJobResponse> createJob(
        com.udb.core.scheduler.services.v1.CreateJobRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateJobMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get a job by id.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.scheduler.services.v1.GetJobResponse> getJob(
        com.udb.core.scheduler.services.v1.GetJobRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetJobMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List jobs for the caller's tenant.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.scheduler.services.v1.ListJobsResponse> listJobs(
        com.udb.core.scheduler.services.v1.ListJobsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListJobsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Delete (soft-delete) a job.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.scheduler.services.v1.DeleteJobResponse> deleteJob(
        com.udb.core.scheduler.services.v1.DeleteJobRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteJobMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Pause a job so the tick stops claiming it.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.scheduler.services.v1.PauseJobResponse> pauseJob(
        com.udb.core.scheduler.services.v1.PauseJobRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPauseJobMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Resume a paused job.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.scheduler.services.v1.ResumeJobResponse> resumeJob(
        com.udb.core.scheduler.services.v1.ResumeJobRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getResumeJobMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_JOB = 0;
  private static final int METHODID_GET_JOB = 1;
  private static final int METHODID_LIST_JOBS = 2;
  private static final int METHODID_DELETE_JOB = 3;
  private static final int METHODID_PAUSE_JOB = 4;
  private static final int METHODID_RESUME_JOB = 5;

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
        case METHODID_CREATE_JOB:
          serviceImpl.createJob((com.udb.core.scheduler.services.v1.CreateJobRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.CreateJobResponse>) responseObserver);
          break;
        case METHODID_GET_JOB:
          serviceImpl.getJob((com.udb.core.scheduler.services.v1.GetJobRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.GetJobResponse>) responseObserver);
          break;
        case METHODID_LIST_JOBS:
          serviceImpl.listJobs((com.udb.core.scheduler.services.v1.ListJobsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.ListJobsResponse>) responseObserver);
          break;
        case METHODID_DELETE_JOB:
          serviceImpl.deleteJob((com.udb.core.scheduler.services.v1.DeleteJobRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.DeleteJobResponse>) responseObserver);
          break;
        case METHODID_PAUSE_JOB:
          serviceImpl.pauseJob((com.udb.core.scheduler.services.v1.PauseJobRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.PauseJobResponse>) responseObserver);
          break;
        case METHODID_RESUME_JOB:
          serviceImpl.resumeJob((com.udb.core.scheduler.services.v1.ResumeJobRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.scheduler.services.v1.ResumeJobResponse>) responseObserver);
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
          getCreateJobMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.scheduler.services.v1.CreateJobRequest,
              com.udb.core.scheduler.services.v1.CreateJobResponse>(
                service, METHODID_CREATE_JOB)))
        .addMethod(
          getGetJobMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.scheduler.services.v1.GetJobRequest,
              com.udb.core.scheduler.services.v1.GetJobResponse>(
                service, METHODID_GET_JOB)))
        .addMethod(
          getListJobsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.scheduler.services.v1.ListJobsRequest,
              com.udb.core.scheduler.services.v1.ListJobsResponse>(
                service, METHODID_LIST_JOBS)))
        .addMethod(
          getDeleteJobMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.scheduler.services.v1.DeleteJobRequest,
              com.udb.core.scheduler.services.v1.DeleteJobResponse>(
                service, METHODID_DELETE_JOB)))
        .addMethod(
          getPauseJobMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.scheduler.services.v1.PauseJobRequest,
              com.udb.core.scheduler.services.v1.PauseJobResponse>(
                service, METHODID_PAUSE_JOB)))
        .addMethod(
          getResumeJobMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.scheduler.services.v1.ResumeJobRequest,
              com.udb.core.scheduler.services.v1.ResumeJobResponse>(
                service, METHODID_RESUME_JOB)))
        .build();
  }

  private static abstract class SchedulerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    SchedulerServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.scheduler.services.v1.SchedulerServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("SchedulerService");
    }
  }

  private static final class SchedulerServiceFileDescriptorSupplier
      extends SchedulerServiceBaseDescriptorSupplier {
    SchedulerServiceFileDescriptorSupplier() {}
  }

  private static final class SchedulerServiceMethodDescriptorSupplier
      extends SchedulerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    SchedulerServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (SchedulerServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new SchedulerServiceFileDescriptorSupplier())
              .addMethod(getCreateJobMethod())
              .addMethod(getGetJobMethod())
              .addMethod(getListJobsMethod())
              .addMethod(getDeleteJobMethod())
              .addMethod(getPauseJobMethod())
              .addMethod(getResumeJobMethod())
              .build();
        }
      }
    }
    return result;
  }
}
