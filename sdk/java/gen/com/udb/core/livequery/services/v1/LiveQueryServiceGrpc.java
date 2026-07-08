package com.udb.core.livequery.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * LiveQueryService (master-plan 9.7) — query results that update themselves. A
 * client subscribes to a tenant-scoped query over a source entity and receives
 * an initial Snapshot (the current matching rows) followed by an open stream of
 * Change deltas (insert / update / delete) as the underlying data mutates.
 * Tenant isolation is the whole point: the snapshot is produced ONLY through the
 * mediated IR read path with the tenant predicate injected server-side from the
 * verified claim (never a raw query), and EVERY delta event is re-checked, fail
 * closed, against the subscriber's tenant scope before it is yielded — a CDC
 * event with a missing or foreign tenant_id is dropped, never streamed. The
 * subscription filter is an IR-expressible predicate set, not a raw query
 * string, so no caller-supplied SQL ever reaches a backend.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class LiveQueryServiceGrpc {

  private LiveQueryServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.livequery.services.v1.LiveQueryService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.livequery.services.v1.SubscribeRequest,
      com.udb.core.livequery.services.v1.SubscribeResponse> getSubscribeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Subscribe",
      requestType = com.udb.core.livequery.services.v1.SubscribeRequest.class,
      responseType = com.udb.core.livequery.services.v1.SubscribeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.udb.core.livequery.services.v1.SubscribeRequest,
      com.udb.core.livequery.services.v1.SubscribeResponse> getSubscribeMethod() {
    io.grpc.MethodDescriptor<com.udb.core.livequery.services.v1.SubscribeRequest, com.udb.core.livequery.services.v1.SubscribeResponse> getSubscribeMethod;
    if ((getSubscribeMethod = LiveQueryServiceGrpc.getSubscribeMethod) == null) {
      synchronized (LiveQueryServiceGrpc.class) {
        if ((getSubscribeMethod = LiveQueryServiceGrpc.getSubscribeMethod) == null) {
          LiveQueryServiceGrpc.getSubscribeMethod = getSubscribeMethod =
              io.grpc.MethodDescriptor.<com.udb.core.livequery.services.v1.SubscribeRequest, com.udb.core.livequery.services.v1.SubscribeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Subscribe"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.livequery.services.v1.SubscribeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.livequery.services.v1.SubscribeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new LiveQueryServiceMethodDescriptorSupplier("Subscribe"))
              .build();
        }
      }
    }
    return getSubscribeMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static LiveQueryServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LiveQueryServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LiveQueryServiceStub>() {
        @java.lang.Override
        public LiveQueryServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LiveQueryServiceStub(channel, callOptions);
        }
      };
    return LiveQueryServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static LiveQueryServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LiveQueryServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LiveQueryServiceBlockingV2Stub>() {
        @java.lang.Override
        public LiveQueryServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LiveQueryServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return LiveQueryServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static LiveQueryServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LiveQueryServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LiveQueryServiceBlockingStub>() {
        @java.lang.Override
        public LiveQueryServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LiveQueryServiceBlockingStub(channel, callOptions);
        }
      };
    return LiveQueryServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static LiveQueryServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LiveQueryServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LiveQueryServiceFutureStub>() {
        @java.lang.Override
        public LiveQueryServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LiveQueryServiceFutureStub(channel, callOptions);
        }
      };
    return LiveQueryServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * LiveQueryService (master-plan 9.7) — query results that update themselves. A
   * client subscribes to a tenant-scoped query over a source entity and receives
   * an initial Snapshot (the current matching rows) followed by an open stream of
   * Change deltas (insert / update / delete) as the underlying data mutates.
   * Tenant isolation is the whole point: the snapshot is produced ONLY through the
   * mediated IR read path with the tenant predicate injected server-side from the
   * verified claim (never a raw query), and EVERY delta event is re-checked, fail
   * closed, against the subscriber's tenant scope before it is yielded — a CDC
   * event with a missing or foreign tenant_id is dropped, never streamed. The
   * subscription filter is an IR-expressible predicate set, not a raw query
   * string, so no caller-supplied SQL ever reaches a backend.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Subscribe to a tenant-scoped live query. SERVER-STREAMING: the first message
     * carries the initial Snapshot (the current rows matching the IR filter, read
     * through the mediated path with the tenant predicate injected server-side);
     * every subsequent message carries a single Change delta. Fails closed
     * (failed_precondition) when the source entity has no resolvable tenant column.
     * </pre>
     */
    default void subscribe(com.udb.core.livequery.services.v1.SubscribeRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.livequery.services.v1.SubscribeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSubscribeMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service LiveQueryService.
   * <pre>
   * LiveQueryService (master-plan 9.7) — query results that update themselves. A
   * client subscribes to a tenant-scoped query over a source entity and receives
   * an initial Snapshot (the current matching rows) followed by an open stream of
   * Change deltas (insert / update / delete) as the underlying data mutates.
   * Tenant isolation is the whole point: the snapshot is produced ONLY through the
   * mediated IR read path with the tenant predicate injected server-side from the
   * verified claim (never a raw query), and EVERY delta event is re-checked, fail
   * closed, against the subscriber's tenant scope before it is yielded — a CDC
   * event with a missing or foreign tenant_id is dropped, never streamed. The
   * subscription filter is an IR-expressible predicate set, not a raw query
   * string, so no caller-supplied SQL ever reaches a backend.
   * </pre>
   */
  public static abstract class LiveQueryServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return LiveQueryServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service LiveQueryService.
   * <pre>
   * LiveQueryService (master-plan 9.7) — query results that update themselves. A
   * client subscribes to a tenant-scoped query over a source entity and receives
   * an initial Snapshot (the current matching rows) followed by an open stream of
   * Change deltas (insert / update / delete) as the underlying data mutates.
   * Tenant isolation is the whole point: the snapshot is produced ONLY through the
   * mediated IR read path with the tenant predicate injected server-side from the
   * verified claim (never a raw query), and EVERY delta event is re-checked, fail
   * closed, against the subscriber's tenant scope before it is yielded — a CDC
   * event with a missing or foreign tenant_id is dropped, never streamed. The
   * subscription filter is an IR-expressible predicate set, not a raw query
   * string, so no caller-supplied SQL ever reaches a backend.
   * </pre>
   */
  public static final class LiveQueryServiceStub
      extends io.grpc.stub.AbstractAsyncStub<LiveQueryServiceStub> {
    private LiveQueryServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LiveQueryServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LiveQueryServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Subscribe to a tenant-scoped live query. SERVER-STREAMING: the first message
     * carries the initial Snapshot (the current rows matching the IR filter, read
     * through the mediated path with the tenant predicate injected server-side);
     * every subsequent message carries a single Change delta. Fails closed
     * (failed_precondition) when the source entity has no resolvable tenant column.
     * </pre>
     */
    public void subscribe(com.udb.core.livequery.services.v1.SubscribeRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.livequery.services.v1.SubscribeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getSubscribeMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service LiveQueryService.
   * <pre>
   * LiveQueryService (master-plan 9.7) — query results that update themselves. A
   * client subscribes to a tenant-scoped query over a source entity and receives
   * an initial Snapshot (the current matching rows) followed by an open stream of
   * Change deltas (insert / update / delete) as the underlying data mutates.
   * Tenant isolation is the whole point: the snapshot is produced ONLY through the
   * mediated IR read path with the tenant predicate injected server-side from the
   * verified claim (never a raw query), and EVERY delta event is re-checked, fail
   * closed, against the subscriber's tenant scope before it is yielded — a CDC
   * event with a missing or foreign tenant_id is dropped, never streamed. The
   * subscription filter is an IR-expressible predicate set, not a raw query
   * string, so no caller-supplied SQL ever reaches a backend.
   * </pre>
   */
  public static final class LiveQueryServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<LiveQueryServiceBlockingV2Stub> {
    private LiveQueryServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LiveQueryServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LiveQueryServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Subscribe to a tenant-scoped live query. SERVER-STREAMING: the first message
     * carries the initial Snapshot (the current rows matching the IR filter, read
     * through the mediated path with the tenant predicate injected server-side);
     * every subsequent message carries a single Change delta. Fails closed
     * (failed_precondition) when the source entity has no resolvable tenant column.
     * </pre>
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<?, com.udb.core.livequery.services.v1.SubscribeResponse>
        subscribe(com.udb.core.livequery.services.v1.SubscribeRequest request) {
      return io.grpc.stub.ClientCalls.blockingV2ServerStreamingCall(
          getChannel(), getSubscribeMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service LiveQueryService.
   * <pre>
   * LiveQueryService (master-plan 9.7) — query results that update themselves. A
   * client subscribes to a tenant-scoped query over a source entity and receives
   * an initial Snapshot (the current matching rows) followed by an open stream of
   * Change deltas (insert / update / delete) as the underlying data mutates.
   * Tenant isolation is the whole point: the snapshot is produced ONLY through the
   * mediated IR read path with the tenant predicate injected server-side from the
   * verified claim (never a raw query), and EVERY delta event is re-checked, fail
   * closed, against the subscriber's tenant scope before it is yielded — a CDC
   * event with a missing or foreign tenant_id is dropped, never streamed. The
   * subscription filter is an IR-expressible predicate set, not a raw query
   * string, so no caller-supplied SQL ever reaches a backend.
   * </pre>
   */
  public static final class LiveQueryServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<LiveQueryServiceBlockingStub> {
    private LiveQueryServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LiveQueryServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LiveQueryServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Subscribe to a tenant-scoped live query. SERVER-STREAMING: the first message
     * carries the initial Snapshot (the current rows matching the IR filter, read
     * through the mediated path with the tenant predicate injected server-side);
     * every subsequent message carries a single Change delta. Fails closed
     * (failed_precondition) when the source entity has no resolvable tenant column.
     * </pre>
     */
    public java.util.Iterator<com.udb.core.livequery.services.v1.SubscribeResponse> subscribe(
        com.udb.core.livequery.services.v1.SubscribeRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getSubscribeMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service LiveQueryService.
   * <pre>
   * LiveQueryService (master-plan 9.7) — query results that update themselves. A
   * client subscribes to a tenant-scoped query over a source entity and receives
   * an initial Snapshot (the current matching rows) followed by an open stream of
   * Change deltas (insert / update / delete) as the underlying data mutates.
   * Tenant isolation is the whole point: the snapshot is produced ONLY through the
   * mediated IR read path with the tenant predicate injected server-side from the
   * verified claim (never a raw query), and EVERY delta event is re-checked, fail
   * closed, against the subscriber's tenant scope before it is yielded — a CDC
   * event with a missing or foreign tenant_id is dropped, never streamed. The
   * subscription filter is an IR-expressible predicate set, not a raw query
   * string, so no caller-supplied SQL ever reaches a backend.
   * </pre>
   */
  public static final class LiveQueryServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<LiveQueryServiceFutureStub> {
    private LiveQueryServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LiveQueryServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LiveQueryServiceFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_SUBSCRIBE = 0;

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
        case METHODID_SUBSCRIBE:
          serviceImpl.subscribe((com.udb.core.livequery.services.v1.SubscribeRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.livequery.services.v1.SubscribeResponse>) responseObserver);
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
          getSubscribeMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              com.udb.core.livequery.services.v1.SubscribeRequest,
              com.udb.core.livequery.services.v1.SubscribeResponse>(
                service, METHODID_SUBSCRIBE)))
        .build();
  }

  private static abstract class LiveQueryServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    LiveQueryServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.livequery.services.v1.LivequeryServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("LiveQueryService");
    }
  }

  private static final class LiveQueryServiceFileDescriptorSupplier
      extends LiveQueryServiceBaseDescriptorSupplier {
    LiveQueryServiceFileDescriptorSupplier() {}
  }

  private static final class LiveQueryServiceMethodDescriptorSupplier
      extends LiveQueryServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    LiveQueryServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (LiveQueryServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new LiveQueryServiceFileDescriptorSupplier())
              .addMethod(getSubscribeMethod())
              .build();
        }
      }
    }
    return result;
  }
}
