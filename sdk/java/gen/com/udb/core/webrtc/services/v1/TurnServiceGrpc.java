package com.udb.core.webrtc.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class TurnServiceGrpc {

  private TurnServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.webrtc.services.v1.TurnService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.IssueCredentialsRequest,
      com.udb.core.webrtc.services.v1.IssueCredentialsResponse> getIssueCredentialsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "IssueCredentials",
      requestType = com.udb.core.webrtc.services.v1.IssueCredentialsRequest.class,
      responseType = com.udb.core.webrtc.services.v1.IssueCredentialsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.IssueCredentialsRequest,
      com.udb.core.webrtc.services.v1.IssueCredentialsResponse> getIssueCredentialsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.IssueCredentialsRequest, com.udb.core.webrtc.services.v1.IssueCredentialsResponse> getIssueCredentialsMethod;
    if ((getIssueCredentialsMethod = TurnServiceGrpc.getIssueCredentialsMethod) == null) {
      synchronized (TurnServiceGrpc.class) {
        if ((getIssueCredentialsMethod = TurnServiceGrpc.getIssueCredentialsMethod) == null) {
          TurnServiceGrpc.getIssueCredentialsMethod = getIssueCredentialsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webrtc.services.v1.IssueCredentialsRequest, com.udb.core.webrtc.services.v1.IssueCredentialsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "IssueCredentials"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.IssueCredentialsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.IssueCredentialsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TurnServiceMethodDescriptorSupplier("IssueCredentials"))
              .build();
        }
      }
    }
    return getIssueCredentialsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TurnServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TurnServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TurnServiceStub>() {
        @java.lang.Override
        public TurnServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TurnServiceStub(channel, callOptions);
        }
      };
    return TurnServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static TurnServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TurnServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TurnServiceBlockingV2Stub>() {
        @java.lang.Override
        public TurnServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TurnServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return TurnServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TurnServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TurnServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TurnServiceBlockingStub>() {
        @java.lang.Override
        public TurnServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TurnServiceBlockingStub(channel, callOptions);
        }
      };
    return TurnServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TurnServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TurnServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TurnServiceFutureStub>() {
        @java.lang.Override
        public TurnServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TurnServiceFutureStub(channel, callOptions);
        }
      };
    return TurnServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     * <pre>
     * Issue ephemeral TURN/STUN credentials
     * </pre>
     */
    default void issueCredentials(com.udb.core.webrtc.services.v1.IssueCredentialsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.IssueCredentialsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getIssueCredentialsMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service TurnService.
   */
  public static abstract class TurnServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return TurnServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service TurnService.
   */
  public static final class TurnServiceStub
      extends io.grpc.stub.AbstractAsyncStub<TurnServiceStub> {
    private TurnServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TurnServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TurnServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Issue ephemeral TURN/STUN credentials
     * </pre>
     */
    public void issueCredentials(com.udb.core.webrtc.services.v1.IssueCredentialsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.IssueCredentialsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getIssueCredentialsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service TurnService.
   */
  public static final class TurnServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<TurnServiceBlockingV2Stub> {
    private TurnServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TurnServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TurnServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Issue ephemeral TURN/STUN credentials
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.IssueCredentialsResponse issueCredentials(com.udb.core.webrtc.services.v1.IssueCredentialsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getIssueCredentialsMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service TurnService.
   */
  public static final class TurnServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<TurnServiceBlockingStub> {
    private TurnServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TurnServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TurnServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Issue ephemeral TURN/STUN credentials
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.IssueCredentialsResponse issueCredentials(com.udb.core.webrtc.services.v1.IssueCredentialsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getIssueCredentialsMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service TurnService.
   */
  public static final class TurnServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<TurnServiceFutureStub> {
    private TurnServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TurnServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TurnServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Issue ephemeral TURN/STUN credentials
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webrtc.services.v1.IssueCredentialsResponse> issueCredentials(
        com.udb.core.webrtc.services.v1.IssueCredentialsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getIssueCredentialsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_ISSUE_CREDENTIALS = 0;

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
        case METHODID_ISSUE_CREDENTIALS:
          serviceImpl.issueCredentials((com.udb.core.webrtc.services.v1.IssueCredentialsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.IssueCredentialsResponse>) responseObserver);
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
          getIssueCredentialsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webrtc.services.v1.IssueCredentialsRequest,
              com.udb.core.webrtc.services.v1.IssueCredentialsResponse>(
                service, METHODID_ISSUE_CREDENTIALS)))
        .build();
  }

  private static abstract class TurnServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TurnServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.webrtc.services.v1.WebrtcServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TurnService");
    }
  }

  private static final class TurnServiceFileDescriptorSupplier
      extends TurnServiceBaseDescriptorSupplier {
    TurnServiceFileDescriptorSupplier() {}
  }

  private static final class TurnServiceMethodDescriptorSupplier
      extends TurnServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    TurnServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (TurnServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TurnServiceFileDescriptorSupplier())
              .addMethod(getIssueCredentialsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
