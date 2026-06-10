package com.udb.core.webrtc.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class SignalingServiceGrpc {

  private SignalingServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.webrtc.services.v1.SignalingService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.SignalRequest,
      com.udb.core.webrtc.services.v1.SignalResponse> getSignalMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Signal",
      requestType = com.udb.core.webrtc.services.v1.SignalRequest.class,
      responseType = com.udb.core.webrtc.services.v1.SignalResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.SignalRequest,
      com.udb.core.webrtc.services.v1.SignalResponse> getSignalMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.SignalRequest, com.udb.core.webrtc.services.v1.SignalResponse> getSignalMethod;
    if ((getSignalMethod = SignalingServiceGrpc.getSignalMethod) == null) {
      synchronized (SignalingServiceGrpc.class) {
        if ((getSignalMethod = SignalingServiceGrpc.getSignalMethod) == null) {
          SignalingServiceGrpc.getSignalMethod = getSignalMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webrtc.services.v1.SignalRequest, com.udb.core.webrtc.services.v1.SignalResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Signal"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.SignalRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.SignalResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SignalingServiceMethodDescriptorSupplier("Signal"))
              .build();
        }
      }
    }
    return getSignalMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static SignalingServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SignalingServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SignalingServiceStub>() {
        @java.lang.Override
        public SignalingServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SignalingServiceStub(channel, callOptions);
        }
      };
    return SignalingServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static SignalingServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SignalingServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SignalingServiceBlockingV2Stub>() {
        @java.lang.Override
        public SignalingServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SignalingServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return SignalingServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static SignalingServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SignalingServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SignalingServiceBlockingStub>() {
        @java.lang.Override
        public SignalingServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SignalingServiceBlockingStub(channel, callOptions);
        }
      };
    return SignalingServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static SignalingServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SignalingServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SignalingServiceFutureStub>() {
        @java.lang.Override
        public SignalingServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SignalingServiceFutureStub(channel, callOptions);
        }
      };
    return SignalingServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     * <pre>
     * Bidirectional signaling channel for SDP offer/answer and ICE exchange.
     * Streaming RPC: no google.api.http (REST mapping is not supported for
     * streaming methods) and no rest_contract.
     * Named `Signal` (not `Connect`) because tonic generates a client `connect`
     * associated constructor; an RPC named `Connect` collides with it.
     * </pre>
     */
    default io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.SignalRequest> signal(
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.SignalResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getSignalMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service SignalingService.
   */
  public static abstract class SignalingServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return SignalingServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service SignalingService.
   */
  public static final class SignalingServiceStub
      extends io.grpc.stub.AbstractAsyncStub<SignalingServiceStub> {
    private SignalingServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SignalingServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SignalingServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Bidirectional signaling channel for SDP offer/answer and ICE exchange.
     * Streaming RPC: no google.api.http (REST mapping is not supported for
     * streaming methods) and no rest_contract.
     * Named `Signal` (not `Connect`) because tonic generates a client `connect`
     * associated constructor; an RPC named `Connect` collides with it.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.SignalRequest> signal(
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.SignalResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getSignalMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service SignalingService.
   */
  public static final class SignalingServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<SignalingServiceBlockingV2Stub> {
    private SignalingServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SignalingServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SignalingServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Bidirectional signaling channel for SDP offer/answer and ICE exchange.
     * Streaming RPC: no google.api.http (REST mapping is not supported for
     * streaming methods) and no rest_contract.
     * Named `Signal` (not `Connect`) because tonic generates a client `connect`
     * associated constructor; an RPC named `Connect` collides with it.
     * </pre>
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<com.udb.core.webrtc.services.v1.SignalRequest, com.udb.core.webrtc.services.v1.SignalResponse>
        signal() {
      return io.grpc.stub.ClientCalls.blockingBidiStreamingCall(
          getChannel(), getSignalMethod(), getCallOptions());
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service SignalingService.
   */
  public static final class SignalingServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<SignalingServiceBlockingStub> {
    private SignalingServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SignalingServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SignalingServiceBlockingStub(channel, callOptions);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service SignalingService.
   */
  public static final class SignalingServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<SignalingServiceFutureStub> {
    private SignalingServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SignalingServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SignalingServiceFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_SIGNAL = 0;

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
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SIGNAL:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.signal(
              (io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.SignalResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getSignalMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              com.udb.core.webrtc.services.v1.SignalRequest,
              com.udb.core.webrtc.services.v1.SignalResponse>(
                service, METHODID_SIGNAL)))
        .build();
  }

  private static abstract class SignalingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    SignalingServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.webrtc.services.v1.WebrtcServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("SignalingService");
    }
  }

  private static final class SignalingServiceFileDescriptorSupplier
      extends SignalingServiceBaseDescriptorSupplier {
    SignalingServiceFileDescriptorSupplier() {}
  }

  private static final class SignalingServiceMethodDescriptorSupplier
      extends SignalingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    SignalingServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (SignalingServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new SignalingServiceFileDescriptorSupplier())
              .addMethod(getSignalMethod())
              .build();
        }
      }
    }
    return result;
  }
}
