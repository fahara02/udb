package com.udb.core.webrtc.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class TrackServiceGrpc {

  private TrackServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.webrtc.services.v1.TrackService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.PublishTrackRequest,
      com.udb.core.webrtc.services.v1.PublishTrackResponse> getPublishTrackMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PublishTrack",
      requestType = com.udb.core.webrtc.services.v1.PublishTrackRequest.class,
      responseType = com.udb.core.webrtc.services.v1.PublishTrackResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.PublishTrackRequest,
      com.udb.core.webrtc.services.v1.PublishTrackResponse> getPublishTrackMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.PublishTrackRequest, com.udb.core.webrtc.services.v1.PublishTrackResponse> getPublishTrackMethod;
    if ((getPublishTrackMethod = TrackServiceGrpc.getPublishTrackMethod) == null) {
      synchronized (TrackServiceGrpc.class) {
        if ((getPublishTrackMethod = TrackServiceGrpc.getPublishTrackMethod) == null) {
          TrackServiceGrpc.getPublishTrackMethod = getPublishTrackMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webrtc.services.v1.PublishTrackRequest, com.udb.core.webrtc.services.v1.PublishTrackResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PublishTrack"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.PublishTrackRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.PublishTrackResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TrackServiceMethodDescriptorSupplier("PublishTrack"))
              .build();
        }
      }
    }
    return getPublishTrackMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.UnpublishTrackRequest,
      com.udb.core.webrtc.services.v1.UnpublishTrackResponse> getUnpublishTrackMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UnpublishTrack",
      requestType = com.udb.core.webrtc.services.v1.UnpublishTrackRequest.class,
      responseType = com.udb.core.webrtc.services.v1.UnpublishTrackResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.UnpublishTrackRequest,
      com.udb.core.webrtc.services.v1.UnpublishTrackResponse> getUnpublishTrackMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.UnpublishTrackRequest, com.udb.core.webrtc.services.v1.UnpublishTrackResponse> getUnpublishTrackMethod;
    if ((getUnpublishTrackMethod = TrackServiceGrpc.getUnpublishTrackMethod) == null) {
      synchronized (TrackServiceGrpc.class) {
        if ((getUnpublishTrackMethod = TrackServiceGrpc.getUnpublishTrackMethod) == null) {
          TrackServiceGrpc.getUnpublishTrackMethod = getUnpublishTrackMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webrtc.services.v1.UnpublishTrackRequest, com.udb.core.webrtc.services.v1.UnpublishTrackResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UnpublishTrack"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.UnpublishTrackRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.UnpublishTrackResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TrackServiceMethodDescriptorSupplier("UnpublishTrack"))
              .build();
        }
      }
    }
    return getUnpublishTrackMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.MuteTrackRequest,
      com.udb.core.webrtc.services.v1.MuteTrackResponse> getMuteTrackMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MuteTrack",
      requestType = com.udb.core.webrtc.services.v1.MuteTrackRequest.class,
      responseType = com.udb.core.webrtc.services.v1.MuteTrackResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.MuteTrackRequest,
      com.udb.core.webrtc.services.v1.MuteTrackResponse> getMuteTrackMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.MuteTrackRequest, com.udb.core.webrtc.services.v1.MuteTrackResponse> getMuteTrackMethod;
    if ((getMuteTrackMethod = TrackServiceGrpc.getMuteTrackMethod) == null) {
      synchronized (TrackServiceGrpc.class) {
        if ((getMuteTrackMethod = TrackServiceGrpc.getMuteTrackMethod) == null) {
          TrackServiceGrpc.getMuteTrackMethod = getMuteTrackMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webrtc.services.v1.MuteTrackRequest, com.udb.core.webrtc.services.v1.MuteTrackResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MuteTrack"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.MuteTrackRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.MuteTrackResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TrackServiceMethodDescriptorSupplier("MuteTrack"))
              .build();
        }
      }
    }
    return getMuteTrackMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.ListTracksRequest,
      com.udb.core.webrtc.services.v1.ListTracksResponse> getListTracksMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListTracks",
      requestType = com.udb.core.webrtc.services.v1.ListTracksRequest.class,
      responseType = com.udb.core.webrtc.services.v1.ListTracksResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.ListTracksRequest,
      com.udb.core.webrtc.services.v1.ListTracksResponse> getListTracksMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.ListTracksRequest, com.udb.core.webrtc.services.v1.ListTracksResponse> getListTracksMethod;
    if ((getListTracksMethod = TrackServiceGrpc.getListTracksMethod) == null) {
      synchronized (TrackServiceGrpc.class) {
        if ((getListTracksMethod = TrackServiceGrpc.getListTracksMethod) == null) {
          TrackServiceGrpc.getListTracksMethod = getListTracksMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webrtc.services.v1.ListTracksRequest, com.udb.core.webrtc.services.v1.ListTracksResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListTracks"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.ListTracksRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.ListTracksResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TrackServiceMethodDescriptorSupplier("ListTracks"))
              .build();
        }
      }
    }
    return getListTracksMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TrackServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TrackServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TrackServiceStub>() {
        @java.lang.Override
        public TrackServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TrackServiceStub(channel, callOptions);
        }
      };
    return TrackServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static TrackServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TrackServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TrackServiceBlockingV2Stub>() {
        @java.lang.Override
        public TrackServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TrackServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return TrackServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TrackServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TrackServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TrackServiceBlockingStub>() {
        @java.lang.Override
        public TrackServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TrackServiceBlockingStub(channel, callOptions);
        }
      };
    return TrackServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TrackServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TrackServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TrackServiceFutureStub>() {
        @java.lang.Override
        public TrackServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TrackServiceFutureStub(channel, callOptions);
        }
      };
    return TrackServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     * <pre>
     * Publish a track
     * </pre>
     */
    default void publishTrack(com.udb.core.webrtc.services.v1.PublishTrackRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.PublishTrackResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPublishTrackMethod(), responseObserver);
    }

    /**
     * <pre>
     * Unpublish a track
     * </pre>
     */
    default void unpublishTrack(com.udb.core.webrtc.services.v1.UnpublishTrackRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.UnpublishTrackResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUnpublishTrackMethod(), responseObserver);
    }

    /**
     * <pre>
     * Mute or unmute a track
     * </pre>
     */
    default void muteTrack(com.udb.core.webrtc.services.v1.MuteTrackRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.MuteTrackResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMuteTrackMethod(), responseObserver);
    }

    /**
     * <pre>
     * List tracks
     * </pre>
     */
    default void listTracks(com.udb.core.webrtc.services.v1.ListTracksRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.ListTracksResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListTracksMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service TrackService.
   */
  public static abstract class TrackServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return TrackServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service TrackService.
   */
  public static final class TrackServiceStub
      extends io.grpc.stub.AbstractAsyncStub<TrackServiceStub> {
    private TrackServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TrackServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TrackServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Publish a track
     * </pre>
     */
    public void publishTrack(com.udb.core.webrtc.services.v1.PublishTrackRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.PublishTrackResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPublishTrackMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Unpublish a track
     * </pre>
     */
    public void unpublishTrack(com.udb.core.webrtc.services.v1.UnpublishTrackRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.UnpublishTrackResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUnpublishTrackMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Mute or unmute a track
     * </pre>
     */
    public void muteTrack(com.udb.core.webrtc.services.v1.MuteTrackRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.MuteTrackResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMuteTrackMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List tracks
     * </pre>
     */
    public void listTracks(com.udb.core.webrtc.services.v1.ListTracksRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.ListTracksResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListTracksMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service TrackService.
   */
  public static final class TrackServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<TrackServiceBlockingV2Stub> {
    private TrackServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TrackServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TrackServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Publish a track
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.PublishTrackResponse publishTrack(com.udb.core.webrtc.services.v1.PublishTrackRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPublishTrackMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Unpublish a track
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.UnpublishTrackResponse unpublishTrack(com.udb.core.webrtc.services.v1.UnpublishTrackRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getUnpublishTrackMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Mute or unmute a track
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.MuteTrackResponse muteTrack(com.udb.core.webrtc.services.v1.MuteTrackRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getMuteTrackMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List tracks
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.ListTracksResponse listTracks(com.udb.core.webrtc.services.v1.ListTracksRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListTracksMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service TrackService.
   */
  public static final class TrackServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<TrackServiceBlockingStub> {
    private TrackServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TrackServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TrackServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Publish a track
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.PublishTrackResponse publishTrack(com.udb.core.webrtc.services.v1.PublishTrackRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPublishTrackMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Unpublish a track
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.UnpublishTrackResponse unpublishTrack(com.udb.core.webrtc.services.v1.UnpublishTrackRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUnpublishTrackMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Mute or unmute a track
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.MuteTrackResponse muteTrack(com.udb.core.webrtc.services.v1.MuteTrackRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMuteTrackMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List tracks
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.ListTracksResponse listTracks(com.udb.core.webrtc.services.v1.ListTracksRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListTracksMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service TrackService.
   */
  public static final class TrackServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<TrackServiceFutureStub> {
    private TrackServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TrackServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TrackServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Publish a track
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webrtc.services.v1.PublishTrackResponse> publishTrack(
        com.udb.core.webrtc.services.v1.PublishTrackRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPublishTrackMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Unpublish a track
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webrtc.services.v1.UnpublishTrackResponse> unpublishTrack(
        com.udb.core.webrtc.services.v1.UnpublishTrackRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUnpublishTrackMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Mute or unmute a track
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webrtc.services.v1.MuteTrackResponse> muteTrack(
        com.udb.core.webrtc.services.v1.MuteTrackRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMuteTrackMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List tracks
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webrtc.services.v1.ListTracksResponse> listTracks(
        com.udb.core.webrtc.services.v1.ListTracksRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListTracksMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_PUBLISH_TRACK = 0;
  private static final int METHODID_UNPUBLISH_TRACK = 1;
  private static final int METHODID_MUTE_TRACK = 2;
  private static final int METHODID_LIST_TRACKS = 3;

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
        case METHODID_PUBLISH_TRACK:
          serviceImpl.publishTrack((com.udb.core.webrtc.services.v1.PublishTrackRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.PublishTrackResponse>) responseObserver);
          break;
        case METHODID_UNPUBLISH_TRACK:
          serviceImpl.unpublishTrack((com.udb.core.webrtc.services.v1.UnpublishTrackRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.UnpublishTrackResponse>) responseObserver);
          break;
        case METHODID_MUTE_TRACK:
          serviceImpl.muteTrack((com.udb.core.webrtc.services.v1.MuteTrackRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.MuteTrackResponse>) responseObserver);
          break;
        case METHODID_LIST_TRACKS:
          serviceImpl.listTracks((com.udb.core.webrtc.services.v1.ListTracksRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.ListTracksResponse>) responseObserver);
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
          getPublishTrackMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webrtc.services.v1.PublishTrackRequest,
              com.udb.core.webrtc.services.v1.PublishTrackResponse>(
                service, METHODID_PUBLISH_TRACK)))
        .addMethod(
          getUnpublishTrackMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webrtc.services.v1.UnpublishTrackRequest,
              com.udb.core.webrtc.services.v1.UnpublishTrackResponse>(
                service, METHODID_UNPUBLISH_TRACK)))
        .addMethod(
          getMuteTrackMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webrtc.services.v1.MuteTrackRequest,
              com.udb.core.webrtc.services.v1.MuteTrackResponse>(
                service, METHODID_MUTE_TRACK)))
        .addMethod(
          getListTracksMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webrtc.services.v1.ListTracksRequest,
              com.udb.core.webrtc.services.v1.ListTracksResponse>(
                service, METHODID_LIST_TRACKS)))
        .build();
  }

  private static abstract class TrackServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TrackServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.webrtc.services.v1.WebrtcServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TrackService");
    }
  }

  private static final class TrackServiceFileDescriptorSupplier
      extends TrackServiceBaseDescriptorSupplier {
    TrackServiceFileDescriptorSupplier() {}
  }

  private static final class TrackServiceMethodDescriptorSupplier
      extends TrackServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    TrackServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (TrackServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TrackServiceFileDescriptorSupplier())
              .addMethod(getPublishTrackMethod())
              .addMethod(getUnpublishTrackMethod())
              .addMethod(getMuteTrackMethod())
              .addMethod(getListTracksMethod())
              .build();
        }
      }
    }
    return result;
  }
}
