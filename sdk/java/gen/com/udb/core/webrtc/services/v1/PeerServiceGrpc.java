package com.udb.core.webrtc.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class PeerServiceGrpc {

  private PeerServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.webrtc.services.v1.PeerService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.JoinRoomRequest,
      com.udb.core.webrtc.services.v1.JoinRoomResponse> getJoinRoomMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "JoinRoom",
      requestType = com.udb.core.webrtc.services.v1.JoinRoomRequest.class,
      responseType = com.udb.core.webrtc.services.v1.JoinRoomResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.JoinRoomRequest,
      com.udb.core.webrtc.services.v1.JoinRoomResponse> getJoinRoomMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.JoinRoomRequest, com.udb.core.webrtc.services.v1.JoinRoomResponse> getJoinRoomMethod;
    if ((getJoinRoomMethod = PeerServiceGrpc.getJoinRoomMethod) == null) {
      synchronized (PeerServiceGrpc.class) {
        if ((getJoinRoomMethod = PeerServiceGrpc.getJoinRoomMethod) == null) {
          PeerServiceGrpc.getJoinRoomMethod = getJoinRoomMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webrtc.services.v1.JoinRoomRequest, com.udb.core.webrtc.services.v1.JoinRoomResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "JoinRoom"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.JoinRoomRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.JoinRoomResponse.getDefaultInstance()))
              .setSchemaDescriptor(new PeerServiceMethodDescriptorSupplier("JoinRoom"))
              .build();
        }
      }
    }
    return getJoinRoomMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.JoinSessionRequest,
      com.udb.core.webrtc.services.v1.JoinSessionResponse> getJoinSessionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "JoinSession",
      requestType = com.udb.core.webrtc.services.v1.JoinSessionRequest.class,
      responseType = com.udb.core.webrtc.services.v1.JoinSessionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.JoinSessionRequest,
      com.udb.core.webrtc.services.v1.JoinSessionResponse> getJoinSessionMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.JoinSessionRequest, com.udb.core.webrtc.services.v1.JoinSessionResponse> getJoinSessionMethod;
    if ((getJoinSessionMethod = PeerServiceGrpc.getJoinSessionMethod) == null) {
      synchronized (PeerServiceGrpc.class) {
        if ((getJoinSessionMethod = PeerServiceGrpc.getJoinSessionMethod) == null) {
          PeerServiceGrpc.getJoinSessionMethod = getJoinSessionMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webrtc.services.v1.JoinSessionRequest, com.udb.core.webrtc.services.v1.JoinSessionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "JoinSession"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.JoinSessionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.JoinSessionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new PeerServiceMethodDescriptorSupplier("JoinSession"))
              .build();
        }
      }
    }
    return getJoinSessionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.LeaveRoomRequest,
      com.udb.core.webrtc.services.v1.LeaveRoomResponse> getLeaveRoomMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "LeaveRoom",
      requestType = com.udb.core.webrtc.services.v1.LeaveRoomRequest.class,
      responseType = com.udb.core.webrtc.services.v1.LeaveRoomResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.LeaveRoomRequest,
      com.udb.core.webrtc.services.v1.LeaveRoomResponse> getLeaveRoomMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.LeaveRoomRequest, com.udb.core.webrtc.services.v1.LeaveRoomResponse> getLeaveRoomMethod;
    if ((getLeaveRoomMethod = PeerServiceGrpc.getLeaveRoomMethod) == null) {
      synchronized (PeerServiceGrpc.class) {
        if ((getLeaveRoomMethod = PeerServiceGrpc.getLeaveRoomMethod) == null) {
          PeerServiceGrpc.getLeaveRoomMethod = getLeaveRoomMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webrtc.services.v1.LeaveRoomRequest, com.udb.core.webrtc.services.v1.LeaveRoomResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "LeaveRoom"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.LeaveRoomRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.LeaveRoomResponse.getDefaultInstance()))
              .setSchemaDescriptor(new PeerServiceMethodDescriptorSupplier("LeaveRoom"))
              .build();
        }
      }
    }
    return getLeaveRoomMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.GetPeerRequest,
      com.udb.core.webrtc.services.v1.GetPeerResponse> getGetPeerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPeer",
      requestType = com.udb.core.webrtc.services.v1.GetPeerRequest.class,
      responseType = com.udb.core.webrtc.services.v1.GetPeerResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.GetPeerRequest,
      com.udb.core.webrtc.services.v1.GetPeerResponse> getGetPeerMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.GetPeerRequest, com.udb.core.webrtc.services.v1.GetPeerResponse> getGetPeerMethod;
    if ((getGetPeerMethod = PeerServiceGrpc.getGetPeerMethod) == null) {
      synchronized (PeerServiceGrpc.class) {
        if ((getGetPeerMethod = PeerServiceGrpc.getGetPeerMethod) == null) {
          PeerServiceGrpc.getGetPeerMethod = getGetPeerMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webrtc.services.v1.GetPeerRequest, com.udb.core.webrtc.services.v1.GetPeerResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetPeer"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.GetPeerRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.GetPeerResponse.getDefaultInstance()))
              .setSchemaDescriptor(new PeerServiceMethodDescriptorSupplier("GetPeer"))
              .build();
        }
      }
    }
    return getGetPeerMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.ListPeersRequest,
      com.udb.core.webrtc.services.v1.ListPeersResponse> getListPeersMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListPeers",
      requestType = com.udb.core.webrtc.services.v1.ListPeersRequest.class,
      responseType = com.udb.core.webrtc.services.v1.ListPeersResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.ListPeersRequest,
      com.udb.core.webrtc.services.v1.ListPeersResponse> getListPeersMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.ListPeersRequest, com.udb.core.webrtc.services.v1.ListPeersResponse> getListPeersMethod;
    if ((getListPeersMethod = PeerServiceGrpc.getListPeersMethod) == null) {
      synchronized (PeerServiceGrpc.class) {
        if ((getListPeersMethod = PeerServiceGrpc.getListPeersMethod) == null) {
          PeerServiceGrpc.getListPeersMethod = getListPeersMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webrtc.services.v1.ListPeersRequest, com.udb.core.webrtc.services.v1.ListPeersResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListPeers"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.ListPeersRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.ListPeersResponse.getDefaultInstance()))
              .setSchemaDescriptor(new PeerServiceMethodDescriptorSupplier("ListPeers"))
              .build();
        }
      }
    }
    return getListPeersMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static PeerServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PeerServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PeerServiceStub>() {
        @java.lang.Override
        public PeerServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PeerServiceStub(channel, callOptions);
        }
      };
    return PeerServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static PeerServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PeerServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PeerServiceBlockingV2Stub>() {
        @java.lang.Override
        public PeerServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PeerServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return PeerServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static PeerServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PeerServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PeerServiceBlockingStub>() {
        @java.lang.Override
        public PeerServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PeerServiceBlockingStub(channel, callOptions);
        }
      };
    return PeerServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static PeerServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PeerServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PeerServiceFutureStub>() {
        @java.lang.Override
        public PeerServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PeerServiceFutureStub(channel, callOptions);
        }
      };
    return PeerServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     * <pre>
     * Join a room
     * </pre>
     */
    default void joinRoom(com.udb.core.webrtc.services.v1.JoinRoomRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.JoinRoomResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getJoinRoomMethod(), responseObserver);
    }

    /**
     * <pre>
     * Join a room and atomically mint TURN credentials for the freshly-inserted peer
     * </pre>
     */
    default void joinSession(com.udb.core.webrtc.services.v1.JoinSessionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.JoinSessionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getJoinSessionMethod(), responseObserver);
    }

    /**
     * <pre>
     * Leave a room
     * </pre>
     */
    default void leaveRoom(com.udb.core.webrtc.services.v1.LeaveRoomRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.LeaveRoomResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getLeaveRoomMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get a peer
     * </pre>
     */
    default void getPeer(com.udb.core.webrtc.services.v1.GetPeerRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.GetPeerResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetPeerMethod(), responseObserver);
    }

    /**
     * <pre>
     * List peers
     * </pre>
     */
    default void listPeers(com.udb.core.webrtc.services.v1.ListPeersRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.ListPeersResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListPeersMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service PeerService.
   */
  public static abstract class PeerServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return PeerServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service PeerService.
   */
  public static final class PeerServiceStub
      extends io.grpc.stub.AbstractAsyncStub<PeerServiceStub> {
    private PeerServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PeerServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PeerServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Join a room
     * </pre>
     */
    public void joinRoom(com.udb.core.webrtc.services.v1.JoinRoomRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.JoinRoomResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getJoinRoomMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Join a room and atomically mint TURN credentials for the freshly-inserted peer
     * </pre>
     */
    public void joinSession(com.udb.core.webrtc.services.v1.JoinSessionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.JoinSessionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getJoinSessionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Leave a room
     * </pre>
     */
    public void leaveRoom(com.udb.core.webrtc.services.v1.LeaveRoomRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.LeaveRoomResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getLeaveRoomMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get a peer
     * </pre>
     */
    public void getPeer(com.udb.core.webrtc.services.v1.GetPeerRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.GetPeerResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetPeerMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List peers
     * </pre>
     */
    public void listPeers(com.udb.core.webrtc.services.v1.ListPeersRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.ListPeersResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListPeersMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service PeerService.
   */
  public static final class PeerServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<PeerServiceBlockingV2Stub> {
    private PeerServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PeerServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PeerServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Join a room
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.JoinRoomResponse joinRoom(com.udb.core.webrtc.services.v1.JoinRoomRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getJoinRoomMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Join a room and atomically mint TURN credentials for the freshly-inserted peer
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.JoinSessionResponse joinSession(com.udb.core.webrtc.services.v1.JoinSessionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getJoinSessionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Leave a room
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.LeaveRoomResponse leaveRoom(com.udb.core.webrtc.services.v1.LeaveRoomRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getLeaveRoomMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a peer
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.GetPeerResponse getPeer(com.udb.core.webrtc.services.v1.GetPeerRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetPeerMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List peers
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.ListPeersResponse listPeers(com.udb.core.webrtc.services.v1.ListPeersRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListPeersMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service PeerService.
   */
  public static final class PeerServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<PeerServiceBlockingStub> {
    private PeerServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PeerServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PeerServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Join a room
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.JoinRoomResponse joinRoom(com.udb.core.webrtc.services.v1.JoinRoomRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getJoinRoomMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Join a room and atomically mint TURN credentials for the freshly-inserted peer
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.JoinSessionResponse joinSession(com.udb.core.webrtc.services.v1.JoinSessionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getJoinSessionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Leave a room
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.LeaveRoomResponse leaveRoom(com.udb.core.webrtc.services.v1.LeaveRoomRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getLeaveRoomMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a peer
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.GetPeerResponse getPeer(com.udb.core.webrtc.services.v1.GetPeerRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetPeerMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List peers
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.ListPeersResponse listPeers(com.udb.core.webrtc.services.v1.ListPeersRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListPeersMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service PeerService.
   */
  public static final class PeerServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<PeerServiceFutureStub> {
    private PeerServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PeerServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PeerServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Join a room
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webrtc.services.v1.JoinRoomResponse> joinRoom(
        com.udb.core.webrtc.services.v1.JoinRoomRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getJoinRoomMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Join a room and atomically mint TURN credentials for the freshly-inserted peer
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webrtc.services.v1.JoinSessionResponse> joinSession(
        com.udb.core.webrtc.services.v1.JoinSessionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getJoinSessionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Leave a room
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webrtc.services.v1.LeaveRoomResponse> leaveRoom(
        com.udb.core.webrtc.services.v1.LeaveRoomRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getLeaveRoomMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get a peer
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webrtc.services.v1.GetPeerResponse> getPeer(
        com.udb.core.webrtc.services.v1.GetPeerRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetPeerMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List peers
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webrtc.services.v1.ListPeersResponse> listPeers(
        com.udb.core.webrtc.services.v1.ListPeersRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListPeersMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_JOIN_ROOM = 0;
  private static final int METHODID_JOIN_SESSION = 1;
  private static final int METHODID_LEAVE_ROOM = 2;
  private static final int METHODID_GET_PEER = 3;
  private static final int METHODID_LIST_PEERS = 4;

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
        case METHODID_JOIN_ROOM:
          serviceImpl.joinRoom((com.udb.core.webrtc.services.v1.JoinRoomRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.JoinRoomResponse>) responseObserver);
          break;
        case METHODID_JOIN_SESSION:
          serviceImpl.joinSession((com.udb.core.webrtc.services.v1.JoinSessionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.JoinSessionResponse>) responseObserver);
          break;
        case METHODID_LEAVE_ROOM:
          serviceImpl.leaveRoom((com.udb.core.webrtc.services.v1.LeaveRoomRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.LeaveRoomResponse>) responseObserver);
          break;
        case METHODID_GET_PEER:
          serviceImpl.getPeer((com.udb.core.webrtc.services.v1.GetPeerRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.GetPeerResponse>) responseObserver);
          break;
        case METHODID_LIST_PEERS:
          serviceImpl.listPeers((com.udb.core.webrtc.services.v1.ListPeersRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.ListPeersResponse>) responseObserver);
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
          getJoinRoomMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webrtc.services.v1.JoinRoomRequest,
              com.udb.core.webrtc.services.v1.JoinRoomResponse>(
                service, METHODID_JOIN_ROOM)))
        .addMethod(
          getJoinSessionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webrtc.services.v1.JoinSessionRequest,
              com.udb.core.webrtc.services.v1.JoinSessionResponse>(
                service, METHODID_JOIN_SESSION)))
        .addMethod(
          getLeaveRoomMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webrtc.services.v1.LeaveRoomRequest,
              com.udb.core.webrtc.services.v1.LeaveRoomResponse>(
                service, METHODID_LEAVE_ROOM)))
        .addMethod(
          getGetPeerMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webrtc.services.v1.GetPeerRequest,
              com.udb.core.webrtc.services.v1.GetPeerResponse>(
                service, METHODID_GET_PEER)))
        .addMethod(
          getListPeersMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webrtc.services.v1.ListPeersRequest,
              com.udb.core.webrtc.services.v1.ListPeersResponse>(
                service, METHODID_LIST_PEERS)))
        .build();
  }

  private static abstract class PeerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    PeerServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.webrtc.services.v1.WebrtcServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("PeerService");
    }
  }

  private static final class PeerServiceFileDescriptorSupplier
      extends PeerServiceBaseDescriptorSupplier {
    PeerServiceFileDescriptorSupplier() {}
  }

  private static final class PeerServiceMethodDescriptorSupplier
      extends PeerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    PeerServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (PeerServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new PeerServiceFileDescriptorSupplier())
              .addMethod(getJoinRoomMethod())
              .addMethod(getJoinSessionMethod())
              .addMethod(getLeaveRoomMethod())
              .addMethod(getGetPeerMethod())
              .addMethod(getListPeersMethod())
              .build();
        }
      }
    }
    return result;
  }
}
