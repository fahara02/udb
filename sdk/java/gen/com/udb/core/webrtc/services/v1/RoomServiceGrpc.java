package com.udb.core.webrtc.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class RoomServiceGrpc {

  private RoomServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.webrtc.services.v1.RoomService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.CreateRoomRequest,
      com.udb.core.webrtc.services.v1.CreateRoomResponse> getCreateRoomMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateRoom",
      requestType = com.udb.core.webrtc.services.v1.CreateRoomRequest.class,
      responseType = com.udb.core.webrtc.services.v1.CreateRoomResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.CreateRoomRequest,
      com.udb.core.webrtc.services.v1.CreateRoomResponse> getCreateRoomMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.CreateRoomRequest, com.udb.core.webrtc.services.v1.CreateRoomResponse> getCreateRoomMethod;
    if ((getCreateRoomMethod = RoomServiceGrpc.getCreateRoomMethod) == null) {
      synchronized (RoomServiceGrpc.class) {
        if ((getCreateRoomMethod = RoomServiceGrpc.getCreateRoomMethod) == null) {
          RoomServiceGrpc.getCreateRoomMethod = getCreateRoomMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webrtc.services.v1.CreateRoomRequest, com.udb.core.webrtc.services.v1.CreateRoomResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateRoom"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.CreateRoomRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.CreateRoomResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RoomServiceMethodDescriptorSupplier("CreateRoom"))
              .build();
        }
      }
    }
    return getCreateRoomMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.GetRoomRequest,
      com.udb.core.webrtc.services.v1.GetRoomResponse> getGetRoomMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetRoom",
      requestType = com.udb.core.webrtc.services.v1.GetRoomRequest.class,
      responseType = com.udb.core.webrtc.services.v1.GetRoomResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.GetRoomRequest,
      com.udb.core.webrtc.services.v1.GetRoomResponse> getGetRoomMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.GetRoomRequest, com.udb.core.webrtc.services.v1.GetRoomResponse> getGetRoomMethod;
    if ((getGetRoomMethod = RoomServiceGrpc.getGetRoomMethod) == null) {
      synchronized (RoomServiceGrpc.class) {
        if ((getGetRoomMethod = RoomServiceGrpc.getGetRoomMethod) == null) {
          RoomServiceGrpc.getGetRoomMethod = getGetRoomMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webrtc.services.v1.GetRoomRequest, com.udb.core.webrtc.services.v1.GetRoomResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetRoom"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.GetRoomRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.GetRoomResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RoomServiceMethodDescriptorSupplier("GetRoom"))
              .build();
        }
      }
    }
    return getGetRoomMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.UpdateRoomRequest,
      com.udb.core.webrtc.services.v1.UpdateRoomResponse> getUpdateRoomMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateRoom",
      requestType = com.udb.core.webrtc.services.v1.UpdateRoomRequest.class,
      responseType = com.udb.core.webrtc.services.v1.UpdateRoomResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.UpdateRoomRequest,
      com.udb.core.webrtc.services.v1.UpdateRoomResponse> getUpdateRoomMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.UpdateRoomRequest, com.udb.core.webrtc.services.v1.UpdateRoomResponse> getUpdateRoomMethod;
    if ((getUpdateRoomMethod = RoomServiceGrpc.getUpdateRoomMethod) == null) {
      synchronized (RoomServiceGrpc.class) {
        if ((getUpdateRoomMethod = RoomServiceGrpc.getUpdateRoomMethod) == null) {
          RoomServiceGrpc.getUpdateRoomMethod = getUpdateRoomMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webrtc.services.v1.UpdateRoomRequest, com.udb.core.webrtc.services.v1.UpdateRoomResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateRoom"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.UpdateRoomRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.UpdateRoomResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RoomServiceMethodDescriptorSupplier("UpdateRoom"))
              .build();
        }
      }
    }
    return getUpdateRoomMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.CloseRoomRequest,
      com.udb.core.webrtc.services.v1.CloseRoomResponse> getCloseRoomMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CloseRoom",
      requestType = com.udb.core.webrtc.services.v1.CloseRoomRequest.class,
      responseType = com.udb.core.webrtc.services.v1.CloseRoomResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.CloseRoomRequest,
      com.udb.core.webrtc.services.v1.CloseRoomResponse> getCloseRoomMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.CloseRoomRequest, com.udb.core.webrtc.services.v1.CloseRoomResponse> getCloseRoomMethod;
    if ((getCloseRoomMethod = RoomServiceGrpc.getCloseRoomMethod) == null) {
      synchronized (RoomServiceGrpc.class) {
        if ((getCloseRoomMethod = RoomServiceGrpc.getCloseRoomMethod) == null) {
          RoomServiceGrpc.getCloseRoomMethod = getCloseRoomMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webrtc.services.v1.CloseRoomRequest, com.udb.core.webrtc.services.v1.CloseRoomResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CloseRoom"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.CloseRoomRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.CloseRoomResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RoomServiceMethodDescriptorSupplier("CloseRoom"))
              .build();
        }
      }
    }
    return getCloseRoomMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.ListRoomsRequest,
      com.udb.core.webrtc.services.v1.ListRoomsResponse> getListRoomsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListRooms",
      requestType = com.udb.core.webrtc.services.v1.ListRoomsRequest.class,
      responseType = com.udb.core.webrtc.services.v1.ListRoomsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.ListRoomsRequest,
      com.udb.core.webrtc.services.v1.ListRoomsResponse> getListRoomsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webrtc.services.v1.ListRoomsRequest, com.udb.core.webrtc.services.v1.ListRoomsResponse> getListRoomsMethod;
    if ((getListRoomsMethod = RoomServiceGrpc.getListRoomsMethod) == null) {
      synchronized (RoomServiceGrpc.class) {
        if ((getListRoomsMethod = RoomServiceGrpc.getListRoomsMethod) == null) {
          RoomServiceGrpc.getListRoomsMethod = getListRoomsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webrtc.services.v1.ListRoomsRequest, com.udb.core.webrtc.services.v1.ListRoomsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListRooms"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.ListRoomsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webrtc.services.v1.ListRoomsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RoomServiceMethodDescriptorSupplier("ListRooms"))
              .build();
        }
      }
    }
    return getListRoomsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RoomServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RoomServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RoomServiceStub>() {
        @java.lang.Override
        public RoomServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RoomServiceStub(channel, callOptions);
        }
      };
    return RoomServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static RoomServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RoomServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RoomServiceBlockingV2Stub>() {
        @java.lang.Override
        public RoomServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RoomServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return RoomServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RoomServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RoomServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RoomServiceBlockingStub>() {
        @java.lang.Override
        public RoomServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RoomServiceBlockingStub(channel, callOptions);
        }
      };
    return RoomServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RoomServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RoomServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RoomServiceFutureStub>() {
        @java.lang.Override
        public RoomServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RoomServiceFutureStub(channel, callOptions);
        }
      };
    return RoomServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     * <pre>
     * Create a room
     * </pre>
     */
    default void createRoom(com.udb.core.webrtc.services.v1.CreateRoomRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.CreateRoomResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateRoomMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get a room
     * </pre>
     */
    default void getRoom(com.udb.core.webrtc.services.v1.GetRoomRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.GetRoomResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetRoomMethod(), responseObserver);
    }

    /**
     * <pre>
     * Update a room
     * </pre>
     */
    default void updateRoom(com.udb.core.webrtc.services.v1.UpdateRoomRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.UpdateRoomResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateRoomMethod(), responseObserver);
    }

    /**
     * <pre>
     * Close a room
     * </pre>
     */
    default void closeRoom(com.udb.core.webrtc.services.v1.CloseRoomRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.CloseRoomResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCloseRoomMethod(), responseObserver);
    }

    /**
     * <pre>
     * List rooms
     * </pre>
     */
    default void listRooms(com.udb.core.webrtc.services.v1.ListRoomsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.ListRoomsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListRoomsMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service RoomService.
   */
  public static abstract class RoomServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return RoomServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service RoomService.
   */
  public static final class RoomServiceStub
      extends io.grpc.stub.AbstractAsyncStub<RoomServiceStub> {
    private RoomServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RoomServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RoomServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Create a room
     * </pre>
     */
    public void createRoom(com.udb.core.webrtc.services.v1.CreateRoomRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.CreateRoomResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateRoomMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get a room
     * </pre>
     */
    public void getRoom(com.udb.core.webrtc.services.v1.GetRoomRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.GetRoomResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetRoomMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Update a room
     * </pre>
     */
    public void updateRoom(com.udb.core.webrtc.services.v1.UpdateRoomRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.UpdateRoomResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateRoomMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Close a room
     * </pre>
     */
    public void closeRoom(com.udb.core.webrtc.services.v1.CloseRoomRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.CloseRoomResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCloseRoomMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List rooms
     * </pre>
     */
    public void listRooms(com.udb.core.webrtc.services.v1.ListRoomsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.ListRoomsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListRoomsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service RoomService.
   */
  public static final class RoomServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<RoomServiceBlockingV2Stub> {
    private RoomServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RoomServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RoomServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Create a room
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.CreateRoomResponse createRoom(com.udb.core.webrtc.services.v1.CreateRoomRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreateRoomMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a room
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.GetRoomResponse getRoom(com.udb.core.webrtc.services.v1.GetRoomRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetRoomMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Update a room
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.UpdateRoomResponse updateRoom(com.udb.core.webrtc.services.v1.UpdateRoomRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getUpdateRoomMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Close a room
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.CloseRoomResponse closeRoom(com.udb.core.webrtc.services.v1.CloseRoomRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCloseRoomMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List rooms
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.ListRoomsResponse listRooms(com.udb.core.webrtc.services.v1.ListRoomsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListRoomsMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service RoomService.
   */
  public static final class RoomServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<RoomServiceBlockingStub> {
    private RoomServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RoomServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RoomServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Create a room
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.CreateRoomResponse createRoom(com.udb.core.webrtc.services.v1.CreateRoomRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateRoomMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a room
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.GetRoomResponse getRoom(com.udb.core.webrtc.services.v1.GetRoomRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetRoomMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Update a room
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.UpdateRoomResponse updateRoom(com.udb.core.webrtc.services.v1.UpdateRoomRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateRoomMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Close a room
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.CloseRoomResponse closeRoom(com.udb.core.webrtc.services.v1.CloseRoomRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCloseRoomMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List rooms
     * </pre>
     */
    public com.udb.core.webrtc.services.v1.ListRoomsResponse listRooms(com.udb.core.webrtc.services.v1.ListRoomsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListRoomsMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service RoomService.
   */
  public static final class RoomServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<RoomServiceFutureStub> {
    private RoomServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RoomServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RoomServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Create a room
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webrtc.services.v1.CreateRoomResponse> createRoom(
        com.udb.core.webrtc.services.v1.CreateRoomRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateRoomMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get a room
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webrtc.services.v1.GetRoomResponse> getRoom(
        com.udb.core.webrtc.services.v1.GetRoomRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetRoomMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Update a room
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webrtc.services.v1.UpdateRoomResponse> updateRoom(
        com.udb.core.webrtc.services.v1.UpdateRoomRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateRoomMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Close a room
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webrtc.services.v1.CloseRoomResponse> closeRoom(
        com.udb.core.webrtc.services.v1.CloseRoomRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCloseRoomMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List rooms
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webrtc.services.v1.ListRoomsResponse> listRooms(
        com.udb.core.webrtc.services.v1.ListRoomsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListRoomsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_ROOM = 0;
  private static final int METHODID_GET_ROOM = 1;
  private static final int METHODID_UPDATE_ROOM = 2;
  private static final int METHODID_CLOSE_ROOM = 3;
  private static final int METHODID_LIST_ROOMS = 4;

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
        case METHODID_CREATE_ROOM:
          serviceImpl.createRoom((com.udb.core.webrtc.services.v1.CreateRoomRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.CreateRoomResponse>) responseObserver);
          break;
        case METHODID_GET_ROOM:
          serviceImpl.getRoom((com.udb.core.webrtc.services.v1.GetRoomRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.GetRoomResponse>) responseObserver);
          break;
        case METHODID_UPDATE_ROOM:
          serviceImpl.updateRoom((com.udb.core.webrtc.services.v1.UpdateRoomRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.UpdateRoomResponse>) responseObserver);
          break;
        case METHODID_CLOSE_ROOM:
          serviceImpl.closeRoom((com.udb.core.webrtc.services.v1.CloseRoomRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.CloseRoomResponse>) responseObserver);
          break;
        case METHODID_LIST_ROOMS:
          serviceImpl.listRooms((com.udb.core.webrtc.services.v1.ListRoomsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webrtc.services.v1.ListRoomsResponse>) responseObserver);
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
          getCreateRoomMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webrtc.services.v1.CreateRoomRequest,
              com.udb.core.webrtc.services.v1.CreateRoomResponse>(
                service, METHODID_CREATE_ROOM)))
        .addMethod(
          getGetRoomMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webrtc.services.v1.GetRoomRequest,
              com.udb.core.webrtc.services.v1.GetRoomResponse>(
                service, METHODID_GET_ROOM)))
        .addMethod(
          getUpdateRoomMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webrtc.services.v1.UpdateRoomRequest,
              com.udb.core.webrtc.services.v1.UpdateRoomResponse>(
                service, METHODID_UPDATE_ROOM)))
        .addMethod(
          getCloseRoomMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webrtc.services.v1.CloseRoomRequest,
              com.udb.core.webrtc.services.v1.CloseRoomResponse>(
                service, METHODID_CLOSE_ROOM)))
        .addMethod(
          getListRoomsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webrtc.services.v1.ListRoomsRequest,
              com.udb.core.webrtc.services.v1.ListRoomsResponse>(
                service, METHODID_LIST_ROOMS)))
        .build();
  }

  private static abstract class RoomServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RoomServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.webrtc.services.v1.WebrtcServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("RoomService");
    }
  }

  private static final class RoomServiceFileDescriptorSupplier
      extends RoomServiceBaseDescriptorSupplier {
    RoomServiceFileDescriptorSupplier() {}
  }

  private static final class RoomServiceMethodDescriptorSupplier
      extends RoomServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    RoomServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (RoomServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RoomServiceFileDescriptorSupplier())
              .addMethod(getCreateRoomMethod())
              .addMethod(getGetRoomMethod())
              .addMethod(getUpdateRoomMethod())
              .addMethod(getCloseRoomMethod())
              .addMethod(getListRoomsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
