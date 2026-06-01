package com.udb.core.tenant.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class TenantServiceGrpc {

  private TenantServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.tenant.services.v1.TenantService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.CreateTenantRequest,
      com.udb.core.tenant.services.v1.CreateTenantResponse> getCreateTenantMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateTenant",
      requestType = com.udb.core.tenant.services.v1.CreateTenantRequest.class,
      responseType = com.udb.core.tenant.services.v1.CreateTenantResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.CreateTenantRequest,
      com.udb.core.tenant.services.v1.CreateTenantResponse> getCreateTenantMethod() {
    io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.CreateTenantRequest, com.udb.core.tenant.services.v1.CreateTenantResponse> getCreateTenantMethod;
    if ((getCreateTenantMethod = TenantServiceGrpc.getCreateTenantMethod) == null) {
      synchronized (TenantServiceGrpc.class) {
        if ((getCreateTenantMethod = TenantServiceGrpc.getCreateTenantMethod) == null) {
          TenantServiceGrpc.getCreateTenantMethod = getCreateTenantMethod =
              io.grpc.MethodDescriptor.<com.udb.core.tenant.services.v1.CreateTenantRequest, com.udb.core.tenant.services.v1.CreateTenantResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateTenant"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.tenant.services.v1.CreateTenantRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.tenant.services.v1.CreateTenantResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TenantServiceMethodDescriptorSupplier("CreateTenant"))
              .build();
        }
      }
    }
    return getCreateTenantMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.GetTenantRequest,
      com.udb.core.tenant.services.v1.GetTenantResponse> getGetTenantMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTenant",
      requestType = com.udb.core.tenant.services.v1.GetTenantRequest.class,
      responseType = com.udb.core.tenant.services.v1.GetTenantResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.GetTenantRequest,
      com.udb.core.tenant.services.v1.GetTenantResponse> getGetTenantMethod() {
    io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.GetTenantRequest, com.udb.core.tenant.services.v1.GetTenantResponse> getGetTenantMethod;
    if ((getGetTenantMethod = TenantServiceGrpc.getGetTenantMethod) == null) {
      synchronized (TenantServiceGrpc.class) {
        if ((getGetTenantMethod = TenantServiceGrpc.getGetTenantMethod) == null) {
          TenantServiceGrpc.getGetTenantMethod = getGetTenantMethod =
              io.grpc.MethodDescriptor.<com.udb.core.tenant.services.v1.GetTenantRequest, com.udb.core.tenant.services.v1.GetTenantResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTenant"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.tenant.services.v1.GetTenantRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.tenant.services.v1.GetTenantResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TenantServiceMethodDescriptorSupplier("GetTenant"))
              .build();
        }
      }
    }
    return getGetTenantMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.ListTenantsRequest,
      com.udb.core.tenant.services.v1.ListTenantsResponse> getListTenantsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListTenants",
      requestType = com.udb.core.tenant.services.v1.ListTenantsRequest.class,
      responseType = com.udb.core.tenant.services.v1.ListTenantsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.ListTenantsRequest,
      com.udb.core.tenant.services.v1.ListTenantsResponse> getListTenantsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.ListTenantsRequest, com.udb.core.tenant.services.v1.ListTenantsResponse> getListTenantsMethod;
    if ((getListTenantsMethod = TenantServiceGrpc.getListTenantsMethod) == null) {
      synchronized (TenantServiceGrpc.class) {
        if ((getListTenantsMethod = TenantServiceGrpc.getListTenantsMethod) == null) {
          TenantServiceGrpc.getListTenantsMethod = getListTenantsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.tenant.services.v1.ListTenantsRequest, com.udb.core.tenant.services.v1.ListTenantsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListTenants"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.tenant.services.v1.ListTenantsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.tenant.services.v1.ListTenantsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TenantServiceMethodDescriptorSupplier("ListTenants"))
              .build();
        }
      }
    }
    return getListTenantsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.UpdateTenantRequest,
      com.udb.core.tenant.services.v1.UpdateTenantResponse> getUpdateTenantMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateTenant",
      requestType = com.udb.core.tenant.services.v1.UpdateTenantRequest.class,
      responseType = com.udb.core.tenant.services.v1.UpdateTenantResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.UpdateTenantRequest,
      com.udb.core.tenant.services.v1.UpdateTenantResponse> getUpdateTenantMethod() {
    io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.UpdateTenantRequest, com.udb.core.tenant.services.v1.UpdateTenantResponse> getUpdateTenantMethod;
    if ((getUpdateTenantMethod = TenantServiceGrpc.getUpdateTenantMethod) == null) {
      synchronized (TenantServiceGrpc.class) {
        if ((getUpdateTenantMethod = TenantServiceGrpc.getUpdateTenantMethod) == null) {
          TenantServiceGrpc.getUpdateTenantMethod = getUpdateTenantMethod =
              io.grpc.MethodDescriptor.<com.udb.core.tenant.services.v1.UpdateTenantRequest, com.udb.core.tenant.services.v1.UpdateTenantResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateTenant"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.tenant.services.v1.UpdateTenantRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.tenant.services.v1.UpdateTenantResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TenantServiceMethodDescriptorSupplier("UpdateTenant"))
              .build();
        }
      }
    }
    return getUpdateTenantMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.GetTenantConfigRequest,
      com.udb.core.tenant.services.v1.GetTenantConfigResponse> getGetTenantConfigMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTenantConfig",
      requestType = com.udb.core.tenant.services.v1.GetTenantConfigRequest.class,
      responseType = com.udb.core.tenant.services.v1.GetTenantConfigResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.GetTenantConfigRequest,
      com.udb.core.tenant.services.v1.GetTenantConfigResponse> getGetTenantConfigMethod() {
    io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.GetTenantConfigRequest, com.udb.core.tenant.services.v1.GetTenantConfigResponse> getGetTenantConfigMethod;
    if ((getGetTenantConfigMethod = TenantServiceGrpc.getGetTenantConfigMethod) == null) {
      synchronized (TenantServiceGrpc.class) {
        if ((getGetTenantConfigMethod = TenantServiceGrpc.getGetTenantConfigMethod) == null) {
          TenantServiceGrpc.getGetTenantConfigMethod = getGetTenantConfigMethod =
              io.grpc.MethodDescriptor.<com.udb.core.tenant.services.v1.GetTenantConfigRequest, com.udb.core.tenant.services.v1.GetTenantConfigResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTenantConfig"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.tenant.services.v1.GetTenantConfigRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.tenant.services.v1.GetTenantConfigResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TenantServiceMethodDescriptorSupplier("GetTenantConfig"))
              .build();
        }
      }
    }
    return getGetTenantConfigMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.UpdateTenantConfigRequest,
      com.udb.core.tenant.services.v1.UpdateTenantConfigResponse> getUpdateTenantConfigMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateTenantConfig",
      requestType = com.udb.core.tenant.services.v1.UpdateTenantConfigRequest.class,
      responseType = com.udb.core.tenant.services.v1.UpdateTenantConfigResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.UpdateTenantConfigRequest,
      com.udb.core.tenant.services.v1.UpdateTenantConfigResponse> getUpdateTenantConfigMethod() {
    io.grpc.MethodDescriptor<com.udb.core.tenant.services.v1.UpdateTenantConfigRequest, com.udb.core.tenant.services.v1.UpdateTenantConfigResponse> getUpdateTenantConfigMethod;
    if ((getUpdateTenantConfigMethod = TenantServiceGrpc.getUpdateTenantConfigMethod) == null) {
      synchronized (TenantServiceGrpc.class) {
        if ((getUpdateTenantConfigMethod = TenantServiceGrpc.getUpdateTenantConfigMethod) == null) {
          TenantServiceGrpc.getUpdateTenantConfigMethod = getUpdateTenantConfigMethod =
              io.grpc.MethodDescriptor.<com.udb.core.tenant.services.v1.UpdateTenantConfigRequest, com.udb.core.tenant.services.v1.UpdateTenantConfigResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateTenantConfig"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.tenant.services.v1.UpdateTenantConfigRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.tenant.services.v1.UpdateTenantConfigResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TenantServiceMethodDescriptorSupplier("UpdateTenantConfig"))
              .build();
        }
      }
    }
    return getUpdateTenantConfigMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TenantServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TenantServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TenantServiceStub>() {
        @java.lang.Override
        public TenantServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TenantServiceStub(channel, callOptions);
        }
      };
    return TenantServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static TenantServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TenantServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TenantServiceBlockingV2Stub>() {
        @java.lang.Override
        public TenantServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TenantServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return TenantServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TenantServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TenantServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TenantServiceBlockingStub>() {
        @java.lang.Override
        public TenantServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TenantServiceBlockingStub(channel, callOptions);
        }
      };
    return TenantServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TenantServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TenantServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TenantServiceFutureStub>() {
        @java.lang.Override
        public TenantServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TenantServiceFutureStub(channel, callOptions);
        }
      };
    return TenantServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     * <pre>
     * Create tenant
     * </pre>
     */
    default void createTenant(com.udb.core.tenant.services.v1.CreateTenantRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.CreateTenantResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateTenantMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get tenant
     * </pre>
     */
    default void getTenant(com.udb.core.tenant.services.v1.GetTenantRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.GetTenantResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTenantMethod(), responseObserver);
    }

    /**
     * <pre>
     * List tenants
     * </pre>
     */
    default void listTenants(com.udb.core.tenant.services.v1.ListTenantsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.ListTenantsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListTenantsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Update tenant
     * </pre>
     */
    default void updateTenant(com.udb.core.tenant.services.v1.UpdateTenantRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.UpdateTenantResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateTenantMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get tenant config
     * </pre>
     */
    default void getTenantConfig(com.udb.core.tenant.services.v1.GetTenantConfigRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.GetTenantConfigResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTenantConfigMethod(), responseObserver);
    }

    /**
     * <pre>
     * Update tenant config
     * </pre>
     */
    default void updateTenantConfig(com.udb.core.tenant.services.v1.UpdateTenantConfigRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.UpdateTenantConfigResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateTenantConfigMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service TenantService.
   */
  public static abstract class TenantServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return TenantServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service TenantService.
   */
  public static final class TenantServiceStub
      extends io.grpc.stub.AbstractAsyncStub<TenantServiceStub> {
    private TenantServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TenantServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TenantServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Create tenant
     * </pre>
     */
    public void createTenant(com.udb.core.tenant.services.v1.CreateTenantRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.CreateTenantResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateTenantMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get tenant
     * </pre>
     */
    public void getTenant(com.udb.core.tenant.services.v1.GetTenantRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.GetTenantResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTenantMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List tenants
     * </pre>
     */
    public void listTenants(com.udb.core.tenant.services.v1.ListTenantsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.ListTenantsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListTenantsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Update tenant
     * </pre>
     */
    public void updateTenant(com.udb.core.tenant.services.v1.UpdateTenantRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.UpdateTenantResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateTenantMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get tenant config
     * </pre>
     */
    public void getTenantConfig(com.udb.core.tenant.services.v1.GetTenantConfigRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.GetTenantConfigResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTenantConfigMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Update tenant config
     * </pre>
     */
    public void updateTenantConfig(com.udb.core.tenant.services.v1.UpdateTenantConfigRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.UpdateTenantConfigResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateTenantConfigMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service TenantService.
   */
  public static final class TenantServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<TenantServiceBlockingV2Stub> {
    private TenantServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TenantServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TenantServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Create tenant
     * </pre>
     */
    public com.udb.core.tenant.services.v1.CreateTenantResponse createTenant(com.udb.core.tenant.services.v1.CreateTenantRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreateTenantMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get tenant
     * </pre>
     */
    public com.udb.core.tenant.services.v1.GetTenantResponse getTenant(com.udb.core.tenant.services.v1.GetTenantRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetTenantMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List tenants
     * </pre>
     */
    public com.udb.core.tenant.services.v1.ListTenantsResponse listTenants(com.udb.core.tenant.services.v1.ListTenantsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListTenantsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Update tenant
     * </pre>
     */
    public com.udb.core.tenant.services.v1.UpdateTenantResponse updateTenant(com.udb.core.tenant.services.v1.UpdateTenantRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getUpdateTenantMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get tenant config
     * </pre>
     */
    public com.udb.core.tenant.services.v1.GetTenantConfigResponse getTenantConfig(com.udb.core.tenant.services.v1.GetTenantConfigRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetTenantConfigMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Update tenant config
     * </pre>
     */
    public com.udb.core.tenant.services.v1.UpdateTenantConfigResponse updateTenantConfig(com.udb.core.tenant.services.v1.UpdateTenantConfigRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getUpdateTenantConfigMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service TenantService.
   */
  public static final class TenantServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<TenantServiceBlockingStub> {
    private TenantServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TenantServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TenantServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Create tenant
     * </pre>
     */
    public com.udb.core.tenant.services.v1.CreateTenantResponse createTenant(com.udb.core.tenant.services.v1.CreateTenantRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateTenantMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get tenant
     * </pre>
     */
    public com.udb.core.tenant.services.v1.GetTenantResponse getTenant(com.udb.core.tenant.services.v1.GetTenantRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTenantMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List tenants
     * </pre>
     */
    public com.udb.core.tenant.services.v1.ListTenantsResponse listTenants(com.udb.core.tenant.services.v1.ListTenantsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListTenantsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Update tenant
     * </pre>
     */
    public com.udb.core.tenant.services.v1.UpdateTenantResponse updateTenant(com.udb.core.tenant.services.v1.UpdateTenantRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateTenantMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get tenant config
     * </pre>
     */
    public com.udb.core.tenant.services.v1.GetTenantConfigResponse getTenantConfig(com.udb.core.tenant.services.v1.GetTenantConfigRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTenantConfigMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Update tenant config
     * </pre>
     */
    public com.udb.core.tenant.services.v1.UpdateTenantConfigResponse updateTenantConfig(com.udb.core.tenant.services.v1.UpdateTenantConfigRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateTenantConfigMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service TenantService.
   */
  public static final class TenantServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<TenantServiceFutureStub> {
    private TenantServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TenantServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TenantServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Create tenant
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.tenant.services.v1.CreateTenantResponse> createTenant(
        com.udb.core.tenant.services.v1.CreateTenantRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateTenantMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get tenant
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.tenant.services.v1.GetTenantResponse> getTenant(
        com.udb.core.tenant.services.v1.GetTenantRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTenantMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List tenants
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.tenant.services.v1.ListTenantsResponse> listTenants(
        com.udb.core.tenant.services.v1.ListTenantsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListTenantsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Update tenant
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.tenant.services.v1.UpdateTenantResponse> updateTenant(
        com.udb.core.tenant.services.v1.UpdateTenantRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateTenantMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get tenant config
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.tenant.services.v1.GetTenantConfigResponse> getTenantConfig(
        com.udb.core.tenant.services.v1.GetTenantConfigRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTenantConfigMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Update tenant config
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.tenant.services.v1.UpdateTenantConfigResponse> updateTenantConfig(
        com.udb.core.tenant.services.v1.UpdateTenantConfigRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateTenantConfigMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_TENANT = 0;
  private static final int METHODID_GET_TENANT = 1;
  private static final int METHODID_LIST_TENANTS = 2;
  private static final int METHODID_UPDATE_TENANT = 3;
  private static final int METHODID_GET_TENANT_CONFIG = 4;
  private static final int METHODID_UPDATE_TENANT_CONFIG = 5;

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
        case METHODID_CREATE_TENANT:
          serviceImpl.createTenant((com.udb.core.tenant.services.v1.CreateTenantRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.CreateTenantResponse>) responseObserver);
          break;
        case METHODID_GET_TENANT:
          serviceImpl.getTenant((com.udb.core.tenant.services.v1.GetTenantRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.GetTenantResponse>) responseObserver);
          break;
        case METHODID_LIST_TENANTS:
          serviceImpl.listTenants((com.udb.core.tenant.services.v1.ListTenantsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.ListTenantsResponse>) responseObserver);
          break;
        case METHODID_UPDATE_TENANT:
          serviceImpl.updateTenant((com.udb.core.tenant.services.v1.UpdateTenantRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.UpdateTenantResponse>) responseObserver);
          break;
        case METHODID_GET_TENANT_CONFIG:
          serviceImpl.getTenantConfig((com.udb.core.tenant.services.v1.GetTenantConfigRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.GetTenantConfigResponse>) responseObserver);
          break;
        case METHODID_UPDATE_TENANT_CONFIG:
          serviceImpl.updateTenantConfig((com.udb.core.tenant.services.v1.UpdateTenantConfigRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.tenant.services.v1.UpdateTenantConfigResponse>) responseObserver);
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
          getCreateTenantMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.tenant.services.v1.CreateTenantRequest,
              com.udb.core.tenant.services.v1.CreateTenantResponse>(
                service, METHODID_CREATE_TENANT)))
        .addMethod(
          getGetTenantMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.tenant.services.v1.GetTenantRequest,
              com.udb.core.tenant.services.v1.GetTenantResponse>(
                service, METHODID_GET_TENANT)))
        .addMethod(
          getListTenantsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.tenant.services.v1.ListTenantsRequest,
              com.udb.core.tenant.services.v1.ListTenantsResponse>(
                service, METHODID_LIST_TENANTS)))
        .addMethod(
          getUpdateTenantMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.tenant.services.v1.UpdateTenantRequest,
              com.udb.core.tenant.services.v1.UpdateTenantResponse>(
                service, METHODID_UPDATE_TENANT)))
        .addMethod(
          getGetTenantConfigMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.tenant.services.v1.GetTenantConfigRequest,
              com.udb.core.tenant.services.v1.GetTenantConfigResponse>(
                service, METHODID_GET_TENANT_CONFIG)))
        .addMethod(
          getUpdateTenantConfigMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.tenant.services.v1.UpdateTenantConfigRequest,
              com.udb.core.tenant.services.v1.UpdateTenantConfigResponse>(
                service, METHODID_UPDATE_TENANT_CONFIG)))
        .build();
  }

  private static abstract class TenantServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TenantServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.tenant.services.v1.TenantServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TenantService");
    }
  }

  private static final class TenantServiceFileDescriptorSupplier
      extends TenantServiceBaseDescriptorSupplier {
    TenantServiceFileDescriptorSupplier() {}
  }

  private static final class TenantServiceMethodDescriptorSupplier
      extends TenantServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    TenantServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (TenantServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TenantServiceFileDescriptorSupplier())
              .addMethod(getCreateTenantMethod())
              .addMethod(getGetTenantMethod())
              .addMethod(getListTenantsMethod())
              .addMethod(getUpdateTenantMethod())
              .addMethod(getGetTenantConfigMethod())
              .addMethod(getUpdateTenantConfigMethod())
              .build();
        }
      }
    }
    return result;
  }
}
