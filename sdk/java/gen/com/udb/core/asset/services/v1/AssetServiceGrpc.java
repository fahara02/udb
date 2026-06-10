package com.udb.core.asset.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class AssetServiceGrpc {

  private AssetServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.asset.services.v1.AssetService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.CreatePipelineDefinitionRequest,
      com.udb.core.asset.services.v1.CreatePipelineDefinitionResponse> getCreatePipelineDefinitionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreatePipelineDefinition",
      requestType = com.udb.core.asset.services.v1.CreatePipelineDefinitionRequest.class,
      responseType = com.udb.core.asset.services.v1.CreatePipelineDefinitionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.CreatePipelineDefinitionRequest,
      com.udb.core.asset.services.v1.CreatePipelineDefinitionResponse> getCreatePipelineDefinitionMethod() {
    io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.CreatePipelineDefinitionRequest, com.udb.core.asset.services.v1.CreatePipelineDefinitionResponse> getCreatePipelineDefinitionMethod;
    if ((getCreatePipelineDefinitionMethod = AssetServiceGrpc.getCreatePipelineDefinitionMethod) == null) {
      synchronized (AssetServiceGrpc.class) {
        if ((getCreatePipelineDefinitionMethod = AssetServiceGrpc.getCreatePipelineDefinitionMethod) == null) {
          AssetServiceGrpc.getCreatePipelineDefinitionMethod = getCreatePipelineDefinitionMethod =
              io.grpc.MethodDescriptor.<com.udb.core.asset.services.v1.CreatePipelineDefinitionRequest, com.udb.core.asset.services.v1.CreatePipelineDefinitionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreatePipelineDefinition"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.asset.services.v1.CreatePipelineDefinitionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.asset.services.v1.CreatePipelineDefinitionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AssetServiceMethodDescriptorSupplier("CreatePipelineDefinition"))
              .build();
        }
      }
    }
    return getCreatePipelineDefinitionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.GetPipelineDefinitionRequest,
      com.udb.core.asset.services.v1.GetPipelineDefinitionResponse> getGetPipelineDefinitionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPipelineDefinition",
      requestType = com.udb.core.asset.services.v1.GetPipelineDefinitionRequest.class,
      responseType = com.udb.core.asset.services.v1.GetPipelineDefinitionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.GetPipelineDefinitionRequest,
      com.udb.core.asset.services.v1.GetPipelineDefinitionResponse> getGetPipelineDefinitionMethod() {
    io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.GetPipelineDefinitionRequest, com.udb.core.asset.services.v1.GetPipelineDefinitionResponse> getGetPipelineDefinitionMethod;
    if ((getGetPipelineDefinitionMethod = AssetServiceGrpc.getGetPipelineDefinitionMethod) == null) {
      synchronized (AssetServiceGrpc.class) {
        if ((getGetPipelineDefinitionMethod = AssetServiceGrpc.getGetPipelineDefinitionMethod) == null) {
          AssetServiceGrpc.getGetPipelineDefinitionMethod = getGetPipelineDefinitionMethod =
              io.grpc.MethodDescriptor.<com.udb.core.asset.services.v1.GetPipelineDefinitionRequest, com.udb.core.asset.services.v1.GetPipelineDefinitionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetPipelineDefinition"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.asset.services.v1.GetPipelineDefinitionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.asset.services.v1.GetPipelineDefinitionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AssetServiceMethodDescriptorSupplier("GetPipelineDefinition"))
              .build();
        }
      }
    }
    return getGetPipelineDefinitionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.RegisterAssetRequest,
      com.udb.core.asset.services.v1.RegisterAssetResponse> getRegisterAssetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RegisterAsset",
      requestType = com.udb.core.asset.services.v1.RegisterAssetRequest.class,
      responseType = com.udb.core.asset.services.v1.RegisterAssetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.RegisterAssetRequest,
      com.udb.core.asset.services.v1.RegisterAssetResponse> getRegisterAssetMethod() {
    io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.RegisterAssetRequest, com.udb.core.asset.services.v1.RegisterAssetResponse> getRegisterAssetMethod;
    if ((getRegisterAssetMethod = AssetServiceGrpc.getRegisterAssetMethod) == null) {
      synchronized (AssetServiceGrpc.class) {
        if ((getRegisterAssetMethod = AssetServiceGrpc.getRegisterAssetMethod) == null) {
          AssetServiceGrpc.getRegisterAssetMethod = getRegisterAssetMethod =
              io.grpc.MethodDescriptor.<com.udb.core.asset.services.v1.RegisterAssetRequest, com.udb.core.asset.services.v1.RegisterAssetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RegisterAsset"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.asset.services.v1.RegisterAssetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.asset.services.v1.RegisterAssetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AssetServiceMethodDescriptorSupplier("RegisterAsset"))
              .build();
        }
      }
    }
    return getRegisterAssetMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.StartPipelineRequest,
      com.udb.core.asset.services.v1.StartPipelineResponse> getStartPipelineMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StartPipeline",
      requestType = com.udb.core.asset.services.v1.StartPipelineRequest.class,
      responseType = com.udb.core.asset.services.v1.StartPipelineResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.StartPipelineRequest,
      com.udb.core.asset.services.v1.StartPipelineResponse> getStartPipelineMethod() {
    io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.StartPipelineRequest, com.udb.core.asset.services.v1.StartPipelineResponse> getStartPipelineMethod;
    if ((getStartPipelineMethod = AssetServiceGrpc.getStartPipelineMethod) == null) {
      synchronized (AssetServiceGrpc.class) {
        if ((getStartPipelineMethod = AssetServiceGrpc.getStartPipelineMethod) == null) {
          AssetServiceGrpc.getStartPipelineMethod = getStartPipelineMethod =
              io.grpc.MethodDescriptor.<com.udb.core.asset.services.v1.StartPipelineRequest, com.udb.core.asset.services.v1.StartPipelineResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StartPipeline"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.asset.services.v1.StartPipelineRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.asset.services.v1.StartPipelineResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AssetServiceMethodDescriptorSupplier("StartPipeline"))
              .build();
        }
      }
    }
    return getStartPipelineMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.GetPipelineRequest,
      com.udb.core.asset.services.v1.GetPipelineResponse> getGetPipelineMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPipeline",
      requestType = com.udb.core.asset.services.v1.GetPipelineRequest.class,
      responseType = com.udb.core.asset.services.v1.GetPipelineResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.GetPipelineRequest,
      com.udb.core.asset.services.v1.GetPipelineResponse> getGetPipelineMethod() {
    io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.GetPipelineRequest, com.udb.core.asset.services.v1.GetPipelineResponse> getGetPipelineMethod;
    if ((getGetPipelineMethod = AssetServiceGrpc.getGetPipelineMethod) == null) {
      synchronized (AssetServiceGrpc.class) {
        if ((getGetPipelineMethod = AssetServiceGrpc.getGetPipelineMethod) == null) {
          AssetServiceGrpc.getGetPipelineMethod = getGetPipelineMethod =
              io.grpc.MethodDescriptor.<com.udb.core.asset.services.v1.GetPipelineRequest, com.udb.core.asset.services.v1.GetPipelineResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetPipeline"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.asset.services.v1.GetPipelineRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.asset.services.v1.GetPipelineResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AssetServiceMethodDescriptorSupplier("GetPipeline"))
              .build();
        }
      }
    }
    return getGetPipelineMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.CompleteStepRequest,
      com.udb.core.asset.services.v1.CompleteStepResponse> getCompleteStepMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CompleteStep",
      requestType = com.udb.core.asset.services.v1.CompleteStepRequest.class,
      responseType = com.udb.core.asset.services.v1.CompleteStepResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.CompleteStepRequest,
      com.udb.core.asset.services.v1.CompleteStepResponse> getCompleteStepMethod() {
    io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.CompleteStepRequest, com.udb.core.asset.services.v1.CompleteStepResponse> getCompleteStepMethod;
    if ((getCompleteStepMethod = AssetServiceGrpc.getCompleteStepMethod) == null) {
      synchronized (AssetServiceGrpc.class) {
        if ((getCompleteStepMethod = AssetServiceGrpc.getCompleteStepMethod) == null) {
          AssetServiceGrpc.getCompleteStepMethod = getCompleteStepMethod =
              io.grpc.MethodDescriptor.<com.udb.core.asset.services.v1.CompleteStepRequest, com.udb.core.asset.services.v1.CompleteStepResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CompleteStep"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.asset.services.v1.CompleteStepRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.asset.services.v1.CompleteStepResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AssetServiceMethodDescriptorSupplier("CompleteStep"))
              .build();
        }
      }
    }
    return getCompleteStepMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.ListAssetsRequest,
      com.udb.core.asset.services.v1.ListAssetsResponse> getListAssetsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListAssets",
      requestType = com.udb.core.asset.services.v1.ListAssetsRequest.class,
      responseType = com.udb.core.asset.services.v1.ListAssetsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.ListAssetsRequest,
      com.udb.core.asset.services.v1.ListAssetsResponse> getListAssetsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.ListAssetsRequest, com.udb.core.asset.services.v1.ListAssetsResponse> getListAssetsMethod;
    if ((getListAssetsMethod = AssetServiceGrpc.getListAssetsMethod) == null) {
      synchronized (AssetServiceGrpc.class) {
        if ((getListAssetsMethod = AssetServiceGrpc.getListAssetsMethod) == null) {
          AssetServiceGrpc.getListAssetsMethod = getListAssetsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.asset.services.v1.ListAssetsRequest, com.udb.core.asset.services.v1.ListAssetsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListAssets"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.asset.services.v1.ListAssetsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.asset.services.v1.ListAssetsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AssetServiceMethodDescriptorSupplier("ListAssets"))
              .build();
        }
      }
    }
    return getListAssetsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.GetAssetRequest,
      com.udb.core.asset.services.v1.GetAssetResponse> getGetAssetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetAsset",
      requestType = com.udb.core.asset.services.v1.GetAssetRequest.class,
      responseType = com.udb.core.asset.services.v1.GetAssetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.GetAssetRequest,
      com.udb.core.asset.services.v1.GetAssetResponse> getGetAssetMethod() {
    io.grpc.MethodDescriptor<com.udb.core.asset.services.v1.GetAssetRequest, com.udb.core.asset.services.v1.GetAssetResponse> getGetAssetMethod;
    if ((getGetAssetMethod = AssetServiceGrpc.getGetAssetMethod) == null) {
      synchronized (AssetServiceGrpc.class) {
        if ((getGetAssetMethod = AssetServiceGrpc.getGetAssetMethod) == null) {
          AssetServiceGrpc.getGetAssetMethod = getGetAssetMethod =
              io.grpc.MethodDescriptor.<com.udb.core.asset.services.v1.GetAssetRequest, com.udb.core.asset.services.v1.GetAssetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetAsset"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.asset.services.v1.GetAssetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.asset.services.v1.GetAssetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AssetServiceMethodDescriptorSupplier("GetAsset"))
              .build();
        }
      }
    }
    return getGetAssetMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static AssetServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AssetServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AssetServiceStub>() {
        @java.lang.Override
        public AssetServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AssetServiceStub(channel, callOptions);
        }
      };
    return AssetServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static AssetServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AssetServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AssetServiceBlockingV2Stub>() {
        @java.lang.Override
        public AssetServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AssetServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return AssetServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static AssetServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AssetServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AssetServiceBlockingStub>() {
        @java.lang.Override
        public AssetServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AssetServiceBlockingStub(channel, callOptions);
        }
      };
    return AssetServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static AssetServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AssetServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AssetServiceFutureStub>() {
        @java.lang.Override
        public AssetServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AssetServiceFutureStub(channel, callOptions);
        }
      };
    return AssetServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     * <pre>
     * Create a reusable pipeline definition
     * </pre>
     */
    default void createPipelineDefinition(com.udb.core.asset.services.v1.CreatePipelineDefinitionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.CreatePipelineDefinitionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreatePipelineDefinitionMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get a pipeline definition
     * </pre>
     */
    default void getPipelineDefinition(com.udb.core.asset.services.v1.GetPipelineDefinitionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.GetPipelineDefinitionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetPipelineDefinitionMethod(), responseObserver);
    }

    /**
     * <pre>
     * Register a managed asset wrapping a storage file
     * </pre>
     */
    default void registerAsset(com.udb.core.asset.services.v1.RegisterAssetRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.RegisterAssetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRegisterAssetMethod(), responseObserver);
    }

    /**
     * <pre>
     * Start a pipeline instance for an asset
     * </pre>
     */
    default void startPipeline(com.udb.core.asset.services.v1.StartPipelineRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.StartPipelineResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStartPipelineMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get a pipeline instance with its steps
     * </pre>
     */
    default void getPipeline(com.udb.core.asset.services.v1.GetPipelineRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.GetPipelineResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetPipelineMethod(), responseObserver);
    }

    /**
     * <pre>
     * Complete (or skip/fail) a pipeline step
     * </pre>
     */
    default void completeStep(com.udb.core.asset.services.v1.CompleteStepRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.CompleteStepResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCompleteStepMethod(), responseObserver);
    }

    /**
     * <pre>
     * List assets
     * </pre>
     */
    default void listAssets(com.udb.core.asset.services.v1.ListAssetsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.ListAssetsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListAssetsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get an asset
     * </pre>
     */
    default void getAsset(com.udb.core.asset.services.v1.GetAssetRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.GetAssetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetAssetMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service AssetService.
   */
  public static abstract class AssetServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return AssetServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service AssetService.
   */
  public static final class AssetServiceStub
      extends io.grpc.stub.AbstractAsyncStub<AssetServiceStub> {
    private AssetServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AssetServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AssetServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Create a reusable pipeline definition
     * </pre>
     */
    public void createPipelineDefinition(com.udb.core.asset.services.v1.CreatePipelineDefinitionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.CreatePipelineDefinitionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreatePipelineDefinitionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get a pipeline definition
     * </pre>
     */
    public void getPipelineDefinition(com.udb.core.asset.services.v1.GetPipelineDefinitionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.GetPipelineDefinitionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetPipelineDefinitionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Register a managed asset wrapping a storage file
     * </pre>
     */
    public void registerAsset(com.udb.core.asset.services.v1.RegisterAssetRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.RegisterAssetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRegisterAssetMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Start a pipeline instance for an asset
     * </pre>
     */
    public void startPipeline(com.udb.core.asset.services.v1.StartPipelineRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.StartPipelineResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStartPipelineMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get a pipeline instance with its steps
     * </pre>
     */
    public void getPipeline(com.udb.core.asset.services.v1.GetPipelineRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.GetPipelineResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetPipelineMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Complete (or skip/fail) a pipeline step
     * </pre>
     */
    public void completeStep(com.udb.core.asset.services.v1.CompleteStepRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.CompleteStepResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCompleteStepMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List assets
     * </pre>
     */
    public void listAssets(com.udb.core.asset.services.v1.ListAssetsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.ListAssetsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListAssetsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get an asset
     * </pre>
     */
    public void getAsset(com.udb.core.asset.services.v1.GetAssetRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.GetAssetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetAssetMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service AssetService.
   */
  public static final class AssetServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<AssetServiceBlockingV2Stub> {
    private AssetServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AssetServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AssetServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Create a reusable pipeline definition
     * </pre>
     */
    public com.udb.core.asset.services.v1.CreatePipelineDefinitionResponse createPipelineDefinition(com.udb.core.asset.services.v1.CreatePipelineDefinitionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreatePipelineDefinitionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a pipeline definition
     * </pre>
     */
    public com.udb.core.asset.services.v1.GetPipelineDefinitionResponse getPipelineDefinition(com.udb.core.asset.services.v1.GetPipelineDefinitionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetPipelineDefinitionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Register a managed asset wrapping a storage file
     * </pre>
     */
    public com.udb.core.asset.services.v1.RegisterAssetResponse registerAsset(com.udb.core.asset.services.v1.RegisterAssetRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRegisterAssetMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Start a pipeline instance for an asset
     * </pre>
     */
    public com.udb.core.asset.services.v1.StartPipelineResponse startPipeline(com.udb.core.asset.services.v1.StartPipelineRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getStartPipelineMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a pipeline instance with its steps
     * </pre>
     */
    public com.udb.core.asset.services.v1.GetPipelineResponse getPipeline(com.udb.core.asset.services.v1.GetPipelineRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetPipelineMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Complete (or skip/fail) a pipeline step
     * </pre>
     */
    public com.udb.core.asset.services.v1.CompleteStepResponse completeStep(com.udb.core.asset.services.v1.CompleteStepRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCompleteStepMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List assets
     * </pre>
     */
    public com.udb.core.asset.services.v1.ListAssetsResponse listAssets(com.udb.core.asset.services.v1.ListAssetsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListAssetsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get an asset
     * </pre>
     */
    public com.udb.core.asset.services.v1.GetAssetResponse getAsset(com.udb.core.asset.services.v1.GetAssetRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetAssetMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service AssetService.
   */
  public static final class AssetServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<AssetServiceBlockingStub> {
    private AssetServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AssetServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AssetServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Create a reusable pipeline definition
     * </pre>
     */
    public com.udb.core.asset.services.v1.CreatePipelineDefinitionResponse createPipelineDefinition(com.udb.core.asset.services.v1.CreatePipelineDefinitionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreatePipelineDefinitionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a pipeline definition
     * </pre>
     */
    public com.udb.core.asset.services.v1.GetPipelineDefinitionResponse getPipelineDefinition(com.udb.core.asset.services.v1.GetPipelineDefinitionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetPipelineDefinitionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Register a managed asset wrapping a storage file
     * </pre>
     */
    public com.udb.core.asset.services.v1.RegisterAssetResponse registerAsset(com.udb.core.asset.services.v1.RegisterAssetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRegisterAssetMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Start a pipeline instance for an asset
     * </pre>
     */
    public com.udb.core.asset.services.v1.StartPipelineResponse startPipeline(com.udb.core.asset.services.v1.StartPipelineRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStartPipelineMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a pipeline instance with its steps
     * </pre>
     */
    public com.udb.core.asset.services.v1.GetPipelineResponse getPipeline(com.udb.core.asset.services.v1.GetPipelineRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetPipelineMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Complete (or skip/fail) a pipeline step
     * </pre>
     */
    public com.udb.core.asset.services.v1.CompleteStepResponse completeStep(com.udb.core.asset.services.v1.CompleteStepRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCompleteStepMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List assets
     * </pre>
     */
    public com.udb.core.asset.services.v1.ListAssetsResponse listAssets(com.udb.core.asset.services.v1.ListAssetsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListAssetsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get an asset
     * </pre>
     */
    public com.udb.core.asset.services.v1.GetAssetResponse getAsset(com.udb.core.asset.services.v1.GetAssetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetAssetMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service AssetService.
   */
  public static final class AssetServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<AssetServiceFutureStub> {
    private AssetServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AssetServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AssetServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Create a reusable pipeline definition
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.asset.services.v1.CreatePipelineDefinitionResponse> createPipelineDefinition(
        com.udb.core.asset.services.v1.CreatePipelineDefinitionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreatePipelineDefinitionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get a pipeline definition
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.asset.services.v1.GetPipelineDefinitionResponse> getPipelineDefinition(
        com.udb.core.asset.services.v1.GetPipelineDefinitionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetPipelineDefinitionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Register a managed asset wrapping a storage file
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.asset.services.v1.RegisterAssetResponse> registerAsset(
        com.udb.core.asset.services.v1.RegisterAssetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRegisterAssetMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Start a pipeline instance for an asset
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.asset.services.v1.StartPipelineResponse> startPipeline(
        com.udb.core.asset.services.v1.StartPipelineRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStartPipelineMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get a pipeline instance with its steps
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.asset.services.v1.GetPipelineResponse> getPipeline(
        com.udb.core.asset.services.v1.GetPipelineRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetPipelineMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Complete (or skip/fail) a pipeline step
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.asset.services.v1.CompleteStepResponse> completeStep(
        com.udb.core.asset.services.v1.CompleteStepRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCompleteStepMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List assets
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.asset.services.v1.ListAssetsResponse> listAssets(
        com.udb.core.asset.services.v1.ListAssetsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListAssetsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get an asset
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.asset.services.v1.GetAssetResponse> getAsset(
        com.udb.core.asset.services.v1.GetAssetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetAssetMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_PIPELINE_DEFINITION = 0;
  private static final int METHODID_GET_PIPELINE_DEFINITION = 1;
  private static final int METHODID_REGISTER_ASSET = 2;
  private static final int METHODID_START_PIPELINE = 3;
  private static final int METHODID_GET_PIPELINE = 4;
  private static final int METHODID_COMPLETE_STEP = 5;
  private static final int METHODID_LIST_ASSETS = 6;
  private static final int METHODID_GET_ASSET = 7;

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
        case METHODID_CREATE_PIPELINE_DEFINITION:
          serviceImpl.createPipelineDefinition((com.udb.core.asset.services.v1.CreatePipelineDefinitionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.CreatePipelineDefinitionResponse>) responseObserver);
          break;
        case METHODID_GET_PIPELINE_DEFINITION:
          serviceImpl.getPipelineDefinition((com.udb.core.asset.services.v1.GetPipelineDefinitionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.GetPipelineDefinitionResponse>) responseObserver);
          break;
        case METHODID_REGISTER_ASSET:
          serviceImpl.registerAsset((com.udb.core.asset.services.v1.RegisterAssetRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.RegisterAssetResponse>) responseObserver);
          break;
        case METHODID_START_PIPELINE:
          serviceImpl.startPipeline((com.udb.core.asset.services.v1.StartPipelineRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.StartPipelineResponse>) responseObserver);
          break;
        case METHODID_GET_PIPELINE:
          serviceImpl.getPipeline((com.udb.core.asset.services.v1.GetPipelineRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.GetPipelineResponse>) responseObserver);
          break;
        case METHODID_COMPLETE_STEP:
          serviceImpl.completeStep((com.udb.core.asset.services.v1.CompleteStepRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.CompleteStepResponse>) responseObserver);
          break;
        case METHODID_LIST_ASSETS:
          serviceImpl.listAssets((com.udb.core.asset.services.v1.ListAssetsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.ListAssetsResponse>) responseObserver);
          break;
        case METHODID_GET_ASSET:
          serviceImpl.getAsset((com.udb.core.asset.services.v1.GetAssetRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.asset.services.v1.GetAssetResponse>) responseObserver);
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
          getCreatePipelineDefinitionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.asset.services.v1.CreatePipelineDefinitionRequest,
              com.udb.core.asset.services.v1.CreatePipelineDefinitionResponse>(
                service, METHODID_CREATE_PIPELINE_DEFINITION)))
        .addMethod(
          getGetPipelineDefinitionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.asset.services.v1.GetPipelineDefinitionRequest,
              com.udb.core.asset.services.v1.GetPipelineDefinitionResponse>(
                service, METHODID_GET_PIPELINE_DEFINITION)))
        .addMethod(
          getRegisterAssetMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.asset.services.v1.RegisterAssetRequest,
              com.udb.core.asset.services.v1.RegisterAssetResponse>(
                service, METHODID_REGISTER_ASSET)))
        .addMethod(
          getStartPipelineMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.asset.services.v1.StartPipelineRequest,
              com.udb.core.asset.services.v1.StartPipelineResponse>(
                service, METHODID_START_PIPELINE)))
        .addMethod(
          getGetPipelineMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.asset.services.v1.GetPipelineRequest,
              com.udb.core.asset.services.v1.GetPipelineResponse>(
                service, METHODID_GET_PIPELINE)))
        .addMethod(
          getCompleteStepMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.asset.services.v1.CompleteStepRequest,
              com.udb.core.asset.services.v1.CompleteStepResponse>(
                service, METHODID_COMPLETE_STEP)))
        .addMethod(
          getListAssetsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.asset.services.v1.ListAssetsRequest,
              com.udb.core.asset.services.v1.ListAssetsResponse>(
                service, METHODID_LIST_ASSETS)))
        .addMethod(
          getGetAssetMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.asset.services.v1.GetAssetRequest,
              com.udb.core.asset.services.v1.GetAssetResponse>(
                service, METHODID_GET_ASSET)))
        .build();
  }

  private static abstract class AssetServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    AssetServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.asset.services.v1.AssetServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("AssetService");
    }
  }

  private static final class AssetServiceFileDescriptorSupplier
      extends AssetServiceBaseDescriptorSupplier {
    AssetServiceFileDescriptorSupplier() {}
  }

  private static final class AssetServiceMethodDescriptorSupplier
      extends AssetServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    AssetServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (AssetServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new AssetServiceFileDescriptorSupplier())
              .addMethod(getCreatePipelineDefinitionMethod())
              .addMethod(getGetPipelineDefinitionMethod())
              .addMethod(getRegisterAssetMethod())
              .addMethod(getStartPipelineMethod())
              .addMethod(getGetPipelineMethod())
              .addMethod(getCompleteStepMethod())
              .addMethod(getListAssetsMethod())
              .addMethod(getGetAssetMethod())
              .build();
        }
      }
    }
    return result;
  }
}
