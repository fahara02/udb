package com.udb.core.config.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * ConfigService (master-plan 9.8) — feature flags and runtime configuration.
 * Flags are scoped to (tenant, project, environment); evaluation precedence is
 * environment &gt; project &gt; tenant-default. EvaluateFlags is a PURE function of
 * (flags, context) — the same algorithm ships in the SDK so client and server
 * agree bit-for-bit (the unit-test fixtures are the SDK&lt;-&gt;server contract).
 * Every mutation is durable, tenant-scoped by the verified claim, bumps a
 * monotone per-row revision, and emits `udb.config.flag.changed.v1`.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class ConfigServiceGrpc {

  private ConfigServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.config.services.v1.ConfigService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.config.services.v1.PutFlagRequest,
      com.udb.core.config.services.v1.PutFlagResponse> getPutFlagMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PutFlag",
      requestType = com.udb.core.config.services.v1.PutFlagRequest.class,
      responseType = com.udb.core.config.services.v1.PutFlagResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.config.services.v1.PutFlagRequest,
      com.udb.core.config.services.v1.PutFlagResponse> getPutFlagMethod() {
    io.grpc.MethodDescriptor<com.udb.core.config.services.v1.PutFlagRequest, com.udb.core.config.services.v1.PutFlagResponse> getPutFlagMethod;
    if ((getPutFlagMethod = ConfigServiceGrpc.getPutFlagMethod) == null) {
      synchronized (ConfigServiceGrpc.class) {
        if ((getPutFlagMethod = ConfigServiceGrpc.getPutFlagMethod) == null) {
          ConfigServiceGrpc.getPutFlagMethod = getPutFlagMethod =
              io.grpc.MethodDescriptor.<com.udb.core.config.services.v1.PutFlagRequest, com.udb.core.config.services.v1.PutFlagResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PutFlag"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.config.services.v1.PutFlagRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.config.services.v1.PutFlagResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConfigServiceMethodDescriptorSupplier("PutFlag"))
              .build();
        }
      }
    }
    return getPutFlagMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.config.services.v1.GetFlagRequest,
      com.udb.core.config.services.v1.GetFlagResponse> getGetFlagMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetFlag",
      requestType = com.udb.core.config.services.v1.GetFlagRequest.class,
      responseType = com.udb.core.config.services.v1.GetFlagResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.config.services.v1.GetFlagRequest,
      com.udb.core.config.services.v1.GetFlagResponse> getGetFlagMethod() {
    io.grpc.MethodDescriptor<com.udb.core.config.services.v1.GetFlagRequest, com.udb.core.config.services.v1.GetFlagResponse> getGetFlagMethod;
    if ((getGetFlagMethod = ConfigServiceGrpc.getGetFlagMethod) == null) {
      synchronized (ConfigServiceGrpc.class) {
        if ((getGetFlagMethod = ConfigServiceGrpc.getGetFlagMethod) == null) {
          ConfigServiceGrpc.getGetFlagMethod = getGetFlagMethod =
              io.grpc.MethodDescriptor.<com.udb.core.config.services.v1.GetFlagRequest, com.udb.core.config.services.v1.GetFlagResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetFlag"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.config.services.v1.GetFlagRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.config.services.v1.GetFlagResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConfigServiceMethodDescriptorSupplier("GetFlag"))
              .build();
        }
      }
    }
    return getGetFlagMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.config.services.v1.ListFlagsRequest,
      com.udb.core.config.services.v1.ListFlagsResponse> getListFlagsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListFlags",
      requestType = com.udb.core.config.services.v1.ListFlagsRequest.class,
      responseType = com.udb.core.config.services.v1.ListFlagsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.config.services.v1.ListFlagsRequest,
      com.udb.core.config.services.v1.ListFlagsResponse> getListFlagsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.config.services.v1.ListFlagsRequest, com.udb.core.config.services.v1.ListFlagsResponse> getListFlagsMethod;
    if ((getListFlagsMethod = ConfigServiceGrpc.getListFlagsMethod) == null) {
      synchronized (ConfigServiceGrpc.class) {
        if ((getListFlagsMethod = ConfigServiceGrpc.getListFlagsMethod) == null) {
          ConfigServiceGrpc.getListFlagsMethod = getListFlagsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.config.services.v1.ListFlagsRequest, com.udb.core.config.services.v1.ListFlagsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListFlags"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.config.services.v1.ListFlagsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.config.services.v1.ListFlagsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConfigServiceMethodDescriptorSupplier("ListFlags"))
              .build();
        }
      }
    }
    return getListFlagsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.config.services.v1.DeleteFlagRequest,
      com.udb.core.config.services.v1.DeleteFlagResponse> getDeleteFlagMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteFlag",
      requestType = com.udb.core.config.services.v1.DeleteFlagRequest.class,
      responseType = com.udb.core.config.services.v1.DeleteFlagResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.config.services.v1.DeleteFlagRequest,
      com.udb.core.config.services.v1.DeleteFlagResponse> getDeleteFlagMethod() {
    io.grpc.MethodDescriptor<com.udb.core.config.services.v1.DeleteFlagRequest, com.udb.core.config.services.v1.DeleteFlagResponse> getDeleteFlagMethod;
    if ((getDeleteFlagMethod = ConfigServiceGrpc.getDeleteFlagMethod) == null) {
      synchronized (ConfigServiceGrpc.class) {
        if ((getDeleteFlagMethod = ConfigServiceGrpc.getDeleteFlagMethod) == null) {
          ConfigServiceGrpc.getDeleteFlagMethod = getDeleteFlagMethod =
              io.grpc.MethodDescriptor.<com.udb.core.config.services.v1.DeleteFlagRequest, com.udb.core.config.services.v1.DeleteFlagResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteFlag"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.config.services.v1.DeleteFlagRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.config.services.v1.DeleteFlagResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConfigServiceMethodDescriptorSupplier("DeleteFlag"))
              .build();
        }
      }
    }
    return getDeleteFlagMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.config.services.v1.EvaluateFlagsRequest,
      com.udb.core.config.services.v1.EvaluateFlagsResponse> getEvaluateFlagsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "EvaluateFlags",
      requestType = com.udb.core.config.services.v1.EvaluateFlagsRequest.class,
      responseType = com.udb.core.config.services.v1.EvaluateFlagsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.config.services.v1.EvaluateFlagsRequest,
      com.udb.core.config.services.v1.EvaluateFlagsResponse> getEvaluateFlagsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.config.services.v1.EvaluateFlagsRequest, com.udb.core.config.services.v1.EvaluateFlagsResponse> getEvaluateFlagsMethod;
    if ((getEvaluateFlagsMethod = ConfigServiceGrpc.getEvaluateFlagsMethod) == null) {
      synchronized (ConfigServiceGrpc.class) {
        if ((getEvaluateFlagsMethod = ConfigServiceGrpc.getEvaluateFlagsMethod) == null) {
          ConfigServiceGrpc.getEvaluateFlagsMethod = getEvaluateFlagsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.config.services.v1.EvaluateFlagsRequest, com.udb.core.config.services.v1.EvaluateFlagsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "EvaluateFlags"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.config.services.v1.EvaluateFlagsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.config.services.v1.EvaluateFlagsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConfigServiceMethodDescriptorSupplier("EvaluateFlags"))
              .build();
        }
      }
    }
    return getEvaluateFlagsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ConfigServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ConfigServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ConfigServiceStub>() {
        @java.lang.Override
        public ConfigServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ConfigServiceStub(channel, callOptions);
        }
      };
    return ConfigServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static ConfigServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ConfigServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ConfigServiceBlockingV2Stub>() {
        @java.lang.Override
        public ConfigServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ConfigServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return ConfigServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ConfigServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ConfigServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ConfigServiceBlockingStub>() {
        @java.lang.Override
        public ConfigServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ConfigServiceBlockingStub(channel, callOptions);
        }
      };
    return ConfigServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ConfigServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ConfigServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ConfigServiceFutureStub>() {
        @java.lang.Override
        public ConfigServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ConfigServiceFutureStub(channel, callOptions);
        }
      };
    return ConfigServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * ConfigService (master-plan 9.8) — feature flags and runtime configuration.
   * Flags are scoped to (tenant, project, environment); evaluation precedence is
   * environment &gt; project &gt; tenant-default. EvaluateFlags is a PURE function of
   * (flags, context) — the same algorithm ships in the SDK so client and server
   * agree bit-for-bit (the unit-test fixtures are the SDK&lt;-&gt;server contract).
   * Every mutation is durable, tenant-scoped by the verified claim, bumps a
   * monotone per-row revision, and emits `udb.config.flag.changed.v1`.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Create or update a flag at a (tenant, project, environment) scope. Bumps the
     * flag's monotone revision and emits `udb.config.flag.changed.v1`.
     * </pre>
     */
    default void putFlag(com.udb.core.config.services.v1.PutFlagRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.config.services.v1.PutFlagResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPutFlagMethod(), responseObserver);
    }

    /**
     * <pre>
     * Fetch a single flag's stored definition at an exact (tenant, project,
     * environment, key) scope. Read-only; performs no rollout evaluation.
     * </pre>
     */
    default void getFlag(com.udb.core.config.services.v1.GetFlagRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.config.services.v1.GetFlagResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetFlagMethod(), responseObserver);
    }

    /**
     * <pre>
     * List a tenant's flags, optionally narrowed to a project and/or environment.
     * </pre>
     */
    default void listFlags(com.udb.core.config.services.v1.ListFlagsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.config.services.v1.ListFlagsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListFlagsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Delete a flag at an exact scope. Destructive; bumps the revision in the
     * emitted change event.
     * </pre>
     */
    default void deleteFlag(com.udb.core.config.services.v1.DeleteFlagRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.config.services.v1.DeleteFlagResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteFlagMethod(), responseObserver);
    }

    /**
     * <pre>
     * Evaluate a set of flag keys for an evaluation context. The server applies the
     * SAME pure algorithm the SDK uses (scope precedence + stable-hash percentage
     * rollout) and returns the resolved typed values plus a server-authoritative
     * cache TTL and the observed config revision. Read-only.
     * </pre>
     */
    default void evaluateFlags(com.udb.core.config.services.v1.EvaluateFlagsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.config.services.v1.EvaluateFlagsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEvaluateFlagsMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service ConfigService.
   * <pre>
   * ConfigService (master-plan 9.8) — feature flags and runtime configuration.
   * Flags are scoped to (tenant, project, environment); evaluation precedence is
   * environment &gt; project &gt; tenant-default. EvaluateFlags is a PURE function of
   * (flags, context) — the same algorithm ships in the SDK so client and server
   * agree bit-for-bit (the unit-test fixtures are the SDK&lt;-&gt;server contract).
   * Every mutation is durable, tenant-scoped by the verified claim, bumps a
   * monotone per-row revision, and emits `udb.config.flag.changed.v1`.
   * </pre>
   */
  public static abstract class ConfigServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return ConfigServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service ConfigService.
   * <pre>
   * ConfigService (master-plan 9.8) — feature flags and runtime configuration.
   * Flags are scoped to (tenant, project, environment); evaluation precedence is
   * environment &gt; project &gt; tenant-default. EvaluateFlags is a PURE function of
   * (flags, context) — the same algorithm ships in the SDK so client and server
   * agree bit-for-bit (the unit-test fixtures are the SDK&lt;-&gt;server contract).
   * Every mutation is durable, tenant-scoped by the verified claim, bumps a
   * monotone per-row revision, and emits `udb.config.flag.changed.v1`.
   * </pre>
   */
  public static final class ConfigServiceStub
      extends io.grpc.stub.AbstractAsyncStub<ConfigServiceStub> {
    private ConfigServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConfigServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ConfigServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Create or update a flag at a (tenant, project, environment) scope. Bumps the
     * flag's monotone revision and emits `udb.config.flag.changed.v1`.
     * </pre>
     */
    public void putFlag(com.udb.core.config.services.v1.PutFlagRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.config.services.v1.PutFlagResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPutFlagMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Fetch a single flag's stored definition at an exact (tenant, project,
     * environment, key) scope. Read-only; performs no rollout evaluation.
     * </pre>
     */
    public void getFlag(com.udb.core.config.services.v1.GetFlagRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.config.services.v1.GetFlagResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetFlagMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List a tenant's flags, optionally narrowed to a project and/or environment.
     * </pre>
     */
    public void listFlags(com.udb.core.config.services.v1.ListFlagsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.config.services.v1.ListFlagsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListFlagsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Delete a flag at an exact scope. Destructive; bumps the revision in the
     * emitted change event.
     * </pre>
     */
    public void deleteFlag(com.udb.core.config.services.v1.DeleteFlagRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.config.services.v1.DeleteFlagResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteFlagMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Evaluate a set of flag keys for an evaluation context. The server applies the
     * SAME pure algorithm the SDK uses (scope precedence + stable-hash percentage
     * rollout) and returns the resolved typed values plus a server-authoritative
     * cache TTL and the observed config revision. Read-only.
     * </pre>
     */
    public void evaluateFlags(com.udb.core.config.services.v1.EvaluateFlagsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.config.services.v1.EvaluateFlagsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEvaluateFlagsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service ConfigService.
   * <pre>
   * ConfigService (master-plan 9.8) — feature flags and runtime configuration.
   * Flags are scoped to (tenant, project, environment); evaluation precedence is
   * environment &gt; project &gt; tenant-default. EvaluateFlags is a PURE function of
   * (flags, context) — the same algorithm ships in the SDK so client and server
   * agree bit-for-bit (the unit-test fixtures are the SDK&lt;-&gt;server contract).
   * Every mutation is durable, tenant-scoped by the verified claim, bumps a
   * monotone per-row revision, and emits `udb.config.flag.changed.v1`.
   * </pre>
   */
  public static final class ConfigServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<ConfigServiceBlockingV2Stub> {
    private ConfigServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConfigServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ConfigServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Create or update a flag at a (tenant, project, environment) scope. Bumps the
     * flag's monotone revision and emits `udb.config.flag.changed.v1`.
     * </pre>
     */
    public com.udb.core.config.services.v1.PutFlagResponse putFlag(com.udb.core.config.services.v1.PutFlagRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPutFlagMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch a single flag's stored definition at an exact (tenant, project,
     * environment, key) scope. Read-only; performs no rollout evaluation.
     * </pre>
     */
    public com.udb.core.config.services.v1.GetFlagResponse getFlag(com.udb.core.config.services.v1.GetFlagRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetFlagMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List a tenant's flags, optionally narrowed to a project and/or environment.
     * </pre>
     */
    public com.udb.core.config.services.v1.ListFlagsResponse listFlags(com.udb.core.config.services.v1.ListFlagsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListFlagsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a flag at an exact scope. Destructive; bumps the revision in the
     * emitted change event.
     * </pre>
     */
    public com.udb.core.config.services.v1.DeleteFlagResponse deleteFlag(com.udb.core.config.services.v1.DeleteFlagRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeleteFlagMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Evaluate a set of flag keys for an evaluation context. The server applies the
     * SAME pure algorithm the SDK uses (scope precedence + stable-hash percentage
     * rollout) and returns the resolved typed values plus a server-authoritative
     * cache TTL and the observed config revision. Read-only.
     * </pre>
     */
    public com.udb.core.config.services.v1.EvaluateFlagsResponse evaluateFlags(com.udb.core.config.services.v1.EvaluateFlagsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getEvaluateFlagsMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service ConfigService.
   * <pre>
   * ConfigService (master-plan 9.8) — feature flags and runtime configuration.
   * Flags are scoped to (tenant, project, environment); evaluation precedence is
   * environment &gt; project &gt; tenant-default. EvaluateFlags is a PURE function of
   * (flags, context) — the same algorithm ships in the SDK so client and server
   * agree bit-for-bit (the unit-test fixtures are the SDK&lt;-&gt;server contract).
   * Every mutation is durable, tenant-scoped by the verified claim, bumps a
   * monotone per-row revision, and emits `udb.config.flag.changed.v1`.
   * </pre>
   */
  public static final class ConfigServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<ConfigServiceBlockingStub> {
    private ConfigServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConfigServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ConfigServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Create or update a flag at a (tenant, project, environment) scope. Bumps the
     * flag's monotone revision and emits `udb.config.flag.changed.v1`.
     * </pre>
     */
    public com.udb.core.config.services.v1.PutFlagResponse putFlag(com.udb.core.config.services.v1.PutFlagRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPutFlagMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch a single flag's stored definition at an exact (tenant, project,
     * environment, key) scope. Read-only; performs no rollout evaluation.
     * </pre>
     */
    public com.udb.core.config.services.v1.GetFlagResponse getFlag(com.udb.core.config.services.v1.GetFlagRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetFlagMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List a tenant's flags, optionally narrowed to a project and/or environment.
     * </pre>
     */
    public com.udb.core.config.services.v1.ListFlagsResponse listFlags(com.udb.core.config.services.v1.ListFlagsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListFlagsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a flag at an exact scope. Destructive; bumps the revision in the
     * emitted change event.
     * </pre>
     */
    public com.udb.core.config.services.v1.DeleteFlagResponse deleteFlag(com.udb.core.config.services.v1.DeleteFlagRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteFlagMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Evaluate a set of flag keys for an evaluation context. The server applies the
     * SAME pure algorithm the SDK uses (scope precedence + stable-hash percentage
     * rollout) and returns the resolved typed values plus a server-authoritative
     * cache TTL and the observed config revision. Read-only.
     * </pre>
     */
    public com.udb.core.config.services.v1.EvaluateFlagsResponse evaluateFlags(com.udb.core.config.services.v1.EvaluateFlagsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEvaluateFlagsMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service ConfigService.
   * <pre>
   * ConfigService (master-plan 9.8) — feature flags and runtime configuration.
   * Flags are scoped to (tenant, project, environment); evaluation precedence is
   * environment &gt; project &gt; tenant-default. EvaluateFlags is a PURE function of
   * (flags, context) — the same algorithm ships in the SDK so client and server
   * agree bit-for-bit (the unit-test fixtures are the SDK&lt;-&gt;server contract).
   * Every mutation is durable, tenant-scoped by the verified claim, bumps a
   * monotone per-row revision, and emits `udb.config.flag.changed.v1`.
   * </pre>
   */
  public static final class ConfigServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<ConfigServiceFutureStub> {
    private ConfigServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConfigServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ConfigServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Create or update a flag at a (tenant, project, environment) scope. Bumps the
     * flag's monotone revision and emits `udb.config.flag.changed.v1`.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.config.services.v1.PutFlagResponse> putFlag(
        com.udb.core.config.services.v1.PutFlagRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPutFlagMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Fetch a single flag's stored definition at an exact (tenant, project,
     * environment, key) scope. Read-only; performs no rollout evaluation.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.config.services.v1.GetFlagResponse> getFlag(
        com.udb.core.config.services.v1.GetFlagRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetFlagMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List a tenant's flags, optionally narrowed to a project and/or environment.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.config.services.v1.ListFlagsResponse> listFlags(
        com.udb.core.config.services.v1.ListFlagsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListFlagsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Delete a flag at an exact scope. Destructive; bumps the revision in the
     * emitted change event.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.config.services.v1.DeleteFlagResponse> deleteFlag(
        com.udb.core.config.services.v1.DeleteFlagRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteFlagMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Evaluate a set of flag keys for an evaluation context. The server applies the
     * SAME pure algorithm the SDK uses (scope precedence + stable-hash percentage
     * rollout) and returns the resolved typed values plus a server-authoritative
     * cache TTL and the observed config revision. Read-only.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.config.services.v1.EvaluateFlagsResponse> evaluateFlags(
        com.udb.core.config.services.v1.EvaluateFlagsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEvaluateFlagsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_PUT_FLAG = 0;
  private static final int METHODID_GET_FLAG = 1;
  private static final int METHODID_LIST_FLAGS = 2;
  private static final int METHODID_DELETE_FLAG = 3;
  private static final int METHODID_EVALUATE_FLAGS = 4;

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
        case METHODID_PUT_FLAG:
          serviceImpl.putFlag((com.udb.core.config.services.v1.PutFlagRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.config.services.v1.PutFlagResponse>) responseObserver);
          break;
        case METHODID_GET_FLAG:
          serviceImpl.getFlag((com.udb.core.config.services.v1.GetFlagRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.config.services.v1.GetFlagResponse>) responseObserver);
          break;
        case METHODID_LIST_FLAGS:
          serviceImpl.listFlags((com.udb.core.config.services.v1.ListFlagsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.config.services.v1.ListFlagsResponse>) responseObserver);
          break;
        case METHODID_DELETE_FLAG:
          serviceImpl.deleteFlag((com.udb.core.config.services.v1.DeleteFlagRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.config.services.v1.DeleteFlagResponse>) responseObserver);
          break;
        case METHODID_EVALUATE_FLAGS:
          serviceImpl.evaluateFlags((com.udb.core.config.services.v1.EvaluateFlagsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.config.services.v1.EvaluateFlagsResponse>) responseObserver);
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
          getPutFlagMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.config.services.v1.PutFlagRequest,
              com.udb.core.config.services.v1.PutFlagResponse>(
                service, METHODID_PUT_FLAG)))
        .addMethod(
          getGetFlagMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.config.services.v1.GetFlagRequest,
              com.udb.core.config.services.v1.GetFlagResponse>(
                service, METHODID_GET_FLAG)))
        .addMethod(
          getListFlagsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.config.services.v1.ListFlagsRequest,
              com.udb.core.config.services.v1.ListFlagsResponse>(
                service, METHODID_LIST_FLAGS)))
        .addMethod(
          getDeleteFlagMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.config.services.v1.DeleteFlagRequest,
              com.udb.core.config.services.v1.DeleteFlagResponse>(
                service, METHODID_DELETE_FLAG)))
        .addMethod(
          getEvaluateFlagsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.config.services.v1.EvaluateFlagsRequest,
              com.udb.core.config.services.v1.EvaluateFlagsResponse>(
                service, METHODID_EVALUATE_FLAGS)))
        .build();
  }

  private static abstract class ConfigServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ConfigServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.config.services.v1.ConfigServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ConfigService");
    }
  }

  private static final class ConfigServiceFileDescriptorSupplier
      extends ConfigServiceBaseDescriptorSupplier {
    ConfigServiceFileDescriptorSupplier() {}
  }

  private static final class ConfigServiceMethodDescriptorSupplier
      extends ConfigServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    ConfigServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (ConfigServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ConfigServiceFileDescriptorSupplier())
              .addMethod(getPutFlagMethod())
              .addMethod(getGetFlagMethod())
              .addMethod(getListFlagsMethod())
              .addMethod(getDeleteFlagMethod())
              .addMethod(getEvaluateFlagsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
