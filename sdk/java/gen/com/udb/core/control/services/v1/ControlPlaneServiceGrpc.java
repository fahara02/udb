package com.udb.core.control.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * ---------------------------------------------------------------------------
 * ControlPlaneService — Versioned, ACK/NACK, nonce-paired, ordered control-plane
 * policy/config distribution (xDS-style). Nodes (data-plane PEPs) open an
 * aggregated state-of-the-world stream (StreamResources) or an incremental delta
 * stream (DeltaResources) and receive versioned resources in dependency order
 * (backend-target definitions before referencing routing/RLS policies), each
 * response carrying a fresh nonce the node echoes to ACK (apply) or NACK (reject
 * without applying). A node that NACKs keeps its last-good version. Unary helpers
 * fetch resources on demand (incl. by tenant) and expose per-node ack visibility.
 *
 * Server-only control plane: runs on the isolated native auth listener with an
 * admin/service-account credential; never exposed on the public DataBroker port.
 * ---------------------------------------------------------------------------
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class ControlPlaneServiceGrpc {

  private ControlPlaneServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.control.services.v1.ControlPlaneService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.control.services.v1.DiscoveryRequest,
      com.udb.core.control.services.v1.DiscoveryResponse> getStreamResourcesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StreamResources",
      requestType = com.udb.core.control.services.v1.DiscoveryRequest.class,
      responseType = com.udb.core.control.services.v1.DiscoveryResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.udb.core.control.services.v1.DiscoveryRequest,
      com.udb.core.control.services.v1.DiscoveryResponse> getStreamResourcesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.control.services.v1.DiscoveryRequest, com.udb.core.control.services.v1.DiscoveryResponse> getStreamResourcesMethod;
    if ((getStreamResourcesMethod = ControlPlaneServiceGrpc.getStreamResourcesMethod) == null) {
      synchronized (ControlPlaneServiceGrpc.class) {
        if ((getStreamResourcesMethod = ControlPlaneServiceGrpc.getStreamResourcesMethod) == null) {
          ControlPlaneServiceGrpc.getStreamResourcesMethod = getStreamResourcesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.control.services.v1.DiscoveryRequest, com.udb.core.control.services.v1.DiscoveryResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StreamResources"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.control.services.v1.DiscoveryRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.control.services.v1.DiscoveryResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ControlPlaneServiceMethodDescriptorSupplier("StreamResources"))
              .build();
        }
      }
    }
    return getStreamResourcesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.control.services.v1.DeltaDiscoveryRequest,
      com.udb.core.control.services.v1.DeltaDiscoveryResponse> getDeltaResourcesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeltaResources",
      requestType = com.udb.core.control.services.v1.DeltaDiscoveryRequest.class,
      responseType = com.udb.core.control.services.v1.DeltaDiscoveryResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.udb.core.control.services.v1.DeltaDiscoveryRequest,
      com.udb.core.control.services.v1.DeltaDiscoveryResponse> getDeltaResourcesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.control.services.v1.DeltaDiscoveryRequest, com.udb.core.control.services.v1.DeltaDiscoveryResponse> getDeltaResourcesMethod;
    if ((getDeltaResourcesMethod = ControlPlaneServiceGrpc.getDeltaResourcesMethod) == null) {
      synchronized (ControlPlaneServiceGrpc.class) {
        if ((getDeltaResourcesMethod = ControlPlaneServiceGrpc.getDeltaResourcesMethod) == null) {
          ControlPlaneServiceGrpc.getDeltaResourcesMethod = getDeltaResourcesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.control.services.v1.DeltaDiscoveryRequest, com.udb.core.control.services.v1.DeltaDiscoveryResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeltaResources"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.control.services.v1.DeltaDiscoveryRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.control.services.v1.DeltaDiscoveryResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ControlPlaneServiceMethodDescriptorSupplier("DeltaResources"))
              .build();
        }
      }
    }
    return getDeltaResourcesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.control.services.v1.GetResourcesRequest,
      com.udb.core.control.services.v1.GetResourcesResponse> getGetResourcesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetResources",
      requestType = com.udb.core.control.services.v1.GetResourcesRequest.class,
      responseType = com.udb.core.control.services.v1.GetResourcesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.control.services.v1.GetResourcesRequest,
      com.udb.core.control.services.v1.GetResourcesResponse> getGetResourcesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.control.services.v1.GetResourcesRequest, com.udb.core.control.services.v1.GetResourcesResponse> getGetResourcesMethod;
    if ((getGetResourcesMethod = ControlPlaneServiceGrpc.getGetResourcesMethod) == null) {
      synchronized (ControlPlaneServiceGrpc.class) {
        if ((getGetResourcesMethod = ControlPlaneServiceGrpc.getGetResourcesMethod) == null) {
          ControlPlaneServiceGrpc.getGetResourcesMethod = getGetResourcesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.control.services.v1.GetResourcesRequest, com.udb.core.control.services.v1.GetResourcesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetResources"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.control.services.v1.GetResourcesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.control.services.v1.GetResourcesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ControlPlaneServiceMethodDescriptorSupplier("GetResources"))
              .build();
        }
      }
    }
    return getGetResourcesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.control.services.v1.ListNodeStatesRequest,
      com.udb.core.control.services.v1.ListNodeStatesResponse> getListNodeStatesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListNodeStates",
      requestType = com.udb.core.control.services.v1.ListNodeStatesRequest.class,
      responseType = com.udb.core.control.services.v1.ListNodeStatesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.control.services.v1.ListNodeStatesRequest,
      com.udb.core.control.services.v1.ListNodeStatesResponse> getListNodeStatesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.control.services.v1.ListNodeStatesRequest, com.udb.core.control.services.v1.ListNodeStatesResponse> getListNodeStatesMethod;
    if ((getListNodeStatesMethod = ControlPlaneServiceGrpc.getListNodeStatesMethod) == null) {
      synchronized (ControlPlaneServiceGrpc.class) {
        if ((getListNodeStatesMethod = ControlPlaneServiceGrpc.getListNodeStatesMethod) == null) {
          ControlPlaneServiceGrpc.getListNodeStatesMethod = getListNodeStatesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.control.services.v1.ListNodeStatesRequest, com.udb.core.control.services.v1.ListNodeStatesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListNodeStates"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.control.services.v1.ListNodeStatesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.control.services.v1.ListNodeStatesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ControlPlaneServiceMethodDescriptorSupplier("ListNodeStates"))
              .build();
        }
      }
    }
    return getListNodeStatesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.control.services.v1.AckStatusRequest,
      com.udb.core.control.services.v1.AckStatusResponse> getAckStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AckStatus",
      requestType = com.udb.core.control.services.v1.AckStatusRequest.class,
      responseType = com.udb.core.control.services.v1.AckStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.control.services.v1.AckStatusRequest,
      com.udb.core.control.services.v1.AckStatusResponse> getAckStatusMethod() {
    io.grpc.MethodDescriptor<com.udb.core.control.services.v1.AckStatusRequest, com.udb.core.control.services.v1.AckStatusResponse> getAckStatusMethod;
    if ((getAckStatusMethod = ControlPlaneServiceGrpc.getAckStatusMethod) == null) {
      synchronized (ControlPlaneServiceGrpc.class) {
        if ((getAckStatusMethod = ControlPlaneServiceGrpc.getAckStatusMethod) == null) {
          ControlPlaneServiceGrpc.getAckStatusMethod = getAckStatusMethod =
              io.grpc.MethodDescriptor.<com.udb.core.control.services.v1.AckStatusRequest, com.udb.core.control.services.v1.AckStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AckStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.control.services.v1.AckStatusRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.control.services.v1.AckStatusResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ControlPlaneServiceMethodDescriptorSupplier("AckStatus"))
              .build();
        }
      }
    }
    return getAckStatusMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ControlPlaneServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ControlPlaneServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ControlPlaneServiceStub>() {
        @java.lang.Override
        public ControlPlaneServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ControlPlaneServiceStub(channel, callOptions);
        }
      };
    return ControlPlaneServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static ControlPlaneServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ControlPlaneServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ControlPlaneServiceBlockingV2Stub>() {
        @java.lang.Override
        public ControlPlaneServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ControlPlaneServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return ControlPlaneServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ControlPlaneServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ControlPlaneServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ControlPlaneServiceBlockingStub>() {
        @java.lang.Override
        public ControlPlaneServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ControlPlaneServiceBlockingStub(channel, callOptions);
        }
      };
    return ControlPlaneServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ControlPlaneServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ControlPlaneServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ControlPlaneServiceFutureStub>() {
        @java.lang.Override
        public ControlPlaneServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ControlPlaneServiceFutureStub(channel, callOptions);
        }
      };
    return ControlPlaneServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * ---------------------------------------------------------------------------
   * ControlPlaneService — Versioned, ACK/NACK, nonce-paired, ordered control-plane
   * policy/config distribution (xDS-style). Nodes (data-plane PEPs) open an
   * aggregated state-of-the-world stream (StreamResources) or an incremental delta
   * stream (DeltaResources) and receive versioned resources in dependency order
   * (backend-target definitions before referencing routing/RLS policies), each
   * response carrying a fresh nonce the node echoes to ACK (apply) or NACK (reject
   * without applying). A node that NACKs keeps its last-good version. Unary helpers
   * fetch resources on demand (incl. by tenant) and expose per-node ack visibility.
   *
   * Server-only control plane: runs on the isolated native auth listener with an
   * admin/service-account credential; never exposed on the public DataBroker port.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * ── Aggregated state-of-the-world (ADS) ───────────────────────────────────
     * </pre>
     */
    default io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.DiscoveryRequest> streamResources(
        io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.DiscoveryResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getStreamResourcesMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── Incremental / delta discovery ─────────────────────────────────────────
     * </pre>
     */
    default io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.DeltaDiscoveryRequest> deltaResources(
        io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.DeltaDiscoveryResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getDeltaResourcesMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── On-demand fetch (incl. by tenant) ─────────────────────────────────────
     * </pre>
     */
    default void getResources(com.udb.core.control.services.v1.GetResourcesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.GetResourcesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetResourcesMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── Admin visibility ──────────────────────────────────────────────────────
     * </pre>
     */
    default void listNodeStates(com.udb.core.control.services.v1.ListNodeStatesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.ListNodeStatesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListNodeStatesMethod(), responseObserver);
    }

    /**
     */
    default void ackStatus(com.udb.core.control.services.v1.AckStatusRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.AckStatusResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAckStatusMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service ControlPlaneService.
   * <pre>
   * ---------------------------------------------------------------------------
   * ControlPlaneService — Versioned, ACK/NACK, nonce-paired, ordered control-plane
   * policy/config distribution (xDS-style). Nodes (data-plane PEPs) open an
   * aggregated state-of-the-world stream (StreamResources) or an incremental delta
   * stream (DeltaResources) and receive versioned resources in dependency order
   * (backend-target definitions before referencing routing/RLS policies), each
   * response carrying a fresh nonce the node echoes to ACK (apply) or NACK (reject
   * without applying). A node that NACKs keeps its last-good version. Unary helpers
   * fetch resources on demand (incl. by tenant) and expose per-node ack visibility.
   *
   * Server-only control plane: runs on the isolated native auth listener with an
   * admin/service-account credential; never exposed on the public DataBroker port.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static abstract class ControlPlaneServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return ControlPlaneServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service ControlPlaneService.
   * <pre>
   * ---------------------------------------------------------------------------
   * ControlPlaneService — Versioned, ACK/NACK, nonce-paired, ordered control-plane
   * policy/config distribution (xDS-style). Nodes (data-plane PEPs) open an
   * aggregated state-of-the-world stream (StreamResources) or an incremental delta
   * stream (DeltaResources) and receive versioned resources in dependency order
   * (backend-target definitions before referencing routing/RLS policies), each
   * response carrying a fresh nonce the node echoes to ACK (apply) or NACK (reject
   * without applying). A node that NACKs keeps its last-good version. Unary helpers
   * fetch resources on demand (incl. by tenant) and expose per-node ack visibility.
   *
   * Server-only control plane: runs on the isolated native auth listener with an
   * admin/service-account credential; never exposed on the public DataBroker port.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static final class ControlPlaneServiceStub
      extends io.grpc.stub.AbstractAsyncStub<ControlPlaneServiceStub> {
    private ControlPlaneServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ControlPlaneServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ControlPlaneServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * ── Aggregated state-of-the-world (ADS) ───────────────────────────────────
     * </pre>
     */
    public io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.DiscoveryRequest> streamResources(
        io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.DiscoveryResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getStreamResourcesMethod(), getCallOptions()), responseObserver);
    }

    /**
     * <pre>
     * ── Incremental / delta discovery ─────────────────────────────────────────
     * </pre>
     */
    public io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.DeltaDiscoveryRequest> deltaResources(
        io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.DeltaDiscoveryResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getDeltaResourcesMethod(), getCallOptions()), responseObserver);
    }

    /**
     * <pre>
     * ── On-demand fetch (incl. by tenant) ─────────────────────────────────────
     * </pre>
     */
    public void getResources(com.udb.core.control.services.v1.GetResourcesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.GetResourcesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetResourcesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── Admin visibility ──────────────────────────────────────────────────────
     * </pre>
     */
    public void listNodeStates(com.udb.core.control.services.v1.ListNodeStatesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.ListNodeStatesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListNodeStatesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void ackStatus(com.udb.core.control.services.v1.AckStatusRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.AckStatusResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAckStatusMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service ControlPlaneService.
   * <pre>
   * ---------------------------------------------------------------------------
   * ControlPlaneService — Versioned, ACK/NACK, nonce-paired, ordered control-plane
   * policy/config distribution (xDS-style). Nodes (data-plane PEPs) open an
   * aggregated state-of-the-world stream (StreamResources) or an incremental delta
   * stream (DeltaResources) and receive versioned resources in dependency order
   * (backend-target definitions before referencing routing/RLS policies), each
   * response carrying a fresh nonce the node echoes to ACK (apply) or NACK (reject
   * without applying). A node that NACKs keeps its last-good version. Unary helpers
   * fetch resources on demand (incl. by tenant) and expose per-node ack visibility.
   *
   * Server-only control plane: runs on the isolated native auth listener with an
   * admin/service-account credential; never exposed on the public DataBroker port.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static final class ControlPlaneServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<ControlPlaneServiceBlockingV2Stub> {
    private ControlPlaneServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ControlPlaneServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ControlPlaneServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * ── Aggregated state-of-the-world (ADS) ───────────────────────────────────
     * </pre>
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<com.udb.core.control.services.v1.DiscoveryRequest, com.udb.core.control.services.v1.DiscoveryResponse>
        streamResources() {
      return io.grpc.stub.ClientCalls.blockingBidiStreamingCall(
          getChannel(), getStreamResourcesMethod(), getCallOptions());
    }

    /**
     * <pre>
     * ── Incremental / delta discovery ─────────────────────────────────────────
     * </pre>
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<com.udb.core.control.services.v1.DeltaDiscoveryRequest, com.udb.core.control.services.v1.DeltaDiscoveryResponse>
        deltaResources() {
      return io.grpc.stub.ClientCalls.blockingBidiStreamingCall(
          getChannel(), getDeltaResourcesMethod(), getCallOptions());
    }

    /**
     * <pre>
     * ── On-demand fetch (incl. by tenant) ─────────────────────────────────────
     * </pre>
     */
    public com.udb.core.control.services.v1.GetResourcesResponse getResources(com.udb.core.control.services.v1.GetResourcesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetResourcesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Admin visibility ──────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.control.services.v1.ListNodeStatesResponse listNodeStates(com.udb.core.control.services.v1.ListNodeStatesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListNodeStatesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.control.services.v1.AckStatusResponse ackStatus(com.udb.core.control.services.v1.AckStatusRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getAckStatusMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service ControlPlaneService.
   * <pre>
   * ---------------------------------------------------------------------------
   * ControlPlaneService — Versioned, ACK/NACK, nonce-paired, ordered control-plane
   * policy/config distribution (xDS-style). Nodes (data-plane PEPs) open an
   * aggregated state-of-the-world stream (StreamResources) or an incremental delta
   * stream (DeltaResources) and receive versioned resources in dependency order
   * (backend-target definitions before referencing routing/RLS policies), each
   * response carrying a fresh nonce the node echoes to ACK (apply) or NACK (reject
   * without applying). A node that NACKs keeps its last-good version. Unary helpers
   * fetch resources on demand (incl. by tenant) and expose per-node ack visibility.
   *
   * Server-only control plane: runs on the isolated native auth listener with an
   * admin/service-account credential; never exposed on the public DataBroker port.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static final class ControlPlaneServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<ControlPlaneServiceBlockingStub> {
    private ControlPlaneServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ControlPlaneServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ControlPlaneServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * ── On-demand fetch (incl. by tenant) ─────────────────────────────────────
     * </pre>
     */
    public com.udb.core.control.services.v1.GetResourcesResponse getResources(com.udb.core.control.services.v1.GetResourcesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetResourcesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Admin visibility ──────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.control.services.v1.ListNodeStatesResponse listNodeStates(com.udb.core.control.services.v1.ListNodeStatesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListNodeStatesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.control.services.v1.AckStatusResponse ackStatus(com.udb.core.control.services.v1.AckStatusRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAckStatusMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service ControlPlaneService.
   * <pre>
   * ---------------------------------------------------------------------------
   * ControlPlaneService — Versioned, ACK/NACK, nonce-paired, ordered control-plane
   * policy/config distribution (xDS-style). Nodes (data-plane PEPs) open an
   * aggregated state-of-the-world stream (StreamResources) or an incremental delta
   * stream (DeltaResources) and receive versioned resources in dependency order
   * (backend-target definitions before referencing routing/RLS policies), each
   * response carrying a fresh nonce the node echoes to ACK (apply) or NACK (reject
   * without applying). A node that NACKs keeps its last-good version. Unary helpers
   * fetch resources on demand (incl. by tenant) and expose per-node ack visibility.
   *
   * Server-only control plane: runs on the isolated native auth listener with an
   * admin/service-account credential; never exposed on the public DataBroker port.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static final class ControlPlaneServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<ControlPlaneServiceFutureStub> {
    private ControlPlaneServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ControlPlaneServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ControlPlaneServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * ── On-demand fetch (incl. by tenant) ─────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.control.services.v1.GetResourcesResponse> getResources(
        com.udb.core.control.services.v1.GetResourcesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetResourcesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── Admin visibility ──────────────────────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.control.services.v1.ListNodeStatesResponse> listNodeStates(
        com.udb.core.control.services.v1.ListNodeStatesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListNodeStatesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.control.services.v1.AckStatusResponse> ackStatus(
        com.udb.core.control.services.v1.AckStatusRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAckStatusMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_RESOURCES = 0;
  private static final int METHODID_LIST_NODE_STATES = 1;
  private static final int METHODID_ACK_STATUS = 2;
  private static final int METHODID_STREAM_RESOURCES = 3;
  private static final int METHODID_DELTA_RESOURCES = 4;

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
        case METHODID_GET_RESOURCES:
          serviceImpl.getResources((com.udb.core.control.services.v1.GetResourcesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.GetResourcesResponse>) responseObserver);
          break;
        case METHODID_LIST_NODE_STATES:
          serviceImpl.listNodeStates((com.udb.core.control.services.v1.ListNodeStatesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.ListNodeStatesResponse>) responseObserver);
          break;
        case METHODID_ACK_STATUS:
          serviceImpl.ackStatus((com.udb.core.control.services.v1.AckStatusRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.AckStatusResponse>) responseObserver);
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
        case METHODID_STREAM_RESOURCES:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.streamResources(
              (io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.DiscoveryResponse>) responseObserver);
        case METHODID_DELTA_RESOURCES:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.deltaResources(
              (io.grpc.stub.StreamObserver<com.udb.core.control.services.v1.DeltaDiscoveryResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getStreamResourcesMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              com.udb.core.control.services.v1.DiscoveryRequest,
              com.udb.core.control.services.v1.DiscoveryResponse>(
                service, METHODID_STREAM_RESOURCES)))
        .addMethod(
          getDeltaResourcesMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              com.udb.core.control.services.v1.DeltaDiscoveryRequest,
              com.udb.core.control.services.v1.DeltaDiscoveryResponse>(
                service, METHODID_DELTA_RESOURCES)))
        .addMethod(
          getGetResourcesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.control.services.v1.GetResourcesRequest,
              com.udb.core.control.services.v1.GetResourcesResponse>(
                service, METHODID_GET_RESOURCES)))
        .addMethod(
          getListNodeStatesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.control.services.v1.ListNodeStatesRequest,
              com.udb.core.control.services.v1.ListNodeStatesResponse>(
                service, METHODID_LIST_NODE_STATES)))
        .addMethod(
          getAckStatusMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.control.services.v1.AckStatusRequest,
              com.udb.core.control.services.v1.AckStatusResponse>(
                service, METHODID_ACK_STATUS)))
        .build();
  }

  private static abstract class ControlPlaneServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ControlPlaneServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.control.services.v1.ControlPlaneServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ControlPlaneService");
    }
  }

  private static final class ControlPlaneServiceFileDescriptorSupplier
      extends ControlPlaneServiceBaseDescriptorSupplier {
    ControlPlaneServiceFileDescriptorSupplier() {}
  }

  private static final class ControlPlaneServiceMethodDescriptorSupplier
      extends ControlPlaneServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    ControlPlaneServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (ControlPlaneServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ControlPlaneServiceFileDescriptorSupplier())
              .addMethod(getStreamResourcesMethod())
              .addMethod(getDeltaResourcesMethod())
              .addMethod(getGetResourcesMethod())
              .addMethod(getListNodeStatesMethod())
              .addMethod(getAckStatusMethod())
              .build();
        }
      }
    }
    return result;
  }
}
