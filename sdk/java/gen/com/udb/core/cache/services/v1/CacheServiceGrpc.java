package com.udb.core.cache.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * CacheService (master-plan 9.6) — a cache that invalidates itself.
 * This PROMOTES the four typed DataBroker cache RPCs (CacheGet/CacheSet/
 * CacheDelete/CacheScan, which remain as additive aliases) into a first-class
 * native service with bounded, namespaced, claim-scoped keys. Every entry lives
 * under `udb:cache:&lt;tenant&gt;:&lt;ns&gt;:&lt;key&gt;` where `&lt;tenant&gt;` is derived from the
 * VERIFIED bearer/claim tenant — never a body-supplied value — so two tenants
 * can use the same namespace and key without colliding and a caller can never
 * read or sweep another tenant's namespace. Each namespace carries a per-tenant
 * memory budget (`max_bytes`); a Set that would exceed it fails closed with
 * `resource_exhausted`. Prefix sweeps use Redis `SCAN`, never `KEYS`. A
 * leader-elected CDC invalidation worker maps source-table changes to a
 * namespace sweep and emits `udb.cache.invalidated.v1`.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class CacheServiceGrpc {

  private CacheServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.cache.services.v1.CacheService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.GetRequest,
      com.udb.core.cache.services.v1.GetResponse> getGetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Get",
      requestType = com.udb.core.cache.services.v1.GetRequest.class,
      responseType = com.udb.core.cache.services.v1.GetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.GetRequest,
      com.udb.core.cache.services.v1.GetResponse> getGetMethod() {
    io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.GetRequest, com.udb.core.cache.services.v1.GetResponse> getGetMethod;
    if ((getGetMethod = CacheServiceGrpc.getGetMethod) == null) {
      synchronized (CacheServiceGrpc.class) {
        if ((getGetMethod = CacheServiceGrpc.getGetMethod) == null) {
          CacheServiceGrpc.getGetMethod = getGetMethod =
              io.grpc.MethodDescriptor.<com.udb.core.cache.services.v1.GetRequest, com.udb.core.cache.services.v1.GetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Get"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.cache.services.v1.GetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.cache.services.v1.GetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CacheServiceMethodDescriptorSupplier("Get"))
              .build();
        }
      }
    }
    return getGetMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.SetRequest,
      com.udb.core.cache.services.v1.SetResponse> getSetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Set",
      requestType = com.udb.core.cache.services.v1.SetRequest.class,
      responseType = com.udb.core.cache.services.v1.SetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.SetRequest,
      com.udb.core.cache.services.v1.SetResponse> getSetMethod() {
    io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.SetRequest, com.udb.core.cache.services.v1.SetResponse> getSetMethod;
    if ((getSetMethod = CacheServiceGrpc.getSetMethod) == null) {
      synchronized (CacheServiceGrpc.class) {
        if ((getSetMethod = CacheServiceGrpc.getSetMethod) == null) {
          CacheServiceGrpc.getSetMethod = getSetMethod =
              io.grpc.MethodDescriptor.<com.udb.core.cache.services.v1.SetRequest, com.udb.core.cache.services.v1.SetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Set"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.cache.services.v1.SetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.cache.services.v1.SetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CacheServiceMethodDescriptorSupplier("Set"))
              .build();
        }
      }
    }
    return getSetMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.DeleteRequest,
      com.udb.core.cache.services.v1.DeleteResponse> getDeleteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Delete",
      requestType = com.udb.core.cache.services.v1.DeleteRequest.class,
      responseType = com.udb.core.cache.services.v1.DeleteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.DeleteRequest,
      com.udb.core.cache.services.v1.DeleteResponse> getDeleteMethod() {
    io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.DeleteRequest, com.udb.core.cache.services.v1.DeleteResponse> getDeleteMethod;
    if ((getDeleteMethod = CacheServiceGrpc.getDeleteMethod) == null) {
      synchronized (CacheServiceGrpc.class) {
        if ((getDeleteMethod = CacheServiceGrpc.getDeleteMethod) == null) {
          CacheServiceGrpc.getDeleteMethod = getDeleteMethod =
              io.grpc.MethodDescriptor.<com.udb.core.cache.services.v1.DeleteRequest, com.udb.core.cache.services.v1.DeleteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Delete"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.cache.services.v1.DeleteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.cache.services.v1.DeleteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CacheServiceMethodDescriptorSupplier("Delete"))
              .build();
        }
      }
    }
    return getDeleteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.ScanRequest,
      com.udb.core.cache.services.v1.ScanResponse> getScanMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Scan",
      requestType = com.udb.core.cache.services.v1.ScanRequest.class,
      responseType = com.udb.core.cache.services.v1.ScanResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.ScanRequest,
      com.udb.core.cache.services.v1.ScanResponse> getScanMethod() {
    io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.ScanRequest, com.udb.core.cache.services.v1.ScanResponse> getScanMethod;
    if ((getScanMethod = CacheServiceGrpc.getScanMethod) == null) {
      synchronized (CacheServiceGrpc.class) {
        if ((getScanMethod = CacheServiceGrpc.getScanMethod) == null) {
          CacheServiceGrpc.getScanMethod = getScanMethod =
              io.grpc.MethodDescriptor.<com.udb.core.cache.services.v1.ScanRequest, com.udb.core.cache.services.v1.ScanResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Scan"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.cache.services.v1.ScanRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.cache.services.v1.ScanResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CacheServiceMethodDescriptorSupplier("Scan"))
              .build();
        }
      }
    }
    return getScanMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.CreateNamespaceRequest,
      com.udb.core.cache.services.v1.CreateNamespaceResponse> getCreateNamespaceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateNamespace",
      requestType = com.udb.core.cache.services.v1.CreateNamespaceRequest.class,
      responseType = com.udb.core.cache.services.v1.CreateNamespaceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.CreateNamespaceRequest,
      com.udb.core.cache.services.v1.CreateNamespaceResponse> getCreateNamespaceMethod() {
    io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.CreateNamespaceRequest, com.udb.core.cache.services.v1.CreateNamespaceResponse> getCreateNamespaceMethod;
    if ((getCreateNamespaceMethod = CacheServiceGrpc.getCreateNamespaceMethod) == null) {
      synchronized (CacheServiceGrpc.class) {
        if ((getCreateNamespaceMethod = CacheServiceGrpc.getCreateNamespaceMethod) == null) {
          CacheServiceGrpc.getCreateNamespaceMethod = getCreateNamespaceMethod =
              io.grpc.MethodDescriptor.<com.udb.core.cache.services.v1.CreateNamespaceRequest, com.udb.core.cache.services.v1.CreateNamespaceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateNamespace"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.cache.services.v1.CreateNamespaceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.cache.services.v1.CreateNamespaceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CacheServiceMethodDescriptorSupplier("CreateNamespace"))
              .build();
        }
      }
    }
    return getCreateNamespaceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.DeleteNamespaceRequest,
      com.udb.core.cache.services.v1.DeleteNamespaceResponse> getDeleteNamespaceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteNamespace",
      requestType = com.udb.core.cache.services.v1.DeleteNamespaceRequest.class,
      responseType = com.udb.core.cache.services.v1.DeleteNamespaceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.DeleteNamespaceRequest,
      com.udb.core.cache.services.v1.DeleteNamespaceResponse> getDeleteNamespaceMethod() {
    io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.DeleteNamespaceRequest, com.udb.core.cache.services.v1.DeleteNamespaceResponse> getDeleteNamespaceMethod;
    if ((getDeleteNamespaceMethod = CacheServiceGrpc.getDeleteNamespaceMethod) == null) {
      synchronized (CacheServiceGrpc.class) {
        if ((getDeleteNamespaceMethod = CacheServiceGrpc.getDeleteNamespaceMethod) == null) {
          CacheServiceGrpc.getDeleteNamespaceMethod = getDeleteNamespaceMethod =
              io.grpc.MethodDescriptor.<com.udb.core.cache.services.v1.DeleteNamespaceRequest, com.udb.core.cache.services.v1.DeleteNamespaceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteNamespace"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.cache.services.v1.DeleteNamespaceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.cache.services.v1.DeleteNamespaceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CacheServiceMethodDescriptorSupplier("DeleteNamespace"))
              .build();
        }
      }
    }
    return getDeleteNamespaceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.GetNamespaceStatsRequest,
      com.udb.core.cache.services.v1.GetNamespaceStatsResponse> getGetNamespaceStatsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetNamespaceStats",
      requestType = com.udb.core.cache.services.v1.GetNamespaceStatsRequest.class,
      responseType = com.udb.core.cache.services.v1.GetNamespaceStatsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.GetNamespaceStatsRequest,
      com.udb.core.cache.services.v1.GetNamespaceStatsResponse> getGetNamespaceStatsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.cache.services.v1.GetNamespaceStatsRequest, com.udb.core.cache.services.v1.GetNamespaceStatsResponse> getGetNamespaceStatsMethod;
    if ((getGetNamespaceStatsMethod = CacheServiceGrpc.getGetNamespaceStatsMethod) == null) {
      synchronized (CacheServiceGrpc.class) {
        if ((getGetNamespaceStatsMethod = CacheServiceGrpc.getGetNamespaceStatsMethod) == null) {
          CacheServiceGrpc.getGetNamespaceStatsMethod = getGetNamespaceStatsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.cache.services.v1.GetNamespaceStatsRequest, com.udb.core.cache.services.v1.GetNamespaceStatsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetNamespaceStats"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.cache.services.v1.GetNamespaceStatsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.cache.services.v1.GetNamespaceStatsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CacheServiceMethodDescriptorSupplier("GetNamespaceStats"))
              .build();
        }
      }
    }
    return getGetNamespaceStatsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static CacheServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CacheServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CacheServiceStub>() {
        @java.lang.Override
        public CacheServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CacheServiceStub(channel, callOptions);
        }
      };
    return CacheServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static CacheServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CacheServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CacheServiceBlockingV2Stub>() {
        @java.lang.Override
        public CacheServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CacheServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return CacheServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static CacheServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CacheServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CacheServiceBlockingStub>() {
        @java.lang.Override
        public CacheServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CacheServiceBlockingStub(channel, callOptions);
        }
      };
    return CacheServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static CacheServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CacheServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CacheServiceFutureStub>() {
        @java.lang.Override
        public CacheServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CacheServiceFutureStub(channel, callOptions);
        }
      };
    return CacheServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * CacheService (master-plan 9.6) — a cache that invalidates itself.
   * This PROMOTES the four typed DataBroker cache RPCs (CacheGet/CacheSet/
   * CacheDelete/CacheScan, which remain as additive aliases) into a first-class
   * native service with bounded, namespaced, claim-scoped keys. Every entry lives
   * under `udb:cache:&lt;tenant&gt;:&lt;ns&gt;:&lt;key&gt;` where `&lt;tenant&gt;` is derived from the
   * VERIFIED bearer/claim tenant — never a body-supplied value — so two tenants
   * can use the same namespace and key without colliding and a caller can never
   * read or sweep another tenant's namespace. Each namespace carries a per-tenant
   * memory budget (`max_bytes`); a Set that would exceed it fails closed with
   * `resource_exhausted`. Prefix sweeps use Redis `SCAN`, never `KEYS`. A
   * leader-elected CDC invalidation worker maps source-table changes to a
   * namespace sweep and emits `udb.cache.invalidated.v1`.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Read a value from a namespaced cache key. Tenant-scoped: the key is derived
     * from the verified claim tenant, so a caller can never read another tenant's
     * entry by spoofing the body tenant_id.
     * </pre>
     */
    default void get(com.udb.core.cache.services.v1.GetRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.GetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetMethod(), responseObserver);
    }

    /**
     * <pre>
     * Write a value with an optional TTL. Bounded: a write that would push the
     * namespace over its per-tenant `max_bytes` budget fails closed with
     * `resource_exhausted`.
     * </pre>
     */
    default void set(com.udb.core.cache.services.v1.SetRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.SetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetMethod(), responseObserver);
    }

    /**
     * <pre>
     * Delete a single namespaced key. Idempotent.
     * </pre>
     */
    default void delete(com.udb.core.cache.services.v1.DeleteRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.DeleteResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteMethod(), responseObserver);
    }

    /**
     * <pre>
     * Cursor-paged scan over a namespace key prefix. Implemented with Redis SCAN
     * (never KEYS), so it never blocks the server on a large keyspace.
     * </pre>
     */
    default void scan(com.udb.core.cache.services.v1.ScanRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.ScanResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScanMethod(), responseObserver);
    }

    /**
     * <pre>
     * Declare (or update) a namespace and its per-tenant byte budget + default TTL.
     * </pre>
     */
    default void createNamespace(com.udb.core.cache.services.v1.CreateNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.CreateNamespaceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateNamespaceMethod(), responseObserver);
    }

    /**
     * <pre>
     * Flush an entire namespace for the caller's tenant (SCAN+DEL sweep) and emit
     * an invalidation event. DESTRUCTIVE — gated by a confirmation token.
     * </pre>
     */
    default void deleteNamespace(com.udb.core.cache.services.v1.DeleteNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.DeleteNamespaceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteNamespaceMethod(), responseObserver);
    }

    /**
     * <pre>
     * Report a namespace's current used-bytes counter, configured budget, and item
     * count for the caller's tenant.
     * </pre>
     */
    default void getNamespaceStats(com.udb.core.cache.services.v1.GetNamespaceStatsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.GetNamespaceStatsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetNamespaceStatsMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service CacheService.
   * <pre>
   * CacheService (master-plan 9.6) — a cache that invalidates itself.
   * This PROMOTES the four typed DataBroker cache RPCs (CacheGet/CacheSet/
   * CacheDelete/CacheScan, which remain as additive aliases) into a first-class
   * native service with bounded, namespaced, claim-scoped keys. Every entry lives
   * under `udb:cache:&lt;tenant&gt;:&lt;ns&gt;:&lt;key&gt;` where `&lt;tenant&gt;` is derived from the
   * VERIFIED bearer/claim tenant — never a body-supplied value — so two tenants
   * can use the same namespace and key without colliding and a caller can never
   * read or sweep another tenant's namespace. Each namespace carries a per-tenant
   * memory budget (`max_bytes`); a Set that would exceed it fails closed with
   * `resource_exhausted`. Prefix sweeps use Redis `SCAN`, never `KEYS`. A
   * leader-elected CDC invalidation worker maps source-table changes to a
   * namespace sweep and emits `udb.cache.invalidated.v1`.
   * </pre>
   */
  public static abstract class CacheServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return CacheServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service CacheService.
   * <pre>
   * CacheService (master-plan 9.6) — a cache that invalidates itself.
   * This PROMOTES the four typed DataBroker cache RPCs (CacheGet/CacheSet/
   * CacheDelete/CacheScan, which remain as additive aliases) into a first-class
   * native service with bounded, namespaced, claim-scoped keys. Every entry lives
   * under `udb:cache:&lt;tenant&gt;:&lt;ns&gt;:&lt;key&gt;` where `&lt;tenant&gt;` is derived from the
   * VERIFIED bearer/claim tenant — never a body-supplied value — so two tenants
   * can use the same namespace and key without colliding and a caller can never
   * read or sweep another tenant's namespace. Each namespace carries a per-tenant
   * memory budget (`max_bytes`); a Set that would exceed it fails closed with
   * `resource_exhausted`. Prefix sweeps use Redis `SCAN`, never `KEYS`. A
   * leader-elected CDC invalidation worker maps source-table changes to a
   * namespace sweep and emits `udb.cache.invalidated.v1`.
   * </pre>
   */
  public static final class CacheServiceStub
      extends io.grpc.stub.AbstractAsyncStub<CacheServiceStub> {
    private CacheServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CacheServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CacheServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Read a value from a namespaced cache key. Tenant-scoped: the key is derived
     * from the verified claim tenant, so a caller can never read another tenant's
     * entry by spoofing the body tenant_id.
     * </pre>
     */
    public void get(com.udb.core.cache.services.v1.GetRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.GetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Write a value with an optional TTL. Bounded: a write that would push the
     * namespace over its per-tenant `max_bytes` budget fails closed with
     * `resource_exhausted`.
     * </pre>
     */
    public void set(com.udb.core.cache.services.v1.SetRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.SetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Delete a single namespaced key. Idempotent.
     * </pre>
     */
    public void delete(com.udb.core.cache.services.v1.DeleteRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.DeleteResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Cursor-paged scan over a namespace key prefix. Implemented with Redis SCAN
     * (never KEYS), so it never blocks the server on a large keyspace.
     * </pre>
     */
    public void scan(com.udb.core.cache.services.v1.ScanRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.ScanResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScanMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Declare (or update) a namespace and its per-tenant byte budget + default TTL.
     * </pre>
     */
    public void createNamespace(com.udb.core.cache.services.v1.CreateNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.CreateNamespaceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateNamespaceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Flush an entire namespace for the caller's tenant (SCAN+DEL sweep) and emit
     * an invalidation event. DESTRUCTIVE — gated by a confirmation token.
     * </pre>
     */
    public void deleteNamespace(com.udb.core.cache.services.v1.DeleteNamespaceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.DeleteNamespaceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteNamespaceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Report a namespace's current used-bytes counter, configured budget, and item
     * count for the caller's tenant.
     * </pre>
     */
    public void getNamespaceStats(com.udb.core.cache.services.v1.GetNamespaceStatsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.GetNamespaceStatsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetNamespaceStatsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service CacheService.
   * <pre>
   * CacheService (master-plan 9.6) — a cache that invalidates itself.
   * This PROMOTES the four typed DataBroker cache RPCs (CacheGet/CacheSet/
   * CacheDelete/CacheScan, which remain as additive aliases) into a first-class
   * native service with bounded, namespaced, claim-scoped keys. Every entry lives
   * under `udb:cache:&lt;tenant&gt;:&lt;ns&gt;:&lt;key&gt;` where `&lt;tenant&gt;` is derived from the
   * VERIFIED bearer/claim tenant — never a body-supplied value — so two tenants
   * can use the same namespace and key without colliding and a caller can never
   * read or sweep another tenant's namespace. Each namespace carries a per-tenant
   * memory budget (`max_bytes`); a Set that would exceed it fails closed with
   * `resource_exhausted`. Prefix sweeps use Redis `SCAN`, never `KEYS`. A
   * leader-elected CDC invalidation worker maps source-table changes to a
   * namespace sweep and emits `udb.cache.invalidated.v1`.
   * </pre>
   */
  public static final class CacheServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<CacheServiceBlockingV2Stub> {
    private CacheServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CacheServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CacheServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Read a value from a namespaced cache key. Tenant-scoped: the key is derived
     * from the verified claim tenant, so a caller can never read another tenant's
     * entry by spoofing the body tenant_id.
     * </pre>
     */
    public com.udb.core.cache.services.v1.GetResponse get(com.udb.core.cache.services.v1.GetRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Write a value with an optional TTL. Bounded: a write that would push the
     * namespace over its per-tenant `max_bytes` budget fails closed with
     * `resource_exhausted`.
     * </pre>
     */
    public com.udb.core.cache.services.v1.SetResponse set(com.udb.core.cache.services.v1.SetRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSetMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a single namespaced key. Idempotent.
     * </pre>
     */
    public com.udb.core.cache.services.v1.DeleteResponse delete(com.udb.core.cache.services.v1.DeleteRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeleteMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Cursor-paged scan over a namespace key prefix. Implemented with Redis SCAN
     * (never KEYS), so it never blocks the server on a large keyspace.
     * </pre>
     */
    public com.udb.core.cache.services.v1.ScanResponse scan(com.udb.core.cache.services.v1.ScanRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getScanMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Declare (or update) a namespace and its per-tenant byte budget + default TTL.
     * </pre>
     */
    public com.udb.core.cache.services.v1.CreateNamespaceResponse createNamespace(com.udb.core.cache.services.v1.CreateNamespaceRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreateNamespaceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Flush an entire namespace for the caller's tenant (SCAN+DEL sweep) and emit
     * an invalidation event. DESTRUCTIVE — gated by a confirmation token.
     * </pre>
     */
    public com.udb.core.cache.services.v1.DeleteNamespaceResponse deleteNamespace(com.udb.core.cache.services.v1.DeleteNamespaceRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeleteNamespaceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Report a namespace's current used-bytes counter, configured budget, and item
     * count for the caller's tenant.
     * </pre>
     */
    public com.udb.core.cache.services.v1.GetNamespaceStatsResponse getNamespaceStats(com.udb.core.cache.services.v1.GetNamespaceStatsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetNamespaceStatsMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service CacheService.
   * <pre>
   * CacheService (master-plan 9.6) — a cache that invalidates itself.
   * This PROMOTES the four typed DataBroker cache RPCs (CacheGet/CacheSet/
   * CacheDelete/CacheScan, which remain as additive aliases) into a first-class
   * native service with bounded, namespaced, claim-scoped keys. Every entry lives
   * under `udb:cache:&lt;tenant&gt;:&lt;ns&gt;:&lt;key&gt;` where `&lt;tenant&gt;` is derived from the
   * VERIFIED bearer/claim tenant — never a body-supplied value — so two tenants
   * can use the same namespace and key without colliding and a caller can never
   * read or sweep another tenant's namespace. Each namespace carries a per-tenant
   * memory budget (`max_bytes`); a Set that would exceed it fails closed with
   * `resource_exhausted`. Prefix sweeps use Redis `SCAN`, never `KEYS`. A
   * leader-elected CDC invalidation worker maps source-table changes to a
   * namespace sweep and emits `udb.cache.invalidated.v1`.
   * </pre>
   */
  public static final class CacheServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<CacheServiceBlockingStub> {
    private CacheServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CacheServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CacheServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Read a value from a namespaced cache key. Tenant-scoped: the key is derived
     * from the verified claim tenant, so a caller can never read another tenant's
     * entry by spoofing the body tenant_id.
     * </pre>
     */
    public com.udb.core.cache.services.v1.GetResponse get(com.udb.core.cache.services.v1.GetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Write a value with an optional TTL. Bounded: a write that would push the
     * namespace over its per-tenant `max_bytes` budget fails closed with
     * `resource_exhausted`.
     * </pre>
     */
    public com.udb.core.cache.services.v1.SetResponse set(com.udb.core.cache.services.v1.SetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a single namespaced key. Idempotent.
     * </pre>
     */
    public com.udb.core.cache.services.v1.DeleteResponse delete(com.udb.core.cache.services.v1.DeleteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Cursor-paged scan over a namespace key prefix. Implemented with Redis SCAN
     * (never KEYS), so it never blocks the server on a large keyspace.
     * </pre>
     */
    public com.udb.core.cache.services.v1.ScanResponse scan(com.udb.core.cache.services.v1.ScanRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScanMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Declare (or update) a namespace and its per-tenant byte budget + default TTL.
     * </pre>
     */
    public com.udb.core.cache.services.v1.CreateNamespaceResponse createNamespace(com.udb.core.cache.services.v1.CreateNamespaceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateNamespaceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Flush an entire namespace for the caller's tenant (SCAN+DEL sweep) and emit
     * an invalidation event. DESTRUCTIVE — gated by a confirmation token.
     * </pre>
     */
    public com.udb.core.cache.services.v1.DeleteNamespaceResponse deleteNamespace(com.udb.core.cache.services.v1.DeleteNamespaceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteNamespaceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Report a namespace's current used-bytes counter, configured budget, and item
     * count for the caller's tenant.
     * </pre>
     */
    public com.udb.core.cache.services.v1.GetNamespaceStatsResponse getNamespaceStats(com.udb.core.cache.services.v1.GetNamespaceStatsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetNamespaceStatsMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service CacheService.
   * <pre>
   * CacheService (master-plan 9.6) — a cache that invalidates itself.
   * This PROMOTES the four typed DataBroker cache RPCs (CacheGet/CacheSet/
   * CacheDelete/CacheScan, which remain as additive aliases) into a first-class
   * native service with bounded, namespaced, claim-scoped keys. Every entry lives
   * under `udb:cache:&lt;tenant&gt;:&lt;ns&gt;:&lt;key&gt;` where `&lt;tenant&gt;` is derived from the
   * VERIFIED bearer/claim tenant — never a body-supplied value — so two tenants
   * can use the same namespace and key without colliding and a caller can never
   * read or sweep another tenant's namespace. Each namespace carries a per-tenant
   * memory budget (`max_bytes`); a Set that would exceed it fails closed with
   * `resource_exhausted`. Prefix sweeps use Redis `SCAN`, never `KEYS`. A
   * leader-elected CDC invalidation worker maps source-table changes to a
   * namespace sweep and emits `udb.cache.invalidated.v1`.
   * </pre>
   */
  public static final class CacheServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<CacheServiceFutureStub> {
    private CacheServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CacheServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CacheServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Read a value from a namespaced cache key. Tenant-scoped: the key is derived
     * from the verified claim tenant, so a caller can never read another tenant's
     * entry by spoofing the body tenant_id.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.cache.services.v1.GetResponse> get(
        com.udb.core.cache.services.v1.GetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Write a value with an optional TTL. Bounded: a write that would push the
     * namespace over its per-tenant `max_bytes` budget fails closed with
     * `resource_exhausted`.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.cache.services.v1.SetResponse> set(
        com.udb.core.cache.services.v1.SetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Delete a single namespaced key. Idempotent.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.cache.services.v1.DeleteResponse> delete(
        com.udb.core.cache.services.v1.DeleteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Cursor-paged scan over a namespace key prefix. Implemented with Redis SCAN
     * (never KEYS), so it never blocks the server on a large keyspace.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.cache.services.v1.ScanResponse> scan(
        com.udb.core.cache.services.v1.ScanRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScanMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Declare (or update) a namespace and its per-tenant byte budget + default TTL.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.cache.services.v1.CreateNamespaceResponse> createNamespace(
        com.udb.core.cache.services.v1.CreateNamespaceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateNamespaceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Flush an entire namespace for the caller's tenant (SCAN+DEL sweep) and emit
     * an invalidation event. DESTRUCTIVE — gated by a confirmation token.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.cache.services.v1.DeleteNamespaceResponse> deleteNamespace(
        com.udb.core.cache.services.v1.DeleteNamespaceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteNamespaceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Report a namespace's current used-bytes counter, configured budget, and item
     * count for the caller's tenant.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.cache.services.v1.GetNamespaceStatsResponse> getNamespaceStats(
        com.udb.core.cache.services.v1.GetNamespaceStatsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetNamespaceStatsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET = 0;
  private static final int METHODID_SET = 1;
  private static final int METHODID_DELETE = 2;
  private static final int METHODID_SCAN = 3;
  private static final int METHODID_CREATE_NAMESPACE = 4;
  private static final int METHODID_DELETE_NAMESPACE = 5;
  private static final int METHODID_GET_NAMESPACE_STATS = 6;

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
        case METHODID_GET:
          serviceImpl.get((com.udb.core.cache.services.v1.GetRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.GetResponse>) responseObserver);
          break;
        case METHODID_SET:
          serviceImpl.set((com.udb.core.cache.services.v1.SetRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.SetResponse>) responseObserver);
          break;
        case METHODID_DELETE:
          serviceImpl.delete((com.udb.core.cache.services.v1.DeleteRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.DeleteResponse>) responseObserver);
          break;
        case METHODID_SCAN:
          serviceImpl.scan((com.udb.core.cache.services.v1.ScanRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.ScanResponse>) responseObserver);
          break;
        case METHODID_CREATE_NAMESPACE:
          serviceImpl.createNamespace((com.udb.core.cache.services.v1.CreateNamespaceRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.CreateNamespaceResponse>) responseObserver);
          break;
        case METHODID_DELETE_NAMESPACE:
          serviceImpl.deleteNamespace((com.udb.core.cache.services.v1.DeleteNamespaceRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.DeleteNamespaceResponse>) responseObserver);
          break;
        case METHODID_GET_NAMESPACE_STATS:
          serviceImpl.getNamespaceStats((com.udb.core.cache.services.v1.GetNamespaceStatsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.cache.services.v1.GetNamespaceStatsResponse>) responseObserver);
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
          getGetMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.cache.services.v1.GetRequest,
              com.udb.core.cache.services.v1.GetResponse>(
                service, METHODID_GET)))
        .addMethod(
          getSetMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.cache.services.v1.SetRequest,
              com.udb.core.cache.services.v1.SetResponse>(
                service, METHODID_SET)))
        .addMethod(
          getDeleteMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.cache.services.v1.DeleteRequest,
              com.udb.core.cache.services.v1.DeleteResponse>(
                service, METHODID_DELETE)))
        .addMethod(
          getScanMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.cache.services.v1.ScanRequest,
              com.udb.core.cache.services.v1.ScanResponse>(
                service, METHODID_SCAN)))
        .addMethod(
          getCreateNamespaceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.cache.services.v1.CreateNamespaceRequest,
              com.udb.core.cache.services.v1.CreateNamespaceResponse>(
                service, METHODID_CREATE_NAMESPACE)))
        .addMethod(
          getDeleteNamespaceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.cache.services.v1.DeleteNamespaceRequest,
              com.udb.core.cache.services.v1.DeleteNamespaceResponse>(
                service, METHODID_DELETE_NAMESPACE)))
        .addMethod(
          getGetNamespaceStatsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.cache.services.v1.GetNamespaceStatsRequest,
              com.udb.core.cache.services.v1.GetNamespaceStatsResponse>(
                service, METHODID_GET_NAMESPACE_STATS)))
        .build();
  }

  private static abstract class CacheServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    CacheServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.cache.services.v1.CacheServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("CacheService");
    }
  }

  private static final class CacheServiceFileDescriptorSupplier
      extends CacheServiceBaseDescriptorSupplier {
    CacheServiceFileDescriptorSupplier() {}
  }

  private static final class CacheServiceMethodDescriptorSupplier
      extends CacheServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    CacheServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (CacheServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new CacheServiceFileDescriptorSupplier())
              .addMethod(getGetMethod())
              .addMethod(getSetMethod())
              .addMethod(getDeleteMethod())
              .addMethod(getScanMethod())
              .addMethod(getCreateNamespaceMethod())
              .addMethod(getDeleteNamespaceMethod())
              .addMethod(getGetNamespaceStatsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
