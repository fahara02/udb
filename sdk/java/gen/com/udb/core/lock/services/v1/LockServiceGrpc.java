package com.udb.core.lock.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * LockService (master-plan 9.2) — distributed locks for applications. Backed by
 * the portable `udb_advisory_leases` mutual-exclusion primitive, with a durable
 * tenant-scoped bookkeeping row and a monotone fencing token per grant so a
 * slow/partitioned holder can be safely fenced off.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class LockServiceGrpc {

  private LockServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.lock.services.v1.LockService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.lock.services.v1.AcquireLockRequest,
      com.udb.core.lock.services.v1.AcquireLockResponse> getAcquireLockMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AcquireLock",
      requestType = com.udb.core.lock.services.v1.AcquireLockRequest.class,
      responseType = com.udb.core.lock.services.v1.AcquireLockResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.lock.services.v1.AcquireLockRequest,
      com.udb.core.lock.services.v1.AcquireLockResponse> getAcquireLockMethod() {
    io.grpc.MethodDescriptor<com.udb.core.lock.services.v1.AcquireLockRequest, com.udb.core.lock.services.v1.AcquireLockResponse> getAcquireLockMethod;
    if ((getAcquireLockMethod = LockServiceGrpc.getAcquireLockMethod) == null) {
      synchronized (LockServiceGrpc.class) {
        if ((getAcquireLockMethod = LockServiceGrpc.getAcquireLockMethod) == null) {
          LockServiceGrpc.getAcquireLockMethod = getAcquireLockMethod =
              io.grpc.MethodDescriptor.<com.udb.core.lock.services.v1.AcquireLockRequest, com.udb.core.lock.services.v1.AcquireLockResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AcquireLock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.lock.services.v1.AcquireLockRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.lock.services.v1.AcquireLockResponse.getDefaultInstance()))
              .setSchemaDescriptor(new LockServiceMethodDescriptorSupplier("AcquireLock"))
              .build();
        }
      }
    }
    return getAcquireLockMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.lock.services.v1.RenewLockRequest,
      com.udb.core.lock.services.v1.RenewLockResponse> getRenewLockMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RenewLock",
      requestType = com.udb.core.lock.services.v1.RenewLockRequest.class,
      responseType = com.udb.core.lock.services.v1.RenewLockResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.lock.services.v1.RenewLockRequest,
      com.udb.core.lock.services.v1.RenewLockResponse> getRenewLockMethod() {
    io.grpc.MethodDescriptor<com.udb.core.lock.services.v1.RenewLockRequest, com.udb.core.lock.services.v1.RenewLockResponse> getRenewLockMethod;
    if ((getRenewLockMethod = LockServiceGrpc.getRenewLockMethod) == null) {
      synchronized (LockServiceGrpc.class) {
        if ((getRenewLockMethod = LockServiceGrpc.getRenewLockMethod) == null) {
          LockServiceGrpc.getRenewLockMethod = getRenewLockMethod =
              io.grpc.MethodDescriptor.<com.udb.core.lock.services.v1.RenewLockRequest, com.udb.core.lock.services.v1.RenewLockResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RenewLock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.lock.services.v1.RenewLockRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.lock.services.v1.RenewLockResponse.getDefaultInstance()))
              .setSchemaDescriptor(new LockServiceMethodDescriptorSupplier("RenewLock"))
              .build();
        }
      }
    }
    return getRenewLockMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.lock.services.v1.ReleaseLockRequest,
      com.udb.core.lock.services.v1.ReleaseLockResponse> getReleaseLockMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReleaseLock",
      requestType = com.udb.core.lock.services.v1.ReleaseLockRequest.class,
      responseType = com.udb.core.lock.services.v1.ReleaseLockResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.lock.services.v1.ReleaseLockRequest,
      com.udb.core.lock.services.v1.ReleaseLockResponse> getReleaseLockMethod() {
    io.grpc.MethodDescriptor<com.udb.core.lock.services.v1.ReleaseLockRequest, com.udb.core.lock.services.v1.ReleaseLockResponse> getReleaseLockMethod;
    if ((getReleaseLockMethod = LockServiceGrpc.getReleaseLockMethod) == null) {
      synchronized (LockServiceGrpc.class) {
        if ((getReleaseLockMethod = LockServiceGrpc.getReleaseLockMethod) == null) {
          LockServiceGrpc.getReleaseLockMethod = getReleaseLockMethod =
              io.grpc.MethodDescriptor.<com.udb.core.lock.services.v1.ReleaseLockRequest, com.udb.core.lock.services.v1.ReleaseLockResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReleaseLock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.lock.services.v1.ReleaseLockRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.lock.services.v1.ReleaseLockResponse.getDefaultInstance()))
              .setSchemaDescriptor(new LockServiceMethodDescriptorSupplier("ReleaseLock"))
              .build();
        }
      }
    }
    return getReleaseLockMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.lock.services.v1.GetLockRequest,
      com.udb.core.lock.services.v1.GetLockResponse> getGetLockMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetLock",
      requestType = com.udb.core.lock.services.v1.GetLockRequest.class,
      responseType = com.udb.core.lock.services.v1.GetLockResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.lock.services.v1.GetLockRequest,
      com.udb.core.lock.services.v1.GetLockResponse> getGetLockMethod() {
    io.grpc.MethodDescriptor<com.udb.core.lock.services.v1.GetLockRequest, com.udb.core.lock.services.v1.GetLockResponse> getGetLockMethod;
    if ((getGetLockMethod = LockServiceGrpc.getGetLockMethod) == null) {
      synchronized (LockServiceGrpc.class) {
        if ((getGetLockMethod = LockServiceGrpc.getGetLockMethod) == null) {
          LockServiceGrpc.getGetLockMethod = getGetLockMethod =
              io.grpc.MethodDescriptor.<com.udb.core.lock.services.v1.GetLockRequest, com.udb.core.lock.services.v1.GetLockResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetLock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.lock.services.v1.GetLockRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.lock.services.v1.GetLockResponse.getDefaultInstance()))
              .setSchemaDescriptor(new LockServiceMethodDescriptorSupplier("GetLock"))
              .build();
        }
      }
    }
    return getGetLockMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.lock.services.v1.ListLocksRequest,
      com.udb.core.lock.services.v1.ListLocksResponse> getListLocksMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListLocks",
      requestType = com.udb.core.lock.services.v1.ListLocksRequest.class,
      responseType = com.udb.core.lock.services.v1.ListLocksResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.lock.services.v1.ListLocksRequest,
      com.udb.core.lock.services.v1.ListLocksResponse> getListLocksMethod() {
    io.grpc.MethodDescriptor<com.udb.core.lock.services.v1.ListLocksRequest, com.udb.core.lock.services.v1.ListLocksResponse> getListLocksMethod;
    if ((getListLocksMethod = LockServiceGrpc.getListLocksMethod) == null) {
      synchronized (LockServiceGrpc.class) {
        if ((getListLocksMethod = LockServiceGrpc.getListLocksMethod) == null) {
          LockServiceGrpc.getListLocksMethod = getListLocksMethod =
              io.grpc.MethodDescriptor.<com.udb.core.lock.services.v1.ListLocksRequest, com.udb.core.lock.services.v1.ListLocksResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListLocks"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.lock.services.v1.ListLocksRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.lock.services.v1.ListLocksResponse.getDefaultInstance()))
              .setSchemaDescriptor(new LockServiceMethodDescriptorSupplier("ListLocks"))
              .build();
        }
      }
    }
    return getListLocksMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static LockServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LockServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LockServiceStub>() {
        @java.lang.Override
        public LockServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LockServiceStub(channel, callOptions);
        }
      };
    return LockServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static LockServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LockServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LockServiceBlockingV2Stub>() {
        @java.lang.Override
        public LockServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LockServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return LockServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static LockServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LockServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LockServiceBlockingStub>() {
        @java.lang.Override
        public LockServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LockServiceBlockingStub(channel, callOptions);
        }
      };
    return LockServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static LockServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LockServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LockServiceFutureStub>() {
        @java.lang.Override
        public LockServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LockServiceFutureStub(channel, callOptions);
        }
      };
    return LockServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * LockService (master-plan 9.2) — distributed locks for applications. Backed by
   * the portable `udb_advisory_leases` mutual-exclusion primitive, with a durable
   * tenant-scoped bookkeeping row and a monotone fencing token per grant so a
   * slow/partitioned holder can be safely fenced off.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Acquire a distributed lock. Quota-aware: a tenant cannot exceed its active
     * lock budget. Returns the monotone fencing token the holder must present on
     * Renew/Release.
     * </pre>
     */
    default void acquireLock(com.udb.core.lock.services.v1.AcquireLockRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.lock.services.v1.AcquireLockResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAcquireLockMethod(), responseObserver);
    }

    /**
     * <pre>
     * Renew (extend the lease of) a lock the caller currently holds. The presented
     * fencing token must not be stale; a lower token is rejected.
     * </pre>
     */
    default void renewLock(com.udb.core.lock.services.v1.RenewLockRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.lock.services.v1.RenewLockResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRenewLockMethod(), responseObserver);
    }

    /**
     * <pre>
     * Release a lock the caller currently holds. The presented fencing token must
     * not be stale; a lower token is rejected.
     * </pre>
     */
    default void releaseLock(com.udb.core.lock.services.v1.ReleaseLockRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.lock.services.v1.ReleaseLockResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReleaseLockMethod(), responseObserver);
    }

    /**
     * <pre>
     * Fetch a single lock by name within the caller's tenant. Read-only; an absent
     * lock returns found=false (not an error) — a tenant-scoped read miss is normal.
     * </pre>
     */
    default void getLock(com.udb.core.lock.services.v1.GetLockRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.lock.services.v1.GetLockResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetLockMethod(), responseObserver);
    }

    /**
     * <pre>
     * List the caller tenant's locks, optionally narrowed by status. Paginated
     * (page_size + opaque page_token). Read-only.
     * </pre>
     */
    default void listLocks(com.udb.core.lock.services.v1.ListLocksRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.lock.services.v1.ListLocksResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListLocksMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service LockService.
   * <pre>
   * LockService (master-plan 9.2) — distributed locks for applications. Backed by
   * the portable `udb_advisory_leases` mutual-exclusion primitive, with a durable
   * tenant-scoped bookkeeping row and a monotone fencing token per grant so a
   * slow/partitioned holder can be safely fenced off.
   * </pre>
   */
  public static abstract class LockServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return LockServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service LockService.
   * <pre>
   * LockService (master-plan 9.2) — distributed locks for applications. Backed by
   * the portable `udb_advisory_leases` mutual-exclusion primitive, with a durable
   * tenant-scoped bookkeeping row and a monotone fencing token per grant so a
   * slow/partitioned holder can be safely fenced off.
   * </pre>
   */
  public static final class LockServiceStub
      extends io.grpc.stub.AbstractAsyncStub<LockServiceStub> {
    private LockServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LockServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LockServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Acquire a distributed lock. Quota-aware: a tenant cannot exceed its active
     * lock budget. Returns the monotone fencing token the holder must present on
     * Renew/Release.
     * </pre>
     */
    public void acquireLock(com.udb.core.lock.services.v1.AcquireLockRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.lock.services.v1.AcquireLockResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAcquireLockMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Renew (extend the lease of) a lock the caller currently holds. The presented
     * fencing token must not be stale; a lower token is rejected.
     * </pre>
     */
    public void renewLock(com.udb.core.lock.services.v1.RenewLockRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.lock.services.v1.RenewLockResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRenewLockMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Release a lock the caller currently holds. The presented fencing token must
     * not be stale; a lower token is rejected.
     * </pre>
     */
    public void releaseLock(com.udb.core.lock.services.v1.ReleaseLockRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.lock.services.v1.ReleaseLockResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReleaseLockMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Fetch a single lock by name within the caller's tenant. Read-only; an absent
     * lock returns found=false (not an error) — a tenant-scoped read miss is normal.
     * </pre>
     */
    public void getLock(com.udb.core.lock.services.v1.GetLockRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.lock.services.v1.GetLockResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetLockMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List the caller tenant's locks, optionally narrowed by status. Paginated
     * (page_size + opaque page_token). Read-only.
     * </pre>
     */
    public void listLocks(com.udb.core.lock.services.v1.ListLocksRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.lock.services.v1.ListLocksResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListLocksMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service LockService.
   * <pre>
   * LockService (master-plan 9.2) — distributed locks for applications. Backed by
   * the portable `udb_advisory_leases` mutual-exclusion primitive, with a durable
   * tenant-scoped bookkeeping row and a monotone fencing token per grant so a
   * slow/partitioned holder can be safely fenced off.
   * </pre>
   */
  public static final class LockServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<LockServiceBlockingV2Stub> {
    private LockServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LockServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LockServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Acquire a distributed lock. Quota-aware: a tenant cannot exceed its active
     * lock budget. Returns the monotone fencing token the holder must present on
     * Renew/Release.
     * </pre>
     */
    public com.udb.core.lock.services.v1.AcquireLockResponse acquireLock(com.udb.core.lock.services.v1.AcquireLockRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getAcquireLockMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Renew (extend the lease of) a lock the caller currently holds. The presented
     * fencing token must not be stale; a lower token is rejected.
     * </pre>
     */
    public com.udb.core.lock.services.v1.RenewLockResponse renewLock(com.udb.core.lock.services.v1.RenewLockRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRenewLockMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Release a lock the caller currently holds. The presented fencing token must
     * not be stale; a lower token is rejected.
     * </pre>
     */
    public com.udb.core.lock.services.v1.ReleaseLockResponse releaseLock(com.udb.core.lock.services.v1.ReleaseLockRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getReleaseLockMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch a single lock by name within the caller's tenant. Read-only; an absent
     * lock returns found=false (not an error) — a tenant-scoped read miss is normal.
     * </pre>
     */
    public com.udb.core.lock.services.v1.GetLockResponse getLock(com.udb.core.lock.services.v1.GetLockRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetLockMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List the caller tenant's locks, optionally narrowed by status. Paginated
     * (page_size + opaque page_token). Read-only.
     * </pre>
     */
    public com.udb.core.lock.services.v1.ListLocksResponse listLocks(com.udb.core.lock.services.v1.ListLocksRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListLocksMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service LockService.
   * <pre>
   * LockService (master-plan 9.2) — distributed locks for applications. Backed by
   * the portable `udb_advisory_leases` mutual-exclusion primitive, with a durable
   * tenant-scoped bookkeeping row and a monotone fencing token per grant so a
   * slow/partitioned holder can be safely fenced off.
   * </pre>
   */
  public static final class LockServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<LockServiceBlockingStub> {
    private LockServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LockServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LockServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Acquire a distributed lock. Quota-aware: a tenant cannot exceed its active
     * lock budget. Returns the monotone fencing token the holder must present on
     * Renew/Release.
     * </pre>
     */
    public com.udb.core.lock.services.v1.AcquireLockResponse acquireLock(com.udb.core.lock.services.v1.AcquireLockRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAcquireLockMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Renew (extend the lease of) a lock the caller currently holds. The presented
     * fencing token must not be stale; a lower token is rejected.
     * </pre>
     */
    public com.udb.core.lock.services.v1.RenewLockResponse renewLock(com.udb.core.lock.services.v1.RenewLockRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRenewLockMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Release a lock the caller currently holds. The presented fencing token must
     * not be stale; a lower token is rejected.
     * </pre>
     */
    public com.udb.core.lock.services.v1.ReleaseLockResponse releaseLock(com.udb.core.lock.services.v1.ReleaseLockRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReleaseLockMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch a single lock by name within the caller's tenant. Read-only; an absent
     * lock returns found=false (not an error) — a tenant-scoped read miss is normal.
     * </pre>
     */
    public com.udb.core.lock.services.v1.GetLockResponse getLock(com.udb.core.lock.services.v1.GetLockRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetLockMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List the caller tenant's locks, optionally narrowed by status. Paginated
     * (page_size + opaque page_token). Read-only.
     * </pre>
     */
    public com.udb.core.lock.services.v1.ListLocksResponse listLocks(com.udb.core.lock.services.v1.ListLocksRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListLocksMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service LockService.
   * <pre>
   * LockService (master-plan 9.2) — distributed locks for applications. Backed by
   * the portable `udb_advisory_leases` mutual-exclusion primitive, with a durable
   * tenant-scoped bookkeeping row and a monotone fencing token per grant so a
   * slow/partitioned holder can be safely fenced off.
   * </pre>
   */
  public static final class LockServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<LockServiceFutureStub> {
    private LockServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LockServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LockServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Acquire a distributed lock. Quota-aware: a tenant cannot exceed its active
     * lock budget. Returns the monotone fencing token the holder must present on
     * Renew/Release.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.lock.services.v1.AcquireLockResponse> acquireLock(
        com.udb.core.lock.services.v1.AcquireLockRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAcquireLockMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Renew (extend the lease of) a lock the caller currently holds. The presented
     * fencing token must not be stale; a lower token is rejected.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.lock.services.v1.RenewLockResponse> renewLock(
        com.udb.core.lock.services.v1.RenewLockRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRenewLockMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Release a lock the caller currently holds. The presented fencing token must
     * not be stale; a lower token is rejected.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.lock.services.v1.ReleaseLockResponse> releaseLock(
        com.udb.core.lock.services.v1.ReleaseLockRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReleaseLockMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Fetch a single lock by name within the caller's tenant. Read-only; an absent
     * lock returns found=false (not an error) — a tenant-scoped read miss is normal.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.lock.services.v1.GetLockResponse> getLock(
        com.udb.core.lock.services.v1.GetLockRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetLockMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List the caller tenant's locks, optionally narrowed by status. Paginated
     * (page_size + opaque page_token). Read-only.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.lock.services.v1.ListLocksResponse> listLocks(
        com.udb.core.lock.services.v1.ListLocksRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListLocksMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_ACQUIRE_LOCK = 0;
  private static final int METHODID_RENEW_LOCK = 1;
  private static final int METHODID_RELEASE_LOCK = 2;
  private static final int METHODID_GET_LOCK = 3;
  private static final int METHODID_LIST_LOCKS = 4;

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
        case METHODID_ACQUIRE_LOCK:
          serviceImpl.acquireLock((com.udb.core.lock.services.v1.AcquireLockRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.lock.services.v1.AcquireLockResponse>) responseObserver);
          break;
        case METHODID_RENEW_LOCK:
          serviceImpl.renewLock((com.udb.core.lock.services.v1.RenewLockRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.lock.services.v1.RenewLockResponse>) responseObserver);
          break;
        case METHODID_RELEASE_LOCK:
          serviceImpl.releaseLock((com.udb.core.lock.services.v1.ReleaseLockRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.lock.services.v1.ReleaseLockResponse>) responseObserver);
          break;
        case METHODID_GET_LOCK:
          serviceImpl.getLock((com.udb.core.lock.services.v1.GetLockRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.lock.services.v1.GetLockResponse>) responseObserver);
          break;
        case METHODID_LIST_LOCKS:
          serviceImpl.listLocks((com.udb.core.lock.services.v1.ListLocksRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.lock.services.v1.ListLocksResponse>) responseObserver);
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
          getAcquireLockMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.lock.services.v1.AcquireLockRequest,
              com.udb.core.lock.services.v1.AcquireLockResponse>(
                service, METHODID_ACQUIRE_LOCK)))
        .addMethod(
          getRenewLockMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.lock.services.v1.RenewLockRequest,
              com.udb.core.lock.services.v1.RenewLockResponse>(
                service, METHODID_RENEW_LOCK)))
        .addMethod(
          getReleaseLockMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.lock.services.v1.ReleaseLockRequest,
              com.udb.core.lock.services.v1.ReleaseLockResponse>(
                service, METHODID_RELEASE_LOCK)))
        .addMethod(
          getGetLockMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.lock.services.v1.GetLockRequest,
              com.udb.core.lock.services.v1.GetLockResponse>(
                service, METHODID_GET_LOCK)))
        .addMethod(
          getListLocksMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.lock.services.v1.ListLocksRequest,
              com.udb.core.lock.services.v1.ListLocksResponse>(
                service, METHODID_LIST_LOCKS)))
        .build();
  }

  private static abstract class LockServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    LockServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.lock.services.v1.LockServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("LockService");
    }
  }

  private static final class LockServiceFileDescriptorSupplier
      extends LockServiceBaseDescriptorSupplier {
    LockServiceFileDescriptorSupplier() {}
  }

  private static final class LockServiceMethodDescriptorSupplier
      extends LockServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    LockServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (LockServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new LockServiceFileDescriptorSupplier())
              .addMethod(getAcquireLockMethod())
              .addMethod(getRenewLockMethod())
              .addMethod(getReleaseLockMethod())
              .addMethod(getGetLockMethod())
              .addMethod(getListLocksMethod())
              .build();
        }
      }
    }
    return result;
  }
}
