package com.udb.core.backup.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * BackupService (master-plan 9.10) — tenant-level logical backup and restore.
 * A backup enumerates the tenant's owned tables via the SAME shared resolver the
 * purge ripple uses, streams each table's tenant rows as JSONL, encrypts them at
 * rest, and writes them plus a checksummed manifest to object storage. Tables
 * without a resolvable tenant column are REPORTED as excluded, never silently
 * skipped. A restore validates the cross-tenant movement scope, refuses to write
 * over a live (non-empty) target tenant, and rewrites the tenant column to the
 * target on insert.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class BackupServiceGrpc {

  private BackupServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.backup.services.v1.BackupService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.StartTenantBackupRequest,
      com.udb.core.backup.services.v1.StartTenantBackupResponse> getStartTenantBackupMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StartTenantBackup",
      requestType = com.udb.core.backup.services.v1.StartTenantBackupRequest.class,
      responseType = com.udb.core.backup.services.v1.StartTenantBackupResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.StartTenantBackupRequest,
      com.udb.core.backup.services.v1.StartTenantBackupResponse> getStartTenantBackupMethod() {
    io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.StartTenantBackupRequest, com.udb.core.backup.services.v1.StartTenantBackupResponse> getStartTenantBackupMethod;
    if ((getStartTenantBackupMethod = BackupServiceGrpc.getStartTenantBackupMethod) == null) {
      synchronized (BackupServiceGrpc.class) {
        if ((getStartTenantBackupMethod = BackupServiceGrpc.getStartTenantBackupMethod) == null) {
          BackupServiceGrpc.getStartTenantBackupMethod = getStartTenantBackupMethod =
              io.grpc.MethodDescriptor.<com.udb.core.backup.services.v1.StartTenantBackupRequest, com.udb.core.backup.services.v1.StartTenantBackupResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StartTenantBackup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.backup.services.v1.StartTenantBackupRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.backup.services.v1.StartTenantBackupResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BackupServiceMethodDescriptorSupplier("StartTenantBackup"))
              .build();
        }
      }
    }
    return getStartTenantBackupMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.RestoreTenantRequest,
      com.udb.core.backup.services.v1.RestoreTenantResponse> getRestoreTenantMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RestoreTenant",
      requestType = com.udb.core.backup.services.v1.RestoreTenantRequest.class,
      responseType = com.udb.core.backup.services.v1.RestoreTenantResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.RestoreTenantRequest,
      com.udb.core.backup.services.v1.RestoreTenantResponse> getRestoreTenantMethod() {
    io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.RestoreTenantRequest, com.udb.core.backup.services.v1.RestoreTenantResponse> getRestoreTenantMethod;
    if ((getRestoreTenantMethod = BackupServiceGrpc.getRestoreTenantMethod) == null) {
      synchronized (BackupServiceGrpc.class) {
        if ((getRestoreTenantMethod = BackupServiceGrpc.getRestoreTenantMethod) == null) {
          BackupServiceGrpc.getRestoreTenantMethod = getRestoreTenantMethod =
              io.grpc.MethodDescriptor.<com.udb.core.backup.services.v1.RestoreTenantRequest, com.udb.core.backup.services.v1.RestoreTenantResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RestoreTenant"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.backup.services.v1.RestoreTenantRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.backup.services.v1.RestoreTenantResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BackupServiceMethodDescriptorSupplier("RestoreTenant"))
              .build();
        }
      }
    }
    return getRestoreTenantMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.ListBackupsRequest,
      com.udb.core.backup.services.v1.ListBackupsResponse> getListBackupsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListBackups",
      requestType = com.udb.core.backup.services.v1.ListBackupsRequest.class,
      responseType = com.udb.core.backup.services.v1.ListBackupsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.ListBackupsRequest,
      com.udb.core.backup.services.v1.ListBackupsResponse> getListBackupsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.ListBackupsRequest, com.udb.core.backup.services.v1.ListBackupsResponse> getListBackupsMethod;
    if ((getListBackupsMethod = BackupServiceGrpc.getListBackupsMethod) == null) {
      synchronized (BackupServiceGrpc.class) {
        if ((getListBackupsMethod = BackupServiceGrpc.getListBackupsMethod) == null) {
          BackupServiceGrpc.getListBackupsMethod = getListBackupsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.backup.services.v1.ListBackupsRequest, com.udb.core.backup.services.v1.ListBackupsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListBackups"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.backup.services.v1.ListBackupsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.backup.services.v1.ListBackupsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BackupServiceMethodDescriptorSupplier("ListBackups"))
              .build();
        }
      }
    }
    return getListBackupsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.GetBackupRequest,
      com.udb.core.backup.services.v1.GetBackupResponse> getGetBackupMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetBackup",
      requestType = com.udb.core.backup.services.v1.GetBackupRequest.class,
      responseType = com.udb.core.backup.services.v1.GetBackupResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.GetBackupRequest,
      com.udb.core.backup.services.v1.GetBackupResponse> getGetBackupMethod() {
    io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.GetBackupRequest, com.udb.core.backup.services.v1.GetBackupResponse> getGetBackupMethod;
    if ((getGetBackupMethod = BackupServiceGrpc.getGetBackupMethod) == null) {
      synchronized (BackupServiceGrpc.class) {
        if ((getGetBackupMethod = BackupServiceGrpc.getGetBackupMethod) == null) {
          BackupServiceGrpc.getGetBackupMethod = getGetBackupMethod =
              io.grpc.MethodDescriptor.<com.udb.core.backup.services.v1.GetBackupRequest, com.udb.core.backup.services.v1.GetBackupResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetBackup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.backup.services.v1.GetBackupRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.backup.services.v1.GetBackupResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BackupServiceMethodDescriptorSupplier("GetBackup"))
              .build();
        }
      }
    }
    return getGetBackupMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.PutBackupPolicyRequest,
      com.udb.core.backup.services.v1.PutBackupPolicyResponse> getPutBackupPolicyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PutBackupPolicy",
      requestType = com.udb.core.backup.services.v1.PutBackupPolicyRequest.class,
      responseType = com.udb.core.backup.services.v1.PutBackupPolicyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.PutBackupPolicyRequest,
      com.udb.core.backup.services.v1.PutBackupPolicyResponse> getPutBackupPolicyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.PutBackupPolicyRequest, com.udb.core.backup.services.v1.PutBackupPolicyResponse> getPutBackupPolicyMethod;
    if ((getPutBackupPolicyMethod = BackupServiceGrpc.getPutBackupPolicyMethod) == null) {
      synchronized (BackupServiceGrpc.class) {
        if ((getPutBackupPolicyMethod = BackupServiceGrpc.getPutBackupPolicyMethod) == null) {
          BackupServiceGrpc.getPutBackupPolicyMethod = getPutBackupPolicyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.backup.services.v1.PutBackupPolicyRequest, com.udb.core.backup.services.v1.PutBackupPolicyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PutBackupPolicy"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.backup.services.v1.PutBackupPolicyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.backup.services.v1.PutBackupPolicyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BackupServiceMethodDescriptorSupplier("PutBackupPolicy"))
              .build();
        }
      }
    }
    return getPutBackupPolicyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.GetBackupPolicyRequest,
      com.udb.core.backup.services.v1.GetBackupPolicyResponse> getGetBackupPolicyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetBackupPolicy",
      requestType = com.udb.core.backup.services.v1.GetBackupPolicyRequest.class,
      responseType = com.udb.core.backup.services.v1.GetBackupPolicyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.GetBackupPolicyRequest,
      com.udb.core.backup.services.v1.GetBackupPolicyResponse> getGetBackupPolicyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.GetBackupPolicyRequest, com.udb.core.backup.services.v1.GetBackupPolicyResponse> getGetBackupPolicyMethod;
    if ((getGetBackupPolicyMethod = BackupServiceGrpc.getGetBackupPolicyMethod) == null) {
      synchronized (BackupServiceGrpc.class) {
        if ((getGetBackupPolicyMethod = BackupServiceGrpc.getGetBackupPolicyMethod) == null) {
          BackupServiceGrpc.getGetBackupPolicyMethod = getGetBackupPolicyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.backup.services.v1.GetBackupPolicyRequest, com.udb.core.backup.services.v1.GetBackupPolicyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetBackupPolicy"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.backup.services.v1.GetBackupPolicyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.backup.services.v1.GetBackupPolicyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BackupServiceMethodDescriptorSupplier("GetBackupPolicy"))
              .build();
        }
      }
    }
    return getGetBackupPolicyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.ListBackupPoliciesRequest,
      com.udb.core.backup.services.v1.ListBackupPoliciesResponse> getListBackupPoliciesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListBackupPolicies",
      requestType = com.udb.core.backup.services.v1.ListBackupPoliciesRequest.class,
      responseType = com.udb.core.backup.services.v1.ListBackupPoliciesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.ListBackupPoliciesRequest,
      com.udb.core.backup.services.v1.ListBackupPoliciesResponse> getListBackupPoliciesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.ListBackupPoliciesRequest, com.udb.core.backup.services.v1.ListBackupPoliciesResponse> getListBackupPoliciesMethod;
    if ((getListBackupPoliciesMethod = BackupServiceGrpc.getListBackupPoliciesMethod) == null) {
      synchronized (BackupServiceGrpc.class) {
        if ((getListBackupPoliciesMethod = BackupServiceGrpc.getListBackupPoliciesMethod) == null) {
          BackupServiceGrpc.getListBackupPoliciesMethod = getListBackupPoliciesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.backup.services.v1.ListBackupPoliciesRequest, com.udb.core.backup.services.v1.ListBackupPoliciesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListBackupPolicies"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.backup.services.v1.ListBackupPoliciesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.backup.services.v1.ListBackupPoliciesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BackupServiceMethodDescriptorSupplier("ListBackupPolicies"))
              .build();
        }
      }
    }
    return getListBackupPoliciesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.DeleteBackupPolicyRequest,
      com.udb.core.backup.services.v1.DeleteBackupPolicyResponse> getDeleteBackupPolicyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteBackupPolicy",
      requestType = com.udb.core.backup.services.v1.DeleteBackupPolicyRequest.class,
      responseType = com.udb.core.backup.services.v1.DeleteBackupPolicyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.DeleteBackupPolicyRequest,
      com.udb.core.backup.services.v1.DeleteBackupPolicyResponse> getDeleteBackupPolicyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.backup.services.v1.DeleteBackupPolicyRequest, com.udb.core.backup.services.v1.DeleteBackupPolicyResponse> getDeleteBackupPolicyMethod;
    if ((getDeleteBackupPolicyMethod = BackupServiceGrpc.getDeleteBackupPolicyMethod) == null) {
      synchronized (BackupServiceGrpc.class) {
        if ((getDeleteBackupPolicyMethod = BackupServiceGrpc.getDeleteBackupPolicyMethod) == null) {
          BackupServiceGrpc.getDeleteBackupPolicyMethod = getDeleteBackupPolicyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.backup.services.v1.DeleteBackupPolicyRequest, com.udb.core.backup.services.v1.DeleteBackupPolicyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteBackupPolicy"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.backup.services.v1.DeleteBackupPolicyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.backup.services.v1.DeleteBackupPolicyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BackupServiceMethodDescriptorSupplier("DeleteBackupPolicy"))
              .build();
        }
      }
    }
    return getDeleteBackupPolicyMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BackupServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BackupServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BackupServiceStub>() {
        @java.lang.Override
        public BackupServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BackupServiceStub(channel, callOptions);
        }
      };
    return BackupServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static BackupServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BackupServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BackupServiceBlockingV2Stub>() {
        @java.lang.Override
        public BackupServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BackupServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return BackupServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BackupServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BackupServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BackupServiceBlockingStub>() {
        @java.lang.Override
        public BackupServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BackupServiceBlockingStub(channel, callOptions);
        }
      };
    return BackupServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BackupServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BackupServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BackupServiceFutureStub>() {
        @java.lang.Override
        public BackupServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BackupServiceFutureStub(channel, callOptions);
        }
      };
    return BackupServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * BackupService (master-plan 9.10) — tenant-level logical backup and restore.
   * A backup enumerates the tenant's owned tables via the SAME shared resolver the
   * purge ripple uses, streams each table's tenant rows as JSONL, encrypts them at
   * rest, and writes them plus a checksummed manifest to object storage. Tables
   * without a resolvable tenant column are REPORTED as excluded, never silently
   * skipped. A restore validates the cross-tenant movement scope, refuses to write
   * over a live (non-empty) target tenant, and rewrites the tenant column to the
   * target on insert.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Start a logical backup of the calling tenant. Enumerates tenant-owned tables
     * via the shared resolver, encrypts each table's rows to object storage, and
     * journals the run. Tenant-less tables are reported as excluded.
     * </pre>
     */
    default void startTenantBackup(com.udb.core.backup.services.v1.StartTenantBackupRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.StartTenantBackupResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStartTenantBackupMethod(), responseObserver);
    }

    /**
     * <pre>
     * Restore a tenant's backup into a FRESH target tenant. DESTRUCTIVE: requires
     * an explicit confirmation token, the cross-tenant movement scope check, and a
     * target tenant that holds no rows (restoring over a live tenant is refused).
     * </pre>
     */
    default void restoreTenant(com.udb.core.backup.services.v1.RestoreTenantRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.RestoreTenantResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRestoreTenantMethod(), responseObserver);
    }

    /**
     * <pre>
     * List the calling tenant's backup/restore journal runs (most recent first).
     * </pre>
     */
    default void listBackups(com.udb.core.backup.services.v1.ListBackupsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.ListBackupsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListBackupsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Fetch one backup run plus its per-table manifest detail.
     * </pre>
     */
    default void getBackup(com.udb.core.backup.services.v1.GetBackupRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.GetBackupResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetBackupMethod(), responseObserver);
    }

    /**
     * <pre>
     * Create or update the calling tenant's backup retention/schedule policy.
     * </pre>
     */
    default void putBackupPolicy(com.udb.core.backup.services.v1.PutBackupPolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.PutBackupPolicyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPutBackupPolicyMethod(), responseObserver);
    }

    /**
     * <pre>
     * Fetch a tenant's backup retention/schedule policy by name.
     * </pre>
     */
    default void getBackupPolicy(com.udb.core.backup.services.v1.GetBackupPolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.GetBackupPolicyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetBackupPolicyMethod(), responseObserver);
    }

    /**
     * <pre>
     * List the calling tenant's backup retention policies.
     * </pre>
     */
    default void listBackupPolicies(com.udb.core.backup.services.v1.ListBackupPoliciesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.ListBackupPoliciesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListBackupPoliciesMethod(), responseObserver);
    }

    /**
     * <pre>
     * Delete a tenant's backup retention policy by name.
     * </pre>
     */
    default void deleteBackupPolicy(com.udb.core.backup.services.v1.DeleteBackupPolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.DeleteBackupPolicyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteBackupPolicyMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service BackupService.
   * <pre>
   * BackupService (master-plan 9.10) — tenant-level logical backup and restore.
   * A backup enumerates the tenant's owned tables via the SAME shared resolver the
   * purge ripple uses, streams each table's tenant rows as JSONL, encrypts them at
   * rest, and writes them plus a checksummed manifest to object storage. Tables
   * without a resolvable tenant column are REPORTED as excluded, never silently
   * skipped. A restore validates the cross-tenant movement scope, refuses to write
   * over a live (non-empty) target tenant, and rewrites the tenant column to the
   * target on insert.
   * </pre>
   */
  public static abstract class BackupServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return BackupServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service BackupService.
   * <pre>
   * BackupService (master-plan 9.10) — tenant-level logical backup and restore.
   * A backup enumerates the tenant's owned tables via the SAME shared resolver the
   * purge ripple uses, streams each table's tenant rows as JSONL, encrypts them at
   * rest, and writes them plus a checksummed manifest to object storage. Tables
   * without a resolvable tenant column are REPORTED as excluded, never silently
   * skipped. A restore validates the cross-tenant movement scope, refuses to write
   * over a live (non-empty) target tenant, and rewrites the tenant column to the
   * target on insert.
   * </pre>
   */
  public static final class BackupServiceStub
      extends io.grpc.stub.AbstractAsyncStub<BackupServiceStub> {
    private BackupServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BackupServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BackupServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Start a logical backup of the calling tenant. Enumerates tenant-owned tables
     * via the shared resolver, encrypts each table's rows to object storage, and
     * journals the run. Tenant-less tables are reported as excluded.
     * </pre>
     */
    public void startTenantBackup(com.udb.core.backup.services.v1.StartTenantBackupRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.StartTenantBackupResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStartTenantBackupMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Restore a tenant's backup into a FRESH target tenant. DESTRUCTIVE: requires
     * an explicit confirmation token, the cross-tenant movement scope check, and a
     * target tenant that holds no rows (restoring over a live tenant is refused).
     * </pre>
     */
    public void restoreTenant(com.udb.core.backup.services.v1.RestoreTenantRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.RestoreTenantResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRestoreTenantMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List the calling tenant's backup/restore journal runs (most recent first).
     * </pre>
     */
    public void listBackups(com.udb.core.backup.services.v1.ListBackupsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.ListBackupsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListBackupsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Fetch one backup run plus its per-table manifest detail.
     * </pre>
     */
    public void getBackup(com.udb.core.backup.services.v1.GetBackupRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.GetBackupResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetBackupMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Create or update the calling tenant's backup retention/schedule policy.
     * </pre>
     */
    public void putBackupPolicy(com.udb.core.backup.services.v1.PutBackupPolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.PutBackupPolicyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPutBackupPolicyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Fetch a tenant's backup retention/schedule policy by name.
     * </pre>
     */
    public void getBackupPolicy(com.udb.core.backup.services.v1.GetBackupPolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.GetBackupPolicyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetBackupPolicyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List the calling tenant's backup retention policies.
     * </pre>
     */
    public void listBackupPolicies(com.udb.core.backup.services.v1.ListBackupPoliciesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.ListBackupPoliciesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListBackupPoliciesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Delete a tenant's backup retention policy by name.
     * </pre>
     */
    public void deleteBackupPolicy(com.udb.core.backup.services.v1.DeleteBackupPolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.DeleteBackupPolicyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteBackupPolicyMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service BackupService.
   * <pre>
   * BackupService (master-plan 9.10) — tenant-level logical backup and restore.
   * A backup enumerates the tenant's owned tables via the SAME shared resolver the
   * purge ripple uses, streams each table's tenant rows as JSONL, encrypts them at
   * rest, and writes them plus a checksummed manifest to object storage. Tables
   * without a resolvable tenant column are REPORTED as excluded, never silently
   * skipped. A restore validates the cross-tenant movement scope, refuses to write
   * over a live (non-empty) target tenant, and rewrites the tenant column to the
   * target on insert.
   * </pre>
   */
  public static final class BackupServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<BackupServiceBlockingV2Stub> {
    private BackupServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BackupServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BackupServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Start a logical backup of the calling tenant. Enumerates tenant-owned tables
     * via the shared resolver, encrypts each table's rows to object storage, and
     * journals the run. Tenant-less tables are reported as excluded.
     * </pre>
     */
    public com.udb.core.backup.services.v1.StartTenantBackupResponse startTenantBackup(com.udb.core.backup.services.v1.StartTenantBackupRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getStartTenantBackupMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Restore a tenant's backup into a FRESH target tenant. DESTRUCTIVE: requires
     * an explicit confirmation token, the cross-tenant movement scope check, and a
     * target tenant that holds no rows (restoring over a live tenant is refused).
     * </pre>
     */
    public com.udb.core.backup.services.v1.RestoreTenantResponse restoreTenant(com.udb.core.backup.services.v1.RestoreTenantRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRestoreTenantMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List the calling tenant's backup/restore journal runs (most recent first).
     * </pre>
     */
    public com.udb.core.backup.services.v1.ListBackupsResponse listBackups(com.udb.core.backup.services.v1.ListBackupsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListBackupsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch one backup run plus its per-table manifest detail.
     * </pre>
     */
    public com.udb.core.backup.services.v1.GetBackupResponse getBackup(com.udb.core.backup.services.v1.GetBackupRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetBackupMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Create or update the calling tenant's backup retention/schedule policy.
     * </pre>
     */
    public com.udb.core.backup.services.v1.PutBackupPolicyResponse putBackupPolicy(com.udb.core.backup.services.v1.PutBackupPolicyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPutBackupPolicyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch a tenant's backup retention/schedule policy by name.
     * </pre>
     */
    public com.udb.core.backup.services.v1.GetBackupPolicyResponse getBackupPolicy(com.udb.core.backup.services.v1.GetBackupPolicyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetBackupPolicyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List the calling tenant's backup retention policies.
     * </pre>
     */
    public com.udb.core.backup.services.v1.ListBackupPoliciesResponse listBackupPolicies(com.udb.core.backup.services.v1.ListBackupPoliciesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListBackupPoliciesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a tenant's backup retention policy by name.
     * </pre>
     */
    public com.udb.core.backup.services.v1.DeleteBackupPolicyResponse deleteBackupPolicy(com.udb.core.backup.services.v1.DeleteBackupPolicyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeleteBackupPolicyMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service BackupService.
   * <pre>
   * BackupService (master-plan 9.10) — tenant-level logical backup and restore.
   * A backup enumerates the tenant's owned tables via the SAME shared resolver the
   * purge ripple uses, streams each table's tenant rows as JSONL, encrypts them at
   * rest, and writes them plus a checksummed manifest to object storage. Tables
   * without a resolvable tenant column are REPORTED as excluded, never silently
   * skipped. A restore validates the cross-tenant movement scope, refuses to write
   * over a live (non-empty) target tenant, and rewrites the tenant column to the
   * target on insert.
   * </pre>
   */
  public static final class BackupServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<BackupServiceBlockingStub> {
    private BackupServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BackupServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BackupServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Start a logical backup of the calling tenant. Enumerates tenant-owned tables
     * via the shared resolver, encrypts each table's rows to object storage, and
     * journals the run. Tenant-less tables are reported as excluded.
     * </pre>
     */
    public com.udb.core.backup.services.v1.StartTenantBackupResponse startTenantBackup(com.udb.core.backup.services.v1.StartTenantBackupRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStartTenantBackupMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Restore a tenant's backup into a FRESH target tenant. DESTRUCTIVE: requires
     * an explicit confirmation token, the cross-tenant movement scope check, and a
     * target tenant that holds no rows (restoring over a live tenant is refused).
     * </pre>
     */
    public com.udb.core.backup.services.v1.RestoreTenantResponse restoreTenant(com.udb.core.backup.services.v1.RestoreTenantRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRestoreTenantMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List the calling tenant's backup/restore journal runs (most recent first).
     * </pre>
     */
    public com.udb.core.backup.services.v1.ListBackupsResponse listBackups(com.udb.core.backup.services.v1.ListBackupsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListBackupsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch one backup run plus its per-table manifest detail.
     * </pre>
     */
    public com.udb.core.backup.services.v1.GetBackupResponse getBackup(com.udb.core.backup.services.v1.GetBackupRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetBackupMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Create or update the calling tenant's backup retention/schedule policy.
     * </pre>
     */
    public com.udb.core.backup.services.v1.PutBackupPolicyResponse putBackupPolicy(com.udb.core.backup.services.v1.PutBackupPolicyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPutBackupPolicyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch a tenant's backup retention/schedule policy by name.
     * </pre>
     */
    public com.udb.core.backup.services.v1.GetBackupPolicyResponse getBackupPolicy(com.udb.core.backup.services.v1.GetBackupPolicyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetBackupPolicyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List the calling tenant's backup retention policies.
     * </pre>
     */
    public com.udb.core.backup.services.v1.ListBackupPoliciesResponse listBackupPolicies(com.udb.core.backup.services.v1.ListBackupPoliciesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListBackupPoliciesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a tenant's backup retention policy by name.
     * </pre>
     */
    public com.udb.core.backup.services.v1.DeleteBackupPolicyResponse deleteBackupPolicy(com.udb.core.backup.services.v1.DeleteBackupPolicyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteBackupPolicyMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service BackupService.
   * <pre>
   * BackupService (master-plan 9.10) — tenant-level logical backup and restore.
   * A backup enumerates the tenant's owned tables via the SAME shared resolver the
   * purge ripple uses, streams each table's tenant rows as JSONL, encrypts them at
   * rest, and writes them plus a checksummed manifest to object storage. Tables
   * without a resolvable tenant column are REPORTED as excluded, never silently
   * skipped. A restore validates the cross-tenant movement scope, refuses to write
   * over a live (non-empty) target tenant, and rewrites the tenant column to the
   * target on insert.
   * </pre>
   */
  public static final class BackupServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<BackupServiceFutureStub> {
    private BackupServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BackupServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BackupServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Start a logical backup of the calling tenant. Enumerates tenant-owned tables
     * via the shared resolver, encrypts each table's rows to object storage, and
     * journals the run. Tenant-less tables are reported as excluded.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.backup.services.v1.StartTenantBackupResponse> startTenantBackup(
        com.udb.core.backup.services.v1.StartTenantBackupRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStartTenantBackupMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Restore a tenant's backup into a FRESH target tenant. DESTRUCTIVE: requires
     * an explicit confirmation token, the cross-tenant movement scope check, and a
     * target tenant that holds no rows (restoring over a live tenant is refused).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.backup.services.v1.RestoreTenantResponse> restoreTenant(
        com.udb.core.backup.services.v1.RestoreTenantRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRestoreTenantMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List the calling tenant's backup/restore journal runs (most recent first).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.backup.services.v1.ListBackupsResponse> listBackups(
        com.udb.core.backup.services.v1.ListBackupsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListBackupsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Fetch one backup run plus its per-table manifest detail.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.backup.services.v1.GetBackupResponse> getBackup(
        com.udb.core.backup.services.v1.GetBackupRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetBackupMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Create or update the calling tenant's backup retention/schedule policy.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.backup.services.v1.PutBackupPolicyResponse> putBackupPolicy(
        com.udb.core.backup.services.v1.PutBackupPolicyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPutBackupPolicyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Fetch a tenant's backup retention/schedule policy by name.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.backup.services.v1.GetBackupPolicyResponse> getBackupPolicy(
        com.udb.core.backup.services.v1.GetBackupPolicyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetBackupPolicyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List the calling tenant's backup retention policies.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.backup.services.v1.ListBackupPoliciesResponse> listBackupPolicies(
        com.udb.core.backup.services.v1.ListBackupPoliciesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListBackupPoliciesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Delete a tenant's backup retention policy by name.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.backup.services.v1.DeleteBackupPolicyResponse> deleteBackupPolicy(
        com.udb.core.backup.services.v1.DeleteBackupPolicyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteBackupPolicyMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_START_TENANT_BACKUP = 0;
  private static final int METHODID_RESTORE_TENANT = 1;
  private static final int METHODID_LIST_BACKUPS = 2;
  private static final int METHODID_GET_BACKUP = 3;
  private static final int METHODID_PUT_BACKUP_POLICY = 4;
  private static final int METHODID_GET_BACKUP_POLICY = 5;
  private static final int METHODID_LIST_BACKUP_POLICIES = 6;
  private static final int METHODID_DELETE_BACKUP_POLICY = 7;

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
        case METHODID_START_TENANT_BACKUP:
          serviceImpl.startTenantBackup((com.udb.core.backup.services.v1.StartTenantBackupRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.StartTenantBackupResponse>) responseObserver);
          break;
        case METHODID_RESTORE_TENANT:
          serviceImpl.restoreTenant((com.udb.core.backup.services.v1.RestoreTenantRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.RestoreTenantResponse>) responseObserver);
          break;
        case METHODID_LIST_BACKUPS:
          serviceImpl.listBackups((com.udb.core.backup.services.v1.ListBackupsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.ListBackupsResponse>) responseObserver);
          break;
        case METHODID_GET_BACKUP:
          serviceImpl.getBackup((com.udb.core.backup.services.v1.GetBackupRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.GetBackupResponse>) responseObserver);
          break;
        case METHODID_PUT_BACKUP_POLICY:
          serviceImpl.putBackupPolicy((com.udb.core.backup.services.v1.PutBackupPolicyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.PutBackupPolicyResponse>) responseObserver);
          break;
        case METHODID_GET_BACKUP_POLICY:
          serviceImpl.getBackupPolicy((com.udb.core.backup.services.v1.GetBackupPolicyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.GetBackupPolicyResponse>) responseObserver);
          break;
        case METHODID_LIST_BACKUP_POLICIES:
          serviceImpl.listBackupPolicies((com.udb.core.backup.services.v1.ListBackupPoliciesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.ListBackupPoliciesResponse>) responseObserver);
          break;
        case METHODID_DELETE_BACKUP_POLICY:
          serviceImpl.deleteBackupPolicy((com.udb.core.backup.services.v1.DeleteBackupPolicyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.backup.services.v1.DeleteBackupPolicyResponse>) responseObserver);
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
          getStartTenantBackupMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.backup.services.v1.StartTenantBackupRequest,
              com.udb.core.backup.services.v1.StartTenantBackupResponse>(
                service, METHODID_START_TENANT_BACKUP)))
        .addMethod(
          getRestoreTenantMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.backup.services.v1.RestoreTenantRequest,
              com.udb.core.backup.services.v1.RestoreTenantResponse>(
                service, METHODID_RESTORE_TENANT)))
        .addMethod(
          getListBackupsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.backup.services.v1.ListBackupsRequest,
              com.udb.core.backup.services.v1.ListBackupsResponse>(
                service, METHODID_LIST_BACKUPS)))
        .addMethod(
          getGetBackupMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.backup.services.v1.GetBackupRequest,
              com.udb.core.backup.services.v1.GetBackupResponse>(
                service, METHODID_GET_BACKUP)))
        .addMethod(
          getPutBackupPolicyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.backup.services.v1.PutBackupPolicyRequest,
              com.udb.core.backup.services.v1.PutBackupPolicyResponse>(
                service, METHODID_PUT_BACKUP_POLICY)))
        .addMethod(
          getGetBackupPolicyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.backup.services.v1.GetBackupPolicyRequest,
              com.udb.core.backup.services.v1.GetBackupPolicyResponse>(
                service, METHODID_GET_BACKUP_POLICY)))
        .addMethod(
          getListBackupPoliciesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.backup.services.v1.ListBackupPoliciesRequest,
              com.udb.core.backup.services.v1.ListBackupPoliciesResponse>(
                service, METHODID_LIST_BACKUP_POLICIES)))
        .addMethod(
          getDeleteBackupPolicyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.backup.services.v1.DeleteBackupPolicyRequest,
              com.udb.core.backup.services.v1.DeleteBackupPolicyResponse>(
                service, METHODID_DELETE_BACKUP_POLICY)))
        .build();
  }

  private static abstract class BackupServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BackupServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.backup.services.v1.BackupServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BackupService");
    }
  }

  private static final class BackupServiceFileDescriptorSupplier
      extends BackupServiceBaseDescriptorSupplier {
    BackupServiceFileDescriptorSupplier() {}
  }

  private static final class BackupServiceMethodDescriptorSupplier
      extends BackupServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    BackupServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (BackupServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BackupServiceFileDescriptorSupplier())
              .addMethod(getStartTenantBackupMethod())
              .addMethod(getRestoreTenantMethod())
              .addMethod(getListBackupsMethod())
              .addMethod(getGetBackupMethod())
              .addMethod(getPutBackupPolicyMethod())
              .addMethod(getGetBackupPolicyMethod())
              .addMethod(getListBackupPoliciesMethod())
              .addMethod(getDeleteBackupPolicyMethod())
              .build();
        }
      }
    }
    return result;
  }
}
