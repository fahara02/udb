package com.udb.core.apikey.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * ---------------------------------------------------------------------------
 * ApiKeyService — Machine-to-machine key lifecycle and validation.
 * HTTP prefix: /v1/api_keys
 * URL conventions (Rule 07): snake_case paths, :&lt;verb&gt; custom method suffix, kebab-case query params.
 * The gateway calls ValidateApiKey on every inbound API request to:
 *   1. Verify key hash
 *   2. Check scope grants
 *   3. Enforce IP allowlist
 *   4. Enforce rate limits (increment usage counter)
 * ---------------------------------------------------------------------------
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class ApiKeyServiceGrpc {

  private ApiKeyServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.apikey.services.v1.ApiKeyService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.CreateApiKeyRequest,
      com.udb.core.apikey.services.v1.CreateApiKeyResponse> getCreateApiKeyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateApiKey",
      requestType = com.udb.core.apikey.services.v1.CreateApiKeyRequest.class,
      responseType = com.udb.core.apikey.services.v1.CreateApiKeyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.CreateApiKeyRequest,
      com.udb.core.apikey.services.v1.CreateApiKeyResponse> getCreateApiKeyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.CreateApiKeyRequest, com.udb.core.apikey.services.v1.CreateApiKeyResponse> getCreateApiKeyMethod;
    if ((getCreateApiKeyMethod = ApiKeyServiceGrpc.getCreateApiKeyMethod) == null) {
      synchronized (ApiKeyServiceGrpc.class) {
        if ((getCreateApiKeyMethod = ApiKeyServiceGrpc.getCreateApiKeyMethod) == null) {
          ApiKeyServiceGrpc.getCreateApiKeyMethod = getCreateApiKeyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.apikey.services.v1.CreateApiKeyRequest, com.udb.core.apikey.services.v1.CreateApiKeyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateApiKey"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.CreateApiKeyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.CreateApiKeyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ApiKeyServiceMethodDescriptorSupplier("CreateApiKey"))
              .build();
        }
      }
    }
    return getCreateApiKeyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.GetApiKeyRequest,
      com.udb.core.apikey.services.v1.GetApiKeyResponse> getGetApiKeyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetApiKey",
      requestType = com.udb.core.apikey.services.v1.GetApiKeyRequest.class,
      responseType = com.udb.core.apikey.services.v1.GetApiKeyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.GetApiKeyRequest,
      com.udb.core.apikey.services.v1.GetApiKeyResponse> getGetApiKeyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.GetApiKeyRequest, com.udb.core.apikey.services.v1.GetApiKeyResponse> getGetApiKeyMethod;
    if ((getGetApiKeyMethod = ApiKeyServiceGrpc.getGetApiKeyMethod) == null) {
      synchronized (ApiKeyServiceGrpc.class) {
        if ((getGetApiKeyMethod = ApiKeyServiceGrpc.getGetApiKeyMethod) == null) {
          ApiKeyServiceGrpc.getGetApiKeyMethod = getGetApiKeyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.apikey.services.v1.GetApiKeyRequest, com.udb.core.apikey.services.v1.GetApiKeyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetApiKey"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.GetApiKeyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.GetApiKeyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ApiKeyServiceMethodDescriptorSupplier("GetApiKey"))
              .build();
        }
      }
    }
    return getGetApiKeyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.ListApiKeysRequest,
      com.udb.core.apikey.services.v1.ListApiKeysResponse> getListApiKeysMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListApiKeys",
      requestType = com.udb.core.apikey.services.v1.ListApiKeysRequest.class,
      responseType = com.udb.core.apikey.services.v1.ListApiKeysResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.ListApiKeysRequest,
      com.udb.core.apikey.services.v1.ListApiKeysResponse> getListApiKeysMethod() {
    io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.ListApiKeysRequest, com.udb.core.apikey.services.v1.ListApiKeysResponse> getListApiKeysMethod;
    if ((getListApiKeysMethod = ApiKeyServiceGrpc.getListApiKeysMethod) == null) {
      synchronized (ApiKeyServiceGrpc.class) {
        if ((getListApiKeysMethod = ApiKeyServiceGrpc.getListApiKeysMethod) == null) {
          ApiKeyServiceGrpc.getListApiKeysMethod = getListApiKeysMethod =
              io.grpc.MethodDescriptor.<com.udb.core.apikey.services.v1.ListApiKeysRequest, com.udb.core.apikey.services.v1.ListApiKeysResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListApiKeys"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.ListApiKeysRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.ListApiKeysResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ApiKeyServiceMethodDescriptorSupplier("ListApiKeys"))
              .build();
        }
      }
    }
    return getListApiKeysMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.UpdateApiKeyRequest,
      com.udb.core.apikey.services.v1.UpdateApiKeyResponse> getUpdateApiKeyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateApiKey",
      requestType = com.udb.core.apikey.services.v1.UpdateApiKeyRequest.class,
      responseType = com.udb.core.apikey.services.v1.UpdateApiKeyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.UpdateApiKeyRequest,
      com.udb.core.apikey.services.v1.UpdateApiKeyResponse> getUpdateApiKeyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.UpdateApiKeyRequest, com.udb.core.apikey.services.v1.UpdateApiKeyResponse> getUpdateApiKeyMethod;
    if ((getUpdateApiKeyMethod = ApiKeyServiceGrpc.getUpdateApiKeyMethod) == null) {
      synchronized (ApiKeyServiceGrpc.class) {
        if ((getUpdateApiKeyMethod = ApiKeyServiceGrpc.getUpdateApiKeyMethod) == null) {
          ApiKeyServiceGrpc.getUpdateApiKeyMethod = getUpdateApiKeyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.apikey.services.v1.UpdateApiKeyRequest, com.udb.core.apikey.services.v1.UpdateApiKeyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateApiKey"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.UpdateApiKeyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.UpdateApiKeyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ApiKeyServiceMethodDescriptorSupplier("UpdateApiKey"))
              .build();
        }
      }
    }
    return getUpdateApiKeyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.RevokeApiKeyRequest,
      com.udb.core.apikey.services.v1.RevokeApiKeyResponse> getRevokeApiKeyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RevokeApiKey",
      requestType = com.udb.core.apikey.services.v1.RevokeApiKeyRequest.class,
      responseType = com.udb.core.apikey.services.v1.RevokeApiKeyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.RevokeApiKeyRequest,
      com.udb.core.apikey.services.v1.RevokeApiKeyResponse> getRevokeApiKeyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.RevokeApiKeyRequest, com.udb.core.apikey.services.v1.RevokeApiKeyResponse> getRevokeApiKeyMethod;
    if ((getRevokeApiKeyMethod = ApiKeyServiceGrpc.getRevokeApiKeyMethod) == null) {
      synchronized (ApiKeyServiceGrpc.class) {
        if ((getRevokeApiKeyMethod = ApiKeyServiceGrpc.getRevokeApiKeyMethod) == null) {
          ApiKeyServiceGrpc.getRevokeApiKeyMethod = getRevokeApiKeyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.apikey.services.v1.RevokeApiKeyRequest, com.udb.core.apikey.services.v1.RevokeApiKeyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RevokeApiKey"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.RevokeApiKeyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.RevokeApiKeyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ApiKeyServiceMethodDescriptorSupplier("RevokeApiKey"))
              .build();
        }
      }
    }
    return getRevokeApiKeyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.RotateApiKeyRequest,
      com.udb.core.apikey.services.v1.RotateApiKeyResponse> getRotateApiKeyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RotateApiKey",
      requestType = com.udb.core.apikey.services.v1.RotateApiKeyRequest.class,
      responseType = com.udb.core.apikey.services.v1.RotateApiKeyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.RotateApiKeyRequest,
      com.udb.core.apikey.services.v1.RotateApiKeyResponse> getRotateApiKeyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.RotateApiKeyRequest, com.udb.core.apikey.services.v1.RotateApiKeyResponse> getRotateApiKeyMethod;
    if ((getRotateApiKeyMethod = ApiKeyServiceGrpc.getRotateApiKeyMethod) == null) {
      synchronized (ApiKeyServiceGrpc.class) {
        if ((getRotateApiKeyMethod = ApiKeyServiceGrpc.getRotateApiKeyMethod) == null) {
          ApiKeyServiceGrpc.getRotateApiKeyMethod = getRotateApiKeyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.apikey.services.v1.RotateApiKeyRequest, com.udb.core.apikey.services.v1.RotateApiKeyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RotateApiKey"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.RotateApiKeyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.RotateApiKeyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ApiKeyServiceMethodDescriptorSupplier("RotateApiKey"))
              .build();
        }
      }
    }
    return getRotateApiKeyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysRequest,
      com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysResponse> getEmergencyRevokeApiKeysMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "EmergencyRevokeApiKeys",
      requestType = com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysRequest.class,
      responseType = com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysRequest,
      com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysResponse> getEmergencyRevokeApiKeysMethod() {
    io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysRequest, com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysResponse> getEmergencyRevokeApiKeysMethod;
    if ((getEmergencyRevokeApiKeysMethod = ApiKeyServiceGrpc.getEmergencyRevokeApiKeysMethod) == null) {
      synchronized (ApiKeyServiceGrpc.class) {
        if ((getEmergencyRevokeApiKeysMethod = ApiKeyServiceGrpc.getEmergencyRevokeApiKeysMethod) == null) {
          ApiKeyServiceGrpc.getEmergencyRevokeApiKeysMethod = getEmergencyRevokeApiKeysMethod =
              io.grpc.MethodDescriptor.<com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysRequest, com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "EmergencyRevokeApiKeys"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ApiKeyServiceMethodDescriptorSupplier("EmergencyRevokeApiKeys"))
              .build();
        }
      }
    }
    return getEmergencyRevokeApiKeysMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.ValidateApiKeyRequest,
      com.udb.core.apikey.services.v1.ValidateApiKeyResponse> getValidateApiKeyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ValidateApiKey",
      requestType = com.udb.core.apikey.services.v1.ValidateApiKeyRequest.class,
      responseType = com.udb.core.apikey.services.v1.ValidateApiKeyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.ValidateApiKeyRequest,
      com.udb.core.apikey.services.v1.ValidateApiKeyResponse> getValidateApiKeyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.ValidateApiKeyRequest, com.udb.core.apikey.services.v1.ValidateApiKeyResponse> getValidateApiKeyMethod;
    if ((getValidateApiKeyMethod = ApiKeyServiceGrpc.getValidateApiKeyMethod) == null) {
      synchronized (ApiKeyServiceGrpc.class) {
        if ((getValidateApiKeyMethod = ApiKeyServiceGrpc.getValidateApiKeyMethod) == null) {
          ApiKeyServiceGrpc.getValidateApiKeyMethod = getValidateApiKeyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.apikey.services.v1.ValidateApiKeyRequest, com.udb.core.apikey.services.v1.ValidateApiKeyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ValidateApiKey"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.ValidateApiKeyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.ValidateApiKeyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ApiKeyServiceMethodDescriptorSupplier("ValidateApiKey"))
              .build();
        }
      }
    }
    return getValidateApiKeyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.GetApiKeyUsageStatsRequest,
      com.udb.core.apikey.services.v1.GetApiKeyUsageStatsResponse> getGetApiKeyUsageStatsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetApiKeyUsageStats",
      requestType = com.udb.core.apikey.services.v1.GetApiKeyUsageStatsRequest.class,
      responseType = com.udb.core.apikey.services.v1.GetApiKeyUsageStatsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.GetApiKeyUsageStatsRequest,
      com.udb.core.apikey.services.v1.GetApiKeyUsageStatsResponse> getGetApiKeyUsageStatsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.apikey.services.v1.GetApiKeyUsageStatsRequest, com.udb.core.apikey.services.v1.GetApiKeyUsageStatsResponse> getGetApiKeyUsageStatsMethod;
    if ((getGetApiKeyUsageStatsMethod = ApiKeyServiceGrpc.getGetApiKeyUsageStatsMethod) == null) {
      synchronized (ApiKeyServiceGrpc.class) {
        if ((getGetApiKeyUsageStatsMethod = ApiKeyServiceGrpc.getGetApiKeyUsageStatsMethod) == null) {
          ApiKeyServiceGrpc.getGetApiKeyUsageStatsMethod = getGetApiKeyUsageStatsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.apikey.services.v1.GetApiKeyUsageStatsRequest, com.udb.core.apikey.services.v1.GetApiKeyUsageStatsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetApiKeyUsageStats"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.GetApiKeyUsageStatsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.apikey.services.v1.GetApiKeyUsageStatsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ApiKeyServiceMethodDescriptorSupplier("GetApiKeyUsageStats"))
              .build();
        }
      }
    }
    return getGetApiKeyUsageStatsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ApiKeyServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ApiKeyServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ApiKeyServiceStub>() {
        @java.lang.Override
        public ApiKeyServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ApiKeyServiceStub(channel, callOptions);
        }
      };
    return ApiKeyServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static ApiKeyServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ApiKeyServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ApiKeyServiceBlockingV2Stub>() {
        @java.lang.Override
        public ApiKeyServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ApiKeyServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return ApiKeyServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ApiKeyServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ApiKeyServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ApiKeyServiceBlockingStub>() {
        @java.lang.Override
        public ApiKeyServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ApiKeyServiceBlockingStub(channel, callOptions);
        }
      };
    return ApiKeyServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ApiKeyServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ApiKeyServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ApiKeyServiceFutureStub>() {
        @java.lang.Override
        public ApiKeyServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ApiKeyServiceFutureStub(channel, callOptions);
        }
      };
    return ApiKeyServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * ---------------------------------------------------------------------------
   * ApiKeyService — Machine-to-machine key lifecycle and validation.
   *
   * HTTP prefix: /v1/api_keys
   * URL conventions (Rule 07): snake_case paths, :&lt;verb&gt; custom method suffix, kebab-case query params.
   *
   * The gateway calls ValidateApiKey on every inbound API request to:
   *   1. Verify key hash
   *   2. Check scope grants
   *   3. Enforce IP allowlist
   *   4. Enforce rate limits (increment usage counter)
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * ── Key lifecycle (admin-only) ────────────────────────────────────────────
     * Returns the plain key ONCE in CreateApiKeyResponse — never again.
     * </pre>
     */
    default void createApiKey(com.udb.core.apikey.services.v1.CreateApiKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.CreateApiKeyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateApiKeyMethod(), responseObserver);
    }

    /**
     */
    default void getApiKey(com.udb.core.apikey.services.v1.GetApiKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.GetApiKeyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetApiKeyMethod(), responseObserver);
    }

    /**
     */
    default void listApiKeys(com.udb.core.apikey.services.v1.ListApiKeysRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.ListApiKeysResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListApiKeysMethod(), responseObserver);
    }

    /**
     */
    default void updateApiKey(com.udb.core.apikey.services.v1.UpdateApiKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.UpdateApiKeyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateApiKeyMethod(), responseObserver);
    }

    /**
     */
    default void revokeApiKey(com.udb.core.apikey.services.v1.RevokeApiKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.RevokeApiKeyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRevokeApiKeyMethod(), responseObserver);
    }

    /**
     * <pre>
     * Rotate a key's secret in place (same key_id + lineage). Returns the new
     * plain key ONCE; the old secret is invalidated immediately.
     * </pre>
     */
    default void rotateApiKey(com.udb.core.apikey.services.v1.RotateApiKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.RotateApiKeyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRotateApiKeyMethod(), responseObserver);
    }

    /**
     * <pre>
     * Emergency bulk revoke by selector (prefix/owner/tenant/project/scope/before).
     * </pre>
     */
    default void emergencyRevokeApiKeys(com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEmergencyRevokeApiKeysMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── Validation (called by API gateway — internal, not public HTTP) ────────
     * </pre>
     */
    default void validateApiKey(com.udb.core.apikey.services.v1.ValidateApiKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.ValidateApiKeyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getValidateApiKeyMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── Usage stats ───────────────────────────────────────────────────────────
     * </pre>
     */
    default void getApiKeyUsageStats(com.udb.core.apikey.services.v1.GetApiKeyUsageStatsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.GetApiKeyUsageStatsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetApiKeyUsageStatsMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service ApiKeyService.
   * <pre>
   * ---------------------------------------------------------------------------
   * ApiKeyService — Machine-to-machine key lifecycle and validation.
   *
   * HTTP prefix: /v1/api_keys
   * URL conventions (Rule 07): snake_case paths, :&lt;verb&gt; custom method suffix, kebab-case query params.
   *
   * The gateway calls ValidateApiKey on every inbound API request to:
   *   1. Verify key hash
   *   2. Check scope grants
   *   3. Enforce IP allowlist
   *   4. Enforce rate limits (increment usage counter)
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static abstract class ApiKeyServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return ApiKeyServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service ApiKeyService.
   * <pre>
   * ---------------------------------------------------------------------------
   * ApiKeyService — Machine-to-machine key lifecycle and validation.
   *
   * HTTP prefix: /v1/api_keys
   * URL conventions (Rule 07): snake_case paths, :&lt;verb&gt; custom method suffix, kebab-case query params.
   *
   * The gateway calls ValidateApiKey on every inbound API request to:
   *   1. Verify key hash
   *   2. Check scope grants
   *   3. Enforce IP allowlist
   *   4. Enforce rate limits (increment usage counter)
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static final class ApiKeyServiceStub
      extends io.grpc.stub.AbstractAsyncStub<ApiKeyServiceStub> {
    private ApiKeyServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ApiKeyServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ApiKeyServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * ── Key lifecycle (admin-only) ────────────────────────────────────────────
     * Returns the plain key ONCE in CreateApiKeyResponse — never again.
     * </pre>
     */
    public void createApiKey(com.udb.core.apikey.services.v1.CreateApiKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.CreateApiKeyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateApiKeyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getApiKey(com.udb.core.apikey.services.v1.GetApiKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.GetApiKeyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetApiKeyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listApiKeys(com.udb.core.apikey.services.v1.ListApiKeysRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.ListApiKeysResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListApiKeysMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void updateApiKey(com.udb.core.apikey.services.v1.UpdateApiKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.UpdateApiKeyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateApiKeyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void revokeApiKey(com.udb.core.apikey.services.v1.RevokeApiKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.RevokeApiKeyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRevokeApiKeyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Rotate a key's secret in place (same key_id + lineage). Returns the new
     * plain key ONCE; the old secret is invalidated immediately.
     * </pre>
     */
    public void rotateApiKey(com.udb.core.apikey.services.v1.RotateApiKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.RotateApiKeyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRotateApiKeyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Emergency bulk revoke by selector (prefix/owner/tenant/project/scope/before).
     * </pre>
     */
    public void emergencyRevokeApiKeys(com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEmergencyRevokeApiKeysMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── Validation (called by API gateway — internal, not public HTTP) ────────
     * </pre>
     */
    public void validateApiKey(com.udb.core.apikey.services.v1.ValidateApiKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.ValidateApiKeyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getValidateApiKeyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── Usage stats ───────────────────────────────────────────────────────────
     * </pre>
     */
    public void getApiKeyUsageStats(com.udb.core.apikey.services.v1.GetApiKeyUsageStatsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.GetApiKeyUsageStatsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetApiKeyUsageStatsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service ApiKeyService.
   * <pre>
   * ---------------------------------------------------------------------------
   * ApiKeyService — Machine-to-machine key lifecycle and validation.
   *
   * HTTP prefix: /v1/api_keys
   * URL conventions (Rule 07): snake_case paths, :&lt;verb&gt; custom method suffix, kebab-case query params.
   *
   * The gateway calls ValidateApiKey on every inbound API request to:
   *   1. Verify key hash
   *   2. Check scope grants
   *   3. Enforce IP allowlist
   *   4. Enforce rate limits (increment usage counter)
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static final class ApiKeyServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<ApiKeyServiceBlockingV2Stub> {
    private ApiKeyServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ApiKeyServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ApiKeyServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * ── Key lifecycle (admin-only) ────────────────────────────────────────────
     * Returns the plain key ONCE in CreateApiKeyResponse — never again.
     * </pre>
     */
    public com.udb.core.apikey.services.v1.CreateApiKeyResponse createApiKey(com.udb.core.apikey.services.v1.CreateApiKeyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreateApiKeyMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.apikey.services.v1.GetApiKeyResponse getApiKey(com.udb.core.apikey.services.v1.GetApiKeyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetApiKeyMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.apikey.services.v1.ListApiKeysResponse listApiKeys(com.udb.core.apikey.services.v1.ListApiKeysRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListApiKeysMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.apikey.services.v1.UpdateApiKeyResponse updateApiKey(com.udb.core.apikey.services.v1.UpdateApiKeyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getUpdateApiKeyMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.apikey.services.v1.RevokeApiKeyResponse revokeApiKey(com.udb.core.apikey.services.v1.RevokeApiKeyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRevokeApiKeyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Rotate a key's secret in place (same key_id + lineage). Returns the new
     * plain key ONCE; the old secret is invalidated immediately.
     * </pre>
     */
    public com.udb.core.apikey.services.v1.RotateApiKeyResponse rotateApiKey(com.udb.core.apikey.services.v1.RotateApiKeyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRotateApiKeyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Emergency bulk revoke by selector (prefix/owner/tenant/project/scope/before).
     * </pre>
     */
    public com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysResponse emergencyRevokeApiKeys(com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getEmergencyRevokeApiKeysMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Validation (called by API gateway — internal, not public HTTP) ────────
     * </pre>
     */
    public com.udb.core.apikey.services.v1.ValidateApiKeyResponse validateApiKey(com.udb.core.apikey.services.v1.ValidateApiKeyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getValidateApiKeyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Usage stats ───────────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.apikey.services.v1.GetApiKeyUsageStatsResponse getApiKeyUsageStats(com.udb.core.apikey.services.v1.GetApiKeyUsageStatsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetApiKeyUsageStatsMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service ApiKeyService.
   * <pre>
   * ---------------------------------------------------------------------------
   * ApiKeyService — Machine-to-machine key lifecycle and validation.
   *
   * HTTP prefix: /v1/api_keys
   * URL conventions (Rule 07): snake_case paths, :&lt;verb&gt; custom method suffix, kebab-case query params.
   *
   * The gateway calls ValidateApiKey on every inbound API request to:
   *   1. Verify key hash
   *   2. Check scope grants
   *   3. Enforce IP allowlist
   *   4. Enforce rate limits (increment usage counter)
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static final class ApiKeyServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<ApiKeyServiceBlockingStub> {
    private ApiKeyServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ApiKeyServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ApiKeyServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * ── Key lifecycle (admin-only) ────────────────────────────────────────────
     * Returns the plain key ONCE in CreateApiKeyResponse — never again.
     * </pre>
     */
    public com.udb.core.apikey.services.v1.CreateApiKeyResponse createApiKey(com.udb.core.apikey.services.v1.CreateApiKeyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateApiKeyMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.apikey.services.v1.GetApiKeyResponse getApiKey(com.udb.core.apikey.services.v1.GetApiKeyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetApiKeyMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.apikey.services.v1.ListApiKeysResponse listApiKeys(com.udb.core.apikey.services.v1.ListApiKeysRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListApiKeysMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.apikey.services.v1.UpdateApiKeyResponse updateApiKey(com.udb.core.apikey.services.v1.UpdateApiKeyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateApiKeyMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.apikey.services.v1.RevokeApiKeyResponse revokeApiKey(com.udb.core.apikey.services.v1.RevokeApiKeyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRevokeApiKeyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Rotate a key's secret in place (same key_id + lineage). Returns the new
     * plain key ONCE; the old secret is invalidated immediately.
     * </pre>
     */
    public com.udb.core.apikey.services.v1.RotateApiKeyResponse rotateApiKey(com.udb.core.apikey.services.v1.RotateApiKeyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRotateApiKeyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Emergency bulk revoke by selector (prefix/owner/tenant/project/scope/before).
     * </pre>
     */
    public com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysResponse emergencyRevokeApiKeys(com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEmergencyRevokeApiKeysMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Validation (called by API gateway — internal, not public HTTP) ────────
     * </pre>
     */
    public com.udb.core.apikey.services.v1.ValidateApiKeyResponse validateApiKey(com.udb.core.apikey.services.v1.ValidateApiKeyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getValidateApiKeyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Usage stats ───────────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.apikey.services.v1.GetApiKeyUsageStatsResponse getApiKeyUsageStats(com.udb.core.apikey.services.v1.GetApiKeyUsageStatsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetApiKeyUsageStatsMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service ApiKeyService.
   * <pre>
   * ---------------------------------------------------------------------------
   * ApiKeyService — Machine-to-machine key lifecycle and validation.
   *
   * HTTP prefix: /v1/api_keys
   * URL conventions (Rule 07): snake_case paths, :&lt;verb&gt; custom method suffix, kebab-case query params.
   *
   * The gateway calls ValidateApiKey on every inbound API request to:
   *   1. Verify key hash
   *   2. Check scope grants
   *   3. Enforce IP allowlist
   *   4. Enforce rate limits (increment usage counter)
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static final class ApiKeyServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<ApiKeyServiceFutureStub> {
    private ApiKeyServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ApiKeyServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ApiKeyServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * ── Key lifecycle (admin-only) ────────────────────────────────────────────
     * Returns the plain key ONCE in CreateApiKeyResponse — never again.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.apikey.services.v1.CreateApiKeyResponse> createApiKey(
        com.udb.core.apikey.services.v1.CreateApiKeyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateApiKeyMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.apikey.services.v1.GetApiKeyResponse> getApiKey(
        com.udb.core.apikey.services.v1.GetApiKeyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetApiKeyMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.apikey.services.v1.ListApiKeysResponse> listApiKeys(
        com.udb.core.apikey.services.v1.ListApiKeysRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListApiKeysMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.apikey.services.v1.UpdateApiKeyResponse> updateApiKey(
        com.udb.core.apikey.services.v1.UpdateApiKeyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateApiKeyMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.apikey.services.v1.RevokeApiKeyResponse> revokeApiKey(
        com.udb.core.apikey.services.v1.RevokeApiKeyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRevokeApiKeyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Rotate a key's secret in place (same key_id + lineage). Returns the new
     * plain key ONCE; the old secret is invalidated immediately.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.apikey.services.v1.RotateApiKeyResponse> rotateApiKey(
        com.udb.core.apikey.services.v1.RotateApiKeyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRotateApiKeyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Emergency bulk revoke by selector (prefix/owner/tenant/project/scope/before).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysResponse> emergencyRevokeApiKeys(
        com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEmergencyRevokeApiKeysMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── Validation (called by API gateway — internal, not public HTTP) ────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.apikey.services.v1.ValidateApiKeyResponse> validateApiKey(
        com.udb.core.apikey.services.v1.ValidateApiKeyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getValidateApiKeyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── Usage stats ───────────────────────────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.apikey.services.v1.GetApiKeyUsageStatsResponse> getApiKeyUsageStats(
        com.udb.core.apikey.services.v1.GetApiKeyUsageStatsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetApiKeyUsageStatsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_API_KEY = 0;
  private static final int METHODID_GET_API_KEY = 1;
  private static final int METHODID_LIST_API_KEYS = 2;
  private static final int METHODID_UPDATE_API_KEY = 3;
  private static final int METHODID_REVOKE_API_KEY = 4;
  private static final int METHODID_ROTATE_API_KEY = 5;
  private static final int METHODID_EMERGENCY_REVOKE_API_KEYS = 6;
  private static final int METHODID_VALIDATE_API_KEY = 7;
  private static final int METHODID_GET_API_KEY_USAGE_STATS = 8;

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
        case METHODID_CREATE_API_KEY:
          serviceImpl.createApiKey((com.udb.core.apikey.services.v1.CreateApiKeyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.CreateApiKeyResponse>) responseObserver);
          break;
        case METHODID_GET_API_KEY:
          serviceImpl.getApiKey((com.udb.core.apikey.services.v1.GetApiKeyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.GetApiKeyResponse>) responseObserver);
          break;
        case METHODID_LIST_API_KEYS:
          serviceImpl.listApiKeys((com.udb.core.apikey.services.v1.ListApiKeysRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.ListApiKeysResponse>) responseObserver);
          break;
        case METHODID_UPDATE_API_KEY:
          serviceImpl.updateApiKey((com.udb.core.apikey.services.v1.UpdateApiKeyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.UpdateApiKeyResponse>) responseObserver);
          break;
        case METHODID_REVOKE_API_KEY:
          serviceImpl.revokeApiKey((com.udb.core.apikey.services.v1.RevokeApiKeyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.RevokeApiKeyResponse>) responseObserver);
          break;
        case METHODID_ROTATE_API_KEY:
          serviceImpl.rotateApiKey((com.udb.core.apikey.services.v1.RotateApiKeyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.RotateApiKeyResponse>) responseObserver);
          break;
        case METHODID_EMERGENCY_REVOKE_API_KEYS:
          serviceImpl.emergencyRevokeApiKeys((com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysResponse>) responseObserver);
          break;
        case METHODID_VALIDATE_API_KEY:
          serviceImpl.validateApiKey((com.udb.core.apikey.services.v1.ValidateApiKeyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.ValidateApiKeyResponse>) responseObserver);
          break;
        case METHODID_GET_API_KEY_USAGE_STATS:
          serviceImpl.getApiKeyUsageStats((com.udb.core.apikey.services.v1.GetApiKeyUsageStatsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.apikey.services.v1.GetApiKeyUsageStatsResponse>) responseObserver);
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
          getCreateApiKeyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.apikey.services.v1.CreateApiKeyRequest,
              com.udb.core.apikey.services.v1.CreateApiKeyResponse>(
                service, METHODID_CREATE_API_KEY)))
        .addMethod(
          getGetApiKeyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.apikey.services.v1.GetApiKeyRequest,
              com.udb.core.apikey.services.v1.GetApiKeyResponse>(
                service, METHODID_GET_API_KEY)))
        .addMethod(
          getListApiKeysMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.apikey.services.v1.ListApiKeysRequest,
              com.udb.core.apikey.services.v1.ListApiKeysResponse>(
                service, METHODID_LIST_API_KEYS)))
        .addMethod(
          getUpdateApiKeyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.apikey.services.v1.UpdateApiKeyRequest,
              com.udb.core.apikey.services.v1.UpdateApiKeyResponse>(
                service, METHODID_UPDATE_API_KEY)))
        .addMethod(
          getRevokeApiKeyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.apikey.services.v1.RevokeApiKeyRequest,
              com.udb.core.apikey.services.v1.RevokeApiKeyResponse>(
                service, METHODID_REVOKE_API_KEY)))
        .addMethod(
          getRotateApiKeyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.apikey.services.v1.RotateApiKeyRequest,
              com.udb.core.apikey.services.v1.RotateApiKeyResponse>(
                service, METHODID_ROTATE_API_KEY)))
        .addMethod(
          getEmergencyRevokeApiKeysMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysRequest,
              com.udb.core.apikey.services.v1.EmergencyRevokeApiKeysResponse>(
                service, METHODID_EMERGENCY_REVOKE_API_KEYS)))
        .addMethod(
          getValidateApiKeyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.apikey.services.v1.ValidateApiKeyRequest,
              com.udb.core.apikey.services.v1.ValidateApiKeyResponse>(
                service, METHODID_VALIDATE_API_KEY)))
        .addMethod(
          getGetApiKeyUsageStatsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.apikey.services.v1.GetApiKeyUsageStatsRequest,
              com.udb.core.apikey.services.v1.GetApiKeyUsageStatsResponse>(
                service, METHODID_GET_API_KEY_USAGE_STATS)))
        .build();
  }

  private static abstract class ApiKeyServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ApiKeyServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.apikey.services.v1.ApikeyServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ApiKeyService");
    }
  }

  private static final class ApiKeyServiceFileDescriptorSupplier
      extends ApiKeyServiceBaseDescriptorSupplier {
    ApiKeyServiceFileDescriptorSupplier() {}
  }

  private static final class ApiKeyServiceMethodDescriptorSupplier
      extends ApiKeyServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    ApiKeyServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (ApiKeyServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ApiKeyServiceFileDescriptorSupplier())
              .addMethod(getCreateApiKeyMethod())
              .addMethod(getGetApiKeyMethod())
              .addMethod(getListApiKeysMethod())
              .addMethod(getUpdateApiKeyMethod())
              .addMethod(getRevokeApiKeyMethod())
              .addMethod(getRotateApiKeyMethod())
              .addMethod(getEmergencyRevokeApiKeysMethod())
              .addMethod(getValidateApiKeyMethod())
              .addMethod(getGetApiKeyUsageStatsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
