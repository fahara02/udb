package com.udb.core.vault.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * VaultService (master-plan 9.1, flagship) — secrets management built into the
 * broker. Three engines, one crypto stack (the broker AES-256-GCM-SIV envelope,
 * reused from `runtime::encryption`):
 *   * KV       — versioned, envelope-encrypted secrets at hierarchical paths
 *                with compare-and-swap, soft delete, and crypto-shred destroy.
 *   * Transit  — encrypt/decrypt/sign/verify/hmac by key NAME; key material is
 *                never exported; versioned keys with ACTIVE/VERIFYING rotation.
 *   * Seal     — every handler fails closed (failed_precondition) when the
 *                master key is unavailable; SealStatus reports the seal state.
 * The sensitive reads (GetSecret, Decrypt) are audited via the outbox compliance
 * envelope. Dynamic database credentials are a declared follow-up.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class VaultServiceGrpc {

  private VaultServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.vault.services.v1.VaultService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.PutSecretRequest,
      com.udb.core.vault.services.v1.PutSecretResponse> getPutSecretMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PutSecret",
      requestType = com.udb.core.vault.services.v1.PutSecretRequest.class,
      responseType = com.udb.core.vault.services.v1.PutSecretResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.PutSecretRequest,
      com.udb.core.vault.services.v1.PutSecretResponse> getPutSecretMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.PutSecretRequest, com.udb.core.vault.services.v1.PutSecretResponse> getPutSecretMethod;
    if ((getPutSecretMethod = VaultServiceGrpc.getPutSecretMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getPutSecretMethod = VaultServiceGrpc.getPutSecretMethod) == null) {
          VaultServiceGrpc.getPutSecretMethod = getPutSecretMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.PutSecretRequest, com.udb.core.vault.services.v1.PutSecretResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PutSecret"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.PutSecretRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.PutSecretResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("PutSecret"))
              .build();
        }
      }
    }
    return getPutSecretMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.GetSecretRequest,
      com.udb.core.vault.services.v1.GetSecretResponse> getGetSecretMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetSecret",
      requestType = com.udb.core.vault.services.v1.GetSecretRequest.class,
      responseType = com.udb.core.vault.services.v1.GetSecretResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.GetSecretRequest,
      com.udb.core.vault.services.v1.GetSecretResponse> getGetSecretMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.GetSecretRequest, com.udb.core.vault.services.v1.GetSecretResponse> getGetSecretMethod;
    if ((getGetSecretMethod = VaultServiceGrpc.getGetSecretMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getGetSecretMethod = VaultServiceGrpc.getGetSecretMethod) == null) {
          VaultServiceGrpc.getGetSecretMethod = getGetSecretMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.GetSecretRequest, com.udb.core.vault.services.v1.GetSecretResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetSecret"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.GetSecretRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.GetSecretResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("GetSecret"))
              .build();
        }
      }
    }
    return getGetSecretMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.ListSecretsRequest,
      com.udb.core.vault.services.v1.ListSecretsResponse> getListSecretsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListSecrets",
      requestType = com.udb.core.vault.services.v1.ListSecretsRequest.class,
      responseType = com.udb.core.vault.services.v1.ListSecretsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.ListSecretsRequest,
      com.udb.core.vault.services.v1.ListSecretsResponse> getListSecretsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.ListSecretsRequest, com.udb.core.vault.services.v1.ListSecretsResponse> getListSecretsMethod;
    if ((getListSecretsMethod = VaultServiceGrpc.getListSecretsMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getListSecretsMethod = VaultServiceGrpc.getListSecretsMethod) == null) {
          VaultServiceGrpc.getListSecretsMethod = getListSecretsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.ListSecretsRequest, com.udb.core.vault.services.v1.ListSecretsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListSecrets"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.ListSecretsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.ListSecretsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("ListSecrets"))
              .build();
        }
      }
    }
    return getListSecretsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.DeleteSecretRequest,
      com.udb.core.vault.services.v1.DeleteSecretResponse> getDeleteSecretMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteSecret",
      requestType = com.udb.core.vault.services.v1.DeleteSecretRequest.class,
      responseType = com.udb.core.vault.services.v1.DeleteSecretResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.DeleteSecretRequest,
      com.udb.core.vault.services.v1.DeleteSecretResponse> getDeleteSecretMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.DeleteSecretRequest, com.udb.core.vault.services.v1.DeleteSecretResponse> getDeleteSecretMethod;
    if ((getDeleteSecretMethod = VaultServiceGrpc.getDeleteSecretMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getDeleteSecretMethod = VaultServiceGrpc.getDeleteSecretMethod) == null) {
          VaultServiceGrpc.getDeleteSecretMethod = getDeleteSecretMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.DeleteSecretRequest, com.udb.core.vault.services.v1.DeleteSecretResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteSecret"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.DeleteSecretRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.DeleteSecretResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("DeleteSecret"))
              .build();
        }
      }
    }
    return getDeleteSecretMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.UndeleteSecretRequest,
      com.udb.core.vault.services.v1.UndeleteSecretResponse> getUndeleteSecretMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UndeleteSecret",
      requestType = com.udb.core.vault.services.v1.UndeleteSecretRequest.class,
      responseType = com.udb.core.vault.services.v1.UndeleteSecretResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.UndeleteSecretRequest,
      com.udb.core.vault.services.v1.UndeleteSecretResponse> getUndeleteSecretMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.UndeleteSecretRequest, com.udb.core.vault.services.v1.UndeleteSecretResponse> getUndeleteSecretMethod;
    if ((getUndeleteSecretMethod = VaultServiceGrpc.getUndeleteSecretMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getUndeleteSecretMethod = VaultServiceGrpc.getUndeleteSecretMethod) == null) {
          VaultServiceGrpc.getUndeleteSecretMethod = getUndeleteSecretMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.UndeleteSecretRequest, com.udb.core.vault.services.v1.UndeleteSecretResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UndeleteSecret"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.UndeleteSecretRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.UndeleteSecretResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("UndeleteSecret"))
              .build();
        }
      }
    }
    return getUndeleteSecretMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.DestroySecretRequest,
      com.udb.core.vault.services.v1.DestroySecretResponse> getDestroySecretMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DestroySecret",
      requestType = com.udb.core.vault.services.v1.DestroySecretRequest.class,
      responseType = com.udb.core.vault.services.v1.DestroySecretResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.DestroySecretRequest,
      com.udb.core.vault.services.v1.DestroySecretResponse> getDestroySecretMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.DestroySecretRequest, com.udb.core.vault.services.v1.DestroySecretResponse> getDestroySecretMethod;
    if ((getDestroySecretMethod = VaultServiceGrpc.getDestroySecretMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getDestroySecretMethod = VaultServiceGrpc.getDestroySecretMethod) == null) {
          VaultServiceGrpc.getDestroySecretMethod = getDestroySecretMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.DestroySecretRequest, com.udb.core.vault.services.v1.DestroySecretResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DestroySecret"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.DestroySecretRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.DestroySecretResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("DestroySecret"))
              .build();
        }
      }
    }
    return getDestroySecretMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.CreateTransitKeyRequest,
      com.udb.core.vault.services.v1.CreateTransitKeyResponse> getCreateTransitKeyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateTransitKey",
      requestType = com.udb.core.vault.services.v1.CreateTransitKeyRequest.class,
      responseType = com.udb.core.vault.services.v1.CreateTransitKeyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.CreateTransitKeyRequest,
      com.udb.core.vault.services.v1.CreateTransitKeyResponse> getCreateTransitKeyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.CreateTransitKeyRequest, com.udb.core.vault.services.v1.CreateTransitKeyResponse> getCreateTransitKeyMethod;
    if ((getCreateTransitKeyMethod = VaultServiceGrpc.getCreateTransitKeyMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getCreateTransitKeyMethod = VaultServiceGrpc.getCreateTransitKeyMethod) == null) {
          VaultServiceGrpc.getCreateTransitKeyMethod = getCreateTransitKeyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.CreateTransitKeyRequest, com.udb.core.vault.services.v1.CreateTransitKeyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateTransitKey"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.CreateTransitKeyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.CreateTransitKeyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("CreateTransitKey"))
              .build();
        }
      }
    }
    return getCreateTransitKeyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.RotateTransitKeyRequest,
      com.udb.core.vault.services.v1.RotateTransitKeyResponse> getRotateTransitKeyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RotateTransitKey",
      requestType = com.udb.core.vault.services.v1.RotateTransitKeyRequest.class,
      responseType = com.udb.core.vault.services.v1.RotateTransitKeyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.RotateTransitKeyRequest,
      com.udb.core.vault.services.v1.RotateTransitKeyResponse> getRotateTransitKeyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.RotateTransitKeyRequest, com.udb.core.vault.services.v1.RotateTransitKeyResponse> getRotateTransitKeyMethod;
    if ((getRotateTransitKeyMethod = VaultServiceGrpc.getRotateTransitKeyMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getRotateTransitKeyMethod = VaultServiceGrpc.getRotateTransitKeyMethod) == null) {
          VaultServiceGrpc.getRotateTransitKeyMethod = getRotateTransitKeyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.RotateTransitKeyRequest, com.udb.core.vault.services.v1.RotateTransitKeyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RotateTransitKey"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.RotateTransitKeyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.RotateTransitKeyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("RotateTransitKey"))
              .build();
        }
      }
    }
    return getRotateTransitKeyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.EncryptRequest,
      com.udb.core.vault.services.v1.EncryptResponse> getEncryptMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Encrypt",
      requestType = com.udb.core.vault.services.v1.EncryptRequest.class,
      responseType = com.udb.core.vault.services.v1.EncryptResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.EncryptRequest,
      com.udb.core.vault.services.v1.EncryptResponse> getEncryptMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.EncryptRequest, com.udb.core.vault.services.v1.EncryptResponse> getEncryptMethod;
    if ((getEncryptMethod = VaultServiceGrpc.getEncryptMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getEncryptMethod = VaultServiceGrpc.getEncryptMethod) == null) {
          VaultServiceGrpc.getEncryptMethod = getEncryptMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.EncryptRequest, com.udb.core.vault.services.v1.EncryptResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Encrypt"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.EncryptRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.EncryptResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("Encrypt"))
              .build();
        }
      }
    }
    return getEncryptMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.DecryptRequest,
      com.udb.core.vault.services.v1.DecryptResponse> getDecryptMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Decrypt",
      requestType = com.udb.core.vault.services.v1.DecryptRequest.class,
      responseType = com.udb.core.vault.services.v1.DecryptResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.DecryptRequest,
      com.udb.core.vault.services.v1.DecryptResponse> getDecryptMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.DecryptRequest, com.udb.core.vault.services.v1.DecryptResponse> getDecryptMethod;
    if ((getDecryptMethod = VaultServiceGrpc.getDecryptMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getDecryptMethod = VaultServiceGrpc.getDecryptMethod) == null) {
          VaultServiceGrpc.getDecryptMethod = getDecryptMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.DecryptRequest, com.udb.core.vault.services.v1.DecryptResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Decrypt"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.DecryptRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.DecryptResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("Decrypt"))
              .build();
        }
      }
    }
    return getDecryptMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.SignRequest,
      com.udb.core.vault.services.v1.SignResponse> getSignMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Sign",
      requestType = com.udb.core.vault.services.v1.SignRequest.class,
      responseType = com.udb.core.vault.services.v1.SignResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.SignRequest,
      com.udb.core.vault.services.v1.SignResponse> getSignMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.SignRequest, com.udb.core.vault.services.v1.SignResponse> getSignMethod;
    if ((getSignMethod = VaultServiceGrpc.getSignMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getSignMethod = VaultServiceGrpc.getSignMethod) == null) {
          VaultServiceGrpc.getSignMethod = getSignMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.SignRequest, com.udb.core.vault.services.v1.SignResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Sign"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.SignRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.SignResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("Sign"))
              .build();
        }
      }
    }
    return getSignMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.VerifyRequest,
      com.udb.core.vault.services.v1.VerifyResponse> getVerifyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Verify",
      requestType = com.udb.core.vault.services.v1.VerifyRequest.class,
      responseType = com.udb.core.vault.services.v1.VerifyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.VerifyRequest,
      com.udb.core.vault.services.v1.VerifyResponse> getVerifyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.VerifyRequest, com.udb.core.vault.services.v1.VerifyResponse> getVerifyMethod;
    if ((getVerifyMethod = VaultServiceGrpc.getVerifyMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getVerifyMethod = VaultServiceGrpc.getVerifyMethod) == null) {
          VaultServiceGrpc.getVerifyMethod = getVerifyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.VerifyRequest, com.udb.core.vault.services.v1.VerifyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Verify"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.VerifyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.VerifyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("Verify"))
              .build();
        }
      }
    }
    return getVerifyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.HmacRequest,
      com.udb.core.vault.services.v1.HmacResponse> getHmacMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Hmac",
      requestType = com.udb.core.vault.services.v1.HmacRequest.class,
      responseType = com.udb.core.vault.services.v1.HmacResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.HmacRequest,
      com.udb.core.vault.services.v1.HmacResponse> getHmacMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.HmacRequest, com.udb.core.vault.services.v1.HmacResponse> getHmacMethod;
    if ((getHmacMethod = VaultServiceGrpc.getHmacMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getHmacMethod = VaultServiceGrpc.getHmacMethod) == null) {
          VaultServiceGrpc.getHmacMethod = getHmacMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.HmacRequest, com.udb.core.vault.services.v1.HmacResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Hmac"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.HmacRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.HmacResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("Hmac"))
              .build();
        }
      }
    }
    return getHmacMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.SealStatusRequest,
      com.udb.core.vault.services.v1.SealStatusResponse> getSealStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SealStatus",
      requestType = com.udb.core.vault.services.v1.SealStatusRequest.class,
      responseType = com.udb.core.vault.services.v1.SealStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.SealStatusRequest,
      com.udb.core.vault.services.v1.SealStatusResponse> getSealStatusMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.SealStatusRequest, com.udb.core.vault.services.v1.SealStatusResponse> getSealStatusMethod;
    if ((getSealStatusMethod = VaultServiceGrpc.getSealStatusMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getSealStatusMethod = VaultServiceGrpc.getSealStatusMethod) == null) {
          VaultServiceGrpc.getSealStatusMethod = getSealStatusMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.SealStatusRequest, com.udb.core.vault.services.v1.SealStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SealStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.SealStatusRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.SealStatusResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("SealStatus"))
              .build();
        }
      }
    }
    return getSealStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.GenerateDatabaseCredentialsRequest,
      com.udb.core.vault.services.v1.GenerateDatabaseCredentialsResponse> getGenerateDatabaseCredentialsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GenerateDatabaseCredentials",
      requestType = com.udb.core.vault.services.v1.GenerateDatabaseCredentialsRequest.class,
      responseType = com.udb.core.vault.services.v1.GenerateDatabaseCredentialsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.GenerateDatabaseCredentialsRequest,
      com.udb.core.vault.services.v1.GenerateDatabaseCredentialsResponse> getGenerateDatabaseCredentialsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.GenerateDatabaseCredentialsRequest, com.udb.core.vault.services.v1.GenerateDatabaseCredentialsResponse> getGenerateDatabaseCredentialsMethod;
    if ((getGenerateDatabaseCredentialsMethod = VaultServiceGrpc.getGenerateDatabaseCredentialsMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getGenerateDatabaseCredentialsMethod = VaultServiceGrpc.getGenerateDatabaseCredentialsMethod) == null) {
          VaultServiceGrpc.getGenerateDatabaseCredentialsMethod = getGenerateDatabaseCredentialsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.GenerateDatabaseCredentialsRequest, com.udb.core.vault.services.v1.GenerateDatabaseCredentialsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GenerateDatabaseCredentials"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.GenerateDatabaseCredentialsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.GenerateDatabaseCredentialsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("GenerateDatabaseCredentials"))
              .build();
        }
      }
    }
    return getGenerateDatabaseCredentialsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.RevokeDatabaseCredentialsRequest,
      com.udb.core.vault.services.v1.RevokeDatabaseCredentialsResponse> getRevokeDatabaseCredentialsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RevokeDatabaseCredentials",
      requestType = com.udb.core.vault.services.v1.RevokeDatabaseCredentialsRequest.class,
      responseType = com.udb.core.vault.services.v1.RevokeDatabaseCredentialsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.RevokeDatabaseCredentialsRequest,
      com.udb.core.vault.services.v1.RevokeDatabaseCredentialsResponse> getRevokeDatabaseCredentialsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.RevokeDatabaseCredentialsRequest, com.udb.core.vault.services.v1.RevokeDatabaseCredentialsResponse> getRevokeDatabaseCredentialsMethod;
    if ((getRevokeDatabaseCredentialsMethod = VaultServiceGrpc.getRevokeDatabaseCredentialsMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getRevokeDatabaseCredentialsMethod = VaultServiceGrpc.getRevokeDatabaseCredentialsMethod) == null) {
          VaultServiceGrpc.getRevokeDatabaseCredentialsMethod = getRevokeDatabaseCredentialsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.RevokeDatabaseCredentialsRequest, com.udb.core.vault.services.v1.RevokeDatabaseCredentialsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RevokeDatabaseCredentials"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.RevokeDatabaseCredentialsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.RevokeDatabaseCredentialsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("RevokeDatabaseCredentials"))
              .build();
        }
      }
    }
    return getRevokeDatabaseCredentialsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsRequest,
      com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsResponse> getEmergencyRevokeDatabaseCredentialsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "EmergencyRevokeDatabaseCredentials",
      requestType = com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsRequest.class,
      responseType = com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsRequest,
      com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsResponse> getEmergencyRevokeDatabaseCredentialsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsRequest, com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsResponse> getEmergencyRevokeDatabaseCredentialsMethod;
    if ((getEmergencyRevokeDatabaseCredentialsMethod = VaultServiceGrpc.getEmergencyRevokeDatabaseCredentialsMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getEmergencyRevokeDatabaseCredentialsMethod = VaultServiceGrpc.getEmergencyRevokeDatabaseCredentialsMethod) == null) {
          VaultServiceGrpc.getEmergencyRevokeDatabaseCredentialsMethod = getEmergencyRevokeDatabaseCredentialsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsRequest, com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "EmergencyRevokeDatabaseCredentials"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("EmergencyRevokeDatabaseCredentials"))
              .build();
        }
      }
    }
    return getEmergencyRevokeDatabaseCredentialsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.GenerateDataKeyRequest,
      com.udb.core.vault.services.v1.GenerateDataKeyResponse> getGenerateDataKeyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GenerateDataKey",
      requestType = com.udb.core.vault.services.v1.GenerateDataKeyRequest.class,
      responseType = com.udb.core.vault.services.v1.GenerateDataKeyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.GenerateDataKeyRequest,
      com.udb.core.vault.services.v1.GenerateDataKeyResponse> getGenerateDataKeyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.GenerateDataKeyRequest, com.udb.core.vault.services.v1.GenerateDataKeyResponse> getGenerateDataKeyMethod;
    if ((getGenerateDataKeyMethod = VaultServiceGrpc.getGenerateDataKeyMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getGenerateDataKeyMethod = VaultServiceGrpc.getGenerateDataKeyMethod) == null) {
          VaultServiceGrpc.getGenerateDataKeyMethod = getGenerateDataKeyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.GenerateDataKeyRequest, com.udb.core.vault.services.v1.GenerateDataKeyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GenerateDataKey"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.GenerateDataKeyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.GenerateDataKeyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("GenerateDataKey"))
              .build();
        }
      }
    }
    return getGenerateDataKeyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.RewrapRequest,
      com.udb.core.vault.services.v1.RewrapResponse> getRewrapMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Rewrap",
      requestType = com.udb.core.vault.services.v1.RewrapRequest.class,
      responseType = com.udb.core.vault.services.v1.RewrapResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.RewrapRequest,
      com.udb.core.vault.services.v1.RewrapResponse> getRewrapMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.RewrapRequest, com.udb.core.vault.services.v1.RewrapResponse> getRewrapMethod;
    if ((getRewrapMethod = VaultServiceGrpc.getRewrapMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getRewrapMethod = VaultServiceGrpc.getRewrapMethod) == null) {
          VaultServiceGrpc.getRewrapMethod = getRewrapMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.RewrapRequest, com.udb.core.vault.services.v1.RewrapResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Rewrap"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.RewrapRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.RewrapResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("Rewrap"))
              .build();
        }
      }
    }
    return getRewrapMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.GetTransitPublicKeyRequest,
      com.udb.core.vault.services.v1.GetTransitPublicKeyResponse> getGetTransitPublicKeyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTransitPublicKey",
      requestType = com.udb.core.vault.services.v1.GetTransitPublicKeyRequest.class,
      responseType = com.udb.core.vault.services.v1.GetTransitPublicKeyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.GetTransitPublicKeyRequest,
      com.udb.core.vault.services.v1.GetTransitPublicKeyResponse> getGetTransitPublicKeyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.GetTransitPublicKeyRequest, com.udb.core.vault.services.v1.GetTransitPublicKeyResponse> getGetTransitPublicKeyMethod;
    if ((getGetTransitPublicKeyMethod = VaultServiceGrpc.getGetTransitPublicKeyMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getGetTransitPublicKeyMethod = VaultServiceGrpc.getGetTransitPublicKeyMethod) == null) {
          VaultServiceGrpc.getGetTransitPublicKeyMethod = getGetTransitPublicKeyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.GetTransitPublicKeyRequest, com.udb.core.vault.services.v1.GetTransitPublicKeyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTransitPublicKey"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.GetTransitPublicKeyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.GetTransitPublicKeyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("GetTransitPublicKey"))
              .build();
        }
      }
    }
    return getGetTransitPublicKeyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.BatchEncryptRequest,
      com.udb.core.vault.services.v1.BatchEncryptResponse> getBatchEncryptMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "BatchEncrypt",
      requestType = com.udb.core.vault.services.v1.BatchEncryptRequest.class,
      responseType = com.udb.core.vault.services.v1.BatchEncryptResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.BatchEncryptRequest,
      com.udb.core.vault.services.v1.BatchEncryptResponse> getBatchEncryptMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.BatchEncryptRequest, com.udb.core.vault.services.v1.BatchEncryptResponse> getBatchEncryptMethod;
    if ((getBatchEncryptMethod = VaultServiceGrpc.getBatchEncryptMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getBatchEncryptMethod = VaultServiceGrpc.getBatchEncryptMethod) == null) {
          VaultServiceGrpc.getBatchEncryptMethod = getBatchEncryptMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.BatchEncryptRequest, com.udb.core.vault.services.v1.BatchEncryptResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "BatchEncrypt"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.BatchEncryptRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.BatchEncryptResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("BatchEncrypt"))
              .build();
        }
      }
    }
    return getBatchEncryptMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.BatchDecryptRequest,
      com.udb.core.vault.services.v1.BatchDecryptResponse> getBatchDecryptMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "BatchDecrypt",
      requestType = com.udb.core.vault.services.v1.BatchDecryptRequest.class,
      responseType = com.udb.core.vault.services.v1.BatchDecryptResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.BatchDecryptRequest,
      com.udb.core.vault.services.v1.BatchDecryptResponse> getBatchDecryptMethod() {
    io.grpc.MethodDescriptor<com.udb.core.vault.services.v1.BatchDecryptRequest, com.udb.core.vault.services.v1.BatchDecryptResponse> getBatchDecryptMethod;
    if ((getBatchDecryptMethod = VaultServiceGrpc.getBatchDecryptMethod) == null) {
      synchronized (VaultServiceGrpc.class) {
        if ((getBatchDecryptMethod = VaultServiceGrpc.getBatchDecryptMethod) == null) {
          VaultServiceGrpc.getBatchDecryptMethod = getBatchDecryptMethod =
              io.grpc.MethodDescriptor.<com.udb.core.vault.services.v1.BatchDecryptRequest, com.udb.core.vault.services.v1.BatchDecryptResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "BatchDecrypt"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.BatchDecryptRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.vault.services.v1.BatchDecryptResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VaultServiceMethodDescriptorSupplier("BatchDecrypt"))
              .build();
        }
      }
    }
    return getBatchDecryptMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static VaultServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<VaultServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<VaultServiceStub>() {
        @java.lang.Override
        public VaultServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new VaultServiceStub(channel, callOptions);
        }
      };
    return VaultServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static VaultServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<VaultServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<VaultServiceBlockingV2Stub>() {
        @java.lang.Override
        public VaultServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new VaultServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return VaultServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static VaultServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<VaultServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<VaultServiceBlockingStub>() {
        @java.lang.Override
        public VaultServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new VaultServiceBlockingStub(channel, callOptions);
        }
      };
    return VaultServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static VaultServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<VaultServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<VaultServiceFutureStub>() {
        @java.lang.Override
        public VaultServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new VaultServiceFutureStub(channel, callOptions);
        }
      };
    return VaultServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * VaultService (master-plan 9.1, flagship) — secrets management built into the
   * broker. Three engines, one crypto stack (the broker AES-256-GCM-SIV envelope,
   * reused from `runtime::encryption`):
   *   * KV       — versioned, envelope-encrypted secrets at hierarchical paths
   *                with compare-and-swap, soft delete, and crypto-shred destroy.
   *   * Transit  — encrypt/decrypt/sign/verify/hmac by key NAME; key material is
   *                never exported; versioned keys with ACTIVE/VERIFYING rotation.
   *   * Seal     — every handler fails closed (failed_precondition) when the
   *                master key is unavailable; SealStatus reports the seal state.
   * The sensitive reads (GetSecret, Decrypt) are audited via the outbox compliance
   * envelope. Dynamic database credentials are a declared follow-up.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Write a new secret version. Compare-and-swap: `expected_version` must equal
     * the current latest version (0 for a brand-new path) or the write is rejected.
     * </pre>
     */
    default void putSecret(com.udb.core.vault.services.v1.PutSecretRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.PutSecretResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPutSecretMethod(), responseObserver);
    }

    /**
     * <pre>
     * Read the secret value (latest active version, or a specific version). This
     * is the sensitive vault read: it is AUDITED via the outbox compliance envelope.
     * </pre>
     */
    default void getSecret(com.udb.core.vault.services.v1.GetSecretRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.GetSecretResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetSecretMethod(), responseObserver);
    }

    /**
     * <pre>
     * List secret paths under an optional prefix. Returns metadata only — NEVER
     * any secret value.
     * </pre>
     */
    default void listSecrets(com.udb.core.vault.services.v1.ListSecretsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.ListSecretsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListSecretsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Soft-delete the latest version (recoverable bookkeeping state). The ciphertext
     * is retained; use DestroySecret to crypto-shred.
     * </pre>
     */
    default void deleteSecret(com.udb.core.vault.services.v1.DeleteSecretRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.DeleteSecretResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteSecretMethod(), responseObserver);
    }

    /**
     * <pre>
     * Restore a soft-DELETED secret: flip its latest deleted version back to ACTIVE.
     * A soft delete keeps the ciphertext + wrapped key, so recovery is exact. A
     * crypto-shredded (DestroySecret) version can NEVER be restored.
     * </pre>
     */
    default void undeleteSecret(com.udb.core.vault.services.v1.UndeleteSecretRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.UndeleteSecretResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUndeleteSecretMethod(), responseObserver);
    }

    /**
     * <pre>
     * Crypto-shred every version of a secret: clears the wrapped DEK + ciphertext
     * so the value is irrecoverable. DESTRUCTIVE + irreversible — a confirmation
     * token is required and an empty token fails closed.
     * </pre>
     */
    default void destroySecret(com.udb.core.vault.services.v1.DestroySecretRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.DestroySecretResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDestroySecretMethod(), responseObserver);
    }

    /**
     * <pre>
     * Create a named transit key (version 1, ACTIVE). Key material is generated
     * server-side and never returned.
     * </pre>
     */
    default void createTransitKey(com.udb.core.vault.services.v1.CreateTransitKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.CreateTransitKeyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateTransitKeyMethod(), responseObserver);
    }

    /**
     * <pre>
     * Rotate a named transit key: the current ACTIVE version is demoted to
     * VERIFYING (still decrypts/verifies during the overlap) and a fresh ACTIVE
     * version is generated. New encryptions/signatures use the new version.
     * </pre>
     */
    default void rotateTransitKey(com.udb.core.vault.services.v1.RotateTransitKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.RotateTransitKeyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRotateTransitKeyMethod(), responseObserver);
    }

    /**
     * <pre>
     * Encrypt plaintext under the ACTIVE version of a named key. Returns a
     * versioned ciphertext envelope; the key material is never returned.
     * </pre>
     */
    default void encrypt(com.udb.core.vault.services.v1.EncryptRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.EncryptResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEncryptMethod(), responseObserver);
    }

    /**
     * <pre>
     * Decrypt a transit ciphertext envelope. The version is read from the envelope
     * and ACTIVE or VERIFYING versions are accepted. This is a sensitive read and is
     * AUDITED via the outbox compliance envelope.
     * </pre>
     */
    default void decrypt(com.udb.core.vault.services.v1.DecryptRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.DecryptResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDecryptMethod(), responseObserver);
    }

    /**
     * <pre>
     * Produce a detached MAC ("signature") over the input under the ACTIVE key
     * version. Implemented as HMAC-SHA256 from the version DEK (symmetric);
     * asymmetric signing is a follow-up. Key material is never returned.
     * </pre>
     */
    default void sign(com.udb.core.vault.services.v1.SignRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.SignResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSignMethod(), responseObserver);
    }

    /**
     * <pre>
     * Verify a MAC/signature over the input. The version is read from the
     * signature and ACTIVE or VERIFYING versions are accepted; comparison is
     * constant-time.
     * </pre>
     */
    default void verify(com.udb.core.vault.services.v1.VerifyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.VerifyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getVerifyMethod(), responseObserver);
    }

    /**
     * <pre>
     * Compute an HMAC-SHA256 over the input under the ACTIVE key version. Key
     * material is never returned.
     * </pre>
     */
    default void hmac(com.udb.core.vault.services.v1.HmacRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.HmacResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHmacMethod(), responseObserver);
    }

    /**
     * <pre>
     * Report whether the vault is sealed (master key unavailable). Always answers,
     * even when sealed, so operators can diagnose a sealed vault.
     * </pre>
     */
    default void sealStatus(com.udb.core.vault.services.v1.SealStatusRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.SealStatusResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSealStatusMethod(), responseObserver);
    }

    /**
     * <pre>
     * Mint short-lived, per-request Postgres credentials with a durable lease.
     * The requested role_name is an operator-configured alias resolved from
     * UDB_VAULT_DB_ROLES_JSON; arbitrary request-supplied role grants fail closed.
     * WORKER_VAULT_LEASE_REAPER revokes and drops expired generated login roles.
     * </pre>
     */
    default void generateDatabaseCredentials(com.udb.core.vault.services.v1.GenerateDatabaseCredentialsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.GenerateDatabaseCredentialsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGenerateDatabaseCredentialsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Revoke one lease in the authenticated tenant/project. The durable state is
     * moved to REVOKING before physical session fencing and becomes REVOKED only
     * after the generated role is proven absent. Replays are naturally idempotent.
     * </pre>
     */
    default void revokeDatabaseCredentials(com.udb.core.vault.services.v1.RevokeDatabaseCredentialsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.RevokeDatabaseCredentialsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRevokeDatabaseCredentialsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Emergency kill-switch for every non-terminal lease in exactly one verified
     * tenant/project. A confirmation token bound to both scope dimensions prevents
     * an accidental tenant-wide or cross-project credential wipe.
     * </pre>
     */
    default void emergencyRevokeDatabaseCredentials(com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEmergencyRevokeDatabaseCredentialsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Generate a fresh 256-bit data key, returned BOTH plaintext (for the caller to
     * encrypt data locally) AND wrapped under the named transit key (store this and
     * Decrypt/Rewrap it later). Envelope-encryption without exposing the transit
     * key. Reuses the transit seal path; AUDITED via the outbox compliance envelope.
     * </pre>
     */
    default void generateDataKey(com.udb.core.vault.services.v1.GenerateDataKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.GenerateDataKeyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGenerateDataKeyMethod(), responseObserver);
    }

    /**
     * <pre>
     * Re-wrap a transit ciphertext under the key's CURRENT active version: decrypt
     * with the version embedded in the envelope, then re-seal with the active
     * version. The post-rotation migration primitive (no plaintext leaves the
     * broker). AUDITED via the outbox compliance envelope.
     * </pre>
     */
    default void rewrap(com.udb.core.vault.services.v1.RewrapRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.RewrapResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRewrapMethod(), responseObserver);
    }

    /**
     * <pre>
     * Export the Ed25519 PUBLIC key(s) of a signing transit key so an external
     * party can verify broker-produced signatures without ever holding the private
     * key — the missing half that makes Sign/Verify genuinely asymmetric. Only
     * valid for keys created with the ed25519 algorithm; READ-ONLY (public keys are
     * not secret). Returns one entry per usable (ACTIVE/VERIFYING) version.
     * </pre>
     */
    default void getTransitPublicKey(com.udb.core.vault.services.v1.GetTransitPublicKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.GetTransitPublicKeyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTransitPublicKeyMethod(), responseObserver);
    }

    /**
     * <pre>
     * Encrypt MANY plaintexts under one transit key in a single call: the key is
     * unwrapped ONCE and each plaintext sealed with the active version, amortizing
     * the master-key unwrap over the batch. Order-preserving. AUDITED.
     * </pre>
     */
    default void batchEncrypt(com.udb.core.vault.services.v1.BatchEncryptRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.BatchEncryptResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getBatchEncryptMethod(), responseObserver);
    }

    /**
     * <pre>
     * Decrypt MANY transit ciphertexts under one key in a single call; each
     * ciphertext carries its own key version in the envelope. Order-preserving.
     * </pre>
     */
    default void batchDecrypt(com.udb.core.vault.services.v1.BatchDecryptRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.BatchDecryptResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getBatchDecryptMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service VaultService.
   * <pre>
   * VaultService (master-plan 9.1, flagship) — secrets management built into the
   * broker. Three engines, one crypto stack (the broker AES-256-GCM-SIV envelope,
   * reused from `runtime::encryption`):
   *   * KV       — versioned, envelope-encrypted secrets at hierarchical paths
   *                with compare-and-swap, soft delete, and crypto-shred destroy.
   *   * Transit  — encrypt/decrypt/sign/verify/hmac by key NAME; key material is
   *                never exported; versioned keys with ACTIVE/VERIFYING rotation.
   *   * Seal     — every handler fails closed (failed_precondition) when the
   *                master key is unavailable; SealStatus reports the seal state.
   * The sensitive reads (GetSecret, Decrypt) are audited via the outbox compliance
   * envelope. Dynamic database credentials are a declared follow-up.
   * </pre>
   */
  public static abstract class VaultServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return VaultServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service VaultService.
   * <pre>
   * VaultService (master-plan 9.1, flagship) — secrets management built into the
   * broker. Three engines, one crypto stack (the broker AES-256-GCM-SIV envelope,
   * reused from `runtime::encryption`):
   *   * KV       — versioned, envelope-encrypted secrets at hierarchical paths
   *                with compare-and-swap, soft delete, and crypto-shred destroy.
   *   * Transit  — encrypt/decrypt/sign/verify/hmac by key NAME; key material is
   *                never exported; versioned keys with ACTIVE/VERIFYING rotation.
   *   * Seal     — every handler fails closed (failed_precondition) when the
   *                master key is unavailable; SealStatus reports the seal state.
   * The sensitive reads (GetSecret, Decrypt) are audited via the outbox compliance
   * envelope. Dynamic database credentials are a declared follow-up.
   * </pre>
   */
  public static final class VaultServiceStub
      extends io.grpc.stub.AbstractAsyncStub<VaultServiceStub> {
    private VaultServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VaultServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new VaultServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Write a new secret version. Compare-and-swap: `expected_version` must equal
     * the current latest version (0 for a brand-new path) or the write is rejected.
     * </pre>
     */
    public void putSecret(com.udb.core.vault.services.v1.PutSecretRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.PutSecretResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPutSecretMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Read the secret value (latest active version, or a specific version). This
     * is the sensitive vault read: it is AUDITED via the outbox compliance envelope.
     * </pre>
     */
    public void getSecret(com.udb.core.vault.services.v1.GetSecretRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.GetSecretResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetSecretMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List secret paths under an optional prefix. Returns metadata only — NEVER
     * any secret value.
     * </pre>
     */
    public void listSecrets(com.udb.core.vault.services.v1.ListSecretsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.ListSecretsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListSecretsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Soft-delete the latest version (recoverable bookkeeping state). The ciphertext
     * is retained; use DestroySecret to crypto-shred.
     * </pre>
     */
    public void deleteSecret(com.udb.core.vault.services.v1.DeleteSecretRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.DeleteSecretResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteSecretMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Restore a soft-DELETED secret: flip its latest deleted version back to ACTIVE.
     * A soft delete keeps the ciphertext + wrapped key, so recovery is exact. A
     * crypto-shredded (DestroySecret) version can NEVER be restored.
     * </pre>
     */
    public void undeleteSecret(com.udb.core.vault.services.v1.UndeleteSecretRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.UndeleteSecretResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUndeleteSecretMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Crypto-shred every version of a secret: clears the wrapped DEK + ciphertext
     * so the value is irrecoverable. DESTRUCTIVE + irreversible — a confirmation
     * token is required and an empty token fails closed.
     * </pre>
     */
    public void destroySecret(com.udb.core.vault.services.v1.DestroySecretRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.DestroySecretResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDestroySecretMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Create a named transit key (version 1, ACTIVE). Key material is generated
     * server-side and never returned.
     * </pre>
     */
    public void createTransitKey(com.udb.core.vault.services.v1.CreateTransitKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.CreateTransitKeyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateTransitKeyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Rotate a named transit key: the current ACTIVE version is demoted to
     * VERIFYING (still decrypts/verifies during the overlap) and a fresh ACTIVE
     * version is generated. New encryptions/signatures use the new version.
     * </pre>
     */
    public void rotateTransitKey(com.udb.core.vault.services.v1.RotateTransitKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.RotateTransitKeyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRotateTransitKeyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Encrypt plaintext under the ACTIVE version of a named key. Returns a
     * versioned ciphertext envelope; the key material is never returned.
     * </pre>
     */
    public void encrypt(com.udb.core.vault.services.v1.EncryptRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.EncryptResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEncryptMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Decrypt a transit ciphertext envelope. The version is read from the envelope
     * and ACTIVE or VERIFYING versions are accepted. This is a sensitive read and is
     * AUDITED via the outbox compliance envelope.
     * </pre>
     */
    public void decrypt(com.udb.core.vault.services.v1.DecryptRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.DecryptResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDecryptMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Produce a detached MAC ("signature") over the input under the ACTIVE key
     * version. Implemented as HMAC-SHA256 from the version DEK (symmetric);
     * asymmetric signing is a follow-up. Key material is never returned.
     * </pre>
     */
    public void sign(com.udb.core.vault.services.v1.SignRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.SignResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSignMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Verify a MAC/signature over the input. The version is read from the
     * signature and ACTIVE or VERIFYING versions are accepted; comparison is
     * constant-time.
     * </pre>
     */
    public void verify(com.udb.core.vault.services.v1.VerifyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.VerifyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getVerifyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Compute an HMAC-SHA256 over the input under the ACTIVE key version. Key
     * material is never returned.
     * </pre>
     */
    public void hmac(com.udb.core.vault.services.v1.HmacRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.HmacResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHmacMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Report whether the vault is sealed (master key unavailable). Always answers,
     * even when sealed, so operators can diagnose a sealed vault.
     * </pre>
     */
    public void sealStatus(com.udb.core.vault.services.v1.SealStatusRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.SealStatusResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSealStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Mint short-lived, per-request Postgres credentials with a durable lease.
     * The requested role_name is an operator-configured alias resolved from
     * UDB_VAULT_DB_ROLES_JSON; arbitrary request-supplied role grants fail closed.
     * WORKER_VAULT_LEASE_REAPER revokes and drops expired generated login roles.
     * </pre>
     */
    public void generateDatabaseCredentials(com.udb.core.vault.services.v1.GenerateDatabaseCredentialsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.GenerateDatabaseCredentialsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGenerateDatabaseCredentialsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Revoke one lease in the authenticated tenant/project. The durable state is
     * moved to REVOKING before physical session fencing and becomes REVOKED only
     * after the generated role is proven absent. Replays are naturally idempotent.
     * </pre>
     */
    public void revokeDatabaseCredentials(com.udb.core.vault.services.v1.RevokeDatabaseCredentialsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.RevokeDatabaseCredentialsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRevokeDatabaseCredentialsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Emergency kill-switch for every non-terminal lease in exactly one verified
     * tenant/project. A confirmation token bound to both scope dimensions prevents
     * an accidental tenant-wide or cross-project credential wipe.
     * </pre>
     */
    public void emergencyRevokeDatabaseCredentials(com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEmergencyRevokeDatabaseCredentialsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Generate a fresh 256-bit data key, returned BOTH plaintext (for the caller to
     * encrypt data locally) AND wrapped under the named transit key (store this and
     * Decrypt/Rewrap it later). Envelope-encryption without exposing the transit
     * key. Reuses the transit seal path; AUDITED via the outbox compliance envelope.
     * </pre>
     */
    public void generateDataKey(com.udb.core.vault.services.v1.GenerateDataKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.GenerateDataKeyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGenerateDataKeyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Re-wrap a transit ciphertext under the key's CURRENT active version: decrypt
     * with the version embedded in the envelope, then re-seal with the active
     * version. The post-rotation migration primitive (no plaintext leaves the
     * broker). AUDITED via the outbox compliance envelope.
     * </pre>
     */
    public void rewrap(com.udb.core.vault.services.v1.RewrapRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.RewrapResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRewrapMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Export the Ed25519 PUBLIC key(s) of a signing transit key so an external
     * party can verify broker-produced signatures without ever holding the private
     * key — the missing half that makes Sign/Verify genuinely asymmetric. Only
     * valid for keys created with the ed25519 algorithm; READ-ONLY (public keys are
     * not secret). Returns one entry per usable (ACTIVE/VERIFYING) version.
     * </pre>
     */
    public void getTransitPublicKey(com.udb.core.vault.services.v1.GetTransitPublicKeyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.GetTransitPublicKeyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTransitPublicKeyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Encrypt MANY plaintexts under one transit key in a single call: the key is
     * unwrapped ONCE and each plaintext sealed with the active version, amortizing
     * the master-key unwrap over the batch. Order-preserving. AUDITED.
     * </pre>
     */
    public void batchEncrypt(com.udb.core.vault.services.v1.BatchEncryptRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.BatchEncryptResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getBatchEncryptMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Decrypt MANY transit ciphertexts under one key in a single call; each
     * ciphertext carries its own key version in the envelope. Order-preserving.
     * </pre>
     */
    public void batchDecrypt(com.udb.core.vault.services.v1.BatchDecryptRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.BatchDecryptResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getBatchDecryptMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service VaultService.
   * <pre>
   * VaultService (master-plan 9.1, flagship) — secrets management built into the
   * broker. Three engines, one crypto stack (the broker AES-256-GCM-SIV envelope,
   * reused from `runtime::encryption`):
   *   * KV       — versioned, envelope-encrypted secrets at hierarchical paths
   *                with compare-and-swap, soft delete, and crypto-shred destroy.
   *   * Transit  — encrypt/decrypt/sign/verify/hmac by key NAME; key material is
   *                never exported; versioned keys with ACTIVE/VERIFYING rotation.
   *   * Seal     — every handler fails closed (failed_precondition) when the
   *                master key is unavailable; SealStatus reports the seal state.
   * The sensitive reads (GetSecret, Decrypt) are audited via the outbox compliance
   * envelope. Dynamic database credentials are a declared follow-up.
   * </pre>
   */
  public static final class VaultServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<VaultServiceBlockingV2Stub> {
    private VaultServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VaultServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new VaultServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Write a new secret version. Compare-and-swap: `expected_version` must equal
     * the current latest version (0 for a brand-new path) or the write is rejected.
     * </pre>
     */
    public com.udb.core.vault.services.v1.PutSecretResponse putSecret(com.udb.core.vault.services.v1.PutSecretRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPutSecretMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Read the secret value (latest active version, or a specific version). This
     * is the sensitive vault read: it is AUDITED via the outbox compliance envelope.
     * </pre>
     */
    public com.udb.core.vault.services.v1.GetSecretResponse getSecret(com.udb.core.vault.services.v1.GetSecretRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetSecretMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List secret paths under an optional prefix. Returns metadata only — NEVER
     * any secret value.
     * </pre>
     */
    public com.udb.core.vault.services.v1.ListSecretsResponse listSecrets(com.udb.core.vault.services.v1.ListSecretsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListSecretsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Soft-delete the latest version (recoverable bookkeeping state). The ciphertext
     * is retained; use DestroySecret to crypto-shred.
     * </pre>
     */
    public com.udb.core.vault.services.v1.DeleteSecretResponse deleteSecret(com.udb.core.vault.services.v1.DeleteSecretRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeleteSecretMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Restore a soft-DELETED secret: flip its latest deleted version back to ACTIVE.
     * A soft delete keeps the ciphertext + wrapped key, so recovery is exact. A
     * crypto-shredded (DestroySecret) version can NEVER be restored.
     * </pre>
     */
    public com.udb.core.vault.services.v1.UndeleteSecretResponse undeleteSecret(com.udb.core.vault.services.v1.UndeleteSecretRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getUndeleteSecretMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Crypto-shred every version of a secret: clears the wrapped DEK + ciphertext
     * so the value is irrecoverable. DESTRUCTIVE + irreversible — a confirmation
     * token is required and an empty token fails closed.
     * </pre>
     */
    public com.udb.core.vault.services.v1.DestroySecretResponse destroySecret(com.udb.core.vault.services.v1.DestroySecretRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDestroySecretMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Create a named transit key (version 1, ACTIVE). Key material is generated
     * server-side and never returned.
     * </pre>
     */
    public com.udb.core.vault.services.v1.CreateTransitKeyResponse createTransitKey(com.udb.core.vault.services.v1.CreateTransitKeyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreateTransitKeyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Rotate a named transit key: the current ACTIVE version is demoted to
     * VERIFYING (still decrypts/verifies during the overlap) and a fresh ACTIVE
     * version is generated. New encryptions/signatures use the new version.
     * </pre>
     */
    public com.udb.core.vault.services.v1.RotateTransitKeyResponse rotateTransitKey(com.udb.core.vault.services.v1.RotateTransitKeyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRotateTransitKeyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Encrypt plaintext under the ACTIVE version of a named key. Returns a
     * versioned ciphertext envelope; the key material is never returned.
     * </pre>
     */
    public com.udb.core.vault.services.v1.EncryptResponse encrypt(com.udb.core.vault.services.v1.EncryptRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getEncryptMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Decrypt a transit ciphertext envelope. The version is read from the envelope
     * and ACTIVE or VERIFYING versions are accepted. This is a sensitive read and is
     * AUDITED via the outbox compliance envelope.
     * </pre>
     */
    public com.udb.core.vault.services.v1.DecryptResponse decrypt(com.udb.core.vault.services.v1.DecryptRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDecryptMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Produce a detached MAC ("signature") over the input under the ACTIVE key
     * version. Implemented as HMAC-SHA256 from the version DEK (symmetric);
     * asymmetric signing is a follow-up. Key material is never returned.
     * </pre>
     */
    public com.udb.core.vault.services.v1.SignResponse sign(com.udb.core.vault.services.v1.SignRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSignMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Verify a MAC/signature over the input. The version is read from the
     * signature and ACTIVE or VERIFYING versions are accepted; comparison is
     * constant-time.
     * </pre>
     */
    public com.udb.core.vault.services.v1.VerifyResponse verify(com.udb.core.vault.services.v1.VerifyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getVerifyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Compute an HMAC-SHA256 over the input under the ACTIVE key version. Key
     * material is never returned.
     * </pre>
     */
    public com.udb.core.vault.services.v1.HmacResponse hmac(com.udb.core.vault.services.v1.HmacRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getHmacMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Report whether the vault is sealed (master key unavailable). Always answers,
     * even when sealed, so operators can diagnose a sealed vault.
     * </pre>
     */
    public com.udb.core.vault.services.v1.SealStatusResponse sealStatus(com.udb.core.vault.services.v1.SealStatusRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSealStatusMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Mint short-lived, per-request Postgres credentials with a durable lease.
     * The requested role_name is an operator-configured alias resolved from
     * UDB_VAULT_DB_ROLES_JSON; arbitrary request-supplied role grants fail closed.
     * WORKER_VAULT_LEASE_REAPER revokes and drops expired generated login roles.
     * </pre>
     */
    public com.udb.core.vault.services.v1.GenerateDatabaseCredentialsResponse generateDatabaseCredentials(com.udb.core.vault.services.v1.GenerateDatabaseCredentialsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGenerateDatabaseCredentialsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Revoke one lease in the authenticated tenant/project. The durable state is
     * moved to REVOKING before physical session fencing and becomes REVOKED only
     * after the generated role is proven absent. Replays are naturally idempotent.
     * </pre>
     */
    public com.udb.core.vault.services.v1.RevokeDatabaseCredentialsResponse revokeDatabaseCredentials(com.udb.core.vault.services.v1.RevokeDatabaseCredentialsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRevokeDatabaseCredentialsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Emergency kill-switch for every non-terminal lease in exactly one verified
     * tenant/project. A confirmation token bound to both scope dimensions prevents
     * an accidental tenant-wide or cross-project credential wipe.
     * </pre>
     */
    public com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsResponse emergencyRevokeDatabaseCredentials(com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getEmergencyRevokeDatabaseCredentialsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Generate a fresh 256-bit data key, returned BOTH plaintext (for the caller to
     * encrypt data locally) AND wrapped under the named transit key (store this and
     * Decrypt/Rewrap it later). Envelope-encryption without exposing the transit
     * key. Reuses the transit seal path; AUDITED via the outbox compliance envelope.
     * </pre>
     */
    public com.udb.core.vault.services.v1.GenerateDataKeyResponse generateDataKey(com.udb.core.vault.services.v1.GenerateDataKeyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGenerateDataKeyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Re-wrap a transit ciphertext under the key's CURRENT active version: decrypt
     * with the version embedded in the envelope, then re-seal with the active
     * version. The post-rotation migration primitive (no plaintext leaves the
     * broker). AUDITED via the outbox compliance envelope.
     * </pre>
     */
    public com.udb.core.vault.services.v1.RewrapResponse rewrap(com.udb.core.vault.services.v1.RewrapRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRewrapMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Export the Ed25519 PUBLIC key(s) of a signing transit key so an external
     * party can verify broker-produced signatures without ever holding the private
     * key — the missing half that makes Sign/Verify genuinely asymmetric. Only
     * valid for keys created with the ed25519 algorithm; READ-ONLY (public keys are
     * not secret). Returns one entry per usable (ACTIVE/VERIFYING) version.
     * </pre>
     */
    public com.udb.core.vault.services.v1.GetTransitPublicKeyResponse getTransitPublicKey(com.udb.core.vault.services.v1.GetTransitPublicKeyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetTransitPublicKeyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Encrypt MANY plaintexts under one transit key in a single call: the key is
     * unwrapped ONCE and each plaintext sealed with the active version, amortizing
     * the master-key unwrap over the batch. Order-preserving. AUDITED.
     * </pre>
     */
    public com.udb.core.vault.services.v1.BatchEncryptResponse batchEncrypt(com.udb.core.vault.services.v1.BatchEncryptRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getBatchEncryptMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Decrypt MANY transit ciphertexts under one key in a single call; each
     * ciphertext carries its own key version in the envelope. Order-preserving.
     * </pre>
     */
    public com.udb.core.vault.services.v1.BatchDecryptResponse batchDecrypt(com.udb.core.vault.services.v1.BatchDecryptRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getBatchDecryptMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service VaultService.
   * <pre>
   * VaultService (master-plan 9.1, flagship) — secrets management built into the
   * broker. Three engines, one crypto stack (the broker AES-256-GCM-SIV envelope,
   * reused from `runtime::encryption`):
   *   * KV       — versioned, envelope-encrypted secrets at hierarchical paths
   *                with compare-and-swap, soft delete, and crypto-shred destroy.
   *   * Transit  — encrypt/decrypt/sign/verify/hmac by key NAME; key material is
   *                never exported; versioned keys with ACTIVE/VERIFYING rotation.
   *   * Seal     — every handler fails closed (failed_precondition) when the
   *                master key is unavailable; SealStatus reports the seal state.
   * The sensitive reads (GetSecret, Decrypt) are audited via the outbox compliance
   * envelope. Dynamic database credentials are a declared follow-up.
   * </pre>
   */
  public static final class VaultServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<VaultServiceBlockingStub> {
    private VaultServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VaultServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new VaultServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Write a new secret version. Compare-and-swap: `expected_version` must equal
     * the current latest version (0 for a brand-new path) or the write is rejected.
     * </pre>
     */
    public com.udb.core.vault.services.v1.PutSecretResponse putSecret(com.udb.core.vault.services.v1.PutSecretRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPutSecretMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Read the secret value (latest active version, or a specific version). This
     * is the sensitive vault read: it is AUDITED via the outbox compliance envelope.
     * </pre>
     */
    public com.udb.core.vault.services.v1.GetSecretResponse getSecret(com.udb.core.vault.services.v1.GetSecretRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetSecretMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List secret paths under an optional prefix. Returns metadata only — NEVER
     * any secret value.
     * </pre>
     */
    public com.udb.core.vault.services.v1.ListSecretsResponse listSecrets(com.udb.core.vault.services.v1.ListSecretsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListSecretsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Soft-delete the latest version (recoverable bookkeeping state). The ciphertext
     * is retained; use DestroySecret to crypto-shred.
     * </pre>
     */
    public com.udb.core.vault.services.v1.DeleteSecretResponse deleteSecret(com.udb.core.vault.services.v1.DeleteSecretRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteSecretMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Restore a soft-DELETED secret: flip its latest deleted version back to ACTIVE.
     * A soft delete keeps the ciphertext + wrapped key, so recovery is exact. A
     * crypto-shredded (DestroySecret) version can NEVER be restored.
     * </pre>
     */
    public com.udb.core.vault.services.v1.UndeleteSecretResponse undeleteSecret(com.udb.core.vault.services.v1.UndeleteSecretRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUndeleteSecretMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Crypto-shred every version of a secret: clears the wrapped DEK + ciphertext
     * so the value is irrecoverable. DESTRUCTIVE + irreversible — a confirmation
     * token is required and an empty token fails closed.
     * </pre>
     */
    public com.udb.core.vault.services.v1.DestroySecretResponse destroySecret(com.udb.core.vault.services.v1.DestroySecretRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDestroySecretMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Create a named transit key (version 1, ACTIVE). Key material is generated
     * server-side and never returned.
     * </pre>
     */
    public com.udb.core.vault.services.v1.CreateTransitKeyResponse createTransitKey(com.udb.core.vault.services.v1.CreateTransitKeyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateTransitKeyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Rotate a named transit key: the current ACTIVE version is demoted to
     * VERIFYING (still decrypts/verifies during the overlap) and a fresh ACTIVE
     * version is generated. New encryptions/signatures use the new version.
     * </pre>
     */
    public com.udb.core.vault.services.v1.RotateTransitKeyResponse rotateTransitKey(com.udb.core.vault.services.v1.RotateTransitKeyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRotateTransitKeyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Encrypt plaintext under the ACTIVE version of a named key. Returns a
     * versioned ciphertext envelope; the key material is never returned.
     * </pre>
     */
    public com.udb.core.vault.services.v1.EncryptResponse encrypt(com.udb.core.vault.services.v1.EncryptRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEncryptMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Decrypt a transit ciphertext envelope. The version is read from the envelope
     * and ACTIVE or VERIFYING versions are accepted. This is a sensitive read and is
     * AUDITED via the outbox compliance envelope.
     * </pre>
     */
    public com.udb.core.vault.services.v1.DecryptResponse decrypt(com.udb.core.vault.services.v1.DecryptRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDecryptMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Produce a detached MAC ("signature") over the input under the ACTIVE key
     * version. Implemented as HMAC-SHA256 from the version DEK (symmetric);
     * asymmetric signing is a follow-up. Key material is never returned.
     * </pre>
     */
    public com.udb.core.vault.services.v1.SignResponse sign(com.udb.core.vault.services.v1.SignRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSignMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Verify a MAC/signature over the input. The version is read from the
     * signature and ACTIVE or VERIFYING versions are accepted; comparison is
     * constant-time.
     * </pre>
     */
    public com.udb.core.vault.services.v1.VerifyResponse verify(com.udb.core.vault.services.v1.VerifyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getVerifyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Compute an HMAC-SHA256 over the input under the ACTIVE key version. Key
     * material is never returned.
     * </pre>
     */
    public com.udb.core.vault.services.v1.HmacResponse hmac(com.udb.core.vault.services.v1.HmacRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHmacMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Report whether the vault is sealed (master key unavailable). Always answers,
     * even when sealed, so operators can diagnose a sealed vault.
     * </pre>
     */
    public com.udb.core.vault.services.v1.SealStatusResponse sealStatus(com.udb.core.vault.services.v1.SealStatusRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSealStatusMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Mint short-lived, per-request Postgres credentials with a durable lease.
     * The requested role_name is an operator-configured alias resolved from
     * UDB_VAULT_DB_ROLES_JSON; arbitrary request-supplied role grants fail closed.
     * WORKER_VAULT_LEASE_REAPER revokes and drops expired generated login roles.
     * </pre>
     */
    public com.udb.core.vault.services.v1.GenerateDatabaseCredentialsResponse generateDatabaseCredentials(com.udb.core.vault.services.v1.GenerateDatabaseCredentialsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGenerateDatabaseCredentialsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Revoke one lease in the authenticated tenant/project. The durable state is
     * moved to REVOKING before physical session fencing and becomes REVOKED only
     * after the generated role is proven absent. Replays are naturally idempotent.
     * </pre>
     */
    public com.udb.core.vault.services.v1.RevokeDatabaseCredentialsResponse revokeDatabaseCredentials(com.udb.core.vault.services.v1.RevokeDatabaseCredentialsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRevokeDatabaseCredentialsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Emergency kill-switch for every non-terminal lease in exactly one verified
     * tenant/project. A confirmation token bound to both scope dimensions prevents
     * an accidental tenant-wide or cross-project credential wipe.
     * </pre>
     */
    public com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsResponse emergencyRevokeDatabaseCredentials(com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEmergencyRevokeDatabaseCredentialsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Generate a fresh 256-bit data key, returned BOTH plaintext (for the caller to
     * encrypt data locally) AND wrapped under the named transit key (store this and
     * Decrypt/Rewrap it later). Envelope-encryption without exposing the transit
     * key. Reuses the transit seal path; AUDITED via the outbox compliance envelope.
     * </pre>
     */
    public com.udb.core.vault.services.v1.GenerateDataKeyResponse generateDataKey(com.udb.core.vault.services.v1.GenerateDataKeyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGenerateDataKeyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Re-wrap a transit ciphertext under the key's CURRENT active version: decrypt
     * with the version embedded in the envelope, then re-seal with the active
     * version. The post-rotation migration primitive (no plaintext leaves the
     * broker). AUDITED via the outbox compliance envelope.
     * </pre>
     */
    public com.udb.core.vault.services.v1.RewrapResponse rewrap(com.udb.core.vault.services.v1.RewrapRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRewrapMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Export the Ed25519 PUBLIC key(s) of a signing transit key so an external
     * party can verify broker-produced signatures without ever holding the private
     * key — the missing half that makes Sign/Verify genuinely asymmetric. Only
     * valid for keys created with the ed25519 algorithm; READ-ONLY (public keys are
     * not secret). Returns one entry per usable (ACTIVE/VERIFYING) version.
     * </pre>
     */
    public com.udb.core.vault.services.v1.GetTransitPublicKeyResponse getTransitPublicKey(com.udb.core.vault.services.v1.GetTransitPublicKeyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTransitPublicKeyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Encrypt MANY plaintexts under one transit key in a single call: the key is
     * unwrapped ONCE and each plaintext sealed with the active version, amortizing
     * the master-key unwrap over the batch. Order-preserving. AUDITED.
     * </pre>
     */
    public com.udb.core.vault.services.v1.BatchEncryptResponse batchEncrypt(com.udb.core.vault.services.v1.BatchEncryptRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getBatchEncryptMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Decrypt MANY transit ciphertexts under one key in a single call; each
     * ciphertext carries its own key version in the envelope. Order-preserving.
     * </pre>
     */
    public com.udb.core.vault.services.v1.BatchDecryptResponse batchDecrypt(com.udb.core.vault.services.v1.BatchDecryptRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getBatchDecryptMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service VaultService.
   * <pre>
   * VaultService (master-plan 9.1, flagship) — secrets management built into the
   * broker. Three engines, one crypto stack (the broker AES-256-GCM-SIV envelope,
   * reused from `runtime::encryption`):
   *   * KV       — versioned, envelope-encrypted secrets at hierarchical paths
   *                with compare-and-swap, soft delete, and crypto-shred destroy.
   *   * Transit  — encrypt/decrypt/sign/verify/hmac by key NAME; key material is
   *                never exported; versioned keys with ACTIVE/VERIFYING rotation.
   *   * Seal     — every handler fails closed (failed_precondition) when the
   *                master key is unavailable; SealStatus reports the seal state.
   * The sensitive reads (GetSecret, Decrypt) are audited via the outbox compliance
   * envelope. Dynamic database credentials are a declared follow-up.
   * </pre>
   */
  public static final class VaultServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<VaultServiceFutureStub> {
    private VaultServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VaultServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new VaultServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Write a new secret version. Compare-and-swap: `expected_version` must equal
     * the current latest version (0 for a brand-new path) or the write is rejected.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.PutSecretResponse> putSecret(
        com.udb.core.vault.services.v1.PutSecretRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPutSecretMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Read the secret value (latest active version, or a specific version). This
     * is the sensitive vault read: it is AUDITED via the outbox compliance envelope.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.GetSecretResponse> getSecret(
        com.udb.core.vault.services.v1.GetSecretRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetSecretMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List secret paths under an optional prefix. Returns metadata only — NEVER
     * any secret value.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.ListSecretsResponse> listSecrets(
        com.udb.core.vault.services.v1.ListSecretsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListSecretsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Soft-delete the latest version (recoverable bookkeeping state). The ciphertext
     * is retained; use DestroySecret to crypto-shred.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.DeleteSecretResponse> deleteSecret(
        com.udb.core.vault.services.v1.DeleteSecretRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteSecretMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Restore a soft-DELETED secret: flip its latest deleted version back to ACTIVE.
     * A soft delete keeps the ciphertext + wrapped key, so recovery is exact. A
     * crypto-shredded (DestroySecret) version can NEVER be restored.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.UndeleteSecretResponse> undeleteSecret(
        com.udb.core.vault.services.v1.UndeleteSecretRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUndeleteSecretMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Crypto-shred every version of a secret: clears the wrapped DEK + ciphertext
     * so the value is irrecoverable. DESTRUCTIVE + irreversible — a confirmation
     * token is required and an empty token fails closed.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.DestroySecretResponse> destroySecret(
        com.udb.core.vault.services.v1.DestroySecretRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDestroySecretMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Create a named transit key (version 1, ACTIVE). Key material is generated
     * server-side and never returned.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.CreateTransitKeyResponse> createTransitKey(
        com.udb.core.vault.services.v1.CreateTransitKeyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateTransitKeyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Rotate a named transit key: the current ACTIVE version is demoted to
     * VERIFYING (still decrypts/verifies during the overlap) and a fresh ACTIVE
     * version is generated. New encryptions/signatures use the new version.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.RotateTransitKeyResponse> rotateTransitKey(
        com.udb.core.vault.services.v1.RotateTransitKeyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRotateTransitKeyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Encrypt plaintext under the ACTIVE version of a named key. Returns a
     * versioned ciphertext envelope; the key material is never returned.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.EncryptResponse> encrypt(
        com.udb.core.vault.services.v1.EncryptRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEncryptMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Decrypt a transit ciphertext envelope. The version is read from the envelope
     * and ACTIVE or VERIFYING versions are accepted. This is a sensitive read and is
     * AUDITED via the outbox compliance envelope.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.DecryptResponse> decrypt(
        com.udb.core.vault.services.v1.DecryptRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDecryptMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Produce a detached MAC ("signature") over the input under the ACTIVE key
     * version. Implemented as HMAC-SHA256 from the version DEK (symmetric);
     * asymmetric signing is a follow-up. Key material is never returned.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.SignResponse> sign(
        com.udb.core.vault.services.v1.SignRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSignMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Verify a MAC/signature over the input. The version is read from the
     * signature and ACTIVE or VERIFYING versions are accepted; comparison is
     * constant-time.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.VerifyResponse> verify(
        com.udb.core.vault.services.v1.VerifyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getVerifyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Compute an HMAC-SHA256 over the input under the ACTIVE key version. Key
     * material is never returned.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.HmacResponse> hmac(
        com.udb.core.vault.services.v1.HmacRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHmacMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Report whether the vault is sealed (master key unavailable). Always answers,
     * even when sealed, so operators can diagnose a sealed vault.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.SealStatusResponse> sealStatus(
        com.udb.core.vault.services.v1.SealStatusRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSealStatusMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Mint short-lived, per-request Postgres credentials with a durable lease.
     * The requested role_name is an operator-configured alias resolved from
     * UDB_VAULT_DB_ROLES_JSON; arbitrary request-supplied role grants fail closed.
     * WORKER_VAULT_LEASE_REAPER revokes and drops expired generated login roles.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.GenerateDatabaseCredentialsResponse> generateDatabaseCredentials(
        com.udb.core.vault.services.v1.GenerateDatabaseCredentialsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGenerateDatabaseCredentialsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Revoke one lease in the authenticated tenant/project. The durable state is
     * moved to REVOKING before physical session fencing and becomes REVOKED only
     * after the generated role is proven absent. Replays are naturally idempotent.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.RevokeDatabaseCredentialsResponse> revokeDatabaseCredentials(
        com.udb.core.vault.services.v1.RevokeDatabaseCredentialsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRevokeDatabaseCredentialsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Emergency kill-switch for every non-terminal lease in exactly one verified
     * tenant/project. A confirmation token bound to both scope dimensions prevents
     * an accidental tenant-wide or cross-project credential wipe.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsResponse> emergencyRevokeDatabaseCredentials(
        com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEmergencyRevokeDatabaseCredentialsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Generate a fresh 256-bit data key, returned BOTH plaintext (for the caller to
     * encrypt data locally) AND wrapped under the named transit key (store this and
     * Decrypt/Rewrap it later). Envelope-encryption without exposing the transit
     * key. Reuses the transit seal path; AUDITED via the outbox compliance envelope.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.GenerateDataKeyResponse> generateDataKey(
        com.udb.core.vault.services.v1.GenerateDataKeyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGenerateDataKeyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Re-wrap a transit ciphertext under the key's CURRENT active version: decrypt
     * with the version embedded in the envelope, then re-seal with the active
     * version. The post-rotation migration primitive (no plaintext leaves the
     * broker). AUDITED via the outbox compliance envelope.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.RewrapResponse> rewrap(
        com.udb.core.vault.services.v1.RewrapRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRewrapMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Export the Ed25519 PUBLIC key(s) of a signing transit key so an external
     * party can verify broker-produced signatures without ever holding the private
     * key — the missing half that makes Sign/Verify genuinely asymmetric. Only
     * valid for keys created with the ed25519 algorithm; READ-ONLY (public keys are
     * not secret). Returns one entry per usable (ACTIVE/VERIFYING) version.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.GetTransitPublicKeyResponse> getTransitPublicKey(
        com.udb.core.vault.services.v1.GetTransitPublicKeyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTransitPublicKeyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Encrypt MANY plaintexts under one transit key in a single call: the key is
     * unwrapped ONCE and each plaintext sealed with the active version, amortizing
     * the master-key unwrap over the batch. Order-preserving. AUDITED.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.BatchEncryptResponse> batchEncrypt(
        com.udb.core.vault.services.v1.BatchEncryptRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getBatchEncryptMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Decrypt MANY transit ciphertexts under one key in a single call; each
     * ciphertext carries its own key version in the envelope. Order-preserving.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.vault.services.v1.BatchDecryptResponse> batchDecrypt(
        com.udb.core.vault.services.v1.BatchDecryptRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getBatchDecryptMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_PUT_SECRET = 0;
  private static final int METHODID_GET_SECRET = 1;
  private static final int METHODID_LIST_SECRETS = 2;
  private static final int METHODID_DELETE_SECRET = 3;
  private static final int METHODID_UNDELETE_SECRET = 4;
  private static final int METHODID_DESTROY_SECRET = 5;
  private static final int METHODID_CREATE_TRANSIT_KEY = 6;
  private static final int METHODID_ROTATE_TRANSIT_KEY = 7;
  private static final int METHODID_ENCRYPT = 8;
  private static final int METHODID_DECRYPT = 9;
  private static final int METHODID_SIGN = 10;
  private static final int METHODID_VERIFY = 11;
  private static final int METHODID_HMAC = 12;
  private static final int METHODID_SEAL_STATUS = 13;
  private static final int METHODID_GENERATE_DATABASE_CREDENTIALS = 14;
  private static final int METHODID_REVOKE_DATABASE_CREDENTIALS = 15;
  private static final int METHODID_EMERGENCY_REVOKE_DATABASE_CREDENTIALS = 16;
  private static final int METHODID_GENERATE_DATA_KEY = 17;
  private static final int METHODID_REWRAP = 18;
  private static final int METHODID_GET_TRANSIT_PUBLIC_KEY = 19;
  private static final int METHODID_BATCH_ENCRYPT = 20;
  private static final int METHODID_BATCH_DECRYPT = 21;

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
        case METHODID_PUT_SECRET:
          serviceImpl.putSecret((com.udb.core.vault.services.v1.PutSecretRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.PutSecretResponse>) responseObserver);
          break;
        case METHODID_GET_SECRET:
          serviceImpl.getSecret((com.udb.core.vault.services.v1.GetSecretRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.GetSecretResponse>) responseObserver);
          break;
        case METHODID_LIST_SECRETS:
          serviceImpl.listSecrets((com.udb.core.vault.services.v1.ListSecretsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.ListSecretsResponse>) responseObserver);
          break;
        case METHODID_DELETE_SECRET:
          serviceImpl.deleteSecret((com.udb.core.vault.services.v1.DeleteSecretRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.DeleteSecretResponse>) responseObserver);
          break;
        case METHODID_UNDELETE_SECRET:
          serviceImpl.undeleteSecret((com.udb.core.vault.services.v1.UndeleteSecretRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.UndeleteSecretResponse>) responseObserver);
          break;
        case METHODID_DESTROY_SECRET:
          serviceImpl.destroySecret((com.udb.core.vault.services.v1.DestroySecretRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.DestroySecretResponse>) responseObserver);
          break;
        case METHODID_CREATE_TRANSIT_KEY:
          serviceImpl.createTransitKey((com.udb.core.vault.services.v1.CreateTransitKeyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.CreateTransitKeyResponse>) responseObserver);
          break;
        case METHODID_ROTATE_TRANSIT_KEY:
          serviceImpl.rotateTransitKey((com.udb.core.vault.services.v1.RotateTransitKeyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.RotateTransitKeyResponse>) responseObserver);
          break;
        case METHODID_ENCRYPT:
          serviceImpl.encrypt((com.udb.core.vault.services.v1.EncryptRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.EncryptResponse>) responseObserver);
          break;
        case METHODID_DECRYPT:
          serviceImpl.decrypt((com.udb.core.vault.services.v1.DecryptRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.DecryptResponse>) responseObserver);
          break;
        case METHODID_SIGN:
          serviceImpl.sign((com.udb.core.vault.services.v1.SignRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.SignResponse>) responseObserver);
          break;
        case METHODID_VERIFY:
          serviceImpl.verify((com.udb.core.vault.services.v1.VerifyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.VerifyResponse>) responseObserver);
          break;
        case METHODID_HMAC:
          serviceImpl.hmac((com.udb.core.vault.services.v1.HmacRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.HmacResponse>) responseObserver);
          break;
        case METHODID_SEAL_STATUS:
          serviceImpl.sealStatus((com.udb.core.vault.services.v1.SealStatusRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.SealStatusResponse>) responseObserver);
          break;
        case METHODID_GENERATE_DATABASE_CREDENTIALS:
          serviceImpl.generateDatabaseCredentials((com.udb.core.vault.services.v1.GenerateDatabaseCredentialsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.GenerateDatabaseCredentialsResponse>) responseObserver);
          break;
        case METHODID_REVOKE_DATABASE_CREDENTIALS:
          serviceImpl.revokeDatabaseCredentials((com.udb.core.vault.services.v1.RevokeDatabaseCredentialsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.RevokeDatabaseCredentialsResponse>) responseObserver);
          break;
        case METHODID_EMERGENCY_REVOKE_DATABASE_CREDENTIALS:
          serviceImpl.emergencyRevokeDatabaseCredentials((com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsResponse>) responseObserver);
          break;
        case METHODID_GENERATE_DATA_KEY:
          serviceImpl.generateDataKey((com.udb.core.vault.services.v1.GenerateDataKeyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.GenerateDataKeyResponse>) responseObserver);
          break;
        case METHODID_REWRAP:
          serviceImpl.rewrap((com.udb.core.vault.services.v1.RewrapRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.RewrapResponse>) responseObserver);
          break;
        case METHODID_GET_TRANSIT_PUBLIC_KEY:
          serviceImpl.getTransitPublicKey((com.udb.core.vault.services.v1.GetTransitPublicKeyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.GetTransitPublicKeyResponse>) responseObserver);
          break;
        case METHODID_BATCH_ENCRYPT:
          serviceImpl.batchEncrypt((com.udb.core.vault.services.v1.BatchEncryptRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.BatchEncryptResponse>) responseObserver);
          break;
        case METHODID_BATCH_DECRYPT:
          serviceImpl.batchDecrypt((com.udb.core.vault.services.v1.BatchDecryptRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.vault.services.v1.BatchDecryptResponse>) responseObserver);
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
          getPutSecretMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.PutSecretRequest,
              com.udb.core.vault.services.v1.PutSecretResponse>(
                service, METHODID_PUT_SECRET)))
        .addMethod(
          getGetSecretMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.GetSecretRequest,
              com.udb.core.vault.services.v1.GetSecretResponse>(
                service, METHODID_GET_SECRET)))
        .addMethod(
          getListSecretsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.ListSecretsRequest,
              com.udb.core.vault.services.v1.ListSecretsResponse>(
                service, METHODID_LIST_SECRETS)))
        .addMethod(
          getDeleteSecretMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.DeleteSecretRequest,
              com.udb.core.vault.services.v1.DeleteSecretResponse>(
                service, METHODID_DELETE_SECRET)))
        .addMethod(
          getUndeleteSecretMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.UndeleteSecretRequest,
              com.udb.core.vault.services.v1.UndeleteSecretResponse>(
                service, METHODID_UNDELETE_SECRET)))
        .addMethod(
          getDestroySecretMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.DestroySecretRequest,
              com.udb.core.vault.services.v1.DestroySecretResponse>(
                service, METHODID_DESTROY_SECRET)))
        .addMethod(
          getCreateTransitKeyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.CreateTransitKeyRequest,
              com.udb.core.vault.services.v1.CreateTransitKeyResponse>(
                service, METHODID_CREATE_TRANSIT_KEY)))
        .addMethod(
          getRotateTransitKeyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.RotateTransitKeyRequest,
              com.udb.core.vault.services.v1.RotateTransitKeyResponse>(
                service, METHODID_ROTATE_TRANSIT_KEY)))
        .addMethod(
          getEncryptMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.EncryptRequest,
              com.udb.core.vault.services.v1.EncryptResponse>(
                service, METHODID_ENCRYPT)))
        .addMethod(
          getDecryptMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.DecryptRequest,
              com.udb.core.vault.services.v1.DecryptResponse>(
                service, METHODID_DECRYPT)))
        .addMethod(
          getSignMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.SignRequest,
              com.udb.core.vault.services.v1.SignResponse>(
                service, METHODID_SIGN)))
        .addMethod(
          getVerifyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.VerifyRequest,
              com.udb.core.vault.services.v1.VerifyResponse>(
                service, METHODID_VERIFY)))
        .addMethod(
          getHmacMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.HmacRequest,
              com.udb.core.vault.services.v1.HmacResponse>(
                service, METHODID_HMAC)))
        .addMethod(
          getSealStatusMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.SealStatusRequest,
              com.udb.core.vault.services.v1.SealStatusResponse>(
                service, METHODID_SEAL_STATUS)))
        .addMethod(
          getGenerateDatabaseCredentialsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.GenerateDatabaseCredentialsRequest,
              com.udb.core.vault.services.v1.GenerateDatabaseCredentialsResponse>(
                service, METHODID_GENERATE_DATABASE_CREDENTIALS)))
        .addMethod(
          getRevokeDatabaseCredentialsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.RevokeDatabaseCredentialsRequest,
              com.udb.core.vault.services.v1.RevokeDatabaseCredentialsResponse>(
                service, METHODID_REVOKE_DATABASE_CREDENTIALS)))
        .addMethod(
          getEmergencyRevokeDatabaseCredentialsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsRequest,
              com.udb.core.vault.services.v1.EmergencyRevokeDatabaseCredentialsResponse>(
                service, METHODID_EMERGENCY_REVOKE_DATABASE_CREDENTIALS)))
        .addMethod(
          getGenerateDataKeyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.GenerateDataKeyRequest,
              com.udb.core.vault.services.v1.GenerateDataKeyResponse>(
                service, METHODID_GENERATE_DATA_KEY)))
        .addMethod(
          getRewrapMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.RewrapRequest,
              com.udb.core.vault.services.v1.RewrapResponse>(
                service, METHODID_REWRAP)))
        .addMethod(
          getGetTransitPublicKeyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.GetTransitPublicKeyRequest,
              com.udb.core.vault.services.v1.GetTransitPublicKeyResponse>(
                service, METHODID_GET_TRANSIT_PUBLIC_KEY)))
        .addMethod(
          getBatchEncryptMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.BatchEncryptRequest,
              com.udb.core.vault.services.v1.BatchEncryptResponse>(
                service, METHODID_BATCH_ENCRYPT)))
        .addMethod(
          getBatchDecryptMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.vault.services.v1.BatchDecryptRequest,
              com.udb.core.vault.services.v1.BatchDecryptResponse>(
                service, METHODID_BATCH_DECRYPT)))
        .build();
  }

  private static abstract class VaultServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    VaultServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.vault.services.v1.VaultServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("VaultService");
    }
  }

  private static final class VaultServiceFileDescriptorSupplier
      extends VaultServiceBaseDescriptorSupplier {
    VaultServiceFileDescriptorSupplier() {}
  }

  private static final class VaultServiceMethodDescriptorSupplier
      extends VaultServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    VaultServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (VaultServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new VaultServiceFileDescriptorSupplier())
              .addMethod(getPutSecretMethod())
              .addMethod(getGetSecretMethod())
              .addMethod(getListSecretsMethod())
              .addMethod(getDeleteSecretMethod())
              .addMethod(getUndeleteSecretMethod())
              .addMethod(getDestroySecretMethod())
              .addMethod(getCreateTransitKeyMethod())
              .addMethod(getRotateTransitKeyMethod())
              .addMethod(getEncryptMethod())
              .addMethod(getDecryptMethod())
              .addMethod(getSignMethod())
              .addMethod(getVerifyMethod())
              .addMethod(getHmacMethod())
              .addMethod(getSealStatusMethod())
              .addMethod(getGenerateDatabaseCredentialsMethod())
              .addMethod(getRevokeDatabaseCredentialsMethod())
              .addMethod(getEmergencyRevokeDatabaseCredentialsMethod())
              .addMethod(getGenerateDataKeyMethod())
              .addMethod(getRewrapMethod())
              .addMethod(getGetTransitPublicKeyMethod())
              .addMethod(getBatchEncryptMethod())
              .addMethod(getBatchDecryptMethod())
              .build();
        }
      }
    }
    return result;
  }
}
