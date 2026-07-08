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
  }

  private static final int METHODID_PUT_SECRET = 0;
  private static final int METHODID_GET_SECRET = 1;
  private static final int METHODID_LIST_SECRETS = 2;
  private static final int METHODID_DELETE_SECRET = 3;
  private static final int METHODID_DESTROY_SECRET = 4;
  private static final int METHODID_CREATE_TRANSIT_KEY = 5;
  private static final int METHODID_ROTATE_TRANSIT_KEY = 6;
  private static final int METHODID_ENCRYPT = 7;
  private static final int METHODID_DECRYPT = 8;
  private static final int METHODID_SIGN = 9;
  private static final int METHODID_VERIFY = 10;
  private static final int METHODID_HMAC = 11;
  private static final int METHODID_SEAL_STATUS = 12;
  private static final int METHODID_GENERATE_DATABASE_CREDENTIALS = 13;

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
              .build();
        }
      }
    }
    return result;
  }
}
