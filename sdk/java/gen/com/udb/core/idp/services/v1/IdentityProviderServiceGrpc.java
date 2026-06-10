package com.udb.core.idp.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * ---------------------------------------------------------------------------
 * IdentityProviderService — Enterprise identity-provider lifecycle, SAML 2.0
 * web SSO (metadata import + ACS), SCIM 2.0 provisioning, JIT user
 * provisioning, and external-identity linking. All RPCs are tenant-scoped and
 * server-only (control-plane); they run on the isolated native auth listener.
 * ---------------------------------------------------------------------------
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class IdentityProviderServiceGrpc {

  private IdentityProviderServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.idp.services.v1.IdentityProviderService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.CreateProviderRequest,
      com.udb.core.idp.services.v1.CreateProviderResponse> getCreateProviderMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateProvider",
      requestType = com.udb.core.idp.services.v1.CreateProviderRequest.class,
      responseType = com.udb.core.idp.services.v1.CreateProviderResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.CreateProviderRequest,
      com.udb.core.idp.services.v1.CreateProviderResponse> getCreateProviderMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.CreateProviderRequest, com.udb.core.idp.services.v1.CreateProviderResponse> getCreateProviderMethod;
    if ((getCreateProviderMethod = IdentityProviderServiceGrpc.getCreateProviderMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getCreateProviderMethod = IdentityProviderServiceGrpc.getCreateProviderMethod) == null) {
          IdentityProviderServiceGrpc.getCreateProviderMethod = getCreateProviderMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.CreateProviderRequest, com.udb.core.idp.services.v1.CreateProviderResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateProvider"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.CreateProviderRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.CreateProviderResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("CreateProvider"))
              .build();
        }
      }
    }
    return getCreateProviderMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.UpdateProviderRequest,
      com.udb.core.idp.services.v1.UpdateProviderResponse> getUpdateProviderMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateProvider",
      requestType = com.udb.core.idp.services.v1.UpdateProviderRequest.class,
      responseType = com.udb.core.idp.services.v1.UpdateProviderResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.UpdateProviderRequest,
      com.udb.core.idp.services.v1.UpdateProviderResponse> getUpdateProviderMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.UpdateProviderRequest, com.udb.core.idp.services.v1.UpdateProviderResponse> getUpdateProviderMethod;
    if ((getUpdateProviderMethod = IdentityProviderServiceGrpc.getUpdateProviderMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getUpdateProviderMethod = IdentityProviderServiceGrpc.getUpdateProviderMethod) == null) {
          IdentityProviderServiceGrpc.getUpdateProviderMethod = getUpdateProviderMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.UpdateProviderRequest, com.udb.core.idp.services.v1.UpdateProviderResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateProvider"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.UpdateProviderRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.UpdateProviderResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("UpdateProvider"))
              .build();
        }
      }
    }
    return getUpdateProviderMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.DisableProviderRequest,
      com.udb.core.idp.services.v1.DisableProviderResponse> getDisableProviderMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DisableProvider",
      requestType = com.udb.core.idp.services.v1.DisableProviderRequest.class,
      responseType = com.udb.core.idp.services.v1.DisableProviderResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.DisableProviderRequest,
      com.udb.core.idp.services.v1.DisableProviderResponse> getDisableProviderMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.DisableProviderRequest, com.udb.core.idp.services.v1.DisableProviderResponse> getDisableProviderMethod;
    if ((getDisableProviderMethod = IdentityProviderServiceGrpc.getDisableProviderMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getDisableProviderMethod = IdentityProviderServiceGrpc.getDisableProviderMethod) == null) {
          IdentityProviderServiceGrpc.getDisableProviderMethod = getDisableProviderMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.DisableProviderRequest, com.udb.core.idp.services.v1.DisableProviderResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DisableProvider"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.DisableProviderRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.DisableProviderResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("DisableProvider"))
              .build();
        }
      }
    }
    return getDisableProviderMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.GetProviderRequest,
      com.udb.core.idp.services.v1.GetProviderResponse> getGetProviderMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetProvider",
      requestType = com.udb.core.idp.services.v1.GetProviderRequest.class,
      responseType = com.udb.core.idp.services.v1.GetProviderResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.GetProviderRequest,
      com.udb.core.idp.services.v1.GetProviderResponse> getGetProviderMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.GetProviderRequest, com.udb.core.idp.services.v1.GetProviderResponse> getGetProviderMethod;
    if ((getGetProviderMethod = IdentityProviderServiceGrpc.getGetProviderMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getGetProviderMethod = IdentityProviderServiceGrpc.getGetProviderMethod) == null) {
          IdentityProviderServiceGrpc.getGetProviderMethod = getGetProviderMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.GetProviderRequest, com.udb.core.idp.services.v1.GetProviderResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetProvider"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.GetProviderRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.GetProviderResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("GetProvider"))
              .build();
        }
      }
    }
    return getGetProviderMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ListProvidersRequest,
      com.udb.core.idp.services.v1.ListProvidersResponse> getListProvidersMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListProviders",
      requestType = com.udb.core.idp.services.v1.ListProvidersRequest.class,
      responseType = com.udb.core.idp.services.v1.ListProvidersResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ListProvidersRequest,
      com.udb.core.idp.services.v1.ListProvidersResponse> getListProvidersMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ListProvidersRequest, com.udb.core.idp.services.v1.ListProvidersResponse> getListProvidersMethod;
    if ((getListProvidersMethod = IdentityProviderServiceGrpc.getListProvidersMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getListProvidersMethod = IdentityProviderServiceGrpc.getListProvidersMethod) == null) {
          IdentityProviderServiceGrpc.getListProvidersMethod = getListProvidersMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.ListProvidersRequest, com.udb.core.idp.services.v1.ListProvidersResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListProviders"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ListProvidersRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ListProvidersResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("ListProviders"))
              .build();
        }
      }
    }
    return getListProvidersMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.TestProviderDiscoveryRequest,
      com.udb.core.idp.services.v1.TestProviderDiscoveryResponse> getTestProviderDiscoveryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TestProviderDiscovery",
      requestType = com.udb.core.idp.services.v1.TestProviderDiscoveryRequest.class,
      responseType = com.udb.core.idp.services.v1.TestProviderDiscoveryResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.TestProviderDiscoveryRequest,
      com.udb.core.idp.services.v1.TestProviderDiscoveryResponse> getTestProviderDiscoveryMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.TestProviderDiscoveryRequest, com.udb.core.idp.services.v1.TestProviderDiscoveryResponse> getTestProviderDiscoveryMethod;
    if ((getTestProviderDiscoveryMethod = IdentityProviderServiceGrpc.getTestProviderDiscoveryMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getTestProviderDiscoveryMethod = IdentityProviderServiceGrpc.getTestProviderDiscoveryMethod) == null) {
          IdentityProviderServiceGrpc.getTestProviderDiscoveryMethod = getTestProviderDiscoveryMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.TestProviderDiscoveryRequest, com.udb.core.idp.services.v1.TestProviderDiscoveryResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TestProviderDiscovery"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.TestProviderDiscoveryRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.TestProviderDiscoveryResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("TestProviderDiscovery"))
              .build();
        }
      }
    }
    return getTestProviderDiscoveryMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ForceJwksRefreshRequest,
      com.udb.core.idp.services.v1.ForceJwksRefreshResponse> getForceJwksRefreshMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ForceJwksRefresh",
      requestType = com.udb.core.idp.services.v1.ForceJwksRefreshRequest.class,
      responseType = com.udb.core.idp.services.v1.ForceJwksRefreshResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ForceJwksRefreshRequest,
      com.udb.core.idp.services.v1.ForceJwksRefreshResponse> getForceJwksRefreshMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ForceJwksRefreshRequest, com.udb.core.idp.services.v1.ForceJwksRefreshResponse> getForceJwksRefreshMethod;
    if ((getForceJwksRefreshMethod = IdentityProviderServiceGrpc.getForceJwksRefreshMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getForceJwksRefreshMethod = IdentityProviderServiceGrpc.getForceJwksRefreshMethod) == null) {
          IdentityProviderServiceGrpc.getForceJwksRefreshMethod = getForceJwksRefreshMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.ForceJwksRefreshRequest, com.udb.core.idp.services.v1.ForceJwksRefreshResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ForceJwksRefresh"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ForceJwksRefreshRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ForceJwksRefreshResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("ForceJwksRefresh"))
              .build();
        }
      }
    }
    return getForceJwksRefreshMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.PreviewClaimMappingRequest,
      com.udb.core.idp.services.v1.PreviewClaimMappingResponse> getPreviewClaimMappingMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PreviewClaimMapping",
      requestType = com.udb.core.idp.services.v1.PreviewClaimMappingRequest.class,
      responseType = com.udb.core.idp.services.v1.PreviewClaimMappingResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.PreviewClaimMappingRequest,
      com.udb.core.idp.services.v1.PreviewClaimMappingResponse> getPreviewClaimMappingMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.PreviewClaimMappingRequest, com.udb.core.idp.services.v1.PreviewClaimMappingResponse> getPreviewClaimMappingMethod;
    if ((getPreviewClaimMappingMethod = IdentityProviderServiceGrpc.getPreviewClaimMappingMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getPreviewClaimMappingMethod = IdentityProviderServiceGrpc.getPreviewClaimMappingMethod) == null) {
          IdentityProviderServiceGrpc.getPreviewClaimMappingMethod = getPreviewClaimMappingMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.PreviewClaimMappingRequest, com.udb.core.idp.services.v1.PreviewClaimMappingResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PreviewClaimMapping"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.PreviewClaimMappingRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.PreviewClaimMappingResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("PreviewClaimMapping"))
              .build();
        }
      }
    }
    return getPreviewClaimMappingMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.PreviewGroupMappingRequest,
      com.udb.core.idp.services.v1.PreviewGroupMappingResponse> getPreviewGroupMappingMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PreviewGroupMapping",
      requestType = com.udb.core.idp.services.v1.PreviewGroupMappingRequest.class,
      responseType = com.udb.core.idp.services.v1.PreviewGroupMappingResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.PreviewGroupMappingRequest,
      com.udb.core.idp.services.v1.PreviewGroupMappingResponse> getPreviewGroupMappingMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.PreviewGroupMappingRequest, com.udb.core.idp.services.v1.PreviewGroupMappingResponse> getPreviewGroupMappingMethod;
    if ((getPreviewGroupMappingMethod = IdentityProviderServiceGrpc.getPreviewGroupMappingMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getPreviewGroupMappingMethod = IdentityProviderServiceGrpc.getPreviewGroupMappingMethod) == null) {
          IdentityProviderServiceGrpc.getPreviewGroupMappingMethod = getPreviewGroupMappingMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.PreviewGroupMappingRequest, com.udb.core.idp.services.v1.PreviewGroupMappingResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PreviewGroupMapping"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.PreviewGroupMappingRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.PreviewGroupMappingResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("PreviewGroupMapping"))
              .build();
        }
      }
    }
    return getPreviewGroupMappingMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ListExternalIdentitiesRequest,
      com.udb.core.idp.services.v1.ListExternalIdentitiesResponse> getListExternalIdentitiesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListExternalIdentities",
      requestType = com.udb.core.idp.services.v1.ListExternalIdentitiesRequest.class,
      responseType = com.udb.core.idp.services.v1.ListExternalIdentitiesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ListExternalIdentitiesRequest,
      com.udb.core.idp.services.v1.ListExternalIdentitiesResponse> getListExternalIdentitiesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ListExternalIdentitiesRequest, com.udb.core.idp.services.v1.ListExternalIdentitiesResponse> getListExternalIdentitiesMethod;
    if ((getListExternalIdentitiesMethod = IdentityProviderServiceGrpc.getListExternalIdentitiesMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getListExternalIdentitiesMethod = IdentityProviderServiceGrpc.getListExternalIdentitiesMethod) == null) {
          IdentityProviderServiceGrpc.getListExternalIdentitiesMethod = getListExternalIdentitiesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.ListExternalIdentitiesRequest, com.udb.core.idp.services.v1.ListExternalIdentitiesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListExternalIdentities"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ListExternalIdentitiesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ListExternalIdentitiesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("ListExternalIdentities"))
              .build();
        }
      }
    }
    return getListExternalIdentitiesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.LinkIdentityRequest,
      com.udb.core.idp.services.v1.LinkIdentityResponse> getLinkIdentityMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "LinkIdentity",
      requestType = com.udb.core.idp.services.v1.LinkIdentityRequest.class,
      responseType = com.udb.core.idp.services.v1.LinkIdentityResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.LinkIdentityRequest,
      com.udb.core.idp.services.v1.LinkIdentityResponse> getLinkIdentityMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.LinkIdentityRequest, com.udb.core.idp.services.v1.LinkIdentityResponse> getLinkIdentityMethod;
    if ((getLinkIdentityMethod = IdentityProviderServiceGrpc.getLinkIdentityMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getLinkIdentityMethod = IdentityProviderServiceGrpc.getLinkIdentityMethod) == null) {
          IdentityProviderServiceGrpc.getLinkIdentityMethod = getLinkIdentityMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.LinkIdentityRequest, com.udb.core.idp.services.v1.LinkIdentityResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "LinkIdentity"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.LinkIdentityRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.LinkIdentityResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("LinkIdentity"))
              .build();
        }
      }
    }
    return getLinkIdentityMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.UnlinkIdentityRequest,
      com.udb.core.idp.services.v1.UnlinkIdentityResponse> getUnlinkIdentityMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UnlinkIdentity",
      requestType = com.udb.core.idp.services.v1.UnlinkIdentityRequest.class,
      responseType = com.udb.core.idp.services.v1.UnlinkIdentityResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.UnlinkIdentityRequest,
      com.udb.core.idp.services.v1.UnlinkIdentityResponse> getUnlinkIdentityMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.UnlinkIdentityRequest, com.udb.core.idp.services.v1.UnlinkIdentityResponse> getUnlinkIdentityMethod;
    if ((getUnlinkIdentityMethod = IdentityProviderServiceGrpc.getUnlinkIdentityMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getUnlinkIdentityMethod = IdentityProviderServiceGrpc.getUnlinkIdentityMethod) == null) {
          IdentityProviderServiceGrpc.getUnlinkIdentityMethod = getUnlinkIdentityMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.UnlinkIdentityRequest, com.udb.core.idp.services.v1.UnlinkIdentityResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UnlinkIdentity"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.UnlinkIdentityRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.UnlinkIdentityResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("UnlinkIdentity"))
              .build();
        }
      }
    }
    return getUnlinkIdentityMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ImportSamlMetadataRequest,
      com.udb.core.idp.services.v1.ImportSamlMetadataResponse> getImportSamlMetadataMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ImportSamlMetadata",
      requestType = com.udb.core.idp.services.v1.ImportSamlMetadataRequest.class,
      responseType = com.udb.core.idp.services.v1.ImportSamlMetadataResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ImportSamlMetadataRequest,
      com.udb.core.idp.services.v1.ImportSamlMetadataResponse> getImportSamlMetadataMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ImportSamlMetadataRequest, com.udb.core.idp.services.v1.ImportSamlMetadataResponse> getImportSamlMetadataMethod;
    if ((getImportSamlMetadataMethod = IdentityProviderServiceGrpc.getImportSamlMetadataMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getImportSamlMetadataMethod = IdentityProviderServiceGrpc.getImportSamlMetadataMethod) == null) {
          IdentityProviderServiceGrpc.getImportSamlMetadataMethod = getImportSamlMetadataMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.ImportSamlMetadataRequest, com.udb.core.idp.services.v1.ImportSamlMetadataResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ImportSamlMetadata"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ImportSamlMetadataRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ImportSamlMetadataResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("ImportSamlMetadata"))
              .build();
        }
      }
    }
    return getImportSamlMetadataMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.StartSamlLoginRequest,
      com.udb.core.idp.services.v1.StartSamlLoginResponse> getStartSamlLoginMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StartSamlLogin",
      requestType = com.udb.core.idp.services.v1.StartSamlLoginRequest.class,
      responseType = com.udb.core.idp.services.v1.StartSamlLoginResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.StartSamlLoginRequest,
      com.udb.core.idp.services.v1.StartSamlLoginResponse> getStartSamlLoginMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.StartSamlLoginRequest, com.udb.core.idp.services.v1.StartSamlLoginResponse> getStartSamlLoginMethod;
    if ((getStartSamlLoginMethod = IdentityProviderServiceGrpc.getStartSamlLoginMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getStartSamlLoginMethod = IdentityProviderServiceGrpc.getStartSamlLoginMethod) == null) {
          IdentityProviderServiceGrpc.getStartSamlLoginMethod = getStartSamlLoginMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.StartSamlLoginRequest, com.udb.core.idp.services.v1.StartSamlLoginResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StartSamlLogin"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.StartSamlLoginRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.StartSamlLoginResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("StartSamlLogin"))
              .build();
        }
      }
    }
    return getStartSamlLoginMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.SamlAcsRequest,
      com.udb.core.idp.services.v1.SamlAcsResponse> getSamlAcsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SamlAcs",
      requestType = com.udb.core.idp.services.v1.SamlAcsRequest.class,
      responseType = com.udb.core.idp.services.v1.SamlAcsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.SamlAcsRequest,
      com.udb.core.idp.services.v1.SamlAcsResponse> getSamlAcsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.SamlAcsRequest, com.udb.core.idp.services.v1.SamlAcsResponse> getSamlAcsMethod;
    if ((getSamlAcsMethod = IdentityProviderServiceGrpc.getSamlAcsMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getSamlAcsMethod = IdentityProviderServiceGrpc.getSamlAcsMethod) == null) {
          IdentityProviderServiceGrpc.getSamlAcsMethod = getSamlAcsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.SamlAcsRequest, com.udb.core.idp.services.v1.SamlAcsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SamlAcs"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.SamlAcsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.SamlAcsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("SamlAcs"))
              .build();
        }
      }
    }
    return getSamlAcsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ResolveExternalIdentityRequest,
      com.udb.core.idp.services.v1.ResolveExternalIdentityResponse> getResolveExternalIdentityMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ResolveExternalIdentity",
      requestType = com.udb.core.idp.services.v1.ResolveExternalIdentityRequest.class,
      responseType = com.udb.core.idp.services.v1.ResolveExternalIdentityResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ResolveExternalIdentityRequest,
      com.udb.core.idp.services.v1.ResolveExternalIdentityResponse> getResolveExternalIdentityMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ResolveExternalIdentityRequest, com.udb.core.idp.services.v1.ResolveExternalIdentityResponse> getResolveExternalIdentityMethod;
    if ((getResolveExternalIdentityMethod = IdentityProviderServiceGrpc.getResolveExternalIdentityMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getResolveExternalIdentityMethod = IdentityProviderServiceGrpc.getResolveExternalIdentityMethod) == null) {
          IdentityProviderServiceGrpc.getResolveExternalIdentityMethod = getResolveExternalIdentityMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.ResolveExternalIdentityRequest, com.udb.core.idp.services.v1.ResolveExternalIdentityResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ResolveExternalIdentity"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ResolveExternalIdentityRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ResolveExternalIdentityResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("ResolveExternalIdentity"))
              .build();
        }
      }
    }
    return getResolveExternalIdentityMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimCreateUserRequest,
      com.udb.core.idp.services.v1.ScimCreateUserResponse> getScimCreateUserMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScimCreateUser",
      requestType = com.udb.core.idp.services.v1.ScimCreateUserRequest.class,
      responseType = com.udb.core.idp.services.v1.ScimCreateUserResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimCreateUserRequest,
      com.udb.core.idp.services.v1.ScimCreateUserResponse> getScimCreateUserMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimCreateUserRequest, com.udb.core.idp.services.v1.ScimCreateUserResponse> getScimCreateUserMethod;
    if ((getScimCreateUserMethod = IdentityProviderServiceGrpc.getScimCreateUserMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getScimCreateUserMethod = IdentityProviderServiceGrpc.getScimCreateUserMethod) == null) {
          IdentityProviderServiceGrpc.getScimCreateUserMethod = getScimCreateUserMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.ScimCreateUserRequest, com.udb.core.idp.services.v1.ScimCreateUserResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScimCreateUser"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimCreateUserRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimCreateUserResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("ScimCreateUser"))
              .build();
        }
      }
    }
    return getScimCreateUserMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimGetUserRequest,
      com.udb.core.idp.services.v1.ScimGetUserResponse> getScimGetUserMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScimGetUser",
      requestType = com.udb.core.idp.services.v1.ScimGetUserRequest.class,
      responseType = com.udb.core.idp.services.v1.ScimGetUserResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimGetUserRequest,
      com.udb.core.idp.services.v1.ScimGetUserResponse> getScimGetUserMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimGetUserRequest, com.udb.core.idp.services.v1.ScimGetUserResponse> getScimGetUserMethod;
    if ((getScimGetUserMethod = IdentityProviderServiceGrpc.getScimGetUserMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getScimGetUserMethod = IdentityProviderServiceGrpc.getScimGetUserMethod) == null) {
          IdentityProviderServiceGrpc.getScimGetUserMethod = getScimGetUserMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.ScimGetUserRequest, com.udb.core.idp.services.v1.ScimGetUserResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScimGetUser"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimGetUserRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimGetUserResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("ScimGetUser"))
              .build();
        }
      }
    }
    return getScimGetUserMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimListUsersRequest,
      com.udb.core.idp.services.v1.ScimListUsersResponse> getScimListUsersMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScimListUsers",
      requestType = com.udb.core.idp.services.v1.ScimListUsersRequest.class,
      responseType = com.udb.core.idp.services.v1.ScimListUsersResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimListUsersRequest,
      com.udb.core.idp.services.v1.ScimListUsersResponse> getScimListUsersMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimListUsersRequest, com.udb.core.idp.services.v1.ScimListUsersResponse> getScimListUsersMethod;
    if ((getScimListUsersMethod = IdentityProviderServiceGrpc.getScimListUsersMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getScimListUsersMethod = IdentityProviderServiceGrpc.getScimListUsersMethod) == null) {
          IdentityProviderServiceGrpc.getScimListUsersMethod = getScimListUsersMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.ScimListUsersRequest, com.udb.core.idp.services.v1.ScimListUsersResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScimListUsers"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimListUsersRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimListUsersResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("ScimListUsers"))
              .build();
        }
      }
    }
    return getScimListUsersMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimReplaceUserRequest,
      com.udb.core.idp.services.v1.ScimReplaceUserResponse> getScimReplaceUserMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScimReplaceUser",
      requestType = com.udb.core.idp.services.v1.ScimReplaceUserRequest.class,
      responseType = com.udb.core.idp.services.v1.ScimReplaceUserResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimReplaceUserRequest,
      com.udb.core.idp.services.v1.ScimReplaceUserResponse> getScimReplaceUserMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimReplaceUserRequest, com.udb.core.idp.services.v1.ScimReplaceUserResponse> getScimReplaceUserMethod;
    if ((getScimReplaceUserMethod = IdentityProviderServiceGrpc.getScimReplaceUserMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getScimReplaceUserMethod = IdentityProviderServiceGrpc.getScimReplaceUserMethod) == null) {
          IdentityProviderServiceGrpc.getScimReplaceUserMethod = getScimReplaceUserMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.ScimReplaceUserRequest, com.udb.core.idp.services.v1.ScimReplaceUserResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScimReplaceUser"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimReplaceUserRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimReplaceUserResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("ScimReplaceUser"))
              .build();
        }
      }
    }
    return getScimReplaceUserMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimPatchUserRequest,
      com.udb.core.idp.services.v1.ScimPatchUserResponse> getScimPatchUserMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScimPatchUser",
      requestType = com.udb.core.idp.services.v1.ScimPatchUserRequest.class,
      responseType = com.udb.core.idp.services.v1.ScimPatchUserResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimPatchUserRequest,
      com.udb.core.idp.services.v1.ScimPatchUserResponse> getScimPatchUserMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimPatchUserRequest, com.udb.core.idp.services.v1.ScimPatchUserResponse> getScimPatchUserMethod;
    if ((getScimPatchUserMethod = IdentityProviderServiceGrpc.getScimPatchUserMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getScimPatchUserMethod = IdentityProviderServiceGrpc.getScimPatchUserMethod) == null) {
          IdentityProviderServiceGrpc.getScimPatchUserMethod = getScimPatchUserMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.ScimPatchUserRequest, com.udb.core.idp.services.v1.ScimPatchUserResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScimPatchUser"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimPatchUserRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimPatchUserResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("ScimPatchUser"))
              .build();
        }
      }
    }
    return getScimPatchUserMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimDeleteUserRequest,
      com.udb.core.idp.services.v1.ScimDeleteUserResponse> getScimDeleteUserMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScimDeleteUser",
      requestType = com.udb.core.idp.services.v1.ScimDeleteUserRequest.class,
      responseType = com.udb.core.idp.services.v1.ScimDeleteUserResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimDeleteUserRequest,
      com.udb.core.idp.services.v1.ScimDeleteUserResponse> getScimDeleteUserMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimDeleteUserRequest, com.udb.core.idp.services.v1.ScimDeleteUserResponse> getScimDeleteUserMethod;
    if ((getScimDeleteUserMethod = IdentityProviderServiceGrpc.getScimDeleteUserMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getScimDeleteUserMethod = IdentityProviderServiceGrpc.getScimDeleteUserMethod) == null) {
          IdentityProviderServiceGrpc.getScimDeleteUserMethod = getScimDeleteUserMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.ScimDeleteUserRequest, com.udb.core.idp.services.v1.ScimDeleteUserResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScimDeleteUser"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimDeleteUserRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimDeleteUserResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("ScimDeleteUser"))
              .build();
        }
      }
    }
    return getScimDeleteUserMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimCreateGroupRequest,
      com.udb.core.idp.services.v1.ScimCreateGroupResponse> getScimCreateGroupMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScimCreateGroup",
      requestType = com.udb.core.idp.services.v1.ScimCreateGroupRequest.class,
      responseType = com.udb.core.idp.services.v1.ScimCreateGroupResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimCreateGroupRequest,
      com.udb.core.idp.services.v1.ScimCreateGroupResponse> getScimCreateGroupMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimCreateGroupRequest, com.udb.core.idp.services.v1.ScimCreateGroupResponse> getScimCreateGroupMethod;
    if ((getScimCreateGroupMethod = IdentityProviderServiceGrpc.getScimCreateGroupMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getScimCreateGroupMethod = IdentityProviderServiceGrpc.getScimCreateGroupMethod) == null) {
          IdentityProviderServiceGrpc.getScimCreateGroupMethod = getScimCreateGroupMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.ScimCreateGroupRequest, com.udb.core.idp.services.v1.ScimCreateGroupResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScimCreateGroup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimCreateGroupRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimCreateGroupResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("ScimCreateGroup"))
              .build();
        }
      }
    }
    return getScimCreateGroupMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimGetGroupRequest,
      com.udb.core.idp.services.v1.ScimGetGroupResponse> getScimGetGroupMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScimGetGroup",
      requestType = com.udb.core.idp.services.v1.ScimGetGroupRequest.class,
      responseType = com.udb.core.idp.services.v1.ScimGetGroupResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimGetGroupRequest,
      com.udb.core.idp.services.v1.ScimGetGroupResponse> getScimGetGroupMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimGetGroupRequest, com.udb.core.idp.services.v1.ScimGetGroupResponse> getScimGetGroupMethod;
    if ((getScimGetGroupMethod = IdentityProviderServiceGrpc.getScimGetGroupMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getScimGetGroupMethod = IdentityProviderServiceGrpc.getScimGetGroupMethod) == null) {
          IdentityProviderServiceGrpc.getScimGetGroupMethod = getScimGetGroupMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.ScimGetGroupRequest, com.udb.core.idp.services.v1.ScimGetGroupResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScimGetGroup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimGetGroupRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimGetGroupResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("ScimGetGroup"))
              .build();
        }
      }
    }
    return getScimGetGroupMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimListGroupsRequest,
      com.udb.core.idp.services.v1.ScimListGroupsResponse> getScimListGroupsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScimListGroups",
      requestType = com.udb.core.idp.services.v1.ScimListGroupsRequest.class,
      responseType = com.udb.core.idp.services.v1.ScimListGroupsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimListGroupsRequest,
      com.udb.core.idp.services.v1.ScimListGroupsResponse> getScimListGroupsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimListGroupsRequest, com.udb.core.idp.services.v1.ScimListGroupsResponse> getScimListGroupsMethod;
    if ((getScimListGroupsMethod = IdentityProviderServiceGrpc.getScimListGroupsMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getScimListGroupsMethod = IdentityProviderServiceGrpc.getScimListGroupsMethod) == null) {
          IdentityProviderServiceGrpc.getScimListGroupsMethod = getScimListGroupsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.ScimListGroupsRequest, com.udb.core.idp.services.v1.ScimListGroupsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScimListGroups"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimListGroupsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimListGroupsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("ScimListGroups"))
              .build();
        }
      }
    }
    return getScimListGroupsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimPatchGroupRequest,
      com.udb.core.idp.services.v1.ScimPatchGroupResponse> getScimPatchGroupMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScimPatchGroup",
      requestType = com.udb.core.idp.services.v1.ScimPatchGroupRequest.class,
      responseType = com.udb.core.idp.services.v1.ScimPatchGroupResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimPatchGroupRequest,
      com.udb.core.idp.services.v1.ScimPatchGroupResponse> getScimPatchGroupMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimPatchGroupRequest, com.udb.core.idp.services.v1.ScimPatchGroupResponse> getScimPatchGroupMethod;
    if ((getScimPatchGroupMethod = IdentityProviderServiceGrpc.getScimPatchGroupMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getScimPatchGroupMethod = IdentityProviderServiceGrpc.getScimPatchGroupMethod) == null) {
          IdentityProviderServiceGrpc.getScimPatchGroupMethod = getScimPatchGroupMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.ScimPatchGroupRequest, com.udb.core.idp.services.v1.ScimPatchGroupResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScimPatchGroup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimPatchGroupRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimPatchGroupResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("ScimPatchGroup"))
              .build();
        }
      }
    }
    return getScimPatchGroupMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimDeleteGroupRequest,
      com.udb.core.idp.services.v1.ScimDeleteGroupResponse> getScimDeleteGroupMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScimDeleteGroup",
      requestType = com.udb.core.idp.services.v1.ScimDeleteGroupRequest.class,
      responseType = com.udb.core.idp.services.v1.ScimDeleteGroupResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimDeleteGroupRequest,
      com.udb.core.idp.services.v1.ScimDeleteGroupResponse> getScimDeleteGroupMethod() {
    io.grpc.MethodDescriptor<com.udb.core.idp.services.v1.ScimDeleteGroupRequest, com.udb.core.idp.services.v1.ScimDeleteGroupResponse> getScimDeleteGroupMethod;
    if ((getScimDeleteGroupMethod = IdentityProviderServiceGrpc.getScimDeleteGroupMethod) == null) {
      synchronized (IdentityProviderServiceGrpc.class) {
        if ((getScimDeleteGroupMethod = IdentityProviderServiceGrpc.getScimDeleteGroupMethod) == null) {
          IdentityProviderServiceGrpc.getScimDeleteGroupMethod = getScimDeleteGroupMethod =
              io.grpc.MethodDescriptor.<com.udb.core.idp.services.v1.ScimDeleteGroupRequest, com.udb.core.idp.services.v1.ScimDeleteGroupResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScimDeleteGroup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimDeleteGroupRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.idp.services.v1.ScimDeleteGroupResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityProviderServiceMethodDescriptorSupplier("ScimDeleteGroup"))
              .build();
        }
      }
    }
    return getScimDeleteGroupMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static IdentityProviderServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<IdentityProviderServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<IdentityProviderServiceStub>() {
        @java.lang.Override
        public IdentityProviderServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new IdentityProviderServiceStub(channel, callOptions);
        }
      };
    return IdentityProviderServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static IdentityProviderServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<IdentityProviderServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<IdentityProviderServiceBlockingV2Stub>() {
        @java.lang.Override
        public IdentityProviderServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new IdentityProviderServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return IdentityProviderServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static IdentityProviderServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<IdentityProviderServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<IdentityProviderServiceBlockingStub>() {
        @java.lang.Override
        public IdentityProviderServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new IdentityProviderServiceBlockingStub(channel, callOptions);
        }
      };
    return IdentityProviderServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static IdentityProviderServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<IdentityProviderServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<IdentityProviderServiceFutureStub>() {
        @java.lang.Override
        public IdentityProviderServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new IdentityProviderServiceFutureStub(channel, callOptions);
        }
      };
    return IdentityProviderServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * ---------------------------------------------------------------------------
   * IdentityProviderService — Enterprise identity-provider lifecycle, SAML 2.0
   * web SSO (metadata import + ACS), SCIM 2.0 provisioning, JIT user
   * provisioning, and external-identity linking. All RPCs are tenant-scoped and
   * server-only (control-plane); they run on the isolated native auth listener.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * ── Provider administration (J2.6) ────────────────────────────────────────
     * </pre>
     */
    default void createProvider(com.udb.core.idp.services.v1.CreateProviderRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.CreateProviderResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateProviderMethod(), responseObserver);
    }

    /**
     */
    default void updateProvider(com.udb.core.idp.services.v1.UpdateProviderRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.UpdateProviderResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateProviderMethod(), responseObserver);
    }

    /**
     */
    default void disableProvider(com.udb.core.idp.services.v1.DisableProviderRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.DisableProviderResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDisableProviderMethod(), responseObserver);
    }

    /**
     */
    default void getProvider(com.udb.core.idp.services.v1.GetProviderRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.GetProviderResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetProviderMethod(), responseObserver);
    }

    /**
     */
    default void listProviders(com.udb.core.idp.services.v1.ListProvidersRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ListProvidersResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListProvidersMethod(), responseObserver);
    }

    /**
     */
    default void testProviderDiscovery(com.udb.core.idp.services.v1.TestProviderDiscoveryRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.TestProviderDiscoveryResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTestProviderDiscoveryMethod(), responseObserver);
    }

    /**
     */
    default void forceJwksRefresh(com.udb.core.idp.services.v1.ForceJwksRefreshRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ForceJwksRefreshResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getForceJwksRefreshMethod(), responseObserver);
    }

    /**
     */
    default void previewClaimMapping(com.udb.core.idp.services.v1.PreviewClaimMappingRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.PreviewClaimMappingResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPreviewClaimMappingMethod(), responseObserver);
    }

    /**
     */
    default void previewGroupMapping(com.udb.core.idp.services.v1.PreviewGroupMappingRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.PreviewGroupMappingResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPreviewGroupMappingMethod(), responseObserver);
    }

    /**
     */
    default void listExternalIdentities(com.udb.core.idp.services.v1.ListExternalIdentitiesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ListExternalIdentitiesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListExternalIdentitiesMethod(), responseObserver);
    }

    /**
     */
    default void linkIdentity(com.udb.core.idp.services.v1.LinkIdentityRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.LinkIdentityResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getLinkIdentityMethod(), responseObserver);
    }

    /**
     */
    default void unlinkIdentity(com.udb.core.idp.services.v1.UnlinkIdentityRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.UnlinkIdentityResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUnlinkIdentityMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── SAML 2.0 (J2.2) ───────────────────────────────────────────────────────
     * </pre>
     */
    default void importSamlMetadata(com.udb.core.idp.services.v1.ImportSamlMetadataRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ImportSamlMetadataResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getImportSamlMetadataMethod(), responseObserver);
    }

    /**
     */
    default void startSamlLogin(com.udb.core.idp.services.v1.StartSamlLoginRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.StartSamlLoginResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStartSamlLoginMethod(), responseObserver);
    }

    /**
     */
    default void samlAcs(com.udb.core.idp.services.v1.SamlAcsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.SamlAcsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSamlAcsMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── JIT provisioning + assurance (J2.4 / J2.5) ────────────────────────────
     * </pre>
     */
    default void resolveExternalIdentity(com.udb.core.idp.services.v1.ResolveExternalIdentityRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ResolveExternalIdentityResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getResolveExternalIdentityMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── SCIM 2.0 (J2.3) ───────────────────────────────────────────────────────
     * </pre>
     */
    default void scimCreateUser(com.udb.core.idp.services.v1.ScimCreateUserRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimCreateUserResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScimCreateUserMethod(), responseObserver);
    }

    /**
     */
    default void scimGetUser(com.udb.core.idp.services.v1.ScimGetUserRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimGetUserResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScimGetUserMethod(), responseObserver);
    }

    /**
     */
    default void scimListUsers(com.udb.core.idp.services.v1.ScimListUsersRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimListUsersResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScimListUsersMethod(), responseObserver);
    }

    /**
     */
    default void scimReplaceUser(com.udb.core.idp.services.v1.ScimReplaceUserRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimReplaceUserResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScimReplaceUserMethod(), responseObserver);
    }

    /**
     */
    default void scimPatchUser(com.udb.core.idp.services.v1.ScimPatchUserRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimPatchUserResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScimPatchUserMethod(), responseObserver);
    }

    /**
     */
    default void scimDeleteUser(com.udb.core.idp.services.v1.ScimDeleteUserRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimDeleteUserResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScimDeleteUserMethod(), responseObserver);
    }

    /**
     */
    default void scimCreateGroup(com.udb.core.idp.services.v1.ScimCreateGroupRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimCreateGroupResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScimCreateGroupMethod(), responseObserver);
    }

    /**
     */
    default void scimGetGroup(com.udb.core.idp.services.v1.ScimGetGroupRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimGetGroupResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScimGetGroupMethod(), responseObserver);
    }

    /**
     */
    default void scimListGroups(com.udb.core.idp.services.v1.ScimListGroupsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimListGroupsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScimListGroupsMethod(), responseObserver);
    }

    /**
     */
    default void scimPatchGroup(com.udb.core.idp.services.v1.ScimPatchGroupRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimPatchGroupResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScimPatchGroupMethod(), responseObserver);
    }

    /**
     */
    default void scimDeleteGroup(com.udb.core.idp.services.v1.ScimDeleteGroupRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimDeleteGroupResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScimDeleteGroupMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service IdentityProviderService.
   * <pre>
   * ---------------------------------------------------------------------------
   * IdentityProviderService — Enterprise identity-provider lifecycle, SAML 2.0
   * web SSO (metadata import + ACS), SCIM 2.0 provisioning, JIT user
   * provisioning, and external-identity linking. All RPCs are tenant-scoped and
   * server-only (control-plane); they run on the isolated native auth listener.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static abstract class IdentityProviderServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return IdentityProviderServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service IdentityProviderService.
   * <pre>
   * ---------------------------------------------------------------------------
   * IdentityProviderService — Enterprise identity-provider lifecycle, SAML 2.0
   * web SSO (metadata import + ACS), SCIM 2.0 provisioning, JIT user
   * provisioning, and external-identity linking. All RPCs are tenant-scoped and
   * server-only (control-plane); they run on the isolated native auth listener.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static final class IdentityProviderServiceStub
      extends io.grpc.stub.AbstractAsyncStub<IdentityProviderServiceStub> {
    private IdentityProviderServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected IdentityProviderServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new IdentityProviderServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * ── Provider administration (J2.6) ────────────────────────────────────────
     * </pre>
     */
    public void createProvider(com.udb.core.idp.services.v1.CreateProviderRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.CreateProviderResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateProviderMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void updateProvider(com.udb.core.idp.services.v1.UpdateProviderRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.UpdateProviderResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateProviderMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void disableProvider(com.udb.core.idp.services.v1.DisableProviderRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.DisableProviderResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDisableProviderMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getProvider(com.udb.core.idp.services.v1.GetProviderRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.GetProviderResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetProviderMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listProviders(com.udb.core.idp.services.v1.ListProvidersRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ListProvidersResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListProvidersMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void testProviderDiscovery(com.udb.core.idp.services.v1.TestProviderDiscoveryRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.TestProviderDiscoveryResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTestProviderDiscoveryMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void forceJwksRefresh(com.udb.core.idp.services.v1.ForceJwksRefreshRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ForceJwksRefreshResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getForceJwksRefreshMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void previewClaimMapping(com.udb.core.idp.services.v1.PreviewClaimMappingRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.PreviewClaimMappingResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPreviewClaimMappingMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void previewGroupMapping(com.udb.core.idp.services.v1.PreviewGroupMappingRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.PreviewGroupMappingResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPreviewGroupMappingMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listExternalIdentities(com.udb.core.idp.services.v1.ListExternalIdentitiesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ListExternalIdentitiesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListExternalIdentitiesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void linkIdentity(com.udb.core.idp.services.v1.LinkIdentityRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.LinkIdentityResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getLinkIdentityMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void unlinkIdentity(com.udb.core.idp.services.v1.UnlinkIdentityRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.UnlinkIdentityResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUnlinkIdentityMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── SAML 2.0 (J2.2) ───────────────────────────────────────────────────────
     * </pre>
     */
    public void importSamlMetadata(com.udb.core.idp.services.v1.ImportSamlMetadataRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ImportSamlMetadataResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getImportSamlMetadataMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void startSamlLogin(com.udb.core.idp.services.v1.StartSamlLoginRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.StartSamlLoginResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStartSamlLoginMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void samlAcs(com.udb.core.idp.services.v1.SamlAcsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.SamlAcsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSamlAcsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── JIT provisioning + assurance (J2.4 / J2.5) ────────────────────────────
     * </pre>
     */
    public void resolveExternalIdentity(com.udb.core.idp.services.v1.ResolveExternalIdentityRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ResolveExternalIdentityResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getResolveExternalIdentityMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── SCIM 2.0 (J2.3) ───────────────────────────────────────────────────────
     * </pre>
     */
    public void scimCreateUser(com.udb.core.idp.services.v1.ScimCreateUserRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimCreateUserResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScimCreateUserMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scimGetUser(com.udb.core.idp.services.v1.ScimGetUserRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimGetUserResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScimGetUserMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scimListUsers(com.udb.core.idp.services.v1.ScimListUsersRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimListUsersResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScimListUsersMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scimReplaceUser(com.udb.core.idp.services.v1.ScimReplaceUserRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimReplaceUserResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScimReplaceUserMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scimPatchUser(com.udb.core.idp.services.v1.ScimPatchUserRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimPatchUserResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScimPatchUserMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scimDeleteUser(com.udb.core.idp.services.v1.ScimDeleteUserRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimDeleteUserResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScimDeleteUserMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scimCreateGroup(com.udb.core.idp.services.v1.ScimCreateGroupRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimCreateGroupResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScimCreateGroupMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scimGetGroup(com.udb.core.idp.services.v1.ScimGetGroupRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimGetGroupResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScimGetGroupMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scimListGroups(com.udb.core.idp.services.v1.ScimListGroupsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimListGroupsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScimListGroupsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scimPatchGroup(com.udb.core.idp.services.v1.ScimPatchGroupRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimPatchGroupResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScimPatchGroupMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scimDeleteGroup(com.udb.core.idp.services.v1.ScimDeleteGroupRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimDeleteGroupResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScimDeleteGroupMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service IdentityProviderService.
   * <pre>
   * ---------------------------------------------------------------------------
   * IdentityProviderService — Enterprise identity-provider lifecycle, SAML 2.0
   * web SSO (metadata import + ACS), SCIM 2.0 provisioning, JIT user
   * provisioning, and external-identity linking. All RPCs are tenant-scoped and
   * server-only (control-plane); they run on the isolated native auth listener.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static final class IdentityProviderServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<IdentityProviderServiceBlockingV2Stub> {
    private IdentityProviderServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected IdentityProviderServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new IdentityProviderServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * ── Provider administration (J2.6) ────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.idp.services.v1.CreateProviderResponse createProvider(com.udb.core.idp.services.v1.CreateProviderRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreateProviderMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.UpdateProviderResponse updateProvider(com.udb.core.idp.services.v1.UpdateProviderRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getUpdateProviderMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.DisableProviderResponse disableProvider(com.udb.core.idp.services.v1.DisableProviderRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDisableProviderMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.GetProviderResponse getProvider(com.udb.core.idp.services.v1.GetProviderRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetProviderMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ListProvidersResponse listProviders(com.udb.core.idp.services.v1.ListProvidersRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListProvidersMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.TestProviderDiscoveryResponse testProviderDiscovery(com.udb.core.idp.services.v1.TestProviderDiscoveryRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getTestProviderDiscoveryMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ForceJwksRefreshResponse forceJwksRefresh(com.udb.core.idp.services.v1.ForceJwksRefreshRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getForceJwksRefreshMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.PreviewClaimMappingResponse previewClaimMapping(com.udb.core.idp.services.v1.PreviewClaimMappingRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPreviewClaimMappingMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.PreviewGroupMappingResponse previewGroupMapping(com.udb.core.idp.services.v1.PreviewGroupMappingRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPreviewGroupMappingMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ListExternalIdentitiesResponse listExternalIdentities(com.udb.core.idp.services.v1.ListExternalIdentitiesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListExternalIdentitiesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.LinkIdentityResponse linkIdentity(com.udb.core.idp.services.v1.LinkIdentityRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getLinkIdentityMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.UnlinkIdentityResponse unlinkIdentity(com.udb.core.idp.services.v1.UnlinkIdentityRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getUnlinkIdentityMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── SAML 2.0 (J2.2) ───────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.idp.services.v1.ImportSamlMetadataResponse importSamlMetadata(com.udb.core.idp.services.v1.ImportSamlMetadataRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getImportSamlMetadataMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.StartSamlLoginResponse startSamlLogin(com.udb.core.idp.services.v1.StartSamlLoginRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getStartSamlLoginMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.SamlAcsResponse samlAcs(com.udb.core.idp.services.v1.SamlAcsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSamlAcsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── JIT provisioning + assurance (J2.4 / J2.5) ────────────────────────────
     * </pre>
     */
    public com.udb.core.idp.services.v1.ResolveExternalIdentityResponse resolveExternalIdentity(com.udb.core.idp.services.v1.ResolveExternalIdentityRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getResolveExternalIdentityMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── SCIM 2.0 (J2.3) ───────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.idp.services.v1.ScimCreateUserResponse scimCreateUser(com.udb.core.idp.services.v1.ScimCreateUserRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getScimCreateUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimGetUserResponse scimGetUser(com.udb.core.idp.services.v1.ScimGetUserRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getScimGetUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimListUsersResponse scimListUsers(com.udb.core.idp.services.v1.ScimListUsersRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getScimListUsersMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimReplaceUserResponse scimReplaceUser(com.udb.core.idp.services.v1.ScimReplaceUserRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getScimReplaceUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimPatchUserResponse scimPatchUser(com.udb.core.idp.services.v1.ScimPatchUserRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getScimPatchUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimDeleteUserResponse scimDeleteUser(com.udb.core.idp.services.v1.ScimDeleteUserRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getScimDeleteUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimCreateGroupResponse scimCreateGroup(com.udb.core.idp.services.v1.ScimCreateGroupRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getScimCreateGroupMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimGetGroupResponse scimGetGroup(com.udb.core.idp.services.v1.ScimGetGroupRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getScimGetGroupMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimListGroupsResponse scimListGroups(com.udb.core.idp.services.v1.ScimListGroupsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getScimListGroupsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimPatchGroupResponse scimPatchGroup(com.udb.core.idp.services.v1.ScimPatchGroupRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getScimPatchGroupMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimDeleteGroupResponse scimDeleteGroup(com.udb.core.idp.services.v1.ScimDeleteGroupRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getScimDeleteGroupMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service IdentityProviderService.
   * <pre>
   * ---------------------------------------------------------------------------
   * IdentityProviderService — Enterprise identity-provider lifecycle, SAML 2.0
   * web SSO (metadata import + ACS), SCIM 2.0 provisioning, JIT user
   * provisioning, and external-identity linking. All RPCs are tenant-scoped and
   * server-only (control-plane); they run on the isolated native auth listener.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static final class IdentityProviderServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<IdentityProviderServiceBlockingStub> {
    private IdentityProviderServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected IdentityProviderServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new IdentityProviderServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * ── Provider administration (J2.6) ────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.idp.services.v1.CreateProviderResponse createProvider(com.udb.core.idp.services.v1.CreateProviderRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateProviderMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.UpdateProviderResponse updateProvider(com.udb.core.idp.services.v1.UpdateProviderRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateProviderMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.DisableProviderResponse disableProvider(com.udb.core.idp.services.v1.DisableProviderRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDisableProviderMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.GetProviderResponse getProvider(com.udb.core.idp.services.v1.GetProviderRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetProviderMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ListProvidersResponse listProviders(com.udb.core.idp.services.v1.ListProvidersRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListProvidersMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.TestProviderDiscoveryResponse testProviderDiscovery(com.udb.core.idp.services.v1.TestProviderDiscoveryRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTestProviderDiscoveryMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ForceJwksRefreshResponse forceJwksRefresh(com.udb.core.idp.services.v1.ForceJwksRefreshRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getForceJwksRefreshMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.PreviewClaimMappingResponse previewClaimMapping(com.udb.core.idp.services.v1.PreviewClaimMappingRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPreviewClaimMappingMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.PreviewGroupMappingResponse previewGroupMapping(com.udb.core.idp.services.v1.PreviewGroupMappingRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPreviewGroupMappingMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ListExternalIdentitiesResponse listExternalIdentities(com.udb.core.idp.services.v1.ListExternalIdentitiesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListExternalIdentitiesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.LinkIdentityResponse linkIdentity(com.udb.core.idp.services.v1.LinkIdentityRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getLinkIdentityMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.UnlinkIdentityResponse unlinkIdentity(com.udb.core.idp.services.v1.UnlinkIdentityRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUnlinkIdentityMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── SAML 2.0 (J2.2) ───────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.idp.services.v1.ImportSamlMetadataResponse importSamlMetadata(com.udb.core.idp.services.v1.ImportSamlMetadataRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getImportSamlMetadataMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.StartSamlLoginResponse startSamlLogin(com.udb.core.idp.services.v1.StartSamlLoginRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStartSamlLoginMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.SamlAcsResponse samlAcs(com.udb.core.idp.services.v1.SamlAcsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSamlAcsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── JIT provisioning + assurance (J2.4 / J2.5) ────────────────────────────
     * </pre>
     */
    public com.udb.core.idp.services.v1.ResolveExternalIdentityResponse resolveExternalIdentity(com.udb.core.idp.services.v1.ResolveExternalIdentityRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getResolveExternalIdentityMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── SCIM 2.0 (J2.3) ───────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.idp.services.v1.ScimCreateUserResponse scimCreateUser(com.udb.core.idp.services.v1.ScimCreateUserRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScimCreateUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimGetUserResponse scimGetUser(com.udb.core.idp.services.v1.ScimGetUserRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScimGetUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimListUsersResponse scimListUsers(com.udb.core.idp.services.v1.ScimListUsersRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScimListUsersMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimReplaceUserResponse scimReplaceUser(com.udb.core.idp.services.v1.ScimReplaceUserRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScimReplaceUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimPatchUserResponse scimPatchUser(com.udb.core.idp.services.v1.ScimPatchUserRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScimPatchUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimDeleteUserResponse scimDeleteUser(com.udb.core.idp.services.v1.ScimDeleteUserRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScimDeleteUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimCreateGroupResponse scimCreateGroup(com.udb.core.idp.services.v1.ScimCreateGroupRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScimCreateGroupMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimGetGroupResponse scimGetGroup(com.udb.core.idp.services.v1.ScimGetGroupRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScimGetGroupMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimListGroupsResponse scimListGroups(com.udb.core.idp.services.v1.ScimListGroupsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScimListGroupsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimPatchGroupResponse scimPatchGroup(com.udb.core.idp.services.v1.ScimPatchGroupRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScimPatchGroupMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.idp.services.v1.ScimDeleteGroupResponse scimDeleteGroup(com.udb.core.idp.services.v1.ScimDeleteGroupRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScimDeleteGroupMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service IdentityProviderService.
   * <pre>
   * ---------------------------------------------------------------------------
   * IdentityProviderService — Enterprise identity-provider lifecycle, SAML 2.0
   * web SSO (metadata import + ACS), SCIM 2.0 provisioning, JIT user
   * provisioning, and external-identity linking. All RPCs are tenant-scoped and
   * server-only (control-plane); they run on the isolated native auth listener.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static final class IdentityProviderServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<IdentityProviderServiceFutureStub> {
    private IdentityProviderServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected IdentityProviderServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new IdentityProviderServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * ── Provider administration (J2.6) ────────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.CreateProviderResponse> createProvider(
        com.udb.core.idp.services.v1.CreateProviderRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateProviderMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.UpdateProviderResponse> updateProvider(
        com.udb.core.idp.services.v1.UpdateProviderRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateProviderMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.DisableProviderResponse> disableProvider(
        com.udb.core.idp.services.v1.DisableProviderRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDisableProviderMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.GetProviderResponse> getProvider(
        com.udb.core.idp.services.v1.GetProviderRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetProviderMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.ListProvidersResponse> listProviders(
        com.udb.core.idp.services.v1.ListProvidersRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListProvidersMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.TestProviderDiscoveryResponse> testProviderDiscovery(
        com.udb.core.idp.services.v1.TestProviderDiscoveryRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTestProviderDiscoveryMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.ForceJwksRefreshResponse> forceJwksRefresh(
        com.udb.core.idp.services.v1.ForceJwksRefreshRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getForceJwksRefreshMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.PreviewClaimMappingResponse> previewClaimMapping(
        com.udb.core.idp.services.v1.PreviewClaimMappingRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPreviewClaimMappingMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.PreviewGroupMappingResponse> previewGroupMapping(
        com.udb.core.idp.services.v1.PreviewGroupMappingRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPreviewGroupMappingMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.ListExternalIdentitiesResponse> listExternalIdentities(
        com.udb.core.idp.services.v1.ListExternalIdentitiesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListExternalIdentitiesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.LinkIdentityResponse> linkIdentity(
        com.udb.core.idp.services.v1.LinkIdentityRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getLinkIdentityMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.UnlinkIdentityResponse> unlinkIdentity(
        com.udb.core.idp.services.v1.UnlinkIdentityRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUnlinkIdentityMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── SAML 2.0 (J2.2) ───────────────────────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.ImportSamlMetadataResponse> importSamlMetadata(
        com.udb.core.idp.services.v1.ImportSamlMetadataRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getImportSamlMetadataMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.StartSamlLoginResponse> startSamlLogin(
        com.udb.core.idp.services.v1.StartSamlLoginRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStartSamlLoginMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.SamlAcsResponse> samlAcs(
        com.udb.core.idp.services.v1.SamlAcsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSamlAcsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── JIT provisioning + assurance (J2.4 / J2.5) ────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.ResolveExternalIdentityResponse> resolveExternalIdentity(
        com.udb.core.idp.services.v1.ResolveExternalIdentityRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getResolveExternalIdentityMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── SCIM 2.0 (J2.3) ───────────────────────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.ScimCreateUserResponse> scimCreateUser(
        com.udb.core.idp.services.v1.ScimCreateUserRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScimCreateUserMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.ScimGetUserResponse> scimGetUser(
        com.udb.core.idp.services.v1.ScimGetUserRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScimGetUserMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.ScimListUsersResponse> scimListUsers(
        com.udb.core.idp.services.v1.ScimListUsersRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScimListUsersMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.ScimReplaceUserResponse> scimReplaceUser(
        com.udb.core.idp.services.v1.ScimReplaceUserRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScimReplaceUserMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.ScimPatchUserResponse> scimPatchUser(
        com.udb.core.idp.services.v1.ScimPatchUserRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScimPatchUserMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.ScimDeleteUserResponse> scimDeleteUser(
        com.udb.core.idp.services.v1.ScimDeleteUserRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScimDeleteUserMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.ScimCreateGroupResponse> scimCreateGroup(
        com.udb.core.idp.services.v1.ScimCreateGroupRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScimCreateGroupMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.ScimGetGroupResponse> scimGetGroup(
        com.udb.core.idp.services.v1.ScimGetGroupRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScimGetGroupMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.ScimListGroupsResponse> scimListGroups(
        com.udb.core.idp.services.v1.ScimListGroupsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScimListGroupsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.ScimPatchGroupResponse> scimPatchGroup(
        com.udb.core.idp.services.v1.ScimPatchGroupRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScimPatchGroupMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.idp.services.v1.ScimDeleteGroupResponse> scimDeleteGroup(
        com.udb.core.idp.services.v1.ScimDeleteGroupRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScimDeleteGroupMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_PROVIDER = 0;
  private static final int METHODID_UPDATE_PROVIDER = 1;
  private static final int METHODID_DISABLE_PROVIDER = 2;
  private static final int METHODID_GET_PROVIDER = 3;
  private static final int METHODID_LIST_PROVIDERS = 4;
  private static final int METHODID_TEST_PROVIDER_DISCOVERY = 5;
  private static final int METHODID_FORCE_JWKS_REFRESH = 6;
  private static final int METHODID_PREVIEW_CLAIM_MAPPING = 7;
  private static final int METHODID_PREVIEW_GROUP_MAPPING = 8;
  private static final int METHODID_LIST_EXTERNAL_IDENTITIES = 9;
  private static final int METHODID_LINK_IDENTITY = 10;
  private static final int METHODID_UNLINK_IDENTITY = 11;
  private static final int METHODID_IMPORT_SAML_METADATA = 12;
  private static final int METHODID_START_SAML_LOGIN = 13;
  private static final int METHODID_SAML_ACS = 14;
  private static final int METHODID_RESOLVE_EXTERNAL_IDENTITY = 15;
  private static final int METHODID_SCIM_CREATE_USER = 16;
  private static final int METHODID_SCIM_GET_USER = 17;
  private static final int METHODID_SCIM_LIST_USERS = 18;
  private static final int METHODID_SCIM_REPLACE_USER = 19;
  private static final int METHODID_SCIM_PATCH_USER = 20;
  private static final int METHODID_SCIM_DELETE_USER = 21;
  private static final int METHODID_SCIM_CREATE_GROUP = 22;
  private static final int METHODID_SCIM_GET_GROUP = 23;
  private static final int METHODID_SCIM_LIST_GROUPS = 24;
  private static final int METHODID_SCIM_PATCH_GROUP = 25;
  private static final int METHODID_SCIM_DELETE_GROUP = 26;

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
        case METHODID_CREATE_PROVIDER:
          serviceImpl.createProvider((com.udb.core.idp.services.v1.CreateProviderRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.CreateProviderResponse>) responseObserver);
          break;
        case METHODID_UPDATE_PROVIDER:
          serviceImpl.updateProvider((com.udb.core.idp.services.v1.UpdateProviderRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.UpdateProviderResponse>) responseObserver);
          break;
        case METHODID_DISABLE_PROVIDER:
          serviceImpl.disableProvider((com.udb.core.idp.services.v1.DisableProviderRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.DisableProviderResponse>) responseObserver);
          break;
        case METHODID_GET_PROVIDER:
          serviceImpl.getProvider((com.udb.core.idp.services.v1.GetProviderRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.GetProviderResponse>) responseObserver);
          break;
        case METHODID_LIST_PROVIDERS:
          serviceImpl.listProviders((com.udb.core.idp.services.v1.ListProvidersRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ListProvidersResponse>) responseObserver);
          break;
        case METHODID_TEST_PROVIDER_DISCOVERY:
          serviceImpl.testProviderDiscovery((com.udb.core.idp.services.v1.TestProviderDiscoveryRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.TestProviderDiscoveryResponse>) responseObserver);
          break;
        case METHODID_FORCE_JWKS_REFRESH:
          serviceImpl.forceJwksRefresh((com.udb.core.idp.services.v1.ForceJwksRefreshRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ForceJwksRefreshResponse>) responseObserver);
          break;
        case METHODID_PREVIEW_CLAIM_MAPPING:
          serviceImpl.previewClaimMapping((com.udb.core.idp.services.v1.PreviewClaimMappingRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.PreviewClaimMappingResponse>) responseObserver);
          break;
        case METHODID_PREVIEW_GROUP_MAPPING:
          serviceImpl.previewGroupMapping((com.udb.core.idp.services.v1.PreviewGroupMappingRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.PreviewGroupMappingResponse>) responseObserver);
          break;
        case METHODID_LIST_EXTERNAL_IDENTITIES:
          serviceImpl.listExternalIdentities((com.udb.core.idp.services.v1.ListExternalIdentitiesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ListExternalIdentitiesResponse>) responseObserver);
          break;
        case METHODID_LINK_IDENTITY:
          serviceImpl.linkIdentity((com.udb.core.idp.services.v1.LinkIdentityRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.LinkIdentityResponse>) responseObserver);
          break;
        case METHODID_UNLINK_IDENTITY:
          serviceImpl.unlinkIdentity((com.udb.core.idp.services.v1.UnlinkIdentityRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.UnlinkIdentityResponse>) responseObserver);
          break;
        case METHODID_IMPORT_SAML_METADATA:
          serviceImpl.importSamlMetadata((com.udb.core.idp.services.v1.ImportSamlMetadataRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ImportSamlMetadataResponse>) responseObserver);
          break;
        case METHODID_START_SAML_LOGIN:
          serviceImpl.startSamlLogin((com.udb.core.idp.services.v1.StartSamlLoginRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.StartSamlLoginResponse>) responseObserver);
          break;
        case METHODID_SAML_ACS:
          serviceImpl.samlAcs((com.udb.core.idp.services.v1.SamlAcsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.SamlAcsResponse>) responseObserver);
          break;
        case METHODID_RESOLVE_EXTERNAL_IDENTITY:
          serviceImpl.resolveExternalIdentity((com.udb.core.idp.services.v1.ResolveExternalIdentityRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ResolveExternalIdentityResponse>) responseObserver);
          break;
        case METHODID_SCIM_CREATE_USER:
          serviceImpl.scimCreateUser((com.udb.core.idp.services.v1.ScimCreateUserRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimCreateUserResponse>) responseObserver);
          break;
        case METHODID_SCIM_GET_USER:
          serviceImpl.scimGetUser((com.udb.core.idp.services.v1.ScimGetUserRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimGetUserResponse>) responseObserver);
          break;
        case METHODID_SCIM_LIST_USERS:
          serviceImpl.scimListUsers((com.udb.core.idp.services.v1.ScimListUsersRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimListUsersResponse>) responseObserver);
          break;
        case METHODID_SCIM_REPLACE_USER:
          serviceImpl.scimReplaceUser((com.udb.core.idp.services.v1.ScimReplaceUserRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimReplaceUserResponse>) responseObserver);
          break;
        case METHODID_SCIM_PATCH_USER:
          serviceImpl.scimPatchUser((com.udb.core.idp.services.v1.ScimPatchUserRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimPatchUserResponse>) responseObserver);
          break;
        case METHODID_SCIM_DELETE_USER:
          serviceImpl.scimDeleteUser((com.udb.core.idp.services.v1.ScimDeleteUserRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimDeleteUserResponse>) responseObserver);
          break;
        case METHODID_SCIM_CREATE_GROUP:
          serviceImpl.scimCreateGroup((com.udb.core.idp.services.v1.ScimCreateGroupRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimCreateGroupResponse>) responseObserver);
          break;
        case METHODID_SCIM_GET_GROUP:
          serviceImpl.scimGetGroup((com.udb.core.idp.services.v1.ScimGetGroupRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimGetGroupResponse>) responseObserver);
          break;
        case METHODID_SCIM_LIST_GROUPS:
          serviceImpl.scimListGroups((com.udb.core.idp.services.v1.ScimListGroupsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimListGroupsResponse>) responseObserver);
          break;
        case METHODID_SCIM_PATCH_GROUP:
          serviceImpl.scimPatchGroup((com.udb.core.idp.services.v1.ScimPatchGroupRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimPatchGroupResponse>) responseObserver);
          break;
        case METHODID_SCIM_DELETE_GROUP:
          serviceImpl.scimDeleteGroup((com.udb.core.idp.services.v1.ScimDeleteGroupRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.idp.services.v1.ScimDeleteGroupResponse>) responseObserver);
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
          getCreateProviderMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.CreateProviderRequest,
              com.udb.core.idp.services.v1.CreateProviderResponse>(
                service, METHODID_CREATE_PROVIDER)))
        .addMethod(
          getUpdateProviderMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.UpdateProviderRequest,
              com.udb.core.idp.services.v1.UpdateProviderResponse>(
                service, METHODID_UPDATE_PROVIDER)))
        .addMethod(
          getDisableProviderMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.DisableProviderRequest,
              com.udb.core.idp.services.v1.DisableProviderResponse>(
                service, METHODID_DISABLE_PROVIDER)))
        .addMethod(
          getGetProviderMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.GetProviderRequest,
              com.udb.core.idp.services.v1.GetProviderResponse>(
                service, METHODID_GET_PROVIDER)))
        .addMethod(
          getListProvidersMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.ListProvidersRequest,
              com.udb.core.idp.services.v1.ListProvidersResponse>(
                service, METHODID_LIST_PROVIDERS)))
        .addMethod(
          getTestProviderDiscoveryMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.TestProviderDiscoveryRequest,
              com.udb.core.idp.services.v1.TestProviderDiscoveryResponse>(
                service, METHODID_TEST_PROVIDER_DISCOVERY)))
        .addMethod(
          getForceJwksRefreshMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.ForceJwksRefreshRequest,
              com.udb.core.idp.services.v1.ForceJwksRefreshResponse>(
                service, METHODID_FORCE_JWKS_REFRESH)))
        .addMethod(
          getPreviewClaimMappingMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.PreviewClaimMappingRequest,
              com.udb.core.idp.services.v1.PreviewClaimMappingResponse>(
                service, METHODID_PREVIEW_CLAIM_MAPPING)))
        .addMethod(
          getPreviewGroupMappingMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.PreviewGroupMappingRequest,
              com.udb.core.idp.services.v1.PreviewGroupMappingResponse>(
                service, METHODID_PREVIEW_GROUP_MAPPING)))
        .addMethod(
          getListExternalIdentitiesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.ListExternalIdentitiesRequest,
              com.udb.core.idp.services.v1.ListExternalIdentitiesResponse>(
                service, METHODID_LIST_EXTERNAL_IDENTITIES)))
        .addMethod(
          getLinkIdentityMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.LinkIdentityRequest,
              com.udb.core.idp.services.v1.LinkIdentityResponse>(
                service, METHODID_LINK_IDENTITY)))
        .addMethod(
          getUnlinkIdentityMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.UnlinkIdentityRequest,
              com.udb.core.idp.services.v1.UnlinkIdentityResponse>(
                service, METHODID_UNLINK_IDENTITY)))
        .addMethod(
          getImportSamlMetadataMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.ImportSamlMetadataRequest,
              com.udb.core.idp.services.v1.ImportSamlMetadataResponse>(
                service, METHODID_IMPORT_SAML_METADATA)))
        .addMethod(
          getStartSamlLoginMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.StartSamlLoginRequest,
              com.udb.core.idp.services.v1.StartSamlLoginResponse>(
                service, METHODID_START_SAML_LOGIN)))
        .addMethod(
          getSamlAcsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.SamlAcsRequest,
              com.udb.core.idp.services.v1.SamlAcsResponse>(
                service, METHODID_SAML_ACS)))
        .addMethod(
          getResolveExternalIdentityMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.ResolveExternalIdentityRequest,
              com.udb.core.idp.services.v1.ResolveExternalIdentityResponse>(
                service, METHODID_RESOLVE_EXTERNAL_IDENTITY)))
        .addMethod(
          getScimCreateUserMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.ScimCreateUserRequest,
              com.udb.core.idp.services.v1.ScimCreateUserResponse>(
                service, METHODID_SCIM_CREATE_USER)))
        .addMethod(
          getScimGetUserMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.ScimGetUserRequest,
              com.udb.core.idp.services.v1.ScimGetUserResponse>(
                service, METHODID_SCIM_GET_USER)))
        .addMethod(
          getScimListUsersMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.ScimListUsersRequest,
              com.udb.core.idp.services.v1.ScimListUsersResponse>(
                service, METHODID_SCIM_LIST_USERS)))
        .addMethod(
          getScimReplaceUserMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.ScimReplaceUserRequest,
              com.udb.core.idp.services.v1.ScimReplaceUserResponse>(
                service, METHODID_SCIM_REPLACE_USER)))
        .addMethod(
          getScimPatchUserMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.ScimPatchUserRequest,
              com.udb.core.idp.services.v1.ScimPatchUserResponse>(
                service, METHODID_SCIM_PATCH_USER)))
        .addMethod(
          getScimDeleteUserMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.ScimDeleteUserRequest,
              com.udb.core.idp.services.v1.ScimDeleteUserResponse>(
                service, METHODID_SCIM_DELETE_USER)))
        .addMethod(
          getScimCreateGroupMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.ScimCreateGroupRequest,
              com.udb.core.idp.services.v1.ScimCreateGroupResponse>(
                service, METHODID_SCIM_CREATE_GROUP)))
        .addMethod(
          getScimGetGroupMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.ScimGetGroupRequest,
              com.udb.core.idp.services.v1.ScimGetGroupResponse>(
                service, METHODID_SCIM_GET_GROUP)))
        .addMethod(
          getScimListGroupsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.ScimListGroupsRequest,
              com.udb.core.idp.services.v1.ScimListGroupsResponse>(
                service, METHODID_SCIM_LIST_GROUPS)))
        .addMethod(
          getScimPatchGroupMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.ScimPatchGroupRequest,
              com.udb.core.idp.services.v1.ScimPatchGroupResponse>(
                service, METHODID_SCIM_PATCH_GROUP)))
        .addMethod(
          getScimDeleteGroupMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.idp.services.v1.ScimDeleteGroupRequest,
              com.udb.core.idp.services.v1.ScimDeleteGroupResponse>(
                service, METHODID_SCIM_DELETE_GROUP)))
        .build();
  }

  private static abstract class IdentityProviderServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    IdentityProviderServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.idp.services.v1.IdentityProviderServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("IdentityProviderService");
    }
  }

  private static final class IdentityProviderServiceFileDescriptorSupplier
      extends IdentityProviderServiceBaseDescriptorSupplier {
    IdentityProviderServiceFileDescriptorSupplier() {}
  }

  private static final class IdentityProviderServiceMethodDescriptorSupplier
      extends IdentityProviderServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    IdentityProviderServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (IdentityProviderServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new IdentityProviderServiceFileDescriptorSupplier())
              .addMethod(getCreateProviderMethod())
              .addMethod(getUpdateProviderMethod())
              .addMethod(getDisableProviderMethod())
              .addMethod(getGetProviderMethod())
              .addMethod(getListProvidersMethod())
              .addMethod(getTestProviderDiscoveryMethod())
              .addMethod(getForceJwksRefreshMethod())
              .addMethod(getPreviewClaimMappingMethod())
              .addMethod(getPreviewGroupMappingMethod())
              .addMethod(getListExternalIdentitiesMethod())
              .addMethod(getLinkIdentityMethod())
              .addMethod(getUnlinkIdentityMethod())
              .addMethod(getImportSamlMetadataMethod())
              .addMethod(getStartSamlLoginMethod())
              .addMethod(getSamlAcsMethod())
              .addMethod(getResolveExternalIdentityMethod())
              .addMethod(getScimCreateUserMethod())
              .addMethod(getScimGetUserMethod())
              .addMethod(getScimListUsersMethod())
              .addMethod(getScimReplaceUserMethod())
              .addMethod(getScimPatchUserMethod())
              .addMethod(getScimDeleteUserMethod())
              .addMethod(getScimCreateGroupMethod())
              .addMethod(getScimGetGroupMethod())
              .addMethod(getScimListGroupsMethod())
              .addMethod(getScimPatchGroupMethod())
              .addMethod(getScimDeleteGroupMethod())
              .build();
        }
      }
    }
    return result;
  }
}
