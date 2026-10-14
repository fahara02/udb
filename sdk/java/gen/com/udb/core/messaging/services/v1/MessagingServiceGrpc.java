package com.udb.core.messaging.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * MessagingService: the delivery service and key directory for end-to-end
 * encrypted user-to-user messaging over MLS (RFC 9420, ciphersuite
 * MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519). Clients do all cryptography. The
 * broker stores opaque ciphertext plus the PUBLIC group state, validates every
 * handshake commit against that public state (linear epochs, an authoritative
 * roster), and fans messages out to per-device inboxes. It never holds a group
 * secret and never sees plaintext. Handshake metadata (who joined or left, group
 * sizes, timing) is visible to the broker by design.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class MessagingServiceGrpc {

  private MessagingServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.messaging.services.v1.MessagingService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.RegisterMessagingDeviceRequest,
      com.udb.core.messaging.services.v1.RegisterMessagingDeviceResponse> getRegisterMessagingDeviceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RegisterMessagingDevice",
      requestType = com.udb.core.messaging.services.v1.RegisterMessagingDeviceRequest.class,
      responseType = com.udb.core.messaging.services.v1.RegisterMessagingDeviceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.RegisterMessagingDeviceRequest,
      com.udb.core.messaging.services.v1.RegisterMessagingDeviceResponse> getRegisterMessagingDeviceMethod() {
    io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.RegisterMessagingDeviceRequest, com.udb.core.messaging.services.v1.RegisterMessagingDeviceResponse> getRegisterMessagingDeviceMethod;
    if ((getRegisterMessagingDeviceMethod = MessagingServiceGrpc.getRegisterMessagingDeviceMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getRegisterMessagingDeviceMethod = MessagingServiceGrpc.getRegisterMessagingDeviceMethod) == null) {
          MessagingServiceGrpc.getRegisterMessagingDeviceMethod = getRegisterMessagingDeviceMethod =
              io.grpc.MethodDescriptor.<com.udb.core.messaging.services.v1.RegisterMessagingDeviceRequest, com.udb.core.messaging.services.v1.RegisterMessagingDeviceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RegisterMessagingDevice"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.RegisterMessagingDeviceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.RegisterMessagingDeviceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("RegisterMessagingDevice"))
              .build();
        }
      }
    }
    return getRegisterMessagingDeviceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.ListMessagingDevicesRequest,
      com.udb.core.messaging.services.v1.ListMessagingDevicesResponse> getListMessagingDevicesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListMessagingDevices",
      requestType = com.udb.core.messaging.services.v1.ListMessagingDevicesRequest.class,
      responseType = com.udb.core.messaging.services.v1.ListMessagingDevicesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.ListMessagingDevicesRequest,
      com.udb.core.messaging.services.v1.ListMessagingDevicesResponse> getListMessagingDevicesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.ListMessagingDevicesRequest, com.udb.core.messaging.services.v1.ListMessagingDevicesResponse> getListMessagingDevicesMethod;
    if ((getListMessagingDevicesMethod = MessagingServiceGrpc.getListMessagingDevicesMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getListMessagingDevicesMethod = MessagingServiceGrpc.getListMessagingDevicesMethod) == null) {
          MessagingServiceGrpc.getListMessagingDevicesMethod = getListMessagingDevicesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.messaging.services.v1.ListMessagingDevicesRequest, com.udb.core.messaging.services.v1.ListMessagingDevicesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListMessagingDevices"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.ListMessagingDevicesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.ListMessagingDevicesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("ListMessagingDevices"))
              .build();
        }
      }
    }
    return getListMessagingDevicesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.RevokeMessagingDeviceRequest,
      com.udb.core.messaging.services.v1.RevokeMessagingDeviceResponse> getRevokeMessagingDeviceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RevokeMessagingDevice",
      requestType = com.udb.core.messaging.services.v1.RevokeMessagingDeviceRequest.class,
      responseType = com.udb.core.messaging.services.v1.RevokeMessagingDeviceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.RevokeMessagingDeviceRequest,
      com.udb.core.messaging.services.v1.RevokeMessagingDeviceResponse> getRevokeMessagingDeviceMethod() {
    io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.RevokeMessagingDeviceRequest, com.udb.core.messaging.services.v1.RevokeMessagingDeviceResponse> getRevokeMessagingDeviceMethod;
    if ((getRevokeMessagingDeviceMethod = MessagingServiceGrpc.getRevokeMessagingDeviceMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getRevokeMessagingDeviceMethod = MessagingServiceGrpc.getRevokeMessagingDeviceMethod) == null) {
          MessagingServiceGrpc.getRevokeMessagingDeviceMethod = getRevokeMessagingDeviceMethod =
              io.grpc.MethodDescriptor.<com.udb.core.messaging.services.v1.RevokeMessagingDeviceRequest, com.udb.core.messaging.services.v1.RevokeMessagingDeviceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RevokeMessagingDevice"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.RevokeMessagingDeviceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.RevokeMessagingDeviceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("RevokeMessagingDevice"))
              .build();
        }
      }
    }
    return getRevokeMessagingDeviceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.PublishKeyPackagesRequest,
      com.udb.core.messaging.services.v1.PublishKeyPackagesResponse> getPublishKeyPackagesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PublishKeyPackages",
      requestType = com.udb.core.messaging.services.v1.PublishKeyPackagesRequest.class,
      responseType = com.udb.core.messaging.services.v1.PublishKeyPackagesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.PublishKeyPackagesRequest,
      com.udb.core.messaging.services.v1.PublishKeyPackagesResponse> getPublishKeyPackagesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.PublishKeyPackagesRequest, com.udb.core.messaging.services.v1.PublishKeyPackagesResponse> getPublishKeyPackagesMethod;
    if ((getPublishKeyPackagesMethod = MessagingServiceGrpc.getPublishKeyPackagesMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getPublishKeyPackagesMethod = MessagingServiceGrpc.getPublishKeyPackagesMethod) == null) {
          MessagingServiceGrpc.getPublishKeyPackagesMethod = getPublishKeyPackagesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.messaging.services.v1.PublishKeyPackagesRequest, com.udb.core.messaging.services.v1.PublishKeyPackagesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PublishKeyPackages"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.PublishKeyPackagesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.PublishKeyPackagesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("PublishKeyPackages"))
              .build();
        }
      }
    }
    return getPublishKeyPackagesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.ClaimKeyPackagesRequest,
      com.udb.core.messaging.services.v1.ClaimKeyPackagesResponse> getClaimKeyPackagesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ClaimKeyPackages",
      requestType = com.udb.core.messaging.services.v1.ClaimKeyPackagesRequest.class,
      responseType = com.udb.core.messaging.services.v1.ClaimKeyPackagesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.ClaimKeyPackagesRequest,
      com.udb.core.messaging.services.v1.ClaimKeyPackagesResponse> getClaimKeyPackagesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.ClaimKeyPackagesRequest, com.udb.core.messaging.services.v1.ClaimKeyPackagesResponse> getClaimKeyPackagesMethod;
    if ((getClaimKeyPackagesMethod = MessagingServiceGrpc.getClaimKeyPackagesMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getClaimKeyPackagesMethod = MessagingServiceGrpc.getClaimKeyPackagesMethod) == null) {
          MessagingServiceGrpc.getClaimKeyPackagesMethod = getClaimKeyPackagesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.messaging.services.v1.ClaimKeyPackagesRequest, com.udb.core.messaging.services.v1.ClaimKeyPackagesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ClaimKeyPackages"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.ClaimKeyPackagesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.ClaimKeyPackagesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("ClaimKeyPackages"))
              .build();
        }
      }
    }
    return getClaimKeyPackagesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.GetKeyPackageStockRequest,
      com.udb.core.messaging.services.v1.GetKeyPackageStockResponse> getGetKeyPackageStockMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetKeyPackageStock",
      requestType = com.udb.core.messaging.services.v1.GetKeyPackageStockRequest.class,
      responseType = com.udb.core.messaging.services.v1.GetKeyPackageStockResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.GetKeyPackageStockRequest,
      com.udb.core.messaging.services.v1.GetKeyPackageStockResponse> getGetKeyPackageStockMethod() {
    io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.GetKeyPackageStockRequest, com.udb.core.messaging.services.v1.GetKeyPackageStockResponse> getGetKeyPackageStockMethod;
    if ((getGetKeyPackageStockMethod = MessagingServiceGrpc.getGetKeyPackageStockMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getGetKeyPackageStockMethod = MessagingServiceGrpc.getGetKeyPackageStockMethod) == null) {
          MessagingServiceGrpc.getGetKeyPackageStockMethod = getGetKeyPackageStockMethod =
              io.grpc.MethodDescriptor.<com.udb.core.messaging.services.v1.GetKeyPackageStockRequest, com.udb.core.messaging.services.v1.GetKeyPackageStockResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetKeyPackageStock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.GetKeyPackageStockRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.GetKeyPackageStockResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("GetKeyPackageStock"))
              .build();
        }
      }
    }
    return getGetKeyPackageStockMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.CreateGroupRequest,
      com.udb.core.messaging.services.v1.CreateGroupResponse> getCreateGroupMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateGroup",
      requestType = com.udb.core.messaging.services.v1.CreateGroupRequest.class,
      responseType = com.udb.core.messaging.services.v1.CreateGroupResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.CreateGroupRequest,
      com.udb.core.messaging.services.v1.CreateGroupResponse> getCreateGroupMethod() {
    io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.CreateGroupRequest, com.udb.core.messaging.services.v1.CreateGroupResponse> getCreateGroupMethod;
    if ((getCreateGroupMethod = MessagingServiceGrpc.getCreateGroupMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getCreateGroupMethod = MessagingServiceGrpc.getCreateGroupMethod) == null) {
          MessagingServiceGrpc.getCreateGroupMethod = getCreateGroupMethod =
              io.grpc.MethodDescriptor.<com.udb.core.messaging.services.v1.CreateGroupRequest, com.udb.core.messaging.services.v1.CreateGroupResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateGroup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.CreateGroupRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.CreateGroupResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("CreateGroup"))
              .build();
        }
      }
    }
    return getCreateGroupMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.SubmitCommitRequest,
      com.udb.core.messaging.services.v1.SubmitCommitResponse> getSubmitCommitMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SubmitCommit",
      requestType = com.udb.core.messaging.services.v1.SubmitCommitRequest.class,
      responseType = com.udb.core.messaging.services.v1.SubmitCommitResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.SubmitCommitRequest,
      com.udb.core.messaging.services.v1.SubmitCommitResponse> getSubmitCommitMethod() {
    io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.SubmitCommitRequest, com.udb.core.messaging.services.v1.SubmitCommitResponse> getSubmitCommitMethod;
    if ((getSubmitCommitMethod = MessagingServiceGrpc.getSubmitCommitMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getSubmitCommitMethod = MessagingServiceGrpc.getSubmitCommitMethod) == null) {
          MessagingServiceGrpc.getSubmitCommitMethod = getSubmitCommitMethod =
              io.grpc.MethodDescriptor.<com.udb.core.messaging.services.v1.SubmitCommitRequest, com.udb.core.messaging.services.v1.SubmitCommitResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SubmitCommit"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.SubmitCommitRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.SubmitCommitResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("SubmitCommit"))
              .build();
        }
      }
    }
    return getSubmitCommitMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.GetGroupInfoRequest,
      com.udb.core.messaging.services.v1.GetGroupInfoResponse> getGetGroupInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetGroupInfo",
      requestType = com.udb.core.messaging.services.v1.GetGroupInfoRequest.class,
      responseType = com.udb.core.messaging.services.v1.GetGroupInfoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.GetGroupInfoRequest,
      com.udb.core.messaging.services.v1.GetGroupInfoResponse> getGetGroupInfoMethod() {
    io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.GetGroupInfoRequest, com.udb.core.messaging.services.v1.GetGroupInfoResponse> getGetGroupInfoMethod;
    if ((getGetGroupInfoMethod = MessagingServiceGrpc.getGetGroupInfoMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getGetGroupInfoMethod = MessagingServiceGrpc.getGetGroupInfoMethod) == null) {
          MessagingServiceGrpc.getGetGroupInfoMethod = getGetGroupInfoMethod =
              io.grpc.MethodDescriptor.<com.udb.core.messaging.services.v1.GetGroupInfoRequest, com.udb.core.messaging.services.v1.GetGroupInfoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetGroupInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.GetGroupInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.GetGroupInfoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("GetGroupInfo"))
              .build();
        }
      }
    }
    return getGetGroupInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.SendMessageRequest,
      com.udb.core.messaging.services.v1.SendMessageResponse> getSendMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendMessage",
      requestType = com.udb.core.messaging.services.v1.SendMessageRequest.class,
      responseType = com.udb.core.messaging.services.v1.SendMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.SendMessageRequest,
      com.udb.core.messaging.services.v1.SendMessageResponse> getSendMessageMethod() {
    io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.SendMessageRequest, com.udb.core.messaging.services.v1.SendMessageResponse> getSendMessageMethod;
    if ((getSendMessageMethod = MessagingServiceGrpc.getSendMessageMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getSendMessageMethod = MessagingServiceGrpc.getSendMessageMethod) == null) {
          MessagingServiceGrpc.getSendMessageMethod = getSendMessageMethod =
              io.grpc.MethodDescriptor.<com.udb.core.messaging.services.v1.SendMessageRequest, com.udb.core.messaging.services.v1.SendMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.SendMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.SendMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("SendMessage"))
              .build();
        }
      }
    }
    return getSendMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.StreamInboxRequest,
      com.udb.core.messaging.services.v1.StreamInboxResponse> getStreamInboxMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StreamInbox",
      requestType = com.udb.core.messaging.services.v1.StreamInboxRequest.class,
      responseType = com.udb.core.messaging.services.v1.StreamInboxResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.StreamInboxRequest,
      com.udb.core.messaging.services.v1.StreamInboxResponse> getStreamInboxMethod() {
    io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.StreamInboxRequest, com.udb.core.messaging.services.v1.StreamInboxResponse> getStreamInboxMethod;
    if ((getStreamInboxMethod = MessagingServiceGrpc.getStreamInboxMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getStreamInboxMethod = MessagingServiceGrpc.getStreamInboxMethod) == null) {
          MessagingServiceGrpc.getStreamInboxMethod = getStreamInboxMethod =
              io.grpc.MethodDescriptor.<com.udb.core.messaging.services.v1.StreamInboxRequest, com.udb.core.messaging.services.v1.StreamInboxResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StreamInbox"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.StreamInboxRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.StreamInboxResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("StreamInbox"))
              .build();
        }
      }
    }
    return getStreamInboxMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.AckInboxRequest,
      com.udb.core.messaging.services.v1.AckInboxResponse> getAckInboxMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AckInbox",
      requestType = com.udb.core.messaging.services.v1.AckInboxRequest.class,
      responseType = com.udb.core.messaging.services.v1.AckInboxResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.AckInboxRequest,
      com.udb.core.messaging.services.v1.AckInboxResponse> getAckInboxMethod() {
    io.grpc.MethodDescriptor<com.udb.core.messaging.services.v1.AckInboxRequest, com.udb.core.messaging.services.v1.AckInboxResponse> getAckInboxMethod;
    if ((getAckInboxMethod = MessagingServiceGrpc.getAckInboxMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getAckInboxMethod = MessagingServiceGrpc.getAckInboxMethod) == null) {
          MessagingServiceGrpc.getAckInboxMethod = getAckInboxMethod =
              io.grpc.MethodDescriptor.<com.udb.core.messaging.services.v1.AckInboxRequest, com.udb.core.messaging.services.v1.AckInboxResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AckInbox"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.AckInboxRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.messaging.services.v1.AckInboxResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("AckInbox"))
              .build();
        }
      }
    }
    return getAckInboxMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MessagingServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MessagingServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MessagingServiceStub>() {
        @java.lang.Override
        public MessagingServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MessagingServiceStub(channel, callOptions);
        }
      };
    return MessagingServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static MessagingServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MessagingServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MessagingServiceBlockingV2Stub>() {
        @java.lang.Override
        public MessagingServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MessagingServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return MessagingServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MessagingServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MessagingServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MessagingServiceBlockingStub>() {
        @java.lang.Override
        public MessagingServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MessagingServiceBlockingStub(channel, callOptions);
        }
      };
    return MessagingServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MessagingServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MessagingServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MessagingServiceFutureStub>() {
        @java.lang.Override
        public MessagingServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MessagingServiceFutureStub(channel, callOptions);
        }
      };
    return MessagingServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * MessagingService: the delivery service and key directory for end-to-end
   * encrypted user-to-user messaging over MLS (RFC 9420, ciphersuite
   * MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519). Clients do all cryptography. The
   * broker stores opaque ciphertext plus the PUBLIC group state, validates every
   * handshake commit against that public state (linear epochs, an authoritative
   * roster), and fans messages out to per-device inboxes. It never holds a group
   * secret and never sees plaintext. Handshake metadata (who joined or left, group
   * sizes, timing) is visible to the broker by design.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Register one device of the calling principal: its device id, its Ed25519
     * signature public key, and its MLS BasicCredential naming
     * `&lt;tenant&gt;/&lt;subject&gt;/&lt;device_id&gt;`. Re-registering the same device with the same
     * key is idempotent; a different key for an existing device id is refused (a key
     * change is a new device).
     * </pre>
     */
    default void registerMessagingDevice(com.udb.core.messaging.services.v1.RegisterMessagingDeviceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.RegisterMessagingDeviceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRegisterMessagingDeviceMethod(), responseObserver);
    }

    /**
     * <pre>
     * List a principal's devices with their key fingerprints, so a peer can verify
     * keys out of band (safety numbers). Revoked devices are included only when
     * asked for.
     * </pre>
     */
    default void listMessagingDevices(com.udb.core.messaging.services.v1.ListMessagingDevicesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.ListMessagingDevicesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListMessagingDevicesMethod(), responseObserver);
    }

    /**
     * <pre>
     * Revoke one device: it stops receiving fan-out, its unclaimed KeyPackages are
     * deleted, and every group it belongs to gets a removal hint in the other member
     * devices' inboxes so a member commits the removal.
     * </pre>
     */
    default void revokeMessagingDevice(com.udb.core.messaging.services.v1.RevokeMessagingDeviceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.RevokeMessagingDeviceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRevokeMessagingDeviceMethod(), responseObserver);
    }

    /**
     * <pre>
     * Upload one-time MLS KeyPackages (and optionally one last-resort package) for a
     * device. Every package is validated: its credential must name the device and
     * its signature key must be the device's registered key.
     * </pre>
     */
    default void publishKeyPackages(com.udb.core.messaging.services.v1.PublishKeyPackagesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.PublishKeyPackagesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPublishKeyPackagesMethod(), responseObserver);
    }

    /**
     * <pre>
     * Atomically claim one KeyPackage per target device. A one-time package is
     * consumed exactly once, even under concurrent claims; when a device has none
     * left its last-resort package is returned without being consumed.
     * </pre>
     */
    default void claimKeyPackages(com.udb.core.messaging.services.v1.ClaimKeyPackagesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.ClaimKeyPackagesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getClaimKeyPackagesMethod(), responseObserver);
    }

    /**
     * <pre>
     * How many unclaimed one-time KeyPackages a device has left, so the client can
     * top up below a threshold.
     * </pre>
     */
    default void getKeyPackageStock(com.udb.core.messaging.services.v1.GetKeyPackageStockRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.GetKeyPackageStockResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetKeyPackageStockMethod(), responseObserver);
    }

    /**
     * <pre>
     * Register a new MLS group from the creator's signed public GroupInfo and
     * ratchet tree. The broker keeps only public group state and never holds group
     * secrets.
     * </pre>
     */
    default void createGroup(com.udb.core.messaging.services.v1.CreateGroupRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.CreateGroupResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateGroupMethod(), responseObserver);
    }

    /**
     * <pre>
     * Submit an MLS PublicMessage commit (plus the Welcome for any added devices).
     * Accepted only when its epoch is the group's current epoch and it validates
     * against the public group state; concurrent commits linearize and the loser
     * receives FAILED_PRECONDITION carrying the current epoch so it can re-base.
     * </pre>
     */
    default void submitCommit(com.udb.core.messaging.services.v1.SubmitCommitRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.SubmitCommitResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSubmitCommitMethod(), responseObserver);
    }

    /**
     * <pre>
     * Return the group's current epoch, its exported public ratchet tree, and the
     * latest signed GroupInfo a member uploaded, for an external join.
     * </pre>
     */
    default void getGroupInfo(com.udb.core.messaging.services.v1.GetGroupInfoRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.GetGroupInfoResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetGroupInfoMethod(), responseObserver);
    }

    /**
     * <pre>
     * Fan an MLS PrivateMessage ciphertext for the group's current epoch out to every
     * member device's inbox. The broker stores the opaque bytes and never decrypts
     * them.
     * </pre>
     */
    default void sendMessage(com.udb.core.messaging.services.v1.SendMessageRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.SendMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendMessageMethod(), responseObserver);
    }

    /**
     * <pre>
     * Stream a device's ordered inbox (commits, welcomes, messages, removal hints)
     * from a cursor. With follow=true the stream stays open and delivers new entries
     * as they are appended.
     * </pre>
     */
    default void streamInbox(com.udb.core.messaging.services.v1.StreamInboxRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.StreamInboxResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStreamInboxMethod(), responseObserver);
    }

    /**
     * <pre>
     * Advance a device's inbox cursor. Acknowledged entries become collectable by
     * the retention worker.
     * </pre>
     */
    default void ackInbox(com.udb.core.messaging.services.v1.AckInboxRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.AckInboxResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAckInboxMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service MessagingService.
   * <pre>
   * MessagingService: the delivery service and key directory for end-to-end
   * encrypted user-to-user messaging over MLS (RFC 9420, ciphersuite
   * MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519). Clients do all cryptography. The
   * broker stores opaque ciphertext plus the PUBLIC group state, validates every
   * handshake commit against that public state (linear epochs, an authoritative
   * roster), and fans messages out to per-device inboxes. It never holds a group
   * secret and never sees plaintext. Handshake metadata (who joined or left, group
   * sizes, timing) is visible to the broker by design.
   * </pre>
   */
  public static abstract class MessagingServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return MessagingServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service MessagingService.
   * <pre>
   * MessagingService: the delivery service and key directory for end-to-end
   * encrypted user-to-user messaging over MLS (RFC 9420, ciphersuite
   * MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519). Clients do all cryptography. The
   * broker stores opaque ciphertext plus the PUBLIC group state, validates every
   * handshake commit against that public state (linear epochs, an authoritative
   * roster), and fans messages out to per-device inboxes. It never holds a group
   * secret and never sees plaintext. Handshake metadata (who joined or left, group
   * sizes, timing) is visible to the broker by design.
   * </pre>
   */
  public static final class MessagingServiceStub
      extends io.grpc.stub.AbstractAsyncStub<MessagingServiceStub> {
    private MessagingServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MessagingServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MessagingServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register one device of the calling principal: its device id, its Ed25519
     * signature public key, and its MLS BasicCredential naming
     * `&lt;tenant&gt;/&lt;subject&gt;/&lt;device_id&gt;`. Re-registering the same device with the same
     * key is idempotent; a different key for an existing device id is refused (a key
     * change is a new device).
     * </pre>
     */
    public void registerMessagingDevice(com.udb.core.messaging.services.v1.RegisterMessagingDeviceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.RegisterMessagingDeviceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRegisterMessagingDeviceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List a principal's devices with their key fingerprints, so a peer can verify
     * keys out of band (safety numbers). Revoked devices are included only when
     * asked for.
     * </pre>
     */
    public void listMessagingDevices(com.udb.core.messaging.services.v1.ListMessagingDevicesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.ListMessagingDevicesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListMessagingDevicesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Revoke one device: it stops receiving fan-out, its unclaimed KeyPackages are
     * deleted, and every group it belongs to gets a removal hint in the other member
     * devices' inboxes so a member commits the removal.
     * </pre>
     */
    public void revokeMessagingDevice(com.udb.core.messaging.services.v1.RevokeMessagingDeviceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.RevokeMessagingDeviceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRevokeMessagingDeviceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Upload one-time MLS KeyPackages (and optionally one last-resort package) for a
     * device. Every package is validated: its credential must name the device and
     * its signature key must be the device's registered key.
     * </pre>
     */
    public void publishKeyPackages(com.udb.core.messaging.services.v1.PublishKeyPackagesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.PublishKeyPackagesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPublishKeyPackagesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Atomically claim one KeyPackage per target device. A one-time package is
     * consumed exactly once, even under concurrent claims; when a device has none
     * left its last-resort package is returned without being consumed.
     * </pre>
     */
    public void claimKeyPackages(com.udb.core.messaging.services.v1.ClaimKeyPackagesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.ClaimKeyPackagesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getClaimKeyPackagesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * How many unclaimed one-time KeyPackages a device has left, so the client can
     * top up below a threshold.
     * </pre>
     */
    public void getKeyPackageStock(com.udb.core.messaging.services.v1.GetKeyPackageStockRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.GetKeyPackageStockResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetKeyPackageStockMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Register a new MLS group from the creator's signed public GroupInfo and
     * ratchet tree. The broker keeps only public group state and never holds group
     * secrets.
     * </pre>
     */
    public void createGroup(com.udb.core.messaging.services.v1.CreateGroupRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.CreateGroupResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateGroupMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Submit an MLS PublicMessage commit (plus the Welcome for any added devices).
     * Accepted only when its epoch is the group's current epoch and it validates
     * against the public group state; concurrent commits linearize and the loser
     * receives FAILED_PRECONDITION carrying the current epoch so it can re-base.
     * </pre>
     */
    public void submitCommit(com.udb.core.messaging.services.v1.SubmitCommitRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.SubmitCommitResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSubmitCommitMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Return the group's current epoch, its exported public ratchet tree, and the
     * latest signed GroupInfo a member uploaded, for an external join.
     * </pre>
     */
    public void getGroupInfo(com.udb.core.messaging.services.v1.GetGroupInfoRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.GetGroupInfoResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetGroupInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Fan an MLS PrivateMessage ciphertext for the group's current epoch out to every
     * member device's inbox. The broker stores the opaque bytes and never decrypts
     * them.
     * </pre>
     */
    public void sendMessage(com.udb.core.messaging.services.v1.SendMessageRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.SendMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Stream a device's ordered inbox (commits, welcomes, messages, removal hints)
     * from a cursor. With follow=true the stream stays open and delivers new entries
     * as they are appended.
     * </pre>
     */
    public void streamInbox(com.udb.core.messaging.services.v1.StreamInboxRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.StreamInboxResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getStreamInboxMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Advance a device's inbox cursor. Acknowledged entries become collectable by
     * the retention worker.
     * </pre>
     */
    public void ackInbox(com.udb.core.messaging.services.v1.AckInboxRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.AckInboxResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAckInboxMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service MessagingService.
   * <pre>
   * MessagingService: the delivery service and key directory for end-to-end
   * encrypted user-to-user messaging over MLS (RFC 9420, ciphersuite
   * MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519). Clients do all cryptography. The
   * broker stores opaque ciphertext plus the PUBLIC group state, validates every
   * handshake commit against that public state (linear epochs, an authoritative
   * roster), and fans messages out to per-device inboxes. It never holds a group
   * secret and never sees plaintext. Handshake metadata (who joined or left, group
   * sizes, timing) is visible to the broker by design.
   * </pre>
   */
  public static final class MessagingServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<MessagingServiceBlockingV2Stub> {
    private MessagingServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MessagingServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MessagingServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Register one device of the calling principal: its device id, its Ed25519
     * signature public key, and its MLS BasicCredential naming
     * `&lt;tenant&gt;/&lt;subject&gt;/&lt;device_id&gt;`. Re-registering the same device with the same
     * key is idempotent; a different key for an existing device id is refused (a key
     * change is a new device).
     * </pre>
     */
    public com.udb.core.messaging.services.v1.RegisterMessagingDeviceResponse registerMessagingDevice(com.udb.core.messaging.services.v1.RegisterMessagingDeviceRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRegisterMessagingDeviceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List a principal's devices with their key fingerprints, so a peer can verify
     * keys out of band (safety numbers). Revoked devices are included only when
     * asked for.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.ListMessagingDevicesResponse listMessagingDevices(com.udb.core.messaging.services.v1.ListMessagingDevicesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListMessagingDevicesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Revoke one device: it stops receiving fan-out, its unclaimed KeyPackages are
     * deleted, and every group it belongs to gets a removal hint in the other member
     * devices' inboxes so a member commits the removal.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.RevokeMessagingDeviceResponse revokeMessagingDevice(com.udb.core.messaging.services.v1.RevokeMessagingDeviceRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRevokeMessagingDeviceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Upload one-time MLS KeyPackages (and optionally one last-resort package) for a
     * device. Every package is validated: its credential must name the device and
     * its signature key must be the device's registered key.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.PublishKeyPackagesResponse publishKeyPackages(com.udb.core.messaging.services.v1.PublishKeyPackagesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPublishKeyPackagesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Atomically claim one KeyPackage per target device. A one-time package is
     * consumed exactly once, even under concurrent claims; when a device has none
     * left its last-resort package is returned without being consumed.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.ClaimKeyPackagesResponse claimKeyPackages(com.udb.core.messaging.services.v1.ClaimKeyPackagesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getClaimKeyPackagesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * How many unclaimed one-time KeyPackages a device has left, so the client can
     * top up below a threshold.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.GetKeyPackageStockResponse getKeyPackageStock(com.udb.core.messaging.services.v1.GetKeyPackageStockRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetKeyPackageStockMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Register a new MLS group from the creator's signed public GroupInfo and
     * ratchet tree. The broker keeps only public group state and never holds group
     * secrets.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.CreateGroupResponse createGroup(com.udb.core.messaging.services.v1.CreateGroupRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreateGroupMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Submit an MLS PublicMessage commit (plus the Welcome for any added devices).
     * Accepted only when its epoch is the group's current epoch and it validates
     * against the public group state; concurrent commits linearize and the loser
     * receives FAILED_PRECONDITION carrying the current epoch so it can re-base.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.SubmitCommitResponse submitCommit(com.udb.core.messaging.services.v1.SubmitCommitRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSubmitCommitMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Return the group's current epoch, its exported public ratchet tree, and the
     * latest signed GroupInfo a member uploaded, for an external join.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.GetGroupInfoResponse getGroupInfo(com.udb.core.messaging.services.v1.GetGroupInfoRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetGroupInfoMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fan an MLS PrivateMessage ciphertext for the group's current epoch out to every
     * member device's inbox. The broker stores the opaque bytes and never decrypts
     * them.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.SendMessageResponse sendMessage(com.udb.core.messaging.services.v1.SendMessageRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSendMessageMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Stream a device's ordered inbox (commits, welcomes, messages, removal hints)
     * from a cursor. With follow=true the stream stays open and delivers new entries
     * as they are appended.
     * </pre>
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<?, com.udb.core.messaging.services.v1.StreamInboxResponse>
        streamInbox(com.udb.core.messaging.services.v1.StreamInboxRequest request) {
      return io.grpc.stub.ClientCalls.blockingV2ServerStreamingCall(
          getChannel(), getStreamInboxMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Advance a device's inbox cursor. Acknowledged entries become collectable by
     * the retention worker.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.AckInboxResponse ackInbox(com.udb.core.messaging.services.v1.AckInboxRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getAckInboxMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service MessagingService.
   * <pre>
   * MessagingService: the delivery service and key directory for end-to-end
   * encrypted user-to-user messaging over MLS (RFC 9420, ciphersuite
   * MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519). Clients do all cryptography. The
   * broker stores opaque ciphertext plus the PUBLIC group state, validates every
   * handshake commit against that public state (linear epochs, an authoritative
   * roster), and fans messages out to per-device inboxes. It never holds a group
   * secret and never sees plaintext. Handshake metadata (who joined or left, group
   * sizes, timing) is visible to the broker by design.
   * </pre>
   */
  public static final class MessagingServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<MessagingServiceBlockingStub> {
    private MessagingServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MessagingServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MessagingServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register one device of the calling principal: its device id, its Ed25519
     * signature public key, and its MLS BasicCredential naming
     * `&lt;tenant&gt;/&lt;subject&gt;/&lt;device_id&gt;`. Re-registering the same device with the same
     * key is idempotent; a different key for an existing device id is refused (a key
     * change is a new device).
     * </pre>
     */
    public com.udb.core.messaging.services.v1.RegisterMessagingDeviceResponse registerMessagingDevice(com.udb.core.messaging.services.v1.RegisterMessagingDeviceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRegisterMessagingDeviceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List a principal's devices with their key fingerprints, so a peer can verify
     * keys out of band (safety numbers). Revoked devices are included only when
     * asked for.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.ListMessagingDevicesResponse listMessagingDevices(com.udb.core.messaging.services.v1.ListMessagingDevicesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListMessagingDevicesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Revoke one device: it stops receiving fan-out, its unclaimed KeyPackages are
     * deleted, and every group it belongs to gets a removal hint in the other member
     * devices' inboxes so a member commits the removal.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.RevokeMessagingDeviceResponse revokeMessagingDevice(com.udb.core.messaging.services.v1.RevokeMessagingDeviceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRevokeMessagingDeviceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Upload one-time MLS KeyPackages (and optionally one last-resort package) for a
     * device. Every package is validated: its credential must name the device and
     * its signature key must be the device's registered key.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.PublishKeyPackagesResponse publishKeyPackages(com.udb.core.messaging.services.v1.PublishKeyPackagesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPublishKeyPackagesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Atomically claim one KeyPackage per target device. A one-time package is
     * consumed exactly once, even under concurrent claims; when a device has none
     * left its last-resort package is returned without being consumed.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.ClaimKeyPackagesResponse claimKeyPackages(com.udb.core.messaging.services.v1.ClaimKeyPackagesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getClaimKeyPackagesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * How many unclaimed one-time KeyPackages a device has left, so the client can
     * top up below a threshold.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.GetKeyPackageStockResponse getKeyPackageStock(com.udb.core.messaging.services.v1.GetKeyPackageStockRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetKeyPackageStockMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Register a new MLS group from the creator's signed public GroupInfo and
     * ratchet tree. The broker keeps only public group state and never holds group
     * secrets.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.CreateGroupResponse createGroup(com.udb.core.messaging.services.v1.CreateGroupRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateGroupMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Submit an MLS PublicMessage commit (plus the Welcome for any added devices).
     * Accepted only when its epoch is the group's current epoch and it validates
     * against the public group state; concurrent commits linearize and the loser
     * receives FAILED_PRECONDITION carrying the current epoch so it can re-base.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.SubmitCommitResponse submitCommit(com.udb.core.messaging.services.v1.SubmitCommitRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSubmitCommitMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Return the group's current epoch, its exported public ratchet tree, and the
     * latest signed GroupInfo a member uploaded, for an external join.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.GetGroupInfoResponse getGroupInfo(com.udb.core.messaging.services.v1.GetGroupInfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetGroupInfoMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fan an MLS PrivateMessage ciphertext for the group's current epoch out to every
     * member device's inbox. The broker stores the opaque bytes and never decrypts
     * them.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.SendMessageResponse sendMessage(com.udb.core.messaging.services.v1.SendMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendMessageMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Stream a device's ordered inbox (commits, welcomes, messages, removal hints)
     * from a cursor. With follow=true the stream stays open and delivers new entries
     * as they are appended.
     * </pre>
     */
    public java.util.Iterator<com.udb.core.messaging.services.v1.StreamInboxResponse> streamInbox(
        com.udb.core.messaging.services.v1.StreamInboxRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getStreamInboxMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Advance a device's inbox cursor. Acknowledged entries become collectable by
     * the retention worker.
     * </pre>
     */
    public com.udb.core.messaging.services.v1.AckInboxResponse ackInbox(com.udb.core.messaging.services.v1.AckInboxRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAckInboxMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service MessagingService.
   * <pre>
   * MessagingService: the delivery service and key directory for end-to-end
   * encrypted user-to-user messaging over MLS (RFC 9420, ciphersuite
   * MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519). Clients do all cryptography. The
   * broker stores opaque ciphertext plus the PUBLIC group state, validates every
   * handshake commit against that public state (linear epochs, an authoritative
   * roster), and fans messages out to per-device inboxes. It never holds a group
   * secret and never sees plaintext. Handshake metadata (who joined or left, group
   * sizes, timing) is visible to the broker by design.
   * </pre>
   */
  public static final class MessagingServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<MessagingServiceFutureStub> {
    private MessagingServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MessagingServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MessagingServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register one device of the calling principal: its device id, its Ed25519
     * signature public key, and its MLS BasicCredential naming
     * `&lt;tenant&gt;/&lt;subject&gt;/&lt;device_id&gt;`. Re-registering the same device with the same
     * key is idempotent; a different key for an existing device id is refused (a key
     * change is a new device).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.messaging.services.v1.RegisterMessagingDeviceResponse> registerMessagingDevice(
        com.udb.core.messaging.services.v1.RegisterMessagingDeviceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRegisterMessagingDeviceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List a principal's devices with their key fingerprints, so a peer can verify
     * keys out of band (safety numbers). Revoked devices are included only when
     * asked for.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.messaging.services.v1.ListMessagingDevicesResponse> listMessagingDevices(
        com.udb.core.messaging.services.v1.ListMessagingDevicesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListMessagingDevicesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Revoke one device: it stops receiving fan-out, its unclaimed KeyPackages are
     * deleted, and every group it belongs to gets a removal hint in the other member
     * devices' inboxes so a member commits the removal.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.messaging.services.v1.RevokeMessagingDeviceResponse> revokeMessagingDevice(
        com.udb.core.messaging.services.v1.RevokeMessagingDeviceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRevokeMessagingDeviceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Upload one-time MLS KeyPackages (and optionally one last-resort package) for a
     * device. Every package is validated: its credential must name the device and
     * its signature key must be the device's registered key.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.messaging.services.v1.PublishKeyPackagesResponse> publishKeyPackages(
        com.udb.core.messaging.services.v1.PublishKeyPackagesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPublishKeyPackagesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Atomically claim one KeyPackage per target device. A one-time package is
     * consumed exactly once, even under concurrent claims; when a device has none
     * left its last-resort package is returned without being consumed.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.messaging.services.v1.ClaimKeyPackagesResponse> claimKeyPackages(
        com.udb.core.messaging.services.v1.ClaimKeyPackagesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getClaimKeyPackagesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * How many unclaimed one-time KeyPackages a device has left, so the client can
     * top up below a threshold.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.messaging.services.v1.GetKeyPackageStockResponse> getKeyPackageStock(
        com.udb.core.messaging.services.v1.GetKeyPackageStockRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetKeyPackageStockMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Register a new MLS group from the creator's signed public GroupInfo and
     * ratchet tree. The broker keeps only public group state and never holds group
     * secrets.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.messaging.services.v1.CreateGroupResponse> createGroup(
        com.udb.core.messaging.services.v1.CreateGroupRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateGroupMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Submit an MLS PublicMessage commit (plus the Welcome for any added devices).
     * Accepted only when its epoch is the group's current epoch and it validates
     * against the public group state; concurrent commits linearize and the loser
     * receives FAILED_PRECONDITION carrying the current epoch so it can re-base.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.messaging.services.v1.SubmitCommitResponse> submitCommit(
        com.udb.core.messaging.services.v1.SubmitCommitRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSubmitCommitMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Return the group's current epoch, its exported public ratchet tree, and the
     * latest signed GroupInfo a member uploaded, for an external join.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.messaging.services.v1.GetGroupInfoResponse> getGroupInfo(
        com.udb.core.messaging.services.v1.GetGroupInfoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetGroupInfoMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Fan an MLS PrivateMessage ciphertext for the group's current epoch out to every
     * member device's inbox. The broker stores the opaque bytes and never decrypts
     * them.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.messaging.services.v1.SendMessageResponse> sendMessage(
        com.udb.core.messaging.services.v1.SendMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendMessageMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Advance a device's inbox cursor. Acknowledged entries become collectable by
     * the retention worker.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.messaging.services.v1.AckInboxResponse> ackInbox(
        com.udb.core.messaging.services.v1.AckInboxRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAckInboxMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_REGISTER_MESSAGING_DEVICE = 0;
  private static final int METHODID_LIST_MESSAGING_DEVICES = 1;
  private static final int METHODID_REVOKE_MESSAGING_DEVICE = 2;
  private static final int METHODID_PUBLISH_KEY_PACKAGES = 3;
  private static final int METHODID_CLAIM_KEY_PACKAGES = 4;
  private static final int METHODID_GET_KEY_PACKAGE_STOCK = 5;
  private static final int METHODID_CREATE_GROUP = 6;
  private static final int METHODID_SUBMIT_COMMIT = 7;
  private static final int METHODID_GET_GROUP_INFO = 8;
  private static final int METHODID_SEND_MESSAGE = 9;
  private static final int METHODID_STREAM_INBOX = 10;
  private static final int METHODID_ACK_INBOX = 11;

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
        case METHODID_REGISTER_MESSAGING_DEVICE:
          serviceImpl.registerMessagingDevice((com.udb.core.messaging.services.v1.RegisterMessagingDeviceRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.RegisterMessagingDeviceResponse>) responseObserver);
          break;
        case METHODID_LIST_MESSAGING_DEVICES:
          serviceImpl.listMessagingDevices((com.udb.core.messaging.services.v1.ListMessagingDevicesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.ListMessagingDevicesResponse>) responseObserver);
          break;
        case METHODID_REVOKE_MESSAGING_DEVICE:
          serviceImpl.revokeMessagingDevice((com.udb.core.messaging.services.v1.RevokeMessagingDeviceRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.RevokeMessagingDeviceResponse>) responseObserver);
          break;
        case METHODID_PUBLISH_KEY_PACKAGES:
          serviceImpl.publishKeyPackages((com.udb.core.messaging.services.v1.PublishKeyPackagesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.PublishKeyPackagesResponse>) responseObserver);
          break;
        case METHODID_CLAIM_KEY_PACKAGES:
          serviceImpl.claimKeyPackages((com.udb.core.messaging.services.v1.ClaimKeyPackagesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.ClaimKeyPackagesResponse>) responseObserver);
          break;
        case METHODID_GET_KEY_PACKAGE_STOCK:
          serviceImpl.getKeyPackageStock((com.udb.core.messaging.services.v1.GetKeyPackageStockRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.GetKeyPackageStockResponse>) responseObserver);
          break;
        case METHODID_CREATE_GROUP:
          serviceImpl.createGroup((com.udb.core.messaging.services.v1.CreateGroupRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.CreateGroupResponse>) responseObserver);
          break;
        case METHODID_SUBMIT_COMMIT:
          serviceImpl.submitCommit((com.udb.core.messaging.services.v1.SubmitCommitRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.SubmitCommitResponse>) responseObserver);
          break;
        case METHODID_GET_GROUP_INFO:
          serviceImpl.getGroupInfo((com.udb.core.messaging.services.v1.GetGroupInfoRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.GetGroupInfoResponse>) responseObserver);
          break;
        case METHODID_SEND_MESSAGE:
          serviceImpl.sendMessage((com.udb.core.messaging.services.v1.SendMessageRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.SendMessageResponse>) responseObserver);
          break;
        case METHODID_STREAM_INBOX:
          serviceImpl.streamInbox((com.udb.core.messaging.services.v1.StreamInboxRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.StreamInboxResponse>) responseObserver);
          break;
        case METHODID_ACK_INBOX:
          serviceImpl.ackInbox((com.udb.core.messaging.services.v1.AckInboxRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.messaging.services.v1.AckInboxResponse>) responseObserver);
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
        .<com.udb.core.messaging.services.v1.RegisterMessagingDeviceRequest, com.udb.core.messaging.services.v1.RegisterMessagingDeviceResponse>addMethod(
          getRegisterMessagingDeviceMethod(),
          io.grpc.stub.ServerCalls.<com.udb.core.messaging.services.v1.RegisterMessagingDeviceRequest, com.udb.core.messaging.services.v1.RegisterMessagingDeviceResponse>asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.messaging.services.v1.RegisterMessagingDeviceRequest,
              com.udb.core.messaging.services.v1.RegisterMessagingDeviceResponse>(
                service, METHODID_REGISTER_MESSAGING_DEVICE)))
        .<com.udb.core.messaging.services.v1.ListMessagingDevicesRequest, com.udb.core.messaging.services.v1.ListMessagingDevicesResponse>addMethod(
          getListMessagingDevicesMethod(),
          io.grpc.stub.ServerCalls.<com.udb.core.messaging.services.v1.ListMessagingDevicesRequest, com.udb.core.messaging.services.v1.ListMessagingDevicesResponse>asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.messaging.services.v1.ListMessagingDevicesRequest,
              com.udb.core.messaging.services.v1.ListMessagingDevicesResponse>(
                service, METHODID_LIST_MESSAGING_DEVICES)))
        .<com.udb.core.messaging.services.v1.RevokeMessagingDeviceRequest, com.udb.core.messaging.services.v1.RevokeMessagingDeviceResponse>addMethod(
          getRevokeMessagingDeviceMethod(),
          io.grpc.stub.ServerCalls.<com.udb.core.messaging.services.v1.RevokeMessagingDeviceRequest, com.udb.core.messaging.services.v1.RevokeMessagingDeviceResponse>asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.messaging.services.v1.RevokeMessagingDeviceRequest,
              com.udb.core.messaging.services.v1.RevokeMessagingDeviceResponse>(
                service, METHODID_REVOKE_MESSAGING_DEVICE)))
        .<com.udb.core.messaging.services.v1.PublishKeyPackagesRequest, com.udb.core.messaging.services.v1.PublishKeyPackagesResponse>addMethod(
          getPublishKeyPackagesMethod(),
          io.grpc.stub.ServerCalls.<com.udb.core.messaging.services.v1.PublishKeyPackagesRequest, com.udb.core.messaging.services.v1.PublishKeyPackagesResponse>asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.messaging.services.v1.PublishKeyPackagesRequest,
              com.udb.core.messaging.services.v1.PublishKeyPackagesResponse>(
                service, METHODID_PUBLISH_KEY_PACKAGES)))
        .<com.udb.core.messaging.services.v1.ClaimKeyPackagesRequest, com.udb.core.messaging.services.v1.ClaimKeyPackagesResponse>addMethod(
          getClaimKeyPackagesMethod(),
          io.grpc.stub.ServerCalls.<com.udb.core.messaging.services.v1.ClaimKeyPackagesRequest, com.udb.core.messaging.services.v1.ClaimKeyPackagesResponse>asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.messaging.services.v1.ClaimKeyPackagesRequest,
              com.udb.core.messaging.services.v1.ClaimKeyPackagesResponse>(
                service, METHODID_CLAIM_KEY_PACKAGES)))
        .<com.udb.core.messaging.services.v1.GetKeyPackageStockRequest, com.udb.core.messaging.services.v1.GetKeyPackageStockResponse>addMethod(
          getGetKeyPackageStockMethod(),
          io.grpc.stub.ServerCalls.<com.udb.core.messaging.services.v1.GetKeyPackageStockRequest, com.udb.core.messaging.services.v1.GetKeyPackageStockResponse>asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.messaging.services.v1.GetKeyPackageStockRequest,
              com.udb.core.messaging.services.v1.GetKeyPackageStockResponse>(
                service, METHODID_GET_KEY_PACKAGE_STOCK)))
        .<com.udb.core.messaging.services.v1.CreateGroupRequest, com.udb.core.messaging.services.v1.CreateGroupResponse>addMethod(
          getCreateGroupMethod(),
          io.grpc.stub.ServerCalls.<com.udb.core.messaging.services.v1.CreateGroupRequest, com.udb.core.messaging.services.v1.CreateGroupResponse>asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.messaging.services.v1.CreateGroupRequest,
              com.udb.core.messaging.services.v1.CreateGroupResponse>(
                service, METHODID_CREATE_GROUP)))
        .<com.udb.core.messaging.services.v1.SubmitCommitRequest, com.udb.core.messaging.services.v1.SubmitCommitResponse>addMethod(
          getSubmitCommitMethod(),
          io.grpc.stub.ServerCalls.<com.udb.core.messaging.services.v1.SubmitCommitRequest, com.udb.core.messaging.services.v1.SubmitCommitResponse>asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.messaging.services.v1.SubmitCommitRequest,
              com.udb.core.messaging.services.v1.SubmitCommitResponse>(
                service, METHODID_SUBMIT_COMMIT)))
        .<com.udb.core.messaging.services.v1.GetGroupInfoRequest, com.udb.core.messaging.services.v1.GetGroupInfoResponse>addMethod(
          getGetGroupInfoMethod(),
          io.grpc.stub.ServerCalls.<com.udb.core.messaging.services.v1.GetGroupInfoRequest, com.udb.core.messaging.services.v1.GetGroupInfoResponse>asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.messaging.services.v1.GetGroupInfoRequest,
              com.udb.core.messaging.services.v1.GetGroupInfoResponse>(
                service, METHODID_GET_GROUP_INFO)))
        .<com.udb.core.messaging.services.v1.SendMessageRequest, com.udb.core.messaging.services.v1.SendMessageResponse>addMethod(
          getSendMessageMethod(),
          io.grpc.stub.ServerCalls.<com.udb.core.messaging.services.v1.SendMessageRequest, com.udb.core.messaging.services.v1.SendMessageResponse>asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.messaging.services.v1.SendMessageRequest,
              com.udb.core.messaging.services.v1.SendMessageResponse>(
                service, METHODID_SEND_MESSAGE)))
        .<com.udb.core.messaging.services.v1.StreamInboxRequest, com.udb.core.messaging.services.v1.StreamInboxResponse>addMethod(
          getStreamInboxMethod(),
          io.grpc.stub.ServerCalls.<com.udb.core.messaging.services.v1.StreamInboxRequest, com.udb.core.messaging.services.v1.StreamInboxResponse>asyncServerStreamingCall(
            new MethodHandlers<
              com.udb.core.messaging.services.v1.StreamInboxRequest,
              com.udb.core.messaging.services.v1.StreamInboxResponse>(
                service, METHODID_STREAM_INBOX)))
        .<com.udb.core.messaging.services.v1.AckInboxRequest, com.udb.core.messaging.services.v1.AckInboxResponse>addMethod(
          getAckInboxMethod(),
          io.grpc.stub.ServerCalls.<com.udb.core.messaging.services.v1.AckInboxRequest, com.udb.core.messaging.services.v1.AckInboxResponse>asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.messaging.services.v1.AckInboxRequest,
              com.udb.core.messaging.services.v1.AckInboxResponse>(
                service, METHODID_ACK_INBOX)))
        .build();
  }

  private static abstract class MessagingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    MessagingServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.messaging.services.v1.MessagingServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("MessagingService");
    }
  }

  private static final class MessagingServiceFileDescriptorSupplier
      extends MessagingServiceBaseDescriptorSupplier {
    MessagingServiceFileDescriptorSupplier() {}
  }

  private static final class MessagingServiceMethodDescriptorSupplier
      extends MessagingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    MessagingServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (MessagingServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new MessagingServiceFileDescriptorSupplier())
              .addMethod(getRegisterMessagingDeviceMethod())
              .addMethod(getListMessagingDevicesMethod())
              .addMethod(getRevokeMessagingDeviceMethod())
              .addMethod(getPublishKeyPackagesMethod())
              .addMethod(getClaimKeyPackagesMethod())
              .addMethod(getGetKeyPackageStockMethod())
              .addMethod(getCreateGroupMethod())
              .addMethod(getSubmitCommitMethod())
              .addMethod(getGetGroupInfoMethod())
              .addMethod(getSendMessageMethod())
              .addMethod(getStreamInboxMethod())
              .addMethod(getAckInboxMethod())
              .build();
        }
      }
    }
    return result;
  }
}
