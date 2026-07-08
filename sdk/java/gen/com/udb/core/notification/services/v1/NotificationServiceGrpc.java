package com.udb.core.notification.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class NotificationServiceGrpc {

  private NotificationServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.notification.services.v1.NotificationService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.SendNotificationRequest,
      com.udb.core.notification.services.v1.SendNotificationResponse> getSendNotificationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendNotification",
      requestType = com.udb.core.notification.services.v1.SendNotificationRequest.class,
      responseType = com.udb.core.notification.services.v1.SendNotificationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.SendNotificationRequest,
      com.udb.core.notification.services.v1.SendNotificationResponse> getSendNotificationMethod() {
    io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.SendNotificationRequest, com.udb.core.notification.services.v1.SendNotificationResponse> getSendNotificationMethod;
    if ((getSendNotificationMethod = NotificationServiceGrpc.getSendNotificationMethod) == null) {
      synchronized (NotificationServiceGrpc.class) {
        if ((getSendNotificationMethod = NotificationServiceGrpc.getSendNotificationMethod) == null) {
          NotificationServiceGrpc.getSendNotificationMethod = getSendNotificationMethod =
              io.grpc.MethodDescriptor.<com.udb.core.notification.services.v1.SendNotificationRequest, com.udb.core.notification.services.v1.SendNotificationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendNotification"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.SendNotificationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.SendNotificationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NotificationServiceMethodDescriptorSupplier("SendNotification"))
              .build();
        }
      }
    }
    return getSendNotificationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.GetNotificationRequest,
      com.udb.core.notification.services.v1.GetNotificationResponse> getGetNotificationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetNotification",
      requestType = com.udb.core.notification.services.v1.GetNotificationRequest.class,
      responseType = com.udb.core.notification.services.v1.GetNotificationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.GetNotificationRequest,
      com.udb.core.notification.services.v1.GetNotificationResponse> getGetNotificationMethod() {
    io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.GetNotificationRequest, com.udb.core.notification.services.v1.GetNotificationResponse> getGetNotificationMethod;
    if ((getGetNotificationMethod = NotificationServiceGrpc.getGetNotificationMethod) == null) {
      synchronized (NotificationServiceGrpc.class) {
        if ((getGetNotificationMethod = NotificationServiceGrpc.getGetNotificationMethod) == null) {
          NotificationServiceGrpc.getGetNotificationMethod = getGetNotificationMethod =
              io.grpc.MethodDescriptor.<com.udb.core.notification.services.v1.GetNotificationRequest, com.udb.core.notification.services.v1.GetNotificationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetNotification"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.GetNotificationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.GetNotificationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NotificationServiceMethodDescriptorSupplier("GetNotification"))
              .build();
        }
      }
    }
    return getGetNotificationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.ListNotificationsRequest,
      com.udb.core.notification.services.v1.ListNotificationsResponse> getListNotificationsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListNotifications",
      requestType = com.udb.core.notification.services.v1.ListNotificationsRequest.class,
      responseType = com.udb.core.notification.services.v1.ListNotificationsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.ListNotificationsRequest,
      com.udb.core.notification.services.v1.ListNotificationsResponse> getListNotificationsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.ListNotificationsRequest, com.udb.core.notification.services.v1.ListNotificationsResponse> getListNotificationsMethod;
    if ((getListNotificationsMethod = NotificationServiceGrpc.getListNotificationsMethod) == null) {
      synchronized (NotificationServiceGrpc.class) {
        if ((getListNotificationsMethod = NotificationServiceGrpc.getListNotificationsMethod) == null) {
          NotificationServiceGrpc.getListNotificationsMethod = getListNotificationsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.notification.services.v1.ListNotificationsRequest, com.udb.core.notification.services.v1.ListNotificationsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListNotifications"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.ListNotificationsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.ListNotificationsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NotificationServiceMethodDescriptorSupplier("ListNotifications"))
              .build();
        }
      }
    }
    return getListNotificationsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.RetryNotificationRequest,
      com.udb.core.notification.services.v1.RetryNotificationResponse> getRetryNotificationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RetryNotification",
      requestType = com.udb.core.notification.services.v1.RetryNotificationRequest.class,
      responseType = com.udb.core.notification.services.v1.RetryNotificationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.RetryNotificationRequest,
      com.udb.core.notification.services.v1.RetryNotificationResponse> getRetryNotificationMethod() {
    io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.RetryNotificationRequest, com.udb.core.notification.services.v1.RetryNotificationResponse> getRetryNotificationMethod;
    if ((getRetryNotificationMethod = NotificationServiceGrpc.getRetryNotificationMethod) == null) {
      synchronized (NotificationServiceGrpc.class) {
        if ((getRetryNotificationMethod = NotificationServiceGrpc.getRetryNotificationMethod) == null) {
          NotificationServiceGrpc.getRetryNotificationMethod = getRetryNotificationMethod =
              io.grpc.MethodDescriptor.<com.udb.core.notification.services.v1.RetryNotificationRequest, com.udb.core.notification.services.v1.RetryNotificationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RetryNotification"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.RetryNotificationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.RetryNotificationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NotificationServiceMethodDescriptorSupplier("RetryNotification"))
              .build();
        }
      }
    }
    return getRetryNotificationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.ReportDeliveryRequest,
      com.udb.core.notification.services.v1.ReportDeliveryResponse> getReportDeliveryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReportDelivery",
      requestType = com.udb.core.notification.services.v1.ReportDeliveryRequest.class,
      responseType = com.udb.core.notification.services.v1.ReportDeliveryResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.ReportDeliveryRequest,
      com.udb.core.notification.services.v1.ReportDeliveryResponse> getReportDeliveryMethod() {
    io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.ReportDeliveryRequest, com.udb.core.notification.services.v1.ReportDeliveryResponse> getReportDeliveryMethod;
    if ((getReportDeliveryMethod = NotificationServiceGrpc.getReportDeliveryMethod) == null) {
      synchronized (NotificationServiceGrpc.class) {
        if ((getReportDeliveryMethod = NotificationServiceGrpc.getReportDeliveryMethod) == null) {
          NotificationServiceGrpc.getReportDeliveryMethod = getReportDeliveryMethod =
              io.grpc.MethodDescriptor.<com.udb.core.notification.services.v1.ReportDeliveryRequest, com.udb.core.notification.services.v1.ReportDeliveryResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReportDelivery"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.ReportDeliveryRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.ReportDeliveryResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NotificationServiceMethodDescriptorSupplier("ReportDelivery"))
              .build();
        }
      }
    }
    return getReportDeliveryMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.UpsertTemplateRequest,
      com.udb.core.notification.services.v1.UpsertTemplateResponse> getUpsertTemplateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpsertTemplate",
      requestType = com.udb.core.notification.services.v1.UpsertTemplateRequest.class,
      responseType = com.udb.core.notification.services.v1.UpsertTemplateResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.UpsertTemplateRequest,
      com.udb.core.notification.services.v1.UpsertTemplateResponse> getUpsertTemplateMethod() {
    io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.UpsertTemplateRequest, com.udb.core.notification.services.v1.UpsertTemplateResponse> getUpsertTemplateMethod;
    if ((getUpsertTemplateMethod = NotificationServiceGrpc.getUpsertTemplateMethod) == null) {
      synchronized (NotificationServiceGrpc.class) {
        if ((getUpsertTemplateMethod = NotificationServiceGrpc.getUpsertTemplateMethod) == null) {
          NotificationServiceGrpc.getUpsertTemplateMethod = getUpsertTemplateMethod =
              io.grpc.MethodDescriptor.<com.udb.core.notification.services.v1.UpsertTemplateRequest, com.udb.core.notification.services.v1.UpsertTemplateResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpsertTemplate"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.UpsertTemplateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.UpsertTemplateResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NotificationServiceMethodDescriptorSupplier("UpsertTemplate"))
              .build();
        }
      }
    }
    return getUpsertTemplateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.GetTemplateRequest,
      com.udb.core.notification.services.v1.GetTemplateResponse> getGetTemplateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTemplate",
      requestType = com.udb.core.notification.services.v1.GetTemplateRequest.class,
      responseType = com.udb.core.notification.services.v1.GetTemplateResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.GetTemplateRequest,
      com.udb.core.notification.services.v1.GetTemplateResponse> getGetTemplateMethod() {
    io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.GetTemplateRequest, com.udb.core.notification.services.v1.GetTemplateResponse> getGetTemplateMethod;
    if ((getGetTemplateMethod = NotificationServiceGrpc.getGetTemplateMethod) == null) {
      synchronized (NotificationServiceGrpc.class) {
        if ((getGetTemplateMethod = NotificationServiceGrpc.getGetTemplateMethod) == null) {
          NotificationServiceGrpc.getGetTemplateMethod = getGetTemplateMethod =
              io.grpc.MethodDescriptor.<com.udb.core.notification.services.v1.GetTemplateRequest, com.udb.core.notification.services.v1.GetTemplateResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTemplate"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.GetTemplateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.GetTemplateResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NotificationServiceMethodDescriptorSupplier("GetTemplate"))
              .build();
        }
      }
    }
    return getGetTemplateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.ListTemplatesRequest,
      com.udb.core.notification.services.v1.ListTemplatesResponse> getListTemplatesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListTemplates",
      requestType = com.udb.core.notification.services.v1.ListTemplatesRequest.class,
      responseType = com.udb.core.notification.services.v1.ListTemplatesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.ListTemplatesRequest,
      com.udb.core.notification.services.v1.ListTemplatesResponse> getListTemplatesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.ListTemplatesRequest, com.udb.core.notification.services.v1.ListTemplatesResponse> getListTemplatesMethod;
    if ((getListTemplatesMethod = NotificationServiceGrpc.getListTemplatesMethod) == null) {
      synchronized (NotificationServiceGrpc.class) {
        if ((getListTemplatesMethod = NotificationServiceGrpc.getListTemplatesMethod) == null) {
          NotificationServiceGrpc.getListTemplatesMethod = getListTemplatesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.notification.services.v1.ListTemplatesRequest, com.udb.core.notification.services.v1.ListTemplatesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListTemplates"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.ListTemplatesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.ListTemplatesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NotificationServiceMethodDescriptorSupplier("ListTemplates"))
              .build();
        }
      }
    }
    return getListTemplatesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.GetDeliveryStatsRequest,
      com.udb.core.notification.services.v1.GetDeliveryStatsResponse> getGetDeliveryStatsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetDeliveryStats",
      requestType = com.udb.core.notification.services.v1.GetDeliveryStatsRequest.class,
      responseType = com.udb.core.notification.services.v1.GetDeliveryStatsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.GetDeliveryStatsRequest,
      com.udb.core.notification.services.v1.GetDeliveryStatsResponse> getGetDeliveryStatsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.GetDeliveryStatsRequest, com.udb.core.notification.services.v1.GetDeliveryStatsResponse> getGetDeliveryStatsMethod;
    if ((getGetDeliveryStatsMethod = NotificationServiceGrpc.getGetDeliveryStatsMethod) == null) {
      synchronized (NotificationServiceGrpc.class) {
        if ((getGetDeliveryStatsMethod = NotificationServiceGrpc.getGetDeliveryStatsMethod) == null) {
          NotificationServiceGrpc.getGetDeliveryStatsMethod = getGetDeliveryStatsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.notification.services.v1.GetDeliveryStatsRequest, com.udb.core.notification.services.v1.GetDeliveryStatsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetDeliveryStats"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.GetDeliveryStatsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.GetDeliveryStatsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NotificationServiceMethodDescriptorSupplier("GetDeliveryStats"))
              .build();
        }
      }
    }
    return getGetDeliveryStatsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.SetPreferenceRequest,
      com.udb.core.notification.services.v1.SetPreferenceResponse> getSetPreferenceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetPreference",
      requestType = com.udb.core.notification.services.v1.SetPreferenceRequest.class,
      responseType = com.udb.core.notification.services.v1.SetPreferenceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.SetPreferenceRequest,
      com.udb.core.notification.services.v1.SetPreferenceResponse> getSetPreferenceMethod() {
    io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.SetPreferenceRequest, com.udb.core.notification.services.v1.SetPreferenceResponse> getSetPreferenceMethod;
    if ((getSetPreferenceMethod = NotificationServiceGrpc.getSetPreferenceMethod) == null) {
      synchronized (NotificationServiceGrpc.class) {
        if ((getSetPreferenceMethod = NotificationServiceGrpc.getSetPreferenceMethod) == null) {
          NotificationServiceGrpc.getSetPreferenceMethod = getSetPreferenceMethod =
              io.grpc.MethodDescriptor.<com.udb.core.notification.services.v1.SetPreferenceRequest, com.udb.core.notification.services.v1.SetPreferenceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetPreference"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.SetPreferenceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.SetPreferenceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NotificationServiceMethodDescriptorSupplier("SetPreference"))
              .build();
        }
      }
    }
    return getSetPreferenceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.GetPreferenceRequest,
      com.udb.core.notification.services.v1.GetPreferenceResponse> getGetPreferenceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPreference",
      requestType = com.udb.core.notification.services.v1.GetPreferenceRequest.class,
      responseType = com.udb.core.notification.services.v1.GetPreferenceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.GetPreferenceRequest,
      com.udb.core.notification.services.v1.GetPreferenceResponse> getGetPreferenceMethod() {
    io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.GetPreferenceRequest, com.udb.core.notification.services.v1.GetPreferenceResponse> getGetPreferenceMethod;
    if ((getGetPreferenceMethod = NotificationServiceGrpc.getGetPreferenceMethod) == null) {
      synchronized (NotificationServiceGrpc.class) {
        if ((getGetPreferenceMethod = NotificationServiceGrpc.getGetPreferenceMethod) == null) {
          NotificationServiceGrpc.getGetPreferenceMethod = getGetPreferenceMethod =
              io.grpc.MethodDescriptor.<com.udb.core.notification.services.v1.GetPreferenceRequest, com.udb.core.notification.services.v1.GetPreferenceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetPreference"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.GetPreferenceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.GetPreferenceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NotificationServiceMethodDescriptorSupplier("GetPreference"))
              .build();
        }
      }
    }
    return getGetPreferenceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.ListPreferencesRequest,
      com.udb.core.notification.services.v1.ListPreferencesResponse> getListPreferencesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListPreferences",
      requestType = com.udb.core.notification.services.v1.ListPreferencesRequest.class,
      responseType = com.udb.core.notification.services.v1.ListPreferencesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.ListPreferencesRequest,
      com.udb.core.notification.services.v1.ListPreferencesResponse> getListPreferencesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.notification.services.v1.ListPreferencesRequest, com.udb.core.notification.services.v1.ListPreferencesResponse> getListPreferencesMethod;
    if ((getListPreferencesMethod = NotificationServiceGrpc.getListPreferencesMethod) == null) {
      synchronized (NotificationServiceGrpc.class) {
        if ((getListPreferencesMethod = NotificationServiceGrpc.getListPreferencesMethod) == null) {
          NotificationServiceGrpc.getListPreferencesMethod = getListPreferencesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.notification.services.v1.ListPreferencesRequest, com.udb.core.notification.services.v1.ListPreferencesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListPreferences"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.ListPreferencesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.notification.services.v1.ListPreferencesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new NotificationServiceMethodDescriptorSupplier("ListPreferences"))
              .build();
        }
      }
    }
    return getListPreferencesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static NotificationServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<NotificationServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<NotificationServiceStub>() {
        @java.lang.Override
        public NotificationServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new NotificationServiceStub(channel, callOptions);
        }
      };
    return NotificationServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static NotificationServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<NotificationServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<NotificationServiceBlockingV2Stub>() {
        @java.lang.Override
        public NotificationServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new NotificationServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return NotificationServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static NotificationServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<NotificationServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<NotificationServiceBlockingStub>() {
        @java.lang.Override
        public NotificationServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new NotificationServiceBlockingStub(channel, callOptions);
        }
      };
    return NotificationServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static NotificationServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<NotificationServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<NotificationServiceFutureStub>() {
        @java.lang.Override
        public NotificationServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new NotificationServiceFutureStub(channel, callOptions);
        }
      };
    return NotificationServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     * <pre>
     * Send a notification (or enqueue it for async delivery).
     * </pre>
     */
    default void sendNotification(com.udb.core.notification.services.v1.SendNotificationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.SendNotificationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendNotificationMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get delivery status for a specific log entry.
     * </pre>
     */
    default void getNotification(com.udb.core.notification.services.v1.GetNotificationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.GetNotificationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetNotificationMethod(), responseObserver);
    }

    /**
     * <pre>
     * List notification logs with rich filters.
     * </pre>
     */
    default void listNotifications(com.udb.core.notification.services.v1.ListNotificationsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.ListNotificationsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListNotificationsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Retry a failed notification.
     * </pre>
     */
    default void retryNotification(com.udb.core.notification.services.v1.RetryNotificationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.RetryNotificationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRetryNotificationMethod(), responseObserver);
    }

    /**
     * <pre>
     * Report the terminal per-channel delivery outcome for a sent notification.
     * Internal seam: the leader-elected delivery worker — or a provider webhook
     * bridge — reports queued/sent/delivered/failed; the handler upserts the
     * NotificationDeliveryAttempt row and emits `udb.notification.delivery.&lt;status&gt;.v1`.
     * </pre>
     */
    default void reportDelivery(com.udb.core.notification.services.v1.ReportDeliveryRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.ReportDeliveryResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReportDeliveryMethod(), responseObserver);
    }

    /**
     * <pre>
     * Upsert a notification template.
     * </pre>
     */
    default void upsertTemplate(com.udb.core.notification.services.v1.UpsertTemplateRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.UpsertTemplateResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpsertTemplateMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get a template by event_type + channel + locale.
     * </pre>
     */
    default void getTemplate(com.udb.core.notification.services.v1.GetTemplateRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.GetTemplateResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTemplateMethod(), responseObserver);
    }

    /**
     * <pre>
     * List all templates.
     * </pre>
     */
    default void listTemplates(com.udb.core.notification.services.v1.ListTemplatesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.ListTemplatesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListTemplatesMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get delivery statistics.
     * </pre>
     */
    default void getDeliveryStats(com.udb.core.notification.services.v1.GetDeliveryStatsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.GetDeliveryStatsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetDeliveryStatsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Set (upsert) a per-user channel/event opt-out preference.
     * </pre>
     */
    default void setPreference(com.udb.core.notification.services.v1.SetPreferenceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.SetPreferenceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetPreferenceMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get a single preference entry.
     * </pre>
     */
    default void getPreference(com.udb.core.notification.services.v1.GetPreferenceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.GetPreferenceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetPreferenceMethod(), responseObserver);
    }

    /**
     * <pre>
     * List all preferences for a user.
     * </pre>
     */
    default void listPreferences(com.udb.core.notification.services.v1.ListPreferencesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.ListPreferencesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListPreferencesMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service NotificationService.
   */
  public static abstract class NotificationServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return NotificationServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service NotificationService.
   */
  public static final class NotificationServiceStub
      extends io.grpc.stub.AbstractAsyncStub<NotificationServiceStub> {
    private NotificationServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected NotificationServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new NotificationServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Send a notification (or enqueue it for async delivery).
     * </pre>
     */
    public void sendNotification(com.udb.core.notification.services.v1.SendNotificationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.SendNotificationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendNotificationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get delivery status for a specific log entry.
     * </pre>
     */
    public void getNotification(com.udb.core.notification.services.v1.GetNotificationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.GetNotificationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetNotificationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List notification logs with rich filters.
     * </pre>
     */
    public void listNotifications(com.udb.core.notification.services.v1.ListNotificationsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.ListNotificationsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListNotificationsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Retry a failed notification.
     * </pre>
     */
    public void retryNotification(com.udb.core.notification.services.v1.RetryNotificationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.RetryNotificationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRetryNotificationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Report the terminal per-channel delivery outcome for a sent notification.
     * Internal seam: the leader-elected delivery worker — or a provider webhook
     * bridge — reports queued/sent/delivered/failed; the handler upserts the
     * NotificationDeliveryAttempt row and emits `udb.notification.delivery.&lt;status&gt;.v1`.
     * </pre>
     */
    public void reportDelivery(com.udb.core.notification.services.v1.ReportDeliveryRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.ReportDeliveryResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReportDeliveryMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Upsert a notification template.
     * </pre>
     */
    public void upsertTemplate(com.udb.core.notification.services.v1.UpsertTemplateRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.UpsertTemplateResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpsertTemplateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get a template by event_type + channel + locale.
     * </pre>
     */
    public void getTemplate(com.udb.core.notification.services.v1.GetTemplateRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.GetTemplateResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTemplateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List all templates.
     * </pre>
     */
    public void listTemplates(com.udb.core.notification.services.v1.ListTemplatesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.ListTemplatesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListTemplatesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get delivery statistics.
     * </pre>
     */
    public void getDeliveryStats(com.udb.core.notification.services.v1.GetDeliveryStatsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.GetDeliveryStatsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetDeliveryStatsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Set (upsert) a per-user channel/event opt-out preference.
     * </pre>
     */
    public void setPreference(com.udb.core.notification.services.v1.SetPreferenceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.SetPreferenceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetPreferenceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get a single preference entry.
     * </pre>
     */
    public void getPreference(com.udb.core.notification.services.v1.GetPreferenceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.GetPreferenceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetPreferenceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List all preferences for a user.
     * </pre>
     */
    public void listPreferences(com.udb.core.notification.services.v1.ListPreferencesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.ListPreferencesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListPreferencesMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service NotificationService.
   */
  public static final class NotificationServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<NotificationServiceBlockingV2Stub> {
    private NotificationServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected NotificationServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new NotificationServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Send a notification (or enqueue it for async delivery).
     * </pre>
     */
    public com.udb.core.notification.services.v1.SendNotificationResponse sendNotification(com.udb.core.notification.services.v1.SendNotificationRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSendNotificationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get delivery status for a specific log entry.
     * </pre>
     */
    public com.udb.core.notification.services.v1.GetNotificationResponse getNotification(com.udb.core.notification.services.v1.GetNotificationRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetNotificationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List notification logs with rich filters.
     * </pre>
     */
    public com.udb.core.notification.services.v1.ListNotificationsResponse listNotifications(com.udb.core.notification.services.v1.ListNotificationsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListNotificationsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Retry a failed notification.
     * </pre>
     */
    public com.udb.core.notification.services.v1.RetryNotificationResponse retryNotification(com.udb.core.notification.services.v1.RetryNotificationRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRetryNotificationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Report the terminal per-channel delivery outcome for a sent notification.
     * Internal seam: the leader-elected delivery worker — or a provider webhook
     * bridge — reports queued/sent/delivered/failed; the handler upserts the
     * NotificationDeliveryAttempt row and emits `udb.notification.delivery.&lt;status&gt;.v1`.
     * </pre>
     */
    public com.udb.core.notification.services.v1.ReportDeliveryResponse reportDelivery(com.udb.core.notification.services.v1.ReportDeliveryRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getReportDeliveryMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Upsert a notification template.
     * </pre>
     */
    public com.udb.core.notification.services.v1.UpsertTemplateResponse upsertTemplate(com.udb.core.notification.services.v1.UpsertTemplateRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getUpsertTemplateMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a template by event_type + channel + locale.
     * </pre>
     */
    public com.udb.core.notification.services.v1.GetTemplateResponse getTemplate(com.udb.core.notification.services.v1.GetTemplateRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetTemplateMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List all templates.
     * </pre>
     */
    public com.udb.core.notification.services.v1.ListTemplatesResponse listTemplates(com.udb.core.notification.services.v1.ListTemplatesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListTemplatesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get delivery statistics.
     * </pre>
     */
    public com.udb.core.notification.services.v1.GetDeliveryStatsResponse getDeliveryStats(com.udb.core.notification.services.v1.GetDeliveryStatsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetDeliveryStatsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Set (upsert) a per-user channel/event opt-out preference.
     * </pre>
     */
    public com.udb.core.notification.services.v1.SetPreferenceResponse setPreference(com.udb.core.notification.services.v1.SetPreferenceRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSetPreferenceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a single preference entry.
     * </pre>
     */
    public com.udb.core.notification.services.v1.GetPreferenceResponse getPreference(com.udb.core.notification.services.v1.GetPreferenceRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetPreferenceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List all preferences for a user.
     * </pre>
     */
    public com.udb.core.notification.services.v1.ListPreferencesResponse listPreferences(com.udb.core.notification.services.v1.ListPreferencesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListPreferencesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service NotificationService.
   */
  public static final class NotificationServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<NotificationServiceBlockingStub> {
    private NotificationServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected NotificationServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new NotificationServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Send a notification (or enqueue it for async delivery).
     * </pre>
     */
    public com.udb.core.notification.services.v1.SendNotificationResponse sendNotification(com.udb.core.notification.services.v1.SendNotificationRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendNotificationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get delivery status for a specific log entry.
     * </pre>
     */
    public com.udb.core.notification.services.v1.GetNotificationResponse getNotification(com.udb.core.notification.services.v1.GetNotificationRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetNotificationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List notification logs with rich filters.
     * </pre>
     */
    public com.udb.core.notification.services.v1.ListNotificationsResponse listNotifications(com.udb.core.notification.services.v1.ListNotificationsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListNotificationsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Retry a failed notification.
     * </pre>
     */
    public com.udb.core.notification.services.v1.RetryNotificationResponse retryNotification(com.udb.core.notification.services.v1.RetryNotificationRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRetryNotificationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Report the terminal per-channel delivery outcome for a sent notification.
     * Internal seam: the leader-elected delivery worker — or a provider webhook
     * bridge — reports queued/sent/delivered/failed; the handler upserts the
     * NotificationDeliveryAttempt row and emits `udb.notification.delivery.&lt;status&gt;.v1`.
     * </pre>
     */
    public com.udb.core.notification.services.v1.ReportDeliveryResponse reportDelivery(com.udb.core.notification.services.v1.ReportDeliveryRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReportDeliveryMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Upsert a notification template.
     * </pre>
     */
    public com.udb.core.notification.services.v1.UpsertTemplateResponse upsertTemplate(com.udb.core.notification.services.v1.UpsertTemplateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpsertTemplateMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a template by event_type + channel + locale.
     * </pre>
     */
    public com.udb.core.notification.services.v1.GetTemplateResponse getTemplate(com.udb.core.notification.services.v1.GetTemplateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTemplateMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List all templates.
     * </pre>
     */
    public com.udb.core.notification.services.v1.ListTemplatesResponse listTemplates(com.udb.core.notification.services.v1.ListTemplatesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListTemplatesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get delivery statistics.
     * </pre>
     */
    public com.udb.core.notification.services.v1.GetDeliveryStatsResponse getDeliveryStats(com.udb.core.notification.services.v1.GetDeliveryStatsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetDeliveryStatsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Set (upsert) a per-user channel/event opt-out preference.
     * </pre>
     */
    public com.udb.core.notification.services.v1.SetPreferenceResponse setPreference(com.udb.core.notification.services.v1.SetPreferenceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetPreferenceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a single preference entry.
     * </pre>
     */
    public com.udb.core.notification.services.v1.GetPreferenceResponse getPreference(com.udb.core.notification.services.v1.GetPreferenceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetPreferenceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List all preferences for a user.
     * </pre>
     */
    public com.udb.core.notification.services.v1.ListPreferencesResponse listPreferences(com.udb.core.notification.services.v1.ListPreferencesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListPreferencesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service NotificationService.
   */
  public static final class NotificationServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<NotificationServiceFutureStub> {
    private NotificationServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected NotificationServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new NotificationServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Send a notification (or enqueue it for async delivery).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.notification.services.v1.SendNotificationResponse> sendNotification(
        com.udb.core.notification.services.v1.SendNotificationRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendNotificationMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get delivery status for a specific log entry.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.notification.services.v1.GetNotificationResponse> getNotification(
        com.udb.core.notification.services.v1.GetNotificationRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetNotificationMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List notification logs with rich filters.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.notification.services.v1.ListNotificationsResponse> listNotifications(
        com.udb.core.notification.services.v1.ListNotificationsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListNotificationsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Retry a failed notification.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.notification.services.v1.RetryNotificationResponse> retryNotification(
        com.udb.core.notification.services.v1.RetryNotificationRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRetryNotificationMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Report the terminal per-channel delivery outcome for a sent notification.
     * Internal seam: the leader-elected delivery worker — or a provider webhook
     * bridge — reports queued/sent/delivered/failed; the handler upserts the
     * NotificationDeliveryAttempt row and emits `udb.notification.delivery.&lt;status&gt;.v1`.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.notification.services.v1.ReportDeliveryResponse> reportDelivery(
        com.udb.core.notification.services.v1.ReportDeliveryRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReportDeliveryMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Upsert a notification template.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.notification.services.v1.UpsertTemplateResponse> upsertTemplate(
        com.udb.core.notification.services.v1.UpsertTemplateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpsertTemplateMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get a template by event_type + channel + locale.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.notification.services.v1.GetTemplateResponse> getTemplate(
        com.udb.core.notification.services.v1.GetTemplateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTemplateMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List all templates.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.notification.services.v1.ListTemplatesResponse> listTemplates(
        com.udb.core.notification.services.v1.ListTemplatesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListTemplatesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get delivery statistics.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.notification.services.v1.GetDeliveryStatsResponse> getDeliveryStats(
        com.udb.core.notification.services.v1.GetDeliveryStatsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetDeliveryStatsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Set (upsert) a per-user channel/event opt-out preference.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.notification.services.v1.SetPreferenceResponse> setPreference(
        com.udb.core.notification.services.v1.SetPreferenceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetPreferenceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get a single preference entry.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.notification.services.v1.GetPreferenceResponse> getPreference(
        com.udb.core.notification.services.v1.GetPreferenceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetPreferenceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List all preferences for a user.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.notification.services.v1.ListPreferencesResponse> listPreferences(
        com.udb.core.notification.services.v1.ListPreferencesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListPreferencesMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SEND_NOTIFICATION = 0;
  private static final int METHODID_GET_NOTIFICATION = 1;
  private static final int METHODID_LIST_NOTIFICATIONS = 2;
  private static final int METHODID_RETRY_NOTIFICATION = 3;
  private static final int METHODID_REPORT_DELIVERY = 4;
  private static final int METHODID_UPSERT_TEMPLATE = 5;
  private static final int METHODID_GET_TEMPLATE = 6;
  private static final int METHODID_LIST_TEMPLATES = 7;
  private static final int METHODID_GET_DELIVERY_STATS = 8;
  private static final int METHODID_SET_PREFERENCE = 9;
  private static final int METHODID_GET_PREFERENCE = 10;
  private static final int METHODID_LIST_PREFERENCES = 11;

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
        case METHODID_SEND_NOTIFICATION:
          serviceImpl.sendNotification((com.udb.core.notification.services.v1.SendNotificationRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.SendNotificationResponse>) responseObserver);
          break;
        case METHODID_GET_NOTIFICATION:
          serviceImpl.getNotification((com.udb.core.notification.services.v1.GetNotificationRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.GetNotificationResponse>) responseObserver);
          break;
        case METHODID_LIST_NOTIFICATIONS:
          serviceImpl.listNotifications((com.udb.core.notification.services.v1.ListNotificationsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.ListNotificationsResponse>) responseObserver);
          break;
        case METHODID_RETRY_NOTIFICATION:
          serviceImpl.retryNotification((com.udb.core.notification.services.v1.RetryNotificationRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.RetryNotificationResponse>) responseObserver);
          break;
        case METHODID_REPORT_DELIVERY:
          serviceImpl.reportDelivery((com.udb.core.notification.services.v1.ReportDeliveryRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.ReportDeliveryResponse>) responseObserver);
          break;
        case METHODID_UPSERT_TEMPLATE:
          serviceImpl.upsertTemplate((com.udb.core.notification.services.v1.UpsertTemplateRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.UpsertTemplateResponse>) responseObserver);
          break;
        case METHODID_GET_TEMPLATE:
          serviceImpl.getTemplate((com.udb.core.notification.services.v1.GetTemplateRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.GetTemplateResponse>) responseObserver);
          break;
        case METHODID_LIST_TEMPLATES:
          serviceImpl.listTemplates((com.udb.core.notification.services.v1.ListTemplatesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.ListTemplatesResponse>) responseObserver);
          break;
        case METHODID_GET_DELIVERY_STATS:
          serviceImpl.getDeliveryStats((com.udb.core.notification.services.v1.GetDeliveryStatsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.GetDeliveryStatsResponse>) responseObserver);
          break;
        case METHODID_SET_PREFERENCE:
          serviceImpl.setPreference((com.udb.core.notification.services.v1.SetPreferenceRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.SetPreferenceResponse>) responseObserver);
          break;
        case METHODID_GET_PREFERENCE:
          serviceImpl.getPreference((com.udb.core.notification.services.v1.GetPreferenceRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.GetPreferenceResponse>) responseObserver);
          break;
        case METHODID_LIST_PREFERENCES:
          serviceImpl.listPreferences((com.udb.core.notification.services.v1.ListPreferencesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.notification.services.v1.ListPreferencesResponse>) responseObserver);
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
          getSendNotificationMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.notification.services.v1.SendNotificationRequest,
              com.udb.core.notification.services.v1.SendNotificationResponse>(
                service, METHODID_SEND_NOTIFICATION)))
        .addMethod(
          getGetNotificationMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.notification.services.v1.GetNotificationRequest,
              com.udb.core.notification.services.v1.GetNotificationResponse>(
                service, METHODID_GET_NOTIFICATION)))
        .addMethod(
          getListNotificationsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.notification.services.v1.ListNotificationsRequest,
              com.udb.core.notification.services.v1.ListNotificationsResponse>(
                service, METHODID_LIST_NOTIFICATIONS)))
        .addMethod(
          getRetryNotificationMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.notification.services.v1.RetryNotificationRequest,
              com.udb.core.notification.services.v1.RetryNotificationResponse>(
                service, METHODID_RETRY_NOTIFICATION)))
        .addMethod(
          getReportDeliveryMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.notification.services.v1.ReportDeliveryRequest,
              com.udb.core.notification.services.v1.ReportDeliveryResponse>(
                service, METHODID_REPORT_DELIVERY)))
        .addMethod(
          getUpsertTemplateMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.notification.services.v1.UpsertTemplateRequest,
              com.udb.core.notification.services.v1.UpsertTemplateResponse>(
                service, METHODID_UPSERT_TEMPLATE)))
        .addMethod(
          getGetTemplateMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.notification.services.v1.GetTemplateRequest,
              com.udb.core.notification.services.v1.GetTemplateResponse>(
                service, METHODID_GET_TEMPLATE)))
        .addMethod(
          getListTemplatesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.notification.services.v1.ListTemplatesRequest,
              com.udb.core.notification.services.v1.ListTemplatesResponse>(
                service, METHODID_LIST_TEMPLATES)))
        .addMethod(
          getGetDeliveryStatsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.notification.services.v1.GetDeliveryStatsRequest,
              com.udb.core.notification.services.v1.GetDeliveryStatsResponse>(
                service, METHODID_GET_DELIVERY_STATS)))
        .addMethod(
          getSetPreferenceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.notification.services.v1.SetPreferenceRequest,
              com.udb.core.notification.services.v1.SetPreferenceResponse>(
                service, METHODID_SET_PREFERENCE)))
        .addMethod(
          getGetPreferenceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.notification.services.v1.GetPreferenceRequest,
              com.udb.core.notification.services.v1.GetPreferenceResponse>(
                service, METHODID_GET_PREFERENCE)))
        .addMethod(
          getListPreferencesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.notification.services.v1.ListPreferencesRequest,
              com.udb.core.notification.services.v1.ListPreferencesResponse>(
                service, METHODID_LIST_PREFERENCES)))
        .build();
  }

  private static abstract class NotificationServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    NotificationServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.notification.services.v1.NotificationServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("NotificationService");
    }
  }

  private static final class NotificationServiceFileDescriptorSupplier
      extends NotificationServiceBaseDescriptorSupplier {
    NotificationServiceFileDescriptorSupplier() {}
  }

  private static final class NotificationServiceMethodDescriptorSupplier
      extends NotificationServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    NotificationServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (NotificationServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new NotificationServiceFileDescriptorSupplier())
              .addMethod(getSendNotificationMethod())
              .addMethod(getGetNotificationMethod())
              .addMethod(getListNotificationsMethod())
              .addMethod(getRetryNotificationMethod())
              .addMethod(getReportDeliveryMethod())
              .addMethod(getUpsertTemplateMethod())
              .addMethod(getGetTemplateMethod())
              .addMethod(getListTemplatesMethod())
              .addMethod(getGetDeliveryStatsMethod())
              .addMethod(getSetPreferenceMethod())
              .addMethod(getGetPreferenceMethod())
              .addMethod(getListPreferencesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
