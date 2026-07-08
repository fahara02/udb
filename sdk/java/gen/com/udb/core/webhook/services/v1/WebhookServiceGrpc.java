package com.udb.core.webhook.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * WebhookService (master-plan 9.4) — delivers tenant-scoped domain events to the
 * outside world. A tenant registers an external HTTPS endpoint with a topic
 * subscription; the leader-elected delivery worker consumes the tenant-bound CDC
 * stream, signs each event body with the per-endpoint secret (HMAC-SHA256 →
 * `X-Udb-Signature`), POSTs it with retries/backoff, dead-letters after
 * `max_attempts`, and journals every delivery. Every external target is run
 * through an SSRF guard at registration AND again at delivery (DNS rebinding):
 * https-only, never a private/loopback/link-local/CGNAT host.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class WebhookServiceGrpc {

  private WebhookServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.webhook.services.v1.WebhookService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.CreateEndpointRequest,
      com.udb.core.webhook.services.v1.CreateEndpointResponse> getCreateEndpointMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateEndpoint",
      requestType = com.udb.core.webhook.services.v1.CreateEndpointRequest.class,
      responseType = com.udb.core.webhook.services.v1.CreateEndpointResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.CreateEndpointRequest,
      com.udb.core.webhook.services.v1.CreateEndpointResponse> getCreateEndpointMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.CreateEndpointRequest, com.udb.core.webhook.services.v1.CreateEndpointResponse> getCreateEndpointMethod;
    if ((getCreateEndpointMethod = WebhookServiceGrpc.getCreateEndpointMethod) == null) {
      synchronized (WebhookServiceGrpc.class) {
        if ((getCreateEndpointMethod = WebhookServiceGrpc.getCreateEndpointMethod) == null) {
          WebhookServiceGrpc.getCreateEndpointMethod = getCreateEndpointMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webhook.services.v1.CreateEndpointRequest, com.udb.core.webhook.services.v1.CreateEndpointResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateEndpoint"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webhook.services.v1.CreateEndpointRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webhook.services.v1.CreateEndpointResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WebhookServiceMethodDescriptorSupplier("CreateEndpoint"))
              .build();
        }
      }
    }
    return getCreateEndpointMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.GetEndpointRequest,
      com.udb.core.webhook.services.v1.GetEndpointResponse> getGetEndpointMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetEndpoint",
      requestType = com.udb.core.webhook.services.v1.GetEndpointRequest.class,
      responseType = com.udb.core.webhook.services.v1.GetEndpointResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.GetEndpointRequest,
      com.udb.core.webhook.services.v1.GetEndpointResponse> getGetEndpointMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.GetEndpointRequest, com.udb.core.webhook.services.v1.GetEndpointResponse> getGetEndpointMethod;
    if ((getGetEndpointMethod = WebhookServiceGrpc.getGetEndpointMethod) == null) {
      synchronized (WebhookServiceGrpc.class) {
        if ((getGetEndpointMethod = WebhookServiceGrpc.getGetEndpointMethod) == null) {
          WebhookServiceGrpc.getGetEndpointMethod = getGetEndpointMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webhook.services.v1.GetEndpointRequest, com.udb.core.webhook.services.v1.GetEndpointResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetEndpoint"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webhook.services.v1.GetEndpointRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webhook.services.v1.GetEndpointResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WebhookServiceMethodDescriptorSupplier("GetEndpoint"))
              .build();
        }
      }
    }
    return getGetEndpointMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.ListEndpointsRequest,
      com.udb.core.webhook.services.v1.ListEndpointsResponse> getListEndpointsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListEndpoints",
      requestType = com.udb.core.webhook.services.v1.ListEndpointsRequest.class,
      responseType = com.udb.core.webhook.services.v1.ListEndpointsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.ListEndpointsRequest,
      com.udb.core.webhook.services.v1.ListEndpointsResponse> getListEndpointsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.ListEndpointsRequest, com.udb.core.webhook.services.v1.ListEndpointsResponse> getListEndpointsMethod;
    if ((getListEndpointsMethod = WebhookServiceGrpc.getListEndpointsMethod) == null) {
      synchronized (WebhookServiceGrpc.class) {
        if ((getListEndpointsMethod = WebhookServiceGrpc.getListEndpointsMethod) == null) {
          WebhookServiceGrpc.getListEndpointsMethod = getListEndpointsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webhook.services.v1.ListEndpointsRequest, com.udb.core.webhook.services.v1.ListEndpointsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListEndpoints"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webhook.services.v1.ListEndpointsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webhook.services.v1.ListEndpointsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WebhookServiceMethodDescriptorSupplier("ListEndpoints"))
              .build();
        }
      }
    }
    return getListEndpointsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.UpdateEndpointRequest,
      com.udb.core.webhook.services.v1.UpdateEndpointResponse> getUpdateEndpointMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateEndpoint",
      requestType = com.udb.core.webhook.services.v1.UpdateEndpointRequest.class,
      responseType = com.udb.core.webhook.services.v1.UpdateEndpointResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.UpdateEndpointRequest,
      com.udb.core.webhook.services.v1.UpdateEndpointResponse> getUpdateEndpointMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.UpdateEndpointRequest, com.udb.core.webhook.services.v1.UpdateEndpointResponse> getUpdateEndpointMethod;
    if ((getUpdateEndpointMethod = WebhookServiceGrpc.getUpdateEndpointMethod) == null) {
      synchronized (WebhookServiceGrpc.class) {
        if ((getUpdateEndpointMethod = WebhookServiceGrpc.getUpdateEndpointMethod) == null) {
          WebhookServiceGrpc.getUpdateEndpointMethod = getUpdateEndpointMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webhook.services.v1.UpdateEndpointRequest, com.udb.core.webhook.services.v1.UpdateEndpointResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateEndpoint"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webhook.services.v1.UpdateEndpointRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webhook.services.v1.UpdateEndpointResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WebhookServiceMethodDescriptorSupplier("UpdateEndpoint"))
              .build();
        }
      }
    }
    return getUpdateEndpointMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.DeleteEndpointRequest,
      com.udb.core.webhook.services.v1.DeleteEndpointResponse> getDeleteEndpointMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteEndpoint",
      requestType = com.udb.core.webhook.services.v1.DeleteEndpointRequest.class,
      responseType = com.udb.core.webhook.services.v1.DeleteEndpointResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.DeleteEndpointRequest,
      com.udb.core.webhook.services.v1.DeleteEndpointResponse> getDeleteEndpointMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.DeleteEndpointRequest, com.udb.core.webhook.services.v1.DeleteEndpointResponse> getDeleteEndpointMethod;
    if ((getDeleteEndpointMethod = WebhookServiceGrpc.getDeleteEndpointMethod) == null) {
      synchronized (WebhookServiceGrpc.class) {
        if ((getDeleteEndpointMethod = WebhookServiceGrpc.getDeleteEndpointMethod) == null) {
          WebhookServiceGrpc.getDeleteEndpointMethod = getDeleteEndpointMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webhook.services.v1.DeleteEndpointRequest, com.udb.core.webhook.services.v1.DeleteEndpointResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteEndpoint"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webhook.services.v1.DeleteEndpointRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webhook.services.v1.DeleteEndpointResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WebhookServiceMethodDescriptorSupplier("DeleteEndpoint"))
              .build();
        }
      }
    }
    return getDeleteEndpointMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.ListDeliveriesRequest,
      com.udb.core.webhook.services.v1.ListDeliveriesResponse> getListDeliveriesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListDeliveries",
      requestType = com.udb.core.webhook.services.v1.ListDeliveriesRequest.class,
      responseType = com.udb.core.webhook.services.v1.ListDeliveriesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.ListDeliveriesRequest,
      com.udb.core.webhook.services.v1.ListDeliveriesResponse> getListDeliveriesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.webhook.services.v1.ListDeliveriesRequest, com.udb.core.webhook.services.v1.ListDeliveriesResponse> getListDeliveriesMethod;
    if ((getListDeliveriesMethod = WebhookServiceGrpc.getListDeliveriesMethod) == null) {
      synchronized (WebhookServiceGrpc.class) {
        if ((getListDeliveriesMethod = WebhookServiceGrpc.getListDeliveriesMethod) == null) {
          WebhookServiceGrpc.getListDeliveriesMethod = getListDeliveriesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.webhook.services.v1.ListDeliveriesRequest, com.udb.core.webhook.services.v1.ListDeliveriesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListDeliveries"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webhook.services.v1.ListDeliveriesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.webhook.services.v1.ListDeliveriesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WebhookServiceMethodDescriptorSupplier("ListDeliveries"))
              .build();
        }
      }
    }
    return getListDeliveriesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static WebhookServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WebhookServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WebhookServiceStub>() {
        @java.lang.Override
        public WebhookServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WebhookServiceStub(channel, callOptions);
        }
      };
    return WebhookServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static WebhookServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WebhookServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WebhookServiceBlockingV2Stub>() {
        @java.lang.Override
        public WebhookServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WebhookServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return WebhookServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static WebhookServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WebhookServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WebhookServiceBlockingStub>() {
        @java.lang.Override
        public WebhookServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WebhookServiceBlockingStub(channel, callOptions);
        }
      };
    return WebhookServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static WebhookServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WebhookServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WebhookServiceFutureStub>() {
        @java.lang.Override
        public WebhookServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WebhookServiceFutureStub(channel, callOptions);
        }
      };
    return WebhookServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * WebhookService (master-plan 9.4) — delivers tenant-scoped domain events to the
   * outside world. A tenant registers an external HTTPS endpoint with a topic
   * subscription; the leader-elected delivery worker consumes the tenant-bound CDC
   * stream, signs each event body with the per-endpoint secret (HMAC-SHA256 →
   * `X-Udb-Signature`), POSTs it with retries/backoff, dead-letters after
   * `max_attempts`, and journals every delivery. Every external target is run
   * through an SSRF guard at registration AND again at delivery (DNS rebinding):
   * https-only, never a private/loopback/link-local/CGNAT host.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Register an external webhook endpoint. The target URL is SSRF-validated
     * (https-only, no private/loopback/link-local/CGNAT host). The per-endpoint
     * signing secret is returned exactly once in the response and never again.
     * </pre>
     */
    default void createEndpoint(com.udb.core.webhook.services.v1.CreateEndpointRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.CreateEndpointResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateEndpointMethod(), responseObserver);
    }

    /**
     * <pre>
     * Fetch one webhook endpoint (the signing secret is NEVER returned on read).
     * </pre>
     */
    default void getEndpoint(com.udb.core.webhook.services.v1.GetEndpointRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.GetEndpointResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetEndpointMethod(), responseObserver);
    }

    /**
     * <pre>
     * List a tenant's webhook endpoints (signing secrets are NEVER returned).
     * </pre>
     */
    default void listEndpoints(com.udb.core.webhook.services.v1.ListEndpointsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.ListEndpointsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListEndpointsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Update an endpoint. A changed URL is SSRF-revalidated before it is stored.
     * </pre>
     */
    default void updateEndpoint(com.udb.core.webhook.services.v1.UpdateEndpointRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.UpdateEndpointResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateEndpointMethod(), responseObserver);
    }

    /**
     * <pre>
     * Delete (soft) a webhook endpoint; no further events are delivered to it.
     * </pre>
     */
    default void deleteEndpoint(com.udb.core.webhook.services.v1.DeleteEndpointRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.DeleteEndpointResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteEndpointMethod(), responseObserver);
    }

    /**
     * <pre>
     * List the delivery journal for a tenant, optionally narrowed to one endpoint
     * or one delivery status.
     * </pre>
     */
    default void listDeliveries(com.udb.core.webhook.services.v1.ListDeliveriesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.ListDeliveriesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListDeliveriesMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service WebhookService.
   * <pre>
   * WebhookService (master-plan 9.4) — delivers tenant-scoped domain events to the
   * outside world. A tenant registers an external HTTPS endpoint with a topic
   * subscription; the leader-elected delivery worker consumes the tenant-bound CDC
   * stream, signs each event body with the per-endpoint secret (HMAC-SHA256 →
   * `X-Udb-Signature`), POSTs it with retries/backoff, dead-letters after
   * `max_attempts`, and journals every delivery. Every external target is run
   * through an SSRF guard at registration AND again at delivery (DNS rebinding):
   * https-only, never a private/loopback/link-local/CGNAT host.
   * </pre>
   */
  public static abstract class WebhookServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return WebhookServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service WebhookService.
   * <pre>
   * WebhookService (master-plan 9.4) — delivers tenant-scoped domain events to the
   * outside world. A tenant registers an external HTTPS endpoint with a topic
   * subscription; the leader-elected delivery worker consumes the tenant-bound CDC
   * stream, signs each event body with the per-endpoint secret (HMAC-SHA256 →
   * `X-Udb-Signature`), POSTs it with retries/backoff, dead-letters after
   * `max_attempts`, and journals every delivery. Every external target is run
   * through an SSRF guard at registration AND again at delivery (DNS rebinding):
   * https-only, never a private/loopback/link-local/CGNAT host.
   * </pre>
   */
  public static final class WebhookServiceStub
      extends io.grpc.stub.AbstractAsyncStub<WebhookServiceStub> {
    private WebhookServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WebhookServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WebhookServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register an external webhook endpoint. The target URL is SSRF-validated
     * (https-only, no private/loopback/link-local/CGNAT host). The per-endpoint
     * signing secret is returned exactly once in the response and never again.
     * </pre>
     */
    public void createEndpoint(com.udb.core.webhook.services.v1.CreateEndpointRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.CreateEndpointResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateEndpointMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Fetch one webhook endpoint (the signing secret is NEVER returned on read).
     * </pre>
     */
    public void getEndpoint(com.udb.core.webhook.services.v1.GetEndpointRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.GetEndpointResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetEndpointMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List a tenant's webhook endpoints (signing secrets are NEVER returned).
     * </pre>
     */
    public void listEndpoints(com.udb.core.webhook.services.v1.ListEndpointsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.ListEndpointsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListEndpointsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Update an endpoint. A changed URL is SSRF-revalidated before it is stored.
     * </pre>
     */
    public void updateEndpoint(com.udb.core.webhook.services.v1.UpdateEndpointRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.UpdateEndpointResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateEndpointMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Delete (soft) a webhook endpoint; no further events are delivered to it.
     * </pre>
     */
    public void deleteEndpoint(com.udb.core.webhook.services.v1.DeleteEndpointRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.DeleteEndpointResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteEndpointMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List the delivery journal for a tenant, optionally narrowed to one endpoint
     * or one delivery status.
     * </pre>
     */
    public void listDeliveries(com.udb.core.webhook.services.v1.ListDeliveriesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.ListDeliveriesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListDeliveriesMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service WebhookService.
   * <pre>
   * WebhookService (master-plan 9.4) — delivers tenant-scoped domain events to the
   * outside world. A tenant registers an external HTTPS endpoint with a topic
   * subscription; the leader-elected delivery worker consumes the tenant-bound CDC
   * stream, signs each event body with the per-endpoint secret (HMAC-SHA256 →
   * `X-Udb-Signature`), POSTs it with retries/backoff, dead-letters after
   * `max_attempts`, and journals every delivery. Every external target is run
   * through an SSRF guard at registration AND again at delivery (DNS rebinding):
   * https-only, never a private/loopback/link-local/CGNAT host.
   * </pre>
   */
  public static final class WebhookServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<WebhookServiceBlockingV2Stub> {
    private WebhookServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WebhookServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WebhookServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Register an external webhook endpoint. The target URL is SSRF-validated
     * (https-only, no private/loopback/link-local/CGNAT host). The per-endpoint
     * signing secret is returned exactly once in the response and never again.
     * </pre>
     */
    public com.udb.core.webhook.services.v1.CreateEndpointResponse createEndpoint(com.udb.core.webhook.services.v1.CreateEndpointRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreateEndpointMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch one webhook endpoint (the signing secret is NEVER returned on read).
     * </pre>
     */
    public com.udb.core.webhook.services.v1.GetEndpointResponse getEndpoint(com.udb.core.webhook.services.v1.GetEndpointRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetEndpointMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List a tenant's webhook endpoints (signing secrets are NEVER returned).
     * </pre>
     */
    public com.udb.core.webhook.services.v1.ListEndpointsResponse listEndpoints(com.udb.core.webhook.services.v1.ListEndpointsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListEndpointsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Update an endpoint. A changed URL is SSRF-revalidated before it is stored.
     * </pre>
     */
    public com.udb.core.webhook.services.v1.UpdateEndpointResponse updateEndpoint(com.udb.core.webhook.services.v1.UpdateEndpointRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getUpdateEndpointMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete (soft) a webhook endpoint; no further events are delivered to it.
     * </pre>
     */
    public com.udb.core.webhook.services.v1.DeleteEndpointResponse deleteEndpoint(com.udb.core.webhook.services.v1.DeleteEndpointRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeleteEndpointMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List the delivery journal for a tenant, optionally narrowed to one endpoint
     * or one delivery status.
     * </pre>
     */
    public com.udb.core.webhook.services.v1.ListDeliveriesResponse listDeliveries(com.udb.core.webhook.services.v1.ListDeliveriesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListDeliveriesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service WebhookService.
   * <pre>
   * WebhookService (master-plan 9.4) — delivers tenant-scoped domain events to the
   * outside world. A tenant registers an external HTTPS endpoint with a topic
   * subscription; the leader-elected delivery worker consumes the tenant-bound CDC
   * stream, signs each event body with the per-endpoint secret (HMAC-SHA256 →
   * `X-Udb-Signature`), POSTs it with retries/backoff, dead-letters after
   * `max_attempts`, and journals every delivery. Every external target is run
   * through an SSRF guard at registration AND again at delivery (DNS rebinding):
   * https-only, never a private/loopback/link-local/CGNAT host.
   * </pre>
   */
  public static final class WebhookServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<WebhookServiceBlockingStub> {
    private WebhookServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WebhookServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WebhookServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register an external webhook endpoint. The target URL is SSRF-validated
     * (https-only, no private/loopback/link-local/CGNAT host). The per-endpoint
     * signing secret is returned exactly once in the response and never again.
     * </pre>
     */
    public com.udb.core.webhook.services.v1.CreateEndpointResponse createEndpoint(com.udb.core.webhook.services.v1.CreateEndpointRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateEndpointMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch one webhook endpoint (the signing secret is NEVER returned on read).
     * </pre>
     */
    public com.udb.core.webhook.services.v1.GetEndpointResponse getEndpoint(com.udb.core.webhook.services.v1.GetEndpointRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetEndpointMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List a tenant's webhook endpoints (signing secrets are NEVER returned).
     * </pre>
     */
    public com.udb.core.webhook.services.v1.ListEndpointsResponse listEndpoints(com.udb.core.webhook.services.v1.ListEndpointsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListEndpointsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Update an endpoint. A changed URL is SSRF-revalidated before it is stored.
     * </pre>
     */
    public com.udb.core.webhook.services.v1.UpdateEndpointResponse updateEndpoint(com.udb.core.webhook.services.v1.UpdateEndpointRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateEndpointMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete (soft) a webhook endpoint; no further events are delivered to it.
     * </pre>
     */
    public com.udb.core.webhook.services.v1.DeleteEndpointResponse deleteEndpoint(com.udb.core.webhook.services.v1.DeleteEndpointRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteEndpointMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List the delivery journal for a tenant, optionally narrowed to one endpoint
     * or one delivery status.
     * </pre>
     */
    public com.udb.core.webhook.services.v1.ListDeliveriesResponse listDeliveries(com.udb.core.webhook.services.v1.ListDeliveriesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListDeliveriesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service WebhookService.
   * <pre>
   * WebhookService (master-plan 9.4) — delivers tenant-scoped domain events to the
   * outside world. A tenant registers an external HTTPS endpoint with a topic
   * subscription; the leader-elected delivery worker consumes the tenant-bound CDC
   * stream, signs each event body with the per-endpoint secret (HMAC-SHA256 →
   * `X-Udb-Signature`), POSTs it with retries/backoff, dead-letters after
   * `max_attempts`, and journals every delivery. Every external target is run
   * through an SSRF guard at registration AND again at delivery (DNS rebinding):
   * https-only, never a private/loopback/link-local/CGNAT host.
   * </pre>
   */
  public static final class WebhookServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<WebhookServiceFutureStub> {
    private WebhookServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WebhookServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WebhookServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register an external webhook endpoint. The target URL is SSRF-validated
     * (https-only, no private/loopback/link-local/CGNAT host). The per-endpoint
     * signing secret is returned exactly once in the response and never again.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webhook.services.v1.CreateEndpointResponse> createEndpoint(
        com.udb.core.webhook.services.v1.CreateEndpointRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateEndpointMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Fetch one webhook endpoint (the signing secret is NEVER returned on read).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webhook.services.v1.GetEndpointResponse> getEndpoint(
        com.udb.core.webhook.services.v1.GetEndpointRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetEndpointMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List a tenant's webhook endpoints (signing secrets are NEVER returned).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webhook.services.v1.ListEndpointsResponse> listEndpoints(
        com.udb.core.webhook.services.v1.ListEndpointsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListEndpointsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Update an endpoint. A changed URL is SSRF-revalidated before it is stored.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webhook.services.v1.UpdateEndpointResponse> updateEndpoint(
        com.udb.core.webhook.services.v1.UpdateEndpointRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateEndpointMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Delete (soft) a webhook endpoint; no further events are delivered to it.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webhook.services.v1.DeleteEndpointResponse> deleteEndpoint(
        com.udb.core.webhook.services.v1.DeleteEndpointRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteEndpointMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List the delivery journal for a tenant, optionally narrowed to one endpoint
     * or one delivery status.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.webhook.services.v1.ListDeliveriesResponse> listDeliveries(
        com.udb.core.webhook.services.v1.ListDeliveriesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListDeliveriesMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_ENDPOINT = 0;
  private static final int METHODID_GET_ENDPOINT = 1;
  private static final int METHODID_LIST_ENDPOINTS = 2;
  private static final int METHODID_UPDATE_ENDPOINT = 3;
  private static final int METHODID_DELETE_ENDPOINT = 4;
  private static final int METHODID_LIST_DELIVERIES = 5;

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
        case METHODID_CREATE_ENDPOINT:
          serviceImpl.createEndpoint((com.udb.core.webhook.services.v1.CreateEndpointRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.CreateEndpointResponse>) responseObserver);
          break;
        case METHODID_GET_ENDPOINT:
          serviceImpl.getEndpoint((com.udb.core.webhook.services.v1.GetEndpointRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.GetEndpointResponse>) responseObserver);
          break;
        case METHODID_LIST_ENDPOINTS:
          serviceImpl.listEndpoints((com.udb.core.webhook.services.v1.ListEndpointsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.ListEndpointsResponse>) responseObserver);
          break;
        case METHODID_UPDATE_ENDPOINT:
          serviceImpl.updateEndpoint((com.udb.core.webhook.services.v1.UpdateEndpointRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.UpdateEndpointResponse>) responseObserver);
          break;
        case METHODID_DELETE_ENDPOINT:
          serviceImpl.deleteEndpoint((com.udb.core.webhook.services.v1.DeleteEndpointRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.DeleteEndpointResponse>) responseObserver);
          break;
        case METHODID_LIST_DELIVERIES:
          serviceImpl.listDeliveries((com.udb.core.webhook.services.v1.ListDeliveriesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.webhook.services.v1.ListDeliveriesResponse>) responseObserver);
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
          getCreateEndpointMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webhook.services.v1.CreateEndpointRequest,
              com.udb.core.webhook.services.v1.CreateEndpointResponse>(
                service, METHODID_CREATE_ENDPOINT)))
        .addMethod(
          getGetEndpointMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webhook.services.v1.GetEndpointRequest,
              com.udb.core.webhook.services.v1.GetEndpointResponse>(
                service, METHODID_GET_ENDPOINT)))
        .addMethod(
          getListEndpointsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webhook.services.v1.ListEndpointsRequest,
              com.udb.core.webhook.services.v1.ListEndpointsResponse>(
                service, METHODID_LIST_ENDPOINTS)))
        .addMethod(
          getUpdateEndpointMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webhook.services.v1.UpdateEndpointRequest,
              com.udb.core.webhook.services.v1.UpdateEndpointResponse>(
                service, METHODID_UPDATE_ENDPOINT)))
        .addMethod(
          getDeleteEndpointMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webhook.services.v1.DeleteEndpointRequest,
              com.udb.core.webhook.services.v1.DeleteEndpointResponse>(
                service, METHODID_DELETE_ENDPOINT)))
        .addMethod(
          getListDeliveriesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.webhook.services.v1.ListDeliveriesRequest,
              com.udb.core.webhook.services.v1.ListDeliveriesResponse>(
                service, METHODID_LIST_DELIVERIES)))
        .build();
  }

  private static abstract class WebhookServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    WebhookServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.webhook.services.v1.WebhookServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("WebhookService");
    }
  }

  private static final class WebhookServiceFileDescriptorSupplier
      extends WebhookServiceBaseDescriptorSupplier {
    WebhookServiceFileDescriptorSupplier() {}
  }

  private static final class WebhookServiceMethodDescriptorSupplier
      extends WebhookServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    WebhookServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (WebhookServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new WebhookServiceFileDescriptorSupplier())
              .addMethod(getCreateEndpointMethod())
              .addMethod(getGetEndpointMethod())
              .addMethod(getListEndpointsMethod())
              .addMethod(getUpdateEndpointMethod())
              .addMethod(getDeleteEndpointMethod())
              .addMethod(getListDeliveriesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
