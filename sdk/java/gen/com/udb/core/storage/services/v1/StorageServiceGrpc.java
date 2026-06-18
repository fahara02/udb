package com.udb.core.storage.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class StorageServiceGrpc {

  private StorageServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.storage.services.v1.StorageService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.RegisterUploadRequest,
      com.udb.core.storage.services.v1.RegisterUploadResponse> getRegisterUploadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RegisterUpload",
      requestType = com.udb.core.storage.services.v1.RegisterUploadRequest.class,
      responseType = com.udb.core.storage.services.v1.RegisterUploadResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.RegisterUploadRequest,
      com.udb.core.storage.services.v1.RegisterUploadResponse> getRegisterUploadMethod() {
    io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.RegisterUploadRequest, com.udb.core.storage.services.v1.RegisterUploadResponse> getRegisterUploadMethod;
    if ((getRegisterUploadMethod = StorageServiceGrpc.getRegisterUploadMethod) == null) {
      synchronized (StorageServiceGrpc.class) {
        if ((getRegisterUploadMethod = StorageServiceGrpc.getRegisterUploadMethod) == null) {
          StorageServiceGrpc.getRegisterUploadMethod = getRegisterUploadMethod =
              io.grpc.MethodDescriptor.<com.udb.core.storage.services.v1.RegisterUploadRequest, com.udb.core.storage.services.v1.RegisterUploadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RegisterUpload"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.storage.services.v1.RegisterUploadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.storage.services.v1.RegisterUploadResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorageServiceMethodDescriptorSupplier("RegisterUpload"))
              .build();
        }
      }
    }
    return getRegisterUploadMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.FinalizeUploadRequest,
      com.udb.core.storage.services.v1.FinalizeUploadResponse> getFinalizeUploadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "FinalizeUpload",
      requestType = com.udb.core.storage.services.v1.FinalizeUploadRequest.class,
      responseType = com.udb.core.storage.services.v1.FinalizeUploadResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.FinalizeUploadRequest,
      com.udb.core.storage.services.v1.FinalizeUploadResponse> getFinalizeUploadMethod() {
    io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.FinalizeUploadRequest, com.udb.core.storage.services.v1.FinalizeUploadResponse> getFinalizeUploadMethod;
    if ((getFinalizeUploadMethod = StorageServiceGrpc.getFinalizeUploadMethod) == null) {
      synchronized (StorageServiceGrpc.class) {
        if ((getFinalizeUploadMethod = StorageServiceGrpc.getFinalizeUploadMethod) == null) {
          StorageServiceGrpc.getFinalizeUploadMethod = getFinalizeUploadMethod =
              io.grpc.MethodDescriptor.<com.udb.core.storage.services.v1.FinalizeUploadRequest, com.udb.core.storage.services.v1.FinalizeUploadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "FinalizeUpload"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.storage.services.v1.FinalizeUploadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.storage.services.v1.FinalizeUploadResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorageServiceMethodDescriptorSupplier("FinalizeUpload"))
              .build();
        }
      }
    }
    return getFinalizeUploadMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.GetDownloadUrlRequest,
      com.udb.core.storage.services.v1.GetDownloadUrlResponse> getGetDownloadUrlMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetDownloadUrl",
      requestType = com.udb.core.storage.services.v1.GetDownloadUrlRequest.class,
      responseType = com.udb.core.storage.services.v1.GetDownloadUrlResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.GetDownloadUrlRequest,
      com.udb.core.storage.services.v1.GetDownloadUrlResponse> getGetDownloadUrlMethod() {
    io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.GetDownloadUrlRequest, com.udb.core.storage.services.v1.GetDownloadUrlResponse> getGetDownloadUrlMethod;
    if ((getGetDownloadUrlMethod = StorageServiceGrpc.getGetDownloadUrlMethod) == null) {
      synchronized (StorageServiceGrpc.class) {
        if ((getGetDownloadUrlMethod = StorageServiceGrpc.getGetDownloadUrlMethod) == null) {
          StorageServiceGrpc.getGetDownloadUrlMethod = getGetDownloadUrlMethod =
              io.grpc.MethodDescriptor.<com.udb.core.storage.services.v1.GetDownloadUrlRequest, com.udb.core.storage.services.v1.GetDownloadUrlResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetDownloadUrl"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.storage.services.v1.GetDownloadUrlRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.storage.services.v1.GetDownloadUrlResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorageServiceMethodDescriptorSupplier("GetDownloadUrl"))
              .build();
        }
      }
    }
    return getGetDownloadUrlMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.DownloadFileRequest,
      com.udb.core.storage.services.v1.DownloadFileChunk> getDownloadFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DownloadFile",
      requestType = com.udb.core.storage.services.v1.DownloadFileRequest.class,
      responseType = com.udb.core.storage.services.v1.DownloadFileChunk.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.DownloadFileRequest,
      com.udb.core.storage.services.v1.DownloadFileChunk> getDownloadFileMethod() {
    io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.DownloadFileRequest, com.udb.core.storage.services.v1.DownloadFileChunk> getDownloadFileMethod;
    if ((getDownloadFileMethod = StorageServiceGrpc.getDownloadFileMethod) == null) {
      synchronized (StorageServiceGrpc.class) {
        if ((getDownloadFileMethod = StorageServiceGrpc.getDownloadFileMethod) == null) {
          StorageServiceGrpc.getDownloadFileMethod = getDownloadFileMethod =
              io.grpc.MethodDescriptor.<com.udb.core.storage.services.v1.DownloadFileRequest, com.udb.core.storage.services.v1.DownloadFileChunk>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DownloadFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.storage.services.v1.DownloadFileRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.storage.services.v1.DownloadFileChunk.getDefaultInstance()))
              .setSchemaDescriptor(new StorageServiceMethodDescriptorSupplier("DownloadFile"))
              .build();
        }
      }
    }
    return getDownloadFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.GetFileRequest,
      com.udb.core.storage.services.v1.GetFileResponse> getGetFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetFile",
      requestType = com.udb.core.storage.services.v1.GetFileRequest.class,
      responseType = com.udb.core.storage.services.v1.GetFileResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.GetFileRequest,
      com.udb.core.storage.services.v1.GetFileResponse> getGetFileMethod() {
    io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.GetFileRequest, com.udb.core.storage.services.v1.GetFileResponse> getGetFileMethod;
    if ((getGetFileMethod = StorageServiceGrpc.getGetFileMethod) == null) {
      synchronized (StorageServiceGrpc.class) {
        if ((getGetFileMethod = StorageServiceGrpc.getGetFileMethod) == null) {
          StorageServiceGrpc.getGetFileMethod = getGetFileMethod =
              io.grpc.MethodDescriptor.<com.udb.core.storage.services.v1.GetFileRequest, com.udb.core.storage.services.v1.GetFileResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.storage.services.v1.GetFileRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.storage.services.v1.GetFileResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorageServiceMethodDescriptorSupplier("GetFile"))
              .build();
        }
      }
    }
    return getGetFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.UpdateFileRequest,
      com.udb.core.storage.services.v1.UpdateFileResponse> getUpdateFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateFile",
      requestType = com.udb.core.storage.services.v1.UpdateFileRequest.class,
      responseType = com.udb.core.storage.services.v1.UpdateFileResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.UpdateFileRequest,
      com.udb.core.storage.services.v1.UpdateFileResponse> getUpdateFileMethod() {
    io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.UpdateFileRequest, com.udb.core.storage.services.v1.UpdateFileResponse> getUpdateFileMethod;
    if ((getUpdateFileMethod = StorageServiceGrpc.getUpdateFileMethod) == null) {
      synchronized (StorageServiceGrpc.class) {
        if ((getUpdateFileMethod = StorageServiceGrpc.getUpdateFileMethod) == null) {
          StorageServiceGrpc.getUpdateFileMethod = getUpdateFileMethod =
              io.grpc.MethodDescriptor.<com.udb.core.storage.services.v1.UpdateFileRequest, com.udb.core.storage.services.v1.UpdateFileResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.storage.services.v1.UpdateFileRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.storage.services.v1.UpdateFileResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorageServiceMethodDescriptorSupplier("UpdateFile"))
              .build();
        }
      }
    }
    return getUpdateFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.DeleteFileRequest,
      com.udb.core.storage.services.v1.DeleteFileResponse> getDeleteFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteFile",
      requestType = com.udb.core.storage.services.v1.DeleteFileRequest.class,
      responseType = com.udb.core.storage.services.v1.DeleteFileResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.DeleteFileRequest,
      com.udb.core.storage.services.v1.DeleteFileResponse> getDeleteFileMethod() {
    io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.DeleteFileRequest, com.udb.core.storage.services.v1.DeleteFileResponse> getDeleteFileMethod;
    if ((getDeleteFileMethod = StorageServiceGrpc.getDeleteFileMethod) == null) {
      synchronized (StorageServiceGrpc.class) {
        if ((getDeleteFileMethod = StorageServiceGrpc.getDeleteFileMethod) == null) {
          StorageServiceGrpc.getDeleteFileMethod = getDeleteFileMethod =
              io.grpc.MethodDescriptor.<com.udb.core.storage.services.v1.DeleteFileRequest, com.udb.core.storage.services.v1.DeleteFileResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.storage.services.v1.DeleteFileRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.storage.services.v1.DeleteFileResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorageServiceMethodDescriptorSupplier("DeleteFile"))
              .build();
        }
      }
    }
    return getDeleteFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.ListFilesRequest,
      com.udb.core.storage.services.v1.ListFilesResponse> getListFilesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListFiles",
      requestType = com.udb.core.storage.services.v1.ListFilesRequest.class,
      responseType = com.udb.core.storage.services.v1.ListFilesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.ListFilesRequest,
      com.udb.core.storage.services.v1.ListFilesResponse> getListFilesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.storage.services.v1.ListFilesRequest, com.udb.core.storage.services.v1.ListFilesResponse> getListFilesMethod;
    if ((getListFilesMethod = StorageServiceGrpc.getListFilesMethod) == null) {
      synchronized (StorageServiceGrpc.class) {
        if ((getListFilesMethod = StorageServiceGrpc.getListFilesMethod) == null) {
          StorageServiceGrpc.getListFilesMethod = getListFilesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.storage.services.v1.ListFilesRequest, com.udb.core.storage.services.v1.ListFilesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListFiles"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.storage.services.v1.ListFilesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.storage.services.v1.ListFilesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorageServiceMethodDescriptorSupplier("ListFiles"))
              .build();
        }
      }
    }
    return getListFilesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static StorageServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<StorageServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<StorageServiceStub>() {
        @java.lang.Override
        public StorageServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new StorageServiceStub(channel, callOptions);
        }
      };
    return StorageServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static StorageServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<StorageServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<StorageServiceBlockingV2Stub>() {
        @java.lang.Override
        public StorageServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new StorageServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return StorageServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static StorageServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<StorageServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<StorageServiceBlockingStub>() {
        @java.lang.Override
        public StorageServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new StorageServiceBlockingStub(channel, callOptions);
        }
      };
    return StorageServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static StorageServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<StorageServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<StorageServiceFutureStub>() {
        @java.lang.Override
        public StorageServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new StorageServiceFutureStub(channel, callOptions);
        }
      };
    return StorageServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     * <pre>
     * Register a new upload and obtain a pre-signed upload URL
     * </pre>
     */
    default void registerUpload(com.udb.core.storage.services.v1.RegisterUploadRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.RegisterUploadResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRegisterUploadMethod(), responseObserver);
    }

    /**
     * <pre>
     * Finalize an upload after the object has been written to the store
     * </pre>
     */
    default void finalizeUpload(com.udb.core.storage.services.v1.FinalizeUploadRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.FinalizeUploadResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getFinalizeUploadMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get a pre-signed download URL for a file
     * </pre>
     */
    default void getDownloadUrl(com.udb.core.storage.services.v1.GetDownloadUrlRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.GetDownloadUrlResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetDownloadUrlMethod(), responseObserver);
    }

    /**
     * <pre>
     * Stream a file's bytes directly through the broker. FALLBACK for clients
     * that cannot use the presigned `GetDownloadUrl` HTTP GET (no egress to the
     * object store, corporate proxy, etc.). The broker streams the object bytes
     * in bounded chunks server-side; it never buffers the whole object.
     * </pre>
     */
    default void downloadFile(com.udb.core.storage.services.v1.DownloadFileRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.DownloadFileChunk> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDownloadFileMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get file metadata
     * </pre>
     */
    default void getFile(com.udb.core.storage.services.v1.GetFileRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.GetFileResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetFileMethod(), responseObserver);
    }

    /**
     * <pre>
     * Update file metadata
     * </pre>
     */
    default void updateFile(com.udb.core.storage.services.v1.UpdateFileRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.UpdateFileResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateFileMethod(), responseObserver);
    }

    /**
     * <pre>
     * Delete a file (soft delete)
     * </pre>
     */
    default void deleteFile(com.udb.core.storage.services.v1.DeleteFileRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.DeleteFileResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteFileMethod(), responseObserver);
    }

    /**
     * <pre>
     * List files
     * </pre>
     */
    default void listFiles(com.udb.core.storage.services.v1.ListFilesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.ListFilesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListFilesMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service StorageService.
   */
  public static abstract class StorageServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return StorageServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service StorageService.
   */
  public static final class StorageServiceStub
      extends io.grpc.stub.AbstractAsyncStub<StorageServiceStub> {
    private StorageServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StorageServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new StorageServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a new upload and obtain a pre-signed upload URL
     * </pre>
     */
    public void registerUpload(com.udb.core.storage.services.v1.RegisterUploadRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.RegisterUploadResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRegisterUploadMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Finalize an upload after the object has been written to the store
     * </pre>
     */
    public void finalizeUpload(com.udb.core.storage.services.v1.FinalizeUploadRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.FinalizeUploadResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getFinalizeUploadMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get a pre-signed download URL for a file
     * </pre>
     */
    public void getDownloadUrl(com.udb.core.storage.services.v1.GetDownloadUrlRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.GetDownloadUrlResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetDownloadUrlMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Stream a file's bytes directly through the broker. FALLBACK for clients
     * that cannot use the presigned `GetDownloadUrl` HTTP GET (no egress to the
     * object store, corporate proxy, etc.). The broker streams the object bytes
     * in bounded chunks server-side; it never buffers the whole object.
     * </pre>
     */
    public void downloadFile(com.udb.core.storage.services.v1.DownloadFileRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.DownloadFileChunk> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getDownloadFileMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get file metadata
     * </pre>
     */
    public void getFile(com.udb.core.storage.services.v1.GetFileRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.GetFileResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetFileMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Update file metadata
     * </pre>
     */
    public void updateFile(com.udb.core.storage.services.v1.UpdateFileRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.UpdateFileResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateFileMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Delete a file (soft delete)
     * </pre>
     */
    public void deleteFile(com.udb.core.storage.services.v1.DeleteFileRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.DeleteFileResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteFileMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List files
     * </pre>
     */
    public void listFiles(com.udb.core.storage.services.v1.ListFilesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.ListFilesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListFilesMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service StorageService.
   */
  public static final class StorageServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<StorageServiceBlockingV2Stub> {
    private StorageServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StorageServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new StorageServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a new upload and obtain a pre-signed upload URL
     * </pre>
     */
    public com.udb.core.storage.services.v1.RegisterUploadResponse registerUpload(com.udb.core.storage.services.v1.RegisterUploadRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRegisterUploadMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Finalize an upload after the object has been written to the store
     * </pre>
     */
    public com.udb.core.storage.services.v1.FinalizeUploadResponse finalizeUpload(com.udb.core.storage.services.v1.FinalizeUploadRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getFinalizeUploadMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a pre-signed download URL for a file
     * </pre>
     */
    public com.udb.core.storage.services.v1.GetDownloadUrlResponse getDownloadUrl(com.udb.core.storage.services.v1.GetDownloadUrlRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetDownloadUrlMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Stream a file's bytes directly through the broker. FALLBACK for clients
     * that cannot use the presigned `GetDownloadUrl` HTTP GET (no egress to the
     * object store, corporate proxy, etc.). The broker streams the object bytes
     * in bounded chunks server-side; it never buffers the whole object.
     * </pre>
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<?, com.udb.core.storage.services.v1.DownloadFileChunk>
        downloadFile(com.udb.core.storage.services.v1.DownloadFileRequest request) {
      return io.grpc.stub.ClientCalls.blockingV2ServerStreamingCall(
          getChannel(), getDownloadFileMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get file metadata
     * </pre>
     */
    public com.udb.core.storage.services.v1.GetFileResponse getFile(com.udb.core.storage.services.v1.GetFileRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetFileMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Update file metadata
     * </pre>
     */
    public com.udb.core.storage.services.v1.UpdateFileResponse updateFile(com.udb.core.storage.services.v1.UpdateFileRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getUpdateFileMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a file (soft delete)
     * </pre>
     */
    public com.udb.core.storage.services.v1.DeleteFileResponse deleteFile(com.udb.core.storage.services.v1.DeleteFileRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeleteFileMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List files
     * </pre>
     */
    public com.udb.core.storage.services.v1.ListFilesResponse listFiles(com.udb.core.storage.services.v1.ListFilesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListFilesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service StorageService.
   */
  public static final class StorageServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<StorageServiceBlockingStub> {
    private StorageServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StorageServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new StorageServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a new upload and obtain a pre-signed upload URL
     * </pre>
     */
    public com.udb.core.storage.services.v1.RegisterUploadResponse registerUpload(com.udb.core.storage.services.v1.RegisterUploadRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRegisterUploadMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Finalize an upload after the object has been written to the store
     * </pre>
     */
    public com.udb.core.storage.services.v1.FinalizeUploadResponse finalizeUpload(com.udb.core.storage.services.v1.FinalizeUploadRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getFinalizeUploadMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a pre-signed download URL for a file
     * </pre>
     */
    public com.udb.core.storage.services.v1.GetDownloadUrlResponse getDownloadUrl(com.udb.core.storage.services.v1.GetDownloadUrlRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetDownloadUrlMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Stream a file's bytes directly through the broker. FALLBACK for clients
     * that cannot use the presigned `GetDownloadUrl` HTTP GET (no egress to the
     * object store, corporate proxy, etc.). The broker streams the object bytes
     * in bounded chunks server-side; it never buffers the whole object.
     * </pre>
     */
    public java.util.Iterator<com.udb.core.storage.services.v1.DownloadFileChunk> downloadFile(
        com.udb.core.storage.services.v1.DownloadFileRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getDownloadFileMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get file metadata
     * </pre>
     */
    public com.udb.core.storage.services.v1.GetFileResponse getFile(com.udb.core.storage.services.v1.GetFileRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetFileMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Update file metadata
     * </pre>
     */
    public com.udb.core.storage.services.v1.UpdateFileResponse updateFile(com.udb.core.storage.services.v1.UpdateFileRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateFileMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a file (soft delete)
     * </pre>
     */
    public com.udb.core.storage.services.v1.DeleteFileResponse deleteFile(com.udb.core.storage.services.v1.DeleteFileRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteFileMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List files
     * </pre>
     */
    public com.udb.core.storage.services.v1.ListFilesResponse listFiles(com.udb.core.storage.services.v1.ListFilesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListFilesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service StorageService.
   */
  public static final class StorageServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<StorageServiceFutureStub> {
    private StorageServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StorageServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new StorageServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a new upload and obtain a pre-signed upload URL
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.storage.services.v1.RegisterUploadResponse> registerUpload(
        com.udb.core.storage.services.v1.RegisterUploadRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRegisterUploadMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Finalize an upload after the object has been written to the store
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.storage.services.v1.FinalizeUploadResponse> finalizeUpload(
        com.udb.core.storage.services.v1.FinalizeUploadRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getFinalizeUploadMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get a pre-signed download URL for a file
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.storage.services.v1.GetDownloadUrlResponse> getDownloadUrl(
        com.udb.core.storage.services.v1.GetDownloadUrlRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetDownloadUrlMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get file metadata
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.storage.services.v1.GetFileResponse> getFile(
        com.udb.core.storage.services.v1.GetFileRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetFileMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Update file metadata
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.storage.services.v1.UpdateFileResponse> updateFile(
        com.udb.core.storage.services.v1.UpdateFileRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateFileMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Delete a file (soft delete)
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.storage.services.v1.DeleteFileResponse> deleteFile(
        com.udb.core.storage.services.v1.DeleteFileRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteFileMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List files
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.storage.services.v1.ListFilesResponse> listFiles(
        com.udb.core.storage.services.v1.ListFilesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListFilesMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_REGISTER_UPLOAD = 0;
  private static final int METHODID_FINALIZE_UPLOAD = 1;
  private static final int METHODID_GET_DOWNLOAD_URL = 2;
  private static final int METHODID_DOWNLOAD_FILE = 3;
  private static final int METHODID_GET_FILE = 4;
  private static final int METHODID_UPDATE_FILE = 5;
  private static final int METHODID_DELETE_FILE = 6;
  private static final int METHODID_LIST_FILES = 7;

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
        case METHODID_REGISTER_UPLOAD:
          serviceImpl.registerUpload((com.udb.core.storage.services.v1.RegisterUploadRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.RegisterUploadResponse>) responseObserver);
          break;
        case METHODID_FINALIZE_UPLOAD:
          serviceImpl.finalizeUpload((com.udb.core.storage.services.v1.FinalizeUploadRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.FinalizeUploadResponse>) responseObserver);
          break;
        case METHODID_GET_DOWNLOAD_URL:
          serviceImpl.getDownloadUrl((com.udb.core.storage.services.v1.GetDownloadUrlRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.GetDownloadUrlResponse>) responseObserver);
          break;
        case METHODID_DOWNLOAD_FILE:
          serviceImpl.downloadFile((com.udb.core.storage.services.v1.DownloadFileRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.DownloadFileChunk>) responseObserver);
          break;
        case METHODID_GET_FILE:
          serviceImpl.getFile((com.udb.core.storage.services.v1.GetFileRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.GetFileResponse>) responseObserver);
          break;
        case METHODID_UPDATE_FILE:
          serviceImpl.updateFile((com.udb.core.storage.services.v1.UpdateFileRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.UpdateFileResponse>) responseObserver);
          break;
        case METHODID_DELETE_FILE:
          serviceImpl.deleteFile((com.udb.core.storage.services.v1.DeleteFileRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.DeleteFileResponse>) responseObserver);
          break;
        case METHODID_LIST_FILES:
          serviceImpl.listFiles((com.udb.core.storage.services.v1.ListFilesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.storage.services.v1.ListFilesResponse>) responseObserver);
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
          getRegisterUploadMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.storage.services.v1.RegisterUploadRequest,
              com.udb.core.storage.services.v1.RegisterUploadResponse>(
                service, METHODID_REGISTER_UPLOAD)))
        .addMethod(
          getFinalizeUploadMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.storage.services.v1.FinalizeUploadRequest,
              com.udb.core.storage.services.v1.FinalizeUploadResponse>(
                service, METHODID_FINALIZE_UPLOAD)))
        .addMethod(
          getGetDownloadUrlMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.storage.services.v1.GetDownloadUrlRequest,
              com.udb.core.storage.services.v1.GetDownloadUrlResponse>(
                service, METHODID_GET_DOWNLOAD_URL)))
        .addMethod(
          getDownloadFileMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              com.udb.core.storage.services.v1.DownloadFileRequest,
              com.udb.core.storage.services.v1.DownloadFileChunk>(
                service, METHODID_DOWNLOAD_FILE)))
        .addMethod(
          getGetFileMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.storage.services.v1.GetFileRequest,
              com.udb.core.storage.services.v1.GetFileResponse>(
                service, METHODID_GET_FILE)))
        .addMethod(
          getUpdateFileMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.storage.services.v1.UpdateFileRequest,
              com.udb.core.storage.services.v1.UpdateFileResponse>(
                service, METHODID_UPDATE_FILE)))
        .addMethod(
          getDeleteFileMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.storage.services.v1.DeleteFileRequest,
              com.udb.core.storage.services.v1.DeleteFileResponse>(
                service, METHODID_DELETE_FILE)))
        .addMethod(
          getListFilesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.storage.services.v1.ListFilesRequest,
              com.udb.core.storage.services.v1.ListFilesResponse>(
                service, METHODID_LIST_FILES)))
        .build();
  }

  private static abstract class StorageServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    StorageServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.storage.services.v1.StorageServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("StorageService");
    }
  }

  private static final class StorageServiceFileDescriptorSupplier
      extends StorageServiceBaseDescriptorSupplier {
    StorageServiceFileDescriptorSupplier() {}
  }

  private static final class StorageServiceMethodDescriptorSupplier
      extends StorageServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    StorageServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (StorageServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new StorageServiceFileDescriptorSupplier())
              .addMethod(getRegisterUploadMethod())
              .addMethod(getFinalizeUploadMethod())
              .addMethod(getGetDownloadUrlMethod())
              .addMethod(getDownloadFileMethod())
              .addMethod(getGetFileMethod())
              .addMethod(getUpdateFileMethod())
              .addMethod(getDeleteFileMethod())
              .addMethod(getListFilesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
