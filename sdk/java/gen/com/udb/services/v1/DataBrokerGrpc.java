package com.udb.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * DataBroker is the UDB-owned wire protocol. Project protos are parsed only as
 * schema input; they do not need to contain or import this service contract.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class DataBrokerGrpc {

  private DataBrokerGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.services.v1.DataBroker";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.SelectRequest,
      com.udb.entity.v1.RecordSet> getSelectMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Select",
      requestType = com.udb.entity.v1.SelectRequest.class,
      responseType = com.udb.entity.v1.RecordSet.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.SelectRequest,
      com.udb.entity.v1.RecordSet> getSelectMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.SelectRequest, com.udb.entity.v1.RecordSet> getSelectMethod;
    if ((getSelectMethod = DataBrokerGrpc.getSelectMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getSelectMethod = DataBrokerGrpc.getSelectMethod) == null) {
          DataBrokerGrpc.getSelectMethod = getSelectMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.SelectRequest, com.udb.entity.v1.RecordSet>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Select"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.SelectRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.RecordSet.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("Select"))
              .build();
        }
      }
    }
    return getSelectMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.SelectRequest,
      com.udb.entity.v1.RecordSet> getBatchSelectMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "BatchSelect",
      requestType = com.udb.entity.v1.SelectRequest.class,
      responseType = com.udb.entity.v1.RecordSet.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.SelectRequest,
      com.udb.entity.v1.RecordSet> getBatchSelectMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.SelectRequest, com.udb.entity.v1.RecordSet> getBatchSelectMethod;
    if ((getBatchSelectMethod = DataBrokerGrpc.getBatchSelectMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getBatchSelectMethod = DataBrokerGrpc.getBatchSelectMethod) == null) {
          DataBrokerGrpc.getBatchSelectMethod = getBatchSelectMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.SelectRequest, com.udb.entity.v1.RecordSet>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "BatchSelect"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.SelectRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.RecordSet.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("BatchSelect"))
              .build();
        }
      }
    }
    return getBatchSelectMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.SelectRequest,
      com.udb.entity.v1.RecordBatchV2> getSelectV2Method;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SelectV2",
      requestType = com.udb.entity.v1.SelectRequest.class,
      responseType = com.udb.entity.v1.RecordBatchV2.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.SelectRequest,
      com.udb.entity.v1.RecordBatchV2> getSelectV2Method() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.SelectRequest, com.udb.entity.v1.RecordBatchV2> getSelectV2Method;
    if ((getSelectV2Method = DataBrokerGrpc.getSelectV2Method) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getSelectV2Method = DataBrokerGrpc.getSelectV2Method) == null) {
          DataBrokerGrpc.getSelectV2Method = getSelectV2Method =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.SelectRequest, com.udb.entity.v1.RecordBatchV2>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SelectV2"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.SelectRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.RecordBatchV2.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("SelectV2"))
              .build();
        }
      }
    }
    return getSelectV2Method;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.UpsertRequest,
      com.udb.entity.v1.MutationResponse> getUpsertMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Upsert",
      requestType = com.udb.entity.v1.UpsertRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.UpsertRequest,
      com.udb.entity.v1.MutationResponse> getUpsertMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.UpsertRequest, com.udb.entity.v1.MutationResponse> getUpsertMethod;
    if ((getUpsertMethod = DataBrokerGrpc.getUpsertMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getUpsertMethod = DataBrokerGrpc.getUpsertMethod) == null) {
          DataBrokerGrpc.getUpsertMethod = getUpsertMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.UpsertRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Upsert"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.UpsertRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("Upsert"))
              .build();
        }
      }
    }
    return getUpsertMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.UpsertRequest,
      com.udb.entity.v1.MutationResponse> getBatchUpsertMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "BatchUpsert",
      requestType = com.udb.entity.v1.UpsertRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.UpsertRequest,
      com.udb.entity.v1.MutationResponse> getBatchUpsertMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.UpsertRequest, com.udb.entity.v1.MutationResponse> getBatchUpsertMethod;
    if ((getBatchUpsertMethod = DataBrokerGrpc.getBatchUpsertMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getBatchUpsertMethod = DataBrokerGrpc.getBatchUpsertMethod) == null) {
          DataBrokerGrpc.getBatchUpsertMethod = getBatchUpsertMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.UpsertRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "BatchUpsert"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.UpsertRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("BatchUpsert"))
              .build();
        }
      }
    }
    return getBatchUpsertMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.DeleteRequest,
      com.udb.entity.v1.MutationResponse> getDeleteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Delete",
      requestType = com.udb.entity.v1.DeleteRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.DeleteRequest,
      com.udb.entity.v1.MutationResponse> getDeleteMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.DeleteRequest, com.udb.entity.v1.MutationResponse> getDeleteMethod;
    if ((getDeleteMethod = DataBrokerGrpc.getDeleteMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getDeleteMethod = DataBrokerGrpc.getDeleteMethod) == null) {
          DataBrokerGrpc.getDeleteMethod = getDeleteMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.DeleteRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Delete"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.DeleteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("Delete"))
              .build();
        }
      }
    }
    return getDeleteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.UpdateRequest,
      com.udb.entity.v1.MutationResponse> getUpdateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Update",
      requestType = com.udb.entity.v1.UpdateRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.UpdateRequest,
      com.udb.entity.v1.MutationResponse> getUpdateMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.UpdateRequest, com.udb.entity.v1.MutationResponse> getUpdateMethod;
    if ((getUpdateMethod = DataBrokerGrpc.getUpdateMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getUpdateMethod = DataBrokerGrpc.getUpdateMethod) == null) {
          DataBrokerGrpc.getUpdateMethod = getUpdateMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.UpdateRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Update"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.UpdateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("Update"))
              .build();
        }
      }
    }
    return getUpdateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.VectorSearchRequest,
      com.udb.entity.v1.VectorSet> getVectorSearchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "VectorSearch",
      requestType = com.udb.entity.v1.VectorSearchRequest.class,
      responseType = com.udb.entity.v1.VectorSet.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.VectorSearchRequest,
      com.udb.entity.v1.VectorSet> getVectorSearchMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.VectorSearchRequest, com.udb.entity.v1.VectorSet> getVectorSearchMethod;
    if ((getVectorSearchMethod = DataBrokerGrpc.getVectorSearchMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getVectorSearchMethod = DataBrokerGrpc.getVectorSearchMethod) == null) {
          DataBrokerGrpc.getVectorSearchMethod = getVectorSearchMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.VectorSearchRequest, com.udb.entity.v1.VectorSet>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "VectorSearch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.VectorSearchRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.VectorSet.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("VectorSearch"))
              .build();
        }
      }
    }
    return getVectorSearchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.VectorHybridSearchRequest,
      com.udb.entity.v1.VectorSet> getVectorHybridSearchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "VectorHybridSearch",
      requestType = com.udb.entity.v1.VectorHybridSearchRequest.class,
      responseType = com.udb.entity.v1.VectorSet.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.VectorHybridSearchRequest,
      com.udb.entity.v1.VectorSet> getVectorHybridSearchMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.VectorHybridSearchRequest, com.udb.entity.v1.VectorSet> getVectorHybridSearchMethod;
    if ((getVectorHybridSearchMethod = DataBrokerGrpc.getVectorHybridSearchMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getVectorHybridSearchMethod = DataBrokerGrpc.getVectorHybridSearchMethod) == null) {
          DataBrokerGrpc.getVectorHybridSearchMethod = getVectorHybridSearchMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.VectorHybridSearchRequest, com.udb.entity.v1.VectorSet>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "VectorHybridSearch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.VectorHybridSearchRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.VectorSet.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("VectorHybridSearch"))
              .build();
        }
      }
    }
    return getVectorHybridSearchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.VectorUpsertRequest,
      com.udb.entity.v1.MutationResponse> getVectorUpsertMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "VectorUpsert",
      requestType = com.udb.entity.v1.VectorUpsertRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.VectorUpsertRequest,
      com.udb.entity.v1.MutationResponse> getVectorUpsertMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.VectorUpsertRequest, com.udb.entity.v1.MutationResponse> getVectorUpsertMethod;
    if ((getVectorUpsertMethod = DataBrokerGrpc.getVectorUpsertMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getVectorUpsertMethod = DataBrokerGrpc.getVectorUpsertMethod) == null) {
          DataBrokerGrpc.getVectorUpsertMethod = getVectorUpsertMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.VectorUpsertRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "VectorUpsert"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.VectorUpsertRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("VectorUpsert"))
              .build();
        }
      }
    }
    return getVectorUpsertMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.VectorUpsertRequest,
      com.udb.entity.v1.MutationResponse> getVectorBatchUpsertMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "VectorBatchUpsert",
      requestType = com.udb.entity.v1.VectorUpsertRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.VectorUpsertRequest,
      com.udb.entity.v1.MutationResponse> getVectorBatchUpsertMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.VectorUpsertRequest, com.udb.entity.v1.MutationResponse> getVectorBatchUpsertMethod;
    if ((getVectorBatchUpsertMethod = DataBrokerGrpc.getVectorBatchUpsertMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getVectorBatchUpsertMethod = DataBrokerGrpc.getVectorBatchUpsertMethod) == null) {
          DataBrokerGrpc.getVectorBatchUpsertMethod = getVectorBatchUpsertMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.VectorUpsertRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "VectorBatchUpsert"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.VectorUpsertRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("VectorBatchUpsert"))
              .build();
        }
      }
    }
    return getVectorBatchUpsertMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.Chunk,
      com.udb.entity.v1.MutationResponse> getPutObjectMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PutObject",
      requestType = com.udb.entity.v1.Chunk.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.Chunk,
      com.udb.entity.v1.MutationResponse> getPutObjectMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.Chunk, com.udb.entity.v1.MutationResponse> getPutObjectMethod;
    if ((getPutObjectMethod = DataBrokerGrpc.getPutObjectMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getPutObjectMethod = DataBrokerGrpc.getPutObjectMethod) == null) {
          DataBrokerGrpc.getPutObjectMethod = getPutObjectMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.Chunk, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PutObject"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.Chunk.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("PutObject"))
              .build();
        }
      }
    }
    return getPutObjectMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.ObjectRequest,
      com.udb.entity.v1.Chunk> getGetObjectMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetObject",
      requestType = com.udb.entity.v1.ObjectRequest.class,
      responseType = com.udb.entity.v1.Chunk.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.ObjectRequest,
      com.udb.entity.v1.Chunk> getGetObjectMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.ObjectRequest, com.udb.entity.v1.Chunk> getGetObjectMethod;
    if ((getGetObjectMethod = DataBrokerGrpc.getGetObjectMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getGetObjectMethod = DataBrokerGrpc.getGetObjectMethod) == null) {
          DataBrokerGrpc.getGetObjectMethod = getGetObjectMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.ObjectRequest, com.udb.entity.v1.Chunk>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetObject"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.ObjectRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.Chunk.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("GetObject"))
              .build();
        }
      }
    }
    return getGetObjectMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.UrlRequest,
      com.udb.entity.v1.UrlResponse> getGeneratePresignedUrlMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GeneratePresignedUrl",
      requestType = com.udb.entity.v1.UrlRequest.class,
      responseType = com.udb.entity.v1.UrlResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.UrlRequest,
      com.udb.entity.v1.UrlResponse> getGeneratePresignedUrlMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.UrlRequest, com.udb.entity.v1.UrlResponse> getGeneratePresignedUrlMethod;
    if ((getGeneratePresignedUrlMethod = DataBrokerGrpc.getGeneratePresignedUrlMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getGeneratePresignedUrlMethod = DataBrokerGrpc.getGeneratePresignedUrlMethod) == null) {
          DataBrokerGrpc.getGeneratePresignedUrlMethod = getGeneratePresignedUrlMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.UrlRequest, com.udb.entity.v1.UrlResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GeneratePresignedUrl"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.UrlRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.UrlResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("GeneratePresignedUrl"))
              .build();
        }
      }
    }
    return getGeneratePresignedUrlMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.MultipartUploadRequest,
      com.udb.entity.v1.MultipartUploadResponse> getInitiateMultipartUploadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "InitiateMultipartUpload",
      requestType = com.udb.entity.v1.MultipartUploadRequest.class,
      responseType = com.udb.entity.v1.MultipartUploadResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.MultipartUploadRequest,
      com.udb.entity.v1.MultipartUploadResponse> getInitiateMultipartUploadMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.MultipartUploadRequest, com.udb.entity.v1.MultipartUploadResponse> getInitiateMultipartUploadMethod;
    if ((getInitiateMultipartUploadMethod = DataBrokerGrpc.getInitiateMultipartUploadMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getInitiateMultipartUploadMethod = DataBrokerGrpc.getInitiateMultipartUploadMethod) == null) {
          DataBrokerGrpc.getInitiateMultipartUploadMethod = getInitiateMultipartUploadMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.MultipartUploadRequest, com.udb.entity.v1.MultipartUploadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "InitiateMultipartUpload"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MultipartUploadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MultipartUploadResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("InitiateMultipartUpload"))
              .build();
        }
      }
    }
    return getInitiateMultipartUploadMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CacheGetRequest,
      com.udb.entity.v1.CacheGetResponse> getCacheGetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CacheGet",
      requestType = com.udb.entity.v1.CacheGetRequest.class,
      responseType = com.udb.entity.v1.CacheGetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CacheGetRequest,
      com.udb.entity.v1.CacheGetResponse> getCacheGetMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CacheGetRequest, com.udb.entity.v1.CacheGetResponse> getCacheGetMethod;
    if ((getCacheGetMethod = DataBrokerGrpc.getCacheGetMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getCacheGetMethod = DataBrokerGrpc.getCacheGetMethod) == null) {
          DataBrokerGrpc.getCacheGetMethod = getCacheGetMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CacheGetRequest, com.udb.entity.v1.CacheGetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CacheGet"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CacheGetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CacheGetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("CacheGet"))
              .build();
        }
      }
    }
    return getCacheGetMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CacheSetRequest,
      com.udb.entity.v1.MutationResponse> getCacheSetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CacheSet",
      requestType = com.udb.entity.v1.CacheSetRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CacheSetRequest,
      com.udb.entity.v1.MutationResponse> getCacheSetMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CacheSetRequest, com.udb.entity.v1.MutationResponse> getCacheSetMethod;
    if ((getCacheSetMethod = DataBrokerGrpc.getCacheSetMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getCacheSetMethod = DataBrokerGrpc.getCacheSetMethod) == null) {
          DataBrokerGrpc.getCacheSetMethod = getCacheSetMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CacheSetRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CacheSet"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CacheSetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("CacheSet"))
              .build();
        }
      }
    }
    return getCacheSetMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CacheDeleteRequest,
      com.udb.entity.v1.MutationResponse> getCacheDeleteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CacheDelete",
      requestType = com.udb.entity.v1.CacheDeleteRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CacheDeleteRequest,
      com.udb.entity.v1.MutationResponse> getCacheDeleteMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CacheDeleteRequest, com.udb.entity.v1.MutationResponse> getCacheDeleteMethod;
    if ((getCacheDeleteMethod = DataBrokerGrpc.getCacheDeleteMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getCacheDeleteMethod = DataBrokerGrpc.getCacheDeleteMethod) == null) {
          DataBrokerGrpc.getCacheDeleteMethod = getCacheDeleteMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CacheDeleteRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CacheDelete"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CacheDeleteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("CacheDelete"))
              .build();
        }
      }
    }
    return getCacheDeleteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CacheScanRequest,
      com.udb.entity.v1.CacheScanResponse> getCacheScanMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CacheScan",
      requestType = com.udb.entity.v1.CacheScanRequest.class,
      responseType = com.udb.entity.v1.CacheScanResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CacheScanRequest,
      com.udb.entity.v1.CacheScanResponse> getCacheScanMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CacheScanRequest, com.udb.entity.v1.CacheScanResponse> getCacheScanMethod;
    if ((getCacheScanMethod = DataBrokerGrpc.getCacheScanMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getCacheScanMethod = DataBrokerGrpc.getCacheScanMethod) == null) {
          DataBrokerGrpc.getCacheScanMethod = getCacheScanMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CacheScanRequest, com.udb.entity.v1.CacheScanResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CacheScan"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CacheScanRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CacheScanResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("CacheScan"))
              .build();
        }
      }
    }
    return getCacheScanMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.DocumentGetRequest,
      com.udb.entity.v1.DocumentSet> getDocumentGetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DocumentGet",
      requestType = com.udb.entity.v1.DocumentGetRequest.class,
      responseType = com.udb.entity.v1.DocumentSet.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.DocumentGetRequest,
      com.udb.entity.v1.DocumentSet> getDocumentGetMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.DocumentGetRequest, com.udb.entity.v1.DocumentSet> getDocumentGetMethod;
    if ((getDocumentGetMethod = DataBrokerGrpc.getDocumentGetMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getDocumentGetMethod = DataBrokerGrpc.getDocumentGetMethod) == null) {
          DataBrokerGrpc.getDocumentGetMethod = getDocumentGetMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.DocumentGetRequest, com.udb.entity.v1.DocumentSet>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DocumentGet"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.DocumentGetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.DocumentSet.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("DocumentGet"))
              .build();
        }
      }
    }
    return getDocumentGetMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.DocumentFindRequest,
      com.udb.entity.v1.DocumentSet> getDocumentFindMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DocumentFind",
      requestType = com.udb.entity.v1.DocumentFindRequest.class,
      responseType = com.udb.entity.v1.DocumentSet.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.DocumentFindRequest,
      com.udb.entity.v1.DocumentSet> getDocumentFindMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.DocumentFindRequest, com.udb.entity.v1.DocumentSet> getDocumentFindMethod;
    if ((getDocumentFindMethod = DataBrokerGrpc.getDocumentFindMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getDocumentFindMethod = DataBrokerGrpc.getDocumentFindMethod) == null) {
          DataBrokerGrpc.getDocumentFindMethod = getDocumentFindMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.DocumentFindRequest, com.udb.entity.v1.DocumentSet>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DocumentFind"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.DocumentFindRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.DocumentSet.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("DocumentFind"))
              .build();
        }
      }
    }
    return getDocumentFindMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.DocumentUpsertRequest,
      com.udb.entity.v1.MutationResponse> getDocumentUpsertMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DocumentUpsert",
      requestType = com.udb.entity.v1.DocumentUpsertRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.DocumentUpsertRequest,
      com.udb.entity.v1.MutationResponse> getDocumentUpsertMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.DocumentUpsertRequest, com.udb.entity.v1.MutationResponse> getDocumentUpsertMethod;
    if ((getDocumentUpsertMethod = DataBrokerGrpc.getDocumentUpsertMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getDocumentUpsertMethod = DataBrokerGrpc.getDocumentUpsertMethod) == null) {
          DataBrokerGrpc.getDocumentUpsertMethod = getDocumentUpsertMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.DocumentUpsertRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DocumentUpsert"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.DocumentUpsertRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("DocumentUpsert"))
              .build();
        }
      }
    }
    return getDocumentUpsertMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.DocumentDeleteRequest,
      com.udb.entity.v1.MutationResponse> getDocumentDeleteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DocumentDelete",
      requestType = com.udb.entity.v1.DocumentDeleteRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.DocumentDeleteRequest,
      com.udb.entity.v1.MutationResponse> getDocumentDeleteMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.DocumentDeleteRequest, com.udb.entity.v1.MutationResponse> getDocumentDeleteMethod;
    if ((getDocumentDeleteMethod = DataBrokerGrpc.getDocumentDeleteMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getDocumentDeleteMethod = DataBrokerGrpc.getDocumentDeleteMethod) == null) {
          DataBrokerGrpc.getDocumentDeleteMethod = getDocumentDeleteMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.DocumentDeleteRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DocumentDelete"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.DocumentDeleteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("DocumentDelete"))
              .build();
        }
      }
    }
    return getDocumentDeleteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.GraphQueryRequest,
      com.udb.entity.v1.GraphResultSet> getGraphQueryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GraphQuery",
      requestType = com.udb.entity.v1.GraphQueryRequest.class,
      responseType = com.udb.entity.v1.GraphResultSet.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.GraphQueryRequest,
      com.udb.entity.v1.GraphResultSet> getGraphQueryMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.GraphQueryRequest, com.udb.entity.v1.GraphResultSet> getGraphQueryMethod;
    if ((getGraphQueryMethod = DataBrokerGrpc.getGraphQueryMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getGraphQueryMethod = DataBrokerGrpc.getGraphQueryMethod) == null) {
          DataBrokerGrpc.getGraphQueryMethod = getGraphQueryMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.GraphQueryRequest, com.udb.entity.v1.GraphResultSet>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GraphQuery"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.GraphQueryRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.GraphResultSet.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("GraphQuery"))
              .build();
        }
      }
    }
    return getGraphQueryMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.GraphMutationRequest,
      com.udb.entity.v1.MutationResponse> getGraphMutateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GraphMutate",
      requestType = com.udb.entity.v1.GraphMutationRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.GraphMutationRequest,
      com.udb.entity.v1.MutationResponse> getGraphMutateMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.GraphMutationRequest, com.udb.entity.v1.MutationResponse> getGraphMutateMethod;
    if ((getGraphMutateMethod = DataBrokerGrpc.getGraphMutateMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getGraphMutateMethod = DataBrokerGrpc.getGraphMutateMethod) == null) {
          DataBrokerGrpc.getGraphMutateMethod = getGraphMutateMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.GraphMutationRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GraphMutate"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.GraphMutationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("GraphMutate"))
              .build();
        }
      }
    }
    return getGraphMutateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.TimeSeriesWriteRequest,
      com.udb.entity.v1.MutationResponse> getTimeSeriesWriteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TimeSeriesWrite",
      requestType = com.udb.entity.v1.TimeSeriesWriteRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.TimeSeriesWriteRequest,
      com.udb.entity.v1.MutationResponse> getTimeSeriesWriteMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.TimeSeriesWriteRequest, com.udb.entity.v1.MutationResponse> getTimeSeriesWriteMethod;
    if ((getTimeSeriesWriteMethod = DataBrokerGrpc.getTimeSeriesWriteMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getTimeSeriesWriteMethod = DataBrokerGrpc.getTimeSeriesWriteMethod) == null) {
          DataBrokerGrpc.getTimeSeriesWriteMethod = getTimeSeriesWriteMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.TimeSeriesWriteRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TimeSeriesWrite"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.TimeSeriesWriteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("TimeSeriesWrite"))
              .build();
        }
      }
    }
    return getTimeSeriesWriteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.TimeSeriesQueryRequest,
      com.udb.entity.v1.TimeSeriesQueryResponse> getTimeSeriesQueryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TimeSeriesQuery",
      requestType = com.udb.entity.v1.TimeSeriesQueryRequest.class,
      responseType = com.udb.entity.v1.TimeSeriesQueryResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.TimeSeriesQueryRequest,
      com.udb.entity.v1.TimeSeriesQueryResponse> getTimeSeriesQueryMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.TimeSeriesQueryRequest, com.udb.entity.v1.TimeSeriesQueryResponse> getTimeSeriesQueryMethod;
    if ((getTimeSeriesQueryMethod = DataBrokerGrpc.getTimeSeriesQueryMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getTimeSeriesQueryMethod = DataBrokerGrpc.getTimeSeriesQueryMethod) == null) {
          DataBrokerGrpc.getTimeSeriesQueryMethod = getTimeSeriesQueryMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.TimeSeriesQueryRequest, com.udb.entity.v1.TimeSeriesQueryResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TimeSeriesQuery"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.TimeSeriesQueryRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.TimeSeriesQueryResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("TimeSeriesQuery"))
              .build();
        }
      }
    }
    return getTimeSeriesQueryMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.AnalyticalQueryRequest,
      com.udb.entity.v1.AnalyticalQueryResponse> getAnalyticalQueryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AnalyticalQuery",
      requestType = com.udb.entity.v1.AnalyticalQueryRequest.class,
      responseType = com.udb.entity.v1.AnalyticalQueryResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.AnalyticalQueryRequest,
      com.udb.entity.v1.AnalyticalQueryResponse> getAnalyticalQueryMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.AnalyticalQueryRequest, com.udb.entity.v1.AnalyticalQueryResponse> getAnalyticalQueryMethod;
    if ((getAnalyticalQueryMethod = DataBrokerGrpc.getAnalyticalQueryMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getAnalyticalQueryMethod = DataBrokerGrpc.getAnalyticalQueryMethod) == null) {
          DataBrokerGrpc.getAnalyticalQueryMethod = getAnalyticalQueryMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.AnalyticalQueryRequest, com.udb.entity.v1.AnalyticalQueryResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AnalyticalQuery"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.AnalyticalQueryRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.AnalyticalQueryResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("AnalyticalQuery"))
              .build();
        }
      }
    }
    return getAnalyticalQueryMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.Mutation,
      com.udb.entity.v1.TxStatus> getBeginTxMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "BeginTx",
      requestType = com.udb.entity.v1.Mutation.class,
      responseType = com.udb.entity.v1.TxStatus.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.Mutation,
      com.udb.entity.v1.TxStatus> getBeginTxMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.Mutation, com.udb.entity.v1.TxStatus> getBeginTxMethod;
    if ((getBeginTxMethod = DataBrokerGrpc.getBeginTxMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getBeginTxMethod = DataBrokerGrpc.getBeginTxMethod) == null) {
          DataBrokerGrpc.getBeginTxMethod = getBeginTxMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.Mutation, com.udb.entity.v1.TxStatus>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "BeginTx"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.Mutation.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.TxStatus.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("BeginTx"))
              .build();
        }
      }
    }
    return getBeginTxMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CDCSubscriptionRequest,
      com.udb.events.v1.CDCEnvelope> getPublishCDCMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PublishCDC",
      requestType = com.udb.entity.v1.CDCSubscriptionRequest.class,
      responseType = com.udb.events.v1.CDCEnvelope.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CDCSubscriptionRequest,
      com.udb.events.v1.CDCEnvelope> getPublishCDCMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CDCSubscriptionRequest, com.udb.events.v1.CDCEnvelope> getPublishCDCMethod;
    if ((getPublishCDCMethod = DataBrokerGrpc.getPublishCDCMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getPublishCDCMethod = DataBrokerGrpc.getPublishCDCMethod) == null) {
          DataBrokerGrpc.getPublishCDCMethod = getPublishCDCMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CDCSubscriptionRequest, com.udb.events.v1.CDCEnvelope>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PublishCDC"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CDCSubscriptionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.events.v1.CDCEnvelope.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("PublishCDC"))
              .build();
        }
      }
    }
    return getPublishCDCMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.ViewDefinition,
      com.udb.entity.v1.MutationResponse> getCreateMaterializedViewMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateMaterializedView",
      requestType = com.udb.entity.v1.ViewDefinition.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.ViewDefinition,
      com.udb.entity.v1.MutationResponse> getCreateMaterializedViewMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.ViewDefinition, com.udb.entity.v1.MutationResponse> getCreateMaterializedViewMethod;
    if ((getCreateMaterializedViewMethod = DataBrokerGrpc.getCreateMaterializedViewMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getCreateMaterializedViewMethod = DataBrokerGrpc.getCreateMaterializedViewMethod) == null) {
          DataBrokerGrpc.getCreateMaterializedViewMethod = getCreateMaterializedViewMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.ViewDefinition, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateMaterializedView"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.ViewDefinition.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("CreateMaterializedView"))
              .build();
        }
      }
    }
    return getCreateMaterializedViewMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.EnqueueOutboxEventRequest,
      com.udb.entity.v1.EnqueueOutboxEventResponse> getEnqueueOutboxEventMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "EnqueueOutboxEvent",
      requestType = com.udb.entity.v1.EnqueueOutboxEventRequest.class,
      responseType = com.udb.entity.v1.EnqueueOutboxEventResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.EnqueueOutboxEventRequest,
      com.udb.entity.v1.EnqueueOutboxEventResponse> getEnqueueOutboxEventMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.EnqueueOutboxEventRequest, com.udb.entity.v1.EnqueueOutboxEventResponse> getEnqueueOutboxEventMethod;
    if ((getEnqueueOutboxEventMethod = DataBrokerGrpc.getEnqueueOutboxEventMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getEnqueueOutboxEventMethod = DataBrokerGrpc.getEnqueueOutboxEventMethod) == null) {
          DataBrokerGrpc.getEnqueueOutboxEventMethod = getEnqueueOutboxEventMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.EnqueueOutboxEventRequest, com.udb.entity.v1.EnqueueOutboxEventResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "EnqueueOutboxEvent"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.EnqueueOutboxEventRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.EnqueueOutboxEventResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("EnqueueOutboxEvent"))
              .build();
        }
      }
    }
    return getEnqueueOutboxEventMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.GenericDispatchRequest,
      com.udb.entity.v1.GenericDispatchResponse> getGenericDispatchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GenericDispatch",
      requestType = com.udb.entity.v1.GenericDispatchRequest.class,
      responseType = com.udb.entity.v1.GenericDispatchResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.GenericDispatchRequest,
      com.udb.entity.v1.GenericDispatchResponse> getGenericDispatchMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.GenericDispatchRequest, com.udb.entity.v1.GenericDispatchResponse> getGenericDispatchMethod;
    if ((getGenericDispatchMethod = DataBrokerGrpc.getGenericDispatchMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getGenericDispatchMethod = DataBrokerGrpc.getGenericDispatchMethod) == null) {
          DataBrokerGrpc.getGenericDispatchMethod = getGenericDispatchMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.GenericDispatchRequest, com.udb.entity.v1.GenericDispatchResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GenericDispatch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.GenericDispatchRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.GenericDispatchResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("GenericDispatch"))
              .build();
        }
      }
    }
    return getGenericDispatchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.ResourceAdminRequest,
      com.udb.entity.v1.MutationResponse> getEnsureResourceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "EnsureResource",
      requestType = com.udb.entity.v1.ResourceAdminRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.ResourceAdminRequest,
      com.udb.entity.v1.MutationResponse> getEnsureResourceMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.ResourceAdminRequest, com.udb.entity.v1.MutationResponse> getEnsureResourceMethod;
    if ((getEnsureResourceMethod = DataBrokerGrpc.getEnsureResourceMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getEnsureResourceMethod = DataBrokerGrpc.getEnsureResourceMethod) == null) {
          DataBrokerGrpc.getEnsureResourceMethod = getEnsureResourceMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.ResourceAdminRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "EnsureResource"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.ResourceAdminRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("EnsureResource"))
              .build();
        }
      }
    }
    return getEnsureResourceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.ResourceAdminRequest,
      com.udb.entity.v1.MutationResponse> getDropResourceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DropResource",
      requestType = com.udb.entity.v1.ResourceAdminRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.ResourceAdminRequest,
      com.udb.entity.v1.MutationResponse> getDropResourceMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.ResourceAdminRequest, com.udb.entity.v1.MutationResponse> getDropResourceMethod;
    if ((getDropResourceMethod = DataBrokerGrpc.getDropResourceMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getDropResourceMethod = DataBrokerGrpc.getDropResourceMethod) == null) {
          DataBrokerGrpc.getDropResourceMethod = getDropResourceMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.ResourceAdminRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DropResource"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.ResourceAdminRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("DropResource"))
              .build();
        }
      }
    }
    return getDropResourceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.ResourceAdminRequest,
      com.udb.entity.v1.ResourceListResponse> getListResourcesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListResources",
      requestType = com.udb.entity.v1.ResourceAdminRequest.class,
      responseType = com.udb.entity.v1.ResourceListResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.ResourceAdminRequest,
      com.udb.entity.v1.ResourceListResponse> getListResourcesMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.ResourceAdminRequest, com.udb.entity.v1.ResourceListResponse> getListResourcesMethod;
    if ((getListResourcesMethod = DataBrokerGrpc.getListResourcesMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getListResourcesMethod = DataBrokerGrpc.getListResourcesMethod) == null) {
          DataBrokerGrpc.getListResourcesMethod = getListResourcesMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.ResourceAdminRequest, com.udb.entity.v1.ResourceListResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListResources"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.ResourceAdminRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.ResourceListResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("ListResources"))
              .build();
        }
      }
    }
    return getListResourcesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.StageCatalogRequest,
      com.udb.entity.v1.CatalogVersionResponse> getStageCatalogMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StageCatalog",
      requestType = com.udb.entity.v1.StageCatalogRequest.class,
      responseType = com.udb.entity.v1.CatalogVersionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.StageCatalogRequest,
      com.udb.entity.v1.CatalogVersionResponse> getStageCatalogMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.StageCatalogRequest, com.udb.entity.v1.CatalogVersionResponse> getStageCatalogMethod;
    if ((getStageCatalogMethod = DataBrokerGrpc.getStageCatalogMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getStageCatalogMethod = DataBrokerGrpc.getStageCatalogMethod) == null) {
          DataBrokerGrpc.getStageCatalogMethod = getStageCatalogMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.StageCatalogRequest, com.udb.entity.v1.CatalogVersionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StageCatalog"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.StageCatalogRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CatalogVersionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("StageCatalog"))
              .build();
        }
      }
    }
    return getStageCatalogMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CatalogVersionRequest,
      com.udb.entity.v1.CatalogVersionResponse> getActivateCatalogMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ActivateCatalog",
      requestType = com.udb.entity.v1.CatalogVersionRequest.class,
      responseType = com.udb.entity.v1.CatalogVersionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CatalogVersionRequest,
      com.udb.entity.v1.CatalogVersionResponse> getActivateCatalogMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CatalogVersionRequest, com.udb.entity.v1.CatalogVersionResponse> getActivateCatalogMethod;
    if ((getActivateCatalogMethod = DataBrokerGrpc.getActivateCatalogMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getActivateCatalogMethod = DataBrokerGrpc.getActivateCatalogMethod) == null) {
          DataBrokerGrpc.getActivateCatalogMethod = getActivateCatalogMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CatalogVersionRequest, com.udb.entity.v1.CatalogVersionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ActivateCatalog"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CatalogVersionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CatalogVersionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("ActivateCatalog"))
              .build();
        }
      }
    }
    return getActivateCatalogMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CatalogVersionRequest,
      com.udb.entity.v1.CatalogVersionResponse> getRollbackCatalogMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RollbackCatalog",
      requestType = com.udb.entity.v1.CatalogVersionRequest.class,
      responseType = com.udb.entity.v1.CatalogVersionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CatalogVersionRequest,
      com.udb.entity.v1.CatalogVersionResponse> getRollbackCatalogMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CatalogVersionRequest, com.udb.entity.v1.CatalogVersionResponse> getRollbackCatalogMethod;
    if ((getRollbackCatalogMethod = DataBrokerGrpc.getRollbackCatalogMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getRollbackCatalogMethod = DataBrokerGrpc.getRollbackCatalogMethod) == null) {
          DataBrokerGrpc.getRollbackCatalogMethod = getRollbackCatalogMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CatalogVersionRequest, com.udb.entity.v1.CatalogVersionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RollbackCatalog"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CatalogVersionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CatalogVersionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("RollbackCatalog"))
              .build();
        }
      }
    }
    return getRollbackCatalogMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.StageCatalogRequest,
      com.udb.entity.v1.CatalogValidationResponse> getValidateCatalogMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ValidateCatalog",
      requestType = com.udb.entity.v1.StageCatalogRequest.class,
      responseType = com.udb.entity.v1.CatalogValidationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.StageCatalogRequest,
      com.udb.entity.v1.CatalogValidationResponse> getValidateCatalogMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.StageCatalogRequest, com.udb.entity.v1.CatalogValidationResponse> getValidateCatalogMethod;
    if ((getValidateCatalogMethod = DataBrokerGrpc.getValidateCatalogMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getValidateCatalogMethod = DataBrokerGrpc.getValidateCatalogMethod) == null) {
          DataBrokerGrpc.getValidateCatalogMethod = getValidateCatalogMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.StageCatalogRequest, com.udb.entity.v1.CatalogValidationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ValidateCatalog"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.StageCatalogRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CatalogValidationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("ValidateCatalog"))
              .build();
        }
      }
    }
    return getValidateCatalogMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CatalogManifestRequest,
      com.udb.entity.v1.CatalogVersionListResponse> getGetCatalogVersionsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetCatalogVersions",
      requestType = com.udb.entity.v1.CatalogManifestRequest.class,
      responseType = com.udb.entity.v1.CatalogVersionListResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CatalogManifestRequest,
      com.udb.entity.v1.CatalogVersionListResponse> getGetCatalogVersionsMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CatalogManifestRequest, com.udb.entity.v1.CatalogVersionListResponse> getGetCatalogVersionsMethod;
    if ((getGetCatalogVersionsMethod = DataBrokerGrpc.getGetCatalogVersionsMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getGetCatalogVersionsMethod = DataBrokerGrpc.getGetCatalogVersionsMethod) == null) {
          DataBrokerGrpc.getGetCatalogVersionsMethod = getGetCatalogVersionsMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CatalogManifestRequest, com.udb.entity.v1.CatalogVersionListResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetCatalogVersions"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CatalogManifestRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CatalogVersionListResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("GetCatalogVersions"))
              .build();
        }
      }
    }
    return getGetCatalogVersionsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CatalogVersionRequest,
      com.udb.entity.v1.CatalogVersionResponse> getGetCatalogVersionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetCatalogVersion",
      requestType = com.udb.entity.v1.CatalogVersionRequest.class,
      responseType = com.udb.entity.v1.CatalogVersionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CatalogVersionRequest,
      com.udb.entity.v1.CatalogVersionResponse> getGetCatalogVersionMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CatalogVersionRequest, com.udb.entity.v1.CatalogVersionResponse> getGetCatalogVersionMethod;
    if ((getGetCatalogVersionMethod = DataBrokerGrpc.getGetCatalogVersionMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getGetCatalogVersionMethod = DataBrokerGrpc.getGetCatalogVersionMethod) == null) {
          DataBrokerGrpc.getGetCatalogVersionMethod = getGetCatalogVersionMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CatalogVersionRequest, com.udb.entity.v1.CatalogVersionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetCatalogVersion"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CatalogVersionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CatalogVersionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("GetCatalogVersion"))
              .build();
        }
      }
    }
    return getGetCatalogVersionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.MigrationPlanRequest,
      com.udb.entity.v1.MigrationPlanResponse> getPlanMigrationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PlanMigration",
      requestType = com.udb.entity.v1.MigrationPlanRequest.class,
      responseType = com.udb.entity.v1.MigrationPlanResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.MigrationPlanRequest,
      com.udb.entity.v1.MigrationPlanResponse> getPlanMigrationMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.MigrationPlanRequest, com.udb.entity.v1.MigrationPlanResponse> getPlanMigrationMethod;
    if ((getPlanMigrationMethod = DataBrokerGrpc.getPlanMigrationMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getPlanMigrationMethod = DataBrokerGrpc.getPlanMigrationMethod) == null) {
          DataBrokerGrpc.getPlanMigrationMethod = getPlanMigrationMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.MigrationPlanRequest, com.udb.entity.v1.MigrationPlanResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PlanMigration"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MigrationPlanRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MigrationPlanResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("PlanMigration"))
              .build();
        }
      }
    }
    return getPlanMigrationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.MigrationApplyRequest,
      com.udb.entity.v1.MigrationStatusResponse> getApplyMigrationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ApplyMigration",
      requestType = com.udb.entity.v1.MigrationApplyRequest.class,
      responseType = com.udb.entity.v1.MigrationStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.MigrationApplyRequest,
      com.udb.entity.v1.MigrationStatusResponse> getApplyMigrationMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.MigrationApplyRequest, com.udb.entity.v1.MigrationStatusResponse> getApplyMigrationMethod;
    if ((getApplyMigrationMethod = DataBrokerGrpc.getApplyMigrationMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getApplyMigrationMethod = DataBrokerGrpc.getApplyMigrationMethod) == null) {
          DataBrokerGrpc.getApplyMigrationMethod = getApplyMigrationMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.MigrationApplyRequest, com.udb.entity.v1.MigrationStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ApplyMigration"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MigrationApplyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MigrationStatusResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("ApplyMigration"))
              .build();
        }
      }
    }
    return getApplyMigrationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.MigrationRunRequest,
      com.udb.entity.v1.MigrationStatusResponse> getGetMigrationStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetMigrationStatus",
      requestType = com.udb.entity.v1.MigrationRunRequest.class,
      responseType = com.udb.entity.v1.MigrationStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.MigrationRunRequest,
      com.udb.entity.v1.MigrationStatusResponse> getGetMigrationStatusMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.MigrationRunRequest, com.udb.entity.v1.MigrationStatusResponse> getGetMigrationStatusMethod;
    if ((getGetMigrationStatusMethod = DataBrokerGrpc.getGetMigrationStatusMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getGetMigrationStatusMethod = DataBrokerGrpc.getGetMigrationStatusMethod) == null) {
          DataBrokerGrpc.getGetMigrationStatusMethod = getGetMigrationStatusMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.MigrationRunRequest, com.udb.entity.v1.MigrationStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetMigrationStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MigrationRunRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MigrationStatusResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("GetMigrationStatus"))
              .build();
        }
      }
    }
    return getGetMigrationStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.MigrationRunListRequest,
      com.udb.entity.v1.MigrationRunListResponse> getListMigrationRunsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListMigrationRuns",
      requestType = com.udb.entity.v1.MigrationRunListRequest.class,
      responseType = com.udb.entity.v1.MigrationRunListResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.MigrationRunListRequest,
      com.udb.entity.v1.MigrationRunListResponse> getListMigrationRunsMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.MigrationRunListRequest, com.udb.entity.v1.MigrationRunListResponse> getListMigrationRunsMethod;
    if ((getListMigrationRunsMethod = DataBrokerGrpc.getListMigrationRunsMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getListMigrationRunsMethod = DataBrokerGrpc.getListMigrationRunsMethod) == null) {
          DataBrokerGrpc.getListMigrationRunsMethod = getListMigrationRunsMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.MigrationRunListRequest, com.udb.entity.v1.MigrationRunListResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListMigrationRuns"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MigrationRunListRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MigrationRunListResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("ListMigrationRuns"))
              .build();
        }
      }
    }
    return getListMigrationRunsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.MigrationRunRequest,
      com.udb.entity.v1.MigrationStatusResponse> getApproveMigrationPlanMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ApproveMigrationPlan",
      requestType = com.udb.entity.v1.MigrationRunRequest.class,
      responseType = com.udb.entity.v1.MigrationStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.MigrationRunRequest,
      com.udb.entity.v1.MigrationStatusResponse> getApproveMigrationPlanMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.MigrationRunRequest, com.udb.entity.v1.MigrationStatusResponse> getApproveMigrationPlanMethod;
    if ((getApproveMigrationPlanMethod = DataBrokerGrpc.getApproveMigrationPlanMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getApproveMigrationPlanMethod = DataBrokerGrpc.getApproveMigrationPlanMethod) == null) {
          DataBrokerGrpc.getApproveMigrationPlanMethod = getApproveMigrationPlanMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.MigrationRunRequest, com.udb.entity.v1.MigrationStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ApproveMigrationPlan"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MigrationRunRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MigrationStatusResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("ApproveMigrationPlan"))
              .build();
        }
      }
    }
    return getApproveMigrationPlanMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.DlqListRequest,
      com.udb.entity.v1.DlqListResponse> getListDlqEventsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListDlqEvents",
      requestType = com.udb.entity.v1.DlqListRequest.class,
      responseType = com.udb.entity.v1.DlqListResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.DlqListRequest,
      com.udb.entity.v1.DlqListResponse> getListDlqEventsMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.DlqListRequest, com.udb.entity.v1.DlqListResponse> getListDlqEventsMethod;
    if ((getListDlqEventsMethod = DataBrokerGrpc.getListDlqEventsMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getListDlqEventsMethod = DataBrokerGrpc.getListDlqEventsMethod) == null) {
          DataBrokerGrpc.getListDlqEventsMethod = getListDlqEventsMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.DlqListRequest, com.udb.entity.v1.DlqListResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListDlqEvents"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.DlqListRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.DlqListResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("ListDlqEvents"))
              .build();
        }
      }
    }
    return getListDlqEventsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.DlqEventRequest,
      com.udb.entity.v1.DlqEventResponse> getGetDlqEventMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetDlqEvent",
      requestType = com.udb.entity.v1.DlqEventRequest.class,
      responseType = com.udb.entity.v1.DlqEventResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.DlqEventRequest,
      com.udb.entity.v1.DlqEventResponse> getGetDlqEventMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.DlqEventRequest, com.udb.entity.v1.DlqEventResponse> getGetDlqEventMethod;
    if ((getGetDlqEventMethod = DataBrokerGrpc.getGetDlqEventMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getGetDlqEventMethod = DataBrokerGrpc.getGetDlqEventMethod) == null) {
          DataBrokerGrpc.getGetDlqEventMethod = getGetDlqEventMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.DlqEventRequest, com.udb.entity.v1.DlqEventResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetDlqEvent"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.DlqEventRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.DlqEventResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("GetDlqEvent"))
              .build();
        }
      }
    }
    return getGetDlqEventMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.DlqActionRequest,
      com.udb.entity.v1.MutationResponse> getReplayDlqEventMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReplayDlqEvent",
      requestType = com.udb.entity.v1.DlqActionRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.DlqActionRequest,
      com.udb.entity.v1.MutationResponse> getReplayDlqEventMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.DlqActionRequest, com.udb.entity.v1.MutationResponse> getReplayDlqEventMethod;
    if ((getReplayDlqEventMethod = DataBrokerGrpc.getReplayDlqEventMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getReplayDlqEventMethod = DataBrokerGrpc.getReplayDlqEventMethod) == null) {
          DataBrokerGrpc.getReplayDlqEventMethod = getReplayDlqEventMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.DlqActionRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReplayDlqEvent"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.DlqActionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("ReplayDlqEvent"))
              .build();
        }
      }
    }
    return getReplayDlqEventMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.DlqActionRequest,
      com.udb.entity.v1.MutationResponse> getDismissDlqEventMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DismissDlqEvent",
      requestType = com.udb.entity.v1.DlqActionRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.DlqActionRequest,
      com.udb.entity.v1.MutationResponse> getDismissDlqEventMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.DlqActionRequest, com.udb.entity.v1.MutationResponse> getDismissDlqEventMethod;
    if ((getDismissDlqEventMethod = DataBrokerGrpc.getDismissDlqEventMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getDismissDlqEventMethod = DataBrokerGrpc.getDismissDlqEventMethod) == null) {
          DataBrokerGrpc.getDismissDlqEventMethod = getDismissDlqEventMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.DlqActionRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DismissDlqEvent"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.DlqActionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("DismissDlqEvent"))
              .build();
        }
      }
    }
    return getDismissDlqEventMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.DlqActionRequest,
      com.udb.entity.v1.MutationResponse> getQuarantineDlqEventMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "QuarantineDlqEvent",
      requestType = com.udb.entity.v1.DlqActionRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.DlqActionRequest,
      com.udb.entity.v1.MutationResponse> getQuarantineDlqEventMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.DlqActionRequest, com.udb.entity.v1.MutationResponse> getQuarantineDlqEventMethod;
    if ((getQuarantineDlqEventMethod = DataBrokerGrpc.getQuarantineDlqEventMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getQuarantineDlqEventMethod = DataBrokerGrpc.getQuarantineDlqEventMethod) == null) {
          DataBrokerGrpc.getQuarantineDlqEventMethod = getQuarantineDlqEventMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.DlqActionRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "QuarantineDlqEvent"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.DlqActionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("QuarantineDlqEvent"))
              .build();
        }
      }
    }
    return getQuarantineDlqEventMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CdcControlRequest,
      com.udb.entity.v1.CdcStatusResponse> getGetCdcStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetCdcStatus",
      requestType = com.udb.entity.v1.CdcControlRequest.class,
      responseType = com.udb.entity.v1.CdcStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CdcControlRequest,
      com.udb.entity.v1.CdcStatusResponse> getGetCdcStatusMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CdcControlRequest, com.udb.entity.v1.CdcStatusResponse> getGetCdcStatusMethod;
    if ((getGetCdcStatusMethod = DataBrokerGrpc.getGetCdcStatusMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getGetCdcStatusMethod = DataBrokerGrpc.getGetCdcStatusMethod) == null) {
          DataBrokerGrpc.getGetCdcStatusMethod = getGetCdcStatusMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CdcControlRequest, com.udb.entity.v1.CdcStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetCdcStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CdcControlRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CdcStatusResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("GetCdcStatus"))
              .build();
        }
      }
    }
    return getGetCdcStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CdcControlRequest,
      com.udb.entity.v1.CdcStatusResponse> getPauseCdcMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PauseCdc",
      requestType = com.udb.entity.v1.CdcControlRequest.class,
      responseType = com.udb.entity.v1.CdcStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CdcControlRequest,
      com.udb.entity.v1.CdcStatusResponse> getPauseCdcMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CdcControlRequest, com.udb.entity.v1.CdcStatusResponse> getPauseCdcMethod;
    if ((getPauseCdcMethod = DataBrokerGrpc.getPauseCdcMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getPauseCdcMethod = DataBrokerGrpc.getPauseCdcMethod) == null) {
          DataBrokerGrpc.getPauseCdcMethod = getPauseCdcMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CdcControlRequest, com.udb.entity.v1.CdcStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PauseCdc"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CdcControlRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CdcStatusResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("PauseCdc"))
              .build();
        }
      }
    }
    return getPauseCdcMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CdcControlRequest,
      com.udb.entity.v1.CdcStatusResponse> getResumeCdcMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ResumeCdc",
      requestType = com.udb.entity.v1.CdcControlRequest.class,
      responseType = com.udb.entity.v1.CdcStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CdcControlRequest,
      com.udb.entity.v1.CdcStatusResponse> getResumeCdcMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CdcControlRequest, com.udb.entity.v1.CdcStatusResponse> getResumeCdcMethod;
    if ((getResumeCdcMethod = DataBrokerGrpc.getResumeCdcMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getResumeCdcMethod = DataBrokerGrpc.getResumeCdcMethod) == null) {
          DataBrokerGrpc.getResumeCdcMethod = getResumeCdcMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CdcControlRequest, com.udb.entity.v1.CdcStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ResumeCdc"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CdcControlRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CdcStatusResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("ResumeCdc"))
              .build();
        }
      }
    }
    return getResumeCdcMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CdcControlRequest,
      com.udb.entity.v1.CdcStatusResponse> getStepDownCdcLeaderMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StepDownCdcLeader",
      requestType = com.udb.entity.v1.CdcControlRequest.class,
      responseType = com.udb.entity.v1.CdcStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CdcControlRequest,
      com.udb.entity.v1.CdcStatusResponse> getStepDownCdcLeaderMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CdcControlRequest, com.udb.entity.v1.CdcStatusResponse> getStepDownCdcLeaderMethod;
    if ((getStepDownCdcLeaderMethod = DataBrokerGrpc.getStepDownCdcLeaderMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getStepDownCdcLeaderMethod = DataBrokerGrpc.getStepDownCdcLeaderMethod) == null) {
          DataBrokerGrpc.getStepDownCdcLeaderMethod = getStepDownCdcLeaderMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CdcControlRequest, com.udb.entity.v1.CdcStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StepDownCdcLeader"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CdcControlRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CdcStatusResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("StepDownCdcLeader"))
              .build();
        }
      }
    }
    return getStepDownCdcLeaderMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CdcRedactionPreviewRequest,
      com.udb.entity.v1.CdcRedactionPreviewResponse> getPreviewCdcRedactionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PreviewCdcRedaction",
      requestType = com.udb.entity.v1.CdcRedactionPreviewRequest.class,
      responseType = com.udb.entity.v1.CdcRedactionPreviewResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CdcRedactionPreviewRequest,
      com.udb.entity.v1.CdcRedactionPreviewResponse> getPreviewCdcRedactionMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CdcRedactionPreviewRequest, com.udb.entity.v1.CdcRedactionPreviewResponse> getPreviewCdcRedactionMethod;
    if ((getPreviewCdcRedactionMethod = DataBrokerGrpc.getPreviewCdcRedactionMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getPreviewCdcRedactionMethod = DataBrokerGrpc.getPreviewCdcRedactionMethod) == null) {
          DataBrokerGrpc.getPreviewCdcRedactionMethod = getPreviewCdcRedactionMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CdcRedactionPreviewRequest, com.udb.entity.v1.CdcRedactionPreviewResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PreviewCdcRedaction"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CdcRedactionPreviewRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CdcRedactionPreviewResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("PreviewCdcRedaction"))
              .build();
        }
      }
    }
    return getPreviewCdcRedactionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.ProjectionDriftScanRequest,
      com.udb.entity.v1.ProjectionDriftScanResponse> getScanProjectionDriftMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScanProjectionDrift",
      requestType = com.udb.entity.v1.ProjectionDriftScanRequest.class,
      responseType = com.udb.entity.v1.ProjectionDriftScanResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.ProjectionDriftScanRequest,
      com.udb.entity.v1.ProjectionDriftScanResponse> getScanProjectionDriftMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.ProjectionDriftScanRequest, com.udb.entity.v1.ProjectionDriftScanResponse> getScanProjectionDriftMethod;
    if ((getScanProjectionDriftMethod = DataBrokerGrpc.getScanProjectionDriftMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getScanProjectionDriftMethod = DataBrokerGrpc.getScanProjectionDriftMethod) == null) {
          DataBrokerGrpc.getScanProjectionDriftMethod = getScanProjectionDriftMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.ProjectionDriftScanRequest, com.udb.entity.v1.ProjectionDriftScanResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScanProjectionDrift"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.ProjectionDriftScanRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.ProjectionDriftScanResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("ScanProjectionDrift"))
              .build();
        }
      }
    }
    return getScanProjectionDriftMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.SagaListRequest,
      com.udb.entity.v1.SagaListResponse> getListSagasMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListSagas",
      requestType = com.udb.entity.v1.SagaListRequest.class,
      responseType = com.udb.entity.v1.SagaListResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.SagaListRequest,
      com.udb.entity.v1.SagaListResponse> getListSagasMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.SagaListRequest, com.udb.entity.v1.SagaListResponse> getListSagasMethod;
    if ((getListSagasMethod = DataBrokerGrpc.getListSagasMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getListSagasMethod = DataBrokerGrpc.getListSagasMethod) == null) {
          DataBrokerGrpc.getListSagasMethod = getListSagasMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.SagaListRequest, com.udb.entity.v1.SagaListResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListSagas"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.SagaListRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.SagaListResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("ListSagas"))
              .build();
        }
      }
    }
    return getListSagasMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.SagaRequest,
      com.udb.entity.v1.SagaResponse> getGetSagaMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetSaga",
      requestType = com.udb.entity.v1.SagaRequest.class,
      responseType = com.udb.entity.v1.SagaResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.SagaRequest,
      com.udb.entity.v1.SagaResponse> getGetSagaMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.SagaRequest, com.udb.entity.v1.SagaResponse> getGetSagaMethod;
    if ((getGetSagaMethod = DataBrokerGrpc.getGetSagaMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getGetSagaMethod = DataBrokerGrpc.getGetSagaMethod) == null) {
          DataBrokerGrpc.getGetSagaMethod = getGetSagaMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.SagaRequest, com.udb.entity.v1.SagaResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetSaga"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.SagaRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.SagaResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("GetSaga"))
              .build();
        }
      }
    }
    return getGetSagaMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.SagaRequest,
      com.udb.entity.v1.SagaResponse> getRetrySagaCompensationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RetrySagaCompensation",
      requestType = com.udb.entity.v1.SagaRequest.class,
      responseType = com.udb.entity.v1.SagaResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.SagaRequest,
      com.udb.entity.v1.SagaResponse> getRetrySagaCompensationMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.SagaRequest, com.udb.entity.v1.SagaResponse> getRetrySagaCompensationMethod;
    if ((getRetrySagaCompensationMethod = DataBrokerGrpc.getRetrySagaCompensationMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getRetrySagaCompensationMethod = DataBrokerGrpc.getRetrySagaCompensationMethod) == null) {
          DataBrokerGrpc.getRetrySagaCompensationMethod = getRetrySagaCompensationMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.SagaRequest, com.udb.entity.v1.SagaResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RetrySagaCompensation"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.SagaRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.SagaResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("RetrySagaCompensation"))
              .build();
        }
      }
    }
    return getRetrySagaCompensationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.SagaRequest,
      com.udb.entity.v1.SagaResponse> getMarkSagaReviewedMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MarkSagaReviewed",
      requestType = com.udb.entity.v1.SagaRequest.class,
      responseType = com.udb.entity.v1.SagaResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.SagaRequest,
      com.udb.entity.v1.SagaResponse> getMarkSagaReviewedMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.SagaRequest, com.udb.entity.v1.SagaResponse> getMarkSagaReviewedMethod;
    if ((getMarkSagaReviewedMethod = DataBrokerGrpc.getMarkSagaReviewedMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getMarkSagaReviewedMethod = DataBrokerGrpc.getMarkSagaReviewedMethod) == null) {
          DataBrokerGrpc.getMarkSagaReviewedMethod = getMarkSagaReviewedMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.SagaRequest, com.udb.entity.v1.SagaResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MarkSagaReviewed"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.SagaRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.SagaResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("MarkSagaReviewed"))
              .build();
        }
      }
    }
    return getMarkSagaReviewedMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.services.v1.EnsureBaselineRequest,
      com.udb.services.v1.EnsureBaselineResponse> getEnsureBaselineMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "EnsureBaseline",
      requestType = com.udb.services.v1.EnsureBaselineRequest.class,
      responseType = com.udb.services.v1.EnsureBaselineResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.services.v1.EnsureBaselineRequest,
      com.udb.services.v1.EnsureBaselineResponse> getEnsureBaselineMethod() {
    io.grpc.MethodDescriptor<com.udb.services.v1.EnsureBaselineRequest, com.udb.services.v1.EnsureBaselineResponse> getEnsureBaselineMethod;
    if ((getEnsureBaselineMethod = DataBrokerGrpc.getEnsureBaselineMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getEnsureBaselineMethod = DataBrokerGrpc.getEnsureBaselineMethod) == null) {
          DataBrokerGrpc.getEnsureBaselineMethod = getEnsureBaselineMethod =
              io.grpc.MethodDescriptor.<com.udb.services.v1.EnsureBaselineRequest, com.udb.services.v1.EnsureBaselineResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "EnsureBaseline"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.services.v1.EnsureBaselineRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.services.v1.EnsureBaselineResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("EnsureBaseline"))
              .build();
        }
      }
    }
    return getEnsureBaselineMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.PolicyListRequest,
      com.udb.entity.v1.PolicyListResponse> getListPoliciesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListPolicies",
      requestType = com.udb.entity.v1.PolicyListRequest.class,
      responseType = com.udb.entity.v1.PolicyListResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.PolicyListRequest,
      com.udb.entity.v1.PolicyListResponse> getListPoliciesMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.PolicyListRequest, com.udb.entity.v1.PolicyListResponse> getListPoliciesMethod;
    if ((getListPoliciesMethod = DataBrokerGrpc.getListPoliciesMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getListPoliciesMethod = DataBrokerGrpc.getListPoliciesMethod) == null) {
          DataBrokerGrpc.getListPoliciesMethod = getListPoliciesMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.PolicyListRequest, com.udb.entity.v1.PolicyListResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListPolicies"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.PolicyListRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.PolicyListResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("ListPolicies"))
              .build();
        }
      }
    }
    return getListPoliciesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.PutPolicyRequest,
      com.udb.entity.v1.MutationResponse> getPutPolicyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PutPolicy",
      requestType = com.udb.entity.v1.PutPolicyRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.PutPolicyRequest,
      com.udb.entity.v1.MutationResponse> getPutPolicyMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.PutPolicyRequest, com.udb.entity.v1.MutationResponse> getPutPolicyMethod;
    if ((getPutPolicyMethod = DataBrokerGrpc.getPutPolicyMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getPutPolicyMethod = DataBrokerGrpc.getPutPolicyMethod) == null) {
          DataBrokerGrpc.getPutPolicyMethod = getPutPolicyMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.PutPolicyRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PutPolicy"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.PutPolicyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("PutPolicy"))
              .build();
        }
      }
    }
    return getPutPolicyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.PolicyRequest,
      com.udb.entity.v1.MutationResponse> getDeletePolicyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeletePolicy",
      requestType = com.udb.entity.v1.PolicyRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.PolicyRequest,
      com.udb.entity.v1.MutationResponse> getDeletePolicyMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.PolicyRequest, com.udb.entity.v1.MutationResponse> getDeletePolicyMethod;
    if ((getDeletePolicyMethod = DataBrokerGrpc.getDeletePolicyMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getDeletePolicyMethod = DataBrokerGrpc.getDeletePolicyMethod) == null) {
          DataBrokerGrpc.getDeletePolicyMethod = getDeletePolicyMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.PolicyRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeletePolicy"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.PolicyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("DeletePolicy"))
              .build();
        }
      }
    }
    return getDeletePolicyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CapabilitiesRequest,
      com.udb.entity.v1.MutationResponse> getReloadPoliciesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReloadPolicies",
      requestType = com.udb.entity.v1.CapabilitiesRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CapabilitiesRequest,
      com.udb.entity.v1.MutationResponse> getReloadPoliciesMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CapabilitiesRequest, com.udb.entity.v1.MutationResponse> getReloadPoliciesMethod;
    if ((getReloadPoliciesMethod = DataBrokerGrpc.getReloadPoliciesMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getReloadPoliciesMethod = DataBrokerGrpc.getReloadPoliciesMethod) == null) {
          DataBrokerGrpc.getReloadPoliciesMethod = getReloadPoliciesMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CapabilitiesRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReloadPolicies"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CapabilitiesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("ReloadPolicies"))
              .build();
        }
      }
    }
    return getReloadPoliciesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CapabilitiesRequest,
      com.udb.entity.v1.PolicyLintResponse> getLintPoliciesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "LintPolicies",
      requestType = com.udb.entity.v1.CapabilitiesRequest.class,
      responseType = com.udb.entity.v1.PolicyLintResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CapabilitiesRequest,
      com.udb.entity.v1.PolicyLintResponse> getLintPoliciesMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CapabilitiesRequest, com.udb.entity.v1.PolicyLintResponse> getLintPoliciesMethod;
    if ((getLintPoliciesMethod = DataBrokerGrpc.getLintPoliciesMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getLintPoliciesMethod = DataBrokerGrpc.getLintPoliciesMethod) == null) {
          DataBrokerGrpc.getLintPoliciesMethod = getLintPoliciesMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CapabilitiesRequest, com.udb.entity.v1.PolicyLintResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "LintPolicies"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CapabilitiesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.PolicyLintResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("LintPolicies"))
              .build();
        }
      }
    }
    return getLintPoliciesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CapabilitiesRequest,
      com.udb.entity.v1.CapabilitiesResponse> getGetCapabilitiesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetCapabilities",
      requestType = com.udb.entity.v1.CapabilitiesRequest.class,
      responseType = com.udb.entity.v1.CapabilitiesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CapabilitiesRequest,
      com.udb.entity.v1.CapabilitiesResponse> getGetCapabilitiesMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CapabilitiesRequest, com.udb.entity.v1.CapabilitiesResponse> getGetCapabilitiesMethod;
    if ((getGetCapabilitiesMethod = DataBrokerGrpc.getGetCapabilitiesMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getGetCapabilitiesMethod = DataBrokerGrpc.getGetCapabilitiesMethod) == null) {
          DataBrokerGrpc.getGetCapabilitiesMethod = getGetCapabilitiesMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CapabilitiesRequest, com.udb.entity.v1.CapabilitiesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetCapabilities"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CapabilitiesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CapabilitiesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("GetCapabilities"))
              .build();
        }
      }
    }
    return getGetCapabilitiesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.CatalogManifestRequest,
      com.udb.entity.v1.CatalogManifestResponse> getGetCatalogManifestMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetCatalogManifest",
      requestType = com.udb.entity.v1.CatalogManifestRequest.class,
      responseType = com.udb.entity.v1.CatalogManifestResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.CatalogManifestRequest,
      com.udb.entity.v1.CatalogManifestResponse> getGetCatalogManifestMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.CatalogManifestRequest, com.udb.entity.v1.CatalogManifestResponse> getGetCatalogManifestMethod;
    if ((getGetCatalogManifestMethod = DataBrokerGrpc.getGetCatalogManifestMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getGetCatalogManifestMethod = DataBrokerGrpc.getGetCatalogManifestMethod) == null) {
          DataBrokerGrpc.getGetCatalogManifestMethod = getGetCatalogManifestMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.CatalogManifestRequest, com.udb.entity.v1.CatalogManifestResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetCatalogManifest"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CatalogManifestRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.CatalogManifestResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("GetCatalogManifest"))
              .build();
        }
      }
    }
    return getGetCatalogManifestMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.MessageSchemaLookupRequest,
      com.udb.entity.v1.MessageSchemaLookupResponse> getLookupMessageSchemaMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "LookupMessageSchema",
      requestType = com.udb.entity.v1.MessageSchemaLookupRequest.class,
      responseType = com.udb.entity.v1.MessageSchemaLookupResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.MessageSchemaLookupRequest,
      com.udb.entity.v1.MessageSchemaLookupResponse> getLookupMessageSchemaMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.MessageSchemaLookupRequest, com.udb.entity.v1.MessageSchemaLookupResponse> getLookupMessageSchemaMethod;
    if ((getLookupMessageSchemaMethod = DataBrokerGrpc.getLookupMessageSchemaMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getLookupMessageSchemaMethod = DataBrokerGrpc.getLookupMessageSchemaMethod) == null) {
          DataBrokerGrpc.getLookupMessageSchemaMethod = getLookupMessageSchemaMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.MessageSchemaLookupRequest, com.udb.entity.v1.MessageSchemaLookupResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "LookupMessageSchema"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MessageSchemaLookupRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MessageSchemaLookupResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("LookupMessageSchema"))
              .build();
        }
      }
    }
    return getLookupMessageSchemaMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.MessageSchemaListRequest,
      com.udb.entity.v1.MessageSchemaListResponse> getListMessageSchemasMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListMessageSchemas",
      requestType = com.udb.entity.v1.MessageSchemaListRequest.class,
      responseType = com.udb.entity.v1.MessageSchemaListResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.MessageSchemaListRequest,
      com.udb.entity.v1.MessageSchemaListResponse> getListMessageSchemasMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.MessageSchemaListRequest, com.udb.entity.v1.MessageSchemaListResponse> getListMessageSchemasMethod;
    if ((getListMessageSchemasMethod = DataBrokerGrpc.getListMessageSchemasMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getListMessageSchemasMethod = DataBrokerGrpc.getListMessageSchemasMethod) == null) {
          DataBrokerGrpc.getListMessageSchemasMethod = getListMessageSchemasMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.MessageSchemaListRequest, com.udb.entity.v1.MessageSchemaListResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListMessageSchemas"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MessageSchemaListRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MessageSchemaListResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("ListMessageSchemas"))
              .build();
        }
      }
    }
    return getListMessageSchemasMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.HealthReportRequest,
      com.udb.entity.v1.HealthReportResponse> getGetHealthReportMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetHealthReport",
      requestType = com.udb.entity.v1.HealthReportRequest.class,
      responseType = com.udb.entity.v1.HealthReportResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.HealthReportRequest,
      com.udb.entity.v1.HealthReportResponse> getGetHealthReportMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.HealthReportRequest, com.udb.entity.v1.HealthReportResponse> getGetHealthReportMethod;
    if ((getGetHealthReportMethod = DataBrokerGrpc.getGetHealthReportMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getGetHealthReportMethod = DataBrokerGrpc.getGetHealthReportMethod) == null) {
          DataBrokerGrpc.getGetHealthReportMethod = getGetHealthReportMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.HealthReportRequest, com.udb.entity.v1.HealthReportResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetHealthReport"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.HealthReportRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.HealthReportResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("GetHealthReport"))
              .build();
        }
      }
    }
    return getGetHealthReportMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.EnsureProjectRequest,
      com.udb.entity.v1.MutationResponse> getEnsureProjectMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "EnsureProject",
      requestType = com.udb.entity.v1.EnsureProjectRequest.class,
      responseType = com.udb.entity.v1.MutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.EnsureProjectRequest,
      com.udb.entity.v1.MutationResponse> getEnsureProjectMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.EnsureProjectRequest, com.udb.entity.v1.MutationResponse> getEnsureProjectMethod;
    if ((getEnsureProjectMethod = DataBrokerGrpc.getEnsureProjectMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getEnsureProjectMethod = DataBrokerGrpc.getEnsureProjectMethod) == null) {
          DataBrokerGrpc.getEnsureProjectMethod = getEnsureProjectMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.EnsureProjectRequest, com.udb.entity.v1.MutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "EnsureProject"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.EnsureProjectRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.MutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("EnsureProject"))
              .build();
        }
      }
    }
    return getEnsureProjectMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.ProjectListRequest,
      com.udb.entity.v1.ProjectListResponse> getListProjectsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListProjects",
      requestType = com.udb.entity.v1.ProjectListRequest.class,
      responseType = com.udb.entity.v1.ProjectListResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.ProjectListRequest,
      com.udb.entity.v1.ProjectListResponse> getListProjectsMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.ProjectListRequest, com.udb.entity.v1.ProjectListResponse> getListProjectsMethod;
    if ((getListProjectsMethod = DataBrokerGrpc.getListProjectsMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getListProjectsMethod = DataBrokerGrpc.getListProjectsMethod) == null) {
          DataBrokerGrpc.getListProjectsMethod = getListProjectsMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.ProjectListRequest, com.udb.entity.v1.ProjectListResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListProjects"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.ProjectListRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.ProjectListResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("ListProjects"))
              .build();
        }
      }
    }
    return getListProjectsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.AdminSummaryRequest,
      com.udb.entity.v1.AdminSummaryResponse> getGetAdminSummaryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetAdminSummary",
      requestType = com.udb.entity.v1.AdminSummaryRequest.class,
      responseType = com.udb.entity.v1.AdminSummaryResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.AdminSummaryRequest,
      com.udb.entity.v1.AdminSummaryResponse> getGetAdminSummaryMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.AdminSummaryRequest, com.udb.entity.v1.AdminSummaryResponse> getGetAdminSummaryMethod;
    if ((getGetAdminSummaryMethod = DataBrokerGrpc.getGetAdminSummaryMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getGetAdminSummaryMethod = DataBrokerGrpc.getGetAdminSummaryMethod) == null) {
          DataBrokerGrpc.getGetAdminSummaryMethod = getGetAdminSummaryMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.AdminSummaryRequest, com.udb.entity.v1.AdminSummaryResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetAdminSummary"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.AdminSummaryRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.AdminSummaryResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("GetAdminSummary"))
              .build();
        }
      }
    }
    return getGetAdminSummaryMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.AdminAuditLogRequest,
      com.udb.entity.v1.AdminAuditLogResponse> getListAdminAuditLogsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListAdminAuditLogs",
      requestType = com.udb.entity.v1.AdminAuditLogRequest.class,
      responseType = com.udb.entity.v1.AdminAuditLogResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.AdminAuditLogRequest,
      com.udb.entity.v1.AdminAuditLogResponse> getListAdminAuditLogsMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.AdminAuditLogRequest, com.udb.entity.v1.AdminAuditLogResponse> getListAdminAuditLogsMethod;
    if ((getListAdminAuditLogsMethod = DataBrokerGrpc.getListAdminAuditLogsMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getListAdminAuditLogsMethod = DataBrokerGrpc.getListAdminAuditLogsMethod) == null) {
          DataBrokerGrpc.getListAdminAuditLogsMethod = getListAdminAuditLogsMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.AdminAuditLogRequest, com.udb.entity.v1.AdminAuditLogResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListAdminAuditLogs"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.AdminAuditLogRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.AdminAuditLogResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("ListAdminAuditLogs"))
              .build();
        }
      }
    }
    return getListAdminAuditLogsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.entity.v1.AdminAuditVerifyRequest,
      com.udb.entity.v1.AdminAuditVerifyResponse> getVerifyAdminAuditLogMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "VerifyAdminAuditLog",
      requestType = com.udb.entity.v1.AdminAuditVerifyRequest.class,
      responseType = com.udb.entity.v1.AdminAuditVerifyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.entity.v1.AdminAuditVerifyRequest,
      com.udb.entity.v1.AdminAuditVerifyResponse> getVerifyAdminAuditLogMethod() {
    io.grpc.MethodDescriptor<com.udb.entity.v1.AdminAuditVerifyRequest, com.udb.entity.v1.AdminAuditVerifyResponse> getVerifyAdminAuditLogMethod;
    if ((getVerifyAdminAuditLogMethod = DataBrokerGrpc.getVerifyAdminAuditLogMethod) == null) {
      synchronized (DataBrokerGrpc.class) {
        if ((getVerifyAdminAuditLogMethod = DataBrokerGrpc.getVerifyAdminAuditLogMethod) == null) {
          DataBrokerGrpc.getVerifyAdminAuditLogMethod = getVerifyAdminAuditLogMethod =
              io.grpc.MethodDescriptor.<com.udb.entity.v1.AdminAuditVerifyRequest, com.udb.entity.v1.AdminAuditVerifyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "VerifyAdminAuditLog"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.AdminAuditVerifyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.entity.v1.AdminAuditVerifyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DataBrokerMethodDescriptorSupplier("VerifyAdminAuditLog"))
              .build();
        }
      }
    }
    return getVerifyAdminAuditLogMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DataBrokerStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DataBrokerStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DataBrokerStub>() {
        @java.lang.Override
        public DataBrokerStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DataBrokerStub(channel, callOptions);
        }
      };
    return DataBrokerStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static DataBrokerBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DataBrokerBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DataBrokerBlockingV2Stub>() {
        @java.lang.Override
        public DataBrokerBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DataBrokerBlockingV2Stub(channel, callOptions);
        }
      };
    return DataBrokerBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DataBrokerBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DataBrokerBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DataBrokerBlockingStub>() {
        @java.lang.Override
        public DataBrokerBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DataBrokerBlockingStub(channel, callOptions);
        }
      };
    return DataBrokerBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DataBrokerFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DataBrokerFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DataBrokerFutureStub>() {
        @java.lang.Override
        public DataBrokerFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DataBrokerFutureStub(channel, callOptions);
        }
      };
    return DataBrokerFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * DataBroker is the UDB-owned wire protocol. Project protos are parsed only as
   * schema input; they do not need to contain or import this service contract.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * ── Relational ─────────────────────────────────────────────────────────────
     * </pre>
     */
    default void select(com.udb.entity.v1.SelectRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.RecordSet> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSelectMethod(), responseObserver);
    }

    /**
     */
    default io.grpc.stub.StreamObserver<com.udb.entity.v1.SelectRequest> batchSelect(
        io.grpc.stub.StreamObserver<com.udb.entity.v1.RecordSet> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getBatchSelectMethod(), responseObserver);
    }

    /**
     * <pre>
     * Additive typed columnar read. Reuses SelectRequest; streams RecordBatchV2.
     * Clients use this only when ProtocolSupport.encodings advertises
     * "record_batch_v2" and otherwise fall back to Select/RecordSet.
     * </pre>
     */
    default void selectV2(com.udb.entity.v1.SelectRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.RecordBatchV2> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSelectV2Method(), responseObserver);
    }

    /**
     */
    default void upsert(com.udb.entity.v1.UpsertRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpsertMethod(), responseObserver);
    }

    /**
     */
    default io.grpc.stub.StreamObserver<com.udb.entity.v1.UpsertRequest> batchUpsert(
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getBatchUpsertMethod(), responseObserver);
    }

    /**
     * <pre>
     * Delete rows without raw SQL.
     * </pre>
     */
    default void delete(com.udb.entity.v1.DeleteRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteMethod(), responseObserver);
    }

    /**
     * <pre>
     * Partial update: SET named columns and/or apply atomic increments on the
     * matched rows — no full-record resend, no read-modify-write counter window.
     * Same filter language, tenant isolation and CAS (`expected`) as
     * Upsert/Delete. A retried keyed Update is deduped in the write tx
     * (fail-closed, tenant+project-scoped durable dedup) and returns
     * was_duplicate=true with the original body.
     * </pre>
     */
    default void update(com.udb.entity.v1.UpdateRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── Vector ──────────────────────────────────────────────────────────────────
     * </pre>
     */
    default void vectorSearch(com.udb.entity.v1.VectorSearchRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.VectorSet> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getVectorSearchMethod(), responseObserver);
    }

    /**
     */
    default void vectorHybridSearch(com.udb.entity.v1.VectorHybridSearchRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.VectorSet> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getVectorHybridSearchMethod(), responseObserver);
    }

    /**
     */
    default void vectorUpsert(com.udb.entity.v1.VectorUpsertRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getVectorUpsertMethod(), responseObserver);
    }

    /**
     */
    default io.grpc.stub.StreamObserver<com.udb.entity.v1.VectorUpsertRequest> vectorBatchUpsert(
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getVectorBatchUpsertMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── Blob ───────────────────────────────────────────────────────────────────
     * </pre>
     */
    default io.grpc.stub.StreamObserver<com.udb.entity.v1.Chunk> putObject(
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getPutObjectMethod(), responseObserver);
    }

    /**
     */
    default void getObject(com.udb.entity.v1.ObjectRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.Chunk> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetObjectMethod(), responseObserver);
    }

    /**
     */
    default void generatePresignedUrl(com.udb.entity.v1.UrlRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.UrlResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGeneratePresignedUrlMethod(), responseObserver);
    }

    /**
     */
    default void initiateMultipartUpload(com.udb.entity.v1.MultipartUploadRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MultipartUploadResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getInitiateMultipartUploadMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── Cache / KV ─────────────────────────────────────────────────────────────
     * </pre>
     */
    default void cacheGet(com.udb.entity.v1.CacheGetRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CacheGetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCacheGetMethod(), responseObserver);
    }

    /**
     */
    default void cacheSet(com.udb.entity.v1.CacheSetRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCacheSetMethod(), responseObserver);
    }

    /**
     */
    default void cacheDelete(com.udb.entity.v1.CacheDeleteRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCacheDeleteMethod(), responseObserver);
    }

    /**
     */
    default void cacheScan(com.udb.entity.v1.CacheScanRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CacheScanResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCacheScanMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── Document / Graph / Time-Series / Analytical Stores ────────────────────
     * </pre>
     */
    default void documentGet(com.udb.entity.v1.DocumentGetRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.DocumentSet> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDocumentGetMethod(), responseObserver);
    }

    /**
     */
    default void documentFind(com.udb.entity.v1.DocumentFindRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.DocumentSet> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDocumentFindMethod(), responseObserver);
    }

    /**
     */
    default void documentUpsert(com.udb.entity.v1.DocumentUpsertRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDocumentUpsertMethod(), responseObserver);
    }

    /**
     */
    default void documentDelete(com.udb.entity.v1.DocumentDeleteRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDocumentDeleteMethod(), responseObserver);
    }

    /**
     */
    default void graphQuery(com.udb.entity.v1.GraphQueryRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.GraphResultSet> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGraphQueryMethod(), responseObserver);
    }

    /**
     */
    default void graphMutate(com.udb.entity.v1.GraphMutationRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGraphMutateMethod(), responseObserver);
    }

    /**
     */
    default void timeSeriesWrite(com.udb.entity.v1.TimeSeriesWriteRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTimeSeriesWriteMethod(), responseObserver);
    }

    /**
     */
    default void timeSeriesQuery(com.udb.entity.v1.TimeSeriesQueryRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.TimeSeriesQueryResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTimeSeriesQueryMethod(), responseObserver);
    }

    /**
     */
    default void analyticalQuery(com.udb.entity.v1.AnalyticalQueryRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.AnalyticalQueryResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAnalyticalQueryMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── Tx / CDC ───────────────────────────────────────────────────────────────
     * </pre>
     */
    default io.grpc.stub.StreamObserver<com.udb.entity.v1.Mutation> beginTx(
        io.grpc.stub.StreamObserver<com.udb.entity.v1.TxStatus> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getBeginTxMethod(), responseObserver);
    }

    /**
     */
    default void publishCDC(com.udb.entity.v1.CDCSubscriptionRequest request,
        io.grpc.stub.StreamObserver<com.udb.events.v1.CDCEnvelope> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPublishCDCMethod(), responseObserver);
    }

    /**
     */
    default void createMaterializedView(com.udb.entity.v1.ViewDefinition request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateMaterializedViewMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── First-Class Event API ─────────────────────────────────────────────────
     * </pre>
     */
    default void enqueueOutboxEvent(com.udb.entity.v1.EnqueueOutboxEventRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.EnqueueOutboxEventResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEnqueueOutboxEventMethod(), responseObserver);
    }

    /**
     * <pre>
     * Generic resource administration.
     * Dispatch a lifecycle operation to any configured backend executor.
     * Requires scope: udb:dispatch
     * </pre>
     */
    default void genericDispatch(com.udb.entity.v1.GenericDispatchRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.GenericDispatchResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGenericDispatchMethod(), responseObserver);
    }

    /**
     * <pre>
     * Ensure a named resource exists on the target backend.
     * Requires scope: udb:admin
     * </pre>
     */
    default void ensureResource(com.udb.entity.v1.ResourceAdminRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEnsureResourceMethod(), responseObserver);
    }

    /**
     * <pre>
     * Drop a named resource on the target backend.
     * Requires scope: udb:admin
     * </pre>
     */
    default void dropResource(com.udb.entity.v1.ResourceAdminRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDropResourceMethod(), responseObserver);
    }

    /**
     * <pre>
     * List all resources managed by the target backend.
     * Requires scope: udb:admin
     * </pre>
     */
    default void listResources(com.udb.entity.v1.ResourceAdminRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.ResourceListResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListResourcesMethod(), responseObserver);
    }

    /**
     * <pre>
     * Catalog administration.
     * Stage a new catalog manifest version (validate + store as STAGED).
     * Requires scope: udb:admin
     * </pre>
     */
    default void stageCatalog(com.udb.entity.v1.StageCatalogRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogVersionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStageCatalogMethod(), responseObserver);
    }

    /**
     * <pre>
     * Activate a STAGED catalog version.
     * Requires scope: udb:admin
     * </pre>
     */
    default void activateCatalog(com.udb.entity.v1.CatalogVersionRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogVersionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getActivateCatalogMethod(), responseObserver);
    }

    /**
     * <pre>
     * Roll back to the previous ACTIVE catalog version.
     * Requires scope: udb:admin
     * </pre>
     */
    default void rollbackCatalog(com.udb.entity.v1.CatalogVersionRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogVersionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRollbackCatalogMethod(), responseObserver);
    }

    /**
     * <pre>
     * Validate a catalog manifest JSON without storing it.
     * Requires scope: udb:admin
     * </pre>
     */
    default void validateCatalog(com.udb.entity.v1.StageCatalogRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogValidationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getValidateCatalogMethod(), responseObserver);
    }

    /**
     * <pre>
     * Return the list of known catalog versions.
     * Requires scope: udb:admin
     * </pre>
     */
    default void getCatalogVersions(com.udb.entity.v1.CatalogManifestRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogVersionListResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetCatalogVersionsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Return one catalog version by catalog_id/version, or active version when empty.
     * Requires scope: udb:admin
     * </pre>
     */
    default void getCatalogVersion(com.udb.entity.v1.CatalogVersionRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogVersionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetCatalogVersionMethod(), responseObserver);
    }

    /**
     * <pre>
     * Migration planning and apply.
     * Plan a migration against the active catalog without executing it.
     * Requires scope: udb:admin
     * </pre>
     */
    default void planMigration(com.udb.entity.v1.MigrationPlanRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MigrationPlanResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPlanMigrationMethod(), responseObserver);
    }

    /**
     * <pre>
     * Apply a previously planned (and optionally approved) migration.
     * Requires scope: udb:admin
     * </pre>
     */
    default void applyMigration(com.udb.entity.v1.MigrationApplyRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MigrationStatusResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getApplyMigrationMethod(), responseObserver);
    }

    /**
     * <pre>
     * Return the status of a migration run.
     * Requires scope: udb:admin
     * </pre>
     */
    default void getMigrationStatus(com.udb.entity.v1.MigrationRunRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MigrationStatusResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetMigrationStatusMethod(), responseObserver);
    }

    /**
     * <pre>
     * Return migration runs for an admin console page.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * </pre>
     */
    default void listMigrationRuns(com.udb.entity.v1.MigrationRunListRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MigrationRunListResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListMigrationRunsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Approve a migration plan that requires review.
     * Requires scope: udb:admin
     * </pre>
     */
    default void approveMigrationPlan(com.udb.entity.v1.MigrationRunRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MigrationStatusResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getApproveMigrationPlanMethod(), responseObserver);
    }

    /**
     * <pre>
     * DLQ management.
     * </pre>
     */
    default void listDlqEvents(com.udb.entity.v1.DlqListRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.DlqListResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListDlqEventsMethod(), responseObserver);
    }

    /**
     */
    default void getDlqEvent(com.udb.entity.v1.DlqEventRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.DlqEventResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetDlqEventMethod(), responseObserver);
    }

    /**
     */
    default void replayDlqEvent(com.udb.entity.v1.DlqActionRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReplayDlqEventMethod(), responseObserver);
    }

    /**
     */
    default void dismissDlqEvent(com.udb.entity.v1.DlqActionRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDismissDlqEventMethod(), responseObserver);
    }

    /**
     */
    default void quarantineDlqEvent(com.udb.entity.v1.DlqActionRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQuarantineDlqEventMethod(), responseObserver);
    }

    /**
     * <pre>
     * CDC control plane.
     * </pre>
     */
    default void getCdcStatus(com.udb.entity.v1.CdcControlRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CdcStatusResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetCdcStatusMethod(), responseObserver);
    }

    /**
     */
    default void pauseCdc(com.udb.entity.v1.CdcControlRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CdcStatusResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPauseCdcMethod(), responseObserver);
    }

    /**
     */
    default void resumeCdc(com.udb.entity.v1.CdcControlRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CdcStatusResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getResumeCdcMethod(), responseObserver);
    }

    /**
     */
    default void stepDownCdcLeader(com.udb.entity.v1.CdcControlRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CdcStatusResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStepDownCdcLeaderMethod(), responseObserver);
    }

    /**
     */
    default void previewCdcRedaction(com.udb.entity.v1.CdcRedactionPreviewRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CdcRedactionPreviewResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPreviewCdcRedactionMethod(), responseObserver);
    }

    /**
     */
    default void scanProjectionDrift(com.udb.entity.v1.ProjectionDriftScanRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.ProjectionDriftScanResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScanProjectionDriftMethod(), responseObserver);
    }

    /**
     * <pre>
     * Saga administration.
     * </pre>
     */
    default void listSagas(com.udb.entity.v1.SagaListRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.SagaListResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListSagasMethod(), responseObserver);
    }

    /**
     */
    default void getSaga(com.udb.entity.v1.SagaRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.SagaResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetSagaMethod(), responseObserver);
    }

    /**
     */
    default void retrySagaCompensation(com.udb.entity.v1.SagaRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.SagaResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRetrySagaCompensationMethod(), responseObserver);
    }

    /**
     */
    default void markSagaReviewed(com.udb.entity.v1.SagaRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.SagaResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMarkSagaReviewedMethod(), responseObserver);
    }

    /**
     * <pre>
     * Idempotently seed a baseline manual-review saga row and a retryable DLQ row
     * for the VERIFIED principal's tenant/project. Privilege-creating: fail-closed,
     * env-gated (UDB_ENABLE_ADMIN_SEED) and requires scope: udb:admin.
     * </pre>
     */
    default void ensureBaseline(com.udb.services.v1.EnsureBaselineRequest request,
        io.grpc.stub.StreamObserver<com.udb.services.v1.EnsureBaselineResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEnsureBaselineMethod(), responseObserver);
    }

    /**
     * <pre>
     * Policy administration.
     * </pre>
     */
    default void listPolicies(com.udb.entity.v1.PolicyListRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.PolicyListResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListPoliciesMethod(), responseObserver);
    }

    /**
     */
    default void putPolicy(com.udb.entity.v1.PutPolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPutPolicyMethod(), responseObserver);
    }

    /**
     */
    default void deletePolicy(com.udb.entity.v1.PolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeletePolicyMethod(), responseObserver);
    }

    /**
     */
    default void reloadPolicies(com.udb.entity.v1.CapabilitiesRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReloadPoliciesMethod(), responseObserver);
    }

    /**
     */
    default void lintPolicies(com.udb.entity.v1.CapabilitiesRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.PolicyLintResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getLintPoliciesMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── Admin API ─────────────────────────────────────────────────────────────
     * </pre>
     */
    default void getCapabilities(com.udb.entity.v1.CapabilitiesRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CapabilitiesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetCapabilitiesMethod(), responseObserver);
    }

    /**
     */
    default void getCatalogManifest(com.udb.entity.v1.CatalogManifestRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogManifestResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetCatalogManifestMethod(), responseObserver);
    }

    /**
     * <pre>
     * Runtime schema lookup for SDK/data operation compatibility negotiation.
     * </pre>
     */
    default void lookupMessageSchema(com.udb.entity.v1.MessageSchemaLookupRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MessageSchemaLookupResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getLookupMessageSchemaMethod(), responseObserver);
    }

    /**
     */
    default void listMessageSchemas(com.udb.entity.v1.MessageSchemaListRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MessageSchemaListResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListMessageSchemasMethod(), responseObserver);
    }

    /**
     */
    default void getHealthReport(com.udb.entity.v1.HealthReportRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.HealthReportResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetHealthReportMethod(), responseObserver);
    }

    /**
     * <pre>
     * Multi-project registry.
     * Ensure a project namespace exists (idempotent).
     * Requires scope: udb:admin
     * </pre>
     */
    default void ensureProject(com.udb.entity.v1.EnsureProjectRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEnsureProjectMethod(), responseObserver);
    }

    /**
     * <pre>
     * List all registered project namespaces.
     * Requires scope: udb:admin
     * </pre>
     */
    default void listProjects(com.udb.entity.v1.ProjectListRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.ProjectListResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListProjectsMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── Unified Admin Surface ────────────────────────────────────────────────
     * Returns a single snapshot covering catalog, CDC, saga, backend, and policy
     * state for the admin console. Requires scope: udb:admin.
     * </pre>
     */
    default void getAdminSummary(com.udb.entity.v1.AdminSummaryRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.AdminSummaryResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetAdminSummaryMethod(), responseObserver);
    }

    /**
     * <pre>
     * Paginated admin audit log view for the admin console.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * </pre>
     */
    default void listAdminAuditLogs(com.udb.entity.v1.AdminAuditLogRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.AdminAuditLogResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListAdminAuditLogsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Verifies the admin audit log hash chain and reports the first broken link.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * </pre>
     */
    default void verifyAdminAuditLog(com.udb.entity.v1.AdminAuditVerifyRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.AdminAuditVerifyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getVerifyAdminAuditLogMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service DataBroker.
   * <pre>
   * DataBroker is the UDB-owned wire protocol. Project protos are parsed only as
   * schema input; they do not need to contain or import this service contract.
   * </pre>
   */
  public static abstract class DataBrokerImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return DataBrokerGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service DataBroker.
   * <pre>
   * DataBroker is the UDB-owned wire protocol. Project protos are parsed only as
   * schema input; they do not need to contain or import this service contract.
   * </pre>
   */
  public static final class DataBrokerStub
      extends io.grpc.stub.AbstractAsyncStub<DataBrokerStub> {
    private DataBrokerStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DataBrokerStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DataBrokerStub(channel, callOptions);
    }

    /**
     * <pre>
     * ── Relational ─────────────────────────────────────────────────────────────
     * </pre>
     */
    public void select(com.udb.entity.v1.SelectRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.RecordSet> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSelectMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.udb.entity.v1.SelectRequest> batchSelect(
        io.grpc.stub.StreamObserver<com.udb.entity.v1.RecordSet> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getBatchSelectMethod(), getCallOptions()), responseObserver);
    }

    /**
     * <pre>
     * Additive typed columnar read. Reuses SelectRequest; streams RecordBatchV2.
     * Clients use this only when ProtocolSupport.encodings advertises
     * "record_batch_v2" and otherwise fall back to Select/RecordSet.
     * </pre>
     */
    public void selectV2(com.udb.entity.v1.SelectRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.RecordBatchV2> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getSelectV2Method(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void upsert(com.udb.entity.v1.UpsertRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpsertMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.udb.entity.v1.UpsertRequest> batchUpsert(
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getBatchUpsertMethod(), getCallOptions()), responseObserver);
    }

    /**
     * <pre>
     * Delete rows without raw SQL.
     * </pre>
     */
    public void delete(com.udb.entity.v1.DeleteRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Partial update: SET named columns and/or apply atomic increments on the
     * matched rows — no full-record resend, no read-modify-write counter window.
     * Same filter language, tenant isolation and CAS (`expected`) as
     * Upsert/Delete. A retried keyed Update is deduped in the write tx
     * (fail-closed, tenant+project-scoped durable dedup) and returns
     * was_duplicate=true with the original body.
     * </pre>
     */
    public void update(com.udb.entity.v1.UpdateRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── Vector ──────────────────────────────────────────────────────────────────
     * </pre>
     */
    public void vectorSearch(com.udb.entity.v1.VectorSearchRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.VectorSet> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getVectorSearchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void vectorHybridSearch(com.udb.entity.v1.VectorHybridSearchRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.VectorSet> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getVectorHybridSearchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void vectorUpsert(com.udb.entity.v1.VectorUpsertRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getVectorUpsertMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.udb.entity.v1.VectorUpsertRequest> vectorBatchUpsert(
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getVectorBatchUpsertMethod(), getCallOptions()), responseObserver);
    }

    /**
     * <pre>
     * ── Blob ───────────────────────────────────────────────────────────────────
     * </pre>
     */
    public io.grpc.stub.StreamObserver<com.udb.entity.v1.Chunk> putObject(
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncClientStreamingCall(
          getChannel().newCall(getPutObjectMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public void getObject(com.udb.entity.v1.ObjectRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.Chunk> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getGetObjectMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void generatePresignedUrl(com.udb.entity.v1.UrlRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.UrlResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGeneratePresignedUrlMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void initiateMultipartUpload(com.udb.entity.v1.MultipartUploadRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MultipartUploadResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getInitiateMultipartUploadMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── Cache / KV ─────────────────────────────────────────────────────────────
     * </pre>
     */
    public void cacheGet(com.udb.entity.v1.CacheGetRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CacheGetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCacheGetMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void cacheSet(com.udb.entity.v1.CacheSetRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCacheSetMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void cacheDelete(com.udb.entity.v1.CacheDeleteRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCacheDeleteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void cacheScan(com.udb.entity.v1.CacheScanRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CacheScanResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCacheScanMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── Document / Graph / Time-Series / Analytical Stores ────────────────────
     * </pre>
     */
    public void documentGet(com.udb.entity.v1.DocumentGetRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.DocumentSet> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDocumentGetMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void documentFind(com.udb.entity.v1.DocumentFindRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.DocumentSet> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDocumentFindMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void documentUpsert(com.udb.entity.v1.DocumentUpsertRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDocumentUpsertMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void documentDelete(com.udb.entity.v1.DocumentDeleteRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDocumentDeleteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void graphQuery(com.udb.entity.v1.GraphQueryRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.GraphResultSet> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGraphQueryMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void graphMutate(com.udb.entity.v1.GraphMutationRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGraphMutateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void timeSeriesWrite(com.udb.entity.v1.TimeSeriesWriteRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTimeSeriesWriteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void timeSeriesQuery(com.udb.entity.v1.TimeSeriesQueryRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.TimeSeriesQueryResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTimeSeriesQueryMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void analyticalQuery(com.udb.entity.v1.AnalyticalQueryRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.AnalyticalQueryResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAnalyticalQueryMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── Tx / CDC ───────────────────────────────────────────────────────────────
     * </pre>
     */
    public io.grpc.stub.StreamObserver<com.udb.entity.v1.Mutation> beginTx(
        io.grpc.stub.StreamObserver<com.udb.entity.v1.TxStatus> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getBeginTxMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public void publishCDC(com.udb.entity.v1.CDCSubscriptionRequest request,
        io.grpc.stub.StreamObserver<com.udb.events.v1.CDCEnvelope> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getPublishCDCMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createMaterializedView(com.udb.entity.v1.ViewDefinition request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateMaterializedViewMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── First-Class Event API ─────────────────────────────────────────────────
     * </pre>
     */
    public void enqueueOutboxEvent(com.udb.entity.v1.EnqueueOutboxEventRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.EnqueueOutboxEventResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEnqueueOutboxEventMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Generic resource administration.
     * Dispatch a lifecycle operation to any configured backend executor.
     * Requires scope: udb:dispatch
     * </pre>
     */
    public void genericDispatch(com.udb.entity.v1.GenericDispatchRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.GenericDispatchResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGenericDispatchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Ensure a named resource exists on the target backend.
     * Requires scope: udb:admin
     * </pre>
     */
    public void ensureResource(com.udb.entity.v1.ResourceAdminRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEnsureResourceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Drop a named resource on the target backend.
     * Requires scope: udb:admin
     * </pre>
     */
    public void dropResource(com.udb.entity.v1.ResourceAdminRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDropResourceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List all resources managed by the target backend.
     * Requires scope: udb:admin
     * </pre>
     */
    public void listResources(com.udb.entity.v1.ResourceAdminRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.ResourceListResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListResourcesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Catalog administration.
     * Stage a new catalog manifest version (validate + store as STAGED).
     * Requires scope: udb:admin
     * </pre>
     */
    public void stageCatalog(com.udb.entity.v1.StageCatalogRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogVersionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStageCatalogMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Activate a STAGED catalog version.
     * Requires scope: udb:admin
     * </pre>
     */
    public void activateCatalog(com.udb.entity.v1.CatalogVersionRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogVersionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getActivateCatalogMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Roll back to the previous ACTIVE catalog version.
     * Requires scope: udb:admin
     * </pre>
     */
    public void rollbackCatalog(com.udb.entity.v1.CatalogVersionRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogVersionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRollbackCatalogMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Validate a catalog manifest JSON without storing it.
     * Requires scope: udb:admin
     * </pre>
     */
    public void validateCatalog(com.udb.entity.v1.StageCatalogRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogValidationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getValidateCatalogMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Return the list of known catalog versions.
     * Requires scope: udb:admin
     * </pre>
     */
    public void getCatalogVersions(com.udb.entity.v1.CatalogManifestRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogVersionListResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetCatalogVersionsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Return one catalog version by catalog_id/version, or active version when empty.
     * Requires scope: udb:admin
     * </pre>
     */
    public void getCatalogVersion(com.udb.entity.v1.CatalogVersionRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogVersionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetCatalogVersionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Migration planning and apply.
     * Plan a migration against the active catalog without executing it.
     * Requires scope: udb:admin
     * </pre>
     */
    public void planMigration(com.udb.entity.v1.MigrationPlanRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MigrationPlanResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPlanMigrationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Apply a previously planned (and optionally approved) migration.
     * Requires scope: udb:admin
     * </pre>
     */
    public void applyMigration(com.udb.entity.v1.MigrationApplyRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MigrationStatusResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getApplyMigrationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Return the status of a migration run.
     * Requires scope: udb:admin
     * </pre>
     */
    public void getMigrationStatus(com.udb.entity.v1.MigrationRunRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MigrationStatusResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetMigrationStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Return migration runs for an admin console page.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * </pre>
     */
    public void listMigrationRuns(com.udb.entity.v1.MigrationRunListRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MigrationRunListResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListMigrationRunsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Approve a migration plan that requires review.
     * Requires scope: udb:admin
     * </pre>
     */
    public void approveMigrationPlan(com.udb.entity.v1.MigrationRunRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MigrationStatusResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getApproveMigrationPlanMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * DLQ management.
     * </pre>
     */
    public void listDlqEvents(com.udb.entity.v1.DlqListRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.DlqListResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListDlqEventsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getDlqEvent(com.udb.entity.v1.DlqEventRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.DlqEventResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetDlqEventMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void replayDlqEvent(com.udb.entity.v1.DlqActionRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReplayDlqEventMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void dismissDlqEvent(com.udb.entity.v1.DlqActionRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDismissDlqEventMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void quarantineDlqEvent(com.udb.entity.v1.DlqActionRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQuarantineDlqEventMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * CDC control plane.
     * </pre>
     */
    public void getCdcStatus(com.udb.entity.v1.CdcControlRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CdcStatusResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetCdcStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void pauseCdc(com.udb.entity.v1.CdcControlRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CdcStatusResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPauseCdcMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void resumeCdc(com.udb.entity.v1.CdcControlRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CdcStatusResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getResumeCdcMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void stepDownCdcLeader(com.udb.entity.v1.CdcControlRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CdcStatusResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStepDownCdcLeaderMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void previewCdcRedaction(com.udb.entity.v1.CdcRedactionPreviewRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CdcRedactionPreviewResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPreviewCdcRedactionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scanProjectionDrift(com.udb.entity.v1.ProjectionDriftScanRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.ProjectionDriftScanResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScanProjectionDriftMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Saga administration.
     * </pre>
     */
    public void listSagas(com.udb.entity.v1.SagaListRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.SagaListResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListSagasMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getSaga(com.udb.entity.v1.SagaRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.SagaResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetSagaMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void retrySagaCompensation(com.udb.entity.v1.SagaRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.SagaResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRetrySagaCompensationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void markSagaReviewed(com.udb.entity.v1.SagaRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.SagaResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMarkSagaReviewedMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Idempotently seed a baseline manual-review saga row and a retryable DLQ row
     * for the VERIFIED principal's tenant/project. Privilege-creating: fail-closed,
     * env-gated (UDB_ENABLE_ADMIN_SEED) and requires scope: udb:admin.
     * </pre>
     */
    public void ensureBaseline(com.udb.services.v1.EnsureBaselineRequest request,
        io.grpc.stub.StreamObserver<com.udb.services.v1.EnsureBaselineResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEnsureBaselineMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Policy administration.
     * </pre>
     */
    public void listPolicies(com.udb.entity.v1.PolicyListRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.PolicyListResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListPoliciesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void putPolicy(com.udb.entity.v1.PutPolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPutPolicyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deletePolicy(com.udb.entity.v1.PolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeletePolicyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void reloadPolicies(com.udb.entity.v1.CapabilitiesRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReloadPoliciesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void lintPolicies(com.udb.entity.v1.CapabilitiesRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.PolicyLintResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getLintPoliciesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── Admin API ─────────────────────────────────────────────────────────────
     * </pre>
     */
    public void getCapabilities(com.udb.entity.v1.CapabilitiesRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CapabilitiesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetCapabilitiesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getCatalogManifest(com.udb.entity.v1.CatalogManifestRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogManifestResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetCatalogManifestMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Runtime schema lookup for SDK/data operation compatibility negotiation.
     * </pre>
     */
    public void lookupMessageSchema(com.udb.entity.v1.MessageSchemaLookupRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MessageSchemaLookupResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getLookupMessageSchemaMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listMessageSchemas(com.udb.entity.v1.MessageSchemaListRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MessageSchemaListResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListMessageSchemasMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getHealthReport(com.udb.entity.v1.HealthReportRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.HealthReportResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetHealthReportMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Multi-project registry.
     * Ensure a project namespace exists (idempotent).
     * Requires scope: udb:admin
     * </pre>
     */
    public void ensureProject(com.udb.entity.v1.EnsureProjectRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEnsureProjectMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List all registered project namespaces.
     * Requires scope: udb:admin
     * </pre>
     */
    public void listProjects(com.udb.entity.v1.ProjectListRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.ProjectListResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListProjectsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── Unified Admin Surface ────────────────────────────────────────────────
     * Returns a single snapshot covering catalog, CDC, saga, backend, and policy
     * state for the admin console. Requires scope: udb:admin.
     * </pre>
     */
    public void getAdminSummary(com.udb.entity.v1.AdminSummaryRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.AdminSummaryResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetAdminSummaryMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Paginated admin audit log view for the admin console.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * </pre>
     */
    public void listAdminAuditLogs(com.udb.entity.v1.AdminAuditLogRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.AdminAuditLogResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListAdminAuditLogsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Verifies the admin audit log hash chain and reports the first broken link.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * </pre>
     */
    public void verifyAdminAuditLog(com.udb.entity.v1.AdminAuditVerifyRequest request,
        io.grpc.stub.StreamObserver<com.udb.entity.v1.AdminAuditVerifyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getVerifyAdminAuditLogMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service DataBroker.
   * <pre>
   * DataBroker is the UDB-owned wire protocol. Project protos are parsed only as
   * schema input; they do not need to contain or import this service contract.
   * </pre>
   */
  public static final class DataBrokerBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<DataBrokerBlockingV2Stub> {
    private DataBrokerBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DataBrokerBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DataBrokerBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * ── Relational ─────────────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.entity.v1.RecordSet select(com.udb.entity.v1.SelectRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSelectMethod(), getCallOptions(), request);
    }

    /**
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<com.udb.entity.v1.SelectRequest, com.udb.entity.v1.RecordSet>
        batchSelect() {
      return io.grpc.stub.ClientCalls.blockingBidiStreamingCall(
          getChannel(), getBatchSelectMethod(), getCallOptions());
    }

    /**
     * <pre>
     * Additive typed columnar read. Reuses SelectRequest; streams RecordBatchV2.
     * Clients use this only when ProtocolSupport.encodings advertises
     * "record_batch_v2" and otherwise fall back to Select/RecordSet.
     * </pre>
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<?, com.udb.entity.v1.RecordBatchV2>
        selectV2(com.udb.entity.v1.SelectRequest request) {
      return io.grpc.stub.ClientCalls.blockingV2ServerStreamingCall(
          getChannel(), getSelectV2Method(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse upsert(com.udb.entity.v1.UpsertRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getUpsertMethod(), getCallOptions(), request);
    }

    /**
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<com.udb.entity.v1.UpsertRequest, com.udb.entity.v1.MutationResponse>
        batchUpsert() {
      return io.grpc.stub.ClientCalls.blockingBidiStreamingCall(
          getChannel(), getBatchUpsertMethod(), getCallOptions());
    }

    /**
     * <pre>
     * Delete rows without raw SQL.
     * </pre>
     */
    public com.udb.entity.v1.MutationResponse delete(com.udb.entity.v1.DeleteRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeleteMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Partial update: SET named columns and/or apply atomic increments on the
     * matched rows — no full-record resend, no read-modify-write counter window.
     * Same filter language, tenant isolation and CAS (`expected`) as
     * Upsert/Delete. A retried keyed Update is deduped in the write tx
     * (fail-closed, tenant+project-scoped durable dedup) and returns
     * was_duplicate=true with the original body.
     * </pre>
     */
    public com.udb.entity.v1.MutationResponse update(com.udb.entity.v1.UpdateRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getUpdateMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Vector ──────────────────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.entity.v1.VectorSet vectorSearch(com.udb.entity.v1.VectorSearchRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getVectorSearchMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.VectorSet vectorHybridSearch(com.udb.entity.v1.VectorHybridSearchRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getVectorHybridSearchMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse vectorUpsert(com.udb.entity.v1.VectorUpsertRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getVectorUpsertMethod(), getCallOptions(), request);
    }

    /**
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<com.udb.entity.v1.VectorUpsertRequest, com.udb.entity.v1.MutationResponse>
        vectorBatchUpsert() {
      return io.grpc.stub.ClientCalls.blockingBidiStreamingCall(
          getChannel(), getVectorBatchUpsertMethod(), getCallOptions());
    }

    /**
     * <pre>
     * ── Blob ───────────────────────────────────────────────────────────────────
     * </pre>
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<com.udb.entity.v1.Chunk, com.udb.entity.v1.MutationResponse>
        putObject() {
      return io.grpc.stub.ClientCalls.blockingClientStreamingCall(
          getChannel(), getPutObjectMethod(), getCallOptions());
    }

    /**
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<?, com.udb.entity.v1.Chunk>
        getObject(com.udb.entity.v1.ObjectRequest request) {
      return io.grpc.stub.ClientCalls.blockingV2ServerStreamingCall(
          getChannel(), getGetObjectMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.UrlResponse generatePresignedUrl(com.udb.entity.v1.UrlRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGeneratePresignedUrlMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MultipartUploadResponse initiateMultipartUpload(com.udb.entity.v1.MultipartUploadRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getInitiateMultipartUploadMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Cache / KV ─────────────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.entity.v1.CacheGetResponse cacheGet(com.udb.entity.v1.CacheGetRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCacheGetMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse cacheSet(com.udb.entity.v1.CacheSetRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCacheSetMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse cacheDelete(com.udb.entity.v1.CacheDeleteRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCacheDeleteMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.CacheScanResponse cacheScan(com.udb.entity.v1.CacheScanRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCacheScanMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Document / Graph / Time-Series / Analytical Stores ────────────────────
     * </pre>
     */
    public com.udb.entity.v1.DocumentSet documentGet(com.udb.entity.v1.DocumentGetRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDocumentGetMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.DocumentSet documentFind(com.udb.entity.v1.DocumentFindRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDocumentFindMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse documentUpsert(com.udb.entity.v1.DocumentUpsertRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDocumentUpsertMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse documentDelete(com.udb.entity.v1.DocumentDeleteRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDocumentDeleteMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.GraphResultSet graphQuery(com.udb.entity.v1.GraphQueryRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGraphQueryMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse graphMutate(com.udb.entity.v1.GraphMutationRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGraphMutateMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse timeSeriesWrite(com.udb.entity.v1.TimeSeriesWriteRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getTimeSeriesWriteMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.TimeSeriesQueryResponse timeSeriesQuery(com.udb.entity.v1.TimeSeriesQueryRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getTimeSeriesQueryMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.AnalyticalQueryResponse analyticalQuery(com.udb.entity.v1.AnalyticalQueryRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getAnalyticalQueryMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Tx / CDC ───────────────────────────────────────────────────────────────
     * </pre>
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<com.udb.entity.v1.Mutation, com.udb.entity.v1.TxStatus>
        beginTx() {
      return io.grpc.stub.ClientCalls.blockingBidiStreamingCall(
          getChannel(), getBeginTxMethod(), getCallOptions());
    }

    /**
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<?, com.udb.events.v1.CDCEnvelope>
        publishCDC(com.udb.entity.v1.CDCSubscriptionRequest request) {
      return io.grpc.stub.ClientCalls.blockingV2ServerStreamingCall(
          getChannel(), getPublishCDCMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse createMaterializedView(com.udb.entity.v1.ViewDefinition request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreateMaterializedViewMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── First-Class Event API ─────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.entity.v1.EnqueueOutboxEventResponse enqueueOutboxEvent(com.udb.entity.v1.EnqueueOutboxEventRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getEnqueueOutboxEventMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Generic resource administration.
     * Dispatch a lifecycle operation to any configured backend executor.
     * Requires scope: udb:dispatch
     * </pre>
     */
    public com.udb.entity.v1.GenericDispatchResponse genericDispatch(com.udb.entity.v1.GenericDispatchRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGenericDispatchMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Ensure a named resource exists on the target backend.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.MutationResponse ensureResource(com.udb.entity.v1.ResourceAdminRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getEnsureResourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Drop a named resource on the target backend.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.MutationResponse dropResource(com.udb.entity.v1.ResourceAdminRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDropResourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List all resources managed by the target backend.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.ResourceListResponse listResources(com.udb.entity.v1.ResourceAdminRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListResourcesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Catalog administration.
     * Stage a new catalog manifest version (validate + store as STAGED).
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.CatalogVersionResponse stageCatalog(com.udb.entity.v1.StageCatalogRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getStageCatalogMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Activate a STAGED catalog version.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.CatalogVersionResponse activateCatalog(com.udb.entity.v1.CatalogVersionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getActivateCatalogMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Roll back to the previous ACTIVE catalog version.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.CatalogVersionResponse rollbackCatalog(com.udb.entity.v1.CatalogVersionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRollbackCatalogMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Validate a catalog manifest JSON without storing it.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.CatalogValidationResponse validateCatalog(com.udb.entity.v1.StageCatalogRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getValidateCatalogMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Return the list of known catalog versions.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.CatalogVersionListResponse getCatalogVersions(com.udb.entity.v1.CatalogManifestRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetCatalogVersionsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Return one catalog version by catalog_id/version, or active version when empty.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.CatalogVersionResponse getCatalogVersion(com.udb.entity.v1.CatalogVersionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetCatalogVersionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Migration planning and apply.
     * Plan a migration against the active catalog without executing it.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.MigrationPlanResponse planMigration(com.udb.entity.v1.MigrationPlanRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPlanMigrationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Apply a previously planned (and optionally approved) migration.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.MigrationStatusResponse applyMigration(com.udb.entity.v1.MigrationApplyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getApplyMigrationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Return the status of a migration run.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.MigrationStatusResponse getMigrationStatus(com.udb.entity.v1.MigrationRunRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetMigrationStatusMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Return migration runs for an admin console page.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * </pre>
     */
    public com.udb.entity.v1.MigrationRunListResponse listMigrationRuns(com.udb.entity.v1.MigrationRunListRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListMigrationRunsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Approve a migration plan that requires review.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.MigrationStatusResponse approveMigrationPlan(com.udb.entity.v1.MigrationRunRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getApproveMigrationPlanMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * DLQ management.
     * </pre>
     */
    public com.udb.entity.v1.DlqListResponse listDlqEvents(com.udb.entity.v1.DlqListRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListDlqEventsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.DlqEventResponse getDlqEvent(com.udb.entity.v1.DlqEventRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetDlqEventMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse replayDlqEvent(com.udb.entity.v1.DlqActionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getReplayDlqEventMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse dismissDlqEvent(com.udb.entity.v1.DlqActionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDismissDlqEventMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse quarantineDlqEvent(com.udb.entity.v1.DlqActionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getQuarantineDlqEventMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * CDC control plane.
     * </pre>
     */
    public com.udb.entity.v1.CdcStatusResponse getCdcStatus(com.udb.entity.v1.CdcControlRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetCdcStatusMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.CdcStatusResponse pauseCdc(com.udb.entity.v1.CdcControlRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPauseCdcMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.CdcStatusResponse resumeCdc(com.udb.entity.v1.CdcControlRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getResumeCdcMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.CdcStatusResponse stepDownCdcLeader(com.udb.entity.v1.CdcControlRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getStepDownCdcLeaderMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.CdcRedactionPreviewResponse previewCdcRedaction(com.udb.entity.v1.CdcRedactionPreviewRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPreviewCdcRedactionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.ProjectionDriftScanResponse scanProjectionDrift(com.udb.entity.v1.ProjectionDriftScanRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getScanProjectionDriftMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Saga administration.
     * </pre>
     */
    public com.udb.entity.v1.SagaListResponse listSagas(com.udb.entity.v1.SagaListRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListSagasMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.SagaResponse getSaga(com.udb.entity.v1.SagaRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetSagaMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.SagaResponse retrySagaCompensation(com.udb.entity.v1.SagaRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRetrySagaCompensationMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.SagaResponse markSagaReviewed(com.udb.entity.v1.SagaRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getMarkSagaReviewedMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Idempotently seed a baseline manual-review saga row and a retryable DLQ row
     * for the VERIFIED principal's tenant/project. Privilege-creating: fail-closed,
     * env-gated (UDB_ENABLE_ADMIN_SEED) and requires scope: udb:admin.
     * </pre>
     */
    public com.udb.services.v1.EnsureBaselineResponse ensureBaseline(com.udb.services.v1.EnsureBaselineRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getEnsureBaselineMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Policy administration.
     * </pre>
     */
    public com.udb.entity.v1.PolicyListResponse listPolicies(com.udb.entity.v1.PolicyListRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListPoliciesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse putPolicy(com.udb.entity.v1.PutPolicyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPutPolicyMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse deletePolicy(com.udb.entity.v1.PolicyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeletePolicyMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse reloadPolicies(com.udb.entity.v1.CapabilitiesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getReloadPoliciesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.PolicyLintResponse lintPolicies(com.udb.entity.v1.CapabilitiesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getLintPoliciesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Admin API ─────────────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.entity.v1.CapabilitiesResponse getCapabilities(com.udb.entity.v1.CapabilitiesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetCapabilitiesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.CatalogManifestResponse getCatalogManifest(com.udb.entity.v1.CatalogManifestRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetCatalogManifestMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Runtime schema lookup for SDK/data operation compatibility negotiation.
     * </pre>
     */
    public com.udb.entity.v1.MessageSchemaLookupResponse lookupMessageSchema(com.udb.entity.v1.MessageSchemaLookupRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getLookupMessageSchemaMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MessageSchemaListResponse listMessageSchemas(com.udb.entity.v1.MessageSchemaListRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListMessageSchemasMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.HealthReportResponse getHealthReport(com.udb.entity.v1.HealthReportRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetHealthReportMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Multi-project registry.
     * Ensure a project namespace exists (idempotent).
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.MutationResponse ensureProject(com.udb.entity.v1.EnsureProjectRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getEnsureProjectMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List all registered project namespaces.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.ProjectListResponse listProjects(com.udb.entity.v1.ProjectListRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListProjectsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Unified Admin Surface ────────────────────────────────────────────────
     * Returns a single snapshot covering catalog, CDC, saga, backend, and policy
     * state for the admin console. Requires scope: udb:admin.
     * </pre>
     */
    public com.udb.entity.v1.AdminSummaryResponse getAdminSummary(com.udb.entity.v1.AdminSummaryRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetAdminSummaryMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Paginated admin audit log view for the admin console.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * </pre>
     */
    public com.udb.entity.v1.AdminAuditLogResponse listAdminAuditLogs(com.udb.entity.v1.AdminAuditLogRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListAdminAuditLogsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Verifies the admin audit log hash chain and reports the first broken link.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * </pre>
     */
    public com.udb.entity.v1.AdminAuditVerifyResponse verifyAdminAuditLog(com.udb.entity.v1.AdminAuditVerifyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getVerifyAdminAuditLogMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service DataBroker.
   * <pre>
   * DataBroker is the UDB-owned wire protocol. Project protos are parsed only as
   * schema input; they do not need to contain or import this service contract.
   * </pre>
   */
  public static final class DataBrokerBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<DataBrokerBlockingStub> {
    private DataBrokerBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DataBrokerBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DataBrokerBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * ── Relational ─────────────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.entity.v1.RecordSet select(com.udb.entity.v1.SelectRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSelectMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Additive typed columnar read. Reuses SelectRequest; streams RecordBatchV2.
     * Clients use this only when ProtocolSupport.encodings advertises
     * "record_batch_v2" and otherwise fall back to Select/RecordSet.
     * </pre>
     */
    public java.util.Iterator<com.udb.entity.v1.RecordBatchV2> selectV2(
        com.udb.entity.v1.SelectRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getSelectV2Method(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse upsert(com.udb.entity.v1.UpsertRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpsertMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete rows without raw SQL.
     * </pre>
     */
    public com.udb.entity.v1.MutationResponse delete(com.udb.entity.v1.DeleteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Partial update: SET named columns and/or apply atomic increments on the
     * matched rows — no full-record resend, no read-modify-write counter window.
     * Same filter language, tenant isolation and CAS (`expected`) as
     * Upsert/Delete. A retried keyed Update is deduped in the write tx
     * (fail-closed, tenant+project-scoped durable dedup) and returns
     * was_duplicate=true with the original body.
     * </pre>
     */
    public com.udb.entity.v1.MutationResponse update(com.udb.entity.v1.UpdateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Vector ──────────────────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.entity.v1.VectorSet vectorSearch(com.udb.entity.v1.VectorSearchRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getVectorSearchMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.VectorSet vectorHybridSearch(com.udb.entity.v1.VectorHybridSearchRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getVectorHybridSearchMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse vectorUpsert(com.udb.entity.v1.VectorUpsertRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getVectorUpsertMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.udb.entity.v1.Chunk> getObject(
        com.udb.entity.v1.ObjectRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getGetObjectMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.UrlResponse generatePresignedUrl(com.udb.entity.v1.UrlRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGeneratePresignedUrlMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MultipartUploadResponse initiateMultipartUpload(com.udb.entity.v1.MultipartUploadRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getInitiateMultipartUploadMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Cache / KV ─────────────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.entity.v1.CacheGetResponse cacheGet(com.udb.entity.v1.CacheGetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCacheGetMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse cacheSet(com.udb.entity.v1.CacheSetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCacheSetMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse cacheDelete(com.udb.entity.v1.CacheDeleteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCacheDeleteMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.CacheScanResponse cacheScan(com.udb.entity.v1.CacheScanRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCacheScanMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Document / Graph / Time-Series / Analytical Stores ────────────────────
     * </pre>
     */
    public com.udb.entity.v1.DocumentSet documentGet(com.udb.entity.v1.DocumentGetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDocumentGetMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.DocumentSet documentFind(com.udb.entity.v1.DocumentFindRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDocumentFindMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse documentUpsert(com.udb.entity.v1.DocumentUpsertRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDocumentUpsertMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse documentDelete(com.udb.entity.v1.DocumentDeleteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDocumentDeleteMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.GraphResultSet graphQuery(com.udb.entity.v1.GraphQueryRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGraphQueryMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse graphMutate(com.udb.entity.v1.GraphMutationRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGraphMutateMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse timeSeriesWrite(com.udb.entity.v1.TimeSeriesWriteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTimeSeriesWriteMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.TimeSeriesQueryResponse timeSeriesQuery(com.udb.entity.v1.TimeSeriesQueryRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTimeSeriesQueryMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.AnalyticalQueryResponse analyticalQuery(com.udb.entity.v1.AnalyticalQueryRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAnalyticalQueryMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.udb.events.v1.CDCEnvelope> publishCDC(
        com.udb.entity.v1.CDCSubscriptionRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getPublishCDCMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse createMaterializedView(com.udb.entity.v1.ViewDefinition request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateMaterializedViewMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── First-Class Event API ─────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.entity.v1.EnqueueOutboxEventResponse enqueueOutboxEvent(com.udb.entity.v1.EnqueueOutboxEventRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEnqueueOutboxEventMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Generic resource administration.
     * Dispatch a lifecycle operation to any configured backend executor.
     * Requires scope: udb:dispatch
     * </pre>
     */
    public com.udb.entity.v1.GenericDispatchResponse genericDispatch(com.udb.entity.v1.GenericDispatchRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGenericDispatchMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Ensure a named resource exists on the target backend.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.MutationResponse ensureResource(com.udb.entity.v1.ResourceAdminRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEnsureResourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Drop a named resource on the target backend.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.MutationResponse dropResource(com.udb.entity.v1.ResourceAdminRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDropResourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List all resources managed by the target backend.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.ResourceListResponse listResources(com.udb.entity.v1.ResourceAdminRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListResourcesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Catalog administration.
     * Stage a new catalog manifest version (validate + store as STAGED).
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.CatalogVersionResponse stageCatalog(com.udb.entity.v1.StageCatalogRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStageCatalogMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Activate a STAGED catalog version.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.CatalogVersionResponse activateCatalog(com.udb.entity.v1.CatalogVersionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getActivateCatalogMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Roll back to the previous ACTIVE catalog version.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.CatalogVersionResponse rollbackCatalog(com.udb.entity.v1.CatalogVersionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRollbackCatalogMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Validate a catalog manifest JSON without storing it.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.CatalogValidationResponse validateCatalog(com.udb.entity.v1.StageCatalogRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getValidateCatalogMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Return the list of known catalog versions.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.CatalogVersionListResponse getCatalogVersions(com.udb.entity.v1.CatalogManifestRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetCatalogVersionsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Return one catalog version by catalog_id/version, or active version when empty.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.CatalogVersionResponse getCatalogVersion(com.udb.entity.v1.CatalogVersionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetCatalogVersionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Migration planning and apply.
     * Plan a migration against the active catalog without executing it.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.MigrationPlanResponse planMigration(com.udb.entity.v1.MigrationPlanRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPlanMigrationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Apply a previously planned (and optionally approved) migration.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.MigrationStatusResponse applyMigration(com.udb.entity.v1.MigrationApplyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getApplyMigrationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Return the status of a migration run.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.MigrationStatusResponse getMigrationStatus(com.udb.entity.v1.MigrationRunRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetMigrationStatusMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Return migration runs for an admin console page.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * </pre>
     */
    public com.udb.entity.v1.MigrationRunListResponse listMigrationRuns(com.udb.entity.v1.MigrationRunListRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListMigrationRunsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Approve a migration plan that requires review.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.MigrationStatusResponse approveMigrationPlan(com.udb.entity.v1.MigrationRunRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getApproveMigrationPlanMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * DLQ management.
     * </pre>
     */
    public com.udb.entity.v1.DlqListResponse listDlqEvents(com.udb.entity.v1.DlqListRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListDlqEventsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.DlqEventResponse getDlqEvent(com.udb.entity.v1.DlqEventRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetDlqEventMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse replayDlqEvent(com.udb.entity.v1.DlqActionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReplayDlqEventMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse dismissDlqEvent(com.udb.entity.v1.DlqActionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDismissDlqEventMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse quarantineDlqEvent(com.udb.entity.v1.DlqActionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQuarantineDlqEventMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * CDC control plane.
     * </pre>
     */
    public com.udb.entity.v1.CdcStatusResponse getCdcStatus(com.udb.entity.v1.CdcControlRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetCdcStatusMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.CdcStatusResponse pauseCdc(com.udb.entity.v1.CdcControlRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPauseCdcMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.CdcStatusResponse resumeCdc(com.udb.entity.v1.CdcControlRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getResumeCdcMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.CdcStatusResponse stepDownCdcLeader(com.udb.entity.v1.CdcControlRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStepDownCdcLeaderMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.CdcRedactionPreviewResponse previewCdcRedaction(com.udb.entity.v1.CdcRedactionPreviewRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPreviewCdcRedactionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.ProjectionDriftScanResponse scanProjectionDrift(com.udb.entity.v1.ProjectionDriftScanRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScanProjectionDriftMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Saga administration.
     * </pre>
     */
    public com.udb.entity.v1.SagaListResponse listSagas(com.udb.entity.v1.SagaListRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListSagasMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.SagaResponse getSaga(com.udb.entity.v1.SagaRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetSagaMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.SagaResponse retrySagaCompensation(com.udb.entity.v1.SagaRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRetrySagaCompensationMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.SagaResponse markSagaReviewed(com.udb.entity.v1.SagaRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMarkSagaReviewedMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Idempotently seed a baseline manual-review saga row and a retryable DLQ row
     * for the VERIFIED principal's tenant/project. Privilege-creating: fail-closed,
     * env-gated (UDB_ENABLE_ADMIN_SEED) and requires scope: udb:admin.
     * </pre>
     */
    public com.udb.services.v1.EnsureBaselineResponse ensureBaseline(com.udb.services.v1.EnsureBaselineRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEnsureBaselineMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Policy administration.
     * </pre>
     */
    public com.udb.entity.v1.PolicyListResponse listPolicies(com.udb.entity.v1.PolicyListRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListPoliciesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse putPolicy(com.udb.entity.v1.PutPolicyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPutPolicyMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse deletePolicy(com.udb.entity.v1.PolicyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeletePolicyMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MutationResponse reloadPolicies(com.udb.entity.v1.CapabilitiesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReloadPoliciesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.PolicyLintResponse lintPolicies(com.udb.entity.v1.CapabilitiesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getLintPoliciesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Admin API ─────────────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.entity.v1.CapabilitiesResponse getCapabilities(com.udb.entity.v1.CapabilitiesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetCapabilitiesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.CatalogManifestResponse getCatalogManifest(com.udb.entity.v1.CatalogManifestRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetCatalogManifestMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Runtime schema lookup for SDK/data operation compatibility negotiation.
     * </pre>
     */
    public com.udb.entity.v1.MessageSchemaLookupResponse lookupMessageSchema(com.udb.entity.v1.MessageSchemaLookupRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getLookupMessageSchemaMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.MessageSchemaListResponse listMessageSchemas(com.udb.entity.v1.MessageSchemaListRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListMessageSchemasMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.entity.v1.HealthReportResponse getHealthReport(com.udb.entity.v1.HealthReportRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetHealthReportMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Multi-project registry.
     * Ensure a project namespace exists (idempotent).
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.MutationResponse ensureProject(com.udb.entity.v1.EnsureProjectRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEnsureProjectMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List all registered project namespaces.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.udb.entity.v1.ProjectListResponse listProjects(com.udb.entity.v1.ProjectListRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListProjectsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Unified Admin Surface ────────────────────────────────────────────────
     * Returns a single snapshot covering catalog, CDC, saga, backend, and policy
     * state for the admin console. Requires scope: udb:admin.
     * </pre>
     */
    public com.udb.entity.v1.AdminSummaryResponse getAdminSummary(com.udb.entity.v1.AdminSummaryRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetAdminSummaryMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Paginated admin audit log view for the admin console.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * </pre>
     */
    public com.udb.entity.v1.AdminAuditLogResponse listAdminAuditLogs(com.udb.entity.v1.AdminAuditLogRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListAdminAuditLogsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Verifies the admin audit log hash chain and reports the first broken link.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * </pre>
     */
    public com.udb.entity.v1.AdminAuditVerifyResponse verifyAdminAuditLog(com.udb.entity.v1.AdminAuditVerifyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getVerifyAdminAuditLogMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service DataBroker.
   * <pre>
   * DataBroker is the UDB-owned wire protocol. Project protos are parsed only as
   * schema input; they do not need to contain or import this service contract.
   * </pre>
   */
  public static final class DataBrokerFutureStub
      extends io.grpc.stub.AbstractFutureStub<DataBrokerFutureStub> {
    private DataBrokerFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DataBrokerFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DataBrokerFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * ── Relational ─────────────────────────────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.RecordSet> select(
        com.udb.entity.v1.SelectRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSelectMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> upsert(
        com.udb.entity.v1.UpsertRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpsertMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Delete rows without raw SQL.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> delete(
        com.udb.entity.v1.DeleteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Partial update: SET named columns and/or apply atomic increments on the
     * matched rows — no full-record resend, no read-modify-write counter window.
     * Same filter language, tenant isolation and CAS (`expected`) as
     * Upsert/Delete. A retried keyed Update is deduped in the write tx
     * (fail-closed, tenant+project-scoped durable dedup) and returns
     * was_duplicate=true with the original body.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> update(
        com.udb.entity.v1.UpdateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── Vector ──────────────────────────────────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.VectorSet> vectorSearch(
        com.udb.entity.v1.VectorSearchRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getVectorSearchMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.VectorSet> vectorHybridSearch(
        com.udb.entity.v1.VectorHybridSearchRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getVectorHybridSearchMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> vectorUpsert(
        com.udb.entity.v1.VectorUpsertRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getVectorUpsertMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.UrlResponse> generatePresignedUrl(
        com.udb.entity.v1.UrlRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGeneratePresignedUrlMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MultipartUploadResponse> initiateMultipartUpload(
        com.udb.entity.v1.MultipartUploadRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getInitiateMultipartUploadMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── Cache / KV ─────────────────────────────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.CacheGetResponse> cacheGet(
        com.udb.entity.v1.CacheGetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCacheGetMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> cacheSet(
        com.udb.entity.v1.CacheSetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCacheSetMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> cacheDelete(
        com.udb.entity.v1.CacheDeleteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCacheDeleteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.CacheScanResponse> cacheScan(
        com.udb.entity.v1.CacheScanRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCacheScanMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── Document / Graph / Time-Series / Analytical Stores ────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.DocumentSet> documentGet(
        com.udb.entity.v1.DocumentGetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDocumentGetMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.DocumentSet> documentFind(
        com.udb.entity.v1.DocumentFindRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDocumentFindMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> documentUpsert(
        com.udb.entity.v1.DocumentUpsertRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDocumentUpsertMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> documentDelete(
        com.udb.entity.v1.DocumentDeleteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDocumentDeleteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.GraphResultSet> graphQuery(
        com.udb.entity.v1.GraphQueryRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGraphQueryMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> graphMutate(
        com.udb.entity.v1.GraphMutationRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGraphMutateMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> timeSeriesWrite(
        com.udb.entity.v1.TimeSeriesWriteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTimeSeriesWriteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.TimeSeriesQueryResponse> timeSeriesQuery(
        com.udb.entity.v1.TimeSeriesQueryRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTimeSeriesQueryMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.AnalyticalQueryResponse> analyticalQuery(
        com.udb.entity.v1.AnalyticalQueryRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAnalyticalQueryMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> createMaterializedView(
        com.udb.entity.v1.ViewDefinition request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateMaterializedViewMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── First-Class Event API ─────────────────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.EnqueueOutboxEventResponse> enqueueOutboxEvent(
        com.udb.entity.v1.EnqueueOutboxEventRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEnqueueOutboxEventMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Generic resource administration.
     * Dispatch a lifecycle operation to any configured backend executor.
     * Requires scope: udb:dispatch
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.GenericDispatchResponse> genericDispatch(
        com.udb.entity.v1.GenericDispatchRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGenericDispatchMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Ensure a named resource exists on the target backend.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> ensureResource(
        com.udb.entity.v1.ResourceAdminRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEnsureResourceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Drop a named resource on the target backend.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> dropResource(
        com.udb.entity.v1.ResourceAdminRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDropResourceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List all resources managed by the target backend.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.ResourceListResponse> listResources(
        com.udb.entity.v1.ResourceAdminRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListResourcesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Catalog administration.
     * Stage a new catalog manifest version (validate + store as STAGED).
     * Requires scope: udb:admin
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.CatalogVersionResponse> stageCatalog(
        com.udb.entity.v1.StageCatalogRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStageCatalogMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Activate a STAGED catalog version.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.CatalogVersionResponse> activateCatalog(
        com.udb.entity.v1.CatalogVersionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getActivateCatalogMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Roll back to the previous ACTIVE catalog version.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.CatalogVersionResponse> rollbackCatalog(
        com.udb.entity.v1.CatalogVersionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRollbackCatalogMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Validate a catalog manifest JSON without storing it.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.CatalogValidationResponse> validateCatalog(
        com.udb.entity.v1.StageCatalogRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getValidateCatalogMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Return the list of known catalog versions.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.CatalogVersionListResponse> getCatalogVersions(
        com.udb.entity.v1.CatalogManifestRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetCatalogVersionsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Return one catalog version by catalog_id/version, or active version when empty.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.CatalogVersionResponse> getCatalogVersion(
        com.udb.entity.v1.CatalogVersionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetCatalogVersionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Migration planning and apply.
     * Plan a migration against the active catalog without executing it.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MigrationPlanResponse> planMigration(
        com.udb.entity.v1.MigrationPlanRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPlanMigrationMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Apply a previously planned (and optionally approved) migration.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MigrationStatusResponse> applyMigration(
        com.udb.entity.v1.MigrationApplyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getApplyMigrationMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Return the status of a migration run.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MigrationStatusResponse> getMigrationStatus(
        com.udb.entity.v1.MigrationRunRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetMigrationStatusMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Return migration runs for an admin console page.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MigrationRunListResponse> listMigrationRuns(
        com.udb.entity.v1.MigrationRunListRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListMigrationRunsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Approve a migration plan that requires review.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MigrationStatusResponse> approveMigrationPlan(
        com.udb.entity.v1.MigrationRunRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getApproveMigrationPlanMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * DLQ management.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.DlqListResponse> listDlqEvents(
        com.udb.entity.v1.DlqListRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListDlqEventsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.DlqEventResponse> getDlqEvent(
        com.udb.entity.v1.DlqEventRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetDlqEventMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> replayDlqEvent(
        com.udb.entity.v1.DlqActionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReplayDlqEventMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> dismissDlqEvent(
        com.udb.entity.v1.DlqActionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDismissDlqEventMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> quarantineDlqEvent(
        com.udb.entity.v1.DlqActionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQuarantineDlqEventMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * CDC control plane.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.CdcStatusResponse> getCdcStatus(
        com.udb.entity.v1.CdcControlRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetCdcStatusMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.CdcStatusResponse> pauseCdc(
        com.udb.entity.v1.CdcControlRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPauseCdcMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.CdcStatusResponse> resumeCdc(
        com.udb.entity.v1.CdcControlRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getResumeCdcMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.CdcStatusResponse> stepDownCdcLeader(
        com.udb.entity.v1.CdcControlRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStepDownCdcLeaderMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.CdcRedactionPreviewResponse> previewCdcRedaction(
        com.udb.entity.v1.CdcRedactionPreviewRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPreviewCdcRedactionMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.ProjectionDriftScanResponse> scanProjectionDrift(
        com.udb.entity.v1.ProjectionDriftScanRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScanProjectionDriftMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Saga administration.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.SagaListResponse> listSagas(
        com.udb.entity.v1.SagaListRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListSagasMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.SagaResponse> getSaga(
        com.udb.entity.v1.SagaRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetSagaMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.SagaResponse> retrySagaCompensation(
        com.udb.entity.v1.SagaRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRetrySagaCompensationMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.SagaResponse> markSagaReviewed(
        com.udb.entity.v1.SagaRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMarkSagaReviewedMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Idempotently seed a baseline manual-review saga row and a retryable DLQ row
     * for the VERIFIED principal's tenant/project. Privilege-creating: fail-closed,
     * env-gated (UDB_ENABLE_ADMIN_SEED) and requires scope: udb:admin.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.services.v1.EnsureBaselineResponse> ensureBaseline(
        com.udb.services.v1.EnsureBaselineRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEnsureBaselineMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Policy administration.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.PolicyListResponse> listPolicies(
        com.udb.entity.v1.PolicyListRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListPoliciesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> putPolicy(
        com.udb.entity.v1.PutPolicyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPutPolicyMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> deletePolicy(
        com.udb.entity.v1.PolicyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeletePolicyMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> reloadPolicies(
        com.udb.entity.v1.CapabilitiesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReloadPoliciesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.PolicyLintResponse> lintPolicies(
        com.udb.entity.v1.CapabilitiesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getLintPoliciesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── Admin API ─────────────────────────────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.CapabilitiesResponse> getCapabilities(
        com.udb.entity.v1.CapabilitiesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetCapabilitiesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.CatalogManifestResponse> getCatalogManifest(
        com.udb.entity.v1.CatalogManifestRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetCatalogManifestMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Runtime schema lookup for SDK/data operation compatibility negotiation.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MessageSchemaLookupResponse> lookupMessageSchema(
        com.udb.entity.v1.MessageSchemaLookupRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getLookupMessageSchemaMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MessageSchemaListResponse> listMessageSchemas(
        com.udb.entity.v1.MessageSchemaListRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListMessageSchemasMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.HealthReportResponse> getHealthReport(
        com.udb.entity.v1.HealthReportRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetHealthReportMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Multi-project registry.
     * Ensure a project namespace exists (idempotent).
     * Requires scope: udb:admin
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.MutationResponse> ensureProject(
        com.udb.entity.v1.EnsureProjectRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEnsureProjectMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List all registered project namespaces.
     * Requires scope: udb:admin
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.ProjectListResponse> listProjects(
        com.udb.entity.v1.ProjectListRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListProjectsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── Unified Admin Surface ────────────────────────────────────────────────
     * Returns a single snapshot covering catalog, CDC, saga, backend, and policy
     * state for the admin console. Requires scope: udb:admin.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.AdminSummaryResponse> getAdminSummary(
        com.udb.entity.v1.AdminSummaryRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetAdminSummaryMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Paginated admin audit log view for the admin console.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.AdminAuditLogResponse> listAdminAuditLogs(
        com.udb.entity.v1.AdminAuditLogRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListAdminAuditLogsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Verifies the admin audit log hash chain and reports the first broken link.
     * Requires scope: udb:admin, udb:admin:viewer, or legacy udb:portal:viewer.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.entity.v1.AdminAuditVerifyResponse> verifyAdminAuditLog(
        com.udb.entity.v1.AdminAuditVerifyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getVerifyAdminAuditLogMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SELECT = 0;
  private static final int METHODID_SELECT_V2 = 1;
  private static final int METHODID_UPSERT = 2;
  private static final int METHODID_DELETE = 3;
  private static final int METHODID_UPDATE = 4;
  private static final int METHODID_VECTOR_SEARCH = 5;
  private static final int METHODID_VECTOR_HYBRID_SEARCH = 6;
  private static final int METHODID_VECTOR_UPSERT = 7;
  private static final int METHODID_GET_OBJECT = 8;
  private static final int METHODID_GENERATE_PRESIGNED_URL = 9;
  private static final int METHODID_INITIATE_MULTIPART_UPLOAD = 10;
  private static final int METHODID_CACHE_GET = 11;
  private static final int METHODID_CACHE_SET = 12;
  private static final int METHODID_CACHE_DELETE = 13;
  private static final int METHODID_CACHE_SCAN = 14;
  private static final int METHODID_DOCUMENT_GET = 15;
  private static final int METHODID_DOCUMENT_FIND = 16;
  private static final int METHODID_DOCUMENT_UPSERT = 17;
  private static final int METHODID_DOCUMENT_DELETE = 18;
  private static final int METHODID_GRAPH_QUERY = 19;
  private static final int METHODID_GRAPH_MUTATE = 20;
  private static final int METHODID_TIME_SERIES_WRITE = 21;
  private static final int METHODID_TIME_SERIES_QUERY = 22;
  private static final int METHODID_ANALYTICAL_QUERY = 23;
  private static final int METHODID_PUBLISH_CDC = 24;
  private static final int METHODID_CREATE_MATERIALIZED_VIEW = 25;
  private static final int METHODID_ENQUEUE_OUTBOX_EVENT = 26;
  private static final int METHODID_GENERIC_DISPATCH = 27;
  private static final int METHODID_ENSURE_RESOURCE = 28;
  private static final int METHODID_DROP_RESOURCE = 29;
  private static final int METHODID_LIST_RESOURCES = 30;
  private static final int METHODID_STAGE_CATALOG = 31;
  private static final int METHODID_ACTIVATE_CATALOG = 32;
  private static final int METHODID_ROLLBACK_CATALOG = 33;
  private static final int METHODID_VALIDATE_CATALOG = 34;
  private static final int METHODID_GET_CATALOG_VERSIONS = 35;
  private static final int METHODID_GET_CATALOG_VERSION = 36;
  private static final int METHODID_PLAN_MIGRATION = 37;
  private static final int METHODID_APPLY_MIGRATION = 38;
  private static final int METHODID_GET_MIGRATION_STATUS = 39;
  private static final int METHODID_LIST_MIGRATION_RUNS = 40;
  private static final int METHODID_APPROVE_MIGRATION_PLAN = 41;
  private static final int METHODID_LIST_DLQ_EVENTS = 42;
  private static final int METHODID_GET_DLQ_EVENT = 43;
  private static final int METHODID_REPLAY_DLQ_EVENT = 44;
  private static final int METHODID_DISMISS_DLQ_EVENT = 45;
  private static final int METHODID_QUARANTINE_DLQ_EVENT = 46;
  private static final int METHODID_GET_CDC_STATUS = 47;
  private static final int METHODID_PAUSE_CDC = 48;
  private static final int METHODID_RESUME_CDC = 49;
  private static final int METHODID_STEP_DOWN_CDC_LEADER = 50;
  private static final int METHODID_PREVIEW_CDC_REDACTION = 51;
  private static final int METHODID_SCAN_PROJECTION_DRIFT = 52;
  private static final int METHODID_LIST_SAGAS = 53;
  private static final int METHODID_GET_SAGA = 54;
  private static final int METHODID_RETRY_SAGA_COMPENSATION = 55;
  private static final int METHODID_MARK_SAGA_REVIEWED = 56;
  private static final int METHODID_ENSURE_BASELINE = 57;
  private static final int METHODID_LIST_POLICIES = 58;
  private static final int METHODID_PUT_POLICY = 59;
  private static final int METHODID_DELETE_POLICY = 60;
  private static final int METHODID_RELOAD_POLICIES = 61;
  private static final int METHODID_LINT_POLICIES = 62;
  private static final int METHODID_GET_CAPABILITIES = 63;
  private static final int METHODID_GET_CATALOG_MANIFEST = 64;
  private static final int METHODID_LOOKUP_MESSAGE_SCHEMA = 65;
  private static final int METHODID_LIST_MESSAGE_SCHEMAS = 66;
  private static final int METHODID_GET_HEALTH_REPORT = 67;
  private static final int METHODID_ENSURE_PROJECT = 68;
  private static final int METHODID_LIST_PROJECTS = 69;
  private static final int METHODID_GET_ADMIN_SUMMARY = 70;
  private static final int METHODID_LIST_ADMIN_AUDIT_LOGS = 71;
  private static final int METHODID_VERIFY_ADMIN_AUDIT_LOG = 72;
  private static final int METHODID_BATCH_SELECT = 73;
  private static final int METHODID_BATCH_UPSERT = 74;
  private static final int METHODID_VECTOR_BATCH_UPSERT = 75;
  private static final int METHODID_PUT_OBJECT = 76;
  private static final int METHODID_BEGIN_TX = 77;

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
        case METHODID_SELECT:
          serviceImpl.select((com.udb.entity.v1.SelectRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.RecordSet>) responseObserver);
          break;
        case METHODID_SELECT_V2:
          serviceImpl.selectV2((com.udb.entity.v1.SelectRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.RecordBatchV2>) responseObserver);
          break;
        case METHODID_UPSERT:
          serviceImpl.upsert((com.udb.entity.v1.UpsertRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_DELETE:
          serviceImpl.delete((com.udb.entity.v1.DeleteRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_UPDATE:
          serviceImpl.update((com.udb.entity.v1.UpdateRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_VECTOR_SEARCH:
          serviceImpl.vectorSearch((com.udb.entity.v1.VectorSearchRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.VectorSet>) responseObserver);
          break;
        case METHODID_VECTOR_HYBRID_SEARCH:
          serviceImpl.vectorHybridSearch((com.udb.entity.v1.VectorHybridSearchRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.VectorSet>) responseObserver);
          break;
        case METHODID_VECTOR_UPSERT:
          serviceImpl.vectorUpsert((com.udb.entity.v1.VectorUpsertRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_GET_OBJECT:
          serviceImpl.getObject((com.udb.entity.v1.ObjectRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.Chunk>) responseObserver);
          break;
        case METHODID_GENERATE_PRESIGNED_URL:
          serviceImpl.generatePresignedUrl((com.udb.entity.v1.UrlRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.UrlResponse>) responseObserver);
          break;
        case METHODID_INITIATE_MULTIPART_UPLOAD:
          serviceImpl.initiateMultipartUpload((com.udb.entity.v1.MultipartUploadRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MultipartUploadResponse>) responseObserver);
          break;
        case METHODID_CACHE_GET:
          serviceImpl.cacheGet((com.udb.entity.v1.CacheGetRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.CacheGetResponse>) responseObserver);
          break;
        case METHODID_CACHE_SET:
          serviceImpl.cacheSet((com.udb.entity.v1.CacheSetRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_CACHE_DELETE:
          serviceImpl.cacheDelete((com.udb.entity.v1.CacheDeleteRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_CACHE_SCAN:
          serviceImpl.cacheScan((com.udb.entity.v1.CacheScanRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.CacheScanResponse>) responseObserver);
          break;
        case METHODID_DOCUMENT_GET:
          serviceImpl.documentGet((com.udb.entity.v1.DocumentGetRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.DocumentSet>) responseObserver);
          break;
        case METHODID_DOCUMENT_FIND:
          serviceImpl.documentFind((com.udb.entity.v1.DocumentFindRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.DocumentSet>) responseObserver);
          break;
        case METHODID_DOCUMENT_UPSERT:
          serviceImpl.documentUpsert((com.udb.entity.v1.DocumentUpsertRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_DOCUMENT_DELETE:
          serviceImpl.documentDelete((com.udb.entity.v1.DocumentDeleteRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_GRAPH_QUERY:
          serviceImpl.graphQuery((com.udb.entity.v1.GraphQueryRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.GraphResultSet>) responseObserver);
          break;
        case METHODID_GRAPH_MUTATE:
          serviceImpl.graphMutate((com.udb.entity.v1.GraphMutationRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_TIME_SERIES_WRITE:
          serviceImpl.timeSeriesWrite((com.udb.entity.v1.TimeSeriesWriteRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_TIME_SERIES_QUERY:
          serviceImpl.timeSeriesQuery((com.udb.entity.v1.TimeSeriesQueryRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.TimeSeriesQueryResponse>) responseObserver);
          break;
        case METHODID_ANALYTICAL_QUERY:
          serviceImpl.analyticalQuery((com.udb.entity.v1.AnalyticalQueryRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.AnalyticalQueryResponse>) responseObserver);
          break;
        case METHODID_PUBLISH_CDC:
          serviceImpl.publishCDC((com.udb.entity.v1.CDCSubscriptionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.events.v1.CDCEnvelope>) responseObserver);
          break;
        case METHODID_CREATE_MATERIALIZED_VIEW:
          serviceImpl.createMaterializedView((com.udb.entity.v1.ViewDefinition) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_ENQUEUE_OUTBOX_EVENT:
          serviceImpl.enqueueOutboxEvent((com.udb.entity.v1.EnqueueOutboxEventRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.EnqueueOutboxEventResponse>) responseObserver);
          break;
        case METHODID_GENERIC_DISPATCH:
          serviceImpl.genericDispatch((com.udb.entity.v1.GenericDispatchRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.GenericDispatchResponse>) responseObserver);
          break;
        case METHODID_ENSURE_RESOURCE:
          serviceImpl.ensureResource((com.udb.entity.v1.ResourceAdminRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_DROP_RESOURCE:
          serviceImpl.dropResource((com.udb.entity.v1.ResourceAdminRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_LIST_RESOURCES:
          serviceImpl.listResources((com.udb.entity.v1.ResourceAdminRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.ResourceListResponse>) responseObserver);
          break;
        case METHODID_STAGE_CATALOG:
          serviceImpl.stageCatalog((com.udb.entity.v1.StageCatalogRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogVersionResponse>) responseObserver);
          break;
        case METHODID_ACTIVATE_CATALOG:
          serviceImpl.activateCatalog((com.udb.entity.v1.CatalogVersionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogVersionResponse>) responseObserver);
          break;
        case METHODID_ROLLBACK_CATALOG:
          serviceImpl.rollbackCatalog((com.udb.entity.v1.CatalogVersionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogVersionResponse>) responseObserver);
          break;
        case METHODID_VALIDATE_CATALOG:
          serviceImpl.validateCatalog((com.udb.entity.v1.StageCatalogRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogValidationResponse>) responseObserver);
          break;
        case METHODID_GET_CATALOG_VERSIONS:
          serviceImpl.getCatalogVersions((com.udb.entity.v1.CatalogManifestRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogVersionListResponse>) responseObserver);
          break;
        case METHODID_GET_CATALOG_VERSION:
          serviceImpl.getCatalogVersion((com.udb.entity.v1.CatalogVersionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogVersionResponse>) responseObserver);
          break;
        case METHODID_PLAN_MIGRATION:
          serviceImpl.planMigration((com.udb.entity.v1.MigrationPlanRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MigrationPlanResponse>) responseObserver);
          break;
        case METHODID_APPLY_MIGRATION:
          serviceImpl.applyMigration((com.udb.entity.v1.MigrationApplyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MigrationStatusResponse>) responseObserver);
          break;
        case METHODID_GET_MIGRATION_STATUS:
          serviceImpl.getMigrationStatus((com.udb.entity.v1.MigrationRunRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MigrationStatusResponse>) responseObserver);
          break;
        case METHODID_LIST_MIGRATION_RUNS:
          serviceImpl.listMigrationRuns((com.udb.entity.v1.MigrationRunListRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MigrationRunListResponse>) responseObserver);
          break;
        case METHODID_APPROVE_MIGRATION_PLAN:
          serviceImpl.approveMigrationPlan((com.udb.entity.v1.MigrationRunRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MigrationStatusResponse>) responseObserver);
          break;
        case METHODID_LIST_DLQ_EVENTS:
          serviceImpl.listDlqEvents((com.udb.entity.v1.DlqListRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.DlqListResponse>) responseObserver);
          break;
        case METHODID_GET_DLQ_EVENT:
          serviceImpl.getDlqEvent((com.udb.entity.v1.DlqEventRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.DlqEventResponse>) responseObserver);
          break;
        case METHODID_REPLAY_DLQ_EVENT:
          serviceImpl.replayDlqEvent((com.udb.entity.v1.DlqActionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_DISMISS_DLQ_EVENT:
          serviceImpl.dismissDlqEvent((com.udb.entity.v1.DlqActionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_QUARANTINE_DLQ_EVENT:
          serviceImpl.quarantineDlqEvent((com.udb.entity.v1.DlqActionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_GET_CDC_STATUS:
          serviceImpl.getCdcStatus((com.udb.entity.v1.CdcControlRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.CdcStatusResponse>) responseObserver);
          break;
        case METHODID_PAUSE_CDC:
          serviceImpl.pauseCdc((com.udb.entity.v1.CdcControlRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.CdcStatusResponse>) responseObserver);
          break;
        case METHODID_RESUME_CDC:
          serviceImpl.resumeCdc((com.udb.entity.v1.CdcControlRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.CdcStatusResponse>) responseObserver);
          break;
        case METHODID_STEP_DOWN_CDC_LEADER:
          serviceImpl.stepDownCdcLeader((com.udb.entity.v1.CdcControlRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.CdcStatusResponse>) responseObserver);
          break;
        case METHODID_PREVIEW_CDC_REDACTION:
          serviceImpl.previewCdcRedaction((com.udb.entity.v1.CdcRedactionPreviewRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.CdcRedactionPreviewResponse>) responseObserver);
          break;
        case METHODID_SCAN_PROJECTION_DRIFT:
          serviceImpl.scanProjectionDrift((com.udb.entity.v1.ProjectionDriftScanRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.ProjectionDriftScanResponse>) responseObserver);
          break;
        case METHODID_LIST_SAGAS:
          serviceImpl.listSagas((com.udb.entity.v1.SagaListRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.SagaListResponse>) responseObserver);
          break;
        case METHODID_GET_SAGA:
          serviceImpl.getSaga((com.udb.entity.v1.SagaRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.SagaResponse>) responseObserver);
          break;
        case METHODID_RETRY_SAGA_COMPENSATION:
          serviceImpl.retrySagaCompensation((com.udb.entity.v1.SagaRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.SagaResponse>) responseObserver);
          break;
        case METHODID_MARK_SAGA_REVIEWED:
          serviceImpl.markSagaReviewed((com.udb.entity.v1.SagaRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.SagaResponse>) responseObserver);
          break;
        case METHODID_ENSURE_BASELINE:
          serviceImpl.ensureBaseline((com.udb.services.v1.EnsureBaselineRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.services.v1.EnsureBaselineResponse>) responseObserver);
          break;
        case METHODID_LIST_POLICIES:
          serviceImpl.listPolicies((com.udb.entity.v1.PolicyListRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.PolicyListResponse>) responseObserver);
          break;
        case METHODID_PUT_POLICY:
          serviceImpl.putPolicy((com.udb.entity.v1.PutPolicyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_DELETE_POLICY:
          serviceImpl.deletePolicy((com.udb.entity.v1.PolicyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_RELOAD_POLICIES:
          serviceImpl.reloadPolicies((com.udb.entity.v1.CapabilitiesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_LINT_POLICIES:
          serviceImpl.lintPolicies((com.udb.entity.v1.CapabilitiesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.PolicyLintResponse>) responseObserver);
          break;
        case METHODID_GET_CAPABILITIES:
          serviceImpl.getCapabilities((com.udb.entity.v1.CapabilitiesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.CapabilitiesResponse>) responseObserver);
          break;
        case METHODID_GET_CATALOG_MANIFEST:
          serviceImpl.getCatalogManifest((com.udb.entity.v1.CatalogManifestRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.CatalogManifestResponse>) responseObserver);
          break;
        case METHODID_LOOKUP_MESSAGE_SCHEMA:
          serviceImpl.lookupMessageSchema((com.udb.entity.v1.MessageSchemaLookupRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MessageSchemaLookupResponse>) responseObserver);
          break;
        case METHODID_LIST_MESSAGE_SCHEMAS:
          serviceImpl.listMessageSchemas((com.udb.entity.v1.MessageSchemaListRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MessageSchemaListResponse>) responseObserver);
          break;
        case METHODID_GET_HEALTH_REPORT:
          serviceImpl.getHealthReport((com.udb.entity.v1.HealthReportRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.HealthReportResponse>) responseObserver);
          break;
        case METHODID_ENSURE_PROJECT:
          serviceImpl.ensureProject((com.udb.entity.v1.EnsureProjectRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
          break;
        case METHODID_LIST_PROJECTS:
          serviceImpl.listProjects((com.udb.entity.v1.ProjectListRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.ProjectListResponse>) responseObserver);
          break;
        case METHODID_GET_ADMIN_SUMMARY:
          serviceImpl.getAdminSummary((com.udb.entity.v1.AdminSummaryRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.AdminSummaryResponse>) responseObserver);
          break;
        case METHODID_LIST_ADMIN_AUDIT_LOGS:
          serviceImpl.listAdminAuditLogs((com.udb.entity.v1.AdminAuditLogRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.AdminAuditLogResponse>) responseObserver);
          break;
        case METHODID_VERIFY_ADMIN_AUDIT_LOG:
          serviceImpl.verifyAdminAuditLog((com.udb.entity.v1.AdminAuditVerifyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.AdminAuditVerifyResponse>) responseObserver);
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
        case METHODID_BATCH_SELECT:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.batchSelect(
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.RecordSet>) responseObserver);
        case METHODID_BATCH_UPSERT:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.batchUpsert(
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
        case METHODID_VECTOR_BATCH_UPSERT:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.vectorBatchUpsert(
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
        case METHODID_PUT_OBJECT:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.putObject(
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.MutationResponse>) responseObserver);
        case METHODID_BEGIN_TX:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.beginTx(
              (io.grpc.stub.StreamObserver<com.udb.entity.v1.TxStatus>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getSelectMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.SelectRequest,
              com.udb.entity.v1.RecordSet>(
                service, METHODID_SELECT)))
        .addMethod(
          getBatchSelectMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              com.udb.entity.v1.SelectRequest,
              com.udb.entity.v1.RecordSet>(
                service, METHODID_BATCH_SELECT)))
        .addMethod(
          getSelectV2Method(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              com.udb.entity.v1.SelectRequest,
              com.udb.entity.v1.RecordBatchV2>(
                service, METHODID_SELECT_V2)))
        .addMethod(
          getUpsertMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.UpsertRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_UPSERT)))
        .addMethod(
          getBatchUpsertMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              com.udb.entity.v1.UpsertRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_BATCH_UPSERT)))
        .addMethod(
          getDeleteMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.DeleteRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_DELETE)))
        .addMethod(
          getUpdateMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.UpdateRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_UPDATE)))
        .addMethod(
          getVectorSearchMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.VectorSearchRequest,
              com.udb.entity.v1.VectorSet>(
                service, METHODID_VECTOR_SEARCH)))
        .addMethod(
          getVectorHybridSearchMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.VectorHybridSearchRequest,
              com.udb.entity.v1.VectorSet>(
                service, METHODID_VECTOR_HYBRID_SEARCH)))
        .addMethod(
          getVectorUpsertMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.VectorUpsertRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_VECTOR_UPSERT)))
        .addMethod(
          getVectorBatchUpsertMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              com.udb.entity.v1.VectorUpsertRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_VECTOR_BATCH_UPSERT)))
        .addMethod(
          getPutObjectMethod(),
          io.grpc.stub.ServerCalls.asyncClientStreamingCall(
            new MethodHandlers<
              com.udb.entity.v1.Chunk,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_PUT_OBJECT)))
        .addMethod(
          getGetObjectMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              com.udb.entity.v1.ObjectRequest,
              com.udb.entity.v1.Chunk>(
                service, METHODID_GET_OBJECT)))
        .addMethod(
          getGeneratePresignedUrlMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.UrlRequest,
              com.udb.entity.v1.UrlResponse>(
                service, METHODID_GENERATE_PRESIGNED_URL)))
        .addMethod(
          getInitiateMultipartUploadMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.MultipartUploadRequest,
              com.udb.entity.v1.MultipartUploadResponse>(
                service, METHODID_INITIATE_MULTIPART_UPLOAD)))
        .addMethod(
          getCacheGetMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CacheGetRequest,
              com.udb.entity.v1.CacheGetResponse>(
                service, METHODID_CACHE_GET)))
        .addMethod(
          getCacheSetMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CacheSetRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_CACHE_SET)))
        .addMethod(
          getCacheDeleteMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CacheDeleteRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_CACHE_DELETE)))
        .addMethod(
          getCacheScanMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CacheScanRequest,
              com.udb.entity.v1.CacheScanResponse>(
                service, METHODID_CACHE_SCAN)))
        .addMethod(
          getDocumentGetMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.DocumentGetRequest,
              com.udb.entity.v1.DocumentSet>(
                service, METHODID_DOCUMENT_GET)))
        .addMethod(
          getDocumentFindMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.DocumentFindRequest,
              com.udb.entity.v1.DocumentSet>(
                service, METHODID_DOCUMENT_FIND)))
        .addMethod(
          getDocumentUpsertMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.DocumentUpsertRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_DOCUMENT_UPSERT)))
        .addMethod(
          getDocumentDeleteMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.DocumentDeleteRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_DOCUMENT_DELETE)))
        .addMethod(
          getGraphQueryMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.GraphQueryRequest,
              com.udb.entity.v1.GraphResultSet>(
                service, METHODID_GRAPH_QUERY)))
        .addMethod(
          getGraphMutateMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.GraphMutationRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_GRAPH_MUTATE)))
        .addMethod(
          getTimeSeriesWriteMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.TimeSeriesWriteRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_TIME_SERIES_WRITE)))
        .addMethod(
          getTimeSeriesQueryMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.TimeSeriesQueryRequest,
              com.udb.entity.v1.TimeSeriesQueryResponse>(
                service, METHODID_TIME_SERIES_QUERY)))
        .addMethod(
          getAnalyticalQueryMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.AnalyticalQueryRequest,
              com.udb.entity.v1.AnalyticalQueryResponse>(
                service, METHODID_ANALYTICAL_QUERY)))
        .addMethod(
          getBeginTxMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              com.udb.entity.v1.Mutation,
              com.udb.entity.v1.TxStatus>(
                service, METHODID_BEGIN_TX)))
        .addMethod(
          getPublishCDCMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              com.udb.entity.v1.CDCSubscriptionRequest,
              com.udb.events.v1.CDCEnvelope>(
                service, METHODID_PUBLISH_CDC)))
        .addMethod(
          getCreateMaterializedViewMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.ViewDefinition,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_CREATE_MATERIALIZED_VIEW)))
        .addMethod(
          getEnqueueOutboxEventMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.EnqueueOutboxEventRequest,
              com.udb.entity.v1.EnqueueOutboxEventResponse>(
                service, METHODID_ENQUEUE_OUTBOX_EVENT)))
        .addMethod(
          getGenericDispatchMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.GenericDispatchRequest,
              com.udb.entity.v1.GenericDispatchResponse>(
                service, METHODID_GENERIC_DISPATCH)))
        .addMethod(
          getEnsureResourceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.ResourceAdminRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_ENSURE_RESOURCE)))
        .addMethod(
          getDropResourceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.ResourceAdminRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_DROP_RESOURCE)))
        .addMethod(
          getListResourcesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.ResourceAdminRequest,
              com.udb.entity.v1.ResourceListResponse>(
                service, METHODID_LIST_RESOURCES)))
        .addMethod(
          getStageCatalogMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.StageCatalogRequest,
              com.udb.entity.v1.CatalogVersionResponse>(
                service, METHODID_STAGE_CATALOG)))
        .addMethod(
          getActivateCatalogMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CatalogVersionRequest,
              com.udb.entity.v1.CatalogVersionResponse>(
                service, METHODID_ACTIVATE_CATALOG)))
        .addMethod(
          getRollbackCatalogMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CatalogVersionRequest,
              com.udb.entity.v1.CatalogVersionResponse>(
                service, METHODID_ROLLBACK_CATALOG)))
        .addMethod(
          getValidateCatalogMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.StageCatalogRequest,
              com.udb.entity.v1.CatalogValidationResponse>(
                service, METHODID_VALIDATE_CATALOG)))
        .addMethod(
          getGetCatalogVersionsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CatalogManifestRequest,
              com.udb.entity.v1.CatalogVersionListResponse>(
                service, METHODID_GET_CATALOG_VERSIONS)))
        .addMethod(
          getGetCatalogVersionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CatalogVersionRequest,
              com.udb.entity.v1.CatalogVersionResponse>(
                service, METHODID_GET_CATALOG_VERSION)))
        .addMethod(
          getPlanMigrationMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.MigrationPlanRequest,
              com.udb.entity.v1.MigrationPlanResponse>(
                service, METHODID_PLAN_MIGRATION)))
        .addMethod(
          getApplyMigrationMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.MigrationApplyRequest,
              com.udb.entity.v1.MigrationStatusResponse>(
                service, METHODID_APPLY_MIGRATION)))
        .addMethod(
          getGetMigrationStatusMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.MigrationRunRequest,
              com.udb.entity.v1.MigrationStatusResponse>(
                service, METHODID_GET_MIGRATION_STATUS)))
        .addMethod(
          getListMigrationRunsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.MigrationRunListRequest,
              com.udb.entity.v1.MigrationRunListResponse>(
                service, METHODID_LIST_MIGRATION_RUNS)))
        .addMethod(
          getApproveMigrationPlanMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.MigrationRunRequest,
              com.udb.entity.v1.MigrationStatusResponse>(
                service, METHODID_APPROVE_MIGRATION_PLAN)))
        .addMethod(
          getListDlqEventsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.DlqListRequest,
              com.udb.entity.v1.DlqListResponse>(
                service, METHODID_LIST_DLQ_EVENTS)))
        .addMethod(
          getGetDlqEventMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.DlqEventRequest,
              com.udb.entity.v1.DlqEventResponse>(
                service, METHODID_GET_DLQ_EVENT)))
        .addMethod(
          getReplayDlqEventMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.DlqActionRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_REPLAY_DLQ_EVENT)))
        .addMethod(
          getDismissDlqEventMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.DlqActionRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_DISMISS_DLQ_EVENT)))
        .addMethod(
          getQuarantineDlqEventMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.DlqActionRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_QUARANTINE_DLQ_EVENT)))
        .addMethod(
          getGetCdcStatusMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CdcControlRequest,
              com.udb.entity.v1.CdcStatusResponse>(
                service, METHODID_GET_CDC_STATUS)))
        .addMethod(
          getPauseCdcMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CdcControlRequest,
              com.udb.entity.v1.CdcStatusResponse>(
                service, METHODID_PAUSE_CDC)))
        .addMethod(
          getResumeCdcMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CdcControlRequest,
              com.udb.entity.v1.CdcStatusResponse>(
                service, METHODID_RESUME_CDC)))
        .addMethod(
          getStepDownCdcLeaderMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CdcControlRequest,
              com.udb.entity.v1.CdcStatusResponse>(
                service, METHODID_STEP_DOWN_CDC_LEADER)))
        .addMethod(
          getPreviewCdcRedactionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CdcRedactionPreviewRequest,
              com.udb.entity.v1.CdcRedactionPreviewResponse>(
                service, METHODID_PREVIEW_CDC_REDACTION)))
        .addMethod(
          getScanProjectionDriftMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.ProjectionDriftScanRequest,
              com.udb.entity.v1.ProjectionDriftScanResponse>(
                service, METHODID_SCAN_PROJECTION_DRIFT)))
        .addMethod(
          getListSagasMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.SagaListRequest,
              com.udb.entity.v1.SagaListResponse>(
                service, METHODID_LIST_SAGAS)))
        .addMethod(
          getGetSagaMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.SagaRequest,
              com.udb.entity.v1.SagaResponse>(
                service, METHODID_GET_SAGA)))
        .addMethod(
          getRetrySagaCompensationMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.SagaRequest,
              com.udb.entity.v1.SagaResponse>(
                service, METHODID_RETRY_SAGA_COMPENSATION)))
        .addMethod(
          getMarkSagaReviewedMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.SagaRequest,
              com.udb.entity.v1.SagaResponse>(
                service, METHODID_MARK_SAGA_REVIEWED)))
        .addMethod(
          getEnsureBaselineMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.services.v1.EnsureBaselineRequest,
              com.udb.services.v1.EnsureBaselineResponse>(
                service, METHODID_ENSURE_BASELINE)))
        .addMethod(
          getListPoliciesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.PolicyListRequest,
              com.udb.entity.v1.PolicyListResponse>(
                service, METHODID_LIST_POLICIES)))
        .addMethod(
          getPutPolicyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.PutPolicyRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_PUT_POLICY)))
        .addMethod(
          getDeletePolicyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.PolicyRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_DELETE_POLICY)))
        .addMethod(
          getReloadPoliciesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CapabilitiesRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_RELOAD_POLICIES)))
        .addMethod(
          getLintPoliciesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CapabilitiesRequest,
              com.udb.entity.v1.PolicyLintResponse>(
                service, METHODID_LINT_POLICIES)))
        .addMethod(
          getGetCapabilitiesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CapabilitiesRequest,
              com.udb.entity.v1.CapabilitiesResponse>(
                service, METHODID_GET_CAPABILITIES)))
        .addMethod(
          getGetCatalogManifestMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.CatalogManifestRequest,
              com.udb.entity.v1.CatalogManifestResponse>(
                service, METHODID_GET_CATALOG_MANIFEST)))
        .addMethod(
          getLookupMessageSchemaMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.MessageSchemaLookupRequest,
              com.udb.entity.v1.MessageSchemaLookupResponse>(
                service, METHODID_LOOKUP_MESSAGE_SCHEMA)))
        .addMethod(
          getListMessageSchemasMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.MessageSchemaListRequest,
              com.udb.entity.v1.MessageSchemaListResponse>(
                service, METHODID_LIST_MESSAGE_SCHEMAS)))
        .addMethod(
          getGetHealthReportMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.HealthReportRequest,
              com.udb.entity.v1.HealthReportResponse>(
                service, METHODID_GET_HEALTH_REPORT)))
        .addMethod(
          getEnsureProjectMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.EnsureProjectRequest,
              com.udb.entity.v1.MutationResponse>(
                service, METHODID_ENSURE_PROJECT)))
        .addMethod(
          getListProjectsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.ProjectListRequest,
              com.udb.entity.v1.ProjectListResponse>(
                service, METHODID_LIST_PROJECTS)))
        .addMethod(
          getGetAdminSummaryMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.AdminSummaryRequest,
              com.udb.entity.v1.AdminSummaryResponse>(
                service, METHODID_GET_ADMIN_SUMMARY)))
        .addMethod(
          getListAdminAuditLogsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.AdminAuditLogRequest,
              com.udb.entity.v1.AdminAuditLogResponse>(
                service, METHODID_LIST_ADMIN_AUDIT_LOGS)))
        .addMethod(
          getVerifyAdminAuditLogMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.entity.v1.AdminAuditVerifyRequest,
              com.udb.entity.v1.AdminAuditVerifyResponse>(
                service, METHODID_VERIFY_ADMIN_AUDIT_LOG)))
        .build();
  }

  private static abstract class DataBrokerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DataBrokerBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.services.v1.DataBrokerProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DataBroker");
    }
  }

  private static final class DataBrokerFileDescriptorSupplier
      extends DataBrokerBaseDescriptorSupplier {
    DataBrokerFileDescriptorSupplier() {}
  }

  private static final class DataBrokerMethodDescriptorSupplier
      extends DataBrokerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    DataBrokerMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (DataBrokerGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DataBrokerFileDescriptorSupplier())
              .addMethod(getSelectMethod())
              .addMethod(getBatchSelectMethod())
              .addMethod(getSelectV2Method())
              .addMethod(getUpsertMethod())
              .addMethod(getBatchUpsertMethod())
              .addMethod(getDeleteMethod())
              .addMethod(getUpdateMethod())
              .addMethod(getVectorSearchMethod())
              .addMethod(getVectorHybridSearchMethod())
              .addMethod(getVectorUpsertMethod())
              .addMethod(getVectorBatchUpsertMethod())
              .addMethod(getPutObjectMethod())
              .addMethod(getGetObjectMethod())
              .addMethod(getGeneratePresignedUrlMethod())
              .addMethod(getInitiateMultipartUploadMethod())
              .addMethod(getCacheGetMethod())
              .addMethod(getCacheSetMethod())
              .addMethod(getCacheDeleteMethod())
              .addMethod(getCacheScanMethod())
              .addMethod(getDocumentGetMethod())
              .addMethod(getDocumentFindMethod())
              .addMethod(getDocumentUpsertMethod())
              .addMethod(getDocumentDeleteMethod())
              .addMethod(getGraphQueryMethod())
              .addMethod(getGraphMutateMethod())
              .addMethod(getTimeSeriesWriteMethod())
              .addMethod(getTimeSeriesQueryMethod())
              .addMethod(getAnalyticalQueryMethod())
              .addMethod(getBeginTxMethod())
              .addMethod(getPublishCDCMethod())
              .addMethod(getCreateMaterializedViewMethod())
              .addMethod(getEnqueueOutboxEventMethod())
              .addMethod(getGenericDispatchMethod())
              .addMethod(getEnsureResourceMethod())
              .addMethod(getDropResourceMethod())
              .addMethod(getListResourcesMethod())
              .addMethod(getStageCatalogMethod())
              .addMethod(getActivateCatalogMethod())
              .addMethod(getRollbackCatalogMethod())
              .addMethod(getValidateCatalogMethod())
              .addMethod(getGetCatalogVersionsMethod())
              .addMethod(getGetCatalogVersionMethod())
              .addMethod(getPlanMigrationMethod())
              .addMethod(getApplyMigrationMethod())
              .addMethod(getGetMigrationStatusMethod())
              .addMethod(getListMigrationRunsMethod())
              .addMethod(getApproveMigrationPlanMethod())
              .addMethod(getListDlqEventsMethod())
              .addMethod(getGetDlqEventMethod())
              .addMethod(getReplayDlqEventMethod())
              .addMethod(getDismissDlqEventMethod())
              .addMethod(getQuarantineDlqEventMethod())
              .addMethod(getGetCdcStatusMethod())
              .addMethod(getPauseCdcMethod())
              .addMethod(getResumeCdcMethod())
              .addMethod(getStepDownCdcLeaderMethod())
              .addMethod(getPreviewCdcRedactionMethod())
              .addMethod(getScanProjectionDriftMethod())
              .addMethod(getListSagasMethod())
              .addMethod(getGetSagaMethod())
              .addMethod(getRetrySagaCompensationMethod())
              .addMethod(getMarkSagaReviewedMethod())
              .addMethod(getEnsureBaselineMethod())
              .addMethod(getListPoliciesMethod())
              .addMethod(getPutPolicyMethod())
              .addMethod(getDeletePolicyMethod())
              .addMethod(getReloadPoliciesMethod())
              .addMethod(getLintPoliciesMethod())
              .addMethod(getGetCapabilitiesMethod())
              .addMethod(getGetCatalogManifestMethod())
              .addMethod(getLookupMessageSchemaMethod())
              .addMethod(getListMessageSchemasMethod())
              .addMethod(getGetHealthReportMethod())
              .addMethod(getEnsureProjectMethod())
              .addMethod(getListProjectsMethod())
              .addMethod(getGetAdminSummaryMethod())
              .addMethod(getListAdminAuditLogsMethod())
              .addMethod(getVerifyAdminAuditLogMethod())
              .build();
        }
      }
    }
    return result;
  }
}
