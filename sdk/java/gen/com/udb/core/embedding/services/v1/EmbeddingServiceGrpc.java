package com.udb.core.embedding.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * EmbeddingService (master-plan 9.11) — the AI data plane. Registers tenant-scoped
 * source entities to vector-index on change. INFERENCE RUNS IN SIDECARS ONLY: no
 * embedding model is ever linked into the broker. On a source row change (and on
 * Backfill) the broker emits a `udb.embedding.work.v1` event carrying ONLY the row
 * primary key + extracted text (NEVER credentials); a sidecar computes the vector
 * and returns it via the internal-only `ReportEmbedding` callback, which upserts
 * it through the shared asset vector-upsert seam tagged with the verified tenant.
 * `Retrieve` delegates to the SearchService (9.5) hybrid-search seam with a
 * server-side tenant filter — never a raw vector query.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class EmbeddingServiceGrpc {

  private EmbeddingServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.embedding.services.v1.EmbeddingService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.RegisterSourceRequest,
      com.udb.core.embedding.services.v1.RegisterSourceResponse> getRegisterSourceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RegisterSource",
      requestType = com.udb.core.embedding.services.v1.RegisterSourceRequest.class,
      responseType = com.udb.core.embedding.services.v1.RegisterSourceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.RegisterSourceRequest,
      com.udb.core.embedding.services.v1.RegisterSourceResponse> getRegisterSourceMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.RegisterSourceRequest, com.udb.core.embedding.services.v1.RegisterSourceResponse> getRegisterSourceMethod;
    if ((getRegisterSourceMethod = EmbeddingServiceGrpc.getRegisterSourceMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getRegisterSourceMethod = EmbeddingServiceGrpc.getRegisterSourceMethod) == null) {
          EmbeddingServiceGrpc.getRegisterSourceMethod = getRegisterSourceMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.RegisterSourceRequest, com.udb.core.embedding.services.v1.RegisterSourceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RegisterSource"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.RegisterSourceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.RegisterSourceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("RegisterSource"))
              .build();
        }
      }
    }
    return getRegisterSourceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ListSourcesRequest,
      com.udb.core.embedding.services.v1.ListSourcesResponse> getListSourcesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListSources",
      requestType = com.udb.core.embedding.services.v1.ListSourcesRequest.class,
      responseType = com.udb.core.embedding.services.v1.ListSourcesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ListSourcesRequest,
      com.udb.core.embedding.services.v1.ListSourcesResponse> getListSourcesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ListSourcesRequest, com.udb.core.embedding.services.v1.ListSourcesResponse> getListSourcesMethod;
    if ((getListSourcesMethod = EmbeddingServiceGrpc.getListSourcesMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getListSourcesMethod = EmbeddingServiceGrpc.getListSourcesMethod) == null) {
          EmbeddingServiceGrpc.getListSourcesMethod = getListSourcesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.ListSourcesRequest, com.udb.core.embedding.services.v1.ListSourcesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListSources"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ListSourcesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ListSourcesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("ListSources"))
              .build();
        }
      }
    }
    return getListSourcesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.DeleteSourceRequest,
      com.udb.core.embedding.services.v1.DeleteSourceResponse> getDeleteSourceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteSource",
      requestType = com.udb.core.embedding.services.v1.DeleteSourceRequest.class,
      responseType = com.udb.core.embedding.services.v1.DeleteSourceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.DeleteSourceRequest,
      com.udb.core.embedding.services.v1.DeleteSourceResponse> getDeleteSourceMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.DeleteSourceRequest, com.udb.core.embedding.services.v1.DeleteSourceResponse> getDeleteSourceMethod;
    if ((getDeleteSourceMethod = EmbeddingServiceGrpc.getDeleteSourceMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getDeleteSourceMethod = EmbeddingServiceGrpc.getDeleteSourceMethod) == null) {
          EmbeddingServiceGrpc.getDeleteSourceMethod = getDeleteSourceMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.DeleteSourceRequest, com.udb.core.embedding.services.v1.DeleteSourceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteSource"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.DeleteSourceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.DeleteSourceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("DeleteSource"))
              .build();
        }
      }
    }
    return getDeleteSourceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.BackfillRequest,
      com.udb.core.embedding.services.v1.BackfillResponse> getBackfillMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Backfill",
      requestType = com.udb.core.embedding.services.v1.BackfillRequest.class,
      responseType = com.udb.core.embedding.services.v1.BackfillResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.BackfillRequest,
      com.udb.core.embedding.services.v1.BackfillResponse> getBackfillMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.BackfillRequest, com.udb.core.embedding.services.v1.BackfillResponse> getBackfillMethod;
    if ((getBackfillMethod = EmbeddingServiceGrpc.getBackfillMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getBackfillMethod = EmbeddingServiceGrpc.getBackfillMethod) == null) {
          EmbeddingServiceGrpc.getBackfillMethod = getBackfillMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.BackfillRequest, com.udb.core.embedding.services.v1.BackfillResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Backfill"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.BackfillRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.BackfillResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("Backfill"))
              .build();
        }
      }
    }
    return getBackfillMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportEmbeddingRequest,
      com.udb.core.embedding.services.v1.ReportEmbeddingResponse> getReportEmbeddingMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReportEmbedding",
      requestType = com.udb.core.embedding.services.v1.ReportEmbeddingRequest.class,
      responseType = com.udb.core.embedding.services.v1.ReportEmbeddingResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportEmbeddingRequest,
      com.udb.core.embedding.services.v1.ReportEmbeddingResponse> getReportEmbeddingMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportEmbeddingRequest, com.udb.core.embedding.services.v1.ReportEmbeddingResponse> getReportEmbeddingMethod;
    if ((getReportEmbeddingMethod = EmbeddingServiceGrpc.getReportEmbeddingMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getReportEmbeddingMethod = EmbeddingServiceGrpc.getReportEmbeddingMethod) == null) {
          EmbeddingServiceGrpc.getReportEmbeddingMethod = getReportEmbeddingMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.ReportEmbeddingRequest, com.udb.core.embedding.services.v1.ReportEmbeddingResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReportEmbedding"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ReportEmbeddingRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ReportEmbeddingResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("ReportEmbedding"))
              .build();
        }
      }
    }
    return getReportEmbeddingMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.RetrieveRequest,
      com.udb.core.embedding.services.v1.RetrieveResponse> getRetrieveMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Retrieve",
      requestType = com.udb.core.embedding.services.v1.RetrieveRequest.class,
      responseType = com.udb.core.embedding.services.v1.RetrieveResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.RetrieveRequest,
      com.udb.core.embedding.services.v1.RetrieveResponse> getRetrieveMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.RetrieveRequest, com.udb.core.embedding.services.v1.RetrieveResponse> getRetrieveMethod;
    if ((getRetrieveMethod = EmbeddingServiceGrpc.getRetrieveMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getRetrieveMethod = EmbeddingServiceGrpc.getRetrieveMethod) == null) {
          EmbeddingServiceGrpc.getRetrieveMethod = getRetrieveMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.RetrieveRequest, com.udb.core.embedding.services.v1.RetrieveResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Retrieve"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.RetrieveRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.RetrieveResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("Retrieve"))
              .build();
        }
      }
    }
    return getRetrieveMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.RegisterModelRequest,
      com.udb.core.embedding.services.v1.RegisterModelResponse> getRegisterModelMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RegisterModel",
      requestType = com.udb.core.embedding.services.v1.RegisterModelRequest.class,
      responseType = com.udb.core.embedding.services.v1.RegisterModelResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.RegisterModelRequest,
      com.udb.core.embedding.services.v1.RegisterModelResponse> getRegisterModelMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.RegisterModelRequest, com.udb.core.embedding.services.v1.RegisterModelResponse> getRegisterModelMethod;
    if ((getRegisterModelMethod = EmbeddingServiceGrpc.getRegisterModelMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getRegisterModelMethod = EmbeddingServiceGrpc.getRegisterModelMethod) == null) {
          EmbeddingServiceGrpc.getRegisterModelMethod = getRegisterModelMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.RegisterModelRequest, com.udb.core.embedding.services.v1.RegisterModelResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RegisterModel"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.RegisterModelRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.RegisterModelResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("RegisterModel"))
              .build();
        }
      }
    }
    return getRegisterModelMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ListModelsRequest,
      com.udb.core.embedding.services.v1.ListModelsResponse> getListModelsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListModels",
      requestType = com.udb.core.embedding.services.v1.ListModelsRequest.class,
      responseType = com.udb.core.embedding.services.v1.ListModelsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ListModelsRequest,
      com.udb.core.embedding.services.v1.ListModelsResponse> getListModelsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ListModelsRequest, com.udb.core.embedding.services.v1.ListModelsResponse> getListModelsMethod;
    if ((getListModelsMethod = EmbeddingServiceGrpc.getListModelsMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getListModelsMethod = EmbeddingServiceGrpc.getListModelsMethod) == null) {
          EmbeddingServiceGrpc.getListModelsMethod = getListModelsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.ListModelsRequest, com.udb.core.embedding.services.v1.ListModelsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListModels"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ListModelsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ListModelsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("ListModels"))
              .build();
        }
      }
    }
    return getListModelsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.DeleteModelRequest,
      com.udb.core.embedding.services.v1.DeleteModelResponse> getDeleteModelMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteModel",
      requestType = com.udb.core.embedding.services.v1.DeleteModelRequest.class,
      responseType = com.udb.core.embedding.services.v1.DeleteModelResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.DeleteModelRequest,
      com.udb.core.embedding.services.v1.DeleteModelResponse> getDeleteModelMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.DeleteModelRequest, com.udb.core.embedding.services.v1.DeleteModelResponse> getDeleteModelMethod;
    if ((getDeleteModelMethod = EmbeddingServiceGrpc.getDeleteModelMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getDeleteModelMethod = EmbeddingServiceGrpc.getDeleteModelMethod) == null) {
          EmbeddingServiceGrpc.getDeleteModelMethod = getDeleteModelMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.DeleteModelRequest, com.udb.core.embedding.services.v1.DeleteModelResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteModel"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.DeleteModelRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.DeleteModelResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("DeleteModel"))
              .build();
        }
      }
    }
    return getDeleteModelMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.SetModelStatusRequest,
      com.udb.core.embedding.services.v1.SetModelStatusResponse> getSetModelStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetModelStatus",
      requestType = com.udb.core.embedding.services.v1.SetModelStatusRequest.class,
      responseType = com.udb.core.embedding.services.v1.SetModelStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.SetModelStatusRequest,
      com.udb.core.embedding.services.v1.SetModelStatusResponse> getSetModelStatusMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.SetModelStatusRequest, com.udb.core.embedding.services.v1.SetModelStatusResponse> getSetModelStatusMethod;
    if ((getSetModelStatusMethod = EmbeddingServiceGrpc.getSetModelStatusMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getSetModelStatusMethod = EmbeddingServiceGrpc.getSetModelStatusMethod) == null) {
          EmbeddingServiceGrpc.getSetModelStatusMethod = getSetModelStatusMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.SetModelStatusRequest, com.udb.core.embedding.services.v1.SetModelStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetModelStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.SetModelStatusRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.SetModelStatusResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("SetModelStatus"))
              .build();
        }
      }
    }
    return getSetModelStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.CutoverModelAliasRequest,
      com.udb.core.embedding.services.v1.CutoverModelAliasResponse> getCutoverModelAliasMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CutoverModelAlias",
      requestType = com.udb.core.embedding.services.v1.CutoverModelAliasRequest.class,
      responseType = com.udb.core.embedding.services.v1.CutoverModelAliasResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.CutoverModelAliasRequest,
      com.udb.core.embedding.services.v1.CutoverModelAliasResponse> getCutoverModelAliasMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.CutoverModelAliasRequest, com.udb.core.embedding.services.v1.CutoverModelAliasResponse> getCutoverModelAliasMethod;
    if ((getCutoverModelAliasMethod = EmbeddingServiceGrpc.getCutoverModelAliasMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getCutoverModelAliasMethod = EmbeddingServiceGrpc.getCutoverModelAliasMethod) == null) {
          EmbeddingServiceGrpc.getCutoverModelAliasMethod = getCutoverModelAliasMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.CutoverModelAliasRequest, com.udb.core.embedding.services.v1.CutoverModelAliasResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CutoverModelAlias"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.CutoverModelAliasRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.CutoverModelAliasResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("CutoverModelAlias"))
              .build();
        }
      }
    }
    return getCutoverModelAliasMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.GetEmbeddingJobStatusRequest,
      com.udb.core.embedding.services.v1.GetEmbeddingJobStatusResponse> getGetEmbeddingJobStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetEmbeddingJobStatus",
      requestType = com.udb.core.embedding.services.v1.GetEmbeddingJobStatusRequest.class,
      responseType = com.udb.core.embedding.services.v1.GetEmbeddingJobStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.GetEmbeddingJobStatusRequest,
      com.udb.core.embedding.services.v1.GetEmbeddingJobStatusResponse> getGetEmbeddingJobStatusMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.GetEmbeddingJobStatusRequest, com.udb.core.embedding.services.v1.GetEmbeddingJobStatusResponse> getGetEmbeddingJobStatusMethod;
    if ((getGetEmbeddingJobStatusMethod = EmbeddingServiceGrpc.getGetEmbeddingJobStatusMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getGetEmbeddingJobStatusMethod = EmbeddingServiceGrpc.getGetEmbeddingJobStatusMethod) == null) {
          EmbeddingServiceGrpc.getGetEmbeddingJobStatusMethod = getGetEmbeddingJobStatusMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.GetEmbeddingJobStatusRequest, com.udb.core.embedding.services.v1.GetEmbeddingJobStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetEmbeddingJobStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.GetEmbeddingJobStatusRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.GetEmbeddingJobStatusResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("GetEmbeddingJobStatus"))
              .build();
        }
      }
    }
    return getGetEmbeddingJobStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsRequest,
      com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsResponse> getListEmbeddingWorkItemsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListEmbeddingWorkItems",
      requestType = com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsRequest.class,
      responseType = com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsRequest,
      com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsResponse> getListEmbeddingWorkItemsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsRequest, com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsResponse> getListEmbeddingWorkItemsMethod;
    if ((getListEmbeddingWorkItemsMethod = EmbeddingServiceGrpc.getListEmbeddingWorkItemsMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getListEmbeddingWorkItemsMethod = EmbeddingServiceGrpc.getListEmbeddingWorkItemsMethod) == null) {
          EmbeddingServiceGrpc.getListEmbeddingWorkItemsMethod = getListEmbeddingWorkItemsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsRequest, com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListEmbeddingWorkItems"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("ListEmbeddingWorkItems"))
              .build();
        }
      }
    }
    return getListEmbeddingWorkItemsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportEmbeddingBatchRequest,
      com.udb.core.embedding.services.v1.ReportEmbeddingBatchResponse> getReportEmbeddingBatchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReportEmbeddingBatch",
      requestType = com.udb.core.embedding.services.v1.ReportEmbeddingBatchRequest.class,
      responseType = com.udb.core.embedding.services.v1.ReportEmbeddingBatchResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportEmbeddingBatchRequest,
      com.udb.core.embedding.services.v1.ReportEmbeddingBatchResponse> getReportEmbeddingBatchMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportEmbeddingBatchRequest, com.udb.core.embedding.services.v1.ReportEmbeddingBatchResponse> getReportEmbeddingBatchMethod;
    if ((getReportEmbeddingBatchMethod = EmbeddingServiceGrpc.getReportEmbeddingBatchMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getReportEmbeddingBatchMethod = EmbeddingServiceGrpc.getReportEmbeddingBatchMethod) == null) {
          EmbeddingServiceGrpc.getReportEmbeddingBatchMethod = getReportEmbeddingBatchMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.ReportEmbeddingBatchRequest, com.udb.core.embedding.services.v1.ReportEmbeddingBatchResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReportEmbeddingBatch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ReportEmbeddingBatchRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ReportEmbeddingBatchResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("ReportEmbeddingBatch"))
              .build();
        }
      }
    }
    return getReportEmbeddingBatchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportEmbeddingFailureRequest,
      com.udb.core.embedding.services.v1.ReportEmbeddingFailureResponse> getReportEmbeddingFailureMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReportEmbeddingFailure",
      requestType = com.udb.core.embedding.services.v1.ReportEmbeddingFailureRequest.class,
      responseType = com.udb.core.embedding.services.v1.ReportEmbeddingFailureResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportEmbeddingFailureRequest,
      com.udb.core.embedding.services.v1.ReportEmbeddingFailureResponse> getReportEmbeddingFailureMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportEmbeddingFailureRequest, com.udb.core.embedding.services.v1.ReportEmbeddingFailureResponse> getReportEmbeddingFailureMethod;
    if ((getReportEmbeddingFailureMethod = EmbeddingServiceGrpc.getReportEmbeddingFailureMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getReportEmbeddingFailureMethod = EmbeddingServiceGrpc.getReportEmbeddingFailureMethod) == null) {
          EmbeddingServiceGrpc.getReportEmbeddingFailureMethod = getReportEmbeddingFailureMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.ReportEmbeddingFailureRequest, com.udb.core.embedding.services.v1.ReportEmbeddingFailureResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReportEmbeddingFailure"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ReportEmbeddingFailureRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ReportEmbeddingFailureResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("ReportEmbeddingFailure"))
              .build();
        }
      }
    }
    return getReportEmbeddingFailureMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.IngestDocumentRequest,
      com.udb.core.embedding.services.v1.IngestDocumentResponse> getIngestDocumentMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "IngestDocument",
      requestType = com.udb.core.embedding.services.v1.IngestDocumentRequest.class,
      responseType = com.udb.core.embedding.services.v1.IngestDocumentResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.IngestDocumentRequest,
      com.udb.core.embedding.services.v1.IngestDocumentResponse> getIngestDocumentMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.IngestDocumentRequest, com.udb.core.embedding.services.v1.IngestDocumentResponse> getIngestDocumentMethod;
    if ((getIngestDocumentMethod = EmbeddingServiceGrpc.getIngestDocumentMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getIngestDocumentMethod = EmbeddingServiceGrpc.getIngestDocumentMethod) == null) {
          EmbeddingServiceGrpc.getIngestDocumentMethod = getIngestDocumentMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.IngestDocumentRequest, com.udb.core.embedding.services.v1.IngestDocumentResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "IngestDocument"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.IngestDocumentRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.IngestDocumentResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("IngestDocument"))
              .build();
        }
      }
    }
    return getIngestDocumentMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.IngestDocumentBatchRequest,
      com.udb.core.embedding.services.v1.IngestDocumentBatchResponse> getIngestDocumentBatchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "IngestDocumentBatch",
      requestType = com.udb.core.embedding.services.v1.IngestDocumentBatchRequest.class,
      responseType = com.udb.core.embedding.services.v1.IngestDocumentBatchResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.IngestDocumentBatchRequest,
      com.udb.core.embedding.services.v1.IngestDocumentBatchResponse> getIngestDocumentBatchMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.IngestDocumentBatchRequest, com.udb.core.embedding.services.v1.IngestDocumentBatchResponse> getIngestDocumentBatchMethod;
    if ((getIngestDocumentBatchMethod = EmbeddingServiceGrpc.getIngestDocumentBatchMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getIngestDocumentBatchMethod = EmbeddingServiceGrpc.getIngestDocumentBatchMethod) == null) {
          EmbeddingServiceGrpc.getIngestDocumentBatchMethod = getIngestDocumentBatchMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.IngestDocumentBatchRequest, com.udb.core.embedding.services.v1.IngestDocumentBatchResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "IngestDocumentBatch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.IngestDocumentBatchRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.IngestDocumentBatchResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("IngestDocumentBatch"))
              .build();
        }
      }
    }
    return getIngestDocumentBatchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportParsedDocumentRequest,
      com.udb.core.embedding.services.v1.ReportParsedDocumentResponse> getReportParsedDocumentMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReportParsedDocument",
      requestType = com.udb.core.embedding.services.v1.ReportParsedDocumentRequest.class,
      responseType = com.udb.core.embedding.services.v1.ReportParsedDocumentResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportParsedDocumentRequest,
      com.udb.core.embedding.services.v1.ReportParsedDocumentResponse> getReportParsedDocumentMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportParsedDocumentRequest, com.udb.core.embedding.services.v1.ReportParsedDocumentResponse> getReportParsedDocumentMethod;
    if ((getReportParsedDocumentMethod = EmbeddingServiceGrpc.getReportParsedDocumentMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getReportParsedDocumentMethod = EmbeddingServiceGrpc.getReportParsedDocumentMethod) == null) {
          EmbeddingServiceGrpc.getReportParsedDocumentMethod = getReportParsedDocumentMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.ReportParsedDocumentRequest, com.udb.core.embedding.services.v1.ReportParsedDocumentResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReportParsedDocument"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ReportParsedDocumentRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ReportParsedDocumentResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("ReportParsedDocument"))
              .build();
        }
      }
    }
    return getReportParsedDocumentMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportRetrievalEvaluationRequest,
      com.udb.core.embedding.services.v1.ReportRetrievalEvaluationResponse> getReportRetrievalEvaluationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReportRetrievalEvaluation",
      requestType = com.udb.core.embedding.services.v1.ReportRetrievalEvaluationRequest.class,
      responseType = com.udb.core.embedding.services.v1.ReportRetrievalEvaluationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportRetrievalEvaluationRequest,
      com.udb.core.embedding.services.v1.ReportRetrievalEvaluationResponse> getReportRetrievalEvaluationMethod() {
    io.grpc.MethodDescriptor<com.udb.core.embedding.services.v1.ReportRetrievalEvaluationRequest, com.udb.core.embedding.services.v1.ReportRetrievalEvaluationResponse> getReportRetrievalEvaluationMethod;
    if ((getReportRetrievalEvaluationMethod = EmbeddingServiceGrpc.getReportRetrievalEvaluationMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getReportRetrievalEvaluationMethod = EmbeddingServiceGrpc.getReportRetrievalEvaluationMethod) == null) {
          EmbeddingServiceGrpc.getReportRetrievalEvaluationMethod = getReportRetrievalEvaluationMethod =
              io.grpc.MethodDescriptor.<com.udb.core.embedding.services.v1.ReportRetrievalEvaluationRequest, com.udb.core.embedding.services.v1.ReportRetrievalEvaluationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReportRetrievalEvaluation"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ReportRetrievalEvaluationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.embedding.services.v1.ReportRetrievalEvaluationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("ReportRetrievalEvaluation"))
              .build();
        }
      }
    }
    return getReportRetrievalEvaluationMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static EmbeddingServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceStub>() {
        @java.lang.Override
        public EmbeddingServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EmbeddingServiceStub(channel, callOptions);
        }
      };
    return EmbeddingServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static EmbeddingServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceBlockingV2Stub>() {
        @java.lang.Override
        public EmbeddingServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EmbeddingServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return EmbeddingServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static EmbeddingServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceBlockingStub>() {
        @java.lang.Override
        public EmbeddingServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EmbeddingServiceBlockingStub(channel, callOptions);
        }
      };
    return EmbeddingServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static EmbeddingServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceFutureStub>() {
        @java.lang.Override
        public EmbeddingServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EmbeddingServiceFutureStub(channel, callOptions);
        }
      };
    return EmbeddingServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * EmbeddingService (master-plan 9.11) — the AI data plane. Registers tenant-scoped
   * source entities to vector-index on change. INFERENCE RUNS IN SIDECARS ONLY: no
   * embedding model is ever linked into the broker. On a source row change (and on
   * Backfill) the broker emits a `udb.embedding.work.v1` event carrying ONLY the row
   * primary key + extracted text (NEVER credentials); a sidecar computes the vector
   * and returns it via the internal-only `ReportEmbedding` callback, which upserts
   * it through the shared asset vector-upsert seam tagged with the verified tenant.
   * `Retrieve` delegates to the SearchService (9.5) hybrid-search seam with a
   * server-side tenant filter — never a raw vector query.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Register a tenant-scoped source to vector-index on change. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * </pre>
     */
    default void registerSource(com.udb.core.embedding.services.v1.RegisterSourceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.RegisterSourceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRegisterSourceMethod(), responseObserver);
    }

    /**
     * <pre>
     * List the calling tenant's registered sources.
     * </pre>
     */
    default void listSources(com.udb.core.embedding.services.v1.ListSourcesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ListSourcesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListSourcesMethod(), responseObserver);
    }

    /**
     * <pre>
     * Delete a tenant-scoped source registration (destructive: stops indexing on
     * change; the engine collection teardown runs on the follow-up worker).
     * </pre>
     */
    default void deleteSource(com.udb.core.embedding.services.v1.DeleteSourceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.DeleteSourceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteSourceMethod(), responseObserver);
    }

    /**
     * <pre>
     * Enqueue embedding work for the source's EXISTING rows. The per-row work
     * enumeration runs in the leader-spawned work emitter, which calls the same
     * `udb.embedding.work.v1` emit path the CDC change handler uses.
     * </pre>
     */
    default void backfill(com.udb.core.embedding.services.v1.BackfillRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.BackfillResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getBackfillMethod(), responseObserver);
    }

    /**
     * <pre>
     * SIDECAR CALLBACK (internal only). A sidecar that computed an embedding for a
     * source row returns the dense vector here; the broker upserts it through the
     * shared asset vector-upsert seam, tagged with the VERIFIED claim tenant (a
     * vector with no/foreign tenant is rejected — no fail-open). `internal_grpc_only`
     * restricts this to a loopback peer; it is never exposed in an SDK facade.
     * </pre>
     */
    default void reportEmbedding(com.udb.core.embedding.services.v1.ReportEmbeddingRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportEmbeddingResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReportEmbeddingMethod(), responseObserver);
    }

    /**
     * <pre>
     * Deadline-bounded semantic search over a source's vector collection. DELEGATES
     * to the SearchService (9.5) hybrid-search seam with a server-side tenant filter
     * injected from the verified claim. The broker never embeds the query (the
     * caller supplies an already-embedded `query_vector`); it never issues a raw
     * engine query. Returns `deadline_exceeded` if the gRPC deadline is past.
     * </pre>
     */
    default void retrieve(com.udb.core.embedding.services.v1.RetrieveRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.RetrieveResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRetrieveMethod(), responseObserver);
    }

    /**
     */
    default void registerModel(com.udb.core.embedding.services.v1.RegisterModelRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.RegisterModelResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRegisterModelMethod(), responseObserver);
    }

    /**
     */
    default void listModels(com.udb.core.embedding.services.v1.ListModelsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ListModelsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListModelsMethod(), responseObserver);
    }

    /**
     */
    default void deleteModel(com.udb.core.embedding.services.v1.DeleteModelRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.DeleteModelResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteModelMethod(), responseObserver);
    }

    /**
     */
    default void setModelStatus(com.udb.core.embedding.services.v1.SetModelStatusRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.SetModelStatusResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetModelStatusMethod(), responseObserver);
    }

    /**
     */
    default void cutoverModelAlias(com.udb.core.embedding.services.v1.CutoverModelAliasRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.CutoverModelAliasResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCutoverModelAliasMethod(), responseObserver);
    }

    /**
     */
    default void getEmbeddingJobStatus(com.udb.core.embedding.services.v1.GetEmbeddingJobStatusRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.GetEmbeddingJobStatusResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetEmbeddingJobStatusMethod(), responseObserver);
    }

    /**
     */
    default void listEmbeddingWorkItems(com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListEmbeddingWorkItemsMethod(), responseObserver);
    }

    /**
     */
    default void reportEmbeddingBatch(com.udb.core.embedding.services.v1.ReportEmbeddingBatchRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportEmbeddingBatchResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReportEmbeddingBatchMethod(), responseObserver);
    }

    /**
     */
    default void reportEmbeddingFailure(com.udb.core.embedding.services.v1.ReportEmbeddingFailureRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportEmbeddingFailureResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReportEmbeddingFailureMethod(), responseObserver);
    }

    /**
     */
    default void ingestDocument(com.udb.core.embedding.services.v1.IngestDocumentRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.IngestDocumentResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getIngestDocumentMethod(), responseObserver);
    }

    /**
     */
    default void ingestDocumentBatch(com.udb.core.embedding.services.v1.IngestDocumentBatchRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.IngestDocumentBatchResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getIngestDocumentBatchMethod(), responseObserver);
    }

    /**
     */
    default void reportParsedDocument(com.udb.core.embedding.services.v1.ReportParsedDocumentRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportParsedDocumentResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReportParsedDocumentMethod(), responseObserver);
    }

    /**
     */
    default void reportRetrievalEvaluation(com.udb.core.embedding.services.v1.ReportRetrievalEvaluationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportRetrievalEvaluationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReportRetrievalEvaluationMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service EmbeddingService.
   * <pre>
   * EmbeddingService (master-plan 9.11) — the AI data plane. Registers tenant-scoped
   * source entities to vector-index on change. INFERENCE RUNS IN SIDECARS ONLY: no
   * embedding model is ever linked into the broker. On a source row change (and on
   * Backfill) the broker emits a `udb.embedding.work.v1` event carrying ONLY the row
   * primary key + extracted text (NEVER credentials); a sidecar computes the vector
   * and returns it via the internal-only `ReportEmbedding` callback, which upserts
   * it through the shared asset vector-upsert seam tagged with the verified tenant.
   * `Retrieve` delegates to the SearchService (9.5) hybrid-search seam with a
   * server-side tenant filter — never a raw vector query.
   * </pre>
   */
  public static abstract class EmbeddingServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return EmbeddingServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service EmbeddingService.
   * <pre>
   * EmbeddingService (master-plan 9.11) — the AI data plane. Registers tenant-scoped
   * source entities to vector-index on change. INFERENCE RUNS IN SIDECARS ONLY: no
   * embedding model is ever linked into the broker. On a source row change (and on
   * Backfill) the broker emits a `udb.embedding.work.v1` event carrying ONLY the row
   * primary key + extracted text (NEVER credentials); a sidecar computes the vector
   * and returns it via the internal-only `ReportEmbedding` callback, which upserts
   * it through the shared asset vector-upsert seam tagged with the verified tenant.
   * `Retrieve` delegates to the SearchService (9.5) hybrid-search seam with a
   * server-side tenant filter — never a raw vector query.
   * </pre>
   */
  public static final class EmbeddingServiceStub
      extends io.grpc.stub.AbstractAsyncStub<EmbeddingServiceStub> {
    private EmbeddingServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EmbeddingServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EmbeddingServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a tenant-scoped source to vector-index on change. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * </pre>
     */
    public void registerSource(com.udb.core.embedding.services.v1.RegisterSourceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.RegisterSourceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRegisterSourceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List the calling tenant's registered sources.
     * </pre>
     */
    public void listSources(com.udb.core.embedding.services.v1.ListSourcesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ListSourcesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListSourcesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Delete a tenant-scoped source registration (destructive: stops indexing on
     * change; the engine collection teardown runs on the follow-up worker).
     * </pre>
     */
    public void deleteSource(com.udb.core.embedding.services.v1.DeleteSourceRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.DeleteSourceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteSourceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Enqueue embedding work for the source's EXISTING rows. The per-row work
     * enumeration runs in the leader-spawned work emitter, which calls the same
     * `udb.embedding.work.v1` emit path the CDC change handler uses.
     * </pre>
     */
    public void backfill(com.udb.core.embedding.services.v1.BackfillRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.BackfillResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getBackfillMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * SIDECAR CALLBACK (internal only). A sidecar that computed an embedding for a
     * source row returns the dense vector here; the broker upserts it through the
     * shared asset vector-upsert seam, tagged with the VERIFIED claim tenant (a
     * vector with no/foreign tenant is rejected — no fail-open). `internal_grpc_only`
     * restricts this to a loopback peer; it is never exposed in an SDK facade.
     * </pre>
     */
    public void reportEmbedding(com.udb.core.embedding.services.v1.ReportEmbeddingRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportEmbeddingResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReportEmbeddingMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Deadline-bounded semantic search over a source's vector collection. DELEGATES
     * to the SearchService (9.5) hybrid-search seam with a server-side tenant filter
     * injected from the verified claim. The broker never embeds the query (the
     * caller supplies an already-embedded `query_vector`); it never issues a raw
     * engine query. Returns `deadline_exceeded` if the gRPC deadline is past.
     * </pre>
     */
    public void retrieve(com.udb.core.embedding.services.v1.RetrieveRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.RetrieveResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRetrieveMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void registerModel(com.udb.core.embedding.services.v1.RegisterModelRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.RegisterModelResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRegisterModelMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listModels(com.udb.core.embedding.services.v1.ListModelsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ListModelsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListModelsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteModel(com.udb.core.embedding.services.v1.DeleteModelRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.DeleteModelResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteModelMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setModelStatus(com.udb.core.embedding.services.v1.SetModelStatusRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.SetModelStatusResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetModelStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void cutoverModelAlias(com.udb.core.embedding.services.v1.CutoverModelAliasRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.CutoverModelAliasResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCutoverModelAliasMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getEmbeddingJobStatus(com.udb.core.embedding.services.v1.GetEmbeddingJobStatusRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.GetEmbeddingJobStatusResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetEmbeddingJobStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listEmbeddingWorkItems(com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListEmbeddingWorkItemsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void reportEmbeddingBatch(com.udb.core.embedding.services.v1.ReportEmbeddingBatchRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportEmbeddingBatchResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReportEmbeddingBatchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void reportEmbeddingFailure(com.udb.core.embedding.services.v1.ReportEmbeddingFailureRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportEmbeddingFailureResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReportEmbeddingFailureMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void ingestDocument(com.udb.core.embedding.services.v1.IngestDocumentRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.IngestDocumentResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getIngestDocumentMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void ingestDocumentBatch(com.udb.core.embedding.services.v1.IngestDocumentBatchRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.IngestDocumentBatchResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getIngestDocumentBatchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void reportParsedDocument(com.udb.core.embedding.services.v1.ReportParsedDocumentRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportParsedDocumentResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReportParsedDocumentMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void reportRetrievalEvaluation(com.udb.core.embedding.services.v1.ReportRetrievalEvaluationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportRetrievalEvaluationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReportRetrievalEvaluationMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service EmbeddingService.
   * <pre>
   * EmbeddingService (master-plan 9.11) — the AI data plane. Registers tenant-scoped
   * source entities to vector-index on change. INFERENCE RUNS IN SIDECARS ONLY: no
   * embedding model is ever linked into the broker. On a source row change (and on
   * Backfill) the broker emits a `udb.embedding.work.v1` event carrying ONLY the row
   * primary key + extracted text (NEVER credentials); a sidecar computes the vector
   * and returns it via the internal-only `ReportEmbedding` callback, which upserts
   * it through the shared asset vector-upsert seam tagged with the verified tenant.
   * `Retrieve` delegates to the SearchService (9.5) hybrid-search seam with a
   * server-side tenant filter — never a raw vector query.
   * </pre>
   */
  public static final class EmbeddingServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<EmbeddingServiceBlockingV2Stub> {
    private EmbeddingServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EmbeddingServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EmbeddingServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a tenant-scoped source to vector-index on change. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.RegisterSourceResponse registerSource(com.udb.core.embedding.services.v1.RegisterSourceRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRegisterSourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List the calling tenant's registered sources.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.ListSourcesResponse listSources(com.udb.core.embedding.services.v1.ListSourcesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListSourcesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a tenant-scoped source registration (destructive: stops indexing on
     * change; the engine collection teardown runs on the follow-up worker).
     * </pre>
     */
    public com.udb.core.embedding.services.v1.DeleteSourceResponse deleteSource(com.udb.core.embedding.services.v1.DeleteSourceRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeleteSourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Enqueue embedding work for the source's EXISTING rows. The per-row work
     * enumeration runs in the leader-spawned work emitter, which calls the same
     * `udb.embedding.work.v1` emit path the CDC change handler uses.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.BackfillResponse backfill(com.udb.core.embedding.services.v1.BackfillRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getBackfillMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * SIDECAR CALLBACK (internal only). A sidecar that computed an embedding for a
     * source row returns the dense vector here; the broker upserts it through the
     * shared asset vector-upsert seam, tagged with the VERIFIED claim tenant (a
     * vector with no/foreign tenant is rejected — no fail-open). `internal_grpc_only`
     * restricts this to a loopback peer; it is never exposed in an SDK facade.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.ReportEmbeddingResponse reportEmbedding(com.udb.core.embedding.services.v1.ReportEmbeddingRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getReportEmbeddingMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Deadline-bounded semantic search over a source's vector collection. DELEGATES
     * to the SearchService (9.5) hybrid-search seam with a server-side tenant filter
     * injected from the verified claim. The broker never embeds the query (the
     * caller supplies an already-embedded `query_vector`); it never issues a raw
     * engine query. Returns `deadline_exceeded` if the gRPC deadline is past.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.RetrieveResponse retrieve(com.udb.core.embedding.services.v1.RetrieveRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRetrieveMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.RegisterModelResponse registerModel(com.udb.core.embedding.services.v1.RegisterModelRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRegisterModelMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.ListModelsResponse listModels(com.udb.core.embedding.services.v1.ListModelsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListModelsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.DeleteModelResponse deleteModel(com.udb.core.embedding.services.v1.DeleteModelRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeleteModelMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.SetModelStatusResponse setModelStatus(com.udb.core.embedding.services.v1.SetModelStatusRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSetModelStatusMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.CutoverModelAliasResponse cutoverModelAlias(com.udb.core.embedding.services.v1.CutoverModelAliasRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCutoverModelAliasMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.GetEmbeddingJobStatusResponse getEmbeddingJobStatus(com.udb.core.embedding.services.v1.GetEmbeddingJobStatusRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetEmbeddingJobStatusMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsResponse listEmbeddingWorkItems(com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListEmbeddingWorkItemsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.ReportEmbeddingBatchResponse reportEmbeddingBatch(com.udb.core.embedding.services.v1.ReportEmbeddingBatchRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getReportEmbeddingBatchMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.ReportEmbeddingFailureResponse reportEmbeddingFailure(com.udb.core.embedding.services.v1.ReportEmbeddingFailureRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getReportEmbeddingFailureMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.IngestDocumentResponse ingestDocument(com.udb.core.embedding.services.v1.IngestDocumentRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getIngestDocumentMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.IngestDocumentBatchResponse ingestDocumentBatch(com.udb.core.embedding.services.v1.IngestDocumentBatchRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getIngestDocumentBatchMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.ReportParsedDocumentResponse reportParsedDocument(com.udb.core.embedding.services.v1.ReportParsedDocumentRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getReportParsedDocumentMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.ReportRetrievalEvaluationResponse reportRetrievalEvaluation(com.udb.core.embedding.services.v1.ReportRetrievalEvaluationRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getReportRetrievalEvaluationMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service EmbeddingService.
   * <pre>
   * EmbeddingService (master-plan 9.11) — the AI data plane. Registers tenant-scoped
   * source entities to vector-index on change. INFERENCE RUNS IN SIDECARS ONLY: no
   * embedding model is ever linked into the broker. On a source row change (and on
   * Backfill) the broker emits a `udb.embedding.work.v1` event carrying ONLY the row
   * primary key + extracted text (NEVER credentials); a sidecar computes the vector
   * and returns it via the internal-only `ReportEmbedding` callback, which upserts
   * it through the shared asset vector-upsert seam tagged with the verified tenant.
   * `Retrieve` delegates to the SearchService (9.5) hybrid-search seam with a
   * server-side tenant filter — never a raw vector query.
   * </pre>
   */
  public static final class EmbeddingServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<EmbeddingServiceBlockingStub> {
    private EmbeddingServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EmbeddingServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EmbeddingServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a tenant-scoped source to vector-index on change. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.RegisterSourceResponse registerSource(com.udb.core.embedding.services.v1.RegisterSourceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRegisterSourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List the calling tenant's registered sources.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.ListSourcesResponse listSources(com.udb.core.embedding.services.v1.ListSourcesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListSourcesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a tenant-scoped source registration (destructive: stops indexing on
     * change; the engine collection teardown runs on the follow-up worker).
     * </pre>
     */
    public com.udb.core.embedding.services.v1.DeleteSourceResponse deleteSource(com.udb.core.embedding.services.v1.DeleteSourceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteSourceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Enqueue embedding work for the source's EXISTING rows. The per-row work
     * enumeration runs in the leader-spawned work emitter, which calls the same
     * `udb.embedding.work.v1` emit path the CDC change handler uses.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.BackfillResponse backfill(com.udb.core.embedding.services.v1.BackfillRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getBackfillMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * SIDECAR CALLBACK (internal only). A sidecar that computed an embedding for a
     * source row returns the dense vector here; the broker upserts it through the
     * shared asset vector-upsert seam, tagged with the VERIFIED claim tenant (a
     * vector with no/foreign tenant is rejected — no fail-open). `internal_grpc_only`
     * restricts this to a loopback peer; it is never exposed in an SDK facade.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.ReportEmbeddingResponse reportEmbedding(com.udb.core.embedding.services.v1.ReportEmbeddingRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReportEmbeddingMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Deadline-bounded semantic search over a source's vector collection. DELEGATES
     * to the SearchService (9.5) hybrid-search seam with a server-side tenant filter
     * injected from the verified claim. The broker never embeds the query (the
     * caller supplies an already-embedded `query_vector`); it never issues a raw
     * engine query. Returns `deadline_exceeded` if the gRPC deadline is past.
     * </pre>
     */
    public com.udb.core.embedding.services.v1.RetrieveResponse retrieve(com.udb.core.embedding.services.v1.RetrieveRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRetrieveMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.RegisterModelResponse registerModel(com.udb.core.embedding.services.v1.RegisterModelRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRegisterModelMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.ListModelsResponse listModels(com.udb.core.embedding.services.v1.ListModelsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListModelsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.DeleteModelResponse deleteModel(com.udb.core.embedding.services.v1.DeleteModelRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteModelMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.SetModelStatusResponse setModelStatus(com.udb.core.embedding.services.v1.SetModelStatusRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetModelStatusMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.CutoverModelAliasResponse cutoverModelAlias(com.udb.core.embedding.services.v1.CutoverModelAliasRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCutoverModelAliasMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.GetEmbeddingJobStatusResponse getEmbeddingJobStatus(com.udb.core.embedding.services.v1.GetEmbeddingJobStatusRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetEmbeddingJobStatusMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsResponse listEmbeddingWorkItems(com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListEmbeddingWorkItemsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.ReportEmbeddingBatchResponse reportEmbeddingBatch(com.udb.core.embedding.services.v1.ReportEmbeddingBatchRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReportEmbeddingBatchMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.ReportEmbeddingFailureResponse reportEmbeddingFailure(com.udb.core.embedding.services.v1.ReportEmbeddingFailureRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReportEmbeddingFailureMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.IngestDocumentResponse ingestDocument(com.udb.core.embedding.services.v1.IngestDocumentRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getIngestDocumentMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.IngestDocumentBatchResponse ingestDocumentBatch(com.udb.core.embedding.services.v1.IngestDocumentBatchRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getIngestDocumentBatchMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.ReportParsedDocumentResponse reportParsedDocument(com.udb.core.embedding.services.v1.ReportParsedDocumentRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReportParsedDocumentMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.embedding.services.v1.ReportRetrievalEvaluationResponse reportRetrievalEvaluation(com.udb.core.embedding.services.v1.ReportRetrievalEvaluationRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReportRetrievalEvaluationMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service EmbeddingService.
   * <pre>
   * EmbeddingService (master-plan 9.11) — the AI data plane. Registers tenant-scoped
   * source entities to vector-index on change. INFERENCE RUNS IN SIDECARS ONLY: no
   * embedding model is ever linked into the broker. On a source row change (and on
   * Backfill) the broker emits a `udb.embedding.work.v1` event carrying ONLY the row
   * primary key + extracted text (NEVER credentials); a sidecar computes the vector
   * and returns it via the internal-only `ReportEmbedding` callback, which upserts
   * it through the shared asset vector-upsert seam tagged with the verified tenant.
   * `Retrieve` delegates to the SearchService (9.5) hybrid-search seam with a
   * server-side tenant filter — never a raw vector query.
   * </pre>
   */
  public static final class EmbeddingServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<EmbeddingServiceFutureStub> {
    private EmbeddingServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EmbeddingServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EmbeddingServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Register a tenant-scoped source to vector-index on change. Fails closed
     * (failed_precondition) when the source table has no resolvable tenant column.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.RegisterSourceResponse> registerSource(
        com.udb.core.embedding.services.v1.RegisterSourceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRegisterSourceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List the calling tenant's registered sources.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.ListSourcesResponse> listSources(
        com.udb.core.embedding.services.v1.ListSourcesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListSourcesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Delete a tenant-scoped source registration (destructive: stops indexing on
     * change; the engine collection teardown runs on the follow-up worker).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.DeleteSourceResponse> deleteSource(
        com.udb.core.embedding.services.v1.DeleteSourceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteSourceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Enqueue embedding work for the source's EXISTING rows. The per-row work
     * enumeration runs in the leader-spawned work emitter, which calls the same
     * `udb.embedding.work.v1` emit path the CDC change handler uses.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.BackfillResponse> backfill(
        com.udb.core.embedding.services.v1.BackfillRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getBackfillMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * SIDECAR CALLBACK (internal only). A sidecar that computed an embedding for a
     * source row returns the dense vector here; the broker upserts it through the
     * shared asset vector-upsert seam, tagged with the VERIFIED claim tenant (a
     * vector with no/foreign tenant is rejected — no fail-open). `internal_grpc_only`
     * restricts this to a loopback peer; it is never exposed in an SDK facade.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.ReportEmbeddingResponse> reportEmbedding(
        com.udb.core.embedding.services.v1.ReportEmbeddingRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReportEmbeddingMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Deadline-bounded semantic search over a source's vector collection. DELEGATES
     * to the SearchService (9.5) hybrid-search seam with a server-side tenant filter
     * injected from the verified claim. The broker never embeds the query (the
     * caller supplies an already-embedded `query_vector`); it never issues a raw
     * engine query. Returns `deadline_exceeded` if the gRPC deadline is past.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.RetrieveResponse> retrieve(
        com.udb.core.embedding.services.v1.RetrieveRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRetrieveMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.RegisterModelResponse> registerModel(
        com.udb.core.embedding.services.v1.RegisterModelRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRegisterModelMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.ListModelsResponse> listModels(
        com.udb.core.embedding.services.v1.ListModelsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListModelsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.DeleteModelResponse> deleteModel(
        com.udb.core.embedding.services.v1.DeleteModelRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteModelMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.SetModelStatusResponse> setModelStatus(
        com.udb.core.embedding.services.v1.SetModelStatusRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetModelStatusMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.CutoverModelAliasResponse> cutoverModelAlias(
        com.udb.core.embedding.services.v1.CutoverModelAliasRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCutoverModelAliasMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.GetEmbeddingJobStatusResponse> getEmbeddingJobStatus(
        com.udb.core.embedding.services.v1.GetEmbeddingJobStatusRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetEmbeddingJobStatusMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsResponse> listEmbeddingWorkItems(
        com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListEmbeddingWorkItemsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.ReportEmbeddingBatchResponse> reportEmbeddingBatch(
        com.udb.core.embedding.services.v1.ReportEmbeddingBatchRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReportEmbeddingBatchMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.ReportEmbeddingFailureResponse> reportEmbeddingFailure(
        com.udb.core.embedding.services.v1.ReportEmbeddingFailureRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReportEmbeddingFailureMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.IngestDocumentResponse> ingestDocument(
        com.udb.core.embedding.services.v1.IngestDocumentRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getIngestDocumentMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.IngestDocumentBatchResponse> ingestDocumentBatch(
        com.udb.core.embedding.services.v1.IngestDocumentBatchRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getIngestDocumentBatchMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.ReportParsedDocumentResponse> reportParsedDocument(
        com.udb.core.embedding.services.v1.ReportParsedDocumentRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReportParsedDocumentMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.embedding.services.v1.ReportRetrievalEvaluationResponse> reportRetrievalEvaluation(
        com.udb.core.embedding.services.v1.ReportRetrievalEvaluationRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReportRetrievalEvaluationMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_REGISTER_SOURCE = 0;
  private static final int METHODID_LIST_SOURCES = 1;
  private static final int METHODID_DELETE_SOURCE = 2;
  private static final int METHODID_BACKFILL = 3;
  private static final int METHODID_REPORT_EMBEDDING = 4;
  private static final int METHODID_RETRIEVE = 5;
  private static final int METHODID_REGISTER_MODEL = 6;
  private static final int METHODID_LIST_MODELS = 7;
  private static final int METHODID_DELETE_MODEL = 8;
  private static final int METHODID_SET_MODEL_STATUS = 9;
  private static final int METHODID_CUTOVER_MODEL_ALIAS = 10;
  private static final int METHODID_GET_EMBEDDING_JOB_STATUS = 11;
  private static final int METHODID_LIST_EMBEDDING_WORK_ITEMS = 12;
  private static final int METHODID_REPORT_EMBEDDING_BATCH = 13;
  private static final int METHODID_REPORT_EMBEDDING_FAILURE = 14;
  private static final int METHODID_INGEST_DOCUMENT = 15;
  private static final int METHODID_INGEST_DOCUMENT_BATCH = 16;
  private static final int METHODID_REPORT_PARSED_DOCUMENT = 17;
  private static final int METHODID_REPORT_RETRIEVAL_EVALUATION = 18;

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
        case METHODID_REGISTER_SOURCE:
          serviceImpl.registerSource((com.udb.core.embedding.services.v1.RegisterSourceRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.RegisterSourceResponse>) responseObserver);
          break;
        case METHODID_LIST_SOURCES:
          serviceImpl.listSources((com.udb.core.embedding.services.v1.ListSourcesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ListSourcesResponse>) responseObserver);
          break;
        case METHODID_DELETE_SOURCE:
          serviceImpl.deleteSource((com.udb.core.embedding.services.v1.DeleteSourceRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.DeleteSourceResponse>) responseObserver);
          break;
        case METHODID_BACKFILL:
          serviceImpl.backfill((com.udb.core.embedding.services.v1.BackfillRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.BackfillResponse>) responseObserver);
          break;
        case METHODID_REPORT_EMBEDDING:
          serviceImpl.reportEmbedding((com.udb.core.embedding.services.v1.ReportEmbeddingRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportEmbeddingResponse>) responseObserver);
          break;
        case METHODID_RETRIEVE:
          serviceImpl.retrieve((com.udb.core.embedding.services.v1.RetrieveRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.RetrieveResponse>) responseObserver);
          break;
        case METHODID_REGISTER_MODEL:
          serviceImpl.registerModel((com.udb.core.embedding.services.v1.RegisterModelRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.RegisterModelResponse>) responseObserver);
          break;
        case METHODID_LIST_MODELS:
          serviceImpl.listModels((com.udb.core.embedding.services.v1.ListModelsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ListModelsResponse>) responseObserver);
          break;
        case METHODID_DELETE_MODEL:
          serviceImpl.deleteModel((com.udb.core.embedding.services.v1.DeleteModelRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.DeleteModelResponse>) responseObserver);
          break;
        case METHODID_SET_MODEL_STATUS:
          serviceImpl.setModelStatus((com.udb.core.embedding.services.v1.SetModelStatusRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.SetModelStatusResponse>) responseObserver);
          break;
        case METHODID_CUTOVER_MODEL_ALIAS:
          serviceImpl.cutoverModelAlias((com.udb.core.embedding.services.v1.CutoverModelAliasRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.CutoverModelAliasResponse>) responseObserver);
          break;
        case METHODID_GET_EMBEDDING_JOB_STATUS:
          serviceImpl.getEmbeddingJobStatus((com.udb.core.embedding.services.v1.GetEmbeddingJobStatusRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.GetEmbeddingJobStatusResponse>) responseObserver);
          break;
        case METHODID_LIST_EMBEDDING_WORK_ITEMS:
          serviceImpl.listEmbeddingWorkItems((com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsResponse>) responseObserver);
          break;
        case METHODID_REPORT_EMBEDDING_BATCH:
          serviceImpl.reportEmbeddingBatch((com.udb.core.embedding.services.v1.ReportEmbeddingBatchRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportEmbeddingBatchResponse>) responseObserver);
          break;
        case METHODID_REPORT_EMBEDDING_FAILURE:
          serviceImpl.reportEmbeddingFailure((com.udb.core.embedding.services.v1.ReportEmbeddingFailureRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportEmbeddingFailureResponse>) responseObserver);
          break;
        case METHODID_INGEST_DOCUMENT:
          serviceImpl.ingestDocument((com.udb.core.embedding.services.v1.IngestDocumentRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.IngestDocumentResponse>) responseObserver);
          break;
        case METHODID_INGEST_DOCUMENT_BATCH:
          serviceImpl.ingestDocumentBatch((com.udb.core.embedding.services.v1.IngestDocumentBatchRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.IngestDocumentBatchResponse>) responseObserver);
          break;
        case METHODID_REPORT_PARSED_DOCUMENT:
          serviceImpl.reportParsedDocument((com.udb.core.embedding.services.v1.ReportParsedDocumentRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportParsedDocumentResponse>) responseObserver);
          break;
        case METHODID_REPORT_RETRIEVAL_EVALUATION:
          serviceImpl.reportRetrievalEvaluation((com.udb.core.embedding.services.v1.ReportRetrievalEvaluationRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.embedding.services.v1.ReportRetrievalEvaluationResponse>) responseObserver);
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
          getRegisterSourceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.RegisterSourceRequest,
              com.udb.core.embedding.services.v1.RegisterSourceResponse>(
                service, METHODID_REGISTER_SOURCE)))
        .addMethod(
          getListSourcesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.ListSourcesRequest,
              com.udb.core.embedding.services.v1.ListSourcesResponse>(
                service, METHODID_LIST_SOURCES)))
        .addMethod(
          getDeleteSourceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.DeleteSourceRequest,
              com.udb.core.embedding.services.v1.DeleteSourceResponse>(
                service, METHODID_DELETE_SOURCE)))
        .addMethod(
          getBackfillMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.BackfillRequest,
              com.udb.core.embedding.services.v1.BackfillResponse>(
                service, METHODID_BACKFILL)))
        .addMethod(
          getReportEmbeddingMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.ReportEmbeddingRequest,
              com.udb.core.embedding.services.v1.ReportEmbeddingResponse>(
                service, METHODID_REPORT_EMBEDDING)))
        .addMethod(
          getRetrieveMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.RetrieveRequest,
              com.udb.core.embedding.services.v1.RetrieveResponse>(
                service, METHODID_RETRIEVE)))
        .addMethod(
          getRegisterModelMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.RegisterModelRequest,
              com.udb.core.embedding.services.v1.RegisterModelResponse>(
                service, METHODID_REGISTER_MODEL)))
        .addMethod(
          getListModelsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.ListModelsRequest,
              com.udb.core.embedding.services.v1.ListModelsResponse>(
                service, METHODID_LIST_MODELS)))
        .addMethod(
          getDeleteModelMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.DeleteModelRequest,
              com.udb.core.embedding.services.v1.DeleteModelResponse>(
                service, METHODID_DELETE_MODEL)))
        .addMethod(
          getSetModelStatusMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.SetModelStatusRequest,
              com.udb.core.embedding.services.v1.SetModelStatusResponse>(
                service, METHODID_SET_MODEL_STATUS)))
        .addMethod(
          getCutoverModelAliasMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.CutoverModelAliasRequest,
              com.udb.core.embedding.services.v1.CutoverModelAliasResponse>(
                service, METHODID_CUTOVER_MODEL_ALIAS)))
        .addMethod(
          getGetEmbeddingJobStatusMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.GetEmbeddingJobStatusRequest,
              com.udb.core.embedding.services.v1.GetEmbeddingJobStatusResponse>(
                service, METHODID_GET_EMBEDDING_JOB_STATUS)))
        .addMethod(
          getListEmbeddingWorkItemsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsRequest,
              com.udb.core.embedding.services.v1.ListEmbeddingWorkItemsResponse>(
                service, METHODID_LIST_EMBEDDING_WORK_ITEMS)))
        .addMethod(
          getReportEmbeddingBatchMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.ReportEmbeddingBatchRequest,
              com.udb.core.embedding.services.v1.ReportEmbeddingBatchResponse>(
                service, METHODID_REPORT_EMBEDDING_BATCH)))
        .addMethod(
          getReportEmbeddingFailureMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.ReportEmbeddingFailureRequest,
              com.udb.core.embedding.services.v1.ReportEmbeddingFailureResponse>(
                service, METHODID_REPORT_EMBEDDING_FAILURE)))
        .addMethod(
          getIngestDocumentMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.IngestDocumentRequest,
              com.udb.core.embedding.services.v1.IngestDocumentResponse>(
                service, METHODID_INGEST_DOCUMENT)))
        .addMethod(
          getIngestDocumentBatchMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.IngestDocumentBatchRequest,
              com.udb.core.embedding.services.v1.IngestDocumentBatchResponse>(
                service, METHODID_INGEST_DOCUMENT_BATCH)))
        .addMethod(
          getReportParsedDocumentMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.ReportParsedDocumentRequest,
              com.udb.core.embedding.services.v1.ReportParsedDocumentResponse>(
                service, METHODID_REPORT_PARSED_DOCUMENT)))
        .addMethod(
          getReportRetrievalEvaluationMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.embedding.services.v1.ReportRetrievalEvaluationRequest,
              com.udb.core.embedding.services.v1.ReportRetrievalEvaluationResponse>(
                service, METHODID_REPORT_RETRIEVAL_EVALUATION)))
        .build();
  }

  private static abstract class EmbeddingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    EmbeddingServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.embedding.services.v1.EmbeddingServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("EmbeddingService");
    }
  }

  private static final class EmbeddingServiceFileDescriptorSupplier
      extends EmbeddingServiceBaseDescriptorSupplier {
    EmbeddingServiceFileDescriptorSupplier() {}
  }

  private static final class EmbeddingServiceMethodDescriptorSupplier
      extends EmbeddingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    EmbeddingServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (EmbeddingServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new EmbeddingServiceFileDescriptorSupplier())
              .addMethod(getRegisterSourceMethod())
              .addMethod(getListSourcesMethod())
              .addMethod(getDeleteSourceMethod())
              .addMethod(getBackfillMethod())
              .addMethod(getReportEmbeddingMethod())
              .addMethod(getRetrieveMethod())
              .addMethod(getRegisterModelMethod())
              .addMethod(getListModelsMethod())
              .addMethod(getDeleteModelMethod())
              .addMethod(getSetModelStatusMethod())
              .addMethod(getCutoverModelAliasMethod())
              .addMethod(getGetEmbeddingJobStatusMethod())
              .addMethod(getListEmbeddingWorkItemsMethod())
              .addMethod(getReportEmbeddingBatchMethod())
              .addMethod(getReportEmbeddingFailureMethod())
              .addMethod(getIngestDocumentMethod())
              .addMethod(getIngestDocumentBatchMethod())
              .addMethod(getReportParsedDocumentMethod())
              .addMethod(getReportRetrievalEvaluationMethod())
              .build();
        }
      }
    }
    return result;
  }
}
