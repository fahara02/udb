package com.udb.core.authz.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * UDB-owned authorization service for RBAC, ABAC, ReBAC, tenant/project
 * domains, and audit-ready access decisions.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class AuthzServiceGrpc {

  private AuthzServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.authz.services.v1.AuthzService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.AuthzRequest,
      com.udb.core.authz.services.v1.AuthzResponse> getAuthorizeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Authorize",
      requestType = com.udb.core.authz.services.v1.AuthzRequest.class,
      responseType = com.udb.core.authz.services.v1.AuthzResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.AuthzRequest,
      com.udb.core.authz.services.v1.AuthzResponse> getAuthorizeMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.AuthzRequest, com.udb.core.authz.services.v1.AuthzResponse> getAuthorizeMethod;
    if ((getAuthorizeMethod = AuthzServiceGrpc.getAuthorizeMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getAuthorizeMethod = AuthzServiceGrpc.getAuthorizeMethod) == null) {
          AuthzServiceGrpc.getAuthorizeMethod = getAuthorizeMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.AuthzRequest, com.udb.core.authz.services.v1.AuthzResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Authorize"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.AuthzRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.AuthzResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("Authorize"))
              .build();
        }
      }
    }
    return getAuthorizeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.CheckAccessRequest,
      com.udb.core.authz.services.v1.CheckAccessResponse> getCheckAccessMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CheckAccess",
      requestType = com.udb.core.authz.services.v1.CheckAccessRequest.class,
      responseType = com.udb.core.authz.services.v1.CheckAccessResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.CheckAccessRequest,
      com.udb.core.authz.services.v1.CheckAccessResponse> getCheckAccessMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.CheckAccessRequest, com.udb.core.authz.services.v1.CheckAccessResponse> getCheckAccessMethod;
    if ((getCheckAccessMethod = AuthzServiceGrpc.getCheckAccessMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getCheckAccessMethod = AuthzServiceGrpc.getCheckAccessMethod) == null) {
          AuthzServiceGrpc.getCheckAccessMethod = getCheckAccessMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.CheckAccessRequest, com.udb.core.authz.services.v1.CheckAccessResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CheckAccess"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.CheckAccessRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.CheckAccessResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("CheckAccess"))
              .build();
        }
      }
    }
    return getCheckAccessMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.CreateRoleRequest,
      com.udb.core.authz.services.v1.CreateRoleResponse> getCreateRoleMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateRole",
      requestType = com.udb.core.authz.services.v1.CreateRoleRequest.class,
      responseType = com.udb.core.authz.services.v1.CreateRoleResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.CreateRoleRequest,
      com.udb.core.authz.services.v1.CreateRoleResponse> getCreateRoleMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.CreateRoleRequest, com.udb.core.authz.services.v1.CreateRoleResponse> getCreateRoleMethod;
    if ((getCreateRoleMethod = AuthzServiceGrpc.getCreateRoleMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getCreateRoleMethod = AuthzServiceGrpc.getCreateRoleMethod) == null) {
          AuthzServiceGrpc.getCreateRoleMethod = getCreateRoleMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.CreateRoleRequest, com.udb.core.authz.services.v1.CreateRoleResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateRole"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.CreateRoleRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.CreateRoleResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("CreateRole"))
              .build();
        }
      }
    }
    return getCreateRoleMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.AssignRoleRequest,
      com.udb.core.authz.services.v1.AssignRoleResponse> getAssignRoleMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AssignRole",
      requestType = com.udb.core.authz.services.v1.AssignRoleRequest.class,
      responseType = com.udb.core.authz.services.v1.AssignRoleResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.AssignRoleRequest,
      com.udb.core.authz.services.v1.AssignRoleResponse> getAssignRoleMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.AssignRoleRequest, com.udb.core.authz.services.v1.AssignRoleResponse> getAssignRoleMethod;
    if ((getAssignRoleMethod = AuthzServiceGrpc.getAssignRoleMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getAssignRoleMethod = AuthzServiceGrpc.getAssignRoleMethod) == null) {
          AuthzServiceGrpc.getAssignRoleMethod = getAssignRoleMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.AssignRoleRequest, com.udb.core.authz.services.v1.AssignRoleResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AssignRole"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.AssignRoleRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.AssignRoleResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("AssignRole"))
              .build();
        }
      }
    }
    return getAssignRoleMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.CreatePolicyRuleRequest,
      com.udb.core.authz.services.v1.CreatePolicyRuleResponse> getCreatePolicyRuleMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreatePolicyRule",
      requestType = com.udb.core.authz.services.v1.CreatePolicyRuleRequest.class,
      responseType = com.udb.core.authz.services.v1.CreatePolicyRuleResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.CreatePolicyRuleRequest,
      com.udb.core.authz.services.v1.CreatePolicyRuleResponse> getCreatePolicyRuleMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.CreatePolicyRuleRequest, com.udb.core.authz.services.v1.CreatePolicyRuleResponse> getCreatePolicyRuleMethod;
    if ((getCreatePolicyRuleMethod = AuthzServiceGrpc.getCreatePolicyRuleMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getCreatePolicyRuleMethod = AuthzServiceGrpc.getCreatePolicyRuleMethod) == null) {
          AuthzServiceGrpc.getCreatePolicyRuleMethod = getCreatePolicyRuleMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.CreatePolicyRuleRequest, com.udb.core.authz.services.v1.CreatePolicyRuleResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreatePolicyRule"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.CreatePolicyRuleRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.CreatePolicyRuleResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("CreatePolicyRule"))
              .build();
        }
      }
    }
    return getCreatePolicyRuleMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListUserPermissionsRequest,
      com.udb.core.authz.services.v1.ListUserPermissionsResponse> getListUserPermissionsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListUserPermissions",
      requestType = com.udb.core.authz.services.v1.ListUserPermissionsRequest.class,
      responseType = com.udb.core.authz.services.v1.ListUserPermissionsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListUserPermissionsRequest,
      com.udb.core.authz.services.v1.ListUserPermissionsResponse> getListUserPermissionsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListUserPermissionsRequest, com.udb.core.authz.services.v1.ListUserPermissionsResponse> getListUserPermissionsMethod;
    if ((getListUserPermissionsMethod = AuthzServiceGrpc.getListUserPermissionsMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getListUserPermissionsMethod = AuthzServiceGrpc.getListUserPermissionsMethod) == null) {
          AuthzServiceGrpc.getListUserPermissionsMethod = getListUserPermissionsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.ListUserPermissionsRequest, com.udb.core.authz.services.v1.ListUserPermissionsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListUserPermissions"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ListUserPermissionsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ListUserPermissionsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("ListUserPermissions"))
              .build();
        }
      }
    }
    return getListUserPermissionsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListAccessDecisionAuditsRequest,
      com.udb.core.authz.services.v1.ListAccessDecisionAuditsResponse> getListAccessDecisionAuditsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListAccessDecisionAudits",
      requestType = com.udb.core.authz.services.v1.ListAccessDecisionAuditsRequest.class,
      responseType = com.udb.core.authz.services.v1.ListAccessDecisionAuditsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListAccessDecisionAuditsRequest,
      com.udb.core.authz.services.v1.ListAccessDecisionAuditsResponse> getListAccessDecisionAuditsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListAccessDecisionAuditsRequest, com.udb.core.authz.services.v1.ListAccessDecisionAuditsResponse> getListAccessDecisionAuditsMethod;
    if ((getListAccessDecisionAuditsMethod = AuthzServiceGrpc.getListAccessDecisionAuditsMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getListAccessDecisionAuditsMethod = AuthzServiceGrpc.getListAccessDecisionAuditsMethod) == null) {
          AuthzServiceGrpc.getListAccessDecisionAuditsMethod = getListAccessDecisionAuditsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.ListAccessDecisionAuditsRequest, com.udb.core.authz.services.v1.ListAccessDecisionAuditsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListAccessDecisionAudits"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ListAccessDecisionAuditsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ListAccessDecisionAuditsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("ListAccessDecisionAudits"))
              .build();
        }
      }
    }
    return getListAccessDecisionAuditsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.RevokeRoleRequest,
      com.udb.core.authz.services.v1.RevokeRoleResponse> getRevokeRoleMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RevokeRole",
      requestType = com.udb.core.authz.services.v1.RevokeRoleRequest.class,
      responseType = com.udb.core.authz.services.v1.RevokeRoleResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.RevokeRoleRequest,
      com.udb.core.authz.services.v1.RevokeRoleResponse> getRevokeRoleMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.RevokeRoleRequest, com.udb.core.authz.services.v1.RevokeRoleResponse> getRevokeRoleMethod;
    if ((getRevokeRoleMethod = AuthzServiceGrpc.getRevokeRoleMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getRevokeRoleMethod = AuthzServiceGrpc.getRevokeRoleMethod) == null) {
          AuthzServiceGrpc.getRevokeRoleMethod = getRevokeRoleMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.RevokeRoleRequest, com.udb.core.authz.services.v1.RevokeRoleResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RevokeRole"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.RevokeRoleRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.RevokeRoleResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("RevokeRole"))
              .build();
        }
      }
    }
    return getRevokeRoleMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListUserRolesRequest,
      com.udb.core.authz.services.v1.ListUserRolesResponse> getListUserRolesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListUserRoles",
      requestType = com.udb.core.authz.services.v1.ListUserRolesRequest.class,
      responseType = com.udb.core.authz.services.v1.ListUserRolesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListUserRolesRequest,
      com.udb.core.authz.services.v1.ListUserRolesResponse> getListUserRolesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListUserRolesRequest, com.udb.core.authz.services.v1.ListUserRolesResponse> getListUserRolesMethod;
    if ((getListUserRolesMethod = AuthzServiceGrpc.getListUserRolesMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getListUserRolesMethod = AuthzServiceGrpc.getListUserRolesMethod) == null) {
          AuthzServiceGrpc.getListUserRolesMethod = getListUserRolesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.ListUserRolesRequest, com.udb.core.authz.services.v1.ListUserRolesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListUserRoles"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ListUserRolesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ListUserRolesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("ListUserRoles"))
              .build();
        }
      }
    }
    return getListUserRolesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.GetRoleRequest,
      com.udb.core.authz.services.v1.GetRoleResponse> getGetRoleMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetRole",
      requestType = com.udb.core.authz.services.v1.GetRoleRequest.class,
      responseType = com.udb.core.authz.services.v1.GetRoleResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.GetRoleRequest,
      com.udb.core.authz.services.v1.GetRoleResponse> getGetRoleMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.GetRoleRequest, com.udb.core.authz.services.v1.GetRoleResponse> getGetRoleMethod;
    if ((getGetRoleMethod = AuthzServiceGrpc.getGetRoleMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getGetRoleMethod = AuthzServiceGrpc.getGetRoleMethod) == null) {
          AuthzServiceGrpc.getGetRoleMethod = getGetRoleMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.GetRoleRequest, com.udb.core.authz.services.v1.GetRoleResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetRole"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.GetRoleRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.GetRoleResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("GetRole"))
              .build();
        }
      }
    }
    return getGetRoleMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListRolesRequest,
      com.udb.core.authz.services.v1.ListRolesResponse> getListRolesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListRoles",
      requestType = com.udb.core.authz.services.v1.ListRolesRequest.class,
      responseType = com.udb.core.authz.services.v1.ListRolesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListRolesRequest,
      com.udb.core.authz.services.v1.ListRolesResponse> getListRolesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListRolesRequest, com.udb.core.authz.services.v1.ListRolesResponse> getListRolesMethod;
    if ((getListRolesMethod = AuthzServiceGrpc.getListRolesMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getListRolesMethod = AuthzServiceGrpc.getListRolesMethod) == null) {
          AuthzServiceGrpc.getListRolesMethod = getListRolesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.ListRolesRequest, com.udb.core.authz.services.v1.ListRolesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListRoles"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ListRolesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ListRolesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("ListRoles"))
              .build();
        }
      }
    }
    return getListRolesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.BatchCheckPermissionsRequest,
      com.udb.core.authz.services.v1.BatchCheckPermissionsResponse> getBatchCheckPermissionsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "BatchCheckPermissions",
      requestType = com.udb.core.authz.services.v1.BatchCheckPermissionsRequest.class,
      responseType = com.udb.core.authz.services.v1.BatchCheckPermissionsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.BatchCheckPermissionsRequest,
      com.udb.core.authz.services.v1.BatchCheckPermissionsResponse> getBatchCheckPermissionsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.BatchCheckPermissionsRequest, com.udb.core.authz.services.v1.BatchCheckPermissionsResponse> getBatchCheckPermissionsMethod;
    if ((getBatchCheckPermissionsMethod = AuthzServiceGrpc.getBatchCheckPermissionsMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getBatchCheckPermissionsMethod = AuthzServiceGrpc.getBatchCheckPermissionsMethod) == null) {
          AuthzServiceGrpc.getBatchCheckPermissionsMethod = getBatchCheckPermissionsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.BatchCheckPermissionsRequest, com.udb.core.authz.services.v1.BatchCheckPermissionsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "BatchCheckPermissions"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.BatchCheckPermissionsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.BatchCheckPermissionsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("BatchCheckPermissions"))
              .build();
        }
      }
    }
    return getBatchCheckPermissionsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.UpdateRoleRequest,
      com.udb.core.authz.services.v1.UpdateRoleResponse> getUpdateRoleMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateRole",
      requestType = com.udb.core.authz.services.v1.UpdateRoleRequest.class,
      responseType = com.udb.core.authz.services.v1.UpdateRoleResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.UpdateRoleRequest,
      com.udb.core.authz.services.v1.UpdateRoleResponse> getUpdateRoleMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.UpdateRoleRequest, com.udb.core.authz.services.v1.UpdateRoleResponse> getUpdateRoleMethod;
    if ((getUpdateRoleMethod = AuthzServiceGrpc.getUpdateRoleMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getUpdateRoleMethod = AuthzServiceGrpc.getUpdateRoleMethod) == null) {
          AuthzServiceGrpc.getUpdateRoleMethod = getUpdateRoleMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.UpdateRoleRequest, com.udb.core.authz.services.v1.UpdateRoleResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateRole"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.UpdateRoleRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.UpdateRoleResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("UpdateRole"))
              .build();
        }
      }
    }
    return getUpdateRoleMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.DeleteRoleRequest,
      com.udb.core.authz.services.v1.DeleteRoleResponse> getDeleteRoleMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteRole",
      requestType = com.udb.core.authz.services.v1.DeleteRoleRequest.class,
      responseType = com.udb.core.authz.services.v1.DeleteRoleResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.DeleteRoleRequest,
      com.udb.core.authz.services.v1.DeleteRoleResponse> getDeleteRoleMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.DeleteRoleRequest, com.udb.core.authz.services.v1.DeleteRoleResponse> getDeleteRoleMethod;
    if ((getDeleteRoleMethod = AuthzServiceGrpc.getDeleteRoleMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getDeleteRoleMethod = AuthzServiceGrpc.getDeleteRoleMethod) == null) {
          AuthzServiceGrpc.getDeleteRoleMethod = getDeleteRoleMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.DeleteRoleRequest, com.udb.core.authz.services.v1.DeleteRoleResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteRole"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.DeleteRoleRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.DeleteRoleResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("DeleteRole"))
              .build();
        }
      }
    }
    return getDeleteRoleMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.GetPolicyRuleRequest,
      com.udb.core.authz.services.v1.GetPolicyRuleResponse> getGetPolicyRuleMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPolicyRule",
      requestType = com.udb.core.authz.services.v1.GetPolicyRuleRequest.class,
      responseType = com.udb.core.authz.services.v1.GetPolicyRuleResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.GetPolicyRuleRequest,
      com.udb.core.authz.services.v1.GetPolicyRuleResponse> getGetPolicyRuleMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.GetPolicyRuleRequest, com.udb.core.authz.services.v1.GetPolicyRuleResponse> getGetPolicyRuleMethod;
    if ((getGetPolicyRuleMethod = AuthzServiceGrpc.getGetPolicyRuleMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getGetPolicyRuleMethod = AuthzServiceGrpc.getGetPolicyRuleMethod) == null) {
          AuthzServiceGrpc.getGetPolicyRuleMethod = getGetPolicyRuleMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.GetPolicyRuleRequest, com.udb.core.authz.services.v1.GetPolicyRuleResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetPolicyRule"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.GetPolicyRuleRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.GetPolicyRuleResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("GetPolicyRule"))
              .build();
        }
      }
    }
    return getGetPolicyRuleMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListPolicyRulesRequest,
      com.udb.core.authz.services.v1.ListPolicyRulesResponse> getListPolicyRulesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListPolicyRules",
      requestType = com.udb.core.authz.services.v1.ListPolicyRulesRequest.class,
      responseType = com.udb.core.authz.services.v1.ListPolicyRulesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListPolicyRulesRequest,
      com.udb.core.authz.services.v1.ListPolicyRulesResponse> getListPolicyRulesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListPolicyRulesRequest, com.udb.core.authz.services.v1.ListPolicyRulesResponse> getListPolicyRulesMethod;
    if ((getListPolicyRulesMethod = AuthzServiceGrpc.getListPolicyRulesMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getListPolicyRulesMethod = AuthzServiceGrpc.getListPolicyRulesMethod) == null) {
          AuthzServiceGrpc.getListPolicyRulesMethod = getListPolicyRulesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.ListPolicyRulesRequest, com.udb.core.authz.services.v1.ListPolicyRulesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListPolicyRules"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ListPolicyRulesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ListPolicyRulesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("ListPolicyRules"))
              .build();
        }
      }
    }
    return getListPolicyRulesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.DeletePolicyRuleRequest,
      com.udb.core.authz.services.v1.DeletePolicyRuleResponse> getDeletePolicyRuleMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeletePolicyRule",
      requestType = com.udb.core.authz.services.v1.DeletePolicyRuleRequest.class,
      responseType = com.udb.core.authz.services.v1.DeletePolicyRuleResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.DeletePolicyRuleRequest,
      com.udb.core.authz.services.v1.DeletePolicyRuleResponse> getDeletePolicyRuleMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.DeletePolicyRuleRequest, com.udb.core.authz.services.v1.DeletePolicyRuleResponse> getDeletePolicyRuleMethod;
    if ((getDeletePolicyRuleMethod = AuthzServiceGrpc.getDeletePolicyRuleMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getDeletePolicyRuleMethod = AuthzServiceGrpc.getDeletePolicyRuleMethod) == null) {
          AuthzServiceGrpc.getDeletePolicyRuleMethod = getDeletePolicyRuleMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.DeletePolicyRuleRequest, com.udb.core.authz.services.v1.DeletePolicyRuleResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeletePolicyRule"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.DeletePolicyRuleRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.DeletePolicyRuleResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("DeletePolicyRule"))
              .build();
        }
      }
    }
    return getDeletePolicyRuleMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.PutRoleBindingRequest,
      com.udb.core.authz.services.v1.AuthMutationResponse> getPutRoleBindingMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PutRoleBinding",
      requestType = com.udb.core.authz.services.v1.PutRoleBindingRequest.class,
      responseType = com.udb.core.authz.services.v1.AuthMutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.PutRoleBindingRequest,
      com.udb.core.authz.services.v1.AuthMutationResponse> getPutRoleBindingMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.PutRoleBindingRequest, com.udb.core.authz.services.v1.AuthMutationResponse> getPutRoleBindingMethod;
    if ((getPutRoleBindingMethod = AuthzServiceGrpc.getPutRoleBindingMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getPutRoleBindingMethod = AuthzServiceGrpc.getPutRoleBindingMethod) == null) {
          AuthzServiceGrpc.getPutRoleBindingMethod = getPutRoleBindingMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.PutRoleBindingRequest, com.udb.core.authz.services.v1.AuthMutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PutRoleBinding"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.PutRoleBindingRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.AuthMutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("PutRoleBinding"))
              .build();
        }
      }
    }
    return getPutRoleBindingMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.PutRelationshipRequest,
      com.udb.core.authz.services.v1.AuthMutationResponse> getPutRelationshipMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PutRelationship",
      requestType = com.udb.core.authz.services.v1.PutRelationshipRequest.class,
      responseType = com.udb.core.authz.services.v1.AuthMutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.PutRelationshipRequest,
      com.udb.core.authz.services.v1.AuthMutationResponse> getPutRelationshipMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.PutRelationshipRequest, com.udb.core.authz.services.v1.AuthMutationResponse> getPutRelationshipMethod;
    if ((getPutRelationshipMethod = AuthzServiceGrpc.getPutRelationshipMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getPutRelationshipMethod = AuthzServiceGrpc.getPutRelationshipMethod) == null) {
          AuthzServiceGrpc.getPutRelationshipMethod = getPutRelationshipMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.PutRelationshipRequest, com.udb.core.authz.services.v1.AuthMutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PutRelationship"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.PutRelationshipRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.AuthMutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("PutRelationship"))
              .build();
        }
      }
    }
    return getPutRelationshipMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.PutAuthzPolicyRequest,
      com.udb.core.authz.services.v1.AuthMutationResponse> getPutAuthzPolicyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PutAuthzPolicy",
      requestType = com.udb.core.authz.services.v1.PutAuthzPolicyRequest.class,
      responseType = com.udb.core.authz.services.v1.AuthMutationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.PutAuthzPolicyRequest,
      com.udb.core.authz.services.v1.AuthMutationResponse> getPutAuthzPolicyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.PutAuthzPolicyRequest, com.udb.core.authz.services.v1.AuthMutationResponse> getPutAuthzPolicyMethod;
    if ((getPutAuthzPolicyMethod = AuthzServiceGrpc.getPutAuthzPolicyMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getPutAuthzPolicyMethod = AuthzServiceGrpc.getPutAuthzPolicyMethod) == null) {
          AuthzServiceGrpc.getPutAuthzPolicyMethod = getPutAuthzPolicyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.PutAuthzPolicyRequest, com.udb.core.authz.services.v1.AuthMutationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PutAuthzPolicy"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.PutAuthzPolicyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.AuthMutationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("PutAuthzPolicy"))
              .build();
        }
      }
    }
    return getPutAuthzPolicyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.LintAuthzPoliciesRequest,
      com.udb.core.authz.services.v1.LintAuthzPoliciesResponse> getLintAuthzPoliciesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "LintAuthzPolicies",
      requestType = com.udb.core.authz.services.v1.LintAuthzPoliciesRequest.class,
      responseType = com.udb.core.authz.services.v1.LintAuthzPoliciesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.LintAuthzPoliciesRequest,
      com.udb.core.authz.services.v1.LintAuthzPoliciesResponse> getLintAuthzPoliciesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.LintAuthzPoliciesRequest, com.udb.core.authz.services.v1.LintAuthzPoliciesResponse> getLintAuthzPoliciesMethod;
    if ((getLintAuthzPoliciesMethod = AuthzServiceGrpc.getLintAuthzPoliciesMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getLintAuthzPoliciesMethod = AuthzServiceGrpc.getLintAuthzPoliciesMethod) == null) {
          AuthzServiceGrpc.getLintAuthzPoliciesMethod = getLintAuthzPoliciesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.LintAuthzPoliciesRequest, com.udb.core.authz.services.v1.LintAuthzPoliciesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "LintAuthzPolicies"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.LintAuthzPoliciesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.LintAuthzPoliciesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("LintAuthzPolicies"))
              .build();
        }
      }
    }
    return getLintAuthzPoliciesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.NativeAccessRequest,
      com.udb.core.authz.services.v1.NativeAccessResponse> getGetNativeAccessMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetNativeAccess",
      requestType = com.udb.core.authz.services.v1.NativeAccessRequest.class,
      responseType = com.udb.core.authz.services.v1.NativeAccessResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.NativeAccessRequest,
      com.udb.core.authz.services.v1.NativeAccessResponse> getGetNativeAccessMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.NativeAccessRequest, com.udb.core.authz.services.v1.NativeAccessResponse> getGetNativeAccessMethod;
    if ((getGetNativeAccessMethod = AuthzServiceGrpc.getGetNativeAccessMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getGetNativeAccessMethod = AuthzServiceGrpc.getGetNativeAccessMethod) == null) {
          AuthzServiceGrpc.getGetNativeAccessMethod = getGetNativeAccessMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.NativeAccessRequest, com.udb.core.authz.services.v1.NativeAccessResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetNativeAccess"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.NativeAccessRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.NativeAccessResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("GetNativeAccess"))
              .build();
        }
      }
    }
    return getGetNativeAccessMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.PolicyBundleRequest,
      com.udb.core.authz.services.v1.PolicyBundleResponse> getGetPolicyBundleMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPolicyBundle",
      requestType = com.udb.core.authz.services.v1.PolicyBundleRequest.class,
      responseType = com.udb.core.authz.services.v1.PolicyBundleResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.PolicyBundleRequest,
      com.udb.core.authz.services.v1.PolicyBundleResponse> getGetPolicyBundleMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.PolicyBundleRequest, com.udb.core.authz.services.v1.PolicyBundleResponse> getGetPolicyBundleMethod;
    if ((getGetPolicyBundleMethod = AuthzServiceGrpc.getGetPolicyBundleMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getGetPolicyBundleMethod = AuthzServiceGrpc.getGetPolicyBundleMethod) == null) {
          AuthzServiceGrpc.getGetPolicyBundleMethod = getGetPolicyBundleMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.PolicyBundleRequest, com.udb.core.authz.services.v1.PolicyBundleResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetPolicyBundle"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.PolicyBundleRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.PolicyBundleResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("GetPolicyBundle"))
              .build();
        }
      }
    }
    return getGetPolicyBundleMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.CreatePolicyDraftRequest,
      com.udb.core.authz.services.v1.PolicyDraftResponse> getCreatePolicyDraftMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreatePolicyDraft",
      requestType = com.udb.core.authz.services.v1.CreatePolicyDraftRequest.class,
      responseType = com.udb.core.authz.services.v1.PolicyDraftResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.CreatePolicyDraftRequest,
      com.udb.core.authz.services.v1.PolicyDraftResponse> getCreatePolicyDraftMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.CreatePolicyDraftRequest, com.udb.core.authz.services.v1.PolicyDraftResponse> getCreatePolicyDraftMethod;
    if ((getCreatePolicyDraftMethod = AuthzServiceGrpc.getCreatePolicyDraftMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getCreatePolicyDraftMethod = AuthzServiceGrpc.getCreatePolicyDraftMethod) == null) {
          AuthzServiceGrpc.getCreatePolicyDraftMethod = getCreatePolicyDraftMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.CreatePolicyDraftRequest, com.udb.core.authz.services.v1.PolicyDraftResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreatePolicyDraft"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.CreatePolicyDraftRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.PolicyDraftResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("CreatePolicyDraft"))
              .build();
        }
      }
    }
    return getCreatePolicyDraftMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.UpdatePolicyDraftRequest,
      com.udb.core.authz.services.v1.PolicyDraftResponse> getUpdatePolicyDraftMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdatePolicyDraft",
      requestType = com.udb.core.authz.services.v1.UpdatePolicyDraftRequest.class,
      responseType = com.udb.core.authz.services.v1.PolicyDraftResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.UpdatePolicyDraftRequest,
      com.udb.core.authz.services.v1.PolicyDraftResponse> getUpdatePolicyDraftMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.UpdatePolicyDraftRequest, com.udb.core.authz.services.v1.PolicyDraftResponse> getUpdatePolicyDraftMethod;
    if ((getUpdatePolicyDraftMethod = AuthzServiceGrpc.getUpdatePolicyDraftMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getUpdatePolicyDraftMethod = AuthzServiceGrpc.getUpdatePolicyDraftMethod) == null) {
          AuthzServiceGrpc.getUpdatePolicyDraftMethod = getUpdatePolicyDraftMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.UpdatePolicyDraftRequest, com.udb.core.authz.services.v1.PolicyDraftResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdatePolicyDraft"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.UpdatePolicyDraftRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.PolicyDraftResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("UpdatePolicyDraft"))
              .build();
        }
      }
    }
    return getUpdatePolicyDraftMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.DiffPolicyDraftRequest,
      com.udb.core.authz.services.v1.DiffPolicyDraftResponse> getDiffPolicyDraftMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DiffPolicyDraft",
      requestType = com.udb.core.authz.services.v1.DiffPolicyDraftRequest.class,
      responseType = com.udb.core.authz.services.v1.DiffPolicyDraftResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.DiffPolicyDraftRequest,
      com.udb.core.authz.services.v1.DiffPolicyDraftResponse> getDiffPolicyDraftMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.DiffPolicyDraftRequest, com.udb.core.authz.services.v1.DiffPolicyDraftResponse> getDiffPolicyDraftMethod;
    if ((getDiffPolicyDraftMethod = AuthzServiceGrpc.getDiffPolicyDraftMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getDiffPolicyDraftMethod = AuthzServiceGrpc.getDiffPolicyDraftMethod) == null) {
          AuthzServiceGrpc.getDiffPolicyDraftMethod = getDiffPolicyDraftMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.DiffPolicyDraftRequest, com.udb.core.authz.services.v1.DiffPolicyDraftResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DiffPolicyDraft"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.DiffPolicyDraftRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.DiffPolicyDraftResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("DiffPolicyDraft"))
              .build();
        }
      }
    }
    return getDiffPolicyDraftMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.SubmitPolicyDraftRequest,
      com.udb.core.authz.services.v1.PolicyDraftResponse> getSubmitPolicyDraftMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SubmitPolicyDraft",
      requestType = com.udb.core.authz.services.v1.SubmitPolicyDraftRequest.class,
      responseType = com.udb.core.authz.services.v1.PolicyDraftResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.SubmitPolicyDraftRequest,
      com.udb.core.authz.services.v1.PolicyDraftResponse> getSubmitPolicyDraftMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.SubmitPolicyDraftRequest, com.udb.core.authz.services.v1.PolicyDraftResponse> getSubmitPolicyDraftMethod;
    if ((getSubmitPolicyDraftMethod = AuthzServiceGrpc.getSubmitPolicyDraftMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getSubmitPolicyDraftMethod = AuthzServiceGrpc.getSubmitPolicyDraftMethod) == null) {
          AuthzServiceGrpc.getSubmitPolicyDraftMethod = getSubmitPolicyDraftMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.SubmitPolicyDraftRequest, com.udb.core.authz.services.v1.PolicyDraftResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SubmitPolicyDraft"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.SubmitPolicyDraftRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.PolicyDraftResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("SubmitPolicyDraft"))
              .build();
        }
      }
    }
    return getSubmitPolicyDraftMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ApprovePolicyDraftRequest,
      com.udb.core.authz.services.v1.PolicyApprovalResponse> getApprovePolicyDraftMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ApprovePolicyDraft",
      requestType = com.udb.core.authz.services.v1.ApprovePolicyDraftRequest.class,
      responseType = com.udb.core.authz.services.v1.PolicyApprovalResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ApprovePolicyDraftRequest,
      com.udb.core.authz.services.v1.PolicyApprovalResponse> getApprovePolicyDraftMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ApprovePolicyDraftRequest, com.udb.core.authz.services.v1.PolicyApprovalResponse> getApprovePolicyDraftMethod;
    if ((getApprovePolicyDraftMethod = AuthzServiceGrpc.getApprovePolicyDraftMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getApprovePolicyDraftMethod = AuthzServiceGrpc.getApprovePolicyDraftMethod) == null) {
          AuthzServiceGrpc.getApprovePolicyDraftMethod = getApprovePolicyDraftMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.ApprovePolicyDraftRequest, com.udb.core.authz.services.v1.PolicyApprovalResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ApprovePolicyDraft"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ApprovePolicyDraftRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.PolicyApprovalResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("ApprovePolicyDraft"))
              .build();
        }
      }
    }
    return getApprovePolicyDraftMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.RejectPolicyDraftRequest,
      com.udb.core.authz.services.v1.PolicyApprovalResponse> getRejectPolicyDraftMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RejectPolicyDraft",
      requestType = com.udb.core.authz.services.v1.RejectPolicyDraftRequest.class,
      responseType = com.udb.core.authz.services.v1.PolicyApprovalResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.RejectPolicyDraftRequest,
      com.udb.core.authz.services.v1.PolicyApprovalResponse> getRejectPolicyDraftMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.RejectPolicyDraftRequest, com.udb.core.authz.services.v1.PolicyApprovalResponse> getRejectPolicyDraftMethod;
    if ((getRejectPolicyDraftMethod = AuthzServiceGrpc.getRejectPolicyDraftMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getRejectPolicyDraftMethod = AuthzServiceGrpc.getRejectPolicyDraftMethod) == null) {
          AuthzServiceGrpc.getRejectPolicyDraftMethod = getRejectPolicyDraftMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.RejectPolicyDraftRequest, com.udb.core.authz.services.v1.PolicyApprovalResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RejectPolicyDraft"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.RejectPolicyDraftRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.PolicyApprovalResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("RejectPolicyDraft"))
              .build();
        }
      }
    }
    return getRejectPolicyDraftMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ActivatePolicyVersionRequest,
      com.udb.core.authz.services.v1.ActivationResponse> getActivatePolicyVersionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ActivatePolicyVersion",
      requestType = com.udb.core.authz.services.v1.ActivatePolicyVersionRequest.class,
      responseType = com.udb.core.authz.services.v1.ActivationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ActivatePolicyVersionRequest,
      com.udb.core.authz.services.v1.ActivationResponse> getActivatePolicyVersionMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ActivatePolicyVersionRequest, com.udb.core.authz.services.v1.ActivationResponse> getActivatePolicyVersionMethod;
    if ((getActivatePolicyVersionMethod = AuthzServiceGrpc.getActivatePolicyVersionMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getActivatePolicyVersionMethod = AuthzServiceGrpc.getActivatePolicyVersionMethod) == null) {
          AuthzServiceGrpc.getActivatePolicyVersionMethod = getActivatePolicyVersionMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.ActivatePolicyVersionRequest, com.udb.core.authz.services.v1.ActivationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ActivatePolicyVersion"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ActivatePolicyVersionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ActivationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("ActivatePolicyVersion"))
              .build();
        }
      }
    }
    return getActivatePolicyVersionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.RollbackPolicyVersionRequest,
      com.udb.core.authz.services.v1.ActivationResponse> getRollbackPolicyVersionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RollbackPolicyVersion",
      requestType = com.udb.core.authz.services.v1.RollbackPolicyVersionRequest.class,
      responseType = com.udb.core.authz.services.v1.ActivationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.RollbackPolicyVersionRequest,
      com.udb.core.authz.services.v1.ActivationResponse> getRollbackPolicyVersionMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.RollbackPolicyVersionRequest, com.udb.core.authz.services.v1.ActivationResponse> getRollbackPolicyVersionMethod;
    if ((getRollbackPolicyVersionMethod = AuthzServiceGrpc.getRollbackPolicyVersionMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getRollbackPolicyVersionMethod = AuthzServiceGrpc.getRollbackPolicyVersionMethod) == null) {
          AuthzServiceGrpc.getRollbackPolicyVersionMethod = getRollbackPolicyVersionMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.RollbackPolicyVersionRequest, com.udb.core.authz.services.v1.ActivationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RollbackPolicyVersion"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.RollbackPolicyVersionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ActivationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("RollbackPolicyVersion"))
              .build();
        }
      }
    }
    return getRollbackPolicyVersionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ActivateCanaryRequest,
      com.udb.core.authz.services.v1.CanaryResponse> getActivateCanaryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ActivateCanary",
      requestType = com.udb.core.authz.services.v1.ActivateCanaryRequest.class,
      responseType = com.udb.core.authz.services.v1.CanaryResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ActivateCanaryRequest,
      com.udb.core.authz.services.v1.CanaryResponse> getActivateCanaryMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ActivateCanaryRequest, com.udb.core.authz.services.v1.CanaryResponse> getActivateCanaryMethod;
    if ((getActivateCanaryMethod = AuthzServiceGrpc.getActivateCanaryMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getActivateCanaryMethod = AuthzServiceGrpc.getActivateCanaryMethod) == null) {
          AuthzServiceGrpc.getActivateCanaryMethod = getActivateCanaryMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.ActivateCanaryRequest, com.udb.core.authz.services.v1.CanaryResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ActivateCanary"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ActivateCanaryRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.CanaryResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("ActivateCanary"))
              .build();
        }
      }
    }
    return getActivateCanaryMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.PromoteCanaryRequest,
      com.udb.core.authz.services.v1.CanaryResponse> getPromoteCanaryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PromoteCanary",
      requestType = com.udb.core.authz.services.v1.PromoteCanaryRequest.class,
      responseType = com.udb.core.authz.services.v1.CanaryResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.PromoteCanaryRequest,
      com.udb.core.authz.services.v1.CanaryResponse> getPromoteCanaryMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.PromoteCanaryRequest, com.udb.core.authz.services.v1.CanaryResponse> getPromoteCanaryMethod;
    if ((getPromoteCanaryMethod = AuthzServiceGrpc.getPromoteCanaryMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getPromoteCanaryMethod = AuthzServiceGrpc.getPromoteCanaryMethod) == null) {
          AuthzServiceGrpc.getPromoteCanaryMethod = getPromoteCanaryMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.PromoteCanaryRequest, com.udb.core.authz.services.v1.CanaryResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PromoteCanary"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.PromoteCanaryRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.CanaryResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("PromoteCanary"))
              .build();
        }
      }
    }
    return getPromoteCanaryMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.GetCanaryStatusRequest,
      com.udb.core.authz.services.v1.GetCanaryStatusResponse> getGetCanaryStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetCanaryStatus",
      requestType = com.udb.core.authz.services.v1.GetCanaryStatusRequest.class,
      responseType = com.udb.core.authz.services.v1.GetCanaryStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.GetCanaryStatusRequest,
      com.udb.core.authz.services.v1.GetCanaryStatusResponse> getGetCanaryStatusMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.GetCanaryStatusRequest, com.udb.core.authz.services.v1.GetCanaryStatusResponse> getGetCanaryStatusMethod;
    if ((getGetCanaryStatusMethod = AuthzServiceGrpc.getGetCanaryStatusMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getGetCanaryStatusMethod = AuthzServiceGrpc.getGetCanaryStatusMethod) == null) {
          AuthzServiceGrpc.getGetCanaryStatusMethod = getGetCanaryStatusMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.GetCanaryStatusRequest, com.udb.core.authz.services.v1.GetCanaryStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetCanaryStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.GetCanaryStatusRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.GetCanaryStatusResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("GetCanaryStatus"))
              .build();
        }
      }
    }
    return getGetCanaryStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListPolicyVersionsRequest,
      com.udb.core.authz.services.v1.ListPolicyVersionsResponse> getListPolicyVersionsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListPolicyVersions",
      requestType = com.udb.core.authz.services.v1.ListPolicyVersionsRequest.class,
      responseType = com.udb.core.authz.services.v1.ListPolicyVersionsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListPolicyVersionsRequest,
      com.udb.core.authz.services.v1.ListPolicyVersionsResponse> getListPolicyVersionsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ListPolicyVersionsRequest, com.udb.core.authz.services.v1.ListPolicyVersionsResponse> getListPolicyVersionsMethod;
    if ((getListPolicyVersionsMethod = AuthzServiceGrpc.getListPolicyVersionsMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getListPolicyVersionsMethod = AuthzServiceGrpc.getListPolicyVersionsMethod) == null) {
          AuthzServiceGrpc.getListPolicyVersionsMethod = getListPolicyVersionsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.ListPolicyVersionsRequest, com.udb.core.authz.services.v1.ListPolicyVersionsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListPolicyVersions"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ListPolicyVersionsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ListPolicyVersionsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("ListPolicyVersions"))
              .build();
        }
      }
    }
    return getListPolicyVersionsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.SimulatePolicyRequest,
      com.udb.core.authz.services.v1.SimulatePolicyResponse> getSimulatePolicyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SimulatePolicy",
      requestType = com.udb.core.authz.services.v1.SimulatePolicyRequest.class,
      responseType = com.udb.core.authz.services.v1.SimulatePolicyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.SimulatePolicyRequest,
      com.udb.core.authz.services.v1.SimulatePolicyResponse> getSimulatePolicyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.SimulatePolicyRequest, com.udb.core.authz.services.v1.SimulatePolicyResponse> getSimulatePolicyMethod;
    if ((getSimulatePolicyMethod = AuthzServiceGrpc.getSimulatePolicyMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getSimulatePolicyMethod = AuthzServiceGrpc.getSimulatePolicyMethod) == null) {
          AuthzServiceGrpc.getSimulatePolicyMethod = getSimulatePolicyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.SimulatePolicyRequest, com.udb.core.authz.services.v1.SimulatePolicyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SimulatePolicy"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.SimulatePolicyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.SimulatePolicyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("SimulatePolicy"))
              .build();
        }
      }
    }
    return getSimulatePolicyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ExplainPolicyRequest,
      com.udb.core.authz.services.v1.ExplainPolicyResponse> getExplainPolicyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ExplainPolicy",
      requestType = com.udb.core.authz.services.v1.ExplainPolicyRequest.class,
      responseType = com.udb.core.authz.services.v1.ExplainPolicyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ExplainPolicyRequest,
      com.udb.core.authz.services.v1.ExplainPolicyResponse> getExplainPolicyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.ExplainPolicyRequest, com.udb.core.authz.services.v1.ExplainPolicyResponse> getExplainPolicyMethod;
    if ((getExplainPolicyMethod = AuthzServiceGrpc.getExplainPolicyMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getExplainPolicyMethod = AuthzServiceGrpc.getExplainPolicyMethod) == null) {
          AuthzServiceGrpc.getExplainPolicyMethod = getExplainPolicyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.ExplainPolicyRequest, com.udb.core.authz.services.v1.ExplainPolicyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ExplainPolicy"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ExplainPolicyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.ExplainPolicyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("ExplainPolicy"))
              .build();
        }
      }
    }
    return getExplainPolicyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.GetAuthzRevisionRequest,
      com.udb.core.authz.services.v1.GetAuthzRevisionResponse> getGetAuthzRevisionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetAuthzRevision",
      requestType = com.udb.core.authz.services.v1.GetAuthzRevisionRequest.class,
      responseType = com.udb.core.authz.services.v1.GetAuthzRevisionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.GetAuthzRevisionRequest,
      com.udb.core.authz.services.v1.GetAuthzRevisionResponse> getGetAuthzRevisionMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.GetAuthzRevisionRequest, com.udb.core.authz.services.v1.GetAuthzRevisionResponse> getGetAuthzRevisionMethod;
    if ((getGetAuthzRevisionMethod = AuthzServiceGrpc.getGetAuthzRevisionMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getGetAuthzRevisionMethod = AuthzServiceGrpc.getGetAuthzRevisionMethod) == null) {
          AuthzServiceGrpc.getGetAuthzRevisionMethod = getGetAuthzRevisionMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.GetAuthzRevisionRequest, com.udb.core.authz.services.v1.GetAuthzRevisionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetAuthzRevision"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.GetAuthzRevisionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.GetAuthzRevisionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("GetAuthzRevision"))
              .build();
        }
      }
    }
    return getGetAuthzRevisionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.InvalidatePolicyBundlesRequest,
      com.udb.core.authz.services.v1.InvalidatePolicyBundlesResponse> getInvalidatePolicyBundlesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "InvalidatePolicyBundles",
      requestType = com.udb.core.authz.services.v1.InvalidatePolicyBundlesRequest.class,
      responseType = com.udb.core.authz.services.v1.InvalidatePolicyBundlesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.InvalidatePolicyBundlesRequest,
      com.udb.core.authz.services.v1.InvalidatePolicyBundlesResponse> getInvalidatePolicyBundlesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.InvalidatePolicyBundlesRequest, com.udb.core.authz.services.v1.InvalidatePolicyBundlesResponse> getInvalidatePolicyBundlesMethod;
    if ((getInvalidatePolicyBundlesMethod = AuthzServiceGrpc.getInvalidatePolicyBundlesMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getInvalidatePolicyBundlesMethod = AuthzServiceGrpc.getInvalidatePolicyBundlesMethod) == null) {
          AuthzServiceGrpc.getInvalidatePolicyBundlesMethod = getInvalidatePolicyBundlesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.InvalidatePolicyBundlesRequest, com.udb.core.authz.services.v1.InvalidatePolicyBundlesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "InvalidatePolicyBundles"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.InvalidatePolicyBundlesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.InvalidatePolicyBundlesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("InvalidatePolicyBundles"))
              .build();
        }
      }
    }
    return getInvalidatePolicyBundlesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.SeedBuiltinRolesRequest,
      com.udb.core.authz.services.v1.SeedBuiltinRolesResponse> getSeedBuiltinRolesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SeedBuiltinRoles",
      requestType = com.udb.core.authz.services.v1.SeedBuiltinRolesRequest.class,
      responseType = com.udb.core.authz.services.v1.SeedBuiltinRolesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.SeedBuiltinRolesRequest,
      com.udb.core.authz.services.v1.SeedBuiltinRolesResponse> getSeedBuiltinRolesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.SeedBuiltinRolesRequest, com.udb.core.authz.services.v1.SeedBuiltinRolesResponse> getSeedBuiltinRolesMethod;
    if ((getSeedBuiltinRolesMethod = AuthzServiceGrpc.getSeedBuiltinRolesMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getSeedBuiltinRolesMethod = AuthzServiceGrpc.getSeedBuiltinRolesMethod) == null) {
          AuthzServiceGrpc.getSeedBuiltinRolesMethod = getSeedBuiltinRolesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.SeedBuiltinRolesRequest, com.udb.core.authz.services.v1.SeedBuiltinRolesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SeedBuiltinRoles"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.SeedBuiltinRolesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.SeedBuiltinRolesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("SeedBuiltinRoles"))
              .build();
        }
      }
    }
    return getSeedBuiltinRolesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.MigrateLegacyPoliciesRequest,
      com.udb.core.authz.services.v1.MigrateLegacyPoliciesResponse> getMigrateLegacyPoliciesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MigrateLegacyPolicies",
      requestType = com.udb.core.authz.services.v1.MigrateLegacyPoliciesRequest.class,
      responseType = com.udb.core.authz.services.v1.MigrateLegacyPoliciesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.MigrateLegacyPoliciesRequest,
      com.udb.core.authz.services.v1.MigrateLegacyPoliciesResponse> getMigrateLegacyPoliciesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authz.services.v1.MigrateLegacyPoliciesRequest, com.udb.core.authz.services.v1.MigrateLegacyPoliciesResponse> getMigrateLegacyPoliciesMethod;
    if ((getMigrateLegacyPoliciesMethod = AuthzServiceGrpc.getMigrateLegacyPoliciesMethod) == null) {
      synchronized (AuthzServiceGrpc.class) {
        if ((getMigrateLegacyPoliciesMethod = AuthzServiceGrpc.getMigrateLegacyPoliciesMethod) == null) {
          AuthzServiceGrpc.getMigrateLegacyPoliciesMethod = getMigrateLegacyPoliciesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authz.services.v1.MigrateLegacyPoliciesRequest, com.udb.core.authz.services.v1.MigrateLegacyPoliciesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MigrateLegacyPolicies"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.MigrateLegacyPoliciesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authz.services.v1.MigrateLegacyPoliciesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthzServiceMethodDescriptorSupplier("MigrateLegacyPolicies"))
              .build();
        }
      }
    }
    return getMigrateLegacyPoliciesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static AuthzServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AuthzServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AuthzServiceStub>() {
        @java.lang.Override
        public AuthzServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AuthzServiceStub(channel, callOptions);
        }
      };
    return AuthzServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static AuthzServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AuthzServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AuthzServiceBlockingV2Stub>() {
        @java.lang.Override
        public AuthzServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AuthzServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return AuthzServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static AuthzServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AuthzServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AuthzServiceBlockingStub>() {
        @java.lang.Override
        public AuthzServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AuthzServiceBlockingStub(channel, callOptions);
        }
      };
    return AuthzServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static AuthzServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AuthzServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AuthzServiceFutureStub>() {
        @java.lang.Override
        public AuthzServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AuthzServiceFutureStub(channel, callOptions);
        }
      };
    return AuthzServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * UDB-owned authorization service for RBAC, ABAC, ReBAC, tenant/project
   * domains, and audit-ready access decisions.
   * </pre>
   */
  public interface AsyncService {

    /**
     */
    default void authorize(com.udb.core.authz.services.v1.AuthzRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.AuthzResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAuthorizeMethod(), responseObserver);
    }

    /**
     */
    default void checkAccess(com.udb.core.authz.services.v1.CheckAccessRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.CheckAccessResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCheckAccessMethod(), responseObserver);
    }

    /**
     */
    default void createRole(com.udb.core.authz.services.v1.CreateRoleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.CreateRoleResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateRoleMethod(), responseObserver);
    }

    /**
     */
    default void assignRole(com.udb.core.authz.services.v1.AssignRoleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.AssignRoleResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAssignRoleMethod(), responseObserver);
    }

    /**
     */
    default void createPolicyRule(com.udb.core.authz.services.v1.CreatePolicyRuleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.CreatePolicyRuleResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreatePolicyRuleMethod(), responseObserver);
    }

    /**
     */
    default void listUserPermissions(com.udb.core.authz.services.v1.ListUserPermissionsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListUserPermissionsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListUserPermissionsMethod(), responseObserver);
    }

    /**
     */
    default void listAccessDecisionAudits(com.udb.core.authz.services.v1.ListAccessDecisionAuditsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListAccessDecisionAuditsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListAccessDecisionAuditsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Revoke a role from a user.
     * </pre>
     */
    default void revokeRole(com.udb.core.authz.services.v1.RevokeRoleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.RevokeRoleResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRevokeRoleMethod(), responseObserver);
    }

    /**
     * <pre>
     * List all role assignments for a user.
     * </pre>
     */
    default void listUserRoles(com.udb.core.authz.services.v1.ListUserRolesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListUserRolesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListUserRolesMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get a role by ID.
     * </pre>
     */
    default void getRole(com.udb.core.authz.services.v1.GetRoleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.GetRoleResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetRoleMethod(), responseObserver);
    }

    /**
     * <pre>
     * List all roles for a domain/tenant.
     * </pre>
     */
    default void listRoles(com.udb.core.authz.services.v1.ListRolesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListRolesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListRolesMethod(), responseObserver);
    }

    /**
     * <pre>
     * Batch check multiple permissions at once.
     * </pre>
     */
    default void batchCheckPermissions(com.udb.core.authz.services.v1.BatchCheckPermissionsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.BatchCheckPermissionsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getBatchCheckPermissionsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Update a role's name, description, or active status.
     * </pre>
     */
    default void updateRole(com.udb.core.authz.services.v1.UpdateRoleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.UpdateRoleResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateRoleMethod(), responseObserver);
    }

    /**
     * <pre>
     * Delete a role (soft-delete; existing assignments are revoked).
     * </pre>
     */
    default void deleteRole(com.udb.core.authz.services.v1.DeleteRoleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.DeleteRoleResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteRoleMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get a single policy rule by ID.
     * </pre>
     */
    default void getPolicyRule(com.udb.core.authz.services.v1.GetPolicyRuleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.GetPolicyRuleResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetPolicyRuleMethod(), responseObserver);
    }

    /**
     * <pre>
     * List policy rules with optional domain/subject/object filters.
     * </pre>
     */
    default void listPolicyRules(com.udb.core.authz.services.v1.ListPolicyRulesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListPolicyRulesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListPolicyRulesMethod(), responseObserver);
    }

    /**
     * <pre>
     * Delete a policy rule.
     * </pre>
     */
    default void deletePolicyRule(com.udb.core.authz.services.v1.DeletePolicyRuleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.DeletePolicyRuleResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeletePolicyRuleMethod(), responseObserver);
    }

    /**
     */
    default void putRoleBinding(com.udb.core.authz.services.v1.PutRoleBindingRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.AuthMutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPutRoleBindingMethod(), responseObserver);
    }

    /**
     */
    default void putRelationship(com.udb.core.authz.services.v1.PutRelationshipRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.AuthMutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPutRelationshipMethod(), responseObserver);
    }

    /**
     */
    default void putAuthzPolicy(com.udb.core.authz.services.v1.PutAuthzPolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.AuthMutationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPutAuthzPolicyMethod(), responseObserver);
    }

    /**
     */
    default void lintAuthzPolicies(com.udb.core.authz.services.v1.LintAuthzPoliciesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.LintAuthzPoliciesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getLintAuthzPoliciesMethod(), responseObserver);
    }

    /**
     * <pre>
     * Stage 2: authorize and, when allowed, mint a short-lived native-access
     * contract (restricted role + scoped DSN + RLS session variables).
     * </pre>
     */
    default void getNativeAccess(com.udb.core.authz.services.v1.NativeAccessRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.NativeAccessResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetNativeAccessMethod(), responseObserver);
    }

    /**
     * <pre>
     * Stage 2: return a signed policy bundle for local SDK authorization caches.
     * </pre>
     */
    default void getPolicyBundle(com.udb.core.authz.services.v1.PolicyBundleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyBundleResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetPolicyBundleMethod(), responseObserver);
    }

    /**
     */
    default void createPolicyDraft(com.udb.core.authz.services.v1.CreatePolicyDraftRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyDraftResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreatePolicyDraftMethod(), responseObserver);
    }

    /**
     */
    default void updatePolicyDraft(com.udb.core.authz.services.v1.UpdatePolicyDraftRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyDraftResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdatePolicyDraftMethod(), responseObserver);
    }

    /**
     */
    default void diffPolicyDraft(com.udb.core.authz.services.v1.DiffPolicyDraftRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.DiffPolicyDraftResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDiffPolicyDraftMethod(), responseObserver);
    }

    /**
     */
    default void submitPolicyDraft(com.udb.core.authz.services.v1.SubmitPolicyDraftRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyDraftResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSubmitPolicyDraftMethod(), responseObserver);
    }

    /**
     */
    default void approvePolicyDraft(com.udb.core.authz.services.v1.ApprovePolicyDraftRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyApprovalResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getApprovePolicyDraftMethod(), responseObserver);
    }

    /**
     */
    default void rejectPolicyDraft(com.udb.core.authz.services.v1.RejectPolicyDraftRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyApprovalResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRejectPolicyDraftMethod(), responseObserver);
    }

    /**
     */
    default void activatePolicyVersion(com.udb.core.authz.services.v1.ActivatePolicyVersionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ActivationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getActivatePolicyVersionMethod(), responseObserver);
    }

    /**
     */
    default void rollbackPolicyVersion(com.udb.core.authz.services.v1.RollbackPolicyVersionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ActivationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRollbackPolicyVersionMethod(), responseObserver);
    }

    /**
     * <pre>
     * Activate a policy version to a canary scope (subset of the fleet) before
     * fleet-wide. A metric-based evaluator then auto-rolls back on breach.
     * </pre>
     */
    default void activateCanary(com.udb.core.authz.services.v1.ActivateCanaryRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.CanaryResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getActivateCanaryMethod(), responseObserver);
    }

    /**
     * <pre>
     * Promote a baked, within-threshold canary to fleet-wide enforcement.
     * </pre>
     */
    default void promoteCanary(com.udb.core.authz.services.v1.PromoteCanaryRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.CanaryResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPromoteCanaryMethod(), responseObserver);
    }

    /**
     * <pre>
     * Read a canary's current state + promote-eligibility.
     * </pre>
     */
    default void getCanaryStatus(com.udb.core.authz.services.v1.GetCanaryStatusRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.GetCanaryStatusResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetCanaryStatusMethod(), responseObserver);
    }

    /**
     */
    default void listPolicyVersions(com.udb.core.authz.services.v1.ListPolicyVersionsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListPolicyVersionsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListPolicyVersionsMethod(), responseObserver);
    }

    /**
     */
    default void simulatePolicy(com.udb.core.authz.services.v1.SimulatePolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.SimulatePolicyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSimulatePolicyMethod(), responseObserver);
    }

    /**
     */
    default void explainPolicy(com.udb.core.authz.services.v1.ExplainPolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ExplainPolicyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExplainPolicyMethod(), responseObserver);
    }

    /**
     */
    default void getAuthzRevision(com.udb.core.authz.services.v1.GetAuthzRevisionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.GetAuthzRevisionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetAuthzRevisionMethod(), responseObserver);
    }

    /**
     */
    default void invalidatePolicyBundles(com.udb.core.authz.services.v1.InvalidatePolicyBundlesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.InvalidatePolicyBundlesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getInvalidatePolicyBundlesMethod(), responseObserver);
    }

    /**
     */
    default void seedBuiltinRoles(com.udb.core.authz.services.v1.SeedBuiltinRolesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.SeedBuiltinRolesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSeedBuiltinRolesMethod(), responseObserver);
    }

    /**
     */
    default void migrateLegacyPolicies(com.udb.core.authz.services.v1.MigrateLegacyPoliciesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.MigrateLegacyPoliciesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMigrateLegacyPoliciesMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service AuthzService.
   * <pre>
   * UDB-owned authorization service for RBAC, ABAC, ReBAC, tenant/project
   * domains, and audit-ready access decisions.
   * </pre>
   */
  public static abstract class AuthzServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return AuthzServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service AuthzService.
   * <pre>
   * UDB-owned authorization service for RBAC, ABAC, ReBAC, tenant/project
   * domains, and audit-ready access decisions.
   * </pre>
   */
  public static final class AuthzServiceStub
      extends io.grpc.stub.AbstractAsyncStub<AuthzServiceStub> {
    private AuthzServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AuthzServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AuthzServiceStub(channel, callOptions);
    }

    /**
     */
    public void authorize(com.udb.core.authz.services.v1.AuthzRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.AuthzResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAuthorizeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void checkAccess(com.udb.core.authz.services.v1.CheckAccessRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.CheckAccessResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCheckAccessMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createRole(com.udb.core.authz.services.v1.CreateRoleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.CreateRoleResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateRoleMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void assignRole(com.udb.core.authz.services.v1.AssignRoleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.AssignRoleResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAssignRoleMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createPolicyRule(com.udb.core.authz.services.v1.CreatePolicyRuleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.CreatePolicyRuleResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreatePolicyRuleMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listUserPermissions(com.udb.core.authz.services.v1.ListUserPermissionsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListUserPermissionsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListUserPermissionsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listAccessDecisionAudits(com.udb.core.authz.services.v1.ListAccessDecisionAuditsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListAccessDecisionAuditsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListAccessDecisionAuditsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Revoke a role from a user.
     * </pre>
     */
    public void revokeRole(com.udb.core.authz.services.v1.RevokeRoleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.RevokeRoleResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRevokeRoleMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List all role assignments for a user.
     * </pre>
     */
    public void listUserRoles(com.udb.core.authz.services.v1.ListUserRolesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListUserRolesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListUserRolesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get a role by ID.
     * </pre>
     */
    public void getRole(com.udb.core.authz.services.v1.GetRoleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.GetRoleResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetRoleMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List all roles for a domain/tenant.
     * </pre>
     */
    public void listRoles(com.udb.core.authz.services.v1.ListRolesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListRolesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListRolesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Batch check multiple permissions at once.
     * </pre>
     */
    public void batchCheckPermissions(com.udb.core.authz.services.v1.BatchCheckPermissionsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.BatchCheckPermissionsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getBatchCheckPermissionsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Update a role's name, description, or active status.
     * </pre>
     */
    public void updateRole(com.udb.core.authz.services.v1.UpdateRoleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.UpdateRoleResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateRoleMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Delete a role (soft-delete; existing assignments are revoked).
     * </pre>
     */
    public void deleteRole(com.udb.core.authz.services.v1.DeleteRoleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.DeleteRoleResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteRoleMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get a single policy rule by ID.
     * </pre>
     */
    public void getPolicyRule(com.udb.core.authz.services.v1.GetPolicyRuleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.GetPolicyRuleResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetPolicyRuleMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List policy rules with optional domain/subject/object filters.
     * </pre>
     */
    public void listPolicyRules(com.udb.core.authz.services.v1.ListPolicyRulesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListPolicyRulesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListPolicyRulesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Delete a policy rule.
     * </pre>
     */
    public void deletePolicyRule(com.udb.core.authz.services.v1.DeletePolicyRuleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.DeletePolicyRuleResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeletePolicyRuleMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void putRoleBinding(com.udb.core.authz.services.v1.PutRoleBindingRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.AuthMutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPutRoleBindingMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void putRelationship(com.udb.core.authz.services.v1.PutRelationshipRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.AuthMutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPutRelationshipMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void putAuthzPolicy(com.udb.core.authz.services.v1.PutAuthzPolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.AuthMutationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPutAuthzPolicyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void lintAuthzPolicies(com.udb.core.authz.services.v1.LintAuthzPoliciesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.LintAuthzPoliciesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getLintAuthzPoliciesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Stage 2: authorize and, when allowed, mint a short-lived native-access
     * contract (restricted role + scoped DSN + RLS session variables).
     * </pre>
     */
    public void getNativeAccess(com.udb.core.authz.services.v1.NativeAccessRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.NativeAccessResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetNativeAccessMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Stage 2: return a signed policy bundle for local SDK authorization caches.
     * </pre>
     */
    public void getPolicyBundle(com.udb.core.authz.services.v1.PolicyBundleRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyBundleResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetPolicyBundleMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createPolicyDraft(com.udb.core.authz.services.v1.CreatePolicyDraftRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyDraftResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreatePolicyDraftMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void updatePolicyDraft(com.udb.core.authz.services.v1.UpdatePolicyDraftRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyDraftResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdatePolicyDraftMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void diffPolicyDraft(com.udb.core.authz.services.v1.DiffPolicyDraftRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.DiffPolicyDraftResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDiffPolicyDraftMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void submitPolicyDraft(com.udb.core.authz.services.v1.SubmitPolicyDraftRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyDraftResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSubmitPolicyDraftMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void approvePolicyDraft(com.udb.core.authz.services.v1.ApprovePolicyDraftRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyApprovalResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getApprovePolicyDraftMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void rejectPolicyDraft(com.udb.core.authz.services.v1.RejectPolicyDraftRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyApprovalResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRejectPolicyDraftMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void activatePolicyVersion(com.udb.core.authz.services.v1.ActivatePolicyVersionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ActivationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getActivatePolicyVersionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void rollbackPolicyVersion(com.udb.core.authz.services.v1.RollbackPolicyVersionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ActivationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRollbackPolicyVersionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Activate a policy version to a canary scope (subset of the fleet) before
     * fleet-wide. A metric-based evaluator then auto-rolls back on breach.
     * </pre>
     */
    public void activateCanary(com.udb.core.authz.services.v1.ActivateCanaryRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.CanaryResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getActivateCanaryMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Promote a baked, within-threshold canary to fleet-wide enforcement.
     * </pre>
     */
    public void promoteCanary(com.udb.core.authz.services.v1.PromoteCanaryRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.CanaryResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPromoteCanaryMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Read a canary's current state + promote-eligibility.
     * </pre>
     */
    public void getCanaryStatus(com.udb.core.authz.services.v1.GetCanaryStatusRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.GetCanaryStatusResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetCanaryStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listPolicyVersions(com.udb.core.authz.services.v1.ListPolicyVersionsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListPolicyVersionsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListPolicyVersionsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void simulatePolicy(com.udb.core.authz.services.v1.SimulatePolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.SimulatePolicyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSimulatePolicyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void explainPolicy(com.udb.core.authz.services.v1.ExplainPolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ExplainPolicyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExplainPolicyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getAuthzRevision(com.udb.core.authz.services.v1.GetAuthzRevisionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.GetAuthzRevisionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetAuthzRevisionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void invalidatePolicyBundles(com.udb.core.authz.services.v1.InvalidatePolicyBundlesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.InvalidatePolicyBundlesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getInvalidatePolicyBundlesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void seedBuiltinRoles(com.udb.core.authz.services.v1.SeedBuiltinRolesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.SeedBuiltinRolesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSeedBuiltinRolesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void migrateLegacyPolicies(com.udb.core.authz.services.v1.MigrateLegacyPoliciesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.MigrateLegacyPoliciesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMigrateLegacyPoliciesMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service AuthzService.
   * <pre>
   * UDB-owned authorization service for RBAC, ABAC, ReBAC, tenant/project
   * domains, and audit-ready access decisions.
   * </pre>
   */
  public static final class AuthzServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<AuthzServiceBlockingV2Stub> {
    private AuthzServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AuthzServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AuthzServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    public com.udb.core.authz.services.v1.AuthzResponse authorize(com.udb.core.authz.services.v1.AuthzRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getAuthorizeMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.CheckAccessResponse checkAccess(com.udb.core.authz.services.v1.CheckAccessRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCheckAccessMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.CreateRoleResponse createRole(com.udb.core.authz.services.v1.CreateRoleRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreateRoleMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.AssignRoleResponse assignRole(com.udb.core.authz.services.v1.AssignRoleRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getAssignRoleMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.CreatePolicyRuleResponse createPolicyRule(com.udb.core.authz.services.v1.CreatePolicyRuleRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreatePolicyRuleMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.ListUserPermissionsResponse listUserPermissions(com.udb.core.authz.services.v1.ListUserPermissionsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListUserPermissionsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.ListAccessDecisionAuditsResponse listAccessDecisionAudits(com.udb.core.authz.services.v1.ListAccessDecisionAuditsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListAccessDecisionAuditsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Revoke a role from a user.
     * </pre>
     */
    public com.udb.core.authz.services.v1.RevokeRoleResponse revokeRole(com.udb.core.authz.services.v1.RevokeRoleRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRevokeRoleMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List all role assignments for a user.
     * </pre>
     */
    public com.udb.core.authz.services.v1.ListUserRolesResponse listUserRoles(com.udb.core.authz.services.v1.ListUserRolesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListUserRolesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a role by ID.
     * </pre>
     */
    public com.udb.core.authz.services.v1.GetRoleResponse getRole(com.udb.core.authz.services.v1.GetRoleRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetRoleMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List all roles for a domain/tenant.
     * </pre>
     */
    public com.udb.core.authz.services.v1.ListRolesResponse listRoles(com.udb.core.authz.services.v1.ListRolesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListRolesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Batch check multiple permissions at once.
     * </pre>
     */
    public com.udb.core.authz.services.v1.BatchCheckPermissionsResponse batchCheckPermissions(com.udb.core.authz.services.v1.BatchCheckPermissionsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getBatchCheckPermissionsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Update a role's name, description, or active status.
     * </pre>
     */
    public com.udb.core.authz.services.v1.UpdateRoleResponse updateRole(com.udb.core.authz.services.v1.UpdateRoleRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getUpdateRoleMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a role (soft-delete; existing assignments are revoked).
     * </pre>
     */
    public com.udb.core.authz.services.v1.DeleteRoleResponse deleteRole(com.udb.core.authz.services.v1.DeleteRoleRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeleteRoleMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a single policy rule by ID.
     * </pre>
     */
    public com.udb.core.authz.services.v1.GetPolicyRuleResponse getPolicyRule(com.udb.core.authz.services.v1.GetPolicyRuleRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetPolicyRuleMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List policy rules with optional domain/subject/object filters.
     * </pre>
     */
    public com.udb.core.authz.services.v1.ListPolicyRulesResponse listPolicyRules(com.udb.core.authz.services.v1.ListPolicyRulesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListPolicyRulesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a policy rule.
     * </pre>
     */
    public com.udb.core.authz.services.v1.DeletePolicyRuleResponse deletePolicyRule(com.udb.core.authz.services.v1.DeletePolicyRuleRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDeletePolicyRuleMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.AuthMutationResponse putRoleBinding(com.udb.core.authz.services.v1.PutRoleBindingRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPutRoleBindingMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.AuthMutationResponse putRelationship(com.udb.core.authz.services.v1.PutRelationshipRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPutRelationshipMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.AuthMutationResponse putAuthzPolicy(com.udb.core.authz.services.v1.PutAuthzPolicyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPutAuthzPolicyMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.LintAuthzPoliciesResponse lintAuthzPolicies(com.udb.core.authz.services.v1.LintAuthzPoliciesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getLintAuthzPoliciesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Stage 2: authorize and, when allowed, mint a short-lived native-access
     * contract (restricted role + scoped DSN + RLS session variables).
     * </pre>
     */
    public com.udb.core.authz.services.v1.NativeAccessResponse getNativeAccess(com.udb.core.authz.services.v1.NativeAccessRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetNativeAccessMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Stage 2: return a signed policy bundle for local SDK authorization caches.
     * </pre>
     */
    public com.udb.core.authz.services.v1.PolicyBundleResponse getPolicyBundle(com.udb.core.authz.services.v1.PolicyBundleRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetPolicyBundleMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.PolicyDraftResponse createPolicyDraft(com.udb.core.authz.services.v1.CreatePolicyDraftRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreatePolicyDraftMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.PolicyDraftResponse updatePolicyDraft(com.udb.core.authz.services.v1.UpdatePolicyDraftRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getUpdatePolicyDraftMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.DiffPolicyDraftResponse diffPolicyDraft(com.udb.core.authz.services.v1.DiffPolicyDraftRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getDiffPolicyDraftMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.PolicyDraftResponse submitPolicyDraft(com.udb.core.authz.services.v1.SubmitPolicyDraftRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSubmitPolicyDraftMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.PolicyApprovalResponse approvePolicyDraft(com.udb.core.authz.services.v1.ApprovePolicyDraftRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getApprovePolicyDraftMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.PolicyApprovalResponse rejectPolicyDraft(com.udb.core.authz.services.v1.RejectPolicyDraftRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRejectPolicyDraftMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.ActivationResponse activatePolicyVersion(com.udb.core.authz.services.v1.ActivatePolicyVersionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getActivatePolicyVersionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.ActivationResponse rollbackPolicyVersion(com.udb.core.authz.services.v1.RollbackPolicyVersionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRollbackPolicyVersionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Activate a policy version to a canary scope (subset of the fleet) before
     * fleet-wide. A metric-based evaluator then auto-rolls back on breach.
     * </pre>
     */
    public com.udb.core.authz.services.v1.CanaryResponse activateCanary(com.udb.core.authz.services.v1.ActivateCanaryRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getActivateCanaryMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Promote a baked, within-threshold canary to fleet-wide enforcement.
     * </pre>
     */
    public com.udb.core.authz.services.v1.CanaryResponse promoteCanary(com.udb.core.authz.services.v1.PromoteCanaryRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPromoteCanaryMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Read a canary's current state + promote-eligibility.
     * </pre>
     */
    public com.udb.core.authz.services.v1.GetCanaryStatusResponse getCanaryStatus(com.udb.core.authz.services.v1.GetCanaryStatusRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetCanaryStatusMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.ListPolicyVersionsResponse listPolicyVersions(com.udb.core.authz.services.v1.ListPolicyVersionsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListPolicyVersionsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.SimulatePolicyResponse simulatePolicy(com.udb.core.authz.services.v1.SimulatePolicyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSimulatePolicyMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.ExplainPolicyResponse explainPolicy(com.udb.core.authz.services.v1.ExplainPolicyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getExplainPolicyMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.GetAuthzRevisionResponse getAuthzRevision(com.udb.core.authz.services.v1.GetAuthzRevisionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetAuthzRevisionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.InvalidatePolicyBundlesResponse invalidatePolicyBundles(com.udb.core.authz.services.v1.InvalidatePolicyBundlesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getInvalidatePolicyBundlesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.SeedBuiltinRolesResponse seedBuiltinRoles(com.udb.core.authz.services.v1.SeedBuiltinRolesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSeedBuiltinRolesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.MigrateLegacyPoliciesResponse migrateLegacyPolicies(com.udb.core.authz.services.v1.MigrateLegacyPoliciesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getMigrateLegacyPoliciesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service AuthzService.
   * <pre>
   * UDB-owned authorization service for RBAC, ABAC, ReBAC, tenant/project
   * domains, and audit-ready access decisions.
   * </pre>
   */
  public static final class AuthzServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<AuthzServiceBlockingStub> {
    private AuthzServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AuthzServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AuthzServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.udb.core.authz.services.v1.AuthzResponse authorize(com.udb.core.authz.services.v1.AuthzRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAuthorizeMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.CheckAccessResponse checkAccess(com.udb.core.authz.services.v1.CheckAccessRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCheckAccessMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.CreateRoleResponse createRole(com.udb.core.authz.services.v1.CreateRoleRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateRoleMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.AssignRoleResponse assignRole(com.udb.core.authz.services.v1.AssignRoleRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAssignRoleMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.CreatePolicyRuleResponse createPolicyRule(com.udb.core.authz.services.v1.CreatePolicyRuleRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreatePolicyRuleMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.ListUserPermissionsResponse listUserPermissions(com.udb.core.authz.services.v1.ListUserPermissionsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListUserPermissionsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.ListAccessDecisionAuditsResponse listAccessDecisionAudits(com.udb.core.authz.services.v1.ListAccessDecisionAuditsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListAccessDecisionAuditsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Revoke a role from a user.
     * </pre>
     */
    public com.udb.core.authz.services.v1.RevokeRoleResponse revokeRole(com.udb.core.authz.services.v1.RevokeRoleRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRevokeRoleMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List all role assignments for a user.
     * </pre>
     */
    public com.udb.core.authz.services.v1.ListUserRolesResponse listUserRoles(com.udb.core.authz.services.v1.ListUserRolesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListUserRolesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a role by ID.
     * </pre>
     */
    public com.udb.core.authz.services.v1.GetRoleResponse getRole(com.udb.core.authz.services.v1.GetRoleRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetRoleMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List all roles for a domain/tenant.
     * </pre>
     */
    public com.udb.core.authz.services.v1.ListRolesResponse listRoles(com.udb.core.authz.services.v1.ListRolesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListRolesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Batch check multiple permissions at once.
     * </pre>
     */
    public com.udb.core.authz.services.v1.BatchCheckPermissionsResponse batchCheckPermissions(com.udb.core.authz.services.v1.BatchCheckPermissionsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getBatchCheckPermissionsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Update a role's name, description, or active status.
     * </pre>
     */
    public com.udb.core.authz.services.v1.UpdateRoleResponse updateRole(com.udb.core.authz.services.v1.UpdateRoleRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateRoleMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a role (soft-delete; existing assignments are revoked).
     * </pre>
     */
    public com.udb.core.authz.services.v1.DeleteRoleResponse deleteRole(com.udb.core.authz.services.v1.DeleteRoleRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteRoleMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a single policy rule by ID.
     * </pre>
     */
    public com.udb.core.authz.services.v1.GetPolicyRuleResponse getPolicyRule(com.udb.core.authz.services.v1.GetPolicyRuleRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetPolicyRuleMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List policy rules with optional domain/subject/object filters.
     * </pre>
     */
    public com.udb.core.authz.services.v1.ListPolicyRulesResponse listPolicyRules(com.udb.core.authz.services.v1.ListPolicyRulesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListPolicyRulesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a policy rule.
     * </pre>
     */
    public com.udb.core.authz.services.v1.DeletePolicyRuleResponse deletePolicyRule(com.udb.core.authz.services.v1.DeletePolicyRuleRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeletePolicyRuleMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.AuthMutationResponse putRoleBinding(com.udb.core.authz.services.v1.PutRoleBindingRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPutRoleBindingMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.AuthMutationResponse putRelationship(com.udb.core.authz.services.v1.PutRelationshipRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPutRelationshipMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.AuthMutationResponse putAuthzPolicy(com.udb.core.authz.services.v1.PutAuthzPolicyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPutAuthzPolicyMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.LintAuthzPoliciesResponse lintAuthzPolicies(com.udb.core.authz.services.v1.LintAuthzPoliciesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getLintAuthzPoliciesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Stage 2: authorize and, when allowed, mint a short-lived native-access
     * contract (restricted role + scoped DSN + RLS session variables).
     * </pre>
     */
    public com.udb.core.authz.services.v1.NativeAccessResponse getNativeAccess(com.udb.core.authz.services.v1.NativeAccessRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetNativeAccessMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Stage 2: return a signed policy bundle for local SDK authorization caches.
     * </pre>
     */
    public com.udb.core.authz.services.v1.PolicyBundleResponse getPolicyBundle(com.udb.core.authz.services.v1.PolicyBundleRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetPolicyBundleMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.PolicyDraftResponse createPolicyDraft(com.udb.core.authz.services.v1.CreatePolicyDraftRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreatePolicyDraftMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.PolicyDraftResponse updatePolicyDraft(com.udb.core.authz.services.v1.UpdatePolicyDraftRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdatePolicyDraftMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.DiffPolicyDraftResponse diffPolicyDraft(com.udb.core.authz.services.v1.DiffPolicyDraftRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDiffPolicyDraftMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.PolicyDraftResponse submitPolicyDraft(com.udb.core.authz.services.v1.SubmitPolicyDraftRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSubmitPolicyDraftMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.PolicyApprovalResponse approvePolicyDraft(com.udb.core.authz.services.v1.ApprovePolicyDraftRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getApprovePolicyDraftMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.PolicyApprovalResponse rejectPolicyDraft(com.udb.core.authz.services.v1.RejectPolicyDraftRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRejectPolicyDraftMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.ActivationResponse activatePolicyVersion(com.udb.core.authz.services.v1.ActivatePolicyVersionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getActivatePolicyVersionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.ActivationResponse rollbackPolicyVersion(com.udb.core.authz.services.v1.RollbackPolicyVersionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRollbackPolicyVersionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Activate a policy version to a canary scope (subset of the fleet) before
     * fleet-wide. A metric-based evaluator then auto-rolls back on breach.
     * </pre>
     */
    public com.udb.core.authz.services.v1.CanaryResponse activateCanary(com.udb.core.authz.services.v1.ActivateCanaryRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getActivateCanaryMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Promote a baked, within-threshold canary to fleet-wide enforcement.
     * </pre>
     */
    public com.udb.core.authz.services.v1.CanaryResponse promoteCanary(com.udb.core.authz.services.v1.PromoteCanaryRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPromoteCanaryMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Read a canary's current state + promote-eligibility.
     * </pre>
     */
    public com.udb.core.authz.services.v1.GetCanaryStatusResponse getCanaryStatus(com.udb.core.authz.services.v1.GetCanaryStatusRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetCanaryStatusMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.ListPolicyVersionsResponse listPolicyVersions(com.udb.core.authz.services.v1.ListPolicyVersionsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListPolicyVersionsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.SimulatePolicyResponse simulatePolicy(com.udb.core.authz.services.v1.SimulatePolicyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSimulatePolicyMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.ExplainPolicyResponse explainPolicy(com.udb.core.authz.services.v1.ExplainPolicyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExplainPolicyMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.GetAuthzRevisionResponse getAuthzRevision(com.udb.core.authz.services.v1.GetAuthzRevisionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetAuthzRevisionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.InvalidatePolicyBundlesResponse invalidatePolicyBundles(com.udb.core.authz.services.v1.InvalidatePolicyBundlesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getInvalidatePolicyBundlesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.SeedBuiltinRolesResponse seedBuiltinRoles(com.udb.core.authz.services.v1.SeedBuiltinRolesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSeedBuiltinRolesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authz.services.v1.MigrateLegacyPoliciesResponse migrateLegacyPolicies(com.udb.core.authz.services.v1.MigrateLegacyPoliciesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMigrateLegacyPoliciesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service AuthzService.
   * <pre>
   * UDB-owned authorization service for RBAC, ABAC, ReBAC, tenant/project
   * domains, and audit-ready access decisions.
   * </pre>
   */
  public static final class AuthzServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<AuthzServiceFutureStub> {
    private AuthzServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AuthzServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AuthzServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.AuthzResponse> authorize(
        com.udb.core.authz.services.v1.AuthzRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAuthorizeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.CheckAccessResponse> checkAccess(
        com.udb.core.authz.services.v1.CheckAccessRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCheckAccessMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.CreateRoleResponse> createRole(
        com.udb.core.authz.services.v1.CreateRoleRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateRoleMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.AssignRoleResponse> assignRole(
        com.udb.core.authz.services.v1.AssignRoleRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAssignRoleMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.CreatePolicyRuleResponse> createPolicyRule(
        com.udb.core.authz.services.v1.CreatePolicyRuleRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreatePolicyRuleMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.ListUserPermissionsResponse> listUserPermissions(
        com.udb.core.authz.services.v1.ListUserPermissionsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListUserPermissionsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.ListAccessDecisionAuditsResponse> listAccessDecisionAudits(
        com.udb.core.authz.services.v1.ListAccessDecisionAuditsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListAccessDecisionAuditsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Revoke a role from a user.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.RevokeRoleResponse> revokeRole(
        com.udb.core.authz.services.v1.RevokeRoleRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRevokeRoleMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List all role assignments for a user.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.ListUserRolesResponse> listUserRoles(
        com.udb.core.authz.services.v1.ListUserRolesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListUserRolesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get a role by ID.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.GetRoleResponse> getRole(
        com.udb.core.authz.services.v1.GetRoleRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetRoleMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List all roles for a domain/tenant.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.ListRolesResponse> listRoles(
        com.udb.core.authz.services.v1.ListRolesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListRolesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Batch check multiple permissions at once.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.BatchCheckPermissionsResponse> batchCheckPermissions(
        com.udb.core.authz.services.v1.BatchCheckPermissionsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getBatchCheckPermissionsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Update a role's name, description, or active status.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.UpdateRoleResponse> updateRole(
        com.udb.core.authz.services.v1.UpdateRoleRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateRoleMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Delete a role (soft-delete; existing assignments are revoked).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.DeleteRoleResponse> deleteRole(
        com.udb.core.authz.services.v1.DeleteRoleRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteRoleMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get a single policy rule by ID.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.GetPolicyRuleResponse> getPolicyRule(
        com.udb.core.authz.services.v1.GetPolicyRuleRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetPolicyRuleMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List policy rules with optional domain/subject/object filters.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.ListPolicyRulesResponse> listPolicyRules(
        com.udb.core.authz.services.v1.ListPolicyRulesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListPolicyRulesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Delete a policy rule.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.DeletePolicyRuleResponse> deletePolicyRule(
        com.udb.core.authz.services.v1.DeletePolicyRuleRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeletePolicyRuleMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.AuthMutationResponse> putRoleBinding(
        com.udb.core.authz.services.v1.PutRoleBindingRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPutRoleBindingMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.AuthMutationResponse> putRelationship(
        com.udb.core.authz.services.v1.PutRelationshipRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPutRelationshipMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.AuthMutationResponse> putAuthzPolicy(
        com.udb.core.authz.services.v1.PutAuthzPolicyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPutAuthzPolicyMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.LintAuthzPoliciesResponse> lintAuthzPolicies(
        com.udb.core.authz.services.v1.LintAuthzPoliciesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getLintAuthzPoliciesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Stage 2: authorize and, when allowed, mint a short-lived native-access
     * contract (restricted role + scoped DSN + RLS session variables).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.NativeAccessResponse> getNativeAccess(
        com.udb.core.authz.services.v1.NativeAccessRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetNativeAccessMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Stage 2: return a signed policy bundle for local SDK authorization caches.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.PolicyBundleResponse> getPolicyBundle(
        com.udb.core.authz.services.v1.PolicyBundleRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetPolicyBundleMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.PolicyDraftResponse> createPolicyDraft(
        com.udb.core.authz.services.v1.CreatePolicyDraftRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreatePolicyDraftMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.PolicyDraftResponse> updatePolicyDraft(
        com.udb.core.authz.services.v1.UpdatePolicyDraftRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdatePolicyDraftMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.DiffPolicyDraftResponse> diffPolicyDraft(
        com.udb.core.authz.services.v1.DiffPolicyDraftRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDiffPolicyDraftMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.PolicyDraftResponse> submitPolicyDraft(
        com.udb.core.authz.services.v1.SubmitPolicyDraftRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSubmitPolicyDraftMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.PolicyApprovalResponse> approvePolicyDraft(
        com.udb.core.authz.services.v1.ApprovePolicyDraftRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getApprovePolicyDraftMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.PolicyApprovalResponse> rejectPolicyDraft(
        com.udb.core.authz.services.v1.RejectPolicyDraftRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRejectPolicyDraftMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.ActivationResponse> activatePolicyVersion(
        com.udb.core.authz.services.v1.ActivatePolicyVersionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getActivatePolicyVersionMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.ActivationResponse> rollbackPolicyVersion(
        com.udb.core.authz.services.v1.RollbackPolicyVersionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRollbackPolicyVersionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Activate a policy version to a canary scope (subset of the fleet) before
     * fleet-wide. A metric-based evaluator then auto-rolls back on breach.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.CanaryResponse> activateCanary(
        com.udb.core.authz.services.v1.ActivateCanaryRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getActivateCanaryMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Promote a baked, within-threshold canary to fleet-wide enforcement.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.CanaryResponse> promoteCanary(
        com.udb.core.authz.services.v1.PromoteCanaryRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPromoteCanaryMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Read a canary's current state + promote-eligibility.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.GetCanaryStatusResponse> getCanaryStatus(
        com.udb.core.authz.services.v1.GetCanaryStatusRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetCanaryStatusMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.ListPolicyVersionsResponse> listPolicyVersions(
        com.udb.core.authz.services.v1.ListPolicyVersionsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListPolicyVersionsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.SimulatePolicyResponse> simulatePolicy(
        com.udb.core.authz.services.v1.SimulatePolicyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSimulatePolicyMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.ExplainPolicyResponse> explainPolicy(
        com.udb.core.authz.services.v1.ExplainPolicyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExplainPolicyMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.GetAuthzRevisionResponse> getAuthzRevision(
        com.udb.core.authz.services.v1.GetAuthzRevisionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetAuthzRevisionMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.InvalidatePolicyBundlesResponse> invalidatePolicyBundles(
        com.udb.core.authz.services.v1.InvalidatePolicyBundlesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getInvalidatePolicyBundlesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.SeedBuiltinRolesResponse> seedBuiltinRoles(
        com.udb.core.authz.services.v1.SeedBuiltinRolesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSeedBuiltinRolesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authz.services.v1.MigrateLegacyPoliciesResponse> migrateLegacyPolicies(
        com.udb.core.authz.services.v1.MigrateLegacyPoliciesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMigrateLegacyPoliciesMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_AUTHORIZE = 0;
  private static final int METHODID_CHECK_ACCESS = 1;
  private static final int METHODID_CREATE_ROLE = 2;
  private static final int METHODID_ASSIGN_ROLE = 3;
  private static final int METHODID_CREATE_POLICY_RULE = 4;
  private static final int METHODID_LIST_USER_PERMISSIONS = 5;
  private static final int METHODID_LIST_ACCESS_DECISION_AUDITS = 6;
  private static final int METHODID_REVOKE_ROLE = 7;
  private static final int METHODID_LIST_USER_ROLES = 8;
  private static final int METHODID_GET_ROLE = 9;
  private static final int METHODID_LIST_ROLES = 10;
  private static final int METHODID_BATCH_CHECK_PERMISSIONS = 11;
  private static final int METHODID_UPDATE_ROLE = 12;
  private static final int METHODID_DELETE_ROLE = 13;
  private static final int METHODID_GET_POLICY_RULE = 14;
  private static final int METHODID_LIST_POLICY_RULES = 15;
  private static final int METHODID_DELETE_POLICY_RULE = 16;
  private static final int METHODID_PUT_ROLE_BINDING = 17;
  private static final int METHODID_PUT_RELATIONSHIP = 18;
  private static final int METHODID_PUT_AUTHZ_POLICY = 19;
  private static final int METHODID_LINT_AUTHZ_POLICIES = 20;
  private static final int METHODID_GET_NATIVE_ACCESS = 21;
  private static final int METHODID_GET_POLICY_BUNDLE = 22;
  private static final int METHODID_CREATE_POLICY_DRAFT = 23;
  private static final int METHODID_UPDATE_POLICY_DRAFT = 24;
  private static final int METHODID_DIFF_POLICY_DRAFT = 25;
  private static final int METHODID_SUBMIT_POLICY_DRAFT = 26;
  private static final int METHODID_APPROVE_POLICY_DRAFT = 27;
  private static final int METHODID_REJECT_POLICY_DRAFT = 28;
  private static final int METHODID_ACTIVATE_POLICY_VERSION = 29;
  private static final int METHODID_ROLLBACK_POLICY_VERSION = 30;
  private static final int METHODID_ACTIVATE_CANARY = 31;
  private static final int METHODID_PROMOTE_CANARY = 32;
  private static final int METHODID_GET_CANARY_STATUS = 33;
  private static final int METHODID_LIST_POLICY_VERSIONS = 34;
  private static final int METHODID_SIMULATE_POLICY = 35;
  private static final int METHODID_EXPLAIN_POLICY = 36;
  private static final int METHODID_GET_AUTHZ_REVISION = 37;
  private static final int METHODID_INVALIDATE_POLICY_BUNDLES = 38;
  private static final int METHODID_SEED_BUILTIN_ROLES = 39;
  private static final int METHODID_MIGRATE_LEGACY_POLICIES = 40;

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
        case METHODID_AUTHORIZE:
          serviceImpl.authorize((com.udb.core.authz.services.v1.AuthzRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.AuthzResponse>) responseObserver);
          break;
        case METHODID_CHECK_ACCESS:
          serviceImpl.checkAccess((com.udb.core.authz.services.v1.CheckAccessRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.CheckAccessResponse>) responseObserver);
          break;
        case METHODID_CREATE_ROLE:
          serviceImpl.createRole((com.udb.core.authz.services.v1.CreateRoleRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.CreateRoleResponse>) responseObserver);
          break;
        case METHODID_ASSIGN_ROLE:
          serviceImpl.assignRole((com.udb.core.authz.services.v1.AssignRoleRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.AssignRoleResponse>) responseObserver);
          break;
        case METHODID_CREATE_POLICY_RULE:
          serviceImpl.createPolicyRule((com.udb.core.authz.services.v1.CreatePolicyRuleRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.CreatePolicyRuleResponse>) responseObserver);
          break;
        case METHODID_LIST_USER_PERMISSIONS:
          serviceImpl.listUserPermissions((com.udb.core.authz.services.v1.ListUserPermissionsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListUserPermissionsResponse>) responseObserver);
          break;
        case METHODID_LIST_ACCESS_DECISION_AUDITS:
          serviceImpl.listAccessDecisionAudits((com.udb.core.authz.services.v1.ListAccessDecisionAuditsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListAccessDecisionAuditsResponse>) responseObserver);
          break;
        case METHODID_REVOKE_ROLE:
          serviceImpl.revokeRole((com.udb.core.authz.services.v1.RevokeRoleRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.RevokeRoleResponse>) responseObserver);
          break;
        case METHODID_LIST_USER_ROLES:
          serviceImpl.listUserRoles((com.udb.core.authz.services.v1.ListUserRolesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListUserRolesResponse>) responseObserver);
          break;
        case METHODID_GET_ROLE:
          serviceImpl.getRole((com.udb.core.authz.services.v1.GetRoleRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.GetRoleResponse>) responseObserver);
          break;
        case METHODID_LIST_ROLES:
          serviceImpl.listRoles((com.udb.core.authz.services.v1.ListRolesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListRolesResponse>) responseObserver);
          break;
        case METHODID_BATCH_CHECK_PERMISSIONS:
          serviceImpl.batchCheckPermissions((com.udb.core.authz.services.v1.BatchCheckPermissionsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.BatchCheckPermissionsResponse>) responseObserver);
          break;
        case METHODID_UPDATE_ROLE:
          serviceImpl.updateRole((com.udb.core.authz.services.v1.UpdateRoleRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.UpdateRoleResponse>) responseObserver);
          break;
        case METHODID_DELETE_ROLE:
          serviceImpl.deleteRole((com.udb.core.authz.services.v1.DeleteRoleRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.DeleteRoleResponse>) responseObserver);
          break;
        case METHODID_GET_POLICY_RULE:
          serviceImpl.getPolicyRule((com.udb.core.authz.services.v1.GetPolicyRuleRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.GetPolicyRuleResponse>) responseObserver);
          break;
        case METHODID_LIST_POLICY_RULES:
          serviceImpl.listPolicyRules((com.udb.core.authz.services.v1.ListPolicyRulesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListPolicyRulesResponse>) responseObserver);
          break;
        case METHODID_DELETE_POLICY_RULE:
          serviceImpl.deletePolicyRule((com.udb.core.authz.services.v1.DeletePolicyRuleRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.DeletePolicyRuleResponse>) responseObserver);
          break;
        case METHODID_PUT_ROLE_BINDING:
          serviceImpl.putRoleBinding((com.udb.core.authz.services.v1.PutRoleBindingRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.AuthMutationResponse>) responseObserver);
          break;
        case METHODID_PUT_RELATIONSHIP:
          serviceImpl.putRelationship((com.udb.core.authz.services.v1.PutRelationshipRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.AuthMutationResponse>) responseObserver);
          break;
        case METHODID_PUT_AUTHZ_POLICY:
          serviceImpl.putAuthzPolicy((com.udb.core.authz.services.v1.PutAuthzPolicyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.AuthMutationResponse>) responseObserver);
          break;
        case METHODID_LINT_AUTHZ_POLICIES:
          serviceImpl.lintAuthzPolicies((com.udb.core.authz.services.v1.LintAuthzPoliciesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.LintAuthzPoliciesResponse>) responseObserver);
          break;
        case METHODID_GET_NATIVE_ACCESS:
          serviceImpl.getNativeAccess((com.udb.core.authz.services.v1.NativeAccessRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.NativeAccessResponse>) responseObserver);
          break;
        case METHODID_GET_POLICY_BUNDLE:
          serviceImpl.getPolicyBundle((com.udb.core.authz.services.v1.PolicyBundleRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyBundleResponse>) responseObserver);
          break;
        case METHODID_CREATE_POLICY_DRAFT:
          serviceImpl.createPolicyDraft((com.udb.core.authz.services.v1.CreatePolicyDraftRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyDraftResponse>) responseObserver);
          break;
        case METHODID_UPDATE_POLICY_DRAFT:
          serviceImpl.updatePolicyDraft((com.udb.core.authz.services.v1.UpdatePolicyDraftRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyDraftResponse>) responseObserver);
          break;
        case METHODID_DIFF_POLICY_DRAFT:
          serviceImpl.diffPolicyDraft((com.udb.core.authz.services.v1.DiffPolicyDraftRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.DiffPolicyDraftResponse>) responseObserver);
          break;
        case METHODID_SUBMIT_POLICY_DRAFT:
          serviceImpl.submitPolicyDraft((com.udb.core.authz.services.v1.SubmitPolicyDraftRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyDraftResponse>) responseObserver);
          break;
        case METHODID_APPROVE_POLICY_DRAFT:
          serviceImpl.approvePolicyDraft((com.udb.core.authz.services.v1.ApprovePolicyDraftRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyApprovalResponse>) responseObserver);
          break;
        case METHODID_REJECT_POLICY_DRAFT:
          serviceImpl.rejectPolicyDraft((com.udb.core.authz.services.v1.RejectPolicyDraftRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.PolicyApprovalResponse>) responseObserver);
          break;
        case METHODID_ACTIVATE_POLICY_VERSION:
          serviceImpl.activatePolicyVersion((com.udb.core.authz.services.v1.ActivatePolicyVersionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ActivationResponse>) responseObserver);
          break;
        case METHODID_ROLLBACK_POLICY_VERSION:
          serviceImpl.rollbackPolicyVersion((com.udb.core.authz.services.v1.RollbackPolicyVersionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ActivationResponse>) responseObserver);
          break;
        case METHODID_ACTIVATE_CANARY:
          serviceImpl.activateCanary((com.udb.core.authz.services.v1.ActivateCanaryRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.CanaryResponse>) responseObserver);
          break;
        case METHODID_PROMOTE_CANARY:
          serviceImpl.promoteCanary((com.udb.core.authz.services.v1.PromoteCanaryRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.CanaryResponse>) responseObserver);
          break;
        case METHODID_GET_CANARY_STATUS:
          serviceImpl.getCanaryStatus((com.udb.core.authz.services.v1.GetCanaryStatusRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.GetCanaryStatusResponse>) responseObserver);
          break;
        case METHODID_LIST_POLICY_VERSIONS:
          serviceImpl.listPolicyVersions((com.udb.core.authz.services.v1.ListPolicyVersionsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ListPolicyVersionsResponse>) responseObserver);
          break;
        case METHODID_SIMULATE_POLICY:
          serviceImpl.simulatePolicy((com.udb.core.authz.services.v1.SimulatePolicyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.SimulatePolicyResponse>) responseObserver);
          break;
        case METHODID_EXPLAIN_POLICY:
          serviceImpl.explainPolicy((com.udb.core.authz.services.v1.ExplainPolicyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.ExplainPolicyResponse>) responseObserver);
          break;
        case METHODID_GET_AUTHZ_REVISION:
          serviceImpl.getAuthzRevision((com.udb.core.authz.services.v1.GetAuthzRevisionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.GetAuthzRevisionResponse>) responseObserver);
          break;
        case METHODID_INVALIDATE_POLICY_BUNDLES:
          serviceImpl.invalidatePolicyBundles((com.udb.core.authz.services.v1.InvalidatePolicyBundlesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.InvalidatePolicyBundlesResponse>) responseObserver);
          break;
        case METHODID_SEED_BUILTIN_ROLES:
          serviceImpl.seedBuiltinRoles((com.udb.core.authz.services.v1.SeedBuiltinRolesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.SeedBuiltinRolesResponse>) responseObserver);
          break;
        case METHODID_MIGRATE_LEGACY_POLICIES:
          serviceImpl.migrateLegacyPolicies((com.udb.core.authz.services.v1.MigrateLegacyPoliciesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authz.services.v1.MigrateLegacyPoliciesResponse>) responseObserver);
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
          getAuthorizeMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.AuthzRequest,
              com.udb.core.authz.services.v1.AuthzResponse>(
                service, METHODID_AUTHORIZE)))
        .addMethod(
          getCheckAccessMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.CheckAccessRequest,
              com.udb.core.authz.services.v1.CheckAccessResponse>(
                service, METHODID_CHECK_ACCESS)))
        .addMethod(
          getCreateRoleMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.CreateRoleRequest,
              com.udb.core.authz.services.v1.CreateRoleResponse>(
                service, METHODID_CREATE_ROLE)))
        .addMethod(
          getAssignRoleMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.AssignRoleRequest,
              com.udb.core.authz.services.v1.AssignRoleResponse>(
                service, METHODID_ASSIGN_ROLE)))
        .addMethod(
          getCreatePolicyRuleMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.CreatePolicyRuleRequest,
              com.udb.core.authz.services.v1.CreatePolicyRuleResponse>(
                service, METHODID_CREATE_POLICY_RULE)))
        .addMethod(
          getListUserPermissionsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.ListUserPermissionsRequest,
              com.udb.core.authz.services.v1.ListUserPermissionsResponse>(
                service, METHODID_LIST_USER_PERMISSIONS)))
        .addMethod(
          getListAccessDecisionAuditsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.ListAccessDecisionAuditsRequest,
              com.udb.core.authz.services.v1.ListAccessDecisionAuditsResponse>(
                service, METHODID_LIST_ACCESS_DECISION_AUDITS)))
        .addMethod(
          getRevokeRoleMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.RevokeRoleRequest,
              com.udb.core.authz.services.v1.RevokeRoleResponse>(
                service, METHODID_REVOKE_ROLE)))
        .addMethod(
          getListUserRolesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.ListUserRolesRequest,
              com.udb.core.authz.services.v1.ListUserRolesResponse>(
                service, METHODID_LIST_USER_ROLES)))
        .addMethod(
          getGetRoleMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.GetRoleRequest,
              com.udb.core.authz.services.v1.GetRoleResponse>(
                service, METHODID_GET_ROLE)))
        .addMethod(
          getListRolesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.ListRolesRequest,
              com.udb.core.authz.services.v1.ListRolesResponse>(
                service, METHODID_LIST_ROLES)))
        .addMethod(
          getBatchCheckPermissionsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.BatchCheckPermissionsRequest,
              com.udb.core.authz.services.v1.BatchCheckPermissionsResponse>(
                service, METHODID_BATCH_CHECK_PERMISSIONS)))
        .addMethod(
          getUpdateRoleMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.UpdateRoleRequest,
              com.udb.core.authz.services.v1.UpdateRoleResponse>(
                service, METHODID_UPDATE_ROLE)))
        .addMethod(
          getDeleteRoleMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.DeleteRoleRequest,
              com.udb.core.authz.services.v1.DeleteRoleResponse>(
                service, METHODID_DELETE_ROLE)))
        .addMethod(
          getGetPolicyRuleMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.GetPolicyRuleRequest,
              com.udb.core.authz.services.v1.GetPolicyRuleResponse>(
                service, METHODID_GET_POLICY_RULE)))
        .addMethod(
          getListPolicyRulesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.ListPolicyRulesRequest,
              com.udb.core.authz.services.v1.ListPolicyRulesResponse>(
                service, METHODID_LIST_POLICY_RULES)))
        .addMethod(
          getDeletePolicyRuleMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.DeletePolicyRuleRequest,
              com.udb.core.authz.services.v1.DeletePolicyRuleResponse>(
                service, METHODID_DELETE_POLICY_RULE)))
        .addMethod(
          getPutRoleBindingMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.PutRoleBindingRequest,
              com.udb.core.authz.services.v1.AuthMutationResponse>(
                service, METHODID_PUT_ROLE_BINDING)))
        .addMethod(
          getPutRelationshipMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.PutRelationshipRequest,
              com.udb.core.authz.services.v1.AuthMutationResponse>(
                service, METHODID_PUT_RELATIONSHIP)))
        .addMethod(
          getPutAuthzPolicyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.PutAuthzPolicyRequest,
              com.udb.core.authz.services.v1.AuthMutationResponse>(
                service, METHODID_PUT_AUTHZ_POLICY)))
        .addMethod(
          getLintAuthzPoliciesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.LintAuthzPoliciesRequest,
              com.udb.core.authz.services.v1.LintAuthzPoliciesResponse>(
                service, METHODID_LINT_AUTHZ_POLICIES)))
        .addMethod(
          getGetNativeAccessMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.NativeAccessRequest,
              com.udb.core.authz.services.v1.NativeAccessResponse>(
                service, METHODID_GET_NATIVE_ACCESS)))
        .addMethod(
          getGetPolicyBundleMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.PolicyBundleRequest,
              com.udb.core.authz.services.v1.PolicyBundleResponse>(
                service, METHODID_GET_POLICY_BUNDLE)))
        .addMethod(
          getCreatePolicyDraftMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.CreatePolicyDraftRequest,
              com.udb.core.authz.services.v1.PolicyDraftResponse>(
                service, METHODID_CREATE_POLICY_DRAFT)))
        .addMethod(
          getUpdatePolicyDraftMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.UpdatePolicyDraftRequest,
              com.udb.core.authz.services.v1.PolicyDraftResponse>(
                service, METHODID_UPDATE_POLICY_DRAFT)))
        .addMethod(
          getDiffPolicyDraftMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.DiffPolicyDraftRequest,
              com.udb.core.authz.services.v1.DiffPolicyDraftResponse>(
                service, METHODID_DIFF_POLICY_DRAFT)))
        .addMethod(
          getSubmitPolicyDraftMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.SubmitPolicyDraftRequest,
              com.udb.core.authz.services.v1.PolicyDraftResponse>(
                service, METHODID_SUBMIT_POLICY_DRAFT)))
        .addMethod(
          getApprovePolicyDraftMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.ApprovePolicyDraftRequest,
              com.udb.core.authz.services.v1.PolicyApprovalResponse>(
                service, METHODID_APPROVE_POLICY_DRAFT)))
        .addMethod(
          getRejectPolicyDraftMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.RejectPolicyDraftRequest,
              com.udb.core.authz.services.v1.PolicyApprovalResponse>(
                service, METHODID_REJECT_POLICY_DRAFT)))
        .addMethod(
          getActivatePolicyVersionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.ActivatePolicyVersionRequest,
              com.udb.core.authz.services.v1.ActivationResponse>(
                service, METHODID_ACTIVATE_POLICY_VERSION)))
        .addMethod(
          getRollbackPolicyVersionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.RollbackPolicyVersionRequest,
              com.udb.core.authz.services.v1.ActivationResponse>(
                service, METHODID_ROLLBACK_POLICY_VERSION)))
        .addMethod(
          getActivateCanaryMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.ActivateCanaryRequest,
              com.udb.core.authz.services.v1.CanaryResponse>(
                service, METHODID_ACTIVATE_CANARY)))
        .addMethod(
          getPromoteCanaryMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.PromoteCanaryRequest,
              com.udb.core.authz.services.v1.CanaryResponse>(
                service, METHODID_PROMOTE_CANARY)))
        .addMethod(
          getGetCanaryStatusMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.GetCanaryStatusRequest,
              com.udb.core.authz.services.v1.GetCanaryStatusResponse>(
                service, METHODID_GET_CANARY_STATUS)))
        .addMethod(
          getListPolicyVersionsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.ListPolicyVersionsRequest,
              com.udb.core.authz.services.v1.ListPolicyVersionsResponse>(
                service, METHODID_LIST_POLICY_VERSIONS)))
        .addMethod(
          getSimulatePolicyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.SimulatePolicyRequest,
              com.udb.core.authz.services.v1.SimulatePolicyResponse>(
                service, METHODID_SIMULATE_POLICY)))
        .addMethod(
          getExplainPolicyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.ExplainPolicyRequest,
              com.udb.core.authz.services.v1.ExplainPolicyResponse>(
                service, METHODID_EXPLAIN_POLICY)))
        .addMethod(
          getGetAuthzRevisionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.GetAuthzRevisionRequest,
              com.udb.core.authz.services.v1.GetAuthzRevisionResponse>(
                service, METHODID_GET_AUTHZ_REVISION)))
        .addMethod(
          getInvalidatePolicyBundlesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.InvalidatePolicyBundlesRequest,
              com.udb.core.authz.services.v1.InvalidatePolicyBundlesResponse>(
                service, METHODID_INVALIDATE_POLICY_BUNDLES)))
        .addMethod(
          getSeedBuiltinRolesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.SeedBuiltinRolesRequest,
              com.udb.core.authz.services.v1.SeedBuiltinRolesResponse>(
                service, METHODID_SEED_BUILTIN_ROLES)))
        .addMethod(
          getMigrateLegacyPoliciesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authz.services.v1.MigrateLegacyPoliciesRequest,
              com.udb.core.authz.services.v1.MigrateLegacyPoliciesResponse>(
                service, METHODID_MIGRATE_LEGACY_POLICIES)))
        .build();
  }

  private static abstract class AuthzServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    AuthzServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.authz.services.v1.AuthzServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("AuthzService");
    }
  }

  private static final class AuthzServiceFileDescriptorSupplier
      extends AuthzServiceBaseDescriptorSupplier {
    AuthzServiceFileDescriptorSupplier() {}
  }

  private static final class AuthzServiceMethodDescriptorSupplier
      extends AuthzServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    AuthzServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (AuthzServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new AuthzServiceFileDescriptorSupplier())
              .addMethod(getAuthorizeMethod())
              .addMethod(getCheckAccessMethod())
              .addMethod(getCreateRoleMethod())
              .addMethod(getAssignRoleMethod())
              .addMethod(getCreatePolicyRuleMethod())
              .addMethod(getListUserPermissionsMethod())
              .addMethod(getListAccessDecisionAuditsMethod())
              .addMethod(getRevokeRoleMethod())
              .addMethod(getListUserRolesMethod())
              .addMethod(getGetRoleMethod())
              .addMethod(getListRolesMethod())
              .addMethod(getBatchCheckPermissionsMethod())
              .addMethod(getUpdateRoleMethod())
              .addMethod(getDeleteRoleMethod())
              .addMethod(getGetPolicyRuleMethod())
              .addMethod(getListPolicyRulesMethod())
              .addMethod(getDeletePolicyRuleMethod())
              .addMethod(getPutRoleBindingMethod())
              .addMethod(getPutRelationshipMethod())
              .addMethod(getPutAuthzPolicyMethod())
              .addMethod(getLintAuthzPoliciesMethod())
              .addMethod(getGetNativeAccessMethod())
              .addMethod(getGetPolicyBundleMethod())
              .addMethod(getCreatePolicyDraftMethod())
              .addMethod(getUpdatePolicyDraftMethod())
              .addMethod(getDiffPolicyDraftMethod())
              .addMethod(getSubmitPolicyDraftMethod())
              .addMethod(getApprovePolicyDraftMethod())
              .addMethod(getRejectPolicyDraftMethod())
              .addMethod(getActivatePolicyVersionMethod())
              .addMethod(getRollbackPolicyVersionMethod())
              .addMethod(getActivateCanaryMethod())
              .addMethod(getPromoteCanaryMethod())
              .addMethod(getGetCanaryStatusMethod())
              .addMethod(getListPolicyVersionsMethod())
              .addMethod(getSimulatePolicyMethod())
              .addMethod(getExplainPolicyMethod())
              .addMethod(getGetAuthzRevisionMethod())
              .addMethod(getInvalidatePolicyBundlesMethod())
              .addMethod(getSeedBuiltinRolesMethod())
              .addMethod(getMigrateLegacyPoliciesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
