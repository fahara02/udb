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
              .build();
        }
      }
    }
    return result;
  }
}
