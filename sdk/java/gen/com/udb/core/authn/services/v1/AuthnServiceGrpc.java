package com.udb.core.authn.services.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * ---------------------------------------------------------------------------
 * AuthnService — native and hybrid authentication for UDB-backed projects.
 *
 * HTTP prefix: /v1/auth
 * URL conventions (Rule 07): snake_case paths, :&lt;verb&gt; custom method suffix, kebab-case query params.
 *
 * Auth method routing is policy-driven. Typical deployments use server-side
 * sessions for browser clients, JWT for APIs/desktop/mobile clients, API keys
 * for service integrations, and external OIDC/SAML/JWT proofs for hybrid auth.
 * ---------------------------------------------------------------------------
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class AuthnServiceGrpc {

  private AuthnServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "udb.core.authn.services.v1.AuthnService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.CreateUserRequest,
      com.udb.core.authn.services.v1.CreateUserResponse> getCreateUserMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateUser",
      requestType = com.udb.core.authn.services.v1.CreateUserRequest.class,
      responseType = com.udb.core.authn.services.v1.CreateUserResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.CreateUserRequest,
      com.udb.core.authn.services.v1.CreateUserResponse> getCreateUserMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.CreateUserRequest, com.udb.core.authn.services.v1.CreateUserResponse> getCreateUserMethod;
    if ((getCreateUserMethod = AuthnServiceGrpc.getCreateUserMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getCreateUserMethod = AuthnServiceGrpc.getCreateUserMethod) == null) {
          AuthnServiceGrpc.getCreateUserMethod = getCreateUserMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.CreateUserRequest, com.udb.core.authn.services.v1.CreateUserResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateUser"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.CreateUserRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.CreateUserResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("CreateUser"))
              .build();
        }
      }
    }
    return getCreateUserMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.GetUserRequest,
      com.udb.core.authn.services.v1.GetUserResponse> getGetUserMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetUser",
      requestType = com.udb.core.authn.services.v1.GetUserRequest.class,
      responseType = com.udb.core.authn.services.v1.GetUserResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.GetUserRequest,
      com.udb.core.authn.services.v1.GetUserResponse> getGetUserMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.GetUserRequest, com.udb.core.authn.services.v1.GetUserResponse> getGetUserMethod;
    if ((getGetUserMethod = AuthnServiceGrpc.getGetUserMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getGetUserMethod = AuthnServiceGrpc.getGetUserMethod) == null) {
          AuthnServiceGrpc.getGetUserMethod = getGetUserMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.GetUserRequest, com.udb.core.authn.services.v1.GetUserResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetUser"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.GetUserRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.GetUserResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("GetUser"))
              .build();
        }
      }
    }
    return getGetUserMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ListUsersRequest,
      com.udb.core.authn.services.v1.ListUsersResponse> getListUsersMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListUsers",
      requestType = com.udb.core.authn.services.v1.ListUsersRequest.class,
      responseType = com.udb.core.authn.services.v1.ListUsersResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ListUsersRequest,
      com.udb.core.authn.services.v1.ListUsersResponse> getListUsersMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ListUsersRequest, com.udb.core.authn.services.v1.ListUsersResponse> getListUsersMethod;
    if ((getListUsersMethod = AuthnServiceGrpc.getListUsersMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getListUsersMethod = AuthnServiceGrpc.getListUsersMethod) == null) {
          AuthnServiceGrpc.getListUsersMethod = getListUsersMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.ListUsersRequest, com.udb.core.authn.services.v1.ListUsersResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListUsers"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ListUsersRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ListUsersResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("ListUsers"))
              .build();
        }
      }
    }
    return getListUsersMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.UpdateUserRequest,
      com.udb.core.authn.services.v1.UpdateUserResponse> getUpdateUserMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateUser",
      requestType = com.udb.core.authn.services.v1.UpdateUserRequest.class,
      responseType = com.udb.core.authn.services.v1.UpdateUserResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.UpdateUserRequest,
      com.udb.core.authn.services.v1.UpdateUserResponse> getUpdateUserMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.UpdateUserRequest, com.udb.core.authn.services.v1.UpdateUserResponse> getUpdateUserMethod;
    if ((getUpdateUserMethod = AuthnServiceGrpc.getUpdateUserMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getUpdateUserMethod = AuthnServiceGrpc.getUpdateUserMethod) == null) {
          AuthnServiceGrpc.getUpdateUserMethod = getUpdateUserMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.UpdateUserRequest, com.udb.core.authn.services.v1.UpdateUserResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateUser"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.UpdateUserRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.UpdateUserResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("UpdateUser"))
              .build();
        }
      }
    }
    return getUpdateUserMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ChangeUserStatusRequest,
      com.udb.core.authn.services.v1.ChangeUserStatusResponse> getChangeUserStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ChangeUserStatus",
      requestType = com.udb.core.authn.services.v1.ChangeUserStatusRequest.class,
      responseType = com.udb.core.authn.services.v1.ChangeUserStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ChangeUserStatusRequest,
      com.udb.core.authn.services.v1.ChangeUserStatusResponse> getChangeUserStatusMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ChangeUserStatusRequest, com.udb.core.authn.services.v1.ChangeUserStatusResponse> getChangeUserStatusMethod;
    if ((getChangeUserStatusMethod = AuthnServiceGrpc.getChangeUserStatusMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getChangeUserStatusMethod = AuthnServiceGrpc.getChangeUserStatusMethod) == null) {
          AuthnServiceGrpc.getChangeUserStatusMethod = getChangeUserStatusMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.ChangeUserStatusRequest, com.udb.core.authn.services.v1.ChangeUserStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ChangeUserStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ChangeUserStatusRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ChangeUserStatusResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("ChangeUserStatus"))
              .build();
        }
      }
    }
    return getChangeUserStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.AdminResetPasswordRequest,
      com.udb.core.authn.services.v1.AdminResetPasswordResponse> getAdminResetPasswordMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AdminResetPassword",
      requestType = com.udb.core.authn.services.v1.AdminResetPasswordRequest.class,
      responseType = com.udb.core.authn.services.v1.AdminResetPasswordResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.AdminResetPasswordRequest,
      com.udb.core.authn.services.v1.AdminResetPasswordResponse> getAdminResetPasswordMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.AdminResetPasswordRequest, com.udb.core.authn.services.v1.AdminResetPasswordResponse> getAdminResetPasswordMethod;
    if ((getAdminResetPasswordMethod = AuthnServiceGrpc.getAdminResetPasswordMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getAdminResetPasswordMethod = AuthnServiceGrpc.getAdminResetPasswordMethod) == null) {
          AuthnServiceGrpc.getAdminResetPasswordMethod = getAdminResetPasswordMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.AdminResetPasswordRequest, com.udb.core.authn.services.v1.AdminResetPasswordResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AdminResetPassword"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.AdminResetPasswordRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.AdminResetPasswordResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("AdminResetPassword"))
              .build();
        }
      }
    }
    return getAdminResetPasswordMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.SendOTPRequest,
      com.udb.core.authn.services.v1.SendOTPResponse> getSendOTPMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendOTP",
      requestType = com.udb.core.authn.services.v1.SendOTPRequest.class,
      responseType = com.udb.core.authn.services.v1.SendOTPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.SendOTPRequest,
      com.udb.core.authn.services.v1.SendOTPResponse> getSendOTPMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.SendOTPRequest, com.udb.core.authn.services.v1.SendOTPResponse> getSendOTPMethod;
    if ((getSendOTPMethod = AuthnServiceGrpc.getSendOTPMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getSendOTPMethod = AuthnServiceGrpc.getSendOTPMethod) == null) {
          AuthnServiceGrpc.getSendOTPMethod = getSendOTPMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.SendOTPRequest, com.udb.core.authn.services.v1.SendOTPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendOTP"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.SendOTPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.SendOTPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("SendOTP"))
              .build();
        }
      }
    }
    return getSendOTPMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.VerifyOTPRequest,
      com.udb.core.authn.services.v1.VerifyOTPResponse> getVerifyOTPMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "VerifyOTP",
      requestType = com.udb.core.authn.services.v1.VerifyOTPRequest.class,
      responseType = com.udb.core.authn.services.v1.VerifyOTPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.VerifyOTPRequest,
      com.udb.core.authn.services.v1.VerifyOTPResponse> getVerifyOTPMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.VerifyOTPRequest, com.udb.core.authn.services.v1.VerifyOTPResponse> getVerifyOTPMethod;
    if ((getVerifyOTPMethod = AuthnServiceGrpc.getVerifyOTPMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getVerifyOTPMethod = AuthnServiceGrpc.getVerifyOTPMethod) == null) {
          AuthnServiceGrpc.getVerifyOTPMethod = getVerifyOTPMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.VerifyOTPRequest, com.udb.core.authn.services.v1.VerifyOTPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "VerifyOTP"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.VerifyOTPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.VerifyOTPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("VerifyOTP"))
              .build();
        }
      }
    }
    return getVerifyOTPMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ResendOTPRequest,
      com.udb.core.authn.services.v1.ResendOTPResponse> getResendOTPMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ResendOTP",
      requestType = com.udb.core.authn.services.v1.ResendOTPRequest.class,
      responseType = com.udb.core.authn.services.v1.ResendOTPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ResendOTPRequest,
      com.udb.core.authn.services.v1.ResendOTPResponse> getResendOTPMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ResendOTPRequest, com.udb.core.authn.services.v1.ResendOTPResponse> getResendOTPMethod;
    if ((getResendOTPMethod = AuthnServiceGrpc.getResendOTPMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getResendOTPMethod = AuthnServiceGrpc.getResendOTPMethod) == null) {
          AuthnServiceGrpc.getResendOTPMethod = getResendOTPMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.ResendOTPRequest, com.udb.core.authn.services.v1.ResendOTPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ResendOTP"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ResendOTPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ResendOTPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("ResendOTP"))
              .build();
        }
      }
    }
    return getResendOTPMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.AuthnRequest,
      com.udb.core.authn.services.v1.AuthnResponse> getAuthenticateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Authenticate",
      requestType = com.udb.core.authn.services.v1.AuthnRequest.class,
      responseType = com.udb.core.authn.services.v1.AuthnResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.AuthnRequest,
      com.udb.core.authn.services.v1.AuthnResponse> getAuthenticateMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.AuthnRequest, com.udb.core.authn.services.v1.AuthnResponse> getAuthenticateMethod;
    if ((getAuthenticateMethod = AuthnServiceGrpc.getAuthenticateMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getAuthenticateMethod = AuthnServiceGrpc.getAuthenticateMethod) == null) {
          AuthnServiceGrpc.getAuthenticateMethod = getAuthenticateMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.AuthnRequest, com.udb.core.authn.services.v1.AuthnResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Authenticate"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.AuthnRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.AuthnResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("Authenticate"))
              .build();
        }
      }
    }
    return getAuthenticateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.LoginRequest,
      com.udb.core.authn.services.v1.LoginResponse> getLoginMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Login",
      requestType = com.udb.core.authn.services.v1.LoginRequest.class,
      responseType = com.udb.core.authn.services.v1.LoginResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.LoginRequest,
      com.udb.core.authn.services.v1.LoginResponse> getLoginMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.LoginRequest, com.udb.core.authn.services.v1.LoginResponse> getLoginMethod;
    if ((getLoginMethod = AuthnServiceGrpc.getLoginMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getLoginMethod = AuthnServiceGrpc.getLoginMethod) == null) {
          AuthnServiceGrpc.getLoginMethod = getLoginMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.LoginRequest, com.udb.core.authn.services.v1.LoginResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Login"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.LoginRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.LoginResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("Login"))
              .build();
        }
      }
    }
    return getLoginMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.RefreshTokenRequest,
      com.udb.core.authn.services.v1.RefreshTokenResponse> getRefreshTokenMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RefreshToken",
      requestType = com.udb.core.authn.services.v1.RefreshTokenRequest.class,
      responseType = com.udb.core.authn.services.v1.RefreshTokenResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.RefreshTokenRequest,
      com.udb.core.authn.services.v1.RefreshTokenResponse> getRefreshTokenMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.RefreshTokenRequest, com.udb.core.authn.services.v1.RefreshTokenResponse> getRefreshTokenMethod;
    if ((getRefreshTokenMethod = AuthnServiceGrpc.getRefreshTokenMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getRefreshTokenMethod = AuthnServiceGrpc.getRefreshTokenMethod) == null) {
          AuthnServiceGrpc.getRefreshTokenMethod = getRefreshTokenMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.RefreshTokenRequest, com.udb.core.authn.services.v1.RefreshTokenResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RefreshToken"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.RefreshTokenRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.RefreshTokenResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("RefreshToken"))
              .build();
        }
      }
    }
    return getRefreshTokenMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.LogoutRequest,
      com.udb.core.authn.services.v1.LogoutResponse> getLogoutMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Logout",
      requestType = com.udb.core.authn.services.v1.LogoutRequest.class,
      responseType = com.udb.core.authn.services.v1.LogoutResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.LogoutRequest,
      com.udb.core.authn.services.v1.LogoutResponse> getLogoutMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.LogoutRequest, com.udb.core.authn.services.v1.LogoutResponse> getLogoutMethod;
    if ((getLogoutMethod = AuthnServiceGrpc.getLogoutMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getLogoutMethod = AuthnServiceGrpc.getLogoutMethod) == null) {
          AuthnServiceGrpc.getLogoutMethod = getLogoutMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.LogoutRequest, com.udb.core.authn.services.v1.LogoutResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Logout"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.LogoutRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.LogoutResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("Logout"))
              .build();
        }
      }
    }
    return getLogoutMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ChangePasswordRequest,
      com.udb.core.authn.services.v1.ChangePasswordResponse> getChangePasswordMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ChangePassword",
      requestType = com.udb.core.authn.services.v1.ChangePasswordRequest.class,
      responseType = com.udb.core.authn.services.v1.ChangePasswordResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ChangePasswordRequest,
      com.udb.core.authn.services.v1.ChangePasswordResponse> getChangePasswordMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ChangePasswordRequest, com.udb.core.authn.services.v1.ChangePasswordResponse> getChangePasswordMethod;
    if ((getChangePasswordMethod = AuthnServiceGrpc.getChangePasswordMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getChangePasswordMethod = AuthnServiceGrpc.getChangePasswordMethod) == null) {
          AuthnServiceGrpc.getChangePasswordMethod = getChangePasswordMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.ChangePasswordRequest, com.udb.core.authn.services.v1.ChangePasswordResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ChangePassword"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ChangePasswordRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ChangePasswordResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("ChangePassword"))
              .build();
        }
      }
    }
    return getChangePasswordMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ValidateTokenRequest,
      com.udb.core.authn.services.v1.ValidateTokenResponse> getValidateTokenMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ValidateToken",
      requestType = com.udb.core.authn.services.v1.ValidateTokenRequest.class,
      responseType = com.udb.core.authn.services.v1.ValidateTokenResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ValidateTokenRequest,
      com.udb.core.authn.services.v1.ValidateTokenResponse> getValidateTokenMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ValidateTokenRequest, com.udb.core.authn.services.v1.ValidateTokenResponse> getValidateTokenMethod;
    if ((getValidateTokenMethod = AuthnServiceGrpc.getValidateTokenMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getValidateTokenMethod = AuthnServiceGrpc.getValidateTokenMethod) == null) {
          AuthnServiceGrpc.getValidateTokenMethod = getValidateTokenMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.ValidateTokenRequest, com.udb.core.authn.services.v1.ValidateTokenResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ValidateToken"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ValidateTokenRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ValidateTokenResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("ValidateToken"))
              .build();
        }
      }
    }
    return getValidateTokenMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.CreateSessionRequest,
      com.udb.core.authn.services.v1.CreateSessionResponse> getCreateSessionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateSession",
      requestType = com.udb.core.authn.services.v1.CreateSessionRequest.class,
      responseType = com.udb.core.authn.services.v1.CreateSessionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.CreateSessionRequest,
      com.udb.core.authn.services.v1.CreateSessionResponse> getCreateSessionMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.CreateSessionRequest, com.udb.core.authn.services.v1.CreateSessionResponse> getCreateSessionMethod;
    if ((getCreateSessionMethod = AuthnServiceGrpc.getCreateSessionMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getCreateSessionMethod = AuthnServiceGrpc.getCreateSessionMethod) == null) {
          AuthnServiceGrpc.getCreateSessionMethod = getCreateSessionMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.CreateSessionRequest, com.udb.core.authn.services.v1.CreateSessionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateSession"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.CreateSessionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.CreateSessionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("CreateSession"))
              .build();
        }
      }
    }
    return getCreateSessionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.RefreshSessionRequest,
      com.udb.core.authn.services.v1.RefreshSessionResponse> getRefreshSessionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RefreshSession",
      requestType = com.udb.core.authn.services.v1.RefreshSessionRequest.class,
      responseType = com.udb.core.authn.services.v1.RefreshSessionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.RefreshSessionRequest,
      com.udb.core.authn.services.v1.RefreshSessionResponse> getRefreshSessionMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.RefreshSessionRequest, com.udb.core.authn.services.v1.RefreshSessionResponse> getRefreshSessionMethod;
    if ((getRefreshSessionMethod = AuthnServiceGrpc.getRefreshSessionMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getRefreshSessionMethod = AuthnServiceGrpc.getRefreshSessionMethod) == null) {
          AuthnServiceGrpc.getRefreshSessionMethod = getRefreshSessionMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.RefreshSessionRequest, com.udb.core.authn.services.v1.RefreshSessionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RefreshSession"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.RefreshSessionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.RefreshSessionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("RefreshSession"))
              .build();
        }
      }
    }
    return getRefreshSessionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.GetSessionRequest,
      com.udb.core.authn.services.v1.GetSessionResponse> getGetSessionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetSession",
      requestType = com.udb.core.authn.services.v1.GetSessionRequest.class,
      responseType = com.udb.core.authn.services.v1.GetSessionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.GetSessionRequest,
      com.udb.core.authn.services.v1.GetSessionResponse> getGetSessionMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.GetSessionRequest, com.udb.core.authn.services.v1.GetSessionResponse> getGetSessionMethod;
    if ((getGetSessionMethod = AuthnServiceGrpc.getGetSessionMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getGetSessionMethod = AuthnServiceGrpc.getGetSessionMethod) == null) {
          AuthnServiceGrpc.getGetSessionMethod = getGetSessionMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.GetSessionRequest, com.udb.core.authn.services.v1.GetSessionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetSession"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.GetSessionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.GetSessionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("GetSession"))
              .build();
        }
      }
    }
    return getGetSessionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ListSessionsRequest,
      com.udb.core.authn.services.v1.ListSessionsResponse> getListSessionsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListSessions",
      requestType = com.udb.core.authn.services.v1.ListSessionsRequest.class,
      responseType = com.udb.core.authn.services.v1.ListSessionsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ListSessionsRequest,
      com.udb.core.authn.services.v1.ListSessionsResponse> getListSessionsMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ListSessionsRequest, com.udb.core.authn.services.v1.ListSessionsResponse> getListSessionsMethod;
    if ((getListSessionsMethod = AuthnServiceGrpc.getListSessionsMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getListSessionsMethod = AuthnServiceGrpc.getListSessionsMethod) == null) {
          AuthnServiceGrpc.getListSessionsMethod = getListSessionsMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.ListSessionsRequest, com.udb.core.authn.services.v1.ListSessionsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListSessions"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ListSessionsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ListSessionsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("ListSessions"))
              .build();
        }
      }
    }
    return getListSessionsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.RevokeSessionRequest,
      com.udb.core.authn.services.v1.RevokeSessionResponse> getRevokeSessionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RevokeSession",
      requestType = com.udb.core.authn.services.v1.RevokeSessionRequest.class,
      responseType = com.udb.core.authn.services.v1.RevokeSessionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.RevokeSessionRequest,
      com.udb.core.authn.services.v1.RevokeSessionResponse> getRevokeSessionMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.RevokeSessionRequest, com.udb.core.authn.services.v1.RevokeSessionResponse> getRevokeSessionMethod;
    if ((getRevokeSessionMethod = AuthnServiceGrpc.getRevokeSessionMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getRevokeSessionMethod = AuthnServiceGrpc.getRevokeSessionMethod) == null) {
          AuthnServiceGrpc.getRevokeSessionMethod = getRevokeSessionMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.RevokeSessionRequest, com.udb.core.authn.services.v1.RevokeSessionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RevokeSession"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.RevokeSessionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.RevokeSessionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("RevokeSession"))
              .build();
        }
      }
    }
    return getRevokeSessionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ValidateCSRFRequest,
      com.udb.core.authn.services.v1.ValidateCSRFResponse> getValidateCSRFMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ValidateCSRF",
      requestType = com.udb.core.authn.services.v1.ValidateCSRFRequest.class,
      responseType = com.udb.core.authn.services.v1.ValidateCSRFResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ValidateCSRFRequest,
      com.udb.core.authn.services.v1.ValidateCSRFResponse> getValidateCSRFMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ValidateCSRFRequest, com.udb.core.authn.services.v1.ValidateCSRFResponse> getValidateCSRFMethod;
    if ((getValidateCSRFMethod = AuthnServiceGrpc.getValidateCSRFMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getValidateCSRFMethod = AuthnServiceGrpc.getValidateCSRFMethod) == null) {
          AuthnServiceGrpc.getValidateCSRFMethod = getValidateCSRFMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.ValidateCSRFRequest, com.udb.core.authn.services.v1.ValidateCSRFResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ValidateCSRF"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ValidateCSRFRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ValidateCSRFResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("ValidateCSRF"))
              .build();
        }
      }
    }
    return getValidateCSRFMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.EnrollMFARequest,
      com.udb.core.authn.services.v1.EnrollMFAResponse> getEnrollMFAMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "EnrollMFA",
      requestType = com.udb.core.authn.services.v1.EnrollMFARequest.class,
      responseType = com.udb.core.authn.services.v1.EnrollMFAResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.EnrollMFARequest,
      com.udb.core.authn.services.v1.EnrollMFAResponse> getEnrollMFAMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.EnrollMFARequest, com.udb.core.authn.services.v1.EnrollMFAResponse> getEnrollMFAMethod;
    if ((getEnrollMFAMethod = AuthnServiceGrpc.getEnrollMFAMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getEnrollMFAMethod = AuthnServiceGrpc.getEnrollMFAMethod) == null) {
          AuthnServiceGrpc.getEnrollMFAMethod = getEnrollMFAMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.EnrollMFARequest, com.udb.core.authn.services.v1.EnrollMFAResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "EnrollMFA"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.EnrollMFARequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.EnrollMFAResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("EnrollMFA"))
              .build();
        }
      }
    }
    return getEnrollMFAMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ConfirmMFAEnrollmentRequest,
      com.udb.core.authn.services.v1.ConfirmMFAEnrollmentResponse> getConfirmMFAEnrollmentMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ConfirmMFAEnrollment",
      requestType = com.udb.core.authn.services.v1.ConfirmMFAEnrollmentRequest.class,
      responseType = com.udb.core.authn.services.v1.ConfirmMFAEnrollmentResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ConfirmMFAEnrollmentRequest,
      com.udb.core.authn.services.v1.ConfirmMFAEnrollmentResponse> getConfirmMFAEnrollmentMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ConfirmMFAEnrollmentRequest, com.udb.core.authn.services.v1.ConfirmMFAEnrollmentResponse> getConfirmMFAEnrollmentMethod;
    if ((getConfirmMFAEnrollmentMethod = AuthnServiceGrpc.getConfirmMFAEnrollmentMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getConfirmMFAEnrollmentMethod = AuthnServiceGrpc.getConfirmMFAEnrollmentMethod) == null) {
          AuthnServiceGrpc.getConfirmMFAEnrollmentMethod = getConfirmMFAEnrollmentMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.ConfirmMFAEnrollmentRequest, com.udb.core.authn.services.v1.ConfirmMFAEnrollmentResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ConfirmMFAEnrollment"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ConfirmMFAEnrollmentRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ConfirmMFAEnrollmentResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("ConfirmMFAEnrollment"))
              .build();
        }
      }
    }
    return getConfirmMFAEnrollmentMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.GenerateRecoveryCodesRequest,
      com.udb.core.authn.services.v1.GenerateRecoveryCodesResponse> getGenerateRecoveryCodesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GenerateRecoveryCodes",
      requestType = com.udb.core.authn.services.v1.GenerateRecoveryCodesRequest.class,
      responseType = com.udb.core.authn.services.v1.GenerateRecoveryCodesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.GenerateRecoveryCodesRequest,
      com.udb.core.authn.services.v1.GenerateRecoveryCodesResponse> getGenerateRecoveryCodesMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.GenerateRecoveryCodesRequest, com.udb.core.authn.services.v1.GenerateRecoveryCodesResponse> getGenerateRecoveryCodesMethod;
    if ((getGenerateRecoveryCodesMethod = AuthnServiceGrpc.getGenerateRecoveryCodesMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getGenerateRecoveryCodesMethod = AuthnServiceGrpc.getGenerateRecoveryCodesMethod) == null) {
          AuthnServiceGrpc.getGenerateRecoveryCodesMethod = getGenerateRecoveryCodesMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.GenerateRecoveryCodesRequest, com.udb.core.authn.services.v1.GenerateRecoveryCodesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GenerateRecoveryCodes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.GenerateRecoveryCodesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.GenerateRecoveryCodesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("GenerateRecoveryCodes"))
              .build();
        }
      }
    }
    return getGenerateRecoveryCodesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.PutMfaPolicyRequest,
      com.udb.core.authn.services.v1.PutMfaPolicyResponse> getPutMfaPolicyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PutMfaPolicy",
      requestType = com.udb.core.authn.services.v1.PutMfaPolicyRequest.class,
      responseType = com.udb.core.authn.services.v1.PutMfaPolicyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.PutMfaPolicyRequest,
      com.udb.core.authn.services.v1.PutMfaPolicyResponse> getPutMfaPolicyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.PutMfaPolicyRequest, com.udb.core.authn.services.v1.PutMfaPolicyResponse> getPutMfaPolicyMethod;
    if ((getPutMfaPolicyMethod = AuthnServiceGrpc.getPutMfaPolicyMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getPutMfaPolicyMethod = AuthnServiceGrpc.getPutMfaPolicyMethod) == null) {
          AuthnServiceGrpc.getPutMfaPolicyMethod = getPutMfaPolicyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.PutMfaPolicyRequest, com.udb.core.authn.services.v1.PutMfaPolicyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PutMfaPolicy"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.PutMfaPolicyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.PutMfaPolicyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("PutMfaPolicy"))
              .build();
        }
      }
    }
    return getPutMfaPolicyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.GetMfaPolicyRequest,
      com.udb.core.authn.services.v1.GetMfaPolicyResponse> getGetMfaPolicyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetMfaPolicy",
      requestType = com.udb.core.authn.services.v1.GetMfaPolicyRequest.class,
      responseType = com.udb.core.authn.services.v1.GetMfaPolicyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.GetMfaPolicyRequest,
      com.udb.core.authn.services.v1.GetMfaPolicyResponse> getGetMfaPolicyMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.GetMfaPolicyRequest, com.udb.core.authn.services.v1.GetMfaPolicyResponse> getGetMfaPolicyMethod;
    if ((getGetMfaPolicyMethod = AuthnServiceGrpc.getGetMfaPolicyMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getGetMfaPolicyMethod = AuthnServiceGrpc.getGetMfaPolicyMethod) == null) {
          AuthnServiceGrpc.getGetMfaPolicyMethod = getGetMfaPolicyMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.GetMfaPolicyRequest, com.udb.core.authn.services.v1.GetMfaPolicyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetMfaPolicy"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.GetMfaPolicyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.GetMfaPolicyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("GetMfaPolicy"))
              .build();
        }
      }
    }
    return getGetMfaPolicyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ForgotPasswordRequest,
      com.udb.core.authn.services.v1.ForgotPasswordResponse> getForgotPasswordMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ForgotPassword",
      requestType = com.udb.core.authn.services.v1.ForgotPasswordRequest.class,
      responseType = com.udb.core.authn.services.v1.ForgotPasswordResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ForgotPasswordRequest,
      com.udb.core.authn.services.v1.ForgotPasswordResponse> getForgotPasswordMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ForgotPasswordRequest, com.udb.core.authn.services.v1.ForgotPasswordResponse> getForgotPasswordMethod;
    if ((getForgotPasswordMethod = AuthnServiceGrpc.getForgotPasswordMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getForgotPasswordMethod = AuthnServiceGrpc.getForgotPasswordMethod) == null) {
          AuthnServiceGrpc.getForgotPasswordMethod = getForgotPasswordMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.ForgotPasswordRequest, com.udb.core.authn.services.v1.ForgotPasswordResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ForgotPassword"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ForgotPasswordRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ForgotPasswordResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("ForgotPassword"))
              .build();
        }
      }
    }
    return getForgotPasswordMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ResetPasswordRequest,
      com.udb.core.authn.services.v1.ResetPasswordResponse> getResetPasswordMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ResetPassword",
      requestType = com.udb.core.authn.services.v1.ResetPasswordRequest.class,
      responseType = com.udb.core.authn.services.v1.ResetPasswordResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ResetPasswordRequest,
      com.udb.core.authn.services.v1.ResetPasswordResponse> getResetPasswordMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.ResetPasswordRequest, com.udb.core.authn.services.v1.ResetPasswordResponse> getResetPasswordMethod;
    if ((getResetPasswordMethod = AuthnServiceGrpc.getResetPasswordMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getResetPasswordMethod = AuthnServiceGrpc.getResetPasswordMethod) == null) {
          AuthnServiceGrpc.getResetPasswordMethod = getResetPasswordMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.ResetPasswordRequest, com.udb.core.authn.services.v1.ResetPasswordResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ResetPassword"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ResetPasswordRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.ResetPasswordResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("ResetPassword"))
              .build();
        }
      }
    }
    return getResetPasswordMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.IntrospectTokenRequest,
      com.udb.core.authn.services.v1.IntrospectTokenResponse> getIntrospectTokenMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "IntrospectToken",
      requestType = com.udb.core.authn.services.v1.IntrospectTokenRequest.class,
      responseType = com.udb.core.authn.services.v1.IntrospectTokenResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.IntrospectTokenRequest,
      com.udb.core.authn.services.v1.IntrospectTokenResponse> getIntrospectTokenMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.IntrospectTokenRequest, com.udb.core.authn.services.v1.IntrospectTokenResponse> getIntrospectTokenMethod;
    if ((getIntrospectTokenMethod = AuthnServiceGrpc.getIntrospectTokenMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getIntrospectTokenMethod = AuthnServiceGrpc.getIntrospectTokenMethod) == null) {
          AuthnServiceGrpc.getIntrospectTokenMethod = getIntrospectTokenMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.IntrospectTokenRequest, com.udb.core.authn.services.v1.IntrospectTokenResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "IntrospectToken"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.IntrospectTokenRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.IntrospectTokenResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("IntrospectToken"))
              .build();
        }
      }
    }
    return getIntrospectTokenMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.SendPhoneVerificationRequest,
      com.udb.core.authn.services.v1.SendPhoneVerificationResponse> getSendPhoneVerificationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendPhoneVerification",
      requestType = com.udb.core.authn.services.v1.SendPhoneVerificationRequest.class,
      responseType = com.udb.core.authn.services.v1.SendPhoneVerificationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.SendPhoneVerificationRequest,
      com.udb.core.authn.services.v1.SendPhoneVerificationResponse> getSendPhoneVerificationMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.SendPhoneVerificationRequest, com.udb.core.authn.services.v1.SendPhoneVerificationResponse> getSendPhoneVerificationMethod;
    if ((getSendPhoneVerificationMethod = AuthnServiceGrpc.getSendPhoneVerificationMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getSendPhoneVerificationMethod = AuthnServiceGrpc.getSendPhoneVerificationMethod) == null) {
          AuthnServiceGrpc.getSendPhoneVerificationMethod = getSendPhoneVerificationMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.SendPhoneVerificationRequest, com.udb.core.authn.services.v1.SendPhoneVerificationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendPhoneVerification"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.SendPhoneVerificationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.SendPhoneVerificationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("SendPhoneVerification"))
              .build();
        }
      }
    }
    return getSendPhoneVerificationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.GetJwksRequest,
      com.udb.core.authn.services.v1.GetJwksResponse> getGetJwksMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetJwks",
      requestType = com.udb.core.authn.services.v1.GetJwksRequest.class,
      responseType = com.udb.core.authn.services.v1.GetJwksResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.GetJwksRequest,
      com.udb.core.authn.services.v1.GetJwksResponse> getGetJwksMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.GetJwksRequest, com.udb.core.authn.services.v1.GetJwksResponse> getGetJwksMethod;
    if ((getGetJwksMethod = AuthnServiceGrpc.getGetJwksMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getGetJwksMethod = AuthnServiceGrpc.getGetJwksMethod) == null) {
          AuthnServiceGrpc.getGetJwksMethod = getGetJwksMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.GetJwksRequest, com.udb.core.authn.services.v1.GetJwksResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetJwks"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.GetJwksRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.GetJwksResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("GetJwks"))
              .build();
        }
      }
    }
    return getGetJwksMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.StartWebAuthnRegistrationRequest,
      com.udb.core.authn.services.v1.StartWebAuthnRegistrationResponse> getStartWebAuthnRegistrationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StartWebAuthnRegistration",
      requestType = com.udb.core.authn.services.v1.StartWebAuthnRegistrationRequest.class,
      responseType = com.udb.core.authn.services.v1.StartWebAuthnRegistrationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.StartWebAuthnRegistrationRequest,
      com.udb.core.authn.services.v1.StartWebAuthnRegistrationResponse> getStartWebAuthnRegistrationMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.StartWebAuthnRegistrationRequest, com.udb.core.authn.services.v1.StartWebAuthnRegistrationResponse> getStartWebAuthnRegistrationMethod;
    if ((getStartWebAuthnRegistrationMethod = AuthnServiceGrpc.getStartWebAuthnRegistrationMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getStartWebAuthnRegistrationMethod = AuthnServiceGrpc.getStartWebAuthnRegistrationMethod) == null) {
          AuthnServiceGrpc.getStartWebAuthnRegistrationMethod = getStartWebAuthnRegistrationMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.StartWebAuthnRegistrationRequest, com.udb.core.authn.services.v1.StartWebAuthnRegistrationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StartWebAuthnRegistration"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.StartWebAuthnRegistrationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.StartWebAuthnRegistrationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("StartWebAuthnRegistration"))
              .build();
        }
      }
    }
    return getStartWebAuthnRegistrationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.FinishWebAuthnRegistrationRequest,
      com.udb.core.authn.services.v1.FinishWebAuthnRegistrationResponse> getFinishWebAuthnRegistrationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "FinishWebAuthnRegistration",
      requestType = com.udb.core.authn.services.v1.FinishWebAuthnRegistrationRequest.class,
      responseType = com.udb.core.authn.services.v1.FinishWebAuthnRegistrationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.FinishWebAuthnRegistrationRequest,
      com.udb.core.authn.services.v1.FinishWebAuthnRegistrationResponse> getFinishWebAuthnRegistrationMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.FinishWebAuthnRegistrationRequest, com.udb.core.authn.services.v1.FinishWebAuthnRegistrationResponse> getFinishWebAuthnRegistrationMethod;
    if ((getFinishWebAuthnRegistrationMethod = AuthnServiceGrpc.getFinishWebAuthnRegistrationMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getFinishWebAuthnRegistrationMethod = AuthnServiceGrpc.getFinishWebAuthnRegistrationMethod) == null) {
          AuthnServiceGrpc.getFinishWebAuthnRegistrationMethod = getFinishWebAuthnRegistrationMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.FinishWebAuthnRegistrationRequest, com.udb.core.authn.services.v1.FinishWebAuthnRegistrationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "FinishWebAuthnRegistration"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.FinishWebAuthnRegistrationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.FinishWebAuthnRegistrationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("FinishWebAuthnRegistration"))
              .build();
        }
      }
    }
    return getFinishWebAuthnRegistrationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.StartWebAuthnAuthenticationRequest,
      com.udb.core.authn.services.v1.StartWebAuthnAuthenticationResponse> getStartWebAuthnAuthenticationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StartWebAuthnAuthentication",
      requestType = com.udb.core.authn.services.v1.StartWebAuthnAuthenticationRequest.class,
      responseType = com.udb.core.authn.services.v1.StartWebAuthnAuthenticationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.StartWebAuthnAuthenticationRequest,
      com.udb.core.authn.services.v1.StartWebAuthnAuthenticationResponse> getStartWebAuthnAuthenticationMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.StartWebAuthnAuthenticationRequest, com.udb.core.authn.services.v1.StartWebAuthnAuthenticationResponse> getStartWebAuthnAuthenticationMethod;
    if ((getStartWebAuthnAuthenticationMethod = AuthnServiceGrpc.getStartWebAuthnAuthenticationMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getStartWebAuthnAuthenticationMethod = AuthnServiceGrpc.getStartWebAuthnAuthenticationMethod) == null) {
          AuthnServiceGrpc.getStartWebAuthnAuthenticationMethod = getStartWebAuthnAuthenticationMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.StartWebAuthnAuthenticationRequest, com.udb.core.authn.services.v1.StartWebAuthnAuthenticationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StartWebAuthnAuthentication"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.StartWebAuthnAuthenticationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.StartWebAuthnAuthenticationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("StartWebAuthnAuthentication"))
              .build();
        }
      }
    }
    return getStartWebAuthnAuthenticationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationRequest,
      com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationResponse> getFinishWebAuthnAuthenticationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "FinishWebAuthnAuthentication",
      requestType = com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationRequest.class,
      responseType = com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationRequest,
      com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationResponse> getFinishWebAuthnAuthenticationMethod() {
    io.grpc.MethodDescriptor<com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationRequest, com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationResponse> getFinishWebAuthnAuthenticationMethod;
    if ((getFinishWebAuthnAuthenticationMethod = AuthnServiceGrpc.getFinishWebAuthnAuthenticationMethod) == null) {
      synchronized (AuthnServiceGrpc.class) {
        if ((getFinishWebAuthnAuthenticationMethod = AuthnServiceGrpc.getFinishWebAuthnAuthenticationMethod) == null) {
          AuthnServiceGrpc.getFinishWebAuthnAuthenticationMethod = getFinishWebAuthnAuthenticationMethod =
              io.grpc.MethodDescriptor.<com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationRequest, com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "FinishWebAuthnAuthentication"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuthnServiceMethodDescriptorSupplier("FinishWebAuthnAuthentication"))
              .build();
        }
      }
    }
    return getFinishWebAuthnAuthenticationMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static AuthnServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AuthnServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AuthnServiceStub>() {
        @java.lang.Override
        public AuthnServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AuthnServiceStub(channel, callOptions);
        }
      };
    return AuthnServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static AuthnServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AuthnServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AuthnServiceBlockingV2Stub>() {
        @java.lang.Override
        public AuthnServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AuthnServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return AuthnServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static AuthnServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AuthnServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AuthnServiceBlockingStub>() {
        @java.lang.Override
        public AuthnServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AuthnServiceBlockingStub(channel, callOptions);
        }
      };
    return AuthnServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static AuthnServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AuthnServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AuthnServiceFutureStub>() {
        @java.lang.Override
        public AuthnServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AuthnServiceFutureStub(channel, callOptions);
        }
      };
    return AuthnServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * ---------------------------------------------------------------------------
   * AuthnService — native and hybrid authentication for UDB-backed projects.
   *
   * HTTP prefix: /v1/auth
   * URL conventions (Rule 07): snake_case paths, :&lt;verb&gt; custom method suffix, kebab-case query params.
   *
   * Auth method routing is policy-driven. Typical deployments use server-side
   * sessions for browser clients, JWT for APIs/desktop/mobile clients, API keys
   * for service integrations, and external OIDC/SAML/JWT proofs for hybrid auth.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * ── User management (admin-only) ─────────────────────────────────────────
     * </pre>
     */
    default void createUser(com.udb.core.authn.services.v1.CreateUserRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.CreateUserResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateUserMethod(), responseObserver);
    }

    /**
     */
    default void getUser(com.udb.core.authn.services.v1.GetUserRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.GetUserResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetUserMethod(), responseObserver);
    }

    /**
     */
    default void listUsers(com.udb.core.authn.services.v1.ListUsersRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ListUsersResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListUsersMethod(), responseObserver);
    }

    /**
     */
    default void updateUser(com.udb.core.authn.services.v1.UpdateUserRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.UpdateUserResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateUserMethod(), responseObserver);
    }

    /**
     */
    default void changeUserStatus(com.udb.core.authn.services.v1.ChangeUserStatusRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ChangeUserStatusResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getChangeUserStatusMethod(), responseObserver);
    }

    /**
     * <pre>
     * Admin-triggered password reset — sends email OTP to complete flow
     * </pre>
     */
    default void adminResetPassword(com.udb.core.authn.services.v1.AdminResetPasswordRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.AdminResetPasswordResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAdminResetPasswordMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── OTP ──────────────────────────────────────────────────────────────────
     * </pre>
     */
    default void sendOTP(com.udb.core.authn.services.v1.SendOTPRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.SendOTPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendOTPMethod(), responseObserver);
    }

    /**
     */
    default void verifyOTP(com.udb.core.authn.services.v1.VerifyOTPRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.VerifyOTPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getVerifyOTPMethod(), responseObserver);
    }

    /**
     */
    default void resendOTP(com.udb.core.authn.services.v1.ResendOTPRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ResendOTPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getResendOTPMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── Authentication ───────────────────────────────────────────────────────
     * </pre>
     */
    default void authenticate(com.udb.core.authn.services.v1.AuthnRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.AuthnResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAuthenticateMethod(), responseObserver);
    }

    /**
     */
    default void login(com.udb.core.authn.services.v1.LoginRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.LoginResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getLoginMethod(), responseObserver);
    }

    /**
     */
    default void refreshToken(com.udb.core.authn.services.v1.RefreshTokenRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.RefreshTokenResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRefreshTokenMethod(), responseObserver);
    }

    /**
     */
    default void logout(com.udb.core.authn.services.v1.LogoutRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.LogoutResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getLogoutMethod(), responseObserver);
    }

    /**
     */
    default void changePassword(com.udb.core.authn.services.v1.ChangePasswordRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ChangePasswordResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getChangePasswordMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── Token validation (called by gateway + per-service interceptors) ───────
     * </pre>
     */
    default void validateToken(com.udb.core.authn.services.v1.ValidateTokenRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ValidateTokenResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getValidateTokenMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── Session management ───────────────────────────────────────────────────
     * </pre>
     */
    default void createSession(com.udb.core.authn.services.v1.CreateSessionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.CreateSessionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateSessionMethod(), responseObserver);
    }

    /**
     */
    default void refreshSession(com.udb.core.authn.services.v1.RefreshSessionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.RefreshSessionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRefreshSessionMethod(), responseObserver);
    }

    /**
     */
    default void getSession(com.udb.core.authn.services.v1.GetSessionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.GetSessionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetSessionMethod(), responseObserver);
    }

    /**
     */
    default void listSessions(com.udb.core.authn.services.v1.ListSessionsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ListSessionsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListSessionsMethod(), responseObserver);
    }

    /**
     */
    default void revokeSession(com.udb.core.authn.services.v1.RevokeSessionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.RevokeSessionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRevokeSessionMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── CSRF (server-side sessions only) ────────────────────────────────────
     * </pre>
     */
    default void validateCSRF(com.udb.core.authn.services.v1.ValidateCSRFRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ValidateCSRFResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getValidateCSRFMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── MFA enrollment ───────────────────────────────────────────────────────
     * Step 1: initiate enrollment — returns TOTP secret / QR URI
     * </pre>
     */
    default void enrollMFA(com.udb.core.authn.services.v1.EnrollMFARequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.EnrollMFAResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEnrollMFAMethod(), responseObserver);
    }

    /**
     * <pre>
     * Step 2: confirm with first TOTP code (or email OTP)
     * </pre>
     */
    default void confirmMFAEnrollment(com.udb.core.authn.services.v1.ConfirmMFAEnrollmentRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ConfirmMFAEnrollmentResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getConfirmMFAEnrollmentMethod(), responseObserver);
    }

    /**
     * <pre>
     * Generate a fresh set of single-use MFA recovery/backup codes (returned once;
     * any prior codes for the user are invalidated).
     * </pre>
     */
    default void generateRecoveryCodes(com.udb.core.authn.services.v1.GenerateRecoveryCodesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.GenerateRecoveryCodesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGenerateRecoveryCodesMethod(), responseObserver);
    }

    /**
     * <pre>
     * Set the per-tenant MFA enforcement policy.
     * </pre>
     */
    default void putMfaPolicy(com.udb.core.authn.services.v1.PutMfaPolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.PutMfaPolicyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPutMfaPolicyMethod(), responseObserver);
    }

    /**
     * <pre>
     * Read the per-tenant MFA enforcement policy.
     * </pre>
     */
    default void getMfaPolicy(com.udb.core.authn.services.v1.GetMfaPolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.GetMfaPolicyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetMfaPolicyMethod(), responseObserver);
    }

    /**
     * <pre>
     * User-initiated password reset: issues a PASSWORD_RESET OTP (delivered to the
     * account's channel). Public — no bearer required.
     * </pre>
     */
    default void forgotPassword(com.udb.core.authn.services.v1.ForgotPasswordRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ForgotPasswordResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getForgotPasswordMethod(), responseObserver);
    }

    /**
     * <pre>
     * Complete a password reset with the OTP from ForgotPassword (no current
     * password required). Public — the OTP is the proof of control.
     * </pre>
     */
    default void resetPassword(com.udb.core.authn.services.v1.ResetPasswordRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ResetPasswordResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getResetPasswordMethod(), responseObserver);
    }

    /**
     * <pre>
     * OAuth2-style token introspection for a UDB-issued JWT.
     * </pre>
     */
    default void introspectToken(com.udb.core.authn.services.v1.IntrospectTokenRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.IntrospectTokenResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getIntrospectTokenMethod(), responseObserver);
    }

    /**
     * <pre>
     * Set the user's phone number and send an SMS verification OTP. Complete with
     * VerifyOTP (the response is verified the same way as email).
     * </pre>
     */
    default void sendPhoneVerification(com.udb.core.authn.services.v1.SendPhoneVerificationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.SendPhoneVerificationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendPhoneVerificationMethod(), responseObserver);
    }

    /**
     * <pre>
     * JSON Web Key Set for verifying UDB-issued JWTs. Public.
     * </pre>
     */
    default void getJwks(com.udb.core.authn.services.v1.GetJwksRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.GetJwksResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetJwksMethod(), responseObserver);
    }

    /**
     * <pre>
     * ── WebAuthn / passkeys ─────────────────────────────────────────────────
     * </pre>
     */
    default void startWebAuthnRegistration(com.udb.core.authn.services.v1.StartWebAuthnRegistrationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.StartWebAuthnRegistrationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStartWebAuthnRegistrationMethod(), responseObserver);
    }

    /**
     */
    default void finishWebAuthnRegistration(com.udb.core.authn.services.v1.FinishWebAuthnRegistrationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.FinishWebAuthnRegistrationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getFinishWebAuthnRegistrationMethod(), responseObserver);
    }

    /**
     */
    default void startWebAuthnAuthentication(com.udb.core.authn.services.v1.StartWebAuthnAuthenticationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.StartWebAuthnAuthenticationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStartWebAuthnAuthenticationMethod(), responseObserver);
    }

    /**
     */
    default void finishWebAuthnAuthentication(com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getFinishWebAuthnAuthenticationMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service AuthnService.
   * <pre>
   * ---------------------------------------------------------------------------
   * AuthnService — native and hybrid authentication for UDB-backed projects.
   *
   * HTTP prefix: /v1/auth
   * URL conventions (Rule 07): snake_case paths, :&lt;verb&gt; custom method suffix, kebab-case query params.
   *
   * Auth method routing is policy-driven. Typical deployments use server-side
   * sessions for browser clients, JWT for APIs/desktop/mobile clients, API keys
   * for service integrations, and external OIDC/SAML/JWT proofs for hybrid auth.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static abstract class AuthnServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return AuthnServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service AuthnService.
   * <pre>
   * ---------------------------------------------------------------------------
   * AuthnService — native and hybrid authentication for UDB-backed projects.
   *
   * HTTP prefix: /v1/auth
   * URL conventions (Rule 07): snake_case paths, :&lt;verb&gt; custom method suffix, kebab-case query params.
   *
   * Auth method routing is policy-driven. Typical deployments use server-side
   * sessions for browser clients, JWT for APIs/desktop/mobile clients, API keys
   * for service integrations, and external OIDC/SAML/JWT proofs for hybrid auth.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static final class AuthnServiceStub
      extends io.grpc.stub.AbstractAsyncStub<AuthnServiceStub> {
    private AuthnServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AuthnServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AuthnServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * ── User management (admin-only) ─────────────────────────────────────────
     * </pre>
     */
    public void createUser(com.udb.core.authn.services.v1.CreateUserRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.CreateUserResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateUserMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getUser(com.udb.core.authn.services.v1.GetUserRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.GetUserResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetUserMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listUsers(com.udb.core.authn.services.v1.ListUsersRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ListUsersResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListUsersMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void updateUser(com.udb.core.authn.services.v1.UpdateUserRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.UpdateUserResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateUserMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void changeUserStatus(com.udb.core.authn.services.v1.ChangeUserStatusRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ChangeUserStatusResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getChangeUserStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Admin-triggered password reset — sends email OTP to complete flow
     * </pre>
     */
    public void adminResetPassword(com.udb.core.authn.services.v1.AdminResetPasswordRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.AdminResetPasswordResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAdminResetPasswordMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── OTP ──────────────────────────────────────────────────────────────────
     * </pre>
     */
    public void sendOTP(com.udb.core.authn.services.v1.SendOTPRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.SendOTPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendOTPMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void verifyOTP(com.udb.core.authn.services.v1.VerifyOTPRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.VerifyOTPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getVerifyOTPMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void resendOTP(com.udb.core.authn.services.v1.ResendOTPRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ResendOTPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getResendOTPMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── Authentication ───────────────────────────────────────────────────────
     * </pre>
     */
    public void authenticate(com.udb.core.authn.services.v1.AuthnRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.AuthnResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAuthenticateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void login(com.udb.core.authn.services.v1.LoginRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.LoginResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getLoginMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void refreshToken(com.udb.core.authn.services.v1.RefreshTokenRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.RefreshTokenResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRefreshTokenMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void logout(com.udb.core.authn.services.v1.LogoutRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.LogoutResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getLogoutMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void changePassword(com.udb.core.authn.services.v1.ChangePasswordRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ChangePasswordResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getChangePasswordMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── Token validation (called by gateway + per-service interceptors) ───────
     * </pre>
     */
    public void validateToken(com.udb.core.authn.services.v1.ValidateTokenRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ValidateTokenResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getValidateTokenMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── Session management ───────────────────────────────────────────────────
     * </pre>
     */
    public void createSession(com.udb.core.authn.services.v1.CreateSessionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.CreateSessionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateSessionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void refreshSession(com.udb.core.authn.services.v1.RefreshSessionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.RefreshSessionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRefreshSessionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getSession(com.udb.core.authn.services.v1.GetSessionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.GetSessionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetSessionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listSessions(com.udb.core.authn.services.v1.ListSessionsRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ListSessionsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListSessionsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void revokeSession(com.udb.core.authn.services.v1.RevokeSessionRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.RevokeSessionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRevokeSessionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── CSRF (server-side sessions only) ────────────────────────────────────
     * </pre>
     */
    public void validateCSRF(com.udb.core.authn.services.v1.ValidateCSRFRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ValidateCSRFResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getValidateCSRFMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── MFA enrollment ───────────────────────────────────────────────────────
     * Step 1: initiate enrollment — returns TOTP secret / QR URI
     * </pre>
     */
    public void enrollMFA(com.udb.core.authn.services.v1.EnrollMFARequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.EnrollMFAResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEnrollMFAMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Step 2: confirm with first TOTP code (or email OTP)
     * </pre>
     */
    public void confirmMFAEnrollment(com.udb.core.authn.services.v1.ConfirmMFAEnrollmentRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ConfirmMFAEnrollmentResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getConfirmMFAEnrollmentMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Generate a fresh set of single-use MFA recovery/backup codes (returned once;
     * any prior codes for the user are invalidated).
     * </pre>
     */
    public void generateRecoveryCodes(com.udb.core.authn.services.v1.GenerateRecoveryCodesRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.GenerateRecoveryCodesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGenerateRecoveryCodesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Set the per-tenant MFA enforcement policy.
     * </pre>
     */
    public void putMfaPolicy(com.udb.core.authn.services.v1.PutMfaPolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.PutMfaPolicyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPutMfaPolicyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Read the per-tenant MFA enforcement policy.
     * </pre>
     */
    public void getMfaPolicy(com.udb.core.authn.services.v1.GetMfaPolicyRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.GetMfaPolicyResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetMfaPolicyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * User-initiated password reset: issues a PASSWORD_RESET OTP (delivered to the
     * account's channel). Public — no bearer required.
     * </pre>
     */
    public void forgotPassword(com.udb.core.authn.services.v1.ForgotPasswordRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ForgotPasswordResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getForgotPasswordMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Complete a password reset with the OTP from ForgotPassword (no current
     * password required). Public — the OTP is the proof of control.
     * </pre>
     */
    public void resetPassword(com.udb.core.authn.services.v1.ResetPasswordRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ResetPasswordResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getResetPasswordMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * OAuth2-style token introspection for a UDB-issued JWT.
     * </pre>
     */
    public void introspectToken(com.udb.core.authn.services.v1.IntrospectTokenRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.IntrospectTokenResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getIntrospectTokenMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Set the user's phone number and send an SMS verification OTP. Complete with
     * VerifyOTP (the response is verified the same way as email).
     * </pre>
     */
    public void sendPhoneVerification(com.udb.core.authn.services.v1.SendPhoneVerificationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.SendPhoneVerificationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendPhoneVerificationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * JSON Web Key Set for verifying UDB-issued JWTs. Public.
     * </pre>
     */
    public void getJwks(com.udb.core.authn.services.v1.GetJwksRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.GetJwksResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetJwksMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ── WebAuthn / passkeys ─────────────────────────────────────────────────
     * </pre>
     */
    public void startWebAuthnRegistration(com.udb.core.authn.services.v1.StartWebAuthnRegistrationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.StartWebAuthnRegistrationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStartWebAuthnRegistrationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void finishWebAuthnRegistration(com.udb.core.authn.services.v1.FinishWebAuthnRegistrationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.FinishWebAuthnRegistrationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getFinishWebAuthnRegistrationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void startWebAuthnAuthentication(com.udb.core.authn.services.v1.StartWebAuthnAuthenticationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.StartWebAuthnAuthenticationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStartWebAuthnAuthenticationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void finishWebAuthnAuthentication(com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationRequest request,
        io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getFinishWebAuthnAuthenticationMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service AuthnService.
   * <pre>
   * ---------------------------------------------------------------------------
   * AuthnService — native and hybrid authentication for UDB-backed projects.
   *
   * HTTP prefix: /v1/auth
   * URL conventions (Rule 07): snake_case paths, :&lt;verb&gt; custom method suffix, kebab-case query params.
   *
   * Auth method routing is policy-driven. Typical deployments use server-side
   * sessions for browser clients, JWT for APIs/desktop/mobile clients, API keys
   * for service integrations, and external OIDC/SAML/JWT proofs for hybrid auth.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static final class AuthnServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<AuthnServiceBlockingV2Stub> {
    private AuthnServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AuthnServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AuthnServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     * <pre>
     * ── User management (admin-only) ─────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.authn.services.v1.CreateUserResponse createUser(com.udb.core.authn.services.v1.CreateUserRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreateUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.GetUserResponse getUser(com.udb.core.authn.services.v1.GetUserRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.ListUsersResponse listUsers(com.udb.core.authn.services.v1.ListUsersRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListUsersMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.UpdateUserResponse updateUser(com.udb.core.authn.services.v1.UpdateUserRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getUpdateUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.ChangeUserStatusResponse changeUserStatus(com.udb.core.authn.services.v1.ChangeUserStatusRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getChangeUserStatusMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Admin-triggered password reset — sends email OTP to complete flow
     * </pre>
     */
    public com.udb.core.authn.services.v1.AdminResetPasswordResponse adminResetPassword(com.udb.core.authn.services.v1.AdminResetPasswordRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getAdminResetPasswordMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── OTP ──────────────────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.authn.services.v1.SendOTPResponse sendOTP(com.udb.core.authn.services.v1.SendOTPRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSendOTPMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.VerifyOTPResponse verifyOTP(com.udb.core.authn.services.v1.VerifyOTPRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getVerifyOTPMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.ResendOTPResponse resendOTP(com.udb.core.authn.services.v1.ResendOTPRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getResendOTPMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Authentication ───────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.authn.services.v1.AuthnResponse authenticate(com.udb.core.authn.services.v1.AuthnRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getAuthenticateMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.LoginResponse login(com.udb.core.authn.services.v1.LoginRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getLoginMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.RefreshTokenResponse refreshToken(com.udb.core.authn.services.v1.RefreshTokenRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRefreshTokenMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.LogoutResponse logout(com.udb.core.authn.services.v1.LogoutRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getLogoutMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.ChangePasswordResponse changePassword(com.udb.core.authn.services.v1.ChangePasswordRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getChangePasswordMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Token validation (called by gateway + per-service interceptors) ───────
     * </pre>
     */
    public com.udb.core.authn.services.v1.ValidateTokenResponse validateToken(com.udb.core.authn.services.v1.ValidateTokenRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getValidateTokenMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Session management ───────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.authn.services.v1.CreateSessionResponse createSession(com.udb.core.authn.services.v1.CreateSessionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCreateSessionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.RefreshSessionResponse refreshSession(com.udb.core.authn.services.v1.RefreshSessionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRefreshSessionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.GetSessionResponse getSession(com.udb.core.authn.services.v1.GetSessionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetSessionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.ListSessionsResponse listSessions(com.udb.core.authn.services.v1.ListSessionsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListSessionsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.RevokeSessionResponse revokeSession(com.udb.core.authn.services.v1.RevokeSessionRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getRevokeSessionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── CSRF (server-side sessions only) ────────────────────────────────────
     * </pre>
     */
    public com.udb.core.authn.services.v1.ValidateCSRFResponse validateCSRF(com.udb.core.authn.services.v1.ValidateCSRFRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getValidateCSRFMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── MFA enrollment ───────────────────────────────────────────────────────
     * Step 1: initiate enrollment — returns TOTP secret / QR URI
     * </pre>
     */
    public com.udb.core.authn.services.v1.EnrollMFAResponse enrollMFA(com.udb.core.authn.services.v1.EnrollMFARequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getEnrollMFAMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Step 2: confirm with first TOTP code (or email OTP)
     * </pre>
     */
    public com.udb.core.authn.services.v1.ConfirmMFAEnrollmentResponse confirmMFAEnrollment(com.udb.core.authn.services.v1.ConfirmMFAEnrollmentRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getConfirmMFAEnrollmentMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Generate a fresh set of single-use MFA recovery/backup codes (returned once;
     * any prior codes for the user are invalidated).
     * </pre>
     */
    public com.udb.core.authn.services.v1.GenerateRecoveryCodesResponse generateRecoveryCodes(com.udb.core.authn.services.v1.GenerateRecoveryCodesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGenerateRecoveryCodesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Set the per-tenant MFA enforcement policy.
     * </pre>
     */
    public com.udb.core.authn.services.v1.PutMfaPolicyResponse putMfaPolicy(com.udb.core.authn.services.v1.PutMfaPolicyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getPutMfaPolicyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Read the per-tenant MFA enforcement policy.
     * </pre>
     */
    public com.udb.core.authn.services.v1.GetMfaPolicyResponse getMfaPolicy(com.udb.core.authn.services.v1.GetMfaPolicyRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetMfaPolicyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * User-initiated password reset: issues a PASSWORD_RESET OTP (delivered to the
     * account's channel). Public — no bearer required.
     * </pre>
     */
    public com.udb.core.authn.services.v1.ForgotPasswordResponse forgotPassword(com.udb.core.authn.services.v1.ForgotPasswordRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getForgotPasswordMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Complete a password reset with the OTP from ForgotPassword (no current
     * password required). Public — the OTP is the proof of control.
     * </pre>
     */
    public com.udb.core.authn.services.v1.ResetPasswordResponse resetPassword(com.udb.core.authn.services.v1.ResetPasswordRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getResetPasswordMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * OAuth2-style token introspection for a UDB-issued JWT.
     * </pre>
     */
    public com.udb.core.authn.services.v1.IntrospectTokenResponse introspectToken(com.udb.core.authn.services.v1.IntrospectTokenRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getIntrospectTokenMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Set the user's phone number and send an SMS verification OTP. Complete with
     * VerifyOTP (the response is verified the same way as email).
     * </pre>
     */
    public com.udb.core.authn.services.v1.SendPhoneVerificationResponse sendPhoneVerification(com.udb.core.authn.services.v1.SendPhoneVerificationRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSendPhoneVerificationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * JSON Web Key Set for verifying UDB-issued JWTs. Public.
     * </pre>
     */
    public com.udb.core.authn.services.v1.GetJwksResponse getJwks(com.udb.core.authn.services.v1.GetJwksRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetJwksMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── WebAuthn / passkeys ─────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.authn.services.v1.StartWebAuthnRegistrationResponse startWebAuthnRegistration(com.udb.core.authn.services.v1.StartWebAuthnRegistrationRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getStartWebAuthnRegistrationMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.FinishWebAuthnRegistrationResponse finishWebAuthnRegistration(com.udb.core.authn.services.v1.FinishWebAuthnRegistrationRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getFinishWebAuthnRegistrationMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.StartWebAuthnAuthenticationResponse startWebAuthnAuthentication(com.udb.core.authn.services.v1.StartWebAuthnAuthenticationRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getStartWebAuthnAuthenticationMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationResponse finishWebAuthnAuthentication(com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getFinishWebAuthnAuthenticationMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service AuthnService.
   * <pre>
   * ---------------------------------------------------------------------------
   * AuthnService — native and hybrid authentication for UDB-backed projects.
   *
   * HTTP prefix: /v1/auth
   * URL conventions (Rule 07): snake_case paths, :&lt;verb&gt; custom method suffix, kebab-case query params.
   *
   * Auth method routing is policy-driven. Typical deployments use server-side
   * sessions for browser clients, JWT for APIs/desktop/mobile clients, API keys
   * for service integrations, and external OIDC/SAML/JWT proofs for hybrid auth.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static final class AuthnServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<AuthnServiceBlockingStub> {
    private AuthnServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AuthnServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AuthnServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * ── User management (admin-only) ─────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.authn.services.v1.CreateUserResponse createUser(com.udb.core.authn.services.v1.CreateUserRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.GetUserResponse getUser(com.udb.core.authn.services.v1.GetUserRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.ListUsersResponse listUsers(com.udb.core.authn.services.v1.ListUsersRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListUsersMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.UpdateUserResponse updateUser(com.udb.core.authn.services.v1.UpdateUserRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.ChangeUserStatusResponse changeUserStatus(com.udb.core.authn.services.v1.ChangeUserStatusRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getChangeUserStatusMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Admin-triggered password reset — sends email OTP to complete flow
     * </pre>
     */
    public com.udb.core.authn.services.v1.AdminResetPasswordResponse adminResetPassword(com.udb.core.authn.services.v1.AdminResetPasswordRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAdminResetPasswordMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── OTP ──────────────────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.authn.services.v1.SendOTPResponse sendOTP(com.udb.core.authn.services.v1.SendOTPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendOTPMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.VerifyOTPResponse verifyOTP(com.udb.core.authn.services.v1.VerifyOTPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getVerifyOTPMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.ResendOTPResponse resendOTP(com.udb.core.authn.services.v1.ResendOTPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getResendOTPMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Authentication ───────────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.authn.services.v1.AuthnResponse authenticate(com.udb.core.authn.services.v1.AuthnRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAuthenticateMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.LoginResponse login(com.udb.core.authn.services.v1.LoginRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getLoginMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.RefreshTokenResponse refreshToken(com.udb.core.authn.services.v1.RefreshTokenRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRefreshTokenMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.LogoutResponse logout(com.udb.core.authn.services.v1.LogoutRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getLogoutMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.ChangePasswordResponse changePassword(com.udb.core.authn.services.v1.ChangePasswordRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getChangePasswordMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Token validation (called by gateway + per-service interceptors) ───────
     * </pre>
     */
    public com.udb.core.authn.services.v1.ValidateTokenResponse validateToken(com.udb.core.authn.services.v1.ValidateTokenRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getValidateTokenMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── Session management ───────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.authn.services.v1.CreateSessionResponse createSession(com.udb.core.authn.services.v1.CreateSessionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateSessionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.RefreshSessionResponse refreshSession(com.udb.core.authn.services.v1.RefreshSessionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRefreshSessionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.GetSessionResponse getSession(com.udb.core.authn.services.v1.GetSessionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetSessionMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.ListSessionsResponse listSessions(com.udb.core.authn.services.v1.ListSessionsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListSessionsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.RevokeSessionResponse revokeSession(com.udb.core.authn.services.v1.RevokeSessionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRevokeSessionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── CSRF (server-side sessions only) ────────────────────────────────────
     * </pre>
     */
    public com.udb.core.authn.services.v1.ValidateCSRFResponse validateCSRF(com.udb.core.authn.services.v1.ValidateCSRFRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getValidateCSRFMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── MFA enrollment ───────────────────────────────────────────────────────
     * Step 1: initiate enrollment — returns TOTP secret / QR URI
     * </pre>
     */
    public com.udb.core.authn.services.v1.EnrollMFAResponse enrollMFA(com.udb.core.authn.services.v1.EnrollMFARequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEnrollMFAMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Step 2: confirm with first TOTP code (or email OTP)
     * </pre>
     */
    public com.udb.core.authn.services.v1.ConfirmMFAEnrollmentResponse confirmMFAEnrollment(com.udb.core.authn.services.v1.ConfirmMFAEnrollmentRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getConfirmMFAEnrollmentMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Generate a fresh set of single-use MFA recovery/backup codes (returned once;
     * any prior codes for the user are invalidated).
     * </pre>
     */
    public com.udb.core.authn.services.v1.GenerateRecoveryCodesResponse generateRecoveryCodes(com.udb.core.authn.services.v1.GenerateRecoveryCodesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGenerateRecoveryCodesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Set the per-tenant MFA enforcement policy.
     * </pre>
     */
    public com.udb.core.authn.services.v1.PutMfaPolicyResponse putMfaPolicy(com.udb.core.authn.services.v1.PutMfaPolicyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPutMfaPolicyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Read the per-tenant MFA enforcement policy.
     * </pre>
     */
    public com.udb.core.authn.services.v1.GetMfaPolicyResponse getMfaPolicy(com.udb.core.authn.services.v1.GetMfaPolicyRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetMfaPolicyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * User-initiated password reset: issues a PASSWORD_RESET OTP (delivered to the
     * account's channel). Public — no bearer required.
     * </pre>
     */
    public com.udb.core.authn.services.v1.ForgotPasswordResponse forgotPassword(com.udb.core.authn.services.v1.ForgotPasswordRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getForgotPasswordMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Complete a password reset with the OTP from ForgotPassword (no current
     * password required). Public — the OTP is the proof of control.
     * </pre>
     */
    public com.udb.core.authn.services.v1.ResetPasswordResponse resetPassword(com.udb.core.authn.services.v1.ResetPasswordRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getResetPasswordMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * OAuth2-style token introspection for a UDB-issued JWT.
     * </pre>
     */
    public com.udb.core.authn.services.v1.IntrospectTokenResponse introspectToken(com.udb.core.authn.services.v1.IntrospectTokenRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getIntrospectTokenMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Set the user's phone number and send an SMS verification OTP. Complete with
     * VerifyOTP (the response is verified the same way as email).
     * </pre>
     */
    public com.udb.core.authn.services.v1.SendPhoneVerificationResponse sendPhoneVerification(com.udb.core.authn.services.v1.SendPhoneVerificationRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendPhoneVerificationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * JSON Web Key Set for verifying UDB-issued JWTs. Public.
     * </pre>
     */
    public com.udb.core.authn.services.v1.GetJwksResponse getJwks(com.udb.core.authn.services.v1.GetJwksRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetJwksMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ── WebAuthn / passkeys ─────────────────────────────────────────────────
     * </pre>
     */
    public com.udb.core.authn.services.v1.StartWebAuthnRegistrationResponse startWebAuthnRegistration(com.udb.core.authn.services.v1.StartWebAuthnRegistrationRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStartWebAuthnRegistrationMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.FinishWebAuthnRegistrationResponse finishWebAuthnRegistration(com.udb.core.authn.services.v1.FinishWebAuthnRegistrationRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getFinishWebAuthnRegistrationMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.StartWebAuthnAuthenticationResponse startWebAuthnAuthentication(com.udb.core.authn.services.v1.StartWebAuthnAuthenticationRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStartWebAuthnAuthenticationMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationResponse finishWebAuthnAuthentication(com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getFinishWebAuthnAuthenticationMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service AuthnService.
   * <pre>
   * ---------------------------------------------------------------------------
   * AuthnService — native and hybrid authentication for UDB-backed projects.
   *
   * HTTP prefix: /v1/auth
   * URL conventions (Rule 07): snake_case paths, :&lt;verb&gt; custom method suffix, kebab-case query params.
   *
   * Auth method routing is policy-driven. Typical deployments use server-side
   * sessions for browser clients, JWT for APIs/desktop/mobile clients, API keys
   * for service integrations, and external OIDC/SAML/JWT proofs for hybrid auth.
   * ---------------------------------------------------------------------------
   * </pre>
   */
  public static final class AuthnServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<AuthnServiceFutureStub> {
    private AuthnServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AuthnServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AuthnServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * ── User management (admin-only) ─────────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.CreateUserResponse> createUser(
        com.udb.core.authn.services.v1.CreateUserRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateUserMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.GetUserResponse> getUser(
        com.udb.core.authn.services.v1.GetUserRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetUserMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.ListUsersResponse> listUsers(
        com.udb.core.authn.services.v1.ListUsersRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListUsersMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.UpdateUserResponse> updateUser(
        com.udb.core.authn.services.v1.UpdateUserRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateUserMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.ChangeUserStatusResponse> changeUserStatus(
        com.udb.core.authn.services.v1.ChangeUserStatusRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getChangeUserStatusMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Admin-triggered password reset — sends email OTP to complete flow
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.AdminResetPasswordResponse> adminResetPassword(
        com.udb.core.authn.services.v1.AdminResetPasswordRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAdminResetPasswordMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── OTP ──────────────────────────────────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.SendOTPResponse> sendOTP(
        com.udb.core.authn.services.v1.SendOTPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendOTPMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.VerifyOTPResponse> verifyOTP(
        com.udb.core.authn.services.v1.VerifyOTPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getVerifyOTPMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.ResendOTPResponse> resendOTP(
        com.udb.core.authn.services.v1.ResendOTPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getResendOTPMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── Authentication ───────────────────────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.AuthnResponse> authenticate(
        com.udb.core.authn.services.v1.AuthnRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAuthenticateMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.LoginResponse> login(
        com.udb.core.authn.services.v1.LoginRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getLoginMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.RefreshTokenResponse> refreshToken(
        com.udb.core.authn.services.v1.RefreshTokenRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRefreshTokenMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.LogoutResponse> logout(
        com.udb.core.authn.services.v1.LogoutRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getLogoutMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.ChangePasswordResponse> changePassword(
        com.udb.core.authn.services.v1.ChangePasswordRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getChangePasswordMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── Token validation (called by gateway + per-service interceptors) ───────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.ValidateTokenResponse> validateToken(
        com.udb.core.authn.services.v1.ValidateTokenRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getValidateTokenMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── Session management ───────────────────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.CreateSessionResponse> createSession(
        com.udb.core.authn.services.v1.CreateSessionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateSessionMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.RefreshSessionResponse> refreshSession(
        com.udb.core.authn.services.v1.RefreshSessionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRefreshSessionMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.GetSessionResponse> getSession(
        com.udb.core.authn.services.v1.GetSessionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetSessionMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.ListSessionsResponse> listSessions(
        com.udb.core.authn.services.v1.ListSessionsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListSessionsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.RevokeSessionResponse> revokeSession(
        com.udb.core.authn.services.v1.RevokeSessionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRevokeSessionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── CSRF (server-side sessions only) ────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.ValidateCSRFResponse> validateCSRF(
        com.udb.core.authn.services.v1.ValidateCSRFRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getValidateCSRFMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── MFA enrollment ───────────────────────────────────────────────────────
     * Step 1: initiate enrollment — returns TOTP secret / QR URI
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.EnrollMFAResponse> enrollMFA(
        com.udb.core.authn.services.v1.EnrollMFARequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEnrollMFAMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Step 2: confirm with first TOTP code (or email OTP)
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.ConfirmMFAEnrollmentResponse> confirmMFAEnrollment(
        com.udb.core.authn.services.v1.ConfirmMFAEnrollmentRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getConfirmMFAEnrollmentMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Generate a fresh set of single-use MFA recovery/backup codes (returned once;
     * any prior codes for the user are invalidated).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.GenerateRecoveryCodesResponse> generateRecoveryCodes(
        com.udb.core.authn.services.v1.GenerateRecoveryCodesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGenerateRecoveryCodesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Set the per-tenant MFA enforcement policy.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.PutMfaPolicyResponse> putMfaPolicy(
        com.udb.core.authn.services.v1.PutMfaPolicyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPutMfaPolicyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Read the per-tenant MFA enforcement policy.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.GetMfaPolicyResponse> getMfaPolicy(
        com.udb.core.authn.services.v1.GetMfaPolicyRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetMfaPolicyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * User-initiated password reset: issues a PASSWORD_RESET OTP (delivered to the
     * account's channel). Public — no bearer required.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.ForgotPasswordResponse> forgotPassword(
        com.udb.core.authn.services.v1.ForgotPasswordRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getForgotPasswordMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Complete a password reset with the OTP from ForgotPassword (no current
     * password required). Public — the OTP is the proof of control.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.ResetPasswordResponse> resetPassword(
        com.udb.core.authn.services.v1.ResetPasswordRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getResetPasswordMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * OAuth2-style token introspection for a UDB-issued JWT.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.IntrospectTokenResponse> introspectToken(
        com.udb.core.authn.services.v1.IntrospectTokenRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getIntrospectTokenMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Set the user's phone number and send an SMS verification OTP. Complete with
     * VerifyOTP (the response is verified the same way as email).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.SendPhoneVerificationResponse> sendPhoneVerification(
        com.udb.core.authn.services.v1.SendPhoneVerificationRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendPhoneVerificationMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * JSON Web Key Set for verifying UDB-issued JWTs. Public.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.GetJwksResponse> getJwks(
        com.udb.core.authn.services.v1.GetJwksRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetJwksMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ── WebAuthn / passkeys ─────────────────────────────────────────────────
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.StartWebAuthnRegistrationResponse> startWebAuthnRegistration(
        com.udb.core.authn.services.v1.StartWebAuthnRegistrationRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStartWebAuthnRegistrationMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.FinishWebAuthnRegistrationResponse> finishWebAuthnRegistration(
        com.udb.core.authn.services.v1.FinishWebAuthnRegistrationRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getFinishWebAuthnRegistrationMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.StartWebAuthnAuthenticationResponse> startWebAuthnAuthentication(
        com.udb.core.authn.services.v1.StartWebAuthnAuthenticationRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStartWebAuthnAuthenticationMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationResponse> finishWebAuthnAuthentication(
        com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getFinishWebAuthnAuthenticationMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_USER = 0;
  private static final int METHODID_GET_USER = 1;
  private static final int METHODID_LIST_USERS = 2;
  private static final int METHODID_UPDATE_USER = 3;
  private static final int METHODID_CHANGE_USER_STATUS = 4;
  private static final int METHODID_ADMIN_RESET_PASSWORD = 5;
  private static final int METHODID_SEND_OTP = 6;
  private static final int METHODID_VERIFY_OTP = 7;
  private static final int METHODID_RESEND_OTP = 8;
  private static final int METHODID_AUTHENTICATE = 9;
  private static final int METHODID_LOGIN = 10;
  private static final int METHODID_REFRESH_TOKEN = 11;
  private static final int METHODID_LOGOUT = 12;
  private static final int METHODID_CHANGE_PASSWORD = 13;
  private static final int METHODID_VALIDATE_TOKEN = 14;
  private static final int METHODID_CREATE_SESSION = 15;
  private static final int METHODID_REFRESH_SESSION = 16;
  private static final int METHODID_GET_SESSION = 17;
  private static final int METHODID_LIST_SESSIONS = 18;
  private static final int METHODID_REVOKE_SESSION = 19;
  private static final int METHODID_VALIDATE_CSRF = 20;
  private static final int METHODID_ENROLL_MFA = 21;
  private static final int METHODID_CONFIRM_MFAENROLLMENT = 22;
  private static final int METHODID_GENERATE_RECOVERY_CODES = 23;
  private static final int METHODID_PUT_MFA_POLICY = 24;
  private static final int METHODID_GET_MFA_POLICY = 25;
  private static final int METHODID_FORGOT_PASSWORD = 26;
  private static final int METHODID_RESET_PASSWORD = 27;
  private static final int METHODID_INTROSPECT_TOKEN = 28;
  private static final int METHODID_SEND_PHONE_VERIFICATION = 29;
  private static final int METHODID_GET_JWKS = 30;
  private static final int METHODID_START_WEB_AUTHN_REGISTRATION = 31;
  private static final int METHODID_FINISH_WEB_AUTHN_REGISTRATION = 32;
  private static final int METHODID_START_WEB_AUTHN_AUTHENTICATION = 33;
  private static final int METHODID_FINISH_WEB_AUTHN_AUTHENTICATION = 34;

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
        case METHODID_CREATE_USER:
          serviceImpl.createUser((com.udb.core.authn.services.v1.CreateUserRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.CreateUserResponse>) responseObserver);
          break;
        case METHODID_GET_USER:
          serviceImpl.getUser((com.udb.core.authn.services.v1.GetUserRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.GetUserResponse>) responseObserver);
          break;
        case METHODID_LIST_USERS:
          serviceImpl.listUsers((com.udb.core.authn.services.v1.ListUsersRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ListUsersResponse>) responseObserver);
          break;
        case METHODID_UPDATE_USER:
          serviceImpl.updateUser((com.udb.core.authn.services.v1.UpdateUserRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.UpdateUserResponse>) responseObserver);
          break;
        case METHODID_CHANGE_USER_STATUS:
          serviceImpl.changeUserStatus((com.udb.core.authn.services.v1.ChangeUserStatusRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ChangeUserStatusResponse>) responseObserver);
          break;
        case METHODID_ADMIN_RESET_PASSWORD:
          serviceImpl.adminResetPassword((com.udb.core.authn.services.v1.AdminResetPasswordRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.AdminResetPasswordResponse>) responseObserver);
          break;
        case METHODID_SEND_OTP:
          serviceImpl.sendOTP((com.udb.core.authn.services.v1.SendOTPRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.SendOTPResponse>) responseObserver);
          break;
        case METHODID_VERIFY_OTP:
          serviceImpl.verifyOTP((com.udb.core.authn.services.v1.VerifyOTPRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.VerifyOTPResponse>) responseObserver);
          break;
        case METHODID_RESEND_OTP:
          serviceImpl.resendOTP((com.udb.core.authn.services.v1.ResendOTPRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ResendOTPResponse>) responseObserver);
          break;
        case METHODID_AUTHENTICATE:
          serviceImpl.authenticate((com.udb.core.authn.services.v1.AuthnRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.AuthnResponse>) responseObserver);
          break;
        case METHODID_LOGIN:
          serviceImpl.login((com.udb.core.authn.services.v1.LoginRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.LoginResponse>) responseObserver);
          break;
        case METHODID_REFRESH_TOKEN:
          serviceImpl.refreshToken((com.udb.core.authn.services.v1.RefreshTokenRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.RefreshTokenResponse>) responseObserver);
          break;
        case METHODID_LOGOUT:
          serviceImpl.logout((com.udb.core.authn.services.v1.LogoutRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.LogoutResponse>) responseObserver);
          break;
        case METHODID_CHANGE_PASSWORD:
          serviceImpl.changePassword((com.udb.core.authn.services.v1.ChangePasswordRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ChangePasswordResponse>) responseObserver);
          break;
        case METHODID_VALIDATE_TOKEN:
          serviceImpl.validateToken((com.udb.core.authn.services.v1.ValidateTokenRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ValidateTokenResponse>) responseObserver);
          break;
        case METHODID_CREATE_SESSION:
          serviceImpl.createSession((com.udb.core.authn.services.v1.CreateSessionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.CreateSessionResponse>) responseObserver);
          break;
        case METHODID_REFRESH_SESSION:
          serviceImpl.refreshSession((com.udb.core.authn.services.v1.RefreshSessionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.RefreshSessionResponse>) responseObserver);
          break;
        case METHODID_GET_SESSION:
          serviceImpl.getSession((com.udb.core.authn.services.v1.GetSessionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.GetSessionResponse>) responseObserver);
          break;
        case METHODID_LIST_SESSIONS:
          serviceImpl.listSessions((com.udb.core.authn.services.v1.ListSessionsRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ListSessionsResponse>) responseObserver);
          break;
        case METHODID_REVOKE_SESSION:
          serviceImpl.revokeSession((com.udb.core.authn.services.v1.RevokeSessionRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.RevokeSessionResponse>) responseObserver);
          break;
        case METHODID_VALIDATE_CSRF:
          serviceImpl.validateCSRF((com.udb.core.authn.services.v1.ValidateCSRFRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ValidateCSRFResponse>) responseObserver);
          break;
        case METHODID_ENROLL_MFA:
          serviceImpl.enrollMFA((com.udb.core.authn.services.v1.EnrollMFARequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.EnrollMFAResponse>) responseObserver);
          break;
        case METHODID_CONFIRM_MFAENROLLMENT:
          serviceImpl.confirmMFAEnrollment((com.udb.core.authn.services.v1.ConfirmMFAEnrollmentRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ConfirmMFAEnrollmentResponse>) responseObserver);
          break;
        case METHODID_GENERATE_RECOVERY_CODES:
          serviceImpl.generateRecoveryCodes((com.udb.core.authn.services.v1.GenerateRecoveryCodesRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.GenerateRecoveryCodesResponse>) responseObserver);
          break;
        case METHODID_PUT_MFA_POLICY:
          serviceImpl.putMfaPolicy((com.udb.core.authn.services.v1.PutMfaPolicyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.PutMfaPolicyResponse>) responseObserver);
          break;
        case METHODID_GET_MFA_POLICY:
          serviceImpl.getMfaPolicy((com.udb.core.authn.services.v1.GetMfaPolicyRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.GetMfaPolicyResponse>) responseObserver);
          break;
        case METHODID_FORGOT_PASSWORD:
          serviceImpl.forgotPassword((com.udb.core.authn.services.v1.ForgotPasswordRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ForgotPasswordResponse>) responseObserver);
          break;
        case METHODID_RESET_PASSWORD:
          serviceImpl.resetPassword((com.udb.core.authn.services.v1.ResetPasswordRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.ResetPasswordResponse>) responseObserver);
          break;
        case METHODID_INTROSPECT_TOKEN:
          serviceImpl.introspectToken((com.udb.core.authn.services.v1.IntrospectTokenRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.IntrospectTokenResponse>) responseObserver);
          break;
        case METHODID_SEND_PHONE_VERIFICATION:
          serviceImpl.sendPhoneVerification((com.udb.core.authn.services.v1.SendPhoneVerificationRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.SendPhoneVerificationResponse>) responseObserver);
          break;
        case METHODID_GET_JWKS:
          serviceImpl.getJwks((com.udb.core.authn.services.v1.GetJwksRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.GetJwksResponse>) responseObserver);
          break;
        case METHODID_START_WEB_AUTHN_REGISTRATION:
          serviceImpl.startWebAuthnRegistration((com.udb.core.authn.services.v1.StartWebAuthnRegistrationRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.StartWebAuthnRegistrationResponse>) responseObserver);
          break;
        case METHODID_FINISH_WEB_AUTHN_REGISTRATION:
          serviceImpl.finishWebAuthnRegistration((com.udb.core.authn.services.v1.FinishWebAuthnRegistrationRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.FinishWebAuthnRegistrationResponse>) responseObserver);
          break;
        case METHODID_START_WEB_AUTHN_AUTHENTICATION:
          serviceImpl.startWebAuthnAuthentication((com.udb.core.authn.services.v1.StartWebAuthnAuthenticationRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.StartWebAuthnAuthenticationResponse>) responseObserver);
          break;
        case METHODID_FINISH_WEB_AUTHN_AUTHENTICATION:
          serviceImpl.finishWebAuthnAuthentication((com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationRequest) request,
              (io.grpc.stub.StreamObserver<com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationResponse>) responseObserver);
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
          getCreateUserMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.CreateUserRequest,
              com.udb.core.authn.services.v1.CreateUserResponse>(
                service, METHODID_CREATE_USER)))
        .addMethod(
          getGetUserMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.GetUserRequest,
              com.udb.core.authn.services.v1.GetUserResponse>(
                service, METHODID_GET_USER)))
        .addMethod(
          getListUsersMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.ListUsersRequest,
              com.udb.core.authn.services.v1.ListUsersResponse>(
                service, METHODID_LIST_USERS)))
        .addMethod(
          getUpdateUserMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.UpdateUserRequest,
              com.udb.core.authn.services.v1.UpdateUserResponse>(
                service, METHODID_UPDATE_USER)))
        .addMethod(
          getChangeUserStatusMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.ChangeUserStatusRequest,
              com.udb.core.authn.services.v1.ChangeUserStatusResponse>(
                service, METHODID_CHANGE_USER_STATUS)))
        .addMethod(
          getAdminResetPasswordMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.AdminResetPasswordRequest,
              com.udb.core.authn.services.v1.AdminResetPasswordResponse>(
                service, METHODID_ADMIN_RESET_PASSWORD)))
        .addMethod(
          getSendOTPMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.SendOTPRequest,
              com.udb.core.authn.services.v1.SendOTPResponse>(
                service, METHODID_SEND_OTP)))
        .addMethod(
          getVerifyOTPMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.VerifyOTPRequest,
              com.udb.core.authn.services.v1.VerifyOTPResponse>(
                service, METHODID_VERIFY_OTP)))
        .addMethod(
          getResendOTPMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.ResendOTPRequest,
              com.udb.core.authn.services.v1.ResendOTPResponse>(
                service, METHODID_RESEND_OTP)))
        .addMethod(
          getAuthenticateMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.AuthnRequest,
              com.udb.core.authn.services.v1.AuthnResponse>(
                service, METHODID_AUTHENTICATE)))
        .addMethod(
          getLoginMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.LoginRequest,
              com.udb.core.authn.services.v1.LoginResponse>(
                service, METHODID_LOGIN)))
        .addMethod(
          getRefreshTokenMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.RefreshTokenRequest,
              com.udb.core.authn.services.v1.RefreshTokenResponse>(
                service, METHODID_REFRESH_TOKEN)))
        .addMethod(
          getLogoutMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.LogoutRequest,
              com.udb.core.authn.services.v1.LogoutResponse>(
                service, METHODID_LOGOUT)))
        .addMethod(
          getChangePasswordMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.ChangePasswordRequest,
              com.udb.core.authn.services.v1.ChangePasswordResponse>(
                service, METHODID_CHANGE_PASSWORD)))
        .addMethod(
          getValidateTokenMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.ValidateTokenRequest,
              com.udb.core.authn.services.v1.ValidateTokenResponse>(
                service, METHODID_VALIDATE_TOKEN)))
        .addMethod(
          getCreateSessionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.CreateSessionRequest,
              com.udb.core.authn.services.v1.CreateSessionResponse>(
                service, METHODID_CREATE_SESSION)))
        .addMethod(
          getRefreshSessionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.RefreshSessionRequest,
              com.udb.core.authn.services.v1.RefreshSessionResponse>(
                service, METHODID_REFRESH_SESSION)))
        .addMethod(
          getGetSessionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.GetSessionRequest,
              com.udb.core.authn.services.v1.GetSessionResponse>(
                service, METHODID_GET_SESSION)))
        .addMethod(
          getListSessionsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.ListSessionsRequest,
              com.udb.core.authn.services.v1.ListSessionsResponse>(
                service, METHODID_LIST_SESSIONS)))
        .addMethod(
          getRevokeSessionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.RevokeSessionRequest,
              com.udb.core.authn.services.v1.RevokeSessionResponse>(
                service, METHODID_REVOKE_SESSION)))
        .addMethod(
          getValidateCSRFMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.ValidateCSRFRequest,
              com.udb.core.authn.services.v1.ValidateCSRFResponse>(
                service, METHODID_VALIDATE_CSRF)))
        .addMethod(
          getEnrollMFAMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.EnrollMFARequest,
              com.udb.core.authn.services.v1.EnrollMFAResponse>(
                service, METHODID_ENROLL_MFA)))
        .addMethod(
          getConfirmMFAEnrollmentMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.ConfirmMFAEnrollmentRequest,
              com.udb.core.authn.services.v1.ConfirmMFAEnrollmentResponse>(
                service, METHODID_CONFIRM_MFAENROLLMENT)))
        .addMethod(
          getGenerateRecoveryCodesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.GenerateRecoveryCodesRequest,
              com.udb.core.authn.services.v1.GenerateRecoveryCodesResponse>(
                service, METHODID_GENERATE_RECOVERY_CODES)))
        .addMethod(
          getPutMfaPolicyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.PutMfaPolicyRequest,
              com.udb.core.authn.services.v1.PutMfaPolicyResponse>(
                service, METHODID_PUT_MFA_POLICY)))
        .addMethod(
          getGetMfaPolicyMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.GetMfaPolicyRequest,
              com.udb.core.authn.services.v1.GetMfaPolicyResponse>(
                service, METHODID_GET_MFA_POLICY)))
        .addMethod(
          getForgotPasswordMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.ForgotPasswordRequest,
              com.udb.core.authn.services.v1.ForgotPasswordResponse>(
                service, METHODID_FORGOT_PASSWORD)))
        .addMethod(
          getResetPasswordMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.ResetPasswordRequest,
              com.udb.core.authn.services.v1.ResetPasswordResponse>(
                service, METHODID_RESET_PASSWORD)))
        .addMethod(
          getIntrospectTokenMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.IntrospectTokenRequest,
              com.udb.core.authn.services.v1.IntrospectTokenResponse>(
                service, METHODID_INTROSPECT_TOKEN)))
        .addMethod(
          getSendPhoneVerificationMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.SendPhoneVerificationRequest,
              com.udb.core.authn.services.v1.SendPhoneVerificationResponse>(
                service, METHODID_SEND_PHONE_VERIFICATION)))
        .addMethod(
          getGetJwksMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.GetJwksRequest,
              com.udb.core.authn.services.v1.GetJwksResponse>(
                service, METHODID_GET_JWKS)))
        .addMethod(
          getStartWebAuthnRegistrationMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.StartWebAuthnRegistrationRequest,
              com.udb.core.authn.services.v1.StartWebAuthnRegistrationResponse>(
                service, METHODID_START_WEB_AUTHN_REGISTRATION)))
        .addMethod(
          getFinishWebAuthnRegistrationMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.FinishWebAuthnRegistrationRequest,
              com.udb.core.authn.services.v1.FinishWebAuthnRegistrationResponse>(
                service, METHODID_FINISH_WEB_AUTHN_REGISTRATION)))
        .addMethod(
          getStartWebAuthnAuthenticationMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.StartWebAuthnAuthenticationRequest,
              com.udb.core.authn.services.v1.StartWebAuthnAuthenticationResponse>(
                service, METHODID_START_WEB_AUTHN_AUTHENTICATION)))
        .addMethod(
          getFinishWebAuthnAuthenticationMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationRequest,
              com.udb.core.authn.services.v1.FinishWebAuthnAuthenticationResponse>(
                service, METHODID_FINISH_WEB_AUTHN_AUTHENTICATION)))
        .build();
  }

  private static abstract class AuthnServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    AuthnServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.udb.core.authn.services.v1.AuthnServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("AuthnService");
    }
  }

  private static final class AuthnServiceFileDescriptorSupplier
      extends AuthnServiceBaseDescriptorSupplier {
    AuthnServiceFileDescriptorSupplier() {}
  }

  private static final class AuthnServiceMethodDescriptorSupplier
      extends AuthnServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    AuthnServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (AuthnServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new AuthnServiceFileDescriptorSupplier())
              .addMethod(getCreateUserMethod())
              .addMethod(getGetUserMethod())
              .addMethod(getListUsersMethod())
              .addMethod(getUpdateUserMethod())
              .addMethod(getChangeUserStatusMethod())
              .addMethod(getAdminResetPasswordMethod())
              .addMethod(getSendOTPMethod())
              .addMethod(getVerifyOTPMethod())
              .addMethod(getResendOTPMethod())
              .addMethod(getAuthenticateMethod())
              .addMethod(getLoginMethod())
              .addMethod(getRefreshTokenMethod())
              .addMethod(getLogoutMethod())
              .addMethod(getChangePasswordMethod())
              .addMethod(getValidateTokenMethod())
              .addMethod(getCreateSessionMethod())
              .addMethod(getRefreshSessionMethod())
              .addMethod(getGetSessionMethod())
              .addMethod(getListSessionsMethod())
              .addMethod(getRevokeSessionMethod())
              .addMethod(getValidateCSRFMethod())
              .addMethod(getEnrollMFAMethod())
              .addMethod(getConfirmMFAEnrollmentMethod())
              .addMethod(getGenerateRecoveryCodesMethod())
              .addMethod(getPutMfaPolicyMethod())
              .addMethod(getGetMfaPolicyMethod())
              .addMethod(getForgotPasswordMethod())
              .addMethod(getResetPasswordMethod())
              .addMethod(getIntrospectTokenMethod())
              .addMethod(getSendPhoneVerificationMethod())
              .addMethod(getGetJwksMethod())
              .addMethod(getStartWebAuthnRegistrationMethod())
              .addMethod(getFinishWebAuthnRegistrationMethod())
              .addMethod(getStartWebAuthnAuthenticationMethod())
              .addMethod(getFinishWebAuthnAuthenticationMethod())
              .build();
        }
      }
    }
    return result;
  }
}
