//! Live Postgres tests for the native auth services.
//!
//! These tests intentionally do not use in-memory stores or snapshot mutation.
//! They apply native auth DDL generated from embedded UDB protos, exercise the
//! Postgres-backed services, and drop the native auth schemas at the end.

mod analytics_live;
mod apikey_live;
mod authn_authenticate_live;
mod authn_mfa_live;
mod authn_otp_password_live;
#[cfg(feature = "redis")]
mod authn_redis_live;
mod authn_security_live;
mod authn_session_live;
mod authn_user_live;
mod authz_admin_live;
mod authz_rbac_live;
mod authz_rebac_live;
#[cfg(feature = "kafka")]
mod cdc_live;
#[cfg(feature = "kafka")]
mod events_live;
#[cfg(feature = "kafka")]
mod notification_events_live;
mod notification_live;
mod support;
mod tenant_live;
