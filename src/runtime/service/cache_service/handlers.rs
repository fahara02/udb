//! The seven `CacheService` RPC handlers, extracted from the trait impl; `mod.rs`
//! delegates one line to each. The tenant is always taken from the VERIFIED
//! claim/header (`validate_request_tenant`), never the request body.

use tonic::{Request, Response, Status};

use crate::proto::udb::core::cache::services::v1 as cache_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{admit_on as native_admit_on, validate_request_tenant};
use super::CacheServiceImpl;
use super::errors::{require_field, validate_namespace};
use super::keys::effective_max_bytes;

#[cfg(feature = "redis")]
use super::config::{
    TOPIC_ENTRY_DELETED, TOPIC_ENTRY_SET, TOPIC_INVALIDATED, TOPIC_NAMESPACE_CREATED,
};
#[cfg(not(feature = "redis"))]
use super::errors::no_redis_status;
#[cfg(feature = "redis")]
use super::events::emit_event;
#[cfg(feature = "redis")]
use super::redis_engine;

pub(crate) async fn get(
    svc: &CacheServiceImpl,
    request: Request<cache_pb::GetRequest>,
) -> Result<Response<cache_pb::GetResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // Cross-tenant guard FIRST: the body tenant_id must match the verified
    // claim/header; after this passes it IS the verified tenant.
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant = req.tenant_id.trim().to_string();
    let namespace = validate_namespace(&req.namespace)?;
    let key = require_field("key", &req.key)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "cache",
        OperationChannel::Read,
        &tenant,
        None,
    )
    .await?;
    #[cfg(feature = "redis")]
    {
        let client = svc.require_redis()?;
        let (found, value, ttl) = redis_engine::get(client, &tenant, &namespace, &key).await?;
        Ok(Response::new(cache_pb::GetResponse {
            found,
            value,
            ttl_remaining_seconds: ttl,
            error: None,
        }))
    }
    #[cfg(not(feature = "redis"))]
    {
        let _ = (&tenant, &namespace, &key);
        Err(no_redis_status())
    }
}

pub(crate) async fn set(
    svc: &CacheServiceImpl,
    request: Request<cache_pb::SetRequest>,
) -> Result<Response<cache_pb::SetResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant = req.tenant_id.trim().to_string();
    let namespace = validate_namespace(&req.namespace)?;
    let key = require_field("key", &req.key)?;
    // Set is an ordinary data mutation, so it shares the Write admission lane
    // with the sibling mutation RPCs (config/metering/notification writes and
    // the data-plane upsert handlers) — the Admin lane is reserved for
    // control-plane operations like namespace lifecycle.
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "cache",
        OperationChannel::Write,
        &tenant,
        None,
    )
    .await?;
    #[cfg(feature = "redis")]
    {
        let client = svc.require_redis()?;
        let outcome = redis_engine::set(
            client,
            &tenant,
            &namespace,
            &key,
            &req.value,
            req.ttl_seconds,
        )
        .await?;
        emit_event(
            svc,
            &metadata,
            TOPIC_ENTRY_SET,
            &tenant,
            &namespace,
            serde_json::json!({
                "tenant_id": tenant,
                "namespace": namespace,
                "key": key,
                "used_bytes": outcome.used_bytes,
            }),
        )
        .await;
        Ok(Response::new(cache_pb::SetResponse {
            stored: true,
            used_bytes: outcome.used_bytes,
            max_bytes: outcome.max_bytes,
            message: "cache entry stored".to_string(),
            error: None,
        }))
    }
    #[cfg(not(feature = "redis"))]
    {
        let _ = (&tenant, &namespace, &key, &req.value, req.ttl_seconds);
        Err(no_redis_status())
    }
}

pub(crate) async fn delete(
    svc: &CacheServiceImpl,
    request: Request<cache_pb::DeleteRequest>,
) -> Result<Response<cache_pb::DeleteResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant = req.tenant_id.trim().to_string();
    let namespace = validate_namespace(&req.namespace)?;
    let key = require_field("key", &req.key)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "cache",
        OperationChannel::Admin,
        &tenant,
        None,
    )
    .await?;
    #[cfg(feature = "redis")]
    {
        let client = svc.require_redis()?;
        let (deleted, used_bytes) = redis_engine::delete(client, &tenant, &namespace, &key).await?;
        if deleted {
            emit_event(
                svc,
                &metadata,
                TOPIC_ENTRY_DELETED,
                &tenant,
                &namespace,
                serde_json::json!({
                    "tenant_id": tenant,
                    "namespace": namespace,
                    "key": key,
                    "used_bytes": used_bytes,
                }),
            )
            .await;
        }
        Ok(Response::new(cache_pb::DeleteResponse {
            deleted,
            used_bytes,
            message: if deleted {
                "cache entry deleted".to_string()
            } else {
                "cache entry not found".to_string()
            },
            error: None,
        }))
    }
    #[cfg(not(feature = "redis"))]
    {
        let _ = (&tenant, &namespace, &key);
        Err(no_redis_status())
    }
}

pub(crate) async fn scan(
    svc: &CacheServiceImpl,
    request: Request<cache_pb::ScanRequest>,
) -> Result<Response<cache_pb::ScanResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant = req.tenant_id.trim().to_string();
    let namespace = validate_namespace(&req.namespace)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "cache",
        OperationChannel::Read,
        &tenant,
        None,
    )
    .await?;
    #[cfg(feature = "redis")]
    {
        let client = svc.require_redis()?;
        let (items, next) = redis_engine::scan(
            client,
            &tenant,
            &namespace,
            req.key_prefix.trim(),
            req.limit,
            req.page_token.trim(),
        )
        .await?;
        Ok(Response::new(cache_pb::ScanResponse {
            items,
            next_page_token: next,
            error: None,
        }))
    }
    #[cfg(not(feature = "redis"))]
    {
        let _ = (
            &tenant,
            &namespace,
            &req.key_prefix,
            req.limit,
            &req.page_token,
        );
        Err(no_redis_status())
    }
}

pub(crate) async fn create_namespace(
    svc: &CacheServiceImpl,
    request: Request<cache_pb::CreateNamespaceRequest>,
) -> Result<Response<cache_pb::CreateNamespaceResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant = req.tenant_id.trim().to_string();
    let namespace = validate_namespace(&req.namespace)?;
    let max_bytes = effective_max_bytes(req.max_bytes);
    let default_ttl = req.default_ttl_seconds.max(0);
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "cache",
        OperationChannel::Admin,
        &tenant,
        None,
    )
    .await?;
    #[cfg(feature = "redis")]
    {
        let client = svc.require_redis()?;
        redis_engine::create_namespace(client, &tenant, &namespace, max_bytes, default_ttl).await?;
        emit_event(
            svc,
            &metadata,
            TOPIC_NAMESPACE_CREATED,
            &tenant,
            &namespace,
            serde_json::json!({
                "tenant_id": tenant,
                "namespace": namespace,
                "max_bytes": max_bytes,
                "default_ttl_seconds": default_ttl,
            }),
        )
        .await;
        Ok(Response::new(cache_pb::CreateNamespaceResponse {
            namespace,
            max_bytes,
            default_ttl_seconds: default_ttl,
            message: "cache namespace ready".to_string(),
            error: None,
        }))
    }
    #[cfg(not(feature = "redis"))]
    {
        let _ = (&tenant, &namespace, max_bytes, default_ttl);
        Err(no_redis_status())
    }
}

pub(crate) async fn delete_namespace(
    svc: &CacheServiceImpl,
    request: Request<cache_pb::DeleteNamespaceRequest>,
) -> Result<Response<cache_pb::DeleteNamespaceResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant = req.tenant_id.trim().to_string();
    let namespace = validate_namespace(&req.namespace)?;
    // DESTRUCTIVE namespace flush: an empty confirmation token fails CLOSED.
    if req.confirmation_token.trim().is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "DeleteNamespace flushes the whole namespace; confirmation_token is required",
            [(
                "confirmation_token",
                "must be present to flush a cache namespace",
            )],
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "cache",
        OperationChannel::Admin,
        &tenant,
        None,
    )
    .await?;
    #[cfg(feature = "redis")]
    {
        let client = svc.require_redis()?;
        let keys_deleted = redis_engine::flush_namespace(client, &tenant, &namespace).await?;
        emit_event(
            svc,
            &metadata,
            TOPIC_INVALIDATED,
            &tenant,
            &namespace,
            serde_json::json!({
                "tenant_id": tenant,
                "namespace": namespace,
                "keys_invalidated": keys_deleted,
                "reason": "delete_namespace",
            }),
        )
        .await;
        Ok(Response::new(cache_pb::DeleteNamespaceResponse {
            namespace,
            keys_deleted,
            message: "cache namespace flushed".to_string(),
            error: None,
        }))
    }
    #[cfg(not(feature = "redis"))]
    {
        let _ = (&tenant, &namespace);
        Err(no_redis_status())
    }
}

pub(crate) async fn get_namespace_stats(
    svc: &CacheServiceImpl,
    request: Request<cache_pb::GetNamespaceStatsRequest>,
) -> Result<Response<cache_pb::GetNamespaceStatsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant = req.tenant_id.trim().to_string();
    let namespace = validate_namespace(&req.namespace)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "cache",
        OperationChannel::Read,
        &tenant,
        None,
    )
    .await?;
    #[cfg(feature = "redis")]
    {
        let client = svc.require_redis()?;
        let stats = redis_engine::stats(client, &tenant, &namespace).await?;
        Ok(Response::new(cache_pb::GetNamespaceStatsResponse {
            namespace,
            used_bytes: stats.used_bytes,
            max_bytes: stats.max_bytes,
            item_count: stats.item_count,
            error: None,
        }))
    }
    #[cfg(not(feature = "redis"))]
    {
        let _ = (&tenant, &namespace);
        Err(no_redis_status())
    }
}
