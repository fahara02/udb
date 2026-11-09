//! A native-service call that reaches the public DataBroker listener names the
//! listener it belongs on. Native services are served only on the control-plane
//! listener (`UDB_AUTH_GRPC_ADDR`, default loopback at the DataBroker port + 10),
//! and the data listener used to answer them with a bare `UNIMPLEMENTED`, which
//! told a client nothing about the port it had wrong.

use tonic::body::BoxBody;
use tonic::codegen::http;
use tonic::codegen::{Context, Future, Pin, Poll, Service};

/// Every native service lives under this package prefix; the data listener mounts
/// only DataBroker, health and reflection, none of which do.
const NATIVE_PATH_PREFIX: &str = "/udb.core.";

/// The refusal for a native call on the data listener.
pub(crate) fn wrong_listener_status(path: &str, control_plane: Option<&str>) -> tonic::Status {
    let service = path
        .trim_start_matches('/')
        .split('/')
        .next()
        .unwrap_or_default();
    let message = match control_plane {
        Some(addr) => format!(
            "{service} is a native service, served on the control-plane listener {addr}, not on \
             this DataBroker listener. Point the SDK's authTarget (UDB_AUTH_TARGET) at {addr}; a \
             remote caller needs the broker's UDB_AUTH_GRPC_ADDR on a reachable interface"
        ),
        None => format!(
            "{service} is a native service, and native services are disabled on this broker \
             (UDB_NATIVE_SERVICES_ENABLED)"
        ),
    };
    crate::runtime::executor_utils::capability_status_with_code(
        tonic::Code::Unimplemented,
        "native",
        "native_service_on_data_listener",
        "native_control_plane_listener",
        message,
    )
}

#[derive(Clone)]
pub(crate) struct WrongListenerLayer {
    control_plane: Option<String>,
}

impl WrongListenerLayer {
    /// `control_plane` is the address native services listen on, or `None` when
    /// this broker serves none.
    pub(crate) fn new(control_plane: Option<String>) -> Self {
        Self { control_plane }
    }
}

impl<S> tower::Layer<S> for WrongListenerLayer {
    type Service = WrongListenerService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        WrongListenerService {
            inner,
            control_plane: self.control_plane.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct WrongListenerService<S> {
    inner: S,
    control_plane: Option<String>,
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for WrongListenerService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = http::Response<BoxBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<http::Response<BoxBody>, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let path = req.uri().path();
        if path.starts_with(NATIVE_PATH_PREFIX) {
            let status = wrong_listener_status(path, self.control_plane.as_deref());
            return Box::pin(async move { Ok(status.into_http()) });
        }
        let future = self.inner.call(req);
        Box::pin(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_native_call_on_the_data_listener_names_the_control_plane() {
        let status = wrong_listener_status(
            "/udb.core.tenant.services.v1.TenantService/ListTenants",
            Some("127.0.0.1:50061"),
        );
        assert_eq!(status.code(), tonic::Code::Unimplemented);
        assert!(
            status
                .message()
                .contains("udb.core.tenant.services.v1.TenantService")
        );
        assert!(status.message().contains("127.0.0.1:50061"));
        assert!(status.message().contains("authTarget"));
        let disabled =
            wrong_listener_status("/udb.core.lock.services.v1.LockService/AcquireLock", None);
        assert!(disabled.message().contains("disabled"));
    }
}
