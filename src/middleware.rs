//! # Middleware Interface & Lifecycle Mapping
//!
//! This module defines the plugin system (`Middleware` trait) for the Gateway. It abstracts
//! the complex `ProxyHttp` lifecycle into a cleaner, pipeline-based interface.
//!
//! ## Lifecycle Mapping
//!
//! The following table describes how Pingora's `ProxyHttp` stages map to our `Middleware` hooks.
//! The Gateway executes these hooks in the order defined in the middleware pipeline.
//!
//! | Stage | Pingora `ProxyHttp` Method | Middleware Hook | Description |
//! | :--- | :--- | :--- | :--- |
//! | **1. Early Request** | `early_request_filter` | `handle_early_request` | **Pre-Routing**: IP blocklists, bot detection, and cheap checks before regex parsing. |
//! | **2. Request** | `request_filter` | `handle_request` | **Post-Routing**: Authentication, Rate Limiting, and business logic. Routing info is available in `ctx`. |
//! | **3. Request Body** | `request_body_filter` | `handle_request_body` | **Streaming**: WAF inspection or DLP on upload chunks. |
//! | **4. Cache Init** | `request_cache_filter` | `init_cache` | **Caching**: Enable/Disable caching for this session. |
//! | **5. Cache Key** | `cache_key_callback` | `cache_key` | **Caching**: Define custom cache keys (e.g. by Accept-Language or API Key). |
//! | **6. Upstream Req** | `upstream_request_filter` | `handle_upstream_request` | **Injection**: Modify headers sent to backend (e.g. add `X-User-ID`, sign requests). |
//! | **7. Connection** | `connected_to_upstream` | `handle_upstream_connected` | **Telemetry**: Log connection establishment, validate peer certificates. |
//! | **8. Upstream Resp**| `upstream_response_filter`| `handle_upstream_response` | **Sanitization**: Modify backend headers *before* they are committed to cache. |
//! | **9. Response** | `response_filter` | `handle_response` | **Security**: Add headers to client response (HSTS, CSP, CORS). |
//! | **10. Resp Body** | `response_body_filter` | `handle_response_body` | **Streaming**: Response WAF, body transformation, or scanning. |
//! | **11. Error** | `fail_to_proxy`, `fail_to_connect`, `error_while_proxy` | `handle_error` | **Recovery**: Serve custom error pages (e.g., 502 Maintenance) or trigger alerts. |
//! | **12. Logging** | `logging` | `handle_logging` | **Observability**: Finalize metrics, write access logs, end tracing spans. |
//!
//! ## Decision Flow
//!
//! For hooks returning `MiddlewareDecision`:
//! * `MiddlewareDecision::Continue`: The Gateway proceeds to the next middleware in the chain.
//! * `MiddlewareDecision::Stop`: The Gateway **halts** the pipeline immediately. This usually implies the middleware has already sent a response (e.g., 403 Forbidden).
use async_trait::async_trait;
use bytes::Bytes;
use pingora::Error;
use pingora::http::{RequestHeader, ResponseHeader};
use pingora::prelude::{HttpPeer, Session};
use pingora::protocols::Digest;
use pingora_cache::CacheKey;
use crate::context::GatewayContext;
use crate::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiddlewareDecision {
    Continue,
    Stop
}

#[async_trait]
pub trait Middleware: Send + Sync {
    fn name(&self) -> &str;

    async fn handle_early_request(
        &self,
        _session: &mut Session,
        _ctx: &mut GatewayContext
    ) -> Result<MiddlewareDecision> {
        Ok(MiddlewareDecision::Continue)
    }

    async fn handle_request(
        &self,
        _session: &mut Session,
        _ctx: &mut GatewayContext
    ) -> Result<MiddlewareDecision> {
        Ok(MiddlewareDecision::Continue)
    }

    async fn handle_upstream_request(
        &self,
        _session: &mut Session,
        _upstream_request: &mut RequestHeader,
        _ctx: &mut GatewayContext,
    ) -> Result<MiddlewareDecision> {
        Ok(MiddlewareDecision::Continue)
    }

    async fn handle_upstream_connected(
        &self,
        _session: &mut Session,
        _reused: bool,
        _peer: &HttpPeer,
        _fd: std::os::unix::io::RawFd,
        _digest: Option<&Digest>,
        _ctx: &mut GatewayContext,
    ) -> Result<MiddlewareDecision> {
        Ok(MiddlewareDecision::Continue)
    }

    fn handle_upstream_response(
        &self,
        _session: &mut Session,
        _upstream_response: &mut ResponseHeader,
        _ctx: &mut GatewayContext,
    ) -> Result<MiddlewareDecision> {
        Ok(MiddlewareDecision::Continue)
    }

    async fn handle_error(
        &self,
        _session: &mut Session,
        _e: &Error,
        _ctx: &mut GatewayContext,
    ) -> Result<MiddlewareDecision> {
        Ok(MiddlewareDecision::Continue)
    }

    async fn handle_request_body(
        &self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _ctx: &mut GatewayContext,
    ) -> Result<MiddlewareDecision> {
        Ok(MiddlewareDecision::Continue)
    }

    async fn handle_response(
        &self,
        _session: &mut Session,
        _upstream_response: &mut ResponseHeader,
        _ctx: &mut GatewayContext,
    ) -> Result<MiddlewareDecision> {
        Ok(MiddlewareDecision::Continue)
    }

    fn handle_response_body(
        &self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _ctx: &mut GatewayContext,
    ) -> Result<MiddlewareDecision> {
        Ok(MiddlewareDecision::Continue)
    }

    fn init_cache(&self, _session: &mut Session, _ctx: &mut GatewayContext) -> Result<MiddlewareDecision> {
        Ok(MiddlewareDecision::Continue)
    }

    fn cache_key(&self, _session: &Session, _ctx: &mut GatewayContext) -> Result<Option<CacheKey>> {
        Ok(None)
    }

    fn should_serve_stale(
        &self,
        _session: &mut Session,
        _ctx: &mut GatewayContext,
        _error: Option<&Error>,
    ) -> bool {
        false
    }

    async fn handle_logging(
        &self,
        _session: &mut Session,
        _e: Option<&Error>,
        _ctx: &mut GatewayContext) -> Result<MiddlewareDecision> {
        Ok(MiddlewareDecision::Continue)
    }
}