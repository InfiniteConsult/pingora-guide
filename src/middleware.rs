//! # Middleware Interface
//!
//! This module defines the plugin system for the Gateway. It allows logic to be
//! injected at various stages of the request lifecycle: Request, Response, and Logging.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `MiddlewareDecision` Enum**:
//!     * Variants:
//!         * `Continue`: Proceed to the next middleware/stage.
//!         * `Stop`: Halt processing immediately (e.g., if a 403 was sent).
//!
//! 2.  **Define `Middleware` Trait**:
//!     * Must inherit `Sync + Send`.
//!     * **`handle_request`**:
//!         * Runs *before* upstream connection.
//!         * Can modify headers, check security, or return `Stop`.
//!     * **`handle_response`**:
//!         * Runs *after* headers are received from upstream.
//!         * Can modify response headers (e.g. `HSTS`) or decide cacheability.
//!     * **`handle_logging`**:
//!         * Runs *after* the session is finished.
//!         * Used for metrics and observability.
//!
//! 3.  **Default Implementations**:
//!     * Provide default "no-op" implementations for all methods so implementors
//!         only need to define the hooks they care about.
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
    ) -> Result<()> {
        Ok(())
    }

    async fn handle_upstream_connected(
        &self,
        _session: &mut Session,
        _reused: bool,
        _peer: &HttpPeer,
        _fd: std::os::unix::io::RawFd,
        _digest: Option<&Digest>,
        _ctx: &mut GatewayContext,
    ) -> Result<()> {
        Ok(())
    }

    async fn handle_upstream_response(
        &self,
        _session: &mut Session,
        _upstream_response: &mut ResponseHeader,
        _ctx: &mut GatewayContext,
    ) -> Result<()> {
        Ok(())
    }

    async fn handle_error(
        &self,
        session: &mut Session,
        e: &Error,
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

    async fn handle_response_body(
        &self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _ctx: &mut GatewayContext,
    ) -> Result<MiddlewareDecision> {
        Ok(MiddlewareDecision::Continue)
    }

    fn init_cache(&self, _session: &mut Session, _ctx: &mut GatewayContext) -> Result<()> {
        Ok(())
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
        _ctx: &mut GatewayContext) -> () {
        ()
    }
}