//! # Gateway Orchestrator
//!
//! The core engine of the library. This struct implements Pingora's native
//! `ProxyHttp` trait and wires together our custom `Middleware` pipeline with
//! our `Upstream` routing logic.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `PingoraGateway` Struct**:
//!     * Fields:
//!         * `upstream`: `Box<dyn Upstream>` (Usually the Router).
//!         * `middlewares`: `Vec<Box<dyn Middleware>>` (The plugin chain).
//!
//! 2.  **Define Associated Type**:
//!     * `type CTX = GatewayContext`.
//!
//! 3.  **Implement `ProxyHttp` Trait**:
//!     * **`new_ctx`**: Initialize an empty `GatewayContext`.
//!     * **`request_filter`**:
//!         * Iterate through `self.middlewares`.
//!         * Call `mw.handle_request(session, ctx)`.
//!         * If any returns `MiddlewareDecision::Stop`, return `Ok(true)` immediately.
//!     * **`upstream_peer`**:
//!         * Call `self.upstream.select_peer(session, ctx)`.
//!         * Map our custom error to Pingora's expected error type.
//!     * **`response_filter`**:
//!         * Iterate through middlewares (order: typically same as request, or reversed).
//!         * Call `mw.handle_response(session, response_header, ctx)`.
//!     * **`logging`**:
//!         * Iterate through middlewares.
//!         * Call `mw.handle_logging(session, error, ctx)`.

use std::collections::HashMap;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use bytes::Bytes;
use pingora::Error;
use pingora::http::{RequestHeader, ResponseHeader};
use pingora::prelude::{HttpPeer, ProxyHttp, Session};
use pingora::protocols::Digest;
use pingora::proxy::FailToProxy;
use crate::config::{GatewayConf, RouteConf};
use crate::context::{GatewayContext, RequestMeta};
use crate::middleware::{Middleware, MiddlewareDecision};
use crate::upstream::Upstream;
use crate::upstreams::router::Router;

pub struct Gateway {
    pub conf: Arc<GatewayConf>,
    pub router: Arc<Router>,
    pub upstreams: Arc<HashMap<String, Box<dyn Upstream>>>,
    pub middlewares: Vec<Box<dyn Middleware>>,
}

#[async_trait]
impl ProxyHttp for Gateway {
    type CTX = GatewayContext;
    fn new_ctx(&self) -> Self::CTX {
        let mut gateway_context = GatewayContext::new();
        gateway_context.insert(RequestMeta::default());
        gateway_context
    }

    async fn upstream_peer(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX
    ) -> pingora::Result<Box<HttpPeer>> {
        let req_meta = ctx.get::<RequestMeta>().expect("RequestMeta should exist");
        let upstream_id = match &req_meta.upstream_id {
            Some(id) => id.clone(),
            None => {
                session.respond_error(500).await?;
                eprintln!("No upstream_id set");
                return Err(Error::explain(
                    pingora::ErrorType::Custom("UpstreamError"),
                    "Upstream id not set"
                ));
            }
        };
        let upstream = match self.upstreams.get(&upstream_id) {
            Some(upstream) => upstream,
            None => {
                session.respond_error(500).await?;
                eprintln!("No upstream_id set");
                return Err(Error::explain(
                    pingora::ErrorType::Custom("UpstreamError"),
                    "Upstream config not found"
                ));
            }
        };

        let peer = upstream.select_peer(session, ctx).await?;
        Ok(peer)
    }

    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> pingora::Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        let id = uuid::Uuid::new_v4().to_string();
        let req_meta = ctx
            .get_mut::<RequestMeta>()
            .expect("Context should already be created");
        // RequestMeta.start_time added by default initializer already.
        req_meta.request_id = id;

        let path = session.req_header().uri.path();
        let host = session.req_header().headers.get("Host")
            .and_then(|v| v.to_str().ok())
            .or_else(|| session.req_header().uri.host());
        let route_match = self.router.match_request(path, host);
        let route = match route_match {
            Some(route_match) => { route_match },
            None => { session.respond_error(404).await?; return Ok(true); }
        };
        req_meta.matched_route_id = Some(route.path.clone());
        req_meta.upstream_id = Some(route.upstream_id.clone());
        ctx.insert(route.clone());

        for middleware in &self.middlewares {
            let result = middleware.handle_request(session, ctx).await?;
            if result == MiddlewareDecision::Stop {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn early_request_filter(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX
    ) -> pingora::Result<()>
    where
        Self::CTX: Send + Sync,
    {
        for middleware in &self.middlewares {
            let result = middleware.handle_early_request(session, ctx).await?;
            if result == MiddlewareDecision::Stop {
                break;
            }
        }
        Ok(())
    }

    async fn request_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX
    ) -> pingora::Result<()>
    where
        Self::CTX: Send + Sync,
    {
        for middleware in &self.middlewares {
            let result = middleware.handle_request_body(
                session,
                body,
                end_of_stream,
                ctx
            ).await?;
            if result == MiddlewareDecision::Stop {
                return Err(Error::explain(
                    pingora::ErrorType::Custom("MiddlewareRejected"),
                    "Body Rejected"
                ));
            }
        }
        Ok(())
    }

    async fn upstream_request_filter(
        &self,
        session: &mut Session,
        upstream_request: &mut RequestHeader,
        ctx: &mut Self::CTX
    ) -> pingora::Result<()>
    where
        Self::CTX: Send + Sync,
    {
        for middleware in &self.middlewares {
            let result = middleware.handle_upstream_request(session, upstream_request, ctx).await?;
            if result == MiddlewareDecision::Stop {
                return Err(Error::explain(
                    pingora::ErrorType::Custom("MiddlewareRejected"),
                    "Upstream Request Rejected"
                ));
            }
        }

        let route = ctx.get::<RouteConf>().expect("RouteConf should exist");
        let req_meta = ctx.get::<RequestMeta>().expect("RequestMeta should exist");

        for (header_key, header_value) in &route.headers.add_req_headers {
            upstream_request.insert_header(header_key.clone(), header_value.clone())?;
        }
        for header_key in &route.headers.remove_req_headers {
            upstream_request.remove_header(header_key);
        }

        if !route.headers.preserve_host_header {
            if let Some(sni) = &req_meta.sni {
                upstream_request.insert_header("Host", sni.clone())?;
            }
        }

        Ok(())
    }

    fn upstream_response_filter(
        &self,
        session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX
    ) -> pingora::Result<()> {
        for middleware in self.middlewares.iter().rev() {
            let result = middleware.handle_upstream_response(session, upstream_response, ctx)?;
            if result == MiddlewareDecision::Stop {
                return Err(Error::explain(
                    pingora::ErrorType::Custom("MiddlewareRejected"),
                    "Upstream Response Filter Aborted"
                ));
            }
        }

        let route = ctx.get::<RouteConf>().expect("RouteConf should exist");
        for header_key in &route.headers.remove_resp_headers {
            upstream_response.remove_header(header_key);
        }

        Ok(())
    }

    async fn response_filter(
        &self,
        session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX
    ) -> pingora::Result<()>
    where
        Self::CTX: Send + Sync,
    {
        for middleware in self.middlewares.iter().rev() {
            let result = middleware.handle_response(session, upstream_response, ctx).await?;
            if result == MiddlewareDecision::Stop {
                return Err(Error::explain(
                    pingora::ErrorType::Custom("MiddlewareRejected"),
                    "Response Filter Aborted"
                ));
            }
        }

        let route = ctx.get::<RouteConf>().expect("RouteConf should exist");
        for (header_key, header_value) in &route.headers.add_resp_headers {
            upstream_response.insert_header(header_key.clone(), header_value.clone())?;
        }
        Ok(())
    }

    fn response_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX
    ) -> pingora::Result<Option<Duration>>
    where
        Self::CTX: Send + Sync,
    {
        for middleware in self.middlewares.iter().rev() {
            let result = middleware.handle_response_body(session, body, end_of_stream, ctx)?;
            if result == MiddlewareDecision::Stop {
                return Err(Error::explain(
                    pingora::ErrorType::Custom("MiddlewareRejected"),
                    "Response Body Rejected")
                )
            }
        }
        Ok(None)
    }

    async fn logging(&self, session: &mut Session, e: Option<&Error>, ctx: &mut Self::CTX)
    where
        Self::CTX: Send + Sync,
    {
        for middleware in self.middlewares.iter().rev() {
            let result = middleware.handle_logging(session, e, ctx)
                .await
                .unwrap_or(MiddlewareDecision::Continue);
            if result == MiddlewareDecision::Stop {
                return
            }
        }
    }

    async fn fail_to_proxy(
        &self,
        session: &mut Session,
        e: &Error,
        ctx: &mut Self::CTX
    ) -> FailToProxy
    where
        Self::CTX: Send + Sync,
    {
        for middleware in self.middlewares.iter().rev() {
            let result = middleware.handle_error(session, e, ctx)
                .await
                .unwrap_or(MiddlewareDecision::Continue);

            if result == MiddlewareDecision::Stop {
                return FailToProxy { error_code: 0, can_reuse_downstream: false }
            }
        }

        let code = match e.etype {
            pingora::ErrorType::ConnectRefused
            | pingora::ErrorType::ConnectNoRoute
            | pingora::ErrorType::BindError => 502,
            pingora::ErrorType::ReadTimedout
            | pingora::ErrorType::ConnectTimedout => 504,
            _ => 500,
        };

        let mut error_page_served = false;

        if let Some(route) = ctx.get::<RouteConf>() {
            if let Some(pages) = &route.error_pages {
                if let Some(path) = pages.get(&code) {
                    match tokio::fs::read(path).await {
                        Ok(content) => {
                            let mut header = ResponseHeader::build(code, Some(4)).unwrap();
                            header.insert_header("Content-Type", "text/html").unwrap();
                            header.insert_header("Content-Length", content.len().to_string()).unwrap();

                            // Attempt to write. If client disconnected, this might fail, which is fine.
                            if session.write_response_header(Box::new(header), false).await.is_ok() {
                                let _ = session.write_response_body(Some(Bytes::from(content)), true).await;
                                error_page_served = true;
                            }
                        },
                        Err(err) => {
                            eprintln!("Failed to read custom error page '{}': {}", path, err);
                        }
                    }
                }
            }
        }

        if !error_page_served {
            let _ = session.respond_error(code).await;
        }

        FailToProxy {
            error_code: code,
            can_reuse_downstream: false,
        }
    }

    async fn connected_to_upstream(
        &self,
        session: &mut Session,
        reused: bool,
        peer: &HttpPeer,
        fd: RawFd,
        digest: Option<&Digest>,
        ctx: &mut Self::CTX
    ) -> pingora::Result<()>
    where
        Self::CTX: Send + Sync,
    {
        for middleware in &self.middlewares {
            let result = middleware.handle_upstream_connected(session, reused, peer, fd, digest, ctx).await?;
            if result == MiddlewareDecision::Stop {
                return Err(Error::explain(
                    pingora::ErrorType::Custom("MiddlewareRejected"),
                    "Upstream Connection Aborted"
                ));
            }
        }
        Ok(())
    }
}