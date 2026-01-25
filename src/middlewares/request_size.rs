use async_trait::async_trait;
use bytes::Bytes;
use http::header::CONTENT_LENGTH;
use log::warn;
use pingora::prelude::*;
use crate::config::ServerConf;
use crate::middleware::{Middleware, MiddlewareDecision};
use crate::context::GatewayContext;
use crate::error::{GatewayError, PingoraGuideError, Result};

pub struct RequestSizeState {
    pub bytes_read: usize,
}

pub struct RequestSizeMiddleware {
    pub max_bytes: usize,
}

impl Default for RequestSizeState {
    fn default() -> Self {
        Self { bytes_read: 0 }
    }
}

impl RequestSizeMiddleware {
    pub fn new(conf: &ServerConf) -> Self {
        let mut max_bytes = 1024 * 1024 * 10;
        if let Some(max_body_size) = conf.client_max_body_size {
            max_bytes = max_body_size;
        };
        RequestSizeMiddleware { max_bytes }
    }
}

#[async_trait]
impl Middleware for RequestSizeMiddleware {
    fn name(&self) -> &str {
        "request_size"
    }

    async fn handle_request(&self, session: &mut Session, ctx: &mut GatewayContext) -> Result<MiddlewareDecision> {
        ctx.insert(RequestSizeState::default());
        if let Some(value) = session.req_header().headers.get(CONTENT_LENGTH) {
            if let Ok(len_str) = value.to_str() {
                if let Ok(len) = len_str.parse::<usize>() {
                    if len > self.max_bytes {
                        eprintln!("Rejecting request by Header: Content-Length: {} > {}", len_str, self.max_bytes);
                        session.respond_error(413).await.map_err(|e| {
                            PingoraGuideError::Gateway(GatewayError::SizeLimited(e.to_string()))
                        })?;
                        return Ok(MiddlewareDecision::Stop);
                    }
                }
            }
        }
        Ok(MiddlewareDecision::Continue)
    }

    async fn handle_request_body(
        &self,
        session: &mut Session,
        body: &mut Option<Bytes>,
        _end_of_stream: bool,
        ctx: &mut GatewayContext
    ) -> Result<MiddlewareDecision> {
        let req_size_state = ctx.get_mut::<RequestSizeState>()
            .expect("Request size state should exist");

        if let Some(b) = body {
            req_size_state.bytes_read += b.len();
            if req_size_state.bytes_read >= self.max_bytes {
                eprintln!("Rejecting request by Stream: Accumulated {} bytes > {}", req_size_state.bytes_read, self.max_bytes);
                session.respond_error(413).await.map_err(|e| {
                    PingoraGuideError::Gateway(GatewayError::SizeLimited(e.to_string()))
                })?;
                return Ok(MiddlewareDecision::Stop);
            }
        }

        Ok(MiddlewareDecision::Continue)
    }
}
