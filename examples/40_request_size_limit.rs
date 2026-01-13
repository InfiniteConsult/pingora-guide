use async_trait::async_trait;
use bytes::Bytes;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;
use pingora::proxy::FailToProxy;
use http::header::CONTENT_LENGTH;

use pingora_load_balancing::{LoadBalancer, selection::RoundRobin};
use std::sync::Arc;

const MAX_BODY_SIZE: usize = 100;

pub struct SizeCtx {
    pub bytes_read: usize,
}

pub struct SizeLimitProxy(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for SizeLimitProxy {
    type CTX = SizeCtx;
    fn new_ctx(&self) -> Self::CTX {
        SizeCtx { bytes_read: 0 }
    }

    async fn request_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        if let Some(value) = session.req_header().headers.get(CONTENT_LENGTH) {
            if let Ok(len_str) = value.to_str() {
                if let Ok(len) = len_str.parse::<usize>() {
                    if len > MAX_BODY_SIZE {
                        warn!("Rejecting request by Header: Content-Length: {} > {}", len_str, MAX_BODY_SIZE);
                        session.respond_error(413).await?;
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    async fn request_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        _end_of_stream: bool,
        ctx: &mut Self::CTX
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        if let Some(b) = body {
            ctx.bytes_read += b.len();
            if ctx.bytes_read >= MAX_BODY_SIZE {
                warn!("Rejecting request by Stream: Accumulated {} bytes > {}", ctx.bytes_read, MAX_BODY_SIZE);
                return Err(Error::explain(ErrorType::Custom("BodyTooLarge"), "Stream exceeded limit"));
            }
        }
        Ok(())
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        let peer = Box::new(HttpPeer::new(upstream, false, "size-limit.cluster".to_string()));
        Ok(peer)
    }

    async fn fail_to_proxy(
        &self,
        session: &mut Session,
        e: &Error,
        _ctx: &mut Self::CTX
    ) -> FailToProxy
    where
        Self::CTX: Send + Sync,
    {
        if let ErrorType::Custom("BodyTooLarge") = e.etype {
            let _ = session.respond_error(413).await;
            return FailToProxy {
                error_code: 413,
                can_reuse_downstream: false,
            };
        }

        FailToProxy {
            error_code: 502,
            can_reuse_downstream: false,
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080",
        "172.28.0.21:8080",
    ])?;

    let mut my_proxy = http_proxy_service(&my_server.configuration, SizeLimitProxy(Arc::new(upstreams)));
    my_proxy.add_tcp("0.0.0.0:6184");

    info!("Request Size Limiter running on 0.0.0.0:6184");
    info!("Max Body Size: {} bytes", MAX_BODY_SIZE);

    my_server.add_service(my_proxy);
    my_server.run_forever();
}