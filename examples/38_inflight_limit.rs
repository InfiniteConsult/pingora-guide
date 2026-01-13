use async_trait::async_trait;
use log::{info, warn};
use once_cell::sync::Lazy;
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;

use pingora_limits::inflight::{Inflight, Guard};
use pingora_load_balancing::{LoadBalancer, selection::RoundRobin};
use std::sync::Arc;
use std::time::Duration;

static INFLIGHT_LIMITER: Lazy<Inflight> = Lazy::new(|| Inflight::new());
const MAX_CONCURRENT_REQ: isize = 2;

pub struct InflightCtx {
    pub guard: Option<Guard>,
}

pub struct InflightLimitProxy(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for InflightLimitProxy {
    type CTX = InflightCtx;
    fn new_ctx(&self) -> Self::CTX {
        InflightCtx { guard: None }
    }

    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        let key = "global_limit";
        let (guard, count) = INFLIGHT_LIMITER.incr(key, 1);

        if count > MAX_CONCURRENT_REQ {
            warn!("In-flight Limit Exceeded {}/{}", count, MAX_CONCURRENT_REQ);
            session.respond_error(429).await?;
            return Ok(true);
        }
        ctx.guard = Some(guard);

        Ok(false)
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        tokio::time::sleep(Duration::from_secs(2)).await;

        let peer = Box::new(HttpPeer::new(upstream, false, "inflight.cluster".to_string()));
        Ok(peer)
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

    let mut my_proxy = http_proxy_service(&my_server.configuration, InflightLimitProxy(Arc::new(upstreams)));
    my_proxy.add_tcp("0.0.0.0:6182");

    info!("In-flight Limiter running on 0.0.0.0:6182");
    info!("Max Concurrent Requests: {}", MAX_CONCURRENT_REQ);
    info!("Note: Upstream connection has artificial 2s delay to simulate load.");

    my_server.add_service(my_proxy);
    my_server.run_forever();
}