use async_trait::async_trait;
use log::{info, warn};
use once_cell::sync::Lazy;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;

use pingora_limits::rate::Rate;
use pingora_load_balancing::{LoadBalancer, selection::RoundRobin};
use std::sync::Arc;
use std::time::Duration;

static RATE_LIMITER: Lazy<Rate> = Lazy::new(|| { Rate::new(Duration::from_secs(1)) });
const MAX_REQ_PER_SEC: isize = 5;

pub struct RateLimiterProxy(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for RateLimiterProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;
        info!("Selected upstream: {:?}", upstream);
        let peer = Box::new(HttpPeer::new(upstream, false, "rate-limited.cluster".to_string()));
        Ok(peer)
    }

    async fn request_filter(
        &self, session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        let client_ip = match session.client_addr() {
            Some(addr) => addr.as_inet().unwrap().ip(),
            None => {
                warn!("Could not determine client IP, allowing request.");
                return Ok(false);
            }
        };

        let curr_req_count = RATE_LIMITER.observe(&client_ip, 1);

        if curr_req_count > MAX_REQ_PER_SEC {
            warn!("Rate Limit Exceeded for {}: {} req/s", client_ip, curr_req_count);
            session.respond_error(429).await?;
            return Ok(true);
        }
        Ok(false)
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

    let mut my_proxy = http_proxy_service(&my_server.configuration, RateLimiterProxy(Arc::new(upstreams)));
    my_proxy.add_tcp("0.0.0.0:6180");

    info!("Rate Limiter Proxy running on 0.0.0.0:6180");
    info!("Limit: {} requests per second per IP", MAX_REQ_PER_SEC);

    my_server.add_service(my_proxy);
    my_server.run_forever();
}