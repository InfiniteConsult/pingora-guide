use async_trait::async_trait;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;

use pingora_load_balancing::{LoadBalancer, selection::RoundRobin};
use std::sync::Arc;

pub struct FirewallProxy(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for FirewallProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn request_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        let client_ip = match session.client_addr() {
            Some(addr) => addr.as_inet().unwrap().ip(),
            None => {
                warn!("Unknown client address, allowing...");
                return Ok(false);
            }
        };

        let bad_actor_ip = "172.28.0.31".parse::<std::net::IpAddr>().unwrap();

        if client_ip == bad_actor_ip {
            warn!("Access Denied for IP: {}", client_ip);
            session.respond_error(403).await?;
            return Ok(true);
        }

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

        let peer = Box::new(HttpPeer::new(upstream, false, "firewall.cluster".to_string()));
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

    let mut my_proxy = http_proxy_service(&my_server.configuration, FirewallProxy(Arc::new(upstreams)));
    my_proxy.add_tcp("0.0.0.0:6183");

    info!("Firewall Proxy running on 0.0.0.0:6183");
    info!("Blocking Bad Actor: 172.28.0.31");

    my_server.add_service(my_proxy);
    my_server.run_forever();
}