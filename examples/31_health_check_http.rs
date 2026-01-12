use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::services::background::background_service;

use pingora_load_balancing::LoadBalancer;
use pingora_load_balancing::selection::RoundRobin;
use pingora_load_balancing::health_check::HttpHealthCheck;
use std::sync::Arc;
use std::time::Duration;

pub struct LB(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for LB {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "All upstreams are down"))?;

        info!("Routed to upstream: {:?}", upstream);

        let mut peer = Box::new(HttpPeer::new(
            upstream,
            false,
            "http-health-check.cluster.local".to_string(),
        ));

        peer.options.read_timeout = Some(Duration::from_secs(1));
        peer.options.connection_timeout = Some(Duration::from_secs(1));

        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        upstream_request.insert_header("Host", "http-health-check-cluster")?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080",
        "172.28.0.21:8080",
    ])?;

    let mut hc = Box::new(HttpHealthCheck::new("localhost", false));

    hc.peer_template.options.read_timeout = Some(Duration::from_secs(1));
    hc.peer_template.options.connection_timeout = Some(Duration::from_secs(1));
    hc.consecutive_success = 1;
    hc.consecutive_failure = 1;

    upstreams.set_health_check(hc);
    upstreams.health_check_frequency = Some(Duration::from_secs(1));
    upstreams.parallel_health_check = true;

    let background = background_service("http_health_check_lb", upstreams);
    let lb_ref = background.task();

    let mut my_proxy = http_proxy_service(&my_server.configuration, LB(lb_ref));
    my_proxy.add_tcp("0.0.0.0:6175");

    info!("HTTP Health Check LB running on 0.0.0.0:6175");

    my_server.add_service(background);
    my_server.add_service(my_proxy);

    my_server.run_forever();
}