use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::services::background::background_service;

use pingora_load_balancing::LoadBalancer;
use pingora_load_balancing::selection::consistent::KetamaHashing;
use std::sync::Arc;

pub struct LB(Arc<LoadBalancer<KetamaHashing>>);

#[async_trait]
impl ProxyHttp for LB {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>>
    {
        let path = session.req_header().uri.path();
        let key = path.as_bytes();

        let upstream = self.0
            .select(key, 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        info!("Path '{}' hashed to upstream: {:?}", path, upstream);
        let peer = Box::new(HttpPeer::new(
            upstream,
            false,
            "consistent.cluster".to_string()
        ));
        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        upstream_request.insert_header("Host", "consistent-hash-cluster")?;
        Ok(())
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

    let background = background_service("consistent_lb", upstreams);
    let lb_ref = background.task();

    let mut my_proxy = http_proxy_service(&my_server.configuration, LB(lb_ref));
    my_proxy.add_tcp("0.0.0.0:6172");

    info!("Consistent Hashing LB running on 0.0.0.0:6172");
    info!("Try: curl http://127.0.0.1:6172/user/123 vs /user/456");

    my_server.add_service(background);
    my_server.add_service(my_proxy);;

    my_server.run_forever();
}