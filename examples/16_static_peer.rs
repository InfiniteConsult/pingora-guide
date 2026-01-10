use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;

pub struct StaticPeerProxy;

#[async_trait]
impl ProxyHttp for StaticPeerProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let addr = ("172.28.0.20", 8080);
        let use_tls = false;
        let sni = "blue.pingora.local".to_string();
        let peer = Box::new(HttpPeer::new(addr, use_tls, sni));

        info!("Connecting to static peer: {:?}", addr);
        Ok(peer)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, StaticPeerProxy);
    my_proxy.add_tcp("0.0.0.0:6160");

    info!("Static Peer Proxy running on 0.0.0.0:6160");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}