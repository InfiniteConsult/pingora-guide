use async_trait::async_trait;
use log::{error, info};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use tokio::net::lookup_host;

pub struct DnsPeerProxy;

#[async_trait]
impl ProxyHttp for DnsPeerProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let hostname = "blue.pingora.local";
        let port = 8080;
        let target = format!("{}:{}", hostname, port);

        info!("Resolving host: {}", target);

        let mut addrs = lookup_host(&target).await
            .map_err(|_e| pingora::Error::new(ErrorType::Custom("DNSResolutionFailed")))?;

        if let Some(addr) = addrs.next() {
            info!("Resolved {} -> {}", hostname, addr);
            let peer = Box::new(HttpPeer::new(addr, false, hostname.to_string()));
            Ok(peer)
        } else {
            error!("DNS lookup returned no records for {}", hostname);
            Err(pingora::Error::new(ErrorType::Custom("DNSNoRecords")))
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, DnsPeerProxy);
    my_proxy.add_tcp("0.0.0.0:6161");

    info!("DNS Peer Proxy running on 0.0.0.0:6161");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}