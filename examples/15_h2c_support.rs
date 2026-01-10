use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::{ALPN, HttpPeer};

pub struct H2cProxy;

#[async_trait]
impl ProxyHttp for H2cProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let addr = ("172.28.0.22", 8081);
        let mut peer = Box::new(HttpPeer::new(
            addr,
            false,
            "advanced.pingora.local".to_string(),
        ));

        peer.options.alpn = ALPN::H2;

        info!("Forwarding to Upstream Advanced via H2C (Port 8081)");
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
        upstream_request.insert_header("Host", "advanced.pingora.local")?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, H2cProxy);

    // We accept standard HTTP/1.1 on the front end for simplicity
    my_proxy.add_tcp("0.0.0.0:6155");

    info!("Proxy running on 0.0.0.0:6155 (HTTP/1.1) -> Forwarding to Upstream (H2C)");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}