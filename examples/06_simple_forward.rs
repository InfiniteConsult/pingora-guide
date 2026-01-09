use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;

pub struct SimpleProxy;

#[async_trait]
impl ProxyHttp for SimpleProxy {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> pingora::Result<Box<HttpPeer>> {
        let addr = ("172.28.0.20", 8080);
        info!("Forwarding request to Upstream Blue ({:?})", addr);
        let peer = Box::new(HttpPeer::new(
            addr,
            false,
            "blue.pingora.local".to_string()
        ));
        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX
    ) -> pingora::Result<()>
    where
        Self::CTX: Send + Sync,
    {
        let _ = upstream_request.insert_header("Host", "blue.pingora.local");
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(
        &my_server.configuration,
        SimpleProxy
    );

    my_proxy.add_tcp("0.0.0.0:6146");

    info!("Simple Proxy running on 0.0.0.0:6146 -> Forwarding to Upstream Blue");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}