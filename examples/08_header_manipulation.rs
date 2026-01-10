use async_trait::async_trait;
use log::info;
use pingora::http::ResponseHeader;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;

pub struct HeaderModProxy;

#[async_trait]
impl ProxyHttp for HeaderModProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> pingora::Result<Box<HttpPeer>> {
        let addr = ("172.28.0.21", 8080);
        let peer = Box::new(HttpPeer::new(
            addr,
            false,
            "green.pingora.local".to_string()
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
        upstream_request.insert_header("Host", "green.pingora.local")?;
        upstream_request.insert_header("X-Pingora-Proxy", "true")?;
        let _ = upstream_request.remove_header("User-Agent");
        info!("Request headers modified: Added X-Pingora-Proxy, Removed User-Agent.");
        Ok(())
    }

    async fn response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        upstream_response.insert_header("X-Edited-By", "Pingora")?;
        info!("Response headers modified: Added X-Edited-By");
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, HeaderModProxy);
    my_proxy.add_tcp("0.0.0.0:6148");

    info!("Header Manipulation Proxy running on 0.0.0.0:6148 -> Forwarding to Upstream Green");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}