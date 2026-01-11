use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;

pub struct SniRouter;

pub struct SniCtx {
    pub target_host: String,
}

#[async_trait]
impl ProxyHttp for SniRouter {
    type CTX = SniCtx;

    fn new_ctx(&self) -> Self::CTX {
        SniCtx {
            target_host: String::new(),
        }
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let addr = ("172.28.0.22", 443);
        let sni = ctx.target_host.clone();
        let peer = Box::new(HttpPeer::new(addr, true, sni.clone()));
        info!("Connecting to IP: {:?} with SNI: {}", addr, sni);
        Ok(peer)
    }

    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        let path = session.req_header().uri.path();
        ctx.target_host = if path.starts_with("/v1") {
            "v1.api.pingora.local".to_string()
        } else {
            "v2.api.pingora.local".to_string()
        };

        Ok(false)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        ctx: &mut Self::CTX
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        upstream_request.insert_header("Host", &ctx.target_host)?;
        Ok(())
    }

}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, SniRouter);
    my_proxy.add_tcp("0.0.0.0:6164");

    info!("SNI Routing Proxy running on 0.0.0.0:6164");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}