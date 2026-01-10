use async_trait::async_trait;
use log::{error, info};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;


#[derive(Debug, Clone, Copy)]
pub enum Target {
    Blue,
    Green,
}

pub struct PathRouter;

#[async_trait]
impl ProxyHttp for PathRouter {
    type CTX = Option<Target>;

    fn new_ctx(&self) -> Self::CTX {
        None
    }

    async fn request_filter(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX
    ) -> pingora::Result<bool> {
        let path = session.req_header().uri.path();

        if path.starts_with("/blue") {
            *ctx = Some(Target::Blue);
        } else if path.starts_with("/green") {
            *ctx = Some(Target::Green);
        } else {
            let _ = session.respond_error(404).await;
            return Ok(true)
        }
        Ok(false)
    }

    async fn upstream_peer(
        &self, session: &mut Session,
        ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let target = ctx.expect("Context should be set by request_filter");

        let (addr, sni) = match target {
            Target::Blue => (("172.28.0.20", 8080), "blue.pingora.local"),
            Target::Green => (("172.28.0.21", 8080), "green.pingora.local"),
        };

        info!("Routing request to {:?} based on path", target);
        let peer = Box::new(HttpPeer::new(addr, false, sni.to_string()));
        Ok(peer)
    }

    async fn upstream_request_filter(
        &self, _session: &mut Session,
        upstream_request: &mut RequestHeader,
        ctx: &mut Self::CTX
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        let target = ctx.expect("Context should be set");
        let host = match target {
            Target::Blue => "blue.pingora.local",
            Target::Green => "green.pingora.local",
        };

        upstream_request.insert_header("Host", host)?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, PathRouter);
    my_proxy.add_tcp("0.0.0.0:6149");

    info!("Path Router running on 0.0.0.0:6149");
    info!("Try: curl http://127.0.0.1:6149/blue or /green");

    my_server.add_service(my_proxy);
    my_server.run_forever();
}
